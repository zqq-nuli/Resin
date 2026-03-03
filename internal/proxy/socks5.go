package proxy

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/Resinat/Resin/internal/netutil"
	"github.com/Resinat/Resin/internal/outbound"
	"github.com/Resinat/Resin/internal/routing"
	M "github.com/sagernet/sing/common/metadata"
)

// SOCKS5 protocol constants.
const (
	socks5Version byte = 0x05

	// Authentication methods.
	socks5AuthNone     byte = 0x00
	socks5AuthPassword byte = 0x02
	socks5AuthNoAccept byte = 0xFF

	// RFC 1929 sub-negotiation version.
	socks5AuthPasswordVersion byte = 0x01

	// Commands.
	socks5CmdConnect byte = 0x01

	// Address types.
	socks5AddrIPv4   byte = 0x01
	socks5AddrDomain byte = 0x03
	socks5AddrIPv6   byte = 0x04

	// Reply codes.
	socks5RepSuccess              byte = 0x00
	socks5RepGeneralFailure       byte = 0x01
	socks5RepConnectionNotAllowed byte = 0x02
	socks5RepHostUnreachable      byte = 0x04
	socks5RepConnectionRefused    byte = 0x05
	socks5RepCommandNotSupported  byte = 0x07
	socks5RepAddrTypeNotSupported byte = 0x08
)

// SOCKS5ProxyConfig holds dependencies for the SOCKS5 proxy.
type SOCKS5ProxyConfig struct {
	ProxyToken  string
	Router      *routing.Router
	Pool        outbound.PoolAccessor
	Health      HealthRecorder
	Events      EventEmitter
	MetricsSink MetricsEventSink
}

// SOCKS5Proxy handles SOCKS5 protocol connections.
type SOCKS5Proxy struct {
	token       string
	router      *routing.Router
	pool        outbound.PoolAccessor
	health      HealthRecorder
	events      EventEmitter
	metricsSink MetricsEventSink
}

// NewSOCKS5Proxy creates a new SOCKS5 proxy handler.
func NewSOCKS5Proxy(cfg SOCKS5ProxyConfig) *SOCKS5Proxy {
	ev := cfg.Events
	if ev == nil {
		ev = NoOpEventEmitter{}
	}
	return &SOCKS5Proxy{
		token:       cfg.ProxyToken,
		router:      cfg.Router,
		pool:        cfg.Pool,
		health:      cfg.Health,
		events:      ev,
		metricsSink: cfg.MetricsSink,
	}
}

// HandleConn handles a single SOCKS5 client connection.
// It performs the full SOCKS5 handshake, authenticates, and tunnels traffic.
func (s *SOCKS5Proxy) HandleConn(conn net.Conn) {
	defer conn.Close()

	// Phase 1: Version + method negotiation.
	platName, account, err := s.negotiate(conn)
	if err != nil {
		return
	}

	// Phase 2: Read CONNECT request.
	target, err := s.readRequest(conn)
	if err != nil {
		return
	}

	// Phase 3: Handle the CONNECT tunnel.
	s.handleConnect(conn, platName, account, target)
}

// negotiate performs SOCKS5 version check and authentication negotiation.
// Returns the extracted platform name and account from credentials.
func (s *SOCKS5Proxy) negotiate(conn net.Conn) (platName string, account string, err error) {
	// Read version + number of methods.
	header := make([]byte, 2)
	if _, err := io.ReadFull(conn, header); err != nil {
		return "", "", err
	}
	if header[0] != socks5Version {
		return "", "", fmt.Errorf("unsupported SOCKS version: %d", header[0])
	}

	nMethods := int(header[1])
	if nMethods == 0 {
		conn.Write([]byte{socks5Version, socks5AuthNoAccept})
		return "", "", fmt.Errorf("no authentication methods offered")
	}

	methods := make([]byte, nMethods)
	if _, err := io.ReadFull(conn, methods); err != nil {
		return "", "", err
	}

	// Check which methods the client supports.
	hasPassword := false
	hasNone := false
	for _, m := range methods {
		switch m {
		case socks5AuthPassword:
			hasPassword = true
		case socks5AuthNone:
			hasNone = true
		}
	}

	if s.token == "" {
		// Auth disabled: accept password auth if offered (to extract identity),
		// otherwise accept no-auth.
		if hasPassword {
			conn.Write([]byte{socks5Version, socks5AuthPassword})
			return s.readPasswordAuth(conn, false)
		}
		if hasNone {
			conn.Write([]byte{socks5Version, socks5AuthNone})
			return "", "", nil
		}
		conn.Write([]byte{socks5Version, socks5AuthNoAccept})
		return "", "", fmt.Errorf("no acceptable method")
	}

	// Auth enabled: require username/password.
	if !hasPassword {
		conn.Write([]byte{socks5Version, socks5AuthNoAccept})
		return "", "", fmt.Errorf("password auth required but not offered")
	}
	conn.Write([]byte{socks5Version, socks5AuthPassword})

	return s.readPasswordAuth(conn, true)
}

// readPasswordAuth reads RFC 1929 username/password sub-negotiation.
// username = PROXY_TOKEN, password = "platform:account"
// When validateToken is true, the username must match the configured proxy token.
func (s *SOCKS5Proxy) readPasswordAuth(conn net.Conn, validateToken bool) (platName string, account string, err error) {
	// RFC 1929: VER(1) | ULEN(1) | UNAME(1-255) | PLEN(1) | PASSWD(1-255)
	ver := make([]byte, 1)
	if _, err := io.ReadFull(conn, ver); err != nil {
		return "", "", err
	}
	if ver[0] != socks5AuthPasswordVersion {
		conn.Write([]byte{socks5AuthPasswordVersion, 0x01})
		return "", "", fmt.Errorf("unsupported auth version: %d", ver[0])
	}

	ulenBuf := make([]byte, 1)
	if _, err := io.ReadFull(conn, ulenBuf); err != nil {
		return "", "", err
	}
	uname := make([]byte, ulenBuf[0])
	if _, err := io.ReadFull(conn, uname); err != nil {
		return "", "", err
	}

	plenBuf := make([]byte, 1)
	if _, err := io.ReadFull(conn, plenBuf); err != nil {
		return "", "", err
	}
	passwd := make([]byte, plenBuf[0])
	if _, err := io.ReadFull(conn, passwd); err != nil {
		return "", "", err
	}

	username := string(uname)
	password := string(passwd)

	// Validate token when auth is enabled.
	if validateToken && username != s.token {
		conn.Write([]byte{socks5AuthPasswordVersion, 0x01})
		return "", "", fmt.Errorf("auth failed")
	}

	// Successful authentication.
	conn.Write([]byte{socks5AuthPasswordVersion, 0x00})

	// Parse "platform:account" from password field.
	platName, account = parsePlatformAccount(password)
	return platName, account, nil
}

// readRequest reads the SOCKS5 request and returns the target address as "host:port".
func (s *SOCKS5Proxy) readRequest(conn net.Conn) (string, error) {
	// VER(1) | CMD(1) | RSV(1) | ATYP(1)
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return "", err
	}
	if header[0] != socks5Version {
		return "", fmt.Errorf("unsupported version in request: %d", header[0])
	}
	if header[1] != socks5CmdConnect {
		s.sendReply(conn, socks5RepCommandNotSupported)
		return "", fmt.Errorf("unsupported command: %d", header[1])
	}

	// Parse target address based on ATYP.
	var host string
	switch header[3] {
	case socks5AddrIPv4:
		addr := make([]byte, 4)
		if _, err := io.ReadFull(conn, addr); err != nil {
			return "", err
		}
		host = net.IP(addr).String()

	case socks5AddrDomain:
		lenBuf := make([]byte, 1)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			return "", err
		}
		domain := make([]byte, lenBuf[0])
		if _, err := io.ReadFull(conn, domain); err != nil {
			return "", err
		}
		host = string(domain)

	case socks5AddrIPv6:
		addr := make([]byte, 16)
		if _, err := io.ReadFull(conn, addr); err != nil {
			return "", err
		}
		host = net.IP(addr).String()

	default:
		s.sendReply(conn, socks5RepAddrTypeNotSupported)
		return "", fmt.Errorf("unsupported address type: %d", header[3])
	}

	// Read port (2 bytes, big-endian).
	portBuf := make([]byte, 2)
	if _, err := io.ReadFull(conn, portBuf); err != nil {
		return "", err
	}
	port := binary.BigEndian.Uint16(portBuf)

	return net.JoinHostPort(host, strconv.Itoa(int(port))), nil
}

// sendReply sends a SOCKS5 reply with the given status code.
func (s *SOCKS5Proxy) sendReply(conn net.Conn, rep byte) {
	// VER(1) | REP(1) | RSV(1) | ATYP(1) | BND.ADDR(4) | BND.PORT(2)
	reply := []byte{
		socks5Version, rep, 0x00, socks5AddrIPv4,
		0, 0, 0, 0, // BND.ADDR = 0.0.0.0
		0, 0, // BND.PORT = 0
	}
	conn.Write(reply)
}

// handleConnect establishes the upstream connection and tunnels bidirectionally.
func (s *SOCKS5Proxy) handleConnect(conn net.Conn, platName, account, target string) {
	lifecycle := newRequestLifecycle(s.events, nil, ProxyTypeSOCKS5, true)
	lifecycle.setTarget(target, "")
	defer lifecycle.finish()
	lifecycle.setAccount(account)

	// Extract client IP from the connection.
	if addr, ok := conn.RemoteAddr().(*net.TCPAddr); ok {
		lifecycle.log.ClientIP = addr.IP.String()
	}

	routed, routeErr := resolveRoutedOutbound(s.router, s.pool, platName, account, target)
	if routeErr != nil {
		lifecycle.setProxyError(routeErr)
		rep := socks5RepGeneralFailure
		switch routeErr {
		case ErrNoAvailableNodes:
			rep = socks5RepHostUnreachable
		case ErrPlatformNotFound:
			rep = socks5RepConnectionNotAllowed
		case ErrAuthRequired, ErrAuthFailed:
			rep = socks5RepConnectionNotAllowed
		}
		s.sendReply(conn, rep)
		return
	}
	lifecycle.setRouteResult(routed.Route)

	domain := netutil.ExtractDomain(target)
	nodeHash := routed.Route.NodeHash
	go s.health.RecordLatency(nodeHash, domain, nil)

	// Dial upstream via the selected outbound.
	upstreamRaw, err := routed.Outbound.DialContext(
		context.Background(),
		"tcp",
		M.ParseSocksaddr(target),
	)
	if err != nil {
		proxyErr := classifyConnectError(err)
		if proxyErr == nil {
			lifecycle.setNetOK(true)
			s.sendReply(conn, socks5RepGeneralFailure)
			return
		}
		lifecycle.setProxyError(proxyErr)
		lifecycle.setUpstreamError("socks5_dial", err)
		go s.health.RecordResult(nodeHash, false)

		rep := socks5RepHostUnreachable
		if proxyErr == ErrUpstreamTimeout {
			rep = socks5RepHostUnreachable
		} else if proxyErr == ErrUpstreamConnectFailed {
			rep = socks5RepConnectionRefused
		}
		s.sendReply(conn, rep)
		return
	}

	// Wrap with counting conn for traffic/connection metrics.
	var upstream net.Conn = upstreamRaw
	if s.metricsSink != nil {
		s.metricsSink.OnConnectionLifecycle(ConnectionOutbound, ConnectionOpen)
		upstream = newCountingConn(upstreamRaw, s.metricsSink)
	}

	// Wrap with TLS latency measurement.
	upstream = newTLSLatencyConn(upstream, func(latency time.Duration) {
		s.health.RecordLatency(nodeHash, domain, &latency)
	})

	// Send SOCKS5 success reply.
	s.sendReply(conn, socks5RepSuccess)
	lifecycle.setNetOK(true)

	// Bidirectional tunnel.
	type copyResult struct {
		n   int64
		err error
	}
	egressCh := make(chan copyResult, 1)
	go func() {
		defer upstream.Close()
		defer conn.Close()
		n, copyErr := io.Copy(upstream, conn)
		egressCh <- copyResult{n: n, err: copyErr}
	}()

	ingressBytes, _ := io.Copy(conn, upstream)
	lifecycle.addIngressBytes(ingressBytes)
	conn.Close()
	upstream.Close()
	egressResult := <-egressCh
	lifecycle.addEgressBytes(egressResult.n)

	okResult := ingressBytes > 0 && egressResult.n > 0
	if !okResult {
		lifecycle.setNetOK(false)
		lifecycle.setProxyError(ErrUpstreamRequestFailed)
	}
	go s.health.RecordResult(nodeHash, okResult)
}
