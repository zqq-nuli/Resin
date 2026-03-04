// resin-import: Bulk-import nodes directly into the Resin database,
// bypassing the API. This is much faster for large imports (100k+ nodes).
//
// Usage:
//
//	resin-import -file nodes.yaml -name my-import \
//	  -state-dir /var/lib/resin -cache-dir /var/cache/resin
//
// IMPORTANT: Stop the Resin service before running this tool to avoid
// SQLite locking conflicts. Restart it after the import completes.
package main

import (
	"encoding/json"
	"flag"
	"log"
	"os"
	"time"

	"github.com/Resinat/Resin/internal/model"
	"github.com/Resinat/Resin/internal/node"
	"github.com/Resinat/Resin/internal/state"
	"github.com/Resinat/Resin/internal/subscription"
	"github.com/google/uuid"
)

func main() {
	var (
		filePath string
		subName  string
		subID    string
		stateDir string
		cacheDir string
		limit    int
		dryRun   bool
	)

	flag.StringVar(&filePath, "file", "", "Path to Clash/sing-box YAML or JSON file (required)")
	flag.StringVar(&subName, "name", "bulk-import", "Subscription/group name for imported nodes")
	flag.StringVar(&subID, "sub-id", "", "Existing subscription ID to append to (optional; creates new if empty)")
	flag.StringVar(&stateDir, "state-dir", "/var/lib/resin", "Path to state.db directory")
	flag.StringVar(&cacheDir, "cache-dir", "/var/cache/resin", "Path to cache.db directory")
	flag.IntVar(&limit, "limit", 0, "Max number of unique nodes to import (0 = all)")
	flag.BoolVar(&dryRun, "dry-run", false, "Parse only, don't write to database")
	flag.Parse()

	if filePath == "" {
		flag.Usage()
		os.Exit(1)
	}

	// --- Step 1: Read and parse the subscription file ---
	log.Printf("Reading file: %s", filePath)
	data, err := os.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Failed to read file: %v", err)
	}
	log.Printf("File size: %.1f MB", float64(len(data))/(1024*1024))

	log.Println("Parsing nodes...")
	parsed, err := subscription.ParseGeneralSubscription(data)
	if err != nil {
		log.Fatalf("Failed to parse subscription: %v", err)
	}
	log.Printf("Parsed %d nodes", len(parsed))

	if len(parsed) == 0 {
		log.Println("No nodes found, exiting.")
		os.Exit(1)
	}

	// --- Step 2: Compute hashes and deduplicate ---
	log.Println("Computing hashes and deduplicating...")
	type nodeEntry struct {
		hash       string
		rawOptions json.RawMessage
		tags       []string
	}

	nodeMap := make(map[string]*nodeEntry, len(parsed))
	for _, p := range parsed {
		h := node.HashFromRawOptions(p.RawOptions)
		hex := h.Hex()
		if existing, ok := nodeMap[hex]; ok {
			existing.tags = append(existing.tags, p.Tag)
		} else {
			nodeMap[hex] = &nodeEntry{
				hash:       hex,
				rawOptions: p.RawOptions,
				tags:       []string{p.Tag},
			}
		}
	}
	log.Printf("Unique nodes after dedup: %d (removed %d duplicates)", len(nodeMap), len(parsed)-len(nodeMap))

	// Apply limit if specified
	if limit > 0 && len(nodeMap) > limit {
		log.Printf("Applying limit: keeping %d out of %d nodes", limit, len(nodeMap))
		trimmed := make(map[string]*nodeEntry, limit)
		i := 0
		for k, v := range nodeMap {
			if i >= limit {
				break
			}
			trimmed[k] = v
			i++
		}
		nodeMap = trimmed
	}

	if dryRun {
		log.Println("[dry-run] Stopping here. No database writes.")
		os.Exit(0)
	}

	// --- Step 3: Open databases ---
	log.Printf("Opening databases (state=%s, cache=%s)...", stateDir, cacheDir)
	engine, closer, err := state.PersistenceBootstrap(stateDir, cacheDir)
	if err != nil {
		log.Fatalf("Failed to open databases: %v", err)
	}
	defer closer.Close()

	// --- Step 4: Create or verify subscription ---
	if subID == "" {
		subID = uuid.New().String()
	}
	nowNs := time.Now().UnixNano()

	sub := model.Subscription{
		ID:                        subID,
		Name:                      subName,
		SourceType:                "local",
		URL:                       "",
		Content:                   "", // Not storing raw content for bulk imports
		UpdateIntervalNs:          int64(24 * time.Hour),
		Enabled:                   true,
		Ephemeral:                 false,
		EphemeralNodeEvictDelayNs: int64(72 * time.Hour),
		CreatedAtNs:               nowNs,
		UpdatedAtNs:               nowNs,
	}
	log.Printf("Creating subscription: id=%s name=%s", subID, subName)
	if err := engine.UpsertSubscription(sub); err != nil {
		log.Fatalf("Failed to create subscription: %v", err)
	}

	// --- Step 5: Prepare bulk data ---
	log.Println("Preparing bulk insert data...")
	statics := make([]model.NodeStatic, 0, len(nodeMap))
	dynamics := make([]model.NodeDynamic, 0, len(nodeMap))
	subNodes := make([]model.SubscriptionNode, 0, len(nodeMap))

	for _, entry := range nodeMap {
		statics = append(statics, model.NodeStatic{
			Hash:        entry.hash,
			RawOptions:  entry.rawOptions,
			CreatedAtNs: nowNs,
		})
		dynamics = append(dynamics, model.NodeDynamic{
			Hash: entry.hash,
		})
		subNodes = append(subNodes, model.SubscriptionNode{
			SubscriptionID: subID,
			NodeHash:       entry.hash,
			Tags:           entry.tags,
			Evicted:        false,
		})
	}

	// --- Step 6: Bulk insert using FlushTx (single transaction) ---
	log.Printf("Writing %d nodes to database in a single transaction...", len(statics))
	start := time.Now()

	if err := engine.FlushTx(state.FlushOps{
		UpsertNodesStatic:       statics,
		UpsertNodesDynamic:      dynamics,
		UpsertSubscriptionNodes: subNodes,
	}); err != nil {
		log.Fatalf("Failed to flush: %v", err)
	}

	elapsed := time.Since(start)
	log.Printf("Done! Wrote %d nodes in %s (%.0f nodes/sec)",
		len(statics), elapsed.Round(time.Millisecond), float64(len(statics))/elapsed.Seconds())
	log.Printf("Subscription ID: %s", subID)
	log.Printf("Subscription Name: %s", subName)
	log.Println("")
	log.Println("Now restart the Resin service to load the imported nodes.")
}
