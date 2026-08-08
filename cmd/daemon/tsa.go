// Copyright (c) 2026 Ruthlessly Practical LLC. All rights reserved.
// Use of this source code is governed by a BSD-3-Clause license
// that can be found in the LICENSE file.

package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/ruthlesslypractical/hippocampus/internal/config"
)

// runTSALoop periodically batches entries without content_hash/TSA proof and timestamps them.
// Two phases per cycle:
//  1. Hash backfill: compute content_hash for entries that don't have one
//  2. Merkle stamp: batch unhashed entries into blocks, submit root to TSA
func runTSALoop(ctx context.Context, rdb *redis.Client, cfg config.TSAConfig) {
	if !cfg.Enabled {
		slog.Info("TSA timestamping disabled")
		return
	}

	tsaURL := cfg.URL
	if tsaURL == "" {
		tsaURL = "https://rfc3161.ai.moda"
	}
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 256
	}
	intervalS := cfg.IntervalS
	if intervalS <= 0 {
		intervalS = 3600
	}

	// Initial delay to let other startup tasks settle
	time.Sleep(60 * time.Second)

	slog.Info("TSA loop starting", "url", tsaURL, "batch_size", batchSize, "interval_s", intervalS)

	for {
		select {
		case <-ctx.Done():
			slog.Info("TSA loop cancelled")
			return
		default:
		}

		// Phase 1: Hash backfill
		hashed := hashBackfill(ctx, rdb)
		if hashed > 0 {
			slog.Info("TSA hash backfill", "hashed", hashed)
		}

		// Phase 2: Merkle stamp
		stamped := merkleStamp(ctx, rdb, tsaURL, batchSize)
		if stamped > 0 {
			slog.Info("TSA merkle stamp", "entries_stamped", stamped)
		}

		// Sleep until next cycle
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Duration(intervalS) * time.Second):
		}
	}
}

// hashBackfill scans timeline entries and computes content_hash for those missing it.
// Returns count of entries hashed.
func hashBackfill(ctx context.Context, rdb *redis.Client) int {
	// Get all entry IDs from timeline
	ids, err := rdb.ZRange(ctx, "timeline", 0, -1).Result()
	if err != nil {
		slog.Error("TSA hash backfill: failed to read timeline", "error", err)
		return 0
	}

	hashed := 0
	for _, id := range ids {
		select {
		case <-ctx.Done():
			return hashed
		default:
		}

		// Check if already has content_hash
		existing, _ := rdb.HGet(ctx, "entry:"+id, "content_hash").Result()
		if existing != "" {
			continue
		}

		// Get content
		content, err := rdb.HGet(ctx, "entry:"+id, "content").Result()
		if err != nil || content == "" {
			continue
		}

		// Compute SHA-256 of (id + content + timestamp) for uniqueness
		ts, _ := rdb.HGet(ctx, "entry:"+id, "timestamp").Result()
		hashInput := fmt.Sprintf("%s\n%s\n%s", id, content, ts)
		hash := sha256.Sum256([]byte(hashInput))
		hashHex := hex.EncodeToString(hash[:])

		// Store
		rdb.HSet(ctx, "entry:"+id, "content_hash", hashHex)
		hashed++
	}

	return hashed
}

// merkleStamp collects entries with content_hash but without a tsa:* tag,
// batches them into Merkle blocks, and submits the root to the TSA.
func merkleStamp(ctx context.Context, rdb *redis.Client, tsaURL string, batchSize int) int {
	// Find entries with content_hash but no tsa: tag
	// Strategy: scan entries that have content_hash, check if they lack tsa:* tag
	ids, err := rdb.ZRange(ctx, "timeline", 0, -1).Result()
	if err != nil {
		return 0
	}

	var unstamped []string
	for _, id := range ids {
		// Must have content_hash
		hash, _ := rdb.HGet(ctx, "entry:"+id, "content_hash").Result()
		if hash == "" {
			continue
		}

		// Check if already stamped (has any tsa: tag)
		tags, _ := rdb.HGet(ctx, "entry:"+id, "tags").Result()
		if strings.Contains(tags, "tsa:") {
			continue
		}

		unstamped = append(unstamped, id)

		if len(unstamped) >= batchSize {
			break // One batch per cycle
		}
	}

	if len(unstamped) == 0 {
		return 0
	}

	// Collect hashes for the Merkle tree
	leafHashes := make([]string, len(unstamped))
	for i, id := range unstamped {
		hash, _ := rdb.HGet(ctx, "entry:"+id, "content_hash").Result()
		leafHashes[i] = hash
	}

	// Build Merkle tree and get root
	root := buildMerkleRoot(leafHashes)

	// Submit to TSA
	tsr, err := submitToTSA(ctx, tsaURL, root)
	if err != nil {
		slog.Error("TSA submission failed", "error", err, "url", tsaURL)
		return 0
	}

	// Store the TSA block
	blockID := fmt.Sprintf("tsa:%d:%s", time.Now().Unix(), root[:16])

	pipe := rdb.Pipeline()
	pipe.HSet(ctx, "entry:"+blockID, map[string]interface{}{
		"id":           blockID,
		"content":      fmt.Sprintf("Merkle block: %d entries, root=%s", len(unstamped), root),
		"tags":         "tsa,meta",
		"timestamp":    time.Now().Unix(),
		"merkle_root":  root,
		"tsr":          base64.StdEncoding.EncodeToString(tsr),
		"tsa_url":      tsaURL,
		"entry_count":  len(unstamped),
		"entry_ids":    strings.Join(unstamped, ","),
		"leaf_hashes":  strings.Join(leafHashes, ","),
	})
	pipe.ZAdd(ctx, "timeline", redis.Z{Score: float64(time.Now().Unix()), Member: blockID})
	pipe.SAdd(ctx, "tag:tsa", blockID)
	pipe.SAdd(ctx, "tag:meta", blockID)
	pipe.SAdd(ctx, "tags:all", "tsa")

	// Tag each entry with the block ID
	tsaTag := "tsa:" + blockID
	for _, id := range unstamped {
		// Add tsa tag to entry
		existingTags, _ := rdb.HGet(ctx, "entry:"+id, "tags").Result()
		newTags := existingTags
		if newTags != "" {
			newTags += ","
		}
		newTags += tsaTag
		pipe.HSet(ctx, "entry:"+id, "tags", newTags)
		pipe.SAdd(ctx, "tag:"+tsaTag, id)
	}
	pipe.SAdd(ctx, "tags:all", tsaTag)

	if _, err := pipe.Exec(ctx); err != nil {
		slog.Error("TSA block storage failed", "error", err)
		return 0
	}

	slog.Info("TSA block created",
		"block_id", blockID,
		"entries", len(unstamped),
		"root", root[:16]+"...",
	)

	return len(unstamped)
}

// buildMerkleRoot computes the Merkle root from a list of hex-encoded leaf hashes.
func buildMerkleRoot(leaves []string) string {
	if len(leaves) == 0 {
		return ""
	}
	if len(leaves) == 1 {
		return leaves[0]
	}

	// Sort leaves for deterministic ordering
	sorted := make([]string, len(leaves))
	copy(sorted, leaves)
	sort.Strings(sorted)

	// Build tree bottom-up
	level := sorted
	for len(level) > 1 {
		var nextLevel []string
		for i := 0; i < len(level); i += 2 {
			if i+1 < len(level) {
				// Hash pair
				combined := level[i] + level[i+1]
				h := sha256.Sum256([]byte(combined))
				nextLevel = append(nextLevel, hex.EncodeToString(h[:]))
			} else {
				// Odd one out — promote to next level
				nextLevel = append(nextLevel, level[i])
			}
		}
		level = nextLevel
	}

	return level[0]
}

// submitToTSA sends an RFC 3161 TimeStampQuery to the TSA and returns the raw TSR.
// Uses the simplified HTTP approach: POST the hash digest, get back a TSR.
func submitToTSA(ctx context.Context, tsaURL string, merkleRootHex string) ([]byte, error) {
	// Decode the hex hash to raw bytes
	rootBytes, err := hex.DecodeString(merkleRootHex)
	if err != nil {
		return nil, fmt.Errorf("invalid merkle root hex: %w", err)
	}

	// Build ASN.1 TimeStampReq (minimal, SHA-256)
	// This is a simplified construction — just the essential fields.
	tsq := buildTimeStampReq(rootBytes)

	// POST to TSA
	req, err := http.NewRequestWithContext(ctx, "POST", tsaURL, bytes.NewReader(tsq))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/timestamp-query")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("TSA request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("TSA returned %d: %s", resp.StatusCode, string(body))
	}

	tsr, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading TSA response: %w", err)
	}

	if len(tsr) == 0 {
		return nil, fmt.Errorf("empty TSA response")
	}

	return tsr, nil
}

// buildTimeStampReq constructs a minimal ASN.1 DER-encoded RFC 3161 TimeStampReq.
// Structure:
//
//	TimeStampReq ::= SEQUENCE {
//	    version        INTEGER { v1(1) },
//	    messageImprint MessageImprint,
//	    certReq        BOOLEAN DEFAULT FALSE
//	}
//	MessageImprint ::= SEQUENCE {
//	    hashAlgorithm  AlgorithmIdentifier,
//	    hashedMessage  OCTET STRING
//	}
//
// For SHA-256: OID = 2.16.840.1.101.3.4.2.1
func buildTimeStampReq(digest []byte) []byte {
	// SHA-256 AlgorithmIdentifier (DER encoded)
	// SEQUENCE { OID 2.16.840.1.101.3.4.2.1, NULL }
	sha256AlgID := []byte{
		0x30, 0x0D, // SEQUENCE, 13 bytes
		0x06, 0x09, // OID, 9 bytes
		0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x01, // 2.16.840.1.101.3.4.2.1
		0x05, 0x00, // NULL
	}

	// MessageImprint: SEQUENCE { algorithmIdentifier, hashedMessage OCTET STRING }
	hashedMessage := asn1OctetString(digest)
	messageImprint := asn1Sequence(append(sha256AlgID, hashedMessage...))

	// version: INTEGER 1
	version := []byte{0x02, 0x01, 0x01}

	// certReq: BOOLEAN TRUE (request the TSA cert for verification)
	certReq := []byte{0x01, 0x01, 0xFF}

	// TimeStampReq: SEQUENCE { version, messageImprint, certReq }
	body := append(version, messageImprint...)
	body = append(body, certReq...)

	return asn1Sequence(body)
}

func asn1Sequence(content []byte) []byte {
	return asn1Wrap(0x30, content)
}

func asn1OctetString(content []byte) []byte {
	return asn1Wrap(0x04, content)
}

func asn1Wrap(tag byte, content []byte) []byte {
	length := len(content)
	if length < 128 {
		return append([]byte{tag, byte(length)}, content...)
	}
	// Long form length
	if length < 256 {
		return append([]byte{tag, 0x81, byte(length)}, content...)
	}
	return append([]byte{tag, 0x82, byte(length >> 8), byte(length)}, content...)
}
