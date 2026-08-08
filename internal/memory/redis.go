// Copyright (c) 2026 Ruthlessly Practical LLC. All rights reserved.
// Use of this source code is governed by a BSD-3-Clause license
// that can be found in the LICENSE file.

package memory

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/ruthlesslypractical/hippocampus/internal/config"
	"github.com/ruthlesslypractical/hippocampus/internal/embedding"
	"github.com/ruthlesslypractical/hippocampus/internal/util"
)

const (
	entryPrefix = "entry:"
	tagPrefix   = "tag:"
	allTagsKey  = "tags:all"
	timelineKey = "timeline"
	linkPrefix  = "link:"
	ftIndexName = "idx:entries"
	vectorKey   = "hippocampus:vectors"
)

// RedisStore implements Store backed by Redis/Valkey.
type RedisStore struct {
	client   *redis.Client
	embedder *embedding.Embedder
}

// NewLightStore creates a Store that only does basic Put/Get operations.
// It skips FT index creation and embedding setup. Used by the hook for low-latency writes.
func NewLightStore(addr, password string, db int) (*RedisStore, error) {
	client := redis.NewClient(&redis.Options{
		Addr:       addr,
		Password:   password,
		DB:         db,
		MaxRetries: 3,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("connecting to redis/valkey: %w", err)
	}

	return &RedisStore{client: client}, nil
}

// NewRedisStore creates a new Redis/Valkey-backed memory store.
// The embedder is optional — pass nil to disable vector search.
func NewRedisStore(cfg config.RedisConfig, embedder *embedding.Embedder) (*RedisStore, error) {
	opts := &redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		Username: cfg.Username,
		DB:       cfg.DB,
	}

	if cfg.MaxRetries > 0 {
		opts.MaxRetries = cfg.MaxRetries
	} else {
		opts.MaxRetries = 3
	}

	if cfg.DialTimeoutS > 0 {
		opts.DialTimeout = time.Duration(cfg.DialTimeoutS) * time.Second
	} else {
		opts.DialTimeout = 5 * time.Second
	}

	if cfg.PoolSize > 0 {
		opts.PoolSize = cfg.PoolSize
	}

	if cfg.TLS {
		tlsCfg := &tls.Config{
			InsecureSkipVerify: cfg.TLSInsecure,
		}

		if cfg.TLSCert != "" && cfg.TLSKey != "" {
			cert, err := tls.LoadX509KeyPair(cfg.TLSCert, cfg.TLSKey)
			if err != nil {
				return nil, fmt.Errorf("loading TLS client cert: %w", err)
			}
			tlsCfg.Certificates = []tls.Certificate{cert}
		}

		if cfg.TLSCA != "" {
			caCert, err := os.ReadFile(cfg.TLSCA)
			if err != nil {
				return nil, fmt.Errorf("reading CA cert: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(caCert) {
				return nil, fmt.Errorf("failed to parse CA cert")
			}
			tlsCfg.RootCAs = pool
		}

		opts.TLSConfig = tlsCfg
	}

	client := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("connecting to redis/valkey: %w", err)
	}

	store := &RedisStore{client: client, embedder: embedder}
	store.ensureIndex(ctx) // best-effort; works without RediSearch

	return store, nil
}

// ensureIndex creates the RediSearch full-text index if available.
func (s *RedisStore) ensureIndex(ctx context.Context) {
	// Check if index exists
	_, err := s.client.Do(ctx, "FT.INFO", ftIndexName).Result()
	if err == nil {
		return // already exists
	}

	// Create index — silently ignore if RediSearch is not loaded
	s.client.Do(ctx,
		"FT.CREATE", ftIndexName,
		"ON", "HASH",
		"PREFIX", "1", entryPrefix,
		"SCHEMA",
		"content", "TEXT", "WEIGHT", "1.0",
		"tags", "TAG", "SEPARATOR", ",",
		"timestamp", "NUMERIC", "SORTABLE",
	)
}

func (s *RedisStore) Put(ctx context.Context, entry Entry) error {
	key := entryPrefix + entry.ID
	tagsStr := strings.Join(entry.Tags, ",")
	timestamp := entry.Timestamp.Unix()

	// Compute content_hash at write time — the single source of integrity truth.
	// Every write (new or overwrite) gets a fresh hash. FAILED means someone
	// bypassed Put() and wrote directly to Redis.
	contentHash := ComputeContentHash(entry.ID, entry.Content, fmt.Sprintf("%d", timestamp))

	pipe := s.client.Pipeline()

	pipe.HSet(ctx, key, map[string]interface{}{
		"id":           entry.ID,
		"content":      entry.Content,
		"tags":         tagsStr,
		"timestamp":    timestamp,
		"content_hash": contentHash,
	})

	pipe.ZAdd(ctx, timelineKey, redis.Z{
		Score:  float64(entry.Timestamp.Unix()),
		Member: entry.ID,
	})

	for _, tag := range entry.Tags {
		pipe.SAdd(ctx, tagPrefix+tag, entry.ID)
		pipe.SAdd(ctx, allTagsKey, tag)
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return err
	}

	// Vector indexing (best-effort, non-blocking on failure)
	if s.embedder != nil {
		go s.embedAndIndex(entry.ID, entry.Content, tagsStr)
	}

	return nil
}

// embedAndIndex generates an embedding and VADDs it to the vector set.
func (s *RedisStore) embedAndIndex(id, content, tags string) {
	vec, err := s.embedder.Embed(content)
	if err != nil || vec == nil {
		return
	}

	// Convert float32 slice to little-endian binary blob for FP32 format
	blob := float32ToBytes(vec)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// VADD hippocampus:vectors FP32 <blob> <entry_id> SETATTR '{"tags":"..."}'
	attrs := fmt.Sprintf(`{"tags":"%s"}`, strings.ReplaceAll(tags, `"`, `\"`))
	s.client.Do(ctx, "VADD", vectorKey, "FP32", blob, id, "SETATTR", attrs)
}

func (s *RedisStore) Get(ctx context.Context, id string) (Entry, error) {
	key := entryPrefix + id
	data, err := s.client.HGetAll(ctx, key).Result()
	if err != nil {
		return Entry{}, err
	}
	if len(data) == 0 {
		return Entry{}, fmt.Errorf("entry not found: %s", id)
	}
	return entryFromHash(data), nil
}

func (s *RedisStore) Delete(ctx context.Context, id string) error {
	entry, err := s.Get(ctx, id)
	if err != nil {
		return err
	}

	pipe := s.client.Pipeline()
	pipe.Del(ctx, entryPrefix+id)
	pipe.ZRem(ctx, timelineKey, id)
	for _, tag := range entry.Tags {
		pipe.SRem(ctx, tagPrefix+tag, id)
	}
	// Clean up links (both old ZSET and new HASH formats)
	pipe.Del(ctx, linkPrefix+id)
	pipe.Del(ctx, "links:"+id)

	_, err = pipe.Exec(ctx)
	return err
}

func (s *RedisStore) Search(ctx context.Context, query string, limit int) ([]SearchResult, error) {
	if limit <= 0 {
		limit = 20
	}

	var allResults []SearchResult

	// Strategy 1: Semantic search via VSIM (if embedder available)
	if s.embedder != nil {
		vec, err := s.embedder.Embed(query)
		if err == nil && vec != nil {
			blob := float32ToBytes(vec)
			res, err := s.client.Do(ctx,
				"VSIM", vectorKey, "FP32", blob,
				"COUNT", fmt.Sprintf("%d", limit),
				"WITHSCORES",
			).Result()
			if err == nil {
				allResults = append(allResults, parseVSIMResults(res)...)
			}
		}
	}

	// Strategy 2: Full-text keyword search via FT.SEARCH
	res, err := s.client.Do(ctx,
		"FT.SEARCH", ftIndexName, query,
		"LIMIT", "0", fmt.Sprintf("%d", limit),
	).Result()
	if err == nil {
		ftResults, _ := parseSearchResults(res)
		allResults = append(allResults, ftResults...)
	} else if len(allResults) == 0 {
		// Fallback: naive substring scan (only if both VSIM and FT.SEARCH failed)
		return s.searchNaive(ctx, query, limit)
	}

	// Deduplicate (prefer higher score)
	allResults = dedupeSearchResults(allResults)

	// Hydrate entries that only have IDs (from VSIM)
	for i := range allResults {
		if allResults[i].Entry.Content == "" && allResults[i].Entry.ID != "" {
			entry, err := s.Get(ctx, allResults[i].Entry.ID)
			if err == nil {
				allResults[i].Entry = entry
			}
		}
	}

	// Sort by score descending
	for i := 1; i < len(allResults); i++ {
		for j := i; j > 0 && allResults[j].Score > allResults[j-1].Score; j-- {
			allResults[j], allResults[j-1] = allResults[j-1], allResults[j]
		}
	}

	if len(allResults) > limit {
		allResults = allResults[:limit]
	}

	return allResults, nil
}

// SearchWithOptions performs full-text search with optional sort order, tag filters, and time bounds.
// When SortBy is "timestamp_asc" or "timestamp_desc", FT.SEARCH uses SORTBY timestamp.
// FilterTags are applied as @tags:{tag1|tag2...} filter (intersection via multiple clauses).
// After/Before constrain the @timestamp numeric range.
func (s *RedisStore) SearchWithOptions(ctx context.Context, query string, limit int, opts SearchOptions) ([]SearchResult, error) {
	if limit <= 0 {
		limit = 20
	}

	// Build the FT.SEARCH query with filters
	ftQuery := query

	// Add tag filter: each tag becomes its own @tags:{tag} clause (AND logic)
	for _, tag := range opts.FilterTags {
		// Escape special RediSearch characters in tag values
		escaped := util.EscapeRedisTag(tag)
		ftQuery += fmt.Sprintf(" @tags:{%s}", escaped)
	}

	// Add timestamp range filter
	if opts.After > 0 || opts.Before > 0 {
		min := "-inf"
		max := "+inf"
		if opts.After > 0 {
			min = fmt.Sprintf("%d", opts.After)
		}
		if opts.Before > 0 {
			max = fmt.Sprintf("%d", opts.Before)
		}
		ftQuery += fmt.Sprintf(" @timestamp:[%s %s]", min, max)
	}

	// Build FT.SEARCH args
	args := []interface{}{"FT.SEARCH", ftIndexName, ftQuery}

	// Add SORTBY if not relevance
	switch opts.SortBy {
	case "timestamp_asc":
		args = append(args, "SORTBY", "timestamp", "ASC")
	case "timestamp_desc":
		args = append(args, "SORTBY", "timestamp", "DESC")
	}

	args = append(args, "LIMIT", "0", fmt.Sprintf("%d", limit))

	res, err := s.client.Do(ctx, args...).Result()
	if err != nil {
		// Fall back to basic Search if FT.SEARCH fails
		return s.Search(ctx, query, limit)
	}

	results, _ := parseSearchResults(res)

	// Hydrate entries
	for i := range results {
		if results[i].Entry.Content == "" && results[i].Entry.ID != "" {
			entry, err := s.Get(ctx, results[i].Entry.ID)
			if err == nil {
				results[i].Entry = entry
			}
		}
	}

	return results, nil
}

// parseVSIMResults parses the VSIM response into SearchResults.
// RESP2: [element1, score1, element2, score2, ...]
// RESP3: map[string]float64 (element → score)
func parseVSIMResults(raw interface{}) []SearchResult {
	var results []SearchResult

	switch v := raw.(type) {
	case map[interface{}]interface{}:
		// RESP3 format: map of element_id → score
		for key, val := range v {
			id, ok := key.(string)
			if !ok {
				continue
			}
			var score float64
			switch s := val.(type) {
			case float64:
				score = s
			case string:
				fmt.Sscanf(s, "%f", &score)
			}
			results = append(results, SearchResult{
				Entry: Entry{ID: id},
				Score: score,
			})
		}
	case map[string]interface{}:
		// RESP3 alternate form
		for id, val := range v {
			var score float64
			switch s := val.(type) {
			case float64:
				score = s
			case string:
				fmt.Sscanf(s, "%f", &score)
			}
			results = append(results, SearchResult{
				Entry: Entry{ID: id},
				Score: score,
			})
		}
	case []interface{}:
		// RESP2 format: [element1, score1, element2, score2, ...]
		for i := 0; i+1 < len(v); i += 2 {
			id, ok := v[i].(string)
			if !ok {
				continue
			}
			var score float64
			switch s := v[i+1].(type) {
			case string:
				fmt.Sscanf(s, "%f", &score)
			case float64:
				score = s
			}
			results = append(results, SearchResult{
				Entry: Entry{ID: id},
				Score: score,
			})
		}
	}

	return results
}

func dedupeSearchResults(results []SearchResult) []SearchResult {
	seen := make(map[string]int) // id → index in output
	var out []SearchResult

	for _, r := range results {
		id := r.Entry.ID
		if idx, exists := seen[id]; exists {
			// Keep higher score
			if r.Score > out[idx].Score {
				out[idx] = r
			}
		} else {
			seen[id] = len(out)
			out = append(out, r)
		}
	}
	return out
}

func (s *RedisStore) searchNaive(ctx context.Context, query string, limit int) ([]SearchResult, error) {
	var results []SearchResult
	queryLower := strings.ToLower(query)

	iter := s.client.Scan(ctx, 0, entryPrefix+"*", 0).Iterator()
	for iter.Next(ctx) {
		if len(results) >= limit {
			break
		}
		key := iter.Val()
		data, err := s.client.HGetAll(ctx, key).Result()
		if err != nil {
			continue
		}
		content := strings.ToLower(data["content"])
		if strings.Contains(content, queryLower) {
			results = append(results, SearchResult{
				Entry: entryFromHash(data),
				Score: 1.0,
			})
		}
	}
	return results, iter.Err()
}

func (s *RedisStore) ByTags(ctx context.Context, tags []string, limit int, offset int) ([]Entry, error) {
	if len(tags) == 0 {
		return nil, nil
	}

	keys := make([]string, len(tags))
	for i, tag := range tags {
		keys[i] = tagPrefix + tag
	}

	ids, err := s.client.SInter(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	return s.getEntries(ctx, ids, limit, offset)
}

func (s *RedisStore) ByAnyTag(ctx context.Context, tags []string, limit int, offset int) ([]Entry, error) {
	if len(tags) == 0 {
		return nil, nil
	}

	keys := make([]string, len(tags))
	for i, tag := range tags {
		keys[i] = tagPrefix + tag
	}

	ids, err := s.client.SUnion(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	return s.getEntries(ctx, ids, limit, offset)
}

func (s *RedisStore) AddTags(ctx context.Context, id string, tags []string) error {
	entry, err := s.Get(ctx, id)
	if err != nil {
		return err
	}

	newTags := util.Dedupe(append(entry.Tags, tags...))
	pipe := s.client.Pipeline()
	pipe.HSet(ctx, entryPrefix+id, "tags", strings.Join(newTags, ","))
	for _, tag := range tags {
		pipe.SAdd(ctx, tagPrefix+tag, id)
		pipe.SAdd(ctx, allTagsKey, tag)
	}
	_, err = pipe.Exec(ctx)
	return err
}

func (s *RedisStore) RemoveTags(ctx context.Context, id string, tags []string) error {
	entry, err := s.Get(ctx, id)
	if err != nil {
		return err
	}

	removeSet := make(map[string]bool)
	for _, t := range tags {
		removeSet[t] = true
	}

	var remaining []string
	for _, t := range entry.Tags {
		if !removeSet[t] {
			remaining = append(remaining, t)
		}
	}

	pipe := s.client.Pipeline()
	pipe.HSet(ctx, entryPrefix+id, "tags", strings.Join(remaining, ","))
	for _, tag := range tags {
		pipe.SRem(ctx, tagPrefix+tag, id)
	}
	_, err = pipe.Exec(ctx)
	return err
}

func (s *RedisStore) ListTags(ctx context.Context) ([]TagInfo, error) {
	tags, err := s.client.SMembers(ctx, allTagsKey).Result()
	if err != nil {
		return nil, err
	}

	var infos []TagInfo
	for _, tag := range tags {
		count, err := s.client.SCard(ctx, tagPrefix+tag).Result()
		if err != nil {
			continue
		}
		if count > 0 {
			infos = append(infos, TagInfo{Name: tag, Count: int(count)})
		}
	}
	return infos, nil
}

func (s *RedisStore) EntriesByTimeRange(ctx context.Context, start, end int64, tags []string, limit int) ([]Entry, error) {
	if limit <= 0 {
		limit = 20
	}

	ids, err := s.client.ZRevRangeByScore(ctx, timelineKey, &redis.ZRangeBy{
		Min:   fmt.Sprintf("%d", start),
		Max:   fmt.Sprintf("%d", end),
		Count: int64(limit),
	}).Result()
	if err != nil {
		return nil, err
	}

	if len(tags) == 0 {
		return s.getEntries(ctx, ids, limit, 0)
	}

	// Filter by tags
	tagSet := make(map[string]bool)
	for _, tag := range tags {
		tagSet[tag] = true
	}

	var filtered []Entry
	for _, id := range ids {
		entry, err := s.Get(ctx, id)
		if err != nil {
			continue
		}
		if entryHasAllTags(entry, tagSet) {
			filtered = append(filtered, entry)
			if len(filtered) >= limit {
				break
			}
		}
	}
	return filtered, nil
}

// Recent returns the N most recent entries from the timeline (newest first).
func (s *RedisStore) Recent(ctx context.Context, limit int) ([]Entry, error) {
	if limit <= 0 {
		limit = 10
	}

	ids, err := s.client.ZRevRange(ctx, timelineKey, 0, int64(limit-1)).Result()
	if err != nil {
		return nil, err
	}

	return s.getEntries(ctx, ids, limit, 0)
}

// SessionContext returns entries surrounding a given entry within the same session.
// It finds the session tag on the target entry, fetches all session members,
// sorts them chronologically, and returns a window of `before` entries before
// the target and `after` entries after it, plus the target itself.
func (s *RedisStore) SessionContext(ctx context.Context, id string, before, after int) ([]Entry, error) {
	// Get the target entry to find its session tag
	entry, err := s.Get(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("entry not found: %w", err)
	}

	// Extract session tag
	var sessionTag string
	for _, tag := range entry.Tags {
		if strings.HasPrefix(tag, "session:") {
			sessionTag = tag
			break
		}
	}
	if sessionTag == "" {
		return nil, fmt.Errorf("entry %s has no session tag", id)
	}

	// Get all entry IDs in this session
	memberIDs, err := s.client.SMembers(ctx, tagPrefix+sessionTag).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch session members: %w", err)
	}

	if len(memberIDs) == 0 {
		return []Entry{entry}, nil
	}

	// Get timeline scores (timestamps) for all members via pipeline
	pipe := s.client.Pipeline()
	scoreCmds := make(map[string]*redis.FloatCmd, len(memberIDs))
	for _, mid := range memberIDs {
		scoreCmds[mid] = pipe.ZScore(ctx, timelineKey, mid)
	}
	pipe.Exec(ctx)

	// Build sorted list of (id, timestamp)
	type idScore struct {
		id    string
		score float64
	}
	sorted := make([]idScore, 0, len(memberIDs))
	for _, mid := range memberIDs {
		cmd := scoreCmds[mid]
		if cmd.Err() != nil {
			continue
		}
		sorted = append(sorted, idScore{id: mid, score: cmd.Val()})
	}

	// Sort chronologically (ascending)
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && sorted[j].score < sorted[j-1].score; j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}

	// Find target's position
	targetIdx := -1
	for i, s := range sorted {
		if s.id == id {
			targetIdx = i
			break
		}
	}
	if targetIdx == -1 {
		// Target not in timeline but exists — just return it
		return []Entry{entry}, nil
	}

	// Compute window bounds
	start := targetIdx - before
	if start < 0 {
		start = 0
	}
	end := targetIdx + after + 1
	if end > len(sorted) {
		end = len(sorted)
	}

	// Fetch entries in the window
	windowIDs := make([]string, 0, end-start)
	for i := start; i < end; i++ {
		windowIDs = append(windowIDs, sorted[i].id)
	}

	// getEntries fetches but doesn't guarantee order, so fetch and reorder
	entries := make([]Entry, 0, len(windowIDs))
	for _, wid := range windowIDs {
		e, err := s.Get(ctx, wid)
		if err != nil {
			continue
		}
		entries = append(entries, e)
	}

	return entries, nil
}

func (s *RedisStore) Link(ctx context.Context, idA, idB string, score float64, relationType string) error {
	if _, err := s.Get(ctx, idA); err != nil {
		return fmt.Errorf("entry A not found: %w", err)
	}
	if _, err := s.Get(ctx, idB); err != nil {
		return fmt.Errorf("entry B not found: %w", err)
	}

	if relationType == "" {
		relationType = "manual"
	}
	value := fmt.Sprintf("%.4f|%s", score, relationType)

	pipe := s.client.Pipeline()
	pipe.HSet(ctx, "links:"+idA, idB, value)
	pipe.HSet(ctx, "links:"+idB, idA, value)
	_, err := pipe.Exec(ctx)
	return err
}

func (s *RedisStore) Unlink(ctx context.Context, idA, idB string) error {
	pipe := s.client.Pipeline()
	pipe.HDel(ctx, "links:"+idA, idB)
	pipe.HDel(ctx, "links:"+idB, idA)
	// Clean up legacy ZSET keys if they exist
	pipe.ZRem(ctx, linkPrefix+idA, idB)
	pipe.ZRem(ctx, linkPrefix+idB, idA)
	pipe.Del(ctx, linkPrefix+"meta:"+idA+":"+idB)
	pipe.Del(ctx, linkPrefix+"meta:"+idB+":"+idA)
	_, err := pipe.Exec(ctx)
	return err
}

func (s *RedisStore) Links(ctx context.Context, id string) ([]Link, error) {
	// Read from new HASH format
	results, err := s.client.HGetAll(ctx, "links:"+id).Result()
	if err != nil {
		return nil, err
	}

	links := make([]Link, 0, len(results))
	for targetID, value := range results {
		score, relType := parseLinkValue(value)
		links = append(links, Link{
			TargetID:     targetID,
			Score:        score,
			RelationType: relType,
		})
	}

	// Also check legacy ZSET format for backward compat (read-only)
	legacyResults, err := s.client.ZRangeWithScores(ctx, linkPrefix+id, 0, -1).Result()
	if err == nil && len(legacyResults) > 0 {
		for _, z := range legacyResults {
			targetID := z.Member.(string)
			// Skip if already in new format
			alreadyExists := false
			for _, l := range links {
				if l.TargetID == targetID {
					alreadyExists = true
					break
				}
			}
			if alreadyExists {
				continue
			}
			relType, _ := s.client.Get(ctx, linkPrefix+"meta:"+id+":"+targetID).Result()
			if relType == "" {
				relType = "legacy"
			}
			links = append(links, Link{
				TargetID:     targetID,
				Score:        z.Score,
				RelationType: relType,
			})
		}
	}

	sortLinksByAbsScore(links)
	return links, nil
}

// parseLinkValue parses a "score|type" string from the links HASH.
func parseLinkValue(value string) (float64, string) {
	parts := strings.SplitN(value, "|", 2)
	if len(parts) != 2 {
		// Try to parse as bare score (shouldn't happen but defensive)
		var s float64
		fmt.Sscanf(value, "%f", &s)
		return s, "unknown"
	}
	var score float64
	fmt.Sscanf(parts[0], "%f", &score)
	return score, parts[1]
}

func (s *RedisStore) TopLinks(ctx context.Context, id string, n int) ([]Link, error) {
	links, err := s.Links(ctx, id)
	if err != nil {
		return nil, err
	}
	if len(links) > n {
		links = links[:n]
	}
	return links, nil
}

func (s *RedisStore) RenameTag(ctx context.Context, oldTag, newTag string) (int, error) {
	ids, err := s.client.SMembers(ctx, tagPrefix+oldTag).Result()
	if err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, fmt.Errorf("tag not found: %s", oldTag)
	}

	for _, id := range ids {
		key := entryPrefix + id
		tagsStr, err := s.client.HGet(ctx, key, "tags").Result()
		if err != nil {
			continue
		}

		tags := strings.Split(tagsStr, ",")
		var newTags []string
		for _, t := range tags {
			if t == oldTag {
				newTags = append(newTags, newTag)
			} else {
				newTags = append(newTags, t)
			}
		}

		pipe := s.client.Pipeline()
		pipe.HSet(ctx, key, "tags", strings.Join(newTags, ","))
		pipe.SRem(ctx, tagPrefix+oldTag, id)
		pipe.SAdd(ctx, tagPrefix+newTag, id)
		pipe.Exec(ctx)
	}

	s.client.SAdd(ctx, allTagsKey, newTag)
	s.client.SRem(ctx, allTagsKey, oldTag)
	s.client.Del(ctx, tagPrefix+oldTag)

	return len(ids), nil
}

// Client returns the underlying Redis client for direct access.
// Used by callers that need raw Redis commands for reads while using
// Store methods for writes.
func (s *RedisStore) Client() *redis.Client {
	return s.client
}

func (s *RedisStore) Close() error {
	return s.client.Close()
}

// --- helpers ---

func sortLinksByAbsScore(links []Link) {
	for i := 1; i < len(links); i++ {
		for j := i; j > 0; j-- {
			absJ := links[j].Score
			if absJ < 0 {
				absJ = -absJ
			}
			absJm1 := links[j-1].Score
			if absJm1 < 0 {
				absJm1 = -absJm1
			}
			if absJ > absJm1 {
				links[j], links[j-1] = links[j-1], links[j]
			} else {
				break
			}
		}
	}
}

func (s *RedisStore) getEntries(ctx context.Context, ids []string, limit, offset int) ([]Entry, error) {
	if offset >= len(ids) {
		return nil, nil
	}
	end := offset + limit
	if end > len(ids) {
		end = len(ids)
	}
	subset := ids[offset:end]

	var entries []Entry
	for _, id := range subset {
		entry, err := s.Get(ctx, id)
		if err != nil {
			continue
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

func entryFromHash(data map[string]string) Entry {
	var tags []string
	if t := data["tags"]; t != "" {
		tags = strings.Split(t, ",")
	}

	var ts time.Time
	if tsStr := data["timestamp"]; tsStr != "" {
		var unix int64
		fmt.Sscanf(tsStr, "%d", &unix)
		ts = time.Unix(unix, 0)
	}

	return Entry{
		ID:        data["id"],
		Content:   data["content"],
		Tags:      tags,
		Timestamp: ts,
	}
}

func entryHasAllTags(entry Entry, required map[string]bool) bool {
	have := make(map[string]bool)
	for _, t := range entry.Tags {
		have[t] = true
	}
	for tag := range required {
		if !have[tag] {
			return false
		}
	}
	return true
}



func parseSearchResults(raw interface{}) ([]SearchResult, error) {
	var results []SearchResult

	switch v := raw.(type) {
	case map[interface{}]interface{}:
		// RESP3 format: map with "results" key containing array of result maps
		rawResults, ok := v["results"]
		if !ok {
			return nil, nil
		}
		resultArr, ok := rawResults.([]interface{})
		if !ok {
			return nil, nil
		}
		for _, item := range resultArr {
			rm, ok := item.(map[interface{}]interface{})
			if !ok {
				continue
			}
			// Extract fields from extra_attributes
			attrs, ok := rm["extra_attributes"].(map[interface{}]interface{})
			if !ok {
				continue
			}
			data := make(map[string]string)
			for k, val := range attrs {
				ks, _ := k.(string)
				vs, _ := val.(string)
				if ks != "" {
					data[ks] = vs
				}
			}
			results = append(results, SearchResult{
				Entry: entryFromHash(data),
				Score: 1.0,
			})
		}
	case []interface{}:
		// RESP2 format: [total, key1, [field, val, ...], key2, [field, val, ...], ...]
		if len(v) < 1 {
			return nil, nil
		}
		for i := 1; i+1 < len(v); i += 2 {
			fields, ok := v[i+1].([]interface{})
			if !ok {
				continue
			}
			data := make(map[string]string)
			for j := 0; j+1 < len(fields); j += 2 {
				k, _ := fields[j].(string)
				val, _ := fields[j+1].(string)
				data[k] = val
			}
			results = append(results, SearchResult{
				Entry: entryFromHash(data),
				Score: 1.0,
			})
		}
	}

	return results, nil
}

// float32ToBytes converts a float32 slice to a little-endian byte blob for VADD FP32.
func float32ToBytes(vec []float32) []byte {
	buf := make([]byte, len(vec)*4)
	for i, v := range vec {
		bits := math.Float32bits(v)
		binary.LittleEndian.PutUint32(buf[i*4:], bits)
	}
	return buf
}
