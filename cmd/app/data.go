// Copyright (c) 2026 Ruthlessly Practical LLC. All rights reserved.
// Use of this source code is governed by a BSD-3-Clause license
// that can be found in the LICENSE file.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	wailsRuntime "github.com/wailsapp/wails/v2/pkg/runtime"

	"github.com/ruthlesslypractical/hippocampus/internal/config"
)

// EntryPreview is a lightweight entry representation for the browser UI.
type EntryPreview struct {
	ID        string   `json:"id"`
	Content   string   `json:"content"` // truncated to ~200 chars
	Tags      []string `json:"tags"`
	Timestamp int64    `json:"timestamp"`
}

// DataStats holds aggregate counts for the data browser header.
type DataStats struct {
	Entries int64 `json:"entries"`
	Tags    int64 `json:"tags"`
	Links   int64 `json:"links"`
}

// GetDataStats returns aggregate counts.
func (a *App) GetDataStats() DataStats {
	if a.redisClient == nil {
		return DataStats{}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	entries, _ := a.redisClient.DBSize(ctx).Result()
	tags, _ := a.redisClient.SCard(ctx, "tags:all").Result()

	// Count link keys (unified HASH format: links:<id>)
	// Use a large COUNT hint to complete the scan within the timeout window.
	var linkCount int64
	iter := a.redisClient.Scan(ctx, 0, "links:*", 10000).Iterator()
	for iter.Next(ctx) {
		linkCount++
	}

	return DataStats{Entries: entries, Tags: tags, Links: linkCount}
}

// BrowseEntries returns a page of entries for infinite scroll.
// Sorted by timestamp descending (newest first).
// filterTag and searchQuery are optional filters.
func (a *App) BrowseEntries(offset, limit int, filterTag, searchQuery string) []EntryPreview {
	if a.redisClient == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var ids []string

	if filterTag != "" {
		// Filter by tag — use a temporary sorted set intersection for efficiency
		// ZINTERSTORE with timeline gives us tag members sorted by timestamp
		tempKey := fmt.Sprintf("_tmp:browse:%d", time.Now().UnixNano())
		a.redisClient.ZInterStore(ctx, tempKey, &redis.ZStore{
			Keys:    []string{"timeline", "tag:" + filterTag},
			Weights: []float64{1, 0}, // only use timeline scores
		})
		a.redisClient.Expire(ctx, tempKey, 30*time.Second) // auto-cleanup

		// Get the page we need (newest first)
		ids, _ = a.redisClient.ZRevRange(ctx, tempKey, int64(offset), int64(offset+limit-1)).Result()
	} else {
		// No tag filter — get from timeline (newest first)
		ids, _ = a.redisClient.ZRevRange(ctx, "timeline", int64(offset), int64(offset+limit-1)).Result()
	}

	var results []EntryPreview
	for _, id := range ids {
		data, err := a.redisClient.HGetAll(ctx, "entry:"+id).Result()
		if err != nil || len(data) == 0 {
			continue
		}

		content := data["content"]
		// Apply search filter
		if searchQuery != "" && !strings.Contains(strings.ToLower(content), strings.ToLower(searchQuery)) {
			continue
		}

		// Truncate content for preview
		if len(content) > 200 {
			content = content[:200] + "…"
		}

		var tags []string
		if t := data["tags"]; t != "" {
			tags = strings.Split(t, ",")
		}

		var ts int64
		fmt.Sscanf(data["timestamp"], "%d", &ts)

		results = append(results, EntryPreview{
			ID:        data["id"],
			Content:   content,
			Tags:      tags,
			Timestamp: ts,
		})
	}

	return results
}

// GetEntry returns a single entry by ID (content truncated for preview).
func (a *App) GetEntry(id string) *EntryPreview {
	if a.redisClient == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	data, err := a.redisClient.HGetAll(ctx, "entry:"+id).Result()
	if err != nil || len(data) == 0 {
		return nil
	}

	content := data["content"]
	if len(content) > 200 {
		content = content[:200] + "…"
	}

	var tags []string
	if t := data["tags"]; t != "" {
		tags = strings.Split(t, ",")
	}

	var ts int64
	fmt.Sscanf(data["timestamp"], "%d", &ts)

	return &EntryPreview{
		ID:        data["id"],
		Content:   content,
		Tags:      tags,
		Timestamp: ts,
	}
}

// GetEntryFull returns the full untruncated content of an entry (for clipboard copy).
func (a *App) GetEntryFull(id string) string {
	if a.redisClient == nil {
		return ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	content, err := a.redisClient.HGet(ctx, "entry:"+id, "content").Result()
	if err != nil {
		return ""
	}
	return content
}

// DeleteEntry removes a single entry by ID.
func (a *App) DeleteEntry(id string) error {
	if a.redisClient == nil {
		return fmt.Errorf("not connected to Redis")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	key := "entry:" + id

	// Get current tags to remove from tag sets
	tagsStr, _ := a.redisClient.HGet(ctx, key, "tags").Result()

	pipe := a.redisClient.Pipeline()
	pipe.Del(ctx, key)
	pipe.ZRem(ctx, "timeline", id)

	if tagsStr != "" {
		for _, tag := range strings.Split(tagsStr, ",") {
			tag = strings.TrimSpace(tag)
			if tag != "" {
				pipe.SRem(ctx, "tag:"+tag, id)
			}
		}
	}

	// Remove links
	pipe.Del(ctx, "link:"+id)

	_, err := pipe.Exec(ctx)
	return err
}

// SetEntryTags replaces all tags on an entry.
func (a *App) SetEntryTags(id string, tags []string) error {
	if a.redisClient == nil {
		return fmt.Errorf("not connected to Redis")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	key := "entry:" + id

	// Get old tags to remove from sets
	oldTagsStr, _ := a.redisClient.HGet(ctx, key, "tags").Result()

	pipe := a.redisClient.Pipeline()

	// Remove from old tag sets
	var oldTags []string
	if oldTagsStr != "" {
		oldTags = strings.Split(oldTagsStr, ",")
		for _, tag := range oldTags {
			tag = strings.TrimSpace(tag)
			if tag != "" {
				pipe.SRem(ctx, "tag:"+tag, id)
			}
		}
	}

	// Set new tags
	newTagsStr := strings.Join(tags, ",")
	pipe.HSet(ctx, key, "tags", newTagsStr)

	// Add to new tag sets
	for _, tag := range tags {
		tag = strings.TrimSpace(tag)
		if tag != "" {
			pipe.SAdd(ctx, "tag:"+tag, id)
			pipe.SAdd(ctx, "tags:all", tag)
		}
	}

	pipe.Exec(ctx)

	// Clean up: remove tags from tags:all if their set is now empty
	for _, tag := range oldTags {
		tag = strings.TrimSpace(tag)
		if tag == "" {
			continue
		}
		// Check if this tag was removed (not in new set)
		found := false
		for _, nt := range tags {
			if strings.TrimSpace(nt) == tag {
				found = true
				break
			}
		}
		if !found {
			count, _ := a.redisClient.SCard(ctx, "tag:"+tag).Result()
			if count == 0 {
				a.redisClient.SRem(ctx, "tags:all", tag)
			}
		}
	}

	return nil
}

// GetAllTags returns all tags with their entry counts.
func (a *App) GetAllTags() []map[string]interface{} {
	if a.redisClient == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	allTags, err := a.redisClient.SMembers(ctx, "tags:all").Result()
	if err != nil {
		return nil
	}

	var results []map[string]interface{}
	for _, tag := range allTags {
		count, _ := a.redisClient.SCard(ctx, "tag:"+tag).Result()
		results = append(results, map[string]interface{}{
			"name":  tag,
			"count": count,
		})
	}
	return results
}

// CreateTag registers a new tag in the global registry.
// Returns an error if the tag already exists.
func (a *App) CreateTag(tag string) error {
	if a.redisClient == nil {
		return fmt.Errorf("not connected to Redis")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Check if already exists
	exists, err := a.redisClient.SIsMember(ctx, "tags:all", tag).Result()
	if err != nil {
		return fmt.Errorf("checking tag: %w", err)
	}
	if exists {
		return fmt.Errorf("tag already exists")
	}

	// Add to global registry
	return a.redisClient.SAdd(ctx, "tags:all", tag).Err()
}

// DeleteTag removes a tag from all entries and the global registry.
func (a *App) DeleteTag(tag string) error {
	if a.redisClient == nil {
		return fmt.Errorf("not connected to Redis")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Get all entries with this tag
	members, err := a.redisClient.SMembers(ctx, "tag:"+tag).Result()
	if err != nil {
		return err
	}

	pipe := a.redisClient.Pipeline()
	for _, id := range members {
		// Remove tag from each entry's tag string
		tagsStr, _ := a.redisClient.HGet(ctx, "entry:"+id, "tags").Result()
		var newTags []string
		for _, t := range strings.Split(tagsStr, ",") {
			t = strings.TrimSpace(t)
			if t != "" && t != tag {
				newTags = append(newTags, t)
			}
		}
		pipe.HSet(ctx, "entry:"+id, "tags", strings.Join(newTags, ","))
	}

	// Remove the tag set and from global registry
	pipe.Del(ctx, "tag:"+tag)
	pipe.SRem(ctx, "tags:all", tag)

	_, err = pipe.Exec(ctx)
	return err
}

// Reset removed — FLUSHDB is not something that should be one click away.
// If you need to nuke the database, you know where redis-cli is.

// Backup exports all memory entries as a JSON file.
// Pops up a native save dialog for the user to choose location and filename.
func (a *App) Backup() error {
	if a.redisClient == nil {
		return fmt.Errorf("not connected to Redis")
	}

	// Show save dialog
	savePath, err := wailsRuntime.SaveFileDialog(a.ctx, wailsRuntime.SaveDialogOptions{
		Title:           "Export Hippocampus Memory",
		DefaultFilename: "hippocampus-backup-" + time.Now().Format("2006-01-02") + ".json",
		Filters: []wailsRuntime.FileFilter{
			{DisplayName: "JSON Files", Pattern: "*.json"},
		},
	})
	if err != nil {
		return fmt.Errorf("save dialog: %w", err)
	}
	if savePath == "" {
		return nil // User cancelled
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Export all entries as JSON
	type exportEntry struct {
		ID        string `json:"id"`
		Content   string `json:"content"`
		Tags      string `json:"tags"`
		Timestamp string `json:"timestamp"`
	}

	var entries []exportEntry

	iter := a.redisClient.Scan(ctx, 0, "entry:*", 0).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()
		data, err := a.redisClient.HGetAll(ctx, key).Result()
		if err != nil || len(data) == 0 {
			continue
		}
		entries = append(entries, exportEntry{
			ID:        data["id"],
			Content:   data["content"],
			Tags:      data["tags"],
			Timestamp: data["timestamp"],
		})
	}
	if err := iter.Err(); err != nil {
		return fmt.Errorf("scanning entries: %w", err)
	}

	// Export links
	type exportLink struct {
		From  string  `json:"from"`
		To    string  `json:"to"`
		Score float64 `json:"score"`
	}
	var links []exportLink

	linkIter := a.redisClient.Scan(ctx, 0, "link:*", 0).Iterator()
	for linkIter.Next(ctx) {
		key := linkIter.Val()
		fromID := strings.TrimPrefix(key, "link:")
		members, err := a.redisClient.ZRangeWithScores(ctx, key, 0, -1).Result()
		if err != nil {
			continue
		}
		for _, m := range members {
			links = append(links, exportLink{
				From:  fromID,
				To:    m.Member.(string),
				Score: m.Score,
			})
		}
	}

	export := struct {
		Version    string        `json:"version"`
		ExportedAt string        `json:"exported_at"`
		EntryCount int           `json:"entry_count"`
		LinkCount  int           `json:"link_count"`
		Entries    []exportEntry `json:"entries"`
		Links      []exportLink  `json:"links"`
	}{
		Version:    config.Version,
		ExportedAt: time.Now().Format(time.RFC3339),
		EntryCount: len(entries),
		LinkCount:  len(links),
		Entries:    entries,
		Links:      links,
	}

	data, err := json.MarshalIndent(export, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling export: %w", err)
	}

	if err := os.WriteFile(savePath, data, 0o644); err != nil {
		return fmt.Errorf("writing backup: %w", err)
	}

	return nil
}

// Restore imports entries and links from a JSON backup file.
// Pops up a native open dialog for the user to select the file.
func (a *App) Restore() (string, error) {
	if a.redisClient == nil {
		return "", fmt.Errorf("not connected to Redis")
	}

	// Show open dialog
	openPath, err := wailsRuntime.OpenFileDialog(a.ctx, wailsRuntime.OpenDialogOptions{
		Title: "Import Hippocampus Backup",
		Filters: []wailsRuntime.FileFilter{
			{DisplayName: "JSON Files", Pattern: "*.json"},
		},
	})
	if err != nil {
		return "", fmt.Errorf("open dialog: %w", err)
	}
	if openPath == "" {
		return "", nil // User cancelled
	}

	data, err := os.ReadFile(openPath)
	if err != nil {
		return "", fmt.Errorf("reading file: %w", err)
	}

	// Parse the export
	type exportEntry struct {
		ID        string `json:"id"`
		Content   string `json:"content"`
		Tags      string `json:"tags"`
		Timestamp string `json:"timestamp"`
	}
	type exportLink struct {
		From  string  `json:"from"`
		To    string  `json:"to"`
		Score float64 `json:"score"`
	}
	var export struct {
		Version    string        `json:"version"`
		EntryCount int           `json:"entry_count"`
		LinkCount  int           `json:"link_count"`
		Entries    []exportEntry `json:"entries"`
		Links      []exportLink  `json:"links"`
	}

	if err := json.Unmarshal(data, &export); err != nil {
		return "", fmt.Errorf("parsing backup file: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// Import entries
	imported := 0
	for _, e := range export.Entries {
		if e.ID == "" {
			continue
		}
		key := "entry:" + e.ID
		pipe := a.redisClient.Pipeline()
		pipe.HSet(ctx, key, map[string]interface{}{
			"id":        e.ID,
			"content":   e.Content,
			"tags":      e.Tags,
			"timestamp": e.Timestamp,
		})

		// Add to timeline
		var ts float64
		fmt.Sscanf(e.Timestamp, "%f", &ts)
		if ts == 0 {
			ts = float64(time.Now().Unix())
		}
		pipe.ZAdd(ctx, "timeline", redis.Z{Score: ts, Member: e.ID})

		// Add to tag sets
		if e.Tags != "" {
			for _, tag := range strings.Split(e.Tags, ",") {
				tag = strings.TrimSpace(tag)
				if tag != "" {
					pipe.SAdd(ctx, "tag:"+tag, e.ID)
					pipe.SAdd(ctx, "tags:all", tag)
				}
			}
		}

		if _, err := pipe.Exec(ctx); err != nil {
			continue
		}
		imported++
	}

	// Import links
	linksImported := 0
	for _, l := range export.Links {
		if l.From == "" || l.To == "" {
			continue
		}
		value := fmt.Sprintf("%.4f|%s", l.Score, "imported")
		a.redisClient.HSet(ctx, "links:"+l.From, l.To, value)
		a.redisClient.HSet(ctx, "links:"+l.To, l.From, value)
		linksImported++
	}

	return fmt.Sprintf("Imported %d entries, %d links from %s", imported, linksImported, filepath.Base(openPath)), nil
}
