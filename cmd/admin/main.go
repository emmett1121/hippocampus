// Copyright (c) 2026 Ruthlessly Practical LLC. All rights reserved.
// Use of this source code is governed by a BSD-3-Clause license
// that can be found in the LICENSE file.

package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/ruthlesslypractical/hippocampus/internal/config"
)

const (
	entryPrefix = "entry:"
	tagPrefix   = "tag:"
	allTagsKey  = "tags:all"
	timelineKey = "timeline"
	linkPrefix  = "link:"
)

var helpText = `hippocampus-admin — CLI tool for managing Hippocampus memory

Usage:
  hippocampus-admin <verb> <noun> [flags/args]

Commands:
  entry show <id>              Print full entry (content, tags, timestamp, links)
  entry list [filters]         List entries matching filter
  entry tag <id> add <tag>     Add a tag to an entry
  entry tag <id> remove <tag>  Remove a tag from an entry
  entry tag <id> promote       Promote track_auto:X → track:X on entry
  entry edit <id>              Open entry in $EDITOR as JSON, apply changes
  entry delete <id>            Delete entry from hash + timeline + all tag sets
  entry search <query>         Full-text search

  tag list                     List all tags with member counts
  tag rename <old> <new>       Rename tag across all entries
  tag delete <tag>             Remove tag from all entries + delete set key
  tag orphans                  List tags with 0 members
  tag promote --track X        Bulk promote track_auto:X → track:X

  epistemic stats              Show counts per status
  epistemic purge              Delete pruned entries
  epistemic nuke               Wipe ALL epistemic:* keys
  epistemic export             Dump verified+contested claims

  summary list                 List all summary entries
  summary wipe                 Delete all summary entries

  orientation list             List all orientation entries (meta:orientation:*)
  orientation show <id>        Print full orientation content
  orientation add <id>         Create new orientation ($EDITOR with template)
  orientation add <id> --file  Store file content as orientation
  orientation edit <id>        Edit existing orientation in $EDITOR
  orientation delete <id>      Delete orientation (with confirmation)

Global Flags:
  --config <path>              Config file path override
  --help                       Show this help

Entry List Flags:
  --all                        Dump everything from timeline
  --unclassified               No track: or track_auto: tag
  --confused                   Has classified:confused tag
  --track X                    Has track:X or track_auto:X
  --session X                  Has session:X tag
  --recent N                   Last N entries (default 20 if no filter)
  --limit N                    Max results
  --offset N                   Skip first N results
  --output human|json|csv      Output format (default: human)
  --output-file path           Write to file instead of stdout
`

func main() {
	args := os.Args[1:]
	if len(args) == 0 || contains(args, "--help") || contains(args, "-h") {
		fmt.Print(helpText)
		os.Exit(0)
	}
	if contains(args, "--version") || contains(args, "-v") {
		fmt.Printf("hippocampus-admin v%s\n", config.Version)
		os.Exit(0)
	}

	// Extract --config flag
	configPath := ""
	args = extractFlag(args, "--config", &configPath)

	// Load config
	if configPath == "" {
		configPath = config.FindConfigPath()
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		fatalf("Failed to load config from %s: %v\n", configPath, err)
	}

	// Connect to Redis
	rdb := cfg.Redis.NewRedisClient()
	defer rdb.Close()

	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		fatalf("Failed to connect to Redis at %s: %v\n", cfg.Redis.Addr, err)
	}

	if len(args) < 2 {
		fmt.Print(helpText)
		os.Exit(1)
	}

	verb := args[0]
	noun := args[1]
	rest := args[2:]

	switch verb {
	case "entry":
		handleEntry(ctx, rdb, noun, rest)
	case "tag":
		handleTag(ctx, rdb, noun, rest)
	case "epistemic":
		handleEpistemic(ctx, rdb, noun, rest)
	case "summary":
		handleSummary(ctx, rdb, noun, rest)
	case "orientation":
		handleOrientation(ctx, rdb, noun, rest)
	default:
		fatalf("Unknown verb: %s\n", verb)
	}
}

// --- Utility functions ---

func fatalf(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format, args...)
	os.Exit(1)
}

func contains(args []string, flag string) bool {
	for _, a := range args {
		if a == flag {
			return true
		}
	}
	return false
}

func extractFlag(args []string, flag string, value *string) []string {
	var out []string
	for i := 0; i < len(args); i++ {
		if args[i] == flag && i+1 < len(args) {
			*value = args[i+1]
			i++ // skip value
		} else {
			out = append(out, args[i])
		}
	}
	return out
}

func extractFlagBool(args []string, flag string) ([]string, bool) {
	var out []string
	found := false
	for _, a := range args {
		if a == flag {
			found = true
		} else {
			out = append(out, a)
		}
	}
	return out, found
}

func extractFlagInt(args []string, flag string, value *int) []string {
	var out []string
	for i := 0; i < len(args); i++ {
		if args[i] == flag && i+1 < len(args) {
			v, err := strconv.Atoi(args[i+1])
			if err == nil {
				*value = v
			}
			i++
		} else {
			out = append(out, args[i])
		}
	}
	return out
}

func extractFlagStr(args []string, flag string, value *string) []string {
	return extractFlag(args, flag, value)
}

func confirm(prompt string) bool {
	fmt.Printf("%s [y/N] ", prompt)
	reader := bufio.NewReader(os.Stdin)
	line, _ := reader.ReadString('\n')
	line = strings.TrimSpace(strings.ToLower(line))
	return line == "y" || line == "yes"
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

func getWriter(outputFile string) (io.Writer, func()) {
	if outputFile == "" {
		return os.Stdout, func() {}
	}
	f, err := os.Create(outputFile)
	if err != nil {
		fatalf("Cannot open output file %s: %v\n", outputFile, err)
	}
	return f, func() { f.Close() }
}

// --- Entry type for admin operations ---

type adminEntry struct {
	ID        string   `json:"id"`
	Content   string   `json:"content"`
	Tags      []string `json:"tags"`
	Timestamp int64    `json:"timestamp"`
}

func fetchEntry(ctx context.Context, rdb *redis.Client, id string) (adminEntry, error) {
	data, err := rdb.HGetAll(ctx, entryPrefix+id).Result()
	if err != nil {
		return adminEntry{}, err
	}
	if len(data) == 0 {
		return adminEntry{}, fmt.Errorf("entry not found: %s", id)
	}
	var tags []string
	if t := data["tags"]; t != "" {
		tags = strings.Split(t, ",")
	}
	ts, _ := strconv.ParseInt(data["timestamp"], 10, 64)
	return adminEntry{
		ID:        data["id"],
		Content:   data["content"],
		Tags:      tags,
		Timestamp: ts,
	}, nil
}

type adminLink struct {
	TargetID     string  `json:"target_id"`
	Score        float64 `json:"score"`
	RelationType string  `json:"relation_type,omitempty"`
}

func fetchLinks(ctx context.Context, rdb *redis.Client, id string) []adminLink {
	results, err := rdb.ZRangeWithScores(ctx, linkPrefix+id, 0, -1).Result()
	if err != nil {
		return nil
	}
	links := make([]adminLink, 0, len(results))
	for _, z := range results {
		targetID := z.Member.(string)
		relType, _ := rdb.Get(ctx, linkPrefix+"meta:"+id+":"+targetID).Result()
		links = append(links, adminLink{
			TargetID:     targetID,
			Score:        z.Score,
			RelationType: relType,
		})
	}
	return links
}

// =============================================================================
// ENTRY commands
// =============================================================================

func handleEntry(ctx context.Context, rdb *redis.Client, noun string, args []string) {
	switch noun {
	case "show":
		entryShow(ctx, rdb, args)
	case "list":
		entryList(ctx, rdb, args)
	case "tag":
		entryTag(ctx, rdb, args)
	case "edit":
		entryEdit(ctx, rdb, args)
	case "delete":
		entryDelete(ctx, rdb, args)
	case "search":
		entrySearch(ctx, rdb, args)
	default:
		fatalf("Unknown entry command: %s\n", noun)
	}
}

func entryShow(ctx context.Context, rdb *redis.Client, args []string) {
	output := "human"
	args = extractFlagStr(args, "--output", &output)

	if len(args) == 0 {
		fatalf("Usage: entry show <id> [--output human|json|csv]\n")
	}
	id := args[0]

	entry, err := fetchEntry(ctx, rdb, id)
	if err != nil {
		fatalf("Error: %v\n", err)
	}
	links := fetchLinks(ctx, rdb, id)

	switch output {
	case "json":
		obj := map[string]interface{}{
			"id":        entry.ID,
			"content":   entry.Content,
			"tags":      entry.Tags,
			"timestamp": entry.Timestamp,
			"time":      time.Unix(entry.Timestamp, 0).Format(time.RFC3339),
			"links":     links,
		}
		data, _ := json.MarshalIndent(obj, "", "  ")
		fmt.Println(string(data))
	case "csv":
		w := csv.NewWriter(os.Stdout)
		w.Write([]string{"id", "content", "tags", "timestamp", "links"})
		linksJSON, _ := json.Marshal(links)
		w.Write([]string{entry.ID, entry.Content, strings.Join(entry.Tags, ";"), fmt.Sprintf("%d", entry.Timestamp), string(linksJSON)})
		w.Flush()
	default: // human
		fmt.Printf("ID:        %s\n", entry.ID)
		fmt.Printf("Timestamp: %s (%d)\n", time.Unix(entry.Timestamp, 0).Format(time.RFC3339), entry.Timestamp)
		fmt.Printf("Tags:      %s\n", strings.Join(entry.Tags, ", "))
		fmt.Printf("Content:\n%s\n", entry.Content)
		if len(links) > 0 {
			fmt.Printf("\nLinks (%d):\n", len(links))
			for _, l := range links {
				rel := ""
				if l.RelationType != "" {
					rel = fmt.Sprintf(" [%s]", l.RelationType)
				}
				fmt.Printf("  → %s (score: %.2f)%s\n", l.TargetID, l.Score, rel)
			}
		}
	}
}

func entryList(ctx context.Context, rdb *redis.Client, args []string) {
	output := "human"
	outputFile := ""
	trackFilter := ""
	sessionFilter := ""
	recent := 0
	limit := 0
	offset := 0

	args = extractFlagStr(args, "--output", &output)
	args = extractFlagStr(args, "--output-file", &outputFile)
	args = extractFlagStr(args, "--track", &trackFilter)
	args = extractFlagStr(args, "--session", &sessionFilter)
	args = extractFlagInt(args, "--recent", &recent)
	args = extractFlagInt(args, "--limit", &limit)
	args = extractFlagInt(args, "--offset", &offset)

	var allFlag, unclassifiedFlag, confusedFlag bool
	args, allFlag = extractFlagBool(args, "--all")
	args, unclassifiedFlag = extractFlagBool(args, "--unclassified")
	args, confusedFlag = extractFlagBool(args, "--confused")

	// Determine which IDs to fetch
	var ids []string

	if confusedFlag {
		members, err := rdb.SMembers(ctx, tagPrefix+"classified:confused").Result()
		if err != nil {
			fatalf("Error fetching confused entries: %v\n", err)
		}
		ids = members
	} else if sessionFilter != "" {
		members, err := rdb.SMembers(ctx, tagPrefix+"session:"+sessionFilter).Result()
		if err != nil {
			fatalf("Error fetching session entries: %v\n", err)
		}
		ids = members
	} else if trackFilter != "" {
		// Union of track:X and track_auto:X
		set1, _ := rdb.SMembers(ctx, tagPrefix+"track:"+trackFilter).Result()
		set2, _ := rdb.SMembers(ctx, tagPrefix+"track_auto:"+trackFilter).Result()
		seen := make(map[string]bool)
		for _, id := range append(set1, set2...) {
			if !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		}
	} else if allFlag {
		members, err := rdb.ZRevRange(ctx, timelineKey, 0, -1).Result()
		if err != nil {
			fatalf("Error fetching timeline: %v\n", err)
		}
		ids = members
	} else {
		// Default: recent N from timeline
		n := recent
		if n <= 0 {
			n = 20
		}
		members, err := rdb.ZRevRange(ctx, timelineKey, 0, int64(n-1)).Result()
		if err != nil {
			fatalf("Error fetching timeline: %v\n", err)
		}
		ids = members
	}

	// Fetch entries
	var entries []adminEntry
	for _, id := range ids {
		entry, err := fetchEntry(ctx, rdb, id)
		if err != nil {
			continue
		}

		// Apply unclassified filter
		if unclassifiedFlag {
			hasTrack := false
			for _, t := range entry.Tags {
				if strings.HasPrefix(t, "track:") || strings.HasPrefix(t, "track_auto:") {
					hasTrack = true
					break
				}
			}
			if hasTrack {
				continue
			}
		}

		entries = append(entries, entry)
	}

	// Apply offset and limit
	if offset > 0 {
		if offset >= len(entries) {
			entries = nil
		} else {
			entries = entries[offset:]
		}
	}
	if limit > 0 && limit < len(entries) {
		entries = entries[:limit]
	}

	// Output
	w, cleanup := getWriter(outputFile)
	defer cleanup()

	switch output {
	case "json":
		for _, e := range entries {
			data, _ := json.Marshal(map[string]interface{}{
				"id":        e.ID,
				"content":   e.Content,
				"tags":      e.Tags,
				"timestamp": e.Timestamp,
			})
			fmt.Fprintln(w, string(data))
		}
	case "csv":
		cw := csv.NewWriter(w)
		cw.Write([]string{"id", "timestamp", "tags", "content"})
		for _, e := range entries {
			cw.Write([]string{e.ID, fmt.Sprintf("%d", e.Timestamp), strings.Join(e.Tags, ";"), e.Content})
		}
		cw.Flush()
	default: // human
		fmt.Fprintf(w, "Found %d entries\n\n", len(entries))
		for _, e := range entries {
			ts := time.Unix(e.Timestamp, 0).Format("2006-01-02 15:04")
			fmt.Fprintf(w, "[%s] %s\n", ts, e.ID)
			fmt.Fprintf(w, "  Tags: %s\n", strings.Join(e.Tags, ", "))
			fmt.Fprintf(w, "  %s\n\n", truncate(e.Content, 200))
		}
	}
}

func entryTag(ctx context.Context, rdb *redis.Client, args []string) {
	if len(args) < 2 {
		fatalf("Usage: entry tag <id> <add|remove|promote> [tag]\n")
	}
	id := args[0]
	action := args[1]

	// Verify entry exists
	entry, err := fetchEntry(ctx, rdb, id)
	if err != nil {
		fatalf("Error: %v\n", err)
	}

	switch action {
	case "add":
		if len(args) < 3 {
			fatalf("Usage: entry tag <id> add <tag>\n")
		}
		tag := args[2]
		pipe := rdb.Pipeline()
		newTags := append(entry.Tags, tag)
		pipe.HSet(ctx, entryPrefix+id, "tags", strings.Join(newTags, ","))
		pipe.SAdd(ctx, tagPrefix+tag, id)
		pipe.SAdd(ctx, allTagsKey, tag)
		if _, err := pipe.Exec(ctx); err != nil {
			fatalf("Error adding tag: %v\n", err)
		}
		fmt.Printf("Added tag '%s' to %s\n", tag, id)

	case "remove":
		if len(args) < 3 {
			fatalf("Usage: entry tag <id> remove <tag>\n")
		}
		tag := args[2]
		var remaining []string
		for _, t := range entry.Tags {
			if t != tag {
				remaining = append(remaining, t)
			}
		}
		pipe := rdb.Pipeline()
		pipe.HSet(ctx, entryPrefix+id, "tags", strings.Join(remaining, ","))
		pipe.SRem(ctx, tagPrefix+tag, id)
		if _, err := pipe.Exec(ctx); err != nil {
			fatalf("Error removing tag: %v\n", err)
		}
		fmt.Printf("Removed tag '%s' from %s\n", tag, id)

	case "promote":
		promoted := 0
		var newTags []string
		pipe := rdb.Pipeline()
		for _, t := range entry.Tags {
			if strings.HasPrefix(t, "track_auto:") {
				track := strings.TrimPrefix(t, "track_auto:")
				newTag := "track:" + track
				newTags = append(newTags, newTag)
				pipe.SRem(ctx, tagPrefix+t, id)
				pipe.SAdd(ctx, tagPrefix+newTag, id)
				pipe.SAdd(ctx, allTagsKey, newTag)
				promoted++
			} else {
				newTags = append(newTags, t)
			}
		}
		if promoted == 0 {
			fmt.Printf("No track_auto: tags to promote on %s\n", id)
			return
		}
		pipe.HSet(ctx, entryPrefix+id, "tags", strings.Join(newTags, ","))
		if _, err := pipe.Exec(ctx); err != nil {
			fatalf("Error promoting tags: %v\n", err)
		}
		fmt.Printf("Promoted %d track_auto: → track: on %s\n", promoted, id)

	default:
		fatalf("Unknown tag action: %s (expected add, remove, promote)\n", action)
	}
}

func entryEdit(ctx context.Context, rdb *redis.Client, args []string) {
	batchStr := ""
	args = extractFlagStr(args, "--batch", &batchStr)

	var ids []string
	if batchStr != "" {
		ids = strings.Split(batchStr, ",")
	} else {
		if len(args) == 0 {
			fatalf("Usage: entry edit <id> [--batch id1,id2,id3]\n")
		}
		ids = []string{args[0]}
	}

	// Fetch entries
	var entries []adminEntry
	for _, id := range ids {
		entry, err := fetchEntry(ctx, rdb, id)
		if err != nil {
			fatalf("Error fetching %s: %v\n", id, err)
		}
		entries = append(entries, entry)
	}

	// Write to temp file
	var original []byte
	if len(entries) == 1 {
		original, _ = json.MarshalIndent(entries[0], "", "  ")
	} else {
		original, _ = json.MarshalIndent(entries, "", "  ")
	}

	tmpFile, err := os.CreateTemp("", "hippocampus-edit-*.json")
	if err != nil {
		fatalf("Error creating temp file: %v\n", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	if _, err := tmpFile.Write(original); err != nil {
		fatalf("Error writing temp file: %v\n", err)
	}
	tmpFile.Close()

	// Open editor
	editor := os.Getenv("EDITOR")
	if editor == "" {
		editor = os.Getenv("VISUAL")
	}
	if editor == "" {
		editor = "vi"
	}

	cmd := exec.Command(editor, tmpPath)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fatalf("Editor exited with error: %v\n", err)
	}

	// Read back edited content
	edited, err := os.ReadFile(tmpPath)
	if err != nil {
		fatalf("Error reading edited file: %v\n", err)
	}

	if string(edited) == string(original) {
		fmt.Println("No changes detected.")
		return
	}

	// Parse edited content
	var editedEntries []adminEntry
	if len(entries) == 1 {
		var single adminEntry
		if err := json.Unmarshal(edited, &single); err != nil {
			fatalf("Error parsing edited JSON: %v\n", err)
		}
		editedEntries = []adminEntry{single}
	} else {
		if err := json.Unmarshal(edited, &editedEntries); err != nil {
			fatalf("Error parsing edited JSON array: %v\n", err)
		}
	}

	// Show diff summary
	fmt.Println("Changes detected:")
	for i, orig := range entries {
		if i >= len(editedEntries) {
			break
		}
		ed := editedEntries[i]
		if orig.Content != ed.Content {
			fmt.Printf("  [%s] content changed\n", orig.ID)
		}
		if strings.Join(orig.Tags, ",") != strings.Join(ed.Tags, ",") {
			fmt.Printf("  [%s] tags: %v → %v\n", orig.ID, orig.Tags, ed.Tags)
		}
	}

	if !confirm("Apply?") {
		fmt.Println("Aborted.")
		return
	}

	// Apply changes
	for i, orig := range entries {
		if i >= len(editedEntries) {
			break
		}
		ed := editedEntries[i]
		pipe := rdb.Pipeline()

		// Update hash fields
		pipe.HSet(ctx, entryPrefix+orig.ID, map[string]interface{}{
			"content": ed.Content,
			"tags":    strings.Join(ed.Tags, ","),
		})

		// Fix tag set memberships
		oldTags := make(map[string]bool)
		for _, t := range orig.Tags {
			oldTags[t] = true
		}
		newTags := make(map[string]bool)
		for _, t := range ed.Tags {
			newTags[t] = true
		}

		// Remove from old tag sets
		for t := range oldTags {
			if !newTags[t] {
				pipe.SRem(ctx, tagPrefix+t, orig.ID)
			}
		}
		// Add to new tag sets
		for t := range newTags {
			if !oldTags[t] {
				pipe.SAdd(ctx, tagPrefix+t, orig.ID)
				pipe.SAdd(ctx, allTagsKey, t)
			}
		}

		if _, err := pipe.Exec(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "Error updating %s: %v\n", orig.ID, err)
		} else {
			fmt.Printf("Updated %s\n", orig.ID)
		}
	}
}

func entryDelete(ctx context.Context, rdb *redis.Client, args []string) {
	if len(args) == 0 {
		fatalf("Usage: entry delete <id>\n")
	}
	id := args[0]

	entry, err := fetchEntry(ctx, rdb, id)
	if err != nil {
		fatalf("Error: %v\n", err)
	}

	fmt.Printf("Deleting entry %s\n", id)
	fmt.Printf("  Content: %s\n", truncate(entry.Content, 100))
	fmt.Printf("  Tags: %s\n", strings.Join(entry.Tags, ", "))

	if !confirm("Delete?") {
		fmt.Println("Aborted.")
		return
	}

	pipe := rdb.Pipeline()
	pipe.Del(ctx, entryPrefix+id)
	pipe.ZRem(ctx, timelineKey, id)
	for _, tag := range entry.Tags {
		pipe.SRem(ctx, tagPrefix+tag, id)
	}
	pipe.Del(ctx, linkPrefix+id)
	pipe.Do(ctx, "VREM", "hippocampus:vectors", id) // the embedding goes with the entry

	if _, err := pipe.Exec(ctx); err != nil {
		fatalf("Error deleting: %v\n", err)
	}
	fmt.Printf("Deleted %s\n", id)
}

func entrySearch(ctx context.Context, rdb *redis.Client, args []string) {
	output := "human"
	limit := 20
	args = extractFlagStr(args, "--output", &output)
	args = extractFlagInt(args, "--limit", &limit)

	if len(args) == 0 {
		fatalf("Usage: entry search <query> [--limit N] [--output human|json|csv]\n")
	}
	query := strings.Join(args, " ")

	// Try FT.SEARCH first
	var entries []adminEntry
	res, err := rdb.Do(ctx, "FT.SEARCH", "idx:entries", query, "LIMIT", "0", fmt.Sprintf("%d", limit)).Result()
	if err == nil {
		entries = parseFTSearchResults(res)
	} else {
		// Fallback: naive SCAN + substring match
		entries = naiveSearch(ctx, rdb, query, limit)
	}

	switch output {
	case "json":
		for _, e := range entries {
			data, _ := json.Marshal(e)
			fmt.Println(string(data))
		}
	case "csv":
		w := csv.NewWriter(os.Stdout)
		w.Write([]string{"id", "timestamp", "tags", "content"})
		for _, e := range entries {
			w.Write([]string{e.ID, fmt.Sprintf("%d", e.Timestamp), strings.Join(e.Tags, ";"), e.Content})
		}
		w.Flush()
	default:
		fmt.Printf("Found %d results\n\n", len(entries))
		for _, e := range entries {
			ts := time.Unix(e.Timestamp, 0).Format("2006-01-02 15:04")
			fmt.Printf("[%s] %s\n", ts, e.ID)
			fmt.Printf("  Tags: %s\n", strings.Join(e.Tags, ", "))
			fmt.Printf("  %s\n\n", truncate(e.Content, 200))
		}
	}
}

func parseFTSearchResults(raw interface{}) []adminEntry {
	arr, ok := raw.([]interface{})
	if !ok || len(arr) < 1 {
		return nil
	}

	var entries []adminEntry
	// FT.SEARCH returns: [total_results, key1, [field1, val1, ...], key2, [field2, val2, ...], ...]
	for i := 1; i+1 < len(arr); i += 2 {
		fields, ok := arr[i+1].([]interface{})
		if !ok {
			continue
		}
		data := make(map[string]string)
		for j := 0; j+1 < len(fields); j += 2 {
			k, _ := fields[j].(string)
			v, _ := fields[j+1].(string)
			data[k] = v
		}
		var tags []string
		if t := data["tags"]; t != "" {
			tags = strings.Split(t, ",")
		}
		ts, _ := strconv.ParseInt(data["timestamp"], 10, 64)
		entries = append(entries, adminEntry{
			ID:        data["id"],
			Content:   data["content"],
			Tags:      tags,
			Timestamp: ts,
		})
	}
	return entries
}

func naiveSearch(ctx context.Context, rdb *redis.Client, query string, limit int) []adminEntry {
	var entries []adminEntry
	queryLower := strings.ToLower(query)

	iter := rdb.Scan(ctx, 0, entryPrefix+"*", 0).Iterator()
	for iter.Next(ctx) {
		if len(entries) >= limit {
			break
		}
		key := iter.Val()
		data, err := rdb.HGetAll(ctx, key).Result()
		if err != nil {
			continue
		}
		if strings.Contains(strings.ToLower(data["content"]), queryLower) {
			var tags []string
			if t := data["tags"]; t != "" {
				tags = strings.Split(t, ",")
			}
			ts, _ := strconv.ParseInt(data["timestamp"], 10, 64)
			entries = append(entries, adminEntry{
				ID:        data["id"],
				Content:   data["content"],
				Tags:      tags,
				Timestamp: ts,
			})
		}
	}
	return entries
}

// =============================================================================
// TAG commands
// =============================================================================

func handleTag(ctx context.Context, rdb *redis.Client, noun string, args []string) {
	switch noun {
	case "list":
		tagList(ctx, rdb)
	case "rename":
		tagRename(ctx, rdb, args)
	case "delete":
		tagDelete(ctx, rdb, args)
	case "orphans":
		tagOrphans(ctx, rdb)
	case "promote":
		tagPromote(ctx, rdb, args)
	default:
		fatalf("Unknown tag command: %s\n", noun)
	}
}

func tagList(ctx context.Context, rdb *redis.Client) {
	tags, err := rdb.SMembers(ctx, allTagsKey).Result()
	if err != nil {
		fatalf("Error listing tags: %v\n", err)
	}

	type tagCount struct {
		Name  string
		Count int64
	}
	var infos []tagCount
	for _, tag := range tags {
		count, err := rdb.SCard(ctx, tagPrefix+tag).Result()
		if err != nil {
			continue
		}
		infos = append(infos, tagCount{Name: tag, Count: count})
	}

	// Sort by count descending
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].Count > infos[j].Count
	})

	fmt.Printf("%-60s %s\n", "TAG", "COUNT")
	fmt.Println(strings.Repeat("-", 70))
	for _, info := range infos {
		fmt.Printf("%-60s %d\n", info.Name, info.Count)
	}
	fmt.Printf("\nTotal: %d tags\n", len(infos))
}

func tagRename(ctx context.Context, rdb *redis.Client, args []string) {
	if len(args) < 2 {
		fatalf("Usage: tag rename <old> <new>\n")
	}
	oldTag := args[0]
	newTag := args[1]

	ids, err := rdb.SMembers(ctx, tagPrefix+oldTag).Result()
	if err != nil || len(ids) == 0 {
		fatalf("Tag not found or empty: %s\n", oldTag)
	}

	fmt.Printf("Renaming '%s' → '%s' across %d entries\n", oldTag, newTag, len(ids))
	if !confirm("Proceed?") {
		fmt.Println("Aborted.")
		return
	}

	for _, id := range ids {
		tagsStr, err := rdb.HGet(ctx, entryPrefix+id, "tags").Result()
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
		pipe := rdb.Pipeline()
		pipe.HSet(ctx, entryPrefix+id, "tags", strings.Join(newTags, ","))
		pipe.SRem(ctx, tagPrefix+oldTag, id)
		pipe.SAdd(ctx, tagPrefix+newTag, id)
		pipe.Exec(ctx)
	}

	rdb.SAdd(ctx, allTagsKey, newTag)
	rdb.SRem(ctx, allTagsKey, oldTag)
	rdb.Del(ctx, tagPrefix+oldTag)

	fmt.Printf("Renamed '%s' → '%s' (%d entries updated)\n", oldTag, newTag, len(ids))
}

func tagDelete(ctx context.Context, rdb *redis.Client, args []string) {
	if len(args) == 0 {
		fatalf("Usage: tag delete <tag>\n")
	}
	tag := args[0]

	ids, err := rdb.SMembers(ctx, tagPrefix+tag).Result()
	if err != nil {
		fatalf("Error: %v\n", err)
	}

	fmt.Printf("Deleting tag '%s' from %d entries\n", tag, len(ids))
	if !confirm("Proceed?") {
		fmt.Println("Aborted.")
		return
	}

	for _, id := range ids {
		tagsStr, err := rdb.HGet(ctx, entryPrefix+id, "tags").Result()
		if err != nil {
			continue
		}
		tags := strings.Split(tagsStr, ",")
		var remaining []string
		for _, t := range tags {
			if t != tag {
				remaining = append(remaining, t)
			}
		}
		rdb.HSet(ctx, entryPrefix+id, "tags", strings.Join(remaining, ","))
	}

	rdb.Del(ctx, tagPrefix+tag)
	rdb.SRem(ctx, allTagsKey, tag)

	fmt.Printf("Deleted tag '%s' (%d entries updated)\n", tag, len(ids))
}

func tagOrphans(ctx context.Context, rdb *redis.Client) {
	tags, err := rdb.SMembers(ctx, allTagsKey).Result()
	if err != nil {
		fatalf("Error listing tags: %v\n", err)
	}

	var orphans []string
	for _, tag := range tags {
		count, err := rdb.SCard(ctx, tagPrefix+tag).Result()
		if err != nil {
			continue
		}
		if count == 0 {
			orphans = append(orphans, tag)
		}
	}

	if len(orphans) == 0 {
		fmt.Println("No orphan tags found.")
		return
	}

	sort.Strings(orphans)
	fmt.Printf("Orphan tags (%d):\n", len(orphans))
	for _, t := range orphans {
		fmt.Printf("  %s\n", t)
	}
}

func tagPromote(ctx context.Context, rdb *redis.Client, args []string) {
	track := ""
	args = extractFlagStr(args, "--track", &track)
	if track == "" {
		fatalf("Usage: tag promote --track <name>\n")
	}

	autoTag := "track_auto:" + track
	newTag := "track:" + track

	ids, err := rdb.SMembers(ctx, tagPrefix+autoTag).Result()
	if err != nil || len(ids) == 0 {
		fatalf("No entries found with tag '%s'\n", autoTag)
	}

	fmt.Printf("Promoting '%s' → '%s' on %d entries\n", autoTag, newTag, len(ids))
	if !confirm("Proceed?") {
		fmt.Println("Aborted.")
		return
	}

	promoted := 0
	for _, id := range ids {
		tagsStr, err := rdb.HGet(ctx, entryPrefix+id, "tags").Result()
		if err != nil {
			continue
		}
		tags := strings.Split(tagsStr, ",")
		var newTags []string
		for _, t := range tags {
			if t == autoTag {
				newTags = append(newTags, newTag)
			} else {
				newTags = append(newTags, t)
			}
		}
		pipe := rdb.Pipeline()
		pipe.HSet(ctx, entryPrefix+id, "tags", strings.Join(newTags, ","))
		pipe.SRem(ctx, tagPrefix+autoTag, id)
		pipe.SAdd(ctx, tagPrefix+newTag, id)
		pipe.SAdd(ctx, allTagsKey, newTag)
		pipe.Exec(ctx)
		promoted++
	}

	rdb.Del(ctx, tagPrefix+autoTag)
	rdb.SRem(ctx, allTagsKey, autoTag)

	fmt.Printf("Promoted %d entries from '%s' → '%s'\n", promoted, autoTag, newTag)
}

// =============================================================================
// EPISTEMIC commands
// =============================================================================

func handleEpistemic(ctx context.Context, rdb *redis.Client, noun string, args []string) {
	switch noun {
	case "stats":
		epistemicStats(ctx, rdb)
	case "purge":
		epistemicPurge(ctx, rdb)
	case "nuke":
		epistemicNuke(ctx, rdb)
	case "export":
		epistemicExport(ctx, rdb, args)
	default:
		fatalf("Unknown epistemic command: %s\n", noun)
	}
}

func epistemicStats(ctx context.Context, rdb *redis.Client) {
	statuses := []string{"unknown", "verified", "contested", "false", "pruned"}
	fmt.Printf("%-12s %s\n", "STATUS", "COUNT")
	fmt.Println(strings.Repeat("-", 25))
	total := int64(0)
	for _, s := range statuses {
		count, err := rdb.SCard(ctx, "epistemic:status:"+s).Result()
		if err != nil {
			count = 0
		}
		fmt.Printf("%-12s %d\n", s, count)
		total += count
	}
	fmt.Println(strings.Repeat("-", 25))
	fmt.Printf("%-12s %d\n", "TOTAL", total)
}

func epistemicPurge(ctx context.Context, rdb *redis.Client) {
	canonicals, err := rdb.SMembers(ctx, "epistemic:status:pruned").Result()
	if err != nil {
		fatalf("Error: %v\n", err)
	}
	if len(canonicals) == 0 {
		fmt.Println("No pruned entries to purge.")
		return
	}

	fmt.Printf("Will purge %d pruned epistemic entries.\n", len(canonicals))
	if !confirm("Proceed?") {
		fmt.Println("Aborted.")
		return
	}

	purged := 0
	for _, canonical := range canonicals {
		key := "epistemic:" + canonical
		vals, err := rdb.HGetAll(ctx, key).Result()
		if err != nil || len(vals) == 0 {
			rdb.SRem(ctx, "epistemic:status:pruned", canonical)
			purged++
			continue
		}

		subject := vals["subject"]
		object := vals["object"]

		pipe := rdb.Pipeline()
		pipe.Del(ctx, key)
		if subject != "" {
			pipe.SRem(ctx, "epistemic:by_subject:"+subject, canonical)
		}
		if object != "" {
			pipe.SRem(ctx, "epistemic:by_object:"+object, canonical)
		}
		pipe.SRem(ctx, "epistemic:status:pruned", canonical)
		if _, err := pipe.Exec(ctx); err != nil {
			continue
		}
		purged++
	}

	fmt.Printf("Purged %d epistemic entries.\n", purged)
}

func epistemicNuke(ctx context.Context, rdb *redis.Client) {
	fmt.Println("WARNING: This will delete ALL epistemic:* keys from Redis.")
	fmt.Println("This includes all tracked claims, status sets, and subject/object indices.")
	if !confirm("Are you absolutely sure?") {
		fmt.Println("Aborted.")
		return
	}

	var cursor uint64
	deleted := 0
	for {
		keys, next, err := rdb.Scan(ctx, cursor, "epistemic:*", 100).Result()
		if err != nil {
			fatalf("Error scanning: %v\n", err)
		}
		if len(keys) > 0 {
			rdb.Del(ctx, keys...)
			deleted += len(keys)
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}

	fmt.Printf("Nuked %d epistemic keys.\n", deleted)
}

func epistemicExport(ctx context.Context, rdb *redis.Client, args []string) {
	output := "json"
	outputFile := ""
	args = extractFlagStr(args, "--output", &output)
	args = extractFlagStr(args, "--output-file", &outputFile)

	// Gather verified + contested entries
	var allEntries []map[string]interface{}
	for _, status := range []string{"verified", "contested"} {
		canonicals, err := rdb.SMembers(ctx, "epistemic:status:"+status).Result()
		if err != nil {
			continue
		}
		for _, canonical := range canonicals {
			vals, err := rdb.HGetAll(ctx, "epistemic:"+canonical).Result()
			if err != nil || len(vals) == 0 {
				continue
			}
			confidence, _ := strconv.ParseFloat(vals["confidence"], 64)
			encounterCount, _ := strconv.Atoi(vals["encounter_count"])
			firstSeen, _ := strconv.ParseInt(vals["first_seen"], 10, 64)
			lastSeen, _ := strconv.ParseInt(vals["last_seen"], 10, 64)

			entry := map[string]interface{}{
				"canonical":        canonical,
				"subject":          vals["subject"],
				"verb":             vals["verb"],
				"object":           vals["object"],
				"status":           vals["status"],
				"confidence":       confidence,
				"encounter_count":  encounterCount,
				"first_seen":       time.Unix(firstSeen, 0).Format(time.RFC3339),
				"last_seen":        time.Unix(lastSeen, 0).Format(time.RFC3339),
				"evidence_for":     vals["evidence_for"],
				"evidence_against": vals["evidence_against"],
				"verified_by":      vals["verified_by"],
			}
			allEntries = append(allEntries, entry)
		}
	}

	w, cleanup := getWriter(outputFile)
	defer cleanup()

	switch output {
	case "csv":
		cw := csv.NewWriter(w)
		cw.Write([]string{"canonical", "subject", "verb", "object", "status", "confidence", "encounter_count", "first_seen", "last_seen", "evidence_for", "evidence_against"})
		for _, e := range allEntries {
			cw.Write([]string{
				fmt.Sprint(e["canonical"]),
				fmt.Sprint(e["subject"]),
				fmt.Sprint(e["verb"]),
				fmt.Sprint(e["object"]),
				fmt.Sprint(e["status"]),
				fmt.Sprintf("%.3f", e["confidence"]),
				fmt.Sprint(e["encounter_count"]),
				fmt.Sprint(e["first_seen"]),
				fmt.Sprint(e["last_seen"]),
				fmt.Sprint(e["evidence_for"]),
				fmt.Sprint(e["evidence_against"]),
			})
		}
		cw.Flush()
	default: // json
		for _, e := range allEntries {
			data, _ := json.Marshal(e)
			fmt.Fprintln(w, string(data))
		}
	}

	if outputFile != "" {
		fmt.Fprintf(os.Stderr, "Exported %d entries to %s\n", len(allEntries), outputFile)
	}
}

// =============================================================================
// SUMMARY commands
// =============================================================================

func handleSummary(ctx context.Context, rdb *redis.Client, noun string, args []string) {
	switch noun {
	case "list":
		summaryList(ctx, rdb, args)
	case "show":
		summaryShow(ctx, rdb, args)
	case "edit":
		summaryEdit(ctx, rdb, args)
	case "delete":
		summaryDelete(ctx, rdb, args)
	case "wipe":
		summaryWipe(ctx, rdb, args)
	default:
		fatalf("Unknown summary command: %s\nUsage: summary [list|show|edit|delete|wipe]\n", noun)
	}
}

func summaryList(ctx context.Context, rdb *redis.Client, args []string) {
	trackFilter := ""
	args = extractFlagStr(args, "--track", &trackFilter)

	// Summary entries are identified by having a summary: tag prefix
	// Gather from tags:all, find all summary:* tags
	allTags, err := rdb.SMembers(ctx, allTagsKey).Result()
	if err != nil {
		fatalf("Error: %v\n", err)
	}

	var summaryTags []string
	for _, tag := range allTags {
		if strings.HasPrefix(tag, "summary:") && tag != "summary:comprehensive" {
			if trackFilter != "" {
				// Filter: only summary tags for the specified track
				if strings.Contains(tag, trackFilter) {
					summaryTags = append(summaryTags, tag)
				}
			} else {
				summaryTags = append(summaryTags, tag)
			}
		}
	}

	// Gather all unique entry IDs with any summary tag
	seen := make(map[string]bool)
	var ids []string
	for _, tag := range summaryTags {
		members, err := rdb.SMembers(ctx, tagPrefix+tag).Result()
		if err != nil {
			continue
		}
		for _, id := range members {
			if !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		}
	}

	fmt.Printf("%-40s %-20s %-20s %s\n", "ID", "TRACK", "TIMESTAMP", "SIZE")
	fmt.Println(strings.Repeat("-", 90))

	for _, id := range ids {
		entry, err := fetchEntry(ctx, rdb, id)
		if err != nil {
			continue
		}

		// If track filter, verify entry matches
		if trackFilter != "" {
			hasTrack := false
			for _, t := range entry.Tags {
				if strings.Contains(t, trackFilter) {
					hasTrack = true
					break
				}
			}
			if !hasTrack {
				continue
			}
		}

		// Extract track from tags
		track := ""
		for _, t := range entry.Tags {
			if strings.HasPrefix(t, "summary:daily:") {
				track = strings.TrimPrefix(t, "summary:daily:")
				break
			}
			if strings.HasPrefix(t, "summary:weekly:") {
				track = strings.TrimPrefix(t, "summary:weekly:")
				break
			}
			if strings.HasPrefix(t, "track:") {
				track = strings.TrimPrefix(t, "track:")
			}
		}

		ts := time.Unix(entry.Timestamp, 0).Format("2006-01-02 15:04")
		fmt.Printf("%-40s %-20s %-20s %d chars\n", truncate(id, 38), track, ts, len(entry.Content))
	}

	fmt.Printf("\nTotal: %d summary entries\n", len(ids))
}

func summaryShow(ctx context.Context, rdb *redis.Client, args []string) {
	if len(args) == 0 {
		fatalf("Usage: summary show <id>\n")
	}
	id := args[0]

	entry, err := fetchEntry(ctx, rdb, id)
	if err != nil {
		fatalf("Error: %v\n", err)
	}

	fmt.Printf("ID:        %s\n", entry.ID)
	fmt.Printf("Tags:      %s\n", strings.Join(entry.Tags, ", "))
	fmt.Printf("Timestamp: %s\n", time.Unix(entry.Timestamp, 0).Format(time.RFC3339))
	fmt.Printf("Size:      %d chars\n", len(entry.Content))
	fmt.Println(strings.Repeat("-", 60))
	fmt.Println(entry.Content)
}

func summaryEdit(ctx context.Context, rdb *redis.Client, args []string) {
	if len(args) == 0 {
		fatalf("Usage: summary edit <id>\n")
	}
	id := args[0]

	entry, err := fetchEntry(ctx, rdb, id)
	if err != nil {
		fatalf("Error: %v\n", err)
	}

	// Write current content to temp file
	tmpFile, err := os.CreateTemp("", "hippocampus-summary-*.md")
	if err != nil {
		fatalf("Error creating temp file: %v\n", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	if _, err := tmpFile.WriteString(entry.Content); err != nil {
		fatalf("Error writing temp file: %v\n", err)
	}
	tmpFile.Close()

	editor := os.Getenv("EDITOR")
	if editor == "" {
		editor = os.Getenv("VISUAL")
	}
	if editor == "" {
		editor = "vi"
	}

	cmd := exec.Command(editor, tmpPath)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fatalf("Editor exited with error: %v\n", err)
	}

	data, err := os.ReadFile(tmpPath)
	if err != nil {
		fatalf("Error reading edited file: %v\n", err)
	}
	content := strings.TrimSpace(string(data))

	if content == strings.TrimSpace(entry.Content) {
		fmt.Println("No changes detected.")
		return
	}

	if content == "" {
		fmt.Println("Empty content. Aborted.")
		return
	}

	// Update entry content and timestamp
	now := time.Now().Unix()
	pipe := rdb.Pipeline()
	pipe.HSet(ctx, entryPrefix+id, map[string]interface{}{
		"content":   content,
		"timestamp": now,
	})
	pipe.ZAdd(ctx, timelineKey, redis.Z{Score: float64(now), Member: id})

	if _, err := pipe.Exec(ctx); err != nil {
		fatalf("Error updating summary: %v\n", err)
	}

	fmt.Printf("Updated %s (%d chars)\n", id, len(content))
}

func summaryDelete(ctx context.Context, rdb *redis.Client, args []string) {
	if len(args) == 0 {
		fatalf("Usage: summary delete <id>\n")
	}
	id := args[0]

	entry, err := fetchEntry(ctx, rdb, id)
	if err != nil {
		fatalf("Error: %v\n", err)
	}

	// Show what we're about to delete
	fmt.Printf("ID:   %s\n", entry.ID)
	fmt.Printf("Tags: %s\n", strings.Join(entry.Tags, ", "))
	fmt.Printf("Size: %d chars\n", len(entry.Content))
	snippet := entry.Content
	if len(snippet) > 120 {
		snippet = snippet[:120] + "…"
	}
	fmt.Printf("Content: %s\n", snippet)

	if !confirm("Delete this summary entry?") {
		fmt.Println("Aborted.")
		return
	}

	pipe := rdb.Pipeline()

	// Remove from timeline
	pipe.ZRem(ctx, timelineKey, id)

	// Remove from all tag sets
	for _, tag := range entry.Tags {
		pipe.SRem(ctx, tagPrefix+tag, id)
	}

	// Delete the entry hash
	pipe.Del(ctx, entryPrefix+id)

	// Remove any links referencing this entry
	pipe.Del(ctx, "links:"+id)
	pipe.Do(ctx, "VREM", "hippocampus:vectors", id) // and its embedding

	if _, err := pipe.Exec(ctx); err != nil {
		fatalf("Error deleting summary: %v\n", err)
	}

	fmt.Printf("Deleted %s\n", id)
}

func summaryWipe(ctx context.Context, rdb *redis.Client, args []string) {
	trackFilter := ""
	args = extractFlagStr(args, "--track", &trackFilter)

	// Gather summary entry IDs
	allTags, err := rdb.SMembers(ctx, allTagsKey).Result()
	if err != nil {
		fatalf("Error: %v\n", err)
	}

	var summaryTags []string
	for _, tag := range allTags {
		if strings.HasPrefix(tag, "summary:") && tag != "summary:comprehensive" {
			summaryTags = append(summaryTags, tag)
		}
	}

	seen := make(map[string]bool)
	var ids []string
	for _, tag := range summaryTags {
		members, err := rdb.SMembers(ctx, tagPrefix+tag).Result()
		if err != nil {
			continue
		}
		for _, id := range members {
			if !seen[id] {
				seen[id] = true
				ids = append(ids, id)
			}
		}
	}

	// Filter by track if specified
	if trackFilter != "" {
		var filtered []string
		for _, id := range ids {
			entry, err := fetchEntry(ctx, rdb, id)
			if err != nil {
				continue
			}
			for _, t := range entry.Tags {
				if strings.Contains(t, trackFilter) {
					filtered = append(filtered, id)
					break
				}
			}
		}
		ids = filtered
	}

	if len(ids) == 0 {
		fmt.Println("No summary entries to wipe.")
		return
	}

	scope := "all"
	if trackFilter != "" {
		scope = "track:" + trackFilter
	}
	fmt.Printf("Will delete %d summary entries (scope: %s)\n", len(ids), scope)
	if !confirm("Proceed?") {
		fmt.Println("Aborted.")
		return
	}

	deleted := 0
	for _, id := range ids {
		entry, err := fetchEntry(ctx, rdb, id)
		if err != nil {
			continue
		}

		pipe := rdb.Pipeline()
		pipe.Del(ctx, entryPrefix+id)
		pipe.ZRem(ctx, timelineKey, id)
		for _, tag := range entry.Tags {
			pipe.SRem(ctx, tagPrefix+tag, id)
		}
		pipe.Del(ctx, linkPrefix+id)
		pipe.Do(ctx, "VREM", "hippocampus:vectors", id) // the embedding goes with the entry
		if _, err := pipe.Exec(ctx); err != nil {
			continue
		}
		deleted++
	}

	fmt.Printf("Wiped %d summary entries.\n", deleted)
}

// =============================================================================
// ORIENTATION commands
// =============================================================================

const orientationPrefix = "meta:orientation:"

const orientationTemplate = `## Track: <Name>

**Nature:** <software | research | analysis | concept | infrastructure | hybrid>

**Artifacts:**
- <where the primary stuff lives — repos, directories, documents, URLs>
- <secondary locations if applicable>

**Interfaces:**
- <how to build/run/deploy/interact>
- <key commands, endpoints, or tools>

**Vocabulary:**
- <term>: <what it means in this track's context>

**Notes:**
- <anything invariant that trips you up without context>
`

func handleOrientation(ctx context.Context, rdb *redis.Client, noun string, args []string) {
	switch noun {
	case "list":
		orientationList(ctx, rdb, args)
	case "show":
		orientationShow(ctx, rdb, args)
	case "add":
		orientationAdd(ctx, rdb, args)
	case "edit":
		orientationEdit(ctx, rdb, args)
	case "delete":
		orientationDelete(ctx, rdb, args)
	default:
		fatalf("Unknown orientation command: %s\nUsage: orientation list|show|add|edit|delete\n", noun)
	}
}

func validateOrientationID(id string) error {
	if !strings.HasPrefix(id, orientationPrefix) {
		return fmt.Errorf("ID must start with %q (got %q)", orientationPrefix, id)
	}
	suffix := strings.TrimPrefix(id, orientationPrefix)
	if suffix == "" {
		return fmt.Errorf("ID must have a suffix after %q", orientationPrefix)
	}
	return nil
}

func orientationTags(id string) []string {
	tags := []string{"meta", "orientation"}
	// Extract track name if it's a track orientation
	if strings.HasPrefix(id, orientationPrefix+"track:") {
		trackName := strings.TrimPrefix(id, orientationPrefix+"track:")
		if trackName != "" {
			tags = append(tags, "track:"+trackName)
		}
	}
	return tags
}

func orientationList(ctx context.Context, rdb *redis.Client, args []string) {
	// Find all entries with both "meta" and "orientation" tags
	metaMembers, err := rdb.SMembers(ctx, tagPrefix+"meta").Result()
	if err != nil {
		fatalf("Error reading meta tag set: %v\n", err)
	}
	orientMembers, err := rdb.SMembers(ctx, tagPrefix+"orientation").Result()
	if err != nil {
		fatalf("Error reading orientation tag set: %v\n", err)
	}

	// Intersect
	orientSet := make(map[string]bool)
	for _, id := range orientMembers {
		orientSet[id] = true
	}

	var ids []string
	for _, id := range metaMembers {
		if orientSet[id] {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)

	if len(ids) == 0 {
		fmt.Println("No orientation entries found.")
		return
	}

	fmt.Printf("%-50s %-15s %-20s %s\n", "REDIS KEY", "TRACK", "UPDATED", "SIZE")
	fmt.Println(strings.Repeat("-", 100))

	for _, id := range ids {
		entry, err := fetchEntry(ctx, rdb, id)
		if err != nil {
			continue
		}
		track := ""
		for _, t := range entry.Tags {
			if strings.HasPrefix(t, "track:") {
				track = strings.TrimPrefix(t, "track:")
				break
			}
		}
		// Display the full Redis key (entry:<id>) so there's no ambiguity
		redisKey := entryPrefix + id
		ts := time.Unix(entry.Timestamp, 0).Format("2006-01-02 15:04")
		fmt.Printf("%-50s %-15s %-20s %d chars\n", truncate(redisKey, 48), track, ts, len(entry.Content))
	}

	fmt.Printf("\nTotal: %d orientation entries\n", len(ids))
}

func orientationShow(ctx context.Context, rdb *redis.Client, args []string) {
	if len(args) == 0 {
		fatalf("Usage: orientation show <id>\n")
	}
	id := args[0]

	entry, err := fetchEntry(ctx, rdb, id)
	if err != nil {
		fatalf("Error: %v\n", err)
	}

	fmt.Printf("ID:        %s\n", entry.ID)
	fmt.Printf("Tags:      %s\n", strings.Join(entry.Tags, ", "))
	fmt.Printf("Updated:   %s\n", time.Unix(entry.Timestamp, 0).Format(time.RFC3339))
	fmt.Printf("Size:      %d chars\n", len(entry.Content))
	fmt.Println(strings.Repeat("-", 60))
	fmt.Println(entry.Content)
}

func orientationAdd(ctx context.Context, rdb *redis.Client, args []string) {
	filePath := ""
	args = extractFlagStr(args, "--file", &filePath)

	if len(args) == 0 {
		fatalf("Usage: orientation add <id> [--file <path>]\n  ID must be: meta:orientation:track:<Name> or meta:orientation:<other>\n")
	}
	id := args[0]

	if err := validateOrientationID(id); err != nil {
		fatalf("Invalid ID: %v\n", err)
	}

	// Check if already exists
	exists, _ := rdb.Exists(ctx, entryPrefix+id).Result()
	if exists > 0 {
		fatalf("Entry %q already exists. Use 'orientation edit' instead.\n", id)
	}

	var content string

	if filePath != "" {
		// Read from file
		data, err := os.ReadFile(filePath)
		if err != nil {
			fatalf("Error reading file %s: %v\n", filePath, err)
		}
		content = string(data)
	} else {
		// Open $EDITOR with template pre-filled
		trackName := ""
		if strings.HasPrefix(id, orientationPrefix+"track:") {
			trackName = strings.TrimPrefix(id, orientationPrefix+"track:")
		}
		tmpl := orientationTemplate
		if trackName != "" {
			tmpl = strings.Replace(tmpl, "<Name>", trackName, 1)
		}

		tmpFile, err := os.CreateTemp("", "hippocampus-orientation-*.md")
		if err != nil {
			fatalf("Error creating temp file: %v\n", err)
		}
		tmpPath := tmpFile.Name()
		defer os.Remove(tmpPath)

		if _, err := tmpFile.WriteString(tmpl); err != nil {
			fatalf("Error writing temp file: %v\n", err)
		}
		tmpFile.Close()

		editor := os.Getenv("EDITOR")
		if editor == "" {
			editor = os.Getenv("VISUAL")
		}
		if editor == "" {
			editor = "vi"
		}

		cmd := exec.Command(editor, tmpPath)
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			fatalf("Editor exited with error: %v\n", err)
		}

		data, err := os.ReadFile(tmpPath)
		if err != nil {
			fatalf("Error reading edited file: %v\n", err)
		}
		content = string(data)

		// Check if content is unchanged from template (user didn't edit)
		if strings.TrimSpace(content) == strings.TrimSpace(tmpl) {
			fmt.Println("No changes from template. Aborted.")
			return
		}
	}

	content = strings.TrimSpace(content)
	if content == "" {
		fmt.Println("Empty content. Aborted.")
		return
	}

	// Store
	tags := orientationTags(id)
	now := time.Now().Unix()

	pipe := rdb.Pipeline()
	pipe.HSet(ctx, entryPrefix+id, map[string]interface{}{
		"id":        id,
		"content":   content,
		"tags":      strings.Join(tags, ","),
		"timestamp": now,
	})
	pipe.ZAdd(ctx, timelineKey, redis.Z{Score: float64(now), Member: id})
	for _, tag := range tags {
		pipe.SAdd(ctx, tagPrefix+tag, id)
		pipe.SAdd(ctx, allTagsKey, tag)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		fatalf("Error storing orientation: %v\n", err)
	}

	fmt.Printf("Created %s (%d chars, tags: %s)\n", id, len(content), strings.Join(tags, ", "))
}

func orientationEdit(ctx context.Context, rdb *redis.Client, args []string) {
	if len(args) == 0 {
		fatalf("Usage: orientation edit <id>\n")
	}
	id := args[0]

	entry, err := fetchEntry(ctx, rdb, id)
	if err != nil {
		fatalf("Error: %v\n", err)
	}

	// Write current content to temp file
	tmpFile, err := os.CreateTemp("", "hippocampus-orientation-*.md")
	if err != nil {
		fatalf("Error creating temp file: %v\n", err)
	}
	tmpPath := tmpFile.Name()
	defer os.Remove(tmpPath)

	if _, err := tmpFile.WriteString(entry.Content); err != nil {
		fatalf("Error writing temp file: %v\n", err)
	}
	tmpFile.Close()

	editor := os.Getenv("EDITOR")
	if editor == "" {
		editor = os.Getenv("VISUAL")
	}
	if editor == "" {
		editor = "vi"
	}

	cmd := exec.Command(editor, tmpPath)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fatalf("Editor exited with error: %v\n", err)
	}

	data, err := os.ReadFile(tmpPath)
	if err != nil {
		fatalf("Error reading edited file: %v\n", err)
	}
	content := strings.TrimSpace(string(data))

	if content == strings.TrimSpace(entry.Content) {
		fmt.Println("No changes detected.")
		return
	}

	if content == "" {
		fmt.Println("Empty content. Aborted.")
		return
	}

	// Update entry
	now := time.Now().Unix()
	pipe := rdb.Pipeline()
	pipe.HSet(ctx, entryPrefix+id, map[string]interface{}{
		"content":   content,
		"timestamp": now,
	})
	pipe.ZAdd(ctx, timelineKey, redis.Z{Score: float64(now), Member: id})

	if _, err := pipe.Exec(ctx); err != nil {
		fatalf("Error updating orientation: %v\n", err)
	}

	fmt.Printf("Updated %s (%d chars)\n", id, len(content))
}

func orientationDelete(ctx context.Context, rdb *redis.Client, args []string) {
	if len(args) == 0 {
		fatalf("Usage: orientation delete <id>\n")
	}
	id := args[0]

	entry, err := fetchEntry(ctx, rdb, id)
	if err != nil {
		fatalf("Error: %v\n", err)
	}

	fmt.Printf("Deleting orientation: %s\n", id)
	fmt.Printf("  Content preview: %s\n", truncate(entry.Content, 120))
	fmt.Printf("  Tags: %s\n", strings.Join(entry.Tags, ", "))

	if !confirm("Delete?") {
		fmt.Println("Aborted.")
		return
	}

	pipe := rdb.Pipeline()
	pipe.Del(ctx, entryPrefix+id)
	pipe.ZRem(ctx, timelineKey, id)
	for _, tag := range entry.Tags {
		pipe.SRem(ctx, tagPrefix+tag, id)
	}
	pipe.Del(ctx, linkPrefix+id)
	pipe.Do(ctx, "VREM", "hippocampus:vectors", id) // the embedding goes with the entry

	if _, err := pipe.Exec(ctx); err != nil {
		fatalf("Error deleting: %v\n", err)
	}
	fmt.Printf("Deleted %s\n", id)
}
