// Copyright (c) 2026 Ruthlessly Practical LLC. All rights reserved.
// Use of this source code is governed by a BSD-3-Clause license
// that can be found in the LICENSE file.

package mcp

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"time"

	"github.com/ruthlesslypractical/hippocampus/internal/config"
	"github.com/ruthlesslypractical/hippocampus/internal/memory"
	"github.com/ruthlesslypractical/hippocampus/pkg/ingest"
)

const (
	protocolVersion = "2024-11-05"
	serverName      = "hippocampus"
)

var serverVersion = config.Version

// Server handles MCP JSON-RPC communication over stdio.
type Server struct {
	store  memory.Store
	cfg    config.Config
	reader *bufio.Reader
	writer io.Writer
	logger *log.Logger
}

// NewServer creates a new MCP server.
func NewServer(store memory.Store, cfg config.Config) *Server {
	return &Server{
		store:  store,
		cfg:    cfg,
		reader: bufio.NewReader(os.Stdin),
		writer: os.Stdout,
		logger: log.New(os.Stderr, "[hippocampus] ", log.LstdFlags),
	}
}

// Run starts the server, reading from stdin and writing to stdout.
func (s *Server) Run(ctx context.Context) error {
	s.logger.Println("MCP server starting...")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line, err := s.reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("reading stdin: %w", err)
		}

		if len(strings.TrimSpace(string(line))) == 0 {
			continue
		}

		var req Request
		if err := json.Unmarshal(line, &req); err != nil {
			s.logger.Printf("invalid JSON: %s", err)
			continue
		}

		resp := s.handleRequest(ctx, req)
		if resp != nil {
			data, _ := json.Marshal(resp)
			fmt.Fprintf(s.writer, "%s\n", data)
		}
	}
}

func (s *Server) handleRequest(ctx context.Context, req Request) *Response {
	switch req.Method {
	case "initialize":
		return s.handleInitialize(req)
	case "notifications/initialized":
		return nil
	case "tools/list":
		return s.handleToolsList(req)
	case "tools/call":
		return s.handleToolsCall(ctx, req)
	case "ping":
		return &Response{JSONRPC: "2.0", ID: req.ID, Result: map[string]string{}}
	default:
		return &Response{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error:   &RPCError{Code: -32601, Message: fmt.Sprintf("method not found: %s", req.Method)},
		}
	}
}

func (s *Server) handleInitialize(req Request) *Response {
	return &Response{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result: InitializeResult{
			ProtocolVersion: protocolVersion,
			ServerInfo:      ServerInfo{Name: serverName, Version: serverVersion},
			Capabilities: Capabilities{
				Tools: &ToolsCapability{},
			},
		},
	}
}

func (s *Server) handleToolsList(req Request) *Response {
	return &Response{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result:  ToolsListResult{Tools: toolDefinitions()},
	}
}

func (s *Server) handleToolsCall(ctx context.Context, req Request) *Response {
	var params CallToolParams
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return &Response{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error:   &RPCError{Code: -32602, Message: "invalid params"},
		}
	}

	result, err := s.dispatchTool(ctx, params)
	if err != nil {
		return &Response{
			JSONRPC: "2.0",
			ID:      req.ID,
			Result: CallToolResult{
				Content: []ContentBlock{{Type: "text", Text: fmt.Sprintf("Error: %s", err)}},
				IsError: true,
			},
		}
	}

	return &Response{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result:  result,
	}
}

func (s *Server) dispatchTool(ctx context.Context, params CallToolParams) (CallToolResult, error) {
	switch params.Name {
	case "memory_store":
		return s.toolStore(ctx, params.Arguments)
	case "memory_search":
		return s.toolSearch(ctx, params.Arguments)
	case "memory_by_tags":
		return s.toolByTags(ctx, params.Arguments)
	case "memory_add_tags":
		return s.toolAddTags(ctx, params.Arguments)
	case "memory_remove_tags":
		return s.toolRemoveTags(ctx, params.Arguments)
	case "memory_list_tags":
		return s.toolListTags(ctx)
	case "memory_get":
		return s.toolGet(ctx, params.Arguments)
	case "memory_delete":
		return s.toolDelete(ctx, params.Arguments)
	case "memory_time_range":
		return s.toolTimeRange(ctx, params.Arguments)
	case "memory_recent":
		return s.toolRecent(ctx, params.Arguments)
	case "memory_link":
		return s.toolLink(ctx, params.Arguments)
	case "memory_unlink":
		return s.toolUnlink(ctx, params.Arguments)
	case "memory_links":
		return s.toolLinks(ctx, params.Arguments)
	case "memory_rename_tag":
		return s.toolRenameTag(ctx, params.Arguments)
	case "memory_ingest_url":
		return s.toolIngestURL(ctx, params.Arguments)
	case "memory_store_chunked":
		return s.toolStoreChunked(ctx, params.Arguments)
	case "memory_get_section":
		return s.toolGetSection(ctx, params.Arguments)
	case "memory_classify":
		return s.toolClassify(ctx, params.Arguments)
	case "memory_classify_range":
		return s.toolClassifyRange(ctx, params.Arguments)
	case "memory_reclassify":
		return s.toolReclassify(ctx, params.Arguments)
	case "memory_summary_tree":
		return s.toolSummaryTree(ctx, params.Arguments)
	case "memory_summary_leaves":
		return s.toolSummaryLeaves(ctx, params.Arguments)
	case "memory_session_context":
		return s.toolSessionContext(ctx, params.Arguments)
	case "memory_verify_attestation":
		return s.toolVerifyAttestation(ctx, params.Arguments)
	case "memory_clock":
		return s.toolClock()
	default:
		return CallToolResult{}, fmt.Errorf("unknown tool: %s", params.Name)
	}
}

// --- Tool implementations ---

func (s *Server) toolStore(ctx context.Context, args map[string]interface{}) (CallToolResult, error) {
	content, _ := args["content"].(string)
	if content == "" {
		return CallToolResult{}, fmt.Errorf("content is required")
	}

	id, _ := args["id"].(string)
	if id == "" {
		id = fmt.Sprintf("%d", time.Now().UnixNano())
	}

	var tags []string
	if t, ok := args["tags"].([]interface{}); ok {
		for _, v := range t {
			if s, ok := v.(string); ok {
				tags = append(tags, s)
			}
		}
	}

	entry := memory.Entry{
		ID:        id,
		Timestamp: time.Now(),
		Content:   content,
		Tags:      tags,
	}

	if err := s.store.Put(ctx, entry); err != nil {
		return CallToolResult{}, err
	}

	return CallToolResult{
		Content: []ContentBlock{{Type: "text", Text: fmt.Sprintf("Stored entry %s with %d tags", id, len(tags))}},
	}, nil
}

func (s *Server) toolSearch(ctx context.Context, args map[string]interface{}) (CallToolResult, error) {
	query, _ := args["query"].(string)
	if query == "" {
		return CallToolResult{}, fmt.Errorf("query is required")
	}

	limit := s.cfg.MCP.DefaultSearchLimit
	if l, ok := args["limit"].(float64); ok {
		limit = int(l)
	}

	// Check for advanced options
	sortBy, _ := args["sort_by"].(string)
	var filterTags []string
	if t, ok := args["filter_tags"].([]interface{}); ok {
		for _, v := range t {
			if s, ok := v.(string); ok {
				filterTags = append(filterTags, s)
			}
		}
	}
	var after, before int64
	if a, ok := args["after"].(float64); ok {
		after = int64(a)
	}
	if b, ok := args["before"].(float64); ok {
		before = int64(b)
	}

	// If any advanced options are set, use SearchWithOptions
	var results []memory.SearchResult
	var err error
	if sortBy != "" || len(filterTags) > 0 || after > 0 || before > 0 {
		opts := memory.SearchOptions{
			SortBy:     sortBy,
			FilterTags: filterTags,
			After:      after,
			Before:     before,
		}
		results, err = s.store.SearchWithOptions(ctx, query, limit, opts)
	} else {
		results, err = s.store.Search(ctx, query, limit)
	}
	if err != nil {
		return CallToolResult{}, err
	}

	// Verify integrity of all returned entries
	s.store.VerifySearchResults(ctx, results)

	data, _ := json.MarshalIndent(results, "", "  ")
	return CallToolResult{
		Content: []ContentBlock{{Type: "text", Text: string(data)}},
	}, nil
}

func (s *Server) toolByTags(ctx context.Context, args map[string]interface{}) (CallToolResult, error) {
	var tags []string
	if t, ok := args["tags"].([]interface{}); ok {
		for _, v := range t {
			if s, ok := v.(string); ok {
				tags = append(tags, s)
			}
		}
	}
	if len(tags) == 0 {
		return CallToolResult{}, fmt.Errorf("tags is required")
	}

	matchAll := true
	if m, ok := args["match_all"].(bool); ok {
		matchAll = m
	}

	limit := s.cfg.MCP.DefaultTagLimit
	if l, ok := args["limit"].(float64); ok {
		limit = int(l)
	}

	offset := 0
	if o, ok := args["offset"].(float64); ok {
		offset = int(o)
	}

	var entries []memory.Entry
	var err error
	if matchAll {
		entries, err = s.store.ByTags(ctx, tags, limit, offset)
	} else {
		entries, err = s.store.ByAnyTag(ctx, tags, limit, offset)
	}
	if err != nil {
		return CallToolResult{}, err
	}

	// Verify integrity of all returned entries
	s.store.VerifyEntries(ctx, entries)

	data, _ := json.MarshalIndent(entries, "", "  ")
	return CallToolResult{
		Content: []ContentBlock{{Type: "text", Text: string(data)}},
	}, nil
}

func (s *Server) toolAddTags(ctx context.Context, args map[string]interface{}) (CallToolResult, error) {
	id, _ := args["id"].(string)
	if id == "" {
		return CallToolResult{}, fmt.Errorf("id is required")
	}

	var tags []string
	if t, ok := args["tags"].([]interface{}); ok {
		for _, v := range t {
			if s, ok := v.(string); ok {
				tags = append(tags, s)
			}
		}
	}
	if len(tags) == 0 {
		return CallToolResult{}, fmt.Errorf("tags is required")
	}

	if err := s.store.AddTags(ctx, id, tags); err != nil {
		return CallToolResult{}, err
	}

	return CallToolResult{
		Content: []ContentBlock{{Type: "text", Text: fmt.Sprintf("Added %d tags to entry %s", len(tags), id)}},
	}, nil
}

func (s *Server) toolRemoveTags(ctx context.Context, args map[string]interface{}) (CallToolResult, error) {
	id, _ := args["id"].(string)
	if id == "" {
		return CallToolResult{}, fmt.Errorf("id is required")
	}

	var tags []string
	if t, ok := args["tags"].([]interface{}); ok {
		for _, v := range t {
			if s, ok := v.(string); ok {
				tags = append(tags, s)
			}
		}
	}

	if err := s.store.RemoveTags(ctx, id, tags); err != nil {
		return CallToolResult{}, err
	}

	return CallToolResult{
		Content: []ContentBlock{{Type: "text", Text: fmt.Sprintf("Removed %d tags from entry %s", len(tags), id)}},
	}, nil
}

func (s *Server) toolListTags(ctx context.Context) (CallToolResult, error) {
	infos, err := s.store.ListTags(ctx)
	if err != nil {
		return CallToolResult{}, err
	}

	data, _ := json.MarshalIndent(infos, "", "  ")
	return CallToolResult{
		Content: []ContentBlock{{Type: "text", Text: string(data)}},
	}, nil
}

func (s *Server) toolGet(ctx context.Context, args map[string]interface{}) (CallToolResult, error) {
	id, _ := args["id"].(string)
	if id == "" {
		return CallToolResult{}, fmt.Errorf("id is required")
	}

	entry, err := s.store.Get(ctx, id)
	if err != nil {
		return CallToolResult{}, err
	}

	// Verify integrity
	s.store.VerifyEntry(ctx, &entry)

	data, _ := json.MarshalIndent(entry, "", "  ")
	return CallToolResult{
		Content: []ContentBlock{{Type: "text", Text: string(data)}},
	}, nil
}

func (s *Server) toolDelete(ctx context.Context, args map[string]interface{}) (CallToolResult, error) {
	id, _ := args["id"].(string)
	if id == "" {
		return CallToolResult{}, fmt.Errorf("id is required")
	}

	if err := s.store.Delete(ctx, id); err != nil {
		return CallToolResult{}, err
	}

	return CallToolResult{
		Content: []ContentBlock{{Type: "text", Text: fmt.Sprintf("Deleted entry %s", id)}},
	}, nil
}

func (s *Server) toolTimeRange(ctx context.Context, args map[string]interface{}) (CallToolResult, error) {
	start, _ := args["start"].(float64)
	end, _ := args["end"].(float64)
	if start == 0 || end == 0 {
		return CallToolResult{}, fmt.Errorf("start and end timestamps are required")
	}

	var tags []string
	if t, ok := args["tags"].([]interface{}); ok {
		for _, v := range t {
			if s, ok := v.(string); ok {
				tags = append(tags, s)
			}
		}
	}

	limit := s.cfg.MCP.DefaultTimeRangeLimit
	if l, ok := args["limit"].(float64); ok {
		limit = int(l)
	}

	entries, err := s.store.EntriesByTimeRange(ctx, int64(start), int64(end), tags, limit)
	if err != nil {
		return CallToolResult{}, err
	}

	// Verify integrity of all returned entries
	s.store.VerifyEntries(ctx, entries)

	data, _ := json.MarshalIndent(entries, "", "  ")
	return CallToolResult{
		Content: []ContentBlock{{Type: "text", Text: string(data)}},
	}, nil
}

func (s *Server) toolRecent(ctx context.Context, args map[string]interface{}) (CallToolResult, error) {
	limit := 10
	if l, ok := args["limit"].(float64); ok && l > 0 {
		limit = int(l)
	}

	entries, err := s.store.Recent(ctx, limit)
	if err != nil {
		return CallToolResult{}, err
	}

	// Verify integrity of all returned entries
	s.store.VerifyEntries(ctx, entries)

	data, _ := json.MarshalIndent(entries, "", "  ")
	return CallToolResult{
		Content: []ContentBlock{{Type: "text", Text: string(data)}},
	}, nil
}

func (s *Server) toolSessionContext(ctx context.Context, args map[string]interface{}) (CallToolResult, error) {
	id, _ := args["id"].(string)
	if id == "" {
		return CallToolResult{}, fmt.Errorf("id is required")
	}

	before := 5
	if b, ok := args["before"].(float64); ok && b >= 0 {
		before = int(b)
	}

	after := 5
	if a, ok := args["after"].(float64); ok && a >= 0 {
		after = int(a)
	}

	entries, err := s.store.SessionContext(ctx, id, before, after)
	if err != nil {
		return CallToolResult{}, err
	}

	// Verify integrity of all returned entries
	s.store.VerifyEntries(ctx, entries)

	data, _ := json.MarshalIndent(entries, "", "  ")
	return CallToolResult{
		Content: []ContentBlock{{Type: "text", Text: string(data)}},
	}, nil
}

func (s *Server) toolLink(ctx context.Context, args map[string]interface{}) (CallToolResult, error) {
	idA, _ := args["id_a"].(string)
	idB, _ := args["id_b"].(string)
	if idA == "" || idB == "" {
		return CallToolResult{}, fmt.Errorf("id_a and id_b are required")
	}

	score, ok := args["score"].(float64)
	if !ok {
		return CallToolResult{}, fmt.Errorf("score is required")
	}
	if score < -1.0 || score > 1.0 {
		return CallToolResult{}, fmt.Errorf("score must be between -1.0 and +1.0")
	}

	relationType, _ := args["relation_type"].(string)

	if err := s.store.Link(ctx, idA, idB, score, relationType); err != nil {
		return CallToolResult{}, err
	}

	direction := "relevant"
	if score < 0 {
		direction = "anti-relevant"
	}
	msg := fmt.Sprintf("Linked %s ↔ %s (score: %.2f, %s)", idA, idB, score, direction)
	if relationType != "" {
		msg = fmt.Sprintf("Linked %s ↔ %s (score: %.2f, %s, type: %s)", idA, idB, score, direction, relationType)
	}
	return CallToolResult{
		Content: []ContentBlock{{Type: "text", Text: msg}},
	}, nil
}

func (s *Server) toolUnlink(ctx context.Context, args map[string]interface{}) (CallToolResult, error) {
	idA, _ := args["id_a"].(string)
	idB, _ := args["id_b"].(string)
	if idA == "" || idB == "" {
		return CallToolResult{}, fmt.Errorf("id_a and id_b are required")
	}

	if err := s.store.Unlink(ctx, idA, idB); err != nil {
		return CallToolResult{}, err
	}

	return CallToolResult{
		Content: []ContentBlock{{Type: "text", Text: fmt.Sprintf("Unlinked %s ↔ %s", idA, idB)}},
	}, nil
}

func (s *Server) toolLinks(ctx context.Context, args map[string]interface{}) (CallToolResult, error) {
	id, _ := args["id"].(string)
	if id == "" {
		return CallToolResult{}, fmt.Errorf("id is required")
	}

	links, err := s.store.Links(ctx, id)
	if err != nil {
		return CallToolResult{}, err
	}

	data, _ := json.MarshalIndent(links, "", "  ")
	return CallToolResult{
		Content: []ContentBlock{{Type: "text", Text: string(data)}},
	}, nil
}

func (s *Server) toolRenameTag(ctx context.Context, args map[string]interface{}) (CallToolResult, error) {
	oldTag, _ := args["old_tag"].(string)
	newTag, _ := args["new_tag"].(string)
	if oldTag == "" || newTag == "" {
		return CallToolResult{}, fmt.Errorf("old_tag and new_tag are required")
	}

	count, err := s.store.RenameTag(ctx, oldTag, newTag)
	if err != nil {
		return CallToolResult{}, err
	}

	return CallToolResult{
		Content: []ContentBlock{{Type: "text", Text: fmt.Sprintf("Renamed tag '%s' → '%s' across %d entries", oldTag, newTag, count)}},
	}, nil
}

func (s *Server) toolIngestURL(ctx context.Context, args map[string]interface{}) (CallToolResult, error) {
	rawURL, _ := args["url"].(string)
	if rawURL == "" {
		return CallToolResult{}, fmt.Errorf("url is required")
	}

	var tags []string
	if t, ok := args["tags"].([]interface{}); ok {
		for _, v := range t {
			if s, ok := v.(string); ok {
				tags = append(tags, s)
			}
		}
	}

	opts := ingest.DefaultOptions()
	opts.Tags = tags
	opts.RejectThreshold = s.cfg.Ingest.RejectThreshold
	opts.SanitizeThreshold = s.cfg.Ingest.SanitizeThreshold
	opts.WebContentWeight = s.cfg.Ingest.WebContentWeight
	opts.StubWeight = s.cfg.Ingest.StubWeight

	result, err := ingest.Pipeline(ctx, s.store, rawURL, opts)
	if err != nil {
		// If we got a result with safety info, include it in the error response
		if result != nil && result.SafetyResult.RiskScore > 0 {
			return CallToolResult{
				Content: []ContentBlock{{Type: "text", Text: fmt.Sprintf("Ingestion failed: %s\nSafety scan: risk_score=%.2f, flags=%d", err, result.SafetyResult.RiskScore, len(result.SafetyResult.Flags))}},
				IsError: true,
			}, nil
		}
		return CallToolResult{}, err
	}

	// Override title if requested
	if titleOverride, ok := args["title_override"].(string); ok && titleOverride != "" {
		result.Title = titleOverride
	}

	// Build response
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("✓ Ingested: \"%s\"\n", result.Title))
	sb.WriteString(fmt.Sprintf("  URL: %s\n", result.URL))
	sb.WriteString(fmt.Sprintf("  Words: %d | Chunks: %d\n", result.WordCount, result.ChunkCount))
	sb.WriteString(fmt.Sprintf("  Stub ID: %s\n", result.StubID))
	sb.WriteString(fmt.Sprintf("  Content IDs: %d entries\n", len(result.ContentIDs)))
	if result.SafetyResult.RiskScore > 0 {
		sb.WriteString(fmt.Sprintf("  Safety: risk_score=%.2f (flags: %d)\n", result.SafetyResult.RiskScore, len(result.SafetyResult.Flags)))
	} else {
		sb.WriteString("  Safety: clean ✓\n")
	}
	if len(result.Warnings) > 0 {
		sb.WriteString(fmt.Sprintf("  Warnings: %s\n", strings.Join(result.Warnings, "; ")))
	}

	return CallToolResult{
		Content: []ContentBlock{{Type: "text", Text: sb.String()}},
	}, nil
}

func (s *Server) toolStoreChunked(ctx context.Context, args map[string]interface{}) (CallToolResult, error) {
	content, _ := args["content"].(string)
	if content == "" {
		return CallToolResult{}, fmt.Errorf("content is required")
	}

	taskID, _ := args["task_id"].(string)
	if taskID == "" {
		taskID = fmt.Sprintf("t%d", time.Now().UnixNano())
	}

	maxChunkSize := 4000
	if m, ok := args["max_chunk_size"].(float64); ok && m > 0 {
		maxChunkSize = int(m)
	}

	var extraTags []string
	if t, ok := args["tags"].([]interface{}); ok {
		for _, v := range t {
			if s, ok := v.(string); ok {
				extraTags = append(extraTags, s)
			}
		}
	}

	// Split content into chunks at line boundaries
	chunks := splitAtLines(content, maxChunkSize)

	var chunkIDs []string
	for i, chunk := range chunks {
		id := fmt.Sprintf("task:%s:chunk:%d", taskID, i)
		tags := []string{
			fmt.Sprintf("task:%s", taskID),
			fmt.Sprintf("task:%s:chunk:%d", taskID, i),
			fmt.Sprintf("chunk:%d/%d", i, len(chunks)),
		}
		tags = append(tags, extraTags...)

		entry := memory.Entry{
			ID:        id,
			Timestamp: time.Now(),
			Content:   chunk,
			Tags:      tags,
		}
		if err := s.store.Put(ctx, entry); err != nil {
			return CallToolResult{}, fmt.Errorf("storing chunk %d: %w", i, err)
		}
		chunkIDs = append(chunkIDs, id)
	}

	// Build manifest response
	manifest := map[string]interface{}{
		"task_id":     taskID,
		"chunk_count": len(chunks),
		"chunk_ids":   chunkIDs,
		"total_chars": len(content),
	}
	data, _ := json.MarshalIndent(manifest, "", "  ")

	return CallToolResult{
		Content: []ContentBlock{{Type: "text", Text: string(data)}},
	}, nil
}

func (s *Server) toolGetSection(ctx context.Context, args map[string]interface{}) (CallToolResult, error) {
	taskID, _ := args["task_id"].(string)
	if taskID == "" {
		return CallToolResult{}, fmt.Errorf("task_id is required")
	}

	index := -1
	if idx, ok := args["index"].(float64); ok {
		index = int(idx)
	}

	if index == -1 {
		// Retrieve all chunks, concatenate in order
		// First, find all entries with task:<id> tag
		entries, err := s.store.ByTags(ctx, []string{fmt.Sprintf("task:%s", taskID)}, 1000, 0)
		if err != nil {
			return CallToolResult{}, err
		}
		if len(entries) == 0 {
			return CallToolResult{}, fmt.Errorf("no chunks found for task %s", taskID)
		}

		// Sort by chunk index (parse from ID)
		sorted := make([]string, len(entries))
		for _, e := range entries {
			// ID format: task:<taskID>:chunk:<N>
			var idx int
			if _, err := fmt.Sscanf(e.ID, "task:"+taskID+":chunk:%d", &idx); err == nil && idx < len(sorted) {
				sorted[idx] = e.Content
			}
		}

		var sb strings.Builder
		for _, c := range sorted {
			if c != "" {
				sb.WriteString(c)
			}
		}

		return CallToolResult{
			Content: []ContentBlock{{Type: "text", Text: sb.String()}},
		}, nil
	}

	// Retrieve specific chunk by index
	id := fmt.Sprintf("task:%s:chunk:%d", taskID, index)
	entry, err := s.store.Get(ctx, id)
	if err != nil {
		return CallToolResult{}, fmt.Errorf("chunk %d not found for task %s: %w", index, taskID, err)
	}

	return CallToolResult{
		Content: []ContentBlock{{Type: "text", Text: entry.Content}},
	}, nil
}

func (s *Server) toolClock() (CallToolResult, error) {
	now := time.Now()
	return CallToolResult{
		Content: []ContentBlock{{Type: "text", Text: fmt.Sprintf(`{"unix":%d,"rfc3339":"%s"}`, now.Unix(), now.Format(time.RFC3339))}},
	}, nil
}

func (s *Server) toolVerifyAttestation(ctx context.Context, args map[string]interface{}) (CallToolResult, error) {
	id, _ := args["id"].(string)
	if id == "" {
		return CallToolResult{}, fmt.Errorf("id is required")
	}

	result, err := s.store.VerifyAttestation(ctx, id)
	if err != nil {
		return CallToolResult{}, err
	}

	data, _ := json.Marshal(result)
	return CallToolResult{
		Content: []ContentBlock{{Type: "text", Text: string(data)}},
	}, nil
}

// splitAtLines splits content into chunks of at most maxSize characters,
// preferring to split at newline boundaries.
func splitAtLines(content string, maxSize int) []string {
	if len(content) <= maxSize {
		return []string{content}
	}

	var chunks []string
	remaining := content

	for len(remaining) > 0 {
		if len(remaining) <= maxSize {
			chunks = append(chunks, remaining)
			break
		}

		// Find the last newline within maxSize
		cutPoint := maxSize
		lastNewline := strings.LastIndex(remaining[:maxSize], "\n")
		if lastNewline > maxSize/4 {
			// Only use the newline split if it's not too early (don't make tiny chunks)
			cutPoint = lastNewline + 1
		}

		chunks = append(chunks, remaining[:cutPoint])
		remaining = remaining[cutPoint:]
	}

	return chunks
}
