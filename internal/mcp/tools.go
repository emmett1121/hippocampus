// Copyright (c) 2026 Ruthlessly Practical LLC. All rights reserved.
// Use of this source code is governed by a BSD-3-Clause license
// that can be found in the LICENSE file.

package mcp

import "encoding/json"

// toolDefinitions returns the MCP tool schemas.
func toolDefinitions() []Tool {
	return []Tool{
		{
			Name:        "memory_store",
			Description: "Store an entry in memory. Entries are the fundamental unit — everything is an entry differentiated by tags. Use for decisions, summaries, insights, observations, or any information worth preserving across sessions.",
			InputSchema: mustJSON(map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"content": map[string]interface{}{
						"type":        "string",
						"description": "The content to store. Should be concise and self-contained.",
					},
					"tags": map[string]interface{}{
						"type":        "array",
						"items":       map[string]interface{}{"type": "string"},
						"description": "Tags for this entry. Use for tracks (track:project-name), summaries (summary:session:id), topics (medical, architecture), temporal (day:2026-07-16), or any other categorization.",
					},
					"id": map[string]interface{}{
						"type":        "string",
						"description": "Optional ID. Auto-generated if not provided.",
					},
				},
				"required": []string{"content", "tags"},
			}),
		},
		{
			Name:        "memory_search",
			Description: "Full-text search across all memory entries. Returns entries ranked by relevance.",
			InputSchema: mustJSON(map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"query": map[string]interface{}{
						"type":        "string",
						"description": "Search query (full-text).",
					},
					"limit": map[string]interface{}{
						"type":        "number",
						"description": "Max results to return. Default 10.",
					},
					"sort_by": map[string]interface{}{
						"type":        "string",
						"description": "Sort order for results. 'relevance' (default) ranks by search score. 'timestamp_asc' returns oldest first (use with limit:1 for 'first mention of X'). 'timestamp_desc' returns newest first.",
						"enum":        []string{"relevance", "timestamp_asc", "timestamp_desc"},
					},
					"filter_tags": map[string]interface{}{
						"type":        "array",
						"items":       map[string]interface{}{"type": "string"},
						"description": "Optional tag filter. Only entries matching ALL specified tags are returned. Tags are matched exactly (e.g., 'kind:user_prompt', 'track:VBrain').",
					},
					"after": map[string]interface{}{
						"type":        "number",
						"description": "Only return entries with timestamp after this value (Unix seconds). Useful for 'first mention since date X'.",
					},
					"before": map[string]interface{}{
						"type":        "number",
						"description": "Only return entries with timestamp before this value (Unix seconds). Useful for 'mentions before date X'.",
					},
				},
				"required": []string{"query"},
			}),
		},
		{
			Name:        "memory_by_tags",
			Description: "Retrieve entries by tags. Supports intersection (all tags must match) or union (any tag matches). Use for browsing tracks, finding cross-pollination between topics, or pulling summaries at a specific level.",
			InputSchema: mustJSON(map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"tags": map[string]interface{}{
						"type":        "array",
						"items":       map[string]interface{}{"type": "string"},
						"description": "Tags to filter by.",
					},
					"match_all": map[string]interface{}{
						"type":        "boolean",
						"description": "If true (default), entries must have ALL tags. If false, entries with ANY of the tags are returned.",
					},
					"limit": map[string]interface{}{
						"type":        "number",
						"description": "Max results. Default 20.",
					},
					"offset": map[string]interface{}{
						"type":        "number",
						"description": "Pagination offset. Default 0.",
					},
				},
				"required": []string{"tags"},
			}),
		},
		{
			Name:        "memory_add_tags",
			Description: "Add tags to an existing entry. Use for retroactive tagging, applying new categories, or linking entries to tracks/summaries.",
			InputSchema: mustJSON(map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"id": map[string]interface{}{
						"type":        "string",
						"description": "Entry ID to tag.",
					},
					"tags": map[string]interface{}{
						"type":        "array",
						"items":       map[string]interface{}{"type": "string"},
						"description": "Tags to add.",
					},
				},
				"required": []string{"id", "tags"},
			}),
		},
		{
			Name:        "memory_remove_tags",
			Description: "Remove tags from an existing entry.",
			InputSchema: mustJSON(map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"id": map[string]interface{}{
						"type":        "string",
						"description": "Entry ID.",
					},
					"tags": map[string]interface{}{
						"type":        "array",
						"items":       map[string]interface{}{"type": "string"},
						"description": "Tags to remove.",
					},
				},
				"required": []string{"id", "tags"},
			}),
		},
		{
			Name:        "memory_list_tags",
			Description: "List all tags in the memory system with entry counts. Useful for discovering what's stored and browsing the tag taxonomy.",
			InputSchema: mustJSON(map[string]interface{}{
				"type":       "object",
				"properties": map[string]interface{}{},
			}),
		},
		{
			Name:        "memory_get",
			Description: "Retrieve a specific entry by ID.",
			InputSchema: mustJSON(map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"id": map[string]interface{}{
						"type":        "string",
						"description": "Entry ID to retrieve.",
					},
				},
				"required": []string{"id"},
			}),
		},
		{
			Name:        "memory_delete",
			Description: "Delete an entry by ID. Removes it from all tag sets and the timeline.",
			InputSchema: mustJSON(map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"id": map[string]interface{}{
						"type":        "string",
						"description": "Entry ID to delete.",
					},
				},
				"required": []string{"id"},
			}),
		},
		{
			Name:        "memory_time_range",
			Description: "Retrieve entries within a time window, optionally filtered by tags. Useful for pulling context from a specific period.",
			InputSchema: mustJSON(map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"start": map[string]interface{}{
						"type":        "number",
						"description": "Start timestamp (Unix seconds).",
					},
					"end": map[string]interface{}{
						"type":        "number",
						"description": "End timestamp (Unix seconds).",
					},
					"tags": map[string]interface{}{
						"type":        "array",
						"items":       map[string]interface{}{"type": "string"},
						"description": "Optional tag filter (entries must have all specified tags).",
					},
					"limit": map[string]interface{}{
						"type":        "number",
						"description": "Max results. Default 20.",
					},
				},
				"required": []string{"start", "end"},
			}),
		},
		{
			Name:        "memory_recent",
			Description: "Retrieve the N most recent entries from the timeline (newest first). No timestamp math needed — just returns the tail of the timeline. Use this instead of memory_time_range when you want 'what just happened' without computing Unix timestamps.",
			InputSchema: mustJSON(map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"limit": map[string]interface{}{
						"type":        "number",
						"description": "Number of recent entries to return. Default 10.",
					},
				},
			}),
		},
		{
			Name:        "memory_link",
			Description: "Create a bidirectional associative link between two entries. Score ranges from -1.0 (anti-relevant: 'we tried this, it failed') to +1.0 (strongly relevant: 'this directly supports/extends that'). Links persist across sessions and are followed during recall.",
			InputSchema: mustJSON(map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"id_a": map[string]interface{}{
						"type":        "string",
						"description": "First entry ID.",
					},
					"id_b": map[string]interface{}{
						"type":        "string",
						"description": "Second entry ID.",
					},
					"score": map[string]interface{}{
						"type":        "number",
						"description": "Associativity score from -1.0 to +1.0. Positive = relevant/supporting. Negative = anti-relevant/contradicting/superseded.",
					},
					"relation_type": map[string]interface{}{
						"type":        "string",
						"description": "Optional relation type: supports, contradicts, extends, preceded_by. Omit for untyped links.",
					},
				},
				"required": []string{"id_a", "id_b", "score"},
			}),
		},
		{
			Name:        "memory_unlink",
			Description: "Remove a bidirectional link between two entries.",
			InputSchema: mustJSON(map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"id_a": map[string]interface{}{
						"type":        "string",
						"description": "First entry ID.",
					},
					"id_b": map[string]interface{}{
						"type":        "string",
						"description": "Second entry ID.",
					},
				},
				"required": []string{"id_a", "id_b"},
			}),
		},
		{
			Name:        "memory_links",
			Description: "Retrieve all associative links for an entry, sorted by |score| descending (strongest signal first, positive or negative).",
			InputSchema: mustJSON(map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"id": map[string]interface{}{
						"type":        "string",
						"description": "Entry ID to get links for.",
					},
				},
				"required": []string{"id"},
			}),
		},
		{
			Name:        "memory_rename_tag",
			Description: "Rename a tag across all entries that have it. Updates every entry's tag string, moves members between tag sets, and updates the global tag registry.",
			InputSchema: mustJSON(map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"old_tag": map[string]interface{}{
						"type":        "string",
						"description": "The existing tag to rename.",
					},
					"new_tag": map[string]interface{}{
						"type":        "string",
						"description": "The new tag name.",
					},
				},
				"required": []string{"old_tag", "new_tag"},
			}),
		},
		{
			Name:        "memory_ingest_url",
			Description: "Ingest a web page into memory. Fetches the URL, extracts readable content (reader mode), scans for prompt injection, chunks if needed, and stores as a two-stage entry: a stub (auto-recalled with metadata) + full content entries (on-demand, untrusted). The stub contains a pointer to the full content which can be loaded with memory_get when needed.",
			InputSchema: mustJSON(map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"url": map[string]interface{}{
						"type":        "string",
						"description": "The URL to fetch and ingest.",
					},
					"tags": map[string]interface{}{
						"type":        "array",
						"items":       map[string]interface{}{"type": "string"},
						"description": "Tags to apply to the ingested entries (in addition to automatic source:web and url:* tags).",
					},
					"title_override": map[string]interface{}{
						"type":        "string",
						"description": "Optional: override the extracted title with a custom one.",
					},
				},
				"required": []string{"url"},
			}),
		},
		{
			Name:        "memory_store_chunked",
			Description: "Store large content by auto-splitting into chunks. Each chunk is stored as a separate entry tagged with task:<id>:chunk:<N>. Returns a manifest with the task ID, chunk count, and chunk IDs. Designed for subagent workflows where intermediate results need to be passed between pipeline stages via shared memory.",
			InputSchema: mustJSON(map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"content": map[string]interface{}{
						"type":        "string",
						"description": "The content to store. Will be split into chunks if it exceeds max_chunk_size.",
					},
					"task_id": map[string]interface{}{
						"type":        "string",
						"description": "A unique task identifier. Used to namespace chunks (task:<id>:chunk:<N>). If not provided, one is auto-generated.",
					},
					"tags": map[string]interface{}{
						"type":        "array",
						"items":       map[string]interface{}{"type": "string"},
						"description": "Additional tags to apply to all chunk entries.",
					},
					"max_chunk_size": map[string]interface{}{
						"type":        "number",
						"description": "Maximum characters per chunk. Default 4000. Splits at line boundaries.",
					},
				},
				"required": []string{"content"},
			}),
		},
		{
			Name:        "memory_get_section",
			Description: "Retrieve a specific chunk from a chunked task by index. Use after memory_store_chunked to read back specific sections. Provide the task_id and chunk index (0-based).",
			InputSchema: mustJSON(map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"task_id": map[string]interface{}{
						"type":        "string",
						"description": "The task ID used when storing (from the manifest returned by memory_store_chunked).",
					},
					"index": map[string]interface{}{
						"type":        "number",
						"description": "Chunk index (0-based). Use -1 to retrieve all chunks concatenated.",
					},
				},
				"required": []string{"task_id", "index"},
			}),
		},
		{
			Name:        "memory_classify",
			Description: "Classify a single entry into one or more project tracks using windowed session context. Returns proposed tracks and confidence. Uses track manifests for disambiguation.",
			InputSchema: mustJSON(map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"id": map[string]interface{}{
						"type":        "string",
						"description": "Entry ID to classify.",
					},
					"force": map[string]interface{}{
						"type":        "boolean",
						"description": "If true, reclassify even if already classified. Default false.",
					},
					"dry_run": map[string]interface{}{
						"type":        "boolean",
						"description": "If true, return proposed classification without applying it.",
					},
				},
				"required": []string{"id"},
			}),
		},
		{
			Name:        "memory_classify_range",
			Description: "Classify all unclassified entries in a time range or session. Uses windowed context and track manifests. Supports force mode to reclassify already-classified entries.",
			InputSchema: mustJSON(map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"start": map[string]interface{}{
						"type":        "number",
						"description": "Start timestamp (Unix seconds).",
					},
					"end": map[string]interface{}{
						"type":        "number",
						"description": "End timestamp (Unix seconds).",
					},
					"session": map[string]interface{}{
						"type":        "string",
						"description": "Optional: classify entries in a specific session instead of a time range.",
					},
					"force": map[string]interface{}{
						"type":        "boolean",
						"description": "If true, reclassify even if already classified. Default false.",
					},
					"dry_run": map[string]interface{}{
						"type":        "boolean",
						"description": "If true, return proposed classifications without applying them.",
					},
				},
			}),
		},
		{
			Name:        "memory_reclassify",
			Description: "Re-examine existing classifications. Strips auto-assigned track tags and re-classifies using improved context. Skips manually corrected entries (classified without classified:auto).",
			InputSchema: mustJSON(map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"start": map[string]interface{}{
						"type":        "number",
						"description": "Start timestamp (Unix seconds).",
					},
					"end": map[string]interface{}{
						"type":        "number",
						"description": "End timestamp (Unix seconds).",
					},
					"tag": map[string]interface{}{
						"type":        "string",
						"description": "Optional: only reclassify entries with this tag (e.g., 'classified:confused').",
					},
					"dry_run": map[string]interface{}{
						"type":        "boolean",
						"description": "If true, return proposed reclassifications without applying them.",
					},
				},
			}),
		},
		{
			Name:        "memory_session_context",
			Description: "Retrieve entries surrounding a given entry within the same session, in chronological order. Use to see what came before/after a specific entry in the conversation flow. Returns up to `before` entries before the target and `after` entries after it, plus the target itself.",
			InputSchema: mustJSON(map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"id": map[string]interface{}{
						"type":        "string",
						"description": "Entry ID to get context around.",
					},
					"before": map[string]interface{}{
						"type":        "number",
						"description": "Number of entries before the target to return. Default 5.",
					},
					"after": map[string]interface{}{
						"type":        "number",
						"description": "Number of entries after the target to return. Default 5.",
					},
				},
				"required": []string{"id"},
			}),
		},
		{
			Name:        "memory_summary_tree",
			Description: "Walk the summary provenance tree. Given a summary ID (weekly/daily/3h), returns the full hierarchy of child summaries and leaf entries that produced it. Use for auditing what went into a summary, tracing misclassifications back to source entries, or understanding summary lineage.",
			InputSchema: mustJSON(map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"id": map[string]interface{}{
						"type":        "string",
						"description": "Summary entry ID (e.g., 'summary:daily:VBrain:2026-06-14', 'summary:weekly:Markets:2026-07-13').",
					},
					"max_depth": map[string]interface{}{
						"type":        "number",
						"description": "Maximum recursion depth (default: 10). Use 1 to get only immediate children.",
					},
				},
				"required": []string{"id"},
			}),
		},
		{
			Name:        "memory_summary_leaves",
			Description: "Get all leaf (non-summary) entry IDs that fed into a summary. Walks the full tree and returns only the deepest entries — the raw prompts/responses/stored artifacts. Use this to iterate over source entries for bulk retag/reclassification operations.",
			InputSchema: mustJSON(map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"id": map[string]interface{}{
						"type":        "string",
						"description": "Summary entry ID to trace leaves for.",
					},
				},
				"required": []string{"id"},
			}),
		},
		{
			Name:        "memory_verify_attestation",
			Description: "Deep TSA chain verification for a single entry. Recomputes content hash, validates Merkle path from leaf to root, parses the RFC 3161 TSR, and verifies the X.509 certificate chain. Use when you want cryptographic proof that a memory existed at the claimed time and hasn't been tampered with. More expensive than the fast hash check — use on-demand, not on every read.",
			InputSchema: mustJSON(map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"id": map[string]interface{}{
						"type":        "string",
						"description": "Entry ID to verify. Can also be a TSA block ID (tsa:*) to verify the block itself.",
					},
				},
				"required": []string{"id"},
			}),
		},
		{
			Name:        "memory_clock",
			Description: "Returns the current time as both Unix epoch seconds and RFC 3339 formatted string. Use to check elapsed time during long-running operations like pacemaker cycles.",
			InputSchema: mustJSON(map[string]interface{}{
				"type":       "object",
				"properties": map[string]interface{}{},
			}),
		},
	}
}

func mustJSON(v interface{}) json.RawMessage {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return data
}
