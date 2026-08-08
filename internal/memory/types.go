// Copyright (c) 2026 Ruthlessly Practical LLC. All rights reserved.
// Use of this source code is governed by a BSD-3-Clause license
// that can be found in the LICENSE file.

package memory

import (
	"strings"
	"time"
)

// Entry is the fundamental unit of memory. Everything is an entry with tags.
// Tracks, summaries, raw conversation chunks, decisions — all entries,
// differentiated solely by tags. No fixed schema beyond this.
type Entry struct {
	ID        string    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	Content   string    `json:"content"`
	Tags      []string  `json:"tags"`
	Weight    float64   `json:"weight,omitempty"`    // Confidence weight: 0.0-1.0
	Integrity string   `json:"integrity,omitempty"` // "verified", "unattested", or "modified"
}

// SearchResult wraps an entry with a relevance score.
type SearchResult struct {
	Entry Entry   `json:"entry"`
	Score float64 `json:"score"`
}

// TagInfo contains metadata about a tag.
type TagInfo struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

// Link represents an associative link between two entries.
// Score ranges from -1.0 (anti-relevant: "we tried this, it failed")
// to +1.0 (strongly relevant: "this directly supports/extends that").
type Link struct {
	TargetID     string  `json:"target_id"`
	Score        float64 `json:"score"`
	RelationType string  `json:"relation_type,omitempty"` // supports, contradicts, extends, preceded_by, or empty
}

// SearchOptions configures advanced search behavior.
type SearchOptions struct {
	SortBy     string   // "relevance" (default), "timestamp_asc", "timestamp_desc"
	FilterTags []string // Only return entries matching ALL of these tags
	After      int64    // Only entries after this unix timestamp (0 = no lower bound)
	Before     int64    // Only entries before this unix timestamp (0 = no upper bound)
}

// DefaultWeightForTags returns a confidence weight based on tag patterns.
// Curated stores get 1.0, summaries 0.9, auto-captured responses 0.5, prompts 0.3.
func DefaultWeightForTags(tags []string) float64 {
	for _, t := range tags {
		if t == "summary:comprehensive" || strings.HasPrefix(t, "summary:daily:") || strings.HasPrefix(t, "summary:weekly:") {
			return 0.9
		}
		if strings.HasPrefix(t, "summary:") {
			return 0.8
		}
	}
	for _, t := range tags {
		if t == "kind:user_prompt" {
			return 0.3
		}
		if t == "kind:assistant_response" || t == "kind:assistantmessage" {
			return 0.5
		}
		if t == "auto:captured" {
			return 0.4
		}
	}
	// Explicitly stored (no auto:captured tag) = highest confidence
	return 1.0
}
