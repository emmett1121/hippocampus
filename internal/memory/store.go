// Copyright (c) 2026 Ruthlessly Practical LLC. All rights reserved.
// Use of this source code is governed by a BSD-3-Clause license
// that can be found in the LICENSE file.

package memory

import "context"

// Store defines the interface for the memory system.
// All tags are just strings — tracks, summaries, topics, temporal scopes, etc.
// Tags do all the organizational work. No fixed schema beyond Entry.
type Store interface {
	// Put stores an entry. If ID already exists, it's overwritten.
	Put(ctx context.Context, entry Entry) error

	// Get retrieves a single entry by ID.
	Get(ctx context.Context, id string) (Entry, error)

	// Delete removes an entry by ID.
	Delete(ctx context.Context, id string) error

	// Search finds entries matching a query string (full-text search).
	// Falls back to naive substring scan if RediSearch is not available.
	Search(ctx context.Context, query string, limit int) ([]SearchResult, error)

	// SearchWithOptions performs full-text search with sort, tag filter, and time bounds.
	// When SortBy is "timestamp_asc" or "timestamp_desc", results are ordered chronologically
	// instead of by relevance. FilterTags restricts to entries matching all specified tags.
	// After/Before constrain the timestamp range.
	SearchWithOptions(ctx context.Context, query string, limit int, opts SearchOptions) ([]SearchResult, error)

	// ByTags returns entries that have ALL of the specified tags.
	ByTags(ctx context.Context, tags []string, limit int, offset int) ([]Entry, error)

	// ByAnyTag returns entries that have ANY of the specified tags.
	ByAnyTag(ctx context.Context, tags []string, limit int, offset int) ([]Entry, error)

	// AddTags adds tags to an existing entry.
	AddTags(ctx context.Context, id string, tags []string) error

	// RemoveTags removes tags from an existing entry.
	RemoveTags(ctx context.Context, id string, tags []string) error

	// ListTags returns all known tags with their entry counts.
	ListTags(ctx context.Context) ([]TagInfo, error)

	// EntriesByTimeRange returns entries within a time window, optionally filtered by tags.
	EntriesByTimeRange(ctx context.Context, start, end int64, tags []string, limit int) ([]Entry, error)

	// Link creates a bidirectional associative link between two entries.
	// Score ranges from -1.0 (anti-relevant) to +1.0 (strongly relevant).
	// RelationType is optional: "supports", "contradicts", "extends", "preceded_by", or "".
	Link(ctx context.Context, idA, idB string, score float64, relationType string) error

	// Unlink removes a bidirectional link between two entries.
	Unlink(ctx context.Context, idA, idB string) error

	// Links returns all links for an entry, sorted by |score| descending.
	Links(ctx context.Context, id string) ([]Link, error)

	// TopLinks returns the top N links for an entry by |score|.
	TopLinks(ctx context.Context, id string, n int) ([]Link, error)

	// RenameTag renames a tag across all entries. Moves members between tag sets,
	// updates each entry's tag string, and updates the global tag registry.
	RenameTag(ctx context.Context, oldTag, newTag string) (int, error)

	// Recent returns the N most recent entries from the timeline (newest first).
	// No timestamp math required — just tail the timeline ZSET.
	Recent(ctx context.Context, limit int) ([]Entry, error)

	// SessionContext returns entries surrounding a given entry within the same session.
	// Returns up to `before` entries before and `after` entries after the target,
	// plus the target itself, all in chronological order.
	SessionContext(ctx context.Context, id string, before, after int) ([]Entry, error)

	// VerifyAttestation performs deep TSA chain verification for a single entry:
	// recomputes content hash, validates Merkle path, parses TSR, verifies X.509 chain.
	VerifyAttestation(ctx context.Context, entryID string) (*AttestationResult, error)

	// VerifyEntry checks the integrity of a single entry by comparing its stored
	// content_hash against a freshly computed hash. Populates entry.Integrity.
	VerifyEntry(ctx context.Context, entry *Entry)

	// VerifyEntries checks integrity for a slice of entries.
	VerifyEntries(ctx context.Context, entries []Entry)

	// VerifySearchResults checks integrity for search results.
	VerifySearchResults(ctx context.Context, results []SearchResult)

	// Close releases resources.
	Close() error
}
