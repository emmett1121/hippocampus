// Copyright (c) 2026 Ruthlessly Practical LLC. All rights reserved.
// Use of this source code is governed by a BSD-3-Clause license
// that can be found in the LICENSE file.

package config

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/redis/go-redis/v9"
)

// Version is the single source of truth for the Hippocampus version.
// Overridden at build time via -ldflags "-X ...config.Version=X.Y.Z"
var Version = "0.0.0-dev"

// Config holds all configuration for Hippocampus.
type Config struct {
	Redis         RedisConfig         `json:"redis"`
	Memory        MemoryConfig        `json:"memory"`
	Ollama        OllamaConfig        `json:"ollama"`
	Ingest        IngestConfig        `json:"ingest"`
	Consolidation ConsolidationConfig `json:"consolidation"`
	Hook          HookConfig          `json:"hook"`
	MCP           MCPConfig           `json:"mcp"`
	Slack         SlackConfig         `json:"slack"`
	Log           LogConfig           `json:"log"`
	WorkingSet    WorkingSetConfig    `json:"working_set"`
	OFC           OFCConfig           `json:"ofc"`
	Epistemic     EpistemicConfig     `json:"epistemic"`
	Daemon        DaemonConfig        `json:"daemon"`
	PubMed        PubMedConfig        `json:"pubmed"`
	Author        string              `json:"author"` // Attribution tag for multi-user (e.g. "alice"). Empty = no attribution.
}

// PubMedConfig holds NCBI E-utilities (PubMed) API settings.
type PubMedConfig struct {
	APIKey string `json:"api_key,omitempty"` // NCBI API key (free, from ncbi.nlm.nih.gov/account)
	Email  string `json:"email,omitempty"`   // Contact email (required by NCBI usage policy)
}

// RedisConfig holds Redis/Valkey connection settings.
type RedisConfig struct {
	Addr     string `json:"addr"`              // host:port
	Password string `json:"password,omitempty"` // AUTH password
	Username string `json:"username,omitempty"` // ACL username (Valkey 6+)
	DB       int    `json:"db"`                // database number

	// TLS settings
	TLS         bool   `json:"tls"`                    // enable TLS
	TLSCert     string `json:"tls_cert,omitempty"`     // client cert path
	TLSKey      string `json:"tls_key,omitempty"`      // client key path
	TLSCA       string `json:"tls_ca,omitempty"`       // CA cert path
	TLSInsecure bool   `json:"tls_insecure,omitempty"` // skip server cert verification

	// Connection tuning
	MaxRetries   int `json:"max_retries,omitempty"`    // default 3
	DialTimeoutS int `json:"dial_timeout_s,omitempty"` // default 5
	PoolSize     int `json:"pool_size,omitempty"`      // default 10
}

// NewRedisClient creates a Redis client from the config. Single source of truth for connection setup.
func (c *RedisConfig) NewRedisClient() *redis.Client {
	opts := &redis.Options{
		Addr:        c.Addr,
		Password:    c.Password,
		Username:    c.Username,
		DB:          c.DB,
		MaxRetries:  c.MaxRetries,
		DialTimeout: time.Duration(c.DialTimeoutS) * time.Second,
		PoolSize:    c.PoolSize,
	}
	if c.TLS {
		tlsCfg := &tls.Config{
			InsecureSkipVerify: c.TLSInsecure,
		}

		// Load custom CA cert for server verification
		if c.TLSCA != "" {
			caCert, err := os.ReadFile(c.TLSCA)
			if err == nil {
				pool := x509.NewCertPool()
				if pool.AppendCertsFromPEM(caCert) {
					tlsCfg.RootCAs = pool
				}
			}
		}

		// Load client certificate (mTLS)
		if c.TLSCert != "" && c.TLSKey != "" {
			cert, err := tls.LoadX509KeyPair(c.TLSCert, c.TLSKey)
			if err == nil {
				tlsCfg.Certificates = []tls.Certificate{cert}
			}
		}

		opts.TLSConfig = tlsCfg
	}
	return redis.NewClient(opts)
}

// MemoryConfig holds memory system tuning.
type MemoryConfig struct {
	MaxSearchResults int `json:"max_search_results"` // MCP search default limit

	// Hook recall settings (what gets injected into the model's context)
	RecallMaxChars   int `json:"recall_max_chars"`   // max total chars injected
	RecallMaxEntries int `json:"recall_max_entries"` // max entries per prompt
	RecallScanDepth  int `json:"recall_scan_depth"`  // how many recent entries to scan in naive fallback (default 100)

	// Hook store settings (what gets persisted from conversations)
	StoreMaxChars       int `json:"store_max_chars"`        // max chars stored per entry (0 = unlimited)
	StoreMinPromptLen   int `json:"store_min_prompt_len"`   // prompts shorter than this are not stored
	StoreMinResponseLen int `json:"store_min_response_len"` // responses shorter than this are not stored

	// Confidence decay — older unreinforced memories fade during recall
	DecayHalfLifeDays float64 `json:"decay_half_life_days"` // half-life in days (0 = no decay, default 30)

	// Recall-time filtering
	RelevanceFloor       float64 `json:"relevance_floor"`         // Min weight to inject recalled entry (default 0.05)
	GlobalSummaryMaxChars int    `json:"global_summary_max_chars"` // Max chars for global track summary (default 4000)

	// Summarizer settings
	SummarizeMaxEntries    int `json:"summarize_max_entries"`      // max entries per track in --all mode (default 200)
	SummarizeMaxInputChars int `json:"summarize_max_input_chars"`  // max chars fed to LLM for summarization (default 50000)
	ClassifyMaxChars       int `json:"classify_max_chars"`         // max chars per entry for classification (default 500)
	CrossTrackMaxChars     int `json:"cross_track_max_chars"`      // max chars per summary for cross-track analysis (default 3000)
}

// OllamaConfig holds local LLM settings for summarization and embedding.
type OllamaConfig struct {
	BaseURL             string `json:"base_url"`              // e.g. http://localhost:11434
	Model               string `json:"model"`                 // e.g. qwen3:32b (for summarization)
	TimeoutMinutes      int    `json:"timeout_minutes"`       // HTTP timeout for generation (default 10)
	WedgeTimeoutSeconds int    `json:"wedge_timeout_seconds"` // Seconds without a token before declaring wedge (default 90)
	MaxRetries          int    `json:"max_retries"`           // Retries per generation after wedge/failure (default 2)
	EmbeddingModel      string `json:"embedding_model"`       // e.g. nomic-embed-text (for vector search)
	EmbeddingDimensions int    `json:"embedding_dimensions"`  // vector dimensions (default 768)
	CACertPath          string `json:"ca_cert"`               // path to PEM CA bundle for TLS (e.g. /etc/pki/tls/certs/private-ca.pem)
	TLSInsecure         bool   `json:"tls_insecure"`          // skip TLS verification (not recommended)
}

// IngestConfig holds web ingestion pipeline settings.
type IngestConfig struct {
	// Safety thresholds
	RejectThreshold   float64 `json:"reject_threshold"`   // Risk score >= this → reject (default 0.8)
	SanitizeThreshold float64 `json:"sanitize_threshold"` // Risk score >= this → sanitize (default 0.5)
	SafetyThreshold   float64 `json:"safety_threshold"`   // Below this = considered safe (default 0.5)

	// Instruction density heuristic
	DensityRatio    float64 `json:"density_ratio"`     // Instruction sentence ratio trigger (default 0.3)
	DensityMinCount int     `json:"density_min_count"` // Min instruction-like sentences to trigger (default 3)

	// Content weights (recall priority)
	WebContentWeight float64 `json:"web_content_weight"` // Weight for full web content entries (default 0.3)
	StubWeight       float64 `json:"stub_weight"`        // Weight for stub/pointer entries (default 0.6)

	// Extraction settings
	FetchTimeoutS int   `json:"fetch_timeout_s"` // HTTP fetch timeout in seconds (default 30)
	MaxBodyBytes  int64 `json:"max_body_bytes"`  // Max HTML download size (default 10MB)
	UserAgent     string `json:"user_agent,omitempty"` // Custom User-Agent (empty = default browser UA)

	// Chunking
	MaxChunkSize int `json:"max_chunk_size"` // Max characters per chunk (default 3000)
	MinChunkSize int `json:"min_chunk_size"` // Min chunk size before merging (default 200)
}

// ConsolidationConfig holds pairwise relevance discovery settings.
type ConsolidationConfig struct {
	PairsPerRun       int     `json:"pairs_per_run"`       // Pairs evaluated per cycle (default 5)
	MinScore          float64 `json:"min_score"`           // Minimum |score| to create link (default 0.4)
	DriftDelta        float64 `json:"drift_delta"`         // Score change threshold to update existing link (default 0.2)
	CyclePauseS       int     `json:"cycle_pause_s"`       // Seconds between cycles (default 600)
	MaxEntries        int     `json:"max_entries"`         // Max entries sampled (default 500)
	ContentTruncation int     `json:"content_truncation"`  // Chars per entry in LLM prompt (default 500)
	MinContentLength  int     `json:"min_content_length"`  // Min content length to consider linkable (default 50)
	Temperature       float64 `json:"temperature"`         // LLM temperature for scoring (default 0.1)
	MaxTokens         int     `json:"max_tokens"`          // Max LLM response tokens (default 200)
	EvalTimeoutS      int     `json:"eval_timeout_s"`      // Per-pair LLM eval timeout in seconds (default 60)
	CooldownTTLS      int     `json:"cooldown_ttl_s"`      // Seconds before re-consolidating (default 3600)
	DiscoveryMinLen   int     `json:"discovery_min_len"`   // Min content chars for random discovery (default 200)
}

// HookConfig holds recall/store hook settings.
type HookConfig struct {
	TimeoutS          int     `json:"timeout_s"`              // Redis operation timeout (default 5)
	BootPhaseTTLH     int     `json:"boot_phase_ttl_h"`       // Hours before re-injecting full orientation (default 24)
	MaxLinkHops       int     `json:"max_link_hops"`          // DEPRECATED: use LinkFollow.MaxHops
	HookTimeoutMs     int     `json:"hook_timeout_ms"`        // Timeout written to generated agent config (default 3000)
	LinkBudgetChars   int     `json:"link_budget_chars"`      // DEPRECATED: use LinkFollow + RRF fusion
	LinkBudgetEntries int     `json:"link_budget_entries"`    // DEPRECATED: use LinkFollow.TopK
	MinLinkFollowScore float64 `json:"min_link_follow_score"` // DEPRECATED: use LinkFollow.MinScore
	Tier2MaxChars     int     `json:"tier2_max_chars"`        // Condensed content length for tier 2 (default 300)
	Tier3SnippetChars int     `json:"tier3_snippet_chars"`    // Breadcrumb snippet length for tier 3 (default 80)
	Tier1Ratio        float64 `json:"tier1_ratio"`            // Fraction of results injected as full text (default 0.2, ceil'd, min 1)
	Tier2Ratio        float64 `json:"tier2_ratio"`            // Fraction of results injected as summary/condensed (default 0.3, ceil'd)
	// Tier 3 = remainder (1.0 - tier1_ratio - tier2_ratio)
	VibeMaxExchanges  int     `json:"vibe_max_exchanges"`     // Max vibe exchanges stored (default 6)
	VibeTruncateChars int     `json:"vibe_truncate_chars"`    // Vibe text truncation per entry (default 200)
	LinkFollow        LinkFollowConfig `json:"link_follow"`   // Graph traversal channel settings
	RRFConstant       int     `json:"rrf_constant"`           // RRF k constant: score = 1/(k + rank). Higher = more equal weighting (default 60)
}

// LinkFollowConfig controls graph traversal as a retrieval channel in the recall hook.
// Links are traversed by |score| (absolute value) for ranking purposes, but the
// original sign is preserved through to the output — negative-scored links surface
// as anti-memories (prior dead ends, rejected approaches, superseded decisions).
type LinkFollowConfig struct {
	Enabled       bool    `json:"enabled"`        // Enable graph traversal channel (default true)
	MaxHops       int     `json:"max_hops"`       // Max traversal depth (default 2)
	DecayFactor   float64 `json:"decay_factor"`   // Score multiplier per hop — exponential death for weak paths (default 0.5)
	MinScore      float64 `json:"min_score"`      // Min |score| post-decay to keep candidate (default 0.4)
	MaxCandidates int     `json:"max_candidates"` // Max candidates to collect before ranking (default 100)
	TopK          int     `json:"top_k"`          // Survivors emitted into RRF fusion (default 10)
}

// MCPConfig holds MCP server settings.
type MCPConfig struct {
	DefaultSearchLimit    int `json:"default_search_limit"`     // Default results for memory_search (default 10)
	DefaultTagLimit       int `json:"default_tag_limit"`        // Default results for memory_by_tags (default 20)
	DefaultTimeRangeLimit int `json:"default_time_range_limit"` // Default results for memory_time_range (default 20)
}

// SlackConfig holds Slack bot integration settings.
type SlackConfig struct {
	BotToken string         `json:"bot_token,omitempty"` // xoxb-... Bot User OAuth Token
	AppToken string         `json:"app_token,omitempty"` // xapp-... App-Level Token (Socket Mode)
	Channels []SlackChannel `json:"channels,omitempty"`  // Channels to monitor
}

// SlackChannel represents a single monitored Slack channel or DM.
type SlackChannel struct {
	ID       string   `json:"id"`                // Slack channel ID (e.g. C0123ABC)
	Name     string   `json:"name"`              // Human-readable name (e.g. #engineering)
	Mode     string   `json:"mode,omitempty"`    // "archive" (default, silent) or "active" (responds to mentions)
	Tags     []string `json:"tags,omitempty"`    // Auto-applied tags for all messages from this channel
	Backfill bool     `json:"backfill,omitempty"` // If true, incrementally ingest channel history
}

// LogConfig holds logging settings for all Hippocampus binaries.
type LogConfig struct {
	LogDir      string `json:"log_dir"`       // Directory for log files (default: ~/Library/Logs/Hippocampus on macOS, ~/.local/share/hippocampus/logs on Linux/FreeBSD)
	Level       string `json:"level"`         // Minimum log level: debug, info, warn, error (default: "info")
	DebugFile   bool   `json:"debug_file"`    // If true, write a separate <module>-debug.log at debug level (default: false)
	AlsoStderr  bool   `json:"also_stderr"`   // If true, also write to stderr (default: true)
}

// WorkingSetConfig holds settings for the Working Set Tracker sidecar.
type WorkingSetConfig struct {
	Enabled       bool   `json:"enabled"`                  // Enable working set tracking
	Model         string `json:"model"`                    // Ollama model for sidecar summarization (e.g. qwen3:1.7b)
	MaxBullets    int    `json:"max_bullets"`              // Max bullet points in working set (default 5)
	MaxChars      int    `json:"max_chars"`                // Max chars in working set (default 500)
	TimeoutS      int    `json:"timeout_s"`                // Timeout for sidecar LLM call in seconds (default 120)
	InheritTTLH   int    `json:"inherit_ttl_h"`            // Hours to inherit from previous session (default 24)
}

// OFCConfig holds settings for the Orbitofrontal Cortex (neuromodulator) module.
type OFCConfig struct {
	Enabled            bool    `json:"enabled"`               // Enable OFC module (also togglable via --ofc flag)
	Model              string  `json:"model"`                 // Ollama model for sentiment analysis (e.g. qwen3:8b). Empty = regex-only fallback.
	ClassifyTimeoutS   int     `json:"classify_timeout_s"`    // OFC model timeout in seconds (default 3)
	DADecay            float64 `json:"da_decay"`              // DA decay rate per prompt (default 0.95, toward 0)
	SHTDecay           float64 `json:"sht_decay"`             // 5HT decay rate per prompt (default 0.98, toward baseline)
	SHTBaseline        float64 `json:"sht_baseline"`          // 5HT neutral baseline (default 0.5)
	DAExplicitPositive float64 `json:"da_explicit_positive"`  // DA bump on explicit positive signal (default 0.08)
	DAExplicitNegative float64 `json:"da_explicit_negative"`  // DA hit on explicit negative signal (default -0.12)
	DAImplicitPositive float64 `json:"da_implicit_positive"`  // DA bump on implicit positive (default 0.03)
	DAImplicitNegative float64 `json:"da_implicit_negative"`  // DA hit on implicit negative (default -0.05)
	SHTPositive        float64 `json:"sht_positive"`          // 5HT bump on positive signal (default 0.03)
	SHTNegative        float64 `json:"sht_negative"`          // 5HT hit on negative signal (default -0.04)
}

// EpistemicConfig holds settings for the fact-checking / epistemic analysis pipeline.
type EpistemicConfig struct {
	// Extraction
	MaxTextLen      int     `json:"max_text_len"`       // Max chars of source text to send to extractor (default 3000)
	MaxVocabTerms   int     `json:"max_vocab_terms"`    // Max vocabulary terms for reconciliation (default 50)
	MinEntryLen     int     `json:"min_entry_len"`      // Skip entries shorter than this (default 50)
	MaxKeywords     int     `json:"max_keywords"`       // Keyword cap for vocabulary lookup (default 30)
	MinKeywordLen   int     `json:"min_keyword_len"`    // Min keyword length to consider (default 4)

	// Verification
	MinEncounters   int     `json:"min_encounters"`     // Encounter count threshold to trigger verification (default 3)
	MaxVerifyBatch  int     `json:"max_verify_batch"`   // Max entries verified per run (default 20)
	AutoPruneConf   float64 `json:"auto_prune_conf"`    // Contested confidence >= this → auto-prune (default 0.90)
	ReinforceBoost  float64 `json:"reinforce_boost"`    // Confidence boost on recheck agreement (default 0.05)
	SourceContextMax int    `json:"source_context_max"` // Max chars per source entry in verification (default 500)
	MaxSourceEntries int    `json:"max_source_entries"` // Max source entries to gather for context (default 5)

	// Recall-time injection (hook)
	WarningConfMin  float64 `json:"warning_conf_min"`   // Only inject warnings with confidence >= this (default 0.70)
	WarningMinKeys  int     `json:"warning_min_keys"`   // Require >= N keyword matches to inject warning (default 2)
	MaxWarnings     int     `json:"max_warnings"`       // Max warnings injected per prompt (default 5)
	EvidenceTrunc   int     `json:"evidence_trunc"`      // Truncate evidence text at N chars (default 80)
	ResponseTrunc   int     `json:"response_trunc"`      // Truncate raw LLM response in error messages (default 300)

	// Structural filters
	VagueMaxLen     int     `json:"vague_max_len"`      // Terms <= this length (no underscore) are "vague" (default 6)
}

// OllamaOverride allows per-subsystem Ollama endpoint/model override.
// Empty fields fall through to the parent scope.
type OllamaOverride struct {
	BaseURL string `json:"base_url,omitempty"`
	Model   string `json:"model,omitempty"`
}

// DaemonConfig holds settings for the background processing daemon.
type DaemonConfig struct {
	Enabled           bool            `json:"enabled"`
	GPUConcurrency    int             `json:"gpu_concurrency"`    // max simultaneous Ollama calls (default 2)
	QueueKey          string          `json:"queue_key"`          // Redis list key (default "ingest:queue")
	BacklogBatch      int             `json:"backlog_batch"`      // entries per backlog cycle (default 50)
	BacklogPauseS     int             `json:"backlog_pause_s"`    // pause between backlog entries (default 5)
	CorecallThreshold int             `json:"corecall_threshold"` // co-recall count to auto-link (default 3)
	RecalledTTLH      int             `json:"recalled_ttl_h"`     // TTL for recalled:<id> sets in hours (default 24)
	IdlePollS         int             `json:"idle_poll_s"`        // Seconds between polling when idle (default 2)
	SelfUpdateCheckS  int             `json:"self_update_check_s"` // Binary mtime check interval (default 30)
	FailureThreshold  int             `json:"failure_threshold"`  // Consecutive Ollama failures before backoff (default 3)
	MaxBackoffS       int             `json:"max_backoff_s"`      // Maximum backoff cap in seconds (default 3600)
	BackoffBaseS      int             `json:"backoff_base_s"`     // Base backoff duration in seconds (default 10)
	Ollama            *OllamaOverride `json:"ollama,omitempty"`   // daemon-level override
	Classifier        SubsystemConfig `json:"classifier"`
	Extractor         SubsystemConfig `json:"extractor"`
	Verifier          SubsystemConfig `json:"verifier"`
	Linker            SubsystemConfig `json:"linker"`
	Condenser         CondenserConfig `json:"condenser"`
	TSA               TSAConfig       `json:"tsa"`
}

// TSAConfig controls RFC 3161 timestamping of memory entries.
type TSAConfig struct {
	Enabled    bool   `json:"enabled"`              // enable/disable TSA timestamping
	URL        string `json:"url"`                  // TSA endpoint (default "https://rfc3161.ai.moda")
	BatchSize  int    `json:"batch_size"`           // entries per Merkle block (default 256)
	IntervalS  int    `json:"interval_s"`           // seconds between TSA runs (default 3600 = hourly)
	HashAlgo   string `json:"hash_algo"`            // "sha256" (default) or "sha512"
}

// CondenserConfig controls per-message condensation (summary generation for individual entries).
type CondenserConfig struct {
	Enabled          bool            `json:"enabled"`              // enable/disable condensation
	MinUserChars     int             `json:"min_user_chars"`       // user prompt threshold (default 300)
	MinAssistantChars int            `json:"min_assistant_chars"`  // assistant response threshold (default 1500)
	MinOtherChars    int             `json:"min_other_chars"`      // other entry types threshold (default 500)
	MaxOutputChars   int             `json:"max_output_chars"`     // max condensed summary length (default 250)
	MaxInputChars    int             `json:"max_input_chars"`      // Input truncation for LLM prompt (default 2000)
	Ollama           *OllamaOverride `json:"ollama,omitempty"`
}

// SubsystemConfig allows per-subsystem Ollama routing and enablement control.
type SubsystemConfig struct {
	Enabled bool            `json:"enabled"`            // live-checked; daemon skips this subsystem if false
	Ollama  *OllamaOverride `json:"ollama,omitempty"`
}

// DefaultConfig returns a config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		Redis: RedisConfig{
			Addr:         "localhost:6379",
			DB:           0,
			MaxRetries:   3,
			DialTimeoutS: 5,
			PoolSize:     10,
		},
		Memory: MemoryConfig{
			MaxSearchResults:       20,
			RecallMaxChars:         8000,
			RecallMaxEntries:       10,
			RecallScanDepth:        100,
			StoreMaxChars:          0, // unlimited
			StoreMinPromptLen:      20,
			StoreMinResponseLen:    100,
			DecayHalfLifeDays:      30,
			RelevanceFloor:         0.05,
			GlobalSummaryMaxChars:  4000,
			SummarizeMaxEntries:    200,
			SummarizeMaxInputChars: 50000,
			ClassifyMaxChars:       500,
			CrossTrackMaxChars:     3000,
		},
		Ollama: OllamaConfig{
			BaseURL:             "http://localhost:11434",
			Model:               "qwen3:32b",
			TimeoutMinutes:      10,
			WedgeTimeoutSeconds: 90,
			MaxRetries:          2,
			EmbeddingModel:      "nomic-embed-text",
			EmbeddingDimensions: 768,
		},
		Ingest: IngestConfig{
			RejectThreshold:   0.8,
			SanitizeThreshold: 0.5,
			SafetyThreshold:   0.5,
			DensityRatio:      0.3,
			DensityMinCount:   3,
			WebContentWeight:  0.3,
			StubWeight:        0.6,
			FetchTimeoutS:     30,
			MaxBodyBytes:      10 * 1024 * 1024,
			MaxChunkSize:      3000,
			MinChunkSize:      200,
		},
		Consolidation: ConsolidationConfig{
			PairsPerRun:       5,
			MinScore:          0.4,
			DriftDelta:        0.2,
			CyclePauseS:       600,
			MaxEntries:        500,
			ContentTruncation: 500,
			MinContentLength:  50,
			Temperature:       0.1,
			MaxTokens:         200,
			EvalTimeoutS:      60,
			CooldownTTLS:      3600,
			DiscoveryMinLen:   200,
		},
		Hook: HookConfig{
			TimeoutS:           10,
			BootPhaseTTLH:      24,
			MaxLinkHops:        3, // deprecated, kept for back-compat
			HookTimeoutMs:      10000,
			LinkBudgetChars:    3000,  // deprecated
			LinkBudgetEntries:  3,     // deprecated
			MinLinkFollowScore: 0.3,   // deprecated
			Tier2MaxChars:      300,
			Tier3SnippetChars:  80,
			Tier1Ratio:         0.2,
			Tier2Ratio:         0.3,
			VibeMaxExchanges:   6,
			VibeTruncateChars:  200,
			RRFConstant:        60,
			LinkFollow: LinkFollowConfig{
				Enabled:       true,
				MaxHops:       2,
				DecayFactor:   0.5,
				MinScore:      0.4,
				MaxCandidates: 100,
				TopK:          10,
			},
		},
		MCP: MCPConfig{
			DefaultSearchLimit:    10,
			DefaultTagLimit:       20,
			DefaultTimeRangeLimit: 20,
		},
		WorkingSet: WorkingSetConfig{
			Enabled:     false,
			Model:       "qwen3:1.7b",
			MaxBullets:  5,
			MaxChars:    500,
			TimeoutS:    120,
			InheritTTLH: 24,
		},
		Log: LogConfig{
			LogDir:     "", // empty = auto-detect based on OS at runtime
			Level:      "info",
			DebugFile:  false,
			AlsoStderr: true,
		},
		OFC: OFCConfig{
			Model:              "qwen3:8b",
			ClassifyTimeoutS:   3,
			DADecay:            0.95,
			SHTDecay:           0.98,
			SHTBaseline:        0.5,
			DAExplicitPositive: 0.08,
			DAExplicitNegative: -0.12,
			DAImplicitPositive: 0.03,
			DAImplicitNegative: -0.05,
			SHTPositive:        0.03,
			SHTNegative:        -0.04,
		},
		Epistemic: EpistemicConfig{
			MaxTextLen:       3000,
			MaxVocabTerms:    50,
			MinEntryLen:      50,
			MaxKeywords:      30,
			MinKeywordLen:    4,
			MinEncounters:    3,
			MaxVerifyBatch:   20,
			AutoPruneConf:    0.90,
			ReinforceBoost:   0.05,
			SourceContextMax: 500,
			MaxSourceEntries: 5,
			WarningConfMin:   0.70,
			WarningMinKeys:   2,
			MaxWarnings:      5,
			EvidenceTrunc:    80,
			ResponseTrunc:    300,
			VagueMaxLen:      6,
		},
		Daemon: DaemonConfig{
			Enabled:           false,
			GPUConcurrency:    2,
			QueueKey:          "ingest:queue",
			BacklogBatch:      50,
			BacklogPauseS:     5,
			CorecallThreshold: 3,
			RecalledTTLH:      24,
			IdlePollS:         2,
			SelfUpdateCheckS:  30,
			FailureThreshold:  3,
			MaxBackoffS:       3600,
			BackoffBaseS:      10,
			Classifier:        SubsystemConfig{Enabled: true},
			Extractor:         SubsystemConfig{Enabled: true},
			Verifier:          SubsystemConfig{Enabled: true},
			Linker:            SubsystemConfig{Enabled: true},
			Condenser: CondenserConfig{
				Enabled:           true,
				MinUserChars:      300,
				MinAssistantChars: 1500,
				MinOtherChars:     500,
				MaxOutputChars:    250,
				MaxInputChars:     2000,
			},
		},
	}
}

// ResolveOllama returns the effective Ollama base_url and model for a subsystem.
// Resolution order: subsystem override → daemon override → global config.
func (c *Config) ResolveOllama(subsystem *OllamaOverride) (baseURL, model string) {
	baseURL = c.Ollama.BaseURL
	model = c.Ollama.Model

	// Daemon-level override
	if c.Daemon.Ollama != nil {
		if c.Daemon.Ollama.BaseURL != "" {
			baseURL = c.Daemon.Ollama.BaseURL
		}
		if c.Daemon.Ollama.Model != "" {
			model = c.Daemon.Ollama.Model
		}
	}

	// Subsystem-level override
	if subsystem != nil {
		if subsystem.BaseURL != "" {
			baseURL = subsystem.BaseURL
		}
		if subsystem.Model != "" {
			model = subsystem.Model
		}
	}

	return baseURL, model
}

// ResolveLogDir returns the effective log directory, auto-detecting based on OS if not configured.
func (c *Config) ResolveLogDir() string {
	if c.Log.LogDir != "" {
		return c.Log.LogDir
	}
	homeDir, _ := os.UserHomeDir()
	if runtime.GOOS == "darwin" {
		return filepath.Join(homeDir, "Library", "Logs", "Hippocampus")
	}
	return filepath.Join(homeDir, ".local", "share", "hippocampus", "logs")
}

// FindConfigPath returns the path to the Hippocampus config file by checking,
// in order: HIPPOCAMPUS_CONFIG env var, macOS Application Support, ~/.config,
// CWD, and /etc. If none exist, it returns a platform-appropriate default.
func FindConfigPath() string {
	if envPath := os.Getenv("HIPPOCAMPUS_CONFIG"); envPath != "" {
		return envPath
	}

	homeDir, _ := os.UserHomeDir()
	candidates := []string{
		homeDir + "/Library/Application Support/Hippocampus/config.json", // macOS app
		homeDir + "/.config/hippocampus/config.json",                     // Unix/Linux
		"config.json",                                                    // CWD fallback
		"/etc/hippocampus/config.json",                                   // system-wide
	}

	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}

	// Default to macOS path on darwin, Unix otherwise
	if runtime.GOOS == "darwin" {
		return homeDir + "/Library/Application Support/Hippocampus/config.json"
	}
	return homeDir + "/.config/hippocampus/config.json"
}

// Load reads config from a JSON file, falling back to defaults for missing fields.
func Load(path string) (Config, error) {
	cfg := DefaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return cfg, fmt.Errorf("reading config: %w", err)
	}

	if err := json.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("parsing config: %w", err)
	}

	return cfg, nil
}

// Save writes config to a JSON file.
func Save(path string, cfg Config) error {
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling config: %w", err)
	}
	return os.WriteFile(path, data, 0o644)
}
