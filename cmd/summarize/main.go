// Copyright (c) 2026 Ruthlessly Practical LLC. All rights reserved.
// Use of this source code is governed by a BSD-3-Clause license
// that can be found in the LICENSE file.

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/ruthlesslypractical/hippocampus/internal/config"
	"github.com/ruthlesslypractical/hippocampus/internal/epistemic"
	"github.com/ruthlesslypractical/hippocampus/internal/logging"
	"github.com/ruthlesslypractical/hippocampus/internal/memory"
	"github.com/ruthlesslypractical/hippocampus/internal/ollama"
	"github.com/ruthlesslypractical/hippocampus/pkg/classify"
)

const (
	entryPrefix = "entry:"
	tagPrefix   = "tag:"
	allTagsKey  = "tags:all"
	timelineKey = "timeline"
)

type entry struct {
	ID        string
	Content   string
	Tags      []string
	Timestamp int64
}

func main() {
	// Check for --config flag early (before other arg parsing)
	configPath := ""
	for i, arg := range os.Args[1:] {
		if arg == "--config" && i+1 < len(os.Args[1:]) {
			configPath = os.Args[i+2]
		}
	}
	if configPath == "" {
		configPath = config.FindConfigPath()
	}
	slog.Info("using config", "path", configPath)
	cfg, err := config.Load(configPath)
	if err != nil {
		slog.Error("loading config failed", "err", err)
		os.Exit(1)
	}

	cleanupLog := logging.Setup(cfg, "summarize")
	defer cleanupLog()
	slog.Info("summarize starting", "config", configPath)
	slog.Info("ollama configured", "url", cfg.Ollama.BaseURL)

	// --- Flag parsing ---
	var (
		flagBackfill   bool
		flagFillGaps   bool
		flag3h         bool
		flagDaily      bool
		flagWeekly     bool
		flagGlobal     bool
		flagAll        bool
		flagCrossTrack bool
		flagClassify   bool
		flagVerify     bool
		flagRandom     bool
		flagPurge      bool
		flagDryRun     bool
		flagForce      bool
		track          string
		modelOverride  string
		batchSize      = 20
	)

	for i, arg := range os.Args[1:] {
		switch arg {
		case "--backfill":
			flagBackfill = true
		case "--fill-gaps":
			flagFillGaps = true
		case "--3h":
			flag3h = true
		case "--daily":
			flagDaily = true
		case "--weekly":
			flagWeekly = true
		case "--global":
			flagGlobal = true
		case "--all":
			flagAll = true
		case "--cross-track":
			flagCrossTrack = true
		case "--classify":
			flagClassify = true
		case "--verify":
			flagVerify = true
		case "--random":
			flagRandom = true
		case "--purge":
			flagPurge = true
		case "--dry-run":
			flagDryRun = true
		case "--force":
			flagForce = true
		case "--track":
			if i+1 < len(os.Args[1:]) {
				track = os.Args[i+2]
			}
		case "--model":
			if i+1 < len(os.Args[1:]) {
				modelOverride = os.Args[i+2]
			}
		case "--batch-size":
			if i+1 < len(os.Args[1:]) {
				fmt.Sscanf(os.Args[i+2], "%d", &batchSize)
			}
		case "--help", "-h":
			fmt.Printf("hippocampus-summarize v%s\n\n", config.Version)
			fmt.Println("Usage: hippocampus-summarize [FLAGS]")
			fmt.Println()
			fmt.Println("Summarization levels:")
			fmt.Println("  --3h           Generate 3h block summaries (default if no level flag)")
			fmt.Println("  --daily        Generate daily rollups from 3h summaries")
			fmt.Println("  --weekly       Generate weekly rollups from dailies")
			fmt.Println("  --global       Generate global track summary (capped ~4000 chars)")
			fmt.Println("  --all          Do 3h + daily + weekly + global")
			fmt.Println("  --cross-track  Also do cross-track theme analysis (additive)")
			fmt.Println()
			fmt.Println("Modes:")
			fmt.Println("  --backfill     Regenerate summaries from beginning of time (use 'admin summary wipe' first to clear)")
			fmt.Println("  --fill-gaps    Scan from first entry forward and generate only missing windows")
			fmt.Println("  --classify     Classify unclassified entries (or --force to reclassify)")
			fmt.Println("  --verify       Run epistemic verification (encounter >= 3)")
			fmt.Println("  --random       Random recheck: sample N entries, re-verify")
			fmt.Println("  --purge        Purge dead epistemic keys")
			fmt.Println()
			fmt.Println("Options:")
			fmt.Println("  --track X      Limit to one track")
			fmt.Println("  --model X      Override Ollama model")
			fmt.Println("  --config X     Config file path")
			fmt.Println("  --batch-size N Entries per classification batch (default 20)")
			fmt.Println("  --dry-run      Show what would be done without writing")
			fmt.Println("  --force        Force regeneration even if summary exists")
			os.Exit(0)
		}
	}

	// Default: if no level flag given and not a special mode, default to --3h
	if !flag3h && !flagDaily && !flagWeekly && !flagGlobal && !flagAll &&
		!flagClassify && !flagVerify && !flagRandom && !flagPurge {
		flag3h = true
	}

	// --all expands to all levels
	if flagAll {
		flag3h = true
		flagDaily = true
		flagWeekly = true
		flagGlobal = true
	}

	store, err := memory.NewRedisStore(cfg.Redis, nil)
	if err != nil {
		slog.Error("connecting to Valkey failed", "err", err)
		os.Exit(1)
	}
	client := store.Client()
	defer store.Close()

	// No global timeout for summarization — individual Ollama calls handle their own
	// timeouts via streaming + wedge detection. Classification also runs unbounded.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ollamaClient := ollama.NewFromConfig(cfg.Ollama.BaseURL, cfg.Ollama.Model, cfg.Ollama.CACertPath, cfg.Ollama.TLSInsecure, cfg.Ollama.TimeoutMinutes)
	ollamaClient.WedgeTimeout = time.Duration(cfg.Ollama.WedgeTimeoutSeconds) * time.Second
	if flagClassify {
		classifyTimeout := cfg.Ollama.TimeoutMinutes
		if classifyTimeout < 30 {
			classifyTimeout = 30
		}
		ollamaClient = ollama.NewFromConfig(cfg.Ollama.BaseURL, cfg.Ollama.Model, cfg.Ollama.CACertPath, cfg.Ollama.TLSInsecure, classifyTimeout)
		ollamaClient.WedgeTimeout = time.Duration(cfg.Ollama.WedgeTimeoutSeconds) * time.Second
	}
	if modelOverride != "" {
		timeout := cfg.Ollama.TimeoutMinutes
		if flagClassify && timeout < 30 {
			timeout = 30
		}
		ollamaClient = ollama.NewFromConfig(cfg.Ollama.BaseURL, modelOverride, cfg.Ollama.CACertPath, cfg.Ollama.TLSInsecure, timeout)
		ollamaClient.WedgeTimeout = time.Duration(cfg.Ollama.WedgeTimeoutSeconds) * time.Second
		slog.Info("using model override", "model", modelOverride)
	}
	slog.Debug("ollama connected", "url", cfg.Ollama.BaseURL, "model", cfg.Ollama.Model)

	// --- Dispatch: separate modes first ---
	switch {
	case flagVerify:
		slog.Info("starting operation", "op", "verify")
		opStart := time.Now()
		epistemicCtx, epistemicCancel := context.WithCancel(context.Background())
		defer epistemicCancel()
		verifier := epistemic.NewVerifier(client, ollamaClient, cfg.Epistemic, flagDryRun)
		minEncounters := cfg.Epistemic.MinEncounters
		maxEntries := cfg.Epistemic.MaxVerifyBatch
		if err := verifier.RunVerification(epistemicCtx, minEncounters, maxEntries, flagForce); err != nil {
			slog.Error("epistemic verification failed", "err", err)
			os.Exit(1)
		}
		if !flagDryRun {
			registry := epistemic.NewRegistry(client)
			purged, err := registry.PurgePruned(epistemicCtx)
			if err != nil {
				slog.Warn("purge failed", "err", err)
			} else if purged > 0 {
				slog.Info("purged dead entries from Redis", "count", purged)
			}
		}
		slog.Info("operation complete", "op", "verify", "duration", time.Since(opStart))
		return

	case flagPurge:
		epistemicCtx, epistemicCancel := context.WithCancel(context.Background())
		defer epistemicCancel()
		registry := epistemic.NewRegistry(client)
		if flagDryRun {
			count, _ := client.SCard(epistemicCtx, "epistemic:status:pruned").Result()
			slog.Info("dry run: would purge entries", "count", count)
		} else {
			purged, err := registry.PurgePruned(epistemicCtx)
			if err != nil {
				slog.Error("purge failed", "err", err)
				os.Exit(1)
			}
			slog.Info("purged dead entries from Redis", "count", purged)
		}
		return

	case flagRandom:
		epistemicCtx, epistemicCancel := context.WithCancel(context.Background())
		defer epistemicCancel()
		verifier := epistemic.NewVerifier(client, ollamaClient, cfg.Epistemic, flagDryRun)
		n := batchSize
		if n <= 0 {
			n = 5
		}
		if err := verifier.RunRandomRecheck(epistemicCtx, n); err != nil {
			slog.Error("epistemic random recheck failed", "err", err)
			os.Exit(1)
		}
		return

	case flagClassify:
		slog.Info("starting operation", "op", "classify")
		opStart := time.Now()
		runClassify(ctx, client, ollamaClient, track, flagForce, flagDryRun, cfg.Memory.ClassifyMaxChars, batchSize)
		slog.Info("operation complete", "op", "classify", "duration", time.Since(opStart))
		return
	}

	// --- Summarization dispatch ---
	slog.Info("starting summarization", "backfill", flagBackfill, "3h", flag3h, "daily", flagDaily, "weekly", flagWeekly, "global", flagGlobal, "cross-track", flagCrossTrack)
	opStart := time.Now()

	tracks := discoverTracks(ctx, client, track)
	if len(tracks) == 0 {
		slog.Info("no tracks found to summarize")
		return
	}
	slog.Info("discovered tracks", "count", len(tracks), "tracks", tracks)

	// Determine start time
	var startTime time.Time
	if flagBackfill || flagFillGaps {
		// Start from earliest entry
		// --backfill: expects wipe first (regenerates everything)
		// --fill-gaps: skips existing summaries, only generates missing windows
		earliest, err := client.ZRangeByScoreWithScores(ctx, timelineKey, &redis.ZRangeBy{
			Min: "-inf", Max: "+inf", Count: 1, Offset: 0,
		}).Result()
		if err != nil || len(earliest) == 0 {
			slog.Info("no entries in timeline")
			return
		}
		startTime = time.Unix(int64(earliest[0].Score), 0)
	} else {
		// Incremental: find last summary timestamp
		startTime = findLastSummaryTime(ctx, client, tracks, flag3h, flagDaily, flagWeekly)
	}

	slog.Info("processing from", "start", startTime.Format("2006-01-02 15:04"))

	// Process hierarchically: 3h → daily → weekly → global
	if flag3h {
		run3hBlocks(ctx, client, ollamaClient, store, tracks, cfg, startTime, flagDryRun)
	}
	if flagDaily {
		runDailyRollups(ctx, client, ollamaClient, store, tracks, cfg, startTime, flagDryRun)
	}
	if flagWeekly {
		runWeeklyRollups(ctx, client, ollamaClient, store, tracks, cfg, startTime, flagDryRun)
	}
	if flagGlobal {
		runGlobal(ctx, client, ollamaClient, store, tracks, cfg, flagDryRun)
	}
	if flagCrossTrack {
		runCrossTrackReview(ctx, client, store, ollamaClient, cfg.Memory.CrossTrackMaxChars, cfg.Ollama.MaxRetries, flagDryRun)
	}

	slog.Info("summarization complete", "duration", time.Since(opStart))
}

// --- New summarization functions ---

func findLastSummaryTime(ctx context.Context, client *redis.Client, tracks []string, do3h, doDaily, doWeekly bool) time.Time {
	// Find the most recent summary timestamp across requested levels
	var latestTS int64

	for _, t := range tracks {
		var prefixes []string
		if do3h {
			prefixes = append(prefixes, "summary:3h:"+t)
		}
		if doDaily {
			prefixes = append(prefixes, "summary:daily:"+t)
		}
		if doWeekly {
			prefixes = append(prefixes, "summary:weekly:"+t)
		}

		for _, prefix := range prefixes {
			ids, err := client.SMembers(ctx, tagPrefix+prefix).Result()
			if err != nil {
				continue
			}
			for _, id := range ids {
				ts, err := client.HGet(ctx, entryPrefix+id, "timestamp").Int64()
				if err != nil {
					continue
				}
				if ts > latestTS {
					latestTS = ts
				}
			}
		}
	}

	if latestTS > 0 {
		return time.Unix(latestTS, 0)
	}
	// No summaries found — start from 24h ago as a reasonable default
	return time.Now().Add(-24 * time.Hour)
}

func wipeSummaries(ctx context.Context, client *redis.Client, tracks []string) {
	trackSet := make(map[string]bool)
	for _, t := range tracks {
		trackSet[t] = true
	}

	ids, err := client.ZRange(ctx, timelineKey, 0, -1).Result()
	if err != nil {
		slog.Error("failed to read timeline for wipe", "err", err)
		return
	}

	var deleted int
	for _, id := range ids {
		if !strings.HasPrefix(id, "summary:") {
			continue
		}

		// If scoped to tracks, check membership
		if len(trackSet) > 0 {
			tagsStr, _ := client.HGet(ctx, entryPrefix+id, "tags").Result()
			belongs := false
			for _, tag := range strings.Split(tagsStr, ",") {
				tag = strings.TrimSpace(tag)
				if strings.HasPrefix(tag, "track:") {
					trk := strings.TrimPrefix(tag, "track:")
					if trackSet[trk] {
						belongs = true
						break
					}
				}
			}
			if !belongs {
				continue
			}
		}

		// Get tags and clean up
		tagsStr, _ := client.HGet(ctx, entryPrefix+id, "tags").Result()
		if tagsStr != "" {
			for _, tag := range strings.Split(tagsStr, ",") {
				tag = strings.TrimSpace(tag)
				if tag != "" {
					client.SRem(ctx, tagPrefix+tag, id)
				}
			}
		}
		client.Del(ctx, entryPrefix+id)
		client.ZRem(ctx, timelineKey, id)
		client.Do(ctx, "VREM", "hippocampus:vectors", id) // the daemon embeds summaries too
		deleted++
	}

	slog.Info("wiped summaries", "count", deleted)
}

func hasEntriesInWindow(ctx context.Context, client *redis.Client, track string, start, end int64) bool {
	// Get entry IDs in time range
	ids, err := client.ZRangeByScore(ctx, timelineKey, &redis.ZRangeBy{
		Min: fmt.Sprintf("%d", start),
		Max: fmt.Sprintf("%d", end),
	}).Result()
	if err != nil || len(ids) == 0 {
		return false
	}

	// Check if any of these entries belong to the track (track:X or track_auto:X)
	trackTag := tagPrefix + "track:" + track
	trackAutoTag := tagPrefix + "track_auto:" + track

	for _, id := range ids {
		isMember, _ := client.SIsMember(ctx, trackTag, id).Result()
		if isMember {
			return true
		}
		isMember, _ = client.SIsMember(ctx, trackAutoTag, id).Result()
		if isMember {
			return true
		}
	}
	return false
}

func run3hBlocks(ctx context.Context, client *redis.Client, ollamaClient *ollama.Client, store *memory.RedisStore, tracks []string, cfg config.Config, startTime time.Time, dryRun bool) {
	loc := time.Now().Location()
	now := time.Now()

	// Align start to 3h boundary in local time
	alignedStart := time.Date(startTime.Year(), startTime.Month(), startTime.Day(),
		(startTime.Hour()/3)*3, 0, 0, 0, loc)

	var generated int
	for windowStart := alignedStart; windowStart.Before(now); windowStart = windowStart.Add(3 * time.Hour) {
		windowEnd := windowStart.Add(3 * time.Hour)
		if windowEnd.After(now) {
			break
		}

		for _, t := range tracks {
			// Skip empty blocks without calling Ollama
			if !hasEntriesInWindow(ctx, client, t, windowStart.Unix(), windowEnd.Unix()) {
				continue
			}

			windowKey := windowStart.Format("2006-01-02-15")
			entryID := fmt.Sprintf("summary:3h:%s:%s", t, windowKey)

			// Check if already exists
			exists, _ := client.Exists(ctx, entryPrefix+entryID).Result()
			if exists > 0 {
				continue
			}

			entries := getTrackEntriesByTime(ctx, client, t, windowStart.Unix(), windowEnd.Unix())
			if len(entries) == 0 {
				continue
			}

			if dryRun {
				slog.Info("3h: would generate", "entry_id", entryID, "entries", len(entries))
				generated++
				continue
			}

			slog.Info("3h: generating", "entry_id", entryID, "entries", len(entries))
			summary, err := generateSummary(ctx, ollamaClient, t, entries, cfg.Memory.SummarizeMaxInputChars, cfg.Ollama.MaxRetries)
			if err != nil {
				slog.Error("3h: generation failed", "track", t, "window", windowKey, "err", err)
				continue
			}

			tags := []string{
				"track:" + t,
				strings.ToLower(t),
				"summary:3h:" + t,
				"summary:3h",
				"date:" + windowStart.Format("2006-01-02"),
			}
			store.Put(ctx, memory.Entry{
				ID:        entryID,
				Content:   summary,
				Tags:      tags,
				Timestamp: windowEnd,
			})

			// Record source entry IDs for dependency tracking (not formal links —
			// these are for summary analysis: "what entries produced this summary?")
			if len(entries) > 0 {
				sourceIDs := make([]string, 0, len(entries))
				for _, e := range entries {
					sourceIDs = append(sourceIDs, e.ID)
				}
				client.HSet(ctx, entryPrefix+entryID, "sources", strings.Join(sourceIDs, ","))
			}

			generated++
		}
	}
	slog.Info("3h blocks complete", "generated", generated)
}

func runDailyRollups(ctx context.Context, client *redis.Client, ollamaClient *ollama.Client, store *memory.RedisStore, tracks []string, cfg config.Config, startTime time.Time, dryRun bool) {
	loc := time.Now().Location()
	now := time.Now()

	alignedDay := time.Date(startTime.Year(), startTime.Month(), startTime.Day(), 0, 0, 0, 0, loc)

	var generated int
	for day := alignedDay; day.Before(now); day = day.AddDate(0, 0, 1) {
		dayEnd := day.AddDate(0, 0, 1)
		if dayEnd.After(now) {
			break
		}

		for _, t := range tracks {
			if !hasEntriesInWindow(ctx, client, t, day.Unix(), dayEnd.Unix()) {
				continue
			}

			dateStr := day.Format("2006-01-02")
			entryID := fmt.Sprintf("summary:daily:%s:%s", t, dateStr)

			exists, _ := client.Exists(ctx, entryPrefix+entryID).Result()
			if exists > 0 {
				continue
			}

			// Try to roll up from 3h summaries first
			entries := get3hSummariesForDay(ctx, client, t, dateStr)
			if len(entries) == 0 {
				// Fall back to raw entries
				entries = getTrackEntriesByTime(ctx, client, t, day.Unix(), dayEnd.Unix())
			}
			if len(entries) == 0 {
				continue
			}

			if dryRun {
				slog.Info("daily: would generate", "entry_id", entryID, "sources", len(entries))
				generated++
				continue
			}

			// Promotion: if only one source summary exists, clone it rather than re-summarizing
			if len(entries) == 1 {
				slog.Info("daily: promoting single source", "entry_id", entryID, "source", entries[0].ID)
				tags := []string{
					"track:" + t,
					strings.ToLower(t),
					"summary:daily:" + t,
					"summary:daily",
					"date:" + dateStr,
				}
				store.Put(ctx, memory.Entry{
					ID:        entryID,
					Content:   entries[0].Content,
					Tags:      tags,
					Timestamp: dayEnd,
				})
				client.HSet(ctx, entryPrefix+entryID, "sources", entries[0].ID)
				generated++
				continue
			}

			slog.Info("daily: generating", "entry_id", entryID, "sources", len(entries))
			summary, err := generateSummary(ctx, ollamaClient, t, entries, cfg.Memory.SummarizeMaxInputChars, cfg.Ollama.MaxRetries)
			if err != nil {
				slog.Error("daily: generation failed", "track", t, "date", dateStr, "err", err)
				continue
			}

			tags := []string{
				"track:" + t,
				strings.ToLower(t),
				"summary:daily:" + t,
				"summary:daily",
				"date:" + dateStr,
			}
			store.Put(ctx, memory.Entry{
				ID:        entryID,
				Content:   summary,
				Tags:      tags,
				Timestamp: dayEnd,
			})

			// Record source entry IDs for dependency tracking
			sourceIDs := make([]string, 0, len(entries))
			for _, e := range entries {
				sourceIDs = append(sourceIDs, e.ID)
			}
			client.HSet(ctx, entryPrefix+entryID, "sources", strings.Join(sourceIDs, ","))

			generated++
		}
	}
	slog.Info("daily rollups complete", "generated", generated)
}

func get3hSummariesForDay(ctx context.Context, client *redis.Client, track, dateStr string) []entry {
	// Look for 3h summaries tagged with this track and date
	summaryTag := "summary:3h:" + track
	dateTag := "date:" + dateStr

	ids, err := client.SInter(ctx, tagPrefix+summaryTag, tagPrefix+dateTag).Result()
	if err != nil || len(ids) == 0 {
		return nil
	}

	return loadEntries(ctx, client, ids)
}

func runWeeklyRollups(ctx context.Context, client *redis.Client, ollamaClient *ollama.Client, store *memory.RedisStore, tracks []string, cfg config.Config, startTime time.Time, dryRun bool) {
	loc := time.Now().Location()
	now := time.Now()

	// Align to Monday
	alignedDay := time.Date(startTime.Year(), startTime.Month(), startTime.Day(), 0, 0, 0, 0, loc)
	daysUntilMonday := (int(alignedDay.Weekday()) - 1 + 7) % 7
	alignedWeek := alignedDay.AddDate(0, 0, -daysUntilMonday)

	var generated int
	for weekStart := alignedWeek; weekStart.Before(now); weekStart = weekStart.AddDate(0, 0, 7) {
		weekEnd := weekStart.AddDate(0, 0, 7)
		if weekEnd.After(now) {
			break
		}

		for _, t := range tracks {
			weekStr := weekStart.Format("2006-01-02")
			entryID := fmt.Sprintf("summary:weekly:%s:%s", t, weekStr)

			exists, _ := client.Exists(ctx, entryPrefix+entryID).Result()
			if exists > 0 {
				continue
			}

			// Roll up from daily summaries
			entries := getDailySummariesForWeek(ctx, client, t, weekStart, weekEnd)
			if len(entries) == 0 {
				// Fall back to raw entries
				if !hasEntriesInWindow(ctx, client, t, weekStart.Unix(), weekEnd.Unix()) {
					continue
				}
				entries = getTrackEntriesByTime(ctx, client, t, weekStart.Unix(), weekEnd.Unix())
			}
			if len(entries) == 0 {
				continue
			}

			if dryRun {
				slog.Info("weekly: would generate", "entry_id", entryID, "sources", len(entries))
				generated++
				continue
			}

			// Promotion: if only one source summary exists, clone it rather than re-summarizing
			if len(entries) == 1 {
				slog.Info("weekly: promoting single source", "entry_id", entryID, "source_ts", entries[0].Timestamp)
				tags := []string{
					"track:" + t,
					strings.ToLower(t),
					"summary:weekly:" + t,
					"summary:weekly",
					"date:" + weekStr,
				}
				store.Put(ctx, memory.Entry{
					ID:        entryID,
					Content:   entries[0].Content,
					Tags:      tags,
					Timestamp: weekEnd,
				})
				generated++
				continue
			}

			slog.Info("weekly: generating", "entry_id", entryID, "sources", len(entries))

			var prompt strings.Builder
			prompt.WriteString(fmt.Sprintf(`You are writing memory notes for an AI assistant that will use these to quickly orient on the "%s" project after context loss. Be extremely concise.

Format: 3-7 bullet points maximum. Each bullet is ONE of:
- A DECISION made (what was chosen and why)
- A STATE change (what moved from X to Y)  
- A BLOCKER (what's stuck and on what)
- A KEY FACT (a number, name, or detail that would be hard to re-derive)

Rules:
- Maximum 200 words total
- No headers, no tables, no "Action Items" sections
- No filler phrases ("This week focused on...", "Key progress includes...")
- Write as if every word costs $1
- If nothing meaningful happened, say "No significant changes" and stop

Daily summaries to compress:
`, t))
			sort.Slice(entries, func(i, j int) bool { return entries[i].Timestamp < entries[j].Timestamp })
			for _, ds := range entries {
				prompt.WriteString(fmt.Sprintf("--- %s ---\n%s\n\n", time.Unix(ds.Timestamp, 0).Format("2006-01-02"), ds.Content))
			}

			summary, err := generateWithRetry(ctx, ollamaClient, prompt.String(), cfg.Ollama.MaxRetries)
			if err != nil {
				slog.Error("weekly: generation failed", "track", t, "week", weekStr, "err", err)
				continue
			}

			tags := []string{
				"track:" + t,
				strings.ToLower(t),
				"summary:weekly:" + t,
				"summary:weekly",
				"date:" + weekStr,
			}
			store.Put(ctx, memory.Entry{
				ID:        entryID,
				Content:   summary,
				Tags:      tags,
				Timestamp: weekEnd,
			})
			generated++
		}
	}
	slog.Info("weekly rollups complete", "generated", generated)
}

func getDailySummariesForWeek(ctx context.Context, client *redis.Client, track string, weekStart, weekEnd time.Time) []entry {
	summaryTag := "summary:daily:" + track
	ids, err := client.SMembers(ctx, tagPrefix+summaryTag).Result()
	if err != nil || len(ids) == 0 {
		return nil
	}

	var entries []entry
	for _, id := range ids {
		data, err := client.HGetAll(ctx, entryPrefix+id).Result()
		if err != nil || len(data) == 0 {
			continue
		}
		var ts int64
		fmt.Sscanf(data["timestamp"], "%d", &ts)
		if ts >= weekStart.Unix() && ts < weekEnd.Unix() {
			var tags []string
			if tStr := data["tags"]; tStr != "" {
				tags = strings.Split(tStr, ",")
			}
			entries = append(entries, entry{ID: data["id"], Content: data["content"], Tags: tags, Timestamp: ts})
		}
	}
	return entries
}

func runGlobal(ctx context.Context, client *redis.Client, ollamaClient *ollama.Client, store *memory.RedisStore, tracks []string, cfg config.Config, dryRun bool) {
	maxGlobalChars := cfg.Memory.GlobalSummaryMaxChars
	if maxGlobalChars <= 0 {
		maxGlobalChars = 4000
	}

	var generated int
	for _, t := range tracks {
		entryID := fmt.Sprintf("summary:global:%s", t)

		// Gather all weekly summaries for this track
		summaryTag := "summary:weekly:" + t
		ids, err := client.SMembers(ctx, tagPrefix+summaryTag).Result()
		if err != nil || len(ids) == 0 {
			continue
		}

		entries := loadEntries(ctx, client, ids)
		if len(entries) == 0 {
			continue
		}

		if dryRun {
			slog.Info("global: would generate", "track", t, "weeklies", len(entries))
			generated++
			continue
		}

		slog.Info("global: generating", "track", t, "weeklies", len(entries))

		sort.Slice(entries, func(i, j int) bool { return entries[i].Timestamp < entries[j].Timestamp })

		var prompt strings.Builder
		prompt.WriteString(fmt.Sprintf(`You are writing a single-page orientation document for the "%s" track. An AI assistant will read this to understand the project's current state after total context loss.

Format:
- One paragraph: what this project IS (2-3 sentences max)
- Then bullet points: current state, key decisions still in effect, active blockers
- Maximum %d characters total
- No history unless it directly impacts current state
- No "Next Steps" unless something is actively blocked
- Write as memory, not as a report

Weekly summaries to compress:
`, t, maxGlobalChars))

		for _, ws := range entries {
			prompt.WriteString(fmt.Sprintf("--- Week of %s ---\n%s\n\n", time.Unix(ws.Timestamp, 0).Format("2006-01-02"), ws.Content))
		}

		summary, err := generateWithRetry(ctx, ollamaClient, prompt.String(), cfg.Ollama.MaxRetries)
		if err != nil {
			slog.Error("global: generation failed", "track", t, "err", err)
			continue
		}

		// Cap output
		if len(summary) > maxGlobalChars {
			summary = summary[:maxGlobalChars]
		}

		now := time.Now()
		tags := []string{
			"track:" + t,
			strings.ToLower(t),
			"summary:global:" + t,
			"summary:global",
			"date:" + now.Format("2006-01-02"),
		}
		store.Put(ctx, memory.Entry{
			ID:        entryID,
			Content:   summary,
			Tags:      tags,
			Timestamp: now,
		})
		generated++
	}
	slog.Info("global summaries complete", "generated", generated)
}

// --- Track discovery ---

func discoverTracks(ctx context.Context, client *redis.Client, filter string) []string {
	if filter != "" {
		return []string{filter}
	}

	allTags, err := client.SMembers(ctx, allTagsKey).Result()
	if err != nil {
		return nil
	}

	// Also check track manifests for authoritative list
	manifestTracks := make(map[string]bool)
	manifestContent, _ := client.HGet(ctx, "entry:meta:track-manifests", "content").Result()
	if manifestContent != "" {
		var manifests map[string]string
		if json.Unmarshal([]byte(manifestContent), &manifests) == nil {
			for name := range manifests {
				manifestTracks[name] = true
			}
		}
	}

	trackSet := make(map[string]bool)
	for _, tag := range allTags {
		var name string
		if strings.HasPrefix(tag, "track:track:") {
			// Double-prefix — opportunistically clean: collapse track:track:X → track:X
			realTag := strings.TrimPrefix(tag, "track:")
			name = strings.TrimPrefix(realTag, "track:")
			// Migrate entries from the broken tag to the correct one
			members, _ := client.SMembers(ctx, tagPrefix+tag).Result()
			if len(members) > 0 {
				correctTag := "track:" + name
				for _, id := range members {
					client.SAdd(ctx, tagPrefix+correctTag, id)
					client.SRem(ctx, tagPrefix+tag, id)
					// Also fix the entry's tag string
					fixEntryTag(ctx, client, id, tag, correctTag)
				}
				client.SRem(ctx, allTagsKey, tag)
				client.SAdd(ctx, allTagsKey, correctTag)
				slog.Info("collapsed double-prefix tag", "from", tag, "to", correctTag, "entries", len(members))
			}
		} else if strings.HasPrefix(tag, "track_auto:track:") {
			// Same for track_auto:track:X → track_auto:X
			name = strings.TrimPrefix(tag, "track_auto:track:")
			members, _ := client.SMembers(ctx, tagPrefix+tag).Result()
			if len(members) > 0 {
				correctTag := "track_auto:" + name
				for _, id := range members {
					client.SAdd(ctx, tagPrefix+correctTag, id)
					client.SRem(ctx, tagPrefix+tag, id)
					fixEntryTag(ctx, client, id, tag, correctTag)
				}
				client.SRem(ctx, allTagsKey, tag)
				client.SAdd(ctx, allTagsKey, correctTag)
				slog.Info("collapsed double-prefix tag", "from", tag, "to", correctTag, "entries", len(members))
			}
		} else if strings.HasPrefix(tag, "track:") && !strings.HasPrefix(tag, "track_auto:") {
			name = strings.TrimPrefix(tag, "track:")
		} else if strings.HasPrefix(tag, "track_auto:") {
			name = strings.TrimPrefix(tag, "track_auto:")
		}
		if name == "" {
			continue
		}
		// Simon Says: only accept tracks that are in the manifest (authoritative list).
		// If no manifest exists, fall back to uppercase heuristic.
		if len(manifestTracks) > 0 {
			if manifestTracks[name] {
				trackSet[name] = true
			} else {
				slog.Debug("rejected phantom track (not in manifest)", "name", name, "tag", tag)
			}
		} else if len(name) > 0 && name[0] >= 'A' && name[0] <= 'Z' {
			trackSet[name] = true
		} else {
			// Stray lowercase tag that's not a real track — remove from tags:all
			client.SRem(ctx, allTagsKey, tag)
			slog.Debug("removed stray tag from tags:all", "tag", tag)
		}
	}

	var tracks []string
	for t := range trackSet {
		tracks = append(tracks, t)
	}
	sort.Strings(tracks)
	return tracks
}

// fixEntryTag replaces oldTag with newTag in an entry's comma-separated tags field.
func fixEntryTag(ctx context.Context, client *redis.Client, entryID, oldTag, newTag string) {
	tagsStr, err := client.HGet(ctx, "entry:"+entryID, "tags").Result()
	if err != nil {
		return
	}
	tags := strings.Split(tagsStr, ",")
	var fixed []string
	for _, t := range tags {
		t = strings.TrimSpace(t)
		if t == oldTag {
			fixed = append(fixed, newTag)
		} else {
			fixed = append(fixed, t)
		}
	}
	client.HSet(ctx, "entry:"+entryID, "tags", strings.Join(fixed, ","))
}

// --- Entry retrieval ---

func getTodayEntries(ctx context.Context, client *redis.Client, track string) []entry {
	today := time.Now().Format("2006-01-02")
	dateTag := "date:" + today
	trackTag := "track:" + track

	ids, err := client.SInter(ctx, tagPrefix+trackTag, tagPrefix+dateTag).Result()
	if err != nil || len(ids) == 0 {
		return getTrackEntriesByTime(ctx, client, track, todayStart(), time.Now().Unix())
	}

	entries := loadEntries(ctx, client, ids)

	var filtered []entry
	for _, e := range entries {
		isSummary := false
		isClassified := false
		hasExplicitTrack := false
		for _, tag := range e.Tags {
			if strings.HasPrefix(tag, "summary:") {
				isSummary = true
			}
			if tag == "classified" {
				isClassified = true
			}
			if tag == "track:"+track || tag == "track_auto:"+track {
				hasExplicitTrack = true
			}
		}
		// Include if: not a summary AND (classified OR has explicit track tag)
		if !isSummary && (isClassified || hasExplicitTrack) {
			filtered = append(filtered, e)
		}
	}
	return filtered
}

func getTrackEntries(ctx context.Context, client *redis.Client, track string, limit int) []entry {
	trackTag := "track:" + track
	ids, err := client.SMembers(ctx, tagPrefix+trackTag).Result()
	if err != nil {
		return nil
	}

	entries := loadEntries(ctx, client, ids)

	var filtered []entry
	for _, e := range entries {
		isSummary := false
		isClassified := false
		hasExplicitTrack := false
		for _, tag := range e.Tags {
			if strings.HasPrefix(tag, "summary:") {
				isSummary = true
			}
			if tag == "classified" {
				isClassified = true
			}
			if tag == "track:"+track || tag == "track_auto:"+track {
				hasExplicitTrack = true
			}
		}
		if !isSummary && (isClassified || hasExplicitTrack) {
			filtered = append(filtered, e)
		}
	}

	sort.Slice(filtered, func(i, j int) bool { return filtered[i].Timestamp > filtered[j].Timestamp })

	if len(filtered) > limit {
		filtered = filtered[:limit]
	}
	return filtered
}

func getTrackEntriesByTime(ctx context.Context, client *redis.Client, track string, start, end int64) []entry {
	ids, err := client.ZRangeByScore(ctx, timelineKey, &redis.ZRangeBy{
		Min: fmt.Sprintf("%d", start),
		Max: fmt.Sprintf("%d", end),
	}).Result()
	if err != nil {
		return nil
	}

	trackTag := "track:" + track
	trackAutoTag := "track_auto:" + track
	trackMembers, err := client.SMembers(ctx, tagPrefix+trackTag).Result()
	if err != nil {
		trackMembers = nil
	}
	trackAutoMembers, err := client.SMembers(ctx, tagPrefix+trackAutoTag).Result()
	if err != nil {
		trackAutoMembers = nil
	}
	trackSet := make(map[string]bool)
	for _, m := range trackMembers {
		trackSet[m] = true
	}
	for _, m := range trackAutoMembers {
		trackSet[m] = true
	}

	var filtered []string
	for _, id := range ids {
		if trackSet[id] {
			filtered = append(filtered, id)
		}
	}

	entries := loadEntries(ctx, client, filtered)

	var result []entry
	for _, e := range entries {
		isSummary := false
		isClassified := false
		hasExplicitTrack := false
		for _, tag := range e.Tags {
			if strings.HasPrefix(tag, "summary:") {
				isSummary = true
			}
			if tag == "classified" {
				isClassified = true
			}
			if tag == "track:"+track || tag == "track_auto:"+track {
				hasExplicitTrack = true
			}
		}
		// Include if: not a summary AND (classified OR has explicit track tag)
		if !isSummary && (isClassified || hasExplicitTrack) {
			result = append(result, e)
		}
	}
	return result
}

func getOrphanEntries(ctx context.Context, client *redis.Client) []entry {
	today := time.Now().Format("2006-01-02")
	ids, err := client.SInter(ctx, tagPrefix+"date:"+today, tagPrefix+"auto:captured").Result()
	if err != nil || len(ids) == 0 {
		return nil
	}

	classifiedIDs, _ := client.SMembers(ctx, tagPrefix+"classified").Result()
	classifiedSet := make(map[string]bool, len(classifiedIDs))
	for _, id := range classifiedIDs {
		classifiedSet[id] = true
	}

	var orphanIDs []string
	for _, id := range ids {
		if !classifiedSet[id] {
			orphanIDs = append(orphanIDs, id)
		}
	}

	return loadEntries(ctx, client, orphanIDs)
}

func filterUnclassified(entries []entry) []entry {
	var unclassified []entry
	for _, e := range entries {
		isClassified := false
		for _, tag := range e.Tags {
			if tag == "classified" {
				isClassified = true
				break
			}
		}
		if !isClassified {
			unclassified = append(unclassified, e)
		}
	}
	return unclassified
}

// --- Classification ---

func classifyEntries(ctx context.Context, client *redis.Client, ollamaClient *ollama.Client, entries []entry, tracks []string, classifyMax int) {
	if len(entries) == 0 {
		return
	}

	var classifyEntriesList []classify.Entry
	for _, e := range entries {
		classifyEntriesList = append(classifyEntriesList, classify.Entry{
			ID:        e.ID,
			Content:   e.Content,
			Tags:      e.Tags,
			Timestamp: e.Timestamp,
		})
	}

	opts := classify.DefaultOptions()
	opts.MaxContentChars = classifyMax

	results, err := classify.ClassifyEntries(ctx, client, ollamaClient, classifyEntriesList, opts)
	if err != nil {
		slog.Error("classification error", "err", err)
		return
	}

	confused := 0
	for _, r := range results {
		if r.Confused {
			confused++
		}
	}

	slog.Info("classified entries", "count", len(results), "confused", confused)
}

// --- Summarization ---

// generateWithRetry wraps an Ollama Generate call with retry-on-failure logic.
// On wedge or error, it force-unloads the model, waits, and retries up to maxRetries times.
func generateWithRetry(ctx context.Context, ollamaClient *ollama.Client, prompt string, maxRetries int) (string, error) {
	if maxRetries <= 0 {
		maxRetries = 2
	}

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			slog.Info("retrying generation", "attempt", attempt, "max_retries", maxRetries)
		}

		result, err := ollamaClient.Generate(ctx, prompt)
		if err == nil {
			return result, nil
		}

		lastErr = err

		// Check if it's a wedge or a transient error worth retrying
		if errors.Is(err, ollama.ErrWedged) || strings.Contains(err.Error(), "context deadline exceeded") || strings.Contains(err.Error(), "connection reset") {
			slog.Warn("generation failed, attempting recovery",
				"attempt", attempt+1,
				"max_retries", maxRetries,
				"err", err,
			)

			// Force unload the model to clear any stuck state
			unloadCtx, unloadCancel := context.WithTimeout(context.Background(), 30*time.Second)
			if unloadErr := ollamaClient.ForceUnload(unloadCtx); unloadErr != nil {
				slog.Warn("force unload failed", "err", unloadErr)
			}
			unloadCancel()

			// Wait for model to fully release resources
			time.Sleep(3 * time.Second)
			continue
		}

		// Non-retryable error (bad request, model not found, etc.)
		return "", err
	}

	return "", fmt.Errorf("generation failed after %d retries: %w", maxRetries, lastErr)
}

func generateSummary(ctx context.Context, ollamaClient *ollama.Client, track string, entries []entry, maxInputChars int, maxRetries int) (string, error) {
	sort.Slice(entries, func(i, j int) bool { return entries[i].Timestamp < entries[j].Timestamp })

	var prompt strings.Builder
	prompt.WriteString(fmt.Sprintf(`You are writing memory notes for an AI assistant about the "%s" track. These notes will be used for rapid orientation after context loss.

Requirements:
- Bullet points only, no headers except a single title line
- Capture: decisions made, state changes, blockers, key facts/numbers
- Preserve technical detail (commands, configs, values) that is EXPLICITLY present
- DO NOT invent details not present in the source entries
- DO NOT add "Action Items", "Next Steps", or "Open Questions" sections unless a specific blocker is identified
- Keep under 800 words
- Write as if every word costs $1 — no filler, no ceremony

Entries:
`, track))

	totalChars := 0
	for _, e := range entries {
		content := e.Content
		if totalChars+len(content) > maxInputChars {
			content = content[:maxInputChars-totalChars]
			prompt.WriteString(fmt.Sprintf("---\n[%s]\n%s\n[TRUNCATED]\n", time.Unix(e.Timestamp, 0).Format("2006-01-02 15:04"), content))
			break
		}
		prompt.WriteString(fmt.Sprintf("---\n[%s]\n%s\n", time.Unix(e.Timestamp, 0).Format("2006-01-02 15:04"), content))
		totalChars += len(content)
	}

	return generateWithRetry(ctx, ollamaClient, prompt.String(), maxRetries)
}

// --- Storage ---

func storeSummary(ctx context.Context, store *memory.RedisStore, track, summary string) {
	today := time.Now().Format("2006-01-02")
	entryID := fmt.Sprintf("summary:daily:%s:%s", track, today)

	tags := []string{
		"track:" + track,
		strings.ToLower(track),
		"summary:daily:" + track,
		"summary:daily",
		"date:" + today,
	}

	store.Put(ctx, memory.Entry{
		ID:        entryID,
		Content:   summary,
		Tags:      tags,
		Timestamp: time.Now(),
	})
}

func store3hSummary(ctx context.Context, store *memory.RedisStore, track, summary string) {
	now := time.Now()
	window := now.Format("2006-01-02-15")
	entryID := fmt.Sprintf("summary:3h:%s:%s", track, window)

	tags := []string{
		"track:" + track,
		strings.ToLower(track),
		"summary:3h:" + track,
		"summary:3h",
		"date:" + now.Format("2006-01-02"),
	}

	store.Put(ctx, memory.Entry{
		ID:        entryID,
		Content:   summary,
		Tags:      tags,
		Timestamp: now,
	})
}

// --- Wipe Summaries (legacy) ---

func runWipeSummaries(ctx context.Context, client *redis.Client, dryRun bool) {
	ids, err := client.ZRange(ctx, timelineKey, 0, -1).Result()
	if err != nil {
		slog.Error("failed to read timeline", "err", err)
		os.Exit(1)
	}

	var summaryIDs []string
	for _, id := range ids {
		if strings.HasPrefix(id, "summary:") {
			summaryIDs = append(summaryIDs, id)
		}
	}

	if len(summaryIDs) == 0 {
		slog.Info("no summaries found")
		return
	}

	if dryRun {
		slog.Info("dry run: would delete summaries", "count", len(summaryIDs))
		return
	}

	slog.Info("deleting summaries", "count", len(summaryIDs))

	for _, id := range summaryIDs {
		key := entryPrefix + id
		tagsStr, _ := client.HGet(ctx, key, "tags").Result()
		if tagsStr != "" {
			for _, tag := range strings.Split(tagsStr, ",") {
				tag = strings.TrimSpace(tag)
				if tag != "" {
					client.SRem(ctx, tagPrefix+tag, id)
				}
			}
		}
		client.Del(ctx, key)
		client.ZRem(ctx, timelineKey, id)
		client.Do(ctx, "VREM", "hippocampus:vectors", id)
	}

	slog.Info("deleted summaries, run --backfill to regenerate", "count", len(summaryIDs))
}

// --- Classify ---

func runClassify(ctx context.Context, client *redis.Client, ollamaClient *ollama.Client, trackFilter string, force, dryRun bool, maxChars int, batchSize int) {
	opts := classify.DefaultOptions()
	opts.Force = force
	opts.DryRun = dryRun
	opts.BatchSize = batchSize
	if maxChars > 0 {
		opts.MaxContentChars = maxChars
	}

	trackAdded := make(map[string]int)
	trackRemoved := make(map[string]int)
	confused := 0

	opts.OnProgress = func(ev classify.ProgressEvent) {
		pct := float64(ev.Current) / float64(ev.Total) * 100
		barLen := 30
		filled := int(float64(barLen) * float64(ev.Current) / float64(ev.Total))
		bar := strings.Repeat("█", filled) + strings.Repeat("░", barLen-filled)

		oldSet := make(map[string]bool)
		for _, t := range ev.OldTracks {
			oldSet[t] = true
		}
		newSet := make(map[string]bool)
		for _, t := range ev.NewTracks {
			if t != "" {
				newSet[t] = true
			}
		}

		for t := range newSet {
			if !oldSet[t] {
				trackAdded[t]++
			}
		}
		for t := range oldSet {
			if !newSet[t] {
				trackRemoved[t]++
			}
		}
		if ev.Confused {
			confused++
		}

		fmt.Fprintf(os.Stderr, "\r  [%s] %d/%d (%.0f%%) ", bar, ev.Current, ev.Total, pct)
	}

	var entryIDs []string

	if force && trackFilter != "" {
		ids, err := client.SMembers(ctx, tagPrefix+("track:"+trackFilter)).Result()
		if err != nil {
			slog.Error("failed to get track members", "err", err)
			os.Exit(1)
		}
		entryIDs = ids
		slog.Info("reclassifying entries on track", "count", len(ids), "track", trackFilter)
	} else if force {
		ids, err := client.ZRange(ctx, timelineKey, 0, -1).Result()
		if err != nil {
			slog.Error("failed to get timeline entries", "err", err)
			os.Exit(1)
		}

		excludePrefixes := []string{"summary:", "meta:", "auto:"}
		for _, id := range ids {
			excluded := false
			for _, prefix := range excludePrefixes {
				if strings.HasPrefix(id, prefix) {
					excluded = true
					break
				}
			}
			if !excluded {
				entryIDs = append(entryIDs, id)
			}
		}
		slog.Info("reclassifying entries", "count", len(entryIDs), "excluded", len(ids)-len(entryIDs))
	} else {
		allCaptured, err := client.SMembers(ctx, tagPrefix+"auto:captured").Result()
		if err != nil {
			slog.Error("failed to get auto:captured members", "err", err)
			os.Exit(1)
		}
		classified, err := client.SMembers(ctx, tagPrefix+"classified").Result()
		if err != nil {
			classified = nil
		}
		classifiedSet := make(map[string]bool)
		for _, id := range classified {
			classifiedSet[id] = true
		}
		for _, id := range allCaptured {
			if !classifiedSet[id] {
				entryIDs = append(entryIDs, id)
			}
		}
		slog.Info("found unclassified entries", "count", len(entryIDs))
	}

	if len(entryIDs) == 0 {
		slog.Info("nothing to classify")
		return
	}

	if force {
		results, err := classify.Reclassify(ctx, client, ollamaClient, entryIDs, opts)
		if err != nil {
			slog.Error("reclassification failed", "err", err)
			os.Exit(1)
		}
		fmt.Fprintln(os.Stderr)
		printClassifyStats(len(results), confused, trackAdded, trackRemoved, dryRun)
	} else {
		var toClassify []classify.Entry
		for _, id := range entryIDs {
			data, err := client.HGetAll(ctx, entryPrefix+id).Result()
			if err != nil || len(data) == 0 {
				continue
			}
			var ts int64
			fmt.Sscanf(data["timestamp"], "%d", &ts)
			toClassify = append(toClassify, classify.Entry{
				ID:        id,
				Content:   data["content"],
				Tags:      strings.Split(data["tags"], ","),
				Timestamp: ts,
			})
		}

		results, err := classify.ClassifyEntries(ctx, client, ollamaClient, toClassify, opts)
		if err != nil {
			slog.Error("classification failed", "err", err)
			os.Exit(1)
		}
		fmt.Fprintln(os.Stderr)
		printClassifyStats(len(results), confused, trackAdded, trackRemoved, dryRun)
	}
}

func printClassifyStats(total, confused int, added, removed map[string]int, dryRun bool) {
	unclassifiable := 0
	if n, ok := added["none"]; ok {
		unclassifiable = n
		delete(added, "none")
	}

	slog.Info("classification complete",
		"dry_run", dryRun,
		"total", total,
		"confused", confused,
		"unclassifiable", unclassifiable,
		"tracks_added", added,
		"tracks_removed", removed,
	)
}

// --- Backfill (legacy — kept for reference, now superseded by run3hBlocks/runDailyRollups/runWeeklyRollups) ---

func runBackfill(ctx context.Context, client *redis.Client, store *memory.RedisStore, ollamaClient *ollama.Client, trackFilter string, cfg config.Config, dryRun bool) {
	tracks := discoverTracks(ctx, client, trackFilter)
	if len(tracks) == 0 {
		slog.Info("no tracks found")
		return
	}

	earliest, err := client.ZRangeByScoreWithScores(ctx, timelineKey, &redis.ZRangeBy{
		Min: "-inf", Max: "+inf", Count: 1, Offset: 0,
	}).Result()
	if err != nil || len(earliest) == 0 {
		slog.Info("no entries in timeline")
		return
	}
	startTime := time.Unix(int64(earliest[0].Score), 0)
	now := time.Now()

	slog.Info("backfilling summaries", "from", startTime.Format("2006-01-02"), "to", now.Format("2006-01-02"))

	alignedStart := startTime.Truncate(3 * time.Hour)

	var generated3h, generatedDaily, generatedWeekly int

	for windowStart := alignedStart; windowStart.Before(now); windowStart = windowStart.Add(3 * time.Hour) {
		windowEnd := windowStart.Add(3 * time.Hour)
		if windowEnd.After(now) {
			break
		}

		for _, t := range tracks {
			windowKey := windowStart.Format("2006-01-02-15")
			entryID := fmt.Sprintf("summary:3h:%s:%s", t, windowKey)

			exists, _ := client.Exists(ctx, entryPrefix+entryID).Result()
			if exists > 0 {
				continue
			}

			entries := getTrackEntriesByTime(ctx, client, t, windowStart.Unix(), windowEnd.Unix())
			if len(entries) == 0 {
				continue
			}

			if dryRun {
				slog.Info("backfill/3h: would generate", "entry_id", entryID, "entries", len(entries))
				generated3h++
				continue
			}

			slog.Info("backfill/3h: generating", "entry_id", entryID, "entries", len(entries))
			summary, err := generateSummary(ctx, ollamaClient, t, entries, cfg.Memory.SummarizeMaxInputChars, cfg.Ollama.MaxRetries)
			if err != nil {
				slog.Error("backfill/3h: generation failed", "err", err)
				continue
			}

			tags := []string{
				"track:" + t,
				strings.ToLower(t),
				"summary:3h:" + t,
				"summary:3h",
				"date:" + windowStart.Format("2006-01-02"),
			}
			store.Put(ctx, memory.Entry{
				ID:        entryID,
				Content:   summary,
				Tags:      tags,
				Timestamp: windowEnd,
			})
			generated3h++
		}
	}

	slog.Info("backfilled 3h summaries", "count", generated3h)

	alignedDay := time.Date(startTime.Year(), startTime.Month(), startTime.Day(), 0, 0, 0, 0, startTime.Location())

	for day := alignedDay; day.Before(now); day = day.AddDate(0, 0, 1) {
		dayEnd := day.AddDate(0, 0, 1)
		if dayEnd.After(now) {
			break
		}

		for _, t := range tracks {
			dateStr := day.Format("2006-01-02")
			entryID := fmt.Sprintf("summary:daily:%s:%s", t, dateStr)

			exists, _ := client.Exists(ctx, entryPrefix+entryID).Result()
			if exists > 0 {
				continue
			}

			entries := getTrackEntriesByTime(ctx, client, t, day.Unix(), dayEnd.Unix())
			if len(entries) == 0 {
				continue
			}

			if dryRun {
				slog.Info("backfill/daily: would generate", "entry_id", entryID, "entries", len(entries))
				generatedDaily++
				continue
			}

			slog.Info("backfill/daily: generating", "entry_id", entryID, "entries", len(entries))
			summary, err := generateSummary(ctx, ollamaClient, t, entries, cfg.Memory.SummarizeMaxInputChars, cfg.Ollama.MaxRetries)
			if err != nil {
				slog.Error("backfill/daily: generation failed", "err", err)
				continue
			}

			tags := []string{
				"track:" + t,
				strings.ToLower(t),
				"summary:daily:" + t,
				"summary:daily",
				"date:" + dateStr,
			}
			store.Put(ctx, memory.Entry{
				ID:        entryID,
				Content:   summary,
				Tags:      tags,
				Timestamp: dayEnd,
			})
			generatedDaily++
		}
	}

	slog.Info("backfilled daily summaries", "count", generatedDaily)

	daysUntilMonday := (int(alignedDay.Weekday()) - 1 + 7) % 7
	alignedWeek := alignedDay.AddDate(0, 0, -daysUntilMonday)

	for weekStart := alignedWeek; weekStart.Before(now); weekStart = weekStart.AddDate(0, 0, 7) {
		weekEnd := weekStart.AddDate(0, 0, 7)
		if weekEnd.After(now) {
			break
		}

		for _, t := range tracks {
			weekStr := weekStart.Format("2006-01-02")
			entryID := fmt.Sprintf("summary:weekly:%s:%s", t, weekStr)

			exists, _ := client.Exists(ctx, entryPrefix+entryID).Result()
			if exists > 0 {
				continue
			}

			entries := getTrackEntriesByTime(ctx, client, t, weekStart.Unix(), weekEnd.Unix())
			if len(entries) == 0 {
				continue
			}

			if dryRun {
				slog.Info("backfill/weekly: would generate", "entry_id", entryID, "entries", len(entries))
				generatedWeekly++
				continue
			}

			slog.Info("backfill/weekly: generating", "entry_id", entryID, "entries", len(entries))
			summary, err := generateSummary(ctx, ollamaClient, t, entries, cfg.Memory.SummarizeMaxInputChars, cfg.Ollama.MaxRetries)
			if err != nil {
				slog.Error("backfill/weekly: generation failed", "err", err)
				continue
			}

			tags := []string{
				"track:" + t,
				strings.ToLower(t),
				"summary:weekly:" + t,
				"summary:weekly",
				"date:" + weekStr,
			}
			store.Put(ctx, memory.Entry{
				ID:        entryID,
				Content:   summary,
				Tags:      tags,
				Timestamp: weekEnd,
			})
			generatedWeekly++
		}
	}

	slog.Info("backfilled weekly summaries", "count", generatedWeekly)
	slog.Info("backfill complete", "3h", generated3h, "daily", generatedDaily, "weekly", generatedWeekly, "total", generated3h+generatedDaily+generatedWeekly)
}

// --- Weekly rollup ---

func runWeeklyRollup(ctx context.Context, client *redis.Client, store *memory.RedisStore, ollamaClient *ollama.Client, trackFilter string, dryRun bool) {
	tracks := discoverTracks(ctx, client, trackFilter)
	if len(tracks) == 0 {
		slog.Info("no tracks found")
		return
	}

	for _, t := range tracks {
		slog.Info("weekly rollup", "track", t)

		summaryTag := "summary:daily:" + t
		ids, err := client.SMembers(ctx, tagPrefix+summaryTag).Result()
		if err != nil || len(ids) == 0 {
			slog.Info("no daily summaries for track", "track", t)
			continue
		}

		weekAgo := time.Now().Add(-7 * 24 * time.Hour).Unix()
		var dailySummaries []entry
		for _, id := range ids {
			data, err := client.HGetAll(ctx, entryPrefix+id).Result()
			if err != nil || len(data) == 0 {
				continue
			}
			var ts int64
			fmt.Sscanf(data["timestamp"], "%d", &ts)
			if ts >= weekAgo {
				var tags []string
				if tStr := data["tags"]; tStr != "" {
					tags = strings.Split(tStr, ",")
				}
				dailySummaries = append(dailySummaries, entry{ID: data["id"], Content: data["content"], Tags: tags, Timestamp: ts})
			}
		}

		if len(dailySummaries) == 0 {
			continue
		}

		slog.Info("rolling up daily summaries", "count", len(dailySummaries))
		if dryRun {
			continue
		}

		var prompt strings.Builder
		prompt.WriteString(fmt.Sprintf(`Produce a weekly summary for the "%s" track from these daily summaries.

Format:
- Typed bullets ONLY. Each bullet starts with one of: DECISION, STATE, BLOCKER, KEY FACT, OPEN
- No markdown headers, no tables, no horizontal rules, no code blocks
- Capture the arc: what started, progressed, was decided, remains open
- Note shifts in direction. Identify patterns.
- Keep under 200 words. Every word costs $1.

Daily summaries:
`, t))

		sort.Slice(dailySummaries, func(i, j int) bool { return dailySummaries[i].Timestamp < dailySummaries[j].Timestamp })
		for _, ds := range dailySummaries {
			prompt.WriteString(fmt.Sprintf("--- %s ---\n%s\n\n", time.Unix(ds.Timestamp, 0).Format("2006-01-02"), ds.Content))
		}

		summary, err := ollamaClient.Generate(ctx, prompt.String())
		if err != nil {
			slog.Error("weekly rollup generation failed", "track", t, "err", err)
			continue
		}

		now := time.Now()
		weekStart := now.Add(-7 * 24 * time.Hour).Format("2006-01-02")
		entryID := fmt.Sprintf("summary:week:%s:%s", t, weekStart)

		tags := []string{"track:" + t, strings.ToLower(t), "summary:week:" + t, "summary:weekly", "date:" + now.Format("2006-01-02")}

		store.Put(ctx, memory.Entry{
			ID:        entryID,
			Content:   summary,
			Tags:      tags,
			Timestamp: now,
		})

		slog.Info("stored weekly summary", "track", t, "chars", len(summary))
	}

	slog.Info("weekly rollup done")
}

// --- Cross-track review ---

func runCrossTrackReview(ctx context.Context, client *redis.Client, store *memory.RedisStore, ollamaClient *ollama.Client, crossTrackMax int, maxRetries int, dryRun bool) {
	slog.Info("cross-track review starting")

	tracks := discoverTracks(ctx, client, "")
	if len(tracks) < 2 {
		slog.Info("need at least 2 tracks for cross-track review")
		return
	}

	type trackSummary struct {
		Track, Summary, ID string
	}

	var summaries []trackSummary
	for _, t := range tracks {
		summaryTag := "summary:daily:" + t
		ids, err := client.SMembers(ctx, tagPrefix+summaryTag).Result()
		if err != nil || len(ids) == 0 {
			continue
		}

		var bestID string
		var bestTS int64
		for _, id := range ids {
			ts, err := client.HGet(ctx, entryPrefix+id, "timestamp").Int64()
			if err != nil {
				continue
			}
			if ts > bestTS {
				bestTS = ts
				bestID = id
			}
		}
		if bestID == "" {
			continue
		}

		content, _ := client.HGet(ctx, entryPrefix+bestID, "content").Result()
		summaries = append(summaries, trackSummary{Track: t, Summary: content, ID: bestID})
	}

	if len(summaries) < 2 {
		slog.Info("not enough summaries for cross-track analysis")
		return
	}

	if dryRun {
		slog.Info("dry run: would analyze track summaries", "count", len(summaries))
		return
	}

	var prompt strings.Builder
	prompt.WriteString(`Analyze these project tracks for cross-cutting themes, shared concepts, and synergies.

Output a JSON array of links:
[{"track_a": "Name", "track_b": "Name", "concept": "shared concept", "score": 0.7}]

Score: 0.9-1.0 direct dependency, 0.7-0.8 same concept different application, 0.5-0.6 tangential.
Only include genuine, non-obvious connections. Respond ONLY with JSON.

Track summaries:
`)

	for _, ts := range summaries {
		content := ts.Summary
		if len(content) > crossTrackMax {
			content = content[:crossTrackMax] + "..."
		}
		prompt.WriteString(fmt.Sprintf("\n=== %s ===\n%s\n", ts.Track, content))
	}

	resp, err := generateWithRetry(ctx, ollamaClient, prompt.String(), maxRetries)
	if err != nil {
		slog.Error("cross-track generation failed", "err", err)
		return
	}

	resp = extractJSON(resp)

	var links []struct {
		TrackA  string  `json:"track_a"`
		TrackB  string  `json:"track_b"`
		Concept string  `json:"concept"`
		Score   float64 `json:"score"`
	}
	if err := json.Unmarshal([]byte(resp), &links); err != nil {
		slog.Error("failed to parse cross-track response", "err", err)
		return
	}

	for _, link := range links {
		var idA, idB string
		for _, ts := range summaries {
			if ts.Track == link.TrackA {
				idA = ts.ID
			}
			if ts.Track == link.TrackB {
				idB = ts.ID
			}
		}
		if idA == "" || idB == "" {
			continue
		}

		// Unified format: HASH links:<id> with field=targetID, value="score|type"
		value := fmt.Sprintf("%.4f|%s", link.Score, "cross-track")
		client.HSet(ctx, "links:"+idA, idB, value)
		client.HSet(ctx, "links:"+idB, idA, value)
		slog.Info("linked tracks", "track_a", link.TrackA, "track_b", link.TrackB, "score", link.Score, "concept", link.Concept)
	}

	if len(links) > 0 {
		var content strings.Builder
		content.WriteString("## Cross-Track Connections\n\n")
		for _, link := range links {
			content.WriteString(fmt.Sprintf("- **%s ↔ %s** (%.1f): %s\n", link.TrackA, link.TrackB, link.Score, link.Concept))
		}

		now := time.Now()
		entryID := fmt.Sprintf("summary:cross-track:%s", now.Format("2006-01-02"))
		tags := []string{"summary:cross-track", "summary:weekly", "date:" + now.Format("2006-01-02")}

		store.Put(ctx, memory.Entry{
			ID:        entryID,
			Content:   content.String(),
			Tags:      tags,
			Timestamp: now,
		})

		slog.Info("stored cross-track summary", "connections", len(links))
	}

	slog.Info("cross-track review done")
}

// --- Helpers ---

func loadEntries(ctx context.Context, client *redis.Client, ids []string) []entry {
	var entries []entry
	for _, id := range ids {
		data, err := client.HGetAll(ctx, entryPrefix+id).Result()
		if err != nil || len(data) == 0 {
			continue
		}
		var ts int64
		fmt.Sscanf(data["timestamp"], "%d", &ts)
		var tags []string
		if t := data["tags"]; t != "" {
			tags = strings.Split(t, ",")
		}

		// Security: exclude full web content entries from summarization.
		if hasTag(tags, "content:full") {
			continue
		}

		entries = append(entries, entry{ID: data["id"], Content: data["content"], Tags: tags, Timestamp: ts})
	}
	return entries
}

func hasTag(tags []string, target string) bool {
	for _, t := range tags {
		if t == target {
			return true
		}
	}
	return false
}

func extractJSON(s string) string {
	if idx := strings.Index(s, "</think>"); idx != -1 {
		s = s[idx+len("</think>"):]
	}
	start := strings.Index(s, "[")
	end := strings.LastIndex(s, "]")
	if start != -1 && end != -1 && end > start {
		return s[start : end+1]
	}
	return strings.TrimSpace(s)
}

func todayStart() int64 {
	now := time.Now()
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location()).Unix()
}
