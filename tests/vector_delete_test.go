// Copyright (c) 2026 Ruthlessly Practical LLC. All rights reserved.
// Use of this source code is governed by a BSD-3-Clause license
// that can be found in the LICENSE file.

package tests

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ruthlesslypractical/hippocampus/internal/config"
	"github.com/ruthlesslypractical/hippocampus/internal/embedding"
	"github.com/ruthlesslypractical/hippocampus/internal/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testVectorKey = "hippocampus:vectors"

// TestDelete_RemovesVector: an entry deleted through the Store (the path memory_delete takes) loses its embedding,
// so VSIM can no longer return it.
func TestDelete_RemovesVector(t *testing.T) {
	tr := SetupRedis(t)
	ctx := context.Background()

	require.NoError(t, tr.Store.Put(ctx, memory.Entry{
		ID: "vec-del-1", Timestamp: time.Now(), Content: "An entry that has an embedding.",
		Tags: []string{"track:TestProject"}, Weight: 1.0,
	}))
	require.NoError(t, tr.Client.Do(ctx, "VADD", testVectorKey, "VALUES", "3", "0.1", "0.2", "0.3", "vec-del-1").Err())
	n, err := tr.Client.Do(ctx, "VCARD", testVectorKey).Int64()
	require.NoError(t, err)
	require.EqualValues(t, 1, n)

	require.NoError(t, tr.Store.Delete(ctx, "vec-del-1"))

	n, err = tr.Client.Do(ctx, "VCARD", testVectorKey).Int64()
	require.NoError(t, err)
	assert.EqualValues(t, 0, n, "the vector must go with the entry")
}

// TestSearch_DropsVectorsWithoutEntries: a vector whose entry no longer exists (orphaned by a delete made before Delete
// removed vectors) is not returned by Search as a result with empty content.
func TestSearch_DropsVectorsWithoutEntries(t *testing.T) {
	tr := SetupRedis(t)
	ctx := context.Background()

	ollama := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"embeddings":[[0.1,0.2,0.3]]}`)
	}))
	defer ollama.Close()
	emb := embedding.NewEmbedder(config.OllamaConfig{BaseURL: ollama.URL, EmbeddingModel: "fake-embed", EmbeddingDimensions: 3})
	store, err := memory.NewRedisStore(config.RedisConfig{Addr: tr.Addr}, emb)
	require.NoError(t, err)
	defer store.Close()

	require.NoError(t, store.Put(ctx, memory.Entry{
		ID: "live-1", Timestamp: time.Now(), Content: "A live entry about harbour electrical work.",
		Tags: []string{"track:TestProject"}, Weight: 1.0,
	}))
	require.NoError(t, tr.Client.Do(ctx, "VADD", testVectorKey, "VALUES", "3", "0.1", "0.2", "0.3", "live-1").Err())
	require.NoError(t, tr.Client.Do(ctx, "VADD", testVectorKey, "VALUES", "3", "0.1", "0.2", "0.31", "ghost-1").Err())

	results, err := store.Search(ctx, "harbour electrical", 10)
	require.NoError(t, err)
	require.NotEmpty(t, results)
	for _, r := range results {
		assert.NotEqual(t, "ghost-1", r.Entry.ID, "a vector without an entry is not a result")
		assert.NotEmpty(t, r.Entry.Content, "no result comes back hollow")
	}
}
