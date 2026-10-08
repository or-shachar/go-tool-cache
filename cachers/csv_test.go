package cachers

import (
	"bytes"
	"context"
	"encoding/csv"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stubLocal is a minimal LocalCache we can wrap in LocalCacheWithCounts
// without touching the disk.
type stubLocal struct{}

func (stubLocal) Kind() string                                            { return "stub-local" }
func (stubLocal) Start(context.Context) error                             { return nil }
func (stubLocal) Close() error                                            { return nil }
func (stubLocal) Get(context.Context, string) (string, string, error)    { return "", "", nil }
func (stubLocal) Put(context.Context, string, string, int64, io.Reader) (string, error) {
	return "", nil
}

type stubRemote struct{}

func (stubRemote) Kind() string                                        { return "stub-remote" }
func (stubRemote) Start(context.Context) error                         { return nil }
func (stubRemote) Close() error                                        { return nil }
func (stubRemote) Get(context.Context, string) (string, int64, io.ReadCloser, error) {
	return "", 0, nil, nil
}
func (stubRemote) Put(context.Context, string, string, int64, io.Reader) error { return nil }

func TestWriteStatsCSV_HeaderOnlyWhenNoVisitor(t *testing.T) {
	// Plain cache with no CountsVisitor in the chain.
	var buf bytes.Buffer
	err := writeStatsCSV(stubLocal{}, &buf, time.Unix(0, 0).UTC())
	require.NoError(t, err)

	rows := readCSV(t, &buf)
	assert.Len(t, rows, 1)
	assert.Equal(t, csvHeader, rows[0])
}

func TestWriteStatsCSV_OneRowPerLayer(t *testing.T) {
	// Build a chain the way getCache does in verbose mode:
	//   LocalCacheWithCounts -> CombinedCache -> {LocalCacheWithCounts -> disk,
	//                                              RemoteCacheWithCounts -> s3}
	localInner := NewLocalCacheStates(stubLocal{})
	remoteInner := NewRemoteCacheStats(stubRemote{})
	combined := &CombinedCache{
		localCache:  localInner,
		remoteCache: remoteInner,
		putsMetrics: newTimeKeeper(),
		getsMetrics: newTimeKeeper(),
	}
	outer := NewLocalCacheStates(combined)

	// Simulate traffic so we can tell the rows apart.
	outer.gets.Store(10)
	outer.hits.Store(7)
	outer.misses.Store(3)
	outer.puts.Store(5)
	localInner.gets.Store(10)
	localInner.hits.Store(7)
	remoteInner.gets.Store(3)
	remoteInner.hits.Store(2)
	remoteInner.misses.Store(1)

	var buf bytes.Buffer
	err := writeStatsCSV(outer, &buf, time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	require.NoError(t, err)

	rows := readCSV(t, &buf)
	require.GreaterOrEqual(t, len(rows), 4, "expected header + 3 layer rows")
	assert.Equal(t, csvHeader, rows[0])

	// Collect rows by kind for order-independent assertions.
	byKind := map[string][]string{}
	for _, r := range rows[1:] {
		byKind[r[1]] = r
	}

	// Outer wrapper reports the combined cache's Kind.
	require.Contains(t, byKind, "combined")
	assert.Equal(t, "10", byKind["combined"][2])
	assert.Equal(t, "7", byKind["combined"][3])

	require.Contains(t, byKind, "stub-local")
	assert.Equal(t, "10", byKind["stub-local"][2])

	require.Contains(t, byKind, "stub-remote")
	assert.Equal(t, "3", byKind["stub-remote"][2])
	assert.Equal(t, "1", byKind["stub-remote"][4])

	// Every row carries the same timestamp.
	for _, r := range rows[1:] {
		assert.Equal(t, "2026-01-02T03:04:05Z", r[0])
	}
}

func readCSV(t *testing.T, r io.Reader) [][]string {
	t.Helper()
	all, err := io.ReadAll(r)
	require.NoError(t, err)
	rows, err := csv.NewReader(strings.NewReader(string(all))).ReadAll()
	require.NoError(t, err)
	return rows
}
