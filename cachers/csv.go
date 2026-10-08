package cachers

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"
)

// csvHeader is the fixed column order emitted by WriteStatsCSV. Keep this
// stable — downstream aggregators (Google Sheets, Grafana imports, etc.)
// depend on it.
var csvHeader = []string{
	"timestamp", "kind",
	"gets", "hits", "misses", "puts",
	"get_errors", "put_errors",
}

// WriteStatsCSV walks cache for CountsVisitor-bearing layers and writes
// one CSV row per layer to path. The file is overwritten; callers who
// want per-run history should point at unique filenames and aggregate
// externally.
//
// If cache does not implement CountsVisitor (no stats wrappers in the
// chain) WriteStatsCSV writes only the header row and returns.
func WriteStatsCSV(cache Cache, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create metrics csv: %w", err)
	}
	defer f.Close()
	return writeStatsCSV(cache, f, time.Now().UTC())
}

func writeStatsCSV(cache Cache, out io.Writer, now time.Time) error {
	w := csv.NewWriter(out)
	if err := w.Write(csvHeader); err != nil {
		return err
	}

	v, ok := cache.(CountsVisitor)
	if !ok {
		w.Flush()
		return w.Error()
	}

	ts := now.Format(time.RFC3339)
	var writeErr error
	v.VisitCounts(func(kind string, c *Counts) {
		if writeErr != nil {
			return
		}
		writeErr = w.Write([]string{
			ts, kind,
			strconv.FormatInt(c.gets.Load(), 10),
			strconv.FormatInt(c.hits.Load(), 10),
			strconv.FormatInt(c.misses.Load(), 10),
			strconv.FormatInt(c.puts.Load(), 10),
			strconv.FormatInt(c.getErrors.Load(), 10),
			strconv.FormatInt(c.putErrors.Load(), 10),
		})
	})
	if writeErr != nil {
		return writeErr
	}
	w.Flush()
	return w.Error()
}
