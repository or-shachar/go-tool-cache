package cachers

import (
	"fmt"
	"sync"
	"time"
)

// timeKeeper records the bytes and durations of cache operations and reports
// a human-readable summary. Updates are serialized under a mutex; the record
// path is called inline from Get/Put, so callers block only for the handful of
// instructions needed to update the counters.
//
// The previous implementation used a buffered channel and a background
// aggregator goroutine. That added two failure modes with no real benefit:
// senders could block when the 1024-slot buffer filled under load, and
// closing the channel at shutdown could race with in-flight DoWithMeasure
// calls and panic. A mutex is cheaper than both.
type timeKeeper struct {
	mu                sync.Mutex
	Count             int64
	TotalBytes        int64
	AvgBytesPerSecond float64
}

func newTimeKeeper() *timeKeeper {
	return &timeKeeper{}
}

func (c *timeKeeper) Summary() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return fmt.Sprintf("%s (%s/sec)",
		formatBytes(float64(c.TotalBytes)), formatBytes(c.AvgBytesPerSecond))
}

func newAverage(oldAverage float64, count int64, newValue float64) float64 {
	return (oldAverage*float64(count) + newValue) / float64(count+1)
}

func (c *timeKeeper) DoWithMeasure(bytesCount int64, f func() (string, error)) (string, error) {
	start := time.Now()
	s, err := f()
	duration := time.Since(start)
	if err == nil {
		c.record(bytesCount, duration)
	}
	return s, err
}

func (c *timeKeeper) record(bytes int64, duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.TotalBytes += bytes
	// Skip speed samples that would produce NaN/Inf (sub-nanosecond
	// durations on very fast ops, or zero-byte transfers). TotalBytes
	// still reflects every event; only the running average is updated
	// from well-defined samples.
	if duration <= 0 || bytes <= 0 {
		return
	}
	speed := float64(bytes) / duration.Seconds()
	c.AvgBytesPerSecond = newAverage(c.AvgBytesPerSecond, c.Count, speed)
	c.Count++
}

// formatBytes formats a number of bytes into a human-readable string.
func formatBytes(size float64) string {
	const (
		_ = 1 << (10 * iota)
		kb
		mb
		gb
		tb
		pb
		eb
	)
	switch {
	case size < kb:
		return fmt.Sprintf("%.2f B", size)
	case size < mb:
		return fmt.Sprintf("%.2f KB", size/float64(kb))
	case size < gb:
		return fmt.Sprintf("%.2f MB", size/float64(mb))
	case size < tb:
		return fmt.Sprintf("%.2f GB", size/float64(gb))
	case size < pb:
		return fmt.Sprintf("%.2f TB", size/float64(tb))
	case size < eb:
		return fmt.Sprintf("%.2f PB", size/float64(pb))
	default:
		return fmt.Sprintf("%.2f EB", size/float64(eb))
	}
}
