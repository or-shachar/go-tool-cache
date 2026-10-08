package cachers

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// AsyncRemoteCache wraps a RemoteCache so Put returns to the caller as soon
// as the body is buffered, and the actual upload happens on a background
// worker pool. Fire-and-forget: upload errors are logged and counted rather
// than surfaced, since by the time an async upload fails the originating
// cmd/go request is long gone.
//
// Get is a plain passthrough — within a single build, cmd/go never reads
// an action it just wrote, so queued puts don't need to be visible to
// subsequent gets.
//
// This is strictly opt-in; wrap only when latency to the remote dominates
// the build (typical for cross-region S3 from CI).
type AsyncRemoteCache struct {
	inner   RemoteCache
	workers int
	block   bool // if true, Put blocks on queue-full instead of dropping

	queue chan putReq
	wg    sync.WaitGroup

	// Shutdown drain is bounded so a hung remote can't hang cacher Close.
	closeDrainTimeout time.Duration

	dropped atomic.Int64
	failed  atomic.Int64
}

type putReq struct {
	actionID, outputID string
	size               int64
	body               []byte
}

// NewAsyncRemoteCache wraps inner. workers controls the number of upload
// goroutines; queueLen is the buffered channel size. If block is true,
// Put blocks when the queue is full (respecting the caller's context);
// otherwise it drops and increments a counter.
//
// workers and queueLen must both be positive.
func NewAsyncRemoteCache(inner RemoteCache, workers, queueLen int, block bool) *AsyncRemoteCache {
	if workers < 1 {
		workers = 1
	}
	if queueLen < 1 {
		queueLen = workers * 10
	}
	return &AsyncRemoteCache{
		inner:             inner,
		workers:           workers,
		block:             block,
		queue:             make(chan putReq, queueLen),
		closeDrainTimeout: 30 * time.Second,
	}
}

func (a *AsyncRemoteCache) Kind() string {
	return "async:" + a.inner.Kind()
}

func (a *AsyncRemoteCache) Start(ctx context.Context) error {
	if err := a.inner.Start(ctx); err != nil {
		return err
	}
	for range a.workers {
		a.wg.Add(1)
		go a.worker(ctx)
	}
	return nil
}

func (a *AsyncRemoteCache) Get(ctx context.Context, actionID string) (string, int64, io.ReadCloser, error) {
	return a.inner.Get(ctx, actionID)
}

func (a *AsyncRemoteCache) Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) error {
	// Snapshot the body so the worker owns an independent copy. The caller
	// (CombinedCache) hands us a bytes.NewReader or a TeeReader over one;
	// either way the content is already in RAM, so this is a memcpy, not
	// extra I/O.
	buf, err := snapshotBody(body, size)
	if err != nil {
		return fmt.Errorf("snapshot put body: %w", err)
	}
	req := putReq{actionID: actionID, outputID: outputID, size: size, body: buf}

	if a.block {
		select {
		case a.queue <- req:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	select {
	case a.queue <- req:
	default:
		a.dropped.Add(1)
	}
	return nil
}

func (a *AsyncRemoteCache) worker(ctx context.Context) {
	defer a.wg.Done()
	for req := range a.queue {
		if err := a.inner.Put(ctx, req.actionID, req.outputID, req.size, bytes.NewReader(req.body)); err != nil {
			a.failed.Add(1)
			log.Printf("[%s]\tbackground put failed for action %s / output %s: %v",
				a.Kind(), req.actionID, req.outputID, err)
		}
	}
}

func (a *AsyncRemoteCache) Close() error {
	close(a.queue)

	// Bound the drain so a stuck remote can't hang shutdown.
	done := make(chan struct{})
	go func() {
		a.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(a.closeDrainTimeout):
		log.Printf("[%s]\tclose: drain timed out after %s; queued uploads may be lost",
			a.Kind(), a.closeDrainTimeout)
	}

	if dropped := a.dropped.Load(); dropped > 0 {
		log.Printf("[%s]\tclose: dropped %d put(s) due to full queue", a.Kind(), dropped)
	}
	if failed := a.failed.Load(); failed > 0 {
		log.Printf("[%s]\tclose: %d background upload(s) failed", a.Kind(), failed)
	}
	return a.inner.Close()
}

var _ RemoteCache = (*AsyncRemoteCache)(nil)

// snapshotBody reads body into a []byte of length size. If body is a
// *bytes.Reader we could in principle alias its backing slice, but it
// doesn't expose one, so a plain read is the honest option.
func snapshotBody(body io.Reader, size int64) ([]byte, error) {
	if size == 0 {
		return nil, nil
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(body, buf); err != nil {
		return nil, err
	}
	return buf, nil
}
