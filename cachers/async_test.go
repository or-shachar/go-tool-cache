package cachers

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// fakeRemote is a minimal RemoteCache that records every Put and can be
// configured to block each Put on a release channel to simulate a slow remote.
type fakeRemote struct {
	puts    atomic.Int64
	release chan struct{} // nil means don't block
	putErr  error

	mu      sync.Mutex
	putKeys []string
}

func (f *fakeRemote) Kind() string                                        { return "fake" }
func (f *fakeRemote) Start(context.Context) error                         { return nil }
func (f *fakeRemote) Close() error                                        { return nil }
func (f *fakeRemote) Get(context.Context, string) (string, int64, io.ReadCloser, error) {
	return "", 0, nil, nil
}

func (f *fakeRemote) Put(_ context.Context, actionID, _ string, _ int64, body io.Reader) error {
	if f.release != nil {
		<-f.release
	}
	if f.putErr != nil {
		return f.putErr
	}
	_, _ = io.Copy(io.Discard, body)
	f.puts.Add(1)
	f.mu.Lock()
	f.putKeys = append(f.putKeys, actionID)
	f.mu.Unlock()
	return nil
}

func TestAsyncRemoteCache_PutReturnsImmediately(t *testing.T) {
	// Remote blocks forever; Put should still return promptly from the
	// caller's perspective. Shut down with a tight drain timeout so the
	// test doesn't take 30s to finish.
	remote := &fakeRemote{release: make(chan struct{})}
	a := NewAsyncRemoteCache(remote, 2, 16, false)
	a.closeDrainTimeout = 50 * time.Millisecond
	assert.NoError(t, a.Start(context.Background()))

	start := time.Now()
	err := a.Put(context.Background(), "action1", "output1", 5, bytes.NewReader([]byte("hello")))
	elapsed := time.Since(start)
	assert.NoError(t, err)
	assert.Less(t, elapsed, 50*time.Millisecond, "Put should return without waiting on the remote")

	close(remote.release) // let any pending uploads finish
	_ = a.Close()
}

func TestAsyncRemoteCache_WorkersDrainQueue(t *testing.T) {
	remote := &fakeRemote{}
	a := NewAsyncRemoteCache(remote, 4, 64, false)
	assert.NoError(t, a.Start(context.Background()))

	const n = 32
	for range n {
		err := a.Put(context.Background(), "a", "o", 3, bytes.NewReader([]byte("xyz")))
		assert.NoError(t, err)
	}
	assert.NoError(t, a.Close())
	assert.Equal(t, int64(n), remote.puts.Load())
	assert.Zero(t, a.dropped.Load())
	assert.Zero(t, a.failed.Load())
}

func TestAsyncRemoteCache_FailuresCounted(t *testing.T) {
	remote := &fakeRemote{putErr: errors.New("boom")}
	a := NewAsyncRemoteCache(remote, 2, 8, false)
	assert.NoError(t, a.Start(context.Background()))

	for range 5 {
		assert.NoError(t, a.Put(context.Background(), "a", "o", 1, bytes.NewReader([]byte("x"))))
	}
	assert.NoError(t, a.Close())
	assert.Equal(t, int64(5), a.failed.Load())
}

func TestAsyncRemoteCache_NonBlockingDropsOnOverflow(t *testing.T) {
	// Hold the workers so the queue fills, then push beyond queueLen.
	remote := &fakeRemote{release: make(chan struct{})}
	a := NewAsyncRemoteCache(remote, 1, 2, false)
	a.closeDrainTimeout = 50 * time.Millisecond
	assert.NoError(t, a.Start(context.Background()))

	// Fill the single worker's in-flight slot + 2 queue slots = 3 absorbed;
	// the rest should be dropped.
	const sent = 10
	for range sent {
		assert.NoError(t, a.Put(context.Background(), "a", "o", 1, bytes.NewReader([]byte("x"))))
	}
	close(remote.release)
	_ = a.Close()
	assert.Positive(t, a.dropped.Load(), "expected some puts to be dropped under overflow")
	assert.Less(t, remote.puts.Load(), int64(sent), "expected some puts never to reach remote")
}

func TestAsyncRemoteCache_BlockingHonorsContext(t *testing.T) {
	// Workers blocked, queue size 1, block=true. Second Put should wait
	// until ctx is cancelled, then return ctx.Err().
	remote := &fakeRemote{release: make(chan struct{})}
	a := NewAsyncRemoteCache(remote, 1, 1, true)
	a.closeDrainTimeout = 50 * time.Millisecond
	assert.NoError(t, a.Start(context.Background()))

	// Fill the worker's slot (it will block on release) and the one queue slot.
	assert.NoError(t, a.Put(context.Background(), "a", "o", 1, bytes.NewReader([]byte("x"))))
	assert.NoError(t, a.Put(context.Background(), "a", "o", 1, bytes.NewReader([]byte("x"))))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := a.Put(ctx, "a", "o", 1, bytes.NewReader([]byte("x")))
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Zero(t, a.dropped.Load(), "blocking mode never drops")

	close(remote.release)
	_ = a.Close()
}

func TestAsyncRemoteCache_CloseDrainsPending(t *testing.T) {
	remote := &fakeRemote{}
	a := NewAsyncRemoteCache(remote, 2, 32, false)
	assert.NoError(t, a.Start(context.Background()))

	for range 16 {
		assert.NoError(t, a.Put(context.Background(), "a", "o", 1, bytes.NewReader([]byte("x"))))
	}
	assert.NoError(t, a.Close())
	assert.Equal(t, int64(16), remote.puts.Load(), "Close should wait for queued uploads")
}
