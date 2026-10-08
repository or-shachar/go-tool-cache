package cachers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"

	"golang.org/x/sync/errgroup"
)

// CombinedCache is a LocalCache that wraps a LocalCache and a RemoteCache.
// It also keeps times for the remote cache Download/Uploads
type CombinedCache struct {
	verbose     bool
	localCache  LocalCache
	remoteCache RemoteCache
	putsMetrics *timeKeeper
	getsMetrics *timeKeeper
}

var _ LocalCache = &CombinedCache{}

func NewCombinedCache(localCache LocalCache, remoteCache RemoteCache, verbose bool) LocalCache {
	cache := &CombinedCache{
		verbose:     verbose,
		localCache:  localCache,
		remoteCache: remoteCache,
		putsMetrics: newTimeKeeper(),
		getsMetrics: newTimeKeeper(),
	}
	if verbose {
		cache.localCache = NewLocalCacheStates(localCache)
		cache.remoteCache = NewRemoteCacheStats(remoteCache)
		return NewLocalCacheStates(cache)
	}
	return cache
}

func (l *CombinedCache) Kind() string {
	return "combined"
}

func (l *CombinedCache) Start(ctx context.Context) error {
	err := l.localCache.Start(ctx)
	if err != nil {
		return fmt.Errorf("local cache start failed: %w", err)
	}
	err = l.remoteCache.Start(ctx)
	if err != nil {
		_ = l.localCache.Close()
		return fmt.Errorf("remote cache start failed: %w", err)
	}
	l.putsMetrics.Start(ctx)
	l.getsMetrics.Start(ctx)
	return nil
}

func (l *CombinedCache) Get(ctx context.Context, actionID string) (string, string, error) {
	outputID, diskPath, err := l.localCache.Get(ctx, actionID)
	if err == nil && outputID != "" {
		return outputID, diskPath, nil
	}
	outputID, size, output, err := l.remoteCache.Get(ctx, actionID)
	if err != nil {
		return "", "", err
	}
	if outputID == "" {
		return "", "", nil
	}
	diskPath, err = l.getsMetrics.DoWithMeasure(size, func() (string, error) {
		defer output.Close() //nolint:errcheck
		return l.localCache.Put(ctx, actionID, outputID, size, output)
	})
	if err != nil {
		return "", "", err
	}
	return outputID, diskPath, nil
}

func (l *CombinedCache) Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (diskPath string, err error) {
	// Fast path for already-in-memory bodies.
	//
	// cacheproc.Run reads each put body fully into []byte before dispatching
	// (see the "stream this" TODO in cacheproc.go), so in production the
	// body here is always a plain *bytes.Reader. The streaming fallback
	// below exists for callers using the cachers package directly with a
	// true io.Reader; keep it so this remains a drop-in CombinedCache.
	if br, ok := body.(*bytes.Reader); ok && size > 0 {
		return l.putBytes(ctx, actionID, outputID, size, br)
	}

	pr, pw := io.Pipe()
	wg, _ := errgroup.WithContext(ctx)
	wg.Go(func() error {
		var putBody io.Reader = pr
		if size == 0 {
			putBody = bytes.NewReader(nil)
		}
		var err2 error
		diskPath, err2 = l.localCache.Put(ctx, actionID, outputID, size, putBody)
		return err2
	})

	var putBody io.Reader
	if size == 0 {
		// Special case the empty file so NewRequest sets "Content-Length: 0",
		// as opposed to thinking we didn't set it and not being able to sniff its size
		// from the type.
		putBody = bytes.NewReader(nil)
	} else {

		putBody = io.TeeReader(body, pw)
	}
	// tolerate remote write errors
	_, _ = l.putsMetrics.DoWithMeasure(size, func() (string, error) {
		e := l.remoteCache.Put(ctx, actionID, outputID, size, putBody)
		return "", e
	})
	_ = pw.Close()
	if err := wg.Wait(); err != nil {
		log.Printf("[%s]\terror: %v", l.localCache.Kind(), err)
		return "", err
	}
	return diskPath, nil

}

// putBytes handles the common in-memory case without io.Pipe/TeeReader.
// Both local and remote Puts run concurrently against independent
// bytes.Readers over the same backing slice. This also avoids the subtle
// failure mode in the streaming path, where an early return from
// remoteCache.Put would leave the local Put blocked on the pipe reader
// until pw.Close runs.
func (l *CombinedCache) putBytes(ctx context.Context, actionID, outputID string, size int64, body *bytes.Reader) (diskPath string, err error) {
	buf := make([]byte, size)
	if _, err := io.ReadFull(body, buf); err != nil {
		return "", fmt.Errorf("reading in-memory put body: %w", err)
	}

	wg, _ := errgroup.WithContext(ctx)
	wg.Go(func() error {
		var err2 error
		diskPath, err2 = l.localCache.Put(ctx, actionID, outputID, size, bytes.NewReader(buf))
		return err2
	})
	// tolerate remote write errors
	_, _ = l.putsMetrics.DoWithMeasure(size, func() (string, error) {
		return "", l.remoteCache.Put(ctx, actionID, outputID, size, bytes.NewReader(buf))
	})
	if err := wg.Wait(); err != nil {
		log.Printf("[%s]\terror: %v", l.localCache.Kind(), err)
		return "", err
	}
	return diskPath, nil
}

func (l *CombinedCache) Close() error {
	var errAll error
	if err := l.localCache.Close(); err != nil {
		errAll = errors.Join(fmt.Errorf("local cache stop failed: %w", err), errAll)
	}
	if err := l.remoteCache.Close(); err != nil {
		errAll = errors.Join(fmt.Errorf("remote cache stop failed: %w", err), errAll)
	}
	if err := l.putsMetrics.Stop(); err != nil {
		errAll = errors.Join(fmt.Errorf("puts metrics stop failed: %w", err), errAll)
	}
	if err := l.getsMetrics.Stop(); err != nil {
		errAll = errors.Join(fmt.Errorf("gets metrics stop failed: %w", err), errAll)
	}
	if l.verbose {
		log.Printf("[%s]\tDownloads: %s, Uploads %s", l.remoteCache.Kind(), l.getsMetrics.Summary(), l.putsMetrics.Summary())
	}
	return errAll
}
