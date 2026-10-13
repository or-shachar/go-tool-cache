package cachers

import (
	"context"
	"io"
	"os"
	"path"
	"runtime"
)

// Cache is the interface implemented by all caches.
type Cache interface {
	Start(ctx context.Context) error
	Close() error
	Kind() string
}

// LocalCache is the basic interface for a local cache.
// It supposed to write to Disk, thus the signature include diskPath.
type LocalCache interface {
	Cache
	Get(ctx context.Context, actionID string) (outputID, diskPath string, err error)
	Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (diskPath string, err error)
}

// RemoteCache is the basic interface for a remote cache.
type RemoteCache interface {
	Cache
	Get(ctx context.Context, actionID string) (outputID string, size int64, output io.ReadCloser, err error)
	Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (err error)
}

// remotePrefix is where a remote cache stores its objects:
// cache/<cacheKey>/<GOARCH>/<GOOS>, using the build's target platform.
func remotePrefix(cacheKey string) string {
	goarch := os.Getenv("GOARCH")
	if goarch == "" {
		goarch = runtime.GOARCH
	}
	goos := os.Getenv("GOOS")
	if goos == "" {
		goos = runtime.GOOS
	}
	return path.Join("cache", cacheKey, goarch, goos)
}
