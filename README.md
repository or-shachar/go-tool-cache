# go-tool-cache

Like Go's built-in build/test caching but wish it weren't purely stored on local disk in the `$GOCACHE` directory?

Want to share your cache over the network between your various machines, coworkers, and CI runs without all that GitHub actions/caches tarring and untarring?

Along with a [modification to Go's `cmd/go` tool](https://go-review.googlesource.com/c/go/+/486715) ([open proposal](https://github.com/golang/go/issues/59719)), this repo lets you write
custom `GOCACHE` implementations to handle the cache however you'd like.

## Status

- Starting from [Go 1.24](https://tip.golang.org/doc/go1.24) you can use the official golang releases. For protocol details - [see the documentation](https://tip.golang.org/cmd/go/internal/cacheprog)
- This was previously behind a GOEXPERIMENT

## Using

First, build your cache child process. For example,

```sh
$ go install github.com/bradfitz/go-tool-cache/cmd/go-cacher
```

Then tell Go to use it:

```sh
$ GOCACHEPROG=$HOME/go/bin/go-cacher go install std
```

See some stats:

```sh
$ GOCACHEPROG="$HOME/go/bin/go-cacher --verbose" go install std
Defaulting to cache dir /home/bradfitz/.cache/go-cacher ...
cacher: closing; 548 gets (0 hits, 548 misses, 0 errors); 1090 puts (0 errors)
```

Run it again and watch the hit rate go up:

```sh
$ GOCACHEPROG="$HOME/go/bin/go-cacher --verbose" go install std
Defaulting to cache dir /home/bradfitz/.cache/go-cacher ...
cacher: closing; 808 gets (808 hits, 0 misses, 0 errors); 0 puts (0 errors)
```

## Configuration

### Local disk
- `GOCACHE_DISK_DIR` - (Optional) Directory for the local on-disk cache.
  Defaults to `<os.UserCacheDir()>/go-cacher`.

### Remote backends

At most one remote backend is used. If both are configured, **S3 takes
precedence over HTTP**.

#### S3

Set the following to enable the S3 remote cache:
- `GOCACHE_S3_BUCKET` - Name of S3 bucket
- `GOCACHE_AWS_REGION` - AWS Region of bucket
- `GOCACHE_AWS_ACCESS_KEY` + `GOCACHE_AWS_SECRET_ACCESS_KEY` (optionally with `GOCACHE_AWS_SESSION_TOKEN` for STS/OIDC flows such as GitHub Actions)
  / `GOCACHE_AWS_CREDS_PROFILE` - Direct credentials or creds profile to use.
- `GOCACHE_S3_URL` - (Optional) Custom S3-compatible endpoint (e.g. MinIO, LocalStack). When set, the client uses path-style addressing. If `GOCACHE_AWS_REGION` is unset, `us-east-1` is used as a placeholder.
- `GOCACHE_CACHE_KEY` - (Optional, default `v1`) Unique key

The cache would be stored to `s3://<bucket>/cache/<cache_key>/<architecture>/<os>/`

If `GOCACHE_S3_BUCKET` is set but credentials or region cannot be resolved, the cacher exits with an error — misconfigurations are no longer silently downgraded to local-only caching.

#### HTTP

- `GOCACHE_HTTP_SERVER_BASE` - Base URL of a `go-cacher-server`
  (scheme + authority only, e.g. `http://localhost:31364`).

### Async remote writes (opt-in)

By default, `Put` waits for both the local disk write and the remote upload
before returning to `cmd/go`. On slow or distant remotes (cross-region S3
from CI) this stalls the build on every cache miss. Opt in to a background
worker pool that returns to the caller as soon as the local write
completes:

- `GOCACHE_REMOTE_ASYNC_WORKERS` - number of upload goroutines. 0 (default)
  keeps the synchronous behavior; any positive value enables async writes.
- `GOCACHE_REMOTE_ASYNC_QUEUE` - buffered queue depth. Defaults to `10 * workers`.
- `GOCACHE_REMOTE_ASYNC_BLOCK` - set to `1` to make `Put` block on a full
  queue (lossless, with back-pressure to `cmd/go`). Default is to drop the
  put and increment a counter logged at shutdown.

Upload errors in async mode are logged and counted but not surfaced to
`cmd/go` — once `Put` returned, the originating request is already gone.
Reads are always synchronous (within a single build, `cmd/go` never
re-reads an action it just wrote).
