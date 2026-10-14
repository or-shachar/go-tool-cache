package cachers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
)

// GCSCache is a remote cache that is backed by a Google Cloud Storage bucket
type GCSCache struct {
	client     *storage.Client
	bucket     *storage.BucketHandle
	bucketName string
	prefix     string
	// verbose optionally specifies whether to log verbose messages.
	verbose bool

	warnPermissionDenied sync.Once
}

var _ RemoteCache = &GCSCache{}

func NewGCSCache(client *storage.Client, bucketName string, cacheKey string, verbose bool) *GCSCache {
	return &GCSCache{
		client:     client,
		bucket:     client.Bucket(bucketName),
		bucketName: bucketName,
		prefix:     remotePrefix(cacheKey),
		verbose:    verbose,
	}
}

func (g *GCSCache) Kind() string {
	return "gcs"
}

func (g *GCSCache) Start(context.Context) error {
	if g.verbose {
		log.Printf("[%s]\tconfigured to gs://%s/%s", g.Kind(), g.bucketName, g.prefix)
	}
	return nil
}

func (g *GCSCache) Get(ctx context.Context, actionID string) (outputID string, size int64, output io.ReadCloser, err error) {
	actionKey := g.actionKey(actionID)
	// The default XML read path returns custom metadata with the object, so a hit is one request.
	reader, err := g.bucket.Object(actionKey).NewReader(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return "", 0, nil, nil
	}
	if gcsErrorCode(err) == http.StatusForbidden {
		g.warnPermissionDenied.Do(func() {
			log.Printf("[%s]\tGCS read returned 403 on gs://%s/%s — treating as cache miss. "+
				"Verify the credentials have storage.objects.get on the bucket.",
				g.Kind(), g.bucketName, actionKey)
		})
		return "", 0, nil, nil
	}
	if err != nil {
		if g.verbose {
			log.Printf("error GCS get for %s:  %v", actionKey, err)
		}
		return "", 0, nil, fmt.Errorf("unexpected GCS get for %s:  %w", actionKey, err)
	}
	outputID = metadataValue(reader.Metadata(), outputIDMetadataKey)
	if outputID == "" {
		_ = reader.Close()
		return "", 0, nil, fmt.Errorf("outputId not found in metadata")
	}
	return outputID, reader.Attrs.Size, reader, nil
}

func (g *GCSCache) Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) error {
	if size == 0 {
		body = bytes.NewReader(nil)
	}
	actionKey := g.actionKey(actionID)

	// Cancelling the context is how a storage.Writer discards a partial upload.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// An action ID maps to a single output, so an existing object already
	// holds what we'd write; DoesNotExist keeps entries write-once.
	writer := g.bucket.Object(actionKey).If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	writer.Metadata = map[string]string{outputIDMetadataKey: outputID}
	// The body is a one-shot stream; upload it in a single request, which
	// also means no retries (we cannot rewind the body).
	writer.ChunkSize = 0

	written, err := io.Copy(writer, body)
	if err == nil && written != size {
		err = fmt.Errorf("body had %d bytes, expected %d", written, size)
	}
	if err != nil {
		cancel()
		_ = writer.Close()
		if g.verbose {
			log.Printf("error GCS put for %s:  %v", actionKey, err)
		}
		return err
	}

	err = writer.Close()
	switch gcsErrorCode(err) {
	case http.StatusPreconditionFailed:
		return nil
	case http.StatusForbidden:
		g.warnPermissionDenied.Do(func() {
			log.Printf("[%s]\tGCS write returned 403 on gs://%s/%s. "+
				"Verify the credentials have storage.objects.create on the bucket; remote cache writes are being dropped.",
				g.Kind(), g.bucketName, actionKey)
		})
	default:
		if err != nil && g.verbose {
			log.Printf("error GCS put for %s:  %v", actionKey, err)
		}
	}
	return err
}

func (g *GCSCache) Close() error {
	return g.client.Close()
}

func (g *GCSCache) actionKey(actionID string) string {
	return fmt.Sprintf("%s/%s", g.prefix, actionID)
}

// metadataValue looks key up case-insensitively: XML reads rebuild metadata
// keys from canonicalized X-Goog-Meta-* headers, so "outputid" reads back as "Outputid".
func metadataValue(metadata map[string]string, key string) string {
	for k, v := range metadata {
		if strings.EqualFold(k, key) {
			return v
		}
	}
	return ""
}

// gcsErrorCode returns the HTTP status of a GCS API error, or 0 if err is
// nil or not a *googleapi.Error.
func gcsErrorCode(err error) int {
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) {
		return apiErr.Code
	}
	return 0
}
