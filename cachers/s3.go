package cachers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"runtime"
	"sync"

	"github.com/aws/smithy-go"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	outputIDMetadataKey = "outputid"
)

// s3Client represents the functions we need from the S3 client
type s3Client interface {
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

// S3Cache is a remote cache that is backed by S3 bucket
type S3Cache struct {
	bucket string
	prefix string
	// verbose optionally specifies whether to log verbose messages.
	verbose  bool
	s3Client s3Client

	// warnAccessDenied logs an IAM warning at most once per cache instance,
	// so a misconfigured policy doesn't silently cost every build its hit rate.
	warnAccessDenied sync.Once
}

var _ RemoteCache = &S3Cache{}

func (s *S3Cache) Kind() string {
	return "s3"
}

func (s *S3Cache) Start(context.Context) error {
	if s.verbose {
		log.Printf("[%s]\tconfigured to s3://%s/%s", s.Kind(), s.bucket, s.prefix)
	}
	return nil
}

func (s *S3Cache) Get(ctx context.Context, actionID string) (outputID string, size int64, output io.ReadCloser, err error) {
	actionKey := s.actionKey(actionID)
	outputResult, getOutputErr := s.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &actionKey,
	})
	switch s3ErrorCode(getOutputErr) {
	case "NoSuchKey":
		return "", 0, nil, nil
	case "AccessDenied":
		// Some S3 setups return AccessDenied instead of NoSuchKey when the
		// caller lacks s3:ListBucket. Preserve the existing miss semantics,
		// but surface the misconfig once per process so a broken IAM policy
		// doesn't silently keep hit rate at 0%.
		s.warnAccessDenied.Do(func() {
			log.Printf("[%s]\tS3 GetObject returned AccessDenied on s3://%s/%s — treating as cache miss. "+
				"If this is unexpected, verify the IAM policy grants s3:GetObject (and ideally s3:ListBucket) on the bucket.",
				s.Kind(), s.bucket, actionKey)
		})
		return "", 0, nil, nil
	}
	if getOutputErr != nil {
		if s.verbose {
			log.Printf("error S3 get for %s:  %v", actionKey, getOutputErr)
		}
		return "", 0, nil, fmt.Errorf("unexpected S3 get for %s:  %w", actionKey, getOutputErr)
	}
	var contentSize int64
	if outputResult.ContentLength != nil {
		contentSize = *outputResult.ContentLength
	}
	outputID, ok := outputResult.Metadata[outputIDMetadataKey]
	if !ok || outputID == "" {
		return "", 0, nil, fmt.Errorf("outputId not found in metadata")
	}
	return outputID, contentSize, outputResult.Body, nil
}

func (s *S3Cache) Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (err error) {
	if size == 0 {
		body = bytes.NewReader(nil)
	}
	actionKey := s.actionKey(actionID)
	_, err = s.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        &s.bucket,
		Key:           &actionKey,
		Body:          body,
		ContentLength: &size,
		Metadata: map[string]string{
			outputIDMetadataKey: outputID,
		},
	}, func(options *s3.Options) {
		options.RetryMaxAttempts = 1 // We cannot perform seek in Body
	})
	if err != nil {
		// AccessDenied on Put is almost always an IAM problem the operator
		// wants to see, even if the surrounding CombinedCache tolerates
		// remote write errors. Log it once per process regardless of verbose.
		if s3ErrorCode(err) == "AccessDenied" {
			s.warnAccessDenied.Do(func() {
				log.Printf("[%s]\tS3 PutObject returned AccessDenied on s3://%s/%s. "+
					"Verify the IAM policy grants s3:PutObject on the bucket; remote cache writes are being dropped.",
					s.Kind(), s.bucket, actionKey)
			})
		} else if s.verbose {
			log.Printf("error S3 put for %s:  %v", actionKey, err)
		}
	}
	return
}

func (s *S3Cache) Close() error {
	return nil
}

func NewS3Cache(client s3Client, bucketName string, cacheKey string, verbose bool) *S3Cache {
	// get target architecture
	goarch := os.Getenv("GOARCH")
	if goarch == "" {
		goarch = runtime.GOARCH
	}
	// get target operating system
	goos := os.Getenv("GOOS")
	if goos == "" {
		goos = runtime.GOOS
	}
	prefix := path.Join("cache", cacheKey, goarch, goos)
	cache := &S3Cache{
		s3Client: client,
		bucket:   bucketName,
		prefix:   prefix,
		verbose:  verbose,
	}
	return cache
}

// s3ErrorCode returns the smithy API error code for err, or "" if err is nil
// or not a smithy.APIError. Lets callers switch on specific S3 codes
// (e.g. "NoSuchKey", "AccessDenied") instead of lumping them together.
func s3ErrorCode(err error) string {
	if err == nil {
		return ""
	}
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return ""
}

func (s *S3Cache) actionKey(actionID string) string {
	return fmt.Sprintf("%s/%s", s.prefix, actionID)
}
