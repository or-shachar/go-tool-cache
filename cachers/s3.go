package cachers

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"runtime"

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
}

func (s *S3Cache) Kind() string {
	return "s3"
}

func (s *S3Cache) Start() error {
	log.Printf("[%s]\tconfigured to s3://%s/%s", s.Kind(), s.bucket, s.prefix)
	return nil
}

func (s *S3Cache) Close() error {
	return nil
}

func (s *S3Cache) Get(ctx context.Context, actionID string) (outputID string, size int64, output io.ReadCloser, err error) {
	actionKey := s.actionKey(actionID)
	outputResult, getOutputErr := s.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &actionKey,
	})
	if isNotFoundError(getOutputErr) {
		// handle object not found
		return "", 0, nil, nil
	} else if getOutputErr != nil {
		if s.verbose {
			log.Printf("error S3 get for %s:  %v", actionKey, getOutputErr)
		}
		return "", 0, nil, fmt.Errorf("unexpected S3 get for %s:  %v", actionKey, getOutputErr)
	}
	contentSize := outputResult.ContentLength
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
		ContentLength: size,
		Metadata: map[string]string{
			outputIDMetadataKey: outputID,
		},
	})
	return
}

var _ RemoteCache = &S3Cache{}

func NewS3Cache(client s3Client, bucketName string, cacheKey string, verbose bool) *S3Cache {
	// get current architecture
	arc := runtime.GOARCH
	// get current operating system
	os := runtime.GOOS
	prefix := fmt.Sprintf("cache/%s/%s/%s", cacheKey, arc, os)
	cache := &S3Cache{
		s3Client: client,
		bucket:   bucketName,
		prefix:   prefix,
		verbose:  verbose,
	}
	return cache
}

func isNotFoundError(err error) bool {
	if err != nil {
		var ae smithy.APIError
		if errors.As(err, &ae) {
			code := ae.ErrorCode()
			return code == "AccessDenied" || code == "NoSuchKey"
		}
	}
	return false
}

func (s *S3Cache) actionKey(actionID string) string {
	return fmt.Sprintf("%s/%s", s.prefix, actionID)
}
