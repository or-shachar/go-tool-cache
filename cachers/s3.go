package cachers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/smithy-go"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Cache struct {
	Bucket string
	cfg    *aws.Config
	// diskCache is where to write the output files to local disk, as required by the
	// cache protocol.
	diskCache *DiskCache

	prefix string
	// verbose optionally specifies whether to log verbose messages.
	verbose bool

	s3Client *s3.Client

	bytesDownloaded       atomic.Int64
	bytesUploaded         atomic.Int64
	downloadCount         int64
	uploadCount           int64
	avgBytesDownloadSpeed float64
	avgBytesUploadSpeed   float64
	downloadsStatsMutex   *sync.Mutex
	uploadStatsMutex      *sync.Mutex
}

func NewS3Cache(bucketName string, cfg *aws.Config, cacheKey string, disk *DiskCache, verbose bool) *S3Cache {
	// get current architecture
	arc := runtime.GOARCH
	// get current operating system
	os := runtime.GOOS
	// get current version of Go
	ver := strings.ReplaceAll(strings.ReplaceAll(runtime.Version(), " ", "-"), ":", "-")
	prefix := fmt.Sprintf("cache/%s/%s/%s/%s", cacheKey, arc, os, ver)
	log.Printf("S3Cache: configured to s3://%s/%s", bucketName, prefix)
	return &S3Cache{
		Bucket:              bucketName,
		cfg:                 cfg,
		diskCache:           disk,
		prefix:              prefix,
		verbose:             verbose,
		downloadsStatsMutex: new(sync.Mutex),
		uploadStatsMutex:    new(sync.Mutex),
	}
}

func (c *S3Cache) client(ctx context.Context) (*s3.Client, error) {
	if c.s3Client != nil {
		return c.s3Client, nil
	}
	c.s3Client = s3.NewFromConfig(*c.cfg)
	return c.s3Client, nil
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

func (c *S3Cache) Get(ctx context.Context, actionID string) (outputID, diskPath string, err error) {
	outputID, diskPath, err = c.diskCache.Get(ctx, actionID)
	if err == nil && outputID != "" {
		return outputID, diskPath, nil
	}
	client, err := c.client(ctx)
	if err != nil {
		if c.verbose {
			log.Printf("error getting S3 client: %v", err)
		}
		return "", "", err
	}
	actionKey := c.actionKey(actionID)
	result, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &c.Bucket,
		Key:    &actionKey,
	})
	// handle object not found
	if isNotFoundError(err) {
		return "", "", nil
	} else if err != nil {
		if c.verbose {
			log.Printf("error S3 get for %s:  %v", actionKey, err)
		}
		return "", "", fmt.Errorf("unexpected S3 get for %s:  %v", actionKey, err)
	}
	defer result.Body.Close()
	var av ActionValue
	body, err := io.ReadAll(result.Body)
	if err != nil {
		return "", "", err
	}
	if err := json.Unmarshal(body, &av); err != nil {
		if c.verbose {
			log.Printf("error unmarshalling JSON for %s:  %v", actionKey, err)
		}
		return "", "", err
	}

	outputID = av.OutputID

	var putBody io.Reader
	if av.Size == 0 {
		putBody = bytes.NewReader(nil)
		diskPath, err = c.diskCache.Put(ctx, actionID, outputID, av.Size, putBody)
	} else {
		outputKey := c.outputKey(outputID)
		outputResult, getOutputErr := client.GetObject(ctx, &s3.GetObjectInput{
			Bucket: &c.Bucket,
			Key:    &outputKey,
		})
		if isNotFoundError(getOutputErr) {
			// handle object not found
			return "", "", nil
		} else if getOutputErr != nil {
			if c.verbose {
				log.Printf("error S3 get for %s:  %v", outputKey, getOutputErr)
			}
			return "", "", fmt.Errorf("unexpected S3 get for %s:  %v", outputKey, getOutputErr)
		}
		downloadFunc := func() error {
			defer outputResult.Body.Close()
			putBody = outputResult.Body
			diskPath, err = c.diskCache.Put(ctx, actionID, outputID, av.Size, putBody)
			if err != nil {
				return err
			}
			c.bytesDownloaded.Add(av.Size)
			return nil
		}
		if c.verbose {
			speed, err := DoAndMeasureSpeed(av.Size, downloadFunc)
			if err == nil {
				c.downloadsStatsMutex.Lock()
				c.avgBytesDownloadSpeed = newAverage(c.avgBytesDownloadSpeed, c.downloadCount, speed)
				c.downloadCount++
				c.downloadsStatsMutex.Unlock()
			}
		} else {
			err = downloadFunc()
		}
	}
	return outputID, diskPath, err
}

func (c *S3Cache) actionKey(actionID string) string {
	return fmt.Sprintf("%s/actions/%s", c.prefix, actionID)
}

func (c *S3Cache) outputKey(outputID string) string {
	return fmt.Sprintf("%s/output/%s", c.prefix, outputID)
}

func (c *S3Cache) Put(ctx context.Context, actionID, outputID string, size int64, body io.Reader) (diskPath string, _ error) {
	// Write to disk locally as we write it remotely, as we need to guarantee
	// it's on disk locally for the caller.
	var readerForDisk io.Reader
	var readerForS3 bytes.Buffer

	if size == 0 {
		// Special case the empty file so NewRequest sets "Content-Length: 0",
		// as opposed to thinking we didn't set it and not being able to sniff its size
		// from the type.
		readerForDisk = bytes.NewReader(nil)
	} else {
		readerForDisk = io.TeeReader(body, &readerForS3)
	}

	diskPath, err := c.diskCache.Put(ctx, actionID, outputID, size, readerForDisk)
	if err != nil {
		return "", err
	}

	client, err := c.client(ctx)
	if err != nil {
		return "", err
	}
	av := ActionValue{
		OutputID: outputID,
		Size:     size,
	}
	avj, err := json.Marshal(av)
	if err == nil {
		actionKey := c.actionKey(actionID)
		_, err = client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: &c.Bucket,
			Key:    &actionKey,
			Body:   bytes.NewReader(avj),
		})
	}
	if size > 0 && err == nil {
		c.uploadOutput(ctx, outputID, client, readerForS3, size)
	}
	c.bytesUploaded.Add(size)

	return
}

func (c *S3Cache) uploadOutput(ctx context.Context, outputID string, client *s3.Client, readerForS3 bytes.Buffer, size int64) {
	outputKey := c.outputKey(outputID)
	putObjectFunc := func() error {
		_, err := client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:        &c.Bucket,
			Key:           &outputKey,
			Body:          &readerForS3,
			ContentLength: size,
		})
		return err
	}
	if c.verbose {
		speed, err := DoAndMeasureSpeed(size, putObjectFunc)
		if err == nil {
			c.uploadStatsMutex.Lock()
			c.avgBytesUploadSpeed = newAverage(c.avgBytesUploadSpeed, c.uploadCount, speed)
			c.uploadCount++
			c.uploadStatsMutex.Unlock()
		}
	} else {
		_ = putObjectFunc()
	}
}

func (c *S3Cache) KBDownloaded() int64 {
	return c.bytesDownloaded.Load() / 1024
}

func (c *S3Cache) KBUploaded() int64 {
	return c.bytesUploaded.Load() / 1024
}

func (c *S3Cache) AvgKBDownloadSpeed() float64 {
	return c.avgBytesDownloadSpeed / 1024
}

func (c *S3Cache) AvgKBUploadSpeed() float64 {
	return c.avgBytesUploadSpeed / 1024
}

func newAverage(oldAverage float64, count int64, newValue float64) float64 {
	return (oldAverage*float64(count) + newValue) / float64(count+1)
}

func DoAndMeasureSpeed(dataSize int64, functionOnData func() error) (float64, error) {
	start := time.Now()
	err := functionOnData()
	elapsed := time.Since(start)
	speed := float64(dataSize) / elapsed.Seconds()
	return speed, err
}
