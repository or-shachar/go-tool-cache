// Copyright 2023 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The go-cacher binary is a cacher helper program that cmd/go can use.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/bradfitz/go-tool-cache/cacheproc"
	"github.com/bradfitz/go-tool-cache/cachers"
)

const defaultCacheKey = "v1"

// All the following env variable names are optional
const (
	// path to local disk directory. defaults to os.UserCacheDir()/go-cacher
	envVarDiskCacheDir = "GOCACHE_DISK_DIR"

	// S3 cache
	envVarS3CacheRegion        = "GOCACHE_AWS_REGION"
	envVarS3AwsAccessKey       = "GOCACHE_AWS_ACCESS_KEY"
	envVarS3AwsSecretAccessKey = "GOCACHE_AWS_SECRET_ACCESS_KEY"
	envVarS3AwsSessionToken    = "GOCACHE_AWS_SESSION_TOKEN"
	envVarS3AwsCredsProfile    = "GOCACHE_AWS_CREDS_PROFILE"
	envVarS3BucketName         = "GOCACHE_S3_BUCKET"
	envVarS3CacheKey           = "GOCACHE_CACHE_KEY"

	// S3-compatible endpoint (e.g. MinIO, LocalStack).
	// When set, the S3 client uses path-style addressing and this endpoint
	// instead of the AWS-resolved one. If GOCACHE_AWS_REGION is empty,
	// a dummy region of "us-east-1" is used so the SDK accepts the config.
	envVarS3CacheURL = "GOCACHE_S3_URL"

	// HTTP cache - optional cache server HTTP prefix (scheme and authority only);
	envVarHttpCacheServerBase = "GOCACHE_HTTP_SERVER_BASE"
)

var (
	verbose = flag.Bool("verbose", false, "be verbose")
)

type Env interface {
	Get(key string) string
}

type osEnv struct{}

func (osEnv) Get(key string) string {
	return os.Getenv(key)
}

func getAwsConfigFromEnv(ctx context.Context, env Env) (*aws.Config, error) {
	awsRegion := env.Get(envVarS3CacheRegion)
	if awsRegion == "" {
		// S3-compatible endpoints (MinIO etc.) don't care about region but
		// the SDK still requires one. Supply a dummy only when a custom URL
		// is set; otherwise require an explicit region so AWS misconfigs
		// aren't masked.
		if env.Get(envVarS3CacheURL) != "" {
			awsRegion = "us-east-1"
		} else {
			return nil, fmt.Errorf("%s not set", envVarS3CacheRegion)
		}
	}

	accessKey := env.Get(envVarS3AwsAccessKey)
	secretAccessKey := env.Get(envVarS3AwsSecretAccessKey)
	sessionToken := env.Get(envVarS3AwsSessionToken)
	if accessKey != "" && secretAccessKey != "" {
		cfg, err := config.LoadDefaultConfig(ctx,
			config.WithRegion(awsRegion),
			config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
				Value: aws.Credentials{
					AccessKeyID:     accessKey,
					SecretAccessKey: secretAccessKey,
					SessionToken:    sessionToken,
				},
			}))
		if err != nil {
			return nil, err
		}
		return &cfg, nil
	}

	if credsProfile := env.Get(envVarS3AwsCredsProfile); credsProfile != "" {
		cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(awsRegion), config.WithSharedConfigProfile(credsProfile))
		if err != nil {
			return nil, err
		}
		return &cfg, nil
	}

	return nil, fmt.Errorf("no S3 credentials: set %s and %s (optionally with %s for STS/OIDC flows) or %s",
		envVarS3AwsAccessKey, envVarS3AwsSecretAccessKey, envVarS3AwsSessionToken, envVarS3AwsCredsProfile)
}

func maybeS3Cache(ctx context.Context, env Env) (cachers.RemoteCache, error) {
	bucket := env.Get(envVarS3BucketName)
	if bucket == "" {
		return nil, nil
	}
	// Bucket is set, so the user intends to use S3. Treat any config
	// failure as a hard error — previously these were swallowed and the
	// build silently ran with no remote cache.
	awsConfig, err := getAwsConfigFromEnv(ctx, env)
	if err != nil {
		return nil, fmt.Errorf("%s set but S3 cache cannot be configured: %w", envVarS3BucketName, err)
	}
	cacheKey := env.Get(envVarS3CacheKey)
	if cacheKey == "" {
		cacheKey = defaultCacheKey
	}
	s3Client := s3.NewFromConfig(*awsConfig, func(o *s3.Options) {
		if u := env.Get(envVarS3CacheURL); u != "" {
			o.BaseEndpoint = &u
			o.UsePathStyle = true
		}
	})
	return cachers.NewS3Cache(s3Client, bucket, cacheKey, *verbose), nil
}

func getCache(ctx context.Context, env Env, verbose bool) cachers.LocalCache {
	dir := getDir(env)
	var local cachers.LocalCache = cachers.NewSimpleDiskCache(verbose, dir)

	remote, err := maybeS3Cache(ctx, env)
	if err != nil {
		log.Fatal(err)
	}
	if remote == nil {
		remote, err = maybeHttpCache(env)
		if err != nil {
			log.Fatal(err)
		}
	}

	if remote != nil {
		return cachers.NewCombinedCache(local, remote, verbose)
	}
	if verbose {
		return cachers.NewLocalCacheStates(local)
	}
	return local
}

func maybeHttpCache(env Env) (cachers.RemoteCache, error) {
	serverBase := env.Get(envVarHttpCacheServerBase)
	if serverBase == "" {
		return nil, nil
	}
	return cachers.NewHttpCache(serverBase, *verbose), nil
}

func getDir(env Env) string {
	dir := env.Get(envVarDiskCacheDir)
	if dir == "" {
		d, err := os.UserCacheDir()
		if err != nil {
			log.Fatal(err)
		}
		d = filepath.Join(d, "go-cacher")
		dir = d
	}
	return dir
}

func main() {
	flag.Parse()
	env := &osEnv{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cache := getCache(ctx, env, *verbose)
	proc := cacheproc.NewCacheProc(cache)
	if err := proc.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
