// Copyright 2023 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The go-cacher binary is a cacher helper program that cmd/go can use.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/aws/aws-sdk-go-v2/aws"

	"github.com/bradfitz/go-tool-cache/cacheproc"
	"github.com/bradfitz/go-tool-cache/cachers"
)

const defaultCacheKey = "v1"

type optionalEnvVarName string

const (
	s3CacheRegion        = optionalEnvVarName("GOCACHE_AWS_REGION")
	s3AwsAccessKey       = optionalEnvVarName("GOCACHE_AWS_ACCESS_KEY")
	s3AwsSecretAccessKey = optionalEnvVarName("GOCACHE_AWS_SECRET_ACCESS_KEY")
	s3AwsCredsProfile    = optionalEnvVarName("GOCACHE_AWS_CREDS_PROFILE")
	s3BucketName         = optionalEnvVarName("GOCACHE_S3_BUCKET")
	s3CacheKey           = optionalEnvVarName("GOCACHE_CACHE_KEY")
)

var (
	serverBase = flag.String("cache-server", "", "optional cache server HTTP prefix (scheme and authority only); should be low latency. empty means to not use one.")
	verbose    = flag.Bool("verbose", false, "be verbose")
)

func getAwsConfigFromEnv() (*aws.Config, error) {
	// read from env
	awsRegion := os.Getenv(string(s3CacheRegion))
	if awsRegion != "" {
		return nil, nil
	}
	accessKey := os.Getenv(string(s3AwsAccessKey))
	secretAccessKey := os.Getenv(string(s3AwsSecretAccessKey))
	if accessKey != "" && secretAccessKey != "" {
		cfg, err := config.LoadDefaultConfig(context.TODO(),
			config.WithRegion(awsRegion),
			config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
				Value: aws.Credentials{
					AccessKeyID:     accessKey,
					SecretAccessKey: secretAccessKey,
				},
			}))
		if err != nil {
			return nil, err
		}
		return &cfg, nil
	}
	credsProfile := os.Getenv(string(s3AwsCredsProfile))
	if credsProfile != "" {
		cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(awsRegion), config.WithSharedConfigProfile(credsProfile))
		if err != nil {
			return nil, err
		}
		return &cfg, nil
	}
	return nil, nil
}

func maybeS3Cache() (cachers.RemoteCache, error) {
	awsConfig, err := getAwsConfigFromEnv()
	if err != nil {
		return nil, err
	}
	bucket := os.Getenv(string(s3BucketName))
	if bucket == "" || awsConfig == nil {
		// We need at least name of bucket and valid aws config
		return nil, nil
	}
	cacheKey := os.Getenv(string(s3CacheKey))
	if cacheKey == "" {
		cacheKey = defaultCacheKey
	}
	s3Client := s3.NewFromConfig(*awsConfig)
	s3Cache := cachers.NewS3Cache(s3Client, bucket, cacheKey, *verbose)
	return s3Cache, nil
}

func getCache(local cachers.LocalCache, remote cachers.RemoteCache, verbose bool) cachers.LocalCache {
	if remote != nil {
		return cachers.NewCombinedCache(local, remote, verbose)
	}
	if verbose {
		return cachers.NewLocalCacheStates(local)
	}
	return local
}

func main() {
	flag.Parse()
	dir := os.Getenv("GOCACHE_DISK_DIR")
	if dir == "" {
		d, err := os.UserCacheDir()
		if err != nil {
			log.Fatal(err)
		}
		d = filepath.Join(d, "go-cacher")
		dir = d
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Fatal(err)
	}
	var localCache cachers.LocalCache = cachers.NewSimpleDiskCache(*verbose, dir)
	s3Cache, err := maybeS3Cache()
	if err != nil {
		log.Fatal(err)
	}
	proc := cacheproc.NewCacheProc(getCache(localCache, s3Cache, *verbose))
	if err := proc.Run(); err != nil {
		log.Fatal(err)
	}
}
