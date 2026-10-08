package main

import (
	"context"
	"maps"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mapEnv struct {
	m map[string]string
}

var _ Env = &mapEnv{}

func (m *mapEnv) Get(key string) string {
	return m.m[key]
}

func TestMaybeS3Cache(t *testing.T) {
	fullMap := map[string]string{
		envVarS3BucketName:         "bucket",
		envVarS3CacheRegion:        "region",
		envVarS3AwsAccessKey:       "accessKey",
		envVarS3AwsSecretAccessKey: "secretAccessKey",
	}

	t.Run("returns (nil, nil) when bucket is unset (S3 not configured)", func(t *testing.T) {
		m := map[string]string{}
		maps.Copy(m, fullMap)
		delete(m, envVarS3BucketName)
		client, err := maybeS3Cache(context.TODO(), &mapEnv{m: m})
		assert.NoError(t, err)
		assert.Nil(t, client)
	})

	// With bucket set the user is committed to S3; missing region or
	// credentials must surface as an error rather than silent fallback.
	for _, k := range []string{envVarS3CacheRegion, envVarS3AwsAccessKey, envVarS3AwsSecretAccessKey} {
		missing := map[string]string{}
		maps.Copy(missing, fullMap)
		delete(missing, k)
		t.Run("returns error when bucket set but "+k+" is missing", func(t *testing.T) {
			client, err := maybeS3Cache(context.TODO(), &mapEnv{m: missing})
			assert.Error(t, err)
			assert.Nil(t, client)
		})
	}

	t.Run("succeeds with all required vars", func(t *testing.T) {
		client, err := maybeS3Cache(context.TODO(), &mapEnv{m: fullMap})
		assert.NoError(t, err)
		assert.NotNil(t, client)
	})

	t.Run("succeeds with session token for STS/OIDC flows", func(t *testing.T) {
		m := map[string]string{}
		maps.Copy(m, fullMap)
		m[envVarS3AwsSessionToken] = "sess-token"
		client, err := maybeS3Cache(context.TODO(), &mapEnv{m: m})
		assert.NoError(t, err)
		assert.NotNil(t, client)
	})

	t.Run("custom URL supplies default region for S3-compatible endpoints", func(t *testing.T) {
		m := map[string]string{
			envVarS3BucketName:         "bucket",
			envVarS3CacheURL:           "http://localhost:9000",
			envVarS3AwsAccessKey:       "accessKey",
			envVarS3AwsSecretAccessKey: "secretAccessKey",
		}
		client, err := maybeS3Cache(context.TODO(), &mapEnv{m: m})
		assert.NoError(t, err)
		assert.NotNil(t, client)
	})
}

func TestMaybeHttpCache(t *testing.T) {
	t.Run("should return nil if "+envVarHttpCacheServerBase+" is missing", func(t *testing.T) {
		env := &mapEnv{m: map[string]string{}}
		client, err := maybeHttpCache(env)
		assert.NoError(t, err)
		assert.Nil(t, client)
	})

	t.Run("should return cache if "+envVarHttpCacheServerBase+" env vars exists", func(t *testing.T) {
		env := &mapEnv{
			m: map[string]string{
				envVarHttpCacheServerBase: "http://localhost:8080",
			},
		}
		client, err := maybeHttpCache(env)
		assert.NoError(t, err)
		assert.NotNil(t, client)
	})
}
