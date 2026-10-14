package cachers

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"cloud.google.com/go/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testBucket = "bucket"

type fakeObject struct {
	data     []byte
	metadata map[string]string
}

// fakeGCS serves the two endpoints GCSCache uses: XML object reads and JSON
// multipart uploads, including the ifGenerationMatch=0 precondition.
type fakeGCS struct {
	mu        sync.Mutex
	objects   map[string]fakeObject
	forbidden bool
}

func (f *fakeGCS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.forbidden {
		writeGCSError(w, http.StatusForbidden)
		return
	}
	switch {
	case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/upload/storage/v1/b/"+testBucket+"/o"):
		f.upload(w, r)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/"+testBucket+"/"):
		f.read(w, strings.TrimPrefix(r.URL.Path, "/"+testBucket+"/"))
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeGCS) object(name string) (fakeObject, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	obj, ok := f.objects[name]
	return obj, ok
}

func (f *fakeGCS) setObject(name string, obj fakeObject) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[name] = obj
}

func (f *fakeGCS) setForbidden() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.forbidden = true
}

func (f *fakeGCS) read(w http.ResponseWriter, name string) {
	obj, ok := f.objects[name]
	if !ok {
		http.NotFound(w, nil)
		return
	}
	for k, v := range obj.metadata {
		w.Header().Set("X-Goog-Meta-"+k, v)
	}
	w.Header().Set("X-Goog-Generation", "1")
	w.Header().Set("X-Goog-Metageneration", "1")
	w.Header().Set("Content-Length", strconv.Itoa(len(obj.data)))
	_, _ = w.Write(obj.data)
}

func (f *fakeGCS) upload(w http.ResponseWriter, r *http.Request) {
	_, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	parts := multipart.NewReader(r.Body, params["boundary"])
	var attrs struct {
		Name     string            `json:"name"`
		Metadata map[string]string `json:"metadata"`
	}
	part, err := parts.NextPart()
	if err == nil {
		err = json.NewDecoder(part).Decode(&attrs)
	}
	if err == nil {
		part, err = parts.NextPart()
	}
	var data []byte
	if err == nil {
		data, err = io.ReadAll(part)
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if _, exists := f.objects[attrs.Name]; exists && r.URL.Query().Get("ifGenerationMatch") == "0" {
		writeGCSError(w, http.StatusPreconditionFailed)
		return
	}
	f.objects[attrs.Name] = fakeObject{data: data, metadata: attrs.Metadata}
	w.Header().Set("Content-Type", "application/json")
	crc := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
	_ = json.NewEncoder(w).Encode(map[string]any{
		"bucket":         testBucket,
		"crc32c":         base64.StdEncoding.EncodeToString(binary.BigEndian.AppendUint32(nil, crc)),
		"name":           attrs.Name,
		"size":           strconv.Itoa(len(data)),
		"generation":     "1",
		"metageneration": "1",
		"metadata":       attrs.Metadata,
	})
}

func writeGCSError(w http.ResponseWriter, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = io.WriteString(w, `{"error":{"code":`+strconv.Itoa(code)+`,"message":"`+http.StatusText(code)+`"}}`)
}

func newTestGCSCache(t *testing.T) (*GCSCache, *fakeGCS) {
	t.Helper()
	fake := &fakeGCS{objects: map[string]fakeObject{}}
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	t.Setenv("STORAGE_EMULATOR_HOST", server.URL)
	t.Setenv("GOOS", "linux")
	t.Setenv("GOARCH", "amd64")
	client, err := storage.NewClient(context.Background())
	require.NoError(t, err)
	cache := NewGCSCache(client, testBucket, "key", false)
	t.Cleanup(func() { _ = cache.Close() })
	return cache, fake
}

func TestGCSCache(t *testing.T) {
	ctx := context.Background()

	t.Run("miss returns no output and no error", func(t *testing.T) {
		cache, _ := newTestGCSCache(t)
		outputID, _, output, err := cache.Get(ctx, "a1")
		assert.NoError(t, err)
		assert.Empty(t, outputID)
		assert.Nil(t, output)
	})

	t.Run("put then get round-trips output and outputID", func(t *testing.T) {
		cache, fake := newTestGCSCache(t)
		require.NoError(t, cache.Put(ctx, "a1", "o1", 5, strings.NewReader("hello")))
		_, stored := fake.object("cache/key/amd64/linux/a1")
		assert.True(t, stored)

		outputID, size, output, err := cache.Get(ctx, "a1")
		require.NoError(t, err)
		defer output.Close() //nolint:errcheck
		data, err := io.ReadAll(output)
		require.NoError(t, err)
		assert.Equal(t, "o1", outputID)
		assert.Equal(t, int64(5), size)
		assert.Equal(t, "hello", string(data))
	})

	t.Run("empty output round-trips", func(t *testing.T) {
		cache, _ := newTestGCSCache(t)
		require.NoError(t, cache.Put(ctx, "a1", "o1", 0, bytes.NewReader(nil)))
		outputID, size, output, err := cache.Get(ctx, "a1")
		require.NoError(t, err)
		defer output.Close() //nolint:errcheck
		assert.Equal(t, "o1", outputID)
		assert.Zero(t, size)
	})

	t.Run("put keeps an existing entry", func(t *testing.T) {
		cache, fake := newTestGCSCache(t)
		require.NoError(t, cache.Put(ctx, "a1", "o1", 5, strings.NewReader("first")))
		assert.NoError(t, cache.Put(ctx, "a1", "o2", 6, strings.NewReader("second")))
		obj, _ := fake.object("cache/key/amd64/linux/a1")
		assert.Equal(t, "first", string(obj.data))
	})

	t.Run("put rejects a body shorter than size", func(t *testing.T) {
		cache, fake := newTestGCSCache(t)
		assert.Error(t, cache.Put(ctx, "a1", "o1", 10, strings.NewReader("short")))
		_, stored := fake.object("cache/key/amd64/linux/a1")
		assert.False(t, stored)
	})

	t.Run("object without outputID is an error", func(t *testing.T) {
		cache, fake := newTestGCSCache(t)
		fake.setObject("cache/key/amd64/linux/a1", fakeObject{data: []byte("x")})
		_, _, output, err := cache.Get(ctx, "a1")
		assert.Error(t, err)
		assert.Nil(t, output)
	})

	t.Run("forbidden get is a miss, forbidden put is an error", func(t *testing.T) {
		cache, fake := newTestGCSCache(t)
		fake.setForbidden()
		outputID, _, output, err := cache.Get(ctx, "a1")
		assert.NoError(t, err)
		assert.Empty(t, outputID)
		assert.Nil(t, output)
		assert.Error(t, cache.Put(ctx, "a1", "o1", 5, strings.NewReader("hello")))
	})
}
