// Copyright 2023 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// The go-cacher binary is a cacher helper program that cmd/go can use.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/logging"

	"github.com/aws/aws-sdk-go-v2/config"

	"github.com/bradfitz/go-tool-cache/cacheproc"
	"github.com/bradfitz/go-tool-cache/cachers"
)

const defaultS3Prefix = "go-cache"
const envVariablePrefix = "GOCACHE_"

func stringFlag(name, value, envVariable string, usage string) *string {
	defaultVal := value
	if envVal, ok := os.LookupEnv(envVariablePrefix + envVariable); ok {
		defaultVal = envVal
	}
	return flag.String(name, defaultVal, usage)
}

func intFlag(name string, value int, envVariable string, usage string) *int {
	defaultVal := value
	if envVal, ok := os.LookupEnv(envVariablePrefix + envVariable); ok {
		var err error
		defaultVal, err = strconv.Atoi(envVal)
		if err != nil {
			log.Fatalf("failed to parse %s as int: %w", name, err)
		}
	}
	return flag.Int(name, defaultVal, usage)
}

func stringArg(pos int, args []string, envVariable string) string {
	if pos < len(args) {
		return args[pos]
	}
	if envVal, ok := os.LookupEnv(envVariablePrefix + envVariable); ok {
		return envVal
	}
	log.Fatalf("missing required argument %d or environment variable %s", pos, envVariable)
	return ""
}

var userCacheDir, _ = os.UserCacheDir()

type awsConfig struct {
	region       string
	profile      string
	accessKey    string
	secretKey    string
	sessionToken string
}

type s3CacheConfig struct {
	verbose       int
	s3Prefix      string
	localCacheDir string
	bucket        string
	queueLen      int
	workers       int
	metCSV        string
	aws           awsConfig
}

// logHandler implements slog.Handler to print logs nicely
// mostly this was an exercise to use slog, probably not the best choice here TBH
type logHandler struct {
	Out    io.Writer
	Level  slog.Level
	attrs  []slog.Attr
	groups []string
}

func (h *logHandler) Enabled(_ context.Context, l slog.Level) bool {
	return l >= h.Level
}

func (h *logHandler) Handle(_ context.Context, r slog.Record) error {
	s := r.Level.String()[:1]
	if len(h.groups) > 0 {
		s += " " + strings.Join(h.groups, ".") + ":"
	}
	s += " " + r.Message
	attrs := h.attrs
	r.Attrs(func(a slog.Attr) bool {
		attrs = append(attrs, a)
		return true
	})
	for i, a := range attrs {
		if i == 0 {
			s += " {"
		}
		s += fmt.Sprintf("%s=%q", a.Key, a.Value)
		if i < len(attrs)-1 {
			s += " "
		} else {
			s += "}"
		}
	}
	s += "\n"
	_, err := h.Out.Write([]byte(s))
	return err
}

func (h *logHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &logHandler{
		Out:    h.Out,
		Level:  h.Level,
		attrs:  append(h.attrs, attrs...),
		groups: h.groups,
	}
}

func (h *logHandler) WithGroup(name string) slog.Handler {
	if h.groups == nil {
		h.groups = []string{}
	}
	return &logHandler{
		Out:    h.Out,
		Level:  h.Level,
		attrs:  h.attrs,
		groups: append(h.groups, name),
	}
}

// Logf allows us to also implement AWS's logging.Logger
func (h *logHandler) Logf(cls logging.Classification, format string, args ...interface{}) {
	var l slog.Level
	switch cls {
	case logging.Debug:
		l = slog.LevelDebug
	case logging.Warn:
		l = slog.LevelWarn
	default:
		l = slog.LevelDebug
	}

	_ = h.Handle(context.Background(), slog.Record{
		Level:   l,
		Message: fmt.Sprintf(format, args...),
	})
}

var levelTrace = slog.Level(slog.LevelDebug - 4)

func parseConfig() s3CacheConfig {
	var defaultLocalCacheDir = filepath.Join(userCacheDir, "go-cacher")
	flagVerbose := intFlag("v", 0, "VERBOSE", "logging verbosity; 0=error, 1=warn, 2=info, 3=debug, 4=trace")
	flagS3Prefix := stringFlag("s3-prefix", defaultS3Prefix, "S3_PREFIX", "s3 prefix")
	flagLocalCacheDir := stringFlag("local-cache-dir", defaultLocalCacheDir, "DISK_DIR", "local cache directory")
	flagQueueLen := intFlag("queue-len", 0, "QUEUE_LEN", "length of the queue for async s3 cache (0=synchronous)")
	flagWorkers := intFlag("workers", 1, "WORKERS", "number of workers for async s3 cache (1=synchronous)")
	flagMetCSV := stringFlag("metrics-csv", "", "METRICS_CSV", "write s3 Get/Put metrics to a CSV file (empty=disabled)")
	flagAwsProfile := stringFlag("aws-profile", "", "AWS_CREDS_PROFILE", "AWS profile to use")
	flagAwsRegion := stringFlag("aws-region", "", "AWS_REGION", "AWS region to use")
	flag.Parse()
	bucket := stringArg(0, flag.Args(), "S3_BUCKET")
	return s3CacheConfig{
		verbose:       *flagVerbose,
		s3Prefix:      *flagS3Prefix,
		localCacheDir: *flagLocalCacheDir,
		bucket:        bucket,
		queueLen:      *flagQueueLen,
		workers:       *flagWorkers,
		metCSV:        *flagMetCSV,
		aws: awsConfig{
			region:  *flagAwsRegion,
			profile: *flagAwsProfile,
			// We don't want to be getting those as flags, only from the environment
			accessKey:    os.Getenv(envVariablePrefix + "AWS_ACCESS_KEY"),
			secretKey:    os.Getenv(envVariablePrefix + "AWS_SECRET_ACCESS_KEY"),
			sessionToken: os.Getenv(envVariablePrefix + "AWS_SESSION_TOKEN"),
		},
	}
}

func getAwsConfig(cfg awsConfig, clientLogMode aws.ClientLogMode, h *logHandler) (aws.Config, error) {
	// Without any special configuration - we just use the default config loader
	// User can customize behavior by providing specific user creds, profile, or region
	loadOpts := []func(*config.LoadOptions) error{
		config.WithClientLogMode(clientLogMode),
		config.WithLogger(h),
	}
	if cfg.region != "" {
		loadOpts = append(loadOpts, config.WithRegion(cfg.region))
	}
	if cfg.accessKey != "" && cfg.secretKey != "" {
		loadOpts = append(loadOpts, config.WithCredentialsProvider(
			credentials.StaticCredentialsProvider{
				Value: aws.Credentials{
					AccessKeyID:     cfg.accessKey,
					SecretAccessKey: cfg.secretKey,
					SessionToken:    cfg.sessionToken,
				},
			}))
	} else if cfg.profile != "" {
		loadOpts = append(loadOpts, config.WithSharedConfigProfile(cfg.profile))
	}
	return config.LoadDefaultConfig(context.TODO(), loadOpts...)
}

func main() {
	cfg := parseConfig()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logLevel := slog.Level(cfg.verbose*-4 + 8)
	h := &logHandler{
		Level: logLevel,
		Out:   os.Stderr,
	}

	slog.SetDefault(slog.New(h))

	slog.Info(fmt.Sprintf("Log level: %s", logLevel))
	slog.Info("starting cache")
	var clientLogMode aws.ClientLogMode
	if logLevel <= levelTrace {
		clientLogMode = aws.LogRetries | aws.LogRequest
	}
	awsConfig, err := getAwsConfig(cfg.aws, clientLogMode, h)
	if err != nil {
		log.Fatal("S3 cache disabled; failed to load AWS config: ", err)
	}
	// TODO: maybe an option to use the async s3 cache vs the sync one?
	diskCacher := cachers.NewDiskCache(cfg.localCacheDir)
	cacher := cachers.NewDiskAsyncS3Cache(
		diskCacher,
		s3.NewFromConfig(awsConfig),
		cfg.bucket,
		cfg.s3Prefix,
		cfg.queueLen,
		cfg.workers,
	)
	proc := cacheproc.NewCacheProc(cacher)
	if err := proc.Run(ctx); err != nil {
		slog.Error(err.Error())
		os.Exit(1)
	}
	if logLevel <= slog.LevelInfo {
		_, _ = fmt.Fprintln(os.Stderr, "disk stats: \n"+diskCacher.GetCounts().Summary())
		_, _ = fmt.Fprintln(os.Stderr, "s3 stats: \n"+cacher.GetCounts().Summary())
	}
	if cfg.metCSV != "" {
		f, err := os.Create(cfg.metCSV)
		if err != nil {
			slog.Error(fmt.Sprintf("failed to create metrics file: %v", err))
		} else {
			defer f.Close()
			_ = cacher.GetCounts().CSV(f, true)
		}

	}
}
