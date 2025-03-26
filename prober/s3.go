// Copyright 2024 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package prober

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	signer "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/prometheus/blackbox_exporter/config"
)

var (
	probeS3SuccessGaugeOpts = prometheus.GaugeOpts{
		Name: "probe_s3_success",
		Help: "Returns 1 if the S3 presigned URL request was successful",
	}
	probeS3DurationGaugeOpts = prometheus.GaugeOpts{
		Name: "probe_s3_duration_seconds",
		Help: "Duration of S3 request in seconds",
	}
	urlCache = &presignedURLCache{
		urls:    make(map[string]cachedURL),
		regions: make(map[string]string),
		clients: make(map[string]*s3.Client),
	}
	urlExpiration = time.Hour
)

type presignedURLCache struct {
	sync.Mutex
	urls    map[string]cachedURL
	regions map[string]string
	clients map[string]*s3.Client
}

type cachedURL struct {
	url       string
	expiresAt time.Time
}

// ProbeS3 checks if a presigned S3 URL can be generated and accessed for the given bucket path
func ProbeS3(ctx context.Context, target string, module config.Module, registry *prometheus.Registry, logger *slog.Logger) bool {
	probeS3SuccessGauge := prometheus.NewGauge(probeS3SuccessGaugeOpts)
	probeS3DurationGauge := prometheus.NewGauge(probeS3DurationGaugeOpts)
	registry.MustRegister(probeS3SuccessGauge)
	registry.MustRegister(probeS3DurationGauge)

	success := 0.0
	defer func() {
		probeS3SuccessGauge.Set(success)
	}()

	method := module.S3.Method
	if method == "" {
		method = "GET"
	}

	// Parse bucket and key from target
	bucket, key := parseS3Target(target)
	if bucket == "" {
		logger.Error("Invalid S3 target format. Expected: bucket/key")
		return false
	}

	// Get presigned URL
	url, err := getPresignedURL(ctx, bucket, key, method)
	if err != nil {
		logger.Error("Failed to get presigned URL", "error", err)
		return false
	}

	// Make HTTP request to presigned URL
	timeout := 5 * time.Second
	if module.S3.Timeout != 0 {
		timeout = module.S3.Timeout * time.Second
	}

	client := &http.Client{
		Timeout: timeout,
	}

	var req *http.Request
	if method == "PUT" {
		var body []byte
		if module.S3.PutFile != "" {
			body, err = os.ReadFile(module.S3.PutFile)
			if err != nil {
				logger.Error("Failed to read PUT file", "error", err)
				return false
			}
		}
		req, err = http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	} else {
		req, err = http.NewRequestWithContext(ctx, method, url, nil)
	}

	if err != nil {
		logger.Error("Failed to create S3 request", "error", err)
		return false
	}

	start := time.Now()
	resp, err := client.Do(req)
	duration := time.Since(start)
	probeS3DurationGauge.Set(duration.Seconds())
	logger.Info("S3 request completed", "duration", duration)

	if err != nil {
		logger.Error("Failed to execute S3 request", "error", err)
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		logger.Error("Unexpected status code", "status", resp.StatusCode)
		return false
	}

	success = 1.0
	return true
}

// parseS3Target splits an S3 target string into bucket and key
// Expected format: bucket/key
func parseS3Target(target string) (bucket, key string) {
	parts := strings.SplitN(target, "/", 2)
	if len(parts) != 2 {
		return "", ""
	}
	return parts[0], parts[1]
}

// getPresignedURL returns a cached URL if valid, otherwise generates a new one
func getPresignedURL(ctx context.Context, bucket, key, method string) (string, error) {
	cacheKey := method + ":" + bucket + "/" + key

	urlCache.Lock()
	defer urlCache.Unlock()

	if cached, ok := urlCache.urls[cacheKey]; ok {
		if time.Now().Before(cached.expiresAt) {
			return cached.url, nil
		}
	}

	var (
		presignedReq *signer.PresignedHTTPRequest
		err          error
		expiresAt    = time.Now().Add(urlExpiration)
	)

	region, ok := urlCache.regions[bucket]
	if !ok {
		awsCfg, err := awsconfig.LoadDefaultConfig(ctx)
		if err != nil {
			return "", fmt.Errorf("failed to load AWS config: %w", err)
		}

		output, err := s3.NewFromConfig(awsCfg).HeadBucket(ctx, &s3.HeadBucketInput{
			Bucket: &bucket,
		})
		if err != nil {
			return "", fmt.Errorf("failed to get bucket region: %w", err)
		}

		region = *output.BucketRegion
		urlCache.regions[bucket] = region
	}

	s3Client, ok := urlCache.clients[region]
	if !ok {
		// Create new client if not in cache
		awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
		if err != nil {
			return "", fmt.Errorf("failed to load AWS config: %w", err)
		}
		s3Client = s3.NewFromConfig(awsCfg)
		urlCache.clients[region] = s3Client
	}

	presignClient := s3.NewPresignClient(s3Client)

	switch method {
	case "GET":
		presignedReq, err = presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
			Bucket: &bucket,
			Key:    &key,
		}, s3.WithPresignExpires(urlExpiration))
	case "PUT":
		presignedReq, err = presignClient.PresignPutObject(ctx, &s3.PutObjectInput{
			Bucket: &bucket,
			Key:    &key,
		}, s3.WithPresignExpires(urlExpiration))
	default:
		return "", fmt.Errorf("unsupported method: %s", method)
	}

	if err != nil {
		return "", err
	}

	urlCache.urls[cacheKey] = cachedURL{
		url:       presignedReq.URL,
		expiresAt: expiresAt,
	}

	return presignedReq.URL, nil
}
