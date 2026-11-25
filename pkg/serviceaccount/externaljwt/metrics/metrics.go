/*
Copyright 2024 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package metrics 提供 external-jwt-signer 相关的指标。
// 监控与外部 JWT 签名器的 gRPC 通信：密钥同步、签名请求、请求延迟等。
// 帮助诊断外部签名器的可用性和性能问题。
package metrics

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const (
	namespace = "apiserver"
	subsystem = "externaljwt"
)

var (
	// lastKeyFetchTimeStamp 记录最后一次成功获取密钥的时间戳。
	// 用于监控密钥同步是否正常，如果长时间不更新说明同步失败。
	lastKeyFetchTimeStamp = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "fetch_keys_success_timestamp",
			Help:           "Unix Timestamp in seconds of the last successful FetchKeys request",
			StabilityLevel: metrics.ALPHA,
		},
		nil,
	)

	// dataTimeStamp 记录外部签名器返回的密钥数据时间戳。
	// 用于判断本地缓存的密钥是否与外部签名器一致。
	dataTimeStamp = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "fetch_keys_data_timestamp",
			Help:           "Unix Timestamp in seconds of the last successful FetchKeys data_timestamp value returned by the external signer",
			StabilityLevel: metrics.ALPHA,
		},
		nil,
	)

	// totalKeyFetch 记录 FetchKeys 请求的总次数，按错误码分类。
	// 用于监控密钥同步的频率和失败率。
	totalKeyFetch = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "fetch_keys_request_total",
			Help:           "Total attempts at syncing supported JWKs",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"code"},
	)

	// tokenGenReqTotal 记录 Sign 请求的总次数，按错误码分类。
	// 用于监控 Token 生成的频率和失败率。
	tokenGenReqTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "sign_request_total",
			Help:           "Total attempts at signing JWT",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"code"},
	)

	// requestDurationSeconds 记录 gRPC 请求的延迟分布。
	// 按方法名（FetchKeys/Sign）和错误码分类，用于性能监控。
	requestDurationSeconds = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Namespace:      namespace,
			Subsystem:      subsystem,
			Name:           "request_duration_seconds",
			Help:           "Request duration and time for calls to external-jwt-signer",
			Buckets:        []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60},
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"method", "code"},
	)
)

var registerMetrics sync.Once

// RegisterMetrics 注册所有 external-jwt-signer 相关的指标。
func RegisterMetrics() {
	registerMetrics.Do(func() {
		legacyregistry.MustRegister(lastKeyFetchTimeStamp)
		legacyregistry.MustRegister(dataTimeStamp)
		legacyregistry.MustRegister(totalKeyFetch)
		legacyregistry.MustRegister(tokenGenReqTotal)
		legacyregistry.MustRegister(requestDurationSeconds)
	})
}

// RecordFetchKeysAttempt 记录 FetchKeys 请求的结果。
// 成功时更新 lastKeyFetchTimeStamp 时间戳。
func RecordFetchKeysAttempt(err error) {
	totalKeyFetch.WithLabelValues(getErrorCode(err)).Inc()
	if err == nil {
		lastKeyFetchTimeStamp.WithLabelValues().SetToCurrentTime()
	}
}

// RecordTokenGenAttempt 记录 Sign 请求的结果。
func RecordTokenGenAttempt(err error) {
	tokenGenReqTotal.WithLabelValues(getErrorCode(err)).Inc()
}

// RecordKeyDataTimeStamp 记录外部签名器返回的密钥数据时间戳。
func RecordKeyDataTimeStamp(timestamp float64) {
	dataTimeStamp.WithLabelValues().Set(timestamp)
}

// gRPCError 是实现了 GRPCStatus() 方法的错误接口，用于提取 gRPC 错误码。
type gRPCError interface {
	GRPCStatus() *status.Status
}

// getErrorCode 从错误中提取错误码字符串。
// 如果是 gRPC 错误，返回 gRPC status code；否则返回 "unknown-non-grpc"。
// 用于指标的 label 分类。
func getErrorCode(err error) string {
	if err == nil {
		return codes.OK.String()
	}

	// 同时处理 fmt.Errorf 等包装后的错误。
	var s gRPCError
	if errors.As(err, &s) {
		return s.GRPCStatus().Code().String()
	}

	// 非 gRPC 错误说明在调用 gRPC 方法之前就失败了（如连接建立失败）。
	return "unknown-non-grpc"
}

// OuboundRequestMetricsInterceptor 是 gRPC 客户端拦截器，记录所有出站请求的延迟。
// 作为 WithChainUnaryInterceptor 使用，自动记录每个 gRPC 调用的耗时和结果。
func OuboundRequestMetricsInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	start := time.Now()
	err := invoker(ctx, method, req, reply, cc, opts...)
	requestDurationSeconds.WithLabelValues(method, getErrorCode(err)).Observe(time.Since(start).Seconds())
	return err
}
