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

package portforward

import (
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/klog/v2"
	streamhttp "k8s.io/streaming/pkg/httpstream"
)

var _ httpstream.Dialer = &FallbackDialer{}
var _ streamhttp.Dialer = &StreamingFallbackDialer{}

// FallbackDialer 封装主拨号器、备用拨号器以及判断主拨号器失败后是否回退的布尔函数，
// 实现 httpstream.Dialer 接口。
//
// 典型用途是优先使用 WebSocket 流式协议，失败时回退到 SPDY；
// shouldFallback 决定哪些错误值得重试，避免对明确的认证/配置错误做无谓尝试。
type FallbackDialer struct {
	primary        httpstream.Dialer
	secondary      httpstream.Dialer
	shouldFallback func(error) bool
}

// NewFallbackDialer 使用主拨号器、备用拨号器以及主拨号器失败判定函数创建 FallbackDialer。
func NewFallbackDialer(primary, secondary httpstream.Dialer, shouldFallback func(error) bool) httpstream.Dialer {
	return &FallbackDialer{
		primary:        primary,
		secondary:      secondary,
		shouldFallback: shouldFallback,
	}
}

// StreamingFallbackDialer 封装带回退行为的主、备用流式拨号器。
type StreamingFallbackDialer struct {
	primary        streamhttp.Dialer
	secondary      streamhttp.Dialer
	shouldFallback func(error) bool
}

// NewFallbackDialerForStreaming 为使用 k8s.io/streaming/pkg/httpstream 类型的
// 仓库内调用方创建带回退行为的拨号器。
func NewFallbackDialerForStreaming(primary, secondary streamhttp.Dialer, shouldFallback func(error) bool) streamhttp.Dialer {
	return &StreamingFallbackDialer{
		primary:        primary,
		secondary:      secondary,
		shouldFallback: shouldFallback,
	}
}

// Dial 实现 httpstream.Dialer 接口，请求给定协议并返回升级后的 httpstream.Connection
// 以及协商通过的协议版本。主拨号器失败且满足回退条件时，会尝试备用拨号器；出错则返回错误。
func (f *FallbackDialer) Dial(protocols ...string) (httpstream.Connection, string, error) {
	conn, version, err := f.primary.Dial(protocols...)
	if err != nil && f.shouldFallback(err) {
		//nolint:logcheck // This code is only used by kubectl where contextual logging is not that useful.
		klog.V(4).Infof("fallback to secondary dialer from primary dialer err: %v", err)
		return f.secondary.Dial(protocols...)
	}
	return conn, version, err
}

// Dial 实现 k8s.io/streaming/pkg/httpstream.Dialer 接口。
func (f *StreamingFallbackDialer) Dial(protocols ...string) (streamhttp.Connection, string, error) {
	conn, version, err := f.primary.Dial(protocols...)
	if err != nil && f.shouldFallback(err) {
		//nolint:logcheck // This code is only used by kubectl and E2E testing where contextual logging is not that useful.
		klog.V(4).Infof("fallback to secondary dialer from primary dialer err: %v", err)
		return f.secondary.Dial(protocols...)
	}
	return conn, version, err
}
