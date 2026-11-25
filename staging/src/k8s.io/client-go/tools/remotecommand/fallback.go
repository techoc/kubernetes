/*
Copyright 2023 The Kubernetes Authors.

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

package remotecommand

import (
	"context"

	"k8s.io/klog/v2"
)

// 编译期断言：FallbackExecutor 满足 Executor 接口。
var _ Executor = &FallbackExecutor{}

// FallbackExecutor 按“先主后备”的顺序尝试两个执行器。
//
// 典型用法是主执行器走 WebSocket、备用执行器走旧版 SPDY，
// 以兼容尚未支持 WebSocket upgrade 的服务器或中间代理。
type FallbackExecutor struct {
	primary        Executor
	secondary      Executor
	shouldFallback func(error) bool
}

// NewFallbackExecutor 创建一个 Executor：先尝试使用 WebSocketExecutor，
// 若最初的 websocket "StreamWithContext" 调用失败，则回退到旧版 SPDYExecutor。
// func NewFallbackExecutor(config *restclient.Config, method string, url *url.URL) (Executor, error) {
func NewFallbackExecutor(primary, secondary Executor, shouldFallback func(error) bool) (Executor, error) {
	return &FallbackExecutor{
		primary:        primary,
		secondary:      secondary,
		shouldFallback: shouldFallback,
	}, nil
}

// Deprecated: 请改用 "StreamWithContext"。
func (f *FallbackExecutor) Stream(options StreamOptions) error {
	return f.StreamWithContext(context.Background(), options)
}

// StreamWithContext 先尝试用主执行器调用 "StreamWithContext"；
// 若主执行器升级到 websocket 连接的最初调用失败，则回退为调用备用执行器。
func (f *FallbackExecutor) StreamWithContext(ctx context.Context, options StreamOptions) error {
	err := f.primary.StreamWithContext(ctx, options)
	// 只有 shouldFallback 判定为可回退的错误（通常是握手失败）才切换；
	// 命令执行本身的错误不应重试，否则可能造成副作用重复执行。
	if err != nil && f.shouldFallback(err) {
		klog.FromContext(ctx).V(4).Info("RemoteCommand fallback", "err", err)
		return f.secondary.StreamWithContext(ctx, options)
	}
	return err
}
