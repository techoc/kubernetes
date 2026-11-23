/*
Copyright 2022 The Kubernetes Authors.

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

package cri

import (
	"context"
	"os"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/cri-client/pkg/util"
)

// TestImageServiceSpansWithTP 验证显式注入 tracer provider 时，
// 镜像服务的 gRPC 调用会产生可被采集的 OpenTelemetry span。
func TestImageServiceSpansWithTP(t *testing.T) {
	// 启动假 CRI 运行时，后续通过真实 gRPC 连接访问它。
	fakeRuntime, endpoint := createAndStartFakeRemoteRuntime(t)
	defer func() {
		fakeRuntime.Stop()
		// clear endpoint file
		// 清理测试产生的 unix socket 文件：解析端点得到路径，存在则删除。
		if addr, _, err := util.GetAddressAndDialer(endpoint); err == nil {
			if _, err := os.Stat(addr); err == nil {
				os.Remove(addr)
			}
		}
	}()
	// 内存 exporter 直接收集 span，便于断言是否真的产生了追踪数据。
	exp := tracetest.NewInMemoryExporter()
	// 构造带 batcher 的 tracer provider，并在创建客户端时显式传入。
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
	)
	ctx := context.Background()
	// 配置端点、连接超时和 tracer provider 后构建客户端。
	imgSvc, err := NewRemoteImageServiceBuilder().
		WithEndpoint(endpoint).
		WithConnectionTimeout(defaultConnectionTimeout).
		WithTracerProvider(tp).
		Build(ctx)
	require.NoError(t, err)
	// 触发一次真实的 PullImage RPC；假运行时把镜像名原样作为 ImageRef 返回。
	imgRef, err := imgSvc.PullImage(ctx, &runtimeapi.ImageSpec{Image: "busybox"}, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, "busybox", imgRef)
	require.NoError(t, err)
	// batcher 会缓存 span，需强制冲刷后才能从 exporter 读取。
	err = tp.ForceFlush(ctx)
	require.NoError(t, err)
	// 断言：显式提供 tracer provider 时，RPC 产生了 span。
	assert.NotEmpty(t, exp.GetSpans())
}

// TestImageServiceSpansWithoutTP 验证未注入 tracer provider 时，
// 客户端退化为 noop provider，不会向外部 exporter 导出任何 span。
func TestImageServiceSpansWithoutTP(t *testing.T) {
	// 启动假 CRI 运行时并注册清理逻辑。
	fakeRuntime, endpoint := createAndStartFakeRemoteRuntime(t)
	defer func() {
		fakeRuntime.Stop()
		// clear endpoint file
		// 清理测试产生的 unix socket 文件。
		if addr, _, err := util.GetAddressAndDialer(endpoint); err == nil {
			if _, err := os.Stat(addr); err == nil {
				os.Remove(addr)
			}
		}
	}()
	// 同样准备内存 exporter，但该 provider 不会传给客户端。
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
	)
	ctx := context.Background()
	// 不调用 WithTracerProvider：内部应安装 noop provider。
	imgSvc, err := NewRemoteImageServiceBuilder().
		WithEndpoint(endpoint).
		WithConnectionTimeout(defaultConnectionTimeout).
		Build(ctx)
	require.NoError(t, err)
	// 发起同样的 PullImage 调用，确认功能本身不受影响。
	imgRef, err := imgSvc.PullImage(ctx, &runtimeapi.ImageSpec{Image: "busybox"}, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, "busybox", imgRef)
	require.NoError(t, err)
	// 冲刷自建的 provider（它并未挂到客户端上）。
	err = tp.ForceFlush(ctx)
	require.NoError(t, err)
	// 断言：没有 tracer provider 时，exporter 中没有任何 span。
	assert.Empty(t, exp.GetSpans())
}

// TestImageServiceBuildValidatesRequiredOptions 验证 Build 对必填配置的校验：
// 缺少 endpoint 或连接超时非正数时都应立即返回明确错误。
func TestImageServiceBuildValidatesRequiredOptions(t *testing.T) {
	ctx := context.Background()
	// 场景一：未设置 endpoint，即使超时有效也必须失败。
	_, err := NewRemoteImageServiceBuilder().
		WithConnectionTimeout(defaultConnectionTimeout).
		Build(ctx)
	require.ErrorContains(t, err, "endpoint is required")

	// 场景二：设置了 endpoint 但未设置连接超时（默认零值），同样失败。
	_, err = NewRemoteImageServiceBuilder().
		WithEndpoint("unix:///tmp/cri-client-test.sock").
		Build(ctx)
	require.ErrorContains(t, err, "connectionTimeout must be positive")
}

// TestNewRemoteImageServiceUnixSocketEndpoint 验证使用 unix socket 端点
// 创建客户端后可以正常调用 ImageFsInfo，是针对该端点形式的回归测试。
func TestNewRemoteImageServiceUnixSocketEndpoint(t *testing.T) {
	// unix socket 在 Windows 上不可用，直接跳过。
	if runtime.GOOS == "windows" {
		t.Skip("unix socket regression test is not applicable on windows")
	}

	fakeRuntime, endpoint := createAndStartFakeRemoteRuntime(t)
	defer func() {
		fakeRuntime.Stop()
		// clear endpoint file
		// 清理 socket 文件；删除失败时用 t.Errorf 暴露，避免残留影响后续测试。
		if addr, _, err := util.GetAddressAndDialer(endpoint); err == nil {
			if _, err := os.Stat(addr); err == nil {
				if err := os.Remove(addr); err != nil {
					t.Errorf("remove %q: %v", addr, err)
				}
			}
		}
	}()

	ctx := context.Background()
	// 用默认配置（不设置 tracer provider）创建客户端，验证建连与 v1 校验均成功。
	imgSvc, err := NewRemoteImageServiceBuilder().
		WithEndpoint(endpoint).
		WithConnectionTimeout(defaultConnectionTimeout).
		Build(ctx)
	require.NoError(t, err)
	// 调用 ImageFsInfo 确认服务可用，并断言返回非空响应。
	info, err := imgSvc.ImageFsInfo(ctx)
	require.NoError(t, err)
	assert.NotNil(t, info)
}
