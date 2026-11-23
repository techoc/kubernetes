/*
Copyright 2017 The Kubernetes Authors.

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
	"errors"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	internalapi "k8s.io/cri-api/pkg/apis"
	kubeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	apitest "k8s.io/cri-api/pkg/apis/testing"
	fakeremote "k8s.io/cri-client/pkg/fake"
	"k8s.io/cri-client/pkg/util"
)

// defaultConnectionTimeout 是测试中构造远程运行时客户端时使用的连接超时。
const defaultConnectionTimeout = 15 * time.Second

// createAndStartFakeRemoteRuntime creates and starts fakeremote.RemoteRuntime.
// It returns the RemoteRuntime, endpoint on success.
// Users should call fakeRuntime.Stop() to cleanup the server.
//
// createAndStartFakeRemoteRuntime 创建并启动一个假的 CRI 运行时服务端。
// 返回假运行时实例与它的监听端点；调用方负责调用 fakeRuntime.Stop() 清理服务。
func createAndStartFakeRemoteRuntime(t *testing.T) (*fakeremote.RemoteRuntime, string) {
	// 生成一个空闲的 socket 端点。
	endpoint, err := fakeremote.GenerateEndpoint()
	require.NoError(t, err)

	// 在该端点上启动假运行时。
	fakeRuntime := fakeremote.NewFakeRemoteRuntime()
	fakeRuntime.Start(endpoint)

	return fakeRuntime, endpoint
}

// TestGetSpans 验证通过 WithTracerProvider 注入的 TracerProvider 能真正产生 OpenTelemetry span。
// 调用 Version RPC 后强制刷新 exporter，断言内存中收集到了非空 span。
func TestGetSpans(t *testing.T) {
	fakeRuntime, endpoint := createAndStartFakeRemoteRuntime(t)
	defer func() {
		fakeRuntime.Stop()
		// clear endpoint file
		if addr, _, err := util.GetAddressAndDialer(endpoint); err == nil {
			if _, err := os.Stat(addr); err == nil {
				os.Remove(addr)
			}
		}
	}()
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
	)
	ctx := context.Background()
	rtSvc, err := NewRemoteRuntimeServiceBuilder().
		WithEndpoint(endpoint).
		WithConnectionTimeout(defaultConnectionTimeout).
		WithTracerProvider(tp).
		Build(ctx)
	require.NoError(t, err)
	_, err = rtSvc.Version(ctx, apitest.FakeVersion)
	require.NoError(t, err)
	err = tp.ForceFlush(ctx)
	require.NoError(t, err)
	assert.NotEmpty(t, exp.GetSpans())
}

// TestVersion 验证 Version RPC 能正确返回假运行时上报的版本与运行时名称。
func TestVersion(t *testing.T) {
	fakeRuntime, endpoint := createAndStartFakeRemoteRuntime(t)
	defer func() {
		fakeRuntime.Stop()
		// clear endpoint file
		if addr, _, err := util.GetAddressAndDialer(endpoint); err == nil {
			if _, err := os.Stat(addr); err == nil {
				os.Remove(addr)
			}
		}
	}()

	ctx := context.Background()
	rtSvc, err := NewRemoteRuntimeServiceBuilder().
		WithEndpoint(endpoint).
		WithConnectionTimeout(defaultConnectionTimeout).
		Build(ctx)
	require.NoError(t, err)
	version, err := rtSvc.Version(ctx, apitest.FakeVersion)
	require.NoError(t, err)
	assert.Equal(t, apitest.FakeVersion, version.Version)
	assert.Equal(t, apitest.FakeRuntimeName, version.RuntimeName)
}

// createTestRemoteRuntimeService 一起创建假运行时服务端与连接到它的 RuntimeService 客户端，
// 并通过 t.Cleanup 注册完整清理（关闭客户端、停止服务端、删除 socket 文件）。
func createTestRemoteRuntimeService(t *testing.T) (*fakeremote.RemoteRuntime, internalapi.RuntimeService) {
	t.Helper()
	fakeRuntime, endpoint := createAndStartFakeRemoteRuntime(t)
	rtSvc, err := NewRemoteRuntimeServiceBuilder().
		WithEndpoint(endpoint).
		WithConnectionTimeout(defaultConnectionTimeout).
		Build(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, rtSvc.Close(context.Background()))
		fakeRuntime.Stop()
		if addr, _, err := util.GetAddressAndDialer(endpoint); err == nil {
			if _, err := os.Stat(addr); err == nil {
				require.NoError(t, os.Remove(addr))
			}
		}
	})
	return fakeRuntime, rtSvc
}

// TestCheckpointPod 验证 CheckpointPod RPC：
//  1. 请求内容（含 sandbox、容器列表、选项）被原样传递给运行时；
//  2. context 的截止时间被正确记录；
//  3. 运行时返回错误与 nil 请求时分别得到相应错误。
func TestCheckpointPod(t *testing.T) {
	fakeRuntime, rtSvc := createTestRemoteRuntimeService(t)
	request := &kubeapi.CheckpointPodRequest{
		PodSandboxId: "sandbox-id",
		OutputPath:   "/var/lib/kubelet/pod-checkpoints/checkpoint",
		ContainerIds: []string{"container-one", "container-two"},
		Options:      map[string]string{"example.runtime/checkpoint-mode": "fast"},
	}
	deadline := time.Now().Add(time.Minute)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	require.NoError(t, rtSvc.CheckpointPod(ctx, request))
	require.Len(t, fakeRuntime.RuntimeService.CheckpointedPods, 1)
	assert.True(t, proto.Equal(request, fakeRuntime.RuntimeService.CheckpointedPods[0]))
	assert.WithinDuration(t, deadline, fakeRuntime.RuntimeService.CheckpointPodDeadline, time.Second)

	fakeRuntime.RuntimeService.InjectError("CheckpointPod", errors.New("checkpoint failed"))
	require.ErrorContains(t, rtSvc.CheckpointPod(context.Background(), request), "checkpoint failed")
	require.ErrorContains(t, rtSvc.CheckpointPod(context.Background(), nil), "requires non-nil")
}

// TestRestorePod 验证 RestorePod RPC：
//  1. 请求（检查点路径、沙箱配置、容器配置等）被原样传递；
//  2. 响应中返回恢复出的沙箱与容器信息；
//  3. context 截止时间被记录，且错误请求与 nil 请求返回相应错误。
func TestRestorePod(t *testing.T) {
	fakeRuntime, rtSvc := createTestRemoteRuntimeService(t)
	request := &kubeapi.RestorePodRequest{
		CheckpointPath: "/var/lib/kubelet/pod-checkpoints/checkpoint",
		Config: &kubeapi.PodSandboxConfig{
			Metadata: &kubeapi.PodSandboxMetadata{Name: "pod", Namespace: "default", Uid: "uid"},
		},
		RuntimeHandler: "checkpoint-handler",
		Options:        map[string]string{"example.runtime/restore-mode": "lazy"},
		ContainerConfigs: []*kubeapi.ContainerConfig{
			{Metadata: &kubeapi.ContainerMetadata{Name: "app"}},
		},
	}
	deadline := time.Now().Add(time.Minute)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	response, err := rtSvc.RestorePod(ctx, request)
	require.NoError(t, err)
	assert.Equal(t, "fake-restored-pod-id", response.GetPodSandboxId())
	require.Len(t, response.GetRestoredContainers(), 1)
	assert.Equal(t, "app", response.GetRestoredContainers()[0].GetName())
	assert.NotEmpty(t, response.GetRestoredContainers()[0].GetContainerId())
	require.Len(t, fakeRuntime.RuntimeService.RestoredPods, 1)
	assert.True(t, proto.Equal(request, fakeRuntime.RuntimeService.RestoredPods[0]))
	assert.WithinDuration(t, deadline, fakeRuntime.RuntimeService.RestorePodDeadline, time.Second)

	fakeRuntime.RuntimeService.InjectError("RestorePod", errors.New("restore failed"))
	_, err = rtSvc.RestorePod(context.Background(), request)
	require.ErrorContains(t, err, "restore failed")
	_, err = rtSvc.RestorePod(context.Background(), nil)
	require.ErrorContains(t, err, "requires non-nil")
}

// TestTracerProviderPropagation verifies that the otelgrpc stats handler
// (which provides gRPC client context propagation) is installed when
// WithTracerProvider is not called or is called with a non-nil provider, and
// is skipped when WithTracerProvider(nil) is called explicitly.
func TestTracerProviderPropagation(t *testing.T) {
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exp))
	tracer := tp.Tracer("test")

	// 表驱动：默认情况与显式注入非 nil provider 时应传播 traceparent；
	// 显式 WithTracerProvider(nil) 时应关闭传播。
	cases := []struct {
		name            string
		withTP          func(*RemoteRuntimeServiceBuilder) *RemoteRuntimeServiceBuilder
		wantTraceParent bool
	}{
		{
			name:            "default propagates",
			withTP:          func(b *RemoteRuntimeServiceBuilder) *RemoteRuntimeServiceBuilder { return b },
			wantTraceParent: true,
		},
		{
			name: "noop tracer provider propagates",
			withTP: func(b *RemoteRuntimeServiceBuilder) *RemoteRuntimeServiceBuilder {
				return b.WithTracerProvider(noop.NewTracerProvider())
			},
			wantTraceParent: true,
		},
		{
			name:            "nil tracer provider disables propagation",
			withTP:          func(b *RemoteRuntimeServiceBuilder) *RemoteRuntimeServiceBuilder { return b.WithTracerProvider(nil) },
			wantTraceParent: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// 启动一个带拦截器（用于观察 traceparent 头）的假运行时。
			endpoint, sawTraceParent, stop := startCapturingFakeRuntime(t)
			defer stop()

			ctx := context.Background()
			b := NewRemoteRuntimeServiceBuilder().
				WithEndpoint(endpoint).
				WithConnectionTimeout(defaultConnectionTimeout)
			rtSvc, err := tc.withTP(b).Build(ctx)
			require.NoError(t, err)

			// 在外部创建 span，再发起 RPC；若传播开启，服务端应看到 traceparent 头。
			rpcCtx, span := tracer.Start(ctx, "outer")
			_, err = rtSvc.Version(rpcCtx, apitest.FakeVersion)
			require.NoError(t, err)
			span.End()

			// 断言是否观察到 traceparent 头，与用例期望一致。
			assert.Equal(t, tc.wantTraceParent, sawTraceParent.Load(), "traceparent header presence")
		})
	}
}

// startCapturingFakeRuntime starts a fake CRI runtime on a fresh endpoint and
// returns a flag that the unary server interceptor flips to true the first
// time it sees a "traceparent" header (the W3C tracecontext header the
// otelgrpc client propagator injects) in incoming gRPC metadata.
func startCapturingFakeRuntime(t *testing.T) (string, *atomic.Bool, func()) {
	endpoint, err := fakeremote.GenerateEndpoint()
	require.NoError(t, err)

	// 用原子布尔记录是否收到过 traceparent 头。
	var sawTraceParent atomic.Bool
	// 一元拦截器：检查传入 gRPC metadata 中是否带有 traceparent（W3C tracecontext 头）。
	interceptor := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if md, ok := metadata.FromIncomingContext(ctx); ok && len(md.Get("traceparent")) > 0 {
			sawTraceParent.Store(true)
		}
		return handler(ctx, req)
	}

	server := grpc.NewServer(grpc.UnaryInterceptor(interceptor))
	fake := fakeremote.NewFakeRemoteRuntime()
	kubeapi.RegisterRuntimeServiceServer(server, fake)
	kubeapi.RegisterImageServiceServer(server, fake)
	fake.RuntimeService.FakeStatus = &kubeapi.RuntimeStatus{
		Conditions: []*kubeapi.RuntimeCondition{
			{Type: kubeapi.RuntimeReady, Status: true},
			{Type: kubeapi.NetworkReady, Status: true},
		},
	}

	lis, err := util.CreateListener(endpoint)
	require.NoError(t, err)
	go func() { _ = server.Serve(lis) }()

	stop := func() {
		server.Stop()
		if addr, _, err := util.GetAddressAndDialer(endpoint); err == nil {
			if _, err := os.Stat(addr); err == nil {
				if err := os.Remove(addr); err != nil {
					t.Errorf("remove %q: %v", addr, err)
				}
			}
		}
	}
	return endpoint, &sawTraceParent, stop
}

// TestBuildValidatesRequiredOptions 验证 Builder.Build 会对必填参数做校验：
// 缺少 endpoint 报 "endpoint is required"；缺少正的超时报 "connectionTimeout must be positive"。
func TestBuildValidatesRequiredOptions(t *testing.T) {
	ctx := context.Background()
	_, err := NewRemoteRuntimeServiceBuilder().
		WithConnectionTimeout(defaultConnectionTimeout).
		Build(ctx)
	require.ErrorContains(t, err, "endpoint is required")

	_, err = NewRemoteRuntimeServiceBuilder().
		WithEndpoint("unix:///tmp/cri-client-test.sock").
		Build(ctx)
	assert.ErrorContains(t, err, "connectionTimeout must be positive")
}

// TestNewRemoteRuntimeServiceUnixSocketEndpoint 是 unix socket 端点的回归测试：
// 验证用 unix:// 形式的端点构造的客户端能正常连接并完成 Version RPC。
// Windows 平台不支持该场景，直接跳过。
func TestNewRemoteRuntimeServiceUnixSocketEndpoint(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("unix socket regression test is not applicable on windows")
	}

	fakeRuntime, endpoint := createAndStartFakeRemoteRuntime(t)
	defer func() {
		fakeRuntime.Stop()
		// clear endpoint file
		if addr, _, err := util.GetAddressAndDialer(endpoint); err == nil {
			if _, err := os.Stat(addr); err == nil {
				if err := os.Remove(addr); err != nil {
					t.Errorf("remove %q: %v", addr, err)
				}
			}
		}
	}()

	ctx := context.Background()
	rtSvc, err := NewRemoteRuntimeServiceBuilder().
		WithEndpoint(endpoint).
		WithConnectionTimeout(defaultConnectionTimeout).
		Build(ctx)
	require.NoError(t, err)
	version, err := rtSvc.Version(ctx, apitest.FakeVersion)
	require.NoError(t, err)
	assert.Equal(t, apitest.FakeVersion, version.Version)
}
