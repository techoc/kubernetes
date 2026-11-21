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

package plugin

import (
	"context"
	"errors"
	"io"
	"net"
	"path"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	drahealthv1 "k8s.io/kubelet/pkg/apis/dra-health/v1"
	drahealthv1alpha1 "k8s.io/kubelet/pkg/apis/dra-health/v1alpha1"
	drapbv1 "k8s.io/kubelet/pkg/apis/dra/v1"
	drapbv1beta1 "k8s.io/kubelet/pkg/apis/dra/v1beta1"
	"k8s.io/kubernetes/test/utils/ktesting"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

// fakeGRPCServerInterface 是 setupGRPCServerWithFake 所需的接口，聚合了三个 gRPC 服务端接口。
type fakeGRPCServerInterface interface {
	drapbv1.DRAPluginServer
	drahealthv1.DRAResourceHealthServer
	drapbv1.UnsafeDRAPluginServer
}

type fakeGRPCServer struct {
	drapbv1beta1.UnimplementedDRAPluginServer
	drahealthv1.UnimplementedDRAResourceHealthServer
	drapbv1.UnsafeDRAPluginServer
}

var _ drapbv1.DRAPluginServer = &fakeGRPCServer{}
var _ fakeGRPCServerInterface = &fakeGRPCServer{}

func (f *fakeGRPCServer) NodePrepareResources(ctx context.Context, in *drapbv1.NodePrepareResourcesRequest) (*drapbv1.NodePrepareResourcesResponse, error) {
	return &drapbv1.NodePrepareResourcesResponse{Claims: map[string]*drapbv1.NodePrepareResourceResponse{"claim-uid": {
		Devices: []*drapbv1.Device{
			{
				RequestNames: []string{"test-request"},
				CdiDeviceIds: []string{"test-cdi-id"},
			},
		},
	}}}, nil
}

func (f *fakeGRPCServer) NodeUnprepareResources(ctx context.Context, in *drapbv1.NodeUnprepareResourcesRequest) (*drapbv1.NodeUnprepareResourcesResponse, error) {

	return &drapbv1.NodeUnprepareResourcesResponse{}, nil
}

func (f *fakeGRPCServer) NodeWatchResources(in *drahealthv1.NodeWatchResourcesRequest, srv drahealthv1.DRAResourceHealth_NodeWatchResourcesServer) error {
	resp := &drahealthv1.NodeWatchResourcesResponse{
		Devices: []*drahealthv1.DeviceHealth{
			{
				Device: &drahealthv1.DeviceIdentifier{
					PoolName:   "pool1",
					DeviceName: "dev1",
				},
				Health: drahealthv1.HealthStatus_HEALTHY,
			},
		},
	}
	if err := srv.Send(resp); err != nil {
		return err
	}
	return nil
}

// tearDown 是一个幂等的清理函数。
type tearDown func()

// setupGRPCServerWithFake 在给定 unix 地址上用指定的 fake 服务端启动一个 gRPC 服务器，
// 按 service 注册对应的 DRA 服务，返回用于停止服务器的 tearDown。
func setupGRPCServerWithFake(ctx context.Context, service, addr string, fakeGRPCServer fakeGRPCServerInterface) (tearDown, error) {
	ctx, cancel := context.WithCancel(ctx)

	listener, err := net.Listen("unix", addr)
	if err != nil {
		cancel()
		return nil, err
	}

	s := grpc.NewServer()

	switch service {
	case drapbv1.DRAPluginService:
		drapbv1.RegisterDRAPluginServer(s, fakeGRPCServer)
	case drapbv1beta1.DRAPluginService:
		drapbv1beta1.RegisterDRAPluginServer(s, drapbv1beta1.V1ServerWrapper{DRAPluginServer: fakeGRPCServer})
	case drahealthv1.DRAResourceHealthService:
		drahealthv1.RegisterDRAResourceHealthServer(s, fakeGRPCServer)
	default:
		if service == "" {
			drapbv1.RegisterDRAPluginServer(s, fakeGRPCServer)
			drapbv1beta1.RegisterDRAPluginServer(s, drapbv1beta1.V1ServerWrapper{DRAPluginServer: fakeGRPCServer})
			drahealthv1.RegisterDRAResourceHealthServer(s, fakeGRPCServer)
		} else {
			cancel()
			return nil, err
		}
	}

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		if err := s.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			panic(err)
		}
	}()

	go func() {
		<-ctx.Done()
		s.GracefulStop()
	}()

	teardown := func() {
		cancel()
		wg.Wait() // wait for Serve() to finish
	}

	return teardown, nil
}

// setupFakeGRPCServer 用默认的 fakeGRPCServer 启动一个 gRPC 测试服务器。
func setupFakeGRPCServer(ctx context.Context, service, addr string) (tearDown, error) {
	return setupGRPCServerWithFake(ctx, service, addr, &fakeGRPCServer{})
}

// TestGRPCConnIsReused 验证 DRA 插件的 gRPC 连接会被复用：
// 并发两次调用 NodePrepareResources 以及后续健康检查与资源操作，
// 都应使用同一个 ClientConn，不会新建连接。
func TestGRPCConnIsReused(t *testing.T) {
	tCtx := ktesting.Init(t)
	addr := path.Join(t.TempDir(), "dra.sock")
	teardown, err := setupFakeGRPCServer(tCtx, "", addr)
	require.NoError(t, err)
	defer teardown()

	reusedConns := make(map[*grpc.ClientConn]int)
	wg := sync.WaitGroup{}
	m := sync.Mutex{}

	driverName := "dummy-driver"

	// 确保所使用的插件已注册到管理器
	draPlugins := NewDRAPluginManager(tCtx, nil, nil, &mockStreamHandler{}, 0)
	tCtx.ExpectNoError(draPlugins.add(driverName, addr, drapbv1.DRAPluginService, drahealthv1.DRAResourceHealthService, defaultClientCallTimeout), "add plugin")
	plugin, err := draPlugins.GetPlugin(driverName)
	tCtx.ExpectNoError(err, "get plugin")
	conn := plugin.conn

	// 调用 NodePrepareResources 两次，检查是创建了新连接还是复用了同一个连接
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			plugin, err := draPlugins.GetPlugin(driverName)
			if err != nil {
				t.Error(err)
				return
			}

			req := &drapbv1.NodePrepareResourcesRequest{
				Claims: []*drapbv1.Claim{
					{
						Namespace: "dummy-namespace",
						Uid:       "dummy-uid",
						Name:      "dummy-claim",
					},
				},
			}

			_, err = plugin.NodePrepareResources(tCtx, req)
			assert.NoError(t, err)

			conn := plugin.conn

			m.Lock()
			defer m.Unlock()
			reusedConns[conn]++
		}()
	}

	wg.Wait()
	// 映射中只应有一个条目，否则说明创建了另一个 gRPC 连接
	require.Len(t, reusedConns, 1, "expected length to be 1 but got %d", len(reusedConns))
	require.Equal(t, 2, reusedConns[conn], "expected counter to be 2 but got %d", reusedConns[conn])

	tCtx.Run("health_api_reuses_connection", func(tCtx ktesting.TContext) {
		ctx, cancel := context.WithTimeout(tCtx, 5*time.Second)
		defer cancel()

		originalConn := plugin.conn

		stream, err := plugin.NodeWatchResources(ctx)
		require.NoError(tCtx, err, "Health stream should work")
		require.NotNil(tCtx, stream)

		require.Equal(tCtx, originalConn, plugin.conn, "Health API should reuse the same connection")

		resp, err := stream.Recv()
		require.NoError(tCtx, err, "Should receive health data")
		require.NotNil(tCtx, resp)
		require.Len(tCtx, resp.Devices, 1)
		assert.Equal(tCtx, "pool1", resp.Devices[0].GetDevice().GetPoolName())
		assert.Equal(tCtx, "dev1", resp.Devices[0].GetDevice().GetDeviceName())
		assert.Equal(tCtx, drahealthv1.HealthStatus_HEALTHY, resp.Devices[0].GetHealth())

		require.Equal(tCtx, originalConn, plugin.conn, "Connection should remain unchanged after health operations")

		prepareReq := &drapbv1.NodePrepareResourcesRequest{
			Claims: []*drapbv1.Claim{
				{
					Namespace: "dummy-namespace",
					Uid:       "dummy-uid",
					Name:      "dummy-claim",
				},
			},
		}

		prepareResp, err := plugin.NodePrepareResources(ctx, prepareReq)
		require.NoError(tCtx, err, "NodePrepareResources should work")
		require.NotNil(tCtx, prepareResp)
		require.NotNil(tCtx, prepareResp.Claims["claim-uid"])

		require.Equal(tCtx, originalConn, plugin.conn, "Connection should remain unchanged after NodePrepareResources")

		unprepareReq := &drapbv1.NodeUnprepareResourcesRequest{
			Claims: []*drapbv1.Claim{
				{
					Namespace: "dummy-namespace",
					Uid:       "dummy-uid",
					Name:      "dummy-claim",
				},
			},
		}

		unprepareResp, err := plugin.NodeUnprepareResources(ctx, unprepareReq)
		require.NoError(tCtx, err, "NodeUnprepareResources should work")
		require.NotNil(tCtx, unprepareResp)

		require.Equal(tCtx, originalConn, plugin.conn, "Connection should remain unchanged after all API calls")
	})
}

// TestGRPCConnUsableAfterIdle 验证连接空闲超时之后，
// NodePrepareResources 调用仍能成功（连接会被重新建立并保持可用）。
func TestGRPCConnUsableAfterIdle(t *testing.T) {
	tCtx := ktesting.Init(t)
	service := drapbv1.DRAPluginService
	addr := path.Join(t.TempDir(), "dra.sock")
	teardown, err := setupFakeGRPCServer(tCtx, service, addr)
	require.NoError(t, err)
	defer teardown()

	driverName := "dummy-driver"

	// 确保所使用的插件已注册到管理器
	draPlugins := NewDRAPluginManager(tCtx, nil, nil, &mockStreamHandler{}, 0)
	draPlugins.withIdleTimeout = 5 * time.Second
	tCtx.ExpectNoError(draPlugins.add(driverName, addr, service, "", defaultClientCallTimeout), "add plugin")
	plugin, err := draPlugins.GetPlugin(driverName)
	tCtx.ExpectNoError(err, "get plugin")

	// 连接并不会真正进入空闲，因为 HandleConn 会调用 Connect
	// 把它重新置为 ready。这里多睡一会儿，确保代码执行到空闲超时的逻辑。
	tCtx.Log("Waiting for idle timeout...")
	time.Sleep(2 * draPlugins.withIdleTimeout)

	req := &drapbv1.NodePrepareResourcesRequest{
		Claims: []*drapbv1.Claim{
			{
				Namespace: "dummy-namespace",
				Uid:       "dummy-uid",
				Name:      "dummy-claim",
			},
		},
	}

	callCtx := tCtx.WithTimeout(10*time.Second, "call timed out")
	_, err = plugin.NodePrepareResources(callCtx, req)
	tCtx.ExpectNoError(err, "NodePrepareResources")
}

// TestGetDRAPlugin 表驱动测试 GetPlugin 的查找行为，覆盖三种场景：
// driverName 为空、驱动不在列表中（均应报错），以及插件已注册（应成功取回）。
func TestGetDRAPlugin(t *testing.T) {
	for _, test := range []struct {
		description string
		setup       func(*DRAPluginManager) error
		driverName  string
		shouldError bool
	}{
		{
			description: "driver-name is empty",
			shouldError: true,
		},
		{
			description: "driver name not found in the list",
			driverName:  "driver-name-not-found-in-the-list",
			shouldError: true,
		},
		{
			description: "plugin exists",
			setup: func(draPlugins *DRAPluginManager) error {
				return draPlugins.add("dummy-driver", "/tmp/dra.sock", "", "", defaultClientCallTimeout)
			},
			driverName: "dummy-driver",
		},
	} {
		t.Run(test.description, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			draPlugins := NewDRAPluginManager(tCtx, nil, nil, &mockStreamHandler{}, 0)
			if test.setup != nil {
				require.NoError(t, test.setup(draPlugins), "setup plugin")
			}
			plugin, err := draPlugins.GetPlugin(test.driverName)
			if test.shouldError {
				assert.Nil(t, plugin)
				assert.Error(t, err)
			} else {
				assert.NotNil(t, plugin)
				assert.NoError(t, err)
			}
		})
	}
}

// TestGRPCMethods 表驱动测试插件服务版本协商：覆盖 v1beta1、v1 服务匹配，
// 以及插件与 kubelet 服务版本不匹配、选择不支持的服务这两种错误场景。
func TestGRPCMethods(t *testing.T) {
	for _, test := range []struct {
		description   string
		service       string
		chosenService string
		expectError   string
	}{
		{
			description:   "v1beta1",
			service:       drapbv1beta1.DRAPluginService,
			chosenService: drapbv1beta1.DRAPluginService,
		},
		{
			description:   "v1",
			service:       drapbv1.DRAPluginService,
			chosenService: drapbv1.DRAPluginService,
		},
		{
			// 实际中，插件与 kubelet 之间出现这种版本不匹配不应发生。
			description:   "mismatch",
			service:       drapbv1.DRAPluginService,
			chosenService: drapbv1beta1.DRAPluginService,
			expectError:   "unknown service k8s.io.kubelet.pkg.apis.dra.v1beta1.DRAPlugin",
		},
		{
			// 实际中，kubelet 不会选择无效的服务。
			description:   "internal-error",
			service:       drapbv1.DRAPluginService,
			chosenService: "some-other-service",
			expectError:   "unsupported chosen service",
		},
	} {
		t.Run(test.description, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			addr := path.Join(t.TempDir(), "dra.sock")
			teardown, err := setupFakeGRPCServer(tCtx, test.service, addr)
			if err != nil {
				t.Fatal(err)
			}
			defer teardown()

			driverName := "dummy-driver"
			draPlugins := NewDRAPluginManager(tCtx, nil, nil, &mockStreamHandler{}, 0)
			tCtx.ExpectNoError(draPlugins.add(driverName, addr, test.chosenService, "", defaultClientCallTimeout))

			plugin, err := draPlugins.GetPlugin(driverName)
			if err != nil {
				t.Fatal(err)
			}

			_, err = plugin.NodePrepareResources(tCtx, &drapbv1.NodePrepareResourcesRequest{})
			assertError(t, test.expectError, err)

			_, err = plugin.NodeUnprepareResources(tCtx, &drapbv1.NodeUnprepareResourcesRequest{})
			assertError(t, test.expectError, err)
		})
	}
}

// TestGRPCWithTimeoutEnforced 验证客户端调用超时会被强制执行：
// 服务端阻塞 NodePrepareResources，客户端应在超时后收到 DeadlineExceeded 错误，
// 服务端随后被解阻塞并确认方法确实被调用。
func TestGRPCWithTimeoutEnforced(t *testing.T) {
	tCtx := ktesting.Init(t)

	service := drapbv1.DRAPluginService
	addr := path.Join(t.TempDir(), "dra.sock")

	// 强制 NodePrepareResources 阻塞，使超时时间耗尽，
	// 客户端因此应中止请求。客户端一旦中止，再解除 NodePrepareResources 的阻塞。
	blocked := make(chan struct{})
	server := &timeoutFakeGRPCServer{
		t:              t,
		fakeGRPCServer: &fakeGRPCServer{},
		blocked:        blocked,
		done:           make(chan struct{}),
	}
	teardown, err := setupGRPCServerWithFake(tCtx, service, addr, server)
	require.NoError(t, err, "failed to setup grpc server")
	defer teardown()

	driverName := "dummy-driver"
	timeout := time.Second
	manager := NewDRAPluginManager(tCtx, nil, nil, nil, 0)
	err = manager.add(driverName, addr, service, "", timeout)
	require.NoError(t, err, "unexpected error while adding the plugin")

	plugin, err := manager.GetPlugin(driverName)
	require.NoError(t, err, "unexpected error while retrieving the plugin")

	// 在新协程中调用该方法，以防超时未生效时调用无限阻塞
	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)
		req := &drapbv1.NodePrepareResourcesRequest{
			Claims: []*drapbv1.Claim{
				{
					Namespace: "dummy-namespace",
					Uid:       "dummy-uid",
					Name:      "dummy-claim",
				},
			},
		}
		_, err := plugin.NodePrepareResources(tCtx, req)
		errCh <- err
	}()

	// 等待 gRPC 调用方超时返回
	select {
	// 不使用 wait.ForeverTestTimeout，最多等待 3*timeout，以容忍 CI 中的抖动
	case <-time.After(3 * timeout):
		t.Errorf("expected the grpc caller to return after the timeout had elapsed")
	case err = <-errCh:
		// gRPC 调用已返回
	}

	// 解除服务端请求处理的阻塞并等待其返回，确保服务端方法确实被调用。
	// 若超时未生效，这里也不会遗留任何协程。
	close(blocked)
	select {
	case <-server.done:
	// 不使用 wait.ForeverTestTimeout，最多等待 3*timeout，以容忍 CI 中的抖动
	case <-time.After(3 * timeout):
		t.Errorf("expected the grpc method to have been invoked")
	}

	require.Error(t, err, "expected the grpc method to return an error")
	status, ok := grpcstatus.FromError(err)
	// 如果不是 gRPC 错误，说明操作可能在 gRPC 方法被调用之前就失败了，
	// 否则我们本应得到 gRPC 错误。
	require.True(t, ok, "expected an error of type: %T, but got: %T", &grpcstatus.Status{}, err)

	// DeadlineExceeded 表示操作在完成前超时。当截止时间耗尽时，
	// gRPC 框架会生成该错误码。
	// 详见：https://github.com/grpc/grpc/blob/master/doc/statuscodes.md
	assert.Equal(t, grpccodes.DeadlineExceeded, status.Code(), "expected status code to match")
}

// timeoutFakeGRPCServer 内嵌一个 fakeGRPCServer，并覆盖 NodePrepareResources 方法
// 来模拟超时场景。
// NodePrepareResources 会一直阻塞，直到测试显式关闭 blocked 通道将其解除；
// 完成时关闭 done 通道，便于测试等待其结束。
type timeoutFakeGRPCServer struct {
	*fakeGRPCServer

	t       *testing.T
	blocked <-chan struct{}
	done    chan struct{}
}

func (f *timeoutFakeGRPCServer) NodePrepareResources(ctx context.Context, in *drapbv1.NodePrepareResourcesRequest) (*drapbv1.NodePrepareResourcesResponse, error) {
	defer close(f.done)
	now := time.Now()
	deadline, ok := ctx.Deadline()
	f.t.Logf("request context has deadline: %t, after: %s", ok, deadline.Sub(now))
	if !ok {
		f.t.Errorf("expected the request context to have a deadline")
	}
	f.t.Logf("NodePrepareResources: blocking so the client times out before the server sends a reply")

	<-f.blocked

	f.t.Logf("NodePrepareResources: blocked for: %s", time.Since(now))
	return &drapbv1.NodePrepareResourcesResponse{}, nil
}

// assertError 校验错误与预期是否一致：预期无错时 err 必须为 nil，
// 预期报错时错误信息需包含期望的片段。
func assertError(t *testing.T, expectError string, err error) {
	t.Helper()
	switch {
	case err != nil && expectError == "":
		t.Errorf("Expected no error, got: %v", err)
	case err == nil && expectError != "":
		t.Errorf("Expected error %q, got none", expectError)
	case err != nil && !strings.Contains(err.Error(), expectError):
		t.Errorf("Expected error %q, got: %v", expectError, err)
	}
}

// TestPlugin_WatchResources 验证健康监控流：首个 Recv 应收到 fake 服务端发送的
// 设备健康消息，服务端返回 nil 关闭流后，第二次 Recv 应得到 io.EOF。
func TestPlugin_WatchResources(t *testing.T) {
	tCtx := ktesting.Init(t)
	ctx, cancel := context.WithCancel(tCtx)
	defer cancel()

	driverName := "test-driver"
	addr := path.Join(t.TempDir(), "dra.sock")

	teardown, err := setupFakeGRPCServer(tCtx, "", addr)
	require.NoError(t, err)
	defer teardown()

	draPlugins := NewDRAPluginManager(tCtx, nil, nil, &mockStreamHandler{}, 0)
	err = draPlugins.add(driverName, addr, drapbv1beta1.DRAPluginService, drahealthv1.DRAResourceHealthService, 5*time.Second)
	require.NoError(t, err)
	defer draPlugins.remove(driverName, addr)

	p, err := draPlugins.GetPlugin(driverName)
	require.NoError(t, err)

	stream, err := p.NodeWatchResources(ctx)
	require.NoError(t, err)
	require.NotNil(t, stream)

	// 1. 接收 fake 服务端发送的第一条消息。
	resp, err := stream.Recv()
	require.NoError(t, err, "The first Recv() should succeed with the message from the server")
	require.NotNil(t, resp)
	require.Len(t, resp.Devices, 1)
	assert.Equal(t, "pool1", resp.Devices[0].GetDevice().GetPoolName())
	assert.Equal(t, drahealthv1.HealthStatus_HEALTHY, resp.Devices[0].GetHealth())

	// 2. 第二次接收应因服务端返回 nil 而关闭流、返回 io.EOF，
	//    以此确认流被干净地结束。
	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF, "The second Recv() should return an io.EOF error to signal a clean stream closure")
}

// fakeV1Alpha1HealthServer 只实现 v1alpha1 版本的 DRAResourceHealth 服务，
// 模拟在 v1 API 出现之前发布的驱动。
type fakeV1Alpha1HealthServer struct {
	drahealthv1alpha1.UnimplementedDRAResourceHealthServer
}

func (f *fakeV1Alpha1HealthServer) NodeWatchResources(in *drahealthv1alpha1.NodeWatchResourcesRequest, srv drahealthv1alpha1.DRAResourceHealth_NodeWatchResourcesServer) error {
	resp := &drahealthv1alpha1.NodeWatchResourcesResponse{
		Devices: []*drahealthv1alpha1.DeviceHealth{
			{
				Device: &drahealthv1alpha1.DeviceIdentifier{
					PoolName:   "pool1",
					DeviceName: "dev1",
				},
				Health:  drahealthv1alpha1.HealthStatus_UNHEALTHY,
				Message: "degraded",
			},
		},
	}
	return srv.Send(resp)
}

// TestPlugin_WatchResources_V1Alpha1 验证向后兼容路径：
// 当插件只提供 v1alpha1 健康服务时，kubelet 协商出 v1alpha1，
// 并把流透明地转换为 v1。
//
// TODO(harche): 在 1.40 中连同 kubelet 的 v1alpha1 客户端支持一起移除。
func TestPlugin_WatchResources_V1Alpha1(t *testing.T) {
	tCtx := ktesting.Init(t)
	ctx, cancel := context.WithCancel(tCtx)
	defer cancel()

	driverName := "test-driver"
	addr := path.Join(t.TempDir(), "dra.sock")

	listener, err := net.Listen("unix", addr)
	require.NoError(t, err)
	s := grpc.NewServer()
	drahealthv1alpha1.RegisterDRAResourceHealthServer(s, &fakeV1Alpha1HealthServer{})
	go func() {
		if err := s.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			panic(err)
		}
	}()
	defer s.Stop()

	draPlugins := NewDRAPluginManager(tCtx, nil, nil, &mockStreamHandler{}, 0)
	err = draPlugins.add(driverName, addr, "", drahealthv1alpha1.DRAResourceHealthService, 5*time.Second)
	require.NoError(t, err)
	defer draPlugins.remove(driverName, addr)

	p, err := draPlugins.GetPlugin(driverName)
	require.NoError(t, err)

	stream, err := p.NodeWatchResources(ctx)
	require.NoError(t, err)
	require.NotNil(t, stream)

	// 服务端发来的 v1alpha1 消息必须以转换后的 v1 消息形式到达。
	resp, err := stream.Recv()
	require.NoError(t, err, "The first Recv() should succeed with the converted message from the v1alpha1 server")
	require.NotNil(t, resp)
	require.Len(t, resp.Devices, 1)
	assert.Equal(t, "pool1", resp.Devices[0].GetDevice().GetPoolName())
	assert.Equal(t, "dev1", resp.Devices[0].GetDevice().GetDeviceName())
	assert.Equal(t, drahealthv1.HealthStatus_UNHEALTHY, resp.Devices[0].GetHealth())
	assert.Equal(t, "degraded", resp.Devices[0].GetMessage())

	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF, "The second Recv() should return io.EOF after the server closed the stream")
}
