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

package dra

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	schedulingapi "k8s.io/api/scheduling/v1beta1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/kubernetes/fake"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	metricstestutil "k8s.io/component-base/metrics/testutil"
	"k8s.io/dynamic-resource-allocation/resourceclaim"
	"k8s.io/klog/v2"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	kubeletmetrics "k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/utils/ptr"

	drahealthv1 "k8s.io/kubelet/pkg/apis/dra-health/v1"
	drapb "k8s.io/kubelet/pkg/apis/dra/v1"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/cm/dra/state"
	"k8s.io/kubernetes/pkg/kubelet/cm/resourceupdates"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// 测试中使用的固定名称：驱动类、Pod、容器与 PodGroup
const (
	driverClassName = "test"
	podName         = "test-pod"
	containerName   = "test-container"
	podGroupName    = "test-podgroup"
)

// shareID/shareUID 是测试中使用的 ShareID 及其 UID；testPodCounter 用于生成唯一的 Pod 名与 UID
var (
	shareID        = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	shareUID       = types.UID(shareID)
	testPodCounter atomic.Uint32
)

// fakeDRADriverGRPCServer 是实现了 DRA 插件与资源健康两个 gRPC 服务的伪造服务端，
// 用于统计各 RPC 调用次数并模拟超时、自定义响应等行为
type fakeDRADriverGRPCServer struct {
	drapb.UnimplementedDRAPluginServer
	drahealthv1.UnimplementedDRAResourceHealthServer
	driverName                 string
	timeout                    *time.Duration
	prepareResourceCalls       atomic.Uint32
	unprepareResourceCalls     atomic.Uint32
	watchResourcesCalls        atomic.Uint32
	prepareResourcesResponse   *drapb.NodePrepareResourcesResponse
	unprepareResourcesResponse *drapb.NodeUnprepareResourcesResponse
	watchResourcesResponses    chan *drahealthv1.NodeWatchResourcesResponse
	watchResourcesError        error
}

// gatherWithoutDurations 在采集指标时剔除 histogram 中与时间相关的字段，
// 使基于耗时直方图的断言不受实际执行时间影响
var gatherWithoutDurations = metricstestutil.GathererFunc(func() ([]*metricstestutil.MetricFamily, error) {
	got, err := kubeletmetrics.GetGather().Gather()
	for _, mf := range got {

		for _, m := range mf.Metric {
			if m.Histogram == nil {
				continue
			}
			// 去掉 histogram 中所有依赖时间的数据，便于结果比较
			m.Histogram.SampleSum = nil
			m.Histogram.Bucket = nil
		}
	}
	return got, err
})

// NodePrepareResources 实现伪造的节点资源准备 RPC：记录调用次数，
// 按需模拟超时，并在未指定响应时返回一个默认的 CDI 设备
func (s *fakeDRADriverGRPCServer) NodePrepareResources(ctx context.Context, req *drapb.NodePrepareResourcesRequest) (*drapb.NodePrepareResourcesResponse, error) {
	s.prepareResourceCalls.Add(1)

	if s.timeout != nil {
		time.Sleep(*s.timeout)
	}

	if s.prepareResourcesResponse == nil {
		cdiDeviceName := "claim-" + req.Claims[0].Uid
		cdiID := s.driverName + "/" + driverClassName + "=" + cdiDeviceName
		return &drapb.NodePrepareResourcesResponse{
			Claims: map[string]*drapb.NodePrepareResourceResponse{
				req.Claims[0].Uid: {
					Devices: []*drapb.Device{
						{
							PoolName:     poolName,
							DeviceName:   deviceName,
							RequestNames: []string{req.Claims[0].Name},
							CdiDeviceIds: []string{cdiID},
						},
					},
				},
			},
		}, nil
	}

	return s.prepareResourcesResponse, nil
}

// NodeUnprepareResources 实现伪造的节点资源释放 RPC，记录调用次数并可按需模拟超时
func (s *fakeDRADriverGRPCServer) NodeUnprepareResources(ctx context.Context, req *drapb.NodeUnprepareResourcesRequest) (*drapb.NodeUnprepareResourcesResponse, error) {
	s.unprepareResourceCalls.Add(1)

	if s.timeout != nil {
		time.Sleep(*s.timeout)
	}

	if s.unprepareResourcesResponse == nil {
		return &drapb.NodeUnprepareResourcesResponse{
			Claims: map[string]*drapb.NodeUnprepareResourceResponse{
				req.Claims[0].Uid: {},
			},
		}, nil
	}

	return s.unprepareResourcesResponse, nil
}

// NodeWatchResources 实现伪造的资源健康监听流：从响应通道读取消息并逐条发送，
// 直到上下文取消、通道关闭或发送失败
func (s *fakeDRADriverGRPCServer) NodeWatchResources(req *drahealthv1.NodeWatchResourcesRequest, stream drahealthv1.DRAResourceHealth_NodeWatchResourcesServer) error {
	s.watchResourcesCalls.Add(1)
	logger := klog.FromContext(stream.Context())
	logger.V(4).Info("Fake Server: WatchResources stream started")

	if s.watchResourcesError != nil {
		logger.Error(s.watchResourcesError, "Fake Server: Returning predefined stream error")
		return s.watchResourcesError
	}

	go func() {
		for {
			select {
			case <-stream.Context().Done():
				logger.Info("Fake Server: WatchResources stream context canceled")
				return
			case resp, ok := <-s.watchResourcesResponses:
				if !ok {
					logger.Info("Fake Server: WatchResources response channel closed")
					return
				}
				logger.V(5).Info("Fake Server: Sending health response", "response", resp)
				// 通过 stream 参数发送响应
				if err := stream.Send(resp); err != nil {
					logger.Error(err, "Fake Server: Error sending response on stream")
					return
				}
			}
		}
	}()

	logger.V(4).Info("Fake Server: WatchResources RPC call returning control to client.")
	return nil
}

// mockWatchResourcesClient 是用于测试 HandleWatchResourcesStream 的健康流客户端 mock，
// 通过 RecvChan 注入响应或错误
type mockWatchResourcesClient struct {
	mock.Mock
	RecvChan chan struct {
		Resp *drahealthv1.NodeWatchResourcesResponse
		Err  error
	}
	Ctx context.Context
}

// Recv 从 RecvChan 读取下一条消息；上下文取消时返回其错误，通道关闭时返回 io.EOF
func (m *mockWatchResourcesClient) Recv() (*drahealthv1.NodeWatchResourcesResponse, error) {
	logger := klog.FromContext(m.Ctx)
	select {
	case <-m.Ctx.Done():
		logger.V(6).Info("mockWatchClient.Recv: Context done", "err", m.Ctx.Err())
		return nil, m.Ctx.Err()
	case item, ok := <-m.RecvChan:
		if !ok {
			logger.V(6).Info("mockWatchClient.Recv: RecvChan closed, returning io.EOF")
			return nil, io.EOF
		}
		return item.Resp, item.Err
	}
}

// Context 返回该 mock 客户端关联的上下文
func (m *mockWatchResourcesClient) Context() context.Context {
	return m.Ctx
}

// Header 返回空的元数据（该 mock 不使用）
func (m *mockWatchResourcesClient) Header() (metadata.MD, error) { return nil, nil }

// Trailer 返回空的 trailer 元数据
func (m *mockWatchResourcesClient) Trailer() metadata.MD { return nil }

// CloseSend 关闭发送方向，mock 中为空操作
func (m *mockWatchResourcesClient) CloseSend() error { return nil }

// RecvMsg 未实现，仅用于满足 grpc.ClientStream 接口
func (m *mockWatchResourcesClient) RecvMsg(v interface{}) error {
	return fmt.Errorf("RecvMsg not implemented")
}

// SendMsg 未实现，仅用于满足 grpc.ClientStream 接口
func (m *mockWatchResourcesClient) SendMsg(v interface{}) error {
	return fmt.Errorf("SendMsg not implemented")
}

// tearDown 是清理函数类型，用于停止测试期间启动的资源
type tearDown func()

// fakeDRAServerInfo 汇总伪造 DRA 服务端的实例、socket 名称与清理函数
type fakeDRAServerInfo struct {
	// 伪造的 DRA 服务端
	server *fakeDRADriverGRPCServer
	// 伪造的 DRA 插件 socket 名称
	socketName string
	// teardownFn 用于停止伪造的 gRPC 服务端
	teardownFn tearDown
}

// setupFakeDRADriverGRPCServer 在临时目录创建 unix socket 并启动伪造的 DRA gRPC 服务端。
// shouldTimeout 为 true 时，服务端会故意阻塞两倍 timeout 以触发客户端超时
func setupFakeDRADriverGRPCServer(ctx context.Context, shouldTimeout bool, pluginClientTimeout *time.Duration, prepareResourcesResponse *drapb.NodePrepareResourcesResponse, unprepareResourcesResponse *drapb.NodeUnprepareResourcesResponse, watchResourcesError error) (fakeDRAServerInfo, error) {
	socketDir, err := os.MkdirTemp("", "dra")
	if err != nil {
		return fakeDRAServerInfo{
			server:     nil,
			socketName: "",
			teardownFn: nil,
		}, err
	}

	socketName := filepath.Join(socketDir, "server.sock")
	stopCh := make(chan struct{})

	teardown := func() {
		close(stopCh)
		if err := os.Remove(socketName); err != nil {
			logger := klog.FromContext(ctx)
			logger.Error(err, "failed to remove socket file", "path", socketName)
		}
	}

	l, err := net.Listen("unix", socketName)
	if err != nil {
		teardown()
		return fakeDRAServerInfo{
			server:     nil,
			socketName: "",
			teardownFn: nil,
		}, err
	}

	s := grpc.NewServer()
	fakeDRADriverGRPCServer := &fakeDRADriverGRPCServer{
		driverName:                 driverName,
		prepareResourcesResponse:   prepareResourcesResponse,
		unprepareResourcesResponse: unprepareResourcesResponse,
		watchResourcesResponses:    make(chan *drahealthv1.NodeWatchResourcesResponse, 10),
		watchResourcesError:        watchResourcesError,
	}
	if shouldTimeout {
		timeout := *pluginClientTimeout * 2
		fakeDRADriverGRPCServer.timeout = &timeout
	}

	drahealthv1.RegisterDRAResourceHealthServer(s, fakeDRADriverGRPCServer)
	drapb.RegisterDRAPluginServer(s, fakeDRADriverGRPCServer)

	go func() {
		go func() {
			logger := klog.FromContext(ctx)
			logger.V(4).Info("Starting fake gRPC server", "address", socketName)
			if err := s.Serve(l); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				logger.Error(err, "failed to serve gRPC")
			}
			logger.V(4).Info("Fake gRPC server stopped serving", "address", socketName)
		}()
		<-stopCh
		logger := klog.FromContext(ctx)
		logger.V(4).Info("Stopping fake gRPC server", "address", socketName)
		s.GracefulStop()
		logger.V(4).Info("Fake gRPC server stopped", "address", socketName)
	}()

	return fakeDRAServerInfo{
		server:     fakeDRADriverGRPCServer,
		socketName: socketName,
		teardownFn: teardown,
	}, nil
}

// genPrepareResourcesResponse 生成针对指定 claim 的 NodePrepareResources 响应，
// 其中包含一个带 CDI 设备 ID 的设备
func genPrepareResourcesResponse(claimUID types.UID) *drapb.NodePrepareResourcesResponse {
	return &drapb.NodePrepareResourcesResponse{
		Claims: map[string]*drapb.NodePrepareResourceResponse{
			string(claimUID): {
				Devices: []*drapb.Device{
					{
						PoolName:     poolName,
						DeviceName:   deviceName,
						RequestNames: []string{requestName},
						CdiDeviceIds: []string{cdiID},
					},
				},
			},
		},
	}
}

// TestNewManagerImpl 验证 NewManager 对状态文件目录的校验：空路径报错，合法路径创建成功
func TestNewManagerImpl(t *testing.T) {
	kubeClient := fake.NewSimpleClientset()
	for _, test := range []struct {
		description        string
		stateFileDirectory string
		wantErr            bool
	}{
		{
			description:        "invalid directory path",
			stateFileDirectory: "",
			wantErr:            true,
		},
		{
			description:        "valid directory path",
			stateFileDirectory: t.TempDir(),
		},
	} {
		t.Run(test.description, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			manager, err := NewManager(tCtx.Logger(), kubeClient, test.stateFileDirectory)
			if test.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			defer manager.Stop()
			assert.NotNil(t, manager.cache)
			assert.NotNil(t, manager.kubeClient)
		})
	}
}

// genTestPod 生成一个引用 ResourceClaim 的测试 Pod
func genTestPod() *v1.Pod {
	claimName := claimName
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
			UID:       podUID,
		},
		Spec: v1.PodSpec{
			ResourceClaims: []v1.PodResourceClaim{
				{
					Name:              claimName,
					ResourceClaimName: &claimName,
				},
			},
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Claims: []v1.ResourceClaim{
							{
								Name: claimName,
							},
						},
					},
				},
			},
		},
	}
}

// genTestPodWithClaimTemplateNoStatus 生成一个通过 ResourceClaimTemplateName 引用 claim、
// 但 Status.ResourceClaimStatuses 为空的 Pod。
// 这模拟 kubernetes/kubernetes#139772：外部组件覆盖了 Pod 状态并丢弃了生成的 claim 名。
// 对这类 Pod，resourceclaim.Name 会返回 ErrClaimNotFound，
// 因此 unprepare 必须依赖 claim info 缓存来获取 claim 名。
func genTestPodWithClaimTemplateNoStatus() *v1.Pod {
	templateName := claimName + "-template"
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
			UID:       podUID,
		},
		Spec: v1.PodSpec{
			ResourceClaims: []v1.PodResourceClaim{
				{
					Name:                      claimName,
					ResourceClaimTemplateName: &templateName,
				},
			},
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Claims: []v1.ResourceClaim{
							{
								Name: claimName,
							},
						},
					},
				},
			},
		},
		// 故意不设置 Status.ResourceClaimStatuses
	}
}

// genTestPodWithClaims 生成一个引用指定多个 ResourceClaim 的测试 Pod，
// Pod 名与 UID 由全局计数器保证唯一
func genTestPodWithClaims(claimNames ...string) *v1.Pod {
	podCounter := testPodCounter.Add(1)

	podName := fmt.Sprintf("test-pod-%d", podCounter)
	podUID := types.UID(fmt.Sprintf("test-pod-uid-%d", podCounter))

	resourceClaims := make([]v1.PodResourceClaim, 0, len(claimNames))
	containerClaims := make([]v1.ResourceClaim, 0, len(claimNames))

	for _, claimName := range claimNames {
		resourceClaims = append(resourceClaims, v1.PodResourceClaim{
			Name:              claimName,
			ResourceClaimName: &claimName,
		})
		containerClaims = append(containerClaims, v1.ResourceClaim{
			Name: claimName,
		})
	}

	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
			UID:       podUID,
		},
		Spec: v1.PodSpec{
			ResourceClaims: resourceClaims,
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Claims: containerClaims,
					},
				},
			},
		},
	}
}

// genTestPodWithExtendedResource 生成一个通过扩展资源（example.com/gpu）映射到 DRA claim 的测试 Pod
func genTestPodWithExtendedResource() *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
			UID:       podUID,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name: containerName,
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							"example.com/gpu": resource.MustParse("1"),
						},
					},
				},
			},
		},
		Status: v1.PodStatus{
			ExtendedResourceClaimStatus: &v1.PodExtendedResourceClaimStatus{
				ResourceClaimName: claimName,
				RequestMappings: []v1.ContainerExtendedResourceRequest{
					{
						ContainerName: containerName,
						ResourceName:  "example.com/gpu",
						RequestName:   "container-0-request-0",
					},
				},
			},
		},
	}
}

// genTestPodWithPodGroup 生成一个设置了 SchedulingGroup 并引用 PodGroup claim 的测试 Pod
func genTestPodWithPodGroup() *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
			UID:       podUID,
		},
		Spec: v1.PodSpec{
			SchedulingGroup: &v1.PodSchedulingGroup{
				PodGroupName: new(podGroupName),
			},
			ResourceClaims: []v1.PodResourceClaim{
				{
					Name:              claimName,
					ResourceClaimName: new(claimName),
				},
			},
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Claims: []v1.ResourceClaim{
							{
								Name: claimName,
							},
						},
					},
				},
			},
		},
	}
}

// genTestClaim 生成一个已分配指定设备并保留给指定 Pod 的 ResourceClaim
func genTestClaim(name, driver, device, podUID string) *resourceapi.ResourceClaim {
	return &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID(fmt.Sprintf("%s-uid", name)),
		},
		Spec: resourceapi.ResourceClaimSpec{
			Devices: resourceapi.DeviceClaim{
				Requests: []resourceapi.DeviceRequest{
					{
						Name: requestName,
						Exactly: &resourceapi.ExactDeviceRequest{
							DeviceClassName: className,
						},
					},
				},
			},
		},
		Status: resourceapi.ResourceClaimStatus{
			Allocation: &resourceapi.AllocationResult{
				Devices: resourceapi.DeviceAllocationResult{
					Results: []resourceapi.DeviceRequestAllocationResult{
						{
							Request: requestName,
							Pool:    poolName,
							Device:  device,
							Driver:  driver,
						},
					},
				},
			},
			ReservedFor: []resourceapi.ResourceClaimConsumerReference{
				{UID: types.UID(podUID)},
			},
		},
	}
}

// genTestClaimWithExtendedResource 生成一个由扩展资源转换而来的、已分配的 ResourceClaim
func genTestClaimWithExtendedResource(name, driver, device, podUID string) *resourceapi.ResourceClaim {
	return &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID(fmt.Sprintf("%s-uid", name)),
		},
		Spec: resourceapi.ResourceClaimSpec{
			Devices: resourceapi.DeviceClaim{
				Requests: []resourceapi.DeviceRequest{
					{
						Name: "container-0-request-0",
						Exactly: &resourceapi.ExactDeviceRequest{
							DeviceClassName: className,
						},
					},
				},
			},
		},
		Status: resourceapi.ResourceClaimStatus{
			Allocation: &resourceapi.AllocationResult{
				Devices: resourceapi.DeviceAllocationResult{
					Results: []resourceapi.DeviceRequestAllocationResult{
						{
							Request: "container-0-request-0",
							Pool:    poolName,
							Device:  device,
							Driver:  driver,
						},
					},
				},
			},
			ReservedFor: []resourceapi.ResourceClaimConsumerReference{
				{UID: types.UID(podUID)},
			},
		},
	}
}

// genTestPodGroupClaim 生成一个保留给 PodGroup（而非单个 Pod）的 ResourceClaim
func genTestPodGroupClaim(name, driver, device, podGroupName string) *resourceapi.ResourceClaim {
	return &resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			UID:       types.UID(fmt.Sprintf("%s-uid", name)),
		},
		Spec: resourceapi.ResourceClaimSpec{
			Devices: resourceapi.DeviceClaim{
				Requests: []resourceapi.DeviceRequest{
					{
						Name: requestName,
						Exactly: &resourceapi.ExactDeviceRequest{
							DeviceClassName: className,
						},
					},
				},
			},
		},
		Status: resourceapi.ResourceClaimStatus{
			Allocation: &resourceapi.AllocationResult{
				Devices: resourceapi.DeviceAllocationResult{
					Results: []resourceapi.DeviceRequestAllocationResult{
						{
							Request: requestName,
							Pool:    poolName,
							Device:  device,
							Driver:  driver,
						},
					},
				},
			},
			ReservedFor: []resourceapi.ResourceClaimConsumerReference{
				{
					APIGroup: schedulingapi.GroupName,
					Resource: "podgroups",
					Name:     podGroupName,
				},
			},
		},
	}
}

// genTestClaimInfo 生成一条 claim info 缓存记录，可指定 prepared 状态与 ShareID
func genTestClaimInfo(claimUID types.UID, podUIDs []string, prepared bool, shareID *types.UID) *ClaimInfo {
	return &ClaimInfo{
		ClaimInfoState: state.ClaimInfoState{
			ClaimUID:  claimUID,
			ClaimName: claimName,
			Namespace: namespace,
			PodUIDs:   sets.New[string](podUIDs...),
			DriverState: map[string]state.DriverState{
				driverName: {
					Devices: []state.Device{{
						PoolName:     poolName,
						DeviceName:   deviceName,
						ShareID:      shareID,
						RequestNames: []string{requestName},
						CDIDeviceIDs: []string{cdiID},
					}},
				},
			},
		},
		prepared: prepared,
	}
}

// genTestClaimInfoWithExtendedResource 生成扩展资源场景的 claim info 缓存记录
func genTestClaimInfoWithExtendedResource(podUIDs []string, prepared bool) *ClaimInfo {
	return &ClaimInfo{
		ClaimInfoState: state.ClaimInfoState{
			ClaimUID:  claimUID,
			ClaimName: claimName,
			Namespace: namespace,
			PodUIDs:   sets.New[string](podUIDs...),
			DriverState: map[string]state.DriverState{
				driverName: {
					Devices: []state.Device{{
						PoolName:     poolName,
						DeviceName:   deviceName,
						RequestNames: []string{"container-0-request-0"},
						CDIDeviceIDs: []string{cdiID},
					}},
				},
			},
		},
		prepared: prepared,
	}
}

// genClaimInfoState 生成 claim info 的持久化状态，cdiDeviceID 非空时附带设备信息
func genClaimInfoState(cdiDeviceID string) state.ClaimInfoState {
	s := state.ClaimInfoState{
		ClaimUID:  claimUID,
		ClaimName: claimName,
		Namespace: namespace,
		PodUIDs:   sets.New[string](podUID),
		DriverState: map[string]state.DriverState{
			driverName: {},
		},
	}
	if cdiDeviceID != "" {
		s.DriverState[driverName] = state.DriverState{Devices: []state.Device{{PoolName: poolName, DeviceName: deviceName, RequestNames: []string{requestName}, CDIDeviceIDs: []string{cdiDeviceID}}}}
	}
	return s
}

// genClaimInfoStateWithExtendedResource 生成扩展资源场景的 claim info 状态，
// 设备请求名为扩展资源映射使用的 container-0-request-0
func genClaimInfoStateWithExtendedResource(cdiDeviceID string) state.ClaimInfoState {
	s := state.ClaimInfoState{
		ClaimUID:  claimUID,
		ClaimName: claimName,
		Namespace: namespace,
		PodUIDs:   sets.New[string](podUID),
		DriverState: map[string]state.DriverState{
			driverName: {},
		},
	}
	if cdiDeviceID != "" {
		s.DriverState[driverName] = state.DriverState{Devices: []state.Device{{PoolName: poolName, DeviceName: deviceName, RequestNames: []string{"container-0-request-0"}, CDIDeviceIDs: []string{cdiDeviceID}}}}
	}
	return s
}

// genClaimInfoStateWithShareID 生成设备带 ShareID 的 claim info 状态
func genClaimInfoStateWithShareID(cdiDeviceID string) state.ClaimInfoState {
	s := state.ClaimInfoState{
		ClaimUID:  claimUID,
		ClaimName: claimName,
		Namespace: namespace,
		PodUIDs:   sets.New[string](podUID),
		DriverState: map[string]state.DriverState{
			driverName: {},
		},
	}
	if cdiDeviceID != "" {
		s.DriverState[driverName] = state.DriverState{Devices: []state.Device{
			{PoolName: poolName, DeviceName: deviceName, ShareID: &shareUID, RequestNames: []string{requestName}, CDIDeviceIDs: []string{cdiDeviceID}},
		}}
	}
	return s
}

// TestGetResources 验证 GetResources 能从 claim info 中提取容器所需的 CDI 设备，
// 覆盖普通 claim、扩展资源 claim、同一 claim 被多个 request 引用以及 claim 缺失等场景
func TestGetResources(t *testing.T) {
	kubeClient := fake.NewSimpleClientset()

	for _, test := range []struct {
		description    string
		container      *v1.Container
		pod            *v1.Pod
		claimInfo      *ClaimInfo
		wantErr        bool
		wantCDIDevices []kubecontainer.CDIDevice
	}{
		{
			description: "claim info with devices",
			container: &v1.Container{
				Name: containerName,
				Resources: v1.ResourceRequirements{
					Claims: []v1.ResourceClaim{
						{
							Name: claimName,
						},
					},
				},
			},
			pod:            genTestPod(),
			claimInfo:      genTestClaimInfo(claimUID, nil, false, nil),
			wantCDIDevices: []kubecontainer.CDIDevice{{Name: cdiID}},
		},
		{
			description: "nil claiminfo",
			container: &v1.Container{
				Name: containerName,
				Resources: v1.ResourceRequirements{
					Claims: []v1.ResourceClaim{
						{
							Name: claimName,
						},
					},
				},
			},
			pod:     genTestPod(),
			wantErr: true,
		},
		{
			description: "extended resource claim info with devices",
			container: &v1.Container{
				Name: containerName,
				Resources: v1.ResourceRequirements{
					Requests: v1.ResourceList{
						"example.com/gpu": resource.MustParse("1"),
					},
				},
			},
			pod:            genTestPodWithExtendedResource(),
			claimInfo:      genTestClaimInfoWithExtendedResource(nil, false),
			wantCDIDevices: []kubecontainer.CDIDevice{{Name: cdiID}},
		},
		{
			description: "same claim referenced by multiple requests",
			container: &v1.Container{
				Name: containerName,
				Resources: v1.ResourceRequirements{
					Claims: []v1.ResourceClaim{
						{
							Name:    claimName,
							Request: requestName,
						},
						{
							Name:    claimName,
							Request: requestName2,
						},
					},
				},
			},
			pod: genTestPod(),
			claimInfo: &ClaimInfo{
				ClaimInfoState: state.ClaimInfoState{
					ClaimUID:  claimUID,
					ClaimName: claimName,
					Namespace: namespace,
					PodUIDs:   sets.New[string](),
					DriverState: map[string]state.DriverState{
						driverName: {
							Devices: []state.Device{{
								PoolName:     poolName,
								DeviceName:   deviceName,
								RequestNames: []string{requestName, requestName2},
								CDIDeviceIDs: []string{cdiID},
							}},
						},
					},
				},
			},
			wantCDIDevices: []kubecontainer.CDIDevice{{Name: cdiID}, {Name: cdiID}},
		},
	} {
		t.Run(test.description, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			manager, err := NewManager(tCtx.Logger(), kubeClient, t.TempDir())
			require.NoError(t, err)
			defer manager.Stop()

			if test.claimInfo != nil {
				manager.cache.add(test.claimInfo)
			}

			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DRAExtendedResource, true)
			containerInfo, err := manager.GetResources(test.pod, test.container)
			if test.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.wantCDIDevices, containerInfo.CDIDevices)
			}
		})
	}
}

// getFakeNode 返回一个固定的节点对象，供插件管理器初始化使用
func getFakeNode(context.Context) (*v1.Node, error) {
	return &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker"}}, nil
}

// TestPrepareResources 验证 PrepareResources 在各种边界条件下的行为与指标：
// claim 不存在、驱动未注册、驱动返回空响应、Pod 无权使用 claim、重试不重复追加设备、
// 超时、扩展资源与 PodGroup claim、ShareID 等
func TestPrepareResources(t *testing.T) {
	claimName := claimName
	fakeKubeClient := fake.NewSimpleClientset()
	anotherClaimUID := types.UID("another-claim-uid")
	shareUID := types.UID(shareID)

	for _, test := range []struct {
		description         string
		driverName          string
		pod                 *v1.Pod
		claimInfo           *ClaimInfo
		claim               *resourceapi.ResourceClaim
		resp                *drapb.NodePrepareResourcesResponse
		wantTimeout         bool
		wantResourceSkipped bool

		draWorkloadResourceClaimsEnabled bool

		expectedErrMsg         string
		expectedClaimInfoState state.ClaimInfoState
		expectedPrepareCalls   uint32
		expectedMetric         string
	}{
		{
			description:    "claim doesn't exist",
			driverName:     driverName,
			pod:            genTestPod(),
			expectedErrMsg: "fetch ResourceClaim ",
			expectedMetric: `# HELP dra_operations_duration_seconds [ALPHA] Latency histogram in seconds for the duration of handling all ResourceClaims referenced by a pod when the pod starts or stops. Identified by the name of the operation (PrepareResources or UnprepareResources) and separated by the success of the operation. The number of failed operations is provided through the histogram's overall count.
# TYPE dra_operations_duration_seconds histogram
dra_operations_duration_seconds_bucket{is_error="true",operation_name="PrepareResources",le="+Inf"} 1
dra_operations_duration_seconds_sum{is_error="true",operation_name="PrepareResources"} 0
dra_operations_duration_seconds_count{is_error="true",operation_name="PrepareResources"} 1
`,
		},
		{
			description:    "unknown driver",
			pod:            genTestPod(),
			claim:          genTestClaim(claimName, "unknown driver", deviceName, podUID),
			expectedErrMsg: "prepare dynamic resources: DRA driver unknown driver is not registered",
			expectedMetric: `# HELP dra_operations_duration_seconds [ALPHA] Latency histogram in seconds for the duration of handling all ResourceClaims referenced by a pod when the pod starts or stops. Identified by the name of the operation (PrepareResources or UnprepareResources) and separated by the success of the operation. The number of failed operations is provided through the histogram's overall count.
# TYPE dra_operations_duration_seconds histogram
dra_operations_duration_seconds_bucket{is_error="true",operation_name="PrepareResources",le="+Inf"} 1
dra_operations_duration_seconds_sum{is_error="true",operation_name="PrepareResources"} 0
dra_operations_duration_seconds_count{is_error="true",operation_name="PrepareResources"} 1
`,
		},
		{
			description:            "should prepare resources, driver returns nil value",
			driverName:             driverName,
			pod:                    genTestPod(),
			claim:                  genTestClaim(claimName, driverName, deviceName, podUID),
			resp:                   &drapb.NodePrepareResourcesResponse{Claims: map[string]*drapb.NodePrepareResourceResponse{string(claimUID): nil}},
			expectedClaimInfoState: genClaimInfoState(""),
			expectedPrepareCalls:   1,
			expectedMetric: `# HELP dra_grpc_operations_duration_seconds [ALPHA] Duration in seconds of the DRA gRPC operations
# TYPE dra_grpc_operations_duration_seconds histogram
dra_grpc_operations_duration_seconds_bucket{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodePrepareResources",le="+Inf"} 1
dra_grpc_operations_duration_seconds_sum{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodePrepareResources"} 0
dra_grpc_operations_duration_seconds_count{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodePrepareResources"} 1
# HELP dra_operations_duration_seconds [ALPHA] Latency histogram in seconds for the duration of handling all ResourceClaims referenced by a pod when the pod starts or stops. Identified by the name of the operation (PrepareResources or UnprepareResources) and separated by the success of the operation. The number of failed operations is provided through the histogram's overall count.
# TYPE dra_operations_duration_seconds histogram
dra_operations_duration_seconds_bucket{is_error="false",operation_name="PrepareResources",le="+Inf"} 1
dra_operations_duration_seconds_sum{is_error="false",operation_name="PrepareResources"} 0
dra_operations_duration_seconds_count{is_error="false",operation_name="PrepareResources"} 1
`,
		},
		{
			description:          "driver returns empty result",
			driverName:           driverName,
			pod:                  genTestPod(),
			claim:                genTestClaim(claimName, driverName, deviceName, podUID),
			resp:                 &drapb.NodePrepareResourcesResponse{Claims: map[string]*drapb.NodePrepareResourceResponse{}},
			expectedPrepareCalls: 1,
			expectedErrMsg:       "NodePrepareResources skipped 1 ResourceClaims",
			expectedMetric: `# HELP dra_grpc_operations_duration_seconds [ALPHA] Duration in seconds of the DRA gRPC operations
# TYPE dra_grpc_operations_duration_seconds histogram
dra_grpc_operations_duration_seconds_bucket{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodePrepareResources",le="+Inf"} 1
dra_grpc_operations_duration_seconds_sum{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodePrepareResources"} 0
dra_grpc_operations_duration_seconds_count{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodePrepareResources"} 1
# HELP dra_operations_duration_seconds [ALPHA] Latency histogram in seconds for the duration of handling all ResourceClaims referenced by a pod when the pod starts or stops. Identified by the name of the operation (PrepareResources or UnprepareResources) and separated by the success of the operation. The number of failed operations is provided through the histogram's overall count.
# TYPE dra_operations_duration_seconds histogram
dra_operations_duration_seconds_bucket{is_error="true",operation_name="PrepareResources",le="+Inf"} 1
dra_operations_duration_seconds_sum{is_error="true",operation_name="PrepareResources"} 0
dra_operations_duration_seconds_count{is_error="true",operation_name="PrepareResources"} 1
`,
		},
		{
			description:    "pod is not allowed to use resource claim",
			driverName:     driverName,
			pod:            genTestPod(),
			claim:          genTestClaim(claimName, driverName, deviceName, ""),
			expectedErrMsg: "is not allowed to use ResourceClaim ",
			expectedMetric: `# HELP dra_operations_duration_seconds [ALPHA] Latency histogram in seconds for the duration of handling all ResourceClaims referenced by a pod when the pod starts or stops. Identified by the name of the operation (PrepareResources or UnprepareResources) and separated by the success of the operation. The number of failed operations is provided through the histogram's overall count.
# TYPE dra_operations_duration_seconds histogram
dra_operations_duration_seconds_bucket{is_error="true",operation_name="PrepareResources",le="+Inf"} 1
dra_operations_duration_seconds_sum{is_error="true",operation_name="PrepareResources"} 0
dra_operations_duration_seconds_count{is_error="true",operation_name="PrepareResources"} 1
`,
		},
		{
			description:                      "pod is not allowed to use resource claim from podgroup",
			draWorkloadResourceClaimsEnabled: true,
			driverName:                       driverName,
			pod:                              genTestPodWithPodGroup(),
			claim:                            genTestPodGroupClaim(claimName, driverName, deviceName, ""),
			expectedErrMsg:                   "is not allowed to use ResourceClaim ",
			expectedMetric: `# HELP dra_operations_duration_seconds [ALPHA] Latency histogram in seconds for the duration of handling all ResourceClaims referenced by a pod when the pod starts or stops. Identified by the name of the operation (PrepareResources or UnprepareResources) and separated by the success of the operation. The number of failed operations is provided through the histogram's overall count.
# TYPE dra_operations_duration_seconds histogram
dra_operations_duration_seconds_bucket{is_error="true",operation_name="PrepareResources",le="+Inf"} 1
dra_operations_duration_seconds_sum{is_error="true",operation_name="PrepareResources"} 0
dra_operations_duration_seconds_count{is_error="true",operation_name="PrepareResources"} 1
`,
		},
		{
			description: "no container uses the claim",
			driverName:  driverName,
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      podName,
					Namespace: namespace,
					UID:       podUID,
				},
				Spec: v1.PodSpec{
					ResourceClaims: []v1.PodResourceClaim{
						{
							Name:              claimName,
							ResourceClaimName: &claimName,
						},
					},
					Containers: []v1.Container{
						{},
					},
				},
			},
			claim:                genTestClaim(claimName, driverName, deviceName, podUID),
			expectedPrepareCalls: 1,
			expectedMetric: `# HELP dra_grpc_operations_duration_seconds [ALPHA] Duration in seconds of the DRA gRPC operations
# TYPE dra_grpc_operations_duration_seconds histogram
dra_grpc_operations_duration_seconds_bucket{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodePrepareResources",le="+Inf"} 1
dra_grpc_operations_duration_seconds_sum{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodePrepareResources"} 0
dra_grpc_operations_duration_seconds_count{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodePrepareResources"} 1
# HELP dra_operations_duration_seconds [ALPHA] Latency histogram in seconds for the duration of handling all ResourceClaims referenced by a pod when the pod starts or stops. Identified by the name of the operation (PrepareResources or UnprepareResources) and separated by the success of the operation. The number of failed operations is provided through the histogram's overall count.
# TYPE dra_operations_duration_seconds histogram
dra_operations_duration_seconds_bucket{is_error="false",operation_name="PrepareResources",le="+Inf"} 1
dra_operations_duration_seconds_sum{is_error="false",operation_name="PrepareResources"} 0
dra_operations_duration_seconds_count{is_error="false",operation_name="PrepareResources"} 1
`,
			expectedClaimInfoState: genClaimInfoState(cdiID),
			resp:                   genPrepareResourcesResponse(claimUID),
		},
		{
			description:            "resource already prepared",
			driverName:             driverName,
			pod:                    genTestPod(),
			claim:                  genTestClaim(claimName, driverName, deviceName, podUID),
			claimInfo:              genTestClaimInfo(claimUID, []string{podUID}, true, nil),
			expectedClaimInfoState: genClaimInfoState(cdiID),
			resp:                   genPrepareResourcesResponse(claimUID),
			expectedMetric: `# HELP dra_operations_duration_seconds [ALPHA] Latency histogram in seconds for the duration of handling all ResourceClaims referenced by a pod when the pod starts or stops. Identified by the name of the operation (PrepareResources or UnprepareResources) and separated by the success of the operation. The number of failed operations is provided through the histogram's overall count.
# TYPE dra_operations_duration_seconds histogram
dra_operations_duration_seconds_bucket{is_error="false",operation_name="PrepareResources",le="+Inf"} 1
dra_operations_duration_seconds_sum{is_error="false",operation_name="PrepareResources"} 0
dra_operations_duration_seconds_count{is_error="false",operation_name="PrepareResources"} 1
`,
		},
		{
			// 回归测试：若上一次 PrepareResources 已把设备追加到缓存的 ClaimInfo，
			// 但在到达 setPrepared 之前就返回（例如同批次中某个驱动失败），
			// 重试时不得重复追加这些设备。
			// 预置的 ClaimInfo 模拟了这种遗留状态：prepared=false，
			// 但 DriverState 已包含 fake driver 在重试时会返回的设备。
			// 重试后 DriverState 必须恰好包含一个设备，而不是两个。
			description:            "should not duplicate devices on prepare retry",
			driverName:             driverName,
			pod:                    genTestPod(),
			claim:                  genTestClaim(claimName, driverName, deviceName, podUID),
			claimInfo:              genTestClaimInfo(claimUID, []string{podUID}, false, nil),
			resp:                   genPrepareResourcesResponse(claimUID),
			expectedClaimInfoState: genClaimInfoState(cdiID),
			expectedPrepareCalls:   1,
			expectedMetric: `# HELP dra_grpc_operations_duration_seconds [ALPHA] Duration in seconds of the DRA gRPC operations
# TYPE dra_grpc_operations_duration_seconds histogram
dra_grpc_operations_duration_seconds_bucket{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodePrepareResources",le="+Inf"} 1
dra_grpc_operations_duration_seconds_sum{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodePrepareResources"} 0
dra_grpc_operations_duration_seconds_count{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodePrepareResources"} 1
# HELP dra_operations_duration_seconds [ALPHA] Latency histogram in seconds for the duration of handling all ResourceClaims referenced by a pod when the pod starts or stops. Identified by the name of the operation (PrepareResources or UnprepareResources) and separated by the success of the operation. The number of failed operations is provided through the histogram's overall count.
# TYPE dra_operations_duration_seconds histogram
dra_operations_duration_seconds_bucket{is_error="false",operation_name="PrepareResources",le="+Inf"} 1
dra_operations_duration_seconds_sum{is_error="false",operation_name="PrepareResources"} 0
dra_operations_duration_seconds_count{is_error="false",operation_name="PrepareResources"} 1
`,
		},
		{
			description:          "should timeout",
			driverName:           driverName,
			pod:                  genTestPod(),
			claim:                genTestClaim(claimName, driverName, deviceName, podUID),
			wantTimeout:          true,
			expectedPrepareCalls: 1,
			expectedErrMsg:       "NodePrepareResources: rpc error: code = DeadlineExceeded",
			expectedMetric: `# HELP dra_grpc_operations_duration_seconds [ALPHA] Duration in seconds of the DRA gRPC operations
# TYPE dra_grpc_operations_duration_seconds histogram
dra_grpc_operations_duration_seconds_bucket{driver_name="test-driver",grpc_status_code="DeadlineExceeded",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodePrepareResources",le="+Inf"} 1
dra_grpc_operations_duration_seconds_sum{driver_name="test-driver",grpc_status_code="DeadlineExceeded",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodePrepareResources"} 0
dra_grpc_operations_duration_seconds_count{driver_name="test-driver",grpc_status_code="DeadlineExceeded",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodePrepareResources"} 1
# HELP dra_operations_duration_seconds [ALPHA] Latency histogram in seconds for the duration of handling all ResourceClaims referenced by a pod when the pod starts or stops. Identified by the name of the operation (PrepareResources or UnprepareResources) and separated by the success of the operation. The number of failed operations is provided through the histogram's overall count.
# TYPE dra_operations_duration_seconds histogram
dra_operations_duration_seconds_bucket{is_error="true",operation_name="PrepareResources",le="+Inf"} 1
dra_operations_duration_seconds_sum{is_error="true",operation_name="PrepareResources"} 0
dra_operations_duration_seconds_count{is_error="true",operation_name="PrepareResources"} 1
`,
		},
		{
			description:            "should prepare resource, claim not in cache",
			driverName:             driverName,
			pod:                    genTestPod(),
			claim:                  genTestClaim(claimName, driverName, deviceName, podUID),
			expectedClaimInfoState: genClaimInfoState(cdiID),
			resp:                   genPrepareResourcesResponse(claimUID),
			expectedPrepareCalls:   1,
			expectedMetric: `# HELP dra_grpc_operations_duration_seconds [ALPHA] Duration in seconds of the DRA gRPC operations
# TYPE dra_grpc_operations_duration_seconds histogram
dra_grpc_operations_duration_seconds_bucket{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodePrepareResources",le="+Inf"} 1
dra_grpc_operations_duration_seconds_sum{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodePrepareResources"} 0
dra_grpc_operations_duration_seconds_count{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodePrepareResources"} 1
# HELP dra_operations_duration_seconds [ALPHA] Latency histogram in seconds for the duration of handling all ResourceClaims referenced by a pod when the pod starts or stops. Identified by the name of the operation (PrepareResources or UnprepareResources) and separated by the success of the operation. The number of failed operations is provided through the histogram's overall count.
# TYPE dra_operations_duration_seconds histogram
dra_operations_duration_seconds_bucket{is_error="false",operation_name="PrepareResources",le="+Inf"} 1
dra_operations_duration_seconds_sum{is_error="false",operation_name="PrepareResources"} 0
dra_operations_duration_seconds_count{is_error="false",operation_name="PrepareResources"} 1
`,
		},
		{
			description:            "should prepare extended resource claim backed by DRA",
			driverName:             driverName,
			pod:                    genTestPodWithExtendedResource(),
			claim:                  genTestClaimWithExtendedResource(claimName, driverName, deviceName, podUID),
			expectedClaimInfoState: genClaimInfoStateWithExtendedResource(cdiID),
			resp: &drapb.NodePrepareResourcesResponse{Claims: map[string]*drapb.NodePrepareResourceResponse{
				string(claimUID): {
					Devices: []*drapb.Device{
						{
							PoolName:     poolName,
							DeviceName:   deviceName,
							RequestNames: []string{"container-0-request-0"},
							CdiDeviceIds: []string{cdiID},
						},
					},
				},
			}},
			expectedPrepareCalls: 1,
			expectedMetric: `# HELP dra_grpc_operations_duration_seconds [ALPHA] Duration in seconds of the DRA gRPC operations
# TYPE dra_grpc_operations_duration_seconds histogram
dra_grpc_operations_duration_seconds_bucket{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodePrepareResources",le="+Inf"} 1
dra_grpc_operations_duration_seconds_sum{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodePrepareResources"} 0
dra_grpc_operations_duration_seconds_count{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodePrepareResources"} 1
# HELP dra_operations_duration_seconds [ALPHA] Latency histogram in seconds for the duration of handling all ResourceClaims referenced by a pod when the pod starts or stops. Identified by the name of the operation (PrepareResources or UnprepareResources) and separated by the success of the operation. The number of failed operations is provided through the histogram's overall count.
# TYPE dra_operations_duration_seconds histogram
dra_operations_duration_seconds_bucket{is_error="false",operation_name="PrepareResources",le="+Inf"} 1
dra_operations_duration_seconds_sum{is_error="false",operation_name="PrepareResources"} 0
dra_operations_duration_seconds_count{is_error="false",operation_name="PrepareResources"} 1
`,
		},
		{
			description:                      "should prepare resource for podgroup, claim not in cache",
			draWorkloadResourceClaimsEnabled: true,
			driverName:                       driverName,
			pod:                              genTestPodWithPodGroup(),
			claim:                            genTestPodGroupClaim(claimName, driverName, deviceName, podGroupName),
			expectedClaimInfoState:           genClaimInfoState(cdiID),
			resp:                             genPrepareResourcesResponse(claimUID),
			expectedPrepareCalls:             1,
			expectedMetric: `# HELP dra_grpc_operations_duration_seconds [ALPHA] Duration in seconds of the DRA gRPC operations
# TYPE dra_grpc_operations_duration_seconds histogram
dra_grpc_operations_duration_seconds_bucket{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodePrepareResources",le="+Inf"} 1
dra_grpc_operations_duration_seconds_sum{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodePrepareResources"} 0
dra_grpc_operations_duration_seconds_count{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodePrepareResources"} 1
# HELP dra_operations_duration_seconds [ALPHA] Latency histogram in seconds for the duration of handling all ResourceClaims referenced by a pod when the pod starts or stops. Identified by the name of the operation (PrepareResources or UnprepareResources) and separated by the success of the operation. The number of failed operations is provided through the histogram's overall count.
# TYPE dra_operations_duration_seconds histogram
dra_operations_duration_seconds_bucket{is_error="false",operation_name="PrepareResources",le="+Inf"} 1
dra_operations_duration_seconds_sum{is_error="false",operation_name="PrepareResources"} 0
dra_operations_duration_seconds_count{is_error="false",operation_name="PrepareResources"} 1
`,
		},
		{
			description:                      "should fail to prepare resource for podgroup when the feature is disabled",
			draWorkloadResourceClaimsEnabled: false,
			driverName:                       driverName,
			pod:                              genTestPodWithPodGroup(),
			claim:                            genTestPodGroupClaim(claimName, driverName, deviceName, podGroupName),
			expectedClaimInfoState:           genClaimInfoState(cdiID),
			resp:                             genPrepareResourcesResponse(claimUID),
			expectedPrepareCalls:             0,
			expectedErrMsg:                   "is not allowed to use ResourceClaim ",
			expectedMetric: `# HELP dra_operations_duration_seconds [ALPHA] Latency histogram in seconds for the duration of handling all ResourceClaims referenced by a pod when the pod starts or stops. Identified by the name of the operation (PrepareResources or UnprepareResources) and separated by the success of the operation. The number of failed operations is provided through the histogram's overall count.
# TYPE dra_operations_duration_seconds histogram
dra_operations_duration_seconds_bucket{is_error="true",operation_name="PrepareResources",le="+Inf"} 1
dra_operations_duration_seconds_sum{is_error="true",operation_name="PrepareResources"} 0
dra_operations_duration_seconds_count{is_error="true",operation_name="PrepareResources"} 1
`,
		},
		{
			description:    "claim UIDs mismatch",
			driverName:     driverName,
			pod:            genTestPod(),
			claim:          genTestClaim(claimName, driverName, deviceName, podUID),
			claimInfo:      genTestClaimInfo(anotherClaimUID, []string{podUID}, false, nil),
			expectedErrMsg: fmt.Sprintf("old ResourceClaim with same name %s and different UID %s still exists", claimName, anotherClaimUID),
			expectedMetric: `# HELP dra_operations_duration_seconds [ALPHA] Latency histogram in seconds for the duration of handling all ResourceClaims referenced by a pod when the pod starts or stops. Identified by the name of the operation (PrepareResources or UnprepareResources) and separated by the success of the operation. The number of failed operations is provided through the histogram's overall count.
# TYPE dra_operations_duration_seconds histogram
dra_operations_duration_seconds_bucket{is_error="true",operation_name="PrepareResources",le="+Inf"} 1
dra_operations_duration_seconds_sum{is_error="true",operation_name="PrepareResources"} 0
dra_operations_duration_seconds_count{is_error="true",operation_name="PrepareResources"} 1
`,
		},
		{
			description: "should prepare resources with share id",
			driverName:  driverName,
			pod:         genTestPod(),
			claim: func() *resourceapi.ResourceClaim {
				claim := genTestClaim(claimName, driverName, deviceName, podUID)
				claim.Status.Allocation.Devices.Results[0].ShareID = &shareUID
				return claim
			}(),
			expectedClaimInfoState: genClaimInfoStateWithShareID(cdiID),
			resp: &drapb.NodePrepareResourcesResponse{Claims: map[string]*drapb.NodePrepareResourceResponse{
				string(claimUID): {
					Devices: []*drapb.Device{
						{
							PoolName:     poolName,
							DeviceName:   deviceName,
							RequestNames: []string{requestName},
							ShareId:      &shareID,
							CdiDeviceIds: []string{cdiID},
						},
					},
				},
			}},
			expectedPrepareCalls: 1,
			expectedMetric: `# HELP dra_grpc_operations_duration_seconds [ALPHA] Duration in seconds of the DRA gRPC operations
# TYPE dra_grpc_operations_duration_seconds histogram
dra_grpc_operations_duration_seconds_bucket{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodePrepareResources",le="+Inf"} 1
dra_grpc_operations_duration_seconds_sum{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodePrepareResources"} 0
dra_grpc_operations_duration_seconds_count{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodePrepareResources"} 1
# HELP dra_operations_duration_seconds [ALPHA] Latency histogram in seconds for the duration of handling all ResourceClaims referenced by a pod when the pod starts or stops. Identified by the name of the operation (PrepareResources or UnprepareResources) and separated by the success of the operation. The number of failed operations is provided through the histogram's overall count.
# TYPE dra_operations_duration_seconds histogram
dra_operations_duration_seconds_bucket{is_error="false",operation_name="PrepareResources",le="+Inf"} 1
dra_operations_duration_seconds_sum{is_error="false",operation_name="PrepareResources"} 0
dra_operations_duration_seconds_count{is_error="false",operation_name="PrepareResources"} 1
`,
		},
	} {
		t.Run(test.description, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			backgroundCtx, cancel := context.WithCancel(tCtx)
			defer cancel()

			kubeletmetrics.Register()
			kubeletmetrics.DRAOperationsDuration.Reset()
			kubeletmetrics.DRAGRPCOperationsDuration.Reset()
			defer func() {
				require.NoError(t,
					metricstestutil.GatherAndCompare(gatherWithoutDurations,
						strings.NewReader(test.expectedMetric),
						kubeletmetrics.DRAOperationsDuration.FQName(),
						kubeletmetrics.DRAGRPCOperationsDuration.FQName(),
					),
				)
			}()

			backgroundCtx = klog.NewContext(backgroundCtx, tCtx.Logger())

			manager, err := NewManager(tCtx.Logger(), fakeKubeClient, t.TempDir())
			require.NoError(t, err, "create DRA manager")
			defer manager.Stop()
			manager.initDRAPluginManager(backgroundCtx, getFakeNode, time.Second /* 测试中用极短的清理延迟 */)

			if test.claim != nil {
				if _, err := fakeKubeClient.ResourceV1().ResourceClaims(test.pod.Namespace).Create(tCtx, test.claim, metav1.CreateOptions{}); err != nil {
					t.Fatalf("failed to create ResourceClaim %s: %+v", test.claim.Name, err)
				}
				defer func() {
					require.NoError(t, fakeKubeClient.ResourceV1().ResourceClaims(test.pod.Namespace).Delete(tCtx, test.claim.Name, metav1.DeleteOptions{}))
				}()
			}

			var pluginClientTimeout *time.Duration
			if test.wantTimeout {
				timeout := time.Millisecond * 20
				pluginClientTimeout = &timeout
			}

			draServerInfo, err := setupFakeDRADriverGRPCServer(backgroundCtx, test.wantTimeout, pluginClientTimeout, test.resp, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer draServerInfo.teardownFn()
			plg := manager.GetWatcherHandler()
			if err := plg.RegisterPlugin(tCtx, test.driverName, draServerInfo.socketName, []string{drapb.DRAPluginService}, pluginClientTimeout); err != nil {
				t.Fatalf("failed to register plugin %s, err: %v", test.driverName, err)
			}

			if test.claimInfo != nil {
				manager.cache.add(test.claimInfo)
			}

			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.DRAExtendedResource:       true,
				features.DRAConsumableCapacity:     true,
				features.DRAWorkloadResourceClaims: test.draWorkloadResourceClaimsEnabled,
				features.GenericWorkload:           test.draWorkloadResourceClaimsEnabled, // dependency of DRAWorkloadResourceClaims
			})
			err = manager.PrepareResources(backgroundCtx, test.pod)

			assert.Equal(t, test.expectedPrepareCalls, draServerInfo.server.prepareResourceCalls.Load())

			if test.expectedErrMsg != "" {
				assert.Error(t, err)
				if err != nil {
					assert.ErrorContains(t, err, test.expectedErrMsg)
				}
				return // PrepareResources returned an error so stopping the test case here
			}

			require.NoError(t, err)

			if test.wantResourceSkipped {
				return // resource skipped so no need to continue
			}

			// 校验缓存中包含预期的 claim info
			var podClaimName *string
			if len(test.pod.Spec.ResourceClaims) > 0 {
				podClaimName, _, err = resourceclaim.Name(test.pod, &test.pod.Spec.ResourceClaims[0])
				require.NoError(t, err)
			}
			if podClaimName == nil {
				podClaimName = &claimName
			}
			claimInfoResult, ok := manager.cache.get(*podClaimName, test.pod.Namespace)
			require.True(t, ok, "claimInfo not found in cache")
			require.True(t, claimInfoResult.PodUIDs.Has(string(test.pod.UID)), "podUIDs mismatch")
			assert.Equal(t, test.expectedClaimInfoState.ClaimUID, claimInfoResult.ClaimUID)
			assert.Equal(t, test.expectedClaimInfoState.ClaimName, claimInfoResult.ClaimName)
			assert.Equal(t, test.expectedClaimInfoState.Namespace, claimInfoResult.Namespace)
			assert.Equal(t, test.expectedClaimInfoState.DriverState, claimInfoResult.DriverState)
			assert.True(t, claimInfoResult.prepared, "ClaimInfo should be marked as prepared")
		})
	}
}

// TestPrepareResourcesWithPreparedAndNewClaim 验证 PrepareResources 能正确处理
// 同时引用“已由其他 Pod 准备过”和“新 claim”的 Pod：
// - 第一个 claim 已由之前的 Pod 准备过；
// - 第二个 claim 是新 claim，需要准备
func TestPrepareResourcesWithPreparedAndNewClaim(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	fakeKubeClient := fake.NewClientset()

	manager, err := NewManager(logger, fakeKubeClient, t.TempDir())
	require.NoError(t, err)
	defer manager.Stop()
	manager.initDRAPluginManager(tCtx, getFakeNode, time.Second)

	secondClaimName := fmt.Sprintf("%s-second", claimName)

	// 构造两个 Pod：第二个 Pod 复用一个已有 claim，并新增一个 claim
	firstPod := genTestPodWithClaims(claimName)
	secondPod := genTestPodWithClaims(claimName, secondClaimName)

	firstClaim := genTestClaim(claimName, driverName, deviceName, string(firstPod.ObjectMeta.UID))
	secondClaim := genTestClaim(secondClaimName, driverName, deviceName, string(secondPod.ObjectMeta.UID))

	// 将第一个 claim 同时保留给第一个和第二个 Pod
	firstClaim.Status.ReservedFor = append(
		firstClaim.Status.ReservedFor,
		resourceapi.ResourceClaimConsumerReference{UID: secondPod.ObjectMeta.UID},
	)

	_, err = fakeKubeClient.ResourceV1().ResourceClaims(namespace).Create(tCtx, firstClaim, metav1.CreateOptions{})
	require.NoError(t, err)

	_, err = fakeKubeClient.ResourceV1().ResourceClaims(namespace).Create(tCtx, secondClaim, metav1.CreateOptions{})
	require.NoError(t, err)

	respFirst := genPrepareResourcesResponse(firstClaim.UID)
	draServerInfo, err := setupFakeDRADriverGRPCServer(tCtx, false, nil, respFirst, nil, nil)
	require.NoError(t, err)
	defer draServerInfo.teardownFn()

	plg := manager.GetWatcherHandler()
	require.NoError(t, plg.RegisterPlugin(tCtx, driverName, draServerInfo.socketName, []string{drapb.DRAPluginService}, nil))

	err = manager.PrepareResources(tCtx, firstPod)
	require.NoError(t, err)

	assert.Equal(t, uint32(1),
		draServerInfo.server.prepareResourceCalls.Load(),
		"first pod should trigger one prepare call",
	)

	respSecond := genPrepareResourcesResponse(secondClaim.UID)
	draServerInfo.server.prepareResourcesResponse = respSecond

	err = manager.PrepareResources(tCtx, secondPod)
	require.NoError(t, err)

	// 第二个 Pod 只应对新 claim 触发一次 prepare 调用（加上之前的一次共 2 次）
	assert.Equal(t, uint32(2),
		draServerInfo.server.prepareResourceCalls.Load(),
		"second pod should trigger one prepare call for the new claim",
	)

	for _, claimName := range []string{firstClaim.Name, secondClaim.Name} {
		claimInfo, exists := manager.cache.get(claimName, namespace)
		require.True(t, exists, "claim %s should exist in cache", claimName)
		assert.True(t, claimInfo.prepared, "claim %s should be marked as prepared", claimName)
	}
}

// TestPrepareResourcesWithUnpreparingClaim 是针对如下竞态的回归测试：
// reconcileLoop 发起（或其他并发路径）的 unprepareResources 已决定调用
// NodeUnprepareResources 并在 RPC 期间释放了缓存锁，此时另一个 Pod 的
// prepareResources 否则会挂到这条仍在缓存中、正在被拆除的 claim 上。
// PrepareResources 必须拒绝操作标记为 unpreparing 的 claim。
func TestPrepareResourcesWithUnpreparingClaim(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	fakeKubeClient := fake.NewClientset()

	manager, err := NewManager(logger, fakeKubeClient, t.TempDir())
	require.NoError(t, err)
	manager.initDRAPluginManager(tCtx, getFakeNode, time.Second)

	pod := genTestPod()
	claim := genTestClaim(claimName, driverName, deviceName, podUID)
	_, err = fakeKubeClient.ResourceV1().ResourceClaims(namespace).Create(tCtx, claim, metav1.CreateOptions{})
	require.NoError(t, err)

	draServerInfo, err := setupFakeDRADriverGRPCServer(tCtx, false, nil, genPrepareResourcesResponse(claim.UID), nil, nil)
	require.NoError(t, err)
	defer draServerInfo.teardownFn()

	plg := manager.GetWatcherHandler()
	require.NoError(t, plg.RegisterPlugin(tCtx, driverName, draServerInfo.socketName, []string{drapb.DRAPluginService}, nil))

	// 预置一条 claimInfo：它已被另一个 Pod 完全准备，且标记为 unpreparing
	// （模拟 unprepareResources 已决定调用 NodeUnprepareResources、
	// 但尚未从缓存中删除的状态）。
	// 由于仍保留对另一个 Pod 的引用，还可以验证该引用未被误删。
	existing := genTestClaimInfo(claimUID, []string{"another-pod-uid"}, true, nil)
	existing.setUnpreparing()
	manager.cache.add(existing)

	err = manager.PrepareResources(tCtx, pod)
	require.Error(t, err, "PrepareResources must fail while the claim is being unprepared")

	assert.Equal(t, uint32(0), draServerInfo.server.prepareResourceCalls.Load(),
		"driver must not be called while the claim is being unprepared")

	// 预置的 claimInfo 必须保持不变：不新增 Pod 引用、仍处于 unpreparing、
	// 仍为 prepared，且另一个 Pod 的引用完好
	cached, ok := manager.cache.get(claimName, namespace)
	require.True(t, ok, "claim info must still be in the cache")
	assert.False(t, cached.PodUIDs.Has(string(pod.UID)),
		"failing PrepareResources must not have added this pod to PodUIDs")
	assert.True(t, cached.PodUIDs.Has("another-pod-uid"),
		"other pod's reference must survive")
	assert.True(t, cached.isUnpreparing(),
		"unpreparing flag must still be set")
}

// TestUnprepareResources 验证 UnprepareResources 的释放逻辑与指标：
// 驱动未注册、claim 仍被其他 Pod 引用而跳过、超时、驱动返回空响应、
// 扩展资源、缓存缺失 claim 名、Pod 引用保护等场景
func TestUnprepareResources(t *testing.T) {
	fakeKubeClient := fake.NewSimpleClientset()
	for _, test := range []struct {
		description         string
		driverName          string
		pod                 *v1.Pod
		claimInfo           *ClaimInfo
		claim               *resourceapi.ResourceClaim
		resp                *drapb.NodeUnprepareResourcesResponse
		wantTimeout         bool
		wantResourceSkipped bool

		expectedUnprepareCalls uint32
		expectedErrMsg         string
		expectedMetric         string
	}{
		{
			description: "unknown driver",
			driverName:  driverName,
			pod:         genTestPod(),
			claimInfo: &ClaimInfo{
				ClaimInfoState: state.ClaimInfoState{
					ClaimUID:  claimUID,
					ClaimName: claimName,
					Namespace: namespace,
					PodUIDs:   sets.New[string](string(podUID)),
					DriverState: map[string]state.DriverState{
						"unknown-driver": {
							Devices: []state.Device{{
								PoolName:     poolName,
								DeviceName:   deviceName,
								RequestNames: []string{requestName},
								CDIDeviceIDs: []string{"random-cdi-id"},
							}},
						},
					},
				},
				prepared: true,
			},
			expectedErrMsg:         "unprepare dynamic resources: DRA driver unknown-driver is not registered",
			expectedUnprepareCalls: 0,
			expectedMetric: `# HELP dra_operations_duration_seconds [ALPHA] Latency histogram in seconds for the duration of handling all ResourceClaims referenced by a pod when the pod starts or stops. Identified by the name of the operation (PrepareResources or UnprepareResources) and separated by the success of the operation. The number of failed operations is provided through the histogram's overall count.
# TYPE dra_operations_duration_seconds histogram
dra_operations_duration_seconds_bucket{is_error="true",operation_name="UnprepareResources",le="+Inf"} 1
dra_operations_duration_seconds_sum{is_error="true",operation_name="UnprepareResources"} 0
dra_operations_duration_seconds_count{is_error="true",operation_name="UnprepareResources"} 1
`,
		},
		{
			description:         "resource claim referenced by other pod(s)",
			driverName:          driverName,
			pod:                 genTestPod(),
			claimInfo:           genTestClaimInfo(claimUID, []string{podUID, "another-pod-uid"}, true, nil),
			wantResourceSkipped: true,
			expectedMetric: `# HELP dra_operations_duration_seconds [ALPHA] Latency histogram in seconds for the duration of handling all ResourceClaims referenced by a pod when the pod starts or stops. Identified by the name of the operation (PrepareResources or UnprepareResources) and separated by the success of the operation. The number of failed operations is provided through the histogram's overall count.
# TYPE dra_operations_duration_seconds histogram
dra_operations_duration_seconds_bucket{is_error="false",operation_name="UnprepareResources",le="+Inf"} 1
dra_operations_duration_seconds_sum{is_error="false",operation_name="UnprepareResources"} 0
dra_operations_duration_seconds_count{is_error="false",operation_name="UnprepareResources"} 1
`,
		},
		{
			description:            "should timeout",
			driverName:             driverName,
			pod:                    genTestPod(),
			claimInfo:              genTestClaimInfo(claimUID, []string{podUID}, true, nil),
			wantTimeout:            true,
			expectedUnprepareCalls: 1,
			expectedErrMsg:         "NodeUnprepareResources: rpc error: code = DeadlineExceeded",
			expectedMetric: `# HELP dra_grpc_operations_duration_seconds [ALPHA] Duration in seconds of the DRA gRPC operations
# TYPE dra_grpc_operations_duration_seconds histogram
dra_grpc_operations_duration_seconds_bucket{driver_name="test-driver",grpc_status_code="DeadlineExceeded",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodeUnprepareResources",le="+Inf"} 1
dra_grpc_operations_duration_seconds_sum{driver_name="test-driver",grpc_status_code="DeadlineExceeded",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodeUnprepareResources"} 0
dra_grpc_operations_duration_seconds_count{driver_name="test-driver",grpc_status_code="DeadlineExceeded",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodeUnprepareResources"} 1
# HELP dra_operations_duration_seconds [ALPHA] Latency histogram in seconds for the duration of handling all ResourceClaims referenced by a pod when the pod starts or stops. Identified by the name of the operation (PrepareResources or UnprepareResources) and separated by the success of the operation. The number of failed operations is provided through the histogram's overall count.
# TYPE dra_operations_duration_seconds histogram
dra_operations_duration_seconds_bucket{is_error="true",operation_name="UnprepareResources",le="+Inf"} 1
dra_operations_duration_seconds_sum{is_error="true",operation_name="UnprepareResources"} 0
dra_operations_duration_seconds_count{is_error="true",operation_name="UnprepareResources"} 1
`,
		},
		{
			description:            "should fail when driver returns empty response",
			driverName:             driverName,
			pod:                    genTestPod(),
			claimInfo:              genTestClaimInfo(claimUID, []string{podUID}, true, nil),
			resp:                   &drapb.NodeUnprepareResourcesResponse{Claims: map[string]*drapb.NodeUnprepareResourceResponse{}},
			expectedUnprepareCalls: 1,
			expectedErrMsg:         "NodeUnprepareResources skipped 1 ResourceClaims",
			expectedMetric: `# HELP dra_grpc_operations_duration_seconds [ALPHA] Duration in seconds of the DRA gRPC operations
# TYPE dra_grpc_operations_duration_seconds histogram
dra_grpc_operations_duration_seconds_bucket{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodeUnprepareResources",le="+Inf"} 1
dra_grpc_operations_duration_seconds_sum{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodeUnprepareResources"} 0
dra_grpc_operations_duration_seconds_count{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodeUnprepareResources"} 1
# HELP dra_operations_duration_seconds [ALPHA] Latency histogram in seconds for the duration of handling all ResourceClaims referenced by a pod when the pod starts or stops. Identified by the name of the operation (PrepareResources or UnprepareResources) and separated by the success of the operation. The number of failed operations is provided through the histogram's overall count.
# TYPE dra_operations_duration_seconds histogram
dra_operations_duration_seconds_bucket{is_error="true",operation_name="UnprepareResources",le="+Inf"} 1
dra_operations_duration_seconds_sum{is_error="true",operation_name="UnprepareResources"} 0
dra_operations_duration_seconds_count{is_error="true",operation_name="UnprepareResources"} 1
`,
		},
		{
			description:            "should unprepare already prepared extended resource backed by DRA",
			driverName:             driverName,
			pod:                    genTestPodWithExtendedResource(),
			claim:                  genTestClaimWithExtendedResource(claimName, driverName, deviceName, podUID),
			claimInfo:              genTestClaimInfoWithExtendedResource([]string{podUID}, true),
			expectedUnprepareCalls: 1,
			expectedMetric: `# HELP dra_grpc_operations_duration_seconds [ALPHA] Duration in seconds of the DRA gRPC operations
# TYPE dra_grpc_operations_duration_seconds histogram
dra_grpc_operations_duration_seconds_bucket{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodeUnprepareResources",le="+Inf"} 1
dra_grpc_operations_duration_seconds_sum{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodeUnprepareResources"} 0
dra_grpc_operations_duration_seconds_count{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodeUnprepareResources"} 1
# HELP dra_operations_duration_seconds [ALPHA] Latency histogram in seconds for the duration of handling all ResourceClaims referenced by a pod when the pod starts or stops. Identified by the name of the operation (PrepareResources or UnprepareResources) and separated by the success of the operation. The number of failed operations is provided through the histogram's overall count.
# TYPE dra_operations_duration_seconds histogram
dra_operations_duration_seconds_bucket{is_error="false",operation_name="UnprepareResources",le="+Inf"} 1
dra_operations_duration_seconds_sum{is_error="false",operation_name="UnprepareResources"} 0
dra_operations_duration_seconds_count{is_error="false",operation_name="UnprepareResources"} 1
`,
		},
		{
			description:            "should unprepare already prepared resource",
			driverName:             driverName,
			pod:                    genTestPod(),
			claim:                  genTestClaim(claimName, driverName, deviceName, podUID),
			claimInfo:              genTestClaimInfo(claimUID, []string{podUID}, true, nil),
			expectedUnprepareCalls: 1,
			expectedMetric: `# HELP dra_grpc_operations_duration_seconds [ALPHA] Duration in seconds of the DRA gRPC operations
# TYPE dra_grpc_operations_duration_seconds histogram
dra_grpc_operations_duration_seconds_bucket{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodeUnprepareResources",le="+Inf"} 1
dra_grpc_operations_duration_seconds_sum{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodeUnprepareResources"} 0
dra_grpc_operations_duration_seconds_count{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodeUnprepareResources"} 1
# HELP dra_operations_duration_seconds [ALPHA] Latency histogram in seconds for the duration of handling all ResourceClaims referenced by a pod when the pod starts or stops. Identified by the name of the operation (PrepareResources or UnprepareResources) and separated by the success of the operation. The number of failed operations is provided through the histogram's overall count.
# TYPE dra_operations_duration_seconds histogram
dra_operations_duration_seconds_bucket{is_error="false",operation_name="UnprepareResources",le="+Inf"} 1
dra_operations_duration_seconds_sum{is_error="false",operation_name="UnprepareResources"} 0
dra_operations_duration_seconds_count{is_error="false",operation_name="UnprepareResources"} 1
`,
		},
		{
			// kubernetes/kubernetes#139772 的回归测试：Pod 通过 template 引用 claim，
			// 但 Status.ResourceClaimStatuses 为空（被外部组件覆盖）。
			// resourceclaim.Name 返回 ErrClaimNotFound，因此 claim 名必须从缓存获取。
			// 此前这会让 Pod 卡在 terminating 状态
			description:            "should unprepare using cache when ResourceClaimStatuses is missing",
			driverName:             driverName,
			pod:                    genTestPodWithClaimTemplateNoStatus(),
			claim:                  genTestClaim(claimName, driverName, deviceName, podUID),
			claimInfo:              genTestClaimInfo(claimUID, []string{podUID}, true, nil),
			expectedUnprepareCalls: 1,
			expectedMetric: `# HELP dra_grpc_operations_duration_seconds [ALPHA] Duration in seconds of the DRA gRPC operations
# TYPE dra_grpc_operations_duration_seconds histogram
dra_grpc_operations_duration_seconds_bucket{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodeUnprepareResources",le="+Inf"} 1
dra_grpc_operations_duration_seconds_sum{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodeUnprepareResources"} 0
dra_grpc_operations_duration_seconds_count{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodeUnprepareResources"} 1
# HELP dra_operations_duration_seconds [ALPHA] Latency histogram in seconds for the duration of handling all ResourceClaims referenced by a pod when the pod starts or stops. Identified by the name of the operation (PrepareResources or UnprepareResources) and separated by the success of the operation. The number of failed operations is provided through the histogram's overall count.
# TYPE dra_operations_duration_seconds histogram
dra_operations_duration_seconds_bucket{is_error="false",operation_name="UnprepareResources",le="+Inf"} 1
dra_operations_duration_seconds_sum{is_error="false",operation_name="UnprepareResources"} 0
dra_operations_duration_seconds_count{is_error="false",operation_name="UnprepareResources"} 1
`,
		},
		{
			// unprepareResources 中 Pod 引用保护的回归测试：
			// 若 claimInfo 未引用该 Pod，则不得调用 NodeUnprepareResources。
			description:            "should skip unprepare for pod that never prepared the claim",
			driverName:             driverName,
			pod:                    genTestPod(), // Pod UID 即 podUID
			claim:                  genTestClaim(claimName, driverName, deviceName, podUID),
			claimInfo:              genTestClaimInfo(claimUID, []string{"another-pod-uid"}, true, nil),
			wantResourceSkipped:    true,
			expectedUnprepareCalls: 0,
			expectedMetric: `# HELP dra_operations_duration_seconds [ALPHA] Latency histogram in seconds for the duration of handling all ResourceClaims referenced by a pod when the pod starts or stops. Identified by the name of the operation (PrepareResources or UnprepareResources) and separated by the success of the operation. The number of failed operations is provided through the histogram's overall count.
# TYPE dra_operations_duration_seconds histogram
dra_operations_duration_seconds_bucket{is_error="false",operation_name="UnprepareResources",le="+Inf"} 1
dra_operations_duration_seconds_sum{is_error="false",operation_name="UnprepareResources"} 0
dra_operations_duration_seconds_count{is_error="false",operation_name="UnprepareResources"} 1
`,
		},
		{
			description:            "should unprepare resource when driver returns nil value",
			driverName:             driverName,
			pod:                    genTestPod(),
			claimInfo:              genTestClaimInfo(claimUID, []string{podUID}, true, nil),
			resp:                   &drapb.NodeUnprepareResourcesResponse{Claims: map[string]*drapb.NodeUnprepareResourceResponse{string(claimUID): nil}},
			expectedUnprepareCalls: 1,
			expectedMetric: `# HELP dra_grpc_operations_duration_seconds [ALPHA] Duration in seconds of the DRA gRPC operations
# TYPE dra_grpc_operations_duration_seconds histogram
dra_grpc_operations_duration_seconds_bucket{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodeUnprepareResources",le="+Inf"} 1
dra_grpc_operations_duration_seconds_sum{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodeUnprepareResources"} 0
dra_grpc_operations_duration_seconds_count{driver_name="test-driver",grpc_status_code="OK",method_name="/k8s.io.kubelet.pkg.apis.dra.v1.DRAPlugin/NodeUnprepareResources"} 1
# HELP dra_operations_duration_seconds [ALPHA] Latency histogram in seconds for the duration of handling all ResourceClaims referenced by a pod when the pod starts or stops. Identified by the name of the operation (PrepareResources or UnprepareResources) and separated by the success of the operation. The number of failed operations is provided through the histogram's overall count.
# TYPE dra_operations_duration_seconds histogram
dra_operations_duration_seconds_bucket{is_error="false",operation_name="UnprepareResources",le="+Inf"} 1
dra_operations_duration_seconds_sum{is_error="false",operation_name="UnprepareResources"} 0
dra_operations_duration_seconds_count{is_error="false",operation_name="UnprepareResources"} 1
`,
		},
	} {
		t.Run(test.description, func(t *testing.T) {
			tCtx := ktesting.Init(t)

			kubeletmetrics.Register()
			kubeletmetrics.DRAOperationsDuration.Reset()
			kubeletmetrics.DRAGRPCOperationsDuration.Reset()
			defer func() {
				require.NoError(t,
					metricstestutil.GatherAndCompare(gatherWithoutDurations,
						strings.NewReader(test.expectedMetric),
						kubeletmetrics.DRAOperationsDuration.FQName(),
						kubeletmetrics.DRAGRPCOperationsDuration.FQName(),
					),
				)
			}()

			var pluginClientTimeout *time.Duration
			if test.wantTimeout {
				timeout := time.Millisecond * 20
				pluginClientTimeout = &timeout
			}

			draServerInfo, err := setupFakeDRADriverGRPCServer(tCtx, test.wantTimeout, pluginClientTimeout, nil, test.resp, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer draServerInfo.teardownFn()

			manager, err := NewManager(tCtx.Logger(), fakeKubeClient, t.TempDir())
			require.NoError(t, err, "create DRA manager")
			defer manager.Stop()
			manager.initDRAPluginManager(tCtx, getFakeNode, time.Second /* 测试中用极短的清理延迟 */)

			plg := manager.GetWatcherHandler()
			if err := plg.RegisterPlugin(tCtx, test.driverName, draServerInfo.socketName, []string{drapb.DRAPluginService}, pluginClientTimeout); err != nil {
				t.Fatalf("failed to register plugin %s, err: %v", test.driverName, err)
			}

			if test.claimInfo != nil {
				manager.cache.add(test.claimInfo)
			}

			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DRAExtendedResource, true)
			err = manager.UnprepareResources(tCtx, test.pod)

			assert.Equal(t, test.expectedUnprepareCalls, draServerInfo.server.unprepareResourceCalls.Load())

			if test.expectedErrMsg != "" {
				assert.Error(t, err)
				if err != nil {
					assert.ErrorContains(t, err, test.expectedErrMsg)
				}
				return // PrepareResources returned an error so stopping the test case here
			}

			require.NoError(t, err)

			if test.wantResourceSkipped {
				if test.claimInfo != nil && len(test.claimInfo.PodUIDs) > 0 {
					cachedClaim, exists := manager.cache.get(test.claimInfo.ClaimName, test.claimInfo.Namespace)
					require.True(t, exists, "ClaimInfo should still exist if skipped")
					assert.False(t, cachedClaim.PodUIDs.Has(string(test.pod.UID)), "Pod UID should not remain in skipped claim")
					// 属于其他 Pod 的 UID 必须在这次空操作 unprepare 后保留下来，
					// 以防错误地拆除其他 Pod 拥有的 claim。
					for uid := range test.claimInfo.PodUIDs {
						if uid == string(test.pod.UID) {
							continue
						}
						assert.True(t, cachedClaim.PodUIDs.Has(uid), "other pod UID %q must still reference the claim", uid)
					}
				}
				return // resource skipped so no need to continue
			}

			// 仅在 unprepare 成功时才应清理缓存
			var podClaimName *string
			if len(test.pod.Spec.ResourceClaims) > 0 {
				// claim 名可能无法从 Pod 推导出来（例如 #139772 中
				// Status.ResourceClaimStatuses 缺失的情况），这不影响后续校验：
				// 下面会回退到已知的 claimName 常量。
				podClaimName, _, _ = resourceclaim.Name(test.pod, &test.pod.Spec.ResourceClaims[0])
			}
			claimName := claimName
			if podClaimName == nil {
				podClaimName = &claimName
			}
			assert.False(t, manager.cache.contains(*podClaimName, test.pod.Namespace), "claimInfo should not be found after successful unprepare")
		})
	}
}

// TestPodMightNeedToUnprepareResources 验证可通过 claim info 缓存判断某个 Pod 是否需要执行 unprepare
func TestPodMightNeedToUnprepareResources(t *testing.T) {
	tCtx := ktesting.Init(t)
	fakeKubeClient := fake.NewSimpleClientset()
	manager, err := NewManager(tCtx.Logger(), fakeKubeClient, t.TempDir())
	require.NoError(t, err, "create DRA manager")
	defer manager.Stop()

	claimInfo := &ClaimInfo{
		ClaimInfoState: state.ClaimInfoState{PodUIDs: sets.New(podUID), ClaimName: claimName, Namespace: namespace},
	}
	manager.cache.add(claimInfo)
	if !manager.cache.contains(claimName, namespace) {
		t.Fatalf("failed to get claimInfo from cache for claim name %s, namespace %s: err:%v", claimName, namespace, err)
	}
	claimInfo.addPodReference(types.UID(podUID))
	needsUnprepare := manager.PodMightNeedToUnprepareResources(types.UID(podUID))
	assert.True(t, needsUnprepare)
}

// TestGetContainerClaimInfos 验证 GetContainerClaimInfos 能为容器找到对应的 claim info，
// 覆盖普通 claim、扩展资源 claim、缓存缺失以及既无 ResourceClaimName 也无模板名等错误场景
func TestGetContainerClaimInfos(t *testing.T) {
	for _, test := range []struct {
		description string
		pod         *v1.Pod
		claimInfo   *ClaimInfo

		expectedClaimName string
		expectedErrMsg    string
	}{
		{
			description:       "should get claim info",
			expectedClaimName: claimName,
			pod:               genTestPod(),
			claimInfo:         genTestClaimInfo(claimUID, []string{podUID}, false, nil),
		},
		{
			description:       "should get extended resource claim info",
			expectedClaimName: claimName,
			pod:               genTestPodWithExtendedResource(),
			claimInfo:         genTestClaimInfoWithExtendedResource([]string{podUID}, false),
		},
		{
			description:    "should fail when claim info not found",
			pod:            genTestPod(),
			claimInfo:      &ClaimInfo{},
			expectedErrMsg: "unable to get information for ResourceClaim ",
		},
		{
			description: "should fail when none of the supported fields are set",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      podName,
					Namespace: namespace,
					UID:       podUID,
				},
				Spec: v1.PodSpec{
					ResourceClaims: []v1.PodResourceClaim{
						{
							Name: claimName,
							// 缺少 ResourceClaimName 或 ResourceClaimTemplateName
						},
					},
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Claims: []v1.ResourceClaim{
									{
										Name: claimName,
									},
								},
							},
						},
					},
				},
			},
			claimInfo:      genTestClaimInfo(claimUID, []string{podUID}, false, nil),
			expectedErrMsg: "none of the supported fields are set",
		},
		{
			description:    "should fail when claim info is not cached",
			pod:            genTestPod(),
			expectedErrMsg: "unable to get information for ResourceClaim ",
		},
	} {
		t.Run(test.description, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			manager, err := NewManager(tCtx.Logger(), nil, t.TempDir())
			require.NoError(t, err, "create DRA manager")
			defer manager.Stop()

			if test.claimInfo != nil {
				manager.cache.add(test.claimInfo)
			}

			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DRAExtendedResource, true)
			claimInfos, err := manager.GetContainerClaimInfos(test.pod, &test.pod.Spec.Containers[0])

			if test.expectedErrMsg != "" {
				assert.Error(t, err)
				if err != nil {
					assert.ErrorContains(t, err, test.expectedErrMsg)
				}
				return
			}

			require.NoError(t, err)
			assert.Len(t, claimInfos, 1)
			assert.Equal(t, test.expectedClaimName, claimInfos[0].ClaimInfoState.ClaimName)
		})
	}
}

// TestParallelPrepareUnprepareResources 并行调用 PrepareResources 与 UnprepareResources，
// 以检测可能的数据竞争
func TestParallelPrepareUnprepareResources(t *testing.T) {
	tCtx := ktesting.Init(t)

	// 启动并注册伪造的 DRA 驱动
	draServerInfo, err := setupFakeDRADriverGRPCServer(tCtx, false, nil, nil, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer draServerInfo.teardownFn()

	// 创建伪造的 Kube 客户端与 DRA manager
	fakeKubeClient := fake.NewSimpleClientset()
	manager, err := NewManager(tCtx.Logger(), fakeKubeClient, t.TempDir())
	require.NoError(t, err, "create DRA manager")
	defer manager.Stop()
	manager.initDRAPluginManager(tCtx, getFakeNode, time.Second /* 测试中用极短的清理延迟 */)

	plg := manager.GetWatcherHandler()
	if err := plg.RegisterPlugin(tCtx, driverName, draServerInfo.socketName, []string{drapb.DRAPluginService}, nil); err != nil {
		t.Fatalf("failed to register plugin %s, err: %v", driverName, err)
	}

	// 并行调用 PrepareResources
	var wgSync, wgStart sync.WaitGroup // groups to sync goroutines
	numGoroutines := 30
	wgSync.Add(numGoroutines)
	wgStart.Add(1)
	for i := 0; i < numGoroutines; i++ {
		go func(t *testing.T, goRoutineNum int) {
			defer wgSync.Done()
			wgStart.Wait() // Wait to start all goroutines at the same time

			var err error
			claimName := fmt.Sprintf("test-pod-claim-%d", goRoutineNum)
			podUID := types.UID(fmt.Sprintf("test-pod-uid-%d", goRoutineNum))
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("test-pod-%d", goRoutineNum),
					Namespace: namespace,
					UID:       podUID,
				},
				Spec: v1.PodSpec{
					ResourceClaims: []v1.PodResourceClaim{
						{
							Name: claimName,
							ResourceClaimName: func() *string {
								s := claimName
								return &s
							}(),
						},
					},
					Containers: []v1.Container{
						{
							Name: fmt.Sprintf("container-%d", goRoutineNum),
							Resources: v1.ResourceRequirements{
								Claims: []v1.ResourceClaim{
									{
										Name: claimName,
									},
								},
							},
						},
					},
				},
			}
			claim := genTestClaim(claimName, driverName, deviceName, string(podUID))

			if _, err = fakeKubeClient.ResourceV1().ResourceClaims(pod.Namespace).Create(tCtx, claim, metav1.CreateOptions{}); err != nil {
				t.Errorf("failed to create ResourceClaim %s: %+v", claim.Name, err)
				return
			}

			defer func() {
				_ = fakeKubeClient.ResourceV1().ResourceClaims(pod.Namespace).Delete(tCtx, claim.Name, metav1.DeleteOptions{})
			}()

			if err = manager.PrepareResources(tCtx, pod); err != nil {
				t.Errorf("GoRoutine %d: pod: %s: PrepareResources failed: %+v", goRoutineNum, pod.Name, err)
				return
			}

			if err = manager.UnprepareResources(tCtx, pod); err != nil {
				t.Errorf("GoRoutine %d: pod: %s: UnprepareResources failed: %+v", goRoutineNum, pod.Name, err)
				return
			}

		}(t, i)
	}
	wgStart.Done() // Start executing goroutines
	wgSync.Wait()  // Wait for all goroutines to finish
}

// TestHandleWatchResourcesStream 验证 manager 处理 DRA 插件 WatchResources 流
// 中健康更新的能力：校验内部健康缓存是否被正确更新、受影响的 Pod 是否被识别，
// 以及更新通知是否通过 manager 的 update 通道发出。覆盖健康变化、流错误与上下文取消等场景。
func TestHandleWatchResourcesStream(t *testing.T) {
	tCtx := ktesting.Init(t)
	logger := tCtx.Logger()
	overallTestCtx, overallTestCancel := context.WithCancel(tCtx)
	defer overallTestCancel()

	// 为每个子测试创建并初始化一个新的 manager 的辅助函数
	setupNewManagerAndRunStreamTest := func(
		st *testing.T,
		testSpecificCtx context.Context,
		initialClaimInfos ...*ClaimInfo,
	) (
		managerInstance *Manager,
		runTestStreamFunc func(context.Context, chan struct {
			Resp *drahealthv1.NodeWatchResourcesResponse
			Err  error
		}) (<-chan resourceupdates.Update, chan struct{}, chan error),
	) {
		tCtx := ktesting.Init(t)
		// 每个子测试使用全新的 manager
		manager, err := NewManager(tCtx.Logger(), nil, st.TempDir())
		require.NoError(st, err)
		defer manager.Stop()

		for _, ci := range initialClaimInfos {
			manager.cache.add(ci)
		}

		managerInstance = manager

		runTestStreamFunc = func(
			streamCtx context.Context,
			responses chan struct {
				Resp *drahealthv1.NodeWatchResourcesResponse
				Err  error
			},
		) (<-chan resourceupdates.Update, chan struct{}, chan error) {
			mockStream := &mockWatchResourcesClient{
				RecvChan: responses,
				Ctx:      streamCtx,
			}
			done := make(chan struct{})
			errChan := make(chan error, 1)
			go func() {
				defer close(done)
				// 使用带子测试名的 logger，便于定位日志
				logger := klog.FromContext(streamCtx).WithName(st.Name())
				hdlCtx := klog.NewContext(streamCtx, logger)

				err := managerInstance.HandleWatchResourcesStream(hdlCtx, mockStream, driverName)
				if err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) {
						logger.V(4).Info("HandleWatchResourcesStream (test goroutine) exited as expected", "error", err)
					} else {
						// 这是应用/流错误，而非正常退出；
						// 对应的 "StreamError" 子测试会断言这个具体错误
						logger.V(2).Info("HandleWatchResourcesStream (test goroutine) exited with application/stream error", "error", err)
					}
				} else {
					logger.V(4).Info("HandleWatchResourcesStream (test goroutine) exited cleanly (nil error, likely from EOF)")
				}
				errChan <- err
				close(errChan)
			}()
			return managerInstance.update, done, errChan
		}
		return managerInstance, runTestStreamFunc
	}

	// 用例 1：已分配设备的健康状态变化
	t.Run("HealthChangeForAllocatedDevice", func(t *testing.T) {
		stCtx, stCancel := context.WithCancel(overallTestCtx)
		defer stCancel()

		// 准备：创建一个缓存中已有相关 claim 的 manager
		initialClaim := genTestClaimInfo(claimUID, []string{string(podUID)}, true, nil)
		manager, runStreamTest := setupNewManagerAndRunStreamTest(t, stCtx, initialClaim)

		t.Log("HealthChangeForAllocatedDevice: Test Case Started")

		responses := make(chan struct {
			Resp *drahealthv1.NodeWatchResourcesResponse
			Err  error
		}, 1)
		updateChan, done, streamErrChan := runStreamTest(stCtx, responses)

		// 发送健康更新消息
		unhealthyDeviceMsg := &drahealthv1.DeviceHealth{
			Device: &drahealthv1.DeviceIdentifier{
				PoolName:   poolName,
				DeviceName: deviceName,
			},
			Health:          drahealthv1.HealthStatus_UNHEALTHY,
			LastUpdatedTime: time.Now().Unix(),
		}
		t.Logf("HealthChangeForAllocatedDevice: Sending health update: %+v", unhealthyDeviceMsg)
		responses <- struct {
			Resp *drahealthv1.NodeWatchResourcesResponse
			Err  error
		}{
			Resp: &drahealthv1.NodeWatchResourcesResponse{Devices: []*drahealthv1.DeviceHealth{unhealthyDeviceMsg}},
		}

		t.Log("HealthChangeForAllocatedDevice: Waiting for update on manager channel")
		select {
		case upd := <-updateChan:
			t.Logf("HealthChangeForAllocatedDevice: Received update: %+v", upd)
			assert.ElementsMatch(t, []string{string(podUID)}, upd.PodUIDs, "Expected pod UID in update")
		case <-time.After(2 * time.Second):
			t.Fatal("HealthChangeForAllocatedDevice: Timeout waiting for pod update on manager.update channel")
		}

		// 检查缓存状态
		cachedHealth := manager.healthInfoCache.getHealthInfo(driverName, poolName, deviceName).Health
		assert.Equal(t, state.DeviceHealthStatus("Unhealthy"), cachedHealth, "Cache update check failed")

		t.Log("HealthChangeForAllocatedDevice: Closing responses channel to signal EOF")
		close(responses)

		t.Log("HealthChangeForAllocatedDevice: Waiting on done channel")
		var finalErr error
		select {
		case <-done:
			finalErr = <-streamErrChan
			t.Log("HealthChangeForAllocatedDevice: done channel closed, stream goroutine finished.")
		case <-time.After(1 * time.Second):
			t.Fatal("HealthChangeForAllocatedDevice: Timed out waiting for HandleWatchResourcesStream to finish after EOF signal")
		}
		// 预期返回 nil（若 EOF 时返回 nil）或 io.EOF
		assert.True(t, finalErr == nil || errors.Is(finalErr, io.EOF), "Expected nil or io.EOF, got %v", finalErr)
	})

	// 用例 2：未分配设备的健康状态变化
	t.Run("NonAllocatedDeviceChange", func(t *testing.T) {
		stCtx, stCancel := context.WithCancel(overallTestCtx)
		defer stCancel()

		// 准备：manager 中没有相关 claim，或 claim 未使用 "other-device"
		manager, runStreamTest := setupNewManagerAndRunStreamTest(t, stCtx)

		t.Log("NonAllocatedDeviceChange: Test Case Started")
		responses := make(chan struct {
			Resp *drahealthv1.NodeWatchResourcesResponse
			Err  error
		}, 1)
		updateChan, done, streamErrChan := runStreamTest(stCtx, responses)

		otherDeviceMsg := &drahealthv1.DeviceHealth{
			Device: &drahealthv1.DeviceIdentifier{
				PoolName:   poolName,
				DeviceName: "other-device",
			},
			Health:          drahealthv1.HealthStatus_UNHEALTHY,
			LastUpdatedTime: time.Now().Unix(),
		}
		responses <- struct {
			Resp *drahealthv1.NodeWatchResourcesResponse
			Err  error
		}{
			Resp: &drahealthv1.NodeWatchResourcesResponse{Devices: []*drahealthv1.DeviceHealth{otherDeviceMsg}},
		}

		select {
		case upd := <-updateChan:
			t.Fatalf("NonAllocatedDeviceChange: Unexpected update on manager.update channel: %+v", upd)
			// 该设备不应触发 manager.update，验证正确
		case <-time.After(200 * time.Millisecond):
			t.Log("NonAllocatedDeviceChange: Correctly received no update on manager channel.")
		}

		// 检查 "other-device" 的健康缓存
		cachedHealthOther := manager.healthInfoCache.getHealthInfo(driverName, poolName, "other-device").Health
		assert.Equal(t, state.DeviceHealthStatus("Unhealthy"), cachedHealthOther, "Cache update for other-device failed")

		close(responses)
		var finalErr error
		select {
		case <-done:
			finalErr = <-streamErrChan
			t.Log("NonAllocatedDeviceChange: Stream handler goroutine finished.")
		case <-time.After(1 * time.Second):
			t.Fatal("NonAllocatedDeviceChange: Timeout waiting for stream handler to finish after EOF")
		}
		assert.True(t, finalErr == nil || errors.Is(finalErr, io.EOF), "Expected nil or io.EOF, got %v", finalErr)
	})

	// 用例 3：健康状态未实际变化（幂等性）
	t.Run("NoActualStateChange", func(t *testing.T) {
		stCtx, stCancel := context.WithCancel(overallTestCtx)
		defer stCancel()

		// 准备：manager 中存在 claim，且健康缓存中该设备已标记为 Unhealthy
		initialClaim := genTestClaimInfo(claimUID, []string{string(podUID)}, true, nil)
		manager, runStreamTest := setupNewManagerAndRunStreamTest(t, stCtx, initialClaim)

		// 预填充健康缓存
		initialHealth := state.DeviceHealth{PoolName: poolName, DeviceName: deviceName, Health: "Unhealthy", LastUpdated: time.Now().Add(-5 * time.Millisecond), HealthCheckTimeout: DefaultHealthTimeout} // 保证 LastUpdated 略早于当前时间
		_, err := manager.healthInfoCache.updateHealthInfo(logger, driverName, []state.DeviceHealth{initialHealth})
		require.NoError(t, err, "Failed to pre-populate health cache")

		t.Log("NoActualStateChange: Test Case Started")
		responses := make(chan struct {
			Resp *drahealthv1.NodeWatchResourcesResponse
			Err  error
		}, 1)
		updateChan, done, streamErrChan := runStreamTest(stCtx, responses)

		// 再次发送相同的 "Unhealthy" 状态
		unhealthyDeviceMsg := &drahealthv1.DeviceHealth{
			Device: &drahealthv1.DeviceIdentifier{
				PoolName:   poolName,
				DeviceName: deviceName,
			},
			Health:          drahealthv1.HealthStatus_UNHEALTHY,
			LastUpdatedTime: time.Now().Unix(),
		}
		responses <- struct {
			Resp *drahealthv1.NodeWatchResourcesResponse
			Err  error
		}{
			Resp: &drahealthv1.NodeWatchResourcesResponse{Devices: []*drahealthv1.DeviceHealth{unhealthyDeviceMsg}},
		}

		select {
		case upd := <-updateChan:
			t.Fatalf("NoActualStateChange: Unexpected update on manager.update channel: %+v", upd)
		case <-time.After(200 * time.Millisecond):
			t.Log("NoActualStateChange: Correctly received no update on manager channel.")
		}

		close(responses)
		var finalErr error
		select {
		case <-done:
			finalErr = <-streamErrChan
			t.Log("NoActualStateChange: Stream handler goroutine finished.")
		case <-time.After(1 * time.Second):
			t.Fatal("NoActualStateChange: Timeout waiting for stream handler to finish after EOF")
		}
		assert.True(t, finalErr == nil || errors.Is(finalErr, io.EOF), "Expected nil or io.EOF, got %v", finalErr)
	})

	// 用例 4：流错误
	t.Run("StreamError", func(t *testing.T) {
		stCtx, stCancel := context.WithCancel(overallTestCtx)
		defer stCancel()

		// 获取新的 manager 以及该子测试作用域的 runStreamTest 辅助函数
		_, runStreamTest := setupNewManagerAndRunStreamTest(t, stCtx)
		t.Log("StreamError: Test Case Started")

		responses := make(chan struct {
			Resp *drahealthv1.NodeWatchResourcesResponse
			Err  error
		}, 1)
		_, done, streamErrChan := runStreamTest(stCtx, responses)

		expectedStreamErr := errors.New("simulated mock stream error")
		responses <- struct {
			Resp *drahealthv1.NodeWatchResourcesResponse
			Err  error
		}{Err: expectedStreamErr}

		t.Log("StreamError: Waiting on done channel")
		var actualErr error
		select {
		case <-done:
			// 读取 HandleWatchResourcesStream goroutine 传播出来的错误
			actualErr = <-streamErrChan
			t.Logf("StreamError: done channel closed. Stream handler returned: %v", actualErr)
		case <-time.After(2 * time.Second):
			t.Fatal("StreamError: Timeout waiting for stream handler to finish after error signal")
		}

		require.Error(t, actualErr, "HandleWatchResourcesStream should have returned an error")
		assert.ErrorIs(t, actualErr, expectedStreamErr)
	})

	// 用例 5：上下文取消
	t.Run("ContextCanceled", func(t *testing.T) {
		stCtx, stCancel := context.WithCancel(overallTestCtx)
		// 该用例故意不 `defer stCancel()`

		_, runStreamTest := setupNewManagerAndRunStreamTest(t, stCtx)
		t.Log("ContextCanceled: Test Case Started")

		responses := make(chan struct {
			Resp *drahealthv1.NodeWatchResourcesResponse
			Err  error
		})
		_, done, streamErrChan := runStreamTest(stCtx, responses)

		t.Log("ContextCanceled: Intentionally canceling context for stream handler after a short delay.")
		time.Sleep(50 * time.Millisecond)
		stCancel()

		t.Log("ContextCanceled: Waiting on done channel")
		var finalErr error
		select {
		case <-done:
			finalErr = <-streamErrChan
			t.Log("ContextCanceled: done channel closed. Stream handler finished after context cancellation.")
		case <-time.After(1 * time.Second):
			t.Fatal("ContextCanceled: Timeout waiting for stream handler to finish after context cancellation")
		}
		require.Error(t, finalErr)
		assert.True(t, errors.Is(finalErr, context.Canceled) || errors.Is(finalErr, context.DeadlineExceeded))
	})

	// 用例 6：带 ShareID 分配的设备的健康状态变化
	// 验证 ShareID 不会干扰健康状态上报
	t.Run("HealthChangeForDeviceWithShareID", func(t *testing.T) {
		stCtx, stCancel := context.WithCancel(overallTestCtx)
		defer stCancel()

		// 准备：manager 中存在一个设备带 ShareID 的 claim
		testShareUID := types.UID("test-share-uid-for-health")
		initialClaim := genTestClaimInfo(claimUID, []string{string(podUID)}, true, &testShareUID)
		manager, runStreamTest := setupNewManagerAndRunStreamTest(t, stCtx, initialClaim)

		t.Log("HealthChangeForDeviceWithShareID: Test Case Started")

		responses := make(chan struct {
			Resp *drahealthv1.NodeWatchResourcesResponse
			Err  error
		}, 1)
		updateChan, done, streamErrChan := runStreamTest(stCtx, responses)

		// 为该设备发送健康更新（即 claim 中带 ShareID 的同一设备）
		unhealthyDeviceMsg := &drahealthv1.DeviceHealth{
			Device: &drahealthv1.DeviceIdentifier{
				PoolName:   poolName,
				DeviceName: deviceName,
			},
			Health:          drahealthv1.HealthStatus_UNHEALTHY,
			LastUpdatedTime: time.Now().Unix(),
		}
		t.Logf("HealthChangeForDeviceWithShareID: Sending health update: %+v", unhealthyDeviceMsg)
		responses <- struct {
			Resp *drahealthv1.NodeWatchResourcesResponse
			Err  error
		}{
			Resp: &drahealthv1.NodeWatchResourcesResponse{Devices: []*drahealthv1.DeviceHealth{unhealthyDeviceMsg}},
		}

		t.Log("HealthChangeForDeviceWithShareID: Waiting for update on manager channel")
		select {
		case upd := <-updateChan:
			t.Logf("HealthChangeForDeviceWithShareID: Received update: %+v", upd)
			assert.ElementsMatch(t, []string{string(podUID)}, upd.PodUIDs, "Expected pod UID in update for device with ShareID")
		case <-time.After(2 * time.Second):
			t.Fatal("HealthChangeForDeviceWithShareID: Timeout waiting for pod update - ShareID may be interfering with health reporting")
		}

		// 验证健康缓存被正确更新
		cachedHealth := manager.healthInfoCache.getHealthInfo(driverName, poolName, deviceName)
		assert.Equal(t, state.DeviceHealthStatusUnhealthy, cachedHealth.Health, "Health cache should be updated for device with ShareID")

		// 验证 claim 中的 ShareID 仍存在（不应在健康更新过程中丢失）
		claimFromCache, exists := manager.cache.get(claimName, namespace)
		require.True(t, exists, "Claim should still exist in cache")
		devices := claimFromCache.DriverState[driverName].Devices
		require.Len(t, devices, 1, "Claim should have one device")
		assert.NotNil(t, devices[0].ShareID, "ShareID should still be set on the device")
		assert.Equal(t, testShareUID, *devices[0].ShareID, "ShareID value should be preserved")

		t.Log("HealthChangeForDeviceWithShareID: Closing responses channel to signal EOF")
		close(responses)

		t.Log("HealthChangeForDeviceWithShareID: Waiting on done channel")
		var finalErr error
		select {
		case <-done:
			finalErr = <-streamErrChan
			t.Log("HealthChangeForDeviceWithShareID: done channel closed, stream goroutine finished.")
		case <-time.After(1 * time.Second):
			t.Fatal("HealthChangeForDeviceWithShareID: Timed out waiting for HandleWatchResourcesStream to finish")
		}
		assert.True(t, finalErr == nil || errors.Is(finalErr, io.EOF), "Expected nil or io.EOF, got %v", finalErr)
	})

	// 用例 7：健康变化影响通过 ShareID 共享同一设备的多个 Pod
	// 当两个 Pod 共享同一物理设备（由 pool/device 标识、但 ShareID 不同）时，
	// 健康变化应通知两个 Pod，因为它们使用同一底层硬件。
	t.Run("HealthChangeForMultiplePodsWithSharedDevice", func(t *testing.T) {
		stCtx, stCancel := context.WithCancel(overallTestCtx)
		defer stCancel()

		// 准备：来自不同 Pod 的两条 claim，使用同一设备但 ShareID 不同
		pod1UID := types.UID("pod-1-sharing-device")
		pod2UID := types.UID("pod-2-sharing-device")
		claim1UID := types.UID("claim-1-shared")
		claim2UID := types.UID("claim-2-shared")
		shareUID1 := types.UID("share-uid-1")
		shareUID2 := types.UID("share-uid-2")

		// 两条 claim 都引用同一物理设备（相同 pool/device），但 ShareID 不同
		claim1 := &ClaimInfo{
			ClaimInfoState: state.ClaimInfoState{
				ClaimUID:  claim1UID,
				ClaimName: "shared-claim-1",
				Namespace: namespace,
				PodUIDs:   sets.New[string](string(pod1UID)),
				DriverState: map[string]state.DriverState{
					driverName: {
						Devices: []state.Device{{
							PoolName:     poolName,
							DeviceName:   deviceName, // 与 claim2 使用同一设备
							ShareID:      &shareUID1,
							RequestNames: []string{requestName},
							CDIDeviceIDs: []string{cdiID},
						}},
					},
				},
			},
			prepared: true,
		}
		claim2 := &ClaimInfo{
			ClaimInfoState: state.ClaimInfoState{
				ClaimUID:  claim2UID,
				ClaimName: "shared-claim-2",
				Namespace: namespace,
				PodUIDs:   sets.New[string](string(pod2UID)),
				DriverState: map[string]state.DriverState{
					driverName: {
						Devices: []state.Device{{
							PoolName:     poolName,
							DeviceName:   deviceName, // 与 claim1 使用同一设备
							ShareID:      &shareUID2,
							RequestNames: []string{requestName},
							CDIDeviceIDs: []string{cdiID},
						}},
					},
				},
			},
			prepared: true,
		}

		manager, runStreamTest := setupNewManagerAndRunStreamTest(t, stCtx, claim1, claim2)

		t.Log("HealthChangeForMultiplePodsWithSharedDevice: Test Case Started")

		responses := make(chan struct {
			Resp *drahealthv1.NodeWatchResourcesResponse
			Err  error
		}, 1)
		updateChan, done, streamErrChan := runStreamTest(stCtx, responses)

		// 为共享设备发送健康更新
		unhealthyDeviceMsg := &drahealthv1.DeviceHealth{
			Device: &drahealthv1.DeviceIdentifier{
				PoolName:   poolName,
				DeviceName: deviceName,
			},
			Health:          drahealthv1.HealthStatus_UNHEALTHY,
			LastUpdatedTime: time.Now().Unix(),
		}
		t.Logf("HealthChangeForMultiplePodsWithSharedDevice: Sending health update: %+v", unhealthyDeviceMsg)
		responses <- struct {
			Resp *drahealthv1.NodeWatchResourcesResponse
			Err  error
		}{
			Resp: &drahealthv1.NodeWatchResourcesResponse{Devices: []*drahealthv1.DeviceHealth{unhealthyDeviceMsg}},
		}

		t.Log("HealthChangeForMultiplePodsWithSharedDevice: Waiting for update on manager channel")
		select {
		case upd := <-updateChan:
			t.Logf("HealthChangeForMultiplePodsWithSharedDevice: Received update: %+v", upd)
			// 两个 Pod 共享同一设备，都应收到通知
			assert.Len(t, upd.PodUIDs, 2, "Both pods sharing the device should be notified")
			assert.Contains(t, upd.PodUIDs, string(pod1UID), "Pod 1 should be notified")
			assert.Contains(t, upd.PodUIDs, string(pod2UID), "Pod 2 should be notified")
		case <-time.After(2 * time.Second):
			t.Fatal("HealthChangeForMultiplePodsWithSharedDevice: Timeout waiting for pod update")
		}

		// 验证健康缓存已更新
		cachedHealth := manager.healthInfoCache.getHealthInfo(driverName, poolName, deviceName)
		assert.Equal(t, state.DeviceHealthStatusUnhealthy, cachedHealth.Health, "Health cache should show Unhealthy for shared device")

		t.Log("HealthChangeForMultiplePodsWithSharedDevice: Closing responses channel to signal EOF")
		close(responses)

		var finalErr error
		select {
		case <-done:
			finalErr = <-streamErrChan
			t.Log("HealthChangeForMultiplePodsWithSharedDevice: done channel closed")
		case <-time.After(1 * time.Second):
			t.Fatal("HealthChangeForMultiplePodsWithSharedDevice: Timed out waiting for stream to finish")
		}
		assert.True(t, finalErr == nil || errors.Is(finalErr, io.EOF), "Expected nil or io.EOF, got %v", finalErr)
	})
}

// TestUpdateAllocatedResourcesStatus 验证 manager 能根据不同类型的 DRA claim
// 正确地把健康信息写入 PodStatus。
// 覆盖以下几类在 e2e 环境中因时序问题难以稳定测试的主要场景：
//  1. 直接 claim：Pod 中的 resource claim 名与 ResourceClaim 对象名一致；
//  2. 重命名 claim：Pod 使用本地名引用对象名不同的 ResourceClaim；
//  3. 模板化 claim：claim 由模板生成，Pod 状态中保存动态生成的 claim 名。
func TestUpdateAllocatedResourcesStatus(t *testing.T) {
	directClaimName := "direct-claim-name"
	renamedClaimObject := "renamed-claim-object"
	templateName := "template-name"
	templatedClaimName := "pod-templated-templated-claim"

	testCases := []struct {
		name                             string
		pod                              *v1.Pod
		claimInfos                       []*ClaimInfo
		initialStatus                    *v1.PodStatus
		expectedAllocatedResourcesStatus []v1.ResourceStatus
	}{
		{
			name: "Direct claim",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "pod-direct", UID: "pod-direct-uid"},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{Name: "container1", Resources: v1.ResourceRequirements{Claims: []v1.ResourceClaim{{Name: "claim1"}}}},
					},
					ResourceClaims: []v1.PodResourceClaim{
						{Name: "claim1", ResourceClaimName: &directClaimName},
					},
				},
				Status: v1.PodStatus{
					ResourceClaimStatuses: []v1.PodResourceClaimStatus{
						{Name: "claim1", ResourceClaimName: &directClaimName},
					},
				},
			},
			claimInfos: []*ClaimInfo{
				{
					ClaimInfoState: state.ClaimInfoState{
						ClaimName: directClaimName,
						PodUIDs:   sets.New("pod-direct-uid"),
						DriverState: map[string]state.DriverState{
							"test-driver": {Devices: []state.Device{{PoolName: "pool", DeviceName: "dev-a"}}},
						},
					},
				},
			},
			initialStatus: &v1.PodStatus{ContainerStatuses: []v1.ContainerStatus{{Name: "container1"}}},
			expectedAllocatedResourcesStatus: []v1.ResourceStatus{
				{
					Name: "claim:claim1",
					Resources: []v1.ResourceHealth{
						{ResourceID: "test-driver/pool/dev-a", Health: v1.ResourceHealthStatusHealthy, Message: ptr.To("Device is operating normally")},
					},
				},
			},
		},
		{
			name: "Renamed claim",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "pod-renamed", UID: "pod-renamed-uid"},
				Spec: v1.PodSpec{
					ResourceClaims: []v1.PodResourceClaim{{Name: "renamed-pod-claim", ResourceClaimName: &renamedClaimObject}},
					Containers:     []v1.Container{{Name: "container1", Resources: v1.ResourceRequirements{Claims: []v1.ResourceClaim{{Name: "renamed-pod-claim"}}}}},
				},
				Status: v1.PodStatus{
					ResourceClaimStatuses: []v1.PodResourceClaimStatus{
						{Name: "renamed-pod-claim", ResourceClaimName: &renamedClaimObject},
					},
				},
			},
			claimInfos: []*ClaimInfo{
				{
					ClaimInfoState: state.ClaimInfoState{
						ClaimName: renamedClaimObject,
						PodUIDs:   sets.New("pod-renamed-uid"),
						DriverState: map[string]state.DriverState{
							"test-driver": {Devices: []state.Device{{PoolName: "pool", DeviceName: "dev-b"}}},
						},
					},
				},
			},
			initialStatus: &v1.PodStatus{ContainerStatuses: []v1.ContainerStatus{{Name: "container1"}}},
			expectedAllocatedResourcesStatus: []v1.ResourceStatus{
				{
					Name: "claim:renamed-pod-claim",
					Resources: []v1.ResourceHealth{
						{ResourceID: "test-driver/pool/dev-b", Health: v1.ResourceHealthStatusHealthy, Message: ptr.To("Device is operating normally")},
					},
				},
			},
		},
		{
			name: "Templated claim",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "pod-templated", UID: "pod-templated-uid"},
				Spec: v1.PodSpec{
					ResourceClaims: []v1.PodResourceClaim{{Name: "templated-claim", ResourceClaimTemplateName: &templateName}},
					Containers:     []v1.Container{{Name: "container1", Resources: v1.ResourceRequirements{Claims: []v1.ResourceClaim{{Name: "templated-claim"}}}}},
				},
				Status: v1.PodStatus{
					ResourceClaimStatuses: []v1.PodResourceClaimStatus{
						{Name: "templated-claim", ResourceClaimName: &templatedClaimName},
					},
				},
			},
			claimInfos: []*ClaimInfo{
				{
					ClaimInfoState: state.ClaimInfoState{
						ClaimName: templatedClaimName,
						PodUIDs:   sets.New("pod-templated-uid"),
						DriverState: map[string]state.DriverState{
							"test-driver": {Devices: []state.Device{{PoolName: "pool", DeviceName: "dev-c"}}},
						},
					},
				},
			},
			initialStatus: &v1.PodStatus{ContainerStatuses: []v1.ContainerStatus{{Name: "container1"}}},
			expectedAllocatedResourcesStatus: []v1.ResourceStatus{
				{
					Name: "claim:templated-claim",
					Resources: []v1.ResourceHealth{
						{ResourceID: "test-driver/pool/dev-c", Health: v1.ResourceHealthStatusHealthy, Message: ptr.To("Device is operating normally")},
					},
				},
			},
		},
		// ShareID 用例：验证带 ShareID 的设备能正确上报健康状态，
		// 即 KEP-4860（ShareID）不会干扰 KEP-4680（Resource Health Status）。
		{
			name: "Claim with ShareID",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "pod-shareid", UID: "pod-shareid-uid"},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{Name: "container1", Resources: v1.ResourceRequirements{Claims: []v1.ResourceClaim{{Name: "shareid-claim"}}}},
					},
					ResourceClaims: []v1.PodResourceClaim{
						{Name: "shareid-claim", ResourceClaimName: ptr.To("shareid-claim-object")},
					},
				},
				Status: v1.PodStatus{
					ResourceClaimStatuses: []v1.PodResourceClaimStatus{
						{Name: "shareid-claim", ResourceClaimName: ptr.To("shareid-claim-object")},
					},
				},
			},
			claimInfos: []*ClaimInfo{
				{
					ClaimInfoState: state.ClaimInfoState{
						ClaimName: "shareid-claim-object",
						PodUIDs:   sets.New("pod-shareid-uid"),
						DriverState: map[string]state.DriverState{
							"test-driver": {Devices: []state.Device{{
								PoolName:   "pool",
								DeviceName: "dev-shared",
								ShareID:    ptr.To(types.UID("test-share-id")), // 该设备带 ShareID
							}}},
						},
					},
				},
			},
			initialStatus: &v1.PodStatus{ContainerStatuses: []v1.ContainerStatus{{Name: "container1"}}},
			expectedAllocatedResourcesStatus: []v1.ResourceStatus{
				{
					Name: "claim:shareid-claim",
					Resources: []v1.ResourceHealth{
						{ResourceID: "test-driver/pool/dev-shared", Health: v1.ResourceHealthStatusHealthy, Message: ptr.To("Device is operating normally")},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ResourceHealthStatusMessage, true)
			tCtx := ktesting.Init(t)
			logger := tCtx.Logger()
			manager, err := NewManager(logger, nil, t.TempDir())
			require.NoError(t, err)
			defer manager.Stop()

			for _, ci := range tc.claimInfos {
				manager.cache.add(ci)
			}

			// 测试前把所有设备都设置为健康
			devices := []state.DeviceHealth{}
			for _, ci := range tc.claimInfos {
				for driverName, ds := range ci.DriverState {
					for _, dev := range ds.Devices {
						devices = append(devices, state.DeviceHealth{
							PoolName:   dev.PoolName,
							DeviceName: dev.DeviceName,
							Health:     state.DeviceHealthStatusHealthy,
							Message:    "Device is operating normally",
						})
					}
					_, err := manager.healthInfoCache.updateHealthInfo(logger, driverName, devices)
					require.NoError(t, err)
				}
			}

			status := tc.initialStatus.DeepCopy()
			manager.UpdateAllocatedResourcesStatus(logger, tc.pod, status)

			require.Len(t, status.ContainerStatuses, 1)
			assert.Equal(t, tc.expectedAllocatedResourcesStatus, status.ContainerStatuses[0].AllocatedResourcesStatus)
		})
	}
}

// TestUpdateAllocatedResourcesStatus_Subrequest 验证请求子路径（subrequest）与设备请求名的匹配规则：
// 子请求应匹配其基名，空请求匹配任意设备，不匹配时不上报健康状态
func TestUpdateAllocatedResourcesStatus_Subrequest(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)

	directClaimName := "test-claim"

	testCases := []struct {
		name                  string
		claimRequest          string
		deviceRequestNames    []string
		expectHealthReported  bool
		expectedHealthMessage string
		expectedHealthStatus  v1.ResourceHealthStatus
	}{
		{
			name:                  "Subrequest matches base name",
			claimRequest:          "req-base/sub",
			deviceRequestNames:    []string{"req-base"},
			expectHealthReported:  true,
			expectedHealthMessage: "Device healthy",
			expectedHealthStatus:  v1.ResourceHealthStatusHealthy,
		},
		{
			name:                 "Subrequest does not match different base name",
			claimRequest:         "req-other/sub",
			deviceRequestNames:   []string{"req-base"},
			expectHealthReported: false,
		},
		{
			name:                  "Base request matches base name",
			claimRequest:          "req-base",
			deviceRequestNames:    []string{"req-base"},
			expectHealthReported:  true,
			expectedHealthMessage: "Device healthy",
			expectedHealthStatus:  v1.ResourceHealthStatusHealthy,
		},
		{
			name:                  "Empty request matches any device",
			claimRequest:          "",
			deviceRequestNames:    []string{"req-base"},
			expectHealthReported:  true,
			expectedHealthMessage: "Device healthy",
			expectedHealthStatus:  v1.ResourceHealthStatusHealthy,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ResourceHealthStatusMessage, true)
			tCtx := ktesting.Init(t)
			manager, err := NewManager(tCtx.Logger(), nil, t.TempDir())
			require.NoError(t, err)
			defer manager.Stop()

			// 准备包含设备的 claim info
			claimInfo := &ClaimInfo{
				ClaimInfoState: state.ClaimInfoState{
					ClaimName: directClaimName,
					PodUIDs:   sets.New("test-pod-uid"),
					DriverState: map[string]state.DriverState{
						"test-driver": {
							Devices: []state.Device{
								{
									PoolName:     "test-pool",
									DeviceName:   "test-device",
									RequestNames: tc.deviceRequestNames,
								},
							},
						},
					},
				},
			}
			manager.cache.add(claimInfo)

			// 准备健康缓存
			devices := []state.DeviceHealth{
				{
					PoolName:   "test-pool",
					DeviceName: "test-device",
					Health:     state.DeviceHealthStatusHealthy,
					Message:    "Device healthy",
				},
			}
			_, err = manager.healthInfoCache.updateHealthInfo(tCtx.Logger(), "test-driver", devices)
			require.NoError(t, err)

			// 创建 Pod 与初始状态
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "test-pod", UID: "test-pod-uid"},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "container1",
							Resources: v1.ResourceRequirements{
								Claims: []v1.ResourceClaim{
									{Name: "claim1", Request: tc.claimRequest},
								},
							},
						},
					},
					ResourceClaims: []v1.PodResourceClaim{
						{Name: "claim1", ResourceClaimName: &directClaimName},
					},
				},
				Status: v1.PodStatus{
					ResourceClaimStatuses: []v1.PodResourceClaimStatus{
						{Name: "claim1", ResourceClaimName: &directClaimName},
					},
				},
			}

			status := &v1.PodStatus{
				ContainerStatuses: []v1.ContainerStatus{
					{Name: "container1"},
				},
			}

			// 调用 UpdateAllocatedResourcesStatus
			manager.UpdateAllocatedResourcesStatus(logger, pod, status)

			// 断言结果
			require.Len(t, status.ContainerStatuses, 1)

			if tc.expectHealthReported {
				// 应有一条 ResourceStatus 记录
				require.Len(t, status.ContainerStatuses[0].AllocatedResourcesStatus, 1)
				resStatus := status.ContainerStatuses[0].AllocatedResourcesStatus[0]

				// 应有一条 ResourceHealth 记录
				require.Len(t, resStatus.Resources, 1, "Expected exactly one ResourceHealth entry")
				healthEntry := resStatus.Resources[0]

				// 验证健康状态与写入缓存的内容一致
				assert.Equal(t, tc.expectedHealthStatus, healthEntry.Health)
				assert.Equal(t, tc.expectedHealthMessage, ptr.Deref(healthEntry.Message, ""))
				assert.Equal(t, v1.ResourceID("test-driver/test-pool/test-device"), healthEntry.ResourceID)
			} else {
				// 不应有任何 ResourceStatus 记录（空状态会被裁剪）
				assert.Empty(t, status.ContainerStatuses[0].AllocatedResourcesStatus,
					"Expected AllocatedResourcesStatus to be empty when device is filtered out")
			}
		})
	}
}

// TestTruncateHealthMessage 验证健康消息截断逻辑：超长消息截断到上限并以 "..." 结尾
func TestTruncateHealthMessage(t *testing.T) {
	testCases := map[string]struct {
		input    string
		expected string
	}{
		"empty message": {
			input:    "",
			expected: "",
		},
		"short message": {
			input:    "Device is healthy",
			expected: "Device is healthy",
		},
		"message at exactly max length": {
			input:    string(make([]byte, v1.ResourceHealthMessageMaxLength)),
			expected: string(make([]byte, v1.ResourceHealthMessageMaxLength)),
		},
		"message one character over max length": {
			input:    string(make([]byte, v1.ResourceHealthMessageMaxLength+1)),
			expected: string(make([]byte, v1.ResourceHealthMessageMaxLength-3)) + "...",
		},
		"very long message": {
			input:    string(make([]byte, v1.ResourceHealthMessageMaxLength*2)),
			expected: string(make([]byte, v1.ResourceHealthMessageMaxLength-3)) + "...",
		},
		"message with meaningful content": {
			input:    "ECC error count exceeded threshold on device GPU-0",
			expected: "ECC error count exceeded threshold on device GPU-0",
		},
		"long message with meaningful content": {
			input:    string(make([]byte, v1.ResourceHealthMessageMaxLength)) + " extra content",
			expected: string(make([]byte, v1.ResourceHealthMessageMaxLength-3)) + "...",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			result := truncateHealthMessage(tc.input)
			assert.Equal(t, tc.expected, result)
			// 验证结果长度不会超过上限
			assert.LessOrEqual(t, len(result), v1.ResourceHealthMessageMaxLength)
			// 验证发生截断时结果以 "..." 结尾
			if len(tc.input) > v1.ResourceHealthMessageMaxLength {
				assert.Len(t, result, v1.ResourceHealthMessageMaxLength)
				assert.Equal(t, "...", result[len(result)-3:])
			}
		})
	}
}

// TestBuildResourceHealth 验证 buildResourceHealth 构造的 ResourceHealth：
// 健康状态映射、资源 ID（优先 CDI ID，缺失时回退为 driver/pool/device）与可选消息
func TestBuildResourceHealth(t *testing.T) {
	testCases := map[string]struct {
		device        state.Device
		healthInfo    state.DeviceHealth
		enableMessage bool
		wantHealth    v1.ResourceHealthStatus
		wantResource  v1.ResourceID
		wantMessage   *string
	}{
		"message enabled and CDI id": {
			device: state.Device{
				PoolName:     "pool",
				DeviceName:   "device",
				CDIDeviceIDs: []string{"driver/pool=device"},
			},
			healthInfo: state.DeviceHealth{
				Health:  state.DeviceHealthStatusHealthy,
				Message: "device ok",
			},
			enableMessage: true,
			wantHealth:    v1.ResourceHealthStatusHealthy,
			wantResource:  v1.ResourceID("driver/pool=device"),
			wantMessage:   ptr.To("device ok"),
		},
		"message disabled": {
			device: state.Device{
				PoolName:     "pool",
				DeviceName:   "device",
				CDIDeviceIDs: []string{"driver/pool=device"},
			},
			healthInfo: state.DeviceHealth{
				Health:  state.DeviceHealthStatusHealthy,
				Message: "device ok",
			},
			enableMessage: false,
			wantHealth:    v1.ResourceHealthStatusHealthy,
			wantResource:  v1.ResourceID("driver/pool=device"),
			wantMessage:   nil,
		},
		"empty message": {
			device: state.Device{
				PoolName:     "pool",
				DeviceName:   "device",
				CDIDeviceIDs: []string{"driver/pool=device"},
			},
			healthInfo: state.DeviceHealth{
				Health:  state.DeviceHealthStatusHealthy,
				Message: "",
			},
			enableMessage: true,
			wantHealth:    v1.ResourceHealthStatusHealthy,
			wantResource:  v1.ResourceID("driver/pool=device"),
			wantMessage:   nil,
		},
		"unhealthy status": {
			device: state.Device{
				PoolName:     "pool",
				DeviceName:   "device",
				CDIDeviceIDs: []string{"driver/pool=device"},
			},
			healthInfo: state.DeviceHealth{
				Health:  state.DeviceHealthStatusUnhealthy,
				Message: "device ok",
			},
			enableMessage: true,
			wantHealth:    v1.ResourceHealthStatusUnhealthy,
			wantResource:  v1.ResourceID("driver/pool=device"),
			wantMessage:   ptr.To("device ok"),
		},
		"unknown status": {
			device: state.Device{
				PoolName:     "pool",
				DeviceName:   "device",
				CDIDeviceIDs: []string{"driver/pool=device"},
			},
			healthInfo: state.DeviceHealth{
				Health:  state.DeviceHealthStatusUnknown,
				Message: "device ok",
			},
			enableMessage: true,
			wantHealth:    v1.ResourceHealthStatusUnknown,
			wantResource:  v1.ResourceID("driver/pool=device"),
			wantMessage:   ptr.To("device ok"),
		},
		"fallback resource id": {
			device: state.Device{
				PoolName:   "pool",
				DeviceName: "device",
			},
			healthInfo: state.DeviceHealth{
				Health:  state.DeviceHealthStatusUnhealthy,
				Message: "device ok",
			},
			enableMessage: true,
			wantHealth:    v1.ResourceHealthStatusUnhealthy,
			wantResource:  v1.ResourceID("driver/pool/device"),
			wantMessage:   ptr.To("device ok"),
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			got := buildResourceHealth("driver", tc.device, tc.healthInfo, tc.enableMessage)
			assert.Equal(t, tc.wantHealth, got.Health)
			assert.Equal(t, tc.wantResource, got.ResourceID)
			assert.Equal(t, tc.wantMessage, got.Message)
		})
	}
}

// TestToResourceHealthStatus 验证内部设备健康状态到 API ResourceHealthStatus 的映射，
// 未知或空值一律映射为 Unknown
func TestToResourceHealthStatus(t *testing.T) {
	testCases := map[string]struct {
		input    state.DeviceHealthStatus
		expected v1.ResourceHealthStatus
	}{
		"healthy": {
			input:    state.DeviceHealthStatusHealthy,
			expected: v1.ResourceHealthStatusHealthy,
		},
		"unhealthy": {
			input:    state.DeviceHealthStatusUnhealthy,
			expected: v1.ResourceHealthStatusUnhealthy,
		},
		"unknown": {
			input:    state.DeviceHealthStatusUnknown,
			expected: v1.ResourceHealthStatusUnknown,
		},
		"empty": {
			input:    state.DeviceHealthStatus(""),
			expected: v1.ResourceHealthStatusUnknown,
		},
		"unexpected": {
			input:    state.DeviceHealthStatus("Other"),
			expected: v1.ResourceHealthStatusUnknown,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, toResourceHealthStatus(tc.input))
		})
	}
}

// TestToDeviceHealthStatus 验证 gRPC 健康状态到内部设备健康状态的映射，未知值映射为 Unknown
func TestToDeviceHealthStatus(t *testing.T) {
	testCases := map[string]struct {
		input    drahealthv1.HealthStatus
		expected state.DeviceHealthStatus
	}{
		"healthy": {
			input:    drahealthv1.HealthStatus_HEALTHY,
			expected: state.DeviceHealthStatusHealthy,
		},
		"unhealthy": {
			input:    drahealthv1.HealthStatus_UNHEALTHY,
			expected: state.DeviceHealthStatusUnhealthy,
		},
		"unknown": {
			input:    drahealthv1.HealthStatus_UNKNOWN,
			expected: state.DeviceHealthStatusUnknown,
		},
		"unexpected": {
			input:    drahealthv1.HealthStatus(99),
			expected: state.DeviceHealthStatusUnknown,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, tc.expected, toDeviceHealthStatus(tc.input))
		})
	}
}

// TestBuildDeviceHealth 验证 buildDeviceHealth 对健康状态、超时与消息的转换：
// 非正超时使用默认值，超长消息会被截断
func TestBuildDeviceHealth(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	longMessage := strings.Repeat("a", v1.ResourceHealthMessageMaxLength+5)

	testCases := map[string]struct {
		health         drahealthv1.HealthStatus
		timeoutSeconds int64
		message        string
		wantHealth     state.DeviceHealthStatus
		wantTimeout    time.Duration
		wantMessage    string
	}{
		"healthy with positive timeout": {
			health:         drahealthv1.HealthStatus_HEALTHY,
			timeoutSeconds: 12,
			message:        "ok",
			wantHealth:     state.DeviceHealthStatusHealthy,
			wantTimeout:    12 * time.Second,
			wantMessage:    "ok",
		},
		"unhealthy with zero timeout": {
			health:         drahealthv1.HealthStatus_UNHEALTHY,
			timeoutSeconds: 0,
			message:        "fail",
			wantHealth:     state.DeviceHealthStatusUnhealthy,
			wantTimeout:    DefaultHealthTimeout,
			wantMessage:    "fail",
		},
		"unknown with negative timeout": {
			health:         drahealthv1.HealthStatus_UNKNOWN,
			timeoutSeconds: -1,
			message:        longMessage,
			wantHealth:     state.DeviceHealthStatusUnknown,
			wantTimeout:    DefaultHealthTimeout,
			wantMessage:    truncateHealthMessage(longMessage),
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			grpcDevice := &drahealthv1.DeviceHealth{
				Device: &drahealthv1.DeviceIdentifier{
					PoolName:   "pool",
					DeviceName: "device",
				},
				Health:                    tc.health,
				LastUpdatedTime:           123,
				HealthCheckTimeoutSeconds: tc.timeoutSeconds,
				Message:                   tc.message,
			}

			got := buildDeviceHealth(logger, grpcDevice)
			assert.Equal(t, "pool", got.PoolName)
			assert.Equal(t, "device", got.DeviceName)
			assert.Equal(t, tc.wantHealth, got.Health)
			assert.Equal(t, time.Unix(123, 0), got.LastUpdated)
			assert.Equal(t, tc.wantTimeout, got.HealthCheckTimeout)
			assert.Equal(t, tc.wantMessage, got.Message)
		})
	}
}

// genTestClaimWithSkip 生成一个设置了 SkipNodeOperations 的 ResourceClaim，
// 用于验证跳过节点操作的场景
func genTestClaimWithSkip(name, driver, device, podUID string, skip ...resourceapi.SkipNodeOperation) *resourceapi.ResourceClaim {
	c := genTestClaim(name, driver, device, podUID)
	c.Status.Allocation.Devices.Results[0].SkipNodeOperations = skip
	return c
}

// TestPrepareUnprepareResourcesSkipNodeOperations 验证 claim 上的 SkipNodeOperations 语义：
// SkipNodeOperationAll 跳过 prepare 与 unprepare；只跳过 unprepare 时 prepare 仍会调用；
// 并在特性开关被关闭（回滚）时保证 unprepare 仍能成功完成
func TestPrepareUnprepareResourcesSkipNodeOperations(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	fakeKubeClient := fake.NewClientset()

	manager, err := NewManager(logger, fakeKubeClient, t.TempDir())
	require.NoError(t, err)
	manager.initDRAPluginManager(tCtx, getFakeNode, time.Second)

	draServerInfo, err := setupFakeDRADriverGRPCServer(tCtx, false, nil, nil, nil, nil)
	require.NoError(t, err)
	defer draServerInfo.teardownFn()

	pluginHandler := manager.GetWatcherHandler()
	require.NoError(t, pluginHandler.RegisterPlugin(tCtx, driverName, draServerInfo.socketName, []string{drapb.DRAPluginService}, nil))

	t.Run("SkipNodeOperationAll", func(t *testing.T) {
		t.Run("Prepare fails when feature gate is disabled", func(t *testing.T) {
			const claimName = "claim-skip-all-fg-disabled"
			pod := genTestPodWithClaims(claimName)
			claim := genTestClaimWithSkip(claimName, driverName, deviceName, string(pod.ObjectMeta.UID), resourceapi.SkipNodeOperationAll)
			claim.Status.ReservedFor = append(claim.Status.ReservedFor, resourceapi.ResourceClaimConsumerReference{UID: pod.ObjectMeta.UID})
			_, err = fakeKubeClient.ResourceV1().ResourceClaims(namespace).Create(tCtx, claim, metav1.CreateOptions{})
			require.NoError(t, err)

			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DRAOptionalNodeOperations, false)
			err = manager.PrepareResources(tCtx, pod)
			require.ErrorContains(t, err, "DRAOptionalNodeOperations feature gate is disabled")
			assert.Equal(t, uint32(0), draServerInfo.server.prepareResourceCalls.Load())
		})

		t.Run("Feature gate enabled for prepare and unprepare", func(t *testing.T) {
			const claimName = "claim-skip-all-normal"
			pod := genTestPodWithClaims(claimName)
			claim := genTestClaimWithSkip(claimName, driverName, deviceName, string(pod.ObjectMeta.UID), resourceapi.SkipNodeOperationAll)
			claim.Status.ReservedFor = append(claim.Status.ReservedFor, resourceapi.ResourceClaimConsumerReference{UID: pod.ObjectMeta.UID})
			_, err = fakeKubeClient.ResourceV1().ResourceClaims(namespace).Create(tCtx, claim, metav1.CreateOptions{})
			require.NoError(t, err)

			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DRAOptionalNodeOperations, true)

			err = manager.PrepareResources(tCtx, pod)
			require.NoError(t, err)
			assert.Equal(t, uint32(0), draServerInfo.server.prepareResourceCalls.Load(), "NodePrepareResources must be skipped for SkipNodeOperationAll")

			err = manager.UnprepareResources(tCtx, pod)
			require.NoError(t, err)
			assert.Equal(t, uint32(0), draServerInfo.server.unprepareResourceCalls.Load(), "NodeUnprepareResources must be skipped for SkipNodeOperationAll")
		})

		t.Run("Feature gate disabled during unprepare", func(t *testing.T) {
			const claimName = "claim-skip-all-rollback"
			pod := genTestPodWithClaims(claimName)
			claim := genTestClaimWithSkip(claimName, driverName, deviceName, string(pod.ObjectMeta.UID), resourceapi.SkipNodeOperationAll)
			claim.Status.ReservedFor = append(claim.Status.ReservedFor, resourceapi.ResourceClaimConsumerReference{UID: pod.ObjectMeta.UID})
			_, err = fakeKubeClient.ResourceV1().ResourceClaims(namespace).Create(tCtx, claim, metav1.CreateOptions{})
			require.NoError(t, err)

			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DRAOptionalNodeOperations, true)
			err = manager.PrepareResources(tCtx, pod)
			require.NoError(t, err)

			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DRAOptionalNodeOperations, false)
			err = manager.UnprepareResources(tCtx, pod)
			require.NoError(t, err, "UnprepareResources must succeed during rollback even if feature gate is disabled")
			assert.Equal(t, uint32(0), draServerInfo.server.unprepareResourceCalls.Load(), "NodeUnprepareResources must be skipped for SkipNodeOperationAll")
		})
	})

	t.Run("SkipNodeOperationNodeUnprepareResources", func(t *testing.T) {
		t.Run("Feature gate enabled for prepare and unprepare", func(t *testing.T) {
			const claimName = "claim-skip-unprepare-normal"
			pod := genTestPodWithClaims(claimName)
			claim := genTestClaimWithSkip(claimName, driverName, deviceName, string(pod.ObjectMeta.UID), resourceapi.SkipNodeOperationNodeUnprepareResources)
			claim.Status.ReservedFor = append(claim.Status.ReservedFor, resourceapi.ResourceClaimConsumerReference{UID: pod.ObjectMeta.UID})
			_, err = fakeKubeClient.ResourceV1().ResourceClaims(namespace).Create(tCtx, claim, metav1.CreateOptions{})
			require.NoError(t, err)

			draServerInfo.server.prepareResourcesResponse = genPrepareResourcesResponse(claim.UID)

			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DRAOptionalNodeOperations, true)

			prepareCallsBefore := draServerInfo.server.prepareResourceCalls.Load()
			err = manager.PrepareResources(tCtx, pod)
			require.NoError(t, err)
			assert.Equal(t, prepareCallsBefore+1, draServerInfo.server.prepareResourceCalls.Load(), "NodePrepareResources must be called for SkipNodeOperationNodeUnprepareResources")

			err = manager.UnprepareResources(tCtx, pod)
			require.NoError(t, err)
			assert.Equal(t, uint32(0), draServerInfo.server.unprepareResourceCalls.Load(), "NodeUnprepareResources must be skipped for SkipNodeOperationNodeUnprepareResources")
		})

		t.Run("Feature gate disabled during unprepare", func(t *testing.T) {
			const claimName = "claim-skip-unprepare-rollback"
			pod := genTestPodWithClaims(claimName)
			claim := genTestClaimWithSkip(claimName, driverName, deviceName, string(pod.ObjectMeta.UID), resourceapi.SkipNodeOperationNodeUnprepareResources)
			claim.Status.ReservedFor = append(claim.Status.ReservedFor, resourceapi.ResourceClaimConsumerReference{UID: pod.ObjectMeta.UID})
			_, err = fakeKubeClient.ResourceV1().ResourceClaims(namespace).Create(tCtx, claim, metav1.CreateOptions{})
			require.NoError(t, err)

			draServerInfo.server.prepareResourcesResponse = genPrepareResourcesResponse(claim.UID)

			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DRAOptionalNodeOperations, true)
			err = manager.PrepareResources(tCtx, pod)
			require.NoError(t, err)

			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DRAOptionalNodeOperations, false)
			err = manager.UnprepareResources(tCtx, pod)
			require.NoError(t, err, "UnprepareResources must succeed during rollback even if feature gate is disabled")
			assert.Equal(t, uint32(0), draServerInfo.server.unprepareResourceCalls.Load(), "NodeUnprepareResources must be skipped for SkipNodeOperationNodeUnprepareResources")
		})
	})
}
