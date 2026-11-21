/*
Copyright The Kubernetes Authors.

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

package pods_test

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/apitesting/fuzzer"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metafuzzer "k8s.io/apimachinery/pkg/apis/meta/fuzzer"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	compmetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/testutil"
	podsv1alpha1 "k8s.io/kubelet/pkg/apis/pods/v1alpha1"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	corefuzzer "k8s.io/kubernetes/pkg/apis/core/fuzzer"
	"k8s.io/kubernetes/pkg/features"
	apisgrpc "k8s.io/kubernetes/pkg/kubelet/apis/grpc"
	podsapi "k8s.io/kubernetes/pkg/kubelet/apis/pods"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	kubepodtest "k8s.io/kubernetes/pkg/kubelet/pod/testing"
	statustest "k8s.io/kubernetes/pkg/kubelet/status/testing"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestStartEventLoop 验证广播器事件循环：Pod 的添加、更新与删除事件按序投递给已注册客户端，
// 删除事件携带完整 Pod 信息，且无多余事件。
func TestStartEventLoop(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	broadcaster := podsapi.NewBroadcaster(tCtx)
	mockManager := new(kubepodtest.MockManager)
	mockStatus := new(statustest.MockPodStatusProvider)
	server := podsapi.NewPodsServerForTest(broadcaster, mockManager, mockStatus)
	pod1 := &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "pod1-uid", Name: "pod1", Namespace: "ns1"}}

	clientChannel := make(chan podsapi.PodWatchEvent, 100)
	broadcaster.Register(logger, clientChannel)
	defer broadcaster.Unregister(logger, clientChannel)

	server.OnPodUpdated(logger, pod1, v1.PodStatus{Phase: v1.PodPending}, true)
	server.OnPodUpdated(logger, pod1, v1.PodStatus{Phase: v1.PodSucceeded}, false)
	server.OnPodRemoved(logger, pod1)

	event := <-clientChannel
	assert.Equal(t, watch.Added, event.Type)
	assert.Equal(t, pod1.UID, event.UID)
	assert.Equal(t, v1.PodPending, event.Pod.Status.Phase)

	event = <-clientChannel
	assert.Equal(t, watch.Modified, event.Type)
	assert.Equal(t, pod1.UID, event.UID)
	assert.Equal(t, v1.PodSucceeded, event.Pod.Status.Phase)

	event = <-clientChannel
	assert.Equal(t, watch.Deleted, event.Type)
	assert.Equal(t, pod1.UID, event.UID)
	require.NotNil(t, event.Pod)
	assert.Equal(t, pod1.Name, event.Pod.Name)
	assert.Equal(t, pod1.Namespace, event.Pod.Namespace)
	assert.Equal(t, pod1.UID, event.Pod.UID)
	assert.Empty(t, event.Pod.Spec.Containers)

	select {
	case event := <-clientChannel:
		t.Errorf("Unexpected event received: %v", event)
	default:
	}
}

// MockWatchPodsServer 是 gRPC Stream 的测试替身，将服务端发送的 WatchPodsEvent 投递到 EventCh，
// 并通过 Ctx 感知流的取消。
type MockWatchPodsServer struct {
	grpc.ServerStream
	Ctx     context.Context
	EventCh chan *podsv1alpha1.WatchPodsEvent
}

// Send 将事件投递到事件通道；通道已满或上下文取消时返回错误。
func (m *MockWatchPodsServer) Send(event *podsv1alpha1.WatchPodsEvent) error {
	select {
	case m.EventCh <- event:
		return nil
	case <-m.Ctx.Done():
		return m.Ctx.Err()
	}
}

// Context 返回流绑定的上下文，供取消检测使用。
func (m *MockWatchPodsServer) Context() context.Context {
	return m.Ctx
}

// TestWatchPods 验证 WatchPods 流的完整事件序列：初始 ADDED 与 INITIAL_SYNC_COMPLETE，
// 以及更新、删除对应的 MODIFIED 与 DELETED 事件。
func TestWatchPods(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodsAPI, true)
	logger, tCtx := ktesting.NewTestContext(t)
	broadcaster := podsapi.NewBroadcaster(tCtx)
	mockManager := new(kubepodtest.MockManager)
	mockStatus := new(statustest.MockPodStatusProvider)
	server := podsapi.NewPodsServerForTest(broadcaster, mockManager, mockStatus)
	pod1 := &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "pod1-uid", Name: "pod1", Namespace: "ns1"}}

	mockManager.On("GetPods").Return([]*v1.Pod{pod1})
	mockManager.On("GetPodByUID", types.UID("pod1-uid")).Return(pod1, true)
	mockStatus.On("GetPodStatus", mock.Anything).Return(v1.PodStatus{}, false)

	streamCtx, cancel := context.WithCancel(tCtx)
	defer cancel()

	mockStream := &MockWatchPodsServer{
		Ctx:     streamCtx,
		EventCh: make(chan *podsv1alpha1.WatchPodsEvent, 10),
	}

	go func() {
		_ = server.WatchPods(&podsv1alpha1.WatchPodsRequest{}, mockStream)
	}()

	// 验证初始 ADDED 事件
	event := <-mockStream.EventCh
	assert.Equal(t, podsv1alpha1.EventType_ADDED, event.Type)
	podOut := &v1.Pod{}
	err := podOut.Unmarshal(event.Pod)
	require.NoError(t, err)
	assert.Equal(t, "pod1", podOut.Name)

	// 验证 INITIAL_SYNC_COMPLETE 事件
	event = <-mockStream.EventCh
	assert.Equal(t, podsv1alpha1.EventType_INITIAL_SYNC_COMPLETE, event.Type)
	assert.Empty(t, event.Pod)

	// 触发一次更新
	server.OnPodUpdated(logger, pod1, pod1.Status, false)

	// 验证 MODIFIED 事件
	event = <-mockStream.EventCh
	assert.Equal(t, podsv1alpha1.EventType_MODIFIED, event.Type)
	podOut = &v1.Pod{}
	err = podOut.Unmarshal(event.Pod)
	require.NoError(t, err)
	assert.Equal(t, "pod1", podOut.Name)

	// 触发一次删除
	server.OnPodRemoved(logger, pod1)

	// 验证 DELETED 事件
	event = <-mockStream.EventCh
	assert.Equal(t, podsv1alpha1.EventType_DELETED, event.Type)
	podOut = &v1.Pod{}
	err = podOut.Unmarshal(event.Pod)
	require.NoError(t, err)
	assert.Equal(t, "pod1", podOut.Name)
	assert.Equal(t, "ns1", podOut.Namespace)
	assert.Equal(t, pod1.UID, podOut.UID)
	assert.Empty(t, podOut.Spec.Containers)
}

// TestListPods 验证 ListPods 返回全部 Pod，且返回内容可正确反序列化。
func TestListPods(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodsAPI, true)
	tCtx := ktesting.Init(t)
	broadcaster := podsapi.NewBroadcaster(tCtx)
	mockManager := new(kubepodtest.MockManager)
	mockStatus := new(statustest.MockPodStatusProvider)
	server := podsapi.NewPodsServerForTest(broadcaster, mockManager, mockStatus)
	pod1 := &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "pod1-uid", Name: "pod1", Namespace: "ns1"}}
	pod2 := &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "pod2-uid", Name: "pod2", Namespace: "ns2"}}

	mockManager.On("GetPods").Return([]*v1.Pod{pod1, pod2})
	mockStatus.On("GetPodStatus", mock.Anything).Return(v1.PodStatus{}, false)

	resp, err := server.ListPods(tCtx, &podsv1alpha1.ListPodsRequest{})
	require.NoError(t, err)
	pod1Out := &v1.Pod{}
	err = pod1Out.Unmarshal(resp.Pods[0])
	require.NoError(t, err)
	pod2Out := &v1.Pod{}
	err = pod2Out.Unmarshal(resp.Pods[1])
	require.NoError(t, err)
	require.Equal(t, "pod1", pod1Out.Name)
	assert.Equal(t, "pod2", pod2Out.Name)
}

// TestGetPod 验证 GetPod 返回单个 Pod，临时容器（ephemeral container）的完整配置也能正确还原。
func TestGetPod(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodsAPI, true)
	tCtx := ktesting.Init(t)
	broadcaster := podsapi.NewBroadcaster(tCtx)
	mockManager := new(kubepodtest.MockManager)
	mockStatus := new(statustest.MockPodStatusProvider)
	server := podsapi.NewPodsServerForTest(broadcaster, mockManager, mockStatus)
	pod1 := &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "pod1-uid", Name: "pod1", Namespace: "ns1"}}
	pod1.Spec.EphemeralContainers = []v1.EphemeralContainer{
		{
			EphemeralContainerCommon: v1.EphemeralContainerCommon{
				Name:    "debugger",
				Image:   "busybox",
				Command: []string{"sh"},
				Stdin:   true,
				TTY:     true,
			},
		},
	}
	pod1.Status = v1.PodStatus{Phase: v1.PodRunning}

	mockManager.On("GetPodByUID", mock.Anything).Return(pod1, true)
	mockStatus.On("GetPodStatus", mock.Anything).Return(v1.PodStatus{}, false)

	resp, err := server.GetPod(tCtx, &podsv1alpha1.GetPodRequest{PodUID: "pod1-uid"})
	require.NoError(t, err)
	podOut := &v1.Pod{}
	err = podOut.Unmarshal(resp.Pod)
	require.NoError(t, err)
	require.Equal(t, "pod1", podOut.Name)
	assert.Equal(t, v1.PodRunning, podOut.Status.Phase)
	require.Len(t, podOut.Spec.EphemeralContainers, 1)
	assert.Equal(t, "debugger", podOut.Spec.EphemeralContainers[0].Name)
	assert.Equal(t, "busybox", podOut.Spec.EphemeralContainers[0].Image)
	assert.Equal(t, []string{"sh"}, podOut.Spec.EphemeralContainers[0].Command)
	assert.True(t, podOut.Spec.EphemeralContainers[0].Stdin)
	assert.True(t, podOut.Spec.EphemeralContainers[0].TTY)
}

// TestStaticPod 验证 GetPod 对带 file 来源注解的静态 Pod 同样可用。
func TestStaticPod(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodsAPI, true)
	tCtx := ktesting.Init(t)
	broadcaster := podsapi.NewBroadcaster(tCtx)
	mockManager := new(kubepodtest.MockManager)
	mockStatus := new(statustest.MockPodStatusProvider)
	server := podsapi.NewPodsServerForTest(broadcaster, mockManager, mockStatus)

	staticPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "static-pod-uid",
			Name:      "static-pod",
			Namespace: "default",
			Annotations: map[string]string{
				"kubernetes.io/config.source": "file",
			},
		},
	}

	mockManager.On("GetPodByUID", types.UID("static-pod-uid")).Return(staticPod, true)
	mockStatus.On("GetPodStatus", mock.Anything).Return(v1.PodStatus{}, false)

	resp, err := server.GetPod(tCtx, &podsv1alpha1.GetPodRequest{PodUID: "static-pod-uid"})
	require.NoError(t, err)

	podOut := &v1.Pod{}
	err = podOut.Unmarshal(resp.Pod)
	require.NoError(t, err)
	assert.Equal(t, "static-pod", podOut.Name)
	assert.Equal(t, "file", podOut.Annotations["kubernetes.io/config.source"])
}

// TestErrorsAndMetrics 验证客户端消费过慢导致事件被丢弃时，丢弃计数指标正确累加。
func TestErrorsAndMetrics(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodsAPI, true)
	metrics.Register()

	// 子测试：缓冲区被填满后第二个事件被丢弃，指标应累加。
	t.Run("DroppedWatchEventIncrementsMetric", func(t *testing.T) {
		logger, tCtx := ktesting.NewTestContext(t)
		broadcaster := podsapi.NewBroadcaster(tCtx)
		mockManager := new(kubepodtest.MockManager)
		mockStatus := new(statustest.MockPodStatusProvider)
		server := podsapi.NewPodsServerForTest(broadcaster, mockManager, mockStatus)
		pod1 := &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "pod1-uid", Name: "pod1", Namespace: "ns1"}}
		mockStatus.On("GetPodStatus", mock.Anything).Return(v1.PodStatus{}, false)
		server.OnPodUpdated(logger, pod1, pod1.Status, true)

		// 测试前重置指标
		metrics.PodWatchEventsDroppedTotal.Reset()
		clientChannel := make(chan podsapi.PodWatchEvent, 1) // Buffered channel of size 1
		broadcaster.Register(logger, clientChannel)
		defer broadcaster.Unregister(logger, clientChannel)

		// 发送两个事件：第一个应填满缓冲区
		broadcaster.Broadcast(logger, podsapi.PodWatchEvent{UID: "1"})
		// 第二个应被丢弃并累加指标
		broadcaster.Broadcast(logger, podsapi.PodWatchEvent{UID: "2"})

		// 等待后台协程处理事件并更新指标
		err := wait.PollUntilContextTimeout(tCtx, 10*time.Millisecond, 2*time.Second, true, func(ctx context.Context) (bool, error) {
			count, err := testutil.GetCounterMetricValue(metrics.PodWatchEventsDroppedTotal)
			if err != nil {
				return false, err
			}
			return count == 1, nil
		})
		require.NoError(t, err, "Metric PodWatchEventsDroppedTotal should be 1")

		err = testutil.CollectAndCompare(metrics.PodWatchEventsDroppedTotal, strings.NewReader(`
			# HELP kubelet_pod_watch_events_dropped_total [ALPHA] Cumulative number of pod watch events dropped.
			# TYPE kubelet_pod_watch_events_dropped_total counter
			kubelet_pod_watch_events_dropped_total 1
		`))
		require.NoError(t, err)
	})
}

// TestSerialize 循环验证 Pod 经 protobuf 序列化与反序列化后内容保持一致（round-trip 一致性）。
func TestSerialize(t *testing.T) {
	apiObjectFuzzer := fuzzer.FuzzerFor(fuzzer.MergeFuzzerFuncs(metafuzzer.Funcs, corefuzzer.Funcs), rand.NewSource(152), legacyscheme.Codecs)
	for range 100 {
		pod := &v1.Pod{}
		apiObjectFuzzer.Fill(pod)
		podBytes, err := pod.Marshal()
		if err != nil {
			t.Fatal(err)
		}

		resp := &podsv1alpha1.ListPodsResponse{Pods: [][]byte{podBytes}}
		data, err := proto.Marshal(resp)
		if err != nil {
			t.Fatal(err)
		}

		resp2 := &podsv1alpha1.ListPodsResponse{}
		if err := proto.Unmarshal(data, resp2); err != nil {
			t.Fatal(err)
		}

		if !proto.Equal(resp, resp2) {
			t.Fatal("round-tripped objects were different")
		}
		pod2 := &v1.Pod{}
		if err := pod2.Unmarshal(resp2.Pods[0]); err != nil {
			t.Fatal(err)
		}
		if !apiequality.Semantic.DeepEqual(pod, pod2) {
			t.Fatal("round-tripped objects were different")
		}
	}
}

// TestBroadcaster_SlowClient 验证广播器对慢客户端的容忍策略：慢客户端最终被丢弃并计入丢弃指标，
// 快客户端不受影响。
func TestBroadcaster_SlowClient(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	broadcaster := podsapi.NewBroadcaster(tCtx)

	numFastClients := 3
	numEvents := 10
	fastClients := make([]chan podsapi.PodWatchEvent, numFastClients)
	for i := range numFastClients {
		fastClients[i] = make(chan podsapi.PodWatchEvent, numEvents)
		broadcaster.Register(logger, fastClients[i])
	}

	slowClient := make(chan podsapi.PodWatchEvent)
	broadcaster.Register(logger, slowClient)

	for i := range numEvents {
		broadcaster.Broadcast(logger, podsapi.PodWatchEvent{UID: types.UID(fmt.Sprintf("event-%d", i))})
	}

	for i := range numFastClients {
		for j := range numEvents {
			select {
			case event := <-fastClients[i]:
				expectedUID := types.UID(fmt.Sprintf("event-%d", j))
				assert.Equal(t, expectedUID, event.UID, "Fast client %d missed or got wrong event", i)
			case <-time.After(2 * time.Second):
				t.Fatalf("Fast client %d timed out waiting for event %d", i, j)
			}
		}
	}

	err := wait.PollUntilContextTimeout(tCtx, 10*time.Millisecond, 2*time.Second, true, func(ctx context.Context) (bool, error) {
		select {
		case _, ok := <-slowClient:
			if !ok {
				return true, nil
			}
			return false, nil
		default:
			count, _ := testutil.GetCounterMetricValue(metrics.PodWatchEventsDroppedTotal)
			return count >= 1, nil
		}
	})
	assert.NoError(t, err, "Slow client should have been dropped")
}

// TestStatusOverlay 验证 GetPod、ListPods 与 WatchPods 初始同步均会用 statusManager 的状态覆盖 Pod 状态。
func TestStatusOverlay(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodsAPI, true)
	tCtx := ktesting.Init(t)
	broadcaster := podsapi.NewBroadcaster(tCtx)
	mockManager := new(kubepodtest.MockManager)
	mockStatus := new(statustest.MockPodStatusProvider)
	server := podsapi.NewPodsServerForTest(broadcaster, mockManager, mockStatus)

	pod1 := &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "pod1-uid", Name: "pod1", Namespace: "ns1"}}
	pod1.Status.Phase = v1.PodPending

	overlaidStatus := v1.PodStatus{Phase: v1.PodRunning}

	mockManager.On("GetPods").Return([]*v1.Pod{pod1})
	mockManager.On("GetPodByUID", pod1.UID).Return(pod1, true)
	mockStatus.On("GetPodStatus", pod1.UID).Return(overlaidStatus, true)

	t.Run("GetPod overlays status", func(t *testing.T) {
		resp, err := server.GetPod(tCtx, &podsv1alpha1.GetPodRequest{PodUID: string(pod1.UID)})
		require.NoError(t, err)
		podOut := &v1.Pod{}
		err = podOut.Unmarshal(resp.Pod)
		require.NoError(t, err)
		assert.Equal(t, v1.PodRunning, podOut.Status.Phase)
	})

	t.Run("ListPods overlays status", func(t *testing.T) {
		resp, err := server.ListPods(tCtx, &podsv1alpha1.ListPodsRequest{})
		require.NoError(t, err)
		podOut := &v1.Pod{}
		err = podOut.Unmarshal(resp.Pods[0])
		require.NoError(t, err)
		assert.Equal(t, v1.PodRunning, podOut.Status.Phase)
	})

	t.Run("WatchPods overlays status in initial sync", func(t *testing.T) {
		streamCtx, cancel := context.WithCancel(tCtx)
		defer cancel()
		mockStream := &MockWatchPodsServer{
			Ctx:     streamCtx,
			EventCh: make(chan *podsv1alpha1.WatchPodsEvent, 10),
		}
		go func() {
			_ = server.WatchPods(&podsv1alpha1.WatchPodsRequest{}, mockStream)
		}()

		event := <-mockStream.EventCh
		assert.Equal(t, podsv1alpha1.EventType_ADDED, event.Type)
		podOut := &v1.Pod{}
		err := podOut.Unmarshal(event.Pod)
		require.NoError(t, err)
		assert.Equal(t, v1.PodRunning, podOut.Status.Phase)
	})
}

// TestInterceptorsAndMetrics 验证 gRPC 拦截器按请求类型与状态码统计指标，
// 覆盖 ListPods、GetPod、WatchPods 的 OK/FailedPrecondition/NotFound/Canceled 等结果。
func TestInterceptorsAndMetrics(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodsAPI, true)
	metrics.Register()

	// 测试前重置指标
	metrics.PodRequestsTotal.Reset()
	metrics.PodRequestsList.Reset()
	metrics.PodRequestsGet.Reset()
	metrics.PodRequestsWatch.Reset()

	_, tCtx := ktesting.NewTestContext(t)
	broadcaster := podsapi.NewBroadcaster(tCtx)
	mockManager := new(kubepodtest.MockManager)
	mockStatus := new(statustest.MockPodStatusProvider)
	fakeSources := &FakeSourcesReady{ready: true}
	server := podsapi.NewPodsServer(broadcaster, mockManager, mockStatus, fakeSources)

	// 设置 mock 期望
	pod1 := &v1.Pod{ObjectMeta: metav1.ObjectMeta{UID: "pod1-uid", Name: "pod1", Namespace: "ns1"}}
	mockManager.On("GetPods").Return([]*v1.Pod{pod1})
	mockManager.On("GetPodByUID", types.UID("pod1-uid")).Return(pod1, true)
	mockManager.On("GetPodByUID", types.UID("non-existent")).Return((*v1.Pod)(nil), false)
	mockStatus.On("GetPodStatus", mock.Anything).Return(v1.PodStatus{}, false)

	// 启动带拦截器的 gRPC 服务端
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = lis.Close() }()

	s := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			apisgrpc.LimiterUnaryServerInterceptor(rate.NewLimiter(rate.Limit(100), 100)),
			podsapi.MetricsUnaryServerInterceptor,
		),
		grpc.StreamInterceptor(podsapi.MetricsStreamServerInterceptor),
	)
	podsv1alpha1.RegisterPodsServer(s, server)
	go func() {
		_ = s.Serve(lis)
	}()
	defer s.GracefulStop()

	// 建立客户端连接
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	client := podsv1alpha1.NewPodsClient(conn)

	// 1. 调用 ListPods（成功 -> OK）
	_, err = client.ListPods(tCtx, &podsv1alpha1.ListPodsRequest{})
	require.NoError(t, err)

	// 1b. 调用 ListPods（失败 -> FailedPrecondition）
	fakeSources.ready = false
	_, err = client.ListPods(tCtx, &podsv1alpha1.ListPodsRequest{})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
	// 恢复就绪状态
	fakeSources.ready = true

	// 2. 调用 GetPod（成功 -> OK）
	_, err = client.GetPod(tCtx, &podsv1alpha1.GetPodRequest{PodUID: "pod1-uid"})
	require.NoError(t, err)

	// 3. 调用 GetPod（失败 -> NotFound）
	_, err = client.GetPod(tCtx, &podsv1alpha1.GetPodRequest{PodUID: "non-existent"})
	require.Error(t, err)

	// 4. 调用 WatchPods
	watchCtx, watchCancel := context.WithCancel(tCtx)
	watchClient, err := client.WatchPods(watchCtx, &podsv1alpha1.WatchPodsRequest{})
	require.NoError(t, err)
	// 接收第一个事件（ADDED）
	_, err = watchClient.Recv()
	require.NoError(t, err)
	// 接收第二个事件（INITIAL_SYNC_COMPLETE）
	_, err = watchClient.Recv()
	require.NoError(t, err)

	// 取消 watch 上下文以触发 WatchPods 错误（context.Canceled）
	watchCancel()
	// 等待流关闭
	_, err = watchClient.Recv()
	require.Error(t, err)

	// 按状态码维度验证指标
	assertCounterEventually(t, metrics.PodRequestsTotal, []string{"v1alpha1", "OK"}, 2)
	assertCounterEventually(t, metrics.PodRequestsTotal, []string{"v1alpha1", "FailedPrecondition"}, 1)
	assertCounterEventually(t, metrics.PodRequestsTotal, []string{"v1alpha1", "NotFound"}, 1)
	assertCounterEventually(t, metrics.PodRequestsTotal, []string{"v1alpha1", "Canceled"}, 1)

	assertCounterEventually(t, metrics.PodRequestsList, []string{"v1alpha1", "OK"}, 1)
	assertCounterEventually(t, metrics.PodRequestsList, []string{"v1alpha1", "FailedPrecondition"}, 1)

	assertCounterEventually(t, metrics.PodRequestsGet, []string{"v1alpha1", "OK"}, 1)
	assertCounterEventually(t, metrics.PodRequestsGet, []string{"v1alpha1", "NotFound"}, 1)

	assertCounterEventually(t, metrics.PodRequestsWatch, []string{"v1alpha1", "Canceled"}, 1)
}

// assertCounterEventually 轮询等待带指定标签的计数器指标达到期望值。
func assertCounterEventually(t *testing.T, vec *compmetrics.CounterVec, labelVals []string, expected float64) {
	require.Eventually(t, func() bool {
		metric, err := vec.GetMetricWithLabelValues(labelVals...)
		if err != nil {
			return false
		}
		val, err := testutil.GetCounterMetricValue(metric)
		if err != nil {
			return false
		}
		return val == expected
	}, 2*time.Second, 10*time.Millisecond, "Metric %v with labels %v expected %f", vec, labelVals, expected)
}

// TestInitializationCheck 验证在 sources 未就绪时，ListPods、GetPod 与 WatchPods 均返回 FailedPrecondition。
func TestInitializationCheck(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodsAPI, true)
	tCtx := ktesting.Init(t)
	broadcaster := podsapi.NewBroadcaster(tCtx)
	mockManager := new(kubepodtest.MockManager)
	mockStatus := new(statustest.MockPodStatusProvider)

	// 创建未就绪的 FakeSourcesReady
	fakeSources := &FakeSourcesReady{ready: false}

	// 使用 NewPodsServer（而非 ForTest）以传入 fakeSources
	server := podsapi.NewPodsServer(broadcaster, mockManager, mockStatus, fakeSources)

	// ListPods 应返回 FAILED_PRECONDITION
	_, err := server.ListPods(tCtx, &podsv1alpha1.ListPodsRequest{})
	assertErrorWithCode(t, err, codes.FailedPrecondition, "Kubelet is initializing")

	// GetPod 应返回 FAILED_PRECONDITION
	_, err = server.GetPod(tCtx, &podsv1alpha1.GetPodRequest{PodUID: "pod1-uid"})
	assertErrorWithCode(t, err, codes.FailedPrecondition, "Kubelet is initializing")

	// WatchPods 应返回 FAILED_PRECONDITION
	mockStream := &MockWatchPodsServer{Ctx: tCtx, EventCh: make(chan *podsv1alpha1.WatchPodsEvent)}
	err = server.WatchPods(&podsv1alpha1.WatchPodsRequest{}, mockStream)
	assertErrorWithCode(t, err, codes.FailedPrecondition, "Kubelet is initializing")
}

// TestUnverifiedReadyStatusOverride 验证当 statusManager 尚无该 Pod 的状态时，
// GetPod 会把过期的 Ready=True 覆盖为 False（ContainersNotReady），避免过期状态泄漏给客户端。
func TestUnverifiedReadyStatusOverride(t *testing.T) {
	tCtx := ktesting.Init(t)

	broadcaster := podsapi.NewBroadcaster(tCtx)
	mockManager := new(kubepodtest.MockManager)
	mockStatus := new(statustest.MockPodStatusProvider)
	server := podsapi.NewPodsServer(broadcaster, mockManager, mockStatus, nil)

	// podManager 中的 Pod 持有来自 APIServer 的过期 Ready=True 状态
	stalePod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "stale-pod",
			Namespace: "default",
			UID:       "stale-uid",
		},
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
			Conditions: []v1.PodCondition{
				{
					Type:   v1.PodReady,
					Status: v1.ConditionTrue,
				},
			},
		},
	}

	mockManager.On("GetPodByUID", types.UID("stale-uid")).Return(stalePod, true)
	// statusManager 尚未对该 Pod 执行过状态更新
	mockStatus.On("GetPodStatus", types.UID("stale-uid")).Return(v1.PodStatus{}, false)

	resp, err := server.GetPod(tCtx, &podsv1alpha1.GetPodRequest{PodUID: "stale-uid"})
	require.NoError(t, err)

	podOut := &v1.Pod{}
	err = podOut.Unmarshal(resp.Pod)
	require.NoError(t, err)

	// 验证 PodReady 条件被覆盖为 False（ContainersNotReady）
	readyCondFound := false
	for _, c := range podOut.Status.Conditions {
		if c.Type == v1.PodReady {
			readyCondFound = true
			assert.Equal(t, v1.ConditionFalse, c.Status)
			assert.Equal(t, "ContainersNotReady", c.Reason)
		}
	}
	assert.True(t, readyCondFound, "PodReady condition should be present")
}

// FakeSourcesReady 是 config.SourcesReady 的测试替身，用 ready 标志模拟就绪状态。
type FakeSourcesReady struct {
	ready bool
}

// AllReady 返回 fake 的就绪状态。
func (f *FakeSourcesReady) AllReady() bool {
	return f.ready
}

// AddSource 为空实现，fake 不跟踪具体来源。
func (f *FakeSourcesReady) AddSource(source string) {}

// assertErrorWithCode 断言错误存在、可解析为 gRPC 状态，且状态码与消息均符合预期。
func assertErrorWithCode(t *testing.T, err error, expectedCode codes.Code, expectedMsg string) {
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, expectedCode, st.Code())
	assert.Contains(t, st.Message(), expectedMsg)
}
