/*
Copyright 2016 The Kubernetes Authors.

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

package garbagecollector

import (
	"context"
	goerrors "errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/time/rate"

	"k8s.io/klog/v2"
	"k8s.io/utils/lru"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"

	_ "k8s.io/kubernetes/pkg/apis/core/install"
	"k8s.io/kubernetes/pkg/controller/garbagecollector/metaonly"
	"k8s.io/utils/ptr"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/meta/testrestmapper"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/metadata"
	fakemetadata "k8s.io/client-go/metadata/fake"
	"k8s.io/client-go/metadata/metadatainformer"
	restclient "k8s.io/client-go/rest"
	clientgotesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	metricsutil "k8s.io/component-base/metrics/testutil"
	"k8s.io/controller-manager/pkg/informerfactory"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	c "k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/garbagecollector/metrics"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// testRESTMapper 包装 meta.RESTMapper，仅为了覆盖 Reset 方法，
// 让测试可以在多个用例之间重置静态映射器的缓存。
type testRESTMapper struct {
	meta.RESTMapper
}

// Reset 重置底层 RESTMapper 的缓存，避免测试间的发现结果相互污染。
func (m *testRESTMapper) Reset() {
	//nolint:logcheck // Context-awareness not important for existing tests.
	meta.MaybeResetRESTMapper(m.RESTMapper)
}

// TestGarbageCollectorConstruction 验证 GC 构造时不会为未知（非核心）资源创建监视器，
// 并且 resyncMonitors 在 Run 前后都能按目标资源集合正确增删监视器。
func TestGarbageCollectorConstruction(t *testing.T) {
	config := &restclient.Config{}
	tweakableRM := meta.NewDefaultRESTMapper(nil)
	rm := &testRESTMapper{meta.MultiRESTMapper{tweakableRM, testrestmapper.TestOnlyStaticRESTMapper(legacyscheme.Scheme)}}
	metadataClient, err := metadata.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}

	podResource := map[schema.GroupVersionResource]struct{}{
		{Version: "v1", Resource: "pods"}: {},
	}
	twoResources := map[schema.GroupVersionResource]struct{}{
		{Version: "v1", Resource: "pods"}:                     {},
		{Group: "tpr.io", Version: "v1", Resource: "unknown"}: {},
	}
	client := fake.NewSimpleClientset()

	sharedInformers := informers.NewSharedInformerFactory(client, 0)
	metadataInformers := metadatainformer.NewSharedInformerFactory(metadataClient, 0)
	// 非核心资源没有对应的监视器实现，GC 构造应当跳过它而不是失败。
	alwaysStarted := make(chan struct{})
	close(alwaysStarted)
	logger, tCtx := ktesting.NewTestContext(t)
	gc, err := NewGarbageCollector(tCtx, client, metadataClient, rm, map[schema.GroupResource]struct{}{},
		informerfactory.NewInformerFactory(sharedInformers, metadataInformers), alwaysStarted)
	if err != nil {
		t.Fatal(err)
	}
	assert.Empty(t, gc.dependencyGraphBuilder.monitors)

	// 确认监视器同步会按当前资源集合创建并停止相应的监视器。
	tweakableRM.Add(schema.GroupVersionKind{Group: "tpr.io", Version: "v1", Kind: "unknown"}, nil)
	err = gc.resyncMonitors(logger, twoResources)
	if err != nil {
		t.Errorf("Failed adding a monitor: %v", err)
	}
	assert.Len(t, gc.dependencyGraphBuilder.monitors, 2)

	err = gc.resyncMonitors(logger, podResource)
	if err != nil {
		t.Errorf("Failed removing a monitor: %v", err)
	}
	assert.Len(t, gc.dependencyGraphBuilder.monitors, 1)

	go gc.Run(tCtx, 1, 5*time.Second)

	err = gc.resyncMonitors(logger, twoResources)
	if err != nil {
		t.Errorf("Failed adding a monitor: %v", err)
	}
	assert.Len(t, gc.dependencyGraphBuilder.monitors, 2)

	err = gc.resyncMonitors(logger, podResource)
	if err != nil {
		t.Errorf("Failed removing a monitor: %v", err)
	}
	assert.Len(t, gc.dependencyGraphBuilder.monitors, 1)
}

// fakeAction 记录一次 HTTP 请求的关键信息，便于测试断言请求是否符合预期。
type fakeAction struct {
	method string
	path   string
	query  string
}

// String 返回 method=path，用于和期望的动作集合做比较。
func (f *fakeAction) String() string {
	return strings.Join([]string{f.method, f.path}, "=")
}

// FakeResponse 描述某个 method+path 应返回的 HTTP 状态码与响应体。
type FakeResponse struct {
	statusCode int
	content    []byte
}

// fakeActionHandler 按 method+path 返回预先配置的响应，同时记录收到的所有请求。
type fakeActionHandler struct {
	// response 保存不同 method + path 对应的状态码与响应内容。
	response map[string]FakeResponse

	lock    sync.Mutex
	actions []fakeAction
}

// ServeHTTP 记录请求并返回对应的状态码；对 watch 请求还会劫持连接并保持打开，
// 以模拟一个长期存在的 watch 流。
func (f *fakeActionHandler) ServeHTTP(response http.ResponseWriter, request *http.Request) {
	func() {
		f.lock.Lock()
		defer f.lock.Unlock()

		f.actions = append(f.actions, fakeAction{method: request.Method, path: request.URL.Path, query: request.URL.RawQuery})
		// 若没有为该请求配置响应，则返回一个空的 List，让依赖列表接口的调用方继续运行。
		fakeResponse, ok := f.response[request.Method+request.URL.Path]
		if !ok {
			fakeResponse.statusCode = 200
			fakeResponse.content = []byte(`{"apiVersion": "v1", "kind": "List"}`)
		}
		response.Header().Set("Content-Type", "application/json")
		response.WriteHeader(fakeResponse.statusCode)
		response.Write(fakeResponse.content)
	}()

	// 对 watch 请求劫持连接并保持打开，模拟一个长期存在的 watch 流；
	// 这样监视器不会因为连接立即关闭而不断重连，干扰对请求动作的断言。
	if strings.Contains(request.URL.RawQuery, "watch=true") {
		hijacker, ok := response.(http.Hijacker)
		if !ok {
			return
		}
		connection, _, err := hijacker.Hijack()
		if err != nil {
			return
		}
		defer connection.Close()
		time.Sleep(30 * time.Second)
	}
}

// testServerAndClientConfig 启动一个使用给定 handler 的测试服务器，
// 并返回指向它的客户端配置。
func testServerAndClientConfig(handler func(http.ResponseWriter, *http.Request)) (*httptest.Server, *restclient.Config) {
	srv := httptest.NewServer(http.HandlerFunc(handler))
	config := &restclient.Config{
		Host: srv.URL,
	}
	return srv, config
}

// garbageCollector 将 GC 实例与停止 informer 的通道打包，便于测试统一释放资源。
type garbageCollector struct {
	*GarbageCollector
	stop chan struct{}
}

// setupGC 用给定的客户端配置构造一个 GC，并启动其 informer；
// 调用方负责在结束时关闭返回的 stop 通道。
func setupGC(t *testing.T, config *restclient.Config) garbageCollector {
	_, ctx := ktesting.NewTestContext(t)
	metadataClient, err := metadata.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}

	client := fake.NewSimpleClientset()
	sharedInformers := informers.NewSharedInformerFactory(client, 0)
	alwaysStarted := make(chan struct{})
	close(alwaysStarted)
	gc, err := NewGarbageCollector(ctx, client, metadataClient, &testRESTMapper{testrestmapper.TestOnlyStaticRESTMapper(legacyscheme.Scheme)}, ignoredResources, sharedInformers, alwaysStarted)
	if err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	sharedInformers.Start(stop)
	return garbageCollector{gc, stop}
}

// getPod 构造一个属于 ns1、UID 固定的 Pod，供各测试作为待删除对象使用。
func getPod(podName string, ownerReferences []metav1.OwnerReference) *v1.Pod {
	return &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            podName,
			Namespace:       "ns1",
			UID:             "456",
			OwnerReferences: ownerReferences,
		},
	}
}

// serilizeOrDie 把对象序列化为 JSON；测试数据中序列化失败属于编程错误，直接终止测试。
func serilizeOrDie(t *testing.T, object interface{}) []byte {
	data, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// TestAttemptToDeleteItemDeleteObjectNotFound 验证待删除对象已从 API 服务器消失时，
// attemptToDeleteItem 会返回 enqueuedVirtualDeleteEventErr，并向 graphChanges 入队
// 一个虚拟删除事件，让依赖图能够清理残留的节点。
func TestAttemptToDeleteItemDeleteObjectNotFound(t *testing.T) {
	pod := getPod("ExternallyDeletedPod", []metav1.OwnerReference{
		{
			Kind:       "ReplicationController",
			Name:       "owner1",
			UID:        "123",
			APIVersion: "v1",
		},
	})
	testHandler := &fakeActionHandler{
		response: map[string]FakeResponse{
			"GET" + "/api/v1/namespaces/ns1/replicationcontrollers/owner1": {
				404,
				[]byte{},
			},
			"GET" + "/api/v1/namespaces/ns1/pods/ExternallyDeletedPod": {
				200,
				serilizeOrDie(t, pod),
			},
			"DELETE" + "/api/v1/namespaces/ns1/pods/ExternallyDeletedPod": {
				404,
				[]byte{},
			},
		},
	}
	srv, clientConfig := testServerAndClientConfig(testHandler.ServeHTTP)
	defer srv.Close()

	gc := setupGC(t, clientConfig)
	defer close(gc.stop)

	item := &node{
		identity: objectReference{
			OwnerReference: metav1.OwnerReference{
				Kind:       pod.Kind,
				APIVersion: pod.APIVersion,
				Name:       pod.Name,
				UID:        pod.UID,
			},
			Namespace: pod.Namespace,
		},
		owners: nil,
	}

	err := gc.attemptToDeleteItem(context.TODO(), item)
	if !goerrors.Is(err, enqueuedVirtualDeleteEventErr) {
		t.Errorf("expected enqueuedVirtualDeleteEventErr, got: %v", err)
	}
	if gc.dependencyGraphBuilder.graphChanges.Len() == 0 {
		t.Errorf("expected a virtual delete event to be enqueued in graphChanges, but the queue is empty")
	}
}

// TestAttemptToDeleteItemDeleteObjectNotFoundWaitingForDependents 验证当对象的属主
// 正在等待依赖者删除（带 foreground finalizer）时，即便对象已不存在，
// 仍会入队虚拟删除事件以推动依赖图收敛。
func TestAttemptToDeleteItemDeleteObjectNotFoundWaitingForDependents(t *testing.T) {
	pod := getPod("ExternallyDeletedPodFG", []metav1.OwnerReference{
		{
			Kind:               "ReplicationController",
			Name:               "owner1",
			UID:                "123",
			APIVersion:         "v1",
			BlockOwnerDeletion: func() *bool { b := true; return &b }(),
		},
	})
	owner := &v1.ReplicationController{
		TypeMeta: metav1.TypeMeta{
			Kind:       "ReplicationController",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:              "owner1",
			Namespace:         "ns1",
			UID:               "123",
			DeletionTimestamp: func() *metav1.Time { t := metav1.Now(); return &t }(),
			Finalizers:        []string{metav1.FinalizerDeleteDependents},
		},
	}
	testHandler := &fakeActionHandler{
		response: map[string]FakeResponse{
			"GET" + "/api/v1/namespaces/ns1/replicationcontrollers/owner1": {
				200,
				serilizeOrDie(t, owner),
			},
			"GET" + "/api/v1/namespaces/ns1/pods/ExternallyDeletedPodFG": {
				200,
				serilizeOrDie(t, pod),
			},
			"DELETE" + "/api/v1/namespaces/ns1/pods/ExternallyDeletedPodFG": {
				404,
				[]byte{},
			},
		},
	}
	srv, clientConfig := testServerAndClientConfig(testHandler.ServeHTTP)
	defer srv.Close()

	gc := setupGC(t, clientConfig)
	defer close(gc.stop)

	item := &node{
		identity: objectReference{
			OwnerReference: metav1.OwnerReference{
				Kind:       pod.Kind,
				APIVersion: pod.APIVersion,
				Name:       pod.Name,
				UID:        pod.UID,
			},
			Namespace: pod.Namespace,
		},
		owners: nil,
	}

	err := gc.attemptToDeleteItem(context.TODO(), item)
	if !goerrors.Is(err, enqueuedVirtualDeleteEventErr) {
		t.Errorf("expected enqueuedVirtualDeleteEventErr, got: %v", err)
	}
	if gc.dependencyGraphBuilder.graphChanges.Len() == 0 {
		t.Errorf("expected a virtual delete event to be enqueued in graphChanges, but the queue is empty")
	}
}

// TestAttemptToDeleteItem 验证 attemptToDeleteItem 会先从服务器获取最新对象，
// 随后对其发起删除，并产生预期的请求序列。
func TestAttemptToDeleteItem(t *testing.T) {
	pod := getPod("ToBeDeletedPod", []metav1.OwnerReference{
		{
			Kind:       "ReplicationController",
			Name:       "owner1",
			UID:        "123",
			APIVersion: "v1",
		},
	})
	testHandler := &fakeActionHandler{
		response: map[string]FakeResponse{
			"GET" + "/api/v1/namespaces/ns1/replicationcontrollers/owner1": {
				404,
				[]byte{},
			},
			"GET" + "/api/v1/namespaces/ns1/pods/ToBeDeletedPod": {
				200,
				serilizeOrDie(t, pod),
			},
		},
	}
	srv, clientConfig := testServerAndClientConfig(testHandler.ServeHTTP)
	defer srv.Close()

	gc := setupGC(t, clientConfig)
	defer close(gc.stop)

	item := &node{
		identity: objectReference{
			OwnerReference: metav1.OwnerReference{
				Kind:       pod.Kind,
				APIVersion: pod.APIVersion,
				Name:       pod.Name,
				UID:        pod.UID,
			},
			Namespace: pod.Namespace,
		},
		// 故意不填 owners，以验证 attemptToDeleteItem 会从服务器重新获取最新对象。
		owners:  nil,
		virtual: true,
	}
	err := gc.attemptToDeleteItem(context.TODO(), item)
	if err != nil {
		t.Errorf("Unexpected Error: %v", err)
	}
	if !item.virtual {
		t.Errorf("attemptToDeleteItem changed virtual to false unexpectedly")
	}
	expectedActionSet := sets.NewString()
	expectedActionSet.Insert("GET=/api/v1/namespaces/ns1/replicationcontrollers/owner1")
	expectedActionSet.Insert("DELETE=/api/v1/namespaces/ns1/pods/ToBeDeletedPod")
	expectedActionSet.Insert("GET=/api/v1/namespaces/ns1/pods/ToBeDeletedPod")

	actualActionSet := sets.NewString()
	for _, action := range testHandler.actions {
		actualActionSet.Insert(action.String())
	}
	if !expectedActionSet.Equal(actualActionSet) {
		t.Errorf("expected actions:\n%v\n but got:\n%v\nDifference:\n%v", expectedActionSet,
			actualActionSet, expectedActionSet.Difference(actualActionSet))
	}
}

// verifyGraphInvariants 校验依赖图的一致性：节点的每个属主都把它列为 dependents，
// 反过来节点的每个 dependent 也把它列为属主。uidToNode 包含图中所有节点。
func verifyGraphInvariants(scenario string, uidToNode map[types.UID]*node, t *testing.T) {
	for myUID, node := range uidToNode {
		for dependentNode := range node.dependents {
			found := false
			for _, owner := range dependentNode.owners {
				if owner.UID == myUID {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("scenario: %s: node %s has node %s as a dependent, but it's not present in the latter node's owners list", scenario, node.identity, dependentNode.identity)
			}
		}

		for _, owner := range node.owners {
			ownerNode, ok := uidToNode[owner.UID]
			if !ok {
				// 属主节点可能尚未被加入图中，这种情况是允许的。
				continue
			}
			if _, ok := ownerNode.dependents[node]; !ok {
				t.Errorf("node %s has node %s as an owner, but it's not present in the latter node's dependents list", node.identity, ownerNode.identity)
			}
		}
	}
}

// createEvent 构造一个只带 UID 与属主引用的 Pod 事件，供事件处理测试使用。
func createEvent(eventType eventType, selfUID string, owners []string) event {
	var ownerReferences []metav1.OwnerReference
	for i := 0; i < len(owners); i++ {
		ownerReferences = append(ownerReferences, metav1.OwnerReference{UID: types.UID(owners[i])})
	}
	return event{
		eventType: eventType,
		obj: &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				UID:             types.UID(selfUID),
				OwnerReferences: ownerReferences,
			},
		},
	}
}

// TestProcessEvent 验证 GraphBuilder 处理增删、更新事件后依赖图的不变性，
// 包括乱序事件、删除中间节点以及属主引用变更的场景。
func TestProcessEvent(t *testing.T) {
	var testScenarios = []struct {
		name string
		// 依次送入 GraphBuilder.graphChanges 的事件序列。
		events []event
	}{
		{
			name: "test1",
			events: []event{
				createEvent(addEvent, "1", []string{}),
				createEvent(addEvent, "2", []string{"1"}),
				createEvent(addEvent, "3", []string{"1", "2"}),
			},
		},
		{
			name: "test2",
			events: []event{
				createEvent(addEvent, "1", []string{}),
				createEvent(addEvent, "2", []string{"1"}),
				createEvent(addEvent, "3", []string{"1", "2"}),
				createEvent(addEvent, "4", []string{"2"}),
				createEvent(deleteEvent, "2", []string{"doesn't matter"}),
			},
		},
		{
			name: "test3",
			events: []event{
				createEvent(addEvent, "1", []string{}),
				createEvent(addEvent, "2", []string{"1"}),
				createEvent(addEvent, "3", []string{"1", "2"}),
				createEvent(addEvent, "4", []string{"3"}),
				createEvent(updateEvent, "2", []string{"4"}),
			},
		},
		{
			name: "reverse test2",
			events: []event{
				createEvent(addEvent, "4", []string{"2"}),
				createEvent(addEvent, "3", []string{"1", "2"}),
				createEvent(addEvent, "2", []string{"1"}),
				createEvent(addEvent, "1", []string{}),
				createEvent(deleteEvent, "2", []string{"doesn't matter"}),
			},
		},
	}

	alwaysStarted := make(chan struct{})
	close(alwaysStarted)
	for _, scenario := range testScenarios {
		logger, _ := ktesting.NewTestContext(t)

		dependencyGraphBuilder := &GraphBuilder{
			informersStarted: alwaysStarted,
			graphChanges:     workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[*event]()),
			uidToNode: &concurrentUIDToNode{
				uidToNodeLock: sync.RWMutex{},
				uidToNode:     make(map[types.UID]*node),
			},
			attemptToDelete:  workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[*node]()),
			absentOwnerCache: NewReferenceCache(2),
		}
		for i := 0; i < len(scenario.events); i++ {
			dependencyGraphBuilder.graphChanges.Add(&scenario.events[i])
			dependencyGraphBuilder.processGraphChanges(logger)
			// 每处理一个事件都校验一次，便于定位是哪一步破坏了图的一致性。
			verifyGraphInvariants(scenario.name, dependencyGraphBuilder.uidToNode.uidToNode, t)
		}
	}
}

// BenchmarkReferencesDiffs 测量 referencesDiffs 计算属主引用增删改差异的开销。
func BenchmarkReferencesDiffs(t *testing.B) {
	t.ReportAllocs()
	t.ResetTimer()
	for n := 0; n < t.N; n++ {
		old := []metav1.OwnerReference{{UID: "1"}, {UID: "2"}}
		new := []metav1.OwnerReference{{UID: "2"}, {UID: "3"}}
		referencesDiffs(old, new)
	}
}

// TestDependentsRace 借助 Go 的竞态检测器检查 dependents 字段上的并发访问安全：
// 一个 goroutine 反复添加/移除 dependent，另一个并发处理 orphan 操作。
func TestDependentsRace(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)

	gc := setupGC(t, &restclient.Config{})
	defer close(gc.stop)

	const updates = 100
	owner := &node{dependents: make(map[*node]struct{})}
	ownerUID := types.UID("owner")
	gc.dependencyGraphBuilder.uidToNode.Write(owner)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < updates; i++ {
			dependent := &node{}
			gc.dependencyGraphBuilder.addDependentToOwners(logger, dependent, []metav1.OwnerReference{{UID: ownerUID}})
			gc.dependencyGraphBuilder.removeDependentFromOwners(dependent, []metav1.OwnerReference{{UID: ownerUID}})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < updates; i++ {
			gc.attemptToOrphan.Add(owner)
			gc.processAttemptToOrphanWorker(logger)
		}
	}()
	wg.Wait()
}

// podToGCNode 把 Pod 转换成 GC 图中的节点表示，只保留身份信息；
// owners 留空以触发从服务器获取最新对象。
func podToGCNode(pod *v1.Pod) *node {
	return &node{
		identity: objectReference{
			OwnerReference: metav1.OwnerReference{
				Kind:       pod.Kind,
				APIVersion: pod.APIVersion,
				Name:       pod.Name,
				UID:        pod.UID,
			},
			Namespace: pod.Namespace,
		},
		// 故意不填 owners，以验证 attemptToDeleteItem 会从服务器重新获取最新对象。
		owners: nil,
	}
}

// TestAbsentOwnerCache 验证不存在的属主会被缓存，命中缓存时不会再向服务器发请求，
// 并确认容量为 2 的 LRU 缓存会淘汰最久未使用的条目。
func TestAbsentOwnerCache(t *testing.T) {
	rc1Pod1 := getPod("rc1Pod1", []metav1.OwnerReference{
		{
			Kind:       "ReplicationController",
			Name:       "rc1",
			UID:        "1",
			APIVersion: "v1",
			Controller: ptr.To(true),
		},
	})
	rc1Pod2 := getPod("rc1Pod2", []metav1.OwnerReference{
		{
			Kind:       "ReplicationController",
			Name:       "rc1",
			UID:        "1",
			APIVersion: "v1",
			Controller: ptr.To(false),
		},
	})
	rc2Pod1 := getPod("rc2Pod1", []metav1.OwnerReference{
		{
			Kind:       "ReplicationController",
			Name:       "rc2",
			UID:        "2",
			APIVersion: "v1",
		},
	})
	rc3Pod1 := getPod("rc3Pod1", []metav1.OwnerReference{
		{
			Kind:       "ReplicationController",
			Name:       "rc3",
			UID:        "3",
			APIVersion: "v1",
		},
	})
	testHandler := &fakeActionHandler{
		response: map[string]FakeResponse{
			"GET" + "/api/v1/namespaces/ns1/pods/rc1Pod1": {
				200,
				serilizeOrDie(t, rc1Pod1),
			},
			"GET" + "/api/v1/namespaces/ns1/pods/rc1Pod2": {
				200,
				serilizeOrDie(t, rc1Pod2),
			},
			"GET" + "/api/v1/namespaces/ns1/pods/rc2Pod1": {
				200,
				serilizeOrDie(t, rc2Pod1),
			},
			"GET" + "/api/v1/namespaces/ns1/pods/rc3Pod1": {
				200,
				serilizeOrDie(t, rc3Pod1),
			},
			"GET" + "/api/v1/namespaces/ns1/replicationcontrollers/rc1": {
				404,
				[]byte{},
			},
			"GET" + "/api/v1/namespaces/ns1/replicationcontrollers/rc2": {
				404,
				[]byte{},
			},
			"GET" + "/api/v1/namespaces/ns1/replicationcontrollers/rc3": {
				404,
				[]byte{},
			},
		},
	}
	srv, clientConfig := testServerAndClientConfig(testHandler.ServeHTTP)
	defer srv.Close()
	gc := setupGC(t, clientConfig)
	defer close(gc.stop)
	gc.absentOwnerCache = NewReferenceCache(2)
	gc.attemptToDeleteItem(context.TODO(), podToGCNode(rc1Pod1))
	gc.attemptToDeleteItem(context.TODO(), podToGCNode(rc2Pod1))
	// rc1 已在缓存中，这次不应再发请求；同时 rc1 会在 LRU 中被提升到最近使用。
	gc.attemptToDeleteItem(context.TODO(), podToGCNode(rc1Pod2))
	// 这次之后，最久未使用的 rc2 应当被淘汰。
	gc.attemptToDeleteItem(context.TODO(), podToGCNode(rc3Pod1))
	// 校验缓存内容。
	if !gc.absentOwnerCache.Has(objectReference{Namespace: "ns1", OwnerReference: metav1.OwnerReference{Kind: "ReplicationController", Name: "rc1", UID: "1", APIVersion: "v1"}}) {
		t.Errorf("expected rc1 to be in the cache")
	}
	if gc.absentOwnerCache.Has(objectReference{Namespace: "ns1", OwnerReference: metav1.OwnerReference{Kind: "ReplicationController", Name: "rc2", UID: "2", APIVersion: "v1"}}) {
		t.Errorf("expected rc2 to not exist in the cache")
	}
	if !gc.absentOwnerCache.Has(objectReference{Namespace: "ns1", OwnerReference: metav1.OwnerReference{Kind: "ReplicationController", Name: "rc3", UID: "3", APIVersion: "v1"}}) {
		t.Errorf("expected rc3 to be in the cache")
	}
	// 校验实际发出的请求数：rc1 只应被请求一次。
	count := 0
	for _, action := range testHandler.actions {
		if action.String() == "GET=/api/v1/namespaces/ns1/replicationcontrollers/rc1" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected only 1 GET rc1 request, got %d", count)
	}
}

// TestDeleteOwnerRefPatch 验证 GenerateDeleteOwnerRefStrategicMergeBytes 生成的
// strategic merge patch 能够正确移除指定的属主引用，同时保留其余引用。
func TestDeleteOwnerRefPatch(t *testing.T) {
	original := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: "100",
			OwnerReferences: []metav1.OwnerReference{
				{UID: "1"},
				{UID: "2"},
				{UID: "3"},
			},
		},
	}
	originalData := serilizeOrDie(t, original)
	expected := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: "100",
			OwnerReferences: []metav1.OwnerReference{
				{UID: "1"},
			},
		},
	}
	p, err := c.GenerateDeleteOwnerRefStrategicMergeBytes("100", []types.UID{"2", "3"})
	if err != nil {
		t.Fatal(err)
	}
	patched, err := strategicpatch.StrategicMergePatch(originalData, p, v1.Pod{})
	if err != nil {
		t.Fatal(err)
	}
	var got v1.Pod
	if err := json.Unmarshal(patched, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expected, got) {
		t.Errorf("expected: %#v,\ngot: %#v", expected, got)
	}
}

// TestUnblockOwnerReference 验证解除属主删除阻塞的 patch 会把 BlockOwnerDeletion
// 为 true 的引用改为 false，而不改动其他引用。
func TestUnblockOwnerReference(t *testing.T) {
	trueVar := true
	falseVar := false
	original := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: "100",
			OwnerReferences: []metav1.OwnerReference{
				{UID: "1", BlockOwnerDeletion: &trueVar},
				{UID: "2", BlockOwnerDeletion: &falseVar},
				{UID: "3"},
			},
		},
	}
	originalData := serilizeOrDie(t, original)
	expected := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID: "100",
			OwnerReferences: []metav1.OwnerReference{
				{UID: "1", BlockOwnerDeletion: &falseVar},
				{UID: "2", BlockOwnerDeletion: &falseVar},
				{UID: "3"},
			},
		},
	}
	accessor, err := meta.Accessor(&original)
	if err != nil {
		t.Fatal(err)
	}
	n := node{
		owners: accessor.GetOwnerReferences(),
	}
	patch, err := n.unblockOwnerReferencesStrategicMergePatch()
	if err != nil {
		t.Fatal(err)
	}
	patched, err := strategicpatch.StrategicMergePatch(originalData, patch, v1.Pod{})
	if err != nil {
		t.Fatal(err)
	}
	var got v1.Pod
	if err := json.Unmarshal(patched, &got); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(expected, got) {
		t.Errorf("expected: %#v,\ngot: %#v", expected, got)
		t.Errorf("expected: %#v,\ngot: %#v", expected.OwnerReferences, got.OwnerReferences)
		for _, ref := range got.OwnerReferences {
			t.Errorf("ref.UID=%s, ref.BlockOwnerDeletion=%v", ref.UID, *ref.BlockOwnerDeletion)
		}
	}
}

// TestOrphanDependentsFailure 验证服务器返回 409 冲突时，orphanDependents
// 会把错误向上传播，调用方据此决定是否重试。
func TestOrphanDependentsFailure(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)

	testHandler := &fakeActionHandler{
		response: map[string]FakeResponse{
			"PATCH" + "/api/v1/namespaces/ns1/pods/pod": {
				409,
				[]byte{},
			},
		},
	}
	srv, clientConfig := testServerAndClientConfig(testHandler.ServeHTTP)
	defer srv.Close()

	gc := setupGC(t, clientConfig)
	defer close(gc.stop)

	dependents := []*node{
		{
			identity: objectReference{
				OwnerReference: metav1.OwnerReference{
					Kind:       "Pod",
					APIVersion: "v1",
					Name:       "pod",
				},
				Namespace: "ns1",
			},
		},
	}
	err := gc.orphanDependents(logger, objectReference{}, dependents)
	expected := `the server reported a conflict`
	if err == nil || !strings.Contains(err.Error(), expected) {
		if err != nil {
			t.Errorf("expected error contains text %q, got %q", expected, err.Error())
		} else {
			t.Errorf("expected error contains text %q, got nil", expected)
		}
	}
}

// TestGetDeletableResources 确保无论 discovery 返回什么结果，
// GetDeletableResources 都能返回可用的资源集合：过滤掉非法 GroupVersion
// 和缺少 delete/list/watch 动词的资源，并在部分发现失败时保留可用结果。
func TestGetDeletableResources(t *testing.T) {
	tests := map[string]struct {
		serverResources    []*metav1.APIResourceList
		err                error
		deletableResources map[schema.GroupVersionResource]struct{}
	}{
		"no error": {
			serverResources: []*metav1.APIResourceList{
				{
					// 合法的 GroupVersion。
					GroupVersion: "apps/v1",
					APIResources: []metav1.APIResource{
						{Name: "pods", Namespaced: true, Kind: "Pod", Verbs: metav1.Verbs{"delete", "list", "watch"}},
						{Name: "services", Namespaced: true, Kind: "Service"},
					},
				},
				{
					// 非法的 GroupVersion，应当被忽略。
					GroupVersion: "foo//whatever",
					APIResources: []metav1.APIResource{
						{Name: "bars", Namespaced: true, Kind: "Bar", Verbs: metav1.Verbs{"delete", "list", "watch"}},
					},
				},
				{
					// GroupVersion 合法但缺少必需的动词，应当被忽略。
					GroupVersion: "acme/v1",
					APIResources: []metav1.APIResource{
						{Name: "widgets", Namespaced: true, Kind: "Widget", Verbs: metav1.Verbs{"delete"}},
					},
				},
			},
			err: nil,
			deletableResources: map[schema.GroupVersionResource]struct{}{
				{Group: "apps", Version: "v1", Resource: "pods"}: {},
			},
		},
		"nonspecific failure, includes usable results": {
			serverResources: []*metav1.APIResourceList{
				{
					GroupVersion: "apps/v1",
					APIResources: []metav1.APIResource{
						{Name: "pods", Namespaced: true, Kind: "Pod", Verbs: metav1.Verbs{"delete", "list", "watch"}},
						{Name: "services", Namespaced: true, Kind: "Service"},
					},
				},
			},
			err: fmt.Errorf("internal error"),
			deletableResources: map[schema.GroupVersionResource]struct{}{
				{Group: "apps", Version: "v1", Resource: "pods"}: {},
			},
		},
		"partial discovery failure, includes usable results": {
			serverResources: []*metav1.APIResourceList{
				{
					GroupVersion: "apps/v1",
					APIResources: []metav1.APIResource{
						{Name: "pods", Namespaced: true, Kind: "Pod", Verbs: metav1.Verbs{"delete", "list", "watch"}},
						{Name: "services", Namespaced: true, Kind: "Service"},
					},
				},
			},
			err: &discovery.ErrGroupDiscoveryFailed{
				Groups: map[schema.GroupVersion]error{
					{Group: "foo", Version: "v1"}: fmt.Errorf("discovery failure"),
				},
			},
			deletableResources: map[schema.GroupVersionResource]struct{}{
				{Group: "apps", Version: "v1", Resource: "pods"}: {},
			},
		},
		"discovery failure, no results": {
			serverResources:    nil,
			err:                fmt.Errorf("internal error"),
			deletableResources: map[schema.GroupVersionResource]struct{}{},
		},
	}

	_, ctx := ktesting.NewTestContext(t)
	for name, test := range tests {
		t.Logf("testing %q", name)
		client := &fakeServerResources{
			PreferredResources: test.serverResources,
			Error:              test.err,
		}
		actual, actualErr := GetDeletableResources(ctx, discovery.ToServerResourcesInterfaceWithContext(client))
		if !reflect.DeepEqual(test.deletableResources, actual) {
			t.Errorf("expected resources:\n%v\ngot:\n%v", test.deletableResources, actual)
		}
		if !reflect.DeepEqual(test.err, actualErr) {
			t.Errorf("expected error:\n%v\ngot:\n%v", test.err, actualErr)
		}
	}
}

// wrappedKubeClientWithUnsupportedWatchListSemantics 包装客户端并声明不支持
// watch-list 语义，迫使 GC 走逐资源 list+watch 的旧路径。
type wrappedKubeClientWithUnsupportedWatchListSemantics struct {
	kubernetes.Interface
}

// IsWatchListSemanticsUnSupported 返回 true，表示客户端不支持 watch-list。
func (c *wrappedKubeClientWithUnsupportedWatchListSemantics) IsWatchListSemanticsUnSupported() bool {
	return true
}

// TestGarbageCollectorSync 确保 discovery 客户端报错或 informer 同步失败时，
// GC 的 Sync 不会永久阻塞，并能在故障恢复后重建正确的监视器集合。
func TestGarbageCollectorSync(t *testing.T) {
	serverResources := []*metav1.APIResourceList{
		{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "pods", Namespaced: true, Kind: "Pod", Verbs: metav1.Verbs{"delete", "list", "watch"}},
			},
		},
		{
			GroupVersion: "apps/v1",
			APIResources: []metav1.APIResource{
				{Name: "deployments", Namespaced: true, Kind: "Deployment", Verbs: metav1.Verbs{"delete", "list", "watch"}},
			},
		},
	}
	appsV1Error := &discovery.ErrGroupDiscoveryFailed{Groups: map[schema.GroupVersion]error{{Group: "apps", Version: "v1"}: fmt.Errorf(":-/")}}

	unsyncableServerResources := []*metav1.APIResourceList{
		{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "pods", Namespaced: true, Kind: "Pod", Verbs: metav1.Verbs{"delete", "list", "watch"}},
				{Name: "secrets", Namespaced: true, Kind: "Secret", Verbs: metav1.Verbs{"delete", "list", "watch"}},
			},
		},
	}
	fakeDiscoveryClient := &fakeServerResources{
		PreferredResources: serverResources,
		Error:              nil,
		Lock:               sync.Mutex{},
	}

	testHandler := &fakeActionHandler{
		response: map[string]FakeResponse{
			"GET" + "/api/v1/pods": {
				200,
				[]byte("{}"),
			},
			"GET" + "/apis/apps/v1/deployments": {
				200,
				[]byte("{}"),
			},
			"GET" + "/api/v1/secrets": {
				404,
				[]byte("{}"),
			},
		},
	}

	testHandler2 := &fakeActionHandler{
		response: map[string]FakeResponse{
			"GET" + "/api/v1/secrets": {
				200,
				[]byte("{}"),
			},
		},
	}
	var secretSyncOK atomic.Bool
	var alternativeTestHandler = func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/api/v1/secrets" && secretSyncOK.Load() {
			testHandler2.ServeHTTP(response, request)
			return
		}
		testHandler.ServeHTTP(response, request)
	}
	srv, clientConfig := testServerAndClientConfig(alternativeTestHandler)
	defer srv.Close()
	clientConfig.ContentConfig.NegotiatedSerializer = nil
	kubeClient, err := kubernetes.NewForConfig(clientConfig)
	if err != nil {
		t.Fatal(err)
	}
	// TODO(#115478): migrate this test to use fakeClient instead of the real client.
	// 这里用真实客户端配合测试服务器，通过 handler 切换来模拟 informer 同步失败。
	client := &wrappedKubeClientWithUnsupportedWatchListSemantics{kubeClient}

	tweakableRM := meta.NewDefaultRESTMapper(nil)
	tweakableRM.AddSpecific(schema.GroupVersionKind{Version: "v1", Kind: "Pod"}, schema.GroupVersionResource{Version: "v1", Resource: "pods"}, schema.GroupVersionResource{Version: "v1", Resource: "pod"}, meta.RESTScopeNamespace)
	tweakableRM.AddSpecific(schema.GroupVersionKind{Version: "v1", Kind: "Secret"}, schema.GroupVersionResource{Version: "v1", Resource: "secrets"}, schema.GroupVersionResource{Version: "v1", Resource: "secret"}, meta.RESTScopeNamespace)
	tweakableRM.AddSpecific(schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}, schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}, schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployment"}, meta.RESTScopeNamespace)
	rm := &testRESTMapper{meta.MultiRESTMapper{tweakableRM, testrestmapper.TestOnlyStaticRESTMapper(legacyscheme.Scheme)}}
	metadataClient, err := metadata.NewForConfig(clientConfig)
	if err != nil {
		t.Fatal(err)
	}

	sharedInformers := informers.NewSharedInformerFactory(client, 0)

	var wg sync.WaitGroup
	defer wg.Wait()

	tCtx := ktesting.Init(t)
	defer tCtx.Cancel("test has completed")
	logger := tCtx.Logger()

	alwaysStarted := make(chan struct{})
	close(alwaysStarted)

	gc, err := NewGarbageCollector(tCtx, client, metadataClient, rm, map[schema.GroupResource]struct{}{}, sharedInformers, alwaysStarted)
	if err != nil {
		t.Fatal(err)
	}

	syncPeriod := 200 * time.Millisecond
	wg.Go(func() {
		gc.Run(tCtx, 1, syncPeriod)
	})
	// GarbageCollector.Sync() 的伪代码：
	// GarbageCollector.Sync(client, period, stopCh):
	//    wait.Until() 以 period 为周期循环，直到 stopCh 关闭：
	//        wait.PollImmediateUntil() 以 100ms（硬编码）为周期循环，直到 stopCh 关闭：
	//            GetDeletableResources()
	//            gc.resyncMonitors()
	//            cache.WaitForNamedCacheSync() 以 syncedPollPeriod（硬编码 100ms）为周期循环，
	//            直到 period 后其停止通道关闭，或所有缓存完成同步。
	//
	// 把 period 设为 200ms，可让 WaitForCacheSync() 在每个
	// wait.PollImmediateUntil() 周期内检查约 2 次缓存同步状态。
	//
	// 测试中每次 sleep 1s，可让 GetDeletableResources 与 gc.resyncMonitors
	// 运行约 5 次，确保 fakeDiscoveryClient 的变更被感知到。
	wg.Go(func() {
		gc.Sync(tCtx, fakeDiscoveryClient, syncPeriod)
	})

	// 等待 Sync 发现初始资源集合。
	time.Sleep(1 * time.Second)

	err = expectSyncNotBlocked(fakeDiscoveryClient)
	if err != nil {
		t.Fatalf("Expected garbagecollector.Sync to still be running but it is blocked: %v", err)
	}
	assertMonitors(t, gc, "pods", "deployments")

	// 模拟 discovery 客户端返回错误。
	fakeDiscoveryClient.setPreferredResources(nil, fmt.Errorf("error calling discoveryClient.ServerPreferredResources()"))

	// 等待 Sync 感知到变更。
	time.Sleep(1 * time.Second)
	// 此时监视器不应发生变化。
	assertMonitors(t, gc, "pods", "deployments")

	// 移除错误，确认 GC 的 Sync 仍能继续工作。
	fakeDiscoveryClient.setPreferredResources(serverResources, nil)

	err = expectSyncNotBlocked(fakeDiscoveryClient)
	if err != nil {
		t.Fatalf("Expected garbagecollector.Sync to still be running but it is blocked: %v", err)
	}
	assertMonitors(t, gc, "pods", "deployments")

	// 模拟 discovery 返回一个 RESTMapper 可解析、但缓存始终无法同步的资源。
	fakeDiscoveryClient.setPreferredResources(unsyncableServerResources, nil)

	// 等待 Sync 感知到变更。
	time.Sleep(1 * time.Second)
	assertMonitors(t, gc, "pods", "secrets")

	// 恢复正常资源集合，确认 GC 的 Sync 能恢复。
	fakeDiscoveryClient.setPreferredResources(serverResources, nil)

	err = expectSyncNotBlocked(fakeDiscoveryClient)
	if err != nil {
		t.Fatalf("Expected garbagecollector.Sync to still be running but it is blocked: %v", err)
	}
	assertMonitors(t, gc, "pods", "deployments")

	// 部分 discovery 失败。
	fakeDiscoveryClient.setPreferredResources(unsyncableServerResources, appsV1Error)
	// 等待 Sync 感知到变更。
	time.Sleep(1 * time.Second)
	// deployments 的监视器仍然保留。
	assertMonitors(t, gc, "pods", "deployments", "secrets")

	// 恢复正常资源集合，确认 GC 的 Sync 能恢复。
	fakeDiscoveryClient.setPreferredResources(serverResources, nil)
	// 等待 Sync 感知到变更。
	time.Sleep(1 * time.Second)
	err = expectSyncNotBlocked(fakeDiscoveryClient)
	if err != nil {
		t.Fatalf("Expected garbagecollector.Sync to still be running but it is blocked: %v", err)
	}
	// 无法同步的监视器已被移除。
	assertMonitors(t, gc, "pods", "deployments")

	// 模拟初始未同步的 informer，稍后再让它完成同步。
	metrics.GarbageCollectorResourcesSyncError.Reset()
	fakeDiscoveryClient.setPreferredResources(unsyncableServerResources, nil)
	time.Sleep(1 * time.Second)
	assertMonitors(t, gc, "pods", "secrets")
	if gc.IsSynced(logger) {
		t.Fatal("cache from garbage collector should not be synced")
	}
	val, _ := metricsutil.GetCounterMetricValue(metrics.GarbageCollectorResourcesSyncError)
	if val < 1 {
		t.Fatalf("expect sync error metric > 0")
	}

	// 此时 informer 已完成同步。
	secretSyncOK.Store(true)
	if err := wait.PollUntilContextTimeout(tCtx, time.Second, wait.ForeverTestTimeout, true, func(ctx context.Context) (bool, error) {
		return gc.IsSynced(logger), nil
	}); err != nil {
		t.Fatal(err)
	}
}

// assertMonitors 断言 GC 当前监视的资源集合与期望一致。
func assertMonitors(t *testing.T, gc *GarbageCollector, resources ...string) {
	t.Helper()
	expected := sets.NewString(resources...)
	actual := sets.NewString()
	for m := range gc.dependencyGraphBuilder.monitors {
		actual.Insert(m.Resource)
	}
	if !actual.Equal(expected) {
		t.Fatalf("expected monitors %v, got %v", expected.List(), actual.List())
	}
}

// expectSyncNotBlocked 在一段时间内检查 discovery 接口是否仍被调用，
// 借此判断 Sync 循环没有卡死。
func expectSyncNotBlocked(fakeDiscoveryClient *fakeServerResources) error {
	before := fakeDiscoveryClient.getInterfaceUsedCount()
	t := 1 * time.Second
	time.Sleep(t)
	after := fakeDiscoveryClient.getInterfaceUsedCount()
	if before == after {
		return fmt.Errorf("discoveryClient.ServerPreferredResources() not called over %v", t)
	}
	return nil
}

// fakeServerResources 是可控的 discovery 接口实现：返回指定的资源列表或错误，
// 并统计 ServerPreferredResources 的调用次数以便检测 Sync 是否阻塞。
type fakeServerResources struct {
	PreferredResources []*metav1.APIResourceList
	Error              error
	Lock               sync.Mutex
	InterfaceUsedCount int
}

// ServerResourcesForGroupVersion 在本测试中不会被使用，返回空结果。
func (*fakeServerResources) ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error) {
	return nil, nil
}

// ServerGroupsAndResources 在本测试中不会被使用，返回空结果。
func (*fakeServerResources) ServerGroupsAndResources() ([]*metav1.APIGroup, []*metav1.APIResourceList, error) {
	return nil, nil, nil
}

// ServerPreferredResources 返回预设的资源与错误，并统计调用次数。
func (f *fakeServerResources) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	f.Lock.Lock()
	defer f.Lock.Unlock()
	f.InterfaceUsedCount++
	return f.PreferredResources, f.Error
}

// setPreferredResources 更新 discovery 返回的资源与错误。
func (f *fakeServerResources) setPreferredResources(resources []*metav1.APIResourceList, err error) {
	f.Lock.Lock()
	defer f.Lock.Unlock()
	f.PreferredResources = resources
	f.Error = err
}

// getInterfaceUsedCount 返回 ServerPreferredResources 被调用的次数。
func (f *fakeServerResources) getInterfaceUsedCount() int {
	f.Lock.Lock()
	defer f.Lock.Unlock()
	return f.InterfaceUsedCount
}

// ServerPreferredNamespacedResources 在本测试中不会被使用，返回空结果。
func (*fakeServerResources) ServerPreferredNamespacedResources() ([]*metav1.APIResourceList, error) {
	return nil, nil
}

// TestConflictingData 通过表驱动的场景验证依赖图如何处理各种“冲突”数据：
// 虚拟节点的命名空间归一化、跨命名空间属主引用、集群作用域与命名空间作用域
// 对象之间的引用等。
func TestConflictingData(t *testing.T) {
	pod1ns1 := makeID("v1", "Pod", "ns1", "podname1", "poduid1")
	pod2ns1 := makeID("v1", "Pod", "ns1", "podname2", "poduid2")
	pod2ns2 := makeID("v1", "Pod", "ns2", "podname2", "poduid2")
	node1 := makeID("v1", "Node", "", "nodename", "nodeuid1")

	role1v1beta1 := makeID("rbac.authorization.k8s.io/v1beta1", "Role", "ns1", "role1", "roleuid1")
	role1v1 := makeID("rbac.authorization.k8s.io/v1", "Role", "ns1", "role1", "roleuid1")

	deployment1apps := makeID("apps/v1", "Deployment", "ns1", "deployment1", "deploymentuid1")
	deployment1extensions := makeID("extensions/v1beta1", "Deployment", "ns1", "deployment1", "deploymentuid1") // 该版本不再提供服务，但仍被引用

	// 当命名空间级资源引用 node1 时，插入的虚拟节点会带上该命名空间坐标
	node1WithNamespace := makeID("v1", "Node", "ns1", "nodename", "nodeuid1")

	// 当集群级资源引用 pod1 时，插入的虚拟节点不带命名空间
	pod1nonamespace := makeID("v1", "Pod", "", "podname1", "poduid1")

	badSecretReferenceWithDeploymentUID := makeID("v1", "Secret", "ns1", "secretname", string(deployment1apps.UID))
	badChildPod := makeID("v1", "Pod", "ns1", "badpod", "badpoduid")
	goodChildPod := makeID("v1", "Pod", "ns1", "goodpod", "goodpoduid")

	var testScenarios = []struct {
		name           string
		initialObjects []runtime.Object
		steps          []step
	}{
		{
			name: "good child in ns1 -> cluster-scoped owner",
			steps: []step{
				// 准备阶段
				createObjectInClient("", "v1", "nodes", "", makeMetadataObj(node1)),
				createObjectInClient("", "v1", "pods", "ns1", makeMetadataObj(pod1ns1, node1)),
				// 先观察到命名空间级子节点，其集群级属主尚未被观察到
				processEvent(makeAddEvent(pod1ns1, node1)),
				assertState(state{
					graphNodes:             []*node{makeNode(pod1ns1, withOwners(node1)), makeNode(node1WithNamespace, virtual)}, // 虚拟节点 node1（命名空间与子节点一致）
					pendingAttemptToDelete: []*node{makeNode(node1WithNamespace, virtual)},                                       // 虚拟节点 node1 仍在删除队列中
				}),
				// 处理队列中虚拟节点的删除
				processAttemptToDelete(1),
				assertState(state{
					clientActions:          []string{"get /v1, Resource=nodes name=nodename"},
					graphNodes:             []*node{makeNode(pod1ns1, withOwners(node1)), makeNode(node1WithNamespace, virtual)}, // 虚拟节点 node1（命名空间与子节点一致）
					pendingAttemptToDelete: []*node{makeNode(node1WithNamespace, virtual)},                                       // 仍未观察到 node1，因此重新入队
				}),
				// 观察到集群级属主
				processEvent(makeAddEvent(node1)),
				assertState(state{
					graphNodes:             []*node{makeNode(pod1ns1, withOwners(node1)), makeNode(node1)}, // node1 已变为真实节点，命名空间坐标得到修正
					pendingAttemptToDelete: []*node{makeNode(node1WithNamespace, virtual)},                 // 虚拟节点 node1 仍在删除队列中
				}),
				// 处理队列中虚拟节点的删除
				// 最终状态：子节点与属主都在图中，队列为空
				processAttemptToDelete(1),
				assertState(state{
					graphNodes: []*node{makeNode(pod1ns1, withOwners(node1)), makeNode(node1)},
				}),
			},
		},
		// 命名空间 A 中的子节点引用了命名空间 B 中的命名空间级属主：
		// * 应当被立即删除
		// * 应在命名空间 A 中记录事件，involvedObject 指向问题子节点以表明错误
		{
			name: "bad child in ns1 -> owner in ns2 (child first)",
			steps: []step{
				// 0,1：准备
				createObjectInClient("", "v1", "pods", "ns1", makeMetadataObj(pod1ns1, pod2ns1)),
				createObjectInClient("", "v1", "pods", "ns2", makeMetadataObj(pod2ns2)),
				// 2,3：观察命名空间级子节点，其属主尚未被观察到
				processEvent(makeAddEvent(pod1ns1, pod2ns2)),
				assertState(state{
					graphNodes:             []*node{makeNode(pod1ns1, withOwners(pod2ns2)), makeNode(pod2ns1, virtual)}, // 虚拟 pod2（命名空间与子节点一致）
					pendingAttemptToDelete: []*node{makeNode(pod2ns1, virtual)},                                         // 虚拟 pod2 已入队等待尝试删除
				}),
				// 4,5：观察到属主
				processEvent(makeAddEvent(pod2ns2)),
				assertState(state{
					graphNodes:             []*node{makeNode(pod1ns1, withOwners(pod2ns2)), makeNode(pod2ns2)}, // pod2 不再是虚拟节点，命名空间已修正
					pendingAttemptToDelete: []*node{makeNode(pod2ns1, virtual), makeNode(pod1ns1)},             // 虚拟 pod2 仍在队列中；问题子节点 pod1 因与实际属主不符也入队
					events:                 []string{`Warning OwnerRefInvalidNamespace ownerRef [v1/Pod, namespace: ns1, name: podname2, uid: poduid2] does not exist in namespace "ns1" involvedObject{kind=Pod,apiVersion=v1}`},
				}),
				// 6,7：处理队列中虚拟属主的删除
				processAttemptToDelete(1),
				assertState(state{
					graphNodes:             []*node{makeNode(pod1ns1, withOwners(pod2ns2)), makeNode(pod2ns2)},
					pendingAttemptToDelete: []*node{makeNode(pod1ns1)}, // 问题子节点 pod1 因与实际属主不符而入队
				}),
				// 8,9：处理问题子节点的删除
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get /v1, Resource=pods ns=ns1 name=podname1",    // 删除前查找 pod1
						"get /v1, Resource=pods ns=ns1 name=podname2",    // 验证错误的属主引用确实不存在
						"delete /v1, Resource=pods ns=ns1 name=podname1", // 删除 pod1
					},
					graphNodes:       []*node{makeNode(pod1ns1, withOwners(pod2ns2)), makeNode(pod2ns2)},
					absentOwnerCache: []objectReference{pod2ns1}, // 缓存错误属主的不存在状态
				}),
				// 10,11：观察第 8 步发出的删除
				// 最终状态：属主在图中，队列为空
				processEvent(makeDeleteEvent(pod1ns1)),
				assertState(state{
					graphNodes:       []*node{makeNode(pod2ns2)}, // 仅剩下合法的属主
					absentOwnerCache: []objectReference{pod2ns1}, // 缓存错误属主的不存在状态
				}),
			},
		},
		{
			name: "bad child in ns1 -> owner in ns2 (owner first)",
			steps: []step{
				// 0,1：准备
				createObjectInClient("", "v1", "pods", "ns1", makeMetadataObj(pod1ns1, pod2ns1)),
				createObjectInClient("", "v1", "pods", "ns2", makeMetadataObj(pod2ns2)),
				// 2,3：观察到属主
				processEvent(makeAddEvent(pod2ns2)),
				assertState(state{
					graphNodes: []*node{makeNode(pod2ns2)},
				}),
				// 4,5：观察到带有跨命名空间非法属主引用的子节点
				processEvent(makeAddEvent(pod1ns1, pod2ns1)),
				assertState(state{
					graphNodes:             []*node{makeNode(pod1ns1, withOwners(pod2ns1)), makeNode(pod2ns2)},
					pendingAttemptToDelete: []*node{makeNode(pod1ns1)}, // 问题子节点已入队等待尝试删除
					events:                 []string{`Warning OwnerRefInvalidNamespace ownerRef [v1/Pod, namespace: ns1, name: podname2, uid: poduid2] does not exist in namespace "ns1" involvedObject{kind=Pod,apiVersion=v1}`},
				}),
				// 6,7：处理问题子节点的删除
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get /v1, Resource=pods ns=ns1 name=podname1",    // 删除前查找 pod1
						"get /v1, Resource=pods ns=ns1 name=podname2",    // 验证错误的属主引用确实不存在
						"delete /v1, Resource=pods ns=ns1 name=podname1", // 删除 pod1
					},
					graphNodes:             []*node{makeNode(pod1ns1, withOwners(pod2ns1)), makeNode(pod2ns2)},
					pendingAttemptToDelete: []*node{},
					absentOwnerCache:       []objectReference{pod2ns1}, // 缓存错误属主的不存在状态
				}),
				// 8,9：观察第 6 步发出的删除
				// 最终状态：属主在图中，队列为空
				processEvent(makeDeleteEvent(pod1ns1)),
				assertState(state{
					graphNodes:       []*node{makeNode(pod2ns2)}, // 仅剩下合法的属主
					absentOwnerCache: []objectReference{pod2ns1}, // 缓存错误属主的不存在状态
				}),
			},
		},
		// 集群级子节点引用命名空间 B 中的命名空间级属主：
		// * 不应被删除
		// * 应在 kube-system 命名空间中记录事件，involvedObject 指向问题子节点以表明错误
		{
			name: "bad cluster-scoped child -> owner in ns1 (child first)",
			steps: []step{
				// 准备阶段
				createObjectInClient("", "v1", "nodes", "", makeMetadataObj(node1, pod1ns1)),
				createObjectInClient("", "v1", "pods", "ns1", makeMetadataObj(pod1ns1)),
				// 2,3：观察集群级子节点，其命名空间级属主尚未被观察到
				processEvent(makeAddEvent(node1, pod1ns1)),
				assertState(state{
					graphNodes:             []*node{makeNode(node1, withOwners(pod1nonamespace)), makeNode(pod1nonamespace, virtual)}, // 虚拟 pod1（不带命名空间）
					pendingAttemptToDelete: []*node{makeNode(pod1nonamespace, virtual)},                                               // 虚拟 pod1 已入队等待尝试删除
				}),
				// 4,5：处理虚拟 pod1 的删除
				processAttemptToDelete(1),
				assertState(state{
					graphNodes:             []*node{makeNode(node1, withOwners(pod1nonamespace)), makeNode(pod1nonamespace, virtual)}, // 虚拟 pod1（不带命名空间）
					pendingAttemptToDelete: []*node{},                                                                                 // 没有命名空间坐标的命名空间级虚拟对象不会重新入队
				}),
				// 6,7：观察到命名空间级属主
				processEvent(makeAddEvent(pod1ns1)),
				assertState(state{
					graphNodes:             []*node{makeNode(node1, withOwners(pod1nonamespace)), makeNode(pod1ns1)}, // pod1 的命名空间得到修正并变为真实节点
					events:                 []string{`Warning OwnerRefInvalidNamespace ownerRef [v1/Pod, namespace: , name: podname1, uid: poduid1] does not exist in namespace "" involvedObject{kind=Node,apiVersion=v1}`},
					pendingAttemptToDelete: []*node{makeNode(node1, withOwners(pod1ns1))}, // 有问题的集群级子节点被加入 attemptToDelete 队列
				}),
				// 8,9：处理队列中问题集群级子节点的删除尝试
				// 最终状态：属主与子节点都在图中，队列为空
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get /v1, Resource=nodes name=nodename", // 删除前查找属主
					},
					graphNodes: []*node{makeNode(node1, withOwners(pod1nonamespace)), makeNode(pod1ns1)},
				}),
			},
		},
		{
			name: "bad cluster-scoped child -> owner in ns1 (owner first)",
			steps: []step{
				// 准备阶段
				createObjectInClient("", "v1", "nodes", "", makeMetadataObj(node1, pod1ns1)),
				createObjectInClient("", "v1", "pods", "ns1", makeMetadataObj(pod1ns1)),
				// 2,3：观察到命名空间级属主
				processEvent(makeAddEvent(pod1ns1)),
				assertState(state{
					graphNodes: []*node{makeNode(pod1ns1)},
				}),
				// 4,5：观察到集群级子节点
				processEvent(makeAddEvent(node1, pod1ns1)),
				assertState(state{
					graphNodes:             []*node{makeNode(node1, withOwners(pod1nonamespace)), makeNode(pod1ns1)},
					events:                 []string{`Warning OwnerRefInvalidNamespace ownerRef [v1/Pod, namespace: , name: podname1, uid: poduid1] does not exist in namespace "" involvedObject{kind=Node,apiVersion=v1}`},
					pendingAttemptToDelete: []*node{makeNode(node1, withOwners(pod1ns1))}, // 有问题的集群级子节点被加入 attemptToDelete 队列
				}),
				// 6,7：处理队列中问题集群级子节点的删除尝试
				// 最终状态：属主与子节点都在图中，队列为空
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get /v1, Resource=nodes name=nodename", // 删除前查找属主
					},
					graphNodes: []*node{makeNode(node1, withOwners(pod1nonamespace)), makeNode(pod1ns1)},
				}),
			},
		},
		// 子节点指向属主对象非首选、但仍在服务的 API 版本（如 rbac/v1beta1）：
		// * 不应被删除 prematurely
		// * 等待期间不应反复轮询 attemptToDelete
		// * 当真正的属主被删除时，子节点应当被删除
		{
			name: "good child -> existing owner with non-preferred accessible API version",
			steps: []step{
				// 准备阶段
				createObjectInClient("rbac.authorization.k8s.io", "v1", "roles", "ns1", makeMetadataObj(role1v1)),
				createObjectInClient("rbac.authorization.k8s.io", "v1beta1", "roles", "ns1", makeMetadataObj(role1v1beta1)),
				createObjectInClient("", "v1", "pods", "ns1", makeMetadataObj(pod1ns1, role1v1beta1)),
				// 3,4：观察到子节点
				processEvent(makeAddEvent(pod1ns1, role1v1beta1)),
				assertState(state{
					graphNodes:             []*node{makeNode(pod1ns1, withOwners(role1v1beta1)), makeNode(role1v1beta1, virtual)},
					pendingAttemptToDelete: []*node{makeNode(role1v1beta1, virtual)}, // 虚拟属主已入队等待删除尝试
				}),
				// 5,6：处理队列中虚拟属主的删除尝试
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get rbac.authorization.k8s.io/v1beta1, Resource=roles ns=ns1 name=role1", // 删除前查找属主
					},
					graphNodes:             []*node{makeNode(pod1ns1, withOwners(role1v1beta1)), makeNode(role1v1beta1, virtual)},
					pendingAttemptToDelete: []*node{makeNode(role1v1beta1, virtual)}, // 尚未观察到属主，仍在 attemptToDelete 队列中
				}),
				// 7,8：通过 v1 观察到属主
				processEvent(makeAddEvent(role1v1)),
				assertState(state{
					graphNodes:             []*node{makeNode(pod1ns1, withOwners(role1v1beta1)), makeNode(role1v1)},               // 属主版本与虚拟状态得到修正
					pendingAttemptToDelete: []*node{makeNode(role1v1beta1, virtual), makeNode(pod1ns1, withOwners(role1v1beta1))}, // 虚拟属主与不匹配的子节点都已入队等待删除尝试
				}),
				// 9,10：处理 attemptToDelete
				// 真实节点已被观察到，虚拟节点从 attemptToDelete 中移除且不再有后续动作
				processAttemptToDelete(1),
				assertState(state{
					graphNodes:             []*node{makeNode(pod1ns1, withOwners(role1v1beta1)), makeNode(role1v1)},
					pendingAttemptToDelete: []*node{makeNode(pod1ns1, withOwners(role1v1beta1))}, // 不匹配的子节点已入队等待删除尝试
				}),
				// 11,12：处理不匹配属主的 attemptToDelete
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get /v1, Resource=pods ns=ns1 name=podname1",                             // 删除前查找子节点
						"get rbac.authorization.k8s.io/v1beta1, Resource=roles ns=ns1 name=role1", // 确认属主确实存在
					},
					graphNodes: []*node{makeNode(pod1ns1, withOwners(role1v1beta1)), makeNode(role1v1)},
				}),
				// 13,14：清理
				deleteObjectFromClient("rbac.authorization.k8s.io", "v1", "roles", "ns1", "role1"),
				deleteObjectFromClient("rbac.authorization.k8s.io", "v1beta1", "roles", "ns1", "role1"),
				// 15,16：通过 v1 观察到删除
				processEvent(makeDeleteEvent(role1v1)),
				assertState(state{
					graphNodes:             []*node{makeNode(pod1ns1, withOwners(role1v1beta1))}, // 仅剩下子节点
					absentOwnerCache:       []objectReference{role1v1},                           // 缓存通过 v1 观察到的属主不存在
					pendingAttemptToDelete: []*node{makeNode(pod1ns1, withOwners(role1v1beta1))},
				}),
				// 17,18：处理子节点的 attemptToDelete
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get /v1, Resource=pods ns=ns1 name=podname1",                             // 删除前查找子节点
						"get rbac.authorization.k8s.io/v1beta1, Resource=roles ns=ns1 name=role1", // 确认属主确实存在
						"delete /v1, Resource=pods ns=ns1 name=podname1",
					},
					absentOwnerCache: []objectReference{role1v1, role1v1beta1}, // 缓存 v1beta1 role 的不存在状态
					graphNodes:       []*node{makeNode(pod1ns1, withOwners(role1v1beta1))},
				}),
				// 19,20：观察第 17 步发出的删除
				// 最终状态：图为空，队列为空
				processEvent(makeDeleteEvent(pod1ns1)),
				assertState(state{
					absentOwnerCache: []objectReference{role1v1, role1v1beta1},
				}),
			},
		},
		// 子节点指向仍存在属主的、已不再服务的 API 版本（如 extensions/v1beta1 deployment）：
		// * 不应被删除 (this is indistinguishable from referencing an unknown kind/version)
		// * 一旦观察到真实属主，虚拟属主不应反复轮询 attemptToDelete
		{
			name: "child -> existing owner with inaccessible API version (child first)",
			steps: []step{
				// 准备阶段
				createObjectInClient("apps", "v1", "deployments", "ns1", makeMetadataObj(deployment1apps)),
				createObjectInClient("", "v1", "pods", "ns1", makeMetadataObj(pod1ns1, deployment1extensions)),
				// 2,3：观察到子节点
				processEvent(makeAddEvent(pod1ns1, deployment1extensions)),
				assertState(state{
					graphNodes:             []*node{makeNode(pod1ns1, withOwners(deployment1extensions)), makeNode(deployment1extensions, virtual)},
					pendingAttemptToDelete: []*node{makeNode(deployment1extensions, virtual)}, // 虚拟属主已入队等待删除尝试
				}),
				// 4,5：处理队列中虚拟属主的删除尝试
				processAttemptToDelete(1),
				assertState(state{
					graphNodes:             []*node{makeNode(pod1ns1, withOwners(deployment1extensions)), makeNode(deployment1extensions, virtual)},
					pendingAttemptToDelete: []*node{makeNode(deployment1extensions, virtual)}, // RESTMapper 报错，重新入队
				}),
				// 6,7：通过 v1 观察到属主
				processEvent(makeAddEvent(deployment1apps)),
				assertState(state{
					graphNodes:             []*node{makeNode(pod1ns1, withOwners(deployment1extensions)), makeNode(deployment1apps)},                // 属主版本与虚拟状态得到修正
					pendingAttemptToDelete: []*node{makeNode(deployment1extensions, virtual), makeNode(pod1ns1, withOwners(deployment1extensions))}, // 虚拟属主与不匹配的子节点都已入队等待删除尝试
				}),
				// 8,9：处理 attemptToDelete
				// 真实节点已被观察到，虚拟节点从 attemptToDelete 中移除且不再有后续动作
				processAttemptToDelete(1),
				assertState(state{
					graphNodes:             []*node{makeNode(pod1ns1, withOwners(deployment1extensions)), makeNode(deployment1apps)},
					pendingAttemptToDelete: []*node{makeNode(pod1ns1, withOwners(deployment1extensions))}, // 不匹配的子节点已入队等待删除尝试
				}),
				// 10,11：处理不匹配子节点的 attemptToDelete
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get /v1, Resource=pods ns=ns1 name=podname1", // 删除前查找子节点
					},
					graphNodes:             []*node{makeNode(pod1ns1, withOwners(deployment1extensions)), makeNode(deployment1apps)},
					pendingAttemptToDelete: []*node{makeNode(pod1ns1, withOwners(deployment1extensions))}, // 不匹配子节点仍在队列中——RESTMapper 报错
				}),
				// 12：清理
				deleteObjectFromClient("apps", "v1", "deployments", "ns1", "deployment1"),
				// 13,14：通过 v1 观察到删除
				processEvent(makeDeleteEvent(deployment1apps)),
				assertState(state{
					graphNodes:             []*node{makeNode(pod1ns1, withOwners(deployment1extensions))}, // 仅剩下子节点
					absentOwnerCache:       []objectReference{deployment1apps},                            // 缓存通过 v1 观察到的属主不存在
					pendingAttemptToDelete: []*node{makeNode(pod1ns1, withOwners(deployment1extensions))},
				}),
				// 17,18：处理子节点的 attemptToDelete
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get /v1, Resource=pods ns=ns1 name=podname1", // 删除前查找子节点
					},
					graphNodes:             []*node{makeNode(pod1ns1, withOwners(deployment1extensions))}, // 仅剩下子节点
					absentOwnerCache:       []objectReference{deployment1apps},
					pendingAttemptToDelete: []*node{makeNode(pod1ns1, withOwners(deployment1extensions))}, // 不匹配子节点仍在队列中——RESTMapper 报错
				}),
			},
		},
		{
			name: "child -> existing owner with inaccessible API version (owner first)",
			steps: []step{
				// 准备阶段
				createObjectInClient("apps", "v1", "deployments", "ns1", makeMetadataObj(deployment1apps)),
				createObjectInClient("", "v1", "pods", "ns1", makeMetadataObj(pod1ns1, deployment1extensions)),
				// 2,3：观察到属主 via v1
				processEvent(makeAddEvent(deployment1apps)),
				assertState(state{
					graphNodes: []*node{makeNode(deployment1apps)},
				}),
				// 4,5：观察到子节点
				processEvent(makeAddEvent(pod1ns1, deployment1extensions)),
				assertState(state{
					graphNodes:             []*node{makeNode(pod1ns1, withOwners(deployment1extensions)), makeNode(deployment1apps)},
					pendingAttemptToDelete: []*node{makeNode(pod1ns1, withOwners(deployment1extensions))}, // 不匹配的子节点已入队等待删除尝试
				}),
				// 6,7：处理不匹配子节点的 attemptToDelete
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get /v1, Resource=pods ns=ns1 name=podname1", // 删除前查找子节点
					},
					graphNodes:             []*node{makeNode(pod1ns1, withOwners(deployment1extensions)), makeNode(deployment1apps)},
					pendingAttemptToDelete: []*node{makeNode(pod1ns1, withOwners(deployment1extensions))}, // 不匹配子节点仍在队列中——RESTMapper 报错
				}),
				// 8：清理
				deleteObjectFromClient("apps", "v1", "deployments", "ns1", "deployment1"),
				// 9,10：通过 v1 观察到删除
				processEvent(makeDeleteEvent(deployment1apps)),
				assertState(state{
					graphNodes:             []*node{makeNode(pod1ns1, withOwners(deployment1extensions))}, // 仅剩下子节点
					absentOwnerCache:       []objectReference{deployment1apps},                            // 缓存通过 v1 观察到的属主不存在
					pendingAttemptToDelete: []*node{makeNode(pod1ns1, withOwners(deployment1extensions))},
				}),
				// 11,12：处理子节点的 attemptToDelete
				// 最终状态：带有无法解析 ownerRef 的子节点保留在图中，仍在 pendingAttemptToDelete 队列里
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get /v1, Resource=pods ns=ns1 name=podname1", // 删除前查找子节点
					},
					graphNodes:             []*node{makeNode(pod1ns1, withOwners(deployment1extensions))}, // 仅剩下子节点
					absentOwnerCache:       []objectReference{deployment1apps},
					pendingAttemptToDelete: []*node{makeNode(pod1ns1, withOwners(deployment1extensions))}, // 不匹配子节点仍在队列中——RESTMapper 报错
				}),
			},
		},
		// 子节点指向已不存在属主的、不再服务的 API 版本（如 extensions/v1beta1 deployment）：
		// * 不应被删除 (this is indistinguishable from referencing an unknown kind/version)
		// * 应反复轮询 attemptToDelete
		// * 不应阻塞缺失 deployment 的合法子节点的删除
		{
			name: "child -> non-existent owner with inaccessible API version (inaccessible parent apiVersion first)",
			steps: []step{
				// 准备阶段
				createObjectInClient("", "v1", "pods", "ns1", makeMetadataObj(pod1ns1, deployment1extensions)),
				createObjectInClient("", "v1", "pods", "ns1", makeMetadataObj(pod2ns1, deployment1apps)),
				// 2,3：观察到指向不再服务的 API 版本的子节点
				processEvent(makeAddEvent(pod1ns1, deployment1extensions)),
				assertState(state{
					graphNodes:             []*node{makeNode(pod1ns1, withOwners(deployment1extensions)), makeNode(deployment1extensions, virtual)},
					pendingAttemptToDelete: []*node{makeNode(deployment1extensions, virtual)}, // 虚拟属主已入队等待删除尝试
				}),
				// 4,5：观察到子节点 pointing at served apiVersion where owner does not exist
				processEvent(makeAddEvent(pod2ns1, deployment1apps)),
				assertState(state{
					graphNodes:             []*node{makeNode(pod1ns1, withOwners(deployment1extensions)), makeNode(deployment1extensions, virtual), makeNode(pod2ns1, withOwners(deployment1apps))},
					pendingAttemptToDelete: []*node{makeNode(deployment1extensions, virtual), makeNode(pod2ns1, withOwners(deployment1apps))}, // 不匹配的子节点已入队等待删除尝试
				}),
				// 6,7：处理不可访问 API 版本的虚拟属主的删除尝试
				processAttemptToDelete(1),
				assertState(state{
					graphNodes:             []*node{makeNode(pod1ns1, withOwners(deployment1extensions)), makeNode(deployment1extensions, virtual), makeNode(pod2ns1, withOwners(deployment1apps))},
					pendingAttemptToDelete: []*node{makeNode(pod2ns1, withOwners(deployment1apps)), makeNode(deployment1extensions, virtual)}, // 不可访问的属主被重新排到队尾
				}),
				// 8,9：处理不匹配子节点的删除尝试
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get /v1, Resource=pods ns=ns1 name=podname2",               // 删除前查找子节点
						"get apps/v1, Resource=deployments ns=ns1 name=deployment1", // 查找属主
						"delete /v1, Resource=pods ns=ns1 name=podname2",            // 删除子节点
					},
					graphNodes:             []*node{makeNode(pod1ns1, withOwners(deployment1extensions)), makeNode(deployment1extensions, virtual), makeNode(pod2ns1, withOwners(deployment1apps))},
					absentOwnerCache:       []objectReference{deployment1apps},                // 已确认不存在的属主被缓存
					pendingAttemptToDelete: []*node{makeNode(deployment1extensions, virtual)}, // 属主已确认不存在，不匹配子节点被删除
				}),
				// 10,11：观察第 8 步发出的删除
				processEvent(makeDeleteEvent(pod2ns1)),
				assertState(state{
					graphNodes:             []*node{makeNode(pod1ns1, withOwners(deployment1extensions)), makeNode(deployment1extensions, virtual)},
					absentOwnerCache:       []objectReference{deployment1apps},
					pendingAttemptToDelete: []*node{makeNode(deployment1extensions, virtual)},
				}),
				// 12,13：最终状态：不可访问的属主在 attemptToDelete 中重新排队
				processAttemptToDelete(1),
				assertState(state{
					graphNodes:             []*node{makeNode(pod1ns1, withOwners(deployment1extensions)), makeNode(deployment1extensions, virtual)},
					absentOwnerCache:       []objectReference{deployment1apps},
					pendingAttemptToDelete: []*node{makeNode(deployment1extensions, virtual)},
				}),
			},
		},

		{
			name: "child -> non-existent owner with inaccessible API version (accessible parent apiVersion first)",
			steps: []step{
				// 准备阶段
				createObjectInClient("", "v1", "pods", "ns1", makeMetadataObj(pod1ns1, deployment1extensions)),
				createObjectInClient("", "v1", "pods", "ns1", makeMetadataObj(pod2ns1, deployment1apps)),
				// 2,3：观察到指向仍在服务、但属主不存在的 API 版本的子节点
				processEvent(makeAddEvent(pod2ns1, deployment1apps)),
				assertState(state{
					graphNodes: []*node{
						makeNode(pod2ns1, withOwners(deployment1apps)),
						makeNode(deployment1apps, virtual)},
					pendingAttemptToDelete: []*node{
						makeNode(deployment1apps, virtual)}, // 虚拟属主已入队等待删除尝试
				}),
				// 4,5：观察到子节点 pointing at no-longer-served apiVersion
				processEvent(makeAddEvent(pod1ns1, deployment1extensions)),
				assertState(state{
					graphNodes: []*node{
						makeNode(pod2ns1, withOwners(deployment1apps)),
						makeNode(deployment1apps, virtual),
						makeNode(pod1ns1, withOwners(deployment1extensions))},
					pendingAttemptToDelete: []*node{
						makeNode(deployment1apps, virtual),
						makeNode(pod1ns1, withOwners(deployment1extensions))}, // 不匹配的子节点已入队等待删除尝试
				}),
				// 6,7：处理可访问 API 版本的虚拟属主的删除尝试
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get apps/v1, Resource=deployments ns=ns1 name=deployment1", // 查找属主, gets 404
					},
					pendingGraphChanges: []*event{makeVirtualDeleteEvent(deployment1apps)}, // 虚拟属主未找到，入队虚拟删除事件
					graphNodes: []*node{
						makeNode(pod2ns1, withOwners(deployment1apps)),
						makeNode(deployment1apps, virtual),
						makeNode(pod1ns1, withOwners(deployment1extensions))},
					pendingAttemptToDelete: []*node{makeNode(pod1ns1, withOwners(deployment1extensions))},
				}),
				// 8,9：处理不匹配子节点的删除尝试
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get /v1, Resource=pods ns=ns1 name=podname1", // 删除前查找子节点
					},
					pendingGraphChanges: []*event{makeVirtualDeleteEvent(deployment1apps)},
					graphNodes: []*node{
						makeNode(pod2ns1, withOwners(deployment1apps)),
						makeNode(deployment1apps, virtual),
						makeNode(pod1ns1, withOwners(deployment1extensions))},
					pendingAttemptToDelete: []*node{makeNode(pod1ns1, withOwners(deployment1extensions))}, // 不可访问属主触发 RESTMapper 错误，重新入队
				}),
				// 10,11：处理队列中的虚拟删除事件
				processPendingGraphChanges(1),
				assertState(state{
					graphNodes: []*node{
						makeNode(pod2ns1, withOwners(deployment1apps)),
						makeNode(deployment1extensions, virtual), // deployment 节点变为另一种虚拟身份
						makeNode(pod1ns1, withOwners(deployment1extensions)),
					},
					absentOwnerCache: []objectReference{deployment1apps}, // 缓存 apps/v1 属主的不存在状态
					pendingAttemptToDelete: []*node{
						makeNode(pod1ns1, withOwners(deployment1extensions)), // 引用不可访问 API 版本的子节点
						makeNode(pod2ns1, withOwners(deployment1apps)),       // 缺失 apps/v1 属主的子节点已入队等待删除尝试
						makeNode(deployment1extensions, virtual),             // 新的虚拟属主已入队等待删除尝试
					},
				}),

				// 12,13：处理引用不可访问 API 版本的子节点的删除尝试
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get /v1, Resource=pods ns=ns1 name=podname1", // 删除前查找子节点
					},
					graphNodes: []*node{
						makeNode(pod2ns1, withOwners(deployment1apps)),
						makeNode(deployment1extensions, virtual),
						makeNode(pod1ns1, withOwners(deployment1extensions))},
					absentOwnerCache: []objectReference{deployment1apps},
					pendingAttemptToDelete: []*node{
						makeNode(pod2ns1, withOwners(deployment1apps)),       // 缺失 apps/v1 属主的子节点已入队等待删除尝试
						makeNode(deployment1extensions, virtual),             // 新的虚拟属主已入队等待删除尝试
						makeNode(pod1ns1, withOwners(deployment1extensions)), // 引用不可访问 API 版本的子节点 - requeued to end
					},
				}),

				// 14,15：处理引用可访问 API 版本的子节点的删除尝试
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get /v1, Resource=pods ns=ns1 name=podname2",    // 删除前查找子节点
						"delete /v1, Resource=pods ns=ns1 name=podname2", // 属主不存在，删除子节点
					},
					graphNodes: []*node{
						makeNode(pod2ns1, withOwners(deployment1apps)),
						makeNode(deployment1extensions, virtual),
						makeNode(pod1ns1, withOwners(deployment1extensions))},
					absentOwnerCache: []objectReference{deployment1apps},
					pendingAttemptToDelete: []*node{
						makeNode(deployment1extensions, virtual),             // 新的虚拟属主已入队等待删除尝试
						makeNode(pod1ns1, withOwners(deployment1extensions)), // 引用不可访问 API 版本的子节点
					},
				}),

				// 16,17：处理不可访问 API 版本的虚拟属主的删除尝试
				processAttemptToDelete(1),
				assertState(state{
					graphNodes: []*node{
						makeNode(pod2ns1, withOwners(deployment1apps)),
						makeNode(deployment1extensions, virtual),
						makeNode(pod1ns1, withOwners(deployment1extensions))},
					absentOwnerCache: []objectReference{deployment1apps},
					pendingAttemptToDelete: []*node{
						makeNode(pod1ns1, withOwners(deployment1extensions)), // 引用不可访问 API 版本的子节点
						makeNode(deployment1extensions, virtual),             // 不可访问 API 版本的虚拟属主被重新排到队尾
					},
				}),

				// 18,19：观察第 14 步发出的 pod2 删除
				// 最终状态：不可访问 API 版本的虚拟属主及其子节点仍留在图中，并以退避方式排队等待删除尝试
				processEvent(makeDeleteEvent(pod2ns1)),
				assertState(state{
					graphNodes: []*node{
						makeNode(deployment1extensions, virtual),
						makeNode(pod1ns1, withOwners(deployment1extensions))},
					absentOwnerCache: []objectReference{deployment1apps},
					pendingAttemptToDelete: []*node{
						makeNode(pod1ns1, withOwners(deployment1extensions)), // 引用不可访问 API 版本的子节点
						makeNode(deployment1extensions, virtual),             // 不可访问 API 版本的虚拟属主
					},
				}),
			},
		},
		// 子节点指向仍存在属主的错误 API 版本/类型（如 UID 为 123 的 core/v1 Secret，而实际存在的是 UID 为 123 的 apps/v1 Deployment）：
		// * 应当被立即删除
		// * 不应触发属主合法子节点的删除
		{
			name: "bad child -> existing owner with incorrect API version (bad child, good child, bad parent delete, good parent)",
			steps: []step{
				// 准备阶段
				createObjectInClient("apps", "v1", "deployments", "ns1", makeMetadataObj(deployment1apps)),
				createObjectInClient("", "v1", "pods", "ns1", makeMetadataObj(badChildPod, badSecretReferenceWithDeploymentUID)),
				createObjectInClient("", "v1", "pods", "ns1", makeMetadataObj(goodChildPod, deployment1apps)),
				// 3,4：观察到问题子节点
				processEvent(makeAddEvent(badChildPod, badSecretReferenceWithDeploymentUID)),
				assertState(state{
					graphNodes: []*node{
						makeNode(badChildPod, withOwners(badSecretReferenceWithDeploymentUID)),
						makeNode(badSecretReferenceWithDeploymentUID, virtual)},
					pendingAttemptToDelete: []*node{
						makeNode(badSecretReferenceWithDeploymentUID, virtual)}, // 虚拟属主已入队等待删除尝试
				}),

				// 5,6：观察到合法子节点
				processEvent(makeAddEvent(goodChildPod, deployment1apps)),
				assertState(state{
					graphNodes: []*node{
						makeNode(goodChildPod, withOwners(deployment1apps)), // 合法子节点已加入
						makeNode(badChildPod, withOwners(badSecretReferenceWithDeploymentUID)),
						makeNode(badSecretReferenceWithDeploymentUID, virtual)},
					pendingAttemptToDelete: []*node{
						makeNode(badSecretReferenceWithDeploymentUID, virtual), // 虚拟属主已入队等待删除尝试
						makeNode(goodChildPod, withOwners(deployment1apps)),    // 合法子节点已入队等待删除尝试
					},
				}),

				// 7,8：处理虚拟属主的待删除项
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get /v1, Resource=secrets ns=ns1 name=secretname", // 查找错误的属主引用
					},
					pendingGraphChanges: []*event{makeVirtualDeleteEvent(badSecretReferenceWithDeploymentUID)}, // 未找到错误虚拟属主，入队虚拟删除事件
					graphNodes: []*node{
						makeNode(goodChildPod, withOwners(deployment1apps)),
						makeNode(badChildPod, withOwners(badSecretReferenceWithDeploymentUID)),
						makeNode(badSecretReferenceWithDeploymentUID, virtual)},
					pendingAttemptToDelete: []*node{
						makeNode(goodChildPod, withOwners(deployment1apps)), // 合法子节点已入队等待删除尝试
					},
				}),

				// 9,10：处理合法子节点的待删除项，返回 200，节点保留
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get /v1, Resource=pods ns=ns1 name=goodpod",                // 删除前查找子节点
						"get apps/v1, Resource=deployments ns=ns1 name=deployment1", // 查找合法属主引用，返回 200
					},
					pendingGraphChanges: []*event{makeVirtualDeleteEvent(badSecretReferenceWithDeploymentUID)}, // 未找到错误虚拟属主，入队虚拟删除事件
					graphNodes: []*node{
						makeNode(goodChildPod, withOwners(deployment1apps)),
						makeNode(badChildPod, withOwners(badSecretReferenceWithDeploymentUID)),
						makeNode(badSecretReferenceWithDeploymentUID, virtual)},
				}),

				// 11,12：处理错误属主引用的虚拟删除事件
				processPendingGraphChanges(1),
				assertState(state{
					graphNodes: []*node{
						makeNode(goodChildPod, withOwners(deployment1apps)),
						makeNode(badChildPod, withOwners(badSecretReferenceWithDeploymentUID)),
						makeNode(deployment1apps, virtual)}, // 属主节点切换为另一种身份，仍是虚拟节点
					absentOwnerCache: []objectReference{badSecretReferenceWithDeploymentUID}, // 记住错误属主坐标的不存在状态
					pendingAttemptToDelete: []*node{
						makeNode(badChildPod, withOwners(badSecretReferenceWithDeploymentUID)), // 错误属主坐标的子节点已入队等待删除尝试
						makeNode(deployment1apps, virtual),                                     // 新的备用虚拟属主身份已入队等待删除尝试
					},
				}),

				// 13,14：处理问题子节点的待删除项
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get /v1, Resource=pods ns=ns1 name=badpod",    // 删除前查找子节点
						"delete /v1, Resource=pods ns=ns1 name=badpod", // 删除问题子节点（错误属主的不存在已被缓存）
					},
					graphNodes: []*node{
						makeNode(goodChildPod, withOwners(deployment1apps)),
						makeNode(badChildPod, withOwners(badSecretReferenceWithDeploymentUID)),
						makeNode(deployment1apps, virtual)}, // 属主节点切换为另一种身份，仍是虚拟节点
					absentOwnerCache: []objectReference{badSecretReferenceWithDeploymentUID},
					pendingAttemptToDelete: []*node{
						makeNode(deployment1apps, virtual), // 新的备用虚拟属主身份已入队等待删除尝试
					},
				}),

				// 15,16：处理新虚拟属主的待删除项
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get apps/v1, Resource=deployments ns=ns1 name=deployment1", // 查找虚拟属主，返回 200
					},
					graphNodes: []*node{
						makeNode(goodChildPod, withOwners(deployment1apps)),
						makeNode(badChildPod, withOwners(badSecretReferenceWithDeploymentUID)),
						makeNode(deployment1apps, virtual)}, // 属主节点切换为另一种身份，仍是虚拟节点
					absentOwnerCache: []objectReference{badSecretReferenceWithDeploymentUID},
					pendingAttemptToDelete: []*node{
						makeNode(deployment1apps, virtual), // 重新入队，尚未被观察到
					},
				}),

				// 17,18：观察到合法属主
				processEvent(makeAddEvent(deployment1apps)),
				assertState(state{
					graphNodes: []*node{
						makeNode(goodChildPod, withOwners(deployment1apps)),
						makeNode(badChildPod, withOwners(badSecretReferenceWithDeploymentUID)),
						makeNode(deployment1apps)}, // 属主节点变为真实节点
					absentOwnerCache: []objectReference{badSecretReferenceWithDeploymentUID},
					pendingAttemptToDelete: []*node{
						makeNode(deployment1apps), // 仍在队列中，但不再是虚拟节点
					},
				}),

				// 19,20：观察第 13 步发出的错误子节点删除
				processEvent(makeDeleteEvent(badChildPod, badSecretReferenceWithDeploymentUID)),
				assertState(state{
					graphNodes: []*node{
						makeNode(goodChildPod, withOwners(deployment1apps)),
						// 问题子节点已从图中移除
						makeNode(deployment1apps)},
					absentOwnerCache: []objectReference{badSecretReferenceWithDeploymentUID},
					pendingAttemptToDelete: []*node{
						makeNode(deployment1apps), // 仍在队列中，但不再是虚拟节点
					},
				}),

				// 21,22：处理合法属主的待删除项
				// 最终状态：合法属主以正确坐标留在图中，合法子节点保留，无待删除项
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get apps/v1, Resource=deployments ns=ns1 name=deployment1", // 查找合法属主，返回 200
					},
					graphNodes: []*node{
						makeNode(goodChildPod, withOwners(deployment1apps)),
						makeNode(deployment1apps)},
					absentOwnerCache: []objectReference{badSecretReferenceWithDeploymentUID},
				}),
			},
		},
		{
			name: "bad child -> existing owner with incorrect API version (bad child, good child, good parent, bad parent delete)",
			steps: []step{
				// 准备阶段
				createObjectInClient("apps", "v1", "deployments", "ns1", makeMetadataObj(deployment1apps)),
				createObjectInClient("", "v1", "pods", "ns1", makeMetadataObj(badChildPod, badSecretReferenceWithDeploymentUID)),
				createObjectInClient("", "v1", "pods", "ns1", makeMetadataObj(goodChildPod, deployment1apps)),
				// 3,4：观察到问题子节点
				processEvent(makeAddEvent(badChildPod, badSecretReferenceWithDeploymentUID)),
				assertState(state{
					graphNodes: []*node{
						makeNode(badChildPod, withOwners(badSecretReferenceWithDeploymentUID)),
						makeNode(badSecretReferenceWithDeploymentUID, virtual)},
					pendingAttemptToDelete: []*node{
						makeNode(badSecretReferenceWithDeploymentUID, virtual)}, // 虚拟属主已入队等待删除尝试
				}),

				// 5,6：观察到合法子节点
				processEvent(makeAddEvent(goodChildPod, deployment1apps)),
				assertState(state{
					graphNodes: []*node{
						makeNode(goodChildPod, withOwners(deployment1apps)), // 合法子节点已加入
						makeNode(badChildPod, withOwners(badSecretReferenceWithDeploymentUID)),
						makeNode(badSecretReferenceWithDeploymentUID, virtual)},
					pendingAttemptToDelete: []*node{
						makeNode(badSecretReferenceWithDeploymentUID, virtual), // 虚拟属主已入队等待删除尝试
						makeNode(goodChildPod, withOwners(deployment1apps)),    // 合法子节点已入队等待删除尝试
					},
				}),

				// 7,8：处理虚拟属主的待删除项
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get /v1, Resource=secrets ns=ns1 name=secretname", // 查找错误的属主引用
					},
					pendingGraphChanges: []*event{makeVirtualDeleteEvent(badSecretReferenceWithDeploymentUID)}, // 未找到错误虚拟属主，入队虚拟删除事件
					graphNodes: []*node{
						makeNode(goodChildPod, withOwners(deployment1apps)),
						makeNode(badChildPod, withOwners(badSecretReferenceWithDeploymentUID)),
						makeNode(badSecretReferenceWithDeploymentUID, virtual)},
					pendingAttemptToDelete: []*node{
						makeNode(goodChildPod, withOwners(deployment1apps)), // 合法子节点已入队等待删除尝试
					},
				}),

				// 9,10：处理合法子节点的待删除项，返回 200，节点保留
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get /v1, Resource=pods ns=ns1 name=goodpod",                // 删除前查找子节点
						"get apps/v1, Resource=deployments ns=ns1 name=deployment1", // 查找合法属主引用，返回 200
					},
					pendingGraphChanges: []*event{makeVirtualDeleteEvent(badSecretReferenceWithDeploymentUID)}, // 未找到错误虚拟属主，入队虚拟删除事件
					graphNodes: []*node{
						makeNode(goodChildPod, withOwners(deployment1apps)),
						makeNode(badChildPod, withOwners(badSecretReferenceWithDeploymentUID)),
						makeNode(badSecretReferenceWithDeploymentUID, virtual)},
				}),

				// 11,12：合法属主的添加事件
				insertEvent(makeAddEvent(deployment1apps)),
				assertState(state{
					pendingGraphChanges: []*event{
						makeAddEvent(deployment1apps),                                // 合法属主的观察插队进入
						makeVirtualDeleteEvent(badSecretReferenceWithDeploymentUID)}, // 未找到错误虚拟属主，入队虚拟删除事件
					graphNodes: []*node{
						makeNode(goodChildPod, withOwners(deployment1apps)),
						makeNode(badChildPod, withOwners(badSecretReferenceWithDeploymentUID)),
						makeNode(badSecretReferenceWithDeploymentUID, virtual)},
				}),

				// 13,14：处理合法属主的添加
				processPendingGraphChanges(1),
				assertState(state{
					pendingGraphChanges: []*event{
						makeVirtualDeleteEvent(badSecretReferenceWithDeploymentUID)}, // 错误虚拟属主的虚拟删除事件仍在队列中
					graphNodes: []*node{
						makeNode(goodChildPod, withOwners(deployment1apps)),
						makeNode(badChildPod, withOwners(badSecretReferenceWithDeploymentUID)),
						makeNode(deployment1apps)}, // 属主节点得到修正，不再是虚拟节点
					pendingAttemptToDelete: []*node{
						makeNode(badChildPod, withOwners(badSecretReferenceWithDeploymentUID))}, // 错误属主坐标的子节点已入队等待删除尝试
				}),

				// 15,16：处理错误属主引用的虚拟删除事件
				processPendingGraphChanges(1),
				assertState(state{
					graphNodes: []*node{
						makeNode(goodChildPod, withOwners(deployment1apps)),
						makeNode(badChildPod, withOwners(badSecretReferenceWithDeploymentUID)),
						makeNode(deployment1apps)},
					absentOwnerCache: []objectReference{badSecretReferenceWithDeploymentUID}, // 记住错误属主坐标的不存在状态
					pendingAttemptToDelete: []*node{
						makeNode(badChildPod, withOwners(badSecretReferenceWithDeploymentUID)), // 错误属主坐标的子节点已入队等待删除尝试
					},
				}),

				// 17,18：处理问题子节点的待删除项
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get /v1, Resource=pods ns=ns1 name=badpod",    // 删除前查找子节点
						"delete /v1, Resource=pods ns=ns1 name=badpod", // 删除问题子节点（错误属主的不存在已被缓存）
					},
					graphNodes: []*node{
						makeNode(goodChildPod, withOwners(deployment1apps)),
						makeNode(badChildPod, withOwners(badSecretReferenceWithDeploymentUID)),
						makeNode(deployment1apps)},
					absentOwnerCache: []objectReference{badSecretReferenceWithDeploymentUID},
				}),

				// 19,20：观察第 17 步发出的错误子节点删除
				// 最终状态：合法属主以正确坐标留在图中，合法子节点保留，无待删除项
				processEvent(makeDeleteEvent(badChildPod, badSecretReferenceWithDeploymentUID)),
				assertState(state{
					graphNodes: []*node{
						makeNode(goodChildPod, withOwners(deployment1apps)),
						// 问题子节点已从图中移除
						makeNode(deployment1apps)},
					absentOwnerCache: []objectReference{badSecretReferenceWithDeploymentUID},
				}),
			},
		},
		{
			name: "bad child -> existing owner with incorrect API version (good child, bad child, good parent)",
			steps: []step{
				// 准备阶段
				createObjectInClient("apps", "v1", "deployments", "ns1", makeMetadataObj(deployment1apps)),
				createObjectInClient("", "v1", "pods", "ns1", makeMetadataObj(badChildPod, badSecretReferenceWithDeploymentUID)),
				createObjectInClient("", "v1", "pods", "ns1", makeMetadataObj(goodChildPod, deployment1apps)),
				// 3,4：观察到合法子节点
				processEvent(makeAddEvent(goodChildPod, deployment1apps)),
				assertState(state{
					graphNodes: []*node{
						makeNode(goodChildPod, withOwners(deployment1apps)), // 合法子节点已加入
						makeNode(deployment1apps, virtual)},                 // 虚拟属主已加入
					pendingAttemptToDelete: []*node{
						makeNode(deployment1apps, virtual), // 虚拟属主已入队等待删除尝试
					},
				}),

				// 5,6：观察到问题子节点
				processEvent(makeAddEvent(badChildPod, badSecretReferenceWithDeploymentUID)),
				assertState(state{
					graphNodes: []*node{
						makeNode(goodChildPod, withOwners(deployment1apps)),
						makeNode(deployment1apps, virtual),
						makeNode(badChildPod, withOwners(badSecretReferenceWithDeploymentUID))}, // 问题子节点已加入
					pendingAttemptToDelete: []*node{
						makeNode(deployment1apps, virtual),                                     // 虚拟属主已入队等待删除尝试
						makeNode(badChildPod, withOwners(badSecretReferenceWithDeploymentUID)), // 问题子节点已入队等待删除尝试
					},
				}),

				// 7,8：处理虚拟属主的待删除项
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get apps/v1, Resource=deployments ns=ns1 name=deployment1", // 查找合法属主引用，返回 200
					},
					graphNodes: []*node{
						makeNode(goodChildPod, withOwners(deployment1apps)),
						makeNode(deployment1apps, virtual),
						makeNode(badChildPod, withOwners(badSecretReferenceWithDeploymentUID))},
					pendingAttemptToDelete: []*node{
						makeNode(badChildPod, withOwners(badSecretReferenceWithDeploymentUID)), // 问题子节点已入队等待删除尝试
						makeNode(deployment1apps, virtual),                                     // 虚拟属主被重新排到队尾，仍为虚拟节点
					},
				}),

				// 9,10：处理问题子节点的待删除项
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get /v1, Resource=pods ns=ns1 name=badpod",        // 删除前查找子节点
						"get /v1, Resource=secrets ns=ns1 name=secretname", // 查找错误的属主引用, returns 404
						"delete /v1, Resource=pods ns=ns1 name=badpod",     // 删除问题子节点
					},
					graphNodes: []*node{
						makeNode(goodChildPod, withOwners(deployment1apps)),
						makeNode(deployment1apps, virtual),
						makeNode(badChildPod, withOwners(badSecretReferenceWithDeploymentUID))},
					absentOwnerCache: []objectReference{badSecretReferenceWithDeploymentUID}, // 记住错误属主的不存在状态
					pendingAttemptToDelete: []*node{
						makeNode(deployment1apps, virtual), // 虚拟属主被重新排到队尾，仍为虚拟节点
					},
				}),

				// 11,12：观察到合法属主
				processEvent(makeAddEvent(deployment1apps)),
				assertState(state{
					graphNodes: []*node{
						makeNode(goodChildPod, withOwners(deployment1apps)),
						makeNode(deployment1apps), // 合法属主不再是虚拟节点
						makeNode(badChildPod, withOwners(badSecretReferenceWithDeploymentUID))},
					absentOwnerCache: []objectReference{badSecretReferenceWithDeploymentUID},
					pendingAttemptToDelete: []*node{
						makeNode(deployment1apps), // 属主被重新排到队尾，不再是虚拟节点
					},
				}),

				// 13,14：观察第 9 步发出的错误子节点删除
				processEvent(makeDeleteEvent(badChildPod, badSecretReferenceWithDeploymentUID)),
				assertState(state{
					graphNodes: []*node{
						makeNode(goodChildPod, withOwners(deployment1apps)),
						// 问题子节点已从图中移除
						makeNode(deployment1apps)},
					absentOwnerCache: []objectReference{badSecretReferenceWithDeploymentUID},
					pendingAttemptToDelete: []*node{
						makeNode(deployment1apps), // 属主被重新排到队尾，不再是虚拟节点
					},
				}),

				// 15,16：处理合法属主的待删除项
				// 最终状态：合法属主以正确坐标留在图中，合法子节点保留，无待删除项
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get apps/v1, Resource=deployments ns=ns1 name=deployment1", // 查找合法属主，返回 200
					},
					graphNodes: []*node{
						makeNode(goodChildPod, withOwners(deployment1apps)),
						makeNode(deployment1apps)},
					absentOwnerCache: []objectReference{badSecretReferenceWithDeploymentUID},
				}),
			},
		},
		{
			// https://github.com/kubernetes/kubernetes/issues/98040
			name: "cluster-scoped bad child, namespaced good child, missing parent",
			steps: []step{
				// 准备阶段
				createObjectInClient("", "v1", "pods", "ns1", makeMetadataObj(pod2ns1, pod1ns1)),     // 合法子节点
				createObjectInClient("", "v1", "nodes", "", makeMetadataObj(node1, pod1nonamespace)), // 问题子节点

				// 2,3：观察到问题子节点
				processEvent(makeAddEvent(node1, pod1nonamespace)),
				assertState(state{
					graphNodes: []*node{
						makeNode(node1, withOwners(pod1nonamespace)),
						makeNode(pod1nonamespace, virtual)},
					pendingAttemptToDelete: []*node{
						makeNode(pod1nonamespace, virtual)}, // 虚拟属主已入队等待删除
				}),

				// 4,5：观察到合法子节点
				processEvent(makeAddEvent(pod2ns1, pod1ns1)),
				assertState(state{
					graphNodes: []*node{
						makeNode(node1, withOwners(pod1nonamespace)),
						makeNode(pod2ns1, withOwners(pod1ns1)),
						makeNode(pod1nonamespace, virtual)},
					pendingAttemptToDelete: []*node{
						makeNode(pod1nonamespace, virtual),     // 虚拟属主已入队等待删除
						makeNode(pod2ns1, withOwners(pod1ns1)), // 不匹配的子节点已入队等待删除
					},
				}),

				// 6,7：处理错误虚拟属主坐标的 attemptToDelete
				processAttemptToDelete(1),
				assertState(state{
					graphNodes: []*node{
						makeNode(node1, withOwners(pod1nonamespace)),
						makeNode(pod2ns1, withOwners(pod1ns1)),
						makeNode(pod1nonamespace, virtual)},
					pendingAttemptToDelete: []*node{
						makeNode(pod2ns1, withOwners(pod1ns1))}, // 不匹配的子节点已入队等待删除
				}),

				// 8,9：处理 attemptToDelete of good child
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get /v1, Resource=pods ns=ns1 name=podname2",    // 获取合法子节点，返回 200
						"get /v1, Resource=pods ns=ns1 name=podname1",    // 获取缺失的属主，返回 404
						"delete /v1, Resource=pods ns=ns1 name=podname2", // 删除合法子节点
					},
					graphNodes: []*node{
						makeNode(node1, withOwners(pod1nonamespace)),
						makeNode(pod2ns1, withOwners(pod1ns1)),
						makeNode(pod1nonamespace, virtual)},
					absentOwnerCache: []objectReference{pod1ns1}, // 缓存缺失的属主
				}),

				// 10,11：观察合法子节点的删除
				// 稳态为问题集群子节点与错误虚拟属主坐标，且不再重试
				processEvent(makeDeleteEvent(pod2ns1, pod1ns1)),
				assertState(state{
					graphNodes: []*node{
						makeNode(node1, withOwners(pod1nonamespace)),
						makeNode(pod1nonamespace, virtual)},
					absentOwnerCache: []objectReference{pod1ns1},
				}),
			},
		},
		{
			// https://github.com/kubernetes/kubernetes/issues/98040
			name: "cluster-scoped bad child, namespaced good child, late observed parent",
			steps: []step{
				// 准备阶段
				createObjectInClient("", "v1", "pods", "ns1", makeMetadataObj(pod1ns1)),              // 合法属主
				createObjectInClient("", "v1", "pods", "ns1", makeMetadataObj(pod2ns1, pod1ns1)),     // 合法子节点
				createObjectInClient("", "v1", "nodes", "", makeMetadataObj(node1, pod1nonamespace)), // 问题子节点

				// 3,4：观察到问题子节点
				processEvent(makeAddEvent(node1, pod1nonamespace)),
				assertState(state{
					graphNodes: []*node{
						makeNode(node1, withOwners(pod1nonamespace)),
						makeNode(pod1nonamespace, virtual)},
					pendingAttemptToDelete: []*node{
						makeNode(pod1nonamespace, virtual)}, // 虚拟属主已入队等待删除
				}),

				// 5,6：观察到合法子节点
				processEvent(makeAddEvent(pod2ns1, pod1ns1)),
				assertState(state{
					graphNodes: []*node{
						makeNode(node1, withOwners(pod1nonamespace)),
						makeNode(pod2ns1, withOwners(pod1ns1)),
						makeNode(pod1nonamespace, virtual)},
					pendingAttemptToDelete: []*node{
						makeNode(pod1nonamespace, virtual),      // 虚拟属主已入队等待删除
						makeNode(pod2ns1, withOwners(pod1ns1))}, // 不匹配的子节点已入队等待删除
				}),

				// 7,8：处理错误虚拟属主坐标的 attemptToDelete
				processAttemptToDelete(1),
				assertState(state{
					graphNodes: []*node{
						makeNode(node1, withOwners(pod1nonamespace)),
						makeNode(pod2ns1, withOwners(pod1ns1)),
						makeNode(pod1nonamespace, virtual)},
					pendingAttemptToDelete: []*node{
						makeNode(pod2ns1, withOwners(pod1ns1))}, // 不匹配的子节点已入队等待删除
				}),

				// 9,10：处理 attemptToDelete of good child
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get /v1, Resource=pods ns=ns1 name=podname2", // 获取合法子节点，返回 200
						"get /v1, Resource=pods ns=ns1 name=podname1", // 获取稍后才被观察到的属主，返回 200
					},
					graphNodes: []*node{
						makeNode(node1, withOwners(pod1nonamespace)),
						makeNode(pod2ns1, withOwners(pod1ns1)),
						makeNode(pod1nonamespace, virtual)},
				}),

				// 11,12：稍后观察到合法属主
				processEvent(makeAddEvent(pod1ns1)),
				assertState(state{
					graphNodes: []*node{
						makeNode(node1, withOwners(pod1nonamespace)),
						makeNode(pod2ns1, withOwners(pod1ns1)),
						makeNode(pod1ns1)},
					// 对错误节点引用发出告警
					events: []string{`Warning OwnerRefInvalidNamespace ownerRef [v1/Pod, namespace: , name: podname1, uid: poduid1] does not exist in namespace "" involvedObject{kind=Node,apiVersion=v1}`},
					pendingAttemptToDelete: []*node{
						makeNode(node1, withOwners(pod1nonamespace))}, // 将问题集群级子节点入队以尝试删除
				}),

				// 13,14：处理问题子节点的 attemptToDelete
				// 稳态为问题集群级子节点保留且不再重试，合法属主与合法子节点留在图中
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get /v1, Resource=nodes name=nodename", // 获取问题子节点，返回 200
					},
					graphNodes: []*node{
						makeNode(node1, withOwners(pod1nonamespace)),
						makeNode(pod2ns1, withOwners(pod1ns1)),
						makeNode(pod1ns1)},
				}),
			},
		},
		{
			// https://github.com/kubernetes/kubernetes/issues/98040
			name: "namespaced good child, cluster-scoped bad child, missing parent",
			steps: []step{
				// 准备阶段
				createObjectInClient("", "v1", "pods", "ns1", makeMetadataObj(pod2ns1, pod1ns1)),     // 合法子节点
				createObjectInClient("", "v1", "nodes", "", makeMetadataObj(node1, pod1nonamespace)), // 问题子节点

				// 2,3：观察到合法子节点
				processEvent(makeAddEvent(pod2ns1, pod1ns1)),
				assertState(state{
					graphNodes: []*node{
						makeNode(pod2ns1, withOwners(pod1ns1)),
						makeNode(pod1ns1, virtual)},
					pendingAttemptToDelete: []*node{
						makeNode(pod1ns1, virtual)}, // 虚拟属主已入队等待删除
				}),

				// 4,5：观察到问题子节点
				processEvent(makeAddEvent(node1, pod1nonamespace)),
				assertState(state{
					graphNodes: []*node{
						makeNode(pod2ns1, withOwners(pod1ns1)),
						makeNode(node1, withOwners(pod1nonamespace)),
						makeNode(pod1ns1, virtual)},
					pendingAttemptToDelete: []*node{
						makeNode(pod1ns1, virtual),                   // 虚拟属主已入队等待删除
						makeNode(node1, withOwners(pod1nonamespace)), // 不匹配的子节点已入队等待删除
					},
				}),

				// 6,7：处理合法虚拟属主坐标的 attemptToDelete
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get /v1, Resource=pods ns=ns1 name=podname1", // 查找缺失的属主，返回 404
					},
					graphNodes: []*node{
						makeNode(node1, withOwners(pod1nonamespace)),
						makeNode(pod2ns1, withOwners(pod1ns1)),
						makeNode(pod1ns1, virtual)},
					pendingGraphChanges: []*event{makeVirtualDeleteEvent(pod1ns1)}, // 虚拟属主未找到，入队虚拟删除事件
					pendingAttemptToDelete: []*node{
						makeNode(node1, withOwners(pod1nonamespace)), // 不匹配的子节点仍在删除队列中
					},
				}),

				// 8,9：处理 attemptToDelete of bad cluster child
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get /v1, Resource=nodes name=nodename", // 查找已存在的节点
					},
					graphNodes: []*node{
						makeNode(node1, withOwners(pod1nonamespace)),
						makeNode(pod2ns1, withOwners(pod1ns1)),
						makeNode(pod1ns1, virtual)},
					pendingGraphChanges: []*event{makeVirtualDeleteEvent(pod1ns1)}, // 虚拟属主的虚拟删除事件仍在队列中
				}),

				// 10,11：处理合法虚拟属主坐标的虚拟删除事件
				processPendingGraphChanges(1),
				assertState(state{
					graphNodes: []*node{
						makeNode(node1, withOwners(pod1nonamespace)),
						makeNode(pod2ns1, withOwners(pod1ns1)),
						makeNode(pod1nonamespace, virtual)}, // 缺失的虚拟属主被替换为备用坐标，仍为虚拟节点
					absentOwnerCache: []objectReference{pod1ns1}, // 缓存缺失属主的不存在状态
					pendingAttemptToDelete: []*node{
						makeNode(pod2ns1, withOwners(pod1ns1)), // 合法子节点 of missing parent enqueued for deletion
						makeNode(pod1nonamespace, virtual),     // 新的虚拟属主坐标已入队等待删除
					},
				}),

				// 12,13：处理合法子节点的 attemptToDelete
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get /v1, Resource=pods ns=ns1 name=podname2",    // 查找合法子节点
						"delete /v1, Resource=pods ns=ns1 name=podname2", // 删除合法子节点
					},
					graphNodes: []*node{
						makeNode(node1, withOwners(pod1nonamespace)),
						makeNode(pod2ns1, withOwners(pod1ns1)),
						makeNode(pod1nonamespace, virtual)},
					absentOwnerCache: []objectReference{pod1ns1},
					pendingAttemptToDelete: []*node{
						makeNode(pod1nonamespace, virtual), // 新的虚拟属主坐标已入队等待删除
					},
				}),

				// 14,15：观察合法子节点的删除
				processEvent(makeDeleteEvent(pod2ns1, pod1ns1)),
				assertState(state{
					graphNodes: []*node{
						makeNode(node1, withOwners(pod1nonamespace)),
						makeNode(pod1nonamespace, virtual)},
					absentOwnerCache: []objectReference{pod1ns1},
					pendingAttemptToDelete: []*node{
						makeNode(pod1nonamespace, virtual), // 新的虚拟属主坐标已入队等待删除
					},
				}),

				// 16,17：处理错误虚拟属主坐标的 attemptToDelete
				// 稳态为问题集群子节点与错误虚拟属主坐标，且不再重试
				processAttemptToDelete(1),
				assertState(state{
					graphNodes: []*node{
						makeNode(node1, withOwners(pod1nonamespace)),
						makeNode(pod1nonamespace, virtual)},
					absentOwnerCache: []objectReference{pod1ns1},
				}),
			},
		},
		{
			// https://github.com/kubernetes/kubernetes/issues/114603
			name: "resourceVersion conflict between Get/Delete while processing object deletion",
			steps: []step{
				// 准备阶段, 0,1
				createObjectInClient("", "v1", "pods", "ns1", makeMetadataObj(pod1ns1)),                        // 合法属主
				createObjectInClient("", "v1", "pods", "ns1", withRV(makeMetadataObj(pod2ns1, pod1ns1), "42")), // 合法子节点
				// 事件，步骤 2,3
				processEvent(makeAddEvent(pod1ns1)),
				processEvent(withRV(makeAddEvent(pod2ns1, pod1ns1), "42")),
				// 删除属主，步骤 4,5,6
				deleteObjectFromClient("", "v1", "pods", "ns1", pod1ns1.Name),
				processEvent(makeDeleteEvent(pod1ns1)),
				assertState(state{
					absentOwnerCache: []objectReference{pod1ns1},
					graphNodes: []*node{
						makeNode(pod2ns1, withOwners(pod1ns1))},
					pendingAttemptToDelete: []*node{
						makeNode(pod2ns1, withOwners(pod1ns1)),
					},
				}),

				// 添加 reactor 以强制校验 RV 前置条件，步骤 7
				prependReactor("delete", "pods", (func() func(ctx stepContext) clientgotesting.ReactionFunc {
					call := 0
					return func(ctx stepContext) clientgotesting.ReactionFunc {
						return func(action clientgotesting.Action) (handled bool, ret runtime.Object, err error) {
							call++
							deleteAction, ok := action.(clientgotesting.DeleteAction)
							if !ok {
								ctx.t.Error("missing DeleteAction")
								return false, nil, nil
							}

							preconditionRV := ""
							if preconditions := deleteAction.GetDeleteOptions().Preconditions; preconditions != nil {
								if rv := preconditions.ResourceVersion; rv != nil {
									preconditionRV = *rv
								}
							}

							objectRV := ""
							var metadataObj *metav1.PartialObjectMetadata
							if obj, err := ctx.metadataClient.Tracker().Get(deleteAction.GetResource(), deleteAction.GetNamespace(), deleteAction.GetName(), metav1.GetOptions{}); err == nil {
								metadataObj = obj.(*metav1.PartialObjectMetadata)
								objectRV = metadataObj.ResourceVersion
							}

							switch call {
							case 1:
								if preconditionRV == "42" && objectRV == "42" {
									// 模拟并发变更，修改了属主引用
									ctx.t.Log("changing rv 42 --> 43 concurrently")
									metadataObj.OwnerReferences = []metav1.OwnerReference{role1v1.OwnerReference}
									metadataObj.ResourceVersion = "43"
									if err := ctx.metadataClient.Tracker().Update(deleteAction.GetResource(), metadataObj, deleteAction.GetNamespace()); err != nil {
										ctx.t.Errorf("unexpected error updating tracker: %v", err)
									}
									return true, nil, errors.NewConflict(deleteAction.GetResource().GroupResource(), deleteAction.GetName(), fmt.Errorf("expected 42, got 43"))
								} else {
									ctx.t.Errorf("expected delete with rv=42 precondition on call 1, got %q, %q", preconditionRV, objectRV)
								}
							case 2:
								if preconditionRV == "43" && objectRV == "43" {
									// 模拟并发变更，但*没有*修改属主引用
									ctx.t.Log("changing rv 43 --> 44 concurrently")
									metadataObj.ResourceVersion = "44"
									if err := ctx.metadataClient.Tracker().Update(deleteAction.GetResource(), metadataObj, deleteAction.GetNamespace()); err != nil {
										ctx.t.Errorf("unexpected error updating tracker: %v", err)
									}
									return true, nil, errors.NewConflict(deleteAction.GetResource().GroupResource(), deleteAction.GetName(), fmt.Errorf("expected 43, got 44"))
								} else {
									ctx.t.Errorf("expected delete with rv=43 precondition on call 2, got %q, %q", preconditionRV, objectRV)
								}
							case 3:
								if preconditionRV != "" {
									ctx.t.Errorf("expected delete with no rv precondition on call 3, got %q", preconditionRV)
								}
							default:
								ctx.t.Errorf("expected delete call %d", call)
							}
							return false, nil, nil
						}
					}
				})()),

				// 8,9
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get /v1, Resource=pods ns=ns1 name=podname2",    // 第一次 get 看到 rv=42，属主为 pod1ns1
						"delete /v1, Resource=pods ns=ns1 name=podname2", // 第一次带 rv=42 前置条件的删除遇到冲突，触发一次实时 get
						"get /v1, Resource=pods ns=ns1 name=podname2",    // get 得到新的 ownerReferences，退出 attemptToDelete
					},
					absentOwnerCache: []objectReference{pod1ns1},
					graphNodes: []*node{
						makeNode(pod2ns1, withOwners(pod1ns1))},
					pendingAttemptToDelete: []*node{
						makeNode(pod2ns1, withOwners(pod1ns1))},
				}),

				// 重新尝试删除，步骤 10,11
				processAttemptToDelete(1),
				assertState(state{
					clientActions: []string{
						"get /v1, Resource=pods ns=ns1 name=podname2",                        // 第一次 get 看到 rv=43，属主为 role1v1
						"get rbac.authorization.k8s.io/v1, Resource=roles ns=ns1 name=role1", // 验证属主确实不存在
						"delete /v1, Resource=pods ns=ns1 name=podname2",                     // 第一次删除时 RV 前置条件触发实时 Get
						"get /v1, Resource=pods ns=ns1 name=podname2",                        // 对象的 ownerReferences 未变，从而执行无条件 Delete
						"delete /v1, Resource=pods ns=ns1 name=podname2",                     // 无条件 Delete
					},
					absentOwnerCache: []objectReference{pod1ns1, role1v1},
					graphNodes: []*node{
						makeNode(pod2ns1, withOwners(pod1ns1))},
				}),
			},
		},
	}

	alwaysStarted := make(chan struct{})
	close(alwaysStarted)
	for _, scenario := range testScenarios {
		t.Run(scenario.name, func(t *testing.T) {

			absentOwnerCache := NewReferenceCache(100)

			eventRecorder := record.NewFakeRecorder(100)
			eventRecorder.IncludeObject = true

			metadataClient := fakemetadata.NewSimpleMetadataClient(fakemetadata.NewTestScheme())

			tweakableRM := meta.NewDefaultRESTMapper(nil)
			tweakableRM.AddSpecific(
				schema.GroupVersionKind{Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "Role"},
				schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "roles"},
				schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "role"},
				meta.RESTScopeNamespace,
			)
			tweakableRM.AddSpecific(
				schema.GroupVersionKind{Group: "rbac.authorization.k8s.io", Version: "v1beta1", Kind: "Role"},
				schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1beta1", Resource: "roles"},
				schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1beta1", Resource: "role"},
				meta.RESTScopeNamespace,
			)
			tweakableRM.AddSpecific(
				schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"},
				schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"},
				schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployment"},
				meta.RESTScopeNamespace,
			)
			restMapper := &testRESTMapper{meta.MultiRESTMapper{tweakableRM, testrestmapper.TestOnlyStaticRESTMapper(legacyscheme.Scheme)}}

			// 准备工作队列
			attemptToDelete := newTrackingWorkqueue[*node]()
			attemptToOrphan := newTrackingWorkqueue[*node]()
			graphChanges := newTrackingWorkqueue[*event]()

			gc := &GarbageCollector{
				metadataClient:   metadataClient,
				restMapper:       restMapper,
				attemptToDelete:  attemptToDelete,
				attemptToOrphan:  attemptToOrphan,
				absentOwnerCache: absentOwnerCache,
				dependencyGraphBuilder: &GraphBuilder{
					eventRecorder:    eventRecorder,
					metadataClient:   metadataClient,
					informersStarted: alwaysStarted,
					graphChanges:     graphChanges,
					uidToNode: &concurrentUIDToNode{
						uidToNodeLock: sync.RWMutex{},
						uidToNode:     make(map[types.UID]*node),
					},
					attemptToDelete:  attemptToDelete,
					attemptToOrphan:  attemptToOrphan,
					absentOwnerCache: absentOwnerCache,
				},
			}

			logger, _ := ktesting.NewTestContext(t)

			ctx := stepContext{
				t:               t,
				logger:          logger,
				gc:              gc,
				eventRecorder:   eventRecorder,
				metadataClient:  metadataClient,
				attemptToDelete: attemptToDelete,
				attemptToOrphan: attemptToOrphan,
				graphChanges:    graphChanges,
			}
			for i, s := range scenario.steps {
				ctx.t.Logf("%d: %s", i, s.name)
				s.check(ctx)
				if ctx.t.Failed() {
					return
				}
				verifyGraphInvariants(fmt.Sprintf("after step %d", i), gc.dependencyGraphBuilder.uidToNode.uidToNode, t)
				if ctx.t.Failed() {
					return
				}
			}
		})
	}
}

// makeID 根据各字段构造对象引用，供场景中的节点与事件使用。
func makeID(groupVersion string, kind string, namespace, name, uid string) objectReference {
	return objectReference{
		OwnerReference: metav1.OwnerReference{APIVersion: groupVersion, Kind: kind, Name: name, UID: types.UID(uid)},
		Namespace:      namespace,
	}
}

// nodeTweak 用于在构造期望节点时按需修改节点字段。
type nodeTweak func(*node) *node

// virtual 把节点标记为虚拟节点，表示属主尚未被真正观察到。
func virtual(n *node) *node {
	n.virtual = true
	return n
}

// withOwners 返回一个 nodeTweak，用于设置节点的属主引用列表。
func withOwners(ownerReferences ...objectReference) nodeTweak {
	return func(n *node) *node {
		var owners []metav1.OwnerReference
		for _, o := range ownerReferences {
			owners = append(owners, o.OwnerReference)
		}
		n.owners = owners
		return n
	}
}

// makeNode 构造期望的节点，并依次应用给定的 tweak。
func makeNode(identity objectReference, tweaks ...nodeTweak) *node {
	n := &node{identity: identity}
	for _, tweak := range tweaks {
		n = tweak(n)
	}
	return n
}

// makeAddEvent 构造添加事件；解析 APIVersion 失败属于编程错误，直接 panic。
func makeAddEvent(identity objectReference, owners ...objectReference) *event {
	gv, err := schema.ParseGroupVersion(identity.APIVersion)
	if err != nil {
		panic(err)
	}
	return &event{
		eventType: addEvent,
		gvk:       gv.WithKind(identity.Kind),
		obj:       makeObj(identity, owners...),
	}
}

// makeVirtualDeleteEvent 构造虚拟删除事件，用于通知图清理虚拟属主节点。
func makeVirtualDeleteEvent(identity objectReference, owners ...objectReference) *event {
	e := makeDeleteEvent(identity, owners...)
	e.virtual = true
	return e
}

// makeDeleteEvent 构造删除事件。
func makeDeleteEvent(identity objectReference, owners ...objectReference) *event {
	gv, err := schema.ParseGroupVersion(identity.APIVersion)
	if err != nil {
		panic(err)
	}
	return &event{
		eventType: deleteEvent,
		gvk:       gv.WithKind(identity.Kind),
		obj:       makeObj(identity, owners...),
	}
}

// makeObj 构造只包含元数据的对象，携带身份与属主引用。
func makeObj(identity objectReference, owners ...objectReference) *metaonly.MetadataOnlyObject {
	obj := &metaonly.MetadataOnlyObject{
		TypeMeta:   metav1.TypeMeta{APIVersion: identity.APIVersion, Kind: identity.Kind},
		ObjectMeta: metav1.ObjectMeta{Namespace: identity.Namespace, UID: identity.UID, Name: identity.Name},
	}
	for _, owner := range owners {
		obj.ObjectMeta.OwnerReferences = append(obj.ObjectMeta.OwnerReferences, owner.OwnerReference)
	}
	return obj
}

// withRV 给对象设置 ResourceVersion，用于模拟删除时的并发冲突。
func withRV[T any](obj T, rv string) T {
	switch t := any(obj).(type) {
	case *metav1.PartialObjectMetadata:
		t.ResourceVersion = rv
	case *event:
		withRV(t.obj, rv)
	}
	return obj
}

// makeMetadataObj 构造 PartialObjectMetadata，供 fake 元数据客户端创建/断言对象使用。
func makeMetadataObj(identity objectReference, owners ...objectReference) *metav1.PartialObjectMetadata {
	obj := &metav1.PartialObjectMetadata{
		TypeMeta:   metav1.TypeMeta{APIVersion: identity.APIVersion, Kind: identity.Kind},
		ObjectMeta: metav1.ObjectMeta{Namespace: identity.Namespace, UID: identity.UID, Name: identity.Name},
	}
	for _, owner := range owners {
		obj.ObjectMeta.OwnerReferences = append(obj.ObjectMeta.OwnerReferences, owner.OwnerReference)
	}
	return obj
}

// stepContext 汇总场景执行过程中需要访问的测试上下文：GC 实例、 recorder、
// 元数据客户端以及各工作队列。
type stepContext struct {
	t               *testing.T
	logger          klog.Logger
	gc              *GarbageCollector
	eventRecorder   *record.FakeRecorder
	metadataClient  *fakemetadata.FakeMetadataClient
	attemptToDelete *trackingWorkqueue[*node]
	attemptToOrphan *trackingWorkqueue[*node]
	graphChanges    *trackingWorkqueue[*event]
}

// step 表示场景中的一个步骤，check 会在一步执行完后校验状态。
type step struct {
	name  string
	check func(stepContext)
}

// processPendingGraphChanges 构造一个步骤，处理指定数量的图变更；count<=0 表示处理全部。
func processPendingGraphChanges(count int) step {
	return step{
		name: "processPendingGraphChanges",
		check: func(ctx stepContext) {
			ctx.t.Helper()
			if count <= 0 {
				// 处理全部
				for ctx.gc.dependencyGraphBuilder.graphChanges.Len() != 0 {
					ctx.gc.dependencyGraphBuilder.processGraphChanges(ctx.logger)
				}
			} else {
				for i := 0; i < count; i++ {
					if ctx.gc.dependencyGraphBuilder.graphChanges.Len() == 0 {
						ctx.t.Errorf("expected at least %d pending changes, got %d", count, i+1)
						return
					}
					ctx.gc.dependencyGraphBuilder.processGraphChanges(ctx.logger)
				}
			}
		},
	}
}

// reactionFuncFactory 根据步骤上下文生成 fake 客户端的 reaction 函数。
type reactionFuncFactory func(ctx stepContext) clientgotesting.ReactionFunc

// prependReactor 向 fake 元数据客户端前置一个 reactor，用于模拟服务器对特定请求的行为。
func prependReactor(verb, resource string, reaction reactionFuncFactory) step {
	return step{
		name: "prependReactor",
		check: func(ctx stepContext) {
			ctx.t.Helper()
			ctx.metadataClient.PrependReactor(verb, resource, reaction(ctx))
		},
	}
}

// processAttemptToDelete 构造一个步骤，处理指定数量的待删除项；count<=0 表示处理全部。
func processAttemptToDelete(count int) step {
	return step{
		name: "processAttemptToDelete",
		check: func(ctx stepContext) {
			ctx.t.Helper()
			if count <= 0 {
				// 处理全部
				for ctx.gc.dependencyGraphBuilder.attemptToDelete.Len() != 0 {
					ctx.gc.processAttemptToDeleteWorker(context.TODO())
				}
			} else {
				for i := 0; i < count; i++ {
					if ctx.gc.dependencyGraphBuilder.attemptToDelete.Len() == 0 {
						ctx.t.Errorf("expected at least %d pending changes, got %d", count, i+1)
						return
					}
					ctx.gc.processAttemptToDeleteWorker(context.TODO())
				}
			}
		},
	}
}

// insertEvent 把新事件插到队列最前面：先取出队列中的事件，加入新事件后再放回，
// 用于验证事件处理顺序对结果的影响。
func insertEvent(e *event) step {
	return step{
		name: "insertEvent",
		check: func(ctx stepContext) {
			ctx.t.Helper()
			// 先把队列中的事件全部取出。
			var items []*event
			for ctx.gc.dependencyGraphBuilder.graphChanges.Len() > 0 {
				item, _ := ctx.gc.dependencyGraphBuilder.graphChanges.Get()
				ctx.gc.dependencyGraphBuilder.graphChanges.Done(item)
				items = append(items, item)
			}

			// 加入新事件。
			ctx.gc.dependencyGraphBuilder.graphChanges.Add(e)

			// 再把原来的事件追加回去，使新事件排在队首。
			for _, item := range items {
				ctx.gc.dependencyGraphBuilder.graphChanges.Add(item)
			}
		},
	}
}

// processEvent 构造一个步骤，向图中加入并处理一个事件；要求此时没有未处理的图变更。
func processEvent(e *event) step {
	return step{
		name: "processEvent",
		check: func(ctx stepContext) {
			ctx.t.Helper()
			if ctx.gc.dependencyGraphBuilder.graphChanges.Len() != 0 {
				ctx.t.Fatalf("events present in graphChanges, must process pending graphChanges before calling processEvent")
			}
			ctx.gc.dependencyGraphBuilder.graphChanges.Add(e)
			ctx.gc.dependencyGraphBuilder.processGraphChanges(ctx.logger)
		},
	}
}

// createObjectInClient 通过 fake 元数据客户端创建对象，并清除该操作产生的 action
// 记录，避免干扰后续对客户端动作的断言。
func createObjectInClient(group, version, resource, namespace string, obj *metav1.PartialObjectMetadata) step {
	return step{
		name: "createObjectInClient",
		check: func(ctx stepContext) {
			ctx.t.Helper()
			if len(ctx.metadataClient.Actions()) > 0 {
				ctx.t.Fatal("cannot call createObjectInClient with pending client actions, call assertClientActions to check and clear first")
			}
			gvr := schema.GroupVersionResource{Group: group, Version: version, Resource: resource}
			var c fakemetadata.MetadataClient
			if namespace == "" {
				c = ctx.metadataClient.Resource(gvr).(fakemetadata.MetadataClient)
			} else {
				c = ctx.metadataClient.Resource(gvr).Namespace(namespace).(fakemetadata.MetadataClient)
			}
			if _, err := c.CreateFake(obj, metav1.CreateOptions{}); err != nil {
				ctx.t.Fatal(err)
			}
			ctx.metadataClient.ClearActions()
		},
	}
}

// deleteObjectFromClient 通过 fake 元数据客户端删除对象，同样清除 action 记录。
func deleteObjectFromClient(group, version, resource, namespace, name string) step {
	return step{
		name: "deleteObjectFromClient",
		check: func(ctx stepContext) {
			ctx.t.Helper()
			if len(ctx.metadataClient.Actions()) > 0 {
				ctx.t.Fatal("cannot call deleteObjectFromClient with pending client actions, call assertClientActions to check and clear first")
			}
			gvr := schema.GroupVersionResource{Group: group, Version: version, Resource: resource}
			var c fakemetadata.MetadataClient
			if namespace == "" {
				c = ctx.metadataClient.Resource(gvr).(fakemetadata.MetadataClient)
			} else {
				c = ctx.metadataClient.Resource(gvr).Namespace(namespace).(fakemetadata.MetadataClient)
			}
			if err := c.Delete(context.TODO(), name, metav1.DeleteOptions{}); err != nil {
				ctx.t.Fatal(err)
			}
			ctx.metadataClient.ClearActions()
		},
	}
}

// state 描述一个步骤执行完后期望的全局状态，包括事件、客户端动作、
// 图中的节点以及各队列和缓存中的内容。
type state struct {
	events                 []string
	clientActions          []string
	graphNodes             []*node
	pendingGraphChanges    []*event
	pendingAttemptToDelete []*node
	pendingAttemptToOrphan []*node
	absentOwnerCache       []objectReference
}

// assertState 构造一个步骤，对事件、客户端动作、图节点、各队列及 absent owner 缓存
// 做全面断言。
func assertState(s state) step {
	return step{
		name: "assertState",
		check: func(ctx stepContext) {
			ctx.t.Helper()

			{
				for _, absent := range s.absentOwnerCache {
					if !ctx.gc.absentOwnerCache.Has(absent) {
						ctx.t.Errorf("expected absent owner %s was not in the absentOwnerCache", absent)
					}
				}
				if len(s.absentOwnerCache) != ctx.gc.absentOwnerCache.cache.Len() {
					// 检查缓存内容的唯一方式是清空它；反正这里已判定测试失败，清空无妨。
					err := ctx.gc.absentOwnerCache.cache.SetEvictionFunc(func(key lru.Key, item interface{}) {
						found := false
						for _, absent := range s.absentOwnerCache {
							if absent == key {
								found = true
								break
							}
						}
						if !found {
							ctx.t.Errorf("unexpected item in absent owner cache: %s", key)
						}
					})
					if err != nil {
						ctx.t.Error("unexpected error setting eviction function: %w", err)
					}
					ctx.gc.absentOwnerCache.cache.Clear()
					ctx.t.Error("unexpected items in absent owner cache")
				}
			}

			{
				var actualEvents []string
				// 取走已发送的事件，避免影响后续步骤。
			loop:
				for {
					select {
					case event := <-ctx.eventRecorder.Events:
						actualEvents = append(actualEvents, event)
					default:
						break loop
					}
				}
				if !reflect.DeepEqual(actualEvents, s.events) {
					ctx.t.Logf("expected:\n%s", strings.Join(s.events, "\n"))
					ctx.t.Logf("actual:\n%s", strings.Join(actualEvents, "\n"))
					ctx.t.Fatalf("did not get expected events")
				}
			}

			{
				var actualClientActions []string
				for _, action := range ctx.metadataClient.Actions() {
					s := fmt.Sprintf("%s %s", action.GetVerb(), action.GetResource())
					if action.GetNamespace() != "" {
						s += " ns=" + action.GetNamespace()
					}
					if get, ok := action.(clientgotesting.GetAction); ok && get.GetName() != "" {
						s += " name=" + get.GetName()
					}
					actualClientActions = append(actualClientActions, s)
				}
				if (len(s.clientActions) > 0 || len(actualClientActions) > 0) && !reflect.DeepEqual(s.clientActions, actualClientActions) {
					ctx.t.Logf("expected:\n%s", strings.Join(s.clientActions, "\n"))
					ctx.t.Logf("actual:\n%s", strings.Join(actualClientActions, "\n"))
					ctx.t.Fatalf("did not get expected client actions")
				}
				ctx.metadataClient.ClearActions()
			}

			{
				if l := len(ctx.gc.dependencyGraphBuilder.uidToNode.uidToNode); l != len(s.graphNodes) {
					ctx.t.Errorf("expected %d nodes, got %d", len(s.graphNodes), l)
				}
				for _, n := range s.graphNodes {
					graphNode, ok := ctx.gc.dependencyGraphBuilder.uidToNode.Read(n.identity.UID)
					if !ok {
						ctx.t.Errorf("%s: no node in graph with uid=%s", n.identity.UID, n.identity.UID)
						continue
					}
					if graphNode.identity != n.identity {
						ctx.t.Errorf("%s: expected identity %v, got %v", n.identity.UID, n.identity, graphNode.identity)
					}
					if graphNode.virtual != n.virtual {
						ctx.t.Errorf("%s: expected virtual %v, got %v", n.identity.UID, n.virtual, graphNode.virtual)
					}
					if (len(graphNode.owners) > 0 || len(n.owners) > 0) && !reflect.DeepEqual(graphNode.owners, n.owners) {
						expectedJSON, _ := json.Marshal(n.owners)
						actualJSON, _ := json.Marshal(graphNode.owners)
						ctx.t.Errorf("%s: expected owners %s, got %s", n.identity.UID, expectedJSON, actualJSON)
					}
				}
			}

			{
				for i := range s.pendingGraphChanges {
					e := s.pendingGraphChanges[i]
					if len(ctx.graphChanges.pendingList) < i+1 {
						ctx.t.Errorf("graphChanges: expected %d events, got %d", len(s.pendingGraphChanges), ctx.graphChanges.Len())
						break
					}

					a := ctx.graphChanges.pendingList[i]
					if !reflect.DeepEqual(e, a) {
						objectDiff := ""
						if !reflect.DeepEqual(e.obj, a.obj) {
							objectDiff = "\nobjectDiff:\n" + cmp.Diff(e.obj, a.obj)
						}
						oldObjectDiff := ""
						if !reflect.DeepEqual(e.oldObj, a.oldObj) {
							oldObjectDiff = "\noldObjectDiff:\n" + cmp.Diff(e.oldObj, a.oldObj)
						}
						ctx.t.Errorf("graphChanges[%d]: expected\n%#v\ngot\n%#v%s%s", i, e, a, objectDiff, oldObjectDiff)
					}
				}
				if ctx.graphChanges.Len() > len(s.pendingGraphChanges) {
					for i, a := range ctx.graphChanges.pendingList[len(s.pendingGraphChanges):] {
						ctx.t.Errorf("graphChanges[%d]: unexpected event: %v", len(s.pendingGraphChanges)+i, a)
					}
				}
			}

			{
				for i := range s.pendingAttemptToDelete {
					e := s.pendingAttemptToDelete[i].identity
					e_virtual := s.pendingAttemptToDelete[i].virtual
					if ctx.attemptToDelete.Len() < i+1 {
						ctx.t.Errorf("attemptToDelete: expected %d events, got %d", len(s.pendingAttemptToDelete), ctx.attemptToDelete.Len())
						break
					}
					a := ctx.attemptToDelete.pendingList[i].identity
					aVirtual := ctx.attemptToDelete.pendingList[i].virtual
					if !reflect.DeepEqual(e, a) {
						ctx.t.Errorf("attemptToDelete[%d]: expected %v, got %v", i, e, a)
					}
					if e_virtual != aVirtual {
						ctx.t.Errorf("attemptToDelete[%d]: expected virtual node %v, got non-virtual node %v", i, e, a)
					}
				}
				if ctx.attemptToDelete.Len() > len(s.pendingAttemptToDelete) {
					for i, a := range ctx.attemptToDelete.pendingList[len(s.pendingAttemptToDelete):] {
						ctx.t.Errorf("attemptToDelete[%d]: unexpected node: %v", len(s.pendingAttemptToDelete)+i, a.identity)
					}
				}
			}

			{
				for i := range s.pendingAttemptToOrphan {
					e := s.pendingAttemptToOrphan[i].identity
					if ctx.attemptToOrphan.Len() < i+1 {
						ctx.t.Errorf("attemptToOrphan: expected %d events, got %d", len(s.pendingAttemptToOrphan), ctx.attemptToOrphan.Len())
						break
					}
					a := ctx.attemptToOrphan.pendingList[i].identity
					if !reflect.DeepEqual(e, a) {
						ctx.t.Errorf("attemptToOrphan[%d]: expected %v, got %v", i, e, a)
					}
				}
				if ctx.attemptToOrphan.Len() > len(s.pendingAttemptToOrphan) {
					for i, a := range ctx.attemptToOrphan.pendingList[len(s.pendingAttemptToOrphan):] {
						ctx.t.Errorf("attemptToOrphan[%d]: unexpected node: %v", len(s.pendingAttemptToOrphan)+i, a.identity)
					}
				}
			}
		},
	}

}

// trackingWorkqueue 实现了 RateLimitingInterface，额外维护队列内容的快照以便断言。
// 它把 AddAfter 和 AddRateLimited 都当成 Add 处理，使入队总是同步的，
// 避免测试因延迟入队而变得不确定。
type trackingWorkqueue[T comparable] struct {
	limiter     workqueue.TypedRateLimitingInterface[T]
	pendingList []T
	pendingMap  map[T]struct{}
}

var _ = workqueue.TypedRateLimitingInterface[string](&trackingWorkqueue[string]{})

// newTrackingWorkqueue 构造一个使用无限速限制器的 trackingWorkqueue，
// 保证入队操作立即生效。
func newTrackingWorkqueue[T comparable]() *trackingWorkqueue[T] {
	return &trackingWorkqueue[T]{
		limiter:    workqueue.NewTypedRateLimitingQueue[T](&workqueue.TypedBucketRateLimiter[T]{Limiter: rate.NewLimiter(rate.Inf, 100)}),
		pendingMap: map[T]struct{}{},
	}
}

// Add 把元素记录到快照并加入底层限速队列。
func (t *trackingWorkqueue[T]) Add(item T) {
	t.queue(item)
	t.limiter.Add(item)
}

// AddAfter 忽略延迟，直接当作 Add 入队。
func (t *trackingWorkqueue[T]) AddAfter(item T, duration time.Duration) {
	t.Add(item)
}

// AddRateLimited 忽略限速，直接当作 Add 入队。
func (t *trackingWorkqueue[T]) AddRateLimited(item T) {
	t.Add(item)
}

// Get 从底层队列取出元素，并同步更新快照。
func (t *trackingWorkqueue[T]) Get() (T, bool) {
	item, shutdown := t.limiter.Get()
	t.dequeue(item)
	return item, shutdown
}

// Done 通知底层队列元素已处理完成。
func (t *trackingWorkqueue[T]) Done(item T) {
	t.limiter.Done(item)
}

// Forget 通知底层队列忘记该元素的失败次数。
func (t *trackingWorkqueue[T]) Forget(item T) {
	t.limiter.Forget(item)
}

// NumRequeues 本实现不做实际限速，因此始终返回 0。
func (t *trackingWorkqueue[T]) NumRequeues(item T) int {
	return 0
}

// Len 返回队列长度，并同时校验快照与底层队列长度一致，防止断言基于错误状态。
func (t *trackingWorkqueue[T]) Len() int {
	if e, a := len(t.pendingList), len(t.pendingMap); e != a {
		panic(fmt.Errorf("pendingList != pendingMap: %d / %d", e, a))
	}
	if e, a := len(t.pendingList), t.limiter.Len(); e != a {
		panic(fmt.Errorf("pendingList != limiter.Len(): %d / %d", e, a))
	}
	return len(t.pendingList)
}

// ShutDown 关闭底层队列。
func (t *trackingWorkqueue[T]) ShutDown() {
	t.limiter.ShutDown()
}

// ShutDownWithDrain 关闭底层队列并等待排空。
func (t *trackingWorkqueue[T]) ShutDownWithDrain() {
	t.limiter.ShutDownWithDrain()
}

// ShuttingDown 返回底层队列是否正在关闭。
func (t *trackingWorkqueue[T]) ShuttingDown() bool {
	return t.limiter.ShuttingDown()
}

// queue 把元素记录到有序快照与去重集合中；重复入队会被忽略。
func (t *trackingWorkqueue[T]) queue(item T) {
	if _, queued := t.pendingMap[item]; queued {
		// fmt.Printf("already queued: %#v\n", item)
		return
	}
	t.pendingMap[item] = struct{}{}
	t.pendingList = append(t.pendingList, item)
}

// dequeue 从快照中移除元素，保持 pendingList 与 pendingMap 一致。
func (t *trackingWorkqueue[T]) dequeue(item T) {
	if _, queued := t.pendingMap[item]; !queued {
		// fmt.Printf("not queued: %#v\n", item)
		return
	}
	delete(t.pendingMap, item)
	newPendingList := []T{}
	for _, p := range t.pendingList {
		if p == item {
			continue
		}
		newPendingList = append(newPendingList, p)
	}
	t.pendingList = newPendingList
}
