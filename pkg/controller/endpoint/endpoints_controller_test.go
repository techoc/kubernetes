/*
Copyright 2014 The Kubernetes Authors.

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

package endpoint

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	clientscheme "k8s.io/client-go/kubernetes/scheme"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	utiltesting "k8s.io/client-go/util/testing"
	endptspkg "k8s.io/kubernetes/pkg/api/v1/endpoints"
	api "k8s.io/kubernetes/pkg/apis/core"
	controllerpkg "k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/test/utils/ktesting"
	utilnet "k8s.io/utils/net"
	"k8s.io/utils/ptr"
)

// alwaysReady/neverReady 作为 informer HasSynced 检查的替身，测试中无需等待真实缓存同步。
var alwaysReady = func() bool { return true }
var neverReady = func() bool { return false }

// emptyNodeName 用作期望端点地址中节点名指针的值（空字符串）。
var emptyNodeName string

// triggerTime 及其字符串形式用于验证 EndpointsLastChangeTriggerTime 注解的写入、覆盖与清除。
// oldTriggerTimeString 表示更早的触发时间，用于验证旧注解会被新值覆盖。
var triggerTime = time.Date(2018, 01, 01, 0, 0, 0, 0, time.UTC)
var triggerTimeString = triggerTime.Format(time.RFC3339Nano)
var oldTriggerTimeString = triggerTime.Add(-time.Hour).Format(time.RFC3339Nano)

// 单栈与双栈集群的 IP 族组合，供各类同步用例复用。
var ipv4only = []v1.IPFamily{v1.IPv4Protocol}
var ipv6only = []v1.IPFamily{v1.IPv6Protocol}
var ipv4ipv6 = []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol}
var ipv6ipv4 = []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol}

// testPod 构造测试用 Pod：编号决定名称、标签与 IP，isReady 决定 PodReady 条件，
// nPorts 决定容器端口数量，ipFamilies 决定 PodIPs 的族与顺序。
// IP 由 id 确定性推导（IPv4 为 1.2.x.y，IPv6 为 2000::x），便于在断言中写出期望地址。
func testPod(namespace string, id int, nPorts int, isReady bool, ipFamilies []v1.IPFamily) *v1.Pod {
	p := &v1.Pod{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1"},
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       namespace,
			Name:            fmt.Sprintf("pod%d", id),
			Labels:          map[string]string{"foo": "bar"},
			ResourceVersion: fmt.Sprint(id),
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{Ports: []v1.ContainerPort{}}},
		},
		Status: v1.PodStatus{
			Conditions: []v1.PodCondition{
				{
					Type:   v1.PodReady,
					Status: v1.ConditionTrue,
				},
			},
		},
	}
	if !isReady {
		p.Status.Conditions[0].Status = v1.ConditionFalse
	}
	for j := 0; j < nPorts; j++ {
		p.Spec.Containers[0].Ports = append(p.Spec.Containers[0].Ports,
			v1.ContainerPort{Name: fmt.Sprintf("port%d", j), ContainerPort: int32(8080 + j)})
	}
	for _, family := range ipFamilies {
		var ip string
		if family == v1.IPv4Protocol {
			// 示例：id=1, ip=1.2.3.4
			// 示例：id=2, ip=1.2.3.5
			// 示例：id=999, ip=1.2.6.235
			ip = fmt.Sprintf("1.2.%d.%d", 3+((4+id)/256), (4+id)%256)
		} else {
			ip = fmt.Sprintf("2000::%x", 4+id)
		}
		p.Status.PodIPs = append(p.Status.PodIPs, v1.PodIP{IP: ip})
	}
	p.Status.PodIP = p.Status.PodIPs[0].IP

	return p
}

// addPods 向 store 批量写入 nPods 个就绪 Pod 与 nNotReady 个未就绪 Pod；
// 前 nPods 个下标对应就绪 Pod，其余对应未就绪 Pod，具体对象由 testPod 生成。
func addPods(store cache.Store, namespace string, nPods int, nPorts int, nNotReady int, ipFamilies []v1.IPFamily) {
	for i := 0; i < nPods+nNotReady; i++ {
		isReady := i < nPods
		pod := testPod(namespace, i, nPorts, isReady, ipFamilies)
		store.Add(pod)
	}
}

// addBadIPPod 写入一个 IP 前多出一个 0 的 Pod，用于验证
// podToEndpointAddressForService 会修正非法 IP 而不是原样透传。
func addBadIPPod(store cache.Store, namespace string, ipFamilies []v1.IPFamily) {
	pod := testPod(namespace, 0, 1, true, ipFamilies)
	pod.Status.PodIPs[0].IP = "0" + pod.Status.PodIPs[0].IP
	pod.Status.PodIP = pod.Status.PodIPs[0].IP
	store.Add(pod)
}

// addNotReadyPodsWithSpecifiedRestartPolicyAndPhase 批量构造未就绪、且处于指定相位与
// 重启策略的 Pod，用于验证终态 Pod 是否会被排除在 Endpoints 之外。
func addNotReadyPodsWithSpecifiedRestartPolicyAndPhase(store cache.Store, namespace string, nPods int, nPorts int, restartPolicy v1.RestartPolicy, podPhase v1.PodPhase) {
	for i := 0; i < nPods; i++ {
		p := &v1.Pod{
			TypeMeta: metav1.TypeMeta{APIVersion: "v1"},
			ObjectMeta: metav1.ObjectMeta{
				Namespace: namespace,
				Name:      fmt.Sprintf("pod%d", i),
				Labels:    map[string]string{"foo": "bar"},
			},
			Spec: v1.PodSpec{
				RestartPolicy: restartPolicy,
				Containers:    []v1.Container{{Ports: []v1.ContainerPort{}}},
			},
			Status: v1.PodStatus{
				PodIP: fmt.Sprintf("1.2.3.%d", 4+i),
				Phase: podPhase,
				Conditions: []v1.PodCondition{
					{
						Type:   v1.PodReady,
						Status: v1.ConditionFalse,
					},
				},
			},
		}
		for j := 0; j < nPorts; j++ {
			p.Spec.Containers[0].Ports = append(p.Spec.Containers[0].Ports,
				v1.ContainerPort{Name: fmt.Sprintf("port%d", j), ContainerPort: int32(8080 + j)})
		}
		store.Add(p)
	}
}

// makeTestServer 启动一个仅服务于指定命名空间 Endpoints 路径的假 API Server。
// 其他路径一律返回 404 并让测试失败，确保控制器没有发出预期外的请求；
// 返回的 FakeHandler 用于断言请求次数与请求内容。
func makeTestServer(t *testing.T, namespace string) (*httptest.Server, *utiltesting.FakeHandler) {
	fakeEndpointsHandler := utiltesting.FakeHandler{
		StatusCode:   http.StatusOK,
		ResponseBody: runtime.EncodeOrDie(clientscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), &v1.Endpoints{}),
	}
	mux := http.NewServeMux()
	if namespace == "" {
		t.Fatal("namespace cannot be empty")
	}
	mux.Handle("/api/v1/namespaces/"+namespace+"/endpoints", &fakeEndpointsHandler)
	mux.Handle("/api/v1/namespaces/"+namespace+"/endpoints/", &fakeEndpointsHandler)
	mux.HandleFunc("/", func(res http.ResponseWriter, req *http.Request) {
		t.Errorf("unexpected request: %v", req.RequestURI)
		http.Error(res, "", http.StatusNotFound)
	})
	return httptest.NewServer(mux), &fakeEndpointsHandler
}

// makeBlockingEndpointTestServer 在收到 Endpoints 的 POST/PUT/DELETE 请求时向 blockNextAction 发信号。
// POST/PUT 会先等待 blockUpdate（若提供），DELETE 会先等待 blockDelete（若提供），
// 从而人为制造“API Server 已处理但本地缓存尚未更新”的窗口。
// controller 为 nil 时直接返回错误，防止控制器就绪前误发请求。
func makeBlockingEndpointTestServer(t *testing.T, controller *endpointController, endpoint *v1.Endpoints, blockUpdate, blockDelete, blockNextAction chan struct{}, namespace string) *httptest.Server {

	handlerFunc := func(res http.ResponseWriter, req *http.Request) {
		if controller == nil {
			res.WriteHeader(http.StatusInternalServerError)
			res.Write([]byte("controller has not been set yet"))
			return
		}

		if req.Method == "POST" || req.Method == "PUT" {
			if blockUpdate != nil {
				go func() {
					// 延迟写入缓存，使 endpoints 缓存与 API Server 状态不一致
					<-blockUpdate
					_ = controller.endpointsStore.Add(endpoint)
				}()
			} else {
				_ = controller.endpointsStore.Add(endpoint)
			}
			blockNextAction <- struct{}{}
		}

		if req.Method == "DELETE" {
			if blockDelete != nil {
				go func() {
					// 延迟从缓存删除，使 endpoints 缓存与 API Server 状态不一致
					<-blockDelete
					_ = controller.endpointsStore.Delete(endpoint)
					controller.onEndpointsDelete(endpoint)
				}()
			} else {
				_ = controller.endpointsStore.Delete(endpoint)
				controller.onEndpointsDelete(endpoint)
			}
			blockNextAction <- struct{}{}
		}

		res.Header().Set("Content-Type", "application/json")
		res.WriteHeader(http.StatusOK)
		_, _ = res.Write([]byte(runtime.EncodeOrDie(clientscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), endpoint)))
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/namespaces/"+namespace+"/endpoints", handlerFunc)
	mux.HandleFunc("/api/v1/namespaces/"+namespace+"/endpoints/", handlerFunc)
	mux.HandleFunc("/api/v1/namespaces/"+namespace+"/events", func(res http.ResponseWriter, req *http.Request) {})
	mux.HandleFunc("/", func(res http.ResponseWriter, req *http.Request) {
		t.Errorf("unexpected request: %v", req.RequestURI)
		http.Error(res, "", http.StatusNotFound)
	})
	return httptest.NewServer(mux)

}

// endpointController 在真实 Controller 之上暴露 informer 的底层 Store，
// 便于测试直接预置 Pod/Service/Endpoints 对象并观察控制器行为。
type endpointController struct {
	*Controller
	podStore       cache.Store
	serviceStore   cache.Store
	endpointsStore cache.Store
}

// newController 基于假 API Server 的 URL 创建控制器，并把三个 HasSynced 检查
// 替换为 alwaysReady，使测试无需等待 informer 完成初始列表同步即可调用 syncService。
func newController(ctx context.Context, url string, batchPeriod time.Duration) *endpointController {
	client := clientset.NewForConfigOrDie(&restclient.Config{Host: url, ContentConfig: restclient.ContentConfig{GroupVersion: &schema.GroupVersion{Group: "", Version: "v1"}, ContentType: runtime.ContentTypeJSON}})
	informerFactory := informers.NewSharedInformerFactory(client, controllerpkg.NoResyncPeriodFunc())
	endpoints := NewEndpointController(ctx, informerFactory.Core().V1().Pods(), informerFactory.Core().V1().Services(),
		informerFactory.Core().V1().Endpoints(), client, batchPeriod)
	endpoints.podsSynced = alwaysReady
	endpoints.servicesSynced = alwaysReady
	endpoints.endpointsSynced = alwaysReady
	return &endpointController{
		endpoints,
		informerFactory.Core().V1().Pods().Informer().GetStore(),
		informerFactory.Core().V1().Services().Informer().GetStore(),
		informerFactory.Core().V1().Endpoints().Informer().GetStore(),
	}
}

// newFakeController 使用 fake clientset 创建控制器，同样跳过 informer 同步等待。
// 返回值中的 client 供测试直接读取 API 侧的 Endpoints，以验证同步结果。
func newFakeController(ctx context.Context, batchPeriod time.Duration) (*fake.Clientset, *endpointController) {
	client := fake.NewSimpleClientset()
	informerFactory := informers.NewSharedInformerFactory(client, controllerpkg.NoResyncPeriodFunc())

	eController := NewEndpointController(
		ctx,
		informerFactory.Core().V1().Pods(),
		informerFactory.Core().V1().Services(),
		informerFactory.Core().V1().Endpoints(),
		client,
		batchPeriod)

	eController.podsSynced = alwaysReady
	eController.servicesSynced = alwaysReady
	eController.endpointsSynced = alwaysReady

	return client, &endpointController{
		eController,
		informerFactory.Core().V1().Pods().Informer().GetStore(),
		informerFactory.Core().V1().Services().Informer().GetStore(),
		informerFactory.Core().V1().Endpoints().Informer().GetStore(),
	}
}

// TestSyncEndpointsItemsPreserveNoSelector 验证无 selector 的 Service：控制器不应接管或
// 覆盖已存在的 Endpoints，因此期望 0 次写请求。
func TestSyncEndpointsItemsPreserveNoSelector(t *testing.T) {
	ns := metav1.NamespaceDefault
	testServer, endpointsHandler := makeTestServer(t, ns)
	defer testServer.Close()

	tCtx := ktesting.Init(t)
	endpoints := newController(tCtx, testServer.URL, 0*time.Second)
	endpoints.endpointsStore.Add(&v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
		},
		Subsets: []v1.EndpointSubset{{
			Addresses: []v1.EndpointAddress{{IP: "6.7.8.9", NodeName: &emptyNodeName}},
			Ports:     []v1.EndpointPort{{Port: 1000}},
		}},
	})
	endpoints.serviceStore.Add(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns},
		Spec:       v1.ServiceSpec{Ports: []v1.ServicePort{{Port: 80}}},
	})
	err := endpoints.syncService(tCtx, ns+"/foo")
	if err != nil {
		t.Errorf("Unexpected error syncing service %v", err)
	}
	endpointsHandler.ValidateRequestCount(t, 0)
}

// TestSyncEndpointsExistingNilSubsets 验证已有的、由本控制器管理的 Endpoints 在
// subsets 为 nil 且计算结果无变化时不会被重复更新。
func TestSyncEndpointsExistingNilSubsets(t *testing.T) {
	ns := metav1.NamespaceDefault
	testServer, endpointsHandler := makeTestServer(t, ns)
	defer testServer.Close()

	tCtx := ktesting.Init(t)
	endpoints := newController(tCtx, testServer.URL, 0*time.Second)
	endpoints.endpointsStore.Add(&v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
			Labels: map[string]string{
				LabelManagedBy: ControllerName,
			},
		},
		Subsets: nil,
	})
	endpoints.serviceStore.Add(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns},
		Spec: v1.ServiceSpec{
			Selector: map[string]string{"foo": "bar"},
			Ports:    []v1.ServicePort{{Port: 80}},
		},
	})
	err := endpoints.syncService(tCtx, ns+"/foo")
	if err != nil {
		t.Errorf("Unexpected error syncing service %v", err)
	}
	endpointsHandler.ValidateRequestCount(t, 0)
}

// TestSyncEndpointsExistingEmptySubsets 与上一个用例类似，区别是 subsets 为空切片而非 nil，
// 同样期望控制器跳过无意义的 PUT。
func TestSyncEndpointsExistingEmptySubsets(t *testing.T) {
	ns := metav1.NamespaceDefault
	testServer, endpointsHandler := makeTestServer(t, ns)
	defer testServer.Close()

	tCtx := ktesting.Init(t)
	endpoints := newController(tCtx, testServer.URL, 0*time.Second)
	endpoints.endpointsStore.Add(&v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
			Labels: map[string]string{
				LabelManagedBy: ControllerName,
			},
		},
		Subsets: []v1.EndpointSubset{},
	})
	endpoints.serviceStore.Add(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns},
		Spec: v1.ServiceSpec{
			Selector: map[string]string{"foo": "bar"},
			Ports:    []v1.ServicePort{{Port: 80}},
		},
	})
	err := endpoints.syncService(tCtx, ns+"/foo")
	if err != nil {
		t.Errorf("Unexpected error syncing service %v", err)
	}
	endpointsHandler.ValidateRequestCount(t, 0)
}

// TestSyncEndpointsWithPodResourceVersionUpdateOnly 验证仅 Pod ResourceVersion 变化时，
// 由于端点地址与端口均未改变，控制器不应重新写入 Endpoints。
func TestSyncEndpointsWithPodResourceVersionUpdateOnly(t *testing.T) {
	ns := metav1.NamespaceDefault
	testServer, endpointsHandler := makeTestServer(t, ns)
	defer testServer.Close()
	pod0 := testPod(ns, 0, 1, true, ipv4only)
	pod1 := testPod(ns, 1, 1, false, ipv4only)
	tCtx := ktesting.Init(t)
	endpoints := newController(tCtx, testServer.URL, 0*time.Second)
	endpoints.endpointsStore.Add(&v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
			Labels: map[string]string{
				LabelManagedBy: ControllerName,
			},
		},
		Subsets: []v1.EndpointSubset{{
			Addresses: []v1.EndpointAddress{
				{
					IP:        pod0.Status.PodIPs[0].IP,
					NodeName:  &emptyNodeName,
					TargetRef: &v1.ObjectReference{Kind: "Pod", Name: pod0.Name, Namespace: ns, ResourceVersion: "1"},
				},
			},
			NotReadyAddresses: []v1.EndpointAddress{
				{
					IP:        pod1.Status.PodIPs[0].IP,
					NodeName:  &emptyNodeName,
					TargetRef: &v1.ObjectReference{Kind: "Pod", Name: pod1.Name, Namespace: ns, ResourceVersion: "2"},
				},
			},
			Ports: []v1.EndpointPort{{Port: 8080, Protocol: "TCP"}},
		}},
	})
	endpoints.serviceStore.Add(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns},
		Spec: v1.ServiceSpec{
			Selector:   map[string]string{"foo": "bar"},
			Ports:      []v1.ServicePort{{Port: 80, Protocol: "TCP", TargetPort: intstr.FromInt32(8080)}},
			IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
		},
	})
	// 只递增 ResourceVersion，保持 IP、端口与就绪状态不变，模拟无实质变化的状态更新。
	pod0.ResourceVersion = "3"
	pod1.ResourceVersion = "4"
	endpoints.podStore.Add(pod0)
	endpoints.podStore.Add(pod1)
	err := endpoints.syncService(tCtx, ns+"/foo")
	if err != nil {
		t.Errorf("Unexpected error syncing service %v", err)
	}

	endpointsHandler.ValidateRequestCount(t, 0)
}

// TestSyncEndpointsNewNoSubsets 验证 Service 已有 selector 但 Endpoints 尚不存在时，
// 控制器会创建一条新的 Endpoints（期望 1 次写请求）。
func TestSyncEndpointsNewNoSubsets(t *testing.T) {
	ns := metav1.NamespaceDefault
	testServer, endpointsHandler := makeTestServer(t, ns)
	defer testServer.Close()
	tCtx := ktesting.Init(t)
	endpoints := newController(tCtx, testServer.URL, 0*time.Second)
	endpoints.serviceStore.Add(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns},
		Spec: v1.ServiceSpec{
			Selector: map[string]string{"foo": "bar"},
			Ports:    []v1.ServicePort{{Port: 80}},
		},
	})
	err := endpoints.syncService(tCtx, ns+"/foo")
	if err != nil {
		t.Errorf("Unexpected error syncing service %v", err)
	}
	endpointsHandler.ValidateRequestCount(t, 1)
}

// TestCheckLeftoverEndpoints 验证 checkLeftoverEndpoints 会把“存在 Endpoints 但无对应
// Service/selector”的键重新入队，以便后续同步清理这些遗留对象。
func TestCheckLeftoverEndpoints(t *testing.T) {
	ns := metav1.NamespaceDefault
	testServer, _ := makeTestServer(t, ns)
	defer testServer.Close()

	tCtx := ktesting.Init(t)
	endpoints := newController(tCtx, testServer.URL, 0*time.Second)
	endpoints.endpointsStore.Add(&v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
		},
		Subsets: []v1.EndpointSubset{{
			Addresses: []v1.EndpointAddress{{IP: "6.7.8.9", NodeName: &emptyNodeName}},
			Ports:     []v1.EndpointPort{{Port: 1000}},
		}},
	})
	endpoints.checkLeftoverEndpoints()
	if e, a := 1, endpoints.queue.Len(); e != a {
		t.Fatalf("Expected %v, got %v", e, a)
	}
	got, _ := endpoints.queue.Get()
	if e, a := ns+"/foo", got; e != a {
		t.Errorf("Expected %v, got %v", e, a)
	}
}

// TestSyncEndpointsProtocolTCP 验证 Service 端口协议 TCP 会传播到 Endpoints 子集，
// 并断言写请求为 PUT 且请求体与期望对象一致。
func TestSyncEndpointsProtocolTCP(t *testing.T) {
	ns := "other"
	testServer, endpointsHandler := makeTestServer(t, ns)
	defer testServer.Close()
	tCtx := ktesting.Init(t)
	endpoints := newController(tCtx, testServer.URL, 0*time.Second)
	endpoints.endpointsStore.Add(&v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
		},
		Subsets: []v1.EndpointSubset{{
			Addresses: []v1.EndpointAddress{{IP: "6.7.8.9", NodeName: &emptyNodeName}},
			Ports:     []v1.EndpointPort{{Port: 1000, Protocol: "TCP"}},
		}},
	})
	addPods(endpoints.podStore, ns, 1, 1, 0, ipv4only)
	endpoints.serviceStore.Add(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns},
		Spec: v1.ServiceSpec{
			Selector:   map[string]string{},
			Ports:      []v1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(8080), Protocol: "TCP"}},
			IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
		},
	})
	err := endpoints.syncService(tCtx, ns+"/foo")
	if err != nil {
		t.Errorf("Unexpected error syncing service %v", err)
	}

	endpointsHandler.ValidateRequestCount(t, 1)
	data := runtime.EncodeOrDie(clientscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), &v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
			Labels: map[string]string{
				LabelManagedBy:       ControllerName,
				v1.IsHeadlessService: "",
			},
		},
		Subsets: []v1.EndpointSubset{{
			Addresses: []v1.EndpointAddress{{IP: "1.2.3.4", NodeName: &emptyNodeName, TargetRef: &v1.ObjectReference{Kind: "Pod", Name: "pod0", Namespace: ns}}},
			Ports:     []v1.EndpointPort{{Port: 8080, Protocol: "TCP"}},
		}},
	})
	endpointsHandler.ValidateRequest(t, "/api/v1/namespaces/"+ns+"/endpoints/foo", "PUT", &data)
}

// TestSyncEndpointsHeadlessServiceLabel 验证当 Endpoints 已带有控制器管理与 headless 标签、
// 且子集为空无需变更时，控制器不会发出写请求。
func TestSyncEndpointsHeadlessServiceLabel(t *testing.T) {
	ns := metav1.NamespaceDefault
	testServer, endpointsHandler := makeTestServer(t, ns)
	defer testServer.Close()
	tCtx := ktesting.Init(t)
	endpoints := newController(tCtx, testServer.URL, 0*time.Second)
	endpoints.endpointsStore.Add(&v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
			Labels: map[string]string{
				LabelManagedBy:       ControllerName,
				v1.IsHeadlessService: "",
			},
		},
		Subsets: []v1.EndpointSubset{},
	})
	endpoints.serviceStore.Add(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns},
		Spec: v1.ServiceSpec{
			Selector: map[string]string{"foo": "bar"},
			Ports:    []v1.ServicePort{{Port: 80}},
		},
	})
	err := endpoints.syncService(tCtx, ns+"/foo")
	if err != nil {
		t.Errorf("Unexpected error syncing service %v", err)
	}

	endpointsHandler.ValidateRequestCount(t, 0)
}

// TestSyncServiceExternalNameType 验证 ExternalName 类型的 Service 无论是否带有 selector
// 或端口都不应生成/更新 Endpoints；四个子用例均断言 0 次写请求。
func TestSyncServiceExternalNameType(t *testing.T) {
	serviceName := "testing-1"
	namespace := metav1.NamespaceDefault

	testCases := []struct {
		// desc 为子用例名称；service 为待同步的 Service 对象。
		desc    string
		service *v1.Service
	}{
		{
			desc: "External name with selector and ports should not receive endpoints",
			service: &v1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: serviceName, Namespace: namespace},
				Spec: v1.ServiceSpec{
					Selector: map[string]string{"foo": "bar"},
					Ports:    []v1.ServicePort{{Port: 80}},
					Type:     v1.ServiceTypeExternalName,
				},
			},
		},
		{
			desc: "External name with ports should not receive endpoints",
			service: &v1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: serviceName, Namespace: namespace},
				Spec: v1.ServiceSpec{
					Ports: []v1.ServicePort{{Port: 80}},
					Type:  v1.ServiceTypeExternalName,
				},
			},
		},
		{
			desc: "External name with selector should not receive endpoints",
			service: &v1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: serviceName, Namespace: namespace},
				Spec: v1.ServiceSpec{
					Selector: map[string]string{"foo": "bar"},
					Type:     v1.ServiceTypeExternalName,
				},
			},
		},
		{
			desc: "External name without selector and ports should not receive endpoints",
			service: &v1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: serviceName, Namespace: namespace},
				Spec: v1.ServiceSpec{
					Type: v1.ServiceTypeExternalName,
				},
			},
		},
	}

	// 所有子用例的共同断言：ExternalName Service 不产生任何 Endpoints 写请求。
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			testServer, endpointsHandler := makeTestServer(t, namespace)

			defer testServer.Close()
			tCtx := ktesting.Init(t)
			endpoints := newController(tCtx, testServer.URL, 0*time.Second)
			err := endpoints.serviceStore.Add(tc.service)
			if err != nil {
				t.Fatalf("Error adding service to service store: %v", err)
			}
			err = endpoints.syncService(tCtx, namespace+"/"+serviceName)
			if err != nil {
				t.Fatalf("Error syncing service: %v", err)
			}
			endpointsHandler.ValidateRequestCount(t, 0)
		})
	}
}

// TestSyncEndpointsProtocolUDP 验证 UDP 协议 Service 生成携带 UDP 端口的 Endpoints。
func TestSyncEndpointsProtocolUDP(t *testing.T) {
	ns := "other"
	testServer, endpointsHandler := makeTestServer(t, ns)
	defer testServer.Close()
	tCtx := ktesting.Init(t)
	endpoints := newController(tCtx, testServer.URL, 0*time.Second)
	endpoints.endpointsStore.Add(&v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
		},
		Subsets: []v1.EndpointSubset{{
			Addresses: []v1.EndpointAddress{{IP: "6.7.8.9", NodeName: &emptyNodeName}},
			Ports:     []v1.EndpointPort{{Port: 1000, Protocol: "UDP"}},
		}},
	})
	addPods(endpoints.podStore, ns, 1, 1, 0, ipv4only)
	endpoints.serviceStore.Add(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns},
		Spec: v1.ServiceSpec{
			Selector:   map[string]string{},
			Ports:      []v1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(8080), Protocol: "UDP"}},
			IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
		},
	})
	err := endpoints.syncService(tCtx, ns+"/foo")
	if err != nil {
		t.Errorf("Unexpected error syncing service %v", err)
	}

	endpointsHandler.ValidateRequestCount(t, 1)
	data := runtime.EncodeOrDie(clientscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), &v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
			Labels: map[string]string{
				LabelManagedBy:       ControllerName,
				v1.IsHeadlessService: "",
			},
		},
		Subsets: []v1.EndpointSubset{{
			Addresses: []v1.EndpointAddress{{IP: "1.2.3.4", NodeName: &emptyNodeName, TargetRef: &v1.ObjectReference{Kind: "Pod", Name: "pod0", Namespace: ns}}},
			Ports:     []v1.EndpointPort{{Port: 8080, Protocol: "UDP"}},
		}},
	})
	endpointsHandler.ValidateRequest(t, "/api/v1/namespaces/"+ns+"/endpoints/foo", "PUT", &data)
}

// TestSyncEndpointsProtocolSCTP 验证 SCTP 协议 Service 生成携带 SCTP 端口的 Endpoints。
func TestSyncEndpointsProtocolSCTP(t *testing.T) {
	ns := "other"
	testServer, endpointsHandler := makeTestServer(t, ns)
	defer testServer.Close()
	tCtx := ktesting.Init(t)
	endpoints := newController(tCtx, testServer.URL, 0*time.Second)
	endpoints.endpointsStore.Add(&v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
		},
		Subsets: []v1.EndpointSubset{{
			Addresses: []v1.EndpointAddress{{IP: "6.7.8.9", NodeName: &emptyNodeName}},
			Ports:     []v1.EndpointPort{{Port: 1000, Protocol: "SCTP"}},
		}},
	})
	addPods(endpoints.podStore, ns, 1, 1, 0, ipv4only)
	endpoints.serviceStore.Add(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns},
		Spec: v1.ServiceSpec{
			Selector:   map[string]string{},
			Ports:      []v1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(8080), Protocol: "SCTP"}},
			IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
		},
	})
	err := endpoints.syncService(tCtx, ns+"/foo")
	if err != nil {
		t.Errorf("Unexpected error syncing service %v", err)
	}

	endpointsHandler.ValidateRequestCount(t, 1)
	data := runtime.EncodeOrDie(clientscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), &v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
			Labels: map[string]string{
				LabelManagedBy:       ControllerName,
				v1.IsHeadlessService: "",
			},
		},
		Subsets: []v1.EndpointSubset{{
			Addresses: []v1.EndpointAddress{{IP: "1.2.3.4", NodeName: &emptyNodeName, TargetRef: &v1.ObjectReference{Kind: "Pod", Name: "pod0", Namespace: ns}}},
			Ports:     []v1.EndpointPort{{Port: 8080, Protocol: "SCTP"}},
		}},
	})
	endpointsHandler.ValidateRequest(t, "/api/v1/namespaces/"+ns+"/endpoints/foo", "PUT", &data)
}

// TestSyncEndpointsProtocolMultiProtocolSamePort 验证同一端口号上的 TCP/UDP 两个 ServicePort
// 会在 Endpoints 中生成两个协议不同、端口相同的 EndpointPort。
//
// 对应 test/e2e/network/service.go 中的
// ConformanceIt("should serve endpoints on same port and different protocols")。
func TestSyncEndpointsProtocolMultiProtocolSamePort(t *testing.T) {
	ns := "other"
	testServer, endpointsHandler := makeTestServer(t, ns)
	defer testServer.Close()
	tCtx := ktesting.Init(t)
	endpoints := newController(tCtx, testServer.URL, 0*time.Second)
	endpoints.endpointsStore.Add(&v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
		},
	})
	addPods(endpoints.podStore, ns, 1, 1, 0, ipv4only)
	endpoints.serviceStore.Add(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns},
		Spec: v1.ServiceSpec{
			Selector: map[string]string{},
			Ports: []v1.ServicePort{
				{Port: 80, TargetPort: intstr.FromInt32(8080), Protocol: "TCP"},
				{Port: 80, TargetPort: intstr.FromInt32(8080), Protocol: "UDP"},
			},
			IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
		},
	})
	err := endpoints.syncService(tCtx, ns+"/foo")
	if err != nil {
		t.Errorf("Unexpected error syncing service %v", err)
	}

	endpointsHandler.ValidateRequestCount(t, 1)
	data := runtime.EncodeOrDie(clientscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), &v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
			Labels: map[string]string{
				LabelManagedBy:       ControllerName,
				v1.IsHeadlessService: "",
			},
		},
		Subsets: []v1.EndpointSubset{{
			Addresses: []v1.EndpointAddress{{IP: "1.2.3.4", NodeName: &emptyNodeName, TargetRef: &v1.ObjectReference{Kind: "Pod", Name: "pod0", Namespace: ns}}},
			Ports: []v1.EndpointPort{
				{Port: 8080, Protocol: "UDP"},
				{Port: 8080, Protocol: "TCP"},
			},
		}},
	})
	endpointsHandler.ValidateRequest(t, "/api/v1/namespaces/"+ns+"/endpoints/foo", "PUT", &data)
}

// TestSyncEndpointsItemsEmptySelectorSelectsAll 验证空 selector 会选中命名空间内所有 Pod。
func TestSyncEndpointsItemsEmptySelectorSelectsAll(t *testing.T) {
	ns := "other"
	testServer, endpointsHandler := makeTestServer(t, ns)
	defer testServer.Close()
	tCtx := ktesting.Init(t)
	endpoints := newController(tCtx, testServer.URL, 0*time.Second)
	endpoints.endpointsStore.Add(&v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
		},
		Subsets: []v1.EndpointSubset{},
	})
	addPods(endpoints.podStore, ns, 1, 1, 0, ipv4only)
	endpoints.serviceStore.Add(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns},
		Spec: v1.ServiceSpec{
			Selector:   map[string]string{},
			Ports:      []v1.ServicePort{{Port: 80, Protocol: "TCP", TargetPort: intstr.FromInt32(8080)}},
			IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
		},
	})
	err := endpoints.syncService(tCtx, ns+"/foo")
	if err != nil {
		t.Errorf("Unexpected error syncing service %v", err)
	}

	data := runtime.EncodeOrDie(clientscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), &v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
			Labels: map[string]string{
				LabelManagedBy:       ControllerName,
				v1.IsHeadlessService: "",
			},
		},
		Subsets: []v1.EndpointSubset{{
			Addresses: []v1.EndpointAddress{{IP: "1.2.3.4", NodeName: &emptyNodeName, TargetRef: &v1.ObjectReference{Kind: "Pod", Name: "pod0", Namespace: ns}}},
			Ports:     []v1.EndpointPort{{Port: 8080, Protocol: "TCP"}},
		}},
	})
	endpointsHandler.ValidateRequest(t, "/api/v1/namespaces/"+ns+"/endpoints/foo", "PUT", &data)
}

// TestSyncEndpointsItemsEmptySelectorSelectsAllNotReady 验证空 selector 下未就绪 Pod
// 被放入 NotReadyAddresses 而非 Addresses。
func TestSyncEndpointsItemsEmptySelectorSelectsAllNotReady(t *testing.T) {
	ns := "other"
	testServer, endpointsHandler := makeTestServer(t, ns)
	defer testServer.Close()

	tCtx := ktesting.Init(t)
	endpoints := newController(tCtx, testServer.URL, 0*time.Second)
	endpoints.endpointsStore.Add(&v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
		},
		Subsets: []v1.EndpointSubset{},
	})
	addPods(endpoints.podStore, ns, 0, 1, 1, ipv4only)
	endpoints.serviceStore.Add(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns},
		Spec: v1.ServiceSpec{
			Selector:   map[string]string{},
			Ports:      []v1.ServicePort{{Port: 80, Protocol: "TCP", TargetPort: intstr.FromInt32(8080)}},
			IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
		},
	})
	err := endpoints.syncService(tCtx, ns+"/foo")
	if err != nil {
		t.Errorf("Unexpected error syncing service %v", err)
	}

	data := runtime.EncodeOrDie(clientscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), &v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
			Labels: map[string]string{
				LabelManagedBy:       ControllerName,
				v1.IsHeadlessService: "",
			},
		},
		Subsets: []v1.EndpointSubset{{
			NotReadyAddresses: []v1.EndpointAddress{{IP: "1.2.3.4", NodeName: &emptyNodeName, TargetRef: &v1.ObjectReference{Kind: "Pod", Name: "pod0", Namespace: ns}}},
			Ports:             []v1.EndpointPort{{Port: 8080, Protocol: "TCP"}},
		}},
	})
	endpointsHandler.ValidateRequest(t, "/api/v1/namespaces/"+ns+"/endpoints/foo", "PUT", &data)
}

// TestSyncEndpointsItemsEmptySelectorSelectsAllMixed 验证空 selector 下就绪与未就绪 Pod
// 分别进入 Addresses 与 NotReadyAddresses。
func TestSyncEndpointsItemsEmptySelectorSelectsAllMixed(t *testing.T) {
	ns := "other"
	testServer, endpointsHandler := makeTestServer(t, ns)
	defer testServer.Close()

	tCtx := ktesting.Init(t)
	endpoints := newController(tCtx, testServer.URL, 0*time.Second)
	endpoints.endpointsStore.Add(&v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
		},
		Subsets: []v1.EndpointSubset{},
	})
	addPods(endpoints.podStore, ns, 1, 1, 1, ipv4only)
	endpoints.serviceStore.Add(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns},
		Spec: v1.ServiceSpec{
			Selector:   map[string]string{},
			Ports:      []v1.ServicePort{{Port: 80, Protocol: "TCP", TargetPort: intstr.FromInt32(8080)}},
			IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
		},
	})
	err := endpoints.syncService(tCtx, ns+"/foo")
	if err != nil {
		t.Errorf("Unexpected error syncing service %v", err)
	}

	data := runtime.EncodeOrDie(clientscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), &v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
			Labels: map[string]string{
				LabelManagedBy:       ControllerName,
				v1.IsHeadlessService: "",
			},
		},
		Subsets: []v1.EndpointSubset{{
			Addresses:         []v1.EndpointAddress{{IP: "1.2.3.4", NodeName: &emptyNodeName, TargetRef: &v1.ObjectReference{Kind: "Pod", Name: "pod0", Namespace: ns}}},
			NotReadyAddresses: []v1.EndpointAddress{{IP: "1.2.3.5", NodeName: &emptyNodeName, TargetRef: &v1.ObjectReference{Kind: "Pod", Name: "pod1", Namespace: ns}}},
			Ports:             []v1.EndpointPort{{Port: 8080, Protocol: "TCP"}},
		}},
	})
	endpointsHandler.ValidateRequest(t, "/api/v1/namespaces/"+ns+"/endpoints/foo", "PUT", &data)
}

// TestSyncEndpointsItemsPreexisting 验证已有的、由控制器管理的 Endpoints 内容陈旧时，
// 控制器通过 PUT 用新计算的子集覆盖它。
func TestSyncEndpointsItemsPreexisting(t *testing.T) {
	ns := "bar"
	testServer, endpointsHandler := makeTestServer(t, ns)
	defer testServer.Close()
	tCtx := ktesting.Init(t)
	endpoints := newController(tCtx, testServer.URL, 0*time.Second)
	endpoints.endpointsStore.Add(&v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
			Labels: map[string]string{
				LabelManagedBy: ControllerName,
			},
		},
		Subsets: []v1.EndpointSubset{{
			Addresses: []v1.EndpointAddress{{IP: "6.7.8.9", NodeName: &emptyNodeName}},
			Ports:     []v1.EndpointPort{{Port: 1000}},
		}},
	})
	addPods(endpoints.podStore, ns, 1, 1, 0, ipv4only)
	endpoints.serviceStore.Add(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns},
		Spec: v1.ServiceSpec{
			Selector:   map[string]string{"foo": "bar"},
			Ports:      []v1.ServicePort{{Port: 80, Protocol: "TCP", TargetPort: intstr.FromInt32(8080)}},
			IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
		},
	})
	err := endpoints.syncService(tCtx, ns+"/foo")
	if err != nil {
		t.Errorf("Unexpected error syncing service %v", err)
	}

	data := runtime.EncodeOrDie(clientscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), &v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
			Labels: map[string]string{
				LabelManagedBy:       ControllerName,
				v1.IsHeadlessService: "",
			},
		},
		Subsets: []v1.EndpointSubset{{
			Addresses: []v1.EndpointAddress{{IP: "1.2.3.4", NodeName: &emptyNodeName, TargetRef: &v1.ObjectReference{Kind: "Pod", Name: "pod0", Namespace: ns}}},
			Ports:     []v1.EndpointPort{{Port: 8080, Protocol: "TCP"}},
		}},
	})
	endpointsHandler.ValidateRequest(t, "/api/v1/namespaces/"+ns+"/endpoints/foo", "PUT", &data)
}

// TestSyncEndpointsItemsPreexistingIdentical 验证已有 Endpoints 与期望结果完全一致时
// 控制器不发出写请求（幂等性）。
func TestSyncEndpointsItemsPreexistingIdentical(t *testing.T) {
	ns := metav1.NamespaceDefault
	testServer, endpointsHandler := makeTestServer(t, ns)
	defer testServer.Close()
	tCtx := ktesting.Init(t)
	endpoints := newController(tCtx, testServer.URL, 0*time.Second)
	endpoints.endpointsStore.Add(&v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			ResourceVersion: "1",
			Name:            "foo",
			Namespace:       ns,
			Labels: map[string]string{
				LabelManagedBy: ControllerName,
			},
		},
		Subsets: []v1.EndpointSubset{{
			Addresses: []v1.EndpointAddress{{IP: "1.2.3.4", NodeName: &emptyNodeName, TargetRef: &v1.ObjectReference{Kind: "Pod", Name: "pod0", Namespace: ns}}},
			Ports:     []v1.EndpointPort{{Port: 8080, Protocol: "TCP"}},
		}},
	})
	addPods(endpoints.podStore, metav1.NamespaceDefault, 1, 1, 0, ipv4only)
	endpoints.serviceStore.Add(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: metav1.NamespaceDefault},
		Spec: v1.ServiceSpec{
			Selector:   map[string]string{"foo": "bar"},
			Ports:      []v1.ServicePort{{Port: 80, Protocol: "TCP", TargetPort: intstr.FromInt32(8080)}},
			IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
		},
	})
	err := endpoints.syncService(tCtx, ns+"/foo")
	if err != nil {
		t.Errorf("Unexpected error syncing service %v", err)
	}
	endpointsHandler.ValidateRequestCount(t, 0)
}

// TestSyncEndpointsItemsPreexistingIdenticalUnsorted 验证 Endpoints 子集顺序与当前
// RepackSubsets 结果不同但语义相同时仍被视为无变化，避免无谓的重新同步。
func TestSyncEndpointsItemsPreexistingIdenticalUnsorted(t *testing.T) {
	ns := metav1.NamespaceDefault
	testServer, endpointsHandler := makeTestServer(t, ns)
	defer testServer.Close()
	tCtx := ktesting.Init(t)
	endpoints := newController(tCtx, testServer.URL, 0*time.Second)
	pod0 := testPod(ns, 0, 1, true, ipv4only)
	pod1 := testPod(ns, 1, 1, true, ipv4only)

	subsets := []v1.EndpointSubset{{
		Addresses: []v1.EndpointAddress{
			// 已知该顺序与当前 hash 算法打包出的顺序不同
			{IP: pod1.Status.PodIPs[0].IP, NodeName: &emptyNodeName, TargetRef: &v1.ObjectReference{Kind: "Pod", Name: pod1.Name, Namespace: ns}},
			{IP: pod0.Status.PodIPs[0].IP, NodeName: &emptyNodeName, TargetRef: &v1.ObjectReference{Kind: "Pod", Name: pod0.Name, Namespace: ns}},
		},
		Ports: []v1.EndpointPort{{Port: 8080, Protocol: "TCP"}},
	}}

	// 先断言这两组子集确实不是排序后的状态，确保用例前提成立；
	// 这样才能证明“未排序”本身不会触发重新同步。
	repacked := endptspkg.RepackSubsets(subsets)
	if reflect.DeepEqual(subsets, repacked) {
		t.Errorf("subsets were already in sorted order")
	}

	endpoints.endpointsStore.Add(&v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			ResourceVersion: "1",
			Name:            "foo",
			Namespace:       ns,
			Labels: map[string]string{
				LabelManagedBy: ControllerName,
			},
		},
		Subsets: subsets,
	})

	endpoints.podStore.Add(pod0)
	endpoints.podStore.Add(pod1)

	endpoints.serviceStore.Add(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: metav1.NamespaceDefault},
		Spec: v1.ServiceSpec{
			Selector:   map[string]string{"foo": "bar"},
			Ports:      []v1.ServicePort{{Port: 80, Protocol: "TCP", TargetPort: intstr.FromInt32(8080)}},
			IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
		},
	})
	err := endpoints.syncService(tCtx, ns+"/foo")
	if err != nil {
		t.Errorf("Unexpected error syncing service %v", err)
	}

	// 同步应当是一次 no-op。
	endpointsHandler.ValidateRequestCount(t, 0)
}

// TestSyncEndpointsItems 验证多 Pod、多端口场景下生成的 Endpoints 子集内容，
// 同时断言写请求为 POST（Endpoints 不存在）且请求体与排序后的期望对象一致。
func TestSyncEndpointsItems(t *testing.T) {
	ns := "other"
	testServer, endpointsHandler := makeTestServer(t, ns)
	defer testServer.Close()
	tCtx := ktesting.Init(t)
	endpoints := newController(tCtx, testServer.URL, 0*time.Second)
	addPods(endpoints.podStore, ns, 3, 2, 0, ipv4only)
	addPods(endpoints.podStore, "blah", 5, 2, 0, ipv4only) // 再造一些其他命名空间的 Pod，确保它们不会被选中。

	endpoints.serviceStore.Add(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns},
		Spec: v1.ServiceSpec{
			Selector: map[string]string{"foo": "bar"},
			Ports: []v1.ServicePort{
				{Name: "port0", Port: 80, Protocol: "TCP", TargetPort: intstr.FromInt32(8080)},
				{Name: "port1", Port: 88, Protocol: "TCP", TargetPort: intstr.FromInt32(8088)},
			},
			IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
		},
	})
	err := endpoints.syncService(tCtx, "other/foo")
	if err != nil {
		t.Errorf("Unexpected error syncing service %v", err)
	}

	expectedSubsets := []v1.EndpointSubset{{
		Addresses: []v1.EndpointAddress{
			{IP: "1.2.3.4", NodeName: &emptyNodeName, TargetRef: &v1.ObjectReference{Kind: "Pod", Name: "pod0", Namespace: ns}},
			{IP: "1.2.3.5", NodeName: &emptyNodeName, TargetRef: &v1.ObjectReference{Kind: "Pod", Name: "pod1", Namespace: ns}},
			{IP: "1.2.3.6", NodeName: &emptyNodeName, TargetRef: &v1.ObjectReference{Kind: "Pod", Name: "pod2", Namespace: ns}},
		},
		Ports: []v1.EndpointPort{
			{Name: "port0", Port: 8080, Protocol: "TCP"},
			{Name: "port1", Port: 8088, Protocol: "TCP"},
		},
	}}
	data := runtime.EncodeOrDie(clientscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), &v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			ResourceVersion: "",
			Name:            "foo",
			Labels: map[string]string{
				LabelManagedBy:       ControllerName,
				v1.IsHeadlessService: "",
			},
		},
		Subsets: endptspkg.SortSubsets(expectedSubsets),
	})
	endpointsHandler.ValidateRequestCount(t, 1)
	endpointsHandler.ValidateRequest(t, "/api/v1/namespaces/"+ns+"/endpoints", "POST", &data)
}

// TestSyncEndpointsItemsWithLabels 验证 Service 的自定义标签会被复制到新建的 Endpoints，
// 并同时附加控制器管理标签与 headless 标签。
func TestSyncEndpointsItemsWithLabels(t *testing.T) {
	ns := "other"
	testServer, endpointsHandler := makeTestServer(t, ns)
	defer testServer.Close()
	tCtx := ktesting.Init(t)
	endpoints := newController(tCtx, testServer.URL, 0*time.Second)
	addPods(endpoints.podStore, ns, 3, 2, 0, ipv4only)
	serviceLabels := map[string]string{"foo": "bar"}
	endpoints.serviceStore.Add(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: ns,
			Labels:    serviceLabels,
		},
		Spec: v1.ServiceSpec{
			Selector: map[string]string{"foo": "bar"},
			Ports: []v1.ServicePort{
				{Name: "port0", Port: 80, Protocol: "TCP", TargetPort: intstr.FromInt32(8080)},
				{Name: "port1", Port: 88, Protocol: "TCP", TargetPort: intstr.FromInt32(8088)},
			},
			IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
		},
	})
	err := endpoints.syncService(tCtx, ns+"/foo")
	if err != nil {
		t.Errorf("Unexpected error syncing service %v", err)
	}

	expectedSubsets := []v1.EndpointSubset{{
		Addresses: []v1.EndpointAddress{
			{IP: "1.2.3.4", NodeName: &emptyNodeName, TargetRef: &v1.ObjectReference{Kind: "Pod", Name: "pod0", Namespace: ns}},
			{IP: "1.2.3.5", NodeName: &emptyNodeName, TargetRef: &v1.ObjectReference{Kind: "Pod", Name: "pod1", Namespace: ns}},
			{IP: "1.2.3.6", NodeName: &emptyNodeName, TargetRef: &v1.ObjectReference{Kind: "Pod", Name: "pod2", Namespace: ns}},
		},
		Ports: []v1.EndpointPort{
			{Name: "port0", Port: 8080, Protocol: "TCP"},
			{Name: "port1", Port: 8088, Protocol: "TCP"},
		},
	}}

	serviceLabels[v1.IsHeadlessService] = ""
	serviceLabels[LabelManagedBy] = ControllerName
	data := runtime.EncodeOrDie(clientscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), &v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			ResourceVersion: "",
			Name:            "foo",
			Labels:          serviceLabels,
		},
		Subsets: endptspkg.SortSubsets(expectedSubsets),
	})
	endpointsHandler.ValidateRequestCount(t, 1)
	endpointsHandler.ValidateRequest(t, "/api/v1/namespaces/"+ns+"/endpoints", "POST", &data)
}

// TestSyncEndpointsItemsPreexistingLabelsChange 验证 Service 标签变化时，已有 Endpoints
// 的标签会被更新（PUT），且子集内容一并刷新。
func TestSyncEndpointsItemsPreexistingLabelsChange(t *testing.T) {
	ns := "bar"
	testServer, endpointsHandler := makeTestServer(t, ns)
	defer testServer.Close()
	tCtx := ktesting.Init(t)
	endpoints := newController(tCtx, testServer.URL, 0*time.Second)
	endpoints.endpointsStore.Add(&v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
			Labels: map[string]string{
				"foo": "bar",
			},
		},
		Subsets: []v1.EndpointSubset{{
			Addresses: []v1.EndpointAddress{{IP: "6.7.8.9", NodeName: &emptyNodeName}},
			Ports:     []v1.EndpointPort{{Port: 1000}},
		}},
	})
	addPods(endpoints.podStore, ns, 1, 1, 0, ipv4only)
	serviceLabels := map[string]string{"baz": "blah"}
	endpoints.serviceStore.Add(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: ns,
			Labels:    serviceLabels,
		},
		Spec: v1.ServiceSpec{
			Selector:   map[string]string{"foo": "bar"},
			Ports:      []v1.ServicePort{{Port: 80, Protocol: "TCP", TargetPort: intstr.FromInt32(8080)}},
			IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
		},
	})
	err := endpoints.syncService(tCtx, ns+"/foo")
	if err != nil {
		t.Errorf("Unexpected error syncing service %v", err)
	}

	serviceLabels[v1.IsHeadlessService] = ""
	serviceLabels[LabelManagedBy] = ControllerName
	data := runtime.EncodeOrDie(clientscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), &v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
			Labels:          serviceLabels,
		},
		Subsets: []v1.EndpointSubset{{
			Addresses: []v1.EndpointAddress{{IP: "1.2.3.4", NodeName: &emptyNodeName, TargetRef: &v1.ObjectReference{Kind: "Pod", Name: "pod0", Namespace: ns}}},
			Ports:     []v1.EndpointPort{{Port: 8080, Protocol: "TCP"}},
		}},
	})
	endpointsHandler.ValidateRequest(t, "/api/v1/namespaces/"+ns+"/endpoints/foo", "PUT", &data)
}

// TestWaitsForAllInformersToBeSynced2 验证控制器必须等 Pod、Service、Endpoints 三个
// informer 全部同步后才处理工作队列：任一 HasSynced 为 false 都不应产生写请求。
func TestWaitsForAllInformersToBeSynced2(t *testing.T) {
	var tests = []struct {
		// 三个字段分别替换对应 informer 的 HasSynced 检查；shouldUpdateEndpoints 表示是否预期写请求。
		podsSynced            func() bool
		servicesSynced        func() bool
		endpointsSynced       func() bool
		shouldUpdateEndpoints bool
	}{
		// 依次覆盖：Pod 未同步、Service 未同步、Endpoints 未同步、三者全部同步。
		{neverReady, alwaysReady, alwaysReady, false},
		{alwaysReady, neverReady, alwaysReady, false},
		{alwaysReady, alwaysReady, neverReady, false},
		{alwaysReady, alwaysReady, alwaysReady, true},
	}

	for _, test := range tests {
		func() {
			ns := "other"
			testServer, endpointsHandler := makeTestServer(t, ns)
			defer testServer.Close()
			tCtx := ktesting.Init(t)
			endpoints := newController(tCtx, testServer.URL, 0*time.Second)
			addPods(endpoints.podStore, ns, 1, 1, 0, ipv4only)

			service := &v1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns},
				Spec: v1.ServiceSpec{
					Selector:   map[string]string{},
					Ports:      []v1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(8080), Protocol: "TCP"}},
					IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
				},
			}
			endpoints.serviceStore.Add(service)
			endpoints.onServiceUpdate(service)
			endpoints.podsSynced = test.podsSynced
			endpoints.servicesSynced = test.servicesSynced
			endpoints.endpointsSynced = test.endpointsSynced
			endpoints.workerLoopPeriod = 10 * time.Millisecond
			go endpoints.Run(tCtx, 1)

			// cache.WaitForNamedCacheSync 的轮询周期是 100ms，而 endpoints worker 的周期是 10ms。
			// 为捕获包括意外写请求在内的所有行为，等待时间至少要覆盖一个同步周期
			// 与一个 worker 周期，并留出一定余量。
			time.Sleep(150 * time.Millisecond)
			if test.shouldUpdateEndpoints {
				// 为避免抖动，轮询等待队列清空，最长等待 1 秒。
				wait.PollImmediate(50*time.Millisecond, 1*time.Second, func() (bool, error) {
					return endpoints.queue.Len() == 0, nil
				})
				endpointsHandler.ValidateRequestCount(t, 1)
			} else {
				endpointsHandler.ValidateRequestCount(t, 0)
			}
		}()
	}
}

// TestSyncEndpointsHeadlessService 验证 ClusterIP=None 的 headless Service：自定义标签
// 会被保留并附加 headless 标签，且同步过程不会修改传入的 Service 对象。
func TestSyncEndpointsHeadlessService(t *testing.T) {
	ns := "headless"
	testServer, endpointsHandler := makeTestServer(t, ns)
	defer testServer.Close()
	tCtx := ktesting.Init(t)
	endpoints := newController(tCtx, testServer.URL, 0*time.Second)
	endpoints.endpointsStore.Add(&v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
		},
		Subsets: []v1.EndpointSubset{{
			Addresses: []v1.EndpointAddress{{IP: "6.7.8.9", NodeName: &emptyNodeName}},
			Ports:     []v1.EndpointPort{{Port: 1000, Protocol: "TCP"}},
		}},
	})
	addPods(endpoints.podStore, ns, 1, 1, 0, ipv4only)
	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns, Labels: map[string]string{"a": "b"}},
		Spec: v1.ServiceSpec{
			Selector:   map[string]string{},
			ClusterIP:  api.ClusterIPNone,
			Ports:      []v1.ServicePort{},
			IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
		},
	}
	originalService := service.DeepCopy()
	endpoints.serviceStore.Add(service)
	err := endpoints.syncService(tCtx, ns+"/foo")
	if err != nil {
		t.Errorf("Unexpected error syncing service %v", err)
	}
	data := runtime.EncodeOrDie(clientscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), &v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
			Labels: map[string]string{
				LabelManagedBy:       ControllerName,
				"a":                  "b",
				v1.IsHeadlessService: "",
			},
		},
		Subsets: []v1.EndpointSubset{{
			Addresses: []v1.EndpointAddress{{IP: "1.2.3.4", NodeName: &emptyNodeName, TargetRef: &v1.ObjectReference{Kind: "Pod", Name: "pod0", Namespace: ns}}},
			Ports:     []v1.EndpointPort{},
		}},
	})
	// 用 DeepCopy 对比防止控制器就地修改共享 informer 缓存中的 Service 对象。
	if !reflect.DeepEqual(originalService, service) {
		t.Fatalf("syncing endpoints changed service: %s", cmp.Diff(service, originalService))
	}
	endpointsHandler.ValidateRequestCount(t, 1)
	endpointsHandler.ValidateRequest(t, "/api/v1/namespaces/"+ns+"/endpoints/foo", "PUT", &data)
}

// TestSyncEndpointsItemsExcludeNotReadyPodsWithRestartPolicyNeverAndPhaseFailed 验证
// RestartPolicy=Never 且 Phase=Failed 的终态 Pod 即使未就绪也不得进入 NotReadyAddresses。
func TestSyncEndpointsItemsExcludeNotReadyPodsWithRestartPolicyNeverAndPhaseFailed(t *testing.T) {
	ns := "other"
	testServer, endpointsHandler := makeTestServer(t, ns)
	defer testServer.Close()

	tCtx := ktesting.Init(t)
	endpoints := newController(tCtx, testServer.URL, 0*time.Second)
	endpoints.endpointsStore.Add(&v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
			Labels: map[string]string{
				LabelManagedBy: ControllerName,
				"foo":          "bar",
			},
		},
		Subsets: []v1.EndpointSubset{},
	})
	addNotReadyPodsWithSpecifiedRestartPolicyAndPhase(endpoints.podStore, ns, 1, 1, v1.RestartPolicyNever, v1.PodFailed)
	endpoints.serviceStore.Add(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns},
		Spec: v1.ServiceSpec{
			Selector:   map[string]string{"foo": "bar"},
			Ports:      []v1.ServicePort{{Port: 80, Protocol: "TCP", TargetPort: intstr.FromInt32(8080)}},
			IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
		},
	})
	err := endpoints.syncService(tCtx, ns+"/foo")
	if err != nil {
		t.Errorf("Unexpected error syncing service %v", err)
	}
	data := runtime.EncodeOrDie(clientscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), &v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
			Labels: map[string]string{
				LabelManagedBy:       ControllerName,
				v1.IsHeadlessService: "",
			},
		},
		Subsets: []v1.EndpointSubset{},
	})
	endpointsHandler.ValidateRequest(t, "/api/v1/namespaces/"+ns+"/endpoints/foo", "PUT", &data)
}

// TestSyncEndpointsItemsExcludeNotReadyPodsWithRestartPolicyNeverAndPhaseSucceeded 验证
// RestartPolicy=Never 且 Phase=Succeeded 的终态 Pod 同样被排除在 Endpoints 之外。
func TestSyncEndpointsItemsExcludeNotReadyPodsWithRestartPolicyNeverAndPhaseSucceeded(t *testing.T) {
	ns := "other"
	testServer, endpointsHandler := makeTestServer(t, ns)
	defer testServer.Close()

	tCtx := ktesting.Init(t)
	endpoints := newController(tCtx, testServer.URL, 0*time.Second)
	endpoints.endpointsStore.Add(&v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
			Labels: map[string]string{
				"foo": "bar",
			},
		},
		Subsets: []v1.EndpointSubset{},
	})
	addNotReadyPodsWithSpecifiedRestartPolicyAndPhase(endpoints.podStore, ns, 1, 1, v1.RestartPolicyNever, v1.PodSucceeded)
	endpoints.serviceStore.Add(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns},
		Spec: v1.ServiceSpec{
			Selector:   map[string]string{"foo": "bar"},
			Ports:      []v1.ServicePort{{Port: 80, Protocol: "TCP", TargetPort: intstr.FromInt32(8080)}},
			IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
		},
	})
	err := endpoints.syncService(tCtx, ns+"/foo")
	if err != nil {
		t.Errorf("Unexpected error syncing service %v", err)
	}
	data := runtime.EncodeOrDie(clientscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), &v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
			Labels: map[string]string{
				LabelManagedBy:       ControllerName,
				v1.IsHeadlessService: "",
			},
		},
		Subsets: []v1.EndpointSubset{},
	})
	endpointsHandler.ValidateRequest(t, "/api/v1/namespaces/"+ns+"/endpoints/foo", "PUT", &data)
}

// TestSyncEndpointsItemsExcludeNotReadyPodsWithRestartPolicyOnFailureAndPhaseSucceeded
// 验证 RestartPolicy=OnFailure 且 Phase=Succeeded 的成功终态 Pod 也被排除。
func TestSyncEndpointsItemsExcludeNotReadyPodsWithRestartPolicyOnFailureAndPhaseSucceeded(t *testing.T) {
	ns := "other"
	testServer, endpointsHandler := makeTestServer(t, ns)
	defer testServer.Close()

	tCtx := ktesting.Init(t)
	endpoints := newController(tCtx, testServer.URL, 0*time.Second)
	endpoints.endpointsStore.Add(&v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
			Labels: map[string]string{
				"foo": "bar",
			},
		},
		Subsets: []v1.EndpointSubset{},
	})
	addNotReadyPodsWithSpecifiedRestartPolicyAndPhase(endpoints.podStore, ns, 1, 1, v1.RestartPolicyOnFailure, v1.PodSucceeded)
	endpoints.serviceStore.Add(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns},
		Spec: v1.ServiceSpec{
			Selector:   map[string]string{"foo": "bar"},
			Ports:      []v1.ServicePort{{Port: 80, Protocol: "TCP", TargetPort: intstr.FromInt32(8080)}},
			IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
		},
	})
	err := endpoints.syncService(tCtx, ns+"/foo")
	if err != nil {
		t.Errorf("Unexpected error syncing service %v", err)
	}

	data := runtime.EncodeOrDie(clientscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), &v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
			Labels: map[string]string{
				LabelManagedBy:       ControllerName,
				v1.IsHeadlessService: "",
			},
		},
		Subsets: []v1.EndpointSubset{},
	})
	endpointsHandler.ValidateRequest(t, "/api/v1/namespaces/"+ns+"/endpoints/foo", "PUT", &data)
}

// TestSyncEndpointsHeadlessWithoutPort 验证无端口的 headless Service 仍会创建 Endpoints，
// 其中子集的 Ports 为 nil。
func TestSyncEndpointsHeadlessWithoutPort(t *testing.T) {
	ns := metav1.NamespaceDefault
	testServer, endpointsHandler := makeTestServer(t, ns)
	defer testServer.Close()
	tCtx := ktesting.Init(t)
	endpoints := newController(tCtx, testServer.URL, 0*time.Second)
	endpoints.serviceStore.Add(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns},
		Spec: v1.ServiceSpec{
			Selector:   map[string]string{"foo": "bar"},
			ClusterIP:  "None",
			Ports:      nil,
			IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
		},
	})
	addPods(endpoints.podStore, ns, 1, 1, 0, ipv4only)

	err := endpoints.syncService(tCtx, ns+"/foo")
	if err != nil {
		t.Errorf("Unexpected error syncing service %v", err)
	}
	endpointsHandler.ValidateRequestCount(t, 1)
	data := runtime.EncodeOrDie(clientscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), &v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name: "foo",
			Labels: map[string]string{
				LabelManagedBy:       ControllerName,
				v1.IsHeadlessService: "",
			},
		},
		Subsets: []v1.EndpointSubset{{
			Addresses: []v1.EndpointAddress{{IP: "1.2.3.4", NodeName: &emptyNodeName, TargetRef: &v1.ObjectReference{Kind: "Pod", Name: "pod0", Namespace: ns}}},
			Ports:     nil,
		}},
	})
	endpointsHandler.ValidateRequest(t, "/api/v1/namespaces/"+ns+"/endpoints", "POST", &data)
}

// TestPodToEndpointAddressForService 以表驱动方式验证 Pod IP 族选择逻辑：根据 Service 的
// ClusterIP/IPFamilies 与 Pod 的 IP 列表选出匹配的 IP，同时验证非法 IP 会被修正以及
// TargetRef 各字段的填充。
func TestPodToEndpointAddressForService(t *testing.T) {
	ipv4 := v1.IPv4Protocol
	ipv6 := v1.IPv6Protocol

	testCases := []struct {
		// name 为用例名；ipFamilies 模拟集群启用的 IP 族；service 为待匹配 Service；
		// expectedEndpointFamily 为期望选中的 IP 族；expectError 表示是否预期报错。
		name                   string
		ipFamilies             []v1.IPFamily
		service                v1.Service
		expectedEndpointFamily v1.IPFamily
		expectError            bool
	}{
		{
			name:       "v4 service, in a single stack cluster",
			ipFamilies: ipv4only,
			service: v1.Service{
				Spec: v1.ServiceSpec{
					ClusterIP:  "10.0.0.1",
					IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
				},
			},
			expectedEndpointFamily: ipv4,
		},
		{
			name:       "v4 service, in a dual stack cluster",
			ipFamilies: ipv4ipv6,
			service: v1.Service{
				Spec: v1.ServiceSpec{
					ClusterIP:  "10.0.0.1",
					IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
				},
			},
			expectedEndpointFamily: ipv4,
		},
		{
			name:       "v4 service, in a dual stack ipv6-primary cluster",
			ipFamilies: ipv6ipv4,
			service: v1.Service{
				Spec: v1.ServiceSpec{
					ClusterIP:  "10.0.0.1",
					IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
				},
			},
			expectedEndpointFamily: ipv4,
		},
		{
			name:       "v4 headless service, in a single stack cluster",
			ipFamilies: ipv4only,
			service: v1.Service{
				Spec: v1.ServiceSpec{
					ClusterIP:  v1.ClusterIPNone,
					IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
				},
			},
			expectedEndpointFamily: ipv4,
		},
		{
			name:       "v4 headless service, in a dual stack cluster",
			ipFamilies: ipv4ipv6,
			service: v1.Service{
				Spec: v1.ServiceSpec{
					ClusterIP:  v1.ClusterIPNone,
					IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
				},
			},
			expectedEndpointFamily: ipv4,
		},
		{
			name:       "v6 service, in a dual stack cluster",
			ipFamilies: ipv4ipv6,
			service: v1.Service{
				Spec: v1.ServiceSpec{
					ClusterIP:  "3000::1",
					IPFamilies: []v1.IPFamily{v1.IPv6Protocol},
				},
			},
			expectedEndpointFamily: ipv6,
		},
		{
			name:       "v6 headless service, in a single stack cluster",
			ipFamilies: ipv6only,
			service: v1.Service{
				Spec: v1.ServiceSpec{
					ClusterIP:  v1.ClusterIPNone,
					IPFamilies: []v1.IPFamily{v1.IPv6Protocol},
				},
			},
			expectedEndpointFamily: ipv6,
		},
		{
			name:       "v6 headless service, in a dual stack cluster",
			ipFamilies: ipv4ipv6,
			service: v1.Service{
				Spec: v1.ServiceSpec{
					ClusterIP:  v1.ClusterIPNone,
					IPFamilies: []v1.IPFamily{v1.IPv6Protocol}, // <- 该字段由 API Server 的默认值逻辑填充
				},
			},
			expectedEndpointFamily: ipv6,
		},
		{
			name:       "v6 service, in a v4 only cluster",
			ipFamilies: ipv4only,
			service: v1.Service{
				Spec: v1.ServiceSpec{
					ClusterIP:  "3000::1",
					IPFamilies: []v1.IPFamily{v1.IPv6Protocol},
				},
			},
			expectError: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			podStore := cache.NewStore(cache.DeletionHandlingMetaNamespaceKeyFunc)
			ns := "test"
			// 使用 addBadIPPod 构造带非法 IP 的 Pod，验证 podToEndpointAddressForService
			// 会修正该 IP 而不是原样透传。
			addBadIPPod(podStore, ns, tc.ipFamilies)
			pods := podStore.List()
			if len(pods) != 1 {
				t.Fatalf("podStore size: expected: %d, got: %d", 1, len(pods))
			}
			pod := pods[0].(*v1.Pod)
			epa, err := podToEndpointAddressForService(&tc.service, pod)

			// 错误检查分两步：先识别“意外的错误”与“缺失的预期错误”，再进入地址内容校验。
			if err != nil && !tc.expectError {
				t.Fatalf("podToEndpointAddressForService returned unexpected error %v", err)
			}

			if err == nil && tc.expectError {
				t.Fatalf("podToEndpointAddressForService should have returned error but it did not")
			}

			if err != nil && tc.expectError {
				return
			}

			// 通过 IsIPv6String 判断地址族而非直接比较具体 IP，便于适配确定性生成的地址。
			if utilnet.IsIPv6String(epa.IP) != (tc.expectedEndpointFamily == ipv6) {
				t.Fatalf("IP: expected %s, got: %s", tc.expectedEndpointFamily, epa.IP)
			}
			// 非法 IP 若未被修正仍会以 0 开头；这里断言修正后的地址不再是非法格式。
			if strings.HasPrefix(epa.IP, "0") {
				t.Fatalf("IP: expected valid, got: %s", epa.IP)
			}
			// NodeName 直接取自 Pod；TargetRef 只填身份信息，ResourceVersion 必须留空。
			if *(epa.NodeName) != pod.Spec.NodeName {
				t.Fatalf("NodeName: expected: %s, got: %s", pod.Spec.NodeName, *(epa.NodeName))
			}
			if epa.TargetRef.Kind != "Pod" {
				t.Fatalf("TargetRef.Kind: expected: %s, got: %s", "Pod", epa.TargetRef.Kind)
			}
			if epa.TargetRef.Namespace != pod.ObjectMeta.Namespace {
				t.Fatalf("TargetRef.Namespace: expected: %s, got: %s", pod.ObjectMeta.Namespace, epa.TargetRef.Namespace)
			}
			if epa.TargetRef.Name != pod.ObjectMeta.Name {
				t.Fatalf("TargetRef.Name: expected: %s, got: %s", pod.ObjectMeta.Name, epa.TargetRef.Name)
			}
			if epa.TargetRef.UID != pod.ObjectMeta.UID {
				t.Fatalf("TargetRef.UID: expected: %s, got: %s", pod.ObjectMeta.UID, epa.TargetRef.UID)
			}
			if epa.TargetRef.ResourceVersion != "" {
				t.Fatalf("TargetRef.ResourceVersion: expected empty, got: %s", epa.TargetRef.ResourceVersion)
			}
		})
	}

}

// TestLastTriggerChangeTimeAnnotation 验证 Service 创建时间会被写入 Endpoints 的
// EndpointsLastChangeTriggerTime 注解（RFC3339Nano 格式）。
func TestLastTriggerChangeTimeAnnotation(t *testing.T) {
	ns := "other"
	testServer, endpointsHandler := makeTestServer(t, ns)
	defer testServer.Close()

	tCtx := ktesting.Init(t)
	endpoints := newController(tCtx, testServer.URL, 0*time.Second)
	endpoints.endpointsStore.Add(&v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
		},
		Subsets: []v1.EndpointSubset{{
			Addresses: []v1.EndpointAddress{{IP: "6.7.8.9", NodeName: &emptyNodeName}},
			Ports:     []v1.EndpointPort{{Port: 1000, Protocol: "TCP"}},
		}},
	})
	addPods(endpoints.podStore, ns, 1, 1, 0, ipv4only)
	endpoints.serviceStore.Add(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns, CreationTimestamp: metav1.NewTime(triggerTime)},
		Spec: v1.ServiceSpec{
			Selector:   map[string]string{},
			Ports:      []v1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(8080), Protocol: "TCP"}},
			IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
		},
	})
	err := endpoints.syncService(tCtx, ns+"/foo")
	if err != nil {
		t.Errorf("Unexpected error syncing service %v", err)
	}

	endpointsHandler.ValidateRequestCount(t, 1)
	data := runtime.EncodeOrDie(clientscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), &v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
			Annotations: map[string]string{
				v1.EndpointsLastChangeTriggerTime: triggerTimeString,
			},
			Labels: map[string]string{
				LabelManagedBy:       ControllerName,
				v1.IsHeadlessService: "",
			},
		},
		Subsets: []v1.EndpointSubset{{
			Addresses: []v1.EndpointAddress{{IP: "1.2.3.4", NodeName: &emptyNodeName, TargetRef: &v1.ObjectReference{Kind: "Pod", Name: "pod0", Namespace: ns}}},
			Ports:     []v1.EndpointPort{{Port: 8080, Protocol: "TCP"}},
		}},
	})
	endpointsHandler.ValidateRequest(t, "/api/v1/namespaces/"+ns+"/endpoints/foo", "PUT", &data)
}

// TestLastTriggerChangeTimeAnnotation_AnnotationOverridden 验证已有的旧触发时间注解
// 会被新的 Service 创建时间覆盖，而不是被保留或删除。
func TestLastTriggerChangeTimeAnnotation_AnnotationOverridden(t *testing.T) {
	ns := "other"
	testServer, endpointsHandler := makeTestServer(t, ns)
	defer testServer.Close()

	tCtx := ktesting.Init(t)
	endpoints := newController(tCtx, testServer.URL, 0*time.Second)
	endpoints.endpointsStore.Add(&v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
			Annotations: map[string]string{
				v1.EndpointsLastChangeTriggerTime: oldTriggerTimeString,
			},
		},
		Subsets: []v1.EndpointSubset{{
			Addresses: []v1.EndpointAddress{{IP: "6.7.8.9", NodeName: &emptyNodeName}},
			Ports:     []v1.EndpointPort{{Port: 1000, Protocol: "TCP"}},
		}},
	})
	addPods(endpoints.podStore, ns, 1, 1, 0, ipv4only)
	endpoints.serviceStore.Add(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns, CreationTimestamp: metav1.NewTime(triggerTime)},
		Spec: v1.ServiceSpec{
			Selector:   map[string]string{},
			Ports:      []v1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(8080), Protocol: "TCP"}},
			IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
		},
	})
	err := endpoints.syncService(tCtx, ns+"/foo")
	if err != nil {
		t.Errorf("Unexpected error syncing service %v", err)
	}

	endpointsHandler.ValidateRequestCount(t, 1)
	data := runtime.EncodeOrDie(clientscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), &v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
			Annotations: map[string]string{
				v1.EndpointsLastChangeTriggerTime: triggerTimeString,
			},
			Labels: map[string]string{
				LabelManagedBy:       ControllerName,
				v1.IsHeadlessService: "",
			},
		},
		Subsets: []v1.EndpointSubset{{
			Addresses: []v1.EndpointAddress{{IP: "1.2.3.4", NodeName: &emptyNodeName, TargetRef: &v1.ObjectReference{Kind: "Pod", Name: "pod0", Namespace: ns}}},
			Ports:     []v1.EndpointPort{{Port: 8080, Protocol: "TCP"}},
		}},
	})
	endpointsHandler.ValidateRequest(t, "/api/v1/namespaces/"+ns+"/endpoints/foo", "PUT", &data)
}

// TestLastTriggerChangeTimeAnnotation_AnnotationCleared 验证当 Pod 与 Service 均没有
// 触发时间信息时，Endpoints 上原有的触发时间注解会被清除。
func TestLastTriggerChangeTimeAnnotation_AnnotationCleared(t *testing.T) {
	ns := "other"
	testServer, endpointsHandler := makeTestServer(t, ns)
	defer testServer.Close()

	tCtx := ktesting.Init(t)
	endpoints := newController(tCtx, testServer.URL, 0*time.Second)
	endpoints.endpointsStore.Add(&v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
			Annotations: map[string]string{
				v1.EndpointsLastChangeTriggerTime: triggerTimeString,
			},
		},
		Subsets: []v1.EndpointSubset{{
			Addresses: []v1.EndpointAddress{{IP: "6.7.8.9", NodeName: &emptyNodeName}},
			Ports:     []v1.EndpointPort{{Port: 1000, Protocol: "TCP"}},
		}},
	})
	// Pod 与 Service 都没有触发时间，因此期望注解被清除。
	addPods(endpoints.podStore, ns, 1, 1, 0, ipv4only)
	endpoints.serviceStore.Add(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns},
		Spec: v1.ServiceSpec{
			Selector:   map[string]string{},
			Ports:      []v1.ServicePort{{Port: 80, TargetPort: intstr.FromInt32(8080), Protocol: "TCP"}},
			IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
		},
	})
	err := endpoints.syncService(tCtx, ns+"/foo")
	if err != nil {
		t.Errorf("Unexpected error syncing service %v", err)
	}

	endpointsHandler.ValidateRequestCount(t, 1)
	data := runtime.EncodeOrDie(clientscheme.Codecs.LegacyCodec(v1.SchemeGroupVersion), &v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
			Labels: map[string]string{
				LabelManagedBy:       ControllerName,
				v1.IsHeadlessService: "",
			}, // 期望的 Endpoints 不再带有该注解。
		},
		Subsets: []v1.EndpointSubset{{
			Addresses: []v1.EndpointAddress{{IP: "1.2.3.4", NodeName: &emptyNodeName, TargetRef: &v1.ObjectReference{Kind: "Pod", Name: "pod0", Namespace: ns}}},
			Ports:     []v1.EndpointPort{{Port: 8080, Protocol: "TCP"}},
		}},
	})
	endpointsHandler.ValidateRequest(t, "/api/v1/namespaces/"+ns+"/endpoints/foo", "PUT", &data)
}

// TestPodUpdatesBatching 验证由 Pod 更新引发的 Endpoints 更新会被批处理（在 batchPeriod 内合并）。
// 本用例依赖真实的 time.Sleep，因为当前控制器没有方便的时钟注入点。
// TODO(mborsz): 待条件允许时迁移到模拟时钟。
func TestPodUpdatesBatching(t *testing.T) {
	// podUpdate 描述一次更新：delay 为相对上次事件的等待时间，podName 与 podIP 指定目标 Pod
	// 与新 IP。
	type podUpdate struct {
		delay   time.Duration
		podName string
		podIP   string
	}

	// name 为用例名；batchPeriod 为批处理窗口；podsCount 为预置 Pod 数；updates 为更新序列；
	// finalDelay 为最后一次更新后的观察窗口；wantRequestCount 为期望的写请求次数。
	tests := []struct {
		name             string
		batchPeriod      time.Duration
		podsCount        int
		updates          []podUpdate
		finalDelay       time.Duration
		wantRequestCount int
	}{
		{
			// 未开启批处理：三次更新应产生三次写请求。
			name:        "three updates with no batching",
			batchPeriod: 0 * time.Second,
			podsCount:   10,
			updates: []podUpdate{
				{
					// endpoints.Run 启动后约需 100ms 才开始处理更新，首个事件多等待一段时间。
					delay:   200 * time.Millisecond,
					podName: "pod0",
					podIP:   "10.0.0.0",
				},
				{
					delay:   100 * time.Millisecond,
					podName: "pod1",
					podIP:   "10.0.0.1",
				},
				{
					delay:   100 * time.Millisecond,
					podName: "pod2",
					podIP:   "10.0.0.2",
				},
			},
			finalDelay:       3 * time.Second,
			wantRequestCount: 3,
		},
		{
			// 1 秒批处理窗口内完成三次更新：应合并为一次写请求。
			name:        "three updates in one batch",
			batchPeriod: 1 * time.Second,
			podsCount:   10,
			updates: []podUpdate{
				{
					// endpoints.Run 启动后约需 100ms 才开始处理更新，首个事件多等待一段时间。
					delay:   200 * time.Millisecond,
					podName: "pod0",
					podIP:   "10.0.0.0",
				},
				{
					delay:   100 * time.Millisecond,
					podName: "pod1",
					podIP:   "10.0.0.1",
				},
				{
					delay:   100 * time.Millisecond,
					podName: "pod2",
					podIP:   "10.0.0.2",
				},
			},
			finalDelay:       3 * time.Second,
			wantRequestCount: 1,
		},
		{
			// 前两次更新落在同一窗口，第三次间隔 1 秒：预期两次写请求。
			name:        "three updates in two batches",
			batchPeriod: 1 * time.Second,
			podsCount:   10,
			updates: []podUpdate{
				{
					// endpoints.Run 启动后约需 100ms 才开始处理更新，首个事件多等待一段时间。
					delay:   200 * time.Millisecond,
					podName: "pod0",
					podIP:   "10.0.0.0",
				},
				{
					delay:   100 * time.Millisecond,
					podName: "pod1",
					podIP:   "10.0.0.1",
				},
				{
					delay:   1 * time.Second,
					podName: "pod2",
					podIP:   "10.0.0.2",
				},
			},
			finalDelay:       3 * time.Second,
			wantRequestCount: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ns := "other"
			resourceVersion := 1
			testServer, endpointsHandler := makeTestServer(t, ns)
			defer testServer.Close()

			tCtx := ktesting.Init(t)
			endpoints := newController(tCtx, testServer.URL, tc.batchPeriod)
			endpoints.podsSynced = alwaysReady
			endpoints.servicesSynced = alwaysReady
			endpoints.endpointsSynced = alwaysReady
			endpoints.workerLoopPeriod = 10 * time.Millisecond

			go endpoints.Run(tCtx, 1)

			addPods(endpoints.podStore, ns, tc.podsCount, 1, 0, ipv4only)

			endpoints.serviceStore.Add(&v1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns},
				Spec: v1.ServiceSpec{
					Selector:   map[string]string{"foo": "bar"},
					Ports:      []v1.ServicePort{{Port: 80}},
					IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
				},
			})

			for _, update := range tc.updates {
				time.Sleep(update.delay)

				old, exists, err := endpoints.podStore.GetByKey(fmt.Sprintf("%s/%s", ns, update.podName))
				if err != nil {
					t.Fatalf("Error while retrieving old value of %q: %v", update.podName, err)
				}
				if !exists {
					t.Fatalf("Pod %q doesn't exist", update.podName)
				}
				oldPod := old.(*v1.Pod)
				newPod := oldPod.DeepCopy()
				newPod.Status.PodIP = update.podIP
				newPod.Status.PodIPs[0].IP = update.podIP
				newPod.ResourceVersion = strconv.Itoa(resourceVersion)
				resourceVersion++

				endpoints.podStore.Update(newPod)
				endpoints.onPodUpdate(oldPod, newPod)
			}

			time.Sleep(tc.finalDelay)
			endpointsHandler.ValidateRequestCount(t, tc.wantRequestCount)
		})
	}
}

// TestPodAddsBatching 验证由 Pod 新增引发的 Endpoints 更新会被批处理。
// 同样依赖真实的 time.Sleep，当前控制器无法 mock 时钟。
// TODO(mborsz): 待条件允许时迁移到模拟时钟。
func TestPodAddsBatching(t *testing.T) {
	// podAdd 只携带相对等待时间 delay，Pod 由循环下标生成。
	type podAdd struct {
		delay time.Duration
	}

	// 字段含义与 PodUpdatesBatching 相同：batchPeriod 决定合并窗口，adds 为新增序列，
	// finalDelay 为观察窗口，wantRequestCount 为期望写请求次数。
	tests := []struct {
		name             string
		batchPeriod      time.Duration
		adds             []podAdd
		finalDelay       time.Duration
		wantRequestCount int
	}{
		// 未开启批处理：三次新增应产生三次写请求。
		{
			name:        "three adds with no batching",
			batchPeriod: 0 * time.Second,
			adds: []podAdd{
				{
					// endpoints.Run 启动后约需 100ms 才开始处理更新，首个事件多等待一段时间。
					delay: 200 * time.Millisecond,
				},
				{
					delay: 100 * time.Millisecond,
				},
				{
					delay: 100 * time.Millisecond,
				},
			},
			finalDelay:       3 * time.Second,
			wantRequestCount: 3,
		},
		{
			// 1 秒窗口内的三次新增合并为一次写请求。
			name:        "three adds in one batch",
			batchPeriod: 1 * time.Second,
			adds: []podAdd{
				{
					// endpoints.Run 启动后约需 100ms 才开始处理更新，首个事件多等待一段时间。
					delay: 200 * time.Millisecond,
				},
				{
					delay: 100 * time.Millisecond,
				},
				{
					delay: 100 * time.Millisecond,
				},
			},
			finalDelay:       3 * time.Second,
			wantRequestCount: 1,
		},
		{
			// 前两次新增在同一窗口，第三次间隔 1 秒：预期两次写请求。
			name:        "three adds in two batches",
			batchPeriod: 1 * time.Second,
			adds: []podAdd{
				{
					// endpoints.Run 启动后约需 100ms 才开始处理更新，首个事件多等待一段时间。
					delay: 200 * time.Millisecond,
				},
				{
					delay: 100 * time.Millisecond,
				},
				{
					delay: 1 * time.Second,
				},
			},
			finalDelay:       3 * time.Second,
			wantRequestCount: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ns := "other"
			testServer, endpointsHandler := makeTestServer(t, ns)
			defer testServer.Close()

			tCtx := ktesting.Init(t)
			endpoints := newController(tCtx, testServer.URL, tc.batchPeriod)
			endpoints.podsSynced = alwaysReady
			endpoints.servicesSynced = alwaysReady
			endpoints.endpointsSynced = alwaysReady
			endpoints.workerLoopPeriod = 10 * time.Millisecond

			go endpoints.Run(tCtx, 1)

			endpoints.serviceStore.Add(&v1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns},
				Spec: v1.ServiceSpec{
					Selector:   map[string]string{"foo": "bar"},
					Ports:      []v1.ServicePort{{Port: 80}},
					IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
				},
			})

			for i, add := range tc.adds {
				time.Sleep(add.delay)

				p := testPod(ns, i, 1, true, ipv4only)
				endpoints.podStore.Add(p)
				endpoints.onPodUpdate(nil, p)
			}

			time.Sleep(tc.finalDelay)
			endpointsHandler.ValidateRequestCount(t, tc.wantRequestCount)
		})
	}
}

// TestPodDeleteBatching 验证由 Pod 删除引发的 Endpoints 更新会被批处理。
// 依赖真实 time.Sleep，当前控制器无法 mock 时钟。
// TODO(mborsz): 待条件允许时迁移到模拟时钟。
func TestPodDeleteBatching(t *testing.T) {
	// podDelete 指定相对等待时间与要删除的 Pod 名称。
	type podDelete struct {
		delay   time.Duration
		podName string
	}

	// podsCount 为预置 Pod 数；deletes 为删除序列；其余字段含义与前两个批处理用例一致。
	tests := []struct {
		name             string
		batchPeriod      time.Duration
		podsCount        int
		deletes          []podDelete
		finalDelay       time.Duration
		wantRequestCount int
	}{
		// 未开启批处理：三次删除应产生三次写请求。
		{
			name:        "three deletes with no batching",
			batchPeriod: 0 * time.Second,
			podsCount:   10,
			deletes: []podDelete{
				{
					// endpoints.Run 启动后约需 100ms 才开始处理更新，首个事件多等待一段时间。
					delay:   200 * time.Millisecond,
					podName: "pod0",
				},
				{
					delay:   100 * time.Millisecond,
					podName: "pod1",
				},
				{
					delay:   100 * time.Millisecond,
					podName: "pod2",
				},
			},
			finalDelay:       3 * time.Second,
			wantRequestCount: 3,
		},
		{
			// 1 秒窗口内的三次删除合并为一次写请求。
			name:        "three deletes in one batch",
			batchPeriod: 1 * time.Second,
			podsCount:   10,
			deletes: []podDelete{
				{
					// endpoints.Run 启动后约需 100ms 才开始处理更新，首个事件多等待一段时间。
					delay:   200 * time.Millisecond,
					podName: "pod0",
				},
				{
					delay:   100 * time.Millisecond,
					podName: "pod1",
				},
				{
					delay:   100 * time.Millisecond,
					podName: "pod2",
				},
			},
			finalDelay:       3 * time.Second,
			wantRequestCount: 1,
		},
		{
			// 前两次删除在同一窗口，第三次间隔 1 秒：预期两次写请求。
			name:        "three deletes in two batches",
			batchPeriod: 1 * time.Second,
			podsCount:   10,
			deletes: []podDelete{
				{
					// endpoints.Run 启动后约需 100ms 才开始处理更新，首个事件多等待一段时间。
					delay:   200 * time.Millisecond,
					podName: "pod0",
				},
				{
					delay:   100 * time.Millisecond,
					podName: "pod1",
				},
				{
					delay:   1 * time.Second,
					podName: "pod2",
				},
			},
			finalDelay:       3 * time.Second,
			wantRequestCount: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ns := "other"
			testServer, endpointsHandler := makeTestServer(t, ns)
			defer testServer.Close()

			tCtx := ktesting.Init(t)
			endpoints := newController(tCtx, testServer.URL, tc.batchPeriod)
			endpoints.podsSynced = alwaysReady
			endpoints.servicesSynced = alwaysReady
			endpoints.endpointsSynced = alwaysReady
			endpoints.workerLoopPeriod = 10 * time.Millisecond

			go endpoints.Run(tCtx, 1)

			addPods(endpoints.podStore, ns, tc.podsCount, 1, 0, ipv4only)

			endpoints.serviceStore.Add(&v1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns},
				Spec: v1.ServiceSpec{
					Selector:   map[string]string{"foo": "bar"},
					Ports:      []v1.ServicePort{{Port: 80}},
					IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
				},
			})

			for _, update := range tc.deletes {
				time.Sleep(update.delay)

				old, exists, err := endpoints.podStore.GetByKey(fmt.Sprintf("%s/%s", ns, update.podName))
				if err != nil {
					t.Fatalf("Error while retrieving old value of %q: %v", update.podName, err)
				}
				if !exists {
					t.Fatalf("Pod %q doesn't exist", update.podName)
				}
				endpoints.podStore.Delete(old)
				endpoints.onPodUpdate(old, nil)
			}

			time.Sleep(tc.finalDelay)
			endpointsHandler.ValidateRequestCount(t, tc.wantRequestCount)
		})
	}
}

// TestSyncEndpointsServiceNotFound 验证 Service 已从缓存中消失但 Endpoints 仍存在时，
// 控制器会发出 DELETE 请求清理孤儿 Endpoints。
func TestSyncEndpointsServiceNotFound(t *testing.T) {
	ns := metav1.NamespaceDefault
	testServer, endpointsHandler := makeTestServer(t, ns)
	defer testServer.Close()

	tCtx := ktesting.Init(t)
	endpoints := newController(tCtx, testServer.URL, 0)
	endpoints.endpointsStore.Add(&v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "1",
		},
	})
	err := endpoints.syncService(tCtx, ns+"/foo")
	if err != nil {
		t.Errorf("Unexpected error syncing service %v", err)
	}
	endpointsHandler.ValidateRequestCount(t, 1)
	endpointsHandler.ValidateRequest(t, "/api/v1/namespaces/"+ns+"/endpoints/foo", "DELETE", nil)
}

// TestSyncServiceOverCapacity 验证 Endpoints 超过 maxCapacity 时的截断行为，以及
// EndpointsOverCapacity 注解的添加、修正与移除时机。
func TestSyncServiceOverCapacity(t *testing.T) {
	testCases := []struct {
		// startingAnnotation 为预置注解值（nil 表示无注解）；numExisting 为已有地址数；
		// numDesired/numDesiredNotReady 为期望的就绪/未就绪 Pod 数；
		// numExpectedReady/numExpectedNotReady 为截断后期望的地址数量；expectedAnnotation 表示
		// 是否预期存在值为 "truncated" 的注解。
		name                string
		startingAnnotation  *string
		numExisting         int
		numDesired          int
		numDesiredNotReady  int
		numExpectedReady    int
		numExpectedNotReady int
		expectedAnnotation  bool
	}{{
		name: "empty",
		// 空场景：无需截断，也不应有注解。
		startingAnnotation:  nil,
		numExisting:         0,
		numDesired:          0,
		numExpectedReady:    0,
		numExpectedNotReady: 0,
		expectedAnnotation:  false,
	}, {
		// 总数超过容量：添加注解，并按比例截断未就绪地址。
		name:                "annotation added past capacity, < than maxCapacity of Ready Addresses",
		startingAnnotation:  nil,
		numExisting:         maxCapacity - 1,
		numDesired:          maxCapacity - 3,
		numDesiredNotReady:  4,
		numExpectedReady:    maxCapacity - 3,
		numExpectedNotReady: 3,
		expectedAnnotation:  true,
	}, {
		// 就绪地址刚好占满容量：未就绪地址被全部截断，仍需注解。
		name:                "annotation added past capacity, maxCapacity of Ready Addresses ",
		startingAnnotation:  nil,
		numExisting:         maxCapacity - 1,
		numDesired:          maxCapacity,
		numDesiredNotReady:  10,
		numExpectedReady:    maxCapacity,
		numExpectedNotReady: 0,
		expectedAnnotation:  true,
	}, {
		// 从超限回落到容量以下：注解应被移除。
		name:                "annotation removed below capacity",
		startingAnnotation:  ptr.To("truncated"),
		numExisting:         maxCapacity - 1,
		numDesired:          maxCapacity - 1,
		numDesiredNotReady:  0,
		numExpectedReady:    maxCapacity - 1,
		numExpectedNotReady: 0,
		expectedAnnotation:  false,
	}, {
		// 恰好处于容量边界且旧注解为 warning：注解应被移除。
		name:                "annotation was set to warning previously, annotation removed at capacity",
		startingAnnotation:  ptr.To("warning"),
		numExisting:         maxCapacity,
		numDesired:          maxCapacity,
		numDesiredNotReady:  0,
		numExpectedReady:    maxCapacity,
		numExpectedNotReady: 0,
		expectedAnnotation:  false,
	}, {
		// 仍处于超限状态但旧注解为 warning：应修正为 truncated。
		name:                "annotation was set to warning previously but still over capacity",
		startingAnnotation:  ptr.To("warning"),
		numExisting:         maxCapacity + 1,
		numDesired:          maxCapacity + 1,
		numDesiredNotReady:  0,
		numExpectedReady:    maxCapacity,
		numExpectedNotReady: 0,
		expectedAnnotation:  true,
	}, {
		// 容量边界且旧注解为 truncated：注解应被移除。
		name:                "annotation removed at capacity",
		startingAnnotation:  ptr.To("truncated"),
		numExisting:         maxCapacity,
		numDesired:          maxCapacity,
		numDesiredNotReady:  0,
		numExpectedReady:    maxCapacity,
		numExpectedNotReady: 0,
		expectedAnnotation:  false,
	}, {
		// 端点数量未变但注解值非法：应修正为 truncated。
		name:                "no endpoints change, annotation value corrected",
		startingAnnotation:  ptr.To("invalid"),
		numExisting:         maxCapacity + 1,
		numDesired:          maxCapacity + 1,
		numDesiredNotReady:  0,
		numExpectedReady:    maxCapacity,
		numExpectedNotReady: 0,
		expectedAnnotation:  true,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			ns := "test"
			client, c := newFakeController(tCtx, 0*time.Second)

			addPods(c.podStore, ns, tc.numDesired, 1, tc.numDesiredNotReady, ipv4only)
			pods := c.podStore.List()

			svc := &v1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns},
				Spec: v1.ServiceSpec{
					Selector:   map[string]string{"foo": "bar"},
					Ports:      []v1.ServicePort{{Port: 80}},
					IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
				},
			}
			c.serviceStore.Add(svc)

			subset := v1.EndpointSubset{}
			for i := 0; i < tc.numExisting; i++ {
				pod := pods[i].(*v1.Pod)
				epa, _ := podToEndpointAddressForService(svc, pod)
				subset.Addresses = append(subset.Addresses, *epa)
			}
			endpoints := &v1.Endpoints{
				ObjectMeta: metav1.ObjectMeta{
					Name:            svc.Name,
					Namespace:       ns,
					ResourceVersion: "1",
					Annotations:     map[string]string{},
				},
				Subsets: []v1.EndpointSubset{subset},
			}
			if tc.startingAnnotation != nil {
				endpoints.Annotations[v1.EndpointsOverCapacity] = *tc.startingAnnotation
			}
			c.endpointsStore.Add(endpoints)
			_, err := client.CoreV1().Endpoints(ns).Create(tCtx, endpoints, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("unexpected error creating endpoints: %v", err)
			}

			err = c.syncService(tCtx, fmt.Sprintf("%s/%s", ns, svc.Name))
			if err != nil {
				t.Errorf("Unexpected error syncing service %v", err)
			}

			actualEndpoints, err := client.CoreV1().Endpoints(ns).Get(tCtx, endpoints.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("unexpected error getting endpoints: %v", err)
			}

			// 分别校验注解的有无与取值，以及截断后就绪/未就绪地址的数量。
			actualAnnotation, ok := actualEndpoints.Annotations[v1.EndpointsOverCapacity]
			if tc.expectedAnnotation {
				if !ok {
					t.Errorf("Expected EndpointsOverCapacity annotation to be set")
				} else if actualAnnotation != "truncated" {
					t.Errorf("Expected EndpointsOverCapacity annotation to be 'truncated', got %s", actualAnnotation)
				}
			} else {
				if ok {
					t.Errorf("Expected EndpointsOverCapacity annotation not to be set, got %s", actualAnnotation)
				}
			}
			numActualReady := 0
			numActualNotReady := 0
			for _, subset := range actualEndpoints.Subsets {
				numActualReady += len(subset.Addresses)
				numActualNotReady += len(subset.NotReadyAddresses)
			}
			if numActualReady != tc.numExpectedReady {
				t.Errorf("Unexpected number of actual ready Endpoints: got %d endpoints, want %d endpoints", numActualReady, tc.numExpectedReady)
			}
			if numActualNotReady != tc.numExpectedNotReady {
				t.Errorf("Unexpected number of actual not ready Endpoints: got %d endpoints, want %d endpoints", numActualNotReady, tc.numExpectedNotReady)
			}
		})
	}
}

// TestTruncateEndpoints 验证 truncateEndpoints 会在多个子集间按比例分配 maxCapacity，
// 并优先保留就绪地址、按需要缩减未就绪地址。
func TestTruncateEndpoints(t *testing.T) {
	testCases := []struct {
		desc string
		// subsetsReady、subsetsNotReady、expectedReady、expectedNotReady
		// 四个切片长度必须一致，每个元素对应一个子集。
		subsetsReady     []int
		subsetsNotReady  []int
		expectedReady    []int
		expectedNotReady []int
	}{{
		// 空输入。
		desc:             "empty",
		subsetsReady:     []int{},
		subsetsNotReady:  []int{},
		expectedReady:    []int{},
		expectedNotReady: []int{},
		// 总数低于容量：不截断。
	}, {
		desc:             "total endpoints < max capacity",
		subsetsReady:     []int{50, 100, 100, 100, 100},
		subsetsNotReady:  []int{50, 100, 100, 100, 100},
		expectedReady:    []int{50, 100, 100, 100, 100},
		expectedNotReady: []int{50, 100, 100, 100, 100},
		// 总数恰好等于容量：保持原样。
	}, {
		desc:             "total endpoints = max capacity",
		subsetsReady:     []int{100, 100, 100, 100, 100},
		subsetsNotReady:  []int{100, 100, 100, 100, 100},
		expectedReady:    []int{100, 100, 100, 100, 100},
		expectedNotReady: []int{100, 100, 100, 100, 100},
		// 就绪地址未超容量但总数超出：只缩减未就绪地址。
	}, {
		desc:             "total ready endpoints < max capacity, but total endpoints > max capacity",
		subsetsReady:     []int{90, 110, 50, 10, 20},
		subsetsNotReady:  []int{101, 200, 200, 201, 298},
		expectedReady:    []int{90, 110, 50, 10, 20},
		expectedNotReady: []int{73, 144, 144, 145, 214},
		// 就绪地址已超容量：未就绪地址清零，就绪地址按比例缩减。
	}, {
		desc:             "total ready endpoints > max capacity",
		subsetsReady:     []int{205, 400, 402, 400, 693},
		subsetsNotReady:  []int{100, 200, 200, 200, 300},
		expectedReady:    []int{98, 191, 192, 191, 328},
		expectedNotReady: []int{0, 0, 0, 0, 0},
	}}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			var subsets []v1.EndpointSubset
			for subsetIndex, numReady := range tc.subsetsReady {
				subset := v1.EndpointSubset{}
				for i := 0; i < numReady; i++ {
					subset.Addresses = append(subset.Addresses, v1.EndpointAddress{})
				}

				numNotReady := tc.subsetsNotReady[subsetIndex]
				for i := 0; i < numNotReady; i++ {
					subset.NotReadyAddresses = append(subset.NotReadyAddresses, v1.EndpointAddress{})
				}
				subsets = append(subsets, subset)
			}

			endpoints := &v1.Endpoints{Subsets: subsets}
			truncateEndpoints(endpoints)

			for i, subset := range endpoints.Subsets {
				if len(subset.Addresses) != tc.expectedReady[i] {
					t.Errorf("Unexpected number of actual ready Endpoints for subset %d: got %d endpoints, want %d endpoints", i, len(subset.Addresses), tc.expectedReady[i])
				}
				if len(subset.NotReadyAddresses) != tc.expectedNotReady[i] {
					t.Errorf("Unexpected number of actual not ready Endpoints for subset %d: got %d endpoints, want %d endpoints", i, len(subset.NotReadyAddresses), tc.expectedNotReady[i])
				}
			}
		})
	}
}

// TestEndpointPortFromServicePort 验证 ServicePort 的 AppProtocol 会原样传播到
// EndpointPort；nil 与具体值两种情况均覆盖，stringVal 仅用于格式化失败信息。
func TestEndpointPortFromServicePort(t *testing.T) {
	http := ptr.To("http")
	testCases := map[string]struct {
		serviceAppProtocol           *string
		expectedEndpointsAppProtocol *string
	}{
		"empty app protocol": {
			serviceAppProtocol:           nil,
			expectedEndpointsAppProtocol: nil,
		},
		"http app protocol": {
			serviceAppProtocol:           http,
			expectedEndpointsAppProtocol: http,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			epp := endpointPortFromServicePort(&v1.ServicePort{Name: "test", AppProtocol: tc.serviceAppProtocol}, 80)

			if epp.AppProtocol != tc.expectedEndpointsAppProtocol {
				t.Errorf("Expected Endpoints AppProtocol to be %s, got %s", stringVal(tc.expectedEndpointsAppProtocol), stringVal(epp.AppProtocol))
			}
		})
	}
}

// TestMultipleServiceChanges 验证因 Endpoints 缓存不同步而未能创建的 Endpoints 最终会被重建。
// 流程：创建 Service 并等待 Endpoints 出现 → 删除 Service，但人为阻塞缓存中的 Endpoints 删除 →
// 重新创建 Service（此时不会立刻发起 Create） → 放行缓存删除 → 期望 Endpoints 被重新创建。
func TestMultipleServiceChanges(t *testing.T) {
	ns := metav1.NamespaceDefault
	expectedSubsets := []v1.EndpointSubset{{
		Addresses: []v1.EndpointAddress{
			{IP: "1.2.3.4", NodeName: &emptyNodeName, TargetRef: &v1.ObjectReference{Kind: "Pod", Name: "pod0", Namespace: ns}},
		},
	}}
	endpoint := &v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns, ResourceVersion: "1"},
		Subsets:    expectedSubsets,
	}

	// 先用空壳控制器启动服务器：服务器回调需要引用控制器指针，随后再用 newController
	// 的结果填充该指针。
	controller := &endpointController{}
	// blockDelete/blockNextAction 用于控制缓存更新时机与等待请求到达。
	blockDelete := make(chan struct{})
	blockNextAction := make(chan struct{})
	stopChan := make(chan struct{})
	testServer := makeBlockingEndpointTestServer(t, controller, endpoint, nil, blockDelete, blockNextAction, ns)
	defer testServer.Close()

	tCtx := ktesting.Init(t)
	*controller = *newController(tCtx, testServer.URL, 0*time.Second)
	addPods(controller.podStore, ns, 1, 1, 0, ipv4only)

	go func() { controller.Run(tCtx, 1) }()

	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns},
		Spec: v1.ServiceSpec{
			Selector:   map[string]string{"foo": "bar"},
			ClusterIP:  "None",
			Ports:      nil,
			IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
		},
	}

	controller.serviceStore.Add(svc)
	controller.onServiceUpdate(svc)
	// 服务器收到 Endpoints 请求后，blockNextAction 应当被解除阻塞。
	waitForChanReceive(t, 1*time.Second, blockNextAction, "Service Add should have caused a request to be sent to the test server")

	controller.serviceStore.Delete(svc)
	controller.onServiceDelete(svc)
	waitForChanReceive(t, 1*time.Second, blockNextAction, "Service Delete should have caused a request to be sent to the test server")

	// 如果 Service 更新被处理时 Endpoints 缓存尚未同步，
	// Service Add 不会立即触发 Create Endpoints 请求。
	controller.serviceStore.Add(svc)
	controller.onServiceUpdate(svc)

	// 为避免抖动，轮询等待队列清空，最长等待 1 秒。
	wait.PollImmediate(50*time.Millisecond, 1*time.Second, func() (bool, error) {
		return controller.queue.Len() == 0, nil
	})

	// 放行测试服务器执行删除操作。
	close(blockDelete)
	waitForChanReceive(t, 1*time.Second, blockNextAction, "Endpoint should have been recreated")

	close(blockNextAction)
	close(stopChan)
}

// TestMultiplePodChanges 验证因 Endpoints 缓存不同步而未能更新的 Endpoints，
// 在多次 Pod 变更后最终会被重新同步。
func TestMultiplePodChanges(t *testing.T) {
	ns := metav1.NamespaceDefault

	readyEndpoints := &v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns, ResourceVersion: "1"},
		Subsets: []v1.EndpointSubset{{
			Addresses: []v1.EndpointAddress{
				{IP: "1.2.3.4", NodeName: &emptyNodeName, TargetRef: &v1.ObjectReference{Kind: "Pod", Name: "pod0", Namespace: ns}},
			},
			Ports: []v1.EndpointPort{{Port: 8080, Protocol: v1.ProtocolTCP}},
		}},
	}
	notReadyEndpoints := &v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns, ResourceVersion: "2"},
		Subsets: []v1.EndpointSubset{{
			NotReadyAddresses: []v1.EndpointAddress{
				{IP: "1.2.3.4", NodeName: &emptyNodeName, TargetRef: &v1.ObjectReference{Kind: "Pod", Name: "pod0", Namespace: ns}},
			},
			Ports: []v1.EndpointPort{{Port: 8080, Protocol: v1.ProtocolTCP}},
		}},
	}

	// 与上一个用例相同，先创建空壳控制器供阻塞服务器回调使用。
	controller := &endpointController{}
	blockUpdate := make(chan struct{})
	blockNextAction := make(chan struct{})
	stopChan := make(chan struct{})
	testServer := makeBlockingEndpointTestServer(t, controller, notReadyEndpoints, blockUpdate, nil, blockNextAction, ns)
	defer testServer.Close()

	tCtx := ktesting.Init(t)
	*controller = *newController(tCtx, testServer.URL, 0*time.Second)
	pod := testPod(ns, 0, 1, true, ipv4only)
	_ = controller.podStore.Add(pod)
	_ = controller.endpointsStore.Add(readyEndpoints)
	_ = controller.serviceStore.Add(&v1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: ns},
		Spec: v1.ServiceSpec{
			Selector:   map[string]string{"foo": "bar"},
			ClusterIP:  "10.0.0.1",
			Ports:      []v1.ServicePort{{Port: 80, Protocol: "TCP", TargetPort: intstr.FromInt32(8080)}},
			IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
		},
	})

	go func() { controller.Run(tCtx, 1) }()

	// 快速连续更新 Pod：Ready → NotReady → Ready。
	pod2 := pod.DeepCopy()
	pod2.ResourceVersion = "2"
	pod2.Status.Conditions[0].Status = v1.ConditionFalse
	_ = controller.podStore.Update(pod2)
	controller.onPodUpdate(pod, pod2)
	// 测试服务器收到 Endpoints 请求后，blockNextAction 最终应解除阻塞。
	waitForChanReceive(t, 1*time.Second, blockNextAction, "Pod Update should have caused a request to be sent to the test server")
	// 此时缓存中的 Endpoints 更新尚未生效。
	pod3 := pod.DeepCopy()
	pod3.ResourceVersion = "3"
	pod3.Status.Conditions[0].Status = v1.ConditionTrue
	_ = controller.podStore.Update(pod3)
	controller.onPodUpdate(pod2, pod3)
	// 缓存中的 Endpoints 仍是旧值，因此这次 Pod 更新不应触发写请求。
	timer := time.NewTimer(100 * time.Millisecond)
	select {
	case <-timer.C:
	case <-blockNextAction:
		t.Errorf("Pod Update shouldn't have caused a request to be sent to the test server")
	}

	// 放行缓存更新后，控制器应重新发起 Endpoints 更新请求。
	close(blockUpdate)
	waitForChanReceive(t, 1*time.Second, blockNextAction, "Endpoints should have been updated")

	close(blockNextAction)
	close(stopChan)
}

// TestSyncServiceAddresses 以表驱动方式验证哪些 Pod 会被计入 Endpoints 地址：
// 覆盖 Pod 各相位（Running/Unknown/Pending/Failed/Succeeded）、就绪条件、是否处于删除中，
// 以及 Service 是否通过 PublishNotReadyAddresses 容忍未就绪地址。
func TestSyncServiceAddresses(t *testing.T) {
	// makeService 构造测试 Service；tolerateUnready 控制是否设置 PublishNotReadyAddresses。
	makeService := func(tolerateUnready bool) *v1.Service {
		return &v1.Service{
			ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "ns"},
			Spec: v1.ServiceSpec{
				Selector:                 map[string]string{"foo": "bar"},
				PublishNotReadyAddresses: tolerateUnready,
				Type:                     v1.ServiceTypeClusterIP,
				ClusterIP:                "1.1.1.1",
				Ports:                    []v1.ServicePort{{Port: 80}},
				IPFamilies:               []v1.IPFamily{v1.IPv4Protocol},
			},
		}
	}

	// makePod 按 Pod 相位、就绪条件与是否处于删除中构造 Pod。
	makePod := func(phase v1.PodPhase, isReady bool, terminating bool) *v1.Pod {
		statusCondition := v1.ConditionFalse
		if isReady {
			statusCondition = v1.ConditionTrue
		}

		now := metav1.Now()
		deletionTimestamp := &now
		if !terminating {
			deletionTimestamp = nil
		}
		return &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace:         "ns",
				Name:              "fakepod",
				DeletionTimestamp: deletionTimestamp,
				Labels:            map[string]string{"foo": "bar"},
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{{Ports: []v1.ContainerPort{
					{Name: "port1", ContainerPort: int32(8080)},
				}}},
			},
			Status: v1.PodStatus{
				Phase: phase,
				Conditions: []v1.PodCondition{
					{
						Type:   v1.PodReady,
						Status: statusCondition,
					},
				},
				PodIP: "10.1.1.1",
				PodIPs: []v1.PodIP{
					{IP: "10.1.1.1"},
				},
			},
		}
	}

	// name 为用例名；pod/service 为输入对象；expectedReady 与 expectedUnready 分别为
	// 期望的就绪与未就绪地址数量。
	testCases := []struct {
		name            string
		pod             *v1.Pod
		service         *v1.Service
		expectedReady   int
		expectedUnready int
	}{
		// 正常分组：Running/Unknown/Pending 且就绪的 Pod 计入就绪地址。
		{
			name:            "pod running phase",
			pod:             makePod(v1.PodRunning, true, false),
			service:         makeService(false),
			expectedReady:   1,
			expectedUnready: 0,
		},
		{
			name:            "pod running phase being deleted",
			pod:             makePod(v1.PodRunning, true, true),
			service:         makeService(false),
			expectedReady:   0,
			expectedUnready: 0,
		},
		{
			name:            "pod unknown phase container ready",
			pod:             makePod(v1.PodUnknown, true, false),
			service:         makeService(false),
			expectedReady:   1,
			expectedUnready: 0,
		},
		{
			name:            "pod unknown phase container ready being deleted",
			pod:             makePod(v1.PodUnknown, true, true),
			service:         makeService(false),
			expectedReady:   0,
			expectedUnready: 0,
		},
		{
			name:            "pod pending phase container ready",
			pod:             makePod(v1.PodPending, true, false),
			service:         makeService(false),
			expectedReady:   1,
			expectedUnready: 0,
		},
		{
			name:            "pod pending phase container ready being deleted",
			pod:             makePod(v1.PodPending, true, true),
			service:         makeService(false),
			expectedReady:   0,
			expectedUnready: 0,
		},
		// 未就绪且不容忍未就绪的 Pod 计入未就绪地址。
		{
			name:            "pod unknown phase container not ready",
			pod:             makePod(v1.PodUnknown, false, false),
			service:         makeService(false),
			expectedReady:   0,
			expectedUnready: 1,
		},
		{
			name:            "pod pending phase container not ready",
			pod:             makePod(v1.PodPending, false, false),
			service:         makeService(false),
			expectedReady:   0,
			expectedUnready: 1,
		},
		// Failed/Succeeded 终态 Pod 不产生任何地址。
		{
			name:            "pod failed phase",
			pod:             makePod(v1.PodFailed, false, false),
			service:         makeService(false),
			expectedReady:   0,
			expectedUnready: 0,
		},
		{
			name:            "pod succeeded phase",
			pod:             makePod(v1.PodSucceeded, false, false),
			service:         makeService(false),
			expectedReady:   0,
			expectedUnready: 0,
		},
		// PublishNotReadyAddresses=true 时，即使 Pod 未就绪也计入就绪地址。
		{
			name:            "pod running phase and tolerate unready",
			pod:             makePod(v1.PodRunning, false, false),
			service:         makeService(true),
			expectedReady:   1,
			expectedUnready: 0,
		},
		{
			name:            "pod running phase and tolerate unready being deleted",
			pod:             makePod(v1.PodRunning, false, true),
			service:         makeService(true),
			expectedReady:   1,
			expectedUnready: 0,
		},
		{
			name:            "pod unknown phase and tolerate unready",
			pod:             makePod(v1.PodUnknown, false, false),
			service:         makeService(true),
			expectedReady:   1,
			expectedUnready: 0,
		},
		{
			name:            "pod unknown phase and tolerate unready being deleted",
			pod:             makePod(v1.PodUnknown, false, true),
			service:         makeService(true),
			expectedReady:   1,
			expectedUnready: 0,
		},
		{
			name:            "pod pending phase and tolerate unready",
			pod:             makePod(v1.PodPending, false, false),
			service:         makeService(true),
			expectedReady:   1,
			expectedUnready: 0,
		},
		{
			name:            "pod pending phase and tolerate unready being deleted",
			pod:             makePod(v1.PodPending, false, true),
			service:         makeService(true),
			expectedReady:   1,
			expectedUnready: 0,
		},
		// 终态 Pod 即便设置 PublishNotReadyAddresses 仍被排除。
		{
			name:            "pod failed phase and tolerate unready",
			pod:             makePod(v1.PodFailed, false, false),
			service:         makeService(true),
			expectedReady:   0,
			expectedUnready: 0,
		},
		{
			name:            "pod succeeded phase and tolerate unready endpoints",
			pod:             makePod(v1.PodSucceeded, false, false),
			service:         makeService(true),
			expectedReady:   0,
			expectedUnready: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tCtx := ktesting.Init(t)

			ns := tc.service.Namespace
			client, c := newFakeController(tCtx, 0*time.Second)

			err := c.podStore.Add(tc.pod)
			if err != nil {
				t.Errorf("Unexpected error adding pod %v", err)
			}
			err = c.serviceStore.Add(tc.service)
			if err != nil {
				t.Errorf("Unexpected error adding service %v", err)
			}
			err = c.syncService(tCtx, fmt.Sprintf("%s/%s", ns, tc.service.Name))
			if err != nil {
				t.Errorf("Unexpected error syncing service %v", err)
			}

			endpoints, err := client.CoreV1().Endpoints(ns).Get(tCtx, tc.service.Name, metav1.GetOptions{})
			if err != nil {
				t.Errorf("Unexpected error %v", err)
			}

			// 分别累加各子集的 Addresses 与 NotReadyAddresses 数量后与期望值比较。
			readyEndpoints := 0
			unreadyEndpoints := 0
			for _, subset := range endpoints.Subsets {
				readyEndpoints += len(subset.Addresses)
				unreadyEndpoints += len(subset.NotReadyAddresses)
			}

			if tc.expectedReady != readyEndpoints {
				t.Errorf("Expected %d ready endpoints, got %d", tc.expectedReady, readyEndpoints)
			}

			if tc.expectedUnready != unreadyEndpoints {
				t.Errorf("Expected %d ready endpoints, got %d", tc.expectedUnready, unreadyEndpoints)
			}
		})
	}
}

// TestEndpointsDeletionEvents 验证 Endpoints 删除事件（无论是否预期）都会把所属 Service
// 入队，以便重新核对是否需要重建 Endpoints。
func TestEndpointsDeletionEvents(t *testing.T) {
	ns := metav1.NamespaceDefault
	testServer, _ := makeTestServer(t, ns)
	defer testServer.Close()

	tCtx := ktesting.Init(t)
	controller := newController(tCtx, testServer.URL, 0)
	store := controller.endpointsStore
	ep1 := &v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "foo",
			Namespace:       ns,
			ResourceVersion: "rv1",
		},
	}

	// 同时覆盖“意外删除”与“预期删除”：从 store 删除后立即触发 onEndpointsDelete。
	store.Delete(ep1)
	controller.onEndpointsDelete(ep1)

	if controller.queue.Len() != 1 {
		t.Errorf("Expected one service to be in the queue, found %d", controller.queue.Len())
	}
}

// stringVal 把可能为 nil 的字符串指针转成可读文本，仅用于断言失败信息。
func stringVal(str *string) string {
	if str == nil {
		return "nil"
	}
	return *str
}

// waitForChanReceive 最多等待 timeout，校验 receivingChan 是否收到信号；
// 超时则用 errorMsg 报错，避免测试永久阻塞。
func waitForChanReceive(t *testing.T, timeout time.Duration, receivingChan chan struct{}, errorMsg string) {
	timer := time.NewTimer(timeout)
	select {
	case <-timer.C:
		t.Error(errorMsg)
	case <-receivingChan:
	}
}

// TestEndpointSubsetsEqualIgnoreResourceVersion 验证 endpointSubsetsEqualIgnoreResourceVersion
// 在比较端点子集时会忽略 TargetRef.ResourceVersion 的变化（包括被清空），
// 但 IP 与端口等实质字段变化仍判为不相等，从而避免仅 RV 变动引发无谓更新。
func TestEndpointSubsetsEqualIgnoreResourceVersion(t *testing.T) {
	// copyAndMutateEndpointSubset 深拷贝子集后应用 mutator，构造“仅某个字段不同”的对照对象。
	copyAndMutateEndpointSubset := func(orig *v1.EndpointSubset, mutator func(*v1.EndpointSubset)) *v1.EndpointSubset {
		newSubSet := orig.DeepCopy()
		mutator(newSubSet)
		return newSubSet
	}
	es1 := &v1.EndpointSubset{
		Addresses: []v1.EndpointAddress{
			{
				IP:        "1.1.1.1",
				TargetRef: &v1.ObjectReference{Kind: "Pod", Name: "pod1-1", Namespace: "ns", ResourceVersion: "1"},
			},
		},
		NotReadyAddresses: []v1.EndpointAddress{
			{
				IP:        "1.1.1.2",
				TargetRef: &v1.ObjectReference{Kind: "Pod", Name: "pod1-2", Namespace: "ns2", ResourceVersion: "2"},
			},
		},
		Ports: []v1.EndpointPort{{Port: 8081, Protocol: "TCP"}},
	}
	es2 := &v1.EndpointSubset{
		Addresses: []v1.EndpointAddress{
			{
				IP:        "2.2.2.1",
				TargetRef: &v1.ObjectReference{Kind: "Pod", Name: "pod2-1", Namespace: "ns", ResourceVersion: "3"},
			},
		},
		NotReadyAddresses: []v1.EndpointAddress{
			{
				IP:        "2.2.2.2",
				TargetRef: &v1.ObjectReference{Kind: "Pod", Name: "pod2-2", Namespace: "ns2", ResourceVersion: "4"},
			},
		},
		Ports: []v1.EndpointPort{{Port: 8082, Protocol: "TCP"}},
	}
	// name 为用例名；subsets1/subsets2 为待比较的子集；expected 为期望的比较结果。
	tests := []struct {
		name     string
		subsets1 []v1.EndpointSubset
		subsets2 []v1.EndpointSubset
		expected bool
	}{
		// 子集数量不同 → 不相等。
		{
			name:     "Subsets removed",
			subsets1: []v1.EndpointSubset{*es1, *es2},
			subsets2: []v1.EndpointSubset{*es1},
			expected: false,
		},
		// 就绪地址 IP 变化 → 不相等。
		{
			name:     "Ready Pod IP changed",
			subsets1: []v1.EndpointSubset{*es1, *es2},
			subsets2: []v1.EndpointSubset{*copyAndMutateEndpointSubset(es1, func(es *v1.EndpointSubset) {
				es.Addresses[0].IP = "1.1.1.10"
			}), *es2},
			expected: false,
		},
		// 未就绪地址 IP 变化 → 不相等。
		{
			name:     "NotReady Pod IP changed",
			subsets1: []v1.EndpointSubset{*es1, *es2},
			subsets2: []v1.EndpointSubset{*es1, *copyAndMutateEndpointSubset(es2, func(es *v1.EndpointSubset) {
				es.NotReadyAddresses[0].IP = "2.2.2.10"
			})},
			expected: false,
		},
		// 仅 TargetRef.ResourceVersion 变化 → 视为相等（本测试的核心断言）。
		{
			name:     "Pod ResourceVersion changed",
			subsets1: []v1.EndpointSubset{*es1, *es2},
			subsets2: []v1.EndpointSubset{*es1, *copyAndMutateEndpointSubset(es2, func(es *v1.EndpointSubset) {
				es.Addresses[0].TargetRef.ResourceVersion = "100"
			})},
			expected: true,
		},
		// RV 从有值变为空 → 同样视为相等。
		{
			name:     "Pod ResourceVersion removed",
			subsets1: []v1.EndpointSubset{*es1, *es2},
			subsets2: []v1.EndpointSubset{*es1, *copyAndMutateEndpointSubset(es2, func(es *v1.EndpointSubset) {
				es.Addresses[0].TargetRef.ResourceVersion = ""
			})},
			expected: true,
		},
		// 端口变化 → 不相等。
		{
			name:     "Ports changed",
			subsets1: []v1.EndpointSubset{*es1, *es2},
			subsets2: []v1.EndpointSubset{*es1, *copyAndMutateEndpointSubset(es1, func(es *v1.EndpointSubset) {
				es.Ports[0].Port = 8082
			})},
			expected: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := endpointSubsetsEqualIgnoreResourceVersion(tt.subsets1, tt.subsets2); got != tt.expected {
				t.Errorf("semanticIgnoreResourceVersion.DeepEqual() = %v, expected %v", got, tt.expected)
			}
		})
	}
}

// TestSyncEndpointsAddDeletePorts 验证 Service 端口的增删会正确反映到 Endpoints：
// 依次追加端口与 Pod、再次追加端口与 Pod、最后删除首个端口，逐步比对期望的 Endpoints 对象。
//
// 对应 test/e2e/network/service.go 中的 It("should be updated after adding or deleting ports")。
func TestSyncEndpointsAddDeletePorts(t *testing.T) {
	ctx := ktesting.Init(t)
	client, c := newFakeController(ctx, 0*time.Second)

	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "ns",
		},
		Spec: v1.ServiceSpec{
			Selector:  map[string]string{"foo": "bar"},
			Type:      v1.ServiceTypeClusterIP,
			ClusterIP: "1.1.1.1",
			Ports: []v1.ServicePort{
				{Name: "portname1", Port: 80, TargetPort: intstr.FromString("svc1")},
			},
			IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
		},
	}

	// podTemplate 作为 Pod 模板，每轮复制后填入名称、IP 与容器端口。
	podTemplate := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: svc.Namespace,
			Labels:    map[string]string{"foo": "bar"},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{}},
		},
		Status: v1.PodStatus{
			Conditions: []v1.PodCondition{
				{
					Type:   v1.PodReady,
					Status: v1.ConditionTrue,
				},
			},
		},
	}

	// expectEndpoints 从 ResourceVersion 非空的占位对象开始，随每轮同步逐步构造期望值。
	expectEndpoints := &v1.Endpoints{
		ObjectMeta: metav1.ObjectMeta{
			Name:        svc.Name,
			Namespace:   svc.Namespace,
			Annotations: map[string]string{},
			Labels: map[string]string{
				LabelManagedBy: ControllerName,
			},
			ResourceVersion: "1",
		},
		Subsets: []v1.EndpointSubset{},
	}
	// 单测需要先放一个 ResourceVersion 非空的占位 Endpoints，
	// 才能让同步走更新路径而非创建路径。
	err := c.endpointsStore.Add(expectEndpoints)
	if err != nil {
		t.Fatalf("Unexpected error adding endpoints: %v", err)
	}
	_, err = client.CoreV1().Endpoints(svc.Namespace).Create(ctx, expectEndpoints, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error adding endpoints: %v", err)
	}

	err = c.serviceStore.Add(svc)
	if err != nil {
		t.Fatalf("Unexpected error adding service: %v", err)
	}

	pod1 := podTemplate.DeepCopy()
	pod1.Name = "pod1"
	pod1.Status.PodIP = "10.1.1.1"
	pod1.Status.PodIPs = []v1.PodIP{{IP: "10.1.1.1"}}
	pod1.Spec.Containers[0].Ports = []v1.ContainerPort{
		{Name: "svc1", ContainerPort: int32(100)},
	}
	err = c.podStore.Add(pod1)
	if err != nil {
		t.Fatalf("Unexpected error adding pod: %v", err)
	}

	err = c.syncService(ctx, fmt.Sprintf("%s/%s", svc.Namespace, svc.Name))
	if err != nil {
		t.Fatalf("Unexpected error syncing service: %v", err)
	}

	endpoints, err := client.CoreV1().Endpoints(svc.Namespace).Get(ctx, svc.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	expectEndpoints.Subsets = append(expectEndpoints.Subsets,
		v1.EndpointSubset{
			Addresses: []v1.EndpointAddress{{
				IP:       pod1.Status.PodIP,
				NodeName: &pod1.Spec.NodeName,
				TargetRef: &v1.ObjectReference{
					Kind:      "Pod",
					Namespace: pod1.Namespace,
					Name:      pod1.Name,
				},
			}},
			Ports: []v1.EndpointPort{{
				Name: "portname1",
				Port: 100,
			}},
		},
	)
	if diff := cmp.Diff(expectEndpoints, endpoints); diff != "" {
		t.Fatalf("incorrect endpoints after adding first pod:\n%s", diff)
	}

	// 给 Service 增加第二个端口。
	svc.Spec.Ports = append(svc.Spec.Ports, v1.ServicePort{
		Name:       "portname2",
		Port:       81,
		TargetPort: intstr.FromString("svc2"),
	})
	err = c.serviceStore.Add(svc)
	if err != nil {
		t.Fatalf("Unexpected error updating service: %v", err)
	}

	// 增加对应的第二个端点 Pod。
	pod2 := podTemplate.DeepCopy()
	pod2.Name = "pod2"
	pod2.Status.PodIP = "10.1.1.2"
	pod2.Status.PodIPs = []v1.PodIP{{IP: "10.1.1.2"}}
	pod2.Spec.Containers[0].Ports = []v1.ContainerPort{
		{Name: "svc2", ContainerPort: int32(101)},
	}
	err = c.podStore.Add(pod2)
	if err != nil {
		t.Fatalf("Unexpected error adding pod: %v", err)
	}

	err = c.syncService(ctx, fmt.Sprintf("%s/%s", svc.Namespace, svc.Name))
	if err != nil {
		t.Fatalf("Unexpected error syncing service: %v", err)
	}

	endpoints, err = client.CoreV1().Endpoints(svc.Namespace).Get(ctx, svc.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expectEndpoints.Subsets = append(expectEndpoints.Subsets,
		v1.EndpointSubset{
			Addresses: []v1.EndpointAddress{{
				IP:       pod2.Status.PodIP,
				NodeName: &pod2.Spec.NodeName,
				TargetRef: &v1.ObjectReference{
					Kind:      "Pod",
					Namespace: pod2.Namespace,
					Name:      pod2.Name,
				},
			}},
			Ports: []v1.EndpointPort{{
				Name: "portname2",
				Port: 101,
			}},
		},
	)
	// 期望子集需先按当前规则重新打包，才能与控制器输出做稳定比较。
	expectEndpoints.Subsets = endptspkg.RepackSubsets(expectEndpoints.Subsets)

	if diff := cmp.Diff(expectEndpoints, endpoints); diff != "" {
		t.Fatalf("incorrect endpoints after adding second pod:\n%s", diff)
	}

	// 从 Service 中删除第一个端口。
	svc.Spec.Ports = svc.Spec.Ports[1:]
	err = c.serviceStore.Add(svc)
	if err != nil {
		t.Fatalf("Unexpected error updating service: %v", err)
	}

	err = c.syncService(ctx, fmt.Sprintf("%s/%s", svc.Namespace, svc.Name))
	if err != nil {
		t.Fatalf("Unexpected error syncing service: %v", err)
	}

	endpoints, err = client.CoreV1().Endpoints(svc.Namespace).Get(ctx, svc.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	expectEndpoints.Subsets = expectEndpoints.Subsets[:1]
	if diff := cmp.Diff(expectEndpoints, endpoints); diff != "" {
		t.Fatalf("incorrect endpoints after deleting first port:\n%s", diff)
	}
}

// TestPodToEndpointAddressForServiceEmptyIPFamilies 验证 Service 未设置 IPFamilies 时，
// 控制器会根据 ClusterIP（headful）或 PodIP（headless）推断地址族并选取匹配的 Pod IP。
func TestPodToEndpointAddressForServiceEmptyIPFamilies(t *testing.T) {
	// name 为用例名；clusterIP 与 podIPs/podIP 模拟 Service/Pod 的地址；
	// wantErr 表示是否预期报错；wantFamily 为期望选中的 IP 族。
	testCases := []struct {
		name       string
		clusterIP  string
		podIPs     []v1.PodIP
		podIP      string
		wantErr    bool
		wantFamily v1.IPFamily
	}{
		{
			name:       "headful IPv4, IPv4 pod",
			clusterIP:  "10.0.0.1",
			podIPs:     []v1.PodIP{{IP: "10.244.0.1"}},
			wantFamily: v1.IPv4Protocol,
		},
		{
			name:       "headful IPv6, IPv6 pod",
			clusterIP:  "fd00::1",
			podIPs:     []v1.PodIP{{IP: "fd00::10"}},
			wantFamily: v1.IPv6Protocol,
		},
		{
			name:      "headful IPv4, no matching pod IP",
			clusterIP: "10.0.0.1",
			podIPs:    []v1.PodIP{{IP: "fd00::10"}},
			wantErr:   true,
		},
		{
			name:       "headless, IPv4 pod",
			clusterIP:  v1.ClusterIPNone,
			podIPs:     []v1.PodIP{{IP: "10.244.0.1"}},
			podIP:      "10.244.0.1",
			wantFamily: v1.IPv4Protocol,
		},
		{
			name:       "headless, IPv6 pod",
			clusterIP:  v1.ClusterIPNone,
			podIPs:     []v1.PodIP{{IP: "fd00::10"}},
			podIP:      "fd00::10",
			wantFamily: v1.IPv6Protocol,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			svc := &v1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "bar"},
				Spec: v1.ServiceSpec{
					// 刻意不设置 IPFamilies，触发控制器的推断逻辑。
					ClusterIP: tc.clusterIP,
				},
			}
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: "foo-pod", Namespace: "bar", UID: "uid-1"},
				Spec:       v1.PodSpec{NodeName: "node-1"},
				Status:     v1.PodStatus{PodIP: tc.podIP, PodIPs: tc.podIPs},
			}

			addr, err := podToEndpointAddressForService(svc, pod)
			// 先校验错误的有无，再校验返回地址的 IP 族。
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error but got addr=%v", addr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if addr == nil {
				t.Fatal("expected an address but got nil")
			}
			isV6 := utilnet.IsIPv6String(addr.IP)
			wantV6 := tc.wantFamily == v1.IPv6Protocol
			if isV6 != wantV6 {
				t.Errorf("got IP %q (IPv6=%v), want family %v", addr.IP, isV6, tc.wantFamily)
			}
		})
	}
}
