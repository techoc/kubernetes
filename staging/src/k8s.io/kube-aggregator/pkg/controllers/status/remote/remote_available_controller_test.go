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

package remote

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/util/proxy"
	v1listers "k8s.io/client-go/listers/core/v1"
	discoveryv1listers "k8s.io/client-go/listers/discovery/v1"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	apiregistration "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	"k8s.io/kube-aggregator/pkg/client/clientset_generated/clientset/fake"
	apiregistrationclient "k8s.io/kube-aggregator/pkg/client/clientset_generated/clientset/typed/apiregistration/v1"
	listers "k8s.io/kube-aggregator/pkg/client/listers/apiregistration/v1"
	availabilitymetrics "k8s.io/kube-aggregator/pkg/controllers/status/metrics"
	"k8s.io/utils/dump"
	"k8s.io/utils/ptr"
)

// 端口常量集中定义，是因为"Service 上的端口"和"EndpointSlice 里的端口名"
// 必须对齐：控制器正是拿这个名字去端点里找可用地址，写错名字就会被判为不可用。
const (
	testServicePort     int32 = 1234
	testServicePortName       = "testPort"
)

// 故意给 EndpointSlice 起一个和 Service 不同的名字，只靠 label 关联，
// 这样能验证控制器是按 service-name label 而不是按对象名查找端点。
func newEndpointSlice(namespace, serviceName string) *discoveryv1.EndpointSlice {
	return &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      serviceName + "-xxx",
			Labels: map[string]string{
				discoveryv1.LabelServiceName: serviceName,
			},
		},
	}
}

// 构造一个默认就绪、带地址的端点，作为"后端可用"用例的基线数据。
func newEndpointSliceWithAddress(namespace, serviceName string, port int32, portName string) *discoveryv1.EndpointSlice {
	slice := newEndpointSlice(namespace, serviceName)
	slice.Endpoints = []discoveryv1.Endpoint{{
		Addresses: []string{"val"},
	}}
	slice.Ports = []discoveryv1.EndpointPort{{
		Name: &portName,
		Port: &port,
	}}
	return slice
}

// 和上面那个构造器唯一的区别是 Ready=false：专门用来验证
// "端点存在但还没就绪"不会被当成可用后端。
func newUnreadyEndpointSliceWithAddress(namespace, serviceName string, port int32, portName string) *discoveryv1.EndpointSlice {
	slice := newEndpointSlice(namespace, serviceName)
	slice.Endpoints = []discoveryv1.Endpoint{{
		Addresses: []string{"val"},
		Conditions: discoveryv1.EndpointConditions{
			Ready: ptr.To(false),
		},
	}}
	slice.Ports = []discoveryv1.EndpointPort{{
		Name: &portName,
		Port: &port,
	}}
	return slice
}

func newService(namespace, name string, port int32, portName string) *v1.Service {
	return &v1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name},
		Spec: v1.ServiceSpec{
			Type: v1.ServiceTypeClusterIP,
			Ports: []v1.ServicePort{
				{Port: port, Name: portName},
			},
		},
	}
}

// 本地 APIService 没有任何 Service 引用：控制器对它直接标为可用，
// 不需要走后面的服务发现检查。
func newLocalAPIService(name string) *apiregistration.APIService {
	return &apiregistration.APIService{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}
}

// 从 "version.group" 格式的名字里反推出 Group 和 Version，
// 正好对应 APIService 命名约定，省得每个用例手工填两遍相同信息。
func newRemoteAPIService(name string) *apiregistration.APIService {
	return &apiregistration.APIService{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: apiregistration.APIServiceSpec{
			Group:   strings.SplitN(name, ".", 2)[0],
			Version: strings.SplitN(name, ".", 2)[1],
			Service: &apiregistration.ServiceReference{
				Namespace: "foo",
				Name:      "bar",
				Port:      ptr.To(testServicePort),
			},
		},
	}
}

// 用最小接口而不是 *testing.T，是为了让同一个 setup 既能给单元测试用，
// 也能给后面的 benchmark（*testing.B）用。
type T interface {
	Fatalf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// 搭一个"够用"的控制器：indexer 代替 informer cache，fake client 代替真实 API，
// testServer 代替后端 discovery 服务。这样测试能专注在可用性判定逻辑上。
func setupAPIServices(t T, apiServices []runtime.Object) (*AvailableConditionController, *fake.Clientset) {
	fakeClient := fake.NewSimpleClientset()
	apiServiceIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	serviceIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	endpointSliceIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})

	endpointSliceGetter, err := proxy.NewEndpointSliceListerGetter(discoveryv1listers.NewEndpointSliceLister(endpointSliceIndexer))
	if err != nil {
		t.Fatalf("error creating endpointSliceGetter: %v", err)
	}

	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer testServer.Close()

	for _, o := range apiServices {
		if err := apiServiceIndexer.Add(o); err != nil {
			t.Fatalf("failed to add APIService: %v", err)
		}
	}

	c := AvailableConditionController{
		apiServiceClient:    fakeClient.ApiregistrationV1(),
		apiServiceLister:    listers.NewAPIServiceLister(apiServiceIndexer),
		serviceLister:       v1listers.NewServiceLister(serviceIndexer),
		endpointSliceGetter: endpointSliceGetter,
		serviceResolver:     &fakeServiceResolver{url: testServer.URL},
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			// We want a fairly tight requeue time.  The controller listens to the API, but because it relies on the routability of the
			// service network, it is possible for an external, non-watchable factor to affect availability.  This keeps
			// the maximum disruption time to a minimum, but it does prevent hot loops.
			// 这个队列配置本身不属于被测逻辑，只是让 setup 能构造出完整控制器；
			// 你也可以把它看成生产配置的缩小版：起步快、上限低，方便测试里观察重试。
			workqueue.NewTypedItemExponentialFailureRateLimiter[string](5*time.Millisecond, 30*time.Second),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "AvailableConditionController"},
		),
		metrics: availabilitymetrics.New(),
	}
	for _, svc := range apiServices {
		c.addAPIService(svc)
	}
	return &c, fakeClient
}

// 这个基准不追单个 sync 的耗时，而是反复增删更新 Service，
// 看控制器的缓存索引在"大量无关 Service 旁边夹着少数相关 Service"时的表现。
func BenchmarkBuildCache(b *testing.B) {
	apiServiceName := "remote.group"
	// model 1 APIService pointing at a given service, and 30 pointing at local group/versions
	apiServices := []runtime.Object{newRemoteAPIService(apiServiceName)}
	for i := range 30 {
		apiServices = append(apiServices, newLocalAPIService(fmt.Sprintf("local.group%d", i)))
	}
	// model one service backing an API service, and 100 unrelated services
	services := []*v1.Service{newService("foo", "bar", testServicePort, testServicePortName)}
	for i := range 100 {
		services = append(services, newService("foo", fmt.Sprintf("bar%d", i), testServicePort, testServicePortName))
	}
	c, _ := setupAPIServices(b, apiServices)
	b.ReportAllocs()
	b.ResetTimer()
	for n := 1; n <= b.N; n++ {
		for _, svc := range services {
			c.addService(svc)
		}
		for _, svc := range services {
			c.updateService(svc, svc)
		}
		for _, svc := range services {
			c.deleteService(svc)
		}
	}
}

// 只确认"建好缓存后同步一次，确实只产生一条写状态的动作"：
// 缓存构建本身正确与否体现在后续用例里，这里先把基本链路打通。
func TestBuildCache(t *testing.T) {
	tests := []struct {
		name string

		apiServiceName string
		apiServices    []runtime.Object
		services       []*v1.Service

		expectedAvailability apiregistration.APIServiceCondition
	}{
		{
			name:           "api service",
			apiServiceName: "remote.group",
			apiServices:    []runtime.Object{newRemoteAPIService("remote.group")},
			services:       []*v1.Service{newService("foo", "bar", testServicePort, testServicePortName)},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, fakeClient := setupAPIServices(t, tc.apiServices)
			for _, svc := range tc.services {
				c.addService(svc)
			}

			c.sync(tc.apiServiceName)

			// ought to have one action writing status
			if e, a := 1, len(fakeClient.Actions()); e != a {
				t.Fatalf("%v expected %v, got %v", tc.name, e, fakeClient.Actions())
			}
		})
	}
}

// 可用性判定的主表驱动用例：从"本地服务直接可用"到各种失败原因
// （找不到 Service、端口不对、没有端点、端点没就绪、后端返回异常状态码），
// 每个用例对应一条明确的 Reason/Message。
// 把后端状态做成可配置字段，是为了让所有分支共用同一套断言逻辑。
func TestSync(t *testing.T) {
	tests := []struct {
		name string

		apiServiceName  string
		apiServices     []runtime.Object
		services        []*v1.Service
		endpointSlices  []*discoveryv1.EndpointSlice
		backendStatus   int
		backendLocation string

		expectedAvailability apiregistration.APIServiceCondition
		expectedSyncError    string
		expectedSkipped      bool
	}{
		{
			name:           "local",
			apiServiceName: "local.group",
			apiServices:    []runtime.Object{newLocalAPIService("local.group")},
			backendStatus:  http.StatusOK,
			expectedAvailability: apiregistration.APIServiceCondition{
				Type:    apiregistration.Available,
				Status:  apiregistration.ConditionTrue,
				Reason:  "Local",
				Message: "Local APIServices are always available",
			},
			expectedSkipped: true,
		},
		{
			name:           "no service",
			apiServiceName: "remote.group",
			apiServices:    []runtime.Object{newRemoteAPIService("remote.group")},
			services:       []*v1.Service{newService("foo", "not-bar", testServicePort, testServicePortName)},
			backendStatus:  http.StatusOK,
			expectedAvailability: apiregistration.APIServiceCondition{
				Type:    apiregistration.Available,
				Status:  apiregistration.ConditionFalse,
				Reason:  "ServiceNotFound",
				Message: `service/bar in "foo" is not present`,
			},
		},
		{
			name:           "service on bad port",
			apiServiceName: "remote.group",
			apiServices:    []runtime.Object{newRemoteAPIService("remote.group")},
			services: []*v1.Service{{
				ObjectMeta: metav1.ObjectMeta{Namespace: "foo", Name: "bar"},
				Spec: v1.ServiceSpec{
					Type: v1.ServiceTypeClusterIP,
					Ports: []v1.ServicePort{
						{Port: 6443},
					},
				},
			}},
			endpointSlices: []*discoveryv1.EndpointSlice{newEndpointSliceWithAddress("foo", "bar", testServicePort, testServicePortName)},
			backendStatus:  http.StatusOK,
			expectedAvailability: apiregistration.APIServiceCondition{
				Type:    apiregistration.Available,
				Status:  apiregistration.ConditionFalse,
				Reason:  "ServicePortError",
				Message: fmt.Sprintf(`service/bar in "foo" is not listening on port %d`, testServicePort),
			},
		},
		{
			name:           "no endpoints",
			apiServiceName: "remote.group",
			apiServices:    []runtime.Object{newRemoteAPIService("remote.group")},
			services:       []*v1.Service{newService("foo", "bar", testServicePort, testServicePortName)},
			backendStatus:  http.StatusOK,
			expectedAvailability: apiregistration.APIServiceCondition{
				Type:    apiregistration.Available,
				Status:  apiregistration.ConditionFalse,
				Reason:  "EndpointsNotFound",
				Message: `cannot find endpointslices for service/bar in "foo"`,
			},
		},
		{
			name:           "missing endpoints",
			apiServiceName: "remote.group",
			apiServices:    []runtime.Object{newRemoteAPIService("remote.group")},
			services:       []*v1.Service{newService("foo", "bar", testServicePort, testServicePortName)},
			endpointSlices: []*discoveryv1.EndpointSlice{newEndpointSlice("foo", "bar")},
			backendStatus:  http.StatusOK,
			expectedAvailability: apiregistration.APIServiceCondition{
				Type:    apiregistration.Available,
				Status:  apiregistration.ConditionFalse,
				Reason:  "MissingEndpoints",
				Message: `endpointslices for service/bar in "foo" have no addresses with port name "testPort"`,
			},
		},
		{
			name:           "wrong endpoint port name",
			apiServiceName: "remote.group",
			apiServices:    []runtime.Object{newRemoteAPIService("remote.group")},
			services:       []*v1.Service{newService("foo", "bar", testServicePort, testServicePortName)},
			endpointSlices: []*discoveryv1.EndpointSlice{newEndpointSliceWithAddress("foo", "bar", testServicePort, "wrongName")},
			backendStatus:  http.StatusOK,
			expectedAvailability: apiregistration.APIServiceCondition{
				Type:    apiregistration.Available,
				Status:  apiregistration.ConditionFalse,
				Reason:  "MissingEndpoints",
				Message: fmt.Sprintf(`endpointslices for service/bar in "foo" have no addresses with port name "%s"`, testServicePortName),
			},
		},
		{
			name:           "endpoints not ready",
			apiServiceName: "remote.group",
			apiServices:    []runtime.Object{newRemoteAPIService("remote.group")},
			services:       []*v1.Service{newService("foo", "bar", testServicePort, testServicePortName)},
			endpointSlices: []*discoveryv1.EndpointSlice{newUnreadyEndpointSliceWithAddress("foo", "bar", testServicePort, testServicePortName)},
			backendStatus:  http.StatusOK,
			expectedAvailability: apiregistration.APIServiceCondition{
				Type:    apiregistration.Available,
				Status:  apiregistration.ConditionFalse,
				Reason:  "MissingEndpoints",
				Message: fmt.Sprintf(`endpointslices for service/bar in "foo" have no addresses with port name "%s"`, testServicePortName),
			},
		},
		{
			name:           "remote",
			apiServiceName: "remote.group",
			apiServices:    []runtime.Object{newRemoteAPIService("remote.group")},
			services:       []*v1.Service{newService("foo", "bar", testServicePort, testServicePortName)},
			endpointSlices: []*discoveryv1.EndpointSlice{newEndpointSliceWithAddress("foo", "bar", testServicePort, testServicePortName)},
			backendStatus:  http.StatusOK,
			expectedAvailability: apiregistration.APIServiceCondition{
				Type:    apiregistration.Available,
				Status:  apiregistration.ConditionTrue,
				Reason:  "Passed",
				Message: `all checks passed`,
			},
		},
		{
			name:           "remote-bad-return",
			apiServiceName: "remote.group",
			apiServices:    []runtime.Object{newRemoteAPIService("remote.group")},
			services:       []*v1.Service{newService("foo", "bar", testServicePort, testServicePortName)},
			endpointSlices: []*discoveryv1.EndpointSlice{newEndpointSliceWithAddress("foo", "bar", testServicePort, testServicePortName)},
			backendStatus:  http.StatusForbidden,
			expectedAvailability: apiregistration.APIServiceCondition{
				Type:    apiregistration.Available,
				Status:  apiregistration.ConditionFalse,
				Reason:  "FailedDiscoveryCheck",
				Message: `failing or missing response from`,
			},
			expectedSyncError: "failing or missing response from",
		},
		{
			name:            "remote-redirect",
			apiServiceName:  "remote.group",
			apiServices:     []runtime.Object{newRemoteAPIService("remote.group")},
			services:        []*v1.Service{newService("foo", "bar", testServicePort, testServicePortName)},
			endpointSlices:  []*discoveryv1.EndpointSlice{newEndpointSliceWithAddress("foo", "bar", testServicePort, testServicePortName)},
			backendStatus:   http.StatusFound,
			backendLocation: "/test",
			expectedAvailability: apiregistration.APIServiceCondition{
				Type:    apiregistration.Available,
				Status:  apiregistration.ConditionFalse,
				Reason:  "FailedDiscoveryCheck",
				Message: `failing or missing response from`,
			},
			expectedSyncError: "failing or missing response from",
		},
		{
			name:           "remote-304",
			apiServiceName: "remote.group",
			apiServices:    []runtime.Object{newRemoteAPIService("remote.group")},
			services:       []*v1.Service{newService("foo", "bar", testServicePort, testServicePortName)},
			endpointSlices: []*discoveryv1.EndpointSlice{newEndpointSliceWithAddress("foo", "bar", testServicePort, testServicePortName)},
			backendStatus:  http.StatusNotModified,
			expectedAvailability: apiregistration.APIServiceCondition{
				Type:    apiregistration.Available,
				Status:  apiregistration.ConditionFalse,
				Reason:  "FailedDiscoveryCheck",
				Message: `failing or missing response from`,
			},
			expectedSyncError: "failing or missing response from",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fakeClient := fake.NewSimpleClientset(tc.apiServices...)
			apiServiceIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
			serviceIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
			endpointSliceIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
			for _, obj := range tc.apiServices {
				apiServiceIndexer.Add(obj) //nolint:errcheck
			}
			for _, obj := range tc.services {
				serviceIndexer.Add(obj) //nolint:errcheck
			}
			for _, obj := range tc.endpointSlices {
				endpointSliceIndexer.Add(obj) //nolint:errcheck
			}

			endpointSliceGetter, err := proxy.NewEndpointSliceListerGetter(discoveryv1listers.NewEndpointSliceLister(endpointSliceIndexer))
			if err != nil {
				t.Fatalf("error creating endpointSliceGetter: %v", err)
			}

			testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.backendLocation != "" {
					w.Header().Set("Location", tc.backendLocation)
				}
				w.WriteHeader(tc.backendStatus)
			}))
			defer testServer.Close()

			c := AvailableConditionController{
				apiServiceClient:           fakeClient.ApiregistrationV1(),
				apiServiceLister:           listers.NewAPIServiceLister(apiServiceIndexer),
				serviceLister:              v1listers.NewServiceLister(serviceIndexer),
				endpointSliceGetter:        endpointSliceGetter,
				serviceResolver:            &fakeServiceResolver{url: testServer.URL},
				proxyCurrentCertKeyContent: func() ([]byte, []byte) { return emptyCert(), emptyCert() },
				metrics:                    availabilitymetrics.New(),
			}
			err = c.sync(tc.apiServiceName)
			if tc.expectedSyncError != "" {
				if err == nil {
					t.Fatalf("%v expected error with %q, got none", tc.name, tc.expectedSyncError)
				} else if !strings.Contains(err.Error(), tc.expectedSyncError) {
					t.Fatalf("%v expected error with %q, got %q", tc.name, tc.expectedSyncError, err.Error())
				}
			} else if err != nil {
				t.Fatalf("%v unexpected sync error: %v", tc.name, err)
			}

			if tc.expectedSkipped {
				if len(fakeClient.Actions()) > 0 {
					t.Fatalf("%v expected no actions, got %v", tc.name, fakeClient.Actions())
				}
				return
			}

			// ought to have one action writing status
			if e, a := 1, len(fakeClient.Actions()); e != a {
				t.Fatalf("%v expected %v, got %v", tc.name, e, fakeClient.Actions())
			}

			action, ok := fakeClient.Actions()[0].(clienttesting.UpdateAction)
			if !ok {
				t.Fatalf("%v got %v", tc.name, ok)
			}

			if e, a := 1, len(action.GetObject().(*apiregistration.APIService).Status.Conditions); e != a {
				t.Fatalf("%v expected %v, got %v", tc.name, e, action.GetObject())
			}
			condition := action.GetObject().(*apiregistration.APIService).Status.Conditions[0]
			if e, a := tc.expectedAvailability.Type, condition.Type; e != a {
				t.Errorf("%v expected %v, got %#v", tc.name, e, condition)
			}
			if e, a := tc.expectedAvailability.Status, condition.Status; e != a {
				t.Errorf("%v expected %v, got %#v", tc.name, e, condition)
			}
			if e, a := tc.expectedAvailability.Reason, condition.Reason; e != a {
				t.Errorf("%v expected %v, got %#v", tc.name, e, condition)
			}
			if e, a := tc.expectedAvailability.Message, condition.Message; !strings.HasPrefix(a, e) {
				t.Errorf("%v expected %v, got %#v", tc.name, e, condition)
			}
			if condition.LastTransitionTime.IsZero() {
				t.Error("expected lastTransitionTime to be non-zero")
			}
		})
	}
}

type fakeServiceResolver struct {
	url string
}

// 直接把请求指向测试 server，跳过 Service 到 ClusterIP 的解析过程；
// 解析器要验证的是"可用性判定"，不是 DNS/iptables 那部分。
func (f *fakeServiceResolver) ResolveEndpoint(namespace, name string, port int32) (*url.URL, error) {
	return url.Parse(f.url)
}

// 状态没变化就不该写 API：这个优化能避免每个同步周期都产生一次无谓的 Update，
// 进而避免无休止的 watch 事件回环。
func TestUpdateAPIServiceStatus(t *testing.T) {
	foo := &apiregistration.APIService{Status: apiregistration.APIServiceStatus{Conditions: []apiregistration.APIServiceCondition{{Type: "foo"}}}}
	bar := &apiregistration.APIService{Status: apiregistration.APIServiceStatus{Conditions: []apiregistration.APIServiceCondition{{Type: "bar"}}}}

	fakeClient := fake.NewSimpleClientset(foo)
	c := AvailableConditionController{
		apiServiceClient: fakeClient.ApiregistrationV1().(apiregistrationclient.APIServicesGetter),
		metrics:          availabilitymetrics.New(),
	}

	if _, err := c.updateAPIServiceStatus(foo, foo); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e, a := 0, len(fakeClient.Actions()); e != a {
		t.Error(dump.Pretty(fakeClient.Actions()))
	}

	fakeClient.ClearActions()
	if _, err := c.updateAPIServiceStatus(foo, bar); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if e, a := 1, len(fakeClient.Actions()); e != a {
		t.Error(dump.Pretty(fakeClient.Actions()))
	}
}

func emptyCert() []byte {
	return []byte{}
}

// 后端返回错误响应后，HTTP 连接不能被留在空闲池里：
// 那些连接指向的可能是已经被替换掉的后端，留着会在后续请求里继续制造失败。
// 这里借助 ConnState 回调观察连接关闭事件，而不是去窥探 client 内部状态。
func TestCloseIdleConnections(t *testing.T) {
	apiServiceName := "remote.group"
	apiServices := []runtime.Object{newRemoteAPIService(apiServiceName)}
	services := []*v1.Service{newService("foo", "bar", testServicePort, testServicePortName)}
	endpointSlices := []*discoveryv1.EndpointSlice{newEndpointSliceWithAddress("foo", "bar", testServicePort, testServicePortName)}

	fakeClient := fake.NewSimpleClientset(apiServices...)
	apiServiceIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	serviceIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	endpointSliceIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	for _, obj := range apiServices {
		if err := apiServiceIndexer.Add(obj); err != nil {
			t.Fatalf("failed to add APIService: %v", err)
		}
	}
	for _, obj := range services {
		if err := serviceIndexer.Add(obj); err != nil {
			t.Fatalf("failed to add service: %v", err)
		}
	}
	for _, obj := range endpointSlices {
		if err := endpointSliceIndexer.Add(obj); err != nil {
			t.Fatalf("failed to add endpointSlice: %v", err)
		}
	}

	endpointSliceGetter, err := proxy.NewEndpointSliceListerGetter(discoveryv1listers.NewEndpointSliceLister(endpointSliceIndexer))
	if err != nil {
		t.Fatalf("error creating endpointSliceGetter: %v", err)
	}

	closed := make(chan struct{}, 1)
	testServer := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	// We want to verify that the client closes the connection.
	// httptest.Server doesn't expose ConnState, but the underlying http.Server does.
	testServer.Config.ConnState = func(c net.Conn, state http.ConnState) {
		if state == http.StateClosed {
			select {
			case closed <- struct{}{}:
			default:
			}
		}
	}
	testServer.Start()
	defer testServer.Close()

	c := AvailableConditionController{
		apiServiceClient:           fakeClient.ApiregistrationV1(),
		apiServiceLister:           listers.NewAPIServiceLister(apiServiceIndexer),
		serviceLister:              v1listers.NewServiceLister(serviceIndexer),
		endpointSliceGetter:        endpointSliceGetter,
		serviceResolver:            &fakeServiceResolver{url: testServer.URL},
		proxyCurrentCertKeyContent: func() ([]byte, []byte) { return emptyCert(), emptyCert() },
		metrics:                    availabilitymetrics.New(),
	}

	// This should trigger the bad gateway response and (with the fix) close the connection.
	err = c.sync(apiServiceName)
	if err == nil {
		t.Fatal("expected error from sync")
	}

	select {
	case <-closed:
		// success, connection was closed
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for connection to be closed")
	}
}
