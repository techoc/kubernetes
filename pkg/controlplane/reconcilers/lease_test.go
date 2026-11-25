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

package reconcilers

/*
原始来源：
https://github.com/openshift/origin/blob/bb340c5dd5ff72718be86fb194dedc0faed7f4c7/pkg/cmd/server/election/lease_endpoint_reconciler_test.go
*/

import (
	"reflect"
	"slices"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/apitesting"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/storage"
	etcd3testing "k8s.io/apiserver/pkg/storage/etcd3/testing"
	"k8s.io/apiserver/pkg/storage/storagebackend/factory"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/kubernetes/pkg/apis/core"
	netutils "k8s.io/utils/net"
)

// init 注册测试所需的 scheme。
func init() {
	var scheme = runtime.NewScheme()

	metav1.AddToGroupVersion(scheme, metav1.SchemeGroupVersion)
	utilruntime.Must(core.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(scheme.SetVersionPriority(corev1.SchemeGroupVersion))

	codecs = serializer.NewCodecFactory(scheme)
}

// codecs 是测试使用的序列化器工厂。
var codecs serializer.CodecFactory

// fakeLeases 是测试用的租约实现。
type fakeLeases struct {
	storageLeases
}

var _ Leases = &fakeLeases{}

// newFakeLeases 创建 fakeLeases 实例。
func newFakeLeases(t *testing.T, baseKey string, s storage.Interface) *fakeLeases {
	return &fakeLeases{
		storageLeases{
			storage:   s,
			destroyFn: func() {},
			baseKey:   baseKey,
			leaseTime: 1 * time.Minute, // 避免租约在测试期间超时
		},
	}
}

// SetKeys 设置 fake 存储中的租约 key。
func (f *fakeLeases) SetKeys(keys []string) error {
	for _, ip := range keys {
		if err := f.UpdateLease(ip); err != nil {
			return err
		}
	}
	return nil
}

// TestLeaseEndpointReconciler 验证基于租约的 endpoints 协调逻辑。
func TestLeaseEndpointReconciler(t *testing.T) {
	server, sc := etcd3testing.NewUnsecuredEtcd3TestClientServer(t)
	t.Cleanup(func() { server.Terminate(t) })

	newFunc := func() runtime.Object { return &corev1.Endpoints{} }
	newListFunc := func() runtime.Object { return &corev1.EndpointsList{} }
	sc.Codec = apitesting.TestStorageCodec(codecs, corev1.SchemeGroupVersion)

	reconcileTests := []struct {
		testName      string
		serviceName   string
		ip            string
		endpointPorts []corev1.EndpointPort
		endpointKeys  []string
		initialState  []runtime.Object
		expectUpdate  []runtime.Object
		expectCreate  []runtime.Object
		expectLeases  []string
	}{
		{
			testName:      "no existing endpoints",
			serviceName:   "foo",
			ip:            "1.2.3.4",
			endpointPorts: []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
			initialState:  nil,
			expectCreate:  makeEndpointsArray("foo", []string{"1.2.3.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectLeases:  []string{"1.2.3.4"},
		},
		{
			testName:      "existing endpoints satisfy",
			serviceName:   "foo",
			ip:            "1.2.3.4",
			endpointPorts: []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
			initialState:  makeEndpointsArray("foo", []string{"1.2.3.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectLeases:  []string{"1.2.3.4"},
		},
		{
			testName:      "existing endpoints satisfy, no endpointslice",
			serviceName:   "foo",
			ip:            "1.2.3.4",
			endpointPorts: []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
			initialState: []runtime.Object{
				makeEndpoints("foo", []string{"1.2.3.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			},
			expectCreate: []runtime.Object{
				makeEndpointSlice("foo", []string{"1.2.3.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			},
			expectLeases: []string{"1.2.3.4"},
		},
		{
			testName:      "existing endpointslice satisfies, no endpoints",
			serviceName:   "foo",
			ip:            "1.2.3.4",
			endpointPorts: []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
			initialState: []runtime.Object{
				makeEndpointSlice("foo", []string{"1.2.3.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			},
			expectCreate: []runtime.Object{
				makeEndpoints("foo", []string{"1.2.3.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			},
			expectLeases: []string{"1.2.3.4"},
		},
		{
			testName:      "existing endpoints satisfy, endpointslice is wrong",
			serviceName:   "foo",
			ip:            "1.2.3.4",
			endpointPorts: []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
			initialState: []runtime.Object{
				makeEndpoints("foo", []string{"1.2.3.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
				makeEndpointSlice("foo", []string{"4.3.2.1"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			},
			expectUpdate: []runtime.Object{
				makeEndpointSlice("foo", []string{"1.2.3.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			},
			expectLeases: []string{"1.2.3.4"},
		},
		{
			testName:      "existing endpointslice satisfies, endpoints is wrong",
			serviceName:   "foo",
			ip:            "1.2.3.4",
			endpointPorts: []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
			initialState: []runtime.Object{
				makeEndpoints("foo", []string{"4.3.2.1"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
				makeEndpointSlice("foo", []string{"1.2.3.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			},
			expectUpdate: []runtime.Object{
				makeEndpoints("foo", []string{"1.2.3.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			},
			expectLeases: []string{"1.2.3.4"},
		},
		{
			testName:      "existing endpoints satisfy + refresh existing key",
			serviceName:   "foo",
			ip:            "1.2.3.4",
			endpointPorts: []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
			endpointKeys:  []string{"1.2.3.4"},
			initialState:  makeEndpointsArray("foo", []string{"1.2.3.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectLeases:  []string{"1.2.3.4"},
		},
		{
			testName:      "existing endpoints satisfy but too many",
			serviceName:   "foo",
			ip:            "1.2.3.4",
			endpointPorts: []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
			initialState:  makeEndpointsArray("foo", []string{"1.2.3.4", "4.3.2.1"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectUpdate:  makeEndpointsArray("foo", []string{"1.2.3.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectLeases:  []string{"1.2.3.4"},
		},
		{
			testName:      "existing endpoints satisfy but too many + extra masters",
			serviceName:   "foo",
			ip:            "1.2.3.4",
			endpointPorts: []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
			endpointKeys:  []string{"1.2.3.4", "4.3.2.2", "4.3.2.3", "4.3.2.4"},
			initialState:  makeEndpointsArray("foo", []string{"1.2.3.4", "4.3.2.1", "4.3.2.2", "4.3.2.3", "4.3.2.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectUpdate:  makeEndpointsArray("foo", []string{"1.2.3.4", "4.3.2.2", "4.3.2.3", "4.3.2.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectLeases:  []string{"1.2.3.4", "4.3.2.2", "4.3.2.3", "4.3.2.4"},
		},
		{
			testName:      "existing endpoints satisfy but too many + extra masters + delete first",
			serviceName:   "foo",
			ip:            "4.3.2.4",
			endpointPorts: []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
			endpointKeys:  []string{"4.3.2.1", "4.3.2.2", "4.3.2.3", "4.3.2.4"},
			initialState:  makeEndpointsArray("foo", []string{"1.2.3.4", "4.3.2.1", "4.3.2.2", "4.3.2.3", "4.3.2.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectUpdate:  makeEndpointsArray("foo", []string{"4.3.2.1", "4.3.2.2", "4.3.2.3", "4.3.2.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectLeases:  []string{"4.3.2.1", "4.3.2.2", "4.3.2.3", "4.3.2.4"},
		},
		{
			testName:      "existing endpoints current IP missing",
			serviceName:   "foo",
			ip:            "4.3.2.2",
			endpointPorts: []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
			endpointKeys:  []string{"4.3.2.1"},
			initialState:  makeEndpointsArray("foo", []string{"4.3.2.1"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectUpdate:  makeEndpointsArray("foo", []string{"4.3.2.1", "4.3.2.2"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectLeases:  []string{"4.3.2.1", "4.3.2.2"},
		},
		{
			testName:      "existing endpoints wrong name",
			serviceName:   "foo",
			ip:            "1.2.3.4",
			endpointPorts: []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
			initialState:  makeEndpointsArray("bar", []string{"1.2.3.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectCreate:  makeEndpointsArray("foo", []string{"1.2.3.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectLeases:  []string{"1.2.3.4"},
		},
		{
			testName:      "existing endpoints wrong IP",
			serviceName:   "foo",
			ip:            "1.2.3.4",
			endpointPorts: []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
			initialState:  makeEndpointsArray("foo", []string{"4.3.2.1"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectUpdate:  makeEndpointsArray("foo", []string{"1.2.3.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectLeases:  []string{"1.2.3.4"},
		},
		{
			testName:      "existing endpoints wrong port",
			serviceName:   "foo",
			ip:            "1.2.3.4",
			endpointPorts: []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
			initialState:  makeEndpointsArray("foo", []string{"1.2.3.4"}, []corev1.EndpointPort{{Name: "foo", Port: 9090, Protocol: "TCP"}}),
			expectUpdate:  makeEndpointsArray("foo", []string{"1.2.3.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectLeases:  []string{"1.2.3.4"},
		},
		{
			testName:      "existing endpoints wrong protocol",
			serviceName:   "foo",
			ip:            "1.2.3.4",
			endpointPorts: []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
			initialState:  makeEndpointsArray("foo", []string{"1.2.3.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "UDP"}}),
			expectUpdate:  makeEndpointsArray("foo", []string{"1.2.3.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectLeases:  []string{"1.2.3.4"},
		},
		{
			testName:      "existing endpoints wrong port name",
			serviceName:   "foo",
			ip:            "1.2.3.4",
			endpointPorts: []corev1.EndpointPort{{Name: "baz", Port: 8080, Protocol: "TCP"}},
			initialState:  makeEndpointsArray("foo", []string{"1.2.3.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectUpdate:  makeEndpointsArray("foo", []string{"1.2.3.4"}, []corev1.EndpointPort{{Name: "baz", Port: 8080, Protocol: "TCP"}}),
			expectLeases:  []string{"1.2.3.4"},
		},
		{
			testName:      "existing endpoints without skip mirror label",
			serviceName:   "foo",
			ip:            "1.2.3.4",
			endpointPorts: []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
			initialState: []runtime.Object{
				// 此处不能用 makeEndpointsArray()，因为它会添加 Endpoints 的 skip-mirror 标签。
				&corev1.Endpoints{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: metav1.NamespaceDefault,
						Name:      "foo",
					},
					Subsets: []corev1.EndpointSubset{{
						Addresses: []corev1.EndpointAddress{{IP: "1.2.3.4"}},
						Ports:     []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
					}},
				},
				makeEndpointSlice("foo", []string{"1.2.3.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			},
			expectUpdate: []runtime.Object{
				makeEndpoints("foo", []string{"1.2.3.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
				// EndpointSlice 因已正确而无需更新。
			},
			expectLeases: []string{"1.2.3.4"},
		},
		{
			testName:    "existing endpoints extra service ports satisfy",
			serviceName: "foo",
			ip:          "1.2.3.4",
			endpointPorts: []corev1.EndpointPort{
				{Name: "foo", Port: 8080, Protocol: "TCP"},
				{Name: "bar", Port: 1000, Protocol: "TCP"},
				{Name: "baz", Port: 1010, Protocol: "TCP"},
			},
			initialState: makeEndpointsArray("foo", []string{"1.2.3.4"},
				[]corev1.EndpointPort{
					{Name: "foo", Port: 8080, Protocol: "TCP"},
					{Name: "bar", Port: 1000, Protocol: "TCP"},
					{Name: "baz", Port: 1010, Protocol: "TCP"},
				},
			),
			expectLeases: []string{"1.2.3.4"},
		},
		{
			testName:    "existing endpoints extra service ports missing port",
			serviceName: "foo",
			ip:          "1.2.3.4",
			endpointPorts: []corev1.EndpointPort{
				{Name: "foo", Port: 8080, Protocol: "TCP"},
				{Name: "bar", Port: 1000, Protocol: "TCP"},
			},
			initialState: makeEndpointsArray("foo", []string{"1.2.3.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectUpdate: makeEndpointsArray("foo", []string{"1.2.3.4"},
				[]corev1.EndpointPort{
					{Name: "foo", Port: 8080, Protocol: "TCP"},
					{Name: "bar", Port: 1000, Protocol: "TCP"},
				},
			),
			expectLeases: []string{"1.2.3.4"},
		},
	}
	for _, test := range reconcileTests {
		t.Run(test.testName, func(t *testing.T) {
			// 使用与控制面相同的 base key，但加上随机
			// 前缀，以便各子测试独立复用 etcd 实例。
			// 控制面中的对应调用见 pkg/controlplane/instance.go:268：
			// masterLeases, err := reconcilers.NewLeases(config, "/masterleases/", ttl)
			// 背景见 https://issues.k8s.io/114049
			baseKey := "/" + uuid.New().String() + "/masterleases/"
			s, dFunc, err := factory.Create(*sc.ForResource(schema.GroupResource{Resource: "endpoints"}), newFunc, newListFunc, baseKey)
			if err != nil {
				t.Fatalf("Error creating storage: %v", err)
			}
			t.Cleanup(dFunc)
			fakeLeases := newFakeLeases(t, baseKey, s)
			err = fakeLeases.SetKeys(test.endpointKeys)
			if err != nil {
				t.Errorf("unexpected error creating keys: %v", err)
			}
			clientset := fake.NewSimpleClientset(test.initialState...)

			epAdapter := NewEndpointsAdapter(clientset.CoreV1(), clientset.DiscoveryV1())
			r := NewLeaseEndpointReconciler(epAdapter, fakeLeases)
			err = r.ReconcileEndpoints(test.serviceName, netutils.ParseIPSloppy(test.ip), test.endpointPorts, true)
			if err != nil {
				t.Errorf("unexpected error reconciling: %v", err)
			}

			err = verifyCreatesAndUpdates(clientset, test.expectCreate, test.expectUpdate)
			if err != nil {
				t.Errorf("unexpected error in side effects: %v", err)
			}

			leases, err := fakeLeases.ListLeases()
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			// 排序以便比较。
			sort.Strings(leases)
			sort.Strings(test.expectLeases)
			if !reflect.DeepEqual(leases, test.expectLeases) {
				t.Errorf("expected %v got: %v", test.expectLeases, leases)
			}
		})
	}

	nonReconcileTests := []struct {
		testName      string
		serviceName   string
		ip            string
		endpointPorts []corev1.EndpointPort
		endpointKeys  []string
		initialState  []runtime.Object
		expectUpdate  []runtime.Object
		expectCreate  []runtime.Object
		expectLeases  []string
	}{
		{
			testName:    "existing endpoints extra service ports missing port no update",
			serviceName: "foo",
			ip:          "1.2.3.4",
			endpointPorts: []corev1.EndpointPort{
				{Name: "foo", Port: 8080, Protocol: "TCP"},
				{Name: "bar", Port: 1000, Protocol: "TCP"},
			},
			initialState: makeEndpointsArray("foo", []string{"1.2.3.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectUpdate: nil,
			expectLeases: []string{"1.2.3.4"},
		},
		{
			testName:    "existing endpoints extra service ports, wrong ports, wrong IP",
			serviceName: "foo",
			ip:          "1.2.3.4",
			endpointPorts: []corev1.EndpointPort{
				{Name: "foo", Port: 8080, Protocol: "TCP"},
				{Name: "bar", Port: 1000, Protocol: "TCP"},
			},
			initialState: makeEndpointsArray("foo", []string{"4.3.2.1"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectUpdate: makeEndpointsArray("foo", []string{"1.2.3.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectLeases: []string{"1.2.3.4"},
		},
		{
			testName:      "no existing endpoints",
			serviceName:   "foo",
			ip:            "1.2.3.4",
			endpointPorts: []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
			initialState:  nil,
			expectCreate:  makeEndpointsArray("foo", []string{"1.2.3.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectLeases:  []string{"1.2.3.4"},
		},
	}
	for _, test := range nonReconcileTests {
		t.Run(test.testName, func(t *testing.T) {
			// 使用与控制面相同的 base key，但加上随机
			// 前缀，以便各子测试独立复用 etcd 实例。
			// 控制面中的对应调用见 pkg/controlplane/instance.go:268：
			// masterLeases, err := reconcilers.NewLeases(config, "/masterleases/", ttl)
			// 背景见 https://issues.k8s.io/114049
			baseKey := "/" + uuid.New().String() + "/masterleases/"
			s, dFunc, err := factory.Create(*sc.ForResource(schema.GroupResource{Resource: "endpoints"}), newFunc, newListFunc, baseKey)
			if err != nil {
				t.Fatalf("Error creating storage: %v", err)
			}
			t.Cleanup(dFunc)
			fakeLeases := newFakeLeases(t, baseKey, s)
			err = fakeLeases.SetKeys(test.endpointKeys)
			if err != nil {
				t.Errorf("unexpected error creating keys: %v", err)
			}
			clientset := fake.NewSimpleClientset(test.initialState...)
			epAdapter := NewEndpointsAdapter(clientset.CoreV1(), clientset.DiscoveryV1())
			r := NewLeaseEndpointReconciler(epAdapter, fakeLeases)
			err = r.ReconcileEndpoints(test.serviceName, netutils.ParseIPSloppy(test.ip), test.endpointPorts, false)
			if err != nil {
				t.Errorf("unexpected error reconciling: %v", err)
			}

			err = verifyCreatesAndUpdates(clientset, test.expectCreate, test.expectUpdate)
			if err != nil {
				t.Errorf("unexpected error in side effects: %v", err)
			}

			leases, err := fakeLeases.ListLeases()
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			// 排序以便比较。
			sort.Strings(leases)
			sort.Strings(test.expectLeases)
			if !reflect.DeepEqual(leases, test.expectLeases) {
				t.Errorf("expected %v got: %v", test.expectLeases, leases)
			}
		})
	}
}

// TestLeaseRemoveEndpoints 验证移除租约时同步更新 endpoints。
func TestLeaseRemoveEndpoints(t *testing.T) {
	server, sc := etcd3testing.NewUnsecuredEtcd3TestClientServer(t)
	t.Cleanup(func() { server.Terminate(t) })

	newFunc := func() runtime.Object { return &corev1.Endpoints{} }
	newListFunc := func() runtime.Object { return &corev1.EndpointsList{} }
	sc.Codec = apitesting.TestStorageCodec(codecs, corev1.SchemeGroupVersion)

	stopTests := []struct {
		testName         string
		serviceName      string
		ip               string
		endpointPorts    []corev1.EndpointPort
		endpointKeys     []string
		initialState     []runtime.Object
		expectUpdate     []runtime.Object
		expectLeases     []string
		apiServerStartup bool
	}{
		{
			testName:         "successful remove previous endpoints before apiserver starts",
			serviceName:      "foo",
			ip:               "1.2.3.4",
			endpointPorts:    []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
			endpointKeys:     []string{"1.2.3.4", "4.3.2.2", "4.3.2.3", "4.3.2.4"},
			initialState:     makeEndpointsArray("foo", []string{"1.2.3.4", "4.3.2.2", "4.3.2.3", "4.3.2.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectUpdate:     makeEndpointsArray("foo", []string{"4.3.2.2", "4.3.2.3", "4.3.2.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectLeases:     []string{"4.3.2.2", "4.3.2.3", "4.3.2.4"},
			apiServerStartup: true,
		},
		{
			testName:      "successful stop reconciling",
			serviceName:   "foo",
			ip:            "1.2.3.4",
			endpointPorts: []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
			endpointKeys:  []string{"1.2.3.4", "4.3.2.2", "4.3.2.3", "4.3.2.4"},
			initialState:  makeEndpointsArray("foo", []string{"1.2.3.4", "4.3.2.2", "4.3.2.3", "4.3.2.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectUpdate:  makeEndpointsArray("foo", []string{"4.3.2.2", "4.3.2.3", "4.3.2.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectLeases:  []string{"4.3.2.2", "4.3.2.3", "4.3.2.4"},
		},
		{
			testName:      "stop reconciling with ip not in endpoint ip list",
			serviceName:   "foo",
			ip:            "5.6.7.8",
			endpointPorts: []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
			endpointKeys:  []string{"1.2.3.4", "4.3.2.2", "4.3.2.3", "4.3.2.4"},
			initialState:  makeEndpointsArray("foo", []string{"1.2.3.4", "4.3.2.2", "4.3.2.3", "4.3.2.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectLeases:  []string{"1.2.3.4", "4.3.2.2", "4.3.2.3", "4.3.2.4"},
		},
		{
			testName:      "endpoint with no subset",
			serviceName:   "foo",
			ip:            "1.2.3.4",
			endpointPorts: []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
			endpointKeys:  []string{"1.2.3.4", "4.3.2.2", "4.3.2.3", "4.3.2.4"},
			initialState:  makeEndpointsArray("foo", nil, nil),
			expectUpdate:  makeEndpointsArray("foo", []string{"4.3.2.2", "4.3.2.3", "4.3.2.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectLeases:  []string{"4.3.2.2", "4.3.2.3", "4.3.2.4"},
		},
		{
			testName:      "the last API server was shut down cleanly",
			serviceName:   "foo",
			ip:            "1.2.3.4",
			endpointPorts: []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
			endpointKeys:  []string{"1.2.3.4"},
			initialState:  makeEndpointsArray("foo", []string{"1.2.3.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectUpdate:  makeEndpointsArray("foo", []string{}, []corev1.EndpointPort{}),
			expectLeases:  []string{},
		},
	}
	for _, test := range stopTests {
		t.Run(test.testName, func(t *testing.T) {
			// 使用与控制面相同的 base key，但加上随机
			// 前缀，以便各子测试独立复用 etcd 实例。
			// 控制面中的对应调用见 pkg/controlplane/instance.go:268：
			// masterLeases, err := reconcilers.NewLeases(config, "/masterleases/", ttl)
			// 背景见 https://issues.k8s.io/114049
			baseKey := "/" + uuid.New().String() + "/masterleases/"
			s, dFunc, err := factory.Create(*sc.ForResource(schema.GroupResource{Resource: "pods"}), newFunc, newListFunc, baseKey)
			if err != nil {
				t.Fatalf("Error creating storage: %v", err)
			}
			t.Cleanup(dFunc)
			fakeLeases := newFakeLeases(t, baseKey, s)
			err = fakeLeases.SetKeys(test.endpointKeys)
			if err != nil {
				t.Errorf("unexpected error creating keys: %v", err)
			}
			clientset := fake.NewSimpleClientset(test.initialState...)
			epAdapter := NewEndpointsAdapter(clientset.CoreV1(), clientset.DiscoveryV1())
			r := NewLeaseEndpointReconciler(epAdapter, fakeLeases)
			if !test.apiServerStartup {
				r.StopReconciling()
			}
			err = r.RemoveEndpoints(test.serviceName, netutils.ParseIPSloppy(test.ip), test.endpointPorts)
			// IP 不在 endpoints 中时应返回存储错误并停止协调。
			if !slices.Contains(test.endpointKeys, test.ip) {
				if !storage.IsNotFound(err) {
					t.Errorf("expected error StorageError: key not found, Code: 1, Key: /registry/base/key/%s got:  %v", test.ip, err)
				}
			} else if err != nil {
				t.Errorf("unexpected error reconciling: %v", err)
			}

			err = verifyCreatesAndUpdates(clientset, nil, test.expectUpdate)
			if err != nil {
				t.Errorf("unexpected error in side effects: %v", err)
			}

			leases, err := fakeLeases.ListLeases()
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			// 排序以便比较。
			sort.Strings(leases)
			sort.Strings(test.expectLeases)
			if !reflect.DeepEqual(leases, test.expectLeases) {
				t.Errorf("expected %v got: %v", test.expectLeases, leases)
			}
		})
	}
}

// TestApiserverShutdown 验证 apiserver 关闭时停止协调但不误删租约。
func TestApiserverShutdown(t *testing.T) {
	server, sc := etcd3testing.NewUnsecuredEtcd3TestClientServer(t)
	t.Cleanup(func() { server.Terminate(t) })

	newFunc := func() runtime.Object { return &corev1.Endpoints{} }
	newListFunc := func() runtime.Object { return &corev1.EndpointsList{} }
	sc.Codec = apitesting.TestStorageCodec(codecs, corev1.SchemeGroupVersion)

	reconcileTests := []struct {
		testName                string
		serviceName             string
		ip                      string
		endpointPorts           []corev1.EndpointPort
		endpointKeys            []string
		initialState            []runtime.Object
		expectUpdate            []runtime.Object
		expectLeases            []string
		shutDownBeforeReconcile bool
	}{
		{
			testName:                "last apiserver shutdown after endpoint reconcile",
			serviceName:             "foo",
			ip:                      "1.2.3.4",
			endpointPorts:           []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
			endpointKeys:            []string{"1.2.3.4"},
			initialState:            makeEndpointsArray("foo", []string{"1.2.3.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectUpdate:            makeEndpointsArray("foo", []string{}, []corev1.EndpointPort{}),
			expectLeases:            []string{},
			shutDownBeforeReconcile: false,
		},
		{
			testName:                "last apiserver shutdown before endpoint reconcile",
			serviceName:             "foo",
			ip:                      "1.2.3.4",
			endpointPorts:           []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
			endpointKeys:            []string{"1.2.3.4"},
			initialState:            makeEndpointsArray("foo", []string{"1.2.3.4"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectUpdate:            makeEndpointsArray("foo", []string{}, []corev1.EndpointPort{}),
			expectLeases:            []string{},
			shutDownBeforeReconcile: true,
		},
		{
			testName:                "not the last apiserver which was shutdown before endpoint reconcile",
			serviceName:             "foo",
			ip:                      "1.2.3.4",
			endpointPorts:           []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
			endpointKeys:            []string{"1.2.3.4", "4.3.2.1"},
			initialState:            makeEndpointsArray("foo", []string{"1.2.3.4", "4.3.2.1"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectUpdate:            makeEndpointsArray("foo", []string{"4.3.2.1"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectLeases:            []string{"4.3.2.1"},
			shutDownBeforeReconcile: true,
		},
		{
			testName:                "not the last apiserver which was shutdown after endpoint reconcile",
			serviceName:             "foo",
			ip:                      "1.2.3.4",
			endpointPorts:           []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}},
			endpointKeys:            []string{"1.2.3.4", "4.3.2.1"},
			initialState:            makeEndpointsArray("foo", []string{"1.2.3.4", "4.3.2.1"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectUpdate:            makeEndpointsArray("foo", []string{"4.3.2.1"}, []corev1.EndpointPort{{Name: "foo", Port: 8080, Protocol: "TCP"}}),
			expectLeases:            []string{"4.3.2.1"},
			shutDownBeforeReconcile: false,
		},
	}
	for _, test := range reconcileTests {
		t.Run(test.testName, func(t *testing.T) {
			// 使用与控制面相同的 base key，但加上随机
			// 前缀，以便各子测试独立复用 etcd 实例。
			// 控制面中的对应调用见 pkg/controlplane/instance.go:268：
			// masterLeases, err := reconcilers.NewLeases(config, "/masterleases/", ttl)
			// 背景见 https://issues.k8s.io/114049
			baseKey := "/" + uuid.New().String() + "/masterleases/"
			s, dFunc, err := factory.Create(*sc.ForResource(schema.GroupResource{Resource: "endpoints"}), newFunc, newListFunc, baseKey)
			if err != nil {
				t.Fatalf("Error creating storage: %v", err)
			}
			t.Cleanup(dFunc)
			fakeLeases := newFakeLeases(t, baseKey, s)
			err = fakeLeases.SetKeys(test.endpointKeys)
			if err != nil {
				t.Errorf("unexpected error creating keys: %v", err)
			}
			clientset := fake.NewSimpleClientset(test.initialState...)

			epAdapter := NewEndpointsAdapter(clientset.CoreV1(), clientset.DiscoveryV1())
			r := NewLeaseEndpointReconciler(epAdapter, fakeLeases)

			if test.shutDownBeforeReconcile {
				// 先停止 apiserver
				r.StopReconciling()
				err = r.RemoveEndpoints(test.serviceName, netutils.ParseIPSloppy(test.ip), test.endpointPorts)
				if err != nil {
					t.Errorf("unexpected error remove endpoints: %v", err)
				}

				// 在另一个 goroutine 中协调 endpoints。
				err = r.ReconcileEndpoints(test.serviceName, netutils.ParseIPSloppy(test.ip), test.endpointPorts, false)
				if err != nil {
					t.Errorf("unexpected error reconciling: %v", err)
				}
			} else {
				// 先协调 endpoints
				err = r.ReconcileEndpoints(test.serviceName, netutils.ParseIPSloppy(test.ip), test.endpointPorts, false)
				if err != nil {
					t.Errorf("unexpected error reconciling: %v", err)
				}

				r.StopReconciling()
				err = r.RemoveEndpoints(test.serviceName, netutils.ParseIPSloppy(test.ip), test.endpointPorts)
				if err != nil {
					t.Errorf("unexpected error remove endpoints: %v", err)
				}
			}

			err = verifyCreatesAndUpdates(clientset, nil, test.expectUpdate)
			if err != nil {
				t.Errorf("unexpected error in side effects: %v", err)
			}

			leases, err := fakeLeases.ListLeases()
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			// 排序以便比较。
			sort.Strings(leases)
			sort.Strings(test.expectLeases)
			if !reflect.DeepEqual(leases, test.expectLeases) {
				t.Errorf("expected %v got: %v", test.expectLeases, leases)
			}
		})
	}
}
