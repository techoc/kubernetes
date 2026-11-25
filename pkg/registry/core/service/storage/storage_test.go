/*
Copyright 2015 The Kubernetes Authors.

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

package storage

import (
	"context"
	"fmt"
	"net"
	"reflect"
	stdruntime "runtime"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	machineryutilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistrytest "k8s.io/apiserver/pkg/registry/generic/testing"
	"k8s.io/apiserver/pkg/registry/rest"
	svctest "k8s.io/kubernetes/pkg/api/service/testing"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/discovery"
	apisdiscoveryv1 "k8s.io/kubernetes/pkg/apis/discovery/v1"
	podstore "k8s.io/kubernetes/pkg/registry/core/pod/storage"
	"k8s.io/kubernetes/pkg/registry/core/service/ipallocator"
	"k8s.io/kubernetes/pkg/registry/core/service/portallocator"
	endpointslicestore "k8s.io/kubernetes/pkg/registry/discovery/endpointslice/storage"
	"k8s.io/kubernetes/pkg/registry/registrytest"
	netutils "k8s.io/utils/net"
	"k8s.io/utils/ptr"
)

type cleanupFunc func()

// storageEndpointSliceGetter 包装了一个 discovery REST storage，并把其中未版本的
// EndpointSlice 转换为 discoveryv1.EndpointSlice。
type storageEndpointSliceGetter struct {
	rest *endpointslicestore.REST
}

func (sg *storageEndpointSliceGetter) GetEndpointSlices(namespaceName, serviceName string) ([]*discoveryv1.EndpointSlice, error) {
	obj, err := sg.rest.List(context.TODO(), &metainternalversion.ListOptions{LabelSelector: labels.SelectorFromSet(labels.Set{discoveryv1.LabelServiceName: serviceName})})
	if err != nil {
		return nil, err
	}
	unversioned := obj.(*discovery.EndpointSliceList)

	// （用 Convert_discovery_EndpointSliceList_To_v1_EndpointSliceList() 会得到
	// []discoveryv1.EndpointSlice 而不是 []*discoveryv1.EndpointSlice，
	// 所以终究还要遍历结果再构造一个新数组。）
	v1Slices := make([]*discoveryv1.EndpointSlice, len(unversioned.Items))
	for i := range unversioned.Items {
		v1Slices[i] = new(discoveryv1.EndpointSlice)
		if err := apisdiscoveryv1.Convert_discovery_EndpointSlice_To_v1_EndpointSlice(&unversioned.Items[i], v1Slices[i], nil); err != nil {
			return nil, err
		}
	}
	return v1Slices, nil
}

// 大多数测试都用它来创建被测的 registry。
func newStorage(t *testing.T, ipFamilies []api.IPFamily) (*wrapperRESTForTests, *StatusREST, cleanupFunc) {
	return newStorageWithPods(t, ipFamilies, nil, nil)
}

func newStorageWithPods(t *testing.T, ipFamilies []api.IPFamily, pods []api.Pod, endpointSlices []*discovery.EndpointSlice) (*wrapperRESTForTests, *StatusREST, cleanupFunc) {
	etcdStorage, server := registrytest.NewEtcdStorage(t, "")
	restOptions := generic.RESTOptions{
		StorageConfig:           etcdStorage.ForResource(schema.GroupResource{Resource: "services"}),
		Decorator:               generic.UndecoratedStorage,
		DeleteCollectionWorkers: 1,
		ResourcePrefix:          "services",
	}

	ipAllocs := map[api.IPFamily]ipallocator.Interface{}
	for _, fam := range ipFamilies {
		switch fam {
		case api.IPv4Protocol:
			_, cidr, _ := netutils.ParseCIDRSloppy("10.0.0.0/16")
			ipAllocs[fam] = makeIPAllocator(cidr)
		case api.IPv6Protocol:
			_, cidr, _ := netutils.ParseCIDRSloppy("2000::/108")
			ipAllocs[fam] = makeIPAllocator(cidr)
		default:
			t.Fatalf("Unknown IPFamily: %v", fam)
		}
	}

	portAlloc := makePortAllocator(*(machineryutilnet.ParsePortRangeOrDie("30000-32767")))

	// 并非所有测试都会指定 pods 和 endpoints。
	podStorage, err := podstore.NewStorage(generic.RESTOptions{
		StorageConfig:           etcdStorage,
		Decorator:               generic.UndecoratedStorage,
		DeleteCollectionWorkers: 3,
		ResourcePrefix:          "pods",
	}, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error from REST storage: %v", err)
	}
	if pods != nil && len(pods) > 0 {
		ctx := genericregistrytest.NewNamespaceScopeContext(podStorage.Pod.Store, metav1.NamespaceDefault)
		for ix := range pods {
			key, _ := podStorage.Pod.KeyFunc(ctx, pods[ix].Name)
			if err := podStorage.Pod.Storage.Create(ctx, key, &pods[ix], nil, 0, false); err != nil {
				t.Fatalf("Couldn't create pod: %v", err)
			}
		}
	}

	etcdDiscoveryStorage, discoveryServer := registrytest.NewEtcdStorage(t, discovery.GroupName)
	endpointSliceStorage, err := endpointslicestore.NewREST(generic.RESTOptions{
		StorageConfig:  etcdDiscoveryStorage,
		Decorator:      generic.UndecoratedStorage,
		ResourcePrefix: "endpointslices",
	})
	if err != nil {
		t.Fatalf("unexpected error from REST storage: %v", err)
	}
	endpointSliceGetter := &storageEndpointSliceGetter{endpointSliceStorage}

	if len(endpointSlices) > 0 {
		ctx := genericregistrytest.NewNamespaceScopeContext(endpointSliceStorage.Store, metav1.NamespaceDefault)
		for ix := range endpointSlices {
			key, _ := endpointSliceStorage.KeyFunc(ctx, endpointSlices[ix].Name)
			if err := endpointSliceStorage.Store.Storage.Create(ctx, key, endpointSlices[ix], nil, 0, false); err != nil {
				t.Fatalf("Couldn't create endpointslice: %v", err)
			}
		}
	}

	serviceStorage, statusStorage, _, err := NewREST(restOptions, ipFamilies[0], ipAllocs, portAlloc, endpointSliceGetter, podStorage.Pod, nil)
	if err != nil {
		t.Fatalf("unexpected error from REST storage: %v", err)
	}

	cleanup := func() {
		server.Terminate(t)
		discoveryServer.Terminate(t)
		serviceStorage.Destroy()
		endpointSliceStorage.Store.Destroy()
	}

	return &wrapperRESTForTests{serviceStorage}, statusStorage, cleanup
}

func makeIPAllocator(cidr *net.IPNet) ipallocator.Interface {
	al, err := ipallocator.NewInMemory(cidr)
	if err != nil {
		panic(fmt.Sprintf("error creating IP allocator: %v", err))
	}
	return al
}

func makePortAllocator(ports machineryutilnet.PortRange) portallocator.Interface {
	al, err := portallocator.NewInMemory(ports)
	if err != nil {
		panic(fmt.Sprintf("error creating port allocator: %v", err))
	}
	return al
}

// wrapperRESTForTests 是真实 REST 的一个*极简*包装，便于我们做一些
// 专门用于提升测试安全性的事情。
type wrapperRESTForTests struct {
	*REST
}

func (f *wrapperRESTForTests) Create(ctx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error) {
	// 这里做一次 DeepCopy，确保对输入的就地修改不会传播到校验逻辑，
	// 以前就发生过这种情况，
	// 导致本该失败的测试却通过了。
	obj = obj.DeepCopyObject()
	return f.REST.Create(ctx, obj, createValidation, options)
}

//
// 通用 registry 测试
//

// 通用 registry 测试会用到这里。
func validService() *api.Service {
	return svctest.MakeService("foo",
		svctest.SetClusterIPs(api.ClusterIPNone),
		svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
		svctest.SetIPFamilies(api.IPv4Protocol))
}

// TestGenericCreate 验证 Service 创建经过通用 REST Store 时会自动分配 ClusterIP、NodePort 并在写入失败时回滚。
func TestGenericCreate(t *testing.T) {
	storage, _, cleanup := newStorage(t, []api.IPFamily{api.IPv4Protocol})
	defer cleanup()
	test := genericregistrytest.New(t, storage.Store)
	svc := validService()
	svc.ObjectMeta = metav1.ObjectMeta{} // 因为 genericregistrytest
	test.TestCreate(
		// 合法
		svc,
		// 非法
		&api.Service{
			Spec: api.ServiceSpec{},
		},
	)
}

// TestGenericUpdate 验证 Service 类型、IPFamily 和端口变化会正确分配新资源并释放不再使用的旧资源。
func TestGenericUpdate(t *testing.T) {
	clusterInternalTrafficPolicy := api.ServiceInternalTrafficPolicyCluster

	storage, _, cleanup := newStorage(t, []api.IPFamily{api.IPv4Protocol})
	defer cleanup()
	test := genericregistrytest.New(t, storage.Store).AllowCreateOnUpdate()
	test.TestUpdate(
		// 合法
		validService(),
		// updateFunc
		func(obj runtime.Object) runtime.Object {
			object := obj.(*api.Service)
			object.Spec = api.ServiceSpec{
				Selector:        map[string]string{"bar": "baz2"},
				ClusterIP:       api.ClusterIPNone,
				ClusterIPs:      []string{api.ClusterIPNone},
				SessionAffinity: api.ServiceAffinityNone,
				Type:            api.ServiceTypeClusterIP,
				Ports: []api.ServicePort{{
					Port:       6502,
					Protocol:   api.ProtocolTCP,
					TargetPort: intstr.FromInt32(6502),
				}},
				InternalTrafficPolicy: &clusterInternalTrafficPolicy,
			}
			return object
		},
	)
}

func TestGenericDelete(t *testing.T) {
	storage, _, cleanup := newStorage(t, []api.IPFamily{api.IPv4Protocol})
	defer cleanup()
	test := genericregistrytest.New(t, storage.Store).AllowCreateOnUpdate().ReturnDeletedObject()
	test.TestDelete(validService())
}

func TestGenericGet(t *testing.T) {
	storage, _, cleanup := newStorage(t, []api.IPFamily{api.IPv4Protocol})
	defer cleanup()
	test := genericregistrytest.New(t, storage.Store).AllowCreateOnUpdate()
	test.TestGet(validService())
}

func TestGenericList(t *testing.T) {
	storage, _, cleanup := newStorage(t, []api.IPFamily{api.IPv4Protocol})
	defer cleanup()
	test := genericregistrytest.New(t, storage.Store).AllowCreateOnUpdate()
	test.TestList(validService())
}

func TestGenericWatch(t *testing.T) {
	storage, _, cleanup := newStorage(t, []api.IPFamily{api.IPv4Protocol})
	defer cleanup()
	test := genericregistrytest.New(t, storage.Store)
	test.TestWatch(
		validService(),
		// 匹配 labels
		[]labels.Set{},
		// 不匹配 labels
		[]labels.Set{
			{"foo": "bar"},
		},
		// 匹配 fields
		[]fields.Set{
			{"metadata.name": "foo"},
		},
		// 不匹配 fields
		[]fields.Set{
			{"metadata.name": "bar"},
		},
	)
}

func TestGenericShortNames(t *testing.T) {
	storage, _, cleanup := newStorage(t, []api.IPFamily{api.IPv4Protocol})
	defer cleanup()
	expected := []string{"svc"}
	registrytest.AssertShortNames(t, storage, expected)
}

func TestGenericCategories(t *testing.T) {
	storage, _, cleanup := newStorage(t, []api.IPFamily{api.IPv4Protocol})
	defer cleanup()
	expected := []string{"all"}
	registrytest.AssertCategories(t, storage, expected)
}

//
// 内部函数测试
//

// TestNormalizeClusterIPs 覆盖旧 ClusterIP 与新 ClusterIPs 字段在创建/更新时的同步和兼容逻辑。
func TestNormalizeClusterIPs(t *testing.T) {
	makeServiceWithClusterIp := func(clusterIP string, clusterIPs []string) *api.Service {
		return &api.Service{
			Spec: api.ServiceSpec{
				ClusterIP:  clusterIP,
				ClusterIPs: clusterIPs,
			},
		}
	}

	testCases := []struct {
		name               string
		oldService         *api.Service
		newService         *api.Service
		expectedClusterIP  string
		expectedClusterIPs []string
	}{{
		name:               "new - only clusterip used",
		oldService:         nil,
		newService:         makeServiceWithClusterIp("10.0.0.10", nil),
		expectedClusterIP:  "10.0.0.10",
		expectedClusterIPs: []string{"10.0.0.10"},
	}, {
		name:               "new - only clusterips used",
		oldService:         nil,
		newService:         makeServiceWithClusterIp("", []string{"10.0.0.10"}),
		expectedClusterIP:  "", // 这属于校验问题，校验环节会发现它
		expectedClusterIPs: []string{"10.0.0.10"},
	}, {
		name:               "new - both used",
		oldService:         nil,
		newService:         makeServiceWithClusterIp("10.0.0.10", []string{"10.0.0.10"}),
		expectedClusterIP:  "10.0.0.10",
		expectedClusterIPs: []string{"10.0.0.10"},
	}, {
		name:               "update - no change",
		oldService:         makeServiceWithClusterIp("10.0.0.10", []string{"10.0.0.10"}),
		newService:         makeServiceWithClusterIp("10.0.0.10", []string{"10.0.0.10"}),
		expectedClusterIP:  "10.0.0.10",
		expectedClusterIPs: []string{"10.0.0.10"},
	}, {
		name:               "update - malformed change",
		oldService:         makeServiceWithClusterIp("10.0.0.10", []string{"10.0.0.10"}),
		newService:         makeServiceWithClusterIp("10.0.0.11", []string{"10.0.0.11"}),
		expectedClusterIP:  "10.0.0.11",
		expectedClusterIPs: []string{"10.0.0.11"},
	}, {
		name:               "update - malformed change on secondary ip",
		oldService:         makeServiceWithClusterIp("10.0.0.10", []string{"10.0.0.10", "2000::1"}),
		newService:         makeServiceWithClusterIp("10.0.0.11", []string{"10.0.0.11", "3000::1"}),
		expectedClusterIP:  "10.0.0.11",
		expectedClusterIPs: []string{"10.0.0.11", "3000::1"},
	}, {
		name:               "update - upgrade",
		oldService:         makeServiceWithClusterIp("10.0.0.10", []string{"10.0.0.10"}),
		newService:         makeServiceWithClusterIp("10.0.0.10", []string{"10.0.0.10", "2000::1"}),
		expectedClusterIP:  "10.0.0.10",
		expectedClusterIPs: []string{"10.0.0.10", "2000::1"},
	}, {
		name:               "update - downgrade",
		oldService:         makeServiceWithClusterIp("10.0.0.10", []string{"10.0.0.10", "2000::1"}),
		newService:         makeServiceWithClusterIp("10.0.0.10", []string{"10.0.0.10"}),
		expectedClusterIP:  "10.0.0.10",
		expectedClusterIPs: []string{"10.0.0.10"},
	}, {
		name:               "update - user cleared cluster IP",
		oldService:         makeServiceWithClusterIp("10.0.0.10", []string{"10.0.0.10"}),
		newService:         makeServiceWithClusterIp("", []string{"10.0.0.10"}),
		expectedClusterIP:  "",
		expectedClusterIPs: nil,
	}, {
		name:               "update - user cleared clusterIPs", // 为兼容旧客户端必须保留
		oldService:         makeServiceWithClusterIp("10.0.0.10", []string{"10.0.0.10"}),
		newService:         makeServiceWithClusterIp("10.0.0.10", nil),
		expectedClusterIP:  "10.0.0.10",
		expectedClusterIPs: []string{"10.0.0.10"},
	}, {
		name:               "update - user cleared both",
		oldService:         makeServiceWithClusterIp("10.0.0.10", []string{"10.0.0.10"}),
		newService:         makeServiceWithClusterIp("", nil),
		expectedClusterIP:  "",
		expectedClusterIPs: nil,
	}, {
		name:               "update - user cleared ClusterIP but changed clusterIPs",
		oldService:         makeServiceWithClusterIp("10.0.0.10", []string{"10.0.0.10"}),
		newService:         makeServiceWithClusterIp("", []string{"10.0.0.11"}),
		expectedClusterIP:  "", /* validation catches this */
		expectedClusterIPs: []string{"10.0.0.11"},
	}, {
		name:               "update - user cleared ClusterIPs but changed ClusterIP",
		oldService:         makeServiceWithClusterIp("10.0.0.10", []string{"10.0.0.10", "2000::1"}),
		newService:         makeServiceWithClusterIp("10.0.0.11", nil),
		expectedClusterIP:  "10.0.0.11",
		expectedClusterIPs: nil,
	}, {
		name:               "update - user changed from None to ClusterIP",
		oldService:         makeServiceWithClusterIp("None", []string{"None"}),
		newService:         makeServiceWithClusterIp("10.0.0.10", []string{"None"}),
		expectedClusterIP:  "10.0.0.10",
		expectedClusterIPs: []string{"10.0.0.10"},
	}, {
		name:               "update - user changed from ClusterIP to None",
		oldService:         makeServiceWithClusterIp("10.0.0.10", []string{"10.0.0.10"}),
		newService:         makeServiceWithClusterIp("None", []string{"10.0.0.10"}),
		expectedClusterIP:  "None",
		expectedClusterIPs: []string{"None"},
	}, {
		name:               "update - user changed from ClusterIP to None and changed ClusterIPs in a dual stack (new client making a mistake)",
		oldService:         makeServiceWithClusterIp("10.0.0.10", []string{"10.0.0.10", "2000::1"}),
		newService:         makeServiceWithClusterIp("None", []string{"10.0.0.11", "2000::1"}),
		expectedClusterIP:  "None",
		expectedClusterIPs: []string{"10.0.0.11", "2000::1"},
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			normalizeClusterIPs(After{tc.newService}, Before{tc.oldService})

			if tc.newService == nil {
				t.Fatalf("unexpected new service to be nil")
			}

			if tc.newService.Spec.ClusterIP != tc.expectedClusterIP {
				t.Fatalf("expected clusterIP [%v] got [%v]", tc.expectedClusterIP, tc.newService.Spec.ClusterIP)
			}

			if len(tc.newService.Spec.ClusterIPs) != len(tc.expectedClusterIPs) {
				t.Fatalf("expected  clusterIPs %v got %v", tc.expectedClusterIPs, tc.newService.Spec.ClusterIPs)
			}

			for idx, clusterIP := range tc.newService.Spec.ClusterIPs {
				if clusterIP != tc.expectedClusterIPs[idx] {
					t.Fatalf("expected clusterIP [%v] at index[%v] got [%v]", tc.expectedClusterIPs[idx], idx, tc.newService.Spec.ClusterIPs[idx])

				}
			}
		})
	}
}

// TestPatchAllocatedValues 验证客户端未提交系统分配字段时沿用旧值，避免无关更新意外触发重新分配。
func TestPatchAllocatedValues(t *testing.T) {
	testCases := []struct {
		name                    string
		before                  *api.Service
		update                  *api.Service
		expectSameClusterIPs    bool
		expectReducedClusterIPs bool
		expectSameNodePort      bool
		expectSameHCNP          bool
	}{{
		name: "all_patched",
		before: svctest.MakeService("foo",
			svctest.SetTypeLoadBalancer,
			svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
			svctest.SetClusterIPs("10.0.0.93", "2000::76"),
			svctest.SetUniqueNodePorts,
			svctest.SetHealthCheckNodePort(31234)),
		update: svctest.MakeService("foo",
			svctest.SetTypeLoadBalancer,
			svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal)),
		expectSameClusterIPs: true,
		expectSameNodePort:   true,
		expectSameHCNP:       true,
	}, {
		name: "IPs_patched",
		before: svctest.MakeService("foo",
			svctest.SetTypeClusterIP,
			svctest.SetClusterIPs("10.0.0.93", "2000::76"),
			// 这些取值并不合法，但足以验证测试逻辑
			svctest.SetUniqueNodePorts,
			svctest.SetHealthCheckNodePort(31234)),
		update: svctest.MakeService("foo",
			svctest.SetTypeClusterIP),
		expectSameClusterIPs: true,
	}, {
		name: "NPs_patched",
		before: svctest.MakeService("foo",
			svctest.SetTypeNodePort,
			svctest.SetClusterIPs("10.0.0.93", "2000::76"),
			svctest.SetUniqueNodePorts,
			// 该取值并不合法，但足以验证测试逻辑
			svctest.SetHealthCheckNodePort(31234)),
		update: svctest.MakeService("foo",
			svctest.SetTypeNodePort,
			svctest.SetClusterIPs("10.0.0.93", "2000::76")),
		expectSameClusterIPs: true,
		expectSameNodePort:   true,
	}, {
		name: "HCNP_patched",
		before: svctest.MakeService("foo",
			svctest.SetTypeLoadBalancer,
			svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
			svctest.SetClusterIPs("10.0.0.93", "2000::76"),
			svctest.SetUniqueNodePorts,
			svctest.SetHealthCheckNodePort(31234)),
		update: svctest.MakeService("foo",
			svctest.SetTypeLoadBalancer,
			svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
			svctest.SetClusterIPs("10.0.0.93", "2000::76"),
			svctest.SetUniqueNodePorts),
		expectSameClusterIPs: true,
		expectSameNodePort:   true,
		expectSameHCNP:       true,
	}, {
		name: "nothing_patched",
		before: svctest.MakeService("foo",
			svctest.SetTypeExternalName,
			// 这些取值并不合法，但足以验证测试逻辑
			svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
			svctest.SetClusterIPs("10.0.0.93", "2000::76"),
			svctest.SetUniqueNodePorts,
			svctest.SetHealthCheckNodePort(31234)),
		update: svctest.MakeService("foo",
			svctest.SetTypeExternalName,
			svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal)),
	}, {
		name: "reset_NodePort",
		before: svctest.MakeService("foo",
			svctest.SetTypeLoadBalancer,
			svctest.SetAllocateLoadBalancerNodePorts(false),
			svctest.SetClusterIPs("10.0.0.93", "2000::76"),
			svctest.SetUniqueNodePorts,
			svctest.SetHealthCheckNodePort(31234)),
		update: svctest.MakeService("foo",
			svctest.SetTypeLoadBalancer,
			svctest.SetAllocateLoadBalancerNodePorts(false),
			svctest.SetClusterIPs("10.0.0.93", "2000::76"),
			svctest.SetNodePorts(0)),
		expectSameClusterIPs: true,
		expectSameNodePort:   false,
	}, {
		name: "reset_partial_NodePorts",
		before: svctest.MakeService("foo",
			svctest.SetTypeLoadBalancer,
			svctest.SetAllocateLoadBalancerNodePorts(false),
			svctest.SetClusterIPs("10.0.0.93", "2000::76"),
			svctest.SetPorts(
				svctest.MakeServicePort("", 93, intstr.FromInt32(76), api.ProtocolTCP),
				svctest.MakeServicePort("", 94, intstr.FromInt32(76), api.ProtocolTCP),
			),
			svctest.SetUniqueNodePorts,
			svctest.SetHealthCheckNodePort(31234)),
		update: svctest.MakeService("foo",
			svctest.SetTypeLoadBalancer,
			svctest.SetAllocateLoadBalancerNodePorts(false),
			svctest.SetClusterIPs("10.0.0.93", "2000::76"),
			svctest.SetPorts(
				svctest.MakeServicePort("", 93, intstr.FromInt32(76), api.ProtocolTCP),
				svctest.MakeServicePort("", 94, intstr.FromInt32(76), api.ProtocolTCP),
			),
			svctest.SetUniqueNodePorts,
			func(service *api.Service) {
				service.Spec.Ports[1].NodePort = 0
			}),
		expectSameClusterIPs: true,
		expectSameNodePort:   false,
	}, {
		name: "keep_NodePort",
		before: svctest.MakeService("foo",
			svctest.SetTypeLoadBalancer,
			svctest.SetAllocateLoadBalancerNodePorts(true),
			svctest.SetClusterIPs("10.0.0.93", "2000::76"),
			svctest.SetUniqueNodePorts,
			svctest.SetHealthCheckNodePort(31234)),
		update: svctest.MakeService("foo",
			svctest.SetTypeLoadBalancer,
			svctest.SetAllocateLoadBalancerNodePorts(true),
			svctest.SetClusterIPs("10.0.0.93", "2000::76"),
			svctest.SetNodePorts(0)),
		expectSameClusterIPs: true,
		expectSameNodePort:   true,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			update := tc.update.DeepCopy()
			patchAllocatedValues(After{update}, Before{tc.before})

			beforeIP := tc.before.Spec.ClusterIP
			updateIP := update.Spec.ClusterIP
			if tc.expectSameClusterIPs || tc.expectReducedClusterIPs {
				if beforeIP != updateIP {
					t.Errorf("expected clusterIP to be patched: %q != %q", beforeIP, updateIP)
				}
			} else if beforeIP == updateIP {
				t.Errorf("expected clusterIP to not be patched: %q == %q", beforeIP, updateIP)
			}

			beforeIPs := tc.before.Spec.ClusterIPs
			updateIPs := update.Spec.ClusterIPs
			if tc.expectSameClusterIPs {
				if !cmp.Equal(beforeIPs, updateIPs) {
					t.Errorf("expected clusterIPs to be patched: %q != %q", beforeIPs, updateIPs)
				}
			} else if tc.expectReducedClusterIPs {
				if len(updateIPs) != 1 || beforeIPs[0] != updateIPs[0] {
					t.Errorf("expected clusterIPs to be trim-patched: %q -> %q", beforeIPs, updateIPs)
				}
			} else if cmp.Equal(beforeIPs, updateIPs) {
				t.Errorf("expected clusterIPs to not be patched: %q == %q", beforeIPs, updateIPs)
			}

			bNodePorts, uNodePorts := make([]int32, 0), make([]int32, 0)
			for _, item := range tc.before.Spec.Ports {
				bNodePorts = append(bNodePorts, item.NodePort)
			}
			for _, item := range update.Spec.Ports {
				uNodePorts = append(uNodePorts, item.NodePort)
			}
			if tc.expectSameNodePort && !reflect.DeepEqual(bNodePorts, uNodePorts) {
				t.Errorf("expected nodePort to be patched: %v != %v", bNodePorts, uNodePorts)
			} else if !tc.expectSameNodePort && reflect.DeepEqual(bNodePorts, uNodePorts) {
				t.Errorf("expected nodePort to not be patched: %v == %v", bNodePorts, uNodePorts)
			}

			if b, u := tc.before.Spec.HealthCheckNodePort, update.Spec.HealthCheckNodePort; tc.expectSameHCNP && b != u {
				t.Errorf("expected healthCheckNodePort to be patched: %d != %d", b, u)
			} else if !tc.expectSameHCNP && b == u {
				t.Errorf("expected healthCheckNodePort to not be patched: %d == %d", b, u)
			}
		})
	}
}

func TestServiceDefaultOnRead(t *testing.T) {
	// Helper 构造一个基本合法的 ServiceList，测试用例可按需调整。
	makeServiceList := func(tweaks ...svctest.Tweak) *api.ServiceList {
		svc := svctest.MakeService("foo", tweaks...)
		list := &api.ServiceList{
			Items: []api.Service{*svc},
		}
		return list
	}

	testCases := []struct {
		name   string
		input  runtime.Object
		expect runtime.Object
	}{{
		name:  "single v4",
		input: svctest.MakeService("foo", svctest.SetClusterIPs("10.0.0.1")),
		expect: svctest.MakeService("foo", svctest.SetClusterIPs("10.0.0.1"),
			svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
			svctest.SetIPFamilies(api.IPv4Protocol)),
	}, {
		name:  "single v6",
		input: svctest.MakeService("foo", svctest.SetClusterIPs("2000::1")),
		expect: svctest.MakeService("foo", svctest.SetClusterIPs("2000::1"),
			svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
			svctest.SetIPFamilies(api.IPv6Protocol)),
	}, {
		name:  "missing clusterIPs v4",
		input: svctest.MakeService("foo", svctest.SetClusterIP("10.0.0.1")),
		expect: svctest.MakeService("foo", svctest.SetClusterIPs("10.0.0.1"),
			svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
			svctest.SetIPFamilies(api.IPv4Protocol)),
	}, {
		name:  "missing clusterIPs v6",
		input: svctest.MakeService("foo", svctest.SetClusterIP("2000::1")),
		expect: svctest.MakeService("foo", svctest.SetClusterIPs("2000::1"),
			svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
			svctest.SetIPFamilies(api.IPv6Protocol)),
	}, {
		name:  "list v4",
		input: makeServiceList(svctest.SetClusterIPs("10.0.0.1")),
		expect: makeServiceList(svctest.SetClusterIPs("10.0.0.1"),
			svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
			svctest.SetIPFamilies(api.IPv4Protocol)),
	}, {
		name:  "list missing clusterIPs v4",
		input: makeServiceList(svctest.SetClusterIP("10.0.0.1")),
		expect: makeServiceList(svctest.SetClusterIPs("10.0.0.1"),
			svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
			svctest.SetIPFamilies(api.IPv4Protocol)),
	}, {
		name:   "external name",
		input:  makeServiceList(svctest.SetTypeExternalName, svctest.SetInternalTrafficPolicy(api.ServiceInternalTrafficPolicyCluster)),
		expect: makeServiceList(svctest.SetTypeExternalName),
	}, {
		name:  "dual v4v6",
		input: svctest.MakeService("foo", svctest.SetClusterIPs("10.0.0.1", "2000::1")),
		expect: svctest.MakeService("foo", svctest.SetClusterIPs("10.0.0.1", "2000::1"),
			svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
			svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
	}, {
		name:  "dual v6v4",
		input: svctest.MakeService("foo", svctest.SetClusterIPs("2000::1", "10.0.0.1")),
		expect: svctest.MakeService("foo", svctest.SetClusterIPs("2000::1", "10.0.0.1"),
			svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
			svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
	}, {
		name:  "headless",
		input: svctest.MakeService("foo", svctest.SetHeadless),
		expect: svctest.MakeService("foo", svctest.SetHeadless,
			svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
			svctest.SetIPFamilies(api.IPv4Protocol)),
	}, {
		name: "headless selectorless",
		input: svctest.MakeService("foo", svctest.SetHeadless,
			svctest.SetSelector(map[string]string{})),
		expect: svctest.MakeService("foo", svctest.SetHeadless,
			svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
			svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
	}, {
		name: "headless selectorless pre-set",
		input: svctest.MakeService("foo", svctest.SetHeadless,
			svctest.SetSelector(map[string]string{}),
			svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
			svctest.SetIPFamilies(api.IPv6Protocol)),
		expect: svctest.MakeService("foo", svctest.SetHeadless,
			svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
			svctest.SetIPFamilies(api.IPv6Protocol)),
	}, {
		name:  "not Service or ServiceList",
		input: &api.Pod{},
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			storage, _, cleanup := newStorage(t, []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol})
			defer cleanup()

			tmp := tc.input.DeepCopyObject()
			storage.defaultOnRead(tmp)

			svc, ok := tmp.(*api.Service)
			if !ok {
				list, ok := tmp.(*api.ServiceList)
				if !ok {
					return
				}
				svc = &list.Items[0]
			}

			exp, ok := tc.expect.(*api.Service)
			if !ok {
				list, ok := tc.expect.(*api.ServiceList)
				if !ok {
					return
				}
				exp = &list.Items[0]
			}

			// 校验我们已知会受影响的字段
			if want, got := exp.Spec.ClusterIP, svc.Spec.ClusterIP; want != got {
				t.Errorf("clusterIP: expected %v, got %v", want, got)
			}
			if want, got := exp.Spec.ClusterIPs, svc.Spec.ClusterIPs; !reflect.DeepEqual(want, got) {
				t.Errorf("clusterIPs: expected %v, got %v", want, got)
			}
			if want, got := fmtIPFamilyPolicy(exp.Spec.IPFamilyPolicy), fmtIPFamilyPolicy(svc.Spec.IPFamilyPolicy); want != got {
				t.Errorf("ipFamilyPolicy: expected %v, got %v", want, got)
			}
			if want, got := exp.Spec.IPFamilies, svc.Spec.IPFamilies; !reflect.DeepEqual(want, got) {
				t.Errorf("ipFamilies: expected %v, got %v", want, got)
			}
			if want, got := fmtInternalTrafficPolicy(exp.Spec.InternalTrafficPolicy), fmtInternalTrafficPolicy(svc.Spec.InternalTrafficPolicy); want != got {
				t.Errorf("internalTrafficPolicy: expected %v, got %v", want, got)
			}
		})
	}
}

//
// 创建-更新-删除测试的脚手架，很多测试可以、也应该
// 基于它来编写。
//

type cudTestCase struct {
	name         string
	line         string // 非空时会随错误一起打印，用 line() 设置
	create       svcTestCase
	beforeUpdate func(t *testing.T, storage *wrapperRESTForTests)
	update       svcTestCase
}

type svcTestCase struct {
	svc         *api.Service
	expectError bool

	// 这些值本可以通过 Service 推算出来，但那样容易引入测试 bug，
	// 更重要的是会让用例变得
	// 不够自解释。
	expectClusterIPs          bool
	expectStackDowngrade      bool
	expectHeadless            bool
	expectNodePorts           bool
	expectHealthCheckNodePort bool

	// 额外的校验点，由使用该脚手架的测试提供。
	prove []svcTestProof
}

type svcTestProof func(t *testing.T, storage *wrapperRESTForTests, before, after *api.Service)

// 大多数测试都会调用它。
func helpTestCreateUpdateDelete(t *testing.T, testCases []cudTestCase) {
	t.Helper()
	helpTestCreateUpdateDeleteWithFamilies(t, testCases, []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol})
}

func helpTestCreateUpdateDeleteWithFamilies(t *testing.T, testCases []cudTestCase, ipFamilies []api.IPFamily) {
	// 注意：这里不要调用 t.Helper()。把错误定位到本函数的行上
	// 比定位到调用方更有用。

	storage, _, cleanup := newStorage(t, ipFamilies)
	defer cleanup()

	for _, tc := range testCases {
		name := tc.name
		if tc.line != "" {
			name += "__@L" + tc.line
		}
		t.Run(name, func(t *testing.T) {
			ctx := genericregistrytest.NewNamespaceScopeContext(storage.Store, metav1.NamespaceDefault)

			// 按指定内容创建对象并检查结果。
			obj, err := storage.Create(ctx, tc.create.svc, rest.ValidateAllObjectFunc, &metav1.CreateOptions{})
			if tc.create.expectError && err != nil {
				return
			}
			if err != nil {
				t.Fatalf("unexpected error creating service: %v", err)
			}
			defer storage.Delete(ctx, tc.create.svc.Name, rest.ValidateAllObjectFunc, &metav1.DeleteOptions{}) // in case
			if tc.create.expectError && err == nil {
				t.Fatalf("unexpected success creating service")
			}
			createdSvc := obj.(*api.Service)
			if !verifyEquiv(t, "create", &tc.create, createdSvc) {
				return
			}
			verifyExpectations(t, storage, tc.create, tc.create.svc, createdSvc)
			lastSvc := createdSvc

			// 更新阶段是可选的。
			if tc.update.svc != nil {
				// 允许调用方在创建与更新之间做点别的。
				if tc.beforeUpdate != nil {
					tc.beforeUpdate(t, storage)
				}

				// 将对象更新为新状态并检查结果。
				obj, created, err := storage.Update(ctx, tc.update.svc.Name,
					rest.DefaultUpdatedObjectInfo(tc.update.svc), rest.ValidateAllObjectFunc,
					rest.ValidateAllObjectUpdateFunc, false, &metav1.UpdateOptions{})
				if tc.update.expectError && err != nil {
					return
				}
				if err != nil {
					t.Fatalf("unexpected error updating service: %v", err)
				}
				if tc.update.expectError && err == nil {
					t.Fatalf("unexpected success updating service")
				}
				if created {
					t.Fatalf("unexpected create-on-update")
				}
				updatedSvc := obj.(*api.Service)
				if !verifyEquiv(t, "update", &tc.update, updatedSvc) {
					return
				}
				verifyExpectations(t, storage, tc.update, createdSvc, updatedSvc)
				lastSvc = updatedSvc
			}

			// 删除对象并检查结果。
			_, _, err = storage.Delete(ctx, tc.create.svc.Name, rest.ValidateAllObjectFunc, &metav1.DeleteOptions{})
			if err != nil {
				t.Fatalf("unexpected error deleting service: %v", err)
			}
			verifyExpectations(t, storage, svcTestCase{ /* all false */ }, lastSvc, nil)
		})
	}
}

// line 尽可能返回调用方的行号。在用例数量很多的测试里，
// 这很有用 —— 出问题时能更快定位
// 到是哪个用例。
func line() string {
	_, _, line, ok := stdruntime.Caller(1)
	var s string
	if ok {
		s = fmt.Sprintf("%d", line)
	} else {
		s = "<??>"
	}
	return s
}

// 这样做可以让测试辅助函数本身也可被测试。
type testingTInterface interface {
	Helper()
	Errorf(format string, args ...interface{})
}

type fakeTestingT struct {
	t *testing.T
}

func (f fakeTestingT) Helper() {}

func (f fakeTestingT) Errorf(format string, args ...interface{}) {}

func verifyEquiv(t testingTInterface, call string, tc *svcTestCase, got *api.Service) bool {
	t.Helper()

	// 用于对象比较时。
	options := []cmp.Option{
		// 这些是系统赋的值，不需要比较。
		cmpopts.IgnoreFields(api.Service{}, "UID", "ResourceVersion", "CreationTimestamp"),
		// 把 nil 切片和空切片视为相同（例如 clusterIPs）。
		cmpopts.EquateEmpty(),
	}

	// 对于分配类字段，我们希望能清晰区分输入是否指定了取值，
	// 而不是拿零值去对比。
	want := tc.svc.DeepCopy()
	if tc.expectClusterIPs || tc.expectHeadless {
		if want.Spec.ClusterIP == "" {
			want.Spec.ClusterIP = got.Spec.ClusterIP
		}
		if want.Spec.IPFamilyPolicy == nil {
			want.Spec.IPFamilyPolicy = got.Spec.IPFamilyPolicy
		}
		if tc.expectStackDowngrade && len(want.Spec.ClusterIPs) > len(got.Spec.ClusterIPs) {
			want.Spec.ClusterIPs = want.Spec.ClusterIPs[0:1]
		} else if len(got.Spec.ClusterIPs) > len(want.Spec.ClusterIPs) {
			want.Spec.ClusterIPs = append(want.Spec.ClusterIPs, got.Spec.ClusterIPs[len(want.Spec.ClusterIPs):]...)
		}
		if tc.expectStackDowngrade && len(want.Spec.IPFamilies) > len(got.Spec.ClusterIPs) {
			want.Spec.IPFamilies = want.Spec.IPFamilies[0:1]
		} else if len(got.Spec.IPFamilies) > len(want.Spec.IPFamilies) {
			want.Spec.IPFamilies = append(want.Spec.IPFamilies, got.Spec.IPFamilies[len(want.Spec.IPFamilies):]...)
		}
	}

	if tc.expectNodePorts {
		for i := range want.Spec.Ports {
			p := &want.Spec.Ports[i]
			if p.NodePort == 0 {
				p.NodePort = got.Spec.Ports[i].NodePort
			}
		}
	}
	if tc.expectHealthCheckNodePort {
		if want.Spec.HealthCheckNodePort == 0 {
			want.Spec.HealthCheckNodePort = got.Spec.HealthCheckNodePort
		}
	}

	if !cmp.Equal(want, got, options...) {
		t.Errorf("unexpected result from %s:\n%s", call, cmp.Diff(want, got, options...))
		return false
	}
	return true
}

// 谁来监督监督者本身？（用于测试辅助函数自身的测试）
func TestVerifyEquiv(t *testing.T) {
	testCases := []struct {
		name   string
		input  svcTestCase
		output *api.Service
		expect bool
	}{{
		name: "ExternalName",
		input: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeExternalName),
		},
		output: svctest.MakeService("foo", svctest.SetTypeExternalName),
		expect: true,
	}, {
		name: "ClusterIPs_unspecified",
		input: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeClusterIP),
			expectClusterIPs: true,
		},
		output: svctest.MakeService("foo", svctest.SetTypeClusterIP, svctest.SetClusterIPs("10.0.0.1", "2000:1")),
		expect: true,
	}, {
		name: "ClusterIPs_specified",
		input: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeClusterIP, svctest.SetClusterIPs("10.0.0.1", "2000:1")),
			expectClusterIPs: true,
		},
		output: svctest.MakeService("foo", svctest.SetTypeClusterIP, svctest.SetClusterIPs("10.0.0.1", "2000:1")),
		expect: true,
	}, {
		name: "ClusterIPs_wrong",
		input: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeClusterIP, svctest.SetClusterIPs("10.0.0.0", "2000:0")),
			expectClusterIPs: true,
		},
		output: svctest.MakeService("foo", svctest.SetTypeClusterIP, svctest.SetClusterIPs("10.0.0.1", "2000:1")),
		expect: false,
	}, {
		name: "ClusterIPs_partial",
		input: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeClusterIP, svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
		},
		output: svctest.MakeService("foo", svctest.SetTypeClusterIP, svctest.SetClusterIPs("10.0.0.1", "2000:1")),
		expect: true,
	}, {
		name: "NodePort_unspecified",
		input: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeNodePort),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
		output: svctest.MakeService("foo", svctest.SetTypeNodePort, svctest.SetUniqueNodePorts),
		expect: true,
	}, {
		name: "NodePort_specified",
		input: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeNodePort, svctest.SetNodePorts(93)),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
		output: svctest.MakeService("foo", svctest.SetTypeNodePort, svctest.SetNodePorts(93)),
		expect: true,
	}, {
		name: "NodePort_wrong",
		input: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeNodePort, svctest.SetNodePorts(93)),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
		output: svctest.MakeService("foo", svctest.SetTypeNodePort, svctest.SetNodePorts(76)),
		expect: false,
	}, {
		name: "NodePort_partial",
		input: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeNodePort,
				svctest.SetPorts(
					svctest.MakeServicePort("p", 80, intstr.FromInt32(80), api.ProtocolTCP),
					svctest.MakeServicePort("q", 443, intstr.FromInt32(443), api.ProtocolTCP)),
				svctest.SetNodePorts(93)),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
		output: svctest.MakeService("foo", svctest.SetTypeNodePort,
			svctest.SetPorts(
				svctest.MakeServicePort("p", 80, intstr.FromInt32(80), api.ProtocolTCP),
				svctest.MakeServicePort("q", 443, intstr.FromInt32(443), api.ProtocolTCP)),
			svctest.SetNodePorts(93, 76)),
		expect: true,
	}, {
		name: "HealthCheckNodePort_unspecified",
		input: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal)),
			expectClusterIPs:          true,
			expectNodePorts:           true,
			expectHealthCheckNodePort: true,
		},
		output: svctest.MakeService("foo", svctest.SetTypeLoadBalancer,
			svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
			svctest.SetHealthCheckNodePort(93)),
		expect: true,
	}, {
		name: "HealthCheckNodePort_specified",
		input: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
				svctest.SetHealthCheckNodePort(93)),
			expectClusterIPs:          true,
			expectNodePorts:           true,
			expectHealthCheckNodePort: true,
		},
		output: svctest.MakeService("foo", svctest.SetTypeLoadBalancer,
			svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
			svctest.SetHealthCheckNodePort(93)),
		expect: true,
	}, {
		name: "HealthCheckNodePort_wrong",
		input: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
				svctest.SetHealthCheckNodePort(93)),
			expectClusterIPs:          true,
			expectNodePorts:           true,
			expectHealthCheckNodePort: true,
		},
		output: svctest.MakeService("foo", svctest.SetTypeLoadBalancer,
			svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
			svctest.SetHealthCheckNodePort(76)),
		expect: false,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := verifyEquiv(fakeTestingT{t}, "test", &tc.input, tc.output)
			if result != tc.expect {
				t.Errorf("expected %v, got %v", tc.expect, result)
			}
		})
	}
}

func verifyExpectations(t *testing.T, storage *wrapperRESTForTests, tc svcTestCase, before, after *api.Service) {
	t.Helper()

	if tc.expectClusterIPs {
		proveClusterIPsAllocated(t, storage, before, after)
	} else if tc.expectHeadless {
		proveHeadless(t, storage, before, after)
	} else {
		proveClusterIPsDeallocated(t, storage, before, after)
	}
	if tc.expectNodePorts {
		proveNodePortsAllocated(t, storage, before, after)
	} else {
		proveNodePortsDeallocated(t, storage, before, after)
	}
	if tc.expectHealthCheckNodePort {
		proveHealthCheckNodePortAllocated(t, storage, before, after)
	} else {
		proveHealthCheckNodePortDeallocated(t, storage, before, after)
	}

	for _, p := range tc.prove {
		p(t, storage, before, after)
	}
}

func callName(before, after *api.Service) string {
	if before == nil && after != nil {
		return "create"
	}
	if before != nil && after != nil {
		return "update"
	}
	if before != nil && after == nil {
		return "delete"
	}
	panic("this test is broken: before and after are both nil")
}

func ipIsAllocated(t *testing.T, alloc ipallocator.Interface, ipstr string) bool {
	t.Helper()
	ip := netutils.ParseIPSloppy(ipstr)
	if ip == nil {
		t.Errorf("error parsing IP %q", ipstr)
		return false
	}
	return alloc.Has(ip)
}

func portIsAllocated(t *testing.T, alloc portallocator.Interface, port int32) bool {
	t.Helper()
	if port == 0 {
		t.Errorf("port is 0")
		return false
	}
	return alloc.Has(int(port))
}

func proveClusterIPsAllocated(t *testing.T, storage *wrapperRESTForTests, before, after *api.Service) {
	t.Helper()

	if sing, plur := after.Spec.ClusterIP, after.Spec.ClusterIPs[0]; sing != plur {
		t.Errorf("%s: expected clusterIP == clusterIPs[0]: %q != %q", callName(before, after), sing, plur)
	}

	for _, clip := range after.Spec.ClusterIPs {
		if !ipIsAllocated(t, storage.alloc.serviceIPAllocatorsByFamily[familyOf(clip)], clip) {
			t.Errorf("%s: expected clusterIP to be allocated: %q", callName(before, after), clip)
		}
	}

	if lc, lf := len(after.Spec.ClusterIPs), len(after.Spec.IPFamilies); lc != lf {
		t.Errorf("%s: expected same number of clusterIPs and ipFamilies: %d != %d", callName(before, after), lc, lf)
	}

	for i, fam := range after.Spec.IPFamilies {
		if want, got := fam, familyOf(after.Spec.ClusterIPs[i]); want != got {
			t.Errorf("%s: clusterIP is the wrong IP family: want %s, got %s", callName(before, after), want, got)
		}
	}

	if after.Spec.IPFamilyPolicy == nil {
		t.Errorf("%s: expected ipFamilyPolicy to be set", callName(before, after))
	} else {
		pol := *after.Spec.IPFamilyPolicy
		fams := len(after.Spec.IPFamilies)
		clus := 1
		if storage.secondaryIPFamily != "" {
			clus = 2
		}
		if pol == api.IPFamilyPolicySingleStack && fams != 1 {
			t.Errorf("%s: expected 1 ipFamily, got %d", callName(before, after), fams)
		} else if pol == api.IPFamilyPolicyRequireDualStack && fams != 2 {
			t.Errorf("%s: expected 2 ipFamilies, got %d", callName(before, after), fams)
		} else if pol == api.IPFamilyPolicyPreferDualStack && fams != clus {
			t.Errorf("%s: expected %d ipFamilies, got %d", callName(before, after), clus, fams)
		}
	}

	if before != nil {
		if before.Spec.ClusterIP != "" {
			if want, got := before.Spec.ClusterIP, after.Spec.ClusterIP; want != got {
				t.Errorf("%s: wrong clusterIP: wanted %q, got %q", callName(before, after), want, got)
			}
		}
		min := func(x, y int) int {
			if x < y {
				return x
			}
			return y
		}
		for i := 0; i < min(len(before.Spec.ClusterIPs), len(after.Spec.ClusterIPs)); i++ {
			if want, got := before.Spec.ClusterIPs[i], after.Spec.ClusterIPs[i]; want != got {
				t.Errorf("%s: wrong clusterIPs[%d]: wanted %q, got %q", callName(before, after), i, want, got)
			}
		}
		for i := 0; i < min(len(before.Spec.IPFamilies), len(after.Spec.IPFamilies)); i++ {
			if want, got := before.Spec.IPFamilies[i], after.Spec.IPFamilies[i]; want != got {
				t.Errorf("%s: wrong ipFamilies[%d]: wanted %q, got %q", callName(before, after), i, want, got)
			}
		}
	}
}

func proveClusterIPsDeallocated(t *testing.T, storage *wrapperRESTForTests, before, after *api.Service) {
	t.Helper()

	if after != nil && after.Spec.ClusterIP != api.ClusterIPNone {
		if after.Spec.ClusterIP != "" {
			t.Errorf("%s: expected clusterIP to be unset: %q", callName(before, after), after.Spec.ClusterIP)
		}
		if len(after.Spec.ClusterIPs) != 0 {
			t.Errorf("%s: expected clusterIPs to be unset: %q", callName(before, after), after.Spec.ClusterIPs)
		}
	}

	if before != nil && before.Spec.ClusterIP != api.ClusterIPNone {
		for _, clip := range before.Spec.ClusterIPs {
			if ipIsAllocated(t, storage.alloc.serviceIPAllocatorsByFamily[familyOf(clip)], clip) {
				t.Errorf("%s: expected clusterIP to be deallocated: %q", callName(before, after), clip)
			}
		}
	}
}

func proveHeadless(t *testing.T, storage *wrapperRESTForTests, before, after *api.Service) {
	t.Helper()

	if sing, plur := after.Spec.ClusterIP, after.Spec.ClusterIPs[0]; sing != plur {
		t.Errorf("%s: expected clusterIP == clusterIPs[0]: %q != %q", callName(before, after), sing, plur)
	}
	if len(after.Spec.ClusterIPs) != 1 || after.Spec.ClusterIPs[0] != api.ClusterIPNone {
		t.Errorf("%s: expected clusterIPs to be [%q]: %q", callName(before, after), api.ClusterIPNone, after.Spec.ClusterIPs)
	}
}

func proveNodePortsAllocated(t *testing.T, storage *wrapperRESTForTests, before, after *api.Service) {
	t.Helper()

	for _, p := range after.Spec.Ports {
		if !portIsAllocated(t, storage.alloc.serviceNodePorts, p.NodePort) {
			t.Errorf("%s: expected nodePort to be allocated: %d", callName(before, after), p.NodePort)
		}
	}
}

func proveNodePortsDeallocated(t *testing.T, storage *wrapperRESTForTests, before, after *api.Service) {
	t.Helper()

	if after != nil {
		for _, p := range after.Spec.Ports {
			if p.NodePort != 0 {
				t.Errorf("%s: expected nodePort to be unset: %d", callName(before, after), p.NodePort)
			}
		}
	}

	if before != nil {
		for _, p := range before.Spec.Ports {
			if p.NodePort != 0 && portIsAllocated(t, storage.alloc.serviceNodePorts, p.NodePort) {
				t.Errorf("%s: expected nodePort to be deallocated: %d", callName(before, after), p.NodePort)
			}
		}
	}
}

func proveHealthCheckNodePortAllocated(t *testing.T, storage *wrapperRESTForTests, before, after *api.Service) {
	t.Helper()

	if !portIsAllocated(t, storage.alloc.serviceNodePorts, after.Spec.HealthCheckNodePort) {
		t.Errorf("%s: expected healthCheckNodePort to be allocated: %d", callName(before, after), after.Spec.HealthCheckNodePort)
	}
}

func proveHealthCheckNodePortDeallocated(t *testing.T, storage *wrapperRESTForTests, before, after *api.Service) {
	t.Helper()

	if after != nil {
		if after.Spec.HealthCheckNodePort != 0 {
			t.Errorf("%s: expected healthCheckNodePort to be unset: %d", callName(before, after), after.Spec.HealthCheckNodePort)
		}
	}

	if before != nil {
		if before.Spec.HealthCheckNodePort != 0 && portIsAllocated(t, storage.alloc.serviceNodePorts, before.Spec.HealthCheckNodePort) {
			t.Errorf("%s: expected healthCheckNodePort to be deallocated: %d", callName(before, after), before.Spec.HealthCheckNodePort)
		}
	}
}

//
// registry 的功能测试
//

func fmtIPFamilyPolicy(pol *api.IPFamilyPolicy) string {
	if pol == nil {
		return "<nil>"
	}
	return string(*pol)
}

func fmtInternalTrafficPolicy(pol *api.ServiceInternalTrafficPolicy) string {
	if pol == nil {
		return "<nil>"
	}
	return string(*pol)
}

func fmtIPFamilies(fams []api.IPFamily) string {
	if fams == nil {
		return "[]"
	}
	return fmt.Sprintf("%v", fams)
}

// 验证当类型为 ExternalName 时，create 会忽略 IP 与 IPFamily 相关字段。
func TestCreateIgnoresIPsForExternalName(t *testing.T) {
	type testCase struct {
		name        string
		svc         *api.Service
		expectError bool
	}
	// 这些用例从全部组合中挑选出来，
	// 以覆盖所有"有意思"的场景。
	testCases := []struct {
		name            string
		clusterFamilies []api.IPFamily
		cases           []testCase
	}{{
		name:            "singlestack:v6",
		clusterFamilies: []api.IPFamily{api.IPv6Protocol},
		cases: []testCase{{
			name: "Policy:unset_Families:unset",
			svc:  svctest.MakeService("foo"),
		}, {
			name: "Policy:SingleStack_Families:v6",
			svc: svctest.MakeService("foo",
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetIPFamilies(api.IPv6Protocol)),
			expectError: true,
		}, {
			name: "Policy:PreferDualStack_Families:v4v6",
			svc: svctest.MakeService("foo",
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
			expectError: true,
		}, {
			name: "Policy:RequireDualStack_Families:v6v4",
			svc: svctest.MakeService("foo",
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
			expectError: true,
		}},
	}, {
		name:            "dualstack:v6v4",
		clusterFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
		cases: []testCase{{
			name: "Policy:unset_Families:unset",
			svc:  svctest.MakeService("foo"),
		}, {
			name: "Policy:SingleStack_Families:v6",
			svc: svctest.MakeService("foo",
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetIPFamilies(api.IPv6Protocol)),
			expectError: true,
		}, {
			name: "Policy:PreferDualStack_Families:v6v4",
			svc: svctest.MakeService("foo",
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
			expectError: true,
		}, {
			name: "Policy:RequireDualStack_Families:v4v6",
			svc: svctest.MakeService("foo",
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
			expectError: true,
		}},
	}}

	for _, otc := range testCases {
		t.Run(otc.name, func(t *testing.T) {
			storage, _, cleanup := newStorage(t, otc.clusterFamilies)
			defer cleanup()

			for _, itc := range otc.cases {
				t.Run(itc.name, func(t *testing.T) {
					// 本测试只涉及 ExternalName 类型的 Service。
					itc.svc.Spec.Type = api.ServiceTypeExternalName
					itc.svc.Spec.ExternalName = "example.com"

					ctx := genericregistrytest.NewNamespaceScopeContext(storage.Store, metav1.NamespaceDefault)
					createdObj, err := storage.Create(ctx, itc.svc, rest.ValidateAllObjectFunc, &metav1.CreateOptions{})
					if itc.expectError && err != nil {
						return
					}
					if err != nil {
						t.Fatalf("unexpected error creating service: %v", err)
					}
					defer storage.Delete(ctx, itc.svc.Name, rest.ValidateAllObjectFunc, &metav1.DeleteOptions{})
					if itc.expectError && err == nil {
						t.Fatalf("unexpected success creating service")
					}
					createdSvc := createdObj.(*api.Service)

					if want, got := fmtIPFamilyPolicy(nil), fmtIPFamilyPolicy(createdSvc.Spec.IPFamilyPolicy); want != got {
						t.Errorf("wrong IPFamilyPolicy: want %s, got %s", want, got)
					}
					if want, got := fmtIPFamilies(nil), fmtIPFamilies(createdSvc.Spec.IPFamilies); want != got {
						t.Errorf("wrong IPFamilies: want %s, got %s", want, got)
					}
					if len(createdSvc.Spec.ClusterIP) != 0 {
						t.Errorf("expected no clusterIP, got %q", createdSvc.Spec.ClusterIP)
					}
					if len(createdSvc.Spec.ClusterIPs) != 0 {
						t.Errorf("expected no clusterIPs, got %q", createdSvc.Spec.ClusterIPs)
					}
				})
			}
		})
	}
}

// 验证 create 会用 clusterIP 初始化 clusterIPs，这样后面的测试
// 就不必再重复验证这一点。
func TestCreateInitClusterIPsFromClusterIP(t *testing.T) {
	testCases := []struct {
		name            string
		clusterFamilies []api.IPFamily
		svc             *api.Service
	}{{
		name:            "singlestack:v4_clusterip:unset",
		clusterFamilies: []api.IPFamily{api.IPv4Protocol},
		svc:             svctest.MakeService("foo"),
	}, {
		name:            "singlestack:v4_clusterip:set",
		clusterFamilies: []api.IPFamily{api.IPv4Protocol},
		svc: svctest.MakeService("foo",
			svctest.SetClusterIP("10.0.0.1")),
	}, {
		name:            "singlestack:v6_clusterip:unset",
		clusterFamilies: []api.IPFamily{api.IPv6Protocol},
		svc:             svctest.MakeService("foo"),
	}, {
		name:            "singlestack:v6_clusterip:set",
		clusterFamilies: []api.IPFamily{api.IPv6Protocol},
		svc: svctest.MakeService("foo",
			svctest.SetClusterIP("2000::1")),
	}, {
		name:            "dualstack:v4v6_clusterip:unset",
		clusterFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
		svc:             svctest.MakeService("foo"),
	}, {
		name:            "dualstack:v4v6_clusterip:set",
		clusterFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
		svc: svctest.MakeService("foo",
			svctest.SetClusterIP("10.0.0.1")),
	}, {
		name:            "dualstack:v6v4_clusterip:unset",
		clusterFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
		svc:             svctest.MakeService("foo"),
	}, {
		name:            "dualstack:v6v4_clusterip:set",
		clusterFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
		svc: svctest.MakeService("foo",
			svctest.SetClusterIP("2000::1")),
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			storage, _, cleanup := newStorage(t, tc.clusterFamilies)
			defer cleanup()

			ctx := genericregistrytest.NewNamespaceScopeContext(storage.Store, metav1.NamespaceDefault)
			createdObj, err := storage.Create(ctx, tc.svc, rest.ValidateAllObjectFunc, &metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("unexpected error creating service: %v", err)
			}
			createdSvc := createdObj.(*api.Service)

			if createdSvc.Spec.ClusterIP == "" {
				t.Errorf("expected ClusterIP to be set")

			}
			if tc.svc.Spec.ClusterIP != "" {
				if want, got := tc.svc.Spec.ClusterIP, createdSvc.Spec.ClusterIP; want != got {
					t.Errorf("wrong ClusterIP: want %s, got %s", want, got)
				}
			}
			if len(createdSvc.Spec.ClusterIPs) == 0 {
				t.Errorf("expected ClusterIPs to be set")
			}
			if want, got := createdSvc.Spec.ClusterIP, createdSvc.Spec.ClusterIPs[0]; want != got {
				t.Errorf("wrong ClusterIPs[0]: want %s, got %s", want, got)
			}
		})
	}
}

// 验证 create 能正确初始化 IPFamily 相关字段。
func TestCreateInitIPFields(t *testing.T) {
	type testCase struct {
		name           string
		line           string
		svc            *api.Service
		expectError    bool
		expectPolicy   api.IPFamilyPolicy
		expectFamilies []api.IPFamily
		expectHeadless bool
	}
	// 这些用例从全部组合中挑选出来，
	// 以覆盖所有"有意思"的场景。
	testCases := []struct {
		name            string
		clusterFamilies []api.IPFamily
		cases           []testCase
	}{
		{
			name:            "singlestack:v4",
			clusterFamilies: []api.IPFamily{api.IPv4Protocol},
			cases: []testCase{
				//----------------------------------------
				// singlestack:v4 ClusterIPs:unset
				//----------------------------------------
				{
					name:           "ClusterIPs:unset_Policy:unset_Families:unset",
					line:           line(),
					svc:            svctest.MakeService("foo"),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:unset_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:unset_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:unset_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:unset_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:SingleStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:SingleStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:SingleStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:SingleStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:SingleStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:PreferDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:PreferDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:PreferDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:PreferDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:PreferDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:RequireDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:RequireDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:RequireDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:RequireDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:RequireDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				},
				//----------------------------------------
				// singlestack:v4 ClusterIPs:v4
				//----------------------------------------
				{
					name: "ClusterIPs:v4_Policy:unset_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1")),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v4_Policy:unset_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v4_Policy:unset_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4_Policy:unset_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4_Policy:unset_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4_Policy:SingleStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v4_Policy:SingleStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v4_Policy:SingleStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4_Policy:SingleStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4_Policy:SingleStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4_Policy:PreferDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v4_Policy:PreferDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v4_Policy:PreferDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4_Policy:PreferDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4_Policy:PreferDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4_Policy:RequireDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4_Policy:RequireDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4_Policy:RequireDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4_Policy:RequireDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4_Policy:RequireDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				},
				//----------------------------------------
				// singlestack:v4 ClusterIPs:v4v6
				//----------------------------------------
				{
					name: "ClusterIPs:v4v6_Policy:unset_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1")),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:unset_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:unset_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:unset_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:unset_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:SingleStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:SingleStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:SingleStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:SingleStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:SingleStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:PreferDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:PreferDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:PreferDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:PreferDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:PreferDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:RequireDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:RequireDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:RequireDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:RequireDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:RequireDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				},
				//----------------------------------------
				// singlestack:v4 ClusterIPs:v6v4
				//----------------------------------------
				{
					name: "ClusterIPs:v6v4_Policy:unset_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1")),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:unset_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:unset_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:unset_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:unset_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:SingleStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:SingleStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:SingleStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:SingleStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:SingleStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:PreferDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:PreferDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:PreferDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:PreferDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:PreferDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:RequireDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:RequireDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:RequireDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:RequireDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:RequireDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				},
				//----------------------------------------
				// singlestack:v4 Headless
				//----------------------------------------
				{
					name: "Headless_Policy:unset_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:unset_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:unset_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "Headless_Policy:unset_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "Headless_Policy:unset_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "Headless_Policy:SingleStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:SingleStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:SingleStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "Headless_Policy:SingleStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "Headless_Policy:SingleStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "Headless_Policy:PreferDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:PreferDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:PreferDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "Headless_Policy:PreferDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "Headless_Policy:PreferDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "Headless_Policy:RequireDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
					expectError: true,
				}, {
					name: "Headless_Policy:RequireDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "Headless_Policy:RequireDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "Headless_Policy:RequireDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "Headless_Policy:RequireDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				},
				//----------------------------------------
				// singlestack:v4 HeadlessSelectorless
				//----------------------------------------
				{
					name: "HeadlessSelectorless_Policy:unset_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:unset_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:unset_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:unset_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:unset_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:SingleStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:SingleStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:SingleStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:SingleStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "HeadlessSelectorless_Policy:SingleStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "HeadlessSelectorless_Policy:PreferDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:PreferDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:PreferDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:PreferDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:PreferDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:RequireDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:RequireDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:RequireDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:RequireDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:RequireDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				},
			},
		}, {
			name:            "singlestack:v6",
			clusterFamilies: []api.IPFamily{api.IPv6Protocol},
			cases: []testCase{
				//----------------------------------------
				// singlestack:v6 ClusterIPs:unset
				//----------------------------------------
				{
					name:           "ClusterIPs:unset_Policy:unset_Families:unset",
					line:           line(),
					svc:            svctest.MakeService("foo"),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:unset_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:unset_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:unset_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:unset_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:SingleStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:SingleStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:SingleStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:SingleStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:SingleStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:PreferDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:PreferDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:PreferDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:PreferDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:PreferDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:RequireDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:RequireDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:RequireDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:RequireDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:RequireDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				},
				//----------------------------------------
				// singlestack:v6 ClusterIPs:v6
				//----------------------------------------
				{
					name: "ClusterIPs:v6_Policy:unset_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1")),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v6_Policy:unset_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:unset_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v6_Policy:unset_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:unset_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:SingleStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v6_Policy:SingleStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:SingleStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v6_Policy:SingleStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:SingleStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:PreferDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v6_Policy:PreferDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:PreferDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v6_Policy:PreferDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:PreferDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:RequireDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:RequireDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:RequireDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:RequireDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:RequireDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				},
				//----------------------------------------
				// singlestack:v6 ClusterIPs:v4v6
				//----------------------------------------
				{
					name: "ClusterIPs:v4v6_Policy:unset_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1")),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:unset_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:unset_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:unset_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:unset_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:SingleStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:SingleStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:SingleStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:SingleStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:SingleStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:PreferDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:PreferDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:PreferDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:PreferDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:PreferDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:RequireDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:RequireDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:RequireDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:RequireDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:RequireDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				},
				//----------------------------------------
				// singlestack:v6 ClusterIPs:v6v4
				//----------------------------------------
				{
					name: "ClusterIPs:v6v4_Policy:unset_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1")),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:unset_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:unset_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:unset_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:unset_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:SingleStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:SingleStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:SingleStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:SingleStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:SingleStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:PreferDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:PreferDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:PreferDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:PreferDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:PreferDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:RequireDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:RequireDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:RequireDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:RequireDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:RequireDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				},
				//----------------------------------------
				// singlestack:v6 Headless
				//----------------------------------------
				{
					name: "Headless_Policy:unset_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:unset_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "Headless_Policy:unset_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:unset_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "Headless_Policy:unset_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "Headless_Policy:SingleStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:SingleStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "Headless_Policy:SingleStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:SingleStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "Headless_Policy:SingleStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "Headless_Policy:PreferDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:PreferDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "Headless_Policy:PreferDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:PreferDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "Headless_Policy:PreferDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "Headless_Policy:RequireDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
					expectError: true,
				}, {
					name: "Headless_Policy:RequireDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "Headless_Policy:RequireDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "Headless_Policy:RequireDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "Headless_Policy:RequireDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				},
				//----------------------------------------
				// singlestack:v6 HeadlessSelectorless
				//----------------------------------------
				{
					name: "HeadlessSelectorless_Policy:unset_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:unset_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:unset_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:unset_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:unset_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:SingleStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:SingleStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:SingleStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:SingleStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "HeadlessSelectorless_Policy:SingleStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "HeadlessSelectorless_Policy:PreferDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:PreferDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:PreferDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:PreferDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:PreferDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:RequireDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:RequireDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:RequireDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:RequireDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:RequireDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				},
			},
		}, {
			name:            "dualstack:v4v6",
			clusterFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
			cases: []testCase{
				//----------------------------------------
				// dualstack:v4v6 ClusterIPs:unset
				//----------------------------------------
				{
					name:           "ClusterIPs:unset_Policy:unset_Families:unset",
					line:           line(),
					svc:            svctest.MakeService("foo"),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:unset_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:unset_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:unset_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:unset_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:SingleStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:SingleStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:SingleStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:SingleStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:SingleStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:PreferDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:PreferDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:PreferDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:PreferDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:PreferDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:RequireDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:RequireDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:RequireDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:RequireDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:RequireDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				},
				//----------------------------------------
				// dualstack:v4v6 ClusterIPs:v4
				//----------------------------------------
				{
					name: "ClusterIPs:v4_Policy:unset_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1")),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v4_Policy:unset_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v4_Policy:unset_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4_Policy:unset_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4_Policy:unset_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4_Policy:SingleStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v4_Policy:SingleStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v4_Policy:SingleStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4_Policy:SingleStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4_Policy:SingleStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4_Policy:PreferDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v4_Policy:PreferDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v4_Policy:PreferDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4_Policy:PreferDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v4_Policy:PreferDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4_Policy:RequireDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v4_Policy:RequireDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v4_Policy:RequireDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4_Policy:RequireDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v4_Policy:RequireDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				},
				//----------------------------------------
				// dualstack:v4v6 ClusterIPs:v6
				//----------------------------------------
				{
					name: "ClusterIPs:v6_Policy:unset_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1")),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v6_Policy:unset_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:unset_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v6_Policy:unset_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:unset_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:SingleStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v6_Policy:SingleStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:SingleStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v6_Policy:SingleStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:SingleStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:PreferDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v6_Policy:PreferDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:PreferDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v6_Policy:PreferDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:PreferDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v6_Policy:RequireDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v6_Policy:RequireDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:RequireDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v6_Policy:RequireDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:RequireDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				},
				//----------------------------------------
				// dualstack:v4v6 ClusterIPs:v4v6
				//----------------------------------------
				{
					name: "ClusterIPs:v4v6_Policy:unset_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1")),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:unset_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:unset_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:unset_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:unset_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:SingleStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:SingleStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:SingleStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:SingleStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:SingleStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:PreferDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v4v6_Policy:PreferDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v4v6_Policy:PreferDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:PreferDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v4v6_Policy:PreferDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:RequireDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v4v6_Policy:RequireDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v4v6_Policy:RequireDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:RequireDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v4v6_Policy:RequireDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				},
				//----------------------------------------
				// dualstack:v4v6 ClusterIPs:v6v4
				//----------------------------------------
				{
					name: "ClusterIPs:v6v4_Policy:unset_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1")),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:unset_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:unset_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:unset_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:unset_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:SingleStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:SingleStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:SingleStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:SingleStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:SingleStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:PreferDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v6v4_Policy:PreferDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:PreferDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v6v4_Policy:PreferDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:PreferDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v6v4_Policy:RequireDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v6v4_Policy:RequireDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:RequireDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v6v4_Policy:RequireDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:RequireDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				},
				//----------------------------------------
				// dualstack:v4v6 Headless
				//----------------------------------------
				{
					name: "Headless_Policy:unset_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:unset_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:unset_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:unset_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "Headless_Policy:unset_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "Headless_Policy:SingleStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:SingleStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:SingleStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:SingleStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "Headless_Policy:SingleStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "Headless_Policy:PreferDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:PreferDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:PreferDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:PreferDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:PreferDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:RequireDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:RequireDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:RequireDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:RequireDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:RequireDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				},
				//----------------------------------------
				// dualstack:v4v6 HeadlessSelectorless
				//----------------------------------------
				{
					name: "HeadlessSelectorless_Policy:unset_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:unset_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:unset_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:unset_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:unset_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:SingleStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:SingleStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:SingleStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:SingleStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "HeadlessSelectorless_Policy:SingleStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "HeadlessSelectorless_Policy:PreferDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:PreferDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:PreferDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:PreferDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:PreferDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:RequireDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:RequireDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:RequireDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:RequireDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:RequireDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				},
			},
		}, {
			name:            "dualstack:v6v4",
			clusterFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
			cases: []testCase{
				//----------------------------------------
				// dualstack:v6v4 ClusterIPs:unset
				//----------------------------------------
				{
					name:           "ClusterIPs:unset_Policy:unset_Families:unset",
					line:           line(),
					svc:            svctest.MakeService("foo"),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:unset_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:unset_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:unset_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:unset_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:SingleStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:SingleStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:SingleStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:SingleStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:SingleStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:unset_Policy:PreferDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:PreferDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:PreferDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:PreferDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:PreferDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:RequireDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:RequireDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:RequireDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:RequireDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:unset_Policy:RequireDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				},
				//----------------------------------------
				// dualstack:v6v4 ClusterIPs:v4
				//----------------------------------------
				{
					name: "ClusterIPs:v4_Policy:unset_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1")),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v4_Policy:unset_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v4_Policy:unset_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4_Policy:unset_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4_Policy:unset_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4_Policy:SingleStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v4_Policy:SingleStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v4_Policy:SingleStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4_Policy:SingleStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4_Policy:SingleStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4_Policy:PreferDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v4_Policy:PreferDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v4_Policy:PreferDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4_Policy:PreferDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v4_Policy:PreferDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4_Policy:RequireDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v4_Policy:RequireDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v4_Policy:RequireDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4_Policy:RequireDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v4_Policy:RequireDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				},
				//----------------------------------------
				// dualstack:v6v4 ClusterIPs:v6
				//----------------------------------------
				{
					name: "ClusterIPs:v6_Policy:unset_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1")),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v6_Policy:unset_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:unset_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v6_Policy:unset_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:unset_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:SingleStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v6_Policy:SingleStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:SingleStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v6_Policy:SingleStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:SingleStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:PreferDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v6_Policy:PreferDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:PreferDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v6_Policy:PreferDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:PreferDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v6_Policy:RequireDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v6_Policy:RequireDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:RequireDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v6_Policy:RequireDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6_Policy:RequireDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				},
				//----------------------------------------
				// dualstack:v6v4 ClusterIPs:v4v6
				//----------------------------------------
				{
					name: "ClusterIPs:v4v6_Policy:unset_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1")),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:unset_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:unset_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:unset_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:unset_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:SingleStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:SingleStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:SingleStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:SingleStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:SingleStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:PreferDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v4v6_Policy:PreferDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v4v6_Policy:PreferDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:PreferDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v4v6_Policy:PreferDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:RequireDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v4v6_Policy:RequireDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v4v6_Policy:RequireDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v4v6_Policy:RequireDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
				}, {
					name: "ClusterIPs:v4v6_Policy:RequireDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("10.0.0.1", "2000::1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				},
				//----------------------------------------
				// dualstack:v6v4 ClusterIPs:v6v4
				//----------------------------------------
				{
					name: "ClusterIPs:v6v4_Policy:unset_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1")),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:unset_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:unset_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:unset_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:unset_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:SingleStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:SingleStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:SingleStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:SingleStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:SingleStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:PreferDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v6v4_Policy:PreferDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:PreferDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v6v4_Policy:PreferDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:PreferDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v6v4_Policy:RequireDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v6v4_Policy:RequireDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:RequireDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				}, {
					name: "ClusterIPs:v6v4_Policy:RequireDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "ClusterIPs:v6v4_Policy:RequireDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetClusterIPs("2000::1", "10.0.0.1"),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
				},
				//----------------------------------------
				// dualstack:v6v4 Headless
				//----------------------------------------
				{
					name: "Headless_Policy:unset_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:unset_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:unset_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:unset_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "Headless_Policy:unset_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "Headless_Policy:SingleStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:SingleStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:SingleStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:SingleStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "Headless_Policy:SingleStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "Headless_Policy:PreferDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:PreferDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:PreferDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:PreferDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:PreferDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:RequireDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:RequireDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:RequireDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:RequireDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "Headless_Policy:RequireDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				},
				//----------------------------------------
				// dualstack:v6v4 HeadlessSelectorless
				//----------------------------------------
				{
					name: "HeadlessSelectorless_Policy:unset_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:unset_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:unset_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:unset_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:unset_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:SingleStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:SingleStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:SingleStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicySingleStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:SingleStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectError: true,
				}, {
					name: "HeadlessSelectorless_Policy:SingleStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectError: true,
				}, {
					name: "HeadlessSelectorless_Policy:PreferDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:PreferDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:PreferDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:PreferDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:PreferDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyPreferDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:RequireDualStack_Families:unset",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:RequireDualStack_Families:v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:RequireDualStack_Families:v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:RequireDualStack_Families:v4v6",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
					expectHeadless: true,
				}, {
					name: "HeadlessSelectorless_Policy:RequireDualStack_Families:v6v4",
					line: line(),
					svc: svctest.MakeService("foo",
						svctest.SetHeadless,
						svctest.SetSelector(nil),
						svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
						svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
					expectPolicy:   api.IPFamilyPolicyRequireDualStack,
					expectFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
					expectHeadless: true,
				},
			},
		},
	}

	for _, otc := range testCases {
		t.Run(otc.name, func(t *testing.T) {

			// 为提升性能，把该操作放到外层循环。
			storage, _, cleanup := newStorage(t, otc.clusterFamilies)
			defer cleanup()

			for _, itc := range otc.cases {
				t.Run(itc.name+"__@L"+itc.line, func(t *testing.T) {
					ctx := genericregistrytest.NewNamespaceScopeContext(storage.Store, metav1.NamespaceDefault)
					createdObj, err := storage.Create(ctx, itc.svc, rest.ValidateAllObjectFunc, &metav1.CreateOptions{})
					if itc.expectError && err != nil {
						return
					}
					if err != nil {
						t.Fatalf("unexpected error creating service: %v", err)
					}
					defer storage.Delete(ctx, itc.svc.Name, rest.ValidateAllObjectFunc, &metav1.DeleteOptions{})
					if itc.expectError && err == nil {
						t.Fatalf("unexpected success creating service")
					}
					createdSvc := createdObj.(*api.Service)

					if want, got := fmtIPFamilyPolicy(&itc.expectPolicy), fmtIPFamilyPolicy(createdSvc.Spec.IPFamilyPolicy); want != got {
						t.Errorf("wrong IPFamilyPolicy: want %s, got %s", want, got)
					}
					if want, got := fmtIPFamilies(itc.expectFamilies), fmtIPFamilies(createdSvc.Spec.IPFamilies); want != got {
						t.Errorf("wrong IPFamilies: want %s, got %s", want, got)
					}
					if itc.expectHeadless {
						proveHeadless(t, storage, nil, createdSvc)
						return
					}
					proveClusterIPsAllocated(t, storage, nil, createdSvc)
				})
			}
		})
	}
}

// 边界情况足够多，因此专门写一个断言报错的测试是有价值的。
// 其中一些在其他测试里也覆盖了，但集中在这里更清晰。
func TestCreateInvalidClusterIPInputs(t *testing.T) {
	testCases := []struct {
		name     string
		families []api.IPFamily
		svc      *api.Service
		expect   []string
	}{{
		name:     "bad_ipFamilyPolicy",
		families: []api.IPFamily{api.IPv4Protocol},
		svc: svctest.MakeService("foo",
			svctest.SetIPFamilyPolicy(api.IPFamilyPolicy("garbage"))),
		expect: []string{"Unsupported value"},
	}, {
		name:     "requiredual_ipFamilyPolicy_on_singlestack",
		families: []api.IPFamily{api.IPv4Protocol},
		svc: svctest.MakeService("foo",
			svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
		expect: []string{"cluster is not configured for dual-stack"},
	}, {
		name:     "bad_ipFamilies_0_value",
		families: []api.IPFamily{api.IPv4Protocol},
		svc: svctest.MakeService("foo",
			svctest.SetIPFamilies(api.IPFamily("garbage"))),
		expect: []string{"Unsupported value"},
	}, {
		name:     "bad_ipFamilies_1_value",
		families: []api.IPFamily{api.IPv4Protocol},
		svc: svctest.MakeService("foo",
			svctest.SetIPFamilies(api.IPv4Protocol, api.IPFamily("garbage"))),
		expect: []string{"Unsupported value"},
	}, {
		name:     "bad_ipFamilies_2_value",
		families: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
		svc: svctest.MakeService("foo",
			svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol, api.IPFamily("garbage"))),
		expect: []string{"Unsupported value"},
	}, {
		name:     "wrong_ipFamily",
		families: []api.IPFamily{api.IPv4Protocol},
		svc: svctest.MakeService("foo",
			svctest.SetIPFamilies(api.IPv6Protocol)),
		expect: []string{"not configured on this cluster"},
	}, {
		name:     "too_many_ipFamilies_on_singlestack",
		families: []api.IPFamily{api.IPv4Protocol},
		svc: svctest.MakeService("foo",
			svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
		expect: []string{"when multiple IP families are specified"},
	}, {
		name:     "dup_ipFamily_singlestack",
		families: []api.IPFamily{api.IPv4Protocol},
		svc: svctest.MakeService("foo",
			svctest.SetIPFamilies(api.IPv4Protocol, api.IPv4Protocol)),
		expect: []string{"Duplicate value"},
	}, {
		name:     "dup_ipFamily_dualstack",
		families: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
		svc: svctest.MakeService("foo",
			svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol, api.IPv6Protocol)),
		expect: []string{"Duplicate value"},
	}, {
		name:     "bad_IP",
		families: []api.IPFamily{api.IPv4Protocol},
		svc: svctest.MakeService("foo",
			svctest.SetClusterIPs("garbage")),
		expect: []string{"must be a valid IP"},
	}, {
		name:     "IP_wrong_family",
		families: []api.IPFamily{api.IPv4Protocol},
		svc: svctest.MakeService("foo",
			svctest.SetClusterIPs("2000::1")),
		expect: []string{"not configured on this cluster"},
	}, {
		name:     "IP_doesnt_match_family",
		families: []api.IPFamily{api.IPv4Protocol},
		svc: svctest.MakeService("foo",
			svctest.SetIPFamilies(api.IPv4Protocol),
			svctest.SetClusterIPs("2000::1")),
		expect: []string{"expected an IPv4 value as indicated"},
	}, {
		name:     "too_many_IPs_singlestack",
		families: []api.IPFamily{api.IPv4Protocol},
		svc: svctest.MakeService("foo",
			svctest.SetClusterIPs("10.0.0.1", "10.0.0.2")),
		expect: []string{"no more than one IP for each IP family"},
	}, {
		name:     "too_many_IPs_dualstack",
		families: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
		svc: svctest.MakeService("foo",
			svctest.SetClusterIPs("10.0.0.1", "2000::1", "10.0.0.2")),
		expect: []string{"only hold up to 2 values"},
	}, {
		name:     "dup_IPs",
		families: []api.IPFamily{api.IPv4Protocol},
		svc: svctest.MakeService("foo",
			svctest.SetClusterIPs("10.0.0.1", "10.0.0.1")),
		expect: []string{"no more than one IP for each IP family"},
	}, {
		name:     "empty_IP",
		families: []api.IPFamily{api.IPv4Protocol},
		svc: svctest.MakeService("foo",
			svctest.SetClusterIPs("")),
		expect: []string{"must be empty when", "must be a valid IP"},
	}, {
		name:     "None_IP_1",
		families: []api.IPFamily{api.IPv4Protocol},
		svc: svctest.MakeService("foo",
			svctest.SetClusterIPs("10.0.0.1", "None")),
		expect: []string{"must be a valid IP"},
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			storage, _, cleanup := newStorage(t, tc.families)
			defer cleanup()

			ctx := genericregistrytest.NewNamespaceScopeContext(storage.Store, metav1.NamespaceDefault)
			_, err := storage.Create(ctx, tc.svc, rest.ValidateAllObjectFunc, &metav1.CreateOptions{})
			if err == nil {
				t.Fatalf("unexpected success creating service")
			}
			for _, s := range tc.expect {
				if !strings.Contains(err.Error(), s) {
					t.Errorf("expected to find %q in the error:\n  %s", s, err.Error())
				}
			}
		})
	}
}

func TestCreateDeleteReuse(t *testing.T) {
	testCases := []struct {
		name string
		svc  *api.Service
	}{{
		name: "v4",
		svc: svctest.MakeService("foo", svctest.SetTypeNodePort,
			svctest.SetIPFamilies(api.IPv4Protocol)),
	}, {
		name: "v6",
		svc: svctest.MakeService("foo", svctest.SetTypeNodePort,
			svctest.SetIPFamilies(api.IPv6Protocol)),
	}, {
		name: "v4v6",
		svc: svctest.MakeService("foo", svctest.SetTypeNodePort,
			svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
			svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			storage, _, cleanup := newStorage(t, []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol})
			defer cleanup()

			ctx := genericregistrytest.NewNamespaceScopeContext(storage.Store, metav1.NamespaceDefault)

			// 创建它
			createdObj, err := storage.Create(ctx, tc.svc, rest.ValidateAllObjectFunc, &metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("unexpected error creating service: %v", err)
			}
			createdSvc := createdObj.(*api.Service)

			// 确认 IP 和端口已被分配
			proveClusterIPsAllocated(t, storage, tc.svc, createdSvc)
			proveNodePortsAllocated(t, storage, tc.svc, createdSvc)

			// 删除它
			_, _, err = storage.Delete(ctx, tc.svc.Name, rest.ValidateAllObjectFunc, &metav1.DeleteOptions{})
			if err != nil {
				t.Fatalf("unexpected error creating service: %v", err)
			}

			// 确认 IP 和端口已被释放
			proveClusterIPsDeallocated(t, storage, createdSvc, nil)
			proveNodePortsDeallocated(t, storage, createdSvc, nil)

			// 强制使用相同的 IP 和端口
			svc2 := tc.svc.DeepCopy()
			svc2.Name += "2"
			svc2.Spec.ClusterIP = createdSvc.Spec.ClusterIP
			svc2.Spec.ClusterIPs = createdSvc.Spec.ClusterIPs
			svc2.Spec.Ports = createdSvc.Spec.Ports

			// 再次创建
			_, err = storage.Create(ctx, svc2, rest.ValidateAllObjectFunc, &metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("unexpected error creating service: %v", err)
			}

			// 确认 IP 和端口已被分配
			proveClusterIPsAllocated(t, storage, svc2, createdSvc)
			proveNodePortsAllocated(t, storage, svc2, createdSvc)
		})
	}
}

func TestCreateInitNodePorts(t *testing.T) {
	testCases := []struct {
		name            string
		svc             *api.Service
		expectError     bool
		expectNodePorts bool
	}{{
		name:            "type:ExternalName",
		svc:             svctest.MakeService("foo"),
		expectNodePorts: false,
	}, {
		name: "type:ExternalName_with_NodePorts",
		svc: svctest.MakeService("foo",
			svctest.SetUniqueNodePorts),
		expectError: true,
	}, {
		name:            "type:ClusterIP",
		svc:             svctest.MakeService("foo"),
		expectNodePorts: false,
	}, {
		name: "type:ClusterIP_with_NodePorts",
		svc: svctest.MakeService("foo",
			svctest.SetUniqueNodePorts),
		expectError: true,
	}, {
		name: "type:NodePort_single_port_unspecified",
		svc: svctest.MakeService("foo",
			svctest.SetTypeNodePort),
		expectNodePorts: true,
	}, {
		name: "type:NodePort_single_port_specified",
		svc: svctest.MakeService("foo",
			svctest.SetTypeNodePort, svctest.SetUniqueNodePorts),
		expectNodePorts: true,
	}, {
		name: "type:NodePort_multiport_unspecified",
		svc: svctest.MakeService("foo",
			svctest.SetTypeNodePort,
			svctest.SetPorts(
				svctest.MakeServicePort("p", 80, intstr.FromInt32(80), api.ProtocolTCP),
				svctest.MakeServicePort("q", 443, intstr.FromInt32(443), api.ProtocolTCP))),
		expectNodePorts: true,
	}, {
		name: "type:NodePort_multiport_specified",
		svc: svctest.MakeService("foo",
			svctest.SetTypeNodePort,
			svctest.SetPorts(
				svctest.MakeServicePort("p", 80, intstr.FromInt32(80), api.ProtocolTCP),
				svctest.MakeServicePort("q", 443, intstr.FromInt32(443), api.ProtocolTCP)),
			svctest.SetUniqueNodePorts),
		expectNodePorts: true,
	}, {
		name: "type:NodePort_multiport_same",
		svc: svctest.MakeService("foo",
			svctest.SetTypeNodePort,
			svctest.SetPorts(
				svctest.MakeServicePort("p", 80, intstr.FromInt32(80), api.ProtocolTCP),
				svctest.MakeServicePort("q", 443, intstr.FromInt32(443), api.ProtocolTCP)),
			svctest.SetNodePorts(30080, 30080)),
		expectError: true,
	}, {
		name: "type:NodePort_multiport_multiproto_unspecified",
		svc: svctest.MakeService("foo",
			svctest.SetTypeNodePort,
			svctest.SetPorts(
				svctest.MakeServicePort("p", 53, intstr.FromInt32(53), api.ProtocolTCP),
				svctest.MakeServicePort("q", 53, intstr.FromInt32(53), api.ProtocolUDP))),
		expectNodePorts: true,
	}, {
		name: "type:NodePort_multiport_multiproto_specified",
		svc: svctest.MakeService("foo",
			svctest.SetTypeNodePort,
			svctest.SetPorts(
				svctest.MakeServicePort("p", 53, intstr.FromInt32(53), api.ProtocolTCP),
				svctest.MakeServicePort("q", 53, intstr.FromInt32(53), api.ProtocolUDP)),
			svctest.SetUniqueNodePorts),
		expectNodePorts: true,
	}, {
		name: "type:NodePort_multiport_multiproto_same",
		svc: svctest.MakeService("foo",
			svctest.SetTypeNodePort,
			svctest.SetPorts(
				svctest.MakeServicePort("p", 53, intstr.FromInt32(53), api.ProtocolTCP),
				svctest.MakeServicePort("q", 53, intstr.FromInt32(53), api.ProtocolUDP)),
			svctest.SetNodePorts(30053, 30053)),
		expectNodePorts: true,
	}, {
		name: "type:NodePort_multiport_multiproto_conflict",
		svc: svctest.MakeService("foo",
			svctest.SetTypeNodePort,
			svctest.SetPorts(
				svctest.MakeServicePort("p", 93, intstr.FromInt32(93), api.ProtocolTCP),
				svctest.MakeServicePort("q", 76, intstr.FromInt32(76), api.ProtocolUDP)),
			svctest.SetNodePorts(30093, 30093)),
		expectError: true,
	}, {
		name: "type:LoadBalancer_single_port_unspecified:on_alloc:false",
		svc: svctest.MakeService("foo",
			svctest.SetTypeLoadBalancer,
			svctest.SetAllocateLoadBalancerNodePorts(false)),
		expectNodePorts: false,
	}, {
		name: "type:LoadBalancer_single_port_unspecified:on_alloc:true",
		svc: svctest.MakeService("foo",
			svctest.SetTypeLoadBalancer,
			svctest.SetAllocateLoadBalancerNodePorts(true)),
		expectNodePorts: true,
	}, {
		name: "type:LoadBalancer_single_port_specified:on_alloc:false",
		svc: svctest.MakeService("foo",
			svctest.SetTypeLoadBalancer,
			svctest.SetUniqueNodePorts,
			svctest.SetAllocateLoadBalancerNodePorts(false)),
		expectNodePorts: true,
	}, {
		name: "type:LoadBalancer_single_port_specified:on_alloc:true",
		svc: svctest.MakeService("foo",
			svctest.SetTypeLoadBalancer,
			svctest.SetUniqueNodePorts,
			svctest.SetAllocateLoadBalancerNodePorts(true)),
		expectNodePorts: true,
	}, {
		name: "type:LoadBalancer_multiport_unspecified:on_alloc:false",
		svc: svctest.MakeService("foo",
			svctest.SetTypeLoadBalancer,
			svctest.SetPorts(
				svctest.MakeServicePort("p", 80, intstr.FromInt32(80), api.ProtocolTCP),
				svctest.MakeServicePort("q", 443, intstr.FromInt32(443), api.ProtocolTCP)),
			svctest.SetAllocateLoadBalancerNodePorts(false)),
		expectNodePorts: false,
	}, {
		name: "type:LoadBalancer_multiport_unspecified:on_alloc:true",
		svc: svctest.MakeService("foo",
			svctest.SetTypeLoadBalancer,
			svctest.SetPorts(
				svctest.MakeServicePort("p", 80, intstr.FromInt32(80), api.ProtocolTCP),
				svctest.MakeServicePort("q", 443, intstr.FromInt32(443), api.ProtocolTCP)),
			svctest.SetAllocateLoadBalancerNodePorts(true)),
		expectNodePorts: true,
	}, {
		name: "type:LoadBalancer_multiport_specified:on_alloc:false",
		svc: svctest.MakeService("foo",
			svctest.SetTypeLoadBalancer,
			svctest.SetPorts(
				svctest.MakeServicePort("p", 80, intstr.FromInt32(80), api.ProtocolTCP),
				svctest.MakeServicePort("q", 443, intstr.FromInt32(443), api.ProtocolTCP)),
			svctest.SetUniqueNodePorts,
			svctest.SetAllocateLoadBalancerNodePorts(false)),
		expectNodePorts: true,
	}, {
		name: "type:LoadBalancer_multiport_specified:on_alloc:true",
		svc: svctest.MakeService("foo",
			svctest.SetTypeLoadBalancer,
			svctest.SetPorts(
				svctest.MakeServicePort("p", 80, intstr.FromInt32(80), api.ProtocolTCP),
				svctest.MakeServicePort("q", 443, intstr.FromInt32(443), api.ProtocolTCP)),
			svctest.SetUniqueNodePorts,
			svctest.SetAllocateLoadBalancerNodePorts(true)),
		expectNodePorts: true,
	}, {
		name: "type:LoadBalancer_multiport_same",
		svc: svctest.MakeService("foo",
			svctest.SetTypeLoadBalancer,
			svctest.SetPorts(
				svctest.MakeServicePort("p", 80, intstr.FromInt32(80), api.ProtocolTCP),
				svctest.MakeServicePort("q", 443, intstr.FromInt32(443), api.ProtocolTCP)),
			svctest.SetNodePorts(30080, 30080)),
		expectError: true,
	}, {
		name: "type:LoadBalancer_multiport_multiproto_unspecified",
		svc: svctest.MakeService("foo",
			svctest.SetTypeLoadBalancer,
			svctest.SetPorts(
				svctest.MakeServicePort("p", 53, intstr.FromInt32(53), api.ProtocolTCP),
				svctest.MakeServicePort("q", 53, intstr.FromInt32(53), api.ProtocolUDP))),
		expectNodePorts: true,
	}, {
		name: "type:LoadBalancer_multiport_multiproto_specified",
		svc: svctest.MakeService("foo",
			svctest.SetTypeLoadBalancer,
			svctest.SetPorts(
				svctest.MakeServicePort("p", 53, intstr.FromInt32(53), api.ProtocolTCP),
				svctest.MakeServicePort("q", 53, intstr.FromInt32(53), api.ProtocolUDP)),
			svctest.SetUniqueNodePorts),
		expectNodePorts: true,
	}, {
		name: "type:LoadBalancer_multiport_multiproto_same",
		svc: svctest.MakeService("foo",
			svctest.SetTypeLoadBalancer,
			svctest.SetPorts(
				svctest.MakeServicePort("p", 53, intstr.FromInt32(53), api.ProtocolTCP),
				svctest.MakeServicePort("q", 53, intstr.FromInt32(53), api.ProtocolUDP)),
			svctest.SetNodePorts(30053, 30053)),
		expectNodePorts: true,
	}, {
		name: "type:LoadBalancer_multiport_multiproto_conflict",
		svc: svctest.MakeService("foo",
			svctest.SetTypeLoadBalancer,
			svctest.SetPorts(
				svctest.MakeServicePort("p", 93, intstr.FromInt32(93), api.ProtocolTCP),
				svctest.MakeServicePort("q", 76, intstr.FromInt32(76), api.ProtocolUDP)),
			svctest.SetNodePorts(30093, 30093)),
		expectError: true,
	}}

	// 为提升性能，把该操作放到外层作用域。
	storage, _, cleanup := newStorage(t, []api.IPFamily{api.IPv4Protocol})
	defer cleanup()

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := genericregistrytest.NewNamespaceScopeContext(storage.Store, metav1.NamespaceDefault)
			createdObj, err := storage.Create(ctx, tc.svc, rest.ValidateAllObjectFunc, &metav1.CreateOptions{})
			if tc.expectError && err != nil {
				return
			}
			if err != nil {
				t.Fatalf("unexpected error creating service: %v", err)
			}
			defer storage.Delete(ctx, tc.svc.Name, rest.ValidateAllObjectFunc, &metav1.DeleteOptions{})
			if tc.expectError && err == nil {
				t.Fatalf("unexpected success creating service")
			}
			createdSvc := createdObj.(*api.Service)

			// 构造端口下标到 nodePort 取值的映射，排除零值。
			ports := map[int]*api.ServicePort{}
			for i := range createdSvc.Spec.Ports {
				p := &createdSvc.Spec.Ports[i]
				if p.NodePort != 0 {
					ports[i] = p
				}
			}

			if tc.expectNodePorts && len(ports) == 0 {
				t.Fatalf("expected NodePorts to be allocated, found none")
			}
			if !tc.expectNodePorts && len(ports) > 0 {
				t.Fatalf("expected NodePorts to not be allocated, found %v", ports)
			}
			if !tc.expectNodePorts {
				return
			}

			// 确认分配数量正确
			if want, got := len(ports), len(tc.svc.Spec.Ports); want != got {
				t.Fatalf("expected %d NodePorts, found %d", want, got)
			}

			// 确认它们都处于已分配状态
			for _, p := range ports {
				if !portIsAllocated(t, storage.alloc.serviceNodePorts, p.NodePort) {
					t.Errorf("expected port to be allocated: %v", p)
				}
			}

			// 确认指定的取值都已正确分配
			for i, p := range tc.svc.Spec.Ports {
				if p.NodePort != 0 {
					if ports[i].NodePort != p.NodePort {
						t.Errorf("expected Ports[%d].NodePort to be %d, got %d", i, p.NodePort, ports[i].NodePort)
					}
					// 从集合中移除已请求的端口
					delete(ports, i)
				}
			}

			// 确认分配到的端口互不重复
			seen := map[int32]int32{}
			for i, p := range ports {
				// 同一个 Port 的不同协议允许复用相同的 NodePort，
				// 因此这里不做限制。
				if prev, found := seen[p.NodePort]; found && prev != p.Port {
					t.Errorf("found non-unique allocation in Ports[%d].NodePort: %d -> %d", i, p.NodePort, p.Port)
				}
				seen[p.NodePort] = p.Port
			}
		})
	}
}

// 验证 Headless Service 在创建时跳过分配。
func TestCreateSkipsAllocationsForHeadless(t *testing.T) {
	testCases := []struct {
		name            string
		clusterFamilies []api.IPFamily
		svc             *api.Service
		expectError     bool
	}{{
		name:            "singlestack:v4",
		clusterFamilies: []api.IPFamily{api.IPv4Protocol},
		svc:             svctest.MakeService("foo"),
	}, {
		name:            "singlestack:v6",
		clusterFamilies: []api.IPFamily{api.IPv6Protocol},
		svc:             svctest.MakeService("foo"),
	}, {
		name:            "dualstack:v4v6",
		clusterFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
		svc:             svctest.MakeService("foo"),
	}, {
		name:            "dualstack:v6v4",
		clusterFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
		svc:             svctest.MakeService("foo"),
	}, {
		name:            "singlestack:v4_type:NodePort",
		clusterFamilies: []api.IPFamily{api.IPv4Protocol},
		svc:             svctest.MakeService("foo", svctest.SetTypeNodePort),
		expectError:     true,
	}, {
		name:            "singlestack:v6_type:LoadBalancer",
		clusterFamilies: []api.IPFamily{api.IPv6Protocol},
		svc:             svctest.MakeService("foo", svctest.SetTypeLoadBalancer),
		expectError:     true,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			storage, _, cleanup := newStorage(t, tc.clusterFamilies)
			defer cleanup()

			// 本测试只涉及 headless Service。
			tc.svc.Spec.ClusterIP = api.ClusterIPNone

			ctx := genericregistrytest.NewNamespaceScopeContext(storage.Store, metav1.NamespaceDefault)
			createdObj, err := storage.Create(ctx, tc.svc, rest.ValidateAllObjectFunc, &metav1.CreateOptions{})
			if tc.expectError && err != nil {
				return
			}
			if err != nil {
				t.Fatalf("unexpected error creating service: %v", err)
			}
			if tc.expectError && err == nil {
				t.Fatalf("unexpected success creating service")
			}
			createdSvc := createdObj.(*api.Service)

			if createdSvc.Spec.ClusterIP != "None" {
				t.Errorf("expected clusterIP \"None\", got %q", createdSvc.Spec.ClusterIP)
			}
			if !reflect.DeepEqual(createdSvc.Spec.ClusterIPs, []string{"None"}) {
				t.Errorf("expected clusterIPs [\"None\"], got %q", createdSvc.Spec.ClusterIPs)
			}
		})
	}
}

// 验证 dry-run 创建不会真正分配 IP 或端口。
func TestCreateDryRun(t *testing.T) {
	testCases := []struct {
		name            string
		clusterFamilies []api.IPFamily
		svc             *api.Service
	}{{
		name:            "singlestack:v4_clusterip:unset",
		clusterFamilies: []api.IPFamily{api.IPv4Protocol},
		svc:             svctest.MakeService("foo"),
	}, {
		name:            "singlestack:v4_clusterip:set",
		clusterFamilies: []api.IPFamily{api.IPv4Protocol},
		svc:             svctest.MakeService("foo", svctest.SetClusterIPs("10.0.0.1")),
	}, {
		name:            "singlestack:v6_clusterip:unset",
		clusterFamilies: []api.IPFamily{api.IPv6Protocol},
		svc:             svctest.MakeService("foo"),
	}, {
		name:            "singlestack:v6_clusterip:set",
		clusterFamilies: []api.IPFamily{api.IPv6Protocol},
		svc:             svctest.MakeService("foo", svctest.SetClusterIPs("2000::1")),
	}, {
		name:            "dualstack:v4v6_clusterip:unset",
		clusterFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
		svc:             svctest.MakeService("foo", svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
	}, {
		name:            "dualstack:v4v6_clusterip:set",
		clusterFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
		svc:             svctest.MakeService("foo", svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack), svctest.SetClusterIPs("10.0.0.1", "2000::1")),
	}, {
		name:            "singlestack:v4_type:NodePort_nodeport:unset",
		clusterFamilies: []api.IPFamily{api.IPv4Protocol},
		svc:             svctest.MakeService("foo", svctest.SetTypeNodePort),
	}, {
		name:            "singlestack:v4_type:LoadBalancer_nodePort:set",
		clusterFamilies: []api.IPFamily{api.IPv4Protocol},
		svc:             svctest.MakeService("foo", svctest.SetTypeLoadBalancer, svctest.SetUniqueNodePorts),
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			storage, _, cleanup := newStorage(t, tc.clusterFamilies)
			defer cleanup()

			ctx := genericregistrytest.NewNamespaceScopeContext(storage.Store, metav1.NamespaceDefault)
			createdObj, err := storage.Create(ctx, tc.svc, rest.ValidateAllObjectFunc, &metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
			if err != nil {
				t.Fatalf("unexpected error creating service: %v", err)
			}
			createdSvc := createdObj.(*api.Service)

			// 确认 IP 已被赋值
			if netutils.ParseIPSloppy(createdSvc.Spec.ClusterIP) == nil {
				t.Errorf("expected valid clusterIP: %q", createdSvc.Spec.ClusterIP)
			}
			for _, ip := range createdSvc.Spec.ClusterIPs {
				if netutils.ParseIPSloppy(ip) == nil {
					t.Errorf("expected valid clusterIP: %q", createdSvc.Spec.ClusterIP)
				}
			}

			// 确认分配器中没有残留分配。
			proveClusterIPsDeallocated(t, storage, createdSvc, nil)
			if tc.svc.Spec.Type != api.ServiceTypeClusterIP {
				proveNodePortsDeallocated(t, storage, createdSvc, nil)
			}
		})
	}
}

func TestDeleteWithFinalizer(t *testing.T) {
	svcName := "foo"

	storage, _, cleanup := newStorage(t, []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol})
	defer cleanup()

	// 这次会分配 cluster IP、NodePort 和 HealthCheckNodePort。
	svc := svctest.MakeService(svcName, svctest.SetTypeLoadBalancer,
		svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
		func(s *api.Service) {
			s.Finalizers = []string{"example.com/test"}
		})

	ctx := genericregistrytest.NewNamespaceScopeContext(storage.Store, metav1.NamespaceDefault)

	// 带 finalizer 创建它。
	obj, err := storage.Create(ctx, svc, rest.ValidateAllObjectFunc, &metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("unexpected error creating service: %v", err)
	}
	createdSvc := obj.(*api.Service)

	// 验证所有资源都已分配。
	obj, err = storage.Get(ctx, svcName, &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("unexpected error getting service: %v", err)
	}
	if !cmp.Equal(createdSvc, obj) {
		t.Errorf("expected the result of Create() and Get() to match: %v", cmp.Diff(createdSvc, obj))
	}
	proveClusterIPsAllocated(t, storage, svc, createdSvc)
	proveNodePortsAllocated(t, storage, svc, createdSvc)
	proveHealthCheckNodePortAllocated(t, storage, svc, createdSvc)

	// 尝试删除，但应被 finalizer 阻塞。
	obj, deleted, err := storage.Delete(ctx, svcName, rest.ValidateAllObjectFunc, &metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("unexpected error deleting service: %v", err)
	}
	if deleted {
		t.Fatalf("expected service to not be deleted")
	}
	deletedSvc := obj.(*api.Service)

	// 验证所有资源仍然处于已分配状态。
	_, err = storage.Get(ctx, svcName, &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("unexpected error getting service: %v", err)
	}
	proveClusterIPsAllocated(t, storage, svc, createdSvc)
	proveNodePortsAllocated(t, storage, svc, createdSvc)
	proveHealthCheckNodePortAllocated(t, storage, svc, createdSvc)

	// 清除 finalizer —— 此时应能删除。
	deletedSvc.Finalizers = nil
	_, _, err = storage.Update(ctx, svcName,
		rest.DefaultUpdatedObjectInfo(deletedSvc), rest.ValidateAllObjectFunc,
		rest.ValidateAllObjectUpdateFunc, false, &metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("unexpected error updating service: %v", err)
	}

	// 验证所有资源都已释放。
	_, err = storage.Get(ctx, svcName, &metav1.GetOptions{})
	if err == nil {
		t.Fatalf("unexpected success getting service")
	}
	proveClusterIPsDeallocated(t, storage, createdSvc, nil)
	proveNodePortsDeallocated(t, storage, createdSvc, nil)
	proveHealthCheckNodePortDeallocated(t, storage, createdSvc, nil)
}

// 验证 dry-run 删除不会真正释放 IP 或端口。
func TestDeleteDryRun(t *testing.T) {
	testCases := []struct {
		name string
		svc  *api.Service
	}{
		{
			name: "v4",
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetIPFamilies(api.IPv4Protocol),
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal)),
		},
		{
			name: "v4v6",
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol),
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal)),
		}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			storage, _, cleanup := newStorage(t, tc.svc.Spec.IPFamilies)
			defer cleanup()

			ctx := genericregistrytest.NewNamespaceScopeContext(storage.Store, metav1.NamespaceDefault)
			createdObj, err := storage.Create(ctx, tc.svc, rest.ValidateAllObjectFunc, &metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("unexpected error creating service: %v", err)
			}
			createdSvc := createdObj.(*api.Service)

			// 确认 IP 和端口已被分配
			proveClusterIPsAllocated(t, storage, tc.svc, createdSvc)
			proveNodePortsAllocated(t, storage, tc.svc, createdSvc)
			proveHealthCheckNodePortAllocated(t, storage, tc.svc, createdSvc)

			_, _, err = storage.Delete(ctx, tc.svc.Name, rest.ValidateAllObjectFunc, &metav1.DeleteOptions{DryRun: []string{metav1.DryRunAll}})
			if err != nil {
				t.Fatalf("unexpected error deleting service: %v", err)
			}

			// 确认它们仍处于已分配状态。
			proveClusterIPsAllocated(t, storage, tc.svc, createdSvc)
			proveNodePortsAllocated(t, storage, tc.svc, createdSvc)
			proveHealthCheckNodePortAllocated(t, storage, tc.svc, createdSvc)
		})
	}
}

// 验证 dry-run 更新不会真正分配或释放 IP 与端口。
func TestUpdateDryRun(t *testing.T) {
	testCases := []struct {
		name            string
		clusterFamilies []api.IPFamily
		svc             *api.Service
		update          *api.Service
		verifyDryAllocs bool
	}{{
		name:            "singlestack:v4_NoAllocs-Allocs",
		clusterFamilies: []api.IPFamily{api.IPv4Protocol},
		svc:             svctest.MakeService("foo", svctest.SetTypeExternalName),
		update: svctest.MakeService("foo", svctest.SetTypeLoadBalancer,
			svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal)),
		verifyDryAllocs: true, // 确认这些取值未被真正分配。
	}, {
		name:            "singlestack:v4_Allocs-NoAllocs",
		clusterFamilies: []api.IPFamily{api.IPv4Protocol},
		svc: svctest.MakeService("foo", svctest.SetTypeLoadBalancer,
			svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal)),
		update:          svctest.MakeService("foo", svctest.SetTypeExternalName),
		verifyDryAllocs: false, // 确认这些取值未被真正释放。
	}, {
		name:            "singlestack:v6_NoAllocs-Allocs",
		clusterFamilies: []api.IPFamily{api.IPv6Protocol},
		svc:             svctest.MakeService("foo", svctest.SetTypeExternalName),
		update: svctest.MakeService("foo", svctest.SetTypeLoadBalancer,
			svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal)),
		verifyDryAllocs: true, // 确认这些取值未被真正分配。
	}, {
		name:            "singlestack:v6_Allocs-NoAllocs",
		clusterFamilies: []api.IPFamily{api.IPv6Protocol},
		svc: svctest.MakeService("foo", svctest.SetTypeLoadBalancer,
			svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal)),
		update:          svctest.MakeService("foo", svctest.SetTypeExternalName),
		verifyDryAllocs: false, // 确认这些取值未被真正释放。
	}, {
		name:            "dualstack:v4v6_NoAllocs-Allocs",
		clusterFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
		svc:             svctest.MakeService("foo", svctest.SetTypeExternalName),
		update: svctest.MakeService("foo", svctest.SetTypeLoadBalancer,
			svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal)),
		verifyDryAllocs: true, // 确认这些取值未被真正分配。
	}, {
		name:            "dualstack:v4v6_Allocs-NoAllocs",
		clusterFamilies: []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol},
		svc: svctest.MakeService("foo", svctest.SetTypeLoadBalancer,
			svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal)),
		update:          svctest.MakeService("foo", svctest.SetTypeExternalName),
		verifyDryAllocs: false, // 确认这些取值未被真正释放。
	}, {
		name:            "dualstack:v6v4_NoAllocs-Allocs",
		clusterFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
		svc:             svctest.MakeService("foo", svctest.SetTypeExternalName),
		update: svctest.MakeService("foo", svctest.SetTypeLoadBalancer,
			svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal)),
		verifyDryAllocs: true, // 确认这些取值未被真正分配。
	}, {
		name:            "dualstack:v6v4_Allocs-NoAllocs",
		clusterFamilies: []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol},
		svc: svctest.MakeService("foo", svctest.SetTypeLoadBalancer,
			svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal)),
		update:          svctest.MakeService("foo", svctest.SetTypeExternalName),
		verifyDryAllocs: false, // 确认这些取值未被真正释放。
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			storage, _, cleanup := newStorage(t, tc.clusterFamilies)
			defer cleanup()

			ctx := genericregistrytest.NewNamespaceScopeContext(storage.Store, metav1.NamespaceDefault)
			obj, err := storage.Create(ctx, tc.svc, rest.ValidateAllObjectFunc, &metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("unexpected error creating service: %v", err)
			}
			createdSvc := obj.(*api.Service)

			if tc.verifyDryAllocs {
				// dry alloc 意味着创建时不做真实分配，确认这些取值
				// 并未真正被分配。
				proveClusterIPsDeallocated(t, storage, nil, createdSvc)
			} else {
				// 确认 IP 已被赋值
				proveClusterIPsAllocated(t, storage, nil, createdSvc)
			}

			// 将对象更新为新状态并检查结果。
			obj, _, err = storage.Update(ctx, tc.update.Name,
				rest.DefaultUpdatedObjectInfo(tc.update), rest.ValidateAllObjectFunc,
				rest.ValidateAllObjectUpdateFunc, false, &metav1.UpdateOptions{DryRun: []string{metav1.DryRunAll}})
			if err != nil {
				t.Fatalf("unexpected error updating service: %v", err)
			}
			updatedSvc := obj.(*api.Service)

			if tc.verifyDryAllocs {
				// dry alloc 表示取值已赋上但尚未
				// 真正分配。
				if netutils.ParseIPSloppy(updatedSvc.Spec.ClusterIP) == nil {
					t.Errorf("expected valid clusterIP: %q", updatedSvc.Spec.ClusterIP)
				}
				for _, ip := range updatedSvc.Spec.ClusterIPs {
					if netutils.ParseIPSloppy(ip) == nil {
						t.Errorf("expected valid clusterIP: %q", updatedSvc.Spec.ClusterIP)
					}
				}
				for i, fam := range updatedSvc.Spec.IPFamilies {
					if ipIsAllocated(t, storage.alloc.serviceIPAllocatorsByFamily[fam], updatedSvc.Spec.ClusterIPs[i]) {
						t.Errorf("expected IP to not be allocated: %q", updatedSvc.Spec.ClusterIPs[i])
					}
				}

				for _, p := range updatedSvc.Spec.Ports {
					if p.NodePort == 0 {
						t.Errorf("expected nodePort to be assigned: %d", p.NodePort)
					}
					if portIsAllocated(t, storage.alloc.serviceNodePorts, p.NodePort) {
						t.Errorf("expected nodePort to not be allocated: %d", p.NodePort)
					}
				}

				if updatedSvc.Spec.HealthCheckNodePort == 0 {
					t.Errorf("expected HCNP to be assigned: %d", updatedSvc.Spec.HealthCheckNodePort)
				}
				if portIsAllocated(t, storage.alloc.serviceNodePorts, updatedSvc.Spec.HealthCheckNodePort) {
					t.Errorf("expected HCNP to not be allocated: %d", updatedSvc.Spec.HealthCheckNodePort)
				}
			} else {
				// 确认 IP 已被清空但未被释放。
				if updatedSvc.Spec.ClusterIP != "" {
					t.Errorf("expected clusterIP to be unset: %q", updatedSvc.Spec.ClusterIP)
				}
				if len(updatedSvc.Spec.ClusterIPs) != 0 {
					t.Errorf("expected clusterIPs to be unset: %q", updatedSvc.Spec.ClusterIPs)
				}
				for i, fam := range createdSvc.Spec.IPFamilies {
					if !ipIsAllocated(t, storage.alloc.serviceIPAllocatorsByFamily[fam], createdSvc.Spec.ClusterIPs[i]) {
						t.Errorf("expected IP to still be allocated: %q", createdSvc.Spec.ClusterIPs[i])
					}
				}

				for _, p := range updatedSvc.Spec.Ports {
					if p.NodePort != 0 {
						t.Errorf("expected nodePort to be unset: %d", p.NodePort)
					}
				}
				for _, p := range createdSvc.Spec.Ports {
					if !portIsAllocated(t, storage.alloc.serviceNodePorts, p.NodePort) {
						t.Errorf("expected nodePort to still be allocated: %d", p.NodePort)
					}
				}

				if updatedSvc.Spec.HealthCheckNodePort != 0 {
					t.Errorf("expected HCNP to be unset: %d", updatedSvc.Spec.HealthCheckNodePort)
				}
				if !portIsAllocated(t, storage.alloc.serviceNodePorts, createdSvc.Spec.HealthCheckNodePort) {
					t.Errorf("expected HCNP to still be allocated: %d", createdSvc.Spec.HealthCheckNodePort)
				}
			}
		})
	}
}

func TestUpdatePatchAllocatedValues(t *testing.T) {
	prove := func(proofs ...svcTestProof) []svcTestProof {
		return proofs
	}
	proveClusterIP := func(idx int, ip string) svcTestProof {
		return func(t *testing.T, storage *wrapperRESTForTests, before, after *api.Service) {
			if want, got := ip, after.Spec.ClusterIPs[idx]; want != got {
				t.Errorf("wrong ClusterIPs[%d]: want %q, got %q", idx, want, got)
			}
		}
	}
	proveNodePort := func(idx int, port int32) svcTestProof {
		return func(t *testing.T, storage *wrapperRESTForTests, before, after *api.Service) {
			got := after.Spec.Ports[idx].NodePort
			if port > 0 && got != port {
				t.Errorf("wrong Ports[%d].NodePort: want %d, got %d", idx, port, got)
			} else if port < 0 && got == -port {
				t.Errorf("wrong Ports[%d].NodePort: wanted anything but %d", idx, got)
			}
		}
	}
	proveHCNP := func(port int32) svcTestProof {
		return func(t *testing.T, storage *wrapperRESTForTests, before, after *api.Service) {
			got := after.Spec.HealthCheckNodePort
			if port > 0 && got != port {
				t.Errorf("wrong HealthCheckNodePort: want %d, got %d", port, got)
			} else if port < 0 && got == -port {
				t.Errorf("wrong HealthCheckNodePort: wanted anything but %d", got)
			}
		}
	}

	// 每次 create 都需要已分配的 clusterIP、NodePort 和 HealthCheckNodePort
	// 每次 update 都需要空白的 clusterIP、NodePort 和/或 HealthCheckNodePort
	testCases := []cudTestCase{{
		name: "single-ip_single-port",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
				svctest.SetClusterIPs("10.0.0.1"),
				svctest.SetNodePorts(30093),
				svctest.SetHealthCheckNodePort(30118)),
			expectClusterIPs:          true,
			expectNodePorts:           true,
			expectHealthCheckNodePort: true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal)),
			expectClusterIPs:          true,
			expectNodePorts:           true,
			expectHealthCheckNodePort: true,
			prove: prove(
				proveClusterIP(0, "10.0.0.1"),
				proveNodePort(0, 30093),
				proveHCNP(30118)),
		},
	}, {
		name: "multi-ip_multi-port",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1"),
				svctest.SetPorts(
					svctest.MakeServicePort("p", 867, intstr.FromInt32(867), api.ProtocolTCP),
					svctest.MakeServicePort("q", 5309, intstr.FromInt32(5309), api.ProtocolTCP)),
				svctest.SetNodePorts(30093, 30076),
				svctest.SetHealthCheckNodePort(30118)),
			expectClusterIPs:          true,
			expectNodePorts:           true,
			expectHealthCheckNodePort: true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
				svctest.SetPorts(
					svctest.MakeServicePort("p", 867, intstr.FromInt32(867), api.ProtocolTCP),
					svctest.MakeServicePort("q", 5309, intstr.FromInt32(5309), api.ProtocolTCP))),
			expectClusterIPs:          true,
			expectNodePorts:           true,
			expectHealthCheckNodePort: true,
			prove: prove(
				proveClusterIP(0, "10.0.0.1"),
				proveClusterIP(1, "2000::1"),
				proveNodePort(0, 30093),
				proveNodePort(1, 30076),
				proveHCNP(30118)),
		},
	}, {
		name: "multi-ip_partial",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1"),
				svctest.SetPorts(
					svctest.MakeServicePort("p", 867, intstr.FromInt32(867), api.ProtocolTCP),
					svctest.MakeServicePort("q", 5309, intstr.FromInt32(5309), api.ProtocolTCP)),
				svctest.SetNodePorts(30093, 30076),
				svctest.SetHealthCheckNodePort(30118)),
			expectClusterIPs:          true,
			expectNodePorts:           true,
			expectHealthCheckNodePort: true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
				svctest.SetClusterIPs("10.0.0.1")),
			expectError: true,
		},
	}, {
		name: "multi-port_partial",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
				svctest.SetPorts(
					svctest.MakeServicePort("p", 867, intstr.FromInt32(867), api.ProtocolTCP),
					svctest.MakeServicePort("q", 5309, intstr.FromInt32(5309), api.ProtocolTCP)),
				svctest.SetNodePorts(30093, 30076),
				svctest.SetHealthCheckNodePort(30118)),
			expectClusterIPs:          true,
			expectNodePorts:           true,
			expectHealthCheckNodePort: true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
				svctest.SetPorts(
					svctest.MakeServicePort("p", 867, intstr.FromInt32(867), api.ProtocolTCP),
					svctest.MakeServicePort("q", 5309, intstr.FromInt32(5309), api.ProtocolTCP)),
				svctest.SetNodePorts(30093, 0)), // 只提供 1 个取值
			expectClusterIPs:          true,
			expectNodePorts:           true,
			expectHealthCheckNodePort: true,
			prove: prove(
				proveNodePort(0, 30093),
				proveNodePort(1, 30076),
				proveHCNP(30118)),
		},
	}, {
		name: "swap-ports",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
				svctest.SetPorts(
					svctest.MakeServicePort("p", 867, intstr.FromInt32(867), api.ProtocolTCP),
					svctest.MakeServicePort("q", 5309, intstr.FromInt32(5309), api.ProtocolTCP)),
				svctest.SetNodePorts(30093, 30076),
				svctest.SetHealthCheckNodePort(30118)),
			expectClusterIPs:          true,
			expectNodePorts:           true,
			expectHealthCheckNodePort: true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
				svctest.SetPorts(
					// 与上面的取值互换
					svctest.MakeServicePort("q", 5309, intstr.FromInt32(5309), api.ProtocolTCP),
					svctest.MakeServicePort("p", 867, intstr.FromInt32(867), api.ProtocolTCP))),
			expectClusterIPs:          true,
			expectNodePorts:           true,
			expectHealthCheckNodePort: true,
			prove: prove(
				proveNodePort(0, 30076),
				proveNodePort(1, 30093),
				proveHCNP(30118)),
		},
	}, {
		name: "partial-swap-ports",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
				svctest.SetPorts(
					svctest.MakeServicePort("p", 867, intstr.FromInt32(867), api.ProtocolTCP),
					svctest.MakeServicePort("q", 5309, intstr.FromInt32(5309), api.ProtocolTCP)),
				svctest.SetNodePorts(30093, 30076),
				svctest.SetHealthCheckNodePort(30118)),
			expectClusterIPs:          true,
			expectNodePorts:           true,
			expectHealthCheckNodePort: true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
				svctest.SetPorts(
					svctest.MakeServicePort("p", 867, intstr.FromInt32(867), api.ProtocolTCP),
					svctest.MakeServicePort("q", 5309, intstr.FromInt32(5309), api.ProtocolTCP)),
				svctest.SetNodePorts(30076, 0), // 把 [0] 设为 [1] 的取值，并省略 [1]
				svctest.SetHealthCheckNodePort(30118)),
			expectClusterIPs:          true,
			expectNodePorts:           true,
			expectHealthCheckNodePort: true,
			prove: prove(
				proveNodePort(0, 30076),
				proveNodePort(1, -30076),
				proveHCNP(30118)),
		},
	}, {
		name: "swap-port-with-hcnp",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
				svctest.SetPorts(
					svctest.MakeServicePort("p", 867, intstr.FromInt32(867), api.ProtocolTCP),
					svctest.MakeServicePort("q", 5309, intstr.FromInt32(5309), api.ProtocolTCP)),
				svctest.SetNodePorts(30093, 30076),
				svctest.SetHealthCheckNodePort(30118)),
			expectClusterIPs:          true,
			expectNodePorts:           true,
			expectHealthCheckNodePort: true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
				svctest.SetPorts(
					svctest.MakeServicePort("p", 867, intstr.FromInt32(867), api.ProtocolTCP),
					svctest.MakeServicePort("q", 5309, intstr.FromInt32(5309), api.ProtocolTCP)),
				svctest.SetNodePorts(30076, 30118)), // 把 [0] 设为 HCNP 的取值
			expectError: true,
		},
	}, {
		name: "partial-swap-port-with-hcnp",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
				svctest.SetPorts(
					svctest.MakeServicePort("p", 867, intstr.FromInt32(867), api.ProtocolTCP),
					svctest.MakeServicePort("q", 5309, intstr.FromInt32(5309), api.ProtocolTCP)),
				svctest.SetNodePorts(30093, 30076),
				svctest.SetHealthCheckNodePort(30118)),
			expectClusterIPs:          true,
			expectNodePorts:           true,
			expectHealthCheckNodePort: true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
				svctest.SetPorts(
					svctest.MakeServicePort("p", 867, intstr.FromInt32(867), api.ProtocolTCP),
					svctest.MakeServicePort("q", 5309, intstr.FromInt32(5309), api.ProtocolTCP)),
				svctest.SetNodePorts(30118, 0)), // 把 [0] 设为 HCNP 的取值，并省略 [1]
			expectError: true,
		},
	}, {
		name: "update-hcnp",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
				svctest.SetPorts(
					svctest.MakeServicePort("p", 867, intstr.FromInt32(867), api.ProtocolTCP),
					svctest.MakeServicePort("q", 5309, intstr.FromInt32(5309), api.ProtocolTCP)),
				svctest.SetNodePorts(30093, 30076),
				svctest.SetHealthCheckNodePort(30118)),
			expectClusterIPs:          true,
			expectNodePorts:           true,
			expectHealthCheckNodePort: true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
				svctest.SetPorts(
					svctest.MakeServicePort("p", 867, intstr.FromInt32(867), api.ProtocolTCP),
					svctest.MakeServicePort("q", 5309, intstr.FromInt32(5309), api.ProtocolTCP)),
				svctest.SetNodePorts(30093, 30076),
				svctest.SetHealthCheckNodePort(30111)),
			expectError: true,
		},
	}}

	helpTestCreateUpdateDelete(t, testCases)
}

// 验证从单栈发起的更新可以正常工作。
func TestUpdateIPsFromSingleStack(t *testing.T) {
	prove := func(proofs ...svcTestProof) []svcTestProof {
		return proofs
	}
	proveNumFamilies := func(n int) svcTestProof {
		return func(t *testing.T, storage *wrapperRESTForTests, before, after *api.Service) {
			t.Helper()
			if got := len(after.Spec.IPFamilies); got != n {
				t.Errorf("wrong number of ipFamilies: expected %d, got %d", n, got)
			}
		}
	}

	// 以单栈用例作为对照。
	testCasesV4 := []cudTestCase{{
		name: "single-single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetSelector(map[string]string{"k2": "v2"})),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
	}, {
		name: "single-dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
			expectError: true,
		},
	}, {
		name: "single-dual_policy",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
			expectError: true,
		},
	}, {
		name: "single-dual_families",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
			expectError: true,
		},
	}}

	t.Run("singlestack:v4", func(t *testing.T) {
		helpTestCreateUpdateDeleteWithFamilies(t, testCasesV4, []api.IPFamily{api.IPv4Protocol})
	})

	// 双栈 v4,v6 用例：覆盖如下完整组合：
	//    policy={nil, single, prefer, require}
	//    families={nil, single, dual}
	//    ips={nil, single, dual}
	testCasesV4V6 := []cudTestCase{{
		name: "policy:nil_families:nil_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"})),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:nil_families:nil_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:nil_families:nil_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectError: true,
		},
	}, {
		name: "policy:nil_families:single_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilies(api.IPv4Protocol)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:nil_families:single_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilies(api.IPv4Protocol),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:nil_families:single_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilies(api.IPv4Protocol),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectError: true,
		},
	}, {
		name: "policy:nil_families:dual_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
			expectError: true,
		},
	}, {
		name: "policy:nil_families:dual_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol),
				svctest.SetClusterIPs("10.0.0.1")),
			expectError: true,
		},
	}, {
		name: "policy:nil_families:dual_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectError: true,
		},
	}, {
		name: "policy:single_families:nil_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:single_families:nil_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:single_families:nil_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectError: true,
		},
	}, {
		name: "policy:single_families:single_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetIPFamilies(api.IPv4Protocol)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:single_families:single_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetIPFamilies(api.IPv4Protocol),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:single_families:single_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetIPFamilies(api.IPv4Protocol),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectError: true,
		},
	}, {
		name: "policy:single_families:dual_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
			expectError: true,
		},
	}, {
		name: "policy:single_families:dual_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol),
				svctest.SetClusterIPs("10.0.0.1")),
			expectError: true,
		},
	}, {
		name: "policy:single_families:dual_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectError: true,
		},
	}, {
		name: "policy:prefer_families:nil_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:prefer_families:nil_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:prefer_families:nil_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:prefer_families:single_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetIPFamilies(api.IPv4Protocol)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:prefer_families:single_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetIPFamilies(api.IPv4Protocol),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:prefer_families:single_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetIPFamilies(api.IPv4Protocol),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:prefer_families:dual_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:prefer_families:dual_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:prefer_families:dual_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:require_families:nil_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:require_families:nil_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:require_families:nil_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:require_families:single_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies(api.IPv4Protocol)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:require_families:single_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies(api.IPv4Protocol),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:require_families:single_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies(api.IPv4Protocol),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:require_families:dual_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:require_families:dual_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:require_families:dual_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "single-dual_wrong_order_families",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
			expectError: true,
		},
	}, {
		name: "single-dual_wrong_order_ips",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectError: true,
		},
	}, {
		name: "single-dual_ip_in_use",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		beforeUpdate: func(t *testing.T, storage *wrapperRESTForTests) {
			alloc := storage.alloc.serviceIPAllocatorsByFamily[api.IPv6Protocol]
			ip := "2000::1"
			if err := alloc.Allocate(netutils.ParseIPSloppy(ip)); err != nil {
				t.Fatalf("test is incorrect, unable to preallocate IP %q: %v", ip, err)
			}
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectError: true,
		},
	}}

	t.Run("dualstack:v4v6", func(t *testing.T) {
		helpTestCreateUpdateDeleteWithFamilies(t, testCasesV4V6, []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol})
	})

	// 双栈 v6,v4 用例：覆盖如下完整组合：
	//    policy={nil, single, prefer, require}
	//    families={nil, single, dual}
	//    ips={nil, single, dual}
	testCasesV6V4 := []cudTestCase{{
		name: "policy:nil_families:nil_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"})),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:nil_families:nil_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:nil_families:nil_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectError: true,
		},
	}, {
		name: "policy:nil_families:single_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilies(api.IPv6Protocol)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:nil_families:single_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilies(api.IPv6Protocol),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:nil_families:single_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilies(api.IPv6Protocol),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectError: true,
		},
	}, {
		name: "policy:nil_families:dual_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
			expectError: true,
		},
	}, {
		name: "policy:nil_families:dual_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol),
				svctest.SetClusterIPs("2000::1")),
			expectError: true,
		},
	}, {
		name: "policy:nil_families:dual_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectError: true,
		},
	}, {
		name: "policy:single_families:nil_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:single_families:nil_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:single_families:nil_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectError: true,
		},
	}, {
		name: "policy:single_families:single_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetIPFamilies(api.IPv6Protocol)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:single_families:single_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetIPFamilies(api.IPv6Protocol),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:single_families:single_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetIPFamilies(api.IPv6Protocol),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectError: true,
		},
	}, {
		name: "policy:single_families:dual_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
			expectError: true,
		},
	}, {
		name: "policy:single_families:dual_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol),
				svctest.SetClusterIPs("2000::1")),
			expectError: true,
		},
	}, {
		name: "policy:single_families:dual_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectError: true,
		},
	}, {
		name: "policy:prefer_families:nil_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:prefer_families:nil_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:prefer_families:nil_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:prefer_families:single_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetIPFamilies(api.IPv6Protocol)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:prefer_families:single_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetIPFamilies(api.IPv6Protocol),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:prefer_families:single_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetIPFamilies(api.IPv6Protocol),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:prefer_families:dual_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:prefer_families:dual_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:prefer_families:dual_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:require_families:nil_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:require_families:nil_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:require_families:nil_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:require_families:single_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies(api.IPv6Protocol)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:require_families:single_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies(api.IPv6Protocol),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:require_families:single_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies(api.IPv6Protocol),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:require_families:dual_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:require_families:dual_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:require_families:dual_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "single-dual_wrong_order_families",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
			expectError: true,
		},
	}, {
		name: "single-dual_wrong_order_ips",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectError: true,
		},
	}, {
		name: "single-dual_ip_in_use",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
		beforeUpdate: func(t *testing.T, storage *wrapperRESTForTests) {
			alloc := storage.alloc.serviceIPAllocatorsByFamily[api.IPv4Protocol]
			ip := "10.0.0.1"
			if err := alloc.Allocate(netutils.ParseIPSloppy(ip)); err != nil {
				t.Fatalf("test is incorrect, unable to preallocate IP %q: %v", ip, err)
			}
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectError: true,
		},
	}}

	t.Run("dualstack:v6v4", func(t *testing.T) {
		helpTestCreateUpdateDeleteWithFamilies(t, testCasesV6V4, []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol})
	})

	// Headless 用例：覆盖如下完整组合：
	//    policy={nil, single, prefer, require}
	//    families={nil, single, dual}
	testCasesHeadless := []cudTestCase{{
		name: "policy:nil_families:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs(api.ClusterIPNone)),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"})),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:nil_families:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs(api.ClusterIPNone)),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilies("IPv4")),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:nil_families:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs(api.ClusterIPNone)),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilies("IPv4", "IPv6")),
			expectError: true,
		},
	}, {
		name: "policy:single_families:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs(api.ClusterIPNone)),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:single_families:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs(api.ClusterIPNone)),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetIPFamilies("IPv4")),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:single_families:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs(api.ClusterIPNone)),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetIPFamilies("IPv4", "IPv6")),
			expectError: true,
		},
	}, {
		name: "policy:prefer_families:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs(api.ClusterIPNone)),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:prefer_families:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs(api.ClusterIPNone)),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetIPFamilies("IPv4")),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:prefer_families:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs(api.ClusterIPNone)),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetIPFamilies("IPv4", "IPv6")),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:require_families:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs(api.ClusterIPNone)),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:require_families:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs(api.ClusterIPNone)),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies("IPv4")),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:require_families:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs(api.ClusterIPNone)),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(1)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies("IPv4", "IPv6")),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(2)),
		},
	}}

	t.Run("headless", func(t *testing.T) {
		helpTestCreateUpdateDeleteWithFamilies(t, testCasesHeadless, []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol})
	})
}

// 验证来自双栈的更新。
func TestUpdateIPsFromDualStack(t *testing.T) {
	prove := func(proofs ...svcTestProof) []svcTestProof {
		return proofs
	}
	proveNumFamilies := func(n int) svcTestProof {
		return func(t *testing.T, storage *wrapperRESTForTests, before, after *api.Service) {
			t.Helper()
			if got := len(after.Spec.IPFamilies); got != n {
				t.Errorf("wrong number of ipFamilies: expected %d, got %d", n, got)
			}
		}
	}

	// 双栈 v4,v6 用例：覆盖如下完整组合：
	//    policy={nil, single, prefer, require}
	//    families={nil, single, dual}
	//    ips={nil, single, dual}
	testCasesV4V6 := []cudTestCase{{
		name: "policy:nil_families:nil_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"})),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:nil_families:nil_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetClusterIPs("10.0.0.1")),
			expectError: true,
		},
	}, {
		name: "policy:nil_families:nil_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:nil_families:single_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilies(api.IPv4Protocol)),
			expectError: true,
		},
	}, {
		name: "policy:nil_families:single_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilies(api.IPv4Protocol),
				svctest.SetClusterIPs("10.0.0.1")),
			expectError: true,
		},
	}, {
		name: "policy:nil_families:single_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilies(api.IPv4Protocol),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectError: true,
		},
	}, {
		name: "policy:nil_families:dual_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:nil_families:dual_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol),
				svctest.SetClusterIPs("10.0.0.1")),
			expectError: true,
		},
	}, {
		name: "policy:nil_families:dual_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:single_families:nil_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:single_families:nil_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:single_families:nil_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs:     true,
			expectStackDowngrade: true,
			prove:                prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:single_families:single_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetIPFamilies(api.IPv4Protocol)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:single_families:single_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetIPFamilies(api.IPv4Protocol),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:single_families:single_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetIPFamilies(api.IPv4Protocol),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs:     true,
			expectStackDowngrade: true,
			prove:                prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:single_families:dual_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
			expectClusterIPs:     true,
			expectStackDowngrade: true,
			prove:                prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:single_families:dual_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol),
				svctest.SetClusterIPs("10.0.0.1")),
			expectClusterIPs:     true,
			expectStackDowngrade: true,
			prove:                prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:single_families:dual_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs:     true,
			expectStackDowngrade: true,
			prove:                prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:prefer_families:nil_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:prefer_families:nil_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectError: true,
		},
	}, {
		name: "policy:prefer_families:nil_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:prefer_families:single_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetIPFamilies(api.IPv4Protocol)),
			expectError: true,
		},
	}, {
		name: "policy:prefer_families:single_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetIPFamilies(api.IPv4Protocol),
				svctest.SetClusterIPs("10.0.0.1")),
			expectError: true,
		},
	}, {
		name: "policy:prefer_families:single_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetIPFamilies(api.IPv4Protocol),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectError: true,
		},
	}, {
		name: "policy:prefer_families:dual_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:prefer_families:dual_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol),
				svctest.SetClusterIPs("10.0.0.1")),
			expectError: true,
		},
	}, {
		name: "policy:prefer_families:dual_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:require_families:nil_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:require_families:nil_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectError: true,
		},
	}, {
		name: "policy:require_families:nil_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:require_families:single_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies(api.IPv4Protocol)),
			expectError: true,
		},
	}, {
		name: "policy:require_families:single_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies(api.IPv4Protocol),
				svctest.SetClusterIPs("10.0.0.1")),
			expectError: true,
		},
	}, {
		name: "policy:require_families:single_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies(api.IPv4Protocol),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectError: true,
		},
	}, {
		name: "policy:require_families:dual_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:require_families:dual_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol),
				svctest.SetClusterIPs("10.0.0.1")),
			expectError: true,
		},
	}, {
		name: "policy:require_families:dual_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies(api.IPv4Protocol, api.IPv6Protocol),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "dual-single_wrong_order_families",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetIPFamilies(api.IPv6Protocol)),
			expectError: true,
		},
	}, {
		name: "dual-single_wrong_order_ips",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("10.0.0.1", "2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectError: true,
		},
	}}

	t.Run("dualstack:v4v6", func(t *testing.T) {
		helpTestCreateUpdateDeleteWithFamilies(t, testCasesV4V6, []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol})
	})

	// 双栈 v6,v4 用例：覆盖如下完整组合：
	//    policy={nil, single, prefer, require}
	//    families={nil, single, dual}
	//    ips={nil, single, dual}
	testCasesV6V4 := []cudTestCase{{
		name: "policy:nil_families:nil_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"})),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:nil_families:nil_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetClusterIPs("2000::1")),
			expectError: true,
		},
	}, {
		name: "policy:nil_families:nil_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:nil_families:single_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilies(api.IPv6Protocol)),
			expectError: true,
		},
	}, {
		name: "policy:nil_families:single_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilies(api.IPv6Protocol),
				svctest.SetClusterIPs("2000::1")),
			expectError: true,
		},
	}, {
		name: "policy:nil_families:single_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilies(api.IPv6Protocol),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectError: true,
		},
	}, {
		name: "policy:nil_families:dual_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:nil_families:dual_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol),
				svctest.SetClusterIPs("2000::1")),
			expectError: true,
		},
	}, {
		name: "policy:nil_families:dual_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:single_families:nil_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:single_families:nil_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:single_families:nil_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs:     true,
			expectStackDowngrade: true,
			prove:                prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:single_families:single_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetIPFamilies(api.IPv6Protocol)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:single_families:single_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetIPFamilies(api.IPv6Protocol),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:single_families:single_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetIPFamilies(api.IPv6Protocol),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs:     true,
			expectStackDowngrade: true,
			prove:                prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:single_families:dual_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
			expectClusterIPs:     true,
			expectStackDowngrade: true,
			prove:                prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:single_families:dual_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol),
				svctest.SetClusterIPs("2000::1")),
			expectClusterIPs:     true,
			expectStackDowngrade: true,
			prove:                prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:single_families:dual_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs:     true,
			expectStackDowngrade: true,
			prove:                prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:prefer_families:nil_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:prefer_families:nil_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetClusterIPs("2000::1")),
			expectError: true,
		},
	}, {
		name: "policy:prefer_families:nil_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:prefer_families:single_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetIPFamilies(api.IPv6Protocol)),
			expectError: true,
		},
	}, {
		name: "policy:prefer_families:single_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetIPFamilies(api.IPv6Protocol),
				svctest.SetClusterIPs("2000::1")),
			expectError: true,
		},
	}, {
		name: "policy:prefer_families:single_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetIPFamilies(api.IPv6Protocol),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectError: true,
		},
	}, {
		name: "policy:prefer_families:dual_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:prefer_families:dual_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol),
				svctest.SetClusterIPs("2000::1")),
			expectError: true,
		},
	}, {
		name: "policy:prefer_families:dual_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:require_families:nil_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:require_families:nil_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1")),
			expectError: true,
		},
	}, {
		name: "policy:require_families:nil_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:require_families:single_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies(api.IPv6Protocol)),
			expectError: true,
		},
	}, {
		name: "policy:require_families:single_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies(api.IPv6Protocol),
				svctest.SetClusterIPs("2000::1")),
			expectError: true,
		},
	}, {
		name: "policy:require_families:single_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies(api.IPv6Protocol),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectError: true,
		},
	}, {
		name: "policy:require_families:dual_ips:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol)),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:require_families:dual_ips:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol),
				svctest.SetClusterIPs("2000::1")),
			expectError: true,
		},
	}, {
		name: "policy:require_families:dual_ips:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies(api.IPv6Protocol, api.IPv4Protocol),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
	}, {
		name: "dual-single_wrong_order_families",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetIPFamilies(api.IPv4Protocol)),
			expectError: true,
		},
	}, {
		name: "dual-single_wrong_order_ips",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs("2000::1", "10.0.0.1")),
			expectClusterIPs: true,
			prove:            prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetClusterIPs("10.0.0.1")),
			expectError: true,
		},
	}}

	t.Run("dualstack:v6v4", func(t *testing.T) {
		helpTestCreateUpdateDeleteWithFamilies(t, testCasesV6V4, []api.IPFamily{api.IPv6Protocol, api.IPv4Protocol})
	})

	// Headless 用例：覆盖如下完整组合：
	//    policy={nil, single, prefer, require}
	//    families={nil, single, dual}
	testCasesHeadless := []cudTestCase{{
		name: "policy:nil_families:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs(api.ClusterIPNone)),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"})),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:nil_families:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs(api.ClusterIPNone)),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilies("IPv4")),
			expectError: true,
		},
	}, {
		name: "policy:nil_families:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs(api.ClusterIPNone)),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilies("IPv4", "IPv6")),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:single_families:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs(api.ClusterIPNone)),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack)),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:single_families:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs(api.ClusterIPNone)),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetIPFamilies("IPv4")),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:single_families:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs(api.ClusterIPNone)),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicySingleStack),
				svctest.SetIPFamilies("IPv4", "IPv6")),
			expectHeadless:       true,
			expectStackDowngrade: true,
			prove:                prove(proveNumFamilies(1)),
		},
	}, {
		name: "policy:prefer_families:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs(api.ClusterIPNone)),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack)),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:prefer_families:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs(api.ClusterIPNone)),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetIPFamilies("IPv4")),
			expectError: true,
		},
	}, {
		name: "policy:prefer_families:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs(api.ClusterIPNone)),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetIPFamilies("IPv4", "IPv6")),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:require_families:nil",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs(api.ClusterIPNone)),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack)),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(2)),
		},
	}, {
		name: "policy:require_families:single",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs(api.ClusterIPNone)),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies("IPv4")),
			expectError: true,
		},
	}, {
		name: "policy:require_families:dual",
		line: line(),
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetClusterIPs(api.ClusterIPNone)),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(2)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSelector(map[string]string{"k2": "v2"}),
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyRequireDualStack),
				svctest.SetIPFamilies("IPv4", "IPv6")),
			expectHeadless: true,
			prove:          prove(proveNumFamilies(2)),
		},
	}}

	t.Run("headless", func(t *testing.T) {
		helpTestCreateUpdateDeleteWithFamilies(t, testCasesHeadless, []api.IPFamily{api.IPv4Protocol, api.IPv6Protocol})
	})
}

func TestFeatureExternalName(t *testing.T) {
	testCases := []cudTestCase{{
		name: "valid-valid",
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeExternalName),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeExternalName, svctest.SetExternalName("updated.example.com")),
		},
	}, {
		name: "valid-blank",
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeExternalName),
		},
		update: svcTestCase{
			svc:         svctest.MakeService("foo", svctest.SetTypeExternalName, svctest.SetExternalName("")),
			expectError: true,
		},
	}}

	helpTestCreateUpdateDelete(t, testCases)
}

func TestFeatureSelector(t *testing.T) {
	testCases := []cudTestCase{{
		name: "valid-valid",
		create: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeClusterIP),
			expectClusterIPs: true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				func(s *api.Service) {
					s.Spec.Selector = map[string]string{"updated": "value"}
				}),
			expectClusterIPs: true,
		},
	}, {
		name: "valid-nil",
		create: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeClusterIP),
			expectClusterIPs: true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				func(s *api.Service) {
					s.Spec.Selector = nil
				}),
			expectClusterIPs: true,
		},
	}, {
		name: "valid-empty",
		create: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeClusterIP),
			expectClusterIPs: true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				func(s *api.Service) {
					s.Spec.Selector = map[string]string{}
				}),
			expectClusterIPs: true,
		},
	}, {
		name: "nil-valid",
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				func(s *api.Service) {
					s.Spec.Selector = nil
				}),
			expectClusterIPs: true,
		},
		update: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeClusterIP),
			expectClusterIPs: true,
		},
	}, {
		name: "empty-valid",
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				func(s *api.Service) {
					s.Spec.Selector = map[string]string{}
				}),
			expectClusterIPs: true,
		},
		update: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeClusterIP),
			expectClusterIPs: true,
		},
	}}

	helpTestCreateUpdateDelete(t, testCases)
}

func TestFeatureClusterIPs(t *testing.T) {
	testCases := []cudTestCase{{
		name: "clusterIP:valid-headless",
		create: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeClusterIP),
			expectClusterIPs: true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetHeadless),
			expectError: true,
		},
	}, {
		name: "clusterIP:headless-valid",
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetHeadless),
			expectHeadless: true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetClusterIP("10.0.0.93")),
			expectError: true,
		},
	}, {
		name: "clusterIP:valid-valid",
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetClusterIP("10.0.0.93")),
			expectClusterIPs: true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetClusterIP("10.0.0.76")),
			expectError: true,
		},
	}, {
		name: "clusterIPs:valid-valid",
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetClusterIPs("10.0.0.93", "2000::93")),
			expectClusterIPs: true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetIPFamilyPolicy(api.IPFamilyPolicyPreferDualStack),
				svctest.SetClusterIPs("10.0.0.76", "2000::76")),
			expectError: true,
		},
	}}

	helpTestCreateUpdateDelete(t, testCases)
}

func TestFeaturePorts(t *testing.T) {
	testCases := []cudTestCase{{
		name: "add_port",
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetPorts(
					svctest.MakeServicePort("p", 80, intstr.FromInt32(80), api.ProtocolTCP))),
			expectClusterIPs: true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetPorts(
					svctest.MakeServicePort("p", 80, intstr.FromInt32(80), api.ProtocolTCP),
					svctest.MakeServicePort("q", 443, intstr.FromInt32(443), api.ProtocolTCP))),
			expectClusterIPs: true,
		},
	}, {
		name: "add_port_ClusterIP-NodePort",
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetPorts(
					svctest.MakeServicePort("p", 80, intstr.FromInt32(80), api.ProtocolTCP))),
			expectClusterIPs: true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeNodePort,
				svctest.SetPorts(
					svctest.MakeServicePort("p", 80, intstr.FromInt32(80), api.ProtocolTCP),
					svctest.MakeServicePort("q", 443, intstr.FromInt32(443), api.ProtocolTCP))),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
	}, {
		name: "add_port_NodePort-ClusterIP",
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeNodePort,
				svctest.SetPorts(
					svctest.MakeServicePort("p", 80, intstr.FromInt32(80), api.ProtocolTCP))),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetPorts(
					svctest.MakeServicePort("p", 80, intstr.FromInt32(80), api.ProtocolTCP),
					svctest.MakeServicePort("q", 443, intstr.FromInt32(443), api.ProtocolTCP))),
			expectClusterIPs: true,
		},
	}, {
		name: "remove_port",
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetPorts(
					svctest.MakeServicePort("p", 80, intstr.FromInt32(80), api.ProtocolTCP),
					svctest.MakeServicePort("q", 443, intstr.FromInt32(443), api.ProtocolTCP))),
			expectClusterIPs: true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetPorts(
					svctest.MakeServicePort("p", 80, intstr.FromInt32(80), api.ProtocolTCP))),
			expectClusterIPs: true,
		},
	}, {
		name: "remove_port_ClusterIP-NodePort",
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetPorts(
					svctest.MakeServicePort("p", 80, intstr.FromInt32(80), api.ProtocolTCP),
					svctest.MakeServicePort("q", 443, intstr.FromInt32(443), api.ProtocolTCP))),
			expectClusterIPs: true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeNodePort,
				svctest.SetPorts(
					svctest.MakeServicePort("p", 80, intstr.FromInt32(80), api.ProtocolTCP))),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
	}, {
		name: "remove_port_NodePort-ClusterIP",
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeNodePort,
				svctest.SetPorts(
					svctest.MakeServicePort("p", 80, intstr.FromInt32(80), api.ProtocolTCP),
					svctest.MakeServicePort("q", 443, intstr.FromInt32(443), api.ProtocolTCP))),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetPorts(
					svctest.MakeServicePort("p", 80, intstr.FromInt32(80), api.ProtocolTCP))),
			expectClusterIPs: true,
		},
	}, {
		name: "swap_ports",
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeNodePort,
				svctest.SetPorts(
					svctest.MakeServicePort("p", 80, intstr.FromInt32(80), api.ProtocolTCP),
					svctest.MakeServicePort("q", 443, intstr.FromInt32(443), api.ProtocolTCP))),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeNodePort,
				svctest.SetPorts(
					svctest.MakeServicePort("q", 443, intstr.FromInt32(443), api.ProtocolTCP),
					svctest.MakeServicePort("p", 80, intstr.FromInt32(80), api.ProtocolTCP))),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
	}, {
		name: "modify_ports",
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeNodePort,
				svctest.SetPorts(
					svctest.MakeServicePort("p", 80, intstr.FromInt32(80), api.ProtocolTCP),
					svctest.MakeServicePort("q", 443, intstr.FromInt32(443), api.ProtocolTCP))),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeNodePort,
				svctest.SetPorts(
					svctest.MakeServicePort("p", 8080, intstr.FromInt32(8080), api.ProtocolTCP),
					svctest.MakeServicePort("q", 8443, intstr.FromInt32(8443), api.ProtocolTCP))),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
	}, {
		name: "modify_protos",
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeNodePort,
				svctest.SetPorts(
					svctest.MakeServicePort("p", 80, intstr.FromInt32(80), api.ProtocolTCP),
					svctest.MakeServicePort("q", 443, intstr.FromInt32(443), api.ProtocolTCP))),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeNodePort,
				svctest.SetPorts(
					svctest.MakeServicePort("p", 80, intstr.FromInt32(80), api.ProtocolUDP),
					svctest.MakeServicePort("q", 443, intstr.FromInt32(443), api.ProtocolUDP))),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
	}, {
		name: "modify_ports_and_protos",
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeNodePort,
				svctest.SetPorts(
					svctest.MakeServicePort("p", 80, intstr.FromInt32(80), api.ProtocolTCP),
					svctest.MakeServicePort("q", 443, intstr.FromInt32(443), api.ProtocolTCP))),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeNodePort,
				svctest.SetPorts(
					svctest.MakeServicePort("r", 53, intstr.FromInt32(53), api.ProtocolTCP),
					svctest.MakeServicePort("s", 53, intstr.FromInt32(53), api.ProtocolUDP))),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
	}, {
		name: "add_alt_proto",
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeNodePort,
				svctest.SetPorts(
					svctest.MakeServicePort("p", 53, intstr.FromInt32(53), api.ProtocolTCP))),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeNodePort,
				svctest.SetPorts(
					svctest.MakeServicePort("p", 53, intstr.FromInt32(53), api.ProtocolTCP),
					svctest.MakeServicePort("q", 53, intstr.FromInt32(53), api.ProtocolUDP))),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
	}, {
		name: "wipe_all",
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeNodePort,
				svctest.SetPorts(
					svctest.MakeServicePort("p", 80, intstr.FromInt32(80), api.ProtocolTCP),
					svctest.MakeServicePort("q", 443, intstr.FromInt32(443), api.ProtocolTCP))),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeNodePort,
				svctest.SetPorts()),
			expectError:     true,
			expectNodePorts: true,
		},
	}}

	helpTestCreateUpdateDelete(t, testCases)
}

func TestFeatureSessionAffinity(t *testing.T) {
	testCases := []cudTestCase{{
		name: "None-ClientIPNoConfig",
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSessionAffinity(api.ServiceAffinityNone)),
			expectClusterIPs: true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				func(s *api.Service) {
					// 不设置 config 就写入该字段
					s.Spec.SessionAffinity = api.ServiceAffinityClientIP
				}),
			expectError: true,
		},
	}, {
		name: "None-ClientIP",
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSessionAffinity(api.ServiceAffinityNone)),
			expectClusterIPs: true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSessionAffinity(api.ServiceAffinityClientIP)),
			expectClusterIPs: true,
		},
	}, {
		name: "ClientIP-NoneWithConfig",
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSessionAffinity(api.ServiceAffinityClientIP)),
			expectClusterIPs: true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSessionAffinity(api.ServiceAffinityClientIP),
				func(s *api.Service) {
					// 写入该字段但不清空 config
					s.Spec.SessionAffinity = api.ServiceAffinityNone
				}),
			expectError: true,
		},
	}, {
		name: "ClientIP-None",
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSessionAffinity(api.ServiceAffinityClientIP)),
			expectClusterIPs: true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeClusterIP,
				svctest.SetSessionAffinity(api.ServiceAffinityNone),
				func(s *api.Service) {
					s.Spec.SessionAffinityConfig = nil
				}),
			expectClusterIPs: true,
		},
	}}

	helpTestCreateUpdateDelete(t, testCases)
}

func TestFeatureType(t *testing.T) {
	testCases := []cudTestCase{{
		name: "ExternalName-ClusterIP",
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeExternalName),
		},
		update: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeClusterIP),
			expectClusterIPs: true,
		},
	}, {
		name: "ClusterIP-ExternalName",
		create: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeClusterIP),
			expectClusterIPs: true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeExternalName),
		},
	}, {
		name: "ExternalName-NodePort",
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeExternalName),
		},
		update: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeNodePort),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
	}, {
		name: "NodePort-ExternalName",
		create: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeNodePort),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeExternalName),
		},
	}, {
		name: "ExternalName-LoadBalancer",
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeExternalName),
		},
		update: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeLoadBalancer),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
	}, {
		name: "LoadBalancer-ExternalName",
		create: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeLoadBalancer),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeExternalName),
		},
	}, {
		name: "ClusterIP-NodePort",
		create: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeClusterIP),
			expectClusterIPs: true,
		},
		update: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeNodePort),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
	}, {
		name: "NodePort-ClusterIP",
		create: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeNodePort),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
		update: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeClusterIP),
			expectClusterIPs: true,
		},
	}, {
		name: "ClusterIP-LoadBalancer",
		create: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeClusterIP),
			expectClusterIPs: true,
		},
		update: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeLoadBalancer),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
	}, {
		name: "LoadBalancer-ClusterIP",
		create: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeLoadBalancer),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
		update: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeClusterIP),
			expectClusterIPs: true,
		},
	}, {
		name: "NodePort-LoadBalancer",
		create: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeNodePort),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
		update: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeLoadBalancer),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
	}, {
		name: "LoadBalancer-NodePort",
		create: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeLoadBalancer),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
		update: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeNodePort),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
	}, {
		name: "Headless-ExternalName",
		create: svcTestCase{
			svc:            svctest.MakeService("foo", svctest.SetHeadless),
			expectHeadless: true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeExternalName),
		},
	}, {
		name: "ExternalName-Headless",
		create: svcTestCase{
			svc: svctest.MakeService("foo", svctest.SetTypeExternalName),
		},
		update: svcTestCase{
			svc:            svctest.MakeService("foo", svctest.SetHeadless),
			expectHeadless: true,
		},
	}, {
		name: "Headless-NodePort",
		create: svcTestCase{
			svc:            svctest.MakeService("foo", svctest.SetHeadless),
			expectHeadless: true,
		},
		update: svcTestCase{
			svc:         svctest.MakeService("foo", svctest.SetTypeNodePort),
			expectError: true,
		},
	}, {
		name: "NodePort-Headless",
		create: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeNodePort),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
		update: svcTestCase{
			svc:         svctest.MakeService("foo", svctest.SetHeadless),
			expectError: true,
		},
	}, {
		name: "Headless-LoadBalancer",
		create: svcTestCase{
			svc:            svctest.MakeService("foo", svctest.SetHeadless),
			expectHeadless: true,
		},
		update: svcTestCase{
			svc:         svctest.MakeService("foo", svctest.SetTypeLoadBalancer),
			expectError: true,
		},
	}, {
		name: "LoadBalancer-Headless",
		create: svcTestCase{
			svc:              svctest.MakeService("foo", svctest.SetTypeLoadBalancer),
			expectClusterIPs: true,
			expectNodePorts:  true,
		},
		update: svcTestCase{
			svc:         svctest.MakeService("foo", svctest.SetHeadless),
			expectError: true,
		},
	}}

	helpTestCreateUpdateDelete(t, testCases)
}

func TestFeatureExternalTrafficPolicy(t *testing.T) {
	testCases := []cudTestCase{{
		name: "ExternalName_policy:none_hcnp:specified",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeExternalName,
				svctest.SetExternalTrafficPolicy(""),
				svctest.SetHealthCheckNodePort(30000)),
			expectError: true,
		},
	}, {
		name: "ExternalName_policy:Cluster_hcnp:none",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeExternalName,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyCluster)),
			expectError: true,
		},
	}, {
		name: "ExternalName_policy:Cluster_hcnp:specified",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeExternalName,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyCluster),
				svctest.SetHealthCheckNodePort(30000)),
			expectError: true,
		},
	}, {
		name: "ExternalName_policy:Local_hcnp:none",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeExternalName,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal)),
			expectError: true,
		},
	}, {
		name: "ExternalName_policy:Local_hcnp:specified",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeExternalName,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
				svctest.SetHealthCheckNodePort(30000)),
			expectError: true,
		},
	}, {
		name: "ClusterIP_policy:none_hcnp:none_policy:Cluster_hcnp:none",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeClusterIP,
				svctest.SetExternalTrafficPolicy("")),
			expectClusterIPs: true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeClusterIP,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyCluster)),
			expectError: true,
		},
	}, {
		name: "ClusterIP_policy:none_hcnp:specified",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeClusterIP,
				svctest.SetExternalTrafficPolicy(""),
				svctest.SetHealthCheckNodePort(30000)),
			expectError: true,
		},
	}, {
		name: "ClusterIP_policy:Cluster_hcnp:none",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeClusterIP,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyCluster)),
			expectError: true,
		},
	}, {
		name: "ClusterIP_policy:Cluster_hcnp:specified",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeClusterIP,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyCluster),
				svctest.SetHealthCheckNodePort(30000)),
			expectError: true,
		},
	}, {
		name: "ClusterIP_policy:Local_hcnp:none",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeClusterIP,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal)),
			expectError: true,
		},
	}, {
		name: "ClusterIP_policy:Local_hcnp:specified",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeClusterIP,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
				svctest.SetHealthCheckNodePort(30000)),
			expectError: true,
		},
	}, {
		name: "NodePort_policy:none_hcnp:none",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeNodePort,
				svctest.SetExternalTrafficPolicy("")),
			expectError: true,
		},
	}, {
		name: "NodePort_policy:none_hcnp:specified",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeNodePort,
				svctest.SetExternalTrafficPolicy(""),
				svctest.SetHealthCheckNodePort(30000)),
			expectError: true,
		},
	}, {
		name: "NodePort_policy:Cluster_hcnp:none_policy:Local_hcnp:none",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeNodePort,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyCluster)),
			expectClusterIPs:          true,
			expectNodePorts:           true,
			expectHealthCheckNodePort: false,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeNodePort,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal)),
			expectClusterIPs:          true,
			expectNodePorts:           true,
			expectHealthCheckNodePort: false,
		},
	}, {
		name: "NodePort_policy:Cluster_hcnp:specified",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeNodePort,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyCluster),
				svctest.SetHealthCheckNodePort(30000)),
			expectError: true,
		},
	}, {
		name: "NodePort_policy:Local_hcnp:none_policy:Cluster_hcnp:none",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeNodePort,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal)),
			expectClusterIPs:          true,
			expectNodePorts:           true,
			expectHealthCheckNodePort: false,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeNodePort,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyCluster)),
			expectClusterIPs:          true,
			expectNodePorts:           true,
			expectHealthCheckNodePort: false,
		},
	}, {
		name: "NodePort_policy:Local_hcnp:specified",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeNodePort,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
				svctest.SetHealthCheckNodePort(30000)),
			expectError: true,
		},
	}, {
		name: "LoadBalancer_policy:none_hcnp:none",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy("")),
			expectError: true,
		},
	}, {
		name: "LoadBalancer_policy:none_hcnp:specified",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(""),
				svctest.SetHealthCheckNodePort(30000)),
			expectError: true,
		},
	}, {
		name: "LoadBalancer_policy:Cluster_hcnp:none_policy:Local_hcnp:none",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyCluster)),
			expectClusterIPs:          true,
			expectNodePorts:           true,
			expectHealthCheckNodePort: false,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal)),
			expectClusterIPs:          true,
			expectNodePorts:           true,
			expectHealthCheckNodePort: true,
		},
	}, {
		name: "LoadBalancer_policy:Cluster_hcnp:none_policy:Local_hcnp:specified",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyCluster)),
			expectClusterIPs:          true,
			expectNodePorts:           true,
			expectHealthCheckNodePort: false,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
				svctest.SetHealthCheckNodePort(30000)),
			expectClusterIPs:          true,
			expectNodePorts:           true,
			expectHealthCheckNodePort: true,
		},
	}, {
		name: "LoadBalancer_policy:Cluster_hcnp:specified",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyCluster),
				svctest.SetHealthCheckNodePort(30000)),
			expectError: true,
		},
	}, {
		name: "LoadBalancer_policy:Local_hcnp:none_policy:Cluster_hcnp:none",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal)),
			expectClusterIPs:          true,
			expectNodePorts:           true,
			expectHealthCheckNodePort: true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyCluster)),
			expectClusterIPs:          true,
			expectNodePorts:           true,
			expectHealthCheckNodePort: false,
		},
	}, {
		name: "LoadBalancer_policy:Local_hcnp:specified_policy:Cluster_hcnp:none",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
				svctest.SetHealthCheckNodePort(30000)),
			expectClusterIPs:          true,
			expectNodePorts:           true,
			expectHealthCheckNodePort: true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyCluster)),
			expectClusterIPs:          true,
			expectNodePorts:           true,
			expectHealthCheckNodePort: false,
		},
	}, {
		name: "LoadBalancer_policy:Local_hcnp:specified_policy:Cluster_hcnp:different",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
				svctest.SetHealthCheckNodePort(30000)),
			expectClusterIPs:          true,
			expectNodePorts:           true,
			expectHealthCheckNodePort: true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
				svctest.SetHealthCheckNodePort(30001)),
			expectError: true,
		},
	}, {
		name: "LoadBalancer_policy:Local_hcnp:none_policy:Inalid",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal)),
			expectClusterIPs:          true,
			expectNodePorts:           true,
			expectHealthCheckNodePort: true,
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy("Invalid")),
			expectError: true,
		},
	}, {
		name: "LoadBalancer_policy:Local_hcnp:negative",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetExternalTrafficPolicy(api.ServiceExternalTrafficPolicyLocal),
				svctest.SetHealthCheckNodePort(-1)),
			expectError: true,
		},
	}}

	helpTestCreateUpdateDelete(t, testCases)
}

func TestFeatureInternalTrafficPolicy(t *testing.T) {
	prove := func(proofs ...svcTestProof) []svcTestProof {
		return proofs
	}
	proveITP := func(want api.ServiceInternalTrafficPolicy) svcTestProof {
		return func(t *testing.T, storage *wrapperRESTForTests, before, after *api.Service) {
			t.Helper()
			if got := after.Spec.InternalTrafficPolicy; got == nil {
				if want != "" {
					t.Errorf("internalTrafficPolicy was nil")
				}
			} else if *got != want {
				if want == "" {
					want = "nil"
				}
				t.Errorf("wrong internalTrafficPoilcy: expected %s, got %s", want, *got)
			}
		}
	}

	testCases := []cudTestCase{{
		name: "ExternalName_policy:none-ExternalName_policy:none",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeExternalName),
			prove: prove(proveITP("")),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeExternalName),
			prove: prove(proveITP("")),
		},
	}, {
		name: "ClusterIP_policy:none-ClusterIP_policy:Local",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeClusterIP),
			expectClusterIPs: true,
			prove:            prove(proveITP(api.ServiceInternalTrafficPolicyCluster)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeClusterIP,
				svctest.SetInternalTrafficPolicy(api.ServiceInternalTrafficPolicyLocal)),
			expectClusterIPs: true,
			prove:            prove(proveITP(api.ServiceInternalTrafficPolicyLocal)),
		},
	}, {
		name: "ClusterIP_policy:Cluster-ClusterIP_policy:Local",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeClusterIP,
				svctest.SetInternalTrafficPolicy(api.ServiceInternalTrafficPolicyCluster)),
			expectClusterIPs: true,
			prove:            prove(proveITP(api.ServiceInternalTrafficPolicyCluster)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeClusterIP,
				svctest.SetInternalTrafficPolicy(api.ServiceInternalTrafficPolicyLocal)),
			expectClusterIPs: true,
			prove:            prove(proveITP(api.ServiceInternalTrafficPolicyLocal)),
		},
	}, {
		name: "NodePort_policy:none-NodePort_policy:Local",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeNodePort),
			expectClusterIPs: true,
			expectNodePorts:  true,
			prove:            prove(proveITP(api.ServiceInternalTrafficPolicyCluster)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeNodePort,
				svctest.SetInternalTrafficPolicy(api.ServiceInternalTrafficPolicyLocal)),
			expectClusterIPs: true,
			expectNodePorts:  true,
			prove:            prove(proveITP(api.ServiceInternalTrafficPolicyLocal)),
		},
	}, {
		name: "NodePort_policy:Cluster-NodePort_policy:Local",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeNodePort,
				svctest.SetInternalTrafficPolicy(api.ServiceInternalTrafficPolicyCluster)),
			expectClusterIPs: true,
			expectNodePorts:  true,
			prove:            prove(proveITP(api.ServiceInternalTrafficPolicyCluster)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeNodePort,
				svctest.SetInternalTrafficPolicy(api.ServiceInternalTrafficPolicyLocal)),
			expectClusterIPs: true,
			expectNodePorts:  true,
			prove:            prove(proveITP(api.ServiceInternalTrafficPolicyLocal)),
		},
	}, {
		name: "LoadBalancer_policy:none-LoadBalancer_policy:Local",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer),
			expectClusterIPs: true,
			expectNodePorts:  true,
			prove:            prove(proveITP(api.ServiceInternalTrafficPolicyCluster)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetInternalTrafficPolicy(api.ServiceInternalTrafficPolicyLocal)),
			expectClusterIPs: true,
			expectNodePorts:  true,
			prove:            prove(proveITP(api.ServiceInternalTrafficPolicyLocal)),
		},
	}, {
		name: "LoadBalancer_policy:Cluster-LoadBalancer_policy:Local",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetInternalTrafficPolicy(api.ServiceInternalTrafficPolicyCluster)),
			expectClusterIPs: true,
			expectNodePorts:  true,
			prove:            prove(proveITP(api.ServiceInternalTrafficPolicyCluster)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetTypeLoadBalancer,
				svctest.SetInternalTrafficPolicy(api.ServiceInternalTrafficPolicyLocal)),
			expectClusterIPs: true,
			expectNodePorts:  true,
			prove:            prove(proveITP(api.ServiceInternalTrafficPolicyLocal)),
		},
	}, {
		name: "Headless_policy:none-Headless_policy:Local",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetHeadless),
			expectHeadless: true,
			prove:          prove(proveITP(api.ServiceInternalTrafficPolicyCluster)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetHeadless,
				svctest.SetInternalTrafficPolicy(api.ServiceInternalTrafficPolicyLocal)),
			expectHeadless: true,
			prove:          prove(proveITP(api.ServiceInternalTrafficPolicyLocal)),
		},
	}, {
		name: "Headless_policy:Cluster-Headless_policy:Local",
		create: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetHeadless,
				svctest.SetInternalTrafficPolicy(api.ServiceInternalTrafficPolicyCluster)),
			expectHeadless: true,
			prove:          prove(proveITP(api.ServiceInternalTrafficPolicyCluster)),
		},
		update: svcTestCase{
			svc: svctest.MakeService("foo",
				svctest.SetHeadless,
				svctest.SetInternalTrafficPolicy(api.ServiceInternalTrafficPolicyLocal)),
			expectHeadless: true,
			prove:          prove(proveITP(api.ServiceInternalTrafficPolicyLocal)),
		},
	}}

	helpTestCreateUpdateDelete(t, testCases)
}

// TODO(thockin): 还需要补充以下特性的功能测试：
//   externalIPs, lbip, lbsourceranges, externalname, PublishNotReadyAddresses, AllocateLoadBalancerNodePorts, LoadBalancerClass, status

// 该函数仅在本文件内使用，因为还不够完善，无法普遍使用。
func makePod(name string, ips ...string) api.Pod {
	p := api.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceDefault,
		},
		Spec: api.PodSpec{
			RestartPolicy: api.RestartPolicyAlways,
			DNSPolicy:     api.DNSDefault,
			Containers:    []api.Container{{Name: "ctr", Image: "img", ImagePullPolicy: api.PullIfNotPresent, TerminationMessagePolicy: api.TerminationMessageReadFile}},
		},
		Status: api.PodStatus{
			PodIPs: []api.PodIP{},
		},
	}

	for _, ip := range ips {
		p.Status.PodIPs = append(p.Status.PodIPs, api.PodIP{IP: ip})
	}

	return p
}

func TestServiceRegistryResourceLocation(t *testing.T) {
	pods := []api.Pod{
		makePod("unnamed", "1.2.3.4", "1.2.3.5"),
		makePod("named", "1.2.3.6", "1.2.3.7"),
		makePod("no-endpoints", "9.9.9.9"), // 用于证明该项不会被选中
	}

	endpoints := []*discovery.EndpointSlice{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "unnamed-ep",
				Namespace: metav1.NamespaceDefault,
				Labels: map[string]string{
					"kubernetes.io/service-name": "unnamed",
				},
			},
			AddressType: discovery.AddressTypeIPv4,
			Endpoints: []discovery.Endpoint{{
				Addresses: []string{"1.2.3.4"},
				TargetRef: &api.ObjectReference{
					Name:      "unnamed",
					Namespace: metav1.NamespaceDefault,
				},
			}},
			Ports: []discovery.EndpointPort{{
				Name: ptr.To(""),
				Port: ptr.To[int32](80),
			}},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "unnamed2-ep",
				Namespace: metav1.NamespaceDefault,
				Labels: map[string]string{
					"kubernetes.io/service-name": "unnamed2",
				},
			},
			AddressType: discovery.AddressTypeIPv4,
			Endpoints: []discovery.Endpoint{{
				Addresses: []string{"1.2.3.5"},
				TargetRef: &api.ObjectReference{
					Name:      "unnamed",
					Namespace: metav1.NamespaceDefault,
				},
			}},
			Ports: []discovery.EndpointPort{{
				Name: ptr.To(""),
				Port: ptr.To[int32](80),
			}},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "named-ep",
				Namespace: metav1.NamespaceDefault,
				Labels: map[string]string{
					"kubernetes.io/service-name": "named",
				},
			},
			AddressType: discovery.AddressTypeIPv4,
			Endpoints: []discovery.Endpoint{{
				Addresses: []string{"1.2.3.6"},
				TargetRef: &api.ObjectReference{
					Name:      "named",
					Namespace: metav1.NamespaceDefault,
				},
			}},
			Ports: []discovery.EndpointPort{{
				Name: ptr.To("p"),
				Port: ptr.To[int32](80),
			}, {
				Name: ptr.To("q"),
				Port: ptr.To[int32](81),
			}},
		},
		// 用于证明该取值不会被选中
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "no-endpoints-ep",
				Namespace: metav1.NamespaceDefault,
				Labels: map[string]string{
					"kubernetes.io/service-name": "no-endpoints",
				},
			},
			AddressType: discovery.AddressTypeIPv4,
		},
	}

	storage, _, cleanup := newStorageWithPods(t, []api.IPFamily{api.IPv4Protocol}, pods, endpoints)
	defer cleanup()

	ctx := genericregistrytest.NewNamespaceScopeContext(storage.Store, metav1.NamespaceDefault)
	for _, name := range []string{"unnamed", "unnamed2", "no-endpoints"} {
		_, err := storage.Create(ctx,
			svctest.MakeService(name,
				svctest.SetPorts(
					svctest.MakeServicePort("", 93, intstr.FromInt32(80), api.ProtocolTCP))),
			rest.ValidateAllObjectFunc, &metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("unexpected error creating service %q: %v", name, err)
		}

	}
	_, err := storage.Create(ctx,
		svctest.MakeService("named",
			svctest.SetPorts(
				svctest.MakeServicePort("p", 93, intstr.FromInt32(80), api.ProtocolTCP),
				svctest.MakeServicePort("q", 76, intstr.FromInt32(81), api.ProtocolTCP))),
		rest.ValidateAllObjectFunc, &metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("unexpected error creating service %q: %v", "named", err)
	}
	redirector := rest.Redirector(storage)

	cases := []struct {
		query  string
		err    bool
		expect string
	}{{
		query:  "unnamed",
		expect: "//1.2.3.4:80",
	}, {
		query:  "unnamed:",
		expect: "//1.2.3.4:80",
	}, {
		query:  "unnamed:93",
		expect: "//1.2.3.4:80",
	}, {
		query:  "http:unnamed:",
		expect: "http://1.2.3.4:80",
	}, {
		query:  "http:unnamed:93",
		expect: "http://1.2.3.4:80",
	}, {
		query: "unnamed:80",
		err:   true,
	}, {
		query:  "unnamed2",
		expect: "//1.2.3.5:80",
	}, {
		query:  "named:p",
		expect: "//1.2.3.6:80",
	}, {
		query:  "named:q",
		expect: "//1.2.3.6:81",
	}, {
		query:  "named:93",
		expect: "//1.2.3.6:80",
	}, {
		query:  "named:76",
		expect: "//1.2.3.6:81",
	}, {
		query:  "http:named:p",
		expect: "http://1.2.3.6:80",
	}, {
		query:  "http:named:q",
		expect: "http://1.2.3.6:81",
	}, {
		query: "named:bad",
		err:   true,
	}, {
		query: "no-endpoints",
		err:   true,
	}, {
		query: "non-existent",
		err:   true,
	}}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			location, _, err := redirector.ResourceLocation(ctx, tc.query)
			if tc.err == false && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.err && err == nil {
				t.Fatalf("unexpected success")
			}
			if !tc.err {
				if location == nil {
					t.Errorf("unexpected location: %v", location)
				}
				if e, a := tc.expect, location.String(); e != a {
					t.Errorf("expected %q, but got %q", e, a)
				}
			}
		})
	}
}

func TestUpdateServiceLoadBalancerStatus(t *testing.T) {
	storage, statusStorage, cleanup := newStorage(t, []api.IPFamily{api.IPv4Protocol})
	defer cleanup()
	defer statusStorage.store.DestroyFunc()

	ipModeVIP := api.LoadBalancerIPModeVIP
	ipModeProxy := api.LoadBalancerIPModeProxy
	ipModeDummy := api.LoadBalancerIPMode("dummy")

	testCases := []struct {
		name                   string
		statusBeforeUpdate     api.ServiceStatus
		newStatus              api.ServiceStatus
		expectedStatus         api.ServiceStatus
		expectErr              bool
		expectedReasonForError metav1.StatusReason
	}{
		{
			name: "ipMode used in old and in new",
			statusBeforeUpdate: api.ServiceStatus{
				LoadBalancer: api.LoadBalancerStatus{
					Ingress: []api.LoadBalancerIngress{{
						IP:     "1.2.3.4",
						IPMode: &ipModeProxy,
					}},
				},
			},
			newStatus: api.ServiceStatus{
				LoadBalancer: api.LoadBalancerStatus{
					Ingress: []api.LoadBalancerIngress{{
						IP:     "1.2.3.4",
						IPMode: &ipModeVIP,
					}},
				},
			},
			expectedStatus: api.ServiceStatus{
				LoadBalancer: api.LoadBalancerStatus{
					Ingress: []api.LoadBalancerIngress{{
						IP:     "1.2.3.4",
						IPMode: &ipModeVIP,
					}},
				},
			},
			expectErr: false,
		}, {
			name: "ipMode used in old, not used in new",
			statusBeforeUpdate: api.ServiceStatus{
				LoadBalancer: api.LoadBalancerStatus{
					Ingress: []api.LoadBalancerIngress{{
						IP:     "1.2.3.4",
						IPMode: &ipModeVIP,
					}},
				},
			},
			newStatus: api.ServiceStatus{
				LoadBalancer: api.LoadBalancerStatus{
					Ingress: []api.LoadBalancerIngress{{
						IP: "1.2.3.4",
					}},
				},
			},
			expectedStatus:         api.ServiceStatus{},
			expectErr:              true,
			expectedReasonForError: metav1.StatusReasonInvalid,
		}, {
			name: "ipMode used in old, invalid value used in new",
			statusBeforeUpdate: api.ServiceStatus{
				LoadBalancer: api.LoadBalancerStatus{
					Ingress: []api.LoadBalancerIngress{{
						IP:     "1.2.3.4",
						IPMode: &ipModeVIP,
					}},
				},
			},
			newStatus: api.ServiceStatus{
				LoadBalancer: api.LoadBalancerStatus{
					Ingress: []api.LoadBalancerIngress{{
						IP:     "1.2.3.4",
						IPMode: &ipModeDummy,
					}},
				},
			},
			expectedStatus:         api.ServiceStatus{},
			expectErr:              true,
			expectedReasonForError: metav1.StatusReasonInvalid,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {

			svc := svctest.MakeService("foo", svctest.SetTypeLoadBalancer)
			ctx := genericregistrytest.NewNamespaceScopeContext(storage.Store, metav1.NamespaceDefault)
			obj, err := storage.Create(ctx, svc, rest.ValidateAllObjectFunc, &metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("created svc: %s", err)
			}
			defer storage.Delete(ctx, svc.Name, rest.ValidateAllObjectFunc, &metav1.DeleteOptions{})

			// 准备 status
			oldSvc := obj.(*api.Service).DeepCopy()
			oldSvc.Status = tc.statusBeforeUpdate
			obj, _, err = statusStorage.Update(ctx, oldSvc.Name, rest.DefaultUpdatedObjectInfo(oldSvc), rest.ValidateAllObjectFunc, rest.ValidateAllObjectUpdateFunc, false, &metav1.UpdateOptions{})
			if err != nil {
				t.Fatalf("updated status: %s", err)
			}

			newSvc := obj.(*api.Service).DeepCopy()
			newSvc.Status = tc.newStatus
			obj, _, err = statusStorage.Update(ctx, newSvc.Name, rest.DefaultUpdatedObjectInfo(newSvc), rest.ValidateAllObjectFunc, rest.ValidateAllObjectUpdateFunc, false, &metav1.UpdateOptions{})
			if err != nil {
				if tc.expectErr && tc.expectedReasonForError == errors.ReasonForError(err) {
					return
				}
				t.Fatalf("updated status: %s", err)
			}

			updated := obj.(*api.Service)
			if !reflect.DeepEqual(tc.expectedStatus, updated.Status) {
				t.Errorf("%v: unexpected svc status: %v", tc.name, cmp.Diff(tc.expectedStatus, updated.Status))
			}
		})
	}
}
