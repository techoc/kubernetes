/*
Copyright 2019 The Kubernetes Authors.

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

package endpointslice

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	corelisters "k8s.io/client-go/listers/core/v1"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/endpointslice/metrics"
	"k8s.io/endpointslice/topologycache"
	endpointsliceutil "k8s.io/endpointslice/util"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/utils/ptr"
)

const (
	// controllerName 是本控制器写入 EndpointSlice 的 managed-by 标签值，用于标识切片归属。
	controllerName = "endpointslice-controller.k8s.io"
)

// init 在包初始化时注册控制器指标，保证测试中可以读取并断言这些指标。
func init() {
	// 注册指标，避免后续 resetMetrics 或采集时指标未注册。
	metrics.RegisterMetrics()
}

// expectAction 断言指定下标处的 clientset action 具有给定的 verb 与 resource。
func expectAction(t *testing.T, actions []k8stesting.Action, index int, verb, resource string) {
	t.Helper()
	// 先确认 action 数量足够，再逐项比对 verb 和 resource。
	if len(actions) <= index {
		t.Fatalf("Expected at least %d actions, got %d", index+1, len(actions))
	}

	action := actions[index]
	if action.GetVerb() != verb {
		t.Errorf("Expected action %d verb to be %s, got %s", index, verb, action.GetVerb())
	}

	if action.GetResource().Resource != resource {
		t.Errorf("Expected action %d resource to be %s, got %s", index, resource, action.GetResource().Resource)
	}
}

// cacheMutationCheck helps ensure that cached objects have not been changed
// in any way throughout a test run.
// cacheMutationCheck 在测试开始时保存对象及其深拷贝，用于检测缓存对象是否被就地修改。
type cacheMutationCheck struct {
	objects []cacheObject
}

// cacheObject stores a reference to an original object as well as a deep copy
// of that object to track any mutations in the original object.
// cacheObject 保存一个原始对象引用和它的深拷贝，两者比对即可发现对缓存对象的改动。
type cacheObject struct {
	original runtime.Object
	deepCopy runtime.Object
}

// newCacheMutationCheck initializes a cacheMutationCheck with EndpointSlices.
// newCacheMutationCheck 用给定的 EndpointSlice 列表构造一个空的检查器并逐个登记。
func newCacheMutationCheck(endpointSlices []*discovery.EndpointSlice) cacheMutationCheck {
	cmc := cacheMutationCheck{}
	for _, endpointSlice := range endpointSlices {
		cmc.Add(endpointSlice)
	}
	return cmc
}

// Add appends a runtime.Object and a deep copy of that object into the
// cacheMutationCheck.
// Add 把一个运行时对象及其深拷贝登记到检查器中。
func (cmc *cacheMutationCheck) Add(o runtime.Object) {
	cmc.objects = append(cmc.objects, cacheObject{
		original: o,
		deepCopy: o.DeepCopyObject(),
	})
}

// Check verifies that no objects in the cacheMutationCheck have been mutated.
// Check 遍历所有登记对象，若原始对象与深拷贝不再深度相等则报告缓存被修改。
func (cmc *cacheMutationCheck) Check(t *testing.T) {
	for _, o := range cmc.objects {
		if !reflect.DeepEqual(o.original, o.deepCopy) {
			// Cached objects can't be safely mutated and instead should be deep
			// copied before changed in any way.
			// 缓存对象不可直接修改，必须先深拷贝再改动，否则会污染 informer 缓存。
			t.Errorf("Cached object was unexpectedly mutated. Original: %+v, Mutated: %+v", o.deepCopy, o.original)
		}
	}
}

// defaultMaxEndpointsPerSlice 是多数用例采用的默认值：单个 EndpointSlice 最多容纳 100 个端点。
var defaultMaxEndpointsPerSlice = int32(100)

// Even when there are no pods, we want to have a placeholder slice for each service
// TestReconcileEmpty 验证即使 Service 没有任何匹配 Pod，也必须为其创建一个空的占位 EndpointSlice。
// 构造 1 个 Service、0 个 Pod 和 0 个已有切片；断言只产生一次 create，切片名以 svc 名开头、带有 service-name 标签、端口与端点均为空，同时校验 generation 记录与各项指标。
func TestReconcileEmpty(t *testing.T) {
	// 创建 fake clientset 并重置指标，保证用例环境干净。
	client := newClientset()
	resetMetrics()
	namespace := "test"
	// 构造一个没有匹配 Pod 的 Service。
	svc, _ := newServiceAndEndpointMeta("foo", namespace)

	// 协调器只需知道一个节点即可。
	r := newReconciler(client, []*corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}}, defaultMaxEndpointsPerSlice)
	// 传入空 Pod 列表和空切片列表执行协调。
	reconcileHelper(t, r, &svc, []*corev1.Pod{}, []*discovery.EndpointSlice{}, time.Now())
	// 断言只产生一次 create action。
	expectActions(t, client.Actions(), 1, "create", "endpointslices")

	// 取出唯一切片，校验名称、标签、端口与端点内容。
	slices := fetchEndpointSlices(t, client, namespace)
	assert.Len(t, slices, 1, "Expected 1 endpoint slices")

	assert.Regexp(t, "^"+svc.Name, slices[0].Name)
	assert.Equal(t, svc.Name, slices[0].Labels[discovery.LabelServiceName])
	assert.EqualValues(t, []discovery.EndpointPort{}, slices[0].Ports)
	assert.EqualValues(t, []discovery.Endpoint{}, slices[0].Endpoints)
	// 校验 tracker 记录该切片的 generation 为 1。
	expectTrackedGeneration(t, r.endpointSliceTracker, &slices[0], 1)
	// 校验期望切片数、实际切片数、端点数、创建数与变更次数等指标。
	expectMetrics(t, expectedMetrics{desiredSlices: 1, actualSlices: 1, desiredEndpoints: 0, addedPerSync: 0, removedPerSync: 0, numCreated: 1, numUpdated: 0, numDeleted: 0, slicesChangedPerSync: 1})
}

// Given a single pod matching a service selector and no existing endpoint slices,
// a slice should be created
// TestReconcile1Pod 验证单个匹配 Pod 在没有已有切片时会按 Service 的地址族创建相应切片。
// Pod 同时具备 IPv4/IPv6 地址、主机名和带 zone/region 标签的节点；表驱动用例覆盖未指定 IPFamily、IPv4/IPv6 单栈、指定 ClusterIP、自定义标签、非法保留标签以及双栈等场景。
// 断言每个地址族各创建一个切片，切片名、标签、触发时间注解、地址类型、端点内容（含 zone、nodeName、TargetRef）与指标均符合预期。
func TestReconcile1Pod(t *testing.T) {
	namespace := "test"
	// 准备多种 Service 变体：未指定 IPFamily、单栈、指定 ClusterIP、自定义/非法标签以及双栈。
	noFamilyService, _ := newServiceAndEndpointMeta("foo", namespace)
	noFamilyService.Spec.ClusterIP = "10.0.0.10"
	noFamilyService.Spec.IPFamilies = nil

	svcv4, _ := newServiceAndEndpointMeta("foo", namespace)
	svcv4ClusterIP, _ := newServiceAndEndpointMeta("foo", namespace)
	svcv4ClusterIP.Spec.ClusterIP = "1.1.1.1"
	svcv4Labels, _ := newServiceAndEndpointMeta("foo", namespace)
	svcv4Labels.Labels = map[string]string{"foo": "bar"}
	svcv4BadLabels, _ := newServiceAndEndpointMeta("foo", namespace)
	svcv4BadLabels.Labels = map[string]string{discovery.LabelServiceName: "bad",
		discovery.LabelManagedBy: "actor", corev1.IsHeadlessService: "invalid"}
	svcv6, _ := newServiceAndEndpointMeta("foo", namespace)
	svcv6.Spec.IPFamilies = []corev1.IPFamily{corev1.IPv6Protocol}
	svcv6ClusterIP, _ := newServiceAndEndpointMeta("foo", namespace)
	svcv6ClusterIP.Spec.ClusterIP = "1234::5678:0000:0000:9abc:def1"
	// newServiceAndEndpointMeta generates v4 single stack
	svcv6ClusterIP.Spec.IPFamilies = []corev1.IPFamily{corev1.IPv6Protocol}

	// dual stack
	dualStackSvc, _ := newServiceAndEndpointMeta("foo", namespace)
	dualStackSvc.Spec.IPFamilies = []corev1.IPFamily{corev1.IPv4Protocol, corev1.IPv6Protocol}
	dualStackSvc.Spec.ClusterIP = "10.0.0.10"
	dualStackSvc.Spec.ClusterIPs = []string{"10.0.0.10", "2000::1"}

	// 构造单个匹配 Pod：设置双栈 IP、主机名与所属节点。
	pod1 := newPod(1, namespace, true, 1, false)
	pod1.Status.PodIPs = []corev1.PodIP{{IP: "1.2.3.4"}, {IP: "1234::5678:0000:0000:9abc:def0"}}
	pod1.Spec.Hostname = "example-hostname"
	// 节点携带 zone/region 标签，供端点填充拓扑信息。
	node1 := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: pod1.Spec.NodeName,
			Labels: map[string]string{
				"topology.kubernetes.io/zone":   "us-central1-a",
				"topology.kubernetes.io/region": "us-central1",
			},
		},
	}

	// 表驱动用例声明各场景的 Service、期望地址类型、端点、标签与端点分布。
	testCases := map[string]struct {
		service                  corev1.Service
		expectedAddressType      discovery.AddressType
		expectedEndpoint         discovery.Endpoint
		expectedLabels           map[string]string
		expectedEndpointPerSlice map[discovery.AddressType][]discovery.Endpoint
	}{
		"no-family-service": {
			service: noFamilyService,
			expectedEndpointPerSlice: map[discovery.AddressType][]discovery.Endpoint{
				discovery.AddressTypeIPv4: {
					{
						Addresses: []string{"1.2.3.4"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(true),
							Serving:     ptr.To(true),
							Terminating: ptr.To(false),
						},
						Zone:     ptr.To("us-central1-a"),
						NodeName: ptr.To("node-1"),
						TargetRef: &corev1.ObjectReference{
							Kind:      "Pod",
							Namespace: namespace,
							Name:      "pod1",
						},
					},
				},
			},
			expectedLabels: map[string]string{
				discovery.LabelManagedBy:   controllerName,
				discovery.LabelServiceName: "foo",
			},
		},
		"ipv4": {
			service: svcv4,
			expectedEndpointPerSlice: map[discovery.AddressType][]discovery.Endpoint{
				discovery.AddressTypeIPv4: {
					{
						Addresses: []string{"1.2.3.4"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(true),
							Serving:     ptr.To(true),
							Terminating: ptr.To(false),
						},
						Zone:     ptr.To("us-central1-a"),
						NodeName: ptr.To("node-1"),
						TargetRef: &corev1.ObjectReference{
							Kind:      "Pod",
							Namespace: namespace,
							Name:      "pod1",
						},
					},
				},
			},
			expectedLabels: map[string]string{
				discovery.LabelManagedBy:   controllerName,
				discovery.LabelServiceName: "foo",
				corev1.IsHeadlessService:   "",
			},
		},
		"ipv4-clusterip": {
			service: svcv4ClusterIP,
			expectedEndpointPerSlice: map[discovery.AddressType][]discovery.Endpoint{
				discovery.AddressTypeIPv4: {
					{
						Addresses: []string{"1.2.3.4"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(true),
							Serving:     ptr.To(true),
							Terminating: ptr.To(false),
						},
						Zone:     ptr.To("us-central1-a"),
						NodeName: ptr.To("node-1"),
						TargetRef: &corev1.ObjectReference{
							Kind:      "Pod",
							Namespace: namespace,
							Name:      "pod1",
						},
					},
				},
			},
			expectedAddressType: discovery.AddressTypeIPv4,
			expectedEndpoint: discovery.Endpoint{
				Addresses: []string{"1.2.3.4"},
				Conditions: discovery.EndpointConditions{
					Ready:       ptr.To(true),
					Serving:     ptr.To(true),
					Terminating: ptr.To(false),
				},
				Zone:     ptr.To("us-central1-a"),
				NodeName: ptr.To("node-1"),
				TargetRef: &corev1.ObjectReference{
					Kind:      "Pod",
					Namespace: namespace,
					Name:      "pod1",
				},
			},
			expectedLabels: map[string]string{
				discovery.LabelManagedBy:   controllerName,
				discovery.LabelServiceName: "foo",
			},
		},
		"ipv4-labels": {
			service: svcv4Labels,
			expectedEndpointPerSlice: map[discovery.AddressType][]discovery.Endpoint{
				discovery.AddressTypeIPv4: {
					{
						Addresses: []string{"1.2.3.4"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(true),
							Serving:     ptr.To(true),
							Terminating: ptr.To(false),
						},
						Zone:     ptr.To("us-central1-a"),
						NodeName: ptr.To("node-1"),
						TargetRef: &corev1.ObjectReference{
							Kind:      "Pod",
							Namespace: namespace,
							Name:      "pod1",
						},
					},
				},
			},
			expectedAddressType: discovery.AddressTypeIPv4,
			expectedEndpoint: discovery.Endpoint{
				Addresses: []string{"1.2.3.4"},
				Conditions: discovery.EndpointConditions{
					Ready:       ptr.To(true),
					Serving:     ptr.To(true),
					Terminating: ptr.To(false),
				},
				Zone:     ptr.To("us-central1-a"),
				NodeName: ptr.To("node-1"),
				TargetRef: &corev1.ObjectReference{
					Kind:      "Pod",
					Namespace: namespace,
					Name:      "pod1",
				},
			},
			expectedLabels: map[string]string{
				discovery.LabelManagedBy:   controllerName,
				discovery.LabelServiceName: "foo",
				"foo":                      "bar",
				corev1.IsHeadlessService:   "",
			},
		},
		"ipv4-bad-labels": {
			service: svcv4BadLabels,
			expectedEndpointPerSlice: map[discovery.AddressType][]discovery.Endpoint{
				discovery.AddressTypeIPv4: {
					{
						Addresses: []string{"1.2.3.4"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(true),
							Serving:     ptr.To(true),
							Terminating: ptr.To(false),
						},
						Zone:     ptr.To("us-central1-a"),
						NodeName: ptr.To("node-1"),
						TargetRef: &corev1.ObjectReference{
							Kind:      "Pod",
							Namespace: namespace,
							Name:      "pod1",
						},
					},
				},
			},
			expectedAddressType: discovery.AddressTypeIPv4,
			expectedEndpoint: discovery.Endpoint{
				Addresses: []string{"1.2.3.4"},
				Conditions: discovery.EndpointConditions{
					Ready:       ptr.To(true),
					Serving:     ptr.To(true),
					Terminating: ptr.To(false),
				},
				Zone:     ptr.To("us-central1-a"),
				NodeName: ptr.To("node-1"),
				TargetRef: &corev1.ObjectReference{
					Kind:      "Pod",
					Namespace: namespace,
					Name:      "pod1",
				},
			},
			expectedLabels: map[string]string{
				discovery.LabelManagedBy:   controllerName,
				discovery.LabelServiceName: "foo",
				corev1.IsHeadlessService:   "",
			},
		},

		"ipv6": {
			service: svcv6,
			expectedEndpointPerSlice: map[discovery.AddressType][]discovery.Endpoint{
				discovery.AddressTypeIPv6: {
					{
						Addresses: []string{"1234::5678:0:0:9abc:def0"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(true),
							Serving:     ptr.To(true),
							Terminating: ptr.To(false),
						},
						Zone:     ptr.To("us-central1-a"),
						NodeName: ptr.To("node-1"),
						TargetRef: &corev1.ObjectReference{
							Kind:      "Pod",
							Namespace: namespace,
							Name:      "pod1",
						},
					},
				},
			},
			expectedLabels: map[string]string{
				discovery.LabelManagedBy:   controllerName,
				discovery.LabelServiceName: "foo",
				corev1.IsHeadlessService:   "",
			},
		},

		"ipv6-clusterip": {
			service: svcv6ClusterIP,
			expectedEndpointPerSlice: map[discovery.AddressType][]discovery.Endpoint{
				discovery.AddressTypeIPv6: {
					{
						Addresses: []string{"1234::5678:0:0:9abc:def0"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(true),
							Serving:     ptr.To(true),
							Terminating: ptr.To(false),
						},
						Zone:     ptr.To("us-central1-a"),
						NodeName: ptr.To("node-1"),
						TargetRef: &corev1.ObjectReference{
							Kind:      "Pod",
							Namespace: namespace,
							Name:      "pod1",
						},
					},
				},
			},
			expectedLabels: map[string]string{
				discovery.LabelManagedBy:   controllerName,
				discovery.LabelServiceName: "foo",
			},
		},

		"dualstack-service": {
			service: dualStackSvc,
			expectedEndpointPerSlice: map[discovery.AddressType][]discovery.Endpoint{
				discovery.AddressTypeIPv6: {
					{
						Addresses: []string{"1234::5678:0:0:9abc:def0"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(true),
							Serving:     ptr.To(true),
							Terminating: ptr.To(false),
						},
						Zone:     ptr.To("us-central1-a"),
						NodeName: ptr.To("node-1"),
						TargetRef: &corev1.ObjectReference{
							Kind:      "Pod",
							Namespace: namespace,
							Name:      "pod1",
						},
					},
				},
				discovery.AddressTypeIPv4: {
					{
						Addresses: []string{"1.2.3.4"},
						Conditions: discovery.EndpointConditions{
							Ready:       ptr.To(true),
							Serving:     ptr.To(true),
							Terminating: ptr.To(false),
						},
						Zone:     ptr.To("us-central1-a"),
						NodeName: ptr.To("node-1"),
						TargetRef: &corev1.ObjectReference{
							Kind:      "Pod",
							Namespace: namespace,
							Name:      "pod1",
						},
					},
				},
			},
			expectedLabels: map[string]string{
				discovery.LabelManagedBy:   controllerName,
				discovery.LabelServiceName: "foo",
			},
		},
	}

	// 逐用例执行协调，并对 action 数、切片元数据、端点内容与指标逐项断言。
	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			// 每个子用例使用独立的 fake clientset、重置指标并记录触发时间。
			client := newClientset()
			resetMetrics()
			// 记录触发时间，随后断言它被写入切片的 last-change-trigger-time 注解。
			triggerTime := time.Now().UTC()
			r := newReconciler(client, []*corev1.Node{node1}, defaultMaxEndpointsPerSlice)
			reconcileHelper(t, r, &testCase.service, []*corev1.Pod{pod1}, []*discovery.EndpointSlice{}, triggerTime)

			// 每个预期的地址族对应一次 create action。
			if len(client.Actions()) != len(testCase.expectedEndpointPerSlice) {
				t.Errorf("Expected %v clientset action, got %d", len(testCase.expectedEndpointPerSlice), len(client.Actions()))
			}

			// 获取全部已创建切片，并校验数量。
			slices := fetchEndpointSlices(t, client, namespace)

			if len(slices) != len(testCase.expectedEndpointPerSlice) {
				t.Fatalf("Expected %v EndpointSlice, got %d", len(testCase.expectedEndpointPerSlice), len(slices))
			}

			for _, slice := range slices {
				if !strings.HasPrefix(slice.Name, testCase.service.Name) {
					t.Fatalf("Expected EndpointSlice name to start with %s, got %s", testCase.service.Name, slice.Name)
				}

				if !reflect.DeepEqual(testCase.expectedLabels, slice.Labels) {
					t.Errorf("Expected EndpointSlice to have labels: %v , got %v", testCase.expectedLabels, slice.Labels)
				}
				if slice.Labels[discovery.LabelServiceName] != testCase.service.Name {
					t.Fatalf("Expected EndpointSlice to have label set with %s value, got %s", testCase.service.Name, slice.Labels[discovery.LabelServiceName])
				}

				if slice.Annotations[corev1.EndpointsLastChangeTriggerTime] != triggerTime.Format(time.RFC3339Nano) {
					t.Fatalf("Expected EndpointSlice trigger time annotation to be %s, got %s", triggerTime.Format(time.RFC3339Nano), slice.Annotations[corev1.EndpointsLastChangeTriggerTime])
				}

				// validate that this slice has address type matching expected
				// 若切片的地址类型不在期望集合中，视为意外创建的切片。
				expectedEndPointList := testCase.expectedEndpointPerSlice[slice.AddressType]
				if expectedEndPointList == nil {
					t.Fatalf("address type %v is not expected", slice.AddressType)
				}

				if len(slice.Endpoints) != len(expectedEndPointList) {
					t.Fatalf("Expected %v Endpoint, got %d", len(expectedEndPointList), len(slice.Endpoints))
				}

				// 本测试每个切片仅包含一个端点，直接校验该端点内容。
				// test is limited to *ONE* endpoint
				endpoint := slice.Endpoints[0]
				if !reflect.DeepEqual(endpoint, expectedEndPointList[0]) {
					t.Fatalf("Expected endpoint: %+v, got: %+v", expectedEndPointList[0], endpoint)
				}

				expectTrackedGeneration(t, r.endpointSliceTracker, &slice, 1)

				// 显式指定 IPFamilies 时，每个地址族单独计一次切片变更。
				expectSlicesChangedPerSync := 1
				if testCase.service.Spec.IPFamilies != nil && len(testCase.service.Spec.IPFamilies) > 0 {
					expectSlicesChangedPerSync = len(testCase.service.Spec.IPFamilies)
				}
				expectMetrics(t,
					expectedMetrics{
						desiredSlices:        1,
						actualSlices:         1,
						desiredEndpoints:     1,
						addedPerSync:         len(testCase.expectedEndpointPerSlice),
						removedPerSync:       0,
						numCreated:           len(testCase.expectedEndpointPerSlice),
						numUpdated:           0,
						numDeleted:           0,
						slicesChangedPerSync: expectSlicesChangedPerSync,
					})
			}
		})
	}
}

// given an existing placeholder endpoint slice and no pods matching the service, the existing
// slice should not change the placeholder
// TestReconcile1EndpointSlice 验证在没有匹配 Pod 时对占位切片的处理：没有占位则创建，已有相同占位则保持不变，已有不同占位则更新。
// 断言新增 action 数量与期望一致、最终仍只有一个切片，并对无需更新的场景校验切片字段未被改动，同时比对指标。
func TestReconcile1EndpointSlice(t *testing.T) {
	namespace := "test"
	svc, epMeta := newServiceAndEndpointMeta("foo", namespace)
	// 构造标签与期望不同的空占位切片，用于触发更新场景。
	emptySlice := newEmptyEndpointSlice(1, namespace, epMeta, svc)
	emptySlice.ObjectMeta.Labels = map[string]string{"bar": "baz"}
	logger, _ := ktesting.NewTestContext(t)

	// 表驱动用例：无占位切片、已有相同占位切片、已有不同占位切片。
	testCases := []struct {
		desc        string
		existing    *discovery.EndpointSlice
		wantUpdate  bool
		wantMetrics expectedMetrics
	}{
		{
			desc:        "No existing placeholder",
			wantUpdate:  true,
			wantMetrics: expectedMetrics{desiredSlices: 1, actualSlices: 1, desiredEndpoints: 0, addedPerSync: 0, removedPerSync: 0, numCreated: 1, numUpdated: 0, numDeleted: 0, slicesChangedPerSync: 1},
		},
		{
			desc:        "Existing placeholder that's the same",
			existing:    newEndpointSlice(logger, &svc, &endpointMeta{ports: []discovery.EndpointPort{}, addressType: discovery.AddressTypeIPv4}, controllerName),
			wantMetrics: expectedMetrics{desiredSlices: 1, actualSlices: 1, desiredEndpoints: 0, addedPerSync: 0, removedPerSync: 0, numCreated: 0, numUpdated: 0, numDeleted: 0, slicesChangedPerSync: 0},
		},
		{
			desc:        "Existing placeholder that's different",
			existing:    emptySlice,
			wantUpdate:  true,
			wantMetrics: expectedMetrics{desiredSlices: 1, actualSlices: 1, desiredEndpoints: 0, addedPerSync: 0, removedPerSync: 0, numCreated: 0, numUpdated: 1, numDeleted: 0, slicesChangedPerSync: 1},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			client := newClientset()
			resetMetrics()

			// 按需把已有切片写入 fake clientset。
			existingSlices := []*discovery.EndpointSlice{}
			if tc.existing != nil {
				existingSlices = append(existingSlices, tc.existing)
				_, createErr := client.DiscoveryV1().EndpointSlices(namespace).Create(context.TODO(), tc.existing, metav1.CreateOptions{})
				assert.NoError(t, createErr, "Expected no error creating endpoint slice")
			}

			// 记录预置完成后的 action 数量，用于计算协调新增的 action。
			numActionsBefore := len(client.Actions())
			r := newReconciler(client, []*corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}}, defaultMaxEndpointsPerSlice)
			reconcileHelper(t, r, &svc, []*corev1.Pod{}, existingSlices, time.Now())

			// 根据是否期望更新，计算期望新增的 action 数量。
			var numUpdates int
			if tc.wantUpdate {
				numUpdates = 1
			}
			wantActions := numActionsBefore + numUpdates
			assert.Len(t, client.Actions(), wantActions, "Expected %d additional clientset actions", numUpdates)

			slices := fetchEndpointSlices(t, client, namespace)
			assert.Len(t, slices, 1, "Expected 1 endpoint slices")

			// 不需要更新时，继续校验占位切片的名称、标签、端口与端点保持正确。
			if !tc.wantUpdate {
				assert.Regexp(t, "^"+svc.Name, slices[0].Name)
				assert.Equal(t, svc.Name, slices[0].Labels[discovery.LabelServiceName])
				assert.EqualValues(t, []discovery.EndpointPort{}, slices[0].Ports)
				assert.EqualValues(t, []discovery.Endpoint{}, slices[0].Endpoints)
				if tc.existing == nil {
					expectTrackedGeneration(t, r.endpointSliceTracker, &slices[0], 1)
				}
			}
			expectMetrics(t, tc.wantMetrics)
		})
	}
}

// TestPlaceHolderSliceCompare 测试 placeholderSliceCompare 比较器的语义。
// 该比较器忽略 ObjectMeta.Name 与创建时间的差异，但标签不同或切片主体（如地址类型）不同则判定为不等；用例覆盖 nil、空对象及上述各差异情形。
func TestPlaceHolderSliceCompare(t *testing.T) {
	// 表驱动用例覆盖 nil、空对象、仅名称/创建时间不同以及标签/地址类型不同等情形。
	testCases := []struct {
		desc string
		x    *discovery.EndpointSlice
		y    *discovery.EndpointSlice
		want bool
	}{
		{
			desc: "Both nil",
			want: true,
		},
		{
			desc: "Y is nil",
			x:    &discovery.EndpointSlice{},
			want: false,
		},
		{
			desc: "X is nil",
			y:    &discovery.EndpointSlice{},
			want: false,
		},
		{
			desc: "Both are empty and non-nil",
			x:    &discovery.EndpointSlice{},
			y:    &discovery.EndpointSlice{},
			want: true,
		},
		{
			desc: "Only ObjectMeta.Name has diff",
			x: &discovery.EndpointSlice{ObjectMeta: metav1.ObjectMeta{
				Name: "foo",
			}},
			y: &discovery.EndpointSlice{ObjectMeta: metav1.ObjectMeta{
				Name: "bar",
			}},
			want: true,
		},
		{
			desc: "Only ObjectMeta.Labels has diff",
			x: &discovery.EndpointSlice{ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					"foo": "true",
				},
			}},
			y: &discovery.EndpointSlice{ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{
					"bar": "true",
				},
			}},
			want: false,
		},
		{
			desc: "Creation time is different",
			x: &discovery.EndpointSlice{ObjectMeta: metav1.ObjectMeta{
				CreationTimestamp: metav1.Unix(1, 0),
			}},
			y: &discovery.EndpointSlice{ObjectMeta: metav1.ObjectMeta{
				CreationTimestamp: metav1.Unix(2, 0),
			}},
			want: true,
		},
		{
			desc: "Different except for ObjectMeta",
			x:    &discovery.EndpointSlice{AddressType: discovery.AddressTypeIPv4},
			y:    &discovery.EndpointSlice{AddressType: discovery.AddressTypeIPv6},
			want: false,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			got := placeholderSliceCompare.DeepEqual(tc.x, tc.y)
			if got != tc.want {
				t.Errorf("sliceEqual(%v, %v) = %t, want %t", tc.x, tc.y, got, tc.want)
			}
		})
	}
}

// when a Service has PublishNotReadyAddresses set to true, corresponding
// Endpoints should be considered ready, even if the backing Pod is not.
// TestReconcile1EndpointSlicePublishNotReadyAddresses 验证 Service 开启 PublishNotReadyAddresses 后，即使底层 Pod 未就绪，对应端点也被视为 ready。
// 构造 50 个 Pod（约 1/3 未就绪），断言只创建 1 个包含全部 50 个端点的切片，且所有端点均为 ready。
func TestReconcile1EndpointSlicePublishNotReadyAddresses(t *testing.T) {
	client := newClientset()
	namespace := "test"
	// Service 开启 PublishNotReadyAddresses。
	svc, _ := newServiceAndEndpointMeta("foo", namespace)
	svc.Spec.PublishNotReadyAddresses = true

	// start with 50 pods, 1/3 not ready
	// 按 i%3 控制 Pod 就绪状态，使约 1/3 的 Pod 未就绪。
	pods := []*corev1.Pod{}
	for i := 0; i < 50; i++ {
		ready := i%3 != 0
		pods = append(pods, newPod(i, namespace, ready, 1, false))
	}

	// 使用默认每切片 100 个端点的上限，50 个端点可放入一个切片。
	r := newReconciler(client, []*corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}}, defaultMaxEndpointsPerSlice)
	reconcileHelper(t, r, &svc, pods, []*discovery.EndpointSlice{}, time.Now())

	// Only 1 action, an EndpointSlice create
	assert.Len(t, client.Actions(), 1, "Expected 1 additional clientset action")
	expectActions(t, client.Actions(), 1, "create", "endpointslices")

	// Two endpoint slices should be completely full, the remainder should be in another one
	endpointSlices := fetchEndpointSlices(t, client, namespace)
	for _, endpointSlice := range endpointSlices {
		// 即使底层 Pod 未就绪，端点也应为 ready。
		for i, endpoint := range endpointSlice.Endpoints {
			if !topologycache.EndpointReady(endpoint) {
				t.Errorf("Expected endpoints[%d] to be ready", i)
			}
		}
	}
	expectUnorderedSlicesWithLengths(t, endpointSlices, []int{50})
}

// a simple use case with 250 pods matching a service and no existing slices
// reconcile should create 3 slices, completely filling 2 of them
// TestReconcileManyPods 验证 250 个 Pod 且无已有切片时的基本装箱行为。
// 期望只产生 3 次 create，切片端点分布为 100/100/50，并校验新增端点数、创建数与变更次数等指标。
func TestReconcileManyPods(t *testing.T) {
	client := newClientset()
	resetMetrics()
	namespace := "test"
	svc, _ := newServiceAndEndpointMeta("foo", namespace)

	// start with 250 pods
	// 约 2/3 的 Pod 处于就绪状态。
	pods := []*corev1.Pod{}
	for i := 0; i < 250; i++ {
		ready := i%3 != 0
		pods = append(pods, newPod(i, namespace, ready, 1, false))
	}

	// 使用默认每切片 100 个端点的上限。
	r := newReconciler(client, []*corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}}, defaultMaxEndpointsPerSlice)
	reconcileHelper(t, r, &svc, pods, []*discovery.EndpointSlice{}, time.Now())

	// This is an ideal scenario where only 3 actions are required, and they're all creates
	// 理想情况下只需 3 次 create action。
	assert.Len(t, client.Actions(), 3, "Expected 3 additional clientset actions")
	expectActions(t, client.Actions(), 3, "create", "endpointslices")

	// Two endpoint slices should be completely full, the remainder should be in another one
	// 断言切片分布为 100/100/50。
	expectUnorderedSlicesWithLengths(t, fetchEndpointSlices(t, client, namespace), []int{100, 100, 50})
	expectMetrics(t, expectedMetrics{desiredSlices: 3, actualSlices: 3, desiredEndpoints: 250, addedPerSync: 250, removedPerSync: 0, numCreated: 3, numUpdated: 0, numDeleted: 0, slicesChangedPerSync: 3})
}

// now with preexisting slices, we have 250 pods matching a service
// the first endpoint slice contains 62 endpoints, all desired
// the second endpoint slice contains 61 endpoints, all desired
// that leaves 127 to add
// to minimize writes, our strategy is to create new slices for multiples of 100
// that leaves 27 to drop in an existing slice
// dropping them in the first slice will result in the slice being closest to full
// this approach requires 1 update + 1 create instead of 2 updates + 1 create
// TestReconcileEndpointSlicesSomePreexisting 验证 250 个 Pod 且已有两个各含约 1/4 端点的切片时的写入最小化策略。
// 剩余 127 个端点中，100 个放入新建切片、27 个放入更接近满载的旧切片，因此只需 1 次 create 加 1 次 update。
// 断言 action 顺序、最终切片长度 89/61/100、指标，并确认缓存对象未被就地修改。
func TestReconcileEndpointSlicesSomePreexisting(t *testing.T) {
	client := newClientset()
	resetMetrics()
	namespace := "test"
	svc, endpointMeta := newServiceAndEndpointMeta("foo", namespace)

	// start with 250 pods
	pods := []*corev1.Pod{}
	for i := 0; i < 250; i++ {
		ready := i%3 != 0
		pods = append(pods, newPod(i, namespace, ready, 1, false))
	}

	// have approximately 1/4 in first slice
	// 约 1/4 的 Pod 放入第一个已有切片。
	endpointSlice1 := newEmptyEndpointSlice(1, namespace, endpointMeta, svc)
	for i := 1; i < len(pods)-4; i += 4 {
		endpointSlice1.Endpoints = append(endpointSlice1.Endpoints, podToEndpoint(pods[i], &corev1.Node{}, &svc, discovery.AddressTypeIPv4))
	}

	// have approximately 1/4 in second slice
	// 约 1/4 的 Pod 放入第二个已有切片。
	endpointSlice2 := newEmptyEndpointSlice(2, namespace, endpointMeta, svc)
	for i := 3; i < len(pods)-4; i += 4 {
		endpointSlice2.Endpoints = append(endpointSlice2.Endpoints, podToEndpoint(pods[i], &corev1.Node{}, &svc, discovery.AddressTypeIPv4))
	}

	// 预置切片并监视其不被就地修改。
	existingSlices := []*discovery.EndpointSlice{endpointSlice1, endpointSlice2}
	cmc := newCacheMutationCheck(existingSlices)
	createEndpointSlices(t, client, namespace, existingSlices)

	numActionsBefore := len(client.Actions())
	r := newReconciler(client, []*corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}}, defaultMaxEndpointsPerSlice)
	reconcileHelper(t, r, &svc, pods, existingSlices, time.Now())

	// 期望新增 1 次 create 和 1 次 update，且顺序为 create 后 update。
	actions := client.Actions()
	assert.Len(t, actions, numActionsBefore+2, "Expected 2 additional client actions as part of reconcile")
	assert.True(t, actions[numActionsBefore].Matches("create", "endpointslices"), "First action should be create endpoint slice")
	assert.True(t, actions[numActionsBefore+1].Matches("update", "endpointslices"), "Second action should be update endpoint slice")

	// 1 new slice (0->100) + 1 updated slice (62->89)
	// 校验最终切片长度分布：89/61/100。
	expectUnorderedSlicesWithLengths(t, fetchEndpointSlices(t, client, namespace), []int{89, 61, 100})
	expectMetrics(t, expectedMetrics{desiredSlices: 3, actualSlices: 3, desiredEndpoints: 250, addedPerSync: 127, removedPerSync: 0, numCreated: 1, numUpdated: 1, numDeleted: 0, slicesChangedPerSync: 2})

	// ensure cache mutation has not occurred
	cmc.Check(t)
}

// now with preexisting slices, we have 300 pods matching a service
// this scenario will show some less ideal allocation
// the first endpoint slice contains 74 endpoints, all desired
// the second endpoint slice contains 74 endpoints, all desired
// that leaves 152 to add
// to minimize writes, our strategy is to create new slices for multiples of 100
// that leaves 52 to drop in an existing slice
// that capacity could fit if split in the 2 existing slices
// to minimize writes though, reconcile create a new slice with those 52 endpoints
// this approach requires 2 creates instead of 2 updates + 1 create
// TestReconcileEndpointSlicesSomePreexistingWorseAllocation 验证 300 个 Pod 且已有两个各含 74 个端点切片时的分配策略。
// 剩余 152 个端点不拆分填入旧切片，而是直接新建 100 与 52 两个切片：2 次 create 优于 2 次 update 加 1 次 create。
// 断言产生 2 次 create、最终长度为 74/74/100/52（实际切片数 4 大于理想值 3）、指标符合预期，且缓存未被修改。
func TestReconcileEndpointSlicesSomePreexistingWorseAllocation(t *testing.T) {
	client := newClientset()
	resetMetrics()
	namespace := "test"
	svc, endpointMeta := newServiceAndEndpointMeta("foo", namespace)

	// start with 300 pods
	pods := []*corev1.Pod{}
	for i := 0; i < 300; i++ {
		ready := i%3 != 0
		pods = append(pods, newPod(i, namespace, ready, 1, false))
	}

	// have approximately 1/4 in first slice
	endpointSlice1 := newEmptyEndpointSlice(1, namespace, endpointMeta, svc)
	for i := 1; i < len(pods)-4; i += 4 {
		endpointSlice1.Endpoints = append(endpointSlice1.Endpoints, podToEndpoint(pods[i], &corev1.Node{}, &svc, discovery.AddressTypeIPv4))
	}

	// have approximately 1/4 in second slice
	endpointSlice2 := newEmptyEndpointSlice(2, namespace, endpointMeta, svc)
	for i := 3; i < len(pods)-4; i += 4 {
		endpointSlice2.Endpoints = append(endpointSlice2.Endpoints, podToEndpoint(pods[i], &corev1.Node{}, &svc, discovery.AddressTypeIPv4))
	}

	// 预置两个各含 74 个端点的切片，并监视缓存不被修改。
	existingSlices := []*discovery.EndpointSlice{endpointSlice1, endpointSlice2}
	cmc := newCacheMutationCheck(existingSlices)
	createEndpointSlices(t, client, namespace, existingSlices)

	numActionsBefore := len(client.Actions())
	r := newReconciler(client, []*corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}}, defaultMaxEndpointsPerSlice)
	reconcileHelper(t, r, &svc, pods, existingSlices, time.Now())

	// 期望新增 2 次 create，没有 update。
	actions := client.Actions()
	assert.Len(t, actions, numActionsBefore+2, "Expected 2 additional client actions as part of reconcile")
	expectActions(t, client.Actions(), 2, "create", "endpointslices")

	// 2 new slices (100, 52) in addition to existing slices (74, 74)
	// 最终得到 4 个切片（74/74/100/52），实际切片数大于理想值 3。
	expectUnorderedSlicesWithLengths(t, fetchEndpointSlices(t, client, namespace), []int{74, 74, 100, 52})
	expectMetrics(t, expectedMetrics{desiredSlices: 3, actualSlices: 4, desiredEndpoints: 300, addedPerSync: 152, removedPerSync: 0, numCreated: 2, numUpdated: 0, numDeleted: 0, slicesChangedPerSync: 2})

	// ensure cache mutation has not occurred
	cmc.Check(t)
}

// In some cases, such as a service port change, all slices for that service will require a change
// This test ensures that we are updating those slices and not calling create + delete for each
// TestReconcileEndpointSlicesUpdating 验证 Service 端口变更导致所有切片都需要改变时，采用 update 而不是 create+delete。
// 先协调生成 3 个切片，修改 TargetPort 后再次协调；断言新增 3 次 update action，切片长度保持 100/100/50。
func TestReconcileEndpointSlicesUpdating(t *testing.T) {
	client := newClientset()
	namespace := "test"
	svc, _ := newServiceAndEndpointMeta("foo", namespace)

	// start with 250 pods
	pods := []*corev1.Pod{}
	for i := 0; i < 250; i++ {
		ready := i%3 != 0
		pods = append(pods, newPod(i, namespace, ready, 1, false))
	}

	// 首次协调生成 3 个切片。
	r := newReconciler(client, []*corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}}, defaultMaxEndpointsPerSlice)
	reconcileHelper(t, r, &svc, pods, []*discovery.EndpointSlice{}, time.Now())
	numActionsExpected := 3
	assert.Len(t, client.Actions(), numActionsExpected, "Expected 3 additional clientset actions")

	slices := fetchEndpointSlices(t, client, namespace)
	numActionsExpected++
	expectUnorderedSlicesWithLengths(t, slices, []int{100, 100, 50})

	// 修改 Service 目标端口，触发所有切片的顶层字段更新。
	svc.Spec.Ports[0].TargetPort.IntVal = 81
	// 再次协调时应复用现有切片并逐个 update。
	reconcileHelper(t, r, &svc, pods, []*discovery.EndpointSlice{&slices[0], &slices[1], &slices[2]}, time.Now())

	// 期望恰好 3 次 update，且没有 create/delete。
	numActionsExpected += 3
	assert.Len(t, client.Actions(), numActionsExpected, "Expected 3 additional clientset actions")
	expectActions(t, client.Actions(), 3, "update", "endpointslices")

	expectUnorderedSlicesWithLengths(t, fetchEndpointSlices(t, client, namespace), []int{100, 100, 50})
}

// In some cases, such as service labels updates, all slices for that service will require a change
// This test ensures that we are updating those slices and not calling create + delete for each
// TestReconcileEndpointSlicesServicesLabelsUpdating 验证 Service 标签变更会传播到其所有切片，且同样采用 update。
// 首次协调生成 3 个切片，修改 Service 标签后再次协调；断言 3 次 update，且每个新切片都带有 foo=bar 标签。
func TestReconcileEndpointSlicesServicesLabelsUpdating(t *testing.T) {
	client := newClientset()
	namespace := "test"
	svc, _ := newServiceAndEndpointMeta("foo", namespace)

	// start with 250 pods
	pods := []*corev1.Pod{}
	for i := 0; i < 250; i++ {
		ready := i%3 != 0
		pods = append(pods, newPod(i, namespace, ready, 1, false))
	}

	// 首次协调生成 3 个切片。
	r := newReconciler(client, []*corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}}, defaultMaxEndpointsPerSlice)
	reconcileHelper(t, r, &svc, pods, []*discovery.EndpointSlice{}, time.Now())
	numActionsExpected := 3
	assert.Len(t, client.Actions(), numActionsExpected, "Expected 3 additional clientset actions")

	slices := fetchEndpointSlices(t, client, namespace)
	numActionsExpected++
	expectUnorderedSlicesWithLengths(t, slices, []int{100, 100, 50})

	// update service with new labels
	// 修改 Service 标签后，期望所有切片被更新并继承新标签。
	svc.Labels = map[string]string{"foo": "bar"}
	reconcileHelper(t, r, &svc, pods, []*discovery.EndpointSlice{&slices[0], &slices[1], &slices[2]}, time.Now())

	numActionsExpected += 3
	assert.Len(t, client.Actions(), numActionsExpected, "Expected 3 additional clientset actions")
	expectActions(t, client.Actions(), 3, "update", "endpointslices")

	newSlices := fetchEndpointSlices(t, client, namespace)
	expectUnorderedSlicesWithLengths(t, newSlices, []int{100, 100, 50})
	// check that the labels were updated
	// 遍历新切片，确认 foo=bar 标签已传播。
	for _, slice := range newSlices {
		w, ok := slice.Labels["foo"]
		if !ok {
			t.Errorf("Expected label \"foo\" from parent service not found")
		} else if "bar" != w {
			t.Errorf("Expected EndpointSlice to have parent service labels: have %s value, expected bar", w)
		}
	}
}

// In some cases, such as service labels updates, all slices for that service will require a change
// However, this should not happen for reserved labels
// TestReconcileEndpointSlicesServicesReservedLabels 验证 Service 上设置的保留标签不应传播到 EndpointSlice。
// 首次协调生成 3 个切片，随后把保留标签写进 Service 并再次协调；断言不产生任何额外 action，切片长度不变。
func TestReconcileEndpointSlicesServicesReservedLabels(t *testing.T) {
	client := newClientset()
	namespace := "test"
	svc, _ := newServiceAndEndpointMeta("foo", namespace)

	// start with 250 pods
	pods := []*corev1.Pod{}
	for i := 0; i < 250; i++ {
		ready := i%3 != 0
		pods = append(pods, newPod(i, namespace, ready, 1, false))
	}

	// 首次协调生成 3 个切片。
	r := newReconciler(client, []*corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}}, defaultMaxEndpointsPerSlice)
	reconcileHelper(t, r, &svc, pods, []*discovery.EndpointSlice{}, time.Now())
	numActionsExpected := 3
	assert.Len(t, client.Actions(), numActionsExpected, "Expected 3 additional clientset actions")
	slices := fetchEndpointSlices(t, client, namespace)
	numActionsExpected++
	expectUnorderedSlicesWithLengths(t, slices, []int{100, 100, 50})

	// update service with new labels
	// 使用保留标签作为 Service 标签，这些标签不应传播到切片。
	svc.Labels = map[string]string{discovery.LabelServiceName: "bad", discovery.LabelManagedBy: "actor", corev1.IsHeadlessService: "invalid"}
	// 再次协调不应产生任何写操作。
	reconcileHelper(t, r, &svc, pods, []*discovery.EndpointSlice{&slices[0], &slices[1], &slices[2]}, time.Now())
	assert.Len(t, client.Actions(), numActionsExpected, "Expected no additional clientset actions")

	newSlices := fetchEndpointSlices(t, client, namespace)
	expectUnorderedSlicesWithLengths(t, newSlices, []int{100, 100, 50})
}

// In this test, we start with 10 slices that only have 30 endpoints each
// An initial reconcile makes no changes (as desired to limit writes)
// When we change a service port, all slices will need to be updated in some way
// reconcile repacks the endpoints into 3 slices, and deletes the extras
// TestReconcileEndpointSlicesRecycling 验证切片回收（recycling）带来的免费重新装箱。
// 构造 10 个各含 30 个端点的切片：首次协调为 no-op；修改端口后所有切片都需变更，协调器将其重新装箱为 3 个满载切片，产生 3 次 update 与 7 次 delete。
// 断言首次无 action、第二次共 10 次 action、最终长度 100/100/100、指标，并确认缓存未被修改。
func TestReconcileEndpointSlicesRecycling(t *testing.T) {
	client := newClientset()
	resetMetrics()
	namespace := "test"
	svc, endpointMeta := newServiceAndEndpointMeta("foo", namespace)

	// start with 300 pods
	pods := []*corev1.Pod{}
	for i := 0; i < 300; i++ {
		ready := i%3 != 0
		pods = append(pods, newPod(i, namespace, ready, 1, false))
	}

	// generate 10 existing slices with 30 pods/endpoints each
	// 按每 30 个端点划分为 10 个切片。
	existingSlices := []*discovery.EndpointSlice{}
	for i, pod := range pods {
		sliceNum := i / 30
		if i%30 == 0 {
			existingSlices = append(existingSlices, newEmptyEndpointSlice(sliceNum, namespace, endpointMeta, svc))
		}
		existingSlices[sliceNum].Endpoints = append(existingSlices[sliceNum].Endpoints, podToEndpoint(pod, &corev1.Node{}, &svc, discovery.AddressTypeIPv4))
	}

	// 预置切片并监视缓存不被修改。
	cmc := newCacheMutationCheck(existingSlices)
	createEndpointSlices(t, client, namespace, existingSlices)

	// 记录预置后的 action 数量。
	numActionsBefore := len(client.Actions())
	r := newReconciler(client, []*corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}}, defaultMaxEndpointsPerSlice)
	// 首次协调应为 no-op。
	reconcileHelper(t, r, &svc, pods, existingSlices, time.Now())
	// initial reconcile should be a no op, all pods are accounted for in slices, no repacking should be done
	// 断言首次协调没有产生额外 action。
	assert.Len(t, client.Actions(), numActionsBefore+0, "Expected 0 additional client actions as part of reconcile")

	// changing a service port should require all slices to be updated, time for a repack
	// 修改端口迫使所有切片更新，从而触发重新装箱。
	svc.Spec.Ports[0].TargetPort.IntVal = 81
	reconcileHelper(t, r, &svc, pods, existingSlices, time.Now())

	// this should reflect 3 updates + 7 deletes
	// 期望 3 次 update 与 7 次 delete。
	assert.Len(t, client.Actions(), numActionsBefore+10, "Expected 10 additional client actions as part of reconcile")

	// thanks to recycling, we get a free repack of endpoints, resulting in 3 full slices instead of 10 mostly empty slices
	// 最终得到 3 个满载切片。
	expectUnorderedSlicesWithLengths(t, fetchEndpointSlices(t, client, namespace), []int{100, 100, 100})
	expectMetrics(t, expectedMetrics{desiredSlices: 3, actualSlices: 3, desiredEndpoints: 300, addedPerSync: 300, removedPerSync: 0, numCreated: 0, numUpdated: 3, numDeleted: 7, slicesChangedPerSync: 10})

	// ensure cache mutation has not occurred
	cmc.Check(t)
}

// In this test, we want to verify that endpoints are added to a slice that will
// be closest to full after the operation, even when slices are already marked
// for update.
// TestReconcileEndpointSlicesUpdatePacking 验证新增端点会被放入操作后最接近满载的切片，即使两个切片都已标记为需要更新。
// 预置 80 与 20 个端点的两个切片，周期性地把 Pod 标记为未就绪以强制两个切片都更新，再加 15 个新端点。
// 断言两个切片都被 update，新端点进入更满的切片，最终长度为 95/20，并确认缓存未被修改。
func TestReconcileEndpointSlicesUpdatePacking(t *testing.T) {
	client := newClientset()
	resetMetrics()
	namespace := "test"
	svc, endpointMeta := newServiceAndEndpointMeta("foo", namespace)

	existingSlices := []*discovery.EndpointSlice{}
	pods := []*corev1.Pod{}

	// 第一个切片预置 80 个端点。
	slice1 := newEmptyEndpointSlice(1, namespace, endpointMeta, svc)
	for i := 0; i < 80; i++ {
		pod := newPod(i, namespace, true, 1, false)
		slice1.Endpoints = append(slice1.Endpoints, podToEndpoint(pod, &corev1.Node{}, &svc, discovery.AddressTypeIPv4))
		pods = append(pods, pod)
	}
	existingSlices = append(existingSlices, slice1)

	// 第二个切片预置 20 个端点（编号 100-119）。
	slice2 := newEmptyEndpointSlice(2, namespace, endpointMeta, svc)
	for i := 100; i < 120; i++ {
		pod := newPod(i, namespace, true, 1, false)
		slice2.Endpoints = append(slice2.Endpoints, podToEndpoint(pod, &corev1.Node{}, &svc, discovery.AddressTypeIPv4))
		pods = append(pods, pod)
	}
	existingSlices = append(existingSlices, slice2)

	// 预置切片并监视缓存不被修改。
	cmc := newCacheMutationCheck(existingSlices)
	createEndpointSlices(t, client, namespace, existingSlices)

	// ensure that endpoints in each slice will be marked for update.
	// 把部分 Pod 标记为未就绪，强制两个切片都需要更新。
	for i, pod := range pods {
		if i%10 == 0 {
			pod.Status.Conditions = []corev1.PodCondition{{
				Type:   corev1.PodReady,
				Status: corev1.ConditionFalse,
			}}
		}
	}

	// add a few additional endpoints - no more than could fit in either slice.
	// 新增 15 个端点，数量足以完全放入任一现有切片。
	for i := 200; i < 215; i++ {
		pods = append(pods, newPod(i, namespace, true, 1, false))
	}

	r := newReconciler(client, []*corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}}, defaultMaxEndpointsPerSlice)
	reconcileHelper(t, r, &svc, pods, existingSlices, time.Now())

	// ensure that both endpoint slices have been updated
	// 期望两个切片都被 update。
	expectActions(t, client.Actions(), 2, "update", "endpointslices")
	expectMetrics(t, expectedMetrics{desiredSlices: 2, actualSlices: 2, desiredEndpoints: 115, addedPerSync: 15, removedPerSync: 0, numCreated: 0, numUpdated: 2, numDeleted: 0, slicesChangedPerSync: 2})

	// additional pods should get added to fuller slice
	// 新端点应进入更新后更接近满载的切片，得到 95/20 的分布。
	expectUnorderedSlicesWithLengths(t, fetchEndpointSlices(t, client, namespace), []int{95, 20})

	// ensure cache mutation has not occurred
	cmc.Check(t)
}

// In this test, we want to verify that old EndpointSlices with a deprecated IP
// address type will be replaced with a newer IPv4 type.
// TestReconcileEndpointSlicesReplaceDeprecated 验证地址类型为已废弃值 "IP" 的旧切片会被新的 IPv4 切片替换。
// 预置两个使用废弃地址类型、但端点实际为 IPv4 地址的切片；协调后断言产生 2 次 delete，新切片被完美装箱为 100/30 且地址类型均为 IPv4，同时确认缓存未被修改。
func TestReconcileEndpointSlicesReplaceDeprecated(t *testing.T) {
	client := newClientset()
	resetMetrics()
	namespace := "test"

	// Service 使用普通 IPv4 配置，但把已有切片的地址类型设为已废弃值。
	svc, endpointMeta := newServiceAndEndpointMeta("foo", namespace)
	// "IP" is a deprecated address type, ensuring that it is handled properly.
	// 已有切片端点本身仍是 IPv4 地址。
	endpointMeta.addressType = discovery.AddressType("IP")

	existingSlices := []*discovery.EndpointSlice{}
	pods := []*corev1.Pod{}

	// 第一个旧切片含 80 个端点。
	slice1 := newEmptyEndpointSlice(1, namespace, endpointMeta, svc)
	for i := 0; i < 80; i++ {
		pod := newPod(i, namespace, true, 1, false)
		slice1.Endpoints = append(slice1.Endpoints, podToEndpoint(pod, &corev1.Node{}, &corev1.Service{Spec: corev1.ServiceSpec{}}, discovery.AddressTypeIPv4))
		pods = append(pods, pod)
	}
	existingSlices = append(existingSlices, slice1)

	// 第二个旧切片含 50 个端点。
	slice2 := newEmptyEndpointSlice(2, namespace, endpointMeta, svc)
	for i := 100; i < 150; i++ {
		pod := newPod(i, namespace, true, 1, false)
		slice2.Endpoints = append(slice2.Endpoints, podToEndpoint(pod, &corev1.Node{}, &corev1.Service{Spec: corev1.ServiceSpec{}}, discovery.AddressTypeIPv4))
		pods = append(pods, pod)
	}
	existingSlices = append(existingSlices, slice2)

	// 预置两个使用废弃地址类型的切片。
	createEndpointSlices(t, client, namespace, existingSlices)

	cmc := newCacheMutationCheck(existingSlices)
	r := newReconciler(client, []*corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}}, defaultMaxEndpointsPerSlice)
	reconcileHelper(t, r, &svc, pods, existingSlices, time.Now())
	// 协调后旧切片应被新的 IPv4 切片替换。

	// ensure that both original endpoint slices have been deleted
	// 期望产生两次 delete action。
	expectActions(t, client.Actions(), 2, "delete", "endpointslices")

	endpointSlices := fetchEndpointSlices(t, client, namespace)

	// since this involved replacing both EndpointSlices, the result should be
	// 新切片应被完美装箱为 100/30。
	// perfectly packed.
	expectUnorderedSlicesWithLengths(t, endpointSlices, []int{100, 30})

	// 断言所有新切片的地址类型均为 IPv4。
	for _, endpointSlice := range endpointSlices {
		if endpointSlice.AddressType != discovery.AddressTypeIPv4 {
			t.Errorf("Expected address type to be IPv4, got %s", endpointSlice.AddressType)
		}
	}

	// ensure cache mutation has not occurred
	cmc.Check(t)
}

// In this test, we want to verify that a Service recreation will result in new
// EndpointSlices being created.
// TestReconcileEndpointSlicesRecreation 验证 Service 重建（UID 变化）后，属于旧 UID 的切片会被替换。
// 表驱动用例分别构造 ownerReference UID 与当前 Service 一致和不一致的切片；一致时不产生写操作，不一致时先 create 新切片再 delete 旧切片，最后只剩 1 个切片。
func TestReconcileEndpointSlicesRecreation(t *testing.T) {
	// 表驱动用例按切片 ownerReference 的 UID 是否属于当前 Service 分组。
	testCases := []struct {
		name           string
		ownedByService bool
		expectChanges  bool
	}{
		{
			name:           "slice owned by Service",
			ownedByService: true,
			expectChanges:  false,
		}, {
			name:           "slice owned by other Service UID",
			ownedByService: false,
			expectChanges:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client := newClientset()
			resetMetrics()
			namespace := "test"

			// 构造 Service 与一个带端点的切片。
			svc, endpointMeta := newServiceAndEndpointMeta("foo", namespace)
			slice := newEmptyEndpointSlice(1, namespace, endpointMeta, svc)

			pod := newPod(1, namespace, true, 1, false)
			slice.Endpoints = append(slice.Endpoints, podToEndpoint(pod, &corev1.Node{}, &corev1.Service{Spec: corev1.ServiceSpec{}}, discovery.AddressTypeIPv4))

			// 篡改 UID 模拟 Service 重建后旧切片归属于其他实例。
			if !tc.ownedByService {
				slice.OwnerReferences[0].UID = "different"
			}
			existingSlices := []*discovery.EndpointSlice{slice}
			createEndpointSlices(t, client, namespace, existingSlices)

			cmc := newCacheMutationCheck(existingSlices)

			numActionsBefore := len(client.Actions())
			r := newReconciler(client, []*corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}}, defaultMaxEndpointsPerSlice)
			reconcileHelper(t, r, &svc, []*corev1.Pod{pod}, existingSlices, time.Now())

			// 归属不一致时应新建切片并删除旧切片。
			if tc.expectChanges {
				if len(client.Actions()) != numActionsBefore+2 {
					t.Fatalf("Expected 2 additional actions, got %d", len(client.Actions())-numActionsBefore)
				}

				expectAction(t, client.Actions(), numActionsBefore, "create", "endpointslices")
				expectAction(t, client.Actions(), numActionsBefore+1, "delete", "endpointslices")

				fetchedSlices := fetchEndpointSlices(t, client, namespace)

				if len(fetchedSlices) != 1 {
					t.Fatalf("Expected 1 EndpointSlice to exist, got %d", len(fetchedSlices))
				}
			} else {
				// 归属一致时不应产生任何写操作。
				if len(client.Actions()) != numActionsBefore {
					t.Errorf("Expected no additional actions, got %d", len(client.Actions())-numActionsBefore)
				}
			}
			// ensure cache mutation has not occurred
			cmc.Check(t)
		})
	}
}

// Named ports can map to different port numbers on different pods.
// This test ensures that EndpointSlices are grouped correctly in that case.
// TestReconcileEndpointSlicesNamedPorts 验证命名端口映射到不同端口号时切片的分组行为。
// Service 使用名为 http 的目标端口，300 个 Pod 的容器端口按 i%5 产生 5 种映射；期望创建 5 个各含 60 个端点的切片，每个切片对应一种端口号，并校验端口与地址类型。
func TestReconcileEndpointSlicesNamedPorts(t *testing.T) {
	client := newClientset()
	resetMetrics()
	namespace := "test"

	// Service 通过名为 http 的端口匹配容器端口。
	portNameIntStr := intstr.IntOrString{
		Type:   intstr.String,
		StrVal: "http",
	}

	// 构造使用命名目标端口的 Service。
	svc := corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "named-port-example", Namespace: namespace},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{{
				TargetPort: portNameIntStr,
				Protocol:   corev1.ProtocolTCP,
			}},
			Selector:   map[string]string{"foo": "bar"},
			IPFamilies: []corev1.IPFamily{corev1.IPv4Protocol},
		},
	}

	// start with 300 pods
	// 每个 Pod 的容器端口按 i%5 取不同值。
	pods := []*corev1.Pod{}
	for i := 0; i < 300; i++ {
		ready := i%3 != 0
		portOffset := i % 5
		pod := newPod(i, namespace, ready, 1, false)
		pod.Spec.Containers[0].Ports = []corev1.ContainerPort{{
			Name:          portNameIntStr.StrVal,
			ContainerPort: int32(8080 + portOffset),
			Protocol:      corev1.ProtocolTCP,
		}}
		pods = append(pods, pod)
	}

	// 使用默认上限进行协调。
	r := newReconciler(client, []*corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}}, defaultMaxEndpointsPerSlice)
	reconcileHelper(t, r, &svc, pods, []*discovery.EndpointSlice{}, time.Now())

	// reconcile should create 5 endpoint slices
	// 期望生成 5 个 create action。
	assert.Len(t, client.Actions(), 5, "Expected 5 client actions as part of reconcile")
	expectActions(t, client.Actions(), 5, "create", "endpointslices")
	expectMetrics(t, expectedMetrics{desiredSlices: 5, actualSlices: 5, desiredEndpoints: 300, addedPerSync: 300, removedPerSync: 0, numCreated: 5, numUpdated: 0, numDeleted: 0, slicesChangedPerSync: 5})

	fetchedSlices := fetchEndpointSlices(t, client, namespace)

	// each slice should have 60 endpoints to match 5 unique variations of named port mapping
	// 断言 5 个切片各含 60 个端点。
	expectUnorderedSlicesWithLengths(t, fetchedSlices, []int{60, 60, 60, 60, 60})

	// generate data structures for expected slice ports and address types
	// 为 5 种端口映射构造期望的顶层属性。
	expectedSlices := []discovery.EndpointSlice{}
	for i := range fetchedSlices {
		expectedSlices = append(expectedSlices, discovery.EndpointSlice{
			Ports: []discovery.EndpointPort{{
				Name:     ptr.To(""),
				Protocol: ptr.To(corev1.ProtocolTCP),
				Port:     ptr.To[int32](int32(8080 + i)),
			}},
			AddressType: discovery.AddressTypeIPv4,
		})
	}

	// slices fetched should match expected address type and ports
	// 校验切片的端口与地址类型符合预期。
	expectUnorderedSlicesWithTopLevelAttrs(t, fetchedSlices, expectedSlices)
}

// This test ensures that maxEndpointsPerSlice configuration results in
// appropriate endpoints distribution among slices
// TestReconcileMaxEndpointsPerSlice 验证 maxEndpointsPerSlice 配置对端点分布的影响。
// 固定 250 个 Pod，表驱动用例覆盖上限 50/80/150/250/500，断言切片长度分布分别为 50x5、80x3+10、150+100、250、250，并校验期望切片数、创建数等指标。
func TestReconcileMaxEndpointsPerSlice(t *testing.T) {
	namespace := "test"
	svc, _ := newServiceAndEndpointMeta("foo", namespace)

	// start with 250 pods
	// 250 个 Pod 作为固定输入。
	pods := []*corev1.Pod{}
	for i := 0; i < 250; i++ {
		ready := i%3 != 0
		pods = append(pods, newPod(i, namespace, ready, 1, false))
	}

	// 表驱动用例覆盖 50/80/150/250/500 五种上限。
	testCases := []struct {
		maxEndpointsPerSlice int32
		expectedSliceLengths []int
		expectedMetricValues expectedMetrics
	}{
		{
			maxEndpointsPerSlice: int32(50),
			expectedSliceLengths: []int{50, 50, 50, 50, 50},
			expectedMetricValues: expectedMetrics{desiredSlices: 5, actualSlices: 5, desiredEndpoints: 250, addedPerSync: 250, numCreated: 5, slicesChangedPerSync: 5},
		}, {
			maxEndpointsPerSlice: int32(80),
			expectedSliceLengths: []int{80, 80, 80, 10},
			expectedMetricValues: expectedMetrics{desiredSlices: 4, actualSlices: 4, desiredEndpoints: 250, addedPerSync: 250, numCreated: 4, slicesChangedPerSync: 4},
		}, {
			maxEndpointsPerSlice: int32(150),
			expectedSliceLengths: []int{150, 100},
			expectedMetricValues: expectedMetrics{desiredSlices: 2, actualSlices: 2, desiredEndpoints: 250, addedPerSync: 250, numCreated: 2, slicesChangedPerSync: 2},
		}, {
			maxEndpointsPerSlice: int32(250),
			expectedSliceLengths: []int{250},
			expectedMetricValues: expectedMetrics{desiredSlices: 1, actualSlices: 1, desiredEndpoints: 250, addedPerSync: 250, numCreated: 1, slicesChangedPerSync: 1},
		}, {
			maxEndpointsPerSlice: int32(500),
			expectedSliceLengths: []int{250},
			expectedMetricValues: expectedMetrics{desiredSlices: 1, actualSlices: 1, desiredEndpoints: 250, addedPerSync: 250, numCreated: 1, slicesChangedPerSync: 1},
		},
	}

	// 逐上限执行协调并断言切片长度分布与指标。
	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("maxEndpointsPerSlice: %d", testCase.maxEndpointsPerSlice), func(t *testing.T) {
			client := newClientset()
			resetMetrics()
			r := newReconciler(client, []*corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}}, testCase.maxEndpointsPerSlice)
			reconcileHelper(t, r, &svc, pods, []*discovery.EndpointSlice{}, time.Now())
			expectUnorderedSlicesWithLengths(t, fetchEndpointSlices(t, client, namespace), testCase.expectedSliceLengths)
			expectMetrics(t, testCase.expectedMetricValues)
		})
	}
}

// TestReconcileEndpointSlicesMetrics 验证两次连续协调后各项指标的变化。
// 首次用 20 个 Pod 创建 1 个切片；第二次只保留前 10 个 Pod，期望移除 10 个端点、更新 1 次，而新增端点数与创建数保留首次协调的累计值。
func TestReconcileEndpointSlicesMetrics(t *testing.T) {
	client := newClientset()
	resetMetrics()
	namespace := "test"
	svc, _ := newServiceAndEndpointMeta("foo", namespace)

	// start with 20 pods
	// 构造 20 个就绪 Pod。
	pods := []*corev1.Pod{}
	for i := 0; i < 20; i++ {
		pods = append(pods, newPod(i, namespace, true, 1, false))
	}

	// 首次协调创建包含 20 个端点的单个切片。
	r := newReconciler(client, []*corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}}, defaultMaxEndpointsPerSlice)
	reconcileHelper(t, r, &svc, pods, []*discovery.EndpointSlice{}, time.Now())

	actions := client.Actions()
	assert.Len(t, actions, 1, "Expected 1 additional client actions as part of reconcile")
	assert.True(t, actions[0].Matches("create", "endpointslices"), "First action should be create endpoint slice")

	expectMetrics(t, expectedMetrics{desiredSlices: 1, actualSlices: 1, desiredEndpoints: 20, addedPerSync: 20, removedPerSync: 0, numCreated: 1, numUpdated: 0, numDeleted: 0, slicesChangedPerSync: 1})

	// 第二次协调仅保留前 10 个 Pod。
	fetchedSlices := fetchEndpointSlices(t, client, namespace)
	reconcileHelper(t, r, &svc, pods[0:10], []*discovery.EndpointSlice{&fetchedSlices[0]}, time.Now())
	expectMetrics(t, expectedMetrics{desiredSlices: 1, actualSlices: 1, desiredEndpoints: 10, addedPerSync: 20, removedPerSync: 10, numCreated: 1, numUpdated: 1, numDeleted: 0, slicesChangedPerSync: 2})
}

// TestReconcileEndpointSlicesMetrics_EndpointsAddedPerSync_CollectAndCompare 验证新增端点数直方图的采集结果。
// 已有切片含 1 个应保留端点，协调后新增 1 个端点；用 CollectAndCompare 断言直方图 sum/count 均为 1。
func TestReconcileEndpointSlicesMetrics_EndpointsAddedPerSync_CollectAndCompare(t *testing.T) {
	resetMetrics()
	client := newClientset()
	namespace := "test"
	svc, endpointMeta := newServiceAndEndpointMeta("foo", namespace)

	// 一个应保留的端点与一个新增的期望 Pod。
	keepPod := newPod(1, namespace, true, 1, false)
	newDesiredPod := newPod(2, namespace, true, 1, false)

	// Existing slice has one endpoint that should remain. Reconcile should add exactly 1 endpoint.
	// 已有切片仅含 keepPod，期望协调后新增 1 个端点。
	existingSlice := newEmptyEndpointSlice(1, namespace, endpointMeta, svc)
	existingSlice.Endpoints = []discovery.Endpoint{
		podToEndpoint(keepPod, &corev1.Node{}, &svc, discovery.AddressTypeIPv4),
	}
	createdSlice, err := client.DiscoveryV1().EndpointSlices(namespace).Create(context.TODO(), existingSlice, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Expected no error creating EndpointSlice, got: %v", err)
	}

	// 执行协调后检查直方图。
	r := newReconciler(client, []*corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}}, defaultMaxEndpointsPerSlice)
	reconcileHelper(t, r, &svc, []*corev1.Pod{keepPod, newDesiredPod}, []*discovery.EndpointSlice{createdSlice}, time.Now())

	want := `
# HELP endpoint_slice_controller_endpoints_added_per_sync [BETA] Number of endpoints added on each Service sync
# TYPE endpoint_slice_controller_endpoints_added_per_sync histogram
endpoint_slice_controller_endpoints_added_per_sync_bucket{le="2"} 1
endpoint_slice_controller_endpoints_added_per_sync_bucket{le="4"} 1
endpoint_slice_controller_endpoints_added_per_sync_bucket{le="8"} 1
endpoint_slice_controller_endpoints_added_per_sync_bucket{le="16"} 1
endpoint_slice_controller_endpoints_added_per_sync_bucket{le="32"} 1
endpoint_slice_controller_endpoints_added_per_sync_bucket{le="64"} 1
endpoint_slice_controller_endpoints_added_per_sync_bucket{le="128"} 1
endpoint_slice_controller_endpoints_added_per_sync_bucket{le="256"} 1
endpoint_slice_controller_endpoints_added_per_sync_bucket{le="512"} 1
endpoint_slice_controller_endpoints_added_per_sync_bucket{le="1024"} 1
endpoint_slice_controller_endpoints_added_per_sync_bucket{le="2048"} 1
endpoint_slice_controller_endpoints_added_per_sync_bucket{le="4096"} 1
endpoint_slice_controller_endpoints_added_per_sync_bucket{le="8192"} 1
endpoint_slice_controller_endpoints_added_per_sync_bucket{le="16384"} 1
endpoint_slice_controller_endpoints_added_per_sync_bucket{le="32768"} 1
endpoint_slice_controller_endpoints_added_per_sync_bucket{le="+Inf"} 1
endpoint_slice_controller_endpoints_added_per_sync_sum 1
endpoint_slice_controller_endpoints_added_per_sync_count 1
`

	if err := testutil.CollectAndCompare(metrics.EndpointsAddedPerSync, strings.NewReader(want), "endpoint_slice_controller_endpoints_added_per_sync"); err != nil {
		t.Fatal(err)
	}
}

// TestReconcileEndpointSlicesMetrics_EndpointsRemovedPerSync_CollectAndCompare 验证移除端点数直方图的采集结果。
// 已有切片含 1 个过期端点和 1 个应保留端点，协调后恰好移除 1 个端点；断言直方图 sum/count 均为 1。
func TestReconcileEndpointSlicesMetrics_EndpointsRemovedPerSync_CollectAndCompare(t *testing.T) {
	resetMetrics()

	client := newClientset()
	namespace := "test"
	svc, endpointMeta := newServiceAndEndpointMeta("foo", namespace)

	// 一个过期端点与一个应保留端点。
	stalePod := newPod(0, namespace, true, 1, false)
	keepPod := newPod(1, namespace, true, 1, false)

	// Existing slice has one stale endpoint and one endpoint that should remain.
	// 期望协调后只保留 keepPod，恰好移除 1 个端点。
	// Reconcile should remove exactly 1 endpoint.
	existingSlice := newEmptyEndpointSlice(1, namespace, endpointMeta, svc)
	existingSlice.Endpoints = []discovery.Endpoint{
		podToEndpoint(stalePod, &corev1.Node{}, &svc, discovery.AddressTypeIPv4),
		podToEndpoint(keepPod, &corev1.Node{}, &svc, discovery.AddressTypeIPv4),
	}
	createdSlice, err := client.DiscoveryV1().EndpointSlices(namespace).Create(context.TODO(), existingSlice, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Expected no error creating EndpointSlice, got: %v", err)
	}

	// 执行协调后检查移除直方图。
	r := newReconciler(client, []*corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}}, defaultMaxEndpointsPerSlice)
	reconcileHelper(t, r, &svc, []*corev1.Pod{keepPod}, []*discovery.EndpointSlice{createdSlice}, time.Now())

	want := `
# HELP endpoint_slice_controller_endpoints_removed_per_sync [BETA] Number of endpoints removed on each Service sync
# TYPE endpoint_slice_controller_endpoints_removed_per_sync histogram
endpoint_slice_controller_endpoints_removed_per_sync_bucket{le="2"} 1
endpoint_slice_controller_endpoints_removed_per_sync_bucket{le="4"} 1
endpoint_slice_controller_endpoints_removed_per_sync_bucket{le="8"} 1
endpoint_slice_controller_endpoints_removed_per_sync_bucket{le="16"} 1
endpoint_slice_controller_endpoints_removed_per_sync_bucket{le="32"} 1
endpoint_slice_controller_endpoints_removed_per_sync_bucket{le="64"} 1
endpoint_slice_controller_endpoints_removed_per_sync_bucket{le="128"} 1
endpoint_slice_controller_endpoints_removed_per_sync_bucket{le="256"} 1
endpoint_slice_controller_endpoints_removed_per_sync_bucket{le="512"} 1
endpoint_slice_controller_endpoints_removed_per_sync_bucket{le="1024"} 1
endpoint_slice_controller_endpoints_removed_per_sync_bucket{le="2048"} 1
endpoint_slice_controller_endpoints_removed_per_sync_bucket{le="4096"} 1
endpoint_slice_controller_endpoints_removed_per_sync_bucket{le="8192"} 1
endpoint_slice_controller_endpoints_removed_per_sync_bucket{le="16384"} 1
endpoint_slice_controller_endpoints_removed_per_sync_bucket{le="32768"} 1
endpoint_slice_controller_endpoints_removed_per_sync_bucket{le="+Inf"} 1
endpoint_slice_controller_endpoints_removed_per_sync_sum 1
endpoint_slice_controller_endpoints_removed_per_sync_count 1
`

	if err := testutil.CollectAndCompare(metrics.EndpointsRemovedPerSync, strings.NewReader(want), "endpoint_slice_controller_endpoints_removed_per_sync"); err != nil {
		t.Fatal(err)
	}
}

// TestReconcileEndpointSlicesMetrics_GaugeUpdates 验证 gauge 类指标在协调后的取值。
// 创建带 PreferClose trafficDistribution 的 Service，用恰好 2 个 Pod 协调；断言期望/实际切片数、期望端点数以及按 trafficDistribution 统计的 Service 数均为确定值。
func TestReconcileEndpointSlicesMetrics_GaugeUpdates(t *testing.T) {
	resetMetrics()
	client := newClientset()
	namespace := "test"
	svc, _ := newServiceAndEndpointMeta("foo", namespace)

	// 构造协调器。
	r := newReconciler(client, []*corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}}, defaultMaxEndpointsPerSlice)

	// 设置 trafficDistribution 以验证对应的 Service 计数指标。
	svc.Spec.TrafficDistribution = ptr.To(corev1.ServiceTrafficDistributionPreferClose)
	if _, err := client.CoreV1().Services(namespace).Create(context.TODO(), &svc, metav1.CreateOptions{}); err != nil {
		t.Fatalf("Expected no error creating Service, got: %v", err)
	}

	// Reconcile with exactly 2 pods to deterministically set gauge metric values.
	// 使用恰好 2 个 Pod，使 gauge 值可精确断言。
	pod1 := newPod(1, namespace, true, 1, false)
	pod2 := newPod(2, namespace, true, 1, false)
	// 用 epoch 时间执行协调，随后采集并比对 gauge。
	reconcileHelper(t, r, &svc, []*corev1.Pod{pod1, pod2}, []*discovery.EndpointSlice{}, time.Unix(0, 0))

	want := `
# HELP endpoint_slice_controller_desired_endpoint_slices [BETA] Number of EndpointSlices that would exist with perfect endpoint allocation
# TYPE endpoint_slice_controller_desired_endpoint_slices gauge
endpoint_slice_controller_desired_endpoint_slices 1
# HELP endpoint_slice_controller_endpoints_desired [BETA] Number of endpoints desired
# TYPE endpoint_slice_controller_endpoints_desired gauge
endpoint_slice_controller_endpoints_desired 2
# HELP endpoint_slice_controller_num_endpoint_slices [BETA] Number of EndpointSlices
# TYPE endpoint_slice_controller_num_endpoint_slices gauge
endpoint_slice_controller_num_endpoint_slices 1
# HELP endpoint_slice_controller_services_count_by_traffic_distribution [BETA] Number of Services using some specific trafficDistribution
# TYPE endpoint_slice_controller_services_count_by_traffic_distribution gauge
endpoint_slice_controller_services_count_by_traffic_distribution{traffic_distribution="PreferClose"} 1
`

	if err := testutil.GatherAndCompare(
		legacyregistry.DefaultGatherer,
		strings.NewReader(want),
		"endpoint_slice_controller_endpoints_desired",
		"endpoint_slice_controller_num_endpoint_slices",
		"endpoint_slice_controller_desired_endpoint_slices",
		"endpoint_slice_controller_services_count_by_traffic_distribution",
	); err != nil {
		t.Fatal(err)
	}
}

// When a Service has a non-nil deletionTimestamp we want to avoid creating any
// new EndpointSlices but continue to allow updates and deletes through. This
// test uses 3 EndpointSlices, 1 "to-create", 1 "to-update", and 1 "to-delete".
// Each test case exercises different combinations of calls to finalize with
// those resources.
// TestReconcilerFinalizeSvcDeletionTimestamp 验证 Service 带有 deletionTimestamp 时 finalize 的行为。
// 规则是：不创建新切片，但允许更新与删除；回收场景下原本要创建的切片会转为更新待删除切片。
// 表驱动用例按 deletionTimestamp 是否为空以及 create/update/delete 三类操作的尝试组合展开，断言三类切片的最终存在性与内容。
func TestReconcilerFinalizeSvcDeletionTimestamp(t *testing.T) {
	now := metav1.Now()

	// 表驱动用例按 deletionTimestamp 以及尝试的 create/update/delete 组合分组。
	testCases := []struct {
		name               string
		deletionTimestamp  *metav1.Time
		attemptCreate      bool
		attemptUpdate      bool
		attemptDelete      bool
		expectCreatedSlice bool
		expectUpdatedSlice bool
		expectDeletedSlice bool
	}{{
		name:               "Attempt create and update, nil deletion timestamp",
		deletionTimestamp:  nil,
		attemptCreate:      true,
		attemptUpdate:      true,
		expectCreatedSlice: true,
		expectUpdatedSlice: true,
		expectDeletedSlice: true,
	}, {
		name:               "Attempt create and update, deletion timestamp set",
		deletionTimestamp:  &now,
		attemptCreate:      true,
		attemptUpdate:      true,
		expectCreatedSlice: false,
		expectUpdatedSlice: true,
		expectDeletedSlice: true,
	}, {
		// Slice scheduled for creation is transitioned to update of Slice
		// scheduled for deletion.
		name:               "Attempt create, update, and delete, nil deletion timestamp, recycling in action",
		deletionTimestamp:  nil,
		attemptCreate:      true,
		attemptUpdate:      true,
		attemptDelete:      true,
		expectCreatedSlice: false,
		expectUpdatedSlice: true,
		expectDeletedSlice: true,
	}, {
		// Slice scheduled for creation is transitioned to update of Slice
		// scheduled for deletion.
		name:               "Attempt create, update, and delete, deletion timestamp set, recycling in action",
		deletionTimestamp:  &now,
		attemptCreate:      true,
		attemptUpdate:      true,
		attemptDelete:      true,
		expectCreatedSlice: false,
		expectUpdatedSlice: true,
		expectDeletedSlice: true,
	}, {
		// Update and delete continue to work when deletionTimestamp is set.
		name:               "Attempt update delete, deletion timestamp set",
		deletionTimestamp:  &now,
		attemptCreate:      false,
		attemptUpdate:      true,
		attemptDelete:      true,
		expectCreatedSlice: false,
		expectUpdatedSlice: true,
		expectDeletedSlice: false,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 每个子用例使用独立的 fake clientset。
			client := newClientset()
			resetMetrics()
			r := newReconciler(client, []*corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}}, defaultMaxEndpointsPerSlice)

			namespace := "test"
			// 构造 Service 及其 ownerReference，供三个切片共用。
			svc, endpointMeta := newServiceAndEndpointMeta("foo", namespace)
			svc.DeletionTimestamp = tc.deletionTimestamp
			gvk := schema.GroupVersionKind{Version: "v1", Kind: "Service"}
			ownerRef := metav1.NewControllerRef(&svc, gvk)

			// 待创建切片不会预先写入 clientset。
			esToCreate := &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:            "to-create",
					OwnerReferences: []metav1.OwnerReference{*ownerRef},
				},
				AddressType: endpointMeta.addressType,
				Ports:       endpointMeta.ports,
			}

			// Add EndpointSlice that can be updated.
			// 写入待更新切片，并预置端点以检验更新是否生效。
			esToUpdate, err := client.DiscoveryV1().EndpointSlices(namespace).Create(context.TODO(), &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:            "to-update",
					OwnerReferences: []metav1.OwnerReference{*ownerRef},
				},
				AddressType: endpointMeta.addressType,
				Ports:       endpointMeta.ports,
			}, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("Expected no error creating EndpointSlice during test setup, got %v", err)
			}
			// Add an endpoint so we can see if this has actually been updated by
			// finalize func.
			esToUpdate.Endpoints = []discovery.Endpoint{{Addresses: []string{"10.2.3.4"}}}

			// Add EndpointSlice that can be deleted.
			// 写入待删除切片。
			esToDelete, err := client.DiscoveryV1().EndpointSlices(namespace).Create(context.TODO(), &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:            "to-delete",
					OwnerReferences: []metav1.OwnerReference{*ownerRef},
				},
				AddressType: endpointMeta.addressType,
				Ports:       endpointMeta.ports,
			}, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("Expected no error creating EndpointSlice during test setup, got %v", err)
			}

			// 按用例开关组装三类待处理切片。
			slicesToCreate := []*discovery.EndpointSlice{}
			if tc.attemptCreate {
				slicesToCreate = append(slicesToCreate, esToCreate.DeepCopy())
			}
			slicesToUpdate := []*discovery.EndpointSlice{}
			if tc.attemptUpdate {
				slicesToUpdate = append(slicesToUpdate, esToUpdate.DeepCopy())
			}
			slicesToDelete := []*discovery.EndpointSlice{}
			if tc.attemptDelete {
				slicesToDelete = append(slicesToDelete, esToDelete.DeepCopy())
			}

			// 调用 finalize 并核验切片的最终存在性与内容。
			err = r.finalize(&svc, slicesToCreate, slicesToUpdate, slicesToDelete, time.Now())
			if err != nil {
				t.Errorf("Error calling r.finalize(): %v", err)
			}

			// 从 clientset 读取最终切片列表。
			fetchedSlices := fetchEndpointSlices(t, client, namespace)

			createdSliceFound := false
			updatedSliceFound := false
			deletedSliceFound := false
			for _, epSlice := range fetchedSlices {
				if epSlice.Name == esToCreate.Name {
					createdSliceFound = true
				}
				if epSlice.Name == esToUpdate.Name {
					updatedSliceFound = true
					if tc.attemptUpdate && len(epSlice.Endpoints) != len(esToUpdate.Endpoints) {
						t.Errorf("Expected EndpointSlice to be updated with %d endpoints, got %d endpoints", len(esToUpdate.Endpoints), len(epSlice.Endpoints))
					}
				}
				if epSlice.Name == esToDelete.Name {
					deletedSliceFound = true
				}
			}

			// 逐项比对三类切片的预期存在性。
			if createdSliceFound != tc.expectCreatedSlice {
				t.Errorf("Expected created EndpointSlice existence to be %t, got %t", tc.expectCreatedSlice, createdSliceFound)
			}

			if updatedSliceFound != tc.expectUpdatedSlice {
				t.Errorf("Expected updated EndpointSlice existence to be %t, got %t", tc.expectUpdatedSlice, updatedSliceFound)
			}

			if deletedSliceFound != tc.expectDeletedSlice {
				t.Errorf("Expected deleted EndpointSlice existence to be %t, got %t", tc.expectDeletedSlice, deletedSliceFound)
			}
		})
	}
}

// When a Pod references a Node that is not present in the informer cache the
// EndpointSlices will continue to allow create, updates and deletes through.
// The Pod with the missing reference will be published or retried depending of
// the service.spec.PublishNotReadyAddresses.
// The test considers two Pods on different nodes, one of the Nodes is not present.
// TestReconcilerPodMissingNode 验证 Pod 引用的 Node 不在 informer 缓存中时的处理。
// 构造两个节点及分别调度其上的两个 Pod，但只把部分节点加入索引；表驱动用例覆盖创建/更新、PublishNotReadyAddresses 开关以及节点缺失程度。
// 当引用了缺失节点且未开启 PublishNotReadyAddresses 时期望返回错误；开启时不报错，端点按规则保留或发布。断言错误、实际切片数与指标。
func TestReconcilerPodMissingNode(t *testing.T) {
	namespace := "test"

	// 构造两个节点和两个分别调度到其上的 Pod。
	nodes := make([]*corev1.Node, 2)
	nodes[0] = &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-0"}}
	nodes[1] = &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node-1"}}

	pods := make([]*corev1.Pod, 2)
	pods[0] = newPod(0, namespace, true, 1, false)
	pods[0].Spec.NodeName = nodes[0].Name
	pods[1] = newPod(1, namespace, true, 1, false)
	pods[1].Spec.NodeName = nodes[1].Name

	// 构造 Service 与端点元数据。
	service, endpointMeta := newServiceAndEndpointMeta("foo", namespace)

	// 表驱动用例覆盖创建/更新场景、PublishNotReadyAddresses 开关以及缺失节点程度。
	testCases := []struct {
		name            string
		publishNotReady bool
		existingNodes   []*corev1.Node
		existingSlice   func() *discovery.EndpointSlice
		expectedMetrics expectedMetrics
		expectError     bool
	}{{
		name:            "Create and publishNotReady false",
		publishNotReady: false,
		existingNodes:   []*corev1.Node{nodes[0]},
		expectedMetrics: expectedMetrics{
			desiredSlices:        1,
			actualSlices:         1,
			desiredEndpoints:     1,
			addedPerSync:         1,
			removedPerSync:       0,
			numCreated:           1,
			numUpdated:           0,
			numDeleted:           0,
			slicesChangedPerSync: 1,
		},
		expectError: true,
	}, {
		name:            "Create and publishNotReady true",
		publishNotReady: true,
		existingNodes:   []*corev1.Node{nodes[0]},
		expectedMetrics: expectedMetrics{
			desiredSlices:        1,
			actualSlices:         1,
			desiredEndpoints:     2,
			addedPerSync:         2,
			removedPerSync:       0,
			numCreated:           1,
			numUpdated:           0,
			numDeleted:           0,
			slicesChangedPerSync: 1,
		},
		expectError: false,
	}, {
		name:            "Update and publishNotReady false",
		publishNotReady: false,
		existingNodes:   []*corev1.Node{nodes[0]},
		existingSlice: func() *discovery.EndpointSlice {
			slice := newEmptyEndpointSlice(1, namespace, endpointMeta, service)
			slice.Endpoints = append(slice.Endpoints, podToEndpoint(pods[0], nodes[0], &service, discovery.AddressTypeIPv4))
			slice.Endpoints = append(slice.Endpoints, podToEndpoint(pods[1], nodes[1], &service, discovery.AddressTypeIPv4))
			return slice
		},
		expectedMetrics: expectedMetrics{
			desiredSlices:        1,
			actualSlices:         1,
			desiredEndpoints:     1,
			addedPerSync:         0,
			removedPerSync:       1,
			numCreated:           0,
			numUpdated:           1,
			numDeleted:           0,
			slicesChangedPerSync: 1,
		},
		expectError: true,
	}, {
		name:            "Update and publishNotReady true and all nodes missing",
		publishNotReady: true,
		existingSlice: func() *discovery.EndpointSlice {
			slice := newEmptyEndpointSlice(1, namespace, endpointMeta, service)
			slice.Endpoints = append(slice.Endpoints, podToEndpoint(pods[0], nodes[0], &service, discovery.AddressTypeIPv4))
			slice.Endpoints = append(slice.Endpoints, podToEndpoint(pods[1], nodes[1], &service, discovery.AddressTypeIPv4))
			return slice
		},
		expectedMetrics: expectedMetrics{
			desiredSlices:        1,
			actualSlices:         1,
			desiredEndpoints:     2,
			addedPerSync:         0,
			removedPerSync:       0,
			numCreated:           0,
			numUpdated:           0,
			numDeleted:           0,
			slicesChangedPerSync: 0,
		},
		expectError: false,
	}, {
		name:            "Update and publishNotReady true",
		publishNotReady: true,
		existingNodes:   []*corev1.Node{nodes[0]},
		existingSlice: func() *discovery.EndpointSlice {
			slice := newEmptyEndpointSlice(1, namespace, endpointMeta, service)
			slice.Endpoints = append(slice.Endpoints, podToEndpoint(pods[0], nodes[0], &service, discovery.AddressTypeIPv4))
			slice.Endpoints = append(slice.Endpoints, podToEndpoint(pods[1], nodes[1], &service, discovery.AddressTypeIPv4))
			return slice
		},
		expectedMetrics: expectedMetrics{
			desiredSlices:        1,
			actualSlices:         1,
			desiredEndpoints:     2,
			addedPerSync:         0,
			removedPerSync:       0,
			numCreated:           0,
			numUpdated:           0,
			numDeleted:           0,
			slicesChangedPerSync: 0,
		},
		expectError: false,
	}, {
		name:            "Update if publishNotReady false and no nodes are present",
		publishNotReady: false,
		existingNodes:   []*corev1.Node{},
		existingSlice: func() *discovery.EndpointSlice {
			slice := newEmptyEndpointSlice(1, namespace, endpointMeta, service)
			slice.Endpoints = append(slice.Endpoints, podToEndpoint(pods[0], nodes[0], &service, discovery.AddressTypeIPv4))
			slice.Endpoints = append(slice.Endpoints, podToEndpoint(pods[1], nodes[1], &service, discovery.AddressTypeIPv4))
			return slice
		},
		expectedMetrics: expectedMetrics{
			desiredSlices:    1,
			actualSlices:     1,
			desiredEndpoints: 0,
			addedPerSync:     0,
			removedPerSync:   2,
			numCreated:       0,
			// the slice is updated not deleted
			numUpdated:           1,
			numDeleted:           0,
			slicesChangedPerSync: 1,
		},
		expectError: true,
	},
	}

	// 逐用例执行协调并断言错误与指标。
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client := newClientset()
			resetMetrics()
			r := newReconciler(client, tc.existingNodes, defaultMaxEndpointsPerSlice)
			logger, _ := ktesting.NewTestContext(t)

			// 深拷贝 Service 并设置用例指定的 PublishNotReadyAddresses。
			svc := service.DeepCopy()
			svc.Spec.PublishNotReadyAddresses = tc.publishNotReady
			// 按需预置包含两个 Pod 端点的切片。
			existingSlices := []*discovery.EndpointSlice{}
			if tc.existingSlice != nil {
				slice := tc.existingSlice()
				existingSlices = append(existingSlices, slice)
				_, createErr := client.DiscoveryV1().EndpointSlices(namespace).Create(context.TODO(), slice, metav1.CreateOptions{})
				if createErr != nil {
					t.Errorf("Expected no error creating endpoint slice")
				}
			}
			// 直接调用 Reconcile 以便断言是否返回预期错误。
			err := r.Reconcile(logger, svc, pods, existingSlices, time.Now())
			if err == nil && tc.expectError {
				t.Errorf("Expected error but no error received")
			}
			if err != nil && !tc.expectError {
				t.Errorf("Unexpected error: %v", err)
			}

			// 断言实际切片数与指标一致，并核验各项指标。
			fetchedSlices := fetchEndpointSlices(t, client, namespace)
			if len(fetchedSlices) != tc.expectedMetrics.actualSlices {
				t.Fatalf("Actual slices %d doesn't match metric %d", len(fetchedSlices), tc.expectedMetrics.actualSlices)
			}
			expectMetrics(t, tc.expectedMetrics)

		})
	}
}

// TestReconcileTopology 验证拓扑感知提示（zone hints）的分配。
// 构造 3 个可用区、每区 10 个节点和 Pod，并预置若干跨区切片；表驱动用例覆盖 topologyCache 开关与 hints 注解为 auto/Auto/disabled 的组合。
// 断言每个 zone 获得的提示数、跨区提示数、缓存未被修改以及对应指标。
func TestReconcileTopology(t *testing.T) {
	ns := "testing"
	svc, endpointMeta := newServiceAndEndpointMeta("foo", ns)

	// 3 zones, 10 nodes and pods per zone
	// 每个可用区 10 个节点，每个节点 1 个 Pod。
	zones := []string{"zone-a", "zone-b", "zone-c"}

	pods := []*corev1.Pod{}
	nodes := []*corev1.Node{}
	nodesByName := map[string]*corev1.Node{}
	for i, zone := range zones {
		for j := 0; j < 10; j++ {
			node := &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: fmt.Sprintf("node-%s-%d", zone, j),
					Labels: map[string]string{
						corev1.LabelTopologyZone: zone,
					},
				},
				Status: corev1.NodeStatus{
					Conditions: []corev1.NodeCondition{{
						Type:   corev1.NodeReady,
						Status: corev1.ConditionTrue,
					}},
					Allocatable: corev1.ResourceList{"cpu": resource.MustParse("100m")},
				},
			}
			nodesByName[node.Name] = node
			nodes = append(nodes, node)

			pod := newPod(i*100+j, ns, true, 1, false)
			pod.Spec.NodeName = node.Name
			pods = append(pods, pod)
		}
	}

	// 预置三个跨区切片供各用例复用。
	slicesByName := map[string]*discovery.EndpointSlice{}
	slicePods := map[string][]*corev1.Pod{
		"zone-a-b": {pods[7], pods[8], pods[16], pods[17], pods[18]},
		"zone-a-c": {pods[5], pods[6], pods[25], pods[26]},
		"zone-c":   {pods[27], pods[28], pods[29]},
	}

	gvk := schema.GroupVersionKind{Version: "v1", Kind: "Service"}
	ownerRef := metav1.NewControllerRef(&svc, gvk)

	// 为每个预置切片填充对应 Pod 的端点。
	for name, pods := range slicePods {
		endpoints := []discovery.Endpoint{}
		for _, pod := range pods {
			endpoints = append(endpoints, podToEndpoint(pod, nodesByName[pod.Spec.NodeName], &svc, endpointMeta.addressType))
		}

		slicesByName[name] = &discovery.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name:            name,
				OwnerReferences: []metav1.OwnerReference{*ownerRef},
				Labels: map[string]string{
					discovery.LabelManagedBy:   controllerName,
					discovery.LabelServiceName: svc.Name,
				},
			},
			AddressType: endpointMeta.addressType,
			Ports:       endpointMeta.ports,
			Endpoints:   endpoints,
		}
	}

	// 表驱动用例覆盖拓扑缓存开关、hints 注解取值以及端点分布比例等场景。
	testCases := []struct {
		name                   string
		topologyCacheEnabled   bool
		hintsAnnotation        string
		existingSlices         []*discovery.EndpointSlice
		pods                   []*corev1.Pod
		nodes                  []*corev1.Node
		expectedHints          map[string]int
		expectedCrossZoneHints int
		expectedMetrics        expectedMetrics
	}{{
		name:                   "no change, topologyCache disabled, annotation == auto",
		topologyCacheEnabled:   false,
		hintsAnnotation:        "auto",
		existingSlices:         []*discovery.EndpointSlice{slicesByName["zone-c"]},
		pods:                   slicePods["zone-c"],
		nodes:                  nodes,
		expectedHints:          nil,
		expectedCrossZoneHints: 0,
		expectedMetrics: expectedMetrics{
			desiredSlices:        1,
			actualSlices:         1,
			desiredEndpoints:     3,
			addedPerSync:         0,
			removedPerSync:       0,
			numCreated:           0,
			numUpdated:           0,
			numDeleted:           0,
			slicesChangedPerSync: 0,
		},
	}, {
		name:                 "enabling topologyCache, hintsAnnotation == auto",
		topologyCacheEnabled: true,
		hintsAnnotation:      "auto",
		existingSlices:       []*discovery.EndpointSlice{slicesByName["zone-c"]},
		pods:                 slicePods["zone-c"],
		nodes:                nodes,
		expectedHints: map[string]int{
			"zone-a": 1,
			"zone-b": 1,
			"zone-c": 1,
		},
		expectedCrossZoneHints: 2,
		expectedMetrics: expectedMetrics{
			desiredSlices:                1,
			actualSlices:                 1,
			desiredEndpoints:             3,
			addedPerSync:                 0,
			removedPerSync:               0,
			numCreated:                   0,
			numUpdated:                   1,
			numDeleted:                   0,
			slicesChangedPerSyncTopology: 1,
		},
	}, {
		name:                   "topology enabled,  hintsAnnotation==auto, ratio beyond threshold",
		topologyCacheEnabled:   true,
		hintsAnnotation:        "auto",
		existingSlices:         []*discovery.EndpointSlice{slicesByName["zone-a-c"]},
		pods:                   slicePods["zone-a-c"],
		nodes:                  nodes,
		expectedHints:          nil,
		expectedCrossZoneHints: 0,
		expectedMetrics: expectedMetrics{
			desiredSlices:                1,
			actualSlices:                 1,
			desiredEndpoints:             4,
			addedPerSync:                 0,
			removedPerSync:               0,
			numCreated:                   0,
			numUpdated:                   0,
			numDeleted:                   0,
			slicesChangedPerSyncTopology: 0,
		},
	}, {
		name:                 "topology enabled, hintsAnnotation==Auto, more slices and endpoints",
		topologyCacheEnabled: true,
		hintsAnnotation:      "Auto",
		existingSlices:       []*discovery.EndpointSlice{slicesByName["zone-a-c"], slicesByName["zone-a-b"]},
		pods:                 append(slicePods["zone-a-c"], slicePods["zone-a-b"]...),
		nodes:                nodes,
		expectedHints: map[string]int{
			"zone-a": 3,
			"zone-b": 3,
			"zone-c": 3,
		},
		expectedCrossZoneHints: 1,
		expectedMetrics: expectedMetrics{
			desiredSlices:    1,
			actualSlices:     2,
			desiredEndpoints: 9,
			addedPerSync:     0,
			removedPerSync:   0,
			numCreated:       0,
			// TODO(robscott): Since we're potentially changing more slices when
			// adding topology hints we could use it as a free repacking
			// opportunity. That would make this value 1.
			numUpdated:                   2,
			numDeleted:                   0,
			slicesChangedPerSyncTopology: 2,
		},
	}, {
		name:                   "topology enabled, hintsAnnotation==disabled, more slices and endpoints",
		topologyCacheEnabled:   true,
		hintsAnnotation:        "disabled",
		existingSlices:         []*discovery.EndpointSlice{slicesByName["zone-a-c"], slicesByName["zone-a-b"]},
		pods:                   append(slicePods["zone-a-c"], slicePods["zone-a-b"]...),
		nodes:                  nodes,
		expectedHints:          nil,
		expectedCrossZoneHints: 0,
		expectedMetrics: expectedMetrics{
			desiredSlices:        1,
			actualSlices:         2,
			desiredEndpoints:     9,
			addedPerSync:         0,
			removedPerSync:       0,
			numCreated:           0,
			numUpdated:           0,
			numDeleted:           0,
			slicesChangedPerSync: 0,
		},
	}}

	// 逐用例执行协调并核验提示与指标。
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 预置切片并监视缓存不被修改。
			client := newClientset()
			cmc := newCacheMutationCheck(tc.existingSlices)
			createEndpointSlices(t, client, ns, tc.existingSlices)
			logger, _ := ktesting.NewTestContext(t)

			// 重置指标后构造协调器。
			resetMetrics()
			r := newReconciler(client, tc.nodes, defaultMaxEndpointsPerSlice)
			// 按需启用拓扑缓存并写入节点。
			if tc.topologyCacheEnabled {
				r.topologyCache = topologycache.NewTopologyCache()
				r.topologyCache.SetNodes(logger, tc.nodes)
			}

			// 用已废弃的 topology-aware-hints 注解控制提示行为。
			service := svc.DeepCopy()
			service.Annotations = map[string]string{
				corev1.DeprecatedAnnotationTopologyAwareHints: tc.hintsAnnotation,
			}
			// 执行协调，随后校验缓存未被修改、指标与切片数量。
			r.Reconcile(logger, service, tc.pods, tc.existingSlices, time.Now())

			cmc.Check(t)
			expectMetrics(t, tc.expectedMetrics)
			fetchedSlices := fetchEndpointSlices(t, client, ns)
			if len(fetchedSlices) != tc.expectedMetrics.actualSlices {
				t.Fatalf("Actual slices %d doesn't match metric %d", len(fetchedSlices), tc.expectedMetrics.actualSlices)
			}

			// 若期望无提示，断言所有端点均无 ForZones。
			if tc.expectedHints == nil {
				for _, slice := range fetchedSlices {
					for _, endpoint := range slice.Endpoints {
						if endpoint.Hints != nil && len(endpoint.Hints.ForZones) > 0 {
							t.Fatalf("Expected endpoint not to have zone hints: %+v", endpoint)
						}
					}
				}
				return
			}

			// 统计每个 zone 获得的提示数以及跨区提示数。
			actualCrossZoneHints := 0
			actualHints := map[string]int{}

			// 遍历所有端点，收集 zone hint。
			for _, slice := range fetchedSlices {
				for _, endpoint := range slice.Endpoints {
					if endpoint.Hints == nil || len(endpoint.Hints.ForZones) == 0 {
						t.Fatalf("Expected endpoint to have zone hints: %+v", endpoint)
					}
					if len(endpoint.Hints.ForZones) > 1 {
						t.Fatalf("Expected endpoint to only have 1 zone hint, got %d", len(endpoint.Hints.ForZones))
					}

					if endpoint.Zone == nil || *endpoint.Zone == "" {
						t.Fatalf("Expected endpoint to have zone: %+v", endpoint)
					}
					zoneHint := endpoint.Hints.ForZones[0].Name
					if *endpoint.Zone != zoneHint {
						actualCrossZoneHints++
					}
					actualHints[zoneHint]++
				}
			}

			if len(actualHints) != len(tc.expectedHints) {
				t.Errorf("Expected hints for %d zones, got %d", len(tc.expectedHints), len(actualHints))
			}

			for zone, expectedNum := range tc.expectedHints {
				actualNum, _ := actualHints[zone]
				if actualNum != expectedNum {
					t.Errorf("Expected %d hints for %s zone, got %d", expectedNum, zone, actualNum)
				}
			}

			if actualCrossZoneHints != tc.expectedCrossZoneHints {
				t.Errorf("Expected %d cross zone hints, got %d", tc.expectedCrossZoneHints, actualCrossZoneHints)
			}
		})
	}
}

// Test reconciliation behaviour for trafficDistribution field.
// TestReconcile_TrafficDistribution 验证 trafficDistribution 字段对端点提示的影响。
// 表驱动用例覆盖 PreferClose、与 topology 注解的优先级、两者均关闭、PreferSameNode 特性开关以及 PreferSameZone 未开启特性开关等场景。
// 断言按 zone/node 的提示分布、跨区提示数以及按 trafficDistribution 维度统计的指标。
func TestReconcile_TrafficDistribution(t *testing.T) {
	// Setup the following topology for the test.
	//
	// 	- node-0 IN zone-a CONTAINS {pod-0}
	// 	- node-1 IN zone-b CONTAINS {pod-1, pod-2, pod-3}
	// 	- node-2 IN zone-c CONTAINS {pod-4, pod-5}
	// 按上述拓扑构造节点与 Pod。
	ns := "ns1"
	svc, _ := newServiceAndEndpointMeta("foo", ns)
	nodes := []*corev1.Node{}
	pods := []*corev1.Pod{}
	// 构造 3 个 Ready 节点，分别位于 zone-a/b/c。
	for i := 0; i < 3; i++ {
		nodes = append(nodes, &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("node-%v", i),
				Labels: map[string]string{
					corev1.LabelTopologyZone: fmt.Sprintf("zone-%c", 'a'+i),
				},
			},
			Status: corev1.NodeStatus{
				Conditions: []corev1.NodeCondition{{
					Type:   corev1.NodeReady,
					Status: corev1.ConditionTrue,
				}},
				Allocatable: corev1.ResourceList{"cpu": resource.MustParse("100m")},
			},
		})
	}
	// 构造 6 个 Pod，随后映射到不同节点。
	for i := 0; i < 6; i++ {
		pods = append(pods, newPod(i, ns, true, 1, false))
	}
	// 按设计分配 Pod 到节点，形成不均衡的 zone 分布。
	pods[0].Spec.NodeName = nodes[0].Name
	pods[1].Spec.NodeName = nodes[1].Name
	pods[2].Spec.NodeName = nodes[1].Name
	pods[3].Spec.NodeName = nodes[1].Name
	pods[4].Spec.NodeName = nodes[2].Name
	pods[5].Spec.NodeName = nodes[2].Name

	// Define test cases.

	// 表驱动用例覆盖 trafficDistribution 与 topology 注解的优先级、特性开关以及两者均关闭等场景。
	testCases := []struct {
		name string
		desc string

		preferSameFeatureGateEnabled bool
		trafficDistribution          *string
		topologyAnnotation           string

		// Defines how many hints belong to a particular zone/node
		wantHintsDistributionByZone map[string]int
		wantHintsDistributionByNode map[string]int
		// Number of endpoints where the zone hints are different from the zone of
		// the endpoint itself.
		wantEndpointsWithCrossZoneHints int
		wantMetrics                     expectedMetrics
	}{
		{
			name:                "trafficDistribution=PreferClose, topologyAnnotation=Disabled",
			desc:                "When trafficDistribution is enabled and topologyAnnotation is disabled, hints should be distributed as per the trafficDistribution field",
			trafficDistribution: ptr.To(corev1.ServiceTrafficDistributionPreferClose),
			topologyAnnotation:  "Disabled",
			wantHintsDistributionByZone: map[string]int{
				"zone-a": 1, // {pod-0}
				"zone-b": 3, // {pod-1, pod-2, pod-3}
				"zone-c": 2, // {pod-4, pod-5}
			},
			wantMetrics: expectedMetrics{
				desiredSlices:                   1,
				actualSlices:                    1,
				desiredEndpoints:                6,
				addedPerSync:                    6,
				removedPerSync:                  0,
				numCreated:                      1,
				numUpdated:                      0,
				numDeleted:                      0,
				slicesChangedPerSync:            0, // 0 means either topologyAnnotation or trafficDistribution was used.
				slicesChangedPerSyncTopology:    0, // 0 means topologyAnnotation was not used.
				slicesChangedPerSyncTrafficDist: 1, // 1 EPS configured using trafficDistribution.
				servicesCountByTrafficDistribution: map[string]int{
					"PreferClose": 1,
				},
			},
		},
		{
			name:                "trafficDistribution=PreferClose, topologyAnnotation=Auto",
			desc:                "When trafficDistribution and topologyAnnotation are both enabled, precedence should be given to topologyAnnotation",
			trafficDistribution: ptr.To(corev1.ServiceTrafficDistributionPreferClose),
			topologyAnnotation:  "Auto",
			wantHintsDistributionByZone: map[string]int{
				"zone-a": 2, // {pod-0, pod-3} (pod-3 is just an example, it could have also been either of the other two)
				"zone-b": 2, // {pod-1, pod-2}
				"zone-c": 2, // {pod-4, pod-5}
			},
			wantEndpointsWithCrossZoneHints: 1, // since a pod from zone-b is likely assigned a hint for zone-a
			wantMetrics: expectedMetrics{
				desiredSlices:                   1,
				actualSlices:                    1,
				desiredEndpoints:                6,
				addedPerSync:                    6,
				removedPerSync:                  0,
				numCreated:                      1,
				numUpdated:                      0,
				numDeleted:                      0,
				slicesChangedPerSync:            0, // 0 means either topologyAnnotation or trafficDistribution was used.
				slicesChangedPerSyncTopology:    1, // 1 EPS configured using topologyAnnotation.
				slicesChangedPerSyncTrafficDist: 0, // 0 means trafficDistribution was not used.
			},
		},
		{
			name:                        "trafficDistribution=nil, topologyAnnotation=<empty>",
			desc:                        "When trafficDistribution and topologyAnnotation are both disabled, no hints should be added",
			trafficDistribution:         nil,
			topologyAnnotation:          "",
			wantHintsDistributionByZone: map[string]int{"": 6}, // Equivalent to no hints.
			wantMetrics: expectedMetrics{
				desiredSlices:                   1,
				actualSlices:                    1,
				desiredEndpoints:                6,
				addedPerSync:                    6,
				removedPerSync:                  0,
				numCreated:                      1,
				numUpdated:                      0,
				numDeleted:                      0,
				slicesChangedPerSync:            1, // 1 means both topologyAnnotation and trafficDistribution were not used.
				slicesChangedPerSyncTopology:    0, // 0 means topologyAnnotation was not used.
				slicesChangedPerSyncTrafficDist: 0, // 0 means trafficDistribution was not used.
			},
		},
		{
			name:                         "trafficDistribution=PreferSameNode, PSTD enabled",
			desc:                         "When trafficDistribution is PreferSameNode and PreferSameTrafficDistribution is enabled, both zone and node hints should be filled out",
			preferSameFeatureGateEnabled: true,
			trafficDistribution:          ptr.To(corev1.ServiceTrafficDistributionPreferSameNode),
			topologyAnnotation:           "Disabled",
			wantHintsDistributionByZone: map[string]int{
				"zone-a": 1, // {pod-0}
				"zone-b": 3, // {pod-1, pod-2, pod-3}
				"zone-c": 2, // {pod-4, pod-5}
			},
			wantHintsDistributionByNode: map[string]int{
				"node-0": 1, // {pod-0}
				"node-1": 3, // {pod-1, pod-2, pod-3}
				"node-2": 2, // {pod-4, pod-5}
			},
			wantMetrics: expectedMetrics{
				desiredSlices:                   1,
				actualSlices:                    1,
				desiredEndpoints:                6,
				addedPerSync:                    6,
				removedPerSync:                  0,
				numCreated:                      1,
				numUpdated:                      0,
				numDeleted:                      0,
				slicesChangedPerSync:            0, // 0 means either topologyAnnotation or trafficDistribution was used.
				slicesChangedPerSyncTopology:    0, // 0 means topologyAnnotation was not used.
				slicesChangedPerSyncTrafficDist: 1, // 1 EPS configured using trafficDistribution.
				servicesCountByTrafficDistribution: map[string]int{
					"PreferSameNode": 1,
				},
			},
		},
		{
			name:                         "trafficDistribution=PreferSameZone, PSTD disabled",
			desc:                         "When trafficDistribution is PreferSameZone and PreferSameTrafficDistribution is disabled, no hints should be set",
			preferSameFeatureGateEnabled: false,
			trafficDistribution:          ptr.To(corev1.ServiceTrafficDistributionPreferSameZone),
			topologyAnnotation:           "Disabled",
			wantHintsDistributionByZone:  map[string]int{"": 6}, // Equivalent to no hints.
			wantMetrics: expectedMetrics{
				desiredSlices:                   1,
				actualSlices:                    1,
				desiredEndpoints:                6,
				addedPerSync:                    6,
				removedPerSync:                  0,
				numCreated:                      1,
				numUpdated:                      0,
				numDeleted:                      0,
				slicesChangedPerSync:            1, // 1 means both topologyAnnotation and trafficDistribution were not used.
				slicesChangedPerSyncTopology:    0, // 0 means topologyAnnotation was not used.
				slicesChangedPerSyncTrafficDist: 0, // 0 means trafficDistribution was not used.
			},
		},
	}

	// Make assertions.

	// 逐用例执行协调并断言提示分布与指标。
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 每个子用例使用独立的 fake clientset 并重置指标。
			client := newClientset()
			logger, _ := ktesting.NewTestContext(t)
			resetMetrics()

			// 按用例配置特性开关并填充拓扑缓存。
			r := newReconciler(client, nodes, defaultMaxEndpointsPerSlice)
			r.preferSameTrafficDistribution = tc.preferSameFeatureGateEnabled
			r.topologyCache = topologycache.NewTopologyCache()
			r.topologyCache.SetNodes(logger, nodes)

			// 设置 trafficDistribution 与 topology 注解。
			service := svc.DeepCopy()
			service.Spec.TrafficDistribution = tc.trafficDistribution
			service.Annotations = map[string]string{
				corev1.DeprecatedAnnotationTopologyAwareHints: tc.topologyAnnotation,
			}

			// 执行协调后统计提示分布。
			err := r.Reconcile(logger, service, pods, nil, time.Now())

			if err != nil {
				t.Errorf("Reconcile(...): return error = %v; want no error", err)
			}

			// 遍历端点，按 zone 汇总提示并统计跨区提示。
			fetchedSlices := fetchEndpointSlices(t, client, ns)
			gotHintsDistributionByZone := make(map[string]int)
			gotEndpointsWithCrossZoneHints := 0
			for _, slice := range fetchedSlices {
				for _, endpoint := range slice.Endpoints {
					var zoneHint string
					if endpoint.Hints != nil && len(endpoint.Hints.ForZones) == 1 {
						zoneHint = endpoint.Hints.ForZones[0].Name
					}
					gotHintsDistributionByZone[zoneHint]++
					if zoneHint != "" && *endpoint.Zone != zoneHint {
						gotEndpointsWithCrossZoneHints++
					}
				}
			}

			// 比对期望与实际的 zone 提示分布。
			if diff := cmp.Diff(tc.wantHintsDistributionByZone, gotHintsDistributionByZone); diff != "" {
				t.Errorf("Reconcile(...): Incorrect distribution of endpoints among zones; (-want, +got)\n%v", diff)
			}
			if gotEndpointsWithCrossZoneHints != tc.wantEndpointsWithCrossZoneHints {
				t.Errorf("Reconcile(...): EndpointSlices have endpoints with incorrect number of cross-zone hints; gotEndpointsWithCrossZoneHints=%v, want=%v", gotEndpointsWithCrossZoneHints, tc.wantEndpointsWithCrossZoneHints)
			}

			expectMetrics(t, tc.wantMetrics)
		})
	}
}

// TestReconcileHeadlessServiceNoPorts verifies that headless services with no ports
// don't cause EndpointSlice churn. Validates fix for https://github.com/kubernetes/kubernetes/issues/133474
// TestReconcileHeadlessServiceNoPorts 验证无端口的 headless Service 不会造成 EndpointSlice 反复变更。
// 首次协调创建一个端口列表为空的切片；随后模拟 API 服务器把空端口切片序列化为 nil，再次协调应不产生任何写操作，仅有一次 list。
// 该用例用于回归 https://github.com/kubernetes/kubernetes/issues/133474。
func TestReconcileHeadlessServiceNoPorts(t *testing.T) {
	namespace := "test"
	client := newClientset()
	resetMetrics()
	// 构造无端口的 headless Service（ClusterIP=None 且未设置 Ports）。
	svc := corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "headless-no-ports",
			Namespace: namespace,
			UID:       "test-uid",
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:  corev1.ClusterIPNone,
			Selector:   map[string]string{"foo": "bar"},
			IPFamilies: []corev1.IPFamily{corev1.IPv4Protocol},
		},
	}

	// 构造一个匹配 Service 选择器的 Pod。
	pod := newPod(1, namespace, true, 1, false)

	// 节点信息取自 Pod 的 nodeName。
	r := newReconciler(client, []*corev1.Node{{ObjectMeta: metav1.ObjectMeta{Name: pod.Spec.NodeName}}}, defaultMaxEndpointsPerSlice)

	// 首次协调应创建一个包含空端口列表的切片。
	reconcileHelper(t, r, &svc, []*corev1.Pod{pod}, []*discovery.EndpointSlice{}, time.Now())
	assert.Len(t, client.Actions(), 1, "Expected 1 additional clientset action")
	expectActions(t, client.Actions(), 1, "create", "endpointslices")

	// 模拟 API 服务器将空端口切片序列化为 nil。
	var existingSlices []*discovery.EndpointSlice
	for _, slice := range fetchEndpointSlices(t, client, namespace) {
		copy := slice.DeepCopy()
		// replicate API server behavior which serializes this empty slice as nil
		copy.Ports = nil
		existingSlices = append(existingSlices, copy)
	}
	assert.Len(t, existingSlices, 1, "Expected 1 endpoint slices")

	// 第二次协调应不产生写操作，仅有一次 list。
	reconcileHelper(t, r, &svc, []*corev1.Pod{pod}, existingSlices, time.Now())

	assert.Len(t, client.Actions(), 2, "Expected second reconcile to only list")
	expectActions(t, client.Actions(), 1, "list", "endpointslices")
}

// Test Helpers

// newReconciler 使用 fake clientset 构造 Reconciler：把传入节点加入 Node informer 的索引器，并用固定的控制器名、事件记录器与 EndpointSliceTracker 完成初始化。
func newReconciler(client *fake.Clientset, nodes []*corev1.Node, maxEndpointsPerSlice int32) *Reconciler {
	// 事件记录器仅用于满足构造函数参数。
	eventRecorder := record.NewFakeRecorder(10)
	informerFactory := informers.NewSharedInformerFactory(client, 0)
	// 把节点加入 informer 索引，使 Node lister 能查询到它们。
	nodeInformer := informerFactory.Core().V1().Nodes()
	indexer := nodeInformer.Informer().GetIndexer()
	for _, node := range nodes {
		indexer.Add(node)
	}

	return NewReconciler(
		client,
		corelisters.NewNodeLister(indexer),
		maxEndpointsPerSlice,
		endpointsliceutil.NewEndpointSliceTracker(),
		nil,
		eventRecorder,
		controllerName,
	)
}

// ensures endpoint slices exist with the desired set of lengths
// expectUnorderedSlicesWithLengths 断言切片集合的端点数量分布与期望一致，不关心切片顺序。
func expectUnorderedSlicesWithLengths(t *testing.T, endpointSlices []discovery.EndpointSlice, expectedLengths []int) {
	assert.Len(t, endpointSlices, len(expectedLengths), "Expected %d endpoint slices", len(expectedLengths))

	lengthsWithNoMatch := []int{}
	desiredLengths := expectedLengths
	actualLengths := []int{}
	for _, endpointSlice := range endpointSlices {
		actualLen := len(endpointSlice.Endpoints)
		actualLengths = append(actualLengths, actualLen)
		matchFound := false
		for i := 0; i < len(desiredLengths); i++ {
			if desiredLengths[i] == actualLen {
				matchFound = true
				desiredLengths = append(desiredLengths[:i], desiredLengths[i+1:]...)
				break
			}
		}

		if !matchFound {
			lengthsWithNoMatch = append(lengthsWithNoMatch, actualLen)
		}
	}

	if len(lengthsWithNoMatch) > 0 || len(desiredLengths) > 0 {
		t.Errorf("Actual slice lengths (%v) don't match expected (%v)", actualLengths, expectedLengths)
	}
}

// ensures endpoint slices exist with the desired set of ports and address types
// expectUnorderedSlicesWithTopLevelAttrs 忽略顺序，断言每个切片的端口与地址类型都能与期望切片匹配。
func expectUnorderedSlicesWithTopLevelAttrs(t *testing.T, endpointSlices []discovery.EndpointSlice, expectedSlices []discovery.EndpointSlice) {
	t.Helper()
	assert.Len(t, endpointSlices, len(expectedSlices), "Expected %d endpoint slices", len(expectedSlices))

	slicesWithNoMatch := []discovery.EndpointSlice{}
	for _, endpointSlice := range endpointSlices {
		matchFound := false
		for i := 0; i < len(expectedSlices); i++ {
			if portsAndAddressTypeEqual(expectedSlices[i], endpointSlice) {
				matchFound = true
				expectedSlices = append(expectedSlices[:i], expectedSlices[i+1:]...)
				break
			}
		}

		if !matchFound {
			slicesWithNoMatch = append(slicesWithNoMatch, endpointSlice)
		}
	}

	assert.Empty(t, slicesWithNoMatch, "EndpointSlice(s) found without matching attributes")
	assert.Empty(t, expectedSlices, "Expected slices(s) not found in EndpointSlices")
}

// expectActions 从 action 列表末尾开始，校验最近 num 个 action 的 verb 与 resource。
func expectActions(t *testing.T, actions []k8stesting.Action, num int, verb, resource string) {
	t.Helper()
	// if actions are less the below logic will panic
	// 若 action 数量不足则直接失败，避免后续越界。
	if num > len(actions) {
		t.Fatalf("len of actions %v is unexpected. Expected to be at least %v", len(actions), num+1)
	}

	// 从末尾开始校验最近 num 个 action。
	for i := 0; i < num; i++ {
		relativePos := len(actions) - i - 1
		assert.Equal(t, verb, actions[relativePos].GetVerb(), "Expected action -%d verb to be %s", i, verb)
		assert.Equal(t, resource, actions[relativePos].GetResource().Resource, "Expected action -%d resource to be %s", i, resource)
	}
}

// expectTrackedGeneration 断言 EndpointSliceTracker 中记录的切片 generation 等于期望值。
func expectTrackedGeneration(t *testing.T, tracker *endpointsliceutil.EndpointSliceTracker, slice *discovery.EndpointSlice, expectedGeneration int64) {
	gfs, ok := tracker.GenerationsForSliceUnsafe(slice)
	if !ok {
		t.Fatalf("Expected Service to be tracked for EndpointSlices %s", slice.Name)
	}
	generation, ok := gfs[slice.UID]
	if !ok {
		t.Fatalf("Expected EndpointSlice %s to be tracked", slice.Name)
	}
	if generation != expectedGeneration {
		t.Errorf("Expected Generation of %s to be %d, got %d", slice.Name, expectedGeneration, generation)
	}
}

// portsAndAddressTypeEqual 使用语义相等比较两个切片的端口与地址类型。
func portsAndAddressTypeEqual(slice1, slice2 discovery.EndpointSlice) bool {
	return apiequality.Semantic.DeepEqual(slice1.Ports, slice2.Ports) && apiequality.Semantic.DeepEqual(slice1.AddressType, slice2.AddressType)
}

// createEndpointSlices 把给定的 EndpointSlice 写入 fake clientset，用于准备预置切片。
func createEndpointSlices(t *testing.T, client *fake.Clientset, namespace string, endpointSlices []*discovery.EndpointSlice) {
	t.Helper()
	for _, endpointSlice := range endpointSlices {
		_, err := client.DiscoveryV1().EndpointSlices(namespace).Create(context.TODO(), endpointSlice, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("Expected no error creating Endpoint Slice, got: %v", err)
		}
	}
}

// fetchEndpointSlices 从 fake clientset 列出命名空间下的所有 EndpointSlice。
func fetchEndpointSlices(t *testing.T, client *fake.Clientset, namespace string) []discovery.EndpointSlice {
	t.Helper()
	fetchedSlices, err := client.DiscoveryV1().EndpointSlices(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("Expected no error fetching Endpoint Slices, got: %v", err)
		return []discovery.EndpointSlice{}
	}
	return fetchedSlices.Items
}

// reconcileHelper 调用 Reconcile，并在返回错误时直接终止测试，简化各用例调用。
func reconcileHelper(t *testing.T, r *Reconciler, service *corev1.Service, pods []*corev1.Pod, existingSlices []*discovery.EndpointSlice, triggerTime time.Time) {
	logger, _ := ktesting.NewTestContext(t)
	t.Helper()
	err := r.Reconcile(logger, service, pods, existingSlices, triggerTime)
	if err != nil {
		t.Fatalf("Expected no error reconciling Endpoint Slices, got: %v", err)
	}
}

// Metrics helpers

// expectedMetrics 汇总一次协调后期望的各项指标取值。
// desiredSlices 为理想切片数，actualSlices 为实际切片数，desiredEndpoints 为期望端点数；
// addedPerSync/removedPerSync 为单次同步新增/移除的端点数；
// numCreated/numUpdated/numDeleted 为切片增删改计数；
// slicesChangedPerSync 及其 Topology/TrafficDist 变体按不同维度记录切片变更次数；
// syncSuccesses/syncErrors 为同步成功/失败次数，servicesCountByTrafficDistribution 按 trafficDistribution 统计 Service 数。
type expectedMetrics struct {
	desiredSlices                      int
	actualSlices                       int
	desiredEndpoints                   int
	addedPerSync                       int
	removedPerSync                     int
	numCreated                         int
	numUpdated                         int
	numDeleted                         int
	slicesChangedPerSync               int
	slicesChangedPerSyncTopology       int
	slicesChangedPerSyncTrafficDist    int
	syncSuccesses                      int
	syncErrors                         int
	servicesCountByTrafficDistribution map[string]int
}

// expectMetrics 逐一读取 gauge、histogram 与 counter 指标，并断言取值与 expectedMetrics 一致。
func expectMetrics(t *testing.T, em expectedMetrics) {
	t.Helper()

	actualDesiredSlices, err := testutil.GetGaugeMetricValue(metrics.DesiredEndpointSlices.WithLabelValues())
	handleErr(t, err, "desiredEndpointSlices")
	if actualDesiredSlices != float64(em.desiredSlices) {
		t.Errorf("Expected desiredEndpointSlices to be %d, got %v", em.desiredSlices, actualDesiredSlices)
	}

	actualNumSlices, err := testutil.GetGaugeMetricValue(metrics.NumEndpointSlices.WithLabelValues())
	handleErr(t, err, "numEndpointSlices")
	if actualNumSlices != float64(em.actualSlices) {
		t.Errorf("Expected numEndpointSlices to be %d, got %v", em.actualSlices, actualNumSlices)
	}

	actualEndpointsDesired, err := testutil.GetGaugeMetricValue(metrics.EndpointsDesired.WithLabelValues())
	handleErr(t, err, "desiredEndpoints")
	if actualEndpointsDesired != float64(em.desiredEndpoints) {
		t.Errorf("Expected desiredEndpoints to be %d, got %v", em.desiredEndpoints, actualEndpointsDesired)
	}

	actualAddedPerSync, err := testutil.GetHistogramMetricValue(metrics.EndpointsAddedPerSync.WithLabelValues())
	handleErr(t, err, "endpointsAddedPerSync")
	if actualAddedPerSync != float64(em.addedPerSync) {
		t.Errorf("Expected endpointsAddedPerSync to be %d, got %v", em.addedPerSync, actualAddedPerSync)
	}

	actualRemovedPerSync, err := testutil.GetHistogramMetricValue(metrics.EndpointsRemovedPerSync.WithLabelValues())
	handleErr(t, err, "endpointsRemovedPerSync")
	if actualRemovedPerSync != float64(em.removedPerSync) {
		t.Errorf("Expected endpointsRemovedPerSync to be %d, got %v", em.removedPerSync, actualRemovedPerSync)
	}

	actualCreated, err := testutil.GetCounterMetricValue(metrics.EndpointSliceChanges.WithLabelValues("create"))
	handleErr(t, err, "endpointSliceChangesCreated")
	if actualCreated != float64(em.numCreated) {
		t.Errorf("Expected endpointSliceChangesCreated to be %d, got %v", em.numCreated, actualCreated)
	}

	actualUpdated, err := testutil.GetCounterMetricValue(metrics.EndpointSliceChanges.WithLabelValues("update"))
	handleErr(t, err, "endpointSliceChangesUpdated")
	if actualUpdated != float64(em.numUpdated) {
		t.Errorf("Expected endpointSliceChangesUpdated to be %d, got %v", em.numUpdated, actualUpdated)
	}

	actualDeleted, err := testutil.GetCounterMetricValue(metrics.EndpointSliceChanges.WithLabelValues("delete"))
	handleErr(t, err, "desiredEndpointSlices")
	if actualDeleted != float64(em.numDeleted) {
		t.Errorf("Expected endpointSliceChangesDeleted to be %d, got %v", em.numDeleted, actualDeleted)
	}

	actualSlicesChangedPerSync, err := testutil.GetHistogramMetricValue(metrics.EndpointSlicesChangedPerSync.WithLabelValues("Disabled", ""))
	handleErr(t, err, "slicesChangedPerSync")
	if actualSlicesChangedPerSync != float64(em.slicesChangedPerSync) {
		t.Errorf("Expected slicesChangedPerSync to be %d, got %v", em.slicesChangedPerSync, actualSlicesChangedPerSync)
	}

	actualSlicesChangedPerSyncTopology, err := testutil.GetHistogramMetricValue(metrics.EndpointSlicesChangedPerSync.WithLabelValues("Auto", ""))
	handleErr(t, err, "slicesChangedPerSyncTopology")
	if actualSlicesChangedPerSyncTopology != float64(em.slicesChangedPerSyncTopology) {
		t.Errorf("Expected slicesChangedPerSyncTopology to be %d, got %v", em.slicesChangedPerSyncTopology, actualSlicesChangedPerSyncTopology)
	}

	// 按 trafficDistribution 标签分别读取切片变更直方图。
	actualSlicesChangedPreferClose, err := testutil.GetHistogramMetricValue(metrics.EndpointSlicesChangedPerSync.WithLabelValues("Disabled", "PreferClose"))
	handleErr(t, err, "slicesChangedPreferClose")
	actualSlicesChangedPreferSameZone, err := testutil.GetHistogramMetricValue(metrics.EndpointSlicesChangedPerSync.WithLabelValues("Disabled", "PreferSameZone"))
	handleErr(t, err, "slicesChangedPreferSameZone")
	actualSlicesChangedPreferSameNode, err := testutil.GetHistogramMetricValue(metrics.EndpointSlicesChangedPerSync.WithLabelValues("Disabled", "PreferSameNode"))
	handleErr(t, err, "slicesChangedPreferSameNode")
	// 三种 trafficDistribution 的变更次数之和即为该指标取值。
	actualSlicesChangedPerSyncTrafficDist := actualSlicesChangedPreferClose + actualSlicesChangedPreferSameZone + actualSlicesChangedPreferSameNode
	if actualSlicesChangedPerSyncTrafficDist != float64(em.slicesChangedPerSyncTrafficDist) {
		t.Errorf("Expected slicesChangedPerSyncTrafficDist to be %d, got %v", em.slicesChangedPerSyncTrafficDist, actualSlicesChangedPerSyncTrafficDist)
	}

	// 校验同步成功与失败计数。
	actualSyncSuccesses, err := testutil.GetCounterMetricValue(metrics.EndpointSliceSyncs.WithLabelValues("success"))
	handleErr(t, err, "syncSuccesses")
	if actualSyncSuccesses != float64(em.syncSuccesses) {
		t.Errorf("Expected endpointSliceSyncSuccesses to be %d, got %v", em.syncSuccesses, actualSyncSuccesses)
	}

	actualSyncErrors, err := testutil.GetCounterMetricValue(metrics.EndpointSliceSyncs.WithLabelValues("error"))
	handleErr(t, err, "syncErrors")
	if actualSyncErrors != float64(em.syncErrors) {
		t.Errorf("Expected endpointSliceSyncErrors to be %d, got %v", em.syncErrors, actualSyncErrors)
	}

	// 遍历各 trafficDistribution 取值校验 Service 计数。
	for _, trafficDistribution := range []string{
		"PreferClose",
		"PreferSameZone",
		"PreferSameNode",
		"ImplementationSpecific",
	} {
		gotServicesCount, err := testutil.GetGaugeMetricValue(metrics.ServicesCountByTrafficDistribution.WithLabelValues(trafficDistribution))
		var wantServicesCount int
		if em.servicesCountByTrafficDistribution != nil {
			wantServicesCount = em.servicesCountByTrafficDistribution[trafficDistribution]
		}
		handleErr(t, err, fmt.Sprintf("%v[traffic_distribution=%v]", "services_count_by_traffic_distribution", trafficDistribution))
		if int(gotServicesCount) != wantServicesCount {
			t.Errorf("Expected servicesCountByTrafficDistribution for traffic_distribution=%v to be %v, got %v", trafficDistribution, wantServicesCount, gotServicesCount)
		}
	}
}

// handleErr 在读取指标出错时记录测试错误。
func handleErr(t *testing.T, err error, metricName string) {
	if err != nil {
		t.Errorf("Failed to get %s value, err: %v", metricName, err)
	}
}

// resetMetrics 重置全部控制器指标，避免用例之间相互污染。
func resetMetrics() {
	metrics.NumEndpointSlices.Reset()
	metrics.DesiredEndpointSlices.Reset()
	metrics.EndpointsDesired.Reset()
	metrics.EndpointsAddedPerSync.Reset()
	metrics.EndpointsRemovedPerSync.Reset()
	metrics.EndpointSliceChanges.Reset()
	metrics.EndpointSlicesChangedPerSync.Reset()
	metrics.EndpointSliceSyncs.Reset()
	metrics.ServicesCountByTrafficDistribution.Reset()
}
