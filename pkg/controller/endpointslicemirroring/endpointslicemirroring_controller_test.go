/*
Copyright 2020 The Kubernetes Authors.

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

package endpointslicemirroring

import (
	"context"
	"fmt"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/controller"
)

// 与 EndpointSlice 分配相关的大部分测试在 reconciler_test.go 中。
// 这里的测试主要关注 reconciler 开始之前控制器独有的功能。

// alwaysReady 始终返回 true，用于在单测中绕过 informer 同步等待。
var alwaysReady = func() bool { return true }

// endpointSliceMirroringController 包装 Controller，同时暴露各 informer 的 store，
// 便于测试直接向缓存中预置对象。
type endpointSliceMirroringController struct {
	*Controller
	endpointsStore     cache.Store
	endpointSliceStore cache.Store
	serviceStore       cache.Store
}

// newController 创建使用 fake client 的镜像控制器，并绕过 Run() 启动事件处理相关组件。
func newController(ctx context.Context, batchPeriod time.Duration) (*fake.Clientset, *endpointSliceMirroringController) {
	client := newClientset()
	informerFactory := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())

	esController := NewController(
		ctx,
		informerFactory.Core().V1().Endpoints(),
		informerFactory.Discovery().V1().EndpointSlices(),
		informerFactory.Core().V1().Services(),
		int32(1000),
		client,
		batchPeriod)

	// 事件处理管线通常由 Run() 启动；单测中不会调用 Run()，因此这里手动启动。
	esController.eventBroadcaster.StartLogging(klog.Infof)
	esController.eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: client.CoreV1().Events("")})

	esController.endpointsSynced = alwaysReady
	esController.endpointSlicesSynced = alwaysReady
	esController.servicesSynced = alwaysReady

	return client, &endpointSliceMirroringController{
		esController,
		informerFactory.Core().V1().Endpoints().Informer().GetStore(),
		informerFactory.Discovery().V1().EndpointSlices().Informer().GetStore(),
		informerFactory.Core().V1().Services().Informer().GetStore(),
	}
}

// TestSyncEndpoints 覆盖 syncEndpoints 的多种场景：跳过镜像、清理旧切片、上限截断等。
func TestSyncEndpoints(t *testing.T) {
	endpointsName := "testing-sync-endpoints"
	namespace := metav1.NamespaceDefault

	testCases := []struct {
		testName           string
		service            *v1.Service
		endpoints          *v1.Endpoints
		endpointSlices     []*discovery.EndpointSlice
		expectedNumActions int
		expectedNumSlices  int
	}{{
		// 没有地址时不应创建任何 EndpointSlice。
		testName: "Endpoints with no addresses",
		service:  &v1.Service{},
		endpoints: &v1.Endpoints{
			Subsets: []v1.EndpointSubset{{
				Ports: []v1.EndpointPort{{Port: 80}},
			}},
		},
		endpointSlices:     []*discovery.EndpointSlice{},
		expectedNumActions: 0,
		expectedNumSlices:  0,
	}, {
		// skip-mirror=true 明确要求跳过镜像。
		testName: "Endpoints with skip label true",
		service:  &v1.Service{},
		endpoints: &v1.Endpoints{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{discovery.LabelSkipMirror: "true"},
			},
			Subsets: []v1.EndpointSubset{{
				Ports:     []v1.EndpointPort{{Port: 80}},
				Addresses: []v1.EndpointAddress{{IP: "10.0.0.1"}},
			}},
		},
		endpointSlices:     []*discovery.EndpointSlice{},
		expectedNumActions: 0,
		expectedNumSlices:  0,
	}, {
		// skip-mirror=false 不影响正常镜像。
		testName: "Endpoints with skip label false",
		service:  &v1.Service{},
		endpoints: &v1.Endpoints{
			ObjectMeta: metav1.ObjectMeta{
				Labels: map[string]string{discovery.LabelSkipMirror: "false"},
			},
			Subsets: []v1.EndpointSubset{{
				Ports:     []v1.EndpointPort{{Port: 80}},
				Addresses: []v1.EndpointAddress{{IP: "10.0.0.1"}},
			}},
		},
		endpointSlices:     []*discovery.EndpointSlice{},
		expectedNumActions: 1,
		expectedNumSlices:  1,
	}, {
		// 没有对应的 Service 时不应镜像。
		testName: "Endpoints with missing Service",
		service:  nil,
		endpoints: &v1.Endpoints{
			Subsets: []v1.EndpointSubset{{
				Ports:     []v1.EndpointPort{{Port: 80}},
				Addresses: []v1.EndpointAddress{{IP: "10.0.0.1"}},
			}},
		},
		endpointSlices:     []*discovery.EndpointSlice{},
		expectedNumActions: 0,
		expectedNumSlices:  0,
	}, {
		// Service 带 selector 时由 EndpointSlice 控制器负责，不应镜像。
		testName: "Endpoints with Service with selector specified",
		service: &v1.Service{
			Spec: v1.ServiceSpec{
				Selector: map[string]string{"foo": "bar"},
			},
		},
		endpoints: &v1.Endpoints{
			Subsets: []v1.EndpointSubset{{
				Ports:     []v1.EndpointPort{{Port: 80}},
				Addresses: []v1.EndpointAddress{{IP: "10.0.0.1"}},
			}},
		},
		endpointSlices:     []*discovery.EndpointSlice{},
		expectedNumActions: 0,
		expectedNumSlices:  0,
	}, {
		// 已存在本控制器管理的旧切片且无地址，应被清理掉。
		testName: "Existing EndpointSlices that need to be cleaned up",
		service:  &v1.Service{},
		endpoints: &v1.Endpoints{
			Subsets: []v1.EndpointSubset{{
				Ports: []v1.EndpointPort{{Port: 80}},
			}},
		},
		endpointSlices: []*discovery.EndpointSlice{{
			ObjectMeta: metav1.ObjectMeta{
				Name: endpointsName + "-1",
				Labels: map[string]string{
					discovery.LabelServiceName: endpointsName,
					discovery.LabelManagedBy:   ControllerName,
				},
			},
		}},
		expectedNumActions: 1,
		expectedNumSlices:  0,
	}, {
		// 其他控制器的切片不应被本控制器清理。
		testName: "Existing EndpointSlices managed by a different controller, no addresses to sync",
		service:  &v1.Service{},
		endpoints: &v1.Endpoints{
			Subsets: []v1.EndpointSubset{{
				Ports: []v1.EndpointPort{{Port: 80}},
			}},
		},
		endpointSlices: []*discovery.EndpointSlice{{
			ObjectMeta: metav1.ObjectMeta{
				Name: endpointsName + "-1",
				Labels: map[string]string{
					discovery.LabelManagedBy: "something-else",
				},
			},
		}},
		expectedNumActions: 0,
		// 查询只覆盖本控制器管理的 EndpointSlice，故预期为 0。
		expectedNumSlices: 0,
	}, {
		// 正好 1000 个地址，位于每个切片上限之内。
		testName: "Endpoints with 1000 addresses",
		service:  &v1.Service{},
		endpoints: &v1.Endpoints{
			Subsets: []v1.EndpointSubset{{
				Ports:     []v1.EndpointPort{{Port: 80}},
				Addresses: generateAddresses(1000),
			}},
		},
		endpointSlices:     []*discovery.EndpointSlice{},
		expectedNumActions: 1,
		expectedNumSlices:  1,
	}, {
		// 1001 个地址超出上限，多余地址不镜像并触发告警事件。
		testName: "Endpoints with 1001 addresses - 1 should not be mirrored",
		service:  &v1.Service{},
		endpoints: &v1.Endpoints{
			Subsets: []v1.EndpointSubset{{
				Ports:     []v1.EndpointPort{{Port: 80}},
				Addresses: generateAddresses(1001),
			}},
		},
		endpointSlices:     []*discovery.EndpointSlice{},
		expectedNumActions: 2, // 额外一次 action 用于创建告警事件。
		expectedNumSlices:  1,
	}}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			client, esController := newController(ctx, time.Duration(0))
			tc.endpoints.Name = endpointsName
			tc.endpoints.Namespace = namespace
			esController.endpointsStore.Add(tc.endpoints)
			if tc.service != nil {
				tc.service.Name = endpointsName
				tc.service.Namespace = namespace
				esController.serviceStore.Add(tc.service)
			}

			for _, epSlice := range tc.endpointSlices {
				epSlice.Namespace = namespace
				esController.endpointSliceStore.Add(epSlice)
				_, err := client.DiscoveryV1().EndpointSlices(namespace).Create(context.TODO(), epSlice, metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("Expected no error creating EndpointSlice, got %v", err)
				}
			}

			// 直接调用 syncEndpoints，避开事件队列的异步性，同步判断结果。
			_, syncCtx := ktesting.NewTestContext(t)
			err := esController.syncEndpoints(syncCtx, fmt.Sprintf("%s/%s", namespace, endpointsName))
			if err != nil {
				t.Fatalf("Unexpected error from syncEndpoints: %v", err)
			}

			numInitialActions := len(tc.endpointSlices)
			// 预置切片会产生初始 action；轮询等待额外 action 出现，
			// 主要用于"1001 个地址"场景中异步产生的告警事件。
			err = wait.PollImmediate(time.Millisecond*100, wait.ForeverTestTimeout, func() (done bool, err error) {
				actions := client.Actions()
				numExtraActions := len(actions) - numInitialActions
				if numExtraActions != tc.expectedNumActions {
					t.Logf("Expected %d additional client actions, got %d: %#v. Will retry", tc.expectedNumActions, numExtraActions, actions[numInitialActions:])
					return false, nil
				}
				return true, nil
			})
			if err != nil {
				t.Fatal("Timed out waiting for expected actions")
			}

			endpointSlices := fetchEndpointSlices(t, client, namespace)
			expectEndpointSlices(t, tc.expectedNumSlices, int(defaultMaxEndpointsPerSubset), *tc.endpoints, endpointSlices)
		})
	}
}

// TestShouldMirror 验证 shouldMirror 对各种标签和注解的判断：
// 仅 skip-mirror=true 或存在 leader election 注解时应跳过镜像。
func TestShouldMirror(t *testing.T) {
	testCases := []struct {
		testName     string
		endpoints    *v1.Endpoints
		shouldMirror bool
	}{{
		// 普通 Endpoints 默认应镜像。
		testName: "Standard Endpoints",
		endpoints: &v1.Endpoints{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-endpoints",
			},
		},
		shouldMirror: true,
	}, {
		// skip-mirror=true 为有效值，应跳过镜像。
		testName: "Endpoints with skip-mirror=true",
		endpoints: &v1.Endpoints{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-endpoints",
				Labels: map[string]string{
					discovery.LabelSkipMirror: "true",
				},
			},
		},
		shouldMirror: false,
	}, {
		// 非布尔值视为无效，按默认行为镜像。
		testName: "Endpoints with skip-mirror=invalid",
		endpoints: &v1.Endpoints{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-endpoints",
				Labels: map[string]string{
					discovery.LabelSkipMirror: "invalid",
				},
			},
		},
		shouldMirror: true,
	}, {
		// 带 leader election 注解的 Endpoints 由 leader election 机制使用，不应镜像。
		testName: "Endpoints with leader election annotation",
		endpoints: &v1.Endpoints{
			ObjectMeta: metav1.ObjectMeta{
				Name: "test-endpoints",
				Annotations: map[string]string{
					resourcelock.LeaderElectionRecordAnnotationKey: "",
				},
			},
		},
		shouldMirror: false,
	}}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			_, c := newController(ctx, time.Duration(0))

			if tc.endpoints != nil {
				err := c.endpointsStore.Add(tc.endpoints)
				if err != nil {
					t.Fatalf("Error adding Endpoints to store: %v", err)
				}
			}

			shouldMirror := c.shouldMirror(tc.endpoints)

			// 断言实际判断结果符合预期，覆盖 true/false 两种分支。
			if shouldMirror != tc.shouldMirror {
				t.Errorf("Expected %t to be returned, got %t", tc.shouldMirror, shouldMirror)
			}
		})
	}
}

// TestEndpointSlicesMirroredForService 验证 endpointSlicesMirroredForService 的标签筛选逻辑：
// 只有命名空间、Service 名称和 managed-by 均匹配的切片才会被返回。
func TestEndpointSlicesMirroredForService(t *testing.T) {
	testCases := []struct {
		testName       string
		namespace      string
		name           string
		endpointSlice  *discovery.EndpointSlice
		expectedInList bool
	}{{
		// 各项标签都匹配，应返回该切片。
		testName:  "Service with matching EndpointSlice",
		namespace: "ns1",
		name:      "svc1",
		endpointSlice: &discovery.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "example-1",
				Namespace: "ns1",
				Labels: map[string]string{
					discovery.LabelServiceName: "svc1",
					discovery.LabelManagedBy:   ControllerName,
				},
			},
		},
		expectedInList: true,
	}, {
		// 命名空间不匹配，不应返回。
		testName:  "Service with EndpointSlice that has different namespace",
		namespace: "ns1",
		name:      "svc1",
		endpointSlice: &discovery.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "example-1",
				Namespace: "ns2",
				Labels: map[string]string{
					discovery.LabelServiceName: "svc1",
					discovery.LabelManagedBy:   ControllerName,
				},
			},
		},
		expectedInList: false,
	}, {
		// Service 名称不匹配，不应返回。
		testName:  "Service with EndpointSlice that has different service name",
		namespace: "ns1",
		name:      "svc1",
		endpointSlice: &discovery.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "example-1",
				Namespace: "ns1",
				Labels: map[string]string{
					discovery.LabelServiceName: "svc2",
					discovery.LabelManagedBy:   ControllerName,
				},
			},
		},
		expectedInList: false,
	}, {
		// managed-by 不匹配，不应返回。
		testName:  "Service with EndpointSlice that has different controller name",
		namespace: "ns1",
		name:      "svc1",
		endpointSlice: &discovery.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "example-1",
				Namespace: "ns1",
				Labels: map[string]string{
					discovery.LabelServiceName: "svc1",
					discovery.LabelManagedBy:   ControllerName + "foo",
				},
			},
		},
		expectedInList: false,
	}, {
		// 缺少 managed-by 标签，不应返回。
		testName:  "Service with EndpointSlice that has missing controller name",
		namespace: "ns1",
		name:      "svc1",
		endpointSlice: &discovery.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "example-1",
				Namespace: "ns1",
				Labels: map[string]string{
					discovery.LabelServiceName: "svc1",
				},
			},
		},
		expectedInList: false,
	}, {
		// 缺少 service-name 标签，不应返回。
		testName:  "Service with EndpointSlice that has missing service name",
		namespace: "ns1",
		name:      "svc1",
		endpointSlice: &discovery.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "example-1",
				Namespace: "ns1",
				Labels: map[string]string{
					discovery.LabelManagedBy: ControllerName,
				},
			},
		},
		expectedInList: false,
	}}

	for _, tc := range testCases {
		t.Run(tc.testName, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			_, c := newController(ctx, time.Duration(0))

			err := c.endpointSliceStore.Add(tc.endpointSlice)
			if err != nil {
				t.Fatalf("Error adding EndpointSlice to store: %v", err)
			}

			// 通过 store 预置切片后，验证 lister 筛选结果的精确性。
			endpointSlices, err := endpointSlicesMirroredForService(c.endpointSliceLister, tc.namespace, tc.name)
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			if tc.expectedInList {
				if len(endpointSlices) != 1 {
					t.Fatalf("Expected 1 EndpointSlice to be in list, got %d", len(endpointSlices))
				}

				if endpointSlices[0].Name != tc.endpointSlice.Name {
					t.Fatalf("Expected %s EndpointSlice to be in list, got %s", tc.endpointSlice.Name, endpointSlices[0].Name)
				}
			} else {
				if len(endpointSlices) != 0 {
					t.Fatalf("Expected no EndpointSlices to be in list, got %d", len(endpointSlices))
				}
			}
		})
	}
}

// generateAddresses 构造指定数量的假 EndpointAddress（形如 10.0.x.y），供镜像测试填充 Endpoints 对象。
func generateAddresses(num int) []v1.EndpointAddress {
	addresses := make([]v1.EndpointAddress, num)
	for i := 0; i < num; i++ {
		part1 := i / 255
		part2 := i % 255
		ip := fmt.Sprintf("10.0.%d.%d", part1, part2)
		addresses[i] = v1.EndpointAddress{IP: ip}
	}
	return addresses
}
