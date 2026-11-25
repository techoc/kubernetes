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

package noderesources

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/version"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/dynamic-resource-allocation/cel"
	"k8s.io/dynamic-resource-allocation/deviceclass/extendedresourcecache"
	"k8s.io/dynamic-resource-allocation/resourceslice/tracker"
	"k8s.io/dynamic-resource-allocation/structured"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/dynamicresources"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	"k8s.io/kubernetes/pkg/scheduler/util"
	"k8s.io/kubernetes/pkg/scheduler/util/assumecache"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/utils/ptr"
)

// TestResourceAllocationScorerCalculateRequests 验证资源分配打分器对 Pod 请求量的计算。
func TestResourceAllocationScorerCalculateRequests(t *testing.T) {
	const oneMi = 1048576
	tests := []struct {
		name     string
		pod      v1.Pod
		expected map[v1.ResourceName]int64
	}{
		{
			name: "overhead only",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					Overhead: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("1"),
						v1.ResourceMemory: resource.MustParse("1Mi"),
					},
				},
			},
			expected: map[v1.ResourceName]int64{
				v1.ResourceCPU:    1000,
				v1.ResourceMemory: oneMi,
			},
		},
		{
			name: "1x requestless container",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{},
					},
				},
			},
			expected: map[v1.ResourceName]int64{
				v1.ResourceCPU:    util.DefaultMilliCPURequest,
				v1.ResourceMemory: util.DefaultMemoryRequest,
			},
		},
		{
			name: "2x requestless container",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{}, {},
					},
				},
			},
			// 没有资源请求的容器也应各累加一次 overhead。
			expected: map[v1.ResourceName]int64{
				v1.ResourceCPU:    2 * util.DefaultMilliCPURequest,
				v1.ResourceMemory: 2 * util.DefaultMemoryRequest,
			},
		},
		{
			name: "request container + requestless container",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("1"),
									v1.ResourceMemory: resource.MustParse("1Mi"),
								},
							},
						},
						{},
					},
				},
			},
			expected: map[v1.ResourceName]int64{
				v1.ResourceCPU:    1000 + util.DefaultMilliCPURequest,
				v1.ResourceMemory: oneMi + util.DefaultMemoryRequest,
			},
		},
		{
			name: "container + requestless container + overhead",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					Overhead: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("1"),
						v1.ResourceMemory: resource.MustParse("1Mi"),
					},
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("1"),
									v1.ResourceMemory: resource.MustParse("1Mi"),
								},
							},
						},
						{},
					},
				},
			},
			expected: map[v1.ResourceName]int64{
				v1.ResourceCPU:    2000 + util.DefaultMilliCPURequest,
				v1.ResourceMemory: 2*oneMi + util.DefaultMemoryRequest,
			},
		},
		{
			name: "init container + container + requestless container + overhead",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					Overhead: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("1"),
						v1.ResourceMemory: resource.MustParse("1Mi"),
					},
					InitContainers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceCPU: resource.MustParse("3"),
								},
							},
						},
					},
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("1"),
									v1.ResourceMemory: resource.MustParse("1Mi"),
								},
							},
						},
						{},
					},
				},
			},
			expected: map[v1.ResourceName]int64{
				v1.ResourceCPU:    4000,
				v1.ResourceMemory: 2*oneMi + util.DefaultMemoryRequest,
			},
		},
		{
			name: "requestless init container + small init container + small container ",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					InitContainers: []v1.Container{
						{},
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("1m"),
									v1.ResourceMemory: resource.MustParse("1"),
								},
							},
						},
					},
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("3m"),
									v1.ResourceMemory: resource.MustParse("3"),
								},
							},
						},
					},
				},
			},
			expected: map[v1.ResourceName]int64{
				v1.ResourceCPU:    util.DefaultMilliCPURequest,
				v1.ResourceMemory: util.DefaultMemoryRequest,
			},
		},
		{
			name: "requestless init container + small init container + small container + requestless container ",
			pod: v1.Pod{
				Spec: v1.PodSpec{
					InitContainers: []v1.Container{
						{},
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("1m"),
									v1.ResourceMemory: resource.MustParse("1"),
								},
							},
						},
					},
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Requests: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("3m"),
									v1.ResourceMemory: resource.MustParse("3"),
								},
							},
						},
						{},
					},
				},
			},
			expected: map[v1.ResourceName]int64{
				v1.ResourceCPU:    3 + util.DefaultMilliCPURequest,
				v1.ResourceMemory: 3 + util.DefaultMemoryRequest,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var scorer resourceAllocationScorer
			for n, exp := range tc.expected {
				got := scorer.calculatePodResourceRequest(&tc.pod, n)
				if got != exp {
					t.Errorf("expected %s = %d, got %d", n, exp, got)
				}
			}
		})
	}
}

// TestCalculateResourceAllocatableRequest 验证可分配资源与已分配资源的计算。
func TestCalculateResourceAllocatableRequest(t *testing.T) {
	testCalculateResourceAllocatableRequest(ktesting.Init(t))
}
func testCalculateResourceAllocatableRequest(tCtx ktesting.TContext) {
	// 初始化各测试共用变量。
	nodeName := "resource-node"
	driverName := "test-driver"
	testClaim := "test-claim"
	explicitExtendedResource := v1.ResourceName(extendedResourceName)
	implicitExtendedResource := v1.ResourceName(resourceapi.ResourceDeviceClassPrefix + deviceClassName)
	celCache := cel.NewCache(10, cel.Features{EnableConsumableCapacity: true})
	draFeatures := structured.Features{
		AdminAccess:            true,
		PrioritizedList:        true,
		PartitionableDevices:   true,
		DeviceTaints:           true,
		DeviceBindingAndStatus: true,
		ConsumableCapacity:     true,
	}

	// 定义覆盖不同 DRA 与资源类型的用例。
	tests := map[string]struct {
		enableDRAExtendedResource bool
		node                      *v1.Node
		extendedResource          v1.ResourceName
		draObjects                []apiruntime.Object
		expectedAllocatable       int64
		expectedAllocated         int64
	}{
		"device-plugin-resource-feature-disabled": {
			enableDRAExtendedResource: false,
			node:                      st.MakeNode().Name(nodeName).Capacity(map[v1.ResourceName]string{explicitExtendedResource: "4"}).Obj(),
			extendedResource:          explicitExtendedResource,
			expectedAllocatable:       4,
			expectedAllocated:         0,
		},
		"device-plugin-resource-feature-enabled": {
			enableDRAExtendedResource: true,
			node:                      st.MakeNode().Name(nodeName).Capacity(map[v1.ResourceName]string{explicitExtendedResource: "4"}).Obj(),
			extendedResource:          explicitExtendedResource,
			expectedAllocatable:       4,
			expectedAllocated:         0,
		},
		"DRA-backed-resource-explicit": {
			enableDRAExtendedResource: true,
			node:                      st.MakeNode().Name(nodeName).Obj(),
			extendedResource:          explicitExtendedResource,
			draObjects: []apiruntime.Object{
				deviceClassWithExtendResourceName,
				st.MakeResourceSlice(nodeName, driverName).Device("device-1").Obj(),
			},
			expectedAllocatable: 1, // 1 个设备 `device-1`
			expectedAllocated:   0,
		},
		"DRA-backed-resource-implicit": {
			enableDRAExtendedResource: true,
			node:                      st.MakeNode().Name(nodeName).Obj(),
			extendedResource:          implicitExtendedResource,
			draObjects: []apiruntime.Object{
				&resourceapi.DeviceClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: deviceClassName,
					},
				},
				st.MakeResourceSlice(nodeName, driverName).Device("device-1").Obj(),
			},
			expectedAllocatable: 1, // 1 个设备 `device-1`
			expectedAllocated:   0,
		},
		"DRA-backed-resource-no-slices": {
			enableDRAExtendedResource: true,
			node:                      st.MakeNode().Name(nodeName).Obj(),
			extendedResource:          explicitExtendedResource,
			draObjects: []apiruntime.Object{
				deviceClassWithExtendResourceName,
			},
			expectedAllocatable: 0,
			expectedAllocated:   0,
		},
		"DRA-backed-resource-with-allocated-device": {
			enableDRAExtendedResource: true,
			node:                      st.MakeNode().Name(nodeName).Obj(),
			extendedResource:          explicitExtendedResource,
			draObjects: []apiruntime.Object{
				deviceClassWithExtendResourceName,
				st.MakeResourceSlice(nodeName, driverName).Devices("device-1", "device-2").Obj(),
				// 构造把 device-1 完全占用的 ResourceClaim。
				st.MakeResourceClaim().
					Name(testClaim).
					Request(deviceClassName).
					Allocation(&resourceapi.AllocationResult{
						Devices: resourceapi.DeviceAllocationResult{
							Results: []resourceapi.DeviceRequestAllocationResult{
								{
									Request: "req-1",
									Driver:  driverName,
									Pool:    nodeName,
									Device:  "device-1",
								},
							},
						},
					}).
					Obj(),
			},
			expectedAllocatable: 2, // 2 个可分配设备 `device-1`、`device-2`
			expectedAllocated:   1, // `testClaim` 已分配 1 个
		},
		"DRA-backed-resource-with-shared-device-allocation": {
			enableDRAExtendedResource: true,
			node:                      st.MakeNode().Name(nodeName).Obj(),
			extendedResource:          explicitExtendedResource,
			draObjects: []apiruntime.Object{
				deviceClassWithExtendResourceName,
				st.MakeResourceSlice(nodeName, driverName).Devices("device-1", "device-2").Obj(),
				// 构造共享分配（可消费容量）的 ResourceClaim。
				st.MakeResourceClaim().
					Name(testClaim).
					Request(deviceClassName).
					Allocation(&resourceapi.AllocationResult{
						Devices: resourceapi.DeviceAllocationResult{
							Results: []resourceapi.DeviceRequestAllocationResult{
								{
									Request: "req-1",
									Driver:  driverName,
									Pool:    nodeName,
									Device:  "device-1",
									ShareID: ptr.To(types.UID("share-123")), // 共享设备分配
								},
							},
						},
					}).
					Obj(),
			},
			expectedAllocatable: 2, // 2 个可分配设备 `device-1`、`device-2`
			expectedAllocated:   1, // `testClaim` 已分配 1 个（共享）
		},
		"DRA-backed-resource-multiple-devices-mixed-allocation": {
			enableDRAExtendedResource: true,
			node:                      st.MakeNode().Name(nodeName).Obj(),
			extendedResource:          explicitExtendedResource,
			draObjects: []apiruntime.Object{
				deviceClassWithExtendResourceName,
				st.MakeResourceSlice(nodeName, driverName).Devices("device-1", "device-2", "device-3").Obj(),
				// 混合完全分配与共享分配。
				st.MakeResourceClaim().
					Name("test-claim-1").
					Request(deviceClassName).
					Allocation(&resourceapi.AllocationResult{
						Devices: resourceapi.DeviceAllocationResult{
							Results: []resourceapi.DeviceRequestAllocationResult{
								{
									Request: "req-1",
									Driver:  driverName,
									Pool:    nodeName,
									Device:  "device-1",
									// 无 ShareID 表示设备被完全占用。
								},
							},
						},
					}).
					Obj(),
				st.MakeResourceClaim().
					Name("test-claim-2").
					Request(deviceClassName).
					Allocation(&resourceapi.AllocationResult{
						Devices: resourceapi.DeviceAllocationResult{
							Results: []resourceapi.DeviceRequestAllocationResult{
								{
									Request: "req-1",
									Driver:  driverName,
									Pool:    nodeName,
									Device:  "device-2",
									ShareID: ptr.To(types.UID("share-456")), // 共享设备分配
								},
							},
						},
					}).
					Obj(),
				// device-3 保持未分配。
			},
			expectedAllocatable: 3, // 3 个可分配设备 `device-1`、`device-2`、`device-3`
			expectedAllocated:   2, // 已分配 2 个（`test-claim-1` 完全占用 1 个，`test-claim-2` 共享 1 个）
		},
		"DRA-backed-resource-with-per-device-node-selection": {
			enableDRAExtendedResource: true,
			node:                      st.MakeNode().Name(nodeName).Label("zone", "us-east-1a").Obj(),
			extendedResource:          explicitExtendedResource,
			draObjects: []apiruntime.Object{
				&resourceapi.DeviceClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: deviceClassName,
					},
					Spec: resourceapi.DeviceClassSpec{
						ExtendedResourceName: &extendedResourceName,
						Selectors: []resourceapi.DeviceSelector{
							{
								CEL: &resourceapi.CELDeviceSelector{
									// 模拟真实 GPU 选择：匹配测试驱动，要求内存至少 8GB 且型号以 SOME- 开头。
									Expression: `device.driver == "test-driver" && device.capacity["test-driver"].memory.compareTo(quantity("8Gi")) >= 0 && device.attributes["test-driver"].model.startsWith("SOME-")`,
								},
							},
						},
					},
				},
				// 构造设备级节点选择的 ResourceSlice。
				&resourceapi.ResourceSlice{
					ObjectMeta: metav1.ObjectMeta{Name: "per-device-slice"},
					Spec: resourceapi.ResourceSliceSpec{
						Driver:                 driverName, // 与 CEL 表达式中的驱动匹配
						Pool:                   resourceapi.ResourcePool{Name: "per-device-pool", ResourceSliceCount: 1},
						PerDeviceNodeSelection: ptr.To(true), // 启用设备级节点选择
						// 注意 PerDeviceNodeSelection 开启时，slice 级别不能设置 NodeName/AllNodes/NodeSelector。
						Devices: []resourceapi.Device{
							{
								Name:     "device-1",
								NodeName: ptr.To(nodeName), // 该设备匹配测试节点
								Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
									"model": {StringValue: ptr.To("SOME-XYZ")},
								},
								Capacity: map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
									"memory": {Value: resource.MustParse("16Gi")}, // 16GB GPU，匹配 CEL（>= 8GB）
								},
							},
							{
								Name:     "device-2",
								NodeName: ptr.To("other-node"), // 该设备匹配另一个节点
								Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
									"model": {StringValue: ptr.To("SOME-ZYX")},
								},
								Capacity: map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
									"memory": {Value: resource.MustParse("12Gi")}, // 12GB GPU，匹配 CEL（>= 8GB）
								},
							},
							{
								Name: "device-3",
								// 用节点选择器确保该设备会走到 nodeMatches 判断。
								NodeSelector: &v1.NodeSelector{
									NodeSelectorTerms: []v1.NodeSelectorTerm{
										{
											MatchExpressions: []v1.NodeSelectorRequirement{
												{
													Key:      "zone",
													Operator: v1.NodeSelectorOpIn,
													Values:   []string{"us-east-1a"},
												},
											},
										},
									},
								},
								Attributes: map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
									"model": {StringValue: ptr.To("SOME-XZY")},
								},
								Capacity: map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
									"memory": {Value: resource.MustParse("24Gi")}, // 24GB GPU，匹配 CEL（>= 8GiB）
								},
							},
						},
					},
				},
				// 构造占用 gpu-1 的 ResourceClaim。
				st.MakeResourceClaim().
					Name("test-claim").
					Request(deviceClassName).
					Allocation(&resourceapi.AllocationResult{
						Devices: resourceapi.DeviceAllocationResult{
							Results: []resourceapi.DeviceRequestAllocationResult{
								{
									Request: "req-1",
									Driver:  driverName,
									Pool:    "per-device-pool",
									Device:  "device-1",
								},
							},
						},
					}).
					Obj(),
			},
			expectedAllocatable: 2, // device-1 匹配测试节点，device-3 通过选择器匹配
			expectedAllocated:   1, // 已分配 1 个（device-1）
		},
	}

	for name, tc := range tests {
		tCtx.SyncTest(name, func(tCtx ktesting.TContext) {
			// 搭建测试环境，创建所需对象。
			if !tc.enableDRAExtendedResource {
				featuregatetesting.SetFeatureGateEmulationVersionDuringTest(tCtx, utilfeature.DefaultFeatureGate, version.MustParse("1.36"))
			}
			featuregatetesting.SetFeatureGateDuringTest(tCtx, utilfeature.DefaultFeatureGate, features.DRAExtendedResource, tc.enableDRAExtendedResource)

			draManager := newTestDRAManager(tCtx, tc.draObjects...)

			nodeInfo := framework.NewNodeInfo()
			nodeInfo.SetNode(tc.node)

			scorer := &resourceAllocationScorer{
				enableDRAExtendedResource: tc.enableDRAExtendedResource,
				draManager:                draManager,
				draFeatures:               draFeatures,
				DRACaches: DRACaches{
					celCache: celCache,
				},
			}

			var draPreScoreState *draPreScoreState
			if tc.enableDRAExtendedResource {
				var status *fwk.Status
				draPreScoreState, status = getDRAPreScoredParams(draManager, []config.ResourceSpec{{Name: string(tc.extendedResource)}})
				if status != nil {
					tCtx.Fatalf("getting DRA pre-scored params failed: %v", status)
				}
			}

			// 验证 calculateResourceAllocatableRequest 的计算结果。
			allocatable, allocated := scorer.calculateResourceAllocatableRequest(tCtx, nodeInfo, tc.extendedResource, draPreScoreState)
			if !cmp.Equal(allocatable, tc.expectedAllocatable) {
				tCtx.Errorf("Expected allocatable=%v, but got %v", tc.expectedAllocatable, allocatable)
			}
			if !cmp.Equal(allocated, tc.expectedAllocated) {
				tCtx.Errorf("Expected allocated=%v, but got %v", tc.expectedAllocated, allocated)
			}
		})
	}
}

// newTestDRAManager 创建测试用 DefaultDRAManager。
// 只能在 syntest bubble 中使用。
func newTestDRAManager(tCtx ktesting.TContext, objects ...apiruntime.Object) *dynamicresources.DefaultDRAManager {
	tCtx = tCtx.WithCancel()
	client := fake.NewClientset(objects...)
	informerFactory := informers.NewSharedInformerFactory(client, 0)
	resourceSliceTrackerOpts := tracker.Options{
		SliceInformer: informerFactory.Resource().V1().ResourceSlices(),
		KubeClient:    client,
	}
	resourceSliceTracker, err := tracker.StartTracker(tCtx, resourceSliceTrackerOpts)
	tCtx.ExpectNoError(err, "couldn't start resource slice tracker")
	draManager := dynamicresources.NewDRAManager(
		tCtx,
		assumecache.NewAssumeCache(
			tCtx.Logger(),
			informerFactory.Resource().V1().ResourceClaims().Informer(),
			"resource claim",
			"",
			nil),
		resourceSliceTracker,
		informerFactory)

	cache := draManager.DeviceClassResolver().(*extendedresourcecache.ExtendedResourceCache)
	handle, err := informerFactory.Resource().V1().DeviceClasses().Informer().AddEventHandler(cache)
	tCtx.ExpectNoError(err, "add device class informer event handler")
	tCtx.Cleanup(func() {
		_ = informerFactory.Resource().V1().DeviceClasses().Informer().RemoveEventHandler(handle)
	})

	informerFactory.Start(tCtx.Done())
	tCtx.Cleanup(func() {
		tCtx.Cancel("test has completed")
		// 现在可以等待所有 goroutine 退出。
		informerFactory.Shutdown()
	})
	informerFactory.WaitForCacheSync(tCtx.Done())

	// 等待 manager 完全初始化，包括所有 informer 事件的处理。
	tCtx.Wait()

	return draManager
}

// getCachedDeviceMatch 查询 DeviceMatches 缓存。
// 返回 (是否匹配, 是否命中)。
func (r *resourceAllocationScorer) getCachedDeviceMatch(expression string, driver string, poolName string, deviceName string) (bool, bool) {
	key := buildDeviceMatchCacheKey(expression, driver, poolName, deviceName)

	if value, ok := r.deviceMatchCache.Load(key); ok {
		return value.(bool), true
	}

	return false, false
}

// setCachedDeviceMatch 把 DeviceMatches 结果写入缓存。
func (r *resourceAllocationScorer) setCachedDeviceMatch(expression string, driver string, poolName string, deviceName string, matches bool) {
	key := buildDeviceMatchCacheKey(expression, driver, poolName, deviceName)
	r.deviceMatchCache.Store(key, matches)
}

// TestDeviceMatchCaching 验证设备匹配结果的缓存读写行为。
func TestDeviceMatchCaching(t *testing.T) {
	// 创建启用缓存的打分器。
	scorer := &resourceAllocationScorer{
		DRACaches: DRACaches{
			celCache: cel.NewCache(10, cel.Features{}),
		},
	}

	expression := `device.attributes["example.com"].test_attr == "test-value"`
	driverName := "example.com/test-driver"
	poolName := "example-pool"
	deviceName := "device-1"

	// 验证缓存操作：初始时缓存应为空。
	matches, found := scorer.getCachedDeviceMatch(expression, driverName, poolName, deviceName)
	if found {
		t.Errorf("Cache should be empty initially, but found an entry")
	}
	if matches {
		t.Errorf("Matches should be false initially, but got true")
	}

	// 向缓存写入结果。
	scorer.setCachedDeviceMatch(expression, driverName, poolName, deviceName, true)

	// 从缓存读取。
	matches, found = scorer.getCachedDeviceMatch(expression, driverName, poolName, deviceName)
	if !found {
		t.Errorf("Result should be found in cache, but was not")
	}
	if !matches {
		t.Errorf("Cached result should match what we stored, expected true but got false")
	}

	// 验证错误结果的缓存行为。
	scorer.setCachedDeviceMatch(expression, driverName, poolName, deviceName, false)

	matches, found = scorer.getCachedDeviceMatch(expression, driverName, poolName, deviceName)
	if !found {
		t.Errorf("Result should be found in cache")
	}
	if matches {
		t.Errorf("Cached result should match what we stored, expected false but got true")
	}

	// 验证不同设备对应不同缓存键。
	matches, found = scorer.getCachedDeviceMatch(expression, driverName, poolName, "device-2")
	if found {
		t.Errorf("Different device should not hit cache")
	}
	if matches {
		t.Errorf("Matches should be false for uncached entry")
	}

	// 验证不同设备池对应不同缓存键。
	matches, found = scorer.getCachedDeviceMatch(expression, driverName, "other-pool", deviceName)
	if found {
		t.Errorf("Different pool should not hit cache")
	}
	if matches {
		t.Errorf("Matches should be false for uncached entry")
	}
}

// BenchmarkDeviceMatchCaching 对比设备匹配缓存命中与未命中的性能。
func BenchmarkDeviceMatchCaching(b *testing.B) {
	expression := `device.attributes["example.com"].test_attr == "test-value"`
	driverName := "example.com/test-driver"
	poolName := "example-pool"
	deviceName := "device-1"

	// 创建启用缓存的打分器。
	scorer := &resourceAllocationScorer{
		DRACaches: DRACaches{
			celCache: cel.NewCache(10, cel.Features{}),
		},
	}

	b.Run("WithoutCache", func(b *testing.B) {
		// 每次创建新打分器以禁用缓存。
		for i := 0; i < b.N; i++ {
			freshScorer := &resourceAllocationScorer{
				DRACaches: DRACaches{
					celCache: cel.NewCache(10, cel.Features{}),
				},
			}

			// 必然缓存未命中。
			_, _ = freshScorer.getCachedDeviceMatch(expression, driverName, poolName, deviceName)
		}
	})

	b.Run("WithCache", func(b *testing.B) {
		// 预热缓存。
		scorer.setCachedDeviceMatch(expression, driverName, poolName, deviceName, true)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// 必然缓存命中。
			_, _ = scorer.getCachedDeviceMatch(expression, driverName, poolName, deviceName)
		}
	})
}

// getCachedNodeMatch 查询 NodeMatches 缓存。
func (r *resourceAllocationScorer) getCachedNodeMatch(nodeName string, nodeNameToMatch string, allNodesMatch bool, nodeSelectorHash string) (bool, bool) {
	key := buildNodeMatchCacheKey(nodeName, nodeNameToMatch, allNodesMatch, nodeSelectorHash)

	if value, ok := r.nodeMatchCache.Load(key); ok {
		return value.(bool), true
	}

	return false, false
}

// setCachedNodeMatch 把 NodeMatches 结果写入缓存。
func (r *resourceAllocationScorer) setCachedNodeMatch(nodeName string, nodeNameToMatch string, allNodesMatch bool, nodeSelectorHash string, matches bool) {
	key := buildNodeMatchCacheKey(nodeName, nodeNameToMatch, allNodesMatch, nodeSelectorHash)
	r.nodeMatchCache.Store(key, matches)
}

// TestNodeMatchCaching 验证节点匹配结果的缓存读写行为。
func TestNodeMatchCaching(t *testing.T) {
	// 创建启用缓存的打分器。
	scorer := &resourceAllocationScorer{
		DRACaches: DRACaches{
			celCache: cel.NewCache(10, cel.Features{}),
		},
	}

	// 创建测试节点。
	testNode := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node-1",
			Labels: map[string]string{
				"zone":          "us-east-1a",
				"instance.type": "gpu-xlarge",
			},
		},
	}

	// 覆盖不同 NodeMatches 场景的用例。
	testCases := []struct {
		name            string
		nodeNameToMatch string
		allNodesMatch   bool
		nodeSelector    *v1.NodeSelector
		expectedMatch   bool
	}{
		{
			name:            "exact node name match",
			nodeNameToMatch: "test-node-1",
			allNodesMatch:   false,
			nodeSelector:    nil,
			expectedMatch:   true,
		},
		{
			name:            "node name mismatch",
			nodeNameToMatch: "different-node",
			allNodesMatch:   false,
			nodeSelector:    nil,
			expectedMatch:   false,
		},
		{
			name:            "all nodes match",
			nodeNameToMatch: "",
			allNodesMatch:   true,
			nodeSelector:    nil,
			expectedMatch:   true,
		},
		{
			name:            "node selector match",
			nodeNameToMatch: "",
			allNodesMatch:   false,
			nodeSelector: &v1.NodeSelector{
				NodeSelectorTerms: []v1.NodeSelectorTerm{
					{
						MatchExpressions: []v1.NodeSelectorRequirement{
							{
								Key:      "zone",
								Operator: v1.NodeSelectorOpIn,
								Values:   []string{"us-east-1a", "us-east-1b"},
							},
						},
					},
				},
			},
			expectedMatch: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			nodeSelectorStr := tc.nodeSelector.String()

			// 第一次调用应缓存未命中。
			_, found1 := scorer.getCachedNodeMatch(testNode.Name, tc.nodeNameToMatch, tc.allNodesMatch, nodeSelectorStr)
			assert.False(t, found1, "Cache should be empty initially")

			// 模拟写入缓存结果。
			scorer.setCachedNodeMatch(testNode.Name, tc.nodeNameToMatch, tc.allNodesMatch, nodeSelectorStr, tc.expectedMatch)

			// 第二次调用应缓存命中。
			matches2, found2 := scorer.getCachedNodeMatch(testNode.Name, tc.nodeNameToMatch, tc.allNodesMatch, nodeSelectorStr)
			assert.True(t, found2, "Result should be found in cache")
			assert.Equal(t, tc.expectedMatch, matches2, "Cached result should match expected value")
		})
	}
}

// TestNodeMatchesCacheHit 验证重复调用 nodeMatches 会命中缓存。
func TestNodeMatchesCacheHit(t *testing.T) {
	scorer := &resourceAllocationScorer{
		DRACaches: DRACaches{
			celCache: cel.NewCache(1, cel.Features{}),
		},
	}

	node := st.MakeNode().Name("cache-node").Label("zone", "us-east-1a").Obj()
	selector := &v1.NodeSelector{
		NodeSelectorTerms: []v1.NodeSelectorTerm{
			{
				MatchExpressions: []v1.NodeSelectorRequirement{
					{
						Key:      "zone",
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"us-east-1a"},
					},
				},
			},
		},
	}

	firstMatch, err := scorer.nodeMatches(node, "", false, selector)
	if err != nil {
		t.Fatalf("unexpected error while evaluating selector: %v", err)
	}
	if !firstMatch {
		t.Fatalf("expected nodeMatches to return true for the selector")
	}

	secondMatch, err := scorer.nodeMatches(node, "", false, selector)
	if err != nil {
		t.Fatalf("unexpected error while hitting cache: %v", err)
	}
	if !secondMatch {
		t.Fatalf("expected cached nodeMatches to be true")
	}
}

// BenchmarkNodeMatchCaching 对比节点匹配缓存命中与未命中的性能。
func BenchmarkNodeMatchCaching(b *testing.B) {
	// 创建测试节点。
	testNode := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-node-1",
			Labels: map[string]string{
				"zone":          "us-east-1a",
				"instance.type": "gpu-xlarge",
			},
		},
	}

	// 构造较复杂的 NodeSelector，让测试有实际意义。
	nodeSelector := &v1.NodeSelector{
		NodeSelectorTerms: []v1.NodeSelectorTerm{
			{
				MatchExpressions: []v1.NodeSelectorRequirement{
					{
						Key:      "zone",
						Operator: v1.NodeSelectorOpIn,
						Values:   []string{"us-east-1a", "us-east-1b", "us-west-2a"},
					},
					{
						Key:      "instance.type",
						Operator: v1.NodeSelectorOpExists,
					},
				},
			},
		},
	}

	b.Run("WithoutCache", func(b *testing.B) {
		nodeSelectorStr := nodeSelector.String()

		// 每轮创建新打分器以避免缓存影响。
		for i := 0; i < b.N; i++ {
			freshScorer := &resourceAllocationScorer{
				DRACaches: DRACaches{
					celCache: cel.NewCache(10, cel.Features{}),
				},
			}

			// 必然缓存未命中。
			_, _ = freshScorer.getCachedNodeMatch(testNode.Name, "", false, nodeSelectorStr)
		}
	})

	b.Run("WithCache", func(b *testing.B) {
		nodeSelectorStr := nodeSelector.String()

		// 创建打分器并预热缓存。
		scorer := &resourceAllocationScorer{
			DRACaches: DRACaches{
				celCache: cel.NewCache(10, cel.Features{}),
			},
		}

		// 预热缓存。
		scorer.setCachedNodeMatch(testNode.Name, "", false, nodeSelectorStr, true)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// 必然缓存命中。
			_, _ = scorer.getCachedNodeMatch(testNode.Name, "", false, nodeSelectorStr)
		}
	})
}
