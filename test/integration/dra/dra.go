/*
Copyright 2025 The Kubernetes Authors.

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
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	resourcealphaapi "k8s.io/api/resource/v1alpha3"
	resourcev1beta1 "k8s.io/api/resource/v1beta1"
	resourcev1beta2 "k8s.io/api/resource/v1beta2"
	schedulingapi "k8s.io/api/scheduling/v1beta1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/version"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
	"k8s.io/component-base/featuregate"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/component-helpers/nodedeclaredfeatures/features/draoptionalnodeoperations"
	"k8s.io/klog/v2"
	kubeschedulerconfigv1 "k8s.io/kube-scheduler/config/v1"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/pkg/controller/resourceclaim"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	kubeschedulerscheme "k8s.io/kubernetes/pkg/scheduler/apis/config/scheme"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/kubernetes/test/integration/util"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
	"k8s.io/kubernetes/test/utils/ktesting/format"
	"k8s.io/utils/ptr"
)

var (
	// 更多测试数据见 pkg/scheduler/framework/plugin/dynamicresources/dynamicresources_test.go。

	podName      = "my-pod"
	namespace    = "default"
	resourceName = "my-resource"
	claimName    = podName + "-" + resourceName
	className    = "my-resource-class"
	device1      = "device-1"
	device2      = "device-2"

	podWithClaimName = st.MakePod().Name(podName).Namespace(namespace).
				Container("my-container").
				PodResourceClaims(v1.PodResourceClaim{Name: resourceName, ResourceClaimName: &claimName}).
				Obj()
	class = &resourceapi.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: className,
		},
	}
	claim = st.MakeResourceClaim().
		Name(claimName).
		Namespace(namespace).
		Request(className).
		Obj()
	claimPrioritizedList = st.MakeResourceClaim().
				Name(claimName).
				Namespace(namespace).
				RequestWithPrioritizedList(st.SubRequest("subreq-1", className, 1)).
				Obj()
)

const (
	numNodes           = 8
	maxPodsPerNode     = 5000 // 无论多少测试并行，都不应让 Pod 数量成为限制因素。
	nodeCPUCapacity    = "100"
	nodeMemoryCapacity = "1k"

	// schedulingTimeout 是一次调度尝试（无论成功与否）允许消耗的时间。
	schedulingTimeout = time.Minute
)

// featureNodeSetups 将 feature gate 映射到针对该特性配置 node 的函数。
var featureNodeSetups = map[featuregate.Feature]func(tCtx ktesting.TContext, nodes []*v1.Node){
	features.DRAOptionalNodeOperations: func(tCtx ktesting.TContext, nodes []*v1.Node) {
		if len(nodes) == 0 {
			return
		}
		// 配置 worker-0 声明支持 DRAOptionalNodeOperations。
		node := nodes[0]
		node.Status.DeclaredFeatures = append(node.Status.DeclaredFeatures, draoptionalnodeoperations.DRAOptionalNodeOperationsFeatureGate)
		updatedNode, err := tCtx.Client().CoreV1().Nodes().UpdateStatus(tCtx, node, metav1.UpdateOptions{})
		tCtx.ExpectNoError(err, "updating status of node %s for DRAOptionalNodeOperations", node.Name)
		nodes[0] = updatedNode
	},
}

func Run(t *testing.T, whatRE string) { run(ktesting.Init(t), whatRE) }
func run(tCtx ktesting.TContext, whatRE string) {
	re, err := regexp.Compile(whatRE)
	if err != nil {
		tCtx.Fatalf("%s: %v", whatRE, err)
	}

	// 每个子测试以特定配置启动 API server。这些子测试必须串行运行，因为它们会修改
	// 全局 DefaultFeatureGate。同一配置下多个测试可以并行，前提是各自小心创建资源。
	//
	// 每种配置初始都有两个 Node（ready，RAM/CPU 足够运行多个 Pod）且没有 ResourceSlice。
	// 要测试调度，子测试必须创建 ResourceSlice。可用 createTestNamespace 创建各测试独有的
	// namespace，再用该 namespace 的名字创建 cluster 级对象，避免测试间冲突。
	//
	// 有些测试逻辑完全相同，却在不同配置下运行，以覆盖被测代码的不同分支。
	for name, tc := range map[string]struct {
		apis     map[schema.GroupVersion]bool
		version  string
		features map[featuregate.Feature]bool
		f        func(tCtx ktesting.TContext)
	}{
		"disabled": {
			version:  "1.34", // 1.34 中仍可禁用 DRA。
			apis:     map[schema.GroupVersion]bool{resourceapi.SchemeGroupVersion: false},
			features: map[featuregate.Feature]bool{features.DynamicResourceAllocation: false},
			f: func(tCtx ktesting.TContext) {
				runSubTest(tCtx, "APIDisabled", testAPIDisabled)
				runSubTest(tCtx, "Pod", func(tCtx ktesting.TContext) { testPod(tCtx, false) })
			},
		},
		"default": {
			apis:     map[schema.GroupVersion]bool{},
			features: map[featuregate.Feature]bool{},
			f: func(tCtx ktesting.TContext) {
				runSubTest(tCtx, "Pod", func(tCtx ktesting.TContext) { testPod(tCtx, true) })
				runSubTest(tCtx, "CompatibilityGroups", func(tCtx ktesting.TContext) { testCompatibilityGroups(tCtx, false) })
				runSubTest(tCtx, "PublishResourceSlices", func(tCtx ktesting.TContext) {
					testPublishResourceSlices(tCtx, true, features.DRADeviceCompatibilityGroups, features.DRAOptionalNodeOperations)
				})
				runSubTest(tCtx, "EvictClusterWithV1Rule", func(tCtx ktesting.TContext) { testEvictCluster(tCtx, useV1Rule) })
				runSubTest(tCtx, "EvictClusterWithSlices", func(tCtx ktesting.TContext) { testEvictCluster(tCtx, useNoRule) })
				runSubTest(tCtx, "NoScheduleWithSlices", func(tCtx ktesting.TContext) { testNoScheduleRule(tCtx, useNoRule) })
				// 每个 slice 的 device 数量选为让 Filter 耗时数秒：既不至于让测试太久，
				// 又足以触发较短的超时。
				runSubTest(tCtx, "FilterTimeout", func(tCtx ktesting.TContext) { testFilterTimeout(tCtx, 21) })
				runSubTest(tCtx, "UsesAllResources", testUsesAllResources)
			},
		},
		// 显式关闭 consumable capacity 的 compatibility group 配置。
		// 两个 feature gate 相互独立；DRAConsumableCapacity 关闭时，调度插件组装
		// allocator 眼中的已有分配会走另一条路径，但强制基线依赖的“已分配 device”
		// 信息在该路径下仍必须传递给 allocator。
		"compatibility-groups-without-consumable-capacity": {
			apis: map[schema.GroupVersion]bool{},
			features: map[featuregate.Feature]bool{
				features.DRAConsumableCapacity:        false,
				features.DRADeviceCompatibilityGroups: true,
				features.DRAPartitionableDevices:      true,
			},
			f: func(tCtx ktesting.TContext) {
				runSubTest(tCtx, "CompatibilityGroups", func(tCtx ktesting.TContext) { testCompatibilityGroups(tCtx, true) })
			},
		},
		// 覆盖仅启用 GA 特性的当前 Kubernetes 版本。
		// 其他版本的 GA 场景用于验证版本模拟是否正常工作，只复刻少量关键测试以控制总运行时长。
		"GA": {
			apis: map[schema.GroupVersion]bool{},
			features: map[featuregate.Feature]bool{
				featuregate.Feature("AllBeta"): false,
			},
			f: func(tCtx ktesting.TContext) {
				runSubTest(tCtx, "AdminAccess", func(tCtx ktesting.TContext) { testAdminAccess(tCtx, false) })
				runSubTest(tCtx, "EvictClusterWithV1Rule", func(tCtx ktesting.TContext) { testEvictCluster(tCtx, useV1Rule) })
				runSubTest(tCtx, "EvictClusterWithSlices", func(tCtx ktesting.TContext) { testEvictCluster(tCtx, useNoRule) })
				runSubTest(tCtx, "NoScheduleWithV1Rule", func(tCtx ktesting.TContext) { testNoScheduleRule(tCtx, useV1Rule) })
				runSubTest(tCtx, "NoScheduleWithSlices", func(tCtx ktesting.TContext) { testNoScheduleRule(tCtx, useNoRule) })
				runSubTest(tCtx, "PartitionableDevices", func(tCtx ktesting.TContext) { testPartitionableDevices(tCtx, false) })
				runSubTest(tCtx, "PrioritizedList", func(tCtx ktesting.TContext) { testPrioritizedList(tCtx, true) })
				runSubTest(tCtx, "Pod", func(tCtx ktesting.TContext) { testPod(tCtx, true) })
				runSubTest(tCtx, "PublishResourceSlices", func(tCtx ktesting.TContext) {
					testPublishResourceSlices(tCtx, true, features.DRAPartitionableDevices, features.DRADeviceBindingConditions, features.DRAOptionalNodeOperations)
				})
				runSubTest(tCtx, "ExplicitExtendedResource", func(tCtx ktesting.TContext) { testExtendedResource(tCtx, true, true) })
				runSubTest(tCtx, "ImplicitExtendedResource", func(tCtx ktesting.TContext) { testExtendedResource(tCtx, true, false) })
				runSubTest(tCtx, "ResourceClaimDeviceStatus", func(tCtx ktesting.TContext) { testResourceClaimDeviceStatus(tCtx, true) })
				runSubTest(tCtx, "DeviceBindingConditions", func(tCtx ktesting.TContext) { testDeviceBindingConditions(tCtx, false) })
				runSubTest(tCtx, "ResourceSliceController", func(tCtx ktesting.TContext) {
					namespace := createTestNamespace(tCtx, nil)
					tCtx = tCtx.WithNamespace(namespace)
					TestCreateResourceSlices(tCtx, 100)
				})
				runSubTest(tCtx, "ShareResourceClaimSequentially", testShareResourceClaimSequentially)
				runSubTest(tCtx, "UsesAllResources", testUsesAllResources)
				runSubTest(tCtx, "WorkloadResourceClaims", func(tCtx ktesting.TContext) { testWorkloadResourceClaims(tCtx, false, false) })
			},
		},
		"GA-1.36": {
			version: "1.36",
			apis:    map[schema.GroupVersion]bool{},
			features: map[featuregate.Feature]bool{
				featuregate.Feature("AllBeta"): false,
			},
			f: func(tCtx ktesting.TContext) {
				runSubTest(tCtx, "AdminAccess", func(tCtx ktesting.TContext) { testAdminAccess(tCtx, false) })
				runSubTest(tCtx, "PrioritizedList", func(tCtx ktesting.TContext) { testPrioritizedList(tCtx, true) })
				runSubTest(tCtx, "Pod", func(tCtx ktesting.TContext) { testPod(tCtx, true) })
				runSubTest(tCtx, "PublishResourceSlices", func(tCtx ktesting.TContext) {
					testPublishResourceSlices(tCtx, true, features.DRADeviceTaints, features.DRAPartitionableDevices, features.DRADeviceBindingConditions, features.DRAOptionalNodeOperations)
				})
			},
		},
		"GA-1.35": {
			version: "1.35",
			apis:    map[schema.GroupVersion]bool{},
			features: map[featuregate.Feature]bool{
				featuregate.Feature("AllBeta"): false,
			},
			f: func(tCtx ktesting.TContext) {
				runSubTest(tCtx, "AdminAccess", func(tCtx ktesting.TContext) { testAdminAccess(tCtx, false) })
				runSubTest(tCtx, "PrioritizedList", func(tCtx ktesting.TContext) { testPrioritizedList(tCtx, false) })
				runSubTest(tCtx, "Pod", func(tCtx ktesting.TContext) { testPod(tCtx, true) })
				runSubTest(tCtx, "PublishResourceSlices", func(tCtx ktesting.TContext) {
					testPublishResourceSlices(tCtx, true, features.DRADeviceTaints, features.DRAPartitionableDevices, features.DRADeviceBindingConditions, features.DRAOptionalNodeOperations)
				})
			},
		},
		"GA-opt-out-1.36": {
			version: "1.36",
			apis:    map[schema.GroupVersion]bool{},
			features: map[featuregate.Feature]bool{
				features.DRAResourceClaimDeviceStatus: false,
				features.DRAPrioritizedList:           false,
			},
			f: func(tCtx ktesting.TContext) {
				runSubTest(tCtx, "ResourceClaimDeviceStatus", func(tCtx ktesting.TContext) { testResourceClaimDeviceStatus(tCtx, false) })
				runSubTest(tCtx, "PrioritizedList", func(tCtx ktesting.TContext) { testPrioritizedList(tCtx, false) })
			},
		},
		"v1beta1": {
			apis: map[schema.GroupVersion]bool{
				resourceapi.SchemeGroupVersion:     false,
				resourcev1beta1.SchemeGroupVersion: true,
			},
			features: map[featuregate.Feature]bool{
				features.DynamicResourceAllocation:    true,
				features.DRADeviceCompatibilityGroups: true,
			},
			f: func(tCtx ktesting.TContext) {
				runSubTest(tCtx, "PublishResourceSlices", func(tCtx ktesting.TContext) {
					testPublishResourceSlices(tCtx, false, features.DRAOptionalNodeOperations)
				})
			},
		},
		"v1beta2": {
			apis: map[schema.GroupVersion]bool{
				resourceapi.SchemeGroupVersion:     false,
				resourcev1beta2.SchemeGroupVersion: true,
			},
			features: map[featuregate.Feature]bool{
				features.DynamicResourceAllocation:    true,
				features.DRADeviceTaintRules:          true,
				features.DRADeviceCompatibilityGroups: true,
			},
			f: func(tCtx ktesting.TContext) {
				runSubTest(tCtx, "PublishResourceSlices", func(tCtx ktesting.TContext) {
					testPublishResourceSlices(tCtx, false, features.DRAOptionalNodeOperations)
				})
			},
		},
		"all": {
			apis: map[schema.GroupVersion]bool{
				resourcev1beta1.SchemeGroupVersion:  true,
				resourcev1beta2.SchemeGroupVersion:  true,
				resourcealphaapi.SchemeGroupVersion: true,
				schedulingapi.SchemeGroupVersion:    true,
			},
			features: map[featuregate.Feature]bool{
				// 新增 DRA feature gate 时按字母顺序添加到这里，供相关测试使用。
				features.DRAAdminAccess:               true,
				features.DRADeviceBindingConditions:   true,
				features.DRAConsumableCapacity:        true,
				features.DRADeviceCompatibilityGroups: true,
				features.DRADeviceTaintRules:          true,
				features.DRADerivedAttributes:         true,
				features.DRAListTypeAttributes:        true,
				features.DRAOptionalNodeOperations:    true,
				features.DRAPartitionableDevices:      true,
				features.DRAPrioritizedList:           true,
				features.DRAResourceClaimDeviceStatus: true,
				features.DRAExtendedResource:          true,
				features.DRANodeAllocatableResources:  true,
				features.DRAWorkloadResourceClaims:    true,
				features.GenericWorkload:              true, // DRAWorkloadResourceClaims 的依赖
				features.NodeDeclaredFeatures:         true, // DRAOptionalNodeOperations 的依赖
			},
			f: func(tCtx ktesting.TContext) {
				// 这些测试必须尽可能并行，以控制整体运行时长！

				runSubTest(tCtx, "AdminAccess", func(tCtx ktesting.TContext) { testAdminAccess(tCtx, true) })
				runSubTest(tCtx, "Convert", testConvert)
				runSubTest(tCtx, "ControllerManagerMetrics", testControllerManagerMetrics)
				runSubTest(tCtx, "ResourceSliceFieldSelectors", testResourceSliceFieldSelectors)
				runSubTest(tCtx, "DerivedAttributes", testDerivedAttributes)
				runSubTest(tCtx, "DeviceBindingConditions", func(tCtx ktesting.TContext) { testDeviceBindingConditions(tCtx, true) })
				runSubTest(tCtx, "OptionalNodeOperations", func(tCtx ktesting.TContext) { testOptionalNodeOperations(tCtx, true) })
				runSubTest(tCtx, "PartitionableDevices", func(tCtx ktesting.TContext) { testPartitionableDevices(tCtx, true) })
				runSubTest(tCtx, "CompatibilityGroups", func(tCtx ktesting.TContext) { testCompatibilityGroups(tCtx, true) })
				runSubTest(tCtx, "PrioritizedList", func(tCtx ktesting.TContext) { testPrioritizedList(tCtx, true) })
				runSubTest(tCtx, "PrioritizedListScoring", func(tCtx ktesting.TContext) { testPrioritizedListScoring(tCtx) })
				runSubTest(tCtx, "PublishResourceSlices", func(tCtx ktesting.TContext) { testPublishResourceSlices(tCtx, true) })
				runSubTest(tCtx, "ExplicitExtendedResource", func(tCtx ktesting.TContext) { testExtendedResource(tCtx, true, true) })
				runSubTest(tCtx, "ImplicitExtendedResource", func(tCtx ktesting.TContext) { testExtendedResource(tCtx, true, false) })
				runSubTest(tCtx, "ResourceClaimDeviceStatus", func(tCtx ktesting.TContext) { testResourceClaimDeviceStatus(tCtx, true) })
				runSubTest(tCtx, "MaxResourceSlice", testMaxResourceSlice)
				runSubTest(tCtx, "EvictClusterWithV1alpha3Rule", func(tCtx ktesting.TContext) { testEvictCluster(tCtx, useV1alpha3Rule) })
				runSubTest(tCtx, "EvictClusterWithV1beta2Rule", func(tCtx ktesting.TContext) { testEvictCluster(tCtx, useV1beta2Rule) })
				runSubTest(tCtx, "EvictClusterWithV1Rule", func(tCtx ktesting.TContext) { testEvictCluster(tCtx, useV1Rule) })
				runSubTest(tCtx, "EvictClusterWithSlices", func(tCtx ktesting.TContext) { testEvictCluster(tCtx, useNoRule) })
				runSubTest(tCtx, "NoScheduleWithV1alpha3Rule", func(tCtx ktesting.TContext) { testNoScheduleRule(tCtx, useV1alpha3Rule) })
				runSubTest(tCtx, "NoScheduleWithV1beta2Rule", func(tCtx ktesting.TContext) { testNoScheduleRule(tCtx, useV1beta2Rule) })
				runSubTest(tCtx, "NoScheduleWithV1Rule", func(tCtx ktesting.TContext) { testNoScheduleRule(tCtx, useV1Rule) })
				runSubTest(tCtx, "NoScheduleWithSlices", func(tCtx ktesting.TContext) { testNoScheduleRule(tCtx, useNoRule) })
				runSubTest(tCtx, "InvalidResourceSlices", testInvalidResourceSlices)
				// 每个 slice 的 device 数量选为让 Filter 耗时数秒：experimental 渠道的 allocator
				// 有优化，因此这里需要比 incubating 和 stable 渠道更大的数量。
				runSubTest(tCtx, "FilterTimeout", func(tCtx ktesting.TContext) { testFilterTimeout(tCtx, 21) })
				runSubTest(tCtx, "ShareResourceClaimSequentially", testShareResourceClaimSequentially)
				runSubTest(tCtx, "UsesAllResources", testUsesAllResources)
				runSubTest(tCtx, "DRANodeAllocatableResources", func(tCtx ktesting.TContext) { testNodeAllocatableResources(tCtx, true) })
				runSubTest(tCtx, "PodGroup", testPodGroup)
				runSubTest(tCtx, "WorkloadResourceClaims", func(tCtx ktesting.TContext) { testWorkloadResourceClaims(tCtx, true, true) })
			},
		},
	} {
		if !re.MatchString(name) {
			continue
		}

		tCtx.Run(name, func(tCtx ktesting.TContext) {
			var entries []string
			for key, value := range tc.features {
				entries = append(entries, fmt.Sprintf("%s=%t", key, value))
			}
			for key, value := range tc.apis {
				entries = append(entries, fmt.Sprintf("%s=%t", key, value))
			}
			sort.Strings(entries)
			tCtx.Logf("Config: %s", strings.Join(entries, ","))

			if tc.version != "" {
				featuregatetesting.SetFeatureGateEmulationVersionDuringTest(tCtx, utilfeature.DefaultFeatureGate, version.MustParse(tc.version))
			}
			featuregatetesting.SetFeatureGatesDuringTest(tCtx, utilfeature.DefaultFeatureGate, tc.features)

			etcdOptions := framework.SharedEtcd()
			apiServerOptions := kubeapiservertesting.NewDefaultTestServerOptions()
			apiServerFlags := framework.DefaultTestServerFlags()
			var runtimeConfigs []string
			for key, value := range tc.apis {
				runtimeConfigs = append(runtimeConfigs, fmt.Sprintf("%s=%t", key, value))
			}
			apiServerFlags = append(apiServerFlags, "--runtime-config="+strings.Join(runtimeConfigs, ","))

			// context 取消后仍保持 apiserver 运行，以便后续收集信息，
			// 最后在清理阶段显式停止。
			server, err := kubeapiservertesting.StartTestServer(tCtx.WithoutCancel(), apiServerOptions, apiServerFlags, etcdOptions)
			tCtx.ExpectNoError(err, "start apiserver")
			tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
				tCtx.Log("Stopping the apiserver...")
				server.TearDownFn()
			})
			tCtx = tCtx.WithRESTConfig(server.ClientConfig)

			nodes := createNodes(tCtx)
			for feature, setup := range featureNodeSetups {
				if utilfeature.DefaultFeatureGate.Enabled(feature) {
					setup(tCtx, nodes)
				}
			}
			tCtx = prepareScheduler(tCtx)
			tCtx = prepareClaimController(tCtx)

			// 为保证测试相互隔离，每个测试必须只分配自己的 device。这里通过检查已分配
			// device 的 driver 名包含 namespace 来验证；所有测试都应按此规则构造 driver 名。
			//
			// 该检查与测试并行运行，因为测试结束后的检查可能漏掉某些 claim。
			informerFactory := informers.NewSharedInformerFactory(tCtx.Client(), 0)
			claimHandle, err := informerFactory.Resource().V1().ResourceClaims().Informer().AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
				UpdateFunc: func(oldObj, newObj any) {
					oldClaim := oldObj.(*resourceapi.ResourceClaim)
					newClaim := newObj.(*resourceapi.ResourceClaim)

					// 分配出现时检查一次。
					if oldClaim.Status.Allocation == nil && newClaim.Status.Allocation != nil {
						for _, result := range newClaim.Status.Allocation.Devices.Results {
							if !strings.Contains(result.Driver, newClaim.Namespace) {
								tCtx.Errorf("Claim %s has allocated device %#v from a driver in some other test (%s):\n%s", klog.KObj(newClaim), result, result.Driver, format.Object(newClaim, 1))
								break
							}
						}
					}
				},
			}, cache.HandlerOptions{
				Logger: ptr.To(tCtx.Logger()),
			})
			tCtx.ExpectNoError(err, "add claim event handler")
			informerFactory.StartWithContext(tCtx)
			tCtx.Cleanup(func() {
				tCtx.Cancel("test is done")
				_ = informerFactory.Resource().V1().ResourceClaims().Informer().RemoveEventHandler(claimHandle)
				informerFactory.Shutdown()
			})

			// 测试失败时先 dump 信息，再继续清理。即使发生超时，也必须完成 dump，
			// 即便随后的清理本身失败。
			tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
				if !tCtx.Failed() {
					return
				}

				namespaces, _ := tCtx.Client().CoreV1().Namespaces().List(tCtx, metav1.ListOptions{})
				tCtx.Logf("Namespaces:\n%s", format.Object(namespaces, 0))
				claims, _ := tCtx.Client().ResourceV1().ResourceClaims("").List(tCtx, metav1.ListOptions{})
				tCtx.Logf("ResourceClaims:\n%s", format.Object(claims, 0))
				classes, _ := tCtx.Client().ResourceV1().DeviceClasses().List(tCtx, metav1.ListOptions{})
				tCtx.Logf("DeviceClasses:\n%s", format.Object(classes, 0))
				slices, _ := tCtx.Client().ResourceV1().ResourceSlices().List(tCtx, metav1.ListOptions{})
				tCtx.Logf("ResourceSlices:\n%s", format.Object(slices, 0))
			})

			tc.f(tCtx)
		})
	}
}

func createNodes(tCtx ktesting.TContext) []*v1.Node {
	var createdNodes []*v1.Node
	for i := range numNodes {
		nodeName := fmt.Sprintf("worker-%d", i)
		// 创建 node。
		node := &v1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: nodeName,
				Labels: map[string]string{
					"kubernetes.io/hostname": nodeName,
				},
			},
		}
		node, err := tCtx.Client().CoreV1().Nodes().Create(tCtx, node, metav1.CreateOptions{FieldValidation: "Strict"})
		tCtx.ExpectNoError(err, fmt.Sprintf("creating node #%d", i))

		// 将 node 置为 ready。
		node.Status = v1.NodeStatus{
			Capacity: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse(nodeCPUCapacity),
				v1.ResourceMemory: resource.MustParse(nodeMemoryCapacity),
				v1.ResourcePods:   *resource.NewScaledQuantity(maxPodsPerNode, 0),
			},
			// test/integration/dra/node_allocatable_resources.go 中的测试需要此字段。
			// DeclaredFeatures 必须包含 DRANodeAllocatableResources，才能通过 admission 的 node feature 校验。
			DeclaredFeatures: []string{"DRANodeAllocatableResources"},
			Phase:            v1.NodeRunning,
			Conditions: []v1.NodeCondition{
				{
					Type:   v1.NodeReady,
					Status: v1.ConditionTrue,
				},
			},
		}
		node, err = tCtx.Client().CoreV1().Nodes().UpdateStatus(tCtx, node, metav1.UpdateOptions{})
		tCtx.ExpectNoError(err, fmt.Sprintf("setting status of node #%d", i))

		// 移除 TaintNodesByCondition admission 检查添加的 taint。
		node.Spec.Taints = nil
		node, err = tCtx.Client().CoreV1().Nodes().Update(tCtx, node, metav1.UpdateOptions{})
		tCtx.ExpectNoError(err, fmt.Sprintf("removing node taint from #%d", i))
		createdNodes = append(createdNodes, node)
	}

	tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
		if !tCtx.Failed() {
			return
		}

		// dump 集群信息。
		nodes, err := tCtx.Client().CoreV1().Nodes().List(tCtx, metav1.ListOptions{})
		if err != nil {
			tCtx.Logf("Retrieving nodes failed: %v", err)
		} else {
			tCtx.Logf("Nodes:\n%s", format.Object(nodes.Items, 1))
		}
	})
	return createdNodes
}

// runSubTest 在子测试内重新初始化 client，确保 user agent 按子测试名设置并体现在
// apiserver 输出中。凭证不会因此改变，因此所有测试在 ManagedFields 中仍是同一个 manager。
//
// tCtx.Run() 本身不做这件事，因为测试可能在父 context 中设置了不同的 REST config 和 client。
func runSubTest(tCtx ktesting.TContext, name string, cb func(tCtx ktesting.TContext)) {
	tCtx.Run(name, func(tCtx ktesting.TContext) {
		tCtx = tCtx.WithRESTConfig(tCtx.RESTConfig())
		cb(tCtx)
	})
}

// prepareScheduler 返回一个 TContext，可传给 startScheduler 在需要时真正启动 scheduler。
// 内部由 schedulerSingleton 保证最多创建一个 scheduler 实例，并在最后一个使用它的测试结束后
// 销毁。理论上可能反复启停，但实际测试都是并行启动并共享同一个实例。
func prepareScheduler(tCtx ktesting.TContext) ktesting.TContext {
	scheduler := &schedulerSingleton{
		rootCtx: tCtx,
	}

	return tCtx.WithValue(schedulerKey, scheduler)
}

// startScheduler 以空配置（即全部使用默认值）启动 scheduler，可用于并行测试。
func startScheduler(tCtx ktesting.TContext) {
	startSchedulerWithConfig(tCtx, "")
}

// startSchedulerWithConfig 以给定配置启动 scheduler；配置非空时只能用于串行测试。
func startSchedulerWithConfig(tCtx ktesting.TContext, config string) {
	tCtx.Helper()
	value := tCtx.Value(schedulerKey)
	if value == nil {
		tCtx.Fatal("internal error: startScheduler without a prior prepareScheduler call")
	}
	scheduler := value.(*schedulerSingleton)
	scheduler.start(tCtx, config)
}

// prepareClaimController 为 ResourceClaimController 做与 prepareScheduler 相同的事。
func prepareClaimController(tCtx ktesting.TContext) ktesting.TContext {
	claimController := &claimControllerSingleton{
		rootCtx: tCtx,
	}

	return tCtx.WithValue(claimControllerKey, claimController)
}

// startClaimController 供测试确保 ResourceClaim controller 已运行，可用于并行测试。
func startClaimController(tCtx ktesting.TContext) {
	tCtx.Helper()
	value := tCtx.Value(claimControllerKey)
	if value == nil {
		tCtx.Fatal("internal error: startClaimController without a prior prepareClaimController call")
	}
	claimController := value.(*claimControllerSingleton)
	claimController.start(tCtx)
}

type schedulerSingleton struct {
	rootCtx ktesting.TContext

	mutex           sync.Mutex
	usageCount      int
	informerFactory informers.SharedInformerFactory
	cancel          func(cause string)
}

func (scheduler *schedulerSingleton) start(tCtx ktesting.TContext, config string) {
	tCtx.Helper()
	scheduler.mutex.Lock()
	defer scheduler.mutex.Unlock()

	scheduler.usageCount++
	tCtx.CleanupCtx(scheduler.stop)
	if scheduler.usageCount > 1 {
		// 此前已启动。
		return
	}

	// 以默认配置启动 scheduler。必须使用 root context，因为传入 start 的 per-test tCtx
	// 会在触发启动的测试结束时被取消。
	tCtx = scheduler.rootCtx
	tCtx.Logf("Starting the scheduler for test %s...", tCtx.Name())
	tCtx = tCtx.WithLogger(klog.LoggerWithName(tCtx.Logger(), "scheduler"))
	schedulerCtx := tCtx.WithCancel()
	scheduler.cancel = schedulerCtx.Cancel
	cfg := newSchedulerComponentConfig(schedulerCtx, config)
	_, scheduler.informerFactory = util.StartScheduler(schedulerCtx, cfg, nil)
	tCtx.Logf("Started the scheduler for test %s.", tCtx.Name())
}

func (scheduler *schedulerSingleton) stop(tCtx ktesting.TContext) {
	scheduler.mutex.Lock()
	defer scheduler.mutex.Unlock()

	scheduler.usageCount--
	if scheduler.usageCount > 0 {
		// 仍被其他测试使用。
		return
	}

	scheduler.rootCtx.Logf("Stopping the scheduler after test %s...", tCtx.Name())
	if scheduler.cancel != nil {
		scheduler.cancel("test is done")
	}
	if scheduler.informerFactory != nil {
		scheduler.informerFactory.Shutdown()
	}
}

type schedulerKeyType int

var schedulerKey schedulerKeyType

func newSchedulerComponentConfig(tCtx ktesting.TContext, cfgData string) *config.KubeSchedulerConfiguration {
	tCtx.Helper()
	gvk := kubeschedulerconfigv1.SchemeGroupVersion.WithKind("KubeSchedulerConfiguration")
	cfg := config.KubeSchedulerConfiguration{}
	_, _, err := kubeschedulerscheme.Codecs.UniversalDecoder().Decode([]byte(cfgData), &gvk, &cfg)
	tCtx.ExpectNoError(err, "decode default scheduler configuration")
	return &cfg
}

type claimControllerSingleton struct {
	rootCtx ktesting.TContext

	mutex           sync.Mutex
	usageCount      int
	wg              sync.WaitGroup
	informerFactory informers.SharedInformerFactory
	cancel          func(cause string)
}

func (claimController *claimControllerSingleton) start(tCtx ktesting.TContext) {
	tCtx.Helper()
	claimController.mutex.Lock()
	defer claimController.mutex.Unlock()

	claimController.usageCount++
	tCtx.CleanupCtx(claimController.stop)
	if claimController.usageCount > 1 {
		// 此前已启动。
		return
	}

	// 以默认配置启动 claimController。必须使用 root context，因为传入 start 的 per-test tCtx
	// 会在触发启动的测试结束时被取消。
	tCtx = claimController.rootCtx
	tCtx.Logf("Starting the ResourceClaim controller for test %s...", tCtx.Name())
	tCtx = tCtx.WithLogger(klog.LoggerWithName(tCtx.Logger(), "claimController"))

	claimControllerCtx := tCtx.WithCancel()
	claimController.cancel = claimControllerCtx.Cancel

	client := claimControllerCtx.Client()
	claimController.informerFactory = informers.NewSharedInformerFactory(client, 0 /* resync period */)
	controller, err := resourceclaim.NewController(
		klog.FromContext(claimControllerCtx),
		claimControllerCtx.Client(),
		claimController.informerFactory.Core().V1().Pods(),
		claimController.informerFactory.Scheduling().V1beta1().PodGroups(),
		claimController.informerFactory.Resource().V1().ResourceClaims(),
		claimController.informerFactory.Resource().V1().ResourceClaimTemplates(),
	)
	tCtx.ExpectNoError(err, "create ResourceClaim controller")

	claimController.informerFactory.Start(claimControllerCtx.Done())
	claimController.wg.Go(func() {
		controller.Run(claimControllerCtx, 1 /* one worker to get more readable log output without interleaving */)
	})
	tCtx.Logf("Started the claimController for test %s.", tCtx.Name())
}

func (claimController *claimControllerSingleton) stop(tCtx ktesting.TContext) {
	claimController.mutex.Lock()
	defer claimController.mutex.Unlock()

	claimController.usageCount--
	if claimController.usageCount > 0 {
		// 仍被其他测试使用。
		return
	}

	claimController.rootCtx.Logf("Stopping the ResourceClaim controller after test %s...", tCtx.Name())
	if claimController.cancel != nil {
		claimController.cancel("test is done")
	}
	if claimController.informerFactory != nil {
		claimController.informerFactory.Shutdown()
	}
	claimController.wg.Wait()
}

type claimControllerKeyType int

var claimControllerKey claimControllerKeyType
