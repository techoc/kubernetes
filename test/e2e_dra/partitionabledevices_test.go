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

package e2edra

import (
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/dynamic-resource-allocation/resourceslice"
	drautils "k8s.io/kubernetes/test/e2e/dra/utils"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
)

// partitionableDeviceResources 测试 DRAPartitionableDevices 特性在升级/降级中的表现。
// 它创建带 SharedCounters 和消费这些 counter 的设备的 ResourceSlice，
// 然后校验这些 API 字段在集群版本切换后被保留。
//
// 本测试聚焦 API 字段保留而非调度行为，
// 与 resourceClaimDeviceStatus 测试类似。
func partitionableDeviceResources(nodes *drautils.Nodes) map[string]resourceslice.DriverResources {
	nodename := nodes.NodeNames[0]
	return map[string]resourceslice.DriverResources{
		nodename: {
			Pools: map[string]resourceslice.Pool{
				"partitionable-pool": {
					Slices: []resourceslice.Slice{
						// 按设计，设备和 SharedCounters 必须是独占的。
						{SharedCounters: partitionableSharedCounters},
						{Devices: partitionableDeviceList},
					},
				},
			},
		},
	}
}

var partitionableDeviceList = []resourceapi.Device{
	{
		Name: "memory0-2g",
		Capacity: map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
			"memory": {Value: resource.MustParse("2Gi")},
		},
		ConsumesCounters: []resourceapi.DeviceCounterConsumption{
			{
				CounterSet: "memory-pool",
				Counters: map[string]resourceapi.Counter{
					"memory": {
						Value: resource.MustParse("2Gi"),
					},
				},
			},
		},
	},
	{
		Name: "memory1-2g",
		Capacity: map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
			"memory": {Value: resource.MustParse("2Gi")},
		},
		ConsumesCounters: []resourceapi.DeviceCounterConsumption{
			{
				CounterSet: "memory-pool",
				Counters: map[string]resourceapi.Counter{
					"memory": {
						Value: resource.MustParse("2Gi"),
					},
				},
			},
		},
	},
	{
		Name: "memory2-2g",
		Capacity: map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
			"memory": {Value: resource.MustParse("2Gi")},
		},
		ConsumesCounters: []resourceapi.DeviceCounterConsumption{
			{
				CounterSet: "memory-pool",
				Counters: map[string]resourceapi.Counter{
					"memory": {
						Value: resource.MustParse("2Gi"),
					},
				},
			},
		},
	},
	{
		Name: "memory3-2g",
		Capacity: map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
			"memory": {Value: resource.MustParse("2Gi")},
		},
		ConsumesCounters: []resourceapi.DeviceCounterConsumption{
			{
				CounterSet: "memory-pool",
				Counters: map[string]resourceapi.Counter{
					"memory": {
						Value: resource.MustParse("2Gi"),
					},
				},
			},
		},
	},
	{
		Name: "memory4-4g",
		Capacity: map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
			"memory": {Value: resource.MustParse("4Gi")},
		},
		ConsumesCounters: []resourceapi.DeviceCounterConsumption{
			{
				CounterSet: "memory-pool",
				Counters: map[string]resourceapi.Counter{
					"memory": {
						Value: resource.MustParse("4Gi"),
					},
				},
			},
		},
	},
	{
		Name: "memory5-4g",
		Capacity: map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
			"memory": {Value: resource.MustParse("4Gi")},
		},
		ConsumesCounters: []resourceapi.DeviceCounterConsumption{
			{
				CounterSet: "memory-pool",
				Counters: map[string]resourceapi.Counter{
					"memory": {
						Value: resource.MustParse("4Gi"),
					},
				},
			},
		},
	},
}

var partitionableSharedCounters = []resourceapi.CounterSet{
	{
		Name: "memory-pool",
		Counters: map[string]resourceapi.Counter{
			"memory": {
				Value: resource.MustParse("8Gi"),
			},
		},
	},
}

func partitionableDevices(tCtx ktesting.TContext, b *drautils.Builder) upgradedTestFunc {
	driverName := b.Driver.Name
	namespace := tCtx.Namespace()

	// 创建通过 resource claim 请求不同内存大小的 Pod。
	// 资源池总计 8Gi：2Gi + 4Gi 的分配应成功（用掉 6Gi），
	// 但第二个 4Gi 分配应失败（会超过 8Gi 容量）。
	pod2g, claim2g := partitionableCreatePodWithClaim(tCtx, b, driverName, "2Gi")
	pod4g, claim4g := partitionableCreatePodWithClaim(tCtx, b, driverName, "4Gi")

	// 等待 Pod 运行，确保第三个 Pod 不会在前两个 Pod 调度前被调度。
	tCtx.ExpectNoError(e2epod.WaitForPodRunningInNamespace(tCtx, tCtx.Client(), pod2g))
	tCtx.ExpectNoError(e2epod.WaitForPodRunningInNamespace(tCtx, tCtx.Client(), pod4g))

	// 第二个 4Gi Pod 应不可调度，因为共享 counter
	// 池只剩 2Gi（8Gi 总量 - 2Gi - 4Gi = 2Gi）。
	pod4g2, claim4g2 := partitionableCreatePodWithClaim(tCtx, b, driverName, "4Gi")
	tCtx.ExpectNoError(e2epod.WaitForPodNameUnschedulableInNamespace(tCtx, tCtx.Client(), pod4g2.Name, namespace), "Pod with a claim over the counter should be unschedulable")

	partitionableDoTest(tCtx, b, driverName, []*v1.Pod{pod2g, pod4g}, pod4g2)

	return func(tCtx ktesting.TContext) downgradedTestFunc {
		// 升级后
		partitionableDoTest(tCtx, b, driverName, []*v1.Pod{pod2g, pod4g}, pod4g2)

		return func(tCtx ktesting.TContext) {
			// 降级后
			partitionableDoTest(tCtx, b, driverName, []*v1.Pod{pod2g, pod4g}, pod4g2)

			// 清理：删除 Pod 和 claim。
			b.DeletePodAndWaitForNotFound(tCtx, pod2g)
			b.DeletePodAndWaitForNotFound(tCtx, pod4g)
			b.DeletePodAndWaitForNotFound(tCtx, pod4g2)

			tCtx.ExpectNoError(tCtx.Client().ResourceV1().ResourceClaims(namespace).Delete(tCtx, claim2g.Name, metav1.DeleteOptions{}))
			tCtx.ExpectNoError(tCtx.Client().ResourceV1().ResourceClaims(namespace).Delete(tCtx, claim4g.Name, metav1.DeleteOptions{}))
			tCtx.ExpectNoError(tCtx.Client().ResourceV1().ResourceClaims(namespace).Delete(tCtx, claim4g2.Name, metav1.DeleteOptions{}))
		}
	}
}

// partitionableDoTest 校验给定运行中 Pod 仍在运行、不可调度的 Pod 仍不可调度、
// 新的 4Gi Pod 不可调度（只剩 2Gi），且
// 新的 2Gi Pod 可调度。
func partitionableDoTest(tCtx ktesting.TContext, b *drautils.Builder, driverName string, runningPods []*v1.Pod, unschedulablePod *v1.Pod) {
	namespace := tCtx.Namespace()

	// 校验之前运行的 Pod 仍在运行。
	for _, pod := range runningPods {
		b.TestPod(tCtx, pod)
	}

	// 校验不可调度的 Pod 仍不可调度。
	tCtx.ExpectNoError(e2epod.WaitForPodNameUnschedulableInNamespace(tCtx, tCtx.Client(), unschedulablePod.Name, namespace), "Pod %s should still be unschedulable", unschedulablePod.Name)

	// 新的 4Gi Pod 应不可调度，因为只剩 2Gi。
	pod4g, claim4g := partitionableCreatePodWithClaim(tCtx, b, driverName, "4Gi")
	tCtx.ExpectNoError(e2epod.WaitForPodNameUnschedulableInNamespace(tCtx, tCtx.Client(), pod4g.Name, namespace), "Pod with a claim over the counter should be unschedulable")
	b.DeletePodAndWaitForNotFound(tCtx, pod4g)
	tCtx.ExpectNoError(tCtx.Client().ResourceV1().ResourceClaims(namespace).Delete(tCtx, claim4g.Name, metav1.DeleteOptions{}))

	// 新的 2Gi Pod 应可调度，因为还有 2Gi 可用。
	pod2g, claim2g := partitionableCreatePodWithClaim(tCtx, b, driverName, "2Gi")
	b.TestPod(tCtx, pod2g)
	b.DeletePodAndWaitForNotFound(tCtx, pod2g)
	tCtx.ExpectNoError(tCtx.Client().ResourceV1().ResourceClaims(namespace).Delete(tCtx, claim2g.Name, metav1.DeleteOptions{}))
}

func partitionableCreatePodWithClaim(tCtx ktesting.TContext, b *drautils.Builder, driverName, value string) (*v1.Pod, *resourceapi.ResourceClaim) {
	claim := b.ExternalClaim()
	claim.Spec.Devices.Requests[0] = resourceapi.DeviceRequest{
		Name: fmt.Sprintf("%s-memory", strings.ToLower(value)),
		Exactly: &resourceapi.ExactDeviceRequest{
			DeviceClassName: b.ClassName(),
			Selectors: []resourceapi.DeviceSelector{
				{
					CEL: &resourceapi.CELDeviceSelector{
						Expression: fmt.Sprintf(`device.capacity["%s"].memory.compareTo(quantity("%s")) == 0`, driverName, value),
					},
				},
			},
		},
	}

	pod := b.Pod()
	podClaimName := fmt.Sprintf("claim-%s", claim.Name)
	pod.Spec.ResourceClaims = []v1.PodResourceClaim{
		{
			Name:              podClaimName,
			ResourceClaimName: &claim.Name,
		},
	}
	pod.Spec.Containers[0].Resources.Claims = []v1.ResourceClaim{{Name: podClaimName}}
	b.Create(tCtx, claim, pod)
	return pod, claim
}
