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
	"github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/dynamic-resource-allocation/resourceslice"
	drautils "k8s.io/kubernetes/test/e2e/dra/utils"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
)

// compatibilityGroupsSharedCounters 是一个可容纳 4 个设备的计数器集（每个设备消耗 1 个单位），
// 足够容纳开关周期中长期存在的双设备 claim，以及重新启用后新分配的单设备 claim，并留有余量。
var compatibilityGroupsSharedCounters = []resourceapi.CounterSet{{
	Name:     "gpu",
	Counters: map[string]resourceapi.Counter{"mem": {Value: resource.MustParse("4")}},
}}

// compatibilityGroupsDeviceList 包含 4 个声明同一 group 的设备，使分组 claim 在共享
// 计数器集上经过 group 校验后共同分配。
var compatibilityGroupsDeviceList = []resourceapi.Device{
	compatibilityGroupsDevice("device-0"),
	compatibilityGroupsDevice("device-1"),
	compatibilityGroupsDevice("device-2"),
	compatibilityGroupsDevice("device-3"),
}

func compatibilityGroupsDevice(name string) resourceapi.Device {
	return resourceapi.Device{
		Name: name,
		ConsumesCounters: []resourceapi.DeviceCounterConsumption{{
			CounterSet:          "gpu",
			Counters:            map[string]resourceapi.Counter{"mem": {Value: resource.MustParse("1")}},
			CompatibilityGroups: []string{"mig"},
		}},
	}
}

func compatibilityGroupsDriverResources(nodes *drautils.Nodes) map[string]resourceslice.DriverResources {
	nodename := nodes.NodeNames[0]
	return map[string]resourceslice.DriverResources{
		nodename: {
			Pools: map[string]resourceslice.Pool{
				"compatibility-groups-pool": {
					Slices: []resourceslice.Slice{
						// 按设计，Devices 和 SharedCounters 必须互斥。
						{SharedCounters: compatibilityGroupsSharedCounters},
						{Devices: compatibilityGroupsDeviceList},
					},
				},
			},
		},
	}
}

// compatibilityGroupsCreatePod 创建一个 Pod，其 claim 请求分组设备类的 count 个设备；
// count>1 会迫使分配器在共享计数器集上共同分配，并执行兼容性 group 交集校验。
func compatibilityGroupsCreatePod(tCtx ktesting.TContext, b *drautils.Builder, count int64) (*v1.Pod, string) {
	claim := b.ExternalClaim()
	claim.Spec.Devices.Requests = []resourceapi.DeviceRequest{{
		Name: "req-0",
		Exactly: &resourceapi.ExactDeviceRequest{
			DeviceClassName: b.ClassName(),
			Count:           count,
			AllocationMode:  resourceapi.DeviceAllocationModeExactCount,
		},
	}}
	pod := b.PodExternal(claim.Name)
	b.Create(tCtx, claim, pod)
	return pod, claim.Name
}

func verifyCompatibilityGroupsPodAllocated(tCtx ktesting.TContext, pod *v1.Pod, claimName string) {
	tCtx.ExpectNoError(e2epod.WaitForPodRunningInNamespace(tCtx, tCtx.Client(), pod))
	claim, err := tCtx.Client().ResourceV1().ResourceClaims(pod.Namespace).Get(tCtx, claimName, metav1.GetOptions{})
	tCtx.ExpectNoError(err, "get ResourceClaim %s", claimName)
	tCtx.Expect(claim.Status.Allocation).ToNot(gomega.BeNil(), "claim %s must be allocated", claimName)
}

// compatibilityGroupsGateCycle 实现 DRADeviceCompatibilityGroups 特性门的 ON→OFF→ON 周期测试
// （KEP 集成测试 #4：升级 → 降级 → 再升级）：
//
//   - 阶段 0（门开启）：一个 claim 共同分配两个共享 group 的设备，执行兼容性校验并调度 Pod。
//   - 阶段 1（门关闭，降级）：已有的分配仍然有效。
//   - 阶段 2（再次开启门，重新升级）：重新启用强制校验不会重新评估或驱逐已有分配，
//     且新的分组分配仍能正常工作。
func compatibilityGroupsGateCycle(tCtx ktesting.TContext, b *drautils.Builder) gateOffFunc {
	var survivor *v1.Pod
	var survivorClaim string
	tCtx.Run("grouped-pod-running", func(tCtx ktesting.TContext) {
		// 两个共享同一 group 的设备在同一个计数器集上共同分配。
		survivor, survivorClaim = compatibilityGroupsCreatePod(tCtx, b, 2)
		verifyCompatibilityGroupsPodAllocated(tCtx, survivor, survivorClaim)
	})

	return func(tCtx ktesting.TContext) gateOnAgainFunc {
		tCtx.Run("allocation-survives-downgrade", func(tCtx ktesting.TContext) {
			verifyCompatibilityGroupsPodAllocated(tCtx, survivor, survivorClaim)
		})

		return func(tCtx ktesting.TContext) {
			tCtx.Run("allocation-survives-reenable", func(tCtx ktesting.TContext) {
				verifyCompatibilityGroupsPodAllocated(tCtx, survivor, survivorClaim)
			})
			tCtx.Run("fresh-grouped-pod-allocates", func(tCtx ktesting.TContext) {
				// 重新启用后，新的分组设备分配端到端成功。
				fresh, freshClaim := compatibilityGroupsCreatePod(tCtx, b, 1)
				verifyCompatibilityGroupsPodAllocated(tCtx, fresh, freshClaim)
			})
		}
	}
}
