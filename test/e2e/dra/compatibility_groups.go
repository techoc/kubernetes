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

package dra

import (
	"context"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/kubernetes/pkg/features"
	drautils "k8s.io/kubernetes/test/e2e/dra/utils"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	admissionapi "k8s.io/pod-security-admission/api"
)

const compatGroupsCounterSet = "gpu"

// compatGroupsCounters 是一个容量为两台设备的 counter set（下面每台设备消耗一半），
// 因此只有兼容性组有交集时，两台设备才能在同一节点上分配给不同的 claim。
// 单靠计数器余量不会让第二个 pod 变得不可调度。
func compatGroupsCounters() []resourceapi.CounterSet {
	return []resourceapi.CounterSet{{
		Name:     compatGroupsCounterSet,
		Counters: map[string]resourceapi.Counter{"mem": {Value: resource.MustParse("2")}},
	}}
}

func compatGroupsDevice(name, group string) resourceapi.Device {
	return resourceapi.Device{
		Name: name,
		ConsumesCounters: []resourceapi.DeviceCounterConsumption{{
			CounterSet:          compatGroupsCounterSet,
			Counters:            map[string]resourceapi.Counter{"mem": {Value: resource.MustParse("1")}},
			CompatibilityGroups: []string{group},
		}},
	}
}

// 本测试套件用 fake DRA driver 端到端验证 DRADeviceCompatibilityGroups 特性：
// driver 上报的设备在共享 counter set 上声明兼容性组。
// 仅在启用该（alpha）feature gate 的集群上运行。
var _ = framework.SIGDescribe("node")(framework.WithLabel("DRA"), framework.WithFeatureGate(features.DRADeviceCompatibilityGroups), func() {
	f := framework.NewDefaultFramework("dra-compat-groups")

	// driver 容器需要 privileged 才能管理 /var/lib/kubelet/plugins。
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	nodes := drautils.NewNodes(f, 1, 1)

	f.Context("device compatibility groups", func() {
		f.Context("incompatible devices", func() {
			// 两台设备分别声明互斥的组（mig、vgpu），共享同一个 counter set。
			driver := drautils.NewDriver(f, nodes, drautils.ToDriverResources(
				compatGroupsCounters(),
				compatGroupsDevice("gpu-mig", "mig"),
				compatGroupsDevice("gpu-vgpu", "vgpu"),
			))
			b := drautils.NewBuilder(f, driver)

			f.It("leave a pod requesting an incompatible device unschedulable", func(ctx context.Context) {
				tCtx := f.TContext(ctx)

				// 第一个 pod 占用两台设备中的一台。
				claim1 := b.ExternalClaim()
				b.Create(tCtx, claim1)
				pod1 := b.PodExternal(claim1.Name)
				b.Create(tCtx, pod1)
				b.TestPod(tCtx, pod1)

				// 剩下的唯一一台设备在同一 counter set 上声明了不兼容的组，
				// 所以即使 counter set 仍有余量，第二个 pod 也无法调度。
				claim2 := b.ExternalClaim()
				b.Create(tCtx, claim2)
				pod2 := b.PodExternal(claim2.Name)
				b.Create(tCtx, pod2)
				framework.ExpectNoError(
					e2epod.WaitForPodNameUnschedulableInNamespace(ctx, f.ClientSet, pod2.Name, pod2.Namespace),
					"second pod must be unschedulable: only an incompatible device remains on the counter set")
			})
		})

		f.Context("compatible devices", func() {
			// 两台设备声明同一个组（mig），共享同一个 counter set。
			driver := drautils.NewDriver(f, nodes, drautils.ToDriverResources(
				compatGroupsCounters(),
				compatGroupsDevice("gpu-0", "mig"),
				compatGroupsDevice("gpu-1", "mig"),
			))
			b := drautils.NewBuilder(f, driver)

			f.It("schedule two pods that share a compatibility group", func(ctx context.Context) {
				tCtx := f.TContext(ctx)

				claim1 := b.ExternalClaim()
				b.Create(tCtx, claim1)
				pod1 := b.PodExternal(claim1.Name)
				b.Create(tCtx, pod1)
				b.TestPod(tCtx, pod1)

				// 第二台设备与第一台同组，因此可以在同一 counter set 上
				// 共同分配，它的 pod 也能运行。
				claim2 := b.ExternalClaim()
				b.Create(tCtx, claim2)
				pod2 := b.PodExternal(claim2.Name)
				b.Create(tCtx, pod2)
				b.TestPod(tCtx, pod2)
			})
		})
	})
})
