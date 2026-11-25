//go:build windows

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

package e2enodewindows

import (
	"context"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gstruct"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	internalapi "k8s.io/cri-api/pkg/apis"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	admissionapi "k8s.io/pod-security-admission/api"
)

/*
 * Windows Topology Manager 协调机制的 e2e 测试。
 *
 * 这些测试验证 Windows 特有的协调逻辑：让内存分配跟随 CPU manager 的决策，
 * 实现在 pkg/kubelet/cm/internal_container_lifecycle_windows.go
 * （PreCreateContainer + computeFinalCpuSet）中。
 *
 * 这里 topology manager 使用 "best-effort" 策略：它总是接纳 pod（从不拒绝），
 * 但与 "none" 不同，它会收集并合并 CPU manager 和 memory manager 的 hint，
 * 使 memory manager 在 CPU manager 选择的同一个 NUMA node 上分配内存。
 * 测试覆盖 computeFinalCpuSet 的两个分支：
 *
 *   1. CPU manager 分配了独占 CPU（Guaranteed + 整数 CPU）：CPU 分配结果是权威的，
 *      直接沿用。最终的 job-object affinity 恰好是 CPU manager 的集合，不会扩大到
 *      整个 NUMA node，即使 memory manager 也在同一 node 上固定了内存。
 *   2. CPU manager 没有分配 CPU（Guaranteed + 小数 CPU）：最终的 affinity 是
 *      memory manager 的 NUMA node CPU 集合，通过 CPU affinity 提供内存局部性
 *      （Windows 没有 cpuset.mems 的等价物）。
 *
 * 纯 computeFinalCpuSet 函数（包括跨 NUMA 和多 group 场景）的单元测试在
 * pkg/kubelet/cm/internal_container_lifecycle_windows_test.go；本套件覆盖两个
 * manager 同时启用时真实的 admission -> PreCreateContainer -> CRI 路径。
 */
var _ = SIGWindowsDescribe(feature.TopologyManager, framework.WithSerial(), func() {
	f := framework.NewDefaultFramework("topologymanager-coordination-windows")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.Context("with the topology manager best-effort policy", func() {
		var oldCfg *kubeletconfig.KubeletConfiguration
		var criClient internalapi.RuntimeService
		var testPod *v1.Pod

		ginkgo.BeforeEach(func(ctx context.Context) {
			var err error
			if oldCfg == nil {
				oldCfg, err = getCurrentKubeletConfig(ctx)
				framework.ExpectNoError(err)
			}

			criClient, _, err = getCRIClient(ctx)
			framework.ExpectNoError(err, "failed to get CRI client")

			// 同时启用 CPU（static）和 memory（BestEffort）manager 并切换 topology
			// 策略会使持久化的 manager 状态 checkpoint 失效，因此重启前先清理
			newCfg := buildWindowsTopologyManagerKubeletConfig(oldCfg, topologymanager.PolicyBestEffort, topologymanager.ContainerTopologyScope)
			updateWindowsKubeletConfigClearState(ctx, f, newCfg)
		})

		ginkgo.AfterEach(func(ctx context.Context) {
			if testPod != nil {
				e2epod.NewPodClient(f).DeleteSync(ctx, testPod.Name, metav1.DeleteOptions{}, f.Timeouts.PodDelete)
				testPod = nil
			}
			// 恢复原策略同样属于 manager 策略变更，也需要清理状态 checkpoint
			updateWindowsKubeletConfigClearState(ctx, f, oldCfg)
		})

		ginkgo.It("should use the CPU manager's exclusive CPUs as-is when a guaranteed integer-CPU pod runs", func(ctx context.Context) {
			// computeFinalCpuSet 的分支 1：CPU manager 分配了独占 CPU，因此
			// memory manager 的 NUMA 决策不能扩大最终的 affinity。pod 申请 2 个
			// 完整 CPU，job-object affinity 必须恰好是 2 个 CPU，而不是整个 NUMA node
			const exclusiveCPUs = 2
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), exclusiveCPUs)

			testPod = createGuaranteedTopologyPod(ctx, f, "topo-int", "2000m")

			ginkgo.By("verifying the container is pinned to exactly the CPU manager's exclusive set")
			gomega.Eventually(ctx, func(ctx context.Context) (int, error) {
				aff, err := getWindowsContainerCPUAffinity(ctx, criClient, testPod, "gu-ctr")
				if err != nil {
					return 0, err
				}
				return countCPUsInAffinities(aff), nil
			}, 30*time.Second, 2*time.Second).Should(gomega.Equal(exclusiveCPUs),
				"guaranteed integer-CPU container must be pinned to exactly its exclusive CPUs, not widened to the NUMA node")

			ginkgo.By("verifying the host job-object affinity agrees with the CRI report")
			verifyHostMatchesCRI(ctx, criClient, testPod, "gu-ctr")

			ginkgo.By("verifying the memory manager also pinned memory (following the CPU decision)")
			matchResourceMetrics := gstruct.MatchKeys(gstruct.IgnoreExtras, gstruct.Keys{
				"kubelet_memory_manager_pinning_requests_total": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(1),
				}),
				"kubelet_memory_manager_pinning_errors_total": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(0),
				}),
			})
			gomega.Eventually(ctx, getWindowsKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchResourceMetrics)
		})

		ginkgo.It("should set affinity to the memory manager's NUMA node when a guaranteed fractional-CPU pod runs", func(ctx context.Context) {
			// computeFinalCpuSet 的分支 2：申请小数 CPU 的 Guaranteed pod 不会从
			// CPU manager 获得独占 CPU，因此最终 affinity 是 memory manager 的
			// NUMA node CPU 集合。这里要做确定性断言必须使用单 NUMA node
			// （无法读取 memory manager 选择了哪个 node）；多 NUMA 路径由单元测试覆盖
			perNodeCPUs := windowsDetectCPUsPerNUMANode()
			if len(perNodeCPUs) != 1 {
				e2eskipper.Skipf("memory-driven affinity assertion requires a single-NUMA node (memory node is deterministic); found %d NUMA nodes", len(perNodeCPUs))
			}
			var expectedNodeCPUs int
			for _, cpus := range perNodeCPUs {
				expectedNodeCPUs = cpus
			}

			testPod = createGuaranteedTopologyPod(ctx, f, "topo-frac", "500m")

			ginkgo.By("verifying no exclusive CPUs were allocated")
			matchNoExclusive := gstruct.MatchKeys(gstruct.IgnoreExtras, gstruct.Keys{
				"kubelet_cpu_manager_exclusive_cpu_allocation_count": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(0),
				}),
			})
			gomega.Eventually(ctx, getWindowsKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchNoExclusive)

			ginkgo.By("verifying the container affinity equals the memory manager's NUMA node CPU set")
			gomega.Eventually(ctx, func(ctx context.Context) (int, error) {
				aff, err := getWindowsContainerCPUAffinity(ctx, criClient, testPod, "gu-ctr")
				if err != nil {
					return 0, err
				}
				return countCPUsInAffinities(aff), nil
			}, 30*time.Second, 2*time.Second).Should(gomega.Equal(expectedNodeCPUs),
				"guaranteed fractional-CPU container must be pinned to the memory manager's NUMA node CPUs")

			ginkgo.By("verifying the memory manager pinned memory")
			matchMemoryPinned := gstruct.MatchKeys(gstruct.IgnoreExtras, gstruct.Keys{
				"kubelet_memory_manager_pinning_requests_total": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(1),
				}),
				"kubelet_memory_manager_pinning_errors_total": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(0),
				}),
			})
			gomega.Eventually(ctx, getWindowsKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchMemoryPinned)
		})

		ginkgo.It("should reject the pod when the memory manager cannot fit its own allocation", func(ctx context.Context) {
			// 这里测试 memory manager 自主决策的路径，这是唯一可能因容量失败的路径。
			// 在 Windows 上，当容器拥有独占 CPU 时，memory manager 会跟随 CPU manager
			// 的 NUMA 决策（cpuFollowingStore），跳过 hint 扩展并以 best-effort 绑定到
			// CPU 所在的 node（此时不会失败——过大的 request 会在后面被 node-fit
			// predicate 拒绝）。要触发 memory manager 自行选择/扩展 NUMA node 并可能
			// 返回分配错误的路径，这里使用申请小数 CPU 的 Guaranteed pod：它拿不到
			// 独占 CPU，cpuFollowingStore 回退到 topology hint，memory manager 自行扩展。
			// 过大的内存 request 无法适配任何 NUMA node，因此 Allocate 失败、pod 被拒绝，
			// 两个内存固定计数器都会增加
			testPod = e2epod.NewPodClient(f).Create(ctx, makeGuaranteedMemoryManagerPodWindows("topo-mem-fail", "500m", "1000Gi"))

			ginkgo.By("verifying the pod is rejected (not admitted to run)")
			gomega.Eventually(ctx, func(ctx context.Context) (v1.PodPhase, error) {
				p, err := e2epod.NewPodClient(f).Get(ctx, testPod.Name, metav1.GetOptions{})
				if err != nil {
					return "", err
				}
				return p.Status.Phase, nil
			}, 1*time.Minute, 5*time.Second).Should(gomega.Equal(v1.PodFailed),
				"a pod the memory manager cannot satisfy on its own-decision path must fail admission")

			ginkgo.By("verifying the memory manager reported a pinning failure")
			matchMemoryPinningFailure := gstruct.MatchKeys(gstruct.IgnoreExtras, gstruct.Keys{
				"kubelet_memory_manager_pinning_requests_total": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(1),
				}),
				"kubelet_memory_manager_pinning_errors_total": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(1),
				}),
			})
			gomega.Eventually(ctx, getWindowsKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchMemoryPinningFailure)
		})

		ginkgo.It("should not report a memory pinning failure for a guaranteed integer-CPU pod even when memory cannot fit", func(ctx context.Context) {
			// 这是上面小数 CPU 失败用例的对照，测试 memory manager 的 FOLLOW-CPU
			// 路径（policy_best_effort.go 中新增的 skip-extend 逻辑）。pod 申请整数 CPU，
			// CPU manager 分配独占 CPU，memory manager 通过 cpuFollowingStore 跟随其
			// NUMA node。由于容器在跟随 CPU manager，即使过大的内存 request 无法适配
			// CPU 所在 node，memory manager 也不会把 hint 扩展到其他 NUMA node——
			// 它以 best-effort 绑定并返回成功。因此与小数 CPU 场景不同，memory manager
			// 只记录一次固定请求，不记录固定错误；pod 最终因内存不足被 node-fit
			// predicate 拒绝，而非被 memory manager 拒绝。
			//
			// 内存 request 与上面的用例同样过大，唯一区别是 CPU request（整数 vs 小数），
			// 这正是切换 skip-extend 行为的因素，也是在单 NUMA node 上观察新增逻辑的
			// 信号
			const exclusiveCPUs = 2
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), exclusiveCPUs)

			testPod = e2epod.NewPodClient(f).Create(ctx, makeGuaranteedMemoryManagerPodWindows("topo-mem-follow", "2", "1000Gi"))

			ginkgo.By("verifying the memory manager recorded a pinning request but no pinning error")
			matchNoPinningFailure := gstruct.MatchKeys(gstruct.IgnoreExtras, gstruct.Keys{
				"kubelet_memory_manager_pinning_requests_total": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(1),
				}),
				"kubelet_memory_manager_pinning_errors_total": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(0),
				}),
			})
			gomega.Eventually(ctx, getWindowsKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchNoPinningFailure)
			gomega.Consistently(ctx, getWindowsKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchNoPinningFailure)
		})
	})
})

// buildWindowsTopologyManagerKubeletConfig 返回一份 KubeletConfiguration，
// 在给定的 topology manager 策略和 scope 下同时启用 CPU manager（static）
// 和 memory manager（BestEffort）。它以 memory manager 配置为基础
// （其中还包含 memory manager 启动时会校验的 reserved-memory 数值），
// 并覆盖 topology 策略/scope，使 topology manager 充当两个 manager 的协调者
func buildWindowsTopologyManagerKubeletConfig(oldCfg *kubeletconfig.KubeletConfiguration, policy, scope string) *kubeletconfig.KubeletConfiguration {
	newCfg := buildWindowsMemoryManagerKubeletConfig(oldCfg)
	newCfg.TopologyManagerPolicy = policy
	newCfg.TopologyManagerScope = scope
	return newCfg
}

// createGuaranteedTopologyPod 创建一个 Guaranteed-QoS pod，其中只有一个名为
// "gu-ctr" 的容器申请指定的 CPU 数量（CPU 和内存的 request == limit），
// 等待其运行后返回 pod。整数 CPU（如 "2000m"）会获得独占 CPU；小数 CPU
// （如 "500m"）虽然仍是 Guaranteed，但拿不到 CPU manager 分配的独占 CPU
func createGuaranteedTopologyPod(ctx context.Context, f *framework.Framework, name, cpu string) *v1.Pod {
	pod := makeWindowsCPUManagerPod(name, []windowsCtnAttribute{
		{name: "gu-ctr", cpuRequest: cpu, cpuLimit: cpu},
	})
	return e2epod.NewPodClient(f).CreateSync(ctx, pod)
}
