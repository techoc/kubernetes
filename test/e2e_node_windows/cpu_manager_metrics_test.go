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
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	admissionapi "k8s.io/pod-security-admission/api"
)

// Windows CPU manager 指标。CPU manager 的 pinning/pool 指标位于跨平台的 static 策略中，
// 因此在 Windows 上的递增方式与 Linux 相同。它们是普通 counter/gauge（不是 label vector），
// 因此冷重启后能正确读取，不会像 topology manager 的 numa_node 序列那样
// 受懒初始化问题影响。
// topology manager numa_node series.
//
// 这里只镜像 OS 无关的 Linux 用例。有意省略 SMT（full-pcpus-only）、
// uncore-cache 和按 NUMA 分配的用例，因为这些选项依赖的 SMT/L3/NUMA-distance 拓扑
// Windows CRI 不会暴露。
// not expose.
var _ = SIGWindowsDescribe(feature.CPUManager, feature.Windows, framework.WithSerial(), func() {
	f := framework.NewDefaultFramework("cpumanager-metrics-windows")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.Context("when querying /metrics", func() {
		var oldCfg *kubeletconfig.KubeletConfiguration
		var testPod *v1.Pod

		ginkgo.BeforeEach(func(ctx context.Context) {
			var err error
			if oldCfg == nil {
				oldCfg, err = getCurrentKubeletConfig(ctx)
				framework.ExpectNoError(err)
			}

			newCfg := buildWindowsCPUManagerKubeletConfig(oldCfg, true)
			// 让 topology manager 不参与准入决策，保证 guaranteed Pod 直接进入
			// CPU manager 的 Allocate，使 pinning / 独占分配计数器递增。
			// 独占分配计数器递增。
			newCfg.TopologyManagerPolicy = topologymanager.PolicyNone
			// 切换 CPU manager 策略会让持久化的 state checkpoint 失效，
			// 因此重启前清除它，避免 kubelet 因策略不匹配而拒绝启动。
			//
			updateWindowsKubeletConfigClearState(ctx, f, newCfg)
		})

		ginkgo.AfterEach(func(ctx context.Context) {
			if testPod != nil {
				e2epod.NewPodClient(f).DeleteSync(ctx, testPod.Name, metav1.DeleteOptions{}, f.Timeouts.PodDelete)
				testPod = nil
			}
			updateWindowsKubeletConfigClearState(ctx, f, oldCfg)
		})

		ginkgo.It("should report zero pinning counters and an idle CPU pool after a fresh restart", func(ctx context.Context) {
			// BeforeEach 中已重启 kubelet，且是 [Serial]，没有其他
			// Pod 在运行：没有发生 pinning，没有独占分配，共享池持有全部可分配 CPU。
			//
			matchResourceMetrics := gstruct.MatchKeys(gstruct.IgnoreExtras, gstruct.Keys{
				"kubelet_cpu_manager_pinning_requests_total": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(0),
				}),
				"kubelet_cpu_manager_pinning_errors_total": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(0),
				}),
				"kubelet_cpu_manager_exclusive_cpu_allocation_count": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(0),
				}),
				"kubelet_cpu_manager_shared_pool_size_millicores": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": checkMetricValueGreaterThan(0),
				}),
			})

			gomega.Eventually(ctx, getWindowsKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchResourceMetrics)
			gomega.Consistently(ctx, getWindowsKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchResourceMetrics)
		})

		ginkgo.It("should report pinning failures when the cpumanager allocation is known to fail", func(ctx context.Context) {
			// Linux 的 failure 用例通过 full-pcpus-only 选项触发 SMTAlignmentError，
			// 但 Windows 上该选项不可用（CRI 不暴露 SMT sibling 拓扑）。改为请求超过节点可分配数的
			// 整个独占 CPU：static 策略的 Allocate 会执行（拓扑准入先于 node-fit 判定），
			// 并以 "not enough cpus available" 失败，递增通用的
			// pinning 请求/错误计数器。有意不断言 physical_cpu 对齐失败序列，
			// 因为它只在 full-pcpus-only 下递增。
			//
			//
			//
			_, cpuAlloc, _ := getLocalNodeCPUDetails(ctx, f)
			testPod = e2epod.NewPodClient(f).Create(ctx, makeGuaranteedCPUExclusivePausePodWindows("cpu-pin-fail", int(cpuAlloc)+1))

			matchResourceMetrics := gstruct.MatchKeys(gstruct.IgnoreExtras, gstruct.Keys{
				"kubelet_cpu_manager_pinning_requests_total": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(1),
				}),
				"kubelet_cpu_manager_pinning_errors_total": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(1),
				}),
				"kubelet_cpu_manager_exclusive_cpu_allocation_count": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(0),
				}),
			})

			gomega.Eventually(ctx, getWindowsKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchResourceMetrics)
			gomega.Consistently(ctx, getWindowsKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchResourceMetrics)
		})

		ginkgo.It("should report pinning success and exclusive allocation when a guaranteed pod runs", func(ctx context.Context) {
			// 请求整颗 CPU 的 Guaranteed Pod 会被 static 策略 pinning：
			// pinning 请求计数器递增，不出现错误，独占分配计数反映发出的 CPU 数量。
			//
			const exclusiveCPUs = 2
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), exclusiveCPUs)

			testPod = e2epod.NewPodClient(f).Create(ctx, makeGuaranteedCPUExclusivePausePodWindows("cpu-pin-ok", exclusiveCPUs))

			matchResourceMetrics := gstruct.MatchKeys(gstruct.IgnoreExtras, gstruct.Keys{
				"kubelet_cpu_manager_pinning_requests_total": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(1),
				}),
				"kubelet_cpu_manager_pinning_errors_total": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(0),
				}),
				"kubelet_cpu_manager_exclusive_cpu_allocation_count": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(exclusiveCPUs),
				}),
			})

			gomega.Eventually(ctx, getWindowsKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchResourceMetrics)
			gomega.Consistently(ctx, getWindowsKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchResourceMetrics)
		})
	})
})
