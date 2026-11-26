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
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	admissionapi "k8s.io/pod-security-admission/api"
)

// Windows 上 memory manager 运行 "BestEffort" 策略（没有 cpuset.mems 等价物，
// 因此内存放置以 best-effort 方式跟随 CPU manager 的 NUMA 决策）。BestEffort 策略把分配
// 委托给与 static 策略相同的逻辑，因此仍会递增
// kubelet_memory_manager_pinning_* 计数器。这些是普通 counter（不是 label vector），
// 所以与 topology manager 的 numa_node 序列不同，它们在注册时创建，
// 冷重启后读取为 0。
// created at registration time and read 0 on a fresh restart.
var _ = SIGWindowsDescribe(feature.MemoryManager, feature.Windows, framework.WithSerial(), func() {
	f := framework.NewDefaultFramework("memorymanager-metrics-windows")
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

			// 切换 memory manager 策略（None <-> BestEffort）会让持久化的 state checkpoint 失效，
			// 因此必须先删除 memory_manager_state 文件，kubelet 才会启动。进入时清除它。
			//
			updateWindowsKubeletConfigClearState(ctx, f, buildWindowsMemoryManagerKubeletConfig(oldCfg))
		})

		ginkgo.AfterEach(func(ctx context.Context) {
			if testPod != nil {
				e2epod.NewPodClient(f).DeleteSync(ctx, testPod.Name, metav1.DeleteOptions{}, f.Timeouts.PodDelete)
				testPod = nil
			}
			// 再次恢复原始（None）策略同样改变了 memory manager 策略，
			// 也需要清除 state checkpoint。
			updateWindowsKubeletConfigClearState(ctx, f, oldCfg)
		})

		ginkgo.It("should report zero pinning counters after a fresh restart", func(ctx context.Context) {
			// BeforeEach 中已重启 kubelet，且是 [Serial]，没有其他
			// Pod 在运行，因此 pinning 计数器必须为 0。
			matchResourceMetrics := gstruct.MatchKeys(gstruct.IgnoreExtras, gstruct.Keys{
				"kubelet_memory_manager_pinning_requests_total": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(0),
				}),
				"kubelet_memory_manager_pinning_errors_total": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(0),
				}),
			})

			gomega.Eventually(ctx, getWindowsKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchResourceMetrics)
			gomega.Consistently(ctx, getWindowsKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchResourceMetrics)
		})

		ginkgo.It("should report pinning failures when the memory manager allocation is known to fail", func(ctx context.Context) {
			// memory manager 只有在自己做 NUMA 放置决策且该 Allocate 失败时，
			// 才会记录 pinning 错误。只有带小数 CPU 请求的 Guaranteed Pod 才会走到这条路径：
			// 它拿不到独占 CPU，因此 memory manager 不会跟随 CPU manager 的节点，
			// 而是自己选择/扩展 NUMA 节点。整数 CPU 请求会走 follow-CPU 路径，
			// 它是 best-effort 且永不报错（Pod 只会在之后被 node-fit
			// predicate 拒绝）。超大的内存请求无法适配任何 NUMA 节点，因此 Allocate
			// 失败，pinning 请求和错误计数器都递增。
			//
			//
			testPod = e2epod.NewPodClient(f).Create(ctx, makeGuaranteedMemoryManagerPodWindows("memmngr-fail", "500m", "1000Gi"))

			matchResourceMetrics := gstruct.MatchKeys(gstruct.IgnoreExtras, gstruct.Keys{
				"kubelet_memory_manager_pinning_requests_total": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(1),
				}),
				"kubelet_memory_manager_pinning_errors_total": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(1),
				}),
			})

			gomega.Eventually(ctx, getWindowsKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchResourceMetrics)
			gomega.Consistently(ctx, getWindowsKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchResourceMetrics)
		})

		ginkgo.It("should not report any pinning failures when the memory manager allocation is expected to succeed", func(ctx context.Context) {
			// 内存请求较小的 guaranteed Pod 会被准入并 pinning，
			// 因此请求计数器递增，错误计数器保持为 0。
			testPod = e2epod.NewPodClient(f).Create(ctx, makeGuaranteedMemoryManagerPodWindows("memmngr-ok", "1", "64Mi"))

			matchResourceMetrics := gstruct.MatchKeys(gstruct.IgnoreExtras, gstruct.Keys{
				"kubelet_memory_manager_pinning_requests_total": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(1),
				}),
				"kubelet_memory_manager_pinning_errors_total": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(0),
				}),
			})

			gomega.Eventually(ctx, getWindowsKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchResourceMetrics)
			gomega.Consistently(ctx, getWindowsKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchResourceMetrics)
		})
	})
})
