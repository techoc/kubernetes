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
	"fmt"
	"reflect"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/kubernetes/pkg/features"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2enodekubelet "k8s.io/kubernetes/test/e2e_node_windows/kubeletconfig"
	admissionapi "k8s.io/pod-security-admission/api"
)

/*
 * Windows CPU 亲和性 Node E2E 测试。
 *
 * 这些测试验证 CPU manager 能通过 CRI 正确设置容器的 Windows CPU group 亲和性，
 * 对应实现位于：
 *   - pkg/kubelet/cm/cpumanager/cpu_manager_windows.go
 *   - pkg/kubelet/cm/internal_container_lifecycle_windows.go
 *
 * 前置条件：
 *   - Windows 节点上 kubelet 以名为 "kubelet" 的 Windows 服务运行
 *   - 节点至少可分配 2 个整数 CPU
 *   - 容器运行时为 containerd
 *
 * 以下仅 Linux 支持的功能不在本测试覆盖范围内：
 *   - SMT/HT 对齐（FullPCPUsOnlyOption）—— Windows CRI 无对应概念
 *   - Uncore cache 对齐（PreferAlignByUnCoreCacheOption）—— Windows CRI 不提供 L3 拓扑
 *   - CFS 配额管理 —— cgroup 概念，Windows 不适用
 *   - 严格 CPU 预留选项 —— Linux 上通过 cgroup 实现
 */

var _ = SIGWindowsDescribe(feature.CPUManager, feature.Windows, ginkgo.Ordered, ginkgo.ContinueOnFailure, framework.WithSerial(), func() {
	f := framework.NewDefaultFramework("cpu-manager-windows")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	var (
		oldCfg    *kubeletconfig.KubeletConfiguration
		criClient internalapi.RuntimeService
		podMap    map[string]*v1.Pod
	)

	// createPodSync 创建 Pod，等待其进入 running，把它
	// 注册到 podMap 以便清理，并返回更新后的 Pod 对象。
	var createPodSync func(ctx context.Context, pod *v1.Pod) *v1.Pod

	ginkgo.BeforeAll(func(ctx context.Context) {
		var err error
		oldCfg, err = getCurrentKubeletConfig(ctx)
		framework.ExpectNoError(err, "failed to get current kubelet config")

		criClient, _, err = getCRIClient(ctx)
		framework.ExpectNoError(err, "failed to get CRI client")
	})

	ginkgo.AfterAll(func(ctx context.Context) {
		updateWindowsKubeletConfig(ctx, f, oldCfg)
	})

	ginkgo.BeforeEach(func(ctx context.Context) {
		podMap = make(map[string]*v1.Pod)
		createPodSync = func(ctx context.Context, pod *v1.Pod) *v1.Pod {
			newPod := e2epod.NewPodClient(f).CreateSync(ctx, pod)
			podMap[string(newPod.UID)] = newPod
			return newPod
		}
	})

	ginkgo.AfterEach(func(ctx context.Context) {
		for _, pod := range podMap {
			e2epod.NewPodClient(f).DeleteSync(ctx, pod.Name, metav1.DeleteOptions{}, f.Timeouts.PodDelete)
		}
	})

	// -------------------------------------------------------------------------
	// 非 Guaranteed（burstable / best-effort）Pod 测试
	// 对应 Linux 的 "running non-guaranteed pods tests" 组。
	// -------------------------------------------------------------------------
	ginkgo.When("running non-guaranteed pods", ginkgo.Label("non-guaranteed"), func() {
		ginkgo.BeforeEach(func(ctx context.Context) {
			updateWindowsKubeletConfigIfNeeded(ctx, f, buildWindowsCPUManagerKubeletConfig(oldCfg, true))
		})

		ginkgo.It("should not set CPU affinity on a burstable container", func(ctx context.Context) {
			pod := makeWindowsCPUManagerPod("burstable-pod", []windowsCtnAttribute{
				{name: "burstable-ctr", cpuRequest: "100m", cpuLimit: "500m"},
			})
			ginkgo.By("creating the burstable pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("verifying no exclusive CPU affinity is set")
			affinities, err := getWindowsContainerCPUAffinity(ctx, criClient, pod, "burstable-ctr")
			framework.ExpectNoError(err)
			cpuCount := countCPUsInAffinities(affinities)
			hostCPUs := int(getLocalNode(ctx, f).Status.Capacity.Cpu().Value())
			gomega.Expect(cpuCount).To(gomega.Equal(hostCPUs),
				"burstable container must not receive exclusive CPU affinity: got %d CPUs (host has %d)", cpuCount, hostCPUs)
		})

		// 对应 Linux 的 "should let the container access all the online non-exclusively-allocated
		// CPUs when using a reserved CPUs set" 用例。
		// Windows 上通过 CRI 校验：guaranteed 容器设置了 affinity，而
		// burstable 容器没有，意味着 burstable 运行在共享池上。
		ginkgo.It("should set affinity only on the guaranteed container when coexisting with a burstable pod", func(ctx context.Context) {
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), 2) // 1 for guaranteed + 1 reserved

			guPod := makeWindowsCPUManagerPod("gu-pod", []windowsCtnAttribute{
				{name: "gu-ctr", cpuRequest: "1000m", cpuLimit: "1000m"},
			})
			ginkgo.By("creating the guaranteed pod")
			guPod = createPodSync(ctx, guPod)

			buPod := makeWindowsCPUManagerPod("bu-pod", []windowsCtnAttribute{
				{name: "bu-ctr", cpuRequest: "100m", cpuLimit: "300m"},
			})
			ginkgo.By("creating the burstable pod")
			buPod = createPodSync(ctx, buPod)

			ginkgo.By("verifying guaranteed container has CPU affinity set")
			gomega.Eventually(ctx, func(ctx context.Context) (int, error) {
				aff, err := getWindowsContainerCPUAffinity(ctx, criClient, guPod, "gu-ctr")
				if err != nil {
					return 0, err
				}
				return countCPUsInAffinities(aff), nil
			}, 30*time.Second, 2*time.Second).Should(gomega.Equal(1),
				"guaranteed container should have exactly 1 CPU affinity")

			ginkgo.By("verifying burstable container runs on the shared pool, not exclusively pinned")
			buAff, err := getWindowsContainerCPUAffinity(ctx, criClient, buPod, "bu-ctr")
			framework.ExpectNoError(err)
			guAff, err := getWindowsContainerCPUAffinity(ctx, criClient, guPod, "gu-ctr")
			framework.ExpectNoError(err)
			// burstable 容器使用的共享池会随着 guaranteed Pod 占用独占 CPU 而缩小，
			// 因此期望大小为 hostCPUs - guaranteedExclusive。
			buCPUs := countCPUsInAffinities(buAff)
			guCPUs := countCPUsInAffinities(guAff)
			hostCPUs := int(getLocalNode(ctx, f).Status.Capacity.Cpu().Value())
			sharedPool := hostCPUs - guCPUs
			gomega.Expect(buCPUs).To(gomega.Equal(sharedPool),
				"burstable container must not receive exclusive CPU affinity: got %d CPUs (host=%d, guaranteed=%d, shared pool=%d)",
				buCPUs, hostCPUs, guCPUs, sharedPool)
		})
	})

	// -------------------------------------------------------------------------
	// Guaranteed Pod 测试（feature gate 开启）
	// 对应 Linux 的 "running guaranteed pod tests" 组。
	// -------------------------------------------------------------------------
	ginkgo.When("running guaranteed pods with exclusive CPU allocation", ginkgo.Label("guaranteed", "exclusive-cpus"), func() {
		ginkgo.BeforeEach(func(ctx context.Context) {
			updateWindowsKubeletConfigIfNeeded(ctx, f, buildWindowsCPUManagerKubeletConfig(oldCfg, true))
		})

		// 对应 "should allocate exclusively a CPU to a 1-container pod"。
		ginkgo.It("should set CPU affinity with exactly 1 CPU for a single-container pod", func(ctx context.Context) {
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), 1)

			pod := makeWindowsCPUManagerPod("gu-pod-1cpu", []windowsCtnAttribute{
				{name: "gu-ctr", cpuRequest: "1000m", cpuLimit: "1000m"},
			})
			ginkgo.By("creating the guaranteed pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("verifying CPU affinity is set to exactly 1 CPU")
			gomega.Eventually(ctx, func(ctx context.Context) (int, error) {
				aff, err := getWindowsContainerCPUAffinity(ctx, criClient, pod, "gu-ctr")
				if err != nil {
					return 0, err
				}
				return countCPUsInAffinities(aff), nil
			}, 30*time.Second, 2*time.Second).Should(gomega.Equal(1),
				"expected exactly 1 CPU in affinity mask")

			ginkgo.By("verifying the host job-object affinity agrees with the CRI report")
			verifyHostMatchesCRI(ctx, criClient, pod, "gu-ctr")
		})

		// 对应 "should allocate exclusively a even number of CPUs to a 1-container pod"。
		ginkgo.It("should set CPU affinity with exactly 2 CPUs for a single-container pod", func(ctx context.Context) {
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), 2)

			pod := makeWindowsCPUManagerPod("gu-pod-2cpu", []windowsCtnAttribute{
				{name: "gu-ctr", cpuRequest: "2000m", cpuLimit: "2000m"},
			})
			ginkgo.By("creating the guaranteed pod requesting 2 CPUs")
			pod = createPodSync(ctx, pod)

			ginkgo.By("verifying CPU affinity is set to exactly 2 CPUs")
			gomega.Eventually(ctx, func(ctx context.Context) (int, error) {
				aff, err := getWindowsContainerCPUAffinity(ctx, criClient, pod, "gu-ctr")
				if err != nil {
					return 0, err
				}
				return countCPUsInAffinities(aff), nil
			}, 30*time.Second, 2*time.Second).Should(gomega.Equal(2),
				"expected exactly 2 CPUs in affinity mask")

			ginkgo.By("verifying the host job-object affinity agrees with the CRI report")
			verifyHostMatchesCRI(ctx, criClient, pod, "gu-ctr")
		})

		// 对应 "should allocate exclusively a odd number of CPUs to a 1-container pod"。
		ginkgo.It("should set CPU affinity with exactly 3 CPUs for a single-container pod", func(ctx context.Context) {
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), 3)

			pod := makeWindowsCPUManagerPod("gu-pod-3cpu", []windowsCtnAttribute{
				{name: "gu-ctr", cpuRequest: "3000m", cpuLimit: "3000m"},
			})
			ginkgo.By("creating the guaranteed pod requesting 3 CPUs")
			pod = createPodSync(ctx, pod)

			ginkgo.By("verifying CPU affinity is set to exactly 3 CPUs")
			gomega.Eventually(ctx, func(ctx context.Context) (int, error) {
				aff, err := getWindowsContainerCPUAffinity(ctx, criClient, pod, "gu-ctr")
				if err != nil {
					return 0, err
				}
				return countCPUsInAffinities(aff), nil
			}, 30*time.Second, 2*time.Second).Should(gomega.Equal(3),
				"expected exactly 3 CPUs in affinity mask")

			ginkgo.By("verifying the host job-object affinity agrees with the CRI report")
			verifyHostMatchesCRI(ctx, criClient, pod, "gu-ctr")
		})

		// 对应 "should allocate exclusively CPUs to a multi-container pod (1+2)"。
		ginkgo.It("should assign non-overlapping CPU affinity to each container in a multi-container pod (1+2)", func(ctx context.Context) {
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), 3)

			pod := makeWindowsCPUManagerPod("gu-pod-1plus2", []windowsCtnAttribute{
				{name: "gu-ctr-1", cpuRequest: "1000m", cpuLimit: "1000m"},
				{name: "gu-ctr-2", cpuRequest: "2000m", cpuLimit: "2000m"},
			})
			ginkgo.By("creating the guaranteed pod with containers requesting 1 and 2 CPUs")
			pod = createPodSync(ctx, pod)

			var aff1, aff2 []*runtimeapi.WindowsCpuGroupAffinity
			ginkgo.By("verifying each container gets the correct CPU count")
			gomega.Eventually(ctx, func(ctx context.Context) error {
				var err error
				aff1, err = getWindowsContainerCPUAffinity(ctx, criClient, pod, "gu-ctr-1")
				if err != nil {
					return err
				}
				aff2, err = getWindowsContainerCPUAffinity(ctx, criClient, pod, "gu-ctr-2")
				if err != nil {
					return err
				}
				c1, c2 := countCPUsInAffinities(aff1), countCPUsInAffinities(aff2)
				if c1 != 1 || c2 != 2 {
					return fmt.Errorf("want cpu counts (1,2), got (%d,%d)", c1, c2)
				}
				return nil
			}, 30*time.Second, 2*time.Second).Should(gomega.Succeed())

			ginkgo.By("verifying the CPU affinity masks do not overlap")
			gomega.Expect(windowsAffinitiesOverlap(aff1, aff2)).To(gomega.BeFalse(),
				"containers in the same pod must receive non-overlapping CPU affinity masks")

			ginkgo.By("verifying the host job-object affinity agrees with the CRI report and masks are disjoint")
			verifyHostMatchesCRI(ctx, criClient, pod, "gu-ctr-1")
			verifyHostMatchesCRI(ctx, criClient, pod, "gu-ctr-2")
			verifyHostMasksDisjoint(pod, "gu-ctr-1", pod, "gu-ctr-2")
		})

		// 对应 "should allocate exclusively CPUs to a multi-container pod (3+2)"。
		ginkgo.It("should assign non-overlapping CPU affinity to each container in a multi-container pod (3+2)", func(ctx context.Context) {
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), 5)

			pod := makeWindowsCPUManagerPod("gu-pod-3plus2", []windowsCtnAttribute{
				{name: "gu-ctr-1", cpuRequest: "3000m", cpuLimit: "3000m"},
				{name: "gu-ctr-2", cpuRequest: "2000m", cpuLimit: "2000m"},
			})
			ginkgo.By("creating the guaranteed pod with containers requesting 3 and 2 CPUs")
			pod = createPodSync(ctx, pod)

			var aff1, aff2 []*runtimeapi.WindowsCpuGroupAffinity
			gomega.Eventually(ctx, func(ctx context.Context) error {
				var err error
				aff1, err = getWindowsContainerCPUAffinity(ctx, criClient, pod, "gu-ctr-1")
				if err != nil {
					return err
				}
				aff2, err = getWindowsContainerCPUAffinity(ctx, criClient, pod, "gu-ctr-2")
				if err != nil {
					return err
				}
				c1, c2 := countCPUsInAffinities(aff1), countCPUsInAffinities(aff2)
				if c1 != 3 || c2 != 2 {
					return fmt.Errorf("want cpu counts (3,2), got (%d,%d)", c1, c2)
				}
				return nil
			}, 30*time.Second, 2*time.Second).Should(gomega.Succeed())

			gomega.Expect(windowsAffinitiesOverlap(aff1, aff2)).To(gomega.BeFalse(),
				"containers in the same pod must receive non-overlapping CPU affinity masks")

			ginkgo.By("verifying the host job-object affinity agrees with the CRI report and masks are disjoint")
			verifyHostMatchesCRI(ctx, criClient, pod, "gu-ctr-1")
			verifyHostMatchesCRI(ctx, criClient, pod, "gu-ctr-2")
			verifyHostMasksDisjoint(pod, "gu-ctr-1", pod, "gu-ctr-2")
		})

		// 对应 "should allocate exclusively a CPU to multiple 1-container pods"。
		ginkgo.It("should assign non-overlapping CPU affinity across separate guaranteed pods (2+2)", func(ctx context.Context) {
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), 4)

			pod1 := makeWindowsCPUManagerPod("gu-pod-a", []windowsCtnAttribute{
				{name: "gu-ctr-a", cpuRequest: "2000m", cpuLimit: "2000m"},
			})
			ginkgo.By("creating guaranteed pod 1")
			pod1 = createPodSync(ctx, pod1)

			pod2 := makeWindowsCPUManagerPod("gu-pod-b", []windowsCtnAttribute{
				{name: "gu-ctr-b", cpuRequest: "2000m", cpuLimit: "2000m"},
			})
			ginkgo.By("creating guaranteed pod 2")
			pod2 = createPodSync(ctx, pod2)

			var affA, affB []*runtimeapi.WindowsCpuGroupAffinity
			ginkgo.By("verifying both containers each get exactly 2 CPUs")
			gomega.Eventually(ctx, func(ctx context.Context) error {
				var err error
				affA, err = getWindowsContainerCPUAffinity(ctx, criClient, pod1, "gu-ctr-a")
				if err != nil {
					return err
				}
				affB, err = getWindowsContainerCPUAffinity(ctx, criClient, pod2, "gu-ctr-b")
				if err != nil {
					return err
				}
				cA, cB := countCPUsInAffinities(affA), countCPUsInAffinities(affB)
				if cA != 2 || cB != 2 {
					return fmt.Errorf("want cpu counts (2,2), got (%d,%d)", cA, cB)
				}
				return nil
			}, 30*time.Second, 2*time.Second).Should(gomega.Succeed())

			ginkgo.By("verifying the two pods' CPU affinity masks do not overlap")
			gomega.Expect(windowsAffinitiesOverlap(affA, affB)).To(gomega.BeFalse(),
				"guaranteed pods must not share exclusively-allocated CPUs")

			ginkgo.By("verifying the host job-object affinity agrees with the CRI report and masks are disjoint")
			verifyHostMatchesCRI(ctx, criClient, pod1, "gu-ctr-a")
			verifyHostMatchesCRI(ctx, criClient, pod2, "gu-ctr-b")
			verifyHostMasksDisjoint(pod1, "gu-ctr-a", pod2, "gu-ctr-b")
		})
	})

	// -------------------------------------------------------------------------
	// feature gate 关闭的测试
	// 对应 Linux 的 "running guaranteed pod tests with feature gates disabled"
	// 组，并针对 Windows 特有的 WindowsCPUAndMemoryAffinity gate 做了适配。
	// gate 关闭时，cpu_manager_windows.go 的 updateContainerCPUSet 会立即返回，
	// 不调用 UpdateContainerResources，因此 CRI 上即使是 guaranteed 容器
	// 也不应报告 AffinityCpus。
	// -------------------------------------------------------------------------
	ginkgo.When("running guaranteed pods with WindowsCPUAndMemoryAffinity feature gate disabled", ginkgo.Label("guaranteed", "feature-gate-disabled"), func() {
		ginkgo.BeforeEach(func(ctx context.Context) {
			updateWindowsKubeletConfigIfNeeded(ctx, f, buildWindowsCPUManagerKubeletConfig(oldCfg, false))
		})

		ginkgo.It("should NOT set CPU affinity on a 1-CPU guaranteed container when feature gate is off", func(ctx context.Context) {
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), 1)

			pod := makeWindowsCPUManagerPod("gu-pod-no-affinity", []windowsCtnAttribute{
				{name: "gu-ctr", cpuRequest: "1000m", cpuLimit: "1000m"},
			})
			ginkgo.By("creating the guaranteed pod")
			pod = createPodSync(ctx, pod)

			// 给 CPU manager 的 reconcile 循环（1 秒周期）几个周期生效，
			// 然后确认它仍未设置任何独占亲和性。在 Windows 上
			// "未分配亲和性" 可能表现为空列表，也可能是覆盖所有主机 CPU 的完整
			// mask；介于两者之间的都是严格子集
			//（即独占 pinning），feature gate 必须阻止这种情况。
			ginkgo.By("verifying no exclusive CPU affinity is set even for a guaranteed container")
			hostCPUs := int(getLocalNode(ctx, f).Status.Capacity.Cpu().Value())
			gomega.Consistently(ctx, func(ctx context.Context) error {
				aff, err := getWindowsContainerCPUAffinity(ctx, criClient, pod, "gu-ctr")
				if err != nil {
					return err
				}
				cpuCount := countCPUsInAffinities(aff)
				if cpuCount != 0 && cpuCount < hostCPUs {
					return fmt.Errorf("feature gate off: guaranteed container received exclusive affinity: got %d CPUs (host has %d)", cpuCount, hostCPUs)
				}
				return nil
			}, 10*time.Second, 2*time.Second).Should(gomega.Succeed())
		})

		ginkgo.It("should NOT set CPU affinity on a multi-container guaranteed pod when feature gate is off", func(ctx context.Context) {
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), 3)

			pod := makeWindowsCPUManagerPod("gu-pod-mc-no-affinity", []windowsCtnAttribute{
				{name: "gu-ctr-1", cpuRequest: "1000m", cpuLimit: "1000m"},
				{name: "gu-ctr-2", cpuRequest: "2000m", cpuLimit: "2000m"},
			})
			ginkgo.By("creating the guaranteed multi-container pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("verifying no exclusive CPU affinity is set on either container")
			hostCPUs := int(getLocalNode(ctx, f).Status.Capacity.Cpu().Value())
			gomega.Consistently(ctx, func(ctx context.Context) error {
				aff1, err := getWindowsContainerCPUAffinity(ctx, criClient, pod, "gu-ctr-1")
				if err != nil {
					return err
				}
				aff2, err := getWindowsContainerCPUAffinity(ctx, criClient, pod, "gu-ctr-2")
				if err != nil {
					return err
				}
				c1 := countCPUsInAffinities(aff1)
				c2 := countCPUsInAffinities(aff2)
				if c1 != 0 && c1 < hostCPUs {
					return fmt.Errorf("feature gate off: gu-ctr-1 received exclusive affinity: got %d CPUs (host has %d)", c1, hostCPUs)
				}
				if c2 != 0 && c2 < hostCPUs {
					return fmt.Errorf("feature gate off: gu-ctr-2 received exclusive affinity: got %d CPUs (host has %d)", c2, hostCPUs)
				}
				return nil
			}, 10*time.Second, 2*time.Second).Should(gomega.Succeed())
		})
	})

	// -------------------------------------------------------------------------
	// Init / sidecar 容器测试
	// Windows 支持 init 容器和 restartable init（sidecar）容器。
	// 对应 Linux 的 "checking the sidecar containers" 组。
	//
	// 校验方式与 Linux 不同：Windows 上无法读取 cpuset 文件系统。改为：
	//
	//   - 对已终止的 init 容器，只能观察主容器的结果（CPU 被释放并复用）。
	//
	//   - 对运行中的 sidecar（restartable init）容器，查询 CRI。
	// -------------------------------------------------------------------------
	ginkgo.When("running pods with init containers", ginkgo.Label("guaranteed", "init-containers"), func() {
		ginkgo.BeforeEach(func(ctx context.Context) {
			updateWindowsKubeletConfigIfNeeded(ctx, f, buildWindowsCPUManagerKubeletConfig(oldCfg, true))
		})

		// 对应 "should reuse init container exclusive CPUs, but not sidecar container
		// exclusive CPUs"。
		// 已终止（不可重启）的 init 容器释放独占 CPU，
		// 让普通 app 容器复用。restartable sidecar init
		// 容器在整个生命周期内持有其 CPU。
		// 校验：
		//   1. sidecar 容器（restartable init）恰好持有 1 个独占 CPU。
		//   2. app 容器恰好持有 1 个独占 CPU。
		//   3. sidecar 与 app 的 CPU 不重叠（都是独占）。
		ginkgo.It("sidecar container should hold exclusive CPUs separately from the app container", func(ctx context.Context) {
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), 2) // 1 non-restartable init + 1 sidecar reused + 1 app = 2 total (init CPUs reused)

			var restartAlways = v1.ContainerRestartPolicyAlways

			pod := makeWindowsCPUManagerInitPod("gu-sidecar-pod",
				[]windowsCtnAttribute{
					// 不可重启 init：很快退出，CPU 被复用
					{name: "init-ctr", cpuRequest: "1000m", cpuLimit: "1000m"},
					// restartable sidecar：一直存活，持有独占 CPU
					{name: "sidecar-ctr", cpuRequest: "1000m", cpuLimit: "1000m", restartPolicy: &restartAlways},
				},
				// app 容器
				windowsCtnAttribute{name: "app-ctr", cpuRequest: "1000m", cpuLimit: "1000m"},
			)
			ginkgo.By("creating the pod with a non-restartable init container, a sidecar, and an app container")
			pod = createPodSync(ctx, pod)

			ginkgo.By("verifying sidecar container holds exactly 1 CPU")
			var sidecarAff, appAff []*runtimeapi.WindowsCpuGroupAffinity
			gomega.Eventually(ctx, func(ctx context.Context) error {
				var err error
				sidecarAff, err = getWindowsContainerCPUAffinity(ctx, criClient, pod, "sidecar-ctr")
				if err != nil {
					return err
				}
				appAff, err = getWindowsContainerCPUAffinity(ctx, criClient, pod, "app-ctr")
				if err != nil {
					return err
				}
				cs, ca := countCPUsInAffinities(sidecarAff), countCPUsInAffinities(appAff)
				if cs != 1 || ca != 1 {
					return fmt.Errorf("want sidecar=1 app=1, got sidecar=%d app=%d", cs, ca)
				}
				return nil
			}, 60*time.Second, 2*time.Second).Should(gomega.Succeed(),
				"both sidecar and app containers must each hold exactly 1 exclusive CPU")

			ginkgo.By("verifying sidecar and app container CPU affinity masks do not overlap")
			gomega.Expect(windowsAffinitiesOverlap(sidecarAff, appAff)).To(gomega.BeFalse(),
				"sidecar and app containers must not share exclusively-allocated CPUs")
		})

		// Pod 只有不可重启的 init 容器：init 容器完成后，
		// app 容器获得独占 CPU（可能与 init 容器先前的 CPU 重叠，这是预期且
		// 正确的；这里只校验 app 容器的数量）。

		ginkgo.It("app container should get exclusive CPUs after a non-restartable init container completes", func(ctx context.Context) {
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), 1)

			pod := makeWindowsCPUManagerInitPod("gu-init-pod",
				[]windowsCtnAttribute{
					// init 容器：短暂 sleep 后退出
					{name: "init-ctr", cpuRequest: "1000m", cpuLimit: "1000m"},
				},
				windowsCtnAttribute{name: "app-ctr", cpuRequest: "1000m", cpuLimit: "1000m"},
			)
			ginkgo.By("creating the pod with a non-restartable init container")
			pod = createPodSync(ctx, pod)

			ginkgo.By("verifying the app container holds exactly 1 exclusive CPU after init completes")
			gomega.Eventually(ctx, func(ctx context.Context) (int, error) {
				aff, err := getWindowsContainerCPUAffinity(ctx, criClient, pod, "app-ctr")
				if err != nil {
					return 0, err
				}
				return countCPUsInAffinities(aff), nil
			}, 60*time.Second, 2*time.Second).Should(gomega.Equal(1),
				"app container must hold exactly 1 exclusive CPU")
		})
	})
	// -------------------------------------------------------------------------
	// Strict 与非 strict CPU reservation
	// strict-cpu-reservation 策略选项会把预留 CPU 从共享池中移除，
	// 因此共享池（burstable）容器被限制在 (online - reserved) 范围。
	// 不启用时（默认）预留 CPU 仍属于共享池，burstable 容器仍可使用。
	// -------------------------------------------------------------------------
	// -------------------------------------------------------------------------
	ginkgo.When("running with the strict-cpu-reservation policy option", ginkgo.Label("non-guaranteed", "strict-cpu-reservation"), func() {
		// 切换 strict-cpu-reservation 会改变预留 CPU 是否属于默认池，
		// 从而使持久化的 CPU manager checkpoint 失效。退出时重置为干净的
		// 非 strict 状态，保证留下的 checkpoint 与套件其余部分兼容。

		ginkgo.AfterEach(func(ctx context.Context) {
			updateWindowsKubeletConfigClearState(ctx, f, buildWindowsCPUManagerKubeletConfig(oldCfg, true))
		})

		ginkgo.It("should exclude the reserved CPU from the burstable shared pool when strict-cpu-reservation is enabled", func(ctx context.Context) {
			node := getLocalNode(ctx, f)
			hostCPUs := int(node.Status.Capacity.Cpu().Value())
			if hostCPUs < 2 {
				ginkgo.Skip(fmt.Sprintf("strict-cpu-reservation test needs >= 2 CPUs (1 reserved + shared pool), node has %d", hostCPUs))
			}

			ginkgo.By("enabling the static CPU manager with strict-cpu-reservation and CPU 0 reserved")
			updateWindowsKubeletConfigClearState(ctx, f, buildWindowsStrictCPUReservationConfig(oldCfg))

			pod := createPodSync(ctx, makeWindowsCPUManagerPod("strict-burstable-pod", []windowsCtnAttribute{
				{name: "bu-ctr", cpuRequest: "100m", cpuLimit: "300m"},
			}))

			// reconcile 循环会在容器启动后不久应用共享池；strict 模式下预留的 CPU 0 被排除在外。
			//
			ginkgo.By("verifying the burstable container is confined to (host - reserved) CPUs")
			gomega.Eventually(ctx, func(ctx context.Context) (int, error) {
				aff, err := getWindowsContainerCPUAffinity(ctx, criClient, pod, "bu-ctr")
				if err != nil {
					return 0, err
				}
				return countCPUsInAffinities(aff), nil
			}, 60*time.Second, 2*time.Second).Should(gomega.Equal(hostCPUs-1),
				"with strict-cpu-reservation the burstable shared pool must exclude the 1 reserved CPU (host=%d)", hostCPUs)
		})

		ginkgo.It("should keep the reserved CPU in the burstable shared pool when strict-cpu-reservation is disabled (default)", func(ctx context.Context) {
			node := getLocalNode(ctx, f)
			hostCPUs := int(node.Status.Capacity.Cpu().Value())
			if hostCPUs < 2 {
				ginkgo.Skip(fmt.Sprintf("test needs >= 2 CPUs, node has %d", hostCPUs))
			}

			ginkgo.By("enabling the static CPU manager without strict-cpu-reservation (CPU 0 reserved)")
			updateWindowsKubeletConfigClearState(ctx, f, buildWindowsCPUManagerKubeletConfig(oldCfg, true))

			pod := createPodSync(ctx, makeWindowsCPUManagerPod("nonstrict-burstable-pod", []windowsCtnAttribute{
				{name: "bu-ctr", cpuRequest: "100m", cpuLimit: "300m"},
			}))

			// 非 strict 且没有独占分配时，共享池是整台机器，
			// 包括预留 CPU。
			ginkgo.By("verifying the burstable container can use all host CPUs (reserved included)")
			gomega.Eventually(ctx, func(ctx context.Context) (int, error) {
				aff, err := getWindowsContainerCPUAffinity(ctx, criClient, pod, "bu-ctr")
				if err != nil {
					return 0, err
				}
				return countCPUsInAffinities(aff), nil
			}, 60*time.Second, 2*time.Second).Should(gomega.Equal(hostCPUs),
				"without strict-cpu-reservation the burstable shared pool must include the reserved CPU (host=%d)", hostCPUs)
		})
	})

	// -------------------------------------------------------------------------
	// 共享池动态调整（reconcile 循环）
	// CPU manager 的 reconcile 循环会在 guaranteed Pod 占用和释放独占 CPU 时，
	// 更新*运行中*共享池容器的 affinity。校验的是实时迁移，不只是 Pod 启动时的稳态。
	// -------------------------------------------------------------------------
	// -------------------------------------------------------------------------
	ginkgo.When("dynamically resizing the shared pool", ginkgo.Label("guaranteed", "non-guaranteed", "shared-pool", "reconcile"), func() {
		ginkgo.BeforeEach(func(ctx context.Context) {
			updateWindowsKubeletConfigIfNeeded(ctx, f, buildWindowsCPUManagerKubeletConfig(oldCfg, true))
		})

		ginkgo.It("should shrink and grow a running burstable container's affinity as a guaranteed pod comes and goes", func(ctx context.Context) {
			node := getLocalNode(ctx, f)
			hostCPUs := int(node.Status.Capacity.Cpu().Value())
			// 需要 >= 2 个 CPU：CPU 0 被预留（非 strict 模式下仍留在共享池），
			// 另需至少一个非预留 CPU 供 guaranteed Pod 独占分配。这样共享池永不为空，
			// burstable mask 会从 hostCPUs 缩小到 hostCPUs-1 再恢复。
			//
			if hostCPUs < 2 {
				ginkgo.Skip(fmt.Sprintf("dynamic shared-pool test needs >= 2 CPUs, node has %d", hostCPUs))
			}

			buPod := createPodSync(ctx, makeWindowsCPUManagerPod("dyn-bu-pod", []windowsCtnAttribute{
				{name: "bu-ctr", cpuRequest: "100m", cpuLimit: "300m"},
			}))
			burstableCPUCount := func(ctx context.Context) (int, error) {
				aff, err := getWindowsContainerCPUAffinity(ctx, criClient, buPod, "bu-ctr")
				if err != nil {
					return 0, err
				}
				return countCPUsInAffinities(aff), nil
			}

			ginkgo.By("waiting for the burstable container to occupy the full shared pool")
			gomega.Eventually(ctx, burstableCPUCount, 60*time.Second, 2*time.Second).Should(gomega.Equal(hostCPUs),
				"with no exclusive allocations the burstable shared pool should span all %d host CPUs", hostCPUs)

			ginkgo.By("creating a guaranteed pod that takes 1 exclusive CPU")
			guPod := createPodSync(ctx, makeWindowsCPUManagerPod("dyn-gu-pod", []windowsCtnAttribute{
				{name: "gu-ctr", cpuRequest: "1000m", cpuLimit: "1000m"},
			}))

			ginkgo.By("verifying the running burstable container's affinity shrinks by the 1 exclusive CPU")
			gomega.Eventually(ctx, burstableCPUCount, 60*time.Second, 2*time.Second).Should(gomega.Equal(hostCPUs-1),
				"the reconcile loop should remove the guaranteed pod's exclusive CPU from the running burstable container's mask")

			ginkgo.By("deleting the guaranteed pod to release its exclusive CPU")
			e2epod.NewPodClient(f).DeleteSync(ctx, guPod.Name, metav1.DeleteOptions{}, f.Timeouts.PodDelete)
			delete(podMap, string(guPod.UID))

			ginkgo.By("verifying the running burstable container's affinity grows back to the full shared pool")
			gomega.Eventually(ctx, burstableCPUCount, 60*time.Second, 2*time.Second).Should(gomega.Equal(hostCPUs),
				"the reconcile loop should return the released CPU to the running burstable container's mask")
		})
	})
})

// -------------------------------------------------------------------------
// Pod / 容器创建 helper
// -------------------------------------------------------------------------

// windowsCtnAttribute 描述单个容器的 CPU 资源需求，
// 供 makeWindowsCPUManagerPod / makeWindowsCPUManagerInitPod 使用。
type windowsCtnAttribute struct {
	name          string
	cpuRequest    string
	cpuLimit      string
	restartPolicy *v1.ContainerRestartPolicy
}

// makeWindowsCPUManagerPod 为 Windows CPU 亲和性测试构建 Pod spec。
// 容器运行 PowerShell sleep，从而无需 Linux 特有的 cgroup 挂载或 /proc 访问
// 就能保持存活。
func makeWindowsCPUManagerPod(podName string, attrs []windowsCtnAttribute) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName},
		Spec: v1.PodSpec{
			RestartPolicy: v1.RestartPolicyNever,
			Containers:    buildWindowsContainers(attrs),
			NodeSelector:  map[string]string{"kubernetes.io/os": "windows"},
		},
	}
}

// verifyHostMatchesCRI 断言 Windows 内核中 ctnName 的 job-object 亲和性
// 与 CRI 上报的 affinity 逐位一致。这证明运行时真正应用了该 mask，而不只是 containerd 记录了它。
func verifyHostMatchesCRI(ctx context.Context, criClient internalapi.RuntimeService, pod *v1.Pod, ctnName string) {
	ginkgo.GinkgoHelper()
	criAff, err := getWindowsContainerCPUAffinity(ctx, criClient, pod, ctnName)
	framework.ExpectNoError(err, "failed to fetch CRI affinity for host comparison of %q", ctnName)
	framework.ExpectNoError(
		validateHostJobAffinityProcessIsolated(pod, ctnName, criAff),
		"host job-object affinity does not match CRI-reported affinity for %q", ctnName)
}

// verifyHostMasksDisjoint 断言两个容器（同一或不同 Pod 中）的内核应用亲和性 mask 不共享任何 CPU ——
// 这是 CRI windowsAffinitiesOverlap 检查在主机侧的对应实现。
// 这是 CRI windowsAffinitiesOverlap 检查在主机侧的对应实现。
func verifyHostMasksDisjoint(pod1 *v1.Pod, ctn1 string, pod2 *v1.Pod, ctn2 string) {
	ginkgo.GinkgoHelper()
	h1, err := getHostJobAffinity(pod1, ctn1)
	framework.ExpectNoError(err, "failed to read host job affinity for %q", ctn1)
	h2, err := getHostJobAffinity(pod2, ctn2)
	framework.ExpectNoError(err, "failed to read host job affinity for %q", ctn2)
	gomega.Expect(hostJobAffinitiesOverlap(h1, h2)).To(gomega.BeFalse(),
		"containers %q and %q must not share exclusively-allocated CPUs at the kernel level", ctn1, ctn2)
}

// makeWindowsCPUManagerInitPod 构建带 init 容器（可能 restartable / sidecar）、
// 后跟普通 app 容器的 Pod spec。
func makeWindowsCPUManagerInitPod(podName string, initAttrs []windowsCtnAttribute, appAttr windowsCtnAttribute) *v1.Pod {
	initContainers := buildWindowsContainers(initAttrs)
	for i := range initContainers {
		if initAttrs[i].restartPolicy != nil {
			initContainers[i].RestartPolicy = initAttrs[i].restartPolicy
		} else {
			// 不可重启的 init 容器很快退出。
			initContainers[i].Command = []string{"powershell.exe", "-Command", "Start-Sleep -Seconds 2"}
		}
	}
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName},
		Spec: v1.PodSpec{
			RestartPolicy:  v1.RestartPolicyNever,
			InitContainers: initContainers,
			Containers:     buildWindowsContainers([]windowsCtnAttribute{appAttr}),
			NodeSelector:   map[string]string{"kubernetes.io/os": "windows"},
		},
	}
}

// buildWindowsContainers 把 windowsCtnAttribute 切片转换为适合 Windows 节点的 v1.Container
// 对象（PowerShell 命令，无 Linux 卷挂载）。
func buildWindowsContainers(attrs []windowsCtnAttribute) []v1.Container {
	var containers []v1.Container
	for _, attr := range attrs {
		requests := v1.ResourceList{
			v1.ResourceMemory: resource.MustParse("128Mi"),
		}
		if attr.cpuRequest != "" {
			requests[v1.ResourceCPU] = resource.MustParse(attr.cpuRequest)
		}
		limits := v1.ResourceList{
			v1.ResourceMemory: resource.MustParse("128Mi"),
		}
		if attr.cpuLimit != "" {
			limits[v1.ResourceCPU] = resource.MustParse(attr.cpuLimit)
		}
		containers = append(containers, v1.Container{
			Name:  attr.name,
			Image: busyboxImage,
			Resources: v1.ResourceRequirements{
				Requests: requests,
				Limits:   limits,
			},
			// 长时间 sleep；powershell.exe 在 Windows BusyBox 镜像中可用。
			Command: []string{"powershell.exe", "-Command", "Start-Sleep -Seconds 86400"},
		})
	}
	return containers
}

// -------------------------------------------------------------------------
// Kubelet 配置 helper
// -------------------------------------------------------------------------

// buildWindowsCPUManagerKubeletConfig 返回使用 static CPU manager 策略的 KubeletConfiguration。
// featureGateOn 为 true 时，还会启用 WindowsCPUAndMemoryAffinity feature gate。
// WindowsCPUAndMemoryAffinity feature gate 也会启用。
func buildWindowsCPUManagerKubeletConfig(oldCfg *kubeletconfig.KubeletConfiguration, featureGateOn bool) *kubeletconfig.KubeletConfiguration {
	newCfg := oldCfg.DeepCopy()
	if newCfg.FeatureGates == nil {
		newCfg.FeatureGates = make(map[string]bool)
	}
	newCfg.FeatureGates[string(features.WindowsCPUAndMemoryAffinity)] = featureGateOn
	newCfg.CPUManagerPolicy = string(cpumanager.PolicyStatic)
	newCfg.CPUManagerReconcilePeriod = metav1.Duration{Duration: 1 * time.Second}
	// 预留 CPU 0，让剩余 CPU 可供 guaranteed Pod 使用。
	newCfg.ReservedSystemCPUs = "0"
	return newCfg
}

// buildWindowsStrictCPUReservationConfig 返回启用 strict-cpu-reservation 策略选项的 static CPU manager 配置
// （feature gate 开启），该选项会从共享池中移除预留 CPU。CPUManagerPolicyOptions 已是 GA
// 且默认锁定开启，因此只需设置 option map。
// 且默认锁定开启，因此只需设置 option map。
func buildWindowsStrictCPUReservationConfig(oldCfg *kubeletconfig.KubeletConfiguration) *kubeletconfig.KubeletConfiguration {
	newCfg := buildWindowsCPUManagerKubeletConfig(oldCfg, true)
	newCfg.CPUManagerPolicyOptions = map[string]string{
		cpumanager.StrictCPUReservationOption: "true",
	}
	return newCfg
}

// updateWindowsKubeletConfig 停止 kubelet Windows 服务，写入新配置文件，并重启服务。
// 并重启服务。
func updateWindowsKubeletConfig(ctx context.Context, f *framework.Framework, cfg *kubeletconfig.KubeletConfiguration) {
	ginkgo.GinkgoHelper()
	kubeletStart := mustStopKubelet(ctx, f)
	framework.ExpectNoError(e2enodekubelet.WriteKubeletConfigFile(cfg), "failed to write kubelet config file")
	kubeletStart(ctx)
}

// updateWindowsKubeletConfigClearState 类似 updateWindowsKubeletConfig，但还会在 kubelet 停止时
// 删除 CPU/memory manager 状态文件。
// 当新配置使持久化 checkpoint 失效时（例如切换 strict-cpu-reservation，这会改变预留 CPU
// 是否属于默认池），必须这样做，否则 kubelet 会拒绝启动并报
// "invalid state, please drain node and remove policy state file"。
// 它对应 Linux 的 updateKubeletConfig(..., deleteStateFiles=true)。
func updateWindowsKubeletConfigClearState(ctx context.Context, f *framework.Framework, cfg *kubeletconfig.KubeletConfiguration) {
	ginkgo.GinkgoHelper()
	kubeletStart := mustStopKubelet(ctx, f)
	deleteStateFile(cpuManagerStateFile)
	deleteStateFile(memoryManagerStateFile)
	framework.ExpectNoError(e2enodekubelet.WriteKubeletConfigFile(cfg), "failed to write kubelet config file")
	kubeletStart(ctx)
}

// updateWindowsKubeletConfigIfNeeded 只在期望配置与当前运行配置不同时才调用 updateWindowsKubeletConfig，
// 避免同一 When 块内测试之间不必要的 kubelet 重启。
func updateWindowsKubeletConfigIfNeeded(ctx context.Context, f *framework.Framework, desired *kubeletconfig.KubeletConfiguration) {
	ginkgo.GinkgoHelper()
	current, err := getCurrentKubeletConfig(ctx)
	framework.ExpectNoError(err, "failed to get current kubelet config")
	if equalKubeletConfiguration(current, desired) {
		framework.Logf("kubelet configuration already matches desired state, skipping restart")
		return
	}
	updateWindowsKubeletConfig(ctx, f, desired)
}

// 两个配置语义相等时 equalKubeletConfiguration 返回 true
// （忽略对比较无意义的 TypeMeta）。
func equalKubeletConfiguration(a, b *kubeletconfig.KubeletConfiguration) bool {
	a = a.DeepCopy()
	b = b.DeepCopy()
	a.TypeMeta = metav1.TypeMeta{}
	b.TypeMeta = metav1.TypeMeta{}
	return reflect.DeepEqual(a, b)
}

// -------------------------------------------------------------------------
// 跳过 / 节点 helper
// -------------------------------------------------------------------------

// 节点没有足够的整数可分配 CPU 满足测试时，
// skipIfAllocatableCPUsLessThan 跳过当前测试。
// buildWindowsCPUManagerKubeletConfig 总是预留 1 个 CPU（CPU 0），
// 因此最小可分配数为 (requested + 1)。
func skipIfAllocatableCPUsLessThan(node *v1.Node, requested int) {
	ginkgo.GinkgoHelper()
	allocatable := node.Status.Allocatable[v1.ResourceCPU]
	need := int64(requested + 1) // +1 for the reserved CPU
	if allocatable.Value() < need {
		ginkgo.Skip(fmt.Sprintf(
			"skipping: node has %d allocatable CPUs but test needs %d (including 1 reserved)",
			allocatable.Value(), need))
	}
}
