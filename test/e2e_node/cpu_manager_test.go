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

package e2enode

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gcustom"
	"github.com/onsi/gomega/types"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	admissionapi "k8s.io/pod-security-admission/api"
	"k8s.io/utils/cpuset"

	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	imageutils "k8s.io/kubernetes/test/utils/image"
)

const (
	defaultCFSPeriod = "100000"
)

// 这很难看，但实用
var (
	e2enodeRuntimeName     string
	e2enodeCgroupV2Enabled bool
	e2enodeCgroupDriver    string
)

/*
   - Serial:
   because the test updates kubelet configuration.

   - Ordered:
   Each spec (It block) need to run with a kubelet configuration in place. At minimum, we need
   the non-default cpumanager static policy, then we have the cpumanager options and so forth.
   The simplest solution is to set the kubelet explicitly each time, but this will cause a kubelet restart
   each time, which takes longer and makes the flow intrinsically more fragile (so more flakes are more likely).
   Using Ordered allows us to use BeforeAll/AfterAll, and most notably to reuse the kubelet config in a batch
   of specs (It blocks). Each it block will still set its kubelet config preconditions, but with a sensible
   test arrangement, many of these preconditions will devolve into noop.
   Arguably, this decision increases the coupling among specs, leaving room for subtle ordering bugs.
   There's no argue the ginkgo spec randomization would help, but the tradeoff here is between
   lane complexity/fragility (reconfiguring the kubelet is not bulletproof yet) and accepting this risk.
   If in the future we decide to pivot to make each spec fully independent, little changes will be needed.
   Finally, worth pointing out that the previous cpumanager e2e test incarnation implemented the same
   concept in a more convoluted way with function helpers, so arguably using Ordered and making it
   explicit is already an improvement.
*/

/*
 * 扩展 cpumanager 测试套件
 * TL;DR：最重要的：
 *
 * 从 SIGDescribe 根节点起嵌套超过 2 层 context 很可能就是坏味道。
 * 深层嵌套的问题在于 BeforeEach 块之间的相互作用和变量作用域变大。
 * 理想的布局是
 *   SIGDescribe # 顶层，唯一
 *     Context   # 可以加更多 context，但请尽量避免嵌套
 *       It      # 自由添加 It 块。可按需新建*非嵌套*的 context
 *     Context
 *       It
 *       It
 * 例外：如果要给不少（比如 3 个以上）It 块加相同的 label，可以加一个**空** context
 * 最重要的是避免长长的 beforeeach/aftereach 链和超过 2 层的 context 嵌套。
 * 所以只用**空** context 来分组 label 是可以接受的：
 *   SIGDescribe
 *     Context(label1, label2) # 这里不要放 beforeeach/aftereach 和变量
 *       Context
 *         It
 *         It
 *     Context
 *       It
 * 最后的经验法则：如果 context 描述的嵌套读起来别扭、可笑，或者作为英语句子不再通顺，
 * 那多半嵌套太深了。
 */

/*
 * 关于预留 CPU：
 * 所有测试都假定第一个可用的 CPUID 是 0，这相当合理且多数时候正确。
 * 更好的办法是检查节点上实际有什么。这与其他改进一起推迟到后续阶段。
 *
 */
var _ = SIGDescribe("CPU Manager", ginkgo.Ordered, ginkgo.ContinueOnFailure, framework.WithSerial(), feature.CPUManager, func() {
	f := framework.NewDefaultFramework("cpumanager-test")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	// 上下文开始前的原始 kubeletconfig，稍后要恢复
	var oldCfg *kubeletconfig.KubeletConfiguration
	var reservedCPUs cpuset.CPUSet
	var onlineCPUs cpuset.CPUSet
	var smtLevel int
	var uncoreGroupSize int
	// 跟踪某个 It() 块创建的所有 Pod。最好每个 It 块一个命名空间
	// TODO: 改成每个 It 块一个命名空间？
	var podMap map[string]*v1.Pod

	// 用闭包只是为了不把 `f` 和 `onlineCPUs` 尴尬地到处传，它们只用于日志
	var skipIfAllocatableCPUsLessThan func(node *v1.Node, cpuReq int)

	var createPodSync func(ctx context.Context, pod *v1.Pod) *v1.Pod

	ginkgo.BeforeAll(func(ctx context.Context) {
		var err error
		oldCfg, err = getCurrentKubeletConfig(ctx)
		framework.ExpectNoError(err)

		onlineCPUs, err = getOnlineCPUs() // this should not change at all, at least during this suite lifetime
		framework.ExpectNoError(err)
		framework.Logf("Online CPUs: %s", onlineCPUs)

		smtLevel = smtLevelFromSysFS() // this should not change at all, at least during this suite lifetime
		framework.Logf("SMT level: %d", smtLevel)

		uncoreGroupSize = getUncoreCPUGroupSize()
		framework.Logf("Uncore Group Size: %d", uncoreGroupSize)

		e2enodeCgroupV2Enabled = IsCgroup2UnifiedMode()
		framework.Logf("cgroup V2 enabled: %v", e2enodeCgroupV2Enabled)

		e2enodeCgroupDriver = oldCfg.CgroupDriver
		framework.Logf("cgroup driver: %s", e2enodeCgroupDriver)

		runtime, _, err := getCRIClient(ctx)
		framework.ExpectNoError(err, "Failed to get CRI client")

		version, err := runtime.Version(ctx, "")
		framework.ExpectNoError(err, "Failed to get runtime version")

		e2enodeRuntimeName = version.GetRuntimeName()
		framework.Logf("runtime: %s", e2enodeRuntimeName)
	})

	ginkgo.AfterAll(func(ctx context.Context) {
		updateKubeletConfig(ctx, f, oldCfg, true)
	})

	ginkgo.BeforeEach(func(ctx context.Context) {
		// 注意有意不设置 reservedCPUs —— 必须逐个测试初始化
		podMap = make(map[string]*v1.Pod)
		createPodSync = func(ctx context.Context, pod *v1.Pod) *v1.Pod {
			newPod := e2epod.NewPodClient(f).CreateSync(ctx, pod)
			podMap[string(newPod.UID)] = newPod
			return newPod
		}
	})

	ginkgo.JustBeforeEach(func(ctx context.Context) {
		// 注意有意不设置 reservedCPUs —— 必须逐个测试初始化

		// 用闭包减少参数，让用法更直观
		skipIfAllocatableCPUsLessThan = func(node *v1.Node, val int) {
			ginkgo.GinkgoHelper()
			checkAllocatableCPUs(node, val, reservedCPUs, onlineCPUs, smtLevel)
		}
	})

	ginkgo.AfterEach(func(ctx context.Context) {
		deletePodsAsync(ctx, f, podMap)
	})

	ginkgo.When("running non-guaranteed pods tests", ginkgo.Label("non-guaranteed", "reserved-cpus"), func() {
		ginkgo.It("should let the container access all the online CPUs without a reserved CPUs set", func(ctx context.Context) {
			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:         string(cpumanager.PolicyStatic),
				reservedSystemCPUs: cpuset.New(),
			}))

			pod := makeCPUManagerPod("non-gu-pod", []ctnAttribute{
				{
					ctnName:    "non-gu-container",
					cpuRequest: "100m",
					cpuLimit:   "200m",
				},
			})
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the expected cpuset was assigned")

			gomega.Expect(pod).To(HaveContainerCPUsEqualTo("non-gu-container", onlineCPUs))
		})

		ginkgo.It("should let the container access all the online CPUs when using a reserved CPUs set", func(ctx context.Context) {
			reservedCPUs = cpuset.New(0)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:         string(cpumanager.PolicyStatic),
				reservedSystemCPUs: reservedCPUs, // Not really needed for the tests but helps to make a more precise check
			}))

			pod := makeCPUManagerPod("non-gu-pod", []ctnAttribute{
				{
					ctnName:    "non-gu-container",
					cpuRequest: "100m",
					cpuLimit:   "200m",
				},
			})
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the expected cpuset was assigned")

			gomega.Expect(pod).To(HaveContainerCPUsEqualTo("non-gu-container", onlineCPUs))
		})

		ginkgo.It("should let the container access all the online non-exclusively-allocated CPUs when using a reserved CPUs set", ginkgo.Label("guaranteed", "exclusive-cpus"), func(ctx context.Context) {
			cpuCount := 1
			reservedCPUs = cpuset.New(0)

			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuCount+1) // 注意为非 guaranteed Pod 额外加的 1 个

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:         string(cpumanager.PolicyStatic),
				reservedSystemCPUs: reservedCPUs, // Not really needed for the tests but helps to make a more precise check
			}))

			podGu := makeCPUManagerPod("gu-pod", []ctnAttribute{
				{
					ctnName:    "gu-container",
					cpuRequest: fmt.Sprintf("%dm", 1000*cpuCount),
					cpuLimit:   fmt.Sprintf("%dm", 1000*cpuCount),
				},
			})
			ginkgo.By("creating the guaranteed test pod")
			podGu = createPodSync(ctx, podGu)

			podBu := makeCPUManagerPod("non-gu-pod", []ctnAttribute{
				{
					ctnName:    "non-gu-container",
					cpuRequest: "200m",
					cpuLimit:   "300m",
				},
			})
			ginkgo.By("creating the burstable test pod")
			podBu = createPodSync(ctx, podBu)

			ginkgo.By("checking if the expected cpuset was assigned")

			// 我们既不能也不应预测容器会得到哪些 CPU
			gomega.Expect(podGu).To(HaveContainerCPUsCount("gu-container", cpuCount))
			gomega.Expect(podGu).To(HaveContainerCPUsASubsetOf("gu-container", onlineCPUs))
			gomega.Expect(podGu).ToNot(HaveContainerCPUsOverlapWith("gu-container", reservedCPUs))

			exclusiveCPUs, err := getContainerAllowedCPUs(podGu, "gu-container", false)
			framework.ExpectNoError(err, "cannot get exclusive CPUs for pod %s/%s", podGu.Namespace, podGu.Name)
			expectedSharedCPUs := onlineCPUs.Difference(exclusiveCPUs)
			gomega.Expect(podBu).To(HaveContainerCPUsEqualTo("non-gu-container", expectedSharedCPUs))
		})
	})

	ginkgo.When("running guaranteed pod tests", ginkgo.Label("guaranteed", "exclusive-cpus"), func() {
		ginkgo.BeforeEach(func(ctx context.Context) {
			reservedCPUs = cpuset.New(0)
		})

		ginkgo.It("should allocate exclusively a CPU to a 1-container pod", func(ctx context.Context) {
			cpuCount := 1

			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuCount)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:         string(cpumanager.PolicyStatic),
				reservedSystemCPUs: reservedCPUs, // Not really needed for the tests but helps to make a more precise check
			}))

			pod := makeCPUManagerPod("gu-pod", []ctnAttribute{
				{
					ctnName:    "gu-container",
					cpuRequest: fmt.Sprintf("%dm", 1000*cpuCount),
					cpuLimit:   fmt.Sprintf("%dm", 1000*cpuCount),
				},
			})
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the expected cpuset was assigned")

			// 我们既不能也不应预测容器会得到哪些 CPU
			gomega.Expect(pod).To(HaveContainerCPUsCount("gu-container", cpuCount))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("gu-container", onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("gu-container", reservedCPUs))
		})

		// we don't use a separate group (gingo.When) with BeforeEach to factor out the tests because each
		// test need to check for the amount of CPUs it needs.

		ginkgo.It("should allocate exclusively a even number of CPUs to a 1-container pod", func(ctx context.Context) {
			cpuCount := 2

			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuCount)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:         string(cpumanager.PolicyStatic),
				reservedSystemCPUs: reservedCPUs, // Not really needed for the tests but helps to make a more precise check
			}))

			pod := makeCPUManagerPod("gu-pod", []ctnAttribute{
				{
					ctnName:    "gu-container",
					cpuRequest: fmt.Sprintf("%dm", 1000*cpuCount),
					cpuLimit:   fmt.Sprintf("%dm", 1000*cpuCount),
				},
			})
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the expected cpuset was assigned")

			// 我们既不能也不应预测容器会得到哪些 CPU
			gomega.Expect(pod).To(HaveContainerCPUsCount("gu-container", cpuCount))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("gu-container", onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("gu-container", reservedCPUs))
			// TODO: this is probably too strict but it is the closest of the old test did
			gomega.Expect(pod).To(HaveContainerCPUsThreadSiblings("gu-container"))
		})

		ginkgo.It("should allocate exclusively a odd number of CPUs to a 1-container pod", func(ctx context.Context) {
			cpuCount := 3

			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuCount)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:         string(cpumanager.PolicyStatic),
				reservedSystemCPUs: reservedCPUs, // Not really needed for the tests but helps to make a more precise check
			}))

			pod := makeCPUManagerPod("gu-pod", []ctnAttribute{
				{
					ctnName:    "gu-container",
					cpuRequest: fmt.Sprintf("%dm", 1000*cpuCount),
					cpuLimit:   fmt.Sprintf("%dm", 1000*cpuCount),
				},
			})
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the expected cpuset was assigned")

			// 我们既不能也不应预测容器会得到哪些 CPU
			gomega.Expect(pod).To(HaveContainerCPUsCount("gu-container", cpuCount))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("gu-container", onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("gu-container", reservedCPUs))
			// TODO: this is probably too strict but it is the closest of the old test did
			toleration := 1
			gomega.Expect(pod).To(HaveContainerCPUsQuasiThreadSiblings("gu-container", toleration))
		})

		ginkgo.It("should allocate exclusively CPUs to a multi-container pod (1+2)", func(ctx context.Context) {
			cpuCount := 3 // 总数

			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuCount)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:         string(cpumanager.PolicyStatic),
				reservedSystemCPUs: reservedCPUs, // Not really needed for the tests but helps to make a more precise check
			}))

			pod := makeCPUManagerPod("gu-pod", []ctnAttribute{
				{
					ctnName:    "gu-container-1",
					cpuRequest: "1000m",
					cpuLimit:   "1000m",
				},
				{
					ctnName:    "gu-container-2",
					cpuRequest: "2000m",
					cpuLimit:   "2000m",
				},
			})
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the expected cpuset was assigned")

			// 我们既不能也不应预测容器会得到哪些 CPU
			gomega.Expect(pod).To(HaveContainerCPUsCount("gu-container-1", 1))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("gu-container-1", onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("gu-container-1", reservedCPUs))

			gomega.Expect(pod).To(HaveContainerCPUsCount("gu-container-2", 2))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("gu-container-2", onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("gu-container-2", reservedCPUs))
			// TODO: this is probably too strict but it is the closest of the old test did
			gomega.Expect(pod).To(HaveContainerCPUsThreadSiblings("gu-container-2"))
		})

		ginkgo.It("should allocate exclusively CPUs to a multi-container pod (3+2)", func(ctx context.Context) {
			cpuCount := 5 // 总数

			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuCount)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:         string(cpumanager.PolicyStatic),
				reservedSystemCPUs: reservedCPUs, // Not really needed for the tests but helps to make a more precise check
			}))

			pod := makeCPUManagerPod("gu-pod", []ctnAttribute{
				{
					ctnName:    "gu-container-1",
					cpuRequest: "3000m",
					cpuLimit:   "3000m",
				},
				{
					ctnName:    "gu-container-2",
					cpuRequest: "2000m",
					cpuLimit:   "2000m",
				},
			})
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the expected cpuset was assigned")

			// 我们既不能也不应预测容器会得到哪些 CPU
			gomega.Expect(pod).To(HaveContainerCPUsCount("gu-container-1", 3))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("gu-container-1", onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("gu-container-1", reservedCPUs))
			toleration := 1
			gomega.Expect(pod).To(HaveContainerCPUsQuasiThreadSiblings("gu-container-1", toleration))

			gomega.Expect(pod).To(HaveContainerCPUsCount("gu-container-2", 2))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("gu-container-2", onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("gu-container-2", reservedCPUs))
			gomega.Expect(pod).To(HaveContainerCPUsThreadSiblings("gu-container-2"))
		})

		ginkgo.It("should allocate exclusively CPUs to a multi-container pod (4+2)", func(ctx context.Context) {
			cpuCount := 6 // 总数

			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuCount)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:         string(cpumanager.PolicyStatic),
				reservedSystemCPUs: reservedCPUs, // Not really needed for the tests but helps to make a more precise check
			}))

			pod := makeCPUManagerPod("gu-pod", []ctnAttribute{
				{
					ctnName:    "gu-container-1",
					cpuRequest: "4000m",
					cpuLimit:   "4000m",
				},
				{
					ctnName:    "gu-container-2",
					cpuRequest: "2000m",
					cpuLimit:   "2000m",
				},
			})
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the expected cpuset was assigned")

			// 我们既不能也不应预测容器会得到哪些 CPU
			gomega.Expect(pod).To(HaveContainerCPUsCount("gu-container-1", 4))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("gu-container-1", onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("gu-container-1", reservedCPUs))
			gomega.Expect(pod).To(HaveContainerCPUsThreadSiblings("gu-container-1"))

			gomega.Expect(pod).To(HaveContainerCPUsCount("gu-container-2", 2))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("gu-container-2", onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("gu-container-2", reservedCPUs))
			gomega.Expect(pod).To(HaveContainerCPUsThreadSiblings("gu-container-2"))
		})

		ginkgo.It("should allocate exclusively a CPU to multiple 1-container pods", func(ctx context.Context) {
			cpuCount := 4 // 总数

			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuCount)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:         string(cpumanager.PolicyStatic),
				reservedSystemCPUs: reservedCPUs, // Not really needed for the tests but helps to make a more precise check
			}))

			pod1 := makeCPUManagerPod("gu-pod-1", []ctnAttribute{
				{
					ctnName:    "gu-container-1",
					cpuRequest: "2000m",
					cpuLimit:   "2000m",
				},
			})
			ginkgo.By("creating the test pod 1")
			pod1 = createPodSync(ctx, pod1)

			pod2 := makeCPUManagerPod("gu-pod-2", []ctnAttribute{
				{
					ctnName:    "gu-container-2",
					cpuRequest: "2000m",
					cpuLimit:   "2000m",
				},
			})
			ginkgo.By("creating the test pod 2")
			pod2 = createPodSync(ctx, pod2)

			ginkgo.By("checking if the expected cpuset was assigned")

			// 我们既不能也不应预测容器会得到哪些 CPU
			gomega.Expect(pod1).To(HaveContainerCPUsCount("gu-container-1", 2))
			gomega.Expect(pod1).To(HaveContainerCPUsASubsetOf("gu-container-1", onlineCPUs))
			gomega.Expect(pod1).ToNot(HaveContainerCPUsOverlapWith("gu-container-1", reservedCPUs))
			gomega.Expect(pod1).To(HaveContainerCPUsThreadSiblings("gu-container-1"))

			gomega.Expect(pod2).To(HaveContainerCPUsCount("gu-container-2", 2))
			gomega.Expect(pod2).To(HaveContainerCPUsASubsetOf("gu-container-2", onlineCPUs))
			gomega.Expect(pod2).ToNot(HaveContainerCPUsOverlapWith("gu-container-2", reservedCPUs))
			gomega.Expect(pod2).To(HaveContainerCPUsThreadSiblings("gu-container-2"))
		})
	})

	ginkgo.When("running guaranteed pod tests with feature gates disabled", ginkgo.Label("guaranteed", "exclusive-cpus", "feature-gate-disabled"), func() {
		ginkgo.BeforeEach(func(ctx context.Context) {
			reservedCPUs = cpuset.New(0)
		})

		ginkgo.It("should allocate exclusively a CPU to a 1-container pod", func(ctx context.Context) {
			cpuCount := 1

			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuCount)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:              string(cpumanager.PolicyStatic),
				reservedSystemCPUs:      reservedCPUs, // Not really needed for the tests but helps to make a more precise check
				enableCPUManagerOptions: false,
			}))

			pod := makeCPUManagerPod("gu-pod", []ctnAttribute{
				{
					ctnName:    "gu-container",
					cpuRequest: fmt.Sprintf("%dm", 1000*cpuCount),
					cpuLimit:   fmt.Sprintf("%dm", 1000*cpuCount),
				},
			})
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the expected cpuset was assigned")

			// 我们既不能也不应预测容器会得到哪些 CPU
			gomega.Expect(pod).To(HaveContainerCPUsCount("gu-container", cpuCount))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("gu-container", onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("gu-container", reservedCPUs))
		})

		// we don't use a separate group (gingo.When) with BeforeEach to factor out the tests because each
		// test need to check for the amount of CPUs it needs.

		ginkgo.It("should allocate exclusively a even number of CPUs to a 1-container pod", func(ctx context.Context) {
			cpuCount := 2

			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuCount)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:              string(cpumanager.PolicyStatic),
				reservedSystemCPUs:      reservedCPUs, // Not really needed for the tests but helps to make a more precise check
				enableCPUManagerOptions: false,
			}))

			pod := makeCPUManagerPod("gu-pod", []ctnAttribute{
				{
					ctnName:    "gu-container",
					cpuRequest: fmt.Sprintf("%dm", 1000*cpuCount),
					cpuLimit:   fmt.Sprintf("%dm", 1000*cpuCount),
				},
			})
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the expected cpuset was assigned")

			// 我们既不能也不应预测容器会得到哪些 CPU
			gomega.Expect(pod).To(HaveContainerCPUsCount("gu-container", cpuCount))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("gu-container", onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("gu-container", reservedCPUs))
			// TODO: this is probably too strict but it is the closest of the old test did
			gomega.Expect(pod).To(HaveContainerCPUsThreadSiblings("gu-container"))
		})

		ginkgo.It("should allocate exclusively a odd number of CPUs to a 1-container pod", func(ctx context.Context) {
			cpuCount := 3

			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuCount)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:              string(cpumanager.PolicyStatic),
				reservedSystemCPUs:      reservedCPUs, // Not really needed for the tests but helps to make a more precise check
				enableCPUManagerOptions: false,
			}))

			pod := makeCPUManagerPod("gu-pod", []ctnAttribute{
				{
					ctnName:    "gu-container",
					cpuRequest: fmt.Sprintf("%dm", 1000*cpuCount),
					cpuLimit:   fmt.Sprintf("%dm", 1000*cpuCount),
				},
			})
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the expected cpuset was assigned")

			// 我们既不能也不应预测容器会得到哪些 CPU
			gomega.Expect(pod).To(HaveContainerCPUsCount("gu-container", cpuCount))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("gu-container", onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("gu-container", reservedCPUs))
			// TODO: this is probably too strict but it is the closest of the old test did
			toleration := 1
			gomega.Expect(pod).To(HaveContainerCPUsQuasiThreadSiblings("gu-container", toleration))
		})

		ginkgo.It("should allocate exclusively CPUs to a multi-container pod (1+2)", func(ctx context.Context) {
			cpuCount := 3 // 总数

			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuCount)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:              string(cpumanager.PolicyStatic),
				reservedSystemCPUs:      reservedCPUs, // Not really needed for the tests but helps to make a more precise check
				enableCPUManagerOptions: false,
			}))

			pod := makeCPUManagerPod("gu-pod", []ctnAttribute{
				{
					ctnName:    "gu-container-1",
					cpuRequest: "1000m",
					cpuLimit:   "1000m",
				},
				{
					ctnName:    "gu-container-2",
					cpuRequest: "2000m",
					cpuLimit:   "2000m",
				},
			})
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the expected cpuset was assigned")

			// 我们既不能也不应预测容器会得到哪些 CPU
			gomega.Expect(pod).To(HaveContainerCPUsCount("gu-container-1", 1))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("gu-container-1", onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("gu-container-1", reservedCPUs))

			gomega.Expect(pod).To(HaveContainerCPUsCount("gu-container-2", 2))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("gu-container-2", onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("gu-container-2", reservedCPUs))
			// TODO: this is probably too strict but it is the closest of the old test did
			gomega.Expect(pod).To(HaveContainerCPUsThreadSiblings("gu-container-2"))
		})

		ginkgo.It("should allocate exclusively CPUs to a multi-container pod (3+2)", func(ctx context.Context) {
			cpuCount := 5 // 总数

			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuCount)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:              string(cpumanager.PolicyStatic),
				reservedSystemCPUs:      reservedCPUs, // Not really needed for the tests but helps to make a more precise check
				enableCPUManagerOptions: false,
			}))

			pod := makeCPUManagerPod("gu-pod", []ctnAttribute{
				{
					ctnName:    "gu-container-1",
					cpuRequest: "3000m",
					cpuLimit:   "3000m",
				},
				{
					ctnName:    "gu-container-2",
					cpuRequest: "2000m",
					cpuLimit:   "2000m",
				},
			})
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the expected cpuset was assigned")

			// 我们既不能也不应预测容器会得到哪些 CPU
			gomega.Expect(pod).To(HaveContainerCPUsCount("gu-container-1", 3))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("gu-container-1", onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("gu-container-1", reservedCPUs))
			toleration := 1
			gomega.Expect(pod).To(HaveContainerCPUsQuasiThreadSiblings("gu-container-1", toleration))

			gomega.Expect(pod).To(HaveContainerCPUsCount("gu-container-2", 2))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("gu-container-2", onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("gu-container-2", reservedCPUs))
			gomega.Expect(pod).To(HaveContainerCPUsThreadSiblings("gu-container-2"))
		})

		ginkgo.It("should allocate exclusively CPUs to a multi-container pod (4+2)", func(ctx context.Context) {
			cpuCount := 6 // 总数

			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuCount)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:              string(cpumanager.PolicyStatic),
				reservedSystemCPUs:      reservedCPUs, // Not really needed for the tests but helps to make a more precise check
				enableCPUManagerOptions: false,
			}))

			pod := makeCPUManagerPod("gu-pod", []ctnAttribute{
				{
					ctnName:    "gu-container-1",
					cpuRequest: "4000m",
					cpuLimit:   "4000m",
				},
				{
					ctnName:    "gu-container-2",
					cpuRequest: "2000m",
					cpuLimit:   "2000m",
				},
			})
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the expected cpuset was assigned")

			// 我们既不能也不应预测容器会得到哪些 CPU
			gomega.Expect(pod).To(HaveContainerCPUsCount("gu-container-1", 4))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("gu-container-1", onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("gu-container-1", reservedCPUs))
			gomega.Expect(pod).To(HaveContainerCPUsThreadSiblings("gu-container-1"))

			gomega.Expect(pod).To(HaveContainerCPUsCount("gu-container-2", 2))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("gu-container-2", onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("gu-container-2", reservedCPUs))
			gomega.Expect(pod).To(HaveContainerCPUsThreadSiblings("gu-container-2"))
		})

		ginkgo.It("should allocate exclusively a CPU to multiple 1-container pods", func(ctx context.Context) {
			cpuCount := 4 // 总数

			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuCount)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:              string(cpumanager.PolicyStatic),
				reservedSystemCPUs:      reservedCPUs, // Not really needed for the tests but helps to make a more precise check
				enableCPUManagerOptions: false,
			}))

			pod1 := makeCPUManagerPod("gu-pod-1", []ctnAttribute{
				{
					ctnName:    "gu-container-1",
					cpuRequest: "2000m",
					cpuLimit:   "2000m",
				},
			})
			ginkgo.By("creating the test pod 1")
			pod1 = createPodSync(ctx, pod1)

			pod2 := makeCPUManagerPod("gu-pod-2", []ctnAttribute{
				{
					ctnName:    "gu-container-2",
					cpuRequest: "2000m",
					cpuLimit:   "2000m",
				},
			})
			ginkgo.By("creating the test pod 2")
			pod2 = createPodSync(ctx, pod2)

			ginkgo.By("checking if the expected cpuset was assigned")

			// 我们既不能也不应预测容器会得到哪些 CPU
			gomega.Expect(pod1).To(HaveContainerCPUsCount("gu-container-1", 2))
			gomega.Expect(pod1).To(HaveContainerCPUsASubsetOf("gu-container-1", onlineCPUs))
			gomega.Expect(pod1).ToNot(HaveContainerCPUsOverlapWith("gu-container-1", reservedCPUs))
			gomega.Expect(pod1).To(HaveContainerCPUsThreadSiblings("gu-container-1"))

			gomega.Expect(pod2).To(HaveContainerCPUsCount("gu-container-2", 2))
			gomega.Expect(pod2).To(HaveContainerCPUsASubsetOf("gu-container-2", onlineCPUs))
			gomega.Expect(pod2).ToNot(HaveContainerCPUsOverlapWith("gu-container-2", reservedCPUs))
			gomega.Expect(pod2).To(HaveContainerCPUsThreadSiblings("gu-container-2"))
		})
	})

	ginkgo.When("running with strict CPU reservation", ginkgo.Label("strict-cpu-reservation"), func() {
		ginkgo.BeforeEach(func(ctx context.Context) {
			reservedCPUs = cpuset.New(0)
		})

		ginkgo.It("should let the container access all the online CPUs without a reserved CPUs set", func(ctx context.Context) {
			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:              string(cpumanager.PolicyStatic),
				reservedSystemCPUs:      cpuset.New(),
				enableCPUManagerOptions: true,
				options: map[string]string{
					cpumanager.StrictCPUReservationOption: "true",
				},
			}))

			pod := makeCPUManagerPod("non-gu-pod", []ctnAttribute{
				{
					ctnName:    "non-gu-container",
					cpuRequest: "100m",
					cpuLimit:   "200m",
				},
			})
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the expected cpuset was assigned")

			// cpumanager 总会至少预留 1 个 CPU。这里不指定是哪个；如果把 cpumanager
			// 当作黑盒（我们本就应该如此），就无法预测是哪一个。因此只能断言有*某个* CPU
			// 因被预留而不可用。
			gomega.Expect(pod).To(HaveContainerCPUsCount("non-gu-container", onlineCPUs.Size()-1))
		})

		ginkgo.It("should let the container access all the online CPUs minus the reserved CPUs set when enabled", func(ctx context.Context) {
			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:              string(cpumanager.PolicyStatic),
				reservedSystemCPUs:      reservedCPUs, // Not really needed for the tests but helps to make a more precise check
				enableCPUManagerOptions: true,
				options: map[string]string{
					cpumanager.StrictCPUReservationOption: "true",
				},
			}))

			pod := makeCPUManagerPod("non-gu-pod", []ctnAttribute{
				{
					ctnName:    "non-gu-container",
					cpuRequest: "100m",
					cpuLimit:   "200m",
				},
			})
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the expected cpuset was assigned")

			gomega.Expect(pod).To(HaveContainerCPUsEqualTo("non-gu-container", onlineCPUs.Difference(reservedCPUs)))
		})

		ginkgo.It("should let the container access all the online non-exclusively-allocated CPUs minus the reserved CPUs set when enabled", func(ctx context.Context) {
			cpuCount := 1

			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuCount+1) // 注意为非 guaranteed Pod 额外加的 1 个

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:              string(cpumanager.PolicyStatic),
				reservedSystemCPUs:      reservedCPUs, // Not really needed for the tests but helps to make a more precise check
				enableCPUManagerOptions: true,
				options: map[string]string{
					cpumanager.StrictCPUReservationOption: "true",
				},
			}))

			cpuReq := fmt.Sprintf("%dm", 1000*cpuCount)

			podGu := makeCPUManagerPod("gu-pod", []ctnAttribute{
				{
					ctnName:    "gu-container",
					cpuRequest: cpuReq,
					cpuLimit:   cpuReq,
				},
			})
			ginkgo.By("creating the guaranteed test pod")
			podGu = createPodSync(ctx, podGu)

			podBu := makeCPUManagerPod("non-gu-pod", []ctnAttribute{
				{
					ctnName:    "non-gu-container",
					cpuRequest: "200m",
					cpuLimit:   "300m",
				},
			})
			ginkgo.By("creating the burstable test pod")
			podBu = createPodSync(ctx, podBu)

			ginkgo.By("checking if the expected cpuset was assigned")

			usableCPUs := onlineCPUs.Difference(reservedCPUs)

			// 任何整核都可以 —— 我们既不能也不应预测具体是哪个
			gomega.Expect(podGu).To(HaveContainerCPUsCount("gu-container", cpuCount))
			gomega.Expect(podGu).To(HaveContainerCPUsASubsetOf("gu-container", usableCPUs))
			gomega.Expect(podGu).ToNot(HaveContainerCPUsOverlapWith("gu-container", reservedCPUs))

			exclusiveCPUs, err := getContainerAllowedCPUs(podGu, "gu-container", false)
			framework.ExpectNoError(err, "cannot get exclusive CPUs for pod %s/%s", podGu.Namespace, podGu.Name)
			expectedSharedCPUs := usableCPUs.Difference(exclusiveCPUs)
			gomega.Expect(podBu).To(HaveContainerCPUsEqualTo("non-gu-container", expectedSharedCPUs))
		})
	})

	ginkgo.When("running with SMT Alignment", ginkgo.Label("smt-alignment"), func() {
		ginkgo.BeforeEach(func(ctx context.Context) {
			// 非 SMT 系统上严格 SMT 对齐自然满足，无需校验
			if smtLevel < minSMTLevel {
				e2eskipper.Skipf("Skipping CPU Manager %q tests since SMT disabled", cpumanager.FullPCPUsOnlyOption)
			}

			reservedCPUs = cpuset.New(0)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:              string(cpumanager.PolicyStatic),
				reservedSystemCPUs:      reservedCPUs,
				enableCPUManagerOptions: true,
				options: map[string]string{
					cpumanager.FullPCPUsOnlyOption: "true",
				},
			}))
		})

		ginkgo.It("should reject workload asking non-SMT-multiple of cpus", func(ctx context.Context) {
			cpuCount := 1
			cpuReq := fmt.Sprintf("%dm", 1000*cpuCount)

			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuCount)

			ginkgo.By("creating the testing pod")
			// negative 测试：运行请求数不是 SMT level 倍数的容器，预期被拒绝
			pod := makeCPUManagerPod("gu-pod", []ctnAttribute{
				{
					ctnName:    "gu-container-neg",
					cpuRequest: cpuReq,
					cpuLimit:   cpuReq,
				},
			})
			ginkgo.By("creating the test pod")
			// CreateSync 会等待 Pod 变为 Ready —— 如果生产代码按预期工作，这永远不会发生！
			pod = e2epod.NewPodClient(f).Create(ctx, pod)
			podMap[string(pod.UID)] = pod

			ginkgo.By("ensuring the testing pod is in failed state")
			err := e2epod.WaitForPodCondition(ctx, f.ClientSet, f.Namespace.Name, pod.Name, "Failed", 30*time.Second, func(pod *v1.Pod) (bool, error) {
				if pod.Status.Phase != v1.PodPending {
					return true, nil
				}
				return false, nil
			})
			framework.ExpectNoError(err)

			ginkgo.By("ensuring the testing pod is failed for the expected reason")
			pod, err = e2epod.NewPodClient(f).Get(ctx, pod.Name, metav1.GetOptions{})
			framework.ExpectNoError(err)
			gomega.Expect(pod).To(BeAPodInPhase(v1.PodFailed))
			gomega.Expect(pod).To(HaveStatusReasonMatchingRegex(`SMT.*Alignment.*Error`))
		})

		ginkgo.It("should admit workload asking SMT-multiple of cpus", func(ctx context.Context) {
			// positive 测试：运行请求数是 SMT level 倍数的容器，检查分配的核心
			// 1. 是同核的兄弟线程
			// 2. 占满一个物理核
			// 警告：这里假定系统是 2 路 SMT —— 我们不知道如何访问其他 SMT level。
			//       这意味着在超过 2 路的 SMT 系统上，本测试证明不了什么
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), smtLevel)

			cpuRequest := fmt.Sprintf("%d000m", smtLevel)
			ginkgo.By(fmt.Sprintf("creating the testing pod cpuRequest=%v", cpuRequest))
			pod := makeCPUManagerPod("gu-pod", []ctnAttribute{
				{
					ctnName:    "gu-container-pos",
					cpuRequest: cpuRequest,
					cpuLimit:   cpuRequest,
				},
			})
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("validating each container in the testing pod")
			for _, cnt := range pod.Spec.Containers {
				ginkgo.By(fmt.Sprintf("validating the container %s on pod %s", cnt.Name, pod.Name))

				gomega.Expect(pod).To(HaveContainerCPUsAlignedTo(cnt.Name, smtLevel))
				gomega.Expect(pod).To(HaveContainerCPUsThreadSiblings(cnt.Name))
			}
		})

		ginkgo.It("should count for pre-allocated CPUs after kubelet restart", func(ctx context.Context) {
			// 本测试校验启用 SMT 对齐时，带预分配 CPU（来自 checkpoint 文件）的 Pod
			// 在 kubelet 重启后不会被拒绝。
			// 这是对将容器存在性检查移到 SMT 对齐检查之前这一修复的回归测试。
			//

			// 关键在于请求足够多的 CPU：若不统计预分配的 CPU，
			// SMT 对齐检查就会因可用物理 CPU 不足而失败。
			//

			// 计算可请求的最大 SMT 对齐 CPU 数
			// 需要请求大部分可分配 CPU 才能触发该 bug
			nodeCPUDetails := cpuDetailsFromNode(getLocalNode(ctx, f))
			allocatableCPUs := int(nodeCPUDetails.Allocatable) - reservedCPUs.Size()

			// 向下取整到 smtLevel 的倍数，满足 SMT 对齐
			cpuCount := (allocatableCPUs / smtLevel) * smtLevel

			// 至少需要 2*smtLevel 个 CPU，测试才有意义
			//（一组给 Pod，剩下的少于 smtLevel，检查就会失败）
			if cpuCount < 2*smtLevel {
				e2eskipper.Skipf("Not enough allocatable CPUs for this test: need at least %d, have %d", 2*smtLevel, allocatableCPUs)
			}

			ginkgo.By(fmt.Sprintf("creating the testing pod cpuRequest=%d (allocatable=%d, reserved=%d)", cpuCount, allocatableCPUs, reservedCPUs.Size()))
			pod := makeCPUManagerPod("gu-pod", []ctnAttribute{
				{
					ctnName:    "gu-container",
					cpuRequest: fmt.Sprintf("%d", cpuCount),
					cpuLimit:   fmt.Sprintf("%d", cpuCount),
				},
			})
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("verifying the pod is running with allocated CPUs")
			cpusBeforeRestart, err := getContainerAllowedCPUs(pod, "gu-container", false)
			framework.ExpectNoError(err, "cannot get CPUs for container gu-container before restart")
			framework.Logf("CPUs before kubelet restart: %s (count=%d)", cpusBeforeRestart.String(), cpusBeforeRestart.Size())
			gomega.Expect(cpusBeforeRestart.Size()).To(gomega.Equal(cpuCount))

			ginkgo.By("restarting the kubelet")
			restartKubelet := mustStopKubelet(ctx, f)
			restartKubelet(ctx)

			ginkgo.By("ensuring kubelet is healthy after restart")
			gomega.Eventually(ctx, func() bool {
				return e2enode.HealthCheck(kubeletHealthCheckURL)
			}).WithTimeout(f.Timeouts.PodStart).WithPolling(f.Timeouts.Poll).Should(gomega.BeTrueBecause("kubelet should be healthy after restart"))

			ginkgo.By("verifying pod is still running after kubelet restart")
			pod, err = e2epod.NewPodClient(f).Get(ctx, pod.Name, metav1.GetOptions{})
			framework.ExpectNoError(err, "failed to get pod after kubelet restart")
			gomega.Expect(pod).To(BeAPodInPhase(v1.PodRunning), "pod should still be running after kubelet restart")

			ginkgo.By("verifying container still has the same CPUs after restart")
			gomega.Expect(pod).To(HaveContainerCPUsEqualTo("gu-container", cpusBeforeRestart))
		})
	})

	ginkgo.When("running with Uncore Cache Alignment", ginkgo.Label("prefer-align-cpus-by-uncore-cache"), func() {
		ginkgo.BeforeEach(func(ctx context.Context) {

			reservedCPUs := cpuset.New(0)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:              string(cpumanager.PolicyStatic),
				reservedSystemCPUs:      reservedCPUs,
				enableCPUManagerOptions: true,
				options: map[string]string{
					cpumanager.PreferAlignByUnCoreCacheOption: "true",
				},
			}))
		})

		ginkgo.It("should admit container asking odd integer amount of cpus", func(ctx context.Context) {
			// 假定一个 uncore cache 对应的 CPU 数总是偶数
			// 最小的奇数 CPU 请求是 1 个 CPU
			// 要有意义的测试，最小可分配 CPU 要求应为：
			// minCPUCapacity + reservedCPUs.Size() + 1 个已分配的 CPU
			cpuCount := minCPUCapacity + reservedCPUs.Size() + 1
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuCount)

			// 检查节点处理器架构是 split 还是 monolithic uncore cache。
			// prefer-align-cpus-by-uncore-cache 可在非 split uncore cache 处理器上启用，
			// 且不改变默认 static 行为
			allocatableCPUs := cpuDetailsFromNode(getLocalNode(ctx, f)).Allocatable
			hasSplitUncore := (allocatableCPUs > int64(uncoreGroupSize))

			if hasSplitUncore {
				// 创建一个请求比整块 uncore cache 少一个 CPU 的容器
				// 假定单个 uncore cache 共享的 CPU 总数总是偶数
				cpuRequest := fmt.Sprintf("%d000m", (uncoreGroupSize - 1))
				ginkgo.By(fmt.Sprintf("creating the testing pod cpuRequest=%v", cpuRequest))
				pod := makeCPUManagerPod("gu-pod", []ctnAttribute{
					{
						ctnName:    "gu-container-pos",
						cpuRequest: cpuRequest,
						cpuLimit:   cpuRequest,
					},
				})
				ginkgo.By("creating the test pod")
				pod = createPodSync(ctx, pod)

				ginkgo.By("validating each container in the testing pod")
				for _, cnt := range pod.Spec.Containers {
					ginkgo.By(fmt.Sprintf("validating the container %s on pod %s", cnt.Name, pod.Name))

					gomega.Expect(pod).To(HaveContainerCPUsWithSameUncoreCacheID(cnt.Name))
				}
			} else {
				// 对 monolithic uncore cache 处理器的节点
				// uncoreGroupSize 就是一个 socket 的 CPU 数
				// 减去 (minCPUCapacity + 1) 个 CPU 的资源约束
				cpuRequest := fmt.Sprintf("%d000m", (uncoreGroupSize - (minCPUCapacity + 1)))
				ginkgo.By(fmt.Sprintf("creating the testing pod cpuRequest=%v", cpuRequest))
				pod := makeCPUManagerPod("gu-pod", []ctnAttribute{
					{
						ctnName:    "gu-container-pos",
						cpuRequest: cpuRequest,
						cpuLimit:   cpuRequest,
					},
				})
				ginkgo.By("creating the test pod")
				pod = createPodSync(ctx, pod)

				ginkgo.By("validating each container in the testing pod")
				for _, cnt := range pod.Spec.Containers {
					ginkgo.By(fmt.Sprintf("validating the container %s on pod %s", cnt.Name, pod.Name))

					gomega.Expect(pod).To(HaveContainerCPUsWithSameUncoreCacheID(cnt.Name))
				}
			}
		})
	})

	ginkgo.When("running with Uncore Cache Alignment disabled", ginkgo.Label("prefer-align-cpus-by-uncore-cache"), func() {
		ginkgo.BeforeEach(func(ctx context.Context) {

			reservedCPUs := cpuset.New(0)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:              string(cpumanager.PolicyStatic),
				reservedSystemCPUs:      reservedCPUs,
				enableCPUManagerOptions: true,
				options: map[string]string{
					cpumanager.PreferAlignByUnCoreCacheOption: "false",
				},
			}))
		})

		ginkgo.It("should allocate exclusively CPUs to a multi-container pod (1+2)", func(ctx context.Context) {
			cpuCount := 3 // 总数

			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuCount)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:         string(cpumanager.PolicyStatic),
				reservedSystemCPUs: reservedCPUs, // Not really needed for the tests but helps to make a more precise check
			}))

			pod := makeCPUManagerPod("gu-pod", []ctnAttribute{
				{
					ctnName:    "gu-container-1",
					cpuRequest: "1000m",
					cpuLimit:   "1000m",
				},
				{
					ctnName:    "gu-container-2",
					cpuRequest: "2000m",
					cpuLimit:   "2000m",
				},
			})
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the expected cpuset was assigned")

			// 我们既不能也不应预测容器会得到哪些 CPU
			gomega.Expect(pod).To(HaveContainerCPUsCount("gu-container-1", 1))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("gu-container-1", onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("gu-container-1", reservedCPUs))

			gomega.Expect(pod).To(HaveContainerCPUsCount("gu-container-2", 2))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("gu-container-2", onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("gu-container-2", reservedCPUs))
			// TODO: this is probably too strict but it is the closest of the old test did
			gomega.Expect(pod).To(HaveContainerCPUsThreadSiblings("gu-container-2"))
		})
	})

	ginkgo.When("checking the compatibility between options", func() {
		// please avoid nesting `BeforeEach` as much as possible. Ideally avoid completely.
		ginkgo.Context("SMT Alignment and strict CPU reservation", ginkgo.Label("smt-alignment", "strict-cpu-reservation"), func() {
			ginkgo.BeforeEach(func(ctx context.Context) {
				// 非 SMT 系统上严格 SMT 对齐自然满足，无需校验
				if smtLevel < minSMTLevel {
					e2eskipper.Skipf("Skipping CPU Manager %q tests since SMT disabled", cpumanager.FullPCPUsOnlyOption)
				}
				reservedCPUs = cpuset.New(0)
			})

			ginkgo.It("should reject workload asking non-SMT-multiple of cpus", func(ctx context.Context) {
				cpuCount := 1

				skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuCount+1) // 注意为非 guaranteed Pod 额外加的 1 个

				updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
					policyName:              string(cpumanager.PolicyStatic),
					reservedSystemCPUs:      reservedCPUs,
					enableCPUManagerOptions: true,
					options: map[string]string{
						cpumanager.FullPCPUsOnlyOption:        "true",
						cpumanager.StrictCPUReservationOption: "true",
					},
				}))

				// negative 测试：运行请求数不是 SMT level 倍数的容器，预期被拒绝
				pod := makeCPUManagerPod("gu-pod", []ctnAttribute{
					{
						ctnName:    "gu-container-neg",
						cpuRequest: "1000m",
						cpuLimit:   "1000m",
					},
				})
				ginkgo.By("creating the testing pod")
				// CreateSync 会等待 Pod 变为 Ready —— 如果生产代码按预期工作，这永远不会发生！
				pod = e2epod.NewPodClient(f).Create(ctx, pod)
				podMap[string(pod.UID)] = pod

				ginkgo.By("ensuring the testing pod is in failed state")
				err := e2epod.WaitForPodCondition(ctx, f.ClientSet, f.Namespace.Name, pod.Name, "Failed", 30*time.Second, func(pod *v1.Pod) (bool, error) {
					if pod.Status.Phase != v1.PodPending {
						return true, nil
					}
					return false, nil
				})
				framework.ExpectNoError(err)

				ginkgo.By("ensuring the testing pod is failed for the expected reason")
				pod, err = e2epod.NewPodClient(f).Get(ctx, pod.Name, metav1.GetOptions{})
				framework.ExpectNoError(err)
				gomega.Expect(pod).To(BeAPodInPhase(v1.PodFailed))
				gomega.Expect(pod).To(HaveStatusReasonMatchingRegex(`SMT.*Alignment.*Error`))
			})

			ginkgo.It("should admit workload asking SMT-multiple of cpus", func(ctx context.Context) {
				// positive 测试：运行请求数是 SMT level 倍数的容器，检查分配的核心
				// 1. 是同核的兄弟线程
				// 2. 占满一个物理核
				// 警告：这里假定系统是 2 路 SMT —— 我们不知道如何访问其他 SMT level。
				//       这意味着在超过 2 路的 SMT 系统上，本测试证明不了什么
				skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), smtLevel)

				updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
					policyName:              string(cpumanager.PolicyStatic),
					reservedSystemCPUs:      reservedCPUs,
					enableCPUManagerOptions: true,
					options: map[string]string{
						cpumanager.FullPCPUsOnlyOption:        "true",
						cpumanager.StrictCPUReservationOption: "true",
					},
				}))

				cpuCount := smtLevel
				cpuRequest := fmt.Sprintf("%d000m", smtLevel)
				ginkgo.By(fmt.Sprintf("creating the testing pod cpuRequest=%v", cpuRequest))
				pod := makeCPUManagerPod("gu-pod", []ctnAttribute{
					{
						ctnName:    "gu-container-x",
						cpuRequest: cpuRequest,
						cpuLimit:   cpuRequest,
					},
				})
				ginkgo.By("creating the testing pod")
				pod = createPodSync(ctx, pod)

				usableCPUs := onlineCPUs.Difference(reservedCPUs)

				gomega.Expect(pod).To(HaveContainerCPUsCount("gu-container-x", cpuCount))
				gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("gu-container-x", usableCPUs))
				gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("gu-container-x", reservedCPUs))

				ginkgo.By("validating each container in the testing pod")
				for _, cnt := range pod.Spec.Containers {
					ginkgo.By(fmt.Sprintf("validating the container %s on pod %s", cnt.Name, pod.Name))

					gomega.Expect(pod).To(HaveContainerCPUsAlignedTo(cnt.Name, smtLevel))
					gomega.Expect(pod).To(HaveContainerCPUsThreadSiblings(cnt.Name))
				}
			})
		})

		// please avoid nesting `BeforeEach` as much as possible. Ideally avoid completely.
		ginkgo.Context("SMT Alignment and Uncore Cache Alignment", ginkgo.Label("smt-alignment", "prefer-align-cpus-by-uncore-cache"), func() {
			ginkgo.BeforeEach(func(ctx context.Context) {
				// 非 SMT 系统上严格 SMT 对齐自然满足，无需校验
				if smtLevel < minSMTLevel {
					e2eskipper.Skipf("Skipping CPU Manager %q tests since SMT disabled", cpumanager.FullPCPUsOnlyOption)
				}
				reservedCPUs = cpuset.New(0)
			})

			ginkgo.It("should assign packed CPUs with prefer-align-cpus-by-uncore-cache disabled and pcpu-only policy options enabled", func(ctx context.Context) {
				skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), smtLevel)

				updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
					policyName:              string(cpumanager.PolicyStatic),
					reservedSystemCPUs:      reservedCPUs,
					enableCPUManagerOptions: true,
					options: map[string]string{
						cpumanager.FullPCPUsOnlyOption:            "true",
						cpumanager.PreferAlignByUnCoreCacheOption: "false",
					},
				}))

				ctnAttrs := []ctnAttribute{
					{
						ctnName:    "test-gu-container-uncore-cache-alignment-disabled",
						cpuRequest: "2000m",
						cpuLimit:   "2000m",
					},
				}
				pod := makeCPUManagerPod("test-pod-uncore-cache-alignment-disabled", ctnAttrs)
				ginkgo.By("creating the test pod")
				pod = createPodSync(ctx, pod)

				ginkgo.By("validating each container in the testing pod")
				for _, cnt := range pod.Spec.Containers {
					ginkgo.By(fmt.Sprintf("validating the container %s on pod %s", cnt.Name, pod.Name))

					gomega.Expect(pod).To(HaveContainerCPUsAlignedTo(cnt.Name, smtLevel))
					gomega.Expect(pod).To(HaveContainerCPUsThreadSiblings(cnt.Name))
				}
			})

			ginkgo.It("should assign CPUs aligned to uncore caches with prefer-align-cpus-by-uncore-cache and pcpu-only policy options enabled", func(ctx context.Context) {

				skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), smtLevel)

				updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
					policyName:              string(cpumanager.PolicyStatic),
					reservedSystemCPUs:      reservedCPUs,
					enableCPUManagerOptions: true,
					options: map[string]string{
						cpumanager.FullPCPUsOnlyOption:            "true",
						cpumanager.PreferAlignByUnCoreCacheOption: "true",
					},
				}))

				// 检查节点处理器架构是 split 还是 monolithic uncore cache。
				// prefer-align-cpus-by-uncore-cache 可在非 split uncore cache 处理器上启用，
				// 且不改变默认 static 行为
				allocatableCPUs := cpuDetailsFromNode(getLocalNode(ctx, f)).Allocatable
				hasSplitUncore := (allocatableCPUs > int64(uncoreGroupSize))

				if hasSplitUncore {
					// 对 split uncore cache 处理器的节点
					// 创建一个请求整块 uncore cache CPU 数的 Pod
					ctnAttrs := []ctnAttribute{
						{
							ctnName:    "test-gu-container-align-cpus-by-uncore-cache-on-split-uncore",
							cpuRequest: fmt.Sprintf("%d", uncoreGroupSize),
							cpuLimit:   fmt.Sprintf("%d", uncoreGroupSize),
						},
					}
					pod := makeCPUManagerPod("test-pod-align-cpus-by-uncore-cache", ctnAttrs)
					ginkgo.By("creating the test pod")
					pod = createPodSync(ctx, pod)

					// 'prefer-align-cpus-by-uncore-cache' 策略选项会尽最大努力分配 CPU，
					// 使跨 uncore cache 的分布最小化。由于测试容器请求整块 uncore cache 的 CPU，
					// 而 CPU0 属于预留 cpuset 不可分配，策略会尝试
					// 从下一个可用的 uncore cache 分配 CPU

					for _, cnt := range pod.Spec.Containers {
						ginkgo.By(fmt.Sprintf("validating the container %s on pod %s", cnt.Name, pod.Name))

						gomega.Expect(pod).To(HaveContainerCPUsAlignedTo(cnt.Name, smtLevel))
						cpus, err := getContainerAllowedCPUs(pod, cnt.Name, false)
						framework.ExpectNoError(err, "cannot get cpus allocated to pod %s/%s cnt %s", pod.Namespace, pod.Name, cnt.Name)

						siblingsCPUs := makeThreadSiblingCPUSet(cpus)
						gomega.Expect(pod).To(HaveContainerCPUsEqualTo(cnt.Name, siblingsCPUs))

						gomega.Expect(pod).To(HaveContainerCPUsWithSameUncoreCacheID(cnt.Name))
						gomega.Expect(pod).ToNot(HaveContainerCPUsShareUncoreCacheWith(cnt.Name, reservedCPUs))
					}
				} else {
					// 对 monolithic uncore cache 处理器的节点
					// 预期启用 pcpu-only 策略时的默认 static 行为，
					// 以及启用 prefer-align-cpus-by-uncore-cache 时的行为
					ctnAttrs := []ctnAttribute{
						{
							ctnName:    "test-gu-container-align-cpus-by-uncore-cache-on-mono-uncore",
							cpuRequest: "2000m",
							cpuLimit:   "2000m",
						},
					}
					pod := makeCPUManagerPod("test-pod-align-cpus-by-uncore-cache", ctnAttrs)
					ginkgo.By("creating the test pod")
					pod = createPodSync(ctx, pod)

					ginkgo.By("validating each container in the testing pod")
					for _, cnt := range pod.Spec.Containers {
						ginkgo.By(fmt.Sprintf("validating the container %s on pod %s", cnt.Name, pod.Name))

						gomega.Expect(pod).To(HaveContainerCPUsAlignedTo(cnt.Name, smtLevel))
						gomega.Expect(pod).To(HaveContainerCPUsThreadSiblings(cnt.Name))
					}
				}
			})
		})

		// please avoid nesting `BeforeEach` as much as possible. Ideally avoid completely.
		ginkgo.Context("Strict CPU Reservation and Uncore Cache Alignment", ginkgo.Label("strict-cpu-reservation", "prefer-align-cpus-by-uncore-cache"), func() {
			ginkgo.BeforeEach(func(ctx context.Context) {
				reservedCPUs = cpuset.New(0)
			})

			ginkgo.It("should assign CPUs aligned to uncore caches with prefer-align-cpus-by-uncore-cache and avoid reserved cpus", func(ctx context.Context) {
				// 假定一个 uncore cache 对应的 CPU 数总是偶数
				// 最小的整数 CPU 请求是 1 个 CPU
				// 要有意义的测试，最小可分配 CPU 要求应为：
				// minCPUCapacity + reservedCPUs.Size() + 1 个已分配的 CPU
				cpuCount := minCPUCapacity + reservedCPUs.Size() + 1
				skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuCount)

				updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
					policyName:              string(cpumanager.PolicyStatic),
					reservedSystemCPUs:      reservedCPUs,
					enableCPUManagerOptions: true,
					options: map[string]string{
						cpumanager.StrictCPUReservationOption:     "true",
						cpumanager.PreferAlignByUnCoreCacheOption: "true",
					},
				}))

				// 检查节点处理器架构是 split 还是 monolithic uncore cache。
				// prefer-align-cpus-by-uncore-cache 可在非 split uncore cache 处理器上启用，
				// 且不改变默认 static 行为
				allocatableCPUs := cpuDetailsFromNode(getLocalNode(ctx, f)).Allocatable
				hasSplitUncore := (allocatableCPUs > int64(uncoreGroupSize))

				if hasSplitUncore {
					// 对 split uncore cache 处理器的节点
					// 创建一个请求整块 uncore cache CPU 数的 Pod
					ctnAttrs := []ctnAttribute{
						{
							ctnName:    "test-gu-container-align-cpus-by-uncore-cache-on-split-uncore",
							cpuRequest: fmt.Sprintf("%d", uncoreGroupSize),
							cpuLimit:   fmt.Sprintf("%d", uncoreGroupSize),
						},
					}
					pod := makeCPUManagerPod("test-pod-align-cpus-by-uncore-cache", ctnAttrs)
					ginkgo.By("creating the test pod")
					pod = createPodSync(ctx, pod)

					// 'prefer-align-cpus-by-uncore-cache' 策略选项会尽最大努力分配 CPU，
					// 使跨 uncore cache 的分布最小化。由于测试容器请求整块 uncore cache 的 CPU，
					// 而 CPU0 属于预留 cpuset 不可分配，策略会尝试
					// 从下一个可用的 uncore cache 分配 CPU

					for _, cnt := range pod.Spec.Containers {
						ginkgo.By(fmt.Sprintf("validating the container %s on pod %s", cnt.Name, pod.Name))

						gomega.Expect(pod).To(HaveContainerCPUsAlignedTo(cnt.Name, smtLevel))
						cpus, err := getContainerAllowedCPUs(pod, cnt.Name, false)
						framework.ExpectNoError(err, "cannot get cpus allocated to pod %s/%s cnt %s", pod.Namespace, pod.Name, cnt.Name)

						siblingsCPUs := makeThreadSiblingCPUSet(cpus)
						gomega.Expect(pod).To(HaveContainerCPUsEqualTo(cnt.Name, siblingsCPUs))

						gomega.Expect(pod).To(HaveContainerCPUsWithSameUncoreCacheID(cnt.Name))
						gomega.Expect(pod).ToNot(HaveContainerCPUsShareUncoreCacheWith(cnt.Name, reservedCPUs))
					}
				} else {
					// 对 monolithic uncore cache 处理器的节点
					// 预期启用 prefer-align-cpus-by-uncore-cache 时的默认 static packed 行为
					//
					ctnAttrs := []ctnAttribute{
						{
							ctnName:    "test-gu-container-align-cpus-by-uncore-cache-on-mono-uncore",
							cpuRequest: "1000m",
							cpuLimit:   "1000m",
						},
					}
					pod := makeCPUManagerPod("test-pod-align-cpus-by-uncore-cache", ctnAttrs)
					ginkgo.By("creating the test pod")
					pod = createPodSync(ctx, pod)

					ginkgo.By("validating each container in the testing pod")
					for _, cnt := range pod.Spec.Containers {
						ginkgo.By(fmt.Sprintf("validating the container %s on pod %s", cnt.Name, pod.Name))

						// 预期分配的 CPU 位于同一 uncore cache
						gomega.Expect(pod).To(HaveContainerCPUsWithSameUncoreCacheID(cnt.Name))
						gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("test-gu-container-align-cpus-by-uncore-cache-on-mono-uncore", reservedCPUs))
					}
				}
			})
		})

		// please avoid nesting `BeforeEach` as much as possible. Ideally avoid completely.
		ginkgo.Context("SMT Alignment and distribution across NUMA", ginkgo.Label("smt-alignment", "distribute-cpus-across-numa"), func() {
			ginkgo.BeforeEach(func(ctx context.Context) {
				// 非 SMT 系统上严格 SMT 对齐自然满足，无需校验
				if smtLevel < minSMTLevel {
					e2eskipper.Skipf("Skipping CPU Manager %q tests since SMT disabled", cpumanager.FullPCPUsOnlyOption)
				}
				reservedCPUs = cpuset.New(0)
			})

			ginkgo.It("should assign packed CPUs with distribute-cpus-across-numa disabled and pcpu-only policy options enabled", func(ctx context.Context) {
				skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), smtLevel)

				updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
					policyName:              string(cpumanager.PolicyStatic),
					reservedSystemCPUs:      reservedCPUs,
					enableCPUManagerOptions: true,
					options: map[string]string{
						cpumanager.FullPCPUsOnlyOption:            "true",
						cpumanager.DistributeCPUsAcrossNUMAOption: "false",
					},
				}))

				ctnAttrs := []ctnAttribute{
					{
						ctnName:    "test-gu-container-distribute-cpus-across-numa-disabled",
						cpuRequest: "2000m",
						cpuLimit:   "2000m",
					},
				}
				pod := makeCPUManagerPod("test-pod-distribute-cpus-across-numa-disabled", ctnAttrs)
				ginkgo.By("creating the test pod")
				pod = createPodSync(ctx, pod)

				ginkgo.By("validating each container in the testing pod")
				for _, cnt := range pod.Spec.Containers {
					ginkgo.By(fmt.Sprintf("validating the container %s on pod %s", cnt.Name, pod.Name))

					gomega.Expect(pod).To(HaveContainerCPUsAlignedTo(cnt.Name, smtLevel))
					gomega.Expect(pod).To(HaveContainerCPUsThreadSiblings(cnt.Name))
				}
			})

			ginkgo.It("should assign CPUs distributed across NUMA with distribute-cpus-across-numa and pcpu-only policy options enabled", func(ctx context.Context) {
				reservedCPUs = cpuset.New(0)

				// 本测试面向多节点 NUMA 系统，且
				// 每个 socket 至少 4 核，不满足要求时 hostcheck 会跳过测试
				//
				numaNodeNum, _, _, cpusNumPerNUMA := hostCheck()
				cpuReq := (cpusNumPerNUMA - smtLevel) * numaNodeNum

				skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuReq)

				updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
					policyName:              string(cpumanager.PolicyStatic),
					reservedSystemCPUs:      reservedCPUs,
					enableCPUManagerOptions: true,
					options: map[string]string{
						cpumanager.FullPCPUsOnlyOption:            "true",
						cpumanager.DistributeCPUsAcrossNUMAOption: "true",
					},
				}))

				// 'distribute-cpus-across-numa' 策略选项确保当一次分配需要跨越多个 NUMA 节点时，
				//  CPU 分配会均匀分布在各 NUMA 节点上。
				// 因此要确保 CPU 请求量超过单个 NUMA 节点可容纳的 CPU 数。
				// 需要挑选满足以下条件的 cpuRequest：
				// 1. CPURequest > cpusNumPerNUMA
				// 2. 不占用节点全部 CPU，为预留 CPU 留出空间
				// 3. CPURequest 是 NUMA 节点数的倍数，才能在各节点均匀分配
				//
				// 总结：cpusNumPerNUMA < CPURequest < ((cpusNumPerNuma * numaNodeNum) - reservedCPUscount)
				// 综合考虑这些约束，选择：CPURequest= (cpusNumPerNUMA-smtLevel)*numaNodeNum

				ctnAttrs := []ctnAttribute{
					{
						ctnName:    "test-gu-container-distribute-cpus-across-numa",
						cpuRequest: fmt.Sprintf("%d", cpuReq),
						cpuLimit:   fmt.Sprintf("%d", cpuReq),
					},
				}
				pod := makeCPUManagerPod("test-pod-distribute-cpus-across-numa", ctnAttrs)
				ginkgo.By("creating the test pod")
				pod = createPodSync(ctx, pod)

				for _, cnt := range pod.Spec.Containers {
					ginkgo.By(fmt.Sprintf("validating the container %s on pod %s", cnt.Name, pod.Name))

					gomega.Expect(pod).To(HaveContainerCPUsAlignedTo(cnt.Name, smtLevel))
					cpus, err := getContainerAllowedCPUs(pod, cnt.Name, false)
					framework.ExpectNoError(err, "cannot get cpus allocated to pod %s/%s cnt %s", pod.Namespace, pod.Name, cnt.Name)

					siblingsCPUs := makeThreadSiblingCPUSet(cpus)
					gomega.Expect(pod).To(HaveContainerCPUsEqualTo(cnt.Name, siblingsCPUs))

					// 预期完全均匀分配 —— 因为 CPU 请求是 4*smtLevel*numaNodeNum。
					expectedSpread := cpus.Size() / numaNodeNum
					gomega.Expect(cpus).To(BeDistributedCPUs(expectedSpread))
				}
			})
		})

		ginkgo.Context("align-by-socket and distribute-cpus-across-numa policy options", ginkgo.Label("align-by-socket", "distribute-cpus-across-numa"), func() {
			ginkgo.BeforeEach(func(ctx context.Context) {
				reservedCPUs = cpuset.New(0)
			})

			ginkgo.It("should assign CPUs within a single socket when align-by-socket is enabled", func(ctx context.Context) {
				_, _, numaPerSocket, cpusNumPerNUMA := hostCheckForSocketPolicyOptions()

				// 请求的 CPU 数超过单个 NUMA 节点所能提供，但少于整个 socket，
				// 因此分配必须跨越同一 socket 上的多个 NUMA 节点。
				cpuReq := cpusNumPerNUMA + 1
				cpusPerSocket := cpusNumPerNUMA * numaPerSocket
				if cpuReq >= cpusPerSocket {
					e2eskipper.Skipf("cannot construct align-by-socket test: need %d < %d CPUs per socket", cpuReq, cpusPerSocket)
				}
				skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuReq)

				updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
					policyName:              string(cpumanager.PolicyStatic),
					reservedSystemCPUs:      reservedCPUs,
					enableCPUManagerOptions: true,
					options: map[string]string{
						cpumanager.AlignBySocketOption:            "true",
						cpumanager.DistributeCPUsAcrossNUMAOption: "false",
					},
				}))

				pod := makeCPUManagerPod("test-pod-align-by-socket", []ctnAttribute{
					{
						ctnName:    "test-gu-container-align-by-socket",
						cpuRequest: fmt.Sprintf("%d", cpuReq),
						cpuLimit:   fmt.Sprintf("%d", cpuReq),
					},
				})
				ginkgo.By("creating the test pod")
				pod = createPodSync(ctx, pod)

				ginkgo.By("validating the container is allocated within a single socket")
				cpus, err := getContainerAllowedCPUs(pod, "test-gu-container-align-by-socket", false)
				framework.ExpectNoError(err)
				gomega.Expect(cpus.Size()).To(gomega.Equal(cpuReq))
				gomega.Expect(cpus).To(BeAllocatedToSingleSocket())
			})

			ginkgo.It("should assign CPUs distributed across NUMA when distribute-cpus-across-numa is enabled", func(ctx context.Context) {
				if smtLevel < minSMTLevel {
					e2eskipper.Skipf("Skipping CPU Manager %q tests since SMT disabled", cpumanager.FullPCPUsOnlyOption)
				}

				numaNodeNum, _, _, cpusNumPerNUMA := hostCheck()
				cpuReq := (cpusNumPerNUMA - smtLevel) * numaNodeNum
				skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuReq)

				updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
					policyName:              string(cpumanager.PolicyStatic),
					reservedSystemCPUs:      reservedCPUs,
					enableCPUManagerOptions: true,
					options: map[string]string{
						cpumanager.FullPCPUsOnlyOption:            "true",
						cpumanager.AlignBySocketOption:            "false",
						cpumanager.DistributeCPUsAcrossNUMAOption: "true",
					},
				}))

				pod := makeCPUManagerPod("test-pod-distribute-cpus-across-numa-only", []ctnAttribute{
					{
						ctnName:    "test-gu-container-distribute-cpus-across-numa-only",
						cpuRequest: fmt.Sprintf("%d", cpuReq),
						cpuLimit:   fmt.Sprintf("%d", cpuReq),
					},
				})
				ginkgo.By("creating the test pod")
				pod = createPodSync(ctx, pod)

				ginkgo.By("validating CPUs are evenly distributed across NUMA nodes")
				cpus, err := getContainerAllowedCPUs(pod, "test-gu-container-distribute-cpus-across-numa-only", false)
				framework.ExpectNoError(err)
				gomega.Expect(pod).To(HaveContainerCPUsAlignedTo("test-gu-container-distribute-cpus-across-numa-only", smtLevel))
				gomega.Expect(pod).To(HaveContainerCPUsThreadSiblings("test-gu-container-distribute-cpus-across-numa-only"))

				expectedSpread := cpus.Size() / numaNodeNum
				gomega.Expect(cpus).To(BeDistributedCPUs(expectedSpread))
			})

			ginkgo.It("should distribute CPUs across NUMA within a single socket when both policy options are enabled", func(ctx context.Context) {
				if smtLevel < minSMTLevel {
					e2eskipper.Skipf("Skipping CPU Manager %q tests since SMT disabled", cpumanager.FullPCPUsOnlyOption)
				}

				_, _, numaPerSocket, cpusNumPerNUMA := hostCheckForSocketPolicyOptions()
				cpuReq := (cpusNumPerNUMA - smtLevel) * numaPerSocket
				skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuReq)

				updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
					policyName:              string(cpumanager.PolicyStatic),
					reservedSystemCPUs:      reservedCPUs,
					enableCPUManagerOptions: true,
					options: map[string]string{
						cpumanager.FullPCPUsOnlyOption:            "true",
						cpumanager.AlignBySocketOption:            "true",
						cpumanager.DistributeCPUsAcrossNUMAOption: "true",
					},
				}))

				pod := makeCPUManagerPod("test-pod-align-by-socket-distribute-across-numa", []ctnAttribute{
					{
						ctnName:    "test-gu-container-align-by-socket-distribute-across-numa",
						cpuRequest: fmt.Sprintf("%d", cpuReq),
						cpuLimit:   fmt.Sprintf("%d", cpuReq),
					},
				})
				ginkgo.By("creating the test pod")
				pod = createPodSync(ctx, pod)

				ginkgo.By("validating CPUs are socket-aligned and evenly distributed across NUMA nodes on that socket")
				cpus, err := getContainerAllowedCPUs(pod, "test-gu-container-align-by-socket-distribute-across-numa", false)
				framework.ExpectNoError(err)
				gomega.Expect(pod).To(HaveContainerCPUsAlignedTo("test-gu-container-align-by-socket-distribute-across-numa", smtLevel))
				gomega.Expect(pod).To(HaveContainerCPUsThreadSiblings("test-gu-container-align-by-socket-distribute-across-numa"))
				gomega.Expect(cpus).To(BeAllocatedToSingleSocket())
				gomega.Expect(cpus).To(BeEvenlyDistributedAcrossActiveNUMANodes())
			})
		})
	})

	ginkgo.When("checking the CFS quota management", ginkgo.Label("cfs-quota"), func() {
		ginkgo.BeforeEach(func(ctx context.Context) {
			requireCGroupV2()
			// 警告：这里假定系统是 2 路 SMT —— 我们不知道如何访问其他 SMT level。
			//       这意味着在超过 2 路的 SMT 系统上，本测试证明不了什么
			reservedCPUs = cpuset.New(0)
			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:         string(cpumanager.PolicyStatic),
				reservedSystemCPUs: reservedCPUs,
			}))
		})

		ginkgo.It("should enforce for best-effort pod", func(ctx context.Context) {
			ctnName := "be-container"
			pod := makeCPUManagerBEPod("be-pod", []ctnAttribute{
				{
					ctnName:    ctnName,
					ctnCommand: "sleep 1d",
				},
			})
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			gomega.Expect(pod).To(HaveSandboxQuota("max"))
			gomega.Expect(pod).To(HaveContainerQuota(ctnName, "max"))
		})

		ginkgo.It("should disable for guaranteed pod with exclusive single CPU assigned", func(ctx context.Context) {
			cpuCount := 1
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuCount)

			ctnName := "gu-container-cfsquota-disabled"
			pod := makeCPUManagerPod("gu-pod-cfsquota-off", []ctnAttribute{
				{
					ctnName:    ctnName,
					cpuRequest: "1",
					cpuLimit:   "1",
				},
			})
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			gomega.Expect(pod).To(HaveSandboxQuota("max"))
			gomega.Expect(pod).To(HaveContainerQuota(ctnName, "max"))
		})

		ginkgo.It("should disable for guaranteed pod with exclusive four CPUs assigned", func(ctx context.Context) {
			cpuCount := 4
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuCount)

			ctnName := "gu-container-cfsquota-disabled"
			pod := makeCPUManagerPod("gu-pod-cfsquota-off", []ctnAttribute{
				{
					ctnName:    ctnName,
					cpuRequest: "3",
					cpuLimit:   "3",
				},
			})
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			gomega.Expect(pod).To(HaveSandboxQuota("max"))
			gomega.Expect(pod).To(HaveContainerQuota(ctnName, "max"))

			gomega.Expect(pod).To(HaveContainerCPUsCount(ctnName, 3))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf(ctnName, onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith(ctnName, reservedCPUs))
		})

		ginkgo.It("should enforce for guaranteed pod", func(ctx context.Context) {
			cpuCount := 1 // 取过估值，最小请求为 1
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuCount)

			ctnName := "gu-container-cfsquota-enabled"
			pod := makeCPUManagerPod("gu-pod-cfs-quota-on", []ctnAttribute{
				{
					ctnName:    ctnName,
					cpuRequest: "500m",
					cpuLimit:   "500m",
				},
			})
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			gomega.Expect(pod).To(HaveSandboxQuota("50000"))
			gomega.Expect(pod).To(HaveContainerQuota(ctnName, "50000"))
		})

		ginkgo.It("should enforce for burstable pod", func(ctx context.Context) {
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), 0)

			ctnName := "bu-container-cfsquota-enabled"
			pod := makeCPUManagerPod("bu-pod-cfs-quota-on", []ctnAttribute{
				{
					ctnName:    ctnName,
					cpuRequest: "100m",
					cpuLimit:   "500m",
				},
			})
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			gomega.Expect(pod).To(HaveSandboxQuota("50000"))
			gomega.Expect(pod).To(HaveContainerQuota(ctnName, "50000"))
		})

		ginkgo.It("should not enforce with multiple containers without exclusive CPUs", func(ctx context.Context) {
			cpuCount := 2
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuCount)

			pod := makeCPUManagerPod("gu-pod-multicontainer", []ctnAttribute{
				{
					ctnName:    "gu-container-non-int-values-1",
					cpuRequest: "100m",
					cpuLimit:   "500m",
				},
				{
					ctnName:    "gu-container-non-int-values-2",
					cpuRequest: "300m",
					cpuLimit:   "1200m",
				},
			})
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			gomega.Expect(pod).To(HaveSandboxQuota("170000"))
			gomega.Expect(pod).To(HaveContainerQuota("gu-container-non-int-values-1", "50000"))
			gomega.Expect(pod).To(HaveContainerQuota("gu-container-non-int-values-2", "120000"))
		})

		ginkgo.It("should not enforce with multiple containers only in the container with exclusive CPUs", func(ctx context.Context) {
			cpuCount := 2
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuCount)

			pod := makeCPUManagerPod("gu-pod-multicontainer-mixed", []ctnAttribute{
				{
					ctnName:    "gu-container-non-int-values",
					cpuRequest: "500m",
					cpuLimit:   "500m",
				},
				{
					ctnName:    "gu-container-int-values",
					cpuRequest: "1",
					cpuLimit:   "1",
				},
			})
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			gomega.Expect(pod).To(HaveSandboxQuota("max"))
			gomega.Expect(pod).To(HaveContainerQuota("gu-container-non-int-values", "50000"))
			gomega.Expect(pod).To(HaveContainerQuota("gu-container-int-values", "max"))
		})
	})

	ginkgo.When("using non-default CPU CFS quota period", ginkgo.Label("cfs-period", "cfs-quota"), func() {
		var cfsPeriod time.Duration
		var testCFSPeriod string

		ginkgo.BeforeEach(func(ctx context.Context) {
			requireCGroupV2()
			// 警告：这里假定系统是 2 路 SMT —— 我们不知道如何访问其他 SMT level。
			//       这意味着在超过 2 路的 SMT 系统上，本测试证明不了什么
			reservedCPUs = cpuset.New(0)

			// 默认 QuotaPeriod 单位是毫秒
			cfsPeriod = time.Duration((cm.QuotaPeriod / 2) * time.Microsecond)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:              string(cpumanager.PolicyStatic),
				reservedSystemCPUs:      reservedCPUs,
				customCPUCFSQuotaPeriod: cfsPeriod, // TODO: 是否需要逐个测试设置？
			}))

			// 我们用微秒思考，内核暴露的却是毫秒
			testCFSPeriod = strconv.FormatInt(int64(cfsPeriod/1000), 10)
		})

		ginkgo.It("should enforce for guaranteed pod", func(ctx context.Context) {
			cpuCount := 1 // 取过估值，最小请求为 1
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuCount)

			ctnName := "gu-container-cfsquota-enabled"
			pod := makeCPUManagerPod("gu-pod-cfs-quota-on", []ctnAttribute{
				{
					ctnName:    ctnName,
					cpuRequest: "500m",
					cpuLimit:   "500m",
				},
			})
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			gomega.Expect(pod).To(HaveSandboxQuotaWithPeriod("25000", testCFSPeriod))
			gomega.Expect(pod).To(HaveContainerQuotaWithPeriod(ctnName, "25000", testCFSPeriod))
		})

		ginkgo.It("should enforce for burstable pod", func(ctx context.Context) {
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), 0)

			ctnName := "bu-container-cfsquota-enabled"
			pod := makeCPUManagerPod("bu-pod-cfs-quota-on", []ctnAttribute{
				{
					ctnName:    ctnName,
					cpuRequest: "100m",
					cpuLimit:   "500m",
				},
			})
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			gomega.Expect(pod).To(HaveSandboxQuotaWithPeriod("25000", testCFSPeriod))
			gomega.Expect(pod).To(HaveContainerQuotaWithPeriod(ctnName, "25000", testCFSPeriod))
		})
	})

	f.Context("When checking the sidecar containers", framework.WithNodeConformance(), func() {
		ginkgo.BeforeEach(func(ctx context.Context) {
			reservedCPUs = cpuset.New(0)
		})

		ginkgo.It("should reuse init container exclusive CPUs, but not sidecar container exclusive CPUS", func(ctx context.Context) {
			cpuCount := 2 // 总数

			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuCount)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:         string(cpumanager.PolicyStatic),
				reservedSystemCPUs: reservedCPUs, // Not really needed for the tests but helps to make a more precise check
			}))

			var containerRestartPolicyAlways = v1.ContainerRestartPolicyAlways

			ctrAttrs := []ctnAttribute{
				{
					ctnName:    "init-container1",
					cpuRequest: "1000m",
					cpuLimit:   "1000m",
				},
				{
					ctnName:       "sidecar-container",
					cpuRequest:    "1000m",
					cpuLimit:      "1000m",
					restartPolicy: &containerRestartPolicyAlways,
				},
			}
			pod := makeCPUManagerInitContainersPod("gu-pod", ctrAttrs)
			ginkgo.By("running a Gu pod with a regular init container and a restartable init container")
			pod = createPodSync(ctx, pod)

			// 执行到这里时真正的 init container 已终止，只能检查它的日志
			ginkgo.By("checking if the expected cpuset was assigned")
			logs, err := e2epod.GetPodLogs(ctx, f.ClientSet, pod.Namespace, pod.Name, pod.Spec.InitContainers[0].Name)
			framework.ExpectNoError(err, "expected log not found in init container [%s] of pod [%s]", pod.Spec.InitContainers[0].Name, pod.Name)

			reusableCPUs := getContainerAllowedCPUsFromLogs(pod.Name, pod.Spec.InitContainers[0].Name, logs)
			gomega.Expect(reusableCPUs.Size()).To(gomega.Equal(1), "expected cpu set size == 1, got %q", reusableCPUs.String())

			nonReusableCPUs, err := getContainerAllowedCPUs(pod, pod.Spec.InitContainers[1].Name, true)
			framework.ExpectNoError(err, "cannot get exclusive CPUs for pod %s/%s", pod.Namespace, pod.Name)

			gomega.Expect(nonReusableCPUs.Size()).To(gomega.Equal(1), "expected cpu set size == 1, got %q", nonReusableCPUs.String())
			gomega.Expect(reusableCPUs.Equals(nonReusableCPUs)).To(gomega.BeTrueBecause("expected reusable cpuset [%s] to be equal to non-reusable cpuset [%s]", reusableCPUs.String(), nonReusableCPUs.String()))

			appContainerName := pod.Spec.Containers[0].Name
			gomega.Expect(pod).To(HaveContainerCPUsCount(appContainerName, 1))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf(appContainerName, onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith(appContainerName, reservedCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith(appContainerName, nonReusableCPUs))
		})
	})

	f.Context("WhenRestartAllContainers", f.WithFeatureGate(features.ContainerRestartRules), f.WithFeatureGate(features.RestartAllContainersOnContainerExits), func() {
		var (
			containerRestartPolicyNever = v1.ContainerRestartPolicyNever
		)
		restartAllContainersRules := []v1.ContainerRestartRule{
			{
				Action: v1.ContainerRestartRuleActionRestartAllContainers,
				ExitCodes: &v1.ContainerRestartRuleOnExitCodes{
					Operator: v1.ContainerRestartRuleOnExitCodesOpIn,
					Values:   []int32{42},
				},
			},
		}

		ginkgo.BeforeEach(func(ctx context.Context) {
			reservedCPUs = cpuset.New(0)
		})

		ginkgo.It("should preserve CPU affinity after restarting all containers", func(ctx context.Context) {
			cpuCount := 4 // 总数
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), cpuCount)
			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:         string(cpumanager.PolicyStatic),
				reservedSystemCPUs: reservedCPUs, // Not really needed for the tests but helps to make a more precise check
			}))

			podName := "restart-all-preserve-affinity-" + string(uuid.NewUUID())
			// 用 Guaranteed Pod 确保各类 manager（如 CPU Manager）参与工作。
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: podName,
				},
				Spec: v1.PodSpec{
					RestartPolicy: v1.RestartPolicyNever,
					Containers: []v1.Container{
						{
							Name:  "affinity-checker",
							Image: imageutils.GetE2EImage(imageutils.BusyBox),
							// 脚本逻辑：
							// 1. 检查共享卷中是否存在 "affinity.txt"。
							// 2. 不存在：校验隔离性，记录当前亲和性，退出 42 触发 RestartAll。
							// 3. 存在：比较当前亲和性与 "affinity.txt"。一致则永久 sleep（成功）。
							Command: []string{"/bin/sh", "-c", `
								AFFINITY_FILE="/mnt/affinity.txt"
								CURRENT_AFFINITY=$(grep Cpus_allowed_list /proc/self/status | cut -f2)
								if [ ! -f $AFFINITY_FILE ]; then
									echo "First run. Recording affinity: $CURRENT_AFFINITY"
									if echo "$CURRENT_AFFINITY" | grep -E [,-]; then
										echo "ERROR: CPU Manager did not assign an exclusive CPU. Affinity list: $CURRENT_AFFINITY"
										exit 1
									fi
									echo "$CURRENT_AFFINITY" > $AFFINITY_FILE
									exit 42
								else
									OLD_AFFINITY=$(cat $AFFINITY_FILE)
									echo "Restarted. Old: $OLD_AFFINITY, New: $CURRENT_AFFINITY"
									if [ "$OLD_AFFINITY" != "$CURRENT_AFFINITY" ]; then
										echo "ERROR: Affinity not preserved!"
										exit 1
									fi
									echo "Affinity preserved. Success."
									sleep 10000
								fi
							`},
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("1"),
									v1.ResourceMemory: resource.MustParse("100Mi"),
								},
								Requests: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("1"),
									v1.ResourceMemory: resource.MustParse("100Mi"),
								},
							},
							RestartPolicy:      &containerRestartPolicyNever,
							RestartPolicyRules: restartAllContainersRules,
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      "workdir",
									MountPath: "/mnt",
								},
							},
						},
						{
							Name:    "regular",
							Image:   imageutils.GetE2EImage(imageutils.BusyBox),
							Command: []string{"/bin/sh", "-c", "sleep 10000"},
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("1"),
									v1.ResourceMemory: resource.MustParse("100Mi"),
								},
								Requests: v1.ResourceList{
									v1.ResourceCPU:    resource.MustParse("1"),
									v1.ResourceMemory: resource.MustParse("100Mi"),
								},
							},
						},
					},
					Volumes: []v1.Volume{
						{
							Name: "workdir",
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{},
							},
						},
					},
				},
			}
			createPodSync(ctx, pod)

			// 所有容器都应重启一次
			ginkgo.By("Waiting for all containers to restart")
			err := e2epod.WaitForPodCondition(ctx, f.ClientSet, f.Namespace.Name, pod.Name, "all containers restarted", 3*time.Minute, func(pod *v1.Pod) (bool, error) {
				restartedCount := 0
				containers := []string{"affinity-checker", "regular"}
				for _, cName := range containers {
					restarted := false
					for _, status := range pod.Status.ContainerStatuses {
						if status.Name == cName && status.RestartCount > 0 {
							restarted = true
						}
					}
					if restarted {
						restartedCount++
					} else {
						framework.Logf("container %s did not restart", cName)
					}
				}
				framework.Logf("%d out of %d containers restarted", restartedCount, len(containers))
				return restartedCount == len(containers), nil
			})
			framework.ExpectNoError(err, "failed to see all containers restart")

			// 等待容器进入 running，这表明 CPU 分配被保留
			framework.ExpectNoError(e2epod.WaitForContainerRunning(ctx, f.ClientSet, f.Namespace.Name, podName, "affinity-checker", 3*time.Minute))
			framework.ExpectNoError(e2epod.WaitForContainerRunning(ctx, f.ClientSet, f.Namespace.Name, podName, "regular", 3*time.Minute))
		})
	})
})

var _ = SIGDescribe("CPU Manager Pod Level Resources", ginkgo.Ordered, ginkgo.ContinueOnFailure, framework.WithSerial(), feature.CPUManager, feature.PodLevelResources, feature.PodLevelResourceManagers, framework.WithFeatureGate(features.PodLevelResources), framework.WithFeatureGate(features.PodLevelResourceManagers), func() {
	f := framework.NewDefaultFramework("cpu-manager-pod-level-resources")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	// 上下文开始前的原始 kubeletconfig，稍后要恢复
	var oldCfg *kubeletconfig.KubeletConfiguration
	var reservedCPUs cpuset.CPUSet
	var onlineCPUs cpuset.CPUSet
	var smtLevel int
	var uncoreGroupSize int
	// 跟踪某个 It() 块创建的所有 Pod。最好每个 It 块一个命名空间
	var podMap map[string]*v1.Pod
	var skipIfAllocatableCPUsLessThan func(node *v1.Node, cpuReq int)

	var createPodSync func(ctx context.Context, pod *v1.Pod) *v1.Pod

	ginkgo.BeforeAll(func(ctx context.Context) {
		var err error
		oldCfg, err = getCurrentKubeletConfig(ctx)
		framework.ExpectNoError(err)

		onlineCPUs, err = getOnlineCPUs() // 至少在本套件生命周期内不应变化
		framework.ExpectNoError(err)
		framework.Logf("Online CPUs: %s", onlineCPUs)

		smtLevel = smtLevelFromSysFS() // 至少在本套件生命周期内不应变化
		framework.Logf("SMT level: %d", smtLevel)

		uncoreGroupSize = getUncoreCPUGroupSize()
		framework.Logf("Uncore Group Size: %d", uncoreGroupSize)

		e2enodeCgroupV2Enabled = IsCgroup2UnifiedMode()
		framework.Logf("cgroup V2 enabled: %v", e2enodeCgroupV2Enabled)

		e2enodeCgroupDriver = oldCfg.CgroupDriver
		framework.Logf("cgroup driver: %s", e2enodeCgroupDriver)

		runtime, _, err := getCRIClient(ctx)
		framework.ExpectNoError(err, "Failed to get CRI client")

		version, err := runtime.Version(ctx, "")
		framework.ExpectNoError(err, "Failed to get runtime version")

		e2enodeRuntimeName = version.GetRuntimeName()
		framework.Logf("runtime: %s", e2enodeRuntimeName)
	})

	ginkgo.AfterAll(func(ctx context.Context) {
		updateKubeletConfig(ctx, f, oldCfg, true)
	})

	ginkgo.BeforeEach(func(ctx context.Context) {
		// 注意有意不设置 reservedCPUs —— 必须逐个测试初始化
		podMap = make(map[string]*v1.Pod)
		createPodSync = func(ctx context.Context, pod *v1.Pod) *v1.Pod {
			newPod := e2epod.NewPodClient(f).CreateSync(ctx, pod)
			podMap[string(newPod.UID)] = newPod
			return newPod
		}
		reservedCPUs = cpuset.New()
	})

	ginkgo.JustBeforeEach(func(ctx context.Context) {
		if !e2enodeCgroupV2Enabled {
			e2eskipper.Skipf("Skipping since CgroupV2 not used")
		}
		// 用闭包减少参数，让用法更直观
		skipIfAllocatableCPUsLessThan = func(node *v1.Node, val int) {
			ginkgo.GinkgoHelper()
			checkAllocatableCPUs(node, val, reservedCPUs, onlineCPUs, smtLevel)
		}
	})

	ginkgo.AfterEach(func(ctx context.Context) {
		deletePodsAsync(ctx, f, podMap)
	})

	ginkgo.Context("when the topology manager scope is 'pod'", func() {
		ginkgo.It("should allocate exclusive CPUs to a guaranteed pod with pod-level resources and guaranteed container, PodLevelResourceManagers enabled", ginkgo.Label("pod-scope"), func(ctx context.Context) {
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), 1)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:                     string(cpumanager.PolicyStatic),
				topologyManagerPolicy:          topologymanager.PolicyRestricted,
				topologyManagerScope:           topologymanager.PodTopologyScope,
				reservedSystemCPUs:             cpuset.New(),
				enablePodLevelResources:        true,
				enablePodLevelResourceManagers: true,
			}))

			pod := makeCPUManagerPod("gu-pod-level-resources", []ctnAttribute{
				{
					ctnName:    "gu-container",
					cpuRequest: "1",
					cpuLimit:   "1",
				},
			})
			pod.Spec.Resources = &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1"),
					v1.ResourceMemory: resource.MustParse("100Mi"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1"),
					v1.ResourceMemory: resource.MustParse("100Mi"),
				},
			}
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the expected cpuset was assigned")

			gomega.Expect(pod).To(HavePodExclusiveCPUs(1))

			gomega.Expect(pod).To(HaveContainerCPUsCount("gu-container", 1))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("gu-container", onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("gu-container", reservedCPUs))
		})

		ginkgo.It("should allocate exclusive CPUs to a guaranteed pod with pod-level resources and non-guaranteed containers, PodLevelResourceManagers enabled", ginkgo.Label("pod-scope"), func(ctx context.Context) {
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), 1)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:                     string(cpumanager.PolicyStatic),
				topologyManagerPolicy:          topologymanager.PolicyRestricted,
				topologyManagerScope:           topologymanager.PodTopologyScope,
				reservedSystemCPUs:             cpuset.CPUSet{},
				enablePodLevelResources:        true,
				enablePodLevelResourceManagers: true,
			}))

			pod := makeCPUManagerPod("gu-pod-level-ngu-ctn", []ctnAttribute{
				{
					ctnName: "ngu-container",
				},
			})
			pod.Spec.Resources = &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1"),
					v1.ResourceMemory: resource.MustParse("200Mi"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1"),
					v1.ResourceMemory: resource.MustParse("200Mi"),
				},
			}
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the expected cpuset was assigned")

			gomega.Expect(pod).To(HavePodExclusiveCPUs(1))

			gomega.Expect(pod).To(HaveContainerCPUsCount("ngu-container", 1))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("ngu-container", onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("ngu-container", reservedCPUs))
		})

		ginkgo.It("should allocate exclusive CPUs to a guaranteed pod with pod-level resources and mix of guaranteed and non-guaranteed containers, PodLevelResourceManagers enabled", ginkgo.Label("pod-scope"), func(ctx context.Context) {
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), 3)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:                     string(cpumanager.PolicyStatic),
				topologyManagerPolicy:          topologymanager.PolicyRestricted,
				topologyManagerScope:           topologymanager.PodTopologyScope,
				reservedSystemCPUs:             cpuset.CPUSet{},
				enablePodLevelResources:        true,
				enablePodLevelResourceManagers: true,
			}))
			pod := makeCPUManagerPod("gu-pod-level-mix-ctn", []ctnAttribute{
				{
					ctnName:    "gu-container-1",
					cpuRequest: "1",
					cpuLimit:   "1",
				},
				{
					ctnName:    "gu-container-2",
					cpuRequest: "1",
					cpuLimit:   "1",
				},
				{
					ctnName: "ngu-container",
				},
			})
			pod.Spec.Resources = &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("3"),
					v1.ResourceMemory: resource.MustParse("300Mi"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("3"),
					v1.ResourceMemory: resource.MustParse("300Mi"),
				},
			}
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the expected cpuset was assigned")

			gomega.Expect(pod).To(HavePodExclusiveCPUs(3))

			gomega.Expect(pod).To(HaveContainerCPUsCount("gu-container-1", 1))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("gu-container-1", onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("gu-container-1", reservedCPUs))

			gomega.Expect(pod).To(HaveContainerCPUsCount("gu-container-2", 1))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("gu-container-2", onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("gu-container-2", reservedCPUs))

			gomega.Expect(pod).To(HaveContainerCPUsCount("ngu-container", 1))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("ngu-container", onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("ngu-container", reservedCPUs))

			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlap("gu-container-1", "gu-container-2"))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlap("gu-container-1", "ngu-container"))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlap("gu-container-2", "ngu-container"))
		})

		ginkgo.It("should allocate exclusive CPUs to a guaranteed pod with pod-level resources and mix of guaranteed and non-guaranteed standard and init containers, PodLevelResourceManagers enabled", ginkgo.Label("pod-scope"), func(ctx context.Context) {
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), 2)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:                     string(cpumanager.PolicyStatic),
				topologyManagerPolicy:          topologymanager.PolicyRestricted,
				topologyManagerScope:           topologymanager.PodTopologyScope,
				reservedSystemCPUs:             cpuset.CPUSet{},
				enablePodLevelResources:        true,
				enablePodLevelResourceManagers: true,
			}))
			pod := makeCPUManagerPod("gu-pod-level-mix-init-ctn", []ctnAttribute{
				{
					ctnName:    "gu-container",
					cpuRequest: "1",
					cpuLimit:   "1",
				},
				{
					ctnName: "ngu-container",
				},
			})
			pod.Spec.InitContainers = []v1.Container{
				{
					Name:    "gu-init-container",
					Image:   busyboxImage,
					Command: []string{"sh", "-c", "echo hello"},
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("1"),
							v1.ResourceMemory: resource.MustParse("100Mi"),
						},
						Limits: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("1"),
							v1.ResourceMemory: resource.MustParse("100Mi"),
						},
					},
				},
			}
			pod.Spec.Resources = &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("2"),
					v1.ResourceMemory: resource.MustParse("200Mi"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("2"),
					v1.ResourceMemory: resource.MustParse("200Mi"),
				},
			}
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the expected cpuset was assigned")

			gomega.Expect(pod).To(HavePodExclusiveCPUs(2))

			gomega.Expect(pod).To(HaveContainerCPUsCount("gu-container", 1))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("gu-container", onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("gu-container", reservedCPUs))

			gomega.Expect(pod).To(HaveContainerCPUsCount("ngu-container", 1))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("ngu-container", onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("ngu-container", reservedCPUs))

			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlap("gu-container", "ngu-container"))
		})

		ginkgo.It("should not allocate exclusive CPUs to a non-guaranteed pod with pod-level resources and guaranteed containers, PodLevelResourceManagers enabled", ginkgo.Label("pod-scope"), func(ctx context.Context) {
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), 1)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:                     string(cpumanager.PolicyStatic),
				topologyManagerPolicy:          topologymanager.PolicyRestricted,
				topologyManagerScope:           topologymanager.PodTopologyScope,
				reservedSystemCPUs:             cpuset.CPUSet{},
				enablePodLevelResources:        true,
				enablePodLevelResourceManagers: true,
			}))

			pod := makeCPUManagerPod("ngu-pod-level-gu-ctn", []ctnAttribute{
				{
					ctnName:    "gu-container",
					cpuRequest: "1",
					cpuLimit:   "1",
				},
			})
			pod.Spec.Resources = &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("1"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("2"),
				},
			}
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the expected cpuset was assigned")

			gomega.Expect(pod).To(HaveContainerCPUsEqualTo("gu-container", onlineCPUs))
		})

		ginkgo.It("should not allocate exclusive CPUs to a non-guaranteed pod with pod-level resources and non-guaranteed containers, PodLevelResourceManagers enabled", ginkgo.Label("pod-scope"), func(ctx context.Context) {
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), 1)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:                     string(cpumanager.PolicyStatic),
				topologyManagerPolicy:          topologymanager.PolicyRestricted,
				topologyManagerScope:           topologymanager.PodTopologyScope,
				reservedSystemCPUs:             cpuset.CPUSet{},
				enablePodLevelResources:        true,
				enablePodLevelResourceManagers: true,
			}))

			pod := makeCPUManagerPod("ngu-pod-level-ngu-ctn", []ctnAttribute{
				{
					ctnName: "ngu-container",
				},
			})
			pod.Spec.Resources = &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("1"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("2"),
				},
			}
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the expected cpuset was assigned")

			gomega.Expect(pod).To(HaveContainerCPUsEqualTo("ngu-container", onlineCPUs))
		})

		ginkgo.It("should reject a pod that would result in an empty pod shared pool", ginkgo.Label("pod-scope"), func(ctx context.Context) {
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), 2)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:                     string(cpumanager.PolicyStatic),
				topologyManagerPolicy:          topologymanager.PolicyRestricted,
				topologyManagerScope:           topologymanager.PodTopologyScope,
				reservedSystemCPUs:             cpuset.CPUSet{},
				enablePodLevelResources:        true,
				enablePodLevelResourceManagers: true,
			}))

			pod := makeCPUManagerPod("gu-pod-level-empty-shared", []ctnAttribute{
				{
					ctnName:    "gu-container-1",
					cpuRequest: "1",
					cpuLimit:   "1",
				},
				{
					ctnName:    "gu-container-2",
					cpuRequest: "1",
					cpuLimit:   "1",
				},
				{
					ctnName: "ngu-container",
				},
			})
			pod.Spec.Resources = &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("2"),
					v1.ResourceMemory: resource.MustParse("300Mi"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("2"),
					v1.ResourceMemory: resource.MustParse("300Mi"),
				},
			}
			ginkgo.By("creating the test pod")
			pod = e2epod.NewPodClient(f).Create(ctx, pod)
			podMap[string(pod.UID)] = pod

			ginkgo.By("waiting for the pod to fail")
			err := e2epod.WaitForPodCondition(ctx, f.ClientSet, f.Namespace.Name, pod.Name, "Failed", 30*time.Second, func(pod *v1.Pod) (bool, error) {
				return pod.Status.Phase == v1.PodFailed, nil
			})
			framework.ExpectNoError(err)

			pod, err = e2epod.NewPodClient(f).Get(ctx, pod.Name, metav1.GetOptions{})
			framework.ExpectNoError(err)
			gomega.Expect(pod).To(BeAPodInPhase(v1.PodFailed))
		})

		// 本测试演示 CPU 被正确释放并重新分配。
		// 它顺序运行两个 Pod，各自请求大量 CPU。
		// 校验第二个 Pod 被分配到与第一个相同的 CPU，
		// 证明第一个 Pod 完成后 CPU manager 释放了资源。
		ginkgo.It("should release and re-allocate CPUs correctly for sequential guaranteed pods with guaranteed and non-guaranteed containers", ginkgo.Label("pod-scope"), func(ctx context.Context) {
			node := getLocalNode(ctx, f)
			nodeCPUDetails := cpuDetailsFromNode(node)
			// 请求几乎所有可分配 CPU，以获得可预测的大集合。
			cpuCount := int(nodeCPUDetails.Allocatable - 1)
			if cpuCount < 2 {
				e2eskipper.Skipf("Skipping test: requires at least 2 allocatable CPUs, but has %d", int(nodeCPUDetails.Allocatable))
			}
			framework.Logf("Node has %d allocatable CPUs. Requesting %d for pods.", int(nodeCPUDetails.Allocatable), cpuCount)

			skipIfAllocatableCPUsLessThan(node, cpuCount)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:                     string(cpumanager.PolicyStatic),
				topologyManagerPolicy:          topologymanager.PolicyRestricted,
				topologyManagerScope:           topologymanager.PodTopologyScope,
				reservedSystemCPUs:             cpuset.New(0),
				enablePodLevelResources:        true,
				enablePodLevelResourceManagers: true,
			}))

			cpuRequest := strconv.Itoa(cpuCount)
			podLevelResources := &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse(cpuRequest),
					v1.ResourceMemory: resource.MustParse("300Mi"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse(cpuRequest),
					v1.ResourceMemory: resource.MustParse("300Mi"),
				},
			}

			guaranteedContainerName := "gu-container"
			nonGuaranteedContainerName := "ngu-container"
			containers := []ctnAttribute{
				{
					ctnName: guaranteedContainerName, cpuRequest: "1", cpuLimit: "1", ctnCommand: "echo 'container done' && sleep 2",
				},
				{
					ctnName: nonGuaranteedContainerName, ctnCommand: "echo 'container done' && sleep 2",
				},
			}

			pod1 := makeCPUManagerPod("gu-pod-sequential-1", containers)
			pod1.Spec.Resources = podLevelResources
			pod1.Spec.RestartPolicy = v1.RestartPolicyNever

			ginkgo.By("creating the first test pod")
			pod1 = createPodSync(ctx, pod1)

			ginkgo.By("verifying CPU allocation for the first pod")
			gomega.Expect(pod1).To(HavePodExclusiveCPUs(cpuCount))
			guCpus1, err := getContainerAllowedCPUs(pod1, guaranteedContainerName, false)
			framework.ExpectNoError(err)
			nguCpus1, err := getContainerAllowedCPUs(pod1, nonGuaranteedContainerName, false)
			framework.ExpectNoError(err)
			totalCpus1 := guCpus1.Union(nguCpus1)
			framework.Logf("Pod 1 allocated total CPUs: %s", totalCpus1.String())

			ginkgo.By("waiting for the first pod to succeed")
			err = e2epod.WaitForPodSuccessInNamespace(ctx, f.ClientSet, pod1.Name, f.Namespace.Name)
			framework.ExpectNoError(err, "first pod failed to complete")

			pod2 := makeCPUManagerPod("gu-pod-sequential-2", containers)
			pod2.Spec.Resources = podLevelResources
			pod2.Spec.RestartPolicy = v1.RestartPolicyNever

			ginkgo.By("creating the second test pod")
			pod2 = createPodSync(ctx, pod2)

			ginkgo.By("verifying CPU allocation for the second pod")
			gomega.Expect(pod2).To(HavePodExclusiveCPUs(cpuCount))
			guCpus2, err := getContainerAllowedCPUs(pod2, guaranteedContainerName, false)
			framework.ExpectNoError(err)
			nguCpus2, err := getContainerAllowedCPUs(pod2, nonGuaranteedContainerName, false)
			framework.ExpectNoError(err)
			totalCpus2 := guCpus2.Union(nguCpus2)
			framework.Logf("Pod 2 allocated total CPUs: %s", totalCpus2.String())

			ginkgo.By("verifying the second pod reused the CPUs from the first pod")
			gomega.Expect(totalCpus2).To(gomega.Equal(totalCpus1), "The CPU set for the second pod should be the same as the first pod's.")
		})

		// 本测试演示 CPU 从 Pod 共享池中正确释放，即使没有容器与之关联。
		// 演示 remove container 逻辑利用状态正确清理资源。
		//
		ginkgo.It("should release resources from the pod shared pool when no containers used it", ginkgo.Label("pod-scope"), func(ctx context.Context) {
			node := getLocalNode(ctx, f)
			nodeCPUDetails := cpuDetailsFromNode(node)
			// 请求几乎所有可分配 CPU，以获得可预测的大集合。
			cpuCount := int(nodeCPUDetails.Allocatable - 1)
			if cpuCount < 2 {
				e2eskipper.Skipf("Skipping test: requires at least 2 allocatable CPUs, but has %d", int(nodeCPUDetails.Allocatable))
			}
			framework.Logf("Node has %d allocatable CPUs. Requesting %d for pods.", int(nodeCPUDetails.Allocatable), cpuCount)

			skipIfAllocatableCPUsLessThan(node, cpuCount)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:                     string(cpumanager.PolicyStatic),
				topologyManagerPolicy:          topologymanager.PolicyRestricted,
				topologyManagerScope:           topologymanager.PodTopologyScope,
				reservedSystemCPUs:             cpuset.New(0),
				enablePodLevelResources:        true,
				enablePodLevelResourceManagers: true,
			}))

			cpuRequest := strconv.Itoa(cpuCount)
			podLevelResources := &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse(cpuRequest),
					v1.ResourceMemory: resource.MustParse("300Mi"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse(cpuRequest),
					v1.ResourceMemory: resource.MustParse("300Mi"),
				},
			}

			guaranteedContainerName := "gu-container"
			containers := []ctnAttribute{
				{
					ctnName: guaranteedContainerName, cpuRequest: "1", cpuLimit: "1", ctnCommand: "echo 'container done' && sleep 2",
				},
			}

			pod1 := makeCPUManagerPod("gu-pod-sequential-1", containers)
			pod1.Spec.Resources = podLevelResources
			pod1.Spec.RestartPolicy = v1.RestartPolicyNever

			ginkgo.By("creating the first test pod")
			pod1 = createPodSync(ctx, pod1)

			ginkgo.By("waiting for the first pod to succeed")
			err := e2epod.WaitForPodSuccessInNamespace(ctx, f.ClientSet, pod1.Name, f.Namespace.Name)
			framework.ExpectNoError(err, "first pod failed to complete")

			pod2 := makeCPUManagerPod("gu-pod-sequential-2", containers)
			pod2.Spec.Resources = podLevelResources
			pod2.Spec.RestartPolicy = v1.RestartPolicyNever

			ginkgo.By("creating the second test pod")
			pod2 = createPodSync(ctx, pod2)

			ginkgo.By("waiting for the second pod to succeed")
			err = e2epod.WaitForPodSuccessInNamespace(ctx, f.ClientSet, pod2.Name, f.Namespace.Name)
			framework.ExpectNoError(err, "second pod failed to complete")
		})

		// 本测试校验 Pod 共享池的 CPU 集合直到 Pod 中所有容器结束才被移除。
		// 创建一个有两个共享该池的容器的 Pod，让其中一个终止，
		// 然后确保新 Pod 不会与仍在运行的容器重叠。
		// 仍在运行的容器重叠。
		ginkgo.It("should not release pod shared pool cpus while at least one container is using them", ginkgo.Label("pod-scope"), func(ctx context.Context) {
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), 3)

			currentCfg, err := getCurrentKubeletConfig(ctx)
			framework.ExpectNoError(err)
			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(currentCfg, &cpuManagerKubeletArguments{
				policyName:                     string(cpumanager.PolicyStatic),
				topologyManagerPolicy:          topologymanager.PolicyRestricted,
				topologyManagerScope:           topologymanager.PodTopologyScope,
				reservedSystemCPUs:             cpuset.CPUSet{},
				enablePodLevelResources:        true,
				enablePodLevelResourceManagers: true,
			}))

			// 设置 Pod A：一个短生命周期容器和一个长生命周期容器。
			podA := makeCPUManagerPod("pod-a-shared-pool", []ctnAttribute{
				{
					ctnName:    "short-container",
					ctnCommand: "sleep 5",
				},
				{
					ctnName:    "long-container",
					ctnCommand: "sleep 3600",
				},
			})
			podA.Spec.Resources = &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("2"),
					v1.ResourceMemory: resource.MustParse("200Mi"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("2"),
					v1.ResourceMemory: resource.MustParse("200Mi"),
				},
			}
			podA.Spec.RestartPolicy = v1.RestartPolicyNever

			ginkgo.By("creating Pod A with shared containers")
			podA = createPodSync(ctx, podA)

			ginkgo.By("waiting for long-container to be running")
			framework.ExpectNoError(e2epod.WaitForContainerRunning(ctx, f.ClientSet, podA.Namespace, podA.Name, "long-container", 2*time.Minute))

			ginkgo.By("waiting for short-container to terminate")
			framework.ExpectNoError(e2epod.WaitForContainerTerminated(ctx, f.ClientSet, podA.Namespace, podA.Name, "short-container", 2*time.Minute))

			// 即使 Pod A 中非 guaranteed 容器提前结束，Pod B 也不应访问任何 Pod 共享池资源。
			podB := makeCPUManagerPod("pod-b-attacker", []ctnAttribute{
				{
					ctnName:    "attacker-container",
					cpuRequest: "1",
					cpuLimit:   "1",
				},
			})
			podB.Spec.Resources = &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1"),
					v1.ResourceMemory: resource.MustParse("100Mi"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1"),
					v1.ResourceMemory: resource.MustParse("100Mi"),
				},
			}

			ginkgo.By("creating Pod B")
			podB = createPodSync(ctx, podB)

			ginkgo.By("verifying Pod B does NOT overlap with Pod A's long-running container")
			cpusA, err := getContainerAllowedCPUs(podA, "long-container", false)
			framework.ExpectNoError(err)

			// 校验 Pod A 的共享池大小仍然正确
			gomega.Expect(cpusA.Size()).To(gomega.Equal(2), "Pod A shared pool size should remain 2")

			cpusB, err := getContainerAllowedCPUs(podB, "attacker-container", false)
			framework.ExpectNoError(err)

			framework.Logf("Pod A (long) CPUs: %v", cpusA)
			framework.Logf("Pod B (attacker) CPUs: %v", cpusB)

			gomega.Expect(cpusA.Intersection(cpusB).IsEmpty()).To(gomega.BeTrueBecause("Pod B CPUs %v overlapped with Pod A CPUs %v", cpusB, cpusA))
		})

		// 本测试校验资源记账在 Pod 级别进行。
		// 创建两个 Pod，各自在 Pod 级别请求动态数量的 CPU，
		// 但包含三个非 guaranteed 容器。若记账不正确
		//（例如把容器资源相加），Pod 可能会被拒绝。
		// 测试确保两个 Pod 都被准入并并发运行。
		ginkgo.It("should admit multiple pods with pod-level resources and many non-guaranteed containers", ginkgo.Label("pod-scope"), func(ctx context.Context) {
			node := getLocalNode(ctx, f)
			nodeCPUDetails := cpuDetailsFromNode(node)
			// 至少需要 2 个 CPU 才能并发运行两个 Pod。
			if nodeCPUDetails.Allocatable < 2 {
				e2eskipper.Skipf("Skipping test: requires at least 2 allocatable CPUs, but has %d", int(nodeCPUDetails.Allocatable))
			}

			// 每个 Pod 请求一半的可分配 CPU。
			cpuCountPerPod := int(nodeCPUDetails.Allocatable / 2)
			framework.Logf("Node has %d allocatable CPUs. Requesting %d for each of the 2 pods.", int(nodeCPUDetails.Allocatable), cpuCountPerPod)
			skipIfAllocatableCPUsLessThan(node, cpuCountPerPod*2)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:                     string(cpumanager.PolicyStatic),
				topologyManagerPolicy:          topologymanager.PolicyRestricted,
				topologyManagerScope:           topologymanager.PodTopologyScope,
				reservedSystemCPUs:             cpuset.New(0),
				enablePodLevelResources:        true,
				enablePodLevelResourceManagers: true,
			}))

			podRequest := strconv.Itoa(cpuCountPerPod)
			podMemRequest := "400Mi"

			makeTestPod := func(name string) *v1.Pod {
				pod := makeCPUManagerPod(name, []ctnAttribute{
					{ctnName: "ngu-container-1", ctnCommand: "sleep 1d"},
					{ctnName: "ngu-container-2", ctnCommand: "sleep 1d"},
					{ctnName: "ngu-container-3", ctnCommand: "sleep 1d"},
				})
				pod.Spec.Resources = &v1.ResourceRequirements{
					Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse(podRequest), v1.ResourceMemory: resource.MustParse(podMemRequest)},
					Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse(podRequest), v1.ResourceMemory: resource.MustParse(podMemRequest)},
				}
				return pod
			}

			pod1 := makeTestPod("concurrent-pod-1")
			pod2 := makeTestPod("concurrent-pod-2")

			ginkgo.By("creating two concurrent pods")
			pod1 = createPodSync(ctx, pod1)
			pod2 = createPodSync(ctx, pod2)

			ginkgo.By("waiting for both pods to be running")
			framework.ExpectNoError(e2epod.WaitForPodRunningInNamespace(ctx, f.ClientSet, pod1))
			framework.ExpectNoError(e2epod.WaitForPodRunningInNamespace(ctx, f.ClientSet, pod2))

			ginkgo.By("verifying CPU allocation for both pods")
			gomega.Expect(pod1).To(HavePodExclusiveCPUs(cpuCountPerPod))
			gomega.Expect(pod2).To(HavePodExclusiveCPUs(cpuCountPerPod))

			ginkgo.By("verifying CPU sets do not overlap")

			// 只需检查每个 Pod 的一个容器，因为 Pod 中所有容器共享同一 Pod 级 cpuset。
			cpus1, err := getContainerAllowedCPUs(pod1, "ngu-container-1", false)
			framework.ExpectNoError(err)
			cpus2, err := getContainerAllowedCPUs(pod2, "ngu-container-1", false)
			framework.ExpectNoError(err)
			gomega.Expect(cpus1.Intersection(cpus2).IsEmpty()).To(gomega.BeTrueBecause("CPU sets of the two pods should not overlap"))
		})

		ginkgo.It("should not maintain CPU quota for a pod with pod-level resources and guaranteed containers in Pod scope, PodLevelResourceManagers enabled", ginkgo.Label("pod-scope"), func(ctx context.Context) {
			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:                     string(cpumanager.PolicyStatic),
				reservedSystemCPUs:             cpuset.CPUSet{},
				topologyManagerPolicy:          topologymanager.PolicyRestricted,
				topologyManagerScope:           topologymanager.PodTopologyScope,
				enablePodLevelResources:        true,
				enablePodLevelResourceManagers: true,
			}))

			pod := makeCPUManagerPod("gu-pod-level-resources-quota", []ctnAttribute{
				{
					ctnName:    "gu-container",
					cpuRequest: "1",
					cpuLimit:   "1",
				},
			})
			pod.Spec.Resources = &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1"),
					v1.ResourceMemory: resource.MustParse("100Mi"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1"),
					v1.ResourceMemory: resource.MustParse("100Mi"),
				},
			}
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the CPU quota is disabled")
			gomega.Expect(pod).To(HaveSandboxQuota("max"))
			gomega.Expect(pod).To(HaveContainerQuota("gu-container", "max"))
		})

		ginkgo.It("should maintain CPU quota for a pod with pod-level resources and non-guaranteed containers in Pod scope, PodLevelResourceManagers enabled", ginkgo.Label("pod-scope"), func(ctx context.Context) {
			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:                     string(cpumanager.PolicyStatic),
				reservedSystemCPUs:             cpuset.CPUSet{},
				topologyManagerPolicy:          topologymanager.PolicyRestricted,
				topologyManagerScope:           topologymanager.PodTopologyScope,
				enablePodLevelResources:        true,
				enablePodLevelResourceManagers: true,
			}))

			pod := makeCPUManagerPod("gu-pod-level-resources-quota", []ctnAttribute{
				{
					ctnName:    "ngu-container",
					cpuRequest: "0",
					cpuLimit:   "1",
				},
			})
			pod.Spec.Resources = &v1.ResourceRequirements{
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1"),
					v1.ResourceMemory: resource.MustParse("100Mi"),
				},
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1"),
					v1.ResourceMemory: resource.MustParse("100Mi"),
				},
			}
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the CPU quota is enabled")
			gomega.Expect(pod).To(HaveSandboxQuota("100000"))
			gomega.Expect(pod).To(HaveContainerQuota("ngu-container", "100000"))
		})
	})

	ginkgo.Context("when the topology manager scope is 'container'", func() {
		ginkgo.It("should allocate exclusive CPUs to a guaranteed pod with pod-level resources and guaranteed container, PodLevelResourceManagers enabled", ginkgo.Label("container-scope"), func(ctx context.Context) {
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), 1)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:                     string(cpumanager.PolicyStatic),
				topologyManagerPolicy:          topologymanager.PolicyRestricted,
				topologyManagerScope:           topologymanager.ContainerTopologyScope,
				reservedSystemCPUs:             cpuset.CPUSet{},
				enablePodLevelResources:        true,
				enablePodLevelResourceManagers: true,
			}))

			pod := makeCPUManagerPod("gu-pod-level-gu-ctn-ctn-scope", []ctnAttribute{
				{
					ctnName:    "gu-container",
					cpuRequest: "1",
					cpuLimit:   "1",
				},
			})
			pod.Spec.Resources = &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1"),
					v1.ResourceMemory: resource.MustParse("100Mi"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1"),
					v1.ResourceMemory: resource.MustParse("100Mi"),
				},
			}
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the expected cpuset was assigned")

			gomega.Expect(pod).To(HaveContainerCPUsCount("gu-container", 1))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("gu-container", onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("gu-container", reservedCPUs))
		})

		ginkgo.It("should not allocate exclusive CPUs to a guaranteed pod with pod-level resources and non-guaranteed containers, PodLevelResourceManagers enabled", ginkgo.Label("container-scope"), func(ctx context.Context) {
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), 1)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:                     string(cpumanager.PolicyStatic),
				topologyManagerPolicy:          topologymanager.PolicyRestricted,
				topologyManagerScope:           topologymanager.ContainerTopologyScope,
				reservedSystemCPUs:             cpuset.CPUSet{},
				enablePodLevelResources:        true,
				enablePodLevelResourceManagers: true,
			}))

			pod := makeCPUManagerPod("gu-pod-level-ngu-ctn-ctn-scope", []ctnAttribute{
				{
					ctnName: "ngu-container",
				},
			})
			pod.Spec.Resources = &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1"),
					v1.ResourceMemory: resource.MustParse("100Mi"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1"),
					v1.ResourceMemory: resource.MustParse("100Mi"),
				},
			}
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the expected cpuset was assigned")

			gomega.Expect(pod).To(HaveContainerCPUsEqualTo("ngu-container", onlineCPUs))
		})

		ginkgo.It("should allocate exclusive CPUs to a guaranteed pod with pod-level resources and mix of guaranteed and non-guaranteed containers, PodLevelResourceManagers enabled", ginkgo.Label("container-scope"), func(ctx context.Context) {
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), 1)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:                     string(cpumanager.PolicyStatic),
				topologyManagerPolicy:          topologymanager.PolicyRestricted,
				topologyManagerScope:           topologymanager.ContainerTopologyScope,
				reservedSystemCPUs:             cpuset.CPUSet{},
				enablePodLevelResources:        true,
				enablePodLevelResourceManagers: true,
			}))

			pod := makeCPUManagerPod("gu-pod-level-mix-ctn-ctn-scope", []ctnAttribute{
				{
					ctnName:    "gu-container",
					cpuRequest: "1",
					cpuLimit:   "1",
				},
				{
					ctnName: "ngu-container",
				},
			})
			pod.Spec.Resources = &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1"),
					v1.ResourceMemory: resource.MustParse("300Mi"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1"),
					v1.ResourceMemory: resource.MustParse("300Mi"),
				},
			}
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the expected cpuset was assigned")

			gomega.Expect(pod).To(HaveContainerCPUsCount("gu-container", 1))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("gu-container", onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("gu-container", reservedCPUs))

			exclusiveCPUs, err := getContainerAllowedCPUs(pod, "gu-container", false)
			framework.ExpectNoError(err)
			expectedSharedCPUs := onlineCPUs.Difference(exclusiveCPUs)
			gomega.Expect(pod).To(HaveContainerCPUsEqualTo("ngu-container", expectedSharedCPUs))
		})

		ginkgo.It("should allocate exclusive CPUs to a guaranteed pod with pod-level resources and mix of guaranteed and non-guaranteed standard and init containers, PodLevelResourceManagers enabled", ginkgo.Label("container-scope"), func(ctx context.Context) {
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), 2)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:                     string(cpumanager.PolicyStatic),
				topologyManagerPolicy:          topologymanager.PolicyRestricted,
				topologyManagerScope:           topologymanager.ContainerTopologyScope,
				reservedSystemCPUs:             cpuset.CPUSet{},
				enablePodLevelResources:        true,
				enablePodLevelResourceManagers: true,
			}))

			pod := makeCPUManagerPod("gu-pod-level-mix-init-ctn-ctn-scope", []ctnAttribute{
				{
					ctnName:    "gu-container",
					cpuRequest: "1",
					cpuLimit:   "1",
				},
				{
					ctnName: "ngu-container",
				},
			})
			pod.Spec.InitContainers = []v1.Container{
				{
					Name:    "gu-init-container",
					Image:   busyboxImage,
					Command: []string{"sh", "-c", "echo hello"},
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("1"),
							v1.ResourceMemory: resource.MustParse("100Mi"),
						},
						Limits: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("1"),
							v1.ResourceMemory: resource.MustParse("100Mi"),
						},
					},
				},
			}
			pod.Spec.Resources = &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("2"),
					v1.ResourceMemory: resource.MustParse("200Mi"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("2"),
					v1.ResourceMemory: resource.MustParse("200Mi"),
				},
			}
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the expected cpuset was assigned")

			gomega.Expect(pod).To(HaveContainerCPUsCount("gu-container", 1))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("gu-container", onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("gu-container", reservedCPUs))

			exclusiveCPUs, err := getContainerAllowedCPUs(pod, "gu-container", false)
			framework.ExpectNoError(err)
			expectedSharedCPUs := onlineCPUs.Difference(exclusiveCPUs)
			gomega.Expect(pod).To(HaveContainerCPUsEqualTo("ngu-container", expectedSharedCPUs))
		})

		ginkgo.It("should not allocate exclusive CPUs to a non-guaranteed pod with pod-level resources and guaranteed containers, PodLevelResourceManagers enabled", ginkgo.Label("container-scope"), func(ctx context.Context) {
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), 2)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:                     string(cpumanager.PolicyStatic),
				topologyManagerPolicy:          topologymanager.PolicyRestricted,
				topologyManagerScope:           topologymanager.ContainerTopologyScope,
				reservedSystemCPUs:             cpuset.CPUSet{},
				enablePodLevelResources:        true,
				enablePodLevelResourceManagers: true,
			}))

			pod := makeCPUManagerPod("ngu-pod-level-gu-ctn-ctn-scope", []ctnAttribute{
				{
					ctnName:    "gu-container",
					cpuRequest: "1",
					cpuLimit:   "1",
				},
			})
			pod.Spec.Resources = &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("1"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("2"),
				},
			}
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the expected cpuset was assigned")

			gomega.Expect(pod).To(HaveContainerCPUsEqualTo("gu-container", onlineCPUs))
		})

		ginkgo.It("should not allocate exclusive CPUs to a non-guaranteed pod with pod-level resources and non-guaranteed containers, PodLevelResourceManagers enabled", ginkgo.Label("container-scope"), func(ctx context.Context) {
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), 1)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:                     string(cpumanager.PolicyStatic),
				topologyManagerPolicy:          topologymanager.PolicyRestricted,
				topologyManagerScope:           topologymanager.ContainerTopologyScope,
				reservedSystemCPUs:             cpuset.CPUSet{},
				enablePodLevelResources:        true,
				enablePodLevelResourceManagers: true,
			}))

			pod := makeCPUManagerPod("ngu-pod-level-ngu-ctn-ctn-scope", []ctnAttribute{
				{
					ctnName: "ngu-container",
				},
			})
			pod.Spec.Resources = &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("1"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("2"),
				},
			}
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the expected cpuset was assigned")

			gomega.Expect(pod).To(HaveContainerCPUsEqualTo("ngu-container", onlineCPUs))
		})

		ginkgo.It("should not reject a pod that would result in an empty pod shared pool, no pod shared pool in container scope", ginkgo.Label("container-scope"), func(ctx context.Context) {
			skipIfAllocatableCPUsLessThan(getLocalNode(ctx, f), 2)

			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:                     string(cpumanager.PolicyStatic),
				topologyManagerPolicy:          topologymanager.PolicyRestricted,
				topologyManagerScope:           topologymanager.ContainerTopologyScope,
				reservedSystemCPUs:             cpuset.CPUSet{},
				enablePodLevelResources:        true,
				enablePodLevelResourceManagers: true,
			}))

			pod := makeCPUManagerPod("gu-pod-level-empty-shared-ctn-scope", []ctnAttribute{
				{
					ctnName:    "gu-container-1",
					cpuRequest: "1",
					cpuLimit:   "1",
				},
				{
					ctnName:    "gu-container-2",
					cpuRequest: "1",
					cpuLimit:   "1",
				},
				{
					ctnName: "ngu-container",
				},
			})
			pod.Spec.Resources = &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("2"),
					v1.ResourceMemory: resource.MustParse("300Mi"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("2"),
					v1.ResourceMemory: resource.MustParse("300Mi"),
				},
			}
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the expected cpuset was assigned")

			gomega.Expect(pod).To(HaveContainerCPUsCount("gu-container-1", 1))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("gu-container-1", onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("gu-container-1", reservedCPUs))

			gomega.Expect(pod).To(HaveContainerCPUsCount("gu-container-2", 1))
			gomega.Expect(pod).To(HaveContainerCPUsASubsetOf("gu-container-2", onlineCPUs))
			gomega.Expect(pod).ToNot(HaveContainerCPUsOverlapWith("gu-container-2", reservedCPUs))

			exclusiveCPUs1, err := getContainerAllowedCPUs(pod, "gu-container-1", false)
			framework.ExpectNoError(err)
			exclusiveCPUs2, err := getContainerAllowedCPUs(pod, "gu-container-2", false)
			framework.ExpectNoError(err)
			exclusiveCPUs := exclusiveCPUs1.Union(exclusiveCPUs2)
			expectedSharedCPUs := onlineCPUs.Difference(exclusiveCPUs)
			gomega.Expect(pod).To(HaveContainerCPUsEqualTo("ngu-container", expectedSharedCPUs))
		})

		ginkgo.It("should not maintain CPU quota for a pod with pod-level resources and guaranteed containers in Container scope, PodLevelResourceManagers enabled", ginkgo.Label("container-scope"), func(ctx context.Context) {
			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:                     string(cpumanager.PolicyStatic),
				reservedSystemCPUs:             cpuset.CPUSet{},
				topologyManagerPolicy:          topologymanager.PolicyRestricted,
				topologyManagerScope:           topologymanager.ContainerTopologyScope,
				enablePodLevelResources:        true,
				enablePodLevelResourceManagers: true,
			}))

			pod := makeCPUManagerPod("gu-pod-level-resources-quota", []ctnAttribute{
				{
					ctnName:    "gu-container",
					cpuRequest: "1",
					cpuLimit:   "1",
				},
			})
			pod.Spec.Resources = &v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1"),
					v1.ResourceMemory: resource.MustParse("100Mi"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1"),
					v1.ResourceMemory: resource.MustParse("100Mi"),
				},
			}
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the CPU quota is disabled")
			gomega.Expect(pod).To(HaveSandboxQuota("max"))
			gomega.Expect(pod).To(HaveContainerQuota("gu-container", "max"))
		})

		ginkgo.It("should maintain CPU quota for a pod with pod-level resources and non-guaranteed containers in Container scope, PodLevelResourceManagers enabled", ginkgo.Label("container-scope"), func(ctx context.Context) {
			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:                     string(cpumanager.PolicyStatic),
				reservedSystemCPUs:             cpuset.CPUSet{},
				topologyManagerPolicy:          topologymanager.PolicyRestricted,
				topologyManagerScope:           topologymanager.ContainerTopologyScope,
				enablePodLevelResources:        true,
				enablePodLevelResourceManagers: true,
			}))

			pod := makeCPUManagerPod("gu-pod-level-resources-quota", []ctnAttribute{
				{
					ctnName:    "ngu-container",
					cpuRequest: "0",
					cpuLimit:   "1",
				},
			})
			pod.Spec.Resources = &v1.ResourceRequirements{
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1"),
					v1.ResourceMemory: resource.MustParse("100Mi"),
				},
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1"),
					v1.ResourceMemory: resource.MustParse("100Mi"),
				},
			}
			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the CPU quota is enabled")
			gomega.Expect(pod).To(HaveSandboxQuota("100000"))
			gomega.Expect(pod).To(HaveContainerQuota("ngu-container", "100000"))
		})

		ginkgo.It("should run on the shared CPU pool for a pod with pod-level resources when PodLevelResourceManagers is disabled and no reserved system CPUs", func(ctx context.Context) {
			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:                     string(cpumanager.PolicyStatic),
				reservedSystemCPUs:             cpuset.CPUSet{},
				enablePodLevelResources:        true,
				enablePodLevelResourceManagers: false,
			}))

			pod := makeCPUManagerPod("gu-pod-level-resources", []ctnAttribute{
				{
					ctnName:    "gu-container",
					cpuRequest: "1",
					cpuLimit:   "1",
				},
			})
			pod.Spec.Resources = &v1.ResourceRequirements{
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1"),
					v1.ResourceMemory: resource.MustParse("100Mi"),
				},
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1"),
					v1.ResourceMemory: resource.MustParse("100Mi"),
				},
			}

			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the expected cpuset was assigned")
			gomega.Expect(pod).To(HaveContainerCPUsEqualTo("gu-container", onlineCPUs))
		})

		ginkgo.It("should run on the shared CPU pool for a pod with pod-level resources when PodLevelResourceManagers is disabled and with reserved system CPUs", func(ctx context.Context) {
			updateKubeletConfigIfNeeded(ctx, f, configureCPUManagerInKubelet(oldCfg, &cpuManagerKubeletArguments{
				policyName:                     string(cpumanager.PolicyStatic),
				reservedSystemCPUs:             cpuset.New(0), // Not really needed for the tests but helps to make a more precise check
				enablePodLevelResources:        true,
				enablePodLevelResourceManagers: false,
			}))

			pod := makeCPUManagerPod("gu-pod-level-resources", []ctnAttribute{
				{
					ctnName:    "gu-container",
					cpuRequest: "1",
					cpuLimit:   "1",
				},
			})
			pod.Spec.Resources = &v1.ResourceRequirements{
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1"),
					v1.ResourceMemory: resource.MustParse("100Mi"),
				},
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("1"),
					v1.ResourceMemory: resource.MustParse("100Mi"),
				},
			}

			ginkgo.By("creating the test pod")
			pod = createPodSync(ctx, pod)

			ginkgo.By("checking if the expected cpuset was assigned")
			gomega.Expect(pod).To(HaveContainerCPUsEqualTo("gu-container", onlineCPUs))
		})
	})
})

// 匹配的 helper

func HaveStatusReasonMatchingRegex(expr string) types.GomegaMatcher {
	return gcustom.MakeMatcher(func(actual *v1.Pod) (bool, error) {
		re, err := regexp.Compile(expr)
		if err != nil {
			return false, err
		}
		return re.MatchString(actual.Status.Reason), nil
	}).WithTemplate("Pod {{.Actual.Namespace}}/{{.Actual.Name}} UID {{.Actual.UID}} reason {{.Actual.Status.Reason}} does not match regexp {{.Data}}", expr)
}

type msgData struct {
	Name             string
	CurrentCPUs      string
	ExpectedCPUs     string
	MismatchedCPUs   string
	UncoreCacheAlign string
	Count            int
	Aligned          int
	CurrentQuota     string
	ExpectedQuota    string
}

func HaveContainerCPUsCount(ctnName string, val int) types.GomegaMatcher {
	md := &msgData{
		Name:  ctnName,
		Count: val,
	}
	return gcustom.MakeMatcher(func(actual *v1.Pod) (bool, error) {
		cpus, err := getContainerAllowedCPUs(actual, ctnName, false)
		md.CurrentCPUs = cpus.String()
		if err != nil {
			framework.Logf("getContainerAllowedCPUs(%s) failed: %v", ctnName, err)
			return false, err
		}
		return cpus.Size() == val, nil
	}).WithTemplate("Pod {{.Actual.Namespace}}/{{.Actual.Name}} UID {{.Actual.UID}} has allowed CPUs <{{.Data.CurrentCPUs}}> not matching expected count <{{.Data.Count}}> for container {{.Data.Name}}", md)
}

func HaveContainerCPUsAlignedTo(ctnName string, val int) types.GomegaMatcher {
	md := &msgData{
		Name:    ctnName,
		Aligned: val,
	}
	return gcustom.MakeMatcher(func(actual *v1.Pod) (bool, error) {
		cpus, err := getContainerAllowedCPUs(actual, ctnName, false)
		md.CurrentCPUs = cpus.String()
		if err != nil {
			framework.Logf("getContainerAllowedCPUs(%s) failed: %v", ctnName, err)
			return false, err
		}
		return cpus.Size()%val == 0, nil
	}).WithTemplate("Pod {{.Actual.Namespace}}/{{.Actual.Name}} UID {{.Actual.UID}} has allowed CPUs <{{.Data.CurrentCPUs}}> not aligned to value <{{.Data.Aligned}}> for container {{.Data.Name}}", md)
}

func HaveContainerCPUsOverlapWith(ctnName string, ref cpuset.CPUSet) types.GomegaMatcher {
	md := &msgData{
		Name:         ctnName,
		ExpectedCPUs: ref.String(),
	}
	return gcustom.MakeMatcher(func(actual *v1.Pod) (bool, error) {
		cpus, err := getContainerAllowedCPUs(actual, ctnName, false)
		md.CurrentCPUs = cpus.String()
		if err != nil {
			framework.Logf("getContainerAllowedCPUs(%s) failed: %v", ctnName, err)
			return false, err
		}
		sharedCPUs := cpus.Intersection(ref)
		return sharedCPUs.Size() > 0, nil
	}).WithTemplate("Pod {{.Actual.Namespace}}/{{.Actual.Name}} UID {{.Actual.UID}} has allowed CPUs <{{.Data.CurrentCPUs}}> overlapping with expected CPUs <{{.Data.ExpectedCPUs}}> for container {{.Data.Name}}", md)
}

func HaveContainerCPUsOverlap(containerName1, containerName2 string) types.GomegaMatcher {
	return gcustom.MakeMatcher(func(pod *v1.Pod) (bool, error) {
		cpus1, err := getContainerAllowedCPUs(pod, containerName1, false)
		if err != nil {
			return false, err
		}
		cpus2, err := getContainerAllowedCPUs(pod, containerName2, false)
		if err != nil {
			return false, err
		}

		return !cpus1.Intersection(cpus2).IsEmpty(), nil
	})
}

func HaveContainerCPUsASubsetOf(ctnName string, ref cpuset.CPUSet) types.GomegaMatcher {
	md := &msgData{
		Name:         ctnName,
		ExpectedCPUs: ref.String(),
	}
	return gcustom.MakeMatcher(func(actual *v1.Pod) (bool, error) {
		cpus, err := getContainerAllowedCPUs(actual, ctnName, false)
		md.CurrentCPUs = cpus.String()
		if err != nil {
			framework.Logf("getContainerAllowedCPUs(%s) failed: %v", ctnName, err)
			return false, err
		}
		return cpus.IsSubsetOf(ref), nil
	}).WithTemplate("Pod {{.Actual.Namespace}}/{{.Actual.Name}} UID {{.Actual.UID}} has allowed CPUs <{{.Data.CurrentCPUs}}> not a subset of expected CPUs <{{.Data.ExpectedCPUs}}> for container {{.Data.Name}}", md)
}

func HaveContainerCPUsEqualTo(ctnName string, expectedCPUs cpuset.CPUSet) types.GomegaMatcher {
	md := &msgData{
		Name:         ctnName,
		ExpectedCPUs: expectedCPUs.String(),
	}
	return gcustom.MakeMatcher(func(actual *v1.Pod) (bool, error) {
		cpus, err := getContainerAllowedCPUs(actual, ctnName, false)
		md.CurrentCPUs = cpus.String()
		if err != nil {
			framework.Logf("getContainerAllowedCPUs(%s) failed: %v", ctnName, err)
			return false, err
		}
		return cpus.Equals(expectedCPUs), nil
	}).WithTemplate("Pod {{.Actual.Namespace}}/{{.Actual.Name}} UID {{.Actual.UID}} has allowed CPUs <{{.Data.CurrentCPUs}}> not matching the expected value <{{.Data.ExpectedCPUs}}> for container {{.Data.Name}}", md)
}

func HaveSandboxQuotaWithPeriod(expectedQuota, cfsPeriod string) types.GomegaMatcher {
	md := &msgData{}
	return gcustom.MakeMatcher(func(actual *v1.Pod) (bool, error) {
		md.Name = klog.KObj(actual).String()
		quota, err := getSandboxCFSQuota(actual)
		md.CurrentQuota = quota
		if err != nil {
			framework.Logf("getSandboxCFSQuota() failed: %v", err)
			return false, err
		}
		md.ExpectedQuota = fmt.Sprintf("^%s %s$", expectedQuota, cfsPeriod)
		re, err := regexp.Compile(md.ExpectedQuota)
		if err != nil {
			return false, err
		}
		return re.MatchString(quota), nil
	}).WithTemplate("Pod {{.Actual.Namespace}}/{{.Actual.Name}} UID {{.Actual.UID}} has quota <{{.Data.CurrentQuota}}> not matching expected value <{{.Data.ExpectedQuota}}>", md)
}

func HaveContainerQuotaWithPeriod(ctnName, expectedQuota, cfsPeriod string) types.GomegaMatcher {
	md := &msgData{
		Name: ctnName,
	}
	return gcustom.MakeMatcher(func(actual *v1.Pod) (bool, error) {
		quota, err := getContainerCFSQuota(actual, ctnName, false)
		md.CurrentQuota = quota
		if err != nil {
			framework.Logf("getContainerCFSQuota(%s) failed: %v", ctnName, err)
			return false, err
		}
		md.ExpectedQuota = fmt.Sprintf("^%s %s$", expectedQuota, cfsPeriod)
		re, err := regexp.Compile(md.ExpectedQuota)
		if err != nil {
			return false, err
		}
		return re.MatchString(quota), nil
	}).WithTemplate("Pod {{.Actual.Namespace}}/{{.Actual.Name}} UID {{.Actual.UID}} has quota <{{.Data.CurrentQuota}}> not matching expected value <{{.Data.ExpectedQuota}}> for container {{.Data.Name}}", md)
}

func HaveSandboxQuota(expectedQuota string) types.GomegaMatcher {
	return HaveSandboxQuotaWithPeriod(expectedQuota, defaultCFSPeriod)
}

func HaveContainerQuota(ctnName, expectedQuota string) types.GomegaMatcher {
	return HaveContainerQuotaWithPeriod(ctnName, expectedQuota, defaultCFSPeriod)
}

func HaveContainerCPUsThreadSiblings(ctnName string) types.GomegaMatcher {
	md := &msgData{
		Name: ctnName,
	}
	return gcustom.MakeMatcher(func(actual *v1.Pod) (bool, error) {
		cpus, err := getContainerAllowedCPUs(actual, ctnName, false)
		md.CurrentCPUs = cpus.String()
		if err != nil {
			framework.Logf("getContainerAllowedCPUs(%s) failed: %v", ctnName, err)
			return false, err
		}
		expectedCPUs := makeThreadSiblingCPUSet(cpus)
		md.ExpectedCPUs = expectedCPUs.String()
		return cpus.Equals(expectedCPUs), nil
	}).WithTemplate("Pod {{.Actual.Namespace}}/{{.Actual.Name}} UID {{.Actual.UID}} has allowed CPUs <{{.Data.CurrentCPUs}}> not all thread sibling pairs (would be <{{.Data.ExpectedCPUs}}>) for container {{.Data.Name}}", md)
}

func HaveContainerCPUsQuasiThreadSiblings(ctnName string, toleration int) types.GomegaMatcher {
	md := &msgData{
		Name:  ctnName,
		Count: toleration,
	}
	return gcustom.MakeMatcher(func(actual *v1.Pod) (bool, error) {
		cpus, err := getContainerAllowedCPUs(actual, ctnName, false)
		md.CurrentCPUs = cpus.String()
		if err != nil {
			framework.Logf("getContainerAllowedCPUs(%s) failed: %v", ctnName, err)
			return false, err
		}
		// 按构造这 >= cpus（极端情况：cpus 由所有非线程兄弟组成）
		expectedCPUs := makeThreadSiblingCPUSet(cpus)
		md.ExpectedCPUs = expectedCPUs.String()
		mismatchedCPUs := expectedCPUs.Difference(cpus)
		md.MismatchedCPUs = mismatchedCPUs.String()
		return mismatchedCPUs.Size() <= toleration, nil
	}).WithTemplate("Pod {{.Actual.Namespace}}/{{.Actual.Name}} UID {{.Actual.UID}} has allowed CPUs <{{.Data.CurrentCPUs}}> not all thread sibling pairs (would be <{{.Data.ExpectedCPUs}}> mismatched <{{.Data.MismatchedCPUs}}> toleration <{{.Data.Count}}>) for container {{.Data.Name}}", md)
}

func HaveContainerCPUsWithSameUncoreCacheID(ctnName string) types.GomegaMatcher {
	md := &msgData{
		Name: ctnName,
	}
	return gcustom.MakeMatcher(func(actual *v1.Pod) (bool, error) {
		cpus, err := getContainerAllowedCPUs(actual, ctnName, false)
		if err != nil {
			return false, fmt.Errorf("getContainerAllowedCPUs(%s) failed: %w", ctnName, err)
		}
		md.CurrentCPUs = cpus.String()

		var commonCacheID *int64

		for _, cpu := range cpus.List() {
			// 确定每个 CPU 的 Uncore Cache ID
			uncoreID, err := uncoreCacheIDFromSysFS(cpu)
			if err != nil {
				return false, fmt.Errorf("failed to read cache ID for CPU %d: %w", cpu, err)
			}

			// 如果这是检查的第一个 CPU，把它的 Uncore Cache ID 设为参考值
			// 后续 CPU 则把 Uncore Cache ID 与参考值比较
			if commonCacheID == nil {
				commonCacheID = &uncoreID
			} else if *commonCacheID != uncoreID {
				md.UncoreCacheAlign = fmt.Sprintf("shared uncoreID mismatch: CPU %d has uncoreID %d, CPUSet has uncoreID %d", cpu, uncoreID, *commonCacheID)
				return false, nil
			}
		}

		// 所有 CPU 的 cache ID 一致
		md.UncoreCacheAlign = fmt.Sprintf("all CPUs share cache ID %d", *commonCacheID)
		return true, nil
	}).WithTemplate(
		"Pod {{.Actual.Namespace}}/{{.Actual.Name}} UID {{.Actual.UID}} container {{.Data.Name}} has CPUSet <{{.Data.CurrentCPUs}}> where not all CPUs share the same uncore cache ID: {{.Data.UncoreCacheAlign}}",
		md,
	)
}

func HaveContainerCPUsShareUncoreCacheWith(ctnName string, ref cpuset.CPUSet) types.GomegaMatcher {
	md := &msgData{
		Name:         ctnName,
		ExpectedCPUs: ref.String(),
	}
	return gcustom.MakeMatcher(func(actual *v1.Pod) (bool, error) {
		containerCPUs, err := getContainerAllowedCPUs(actual, ctnName, false)
		if err != nil {
			return false, fmt.Errorf("getContainerAllowedCPUs(%s) failed: %w", ctnName, err)
		}
		md.CurrentCPUs = containerCPUs.String()

		// 从参考 cpuset 构建 uncore cache ID 集合
		refUncoreIDs := sets.New[int64]()
		for _, cpu := range ref.UnsortedList() {
			uncoreID, err := uncoreCacheIDFromSysFS(cpu)
			if err != nil {
				return false, fmt.Errorf("failed to read uncore cache ID for reference CPU %d: %w", cpu, err)
			}
			refUncoreIDs.Insert(uncoreID)
		}

		// 检查是否有容器 CPU 与参考集合共享 uncore ID
		for _, cpu := range containerCPUs.UnsortedList() {
			uncoreID, err := uncoreCacheIDFromSysFS(cpu)
			if err != nil {
				return false, fmt.Errorf("failed to read uncore cache ID for container CPU %d: %w", cpu, err)
			}
			if refUncoreIDs.Has(uncoreID) {
				md.UncoreCacheAlign = fmt.Sprintf("%d", uncoreID)
				return true, nil
			}
		}

		return false, nil
	}).WithTemplate(
		"Pod {{.Actual.Namespace}}/{{.Actual.Name}} UID {{.Actual.UID}} container {{.Data.Name}} has CPUSet <{{.Data.CurrentCPUs}}> sharing uncoreCache ID <{{.Data.UncoreCacheAlign}}> with reference CPUSet <{{.Data.ExpectedCPUs}}>",
		md,
	)
}

// HavePodExclusiveCPUs 做 Pod 级 CPU 分配校验：校验
// Pod 中所有容器被分配的唯一 CPU 总数等于预期值。
// 预期值。
func HavePodExclusiveCPUs(expectedTotalCPUs int) types.GomegaMatcher {
	return gcustom.MakeMatcher(func(pod *v1.Pod) (bool, error) {
		allCPUSets := make(map[string]cpuset.CPUSet)

		// 对容器分类并获取它们的 CPU 集合
		for _, container := range pod.Spec.Containers {
			cpus, err := getContainerAllowedCPUs(pod, container.Name, false)
			if err != nil {
				return false, fmt.Errorf("failed to get CPUs for container %s: %w", container.Name, err)
			}

			allCPUSets[container.Name] = cpus
		}

		// 检查 Pod 的总 CPU 占用
		totalPodCPUs := cpuset.New()
		for _, cpus := range allCPUSets {
			totalPodCPUs = totalPodCPUs.Union(cpus)
		}
		if totalPodCPUs.Size() != expectedTotalCPUs {
			return false, fmt.Errorf("expected pod to have a total of %d exclusive CPUs, but it has %d (%s)", expectedTotalCPUs, totalPodCPUs.Size(), totalPodCPUs.String())
		}

		return true, nil
	}).WithMessage("to have exclusive CPUs assigned correctly at the pod level")
}

// 用于检查 CPU 是否 packed 的自定义 matcher。
func BePackedCPUs() types.GomegaMatcher {
	return gcustom.MakeMatcher(func(allocatedCPUs cpuset.CPUSet) (bool, error) {
		distribution := computeNUMADistribution(allocatedCPUs)
		for _, count := range distribution {
			// 这个假设在单个 NUMA 节点有足够 CPU 时成立。
			// 我们有意把 CPU 请求限制为 2，以尽量减少满足该场景所需的 CPU 数，
			// 从而最大化正确校验该场景的机会。
			//
			if count == allocatedCPUs.Size() {
				return true, nil
			}
		}
		return false, nil
	}).WithMessage("expected CPUs to be packed")
}

// 用于检查 CPU 是否 distributed 的自定义 matcher。
func BeDistributedCPUs(expectedSpread int) types.GomegaMatcher {
	return gcustom.MakeMatcher(func(allocatedCPUs cpuset.CPUSet) (bool, error) {
		counts, err := numaAllocationCounts(allocatedCPUs, false)
		if err != nil {
			return false, err
		}
		for _, count := range counts {
			if count != expectedSpread {
				return false, nil
			}
		}
		return true, nil
	}).WithTemplate("expected CPUs to be evenly distributed across NUMA nodes\nExpected: {{.Data}}\nGot:\n{{.FormattedActual}}\nDistribution: {{.Data}}\n").WithTemplateData(expectedSpread)
}

// 其他 helper

func numaAllocationCounts(allocatedCPUs cpuset.CPUSet, activeOnly bool) ([]int, error) {
	distribution := computeNUMADistribution(allocatedCPUs)
	counts := make([]int, 0, len(distribution))
	for _, count := range distribution {
		if activeOnly && count == 0 {
			continue
		}
		counts = append(counts, count)
	}
	if len(counts) == 0 {
		return nil, fmt.Errorf("no CPUs allocated on any NUMA node")
	}
	return counts, nil
}

func hostCheckForSocketPolicyOptions() (numaNodes, sockets, numaPerSocket, cpusNumPerNUMA int) {
	numaNodes, _, _, cpusNumPerNUMA = hostCheck()
	sockets = detectSockets()
	if sockets < 2 {
		e2eskipper.Skipf("align-by-socket policy option tests require a multi-socket system")
	}
	// 假定 socket 到 NUMA node 的层级是干净的（一个 socket 多个 NUMA node，而不是反过来）。
	// 不满足时跳过，例如单个 NUMA node 跨多个 socket。
	numaPerSocket = numaNodes / sockets
	if numaPerSocket < 2 {
		e2eskipper.Skipf("align-by-socket policy option tests require at least 2 NUMA nodes per socket")
	}
	return numaNodes, sockets, numaPerSocket, cpusNumPerNUMA
}

func BeAllocatedToSingleSocket() types.GomegaMatcher {
	return gcustom.MakeMatcher(func(allocatedCPUs cpuset.CPUSet) (bool, error) {
		if allocatedCPUs.IsEmpty() {
			return false, fmt.Errorf("empty cpuset")
		}
		socketIDs := sets.New[int]()
		for _, cpu := range allocatedCPUs.UnsortedList() {
			socketID, err := getCPUSocketID(cpu)
			if err != nil {
				return false, err
			}
			socketIDs.Insert(socketID)
		}
		if socketIDs.Len() != 1 {
			return false, fmt.Errorf("CPUs span %d sockets (%v), expected 1", socketIDs.Len(), socketIDs.UnsortedList())
		}
		return true, nil
	}).WithMessage("expected CPUs to be allocated within a single socket")
}

func BeEvenlyDistributedAcrossActiveNUMANodes() types.GomegaMatcher {
	return gcustom.MakeMatcher(func(allocatedCPUs cpuset.CPUSet) (bool, error) {
		counts, err := numaAllocationCounts(allocatedCPUs, true)
		if err != nil {
			return false, err
		}
		if !allCountsEqual(counts) {
			return false, fmt.Errorf("uneven NUMA distribution across active nodes: %v", counts)
		}
		return true, nil
	}).WithMessage("expected CPUs to be evenly distributed across NUMA nodes with non-zero allocation")
}
func allCountsEqual(counts []int) bool {
	expected := counts[0]
	for _, count := range counts[1:] {
		if count != expected {
			return false
		}
	}
	return true
}
func getContainerAllowedCPUsFromLogs(podName, cntName, logs string) cpuset.CPUSet {
	framework.Logf("got pod logs: <%v>", logs)
	cpus, err := cpuset.Parse(strings.TrimSpace(logs))
	framework.ExpectNoError(err, "parsing cpuset from logs for [%s] of pod [%s]", cntName, podName)
	return cpus
}

// computeNUMADistribution 计算每个 NUMA 节点的 CPU 分布。
func computeNUMADistribution(allocatedCPUs cpuset.CPUSet) map[int]int {
	numaCPUs, err := getNumaNodeCPUs()
	framework.ExpectNoError(err, "Error retrieving NUMA nodes")
	framework.Logf("NUMA Node CPUs allocation: %v", numaCPUs)

	distribution := make(map[int]int)
	for node, cpus := range numaCPUs {
		distribution[node] = cpus.Intersection(allocatedCPUs).Size()
	}

	framework.Logf("allocated CPUs %s distribution: %v", allocatedCPUs.String(), distribution)
	return distribution
}

func getContainerAllowedCPUs(pod *v1.Pod, ctnName string, isInit bool) (cpuset.CPUSet, error) {
	cgPath, err := makeCgroupPathForContainer(pod, ctnName, isInit, e2enodeCgroupV2Enabled)
	if err != nil {
		return cpuset.New(), err
	}
	cgPath = filepath.Join(cgPath, cpusetFileNameFromVersion(e2enodeCgroupV2Enabled))
	framework.Logf("pod %s/%s cnt %s qos=%s path %q", pod.Namespace, pod.Name, ctnName, pod.Status.QOSClass, cgPath)
	data, err := os.ReadFile(cgPath)
	if err != nil {
		return cpuset.New(), err
	}
	cpus := strings.TrimSpace(string(data))
	framework.Logf("pod %s/%s cnt %s cpuset %q", pod.Namespace, pod.Name, ctnName, cpus)
	return cpuset.Parse(cpus)
}

func getSandboxCFSQuota(pod *v1.Pod) (string, error) {
	if !e2enodeCgroupV2Enabled {
		return "", fmt.Errorf("only Cgroup V2 is supported")
	}
	cgPath := filepath.Join(makeCgroupPathForPod(pod, true), "cpu.max")
	data, err := os.ReadFile(cgPath)
	if err != nil {
		return "", err
	}
	quota := strings.TrimSpace(string(data))
	framework.Logf("pod %s/%s qos=%s path %q quota %q", pod.Namespace, pod.Name, pod.Status.QOSClass, cgPath, quota)
	return quota, nil
}

func getContainerCFSQuota(pod *v1.Pod, ctnName string, isInit bool) (string, error) {
	if !e2enodeCgroupV2Enabled {
		return "", fmt.Errorf("only Cgroup V2 is supported")
	}
	cgPath, err := makeCgroupPathForContainer(pod, ctnName, isInit, true)
	if err != nil {
		return "", err
	}
	data, err := os.ReadFile(filepath.Join(cgPath, "cpu.max"))
	if err != nil {
		return "", err
	}
	quota := strings.TrimSpace(string(data))
	framework.Logf("pod %s/%s qos=%s cnt %s path %q quota %q", pod.Namespace, pod.Name, pod.Status.QOSClass, ctnName, cgPath, quota)
	return quota, nil
}

const (
	kubeCgroupRoot = "/sys/fs/cgroup"
)

// 示例路径（systemd、crio、v2）：
// /sys/fs/cgroup/ kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod0b7632a2_a56e_4278_987a_22de18008dbe.slice/ crio-conmon-0bc5eac79e3ae7a0c2651f14722aa10fa333eb2325c2ca97da33aa284cda81b0.scope
// 示例路径（cgroup、containerd、v1）：
// /sys/fs/cgroup/cpuset kubepods/burstable pod8e414e92-17c2-41de-81c7-0045bba9103b b5791f89a6971bb4a751ffbebf533399c91630aa2906d7c6b5e239f405f3b97a

func makeCgroupPathForPod(pod *v1.Pod, isV2 bool) string {
	components := []string{defaultNodeAllocatableCgroup}
	if pod.Status.QOSClass != v1.PodQOSGuaranteed {
		components = append(components, strings.ToLower(string(pod.Status.QOSClass)))
	}
	components = append(components, "pod"+string(pod.UID))

	cgroupName := cm.NewCgroupName(cm.RootCgroupName, components...)
	cgroupFsName := ""
	// 用全局变量很难看，但省得在整个调用栈里反复传参数
	if e2enodeCgroupDriver == "systemd" {
		cgroupFsName = cgroupName.ToSystemd()
	} else {
		cgroupFsName = cgroupName.ToCgroupfs()
	}
	if !isV2 {
		cgroupFsName = filepath.Join("cpuset", cgroupFsName)
	}
	return filepath.Join(kubeCgroupRoot, cgroupFsName)
}

func makeCgroupPathForContainer(pod *v1.Pod, ctnName string, isInit, isV2 bool) (string, error) {
	fullCntID, ok := findContainerIDByName(pod, ctnName, isInit)
	if !ok {
		return "", fmt.Errorf("cannot find status for container %q", ctnName)
	}
	cntID, err := parseContainerID(fullCntID)
	if err != nil {
		return "", err
	}
	cntPath := ""
	if e2enodeCgroupDriver == "systemd" {
		cntPath = containerCgroupPathPrefixFromDriver(e2enodeRuntimeName) + "-" + cntID + ".scope"
	} else {
		cntPath = cntID
	}

	return filepath.Join(makeCgroupPathForPod(pod, isV2), cntPath), nil
}

func cpusetFileNameFromVersion(isV2 bool) string {
	if isV2 {
		return "cpuset.cpus.effective"
	}
	return "cpuset.cpus"
}

func containerCgroupPathPrefixFromDriver(runtimeName string) string {
	if runtimeName == "cri-o" {
		return "crio"
	}
	return "cri-containerd"
}

func parseContainerID(fullID string) (string, error) {
	_, cntID, found := strings.Cut(fullID, "://")
	if !found {
		return "", fmt.Errorf("unsupported containerID: %q", fullID)
	}
	// TODO: 是否需要校验 kind？
	return cntID, nil
}

func findContainerIDByName(pod *v1.Pod, ctnName string, isInit bool) (string, bool) {
	cntStatuses := pod.Status.ContainerStatuses
	if isInit {
		cntStatuses = pod.Status.InitContainerStatuses
	}
	for idx := range cntStatuses {
		if cntStatuses[idx].Name == ctnName {
			return cntStatuses[idx].ContainerID, true
		}
	}
	return "", false
}

func makeThreadSiblingCPUSet(cpus cpuset.CPUSet) cpuset.CPUSet {
	siblingsCPUs := cpuset.New()
	for _, cpuID := range cpus.UnsortedList() {
		siblingsCPUs = siblingsCPUs.Union(cpuSiblingListFromSysFS(int64(cpuID)))
	}
	return siblingsCPUs
}

func updateKubeletConfigIfNeeded(ctx context.Context, f *framework.Framework, desiredCfg *kubeletconfig.KubeletConfiguration) *v1.Node {
	curCfg, err := getCurrentKubeletConfig(ctx)
	framework.ExpectNoError(err)

	if equalKubeletConfiguration(curCfg, desiredCfg) {
		framework.Logf("Kubelet configuration already compliant, nothing to do")
		return getLocalNode(ctx, f)
	}

	framework.Logf("Updating Kubelet configuration")
	updateKubeletConfig(ctx, f, desiredCfg, true)
	framework.Logf("Updated Kubelet configuration")

	return getLocalNode(ctx, f)
}

func equalKubeletConfiguration(cfgA, cfgB *kubeletconfig.KubeletConfiguration) bool {
	cfgA = cfgA.DeepCopy()
	cfgB = cfgB.DeepCopy()
	// 只关心 payload，强制 metadata 统一
	cfgA.TypeMeta = metav1.TypeMeta{}
	cfgB.TypeMeta = metav1.TypeMeta{}
	return reflect.DeepEqual(cfgA, cfgB)
}

type nodeCPUDetails struct {
	Capacity    int64
	Allocatable int64
	Reserved    int64
}

func cpuDetailsFromNode(node *v1.Node) nodeCPUDetails {
	localNodeCap := node.Status.Capacity
	cpuCap := localNodeCap[v1.ResourceCPU]
	localNodeAlloc := node.Status.Allocatable
	cpuAlloc := localNodeAlloc[v1.ResourceCPU]
	cpuRes := cpuCap.DeepCopy()
	cpuRes.Sub(cpuAlloc)
	// 对预留 CPU 向上取整，只保留整核。
	cpuRes.RoundUp(0)
	return nodeCPUDetails{
		Capacity:    cpuCap.Value(),
		Allocatable: cpuCap.Value() - cpuRes.Value(),
		Reserved:    cpuRes.Value(),
	}
}

// smtLevelFromSysFS 返回处理器提供的对称多线程（SMT）执行单元数量。
// x86_64 上最常见的值是 2（每物理核 2 个虚拟线程/核），即 smtLevel == 2。
// 以下是同义词：threadsPerCore、smtLevel
// 注意：找不到既好又不冗长的名字，"threadSiblingCount"、"smtLevel"、"threadsPerCore" 都有问题。
func smtLevelFromSysFS() int {
	cpuID := int64(0) // 这只是随机系统中最可能存在的 CPU，没有其他特殊含义。
	cpus := cpuSiblingListFromSysFS(cpuID)
	return cpus.Size()
}

func cpuSiblingListFromSysFS(cpuID int64) cpuset.CPUSet {
	data, err := os.ReadFile(fmt.Sprintf("/sys/devices/system/cpu/cpu%d/topology/thread_siblings_list", cpuID))
	framework.ExpectNoError(err)
	// 有多少线程兄弟 = SMT level
	// 例如：2 路 SMT 表示每个线程有 2 个兄弟线程
	cpus, err := cpuset.Parse(strings.TrimSpace(string(data)))
	framework.ExpectNoError(err)
	return cpus
}

func uncoreCacheIDFromSysFS(cpuID int) (int64, error) {
	// 期望每个 CPU 的 Uncore Cache ID 的 sysfs 路径为：
	// /sys/devices/system/cpu/cpu#/cache/index3/id
	cacheIDPath := filepath.Join("/sys/devices/system/cpu", fmt.Sprintf("cpu%d", cpuID), "cache", "index3", "id")
	cacheIDBytes, err := os.ReadFile(cacheIDPath)
	if err != nil {
		return 0, fmt.Errorf("failed to read cache ID for CPU %d: %w", cpuID, err)
	}

	cacheIDStr := strings.TrimSpace(string(cacheIDBytes))
	cacheID, err := strconv.ParseInt(cacheIDStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse cache ID for CPU %d: %w", cpuID, err)
	}

	return cacheID, nil
}

func makeCPUManagerBEPod(podName string, ctnAttributes []ctnAttribute) *v1.Pod {
	var containers []v1.Container
	for _, ctnAttr := range ctnAttributes {
		ctn := v1.Container{
			Name:    ctnAttr.ctnName,
			Image:   busyboxImage,
			Command: []string{"sh", "-c", ctnAttr.ctnCommand},
			VolumeMounts: []v1.VolumeMount{
				{
					Name:      "sysfscgroup",
					MountPath: "/sysfscgroup",
				},
				{
					Name:      "podinfo",
					MountPath: "/podinfo",
				},
			},
		}
		containers = append(containers, ctn)
	}

	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
		},
		Spec: v1.PodSpec{
			RestartPolicy: v1.RestartPolicyNever,
			Containers:    containers,
			Volumes: []v1.Volume{
				{
					Name: "sysfscgroup",
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{Path: "/sys/fs/cgroup"},
					},
				},
				{
					Name: "podinfo",
					VolumeSource: v1.VolumeSource{
						DownwardAPI: &v1.DownwardAPIVolumeSource{
							Items: []v1.DownwardAPIVolumeFile{
								{
									Path: "uid",
									FieldRef: &v1.ObjectFieldSelector{
										APIVersion: "v1",
										FieldPath:  "metadata.uid",
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func requireCGroupV2() {
	if e2enodeCgroupV2Enabled {
		return
	}
	e2eskipper.Skipf("Skipping since CgroupV2 not used")

}

const (
	minSMTLevel    = 2
	minCPUCapacity = 2
)

// makeCPUManagerPod() 的 helper
type ctnAttribute struct {
	ctnName       string
	ctnCommand    string
	cpuRequest    string
	cpuLimit      string
	restartPolicy *v1.ContainerRestartPolicy
}

// makeCPUMangerPod 返回带给定 ctnAttributes 的 Pod。
func makeCPUManagerPod(podName string, ctnAttributes []ctnAttribute) *v1.Pod {
	var containers []v1.Container
	for _, ctnAttr := range ctnAttributes {
		var cpusetCmd string
		if ctnAttr.ctnCommand != "" {
			cpusetCmd = ctnAttr.ctnCommand
		} else {
			cpusetCmd = "grep Cpus_allowed_list /proc/self/status | cut -f2 && sleep 1d"
		}

		requests := v1.ResourceList{
			v1.ResourceMemory: resource.MustParse("100Mi"),
		}
		if ctnAttr.cpuRequest != "" {
			requests[v1.ResourceCPU] = resource.MustParse(ctnAttr.cpuRequest)
		}

		limits := v1.ResourceList{
			v1.ResourceMemory: resource.MustParse("100Mi"),
		}
		if ctnAttr.cpuLimit != "" {
			limits[v1.ResourceCPU] = resource.MustParse(ctnAttr.cpuLimit)
		}

		ctn := v1.Container{
			Name:  ctnAttr.ctnName,
			Image: busyboxImage,
			Resources: v1.ResourceRequirements{
				Requests: requests,
				Limits:   limits,
			},
			Command: []string{"sh", "-c", cpusetCmd},
			VolumeMounts: []v1.VolumeMount{
				{
					Name:      "sysfscgroup",
					MountPath: "/sysfscgroup",
				},
				{
					Name:      "podinfo",
					MountPath: "/podinfo",
				},
			},
		}
		containers = append(containers, ctn)
	}

	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
		},
		Spec: v1.PodSpec{
			RestartPolicy: v1.RestartPolicyNever,
			Containers:    containers,
			Volumes: []v1.Volume{
				{
					Name: "sysfscgroup",
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{Path: "/sys/fs/cgroup"},
					},
				},
				{
					Name: "podinfo",
					VolumeSource: v1.VolumeSource{
						DownwardAPI: &v1.DownwardAPIVolumeSource{
							Items: []v1.DownwardAPIVolumeFile{
								{
									Path: "uid",
									FieldRef: &v1.ObjectFieldSelector{
										APIVersion: "v1",
										FieldPath:  "metadata.uid",
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

// makeCPUMangerInitContainersPod 返回带 init 容器的 Pod，
// 容器属性由给定 ctnAttributes 指定。
func makeCPUManagerInitContainersPod(podName string, ctnAttributes []ctnAttribute) *v1.Pod {
	var containers []v1.Container
	cpusetCmd := "grep Cpus_allowed_list /proc/self/status | cut -f2"
	cpusetAndSleepCmd := "grep Cpus_allowed_list /proc/self/status | cut -f2 && sleep 1d"
	for _, ctnAttr := range ctnAttributes {
		ctn := v1.Container{
			Name:  ctnAttr.ctnName,
			Image: busyboxImage,
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse(ctnAttr.cpuRequest),
					v1.ResourceMemory: resource.MustParse("100Mi"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse(ctnAttr.cpuLimit),
					v1.ResourceMemory: resource.MustParse("100Mi"),
				},
			},
			Command:       []string{"sh", "-c", cpusetCmd},
			RestartPolicy: ctnAttr.restartPolicy,
		}
		if ctnAttr.restartPolicy != nil && *ctnAttr.restartPolicy == v1.ContainerRestartPolicyAlways {
			ctn.Command = []string{"sh", "-c", cpusetAndSleepCmd}
		}
		containers = append(containers, ctn)
	}

	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
		},
		Spec: v1.PodSpec{
			RestartPolicy:  v1.RestartPolicyNever,
			InitContainers: containers,
			Containers: []v1.Container{
				{
					Name:  "regular",
					Image: busyboxImage,
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("1000m"),
							v1.ResourceMemory: resource.MustParse("100Mi"),
						},
						Limits: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("1000m"),
							v1.ResourceMemory: resource.MustParse("100Mi"),
						},
					},
					Command: []string{"sh", "-c", cpusetAndSleepCmd},
				},
			},
		},
	}
}

type cpuManagerKubeletArguments struct {
	policyName                     string
	topologyManagerPolicy          string
	topologyManagerScope           string
	enableCPUManagerOptions        bool
	enablePodLevelResources        bool
	customCPUCFSQuotaPeriod        time.Duration
	enablePodLevelResourceManagers bool
	reservedSystemCPUs             cpuset.CPUSet
	options                        map[string]string
}

func configureCPUManagerInKubelet(oldCfg *kubeletconfig.KubeletConfiguration, kubeletArguments *cpuManagerKubeletArguments) *kubeletconfig.KubeletConfiguration {
	newCfg := oldCfg.DeepCopy()
	if newCfg.FeatureGates == nil {
		newCfg.FeatureGates = make(map[string]bool)
	}

	newCfg.FeatureGates["CPUManagerPolicyBetaOptions"] = kubeletArguments.enableCPUManagerOptions
	newCfg.FeatureGates["CPUManagerPolicyAlphaOptions"] = kubeletArguments.enableCPUManagerOptions
	newCfg.FeatureGates["PodLevelResources"] = kubeletArguments.enablePodLevelResources
	// InPlacePodLevelResourcesVerticalScaling、PodLevelResourcesFixDefaulting 和 PodLevelResourcesFixKubeletQOSClass 仅在 PodLevelResources 启用时支持
	if !kubeletArguments.enablePodLevelResources {
		newCfg.FeatureGates["InPlacePodLevelResourcesVerticalScaling"] = kubeletArguments.enablePodLevelResources
		newCfg.FeatureGates["PodLevelResourcesFixDefaulting"] = kubeletArguments.enablePodLevelResources
		newCfg.FeatureGates["PodLevelResourcesFixKubeletQOSClass"] = kubeletArguments.enablePodLevelResources
	}
	newCfg.FeatureGates["PodLevelResourceManagers"] = kubeletArguments.enablePodLevelResourceManagers

	if kubeletArguments.customCPUCFSQuotaPeriod != 0 {
		newCfg.FeatureGates["CustomCPUCFSQuotaPeriod"] = true
		newCfg.CPUCFSQuotaPeriod.Duration = kubeletArguments.customCPUCFSQuotaPeriod
	} else {
		newCfg.FeatureGates["CustomCPUCFSQuotaPeriod"] = false
	}

	newCfg.CPUManagerPolicy = kubeletArguments.policyName
	newCfg.TopologyManagerPolicy = kubeletArguments.topologyManagerPolicy
	newCfg.TopologyManagerScope = kubeletArguments.topologyManagerScope
	newCfg.CPUManagerReconcilePeriod = metav1.Duration{Duration: 1 * time.Second}

	if kubeletArguments.options != nil {
		newCfg.CPUManagerPolicyOptions = kubeletArguments.options
	}

	if kubeletArguments.reservedSystemCPUs.Size() > 0 {
		cpus := kubeletArguments.reservedSystemCPUs.String()
		framework.Logf("configureCPUManagerInKubelet: using reservedSystemCPUs=%q", cpus)
		newCfg.ReservedSystemCPUs = cpus
	} else {
		// 启用 CPU Manager 时，如果 kube-reserved 或 system-reserved 未设置，
		// Kubelet 会 panic。把 kube-reserved 中的 cpu 设为 > 0，
		// 避免 kubelet panic。
		if newCfg.KubeReserved == nil {
			newCfg.KubeReserved = map[string]string{}
		}

		if _, ok := newCfg.KubeReserved["cpu"]; !ok {
			newCfg.KubeReserved["cpu"] = "200m"
		}
	}

	return newCfg
}

func checkAllocatableCPUs(node *v1.Node, val int, reservedCPUs cpuset.CPUSet, onlineCPUs cpuset.CPUSet, smtLevel int) {
	ginkgo.GinkgoHelper()
	cpuReq := int64(val + reservedCPUs.Size()) // 预留 CPU 不可用，需要计入
	// framework 是用注入的 BeforeEach 节点初始化的，因此
	// 我们能做的最早初始化就是在这里初始化其他对象
	nodeCPUDetails := cpuDetailsFromNode(node)

	msg := fmt.Sprintf("%v full CPUs (detected=%v requested=%v reserved=%v online=%v smt=%v)", cpuReq, nodeCPUDetails.Allocatable, val, reservedCPUs.Size(), onlineCPUs.Size(), smtLevel)
	ginkgo.By("Checking if allocatable: " + msg)
	if nodeCPUDetails.Allocatable < cpuReq {
		e2eskipper.Skipf("Skipping CPU Manager test: not allocatable %s", msg)
	}
}
