/*
Copyright 2016 The Kubernetes Authors.

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
	"math"
	"os"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	kubeletstatsv1alpha1 "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2evolume "k8s.io/kubernetes/test/e2e/framework/volume"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"

	systemdutil "github.com/coreos/go-systemd/v22/util"
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gstruct"
	"github.com/onsi/gomega/types"
)

var _ = SIGDescribe("Summary API", framework.WithNodeConformance(), func() {
	f := framework.NewDefaultFramework("summary-test")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	ginkgo.Context("when querying /stats/summary", func() {
		ginkgo.AfterEach(func(ctx context.Context) {
			if !ginkgo.CurrentSpecReport().Failed() {
				return
			}
			if framework.TestContext.DumpLogsOnFailure {
				e2ekubectl.LogFailedContainers(ctx, f.ClientSet, f.Namespace.Name, framework.Logf)
			}
			ginkgo.By("Recording processes in system cgroups")
			recordSystemCgroupProcesses(ctx)
		})
		ginkgo.It("should report resource usage through the stats api", func(ctx context.Context) {
			const pod0 = "stats-busybox-0"
			const pod1 = "stats-busybox-1"

			ginkgo.By("Creating test pods")
			numRestarts := int32(1)
			pods := getSummaryTestPods(f, numRestarts, pod0, pod1)
			e2epod.NewPodClient(f).CreateBatch(ctx, pods)

			ginkgo.By("restarting the containers to ensure container metrics are still being gathered after a container is restarted")
			gomega.Eventually(ctx, func() error {
				for _, pod := range pods {
					err := verifyPodRestartCount(ctx, f, pod.Name, len(pod.Spec.Containers), numRestarts)
					if err != nil {
						return err
					}
				}
				return nil
			}, time.Minute, 5*time.Second).Should(gomega.BeNil())

			ginkgo.By("Waiting 15 seconds for cAdvisor to collect 2 stats points")
			time.Sleep(15 * time.Second)

			// 设置期望值。
			const (
				maxStartAge = time.Hour * 24 * 365 // 1 年
				maxStatsAge = time.Minute
			)
			ginkgo.By("Fetching node so we can match against an appropriate memory limit")
			node := getLocalNode(ctx, f)
			memoryCapacity := node.Status.Capacity["memory"]
			memoryLimit := memoryCapacity.Value()
			fsCapacityBounds := bounded(100*e2evolume.Mb, 10*e2evolume.Tb)
			// 对系统容器的期望。
			sysContExpectations := func() types.GomegaMatcher {
				return gstruct.MatchAllFields(gstruct.Fields{
					"Name":      gstruct.Ignore(),
					"StartTime": recent(maxStartAge),
					"CPU": ptrMatchAllFields(gstruct.Fields{
						"Time": recent(maxStatsAge),
						// CRI stats provider 尝试估算 UsageNanoCores 的值，该值可能是
						// 0，或介于 10000 到 2e10 之间（测试运行中观察到的上限）。
						// 更多信息参见 https://github.com/kubernetes/kubernetes/pull/95345#discussion_r501630942
						//
						"UsageNanoCores":       gomega.SatisfyAny(gstruct.PointTo(gomega.BeZero()), bounded(10000, 2e10)),
						"UsageCoreNanoSeconds": bounded(10000000, 1e15),
						"PSI":                  psiExpectation(),
					}),
					"Memory": ptrMatchAllFields(gstruct.Fields{
						"Time": recent(maxStatsAge),
						// 我们不限制系统容器的内存。
						"AvailableBytes":  gomega.BeNil(),
						"UsageBytes":      bounded(1*e2evolume.Mb, memoryLimit),
						"WorkingSetBytes": bounded(1*e2evolume.Mb, memoryLimit),
						// 现在它返回 /sys/fs/cgroup/memory.stat 的 total_rss
						"RSSBytes":        bounded(1*e2evolume.Mb, memoryLimit),
						"PageFaults":      bounded(1000, 1e9),
						"MajorPageFaults": bounded(0, 1e9),
						"PSI":             psiExpectation(),
					}),
					"IO":                 ioExpectation(maxStatsAge),
					"Swap":               swapExpectation(memoryLimit),
					"Accelerators":       gomega.BeEmpty(),
					"Rootfs":             gomega.BeNil(),
					"Logs":               gomega.BeNil(),
					"UserDefinedMetrics": gomega.BeEmpty(),
				})
			}
			expectedPageFaultsUpperBound := 1000000
			expectedMajorPageFaultsUpperBound := 1e9
			if IsCgroup2UnifiedMode() {
				// 在 cgroupv2 上这些统计是递归的，因此确保它们至少达到
				// 上面为容器设置的值。
				expectedPageFaultsUpperBound = 1e9
				expectedMajorPageFaultsUpperBound = 1e9
			}

			podsContExpectations := sysContExpectations().(*gstruct.FieldsMatcher)
			podsContExpectations.Fields["Memory"] = ptrMatchAllFields(gstruct.Fields{
				"Time": recent(maxStatsAge),
				// Pod 受 Node Allocatable 限制
				"AvailableBytes":  bounded(1*e2evolume.Kb, memoryLimit),
				"UsageBytes":      bounded(10*e2evolume.Kb, memoryLimit),
				"WorkingSetBytes": bounded(10*e2evolume.Kb, memoryLimit),
				"RSSBytes":        bounded(1*e2evolume.Kb, memoryLimit),
				"PageFaults":      bounded(0, expectedPageFaultsUpperBound),
				"MajorPageFaults": bounded(0, expectedMajorPageFaultsUpperBound),
				"PSI":             psiExpectation(),
			})
			runtimeContExpectations := sysContExpectations().(*gstruct.FieldsMatcher)
			systemContainers := gstruct.Elements{
				"kubelet": sysContExpectations(),
				"runtime": runtimeContExpectations,
				"pods":    podsContExpectations,
			}
			// 只有当宿主机未运行 systemd 时，Kubelet 才管理 'misc' 系统容器。
			if !systemdutil.IsRunningSystemd() {
				framework.Logf("Host not running systemd; expecting 'misc' system container.")
				miscContExpectations := sysContExpectations().(*gstruct.FieldsMatcher)
				// Misc 进程依赖具体系统，因此放宽内存约束。
				miscContExpectations.Fields["Memory"] = ptrMatchAllFields(gstruct.Fields{
					"Time": recent(maxStatsAge),
					// 我们不限制系统容器的内存。
					"AvailableBytes":  gomega.BeNil(),
					"UsageBytes":      bounded(100*e2evolume.Kb, memoryLimit),
					"WorkingSetBytes": bounded(100*e2evolume.Kb, memoryLimit),
					"RSSBytes":        bounded(100*e2evolume.Kb, memoryLimit),
					"PageFaults":      bounded(1000, 1e9),
					"MajorPageFaults": bounded(0, 1e9),
					"PSI":             psiExpectation(),
				})
				systemContainers["misc"] = miscContExpectations
			}
			// 对 Pod 的期望。
			podExpectations := gstruct.MatchAllFields(gstruct.Fields{
				"PodRef":    gstruct.Ignore(),
				"StartTime": recent(maxStartAge),
				"Containers": gstruct.MatchAllElements(summaryObjectID, gstruct.Elements{
					"busybox-container": gstruct.MatchAllFields(gstruct.Fields{
						"Name":      gomega.Equal("busybox-container"),
						"StartTime": recent(maxStartAge),
						"CPU": ptrMatchAllFields(gstruct.Fields{
							"Time":                 recent(maxStatsAge),
							"UsageNanoCores":       bounded(10000, 1e9),
							"UsageCoreNanoSeconds": bounded(10000000, 1e12),
							"PSI":                  psiExpectation(),
						}),
						"Memory": ptrMatchAllFields(gstruct.Fields{
							"Time":            recent(maxStatsAge),
							"AvailableBytes":  bounded(1*e2evolume.Kb, 80*e2evolume.Mb),
							"UsageBytes":      bounded(10*e2evolume.Kb, 80*e2evolume.Mb),
							"WorkingSetBytes": bounded(10*e2evolume.Kb, 80*e2evolume.Mb),
							"RSSBytes":        bounded(1*e2evolume.Kb, 80*e2evolume.Mb),
							"PageFaults":      bounded(100, expectedPageFaultsUpperBound),
							"MajorPageFaults": bounded(0, expectedMajorPageFaultsUpperBound),
							"PSI":             psiExpectation(),
						}),
						"IO":           ioExpectation(maxStatsAge),
						"Swap":         swapExpectation(memoryLimit),
						"Accelerators": gomega.BeEmpty(),
						"Rootfs": ptrMatchAllFields(gstruct.Fields{
							"Time":           recent(maxStatsAge),
							"AvailableBytes": fsCapacityBounds,
							"CapacityBytes":  fsCapacityBounds,
							"UsedBytes":      bounded(e2evolume.Kb, 10*e2evolume.Mb),
							"InodesFree":     bounded(1e4, 1e8),
							"Inodes":         bounded(1e4, 1e8),
							"InodesUsed":     bounded(0, 1e8),
						}),
						"Logs": ptrMatchAllFields(gstruct.Fields{
							"Time":           recent(maxStatsAge),
							"AvailableBytes": fsCapacityBounds,
							"CapacityBytes":  fsCapacityBounds,
							"UsedBytes":      bounded(e2evolume.Kb, 10*e2evolume.Mb),
							"InodesFree":     bounded(1e4, 1e8),
							"Inodes":         bounded(1e4, 1e8),
							"InodesUsed":     bounded(0, 1e8),
						}),
						"UserDefinedMetrics": gomega.BeEmpty(),
					}),
				}),
				"Network": ptrMatchAllFields(gstruct.Fields{
					"Time": recent(maxStatsAge),
					"InterfaceStats": gstruct.MatchAllFields(gstruct.Fields{
						"Name":     gomega.Equal("eth0"),
						"RxBytes":  bounded(10, 10*e2evolume.Mb),
						"RxErrors": bounded(0, 1000),
						"TxBytes":  bounded(10, 10*e2evolume.Mb),
						"TxErrors": bounded(0, 1000),
					}),
					"Interfaces": gomega.Not(gomega.BeNil()),
				}),
				"CPU": ptrMatchAllFields(gstruct.Fields{
					"Time":                 recent(maxStatsAge),
					"UsageNanoCores":       bounded(10000, 1e9),
					"UsageCoreNanoSeconds": bounded(10000000, 1e12),
					"PSI":                  psiExpectation(),
				}),
				"Memory": ptrMatchAllFields(gstruct.Fields{
					"Time":            recent(maxStatsAge),
					"AvailableBytes":  bounded(1*e2evolume.Kb, 80*e2evolume.Mb),
					"UsageBytes":      bounded(10*e2evolume.Kb, 80*e2evolume.Mb),
					"WorkingSetBytes": bounded(10*e2evolume.Kb, 80*e2evolume.Mb),
					"RSSBytes":        bounded(1*e2evolume.Kb, 80*e2evolume.Mb),
					"PageFaults":      bounded(0, expectedPageFaultsUpperBound),
					"MajorPageFaults": bounded(0, expectedMajorPageFaultsUpperBound),
					"PSI":             psiExpectation(),
				}),
				"IO":   ioExpectation(maxStatsAge),
				"Swap": swapExpectation(memoryLimit),
				"VolumeStats": gstruct.MatchAllElements(summaryObjectID, gstruct.Elements{
					"test-empty-dir": gstruct.MatchAllFields(gstruct.Fields{
						"Name":   gomega.Equal("test-empty-dir"),
						"PVCRef": gomega.BeNil(),
						"FsStats": gstruct.MatchAllFields(gstruct.Fields{
							"Time":           recent(maxStatsAge),
							"AvailableBytes": fsCapacityBounds,
							"CapacityBytes":  fsCapacityBounds,
							"UsedBytes":      bounded(e2evolume.Kb, 1*e2evolume.Mb),
							"InodesFree":     bounded(1e4, 1e8),
							"Inodes":         bounded(1e4, 1e8),
							"InodesUsed":     bounded(0, 1e8),
						}),
					}),
				}),
				"EphemeralStorage": ptrMatchAllFields(gstruct.Fields{
					"Time":           recent(maxStatsAge),
					"AvailableBytes": fsCapacityBounds,
					"CapacityBytes":  fsCapacityBounds,
					"UsedBytes":      bounded(e2evolume.Kb, 21*e2evolume.Mb),
					"InodesFree":     bounded(1e4, 1e8),
					"Inodes":         bounded(1e4, 1e8),
					"InodesUsed":     bounded(0, 1e8),
				}),
				"ProcessStats": ptrMatchAllFields(gstruct.Fields{
					"ProcessCount": bounded(1, 1e8),
				}),
			})

			matchExpectations := ptrMatchAllFields(gstruct.Fields{
				"Node": gstruct.MatchAllFields(gstruct.Fields{
					"NodeName":         gomega.Equal(framework.TestContext.NodeName),
					"StartTime":        recent(maxStartAge),
					"SystemContainers": gstruct.MatchAllElements(summaryObjectID, systemContainers),
					"CPU": ptrMatchAllFields(gstruct.Fields{
						"Time":                 recent(maxStatsAge),
						"UsageNanoCores":       bounded(100e3, 2e10),
						"UsageCoreNanoSeconds": bounded(1e9, 1e15),
						"PSI":                  psiExpectation(),
					}),
					"Memory": ptrMatchAllFields(gstruct.Fields{
						"Time":            recent(maxStatsAge),
						"AvailableBytes":  bounded(100*e2evolume.Mb, memoryLimit),
						"UsageBytes":      bounded(10*e2evolume.Mb, memoryLimit),
						"WorkingSetBytes": bounded(10*e2evolume.Mb, memoryLimit),
						// 现在它返回 /sys/fs/cgroup/memory.stat 的 total_rss
						"RSSBytes":        bounded(1*e2evolume.Kb, memoryLimit),
						"PageFaults":      bounded(1000, 1e9),
						"MajorPageFaults": bounded(0, 1e9),
						"PSI":             psiExpectation(),
					}),
					"IO":   ioExpectation(maxStatsAge),
					"Swap": swapExpectation(memoryLimit),
					// TODO(#28407): Handle non-eth0 network interface names.
					"Network": ptrMatchAllFields(gstruct.Fields{
						"Time": recent(maxStatsAge),
						"InterfaceStats": gstruct.MatchAllFields(gstruct.Fields{
							"Name":     gomega.Or(gomega.BeEmpty(), gomega.Equal("eth0")),
							"RxBytes":  gomega.Or(gomega.BeNil(), bounded(1*e2evolume.Mb, 100*e2evolume.Gb)),
							"RxErrors": gomega.Or(gomega.BeNil(), bounded(0, 100000)),
							"TxBytes":  gomega.Or(gomega.BeNil(), bounded(10*e2evolume.Kb, 10*e2evolume.Gb)),
							"TxErrors": gomega.Or(gomega.BeNil(), bounded(0, 100000)),
						}),
						"Interfaces": gomega.Not(gomega.BeNil()),
					}),
					"Fs": ptrMatchAllFields(gstruct.Fields{
						"Time":           recent(maxStatsAge),
						"AvailableBytes": fsCapacityBounds,
						"CapacityBytes":  fsCapacityBounds,
						// 假定测试机器的磁盘不超过 10TB
						"UsedBytes":  bounded(e2evolume.Kb, 10*e2evolume.Tb),
						"InodesFree": bounded(1e4, 1e8),
						"Inodes":     bounded(1e4, 1e8),
						"InodesUsed": bounded(0, 1e8),
					}),
					"Runtime": ptrMatchAllFields(gstruct.Fields{
						"ImageFs": ptrMatchAllFields(gstruct.Fields{
							"Time":           recent(maxStatsAge),
							"AvailableBytes": fsCapacityBounds,
							"CapacityBytes":  fsCapacityBounds,
							// 假定测试机器的磁盘不超过 10TB
							"UsedBytes":  bounded(e2evolume.Kb, 10*e2evolume.Tb),
							"InodesFree": bounded(1e4, 1e8),
							"Inodes":     bounded(1e4, 1e8),
							"InodesUsed": bounded(0, 1e8),
						}),
						"ContainerFs": ptrMatchAllFields(gstruct.Fields{
							"Time":           recent(maxStatsAge),
							"AvailableBytes": fsCapacityBounds,
							"CapacityBytes":  fsCapacityBounds,
							// 假定测试机器的磁盘不超过 10TB
							"UsedBytes":  bounded(e2evolume.Kb, 10*e2evolume.Tb),
							"InodesFree": bounded(1e4, 1e8),
							"Inodes":     bounded(1e4, 1e8),
							"InodesUsed": bounded(0, 1e8),
						}),
					}),
					"Rlimit": ptrMatchAllFields(gstruct.Fields{
						"Time":                  recent(maxStatsAge),
						"MaxPID":                bounded(0, 1e8),
						"NumOfRunningProcesses": bounded(0, 1e8),
					}),
				}),
				// 忽略额外的 Pod，因为测试是并行运行的。
				"Pods": gstruct.MatchElements(summaryObjectID, gstruct.IgnoreExtras, gstruct.Elements{
					fmt.Sprintf("%s::%s", f.Namespace.Name, pod0): podExpectations,
					fmt.Sprintf("%s::%s", f.Namespace.Name, pod1): podExpectations,
				}),
			})

			ginkgo.By("Validating /stats/summary")
			// 给 Pod 一分钟真正启动。
			gomega.Eventually(ctx, getNodeSummary, 180*time.Second, 15*time.Second).Should(matchExpectations)
			// 之后 summary 应在几次内持续符合期望。
			gomega.Consistently(ctx, getNodeSummary, 30*time.Second, 15*time.Second).Should(matchExpectations)
		})
	})

	framework.Context("when querying /stats/summary under pressure", framework.WithSerial(), framework.WithNodeConformance(), framework.WithFeatureGate(features.KubeletPSI), func() {
		ginkgo.BeforeEach(func() {
			if !IsCgroup2UnifiedMode() {
				ginkgo.Skip("Skipping since CgroupV2 not used")
			}
		})

		ginkgo.It("should report CPU pressure in PSI metrics", func(ctx context.Context) {
			podName := "cpu-pressure-pod"
			ginkgo.By("Creating a pod to generate CPU pressure")
			podSpec := getStressTestPod(podName, "cpu-stress", []string{"stress", "--cpus", "1"})
			podSpec.Spec.Containers[0].Resources = v1.ResourceRequirements{
				Limits: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("500m"),
				},
				Requests: v1.ResourceList{
					v1.ResourceCPU: resource.MustParse("500m"),
				},
			}
			pod := e2epod.NewPodClient(f).Create(ctx, podSpec)

			ginkgo.By("Waiting for the pod to start")
			framework.ExpectNoError(e2epod.WaitForPodRunningInNamespace(ctx, f.ClientSet, pod))

			ginkgo.By("Validating that CPU PSI metrics reflect pressure")
			gomega.Eventually(ctx, func(g gomega.Gomega) {
				summary, err := getNodeSummary(ctx)
				framework.ExpectNoError(err)
				g.Expect(summary.Pods).To(gstruct.MatchElements(summaryObjectID, gstruct.IgnoreExtras, gstruct.Elements{
					fmt.Sprintf("%s::%s", f.Namespace.Name, podName): gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
						"CPU": pressureDetected("some", 0.1),
					}),
				}))
			}, 2*time.Minute, 15*time.Second).Should(gomega.Succeed())
			framework.ExpectNoError(e2epod.NewPodClient(f).Delete(ctx, pod.Name, metav1.DeleteOptions{}))
		})

		ginkgo.It("should report Memory pressure in PSI metrics", func(ctx context.Context) {
			podName := "memory-pressure-pod"
			ginkgo.By("Creating a pod to generate Memory pressure")
			// 创建一个通过持续写文件来产生内存压力的 Pod，
			// 强制内核回收 page cache。
			podSpec := getStressTestPod(podName, "memory-stress", []string{})
			podSpec.Spec.Containers[0].Command = []string{"/bin/sh", "-c"}
			podSpec.Spec.Containers[0].Args = []string{
				// 在无限循环中写入 50MB 文件，轮流使用 5 个文件名，从而
				// 内核必须在 200MB 限制内不断回收文件缓存。
				// 第一轮写入较小的 10MB 文件：如果没有这个渐进过程，
				// 第一轮会在回写开始前用脏页填满整个限制，
				// 而在保留脏页较久的文件系统（如 Fedora CoreOS 上的
				// XFS）上，内核会 OOM 杀死容器而非回收内存。
				"i=0; while [ $i -lt 5 ]; do dd if=/dev/zero of=testfile.$i bs=1M count=10 &>/dev/null; i=$((i+1)); sleep 0.1; done; " +
					"i=0; while true; do dd if=/dev/zero of=testfile.$i bs=1M count=50 &>/dev/null; i=$(((i+1)%5)); sleep 0.1; done",
			}
			podSpec.Spec.Containers[0].Resources = v1.ResourceRequirements{
				Limits: v1.ResourceList{
					v1.ResourceMemory: resource.MustParse("200M"),
				},
				Requests: v1.ResourceList{
					v1.ResourceMemory: resource.MustParse("200M"),
				},
			}
			pod := e2epod.NewPodClient(f).Create(ctx, podSpec)

			ginkgo.By("Waiting for the pod to start")
			framework.ExpectNoError(e2epod.WaitForPodRunningInNamespace(ctx, f.ClientSet, pod))

			// Linux 6.16 及更新版本能在不长时间阻塞 Pod 的情况下
			// 回收该工作负载的文件缓存，因此 Full.Avg10 保持为零，
			// 检查平均值无法通过。总停顿时间
			// 在所有内核上仍会增长，因此检查 Full.Total 是否变为
			// 正数并在工作负载运行期间持续增长。空闲
			// Pod 不会增加停顿时间，因此增长证明压力来自
			// 该 Pod。
			ginkgo.By("Validating that Memory PSI metrics report stall time")
			var baselineTotal uint64
			gomega.Eventually(ctx, func(ctx context.Context) (uint64, error) {
				total, err := podMemoryPSIFullTotal(ctx, f.Namespace.Name, podName)
				if err != nil {
					return 0, err
				}
				baselineTotal = total
				return total, nil
			}, 2*time.Minute, 15*time.Second).Should(gomega.BeNumerically(">", uint64(0)))

			ginkgo.By("Validating that Memory PSI total stall time grows under sustained pressure")
			gomega.Eventually(ctx, func(ctx context.Context) (uint64, error) {
				return podMemoryPSIFullTotal(ctx, f.Namespace.Name, podName)
			}, 2*time.Minute, 15*time.Second).Should(gomega.BeNumerically(">", baselineTotal))
			framework.ExpectNoError(e2epod.NewPodClient(f).Delete(ctx, pod.Name, metav1.DeleteOptions{}))
		})

		ginkgo.It("should report I/O pressure in PSI metrics", func(ctx context.Context) {
			podName := "io-pressure-pod"
			ginkgo.By("Creating a pod to generate I/O pressure")
			// 该工作负载使用 shell 循环持续向磁盘写文件并
			// 同步，从而产生持续的 I/O 压力，是触发和测量
			// I/O 相关 PSI 指标的可靠方法。
			podSpec := getStressTestPod(podName, "io-stress", []string{})
			podSpec.Spec.Containers[0].Command = []string{"/bin/sh", "-c"}
			podSpec.Spec.Containers[0].Args = []string{
				// 此命令运行无限循环以产生持续的 I/O 压力。
				// 每次迭代用 `dd` 写入 128MB 文件，然后调用 `sync`
				// 确保数据从内存刷到磁盘，从而产生真实的 I/O 停顿。
				"while true; do dd if=/dev/zero of=testfile bs=1M count=128 &>/dev/null; sync; rm testfile &>/dev/null; done",
			}
			pod := e2epod.NewPodClient(f).Create(ctx, podSpec)

			ginkgo.By("Waiting for the pod to start")
			framework.ExpectNoError(e2epod.WaitForPodRunningInNamespace(ctx, f.ClientSet, pod))

			ginkgo.By("Validating that I/O PSI metrics reflect pressure")
			gomega.Eventually(ctx, func(g gomega.Gomega) {
				summary, err := getNodeSummary(ctx)
				framework.ExpectNoError(err)
				g.Expect(summary.Pods).To(gstruct.MatchElements(summaryObjectID, gstruct.IgnoreExtras, gstruct.Elements{
					fmt.Sprintf("%s::%s", f.Namespace.Name, podName): gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
						"IO": pressureDetected("some", 0.1),
					}),
				}))
			}, 2*time.Minute, 15*time.Second).Should(gomega.Succeed())
			framework.ExpectNoError(e2epod.NewPodClient(f).Delete(ctx, pod.Name, metav1.DeleteOptions{}))
		})
	})
})

func getSummaryTestPods(f *framework.Framework, numRestarts int32, names ...string) []*v1.Pod {
	pods := make([]*v1.Pod, 0, len(names))
	for _, name := range names {
		pods = append(pods, &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
			},
			Spec: v1.PodSpec{
				RestartPolicy: v1.RestartPolicyAlways,
				Containers: []v1.Container{
					{
						Name:  "busybox-container",
						Image: busyboxImage,
						SecurityContext: &v1.SecurityContext{
							Capabilities: &v1.Capabilities{
								Add: []v1.Capability{"NET_RAW"},
							},
						},
						Command: getRestartingContainerCommand("/test-empty-dir-mnt", 0, numRestarts, "dd if=/dev/zero of=/outside_the_volume.txt oflag=direct bs=4096 count=2 2>/dev/null; dd if=/outside_the_volume.txt of=/dev/null iflag=direct bs=4096 count=2 2>/dev/null; ping -c 1 google.com; echo 'hello world' >> /test-empty-dir-mnt/file; dd if=/dev/zero of=/dev/null bs=1 count=10000000;"),
						Resources: v1.ResourceRequirements{
							Limits: v1.ResourceList{
								// 必须设置内存限制才能获得 MemoryStats.AvailableBytes
								v1.ResourceMemory: resource.MustParse("80M"),
							},
						},
						VolumeMounts: []v1.VolumeMount{
							{MountPath: "/test-empty-dir-mnt", Name: "test-empty-dir"},
						},
					},
				},
				SecurityContext: &v1.PodSecurityContext{
					SELinuxOptions: &v1.SELinuxOptions{
						Level: "s0",
					},
				},
				Volumes: []v1.Volume{
					// TODO(#28393): Test secret volumes
					// TODO(#28394): Test hostpath volumes
					{Name: "test-empty-dir", VolumeSource: v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}}},
				},
			},
		})
	}
	return pods
}

// gstruct.MatchAllElements 的映射函数
func summaryObjectID(element interface{}) string {
	switch el := element.(type) {
	case kubeletstatsv1alpha1.PodStats:
		return fmt.Sprintf("%s::%s", el.PodRef.Namespace, el.PodRef.Name)
	case kubeletstatsv1alpha1.ContainerStats:
		return el.Name
	case kubeletstatsv1alpha1.VolumeStats:
		return el.Name
	case kubeletstatsv1alpha1.UserDefinedMetric:
		return el.Name
	default:
		framework.Failf("Unknown type: %T", el)
		return "???"
	}
}

// 常用 matcher 组合的便捷函数。
func ptrMatchAllFields(fields gstruct.Fields) types.GomegaMatcher {
	return gstruct.PointTo(gstruct.MatchAllFields(fields))
}

func bounded(lower, upper interface{}) types.GomegaMatcher {
	return gstruct.PointTo(boundedValue(lower, upper))
}

func boundedValue(lower, upper interface{}) types.GomegaMatcher {
	return gomega.And(
		gomega.BeNumerically(">=", lower),
		gomega.BeNumerically("<=", upper))
}

func swapExpectation(upper interface{}) types.GomegaMatcher {
	// 超过此大小后我们认为内存"无限"。它不是
	// MaxInt64，因为内核会取整。
	const maxMemorySize = uint64(1 << 62)

	swapBytesMatcher := gomega.Or(
		gomega.BeNil(),
		bounded(0, upper),
		gstruct.PointTo(gomega.BeNumerically(">=", maxMemorySize)),
	)

	return gomega.Or(
		gomega.BeNil(),
		ptrMatchAllFields(gstruct.Fields{
			"Time":               recent(maxStatsAge),
			"SwapUsageBytes":     swapBytesMatcher,
			"SwapAvailableBytes": swapBytesMatcher,
		}),
	)
}

func recent(d time.Duration) types.GomegaMatcher {
	return gomega.WithTransform(func(t metav1.Time) time.Time {
		return t.Time
	}, gomega.And(
		gomega.BeTemporally(">=", time.Now().Add(-d)),
		// Now() 是测试开始时间而非匹配时间，因此允许多几分钟。
		gomega.BeTemporally("<", time.Now().Add(3*time.Minute))))
}

func recordSystemCgroupProcesses(ctx context.Context) {
	cfg, err := getCurrentKubeletConfig(ctx)
	if err != nil {
		framework.Logf("Failed to read kubelet config: %v", err)
		return
	}
	cgroups := map[string]string{
		"kubelet": cfg.KubeletCgroups,
		"misc":    cfg.SystemCgroups,
	}
	for name, cgroup := range cgroups {
		if cgroup == "" {
			framework.Logf("Skipping unconfigured cgroup %s", name)
			continue
		}

		filePattern := "/sys/fs/cgroup/cpu/%s/cgroup.procs"
		if IsCgroup2UnifiedMode() {
			filePattern = "/sys/fs/cgroup/%s/cgroup.procs"
		}
		pids, err := os.ReadFile(fmt.Sprintf(filePattern, cgroup))
		if err != nil {
			framework.Logf("Failed to read processes in cgroup %s: %v", name, err)
			continue
		}

		framework.Logf("Processes in %s cgroup (%s):", name, cgroup)
		for _, pid := range strings.Fields(string(pids)) {
			path := fmt.Sprintf("/proc/%s/cmdline", pid)
			cmd, err := os.ReadFile(path)
			if err != nil {
				framework.Logf("  ginkgo.Failed to read %s: %v", path, err)
			} else {
				framework.Logf("  %s", cmd)
			}
		}
	}
}

func psiExpectation() types.GomegaMatcher {
	if !utilfeature.DefaultFeatureGate.Enabled(features.KubeletPSI) {
		return gomega.BeNil()
	}
	psiDataExpectation := gstruct.MatchAllFields(gstruct.Fields{
		"Total":  boundedValue(0, uint64(math.MaxUint64)),
		"Avg10":  boundedValue(0, 100),
		"Avg60":  boundedValue(0, 100),
		"Avg300": boundedValue(0, 100),
	})
	return ptrMatchAllFields(gstruct.Fields{
		"Full": psiDataExpectation,
		"Some": psiDataExpectation,
	})
}

func ioExpectation(maxStatsAge time.Duration) types.GomegaMatcher {
	if !utilfeature.DefaultFeatureGate.Enabled(features.KubeletPSI) {
		return gomega.BeNil()
	}
	return gomega.Or(gomega.BeNil(),
		ptrMatchAllFields(gstruct.Fields{
			"Time": recent(maxStatsAge),
			"PSI":  psiExpectation(),
		}))
}

// getStressTestPod 返回用于运行压力测试的 Pod 定义。
// 这遵循测试计划的策略：使用可配置的容器产生负载。
func getStressTestPod(name, containerName string, args []string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1.PodSpec{
			RestartPolicy: v1.RestartPolicyNever,
			Containers: []v1.Container{
				{
					Name:  containerName,
					Image: imageutils.GetE2EImage(imageutils.Agnhost),
					Args:  args,
				},
			},
		},
	}
}

// pressureDetected 是用于验证 PSI 统计是否显示压力的 matcher。
// 测试计划要求断言数值与负载合理相关，
// 而不仅仅是字段存在。
func pressureDetected(level string, threshold float64) types.GomegaMatcher {
	var fields gstruct.Fields
	switch level {
	case "some":
		fields = gstruct.Fields{
			"Some": gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
				"Avg10": gomega.BeNumerically(">", threshold),
				"Total": gomega.BeNumerically(">", uint64(0)),
			}),
		}
	case "full":
		fields = gstruct.Fields{
			"Full": gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
				"Avg10": gomega.BeNumerically(">", threshold),
				"Total": gomega.BeNumerically(">", uint64(0)),
			}),
		}
	default:
		framework.Failf("Unknown pressure level: %s", level)
		return nil
	}

	return gstruct.PointTo(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
		"PSI": gstruct.PointTo(gstruct.MatchFields(gstruct.IgnoreExtras, fields)),
	}))
}

// podMemoryPSIFullTotal 返回给定 Pod 在 /stats/summary 中报告的
// 内存 PSI full 总停顿时间（微秒）。
func podMemoryPSIFullTotal(ctx context.Context, namespace, podName string) (uint64, error) {
	summary, err := getNodeSummary(ctx)
	if err != nil {
		return 0, err
	}
	for _, pod := range summary.Pods {
		if pod.PodRef.Namespace != namespace || pod.PodRef.Name != podName {
			continue
		}
		if pod.Memory == nil || pod.Memory.PSI == nil {
			return 0, fmt.Errorf("pod %s/%s has no memory PSI data in summary", namespace, podName)
		}
		return pod.Memory.PSI.Full.Total, nil
	}
	return 0, fmt.Errorf("pod %s/%s not found in summary", namespace, podName)
}
