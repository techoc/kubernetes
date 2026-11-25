/*
Copyright 2022 The Kubernetes Authors.

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
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gstruct"
	"github.com/onsi/gomega/types"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeletpodresourcesv1 "k8s.io/kubelet/pkg/apis/podresources/v1"
	"k8s.io/kubernetes/pkg/features"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/apis/podresources"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/kubernetes/pkg/kubelet/util"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	admissionapi "k8s.io/pod-security-admission/api"
	"k8s.io/utils/cpuset"
)

var _ = SIGDescribe("CPU Manager Metrics", framework.WithSerial(), feature.CPUManager, func() {
	f := framework.NewDefaultFramework("cpumanager-metrics")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.Context("when querying /metrics", func() {
		var oldCfg *kubeletconfig.KubeletConfiguration
		var testPod *v1.Pod
		var cpuAlloc int64
		var smtLevel int
		var uncoreGroupSize int
		var hasSplitUncore bool

		ginkgo.BeforeEach(func(ctx context.Context) {
			var err error
			if oldCfg == nil {
				oldCfg, err = getCurrentKubeletConfig(ctx)
				framework.ExpectNoError(err)
			}

			fullCPUsOnlyOpt := fmt.Sprintf("option=%s", cpumanager.FullPCPUsOnlyOption)
			_, cpuAlloc, _ := getLocalNodeCPUDetails(ctx, f)
			smtLevel = smtLevelFromSysFS()

			// 非 SMT 系统上，严格的 SMT 对齐无需验证即可直接满足
			if smtLevel < 2 {
				e2eskipper.Skipf("Skipping CPU Manager %s tests since SMT disabled", fullCPUsOnlyOpt)
			}

			// 测试需要分配最多一个完整核心，因此至少需要 2*2=4 个虚拟 CPU
			if cpuAlloc < int64(smtLevel*2) {
				e2eskipper.Skipf("Skipping CPU Manager %s tests since the CPU capacity < 4", fullCPUsOnlyOpt)
			}

			framework.Logf("SMT level %d", smtLevel)

			uncoreGroupSize = getUncoreCPUGroupSize()
			if uncoreGroupSize == 0 {
				hasSplitUncore = false
			} else {
				// 既要有物理上拆分的 Uncore，也要有足够的 CPU 跑出有意义的测试结果，二者缺一不可
				hasSplitUncore = (cpuAlloc > int64(uncoreGroupSize))
			}

			framework.Logf("Uncore Group Size %d; Split Uncore detected=%v", uncoreGroupSize, hasSplitUncore)

			// TODO: 这里假设第一个可用的 CPUID 是 0，大体合理，但还是应该检查节点上的实际情况
			cpuPolicyOptions := map[string]string{
				cpumanager.FullPCPUsOnlyOption:            "true",
				cpumanager.PreferAlignByUnCoreCacheOption: strconv.FormatBool(hasSplitUncore),
			}
			newCfg := configureCPUManagerInKubelet(oldCfg,
				&cpuManagerKubeletArguments{
					policyName:              string(cpumanager.PolicyStatic),
					reservedSystemCPUs:      cpuset.New(0),
					enableCPUManagerOptions: true,
					options:                 cpuPolicyOptions,
				},
			)
			updateKubeletConfig(ctx, f, newCfg, true)
		})

		ginkgo.AfterEach(func(ctx context.Context) {
			if testPod != nil {
				deletePodSyncByName(ctx, f, testPod.Name)
				waitForContainerRemoval(ctx, testPod.Spec.Containers[0].Name, testPod.Name, testPod.Namespace)
			}
			updateKubeletConfig(ctx, f, oldCfg, true)
		})

		ginkgo.It("should report zero pinning counters after a fresh restart", func(ctx context.Context) {
			// BeforeEach 中已更新 kubelet 配置，因此可认为是从干净状态开始。
			// 由于是 [Serial]，也不会有其他测试同时运行 pod。
			ginkgo.By("Checking the cpumanager metrics right after the kubelet restart, with no pods running")

			idFn := makeCustomPairID("scope", "boundary")
			matchResourceMetrics := gstruct.MatchKeys(gstruct.IgnoreExtras, gstruct.Keys{
				"kubelet_cpu_manager_pinning_requests_total": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(0),
				}),
				"kubelet_cpu_manager_pinning_errors_total": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(0),
				}),
				"kubelet_container_aligned_compute_resources_failure_count": gstruct.MatchElements(idFn, gstruct.IgnoreExtras, gstruct.Elements{
					"container::physical_cpu": timelessSample(0),
				}),
			})

			ginkgo.By("Giving the Kubelet time to start up and produce metrics")
			gomega.Eventually(ctx, getKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchResourceMetrics)
			ginkgo.By("Ensuring the metrics match the expectations a few more times")
			gomega.Consistently(ctx, getKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchResourceMetrics)
		})

		ginkgo.It("should report pinning failures when the cpumanager allocation is known to fail", func(ctx context.Context) {
			ginkgo.By("Creating the test pod which will be rejected for SMTAlignmentError")
			testPod = e2epod.NewPodClient(f).Create(ctx, makeGuaranteedCPUExclusiveSleeperPod("smt-align-err", 1))

			// BeforeEach 中已更新 kubelet 配置，因此可认为是从干净状态开始。
			// 由于是 [Serial]，也不会有其他测试同时运行 pod。
			ginkgo.By("Checking the cpumanager metrics right after the kubelet restart, with pod failed to admit")

			idFn := makeCustomPairID("scope", "boundary")
			matchResourceMetrics := gstruct.MatchKeys(gstruct.IgnoreExtras, gstruct.Keys{
				"kubelet_cpu_manager_pinning_requests_total": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(1),
				}),
				"kubelet_cpu_manager_pinning_errors_total": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(1),
				}),
				"kubelet_container_aligned_compute_resources_failure_count": gstruct.MatchElements(idFn, gstruct.IgnoreExtras, gstruct.Elements{
					"container::physical_cpu": timelessSample(1),
				}),
			})

			ginkgo.By("Giving the Kubelet time to start up and produce metrics")
			gomega.Eventually(ctx, getKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchResourceMetrics)
			ginkgo.By("Ensuring the metrics match the expectations a few more times")
			gomega.Consistently(ctx, getKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchResourceMetrics)
		})

		ginkgo.It("should not report any pinning failures when the cpumanager allocation is expected to succeed", func(ctx context.Context) {
			ginkgo.By("Creating the test pod")
			testPod = e2epod.NewPodClient(f).Create(ctx, makeGuaranteedCPUExclusiveSleeperPod("smt-align-ok", smtLevel))

			// BeforeEach 中已更新 kubelet 配置，因此可认为是从干净状态开始。
			// 由于是 [Serial]，也不会有其他测试同时运行 pod。
			ginkgo.By("Checking the cpumanager metrics right after the kubelet restart, with pod should be admitted")

			idFn := makeCustomPairID("scope", "boundary")
			matchResourceMetrics := gstruct.MatchKeys(gstruct.IgnoreExtras, gstruct.Keys{
				"kubelet_cpu_manager_pinning_requests_total": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(1),
				}),
				"kubelet_cpu_manager_pinning_errors_total": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(0),
				}),
				"kubelet_container_aligned_compute_resources_failure_count": gstruct.MatchElements(idFn, gstruct.IgnoreExtras, gstruct.Elements{
					"container::physical_cpu": timelessSample(0),
				}),
			})

			ginkgo.By("Giving the Kubelet time to start up and produce metrics")
			gomega.Eventually(ctx, getKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchResourceMetrics)
			ginkgo.By("Ensuring the metrics match the expectations a few more times")
			gomega.Consistently(ctx, getKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchResourceMetrics)
		})

		ginkgo.It("should return updated SMT alignment counters when pod successfully run", func(ctx context.Context) {
			ginkgo.By("Creating the test pod")
			testPod = e2epod.NewPodClient(f).Create(ctx, makeGuaranteedCPUExclusiveSleeperPod("count-align-smt-ok", smtLevel))

			// BeforeEach 中已更新 kubelet 配置，因此可认为是从干净状态开始。
			// 由于是 [Serial]，也不会有其他测试同时运行 pod。
			ginkgo.By("Checking the cpumanager metrics right after the kubelet restart, with pod should be admitted")

			idFn := makeCustomPairID("scope", "boundary")
			matchAlignmentMetrics := gstruct.MatchKeys(gstruct.IgnoreExtras, gstruct.Keys{
				"kubelet_container_aligned_compute_resources_count": gstruct.MatchElements(idFn, gstruct.IgnoreExtras, gstruct.Elements{
					"container::physical_cpu": timelessSample(1),
				}),
				"kubelet_container_aligned_compute_resources_failure_count": gstruct.MatchElements(idFn, gstruct.IgnoreExtras, gstruct.Elements{
					"container::physical_cpu": timelessSample(0),
				}),
			})

			ginkgo.By("Giving the Kubelet time to update the alignment metrics")
			gomega.Eventually(ctx, getKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchAlignmentMetrics)
			ginkgo.By("Ensuring the metrics match the expectations about alignment metrics a few more times")
			gomega.Consistently(ctx, getKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchAlignmentMetrics)
		})

		ginkgo.It("should report the default idle cpu pool size", func(ctx context.Context) {
			ginkgo.By("Querying the podresources endpoint to get the baseline")
			endpoint, err := util.LocalEndpoint(defaultPodResourcesPath, podresources.Socket)
			framework.ExpectNoError(err, "LocalEndpoint() failed err: %v", err)

			cli, conn, err := podresources.GetV1Client(ctx, endpoint, defaultPodResourcesTimeout, defaultPodResourcesMaxSize)
			framework.ExpectNoError(err, "GetV1Client() failed err: %v", err)
			defer func() {
				framework.ExpectNoError(conn.Close())
			}()

			ginkgo.By("Checking the pool allocatable resources from the kubelet")
			resp, err := cli.GetAllocatableResources(ctx, &kubeletpodresourcesv1.AllocatableResourcesRequest{})
			framework.ExpectNoError(err, "failed to get the kubelet allocatable resources")
			allocatableCPUs, _ := demuxCPUsAndDevicesFromGetAllocatableResources(resp)

			matchResourceMetrics := gstruct.MatchKeys(gstruct.IgnoreExtras, gstruct.Keys{
				"kubelet_cpu_manager_shared_pool_size_millicores": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(int(allocatableCPUs.Size() * 1000)),
				}),
				"kubelet_cpu_manager_exclusive_cpu_allocation_count": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(0),
				}),
			})

			ginkgo.By("Giving the Kubelet time to start up and produce metrics about idle pool size")
			gomega.Eventually(ctx, getKubeletMetrics, 1*time.Minute, 10*time.Second).Should(matchResourceMetrics)
			ginkgo.By("Ensuring the metrics match the expectations about idle pool size a few more times")
			gomega.Consistently(ctx, getKubeletMetrics, 30*time.Second, 10*time.Second).Should(matchResourceMetrics)
		})

		ginkgo.It("should report mutating cpu pool size when handling guaranteed pods", func(ctx context.Context) {
			ginkgo.By("Querying the podresources endpoint to get the baseline")
			endpoint, err := util.LocalEndpoint(defaultPodResourcesPath, podresources.Socket)
			framework.ExpectNoError(err, "LocalEndpoint() failed err: %v", err)

			cli, conn, err := podresources.GetV1Client(ctx, endpoint, defaultPodResourcesTimeout, defaultPodResourcesMaxSize)
			framework.ExpectNoError(err, "GetV1Client() failed err: %v", err)
			defer func() {
				framework.ExpectNoError(conn.Close())
			}()

			ginkgo.By("Checking the pool allocatable resources from the kubelet")
			resp, err := cli.GetAllocatableResources(ctx, &kubeletpodresourcesv1.AllocatableResourcesRequest{})
			framework.ExpectNoError(err, "failed to get the kubelet allocatable resources")
			allocatableCPUs, _ := demuxCPUsAndDevicesFromGetAllocatableResources(resp)

			allocatableCPUsIdleMillis := int(allocatableCPUs.Size() * 1000)

			matchResourceMetricsIdle := gstruct.MatchKeys(gstruct.IgnoreExtras, gstruct.Keys{
				"kubelet_cpu_manager_shared_pool_size_millicores": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(allocatableCPUsIdleMillis),
				}),
				"kubelet_cpu_manager_exclusive_cpu_allocation_count": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(0),
				}),
			})
			ginkgo.By(fmt.Sprintf("Pool allocatable resources from the kubelet: shared pool %d cpus %d millis", allocatableCPUs.Size(), allocatableCPUsIdleMillis))

			ginkgo.By("Giving the Kubelet time to start up and produce metrics about idle pool size")
			gomega.Eventually(ctx, getKubeletMetrics, 1*time.Minute, 10*time.Second).Should(matchResourceMetricsIdle)
			ginkgo.By("Ensuring the metrics match the expectations about idle pool size a few more times")
			gomega.Consistently(ctx, getKubeletMetrics, 30*time.Second, 10*time.Second).Should(matchResourceMetricsIdle)

			ginkgo.By("Creating the test pod to consume exclusive cpus from the pool")
			testPod = e2epod.NewPodClient(f).CreateSync(ctx, makeGuaranteedCPUExclusiveSleeperPod("smt-cpupool", smtLevel))

			matchResourceMetricsBusy := gstruct.MatchKeys(gstruct.IgnoreExtras, gstruct.Keys{
				"kubelet_cpu_manager_shared_pool_size_millicores": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(allocatableCPUsIdleMillis - (smtLevel * 1000)),
				}),
				"kubelet_cpu_manager_exclusive_cpu_allocation_count": gstruct.MatchAllElements(nodeID, gstruct.Elements{
					"": timelessSample(smtLevel),
				}),
			})

			ginkgo.By("Giving the Kubelet time to start up and produce metrics")
			gomega.Eventually(ctx, getKubeletMetrics, 1*time.Minute, 10*time.Second).Should(matchResourceMetricsBusy)
			ginkgo.By("Ensuring the metrics match the expectations a few more times")
			gomega.Consistently(ctx, getKubeletMetrics, 30*time.Second, 10*time.Second).Should(matchResourceMetricsBusy)

			deletePodSyncByName(ctx, f, testPod.Name)

			ginkgo.By("Giving the Kubelet time to start up and produce metrics")
			gomega.Eventually(ctx, getKubeletMetrics, 1*time.Minute, 10*time.Second).Should(matchResourceMetricsIdle)
			ginkgo.By("Ensuring the metrics match the expectations a few more times")
			gomega.Consistently(ctx, getKubeletMetrics, 30*time.Second, 10*time.Second).Should(matchResourceMetricsIdle)
		})

		ginkgo.It("should update alignment counters when pod successfully run taking less than uncore cache group", func(ctx context.Context) {
			if !hasSplitUncore {
				e2eskipper.Skip("Skipping CPU Manager uncore alignment test - not split Uncore detected")
			}
			if smtLevel >= uncoreGroupSize {
				// 按照 uncore cache 的定义（跨核心块共享的缓存，同一块内的超线程共享独占缓存块），
				// 这种情况说不通，必然是配置或探测出了问题，直接失败以暴露问题
				framework.Failf("Failed preconditions for CPU Manager uncore alignment test - SMT level more than Uncore group size - this is unexpected")
			}

			ginkgo.By("Creating the test pod")
			testPod = e2epod.NewPodClient(f).Create(ctx, makeGuaranteedCPUExclusiveSleeperPod("count-align-uncore-ok", smtLevel))

			// BeforeEach 中已更新 kubelet 配置，因此可认为是从干净状态开始。
			// 由于是 [Serial]，也不会有其他测试同时运行 pod。
			ginkgo.By("Checking the cpumanager metrics right after the kubelet restart, with pod should be admitted")

			idFn := makeCustomPairID("scope", "boundary")
			matchAlignmentMetrics := gstruct.MatchKeys(gstruct.IgnoreExtras, gstruct.Keys{
				"kubelet_container_aligned_compute_resources_count": gstruct.MatchElements(idFn, gstruct.IgnoreExtras, gstruct.Elements{
					"container::uncore_cache": timelessSample(1),
				}),
			})

			ginkgo.By("Giving the Kubelet time to update the alignment metrics")
			gomega.Eventually(ctx, getKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchAlignmentMetrics)
			ginkgo.By("Ensuring the metrics match the expectations about alignment metrics a few more times")
			gomega.Consistently(ctx, getKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchAlignmentMetrics)
		})

		ginkgo.It("should update alignment counters when pod successfully run taking a full uncore cache group", func(ctx context.Context) {
			if !hasSplitUncore {
				e2eskipper.Skip("Skipping CPU Manager uncore alignment test - not split Uncore detected")
			}

			ginkgo.By("Creating the test pod")
			testPod = e2epod.NewPodClient(f).Create(ctx, makeGuaranteedCPUExclusiveSleeperPod("count-align-uncore-ok", uncoreGroupSize))

			// BeforeEach 中已更新 kubelet 配置，因此可认为是从干净状态开始。
			// 由于是 [Serial]，也不会有其他测试同时运行 pod。
			ginkgo.By("Checking the cpumanager metrics right after the kubelet restart, with pod should be admitted")

			idFn := makeCustomPairID("scope", "boundary")
			matchAlignmentMetrics := gstruct.MatchKeys(gstruct.IgnoreExtras, gstruct.Keys{
				"kubelet_container_aligned_compute_resources_count": gstruct.MatchElements(idFn, gstruct.IgnoreExtras, gstruct.Elements{
					"container::uncore_cache": timelessSample(1),
				}),
			})

			ginkgo.By("Giving the Kubelet time to update the alignment metrics")
			gomega.Eventually(ctx, getKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchAlignmentMetrics)
			ginkgo.By("Ensuring the metrics match the expectations about alignment metrics a few more times")
			gomega.Consistently(ctx, getKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchAlignmentMetrics)
		})

		ginkgo.It("should not update alignment counters when pod successfully run taking more than a uncore cache group", func(ctx context.Context) {
			if !hasSplitUncore {
				e2eskipper.Skip("Skipping CPU Manager uncore alignment test - not split Uncore detected")
			}
			if cpuAlloc < int64(uncoreGroupSize+smtLevel) {
				e2eskipper.Skipf("Skipping CPU Manager uncore alignment test - not enough available CPUs (needs %d allocatable %d)", uncoreGroupSize+smtLevel, cpuAlloc)
			}

			ginkgo.By("Creating the test pod")
			testPod = e2epod.NewPodClient(f).Create(ctx, makeGuaranteedCPUExclusiveSleeperPod("count-align-uncore-ok", uncoreGroupSize+smtLevel))

			// BeforeEach 中已更新 kubelet 配置，因此可认为是从干净状态开始。
			// 由于是 [Serial]，也不会有其他测试同时运行 pod。
			ginkgo.By("Checking the cpumanager metrics right after the kubelet restart, with pod should be admitted")

			idFn := makeCustomPairID("scope", "boundary")
			matchAlignmentMetrics := gstruct.MatchKeys(gstruct.IgnoreExtras, gstruct.Keys{
				"kubelet_container_aligned_compute_resources_count": gstruct.MatchElements(idFn, gstruct.IgnoreExtras, gstruct.Elements{
					"container::uncore_cache": timelessSample(0),
				}),
			})

			ginkgo.By("Giving the Kubelet time to update the alignment metrics")
			gomega.Eventually(ctx, getKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchAlignmentMetrics)
			ginkgo.By("Ensuring the metrics match the expectations about alignment metrics a few more times")
			gomega.Consistently(ctx, getKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchAlignmentMetrics)
		})
		ginkgo.It("should report zero counters for allocation per NUMA after a fresh restart", func(ctx context.Context) {

			cpuPolicyOptions := map[string]string{
				cpumanager.DistributeCPUsAcrossNUMAOption: "true",
				cpumanager.FullPCPUsOnlyOption:            "true",
			}
			newCfg := configureCPUManagerInKubelet(oldCfg,
				&cpuManagerKubeletArguments{
					policyName:              string(cpumanager.PolicyStatic),
					reservedSystemCPUs:      cpuset.New(0),
					enableCPUManagerOptions: true,
					options:                 cpuPolicyOptions,
				},
			)

			updateKubeletConfig(ctx, f, newCfg, true)

			ginkgo.By("Checking the cpumanager allocation per NUMA metric right after the kubelet restart, with no pods running")
			numaNodes, _, _, _ := hostCheck()

			framework.Logf("numaNodes on the system %d", numaNodes)

			keys := make(map[interface{}]types.GomegaMatcher)
			idFn := makeCustomLabelID(metrics.AlignedNUMANode)

			for i := range numaNodes {
				keys["kubelet_cpu_manager_allocation_per_numa"] = gstruct.MatchAllElements(idFn, gstruct.Elements{
					fmt.Sprintf("%d", i): timelessSample(0),
				})

			}

			matchSpreadMetrics := gstruct.MatchKeys(gstruct.IgnoreExtras, keys)

			ginkgo.By("Giving the Kubelet time to start up and produce metrics")
			gomega.Eventually(ctx, getKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchSpreadMetrics)
			ginkgo.By("Ensuring the metrics match the expectations a few more times")
			gomega.Consistently(ctx, getKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchSpreadMetrics)

		})

		ginkgo.It("should report allocation per NUMA metric when handling guaranteed pods", func(ctx context.Context) {
			cpuPolicyOptions := map[string]string{
				cpumanager.DistributeCPUsAcrossNUMAOption: "true",
				cpumanager.FullPCPUsOnlyOption:            "true",
			}
			newCfg := configureCPUManagerInKubelet(oldCfg,
				&cpuManagerKubeletArguments{
					policyName:              string(cpumanager.PolicyStatic),
					reservedSystemCPUs:      cpuset.New(0),
					enableCPUManagerOptions: true,
					options:                 cpuPolicyOptions,
				},
			)

			updateKubeletConfig(ctx, f, newCfg, true)

			numaNodes, _, _, cpusNumPerNUMA := hostCheck()

			smtLevel = smtLevelFromSysFS()
			framework.Logf("SMT Level on the system %d", smtLevel)

			ginkgo.By("Querying the podresources endpoint to get the baseline")
			endpoint, err := util.LocalEndpoint(defaultPodResourcesPath, podresources.Socket)
			framework.ExpectNoError(err, "LocalEndpoint() failed err: %v", err)

			cli, conn, err := podresources.GetV1Client(ctx, endpoint, defaultPodResourcesTimeout, defaultPodResourcesMaxSize)
			framework.ExpectNoError(err, "GetV1Client() failed err: %v", err)
			defer func() {
				framework.ExpectNoError(conn.Close())
			}()

			ginkgo.By("Checking the pool allocatable resources from the kubelet")
			resp, err := cli.GetAllocatableResources(ctx, &kubeletpodresourcesv1.AllocatableResourcesRequest{})
			framework.ExpectNoError(err, "failed to get the kubelet allocatable resources")
			allocatableCPUs, _ := demuxCPUsAndDevicesFromGetAllocatableResources(resp)

			// 'distribute-cpus-across-numa' 策略选项保证当分配需要跨多个 NUMA node 时，
			// CPU 会均匀分布在各个 NUMA node 上。
			// 因此要保证 CPU request 超过单个 NUMA node 所能容纳的 CPU 数量。
			// cpuRequest 的选择需满足：
			// 1. CPURequest > cpusNumPerNUMA
			// 2. 不占满节点所有 CPU，为 reserved CPU 留出空间
			// 3. CPURequest 是 NUMA node 数量的倍数，才能在各 NUMA node 间均匀分配
			//
			// 总结：cpusNumPerNUMA < CPURequest < ((cpusNumPerNuma * numaNodes) - reservedCPUscount)
			// 综合这些约束，取 CPURequest= (cpusNumPerNUMA-smtLevel)*numaNodes
			cpuRequest := (cpusNumPerNUMA - smtLevel) * numaNodes
			if cpuRequest > allocatableCPUs.Size() {
				e2eskipper.Skipf("Pod requesting %d CPUs which is more than allocatable CPUs:%d", cpuRequest, allocatableCPUs.Size())
			}

			ginkgo.By("Creating the test pod")
			testPod = e2epod.NewPodClient(f).Create(ctx, makeGuaranteedCPUExclusiveSleeperPod("test-pod-allocation-per-numa", cpuRequest))

			ginkgo.By("Checking the cpumanager metrics after pod creation")

			keys := make(map[interface{}]types.GomegaMatcher)
			idFn := makeCustomLabelID(metrics.AlignedNUMANode)

			// 在干净环境且没有其他 pod 运行时，若启用了 distribute-across-numa 策略选项
			for i := range numaNodes {
				keys["kubelet_cpu_manager_allocation_per_numa"] = gstruct.MatchAllElements(idFn, gstruct.Elements{
					fmt.Sprintf("%d", i): timelessSample(2),
				})

			}

			matchSpreadMetrics := gstruct.MatchKeys(gstruct.IgnoreExtras, keys)

			ginkgo.By("Giving the Kubelet time to start up and produce metrics")
			gomega.Eventually(ctx, getKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchSpreadMetrics)
			ginkgo.By("Ensuring the metrics match the expectations a few more times")
			gomega.Consistently(ctx, getKubeletMetrics, 1*time.Minute, 15*time.Second).Should(matchSpreadMetrics)
		})

	})
})

func makeGuaranteedCPUExclusiveSleeperPod(name string, cpus int) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name + "-pod",
		},
		Spec: v1.PodSpec{
			RestartPolicy: v1.RestartPolicyNever,
			Containers: []v1.Container{
				{
					Name:  name + "-cnt",
					Image: busyboxImage,
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse(fmt.Sprintf("%d", cpus)),
							v1.ResourceMemory: resource.MustParse("64Mi"),
						},
						Limits: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse(fmt.Sprintf("%d", cpus)),
							v1.ResourceMemory: resource.MustParse("64Mi"),
						},
					},
					Command: []string{"sh", "-c", "sleep 1d"},
				},
			},
		},
	}
}

func timelessSample(value interface{}) types.GomegaMatcher {
	return gstruct.PointTo(gstruct.MatchAllFields(gstruct.Fields{
		// Metric 已在匹配 Id 时校验过
		"Metric":    gstruct.Ignore(),
		"Value":     gomega.BeNumerically("==", value),
		"Timestamp": gstruct.Ignore(),
		"Histogram": gstruct.Ignore(),
	}))
}

var _ = SIGDescribe("CPU Manager Metrics Pod Level Resources", ginkgo.Ordered, ginkgo.ContinueOnFailure, framework.WithSerial(), feature.CPUManager, feature.PodLevelResources, feature.PodLevelResourceManagers, framework.WithFeatureGate(features.PodLevelResources), framework.WithFeatureGate(features.PodLevelResourceManagers), func() {
	f := framework.NewDefaultFramework("cpu-manager-metrics-pod-level-resources")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	var oldCfg *kubeletconfig.KubeletConfiguration
	var testPod *v1.Pod

	ginkgo.BeforeEach(func(ctx context.Context) {
		var err error
		if oldCfg == nil {
			oldCfg, err = getCurrentKubeletConfig(ctx)
			framework.ExpectNoError(err)
		}
		_, cpuAlloc, _ := getLocalNodeCPUDetails(ctx, f)
		if cpuAlloc < 3 {
			e2eskipper.Skipf("Skipping CPU Manager Metrics Pod Level Resources tests since the CPU capacity %d < 3", cpuAlloc)
		}
	})

	ginkgo.AfterEach(func(ctx context.Context) {
		if testPod != nil {
			deletePodSyncByName(ctx, f, testPod.Name)
			waitForContainerRemoval(ctx, testPod.Spec.Containers[0].Name, testPod.Name, testPod.Namespace)
		}
		updateKubeletConfig(ctx, f, oldCfg, true)
	})

	ginkgo.It("should increment allocation and container assignment metrics in pod scope", func(ctx context.Context) {
		newCfg := configureCPUManagerInKubelet(oldCfg,
			&cpuManagerKubeletArguments{
				policyName:                     string(cpumanager.PolicyStatic),
				topologyManagerPolicy:          string(topologymanager.PolicyRestricted),
				topologyManagerScope:           string(topologymanager.PodTopologyScope),
				enablePodLevelResources:        true,
				enablePodLevelResourceManagers: true,
				reservedSystemCPUs:             cpuset.New(0),
			},
		)
		updateKubeletConfig(ctx, f, newCfg, true)

		ginkgo.By("Getting baseline metrics")
		baselineMetrics, err := getKubeletMetrics(ctx)
		framework.ExpectNoError(err)

		baseAllocations, err := getCounterMetricValue(baselineMetrics, "kubelet_resource_manager_allocations_total", map[string]string{"resource_name": "cpu", "source": "pod"})
		if !errors.Is(err, ErrMetricNotFound) {
			framework.ExpectNoError(err)
		}

		baseExclusiveAssignments, err := getCounterMetricValue(baselineMetrics, "kubelet_resource_manager_container_assignments_total", map[string]string{"resource_name": "cpu", "assignment_type": "pod_exclusive"})
		if !errors.Is(err, ErrMetricNotFound) {
			framework.ExpectNoError(err)
		}

		baseSharedAssignments, err := getCounterMetricValue(baselineMetrics, "kubelet_resource_manager_container_assignments_total", map[string]string{"resource_name": "cpu", "assignment_type": "pod_shared"})
		if !errors.Is(err, ErrMetricNotFound) {
			framework.ExpectNoError(err)
		}

		ginkgo.By("Creating a Guaranteed pod with pod-level resources and a mix of guaranteed and non-guaranteed containers")
		testPod = &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "plrm-metrics-pod",
			},
			Spec: v1.PodSpec{
				RestartPolicy: v1.RestartPolicyNever,
				Containers: []v1.Container{
					{
						Name:  "exclusive-container",
						Image: busyboxImage,
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("2"),
								v1.ResourceMemory: resource.MustParse("64Mi"),
							},
							Limits: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("2"),
								v1.ResourceMemory: resource.MustParse("64Mi"),
							},
						},
						Command: []string{"sh", "-c", "sleep 1d"},
					},
					{
						Name:    "shared-container",
						Image:   busyboxImage,
						Command: []string{"sh", "-c", "sleep 1d"},
					},
				},
			},
		}
		testPod.Spec.Resources = &v1.ResourceRequirements{
			Requests: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("3"),
				v1.ResourceMemory: resource.MustParse("128Mi"),
			},
			Limits: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("3"),
				v1.ResourceMemory: resource.MustParse("128Mi"),
			},
		}

		testPod = e2epod.NewPodClient(f).CreateSync(ctx, testPod)

		ginkgo.By("Verifying metrics show correct updates")
		gomega.Eventually(ctx, func(ctx context.Context) error {
			currentMetrics, err := getKubeletMetrics(ctx)
			if err != nil {
				return err
			}

			allocations, err := getCounterMetricValue(currentMetrics, "kubelet_resource_manager_allocations_total", map[string]string{"resource_name": "cpu", "source": "pod"})
			if err != nil {
				return err
			}

			exclusiveAssignments, err := getCounterMetricValue(currentMetrics, "kubelet_resource_manager_container_assignments_total", map[string]string{"resource_name": "cpu", "assignment_type": "pod_exclusive"})
			if err != nil {
				return err
			}

			sharedAssignments, err := getCounterMetricValue(currentMetrics, "kubelet_resource_manager_container_assignments_total", map[string]string{"resource_name": "cpu", "assignment_type": "pod_shared"})
			if err != nil {
				return err
			}

			// pod scope 下 2 个容器（1 个 exclusive、1 个 shared）预期的指标增量：
			// - 2 次 allocation：pod 级资源管理器分配时为每个容器各处理一次。
			// - 1 次 pod_exclusive assignment：分配给请求独占 CPU 的容器。
			// - 1 次 pod_shared assignment：分配给使用 pod 共享池的容器。
			if allocations-baseAllocations != 2 {
				return fmt.Errorf("expected 2 new allocations, got %v", allocations-baseAllocations)
			}
			if exclusiveAssignments-baseExclusiveAssignments != 1 {
				return fmt.Errorf("expected 1 new exclusive assignment, got %v", exclusiveAssignments-baseExclusiveAssignments)
			}
			if sharedAssignments-baseSharedAssignments != 1 {
				return fmt.Errorf("expected 1 new shared assignment, got %v", sharedAssignments-baseSharedAssignments)
			}
			return nil
		}).WithTimeout(2 * time.Minute).WithPolling(10 * time.Second).Should(gomega.Succeed())
	})

	ginkgo.It("should increment allocation and container assignment metrics in container scope", func(ctx context.Context) {
		newCfg := configureCPUManagerInKubelet(oldCfg,
			&cpuManagerKubeletArguments{
				policyName:                     string(cpumanager.PolicyStatic),
				topologyManagerPolicy:          string(topologymanager.PolicyRestricted),
				topologyManagerScope:           string(topologymanager.ContainerTopologyScope),
				enablePodLevelResources:        true,
				enablePodLevelResourceManagers: true,
				reservedSystemCPUs:             cpuset.New(0),
			},
		)
		updateKubeletConfig(ctx, f, newCfg, true)

		ginkgo.By("Getting baseline metrics")
		baselineMetrics, err := getKubeletMetrics(ctx)
		framework.ExpectNoError(err)

		baseAllocations, err := getCounterMetricValue(baselineMetrics, "kubelet_resource_manager_allocations_total", map[string]string{"resource_name": "cpu", "source": "node"})
		if !errors.Is(err, ErrMetricNotFound) {
			framework.ExpectNoError(err)
		}

		baseExclusiveAssignments, err := getCounterMetricValue(baselineMetrics, "kubelet_resource_manager_container_assignments_total", map[string]string{"resource_name": "cpu", "assignment_type": "node_exclusive"})
		if !errors.Is(err, ErrMetricNotFound) {
			framework.ExpectNoError(err)
		}

		ginkgo.By("Creating a Guaranteed pod with pod-level resources and a mix of guaranteed and non-guaranteed containers")
		testPod = &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "plrm-metrics-pod-container-scope",
			},
			Spec: v1.PodSpec{
				RestartPolicy: v1.RestartPolicyNever,
				Containers: []v1.Container{
					{
						Name:  "exclusive-container",
						Image: busyboxImage,
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("2"),
								v1.ResourceMemory: resource.MustParse("64Mi"),
							},
							Limits: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("2"),
								v1.ResourceMemory: resource.MustParse("64Mi"),
							},
						},
						Command: []string{"sh", "-c", "sleep 1d"},
					},
					{
						Name:    "shared-container",
						Image:   busyboxImage,
						Command: []string{"sh", "-c", "sleep 1d"},
					},
				},
			},
		}
		testPod.Spec.Resources = &v1.ResourceRequirements{
			Requests: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("3"),
				v1.ResourceMemory: resource.MustParse("128Mi"),
			},
			Limits: v1.ResourceList{
				v1.ResourceCPU:    resource.MustParse("3"),
				v1.ResourceMemory: resource.MustParse("128Mi"),
			},
		}

		testPod = e2epod.NewPodClient(f).CreateSync(ctx, testPod)

		ginkgo.By("Verifying metrics show correct updates in container scope")
		gomega.Eventually(ctx, func(ctx context.Context) error {
			currentMetrics, err := getKubeletMetrics(ctx)
			if err != nil {
				return err
			}

			allocations, err := getCounterMetricValue(currentMetrics, "kubelet_resource_manager_allocations_total", map[string]string{"resource_name": "cpu", "source": "node"})
			if err != nil {
				return err
			}

			exclusiveAssignments, err := getCounterMetricValue(currentMetrics, "kubelet_resource_manager_container_assignments_total", map[string]string{"resource_name": "cpu", "assignment_type": "node_exclusive"})
			if err != nil {
				return err
			}

			// container scope 下 2 个容器（1 个 exclusive、1 个未声明 request）预期的指标增量：
			// - 1 次 allocation：container scope 下未声明资源请求的容器不参与分配。
			// - 1 次 node_exclusive assignment：分配给请求独占 CPU 的容器。
			if allocations-baseAllocations != 1 {
				return fmt.Errorf("expected 1 new allocation, got %v", allocations-baseAllocations)
			}
			if exclusiveAssignments-baseExclusiveAssignments != 1 {
				return fmt.Errorf("expected 1 new node_exclusive assignment, got %v", exclusiveAssignments-baseExclusiveAssignments)
			}
			return nil
		}).WithTimeout(2 * time.Minute).WithPolling(10 * time.Second).Should(gomega.Succeed())
	})
})
