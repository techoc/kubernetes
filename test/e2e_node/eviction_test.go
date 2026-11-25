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
	"path/filepath"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	kubeletstatsv1alpha1 "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/eviction"
	evictionapi "k8s.io/kubernetes/pkg/kubelet/eviction/api"
	kubeletmetrics "k8s.io/kubernetes/pkg/kubelet/metrics"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	testutils "k8s.io/kubernetes/test/utils"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"
	"k8s.io/utils/ptr"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

// Eviction 策略见：
// https://github.com/kubernetes/design-proposals-archive/blob/main/node/kubelet-eviction.md

const (
	postTestConditionMonitoringPeriod = 1 * time.Minute
	evictionPollInterval              = 2 * time.Second
	pressureDisappearTimeout          = 10 * time.Minute
	// pressure condition 往往在驱逐之后才显现，因为 kubelet 只会周期性地更新
	// node condition。
	// 驱逐后等待这段时间，以确保熬过这个延迟。
	pressureDelay     = 20 * time.Second
	testContextFmt    = "when we run containers that should cause %s"
	noPressure        = v1.NodeConditionType("NoPressure")
	lotsOfDisk        = 10240      // 10 Gb，单位为 Mb
	lotsOfFiles       = 1000000000 // 10 亿
	resourceInodes    = v1.ResourceName("inodes")
	noStarvedResource = v1.ResourceName("none")
)

// InodeEviction 测试节点在遇到 disk pressure 时是否只驱逐应当承担责任的 Pod。
// 通过耗尽节点上的所有 inode 来触发 disk pressure。
var _ = SIGDescribe("InodeEviction", framework.WithSlow(), framework.WithSerial(), framework.WithDisruptive(), feature.Eviction, func() {
	f := framework.NewDefaultFramework("inode-eviction-test")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	const expectedNodeCondition = v1.NodeDiskPressure
	const expectedStarvedResource = resourceInodes
	const pressureTimeout = 15 * time.Minute
	const inodesToThreshold = uint64(100000)
	ginkgo.Context(fmt.Sprintf(testContextFmt, expectedNodeCondition), func() {
		tempSetCurrentKubeletConfig(f, func(ctx context.Context, initialConfig *kubeletconfig.KubeletConfiguration) {
			// 把驱逐阈值设为 inodesFree - inodesToThreshold，这样消耗掉 inodesToThreshold 个 inode 就会触发驱逐。
			summary := eventuallyGetSummary(ctx)
			inodesFree := *summary.Node.Fs.InodesFree
			if inodesFree <= inodesToThreshold {
				e2eskipper.Skipf("Too few inodes free on the host for the InodeEviction test to run")
			}
			initialConfig.EvictionHard = map[string]string{string(evictionapi.SignalNodeFsInodesFree): fmt.Sprintf("%d", inodesFree-inodesToThreshold)}
			initialConfig.EvictionMinimumReclaim = map[string]string{}
		})
		runEvictionTest(f, pressureTimeout, expectedNodeCondition, expectedStarvedResource, logInodeMetrics, []podEvictSpec{
			{
				evictionPriority: 1,
				// TODO(#127864): 容器 runtime 在 Pod 被驱逐后可能不会立即释放资源，
				// 导致测试失败。我们提供一个 emptyDir volume，以避开对 runtime 行为的依赖。
				pod: inodeConsumingPod("volume-inode-hog", lotsOfFiles, &v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}}, true),
			},
			{
				evictionPriority: 0,
				pod:              innocentPod(),
			},
		})
	})
})

// ImageGCNoEviction 测试 eviction manager 能否通过 image garbage collection
// 回收资源（inode）来避免驱逐。
// 通过消耗节点上大量 inode 来触发 disk pressure。
// 在运行测试负载前预先拉取镜像，确保 image garbage collector
// 可以删除这些镜像以避免驱逐。
var _ = SIGDescribe("ImageGCNoEviction", framework.WithSlow(), framework.WithSerial(), framework.WithDisruptive(), feature.Eviction, func() {
	f := framework.NewDefaultFramework("image-gc-eviction-test")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	const pressureTimeout = 10 * time.Minute
	const expectedNodeCondition = v1.NodeDiskPressure
	const expectedStarvedResource = resourceInodes
	const inodesToThreshold = uint64(100000)
	ginkgo.Context(fmt.Sprintf(testContextFmt, expectedNodeCondition), func() {
		prepull := func(ctx context.Context) {
			// 预先拉取镜像，让 image garbage collector 在回收资源时可以删除它们
			err := PrePullAllImages(ctx)
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		}
		ginkgo.BeforeEach(prepull)
		if framework.TestContext.PrepullImages {
			ginkgo.AfterEach(prepull)
		}

		tempSetCurrentKubeletConfig(f, func(ctx context.Context, initialConfig *kubeletconfig.KubeletConfiguration) {
			// 把驱逐阈值设为 inodesFree - inodesToThreshold，这样消耗掉 inodesToThreshold 个 inode 就会触发驱逐。
			summary := eventuallyGetSummary(ctx)
			inodesFree := *summary.Node.Fs.InodesFree
			if inodesFree <= inodesToThreshold {
				e2eskipper.Skipf("Too few inodes free on the host for the InodeEviction test to run")
			}
			framework.Logf("Setting eviction threshold to %d inodes", inodesFree-inodesToThreshold)
			initialConfig.EvictionHard = map[string]string{string(evictionapi.SignalNodeFsInodesFree): fmt.Sprintf("%d", inodesFree-inodesToThreshold)}
			initialConfig.EvictionMinimumReclaim = map[string]string{}
		})
		// 消耗足够多的 inode 以触发 disk pressure，
		// 但预期 image garbage collection 能回收足够的空间，从而避免驱逐
		runEvictionTest(f, pressureTimeout, expectedNodeCondition, expectedStarvedResource, logInodeMetrics, []podEvictSpec{
			{
				evictionPriority: 0,
				pod:              inodeConsumingPod("container-inode", 110000, nil, true),
			},
		})
	})
})

// ImageGCTerminatedPodsContainersCleanup 测试节点在遇到 disk pressure 时，
// 如果回收已终止 Pod 中容器的资源足以消除 disk pressure，
// 节点会清理这些容器而不驱逐正在运行的 Pod。
// 通过拉取大镜像来触发 disk pressure。
var _ = SIGDescribe("ImageGCTerminatedPodsContainersCleanup", framework.WithSlow(), framework.WithSerial(), framework.WithDisruptive(), feature.Eviction, func() {
	f := framework.NewDefaultFramework("image-gc-terminated-pods-containers-cleanup-test")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	const pressureTimeout = 10 * time.Minute
	const expectedNodeCondition = v1.NodeDiskPressure
	const expectedStarvedResource = resourceInodes
	const inodesToThreshold = uint64(50000)

	ginkgo.Context(fmt.Sprintf(testContextFmt, expectedNodeCondition), func() {
		prepull := func(ctx context.Context) {
			// 预先拉取镜像，让 image garbage collector 在回收资源时可以删除它们
			err := PrePullAllImages(ctx)
			gomega.Expect(err).ShouldNot(gomega.HaveOccurred())
		}
		ginkgo.BeforeEach(prepull)
		if framework.TestContext.PrepullImages {
			ginkgo.AfterEach(prepull)
		}

		tempSetCurrentKubeletConfig(f, func(ctx context.Context, initialConfig *kubeletconfig.KubeletConfiguration) {
			// 把驱逐阈值设为 inodesFree - inodesToThreshold，这样消耗掉 inodesToThreshold 个 inode 就会触发驱逐。
			summary := eventuallyGetSummary(ctx)
			inodesFree := *summary.Node.Fs.InodesFree
			if inodesFree <= inodesToThreshold {
				e2eskipper.Skipf("Too few inodes free on the host for the InodeEviction test to run")
			}
			framework.Logf("Setting eviction threshold to %d inodes", inodesFree-inodesToThreshold)
			initialConfig.EvictionHard = map[string]string{string(evictionapi.SignalNodeFsInodesFree): fmt.Sprintf("%d", inodesFree-inodesToThreshold)}
			initialConfig.EvictionMinimumReclaim = map[string]string{}
		})
		runEvictionTest(f, pressureTimeout, expectedNodeCondition, expectedStarvedResource, logInodeMetrics, []podEvictSpec{
			// 将 volume 设为 nil 会让文件写入容器的可写层，
			// 而该层默认使用节点的 root fs。
			{
				evictionPriority: 0,
				pod:              inodeConsumingPod("disk-hog", 30000, nil, false),
				expectToSucceed:  true,
			},
			{
				evictionPriority: 0,
				pod:              inodeConsumingPod("container-inode", 30000, &v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}}, true),
			},
		})
	})
})

// MemoryAllocatableEviction 测试节点在遇到 memory pressure 时是否只驱逐应当承担责任的 Pod。
// 通过 kube-reserved 预留节点的大部分容量，人为制造 memory pressure。
var _ = SIGDescribe("MemoryAllocatableEviction", framework.WithSlow(), framework.WithSerial(), framework.WithDisruptive(), feature.Eviction, func() {
	f := framework.NewDefaultFramework("memory-allocatable-eviction-test")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	const expectedNodeCondition = v1.NodeMemoryPressure
	const expectedStarvedResource = v1.ResourceMemory
	const pressureTimeout = 10 * time.Minute
	ginkgo.Context(fmt.Sprintf(testContextFmt, expectedNodeCondition), func() {
		tempSetCurrentKubeletConfig(f, func(ctx context.Context, initialConfig *kubeletconfig.KubeletConfiguration) {
			// 设置较大的 system 和 kube reserved 值，让 allocatable 阈值远早于硬驱逐阈值被触发。
			kubeReserved := getNodeCPUAndMemoryCapacity(ctx, f)[v1.ResourceMemory]
			// 默认的硬驱逐阈值是 250Mb，因此 Allocatable = Capacity - Reserved - 250Mb。
			// 我们希望 Allocatable = 50Mb，于是设置 Reserved = Capacity - Allocatable - 250Mb = Capacity - 300Mb。
			kubeReserved.Sub(resource.MustParse("300Mi"))
			initialConfig.KubeReserved = map[string]string{
				string(v1.ResourceMemory): kubeReserved.String(),
			}
			initialConfig.EnforceNodeAllocatable = []string{kubetypes.NodeAllocatableEnforcementKey}
			initialConfig.CgroupsPerQOS = true
		})
		runEvictionTest(f, pressureTimeout, expectedNodeCondition, expectedStarvedResource, logMemoryMetrics, []podEvictSpec{
			{
				evictionPriority: 1,
				pod:              getMemhogPod("memory-hog-pod", "memory-hog", v1.ResourceRequirements{}),
			},
			{
				evictionPriority: 0,
				pod:              innocentPod(),
			},
		})
	})
})

// MemoryAllocatableEviction 测试在使用 Pod 级资源时，节点遇到 memory pressure 是否只驱逐应当承担责任的 Pod。
// 通过 kube-reserved 预留节点的大部分容量，人为制造 memory pressure。
var _ = SIGDescribe("MemoryAllocatableEvictionPodLevelResources", framework.WithSlow(), framework.WithSerial(), framework.WithDisruptive(), feature.PodLevelResources, func() {
	f := framework.NewDefaultFramework("memory-allocatable-eviction-test-pod-level-resources")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	const expectedNodeCondition = v1.NodeMemoryPressure
	const expectedStarvedResource = v1.ResourceMemory
	const pressureTimeout = 10 * time.Minute
	ginkgo.Context(fmt.Sprintf(testContextFmt, expectedNodeCondition), func() {
		tempSetCurrentKubeletConfig(f, func(ctx context.Context, initialConfig *kubeletconfig.KubeletConfiguration) {
			// 设置较大的 system 和 kube reserved 值，让 allocatable 阈值远早于硬驱逐阈值被触发。
			kubeReserved := getNodeCPUAndMemoryCapacity(ctx, f)[v1.ResourceMemory]
			// 默认的硬驱逐阈值是 250Mb，因此 Allocatable = Capacity - Reserved - 250Mb。
			// 我们希望 Allocatable = 50Mb，于是设置 Reserved = Capacity - Allocatable - 250Mb = Capacity - 300Mb。
			kubeReserved.Sub(resource.MustParse("300Mi"))
			initialConfig.KubeReserved = map[string]string{
				string(v1.ResourceMemory): kubeReserved.String(),
			}
			initialConfig.EnforceNodeAllocatable = []string{kubetypes.NodeAllocatableEnforcementKey}
			initialConfig.CgroupsPerQOS = true
		})
		runEvictionTest(f, pressureTimeout, expectedNodeCondition, expectedStarvedResource, logMemoryMetrics, []podEvictSpec{
			{
				evictionPriority: 1,
				pod:              getMemhogPodWithPodLevelResources("memory-hog-pod", v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceMemory: resource.MustParse("100Mi")}}, "memory-hog", v1.ResourceRequirements{}),
			},
			{
				evictionPriority: 0,
				pod:              innocentPod(),
			},
		})
	})
})

// LocalStorageEviction 测试节点在遇到 disk pressure 时是否只驱逐应当承担责任的 Pod。
// 通过运行消耗磁盘空间的 Pod 来触发 disk pressure。
var _ = SIGDescribe("LocalStorageEviction", framework.WithSlow(), framework.WithSerial(), framework.WithDisruptive(), feature.Eviction, func() {
	f := framework.NewDefaultFramework("localstorage-eviction-test")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	const pressureTimeout = 15 * time.Minute
	const expectedNodeCondition = v1.NodeDiskPressure
	const expectedStarvedResource = v1.ResourceEphemeralStorage
	ginkgo.Context(fmt.Sprintf(testContextFmt, expectedNodeCondition), func() {

		tempSetCurrentKubeletConfig(f, func(ctx context.Context, initialConfig *kubeletconfig.KubeletConfiguration) {
			summary := eventuallyGetSummary(ctx)

			diskConsumedByTest := resource.MustParse("4Gi")
			availableBytesOnSystem := *(summary.Node.Fs.AvailableBytes)
			evictionThreshold := strconv.FormatUint(availableBytesOnSystem-uint64(diskConsumedByTest.Value()), 10)

			if availableBytesOnSystem <= uint64(diskConsumedByTest.Value()) {
				e2eskipper.Skipf("Too little disk free on the host for the LocalStorageEviction test to run")
			}

			initialConfig.EvictionHard = map[string]string{string(evictionapi.SignalNodeFsAvailable): evictionThreshold}
			initialConfig.EvictionMinimumReclaim = map[string]string{}
		})

		runEvictionTest(f, pressureTimeout, expectedNodeCondition, expectedStarvedResource, logDiskMetrics, []podEvictSpec{
			{
				evictionPriority: 1,
				// TODO(#127864): 容器 runtime 在 Pod 被驱逐后可能不会立即释放资源，
				// 导致测试失败。我们提供一个 emptyDir volume，以避开对 runtime 行为的依赖。
				pod: diskConsumingPod("container-disk-hog", lotsOfDisk, &v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}}, v1.ResourceRequirements{}),
			},
			{
				evictionPriority: 0,
				pod:              innocentPod(),
			},
		})
	})
})

// LocalStorageEviction 测试节点在遇到 disk pressure 时是否只驱逐应当承担责任的 Pod。
// 通过运行消耗磁盘空间并超过软驱逐阈值的 Pod 来触发 disk pressure。
// 注意：本测试的目的是测试软驱逐。选择本地存储是因为它的运行成本最低。
var _ = SIGDescribe("LocalStorageSoftEviction", framework.WithSlow(), framework.WithSerial(), framework.WithDisruptive(), feature.Eviction, func() {
	f := framework.NewDefaultFramework("localstorage-eviction-test")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	const pressureTimeout = 10 * time.Minute
	const expectedNodeCondition = v1.NodeDiskPressure
	const expectedStarvedResource = v1.ResourceEphemeralStorage
	ginkgo.Context(fmt.Sprintf(testContextFmt, expectedNodeCondition), func() {
		tempSetCurrentKubeletConfig(f, func(ctx context.Context, initialConfig *kubeletconfig.KubeletConfiguration) {
			diskConsumed := resource.MustParse("4Gi")
			summary := eventuallyGetSummary(ctx)
			availableBytes := *(summary.Node.Fs.AvailableBytes)
			if availableBytes <= uint64(diskConsumed.Value()) {
				e2eskipper.Skipf("Too little disk free on the host for the LocalStorageSoftEviction test to run")
			}
			initialConfig.EvictionSoft = map[string]string{string(evictionapi.SignalNodeFsAvailable): fmt.Sprintf("%d", availableBytes-uint64(diskConsumed.Value()))}
			initialConfig.EvictionSoftGracePeriod = map[string]string{string(evictionapi.SignalNodeFsAvailable): "1m"}
			// 沿用 Pod 默认的 grace period
			initialConfig.EvictionMaxPodGracePeriod = 30
			initialConfig.EvictionMinimumReclaim = map[string]string{}
			// 确保 Pod 不会因为 eviction-hard 阈值而被驱逐。
			// 将阈值设为 0% 即表示禁用；非空 map 会覆盖默认值（因 omitempty 而必须这样做）。
			initialConfig.EvictionHard = map[string]string{string(evictionapi.SignalMemoryAvailable): "0%"}
		})
		runEvictionTest(f, pressureTimeout, expectedNodeCondition, expectedStarvedResource, logDiskMetrics, []podEvictSpec{
			{
				evictionPriority: 1,
				// TODO(#127864): 容器 runtime 在 Pod 被驱逐后可能不会立即释放资源，
				// 导致测试失败。我们提供一个 emptyDir volume，以避开对 runtime 行为的依赖。
				pod: diskConsumingPod("container-disk-hog", lotsOfDisk, &v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}}, v1.ResourceRequirements{}),
			},
			{
				evictionPriority: 0,
				pod:              innocentPod(),
			},
		})
	})
})

var _ = SIGDescribe("LocalStorageSoftEvictionNotOverwriteTerminationGracePeriodSeconds", framework.WithSlow(), framework.WithSerial(), framework.WithDisruptive(), feature.Eviction, func() {
	f := framework.NewDefaultFramework("localstorage-eviction-test")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	const pressureTimeout = 10 * time.Minute
	const expectedNodeCondition = v1.NodeDiskPressure
	const expectedStarvedResource = v1.ResourceEphemeralStorage

	const evictionMaxPodGracePeriod = 30
	const evictionSoftGracePeriod = 30
	ginkgo.Context(fmt.Sprintf(testContextFmt, expectedNodeCondition), func() {
		tempSetCurrentKubeletConfig(f, func(ctx context.Context, initialConfig *kubeletconfig.KubeletConfiguration) {
			diskConsumed := resource.MustParse("4Gi")
			summary := eventuallyGetSummary(ctx)
			availableBytes := *(summary.Node.Fs.AvailableBytes)
			if availableBytes <= uint64(diskConsumed.Value()) {
				e2eskipper.Skipf("Too little disk free on the host for the LocalStorageSoftEviction test to run")
			}
			initialConfig.EvictionSoft = map[string]string{string(evictionapi.SignalNodeFsAvailable): fmt.Sprintf("%d", availableBytes-uint64(diskConsumed.Value()))}
			initialConfig.EvictionSoftGracePeriod = map[string]string{string(evictionapi.SignalNodeFsAvailable): "30s"}
			// 沿用 Pod 默认的 grace period
			initialConfig.EvictionMaxPodGracePeriod = int32(evictionMaxPodGracePeriod)
			initialConfig.EvictionMinimumReclaim = map[string]string{}
			// 确保 Pod 不会因为 eviction-hard 阈值而被驱逐。
			// 将阈值设为 0% 即表示禁用；非空 map 会覆盖默认值（因 omitempty 而必须这样做）。
			initialConfig.EvictionHard = map[string]string{string(evictionapi.SignalMemoryAvailable): "0%"}
		})

		runEvictionTest(f, pressureTimeout, expectedNodeCondition, expectedStarvedResource, logDiskMetrics, []podEvictSpec{
			{
				evictionMaxPodGracePeriod: evictionSoftGracePeriod,
				evictionSoftGracePeriod:   evictionMaxPodGracePeriod,
				evictionPriority:          1,
				pod:                       diskConsumingPod("container-disk-hog", lotsOfDisk, nil, v1.ResourceRequirements{}),
			},
		})
	})
})

// LocalStorageCapacityIsolationEviction 测试容器和 volume 的本地存储限制是否通过驱逐来强制执行
var _ = SIGDescribe("LocalStorageCapacityIsolationEviction", framework.WithSlow(), framework.WithSerial(), framework.WithDisruptive(), feature.LocalStorageCapacityIsolationQuota, feature.Eviction, func() {
	f := framework.NewDefaultFramework("localstorage-eviction-test")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	const evictionTestTimeout = 10 * time.Minute
	ginkgo.Context(fmt.Sprintf(testContextFmt, "evictions due to pod local storage violations"), func() {
		tempSetCurrentKubeletConfig(f, func(ctx context.Context, initialConfig *kubeletconfig.KubeletConfiguration) {
			// 将阈值设为 0% 即表示禁用；非空 map 会覆盖默认值（因 omitempty 而必须这样做）
			initialConfig.EvictionHard = map[string]string{string(evictionapi.SignalMemoryAvailable): "0%"}
		})
		sizeLimit := resource.MustParse("100Mi")
		useOverLimit := 101 /* Mb */
		useUnderLimit := 99 /* Mb */
		containerLimit := v1.ResourceList{v1.ResourceEphemeralStorage: sizeLimit}

		runEvictionTest(f, evictionTestTimeout, noPressure, noStarvedResource, logDiskMetrics, []podEvictSpec{
			{
				evictionPriority: 1, // 该 Pod 因 emptyDir（默认存储类型）用量超限，应当被驱逐
				pod: diskConsumingPod("emptydir-disk-sizelimit", useOverLimit, &v1.VolumeSource{
					EmptyDir: &v1.EmptyDirVolumeSource{SizeLimit: &sizeLimit},
				}, v1.ResourceRequirements{}),
			},
			{
				evictionPriority: 1, // 该 Pod 通过写入可写层超过 container 限制，应当被驱逐。
				pod:              diskConsumingPod("container-disk-limit", useOverLimit, nil, v1.ResourceRequirements{Limits: containerLimit}),
			},
			{
				evictionPriority: 1, // 该 Pod 通过写入 emptydir 触及 container 限制，应当被驱逐
				pod: diskConsumingPod("container-emptydir-disk-limit", useOverLimit, &v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}},
					v1.ResourceRequirements{Limits: containerLimit}),
			},
			{
				evictionPriority: 0, // 该 Pod 不应被驱逐，因为 MemoryBackedVolume 的用量不会超过分配给它的空间
				pod: diskConsumingPod("emptydir-memory-sizelimit", useOverLimit, &v1.VolumeSource{
					EmptyDir: &v1.EmptyDirVolumeSource{Medium: "Memory", SizeLimit: &sizeLimit},
				}, v1.ResourceRequirements{}),
			},
			{
				evictionPriority: 0, // 该 Pod 用量低于限制，不应被驱逐
				pod: diskConsumingPod("emptydir-disk-below-sizelimit", useUnderLimit, &v1.VolumeSource{
					EmptyDir: &v1.EmptyDirVolumeSource{SizeLimit: &sizeLimit},
				}, v1.ResourceRequirements{}),
			},
			{
				evictionPriority: 0, // 该 Pod 用量低于限制，不应被驱逐
				pod:              diskConsumingPod("container-disk-below-sizelimit", useUnderLimit, nil, v1.ResourceRequirements{Limits: containerLimit}),
			},
			{
				evictionPriority: 1, // 可重启的 init container（sidecar）超过了自身的 ephemeral-storage 限制，因此该 Pod 应当被驱逐。
				pod:              diskConsumingSidecarPod("sidecar-container-disk-limit", useOverLimit, v1.ResourceRequirements{Limits: containerLimit}),
			},
			{
				evictionPriority: 0, // 可重启的 init container（sidecar）用量低于限制，因此该 Pod 不应被驱逐。
				pod:              diskConsumingSidecarPod("sidecar-container-disk-below-sizelimit", useUnderLimit, v1.ResourceRequirements{Limits: containerLimit}),
			},
		})
	})
})

// PriorityMemoryEvictionOrdering 测试节点在遇到 memory pressure 时如何驱逐 Pod。
// 测试验证 guaranteed Pod 永远不会被驱逐，并且低优先级 Pod 会在高优先级 Pod 之前被驱逐。
var _ = SIGDescribe("PriorityMemoryEvictionOrdering", framework.WithSlow(), framework.WithSerial(), framework.WithDisruptive(), feature.Eviction, func() {
	f := framework.NewDefaultFramework("priority-memory-eviction-ordering-test")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	const expectedNodeCondition = v1.NodeMemoryPressure
	const expectedStarvedResource = v1.ResourceMemory
	const pressureTimeout = 10 * time.Minute

	highPriorityClassName := f.BaseName + "-high-priority"
	const highPriority = int32(999999999)

	ginkgo.Context(fmt.Sprintf(testContextFmt, expectedNodeCondition), func() {
		tempSetCurrentKubeletConfig(f, func(ctx context.Context, initialConfig *kubeletconfig.KubeletConfiguration) {
			memoryConsumed := resource.MustParse("600Mi")
			summary := eventuallyGetSummary(ctx)
			availableBytes := *(summary.Node.Memory.AvailableBytes)
			if availableBytes <= uint64(memoryConsumed.Value()) {
				e2eskipper.Skipf("Too little memory free on the host for the PriorityMemoryEvictionOrdering test to run")
			}
			initialConfig.EvictionHard = map[string]string{string(evictionapi.SignalMemoryAvailable): fmt.Sprintf("%d", availableBytes-uint64(memoryConsumed.Value()))}
			initialConfig.EvictionMinimumReclaim = map[string]string{}
		})
		ginkgo.BeforeEach(func(ctx context.Context) {
			_, err := f.ClientSet.SchedulingV1().PriorityClasses().Create(ctx, &schedulingv1.PriorityClass{ObjectMeta: metav1.ObjectMeta{Name: highPriorityClassName}, Value: highPriority}, metav1.CreateOptions{})
			if err != nil && !apierrors.IsAlreadyExists(err) {
				framework.ExpectNoError(err, "failed to create priority class")
			}
		})
		ginkgo.AfterEach(func(ctx context.Context) {
			err := f.ClientSet.SchedulingV1().PriorityClasses().Delete(ctx, highPriorityClassName, metav1.DeleteOptions{})
			framework.ExpectNoError(err)
		})
		specs := []podEvictSpec{
			{
				evictionPriority: 2,
				pod:              getMemhogPod("memory-hog-pod", "memory-hog", v1.ResourceRequirements{}),
			},
			{
				evictionPriority: 1,
				pod:              getMemhogPod("high-priority-memory-hog-pod", "high-priority-memory-hog", v1.ResourceRequirements{}),
				prePodCreationModificationFunc: func(ctx context.Context, pod *v1.Pod) {
					node := getLocalNode(ctx, f)

					nodeSwapInfo := node.Status.NodeInfo.Swap
					if nodeSwapInfo == nil || nodeSwapInfo.Capacity == nil || *nodeSwapInfo.Capacity <= 0 {
						return
					}

					// 节点上启用 swap 后，内核可能回收更多内存，
					// 因而更难让节点进入 memory pressure。这里增加一个容器，分配与 swap 容量相等的内存，
					// 帮助把节点推入 memory pressure。
					pod.Spec.Containers = append(pod.Spec.Containers, v1.Container{
						Name:            pod.Spec.Containers[0].Name + "-static-allocator",
						Image:           imageutils.GetE2EImage(imageutils.Agnhost),
						ImagePullPolicy: "Always",
						Args:            []string{"stress", "--mem-alloc-size", "2Mi", "--mem-alloc-sleep", "1s", "--mem-total", fmt.Sprintf("%d", *nodeSwapInfo.Capacity)},
					})
				},
			},
			{
				evictionPriority: 0,
				pod: getMemhogPod("guaranteed-pod", "guaranteed-pod", v1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceMemory: resource.MustParse("300Mi"),
					},
					Limits: v1.ResourceList{
						v1.ResourceMemory: resource.MustParse("300Mi"),
					},
				}),
			},
		}
		specs[1].pod.Spec.PriorityClassName = highPriorityClassName
		runEvictionTest(f, pressureTimeout, expectedNodeCondition, expectedStarvedResource, logMemoryMetrics, specs)
	})
})

// PriorityMemoryEvictionOrdering 测试在使用 Pod 级资源时，节点遇到 memory pressure 如何驱逐 Pod。
// 测试验证 guaranteed Pod 永远不会被驱逐，并且低优先级 Pod 会在高优先级 Pod 之前被驱逐。
var _ = SIGDescribe("PriorityMemoryEvictionOrderingPodLevelResources", framework.WithSlow(), framework.WithSerial(), framework.WithDisruptive(), feature.PodLevelResources, func() {
	f := framework.NewDefaultFramework("priority-memory-eviction-ordering-test-pod-level-resources")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	const expectedNodeCondition = v1.NodeMemoryPressure
	const expectedStarvedResource = v1.ResourceMemory
	const pressureTimeout = 10 * time.Minute

	highPriorityClassName := f.BaseName + "-high-priority"
	const highPriority = int32(999999999)

	ginkgo.Context(fmt.Sprintf(testContextFmt, expectedNodeCondition), func() {
		tempSetCurrentKubeletConfig(f, func(ctx context.Context, initialConfig *kubeletconfig.KubeletConfiguration) {
			memoryConsumed := resource.MustParse("600Mi")
			summary := eventuallyGetSummary(ctx)
			availableBytes := *(summary.Node.Memory.AvailableBytes)
			if availableBytes <= uint64(memoryConsumed.Value()) {
				e2eskipper.Skipf("Too little memory free on the host for the PriorityMemoryEvictionOrdering test to run")
			}
			initialConfig.EvictionHard = map[string]string{string(evictionapi.SignalMemoryAvailable): fmt.Sprintf("%d", availableBytes-uint64(memoryConsumed.Value()))}
			initialConfig.EvictionMinimumReclaim = map[string]string{}
		})
		ginkgo.BeforeEach(func(ctx context.Context) {
			_, err := f.ClientSet.SchedulingV1().PriorityClasses().Create(ctx, &schedulingv1.PriorityClass{ObjectMeta: metav1.ObjectMeta{Name: highPriorityClassName}, Value: highPriority}, metav1.CreateOptions{})
			if err != nil && !apierrors.IsAlreadyExists(err) {
				framework.ExpectNoError(err, "failed to create priority class")
			}
		})
		ginkgo.AfterEach(func(ctx context.Context) {
			err := f.ClientSet.SchedulingV1().PriorityClasses().Delete(ctx, highPriorityClassName, metav1.DeleteOptions{})
			framework.ExpectNoError(err)
		})
		specs := []podEvictSpec{
			{
				evictionPriority: 2,
				pod:              getMemhogPod("memory-hog-pod", "memory-hog", v1.ResourceRequirements{}),
			},
			{
				evictionPriority: 1,
				pod:              getMemhogPod("high-priority-memory-hog-pod", "high-priority-memory-hog", v1.ResourceRequirements{}),
			},
			{
				evictionPriority: 0,
				pod: getMemhogPodWithPodLevelResources("guaranteed-pod", v1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceMemory: resource.MustParse("300Mi"),
					},
					Limits: v1.ResourceList{
						v1.ResourceMemory: resource.MustParse("300Mi"),
					},
				}, "guaranteed-container", v1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceMemory: resource.MustParse("250Mi"),
					},
					Limits: v1.ResourceList{
						v1.ResourceMemory: resource.MustParse("250Mi"),
					},
				}),
			},
		}
		specs[1].pod.Spec.PriorityClassName = highPriorityClassName
		runEvictionTest(f, pressureTimeout, expectedNodeCondition, expectedStarvedResource, logMemoryMetrics, specs)
	})
})

// PriorityLocalStorageEvictionOrdering 测试节点在遇到 disk pressure 时如何驱逐 Pod。
// 测试验证 guaranteed Pod 永远不会被驱逐，并且低优先级 Pod 会在高优先级 Pod 之前被驱逐。
var _ = SIGDescribe("PriorityLocalStorageEvictionOrdering", framework.WithSlow(), framework.WithSerial(), framework.WithDisruptive(), feature.Eviction, func() {
	f := framework.NewDefaultFramework("priority-disk-eviction-ordering-test")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	const expectedNodeCondition = v1.NodeDiskPressure
	const expectedStarvedResource = v1.ResourceEphemeralStorage
	const pressureTimeout = 15 * time.Minute

	highPriorityClassName := f.BaseName + "-high-priority"
	const highPriority = int32(999999999)

	ginkgo.Context(fmt.Sprintf(testContextFmt, expectedNodeCondition), func() {
		tempSetCurrentKubeletConfig(f, func(ctx context.Context, initialConfig *kubeletconfig.KubeletConfiguration) {
			diskConsumed := resource.MustParse("4Gi")
			summary := eventuallyGetSummary(ctx)
			availableBytes := *(summary.Node.Fs.AvailableBytes)
			if availableBytes <= uint64(diskConsumed.Value()) {
				e2eskipper.Skipf("Too little disk free on the host for the PriorityLocalStorageEvictionOrdering test to run")
			}
			initialConfig.EvictionHard = map[string]string{string(evictionapi.SignalNodeFsAvailable): fmt.Sprintf("%d", availableBytes-uint64(diskConsumed.Value()))}
			initialConfig.EvictionMinimumReclaim = map[string]string{}
		})
		ginkgo.BeforeEach(func(ctx context.Context) {
			_, err := f.ClientSet.SchedulingV1().PriorityClasses().Create(ctx, &schedulingv1.PriorityClass{ObjectMeta: metav1.ObjectMeta{Name: highPriorityClassName}, Value: highPriority}, metav1.CreateOptions{})
			if err != nil && !apierrors.IsAlreadyExists(err) {
				framework.ExpectNoError(err, "failed to create priority class")
			}
		})
		ginkgo.AfterEach(func(ctx context.Context) {
			err := f.ClientSet.SchedulingV1().PriorityClasses().Delete(ctx, highPriorityClassName, metav1.DeleteOptions{})
			framework.ExpectNoError(err)
		})
		specs := []podEvictSpec{
			{
				evictionPriority: 2,
				// TODO(#127864): 容器 runtime 在 Pod 被驱逐后可能不会立即释放资源，
				// 导致测试失败。我们提供一个 emptyDir volume，以避开对 runtime 行为的依赖。
				pod: diskConsumingPod("best-effort-disk", lotsOfDisk, &v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}}, v1.ResourceRequirements{}),
			},
			{
				evictionPriority: 1,
				// TODO(#127864): 容器 runtime 在 Pod 被驱逐后可能不会立即释放资源，
				// 导致测试失败。我们提供一个 emptyDir volume，以避开对 runtime 行为的依赖。
				pod: diskConsumingPod("high-priority-disk", lotsOfDisk, &v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}}, v1.ResourceRequirements{}),
			},
			{
				evictionPriority: 0,
				// 只要求 99% 的精度（297/300 Mb），因为某些 OS 发行版上，文件本身（不含内容）也会占用磁盘空间。
				pod: diskConsumingPod("guaranteed-disk", 297 /* Mb */, &v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}}, v1.ResourceRequirements{
					Requests: v1.ResourceList{
						v1.ResourceEphemeralStorage: resource.MustParse("300Mi"),
					},
					Limits: v1.ResourceList{
						v1.ResourceEphemeralStorage: resource.MustParse("300Mi"),
					},
				}),
			},
		}
		specs[1].pod.Spec.PriorityClassName = highPriorityClassName
		runEvictionTest(f, pressureTimeout, expectedNodeCondition, expectedStarvedResource, logDiskMetrics, specs)
	})
})

// PriorityPidEvictionOrdering 测试节点在 fork bomb 下是否产生 pid pressure，并按优先级驱逐 Pod
var _ = SIGDescribe("PriorityPidEvictionOrdering", framework.WithSlow(), framework.WithSerial(), framework.WithDisruptive(), feature.Eviction, func() {
	f := framework.NewDefaultFramework("pidpressure-eviction-test")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	const pressureTimeout = 10 * time.Minute
	const expectedNodeCondition = v1.NodePIDPressure
	const expectedStarvedResource = noStarvedResource

	highPriorityClassName := f.BaseName + "-high-priority"
	const highPriority = int32(999999999)
	// 看起来在 10,000+ 附近存在一个阈值。超过该阈值时，
	// 进程很可能被 oom-kill 而不是被驱逐。
	// 为避免 oom-kill，一个测试中同时最多只能有两个 pidConsumingPod。
	const processes = 5000

	// 如果 criStats 为 true，kubelet 在获取 Pod 的 pid 数量时会使用 CRI 的数据，而不是 cadvisor 的数据
	for _, criStats := range []bool{true, false} {
		ginkgo.Context(fmt.Sprintf("when we run containers with PodAndContainerStatsFromCRI=%v that should cause %s", criStats, expectedNodeCondition), func() {
			tempSetCurrentKubeletConfig(f, func(ctx context.Context, initialConfig *kubeletconfig.KubeletConfiguration) {
				pidsConsumed := int64(4000)
				summary := eventuallyGetSummary(ctx)
				availablePids := *(summary.Node.Rlimit.MaxPID) - *(summary.Node.Rlimit.NumOfRunningProcesses)
				initialConfig.EvictionHard = map[string]string{string(evictionapi.SignalPIDAvailable): fmt.Sprintf("%d", availablePids-pidsConsumed)}
				initialConfig.EvictionMinimumReclaim = map[string]string{}
				if initialConfig.FeatureGates == nil {
					initialConfig.FeatureGates = make(map[string]bool)
				}
				if criStats {
					initialConfig.FeatureGates["PodAndContainerStatsFromCRI"] = true
				}

			})
			ginkgo.BeforeEach(func(ctx context.Context) {
				_, err := f.ClientSet.SchedulingV1().PriorityClasses().Create(ctx, &schedulingv1.PriorityClass{ObjectMeta: metav1.ObjectMeta{Name: highPriorityClassName}, Value: highPriority}, metav1.CreateOptions{})
				if err != nil && !apierrors.IsAlreadyExists(err) {
					framework.ExpectNoError(err, "failed to create priority class")
				}
			})
			ginkgo.AfterEach(func(ctx context.Context) {
				err := f.ClientSet.SchedulingV1().PriorityClasses().Delete(ctx, highPriorityClassName, metav1.DeleteOptions{})
				framework.ExpectNoError(err)
			})
			specs := []podEvictSpec{
				{
					evictionPriority: 2,
					pod:              pidConsumingPod("fork-bomb-container-with-low-priority", processes),
				},
				{
					evictionPriority: 0,
					pod:              innocentPod(),
				},
				{
					evictionPriority: 1,
					pod:              pidConsumingPod("fork-bomb-container-with-high-priority", processes),
				},
			}
			specs[1].pod.Spec.PriorityClassName = highPriorityClassName
			specs[2].pod.Spec.PriorityClassName = highPriorityClassName
			runEvictionTest(f, pressureTimeout, expectedNodeCondition, expectedStarvedResource, logPidMetrics, specs)
		})
	}

	f.Context(fmt.Sprintf(testContextFmt, expectedNodeCondition)+"; baseline scenario to verify DisruptionTarget is added", func() {
		tempSetCurrentKubeletConfig(f, func(ctx context.Context, initialConfig *kubeletconfig.KubeletConfiguration) {
			pidsConsumed := int64(4000)
			summary := eventuallyGetSummary(ctx)
			availablePids := *(summary.Node.Rlimit.MaxPID) - *(summary.Node.Rlimit.NumOfRunningProcesses)
			initialConfig.EvictionHard = map[string]string{string(evictionapi.SignalPIDAvailable): fmt.Sprintf("%d", availablePids-pidsConsumed)}
			initialConfig.EvictionMinimumReclaim = map[string]string{}
		})
		specs := []podEvictSpec{
			{
				evictionPriority:           1,
				pod:                        pidConsumingPod("fork-bomb-container", processes),
				wantPodDisruptionCondition: ptr.To(v1.DisruptionTarget),
			},
		}
		runEvictionTest(f, pressureTimeout, expectedNodeCondition, expectedStarvedResource, logPidMetrics, specs)
	})
})

// runEvictionTest 使用的结构体，指定 Pod 以及它相对于其他 Pod 应当何时被驱逐
type podEvictSpec struct {
	// P0 永远不应被驱逐，P1 不应在 P2 之前被驱逐，以此类推。
	// 如果两个 Pod 都排在 P1，那么任意一个先失败都是允许的。
	// 当除 P0 之外的所有 Pod 都被驱逐后，测试结束
	evictionPriority           int
	pod                        *v1.Pod
	wantPodDisruptionCondition *v1.PodConditionType
	expectToSucceed            bool

	evictionMaxPodGracePeriod int
	evictionSoftGracePeriod   int

	// 可用于根据运行时数据修改 Pod
	prePodCreationModificationFunc func(ctx context.Context, pod *v1.Pod)
}

// runEvictionTest 根据给定的 Pod 搭建测试环境，并检查以下事项：
//
//	确保目标 expectedNodeCondition 确实被触发。
//	确保 evictionPriority 为 0 的 Pod 不会被驱逐。
//	确保低 evictionPriority 的 Pod 总是在高 evictionPriority 的 Pod 之前被驱逐（2 先于 1 被驱逐，以此类推）。
//	确保所有 evictionPriority 非零的 Pod 最终都被驱逐。
//
// runEvictionTest 随后删除给定的 Pod 以清理测试环境，并确认 expectedNodeCondition 不再存在
func runEvictionTest(f *framework.Framework, pressureTimeout time.Duration, expectedNodeCondition v1.NodeConditionType, expectedStarvedResource v1.ResourceName, logFunc func(ctx context.Context), testSpecs []podEvictSpec) {
	// 把测试的其余部分放在一个 context 内，以便在测试前后设置 kubelet config。
	ginkgo.Context("", func() {
		ginkgo.BeforeEach(func(ctx context.Context) {
			// 降低 allocatable cgroup 的内存用量，确保不会出现 MemoryPressure
			reduceAllocatableMemoryUsageIfCgroupv1()
			// 节点不会立即上报本地存储容量
			// 等待片刻，避免请求本地存储的 Pod 调度失败
			time.Sleep(30 * time.Second)
			ginkgo.By("setting up pods to be used by tests")
			pods := []*v1.Pod{}
			podsToSucceed := []*v1.Pod{}
			for _, spec := range testSpecs {
				if spec.prePodCreationModificationFunc != nil {
					spec.prePodCreationModificationFunc(ctx, spec.pod)
				}
				if spec.expectToSucceed {
					podsToSucceed = append(podsToSucceed, spec.pod)
					continue
				}
				pods = append(pods, spec.pod)
			}

			e2ePodClient := e2epod.NewPodClient(f)

			// 创建所有预期成功的 Pod，并等待它们运行成功。
			// 这里没有使用 CreateBatch，是为了避免竞态：它底层调用 CreateSync，
			// 如果 wait 函数没能在 Pod 终止前观察到它就绪，就可能失败。
			for _, pod := range podsToSucceed {
				*pod = *e2ePodClient.Create(ctx, pod)
			}

			ginkgo.By(fmt.Sprintf("Waiting for the expected pods to complete: %v", podsToSucceed))
			for _, pod := range podsToSucceed {
				framework.ExpectNoError(e2epod.WaitForPodSuccessInNamespace(ctx, f.ClientSet, pod.Name, pod.Namespace))
			}

			// 等预期成功的 Pod 终止后，再创建其余所有 Pod
			e2ePodClient.CreateBatch(ctx, pods)
		})

		ginkgo.It("should eventually evict all of the correct pods", func(ctx context.Context) {
			ginkgo.By(fmt.Sprintf("Waiting for node to have NodeCondition: %s", expectedNodeCondition))
			gomega.Eventually(ctx, func(ctx context.Context) error {
				logFunc(ctx)
				if expectedNodeCondition == noPressure || hasNodeCondition(ctx, f, expectedNodeCondition) {
					return nil
				}
				return fmt.Errorf("NodeCondition: %s not encountered", expectedNodeCondition)
			}, pressureTimeout, evictionPollInterval).Should(gomega.BeNil())

			ginkgo.By("Waiting for evictions to occur")
			nodeUnreadyTime := time.Now()

			gomega.Eventually(ctx, func(ctx context.Context) error {
				if expectedNodeCondition != noPressure {
					if hasNodeCondition(ctx, f, expectedNodeCondition) {
						framework.Logf("Node has %s", expectedNodeCondition)
					} else {
						framework.Logf("Node does NOT have %s", expectedNodeCondition)
						nodeUnreadyTime = time.Now()
					}
				}
				logKubeletLatencyMetrics(ctx, kubeletmetrics.EvictionStatsAgeKey)
				logFunc(ctx)

				verifyEvictionPeriod(ctx, f, testSpecs, nodeUnreadyTime)
				return verifyEvictionOrdering(ctx, f, testSpecs)
			}, pressureTimeout, evictionPollInterval).Should(gomega.Succeed())

			ginkgo.By("checking for the expected pod conditions for evicted pods")
			verifyPodConditions(ctx, f, testSpecs)

			// 我们从 apiserver 观察 pressure，而 eviction manager 从 kubelet 的内部统计数据观察 pressure。
			// 这意味着 eviction manager 会比我们更早察觉 pressure：它驱逐 Pod 的时刻，与我们通过查询 apiserver
			// 看到 pressure 的时刻之间存在延迟。这里加一段等待来覆盖这个延迟。
			ginkgo.By("making sure pressure from test has surfaced before continuing")
			time.Sleep(pressureDelay)

			ginkgo.By(fmt.Sprintf("Waiting for NodeCondition: %s to no longer exist on the node", expectedNodeCondition))
			gomega.Eventually(ctx, func(ctx context.Context) error {
				logFunc(ctx)
				logKubeletLatencyMetrics(ctx, kubeletmetrics.EvictionStatsAgeKey)
				if expectedNodeCondition != noPressure && hasNodeCondition(ctx, f, expectedNodeCondition) {
					return fmt.Errorf("Conditions haven't returned to normal, node still has %s", expectedNodeCondition)
				}
				return nil
			}, pressureDisappearTimeout, evictionPollInterval).Should(gomega.BeNil())

			ginkgo.By("checking for stable, pressure-free condition without unexpected pod failures")
			gomega.Consistently(ctx, func(ctx context.Context) error {
				if expectedNodeCondition != noPressure && hasNodeCondition(ctx, f, expectedNodeCondition) {
					return fmt.Errorf("%s disappeared and then reappeared", expectedNodeCondition)
				}
				logFunc(ctx)
				logKubeletLatencyMetrics(ctx, kubeletmetrics.EvictionStatsAgeKey)
				return verifyEvictionOrdering(ctx, f, testSpecs)
			}, postTestConditionMonitoringPeriod, evictionPollInterval).Should(gomega.Succeed())

			ginkgo.By("checking for correctly formatted eviction events")
			verifyEvictionEvents(ctx, f, testSpecs, expectedStarvedResource)
		})

		ginkgo.AfterEach(func(ctx context.Context) {
			ginkgo.By("deleting pods")
			for _, spec := range testSpecs {
				ginkgo.By(fmt.Sprintf("deleting pod: %s", spec.pod.Name))
				e2epod.NewPodClient(f).DeleteSync(ctx, spec.pod.Name, metav1.DeleteOptions{}, 10*time.Minute)
			}

			// 如果测试在确认 NodeCondition 已从节点上消失之前就失败了，
			// 仍然需要等待 NodeCondition 消失
			ginkgo.By(fmt.Sprintf("making sure NodeCondition %s no longer exists on the node", expectedNodeCondition))
			gomega.Eventually(ctx, func(ctx context.Context) error {
				if expectedNodeCondition != noPressure && hasNodeCondition(ctx, f, expectedNodeCondition) {
					return fmt.Errorf("Conditions haven't returned to normal, node still has %s", expectedNodeCondition)
				}
				return nil
			}, pressureDisappearTimeout, evictionPollInterval).Should(gomega.BeNil())

			reduceAllocatableMemoryUsageIfCgroupv1()

			ginkgo.By("making sure we can start a new pod after the test")
			podName := "test-admit-pod"
			e2epod.NewPodClient(f).CreateSync(ctx, &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: podName,
				},
				Spec: v1.PodSpec{
					RestartPolicy: v1.RestartPolicyNever,
					Containers: []v1.Container{
						{
							Image:           imageutils.GetPauseImageName(),
							Name:            podName,
							ImagePullPolicy: "Always",
						},
					},
				},
			})

			if ginkgo.CurrentSpecReport().Failed() {
				if framework.TestContext.DumpLogsOnFailure {
					logPodEvents(ctx, f)
					logNodeEvents(ctx, f)
				}
			}
		})
	})
}

// verifyEvictionOrdering 在所有非零优先级 Pod 尚未全部被驱逐时返回 error，否则返回 nil。
// 如果驱逐顺序被破坏，或优先级为 0 的 Pod 失败，该函数会通过 Expect 触发 panic。
func verifyEvictionOrdering(ctx context.Context, f *framework.Framework, testSpecs []podEvictSpec) error {
	// 收集当前信息
	updatedPodList, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	updatedPods := updatedPodList.Items
	for _, p := range updatedPods {
		framework.Logf("fetching pod %s; phase= %v", p.Name, p.Status.Phase)
	}

	ginkgo.By("checking eviction ordering and ensuring important pods don't fail")
	done := true
	pendingPods := []string{}
	for _, priorityPodSpec := range testSpecs {
		var priorityPod v1.Pod
		for _, p := range updatedPods {
			if p.Name == priorityPodSpec.pod.Name {
				priorityPod = p
			}
		}
		gomega.Expect(priorityPod).NotTo(gomega.BeNil())
		if !priorityPodSpec.expectToSucceed {
			gomega.Expect(priorityPod.Status.Phase).ToNot(gomega.Equal(v1.PodSucceeded),
				fmt.Sprintf("pod: %s succeeded unexpectedly", priorityPod.Name))
		}

		// 检查驱逐顺序。
		// 注意：例如优先级 1 和优先级 2 的 Pod 在同一轮失败是可以接受的，
		// 但优先级 1 的 Pod 在优先级 2 的 Pod 仍在运行时失败则绝不允许
		for _, lowPriorityPodSpec := range testSpecs {
			var lowPriorityPod v1.Pod
			for _, p := range updatedPods {
				if p.Name == lowPriorityPodSpec.pod.Name {
					lowPriorityPod = p
				}
			}
			gomega.Expect(lowPriorityPod).NotTo(gomega.BeNil())
			if priorityPodSpec.evictionPriority < lowPriorityPodSpec.evictionPriority && lowPriorityPod.Status.Phase == v1.PodRunning {
				gomega.Expect(priorityPod.Status.Phase).ToNot(gomega.Equal(v1.PodFailed),
					fmt.Sprintf("priority %d pod: %s failed before priority %d pod: %s",
						priorityPodSpec.evictionPriority, priorityPodSpec.pod.Name, lowPriorityPodSpec.evictionPriority, lowPriorityPodSpec.pod.Name))
			}
		}

		if priorityPod.Status.Phase == v1.PodFailed {
			gomega.Expect(priorityPod.Status.Reason).To(gomega.Equal(eviction.Reason), "pod %s failed; expected Status.Reason to be %s, but got %s",
				priorityPod.Name, eviction.Reason, priorityPod.Status.Reason)
		}

		// EvictionPriority 为 0 的 Pod 不应失败
		if priorityPodSpec.evictionPriority == 0 {
			gomega.Expect(priorityPod.Status.Phase).ToNot(gomega.Equal(v1.PodFailed),
				fmt.Sprintf("priority 0 pod: %s failed", priorityPod.Name))
		}

		// 如果有 evictionPriority 非 0 的 Pod 尚未被驱逐，就说明还没结束
		if priorityPodSpec.evictionPriority != 0 && priorityPod.Status.Phase != v1.PodFailed {
			pendingPods = append(pendingPods, priorityPod.ObjectMeta.Name)
			done = false
		}
	}
	if done {
		return nil
	}
	return fmt.Errorf("pods that should be evicted are still running: %#v", pendingPods)
}

func verifyEvictionPeriod(ctx context.Context, f *framework.Framework, testSpecs []podEvictSpec, nodeUnreadyTime time.Time) {
	for i, spec := range testSpecs {
		if spec.evictionMaxPodGracePeriod == 0 && spec.evictionSoftGracePeriod == 0 {
			continue
		}
		softEvictionPeriod := spec.evictionMaxPodGracePeriod + spec.evictionSoftGracePeriod

		pod, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Get(ctx, spec.pod.Name, metav1.GetOptions{})
		framework.ExpectNoError(err, "Failed to get the recent pod object for name: %q", pod.Name)

		minSoftEvictionPeriod := min(float64(softEvictionPeriod), float64(*spec.pod.Spec.TerminationGracePeriodSeconds+int64(spec.evictionSoftGracePeriod)))
		if pod.Status.Phase == v1.PodFailed {
			if time.Since(nodeUnreadyTime).Seconds() > minSoftEvictionPeriod+15 {
				framework.Failf("pod %s should be evicted within %f seconds, but it has not been evicted for %f seconds.", pod.Name, minSoftEvictionPeriod, time.Since(nodeUnreadyTime).Seconds())
			} else {
				testSpecs[i].evictionMaxPodGracePeriod = 0
				testSpecs[i].evictionSoftGracePeriod = 0
			}
		}
	}
}

func verifyPodConditions(ctx context.Context, f *framework.Framework, testSpecs []podEvictSpec) {
	for _, spec := range testSpecs {
		if spec.wantPodDisruptionCondition != nil {
			pod, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Get(ctx, spec.pod.Name, metav1.GetOptions{})
			framework.ExpectNoError(err, "Failed to get the recent pod object for name: %q", pod.Name)

			cType := *spec.wantPodDisruptionCondition
			podDisruptionCondition := e2epod.FindPodConditionByType(&pod.Status, cType)
			if podDisruptionCondition == nil {
				framework.Failf("pod %q should have the condition: %q, pod status: %v", pod.Name, cType, pod.Status)
			}
		}
	}
}

func verifyEvictionEvents(ctx context.Context, f *framework.Framework, testSpecs []podEvictSpec, expectedStarvedResource v1.ResourceName) {
	for _, spec := range testSpecs {
		pod := spec.pod
		if spec.evictionPriority != 0 {
			selector := fields.Set{
				"involvedObject.kind":      "Pod",
				"involvedObject.name":      pod.Name,
				"involvedObject.namespace": f.Namespace.Name,
				"reason":                   eviction.Reason,
			}.AsSelector().String()
			podEvictEvents, err := f.ClientSet.CoreV1().Events(f.Namespace.Name).List(ctx, metav1.ListOptions{FieldSelector: selector})
			framework.ExpectNoError(err, "getting events")
			gomega.Expect(podEvictEvents.Items).To(gomega.HaveLen(1), "Expected to find 1 eviction event for pod %s, got %d", pod.Name, len(podEvictEvents.Items))
			event := podEvictEvents.Items[0]

			if expectedStarvedResource != noStarvedResource {
				// 检查 eviction.StarvedResourceKey
				starved, found := event.Annotations[eviction.StarvedResourceKey]
				if !found {
					framework.Failf("Expected to find an annotation on the eviction event for pod %s containing the starved resource %s, but it was not found",
						pod.Name, expectedStarvedResource)
				}
				starvedResource := v1.ResourceName(starved)
				gomega.Expect(starvedResource).To(gomega.Equal(expectedStarvedResource), "Expected to the starved_resource annotation on pod %s to contain %s, but got %s instead",
					pod.Name, expectedStarvedResource, starvedResource)

				// 只对 memory 检查这些 key，因为 ephemeral storage 驱逐可能由 volume 用量引起，
				// 这种情况下不会出现这些值
				if expectedStarvedResource == v1.ResourceMemory {
					// 检查 eviction.OffendingContainersKey
					offendersString, found := event.Annotations[eviction.OffendingContainersKey]
					if !found {
						framework.Failf("Expected to find an annotation on the eviction event for pod %s containing the offending containers, but it was not found",
							pod.Name)
					}
					offendingContainers := strings.Split(offendersString, ",")
					gomega.Expect(len(offendingContainers)).To(gomega.And(
						gomega.BeNumerically(">", 0),
						gomega.BeNumerically("<=", len(pod.Spec.Containers))),
						"Expected the length of offending container usage in the %s annotation to be above 0 and below len(pod.Spec.Containers)=%d: %v", eviction.OffendingContainersKey, len(pod.Spec.Containers), offendingContainers,
					)
					gomega.Expect(offendingContainers).To(gomega.ContainElement(pod.Spec.Containers[0].Name), "Expected to find the offending container: %s's usage in the %s annotation, but found %v instead",
						pod.Spec.Containers[0].Name, eviction.OffendingContainersKey, offendingContainers)

					// 检查 eviction.OffendingContainersUsageKey
					offendingUsageString, found := event.Annotations[eviction.OffendingContainersUsageKey]
					if !found {
						framework.Failf("Expected to find an annotation on the eviction event for pod %s containing the offending containers' usage, but it was not found",
							pod.Name)
					}
					offendingContainersUsage := strings.Split(offendingUsageString, ",")
					gomega.Expect(len(offendingContainersUsage)).To(gomega.And(
						gomega.BeNumerically(">", 0),
						gomega.BeNumerically("<=", len(pod.Spec.Containers))),
						"Expected the length of offending container usage in the %s annotation to be above 0 and below len(pod.Spec.Containers)=%d: %v", eviction.OffendingContainersUsageKey, len(pod.Spec.Containers), offendingContainersUsage,
					)
					usageQuantity, err := resource.ParseQuantity(offendingContainersUsage[0])
					framework.ExpectNoError(err, "parsing pod %s's %s annotation as a quantity", pod.Name, eviction.OffendingContainersUsageKey)
					request := pod.Spec.Containers[0].Resources.Requests[starvedResource]
					gomega.Expect(usageQuantity.Cmp(request)).To(gomega.Equal(1), "Expected usage of offending container: %s in pod %s to exceed its request %s",
						usageQuantity.String(), pod.Name, request.String())
				}
			}
		}
	}
}

// 节点具有该 node condition 时返回 true，否则返回 false
func hasNodeCondition(ctx context.Context, f *framework.Framework, expectedNodeCondition v1.NodeConditionType) bool {
	localNodeStatus := getLocalNode(ctx, f).Status
	_, actualNodeCondition := testutils.GetNodeCondition(&localNodeStatus, expectedNodeCondition)
	gomega.Expect(actualNodeCondition).NotTo(gomega.BeNil())
	return actualNodeCondition.Status == v1.ConditionTrue
}

func logInodeMetrics(ctx context.Context) {
	summary, err := getNodeSummary(ctx)
	if err != nil {
		framework.Logf("Error getting summary: %v", err)
		return
	}
	if summary.Node.Runtime != nil && summary.Node.Runtime.ImageFs != nil && summary.Node.Runtime.ImageFs.Inodes != nil && summary.Node.Runtime.ImageFs.InodesFree != nil {
		framework.Logf("imageFsInfo.Inodes: %d, imageFsInfo.InodesFree: %d", *summary.Node.Runtime.ImageFs.Inodes, *summary.Node.Runtime.ImageFs.InodesFree)
	}
	if summary.Node.Fs != nil && summary.Node.Fs.Inodes != nil && summary.Node.Fs.InodesFree != nil {
		framework.Logf("rootFsInfo.Inodes: %d, rootFsInfo.InodesFree: %d", *summary.Node.Fs.Inodes, *summary.Node.Fs.InodesFree)
	}
	for _, pod := range summary.Pods {
		framework.Logf("Pod: %s", pod.PodRef.Name)
		for _, container := range pod.Containers {
			if container.Rootfs != nil && container.Rootfs.InodesUsed != nil {
				framework.Logf("--- summary Container: %s inodeUsage: %d", container.Name, *container.Rootfs.InodesUsed)
			}
		}
		for _, volume := range pod.VolumeStats {
			if volume.FsStats.InodesUsed != nil {
				framework.Logf("--- summary Volume: %s inodeUsage: %d", volume.Name, *volume.FsStats.InodesUsed)
			}
		}
	}
}

func logDiskMetrics(ctx context.Context) {
	summary, err := getNodeSummary(ctx)
	if err != nil {
		framework.Logf("Error getting summary: %v", err)
		return
	}
	if summary.Node.Runtime != nil && summary.Node.Runtime.ImageFs != nil && summary.Node.Runtime.ImageFs.CapacityBytes != nil && summary.Node.Runtime.ImageFs.AvailableBytes != nil {
		framework.Logf("imageFsInfo.CapacityBytes: %d, imageFsInfo.AvailableBytes: %d", *summary.Node.Runtime.ImageFs.CapacityBytes, *summary.Node.Runtime.ImageFs.AvailableBytes)
	}
	if summary.Node.Fs != nil && summary.Node.Fs.CapacityBytes != nil && summary.Node.Fs.AvailableBytes != nil {
		framework.Logf("rootFsInfo.CapacityBytes: %d, rootFsInfo.AvailableBytes: %d", *summary.Node.Fs.CapacityBytes, *summary.Node.Fs.AvailableBytes)
	}
	if summary.Node.Fs != nil && summary.Node.Fs.Inodes != nil && summary.Node.Fs.InodesUsed != nil && summary.Node.Fs.InodesFree != nil {
		framework.Logf("rootFsInfo.Inodes: %d, rootFsInfo.InodesUsed: %d, rootFsInfo.InodesFree: %d", *summary.Node.Fs.Inodes, *summary.Node.Fs.InodesUsed, *summary.Node.Fs.InodesFree)
	}
	for _, pod := range summary.Pods {
		framework.Logf("Pod: %s", pod.PodRef.Name)
		for _, container := range pod.Containers {
			if container.Rootfs != nil && container.Rootfs.UsedBytes != nil && container.Rootfs.AvailableBytes != nil {
				framework.Logf("--- summary Container: %s UsedBytes: %d, AvailableBytes: %d", container.Name, *container.Rootfs.UsedBytes, *container.Rootfs.AvailableBytes)
			}
			if container.Rootfs != nil && container.Rootfs.Inodes != nil && container.Rootfs.InodesUsed != nil && container.Rootfs.InodesFree != nil {
				framework.Logf("--- summary Container: %s Inodes: %d, InodesUsed: %d, InodesFree: %d", container.Name, *container.Rootfs.Inodes, *container.Rootfs.InodesUsed, *container.Rootfs.InodesFree)
			}
		}
		for _, volume := range pod.VolumeStats {
			if volume.FsStats.UsedBytes != nil && volume.FsStats.AvailableBytes != nil {
				framework.Logf("--- summary Volume: %s UsedBytes: %d, AvailableBytest: %d", volume.Name, *volume.FsStats.UsedBytes, *volume.FsStats.AvailableBytes)
			}
			if volume.FsStats.Inodes != nil && volume.FsStats.InodesUsed != nil && volume.FsStats.InodesFree != nil {
				framework.Logf("--- summary Volume: %s Inodes: %d, InodesUsed: %d, InodesFree: %d", volume.Name, *volume.FsStats.Inodes, *volume.FsStats.InodesUsed, *volume.FsStats.InodesFree)
			}
		}
	}
}

func logMemoryMetricsWithSummary(ctx context.Context, summary *kubeletstatsv1alpha1.Summary) {
	if summary.Node.Memory != nil && summary.Node.Memory.WorkingSetBytes != nil && summary.Node.Memory.AvailableBytes != nil {
		framework.Logf("Node.Memory.WorkingSetBytes: %d, Node.Memory.AvailableBytes: %d", *summary.Node.Memory.WorkingSetBytes, *summary.Node.Memory.AvailableBytes)
	}
	if summary.Node.Swap != nil && summary.Node.Swap.SwapUsageBytes != nil && summary.Node.Swap.SwapAvailableBytes != nil {
		framework.Logf("summary.Node.Swap.SwapUsageBytes: %d, summary.Node.Swap.SwapAvailableBytes: %d", *summary.Node.Swap.SwapUsageBytes, *summary.Node.Swap.SwapAvailableBytes)
	}
	for _, sysContainer := range summary.Node.SystemContainers {
		if sysContainer.Name == kubeletstatsv1alpha1.SystemContainerPods && sysContainer.Memory != nil && sysContainer.Memory.WorkingSetBytes != nil && sysContainer.Memory.AvailableBytes != nil {
			framework.Logf("Allocatable.Memory.WorkingSetBytes: %d, Allocatable.Memory.AvailableBytes: %d", *sysContainer.Memory.WorkingSetBytes, *sysContainer.Memory.AvailableBytes)
		}
	}
	for _, pod := range summary.Pods {
		framework.Logf("Pod: %s", pod.PodRef.Name)
		for _, container := range pod.Containers {
			if container.Memory != nil && container.Memory.WorkingSetBytes != nil {
				framework.Logf("--- summary Container: %s WorkingSetBytes: %d", container.Name, *container.Memory.WorkingSetBytes)
				if container.Memory.UsageBytes != nil {
					framework.Logf("--- summary Container: %s UsageBytes: %d", container.Name, *container.Memory.UsageBytes)
				}
			}
			if container.Swap != nil {
				if container.Swap.SwapUsageBytes != nil {
					framework.Logf("--- summary Container: %s SwapUsageBytes: %d", container.Name, *container.Swap.SwapUsageBytes)
				}
				if container.Swap.SwapAvailableBytes != nil {
					framework.Logf("--- summary Container: %s SwapAvailableBytes: %d", container.Name, *container.Swap.SwapAvailableBytes)
				}
			}
		}
	}
}

func logMemoryMetrics(ctx context.Context) {
	summary, err := getNodeSummary(ctx)
	if err != nil {
		framework.Logf("Error getting summary: %v", err)
		return
	}
	logMemoryMetricsWithSummary(ctx, summary)
}

func logPidMetrics(ctx context.Context) {
	summary, err := getNodeSummary(ctx)
	if err != nil {
		framework.Logf("Error getting summary: %v", err)
		return
	}
	if summary.Node.Rlimit != nil && summary.Node.Rlimit.MaxPID != nil && summary.Node.Rlimit.NumOfRunningProcesses != nil {
		framework.Logf("Node.Rlimit.MaxPID: %d, Node.Rlimit.RunningProcesses: %d", *summary.Node.Rlimit.MaxPID, *summary.Node.Rlimit.NumOfRunningProcesses)
	}
}

func eventuallyGetSummary(ctx context.Context) (s *kubeletstatsv1alpha1.Summary) {
	gomega.Eventually(ctx, func() error {
		summary, err := getNodeSummary(ctx)
		if err != nil {
			return err
		}
		if summary == nil || summary.Node.Fs == nil || summary.Node.Fs.InodesFree == nil || summary.Node.Fs.AvailableBytes == nil {
			return fmt.Errorf("some part of data is nil")
		}
		s = summary
		return nil
	}, time.Minute, evictionPollInterval).Should(gomega.BeNil())
	return
}

// 返回一个不消耗任何资源的 Pod
func innocentPod() *v1.Pod {
	// 由于 https://github.com/kubernetes/kubernetes/issues/115819，
	// 使用 evictionHard 时，我们曾把 grace period 设为 0，这意味着采用默认设置（30 秒）。
	// 立即发送 SIGTERM 有助于减少测试的不稳定性。
	var gracePeriod int64 = 1
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "innocent-pod"},
		Spec: v1.PodSpec{
			RestartPolicy:                 v1.RestartPolicyNever,
			TerminationGracePeriodSeconds: &gracePeriod,
			Containers: []v1.Container{
				{
					Image: busyboxImage,
					Name:  "innocent-container",
					Command: []string{
						"sh",
						"-c",
						"while true; do sleep 5; done",
					},
					Resources: v1.ResourceRequirements{
						// 设置这些值，是为了不让这个 Pod 被判定为超出限制。
						// 如果不设置 requests，就会假定 request 为 0，从而永远被判定为超限。
						// 对 innocent Pod 来说，这样可以避免这个问题。
						Requests: v1.ResourceList{
							v1.ResourceEphemeralStorage: resource.MustParse("50Mi"),
							v1.ResourceMemory:           resource.MustParse("50Mi"),
						},
						Limits: v1.ResourceList{
							v1.ResourceEphemeralStorage: resource.MustParse("50Mi"),
							v1.ResourceMemory:           resource.MustParse("50Mi"),
						},
					},
				},
			},
		},
	}
}

const (
	volumeMountPath = "/test-mnt"
	volumeName      = "test-volume"
)

func inodeConsumingPod(name string, numFiles int, volumeSource *v1.VolumeSource, sleepAfterExecuting bool) *v1.Pod {
	path := ""
	if volumeSource != nil {
		path = volumeMountPath
	}
	// 每次迭代创建一个空文件
	return podWithCommand(volumeSource, v1.ResourceRequirements{}, numFiles, name, fmt.Sprintf("touch %s${i}.txt; sleep 0.001;", filepath.Join(path, "file")), sleepAfterExecuting)
}

func diskConsumingPod(name string, diskConsumedMB int, volumeSource *v1.VolumeSource, resources v1.ResourceRequirements) *v1.Pod {
	path := ""
	if volumeSource != nil {
		path = volumeMountPath
	}
	// 每次迭代写入 1 Mb，因此迭代 diskConsumedMB 次。
	return podWithCommand(volumeSource, resources, diskConsumedMB, name, fmt.Sprintf("dd if=/dev/urandom of=%s${i} bs=1048576 count=1 2>/dev/null; sleep .1;", filepath.Join(path, "file")), true)
}

// diskConsumingSidecarPod 返回一个 Pod，其可重启的 init container（sidecar）
// 向自己的可写层写入 diskConsumedMB MB 数据，并对该 sidecar 应用给定的资源限制与请求。
// 主容器只负责休眠，这样该 Pod 是否会被驱逐完全取决于 sidecar 的磁盘用量。
func diskConsumingSidecarPod(name string, diskConsumedMB int, sidecarResources v1.ResourceRequirements) *v1.Pod {
	var gracePeriod int64 = 1
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-pod", name)},
		Spec: v1.PodSpec{
			RestartPolicy:                 v1.RestartPolicyNever,
			TerminationGracePeriodSeconds: &gracePeriod,
			InitContainers: []v1.Container{
				{
					Image:         busyboxImage,
					Name:          fmt.Sprintf("%s-sidecar", name),
					RestartPolicy: &containerRestartPolicyAlways,
					Command: []string{
						"sh",
						"-c",
						fmt.Sprintf("i=0; while [ $i -lt %d ]; do dd if=/dev/urandom of=file${i} bs=1048576 count=1 2>/dev/null; sleep .1; i=$(($i+1)); done; while true; do sleep 5; done", diskConsumedMB),
					},
					Resources: sidecarResources,
				},
			},
			Containers: []v1.Container{
				{
					Image:   busyboxImage,
					Name:    fmt.Sprintf("%s-container", name),
					Command: []string{"sh", "-c", "sleep infinity"},
				},
			},
		},
	}
}

func pidConsumingPod(name string, numProcesses int) *v1.Pod {
	// 放慢迭代速度，避免竞态：在进程数激增时，驱逐可能发生在统计数据
	// 记录到正确进程数之前。
	return podWithCommand(nil, v1.ResourceRequirements{}, numProcesses, name, "/bin/sleep 0.01; (/bin/sleep 3600)&", true)
}

// podWithCommand 返回一个使用给定 volumeSource 和 resourceRequirements 的 Pod。
func podWithCommand(volumeSource *v1.VolumeSource, resources v1.ResourceRequirements, iterations int, name, command string, sleepAfterExecuting bool) *v1.Pod {
	// 由于 https://github.com/kubernetes/kubernetes/issues/115819，
	// 使用 evictionHard 时，我们曾把 grace period 设为 0，这意味着采用默认设置（30 秒）。
	// 立即发送 SIGTERM 有助于减少测试的不稳定性。
	var gracePeriod int64 = 1
	volumeMounts := []v1.VolumeMount{}
	volumes := []v1.Volume{}
	if volumeSource != nil {
		volumeMounts = []v1.VolumeMount{{MountPath: volumeMountPath, Name: volumeName}}
		volumes = []v1.Volume{{Name: volumeName, VolumeSource: *volumeSource}}
	}

	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-pod", name)},
		Spec: v1.PodSpec{
			RestartPolicy:                 v1.RestartPolicyNever,
			TerminationGracePeriodSeconds: &gracePeriod,
			Containers: []v1.Container{
				{
					Image: busyboxImage,
					Name:  fmt.Sprintf("%s-container", name),
					Command: []string{
						"sh",
						"-c",
						fmt.Sprintf("i=0; while [ $i -lt %d ]; do %s i=$(($i+1)); done; while %v; do sleep 5; done", iterations, command, sleepAfterExecuting),
					},
					Resources:    resources,
					VolumeMounts: volumeMounts,
				},
			},
			Volumes: volumes,
		},
	}
}

func getMemhogPod(podName string, ctnName string, res v1.ResourceRequirements) *v1.Pod {
	// 由于 https://github.com/kubernetes/kubernetes/issues/115819，
	// 使用 evictionHard 时，我们曾把 grace period 设为 0，这意味着采用默认设置（30 秒）。
	// 立即发送 SIGTERM 有助于减少测试的不稳定性。
	var gracePeriod int64 = 1
	env := []v1.EnvVar{
		{
			Name: "MEMORY_LIMIT",
			ValueFrom: &v1.EnvVarSource{
				ResourceFieldRef: &v1.ResourceFieldSelector{
					Resource: "limits.memory",
				},
			},
		},
	}

	// 如果指定了 limit，就把它的 80% 传给 -mem-total；否则通过 downward API
	// 传入 limits.memory，它代表了可用的总内存。
	// 这样可避免 guaranteed Pod 因内存限制较低而触发 OOM kill，
	// 否则测试会因此不合理地失败。
	var memLimit string
	if limit, ok := res.Limits[v1.ResourceMemory]; ok {
		memLimit = strconv.Itoa(int(
			float64(limit.Value()) * 0.8))
	} else {
		memLimit = "$(MEMORY_LIMIT)"
	}

	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
		},
		Spec: v1.PodSpec{
			RestartPolicy:                 v1.RestartPolicyNever,
			TerminationGracePeriodSeconds: &gracePeriod,
			Containers: []v1.Container{
				{
					Name:            ctnName,
					Image:           imageutils.GetE2EImage(imageutils.Agnhost),
					ImagePullPolicy: "Always",
					Env:             env,
					// 60 分钟超时 * 60s / 每 10s 一个 tick = 超时前有 360 个 tick => 约 11.11Mi/tick
					// 可以填满约 4Gi 内存，因此初步估计取 12Mi/tick。
					// 如果节点的总内存增加，可能会因超时而出现间歇性失败。
					Args:      []string{"stress", "--mem-alloc-size", "12Mi", "--mem-alloc-sleep", "10s", "--mem-total", memLimit},
					Resources: res,
				},
			},
		},
	}
}

func getMemhogPodWithPodLevelResources(podName string, podLevelRes v1.ResourceRequirements, ctnName string, res v1.ResourceRequirements) *v1.Pod {
	pod := getMemhogPod(podName, ctnName, res)
	pod.Spec.Resources = &podLevelRes

	return pod
}
