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

package eviction

import (
	"context"
	"fmt"
	goruntime "runtime"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/record"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	kubeapi "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/scheduling"
	"k8s.io/kubernetes/pkg/features"
	evictionapi "k8s.io/kubernetes/pkg/kubelet/eviction/api"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	kubelettypes "k8s.io/kubernetes/pkg/kubelet/types"
	"k8s.io/kubernetes/test/utils/ktesting"
	testingclock "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
)

// 测试中使用的 Pod 优先级取值：低、默认与高
const (
	lowPriority     = -1
	defaultPriority = 0
	highPriority    = 1
)

// mockPodKiller 是一个 mock 实现，用于记录测试中被驱逐的是哪个 Pod
type mockPodKiller struct {
	pod                 *v1.Pod
	evict               bool
	statusFn            func(*v1.PodStatus)
	gracePeriodOverride *int64
}

// killPodNow 记录被 kill 的 Pod
func (m *mockPodKiller) killPodNow(pod *v1.Pod, evict bool, gracePeriodOverride *int64, statusFn func(*v1.PodStatus)) error {
	m.pod = pod
	m.statusFn = statusFn
	m.evict = evict
	m.gracePeriodOverride = gracePeriodOverride
	return nil
}

// mockDiskInfoProvider 用于模拟测试中的磁盘信息提供者
type mockDiskInfoProvider struct {
	dedicatedImageFs     *bool
	dedicatedContainerFs *bool
}

// HasDedicatedImageFs 返回 mock 的专用镜像文件系统标志
func (m *mockDiskInfoProvider) HasDedicatedImageFs(_ context.Context) (bool, error) {
	return ptr.Deref(m.dedicatedImageFs, false), nil
}

// HasDedicatedContainerFs 返回 mock 的专用容器文件系统标志
func (m *mockDiskInfoProvider) HasDedicatedContainerFs(_ context.Context) (bool, error) {
	return ptr.Deref(m.dedicatedContainerFs, false), nil
}

// mockDiskGC 用于模拟触发镜像和容器垃圾回收
type mockDiskGC struct {
	err                 error
	imageGCInvoked      bool
	containerGCInvoked  bool
	fakeSummaryProvider *fakeSummaryProvider
	summaryAfterGC      *statsapi.Summary
}

// DeleteUnusedImages 返回 mock 的删除镜像结果
func (m *mockDiskGC) DeleteUnusedImages(_ context.Context) error {
	m.imageGCInvoked = true
	if m.summaryAfterGC != nil && m.fakeSummaryProvider != nil {
		m.fakeSummaryProvider.result = m.summaryAfterGC
	}
	return m.err
}

// DeleteAllUnusedContainers 返回 mock 的删除容器结果
func (m *mockDiskGC) DeleteAllUnusedContainers(_ context.Context) error {
	m.containerGCInvoked = true
	if m.summaryAfterGC != nil && m.fakeSummaryProvider != nil {
		m.fakeSummaryProvider.result = m.summaryAfterGC
	}
	return m.err
}

// makePodWithMemoryStats 构造一个带内存工作集统计的 Pod 及其统计信息
// requests/limits 同时包含 CPU 与内存，memoryWorkingSet 为该 Pod 的内存工作集大小
func makePodWithMemoryStats(name string, priority int32, requests v1.ResourceList, limits v1.ResourceList, memoryWorkingSet string) (*v1.Pod, statsapi.PodStats) {
	pod := newPod(name, priority, []v1.Container{
		newContainer(name, requests, limits),
	}, nil)
	podStats := newPodMemoryStats(pod, resource.MustParse(memoryWorkingSet))
	return pod, podStats
}

// makePodWithPIDStats 构造一个带进程数统计的 Pod 及其统计信息，processCount 为该 Pod 的进程数
func makePodWithPIDStats(name string, priority int32, processCount uint64) (*v1.Pod, statsapi.PodStats) {
	pod := newPod(name, priority, []v1.Container{
		newContainer(name, nil, nil),
	}, nil)
	podStats := newPodProcessStats(pod, processCount)
	return pod, podStats
}

// makePodWithDiskStats 构造一个带磁盘用量统计的 Pod 及其统计信息
// 分别指定 rootfs、日志与本地卷的用量，并可附加卷列表
func makePodWithDiskStats(name string, priority int32, requests v1.ResourceList, limits v1.ResourceList, rootFsUsed, logsUsed, perLocalVolumeUsed string, volumes []v1.Volume) (*v1.Pod, statsapi.PodStats) {
	pod := newPod(name, priority, []v1.Container{
		newContainer(name, requests, limits),
	}, volumes)
	podStats := newPodDiskStats(pod, parseQuantity(rootFsUsed), parseQuantity(logsUsed), parseQuantity(perLocalVolumeUsed))
	return pod, podStats
}

// makePodWithLocalStorageCapacityIsolationOpen 构造本地存储容量隔离场景的 Pod：
// 为 emptyDir 卷设置 SizeLimit，并按用例名返回相应的内存或临时存储用量统计
func makePodWithLocalStorageCapacityIsolationOpen(name string, priority int32, requests v1.ResourceList, limits v1.ResourceList, memoryWorkingSet string) (*v1.Pod, statsapi.PodStats) {
	vol := newVolume("local-volume", v1.VolumeSource{
		EmptyDir: &v1.EmptyDirVolumeSource{
			SizeLimit: resource.NewQuantity(requests.Memory().Value(), resource.BinarySI),
		},
	})
	var vols []v1.Volume
	vols = append(vols, vol)
	pod := newPod(name, priority, []v1.Container{
		newContainer(name, requests, limits),
	}, vols)

	var podStats statsapi.PodStats
	switch name {
	case "empty-dir":
		podStats = newPodMemoryStats(pod, *resource.NewQuantity(requests.Memory().Value()*2, resource.BinarySI))
	case "container-ephemeral-storage-limit":
		podStats = newPodMemoryStats(pod, *resource.NewQuantity(limits.StorageEphemeral().Value(), resource.BinarySI))
	case "pod-ephemeral-storage-limit":
		podStats = newPodMemoryStats(pod, *resource.NewQuantity(limits.StorageEphemeral().Value()*2, resource.BinarySI))
	default:
		podStats = newPodMemoryStats(pod, resource.MustParse(memoryWorkingSet))
	}
	return pod, podStats
}

// makePIDStats 根据节点可用 PID 数与运行中的进程数生成节点统计摘要，并附带各 Pod 的统计信息
func makePIDStats(nodeAvailablePIDs string, numberOfRunningProcesses string, podStats map[*v1.Pod]statsapi.PodStats) *statsapi.Summary {
	val := resource.MustParse(nodeAvailablePIDs)
	availablePIDs := int64(val.Value())

	parsed := resource.MustParse(numberOfRunningProcesses)
	NumberOfRunningProcesses := int64(parsed.Value())
	result := &statsapi.Summary{
		Node: statsapi.NodeStats{
			Rlimit: &statsapi.RlimitStats{
				MaxPID:                &availablePIDs,
				NumOfRunningProcesses: &NumberOfRunningProcesses,
			},
		},
		Pods: []statsapi.PodStats{},
	}
	for _, podStat := range podStats {
		result.Pods = append(result.Pods, podStat)
	}
	return result
}

// makeMemoryStats 根据节点可用内存与工作集生成节点统计摘要，
// 同时注入 Linux 与 Windows 的系统容器统计以覆盖两条观测路径，并附带各 Pod 的统计信息
func makeMemoryStats(nodeAvailableBytes string, podStats map[*v1.Pod]statsapi.PodStats) *statsapi.Summary {
	val := resource.MustParse(nodeAvailableBytes)
	availableBytes := uint64(val.Value())
	WorkingSetBytes := uint64(val.Value())
	result := &statsapi.Summary{
		Node: statsapi.NodeStats{
			Memory: &statsapi.MemoryStats{
				AvailableBytes:  &availableBytes,
				WorkingSetBytes: &WorkingSetBytes,
			},
			SystemContainers: []statsapi.ContainerStats{
				// 用于观测 Linux 平台上的内存信号
				{
					Name: statsapi.SystemContainerPods,
					Memory: &statsapi.MemoryStats{
						AvailableBytes:  &availableBytes,
						WorkingSetBytes: &WorkingSetBytes,
					},
				},
				// 用于观测 Windows 平台上的内存信号
				{
					Name: statsapi.SystemContainerWindowsGlobalCommitMemory,
					Memory: &statsapi.MemoryStats{
						AvailableBytes: &availableBytes,
						UsageBytes:     &WorkingSetBytes,
					},
				},
			},
		},
		Pods: []statsapi.PodStats{},
	}
	for _, podStat := range podStats {
		result.Pods = append(result.Pods, podStat)
	}
	return result
}

// diskStats 描述节点 rootfs、imagefs 与 containerfs 的可用字节数以及各 Pod 的磁盘用量统计
type diskStats struct {
	rootFsAvailableBytes  string
	imageFsAvailableBytes string
	// 可选的容器文件系统
	// 若未指定，则假定 imagefs 与 containerfs 相同
	containerFsAvailableBytes string
	podStats                  map[*v1.Pod]statsapi.PodStats
}

// makeDiskStats 根据 diskStats 生成节点文件系统与镜像/容器文件系统的容量统计摘要
// （容量按可用量的 2 倍模拟），并附带各 Pod 的统计信息
func makeDiskStats(diskStats diskStats) *statsapi.Summary {
	rootFsVal := resource.MustParse(diskStats.rootFsAvailableBytes)
	rootFsBytes := uint64(rootFsVal.Value())
	rootFsCapacityBytes := uint64(rootFsVal.Value() * 2)
	imageFsVal := resource.MustParse(diskStats.imageFsAvailableBytes)
	imageFsBytes := uint64(imageFsVal.Value())
	imageFsCapacityBytes := uint64(imageFsVal.Value() * 2)
	if diskStats.containerFsAvailableBytes == "" {
		diskStats.containerFsAvailableBytes = diskStats.imageFsAvailableBytes
	}
	containerFsVal := resource.MustParse(diskStats.containerFsAvailableBytes)
	containerFsBytes := uint64(containerFsVal.Value())
	containerFsCapacityBytes := uint64(containerFsVal.Value() * 2)
	result := &statsapi.Summary{
		Node: statsapi.NodeStats{
			Fs: &statsapi.FsStats{
				AvailableBytes: &rootFsBytes,
				CapacityBytes:  &rootFsCapacityBytes,
			},
			Runtime: &statsapi.RuntimeStats{
				ImageFs: &statsapi.FsStats{
					AvailableBytes: &imageFsBytes,
					CapacityBytes:  &imageFsCapacityBytes,
				},
				ContainerFs: &statsapi.FsStats{
					AvailableBytes: &containerFsBytes,
					CapacityBytes:  &containerFsCapacityBytes,
				},
			},
		},
		Pods: []statsapi.PodStats{},
	}
	for _, podStat := range diskStats.podStats {
		result.Pods = append(result.Pods, podStat)
	}
	return result
}

// podToMake 描述测试中需要构造的 Pod 在各维度上的用量，供不同测试用例复用
type podToMake struct {
	name                     string
	priority                 int32
	requests                 v1.ResourceList
	limits                   v1.ResourceList
	memoryWorkingSet         string
	pidUsage                 uint64
	rootFsUsed               string
	logsFsUsed               string
	logsFsInodesUsed         string
	rootFsInodesUsed         string
	perLocalVolumeUsed       string
	perLocalVolumeInodesUsed string
}

// TestMemoryPressure_VerifyPodStatus 验证内存压力下被驱逐 Pod 的状态字段
// （Phase、Reason、Message 与 DisruptionTarget 条件）是否正确
func TestMemoryPressure_VerifyPodStatus(t *testing.T) {
	tCtx := ktesting.Init(t)
	podMaker := makePodWithMemoryStats
	summaryStatsMaker := makeMemoryStats
	podsToMake := []podToMake{
		{name: "below-requests", requests: newResourceList("", "1Gi", ""), limits: newResourceList("", "1Gi", ""), memoryWorkingSet: "900Mi"},
		{name: "above-requests", requests: newResourceList("", "100Mi", ""), limits: newResourceList("", "1Gi", ""), memoryWorkingSet: "700Mi"},
	}
	pods := []*v1.Pod{}
	podStats := map[*v1.Pod]statsapi.PodStats{}
	for _, podToMake := range podsToMake {
		pod, podStat := podMaker(podToMake.name, podToMake.priority, podToMake.requests, podToMake.limits, podToMake.memoryWorkingSet)
		pods = append(pods, pod)
		podStats[pod] = podStat
	}
	activePodsFunc := func() []*v1.Pod {
		return pods
	}

	fakeClock := testingclock.NewFakeClock(time.Now())
	podKiller := &mockPodKiller{}
	diskInfoProvider := &mockDiskInfoProvider{dedicatedImageFs: ptr.To(false)}
	diskGC := &mockDiskGC{err: nil}
	nodeRef := &v1.ObjectReference{Kind: "Node", Name: "test", UID: types.UID("test"), Namespace: ""}

	config := Config{
		PressureTransitionPeriod: time.Minute * 5,
		Thresholds: []evictionapi.Threshold{
			{
				Signal:   evictionapi.SignalMemoryAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("2Gi"),
				},
			},
		},
	}
	summaryProvider := &fakeSummaryProvider{result: summaryStatsMaker("1500Mi", podStats)}
	manager := &managerImpl{
		clock:                        fakeClock,
		killPodFunc:                  podKiller.killPodNow,
		imageGC:                      diskGC,
		containerGC:                  diskGC,
		config:                       config,
		recorder:                     &record.FakeRecorder{},
		summaryProvider:              summaryProvider,
		nodeRef:                      nodeRef,
		nodeConditionsLastObservedAt: nodeConditionsObservedAt{},
		thresholdsFirstObservedAt:    thresholdsObservedAt{},
	}

	// 同步一次以检测内存压力
	_, err := manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

	if err != nil {
		t.Fatalf("Manager expects no error but got %v", err)
	}
	// 验证已检测到内存压力
	if !manager.IsUnderMemoryPressure() {
		t.Fatalf("Manager should have detected memory pressure")
	}

	// 验证已选中一个 Pod 用于驱逐
	if podKiller.pod == nil {
		t.Fatalf("Manager should have selected a pod for eviction")
	}

	wantPodStatus := v1.PodStatus{
		Phase:   v1.PodFailed,
		Reason:  "Evicted",
		Message: "The node was low on resource: memory. Threshold quantity: 2Gi, available: 1500Mi. ",
		Conditions: []v1.PodCondition{{
			Type:    "DisruptionTarget",
			Status:  "True",
			Reason:  "TerminationByKubelet",
			Message: "The node was low on resource: memory. Threshold quantity: 2Gi, available: 1500Mi. ",
		}},
	}

	// 应用状态更新函数后，验证被驱逐 Pod 的状态
	podKiller.statusFn(&podKiller.pod.Status)
	if diff := cmp.Diff(wantPodStatus, podKiller.pod.Status, cmpopts.IgnoreFields(v1.PodCondition{}, "LastProbeTime", "LastTransitionTime")); diff != "" {
		t.Errorf("Unexpected pod status of the evicted pod (-want,+got):\n%s", diff)
	}
}

// TestPIDPressure_VerifyPodStatus 验证 PID 压力下被驱逐 Pod 的状态字段是否正确；
// Windows 不支持 PID 压力，直接跳过该测试
func TestPIDPressure_VerifyPodStatus(t *testing.T) {
	tCtx := ktesting.Init(t)
	if goruntime.GOOS == "windows" {
		t.Skip("PID pressure is not supported on Windows")
	}
	testCases := map[string]struct {
		wantPodStatus v1.PodStatus
	}{
		"eviction due to pid pressure": {
			wantPodStatus: v1.PodStatus{
				Phase:   v1.PodFailed,
				Reason:  "Evicted",
				Message: "The node was low on resource: pids. Threshold quantity: 1200, available: 500. ",
			},
		},
	}
	for _, tc := range testCases {
		podMaker := makePodWithPIDStats
		summaryStatsMaker := makePIDStats
		podsToMake := []podToMake{
			{name: "pod1", priority: lowPriority, pidUsage: 500},
			{name: "pod2", priority: defaultPriority, pidUsage: 500},
		}
		pods := []*v1.Pod{}
		podStats := map[*v1.Pod]statsapi.PodStats{}
		for _, podToMake := range podsToMake {
			pod, podStat := podMaker(podToMake.name, podToMake.priority, 2)
			pods = append(pods, pod)
			podStats[pod] = podStat
		}
		activePodsFunc := func() []*v1.Pod {
			return pods
		}

		fakeClock := testingclock.NewFakeClock(time.Now())
		podKiller := &mockPodKiller{}
		diskInfoProvider := &mockDiskInfoProvider{dedicatedImageFs: ptr.To(false), dedicatedContainerFs: ptr.To(false)}
		diskGC := &mockDiskGC{err: nil}
		nodeRef := &v1.ObjectReference{Kind: "Node", Name: "test", UID: types.UID("test"), Namespace: ""}

		config := Config{
			PressureTransitionPeriod: time.Minute * 5,
			Thresholds: []evictionapi.Threshold{
				{
					Signal:   evictionapi.SignalPIDAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1200"),
					},
				},
			},
		}
		summaryProvider := &fakeSummaryProvider{result: summaryStatsMaker("1500", "1000", podStats)}
		manager := &managerImpl{
			clock:                        fakeClock,
			killPodFunc:                  podKiller.killPodNow,
			imageGC:                      diskGC,
			containerGC:                  diskGC,
			config:                       config,
			recorder:                     &record.FakeRecorder{},
			summaryProvider:              summaryProvider,
			nodeRef:                      nodeRef,
			nodeConditionsLastObservedAt: nodeConditionsObservedAt{},
			thresholdsFirstObservedAt:    thresholdsObservedAt{},
		}

		// 同步一次以检测 PID 压力
		_, err := manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

		if err != nil {
			t.Fatalf("Manager expects no error but got %v", err)
		}

		// 验证已检测到 PID 压力
		if !manager.IsUnderPIDPressure() {
			t.Fatalf("Manager should have detected PID pressure")
		}

		// 验证已选中一个 Pod 用于驱逐
		if podKiller.pod == nil {
			t.Fatalf("Manager should have selected a pod for eviction")
		}

		wantPodStatus := tc.wantPodStatus.DeepCopy()
		wantPodStatus.Conditions = append(wantPodStatus.Conditions, v1.PodCondition{
			Type:    "DisruptionTarget",
			Status:  "True",
			Reason:  "TerminationByKubelet",
			Message: "The node was low on resource: pids. Threshold quantity: 1200, available: 500. ",
		})

		// 应用状态更新函数后，验证被驱逐 Pod 的状态
		podKiller.statusFn(&podKiller.pod.Status)
		if diff := cmp.Diff(*wantPodStatus, podKiller.pod.Status, cmpopts.IgnoreFields(v1.PodCondition{}, "LastProbeTime", "LastTransitionTime")); diff != "" {
			t.Errorf("Unexpected pod status of the evicted pod (-want,+got):\n%s", diff)
		}
	}
}

// TestDiskPressureNodeFs_VerifyPodStatus 验证磁盘压力下被驱逐 Pod 的状态字段，
// 覆盖无独立 imagefs、独立 imagefs、独立 containerfs 以及特性开关关闭时报错等场景
func TestDiskPressureNodeFs_VerifyPodStatus(t *testing.T) {
	tCtx := ktesting.Init(t)
	testCases := map[string]struct {
		nodeFsStats                   string
		imageFsStats                  string
		containerFsStats              string
		evictionMessage               string
		kubeletSeparateDiskFeature    bool
		writeableSeparateFromReadOnly bool
		thresholdToMonitor            evictionapi.Threshold
		podToMakes                    []podToMake
		dedicatedImageFs              *bool
		expectErr                     string
	}{
		"eviction due to disk pressure; no image fs": {
			dedicatedImageFs: ptr.To(false),
			nodeFsStats:      "1.5Gi",
			imageFsStats:     "10Gi",
			containerFsStats: "10Gi",
			thresholdToMonitor: evictionapi.Threshold{
				Signal:   evictionapi.SignalNodeFsAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("2Gi"),
				},
			},
			evictionMessage: "The node was low on resource: ephemeral-storage. Threshold quantity: 2Gi, available: 1536Mi. Container above-requests was using 700Mi, request is 100Mi, has larger consumption of ephemeral-storage. ",
			podToMakes: []podToMake{
				{name: "below-requests", requests: newResourceList("", "", "1Gi"), limits: newResourceList("", "", "1Gi"), rootFsUsed: "900Mi"},
				{name: "above-requests", requests: newResourceList("", "", "100Mi"), limits: newResourceList("", "", "1Gi"), rootFsUsed: "700Mi"},
			},
		},
		"eviction due to image disk pressure; image fs": {
			dedicatedImageFs: ptr.To(true),
			nodeFsStats:      "1Gi",
			imageFsStats:     "10Gi",
			containerFsStats: "10Gi",
			evictionMessage:  "The node was low on resource: ephemeral-storage. Threshold quantity: 50Gi, available: 10Gi. Container above-requests was using 80Gi, request is 50Gi, has larger consumption of ephemeral-storage. ",
			thresholdToMonitor: evictionapi.Threshold{
				Signal:   evictionapi.SignalImageFsAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("50Gi"),
				},
			},
			podToMakes: []podToMake{
				{name: "below-requests", requests: newResourceList("", "", "1Gi"), limits: newResourceList("", "", "1Gi"), rootFsUsed: "900Mi"},
				{name: "above-requests", requests: newResourceList("", "", "50Gi"), limits: newResourceList("", "", "50Gi"), rootFsUsed: "80Gi"},
			},
		},
		"eviction due to container disk pressure; feature off; error; container fs": {
			dedicatedImageFs:              ptr.To(true),
			kubeletSeparateDiskFeature:    false,
			writeableSeparateFromReadOnly: true,
			expectErr:                     "KubeletSeparateDiskGC is turned off but we still have a split filesystem",
			nodeFsStats:                   "1Gi",
			imageFsStats:                  "100Gi",
			containerFsStats:              "10Gi",
			evictionMessage:               "The node was low on resource: ephemeral-storage. Threshold quantity: 50Gi, available: 10Gi.Container above-requests was using 80Gi, request is 50Gi, has larger consumption of ephemeral-storage. ",
			thresholdToMonitor: evictionapi.Threshold{
				Signal:   evictionapi.SignalContainerFsAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("50Gi"),
				},
			},
			podToMakes: []podToMake{
				{name: "below-requests", requests: newResourceList("", "", "1Gi"), limits: newResourceList("", "", "1Gi"), rootFsUsed: "900Mi"},
				{name: "above-requests", requests: newResourceList("", "", "50Gi"), limits: newResourceList("", "", "50Gi"), rootFsUsed: "80Gi"},
			},
		},
		"eviction due to container disk pressure; container fs": {
			dedicatedImageFs:              ptr.To(true),
			kubeletSeparateDiskFeature:    true,
			writeableSeparateFromReadOnly: true,
			nodeFsStats:                   "10Gi",
			imageFsStats:                  "100Gi",
			containerFsStats:              "10Gi",
			evictionMessage:               "The node was low on resource: ephemeral-storage. Threshold quantity: 50Gi, available: 10Gi. Container above-requests was using 80Gi, request is 50Gi, has larger consumption of ephemeral-storage. ",
			thresholdToMonitor: evictionapi.Threshold{
				Signal:   evictionapi.SignalNodeFsAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("50Gi"),
				},
			},
			podToMakes: []podToMake{
				{name: "below-requests", requests: newResourceList("", "", "1Gi"), limits: newResourceList("", "", "1Gi"), rootFsUsed: "900Mi"},
				{name: "above-requests", requests: newResourceList("", "", "50Gi"), limits: newResourceList("", "", "50Gi"), rootFsUsed: "80Gi"},
			},
		},
	}
	for _, tc := range testCases {
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.KubeletSeparateDiskGC, tc.kubeletSeparateDiskFeature)

		podMaker := makePodWithDiskStats
		summaryStatsMaker := makeDiskStats
		podsToMake := tc.podToMakes
		wantPodStatus := v1.PodStatus{
			Phase:   v1.PodFailed,
			Reason:  "Evicted",
			Message: tc.evictionMessage,
		}
		pods := []*v1.Pod{}
		podStats := map[*v1.Pod]statsapi.PodStats{}
		for _, podToMake := range podsToMake {
			pod, podStat := podMaker(podToMake.name, podToMake.priority, podToMake.requests, podToMake.limits, podToMake.rootFsUsed, podToMake.logsFsUsed, podToMake.perLocalVolumeUsed, nil)
			pods = append(pods, pod)
			podStats[pod] = podStat
		}
		activePodsFunc := func() []*v1.Pod {
			return pods
		}

		fakeClock := testingclock.NewFakeClock(time.Now())
		podKiller := &mockPodKiller{}
		diskInfoProvider := &mockDiskInfoProvider{dedicatedImageFs: tc.dedicatedImageFs, dedicatedContainerFs: &tc.writeableSeparateFromReadOnly}
		diskGC := &mockDiskGC{err: nil}
		nodeRef := &v1.ObjectReference{Kind: "Node", Name: "test", UID: types.UID("test"), Namespace: ""}

		config := Config{
			PressureTransitionPeriod: time.Minute * 5,
			Thresholds:               []evictionapi.Threshold{tc.thresholdToMonitor},
		}
		diskStat := diskStats{
			rootFsAvailableBytes:      tc.nodeFsStats,
			imageFsAvailableBytes:     tc.imageFsStats,
			containerFsAvailableBytes: tc.containerFsStats,
			podStats:                  podStats,
		}
		summaryProvider := &fakeSummaryProvider{result: summaryStatsMaker(diskStat)}
		manager := &managerImpl{
			clock:                        fakeClock,
			killPodFunc:                  podKiller.killPodNow,
			imageGC:                      diskGC,
			containerGC:                  diskGC,
			config:                       config,
			recorder:                     &record.FakeRecorder{},
			summaryProvider:              summaryProvider,
			nodeRef:                      nodeRef,
			nodeConditionsLastObservedAt: nodeConditionsObservedAt{},
			thresholdsFirstObservedAt:    thresholdsObservedAt{},
		}

		// 同步
		pods, synchErr := manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

		if synchErr == nil && tc.expectErr != "" {
			t.Fatalf("Manager should report error but did not")
		} else if tc.expectErr != "" && synchErr != nil {
			if diff := cmp.Diff(tc.expectErr, synchErr.Error()); diff != "" {
				t.Errorf("Unexpected error (-want,+got):\n%s", diff)
			}
		} else {
			// 验证管理器已检测到磁盘压力
			if !manager.IsUnderDiskPressure() {
				t.Fatalf("Manager should report disk pressure")
			}

			// 验证已选中一个 Pod 用于驱逐
			if podKiller.pod == nil {
				t.Fatalf("Manager should have selected a pod for eviction")
			}

			wantPodStatus.Conditions = append(wantPodStatus.Conditions, v1.PodCondition{
				Type:    "DisruptionTarget",
				Status:  "True",
				Reason:  "TerminationByKubelet",
				Message: tc.evictionMessage,
			})

			// 应用状态更新函数后，验证被驱逐 Pod 的状态
			podKiller.statusFn(&podKiller.pod.Status)
			if diff := cmp.Diff(wantPodStatus, podKiller.pod.Status, cmpopts.IgnoreFields(v1.PodCondition{}, "LastProbeTime", "LastTransitionTime")); diff != "" {
				t.Errorf("Unexpected pod status of the evicted pod (-want,+got):\n%s", diff)
			}
		}
	}
}

// TestMemoryPressure 验证内存压力下驱逐管理器的完整行为：
// 软阈值在宽限期内只上报压力而不驱逐 Pod，超过宽限期后按优先级与用量选择目标 Pod 驱逐；
// 压力解除后需经过转换期，节点上的内存压力条件才会被清除；期间准入控制随压力状态变化。
func TestMemoryPressure(t *testing.T) {
	tCtx := ktesting.Init(t)
	podMaker := makePodWithMemoryStats
	summaryStatsMaker := makeMemoryStats
	podsToMake := []podToMake{
		{name: "guaranteed-low-priority-high-usage", priority: lowPriority, requests: newResourceList("100m", "1Gi", ""), limits: newResourceList("100m", "1Gi", ""), memoryWorkingSet: "900Mi"},
		{name: "burstable-below-requests", priority: defaultPriority, requests: newResourceList("100m", "100Mi", ""), limits: newResourceList("200m", "1Gi", ""), memoryWorkingSet: "50Mi"},
		{name: "burstable-above-requests", priority: defaultPriority, requests: newResourceList("100m", "100Mi", ""), limits: newResourceList("200m", "1Gi", ""), memoryWorkingSet: "400Mi"},
		{name: "best-effort-high-priority-high-usage", priority: highPriority, requests: newResourceList("", "", ""), limits: newResourceList("", "", ""), memoryWorkingSet: "400Mi"},
		{name: "best-effort-low-priority-low-usage", priority: lowPriority, requests: newResourceList("", "", ""), limits: newResourceList("", "", ""), memoryWorkingSet: "100Mi"},
	}
	pods := []*v1.Pod{}
	podStats := map[*v1.Pod]statsapi.PodStats{}
	for _, podToMake := range podsToMake {
		pod, podStat := podMaker(podToMake.name, podToMake.priority, podToMake.requests, podToMake.limits, podToMake.memoryWorkingSet)
		pods = append(pods, pod)
		podStats[pod] = podStat
	}
	podToEvict := pods[4]
	activePodsFunc := func() []*v1.Pod {
		return pods
	}

	fakeClock := testingclock.NewFakeClock(time.Now())
	podKiller := &mockPodKiller{}
	diskInfoProvider := &mockDiskInfoProvider{dedicatedImageFs: ptr.To(false)}
	diskGC := &mockDiskGC{err: nil}
	nodeRef := &v1.ObjectReference{Kind: "Node", Name: "test", UID: types.UID("test"), Namespace: ""}

	config := Config{
		MaxPodGracePeriodSeconds: 5,
		PressureTransitionPeriod: time.Minute * 5,
		Thresholds: []evictionapi.Threshold{
			{
				Signal:   evictionapi.SignalMemoryAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("1Gi"),
				},
			},
			{
				Signal:   evictionapi.SignalMemoryAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("2Gi"),
				},
				GracePeriod: time.Minute * 2,
			},
		},
	}
	summaryProvider := &fakeSummaryProvider{result: summaryStatsMaker("2Gi", podStats)}
	manager := &managerImpl{
		clock:                        fakeClock,
		killPodFunc:                  podKiller.killPodNow,
		imageGC:                      diskGC,
		containerGC:                  diskGC,
		config:                       config,
		recorder:                     &record.FakeRecorder{},
		summaryProvider:              summaryProvider,
		nodeRef:                      nodeRef,
		nodeConditionsLastObservedAt: nodeConditionsObservedAt{},
		thresholdsFirstObservedAt:    thresholdsObservedAt{},
	}

	// 创建一个 best-effort Pod 用于测试准入
	bestEffortPodToAdmit, _ := podMaker("best-admit", defaultPriority, newResourceList("", "", ""), newResourceList("", "", ""), "0Gi")
	burstablePodToAdmit, _ := podMaker("burst-admit", defaultPriority, newResourceList("100m", "100Mi", ""), newResourceList("200m", "200Mi", ""), "0Gi")

	// 同步
	_, err := manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

	if err != nil {
		t.Fatalf("Manager expects no error but got %v", err)
	}

	// 此时不应有内存压力
	if manager.IsUnderMemoryPressure() {
		t.Errorf("Manager should not report memory pressure")
	}

	// 尝试准入两个 Pod（都应成功）
	expected := []bool{true, true}
	for i, pod := range []*v1.Pod{bestEffortPodToAdmit, burstablePodToAdmit} {
		if result := manager.Admit(tCtx, &lifecycle.PodAdmitAttributes{Pod: pod}); expected[i] != result.Admit {
			t.Errorf("Admit pod: %v, expected: %v, actual: %v", pod, expected[i], result.Admit)
		}
	}

	// 触发软阈值
	fakeClock.Step(1 * time.Minute)
	summaryProvider.result = summaryStatsMaker("1500Mi", podStats)
	_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

	if err != nil {
		t.Fatalf("Manager expects no error but got %v", err)
	}

	// 此时应有内存压力
	if !manager.IsUnderMemoryPressure() {
		t.Errorf("Manager should report memory pressure since soft threshold was met")
	}

	// 时间尚未超过宽限期，验证尚无 Pod 被驱逐
	if podKiller.pod != nil {
		t.Errorf("Manager should not have killed a pod yet, but killed: %v", podKiller.pod.Name)
	}

	// 将时钟拨过宽限期
	fakeClock.Step(3 * time.Minute)
	summaryProvider.result = summaryStatsMaker("1500Mi", podStats)
	_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

	if err != nil {
		t.Fatalf("Manager expects no error but got %v", err)
	}

	// 此时应有内存压力
	if !manager.IsUnderMemoryPressure() {
		t.Errorf("Manager should report memory pressure since soft threshold was met")
	}

	// 验证被驱逐的 Pod 与宽限期覆盖值均正确
	if podKiller.pod != podToEvict {
		t.Errorf("Manager chose to kill pod: %v, but should have chosen %v", podKiller.pod.Name, podToEvict.Name)
	}
	if podKiller.gracePeriodOverride == nil {
		t.Errorf("Manager chose to kill pod but should have had a grace period override.")
	}
	observedGracePeriod := *podKiller.gracePeriodOverride
	if observedGracePeriod != manager.config.MaxPodGracePeriodSeconds {
		t.Errorf("Manager chose to kill pod with incorrect grace period.  Expected: %d, actual: %d", manager.config.MaxPodGracePeriodSeconds, observedGracePeriod)
	}
	// 重置状态
	podKiller.pod = nil
	podKiller.gracePeriodOverride = nil

	// 消除内存压力
	fakeClock.Step(20 * time.Minute)
	summaryProvider.result = summaryStatsMaker("3Gi", podStats)
	_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

	if err != nil {
		t.Fatalf("Manager expects no error but got %v", err)
	}

	// 此时不应有内存压力
	if manager.IsUnderMemoryPressure() {
		t.Errorf("Manager should not report memory pressure")
	}

	// 诱发内存压力！
	fakeClock.Step(1 * time.Minute)
	summaryProvider.result = summaryStatsMaker("500Mi", podStats)
	_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

	if err != nil {
		t.Fatalf("Manager expects no error but got %v", err)
	}

	// 此时应有内存压力
	if !manager.IsUnderMemoryPressure() {
		t.Errorf("Manager should report memory pressure")
	}

	// 验证被驱逐的是正确的 Pod
	if podKiller.pod != podToEvict {
		t.Errorf("Manager chose to kill pod: %v, but should have chosen %v", podKiller.pod.Name, podToEvict.Name)
	}
	observedGracePeriod = *podKiller.gracePeriodOverride
	if observedGracePeriod != int64(1) {
		t.Errorf("Manager chose to kill pod with incorrect grace period.  Expected: %d, actual: %d", 1, observedGracePeriod)
	}

	// best-effort Pod 不应被准入，burstable 应被准入
	expected = []bool{false, true}
	for i, pod := range []*v1.Pod{bestEffortPodToAdmit, burstablePodToAdmit} {
		if result := manager.Admit(tCtx, &lifecycle.PodAdmitAttributes{Pod: pod}); expected[i] != result.Admit {
			t.Errorf("Admit pod: %v, expected: %v, actual: %v", pod, expected[i], result.Admit)
		}
	}

	// 降低内存压力
	fakeClock.Step(1 * time.Minute)
	summaryProvider.result = summaryStatsMaker("2Gi", podStats)
	podKiller.pod = nil // 重置状态
	_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

	if err != nil {
		t.Fatalf("Manager expects no error but got %v", err)
	}

	// 仍应有内存压力（转换期尚未结束）
	if !manager.IsUnderMemoryPressure() {
		t.Errorf("Manager should report memory pressure")
	}

	// 不应有 Pod 被驱逐
	if podKiller.pod != nil {
		t.Errorf("Manager chose to kill pod: %v when no pod should have been killed", podKiller.pod.Name)
	}

	// best-effort Pod 不应被准入，burstable 应被准入
	expected = []bool{false, true}
	for i, pod := range []*v1.Pod{bestEffortPodToAdmit, burstablePodToAdmit} {
		if result := manager.Admit(tCtx, &lifecycle.PodAdmitAttributes{Pod: pod}); expected[i] != result.Admit {
			t.Errorf("Admit pod: %v, expected: %v, actual: %v", pod, expected[i], result.Admit)
		}
	}

	// 将时钟拨过转换期，确保压力状态被清除
	fakeClock.Step(5 * time.Minute)
	summaryProvider.result = summaryStatsMaker("2Gi", podStats)
	podKiller.pod = nil // 重置状态
	_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

	if err != nil {
		t.Fatalf("Manager expects no error but got %v", err)
	}

	// 不应再有内存压力（转换期已结束）
	if manager.IsUnderMemoryPressure() {
		t.Errorf("Manager should not report memory pressure")
	}

	// 不应有 Pod 被驱逐
	if podKiller.pod != nil {
		t.Errorf("Manager chose to kill pod: %v when no pod should have been killed", podKiller.pod.Name)
	}

	// 所有 Pod 现在都应被准入
	expected = []bool{true, true}
	for i, pod := range []*v1.Pod{bestEffortPodToAdmit, burstablePodToAdmit} {
		if result := manager.Admit(tCtx, &lifecycle.PodAdmitAttributes{Pod: pod}); expected[i] != result.Admit {
			t.Errorf("Admit pod: %v, expected: %v, actual: %v", pod, expected[i], result.Admit)
		}
	}
}

// makeContainersByQOS 按 QoS 级别返回对应的容器列表：
// Guaranteed 同时设置 requests 与 limits，Burstable 仅设置 requests，BestEffort 均不设置
func makeContainersByQOS(class v1.PodQOSClass) []v1.Container {
	resource := newResourceList("100m", "1Gi", "")
	switch class {
	case v1.PodQOSGuaranteed:
		return []v1.Container{newContainer("guaranteed-container", resource, resource)}
	case v1.PodQOSBurstable:
		return []v1.Container{newContainer("burtable-container", resource, nil)}
	case v1.PodQOSBestEffort:
		fallthrough
	default:
		return []v1.Container{newContainer("best-effort-container", nil, nil)}
	}
}

// TestPIDPressure 验证 PID 压力下的完整驱逐流程，与 TestMemoryPressure 结构一致：
// 软阈值宽限期、驱逐目标选择、准入控制以及转换期后压力状态的清除
func TestPIDPressure(t *testing.T) {
	testCases := []struct {
		name                               string
		podsToMake                         []podToMake
		evictPodIndex                      int
		noPressurePIDUsage                 string
		pressurePIDUsageWithGracePeriod    string
		pressurePIDUsageWithoutGracePeriod string
		totalPID                           string
	}{
		{
			name: "eviction due to pid pressure",
			podsToMake: []podToMake{
				{name: "high-priority-high-usage", priority: highPriority, pidUsage: 900},
				{name: "default-priority-low-usage", priority: defaultPriority, pidUsage: 100},
				{name: "default-priority-medium-usage", priority: defaultPriority, pidUsage: 400},
				{name: "low-priority-high-usage", priority: lowPriority, pidUsage: 600},
				{name: "low-priority-low-usage", priority: lowPriority, pidUsage: 50},
			},
			evictPodIndex:                      3, // 预期被驱逐的是 low-priority-high-usage Pod
			noPressurePIDUsage:                 "300",
			pressurePIDUsageWithGracePeriod:    "700",
			pressurePIDUsageWithoutGracePeriod: "1200",
			totalPID:                           "2000",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			podMaker := makePodWithPIDStats
			summaryStatsMaker := makePIDStats
			pods := []*v1.Pod{}
			podStats := map[*v1.Pod]statsapi.PodStats{}
			for _, podToMake := range tc.podsToMake {
				pod, podStat := podMaker(podToMake.name, podToMake.priority, podToMake.pidUsage)
				pods = append(pods, pod)
				podStats[pod] = podStat
			}
			podToEvict := pods[tc.evictPodIndex]
			activePodsFunc := func() []*v1.Pod { return pods }

			fakeClock := testingclock.NewFakeClock(time.Now())
			podKiller := &mockPodKiller{}
			diskInfoProvider := &mockDiskInfoProvider{dedicatedImageFs: ptr.To(false)}
			diskGC := &mockDiskGC{err: nil}
			nodeRef := &v1.ObjectReference{Kind: "Node", Name: "test", UID: types.UID("test"), Namespace: ""}

			config := Config{
				MaxPodGracePeriodSeconds: 5,
				PressureTransitionPeriod: time.Minute * 5,
				Thresholds: []evictionapi.Threshold{
					{
						Signal:   evictionapi.SignalPIDAvailable,
						Operator: evictionapi.OpLessThan,
						Value: evictionapi.ThresholdValue{
							Quantity: quantityMustParse("1200"),
						},
					},
					{
						Signal:   evictionapi.SignalPIDAvailable,
						Operator: evictionapi.OpLessThan,
						Value: evictionapi.ThresholdValue{
							Quantity: quantityMustParse("1500"),
						},
						GracePeriod: time.Minute * 2,
					},
				},
			}

			summaryProvider := &fakeSummaryProvider{result: summaryStatsMaker(tc.totalPID, tc.noPressurePIDUsage, podStats)}
			manager := &managerImpl{
				clock:                        fakeClock,
				killPodFunc:                  podKiller.killPodNow,
				imageGC:                      diskGC,
				containerGC:                  diskGC,
				config:                       config,
				recorder:                     &record.FakeRecorder{},
				summaryProvider:              summaryProvider,
				nodeRef:                      nodeRef,
				nodeConditionsLastObservedAt: nodeConditionsObservedAt{},
				thresholdsFirstObservedAt:    thresholdsObservedAt{},
			}

			// 创建一个 Pod 用于测试准入
			podToAdmit, _ := podMaker("pod-to-admit", defaultPriority, 50)

			// 同步
			_, err := manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

			if err != nil {
				t.Fatalf("Manager expects no error but got %v", err)
			}

			// 此时不应有 PID 压力
			if manager.IsUnderPIDPressure() {
				t.Fatalf("Manager should not report PID pressure")
			}

			// 尝试准入该 Pod（应成功）
			if result := manager.Admit(tCtx, &lifecycle.PodAdmitAttributes{Pod: podToAdmit}); !result.Admit {
				t.Fatalf("Admit pod: %v, expected: %v, actual: %v", podToAdmit, true, result.Admit)
			}

			// 触发 PID 软阈值
			fakeClock.Step(1 * time.Minute)
			summaryProvider.result = summaryStatsMaker(tc.totalPID, tc.pressurePIDUsageWithGracePeriod, podStats)
			_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

			if err != nil {
				t.Fatalf("Manager expects no error but got %v", err)
			}

			// 现在应有 PID 压力
			if !manager.IsUnderPIDPressure() {
				t.Errorf("Manager should report PID pressure since soft threshold was met")
			}

			// 时间尚未超过宽限期，验证尚无 Pod 被驱逐
			if podKiller.pod != nil {
				t.Errorf("Manager should not have killed a pod yet, but killed: %v", podKiller.pod.Name)
			}

			// 将时钟拨过宽限期
			fakeClock.Step(3 * time.Minute)
			// PID 统计保持不变，模拟压力持续存在
			_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

			if err != nil {
				t.Fatalf("Manager expects no error but got %v", err)
			}

			// 验证仍报告 PID 压力
			if !manager.IsUnderPIDPressure() {
				t.Errorf("Manager should still report PID pressure")
			}

			// 验证被驱逐的 Pod 与宽限期覆盖值均正确
			if podKiller.pod != podToEvict {
				t.Errorf("Manager chose to kill pod: %v, but should have chosen %v", podKiller.pod.Name, podToEvict.Name)
			}
			if podKiller.gracePeriodOverride == nil {
				t.Errorf("Manager chose to kill pod but should have had a grace period override.")
			}
			observedGracePeriod := *podKiller.gracePeriodOverride
			if observedGracePeriod != manager.config.MaxPodGracePeriodSeconds {
				t.Errorf("Manager chose to kill pod with incorrect grace period.  Expected: %d, actual: %d", manager.config.MaxPodGracePeriodSeconds, observedGracePeriod)
			}

			// 重置状态
			podKiller.pod = nil
			podKiller.gracePeriodOverride = nil

			// 模拟可用 PID 数量回升，消除 PID 压力
			fakeClock.Step(20 * time.Minute)
			summaryProvider.result = summaryStatsMaker(tc.totalPID, tc.noPressurePIDUsage, podStats) // 模拟可用 PID 数量回升
			_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

			if err != nil {
				t.Fatalf("Manager expects no error but got %v", err)
			}

			// 验证 PID 压力已解除
			if manager.IsUnderPIDPressure() {
				t.Errorf("Manager should not report PID pressure")
			}

			// 再次诱发 PID 压力
			fakeClock.Step(1 * time.Minute)
			summaryProvider.result = summaryStatsMaker(tc.totalPID, tc.pressurePIDUsageWithoutGracePeriod, podStats)
			_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

			if err != nil {
				t.Fatalf("Manager expects no error but got %v", err)
			}

			// 验证再次报告 PID 压力
			if !manager.IsUnderPIDPressure() {
				t.Errorf("Manager should report PID pressure")
			}

			// 验证被驱逐的 Pod 与宽限期覆盖值均正确
			if podKiller.pod != podToEvict {
				t.Errorf("Manager chose to kill pod: %v, but should have chosen %v", podKiller.pod.Name, podToEvict.Name)
			}
			if podKiller.gracePeriodOverride == nil {
				t.Errorf("Manager chose to kill pod but should have had a grace period override.")
			}
			observedGracePeriod = *podKiller.gracePeriodOverride
			if observedGracePeriod != int64(1) {
				t.Errorf("Manager chose to kill pod with incorrect grace period.  Expected: %d, actual: %d", 1, observedGracePeriod)
			}

			// 尝试准入该 Pod（应失败）
			if result := manager.Admit(tCtx, &lifecycle.PodAdmitAttributes{Pod: podToAdmit}); result.Admit {
				t.Fatalf("Admit pod: %v, expected: %v, actual: %v", podToAdmit, false, result.Admit)
			}

			// 降低 PID 压力
			fakeClock.Step(1 * time.Minute)
			summaryProvider.result = summaryStatsMaker(tc.totalPID, tc.noPressurePIDUsage, podStats)
			podKiller.pod = nil // 重置状态
			_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

			if err != nil {
				t.Fatalf("Manager expects no error but got %v", err)
			}

			// 仍应有 PID 压力（转换期尚未结束）
			if !manager.IsUnderPIDPressure() {
				t.Errorf("Manager should report PID pressure")
			}

			// 不应有 Pod 被驱逐
			if podKiller.pod != nil {
				t.Errorf("Manager chose to kill pod: %v when no pod should have been killed", podKiller.pod.Name)
			}

			// 尝试准入该 Pod（应失败）
			if result := manager.Admit(tCtx, &lifecycle.PodAdmitAttributes{Pod: podToAdmit}); result.Admit {
				t.Fatalf("Admit pod: %v, expected: %v, actual: %v", podToAdmit, false, result.Admit)
			}

			// 将时钟拨过转换期
			fakeClock.Step(5 * time.Minute)
			summaryProvider.result = summaryStatsMaker(tc.totalPID, tc.noPressurePIDUsage, podStats)
			_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

			if err != nil {
				t.Fatalf("Manager expects no error but got %v", err)
			}

			// 不应再有 PID 压力（转换期已结束）
			if manager.IsUnderPIDPressure() {
				t.Errorf("Manager should not report PID pressure")
			}

			// 不应有 Pod 被驱逐
			if podKiller.pod != nil {
				t.Errorf("Manager chose to kill pod: %v when no pod should have been killed", podKiller.pod.Name)
			}

			// 尝试准入该 Pod（应成功）
			if result := manager.Admit(tCtx, &lifecycle.PodAdmitAttributes{Pod: podToAdmit}); !result.Admit {
				t.Fatalf("Admit pod: %v, expected: %v, actual: %v", podToAdmit, true, result.Admit)
			}
		})
	}
}

// TestAdmitUnderNodeConditions 验证不同节点压力条件下各 QoS 级别 Pod 的准入结果：
// 内存压力下仅拒绝 best-effort，磁盘压力下拒绝所有 Pod
func TestAdmitUnderNodeConditions(t *testing.T) {
	tCtx := ktesting.Init(t)
	manager := &managerImpl{}
	pods := []*v1.Pod{
		newPod("guaranteed-pod", scheduling.DefaultPriorityWhenNoDefaultClassExists, makeContainersByQOS(v1.PodQOSGuaranteed), nil),
		newPod("burstable-pod", scheduling.DefaultPriorityWhenNoDefaultClassExists, makeContainersByQOS(v1.PodQOSBurstable), nil),
		newPod("best-effort-pod", scheduling.DefaultPriorityWhenNoDefaultClassExists, makeContainersByQOS(v1.PodQOSBestEffort), nil),
	}

	expected := []bool{true, true, true}
	for i, pod := range pods {
		if result := manager.Admit(tCtx, &lifecycle.PodAdmitAttributes{Pod: pod}); expected[i] != result.Admit {
			t.Errorf("Admit pod: %v, expected: %v, actual: %v", pod, expected[i], result.Admit)
		}
	}

	manager.nodeConditions = []v1.NodeConditionType{v1.NodeMemoryPressure}
	expected = []bool{true, true, false}
	for i, pod := range pods {
		if result := manager.Admit(tCtx, &lifecycle.PodAdmitAttributes{Pod: pod}); expected[i] != result.Admit {
			t.Errorf("Admit pod: %v, expected: %v, actual: %v", pod, expected[i], result.Admit)
		}
	}

	manager.nodeConditions = []v1.NodeConditionType{v1.NodeMemoryPressure, v1.NodeDiskPressure}
	expected = []bool{false, false, false}
	for i, pod := range pods {
		if result := manager.Admit(tCtx, &lifecycle.PodAdmitAttributes{Pod: pod}); expected[i] != result.Admit {
			t.Errorf("Admit pod: %v, expected: %v, actual: %v", pod, expected[i], result.Admit)
		}
	}
}

// parseQuantity 解析给定的资源量字符串；若为空则返回 0 值
func parseQuantity(value string) resource.Quantity {
	if len(value) == 0 {
		return resource.MustParse("0")
	}
	return resource.MustParse(value)
}

// TestDiskPressureNodeFs 验证磁盘压力下的完整驱逐与准入流程，
// 覆盖 nodefs、imagefs 与 containerfs 三种压力来源的软/硬阈值场景
func TestDiskPressureNodeFs(t *testing.T) {

	testCases := map[string]struct {
		nodeFsStats                   string
		imageFsStats                  string
		containerFsStats              string
		kubeletSeparateDiskFeature    bool
		writeableSeparateFromReadOnly bool
		thresholdToMonitor            []evictionapi.Threshold
		podToMakes                    []podToMake
		dedicatedImageFs              *bool
		expectErr                     string
		inducePressureOnWhichFs       string
		softDiskPressure              string
		hardDiskPressure              string
	}{
		"eviction due to disk pressure; no image fs": {
			dedicatedImageFs:        ptr.To(false),
			nodeFsStats:             "16Gi",
			imageFsStats:            "16Gi",
			containerFsStats:        "16Gi",
			inducePressureOnWhichFs: "nodefs",
			softDiskPressure:        "1.5Gi",
			hardDiskPressure:        "750Mi",
			thresholdToMonitor: []evictionapi.Threshold{
				{
					Signal:   evictionapi.SignalNodeFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalNodeFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("2Gi"),
					},
					GracePeriod: time.Minute * 2,
				},
			},
			podToMakes: []podToMake{
				{name: "low-priority-high-usage", priority: lowPriority, requests: newResourceList("100m", "1Gi", ""), limits: newResourceList("100m", "1Gi", ""), rootFsUsed: "900Mi"},
				{name: "below-requests", priority: defaultPriority, requests: newResourceList("100m", "100Mi", ""), limits: newResourceList("200m", "1Gi", ""), logsFsUsed: "50Mi"},
				{name: "above-requests", priority: defaultPriority, requests: newResourceList("100m", "100Mi", ""), limits: newResourceList("200m", "1Gi", ""), rootFsUsed: "400Mi"},
				{name: "high-priority-high-usage", priority: highPriority, requests: newResourceList("", "", ""), limits: newResourceList("", "", ""), perLocalVolumeUsed: "400Mi"},
				{name: "low-priority-low-usage", priority: lowPriority, requests: newResourceList("", "", ""), limits: newResourceList("", "", ""), rootFsUsed: "100Mi"},
			},
		},
		"eviction due to image disk pressure; image fs": {
			dedicatedImageFs:        ptr.To(true),
			nodeFsStats:             "16Gi",
			imageFsStats:            "16Gi",
			containerFsStats:        "16Gi",
			softDiskPressure:        "1.5Gi",
			hardDiskPressure:        "750Mi",
			inducePressureOnWhichFs: "imagefs",
			thresholdToMonitor: []evictionapi.Threshold{
				{
					Signal:   evictionapi.SignalImageFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalImageFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("2Gi"),
					},
					GracePeriod: time.Minute * 2,
				},
			},
			podToMakes: []podToMake{
				{name: "low-priority-high-usage", priority: lowPriority, requests: newResourceList("100m", "1Gi", ""), limits: newResourceList("100m", "1Gi", ""), rootFsUsed: "900Mi"},
				{name: "below-requests", priority: defaultPriority, requests: newResourceList("100m", "100Mi", ""), limits: newResourceList("200m", "1Gi", ""), logsFsUsed: "50Mi"},
				{name: "above-requests", priority: defaultPriority, requests: newResourceList("100m", "100Mi", ""), limits: newResourceList("200m", "1Gi", ""), rootFsUsed: "400Mi"},
				{name: "high-priority-high-usage", priority: highPriority, requests: newResourceList("", "", ""), limits: newResourceList("", "", ""), perLocalVolumeUsed: "400Mi"},
				{name: "low-priority-low-usage", priority: lowPriority, requests: newResourceList("", "", ""), limits: newResourceList("", "", ""), rootFsUsed: "100Mi"},
			},
		},
		"eviction due to container disk pressure; container fs": {
			dedicatedImageFs:              ptr.To(true),
			kubeletSeparateDiskFeature:    true,
			writeableSeparateFromReadOnly: true,
			nodeFsStats:                   "16Gi",
			imageFsStats:                  "16Gi",
			containerFsStats:              "16Gi",
			softDiskPressure:              "1.5Gi",
			hardDiskPressure:              "750Mi",
			inducePressureOnWhichFs:       "containerfs",
			thresholdToMonitor: []evictionapi.Threshold{
				{
					Signal:   evictionapi.SignalNodeFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Gi"),
					},
				},
				{
					Signal:   evictionapi.SignalNodeFsAvailable,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("2Gi"),
					},
					GracePeriod: time.Minute * 2,
				},
			},
			podToMakes: []podToMake{
				{name: "low-priority-high-usage", priority: lowPriority, requests: newResourceList("100m", "1Gi", ""), limits: newResourceList("100m", "1Gi", ""), rootFsUsed: "900Mi"},
				{name: "below-requests", priority: defaultPriority, requests: newResourceList("100m", "100Mi", ""), limits: newResourceList("200m", "1Gi", ""), logsFsUsed: "50Mi"},
				{name: "above-requests", priority: defaultPriority, requests: newResourceList("100m", "100Mi", ""), limits: newResourceList("200m", "1Gi", ""), rootFsUsed: "400Mi"},
				{name: "high-priority-high-usage", priority: highPriority, requests: newResourceList("", "", ""), limits: newResourceList("", "", ""), perLocalVolumeUsed: "400Mi"},
				{name: "low-priority-low-usage", priority: lowPriority, requests: newResourceList("", "", ""), limits: newResourceList("", "", ""), rootFsUsed: "100Mi"},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.KubeletSeparateDiskGC, tc.kubeletSeparateDiskFeature)

			podMaker := makePodWithDiskStats
			summaryStatsMaker := makeDiskStats
			podsToMake := tc.podToMakes
			pods := []*v1.Pod{}
			podStats := map[*v1.Pod]statsapi.PodStats{}
			for _, podToMake := range podsToMake {
				pod, podStat := podMaker(podToMake.name, podToMake.priority, podToMake.requests, podToMake.limits, podToMake.rootFsUsed, podToMake.logsFsUsed, podToMake.perLocalVolumeUsed, nil)
				pods = append(pods, pod)
				podStats[pod] = podStat
			}
			podToEvict := pods[0]
			activePodsFunc := func() []*v1.Pod {
				return pods
			}

			fakeClock := testingclock.NewFakeClock(time.Now())
			podKiller := &mockPodKiller{}
			diskInfoProvider := &mockDiskInfoProvider{dedicatedImageFs: tc.dedicatedImageFs, dedicatedContainerFs: &tc.writeableSeparateFromReadOnly}
			diskGC := &mockDiskGC{err: nil}
			nodeRef := &v1.ObjectReference{Kind: "Node", Name: "test", UID: types.UID("test"), Namespace: ""}

			config := Config{
				MaxPodGracePeriodSeconds: 5,
				PressureTransitionPeriod: time.Minute * 5,
				Thresholds:               tc.thresholdToMonitor,
			}

			diskStatStart := diskStats{
				rootFsAvailableBytes:      tc.nodeFsStats,
				imageFsAvailableBytes:     tc.imageFsStats,
				containerFsAvailableBytes: tc.containerFsStats,
				podStats:                  podStats,
			}
			diskStatConst := diskStatStart
			summaryProvider := &fakeSummaryProvider{result: summaryStatsMaker(diskStatStart)}
			manager := &managerImpl{
				clock:                        fakeClock,
				killPodFunc:                  podKiller.killPodNow,
				imageGC:                      diskGC,
				containerGC:                  diskGC,
				config:                       config,
				recorder:                     &record.FakeRecorder{},
				summaryProvider:              summaryProvider,
				nodeRef:                      nodeRef,
				nodeConditionsLastObservedAt: nodeConditionsObservedAt{},
				thresholdsFirstObservedAt:    thresholdsObservedAt{},
			}

			// 创建一个 best-effort Pod 用于测试准入
			podToAdmit, _ := podMaker("pod-to-admit", defaultPriority, newResourceList("", "", ""), newResourceList("", "", ""), "0Gi", "0Gi", "0Gi", nil)

			// 同步
			_, err := manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

			if err != nil {
				t.Fatalf("Manager expects no error but got %v", err)
			}

			// 此时不应有磁盘压力
			if manager.IsUnderDiskPressure() {
				t.Fatalf("Manager should not report disk pressure")
			}

			// 尝试准入该 Pod（应成功）
			if result := manager.Admit(tCtx, &lifecycle.PodAdmitAttributes{Pod: podToAdmit}); !result.Admit {
				t.Fatalf("Admit pod: %v, expected: %v, actual: %v", podToAdmit, true, result.Admit)
			}

			// 触发软阈值
			fakeClock.Step(1 * time.Minute)

			if tc.inducePressureOnWhichFs == "nodefs" {
				diskStatStart.rootFsAvailableBytes = tc.softDiskPressure
			} else if tc.inducePressureOnWhichFs == "imagefs" {
				diskStatStart.imageFsAvailableBytes = tc.softDiskPressure
			} else if tc.inducePressureOnWhichFs == "containerfs" {
				diskStatStart.containerFsAvailableBytes = tc.softDiskPressure
			}
			summaryProvider.result = summaryStatsMaker(diskStatStart)
			_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

			if err != nil {
				t.Fatalf("Manager expects no error but got %v", err)
			}

			// 此时应有磁盘压力
			if !manager.IsUnderDiskPressure() {
				t.Fatalf("Manager should report disk pressure since soft threshold was met")
			}

			// 时间尚未超过宽限期，验证尚无 Pod 被驱逐
			if podKiller.pod != nil {
				t.Fatalf("Manager should not have killed a pod yet, but killed: %v", podKiller.pod.Name)
			}

			// 将时钟拨过宽限期
			fakeClock.Step(3 * time.Minute)
			summaryProvider.result = summaryStatsMaker(diskStatStart)
			_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

			if err != nil {
				t.Fatalf("Manager expects no error but got %v", err)
			}

			// 此时应有磁盘压力
			if !manager.IsUnderDiskPressure() {
				t.Fatalf("Manager should report disk pressure since soft threshold was met")
			}

			// 验证被驱逐的 Pod 与宽限期覆盖值均正确
			if podKiller.pod != podToEvict {
				t.Fatalf("Manager chose to kill pod: %v, but should have chosen %v", podKiller.pod.Name, podToEvict.Name)
			}
			if podKiller.gracePeriodOverride == nil {
				t.Fatalf("Manager chose to kill pod but should have had a grace period override.")
			}
			observedGracePeriod := *podKiller.gracePeriodOverride
			if observedGracePeriod != manager.config.MaxPodGracePeriodSeconds {
				t.Fatalf("Manager chose to kill pod with incorrect grace period.  Expected: %d, actual: %d", manager.config.MaxPodGracePeriodSeconds, observedGracePeriod)
			}
			// 重置状态
			podKiller.pod = nil
			podKiller.gracePeriodOverride = nil

			// 消除磁盘压力
			fakeClock.Step(20 * time.Minute)
			summaryProvider.result = summaryStatsMaker(diskStatConst)
			_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

			if err != nil {
				t.Fatalf("Manager expects no error but got %v", err)
			}

			// 此时不应有磁盘压力
			if manager.IsUnderDiskPressure() {
				t.Fatalf("Manager should not report disk pressure")
			}

			// 诱发磁盘压力！
			fakeClock.Step(1 * time.Minute)
			if tc.inducePressureOnWhichFs == "nodefs" {
				diskStatStart.rootFsAvailableBytes = tc.hardDiskPressure
			} else if tc.inducePressureOnWhichFs == "imagefs" {
				diskStatStart.imageFsAvailableBytes = tc.hardDiskPressure
			} else if tc.inducePressureOnWhichFs == "containerfs" {
				diskStatStart.containerFsAvailableBytes = tc.hardDiskPressure
			}
			summaryProvider.result = summaryStatsMaker(diskStatStart)
			_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

			if err != nil {
				t.Fatalf("Manager expects no error but got %v", err)
			}

			// 此时应有磁盘压力
			if !manager.IsUnderDiskPressure() {
				t.Fatalf("Manager should report disk pressure")
			}

			// 验证被驱逐的是正确的 Pod
			if podKiller.pod != podToEvict {
				t.Fatalf("Manager chose to kill pod: %v, but should have chosen %v", podKiller.pod.Name, podToEvict.Name)
			}
			observedGracePeriod = *podKiller.gracePeriodOverride
			if observedGracePeriod != int64(1) {
				t.Fatalf("Manager chose to kill pod with incorrect grace period.  Expected: %d, actual: %d", 1, observedGracePeriod)
			}

			// 尝试准入该 Pod（应失败）
			if result := manager.Admit(tCtx, &lifecycle.PodAdmitAttributes{Pod: podToAdmit}); result.Admit {
				t.Fatalf("Admit pod: %v, expected: %v, actual: %v", podToAdmit, false, result.Admit)
			}

			// 降低磁盘压力
			fakeClock.Step(1 * time.Minute)

			summaryProvider.result = summaryStatsMaker(diskStatConst)
			podKiller.pod = nil // 重置状态
			_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

			if err != nil {
				t.Fatalf("Manager should not have an error %v", err)
			}
			// 仍应有磁盘压力（转换期尚未结束）
			if !manager.IsUnderDiskPressure() {
				t.Fatalf("Manager should report disk pressure")
			}

			// 不应有 Pod 被驱逐
			if podKiller.pod != nil {
				t.Fatalf("Manager chose to kill pod: %v when no pod should have been killed", podKiller.pod.Name)
			}

			// 尝试准入该 Pod（应失败）
			if result := manager.Admit(tCtx, &lifecycle.PodAdmitAttributes{Pod: podToAdmit}); result.Admit {
				t.Fatalf("Admit pod: %v, expected: %v, actual: %v", podToAdmit, false, result.Admit)
			}

			// 将时钟拨过转换期，确保压力状态被清除
			fakeClock.Step(5 * time.Minute)
			summaryProvider.result = summaryStatsMaker(diskStatConst)
			podKiller.pod = nil // 重置状态
			_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

			if err != nil {
				t.Fatalf("Manager should not have an error %v", err)
			}

			// 不应再有磁盘压力（转换期已结束）
			if manager.IsUnderDiskPressure() {
				t.Fatalf("Manager should not report disk pressure")
			}

			// 不应有 Pod 被驱逐
			if podKiller.pod != nil {
				t.Fatalf("Manager chose to kill pod: %v when no pod should have been killed", podKiller.pod.Name)
			}

			// 尝试准入该 Pod（应成功）
			if result := manager.Admit(tCtx, &lifecycle.PodAdmitAttributes{Pod: podToAdmit}); !result.Admit {
				t.Fatalf("Admit pod: %v, expected: %v, actual: %v", podToAdmit, true, result.Admit)
			}
		})
	}
}

// TestMinReclaim 验证 min-reclaim 的语义：
// 可用内存低于阈值时必须回收至少 min-reclaim 的量才算解除压力；
// GC 无法独立完成回收时驱逐 Pod，满足回收量后压力解除并经过转换期才清除节点条件。
func TestMinReclaim(t *testing.T) {
	tCtx := ktesting.Init(t)
	podMaker := makePodWithMemoryStats
	summaryStatsMaker := makeMemoryStats
	podsToMake := []podToMake{
		{name: "guaranteed-low-priority-high-usage", priority: lowPriority, requests: newResourceList("100m", "1Gi", ""), limits: newResourceList("100m", "1Gi", ""), memoryWorkingSet: "900Mi"},
		{name: "burstable-below-requests", priority: defaultPriority, requests: newResourceList("100m", "100Mi", ""), limits: newResourceList("200m", "1Gi", ""), memoryWorkingSet: "50Mi"},
		{name: "burstable-above-requests", priority: defaultPriority, requests: newResourceList("100m", "100Mi", ""), limits: newResourceList("200m", "1Gi", ""), memoryWorkingSet: "400Mi"},
		{name: "best-effort-high-priority-high-usage", priority: highPriority, requests: newResourceList("", "", ""), limits: newResourceList("", "", ""), memoryWorkingSet: "400Mi"},
		{name: "best-effort-low-priority-low-usage", priority: lowPriority, requests: newResourceList("", "", ""), limits: newResourceList("", "", ""), memoryWorkingSet: "100Mi"},
	}
	pods := []*v1.Pod{}
	podStats := map[*v1.Pod]statsapi.PodStats{}
	for _, podToMake := range podsToMake {
		pod, podStat := podMaker(podToMake.name, podToMake.priority, podToMake.requests, podToMake.limits, podToMake.memoryWorkingSet)
		pods = append(pods, pod)
		podStats[pod] = podStat
	}
	podToEvict := pods[4]
	activePodsFunc := func() []*v1.Pod {
		return pods
	}

	fakeClock := testingclock.NewFakeClock(time.Now())
	podKiller := &mockPodKiller{}
	diskInfoProvider := &mockDiskInfoProvider{dedicatedImageFs: ptr.To(false)}
	diskGC := &mockDiskGC{err: nil}
	nodeRef := &v1.ObjectReference{Kind: "Node", Name: "test", UID: types.UID("test"), Namespace: ""}

	config := Config{
		MaxPodGracePeriodSeconds: 5,
		PressureTransitionPeriod: time.Minute * 5,
		Thresholds: []evictionapi.Threshold{
			{
				Signal:   evictionapi.SignalMemoryAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("1Gi"),
				},
				MinReclaim: &evictionapi.ThresholdValue{
					Quantity: quantityMustParse("500Mi"),
				},
			},
		},
	}
	summaryProvider := &fakeSummaryProvider{result: summaryStatsMaker("2Gi", podStats)}
	manager := &managerImpl{
		clock:                        fakeClock,
		killPodFunc:                  podKiller.killPodNow,
		imageGC:                      diskGC,
		containerGC:                  diskGC,
		config:                       config,
		recorder:                     &record.FakeRecorder{},
		summaryProvider:              summaryProvider,
		nodeRef:                      nodeRef,
		nodeConditionsLastObservedAt: nodeConditionsObservedAt{},
		thresholdsFirstObservedAt:    thresholdsObservedAt{},
	}

	// 同步
	_, err := manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)
	if err != nil {
		t.Errorf("Manager should not report any errors")
	}
	// 此时不应有内存压力
	if manager.IsUnderMemoryPressure() {
		t.Errorf("Manager should not report memory pressure")
	}

	// 诱发内存压力！
	fakeClock.Step(1 * time.Minute)
	summaryProvider.result = summaryStatsMaker("500Mi", podStats)
	_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

	if err != nil {
		t.Fatalf("Manager should not have an error %v", err)
	}

	// 此时应有内存压力
	if !manager.IsUnderMemoryPressure() {
		t.Errorf("Manager should report memory pressure")
	}

	// 验证被驱逐的是正确的 Pod
	if podKiller.pod != podToEvict {
		t.Errorf("Manager chose to kill pod: %v, but should have chosen %v", podKiller.pod.Name, podToEvict.Name)
	}
	observedGracePeriod := *podKiller.gracePeriodOverride
	if observedGracePeriod != int64(1) {
		t.Errorf("Manager chose to kill pod with incorrect grace period.  Expected: %d, actual: %d", 1, observedGracePeriod)
	}

	// 降低内存压力，但仍不足 min-reclaim 要求的回收量
	fakeClock.Step(1 * time.Minute)
	summaryProvider.result = summaryStatsMaker("1.2Gi", podStats)
	podKiller.pod = nil // 重置状态
	_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

	if err != nil {
		t.Fatalf("Manager should not have an error %v", err)
	}

	// 仍应有内存压力（转换期尚未结束）
	if !manager.IsUnderMemoryPressure() {
		t.Errorf("Manager should report memory pressure")
	}

	// 验证被驱逐的是正确的 Pod
	if podKiller.pod != podToEvict {
		t.Errorf("Manager chose to kill pod: %v, but should have chosen %v", podKiller.pod.Name, podToEvict.Name)
	}
	observedGracePeriod = *podKiller.gracePeriodOverride
	if observedGracePeriod != int64(1) {
		t.Errorf("Manager chose to kill pod with incorrect grace period.  Expected: %d, actual: %d", 1, observedGracePeriod)
	}

	// 将可用内存恢复到满足 min-reclaim 的水平
	fakeClock.Step(1 * time.Minute)
	summaryProvider.result = summaryStatsMaker("2Gi", podStats)
	podKiller.pod = nil // 重置状态
	_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

	if err != nil {
		t.Fatalf("Manager should not have an error %v", err)
	}

	// 仍应有内存压力（转换期尚未结束）
	if !manager.IsUnderMemoryPressure() {
		t.Errorf("Manager should report memory pressure")
	}

	// 不应有 Pod 被驱逐
	if podKiller.pod != nil {
		t.Errorf("Manager chose to kill pod: %v when no pod should have been killed", podKiller.pod.Name)
	}

	// 将时钟拨过转换期，确保压力状态被清除
	fakeClock.Step(5 * time.Minute)
	summaryProvider.result = summaryStatsMaker("2Gi", podStats)
	podKiller.pod = nil // 重置状态
	_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

	if err != nil {
		t.Fatalf("Manager should not have an error %v", err)
	}

	// 不应再有内存压力（转换期已结束）
	if manager.IsUnderMemoryPressure() {
		t.Errorf("Manager should not report memory pressure")
	}

	// 不应有 Pod 被驱逐
	if podKiller.pod != nil {
		t.Errorf("Manager chose to kill pod: %v when no pod should have been killed", podKiller.pod.Name)
	}
}

// TestNodeReclaimFuncs 验证磁盘压力下节点回收函数（镜像/容器 GC）的调用时机：
// 压力可被 GC 消化时只执行 GC 而不驱逐 Pod；GC 回收量不足或未回收时再驱逐 Pod
func TestNodeReclaimFuncs(t *testing.T) {
	testCases := map[string]struct {
		nodeFsStats                   string
		imageFsStats                  string
		containerFsStats              string
		kubeletSeparateDiskFeature    bool
		writeableSeparateFromReadOnly bool
		expectContainerGcCall         bool
		expectImageGcCall             bool
		thresholdToMonitor            evictionapi.Threshold
		podToMakes                    []podToMake
		dedicatedImageFs              *bool
		expectErr                     string
		inducePressureOnWhichFs       string
		softDiskPressure              string
		hardDiskPressure              string
	}{
		"eviction due to disk pressure; no image fs": {
			dedicatedImageFs:        ptr.To(false),
			nodeFsStats:             "16Gi",
			imageFsStats:            "16Gi",
			containerFsStats:        "16Gi",
			inducePressureOnWhichFs: "nodefs",
			softDiskPressure:        "1.5Gi",
			hardDiskPressure:        "750Mi",
			expectContainerGcCall:   true,
			expectImageGcCall:       true,
			thresholdToMonitor: evictionapi.Threshold{
				Signal:   evictionapi.SignalNodeFsAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("1Gi"),
				},
				MinReclaim: &evictionapi.ThresholdValue{
					Quantity: quantityMustParse("500Mi"),
				},
			},
			podToMakes: []podToMake{
				{name: "low-priority-high-usage", priority: lowPriority, requests: newResourceList("100m", "1Gi", ""), limits: newResourceList("100m", "1Gi", ""), rootFsUsed: "900Mi"},
				{name: "below-requests", priority: defaultPriority, requests: newResourceList("100m", "100Mi", ""), limits: newResourceList("200m", "1Gi", ""), logsFsUsed: "50Mi"},
				{name: "above-requests", priority: defaultPriority, requests: newResourceList("100m", "100Mi", ""), limits: newResourceList("200m", "1Gi", ""), rootFsUsed: "400Mi"},
				{name: "high-priority-high-usage", priority: highPriority, requests: newResourceList("", "", ""), limits: newResourceList("", "", ""), perLocalVolumeUsed: "400Mi"},
				{name: "low-priority-low-usage", priority: lowPriority, requests: newResourceList("", "", ""), limits: newResourceList("", "", ""), rootFsUsed: "100Mi"},
			},
		},
		"eviction due to image disk pressure; image fs": {
			dedicatedImageFs:        ptr.To(true),
			nodeFsStats:             "16Gi",
			imageFsStats:            "16Gi",
			containerFsStats:        "16Gi",
			softDiskPressure:        "1.5Gi",
			hardDiskPressure:        "750Mi",
			inducePressureOnWhichFs: "imagefs",
			expectContainerGcCall:   true,
			expectImageGcCall:       true,
			thresholdToMonitor: evictionapi.Threshold{
				Signal:   evictionapi.SignalImageFsAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("1Gi"),
				},
				MinReclaim: &evictionapi.ThresholdValue{
					Quantity: quantityMustParse("500Mi"),
				},
			},
			podToMakes: []podToMake{
				{name: "low-priority-high-usage", priority: lowPriority, requests: newResourceList("100m", "1Gi", ""), limits: newResourceList("100m", "1Gi", ""), rootFsUsed: "900Mi"},
				{name: "below-requests", priority: defaultPriority, requests: newResourceList("100m", "100Mi", ""), limits: newResourceList("200m", "1Gi", ""), logsFsUsed: "50Mi"},
				{name: "above-requests", priority: defaultPriority, requests: newResourceList("100m", "100Mi", ""), limits: newResourceList("200m", "1Gi", ""), rootFsUsed: "400Mi"},
				{name: "high-priority-high-usage", priority: highPriority, requests: newResourceList("", "", ""), limits: newResourceList("", "", ""), perLocalVolumeUsed: "400Mi"},
				{name: "low-priority-low-usage", priority: lowPriority, requests: newResourceList("", "", ""), limits: newResourceList("", "", ""), rootFsUsed: "100Mi"},
			},
		},
		"eviction due to container disk pressure; container fs": {
			dedicatedImageFs:              ptr.To(true),
			kubeletSeparateDiskFeature:    true,
			writeableSeparateFromReadOnly: true,
			nodeFsStats:                   "16Gi",
			imageFsStats:                  "16Gi",
			containerFsStats:              "16Gi",
			softDiskPressure:              "1.5Gi",
			hardDiskPressure:              "750Mi",
			inducePressureOnWhichFs:       "nodefs",
			expectContainerGcCall:         true,
			expectImageGcCall:             false,
			thresholdToMonitor: evictionapi.Threshold{
				Signal:   evictionapi.SignalNodeFsAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("1Gi"),
				},
				MinReclaim: &evictionapi.ThresholdValue{
					Quantity: quantityMustParse("500Mi"),
				},
			},
			podToMakes: []podToMake{
				{name: "low-priority-high-usage", priority: lowPriority, requests: newResourceList("100m", "1Gi", ""), limits: newResourceList("100m", "1Gi", ""), rootFsUsed: "900Mi"},
				{name: "below-requests", priority: defaultPriority, requests: newResourceList("100m", "100Mi", ""), limits: newResourceList("200m", "1Gi", ""), logsFsUsed: "50Mi"},
				{name: "above-requests", priority: defaultPriority, requests: newResourceList("100m", "100Mi", ""), limits: newResourceList("200m", "1Gi", ""), rootFsUsed: "400Mi"},
				{name: "high-priority-high-usage", priority: highPriority, requests: newResourceList("", "", ""), limits: newResourceList("", "", ""), perLocalVolumeUsed: "400Mi"},
				{name: "low-priority-low-usage", priority: lowPriority, requests: newResourceList("", "", ""), limits: newResourceList("", "", ""), rootFsUsed: "100Mi"},
			},
		},
		"eviction due to image disk pressure; container fs": {
			dedicatedImageFs:              ptr.To(true),
			kubeletSeparateDiskFeature:    true,
			writeableSeparateFromReadOnly: true,
			nodeFsStats:                   "16Gi",
			imageFsStats:                  "16Gi",
			containerFsStats:              "16Gi",
			softDiskPressure:              "1.5Gi",
			hardDiskPressure:              "750Mi",
			inducePressureOnWhichFs:       "imagefs",
			expectContainerGcCall:         false,
			expectImageGcCall:             true,
			thresholdToMonitor: evictionapi.Threshold{
				Signal:   evictionapi.SignalImageFsAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("1Gi"),
				},
				MinReclaim: &evictionapi.ThresholdValue{
					Quantity: quantityMustParse("500Mi"),
				},
			},
			podToMakes: []podToMake{
				{name: "low-priority-high-usage", priority: lowPriority, requests: newResourceList("100m", "1Gi", ""), limits: newResourceList("100m", "1Gi", ""), rootFsUsed: "900Mi"},
				{name: "below-requests", priority: defaultPriority, requests: newResourceList("100m", "100Mi", ""), limits: newResourceList("200m", "1Gi", ""), logsFsUsed: "50Mi"},
				{name: "above-requests", priority: defaultPriority, requests: newResourceList("100m", "100Mi", ""), limits: newResourceList("200m", "1Gi", ""), rootFsUsed: "400Mi"},
				{name: "high-priority-high-usage", priority: highPriority, requests: newResourceList("", "", ""), limits: newResourceList("", "", ""), perLocalVolumeUsed: "400Mi"},
				{name: "low-priority-low-usage", priority: lowPriority, requests: newResourceList("", "", ""), limits: newResourceList("", "", ""), rootFsUsed: "100Mi"},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.KubeletSeparateDiskGC, tc.kubeletSeparateDiskFeature)

			podMaker := makePodWithDiskStats
			summaryStatsMaker := makeDiskStats
			podsToMake := tc.podToMakes
			pods := []*v1.Pod{}
			podStats := map[*v1.Pod]statsapi.PodStats{}
			for _, podToMake := range podsToMake {
				pod, podStat := podMaker(podToMake.name, podToMake.priority, podToMake.requests, podToMake.limits, podToMake.rootFsUsed, podToMake.logsFsUsed, podToMake.perLocalVolumeUsed, nil)
				pods = append(pods, pod)
				podStats[pod] = podStat
			}
			podToEvict := pods[0]
			activePodsFunc := func() []*v1.Pod {
				return pods
			}

			fakeClock := testingclock.NewFakeClock(time.Now())
			podKiller := &mockPodKiller{}
			diskInfoProvider := &mockDiskInfoProvider{dedicatedImageFs: tc.dedicatedImageFs, dedicatedContainerFs: &tc.writeableSeparateFromReadOnly}
			nodeRef := &v1.ObjectReference{Kind: "Node", Name: "test", UID: types.UID("test"), Namespace: ""}

			config := Config{
				MaxPodGracePeriodSeconds: 5,
				PressureTransitionPeriod: time.Minute * 5,
				Thresholds:               []evictionapi.Threshold{tc.thresholdToMonitor},
			}
			diskStatStart := diskStats{
				rootFsAvailableBytes:      tc.nodeFsStats,
				imageFsAvailableBytes:     tc.imageFsStats,
				containerFsAvailableBytes: tc.containerFsStats,
				podStats:                  podStats,
			}
			// 该常量用于验证磁盘压力已解除，请勿修改！
			diskStatConst := diskStatStart
			summaryProvider := &fakeSummaryProvider{result: summaryStatsMaker(diskStatStart)}
			diskGC := &mockDiskGC{fakeSummaryProvider: summaryProvider, err: nil}
			manager := &managerImpl{
				clock:                        fakeClock,
				killPodFunc:                  podKiller.killPodNow,
				imageGC:                      diskGC,
				containerGC:                  diskGC,
				config:                       config,
				recorder:                     &record.FakeRecorder{},
				summaryProvider:              summaryProvider,
				nodeRef:                      nodeRef,
				nodeConditionsLastObservedAt: nodeConditionsObservedAt{},
				thresholdsFirstObservedAt:    thresholdsObservedAt{},
			}

			// 同步
			_, err := manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

			if err != nil {
				t.Fatalf("Manager should not have an error %v", err)
			}

			// 此时不应有磁盘压力
			if manager.IsUnderDiskPressure() {
				t.Errorf("Manager should not report disk pressure")
			}

			// 触发硬阈值
			fakeClock.Step(1 * time.Minute)

			setDiskStatsBasedOnFs := func(whichFs string, diskPressure string, diskStat diskStats) diskStats {
				if tc.inducePressureOnWhichFs == "nodefs" {
					diskStat.rootFsAvailableBytes = diskPressure
				} else if tc.inducePressureOnWhichFs == "imagefs" {
					diskStat.imageFsAvailableBytes = diskPressure
				} else if tc.inducePressureOnWhichFs == "containerfs" {
					diskStat.containerFsAvailableBytes = diskPressure
				}
				return diskStat
			}
			newDiskAfterHardEviction := setDiskStatsBasedOnFs(tc.inducePressureOnWhichFs, tc.hardDiskPressure, diskStatStart)
			summaryProvider.result = summaryStatsMaker(newDiskAfterHardEviction)
			// 让 GC 成功将磁盘用量恢复到原先水平
			diskGC.summaryAfterGC = summaryStatsMaker(diskStatConst)
			_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

			if err != nil {
				t.Fatalf("Manager should not have an error %v", err)
			}

			// 此时应有磁盘压力
			if !manager.IsUnderDiskPressure() {
				t.Fatalf("Manager should report disk pressure since soft threshold was met")
			}

			// 验证镜像 GC 或容器 GC（或两者）被调用
			// 分离文件系统下可能只调用容器 GC 而不调用镜像 GC
			// 同一文件系统下两者都应在 GC 时被调用
			if diskGC.imageGCInvoked != tc.expectImageGcCall && diskGC.containerGCInvoked != tc.expectContainerGcCall {
				t.Fatalf("Manager should have invoked image gc")
			}

			// 镜像 GC 已足够回收，验证没有 Pod 被驱逐
			if podKiller.pod != nil {
				t.Fatalf("Manager should not have killed a pod, but killed: %v", podKiller.pod.Name)
			}

			// 重置状态
			diskGC.imageGCInvoked = false
			diskGC.containerGCInvoked = false

			// 消除磁盘压力
			fakeClock.Step(20 * time.Minute)
			summaryProvider.result = summaryStatsMaker(diskStatConst)
			_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

			if err != nil {
				t.Fatalf("Manager should not have an error %v", err)
			}

			// 此时不应有磁盘压力
			if manager.IsUnderDiskPressure() {
				t.Fatalf("Manager should not report disk pressure")
			}

			// 同步
			_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

			if err != nil {
				t.Fatalf("Manager should not have an error %v", err)
			}

			// 此时不应有磁盘压力
			if manager.IsUnderDiskPressure() {
				t.Fatalf("Manager should not report disk pressure")
			}

			// 触发硬阈值
			fakeClock.Step(1 * time.Minute)
			newDiskAfterHardEviction = setDiskStatsBasedOnFs(tc.inducePressureOnWhichFs, tc.hardDiskPressure, diskStatStart)
			summaryProvider.result = summaryStatsMaker(newDiskAfterHardEviction)
			// 让 GC 将磁盘用量降到阈值以下，但不满足 minReclaim 要求
			gcBelowThreshold := setDiskStatsBasedOnFs(tc.inducePressureOnWhichFs, "1.1G", newDiskAfterHardEviction)
			diskGC.summaryAfterGC = summaryStatsMaker(gcBelowThreshold)
			_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

			if err != nil {
				t.Fatalf("Manager should not have an error %v", err)
			}

			// 此时应有磁盘压力
			if !manager.IsUnderDiskPressure() {
				t.Fatalf("Manager should report disk pressure since soft threshold was met")
			}

			// 验证镜像 GC 或容器 GC（或两者）被调用
			// 分离文件系统下可能只调用容器 GC 而不调用镜像 GC
			// 同一文件系统下两者都应在 GC 时被调用
			if diskGC.imageGCInvoked != tc.expectImageGcCall && diskGC.containerGCInvoked != tc.expectContainerGcCall {
				t.Fatalf("Manager should have invoked image gc")
			}

			// 镜像 GC 回收量不足以满足 minReclaim，验证 Pod 被驱逐
			if podKiller.pod == nil {
				t.Fatalf("Manager should have killed a pod, but didn't")
			}

			// 重置状态
			diskGC.imageGCInvoked = false
			diskGC.containerGCInvoked = false
			podKiller.pod = nil

			// 消除磁盘压力
			fakeClock.Step(20 * time.Minute)
			summaryProvider.result = summaryStatsMaker(diskStatConst)
			_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

			if err != nil {
				t.Fatalf("Manager should not have an error %v", err)
			}

			// 此时不应有磁盘压力
			if manager.IsUnderDiskPressure() {
				t.Fatalf("Manager should not report disk pressure")
			}

			// 诱发磁盘压力！
			fakeClock.Step(1 * time.Minute)
			softDiskPressure := setDiskStatsBasedOnFs(tc.inducePressureOnWhichFs, tc.hardDiskPressure, diskStatStart)
			summaryProvider.result = summaryStatsMaker(softDiskPressure)
			// GC 不回收任何磁盘
			diskGC.summaryAfterGC = summaryStatsMaker(softDiskPressure)
			_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

			if err != nil {
				t.Fatalf("Manager should not have an error %v", err)
			}

			// 此时应有磁盘压力
			if !manager.IsUnderDiskPressure() {
				t.Fatalf("Manager should report disk pressure")
			}

			// 验证镜像 GC 或容器 GC（或两者）被调用
			// 分离文件系统下可能只调用容器 GC 而不调用镜像 GC
			// 同一文件系统下两者都应在 GC 时被调用
			if diskGC.imageGCInvoked != tc.expectImageGcCall && diskGC.containerGCInvoked != tc.expectContainerGcCall {
				t.Fatalf("Manager should have invoked image gc")
			}

			// 验证被驱逐的是正确的 Pod
			if podKiller.pod != podToEvict {
				t.Fatalf("Manager chose to kill pod: %v, but should have chosen %v", podKiller.pod.Name, podToEvict.Name)
			}
			observedGracePeriod := *podKiller.gracePeriodOverride
			if observedGracePeriod != int64(1) {
				t.Fatalf("Manager chose to kill pod with incorrect grace period.  Expected: %d, actual: %d", 1, observedGracePeriod)
			}

			// 降低磁盘压力
			fakeClock.Step(1 * time.Minute)
			summaryProvider.result = summaryStatsMaker(diskStatConst)
			diskGC.imageGCInvoked = false     // 重置状态
			diskGC.containerGCInvoked = false // 重置状态
			podKiller.pod = nil               // 重置状态
			_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

			if err != nil {
				t.Fatalf("Manager should not have an error %v", err)
			}

			// 仍应有磁盘压力（转换期尚未结束）
			if !manager.IsUnderDiskPressure() {
				t.Fatalf("Manager should report disk pressure")
			}

			if diskGC.imageGCInvoked || diskGC.containerGCInvoked {
				t.Errorf("Manager chose to perform image gc when it was not needed")
			}

			// 不应有 Pod 被驱逐
			if podKiller.pod != nil {
				t.Fatalf("Manager chose to kill pod: %v when no pod should have been killed", podKiller.pod.Name)
			}

			// 将时钟拨过转换期，确保压力状态被清除
			fakeClock.Step(5 * time.Minute)
			summaryProvider.result = summaryStatsMaker(diskStatConst)
			diskGC.imageGCInvoked = false     // 重置状态
			diskGC.containerGCInvoked = false // 重置状态
			podKiller.pod = nil               // 重置状态
			_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

			if err != nil {
				t.Fatalf("Manager should not have an error %v", err)
			}

			// 不应再有磁盘压力（转换期已结束）
			if manager.IsUnderDiskPressure() {
				t.Fatalf("Manager should not report disk pressure")
			}

			if diskGC.imageGCInvoked || diskGC.containerGCInvoked {
				t.Errorf("Manager chose to perform image gc when it was not needed")
			}

			// 不应有 Pod 被驱逐
			if podKiller.pod != nil {
				t.Fatalf("Manager chose to kill pod: %v when no pod should have been killed", podKiller.pod.Name)
			}
		})
	}
}

// TestInodePressureFsInodes 验证基于 inode 可用数（SignalNodeFsInodesFree / SignalImageFsInodesFree）的驱逐流程，
// 覆盖软/硬阈值、宽限期、准入控制与转换期
func TestInodePressureFsInodes(t *testing.T) {
	podMaker := func(name string, priority int32, requests v1.ResourceList, limits v1.ResourceList, rootInodes, logInodes, volumeInodes string) (*v1.Pod, statsapi.PodStats) {
		pod := newPod(name, priority, []v1.Container{
			newContainer(name, requests, limits),
		}, nil)
		podStats := newPodInodeStats(pod, parseQuantity(rootInodes), parseQuantity(logInodes), parseQuantity(volumeInodes))
		return pod, podStats
	}
	summaryStatsMaker := func(rootFsInodesFree, rootFsInodes, imageFsInodesFree, imageFsInodes, containerFsInodesFree, containerFsInodes string, podStats map[*v1.Pod]statsapi.PodStats) *statsapi.Summary {
		rootFsInodesFreeVal := resource.MustParse(rootFsInodesFree)
		internalRootFsInodesFree := uint64(rootFsInodesFreeVal.Value())
		rootFsInodesVal := resource.MustParse(rootFsInodes)
		internalRootFsInodes := uint64(rootFsInodesVal.Value())

		imageFsInodesFreeVal := resource.MustParse(imageFsInodesFree)
		internalImageFsInodesFree := uint64(imageFsInodesFreeVal.Value())
		imageFsInodesVal := resource.MustParse(imageFsInodes)
		internalImageFsInodes := uint64(imageFsInodesVal.Value())

		containerFsInodesFreeVal := resource.MustParse(containerFsInodesFree)
		internalContainerFsInodesFree := uint64(containerFsInodesFreeVal.Value())
		containerFsInodesVal := resource.MustParse(containerFsInodes)
		internalContainerFsInodes := uint64(containerFsInodesVal.Value())

		result := &statsapi.Summary{
			Node: statsapi.NodeStats{
				Fs: &statsapi.FsStats{
					InodesFree: &internalRootFsInodesFree,
					Inodes:     &internalRootFsInodes,
				},
				Runtime: &statsapi.RuntimeStats{
					ImageFs: &statsapi.FsStats{
						InodesFree: &internalImageFsInodesFree,
						Inodes:     &internalImageFsInodes,
					},
					ContainerFs: &statsapi.FsStats{
						InodesFree: &internalContainerFsInodesFree,
						Inodes:     &internalContainerFsInodes,
					},
				},
			},
			Pods: []statsapi.PodStats{},
		}
		for _, podStat := range podStats {
			result.Pods = append(result.Pods, podStat)
		}
		return result
	}

	setINodesFreeBasedOnFs := func(whichFs string, inodesFree string, diskStat *statsapi.Summary) *statsapi.Summary {
		inodesFreeVal := resource.MustParse(inodesFree)
		internalFsInodesFree := uint64(inodesFreeVal.Value())

		if whichFs == "nodefs" {
			diskStat.Node.Fs.InodesFree = &internalFsInodesFree
		} else if whichFs == "imagefs" {
			diskStat.Node.Runtime.ImageFs.InodesFree = &internalFsInodesFree
		} else if whichFs == "containerfs" {
			diskStat.Node.Runtime.ContainerFs.InodesFree = &internalFsInodesFree
		}
		return diskStat
	}

	testCases := map[string]struct {
		nodeFsInodesFree              string
		nodeFsInodes                  string
		imageFsInodesFree             string
		imageFsInodes                 string
		containerFsInodesFree         string
		containerFsInodes             string
		kubeletSeparateDiskFeature    bool
		writeableSeparateFromReadOnly bool
		thresholdToMonitor            []evictionapi.Threshold
		podToMakes                    []podToMake
		dedicatedImageFs              *bool
		expectErr                     string
		inducePressureOnWhichFs       string
		softINodePressure             string
		hardINodePressure             string
	}{
		"eviction due to disk pressure; no image fs": {
			dedicatedImageFs:        ptr.To(false),
			nodeFsInodesFree:        "3Mi",
			nodeFsInodes:            "4Mi",
			imageFsInodesFree:       "3Mi",
			imageFsInodes:           "4Mi",
			containerFsInodesFree:   "3Mi",
			containerFsInodes:       "4Mi",
			inducePressureOnWhichFs: "nodefs",
			softINodePressure:       "1.5Mi",
			hardINodePressure:       "0.5Mi",
			thresholdToMonitor: []evictionapi.Threshold{
				{
					Signal:   evictionapi.SignalNodeFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Mi"),
					},
				},
				{
					Signal:   evictionapi.SignalNodeFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("2Mi"),
					},
					GracePeriod: time.Minute * 2,
				},
			},
			podToMakes: []podToMake{
				{name: "low-priority-high-usage", priority: lowPriority, requests: newResourceList("100m", "1Gi", ""), limits: newResourceList("100m", "1Gi", ""), rootFsInodesUsed: "900Mi"},
				{name: "below-requests", priority: defaultPriority, requests: newResourceList("100m", "100Mi", ""), limits: newResourceList("200m", "1Gi", ""), rootFsInodesUsed: "50Mi"},
				{name: "above-requests", priority: defaultPriority, requests: newResourceList("100m", "100Mi", ""), limits: newResourceList("200m", "1Gi", ""), rootFsInodesUsed: "400Mi"},
				{name: "high-priority-high-usage", priority: highPriority, requests: newResourceList("", "", ""), limits: newResourceList("", "", ""), rootFsInodesUsed: "400Mi"},
				{name: "low-priority-low-usage", priority: lowPriority, requests: newResourceList("", "", ""), limits: newResourceList("", "", ""), rootFsInodesUsed: "100Mi"},
			},
		},
		"eviction due to image disk pressure; image fs": {
			dedicatedImageFs:        ptr.To(true),
			nodeFsInodesFree:        "3Mi",
			nodeFsInodes:            "4Mi",
			imageFsInodesFree:       "3Mi",
			imageFsInodes:           "4Mi",
			containerFsInodesFree:   "3Mi",
			containerFsInodes:       "4Mi",
			softINodePressure:       "1.5Mi",
			hardINodePressure:       "0.5Mi",
			inducePressureOnWhichFs: "imagefs",
			thresholdToMonitor: []evictionapi.Threshold{
				{
					Signal:   evictionapi.SignalImageFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Mi"),
					},
				},
				{
					Signal:   evictionapi.SignalImageFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("2Mi"),
					},
					GracePeriod: time.Minute * 2,
				},
			},
			podToMakes: []podToMake{
				{name: "low-priority-high-usage", priority: lowPriority, requests: newResourceList("100m", "1Gi", ""), limits: newResourceList("100m", "1Gi", ""), rootFsInodesUsed: "900Mi"},
				{name: "below-requests", priority: defaultPriority, requests: newResourceList("100m", "100Mi", ""), limits: newResourceList("200m", "1Gi", ""), rootFsInodesUsed: "50Mi"},
				{name: "above-requests", priority: defaultPriority, requests: newResourceList("100m", "100Mi", ""), limits: newResourceList("200m", "1Gi", ""), rootFsInodesUsed: "400Mi"},
				{name: "high-priority-high-usage", priority: highPriority, requests: newResourceList("", "", ""), limits: newResourceList("", "", ""), rootFsInodesUsed: "400Mi"},
				{name: "low-priority-low-usage", priority: lowPriority, requests: newResourceList("", "", ""), limits: newResourceList("", "", ""), rootFsInodesUsed: "100Mi"},
			},
		},
		"eviction due to container disk pressure; container fs": {
			dedicatedImageFs:              ptr.To(true),
			kubeletSeparateDiskFeature:    true,
			writeableSeparateFromReadOnly: true,
			nodeFsInodesFree:              "3Mi",
			nodeFsInodes:                  "4Mi",
			imageFsInodesFree:             "3Mi",
			imageFsInodes:                 "4Mi",
			containerFsInodesFree:         "3Mi",
			containerFsInodes:             "4Mi",
			softINodePressure:             "1.5Mi",
			hardINodePressure:             "0.5Mi",
			inducePressureOnWhichFs:       "nodefs",
			thresholdToMonitor: []evictionapi.Threshold{
				{
					Signal:   evictionapi.SignalNodeFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("1Mi"),
					},
				},
				{
					Signal:   evictionapi.SignalNodeFsInodesFree,
					Operator: evictionapi.OpLessThan,
					Value: evictionapi.ThresholdValue{
						Quantity: quantityMustParse("2Mi"),
					},
					GracePeriod: time.Minute * 2,
				},
			},
			podToMakes: []podToMake{
				{name: "low-priority-high-usage", priority: lowPriority, requests: newResourceList("100m", "1Gi", ""), limits: newResourceList("100m", "1Gi", ""), rootFsInodesUsed: "900Mi"},
				{name: "below-requests", priority: defaultPriority, requests: newResourceList("100m", "100Mi", ""), limits: newResourceList("200m", "1Gi", ""), rootFsInodesUsed: "50Mi"},
				{name: "above-requests", priority: defaultPriority, requests: newResourceList("100m", "100Mi", ""), limits: newResourceList("200m", "1Gi", ""), rootFsInodesUsed: "400Mi"},
				{name: "high-priority-high-usage", priority: highPriority, requests: newResourceList("", "", ""), limits: newResourceList("", "", ""), rootFsInodesUsed: "400Mi"},
				{name: "low-priority-low-usage", priority: lowPriority, requests: newResourceList("", "", ""), limits: newResourceList("", "", ""), rootFsInodesUsed: "100Mi"},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.KubeletSeparateDiskGC, tc.kubeletSeparateDiskFeature)

			podMaker := podMaker
			summaryStatsMaker := summaryStatsMaker
			podsToMake := tc.podToMakes
			pods := []*v1.Pod{}
			podStats := map[*v1.Pod]statsapi.PodStats{}
			for _, podToMake := range podsToMake {
				pod, podStat := podMaker(podToMake.name, podToMake.priority, podToMake.requests, podToMake.limits, podToMake.rootFsInodesUsed, podToMake.logsFsInodesUsed, podToMake.perLocalVolumeInodesUsed)
				pods = append(pods, pod)
				podStats[pod] = podStat
			}
			podToEvict := pods[0]
			activePodsFunc := func() []*v1.Pod {
				return pods
			}

			fakeClock := testingclock.NewFakeClock(time.Now())
			podKiller := &mockPodKiller{}
			diskInfoProvider := &mockDiskInfoProvider{dedicatedImageFs: tc.dedicatedImageFs, dedicatedContainerFs: &tc.writeableSeparateFromReadOnly}
			diskGC := &mockDiskGC{err: nil}
			nodeRef := &v1.ObjectReference{Kind: "Node", Name: "test", UID: types.UID("test"), Namespace: ""}

			config := Config{
				MaxPodGracePeriodSeconds: 5,
				PressureTransitionPeriod: time.Minute * 5,
				Thresholds:               tc.thresholdToMonitor,
			}
			startingStatsConst := summaryStatsMaker(tc.nodeFsInodesFree, tc.nodeFsInodes, tc.imageFsInodesFree, tc.imageFsInodes, tc.containerFsInodesFree, tc.containerFsInodes, podStats)
			startingStatsModified := summaryStatsMaker(tc.nodeFsInodesFree, tc.nodeFsInodes, tc.imageFsInodesFree, tc.imageFsInodes, tc.containerFsInodesFree, tc.containerFsInodes, podStats)
			summaryProvider := &fakeSummaryProvider{result: startingStatsModified}
			manager := &managerImpl{
				clock:                        fakeClock,
				killPodFunc:                  podKiller.killPodNow,
				imageGC:                      diskGC,
				containerGC:                  diskGC,
				config:                       config,
				recorder:                     &record.FakeRecorder{},
				summaryProvider:              summaryProvider,
				nodeRef:                      nodeRef,
				nodeConditionsLastObservedAt: nodeConditionsObservedAt{},
				thresholdsFirstObservedAt:    thresholdsObservedAt{},
			}

			// 创建一个 best-effort Pod 用于测试准入
			podToAdmit, _ := podMaker("pod-to-admit", defaultPriority, newResourceList("", "", ""), newResourceList("", "", ""), "0", "0", "0")

			// 同步
			_, err := manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

			if err != nil {
				t.Fatalf("Manager should not have an error %v", err)
			}

			// 此时不应有磁盘压力
			if manager.IsUnderDiskPressure() {
				t.Fatalf("Manager should not report inode pressure")
			}

			// 尝试准入该 Pod（应成功）
			if result := manager.Admit(tCtx, &lifecycle.PodAdmitAttributes{Pod: podToAdmit}); !result.Admit {
				t.Fatalf("Admit pod: %v, expected: %v, actual: %v", podToAdmit, true, result.Admit)
			}

			// 触发软阈值
			fakeClock.Step(1 * time.Minute)
			summaryProvider.result = setINodesFreeBasedOnFs(tc.inducePressureOnWhichFs, tc.softINodePressure, startingStatsModified)
			_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

			if err != nil {
				t.Fatalf("Manager should not have an error %v", err)
			}

			// 此时应有磁盘压力
			if !manager.IsUnderDiskPressure() {
				t.Fatalf("Manager should report inode pressure since soft threshold was met")
			}

			// 时间尚未超过宽限期，验证尚无 Pod 被驱逐
			if podKiller.pod != nil {
				t.Fatalf("Manager should not have killed a pod yet, but killed: %v", podKiller.pod.Name)
			}

			// 将时钟拨过宽限期
			fakeClock.Step(3 * time.Minute)
			summaryProvider.result = setINodesFreeBasedOnFs(tc.inducePressureOnWhichFs, tc.softINodePressure, startingStatsModified)
			_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

			if err != nil {
				t.Fatalf("Manager should not have an error %v", err)
			}

			// 此时应有磁盘压力
			if !manager.IsUnderDiskPressure() {
				t.Fatalf("Manager should report inode pressure since soft threshold was met")
			}

			// 验证被驱逐的 Pod 与宽限期覆盖值均正确
			if podKiller.pod != podToEvict {
				t.Fatalf("Manager chose to kill pod: %v, but should have chosen %v", podKiller.pod.Name, podToEvict.Name)
			}
			if podKiller.gracePeriodOverride == nil {
				t.Fatalf("Manager chose to kill pod but should have had a grace period override.")
			}
			observedGracePeriod := *podKiller.gracePeriodOverride
			if observedGracePeriod != manager.config.MaxPodGracePeriodSeconds {
				t.Fatalf("Manager chose to kill pod with incorrect grace period.  Expected: %d, actual: %d", manager.config.MaxPodGracePeriodSeconds, observedGracePeriod)
			}
			// 重置状态
			podKiller.pod = nil
			podKiller.gracePeriodOverride = nil

			// 消除 inode 压力
			fakeClock.Step(20 * time.Minute)
			summaryProvider.result = startingStatsConst
			_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

			if err != nil {
				t.Fatalf("Manager should not have an error %v", err)
			}

			// 此时不应有磁盘压力
			if manager.IsUnderDiskPressure() {
				t.Fatalf("Manager should not report inode pressure")
			}

			// 诱发 inode 压力！
			fakeClock.Step(1 * time.Minute)
			summaryProvider.result = setINodesFreeBasedOnFs(tc.inducePressureOnWhichFs, tc.hardINodePressure, startingStatsModified)
			_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

			if err != nil {
				t.Fatalf("Manager should not have an error %v", err)
			}

			// 此时应有磁盘压力
			if !manager.IsUnderDiskPressure() {
				t.Fatalf("Manager should report inode pressure")
			}

			// 验证被驱逐的是正确的 Pod
			if podKiller.pod != podToEvict {
				t.Fatalf("Manager chose to kill pod: %v, but should have chosen %v", podKiller.pod.Name, podToEvict.Name)
			}
			observedGracePeriod = *podKiller.gracePeriodOverride
			if observedGracePeriod != int64(1) {
				t.Fatalf("Manager chose to kill pod with incorrect grace period.  Expected: %d, actual: %d", 1, observedGracePeriod)
			}

			// 尝试准入该 Pod（应失败）
			if result := manager.Admit(tCtx, &lifecycle.PodAdmitAttributes{Pod: podToAdmit}); result.Admit {
				t.Fatalf("Admit pod: %v, expected: %v, actual: %v", podToAdmit, false, result.Admit)
			}

			// 降低 inode 压力
			fakeClock.Step(1 * time.Minute)
			summaryProvider.result = startingStatsConst
			podKiller.pod = nil // 重置状态
			_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

			if err != nil {
				t.Fatalf("Manager should not have an error %v", err)
			}

			// 仍应有磁盘压力（转换期尚未结束）
			if !manager.IsUnderDiskPressure() {
				t.Fatalf("Manager should report inode pressure")
			}

			// 不应有 Pod 被驱逐
			if podKiller.pod != nil {
				t.Fatalf("Manager chose to kill pod: %v when no pod should have been killed", podKiller.pod.Name)
			}

			// 尝试准入该 Pod（应失败）
			if result := manager.Admit(tCtx, &lifecycle.PodAdmitAttributes{Pod: podToAdmit}); result.Admit {
				t.Fatalf("Admit pod: %v, expected: %v, actual: %v", podToAdmit, false, result.Admit)
			}

			// 将时钟拨过转换期，确保压力状态被清除
			fakeClock.Step(5 * time.Minute)
			summaryProvider.result = startingStatsConst
			podKiller.pod = nil // 重置状态
			_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

			if err != nil {
				t.Fatalf("Manager should not have an error %v", err)
			}

			// 不应再有磁盘压力（转换期已结束）
			if manager.IsUnderDiskPressure() {
				t.Fatalf("Manager should not report inode pressure")
			}

			// 不应有 Pod 被驱逐
			if podKiller.pod != nil {
				t.Fatalf("Manager chose to kill pod: %v when no pod should have been killed", podKiller.pod.Name)
			}

			// 尝试准入该 Pod（应成功）
			if result := manager.Admit(tCtx, &lifecycle.PodAdmitAttributes{Pod: podToAdmit}); !result.Admit {
				t.Fatalf("Admit pod: %v, expected: %v, actual: %v", podToAdmit, true, result.Admit)
			}
		})
	}
}

// TestStaticCriticalPodsAreNotEvicted 验证静态托管的 critical Pod 不会被驱逐：
// Pod 具有 SystemCriticalPriority 且来自静态文件源时，即使内存压力持续也不被驱逐；
// 去掉 critical 标记并再次诱发压力后，该 Pod 重新成为驱逐候选。
func TestStaticCriticalPodsAreNotEvicted(t *testing.T) {
	tCtx := ktesting.Init(t)
	podMaker := makePodWithMemoryStats
	summaryStatsMaker := makeMemoryStats
	podsToMake := []podToMake{
		{name: "critical", priority: scheduling.SystemCriticalPriority, requests: newResourceList("100m", "1Gi", ""), limits: newResourceList("100m", "1Gi", ""), memoryWorkingSet: "800Mi"},
	}
	pods := []*v1.Pod{}
	podStats := map[*v1.Pod]statsapi.PodStats{}
	for _, podToMake := range podsToMake {
		pod, podStat := podMaker(podToMake.name, podToMake.priority, podToMake.requests, podToMake.limits, podToMake.memoryWorkingSet)
		pods = append(pods, pod)
		podStats[pod] = podStat
	}

	pods[0].Annotations = map[string]string{
		kubelettypes.ConfigSourceAnnotationKey: kubelettypes.FileSource,
	}
	// 将 Pod 标记为 critical
	podPriority := scheduling.SystemCriticalPriority
	pods[0].Spec.Priority = &podPriority
	pods[0].Namespace = kubeapi.NamespaceSystem

	podToEvict := pods[0]
	activePodsFunc := func() []*v1.Pod {
		return pods
	}

	fakeClock := testingclock.NewFakeClock(time.Now())
	podKiller := &mockPodKiller{}
	diskInfoProvider := &mockDiskInfoProvider{dedicatedImageFs: ptr.To(false)}
	diskGC := &mockDiskGC{err: nil}
	nodeRef := &v1.ObjectReference{
		Kind: "Node", Name: "test", UID: types.UID("test"), Namespace: "",
	}

	config := Config{
		MaxPodGracePeriodSeconds: 5,
		PressureTransitionPeriod: time.Minute * 5,
		Thresholds: []evictionapi.Threshold{
			{
				Signal:   evictionapi.SignalMemoryAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("1Gi"),
				},
			},
			{
				Signal:   evictionapi.SignalMemoryAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("2Gi"),
				},
				GracePeriod: time.Minute * 2,
			},
		},
	}
	summaryProvider := &fakeSummaryProvider{result: summaryStatsMaker("2Gi", podStats)}
	manager := &managerImpl{
		clock:                        fakeClock,
		killPodFunc:                  podKiller.killPodNow,
		imageGC:                      diskGC,
		containerGC:                  diskGC,
		config:                       config,
		recorder:                     &record.FakeRecorder{},
		summaryProvider:              summaryProvider,
		nodeRef:                      nodeRef,
		nodeConditionsLastObservedAt: nodeConditionsObservedAt{},
		thresholdsFirstObservedAt:    thresholdsObservedAt{},
	}

	fakeClock.Step(1 * time.Minute)
	summaryProvider.result = summaryStatsMaker("1500Mi", podStats)
	_, err := manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

	if err != nil {
		t.Fatalf("Manager should not have an error %v", err)
	}

	// 此时应有内存压力
	if !manager.IsUnderMemoryPressure() {
		t.Errorf("Manager should report memory pressure since soft threshold was met")
	}

	// 时间尚未超过宽限期，验证尚无 Pod 被驱逐
	if podKiller.pod != nil {
		t.Errorf("Manager should not have killed a pod yet, but killed: %v", podKiller.pod.Name)
	}

	// 将时钟拨过宽限期
	fakeClock.Step(3 * time.Minute)
	summaryProvider.result = summaryStatsMaker("1500Mi", podStats)
	_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

	if err != nil {
		t.Fatalf("Manager should not have an error %v", err)
	}

	// 此时应有内存压力
	if !manager.IsUnderMemoryPressure() {
		t.Errorf("Manager should report memory pressure since soft threshold was met")
	}

	// 验证被驱逐的 Pod 与宽限期覆盖值均正确
	if podKiller.pod == podToEvict {
		t.Errorf("Manager chose to kill critical pod: %v, but should have ignored it", podKiller.pod.Name)
	}
	// 重置状态
	podKiller.pod = nil
	podKiller.gracePeriodOverride = nil

	// 消除内存压力
	fakeClock.Step(20 * time.Minute)
	summaryProvider.result = summaryStatsMaker("3Gi", podStats)
	_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

	if err != nil {
		t.Fatalf("Manager should not have an error %v", err)
	}

	// 此时不应有内存压力
	if manager.IsUnderMemoryPressure() {
		t.Errorf("Manager should not report memory pressure")
	}

	pods[0].Annotations = map[string]string{
		kubelettypes.ConfigSourceAnnotationKey: kubelettypes.FileSource,
	}
	pods[0].Spec.Priority = nil
	pods[0].Namespace = kubeapi.NamespaceSystem

	// 诱发内存压力！
	fakeClock.Step(1 * time.Minute)
	summaryProvider.result = summaryStatsMaker("500Mi", podStats)
	_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

	if err != nil {
		t.Fatalf("Manager should not have an error %v", err)
	}

	// 此时应有内存压力
	if !manager.IsUnderMemoryPressure() {
		t.Errorf("Manager should report memory pressure")
	}
}

// TestStorageLimitEvictions 验证本地存储容量隔离开启时，
// rootfs、日志或本地卷用量超过 limit 的 Pod 会被驱逐
func TestStorageLimitEvictions(t *testing.T) {
	volumeSizeLimit := resource.MustParse("1Gi")

	testCases := map[string]struct {
		pod     podToMake
		volumes []v1.Volume
	}{
		"eviction due to rootfs above limit": {
			pod: podToMake{name: "rootfs-above-limits", priority: defaultPriority, requests: newResourceList("", "", "1Gi"), limits: newResourceList("", "", "1Gi"), rootFsUsed: "2Gi"},
		},
		"eviction due to logsfs above limit": {
			pod: podToMake{name: "logsfs-above-limits", priority: defaultPriority, requests: newResourceList("", "", "1Gi"), limits: newResourceList("", "", "1Gi"), logsFsUsed: "2Gi"},
		},
		"eviction due to local volume above limit": {
			pod: podToMake{name: "localvolume-above-limits", priority: defaultPriority, requests: newResourceList("", "", ""), limits: newResourceList("", "", ""), perLocalVolumeUsed: "2Gi"},
			volumes: []v1.Volume{{
				Name: "emptyDirVolume",
				VolumeSource: v1.VolumeSource{
					EmptyDir: &v1.EmptyDirVolumeSource{
						SizeLimit: &volumeSizeLimit,
					},
				},
			}},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			podMaker := makePodWithDiskStats
			summaryStatsMaker := makeDiskStats
			podsToMake := []podToMake{
				tc.pod,
			}
			pods := []*v1.Pod{}
			podStats := map[*v1.Pod]statsapi.PodStats{}
			for _, podToMake := range podsToMake {
				pod, podStat := podMaker(podToMake.name, podToMake.priority, podToMake.requests, podToMake.limits, podToMake.rootFsUsed, podToMake.logsFsUsed, podToMake.perLocalVolumeUsed, tc.volumes)
				pods = append(pods, pod)
				podStats[pod] = podStat
			}

			podToEvict := pods[0]
			activePodsFunc := func() []*v1.Pod {
				return pods
			}

			fakeClock := testingclock.NewFakeClock(time.Now())
			podKiller := &mockPodKiller{}
			diskInfoProvider := &mockDiskInfoProvider{dedicatedImageFs: ptr.To(false)}
			diskGC := &mockDiskGC{err: nil}
			nodeRef := &v1.ObjectReference{
				Kind: "Node", Name: "test", UID: types.UID("test"), Namespace: "",
			}

			config := Config{
				MaxPodGracePeriodSeconds: 5,
				PressureTransitionPeriod: time.Minute * 5,
				Thresholds: []evictionapi.Threshold{
					{
						Signal:   evictionapi.SignalNodeFsAvailable,
						Operator: evictionapi.OpLessThan,
						Value: evictionapi.ThresholdValue{
							Quantity: quantityMustParse("1Gi"),
						},
					},
				},
			}

			diskStat := diskStats{
				rootFsAvailableBytes:  "200Mi",
				imageFsAvailableBytes: "200Mi",
				podStats:              podStats,
			}
			summaryProvider := &fakeSummaryProvider{result: summaryStatsMaker(diskStat)}
			manager := &managerImpl{
				clock:                         fakeClock,
				killPodFunc:                   podKiller.killPodNow,
				imageGC:                       diskGC,
				containerGC:                   diskGC,
				config:                        config,
				recorder:                      &record.FakeRecorder{},
				summaryProvider:               summaryProvider,
				nodeRef:                       nodeRef,
				nodeConditionsLastObservedAt:  nodeConditionsObservedAt{},
				thresholdsFirstObservedAt:     thresholdsObservedAt{},
				localStorageCapacityIsolation: true,
			}

			_, err := manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)
			if err != nil {
				t.Fatalf("Manager expects no error but got %v", err)
			}

			if podKiller.pod == nil {
				t.Fatalf("Manager should have selected a pod for eviction")
			}
			if podKiller.pod != podToEvict {
				t.Errorf("Manager should have killed pod: %v, but instead killed: %v", podToEvict.Name, podKiller.pod.Name)
			}
			if *podKiller.gracePeriodOverride != 1 {
				t.Errorf("Manager should have evicted with gracePeriodOverride of 1, but used: %v", *podKiller.gracePeriodOverride)
			}
		})
	}
}

// TestEmptyDirLimitEviction 验证 emptyDir 卷的驱逐判定：
// Memory 介质且 InPlacePodVerticalScalingMemoryBackedVolumes 特性开启时使用统计容量（容量缺失则跳过评估），
// 其余情况回退到 spec 中的 sizeLimit
func TestEmptyDirLimitEviction(t *testing.T) {
	specLimit := resource.MustParse("1Gi") // 1Gi 是 podspec 中设置的"期望" sizeLimit

	tests := []struct {
		testName            string
		medium              v1.StorageMedium
		enableGate          bool
		actualCapacityBytes *uint64
		usedBytes           uint64
		expectEviction      bool
	}{
		{
			testName:            "Memory medium, gate disabled: falls back to spec limit, used (2Gi) > spec (1Gi) -> evict",
			medium:              v1.StorageMediumMemory,
			enableGate:          false,
			actualCapacityBytes: new(uint64(3 * 1024 * 1024 * 1024)), // 3Gi 的 stat 容量（被忽略）
			usedBytes:           2 * 1024 * 1024 * 1024,              // 使用 2Gi（超过"期望"值，但在"分配"范围内）
			expectEviction:      true,
		},
		{
			testName:            "Memory medium, gate disabled: within spec limit, used (500Mi) < spec (1Gi) -> NO evict",
			medium:              v1.StorageMediumMemory,
			enableGate:          false,
			actualCapacityBytes: new(uint64(500 * 1024 * 1024)),
			usedBytes:           500 * 1024 * 1024,
			expectEviction:      false,
		},
		{
			testName:            "Memory medium, gate enabled: uses stat capacity, identical usage (2Gi) < stat capacity (3Gi) -> NO evict",
			medium:              v1.StorageMediumMemory,
			enableGate:          true,
			actualCapacityBytes: new(uint64(3 * 1024 * 1024 * 1024)), // 3Gi 的 stat 容量（生效）
			usedBytes:           2 * 1024 * 1024 * 1024,              // 使用 2Gi（与用例 1 相同，只是开启了特性开关）
			expectEviction:      false,
		},
		{
			testName:            "Memory medium, gate enabled: missing stat capacity skips evaluation -> NO evict",
			medium:              v1.StorageMediumMemory,
			enableGate:          true,
			actualCapacityBytes: nil, // 暂时缺失的 stat
			usedBytes:           2 * 1024 * 1024 * 1024,
			expectEviction:      false,
		},

		{
			testName:            "Default medium: ignores stat capacity entirely, used (2Gi) > spec (1Gi) -> evict",
			medium:              v1.StorageMediumDefault,
			enableGate:          true,                                // 特性开关状态对默认介质卷无关紧要
			actualCapacityBytes: new(uint64(3 * 1024 * 1024 * 1024)), // 3Gi 的 stat 容量（被忽略）
			usedBytes:           2 * 1024 * 1024 * 1024,              // 使用 2Gi（超过"期望"值，但在"分配"范围内）
			expectEviction:      true,
		},
		{
			testName:            "Default medium: within spec limit, used (500Mi) < spec (1Gi) -> NO evict",
			medium:              v1.StorageMediumDefault,
			enableGate:          true,
			actualCapacityBytes: new(uint64(500 * 1024 * 1024)),
			usedBytes:           500 * 1024 * 1024,
			expectEviction:      false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.testName, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodVerticalScalingMemoryBackedVolumes, tc.enableGate)

			logger, _ := ktesting.NewTestContext(t)

			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-pod",
					UID:  types.UID("test-uid"),
				},
				Spec: v1.PodSpec{
					Volumes: []v1.Volume{
						{
							Name: "test-volume",
							VolumeSource: v1.VolumeSource{
								EmptyDir: &v1.EmptyDirVolumeSource{
									Medium:    tc.medium,
									SizeLimit: &specLimit,
								},
							},
						},
					},
				},
			}

			podStats := statsapi.PodStats{
				PodRef: statsapi.PodReference{
					Name: pod.Name, Namespace: pod.Namespace, UID: string(pod.UID),
				},
				VolumeStats: []statsapi.VolumeStats{
					{
						Name: "test-volume",
						FsStats: statsapi.FsStats{
							UsedBytes:     &tc.usedBytes,
							CapacityBytes: tc.actualCapacityBytes,
						},
					},
				},
			}

			podKiller := &mockPodKiller{}
			m := &managerImpl{
				killPodFunc: podKiller.killPodNow,
				recorder:    &record.FakeRecorder{},
			}

			evicted := m.emptyDirLimitEviction(logger, podStats, pod)

			assert.Equal(t, tc.expectEviction, evicted, "Eviction decision boundary failed")

			if tc.expectEviction {
				assert.NotNil(t, podKiller.pod, "Expected pod to be killed")
			} else {
				assert.Nil(t, podKiller.pod, "Expected pod killer to remain inactive")
			}
		})
	}
}

// TestAllocatableMemoryPressure 验证基于可分配内存（SignalAllocatableMemoryAvailable）的驱逐流程：
// 压力基于节点可分配内存而非整机内存，新增高请求 Pod 压低可分配内存后触发驱逐与准入拒绝；
// 压力解除后同样需经过转换期才恢复准入。
func TestAllocatableMemoryPressure(t *testing.T) {
	tCtx := ktesting.Init(t)
	podMaker := makePodWithMemoryStats
	summaryStatsMaker := makeMemoryStats
	podsToMake := []podToMake{
		{name: "guaranteed-low-priority-high-usage", priority: lowPriority, requests: newResourceList("100m", "1Gi", ""), limits: newResourceList("100m", "1Gi", ""), memoryWorkingSet: "900Mi"},
		{name: "burstable-below-requests", priority: defaultPriority, requests: newResourceList("100m", "100Mi", ""), limits: newResourceList("200m", "1Gi", ""), memoryWorkingSet: "50Mi"},
		{name: "burstable-above-requests", priority: defaultPriority, requests: newResourceList("100m", "100Mi", ""), limits: newResourceList("200m", "1Gi", ""), memoryWorkingSet: "400Mi"},
		{name: "best-effort-high-priority-high-usage", priority: highPriority, requests: newResourceList("", "", ""), limits: newResourceList("", "", ""), memoryWorkingSet: "400Mi"},
		{name: "best-effort-low-priority-low-usage", priority: lowPriority, requests: newResourceList("", "", ""), limits: newResourceList("", "", ""), memoryWorkingSet: "100Mi"},
	}
	pods := []*v1.Pod{}
	podStats := map[*v1.Pod]statsapi.PodStats{}
	for _, podToMake := range podsToMake {
		pod, podStat := podMaker(podToMake.name, podToMake.priority, podToMake.requests, podToMake.limits, podToMake.memoryWorkingSet)
		pods = append(pods, pod)
		podStats[pod] = podStat
	}
	podToEvict := pods[4]
	activePodsFunc := func() []*v1.Pod {
		return pods
	}

	fakeClock := testingclock.NewFakeClock(time.Now())
	podKiller := &mockPodKiller{}
	diskInfoProvider := &mockDiskInfoProvider{dedicatedImageFs: ptr.To(false)}
	diskGC := &mockDiskGC{err: nil}
	nodeRef := &v1.ObjectReference{Kind: "Node", Name: "test", UID: types.UID("test"), Namespace: ""}

	config := Config{
		MaxPodGracePeriodSeconds: 5,
		PressureTransitionPeriod: time.Minute * 5,
		Thresholds: []evictionapi.Threshold{
			{
				Signal:   evictionapi.SignalAllocatableMemoryAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("1Gi"),
				},
			},
		},
	}
	summaryProvider := &fakeSummaryProvider{result: summaryStatsMaker("4Gi", podStats)}
	manager := &managerImpl{
		clock:                        fakeClock,
		killPodFunc:                  podKiller.killPodNow,
		imageGC:                      diskGC,
		containerGC:                  diskGC,
		config:                       config,
		recorder:                     &record.FakeRecorder{},
		summaryProvider:              summaryProvider,
		nodeRef:                      nodeRef,
		nodeConditionsLastObservedAt: nodeConditionsObservedAt{},
		thresholdsFirstObservedAt:    thresholdsObservedAt{},
	}

	// 创建一个 best-effort Pod 用于测试准入
	bestEffortPodToAdmit, _ := podMaker("best-admit", defaultPriority, newResourceList("", "", ""), newResourceList("", "", ""), "0Gi")
	burstablePodToAdmit, _ := podMaker("burst-admit", defaultPriority, newResourceList("100m", "100Mi", ""), newResourceList("200m", "200Mi", ""), "0Gi")

	// 同步
	_, err := manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

	if err != nil {
		t.Fatalf("Manager should not have an error %v", err)
	}

	// 此时不应有内存压力
	if manager.IsUnderMemoryPressure() {
		t.Errorf("Manager should not report memory pressure")
	}

	// 尝试准入两个 Pod（都应成功）
	expected := []bool{true, true}
	for i, pod := range []*v1.Pod{bestEffortPodToAdmit, burstablePodToAdmit} {
		if result := manager.Admit(tCtx, &lifecycle.PodAdmitAttributes{Pod: pod}); expected[i] != result.Admit {
			t.Errorf("Admit pod: %v, expected: %v, actual: %v", pod, expected[i], result.Admit)
		}
	}

	// 诱发内存压力！
	fakeClock.Step(1 * time.Minute)
	pod, podStat := podMaker("guaranteed-high-2", defaultPriority, newResourceList("100m", "1Gi", ""), newResourceList("100m", "1Gi", ""), "1Gi")
	podStats[pod] = podStat
	summaryProvider.result = summaryStatsMaker("500Mi", podStats)
	_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

	if err != nil {
		t.Fatalf("Manager should not have an error %v", err)
	}

	// 此时应有内存压力
	if !manager.IsUnderMemoryPressure() {
		t.Errorf("Manager should report memory pressure")
	}

	// 验证被驱逐的是正确的 Pod
	if podKiller.pod != podToEvict {
		t.Errorf("Manager chose to kill pod: %v, but should have chosen %v", podKiller.pod.Name, podToEvict.Name)
	}
	observedGracePeriod := *podKiller.gracePeriodOverride
	if observedGracePeriod != int64(1) {
		t.Errorf("Manager chose to kill pod with incorrect grace period.  Expected: %d, actual: %d", 1, observedGracePeriod)
	}
	// 重置状态
	podKiller.pod = nil
	podKiller.gracePeriodOverride = nil

	// best-effort Pod 不应被准入，burstable 应被准入
	expected = []bool{false, true}
	for i, pod := range []*v1.Pod{bestEffortPodToAdmit, burstablePodToAdmit} {
		if result := manager.Admit(tCtx, &lifecycle.PodAdmitAttributes{Pod: pod}); expected[i] != result.Admit {
			t.Errorf("Admit pod: %v, expected: %v, actual: %v", pod, expected[i], result.Admit)
		}
	}

	// 降低内存压力
	fakeClock.Step(1 * time.Minute)
	for pod := range podStats {
		if pod.Name == "guaranteed-high-2" {
			delete(podStats, pod)
		}
	}
	summaryProvider.result = summaryStatsMaker("2Gi", podStats)
	podKiller.pod = nil // 重置状态
	_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

	if err != nil {
		t.Fatalf("Manager should not have an error %v", err)
	}

	// 仍应有内存压力（转换期尚未结束）
	if !manager.IsUnderMemoryPressure() {
		t.Errorf("Manager should report memory pressure")
	}

	// 不应有 Pod 被驱逐
	if podKiller.pod != nil {
		t.Errorf("Manager chose to kill pod: %v when no pod should have been killed", podKiller.pod.Name)
	}

	// best-effort Pod 不应被准入，burstable 应被准入
	expected = []bool{false, true}
	for i, pod := range []*v1.Pod{bestEffortPodToAdmit, burstablePodToAdmit} {
		if result := manager.Admit(tCtx, &lifecycle.PodAdmitAttributes{Pod: pod}); expected[i] != result.Admit {
			t.Errorf("Admit pod: %v, expected: %v, actual: %v", pod, expected[i], result.Admit)
		}
	}

	// 将时钟拨过转换期，确保压力状态被清除
	fakeClock.Step(5 * time.Minute)
	summaryProvider.result = summaryStatsMaker("2Gi", podStats)
	podKiller.pod = nil // 重置状态
	_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

	if err != nil {
		t.Fatalf("Manager should not have an error %v", err)
	}

	// 不应再有内存压力（转换期已结束）
	if manager.IsUnderMemoryPressure() {
		t.Errorf("Manager should not report memory pressure")
	}

	// 不应有 Pod 被驱逐
	if podKiller.pod != nil {
		t.Errorf("Manager chose to kill pod: %v when no pod should have been killed", podKiller.pod.Name)
	}

	// 所有 Pod 现在都应被准入
	expected = []bool{true, true}
	for i, pod := range []*v1.Pod{bestEffortPodToAdmit, burstablePodToAdmit} {
		if result := manager.Admit(tCtx, &lifecycle.PodAdmitAttributes{Pod: pod}); expected[i] != result.Admit {
			t.Errorf("Admit pod: %v, expected: %v, actual: %v", pod, expected[i], result.Admit)
		}
	}
}

// TestUpdateMemcgThreshold 验证 memcg 阈值 notifier 的刷新节奏：
// 首次同步立即更新，未到 notifierRefreshInterval 不重复更新，刷新间隔后再次更新；
// 更新失败时会调用 Description 记录错误信息
func TestUpdateMemcgThreshold(t *testing.T) {
	tCtx := ktesting.Init(t)
	activePodsFunc := func() []*v1.Pod {
		return []*v1.Pod{}
	}

	fakeClock := testingclock.NewFakeClock(time.Now())
	podKiller := &mockPodKiller{}
	diskInfoProvider := &mockDiskInfoProvider{dedicatedImageFs: ptr.To(false)}
	diskGC := &mockDiskGC{err: nil}
	nodeRef := &v1.ObjectReference{Kind: "Node", Name: "test", UID: types.UID("test"), Namespace: ""}

	config := Config{
		MaxPodGracePeriodSeconds: 5,
		PressureTransitionPeriod: time.Minute * 5,
		Thresholds: []evictionapi.Threshold{
			{
				Signal:   evictionapi.SignalMemoryAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("1Gi"),
				},
			},
		},
		PodCgroupRoot: "kubepods",
	}
	summaryProvider := &fakeSummaryProvider{result: makeMemoryStats("2Gi", map[*v1.Pod]statsapi.PodStats{})}

	thresholdNotifier := NewMockThresholdNotifier(t)
	thresholdNotifier.EXPECT().UpdateThreshold(mock.Anything, summaryProvider.result).Return(nil).Times(2)

	manager := &managerImpl{
		clock:                        fakeClock,
		killPodFunc:                  podKiller.killPodNow,
		imageGC:                      diskGC,
		containerGC:                  diskGC,
		config:                       config,
		recorder:                     &record.FakeRecorder{},
		summaryProvider:              summaryProvider,
		nodeRef:                      nodeRef,
		nodeConditionsLastObservedAt: nodeConditionsObservedAt{},
		thresholdsFirstObservedAt:    thresholdsObservedAt{},
		thresholdNotifiers:           []ThresholdNotifier{thresholdNotifier},
	}

	// 首次运行时，UpdateThreshold 应被调用一次
	_, err := manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

	if err != nil {
		t.Fatalf("Manager should not have an error %v", err)
	}

	// 距上次调用时间不足，UpdateThreshold 不应再次被调用
	_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

	if err != nil {
		t.Fatalf("Manager should not have an error %v", err)
	}

	// 距上次调用已足够久，UpdateThreshold 应再次被调用
	fakeClock.Step(2 * notifierRefreshInterval)
	_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

	if err != nil {
		t.Fatalf("Manager should not have an error %v", err)
	}

	// 换用一个更新阈值时返回错误的 notifier
	thresholdNotifier = NewMockThresholdNotifier(t)
	thresholdNotifier.EXPECT().UpdateThreshold(mock.Anything, summaryProvider.result).Return(fmt.Errorf("error updating threshold")).Times(1)
	thresholdNotifier.EXPECT().Description().Return("mock thresholdNotifier").Times(1)
	manager.thresholdNotifiers = []ThresholdNotifier{thresholdNotifier}

	// 距上次调用已超过 notifierRefreshInterval，UpdateThreshold 应被调用
	// UpdateThreshold 返回错误后，Description 应被调用（用于日志记录）
	fakeClock.Step(2 * notifierRefreshInterval)
	_, err = manager.synchronize(tCtx, diskInfoProvider, activePodsFunc)

	if err != nil {
		t.Fatalf("Manager should not have an error %v", err)
	}
}

// TestManagerWithLocalStorageCapacityIsolationOpen 验证本地存储容量隔离开启时，
// emptyDir 与临时存储超过限制的 Pod 被驱逐，且返回的被驱逐 Pod 列表正确
func TestManagerWithLocalStorageCapacityIsolationOpen(t *testing.T) {
	tCtx := ktesting.Init(t)
	podMaker := makePodWithLocalStorageCapacityIsolationOpen
	summaryStatsMaker := makeDiskStats
	podsToMake := []podToMake{
		{name: "empty-dir", requests: newResourceList("", "900Mi", ""), limits: newResourceList("", "1Gi", "")},
		{name: "container-ephemeral-storage-limit", requests: newResourceList("", "", "900Mi"), limits: newResourceList("", "", "800Mi")},
		{name: "pod-ephemeral-storage-limit", requests: newResourceList("", "", "1Gi"), limits: newResourceList("", "", "800Mi")},
	}

	pods := []*v1.Pod{}
	podStats := map[*v1.Pod]statsapi.PodStats{}
	for _, podToMake := range podsToMake {
		pod, podStat := podMaker(podToMake.name, podToMake.priority, podToMake.requests, podToMake.limits, podToMake.memoryWorkingSet)
		pods = append(pods, pod)
		podStats[pod] = podStat
	}

	diskStat := diskStats{
		rootFsAvailableBytes:  "1Gi",
		imageFsAvailableBytes: "200Mi",
		podStats:              podStats,
	}
	summaryProvider := &fakeSummaryProvider{result: summaryStatsMaker(diskStat)}

	config := Config{
		MaxPodGracePeriodSeconds: 5,
		PressureTransitionPeriod: time.Minute * 5,
		Thresholds: []evictionapi.Threshold{
			{
				Signal:   evictionapi.SignalAllocatableMemoryAvailable,
				Operator: evictionapi.OpLessThan,
				Value: evictionapi.ThresholdValue{
					Quantity: quantityMustParse("1Gi"),
				},
			},
		},
	}

	podKiller := &mockPodKiller{}
	diskGC := &mockDiskGC{err: nil}
	nodeRef := &v1.ObjectReference{Kind: "Node", Name: "test", UID: types.UID("test"), Namespace: ""}
	fakeClock := testingclock.NewFakeClock(time.Now())
	diskInfoProvider := &mockDiskInfoProvider{dedicatedImageFs: ptr.To(false)}

	mgr := &managerImpl{
		clock:                         fakeClock,
		killPodFunc:                   podKiller.killPodNow,
		imageGC:                       diskGC,
		containerGC:                   diskGC,
		config:                        config,
		recorder:                      &record.FakeRecorder{},
		summaryProvider:               summaryProvider,
		nodeRef:                       nodeRef,
		localStorageCapacityIsolation: true,
		dedicatedImageFs:              diskInfoProvider.dedicatedImageFs,
	}

	activePodsFunc := func() []*v1.Pod {
		return pods
	}

	evictedPods, err := mgr.synchronize(tCtx, diskInfoProvider, activePodsFunc)

	if err != nil {
		t.Fatalf("Manager should not have error but got %v", err)
	}
	if podKiller.pod == nil {
		t.Fatalf("Manager should have selected a pod for eviction")
	}

	if diff := cmp.Diff(pods, evictedPods); diff != "" {
		t.Fatalf("Unexpected evicted pod (-want,+got):\n%s", diff)
	}
}

// TestContainerEphemeralStorageLimitEvictionForRestartableInitContainers 验证可重启 init 容器（sidecar）
// 超过临时存储 limit 时 Pod 会被驱逐
func TestContainerEphemeralStorageLimitEvictionForRestartableInitContainers(t *testing.T) {
	tCtx := ktesting.Init(t)

	initContainer := newRestartableInitContainer("sidecar", newResourceList("", "", ""), newResourceList("", "", "10Mi"))
	mainContainer := newContainer("main", newResourceList("", "", ""), newResourceList("", "", ""))

	pod := newPod("sidecar-ephemeral-repro", 0, []v1.Container{mainContainer}, nil)
	pod.Spec.InitContainers = []v1.Container{initContainer}

	sidecarQuantity := resource.MustParse("50Mi")
	sidecarUsed := uint64(sidecarQuantity.Value())
	mainUsed := uint64(0)
	podStats := statsapi.PodStats{
		PodRef: statsapi.PodReference{
			Name: pod.Name, Namespace: pod.Namespace, UID: string(pod.UID),
		},
		Containers: []statsapi.ContainerStats{
			{
				Name:   "sidecar",
				Logs:   &statsapi.FsStats{UsedBytes: &sidecarUsed},
				Rootfs: &statsapi.FsStats{UsedBytes: &sidecarUsed},
			},
			{
				Name:   "main",
				Logs:   &statsapi.FsStats{UsedBytes: &mainUsed},
				Rootfs: &statsapi.FsStats{UsedBytes: &mainUsed},
			},
		},
	}

	diskStat := diskStats{
		rootFsAvailableBytes:  "1Gi",
		imageFsAvailableBytes: "200Mi",
		podStats:              map[*v1.Pod]statsapi.PodStats{pod: podStats},
	}
	summaryProvider := &fakeSummaryProvider{result: makeDiskStats(diskStat)}

	config := Config{
		MaxPodGracePeriodSeconds: 5,
		PressureTransitionPeriod: time.Minute * 5,
		Thresholds:               []evictionapi.Threshold{},
	}

	podKiller := &mockPodKiller{}
	nodeRef := &v1.ObjectReference{Kind: "Node", Name: "test", UID: types.UID("test"), Namespace: ""}
	fakeClock := testingclock.NewFakeClock(time.Now())

	mgr := &managerImpl{
		clock:                         fakeClock,
		killPodFunc:                   podKiller.killPodNow,
		imageGC:                       &mockDiskGC{err: nil},
		containerGC:                   &mockDiskGC{err: nil},
		config:                        config,
		recorder:                      &record.FakeRecorder{},
		summaryProvider:               summaryProvider,
		nodeRef:                       nodeRef,
		localStorageCapacityIsolation: true,
		dedicatedImageFs:              ptr.To(false),
	}

	activePodsFunc := func() []*v1.Pod {
		return []*v1.Pod{pod}
	}

	evictedPods, err := mgr.synchronize(tCtx, &mockDiskInfoProvider{dedicatedImageFs: ptr.To(false)}, activePodsFunc)
	if err != nil {
		t.Fatalf("Manager should not have error but got %v", err)
	}
	if podKiller.pod == nil {
		t.Fatalf("Manager should have evicted the pod for restartable init container exceeding ephemeral-storage limit")
	}
	if len(evictedPods) != 1 || evictedPods[0].Name != pod.Name {
		t.Fatalf("Expected evicted pod %q, got %v", pod.Name, evictedPods)
	}
}
