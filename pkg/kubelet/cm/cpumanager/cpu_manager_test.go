/*
Copyright 2017 The Kubernetes Authors.

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

package cpumanager

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/features"

	cadvisorapi "github.com/google/cadvisor/lib/model"
	"github.com/stretchr/testify/require"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/kubernetes/test/utils/ktesting/initoption"

	"k8s.io/kubernetes/pkg/kubelet/cm/containermap"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/state"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/topology"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/utils/cpuset"
)

// mockState 是 state.State 的内存实现，供测试直接操纵 CPU 分配状态。
type mockState struct {
	assignments    state.ContainerCPUAssignments
	baselines      state.ContainerCPUBaselines
	podAssignments state.PodCPUAssignments
	defaultCPUSet  cpuset.CPUSet
}

// GetBaselineCPUSet 返回准入时记录的 baseline cpuset；不存在时返回空集合与 false。
func (s *mockState) GetBaselineCPUSet(podUID string, containerName string) (cpuset.CPUSet, bool) {
	res, exists := s.baselines[podUID][containerName]
	return res.Baseline.Clone(), exists
}

// GetCPUSet 返回容器当前独占分配的 cpuset。
func (s *mockState) GetCPUSet(podUID string, containerName string) (cpuset.CPUSet, bool) {
	res, ok := s.assignments[podUID][containerName]
	return res.Clone(), ok
}

// GetDefaultCPUSet 返回未分配独占 CPU 的容器共享的默认 cpuset。
func (s *mockState) GetDefaultCPUSet() cpuset.CPUSet {
	return s.defaultCPUSet.Clone()
}

// GetCPUSetOrDefault 优先返回容器独占分配，否则回退到默认共享 cpuset。
func (s *mockState) GetCPUSetOrDefault(podUID string, containerName string) cpuset.CPUSet {
	if res, ok := s.GetCPUSet(podUID, containerName); ok {
		return res
	}
	return s.GetDefaultCPUSet()
}

// GetPodCPUSet 返回 Pod 级独占 cpuset，仅在 Pod 级资源场景使用。
func (s *mockState) GetPodCPUSet(podUID string) (cpuset.CPUSet, bool) {
	res, ok := s.podAssignments[podUID]
	return res.CPUSet.Clone(), ok
}

// SetCPUSet 记录容器的独占分配；特性门控开启时同时记录 baseline。
func (s *mockState) SetCPUSet(podUID string, containerName string, cset cpuset.CPUSet) {
	if _, exists := s.assignments[podUID]; !exists {
		s.assignments[podUID] = make(map[string]cpuset.CPUSet)
	}
	// baseline 只在首次分配时写入，模拟准入时确定的初始 cpuset；resize 不应覆盖它。
	s.assignments[podUID][containerName] = cset
	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingExclusiveCPUs) {
		if _, exists := s.baselines[podUID]; !exists {
			s.baselines[podUID] = make(map[string]state.ContainerCPUBaseline)
			s.baselines[podUID][containerName] = state.ContainerCPUBaseline{Baseline: cset.Clone()}
		} else {
			if _, exists := s.baselines[podUID][containerName]; !exists {
				s.baselines[podUID][containerName] = state.ContainerCPUBaseline{Baseline: cset.Clone()}
			}
		}
	}
}

// SetPodCPUSet 设置 pod 级独占 cpuset。
func (s *mockState) SetPodCPUSet(podUID string, cset cpuset.CPUSet) {
	// 零值构造的 mockState 没有该 map，这里惰性初始化。
	if s.podAssignments == nil {
		s.podAssignments = make(state.PodCPUAssignments)
	}

	podAssigments := s.podAssignments[podUID]
	podAssigments.CPUSet = cset
	s.podAssignments[podUID] = podAssigments
}

// SetDefaultCPUSet 直接覆盖默认共享 cpuset。
func (s *mockState) SetDefaultCPUSet(cset cpuset.CPUSet) {
	s.defaultCPUSet = cset
}

// Delete 删除容器分配与 baseline；容器清空时连带删除 pod 条目，避免残留空 map。
func (s *mockState) Delete(podUID string, containerName string) {
	delete(s.assignments[podUID], containerName)
	if len(s.assignments[podUID]) == 0 {
		delete(s.assignments, podUID)
	}
	// baseline 仅在特性门控开启时维护，与 SetCPUSet 的写入条件保持一致。
	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingExclusiveCPUs) {
		delete(s.baselines[podUID], containerName)
		if len(s.baselines[podUID]) == 0 {
			delete(s.baselines, podUID)
		}
	}
}

// ClearState 重置全部状态，用于模拟 kubelet 重启后的冷启动。
func (s *mockState) ClearState() {
	s.defaultCPUSet = cpuset.New()
	s.assignments = make(state.ContainerCPUAssignments)
	s.baselines = make(state.ContainerCPUBaselines)
	s.podAssignments = make(state.PodCPUAssignments)
}

// DeletePod 只删除 pod 级 cpuset，容器级分配由 Delete 负责。
func (s *mockState) DeletePod(podUID string) {
	delete(s.podAssignments, podUID)
}

// SetCPUAssignments 用给定分配表替换当前状态；克隆以避免测试间共享底层 map。
func (s *mockState) SetCPUAssignments(a state.ContainerCPUAssignments) {
	s.assignments = a.Clone()
}

// SetPodCPUAssignments 用给定 pod 分配表替换当前状态。
func (s *mockState) SetPodCPUAssignments(a state.PodCPUAssignments) {
	s.podAssignments = a.Clone()
}

// SetCPUBaselines 用给定 baseline 表替换当前状态。
func (s *mockState) SetCPUBaselines(a state.ContainerCPUBaselines) {
	s.baselines = a.Clone()
}

// GetCPUAssignments 返回分配表的克隆，防止调用方改动内部状态。
func (s *mockState) GetCPUAssignments() state.ContainerCPUAssignments {
	return s.assignments.Clone()
}

// GetPodCPUAssignments 返回 pod 分配表的克隆。
func (s *mockState) GetPodCPUAssignments() state.PodCPUAssignments {
	return s.podAssignments.Clone()
}

// GetCPUBaselines 返回 baseline 表的克隆。
func (s *mockState) GetCPUBaselines() state.ContainerCPUBaselines {
	return s.baselines.Clone()
}

// mockPolicy 是 Policy 的空实现，通过 err 字段统一注入失败，用于验证错误传播。
type mockPolicy struct {
	err error
}

// Name 固定返回 "mock"，便于断言。
func (p *mockPolicy) Name() string {
	return "mock"
}

// Start 直接返回注入的错误。
func (p *mockPolicy) Start(_ klog.Logger, s state.State) error {
	return p.err
}

// Allocate 返回注入的错误，模拟策略分配失败。
func (p *mockPolicy) Allocate(_ klog.Logger, s state.State, pod *v1.Pod, container *v1.Container, _ lifecycle.Operation) error {
	return p.err
}

// RemoveContainer 返回注入的错误。
func (p *mockPolicy) RemoveContainer(_ klog.Logger, s state.State, podUID string, containerName string) error {
	return p.err
}

// GetTopologyHints 不提供拓扑提示。
func (p *mockPolicy) GetTopologyHints(_ klog.Logger, s state.State, pod *v1.Pod, container *v1.Container, _ lifecycle.Operation) map[string][]topologymanager.TopologyHint {
	return nil
}

// GetPodTopologyHints 不提供 pod 级拓扑提示。
func (p *mockPolicy) GetPodTopologyHints(_ klog.Logger, s state.State, pod *v1.Pod, _ lifecycle.Operation) map[string][]topologymanager.TopologyHint {
	return nil
}

// AllocatePod 返回注入的错误。
func (p *mockPolicy) AllocatePod(_ klog.Logger, s state.State, pod *v1.Pod, _ lifecycle.Operation) error {
	return p.err
}

// GetAllocatableCPUs 返回空集合，mock 不关心可分配 CPU。
func (p *mockPolicy) GetAllocatableCPUs(m state.State) cpuset.CPUSet {
	return cpuset.New()
}

// mockRuntimeService 只实现 UpdateContainerResources，用于注入 cgroup 更新错误。
type mockRuntimeService struct {
	err error
}

// UpdateContainerResources 返回构造时注入的错误。
func (rt mockRuntimeService) UpdateContainerResources(_ context.Context, id string, resources *runtimeapi.ContainerResources) error {
	return rt.err
}

// mockPodStatusProvider 固定返回预置的 PodStatus，模拟 kubelet 的 pod 状态来源。
type mockPodStatusProvider struct {
	podStatus v1.PodStatus
	found     bool
}

// GetPodStatus 忽略 uid，始终返回预置状态与 found 标志。
func (psp mockPodStatusProvider) GetPodStatus(uid types.UID) (v1.PodStatus, bool) {
	return psp.podStatus, psp.found
}

// makePod 构造单容器 pod；内存固定 1G，使断言只关注 CPU 维度。
func makePod(podUID, containerName, cpuRequest, cpuLimit string) *v1.Pod {
	pod := &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceName(v1.ResourceCPU):    resource.MustParse(cpuRequest),
							v1.ResourceName(v1.ResourceMemory): resource.MustParse("1G"),
						},
						Limits: v1.ResourceList{
							v1.ResourceName(v1.ResourceCPU):    resource.MustParse(cpuLimit),
							v1.ResourceName(v1.ResourceMemory): resource.MustParse("1G"),
						},
					},
				},
			},
		},
	}

	pod.UID = types.UID(podUID)
	pod.Spec.Containers[0].Name = containerName

	return pod
}

// makePodWithPodLevelResources 在容器级资源之上追加 Pod 级资源，用于 Pod 级资源测试。
func makePodWithPodLevelResources(podUID, podCPURequest, podCPULimit, containerName, cpuRequest, cpuLimit string) *v1.Pod {
	pod := makePod(podUID, containerName, cpuRequest, cpuLimit)
	pod.Spec.Resources = &v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceName(v1.ResourceCPU):    resource.MustParse(podCPURequest),
			v1.ResourceName(v1.ResourceMemory): resource.MustParse("1G"),
		},
		Limits: v1.ResourceList{
			v1.ResourceName(v1.ResourceCPU):    resource.MustParse(podCPULimit),
			v1.ResourceName(v1.ResourceMemory): resource.MustParse("1G"),
		},
	}

	return pod
}

// makeMultiContainerPod 构造含多个 init 与 app 容器的 pod，切片顺序即测试中的分配顺序。
func makeMultiContainerPod(initCPUs, appCPUs []struct{ request, limit string }) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod",
			UID:  "podUID",
		},
		Spec: v1.PodSpec{
			InitContainers: []v1.Container{},
			Containers:     []v1.Container{},
		},
	}

	for i, cpu := range initCPUs {
		pod.Spec.InitContainers = append(pod.Spec.InitContainers, v1.Container{
			Name: "initContainer-" + strconv.Itoa(i),
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceCPU):    resource.MustParse(cpu.request),
					v1.ResourceName(v1.ResourceMemory): resource.MustParse("1G"),
				},
				Limits: v1.ResourceList{
					v1.ResourceName(v1.ResourceCPU):    resource.MustParse(cpu.limit),
					v1.ResourceName(v1.ResourceMemory): resource.MustParse("1G"),
				},
			},
		})
	}

	for i, cpu := range appCPUs {
		pod.Spec.Containers = append(pod.Spec.Containers, v1.Container{
			Name: "appContainer-" + strconv.Itoa(i),
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceName(v1.ResourceCPU):    resource.MustParse(cpu.request),
					v1.ResourceName(v1.ResourceMemory): resource.MustParse("1G"),
				},
				Limits: v1.ResourceList{
					v1.ResourceName(v1.ResourceCPU):    resource.MustParse(cpu.limit),
					v1.ResourceName(v1.ResourceMemory): resource.MustParse("1G"),
				},
			},
		})
	}

	return pod
}

// makeMultiContainerPodWithOptions 用 containerOptions 构造 pod，可省略资源或设置 restartPolicy。
func makeMultiContainerPodWithOptions(initCPUs, appCPUs []*containerOptions) *v1.Pod {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod",
			UID:  "podUID",
		},
		Spec: v1.PodSpec{
			InitContainers: []v1.Container{},
			Containers:     []v1.Container{},
		},
	}

	for i, opts := range initCPUs {
		container := v1.Container{
			Name: "initContainer-" + strconv.Itoa(i),
		}
		if opts.request != "" {
			container.Resources = v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse(opts.request),
					v1.ResourceMemory: resource.MustParse("1G"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse(opts.limit),
					v1.ResourceMemory: resource.MustParse("1G"),
				},
			}
		}
		if opts.restartPolicy != "" {
			container.RestartPolicy = &opts.restartPolicy
		}
		pod.Spec.InitContainers = append(pod.Spec.InitContainers, container)
	}

	for i, opts := range appCPUs {
		container := v1.Container{
			Name: "appContainer-" + strconv.Itoa(i),
		}
		if opts.request != "" {
			container.Resources = v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse(opts.request),
					v1.ResourceMemory: resource.MustParse("1G"),
				},
				Limits: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse(opts.limit),
					v1.ResourceMemory: resource.MustParse("1G"),
				},
			}
		}
		pod.Spec.Containers = append(pod.Spec.Containers, container)
	}

	return pod
}

// makeMultiContainerPodWithOptionsAndPodLevelResources 在此基础上追加 pod 级 CPU 资源。
func makeMultiContainerPodWithOptionsAndPodLevelResources(podLevelCPUs string, initCPUs, appCPUs []*containerOptions) *v1.Pod {
	pod := makeMultiContainerPodWithOptions(initCPUs, appCPUs)

	pod.Spec.Resources = &v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceName(v1.ResourceCPU):    resource.MustParse(podLevelCPUs),
			v1.ResourceName(v1.ResourceMemory): resource.MustParse("1G"),
		},
		Limits: v1.ResourceList{
			v1.ResourceName(v1.ResourceCPU):    resource.MustParse(podLevelCPUs),
			v1.ResourceName(v1.ResourceMemory): resource.MustParse("1G"),
		},
	}

	return pod
}

// TestCPUManagerAdd 验证 Allocate + AddContainer 后默认 cpuset 的扣减，以及策略错误是否被返回。
func TestCPUManagerAdd(t *testing.T) {
	if runtime.GOOS == "windows" {
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.WindowsCPUAndMemoryAffinity, true)
	}

	logger, tCtx := ktesting.NewTestContext(t)
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodVerticalScalingExclusiveCPUs, false)

	testPolicy, _ := NewStaticPolicy(
		logger,
		&topology.CPUTopology{
			NumCPUs:    4,
			NumSockets: 1,
			NumCores:   4,
			CPUDetails: map[int]topology.CPUInfo{
				0: {CoreID: 0, SocketID: 0},
				1: {CoreID: 1, SocketID: 0},
				2: {CoreID: 2, SocketID: 0},
				3: {CoreID: 3, SocketID: 0},
			},
		},
		0,
		cpuset.New(),
		topologymanager.NewFakeManager(logger),
		nil)
	testCases := []struct {
		description        string
		updateErr          error
		policy             Policy
		expCPUSet          cpuset.CPUSet
		expAllocateErr     error
		expAddContainerErr error
	}{
		{
			description:        "cpu manager add - no error",
			updateErr:          nil,
			policy:             testPolicy,
			expCPUSet:          cpuset.New(3, 4),
			expAllocateErr:     nil,
			expAddContainerErr: nil,
		},
		{
			description: "cpu manager add - policy add container error",
			updateErr:   nil,
			policy: &mockPolicy{
				err: fmt.Errorf("fake reg error"),
			},
			expCPUSet:          cpuset.New(1, 2, 3, 4),
			expAllocateErr:     fmt.Errorf("fake reg error"),
			expAddContainerErr: nil,
		},
	}

	for _, testCase := range testCases {
		// 每个用例使用独立 manager，由 mockRuntimeService 注入 cgroup 更新错误。
		mgr := &manager{
			policy:          testCase.policy,
			reconcilePeriod: 10 * time.Second,
			state: &mockState{
				assignments:   state.ContainerCPUAssignments{},
				defaultCPUSet: cpuset.New(1, 2, 3, 4),
			},
			lastUpdateState: state.NewMemoryState(logger),
			containerRuntime: mockRuntimeService{
				err: testCase.updateErr,
			},
			containerMap:      containermap.NewContainerMap(),
			podStatusProvider: mockPodStatusProvider{},
			sourcesReady:      &sourcesReadyStub{},
		}

		pod := makePod("fakePod", "fakeContainer", "2", "2")
		container := &pod.Spec.Containers[0]
		mgr.activePods = func() []*v1.Pod { return []*v1.Pod{pod} }

		err := mgr.Allocate(tCtx, pod, container, lifecycle.AddOperation)
		if !reflect.DeepEqual(err, testCase.expAllocateErr) {
			t.Errorf("CPU Manager Allocate() error (%v). expected error: %v but got: %v",
				testCase.description, testCase.expAllocateErr, err)
		}

		mgr.AddContainer(logger, pod, container, "fakeID")
		_, _, err = mgr.containerMap.GetContainerRef("fakeID")
		if !reflect.DeepEqual(err, testCase.expAddContainerErr) {
			t.Errorf("CPU Manager AddContainer() error (%v). expected error: %v but got: %v",
				testCase.description, testCase.expAddContainerErr, err)
		}
		if !testCase.expCPUSet.Equals(mgr.state.GetDefaultCPUSet()) {
			t.Errorf("CPU Manager AddContainer() error (%v). expected cpuset: %v but got: %v",
				testCase.description, testCase.expCPUSet, mgr.state.GetDefaultCPUSet())
		}
	}
}

// TestCPUManagerAddWithInitContainers 验证 init 容器与 app 容器的独占 CPU 分配及共享关系。
func TestCPUManagerAddWithInitContainers(t *testing.T) {
	if runtime.GOOS == "windows" {
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.WindowsCPUAndMemoryAffinity, true)
	}

	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodVerticalScalingExclusiveCPUs, false)
	testCases := []struct {
		description      string
		topo             *topology.CPUTopology
		numReservedCPUs  int
		initContainerIDs []string
		containerIDs     []string
		stAssignments    state.ContainerCPUAssignments
		stDefaultCPUSet  cpuset.CPUSet
		pod              *v1.Pod
		expInitCSets     []cpuset.CPUSet
		expCSets         []cpuset.CPUSet
	}{
		{
			description:      "No Guaranteed Init CPUs",
			topo:             topoSingleSocketHT,
			numReservedCPUs:  0,
			stAssignments:    state.ContainerCPUAssignments{},
			stDefaultCPUSet:  cpuset.New(0, 1, 2, 3, 4, 5, 6, 7),
			initContainerIDs: []string{"initFakeID"},
			containerIDs:     []string{"appFakeID"},
			pod: makeMultiContainerPod(
				[]struct{ request, limit string }{{"100m", "100m"}},
				[]struct{ request, limit string }{{"4000m", "4000m"}}),
			expInitCSets: []cpuset.CPUSet{
				cpuset.New()},
			expCSets: []cpuset.CPUSet{
				cpuset.New(0, 4, 1, 5)},
		},
		{
			description:      "Equal Number of Guaranteed CPUs",
			topo:             topoSingleSocketHT,
			numReservedCPUs:  0,
			stAssignments:    state.ContainerCPUAssignments{},
			stDefaultCPUSet:  cpuset.New(0, 1, 2, 3, 4, 5, 6, 7),
			initContainerIDs: []string{"initFakeID"},
			containerIDs:     []string{"appFakeID"},
			pod: makeMultiContainerPod(
				[]struct{ request, limit string }{{"4000m", "4000m"}},
				[]struct{ request, limit string }{{"4000m", "4000m"}}),
			expInitCSets: []cpuset.CPUSet{
				cpuset.New(0, 4, 1, 5)},
			expCSets: []cpuset.CPUSet{
				cpuset.New(0, 4, 1, 5)},
		},
		{
			description:      "More Init Container Guaranteed CPUs",
			topo:             topoSingleSocketHT,
			numReservedCPUs:  0,
			stAssignments:    state.ContainerCPUAssignments{},
			stDefaultCPUSet:  cpuset.New(0, 1, 2, 3, 4, 5, 6, 7),
			initContainerIDs: []string{"initFakeID"},
			containerIDs:     []string{"appFakeID"},
			pod: makeMultiContainerPod(
				[]struct{ request, limit string }{{"6000m", "6000m"}},
				[]struct{ request, limit string }{{"4000m", "4000m"}}),
			expInitCSets: []cpuset.CPUSet{
				cpuset.New(0, 4, 1, 5, 2, 6)},
			expCSets: []cpuset.CPUSet{
				cpuset.New(0, 4, 1, 5)},
		},
		{
			description:      "Less Init Container Guaranteed CPUs",
			topo:             topoSingleSocketHT,
			numReservedCPUs:  0,
			stAssignments:    state.ContainerCPUAssignments{},
			stDefaultCPUSet:  cpuset.New(0, 1, 2, 3, 4, 5, 6, 7),
			initContainerIDs: []string{"initFakeID"},
			containerIDs:     []string{"appFakeID"},
			pod: makeMultiContainerPod(
				[]struct{ request, limit string }{{"2000m", "2000m"}},
				[]struct{ request, limit string }{{"4000m", "4000m"}}),
			expInitCSets: []cpuset.CPUSet{
				cpuset.New(0, 4)},
			expCSets: []cpuset.CPUSet{
				cpuset.New(0, 4, 1, 5)},
		},
		{
			description:      "Multi Init Container Equal CPUs",
			topo:             topoSingleSocketHT,
			numReservedCPUs:  0,
			stAssignments:    state.ContainerCPUAssignments{},
			stDefaultCPUSet:  cpuset.New(0, 1, 2, 3, 4, 5, 6, 7),
			initContainerIDs: []string{"initFakeID-1", "initFakeID-2"},
			containerIDs:     []string{"appFakeID"},
			pod: makeMultiContainerPod(
				[]struct{ request, limit string }{
					{"2000m", "2000m"},
					{"2000m", "2000m"}},
				[]struct{ request, limit string }{
					{"2000m", "2000m"}}),
			expInitCSets: []cpuset.CPUSet{
				cpuset.New(0, 4),
				cpuset.New(0, 4)},
			expCSets: []cpuset.CPUSet{
				cpuset.New(0, 4)},
		},
		{
			description:      "Multi Init Container Less CPUs",
			topo:             topoSingleSocketHT,
			numReservedCPUs:  0,
			stAssignments:    state.ContainerCPUAssignments{},
			stDefaultCPUSet:  cpuset.New(0, 1, 2, 3, 4, 5, 6, 7),
			initContainerIDs: []string{"initFakeID-1", "initFakeID-2"},
			containerIDs:     []string{"appFakeID"},
			pod: makeMultiContainerPod(
				[]struct{ request, limit string }{
					{"4000m", "4000m"},
					{"4000m", "4000m"}},
				[]struct{ request, limit string }{
					{"2000m", "2000m"}}),
			expInitCSets: []cpuset.CPUSet{
				cpuset.New(0, 4, 1, 5),
				cpuset.New(0, 4, 1, 5)},
			expCSets: []cpuset.CPUSet{
				cpuset.New(0, 4)},
		},
		{
			description:      "Multi Init Container More CPUs",
			topo:             topoSingleSocketHT,
			numReservedCPUs:  0,
			stAssignments:    state.ContainerCPUAssignments{},
			stDefaultCPUSet:  cpuset.New(0, 1, 2, 3, 4, 5, 6, 7),
			initContainerIDs: []string{"initFakeID-1", "initFakeID-2"},
			containerIDs:     []string{"appFakeID"},
			pod: makeMultiContainerPod(
				[]struct{ request, limit string }{
					{"2000m", "2000m"},
					{"2000m", "2000m"}},
				[]struct{ request, limit string }{
					{"4000m", "4000m"}}),
			expInitCSets: []cpuset.CPUSet{
				cpuset.New(0, 4),
				cpuset.New(0, 4)},
			expCSets: []cpuset.CPUSet{
				cpuset.New(0, 4, 1, 5)},
		},
		{
			description:      "Multi Init Container Increasing CPUs",
			topo:             topoSingleSocketHT,
			numReservedCPUs:  0,
			stAssignments:    state.ContainerCPUAssignments{},
			stDefaultCPUSet:  cpuset.New(0, 1, 2, 3, 4, 5, 6, 7),
			initContainerIDs: []string{"initFakeID-1", "initFakeID-2"},
			containerIDs:     []string{"appFakeID"},
			pod: makeMultiContainerPod(
				[]struct{ request, limit string }{
					{"2000m", "2000m"},
					{"4000m", "4000m"}},
				[]struct{ request, limit string }{
					{"6000m", "6000m"}}),
			expInitCSets: []cpuset.CPUSet{
				cpuset.New(0, 4),
				cpuset.New(0, 4, 1, 5)},
			expCSets: []cpuset.CPUSet{
				cpuset.New(0, 4, 1, 5, 2, 6)},
		},
		{
			description:      "Multi Init, Multi App Container Split CPUs",
			topo:             topoSingleSocketHT,
			numReservedCPUs:  0,
			stAssignments:    state.ContainerCPUAssignments{},
			stDefaultCPUSet:  cpuset.New(0, 1, 2, 3, 4, 5, 6, 7),
			initContainerIDs: []string{"initFakeID-1", "initFakeID-2"},
			containerIDs:     []string{"appFakeID-1", "appFakeID-2"},
			pod: makeMultiContainerPod(
				[]struct{ request, limit string }{
					{"2000m", "2000m"},
					{"4000m", "4000m"}},
				[]struct{ request, limit string }{
					{"2000m", "2000m"},
					{"2000m", "2000m"}}),
			expInitCSets: []cpuset.CPUSet{
				cpuset.New(0, 4),
				cpuset.New(0, 4, 1, 5)},
			expCSets: []cpuset.CPUSet{
				cpuset.New(0, 4),
				cpuset.New(1, 5)},
		},
	}

	for _, testCase := range testCases {
		logger, ctx := ktesting.NewTestContext(t)
		policy, _ := NewStaticPolicy(logger, testCase.topo, testCase.numReservedCPUs, cpuset.New(), topologymanager.NewFakeManager(logger), nil)

		mockState := &mockState{
			assignments:   testCase.stAssignments,
			defaultCPUSet: testCase.stDefaultCPUSet,
		}

		mgr := &manager{
			policy:            policy,
			state:             mockState,
			lastUpdateState:   state.NewMemoryState(logger),
			containerRuntime:  mockRuntimeService{},
			containerMap:      containermap.NewContainerMap(),
			podStatusProvider: mockPodStatusProvider{},
			sourcesReady:      &sourcesReadyStub{},
			activePods: func() []*v1.Pod {
				return []*v1.Pod{testCase.pod}
			},
		}

		// 按 init 容器在前、app 容器在后的顺序依次分配，与 kubelet 实际启动顺序一致。
		containers := append(
			testCase.pod.Spec.InitContainers,
			testCase.pod.Spec.Containers...)

		containerIDs := append(
			testCase.initContainerIDs,
			testCase.containerIDs...)

		expCSets := append(
			testCase.expInitCSets,
			testCase.expCSets...)

		// cumCSet 累积所有已分配 CPU，用于校验默认 cpuset 只被扣减一次。
		cumCSet := cpuset.New()

		for i := range containers {
			err := mgr.Allocate(ctx, testCase.pod, &containers[i], lifecycle.AddOperation)
			if err != nil {
				t.Errorf("StaticPolicy Allocate() error (%v). unexpected error for container id: %v: %v",
					testCase.description, containerIDs[i], err)
			}

			mgr.AddContainer(logger, testCase.pod, &containers[i], containerIDs[i])
			_, _, err = mgr.containerMap.GetContainerRef(containerIDs[i])
			if err != nil {
				t.Errorf("StaticPolicy AddContainer() error (%v). unexpected error for container id: %v: %v",
					testCase.description, containerIDs[i], err)
			}

			cset, found := mockState.assignments[string(testCase.pod.UID)][containers[i].Name]
			if !expCSets[i].IsEmpty() && !found {
				t.Errorf("StaticPolicy AddContainer() error (%v). expected container %v to be present in assignments %v",
					testCase.description, containers[i].Name, mockState.assignments)
			}

			if found && !cset.Equals(expCSets[i]) {
				t.Errorf("StaticPolicy AddContainer() error (%v). expected cpuset %v for container %v but got %v",
					testCase.description, expCSets[i], containers[i].Name, cset)
			}

			cumCSet = cumCSet.Union(cset)
		}

		// 不变式：最终默认 cpuset 应等于初始默认集合减去全部已分配 CPU。
		if !testCase.stDefaultCPUSet.Difference(cumCSet).Equals(mockState.defaultCPUSet) {
			t.Errorf("StaticPolicy error (%v). expected final state for defaultCPUSet %v but got %v",
				testCase.description, testCase.stDefaultCPUSet.Difference(cumCSet), mockState.defaultCPUSet)
		}
	}
}

// TestCPUManagerGenerate 验证 NewManager 对策略名、reservation 与拓扑的校验。
func TestCPUManagerGenerate(t *testing.T) {
	if runtime.GOOS == "windows" {
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.WindowsCPUAndMemoryAffinity, true)
	}

	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodVerticalScalingExclusiveCPUs, false)
	testCases := []struct {
		description                string
		cpuPolicyName              string
		nodeAllocatableReservation v1.ResourceList
		isTopologyBroken           bool
		expectedPolicy             string
		expectedError              error
	}{
		{
			description:                "set none policy",
			cpuPolicyName:              "none",
			nodeAllocatableReservation: nil,
			expectedPolicy:             "none",
		},
		{
			description:                "invalid policy name",
			cpuPolicyName:              "invalid",
			nodeAllocatableReservation: nil,
			expectedError:              fmt.Errorf("unknown policy: \"invalid\""),
		},
		{
			description:                "static policy",
			cpuPolicyName:              "static",
			nodeAllocatableReservation: v1.ResourceList{v1.ResourceCPU: *resource.NewQuantity(3, resource.DecimalSI)},
			expectedPolicy:             "static",
		},
		{
			description:                "static policy - broken topology",
			cpuPolicyName:              "static",
			nodeAllocatableReservation: v1.ResourceList{},
			isTopologyBroken:           true,
			expectedError:              fmt.Errorf("could not detect number of cpus"),
		},
		{
			description:                "static policy - broken reservation",
			cpuPolicyName:              "static",
			nodeAllocatableReservation: v1.ResourceList{},
			expectedError:              fmt.Errorf("unable to determine reserved CPU resources for static policy"),
		},
		{
			description:                "static policy - no CPU resources",
			cpuPolicyName:              "static",
			nodeAllocatableReservation: v1.ResourceList{v1.ResourceCPU: *resource.NewQuantity(0, resource.DecimalSI)},
			expectedError:              fmt.Errorf("the static policy requires systemreserved.cpu + kubereserved.cpu to be greater than zero"),
		},
	}

	// 扁平 4 core 拓扑，避免超线程带来的分配歧义。
	mockedMachineInfo := cadvisorapi.MachineInfo{
		NumCores: 4,
		Topology: []cadvisorapi.Node{
			{
				Cores: []cadvisorapi.Core{
					{
						Id:           0,
						Threads:      []int{0},
						UncoreCaches: []cadvisorapi.Cache{{Id: 1}},
					},
					{
						Id:           1,
						Threads:      []int{1},
						UncoreCaches: []cadvisorapi.Cache{{Id: 1}},
					},
					{
						Id:           2,
						Threads:      []int{2},
						UncoreCaches: []cadvisorapi.Cache{{Id: 1}},
					},
					{
						Id:           3,
						Threads:      []int{3},
						UncoreCaches: []cadvisorapi.Cache{{Id: 1}},
					},
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			machineInfo := &mockedMachineInfo
			if testCase.isTopologyBroken {
				machineInfo = &cadvisorapi.MachineInfo{}
			}
			sDir, err := os.MkdirTemp("", "cpu_manager_test")
			if err != nil {
				t.Errorf("cannot create state file: %s", err.Error())
			}
			t.Cleanup(func() {
				require.NoErrorf(t, os.RemoveAll(sDir), "unable to remove dir %s", sDir)
			})

			logger, _ := ktesting.NewTestContext(t)
			// NewManager 负责校验策略参数与拓扑；错误用例只断言错误文本。
			mgr, err := NewManager(logger, testCase.cpuPolicyName, nil, 5*time.Second, machineInfo, cpuset.New(), testCase.nodeAllocatableReservation, sDir, topologymanager.NewFakeManager(logger))
			if testCase.expectedError != nil {
				if !strings.Contains(err.Error(), testCase.expectedError.Error()) {
					t.Errorf("Unexpected error message. Have: %s wants %s", err.Error(), testCase.expectedError.Error())
				}
			} else {
				rawMgr := mgr.(*manager)
				if rawMgr.policy.Name() != testCase.expectedPolicy {
					t.Errorf("Unexpected policy name. Have: %q wants %q", rawMgr.policy.Name(), testCase.expectedPolicy)
				}
				if rawMgr.topology == nil {
					t.Errorf("Expected topology to be non-nil for policy '%v'. Have: %v", rawMgr.policy.Name(), rawMgr.topology)
				}
			}
		})

	}
}

// TestCPUManagerRemove 验证 RemoveContainer 的正常路径与策略错误传播。
func TestCPUManagerRemove(t *testing.T) {
	if runtime.GOOS == "windows" {
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.WindowsCPUAndMemoryAffinity, true)
	}

	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodVerticalScalingExclusiveCPUs, false)
	containerID := "fakeID"
	containerMap := containermap.NewContainerMap()

	logger, _ := ktesting.NewTestContext(t)
	mgr := &manager{
		policy: &mockPolicy{
			err: nil,
		},
		state: &mockState{
			assignments:   state.ContainerCPUAssignments{},
			defaultCPUSet: cpuset.New(),
		},
		lastUpdateState:   state.NewMemoryState(logger),
		containerRuntime:  mockRuntimeService{},
		containerMap:      containerMap,
		activePods:        func() []*v1.Pod { return nil },
		podStatusProvider: mockPodStatusProvider{},
	}

	containerMap.Add("", "", containerID)
	err := mgr.RemoveContainer(logger, containerID)
	if err != nil {
		t.Errorf("CPU Manager RemoveContainer() error. expected error to be nil but got: %v", err)
	}

	mgr = &manager{
		policy: &mockPolicy{
			err: fmt.Errorf("fake error"),
		},
		state:             state.NewMemoryState(logger),
		containerRuntime:  mockRuntimeService{},
		containerMap:      containerMap,
		activePods:        func() []*v1.Pod { return nil },
		podStatusProvider: mockPodStatusProvider{},
	}

	containerMap.Add("", "", containerID)
	err = mgr.RemoveContainer(logger, containerID)
	if !reflect.DeepEqual(err, fmt.Errorf("fake error")) {
		t.Errorf("CPU Manager RemoveContainer() error. expected error: fake error but got: %v", err)
	}
}

// TestReconcileState 覆盖 reconcileState 对活跃容器、缺失状态与陈旧分配的处理。
func TestReconcileState(t *testing.T) {
	if runtime.GOOS == "windows" {
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.WindowsCPUAndMemoryAffinity, true)
	}

	logger, tCtx := ktesting.NewTestContext(t)
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodVerticalScalingExclusiveCPUs, false)

	testPolicy, _ := NewStaticPolicy(
		logger,
		&topology.CPUTopology{
			NumCPUs:    8,
			NumSockets: 2,
			NumCores:   4,
			CPUDetails: map[int]topology.CPUInfo{
				0: {CoreID: 0, SocketID: 0},
				1: {CoreID: 1, SocketID: 0},
				2: {CoreID: 2, SocketID: 0},
				3: {CoreID: 3, SocketID: 0},
				4: {CoreID: 0, SocketID: 1},
				5: {CoreID: 1, SocketID: 1},
				6: {CoreID: 2, SocketID: 1},
				7: {CoreID: 3, SocketID: 1},
			},
		},
		0,
		cpuset.New(),
		topologymanager.NewFakeManager(logger),
		nil)

	testCases := []struct {
		description                  string
		policy                       Policy
		activePods                   []*v1.Pod
		pspPS                        v1.PodStatus
		pspFound                     bool
		updateErr                    error
		stAssignments                state.ContainerCPUAssignments
		stDefaultCPUSet              cpuset.CPUSet
		lastUpdateStAssignments      state.ContainerCPUAssignments
		lastUpdateStDefaultCPUSet    cpuset.CPUSet
		expectStAssignments          state.ContainerCPUAssignments
		expectStDefaultCPUSet        cpuset.CPUSet
		expectSucceededContainerName string
		expectFailedContainerName    string
	}{
		{
			description: "cpu manager reconcile - no error",
			policy:      testPolicy,
			activePods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "fakePodName",
						UID:  "fakePodUID",
					},
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name: "fakeContainerName",
							},
						},
					},
				},
			},
			pspPS: v1.PodStatus{
				ContainerStatuses: []v1.ContainerStatus{
					{
						Name:        "fakeContainerName",
						ContainerID: "docker://fakeContainerID",
						State: v1.ContainerState{
							Running: &v1.ContainerStateRunning{},
						},
					},
				},
			},
			pspFound:  true,
			updateErr: nil,
			stAssignments: state.ContainerCPUAssignments{
				"fakePodUID": map[string]cpuset.CPUSet{
					"fakeContainerName": cpuset.New(1, 2),
				},
			},
			stDefaultCPUSet:           cpuset.New(3, 4, 5, 6, 7),
			lastUpdateStAssignments:   state.ContainerCPUAssignments{},
			lastUpdateStDefaultCPUSet: cpuset.New(),
			expectStAssignments: state.ContainerCPUAssignments{
				"fakePodUID": map[string]cpuset.CPUSet{
					"fakeContainerName": cpuset.New(1, 2),
				},
			},
			expectStDefaultCPUSet:        cpuset.New(3, 4, 5, 6, 7),
			expectSucceededContainerName: "fakeContainerName",
			expectFailedContainerName:    "",
		},
		{
			description: "cpu manager reconcile init container - no error",
			policy:      testPolicy,
			activePods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "fakePodName",
						UID:  "fakePodUID",
					},
					Spec: v1.PodSpec{
						InitContainers: []v1.Container{
							{
								Name: "fakeContainerName",
							},
						},
					},
				},
			},
			pspPS: v1.PodStatus{
				InitContainerStatuses: []v1.ContainerStatus{
					{
						Name:        "fakeContainerName",
						ContainerID: "docker://fakeContainerID",
						State: v1.ContainerState{
							Running: &v1.ContainerStateRunning{},
						},
					},
				},
			},
			pspFound:  true,
			updateErr: nil,
			stAssignments: state.ContainerCPUAssignments{
				"fakePodUID": map[string]cpuset.CPUSet{
					"fakeContainerName": cpuset.New(1, 2),
				},
			},
			stDefaultCPUSet:           cpuset.New(3, 4, 5, 6, 7),
			lastUpdateStAssignments:   state.ContainerCPUAssignments{},
			lastUpdateStDefaultCPUSet: cpuset.New(),
			expectStAssignments: state.ContainerCPUAssignments{
				"fakePodUID": map[string]cpuset.CPUSet{
					"fakeContainerName": cpuset.New(1, 2),
				},
			},
			expectStDefaultCPUSet:        cpuset.New(3, 4, 5, 6, 7),
			expectSucceededContainerName: "fakeContainerName",
			expectFailedContainerName:    "",
		},
		{
			description: "cpu manager reconcile - pod status not found",
			policy:      testPolicy,
			activePods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "fakePodName",
						UID:  "fakePodUID",
					},
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name: "fakeContainerName",
							},
						},
					},
				},
			},
			pspPS:                        v1.PodStatus{},
			pspFound:                     false,
			updateErr:                    nil,
			stAssignments:                state.ContainerCPUAssignments{},
			stDefaultCPUSet:              cpuset.New(),
			lastUpdateStAssignments:      state.ContainerCPUAssignments{},
			lastUpdateStDefaultCPUSet:    cpuset.New(),
			expectStAssignments:          state.ContainerCPUAssignments{},
			expectStDefaultCPUSet:        cpuset.New(),
			expectSucceededContainerName: "",
			expectFailedContainerName:    "",
		},
		{
			description: "cpu manager reconcile - container state not found",
			policy:      testPolicy,
			activePods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "fakePodName",
						UID:  "fakePodUID",
					},
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name: "fakeContainerName",
							},
						},
					},
				},
			},
			pspPS: v1.PodStatus{
				ContainerStatuses: []v1.ContainerStatus{
					{
						Name:        "fakeContainerName1",
						ContainerID: "docker://fakeContainerID",
					},
				},
			},
			pspFound:                     true,
			updateErr:                    nil,
			stAssignments:                state.ContainerCPUAssignments{},
			stDefaultCPUSet:              cpuset.New(),
			lastUpdateStAssignments:      state.ContainerCPUAssignments{},
			lastUpdateStDefaultCPUSet:    cpuset.New(),
			expectStAssignments:          state.ContainerCPUAssignments{},
			expectStDefaultCPUSet:        cpuset.New(),
			expectSucceededContainerName: "",
			expectFailedContainerName:    "fakeContainerName",
		},
		{
			description: "cpu manager reconclie - cpuset is empty",
			policy:      testPolicy,
			activePods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "fakePodName",
						UID:  "fakePodUID",
					},
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name: "fakeContainerName",
							},
						},
					},
				},
			},
			pspPS: v1.PodStatus{
				ContainerStatuses: []v1.ContainerStatus{
					{
						Name:        "fakeContainerName",
						ContainerID: "docker://fakeContainerID",
						State: v1.ContainerState{
							Running: &v1.ContainerStateRunning{},
						},
					},
				},
			},
			pspFound:  true,
			updateErr: nil,
			stAssignments: state.ContainerCPUAssignments{
				"fakePodUID": map[string]cpuset.CPUSet{
					"fakeContainerName": cpuset.New(),
				},
			},
			stDefaultCPUSet:           cpuset.New(1, 2, 3, 4, 5, 6, 7),
			lastUpdateStAssignments:   state.ContainerCPUAssignments{},
			lastUpdateStDefaultCPUSet: cpuset.New(),
			expectStAssignments: state.ContainerCPUAssignments{
				"fakePodUID": map[string]cpuset.CPUSet{
					"fakeContainerName": cpuset.New(),
				},
			},
			expectStDefaultCPUSet:        cpuset.New(1, 2, 3, 4, 5, 6, 7),
			expectSucceededContainerName: "",
			expectFailedContainerName:    "fakeContainerName",
		},
		{
			description: "cpu manager reconclie - container update error",
			policy:      testPolicy,
			activePods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "fakePodName",
						UID:  "fakePodUID",
					},
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name: "fakeContainerName",
							},
						},
					},
				},
			},
			pspPS: v1.PodStatus{
				ContainerStatuses: []v1.ContainerStatus{
					{
						Name:        "fakeContainerName",
						ContainerID: "docker://fakeContainerID",
						State: v1.ContainerState{
							Running: &v1.ContainerStateRunning{},
						},
					},
				},
			},
			pspFound:  true,
			updateErr: fmt.Errorf("fake container update error"),
			stAssignments: state.ContainerCPUAssignments{
				"fakePodUID": map[string]cpuset.CPUSet{
					"fakeContainerName": cpuset.New(1, 2),
				},
			},
			stDefaultCPUSet:           cpuset.New(3, 4, 5, 6, 7),
			lastUpdateStAssignments:   state.ContainerCPUAssignments{},
			lastUpdateStDefaultCPUSet: cpuset.New(),
			expectStAssignments: state.ContainerCPUAssignments{
				"fakePodUID": map[string]cpuset.CPUSet{
					"fakeContainerName": cpuset.New(1, 2),
				},
			},
			expectStDefaultCPUSet:        cpuset.New(3, 4, 5, 6, 7),
			expectSucceededContainerName: "",
			expectFailedContainerName:    "fakeContainerName",
		},
		{
			description: "cpu manager reconcile - state has inactive container",
			policy:      testPolicy,
			activePods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "fakePodName",
						UID:  "fakePodUID",
					},
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name: "fakeContainerName",
							},
						},
					},
				},
			},
			pspPS: v1.PodStatus{
				ContainerStatuses: []v1.ContainerStatus{
					{
						Name:        "fakeContainerName",
						ContainerID: "docker://fakeContainerID",
						State: v1.ContainerState{
							Running: &v1.ContainerStateRunning{},
						},
					},
				},
			},
			pspFound:  true,
			updateErr: nil,
			stAssignments: state.ContainerCPUAssignments{
				"fakePodUID": map[string]cpuset.CPUSet{
					"fakeContainerName": cpuset.New(1, 2),
				},
				"secondfakePodUID": map[string]cpuset.CPUSet{
					"secondfakeContainerName": cpuset.New(3, 4),
				},
			},
			stDefaultCPUSet:           cpuset.New(5, 6, 7),
			lastUpdateStAssignments:   state.ContainerCPUAssignments{},
			lastUpdateStDefaultCPUSet: cpuset.New(),
			expectStAssignments: state.ContainerCPUAssignments{
				"fakePodUID": map[string]cpuset.CPUSet{
					"fakeContainerName": cpuset.New(1, 2),
				},
			},
			expectStDefaultCPUSet:        cpuset.New(3, 4, 5, 6, 7),
			expectSucceededContainerName: "fakeContainerName",
			expectFailedContainerName:    "",
		},
		{
			description: "cpu manager reconcile - last update state is current",
			policy:      testPolicy,
			activePods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "fakePodName",
						UID:  "fakePodUID",
					},
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name: "fakeContainerName",
							},
						},
					},
				},
			},
			pspPS: v1.PodStatus{
				ContainerStatuses: []v1.ContainerStatus{
					{
						Name:        "fakeContainerName",
						ContainerID: "docker://fakeContainerID",
						State: v1.ContainerState{
							Running: &v1.ContainerStateRunning{},
						},
					},
				},
			},
			pspFound:  true,
			updateErr: nil,
			stAssignments: state.ContainerCPUAssignments{
				"fakePodUID": map[string]cpuset.CPUSet{
					"fakeContainerName": cpuset.New(1, 2),
				},
			},
			stDefaultCPUSet: cpuset.New(5, 6, 7),
			lastUpdateStAssignments: state.ContainerCPUAssignments{
				"fakePodUID": map[string]cpuset.CPUSet{
					"fakeContainerName": cpuset.New(1, 2),
				},
			},
			lastUpdateStDefaultCPUSet: cpuset.New(5, 6, 7),
			expectStAssignments: state.ContainerCPUAssignments{
				"fakePodUID": map[string]cpuset.CPUSet{
					"fakeContainerName": cpuset.New(1, 2),
				},
			},
			expectStDefaultCPUSet:        cpuset.New(5, 6, 7),
			expectSucceededContainerName: "fakeContainerName",
			expectFailedContainerName:    "",
		},
		{
			description: "cpu manager reconcile - last update state is not current",
			policy:      testPolicy,
			activePods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "fakePodName",
						UID:  "fakePodUID",
					},
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name: "fakeContainerName",
							},
						},
					},
				},
			},
			pspPS: v1.PodStatus{
				ContainerStatuses: []v1.ContainerStatus{
					{
						Name:        "fakeContainerName",
						ContainerID: "docker://fakeContainerID",
						State: v1.ContainerState{
							Running: &v1.ContainerStateRunning{},
						},
					},
				},
			},
			pspFound:  true,
			updateErr: nil,
			stAssignments: state.ContainerCPUAssignments{
				"fakePodUID": map[string]cpuset.CPUSet{
					"fakeContainerName": cpuset.New(1, 2),
				},
			},
			stDefaultCPUSet: cpuset.New(3, 4, 5, 6, 7),
			lastUpdateStAssignments: state.ContainerCPUAssignments{
				"fakePodUID": map[string]cpuset.CPUSet{
					"fakeContainerName": cpuset.New(3, 4),
				},
			},
			lastUpdateStDefaultCPUSet: cpuset.New(1, 2, 5, 6, 7),
			expectStAssignments: state.ContainerCPUAssignments{
				"fakePodUID": map[string]cpuset.CPUSet{
					"fakeContainerName": cpuset.New(1, 2),
				},
			},
			expectStDefaultCPUSet:        cpuset.New(3, 4, 5, 6, 7),
			expectSucceededContainerName: "fakeContainerName",
			expectFailedContainerName:    "",
		},
	}

	for _, testCase := range testCases {
		mgr := &manager{
			policy: testCase.policy,
			state: &mockState{
				assignments:   testCase.stAssignments,
				defaultCPUSet: testCase.stDefaultCPUSet,
			},
			lastUpdateState: state.NewMemoryState(logger),
			containerRuntime: mockRuntimeService{
				err: testCase.updateErr,
			},
			containerMap: containermap.NewContainerMap(),
			activePods: func() []*v1.Pod {
				return testCase.activePods
			},
			podStatusProvider: mockPodStatusProvider{
				podStatus: testCase.pspPS,
				found:     testCase.pspFound,
			},
		}
		mgr.sourcesReady = &sourcesReadyStub{}
		// reconcileState 对比期望分配与运行时状态，返回成功与失败的容器列表。
		success, failure := mgr.reconcileState(tCtx)

		if !reflect.DeepEqual(testCase.expectStAssignments, mgr.state.GetCPUAssignments()) {
			t.Errorf("%v", testCase.description)
			t.Errorf("Expected state container cpu assignments: %v, actual: %v", testCase.expectStAssignments, mgr.state.GetCPUAssignments())

		}

		if !reflect.DeepEqual(testCase.expectStDefaultCPUSet, mgr.state.GetDefaultCPUSet()) {
			t.Errorf("%v", testCase.description)
			t.Errorf("Expected state default cpuset: %v, actual: %v", testCase.expectStDefaultCPUSet, mgr.state.GetDefaultCPUSet())

		}

		if testCase.expectSucceededContainerName != "" {
			// 在成功列表中查找期望的容器名。
			foundSucceededContainer := false
			for _, reconciled := range success {
				if reconciled.containerName == testCase.expectSucceededContainerName {
					foundSucceededContainer = true
					break
				}
			}
			if !foundSucceededContainer {
				t.Errorf("%v", testCase.description)
				t.Errorf("Expected reconciliation success for container: %s", testCase.expectSucceededContainerName)
			}
		}

		if testCase.expectFailedContainerName != "" {
			// 在失败列表中查找期望的容器名。
			foundFailedContainer := false
			for _, reconciled := range failure {
				if reconciled.containerName == testCase.expectFailedContainerName {
					foundFailedContainer = true
					break
				}
			}
			if !foundFailedContainer {
				t.Errorf("%v", testCase.description)
				t.Errorf("Expected reconciliation failure for container: %s", testCase.expectFailedContainerName)
			}
		}
	}
}

// TestReconcileStateBaselineLog 验证 baseline 与当前分配不一致时是否输出告警日志。
func TestReconcileStateBaselineLog(t *testing.T) {
	testCases := []struct {
		description   string
		fgEnabled     bool
		stAssignments state.ContainerCPUAssignments
		stBaselines   state.ContainerCPUBaselines
		expectLog     bool
	}{
		{
			description: "feature gate disabled, current container CPU assignments differ from CPU assignments recorded at admission time, no log expected",
			fgEnabled:   false,
			stAssignments: state.ContainerCPUAssignments{
				"fakePodUID": map[string]cpuset.CPUSet{
					"fakeContainerName": cpuset.New(1, 2),
				},
			},
			stBaselines: state.ContainerCPUBaselines{
				"fakePodUID": map[string]state.ContainerCPUBaseline{
					"fakeContainerName": {Baseline: cpuset.New(3, 4)},
				},
			},
			expectLog: false,
		},
		{
			description: "feature gate enabled, current container CPU assignments matches CPU assignments recorded at admission time, no log expected",
			fgEnabled:   true,
			stAssignments: state.ContainerCPUAssignments{
				"fakePodUID": map[string]cpuset.CPUSet{
					"fakeContainerName": cpuset.New(1, 2),
				},
			},
			stBaselines: state.ContainerCPUBaselines{
				"fakePodUID": map[string]state.ContainerCPUBaseline{
					"fakeContainerName": {Baseline: cpuset.New(1, 2)},
				},
			},
			expectLog: false,
		},
		{
			description: "feature gate enabled, current container CPU assignments differs from CPU assignments recorded at admission time, log expected",
			fgEnabled:   true,
			stAssignments: state.ContainerCPUAssignments{
				"fakePodUID": map[string]cpuset.CPUSet{
					"fakeContainerName": cpuset.New(1, 2),
				},
			},
			stBaselines: state.ContainerCPUBaselines{
				"fakePodUID": map[string]state.ContainerCPUBaseline{
					"fakeContainerName": {Baseline: cpuset.New(3, 4)},
				},
			},
			expectLog: true,
		},
		{
			description: "feature gate enabled, no CPU assignments recorded at admission time, no log expected",
			fgEnabled:   true,
			stAssignments: state.ContainerCPUAssignments{
				"fakePodUID": map[string]cpuset.CPUSet{
					"fakeContainerName": cpuset.New(1, 2),
				},
			},
			stBaselines: state.ContainerCPUBaselines{},
			expectLog:   false,
		},
		{
			description:   "feature gate enabled, no current container CPU assignments, CPU assignments recorded at admission time doesn't match default shared CPU set, log expected",
			fgEnabled:     true,
			stAssignments: state.ContainerCPUAssignments{}, // 这样容器就能使用默认的 CPUSet。
			stBaselines: state.ContainerCPUBaselines{
				"fakePodUID": map[string]state.ContainerCPUBaseline{
					"fakeContainerName": {Baseline: cpuset.New(3, 4)},
				},
			},
			expectLog: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodVerticalScalingExclusiveCPUs, tc.fgEnabled)
			// 提高日志级别并缓冲输出，便于断言告警文本是否存在。
			ktesting.SetDefaultVerbosity(4)
			tCtx := ktesting.Init(t, initoption.BufferLogs(true))
			logger := tCtx.Logger()

			testPolicy, _ := NewStaticPolicy(
				logger,
				&topology.CPUTopology{
					NumCPUs:    8,
					NumSockets: 2,
					NumCores:   4,
					CPUDetails: map[int]topology.CPUInfo{
						0: {CoreID: 0, SocketID: 0},
						1: {CoreID: 1, SocketID: 0},
						2: {CoreID: 2, SocketID: 0},
						3: {CoreID: 3, SocketID: 0},
						4: {CoreID: 0, SocketID: 1},
						5: {CoreID: 1, SocketID: 1},
						6: {CoreID: 2, SocketID: 1},
						7: {CoreID: 3, SocketID: 1},
					},
				},
				0,
				cpuset.New(),
				topologymanager.NewFakeManager(logger),
				nil)

			mgr := &manager{
				policy: testPolicy,
				state: &mockState{
					assignments:   tc.stAssignments,
					defaultCPUSet: cpuset.New(5, 6, 7),
					baselines:     tc.stBaselines,
				},
				lastUpdateState: state.NewMemoryState(logger),
				containerRuntime: mockRuntimeService{
					err: nil,
				},
				containerMap: containermap.NewContainerMap(),
				activePods: func() []*v1.Pod {
					return []*v1.Pod{
						{
							ObjectMeta: metav1.ObjectMeta{
								Name: "fakePodName",
								UID:  "fakePodUID",
							},
							Spec: v1.PodSpec{
								Containers: []v1.Container{
									{Name: "fakeContainerName"},
								},
							},
						},
					}
				},
				podStatusProvider: mockPodStatusProvider{
					podStatus: v1.PodStatus{
						ContainerStatuses: []v1.ContainerStatus{
							{
								Name:        "fakeContainerName",
								ContainerID: "docker://fakeContainerID",
								State: v1.ContainerState{
									Running: &v1.ContainerStateRunning{},
								},
							},
						},
					},
					found: true,
				},
			}
			mgr.sourcesReady = &sourcesReadyStub{}
			mgr.reconcileState(tCtx)

			// 通过 ktesting 的缓冲日志断言是否输出了 baseline 不一致告警。
			underlier, ok := logger.GetSink().(ktesting.Underlier)
			if !ok {
				t.Fatalf("expected ktesting LogSink, got %T", logger.GetSink())
			}
			logs := underlier.GetBuffer().String()
			expectedLog := "Current container CPU assignments differ from CPU assignments recorded at admission time"
			if tc.expectLog && !strings.Contains(logs, expectedLog) {
				t.Errorf("expected log %q not found in logs:\n%s", expectedLog, logs)
			}
			if !tc.expectLog && strings.Contains(logs, expectedLog) {
				t.Errorf("unexpected log %q found in logs:\n%s", expectedLog, logs)
			}
		})
	}
}

// 以上用例未配置 kubelet --reserved-cpus 选项。
// 以下用例配置了 --reserved-cpus，预留 CPU 从默认 cpuset 中扣除。
// TestCPUManagerAddWithResvList 验证 --reserved-cpus 配置下预留 CPU 不会被分配给容器。
func TestCPUManagerAddWithResvList(t *testing.T) {
	if runtime.GOOS == "windows" {
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.WindowsCPUAndMemoryAffinity, true)
	}

	logger, tCtx := ktesting.NewTestContext(t)
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodVerticalScalingExclusiveCPUs, false)
	testPolicy, _ := NewStaticPolicy(
		logger,
		&topology.CPUTopology{
			NumCPUs:    4,
			NumSockets: 1,
			NumCores:   4,
			CPUDetails: map[int]topology.CPUInfo{
				0: {CoreID: 0, SocketID: 0},
				1: {CoreID: 1, SocketID: 0},
				2: {CoreID: 2, SocketID: 0},
				3: {CoreID: 3, SocketID: 0},
			},
		},
		1,
		cpuset.New(0),
		topologymanager.NewFakeManager(logger),
		nil)
	testCases := []struct {
		description        string
		updateErr          error
		policy             Policy
		expCPUSet          cpuset.CPUSet
		expAllocateErr     error
		expAddContainerErr error
	}{
		{
			description:        "cpu manager add - no error",
			updateErr:          nil,
			policy:             testPolicy,
			expCPUSet:          cpuset.New(0, 3),
			expAllocateErr:     nil,
			expAddContainerErr: nil,
		},
	}

	for _, testCase := range testCases {
		mgr := &manager{
			policy: testCase.policy,
			state: &mockState{
				assignments:   state.ContainerCPUAssignments{},
				defaultCPUSet: cpuset.New(0, 1, 2, 3),
			},
			lastUpdateState: state.NewMemoryState(logger),
			containerRuntime: mockRuntimeService{
				err: testCase.updateErr,
			},
			containerMap:      containermap.NewContainerMap(),
			podStatusProvider: mockPodStatusProvider{},
			sourcesReady:      &sourcesReadyStub{},
		}

		pod := makePod("fakePod", "fakeContainer", "2", "2")
		container := &pod.Spec.Containers[0]
		mgr.activePods = func() []*v1.Pod { return []*v1.Pod{pod} }

		err := mgr.Allocate(tCtx, pod, container, lifecycle.AddOperation)
		if !reflect.DeepEqual(err, testCase.expAllocateErr) {
			t.Errorf("CPU Manager Allocate() error (%v). expected error: %v but got: %v",
				testCase.description, testCase.expAllocateErr, err)
		}

		mgr.AddContainer(logger, pod, container, "fakeID")
		_, _, err = mgr.containerMap.GetContainerRef("fakeID")
		if !reflect.DeepEqual(err, testCase.expAddContainerErr) {
			t.Errorf("CPU Manager AddContainer() error (%v). expected error: %v but got: %v",
				testCase.description, testCase.expAddContainerErr, err)
		}
		if !testCase.expCPUSet.Equals(mgr.state.GetDefaultCPUSet()) {
			t.Errorf("CPU Manager AddContainer() error (%v). expected cpuset: %v but got: %v",
				testCase.description, testCase.expCPUSet, mgr.state.GetDefaultCPUSet())
		}
	}
}

// TestCPUManagerHandlePolicyOptions 验证不支持的策略选项会被 NewManager 拒绝。
func TestCPUManagerHandlePolicyOptions(t *testing.T) {
	if runtime.GOOS == "windows" {
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.WindowsCPUAndMemoryAffinity, true)
	}

	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodVerticalScalingExclusiveCPUs, false)
	testCases := []struct {
		description      string
		cpuPolicyName    string
		cpuPolicyOptions map[string]string
		expectedError    error
	}{
		{
			description:   "options to none policy",
			cpuPolicyName: "none",
			cpuPolicyOptions: map[string]string{
				FullPCPUsOnlyOption: "true",
			},
			expectedError: fmt.Errorf("received unsupported options"),
		},
	}

	// 任何真实可用的拓扑都行，这里选一个简单的。
	mockedMachineInfo := cadvisorapi.MachineInfo{
		NumCores: 4,
		Topology: []cadvisorapi.Node{
			{
				Cores: []cadvisorapi.Core{
					{
						Id:      0,
						Threads: []int{0},
					},
					{
						Id:      1,
						Threads: []int{1},
					},
					{
						Id:      2,
						Threads: []int{2},
					},
					{
						Id:      3,
						Threads: []int{3},
					},
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			machineInfo := &mockedMachineInfo
			nodeAllocatableReservation := v1.ResourceList{}
			sDir, err := os.MkdirTemp("", "cpu_manager_test")
			if err != nil {
				t.Errorf("cannot create state file: %s", err.Error())
			}
			t.Cleanup(func() {
				require.NoErrorf(t, os.RemoveAll(sDir), "unable to remove dir %s", sDir)
			})

			logger, _ := ktesting.NewTestContext(t)
			_, err = NewManager(logger, testCase.cpuPolicyName, testCase.cpuPolicyOptions, 5*time.Second, machineInfo, cpuset.New(), nodeAllocatableReservation, sDir, topologymanager.NewFakeManager(logger))
			if err == nil {
				t.Errorf("Expected error, but NewManager succeeded")
			}
			if !strings.Contains(err.Error(), testCase.expectedError.Error()) {
				t.Errorf("Unexpected error message. Have: %s wants %s", err.Error(), testCase.expectedError.Error())
			}
		})

	}
}

// TestCPUManagerGetAllocatableCPUs 验证各策略暴露的可分配 CPU 集合。
func TestCPUManagerGetAllocatableCPUs(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	if runtime.GOOS == "windows" {
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.WindowsCPUAndMemoryAffinity, true)
	}

	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodVerticalScalingExclusiveCPUs, false)
	nonePolicy, _ := NewNonePolicy(nil)
	staticPolicy, _ := NewStaticPolicy(
		logger,
		&topology.CPUTopology{
			NumCPUs:    4,
			NumSockets: 1,
			NumCores:   4,
			CPUDetails: map[int]topology.CPUInfo{
				0: {CoreID: 0, SocketID: 0},
				1: {CoreID: 1, SocketID: 0},
				2: {CoreID: 2, SocketID: 0},
				3: {CoreID: 3, SocketID: 0},
			},
		},
		1,
		cpuset.New(0),
		topologymanager.NewFakeManager(logger),
		nil)

	testCases := []struct {
		description        string
		policy             Policy
		expAllocatableCPUs cpuset.CPUSet
	}{
		{
			description:        "None Policy",
			policy:             nonePolicy,
			expAllocatableCPUs: cpuset.New(),
		},
		{
			description:        "Static Policy",
			policy:             staticPolicy,
			expAllocatableCPUs: cpuset.New(1, 2, 3),
		},
	}
	for _, testCase := range testCases {
		mgr := &manager{
			policy:     testCase.policy,
			activePods: func() []*v1.Pod { return nil },
			state: &mockState{
				assignments:   state.ContainerCPUAssignments{},
				defaultCPUSet: cpuset.New(0, 1, 2, 3),
			},
			lastUpdateState:   state.NewMemoryState(logger),
			containerMap:      containermap.NewContainerMap(),
			podStatusProvider: mockPodStatusProvider{},
			sourcesReady:      &sourcesReadyStub{},
		}
		mgr.sourcesReady = &sourcesReadyStub{}
		// 显式设置可分配 CPU 缓存，模拟 Start 阶段的初始化结果。
		mgr.allocatableCPUs = testCase.policy.GetAllocatableCPUs(mgr.state)

		pod := makePod("fakePod", "fakeContainer", "2", "2")
		container := &pod.Spec.Containers[0]

		_ = mgr.Allocate(tCtx, pod, container, lifecycle.AddOperation)

		if !mgr.GetAllocatableCPUs().Equals(testCase.expAllocatableCPUs) {
			t.Errorf("Policy GetAllocatableCPUs() error (%v). expected cpuset %v for container %v but got %v",
				testCase.description, testCase.expAllocatableCPUs, "fakeContainer", mgr.GetAllocatableCPUs())
		}
	}
}

// TestCPUManagerAddResize 验证 resize 操作不会触发重新分配，只有 add 才分配 CPU。
func TestCPUManagerAddResize(t *testing.T) {
	if runtime.GOOS == "windows" {
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.WindowsCPUAndMemoryAffinity, true)
	}

	logger, tCtx := ktesting.NewTestContext(t)

	testPolicy, _ := NewStaticPolicy(
		logger,
		&topology.CPUTopology{
			NumCPUs:    4,
			NumSockets: 1,
			NumCores:   4,
			CPUDetails: map[int]topology.CPUInfo{
				0: {CoreID: 0, SocketID: 0},
				1: {CoreID: 1, SocketID: 0},
				2: {CoreID: 2, SocketID: 0},
				3: {CoreID: 3, SocketID: 0},
			},
		},
		0,
		cpuset.New(),
		topologymanager.NewFakeManager(logger),
		nil)

	// testStep 描述单次 Allocate + AddContainer 的期望结果。
	type testStep struct {
		description      string
		operation        lifecycle.Operation
		expDefaultCPUSet cpuset.CPUSet
		expAllocateErr   error
		expContainerErr  error
	}

	testCases := []struct {
		description string
		updateErr   error
		policy      Policy
		testSteps   []testStep
	}{
		{
			description: "CPU Manager first add then resize",
			updateErr:   nil,
			policy:      testPolicy,
			testSteps: []testStep{
				{
					description:      "first add operation - no error",
					operation:        lifecycle.AddOperation,
					expDefaultCPUSet: cpuset.New(3, 4),
					expAllocateErr:   nil,
					expContainerErr:  nil,
				},
				{
					description:      "then resize operation - no error - skip resize",
					operation:        lifecycle.ResizeOperation,
					expDefaultCPUSet: cpuset.New(3, 4),
					expAllocateErr:   nil,
					expContainerErr:  nil,
				},
			},
		},
		{
			description: "CPU Manager attempt to resize",
			updateErr:   nil,
			policy:      testPolicy,
			testSteps: []testStep{
				{
					description:      "no error - skip allocation",
					operation:        lifecycle.ResizeOperation,
					expDefaultCPUSet: cpuset.New(1, 2, 3, 4),
					expAllocateErr:   nil,
					expContainerErr:  nil,
				},
			},
		},
		{
			description: "CPU Manager attempt to use empty operation",
			updateErr:   nil,
			policy:      testPolicy,
			testSteps: []testStep{
				{
					description:      "no error - skip allocation",
					operation:        "",
					expDefaultCPUSet: cpuset.New(1, 2, 3, 4),
					expAllocateErr:   nil,
					expContainerErr:  nil,
				},
			},
		},
		{
			description: "CPU Manager first resize then add",
			updateErr:   nil,
			policy:      testPolicy,
			testSteps: []testStep{
				{
					description:      "first resize operation - no error - skip resize",
					operation:        lifecycle.ResizeOperation,
					expDefaultCPUSet: cpuset.New(1, 2, 3, 4),
					expAllocateErr:   nil,
					expContainerErr:  nil,
				},
				{
					description:      "then add operation - no error",
					operation:        lifecycle.AddOperation,
					expDefaultCPUSet: cpuset.New(3, 4),
					expAllocateErr:   nil,
					expContainerErr:  nil,
				},
			},
		},
	}

	for _, testCase := range testCases {
		mgr := &manager{
			policy: testCase.policy,
			state: &mockState{
				assignments:   state.ContainerCPUAssignments{},
				defaultCPUSet: cpuset.New(1, 2, 3, 4),
			},
			lastUpdateState: state.NewMemoryState(logger),
			containerRuntime: mockRuntimeService{
				err: testCase.updateErr,
			},
			containerMap:      containermap.NewContainerMap(),
			podStatusProvider: mockPodStatusProvider{},
			sourcesReady:      &sourcesReadyStub{},
		}

		pod := makePod("fakePod", "fakeContainer", "2", "2")
		container := &pod.Spec.Containers[0]
		mgr.activePods = func() []*v1.Pod { return []*v1.Pod{pod} }

		// 同一 manager 上顺序执行多个操作，验证状态在步骤之间正确累积。
		for _, testStep := range testCase.testSteps {
			err := mgr.Allocate(tCtx, pod, container, testStep.operation)
			if !reflect.DeepEqual(err, testStep.expAllocateErr) {
				t.Errorf("CPU Manager Allocate(%v) error (%v). expected error: %v but got: %v",
					testStep.operation, testStep.description, testStep.expAllocateErr, err)
			}

			mgr.AddContainer(logger, pod, container, "fakeID")
			_, _, err = mgr.containerMap.GetContainerRef("fakeID")
			if !reflect.DeepEqual(err, testStep.expContainerErr) {
				t.Errorf("CPU Manager AddContainer(%v) error (%v). expected error: %v but got: %v",
					testStep.operation, testStep.description, testStep.expContainerErr, err)
			}
			if !testStep.expDefaultCPUSet.Equals(mgr.state.GetDefaultCPUSet()) {
				t.Errorf("CPU Manager AddContainer(%v) error (%v). expected default cpuset: %v but got: %v",
					testStep.operation, testStep.description, testStep.expDefaultCPUSet, mgr.state.GetDefaultCPUSet())
			}
		}
	}
}
