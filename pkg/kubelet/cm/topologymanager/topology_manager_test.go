/*
Copyright 2019 The Kubernetes Authors.

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

package topologymanager

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/klog/v2"

	cadvisorapi "github.com/google/cadvisor/lib/model"

	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager/bitmask"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// NewTestBitMask 构造测试用位掩码。
func NewTestBitMask(sockets ...int) bitmask.BitMask {
	s, _ := bitmask.NewBitMask(sockets...)
	return s
}

// TestNewManager 校验策略名与策略选项解析，以及 NUMA 节点数上限、
// 距离信息不可用等错误路径。
func TestNewManager(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	numaDistanceErr := "error getting NUMA distances from cadvisor"
	if runtime.GOOS == "windows" {
		numaDistanceErr = fmt.Sprintf("the %q policy option is not supported on Windows because NUMA node distances are not available", PreferClosestNUMANodes)
	}

	tcases := []struct {
		description    string
		policyName     string
		expectedPolicy string
		expectedError  error
		topologyError  error
		policyOptions  map[string]string
		topology       []cadvisorapi.Node
	}{
		{
			description:    "Policy is set to none",
			policyName:     "none",
			expectedPolicy: "none",
		},
		{
			description:    "Policy is set to best-effort",
			policyName:     "best-effort",
			expectedPolicy: "best-effort",
		},
		{
			description:    "Policy is set to restricted",
			policyName:     "restricted",
			expectedPolicy: "restricted",
		},
		{
			description:    "Policy is set to single-numa-node",
			policyName:     "single-numa-node",
			expectedPolicy: "single-numa-node",
		},
		{
			description:   "Policy is set to unknown",
			policyName:    "unknown",
			expectedError: fmt.Errorf("unknown policy: \"unknown\""),
		},
		{
			description:    "Unknown policy name best-effort policy",
			policyName:     "best-effort",
			expectedPolicy: "best-effort",
			expectedError:  fmt.Errorf("unknown Topology Manager Policy option:"),
			policyOptions: map[string]string{
				"unknown-option": "true",
			},
		},
		{
			description:    "Unknown policy name restricted policy",
			policyName:     "restricted",
			expectedPolicy: "restricted",
			expectedError:  fmt.Errorf("unknown Topology Manager Policy option:"),
			policyOptions: map[string]string{
				"unknown-option": "true",
			},
		},
		{
			description:    "can't get NUMA distances",
			policyName:     "best-effort",
			expectedPolicy: "best-effort",
			policyOptions: map[string]string{
				PreferClosestNUMANodes: "true",
			},
			expectedError: fmt.Errorf("%s", numaDistanceErr),
			topology: []cadvisorapi.Node{
				{
					Id: 0,
				},
			},
		},
		{
			description:    "more than 8 NUMA nodes",
			policyName:     "best-effort",
			expectedPolicy: "best-effort",
			expectedError:  fmt.Errorf("unsupported on machines with more than %v NUMA Nodes", defaultMaxAllowableNUMANodes),
			topology: []cadvisorapi.Node{
				{
					Id: 0,
				},
				{
					Id: 1,
				},
				{
					Id: 2,
				},
				{
					Id: 3,
				},
				{
					Id: 4,
				},
				{
					Id: 5,
				},
				{
					Id: 6,
				},
				{
					Id: 7,
				},
				{
					Id: 8,
				},
			},
		},
	}

	for _, tc := range tcases {
		topology := tc.topology

		// 错误路径只比较错误消息子串；成功路径则解包 scope 检查策略名。
		mngr, err := NewManager(logger, topology, tc.policyName, "container", tc.policyOptions)
		if tc.expectedError != nil {
			if !strings.Contains(err.Error(), tc.expectedError.Error()) {
				t.Errorf("Unexpected error message. Have: %s wants %s", err.Error(), tc.expectedError.Error())
			}
		} else {
			rawMgr := mngr.(*manager)
			var policyName string
			if rawScope, ok := rawMgr.scope.(*containerScope); ok {
				policyName = rawScope.policy.Name()
			} else if rawScope, ok := rawMgr.scope.(*noneScope); ok {
				policyName = rawScope.policy.Name()
			}
			if policyName != tc.expectedPolicy {
				t.Errorf("Unexpected policy name. Have: %q wants %q", policyName, tc.expectedPolicy)
			}
		}
	}
}

// TestManagerScope 校验 scope 名称（container/pod）的解析。
func TestManagerScope(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	tcases := []struct {
		description   string
		scopeName     string
		expectedScope string
		expectedError error
	}{
		{
			description:   "Topology Manager Scope is set to container",
			scopeName:     "container",
			expectedScope: "container",
		},
		{
			description:   "Topology Manager Scope is set to pod",
			scopeName:     "pod",
			expectedScope: "pod",
		},
		{
			description:   "Topology Manager Scope is set to unknown",
			scopeName:     "unknown",
			expectedError: fmt.Errorf("unknown scope: \"unknown\""),
		},
	}

	for _, tc := range tcases {
		mngr, err := NewManager(logger, nil, "best-effort", tc.scopeName, nil)

		if tc.expectedError != nil {
			if !strings.Contains(err.Error(), tc.expectedError.Error()) {
				t.Errorf("Unexpected error message. Have: %s wants %s", err.Error(), tc.expectedError.Error())
			}
		} else {
			rawMgr := mngr.(*manager)
			if rawMgr.scope.Name() != tc.expectedScope {
				t.Errorf("Unexpected scope name. Have: %q wants %q", rawMgr.scope, tc.expectedScope)
			}
		}
	}
}

// mockHintProvider 是返回预置拓扑提示的 HintProvider 桩。
type mockHintProvider struct {
	th map[string][]TopologyHint
	//TODO: Add this field and add some tests to make sure things error out
	//appropriately on allocation errors.
	//allocateError error
}

// GetTopologyHints 固定返回预置的容器级提示。
func (m *mockHintProvider) GetTopologyHints(logger klog.Logger, pod *v1.Pod, container *v1.Container, _ lifecycle.Operation) map[string][]TopologyHint {
	return m.th
}

// GetPodTopologyHints 固定返回预置的 Pod 级提示。
func (m *mockHintProvider) GetPodTopologyHints(logger klog.Logger, pod *v1.Pod, _ lifecycle.Operation) map[string][]TopologyHint {
	return m.th
}

// AllocatePod 为空实现，始终成功。
func (m *mockHintProvider) AllocatePod(logger klog.Logger, pod *v1.Pod, _ lifecycle.Operation) error {
	return nil
}

// Allocate 为空实现，始终成功。
func (m *mockHintProvider) Allocate(ctx context.Context, pod *v1.Pod, container *v1.Container, _ lifecycle.Operation) error {
	//return allocateError
	return nil
}

// mockPolicy 内嵌 nonePolicy，并记录 Merge 收到的提示。
type mockPolicy struct {
	nonePolicy
	ph []map[string][]TopologyHint
}

// Merge 记录各 provider 的提示并返回一个可接受的空提示。
func (p *mockPolicy) Merge(logger klog.Logger, providersHints []map[string][]TopologyHint) (TopologyHint, bool) {
	p.ph = providersHints
	return TopologyHint{}, true
}

// TestAddHintProvider 校验 HintProvider 被追加到 scope 的 provider 列表。
func TestAddHintProvider(t *testing.T) {
	tcases := []struct {
		name string
		hp   []HintProvider
	}{
		{
			name: "Add HintProvider",
			hp: []HintProvider{
				&mockHintProvider{},
				&mockHintProvider{},
				&mockHintProvider{},
			},
		},
	}
	mngr := manager{}
	mngr.scope = NewContainerScope(NewNonePolicy())
	logger, _ := ktesting.NewTestContext(t)
	for _, tc := range tcases {
		for _, hp := range tc.hp {
			mngr.AddHintProvider(logger, hp)
		}
		if len(tc.hp) != len(mngr.scope.(*containerScope).hintProviders) {
			t.Errorf("error")
		}
	}
}

// TestAdmit 表驱动校验容器 scope 与 Pod scope 的准入结果，
// 覆盖 none/best-effort/restricted/single-numa-node 策略。
func TestAdmit(t *testing.T) {
	tCtx := ktesting.Init(t)
	numaInfo := &NUMAInfo{
		Nodes: []int{0, 1},
		NUMADistances: NUMADistances{
			0: {10, 11},
			1: {11, 10},
		},
	}

	opts := PolicyOptions{}
	bePolicy := NewBestEffortPolicy(numaInfo, opts)
	restrictedPolicy := NewRestrictedPolicy(numaInfo, opts)
	singleNumaPolicy := NewSingleNumaNodePolicy(numaInfo, opts)

	tcases := []struct {
		name     string
		result   lifecycle.PodAdmitResult
		qosClass v1.PodQOSClass
		policy   Policy
		hp       []HintProvider
		expected bool
	}{
		{
			name:     "QOSClass set as BestEffort. None Policy. No Hints.",
			qosClass: v1.PodQOSBestEffort,
			policy:   NewNonePolicy(),
			hp:       []HintProvider{},
			expected: true,
		},
		{
			name:     "QOSClass set as Guaranteed. None Policy. No Hints.",
			qosClass: v1.PodQOSGuaranteed,
			policy:   NewNonePolicy(),
			hp:       []HintProvider{},
			expected: true,
		},
		{
			name:     "QOSClass set as BestEffort. single-numa-node Policy. No Hints.",
			qosClass: v1.PodQOSBestEffort,
			policy:   singleNumaPolicy,
			hp: []HintProvider{
				&mockHintProvider{},
			},
			expected: true,
		},
		{
			name:     "QOSClass set as BestEffort. Restricted Policy. No Hints.",
			qosClass: v1.PodQOSBestEffort,
			policy:   restrictedPolicy,
			hp: []HintProvider{
				&mockHintProvider{},
			},
			expected: true,
		},
		{
			name:     "QOSClass set as Guaranteed. BestEffort Policy. Preferred Affinity.",
			qosClass: v1.PodQOSGuaranteed,
			policy:   bePolicy,
			hp: []HintProvider{
				&mockHintProvider{
					map[string][]TopologyHint{
						"resource": {
							{
								NUMANodeAffinity: NewTestBitMask(0),
								Preferred:        true,
							},
							{
								NUMANodeAffinity: NewTestBitMask(0, 1),
								Preferred:        false,
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name:     "QOSClass set as Guaranteed. BestEffort Policy. More than one Preferred Affinity.",
			qosClass: v1.PodQOSGuaranteed,
			policy:   bePolicy,
			hp: []HintProvider{
				&mockHintProvider{
					map[string][]TopologyHint{
						"resource": {
							{
								NUMANodeAffinity: NewTestBitMask(0),
								Preferred:        true,
							},
							{
								NUMANodeAffinity: NewTestBitMask(1),
								Preferred:        true,
							},
							{
								NUMANodeAffinity: NewTestBitMask(0, 1),
								Preferred:        false,
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name:     "QOSClass set as Burstable. BestEffort Policy. More than one Preferred Affinity.",
			qosClass: v1.PodQOSBurstable,
			policy:   bePolicy,
			hp: []HintProvider{
				&mockHintProvider{
					map[string][]TopologyHint{
						"resource": {
							{
								NUMANodeAffinity: NewTestBitMask(0),
								Preferred:        true,
							},
							{
								NUMANodeAffinity: NewTestBitMask(1),
								Preferred:        true,
							},
							{
								NUMANodeAffinity: NewTestBitMask(0, 1),
								Preferred:        false,
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name:     "QOSClass set as Guaranteed. BestEffort Policy. No Preferred Affinity.",
			qosClass: v1.PodQOSGuaranteed,
			policy:   bePolicy,
			hp: []HintProvider{
				&mockHintProvider{
					map[string][]TopologyHint{
						"resource": {
							{
								NUMANodeAffinity: NewTestBitMask(0, 1),
								Preferred:        false,
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name:     "QOSClass set as Guaranteed. Restricted Policy. Preferred Affinity.",
			qosClass: v1.PodQOSGuaranteed,
			policy:   restrictedPolicy,
			hp: []HintProvider{
				&mockHintProvider{
					map[string][]TopologyHint{
						"resource": {
							{
								NUMANodeAffinity: NewTestBitMask(0),
								Preferred:        true,
							},
							{
								NUMANodeAffinity: NewTestBitMask(0, 1),
								Preferred:        false,
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name:     "QOSClass set as Burstable. Restricted Policy. Preferred Affinity.",
			qosClass: v1.PodQOSBurstable,
			policy:   restrictedPolicy,
			hp: []HintProvider{
				&mockHintProvider{
					map[string][]TopologyHint{
						"resource": {
							{
								NUMANodeAffinity: NewTestBitMask(0),
								Preferred:        true,
							},
							{
								NUMANodeAffinity: NewTestBitMask(0, 1),
								Preferred:        false,
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name:     "QOSClass set as Guaranteed. Restricted Policy. More than one Preferred affinity.",
			qosClass: v1.PodQOSGuaranteed,
			policy:   restrictedPolicy,
			hp: []HintProvider{
				&mockHintProvider{
					map[string][]TopologyHint{
						"resource": {
							{
								NUMANodeAffinity: NewTestBitMask(0),
								Preferred:        true,
							},
							{
								NUMANodeAffinity: NewTestBitMask(1),
								Preferred:        true,
							},
							{
								NUMANodeAffinity: NewTestBitMask(0, 1),
								Preferred:        false,
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name:     "QOSClass set as Burstable. Restricted Policy. More than one Preferred affinity.",
			qosClass: v1.PodQOSBurstable,
			policy:   restrictedPolicy,
			hp: []HintProvider{
				&mockHintProvider{
					map[string][]TopologyHint{
						"resource": {
							{
								NUMANodeAffinity: NewTestBitMask(0),
								Preferred:        true,
							},
							{
								NUMANodeAffinity: NewTestBitMask(1),
								Preferred:        true,
							},
							{
								NUMANodeAffinity: NewTestBitMask(0, 1),
								Preferred:        false,
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name:     "QOSClass set as Guaranteed. Restricted Policy. No Preferred affinity.",
			qosClass: v1.PodQOSGuaranteed,
			policy:   restrictedPolicy,
			hp: []HintProvider{
				&mockHintProvider{
					map[string][]TopologyHint{
						"resource": {
							{
								NUMANodeAffinity: NewTestBitMask(0, 1),
								Preferred:        false,
							},
						},
					},
				},
			},
			expected: false,
		},
		{
			name:     "QOSClass set as Burstable. Restricted Policy. No Preferred affinity.",
			qosClass: v1.PodQOSBurstable,
			policy:   restrictedPolicy,
			hp: []HintProvider{
				&mockHintProvider{
					map[string][]TopologyHint{
						"resource": {
							{
								NUMANodeAffinity: NewTestBitMask(0, 1),
								Preferred:        false,
							},
						},
					},
				},
			},
			expected: false,
		},
	}
	for _, tc := range tcases {
		// 同一组用例分别在容器 scope 与 Pod scope 下执行，确保两种 scope 行为一致。
		ctnScopeManager := manager{}
		ctnScopeManager.scope = NewContainerScope(tc.policy)
		ctnScopeManager.scope.(*containerScope).hintProviders = tc.hp

		podScopeManager := manager{}
		podScopeManager.scope = NewPodScope(tc.policy)
		podScopeManager.scope.(*podScope).hintProviders = tc.hp

		pod := &v1.Pod{
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Resources: v1.ResourceRequirements{},
					},
				},
			},
			Status: v1.PodStatus{
				QOSClass: tc.qosClass,
			},
		}

		podAttr := lifecycle.PodAdmitAttributes{
			Pod: pod,
		}

		// 容器 scope 的准入结果。
		ctnActual := ctnScopeManager.Admit(tCtx, &podAttr)
		if ctnActual.Admit != tc.expected {
			t.Errorf("Error occurred, expected Admit in result to be %v got %v", tc.expected, ctnActual.Admit)
		}
		if !ctnActual.Admit && ctnActual.Reason != ErrorTopologyAffinity {
			t.Errorf("Error occurred, expected Reason in result to be %v got %v", ErrorTopologyAffinity, ctnActual.Reason)
		}

		// Pod scope 的准入结果。
		podActual := podScopeManager.Admit(tCtx, &podAttr)
		if podActual.Admit != tc.expected {
			t.Errorf("Error occurred, expected Admit in result to be %v got %v", tc.expected, podActual.Admit)
		}
		if !ctnActual.Admit && ctnActual.Reason != ErrorTopologyAffinity {
			t.Errorf("Error occurred, expected Reason in result to be %v got %v", ErrorTopologyAffinity, ctnActual.Reason)
		}
	}
}

// trackingHintProvider 记录各回调是否被调用，用于断言不同特性组合
// 实际走的代码路径。
type trackingHintProvider struct {
	podHintsCalled          bool
	containerHintsCalled    bool
	allocatePodCalled       bool
	allocateContainerCalled bool
	hints                   map[string][]TopologyHint
}

// GetTopologyHints 标记容器级提示查询被调用，并返回预置提示。
func (m *trackingHintProvider) GetTopologyHints(logger klog.Logger, pod *v1.Pod, container *v1.Container, operation lifecycle.Operation) map[string][]TopologyHint {
	m.containerHintsCalled = true
	return m.hints
}

// GetPodTopologyHints 标记 Pod 级提示查询被调用，并返回预置提示。
func (m *trackingHintProvider) GetPodTopologyHints(logger klog.Logger, pod *v1.Pod, operation lifecycle.Operation) map[string][]TopologyHint {
	m.podHintsCalled = true
	return m.hints
}

// AllocatePod 标记 Pod 级分配被调用，并返回成功。
func (m *trackingHintProvider) AllocatePod(logger klog.Logger, pod *v1.Pod, operation lifecycle.Operation) error {
	m.allocatePodCalled = true
	return nil
}

// Allocate 标记容器级分配被调用，并返回成功。
func (m *trackingHintProvider) Allocate(ctx context.Context, pod *v1.Pod, container *v1.Container, operation lifecycle.Operation) error {
	m.allocateContainerCalled = true
	return nil
}

// TestAdmitWithPodLevelResources 校验 PodLevelResources 与 PodLevelResourceManagers
// 两个门控组合时，Pod scope 选择 Pod 级还是容器级流程。
func TestAdmitWithPodLevelResources(t *testing.T) {
	numaInfo := &NUMAInfo{
		Nodes: []int{0, 1},
		NUMADistances: NUMADistances{
			0: {10, 11},
			1: {11, 10},
		},
	}
	opts := PolicyOptions{}
	restrictedPolicy := NewRestrictedPolicy(numaInfo, opts)

	tcases := []struct {
		name                            string
		podLevelResourcesEnabled        bool
		podLevelResourceManagersEnabled bool
		pod                             *v1.Pod
		expectedAdmit                   bool
		expectedPodHintsCalled          bool
		expectedContainerHintsCalled    bool
		expectedAllocatePodCalled       bool
		expectedAllocateContainerCalled bool
		scope                           Scope
	}{
		{
			name:                            "pod scope, feature disabled, falls back to container level flow",
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: false,
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Resources:  &v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")}},
					Containers: []v1.Container{{Name: "c1", Resources: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")}}}},
				},
				Status: v1.PodStatus{QOSClass: v1.PodQOSGuaranteed},
			},
			expectedAdmit:                   true,
			expectedPodHintsCalled:          true,
			expectedContainerHintsCalled:    false,
			expectedAllocatePodCalled:       false,
			expectedAllocateContainerCalled: true,
			scope:                           NewPodScope(restrictedPolicy),
		},
		{
			name:                            "pod scope, feature enabled, uses pod-level flow",
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: true,
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Resources:  &v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")}},
					Containers: []v1.Container{{Name: "c1", Resources: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")}}}},
				},
				Status: v1.PodStatus{QOSClass: v1.PodQOSGuaranteed},
			},
			expectedAdmit:                   true,
			expectedPodHintsCalled:          true,
			expectedContainerHintsCalled:    false,
			expectedAllocatePodCalled:       true,
			expectedAllocateContainerCalled: false,
			scope:                           NewPodScope(restrictedPolicy),
		},
		{
			name:                            "container scope, feature enabled, uses container-level flow",
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: true,
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Resources:  &v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")}},
					Containers: []v1.Container{{Name: "c1", Resources: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")}}}},
				},
				Status: v1.PodStatus{QOSClass: v1.PodQOSGuaranteed},
			},
			expectedAdmit:                   true,
			expectedPodHintsCalled:          false,
			expectedContainerHintsCalled:    true,
			expectedAllocatePodCalled:       false,
			expectedAllocateContainerCalled: true,
			scope:                           NewContainerScope(restrictedPolicy),
		},
		{
			name:                            "container scope, feature disabled, uses container-level flow",
			podLevelResourcesEnabled:        true,
			podLevelResourceManagersEnabled: false,
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Resources:  &v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")}},
					Containers: []v1.Container{{Name: "c1", Resources: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")}}}},
				},
				Status: v1.PodStatus{QOSClass: v1.PodQOSGuaranteed},
			},
			expectedAdmit:                   true,
			expectedPodHintsCalled:          false,
			expectedContainerHintsCalled:    true,
			expectedAllocatePodCalled:       false,
			expectedAllocateContainerCalled: true,
			scope:                           NewContainerScope(restrictedPolicy),
		},
	}

	for _, tc := range tcases {
		t.Run(tc.name, func(t *testing.T) {
			// 每个子测试独立设置门控，避免特性状态在用例之间泄漏。
			tCtx := ktesting.Init(t)
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodLevelResources, tc.podLevelResourcesEnabled)
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodLevelResourceManagers, tc.podLevelResourceManagersEnabled)

			// 每个子测试新建 tracker，避免调用标记在用例间复用。
			tracker := &trackingHintProvider{
				hints: map[string][]TopologyHint{
					"resource": {
						{NUMANodeAffinity: NewTestBitMask(0), Preferred: true},
					},
				},
			}

			m := manager{scope: tc.scope}
			tc.scope.AddHintProvider(tCtx.Logger(), tracker)

			podAttr := lifecycle.PodAdmitAttributes{Pod: tc.pod}
			actual := m.Admit(tCtx, &podAttr)

			if actual.Admit != tc.expectedAdmit {
				t.Errorf("Expected Admit to be %v got %v", tc.expectedAdmit, actual.Admit)
			}
			if tracker.podHintsCalled != tc.expectedPodHintsCalled {
				t.Errorf("Expected podHintsCalled to be %v got %v", tc.expectedPodHintsCalled, tracker.podHintsCalled)
			}
			if tracker.containerHintsCalled != tc.expectedContainerHintsCalled {
				t.Errorf("Expected containerHintsCalled to be %v got %v", tc.expectedContainerHintsCalled, tracker.containerHintsCalled)
			}
			if tracker.allocatePodCalled != tc.expectedAllocatePodCalled {
				t.Errorf("Expected allocatePodCalled to be %v got %v", tc.expectedAllocatePodCalled, tracker.allocatePodCalled)
			}
			if tracker.allocateContainerCalled != tc.expectedAllocateContainerCalled {
				t.Errorf("Expected allocateContainerCalled to be %v got %v", tc.expectedAllocateContainerCalled, tracker.allocateContainerCalled)
			}
		})
	}
}
