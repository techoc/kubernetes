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
	"testing"

	"k8s.io/kubernetes/test/utils/ktesting"

	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/state"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/utils/cpuset"
)

// TestNonePolicyName 验证 none 策略的名称固定为 "none"。
func TestNonePolicyName(t *testing.T) {
	policy := &nonePolicy{}

	policyName := policy.Name()
	if policyName != "none" {
		t.Errorf("NonePolicy Name() error. expected: none, returned: %v", policyName)
	}
}

// TestNonePolicyAllocate 验证 none 策略的 Allocate 不做任何分配且不报错。
func TestNonePolicyAllocate(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	policy := &nonePolicy{}

	st := &mockState{
		assignments:   state.ContainerCPUAssignments{},
		defaultCPUSet: cpuset.New(1, 2, 3, 4, 5, 6, 7),
	}

	testPod := makePod("fakePod", "fakeContainer", "1000m", "1000m")

	container := &testPod.Spec.Containers[0]
	err := policy.Allocate(logger, st, testPod, container, lifecycle.AddOperation)
	if err != nil {
		t.Errorf("NonePolicy Allocate(Add) error. expected no error but got: %v", err)
	}
}

// TestNonePolicyRemove 验证 none 策略的 RemoveContainer 不做任何操作且不报错。
func TestNonePolicyRemove(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	policy := &nonePolicy{}

	st := &mockState{
		assignments:   state.ContainerCPUAssignments{},
		defaultCPUSet: cpuset.New(1, 2, 3, 4, 5, 6, 7),
	}

	testPod := makePod("fakePod", "fakeContainer", "1000m", "1000m")

	container := &testPod.Spec.Containers[0]
	err := policy.RemoveContainer(logger, st, string(testPod.UID), container.Name)
	if err != nil {
		t.Errorf("NonePolicy RemoveContainer() error. expected no error but got %v", err)
	}
}

// TestNonePolicyGetAllocatableCPUs 验证 none 策略不提供可分配 CPU。
func TestNonePolicyGetAllocatableCPUs(t *testing.T) {
	// 任意拓扑均可。

	// 收集所有 CPU ID 作为默认可分配集合。
	var cpuIDs []int
	for cpuID := range topoSingleSocketHT.CPUDetails {
		cpuIDs = append(cpuIDs, cpuID)
	}

	policy := &nonePolicy{}

	st := &mockState{
		assignments:   state.ContainerCPUAssignments{},
		defaultCPUSet: cpuset.New(cpuIDs...),
	}

	cpus := policy.GetAllocatableCPUs(st)
	if cpus.Size() != 0 {
		t.Errorf("NonePolicy GetAllocatableCPUs() error. expected empty set, returned: %v", cpus)
	}
}

// TestNonePolicyOptions 验证 none 策略接受 nil 选项但拒绝任何非空选项。
func TestNonePolicyOptions(t *testing.T) {
	var err error

	// nil 选项应被接受。
	_, err = NewNonePolicy(nil)
	if err != nil {
		t.Errorf("NewNonePolicy with nil options failure. expected no error but got: %v", err)
	}

	opts := map[string]string{
		FullPCPUsOnlyOption: "true",
	}
	// none 策略不接受任何选项。
	_, err = NewNonePolicy(opts)
	if err == nil {
		t.Errorf("NewNonePolicy with (any) options failure. expected error but got none")
	}
}
