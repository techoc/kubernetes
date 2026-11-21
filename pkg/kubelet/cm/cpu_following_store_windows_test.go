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

package cm

import (
	"testing"

	cadvisorapi "github.com/google/cadvisor/lib/model"

	"k8s.io/klog/v2"
	"k8s.io/utils/cpuset"

	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager/bitmask"
)

// cpuFollowingFakeCPUManager 实现 cpumanager.Manager，但只有 GetExclusiveCPUs 有意义；
// 其余方法由内嵌的 nil 接口满足，被测代码不得调用（调用会 panic）。
type cpuFollowingFakeCPUManager struct {
	cpumanager.Manager
	exclusive map[string]cpuset.CPUSet
}

// GetExclusiveCPUs 返回预设的独占 CPU 集合，未配置时返回空集合。
func (f *cpuFollowingFakeCPUManager) GetExclusiveCPUs(podUID, containerName string) cpuset.CPUSet {
	if cpus, ok := f.exclusive[podUID+"/"+containerName]; ok {
		return cpus
	}
	return cpuset.New()
}

// cpuFollowingFakeStore 是最小的 topologymanager.Store 桩，总是返回预设 hint。
type cpuFollowingFakeStore struct {
	base topologymanager.TopologyHint
}

// GetAffinity 返回预设 hint。
func (f *cpuFollowingFakeStore) GetAffinity(_ klog.Logger, podUID, containerName string) topologymanager.TopologyHint {
	return f.base
}

// GetPolicy 返回 nil。
func (f *cpuFollowingFakeStore) GetPolicy() topologymanager.Policy { return nil }

// Name 返回桩名称。
func (f *cpuFollowingFakeStore) Name() string { return "fake" }

// cpuFollowingMustBitMask 构造位掩码，失败时终止测试。
func cpuFollowingMustBitMask(t *testing.T, bits ...int) bitmask.BitMask {
	t.Helper()
	m, err := bitmask.NewBitMask(bits...)
	if err != nil {
		t.Fatalf("NewBitMask(%v): %v", bits, err)
	}
	return m
}

// cpuFollowingTestMachineInfo 描述两个 NUMA 节点：CPU 0-3 在节点 0，CPU 4-7 在节点 1。
func cpuFollowingTestMachineInfo() *cadvisorapi.MachineInfo {
	return &cadvisorapi.MachineInfo{
		Topology: []cadvisorapi.Node{
			{Id: 0, Cores: []cadvisorapi.Core{
				{Id: 0, Threads: []int{0, 1}},
				{Id: 1, Threads: []int{2, 3}},
			}},
			{Id: 1, Cores: []cadvisorapi.Core{
				{Id: 2, Threads: []int{4, 5}},
				{Id: 3, Threads: []int{6, 7}},
			}},
		},
	}
}

// TestCPUFollowingStoreGetAffinity 验证 GetAffinity 在跟随 CPU manager
// 与回退基础 hint 之间的行为。
func TestCPUFollowingStoreGetAffinity(t *testing.T) {
	// base 是 Topology Manager 自己的 hint，故意选在节点 {1}，
	// 与独占 CPU 推导出的掩码不同，便于区分"跟随"与"回退"。
	baseHint := topologymanager.TopologyHint{NUMANodeAffinity: cpuFollowingMustBitMask(t, 1), Preferred: true}

	// 用例覆盖单节点、跨节点、部分映射、无独占 CPU、无法映射等场景。
	testCases := []struct {
		name          string
		exclusiveCPUs cpuset.CPUSet
		expectFollow  bool  // true → expect derived mask; false → expect base returned
		expectedNodes []int // only used when expectFollow is true
	}{
		{
			name:          "exclusive CPUs on a single node follows that node",
			exclusiveCPUs: cpuset.New(0, 1),
			expectFollow:  true,
			expectedNodes: []int{0},
		},
		{
			name:          "exclusive CPUs spanning two nodes follows both",
			exclusiveCPUs: cpuset.New(1, 4),
			expectFollow:  true,
			expectedNodes: []int{0, 1},
		},
		{
			name:          "exclusive CPUs partially mapped follows only the mapped node(s)",
			exclusiveCPUs: cpuset.New(0, 99), // 0 -> node 0; 99 has no NUMA node and is dropped
			expectFollow:  true,
			expectedNodes: []int{0},
		},
		{
			name:          "no exclusive CPUs (e.g. CPU policy none) falls back to base",
			exclusiveCPUs: cpuset.New(),
			expectFollow:  false,
		},
		{
			name:          "exclusive CPUs not mapped to any NUMA node falls back to base",
			exclusiveCPUs: cpuset.New(99),
			expectFollow:  false,
		},
	}

	// 逐用例构造 store 并校验返回的 NUMA 掩码与 Preferred。
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cpuMgr := &cpuFollowingFakeCPUManager{exclusive: map[string]cpuset.CPUSet{"pod/ctr": tc.exclusiveCPUs}}
			store := newCPUFollowingStore(&cpuFollowingFakeStore{base: baseHint}, cpuMgr, cpuFollowingTestMachineInfo())

			got := store.GetAffinity(klog.Background(), "pod", "ctr")

			if !tc.expectFollow {
				if !got.NUMANodeAffinity.IsEqual(baseHint.NUMANodeAffinity) {
					t.Errorf("expected fall back to base %v, got %v", baseHint.NUMANodeAffinity, got.NUMANodeAffinity)
				}
				return
			}

			want := cpuFollowingMustBitMask(t, tc.expectedNodes...)
			if got.NUMANodeAffinity == nil || !got.NUMANodeAffinity.IsEqual(want) {
				t.Errorf("expected NUMA affinity %v, got %v", want, got.NUMANodeAffinity)
			}
			if got.Preferred != baseHint.Preferred {
				t.Errorf("expected Preferred %v (carried from base), got %v", baseHint.Preferred, got.Preferred)
			}
		})
	}
}

// TestCPUFollowingStoreIsHintAuthoritative 验证只有拥有独占 CPU 的容器
// 才被视为权威 hint。
func TestCPUFollowingStoreIsHintAuthoritative(t *testing.T) {
	cpuMgr := &cpuFollowingFakeCPUManager{exclusive: map[string]cpuset.CPUSet{
		"pod/with":    cpuset.New(0, 1),
		"pod/without": cpuset.New(),
	}}
	store := newCPUFollowingStore(&cpuFollowingFakeStore{}, cpuMgr, cpuFollowingTestMachineInfo())

	if !store.IsHintAuthoritative("pod", "with") {
		t.Errorf("expected IsHintAuthoritative=true for a container with exclusive CPUs")
	}
	if store.IsHintAuthoritative("pod", "without") {
		t.Errorf("expected IsHintAuthoritative=false for a container with an empty CPU set")
	}
	if store.IsHintAuthoritative("pod", "missing") {
		t.Errorf("expected IsHintAuthoritative=false for an unknown container")
	}
}
