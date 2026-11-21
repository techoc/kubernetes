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

package topologymanager

import (
	"fmt"
	"runtime"

	cadvisorapi "github.com/google/cadvisor/lib/model"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager/bitmask"
)

// NUMADistances 是 NUMA 节点间的距离矩阵。
type NUMADistances map[int][]uint64

// NUMAInfo 保存 NUMA 节点列表及其距离信息。
type NUMAInfo struct {
	// Nodes 是 NUMA 节点 ID 列表。
	Nodes []int
	// NUMADistances 是节点 ID 到距离数组的映射。
	NUMADistances NUMADistances
}

// NewNUMAInfo 根据 cAdvisor 拓扑构造 NUMAInfo。
func NewNUMAInfo(topology []cadvisorapi.Node, opts PolicyOptions) (*NUMAInfo, error) {
	var numaNodes []int
	distances := map[int][]uint64{}
	// 逐个节点收集 ID；启用 PreferClosestNUMA 时同时收集距离。
	for _, node := range topology {
		numaNodes = append(numaNodes, node.Id)

		var nodeDistance []uint64
		if opts.PreferClosestNUMA {
			nodeDistance = node.Distances
			// 缺少距离信息时无法按距离选择，直接报错。
			if nodeDistance == nil {
				if runtime.GOOS == "windows" {
					return nil, fmt.Errorf("the %q policy option is not supported on Windows because NUMA node distances are not available", PreferClosestNUMANodes)
				}
				return nil, fmt.Errorf("error getting NUMA distances from cadvisor")
			}
		}
		distances[node.Id] = nodeDistance
	}

	numaInfo := &NUMAInfo{
		Nodes:         numaNodes,
		NUMADistances: distances,
	}

	return numaInfo, nil
}

// Narrowest 返回两个位掩码中更窄（置位更少）的一个。
func (n *NUMAInfo) Narrowest(m1 bitmask.BitMask, m2 bitmask.BitMask) bitmask.BitMask {
	if m1.IsNarrowerThan(m2) {
		return m1
	}
	return m2
}

// Closest 返回 NUMA 距离更近的位掩码。
func (n *NUMAInfo) Closest(m1 bitmask.BitMask, m2 bitmask.BitMask) bitmask.BitMask {
	// 两个位掩码置位数不同时，选择更窄的那个。
	if m1.Count() != m2.Count() {
		return n.Narrowest(m1, m2)
	}

	m1Distance := n.NUMADistances.CalculateAverageFor(m1)
	m2Distance := n.NUMADistances.CalculateAverageFor(m2)
	// 平均距离相同时，选择低位编号置位更多的位掩码。
	if m1Distance == m2Distance {
		if m1.IsLessThan(m2) {
			return m1
		}
		return m2
	}

	// 否则返回 NUMA 节点间平均距离更短的位掩码。
	if m1Distance < m2Distance {
		return m1
	}

	return m2
}

// DefaultAffinityMask 返回包含所有 NUMA 节点的位掩码。
func (n NUMAInfo) DefaultAffinityMask() bitmask.BitMask {
	defaultAffinity, _ := bitmask.NewBitMask(n.Nodes...)
	return defaultAffinity
}

// CalculateAverageFor 计算位掩码中所有节点两两之间的平均距离。
func (d NUMADistances) CalculateAverageFor(bm bitmask.BitMask) float64 {
	// 正常情况下不会发生，但仍需防止除零。
	if bm.Count() == 0 {
		return 0
	}

	var count float64 = 0
	var sum float64 = 0
	for _, node1 := range bm.GetBits() {
		for _, node2 := range bm.GetBits() {
			sum += float64(d[node1][node2])
			count++
		}
	}

	return sum / count
}
