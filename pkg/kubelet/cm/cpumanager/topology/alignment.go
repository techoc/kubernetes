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

package topology

import (
	"fmt"

	"k8s.io/utils/cpuset"
)

// Alignment 是 cpuset 分配的元数据。
type Alignment struct {
	// UncoreCache 表示所有 CPU 是否在 Uncore Cache 上对齐，
	// 即是否共享同一个 Uncore cache 块。
	// 若分配的 CPU 数量超过一个 Uncore Group 的大小，则无法对齐；否则可以对齐。
	// 该标志只表示对齐情况，不代表是否存在干扰。
	UncoreCache bool
}

// String 返回 Alignment 的可读字符串表示。
func (ca Alignment) String() string {
	return fmt.Sprintf("aligned=<uncore:%v>", ca.UncoreCache)
}

// Allocation 表示 CPU 集合及其对齐元数据。
type Allocation struct {
	CPUs    cpuset.CPUSet
	Aligned Alignment
}

// String 返回 CPU 集合与对齐信息的可读字符串表示。
func (ca Allocation) String() string {
	return ca.CPUs.String() + " " + ca.Aligned.String()
}

// EmptyAllocation 返回零值的 CPU 分配。注意：空 cpuset 在我们考虑的所有维度上
// 都视为已对齐。
func EmptyAllocation() Allocation {
	return Allocation{
		CPUs: cpuset.New(),
		Aligned: Alignment{
			UncoreCache: true,
		},
	}
}

// isAlignedAtUncoreCache 判断给定 CPU 是否全部位于同一个 Uncore Cache 块内；
// 单个 CPU 视为天然对齐。
func isAlignedAtUncoreCache(topo *CPUTopology, cpuList ...int) bool {
	if len(cpuList) <= 1 {
		return true
	}
	reference, ok := topo.CPUDetails[cpuList[0]]
	if !ok {
		return false
	}
	for _, cpu := range cpuList[1:] {
		info, ok := topo.CPUDetails[cpu]
		if !ok {
			return false
		}
		if info.UncoreCacheID != reference.UncoreCacheID {
			return false
		}
	}
	return true
}
