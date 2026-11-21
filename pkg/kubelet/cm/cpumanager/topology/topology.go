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

package topology

import (
	"fmt"

	cadvisorapi "github.com/google/cadvisor/lib/model"
	"k8s.io/klog/v2"
	"k8s.io/utils/cpuset"
)

// NUMANodeInfo 是从 NUMA 节点 ID 到该节点所含 CPU ID 集合的映射。
type NUMANodeInfo map[int]cpuset.CPUSet

// CPUDetails 是从 CPU ID 到其核心、插槽、NUMA 节点与 Uncore 缓存信息的映射。
type CPUDetails map[int]CPUInfo

// CPUTopology 描述节点 CPU 的拓扑结构，术语对应关系如下：
// CPU         - 逻辑 CPU，对应 cadvisor 的 thread
// Core        - 物理 CPU，对应 cadvisor 的 Core
// Socket      - 插槽，对应 cadvisor 的 Socket
// NUMA Node   - NUMA 单元，对应 cadvisor 的 Node
// UncoreCache - 分片的 L3 缓存拓扑，对应 cadvisor 的 UncoreCache
type CPUTopology struct {
	NumCPUs        int
	NumCores       int
	NumUncoreCache int
	NumSockets     int
	NumNUMANodes   int
	CPUDetails     CPUDetails
}

// CPUsPerCore 返回每个物理核心关联的逻辑 CPU 数量。
func (topo *CPUTopology) CPUsPerCore() int {
	if topo.NumCores == 0 {
		return 0
	}
	return topo.NumCPUs / topo.NumCores
}

// CPUsPerSocket 返回每个插槽关联的逻辑 CPU 数量。
func (topo *CPUTopology) CPUsPerSocket() int {
	if topo.NumSockets == 0 {
		return 0
	}
	return topo.NumCPUs / topo.NumSockets
}

// CPUsPerUncore 返回每个 Uncore 缓存关联的逻辑 CPU 数量。
func (topo *CPUTopology) CPUsPerUncore() int {
	if topo.NumUncoreCache == 0 {
		return 0
	}
	return topo.NumCPUs / topo.NumUncoreCache
}

// CPUCoreID 返回给定逻辑 CPU 所属的物理核心 ID。
func (topo *CPUTopology) CPUCoreID(cpu int) (int, error) {
	info, ok := topo.CPUDetails[cpu]
	if !ok {
		return -1, fmt.Errorf("unknown CPU ID: %d", cpu)
	}
	return info.CoreID, nil
}

// CPUSocketID 返回给定逻辑 CPU 所属的插槽 ID。
func (topo *CPUTopology) CPUSocketID(cpu int) (int, error) {
	info, ok := topo.CPUDetails[cpu]
	if !ok {
		return -1, fmt.Errorf("unknown CPU ID: %d", cpu)
	}
	return info.SocketID, nil
}

// CPUNUMANodeID 返回给定逻辑 CPU 所属的 NUMA 节点 ID。
func (topo *CPUTopology) CPUNUMANodeID(cpu int) (int, error) {
	info, ok := topo.CPUDetails[cpu]
	if !ok {
		return -1, fmt.Errorf("unknown CPU ID: %d", cpu)
	}
	return info.NUMANodeID, nil
}

// CheckAlignment 结合当前 CPU 拓扑，返回给定 CPU 集合的对齐信息。
func (topo *CPUTopology) CheckAlignment(cpus cpuset.CPUSet) Alignment {
	cpuList := cpus.UnsortedList()
	return Alignment{
		UncoreCache: isAlignedAtUncoreCache(topo, cpuList...),
	}
}

// CPUInfo 保存某个 CPU 关联的 NUMA 节点、插槽、Uncore 缓存与物理核心 ID。
type CPUInfo struct {
	NUMANodeID    int
	SocketID      int
	CoreID        int
	UncoreCacheID int
}

// KeepOnly 返回仅包含指定 CPU 的新 CPUDetails。
func (d CPUDetails) KeepOnly(cpus cpuset.CPUSet) CPUDetails {
	result := CPUDetails{}
	for cpu, info := range d {
		if cpus.Contains(cpu) {
			result[cpu] = info
		}
	}
	return result
}

// UncoreCaches 返回该 CPUDetails 中所有 CPU 关联的 Uncore 缓存 ID（L3 索引）。
func (d CPUDetails) UncoreCaches() cpuset.CPUSet {
	var numUnCoreIDs []int
	for _, info := range d {
		numUnCoreIDs = append(numUnCoreIDs, info.UncoreCacheID)
	}
	return cpuset.New(numUnCoreIDs...)
}

// UncoreInNUMANodes 返回该 CPUDetails 中与给定 NUMA 节点 ID 关联的所有 Uncore 缓存 ID。
func (d CPUDetails) UncoreInNUMANodes(ids ...int) cpuset.CPUSet {
	var unCoreIDs []int
	for _, id := range ids {
		for _, info := range d {
			if info.NUMANodeID == id {
				unCoreIDs = append(unCoreIDs, info.UncoreCacheID)
			}
		}
	}
	return cpuset.New(unCoreIDs...)
}

// CoresNeededInUncoreCache 返回与给定 Uncore 缓存 ID 关联的核心 ID：
// 若所需核心数不小于全部可用核心数，则返回全部；否则返回其中一部分。
func (d CPUDetails) CoresNeededInUncoreCache(numCoresNeeded int, ids ...int) cpuset.CPUSet {
	coreIDs := d.coresInUncoreCache(ids...)
	if coreIDs.Size() <= numCoresNeeded {
		return coreIDs
	}
	tmpCoreIDs := coreIDs.List()
	return cpuset.New(tmpCoreIDs[:numCoresNeeded]...)
}

// coresInUncoreCache 收集与给定 Uncore 缓存 ID 关联的全部核心 ID。
func (d CPUDetails) coresInUncoreCache(ids ...int) cpuset.CPUSet {
	var coreIDs []int
	for _, id := range ids {
		for _, info := range d {
			if info.UncoreCacheID == id {
				coreIDs = append(coreIDs, info.CoreID)
			}
		}
	}
	return cpuset.New(coreIDs...)
}

// CPUsInUncoreCaches 返回该 CPUDetails 中与给定 Uncore 缓存 ID 关联的所有逻辑 CPU ID。
func (d CPUDetails) CPUsInUncoreCaches(ids ...int) cpuset.CPUSet {
	var cpuIDs []int
	for _, id := range ids {
		for cpu, info := range d {
			if info.UncoreCacheID == id {
				cpuIDs = append(cpuIDs, cpu)
			}
		}
	}
	return cpuset.New(cpuIDs...)
}

// NUMANodes 返回该 CPUDetails 中所有 CPU 关联的 NUMA 节点 ID。
func (d CPUDetails) NUMANodes() cpuset.CPUSet {
	var numaNodeIDs []int
	for _, info := range d {
		numaNodeIDs = append(numaNodeIDs, info.NUMANodeID)
	}
	return cpuset.New(numaNodeIDs...)
}

// NUMANodesInSockets 返回该 CPUDetails 中与给定插槽 ID 关联的所有 NUMA 节点 ID。
func (d CPUDetails) NUMANodesInSockets(ids ...int) cpuset.CPUSet {
	var numaNodeIDs []int
	for _, id := range ids {
		for _, info := range d {
			if info.SocketID == id {
				numaNodeIDs = append(numaNodeIDs, info.NUMANodeID)
			}
		}
	}
	return cpuset.New(numaNodeIDs...)
}

// Sockets 返回该 CPUDetails 中所有 CPU 关联的插槽 ID。
func (d CPUDetails) Sockets() cpuset.CPUSet {
	var socketIDs []int
	for _, info := range d {
		socketIDs = append(socketIDs, info.SocketID)
	}
	return cpuset.New(socketIDs...)
}

// CPUsInSockets 返回该 CPUDetails 中与给定插槽 ID 关联的所有逻辑 CPU ID。
func (d CPUDetails) CPUsInSockets(ids ...int) cpuset.CPUSet {
	var cpuIDs []int
	for _, id := range ids {
		for cpu, info := range d {
			if info.SocketID == id {
				cpuIDs = append(cpuIDs, cpu)
			}
		}
	}
	return cpuset.New(cpuIDs...)
}

// SocketsInNUMANodes 返回该 CPUDetails 中与给定 NUMA 节点 ID 关联的所有插槽 ID。
func (d CPUDetails) SocketsInNUMANodes(ids ...int) cpuset.CPUSet {
	var socketIDs []int
	for _, id := range ids {
		for _, info := range d {
			if info.NUMANodeID == id {
				socketIDs = append(socketIDs, info.SocketID)
			}
		}
	}
	return cpuset.New(socketIDs...)
}

// Cores 返回该 CPUDetails 中所有 CPU 关联的物理核心 ID。
func (d CPUDetails) Cores() cpuset.CPUSet {
	var coreIDs []int
	for _, info := range d {
		coreIDs = append(coreIDs, info.CoreID)
	}
	return cpuset.New(coreIDs...)
}

// CoresInNUMANodes 返回该 CPUDetails 中与给定 NUMA 节点 ID 关联的所有物理核心 ID。
func (d CPUDetails) CoresInNUMANodes(ids ...int) cpuset.CPUSet {
	var coreIDs []int
	for _, id := range ids {
		for _, info := range d {
			if info.NUMANodeID == id {
				coreIDs = append(coreIDs, info.CoreID)
			}
		}
	}
	return cpuset.New(coreIDs...)
}

// CoresInSockets 返回该 CPUDetails 中与给定插槽 ID 关联的所有物理核心 ID。
func (d CPUDetails) CoresInSockets(ids ...int) cpuset.CPUSet {
	var coreIDs []int
	for _, id := range ids {
		for _, info := range d {
			if info.SocketID == id {
				coreIDs = append(coreIDs, info.CoreID)
			}
		}
	}
	return cpuset.New(coreIDs...)
}

// CPUs 返回该 CPUDetails 中的全部逻辑 CPU ID。
func (d CPUDetails) CPUs() cpuset.CPUSet {
	var cpuIDs []int
	for cpuID := range d {
		cpuIDs = append(cpuIDs, cpuID)
	}
	return cpuset.New(cpuIDs...)
}

// CPUsInNUMANodes 返回该 CPUDetails 中与给定 NUMA 节点 ID 关联的所有逻辑 CPU ID。
func (d CPUDetails) CPUsInNUMANodes(ids ...int) cpuset.CPUSet {
	var cpuIDs []int
	for _, id := range ids {
		for cpu, info := range d {
			if info.NUMANodeID == id {
				cpuIDs = append(cpuIDs, cpu)
			}
		}
	}
	return cpuset.New(cpuIDs...)
}

// CPUsInCores 返回该 CPUDetails 中与给定物理核心 ID 关联的所有逻辑 CPU ID。
func (d CPUDetails) CPUsInCores(ids ...int) cpuset.CPUSet {
	var cpuIDs []int
	for _, id := range ids {
		for cpu, info := range d {
			if info.CoreID == id {
				cpuIDs = append(cpuIDs, cpu)
			}
		}
	}
	return cpuset.New(cpuIDs...)
}

// AreNUMANodesInSameSocket 判断给定的 NUMA 节点是否都位于同一个插槽内。
func (d CPUDetails) AreNUMANodesInSameSocket(numaNodes []int) bool {
	allNUMAs := d.NUMANodes()
	for _, id := range numaNodes {
		if !allNUMAs.Contains(id) {
			// 任一 NUMA 节点不在当前拓扑范围内即返回 false。
			return false
		}
	}
	return d.SocketsInNUMANodes(numaNodes...).Size() <= 1
}

// getUncoreCacheID 返回核心所属的 Uncore 缓存 ID；
// cAdvisor 未提供该信息时回退为插槽 ID。
func getUncoreCacheID(core cadvisorapi.Core) int {
	if len(core.UncoreCaches) < 1 {
		// cAdvisor 未提供 Uncore 缓存信息时回退到插槽对齐：
		// 此时按插槽作为缓存共享边界处理。
		return core.SocketID
	}
	// 虽然 cAdvisor API 返回的是切片，但预期最多只有一个 Uncore 缓存，
	// 因此第一个元素之后的内容都忽略。
	return core.UncoreCaches[0].Id
}

// Discover 基于 cadvisor 提供的节点信息构建 CPU 拓扑。
func Discover(logger klog.Logger, machineInfo *cadvisorapi.MachineInfo) (*CPUTopology, error) {
	// 无法检测到 CPU 数量时直接报错，避免返回无意义的空拓扑。
	if machineInfo.NumCores == 0 {
		return nil, fmt.Errorf("could not detect number of cpus")
	}

	CPUDetails := CPUDetails{}
	numPhysicalCores := 0

	// 遍历 cadvisor 的 NUMA 拓扑，为每个线程建立 CPU 到核心/插槽/节点的映射。
	for _, node := range machineInfo.Topology {
		numPhysicalCores += len(node.Cores)
		for _, core := range node.Cores {
			if coreID, err := getUniqueCoreID(core.Threads); err == nil {
				for _, cpu := range core.Threads {
					CPUDetails[cpu] = CPUInfo{
						CoreID:        coreID,
						SocketID:      core.SocketID,
						NUMANodeID:    node.Id,
						UncoreCacheID: getUncoreCacheID(core),
					}
				}
			} else {
				logger.Info("Could not get unique coreID for socket", "socket", core.SocketID, "core", core.Id, "threads", core.Threads)
				return nil, err
			}
		}
	}

	return &CPUTopology{
		NumCPUs:        machineInfo.NumCores,
		NumSockets:     machineInfo.NumSockets,
		NumCores:       numPhysicalCores,
		NumNUMANodes:   CPUDetails.NUMANodes().Size(),
		NumUncoreCache: CPUDetails.UncoreCaches().Size(),
		CPUDetails:     CPUDetails,
	}, nil
}

// getUniqueCoreID 以给定线程列表中的最小 CPU ID 作为核心 ID。
// 这样可保证核心 ID 在平台上唯一，弥补 cAdvisor 上报值的不足。
func getUniqueCoreID(threads []int) (coreID int, err error) {
	if len(threads) == 0 {
		return 0, fmt.Errorf("no cpus provided")
	}

	// 线程列表中存在重复时无法确定唯一核心 ID，直接报错。
	if len(threads) != cpuset.New(threads...).Size() {
		return 0, fmt.Errorf("cpus provided are not unique")
	}

	min := threads[0]
	for _, thread := range threads[1:] {
		if thread < min {
			min = thread
		}
	}

	return min, nil
}
