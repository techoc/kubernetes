//go:build windows

/*
Copyright 2024 The Kubernetes Authors.

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

package winstats

import (
	"fmt"
	"syscall"
	"unsafe"

	cadvisorapi "github.com/google/cadvisor/lib/model"

	"k8s.io/klog/v2"
)

// 通过 kernel32 暴露的 Windows 处理器/内存拓扑相关系统调用。
var (
	procGetLogicalProcessorInformationEx = modkernel32.NewProc("GetLogicalProcessorInformationEx")
	getNumaAvailableMemoryNodeEx         = modkernel32.NewProc("GetNumaAvailableMemoryNodeEx")
	procGetNumaNodeProcessorMask2        = modkernel32.NewProc("GetNumaNodeProcessorMask2")
	procGetMaximumProcessorGroupCount    = modkernel32.NewProc("GetMaximumProcessorGroupCount")
)

// relationType 对应 Windows 的 LOGICAL_PROCESSOR_RELATIONSHIP 枚举值。
type relationType int

const (
	// Windows 逻辑处理器关系类型，取值与 winnt.h 中的定义保持一致。
	relationProcessorCore relationType = iota
	relationNumaNode
	relationCache
	relationProcessorPackage
	relationGroup
	relationProcessorDie
	relationNumaNodeEx
	relationProcessorModule
	relationAll = 0xffff
)

// systemLogicalProcessorInformationEx 对应 SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX 结构体。
type systemLogicalProcessorInformationEx struct {
	Relationship uint32
	Size         uint32
	data         interface{}
}

// processorRelationship 对应 PROCESSOR_RELATIONSHIP 结构体。
type processorRelationship struct {
	Flags           byte
	EfficiencyClass byte
	Reserved        [20]byte
	GroupCount      uint16
	// groupMasks 是 []GroupAffinity。在 C++ 中它是根据 GroupCount 决定单个或多个
	// GroupAffinity 的联合体。
	GroupMasks interface{}
}

// GroupAffinity 表示 CPU 的处理器组亲和性。
// https://learn.microsoft.com/en-us/windows/win32/api/winnt/ns-winnt-group_affinity
type GroupAffinity struct {
	Mask     uint64
	Group    uint16
	Reserved [3]uint16
}

// MaskString 以 0/1 字符串形式返回亲和性掩码。
func (a GroupAffinity) MaskString() string {
	return fmt.Sprintf("%064b", a.Mask)
}

// Processors 返回亲和性掩码中处理器 ID 的列表。
// Windows 本身不按 ID 跟踪处理器，kubelet 将其转换为编号（组号*64 + 组内位号）。
func (a GroupAffinity) Processors() []int {
	processors := []int{}
	for i := 0; i < 64; i++ {
		if a.Mask&(1<<i) != 0 {
			// 组内位号加上组基址（group*64）得到全局处理器编号。
			processors = append(processors, i+(int(a.Group)*64))
		}
	}
	return processors
}

// CpusToGroupAffinity 将 CPU 列表转换为按 Windows CPU 组划分的 GroupAffinity 映射。
// Windows 不按 ID 跟踪处理器，而 kubelet 将其转换为编号；本函数执行相反的转换。
func CpusToGroupAffinity(cpus []int) map[int]*GroupAffinity {
	groupAffinities := make(map[int]*GroupAffinity)
	for _, cpu := range cpus {
		// 每 64 个逻辑处理器构成一个处理器组。
		group := uint16(cpu / 64)

		groupAffinity, ok := groupAffinities[int(group)]
		if !ok {
			groupAffinity = &GroupAffinity{
				Group: group,
			}
			groupAffinities[int(group)] = groupAffinity
		}
		// 同一组内的 CPU 按位或进该组的掩码。
		mask := uint64(1 << (cpu % 64))
		groupAffinity.Mask |= mask
	}
	return groupAffinities
}

// GetCPUsforNUMANode 查询属于指定 NUMA 节点的 CPU。
// 使用 GetNumaNodeProcessorMask2，可正确返回跨多个处理器组的 NUMA 节点的全部组
// （超过 64 个逻辑处理器）；需要 Windows Server 2022+（Build 20348+）。
func GetCPUsforNUMANode(nodeNumber uint16) ([]GroupAffinity, error) {
	// 先查询处理器组的最大数量，用于确定缓冲区大小。
	r1, _, _ := procGetMaximumProcessorGroupCount.Call()
	maxGroupCount := uint16(r1)
	// 至少分配一个组，防止组数为 0 时缓冲区为空。
	if maxGroupCount == 0 {
		maxGroupCount = 1
	}

	masks := make([]GroupAffinity, maxGroupCount)
	var requiredCount uint16

	r1, _, err := procGetNumaNodeProcessorMask2.Call(
		uintptr(nodeNumber),
		uintptr(unsafe.Pointer(&masks[0])),
		uintptr(maxGroupCount),
		uintptr(unsafe.Pointer(&requiredCount)),
	)
	if r1 == 0 {
		return nil, fmt.Errorf("GetNumaNodeProcessorMask2 failed for NUMA node %d: %v", nodeNumber, err)
	}

	// requiredCount 是实际写入的组数量，据此截断结果。
	return masks[:requiredCount], nil
}

// numaNodeRelationship 对应 NUMA_NODE_RELATIONSHIP 结构体。
type numaNodeRelationship struct {
	NodeNumber uint32
	Reserved   [18]byte
	GroupCount uint16
	GroupMasks interface{} // []GroupAffinity；在 C++ 中它是根据 GroupCount 决定单个或多个 GroupAffinity 的联合体。
}

// processor 记录逻辑处理器所属的物理核、插槽与 NUMA 节点。
type processor struct {
	CoreID   int
	SocketID int
	NodeID   int
}

// processorInfo 查询 Windows 处理器拓扑，返回核数、插槽数以及 cadvisor 的 NUMA 节点列表。
func processorInfo(logger klog.Logger, relationShip relationType) (int, int, []cadvisorapi.Node, error) {
	// 首次调用传空缓冲区，以获取所需的数据长度。
	var returnLength uint32 = 0
	r1, _, err := procGetLogicalProcessorInformationEx.Call(
		uintptr(relationShip),
		uintptr(0),
		uintptr(unsafe.Pointer(&returnLength)),
	)
	// 第一次调用预期返回 ERROR_INSUFFICIENT_BUFFER，其它错误才视为失败。
	if r1 != 0 && err.(syscall.Errno) != syscall.ERROR_INSUFFICIENT_BUFFER {
		return 0, 0, nil, fmt.Errorf("call to GetLogicalProcessorInformationEx failed: %v", err)
	}

	// 按所需长度分配缓冲区。
	buffer := make([]byte, returnLength)

	// 再次调用以获取实际的拓扑信息。
	r1, _, err = procGetLogicalProcessorInformationEx.Call(
		uintptr(relationShip),
		uintptr(unsafe.Pointer(&buffer[0])),
		uintptr(unsafe.Pointer(&returnLength)),
	)
	if r1 == 0 {
		return 0, 0, nil, fmt.Errorf("call to GetLogicalProcessorInformationEx failed: %v", err)
	}

	return convertWinApiToCadvisorApi(logger, buffer)
}

// convertWinApiToCadvisorApi 解析 GetLogicalProcessorInformationEx 返回的缓冲区，
// 构建 cadvisor 的 NUMA 节点、核与插槽信息。
func convertWinApiToCadvisorApi(logger klog.Logger, buffer []byte) (int, int, []cadvisorapi.Node, error) {
	logicalProcessors := make(map[int]*processor)
	numofSockets := 0
	numOfcores := 0
	nodes := []cadvisorapi.Node{}
	for offset := 0; offset < len(buffer); {
		// 先按固定头部大小检查剩余缓冲区，避免越界读取（此时还不知道实际结构大小）。
		if offset+int(unsafe.Sizeof(systemLogicalProcessorInformationEx{})) > len(buffer) {
			return 0, 0, nil, fmt.Errorf("remaining buffer too small while reading windows processor relationship")
		}
		info := (*systemLogicalProcessorInformationEx)(unsafe.Pointer(&buffer[offset]))
		// 已知结构大小后再检查一次，确保整个结构都在缓冲区范围内。
		if offset+int(info.Size) > len(buffer) {
			return 0, 0, nil, fmt.Errorf("remaining buffer too small while reading windows processor relationship")
		}
		// 根据关系类型分别解析处理器核/插槽或 NUMA 节点信息。
		switch (relationType)(info.Relationship) {
		case relationProcessorCore, relationProcessorPackage:
			relationship := (*processorRelationship)(unsafe.Pointer(&info.data))
			groupMasks := make([]GroupAffinity, relationship.GroupCount)
			for i := 0; i < int(relationship.GroupCount); i++ {
				groupMasks[i] = *(*GroupAffinity)(unsafe.Pointer(uintptr(unsafe.Pointer(&relationship.GroupMasks)) + uintptr(i)*unsafe.Sizeof(GroupAffinity{})))
			}

			if relationProcessorCore == (relationType)(info.Relationship) {
				numOfcores++
			}

			if relationProcessorPackage == (relationType)(info.Relationship) {
				numofSockets++
			}

			// 遍历组掩码，把每个处理器加入映射。
			for _, groupMask := range groupMasks {
				for _, processorId := range groupMask.Processors() {
					p, ok := logicalProcessors[processorId]
					if !ok {
						p = &processor{}
						logicalProcessors[processorId] = p
					}
					if relationProcessorCore == (relationType)(info.Relationship) {
						p.CoreID = numOfcores
					}
					if relationProcessorPackage == (relationType)(info.Relationship) {
						p.SocketID = numofSockets
					}
				}
			}

		case relationNumaNode, relationNumaNodeEx:
			numaNodeRelationship := (*numaNodeRelationship)(unsafe.Pointer(&info.data))
			groupMasks := make([]GroupAffinity, numaNodeRelationship.GroupCount)
			for i := 0; i < int(numaNodeRelationship.GroupCount); i++ {
				groupMasks[i] = *(*GroupAffinity)(unsafe.Pointer(uintptr(unsafe.Pointer(&numaNodeRelationship.GroupMasks)) + uintptr(i)*unsafe.Sizeof(GroupAffinity{})))
			}

			nodes = append(nodes, cadvisorapi.Node{Id: int(numaNodeRelationship.NodeNumber)})

			for _, groupMask := range groupMasks {
				for processorId := range groupMask.Processors() {
					p, ok := logicalProcessors[processorId]
					if !ok {
						p = &processor{}
						logicalProcessors[processorId] = p
					}
					p.NodeID = int(numaNodeRelationship.NodeNumber)
				}
			}

		default:
			// 缓存、处理器组等关系类型不影响拓扑计算，仅在详细日志中记录。
			logger.V(4).Info("Not using Windows CPU relationship type", "relationship", info.Relationship)
		}

		// 按结构自身大小前进到下一个条目。
		offset += int(info.Size)
	}

	// 逻辑处理器按 ID 汇总后，再回填其 NUMA 节点内存与核心信息。
	for processId, p := range logicalProcessors {
		node := nodes[p.NodeID]
		// 校验处理器所属 NUMA 节点与节点列表中的 ID 一致，防止解析错位。
		if node.Id != p.NodeID {
			return 0, 0, nil, fmt.Errorf("node ID mismatch: %d != %d", node.Id, p.NodeID)
		}
		availableBytes := uint64(0)
		r1, _, err := getNumaAvailableMemoryNodeEx.Call(uintptr(p.NodeID), uintptr(unsafe.Pointer(&availableBytes)))
		if r1 == 0 {
			return 0, 0, nil, fmt.Errorf("call to GetNumaAvailableMemoryNodeEx failed: %v", err)
		}
		node.Memory = availableBytes
		node.AddThread(processId, p.CoreID)
		ok, coreIdx := node.FindCore(p.CoreID)
		if !ok {
			return 0, 0, nil, fmt.Errorf("core not found: %d", p.CoreID)
		}
		node.Cores[coreIdx].SocketID = p.SocketID
		nodes[p.NodeID] = node
	}

	return numOfcores, numofSockets, nodes, nil
}
