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
	"testing"
	"unsafe"

	cadvisorapi "github.com/google/cadvisor/lib/model"
	"github.com/stretchr/testify/assert"

	"k8s.io/klog/v2/ktesting"
)

// TestGROUP_AFFINITY_Processors 验证 GroupAffinity.Processors 能把掩码与处理器组
// 转换为全局 CPU 编号，覆盖空掩码、跨组与满掩码等场景。
func TestGROUP_AFFINITY_Processors(t *testing.T) {
	tests := []struct {
		name  string
		Mask  uint64
		Group uint16
		want  []int
	}{
		{
			name:  "empty",
			Mask:  0,
			Group: 0,
			want:  []int{},
		},
		{
			name:  "empty group 2",
			Mask:  0,
			Group: 1,
			want:  []int{},
		},
		{
			name:  "cpu 1 Group 0",
			Mask:  1,
			Group: 0,
			want:  []int{0},
		},
		{
			name:  "cpu 64 Group 0",
			Mask:  1 << 63,
			Group: 0,
			want:  []int{63},
		},
		{
			name:  "cpu 128 Group 1",
			Mask:  1 << 63,
			Group: 1,
			want:  []int{127},
		},
		{
			name:  "cpu 128 (Group 1)",
			Mask:  1 << 63,
			Group: 1,
			want:  []int{127},
		},
		{
			name:  "Mask 1 Group 2",
			Mask:  1,
			Group: 2,
			want:  []int{128},
		},
		{
			name:  "64 cpus group 0",
			Mask:  0xffffffffffffffff,
			Group: 0,
			want:  makeRange(0, 63),
		},
		{
			name:  "64 cpus group 1",
			Mask:  0xffffffffffffffff,
			Group: 1,
			want:  makeRange(64, 127),
		},
		{
			name:  "64 cpus group 1",
			Mask:  0xffffffffffffffff,
			Group: 1,
			want:  makeRange(64, 127),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 由掩码与组号构造亲和性，再断言其展开出的 CPU 列表。
			a := GroupAffinity{
				Mask:  tt.Mask,
				Group: tt.Group,
			}
			assert.Equalf(t, tt.want, a.Processors(), "Processors()")
		})
	}
}

// makeRange 返回 [min, max] 闭区间内的整数切片，便于构造连续的 CPU 编号。
// 参考：https://stackoverflow.com/a/39868255/697126
func makeRange(min, max int) []int {
	a := make([]int, max-min+1)
	for i := range a {
		a[i] = min + i
	}
	return a
}

// TestCpusToGroupAffinity 验证 CpusToGroupAffinity 能把 CPU 编号列表按处理器组
// 聚合为组号到亲和性掩码的映射。
func TestCpusToGroupAffinity(t *testing.T) {
	tests := []struct {
		name string
		cpus []int
		want map[int]*GroupAffinity
	}{
		{
			name: "empty",
			want: map[int]*GroupAffinity{},
		},
		{
			name: "single cpu group 0",
			cpus: []int{0},
			want: map[int]*GroupAffinity{
				0: {
					Mask:  1,
					Group: 0,
				},
			},
		},
		{
			name: "single cpu group 0",
			cpus: []int{63},
			want: map[int]*GroupAffinity{
				0: {
					Mask:  1 << 63,
					Group: 0,
				},
			},
		},
		{
			name: "single cpu group 1",
			cpus: []int{64},
			want: map[int]*GroupAffinity{
				1: {
					Mask:  1,
					Group: 1,
				},
			},
		},
		{
			name: "multiple cpus same group",
			cpus: []int{0, 1, 2},
			want: map[int]*GroupAffinity{
				0: {
					Mask:  1 | 2 | 4, // 按位或合并多个 CPU 的掩码
					Group: 0,
				},
			},
		},
		{
			name: "multiple cpus different groups",
			cpus: []int{0, 64},
			want: map[int]*GroupAffinity{
				0: {
					Mask:  1,
					Group: 0,
				},
				1: {
					Mask:  1,
					Group: 1,
				},
			},
		},
		{
			name: "multiple cpus different groups",
			cpus: []int{0, 1, 2, 64, 65, 66},
			want: map[int]*GroupAffinity{
				0: {
					Mask:  1 | 2 | 4,
					Group: 0,
				},
				1: {
					Mask:  1 | 2 | 4,
					Group: 1,
				},
			},
		},
		{
			name: "64 cpus group 0",
			cpus: makeRange(0, 63),
			want: map[int]*GroupAffinity{
				0: {
					Mask:  0xffffffffffffffff, // 64 位全部置位
					Group: 0,
				},
			},
		},
		{
			name: "64 cpus group 1",
			cpus: makeRange(64, 127),
			want: map[int]*GroupAffinity{
				1: {
					Mask:  0xffffffffffffffff, // 64 位全部置位
					Group: 1,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 直接比对聚合结果，用例覆盖单组、多组与整组满掩码。
			assert.Equalf(t, tt.want, CpusToGroupAffinity(tt.cpus), "CpusToGroupAffinity(%v)", tt.cpus)
		})
	}
}

// Test_convertWinApiToCadvisorApi 验证 convertWinApiToCadvisorApi 能把 Windows 逻辑
// 处理器信息缓冲区解析为 cadvisor 的 NUMA 节点、核心与线程结构。
func Test_convertWinApiToCadvisorApi(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	tests := []struct {
		name                 string
		buffer               []byte
		expectedNumOfCores   int
		expectedNumOfSockets int
		expectedNodes        []cadvisorapi.Node
		wantErr              bool
	}{
		{
			name:                 "empty",
			buffer:               []byte{},
			expectedNumOfCores:   0,
			expectedNumOfSockets: 0,
			expectedNodes:        []cadvisorapi.Node{},
			wantErr:              false,
		},
		{
			name:                 "single core",
			buffer:               createProcessorRelationships([]int{0}),
			expectedNumOfCores:   1,
			expectedNumOfSockets: 1,
			expectedNodes: []cadvisorapi.Node{
				{
					Id: 0,
					Cores: []cadvisorapi.Core{
						{
							Id:      1,
							Threads: []int{0},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name:                 "single core, multiple cpus",
			buffer:               createProcessorRelationships([]int{0, 1, 2}),
			expectedNumOfCores:   1,
			expectedNumOfSockets: 1,
			expectedNodes: []cadvisorapi.Node{
				{
					Id: 0,
					Cores: []cadvisorapi.Core{
						{
							Id:      1,
							Threads: []int{0, 1, 2},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name:                 "single core, multiple groups",
			buffer:               createProcessorRelationships([]int{0, 64}),
			expectedNumOfCores:   1,
			expectedNumOfSockets: 1,
			expectedNodes: []cadvisorapi.Node{
				{
					Id: 0,
					Cores: []cadvisorapi.Core{
						{
							Id:      1,
							Threads: []int{0, 64},
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name:                 "multi-group NUMA node (128 cpus across 2 groups)",
			buffer:               createProcessorRelationships(makeRange(0, 127)),
			expectedNumOfCores:   1,
			expectedNumOfSockets: 1,
			expectedNodes: []cadvisorapi.Node{
				{
					Id: 0,
					Cores: []cadvisorapi.Core{
						{
							Id:      1,
							Threads: makeRange(0, 127),
						},
					},
				},
			},
			wantErr: false,
		},
		{
			name:                 "buffer to small",
			buffer:               createProcessorRelationships([]int{0, 64})[:48],
			expectedNumOfCores:   1,
			expectedNumOfSockets: 1,
			expectedNodes: []cadvisorapi.Node{
				{
					Id: 0,
					Cores: []cadvisorapi.Core{
						{
							Id:      1,
							Threads: []int{0, 64},
						},
					},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 缓冲区被截断时应返回错误，其余场景逐项比对解析结果。
			numOfCores, numOfSockets, nodes, err := convertWinApiToCadvisorApi(logger, tt.buffer)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.Equalf(t, tt.expectedNumOfCores, numOfCores, "num of cores")
			assert.Equalf(t, tt.expectedNumOfSockets, numOfSockets, "num of sockets")
			for node := range nodes {
				assert.Equalf(t, tt.expectedNodes[node].Id, nodes[node].Id, "node id")
				for core := range nodes[node].Cores {
					assert.Equalf(t, tt.expectedNodes[node].Cores[core].Id, nodes[node].Cores[core].Id, "core id")
					assert.Equalf(t, len(tt.expectedNodes[node].Cores[core].Threads), len(nodes[node].Cores[core].Threads), "num of threads")
					for _, thread := range nodes[node].Cores[core].Threads {
						assert.Truef(t, containsThread(tt.expectedNodes[node].Cores[core].Threads, thread), "thread %d", thread)
					}
				}
			}
		})
	}
}

// containsThread 判断线程列表是否包含指定线程，用于断言线程集合而非顺序。
func containsThread(threads []int, thread int) bool {
	for _, t := range threads {
		if t == thread {
			return true
		}
	}
	return false
}

// genBuffer 将多条逻辑处理器信息序列化拼接为一个字节缓冲区。
func genBuffer(infos ...systemLogicalProcessorInformationEx) []byte {
	var buffer []byte
	for _, info := range infos {
		buffer = append(buffer, structToBytes(info)...)
	}
	return buffer
}

// createProcessorRelationships 按给定 CPU 列表构造模拟的 Windows 处理器关系缓冲区，
// 包含处理器核心、NUMA 节点与处理器封装三类关系记录。
func createProcessorRelationships(cpus []int) []byte {
	groups := CpusToGroupAffinity(cpus)
	grouplen := len(groups)
	groupAffinities := make([]GroupAffinity, 0, grouplen)
	for _, group := range groups {
		groupAffinities = append(groupAffinities, *group)
	}
	return genBuffer(systemLogicalProcessorInformationEx{
		Relationship: uint32(relationProcessorCore),
		Size:         uint32(SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX_SIZE + PROCESSOR_RELATIONSHIP_SIZE + (GROUP_AFFINITY_SIZE * grouplen)),
		data: processorRelationship{
			Flags:           0,
			EfficiencyClass: 0,
			Reserved:        [20]byte{},
			GroupCount:      uint16(grouplen),
			GroupMasks:      groupAffinities,
		},
	}, systemLogicalProcessorInformationEx{
		Relationship: uint32(relationNumaNode),
		Size:         uint32(SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX_SIZE + NUMA_NODE_RELATIONSHIP_SIZE + (GROUP_AFFINITY_SIZE * grouplen)),
		data: numaNodeRelationship{
			NodeNumber: 0,
			Reserved:   [18]byte{},
			GroupCount: uint16(grouplen),
			GroupMasks: groupAffinities,
		}}, systemLogicalProcessorInformationEx{
		Relationship: uint32(relationProcessorPackage),
		Size:         uint32(SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX_SIZE + PROCESSOR_RELATIONSHIP_SIZE + (GROUP_AFFINITY_SIZE * grouplen)),
		data: processorRelationship{
			Flags:           0,
			EfficiencyClass: 0,
			Reserved:        [20]byte{},
			GroupCount:      uint16(grouplen),
			GroupMasks:      groupAffinities,
		},
	})
}

const SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX_SIZE = 8
const PROCESSOR_RELATIONSHIP_SIZE = 24
const NUMA_NODE_RELATIONSHIP_SIZE = 24

// GROUP_AFFINITY_SIZE 在编译期即可确定，因此直接用 unsafe.Sizeof 计算。
const GROUP_AFFINITY_SIZE = int(unsafe.Sizeof(GroupAffinity{}))

// structToBytes 将单条逻辑处理器信息按 Windows API 的内存布局序列化为字节，
// 以便构造供解析函数使用的测试缓冲区。
func structToBytes(info systemLogicalProcessorInformationEx) []byte {
	var pri []byte = (*(*[SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX_SIZE]byte)(unsafe.Pointer(&info)))[:SYSTEM_LOGICAL_PROCESSOR_INFORMATION_EX_SIZE]

	// 根据 data 的具体类型追加对应的结构体字节与组亲和性数组。
	switch info.data.(type) {
	case processorRelationship:
		rel := info.data.(processorRelationship)
		var prBytes []byte = (*(*[PROCESSOR_RELATIONSHIP_SIZE]byte)(unsafe.Pointer(&rel)))[:PROCESSOR_RELATIONSHIP_SIZE]
		pri = append(pri, prBytes...)

		groupAffinities := rel.GroupMasks.([]GroupAffinity)

		for _, groupAffinity := range groupAffinities {
			var groupByte []byte = (*(*[GROUP_AFFINITY_SIZE]byte)(unsafe.Pointer(&groupAffinity)))[:]
			pri = append(pri, groupByte...)
		}
	case numaNodeRelationship:
		numa := info.data.(numaNodeRelationship)
		var nameBytes []byte = (*(*[NUMA_NODE_RELATIONSHIP_SIZE]byte)(unsafe.Pointer(&numa)))[:NUMA_NODE_RELATIONSHIP_SIZE]
		pri = append(pri, nameBytes...)

		groupAffinities := numa.GroupMasks.([]GroupAffinity)

		for _, groupAffinity := range groupAffinities {
			var groupByte []byte = (*(*[GROUP_AFFINITY_SIZE]byte)(unsafe.Pointer(&groupAffinity)))[:]
			pri = append(pri, groupByte...)
		}
	}

	return pri
}
