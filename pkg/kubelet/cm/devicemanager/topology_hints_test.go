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

package devicemanager

import (
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager/bitmask"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// mockAffinityStore 是 topologyAffinityStore 的测试桩，固定返回预置的 hint。
type mockAffinityStore struct {
	hint topologymanager.TopologyHint
}

// GetAffinity 忽略传入参数，直接返回桩中固定的 hint。
func (m *mockAffinityStore) GetAffinity(_ klog.Logger, podUID string, containerName string) topologymanager.TopologyHint {
	return m.hint
}

// GetPolicy 在测试中不需要真实策略，返回 nil。
func (m *mockAffinityStore) GetPolicy() topologymanager.Policy {
	return nil
}

// Name 返回固定的 scope 名；测试不校验该值。
func (m *mockAffinityStore) Name() string {
	return "container"
}

// makeNUMADevice 构造一个绑定到指定 NUMA 节点的 device plugin 设备。
func makeNUMADevice(id string, numa int) *pluginapi.Device {
	return &pluginapi.Device{
		ID:       id,
		Topology: &pluginapi.TopologyInfo{Nodes: []*pluginapi.NUMANode{{ID: int64(numa)}}},
	}
}

// makeSocketMask 用给定 NUMA 节点号构造 bitmask；测试只传合法节点号，故忽略错误。
func makeSocketMask(sockets ...int) bitmask.BitMask {
	mask, _ := bitmask.NewBitMask(sockets...)
	return mask
}

// TestGetTopologyHints 校验容器维度的 hint 生成：请求量、设备的 NUMA 分布
// 以及已分配设备共同决定每种资源的 hint 集合。
func TestGetTopologyHints(t *testing.T) {
	tcases := getCommonTestCases()

	logger, _ := ktesting.NewTestContext(t)

	for _, tc := range tcases {
		// 假定测试机器有 0、1 两个 NUMA 节点；未声明拓扑的设备不贡献 NUMA 亲和性。
		m := ManagerImpl{
			allDevices:       NewResourceDeviceInstances(),
			healthyDevices:   make(map[string]sets.Set[string]),
			allocatedDevices: make(map[string]sets.Set[string]),
			podDevices:       newPodDevices(),
			sourcesReady:     &sourcesReadyStub{},
			activePods:       func() []*v1.Pod { return []*v1.Pod{tc.pod} },
			numaNodes:        []int{0, 1},
		}

		for r := range tc.devices {
			m.allDevices[r] = make(DeviceInstances)
			m.healthyDevices[r] = sets.New[string]()

			for _, d := range tc.devices[r] {
				m.allDevices[r][d.ID] = d
				m.healthyDevices[r].Insert(d.ID)
			}
		}

		// 预置已分配设备，验证生成 hint 时会从可用池中扣除这些设备。
		for p := range tc.allocatedDevices {
			for c := range tc.allocatedDevices[p] {
				for r, devices := range tc.allocatedDevices[p][c] {
					m.podDevices.insert(p, c, r, constructDevices(devices), nil)

					m.allocatedDevices[r] = sets.New[string]()
					for _, d := range devices {
						m.allocatedDevices[r].Insert(d)
					}
				}
			}
		}

		hints := m.GetTopologyHints(logger, tc.pod, &tc.pod.Spec.Containers[0], lifecycle.AddOperation)

		// hint 的返回顺序不作保证，先排序再比较，避免因顺序不同而误报。
		for r := range tc.expectedHints {
			sort.SliceStable(hints[r], func(i, j int) bool {
				return hints[r][i].LessThan(hints[r][j])
			})
			sort.SliceStable(tc.expectedHints[r], func(i, j int) bool {
				return tc.expectedHints[r][i].LessThan(tc.expectedHints[r][j])
			})
			if !reflect.DeepEqual(hints[r], tc.expectedHints[r]) {
				t.Errorf("%v: Expected result to be %#v, got %#v", tc.description, tc.expectedHints[r], hints[r])
			}
		}
	}
}

// TestTopologyAlignedAllocation 覆盖 devicesToAllocate 的拓扑对齐路径：
// 按 hint 过滤可用设备，再调用 device plugin 的 GetPreferredAllocation，
// 最后校验入选设备落在预期的 NUMA 节点上。
func TestTopologyAlignedAllocation(t *testing.T) {
	tCtx := ktesting.Init(t)
	tcases := []struct {
		description                 string
		resource                    string
		request                     int
		devices                     []*pluginapi.Device
		allocatedDevices            []string
		hint                        topologymanager.TopologyHint
		getPreferredAllocationFunc  func(available, mustInclude []string, size int) (*pluginapi.PreferredAllocationResponse, error)
		expectedPreferredAllocation []string
		expectedAlignment           map[int]int
	}{
		{
			description: "Single Request, no alignment",
			resource:    "resource",
			request:     1,
			devices: []*pluginapi.Device{
				{ID: "Dev1"},
				{ID: "Dev2"},
			},
			hint: topologymanager.TopologyHint{
				NUMANodeAffinity: makeSocketMask(0, 1),
				Preferred:        true,
			},
			expectedAlignment: map[int]int{},
		},
		{
			description: "Request for 1, partial alignment",
			resource:    "resource",
			request:     1,
			devices: []*pluginapi.Device{
				{ID: "Dev1"},
				makeNUMADevice("Dev2", 1),
			},
			hint: topologymanager.TopologyHint{
				NUMANodeAffinity: makeSocketMask(1),
				Preferred:        true,
			},
			expectedAlignment: map[int]int{1: 1},
		},
		{
			description: "Single Request, socket 0",
			resource:    "resource",
			request:     1,
			devices: []*pluginapi.Device{
				makeNUMADevice("Dev1", 0),
				makeNUMADevice("Dev2", 1),
			},
			hint: topologymanager.TopologyHint{
				NUMANodeAffinity: makeSocketMask(0),
				Preferred:        true,
			},
			expectedAlignment: map[int]int{0: 1},
		},
		{
			description: "Single Request, socket 1",
			resource:    "resource",
			request:     1,
			devices: []*pluginapi.Device{
				makeNUMADevice("Dev1", 0),
				makeNUMADevice("Dev2", 1),
			},
			hint: topologymanager.TopologyHint{
				NUMANodeAffinity: makeSocketMask(1),
				Preferred:        true,
			},
			expectedAlignment: map[int]int{1: 1},
		},
		{
			description: "Request for 2, socket 0",
			resource:    "resource",
			request:     2,
			devices: []*pluginapi.Device{
				makeNUMADevice("Dev1", 0),
				makeNUMADevice("Dev2", 1),
				makeNUMADevice("Dev3", 0),
				makeNUMADevice("Dev4", 1),
			},
			hint: topologymanager.TopologyHint{
				NUMANodeAffinity: makeSocketMask(0),
				Preferred:        true,
			},
			expectedAlignment: map[int]int{0: 2},
		},
		{
			description: "Request for 2, socket 1",
			resource:    "resource",
			request:     2,
			devices: []*pluginapi.Device{
				makeNUMADevice("Dev1", 0),
				makeNUMADevice("Dev2", 1),
				makeNUMADevice("Dev3", 0),
				makeNUMADevice("Dev4", 1),
			},
			hint: topologymanager.TopologyHint{
				NUMANodeAffinity: makeSocketMask(1),
				Preferred:        true,
			},
			expectedAlignment: map[int]int{1: 2},
		},
		{
			description: "Request for 4, unsatisfiable, prefer socket 0",
			resource:    "resource",
			request:     4,
			devices: []*pluginapi.Device{
				makeNUMADevice("Dev1", 0),
				makeNUMADevice("Dev2", 1),
				makeNUMADevice("Dev3", 0),
				makeNUMADevice("Dev4", 1),
				makeNUMADevice("Dev5", 0),
				makeNUMADevice("Dev6", 1),
			},
			hint: topologymanager.TopologyHint{
				NUMANodeAffinity: makeSocketMask(0),
				Preferred:        true,
			},
			expectedAlignment: map[int]int{0: 3, 1: 1},
		},
		{
			description: "Request for 4, unsatisfiable, prefer socket 1",
			resource:    "resource",
			request:     4,
			devices: []*pluginapi.Device{
				makeNUMADevice("Dev1", 0),
				makeNUMADevice("Dev2", 1),
				makeNUMADevice("Dev3", 0),
				makeNUMADevice("Dev4", 1),
				makeNUMADevice("Dev5", 0),
				makeNUMADevice("Dev6", 1),
			},
			hint: topologymanager.TopologyHint{
				NUMANodeAffinity: makeSocketMask(1),
				Preferred:        true,
			},
			expectedAlignment: map[int]int{0: 1, 1: 3},
		},
		{
			description: "Request for 4, multisocket",
			resource:    "resource",
			request:     4,
			devices: []*pluginapi.Device{
				makeNUMADevice("Dev1", 0),
				makeNUMADevice("Dev2", 1),
				makeNUMADevice("Dev3", 2),
				makeNUMADevice("Dev4", 3),
				makeNUMADevice("Dev5", 0),
				makeNUMADevice("Dev6", 1),
				makeNUMADevice("Dev7", 2),
				makeNUMADevice("Dev8", 3),
			},
			hint: topologymanager.TopologyHint{
				NUMANodeAffinity: makeSocketMask(1, 3),
				Preferred:        true,
			},
			expectedAlignment: map[int]int{1: 2, 3: 2},
		},
		{
			description: "Request for 5, socket 0, preferred aligned accepted",
			resource:    "resource",
			request:     5,
			devices: func() []*pluginapi.Device {
				devices := []*pluginapi.Device{}
				for i := 0; i < 100; i++ {
					id := fmt.Sprintf("Dev%d", i)
					devices = append(devices, makeNUMADevice(id, 0))
				}
				for i := 100; i < 200; i++ {
					id := fmt.Sprintf("Dev%d", i)
					devices = append(devices, makeNUMADevice(id, 1))
				}
				return devices
			}(),
			hint: topologymanager.TopologyHint{
				NUMANodeAffinity: makeSocketMask(0),
				Preferred:        true,
			},
			getPreferredAllocationFunc: func(available, mustInclude []string, size int) (*pluginapi.PreferredAllocationResponse, error) {
				return &pluginapi.PreferredAllocationResponse{
					ContainerResponses: []*pluginapi.ContainerPreferredAllocationResponse{
						{DeviceIDs: []string{"Dev0", "Dev19", "Dev83", "Dev42", "Dev77"}},
					},
				}, nil
			},
			expectedPreferredAllocation: []string{"Dev0", "Dev19", "Dev83", "Dev42", "Dev77"},
			expectedAlignment:           map[int]int{0: 5},
		},
		{
			description: "Request for 5, socket 0, preferred aligned accepted, unaligned ignored",
			resource:    "resource",
			request:     5,
			devices: func() []*pluginapi.Device {
				devices := []*pluginapi.Device{}
				for i := 0; i < 100; i++ {
					id := fmt.Sprintf("Dev%d", i)
					devices = append(devices, makeNUMADevice(id, 0))
				}
				for i := 100; i < 200; i++ {
					id := fmt.Sprintf("Dev%d", i)
					devices = append(devices, makeNUMADevice(id, 1))
				}
				return devices
			}(),
			hint: topologymanager.TopologyHint{
				NUMANodeAffinity: makeSocketMask(0),
				Preferred:        true,
			},
			getPreferredAllocationFunc: func(available, mustInclude []string, size int) (*pluginapi.PreferredAllocationResponse, error) {
				return &pluginapi.PreferredAllocationResponse{
					ContainerResponses: []*pluginapi.ContainerPreferredAllocationResponse{
						{DeviceIDs: []string{"Dev0", "Dev19", "Dev83", "Dev150", "Dev186"}},
					},
				}, nil
			},
			expectedPreferredAllocation: []string{"Dev0", "Dev19", "Dev83"},
			expectedAlignment:           map[int]int{0: 5},
		},
		{
			description: "Request for 5, socket 1, preferred aligned accepted, bogus ignored",
			resource:    "resource",
			request:     5,
			devices: func() []*pluginapi.Device {
				devices := []*pluginapi.Device{}
				for i := 0; i < 100; i++ {
					id := fmt.Sprintf("Dev%d", i)
					devices = append(devices, makeNUMADevice(id, 1))
				}
				return devices
			}(),
			hint: topologymanager.TopologyHint{
				NUMANodeAffinity: makeSocketMask(1),
				Preferred:        true,
			},
			getPreferredAllocationFunc: func(available, mustInclude []string, size int) (*pluginapi.PreferredAllocationResponse, error) {
				return &pluginapi.PreferredAllocationResponse{
					ContainerResponses: []*pluginapi.ContainerPreferredAllocationResponse{
						{DeviceIDs: []string{"Dev0", "Dev19", "Dev83", "bogus0", "bogus1"}},
					},
				}, nil
			},
			expectedPreferredAllocation: []string{"Dev0", "Dev19", "Dev83"},
			expectedAlignment:           map[int]int{1: 5},
		},
		{
			description: "Request for 5, multisocket, preferred accepted",
			resource:    "resource",
			request:     5,
			devices: func() []*pluginapi.Device {
				devices := []*pluginapi.Device{}
				for i := 0; i < 3; i++ {
					id := fmt.Sprintf("Dev%d", i)
					devices = append(devices, makeNUMADevice(id, 0))
				}
				for i := 3; i < 100; i++ {
					id := fmt.Sprintf("Dev%d", i)
					devices = append(devices, makeNUMADevice(id, 1))
				}
				return devices
			}(),
			hint: topologymanager.TopologyHint{
				NUMANodeAffinity: makeSocketMask(0),
				Preferred:        true,
			},
			getPreferredAllocationFunc: func(available, mustInclude []string, size int) (*pluginapi.PreferredAllocationResponse, error) {
				return &pluginapi.PreferredAllocationResponse{
					ContainerResponses: []*pluginapi.ContainerPreferredAllocationResponse{
						{DeviceIDs: []string{"Dev0", "Dev1", "Dev2", "Dev42", "Dev83"}},
					},
				}, nil
			},
			expectedPreferredAllocation: []string{"Dev0", "Dev1", "Dev2", "Dev42", "Dev83"},
			expectedAlignment:           map[int]int{0: 3, 1: 2},
		},
		{
			description: "Request for 5, multisocket, preferred unaligned accepted, bogus ignored",
			resource:    "resource",
			request:     5,
			devices: func() []*pluginapi.Device {
				devices := []*pluginapi.Device{}
				for i := 0; i < 3; i++ {
					id := fmt.Sprintf("Dev%d", i)
					devices = append(devices, makeNUMADevice(id, 0))
				}
				for i := 3; i < 100; i++ {
					id := fmt.Sprintf("Dev%d", i)
					devices = append(devices, makeNUMADevice(id, 1))
				}
				return devices
			}(),
			hint: topologymanager.TopologyHint{
				NUMANodeAffinity: makeSocketMask(0),
				Preferred:        true,
			},
			getPreferredAllocationFunc: func(available, mustInclude []string, size int) (*pluginapi.PreferredAllocationResponse, error) {
				return &pluginapi.PreferredAllocationResponse{
					ContainerResponses: []*pluginapi.ContainerPreferredAllocationResponse{
						{DeviceIDs: []string{"Dev0", "Dev1", "Dev2", "Dev42", "bogus0"}},
					},
				}, nil
			},
			expectedPreferredAllocation: []string{"Dev0", "Dev1", "Dev2", "Dev42"},
			expectedAlignment:           map[int]int{0: 3, 1: 2},
		},
	}
	for _, tc := range tcases {
		m := ManagerImpl{
			allDevices:            NewResourceDeviceInstances(),
			healthyDevices:        make(map[string]sets.Set[string]),
			allocatedDevices:      make(map[string]sets.Set[string]),
			endpoints:             make(map[string]endpointInfo),
			podDevices:            newPodDevices(),
			sourcesReady:          &sourcesReadyStub{},
			activePods:            func() []*v1.Pod { return []*v1.Pod{} },
			topologyAffinityStore: &mockAffinityStore{tc.hint},
		}

		m.allDevices[tc.resource] = make(DeviceInstances)
		m.healthyDevices[tc.resource] = sets.New[string]()
		m.endpoints[tc.resource] = endpointInfo{}

		for _, d := range tc.devices {
			m.allDevices[tc.resource][d.ID] = d
			m.healthyDevices[tc.resource].Insert(d.ID)
		}

		// 仅当 device plugin 声明支持 GetPreferredAllocation 时才注册 endpoint。
		if tc.getPreferredAllocationFunc != nil {
			m.endpoints[tc.resource] = endpointInfo{
				e: &MockEndpoint{
					getPreferredAllocationFunc: tc.getPreferredAllocationFunc,
				},
				opts: &pluginapi.DevicePluginOptions{GetPreferredAllocationAvailable: true},
			}
		}

		allocated, err := m.devicesToAllocate(tCtx, "podUID", "containerName", tc.resource, tc.request, sets.New[string]())
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
			continue
		}

		if len(allocated) != tc.request {
			t.Errorf("%v. expected allocation size: %v but got: %v", tc.description, tc.request, len(allocated))
		}

		// plugin 返回的设备可能包含非法 ID 或跨 NUMA 的设备，必须被过滤掉。
		if !allocated.HasAll(tc.expectedPreferredAllocation...) {
			t.Errorf("%v. expected preferred allocation: %v but not present in: %v", tc.description, tc.expectedPreferredAllocation, allocated.UnsortedList())
		}

		// 统计实际分配的设备在各 NUMA 节点上的数量，用于验证是否与 hint 对齐。
		alignment := make(map[int]int)
		if m.deviceHasTopologyAlignment(tc.resource) {
			for d := range allocated {
				if m.allDevices[tc.resource][d].Topology != nil {
					alignment[int(m.allDevices[tc.resource][d].Topology.Nodes[0].ID)]++
				}
			}
		}

		if !reflect.DeepEqual(alignment, tc.expectedAlignment) {
			t.Errorf("%v. expected alignment: %v but got: %v", tc.description, tc.expectedAlignment, alignment)
		}
	}
}

// TestGetPreferredAllocationParameters 校验传给 device plugin 的三个参数：
// available（可选设备）、mustInclude（已分配且可复用的设备）、size（本次请求数量）。
func TestGetPreferredAllocationParameters(t *testing.T) {
	tCtx := ktesting.Init(t)
	tcases := []struct {
		description         string
		resource            string
		request             int
		allDevices          []*pluginapi.Device
		allocatedDevices    []string
		reusableDevices     []string
		hint                topologymanager.TopologyHint
		expectedAvailable   []string
		expectedMustInclude []string
		expectedSize        int
	}{
		{
			description: "Request for 1, socket 0, 0 already allocated, 0 reusable",
			resource:    "resource",
			request:     1,
			allDevices: []*pluginapi.Device{
				makeNUMADevice("Dev0", 0),
				makeNUMADevice("Dev1", 0),
				makeNUMADevice("Dev2", 0),
				makeNUMADevice("Dev3", 0),
			},
			allocatedDevices: []string{},
			reusableDevices:  []string{},
			hint: topologymanager.TopologyHint{
				NUMANodeAffinity: makeSocketMask(0),
				Preferred:        true,
			},
			expectedAvailable:   []string{"Dev0", "Dev1", "Dev2", "Dev3"},
			expectedMustInclude: []string{},
			expectedSize:        1,
		},
		{
			description: "Request for 4, socket 0, 2 already allocated, 2 reusable",
			resource:    "resource",
			request:     4,
			allDevices: []*pluginapi.Device{
				makeNUMADevice("Dev0", 0),
				makeNUMADevice("Dev1", 0),
				makeNUMADevice("Dev2", 0),
				makeNUMADevice("Dev3", 0),
				makeNUMADevice("Dev4", 0),
				makeNUMADevice("Dev5", 0),
				makeNUMADevice("Dev6", 0),
				makeNUMADevice("Dev7", 0),
			},
			allocatedDevices: []string{"Dev0", "Dev5"},
			reusableDevices:  []string{"Dev0", "Dev5"},
			hint: topologymanager.TopologyHint{
				NUMANodeAffinity: makeSocketMask(0),
				Preferred:        true,
			},
			expectedAvailable:   []string{"Dev0", "Dev1", "Dev2", "Dev3", "Dev4", "Dev5", "Dev6", "Dev7"},
			expectedMustInclude: []string{"Dev0", "Dev5"},
			expectedSize:        4,
		},
		{
			description: "Request for 4, socket 0, 4 already allocated, 2 reusable",
			resource:    "resource",
			request:     4,
			allDevices: []*pluginapi.Device{
				makeNUMADevice("Dev0", 0),
				makeNUMADevice("Dev1", 0),
				makeNUMADevice("Dev2", 0),
				makeNUMADevice("Dev3", 0),
				makeNUMADevice("Dev4", 0),
				makeNUMADevice("Dev5", 0),
				makeNUMADevice("Dev6", 0),
				makeNUMADevice("Dev7", 0),
			},
			allocatedDevices: []string{"Dev0", "Dev5", "Dev4", "Dev1"},
			reusableDevices:  []string{"Dev0", "Dev5"},
			hint: topologymanager.TopologyHint{
				NUMANodeAffinity: makeSocketMask(0),
				Preferred:        true,
			},
			expectedAvailable:   []string{"Dev0", "Dev2", "Dev3", "Dev5", "Dev6", "Dev7"},
			expectedMustInclude: []string{"Dev0", "Dev5"},
			expectedSize:        4,
		},
		{
			description: "Request for 6, multisocket, 2 already allocated, 2 reusable",
			resource:    "resource",
			request:     6,
			allDevices: []*pluginapi.Device{
				makeNUMADevice("Dev0", 0),
				makeNUMADevice("Dev1", 0),
				makeNUMADevice("Dev2", 0),
				makeNUMADevice("Dev3", 0),
				makeNUMADevice("Dev4", 1),
				makeNUMADevice("Dev5", 1),
				makeNUMADevice("Dev6", 1),
				makeNUMADevice("Dev7", 1),
			},
			allocatedDevices: []string{"Dev1", "Dev6"},
			reusableDevices:  []string{"Dev1", "Dev6"},
			hint: topologymanager.TopologyHint{
				NUMANodeAffinity: makeSocketMask(0),
				Preferred:        true,
			},
			expectedAvailable:   []string{"Dev0", "Dev1", "Dev2", "Dev3", "Dev4", "Dev5", "Dev6", "Dev7"},
			expectedMustInclude: []string{"Dev0", "Dev1", "Dev2", "Dev3", "Dev6"},
			expectedSize:        6,
		},
		{
			description: "Request for 6, multisocket, 4 already allocated, 2 reusable",
			resource:    "resource",
			request:     6,
			allDevices: []*pluginapi.Device{
				makeNUMADevice("Dev0", 0),
				makeNUMADevice("Dev1", 0),
				makeNUMADevice("Dev2", 0),
				makeNUMADevice("Dev3", 0),
				makeNUMADevice("Dev4", 1),
				makeNUMADevice("Dev5", 1),
				makeNUMADevice("Dev6", 1),
				makeNUMADevice("Dev7", 1),
			},
			allocatedDevices: []string{"Dev0", "Dev1", "Dev6", "Dev7"},
			reusableDevices:  []string{"Dev1", "Dev6"},
			hint: topologymanager.TopologyHint{
				NUMANodeAffinity: makeSocketMask(0),
				Preferred:        true,
			},
			expectedAvailable:   []string{"Dev1", "Dev2", "Dev3", "Dev4", "Dev5", "Dev6"},
			expectedMustInclude: []string{"Dev1", "Dev2", "Dev3", "Dev6"},
			expectedSize:        6,
		},
	}
	for _, tc := range tcases {
		m := ManagerImpl{
			allDevices:            NewResourceDeviceInstances(),
			healthyDevices:        make(map[string]sets.Set[string]),
			allocatedDevices:      make(map[string]sets.Set[string]),
			endpoints:             make(map[string]endpointInfo),
			podDevices:            newPodDevices(),
			sourcesReady:          &sourcesReadyStub{},
			activePods:            func() []*v1.Pod { return []*v1.Pod{} },
			topologyAffinityStore: &mockAffinityStore{tc.hint},
		}

		m.allDevices[tc.resource] = make(DeviceInstances)
		m.healthyDevices[tc.resource] = sets.New[string]()
		for _, d := range tc.allDevices {
			m.allDevices[tc.resource][d.ID] = d
			m.healthyDevices[tc.resource].Insert(d.ID)
		}

		m.allocatedDevices[tc.resource] = sets.New[string]()
		for _, d := range tc.allocatedDevices {
			m.allocatedDevices[tc.resource].Insert(d)
		}

		actualAvailable := []string{}
		actualMustInclude := []string{}
		actualSize := 0
		m.endpoints[tc.resource] = endpointInfo{
			e: &MockEndpoint{
				getPreferredAllocationFunc: func(available, mustInclude []string, size int) (*pluginapi.PreferredAllocationResponse, error) {
					actualAvailable = append(actualAvailable, available...)
					actualMustInclude = append(actualMustInclude, mustInclude...)
					actualSize = size
					return nil, nil
				},
			},
			opts: &pluginapi.DevicePluginOptions{GetPreferredAllocationAvailable: true},
		}

		// 由 mock endpoint 捕获实际传入的参数，稍后与期望值比较。
		_, err := m.devicesToAllocate(tCtx, "podUID", "containerName", tc.resource, tc.request, sets.New[string](tc.reusableDevices...))
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
			continue
		}

		if !sets.New[string](actualAvailable...).Equal(sets.New[string](tc.expectedAvailable...)) {
			t.Errorf("%v. expected available: %v but got: %v", tc.description, tc.expectedAvailable, actualAvailable)
		}

		if !sets.New[string](actualAvailable...).Equal(sets.New[string](tc.expectedAvailable...)) {
			t.Errorf("%v. expected mustInclude: %v but got: %v", tc.description, tc.expectedMustInclude, actualMustInclude)
		}

		if actualSize != tc.expectedSize {
			t.Errorf("%v. expected size: %v but got: %v", tc.description, tc.expectedSize, actualSize)
		}
	}
}

// TestGetPodDeviceRequest 校验 pod 级资源请求聚合：init 容器之间取最大值，
// 用户容器求和，最终每种资源取两者较大值，与调度器对 extended resource 的语义一致。
func TestGetPodDeviceRequest(t *testing.T) {
	tcases := []struct {
		description       string
		pod               *v1.Pod
		registeredDevices []string
		expected          map[string]int
	}{
		{
			description:       "empty pod",
			pod:               &v1.Pod{},
			registeredDevices: []string{},
			expected:          map[string]int{},
		},
		{
			description: "Init container requests device plugin resource",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					InitContainers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName(v1.ResourceCPU):    resource.MustParse("2"),
									v1.ResourceName(v1.ResourceMemory): resource.MustParse("1G"),
									v1.ResourceName("gpu"):             resource.MustParse("2"),
								},
							},
						},
					},
				},
			},
			registeredDevices: []string{"gpu"},
			expected:          map[string]int{"gpu": 2},
		},
		{
			description: "Init containers request device plugin resource",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					InitContainers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName(v1.ResourceCPU):    resource.MustParse("2"),
									v1.ResourceName(v1.ResourceMemory): resource.MustParse("1G"),
									v1.ResourceName("gpu"):             resource.MustParse("2"),
								},
							},
						},
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName(v1.ResourceCPU):    resource.MustParse("2"),
									v1.ResourceName(v1.ResourceMemory): resource.MustParse("1G"),
									v1.ResourceName("gpu"):             resource.MustParse("4"),
								},
							},
						},
					},
				},
			},
			registeredDevices: []string{"gpu"},
			expected:          map[string]int{"gpu": 4},
		},
		{
			description: "User container requests device plugin resource",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName(v1.ResourceCPU):    resource.MustParse("2"),
									v1.ResourceName(v1.ResourceMemory): resource.MustParse("1G"),
									v1.ResourceName("gpu"):             resource.MustParse("2"),
								},
							},
						},
					},
				},
			},
			registeredDevices: []string{"gpu"},
			expected:          map[string]int{"gpu": 2},
		},
		{
			description: "Init containers and user containers request the same amount of device plugin resources",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					InitContainers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName(v1.ResourceCPU):    resource.MustParse("2"),
									v1.ResourceName(v1.ResourceMemory): resource.MustParse("1G"),
									v1.ResourceName("gpu"):             resource.MustParse("2"),
									v1.ResourceName("nic"):             resource.MustParse("2"),
								},
							},
						},
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName(v1.ResourceCPU):    resource.MustParse("2"),
									v1.ResourceName(v1.ResourceMemory): resource.MustParse("1G"),
									v1.ResourceName("gpu"):             resource.MustParse("2"),
									v1.ResourceName("nic"):             resource.MustParse("2"),
								},
							},
						},
					},
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName(v1.ResourceCPU):    resource.MustParse("2"),
									v1.ResourceName(v1.ResourceMemory): resource.MustParse("1G"),
									v1.ResourceName("gpu"):             resource.MustParse("1"),
									v1.ResourceName("nic"):             resource.MustParse("1"),
								},
							},
						},
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName(v1.ResourceCPU):    resource.MustParse("2"),
									v1.ResourceName(v1.ResourceMemory): resource.MustParse("1G"),
									v1.ResourceName("gpu"):             resource.MustParse("1"),
									v1.ResourceName("nic"):             resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			registeredDevices: []string{"gpu", "nic"},
			expected:          map[string]int{"gpu": 2, "nic": 2},
		},
		{
			description: "Init containers request more device plugin resources than user containers",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					InitContainers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName(v1.ResourceCPU):    resource.MustParse("2"),
									v1.ResourceName(v1.ResourceMemory): resource.MustParse("1G"),
									v1.ResourceName("gpu"):             resource.MustParse("2"),
									v1.ResourceName("nic"):             resource.MustParse("1"),
								},
							},
						},
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName(v1.ResourceCPU):    resource.MustParse("2"),
									v1.ResourceName(v1.ResourceMemory): resource.MustParse("1G"),
									v1.ResourceName("gpu"):             resource.MustParse("3"),
									v1.ResourceName("nic"):             resource.MustParse("2"),
								},
							},
						},
					},
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName(v1.ResourceCPU):    resource.MustParse("1"),
									v1.ResourceName(v1.ResourceMemory): resource.MustParse("1G"),
									v1.ResourceName("gpu"):             resource.MustParse("1"),
									v1.ResourceName("nic"):             resource.MustParse("1"),
								},
							},
						},
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName(v1.ResourceCPU):    resource.MustParse("1"),
									v1.ResourceName(v1.ResourceMemory): resource.MustParse("1G"),
									v1.ResourceName("gpu"):             resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			registeredDevices: []string{"gpu", "nic"},
			expected:          map[string]int{"gpu": 3, "nic": 2},
		},
		{
			description: "User containers request more device plugin resources than init containers",
			pod: &v1.Pod{
				Spec: v1.PodSpec{
					InitContainers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName(v1.ResourceCPU):    resource.MustParse("2"),
									v1.ResourceName(v1.ResourceMemory): resource.MustParse("1G"),
									v1.ResourceName("gpu"):             resource.MustParse("2"),
									v1.ResourceName("nic"):             resource.MustParse("1"),
								},
							},
						},
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName(v1.ResourceCPU):    resource.MustParse("2"),
									v1.ResourceName(v1.ResourceMemory): resource.MustParse("1G"),
									v1.ResourceName("gpu"):             resource.MustParse("2"),
									v1.ResourceName("nic"):             resource.MustParse("1"),
								},
							},
						},
					},
					Containers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName(v1.ResourceCPU):    resource.MustParse("1"),
									v1.ResourceName(v1.ResourceMemory): resource.MustParse("1G"),
									v1.ResourceName("gpu"):             resource.MustParse("3"),
									v1.ResourceName("nic"):             resource.MustParse("2"),
								},
							},
						},
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName(v1.ResourceCPU):    resource.MustParse("1"),
									v1.ResourceName(v1.ResourceMemory): resource.MustParse("1G"),
									v1.ResourceName("gpu"):             resource.MustParse("3"),
									v1.ResourceName("nic"):             resource.MustParse("2"),
								},
							},
						},
					},
				},
			},
			registeredDevices: []string{"gpu", "nic"},
			expected:          map[string]int{"gpu": 6, "nic": 4},
		},
	}

	for _, tc := range tcases {
		m := ManagerImpl{
			healthyDevices: make(map[string]sets.Set[string]),
		}

		// 只有已注册的资源参与聚合，CPU、memory 等内置资源会被忽略。
		for _, res := range tc.registeredDevices {
			m.healthyDevices[res] = sets.New[string]()
		}

		// 聚合结果与期望值整体比较，覆盖多容器、init 容器组合等场景。
		accumulatedResourceRequests := m.getPodDeviceRequest(tc.pod)

		if !reflect.DeepEqual(accumulatedResourceRequests, tc.expected) {
			t.Errorf("%v. expected alignment: %v but got: %v", tc.description, tc.expected, accumulatedResourceRequests)
		}
	}
}

// TestGetPodTopologyHints 校验 pod 维度的 hint 生成：同 pod 内所有容器共享设备池，
// 已分配设备需从可用池中扣除，未注册的资源不参与计算。
func TestGetPodTopologyHints(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	tcases := getCommonTestCases()
	tcases = append(tcases, getPodScopeTestCases()...)

	for _, tc := range tcases {
		m := ManagerImpl{
			allDevices:       NewResourceDeviceInstances(),
			healthyDevices:   make(map[string]sets.Set[string]),
			allocatedDevices: make(map[string]sets.Set[string]),
			podDevices:       newPodDevices(),
			sourcesReady:     &sourcesReadyStub{},
			activePods:       func() []*v1.Pod { return []*v1.Pod{tc.pod, {ObjectMeta: metav1.ObjectMeta{UID: "fakeOtherPod"}}} },
			numaNodes:        []int{0, 1},
		}

		for r := range tc.devices {
			m.allDevices[r] = make(DeviceInstances)
			m.healthyDevices[r] = sets.New[string]()

			for _, d := range tc.devices[r] {
				// 注册带 NUMA 拓扑的 device plugin 设备。
				m.allDevices[r][d.ID] = d
				m.healthyDevices[r].Insert(d.ID)
			}
		}

		for p := range tc.allocatedDevices {
			for c := range tc.allocatedDevices[p] {
				for r, devices := range tc.allocatedDevices[p][c] {
					m.podDevices.insert(p, c, r, constructDevices(devices), nil)

					m.allocatedDevices[r] = sets.New[string]()
					for _, d := range devices {
						m.allocatedDevices[r].Insert(d)
					}
				}
			}
		}

		hints := m.GetPodTopologyHints(logger, tc.pod, lifecycle.AddOperation)

		for r := range tc.expectedHints {
			sort.SliceStable(hints[r], func(i, j int) bool {
				return hints[r][i].LessThan(hints[r][j])
			})
			sort.SliceStable(tc.expectedHints[r], func(i, j int) bool {
				return tc.expectedHints[r][i].LessThan(tc.expectedHints[r][j])
			})
			if !reflect.DeepEqual(hints[r], tc.expectedHints[r]) {
				t.Errorf("%v: Expected result to be %v, got %v", tc.description, tc.expectedHints[r], hints[r])
			}
		}
	}
}

// TestDeviceNUMANodes 校验从设备拓扑中收集实际使用的 NUMA 节点，
// 并丢弃超出机器 numaNodes 范围的非法节点 ID。
func TestDeviceNUMANodes(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	resource := "testdevice"
	deviceOnNode := func(id string, node int) *pluginapi.Device {
		return &pluginapi.Device{
			ID:       id,
			Topology: &pluginapi.TopologyInfo{Nodes: []*pluginapi.NUMANode{{ID: int64(node)}}},
		}
	}

	// 汇总所有带拓扑设备实际所在的 NUMA 节点。
	t.Run("collects NUMA nodes from all devices", func(t *testing.T) {
		m := ManagerImpl{
			allDevices: NewResourceDeviceInstances(),
			numaNodes:  []int{0, 3, 5, 7},
		}
		m.allDevices[resource] = DeviceInstances{
			"a": deviceOnNode("a", 3),
			"b": deviceOnNode("b", 5),
		}

		nodes := m.deviceNUMANodes(logger, resource)
		expected := []int{3, 5}
		if !reflect.DeepEqual(nodes, expected) {
			t.Fatalf("expected nodes %v, got %v", expected, nodes)
		}
	})

	// 未声明拓扑的设备不贡献任何节点。
	t.Run("device without topology is ignored", func(t *testing.T) {
		m := ManagerImpl{
			allDevices: NewResourceDeviceInstances(),
			numaNodes:  []int{0, 4},
		}
		m.allDevices[resource] = DeviceInstances{
			"a": deviceOnNode("a", 4),
			"b": {ID: "b", Topology: nil},
		}

		nodes := m.deviceNUMANodes(logger, resource)
		expected := []int{4}
		if !reflect.DeepEqual(nodes, expected) {
			t.Fatalf("expected nodes %v, got %v", expected, nodes)
		}
	})

	// 没有任何带拓扑的设备时应返回空集合，而非全量节点。
	t.Run("returns empty when no device has topology", func(t *testing.T) {
		m := ManagerImpl{
			allDevices: NewResourceDeviceInstances(),
		}
		m.allDevices[resource] = DeviceInstances{
			"a": {ID: "a", Topology: nil},
			"b": {ID: "b", Topology: nil},
		}

		nodes := m.deviceNUMANodes(logger, resource)
		if len(nodes) != 0 {
			t.Fatalf("expected empty nodes, got %v", nodes)
		}
	})

	// 设备上报的未知 NUMA ID 必须丢弃，否则会生成越界的 bitmask。
	t.Run("unknown NUMA IDs from device plugin are dropped", func(t *testing.T) {
		m := ManagerImpl{
			allDevices: NewResourceDeviceInstances(),
			numaNodes:  []int{0, 1},
		}
		m.allDevices[resource] = DeviceInstances{
			"a": deviceOnNode("a", 0),
			"b": deviceOnNode("b", 99),
		}

		nodes := m.deviceNUMANodes(logger, resource)
		expected := []int{0}
		if !reflect.DeepEqual(nodes, expected) {
			t.Fatalf("expected nodes %v (node 99 should be dropped), got %v", expected, nodes)
		}
	})
}

// TestGenerateDeviceTopologyHintsFiltersNUMANodes 校验 hint 生成只使用设备实际
// 所在的 NUMA 节点，避免在大规模 NUMA 机器上枚举出指数级的无效组合。
func TestGenerateDeviceTopologyHintsFiltersNUMANodes(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	resource := "gpu"

	// 设备只在一个节点时，只应生成该节点的 preferred hint。
	t.Run("two node machine, device on one node", func(t *testing.T) {
		m := ManagerImpl{
			allDevices: NewResourceDeviceInstances(),
			numaNodes:  []int{0, 1},
		}
		m.allDevices[resource] = DeviceInstances{
			"a": {
				ID:       "a",
				Topology: &pluginapi.TopologyInfo{Nodes: []*pluginapi.NUMANode{{ID: 0}}},
			},
		}

		hints := m.generateDeviceTopologyHints(logger, resource, sets.New[string]("a"), nil, 1)

		maskNode0, _ := bitmask.NewBitMask(0)
		expected := []topologymanager.TopologyHint{
			{NUMANodeAffinity: maskNode0, Preferred: true},
		}

		if !reflect.DeepEqual(hints, expected) {
			t.Fatalf("expected hints %v, got %v", expected, hints)
		}
	})

	// 34 节点机器上枚举全部组合会爆炸；这里只应出现设备所在节点的组合。
	t.Run("large NUMA machine with devices on small subset", func(t *testing.T) {
		allNUMA := make([]int, 34)
		for i := range allNUMA {
			allNUMA[i] = i
		}

		m := ManagerImpl{
			allDevices: NewResourceDeviceInstances(),
			numaNodes:  allNUMA,
		}
		m.allDevices[resource] = DeviceInstances{
			"gpu0": {
				ID:       "gpu0",
				Topology: &pluginapi.TopologyInfo{Nodes: []*pluginapi.NUMANode{{ID: 0}}},
			},
			"gpu1": {
				ID:       "gpu1",
				Topology: &pluginapi.TopologyInfo{Nodes: []*pluginapi.NUMANode{{ID: 1}}},
			},
		}

		hints := m.generateDeviceTopologyHints(logger, resource, sets.New[string]("gpu0", "gpu1"), nil, 1)
		sort.SliceStable(hints, func(i, j int) bool { return hints[i].LessThan(hints[j]) })

		maskNode0, _ := bitmask.NewBitMask(0)
		maskNode1, _ := bitmask.NewBitMask(1)
		maskBoth, _ := bitmask.NewBitMask(0, 1)
		expected := []topologymanager.TopologyHint{
			{NUMANodeAffinity: maskNode0, Preferred: true},
			{NUMANodeAffinity: maskNode1, Preferred: true},
			{NUMANodeAffinity: maskBoth, Preferred: false},
		}
		sort.SliceStable(expected, func(i, j int) bool { return expected[i].LessThan(expected[j]) })

		if !reflect.DeepEqual(hints, expected) {
			t.Fatalf("expected hints %v, got %v", expected, hints)
		}
	})

	// 设备分布在两个节点时，仍应恰好生成一个覆盖整机的非 preferred hint。
	t.Run("devices span all NUMA nodes", func(t *testing.T) {
		m := ManagerImpl{
			allDevices: NewResourceDeviceInstances(),
			numaNodes:  []int{0, 1},
		}
		m.allDevices[resource] = DeviceInstances{
			"a": {
				ID:       "a",
				Topology: &pluginapi.TopologyInfo{Nodes: []*pluginapi.NUMANode{{ID: 0}}},
			},
			"b": {
				ID:       "b",
				Topology: &pluginapi.TopologyInfo{Nodes: []*pluginapi.NUMANode{{ID: 1}}},
			},
		}

		hints := m.generateDeviceTopologyHints(logger, resource, sets.New[string]("a", "b"), nil, 1)

		fullMask, _ := bitmask.NewBitMask(0, 1)
		fullMaskCount := 0
		for _, h := range hints {
			if h.NUMANodeAffinity.IsEqual(fullMask) {
				fullMaskCount++
			}
		}
		if fullMaskCount != 1 {
			t.Fatalf("expected exactly one full-machine mask, got %d in hints: %v", fullMaskCount, hints)
		}
	})

	// 可复用设备所在的节点即使不在 available 中，也必须被纳入。
	t.Run("reusable device on filtered node", func(t *testing.T) {
		m := ManagerImpl{
			allDevices: NewResourceDeviceInstances(),
			numaNodes:  []int{0, 1, 2, 3},
		}
		m.allDevices[resource] = DeviceInstances{
			"a": {
				ID:       "a",
				Topology: &pluginapi.TopologyInfo{Nodes: []*pluginapi.NUMANode{{ID: 0}}},
			},
		}

		hints := m.generateDeviceTopologyHints(logger, resource, nil, sets.New[string]("a"), 1)

		expected := []topologymanager.TopologyHint{
			{NUMANodeAffinity: makeSocketMask(0), Preferred: true},
		}
		if !reflect.DeepEqual(hints, expected) {
			t.Fatalf("expected hints %v, got %v", expected, hints)
		}
	})

	// 可复用与可用设备位于不同节点时，需要生成覆盖两者的跨节点 hint。
	t.Run("reusable and available on different nodes", func(t *testing.T) {
		m := ManagerImpl{
			allDevices: NewResourceDeviceInstances(),
			numaNodes:  []int{0, 1, 2, 3},
		}
		m.allDevices[resource] = DeviceInstances{
			"a": {
				ID:       "a",
				Topology: &pluginapi.TopologyInfo{Nodes: []*pluginapi.NUMANode{{ID: 0}}},
			},
			"b": {
				ID:       "b",
				Topology: &pluginapi.TopologyInfo{Nodes: []*pluginapi.NUMANode{{ID: 1}}},
			},
		}

		hints := m.generateDeviceTopologyHints(logger, resource, sets.New[string]("b"), sets.New[string]("a"), 2)

		expected := []topologymanager.TopologyHint{
			{NUMANodeAffinity: makeSocketMask(0, 1), Preferred: true},
		}
		if !reflect.DeepEqual(hints, expected) {
			t.Fatalf("expected hints %v, got %v", expected, hints)
		}
	})
}

// TestFilteredDeviceHintsMergeWithOtherProviders 在不搭建真实 CPU/memory manager
// 的前提下，用本代码生成的设备 hint 与其它 provider 的 hint 走 policy.Merge。
func TestFilteredDeviceHintsMergeWithOtherProviders(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	// 设备与 CPU/memory 的 preferred hint 都在 node 0，合并结果应落在 node 0。
	t.Run("device on node 0", func(t *testing.T) {
		numaNodes := []int{0, 1}
		numaInfo := &topologymanager.NUMAInfo{
			Nodes:         numaNodes,
			NUMADistances: topologymanager.NUMADistances{},
		}

		m := ManagerImpl{
			allDevices: NewResourceDeviceInstances(),
			numaNodes:  numaNodes,
		}
		m.allDevices["gpu"] = DeviceInstances{
			"gpu0": {
				ID:       "gpu0",
				Topology: &pluginapi.TopologyInfo{Nodes: []*pluginapi.NUMANode{{ID: 0}}},
			},
		}

		deviceHints := m.generateDeviceTopologyHints(logger, "gpu", sets.New[string]("gpu0"), nil, 1)

		providersHints := []map[string][]topologymanager.TopologyHint{
			{"gpu": deviceHints},
			{"cpu": {
				{NUMANodeAffinity: makeSocketMask(0), Preferred: true},
				{NUMANodeAffinity: makeSocketMask(0, 1), Preferred: false},
			}},
			{"memory": {
				{NUMANodeAffinity: makeSocketMask(0), Preferred: true},
				{NUMANodeAffinity: makeSocketMask(0, 1), Preferred: false},
			}},
		}

		assertMergePreferred(t, numaInfo, providersHints, makeSocketMask(0))
	})

	// 设备只在 node 1，合并结果必须跟随设备所在的节点。
	t.Run("device on non-zero node only", func(t *testing.T) {
		numaNodes := []int{0, 1}
		numaInfo := &topologymanager.NUMAInfo{
			Nodes:         numaNodes,
			NUMADistances: topologymanager.NUMADistances{},
		}

		m := ManagerImpl{
			allDevices: NewResourceDeviceInstances(),
			numaNodes:  numaNodes,
		}
		m.allDevices["gpu"] = DeviceInstances{
			"gpu0": {
				ID:       "gpu0",
				Topology: &pluginapi.TopologyInfo{Nodes: []*pluginapi.NUMANode{{ID: 1}}},
			},
		}

		deviceHints := m.generateDeviceTopologyHints(logger, "gpu", sets.New[string]("gpu0"), nil, 1)

		providersHints := []map[string][]topologymanager.TopologyHint{
			{"gpu": deviceHints},
			{"cpu": {
				{NUMANodeAffinity: makeSocketMask(1), Preferred: true},
				{NUMANodeAffinity: makeSocketMask(0, 1), Preferred: false},
			}},
			{"memory": {
				{NUMANodeAffinity: makeSocketMask(1), Preferred: true},
				{NUMANodeAffinity: makeSocketMask(0, 1), Preferred: false},
			}},
		}

		assertMergePreferred(t, numaInfo, providersHints, makeSocketMask(1))
	})

	// 机器不含 node 0 时，高编号节点同样应被正确处理。
	t.Run("high NUMA node IDs without node 0", func(t *testing.T) {
		numaNodes := []int{3, 7}
		numaInfo := &topologymanager.NUMAInfo{
			Nodes:         numaNodes,
			NUMADistances: topologymanager.NUMADistances{},
		}

		m := ManagerImpl{
			allDevices: NewResourceDeviceInstances(),
			numaNodes:  numaNodes,
		}
		m.allDevices["gpu"] = DeviceInstances{
			"gpu0": {
				ID:       "gpu0",
				Topology: &pluginapi.TopologyInfo{Nodes: []*pluginapi.NUMANode{{ID: 7}}},
			},
		}

		deviceHints := m.generateDeviceTopologyHints(logger, "gpu", sets.New[string]("gpu0"), nil, 1)

		providersHints := []map[string][]topologymanager.TopologyHint{
			{"gpu": deviceHints},
			{"cpu": {
				{NUMANodeAffinity: makeSocketMask(7), Preferred: true},
				{NUMANodeAffinity: makeSocketMask(3, 7), Preferred: false},
			}},
			{"memory": {
				{NUMANodeAffinity: makeSocketMask(7), Preferred: true},
				{NUMANodeAffinity: makeSocketMask(3, 7), Preferred: false},
			}},
		}

		assertMergePreferred(t, numaInfo, providersHints, makeSocketMask(7))
	})
}

// assertMergePreferred 在 best-effort 与 restricted 两种策略下验证合并后的
// best hint 等于 expectedMask 且为 preferred。
func assertMergePreferred(t *testing.T, numaInfo *topologymanager.NUMAInfo, providersHints []map[string][]topologymanager.TopologyHint, expectedMask bitmask.BitMask) {
	t.Helper()
	logger, _ := ktesting.NewTestContext(t)
	// 两种策略都要求最终 hint 为 preferred；restricted 策略下不满足会被拒绝。
	for _, policyName := range []string{"best-effort", "restricted"} {
		t.Run(policyName, func(t *testing.T) {
			var policy topologymanager.Policy
			switch policyName {
			case "best-effort":
				policy = topologymanager.NewBestEffortPolicy(numaInfo, topologymanager.PolicyOptions{})
			case "restricted":
				policy = topologymanager.NewRestrictedPolicy(numaInfo, topologymanager.PolicyOptions{})
			}

			bestHint, admit := policy.Merge(logger, providersHints)
			if !admit {
				t.Fatalf("expected pod to be admitted under %s policy", policyName)
			}
			if bestHint.NUMANodeAffinity == nil {
				t.Fatalf("expected non-nil NUMANodeAffinity")
			}
			if !bestHint.NUMANodeAffinity.IsEqual(expectedMask) {
				t.Fatalf("expected best hint %v, got %v", expectedMask, bestHint.NUMANodeAffinity)
			}
			if !bestHint.Preferred {
				t.Fatalf("expected best hint to be preferred")
			}
		})
	}
}

// topologyHintTestCase 是 hint 生成测试的通用用例结构，
// 同时被容器维度和 pod 维度的测试复用。
type topologyHintTestCase struct {
	// description 是用例描述，失败时用于定位。
	description string
	// pod 是被测 pod，包含设备资源请求。
	pod *v1.Pod
	// devices 按资源名索引该资源的可用设备列表。
	devices map[string][]*pluginapi.Device
	// allocatedDevices 记录已分配设备：pod -> 容器 -> 资源 -> 设备 ID。
	allocatedDevices map[string]map[string]map[string][]string
	// expectedHints 按资源名索引期望的 hint 集合。
	expectedHints map[string][]topologymanager.TopologyHint
}

// getCommonTestCases 返回容器维度与 pod 维度测试共用的基础用例。
func getCommonTestCases() []topologyHintTestCase {
	return []topologyHintTestCase{
		{
			description: "Single Request, no alignment",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					UID: "fakePod",
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "fakeContainer",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName("testdevice"): resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			devices: map[string][]*pluginapi.Device{
				"testdevice": {
					{ID: "Dev1"},
					{ID: "Dev2"},
					{ID: "Dev3", Topology: &pluginapi.TopologyInfo{Nodes: []*pluginapi.NUMANode{}}},
					{ID: "Dev4", Topology: &pluginapi.TopologyInfo{Nodes: nil}},
				},
			},
			expectedHints: map[string][]topologymanager.TopologyHint{
				"testdevice": nil,
			},
		},
		{
			description: "Single Request, only one with alignment",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					UID: "fakePod",
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "fakeContainer",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName("testdevice"): resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			devices: map[string][]*pluginapi.Device{
				"testdevice": {
					{ID: "Dev1"},
					makeNUMADevice("Dev2", 1),
				},
			},
			expectedHints: map[string][]topologymanager.TopologyHint{
				"testdevice": {
					{
						NUMANodeAffinity: makeSocketMask(1),
						Preferred:        true,
					},
				},
			},
		},
		{
			description: "Single Request, one device per socket",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					UID: "fakePod",
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "fakeContainer",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName("testdevice"): resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			devices: map[string][]*pluginapi.Device{
				"testdevice": {
					makeNUMADevice("Dev1", 0),
					makeNUMADevice("Dev2", 1),
				},
			},
			expectedHints: map[string][]topologymanager.TopologyHint{
				"testdevice": {
					{
						NUMANodeAffinity: makeSocketMask(0),
						Preferred:        true,
					},
					{
						NUMANodeAffinity: makeSocketMask(1),
						Preferred:        true,
					},
					{
						NUMANodeAffinity: makeSocketMask(0, 1),
						Preferred:        false,
					},
				},
			},
		},
		{
			description: "Request for 2, one device per socket",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					UID: "fakePod",
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "fakeContainer",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName("testdevice"): resource.MustParse("2"),
								},
							},
						},
					},
				},
			},
			devices: map[string][]*pluginapi.Device{
				"testdevice": {
					makeNUMADevice("Dev1", 0),
					makeNUMADevice("Dev2", 1),
				},
			},
			expectedHints: map[string][]topologymanager.TopologyHint{
				"testdevice": {
					{
						NUMANodeAffinity: makeSocketMask(0, 1),
						Preferred:        true,
					},
				},
			},
		},
		{
			description: "Request for 2, 2 devices per socket",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					UID: "fakePod",
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "fakeContainer",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName("testdevice"): resource.MustParse("2"),
								},
							},
						},
					},
				},
			},
			devices: map[string][]*pluginapi.Device{
				"testdevice": {
					makeNUMADevice("Dev1", 0),
					makeNUMADevice("Dev2", 1),
					makeNUMADevice("Dev3", 0),
					makeNUMADevice("Dev4", 1),
				},
			},
			expectedHints: map[string][]topologymanager.TopologyHint{
				"testdevice": {
					{
						NUMANodeAffinity: makeSocketMask(0),
						Preferred:        true,
					},
					{
						NUMANodeAffinity: makeSocketMask(1),
						Preferred:        true,
					},
					{
						NUMANodeAffinity: makeSocketMask(0, 1),
						Preferred:        false,
					},
				},
			},
		},
		{
			description: "Request for 2, optimal on 1 NUMA node, forced cross-NUMA",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					UID: "fakePod",
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "fakeContainer",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName("testdevice"): resource.MustParse("2"),
								},
							},
						},
					},
				},
			},
			devices: map[string][]*pluginapi.Device{
				"testdevice": {
					makeNUMADevice("Dev1", 0),
					makeNUMADevice("Dev2", 1),
					makeNUMADevice("Dev3", 0),
					makeNUMADevice("Dev4", 1),
				},
			},
			allocatedDevices: map[string]map[string]map[string][]string{
				"fakePod": {
					"fakeOtherContainer": {
						"testdevice": {"Dev1", "Dev2"},
					},
				},
			},
			expectedHints: map[string][]topologymanager.TopologyHint{
				"testdevice": {
					{
						NUMANodeAffinity: makeSocketMask(0, 1),
						Preferred:        false,
					},
				},
			},
		},
		{
			description: "2 device types, mixed configuration",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					UID: "fakePod",
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "fakeContainer",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName("testdevice1"): resource.MustParse("2"),
									v1.ResourceName("testdevice2"): resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			devices: map[string][]*pluginapi.Device{
				"testdevice1": {
					makeNUMADevice("Dev1", 0),
					makeNUMADevice("Dev2", 1),
					makeNUMADevice("Dev3", 0),
					makeNUMADevice("Dev4", 1),
				},
				"testdevice2": {
					makeNUMADevice("Dev1", 0),
				},
			},
			expectedHints: map[string][]topologymanager.TopologyHint{
				"testdevice1": {
					{
						NUMANodeAffinity: makeSocketMask(0),
						Preferred:        true,
					},
					{
						NUMANodeAffinity: makeSocketMask(1),
						Preferred:        true,
					},
					{
						NUMANodeAffinity: makeSocketMask(0, 1),
						Preferred:        false,
					},
				},
				"testdevice2": {
					{
						NUMANodeAffinity: makeSocketMask(0),
						Preferred:        true,
					},
				},
			},
		},
		{
			description: "Single device type, more requested than available",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					UID: "fakePod",
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "fakeContainer",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName("testdevice"): resource.MustParse("6"),
								},
							},
						},
					},
				},
			},
			devices: map[string][]*pluginapi.Device{
				"testdevice": {
					makeNUMADevice("Dev1", 0),
					makeNUMADevice("Dev2", 0),
					makeNUMADevice("Dev3", 1),
					makeNUMADevice("Dev4", 1),
				},
			},
			expectedHints: map[string][]topologymanager.TopologyHint{
				"testdevice": {},
			},
		},
		{
			description: "Single device type, all already allocated to container",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					UID: "fakePod",
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "fakeContainer",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName("testdevice"): resource.MustParse("2"),
								},
							},
						},
					},
				},
			},
			devices: map[string][]*pluginapi.Device{
				"testdevice": {
					makeNUMADevice("Dev1", 0),
					makeNUMADevice("Dev2", 0),
				},
			},
			allocatedDevices: map[string]map[string]map[string][]string{
				"fakePod": {
					"fakeContainer": {
						"testdevice": {"Dev1", "Dev2"},
					},
				},
			},
			expectedHints: map[string][]topologymanager.TopologyHint{
				"testdevice": {
					{
						NUMANodeAffinity: makeSocketMask(0),
						Preferred:        true,
					},
				},
			},
		},
		{
			description: "Single device type, less already allocated to container than requested",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					UID: "fakePod",
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "fakeContainer",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName("testdevice"): resource.MustParse("4"),
								},
							},
						},
					},
				},
			},
			devices: map[string][]*pluginapi.Device{
				"testdevice": {
					makeNUMADevice("Dev1", 0),
					makeNUMADevice("Dev2", 0),
					makeNUMADevice("Dev3", 1),
					makeNUMADevice("Dev4", 1),
				},
			},
			allocatedDevices: map[string]map[string]map[string][]string{
				"fakePod": {
					"fakeContainer": {
						"testdevice": {"Dev1", "Dev2"},
					},
				},
			},
			expectedHints: map[string][]topologymanager.TopologyHint{
				"testdevice": {},
			},
		},
		{
			description: "Single device type, more already allocated to container than requested",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					UID: "fakePod",
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "fakeContainer",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName("testdevice"): resource.MustParse("2"),
								},
							},
						},
					},
				},
			},
			devices: map[string][]*pluginapi.Device{
				"testdevice": {
					makeNUMADevice("Dev1", 0),
					makeNUMADevice("Dev2", 0),
					makeNUMADevice("Dev3", 1),
					makeNUMADevice("Dev4", 1),
				},
			},
			allocatedDevices: map[string]map[string]map[string][]string{
				"fakePod": {
					"fakeContainer": {
						"testdevice": {"Dev1", "Dev2", "Dev3", "Dev4"},
					},
				},
			},
			expectedHints: map[string][]topologymanager.TopologyHint{
				"testdevice": {},
			},
		},
	}
}

// getPodScopeTestCases 返回只在 pod 维度生效的用例：多容器共享设备池、
// 未注册资源被忽略等场景。
func getPodScopeTestCases() []topologyHintTestCase {
	return []topologyHintTestCase{
		{
			description: "2 device types, user container only",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					UID: "fakePod",
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "fakeContainer1",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName("testdevice1"): resource.MustParse("2"),
								},
							},
						},
						{
							Name: "fakeContainer2",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName("testdevice2"): resource.MustParse("2"),
								},
							},
						},
						{
							Name: "fakeContainer3",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName("notRegistered"): resource.MustParse("2"),
								},
							},
						},
					},
				},
			},
			devices: map[string][]*pluginapi.Device{
				"testdevice1": {
					makeNUMADevice("Dev1", 0),
					makeNUMADevice("Dev2", 0),
					makeNUMADevice("Dev3", 1),
					makeNUMADevice("Dev4", 1),
				},
				"testdevice2": {
					makeNUMADevice("Dev1", 0),
					makeNUMADevice("Dev2", 0),
					makeNUMADevice("Dev3", 1),
					makeNUMADevice("Dev4", 1),
				},
			},
			expectedHints: map[string][]topologymanager.TopologyHint{
				"testdevice1": {
					{
						NUMANodeAffinity: makeSocketMask(0),
						Preferred:        true,
					},
					{
						NUMANodeAffinity: makeSocketMask(1),
						Preferred:        true,
					},
					{
						NUMANodeAffinity: makeSocketMask(0, 1),
						Preferred:        false,
					},
				},
				"testdevice2": {
					{
						NUMANodeAffinity: makeSocketMask(0),
						Preferred:        true,
					},
					{
						NUMANodeAffinity: makeSocketMask(1),
						Preferred:        true,
					},
					{
						NUMANodeAffinity: makeSocketMask(0, 1),
						Preferred:        false,
					},
				},
			},
		},
		{
			description: "2 device types, request resources for init containers and user container",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					UID: "fakePod",
				},
				Spec: v1.PodSpec{
					InitContainers: []v1.Container{
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName("testdevice1"): resource.MustParse("1"),
									v1.ResourceName("testdevice2"): resource.MustParse("1"),
								},
							},
						},
						{
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName("testdevice1"): resource.MustParse("1"),
									v1.ResourceName("testdevice2"): resource.MustParse("2"),
								},
							},
						},
					},
					Containers: []v1.Container{
						{
							Name: "fakeContainer1",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName("testdevice1"): resource.MustParse("1"),
									v1.ResourceName("testdevice2"): resource.MustParse("1"),
								},
							},
						},
						{
							Name: "fakeContainer2",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName("testdevice1"): resource.MustParse("1"),
									v1.ResourceName("testdevice2"): resource.MustParse("1"),
								},
							},
						},
						{
							Name: "fakeContainer3",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName("notRegistered"): resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			devices: map[string][]*pluginapi.Device{
				"testdevice1": {
					makeNUMADevice("Dev1", 0),
					makeNUMADevice("Dev2", 0),
					makeNUMADevice("Dev3", 1),
					makeNUMADevice("Dev4", 1),
				},
				"testdevice2": {
					makeNUMADevice("Dev1", 0),
					makeNUMADevice("Dev2", 0),
					makeNUMADevice("Dev3", 1),
					makeNUMADevice("Dev4", 1),
				},
			},
			expectedHints: map[string][]topologymanager.TopologyHint{
				"testdevice1": {
					{
						NUMANodeAffinity: makeSocketMask(0),
						Preferred:        true,
					},
					{
						NUMANodeAffinity: makeSocketMask(1),
						Preferred:        true,
					},
					{
						NUMANodeAffinity: makeSocketMask(0, 1),
						Preferred:        false,
					},
				},
				"testdevice2": {
					{
						NUMANodeAffinity: makeSocketMask(0),
						Preferred:        true,
					},
					{
						NUMANodeAffinity: makeSocketMask(1),
						Preferred:        true,
					},
					{
						NUMANodeAffinity: makeSocketMask(0, 1),
						Preferred:        false,
					},
				},
			},
		},
		{
			description: "2 device types, user container only, optimal on 1 NUMA node, forced cross-NUMA",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					UID: "fakePod",
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "fakeContainer1",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName("testdevice1"): resource.MustParse("1"),
									v1.ResourceName("testdevice2"): resource.MustParse("1"),
								},
							},
						},
						{
							Name: "fakeContainer2",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName("testdevice1"): resource.MustParse("1"),
									v1.ResourceName("testdevice2"): resource.MustParse("1"),
								},
							},
						},
						{
							Name: "fakeContainer3",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName("notRegistered"): resource.MustParse("1"),
								},
							},
						},
					},
				},
			},
			devices: map[string][]*pluginapi.Device{
				"testdevice1": {
					makeNUMADevice("Dev1", 0),
					makeNUMADevice("Dev2", 0),
					makeNUMADevice("Dev3", 1),
					makeNUMADevice("Dev4", 1),
				},
				"testdevice2": {
					makeNUMADevice("Dev1", 0),
					makeNUMADevice("Dev2", 0),
					makeNUMADevice("Dev3", 1),
					makeNUMADevice("Dev4", 1),
				},
			},
			allocatedDevices: map[string]map[string]map[string][]string{
				"fakeOtherPod": {
					"fakeOtherContainer": {
						"testdevice1": {"Dev1", "Dev3"},
						"testdevice2": {"Dev1", "Dev3"},
					},
				},
			},
			expectedHints: map[string][]topologymanager.TopologyHint{
				"testdevice1": {
					{
						NUMANodeAffinity: makeSocketMask(0, 1),
						Preferred:        false,
					},
				},
				"testdevice2": {
					{
						NUMANodeAffinity: makeSocketMask(0, 1),
						Preferred:        false,
					},
				},
			},
		},
	}
}

// 以下生命周期测试验证 device manager 是否根据传入的 lifecycle.Operation
// 处理或跳过操作。
// GetTopologyHints* 在操作不受支持时不返回错误（而是静默返回空 hints，
// 以免中断所有 hint provider 的整体流程），因此测试通过日志输出来确认
// 操作是被处理还是被跳过。
//
// 测试中使用的取值（如设备 ID、pod 资源请求）是任意但合法的，仅用于让代码
// 走通正常路径；具体数值没有特殊含义。这些测试不校验 hint 结果是否正确，
// 只校验给定生命周期操作下操作是否被处理或跳过。

// TestLifecycleGetTopologyHints 校验容器维度 hint 生成对生命周期操作的处理；
// device manager 目前不区分操作类型，均正常执行。
func TestLifecycleGetTopologyHints(t *testing.T) {
	testCases := []struct {
		description string
		operation   lifecycle.Operation
		skipped     bool
	}{
		{
			description: "DeviceManager ignores AddOperation and runs",
			operation:   lifecycle.AddOperation,
			skipped:     false,
		},
		{
			description: "DeviceManager ignores ResizeOperation and runs",
			operation:   lifecycle.ResizeOperation,
			skipped:     false,
		},
		{
			description: "DeviceManager ignores empty operation and runs",
			operation:   "",
			skipped:     false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)

			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					UID: "fakePod",
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "fakeContainer1",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName("testdevice1"): resource.MustParse("2"),
								},
							},
						},
						{
							Name: "fakeContainer2",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName("testdevice2"): resource.MustParse("2"),
								},
							},
						},
						{
							Name: "fakeContainer3",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName("notRegistered"): resource.MustParse("2"),
								},
							},
						},
					},
				},
			}
			devices := map[string][]*pluginapi.Device{
				"testdevice1": {
					makeNUMADevice("Dev1", 0),
					makeNUMADevice("Dev2", 0),
					makeNUMADevice("Dev3", 1),
					makeNUMADevice("Dev4", 1),
				},
				"testdevice2": {
					makeNUMADevice("Dev1", 0),
					makeNUMADevice("Dev2", 0),
					makeNUMADevice("Dev3", 1),
					makeNUMADevice("Dev4", 1),
				},
			}

			m := ManagerImpl{
				allDevices:       NewResourceDeviceInstances(),
				healthyDevices:   make(map[string]sets.Set[string]),
				allocatedDevices: make(map[string]sets.Set[string]),
				podDevices:       newPodDevices(),
				sourcesReady:     &sourcesReadyStub{},
				activePods: func() []*v1.Pod {
					return []*v1.Pod{pod, {ObjectMeta: metav1.ObjectMeta{UID: "fakeOtherPod"}}}
				},
				numaNodes: []int{0, 1},
			}

			for r := range devices {
				m.allDevices[r] = make(DeviceInstances)
				m.healthyDevices[r] = sets.New[string]()

				for _, d := range devices[r] {
					m.allDevices[r][d.ID] = d
					m.healthyDevices[r].Insert(d.ID)
				}
			}

			hints := m.GetTopologyHints(logger, pod, &pod.Spec.Containers[0], testCase.operation)
			if testCase.skipped {
				require.Empty(t, hints)
			} else {
				require.NotEmpty(t, hints)
			}
		})
	}
}

// TestLifecycleGetPodTopologyHints 校验 pod 维度 hint 生成对生命周期操作的处理；
// 当前所有操作均被支持。
func TestLifecycleGetPodTopologyHints(t *testing.T) {
	testCases := []struct {
		description string
		operation   lifecycle.Operation
		skipped     bool
	}{
		{
			description: "DeviceManager ignores AddOperation and runs",
			operation:   lifecycle.AddOperation,
			skipped:     false,
		},
		{
			description: "DeviceManager ignores ResizeOperation and runs",
			operation:   lifecycle.ResizeOperation,
			skipped:     false,
		},
		{
			description: "DeviceManager ignores empty operation and runs",
			operation:   "",
			skipped:     false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.description, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)

			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					UID: "fakePod",
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{
							Name: "fakeContainer1",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName("testdevice1"): resource.MustParse("2"),
								},
							},
						},
						{
							Name: "fakeContainer2",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName("testdevice2"): resource.MustParse("2"),
								},
							},
						},
						{
							Name: "fakeContainer3",
							Resources: v1.ResourceRequirements{
								Limits: v1.ResourceList{
									v1.ResourceName("notRegistered"): resource.MustParse("2"),
								},
							},
						},
					},
				},
			}
			devices := map[string][]*pluginapi.Device{
				"testdevice1": {
					makeNUMADevice("Dev1", 0),
					makeNUMADevice("Dev2", 0),
					makeNUMADevice("Dev3", 1),
					makeNUMADevice("Dev4", 1),
				},
				"testdevice2": {
					makeNUMADevice("Dev1", 0),
					makeNUMADevice("Dev2", 0),
					makeNUMADevice("Dev3", 1),
					makeNUMADevice("Dev4", 1),
				},
			}

			m := ManagerImpl{
				allDevices:       NewResourceDeviceInstances(),
				healthyDevices:   make(map[string]sets.Set[string]),
				allocatedDevices: make(map[string]sets.Set[string]),
				podDevices:       newPodDevices(),
				sourcesReady:     &sourcesReadyStub{},
				activePods: func() []*v1.Pod {
					return []*v1.Pod{pod, {ObjectMeta: metav1.ObjectMeta{UID: "fakeOtherPod"}}}
				},
				numaNodes: []int{0, 1},
			}

			for r := range devices {
				m.allDevices[r] = make(DeviceInstances)
				m.healthyDevices[r] = sets.New[string]()

				for _, d := range devices[r] {
					m.allDevices[r][d.ID] = d
					m.healthyDevices[r].Insert(d.ID)
				}
			}

			hints := m.GetPodTopologyHints(logger, pod, testCase.operation)
			if testCase.skipped {
				require.Empty(t, hints)
			} else {
				require.NotEmpty(t, hints)
			}
		})
	}
}
