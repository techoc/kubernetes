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

package deviceattribute

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	resourceapi "k8s.io/api/resource/v1"
)

// TestGetNUMANodeAttributeByPCIBusID 表驱动测试 GetNUMANodeAttributeByPCIBusID：
// 用临时目录模拟 sysfs，输入为 PCI 地址与属性形式，覆盖标量/列表形式、同插槽过滤、
// 无 NUMA 亲和性、空地址及缺失 numa_node 等场景；断言返回的 DeviceAttribute 或错误信息符合预期。
func TestGetNUMANodeAttributeByPCIBusID(t *testing.T) {
	pciBusID := "0000:02:00.0"
	// 该 PCI 设备 numa_node 属性在模拟 sysfs 中的相对路径。
	numaNodePath := filepath.Join("bus", "pci", "devices", pciBusID, "numa_node")

	tests := map[string]struct {
		setup             func(t *testing.T, root string)
		address           string
		attrForm          AttributeForm
		expectedAttribute *DeviceAttribute
		expectsError      bool
		expectedErrMsg    string
	}{
		"scalar": {
			setup: func(t *testing.T, root string) {
				writeFile(t, filepath.Join(root, numaNodePath), "4\n")
			},
			address:  pciBusID,
			attrForm: ScalarAttribute,
			expectedAttribute: &DeviceAttribute{
				Name:  StandardDeviceAttributeNUMANode,
				Value: resourceapi.DeviceAttribute{IntValue: new(int64(4))},
			},
		},
		"list with equidistant nodes": {
			setup: func(t *testing.T, root string) {
				writeFile(t, filepath.Join(root, numaNodePath), "1\n")
				// node1 SLIT row: node0=12, node1(self)=10, node2=12.
				// Minimum non-self distance is 12 -> {0,2}; physical node first.
				// No cpulist/topology files, so the socket filter is inactive and
				// all minimum-distance nodes are kept.
				writeFile(t, filepath.Join(root, "devices", "system", "node", "node1", "distance"), "12 10 12\n")
			},
			address:  pciBusID,
			attrForm: ListAttribute,
			expectedAttribute: &DeviceAttribute{
				Name:  StandardDeviceAttributeNUMANode,
				Value: resourceapi.DeviceAttribute{IntValues: []int64{1, 0, 2}},
			},
		},
		"list single NUMA node": {
			setup: func(t *testing.T, root string) {
				writeFile(t, filepath.Join(root, numaNodePath), "0\n")
				// Only a self distance, so there are no equidistant peers.
				writeFile(t, filepath.Join(root, "devices", "system", "node", "node0", "distance"), "10\n")
			},
			address:  pciBusID,
			attrForm: ListAttribute,
			expectedAttribute: &DeviceAttribute{
				Name:  StandardDeviceAttributeNUMANode,
				Value: resourceapi.DeviceAttribute{IntValues: []int64{0}},
			},
		},
		"list with same-socket filter": {
			setup: func(t *testing.T, root string) {
				writeFile(t, filepath.Join(root, numaNodePath), "0\n")
				// node0 self=10; node1 and node2 both at minimum distance 12; node3 farther.
				writeFile(t, filepath.Join(root, "devices", "system", "node", "node0", "distance"), "10 12 12 20\n")
				// node0 and node1 are on socket 0; node2 is on socket 1, so node2 is filtered out.
				writeFile(t, filepath.Join(root, "devices", "system", "node", "node0", "cpulist"), "0\n")
				writeFile(t, filepath.Join(root, "devices", "system", "node", "node1", "cpulist"), "1\n")
				writeFile(t, filepath.Join(root, "devices", "system", "node", "node2", "cpulist"), "2\n")
				writeFile(t, filepath.Join(root, "devices", "system", "cpu", "cpu0", "topology", "physical_package_id"), "0\n")
				writeFile(t, filepath.Join(root, "devices", "system", "cpu", "cpu1", "topology", "physical_package_id"), "0\n")
				writeFile(t, filepath.Join(root, "devices", "system", "cpu", "cpu2", "topology", "physical_package_id"), "1\n")
			},
			address:  pciBusID,
			attrForm: ListAttribute,
			expectedAttribute: &DeviceAttribute{
				Name:  StandardDeviceAttributeNUMANode,
				Value: resourceapi.DeviceAttribute{IntValues: []int64{0, 1}},
			},
		},
		"no NUMA affinity is rejected": {
			setup: func(t *testing.T, root string) {
				writeFile(t, filepath.Join(root, numaNodePath), "-1\n")
			},
			address:        pciBusID,
			attrForm:       ListAttribute,
			expectsError:   true,
			expectedErrMsg: "no NUMA affinity",
		},
		"empty PCI Bus ID": {
			address:        "",
			attrForm:       ListAttribute,
			expectsError:   true,
			expectedErrMsg: "PCI Bus ID cannot be empty",
		},
		"missing numa_node": {
			address:        pciBusID,
			attrForm:       ScalarAttribute,
			expectsError:   true,
			expectedErrMsg: "failed to read NUMA node",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			// 每个用例使用独立的临时目录，避免相互污染。
			root := t.TempDir()
			if test.setup != nil {
				test.setup(t, root)
			}
			// 用 WithFSFromRoot 把被测函数指向模拟的 sysfs 根目录。
			got, err := GetNUMANodeAttributeByPCIBusID(test.address, test.attrForm, WithFSFromRoot(root))
			if test.expectsError {
				// 错误路径：先断言确实出错，再核对错误关键信息。
				if err == nil {
					t.Fatalf("expected error but got none")
				}
				if !strings.Contains(err.Error(), test.expectedErrMsg) {
					t.Errorf("expected error containing %q, got %q", test.expectedErrMsg, err.Error())
				}
				return
			}
			// 成功路径：不应有错误，且返回属性需与期望完全一致。
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, *test.expectedAttribute) {
				t.Errorf("expected %+v, got %+v", *test.expectedAttribute, got)
			}
		})
	}
}

// TestGetNUMANodeAttribute 表驱动测试已知节点号场景下的 GetNUMANodeAttribute。
// 子用例覆盖：标量形式的正确性、列表形式在等距节点下的展开、以及负节点号（无 NUMA 亲和性）被拒绝。
// 断言既检查返回属性的名称与值，也检查标量/列表形式中不应出现的字段确实为 nil。
func TestGetNUMANodeAttribute(t *testing.T) {
	t.Run("scalar", func(t *testing.T) {
		// 输入节点号 4 与标量形式，期望 IntValue=4 且 IntValues 为 nil。
		got, err := GetNUMANodeAttribute(4, ScalarAttribute)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := DeviceAttribute{
			Name:  StandardDeviceAttributeNUMANode,
			Value: resourceapi.DeviceAttribute{IntValue: new(int64(4))},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("expected %+v, got %+v", want, got)
		}
		if got.Value.IntValues != nil {
			t.Errorf("expected nil IntValues for scalar attribute, got %v", got.Value.IntValues)
		}
	})

	t.Run("list with equidistant nodes", func(t *testing.T) {
		// 模拟 node1 的 SLIT 距离行，最小非自身距离为 12，对应节点 0 和 2。
		root := t.TempDir()
		writeFile(t, filepath.Join(root, "devices", "system", "node", "node1", "distance"), "12 10 12\n")
		got, err := GetNUMANodeAttribute(1, ListAttribute, WithFSFromRoot(root))
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		// 期望列表以物理节点 1 开头，后接等距节点 0、2。
		want := DeviceAttribute{
			Name:  StandardDeviceAttributeNUMANode,
			Value: resourceapi.DeviceAttribute{IntValues: []int64{1, 0, 2}},
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("expected %+v, got %+v", want, got)
		}
		if got.Value.IntValue != nil {
			t.Errorf("expected nil IntValue for list attribute, got %v", *got.Value.IntValue)
		}
	})

	t.Run("no NUMA affinity is rejected", func(t *testing.T) {
		// 输入 -1 表示无 NUMA 亲和性，期望返回包含 invalid NUMA node 的错误。
		_, err := GetNUMANodeAttribute(-1, ScalarAttribute)
		if err == nil {
			t.Fatal("expected error for a device with no NUMA affinity, got none")
		}
		if !strings.Contains(err.Error(), "invalid NUMA node") {
			t.Errorf("expected error about invalid NUMA node, got %q", err.Error())
		}
	})
}

// TestGetNUMANodeForCPU 表驱动测试 GetNUMANodeForCPU：
// 输入为 CPU 号及模拟的 cpuX/nodeY 目录，覆盖 CPU 属于 node1、属于 node0 以及不存在三种情况；
// 断言正确返回节点号，或返回包含 not found in any NUMA node 的错误。
func TestGetNUMANodeForCPU(t *testing.T) {
	tests := map[string]struct {
		setup       func(t *testing.T, root string)
		cpuID       int
		wantNode    int
		expectsErr  bool
		errContains string
	}{
		"cpu on node1": {
			setup: func(t *testing.T, root string) {
				mkDirAll(t, filepath.Join(root, "devices", "system", "cpu", "cpu5", "node1"))
			},
			cpuID:    5,
			wantNode: 1,
		},
		"cpu on node0": {
			setup: func(t *testing.T, root string) {
				mkDirAll(t, filepath.Join(root, "devices", "system", "cpu", "cpu9", "node0"))
			},
			cpuID:    9,
			wantNode: 0,
		},
		"cpu not found": {
			setup:       func(t *testing.T, root string) {},
			cpuID:       99,
			expectsErr:  true,
			errContains: "not found in any NUMA node",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			// 每个用例独立临时目录，按 setup 创建对应的 nodeY 链接目录。
			root := t.TempDir()
			if test.setup != nil {
				test.setup(t, root)
			}
			got, err := GetNUMANodeForCPU(test.cpuID, WithFSFromRoot(root))
			if test.expectsErr {
				// 错误路径：断言出错且错误信息包含预期子串。
				if err == nil {
					t.Fatalf("expected error but got node %d", got)
				}
				if !strings.Contains(err.Error(), test.errContains) {
					t.Errorf("expected error containing %q, got %q", test.errContains, err.Error())
				}
				return
			}
			// 成功路径：断言返回的 NUMA 节点号正确。
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != test.wantNode {
				t.Errorf("expected NUMA node %d, got %d", test.wantNode, got)
			}
		})
	}
}

// writeFile 是测试辅助函数：先递归创建父目录，再把 content 写入指定路径，失败即终止测试。
func writeFile(t *testing.T, path, content string) {
	t.Helper()
	mkDirAll(t, filepath.Dir(path))
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to write file %s: %v", path, err)
	}
}
