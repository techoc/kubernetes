/*
Copyright 2018 The Kubernetes Authors.

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

package cache

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2/ktesting"
)

// allNodes 是一组覆盖各种 region/zone 标签组合的节点，供拓扑树测试复用：
// 含无标签、仅 region、仅 zone、正常 zone、旧标签以及跨 region 的节点。
var allNodes = []*v1.Node{
	// 节点 0：没有任何 region/zone 标签
	{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-0",
		},
	},
	// 节点 1：仅有 region 标签
	{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-1",
			Labels: map[string]string{
				v1.LabelTopologyRegion: "region-1",
			},
		},
	},
	// 节点 2：仅有 zone 标签
	{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-2",
			Labels: map[string]string{
				v1.LabelTopologyZone: "zone-2",
			},
		},
	},
	// 节点 3：同时带有正常的 region 与 zone 标签
	{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-3",
			Labels: map[string]string{
				v1.LabelTopologyRegion: "region-1",
				v1.LabelTopologyZone:   "zone-2",
			},
		},
	},
	// 节点 4：与节点 3 同 region/zone，用于测试同 zone 多节点
	{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-4",
			Labels: map[string]string{
				v1.LabelTopologyRegion: "region-1",
				v1.LabelTopologyZone:   "zone-2",
			},
		},
	},
	// 节点 5：与上面同 region、但不同 zone
	{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-5",
			Labels: map[string]string{
				v1.LabelTopologyRegion: "region-1",
				v1.LabelTopologyZone:   "zone-3",
			},
		},
	},
	// 节点 6：全新的 region 与 zone
	{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-6",
			Labels: map[string]string{
				v1.LabelTopologyRegion: "region-2",
				v1.LabelTopologyZone:   "zone-2",
			},
		},
	},
	// 节点 7：与节点 6 同 region/zone
	{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-7",
			Labels: map[string]string{
				v1.LabelTopologyRegion: "region-2",
				v1.LabelTopologyZone:   "zone-2",
			},
		},
	},
	// 节点 8：与节点 6 同 region/zone
	{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-8",
			Labels: map[string]string{
				v1.LabelTopologyRegion: "region-2",
				v1.LabelTopologyZone:   "zone-2",
			},
		},
	},
	// 节点 9：同时带有新旧两套 region/zone 标签
	{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-9",
			Labels: map[string]string{
				v1.LabelTopologyRegion:          "region-2",
				v1.LabelTopologyZone:            "zone-2",
				v1.LabelFailureDomainBetaRegion: "region-2",
				v1.LabelFailureDomainBetaZone:   "zone-2",
			},
		},
	},
	// 节点 10：仅带有已废弃的 region/zone 标签
	{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node-10",
			Labels: map[string]string{
				v1.LabelFailureDomainBetaRegion: "region-2",
				v1.LabelFailureDomainBetaZone:   "zone-3",
			},
		},
	},
}

// verifyNodeTree 统一校验树的三个维度：节点总数、zone 到节点列表的映射以及 zones 集合。
// 避免每个测试重复编写断言，也保证 numNodes 与 tree 内容不会各自独立出错。
func verifyNodeTree(t *testing.T, nt *nodeTree, expectedTree map[string][]string) {
	expectedNumNodes := int(0)
	for _, na := range expectedTree {
		expectedNumNodes += len(na)
	}
	if numNodes := nt.numNodes; numNodes != expectedNumNodes {
		t.Errorf("unexpected nodeTree.numNodes. Expected: %v, Got: %v", expectedNumNodes, numNodes)
	}
	if diff := cmp.Diff(expectedTree, nt.tree); diff != "" {
		t.Errorf("Unexpected node tree (-want, +got):\n%s", diff)
	}
	if len(nt.zones) != len(expectedTree) {
		t.Errorf("Number of zones in nodeTree.zones is not expected. Expected: %v, Got: %v", len(expectedTree), len(nt.zones))
	}
	for _, z := range nt.zones {
		if _, ok := expectedTree[z]; !ok {
			t.Errorf("zone %v is not expected to exist in nodeTree.zones", z)
		}
	}
}

// TestNodeTree_AddNode 验证 addNode 按 region/zone 标签把节点放入正确的 zone 桶，
// 覆盖无标签、重复添加、标签残缺及旧标签节点。
func TestNodeTree_AddNode(t *testing.T) {
	tests := []struct {
		name         string
		nodesToAdd   []*v1.Node
		expectedTree map[string][]string
	}{
		{
			name:         "single node no labels",
			nodesToAdd:   allNodes[:1],
			expectedTree: map[string][]string{"": {"node-0"}},
		},
		{
			name:         "same node specified twice",
			nodesToAdd:   []*v1.Node{allNodes[0], allNodes[0]},
			expectedTree: map[string][]string{"": {"node-0"}},
		},
		{
			name:       "mix of nodes with and without proper labels",
			nodesToAdd: allNodes[:4],
			expectedTree: map[string][]string{
				"":                     {"node-0"},
				"region-1:\x00:":       {"node-1"},
				":\x00:zone-2":         {"node-2"},
				"region-1:\x00:zone-2": {"node-3"},
			},
		},
		{
			name:       "mix of nodes with and without proper labels and some zones with multiple nodes",
			nodesToAdd: allNodes[:7],
			expectedTree: map[string][]string{
				"":                     {"node-0"},
				"region-1:\x00:":       {"node-1"},
				":\x00:zone-2":         {"node-2"},
				"region-1:\x00:zone-2": {"node-3", "node-4"},
				"region-1:\x00:zone-3": {"node-5"},
				"region-2:\x00:zone-2": {"node-6"},
			},
		},
		{
			name:       "nodes also using deprecated zone/region label",
			nodesToAdd: allNodes[9:],
			expectedTree: map[string][]string{
				"region-2:\x00:zone-2": {"node-9"},
				"region-2:\x00:zone-3": {"node-10"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)
			nt := newNodeTree(logger, nil)
			for _, n := range test.nodesToAdd {
				nt.addNode(logger, n)
			}
			verifyNodeTree(t, nt, test.expectedTree)
		})
	}
}

// TestNodeTree_RemoveNode 验证 removeNode 从对应 zone 移除节点，并在节点不存在时返回错误。
func TestNodeTree_RemoveNode(t *testing.T) {
	tests := []struct {
		name          string
		existingNodes []*v1.Node
		nodesToRemove []*v1.Node
		expectedTree  map[string][]string
		expectError   bool
	}{
		{
			name:          "remove a single node with no labels",
			existingNodes: allNodes[:7],
			nodesToRemove: allNodes[:1],
			expectedTree: map[string][]string{
				"region-1:\x00:":       {"node-1"},
				":\x00:zone-2":         {"node-2"},
				"region-1:\x00:zone-2": {"node-3", "node-4"},
				"region-1:\x00:zone-3": {"node-5"},
				"region-2:\x00:zone-2": {"node-6"},
			},
		},
		{
			name:          "remove a few nodes including one from a zone with multiple nodes",
			existingNodes: allNodes[:7],
			nodesToRemove: allNodes[1:4],
			expectedTree: map[string][]string{
				"":                     {"node-0"},
				"region-1:\x00:zone-2": {"node-4"},
				"region-1:\x00:zone-3": {"node-5"},
				"region-2:\x00:zone-2": {"node-6"},
			},
		},
		{
			name:          "remove all nodes",
			existingNodes: allNodes[:7],
			nodesToRemove: allNodes[:7],
			expectedTree:  map[string][]string{},
		},
		{
			name:          "remove non-existing node",
			existingNodes: nil,
			nodesToRemove: allNodes[:5],
			expectedTree:  map[string][]string{},
			expectError:   true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)
			nt := newNodeTree(logger, test.existingNodes)
			for _, n := range test.nodesToRemove {
				err := nt.removeNode(logger, n)
				if test.expectError == (err == nil) {
					t.Errorf("unexpected returned error value: %v", err)
				}
			}
			verifyNodeTree(t, nt, test.expectedTree)
		})
	}
}

// TestNodeTree_UpdateNode 验证 updateNode 在节点标签变化后将其迁移到新 zone，
// 并覆盖更新不存在节点（等同新增）的情形。
func TestNodeTree_UpdateNode(t *testing.T) {
	tests := []struct {
		name          string
		existingNodes []*v1.Node
		nodeToUpdate  *v1.Node
		expectedTree  map[string][]string
	}{
		{
			name:          "update a node without label",
			existingNodes: allNodes[:7],
			nodeToUpdate: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-0",
					Labels: map[string]string{
						v1.LabelTopologyRegion: "region-1",
						v1.LabelTopologyZone:   "zone-2",
					},
				},
			},
			expectedTree: map[string][]string{
				"region-1:\x00:":       {"node-1"},
				":\x00:zone-2":         {"node-2"},
				"region-1:\x00:zone-2": {"node-3", "node-4", "node-0"},
				"region-1:\x00:zone-3": {"node-5"},
				"region-2:\x00:zone-2": {"node-6"},
			},
		},
		{
			name:          "update the only existing node",
			existingNodes: allNodes[:1],
			nodeToUpdate: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-0",
					Labels: map[string]string{
						v1.LabelTopologyRegion: "region-1",
						v1.LabelTopologyZone:   "zone-2",
					},
				},
			},
			expectedTree: map[string][]string{
				"region-1:\x00:zone-2": {"node-0"},
			},
		},
		{
			name:          "update non-existing node",
			existingNodes: allNodes[:1],
			nodeToUpdate: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{
					Name: "node-new",
					Labels: map[string]string{
						v1.LabelTopologyRegion: "region-1",
						v1.LabelTopologyZone:   "zone-2",
					},
				},
			},
			expectedTree: map[string][]string{
				"":                     {"node-0"},
				"region-1:\x00:zone-2": {"node-new"},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)
			nt := newNodeTree(logger, test.existingNodes)
			var oldNode *v1.Node
			for _, n := range allNodes {
				if n.Name == test.nodeToUpdate.Name {
					oldNode = n
					break
				}
			}
			if oldNode == nil {
				oldNode = &v1.Node{ObjectMeta: metav1.ObjectMeta{Name: "nonexisting-node"}}
			}
			nt.updateNode(logger, oldNode, test.nodeToUpdate)
			verifyNodeTree(t, nt, test.expectedTree)
		})
	}
}

// TestNodeTree_List 验证 list 返回所有节点，且顺序由随机数源决定的轮转遍历给出。
func TestNodeTree_List(t *testing.T) {
	tests := []struct {
		name           string
		nodesToAdd     []*v1.Node
		expectedOutput []string
	}{
		{
			name:           "empty tree",
			nodesToAdd:     nil,
			expectedOutput: nil,
		},
		{
			name:           "one node",
			nodesToAdd:     allNodes[:1],
			expectedOutput: []string{"node-0"},
		},
		{
			name:           "four nodes",
			nodesToAdd:     allNodes[:4],
			expectedOutput: []string{"node-0", "node-1", "node-2", "node-3"},
		},
		{
			name:           "all nodes",
			nodesToAdd:     allNodes[:9],
			expectedOutput: []string{"node-0", "node-1", "node-2", "node-3", "node-5", "node-6", "node-4", "node-7", "node-8"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)
			nt := newNodeTree(logger, test.nodesToAdd)

			output, err := nt.list()
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(test.expectedOutput, output); diff != "" {
				t.Errorf("Unexpected output (-want, +got):\n%s", diff)
			}
		})
	}
}

// TestNodeTree_List_Exhausted 验证当记录的节点数超出实际数量时，
// list 能检测到 zone 耗尽并报错，避免死循环。
func TestNodeTree_List_Exhausted(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	nt := newNodeTree(logger, allNodes[:9])
	// 人为制造计数不一致，触发耗尽检测。
	nt.numNodes++
	_, err := nt.list()
	if err == nil {
		t.Fatal("Expected an error from zone exhaustion")
	}
}

// TestNodeTreeMultiOperations 验证增删混合操作序列后的最终节点集合，
// 包括 zone 耗尽后继续添加以及删除整个 zone 再重建。
func TestNodeTreeMultiOperations(t *testing.T) {
	tests := []struct {
		name           string
		nodesToAdd     []*v1.Node
		nodesToRemove  []*v1.Node
		operations     []string
		expectedOutput []string
	}{
		{
			name:           "add and remove all nodes",
			nodesToAdd:     allNodes[2:9],
			nodesToRemove:  allNodes[2:9],
			operations:     []string{"add", "add", "add", "remove", "remove", "remove"},
			expectedOutput: nil,
		},
		{
			name:           "add and remove some nodes",
			nodesToAdd:     allNodes[2:9],
			nodesToRemove:  allNodes[2:9],
			operations:     []string{"add", "add", "add", "remove"},
			expectedOutput: []string{"node-3", "node-4"},
		},
		{
			name:           "remove three nodes",
			nodesToAdd:     allNodes[2:9],
			nodesToRemove:  allNodes[2:9],
			operations:     []string{"add", "add", "add", "remove", "remove", "remove", "add"},
			expectedOutput: []string{"node-5"},
		},
		{
			name:           "add more nodes to an exhausted zone",
			nodesToAdd:     append(allNodes[4:9:9], allNodes[3]),
			nodesToRemove:  nil,
			operations:     []string{"add", "add", "add", "add", "add", "add"},
			expectedOutput: []string{"node-4", "node-5", "node-6", "node-3", "node-7", "node-8"},
		},
		{
			name:           "remove zone and add new",
			nodesToAdd:     append(allNodes[3:5:5], allNodes[6:8]...),
			nodesToRemove:  allNodes[3:5],
			operations:     []string{"add", "add", "remove", "add", "add", "remove"},
			expectedOutput: []string{"node-6", "node-7"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)
			nt := newNodeTree(logger, nil)
			addIndex := 0
			removeIndex := 0
			for _, op := range test.operations {
				switch op {
				case "add":
					if addIndex >= len(test.nodesToAdd) {
						t.Error("more add operations than nodesToAdd")
					} else {
						nt.addNode(logger, test.nodesToAdd[addIndex])
						addIndex++
					}
				case "remove":
					if removeIndex >= len(test.nodesToRemove) {
						t.Error("more remove operations than nodesToRemove")
					} else {
						nt.removeNode(logger, test.nodesToRemove[removeIndex])
						removeIndex++
					}
				default:
					t.Errorf("unknown operation: %v", op)
				}
			}
			output, err := nt.list()
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(test.expectedOutput, output); diff != "" {
				t.Errorf("Unexpected output (-want, +got):\n%s", diff)
			}
		})
	}
}
