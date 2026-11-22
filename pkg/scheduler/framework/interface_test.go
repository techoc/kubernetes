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

package framework

import (
	"fmt"
	"testing"

	"k8s.io/apimachinery/pkg/util/sets"
	fwk "k8s.io/kube-scheduler/framework"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
)

// nodeInfoLister 是 NodeInfoLister 的测试替身，直接把切片当作节点列表返回。
type nodeInfoLister []fwk.NodeInfo

// Get 按节点名查找；未找到时返回错误，符合 NodeInfoLister 的接口约定。
func (nodes nodeInfoLister) Get(nodeName string) (fwk.NodeInfo, error) {
	for _, node := range nodes {
		if node != nil && node.Node().Name == nodeName {
			return node, nil
		}
	}
	return nil, fmt.Errorf("unable to find node: %s", nodeName)
}

// List 返回全部节点。
func (nodes nodeInfoLister) List() ([]fwk.NodeInfo, error) {
	return nodes, nil
}

// HavePodsWithAffinityList 测试替身：不区分节点属性，返回全部节点。
func (nodes nodeInfoLister) HavePodsWithAffinityList() ([]fwk.NodeInfo, error) {
	return nodes, nil
}

// HavePodsWithRequiredAntiAffinityList 测试替身：返回全部节点。
func (nodes nodeInfoLister) HavePodsWithRequiredAntiAffinityList() ([]fwk.NodeInfo, error) {
	return nodes, nil
}

// HavePodsWithRequiredNonHostScopedAntiAffinityList 测试替身：返回全部节点。
func (nodes nodeInfoLister) HavePodsWithRequiredNonHostScopedAntiAffinityList() ([]fwk.NodeInfo, error) {
	return nodes, nil
}

// TestNodesForStatusCode 验证 NodesForStatusCode 能按给定状态码筛出"值得重试"的节点，
// 包括显式状态、默认状态以及缺失节点被视为默认状态的情况。
func TestNodesForStatusCode(t *testing.T) {
	// 准备 4 个节点名，供各用例构造 NodeInfo。
	nodeNames := []string{"node1", "node2", "node3", "node4"}
	tests := []struct {
		name          string
		nodesStatuses *NodeToStatus
		code          fwk.Code
		expected      sets.Set[string] // 期望重试的节点名集合。
	}{
		{
			name: "No node should be attempted",
			nodesStatuses: NewNodeToStatus(map[string]*fwk.Status{
				"node1": fwk.NewStatus(fwk.UnschedulableAndUnresolvable),
				"node2": fwk.NewStatus(fwk.UnschedulableAndUnresolvable),
				"node3": fwk.NewStatus(fwk.UnschedulableAndUnresolvable),
				"node4": fwk.NewStatus(fwk.UnschedulableAndUnresolvable),
			}, fwk.NewStatus(fwk.UnschedulableAndUnresolvable)),
			code:     fwk.Unschedulable,
			expected: sets.New[string](),
		},
		{
			name: "All nodes should be attempted",
			nodesStatuses: NewNodeToStatus(map[string]*fwk.Status{
				"node1": fwk.NewStatus(fwk.UnschedulableAndUnresolvable),
				"node2": fwk.NewStatus(fwk.UnschedulableAndUnresolvable),
				"node3": fwk.NewStatus(fwk.UnschedulableAndUnresolvable),
				"node4": fwk.NewStatus(fwk.UnschedulableAndUnresolvable),
			}, fwk.NewStatus(fwk.UnschedulableAndUnresolvable)),
			code:     fwk.UnschedulableAndUnresolvable,
			expected: sets.New[string]("node1", "node2", "node3", "node4"),
		},
		{
			name:          "No node should be attempted, as all are implicitly not matching the code",
			nodesStatuses: NewDefaultNodeToStatus(),
			code:          fwk.Unschedulable,
			expected:      sets.New[string](),
		},
		{
			name:          "All nodes should be attempted, as all are implicitly matching the code",
			nodesStatuses: NewDefaultNodeToStatus(),
			code:          fwk.UnschedulableAndUnresolvable,
			expected:      sets.New[string]("node1", "node2", "node3", "node4"),
		},
		{
			name: "UnschedulableAndUnresolvable status should be skipped but Unschedulable should be tried",
			nodesStatuses: NewNodeToStatus(map[string]*fwk.Status{
				"node1": fwk.NewStatus(fwk.Unschedulable),
				"node2": fwk.NewStatus(fwk.UnschedulableAndUnresolvable),
				"node3": fwk.NewStatus(fwk.Unschedulable),
				// node4 未在 map 中列出，按默认状态视为 UnschedulableAndUnresolvable。
			}, fwk.NewStatus(fwk.UnschedulableAndUnresolvable)),
			code:     fwk.Unschedulable,
			expected: sets.New("node1", "node3"),
		},
		{
			name: "Unschedulable status should be skipped but UnschedulableAndUnresolvable should be tried",
			nodesStatuses: NewNodeToStatus(map[string]*fwk.Status{
				"node1": fwk.NewStatus(fwk.Unschedulable),
				"node2": fwk.NewStatus(fwk.UnschedulableAndUnresolvable),
				"node3": fwk.NewStatus(fwk.Unschedulable),
				// node4 未在 map 中列出，按默认状态视为 UnschedulableAndUnresolvable。
			}, fwk.NewStatus(fwk.UnschedulableAndUnresolvable)),
			code:     fwk.UnschedulableAndUnresolvable,
			expected: sets.New("node2", "node4"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var nodeInfos nodeInfoLister
			for _, name := range nodeNames {
				ni := NewNodeInfo()
				ni.SetNode(st.MakeNode().Name(name).Obj())
				nodeInfos = append(nodeInfos, ni)
			}
			nodes, err := tt.nodesStatuses.NodesForStatusCode(nodeInfos, tt.code)
			if err != nil {
				t.Fatalf("Failed to get nodes for status code: %s", err)
			}
			if len(tt.expected) != len(nodes) {
				t.Errorf("Number of nodes is not the same as expected. expected: %d, got: %d. Nodes: %v", len(tt.expected), len(nodes), nodes)
			}
			for _, node := range nodes {
				name := node.Node().Name
				if _, found := tt.expected[name]; !found {
					t.Errorf("Node %v is not expected", name)
				}
			}
		})
	}
}
