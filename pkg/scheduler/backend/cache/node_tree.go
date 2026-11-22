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
	"errors"
	"fmt"

	v1 "k8s.io/api/core/v1"
	utilnode "k8s.io/component-helpers/node/topology"
	"k8s.io/klog/v2"
)

// nodeTree 是树形数据结构，按 zone 保存节点名称。zone 名是 "NodeTree.tree" 的键，
// "NodeTree.tree" 的值是节点名称数组。
// nodeTree 不是线程安全的，任何并发读写都必须由调用方同步。
// 它只被 schedulerCache 使用，也应继续保持如此。
type nodeTree struct {
	tree     map[string][]string // 从 zone（region-zone）到该 zone 内节点数组的映射。
	zones    []string            // 树中所有 zone（键）的列表。
	numNodes int
}

// newNodeTree 根据节点列表创建 NodeTree。
func newNodeTree(logger klog.Logger, nodes []*v1.Node) *nodeTree {
	nt := &nodeTree{
		tree: make(map[string][]string, len(nodes)),
	}
	for _, n := range nodes {
		nt.addNode(logger, n)
	}
	return nt
}

// addNode 把节点及其所属 zone 加入树。若 zone 已存在，则把节点追加到该 zone 的节点数组中。
func (nt *nodeTree) addNode(logger klog.Logger, n *v1.Node) {
	zone := utilnode.GetZoneKey(n)
	if na, ok := nt.tree[zone]; ok {
		for _, nodeName := range na {
			if nodeName == n.Name {
				logger.Info("Did not add to the NodeTree because it already exists", "node", klog.KObj(n))
				return
			}
		}
		nt.tree[zone] = append(na, n.Name)
	} else {
		nt.zones = append(nt.zones, zone)
		nt.tree[zone] = []string{n.Name}
	}
	logger.V(2).Info("Added node to NodeTree", "node", klog.KObj(n), "zone", zone)
	nt.numNodes++
}

// removeNode 从 NodeTree 中删除节点。
func (nt *nodeTree) removeNode(logger klog.Logger, n *v1.Node) error {
	zone := utilnode.GetZoneKey(n)
	if na, ok := nt.tree[zone]; ok {
		for i, nodeName := range na {
			if nodeName == n.Name {
				nt.tree[zone] = append(na[:i], na[i+1:]...)
				// zone 已空，必须同步清理，否则 list 会碰到空列表。
				if len(nt.tree[zone]) == 0 {
					nt.removeZone(zone)
				}
				logger.V(2).Info("Removed node from NodeTree", "node", klog.KObj(n), "zone", zone)
				nt.numNodes--
				return nil
			}
		}
	}
	logger.Error(nil, "Did not remove Node in NodeTree because it was not found", "node", klog.KObj(n), "zone", zone)
	return fmt.Errorf("node %q in group %q was not found", n.Name, zone)
}

// removeZone 从树中删除 zone。
// 调用本函数时必须持有写锁。
func (nt *nodeTree) removeZone(zone string) {
	delete(nt.tree, zone)
	for i, z := range nt.zones {
		if z == zone {
			nt.zones = append(nt.zones[:i], nt.zones[i+1:]...)
			return
		}
	}
}

// updateNode 更新 NodeTree 中的节点。
func (nt *nodeTree) updateNode(logger klog.Logger, old, new *v1.Node) {
	var oldZone string
	if old != nil {
		oldZone = utilnode.GetZoneKey(old)
	}
	newZone := utilnode.GetZoneKey(new)
	// zone 未变则无需操作；节点名在更新中不会改变。
	if oldZone == newZone {
		return
	}
	nt.removeNode(logger, old) // 不检查错误，旧节点是否存在都无妨。
	nt.addNode(logger, new)
}

// list 返回节点名称列表。NodeTree 遍历各个 zone，并在每个 zone 内以轮询方式遍历节点，
// 避免相邻节点总是集中在同一个 zone。
func (nt *nodeTree) list() ([]string, error) {
	if len(nt.zones) == 0 {
		return nil, nil
	}
	nodesList := make([]string, 0, nt.numNodes)
	numExhaustedZones := 0
	nodeIndex := 0
	for len(nodesList) < nt.numNodes {
		if numExhaustedZones >= len(nt.zones) { // 所有 zone 都遍历完了。
			return nodesList, errors.New("all zones exhausted before reaching count of nodes expected")
		}
		for zoneIndex := 0; zoneIndex < len(nt.zones); zoneIndex++ {
			na := nt.tree[nt.zones[zoneIndex]]
			if nodeIndex >= len(na) { // 该 zone 已遍历完，跳过。
				if nodeIndex == len(na) { // 第一次遍历完时计数一次。
					numExhaustedZones++
				}
				continue
			}
			nodesList = append(nodesList, na[nodeIndex])
		}
		nodeIndex++
	}
	return nodesList, nil
}
