/*
Copyright 2021 The Kubernetes Authors.

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

package util

import (
	"context"
	"encoding/json"
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
)

// PatchNodeStatus 通过 patch 更新节点状态，并返回更新后的节点与实际提交的 patch 字节。
// 走 status 子资源而不是整体更新，是为了隔离 spec 的改动；
// 同时返回生成的 patch 字节，方便调用方记录或复用这次变更内容。
func PatchNodeStatus(c v1core.CoreV1Interface, nodeName types.NodeName, oldNode *v1.Node, newNode *v1.Node) (*v1.Node, []byte, error) {
	// 先在本地算出新旧状态的差异补丁再提交，保证失败时不会留下半成品写入。
	patchBytes, err := preparePatchBytesforNodeStatus(nodeName, oldNode, newNode)
	if err != nil {
		return nil, nil, err
	}

	updatedNode, err := c.Nodes().Patch(context.TODO(), string(nodeName), types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{}, "status")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to patch status %q for node %q: %v", patchBytes, nodeName, err)
	}
	return updatedNode, patchBytes, nil
}

// preparePatchBytesforNodeStatus 先把新旧对象序列化再算两方合并补丁，
// 而不是直接逐字段比较：strategic merge patch 依赖结构标签来处理列表和 map，
// 让 apiserver 按同一套合并规则应用变更。这里刻意不重置 ObjectMeta，
// 因为部分组件确实借这个补丁更新节点注解。
func preparePatchBytesforNodeStatus(nodeName types.NodeName, oldNode *v1.Node, newNode *v1.Node) ([]byte, error) {
	oldData, err := json.Marshal(oldNode)
	if err != nil {
		return nil, fmt.Errorf("failed to Marshal oldData for node %q: %v", nodeName, err)
	}

	// NodeStatus.Addresses 上的 patchStrategy=merge 标注是错误的，
	// 一旦地址发生变化，strategicpatch.CreateTwoWayMergePatch 会生成错误的补丁。
	// 所以地址变更要绕开自动补丁：先从 diff 对象里抹掉地址，让补丁生成器别碰它，
	// 最后再用 $patch: replace 整体替换。旧列表为空时跳过，是因为那属于首次写入场景，
	// 自动生成的补丁语义已经正确，多此一举反而可能引入意外字段。
	manuallyPatchAddresses := (len(oldNode.Status.Addresses) > 0) && !equality.Semantic.DeepEqual(oldNode.Status.Addresses, newNode.Status.Addresses)

	// 把 diff 对象的 spec 重置为旧值，确保生成的补丁只涉及 Status 或 ObjectMeta。
	// 这里刻意不重置 ObjectMeta，因为：
	// 1. 这与 Nodes().UpdateStatus() 的行为一致。
	// 2. 确实有组件借这个接口更新节点注解。
	diffNode := newNode.DeepCopy()
	diffNode.Spec = oldNode.Spec
	if manuallyPatchAddresses {
		diffNode.Status.Addresses = oldNode.Status.Addresses
	}
	newData, err := json.Marshal(diffNode)
	if err != nil {
		return nil, fmt.Errorf("failed to Marshal newData for node %q: %v", nodeName, err)
	}

	patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, v1.Node{})
	if err != nil {
		return nil, fmt.Errorf("failed to CreateTwoWayMergePatch for node %q: %v", nodeName, err)
	}
	// 把地址列表以 replace 语义塞回补丁，覆盖掉前面刻意保持的地址旧值；
	// 这样 apiserver 会用新列表整体替换，而不是按错误的 merge 策略逐项合并。
	if manuallyPatchAddresses {
		patchBytes, err = fixupPatchForNodeStatusAddresses(patchBytes, newNode.Status.Addresses)
		if err != nil {
			return nil, fmt.Errorf("failed to fix up NodeAddresses in patch for node %q: %v", nodeName, err)
		}
	}

	return patchBytes, nil
}

// fixupPatchForNodeStatusAddresses 往已有补丁中追加一段针对 Status.Addresses 的 replace 策略补丁。
// $patch: replace 作为数组最后一个元素，是 strategic merge patch 的约定写法，
// 表示用整个新数组替换旧值，从而绕开地址字段标注错误带来的按项合并行为。
func fixupPatchForNodeStatusAddresses(patchBytes []byte, addresses []v1.NodeAddress) ([]byte, error) {
	// 例如给定 patchBytes='{"status": {"conditions": [ ... ], "phase": ...}}' 与
	// addresses=[{"type": "InternalIP", "address": "10.0.0.1"}]，需要生成：
	//
	//   {
	//     "status": {
	//       "conditions": [ ... ],
	//       "phase": ...,
	//       "addresses": [
	//         {
	//           "type": "InternalIP",
	//           "address": "10.0.0.1"
	//         },
	//         {
	//           "$patch": "replace"
	//         }
	//       ]
	//     }
	//   }

	// 先把补丁反序列化成 map，才能往里插入地址字段。
	var patchMap map[string]interface{}
	if err := json.Unmarshal(patchBytes, &patchMap); err != nil {
		return nil, err
	}

	// 地址列表先序列化再反序列化成 []interface{}，便于追加 $patch 标记。
	addrBytes, err := json.Marshal(addresses)
	if err != nil {
		return nil, err
	}
	var addrArray []interface{}
	if err := json.Unmarshal(addrBytes, &addrArray); err != nil {
		return nil, err
	}
	// 在数组末尾追加 replace 指令，指示 apiserver 整体替换地址列表。
	addrArray = append(addrArray, map[string]interface{}{"$patch": "replace"})

	// status 字段可能不存在，需要时新建一个空 map。
	status := patchMap["status"]
	if status == nil {
		status = map[string]interface{}{}
		patchMap["status"] = status
	}
	statusMap, ok := status.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected data in patch")
	}
	statusMap["addresses"] = addrArray

	// 最后重新序列化，得到包含地址 replace 指令的完整补丁。
	return json.Marshal(patchMap)
}
