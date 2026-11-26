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

/*

NOTE: the contents of this file has been copied from k8s.io/kubernetes/pkg/controller
and k8s.io/kubernetes/pkg/util/taints. The reason for duplicating this code is to remove
dependencies to k8s.io/kubernetes in all the cloud providers. Once k8s.io/kubernetes/pkg/util/taints
is moved to an external repository, this file should be removed and replaced with that one.
*/

package helpers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	clientretry "k8s.io/client-go/util/retry"
)

// updateTaintBackoff 控制 taint 更新的冲突重试；与标签更新保持同一节奏。
var updateTaintBackoff = wait.Backoff{
	Steps:    5,
	Duration: 100 * time.Millisecond,
	Jitter:   1.0,
}

// AddOrUpdateTaintOnNode 为节点添加 taint。若 taint 确实写入了节点则发起 API 调用更新节点，
// 否则不产生任何 API 调用，出错时返回错误。
// 返回值只反映 API 调用是否成功；taint 已存在且内容相同时直接返回 nil，避免无意义写入。
func AddOrUpdateTaintOnNode(c clientset.Interface, nodeName string, taints ...*v1.Taint) error {
	if len(taints) == 0 {
		return nil
	}
	firstTry := true
	return clientretry.RetryOnConflict(updateTaintBackoff, func() error {
		var err error
		var oldNode *v1.Node
		// 先尝试从 API server 缓存读取节点（开销更小）；失败时再从 etcd 强一致读取，确保拿到最新数据。
		// 重试时必须强一致读取，否则可能基于旧版本反复生成同样的冲突补丁。
		if firstTry {
			oldNode, err = c.CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{ResourceVersion: "0"})
			firstTry = false
		} else {
			oldNode, err = c.CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
		}
		if err != nil {
			return err
		}

		var newNode *v1.Node
		oldNodeCopy := oldNode
		updated := false
		// 逐个应用 taint，任一发生变化即需要提交一次补丁。
		for _, taint := range taints {
			curNewNode, ok := addOrUpdateTaint(oldNodeCopy, taint)
			updated = updated || ok
			newNode = curNewNode
			oldNodeCopy = curNewNode
		}
		if !updated {
			return nil
		}
		return PatchNodeTaints(c, nodeName, oldNode, newNode)
	})
}

// PatchNodeTaints 提交节点 taints 的补丁。
// 仅对 .spec.taints 做 strategic merge；由于该字段没有 patchMergeKey，
// 这里特意去掉旧对象的 resourceVersion 并只替换 taints，避免错误合并。
func PatchNodeTaints(c clientset.Interface, nodeName string, oldNode *v1.Node, newNode *v1.Node) error {
	// 去掉基准对象的 resourceVersion，让 Patch 请求带上 RV 以检测 .spec.taints 冲突。
	// .spec.taints 没有定义 patchMergeKey 与 patchStrategy，出于兼容也无法补上；
	// 换用其他 patch 策略虽能新增 taint，却解决不了删除 taint 的问题。
	oldNodeNoRV := oldNode.DeepCopy()
	oldNodeNoRV.ResourceVersion = ""
	oldDataNoRV, err := json.Marshal(&oldNodeNoRV)
	if err != nil {
		return fmt.Errorf("failed to marshal old node %#v for node %q: %v", oldNodeNoRV, nodeName, err)
	}

	// 新对象直接以“旧对象 + 新 taints”构造，确保 diff 只包含 taints 变化。
	newTaints := newNode.Spec.Taints
	newNodeClone := oldNode.DeepCopy()
	newNodeClone.Spec.Taints = newTaints
	newData, err := json.Marshal(newNodeClone)
	if err != nil {
		return fmt.Errorf("failed to marshal new node %#v for node %q: %v", newNodeClone, nodeName, err)
	}

	patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldDataNoRV, newData, v1.Node{})
	if err != nil {
		return fmt.Errorf("failed to create patch for node %q: %v", nodeName, err)
	}

	_, err = c.CoreV1().Nodes().Patch(context.TODO(), nodeName, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
	return err
}

// addOrUpdateTaint 尝试把污点加入列表，返回更新后的 Node 副本；第二个返回值表示是否发生了修改。
// 匹配同名同效果的 taint 视为同一项：值相同则不变，值不同则原地替换并保持顺序。
func addOrUpdateTaint(node *v1.Node, taint *v1.Taint) (*v1.Node, bool) {
	newNode := node.DeepCopy()
	nodeTaints := newNode.Spec.Taints

	var newTaints []v1.Taint
	updated := false
	for i := range nodeTaints {
		if taint.MatchTaint(&nodeTaints[i]) {
			// 内容完全一致，返回 false 以便上层跳过 API 调用。
			if equality.Semantic.DeepEqual(*taint, nodeTaints[i]) {
				return newNode, false
			}
			newTaints = append(newTaints, *taint)
			updated = true
			continue
		}

		newTaints = append(newTaints, nodeTaints[i])
	}

	if !updated {
		newTaints = append(newTaints, *taint)
	}

	newNode.Spec.Taints = newTaints
	return newNode, true
}

// RemoveTaintOffNode 用于清理临时加到节点上的 taint；目标 taint 不存在或已被移除时不会失败。
// 若传入了 node，会先检查是否有事可做——taint 不存在时不发起任何 API 调用。
// 幂等设计：移除不存在的 taint 不是错误，调用方可放心重试。
func RemoveTaintOffNode(c clientset.Interface, nodeName string, node *v1.Node, taints ...*v1.Taint) error {
	if len(taints) == 0 {
		return nil
	}
	// 短路逻辑以减少 API 调用次数：调用方通常持有 informer 中的节点快照，
	// 可据此避免一次无意义的读-改-写。
	if node != nil {
		match := false
		for _, taint := range taints {
			if taintExists(node.Spec.Taints, taint) {
				match = true
				break
			}
		}
		if !match {
			return nil
		}
	}

	firstTry := true
	return clientretry.RetryOnConflict(updateTaintBackoff, func() error {
		var err error
		var oldNode *v1.Node
		// 先读 API Server 缓存，代价更低；失败再读 etcd 拿最新数据。
		if firstTry {
			oldNode, err = c.CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{ResourceVersion: "0"})
			firstTry = false
		} else {
			oldNode, err = c.CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
		}
		if err != nil {
			return err
		}

		var newNode *v1.Node
		oldNodeCopy := oldNode
		updated := false
		for _, taint := range taints {
			curNewNode, ok := removeTaint(oldNodeCopy, taint)
			updated = updated || ok
			newNode = curNewNode
			oldNodeCopy = curNewNode
		}
		if !updated {
			return nil
		}
		return PatchNodeTaints(c, nodeName, oldNode, newNode)
	})
}

// taintExists 检查给定 taint 是否存在于 taint 列表中，存在返回 true，否则返回 false。
// 只按 key+effect 匹配，不比较 value，与 MatchTaint 的语义保持一致。
func taintExists(taints []v1.Taint, taintToFind *v1.Taint) bool {
	for _, taint := range taints {
		if taint.MatchTaint(taintToFind) {
			return true
		}
	}
	return false
}

// removeTaint 尝试从列表删除污点，返回更新后的 Node 副本；第二个返回值表示是否发生了修改。
// 不存在的 taint 返回 false，使上层跳过补丁提交。
func removeTaint(node *v1.Node, taint *v1.Taint) (*v1.Node, bool) {
	newNode := node.DeepCopy()
	nodeTaints := newNode.Spec.Taints
	if len(nodeTaints) == 0 {
		return newNode, false
	}

	if !taintExists(nodeTaints, taint) {
		return newNode, false
	}

	newTaints, _ := deleteTaint(nodeTaints, taint)
	newNode.Spec.Taints = newTaints
	return newNode, true
}

// deleteTaint 删除所有与给定 taintToDelete 键和效果相同的污点。
func deleteTaint(taints []v1.Taint, taintToDelete *v1.Taint) ([]v1.Taint, bool) {
	newTaints := []v1.Taint{}
	deleted := false
	for i := range taints {
		if taintToDelete.MatchTaint(&taints[i]) {
			deleted = true
			continue
		}
		newTaints = append(newTaints, taints[i])
	}
	return newTaints, deleted
}
