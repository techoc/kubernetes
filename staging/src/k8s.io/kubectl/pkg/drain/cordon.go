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

package drain

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/client-go/kubernetes"
)

// CordonHelper 封装了 cordon/uncordon 节点的功能
type CordonHelper struct {
	node    *corev1.Node
	desired bool
}

// NewCordonHelper 返回一个新的 CordonHelper
func NewCordonHelper(node *corev1.Node) *CordonHelper {
	return &CordonHelper{
		node: node,
	}
}

// NewCordonHelperFromRuntimeObject 返回一个新的 CordonHelper；若给定对象不是
// node 或无法编码为 JSON，则返回错误
func NewCordonHelperFromRuntimeObject(nodeObject runtime.Object, scheme *runtime.Scheme, gvk schema.GroupVersionKind) (*CordonHelper, error) {
	nodeObject, err := scheme.ConvertToVersion(nodeObject, gvk.GroupVersion())
	if err != nil {
		return nil, err
	}

	node, ok := nodeObject.(*corev1.Node)
	if !ok {
		return nil, fmt.Errorf("unexpected type %T", nodeObject)
	}

	return NewCordonHelper(node), nil
}

// UpdateIfRequired 在 c.node.Spec.Unschedulable 尚未设置时返回 true，
// 无需变更时返回 false
func (c *CordonHelper) UpdateIfRequired(desired bool) bool {
	c.desired = desired

	return c.node.Spec.Unschedulable != c.desired
}

// PatchOrReplace 使用给定的 clientset 更新 node 状态，方式为 patch
// 或 update 给定的 node 对象。若对象无法编码为 JSON，或
// patch/update 调用失败，则可能返回错误；创建 patch 时
// 失败还会返回第二个错误
func (c *CordonHelper) PatchOrReplace(clientset kubernetes.Interface, serverDryRun bool) (error, error) {
	return c.PatchOrReplaceWithContext(context.TODO(), clientset, serverDryRun)
}

// PatchOrReplaceWithContext 提供在更新 node 状态时
// 传入自定义 context 的选项
func (c *CordonHelper) PatchOrReplaceWithContext(clientCtx context.Context, clientset kubernetes.Interface, serverDryRun bool) (error, error) {
	client := clientset.CoreV1().Nodes()

	oldData, err := json.Marshal(c.node)
	if err != nil {
		return err, nil
	}

	desiredNode := c.node.DeepCopy()
	desiredNode.Spec.Unschedulable = c.desired

	newData, err := json.Marshal(desiredNode)
	if err != nil {
		return err, nil
	}

	patchBytes, patchErr := strategicpatch.CreateTwoWayMergePatch(oldData, newData, c.node)
	if patchErr == nil {
		patchOptions := metav1.PatchOptions{}
		if serverDryRun {
			patchOptions.DryRun = []string{metav1.DryRunAll}
		}
		var updatedNode *corev1.Node
		updatedNode, err = client.Patch(clientCtx, c.node.Name, types.StrategicMergePatchType, patchBytes, patchOptions)
		if err == nil {
			c.node.Spec.Unschedulable = updatedNode.Spec.Unschedulable
		}
	} else {
		updateOptions := metav1.UpdateOptions{}
		if serverDryRun {
			updateOptions.DryRun = []string{metav1.DryRunAll}
		}
		var updatedNode *corev1.Node
		updatedNode, err = client.Update(clientCtx, desiredNode, updateOptions)
		if err == nil {
			c.node.Spec.Unschedulable = updatedNode.Spec.Unschedulable
		}
	}
	return err, patchErr
}
