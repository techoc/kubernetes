/*
Copyright 2017 The Kubernetes Authors.

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

// TODO: 这个文件或许可以移到一个公共位置，供 e2e 测试和集成测试共用。

package framework

import (
	"context"
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	nodectlr "k8s.io/kubernetes/pkg/controller/nodelifecycle"
)

// CreateNamespaceOrDie 创建一个命名空间。
func CreateNamespaceOrDie(c clientset.Interface, baseName string, t testing.TB) *v1.Namespace {
	ns := &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: baseName}}
	result, err := c.CoreV1().Namespaces().Create(context.TODO(), ns, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create namespace: %v", err)
	}
	return result
}

// DeleteNamespaceOrDie 删除一个命名空间。
func DeleteNamespaceOrDie(c clientset.Interface, ns *v1.Namespace, t testing.TB) {
	err := c.CoreV1().Namespaces().Delete(context.TODO(), ns.Name, metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("Failed to delete namespace: %v", err)
	}
}

// Filter 就地对 NodeList 中的节点进行过滤，移除不满足
// 给定条件的节点。
func Filter(nodeList *v1.NodeList, fn func(node v1.Node) bool) {
	var l []v1.Node

	for _, node := range nodeList.Items {
		if fn(node) {
			l = append(l, node)
		}
	}
	nodeList.Items = l
}

// IsNodeSchedulable 在以下情况下返回 true：
// 1) 未设置 "unschedulable" 字段
// 2) IsNodeReady 也返回 true
func IsNodeSchedulable(node *v1.Node) bool {
	if node == nil {
		return false
	}
	return !node.Spec.Unschedulable && IsNodeReady(node)
}

// IsNodeReady 在以下情况下返回 true：
// 1) Ready condition 被设置为 true
// 2) NetworkUnavailable condition 未被设置为 true
func IsNodeReady(node *v1.Node) bool {
	nodeReady := IsConditionSetAsExpected(node, v1.NodeReady, true)
	networkReady := isConditionUnset(node, v1.NodeNetworkUnavailable) ||
		IsConditionSetAsExpectedSilent(node, v1.NodeNetworkUnavailable, false)
	return nodeReady && networkReady
}

// IsConditionSetAsExpected 如果节点的 condition 与 conditionType 匹配，则返回 wantTrue 值，否则返回 wantTrue 的相反值，并记录详细日志。
func IsConditionSetAsExpected(node *v1.Node, conditionType v1.NodeConditionType, wantTrue bool) bool {
	return isNodeConditionSetAsExpected(node, conditionType, wantTrue, false)
}

// IsConditionSetAsExpectedSilent 如果节点的 condition 与 conditionType 匹配，则返回 wantTrue 值，否则返回 wantTrue 的相反值。
func IsConditionSetAsExpectedSilent(node *v1.Node, conditionType v1.NodeConditionType, wantTrue bool) bool {
	return isNodeConditionSetAsExpected(node, conditionType, wantTrue, true)
}

// isConditionUnset 如果给定节点的 conditions 中没有与给定 conditionType 匹配的项，则返回 true，否则返回 false。
func isConditionUnset(node *v1.Node, conditionType v1.NodeConditionType) bool {
	for _, cond := range node.Status.Conditions {
		if cond.Type == conditionType {
			return false
		}
	}
	return true
}

// isNodeConditionSetAsExpected 检查节点的某个 condition，当期望值与该 condition 的值相同时返回 'true'，适用于轮询等待节点上的 condition 达成。
func isNodeConditionSetAsExpected(node *v1.Node, conditionType v1.NodeConditionType, wantTrue, silent bool) bool {
	// 检查节点的就绪 condition（记录所有日志）。
	for _, cond := range node.Status.Conditions {
		// 确保 condition 的类型和状态与期望一致。
		if cond.Type == conditionType {
			// 对于 NodeReady condition，还需要检查 Taints
			if cond.Type == v1.NodeReady {
				hasNodeControllerTaints := false
				// 对于 NodeReady，还需要检查 Taints 是否已移除
				taints := node.Spec.Taints
				for _, taint := range taints {
					if taint.MatchTaint(nodectlr.UnreachableTaintTemplate) || taint.MatchTaint(nodectlr.NotReadyTaintTemplate) {
						hasNodeControllerTaints = true
						break
					}
				}
				if wantTrue {
					if (cond.Status == v1.ConditionTrue) && !hasNodeControllerTaints {
						return true
					}
					msg := ""
					if !hasNodeControllerTaints {
						msg = fmt.Sprintf("Condition %s of node %s is %v instead of %t. Reason: %v, message: %v",
							conditionType, node.Name, cond.Status == v1.ConditionTrue, wantTrue, cond.Reason, cond.Message)
					} else {
						msg = fmt.Sprintf("Condition %s of node %s is %v, but Node is tainted by NodeController with %v. Failure",
							conditionType, node.Name, cond.Status == v1.ConditionTrue, taints)
					}
					if !silent {
						klog.Info(msg)
					}
					return false
				}
				// TODO: 在默认启用 NC 的 notReady/unreachable taints 后，检查 Node 是否带有污点
				if cond.Status != v1.ConditionTrue {
					return true
				}
				if !silent {
					klog.Infof("Condition %s of node %s is %v instead of %t. Reason: %v, message: %v",
						conditionType, node.Name, cond.Status == v1.ConditionTrue, wantTrue, cond.Reason, cond.Message)
				}
				return false
			}
			if (wantTrue && (cond.Status == v1.ConditionTrue)) || (!wantTrue && (cond.Status != v1.ConditionTrue)) {
				return true
			}
			if !silent {
				klog.Infof("Condition %s of node %s is %v instead of %t. Reason: %v, message: %v",
					conditionType, node.Name, cond.Status == v1.ConditionTrue, wantTrue, cond.Reason, cond.Message)
			}
			return false
		}

	}
	if !silent {
		klog.Infof("Couldn't find condition %v on node %v", conditionType, node.Name)
	}
	return false
}
