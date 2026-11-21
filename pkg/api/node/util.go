/*
Copyright 2022 The Kubernetes Authors.

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

package node

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/node"
)

// deprecatedNodeLabels 收集了历史上被弃用的节点标签及其替代说明。
// 这里的 key 是“旧标签名”，value 是给用户的迁移建议，
// 供各处警告逻辑统一查询，避免同一套弃用信息散落多处。
var deprecatedNodeLabels = map[string]string{
	`beta.kubernetes.io/arch`:                       `deprecated since v1.14; use "kubernetes.io/arch" instead`,
	`beta.kubernetes.io/os`:                         `deprecated since v1.14; use "kubernetes.io/os" instead`,
	`failure-domain.beta.kubernetes.io/region`:      `deprecated since v1.17; use "topology.kubernetes.io/region" instead`,
	`failure-domain.beta.kubernetes.io/zone`:        `deprecated since v1.17; use "topology.kubernetes.io/zone" instead`,
	`beta.kubernetes.io/instance-type`:              `deprecated since v1.17; use "node.kubernetes.io/instance-type" instead`,
	`app.kubernetes.io/created-by`:                  `deprecated since v1.9`,
	`scheduler.alpha.kubernetes.io/preferAvoidPods`: `deprecated since v1.22; use Taints and Tolerations instead`,
	`node-role.kubernetes.io/master`:                `use "node-role.kubernetes.io/control-plane" instead`,
}

// GetNodeLabelDeprecatedMessage 返回指定标签的弃用说明，
// 第二个返回值表示该标签是否在弃用列表中。
func GetNodeLabelDeprecatedMessage(key string) (string, bool) {
	msg, ok := deprecatedNodeLabels[key]
	return msg, ok
}

// GetWarningsForRuntimeClass 检查 RuntimeClass 的调度节点选择器是否使用了弃用的节点标签。
func GetWarningsForRuntimeClass(rc *node.RuntimeClass) []string {
	var warnings []string

	if rc != nil && rc.Scheduling != nil && rc.Scheduling.NodeSelector != nil {
		// scheduling.nodeSelector 中的 key 可能引用了弃用的节点标签。
		for key := range rc.Scheduling.NodeSelector {
			if msg, deprecated := GetNodeLabelDeprecatedMessage(key); deprecated {
				warnings = append(warnings, fmt.Sprintf("%s: %s", field.NewPath("scheduling", "nodeSelector"), msg))
			}
		}
	}

	return warnings
}

// GetWarningsForNodeSelector 检查节点选择器（LabelSelector）中是否存在弃用的标签。
// matchExpressions 的 key 和 matchLabels 的 label 都会检查。
func GetWarningsForNodeSelector(nodeSelector *metav1.LabelSelector, fieldPath *field.Path) []string {
	if nodeSelector == nil {
		return nil
	}

	var warnings []string
	// matchExpressions 里每个表达式都有独立的 key，需要带上索引定位到具体项。
	for i, expression := range nodeSelector.MatchExpressions {
		if msg, deprecated := GetNodeLabelDeprecatedMessage(expression.Key); deprecated {
			warnings = append(
				warnings,
				fmt.Sprintf(
					"%s: %s is %s",
					fieldPath.Child("matchExpressions").Index(i).Child("key"),
					expression.Key,
					msg,
				),
			)
		}
	}

	// matchLabels 的 key 直接作为字段路径的一段。
	for label := range nodeSelector.MatchLabels {
		if msg, deprecated := GetNodeLabelDeprecatedMessage(label); deprecated {
			warnings = append(warnings, fmt.Sprintf("%s: %s", fieldPath.Child("matchLabels").Child(label), msg))
		}
	}
	return warnings
}

// GetWarningsForNodeSelectorTerm 检查单个 NodeSelectorTerm 中的 matchExpressions。
// checkLabelValue 为 true 时还会校验表达式中的 value 是否是合法标签值，
// 这是为了兼容历史上某些 API 不校验 value 的场景。
func GetWarningsForNodeSelectorTerm(nodeSelectorTerm api.NodeSelectorTerm, checkLabelValue bool, fieldPath *field.Path) []string {
	var warnings []string
	for i, expression := range nodeSelectorTerm.MatchExpressions {
		// 弃用标签的 key 检查。
		if msg, deprecated := GetNodeLabelDeprecatedMessage(expression.Key); deprecated {
			warnings = append(
				warnings,
				fmt.Sprintf(
					"%s: %s is %s",
					fieldPath.Child("matchExpressions").Index(i).Child("key"),
					expression.Key,
					msg,
				),
			)
		}
		if checkLabelValue {
			// 每个 value 可能有多条校验错误，逐条生成警告。
			for index, value := range expression.Values {
				for _, msg := range validation.IsValidLabelValue(value) {
					warnings = append(warnings,
						fmt.Sprintf(
							"%s: %s is invalid, %s",
							fieldPath.Child("matchExpressions").Index(i).Child("values").Index(index),
							value,
							msg,
						))
				}
			}
		}
	}
	return warnings
}
