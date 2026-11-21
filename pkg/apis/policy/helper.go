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

package policy

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// PDBV1beta1Label 是一个哨兵标签，专门用来在内部/v1 版本的 PodDisruptionBudget 中
	// 表达 v1beta1 特有的"匹配全部 Pod"语义，不会与真实的用户标签冲突。
	PDBV1beta1Label = "pdb.kubernetes.io/deprecated-v1beta1-empty-selector-match"
)

var (
	// 下面四个 selector 是 PDB 跨版本转换时使用的规范表示。
	// v1beta1 对空 selector 的解释与内部/v1 版本相反：v1beta1 中空 selector 表示不匹配任何 Pod，
	// 而内部/v1 中空 selector 表示匹配全部 Pod。因此用一个带哨兵标签的条件来无损表达
	// "内部版本中的全部不匹配"，而空 selector 则代表"内部版本中的全部匹配"。

	// NonV1beta1MatchAllSelector 是内部/v1 版本里匹配全部 Pod 的空 selector。
	NonV1beta1MatchAllSelector = &metav1.LabelSelector{}
	// NonV1beta1MatchNoneSelector 是内部/v1 版本里不匹配任何 Pod 的 selector：
	// 要求哨兵标签存在，而真实 Pod 不可能带这个已弃用的哨兵标签。
	NonV1beta1MatchNoneSelector = &metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{{Key: PDBV1beta1Label, Operator: metav1.LabelSelectorOpExists}},
	}

	// V1beta1MatchNoneSelector 是 v1beta1 版本里不匹配任何 Pod 的空 selector。
	V1beta1MatchNoneSelector = &metav1.LabelSelector{}
	// V1beta1MatchAllSelector 是 v1beta1 版本里匹配全部 Pod 的 selector：
	// 要求哨兵标签不存在，由于 Pod 实际不会带该标签，因此条件恒成立。
	V1beta1MatchAllSelector = &metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{{Key: PDBV1beta1Label, Operator: metav1.LabelSelectorOpDoesNotExist}},
	}
)

// StripPDBV1beta1Label 从 selector 的 MatchExpressions 中原地剔除用于表达
// v1beta1 空 selector 语义的哨兵标签条件，避免这个内部实现细节泄漏到用户可见的
// selector 中。传入 nil 时无需处理；用 [:0] 原地过滤以免额外分配切片。
func StripPDBV1beta1Label(selector *metav1.LabelSelector) {
	if selector == nil {
		return
	}

	trimmedMatchExpressions := selector.MatchExpressions[:0]
	for _, exp := range selector.MatchExpressions {
		// 只丢弃哨兵标签对应的条件，其余用户显式指定的条件必须原样保留。
		if exp.Key != PDBV1beta1Label {
			trimmedMatchExpressions = append(trimmedMatchExpressions, exp)
		}
	}
	selector.MatchExpressions = trimmedMatchExpressions
}
