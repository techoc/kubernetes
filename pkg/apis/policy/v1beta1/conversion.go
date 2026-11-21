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

package v1beta1

import (
	"k8s.io/api/policy/v1beta1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/kubernetes/pkg/apis/policy"
)

// Convert_v1beta1_PodDisruptionBudget_To_policy_PodDisruptionBudget
// 把 v1beta1 版本的 PodDisruptionBudget 转换为内部版本。
// 转换的关键在于 v1beta1 的 selector 对"空"的解释与 v1/内部版本不同：
// v1beta1 中 nil selector 表示匹配全部 Pod，而空但非 nil 的 selector 表示不匹配任何 Pod；
// 在内部版本中语义正好相反（空 selector 表示匹配全部）。因此需要在这里做特殊映射，
// 以保证跨版本转换后选择语义保持不变。
func Convert_v1beta1_PodDisruptionBudget_To_policy_PodDisruptionBudget(in *v1beta1.PodDisruptionBudget, out *policy.PodDisruptionBudget, s conversion.Scope) error {
	if err := autoConvert_v1beta1_PodDisruptionBudget_To_policy_PodDisruptionBudget(in, out, s); err != nil {
		return err
	}

	switch {
	case apiequality.Semantic.DeepEqual(in.Spec.Selector, policy.V1beta1MatchNoneSelector):
		// v1beta1 中"空但非 nil"的 selector 表示不匹配任何 Pod，
		// 即使在内部版本或 v1 API 中也应保持这一语义。
		// 因此用一个非空但永远不会匹配任何 Pod 的 selector 来替代。
		out.Spec.Selector = policy.NonV1beta1MatchNoneSelector.DeepCopy()
	case apiequality.Semantic.DeepEqual(in.Spec.Selector, policy.V1beta1MatchAllSelector):
		// v1beta1 特有的"匹配全部"selector 需要换成 v1 中更简单的空"匹配全部"selector。
		out.Spec.Selector = policy.NonV1beta1MatchAllSelector.DeepCopy()
	default:
		// 其他情况下，确保用于表达"匹配全部/全部不匹配"的特殊标签
		// 不会与用户显式指定的字段混在一起，避免破坏选择语义。
		policy.StripPDBV1beta1Label(out.Spec.Selector)
	}
	return nil
}

// Convert_policy_PodDisruptionBudget_To_v1beta1_PodDisruptionBudget
// 把内部版本的 PodDisruptionBudget 转换为 v1beta1 版本。
// 与反向转换对称：内部版本的 selector 语义需要映射回 v1beta1 的惯例
// （nil 表示全部匹配，空但非 nil 表示全部不匹配）。
func Convert_policy_PodDisruptionBudget_To_v1beta1_PodDisruptionBudget(in *policy.PodDisruptionBudget, out *v1beta1.PodDisruptionBudget, s conversion.Scope) error {
	if err := autoConvert_policy_PodDisruptionBudget_To_v1beta1_PodDisruptionBudget(in, out, s); err != nil {
		return err
	}

	switch {
	case apiequality.Semantic.DeepEqual(in.Spec.Selector, policy.NonV1beta1MatchNoneSelector):
		// 内部版本中是 v1beta1 特有的"全部不匹配"selector，
		// 换成 v1beta1 中更简单的空"全部不匹配"selector。
		out.Spec.Selector = policy.V1beta1MatchNoneSelector.DeepCopy()
	case apiequality.Semantic.DeepEqual(in.Spec.Selector, policy.NonV1beta1MatchAllSelector):
		// 内部版本中"空但非 nil"的 selector 表示匹配所有 Pod。
		// 由于 v1beta1 API 对空 selector 的解释不同，这里必须加上
		// 一个显式匹配所有 Pod 的非空 selector 来保证语义一致。
		out.Spec.Selector = policy.V1beta1MatchAllSelector.DeepCopy()
	}
	return nil
}
