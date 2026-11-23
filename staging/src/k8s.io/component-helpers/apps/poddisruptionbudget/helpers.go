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

// Package poddisruptionbudget 提供处理 PodDisruptionBudget（PDB）状态的辅助函数，
// 主要是把控制器算出的可驱逐余额折算成 Condition，并判断该条件是否已是最新。
package poddisruptionbudget

import (
	policy "k8s.io/api/policy/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// UpdateDisruptionAllowedCondition 根据 DisruptionsAllowed 字段的取值，更新 PodDisruptionBudget
// 上的 DisruptionAllowed 条件。
//
// DisruptionsAllowed 是控制器计算出来的数值，而 Condition 是面向 API 消费者的布尔信号；
// 这里把前者折算成后者。函数只改内存中的对象、不做持久化，由调用方决定何时写回。
// ObservedGeneration 复用 PDB 已有的值，表示这个判断是针对控制器已经观察过的那一版 spec。
func UpdateDisruptionAllowedCondition(pdb *policy.PodDisruptionBudget) {
	// apimeta.SetStatusCondition 期望拿到非 nil 的条件切片；空切片和 nil 在 JSON 序列化时
	// 表现不同（分别是 [] 和省略），这里显式初始化以保持输出稳定。
	if pdb.Status.Conditions == nil {
		pdb.Status.Conditions = make([]metav1.Condition, 0)
	}
	// 允许驱逐的余额大于零即视为“可驱逐”，余额耗尽则给出“不足”的原因码，
	// 这样调用方无需自己解释数字的含义。
	if pdb.Status.DisruptionsAllowed > 0 {
		apimeta.SetStatusCondition(&pdb.Status.Conditions, metav1.Condition{
			Type:               policy.DisruptionAllowedCondition,
			Reason:             policy.SufficientPodsReason,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: pdb.Status.ObservedGeneration,
		})
	} else {
		apimeta.SetStatusCondition(&pdb.Status.Conditions, metav1.Condition{
			Type:               policy.DisruptionAllowedCondition,
			Reason:             policy.InsufficientPodsReason,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: pdb.Status.ObservedGeneration,
		})
	}
}

// ConditionsAreUpToDate 检查 DisruptionAllowed 条件的 Status 与 Reason 是否与
// DisruptionsAllowed 字段推算出的期望值一致。
//
// 这是 UpdateDisruptionAllowedCondition 的“是否需要重写”判定，让控制器能跳过无意义的
// status 更新。三道检查分别排除条件缺失、spec 已被改动但状态还没跟上、以及条件与余额
// 不一致这三种情况；同时比对 Status 和 Reason 是因为只看 Status 无法区分“可驱逐”和
// “因余额不足而不可驱逐”等不同来源的语义。
func ConditionsAreUpToDate(pdb *policy.PodDisruptionBudget) bool {
	cond := apimeta.FindStatusCondition(pdb.Status.Conditions, policy.DisruptionAllowedCondition)
	// 条件还没建立，显然不是最新状态。
	if cond == nil {
		return false
	}

	// 用户已经修改了 spec 但控制器还没观察到新版本时，即便条件值与余额吻合也不能算最新，
	// 否则会把过期 spec 下的结论误判成有效。
	if pdb.Status.ObservedGeneration != pdb.Generation {
		return false
	}

	// 余额决定期望的布尔值；这里再带上 Reason 一起比对，防止条件来自旧逻辑或手工篡改。
	if pdb.Status.DisruptionsAllowed > 0 {
		return cond.Status == metav1.ConditionTrue && cond.Reason == policy.SufficientPodsReason
	}
	return cond.Status == metav1.ConditionFalse && cond.Reason == policy.InsufficientPodsReason
}
