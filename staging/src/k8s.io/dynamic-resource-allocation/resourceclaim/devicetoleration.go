/*
Copyright 2025 The Kubernetes Authors.

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

package resourceclaim

import (
	resourceapi "k8s.io/api/resource/v1"
)

// ToleratesTaint 判断给定的设备容忍度是否能够容忍指定的设备污点。
// 匹配规则：effect 为空时匹配所有 effect，否则必须与污点的 effect 相等；
// operator 为 Exists 时不比较 value，为 Equal（或空值，即默认值）时要求 value 严格相等；
// key 为空时匹配所有 key；此时 operator 必须为 Exists，从而同时匹配所有 value。
// 匹配逻辑照搬核心 Pod 容忍规则，未放入 resourceapi，因为 API 包不应承载行为逻辑。
// 来源：https://github.com/kubernetes/kubernetes/blob/f007012f5fe49e40ae0596cf463a8e7b247b3357/staging/src/k8s.io/api/core/v1/toleration.go#L39-L56。
// 调用方仍需自行判断 effect 类型：未知 effect 与 None effect 应被忽略，本函数只负责字段匹配。
func ToleratesTaint(toleration resourceapi.DeviceToleration, taint resourceapi.DeviceTaint) bool {
	// 1. effect 非空时必须与污点的 effect 完全一致。
	if toleration.Effect != "" && toleration.Effect != taint.Effect {
		return false
	}

	// 2. key 非空时必须与污点的 key 完全一致；key 为空则匹配所有 key。
	if toleration.Key != "" && toleration.Key != taint.Key {
		return false
	}

	// 3. 按 operator 区分 Equal 与 Exists 两种匹配语义。
	switch toleration.Operator {
	// Equal 语义：容忍度的 value 必须与污点的 value 完全相同。
	case "", resourceapi.DeviceTolerationOpEqual:
		return toleration.Value == taint.Value
	// Exists 语义：只要 key 与 effect 匹配即容忍，不比较 value。
	case resourceapi.DeviceTolerationOpExists:
		return true
	// 未知 operator 无法匹配，保守返回 false。
	default:
		return false
	}
}
