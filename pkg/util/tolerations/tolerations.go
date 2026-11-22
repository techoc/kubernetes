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

package tolerations

import (
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/klog/v2"
	api "k8s.io/kubernetes/pkg/apis/core"
)

// VerifyAgainstWhitelist 检查给定的 tolerations 是否都满足白名单：
// 每一个 toleration 都必须被白名单中某个项覆盖，否则返回 false。
// 任一侧为空时视为通过。
func VerifyAgainstWhitelist(tolerations, whitelist []api.Toleration) bool {
	if len(whitelist) == 0 || len(tolerations) == 0 {
		return true
	}

next:
	for _, t := range tolerations {
		for _, w := range whitelist {
			if isSuperset(w, t) {
				continue next
			}
		}
		return false
	}

	return true
}

// MergeTolerations 合并两组 tolerations。若某个 toleration 是另一个的超集，
// 只保留超集，从而去掉被覆盖的冗余项。
func MergeTolerations(first, second []api.Toleration) []api.Toleration {
	all := append(first, second...)
	var merged []api.Toleration

next:
	for i, t := range all {
		for _, t2 := range merged {
			if isSuperset(t2, t) {
				continue next // t 是冗余项，忽略
			}
		}
		if i+1 < len(all) {
			for _, t2 := range all[i+1:] {
				// 相等时保留靠前的那个，只有严格超集才替换。
				if !apiequality.Semantic.DeepEqual(&t, &t2) && isSuperset(t2, t) {
					continue next // t 是冗余项，忽略
				}
			}
		}
		merged = append(merged, t)
	}

	return merged
}

// isSuperset 判断 ss 容忍的范围是否是 t 的超集（即 ss 能覆盖 t）。
func isSuperset(ss, t api.Toleration) bool {
	if apiequality.Semantic.DeepEqual(&t, &ss) {
		return true
	}

	if t.Key != ss.Key &&
		// 空 key 配合 Exists 操作符表示匹配所有 key 和 value。
		(ss.Key != "" || ss.Operator != api.TolerationOpExists) {
		return false
	}

	// 空 effect 表示匹配所有 effect。
	if t.Effect != ss.Effect && ss.Effect != "" {
		return false
	}

	if ss.Effect == api.TaintEffectNoExecute {
		if ss.TolerationSeconds != nil {
			if t.TolerationSeconds == nil ||
				*t.TolerationSeconds > *ss.TolerationSeconds {
				return false
			}
		}
	}

	switch ss.Operator {
	case api.TolerationOpEqual, "": // 空操作符等同于 Equal
		return t.Operator == api.TolerationOpEqual && t.Value == ss.Value
	case api.TolerationOpExists:
		return true
	default:
		klog.Errorf("Unknown toleration operator: %s", ss.Operator)
		return false
	}
}
