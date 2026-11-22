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

// Package coverage 提供声明式校验（validation-gen 生成的规则）的覆盖率断言工具。
// 测试进程登记“声明了什么规则”和“实际观察到了什么错误”，最后用
// AssertDeclarativeCoverage 检查每条声明规则都至少被触发过一次。
package coverage

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Rule 表示一条已声明的字段校验错误：错误类型加上可选的来源。
type Rule struct {
	ErrorType string
	Origin    string
}

// FieldRules 把字段路径映射到单个 Kind 上声明的规则列表。
type FieldRules map[string][]Rule

// ruleKey 是一条声明或观测规则在整个测试进程中的扁平身份，
// 用于对两个集合做差集。
type ruleKey struct {
	gvk       schema.GroupVersionKind
	path      string
	errorType string
	origin    string
}

// indexKeyRe 把运行时路径下标（"[0]"、"[my-key]"）归一化为 "[*]"，
// 使观测到的路径能与 validation-gen 生成的标准路径对齐。
var indexKeyRe = regexp.MustCompile(`\[[^\]]+\]`)

var (
	rulesMu       sync.Mutex
	declared      = sets.New[ruleKey]()
	observed      = sets.New[ruleKey]()
	nilPathString = (*field.Path)(nil).String()
)

// RegisterDeclaredRules 记录 validation-gen 为一个 GVK 声明的规则。
func RegisterDeclaredRules(gvk schema.GroupVersionKind, rules FieldRules) {
	rulesMu.Lock()
	defer rulesMu.Unlock()
	for path, rs := range rules {
		for _, r := range rs {
			declared.Insert(ruleKey{gvk: gvk, path: path, errorType: r.ErrorType, origin: r.Origin})
		}
	}
}

// RecordObservedRules 把 errs 中的每个错误标记为已针对给定 GVK 观测到。
// 对 (GVK, path, errorType, origin) 幂等。调用方需从自身上下文中推导 GVK
// （例如运行时 scheme 加请求信息）。
func RecordObservedRules(gvk schema.GroupVersionKind, errs field.ErrorList) {
	rulesMu.Lock()
	defer rulesMu.Unlock()
	// 运行时错误带真实下标（"spec.items[3]"、"metadata.labels[my-key]"），
	// 而声明路径使用 "[*]"。把所有 "[...]" 替换为 "[*]"，以便与 AssertDeclarativeCoverage
	// 中的差集查找匹配。
	for _, e := range errs {
		path := e.Field
		// nil 路径在运行时用 "<nil>" 之类的字符串表示，归一化为空串与声明侧一致。
		if path == nilPathString {
			path = ""
		}
		path = indexKeyRe.ReplaceAllString(path, "[*]")
		observed.Insert(ruleKey{gvk: gvk, path: path, errorType: string(e.Type), origin: e.Origin})
	}
}

// AssertDeclarativeCoverage 在通过 RegisterDeclaredRules 登记的每条规则都至少被观测到
// 一次时返回 nil；否则返回列出未覆盖规则的错误。
func AssertDeclarativeCoverage() error {
	rulesMu.Lock()
	uncovered := declared.Difference(observed).UnsortedList()
	rulesMu.Unlock()
	if len(uncovered) == 0 {
		return nil
	}
	return fmt.Errorf("%d uncovered declarative-validation rules:\n%s", len(uncovered), formatUncovered(uncovered))
}

// formatUncovered 按 GVK 分组渲染未覆盖的规则，每条一行。
// GVK 按 group/version/kind 排序；同一 GVK 内按 path/errorType/origin 排序，
// 保证失败输出稳定、可 diff。
//
// Example:
//
//	example.com/v1, Kind=Widget:
//	  spec.name  FieldValueRequired
//	  spec.name  FieldValueInvalid origin="format=dns-label"
func formatUncovered(uncovered []ruleKey) string {
	// 先按 GVK 分组，再分别排序，避免依赖 map 遍历顺序导致输出抖动。
	gvksToRules := map[schema.GroupVersionKind][]ruleKey{}
	for _, k := range uncovered {
		gvksToRules[k.gvk] = append(gvksToRules[k.gvk], k)
	}
	gvks := make([]schema.GroupVersionKind, 0, len(gvksToRules))
	for gvk := range gvksToRules {
		gvks = append(gvks, gvk)
	}
	sort.Slice(gvks, func(i, j int) bool {
		if gvks[i].Group != gvks[j].Group {
			return gvks[i].Group < gvks[j].Group
		}
		if gvks[i].Version != gvks[j].Version {
			return gvks[i].Version < gvks[j].Version
		}
		return gvks[i].Kind < gvks[j].Kind
	})
	var sb strings.Builder
	for _, gvk := range gvks {
		// core 组的 group 为空，此时只用版本号（如 "v1"）表示，与 API 惯例一致。
		gv := gvk.Group + "/" + gvk.Version
		if gvk.Group == "" {
			gv = gvk.Version
		}
		fmt.Fprintf(&sb, "%s, Kind=%s:\n", gv, gvk.Kind)
		keys := gvksToRules[gvk]
		sort.Slice(keys, func(i, j int) bool {
			if keys[i].path != keys[j].path {
				return keys[i].path < keys[j].path
			}
			if keys[i].errorType != keys[j].errorType {
				return keys[i].errorType < keys[j].errorType
			}
			return keys[i].origin < keys[j].origin
		})
		for _, k := range keys {
			if k.origin != "" {
				fmt.Fprintf(&sb, "  %s  %s origin=%q\n", k.path, k.errorType, k.origin)
			} else {
				fmt.Fprintf(&sb, "  %s  %s\n", k.path, k.errorType)
			}
		}
	}
	return sb.String()
}
