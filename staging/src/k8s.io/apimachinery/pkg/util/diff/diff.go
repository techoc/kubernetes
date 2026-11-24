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

// Package diff 提供用于测试与调试的对象差异字符串生成工具。
package diff

import (
	"encoding/json"
	"fmt"

	"github.com/pmezard/go-difflib/difflib"

	"k8s.io/utils/dump"
)

// Diff 返回两个对象差异的字符串表示。未启用 usegocmp 构建标签时，使用
// go-difflib/difflib 生成统一 diff。优先尝试 JSON 序列化；任一对象序列化失败时，
// 两侧都会回退为 dump 包生成的对象转储，以保证比较输入格式一致。
//
// Diff 返回两个对象差异的字符串表示。优先使用 JSON 序列化，
// JSON 序列化失败时回退到 dump 包的对象转储；
// 未使用 usegocmp 构建标签时通过 go-difflib/difflib 生成统一 diff。
func Diff(a, b any) string {

	aStr, aErr := toPrettyJSON(a)
	bStr, bErr := toPrettyJSON(b)
	if aErr != nil || bErr != nil {
		// 任一对象无法 JSON 序列化（如含不可导出字段、函数等）时改用 dump。
		aStr = dump.Pretty(a)
		bStr = dump.Pretty(b)
	}

	diff := difflib.UnifiedDiff{
		A:       difflib.SplitLines(aStr),
		B:       difflib.SplitLines(bStr),
		Context: 3,
	}

	diffstr, err := difflib.GetUnifiedDiffString(diff)
	if err != nil {
		return fmt.Sprintf("error generating diff: %v", err)
	}

	return diffstr
}

// toPrettyJSON 把对象转换为带缩进的 JSON 字符串。
func toPrettyJSON(data any) (string, error) {
	jsonData, err := json.MarshalIndent(data, "", " ")
	return string(jsonData), err
}
