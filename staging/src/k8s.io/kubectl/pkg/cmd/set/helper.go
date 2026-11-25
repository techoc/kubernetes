/*
Copyright 2016 The Kubernetes Authors.

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

package set

import (
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/cli-runtime/pkg/resource"
)

// selectContainers 允许用一个字符串或通配符匹配一个或多个容器
func selectContainers(containers []v1.Container, spec string) ([]*v1.Container, []*v1.Container) {
	out := []*v1.Container{}
	skipped := []*v1.Container{}
	for i, c := range containers {
		if selectString(c.Name, spec) {
			out = append(out, &containers[i])
		} else {
			skipped = append(skipped, &containers[i])
		}
	}
	return out, skipped
}

// selectString 在提供的字符串匹配 spec 时返回 true，其中 spec 是带有
// 非贪婪 '*' 通配符运算符的字符串。
// TODO: 改用正则表达式，并处理贪婪匹配和回溯。
func selectString(s, spec string) bool {
	if spec == "*" {
		return true
	}
	if !strings.Contains(spec, "*") {
		return s == spec
	}

	pos := 0
	match := true
	parts := strings.Split(spec, "*")
Loop:
	for i, part := range parts {
		if len(part) == 0 {
			continue
		}
		next := strings.Index(s[pos:], part)
		switch {
		// 下一部分不在字符串中
		case next < pos:
			fallthrough
		// 第一部分与字符串开头不匹配
		case i == 0 && pos != 0:
			fallthrough
		// 最后一部分与字符串剩余部分不完全匹配
		case i == (len(parts)-1) && len(s) != (len(part)+next):
			match = false
			break Loop
		default:
			pos = next
		}
	}
	return match
}

// Patch 表示对对象所做的修改的结果。
type Patch struct {
	Info *resource.Info
	Err  error

	Before []byte
	After  []byte
	Patch  []byte
}

// PatchFn 是一个接受 info 对象并返回字节切片的函数类型。
// PatchFn 的实现应更新对象并返回其编码后的形式。
type PatchFn func(runtime.Object) ([]byte, error)

// CalculatePatch 对提供的 info 对象调用修改函数，并为对象中的变更生成
// strategic merge patch。Encoder 必须能将该 info 编码为适当的目标类型。
// 该函数返回修改函数是否对原对象做出了任何更改。
func CalculatePatch(patch *Patch, encoder runtime.Encoder, mutateFn PatchFn) bool {
	patch.Before, patch.Err = runtime.Encode(encoder, patch.Info.Object)
	patch.After, patch.Err = mutateFn(patch.Info.Object)
	if patch.Err != nil {
		return true
	}
	if patch.After == nil {
		return false
	}

	patch.Patch, patch.Err = strategicpatch.CreateTwoWayMergePatch(patch.Before, patch.After, patch.Info.Object)
	return true
}

// CalculatePatches 为每个提供的 info 对象计算补丁。如果提供的 mutateFn
// 未对某个对象做出更改，则该对象不会包含在最终的补丁列表中。
func CalculatePatches(infos []*resource.Info, encoder runtime.Encoder, mutateFn PatchFn) []*Patch {
	var patches []*Patch
	for _, info := range infos {
		patch := &Patch{Info: info}
		if CalculatePatch(patch, encoder, mutateFn) {
			patches = append(patches, patch)
		}
	}
	return patches
}

func findEnv(env []v1.EnvVar, name string) (v1.EnvVar, bool) {
	for _, e := range env {
		if e.Name == name {
			return e, true
		}
	}
	return v1.EnvVar{}, false
}

// updateEnv 在现有环境变量中添加和删除指定的环境变量。
// 添加的变量会替换所有同名的现有变量。
// 删除变量会移除所有同名的现有变量。
// 如果现有列表包含与所添加和删除的变量无关的重复项，
// 这些重复项会原样保留在结果中。
// 如果一个变量同时被添加和删除，则删除优先。
func updateEnv(existing []v1.EnvVar, env []v1.EnvVar, remove []string) []v1.EnvVar {
	out := []v1.EnvVar{}
	covered := sets.New[string](remove...)
	for _, e := range existing {
		if covered.Has(e.Name) {
			continue
		}
		newer, ok := findEnv(env, e.Name)
		if ok {
			covered.Insert(e.Name)
			out = append(out, newer)
			continue
		}
		out = append(out, e)
	}
	for _, e := range env {
		if covered.Has(e.Name) {
			continue
		}
		covered.Insert(e.Name)
		out = append(out, e)
	}
	return out
}
