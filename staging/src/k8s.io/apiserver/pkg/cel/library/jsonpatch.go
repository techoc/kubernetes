/*
Copyright 2024 The Kubernetes Authors.

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

package library

import (
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"strings"
)

// JSONPatch 提供 JSONPatch 相关的 CEL 函数库。
//
// jsonpatch.escapeKey
//
// 把字符串转义为可用作 JSONPatch path key 的形式。
//
//	jsonpatch.escapeKey(<string>) <string>
//
// 示例：
//
//	"/metadata/labels/" + jsonpatch.escapeKey('k8s.io/my~label') // 返回 "/metadata/labels/k8s.io~1my~0label"
func JSONPatch() cel.EnvOption {
	return cel.Lib(jsonPatchLib)
}

var jsonPatchLib = &jsonPatch{}

// jsonPatch 实现 cel.Library。
type jsonPatch struct{}

// LibraryName 返回库名称。
func (*jsonPatch) LibraryName() string {
	return "kubernetes.jsonpatch"
}

// declarations 返回函数重载声明。
func (*jsonPatch) declarations() map[string][]cel.FunctionOpt {
	return jsonPatchLibraryDecls
}

// Types 返回该库引入的 CEL 类型：无。
func (*jsonPatch) Types() []*cel.Type {
	return []*cel.Type{}
}

// jsonPatchLibraryDecls 定义 jsonpatch.escapeKey 的全局函数重载。
var jsonPatchLibraryDecls = map[string][]cel.FunctionOpt{
	"jsonpatch.escapeKey": {
		cel.Overload("string_jsonpatch_escapeKey_string", []*cel.Type{cel.StringType}, cel.StringType,
			cel.UnaryBinding(escape)),
	},
}

// CompileOptions 把声明转换为 CEL 环境选项。
func (*jsonPatch) CompileOptions() []cel.EnvOption {
	var options []cel.EnvOption
	for name, overloads := range jsonPatchLibraryDecls {
		options = append(options, cel.Function(name, overloads...))
	}
	return options
}

// ProgramOptions 无额外程序选项。
func (*jsonPatch) ProgramOptions() []cel.ProgramOption {
	return []cel.ProgramOption{}
}

// jsonPatchReplacer 实现 JSON Pointer 的转义（RFC 6901）：'~' -> '~0'，'/' -> '~1'。
// 使用 strings.Replacer 单趟完成，避免先替换 '~' 后把新引入的 '~' 再次转义的经典错误。
var jsonPatchReplacer = strings.NewReplacer("/", "~1", "~", "~0")

// escapeKey 对 key 做 JSON Pointer 转义。
func escapeKey(k string) string {
	return jsonPatchReplacer.Replace(k)
}

// escape 是 jsonpatch.escapeKey 的 CEL 绑定实现。
func escape(arg ref.Val) ref.Val {
	s, ok := arg.Value().(string)
	if !ok {
		return types.MaybeNoSuchOverloadErr(arg)
	}
	escaped := escapeKey(s)
	return types.String(escaped)
}
