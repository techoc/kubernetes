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

package openapi

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"unicode"

	restful "github.com/emicklei/go-restful/v3"

	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
	"k8s.io/kube-openapi/pkg/util"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// verbs 是操作名前缀的字典树，用于从 "getPod" 这类名字中切出动词。
// "patch" 重复出现一次是历史遗留，Trie 会去重，不影响行为。
var verbs = util.NewTrie([]string{"get", "log", "read", "replace", "patch", "delete", "deletecollection", "watch", "connect", "proxy", "list", "create", "patch"})

const (
	// extensionGVK 是 OpenAPI 定义上的扩展键，声明该类型对应的全部 GVK。
	extensionGVK = "x-kubernetes-group-version-kind"
)

// ToValidOperationID makes an string a valid op ID (e.g. removing punctuations and whitespaces and make it camel case)
//
// 处理规则：保留字母、下划线和非首位的数字；其余字符作为分隔并触发下一个字符大写。
// 首字符不允许数字，以保证生成的 ID 是合法的编程语言标识符。
func ToValidOperationID(s string, capitalizeFirstLetter bool) string {
	var buffer bytes.Buffer
	capitalize := capitalizeFirstLetter
	for i, r := range s {
		if unicode.IsLetter(r) || r == '_' || (i != 0 && unicode.IsDigit(r)) {
			if capitalize {
				buffer.WriteRune(unicode.ToUpper(r))
				capitalize = false
			} else {
				buffer.WriteRune(r)
			}
		} else {
			capitalize = true
		}
	}
	return buffer.String()
}

// GetOperationIDAndTags returns a customize operation ID and a list of tags for kubernetes API server's OpenAPI spec to prevent duplicate IDs.
//
// 操作 ID 由动词 + 组 + 版本 + 资源拼成，并去掉 ".k8s.io" 后缀；tags 用于
// 在 OpenAPI 文档中按组/资源分组。无法识别动词时返回错误，提示路由命名不规范。
func GetOperationIDAndTags(r *restful.Route) (string, []string, error) {
	op := r.Operation
	path := r.Path
	var tags []string
	prefix, exists := verbs.GetPrefix(op)
	if !exists {
		return op, tags, fmt.Errorf("operation names should start with a verb. Cannot determine operation verb from %v", op)
	}
	op = op[len(prefix):]
	parts := strings.Split(strings.Trim(path, "/"), "/")
	// Assume /api is /apis/core, remove this when we actually server /api/... on /apis/core/...
	if len(parts) >= 1 && parts[0] == "api" {
		parts = append([]string{"apis", "core"}, parts[1:]...)
	}
	if len(parts) >= 2 && parts[0] == "apis" {
		trimmed := strings.TrimSuffix(parts[1], ".k8s.io")
		prefix = prefix + ToValidOperationID(trimmed, prefix != "")
		tag := ToValidOperationID(trimmed, false)
		if len(parts) > 2 {
			prefix = prefix + ToValidOperationID(parts[2], prefix != "")
			tag = tag + "_" + ToValidOperationID(parts[2], false)
		}
		tags = append(tags, tag)
	} else if len(parts) >= 1 {
		tags = append(tags, ToValidOperationID(parts[0], false))
	}
	return prefix + ToValidOperationID(op, prefix != ""), tags, nil
}

// groupVersionKinds 是可按 (group, version, kind) 排序的 GVK 列表，
// 保证同一 OpenAPI 定义多次生成时扩展内容顺序稳定。
type groupVersionKinds []v1.GroupVersionKind

// Len 实现 sort.Interface。
func (s groupVersionKinds) Len() int {
	return len(s)
}

// Swap 实现 sort.Interface。
func (s groupVersionKinds) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Less 按 group、version、kind 依次比较，提供全序。
func (s groupVersionKinds) Less(i, j int) bool {
	if s[i].Group == s[j].Group {
		if s[i].Version == s[j].Version {
			return s[i].Kind < s[j].Kind
		}
		return s[i].Version < s[j].Version
	}
	return s[i].Group < s[j].Group
}

// JSON 把 GVK 列表转换为可嵌入 spec.Extensions 的结构。
func (s groupVersionKinds) JSON() []interface{} {
	j := []interface{}{}
	for _, gvk := range s {
		j = append(j, map[string]interface{}{
			"group":   gvk.Group,
			"version": gvk.Version,
			"kind":    gvk.Kind,
		})
	}
	return j
}

// DefinitionNamer is the type to customize OpenAPI definition name.
type DefinitionNamer struct {
	// typeGroupVersionKinds 以 OpenAPI 定义名为键，记录该定义对应的所有 GVK。
	typeGroupVersionKinds map[string]groupVersionKinds
}

// gvkConvert 在 schema 与 metav1 的 GVK 类型间转换。
func gvkConvert(gvk schema.GroupVersionKind) v1.GroupVersionKind {
	return v1.GroupVersionKind{
		Group:   gvk.Group,
		Version: gvk.Version,
		Kind:    gvk.Kind,
	}
}

// NewDefinitionNamer constructs a new DefinitionNamer to be used to customize OpenAPI spec.
//
// 多个 scheme 可能包含同名类型（不同组/版本），这里按定义名聚合去重，
// 并排序后写入扩展，避免生成结果随 map 遍历顺序抖动。
func NewDefinitionNamer(schemes ...*runtime.Scheme) *DefinitionNamer {
	ret := &DefinitionNamer{
		typeGroupVersionKinds: map[string]groupVersionKinds{},
	}
	for _, s := range schemes {
		for gvk := range s.AllKnownTypes() {
			newGVK := gvkConvert(gvk)
			exists := false
			name, err := s.ToOpenAPIDefinitionName(gvk)
			if err != nil {
				// 定义名冲突属于 scheme 配置错误，启动期直接失败比生成错误文档更安全。
				klog.Fatalf("failed to get OpenAPI definition name for %v: %v", gvk, err)
				continue
			}
			// 同一 GVK 可能被多个 scheme 注册，去重避免扩展里出现重复项。
			for _, existingGVK := range ret.typeGroupVersionKinds[name] {
				if newGVK == existingGVK {
					exists = true
					break
				}
			}
			if !exists {
				ret.typeGroupVersionKinds[name] = append(ret.typeGroupVersionKinds[name], newGVK)
			}
		}
	}
	for _, gvk := range ret.typeGroupVersionKinds {
		sort.Sort(gvk)
	}
	return ret
}

// GetDefinitionName returns the name and tags for a given definition
// 未知定义名原样返回且不带扩展，供非 Kubernetes 类型使用。
func (d *DefinitionNamer) GetDefinitionName(name string) (string, spec.Extensions) {
	if groupVersionKinds, ok := d.typeGroupVersionKinds[name]; ok {
		return name, spec.Extensions{
			extensionGVK: groupVersionKinds.JSON(),
		}
	}
	return name, nil
}
