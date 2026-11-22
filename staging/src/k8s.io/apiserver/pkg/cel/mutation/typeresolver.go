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

package mutation

import (
	"strings"

	"k8s.io/apiserver/pkg/cel/common"
	"k8s.io/apiserver/pkg/cel/mutation/dynamic"
)

// ObjectTypeName is the name of Object types that are used to declare the types of
// Kubernetes objects in CEL dynamically using the naming scheme "Object.<fieldName>...<fieldName>".
// For example "Object.spec.containers" is the type of the spec.containers field of the object in scope.
const ObjectTypeName = "Object"

// JSONPatchTypeName is the name of the JSONPatch type. This type is typically used to create JSON patches
// in CEL expressions.
const JSONPatchTypeName = "JSONPatch"

// DynamicTypeResolver resolves the Object and JSONPatch types when compiling
// CEL expressions without schema information about the object.
type DynamicTypeResolver struct{}

// Resolve 按类型名惰性构造 CEL 类型。
//
// 之所以叫"动态"：变异表达式在编译时可能拿不到 OpenAPI schema，只能从表达式里
// 用到的字段路径推断类型。返回 (nil, false) 表示不归本解析器负责，交由调用链
// 上其他解析器处理，这是 CEL 类型解析的组合约定。
func (r *DynamicTypeResolver) Resolve(name string) (common.ResolvedType, bool) {
	if name == JSONPatchTypeName {
		return &JSONPatchType{}, true
	}
	// "Object" 或 "Object.xxx" 形式统一交给 dynamic 包构造，后者从后缀路径推导字段类型。
	if name == ObjectTypeName || strings.HasPrefix(name, ObjectTypeName+".") {
		return dynamic.NewObjectType(name), true
	}
	return nil, false
}
