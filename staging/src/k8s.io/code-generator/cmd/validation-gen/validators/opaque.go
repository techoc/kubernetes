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

package validators

import (
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/code-generator/cmd/validation-gen/util"
	"k8s.io/gengo/v2/codetags"
	"k8s.io/gengo/v2/types"
)

const (
	opaqueTypeTagName = "k8s:opaqueType"
)

// globalOpaqueTypes 与 globalOpaqueMembers 记录标为 opaque 的类型与结构体成员，
// 由 opaqueTypeTagValidator 在处理标签时填充。其他校验器（目前是 subfield）
// 据此决定是否从字段继承短路校验。因此查找必须放在延迟回调中，
// 等所有 opaque 标签都注册完再查。
//
// 全局状态是刻意为之：标签处理顺序无法保证，opaque 信息必须在所有标签都
// 处理完后才能查询，因此查询点放在 Deferred 回调里。
var (
	globalOpaqueTypes   = map[*types.Type]bool{}
	globalOpaqueMembers = map[*types.Member]bool{}
)

// opaqueTypeTagValidator 处理 +k8s:opaqueType 标签，并在全局表中登记
// 被标记的类型与字段。
type opaqueTypeTagValidator struct{}

func init() {
	RegisterTagValidator(opaqueTypeTagValidator{})
}

// Init 无需额外配置。
func (opaqueTypeTagValidator) Init(Config) {}

// TagName 返回标签全名。
func (opaqueTypeTagValidator) TagName() string {
	return opaqueTypeTagName
}

// ValidScopes: opaque 可标在类型、字段以及 list/map 的元素位置，
// 从而覆盖集合内的元素类型。
func (opaqueTypeTagValidator) ValidScopes() sets.Set[Scope] {
	return sets.New(ScopeType, ScopeField, ScopeListVal, ScopeMapKey, ScopeMapVal)
}

// GetValidations 登记 opaque 信息并返回 OpaqueType 标记。
//
// 只在不透明类型或字段的实际声明位置登记；调用方的字段查找使用
// globalOpaqueMembers，因此 Member 为 nil 时跳过。
func (opaqueTypeTagValidator) GetValidations(context Context, _ codetags.Tag) (Validations, error) {
	// 记录不透明类型与字段的信息，避免其他校验器
	// 重复提取。
	switch context.Scope {
	case ScopeType:
		globalOpaqueTypes[util.NonPointer(util.NativeType(context.Type))] = true
	case ScopeField:
		if context.Member != nil {
			globalOpaqueMembers[context.Member] = true
		}
	}
	return Validations{OpaqueType: true}, nil
}

// isFieldOpaque 判断字段或其类型是否被标记
// +k8s:opaqueType. It reads from the package-level registries above, so
// 调用方必须在延迟回调中（或其他 opaque 标签处理已完成的
// 上下文中）调用它。
//
// 同时检查字段级与类型级：字段级覆盖更精确，类型级让同一类型的所有使用处
// 都保持不透明；查询前先剥掉指针与 alias，确保比较的是底层类型。
func isFieldOpaque(context Context) bool {
	if context.Member != nil && globalOpaqueMembers[context.Member] {
		return true
	}
	if context.Type != nil && globalOpaqueTypes[util.NonPointer(util.NativeType(context.Type))] {
		return true
	}
	return false
}

// Docs 返回面向用户的标签文档。
func (v opaqueTypeTagValidator) Docs() TagDoc {
	doc := TagDoc{
		Tag:            opaqueTypeTagName,
		StabilityLevel: TagStabilityLevelStable,
		Scopes:         sets.List(v.ValidScopes()),
		Description: "Indicates that any validations declared on the referenced type will be ignored. " +
			"If a referenced type's package is not included in the generator's current " +
			"flags, this tag must be set, or code generation will fail (preventing silent " +
			"mistakes). If the validations should not be ignored, add the type's package " +
			"to the generator using the --readonly-pkg flag.",
	}
	return doc
}
