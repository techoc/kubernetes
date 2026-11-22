/*
Copyright The Kubernetes Authors.

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

// +k8s:validation-gen=TypesWithField=TypeMeta
// +k8s:validation-gen-scheme-registry=k8s.io/code-generator/cmd/validation-gen/testscheme.Scheme

// Package shortcircuit contains test types for testing subfield short-circuit behavior.
// +k8s:validation-gen-nolint
// 验证 subfield 校验与目标字段自身校验（required/immutable/optional/forbidden/
// update 等）的短路关系：目标字段的校验失败时，subfield 校验不应再执行。
package shortcircuit

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// TargetWithRequired 的被选中字段是 required 指针。
type TargetWithRequired struct {
	// +k8s:required
	Value *string `json:"value"`
}

// TargetWithImmutable 的被选中字段是 immutable。
type TargetWithImmutable struct {
	// +k8s:immutable
	Value string `json:"value"`
	Other string `json:"other"`
}

// ParentWithRequired 在 required 目标上叠加 subfield 校验。
type ParentWithRequired struct {
	TypeMeta int `json:"typeMeta"`
	// +k8s:subfield(value)=+k8s:validateFalse="subfield ParentWithRequired.Field.Value"
	Field TargetWithRequired `json:"field"`
}

// ParentWithImmutable 在 immutable 目标上叠加 subfield 校验。
type ParentWithImmutable struct {
	TypeMeta int `json:"typeMeta"`
	// +k8s:subfield(value)=+k8s:validateFalse="subfield ParentWithImmutable.Field.Value"
	Field TargetWithImmutable `json:"field"`
}

// ParentWithOpaqueField 将目标标记为 opaqueType：不进入其内部校验。
type ParentWithOpaqueField struct {
	TypeMeta int `json:"typeMeta"`
	// +k8s:subfield(value)=+k8s:validateFalse="subfield ParentWithOpaqueField.Field.Value"
	// +k8s:opaqueType
	Field TargetWithRequired `json:"field"`
}

// ParentWithOpaqueImmutableField 是 opaqueType 与 immutable 的组合。
type ParentWithOpaqueImmutableField struct {
	TypeMeta int `json:"typeMeta"`
	// +k8s:subfield(value)=+k8s:validateFalse="subfield ParentWithOpaqueImmutableField.Field.Value"
	// +k8s:opaqueType
	Field TargetWithImmutable `json:"field"`
}

// ParentWithAlphaOpaqueField 用 alpha 级别标记 opaqueType。
type ParentWithAlphaOpaqueField struct {
	TypeMeta int `json:"typeMeta"`
	// +k8s:subfield(value)=+k8s:validateFalse="subfield ParentWithAlphaOpaqueField.Field.Value"
	// +k8s:alpha=+k8s:opaqueType
	Field TargetWithRequired `json:"field"`
}

// ParentWithAlphaOpaqueImmutableField 是 alpha opaqueType 与 immutable 的组合。
type ParentWithAlphaOpaqueImmutableField struct {
	TypeMeta int `json:"typeMeta"`
	// +k8s:subfield(value)=+k8s:validateFalse="subfield ParentWithAlphaOpaqueImmutableField.Field.Value"
	// +k8s:alpha=+k8s:opaqueType
	Field TargetWithImmutable `json:"field"`
}

// AliasOpaqueTargetWithRequired 在 typedef 上声明 opaqueType。
// +k8s:opaqueType
type AliasOpaqueTargetWithRequired TargetWithRequired

// ParentWithOpaqueAlias 通过 typedef 间接获得 opaque 语义。
type ParentWithOpaqueAlias struct {
	TypeMeta int `json:"typeMeta"`
	// +k8s:subfield(value)=+k8s:validateFalse="subfield ParentWithOpaqueAlias.Field.Value"
	Field AliasOpaqueTargetWithRequired `json:"field"`
}

// ParentWithPointerOpaqueAlias 用指针指向 opaque typedef。
type ParentWithPointerOpaqueAlias struct {
	TypeMeta int `json:"typeMeta"`
	// +k8s:subfield(value)=+k8s:validateFalse="subfield ParentWithPointerOpaqueAlias.Field.Value"
	Field *AliasOpaqueTargetWithRequired `json:"field"`
}

// AliasOpaqueTargetWithImmutable 在 typedef 上声明 opaqueType（immutable 目标）。
// +k8s:opaqueType
type AliasOpaqueTargetWithImmutable TargetWithImmutable

// ParentWithOpaqueImmutableAlias 通过 typedef 获得 opaque+immutable 语义。
type ParentWithOpaqueImmutableAlias struct {
	TypeMeta int `json:"typeMeta"`
	// +k8s:subfield(value)=+k8s:validateFalse="subfield ParentWithOpaqueImmutableAlias.Field.Value"
	Field AliasOpaqueTargetWithImmutable `json:"field"`
}

// ParentWithMultipleShortCircuit 同时叠加 required、immutable 与 subfield 校验。
type ParentWithMultipleShortCircuit struct {
	TypeMeta int `json:"typeMeta"`
	// +k8s:required
	// +k8s:subfield(value)=+k8s:immutable
	// +k8s:subfield(value)=+k8s:validateFalse="subfield ParentWithMultipleShortCircuit.Field.Value"
	Field *TargetWithRequired `json:"field"`
}

// TargetWithOptional 的被选中字段是 optional。
type TargetWithOptional struct {
	// +k8s:optional
	Value *string `json:"value"`
}

// ParentWithOptional 在 optional 目标上叠加 subfield 校验。
type ParentWithOptional struct {
	TypeMeta int `json:"typeMeta"`
	// +k8s:subfield(value)=+k8s:validateFalse="subfield ParentWithOptional.Field.Value"
	Field TargetWithOptional `json:"field"`
}

// ParentWithSubfieldRequiredAndChildOptional 对 optional 子字段强制 required。
type ParentWithSubfieldRequiredAndChildOptional struct {
	TypeMeta int `json:"typeMeta"`
	// +k8s:subfield(value)=+k8s:required
	// +k8s:subfield(value)=+k8s:validateFalse="subfield ParentWithSubfieldRequiredAndChildOptional.Field.Value"
	Field TargetWithOptional `json:"field"`
}

// TargetWithForbidden 的被选中字段被禁止设置。
type TargetWithForbidden struct {
	// +k8s:forbidden
	Value *string `json:"value"`
}

// ParentWithForbidden 在 forbidden 目标上叠加 subfield 校验。
type ParentWithForbidden struct {
	TypeMeta int `json:"typeMeta"`
	// +k8s:subfield(value)=+k8s:validateFalse="subfield ParentWithForbidden.Field.Value"
	Field TargetWithForbidden `json:"field"`
}

// TargetWithUpdate 的被选中字段声明了 update=NoModify。
type TargetWithUpdate struct {
	// +k8s:update=NoModify
	Value string `json:"value"`
}

// ParentWithUpdate 在 update 约束目标上叠加 subfield 校验。
type ParentWithUpdate struct {
	TypeMeta int `json:"typeMeta"`
	// +k8s:subfield(value)=+k8s:validateFalse="subfield ParentWithUpdate.Field.Value"
	Field TargetWithUpdate `json:"field"`
}

// TargetWithMaxItems 的被选中字段有列表长度上限。
type TargetWithMaxItems struct {
	// +k8s:maxItems=2
	Value []string `json:"value"`
}

// ParentWithMaxItems 在 maxItems 目标上叠加 subfield 校验。
type ParentWithMaxItems struct {
	TypeMeta int `json:"typeMeta"`
	// +k8s:subfield(value)=+k8s:validateFalse="subfield ParentWithMaxItems.Field.Value"
	Field TargetWithMaxItems `json:"field"`
}

// TargetWithMaxProperties 的被选中字段有 map 属性数量上限。
type TargetWithMaxProperties struct {
	// +k8s:maxProperties=2
	Value map[string]string `json:"value"`
}

// ParentWithMaxProperties 在 maxProperties 目标上叠加 subfield 校验。
type ParentWithMaxProperties struct {
	TypeMeta int `json:"typeMeta"`
	// +k8s:subfield(value)=+k8s:validateFalse="subfield ParentWithMaxProperties.Field.Value"
	Field TargetWithMaxProperties `json:"field"`
}
