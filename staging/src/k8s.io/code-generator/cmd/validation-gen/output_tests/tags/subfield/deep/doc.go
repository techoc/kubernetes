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

// +k8s:validation-gen=TypesWithField=TypeMeta
// +k8s:validation-gen-scheme-registry=k8s.io/code-generator/cmd/validation-gen/testscheme.Scheme

// Package subfield 包含用于测试 subfield 字段校验标签的类型。
// +k8s:validation-gen-nolint
// 本文件当前没有可生成的内容：下方用例被整体注释，等待生成器支持为
// 中间字段配置 optionality/requiredness 以处理 NPE。
package deep

// TODO: 等生成的校验代码能通过配置中间字段的 optional/required 处理 NPE 后，
// 再取消注释以下代码。

/**
// 被注释掉的用例覆盖：多层 subfield 链（结构体→结构体/切片/map）、
// optional 与 setByServer/required 的组合，以及 opaqueType 与指针场景。
import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// Struct 演示结构体 subfield 字段的校验。
type Struct struct {
	TypeMeta int `json:"typeMeta"`

	// +k8s:subfield(structField)=+k8s:subfield(stringField)=+k8s:validateFalse="Struct.StructField.StructField 1"
	// +k8s:subfield(structField)=+k8s:subfield(stringField)=+k8s:validateFalse="Struct.StructField.StructField 2"
	// +k8s:subfield(sliceField)=+k8s:eachVal=+k8s:subfield(stringField)=+k8s:validateFalse="Struct.StructField.SliceField"
	// +k8s:subfield(mapField)=+k8s:eachVal=+k8s:subfield(stringField)=+k8s:validateFalse="Struct.StructField.MapField"
	StructField OtherStruct `json:"structField"`

	// +k8s:subfield(structField)=+k8s:subfield(stringField)=+k8s:validateFalse="Struct.StructPtrField.StructField 1"
	// +k8s:subfield(structField)=+k8s:subfield(stringField)=+k8s:validateFalse="Struct.StructPtrField.StructField 2"
	// +k8s:subfield(sliceField)=+k8s:eachVal=+k8s:subfield(stringField)=+k8s:validateFalse="Struct.StructPtrField.SliceField"
	// +k8s:subfield(mapField)=+k8s:eachVal=+k8s:subfield(stringField)=+k8s:validateFalse="Struct.StructPtrField.MapField"
	StructPtrField *OtherStruct `json:"structPtrField"`
}

type OtherStruct struct {
	StructField SmallStruct            `json:"structField"`
	SliceField  []SmallStruct          `json:"sliceField"`
	MapField    map[string]SmallStruct `json:"mapField"`
}

type SmallStruct struct {
	StringField string `json:"stringField"`
}

type StructWithOptionalField struct {
	// +k8s:optional
	OptionalField string `json:"optionalField"`
}

type SetByServerStruct struct {
	TypeMeta int `json:"typeMeta"`

	// +k8s:subfield(optionalField)=+k8s:setByServer
	SubfieldSetByServer StructWithOptionalField `json:"subfieldSetByServer"`

	// +k8s:subfield(stringField)=+k8s:setByServer
	// +k8s:subfield(stringField)=+k8s:optional
	EmbeddedSetByServer UnvalidatedStruct `json:"embeddedSetByServer"`

	// +k8s:subfield(optionalField)=+k8s:required
	// +k8s:opaqueType
	OpaqueSubfieldRequired StructWithOptionalField `json:"opaqueSubfieldRequired"`

	// +k8s:subfield(optionalField)=+k8s:required
	SubfieldRequired StructWithOptionalField `json:"subfieldRequired"`

	// +k8s:optional
	// +k8s:subfield(optionalField)=+k8s:setByServer
	SubfieldPtrSetByServer *StructWithOptionalField `json:"subfieldPtrSetByServer"`

	// +k8s:optional
	// +k8s:subfield(optionalField)=+k8s:required
	SubfieldPtrRequired *StructWithOptionalField `json:"subfieldPtrRequired"`
}
**/
