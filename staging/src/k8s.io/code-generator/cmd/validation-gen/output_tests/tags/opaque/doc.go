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

// +k8s:validation-gen=*
// +k8s:validation-gen-scheme-registry=k8s.io/code-generator/cmd/validation-gen/testscheme.Scheme
// +k8s:validation-gen-test-targets

// 这是一个测试包。
// opaque 验证 +k8s:opaqueType 关闭嵌套类型校验的各种放置方式：
// 字段、内嵌、eachVal/eachKey 以及类型定义。
// +k8s:validation-gen-nolint
package opaque

import (
	"k8s.io/code-generator/cmd/validation-gen/testscheme"
)

var localSchemeBuilder = testscheme.New()

type Struct struct {
	TypeMeta int

	// +k8s:validateFalse="field Struct.StructField"
	StructField OtherStruct `json:"structField"`

	// +k8s:validateFalse="field Struct.StructPtrField"
	// +k8s:required
	StructPtrField *OtherStruct `json:"structPtrField"`

	// +k8s:validateFalse="field Struct.OpaqueStructField"
	// +k8s:opaqueType
	OpaqueStructField OtherStruct `json:"opaqueStructField"`

	// +k8s:validateFalse="field Struct.OpaqueStructPtrField"
	// +k8s:required
	// +k8s:opaqueType
	OpaqueStructPtrField *OtherStruct `json:"opaqueStructPtrField"`

	// +k8s:validateFalse="field Struct.SliceOfStructField"
	// +k8s:eachVal=+k8s:validateFalse="field Struct.SliceOfStructField vals"
	SliceOfStructField []OtherStruct `json:"sliceOfStructField"`

	// +k8s:validateFalse="field Struct.SliceOfOpaqueStructField"
	// +k8s:eachVal=+k8s:validateFalse="field Struct.SliceOfOpaqueStructField vals"
	// +k8s:eachVal=+k8s:opaqueType
	SliceOfOpaqueStructField []OtherStruct `json:"sliceOfOpaqueStructField"`

	// +k8s:validateFalse="field Struct.ListMapOfStructField"
	// +k8s:eachVal=+k8s:validateFalse="field Struct.ListMapOfStructField vals"
	// +k8s:listType=map
	// +k8s:listMapKey=stringField
	ListMapOfStructField []OtherStruct `json:"listMapOfStructField"`

	// +k8s:validateFalse="field Struct.ListMapOfOpaqueStructField"
	// +k8s:eachVal=+k8s:validateFalse="field Struct.ListMapOfOpaqueStructField vals"
	// +k8s:listType=map
	// +k8s:listMapKey=stringField
	// +k8s:eachVal=+k8s:opaqueType
	ListMapOfOpaqueStructField []OtherStruct `json:"listMapOfOpaqueStructField"`

	// +k8s:validateFalse="field Struct.MapOfStringToStructField"
	// +k8s:eachKey=+k8s:validateFalse="field Struct.MapOfStringToStructField keys"
	// +k8s:eachVal=+k8s:validateFalse="field Struct.MapOfStringToStructField vals"
	MapOfStringToStructField map[OtherString]OtherStruct `json:"mapOfStringToStructField"`

	// +k8s:validateFalse="field Struct.MapOfStringToOpaqueStructField"
	// +k8s:eachKey=+k8s:validateFalse="field Struct.MapOfStringToOpaqueStructField keys"
	// +k8s:eachVal=+k8s:validateFalse="field Struct.MapOfStringToOpaqueStructField vals"
	// +k8s:eachKey=+k8s:opaqueType
	// +k8s:eachVal=+k8s:opaqueType
	MapOfStringToOpaqueStructField map[OtherString]OtherStruct `json:"mapOfStringToOpaqueStructField"`
}

// +k8s:validateFalse="type OtherStruct"
type OtherStruct struct {
	// +k8s:validateFalse="field OtherStruct.StringField"
	StringField string `json:"stringField"`
}

// OpaqueFieldsStruct 含带 opaque 标记的字段且无字段校验，不应为这些字段生成校验。
// +k8s:validateTrue="type OpaqueFieldsStruct"
type OpaqueFieldsStruct struct {
	// +k8s:opaqueType
	OtherStruct

	// +k8s:eachVal=+k8s:opaqueType
	OpaqueSliceField []OtherStruct `json:"opaqueSliceField"`

	// +k8s:eachKey=+k8s:opaqueType
	// +k8s:eachVal=+k8s:opaqueType
	OpaqueMapField map[OtherString]OtherStruct `json:"opaqueMapField"`

	TypedefOpaqueStructField TypedefOpaqueStruct `json:"typedefOpaqueStructField"`

	TypedefOpaqueSliceField TypedefOpaqueSlice `json:"typedefOpaqueSliceField"`

	TypedefOpaqueMapField TypedefOpaqueMap `json:"typedefOpaqueMapField"`

	// +k8s:opaqueType
	IsolatedOpaqueStructField OtherStruct `json:"isolatedOpaqueStructField"`
}

// +k8s:validateFalse="type OtherString"
type OtherString string

// TODO: validateFalse 夹具尚不支持 map/slice 类型，修复需先修 randfill，日后再说。
// 目前以下类型已验证能配合生成正确代码：
// +k8s:opaqueType.

// +k8s:opaqueType
type TypedefOpaqueStruct struct {
	// +k8s:required
	StringField string `json:"stringField"`
}

// +k8s:eachVal=+k8s:opaqueType
type TypedefOpaqueSlice []OtherStruct

// +k8s:eachKey=+k8s:opaqueType
// +k8s:eachVal=+k8s:opaqueType
type TypedefOpaqueMap map[OtherString]OtherStruct

// +k8s:validateTrue="type TypedefSliceOther"
// +k8s:eachVal=+k8s:opaqueType
type TypedefSliceOther []OtherStruct

// +k8s:validateTrue="type TypedefMapOther"
// +k8s:eachKey=+k8s:opaqueType
// +k8s:eachVal=+k8s:opaqueType
type TypedefMapOther map[OtherString]OtherStruct

type NoValidationStruct struct {
	StringField string `json:"stringField"`
}

type NoValidationString string

// OpaqueNoValidationFieldsStruct 验证：type/key/val 标记 opaque 但无校验时，
// 不会生成任何校验调用。
// +k8s:validateTrue="type OpaqueNoValidationFieldsStruct"
type OpaqueNoValidationFieldsStruct struct {
	// +k8s:opaqueType
	NoValidationStruct

	// +k8s:eachVal=+k8s:opaqueType
	OpaqueSliceField []NoValidationStruct `json:"opaqueSliceField"`

	// +k8s:eachKey=+k8s:opaqueType
	// +k8s:eachVal=+k8s:opaqueType
	OpaqueMapField map[NoValidationString]NoValidationStruct `json:"opaqueMapField"`

	// +k8s:opaqueType
	IsolatedOpaqueStructField NoValidationStruct `json:"isolatedOpaqueStructField"`
}

// +k8s:opaqueType
// +k8s:validateFalse="type OpaqueStructWithValidation"
type OpaqueStructWithValidation struct {
	// +k8s:validateFalse="field OpaqueStructWithValidation.StringField"
	StringField string `json:"stringField"`
}

type BaseStruct struct {
	// +k8s:validateFalse="field BaseStruct.Field"
	Field string `json:"field"`
}

// +k8s:opaqueType
// +k8s:validateFalse="type OpaqueAliasWithValidation"
type OpaqueAliasWithValidation BaseStruct

type ParentWithOpaqueAliasWithValidation struct {
	TypeMeta int                       `json:"typeMeta"`
	Field    OpaqueAliasWithValidation `json:"field"`
}
