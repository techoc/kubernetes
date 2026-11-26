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

// 这是一个测试包。
// +k8s:validation-gen-nolint
// 覆盖 +k8s:required 对各类可空/不可空字段的语义：
// 标量按零值判断，指针按 nil 判断，slice/map 按 nil 判断（空但非 nil 视为已设置）；
// typedef 上同时带验证标签，用于确认 required 与验证并行执行。
package required

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

type Struct struct {
	TypeMeta int

	// +k8s:required
	// +k8s:validateFalse="field Struct.StringField"
	StringField string `json:"stringField"`

	// +k8s:required
	// +k8s:validateFalse="field Struct.StringPtrField"
	StringPtrField *string `json:"stringPtrField"`

	// +k8s:required
	// +k8s:validateFalse="field Struct.StringTypedefField"
	StringTypedefField StringType `json:"stringTypedefField"`

	// +k8s:required
	// +k8s:validateFalse="field Struct.StringTypedefPtrField"
	StringTypedefPtrField *StringType `json:"stringTypedefPtrField"`

	// +k8s:required
	// +k8s:validateFalse="field Struct.IntField"
	IntField int `json:"intField"`

	// +k8s:required
	// +k8s:validateFalse="field Struct.IntPtrField"
	IntPtrField *int `json:"intPtrField"`

	// +k8s:required
	// +k8s:validateFalse="field Struct.IntTypedefField"
	IntTypedefField IntType `json:"intTypedefField"`

	// +k8s:required
	// +k8s:validateFalse="field Struct.IntTypedefPtrField"
	IntTypedefPtrField *IntType `json:"intTypedefPtrField"`

	// +k8s:required
	// +k8s:validateFalse="field Struct.BoolField"
	BoolField bool `json:"boolField"`

	// +k8s:required
	// +k8s:validateFalse="field Struct.FloatField"
	FloatField float64 `json:"floatField"`

	// +k8s:required
	// +k8s:validateFalse="field Struct.ByteField"
	ByteField byte `json:"byteField"`

	// 非指针结构体字段不能声明 required 或 optional

	// +k8s:required
	// +k8s:validateFalse="field Struct.OtherStructPtrField"
	OtherStructPtrField *OtherStruct `json:"otherStructPtrField"`

	// +k8s:required
	// +k8s:validateFalse="field Struct.SliceField"
	SliceField []string `json:"sliceField"`

	// +k8s:required
	// +k8s:validateFalse="field Struct.SliceTypedefField"
	SliceTypedefField SliceType `json:"sliceTypedefField"`

	// +k8s:required
	// +k8s:validateFalse="field Struct.ByteArrayField"
	ByteArrayField []byte `json:"byteArrayField"`

	// +k8s:required
	// +k8s:validateFalse="field Struct.MapField"
	MapField map[string]string `json:"mapField"`

	// +k8s:required
	// +k8s:validateFalse="field Struct.MapTypedefField"
	MapTypedefField MapType `json:"mapTypedefField"`
}

// +k8s:validateFalse="type StringType"
type StringType string

// +k8s:validateFalse="type IntType"
type IntType int

// +k8s:validateFalse="type OtherStruct"
type OtherStruct struct{}

// +k8s:validateFalse="type SliceType"
type SliceType []string

// +k8s:validateFalse="type MapType"
type MapType map[string]string
