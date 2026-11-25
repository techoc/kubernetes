/*
Copyright 2017 The Kubernetes Authors.

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

package strategicpatch

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"k8s.io/apimachinery/pkg/util/mergepatch"
	forkedjson "k8s.io/apimachinery/third_party/forked/golang/json"
	openapi "k8s.io/kube-openapi/pkg/util/proto"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// 这两个扩展键是 OpenAPI 中承载 strategic merge patch 元数据的约定名称。
const patchMergeKey = "x-kubernetes-patch-merge-key"
const patchStrategy = "x-kubernetes-patch-strategy"

// PatchMeta 保存某个字段的 patch 元数据：合并键与策略列表。
// 字段私有，只能通过 getter/setter 访问，便于将来改变内部表示。
type PatchMeta struct {
	patchStrategies []string
	patchMergeKey   string
}

// GetPatchStrategies 返回 patch 策略列表；未设置时返回空切片而非 nil，
// 避免调用方对 nil 做额外判断。
func (pm *PatchMeta) GetPatchStrategies() []string {
	if pm.patchStrategies == nil {
		return []string{}
	}
	return pm.patchStrategies
}

// SetPatchStrategies 设置 patch 策略列表。
func (pm *PatchMeta) SetPatchStrategies(ps []string) {
	pm.patchStrategies = ps
}

// GetPatchMergeKey 返回列表元素的合并键字段名。
func (pm *PatchMeta) GetPatchMergeKey() string {
	return pm.patchMergeKey
}

// SetPatchMergeKey 设置列表元素的合并键字段名。
func (pm *PatchMeta) SetPatchMergeKey(pmk string) {
	pm.patchMergeKey = pmk
}

// LookupPatchMeta 抽象"按字段名下钻 schema 并取出 patch 元数据"的能力，
// 使 strategic merge patch 既能基于 Go 结构体 tag，也能基于 OpenAPI 文档工作。
type LookupPatchMeta interface {
	//LookupPatchMetadataForStruct 获取映射的子 schema 及补丁元数据（如 patch strategy 和 merge key）。
	LookupPatchMetadataForStruct(key string) (LookupPatchMeta, PatchMeta, error)
	//LookupPatchMetadataForSlice 获取切片的子 schema 及补丁元数据。
	LookupPatchMetadataForSlice(key string) (LookupPatchMeta, PatchMeta, error)
	//获取字段的类型名。
	Name() string
}

// PatchMetaFromStruct 从 Go 结构体的 patchStrategy/patchMergeKey tag 中提取元数据。
type PatchMetaFromStruct struct {
	T reflect.Type
}

// NewPatchMetaFromStruct 由任意结构体（或指向结构体的指针）构造 PatchMetaFromStruct。
func NewPatchMetaFromStruct(dataStruct interface{}) (PatchMetaFromStruct, error) {
	t, err := getTagStructType(dataStruct)
	return PatchMetaFromStruct{T: t}, err
}

// 编译期断言：PatchMetaFromStruct 实现 LookupPatchMeta。
var _ LookupPatchMeta = PatchMetaFromStruct{}

// LookupPatchMetadataForStruct 下钻到 key 对应的结构体字段，读取其 patch tag。
func (s PatchMetaFromStruct) LookupPatchMetadataForStruct(key string) (LookupPatchMeta, PatchMeta, error) {
	fieldType, fieldPatchStrategies, fieldPatchMergeKey, err := forkedjson.LookupPatchMetadataForStruct(s.T, key)
	if err != nil {
		return nil, PatchMeta{}, err
	}

	return PatchMetaFromStruct{T: fieldType},
		PatchMeta{
			patchStrategies: fieldPatchStrategies,
			patchMergeKey:   fieldPatchMergeKey,
		}, nil
}

// LookupPatchMetadataForSlice 下钻到切片字段，并解出元素类型作为后续下钻的 schema。
func (s PatchMetaFromStruct) LookupPatchMetadataForSlice(key string) (LookupPatchMeta, PatchMeta, error) {
	subschema, patchMeta, err := s.LookupPatchMetadataForStruct(key)
	if err != nil {
		return nil, PatchMeta{}, err
	}
	elemPatchMetaFromStruct := subschema.(PatchMetaFromStruct)
	t := elemPatchMetaFromStruct.T

	var elemType reflect.Type
	switch t.Kind() {
	//如果 t 是数组或切片，则获取元素类型。
	//如果元素仍是数组或切片，则返回错误。
	//否则返回元素类型。
	case reflect.Array, reflect.Slice:
		elemType = t.Elem()
		if elemType.Kind() == reflect.Array || elemType.Kind() == reflect.Slice {
			return nil, PatchMeta{}, errors.New("unexpected slice of slice")
		}
	//如果 t 是指针，则获取其指向的元素。
	//如果底层元素既不是数组也不是切片，说明指针指向切片，
	//例如 https://github.com/kubernetes/kubernetes/blob/bc22e206c79282487ea0bf5696d5ccec7e839a76/staging/src/k8s.io/apimachinery/pkg/util/strategicpatch/patch_test.go#L2782-L2822
	//如果底层元素是数组或切片，则返回其元素类型。
	case reflect.Pointer:
		t = t.Elem()
		if t.Kind() == reflect.Array || t.Kind() == reflect.Slice {
			t = t.Elem()
		}
		elemType = t
	default:
		return nil, PatchMeta{}, fmt.Errorf("expected slice or array type, but got: %s", s.T.Kind().String())
	}

	return PatchMetaFromStruct{T: elemType}, patchMeta, nil
}

// Name 返回当前 schema 对应的 Go 类型种类名，用于错误信息。
func (s PatchMetaFromStruct) Name() string {
	return s.T.Kind().String()
}

// getTagStructType 取出结构体类型：nil 或非结构体输入返回错误，指针则解引用。
func getTagStructType(dataStruct interface{}) (reflect.Type, error) {
	if dataStruct == nil {
		return nil, mergepatch.ErrBadArgKind(struct{}{}, nil)
	}

	t := reflect.TypeOf(dataStruct)
	//获取指针的底层类型。
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return nil, mergepatch.ErrBadArgKind(struct{}{}, dataStruct)
	}

	return t, nil
}

// GetTagStructTypeOrDie 与 getTagStructType 相同，但失败时 panic，
// 用于启动阶段由编译期已知类型构造元数据的场景。
func GetTagStructTypeOrDie(dataStruct interface{}) reflect.Type {
	t, err := getTagStructType(dataStruct)
	if err != nil {
		panic(err)
	}
	return t
}

// PatchMetaFromOpenAPIV3 从 OpenAPI v3 schema 中提取 patch 元数据。
type PatchMetaFromOpenAPIV3 struct {
	//需要 SchemaList 来解析 OpenAPI v3 引用。
	SchemaList map[string]*spec.Schema
	Schema     *spec.Schema
}

// traverse 下钻到 key 对应的子 schema；schema 为空时返回空结果而非错误，
// 以便对没有 schema 的字段保持宽松。
func (s PatchMetaFromOpenAPIV3) traverse(key string) (PatchMetaFromOpenAPIV3, error) {
	if s.Schema == nil {
		return PatchMetaFromOpenAPIV3{}, nil
	}
	if len(s.Schema.Properties) == 0 {
		return PatchMetaFromOpenAPIV3{}, fmt.Errorf("unable to find api field \"%s\"", key)
	}
	subschema, ok := s.Schema.Properties[key]
	if !ok {
		return PatchMetaFromOpenAPIV3{}, fmt.Errorf("unable to find api field \"%s\"", key)
	}
	return PatchMetaFromOpenAPIV3{SchemaList: s.SchemaList, Schema: &subschema}, nil
}

// resolve 展开 allOf 与 $ref 引用，把 schema 指针替换为实际定义。
// 只取 allOf 的第一项，这是 Kubernetes OpenAPI v3 输出中 allOf 的既有形态。
func resolve(l *PatchMetaFromOpenAPIV3) error {
	if len(l.Schema.AllOf) > 0 {
		l.Schema = &l.Schema.AllOf[0]
	}
	if refString := l.Schema.Ref.String(); refString != "" {
		str := strings.TrimPrefix(refString, "#/components/schemas/")
		sch, ok := l.SchemaList[str]
		if ok {
			l.Schema = sch
		} else {
			return fmt.Errorf("unable to resolve %s in OpenAPI V3", refString)
		}
	}
	return nil
}

// LookupPatchMetadataForStruct 从 OpenAPI v3 schema 的扩展字段读取 patch 元数据，
// 然后解析 $ref 以便调用方继续下钻。
func (s PatchMetaFromOpenAPIV3) LookupPatchMetadataForStruct(key string) (LookupPatchMeta, PatchMeta, error) {
	l, err := s.traverse(key)
	if err != nil {
		return l, PatchMeta{}, err
	}
	p := PatchMeta{}
	f, ok := l.Schema.Extensions[patchMergeKey]
	if ok {
		p.SetPatchMergeKey(f.(string))
	}
	g, ok := l.Schema.Extensions[patchStrategy]
	if ok {
		p.SetPatchStrategies(strings.Split(g.(string), ","))
	}

	err = resolve(&l)
	return l, p, err
}

// LookupPatchMetadataForSlice 与结构体版本类似，但额外下钻到 items schema，
// 使后续查询针对切片元素而非切片本身。
func (s PatchMetaFromOpenAPIV3) LookupPatchMetadataForSlice(key string) (LookupPatchMeta, PatchMeta, error) {
	l, err := s.traverse(key)
	if err != nil {
		return l, PatchMeta{}, err
	}
	p := PatchMeta{}
	f, ok := l.Schema.Extensions[patchMergeKey]
	if ok {
		p.SetPatchMergeKey(f.(string))
	}
	g, ok := l.Schema.Extensions[patchStrategy]
	if ok {
		p.SetPatchStrategies(strings.Split(g.(string), ","))
	}
	if l.Schema.Items != nil {
		l.Schema = l.Schema.Items.Schema
	}
	err = resolve(&l)
	return l, p, err
}

// Name 返回 schema 的类型名；未声明类型时回退为 "Struct"。
func (s PatchMetaFromOpenAPIV3) Name() string {
	schema := s.Schema
	if len(schema.Type) > 0 {
		return strings.Join(schema.Type, "")
	}
	return "Struct"
}

// PatchMetaFromOpenAPI 基于 kube-openapi 的 proto.Schema 抽象提取 patch 元数据。
type PatchMetaFromOpenAPI struct {
	Schema openapi.Schema
}

// NewPatchMetaFromOpenAPI 由给定 schema 构造 PatchMetaFromOpenAPI。
func NewPatchMetaFromOpenAPI(s openapi.Schema) PatchMetaFromOpenAPI {
	return PatchMetaFromOpenAPI{Schema: s}
}

// 编译期断言：PatchMetaFromOpenAPI 实现 LookupPatchMeta。
var _ LookupPatchMeta = PatchMetaFromOpenAPI{}

// LookupPatchMetadataForStruct 通过 KindItem 访问者下钻到结构体字段并收集 patch 元数据。
func (s PatchMetaFromOpenAPI) LookupPatchMetadataForStruct(key string) (LookupPatchMeta, PatchMeta, error) {
	if s.Schema == nil {
		return &PatchMetaFromOpenAPI{}, PatchMeta{}, nil
	}
	kindItem := NewKindItem(key, s.Schema.GetPath())
	s.Schema.Accept(kindItem)

	err := kindItem.Error()
	if err != nil {
		return nil, PatchMeta{}, err
	}
	return PatchMetaFromOpenAPI{Schema: kindItem.subschema},
		kindItem.patchmeta, nil
}

// LookupPatchMetadataForSlice 通过 SliceItem 访问者下钻到切片元素 schema。
func (s PatchMetaFromOpenAPI) LookupPatchMetadataForSlice(key string) (LookupPatchMeta, PatchMeta, error) {
	if s.Schema == nil {
		return nil, PatchMeta{}, nil
	}
	sliceItem := NewSliceItem(key, s.Schema.GetPath())
	s.Schema.Accept(sliceItem)

	err := sliceItem.Error()
	if err != nil {
		return nil, PatchMeta{}, err
	}
	return PatchMetaFromOpenAPI{Schema: sliceItem.subschema},
		sliceItem.patchmeta, nil
}

// Name 返回 schema 自身的名称。
func (s PatchMetaFromOpenAPI) Name() string {
	schema := s.Schema
	return schema.GetName()
}
