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
	"strings"

	"k8s.io/apimachinery/pkg/util/mergepatch"
	openapi "k8s.io/kube-openapi/pkg/util/proto"
)

const (
	// OpenAPI 扩展字段名：声明列表的 patch 策略（如 merge/retainKeys）。
	patchStrategyOpenapiextensionKey = "x-kubernetes-patch-strategy"
	// OpenAPI 扩展字段名：声明列表合并时用于识别同一元素的键。
	patchMergeKeyOpenapiextensionKey = "x-kubernetes-patch-merge-key"
)

// LookupPatchItem 是在 OpenAPI schema 中查找补丁项的访问者接口。
type LookupPatchItem interface {
	openapi.SchemaVisitor

	Error() error
	Path() *openapi.Path
}

// kindItem 用于查找“对象字段”对应的补丁元数据。
type kindItem struct {
	key          string
	path         *openapi.Path
	err          error
	patchmeta    PatchMeta
	subschema    openapi.Schema
	hasVisitKind bool
}

// NewKindItem 创建一个在指定路径下查找 key 字段补丁元数据的访问者。
func NewKindItem(key string, path *openapi.Path) *kindItem {
	return &kindItem{
		key:  key,
		path: path,
	}
}

var _ LookupPatchItem = &kindItem{}

func (item *kindItem) Error() error {
	return item.err
}

func (item *kindItem) Path() *openapi.Path {
	return item.path
}

// VisitPrimitive 报告类型不匹配：目标应为对象（kind）。
func (item *kindItem) VisitPrimitive(schema *openapi.Primitive) {
	item.err = errors.New("expected kind, but got primitive")
}

// VisitArray 报告类型不匹配：目标应为对象（kind）。
func (item *kindItem) VisitArray(schema *openapi.Array) {
	item.err = errors.New("expected kind, but got slice")
}

// VisitMap 报告类型不匹配：目标应为对象（kind）。
func (item *kindItem) VisitMap(schema *openapi.Map) {
	item.err = errors.New("expected kind, but got map")
}

// VisitReference 在尚未到达目标 kind 时展开引用继续下钻；
// 只展开一次，避免循环引用导致无限递归。
func (item *kindItem) VisitReference(schema openapi.Reference) {
	if !item.hasVisitKind {
		schema.SubSchema().Accept(item)
	}
}

// VisitKind 在目标对象中查找字段，并解析其 patch 策略与合并键。
func (item *kindItem) VisitKind(schema *openapi.Kind) {
	subschema, ok := schema.Fields[item.key]
	if !ok {
		item.err = FieldNotFoundError{Path: schema.GetPath().String(), Field: item.key}
		return
	}

	mergeKey, patchStrategies, err := parsePatchMetadata(subschema.GetExtensions())
	if err != nil {
		item.err = err
		return
	}
	item.patchmeta = PatchMeta{
		patchStrategies: patchStrategies,
		patchMergeKey:   mergeKey,
	}
	item.subschema = subschema
}

// sliceItem 用于查找“列表字段”对应的补丁元数据与其元素 schema。
type sliceItem struct {
	key          string
	path         *openapi.Path
	err          error
	patchmeta    PatchMeta
	subschema    openapi.Schema
	hasVisitKind bool
}

// NewSliceItem 创建一个在指定路径下查找 key 列表字段补丁元数据的访问者。
func NewSliceItem(key string, path *openapi.Path) *sliceItem {
	return &sliceItem{
		key:  key,
		path: path,
	}
}

var _ LookupPatchItem = &sliceItem{}

func (item *sliceItem) Error() error {
	return item.err
}

func (item *sliceItem) Path() *openapi.Path {
	return item.path
}

// VisitPrimitive 报告类型不匹配：目标应为列表。
func (item *sliceItem) VisitPrimitive(schema *openapi.Primitive) {
	item.err = errors.New("expected slice, but got primitive")
}

// VisitArray 记录列表元素的 schema；必须先访问到 kind 才有效。
func (item *sliceItem) VisitArray(schema *openapi.Array) {
	if !item.hasVisitKind {
		item.err = errors.New("expected visit kind first, then visit array")
	}
	subschema := schema.SubType
	item.subschema = subschema
}

// VisitMap 报告类型不匹配：目标应为列表。
func (item *sliceItem) VisitMap(schema *openapi.Map) {
	item.err = errors.New("expected slice, but got map")
}

// VisitReference 未到达 kind 前展开引用继续查找；到达后直接取引用的子 schema
// 作为列表元素 schema。
func (item *sliceItem) VisitReference(schema openapi.Reference) {
	if !item.hasVisitKind {
		schema.SubSchema().Accept(item)
	} else {
		item.subschema = schema.SubSchema()
	}
}

// VisitKind 在目标对象中查找字段、解析补丁元数据，然后下钻到字段 schema
// 以继续访问（通常随后到达 array）。
func (item *sliceItem) VisitKind(schema *openapi.Kind) {
	subschema, ok := schema.Fields[item.key]
	if !ok {
		item.err = FieldNotFoundError{Path: schema.GetPath().String(), Field: item.key}
		return
	}

	mergeKey, patchStrategies, err := parsePatchMetadata(subschema.GetExtensions())
	if err != nil {
		item.err = err
		return
	}
	item.patchmeta = PatchMeta{
		patchStrategies: patchStrategies,
		patchMergeKey:   mergeKey,
	}
	item.hasVisitKind = true
	subschema.Accept(item)
}

// parsePatchMetadata 从 OpenAPI 扩展中解析补丁策略与合并键。
// 扩展值类型不符时返回错误，而不是静默忽略。
func parsePatchMetadata(extensions map[string]interface{}) (string, []string, error) {
	ps, foundPS := extensions[patchStrategyOpenapiextensionKey]
	var patchStrategies []string
	var mergeKey, patchStrategy string
	var ok bool
	if foundPS {
		patchStrategy, ok = ps.(string)
		if ok {
			// 策略可以是逗号分隔的多个值，例如 "merge,retainKeys"。
			patchStrategies = strings.Split(patchStrategy, ",")
		} else {
			return "", nil, mergepatch.ErrBadArgType(patchStrategy, ps)
		}
	}
	mk, foundMK := extensions[patchMergeKeyOpenapiextensionKey]
	if foundMK {
		mergeKey, ok = mk.(string)
		if !ok {
			return "", nil, mergepatch.ErrBadArgType(mergeKey, mk)
		}
	}
	return mergeKey, patchStrategies, nil
}
