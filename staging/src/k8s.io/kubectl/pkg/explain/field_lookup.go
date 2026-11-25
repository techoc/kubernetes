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

package explain

import (
	"fmt"

	"k8s.io/kube-openapi/pkg/util/proto"
)

// fieldLookup 沿路径遍历 schema，
// 并返回最终的 schema。
type fieldLookup struct {
	// 要遍历的路径
	Path []string

	// 返回信息：找到的 Schema，或错误。
	Schema proto.Schema
	Error  error
}

// SaveLeafSchema 用于判断路径是否已遍历完毕，
// 并将该 schema 保存为匹配结果。
func (f *fieldLookup) SaveLeafSchema(schema proto.Schema) bool {
	if len(f.Path) != 0 {
		return false
	}

	f.Schema = schema

	return true
}

// VisitArray 基本只是原样传递。
func (f *fieldLookup) VisitArray(a *proto.Array) {
	if f.SaveLeafSchema(a) {
		return
	}

	// 原样传递数组。
	a.SubType.Accept(f)
}

// VisitMap 基本只是原样传递。
func (f *fieldLookup) VisitMap(m *proto.Map) {
	if f.SaveLeafSchema(m) {
		return
	}

	// 原样传递 map。
	m.SubType.Accept(f)
}

// VisitPrimitive 会停止操作并将自身作为找到的 schema 返回，
// 即使还有更多路径需要遍历。
func (f *fieldLookup) VisitPrimitive(p *proto.Primitive) {
	// 即使 Path 不为空（即预期不是叶子节点），
	// 也返回该 primitive。
	f.Schema = p
}

// VisitKind 在找到字段时将其逐个从路径中弹出。
func (f *fieldLookup) VisitKind(k *proto.Kind) {
	if f.SaveLeafSchema(k) {
		return
	}

	subSchema, ok := k.Fields[f.Path[0]]
	if !ok {
		f.Error = fmt.Errorf("field %q does not exist", f.Path[0])
		return
	}

	f.Path = f.Path[1:]
	subSchema.Accept(f)
}

func (f *fieldLookup) VisitArbitrary(a *proto.Arbitrary) {
	f.Schema = a
}

// VisitReference 基本只是原样传递。
func (f *fieldLookup) VisitReference(r proto.Reference) {
	if f.SaveLeafSchema(r) {
		return
	}

	// 原样传递引用。
	r.SubSchema().Accept(f)
}

// LookupSchemaForField 在基础 schema 中查找给定路径对应的 schema。
func LookupSchemaForField(schema proto.Schema, path []string) (proto.Schema, error) {
	f := &fieldLookup{Path: path}
	schema.Accept(f)
	return f.Schema, f.Error
}
