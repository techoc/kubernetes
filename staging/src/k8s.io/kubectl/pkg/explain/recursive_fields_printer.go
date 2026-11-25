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

import "k8s.io/kube-openapi/pkg/util/proto"

// indentPerLevel 是每次字段递归的缩进层级。
const indentPerLevel = 3

// recursiveFieldsPrinter 针对给定的 schema
// 递归打印其所有字段。
type recursiveFieldsPrinter struct {
	Writer   *Formatter
	Error    error
	MaxDepth int
	Depth    int
}

var _ proto.SchemaVisitor = &recursiveFieldsPrinter{}
var _ fieldsPrinter = &recursiveFieldsPrinter{}
var visitedReferences = map[string]struct{}{}

// VisitArray 只是直接透传。
func (f *recursiveFieldsPrinter) VisitArray(a *proto.Array) {
	a.SubType.Accept(f)
}

// VisitKind 打印其所有字段及其类型，
// 然后递归进入每个字段内部（前序遍历）。
func (f *recursiveFieldsPrinter) VisitKind(k *proto.Kind) {
	for _, key := range k.Keys() {
		v := k.Fields[key]
		f.Writer.Write("%s\t<%s>", key, GetTypeName(v))
		if f.MaxDepth > 0 && f.Depth >= f.MaxDepth {
			continue
		}
		subFields := &recursiveFieldsPrinter{
			Writer:   f.Writer.Indent(indentPerLevel),
			MaxDepth: f.MaxDepth,
			Depth:    f.Depth + 1,
		}
		if err := subFields.PrintFields(v); err != nil {
			f.Error = err
			return
		}
	}
}

// VisitMap 只是直接透传。
func (f *recursiveFieldsPrinter) VisitMap(m *proto.Map) {
	m.SubType.Accept(f)
}

// VisitPrimitive 不做任何事，因为它没有子字段。
func (f *recursiveFieldsPrinter) VisitPrimitive(p *proto.Primitive) {
	// 无需处理。
}

// VisitReference 只是直接透传。
func (f *recursiveFieldsPrinter) VisitReference(r proto.Reference) {
	if _, ok := visitedReferences[r.Reference()]; ok {
		return
	}
	visitedReferences[r.Reference()] = struct{}{}
	r.SubSchema().Accept(f)
	delete(visitedReferences, r.Reference())
}

// PrintFields 将针对给定的 schema
// 递归打印其所有字段。
func (f *recursiveFieldsPrinter) PrintFields(schema proto.Schema) error {
	schema.Accept(f)
	return f.Error
}
