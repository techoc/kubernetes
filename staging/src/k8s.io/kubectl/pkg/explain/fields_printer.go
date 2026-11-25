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

// indentDesc 是描述文本的缩进层级。
const indentDesc = 2

// regularFieldsPrinter 打印字段及其类型和描述。
type regularFieldsPrinter struct {
	Writer *Formatter
	Error  error
}

var _ proto.SchemaVisitor = &regularFieldsPrinter{}
var _ fieldsPrinter = &regularFieldsPrinter{}

// VisitArray 打印 Array 类型。它只是直接透传。
func (f *regularFieldsPrinter) VisitArray(a *proto.Array) {
	a.SubType.Accept(f)
}

// VisitKind 打印 Kind 类型。它打印 Kind 中的每个 key，
// 包括其类型、必填标记和描述。
func (f *regularFieldsPrinter) VisitKind(k *proto.Kind) {
	for _, key := range k.Keys() {
		v := k.Fields[key]
		required := ""
		if k.IsRequired(key) {
			required = " -required-"
		}

		if err := f.Writer.Write("%s\t<%s>%s", key, GetTypeName(v), required); err != nil {
			f.Error = err
			return
		}
		if err := f.Writer.Indent(indentDesc).WriteWrapped("%s", v.GetDescription()); err != nil {
			f.Error = err
			return
		}
		if err := f.Writer.Write(""); err != nil {
			f.Error = err
			return
		}
	}
}

// VisitMap 打印 Map 类型。它只是直接透传。
func (f *regularFieldsPrinter) VisitMap(m *proto.Map) {
	m.SubType.Accept(f)
}

// VisitPrimitive 打印 Primitive 类型。它终止递归。
func (f *regularFieldsPrinter) VisitPrimitive(p *proto.Primitive) {
	// 无需处理。实际上不应走到这里。
}

// VisitReference 打印 Reference 类型。它只是直接透传。
func (f *regularFieldsPrinter) VisitReference(r proto.Reference) {
	r.SubSchema().Accept(f)
}

// PrintFields 将写出 schema 中的类型。
func (f *regularFieldsPrinter) PrintFields(schema proto.Schema) error {
	schema.Accept(f)
	return f.Error
}
