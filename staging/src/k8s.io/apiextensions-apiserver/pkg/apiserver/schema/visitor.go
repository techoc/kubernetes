/*
Copyright 2019 The Kubernetes Authors.

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

package schema

// Visitor 递归遍历结构化 schema。
type Visitor struct {
	// Structural 在每个 Structural 节点上、递归进入子树之前调用。
	// 允许修改 s。若发生了修改则返回 true。
	// +optional
	Structural func(s *Structural) bool
	// NestedValueValidation 在每个 NestedValueValidation 节点上、递归进入子树之前调用。
	// 允许修改 vv。若发生了修改则返回 true。
	// +optional
	NestedValueValidation func(vv *NestedValueValidation) bool
}

// Visit 递归遍历结构化 schema，并在对应类型的每个节点上调用给定回调。
func (m *Visitor) Visit(s *Structural) {
	m.visitStructural(s)
}

// visitStructural 访问一个 Structural 节点及其子树，返回是否有修改。
func (m *Visitor) visitStructural(s *Structural) bool {
	ret := false
	if m.Structural != nil {
		ret = m.Structural(s)
	}

	if s.Items != nil {
		m.visitStructural(s.Items)
	}
	for k, v := range s.Properties {
		// 回调可能就地修改值，需写回 map
		if changed := m.visitStructural(&v); changed {
			ret = true
			s.Properties[k] = v
		}
	}
	if s.AdditionalProperties != nil && s.AdditionalProperties.Structural != nil {
		m.visitStructural(s.AdditionalProperties.Structural)
	}
	if s.ValueValidation != nil {
		for i := range s.ValueValidation.AllOf {
			m.visitNestedValueValidation(&s.ValueValidation.AllOf[i])
		}
		for i := range s.ValueValidation.AnyOf {
			m.visitNestedValueValidation(&s.ValueValidation.AnyOf[i])
		}
		for i := range s.ValueValidation.OneOf {
			m.visitNestedValueValidation(&s.ValueValidation.OneOf[i])
		}
		if s.ValueValidation.Not != nil {
			m.visitNestedValueValidation(s.ValueValidation.Not)
		}
	}

	return ret
}

// visitNestedValueValidation 访问一个 NestedValueValidation 节点及其子树，返回是否有修改。
func (m *Visitor) visitNestedValueValidation(vv *NestedValueValidation) bool {
	ret := false
	if m.NestedValueValidation != nil {
		ret = m.NestedValueValidation(vv)
	}

	if vv.Items != nil {
		m.visitNestedValueValidation(vv.Items)
	}
	for k, v := range vv.Properties {
		// 回调可能就地修改值，需写回 map
		if changed := m.visitNestedValueValidation(&v); changed {
			ret = true
			vv.Properties[k] = v
		}
	}
	if vv.AdditionalProperties != nil {
		m.visitNestedValueValidation(vv.AdditionalProperties)
	}
	for i := range vv.ValueValidation.AllOf {
		m.visitNestedValueValidation(&vv.ValueValidation.AllOf[i])
	}
	for i := range vv.ValueValidation.AnyOf {
		m.visitNestedValueValidation(&vv.ValueValidation.AnyOf[i])
	}
	for i := range vv.ValueValidation.OneOf {
		m.visitNestedValueValidation(&vv.ValueValidation.OneOf[i])
	}
	if vv.ValueValidation.Not != nil {
		m.visitNestedValueValidation(vv.ValueValidation.Not)
	}

	return ret
}
