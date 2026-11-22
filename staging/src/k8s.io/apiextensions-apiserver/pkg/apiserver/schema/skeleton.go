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

// StripValueValidations 返回一份去掉了值校验的副本。
// OpenAPI 输出等下游消费者不需要校验规则，剥离可减小文档体积；副本保证不改动原 schema。
func (s *Structural) StripValueValidations() *Structural {
	s = s.DeepCopy()
	v := Visitor{
		Structural: func(s *Structural) bool {
			changed := false
			if s.ValueValidation != nil {
				s.ValueValidation = nil
				changed = true
			}
			return changed
		},
	}
	v.Visit(s)
	return s
}

// StripNullable 返回一份去掉了 nullable 标记的副本。
// OpenAPI v2 不支持 nullable，需要在其输出前剥离；副本保证不改动原 schema。
func (s *Structural) StripNullable() *Structural {
	s = s.DeepCopy()
	v := Visitor{
		Structural: func(s *Structural) bool {
			changed := s.Nullable
			s.Nullable = false
			return changed
		},
	}
	v.Visit(s)
	return s
}
