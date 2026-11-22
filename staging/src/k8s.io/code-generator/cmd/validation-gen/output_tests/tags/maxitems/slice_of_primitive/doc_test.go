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

package sliceofprimitive

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Test 覆盖 +k8s:maxItems 的边界：nil 与空切片合法，长度为上限时合法，
// 超限时报 TooMany，并验证相同内容的旧值可被 ratcheting 放行。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	st.Value(&Struct{
		// All zero values
	}).ExpectValid()

	st.Value(&Struct{
		Max0Field:         make([]int, 0),
		Max10Field:        make([]int, 0),
		Max0TypedefField:  make([]IntType, 0),
		Max10TypedefField: make([]IntType, 0),
	}).ExpectValid()

	st.Value(&Struct{
		Max10Field:        make([]int, 1),
		Max10TypedefField: make([]IntType, 1),
	}).ExpectValid()

	st.Value(&Struct{
		Max10Field:        make([]int, 9),
		Max10TypedefField: make([]IntType, 9),
	}).ExpectValid()

	st.Value(&Struct{
		Max10Field:        make([]int, 10),
		Max10TypedefField: make([]IntType, 10),
	}).ExpectValid()

	testVal := &Struct{
		Max0Field:         make([]int, 1),
		Max10Field:        make([]int, 11),
		Max0TypedefField:  make([]IntType, 1),
		Max10TypedefField: make([]IntType, 11),
	}
	st.Value(testVal).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.TooMany(field.NewPath("max0Field"), 1, 0),
		field.TooMany(field.NewPath("max10Field"), 11, 10),
		field.TooMany(field.NewPath("max0TypedefField"), 1, 0),
		field.TooMany(field.NewPath("max10TypedefField"), 11, 10),
	})
	// Test validation ratcheting
	st.Value(&Struct{
		Max0Field:         make([]int, 1),
		Max10Field:        make([]int, 11),
		Max0TypedefField:  make([]IntType, 1),
		Max10TypedefField: make([]IntType, 11),
	}).OldValue(&Struct{
		Max0Field:         make([]int, 1),
		Max10Field:        make([]int, 11),
		Max0TypedefField:  make([]IntType, 1),
		Max10TypedefField: make([]IntType, 11),
	}).ExpectValid()

}
