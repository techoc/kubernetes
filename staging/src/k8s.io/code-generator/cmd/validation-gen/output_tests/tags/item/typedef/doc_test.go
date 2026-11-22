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

package typedef

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Test 验证 item 选择器声明在切片 typedef 上时的行为，以及字段级与
// typedef 级选择器的叠加、嵌套 typedef 与同 key 冲突等场景。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 未命中 item 选择器的元素合法。
	st.Value(&Struct{
		TypedefItems: ItemList{
			{Key: "a", Data: "d1"},
			{Key: "b", Data: "d2"},
		},
	}).ExpectValid()

	// 命中 typedef 上声明的 item 选择器，错误定位到元素索引。
	st.Value(&Struct{
		TypedefItems: ItemList{
			{Key: "a", Data: "d1"},
			{Key: "validated", Data: "d2"},
		},
	}).ExpectValidateFalseByPath(map[string][]string{
		`typedefItems[1]`: {"item ItemList[key=validated]"},
	})

	// 验证 typedef 上的不可变约束：同一 key 的元素数据被修改应报错。
	// Test immutability on typedef.
	oldStruct := &Struct{
		TypedefItems: ItemList{
			{Key: "immutable", Data: "original"},
		},
	}
	newStruct := &Struct{
		TypedefItems: ItemList{
			{Key: "immutable", Data: "changed"},
		},
	}
	st.Value(newStruct).OldValue(oldStruct).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("typedefItems").Index(0), nil, "immutable").WithOrigin("immutable"),
	})

	// 验证 typedef 的 typedef：选择器应沿别名链继续生效。
	// Test nested typedef (typedef of typedef).
	st.Value(&Struct{
		NestedTypedefItems: ItemListAlias{
			{Key: "normal", Data: "d1"},
			{Key: "aliased", Data: "d2"},
		},
	}).ExpectValidateFalseByPath(map[string][]string{
		`nestedTypedefItems[1]`: {"item ItemListAlias[key=aliased]"},
	})

	// 字段级与 typedef 级各有一个 item 选择器，两条校验都应触发。
	// Test tag on field and typedef.
	st.Value(&Struct{
		DualItems: DualItemList{
			{ID: "a", Name: "n1"},
			{ID: "typedef-target", Name: "n2"},
			{ID: "field-target", Name: "n3"},
		},
	}).ExpectValidateFalseByPath(map[string][]string{
		`dualItems[1]`: {"item DualItems[id=typedef-target] from typedef"},
		`dualItems[2]`: {"item DualItems[id=field-target] from field"},
	})

	// 字段级与 typedef 级选择器指向同一个 key 时，两条校验都保留。
	// Test tag on field and typedef with same key.
	st.Value(&Struct{
		ConflictingItems: ConflictingItemList{
			{ID: "a", Name: "n1"},
			{ID: "target", Name: "n2"},
		},
	}).ExpectValidateFalseByPath(map[string][]string{
		`conflictingItems[1]`: {
			"item ConflictingItems[id=target] from typedef",
			"item ConflictingItems[id=target] from field",
		},
	})
}
