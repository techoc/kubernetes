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

package maps

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// TestUpdateMapTags 覆盖 map 字段上的 update 约束：
// NoSet/NoUnset（整体存在性）、NoAddItem/NoRemoveItem（键集合）、
// FrozenShape（键集合整体冻结）以及 eachVal 与 NoModify 的组合。
func TestUpdateMapTags(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 只比较类型、字段、详情子串与 origin，避免绑定完整的错误文本。
	matcher := field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin()
	base := UpdateMapStruct{}

	// map NoSet/NoUnset
	//
	// 空 map 视为“未设置”，非空视为“已设置”，nil 同样算清空。
	{
		old := base
		cur := base
		cur.MapNoSet = map[string]string{"a": "1"}
		st.Value(&cur).OldValue(&old).ExpectMatches(matcher, field.ErrorList{
			field.Invalid(field.NewPath("mapNoSet"), nil, "field cannot be set once created").WithOrigin("update"),
		})
	}
	{
		old := base
		old.MapNoUnset = map[string]string{"a": "1"}
		cur := base
		cur.MapNoUnset = nil
		st.Value(&cur).OldValue(&old).ExpectMatches(matcher, field.ErrorList{
			field.Invalid(field.NewPath("mapNoUnset"), nil, "field cannot be cleared once set").WithOrigin("update"),
		})
	}

	// map NoAddItem/NoRemoveItem
	//
	// 只约束键集合的增删；已有键的值变化不受影响。
	{
		old := base
		old.MapNoAdd = map[string]string{"a": "1"}
		cur := base
		cur.MapNoAdd = map[string]string{"a": "1", "b": "2"}
		st.Value(&cur).OldValue(&old).ExpectMatches(matcher, field.ErrorList{
			field.Forbidden(field.NewPath("mapNoAdd").Key("b"), "item may not be added").WithOrigin("update"),
		})

		// 修改已有键的值允许。
		modify := base
		modify.MapNoAdd = map[string]string{"a": "99"}
		st.Value(&modify).OldValue(&old).ExpectValid()
	}
	{
		old := base
		old.MapNoRemove = map[string]string{"a": "1", "b": "2"}
		cur := base
		cur.MapNoRemove = map[string]string{"a": "1"}
		st.Value(&cur).OldValue(&old).ExpectMatches(matcher, field.ErrorList{
			field.Forbidden(field.NewPath("mapNoRemove").Key("b"), "item may not be removed").WithOrigin("update"),
		})
	}

	// map 固定形状
	{
		old := base
		old.MapFrozenShape = map[string]string{"a": "1", "b": "2"}

		// 仅值变化：允许。
		modify := base
		modify.MapFrozenShape = map[string]string{"a": "11", "b": "22"}
		st.Value(&modify).OldValue(&old).ExpectValid()

		// 替换一个键：算一次新增加一次删除。
		//
		// 交换键会同时触发“新增”和“删除”两条错误。
		swap := base
		swap.MapFrozenShape = map[string]string{"a": "1", "c": "3"}
		st.Value(&swap).OldValue(&old).ExpectMatches(matcher, field.ErrorList{
			field.Forbidden(field.NewPath("mapFrozenShape").Key("c"), "item may not be added").WithOrigin("update"),
			field.Forbidden(field.NewPath("mapFrozenShape").Key("b"), "item may not be removed").WithOrigin("update"),
		})
	}

	// map 组合 NoSet + NoAddItem
	{
		old := base
		cur := base
		cur.MapSetThenFreeze = map[string]MapItem{"k": {Name: "k", Value: "v"}}

		// 两个约束同时触发：空到非空转换触发 NoSet，
		// 新键触发 NoAddItem。
		//
		// 两条约束检查的是不同层面，因此同时上报，而不是短路。
		st.Value(&cur).OldValue(&old).ExpectMatches(matcher, field.ErrorList{
			field.Invalid(field.NewPath("mapSetThenFreeze"), nil, "field cannot be set once created").WithOrigin("update"),
			field.Forbidden(field.NewPath("mapSetThenFreeze").Key("k"), "item may not be added").WithOrigin("update"),
		})
	}

	// map 上的 eachVal 组合：逐值 NoModify
	{
		old := base
		old.EachValNoModifyMap = map[string]MapItem{
			"a": {Name: "a", Value: "1"},
		}

		// 增删键是允许的。
		add := base
		add.EachValNoModifyMap = map[string]MapItem{
			"a": {Name: "a", Value: "1"},
			"b": {Name: "b", Value: "2"},
		}
		st.Value(&add).OldValue(&old).ExpectValid()

		// 修改已有值会触发校验。
		mutate := base
		mutate.EachValNoModifyMap = map[string]MapItem{
			"a": {Name: "a", Value: "99"},
		}
		st.Value(&mutate).OldValue(&old).ExpectMatches(matcher, field.ErrorList{
			field.Invalid(field.NewPath("eachValNoModifyMap").Key("a"), nil, "cannot be modified").WithOrigin("update"),
		})
	}
}
