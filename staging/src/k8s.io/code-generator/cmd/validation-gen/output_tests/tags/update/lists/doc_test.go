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

package lists

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// TestUpdateListTags 覆盖 update 标签在列表上的全部组合：
// NoSet/NoUnset（整个字段）、NoAddItem/NoRemoveItem（逐元素），
// 以及它们与 listType=set/map、复合 key、atomic+unique、eachVal 的交互。
// 关键不变式：更新校验按 key（而非下标）关联元素，重排不算修改。
func TestUpdateListTags(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	matcher := field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin()
	base := UpdateListStruct{}

	// slice NoSet（len == 0 语义）
	{
		old := base
		old.StringSliceNoSet = nil
		cur := base
		cur.StringSliceNoSet = []string{"a"}

		st.Value(&cur).OldValue(&old).ExpectMatches(matcher, field.ErrorList{
			field.Invalid(field.NewPath("stringSliceNoSet"), nil, "field cannot be set once created").WithOrigin("update"),
		})

		// nil -> nil 是允许的
		st.Value(&old).OldValue(&old).ExpectValid()
	}

	// slice NoUnset
	{
		old := base
		old.StringSliceNoUnset = []string{"a"}
		cur := base
		cur.StringSliceNoUnset = nil

		st.Value(&cur).OldValue(&old).ExpectMatches(matcher, field.ErrorList{
			field.Invalid(field.NewPath("stringSliceNoUnset"), nil, "field cannot be cleared once set").WithOrigin("update"),
		})
	}

	// listType=set + NoAddItem
	{
		old := base
		old.StringSetNoAdd = []string{"a", "b"}
		cur := base
		cur.StringSetNoAdd = []string{"a", "b", "c"}

		st.Value(&cur).OldValue(&old).ExpectMatches(matcher, field.ErrorList{
			field.Forbidden(field.NewPath("stringSetNoAdd").Index(2), "item may not be added").WithOrigin("update"),
		})

		// 已有元素重排允许。
		reordered := base
		reordered.StringSetNoAdd = []string{"b", "a"}
		st.Value(&reordered).OldValue(&old).ExpectValid()
	}

	// listType=set + NoRemoveItem
	{
		old := base
		old.StringSetNoRemove = []string{"a", "b", "c"}
		cur := base
		cur.StringSetNoRemove = []string{"a", "c"}

		st.Value(&cur).OldValue(&old).ExpectMatches(matcher, field.ErrorList{
			field.Forbidden(field.NewPath("stringSetNoRemove"), "may not be removed").WithOrigin("update"),
		})
	}

	// 固定形状 set：NoAddItem + NoRemoveItem
	{
		old := base
		old.StringSetFrozenShape = []string{"a", "b"}

		// 排列合法。
		perm := base
		perm.StringSetFrozenShape = []string{"b", "a"}
		st.Value(&perm).OldValue(&old).ExpectValid()

		// 替换一个元素：算一次新增加一次删除，两者都上报。
		swap := base
		swap.StringSetFrozenShape = []string{"a", "c"}
		st.Value(&swap).OldValue(&old).ExpectMatches(matcher, field.ErrorList{
			field.Forbidden(field.NewPath("stringSetFrozenShape").Index(1), "item may not be added").WithOrigin("update"),
			field.Forbidden(field.NewPath("stringSetFrozenShape"), "may not be removed").WithOrigin("update"),
		})
	}

	// listType=map + 单键 + NoAddItem
	{
		old := base
		old.MapListNoAdd = []UpdateItem{{Name: "alpha", Value: "1"}}
		cur := base
		cur.MapListNoAdd = []UpdateItem{
			{Name: "alpha", Value: "1"},
			{Name: "beta", Value: "2"},
		}

		st.Value(&cur).OldValue(&old).ExpectMatches(matcher, field.ErrorList{
			field.Forbidden(field.NewPath("mapListNoAdd").Index(1), "item may not be added").WithOrigin("update"),
		})

		// 修改已有带键元素的值不算“新增”。
		modified := base
		modified.MapListNoAdd = []UpdateItem{{Name: "alpha", Value: "999"}}
		st.Value(&modified).OldValue(&old).ExpectValid()
	}

	// listType=map + 单键 + 固定形状
	{
		old := base
		old.MapListFrozenShape = []UpdateItem{
			{Name: "alpha", Value: "1"},
			{Name: "beta", Value: "2"},
		}

		// 修改已有带键元素的值允许。
		modify := base
		modify.MapListFrozenShape = []UpdateItem{
			{Name: "alpha", Value: "99"},
			{Name: "beta", Value: "2"},
		}
		st.Value(&modify).OldValue(&old).ExpectValid()

		// 删除键会产生 NoRemoveItem 错误。
		removed := base
		removed.MapListFrozenShape = []UpdateItem{{Name: "alpha", Value: "1"}}
		st.Value(&removed).OldValue(&old).ExpectMatches(matcher, field.ErrorList{
			field.Forbidden(field.NewPath("mapListFrozenShape"), "may not be removed").WithOrigin("update"),
		})
	}

	// listType=map + 复合键 + NoRemoveItem
	{
		old := base
		old.CompositeKeyList = []CompositeKeyItem{
			{Name: "alpha", Priority: 1, Value: "x"},
			{Name: "alpha", Priority: 2, Value: "y"},
		}

		// 键相同、值不同：允许。
		modify := base
		modify.CompositeKeyList = []CompositeKeyItem{
			{Name: "alpha", Priority: 1, Value: "x"},
			{Name: "alpha", Priority: 2, Value: "y2"},
		}
		st.Value(&modify).OldValue(&old).ExpectValid()

		// 整对删除：拒绝。
		removed := base
		removed.CompositeKeyList = []CompositeKeyItem{
			{Name: "alpha", Priority: 1, Value: "x"},
		}
		st.Value(&removed).OldValue(&old).ExpectMatches(matcher, field.ErrorList{
			field.Forbidden(field.NewPath("compositeKeyList"), "may not be removed").WithOrigin("update"),
		})
	}

	// listType=atomic + unique=set + NoAddItem
	{
		old := base
		old.AtomicUniqueSetNoAdd = []string{"a"}
		cur := base
		cur.AtomicUniqueSetNoAdd = []string{"a", "b"}
		st.Value(&cur).OldValue(&old).ExpectMatches(matcher, field.ErrorList{
			field.Forbidden(field.NewPath("atomicUniqueSetNoAdd").Index(1), "item may not be added").WithOrigin("update"),
		})
	}

	// listType=atomic + unique=map：经 unique= 走语义 map 路径
	{
		old := base
		old.AtomicUniqueMapFrozenShape = []UpdateItem{
			{Name: "alpha", Value: "1"},
			{Name: "beta", Value: "2"},
		}

		// 修改已有带键元素的值允许。这
		// 与 listType=map 行为一致。
		modify := base
		modify.AtomicUniqueMapFrozenShape = []UpdateItem{
			{Name: "alpha", Value: "99"},
			{Name: "beta", Value: "2"},
		}
		st.Value(&modify).OldValue(&old).ExpectValid()

		// 新增键被拒绝。
		add := base
		add.AtomicUniqueMapFrozenShape = []UpdateItem{
			{Name: "alpha", Value: "1"},
			{Name: "beta", Value: "2"},
			{Name: "gamma", Value: "3"},
		}
		st.Value(&add).OldValue(&old).ExpectMatches(matcher, field.ErrorList{
			field.Forbidden(field.NewPath("atomicUniqueMapFrozenShape").Index(2), "item may not be added").WithOrigin("update"),
		})
	}

	// listType=set 用于不可直接比较的元素
	{
		old := base
		old.NonComparableSetFrozenShape = []NonComparableItem{
			{Name: "alpha", Tags: []string{"x", "y"}},
			{Name: "beta", Tags: []string{"z"}},
		}

		// 整值元素重排允许（SemanticDeepEqual 匹配
		// 旧列表中的每一个元素）。
		reordered := base
		reordered.NonComparableSetFrozenShape = []NonComparableItem{
			{Name: "beta", Tags: []string{"z"}},
			{Name: "alpha", Tags: []string{"x", "y"}},
		}
		st.Value(&reordered).OldValue(&old).ExpectValid()

		// 修改元素内部的标签会改变其在下面的标识：
		// set 语义（整个元素就是键），因此
		// 会同时报新增和删除。
		mutated := base
		mutated.NonComparableSetFrozenShape = []NonComparableItem{
			{Name: "alpha", Tags: []string{"x", "y", "NEW"}},
			{Name: "beta", Tags: []string{"z"}},
		}
		st.Value(&mutated).OldValue(&old).ExpectMatches(matcher, field.ErrorList{
			field.Forbidden(field.NewPath("nonComparableSetFrozenShape").Index(0), "item may not be added").WithOrigin("update"),
			field.Forbidden(field.NewPath("nonComparableSetFrozenShape"), "may not be removed").WithOrigin("update"),
		})
	}

	// typedef 列表：元数据从类型继承
	{
		old := base
		old.TypedefFrozenList = FrozenUserList{{Name: "alpha", Value: "1"}}

		// 仅修改已有带键元素的值：允许。
		modify := base
		modify.TypedefFrozenList = FrozenUserList{{Name: "alpha", Value: "2"}}
		st.Value(&modify).OldValue(&old).ExpectValid()

		// 新增带键的元素：拒绝。
		add := base
		add.TypedefFrozenList = FrozenUserList{
			{Name: "alpha", Value: "1"},
			{Name: "beta", Value: "9"},
		}
		st.Value(&add).OldValue(&old).ExpectMatches(matcher, field.ErrorList{
			field.Forbidden(field.NewPath("typedefFrozenList").Index(1), "item may not be added").WithOrigin("update"),
		})
	}

	// 列表上的 eachVal 组合：逐元素 NoModify
	{
		old := base
		old.EachValNoModifyList = []UpdateItem{
			{Name: "alpha", Value: "1"},
			{Name: "beta", Value: "2"},
		}

		// 新增带键元素允许，NoModify 只约束单个元素。
		add := base
		add.EachValNoModifyList = []UpdateItem{
			{Name: "alpha", Value: "1"},
			{Name: "beta", Value: "2"},
			{Name: "gamma", Value: "3"},
		}
		st.Value(&add).OldValue(&old).ExpectValid()

		// 删除带键元素允许，NoModify 只约束单个元素。
		remove := base
		remove.EachValNoModifyList = []UpdateItem{{Name: "alpha", Value: "1"}}
		st.Value(&remove).OldValue(&old).ExpectValid()

		// 修改已有带键元素的非键 Value 字段会触发。
		mutate := base
		mutate.EachValNoModifyList = []UpdateItem{
			{Name: "alpha", Value: "99"},
			{Name: "beta", Value: "2"},
		}
		st.Value(&mutate).OldValue(&old).ExpectMatches(matcher, field.ErrorList{
			field.Invalid(field.NewPath("eachValNoModifyList").Index(0), nil, "cannot be modified").WithOrigin("update"),
		})
	}

	// 指针 slice NoSet
	{
		old := base
		old.PointerSliceNoSet = nil
		cur := base
		cur.PointerSliceNoSet = []*string{new("a")}

		st.Value(&cur).OldValue(&old).ExpectMatches(matcher, field.ErrorList{
			field.Invalid(field.NewPath("pointerSliceNoSet"), nil, "field cannot be set once created").WithOrigin("update"),
		})

		// nil -> nil 是允许的
		st.Value(&old).OldValue(&old).ExpectValid()
	}

	// 指针 listType=set + NoAddItem
	{
		old := base
		old.PointerSetNoAdd = []*string{new("a"), new("b")}
		cur := base
		cur.PointerSetNoAdd = []*string{new("a"), new("b"), new("c")}

		st.Value(&cur).OldValue(&old).ExpectMatches(matcher, field.ErrorList{
			field.Forbidden(field.NewPath("pointerSetNoAdd").Index(2), "item may not be added").WithOrigin("update"),
		})
	}

	// 指针 listType=map + NoRemoveItem
	{
		old := base
		old.PointerMapListNoRemove = []*UpdateItem{
			{Name: "alpha", Value: "1"},
			{Name: "beta", Value: "2"},
		}
		cur := base
		cur.PointerMapListNoRemove = []*UpdateItem{
			{Name: "alpha", Value: "1"},
		}

		st.Value(&cur).OldValue(&old).ExpectMatches(matcher, field.ErrorList{
			field.Forbidden(field.NewPath("pointerMapListNoRemove"), "item may not be removed").WithOrigin("update"),
		})
	}

	// 更新时指针 slice 中的 nil 元素应由 PtrSliceUpdate 报 Required 错误
	{
		old := base
		old.PointerSetNoAdd = []*string{new("a")}
		cur := base
		cur.PointerSetNoAdd = []*string{nil}

		st.Value(&cur).OldValue(&old).ExpectMatches(matcher, field.ErrorList{
			field.Required(field.NewPath("pointerSetNoAdd").Index(0), ""),
		})
	}
}
