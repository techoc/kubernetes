/*
Copyright 2015 The Kubernetes Authors.

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

package fields

import (
	"testing"
)

// matches 断言 Fields 的字符串形式；Set 的 String() 按 key 排序输出，
// 因此期望值必须使用排序后的顺序。
func matches(t *testing.T, ls Set, want string) {
	if ls.String() != want {
		t.Errorf("Expected '%s', but got '%s'", want, ls.String())
	}
}

// TestSetString 确认 String() 的输出是稳定、可比较的 key=value 列表。
func TestSetString(t *testing.T) {
	matches(t, Set{"x": "y"}, "x=y")
	matches(t, Set{"foo": "bar"}, "foo=bar")
	// 多字段时按 key 字典序，而不是插入顺序。
	matches(t, Set{"foo": "bar", "baz": "qup"}, "baz=qup,foo=bar")
}

// TestFieldHas 区分“key 存在但值为空”与“key 不存在”：Has 只看 key。
func TestFieldHas(t *testing.T) {
	fieldHasTests := []struct {
		Ls  Fields
		Key string
		Has bool
	}{
		{Set{"x": "y"}, "x", true},
		{Set{"x": ""}, "x", true},
		{Set{"x": "y"}, "foo", false},
	}
	for _, lh := range fieldHasTests {
		if has := lh.Ls.Has(lh.Key); has != lh.Has {
			t.Errorf("%#v.Has(%#v) => %v, expected %v", lh.Ls, lh.Key, has, lh.Has)
		}
	}
}

// TestFieldGet 确认 Get 返回对应 key 的值。
func TestFieldGet(t *testing.T) {
	ls := Set{"x": "y"}
	if ls.Get("x") != "y" {
		t.Errorf("Set.Get is broken")
	}
}
