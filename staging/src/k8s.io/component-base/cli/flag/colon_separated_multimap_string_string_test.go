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

package flag

import (
	"reflect"
	"testing"
)

// TestStringColonSeparatedMultimapStringString 验证 String() 的输出：
// 每个键值对展开成 "key:value"，多个值重复键，且排序保证结果稳定。
func TestStringColonSeparatedMultimapStringString(t *testing.T) {
	var nilMap map[string][]string
	cases := []struct {
		desc   string
		m      *ColonSeparatedMultimapStringString
		expect string
	}{
		{"nil", NewColonSeparatedMultimapStringString(&nilMap), ""},
		{"empty", NewColonSeparatedMultimapStringString(&map[string][]string{}), ""},
		// 空键合法，序列化后表现为以 ":" 开头。
		{"empty key", NewColonSeparatedMultimapStringString(
			&map[string][]string{
				"": {"foo"},
			}),
			":foo"},
		{"one key", NewColonSeparatedMultimapStringString(
			&map[string][]string{
				"one": {"foo"},
			}),
			"one:foo"},
		{"two keys", NewColonSeparatedMultimapStringString(
			&map[string][]string{
				"one": {"foo"},
				"two": {"bar"},
			}),
			"one:foo,two:bar"},
		// 同一个键的多个值会被拆成多个键值对，并按值排序。
		{"two keys, multiple items in one key", NewColonSeparatedMultimapStringString(
			&map[string][]string{
				"one": {"foo", "baz"},
				"two": {"bar"},
			}),
			"one:foo,one:baz,two:bar"},
		{"three keys, multiple items in one key", NewColonSeparatedMultimapStringString(
			&map[string][]string{
				"a": {"hello"},
				"b": {"again", "beautiful"},
				"c": {"world"},
			}),
			"a:hello,b:again,b:beautiful,c:world"},
	}
	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			str := c.m.String()
			if c.expect != str {
				t.Fatalf("expect %q but got %q", c.expect, str)
			}
		})
	}
}

// TestSetColonSeparatedMultimapStringString 验证 Set 的解析、合并与错误路径：
// 空串表示清空、缺冒号在默认配置下报错、多次 Set 会累加到同一 map。
func TestSetColonSeparatedMultimapStringString(t *testing.T) {
	var nilMap map[string][]string
	cases := []struct {
		desc   string
		vals   []string
		start  *ColonSeparatedMultimapStringString
		expect *ColonSeparatedMultimapStringString
		err    string
	}{
		// 用默认键初始化 map，Set 应将其清除。
		{"clears defaults", []string{""},
			NewColonSeparatedMultimapStringString(&map[string][]string{"default": {}}),
			&ColonSeparatedMultimapStringString{
				initialized: true,
				Multimap:    &map[string][]string{}}, ""},
		// 即使 Map 初始为 nil，已初始化的对象也要为其分配底层 map。
		{"allocates map if currently nil", []string{""},
			&ColonSeparatedMultimapStringString{initialized: true, Multimap: &nilMap},
			&ColonSeparatedMultimapStringString{
				initialized: true,
				Multimap:    &map[string][]string{},
			}, ""},
		// 大多数用例复用 nilMap，由 Set 分配，并在每个用例前重置。
		{"empty", []string{""},
			NewColonSeparatedMultimapStringString(&nilMap),
			&ColonSeparatedMultimapStringString{
				initialized: true,
				Multimap:    &map[string][]string{}}, ""},
		// 默认不允许空键，缺冒号即视为格式错误。
		{"empty key no colon", []string{"foo"},
			NewColonSeparatedMultimapStringString(&nilMap),
			&ColonSeparatedMultimapStringString{
				initialized: true,
				Multimap: &map[string][]string{
					"": {"foo"},
				}}, "malformed pair, expect string:string"},
		// AllowDefaultEmptyKey 模式下没有冒号的值归入空键，而不是报错。
		{"empty key no colon allowed", []string{"foo"},
			NewColonSeparatedMultimapStringStringAllowDefaultEmptyKey(&nilMap),
			&ColonSeparatedMultimapStringString{
				initialized:          true,
				allowDefaultEmptyKey: true,
				Multimap: &map[string][]string{
					"": {"foo"},
				}}, ""},
		{"empty key", []string{":foo"},
			NewColonSeparatedMultimapStringString(&nilMap),
			&ColonSeparatedMultimapStringString{
				initialized: true,
				Multimap: &map[string][]string{
					"": {"foo"},
				}}, ""},
		{"one key", []string{"one:foo"},
			NewColonSeparatedMultimapStringString(&nilMap),
			&ColonSeparatedMultimapStringString{
				initialized: true,
				Multimap: &map[string][]string{
					"one": {"foo"},
				}}, ""},
		{"two keys", []string{"one:foo,two:bar"},
			NewColonSeparatedMultimapStringString(&nilMap),
			&ColonSeparatedMultimapStringString{
				initialized: true,
				Multimap: &map[string][]string{
					"one": {"foo"},
					"two": {"bar"},
				}}, ""},
		// 逗号后的空格应被裁剪掉。
		{"two keys with space", []string{"one:foo, two:bar"},
			NewColonSeparatedMultimapStringString(&nilMap),
			&ColonSeparatedMultimapStringString{
				initialized: true,
				Multimap: &map[string][]string{
					"one": {"foo"},
					"two": {"bar"},
				}}, ""},
		{"two keys, multiple items in one key", []string{"one: foo, two:bar, one:baz"},
			NewColonSeparatedMultimapStringString(&nilMap),
			&ColonSeparatedMultimapStringString{
				initialized: true,
				Multimap: &map[string][]string{
					"one": {"foo", "baz"},
					"two": {"bar"},
				}}, ""},
		{"three keys, multiple items in one key", []string{"a:hello,b:again,c:world,b:beautiful"},
			NewColonSeparatedMultimapStringString(&nilMap),
			&ColonSeparatedMultimapStringString{
				initialized: true,
				Multimap: &map[string][]string{
					"a": {"hello"},
					"b": {"again", "beautiful"},
					"c": {"world"},
				}}, ""},
		// 多次 Set 调用应累加而不是覆盖。
		{"three keys, multiple items in one key, multiple Set invocations", []string{"a:hello,b:again", "c:world", "b:beautiful"},
			NewColonSeparatedMultimapStringString(&nilMap),
			&ColonSeparatedMultimapStringString{
				initialized: true,
				Multimap: &map[string][]string{
					"a": {"hello"},
					"b": {"again", "beautiful"},
					"c": {"world"},
				}}, ""},
		{"missing value", []string{"a"},
			NewColonSeparatedMultimapStringString(&nilMap),
			nil,
			"malformed pair, expect string:string"},
		{"no target", []string{"a:foo"},
			NewColonSeparatedMultimapStringString(nil),
			nil,
			"no target (nil pointer to map[string][]string)"},
	}

	for _, c := range cases {
		nilMap = nil
		t.Run(c.desc, func(t *testing.T) {
			var err error
			// 依次应用所有输入，遇到错误即停止。
			for _, val := range c.vals {
				err = c.start.Set(val)
				if err != nil {
					break
				}
			}
			if c.err != "" {
				if err == nil || err.Error() != c.err {
					t.Fatalf("expect error %s but got %v", c.err, err)
				}
				return
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(c.expect, c.start) {
				t.Fatalf("expect %#v but got %#v", c.expect, c.start)
			}
		})
	}
}

// TestRoundTripColonSeparatedMultimapStringString 验证 Set 后再 String 的往返：
// 无论输入如何分组，输出都应归一化为按键、值排序后的规范形式。
func TestRoundTripColonSeparatedMultimapStringString(t *testing.T) {
	cases := []struct {
		desc   string
		vals   []string
		expect string
	}{
		{"empty", []string{""}, ""},
		{"empty key", []string{":foo"}, ":foo"},
		{"one key", []string{"one:foo"}, "one:foo"},
		{"two keys", []string{"one:foo,two:bar"}, "one:foo,two:bar"},
		{"two keys, multiple items in one key", []string{"one:foo, two:bar, one:baz"}, "one:foo,one:baz,two:bar"},
		{"three keys, multiple items in one key", []string{"a:hello,b:again,c:world,b:beautiful"}, "a:hello,b:again,b:beautiful,c:world"},
		{"three keys, multiple items in one key, multiple Set invocations", []string{"a:hello,b:again", "c:world", "b:beautiful"}, "a:hello,b:again,b:beautiful,c:world"},
	}

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			m := NewColonSeparatedMultimapStringString(&map[string][]string{})
			for _, val := range c.vals {
				if err := m.Set(val); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}
			str := m.String()
			if c.expect != str {
				t.Fatalf("expect %q but got %q", c.expect, str)
			}
		})
	}
}

// TestEmptyColonSeparatedMultimapStringString 验证 Empty()：未初始化或没有
// 任何键时返回 true，存在键（即使值为空列表）时返回 false。
func TestEmptyColonSeparatedMultimapStringString(t *testing.T) {
	var nilMap map[string][]string
	cases := []struct {
		desc   string
		val    *ColonSeparatedMultimapStringString
		expect bool
	}{
		{"nil", NewColonSeparatedMultimapStringString(&nilMap), true},
		{"empty", NewColonSeparatedMultimapStringString(&map[string][]string{}), true},
		{"populated", NewColonSeparatedMultimapStringString(&map[string][]string{"foo": {}}), false},
	}
	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			result := c.val.Empty()
			if result != c.expect {
				t.Fatalf("expect %t but got %t", c.expect, result)
			}
		})
	}
}
