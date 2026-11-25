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

// TestStringLangleSeparatedMapStringString 验证 String() 输出为逗号分隔的 key<value，
// 且 nil map 与空 map 都输出空串。
func TestStringLangleSeparatedMapStringString(t *testing.T) {
	var nilMap map[string]string
	cases := []struct {
		desc   string
		m      *LangleSeparatedMapStringString
		expect string
	}{
		{"nil", NewLangleSeparatedMapStringString(&nilMap), ""},
		{"empty", NewLangleSeparatedMapStringString(&map[string]string{}), ""},
		{"one key", NewLangleSeparatedMapStringString(&map[string]string{"one": "foo"}), "one<foo"},
		{"two keys", NewLangleSeparatedMapStringString(&map[string]string{"one": "foo", "two": "bar"}), "one<foo,two<bar"},
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

// TestSetLangleSeparatedMapStringString 覆盖 Set 的累积与默认值清除语义：
// 首次设置丢弃默认值、多次调用累加、容忍空格与尾随逗号，并校验错误信息。
func TestSetLangleSeparatedMapStringString(t *testing.T) {
	var nilMap map[string]string
	cases := []struct {
		desc   string
		vals   []string
		start  *LangleSeparatedMapStringString
		expect *LangleSeparatedMapStringString
		err    string
	}{
		// 初始化一个默认键，Set 应将其清除。
		{"clears defaults", []string{""},
			NewLangleSeparatedMapStringString(&map[string]string{"default": ""}),
			&LangleSeparatedMapStringString{
				initialized: true,
				Map:         &map[string]string{},
			}, ""},
		// 确保 Map 初始为 nil map 时仍能分配空间。
		{"allocates map if currently nil", []string{""},
			&LangleSeparatedMapStringString{initialized: true, Map: &nilMap},
			&LangleSeparatedMapStringString{
				initialized: true,
				Map:         &map[string]string{},
			}, ""},
		// 多数用例复用 nilMap，由 Set 分配空间，并在每个用例前重置。
		{"empty", []string{""},
			NewLangleSeparatedMapStringString(&nilMap),
			&LangleSeparatedMapStringString{
				initialized: true,
				Map:         &map[string]string{},
			}, ""},
		{"one key", []string{"one<foo"},
			NewLangleSeparatedMapStringString(&nilMap),
			&LangleSeparatedMapStringString{
				initialized: true,
				Map:         &map[string]string{"one": "foo"},
			}, ""},
		{"two keys", []string{"one<foo,two<bar"},
			NewLangleSeparatedMapStringString(&nilMap),
			&LangleSeparatedMapStringString{
				initialized: true,
				Map:         &map[string]string{"one": "foo", "two": "bar"},
			}, ""},
		{"two keys, multiple Set invocations", []string{"one<foo", "two<bar"},
			NewLangleSeparatedMapStringString(&nilMap),
			&LangleSeparatedMapStringString{
				initialized: true,
				Map:         &map[string]string{"one": "foo", "two": "bar"},
			}, ""},
		{"two keys with space", []string{"one<foo, two<bar"},
			NewLangleSeparatedMapStringString(&nilMap),
			&LangleSeparatedMapStringString{
				initialized: true,
				Map:         &map[string]string{"one": "foo", "two": "bar"},
			}, ""},
		{"empty key", []string{"<foo"},
			NewLangleSeparatedMapStringString(&nilMap),
			&LangleSeparatedMapStringString{
				initialized: true,
				Map:         &map[string]string{"": "foo"},
			}, ""},
		{"missing value", []string{"one"},
			NewLangleSeparatedMapStringString(&nilMap),
			nil,
			"malformed pair, expect string<string"},
		{"no target", []string{"a:foo"},
			NewLangleSeparatedMapStringString(nil),
			nil,
			"no target (nil pointer to map[string]string)"},
	}
	for _, c := range cases {
		// 每个用例复用同一个 nilMap，先复位避免用例间串数据。
		nilMap = nil
		t.Run(c.desc, func(t *testing.T) {
			var err error
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

// TestEmptyLangleSeparatedMapStringString 验证 Empty() 对 nil 与空 map 返回 true。
func TestEmptyLangleSeparatedMapStringString(t *testing.T) {
	var nilMap map[string]string
	cases := []struct {
		desc   string
		val    *LangleSeparatedMapStringString
		expect bool
	}{
		{"nil", NewLangleSeparatedMapStringString(&nilMap), true},
		{"empty", NewLangleSeparatedMapStringString(&map[string]string{}), true},
		{"populated", NewLangleSeparatedMapStringString(&map[string]string{"foo": ""}), false},
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
