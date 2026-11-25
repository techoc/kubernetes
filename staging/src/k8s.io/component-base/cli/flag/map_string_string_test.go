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

// TestStringMapStringString 验证 String() 的稳定输出：键排序、
// nil 与空 map 都输出空串。
func TestStringMapStringString(t *testing.T) {
	var nilMap map[string]string
	cases := []struct {
		desc   string
		m      *MapStringString
		expect string
	}{
		{"nil", NewMapStringString(&nilMap), ""},
		{"empty", NewMapStringString(&map[string]string{}), ""},
		{"one key", NewMapStringString(&map[string]string{"one": "foo"}), "one=foo"},
		{"two keys", NewMapStringString(&map[string]string{"one": "foo", "two": "bar"}), "one=foo,two=bar"},
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

// TestSetMapStringString 覆盖 Set 的边界：首次设置清除默认值、
// nil map 的惰性分配、NoSplit 下逗号作为普通字符、空白与错误输入。
func TestSetMapStringString(t *testing.T) {
	var nilMap map[string]string
	cases := []struct {
		desc   string
		vals   []string
		start  *MapStringString
		expect *MapStringString
		err    string
	}{
		// 初始化一个默认键，Set 应将其清除。
		{"clears defaults", []string{""},
			NewMapStringString(&map[string]string{"default": ""}),
			&MapStringString{
				initialized: true,
				Map:         &map[string]string{},
				NoSplit:     false,
			}, ""},
		// 确保 Map 初始为 nil map 时仍能分配空间。
		{"allocates map if currently nil", []string{""},
			&MapStringString{initialized: true, Map: &nilMap},
			&MapStringString{
				initialized: true,
				Map:         &map[string]string{},
				NoSplit:     false,
			}, ""},
		// 多数用例复用 nilMap，由 Set 分配空间，并在每个用例前重置。
		{"empty", []string{""},
			NewMapStringString(&nilMap),
			&MapStringString{
				initialized: true,
				Map:         &map[string]string{},
				NoSplit:     false,
			}, ""},
		{"one key", []string{"one=foo"},
			NewMapStringString(&nilMap),
			&MapStringString{
				initialized: true,
				Map:         &map[string]string{"one": "foo"},
				NoSplit:     false,
			}, ""},
		{"two keys", []string{"one=foo,two=bar"},
			NewMapStringString(&nilMap),
			&MapStringString{
				initialized: true,
				Map:         &map[string]string{"one": "foo", "two": "bar"},
				NoSplit:     false,
			}, ""},
		{"one key, multi flag invocation only", []string{"one=foo,bar"},
			NewMapStringStringNoSplit(&nilMap),
			&MapStringString{
				initialized: true,
				Map:         &map[string]string{"one": "foo,bar"},
				NoSplit:     true,
			}, ""},
		{"two keys, multi flag invocation only", []string{"one=foo,bar", "two=foo,bar"},
			NewMapStringStringNoSplit(&nilMap),
			&MapStringString{
				initialized: true,
				Map:         &map[string]string{"one": "foo,bar", "two": "foo,bar"},
				NoSplit:     true,
			}, ""},
		{"two keys, multiple Set invocations", []string{"one=foo", "two=bar"},
			NewMapStringString(&nilMap),
			&MapStringString{
				initialized: true,
				Map:         &map[string]string{"one": "foo", "two": "bar"},
				NoSplit:     false,
			}, ""},
		{"two keys with space", []string{"one=foo, two=bar"},
			NewMapStringString(&nilMap),
			&MapStringString{
				initialized: true,
				Map:         &map[string]string{"one": "foo", "two": "bar"},
				NoSplit:     false,
			}, ""},
		{"empty key", []string{"=foo"},
			NewMapStringString(&nilMap),
			&MapStringString{
				initialized: true,
				Map:         &map[string]string{"": "foo"},
				NoSplit:     false,
			}, ""},
		{"missing value", []string{"one"},
			NewMapStringString(&nilMap),
			nil,
			"malformed pair, expect string=string"},
		{"no target", []string{"a:foo"},
			NewMapStringString(nil),
			nil,
			"no target (nil pointer to map[string]string)"},
	}
	for _, c := range cases {
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

// TestEmptyMapStringString 验证 Empty() 只取决于 map 长度，
// nil 与空 map 都视为空。
func TestEmptyMapStringString(t *testing.T) {
	var nilMap map[string]string
	cases := []struct {
		desc   string
		val    *MapStringString
		expect bool
	}{
		{"nil", NewMapStringString(&nilMap), true},
		{"empty", NewMapStringString(&map[string]string{}), true},
		{"populated", NewMapStringString(&map[string]string{"foo": ""}), false},
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
