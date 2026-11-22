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

// TestStringMapStringBool 验证 MapStringBool 的 String() 输出：
// nil 与空 map 都渲染为空串，多项按排序后的 "k=true" 连接。
func TestStringMapStringBool(t *testing.T) {
	var nilMap map[string]bool
	cases := []struct {
		desc   string
		m      *MapStringBool
		expect string
	}{
		{"nil", NewMapStringBool(&nilMap), ""},
		{"empty", NewMapStringBool(&map[string]bool{}), ""},
		{"one key", NewMapStringBool(&map[string]bool{"one": true}), "one=true"},
		{"two keys", NewMapStringBool(&map[string]bool{"one": true, "two": false}), "one=true,two=false"},
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

// TestSetMapStringBool 验证 Set 的解析语义：空输入清空、重复 Set 累加、
// 非法布尔值和缺失 "=" 会返回可定位的错误。
func TestSetMapStringBool(t *testing.T) {
	var nilMap map[string]bool
	cases := []struct {
		desc   string
		vals   []string
		start  *MapStringBool
		expect *MapStringBool
		err    string
	}{
		// 用默认键初始化 map，Set 应将其清除。
		{"clears defaults", []string{""},
			NewMapStringBool(&map[string]bool{"default": true}),
			&MapStringBool{
				initialized: true,
				Map:         &map[string]bool{},
			}, ""},
		// 已初始化的对象即使底层 map 为 nil，也要分配后再写入。
		{"allocates map if currently nil", []string{""},
			&MapStringBool{initialized: true, Map: &nilMap},
			&MapStringBool{
				initialized: true,
				Map:         &map[string]bool{},
			}, ""},
		// 大多数用例复用 nilMap，由 Set 分配，并在每个用例前重置。
		{"empty", []string{""},
			NewMapStringBool(&nilMap),
			&MapStringBool{
				initialized: true,
				Map:         &map[string]bool{},
			}, ""},
		{"one key", []string{"one=true"},
			NewMapStringBool(&nilMap),
			&MapStringBool{
				initialized: true,
				Map:         &map[string]bool{"one": true},
			}, ""},
		{"two keys", []string{"one=true,two=false"},
			NewMapStringBool(&nilMap),
			&MapStringBool{
				initialized: true,
				Map:         &map[string]bool{"one": true, "two": false},
			}, ""},
		// 多次 Set 调用应累加而不是覆盖。
		{"two keys, multiple Set invocations", []string{"one=true", "two=false"},
			NewMapStringBool(&nilMap),
			&MapStringBool{
				initialized: true,
				Map:         &map[string]bool{"one": true, "two": false},
			}, ""},
		// 逗号后的空格应被裁剪掉。
		{"two keys with space", []string{"one=true, two=false"},
			NewMapStringBool(&nilMap),
			&MapStringBool{
				initialized: true,
				Map:         &map[string]bool{"one": true, "two": false},
			}, ""},
		{"empty key", []string{"=true"},
			NewMapStringBool(&nilMap),
			&MapStringBool{
				initialized: true,
				Map:         &map[string]bool{"": true},
			}, ""},
		{"missing value", []string{"one"},
			NewMapStringBool(&nilMap),
			nil,
			"malformed pair, expect string=bool"},
		{"non-boolean value", []string{"one=foo"},
			NewMapStringBool(&nilMap),
			nil,
			`invalid value of one: foo, err: strconv.ParseBool: parsing "foo": invalid syntax`},
		{"no target", []string{"one=true"},
			NewMapStringBool(nil),
			nil,
			"no target (nil pointer to map[string]bool)"},
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

// TestEmptyMapStringBool 验证 Empty()：nil 或空 map 视为空，任意键存在即非空。
func TestEmptyMapStringBool(t *testing.T) {
	var nilMap map[string]bool
	cases := []struct {
		desc   string
		val    *MapStringBool
		expect bool
	}{
		{"nil", NewMapStringBool(&nilMap), true},
		{"empty", NewMapStringBool(&map[string]bool{}), true},
		{"populated", NewMapStringBool(&map[string]bool{"foo": true}), false},
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
