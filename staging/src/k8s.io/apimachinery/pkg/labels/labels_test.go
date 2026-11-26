/*
Copyright 2014 The Kubernetes Authors.

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

package labels

import (
	"testing"
)

// matches 断言 Labels 的排序后字符串形式。
func matches(t *testing.T, ls Set, want string) {
	if ls.String() != want {
		t.Errorf("Expected '%s', but got '%s'", want, ls.String())
	}
}

// TestSetString 确认输出按 key 排序；注意目前无法无歧义地表示名字中含
// ",=!" 的 label，见下方待办注释。
func TestSetString(t *testing.T) {
	matches(t, Set{"x": "y"}, "x=y")
	matches(t, Set{"foo": "bar"}, "foo=bar")
	matches(t, Set{"foo": "bar", "baz": "qup"}, "baz=qup,foo=bar")

	// TODO: 让 label 表示法足够健壮，支持名称中包含 ",=!" 的 label。
}

// TestLabelHas 只判断 key 是否存在，空值也算存在。
func TestLabelHas(t *testing.T) {
	labelHasTests := []struct {
		Ls  Labels
		Key string
		Has bool
	}{
		{Set{"x": "y"}, "x", true},
		{Set{"x": ""}, "x", true},
		{Set{"x": "y"}, "foo", false},
	}
	for _, lh := range labelHasTests {
		if has := lh.Ls.Has(lh.Key); has != lh.Has {
			t.Errorf("%#v.Has(%#v) => %v, expected %v", lh.Ls, lh.Key, has, lh.Has)
		}
	}
}

// TestLabelGet 确认按 key 取值。
func TestLabelGet(t *testing.T) {
	ls := Set{"x": "y"}
	if ls.Get("x") != "y" {
		t.Errorf("Set.Get is broken")
	}
}

// TestLabelConflict 定义冲突语义：仅当同一个 key 在两边都存在且取值不同时
// 才算冲突；一方缺失或取值相同都不冲突。
func TestLabelConflict(t *testing.T) {
	tests := []struct {
		labels1  map[string]string
		labels2  map[string]string
		conflict bool
	}{
		{
			labels1:  map[string]string{},
			labels2:  map[string]string{},
			conflict: false,
		},
		{
			labels1:  map[string]string{"env": "test"},
			labels2:  map[string]string{"infra": "true"},
			conflict: false,
		},
		{
			labels1:  map[string]string{"env": "test"},
			labels2:  map[string]string{"infra": "true", "env": "test"},
			conflict: false,
		},
		{
			labels1:  map[string]string{"env": "test"},
			labels2:  map[string]string{"env": "dev"},
			conflict: true,
		},
		{
			labels1:  map[string]string{"env": "test", "infra": "false"},
			labels2:  map[string]string{"infra": "true", "color": "blue"},
			conflict: true,
		},
	}
	for _, test := range tests {
		conflict := Conflicts(Set(test.labels1), Set(test.labels2))
		if conflict != test.conflict {
			t.Errorf("expected: %v but got: %v", test.conflict, conflict)
		}
	}
}

func TestLabelMerge(t *testing.T) {
	tests := []struct {
		labels1      map[string]string
		labels2      map[string]string
		mergedLabels map[string]string
	}{
		{
			labels1:      map[string]string{},
			labels2:      map[string]string{},
			mergedLabels: map[string]string{},
		},
		{
			labels1:      map[string]string{"infra": "true"},
			labels2:      map[string]string{},
			mergedLabels: map[string]string{"infra": "true"},
		},
		{
			labels1:      map[string]string{"infra": "true"},
			labels2:      map[string]string{"env": "test", "color": "blue"},
			mergedLabels: map[string]string{"infra": "true", "env": "test", "color": "blue"},
		},
	}
	// TestLabelMerge 确认 Merge 是两个集合的并集；同名 key 在 labels2 中会
	// 覆盖 labels1（由 Merge 的实现决定，测试仅验证无重叠场景）。
	for _, test := range tests {
		mergedLabels := Merge(Set(test.labels1), Set(test.labels2))
		if !Equals(mergedLabels, test.mergedLabels) {
			t.Errorf("expected: %v but got: %v", test.mergedLabels, mergedLabels)
		}
	}
}

// TestLabelSelectorParse 覆盖“等值选择器 -> 标签 map”的解析；仅支持
// key=value 形式（逗号分隔），集合/不等运算与空格、非法字符都应报错。
func TestLabelSelectorParse(t *testing.T) {
	tests := []struct {
		selector string
		labels   map[string]string
		valid    bool
	}{
		// 空选择器合法，表示匹配所有标签。
		{
			selector: "",
			labels:   map[string]string{},
			valid:    true,
		},
		{
			selector: "x=a",
			labels:   map[string]string{"x": "a"},
			valid:    true,
		},
		{
			selector: "x=a,y=b,z=c",
			labels:   map[string]string{"x": "a", "y": "b", "z": "c"},
			valid:    true,
		},
		{
			selector: " x = a , y = b , z = c ",
			labels:   map[string]string{"x": "a", "y": "b", "z": "c"},
			valid:    true,
		},
		{
			selector: "color=green,env=test,service=front",
			labels:   map[string]string{"color": "green", "env": "test", "service": "front"},
			valid:    true,
		},
		{
			selector: "color=green, env=test, service=front",
			labels:   map[string]string{"color": "green", "env": "test", "service": "front"},
			valid:    true,
		},
		// 以下均为非法输入：空项、缺值、非法字符、集合运算和不等运算。
		{
			selector: ",",
			labels:   map[string]string{},
			valid:    false,
		},
		{
			selector: "x",
			labels:   map[string]string{},
			valid:    false,
		},
		{
			selector: "x,y",
			labels:   map[string]string{},
			valid:    false,
		},
		{
			selector: "x=$y",
			labels:   map[string]string{},
			valid:    false,
		},
		{
			selector: "x!=y",
			labels:   map[string]string{},
			valid:    false,
		},
		{
			selector: "x==y",
			labels:   map[string]string{},
			valid:    false,
		},
		{
			selector: "x=a||y=b",
			labels:   map[string]string{},
			valid:    false,
		},
		{
			selector: "x in (y)",
			labels:   map[string]string{},
			valid:    false,
		},
		{
			selector: "x notin (y)",
			labels:   map[string]string{},
			valid:    false,
		},
		{
			selector: "x y",
			labels:   map[string]string{},
			valid:    false,
		},
	}
	for _, test := range tests {
		labels, err := ConvertSelectorToLabelsMap(test.selector)
		if test.valid && err != nil {
			t.Errorf("selector: %s, expected no error but got: %s", test.selector, err)
		} else if !test.valid && err == nil {
			t.Errorf("selector: %s, expected an error", test.selector)
		}

		if !Equals(Set(labels), test.labels) {
			t.Errorf("expected: %s but got: %s", test.labels, labels)
		}
	}
}
