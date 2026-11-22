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

package field

import "testing"

// TestPath 验证 Path 的拼接、索引、键访问与回溯行为。
//
// 用例按顺序作用在同一个 Path 上：op 接收当前路径并返回下一步的路径，
// 因此每一步都在上一步结果的基础上构建，模拟校验错误路径的逐层下钻。
// 同时断言 Root 始终指向最初创建的根节点，确保路径链不会在派生过程中断裂。
func TestPath(t *testing.T) {
	testCases := []struct {
		op       func(*Path) *Path
		expected string
	}{
		{
			// 初始路径。
			func(p *Path) *Path { return p },
			"root",
		},
		{
			func(p *Path) *Path { return p.Child("first") },
			"root.first",
		},
		{
			// Child 返回新路径而不修改原路径，因此每次派生都从上一轮的结果继续。
			func(p *Path) *Path { return p.Child("second") },
			"root.first.second",
		},
		{
			func(p *Path) *Path { return p.Index(0) },
			"root.first.second[0]",
		},
		{
			func(p *Path) *Path { return p.Child("third") },
			"root.first.second[0].third",
		},
		{
			// 大索引值用于确认格式化不会截断或使用非十进制表示。
			func(p *Path) *Path { return p.Index(93) },
			"root.first.second[0].third[93]",
		},
		{
			// parent 回溯一级，跳过索引段。
			func(p *Path) *Path { return p.parent },
			"root.first.second[0].third",
		},
		{
			func(p *Path) *Path { return p.parent },
			"root.first.second[0]",
		},
		{
			// 键段使用方括号包裹，与索引段形式相近但语义不同。
			func(p *Path) *Path { return p.Key("key") },
			"root.first.second[0][key]",
		},
	}

	root := NewPath("root")
	p := root
	for i, tc := range testCases {
		p = tc.op(p)
		if p.String() != tc.expected {
			t.Errorf("[%d] Expected %q, got %q", i, tc.expected, p.String())
		}
		if p.Root() != root {
			t.Errorf("[%d] Wrong root: %#v", i, p.Root())
		}
	}
}

// TestPathMultiArg 验证 NewPath 与 Child 支持多段名称，一次拼接多个层级。
//
// 多段形式可减少中间路径对象的分配，且保证这些层级共享同一个根节点。
func TestPathMultiArg(t *testing.T) {
	testCases := []struct {
		op       func(*Path) *Path
		expected string
	}{
		{
			func(p *Path) *Path { return p },
			"root.first",
		},
		{
			// 一次追加两段，等价于连续调用两次 Child。
			func(p *Path) *Path { return p.Child("second", "third") },
			"root.first.second.third",
		},
		{
			func(p *Path) *Path { return p.Index(0) },
			"root.first.second.third[0]",
		},
		{
			func(p *Path) *Path { return p.parent },
			"root.first.second.third",
		},
		{
			func(p *Path) *Path { return p.parent },
			"root.first.second",
		},
		{
			func(p *Path) *Path { return p.parent },
			"root.first",
		},
		{
			// 回溯到根：多段名被拆分为独立的路径节点，需逐级回退。
			func(p *Path) *Path { return p.parent },
			"root",
		},
	}

	root := NewPath("root", "first")
	p := root
	for i, tc := range testCases {
		p = tc.op(p)
		if p.String() != tc.expected {
			t.Errorf("[%d] Expected %q, got %q", i, tc.expected, p.String())
		}
		if p.Root() != root.Root() {
			t.Errorf("[%d] Wrong root: %#v", i, p.Root())
		}
	}
}
