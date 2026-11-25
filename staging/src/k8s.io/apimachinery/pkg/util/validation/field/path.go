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

// Package field 定义 API 字段校验使用的字段路径与错误类型，
// 便于把校验失败定位到具体字段并以结构化形式返回。
package field

import (
	"bytes"
	"fmt"
	"strconv"
)

// pathOptions 收集可选参数，供 ToPath 构造 Path。
type pathOptions struct {
	path *Path
}

// PathOption 修改 pathOptions 的函数式选项。
type PathOption func(o *pathOptions)

// WithPath 生成一个把 path 写入选项的 PathOption。
func WithPath(p *Path) PathOption {
	return func(o *pathOptions) {
		o.path = p
	}
}

// ToPath 根据一组 PathOption 生成 *Path。
func ToPath(opts ...PathOption) *Path {
	c := &pathOptions{}
	for _, opt := range opts {
		opt(c)
	}
	return c.path
}

// Path 表示从某个根到特定字段的路径。
type Path struct {
	name   string //字段名；如果是索引则为 ""。
	index  string //name 为 "" 时，表示前一个元素的下标（索引或映射 key）。
	parent *Path  //根元素为 nil。
}

// NewPath 创建根 Path，后续名称通过链式挂到父节点上。
func NewPath(name string, moreNames ...string) *Path {
	r := &Path{name: name, parent: nil}
	for _, anotherName := range moreNames {
		r = &Path{name: anotherName, parent: r}
	}
	return r
}

// Root 沿 parent 链返回路径的根元素。
func (p *Path) Root() *Path {
	for ; p.parent != nil; p = p.parent {
		//不处理。
	}
	return p
}

// Child 创建以当前路径为父节点的新路径。
func (p *Path) Child(name string, moreNames ...string) *Path {
	r := NewPath(name, moreNames...)
	r.Root().parent = p
	return r
}

// Index 表示对前一个路径按整数下标取值。
// 与 Key 共用同一底层字段。
func (p *Path) Index(index int) *Path {
	return &Path{index: strconv.Itoa(index), parent: p}
}

// Key 表示对前一个路径按字符串键取值。
// 与 Index 共用同一底层字段。
func (p *Path) Key(key string) *Path {
	return &Path{index: key, parent: p}
}

// String 生成路径的字符串表示。
func (p *Path) String() string {
	if p == nil {
		return "<nil>"
	}
	//创建用于遍历的切片。
	// 先收集节点再反向输出：parent 链是从叶子指向根。
	elems := []*Path{}
	for ; p != nil; p = p.parent {
		elems = append(elems, p)
	}

	//必须反向遍历。
	buf := bytes.NewBuffer(nil)
	for i := range elems {
		p := elems[len(elems)-1-i]
		if p.parent != nil && len(p.name) > 0 {
			//这是根元素或下标。
			// 非根且带名字的节点前要加点号，形成 "a.b" 这样可读的路径。
			buf.WriteString(".")
		}
		if len(p.name) > 0 {
			buf.WriteString(p.name)
		} else {
			// 下标节点输出为 "[i]" 或 "[key]"。
			fmt.Fprintf(buf, "[%s]", p.index)
		}
	}
	return buf.String()
}
