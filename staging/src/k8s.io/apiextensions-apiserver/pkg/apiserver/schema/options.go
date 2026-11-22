/*
Copyright 2022 The Kubernetes Authors.

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

// Package schema 定义结构化 schema 模型，并实现剪枝、默认值填充与校验等能力。
package schema

import (
	"strconv"
	"strings"
)

// UnknownFieldPathOptions 用于在遍历过程中记录未知字段的路径。
type UnknownFieldPathOptions struct {
	// TrackUnknownFieldPaths 决定是否记录未知字段路径。
	TrackUnknownFieldPaths bool
	// ParentPath 在递归遍历对象时逐步构建出通往未知字段的路径。
	ParentPath []string
	// UnknownFieldPaths 保存所有已识别出的未知字段路径。
	UnknownFieldPaths []string
}

// RecordUnknownField 在 TrackUnknownFieldPaths 为 true 时，
// 将某个未知字段的路径追加到 UnknownFieldPaths。
func (o *UnknownFieldPathOptions) RecordUnknownField(field string) {
	if !o.TrackUnknownFieldPaths {
		return
	}
	// 先把字段拼进当前路径，记录完整路径后再回退，保持父路径不变。
	l := len(o.ParentPath)
	o.AppendKey(field)
	o.UnknownFieldPaths = append(o.UnknownFieldPaths, strings.Join(o.ParentPath, ""))
	o.ParentPath = o.ParentPath[:l]
}

// AppendKey 在 TrackUnknownFieldPaths 为 true 时，将键（即字段名）
// 追加到当前父路径末尾。
func (o *UnknownFieldPathOptions) AppendKey(key string) {
	if !o.TrackUnknownFieldPaths {
		return
	}
	// 非首层字段前补 "."，拼接出 obj.field 形式的路径。
	if len(o.ParentPath) > 0 {
		o.ParentPath = append(o.ParentPath, ".")
	}
	o.ParentPath = append(o.ParentPath, key)
}

// AppendIndex 在 TrackUnknownFieldPaths 为 true 时，为当前父路径中
// 最近的一个字段追加数组下标。
func (o *UnknownFieldPathOptions) AppendIndex(index int) {
	if !o.TrackUnknownFieldPaths {
		return
	}
	o.ParentPath = append(o.ParentPath, "[", strconv.Itoa(index), "]")
}
