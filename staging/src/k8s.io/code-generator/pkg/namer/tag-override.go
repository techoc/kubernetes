/*
Copyright 2016 The Kubernetes Authors.

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

// Package namer 提供基于注释标签覆盖默认命名规则的 namer 实现。
package namer

import (
	"k8s.io/gengo/v2"
	"k8s.io/gengo/v2/namer"
	"k8s.io/gengo/v2/types"
)

// TagOverrideNamer 是一个 namer：若类型上指定了标签则取标签值，
// 否则回退到另一个 namer。这样生成器可以在个别类型上使用自定义名称，
// 同时保持默认命名规则不变。
type TagOverrideNamer struct {
	tagName  string
	fallback namer.Namer
}

// Name 返回标签值（若存在）。若未找到标签，则使用 fallback namer。
// Name returns the tag value if it exists. It no tag was found the fallback namer will be used
func (n *TagOverrideNamer) Name(t *types.Type) string {
	// SecondClosestCommentLines 与 CommentLines 都参与解析：标签可能写在
	// 类型声明紧邻注释或上方的类型注释块中。
	if nameOverride := extractTag(n.tagName, append(t.SecondClosestCommentLines, t.CommentLines...)); nameOverride != "" {
		return nameOverride
	}

	return n.fallback.Name(t)
}

// NewTagOverrideNamer 创建一个 namer.Namer：优先使用给定标签的内容作为名称，
// 标签不存在时回退到另一个 Namer。
// NewTagOverrideNamer creates a namer.Namer which uses the contents of the given tag as
// the name, or falls back to another Namer if the tag is not present.
func NewTagOverrideNamer(tagName string, fallback namer.Namer) namer.Namer {
	return &TagOverrideNamer{
		tagName:  tagName,
		fallback: fallback,
	}
}

// extractTag 获取指定 key 的注释标签；若标签不存在则返回空字符串。
// 同一标签出现多次时只取第一个值，与 gengo 的约定一致。
// extractTag gets the comment-tags for the key.  If the tag did not exist, it
// returns the empty string.
func extractTag(key string, lines []string) string {
	val, present := gengo.ExtractCommentTags("+", lines)[key]
	if !present || len(val) < 1 {
		return ""
	}

	return val[0]
}
