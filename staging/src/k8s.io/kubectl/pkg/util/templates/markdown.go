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

package templates

import (
	"fmt"
	"io"
	"strings"

	"github.com/russross/blackfriday/v2"
)

const linebreak = "\n"

// 实现 blackfriday.Renderer。
var _ blackfriday.Renderer = &ASCIIRenderer{}

// 把 Markdown 渲染为纯文本的 blackfriday.Renderer，
// 适合在终端上阅读。
type ASCIIRenderer struct {
	Indentation string

	listItemCount uint
	listLevel     uint
}

// 把 Markdown 渲染为文本。
func (r *ASCIIRenderer) RenderNode(w io.Writer, node *blackfriday.Node, entering bool) blackfriday.WalkStatus {
	switch node.Type {
	case blackfriday.Text:
		raw := string(node.Literal)
		lines := strings.Split(raw, linebreak)
		for _, line := range lines {
			trimmed := strings.Trim(line, " \n\t")
			if len(trimmed) > 0 && trimmed[0] != '_' {
				w.Write([]byte(" "))
			}
			w.Write([]byte(trimmed))
		}
	case blackfriday.HorizontalRule, blackfriday.Hardbreak:
		w.Write([]byte(linebreak + "----------" + linebreak))
	case blackfriday.Code, blackfriday.CodeBlock:
		w.Write([]byte(linebreak))
		lines := []string{}
		for _, line := range strings.Split(string(node.Literal), linebreak) {
			trimmed := strings.Trim(line, " \t")
			// 增加四级缩进，让 blackfriday 下次仍把这段
			// 字面量识别为 Code 或 CodeBlock。
			indented := strings.Repeat(r.Indentation, 4) + trimmed
			lines = append(lines, indented)
		}
		w.Write([]byte(strings.Join(lines, linebreak)))
	case blackfriday.Image:
		w.Write(node.LinkData.Destination)
	case blackfriday.Link:
		w.Write([]byte(" "))
		w.Write(node.LinkData.Destination)
	case blackfriday.Paragraph:
		if r.listLevel == 0 {
			w.Write([]byte(linebreak))
		}
	case blackfriday.List:
		if entering {
			w.Write([]byte(linebreak))
			r.listLevel++
		} else {
			r.listLevel--
			r.listItemCount = 0
		}
	case blackfriday.Item:
		if entering {
			r.listItemCount++
			for i := 0; uint(i) < r.listLevel; i++ {
				w.Write([]byte(r.Indentation))
			}
			if node.ListFlags&blackfriday.ListTypeOrdered != 0 {
				w.Write([]byte(fmt.Sprintf("%d. ", r.listItemCount)))
			} else {
				w.Write([]byte("* "))
			}
		} else {
			w.Write([]byte(linebreak))
		}
	default:
		normalText(w, node.Literal)
	}
	return blackfriday.GoToNext
}

func normalText(w io.Writer, text []byte) {
	w.Write([]byte(strings.Trim(string(text), " \n\t")))
}

// 输出文档前言，必要时输出目录。
func (r *ASCIIRenderer) RenderHeader(w io.Writer, ast *blackfriday.Node) {

}

// 输出文档页脚。
func (r *ASCIIRenderer) RenderFooter(w io.Writer, ast *blackfriday.Node) {
	io.WriteString(w, "\n")
}
