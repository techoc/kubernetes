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

package diff

import (
	"bytes"
	"fmt"
	"strings"
	"text/tabwriter"

	"k8s.io/utils/dump"
)

// ObjectGoPrintSideBySide 把 a 和 b 的文本转储左右并排打印，便于肉眼快速扫描差异。
// 保留此函数是为了兼容仍在调用它的旧代码；新代码应使用 go-cmp。
func ObjectGoPrintSideBySide(a, b interface{}) string {
	sA := dump.Pretty(a)
	sB := dump.Pretty(b)

	linesA := strings.Split(sA, "\n")
	linesB := strings.Split(sB, "\n")
	// 先求出两侧最长行的宽度，作为 tabwriter 的第一列最小宽度，保证两列对齐。
	width := 0
	for _, s := range linesA {
		l := len(s)
		if l > width {
			width = l
		}
	}
	for _, s := range linesB {
		l := len(s)
		if l > width {
			width = l
		}
	}
	buf := &bytes.Buffer{}
	w := tabwriter.NewWriter(buf, width, 0, 1, ' ', 0)
	max := len(linesA)
	if len(linesB) > max {
		max = len(linesB)
	}
	// 按行并行输出；较短一侧不足的部分留空，保证行号对应关系不乱。
	for i := 0; i < max; i++ {
		var a, b string
		if i < len(linesA) {
			a = linesA[i]
		}
		if i < len(linesB) {
			b = linesB[i]
		}
		_, _ = fmt.Fprintf(w, "%s\t%s\n", a, b)
	}
	_ = w.Flush()
	return buf.String()
}
