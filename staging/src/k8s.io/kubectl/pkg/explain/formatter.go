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

package explain

import (
	"fmt"
	"io"
	"regexp"
	"strings"
)

// Formatter 帮助你带缩进地写入内容，并可在需要时换行。
type Formatter struct {
	IndentLevel int
	Wrap        int
	Writer      io.Writer
}

// Indent 创建一个新的 Formatter，将代码的缩进增加指定的量。
func (f Formatter) Indent(indent int) *Formatter {
	f.IndentLevel = f.IndentLevel + indent
	return &f
}

// Write 按 Formatter 设置的缩进写入字符串。
// 该方法不会换行文本。
func (f *Formatter) Write(str string, a ...interface{}) error {
	// 空行不缩进
	if str == "" {
		_, err := io.WriteString(f.Writer, "\n")
		return err
	}

	indent := ""
	for i := 0; i < f.IndentLevel; i++ {
		indent = indent + " "
	}

	if len(a) > 0 {
		str = fmt.Sprintf(str, a...)
	}
	_, err := io.WriteString(f.Writer, indent+str+"\n")
	return err
}

// WriteWrapped 按 Formatter 设置的缩进写入字符串，
// 并根据需要进行换行。
func (f *Formatter) WriteWrapped(str string, a ...interface{}) error {
	if f.Wrap == 0 {
		return f.Write(str, a...)
	}
	text := fmt.Sprintf(str, a...)
	strs := wrapString(text, f.Wrap-f.IndentLevel)
	for _, substr := range strs {
		if err := f.Write(substr); err != nil {
			return err
		}
	}
	return nil
}

type line struct {
	wrap  int
	words []string
}

func (l *line) String() string {
	return strings.Join(l.words, " ")
}

func (l *line) Empty() bool {
	return len(l.words) == 0
}

func (l *line) Len() int {
	return len(l.String())
}

// Add 尝试将单词添加到当前行，成功则返回 true，空间不足则返回 false。
// 对于空行，添加总是可行的。
func (l *line) Add(word string) bool {
	newLine := line{
		wrap:  l.wrap,
		words: append(l.words, word),
	}
	if newLine.Len() <= l.wrap || len(l.words) == 0 {
		l.words = newLine.words
		return true
	}
	return false
}

var bullet = regexp.MustCompile(`^(\d+\.?|-|\*)\s`)

func shouldStartNewLine(lastWord, str string) bool {
	// 保留以 : 结尾的行后的换行
	if strings.HasSuffix(lastWord, ":") {
		return true
	}

	// 保留代码块
	if strings.HasPrefix(str, "    ") {
		return true
	}
	str = strings.TrimSpace(str)
	// 保留空行
	if len(str) == 0 {
		return true
	}
	// 保留看起来是列表开头的行
	if bullet.MatchString(str) {
		return true
	}
	// 否则合并
	return false
}

func wrapString(str string, wrap int) []string {
	wrapped := []string{}
	l := line{wrap: wrap}
	// 记录添加到当前行的最后一个单词
	lastWord := ""
	flush := func() {
		if !l.Empty() {
			lastWord = ""
			wrapped = append(wrapped, l.String())
			l = line{wrap: wrap}
		}
	}

	// 遍历原始描述中的各行
	for _, str := range strings.Split(str, "\n") {
		// 原样保留代码块和引用块
		if strings.HasPrefix(str, "    ") {
			flush()
			wrapped = append(wrapped, str)
			continue
		}

		// 保留第一行之后的空行，因为它们可用于分隔逻辑段落
		if len(wrapped) > 0 && len(strings.TrimSpace(str)) == 0 {
			flush()
			wrapped = append(wrapped, "")
			continue
		}

		// 如果需要开始新行，则先刷出当前行
		if shouldStartNewLine(lastWord, str) {
			flush()
		}
		words := strings.Fields(str)
		for _, word := range words {
			lastWord = word
			if !l.Add(word) {
				flush()
				if !l.Add(word) {
					panic("Couldn't add to empty line.")
				}
			}
		}
	}
	flush()
	return wrapped
}
