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

package printers

import (
	"io"
	"os"
	"runtime"
	"strings"

	"github.com/moby/term"
)

// terminalEscaper 替换 ANSI 转义序列及其他终端特殊字符，以避免终端转义字符攻击（issue #101695）。
// 替换结果是不可执行的可见文本，防止恶意对象字段在用户终端里执行控制序列。
var terminalEscaper = strings.NewReplacer("\x1b", "^[", "\r", "\\r")

// cellSpecialChars 是打印表格单元格取值时需要特殊处理的字符集合：
// 断行字符（\f、\n、\r）会导致截断，\x1b (ESC) 需要转义。
// 它同时充当 WriteEscaped 与 appendCellValue（tableprinter.go）中快速路径的判据：
// \f 与 \n 会截断表格行，\r 可覆盖已有内容，ESC 可注入控制序列，都要走慢路径处理。
const cellSpecialChars = "\f\n\r\x1b"

// WriteEscaped replaces unsafe terminal characters with replacement strings
// and writes them to the given writer.
func WriteEscaped(writer io.Writer, output string) error {
	// 快速路径：字符串不含需要转义的字符时直接写出，不经过 Replacer；
	// 绝大多数单元格不含特殊字符，这样可避免 Replacer 带来的额外分配。
	if !strings.ContainsAny(output, cellSpecialChars) {
		_, err := io.WriteString(writer, output)
		return err
	}
	_, err := terminalEscaper.WriteString(writer, output)
	return err
}

// EscapeTerminal 以人类可读（但不可逆）的格式转义终端特殊字符。
// 只用于展示，替换后无法还原原始字节，切勿用于需要保真的场景。
func EscapeTerminal(in string) string {
	return terminalEscaper.Replace(in)
}

// IsTerminal returns whether the passed object is a terminal or not
func IsTerminal(i interface{}) bool {
	_, terminal := term.GetFdInfo(i)
	return terminal
}

// AllowsColorOutput returns true if the specified writer is a terminal and
// the process environment indicates color output is supported and desired.
func AllowsColorOutput(w io.Writer) bool {
	// 非终端（管道、重定向）默认不加颜色，避免污染被程序消费的输出。
	if !IsTerminal(w) {
		return false
	}

	// https://en.wikipedia.org/wiki/Computer_terminal#Dumb_terminals
	// dumb 终端不保证支持 ANSI 转义。
	if os.Getenv("TERM") == "dumb" {
		return false
	}

	// https://no-color.org/
	// 遵循 NO_COLOR 约定：变量存在即关闭颜色，内容无关。
	if _, nocolor := os.LookupEnv("NO_COLOR"); nocolor {
		return false
	}

	// Windows 上由现代终端组件设置 WT_SESSION 变量。
	// 老式终端对 UTF-8、VT 转义码等支持很差，因此只在 Windows Terminal 下启用颜色。
	if runtime.GOOS == "windows" && os.Getenv("WT_SESSION") == "" {
		return false
	}

	return true
}
