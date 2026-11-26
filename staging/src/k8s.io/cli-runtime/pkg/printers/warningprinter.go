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
	"fmt"
	"io"
)

const (
	// 黄色加粗 + 复位码，仅在 opts.Color 为 true 时使用（例如 TTY 输出）。
	yellowColor = "\u001b[33;1m"
	resetColor  = "\u001b[0m"
)

// WarningPrinter 把 API server 返回的 warning 写到指定 writer（通常是 stderr）。
type WarningPrinter struct {
	// out 是警告的输出目标。
	out io.Writer
	// opts 控制警告的输出行为。
	opts WarningPrinterOptions
}

// WarningPrinterOptions 控制 NewWarningPrinter 构造出的 WarningPrinter 的行为。
type WarningPrinterOptions struct {
	// Color 表示警告输出可以包含 ANSI 颜色码。
	Color bool
}

// NewWarningPrinter 返回把警告写入指定 writer 的 warningPrinter。
func NewWarningPrinter(out io.Writer, opts WarningPrinterOptions) *WarningPrinter {
	h := &WarningPrinter{out: out, opts: opts}
	return h
}

// Print 把警告写入配置的 writer。
//
// "Warning:" 前缀是 kubectl 的稳定输出格式之一，脚本可能据此过滤；
// 着色时仅给前缀上色，消息正文保持原样。
func (w *WarningPrinter) Print(message string) {
	if w.opts.Color {
		fmt.Fprintf(w.out, "%sWarning:%s %s\n", yellowColor, resetColor, message)
	} else {
		fmt.Fprintf(w.out, "Warning: %s\n", message)
	}
}
