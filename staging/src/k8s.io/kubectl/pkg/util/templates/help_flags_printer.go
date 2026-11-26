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

package templates

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/mitchellh/go-wordwrap"
	flag "github.com/spf13/pflag"
)

const offset = 10

// 处理 help 标志并写入 I/O writer
type HelpFlagPrinter struct {
	wrapLimit uint
	out       io.Writer
}

// 用给定 I/O writer 初始化 HelpFlagPrinter。
func NewHelpFlagPrinter(out io.Writer, wrapLimit uint) *HelpFlagPrinter {
	return &HelpFlagPrinter{
		wrapLimit: wrapLimit,
		out:       out,
	}
}

// 美化 help 标志并写到 p.out；
func (p *HelpFlagPrinter) PrintHelpFlag(flag *flag.Flag) {
	formatBuf := new(bytes.Buffer)
	writeFlag(formatBuf, flag)

	wrappedStr := formatBuf.String()
	flagAndUsage := strings.Split(formatBuf.String(), "\n")
	flagStr := flagAndUsage[0]

	// 用法超过一行时重新折行。
	if len(flagAndUsage) > 1 {
		nextLines := strings.Join(flagAndUsage[1:], " ")
		wrappedUsages := wordwrap.WrapString(nextLines, p.wrapLimit-offset)
		wrappedStr = flagStr + "\n" + wrappedUsages
	}
	appendTabStr := strings.ReplaceAll(wrappedStr, "\n", "\n\t")

	fmt.Fprint(p.out, appendTabStr+"\n\n")
}

// 按 getFlagFormat 提供的格式把 help 标志
// 写入 I/O writer。
func writeFlag(out io.Writer, f *flag.Flag) {
	deprecated := ""
	if f.Deprecated != "" {
		deprecated = fmt.Sprintf(" (DEPRECATED: %s)", f.Deprecated)
	}
	fmt.Fprintf(out, getFlagFormat(f), f.Shorthand, f.Name, f.DefValue, f.Usage, deprecated)
}
