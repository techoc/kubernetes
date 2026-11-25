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

package util

import (
	"strings"
)

var (
	// AlphaDisclaimer 置于 alpha 版本命令描述的末尾。
	AlphaDisclaimer = `
		Alpha Disclaimer: this command is currently alpha.
	`
)

// LongDesc 用于在代码中生成更清晰的命令行长描述。
// 其行为受 kubectl 同名函数启发，后者接受 Markdown 输入。
// 此函数不兼容 Markdown，但满足 kubeadm 的需要，具体规则如下：
// - 删除输出开头和结尾的空格（包括空行）。
// - 连续非空行以空格连接为段落。
// - 一个或多个空行，或仅包含空格的行，用于分隔段落。
// - 输出中段落之间恰好保留一个空行。
// - 在文本行末尾添加两个空格可强制换行。
// - 删除所有缩进，输出不缩进。
func LongDesc(s string) string {
	// 删除开头和结尾的空格（包括空行），并将各行拆分为切片。
	lines := strings.Split(strings.TrimSpace(s), "\n")

	output := []string{}
	paragraph := []string{}

	for _, line := range lines {
		// 删除当前行的缩进和末尾空格。
		trimmedLine := strings.TrimSpace(line)
		if trimmedLine == "" {
			if len(paragraph) > 0 {
				// 若当前行为空且当前段落非空，则已到达段落末尾。
				// 若段落和当前行均为空，则这是段落之间的非首个空行，应忽略。
				// 此时以单个空格连接段落中的所有行，添加末尾换行以确保段落后有空行，
				// 将段落文本追加到输出，并清空当前段落切片。
				output = append(output, strings.Join(paragraph, " ")+"\n")
				paragraph = []string{}
			}
		} else {
			// 非空文本行，追加到当前段落。
			paragraph = append(paragraph, trimmedLine)
			if strings.HasSuffix(line, "  ") {
				// 若原始行末尾有两个空格，则添加换行。
				// 实现方式是输出当前段落并开始新段落。
				// 输出的段落末尾不添加空格，因此新旧段落之间只有普通换行而非空行。
				output = append(output, strings.Join(paragraph, " "))
				paragraph = []string{}
			}
		}
	}

	// 最后一个段落始终尚未输出，因此在此输出。
	// 不添加末尾换行，以免还需裁剪输出。
	output = append(output, strings.Join(paragraph, " "))

	// 以换行连接所有段落。
	return strings.Join(output, "\n")
}

// Examples 用于生成命令行用法示例，行为在以下方面类似 kubectl 的同名函数：
// - 删除输出开头和结尾的空格（包括空行）。
// - 删除每行开头和结尾的空格（因此丢失原缩进），然后统一缩进两个空格。
func Examples(s string) string {
	trimmedText := strings.TrimSpace(s)
	if trimmedText == "" {
		return ""
	}

	const indent = `  `
	inLines := strings.Split(trimmedText, "\n")
	outLines := make([]string, 0, len(inLines))

	for _, line := range inLines {
		outLines = append(outLines, indent+strings.TrimSpace(line))
	}

	return strings.Join(outLines, "\n")
}
