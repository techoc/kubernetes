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

package printers

import (
	"io"

	"github.com/liggitt/tabwriter"
)

const (
	// 最小列宽 6、制表步长 4、列间隔 3 个空格，与 kubectl get 的观感对齐。
	tabwriterMinWidth = 6
	tabwriterWidth    = 4
	tabwriterPadding  = 3
	tabwriterPadChar  = ' '
	// 记住已见列宽，使同一表格后续行对齐稳定，不会因长值出现而整体抖动。
	tabwriterFlags = tabwriter.RememberWidths
)

// GetNewTabWriter 返回能把输入中的制表符列对齐成文本的 tabwriter。
func GetNewTabWriter(output io.Writer) *tabwriter.Writer {
	// 调用方负责 Flush，否则缓冲内容不会写出。
	return tabwriter.NewWriter(output, tabwriterMinWidth, tabwriterWidth, tabwriterPadding, tabwriterPadChar, tabwriterFlags)
}
