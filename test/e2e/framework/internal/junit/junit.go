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

package junit

import (
	"slices"
	"strings"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/ginkgo/v2/reporters"
	"github.com/onsi/ginkgo/v2/types"
)

// WriteJUnitReport 生成的 JUnit 文件比
// `ginkgo --junit-report` 通常生成的更短。因为完整
// 报告对 Spyglass 等工具可能太大。
// (https://github.com/kubernetes/kubernetes/issues/111510).
func WriteJUnitReport(report ginkgo.Report, filename string) error {
	config := reporters.JunitReportConfig{
		// 去掉我们不关心的 spec 的详情。
		OmitTimelinesForSpecState: types.SpecStatePassed | types.SpecStateSkipped,

		// 不写 <failure message="summary">。同样的文本
		// 也在失败的完整文本中。如果两者都写，
		// kettle 和 spyglass 等工具会拼接
		// 这两个字符串，从而显示重复信息。
		OmitFailureMessageAttr: true,

		// 所有标签也已作为内联 [] 标签出现在 spec 文本中，
		// 无需单独写出。
		OmitSpecLabels: true,
	}

	// 按完整名称排序 spec。默认按开始（或完成？）时间排序，
	// 在 spyglass 中用处不大，因为那里不显示这些时间，
	// 测试看起来像是无序列出的。
	slices.SortFunc(report.SpecReports, func(a, b types.SpecReport) int {
		res := strings.Compare(a.FullText(), b.FullText())
		if res == 0 {
			// 两个 spec 完整名称相同时，用开始时间作为决胜键
			// （这种可能性很小）。
			return a.StartTime.Compare(b.StartTime)
		}
		return res
	})

	detectDataRaces(report)

	return reporters.GenerateJUnitReportWithConfig(report, filename, config)
}
