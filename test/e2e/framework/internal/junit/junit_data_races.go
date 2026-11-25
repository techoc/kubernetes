//go:build race

/*
Copyright 2025 The Kubernetes Authors.

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
	"regexp"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/ginkgo/v2/types"
)

var dataRaceRE = regexp.MustCompile(`(?m)^WARNING: DATA RACE$`)

func containsDataRace(output string) bool {
	return dataRaceRE.MatchString(output)
}

// 检测输出中的数据竞争，并将相关测试标记为失败。
// Ginkgo 自身会在打印警告的测试输出中捕获该警告，
// 但不会把测试标记为失败。
// 这不完全是错的（原因可能在于别处启动的代码），
// 但如果测试没有被标记为 "failed"，
// prune-junit-xml 会裁剪输出，spyglass 也不会
// 将该测试显示为失败。
//
// 修改报告。不触碰已经失败的测试，
// 因为它们已经需要排查了。
func detectDataRaces(report ginkgo.Report) {
	for i, spec := range report.SpecReports {
		if spec.State != types.SpecStatePassed {
			continue
		}

		// 为保险起见，检查捕获输出的两种形式。
		ginkgoWriterRace := containsDataRace(spec.CapturedGinkgoWriterOutput)
		stdoutRace := containsDataRace(spec.CapturedStdOutErr)
		if ginkgoWriterRace || stdoutRace {
			spec.State = types.SpecStateFailed
			spec.Failure = types.Failure{
				FailureNodeContext: types.FailureNodeIsLeafNode,
				FailureNodeType:    spec.LeafNodeType,
				Location:           types.NewCustomCodeLocation("output analysis"),
				TimelineLocation: types.TimelineLocation{
					Time: time.Now(),
				},
			}
			// 将这段文本移到失败消息中。
			if stdoutRace {
				spec.Failure.Message = spec.CapturedStdOutErr
				spec.CapturedStdOutErr = ""
			} else {
				spec.Failure.Message = spec.CapturedGinkgoWriterOutput
				spec.CapturedGinkgoWriterOutput = ""
			}
			report.SpecReports[i] = spec
		}
	}
}
