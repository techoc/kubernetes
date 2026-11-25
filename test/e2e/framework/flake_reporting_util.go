/*
Copyright 2018 The Kubernetes Authors.

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

package framework

import (
	"bytes"
	"fmt"
	"sync"
)

// FlakeReport 用于管理 flaky 测试报告。
type FlakeReport struct {
	lock       sync.RWMutex
	Flakes     []string `json:"flakes"`
	FlakeCount int      `json:"flakeCount"`
}

// NewFlakeReport 返回一个新的 flaky 测试报告。
func NewFlakeReport() *FlakeReport {
	return &FlakeReport{
		Flakes: []string{},
	}
}

func buildDescription(optionalDescription ...interface{}) string {
	switch len(optionalDescription) {
	case 0:
		return ""
	default:
		return fmt.Sprintf(optionalDescription[0].(string), optionalDescription[1:]...)
	}
}

// RecordFlakeIfError 将非 nil 的错误连同可选描述记录为一次 flaky 事件。
// 对于可视为 flake 的非关键错误，可用它替代 framework.ExpectNoError()，避免导致测试失败。
func (f *FlakeReport) RecordFlakeIfError(err error, optionalDescription ...interface{}) {
	if err == nil {
		return
	}
	msg := fmt.Sprintf("Unexpected error occurred: %v", err)
	desc := buildDescription(optionalDescription...)
	if desc != "" {
		msg = fmt.Sprintf("%v (Description: %v)", msg, desc)
	}
	Logf("%s", msg)
	f.lock.Lock()
	defer f.lock.Unlock()
	f.Flakes = append(f.Flakes, msg)
	f.FlakeCount++
}

// GetFlakeCount 返回 flaky 事件的数量。
func (f *FlakeReport) GetFlakeCount() int {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.FlakeCount
}

// PrintHumanReadable 返回 flaky 测试报告的可读字符串形式。
func (f *FlakeReport) PrintHumanReadable() string {
	f.lock.RLock()
	defer f.lock.RUnlock()
	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("FlakeCount: %v\n", f.FlakeCount))
	buf.WriteString("Flakes:\n")
	for _, flake := range f.Flakes {
		buf.WriteString(fmt.Sprintf("%v\n", flake))
	}
	return buf.String()
}

// PrintJSON 以 JSON 格式返回 flaky 测试报告的摘要。
func (f *FlakeReport) PrintJSON() string {
	f.lock.RLock()
	defer f.lock.RUnlock()
	return PrettyPrintJSON(f)
}

// SummaryKind 返回 flaky 测试报告的摘要类型。
func (f *FlakeReport) SummaryKind() string {
	return "FlakeReport"
}
