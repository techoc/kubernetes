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

package timer

import (
	"time"

	"bytes"
	"fmt"

	"sync"

	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/e2e/perftype"
)

var now = time.Now

// Phase 表示测试的一个阶段。阶段之间可以重叠。
type Phase struct {
	sequenceNumber int
	name           string
	startTime      time.Time
	endTime        time.Time
}

func (phase *Phase) ended() bool {
	return !phase.endTime.IsZero()
}

// End 把阶段标记为已结束，除非之前已经结束过。
func (phase *Phase) End() {
	if !phase.ended() {
		phase.endTime = now()
	}
}

func (phase *Phase) label() string {
	return fmt.Sprintf("%03d-%s", phase.sequenceNumber, phase.name)
}

func (phase *Phase) duration() time.Duration {
	endTime := phase.endTime
	if !phase.ended() {
		endTime = now()
	}
	return endTime.Sub(phase.startTime)
}

func (phase *Phase) humanReadable() string {
	if phase.ended() {
		return fmt.Sprintf("Phase %s: %v\n", phase.label(), phase.duration())
	}
	return fmt.Sprintf("Phase %s: %v so far\n", phase.label(), phase.duration())
}

// TestPhaseTimer 对阶段分组，并提供把测量结果导出为 JSON 或可读文本的方式。
// 可并发使用。
type TestPhaseTimer struct {
	lock   sync.Mutex
	phases []*Phase
}

// NewTestPhaseTimer 创建一个新的 TestPhaseTimer。
func NewTestPhaseTimer() *TestPhaseTimer {
	return &TestPhaseTimer{}
}

// StartPhase 启动一个新阶段。
// sequenceNumber 是输出中加在 phaseName 前的整数，便于 perfdash 按字典序
// 排序阶段时还原执行顺序。遗憾的是它必须手动提供：简单递增计数器会导致
// 插入新阶段时后续阶段重新编号，破坏历史记录的连续性。
func (timer *TestPhaseTimer) StartPhase(sequenceNumber int, phaseName string) *Phase {
	timer.lock.Lock()
	defer timer.lock.Unlock()
	newPhase := &Phase{sequenceNumber: sequenceNumber, name: phaseName, startTime: now()}
	timer.phases = append(timer.phases, newPhase)
	return newPhase
}

// SummaryKind 返回测试摘要的摘要。
func (timer *TestPhaseTimer) SummaryKind() string {
	return "TestPhaseTimer"
}

// PrintHumanReadable 输出所有阶段的持续时间。
func (timer *TestPhaseTimer) PrintHumanReadable() string {
	buf := bytes.Buffer{}
	timer.lock.Lock()
	defer timer.lock.Unlock()
	for _, phase := range timer.phases {
		buf.WriteString(phase.humanReadable())
	}
	return buf.String()
}

// PrintJSON 以 JSON 格式输出所有阶段的持续时间。
func (timer *TestPhaseTimer) PrintJSON() string {
	data := perftype.PerfData{
		Version: "v1",
		DataItems: []perftype.DataItem{{
			Unit:   "s",
			Labels: map[string]string{"test": "phases"},
			Data:   make(map[string]float64)}}}
	timer.lock.Lock()
	defer timer.lock.Unlock()
	for _, phase := range timer.phases {
		data.DataItems[0].Data[phase.label()] = phase.duration().Seconds()
		if !phase.ended() {
			data.DataItems[0].Labels["ended"] = "false"
		}
	}
	return framework.PrettyPrintJSON(data)
}
