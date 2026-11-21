/*
Copyright 2015 The Kubernetes Authors.

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

package oom

import (
	"context"
	"fmt"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/kubernetes/third_party/forked/cadvisor/oomparser"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeStreamer 是向通道推送预设 OOM 实例的 OomStreamer 桩。
type fakeStreamer struct {
	oomInstancesToStream []*oomparser.OomInstance
}

// StreamOoms 依次把预设的 OOM 实例写入输出通道。
func (fs *fakeStreamer) StreamOoms(_ context.Context, outStream chan<- *oomparser.OomInstance) {
	for _, oi := range fs.oomInstancesToStream {
		outStream <- oi
	}
}

// TestWatcherRecordsEventsForOomEvents 验证 StreamOoms 推送的 OomInstance
// 会被转换为 recorder 事件。
func TestWatcherRecordsEventsForOomEvents(t *testing.T) {
	tCtx := ktesting.Init(t)
	oomInstancesToStream := []*oomparser.OomInstance{
		{
			Pid:                 1000,
			ProcessName:         "fakeProcess",
			TimeOfDeath:         time.Now(),
			ContainerName:       recordEventContainerName + "some-container",
			VictimContainerName: recordEventContainerName,
		},
	}
	numExpectedOomEvents := len(oomInstancesToStream)

	fakeStreamer := &fakeStreamer{
		oomInstancesToStream: oomInstancesToStream,
	}

	fakeRecorder := record.NewFakeRecorder(numExpectedOomEvents)
	node := &v1.ObjectReference{}

	oomWatcher := &realWatcher{
		recorder:    fakeRecorder,
		oomStreamer: fakeStreamer,
	}
	// 启动 watcher；它在后台消费 OOM 流并记录事件。
	require.NoError(t, oomWatcher.Start(tCtx, node))

	eventsRecorded := getRecordedEvents(fakeRecorder, numExpectedOomEvents)
	assert.Len(t, eventsRecorded, numExpectedOomEvents)
}

// getRecordedEvents 收集 recorder 中的事件，最多等待 10 秒。
func getRecordedEvents(fakeRecorder *record.FakeRecorder, numExpectedOomEvents int) []string {
	eventsRecorded := []string{}

	select {
	case event := <-fakeRecorder.Events:
		eventsRecorded = append(eventsRecorded, event)

		if len(eventsRecorded) == numExpectedOomEvents {
			break
		}
	// 超时后返回已收集的事件，由调用方断言数量。
	case <-time.After(10 * time.Second):
		break
	}

	return eventsRecorded
}

// TestWatcherRecordsEventsForOomEventsCorrectContainerName 验证
// 只对容器名为 "/" 的 OOM 事件记录事件。
func TestWatcherRecordsEventsForOomEventsCorrectContainerName(t *testing.T) {
	// 这里"错误的容器名"指不应触发事件记录的容器名。
	tCtx := ktesting.Init(t)
	// 两个实例中只有一个应产生事件。
	numOomEventsWithIncorrectContainerName := 1
	oomInstancesToStream := []*oomparser.OomInstance{
		{
			Pid:                 1000,
			ProcessName:         "fakeProcess",
			TimeOfDeath:         time.Now(),
			ContainerName:       recordEventContainerName + "some-container",
			VictimContainerName: recordEventContainerName,
		},
		{
			Pid:                 1000,
			ProcessName:         "fakeProcess",
			TimeOfDeath:         time.Now(),
			ContainerName:       recordEventContainerName + "kubepods/some-container",
			VictimContainerName: recordEventContainerName + "kubepods",
		},
	}
	numExpectedOomEvents := len(oomInstancesToStream) - numOomEventsWithIncorrectContainerName

	fakeStreamer := &fakeStreamer{
		oomInstancesToStream: oomInstancesToStream,
	}

	fakeRecorder := record.NewFakeRecorder(numExpectedOomEvents)
	node := &v1.ObjectReference{}

	oomWatcher := &realWatcher{
		recorder:    fakeRecorder,
		oomStreamer: fakeStreamer,
	}
	require.NoError(t, oomWatcher.Start(tCtx, node))

	eventsRecorded := getRecordedEvents(fakeRecorder, numExpectedOomEvents)
	assert.Len(t, eventsRecorded, numExpectedOomEvents)
}

// TestWatcherRecordsEventsForOomEventsWithAdditionalInfo 验证
// 事件消息中包含正确的 pid 与进程信息。
func TestWatcherRecordsEventsForOomEventsWithAdditionalInfo(t *testing.T) {
	// 进程与事件信息应出现在事件消息中。
	eventPid := 1000
	processName := "fakeProcess"

	tCtx := ktesting.Init(t)

	oomInstancesToStream := []*oomparser.OomInstance{
		{
			Pid:                 eventPid,
			ProcessName:         processName,
			TimeOfDeath:         time.Now(),
			ContainerName:       recordEventContainerName + "some-container",
			VictimContainerName: recordEventContainerName,
		},
	}
	numExpectedOomEvents := len(oomInstancesToStream)

	fakeStreamer := &fakeStreamer{
		oomInstancesToStream: oomInstancesToStream,
	}

	fakeRecorder := record.NewFakeRecorder(numExpectedOomEvents)
	node := &v1.ObjectReference{}

	oomWatcher := &realWatcher{
		recorder:    fakeRecorder,
		oomStreamer: fakeStreamer,
	}
	require.NoError(t, oomWatcher.Start(tCtx, node))

	eventsRecorded := getRecordedEvents(fakeRecorder, numExpectedOomEvents)

	assert.Len(t, eventsRecorded, numExpectedOomEvents)
	// 校验事件消息包含 OOM 类型、pid 与受害者进程名。
	assert.Contains(t, eventsRecorded[0], systemOOMEvent)
	assert.Contains(t, eventsRecorded[0], fmt.Sprintf("pid: %d", eventPid))
	assert.Contains(t, eventsRecorded[0], fmt.Sprintf("victim process: %s", processName))
}
