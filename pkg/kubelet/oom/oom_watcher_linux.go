//go:build linux

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

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/third_party/forked/cadvisor/oomparser"
)

// streamer 抽象 OOM 事件流的读取能力。
type streamer interface {
	// StreamOoms 把 OOM 事件写入 outStream。
	StreamOoms(ctx context.Context, outStream chan<- *oomparser.OomInstance)
}

// 编译期断言：OomParser 实现了 streamer 接口。
var _ streamer = &oomparser.OomParser{}

// realWatcher 是 Watcher 的 Linux 实现，基于内核日志监听 OOM。
type realWatcher struct {
	// recorder 用于记录 OOM 事件。
	recorder record.EventRecorderLogger
	// oomStreamer 提供 OOM 事件流。
	oomStreamer streamer
}

// 编译期断言：realWatcher 实现了 Watcher 接口。
var _ Watcher = &realWatcher{}

// NewWatcher 创建基于内核日志（/dev/kmsg）OOM 流的 Watcher。
func NewWatcher(recorder record.EventRecorderLogger) (Watcher, error) {
	// 测试场景下无需真实监听。
	// 测试使用 FakeRecorder 时不需要真实 OOM 监听。
	_, ok := recorder.(*record.FakeRecorder)
	if ok {
		return nil, nil
	}

	// OomParser 从内核日志读取 OOM 事件。
	oomStreamer, err := oomparser.New()
	if err != nil {
		return nil, err
	}

	watcher := &realWatcher{
		recorder:    recorder,
		oomStreamer: oomStreamer,
	}

	return watcher, nil
}

const (
	// 系统 OOM 事件使用的事件原因与容器名。
	systemOOMEvent           = "SystemOOM"
	recordEventContainerName = "/"
)

// Start 监听系统 OOM，并为每次 OOM 记录事件。
func (ow *realWatcher) Start(ctx context.Context, ref *v1.ObjectReference) error {
	// 事件流带缓冲，避免内核日志读取被短暂阻塞。
	outStream := make(chan *oomparser.OomInstance, 10)
	go ow.oomStreamer.StreamOoms(ctx, outStream)

	// 后台协程消费事件流并记录事件。
	go func() {
		logger := klog.FromContext(ctx)
		defer runtime.HandleCrashWithContext(ctx)

		for event := range outStream {
			// 统计每个容器的 OOM kill 次数，用于 container_oom_events_total 指标。
			recordOOMKill(event.ContainerName)

			// 只有容器名为 "/" 的事件才代表系统级 OOM。
			if event.VictimContainerName == recordEventContainerName {
				logger.V(1).Info("Got sys oom event", "event", event)
				eventMsg := "System OOM encountered"
				if event.ProcessName != "" && event.Pid != 0 {
					eventMsg = fmt.Sprintf("%s, victim process: %s, pid: %d", eventMsg, event.ProcessName, event.Pid)
				}
				ow.recorder.WithLogger(logger).Eventf(ref, v1.EventTypeWarning, systemOOMEvent, "%s", eventMsg)
			}
		}
		logger.Error(nil, "Unexpectedly stopped receiving OOM notifications")
	}()
	return nil
}
