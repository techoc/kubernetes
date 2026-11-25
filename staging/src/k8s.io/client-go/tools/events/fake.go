/*
Copyright 2019 The Kubernetes Authors.

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

package events

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
)

// FakeRecorder 是测试期间使用的假实现，线程安全。
// 也可以不用 NewFakeRecorder 而手工创建，但这种情况下事件可能全部被丢弃：
// 线程安全来自调用方对 channel 的使用，结构体本身无锁；手工构造时若
// Events 为 nil，writeEvent 会静默丢弃事件，因此通常应使用 NewFakeRecorder。
type FakeRecorder struct {
	// Events 接收格式化后的事件字符串；nil 表示不记录任何事件。
	Events chan string
	// Verbose 为 true 时输出包含 action、相关对象与注解的完整信息。
	Verbose bool
}

var _ EventRecorderLogger = &FakeRecorder{}
var _ AnnotatedEventRecorder = &FakeRecorder{}

// Eventf 上报一个事件。
// 按接口要求，实际格式化与发送在 writeEvent 中完成。
func (f *FakeRecorder) Eventf(regarding runtime.Object, related runtime.Object, eventtype, reason, action, note string, args ...interface{}) {
	f.writeEvent(regarding, related, nil, eventtype, reason, action, note, args...)
}

// AnnotatedEventf 与 Eventf 相同地上报事件，但会附带 annotations
func (f *FakeRecorder) AnnotatedEventf(regarding runtime.Object, related runtime.Object, annotations map[string]string, eventtype, reason, action, note string, args ...interface{}) {
	f.writeEvent(regarding, related, annotations, eventtype, reason, action, note, args...)
}

// WithLogger 实现 EventRecorderLogger；fake 不需要日志，直接返回自身。
func (f *FakeRecorder) WithLogger(logger klog.Logger) EventRecorderLogger {
	return f
}

// writeEvent 把事件参数拼成字符串并发送到 Events channel。
// Events 为 nil 或缓冲已满时的行为由调用方决定——这里只是阻塞式发送，
// 测试需保证消费速度，否则记录方会被阻塞。
func (f *FakeRecorder) writeEvent(regarding runtime.Object, related runtime.Object, annotations map[string]string, eventtype, reason, action, note string, args ...interface{}) {
	if f.Events != nil {
		// 默认只保留 type/reason/note，便于测试做简单断言；Verbose 才拼接额外上下文。
		msg := fmt.Sprintf(eventtype+" "+reason+" "+note, args...)
		if f.Verbose {
			msg = eventtype + " " + reason + " " + action + " " + fmt.Sprintf(note, args...) +
				objectString(regarding) + objectString(related) + annotationsString(annotations)
		}
		f.Events <- msg
	}
}

// annotationsString 返回 annotations map 的格式化字符串，为空时返回空串
func annotationsString(annotations map[string]string) string {
	annotationString := ""
	if len(annotations) > 0 {
		annotationString = " " + fmt.Sprint(annotations)
	}
	return annotationString
}

// objectString 返回包含对象 kind 和 apiVersion 的格式化字符串。
// 仅当对象已设置 GVK 时才输出，避免产生空 kind/apiVersion 的噪声。
func objectString(object runtime.Object) string {
	objectString := ""
	if object != nil {
		gvk := object.GetObjectKind().GroupVersionKind()
		if !gvk.Empty() {
			objectString = fmt.Sprintf(" {kind=%s,apiVersion=%s}",
				gvk.Kind,
				gvk.GroupVersion(),
			)
		}
	}
	return objectString
}

// NewFakeRecorder 创建新的事件记录器 fake，其事件 channel 具有指定缓冲大小。
// 缓冲大小决定可积压的事件数；测试通常在后台持续读取 Events 并断言内容。
func NewFakeRecorder(bufferSize int) *FakeRecorder {
	return &FakeRecorder{
		Events:  make(chan string, bufferSize),
		Verbose: false,
	}
}
