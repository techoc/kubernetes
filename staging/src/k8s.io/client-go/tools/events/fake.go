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

// FakeRecorder is used as a fake during tests. It is thread safe. It is usable
// when created manually and not by NewFakeRecorder, however all events may be
// thrown away in this case.
//
// 中文说明：线程安全来自调用方对 channel 的使用，结构体本身无锁。手工构造时若
// Events 为 nil，writeEvent 会静默丢弃事件，因此通常应使用 NewFakeRecorder。
type FakeRecorder struct {
	// Events 接收格式化后的事件字符串；nil 表示不记录任何事件。
	Events chan string
	// Verbose 为 true 时输出包含 action、相关对象与注解的完整信息。
	Verbose bool
}

var _ EventRecorderLogger = &FakeRecorder{}
var _ AnnotatedEventRecorder = &FakeRecorder{}

// Eventf emits an event
//
// 中文：接口要求，实际格式化与发送在 writeEvent 中完成。
func (f *FakeRecorder) Eventf(regarding runtime.Object, related runtime.Object, eventtype, reason, action, note string, args ...interface{}) {
	f.writeEvent(regarding, related, nil, eventtype, reason, action, note, args...)
}

// AnnotatedEventf emits an event like Eventf, but with annotations attached
func (f *FakeRecorder) AnnotatedEventf(regarding runtime.Object, related runtime.Object, annotations map[string]string, eventtype, reason, action, note string, args ...interface{}) {
	f.writeEvent(regarding, related, annotations, eventtype, reason, action, note, args...)
}

// WithLogger 实现 EventRecorderLogger；fake 不需要日志，直接返回自身。
func (f *FakeRecorder) WithLogger(logger klog.Logger) EventRecorderLogger {
	return f
}

// writeEvent constructs a string from the event parameters and sends
// it to the Events channel
//
// 中文说明：Events 为 nil 或缓冲已满时的行为由调用方决定——这里只是阻塞式发送，
// 测试需保证消费速度，否则记录方会被阻塞。
func (f *FakeRecorder) writeEvent(regarding runtime.Object, related runtime.Object, annotations map[string]string, eventtype, reason, action, note string, args ...interface{}) {
	if f.Events != nil {
		// 中文：默认只保留 type/reason/note，便于测试做简单断言；Verbose 才拼接额外上下文。
		msg := fmt.Sprintf(eventtype+" "+reason+" "+note, args...)
		if f.Verbose {
			msg = eventtype + " " + reason + " " + action + " " + fmt.Sprintf(note, args...) +
				objectString(regarding) + objectString(related) + annotationsString(annotations)
		}
		f.Events <- msg
	}
}

// annotationsString returns a formatted string of the annotations map,
// or empty string if none
func annotationsString(annotations map[string]string) string {
	annotationString := ""
	if len(annotations) > 0 {
		annotationString = " " + fmt.Sprint(annotations)
	}
	return annotationString
}

// objectString returns a formatted string with the object's kind and
// apiVersion
//
// 中文：仅当对象已设置 GVK 时才输出，避免产生空 kind/apiVersion 的噪声。
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

// NewFakeRecorder creates new fake event recorder with event channel with
// buffer of given size.
//
// 中文说明：缓冲大小决定可积压的事件数；测试通常在后台持续读取 Events 并断言内容。
func NewFakeRecorder(bufferSize int) *FakeRecorder {
	return &FakeRecorder{
		Events:  make(chan string, bufferSize),
		Verbose: false,
	}
}
