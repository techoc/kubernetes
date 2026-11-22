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

package record

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
)

// FakeRecorder is used as a fake during tests. It is thread safe. It is usable
// when created manually and not by NewFakeRecorder, however all events may be
// thrown away in this case.
//
// Events 为 nil 时写事件会被静默丢弃，因此手工构造零值时请确保测试不需要断言事件。
type FakeRecorder struct {
	Events chan string

	// IncludeObject 为 true 时，事件文本会附带 involvedObject 的 kind/apiVersion，便于断言。
	IncludeObject bool
}

var _ EventRecorderLogger = &FakeRecorder{}

// objectString 生成 involvedObject 后缀；不包含对象时返回空串。
func objectString(object runtime.Object, includeObject bool) string {
	if !includeObject {
		return ""
	}
	return fmt.Sprintf(" involvedObject{kind=%s,apiVersion=%s}",
		object.GetObjectKind().GroupVersionKind().Kind,
		object.GetObjectKind().GroupVersionKind().GroupVersion(),
	)
}

// annotationsString 生成注解后缀；无注解时返回空串。
func annotationsString(annotations map[string]string) string {
	if len(annotations) == 0 {
		return ""
	} else {
		return " " + fmt.Sprint(annotations)
	}
}

// writeEvent 把事件格式化为 "type reason message" 并追加对象/注解信息。
// 注意 channel 满时会阻塞调用方，测试需保证有消费者或缓冲足够。
func (f *FakeRecorder) writeEvent(object runtime.Object, annotations map[string]string, eventtype, reason, messageFmt string, args ...interface{}) {
	if f.Events != nil {
		f.Events <- fmt.Sprintf(eventtype+" "+reason+" "+messageFmt, args...) +
			objectString(object, f.IncludeObject) + annotationsString(annotations)
	}
}

// Event 记录固定消息事件。
func (f *FakeRecorder) Event(object runtime.Object, eventtype, reason, message string) {
	f.writeEvent(object, nil, eventtype, reason, "%s", message)
}

// Eventf 记录格式化消息事件。
func (f *FakeRecorder) Eventf(object runtime.Object, eventtype, reason, messageFmt string, args ...interface{}) {
	f.writeEvent(object, nil, eventtype, reason, messageFmt, args...)
}

// AnnotatedEventf 记录带注解的格式化事件。
func (f *FakeRecorder) AnnotatedEventf(object runtime.Object, annotations map[string]string, eventtype, reason, messageFmt string, args ...interface{}) {
	f.writeEvent(object, annotations, eventtype, reason, messageFmt, args...)
}

// WithLogger 直接返回自身：fake 不区分 logger，保持接口兼容。
func (f *FakeRecorder) WithLogger(logger klog.Logger) EventRecorderLogger {
	return f
}

// NewFakeRecorder creates new fake event recorder with event channel with
// buffer of given size.
func NewFakeRecorder(bufferSize int) *FakeRecorder {
	return &FakeRecorder{
		Events: make(chan string, bufferSize),
	}
}
