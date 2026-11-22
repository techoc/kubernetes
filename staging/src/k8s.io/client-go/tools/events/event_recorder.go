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
	"time"

	v1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/record/util"
	"k8s.io/client-go/tools/reference"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

// recorderImpl 是 events/v1 事件记录器的核心实现。
// 它把事件发到 Broadcaster，由后者异步交给 sink；recorder 本身不做重试。
type recorderImpl struct {
	scheme              *runtime.Scheme
	reportingController string
	reportingInstance   string
	*watch.Broadcaster
	// clock 可注入，测试中用 fake clock 固定事件时间。
	clock clock.Clock
}

var _ EventRecorder = &recorderImpl{}
var _ AnnotatedEventRecorder = &recorderImpl{}

func (recorder *recorderImpl) Eventf(regarding runtime.Object, related runtime.Object, eventtype, reason, action, note string, args ...interface{}) {
	recorder.eventf(klog.Background(), regarding, related, nil, eventtype, reason, action, note, args...)
}

func (recorder *recorderImpl) AnnotatedEventf(regarding runtime.Object, related runtime.Object, annotations map[string]string, eventtype, reason, action, note string, args ...interface{}) {
	recorder.eventf(klog.Background(), regarding, related, annotations, eventtype, reason, action, note, args...)
}

// recorderImplLogger 在 recorderImpl 之上绑定一个 logger，
// 使事件构造失败等错误带上调用方上下文，而不是写进全局日志。
type recorderImplLogger struct {
	*recorderImpl
	logger klog.Logger
}

var _ EventRecorderLogger = &recorderImplLogger{}

func (recorder *recorderImplLogger) Eventf(regarding runtime.Object, related runtime.Object, eventtype, reason, action, note string, args ...interface{}) {
	recorder.eventf(recorder.logger, regarding, related, nil, eventtype, reason, action, note, args...)
}

func (recorder *recorderImplLogger) AnnotatedEventf(regarding runtime.Object, related runtime.Object, annotations map[string]string, eventtype, reason, action, note string, args ...interface{}) {
	recorder.eventf(recorder.logger, regarding, related, annotations, eventtype, reason, action, note, args...)
}

// WithLogger 返回共享同一 recorderImpl 的新视图，仅替换 logger，开销很小。
func (recorder *recorderImplLogger) WithLogger(logger klog.Logger) EventRecorderLogger {
	return &recorderImplLogger{recorderImpl: recorder.recorderImpl, logger: logger}
}

// eventf 是各导出方法的公共实现：构造引用与事件对象，然后非阻塞地投递给 Broadcaster。
//
// 引用构造失败或事件类型非法时直接放弃并记录日志：这两类错误重试也不会成功。
func (recorder *recorderImpl) eventf(logger klog.Logger, regarding runtime.Object, related runtime.Object, annotations map[string]string, eventtype, reason, action, note string, args ...interface{}) {
	// 事件时间在调用时采样，而不是等到异步发送时，避免排队造成时间漂移。
	timestamp := metav1.MicroTime{Time: time.Now()}
	message := fmt.Sprintf(note, args...)
	refRegarding, err := reference.GetReference(recorder.scheme, regarding)
	if err != nil {
		logger.Error(err, "Could not construct reference, will not report event", "object", regarding, "eventType", eventtype, "reason", reason, "message", message)
		return
	}

	var refRelated *v1.ObjectReference
	if related != nil {
		refRelated, err = reference.GetReference(recorder.scheme, related)
		if err != nil {
			// related 对象失败只降级为 nil，不影响主事件上报。
			logger.V(9).Info("Could not construct reference", "object", related, "err", err)
		}
	}
	if !util.ValidateEventType(eventtype) {
		logger.Error(nil, "Unsupported event type", "eventType", eventtype)
		return
	}
	event := recorder.makeEvent(refRegarding, refRelated, timestamp, annotations, eventtype, reason, message, recorder.reportingController, recorder.reportingInstance, action)
	// 放到 goroutine 中发送，使记录事件对调用方非阻塞；
	// Broadcaster 自身有队列上限，过载时会丢弃而不是无限堆积。
	go func() {
		defer utilruntime.HandleCrash()
		recorder.Action(watch.Added, event)
	}()
}

// makeEvent 组装 events/v1.Event。
// 事件与目标对象位于同一 namespace；目标无 namespace 时落到 default。
func (recorder *recorderImpl) makeEvent(refRegarding *v1.ObjectReference, refRelated *v1.ObjectReference, timestamp metav1.MicroTime, annotations map[string]string, eventtype, reason, message string, reportingController string, reportingInstance string, action string) *eventsv1.Event {
	t := metav1.Time{Time: recorder.clock.Now()}
	namespace := refRegarding.Namespace
	if namespace == "" {
		namespace = metav1.NamespaceDefault
	}
	return &eventsv1.Event{
		ObjectMeta: metav1.ObjectMeta{
			// 名称由对象名 + 纳秒时间戳生成，保证同一对象的高频事件也互不覆盖。
			Name:        util.GenerateEventName(refRegarding.Name, t.UnixNano()),
			Namespace:   namespace,
			Annotations: annotations,
		},
		EventTime:           timestamp,
		Series:              nil,
		ReportingController: reportingController,
		ReportingInstance:   reportingInstance,
		Action:              action,
		Reason:              reason,
		Regarding:           *refRegarding,
		Related:             refRelated,
		Note:                message,
		Type:                eventtype,
	}
}
