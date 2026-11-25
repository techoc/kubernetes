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
	"context"

	eventsv1 "k8s.io/api/events/v1"
	"k8s.io/apimachinery/pkg/runtime"
	internalevents "k8s.io/client-go/tools/internal/events"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
)

// EventRecorder 记录事件；别名指向 tools/internal/events，保证新旧入口类型
// 完全一致。已弃用的是旧事件实现，此接口仍是推荐入口。
type EventRecorder = internalevents.EventRecorder

// EventRecorderLogger 是带 logger 的 EventRecorder，可附带上下文相关的日志
// 字段。推荐新代码使用。
type EventRecorderLogger = internalevents.EventRecorderLogger

// AnnotatedEventRecorder 支持在事件上附加注解，例如记录触发源的额外信息。
type AnnotatedEventRecorder = internalevents.AnnotatedEventRecorder

// EventBroadcaster 知道如何接收事件并发送到任意 EventSink、watcher 或日志。
type EventBroadcaster interface {
	// StartRecordingToSink 开始把指定 eventBroadcaster 收到的事件发送出去。
	// Deprecated: 请改用 StartRecordingToSinkWithContext。旧接口无法感知
	// context 取消，新代码应使用带 Context 的版本。
	StartRecordingToSink(stopCh <-chan struct{})

	// StartRecordingToSinkWithContext 开始把指定 eventBroadcaster 收到的事件发送出去。
	// 返回 error 而不是静默失败，便于调用方决定是否继续记录事件。
	StartRecordingToSinkWithContext(ctx context.Context) error

	// NewRecorder 返回一个 EventRecorder，可把事件发送到该 EventBroadcaster，
	// 并将事件上报组件（reportingController）设置为给定值。
	NewRecorder(scheme *runtime.Scheme, reportingController string) EventRecorderLogger

	// StartEventWatcher 让你在不使用 StartRecordingToSink 的情况下观察发出的事件，
	// 从而以自定义方式处理事件（例如在测试中）。
	// 注意：eventHandler 收到的事件在使用前应先复制。
	// TODO: 弄清该接口是否可以移除。
	StartEventWatcher(eventHandler func(event runtime.Object)) (func(), error)

	// StartStructuredLogging 开始把该 EventBroadcaster 收到的事件发送给结构化日志函数。
	// 返回值可以忽略，也可用于按需停止记录。
	// Deprecated: 请改用 StartLogging。
	StartStructuredLogging(verbosity klog.Level) func()

	// StartLogging 开始把该 EventBroadcaster 收到的事件发送给结构化 logger。
	// 若要调整日志级别，请使用 logger 的 V 方法（即传入 `logger.V(3)` 而不是 `logger`）。
	// 返回的函数可以忽略，也可用于按需停止记录。
	StartLogging(logger klog.Logger) (func(), error)

	// Shutdown 关闭 broadcaster。
	// 关闭后不再接收新事件；未 Start 的 broadcaster 调用应为幂等。
	Shutdown()
}

// EventSink 知道如何存储事件（client-go 实现了它）。
// EventSink 必须遵守 'event' 中嵌入的 namespace；
// 并假定 EventSink 会返回与 client-go REST 客户端同类的错误。
type EventSink interface {
	// Create 持久化一个新事件。
	Create(ctx context.Context, event *eventsv1.Event) (*eventsv1.Event, error)
	// Update 覆盖已有事件，通常用于聚合计数。
	Update(ctx context.Context, event *eventsv1.Event) (*eventsv1.Event, error)
	// Patch 对事件做部分更新；data 是已序列化的 patch 内容。
	Patch(ctx context.Context, oldEvent *eventsv1.Event, data []byte) (*eventsv1.Event, error)
}

// EventBroadcasterAdapter 是用于简化向新事件 API 迁移的辅助接口，
// 它包装了新旧两个 broadcaster 并智能选择使用哪一个。
//
// Deprecated: 迁移完成后该接口将被移除。
type EventBroadcasterAdapter interface {
	// StartRecordingToSink 开始把指定 eventBroadcaster 收到的事件发送出去。
	StartRecordingToSink(stopCh <-chan struct{})

	// NewRecorder 创建一个使用指定名称的新事件记录器。
	NewRecorder(name string) EventRecorderLogger

	// DeprecatedNewLegacyRecorder 创建一个使用指定名称的旧版事件记录器。
	// 仅供尚未迁移到新 events API 的调用方使用，事件会走 core/v1 路径。
	DeprecatedNewLegacyRecorder(name string) record.EventRecorderLogger

	// Shutdown 关闭 broadcaster。
	Shutdown()
}
