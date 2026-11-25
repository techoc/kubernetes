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

// Package internal 的存在纯粹是为了切断导入环：record.EventRecorderAdapter
// 需要这份接口定义才能实现它，但 event.NewEventBroadcasterAdapter 又依赖
// record.NewBroadcaster，因此这个接口不能放在 event/interfaces.go 里。
package internal

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
)

// EventRecorder 知道如何代表 EventSource 记录事件。
type EventRecorder interface {
	// Eventf 根据给定信息构造一个事件并放入发送队列。
	// 'regarding' 是此事件所指向的对象。事件会为其创建引用——也可以直接传入对象的引用。
	// 'related' 是面向更复杂操作的次要对象，例如 regarding 对象触发了 related 对象的创建或删除。
	// 'type' 是事件类型，只能是 Normal 或 Warning 之一，未来可能增加新类型。
	// 'reason' 是生成此事件的原因，应简短且唯一，采用 UpperCamelCase 格式（首字母大写）。
	// "reason" 会被用于事件的自动化处理，可以想象人们会为它编写 switch 语句，
	// 所以要让它易于使用。
	// 'action' 解释 regarding 上发生了什么、ReportingController 以 regarding 的名字
	// 采取了什么动作（ReportingController 指上报事件的控制器类型，
	// 例如 k8s.io/node-controller、k8s.io/kubelet），应采用 UpperCamelCase 格式（首字母大写）。
	// 'note' 面向人类可读。
	//
	// 注意 reason/action 的命名会进入自动化管道，保持稳定的 UpperCamelCase
	// 比措辞优雅更重要；note 才是自由文本。
	Eventf(regarding runtime.Object, related runtime.Object, eventtype, reason, action, note string, args ...interface{})
}

// AnnotatedEventRecorder 是 EventRecorder 的可选扩展，
// 支持在创建事件时为事件附加注解。
// 注解用于低频的相关性关联元数据，而不是通用的结构化数据通道。
// Kubernetes 事件是尽力而为的补充性可观测数据，可能在管道任意环节被丢弃、去重或聚合，
// 不要依赖注解来做事件驱动的控制流。
type AnnotatedEventRecorder interface {
	// AnnotatedEventf 与 Eventf 相同，但允许在创建事件时附加注解元数据。
	AnnotatedEventf(regarding runtime.Object, related runtime.Object, annotations map[string]string, eventtype, reason, action, note string, args ...interface{})
}

// EventRecorderLogger 扩展 EventRecorder，使其可以为 EventRecorder 中的
// 方法设置日志记录器。通常这些方法使用全局默认记录器来记录错误和调试消息；
// 如果不希望如此，可用 WithLogger 提供一个 logger 实例。
type EventRecorderLogger interface {
	EventRecorder

	// WithLogger 替换用于日志的上下文。这是一个廉价调用，用于上下文日志：
	//    recorder := ...
	//    logger := klog.FromContext(ctx)
	//    recorder.WithLogger(logger).Eventf(...)
	//
	// 返回新的记录器而非原地修改，因此可安全地在并发路径上派生不同 logger。
	WithLogger(logger klog.Logger) EventRecorderLogger
}
