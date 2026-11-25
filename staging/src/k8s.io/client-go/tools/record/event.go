/*
Copyright 2014 The Kubernetes Authors.

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
	"context"
	"fmt"
	"math/rand"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/watch"
	restclient "k8s.io/client-go/rest"
	internalevents "k8s.io/client-go/tools/internal/events"
	"k8s.io/client-go/tools/record/util"
	ref "k8s.io/client-go/tools/reference"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

const maxTriesPerEvent = 12

var defaultSleepDuration = 10 * time.Second

const maxQueuedEvents = 1000

// EventSink 知道如何存储事件（client.Client 实现了它）。
// EventSink 必须遵守 'event' 中嵌入的 namespace；
// 并假定 EventSink 会返回与 pkg/client REST 客户端同类的错误。
//
// 名称里虽只有 Create/Update/Patch，实现方通常直接包一层 typed client 的 Events 接口。
type EventSink interface {
	Create(event *v1.Event) (*v1.Event, error)
	Update(event *v1.Event) (*v1.Event, error)
	Patch(oldEvent *v1.Event, data []byte) (*v1.Event, error)
}

// CorrelatorOptions 允许修改 EventCorrelator 中 EventSourceObjectSpamFilter
// 和 EventAggregator 的默认配置。
//
// 零值字段会由 EventCorrelator 的构造函数补上默认值，调用方只需覆盖关心的项。
type CorrelatorOptions struct {
	// LRUCacheSize 是 EventSourceObjectSpamFilter 和 EventAggregator 共用的 LRU 缓存大小。
	// 未指定（零值）时使用 events_cache.go 中定义的默认值，
	// 也就是说 LRUCacheSize 必须大于 0。
	LRUCacheSize int
	// BurstSize 是 EventSourceObjectSpamFilter 中令牌桶限速的突发容量。
	// 未指定（零值）时使用 events_cache.go 中定义的默认值，
	// 也就是说 BurstSize 必须大于 0。
	BurstSize int
	// QPS 是 EventSourceObjectSpamFilter 中令牌桶的填充速率（每秒查询数）。
	// 未指定（零值）时使用 events_cache.go 中定义的默认值，
	// 也就是说 QPS 必须大于 0。
	QPS float32
	// KeyFunc 是 EventAggregator 用于对事件 key 分组聚合的函数。
	// 未指定（零值）时使用 EventAggregatorByReasonFunc
	KeyFunc EventAggregatorKeyFunc
	// MessageFunc 是 EventAggregator 用于生成聚合消息的函数。
	// 未指定（零值）时使用 EventAggregatorByReasonMessageFunc
	MessageFunc EventAggregatorMessageFunc
	// MaxEvents 是 EventAggregator 发生聚合前，一个区间内需要积累的事件数。
	// 未指定（零值）时使用 events_cache.go 中定义的默认值，
	// 也就是说 MaxEvents 必须大于 0
	MaxEvents int
	// MaxIntervalInSeconds 是 EventAggregator 把相似事件视为新事件前，距离上次相似事件出现必须经过的秒数。
	// 未指定（零值）时使用 events_cache.go 中定义的默认值，
	// 也就是说 MaxIntervalInSeconds 必须大于 0
	MaxIntervalInSeconds int
	// Clock 是 EventAggregator 使用的时钟，便于测试。
	// 未指定（零值）时使用 clock.RealClock{}
	Clock clock.PassiveClock
	// SpamKeyFunc 是 EventFilterFunc 使用的函数：为给定事件返回 key，并据此过滤。
	// 未指定（零值）时使用 getSpamKey
	SpamKeyFunc EventSpamKeyFunc
}

// EventRecorder 知道如何代表某个 EventSource 记录事件。
type EventRecorder interface {
	// Event 根据给定信息构造一个事件并放入待发送队列。
	// 'object' 是该事件关联的对象。Event 会自动生成引用——也可以直接
	// 传入对象引用。
	// 'eventtype' 是事件类型，可取 Normal 或 Warning，未来可能新增类型。
	// 'reason' 是产生该事件的原因，应简短且唯一，使用 UpperCamelCase 格式
	// （首字母大写）。"reason" 会被用于事件的自动化处理，可以想象人们会为
	// 它编写 switch 语句——请让这件事变得简单。
	// 'message' 面向人类可读。
	//
	// 生成的事件将被创建在引用对象所在的命名空间中。
	Event(object runtime.Object, eventtype, reason, message string)

	// Eventf 与 Event 相同，只是 message 字段使用 Sprintf 格式化。
	Eventf(object runtime.Object, eventtype, reason, messageFmt string, args ...interface{})

	// AnnotatedEventf 与 Eventf 相同，但会附加 annotations
	AnnotatedEventf(object runtime.Object, annotations map[string]string, eventtype, reason, messageFmt string, args ...interface{})
}

// EventRecorderLogger 扩展了 EventRecorder，使其方法可以设置 logger。
// 默认情况下，这些方法使用全局默认 logger 记录错误和调试信息；
// 如不希望如此，可用 WithLogger 提供 logger 实例。
type EventRecorderLogger interface {
	EventRecorder

	// WithLogger 替换用于日志的上下文。这是一个很轻的调用，适用于上下文日志：
	//    recorder := ...
	//    logger := klog.FromContext(ctx)
	//    recorder.WithLogger(logger).Eventf(...)
	WithLogger(logger klog.Logger) EventRecorderLogger
}

// EventBroadcaster 知道如何接收事件并发送到任意 EventSink、watcher 或日志。
type EventBroadcaster interface {
	// StartEventWatcher 开始把该 EventBroadcaster 收到的事件发送给指定的事件处理函数。
	// 返回值可以忽略，也可用于按需停止记录。
	StartEventWatcher(eventHandler func(*v1.Event)) watch.Interface

	// StartRecordingToSink 开始把该 EventBroadcaster 收到的事件发送给指定的 sink。
	// 返回值可以忽略，也可用于按需停止记录。
	StartRecordingToSink(sink EventSink) watch.Interface

	// StartLogging 开始把该 EventBroadcaster 收到的事件发送给指定的日志函数。
	// 返回值可以忽略，也可用于按需停止记录。
	StartLogging(logf func(format string, args ...interface{})) watch.Interface

	// StartStructuredLogging 开始把该 EventBroadcaster 收到的事件发送给结构化日志函数。
	// 返回值可以忽略，也可用于按需停止记录。
	StartStructuredLogging(verbosity klog.Level) watch.Interface

	// NewRecorder 返回一个 EventRecorder，可把事件发送到该 EventBroadcaster，
	// 并将事件源设置为给定的 event source。
	NewRecorder(scheme *runtime.Scheme, source v1.EventSource) EventRecorderLogger

	// Shutdown 关闭 broadcaster。关闭后，事件只会尝试写入 sink 一次，
	// 失败即带错误信息放弃。
	Shutdown()
}

// EventRecorderAdapter 包装了 "k8s.io/client-go/tools/record".EventRecorder，
// 实现新的 "k8s.io/client-go/tools/events".EventRecorder 接口。
//
// 用于让仍只支持 events/v1 的调用方复用旧的 core/v1 事件记录器。
type EventRecorderAdapter struct {
	recorder EventRecorderLogger
}

var _ internalevents.EventRecorder = &EventRecorderAdapter{}

// NewEventRecorderAdapter 返回一个实现了新的
// "k8s.io/client-go/tools/events".EventRecorder 接口的适配器。
func NewEventRecorderAdapter(recorder EventRecorderLogger) *EventRecorderAdapter {
	return &EventRecorderAdapter{
		recorder: recorder,
	}
}

// Eventf 是对 v1 Eventf 的包装
func (a *EventRecorderAdapter) Eventf(regarding, _ runtime.Object, eventtype, reason, action, note string, args ...interface{}) {
	a.recorder.Eventf(regarding, eventtype, reason, note, args...)
}

// AnnotatedEventf 是对 v1 AnnotatedEventf 的包装
func (a *EventRecorderAdapter) AnnotatedEventf(regarding, _ runtime.Object, annotations map[string]string, eventtype, reason, action, note string, args ...interface{}) {
	//nolint:forbidigo // Legacy usage
	a.recorder.AnnotatedEventf(regarding, annotations, eventtype, reason, note, args...)
}

func (a *EventRecorderAdapter) WithLogger(logger klog.Logger) internalevents.EventRecorderLogger {
	return &EventRecorderAdapter{
		recorder: a.recorder.WithLogger(logger),
	}
}

// 创建一个新的 event broadcaster。
//
// 默认使用长度 1000、满则丢弃的 watch 队列：事件属于尽力而为的观测数据，
// 宁可丢弃也不能阻塞业务逻辑。
func NewBroadcaster(opts ...BroadcasterOption) EventBroadcaster {
	c := config{
		sleepDuration: defaultSleepDuration,
	}
	for _, opt := range opts {
		opt(&c)
	}
	eventBroadcaster := &eventBroadcasterImpl{
		Broadcaster:   watch.NewLongQueueBroadcaster(maxQueuedEvents, watch.DropIfChannelFull),
		sleepDuration: c.sleepDuration,
		options:       c.CorrelatorOptions,
	}
	ctx := c.Context
	if ctx == nil {
		ctx = context.Background()
	}
	// 有两种场景下等待 context 取消没有意义：
	// - context 为 nil。
	// - context 一开始就是 context.Background()。
	//
	// 这里会同时检查这两种情况：当且仅当存在取消 channel 时才认为可取消。
	haveCtxCancelation := ctx.Done() != nil

	eventBroadcaster.cancelationCtx, eventBroadcaster.cancel = context.WithCancel(ctx)

	if haveCtxCancelation {
		// 提供了 context 时无需显式调用 Shutdown：context 被取消时，
		// 这个 goroutine 会关闭 broadcaster。
		//
		// 如果先调用了 Shutdown，这个 goroutine 同样会停止。
		go func() {
			<-eventBroadcaster.cancelationCtx.Done()
			eventBroadcaster.Broadcaster.Shutdown()
		}()
	}

	return eventBroadcaster
}

func NewBroadcasterForTests(sleepDuration time.Duration) EventBroadcaster {
	return NewBroadcaster(WithSleepDuration(sleepDuration))
}

func NewBroadcasterWithCorrelatorOptions(options CorrelatorOptions) EventBroadcaster {
	return NewBroadcaster(WithCorrelatorOptions(options))
}

func WithCorrelatorOptions(options CorrelatorOptions) BroadcasterOption {
	return func(c *config) {
		c.CorrelatorOptions = options
	}
}

// WithContext 为 broadcaster 设置 context。取消该 context 即会关闭 broadcaster，
// 无需再调用 Shutdown。context 也可以用于提供 logger。
func WithContext(ctx context.Context) BroadcasterOption {
	return func(c *config) {
		c.Context = ctx
	}
}

func WithSleepDuration(sleepDuration time.Duration) BroadcasterOption {
	return func(c *config) {
		c.sleepDuration = sleepDuration
	}
}

type BroadcasterOption func(*config)

type config struct {
	CorrelatorOptions
	context.Context
	sleepDuration time.Duration
}

type eventBroadcasterImpl struct {
	*watch.Broadcaster
	sleepDuration  time.Duration
	options        CorrelatorOptions
	cancelationCtx context.Context
	cancel         func()
}

// StartRecordingToSink 开始把指定 eventBroadcaster 收到的事件发送给给定的 sink。
// 返回值可以忽略，也可用于按需停止记录。
// TODO: 把我改造成一个可配置队列长度与重试间隔的对象
func (e *eventBroadcasterImpl) StartRecordingToSink(sink EventSink) watch.Interface {
	eventCorrelator := NewEventCorrelatorWithOptions(e.options)
	return e.StartEventWatcher(
		func(event *v1.Event) {
			e.recordToSink(sink, event, eventCorrelator)
		})
}

func (e *eventBroadcasterImpl) Shutdown() {
	e.Broadcaster.Shutdown()
	e.cancel()
}

func (e *eventBroadcasterImpl) recordToSink(sink EventSink, event *v1.Event, eventCorrelator *EventCorrelator) {
	// 修改前先复制一份，因为可能有多个监听器；事件这样直接复制是安全的。
	//
	// 同一事件会先经过 correlation/aggregation 再写 sink，
	// 复制一份可避免修改广播给其他监听器看到的原始对象。
	eventCopy := *event
	event = &eventCopy
	result, err := eventCorrelator.EventCorrelate(event)
	if err != nil {
		utilruntime.HandleErrorWithContext(e.cancelationCtx, err, "Event correlation failed")
	}
	if result.Skip {
		return
	}
	tries := 0
	for {
		if recordEvent(e.cancelationCtx, sink, result.Event, result.Patch, result.Event.Count > 1, eventCorrelator) {
			break
		}
		tries++
		if tries >= maxTriesPerEvent {
			klog.FromContext(e.cancelationCtx).Error(nil, "Unable to write event (retry limit exceeded!)", "event", event)
			break
		}

		// 首次休眠使用随机时长，避免 master 宕机时所有客户端节奏一致。
		//
		// 首次重试使用 [0, sleepDuration) 的随机延迟，避免大量客户端在
		// apiserver 恢复瞬间同时重试造成惊群。
		delay := e.sleepDuration
		if tries == 1 {
			delay = time.Duration(float64(delay) * rand.Float64())
		}
		select {
		case <-e.cancelationCtx.Done():
			klog.FromContext(e.cancelationCtx).Error(nil, "Unable to write event (broadcaster is shut down)", "event", event)
			return
		case <-time.After(delay):
		}
	}
}

// recordEvent 尝试把 event 写入 sink。事件已成功记录或被丢弃时返回 true，
// 需要重试时返回 false。updateExistingEvent 为 false 时创建新事件，
// 否则更新已有事件。
func recordEvent(ctx context.Context, sink EventSink, event *v1.Event, patch []byte, updateExistingEvent bool, eventCorrelator *EventCorrelator) bool {
	var newEvent *v1.Event
	var err error
	if updateExistingEvent {
		newEvent, err = sink.Patch(event, patch)
	}
	// Update 可能失败，因为事件可能已被删除、不再存在。
	// 聚合事件可能已被 TTL 清理，Patch 返回 NotFound 时退回 Create，
	// 并清空 ResourceVersion，否则服务端会拒绝带旧 RV 的创建请求。
	if !updateExistingEvent || util.IsKeyNotFoundError(err) {
		// 确保创建时 ResourceVersion 为空
		event.ResourceVersion = ""
		newEvent, err = sink.Create(event)
	}
	if err == nil {
		// 需要用服务端返回的状态更新 event correlator，以处理 name/resourceversion
		eventCorrelator.UpdateState(newEvent)
		return true
	}

	// 如果无法联系服务端，就保留一切并重试。
	// 否则说明事件本身有问题，应当放弃它。
	switch err.(type) {
	case *restclient.RequestConstructionError:
		// 下次构造的请求也会一样，因此不再重试。
		klog.FromContext(ctx).Error(err, "Unable to construct event (will not retry!)", "event", event)
		return true
	case *errors.StatusError:
		// 服务端明确拒绝（已存在、namespace 正在终止等）属于确定性错误，
		// 重试无意义，但也不应阻塞后续事件。
		if errors.IsAlreadyExists(err) || errors.HasStatusCause(err, v1.NamespaceTerminatingCause) {
			klog.FromContext(ctx).V(5).Info("Server rejected event (will not retry!)", "event", event, "err", err)
		} else {
			klog.FromContext(ctx).Error(err, "Server rejected event (will not retry!)", "event", event)
		}
		return true
	case *errors.UnexpectedObjectError:
		// 这种情况不常见：说明服务端的响应不符合已知模式。继续重试。
	default:
		// 该分支包含实际的 HTTP 传输错误。继续重试。
	}
	klog.FromContext(ctx).Error(err, "Unable to write event (may retry after sleeping)", "event", event)
	return false
}

// StartLogging 开始把该 EventBroadcaster 收到的事件发送给指定的日志函数。
// 返回值可以忽略，也可用于按需停止记录。
func (e *eventBroadcasterImpl) StartLogging(logf func(format string, args ...interface{})) watch.Interface {
	return e.StartEventWatcher(
		func(e *v1.Event) {
			logf("Event(%#v): type: '%v' reason: '%v' %v", e.InvolvedObject, e.Type, e.Reason, e.Message)
		})
}

// StartStructuredLogging 开始把该 EventBroadcaster 收到的事件发送给结构化 logger。
// 若 broadcaster 构造时带有 context，则从 context 中获取 logger，否则使用全局默认 logger。
// 返回值可以忽略，也可用于按需停止记录。
func (e *eventBroadcasterImpl) StartStructuredLogging(verbosity klog.Level) watch.Interface {
	loggerV := klog.FromContext(e.cancelationCtx).V(int(verbosity))
	return e.StartEventWatcher(
		func(e *v1.Event) {
			loggerV.Info("Event occurred", "object", klog.KRef(e.InvolvedObject.Namespace, e.InvolvedObject.Name), "fieldPath", e.InvolvedObject.FieldPath, "kind", e.InvolvedObject.Kind, "apiVersion", e.InvolvedObject.APIVersion, "type", e.Type, "reason", e.Reason, "message", e.Message)
		})
}

// StartEventWatcher 开始把该 EventBroadcaster 收到的事件发送给指定的事件处理函数。
// 返回值可以忽略，也可用于按需停止记录。
func (e *eventBroadcasterImpl) StartEventWatcher(eventHandler func(*v1.Event)) watch.Interface {
	watcher, err := e.Watch()
	if err != nil {
		// 这个函数传统上不返回错误，即使它可能失败。这里改为记录错误并
		// 返回一个空的 watch，确保调用方在调用 Stop 时不会崩溃。
		klog.FromContext(e.cancelationCtx).Error(err, "Unable start event watcher (will not retry!)")
		return watch.NewEmptyWatch()
	}
	go func() {
		defer utilruntime.HandleCrashWithContext(e.cancelationCtx)
		for {
			select {
			case <-e.cancelationCtx.Done():
				watcher.Stop()
				return
			case watchEvent, ok := <-watcher.ResultChan():
				if !ok {
					// Broadcaster 已关闭；必须退出，否则会在此处忙等已关闭的 channel。
					return
				}
				event, ok := watchEvent.Object.(*v1.Event)
				if !ok {
					// 数据全部来自本地，理论上不应该出现这种情况。
					continue
				}
				eventHandler(event)
			}
		}
	}()
	return watcher
}

// NewRecorder 返回一个 EventRecorder，用给定的事件源记录事件。
func (e *eventBroadcasterImpl) NewRecorder(scheme *runtime.Scheme, source v1.EventSource) EventRecorderLogger {
	return &recorderImplLogger{recorderImpl: &recorderImpl{scheme, source, e.Broadcaster, clock.RealClock{}}, logger: klog.Background()}
}

type recorderImpl struct {
	scheme *runtime.Scheme
	source v1.EventSource
	*watch.Broadcaster
	clock clock.PassiveClock
}

var _ EventRecorder = &recorderImpl{}

func (recorder *recorderImpl) generateEvent(logger klog.Logger, object runtime.Object, annotations map[string]string, eventtype, reason, message string) {
	// 事件引用必须能在 recorder 的 scheme 中解析，否则调用方传入了未注册类型。
	ref, err := ref.GetReference(recorder.scheme, object)
	if err != nil {
		logger.Error(err, "Could not construct reference, will not report event", "object", object, "eventType", eventtype, "reason", reason, "message", message)
		return
	}

	if !util.ValidateEventType(eventtype) {
		logger.Error(nil, "Unsupported event type", "eventType", eventtype)
		return
	}

	event := recorder.makeEvent(ref, annotations, eventtype, reason, message)
	event.Source = recorder.source

	event.ReportingInstance = recorder.source.Host
	event.ReportingController = recorder.source.Component

	// 注意：记录事件应当是非阻塞操作，但也不能放到 goroutine 里，
	// 否则关闭 broadcaster 时会与向已关闭 channel 的写操作产生竞争。
	// 过载时直接丢弃事件并记录错误（我们本来就配置了 broadcaster
	// 丢弃外发事件）。
	//
	// ActionOrDrop 内部不会阻塞：队列满时返回 sent=false，由这里记日志。
	sent, err := recorder.ActionOrDrop(watch.Added, event)
	if err != nil {
		logger.Error(err, "Unable to record event (will not retry!)")
		return
	}
	if !sent {
		logger.Error(nil, "Unable to record event: too many queued events, dropped event", "event", event)
	}
}

func (recorder *recorderImpl) Event(object runtime.Object, eventtype, reason, message string) {
	recorder.generateEvent(klog.Background(), object, nil, eventtype, reason, message)
}

func (recorder *recorderImpl) Eventf(object runtime.Object, eventtype, reason, messageFmt string, args ...interface{}) {
	recorder.Event(object, eventtype, reason, fmt.Sprintf(messageFmt, args...))
}

func (recorder *recorderImpl) AnnotatedEventf(object runtime.Object, annotations map[string]string, eventtype, reason, messageFmt string, args ...interface{}) {
	recorder.generateEvent(klog.Background(), object, annotations, eventtype, reason, fmt.Sprintf(messageFmt, args...))
}

func (recorder *recorderImpl) makeEvent(ref *v1.ObjectReference, annotations map[string]string, eventtype, reason, message string) *v1.Event {
	t := metav1.Time{Time: recorder.clock.Now()}
	namespace := ref.Namespace
	if namespace == "" {
		namespace = metav1.NamespaceDefault
	}
	return &v1.Event{
		ObjectMeta: metav1.ObjectMeta{
			// 名称包含纳秒时间戳，避免同一对象短时间内产生同名事件。
			Name:        util.GenerateEventName(ref.Name, t.UnixNano()),
			Namespace:   namespace,
			Annotations: annotations,
		},
		InvolvedObject: *ref,
		Reason:         reason,
		Message:        message,
		FirstTimestamp: t,
		LastTimestamp:  t,
		Count:          1,
		Type:           eventtype,
	}
}

type recorderImplLogger struct {
	*recorderImpl
	logger klog.Logger
}

var _ EventRecorderLogger = &recorderImplLogger{}

func (recorder recorderImplLogger) Event(object runtime.Object, eventtype, reason, message string) {
	recorder.recorderImpl.generateEvent(recorder.logger, object, nil, eventtype, reason, message)
}

func (recorder recorderImplLogger) Eventf(object runtime.Object, eventtype, reason, messageFmt string, args ...interface{}) {
	recorder.Event(object, eventtype, reason, fmt.Sprintf(messageFmt, args...))
}

func (recorder recorderImplLogger) AnnotatedEventf(object runtime.Object, annotations map[string]string, eventtype, reason, messageFmt string, args ...interface{}) {
	recorder.generateEvent(recorder.logger, object, annotations, eventtype, reason, fmt.Sprintf(messageFmt, args...))
}

func (recorder recorderImplLogger) WithLogger(logger klog.Logger) EventRecorderLogger {
	return recorderImplLogger{recorderImpl: recorder.recorderImpl, logger: logger}
}
