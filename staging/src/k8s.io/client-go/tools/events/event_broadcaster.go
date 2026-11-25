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
	"fmt"
	"os"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedv1core "k8s.io/client-go/kubernetes/typed/core/v1"
	typedeventsv1 "k8s.io/client-go/kubernetes/typed/events/v1"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/tools/record/util"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

const (
	// 单条事件最多尝试写入的次数，超过后丢弃，避免持续占用 goroutine。
	maxTriesPerEvent = 12
	// 一个事件序列在该时长内没有新事件后视为结束，写入最终计数。
	finishTime = 6 * time.Minute
	// 周期性刷新仍活跃的事件序列，以更新服务端的 TTL 时间戳。
	refreshTime = 30 * time.Minute
	// 广播通道容量；打满时丢弃事件而不是阻塞生产者。
	maxQueuedEvents = 1000
)

var defaultSleepDuration = 10 * time.Second

// TODO: 验证复制开销的影响，并调研改用哈希的方案
// eventKey 是事件聚合的等价键：同一键的事件被视为“同构”，只累加 Series 计数。
// 刻意不包含 Note 与时间等字段，使重复上报的同一事件能够合并。
type eventKey struct {
	eventType           string
	action              string
	reason              string
	reportingController string
	reportingInstance   string
	regarding           corev1.ObjectReference
	related             corev1.ObjectReference
}

// eventBroadcasterImpl 是 EventBroadcaster 的实现。
//
// eventCache 保存“已上报但尚未结束”的同构事件，用于把高频重复事件聚合为
// Series；所有对该 map 的访问都必须持有 mu。
type eventBroadcasterImpl struct {
	*watch.Broadcaster
	mu            sync.Mutex
	eventCache    map[eventKey]*eventsv1.Event
	sleepDuration time.Duration
	sink          EventSink
}

// EventSinkImpl 包装 EventsV1Interface 以实现 EventSink。
// TODO: 这样更便于测试，也隐藏了发起 API 调用的细节；
// 注意即使回退到原始 clientset 也应对调用方透明。
// EventSinkImpl 把 events/v1 类型化客户端适配为 EventSink。namespace 是
// 事件归属的唯一路由依据，因此下面三个方法都拒绝空 namespace。
type EventSinkImpl struct {
	Interface typedeventsv1.EventsV1Interface
}

// Create 接受事件的表示并创建它，返回服务端的表示，出错时返回错误。
func (e *EventSinkImpl) Create(ctx context.Context, event *eventsv1.Event) (*eventsv1.Event, error) {
	if event.Namespace == "" {
		return nil, fmt.Errorf("can't create an event with empty namespace")
	}
	return e.Interface.Events(event.Namespace).Create(ctx, event, metav1.CreateOptions{})
}

// Update 接受事件的表示并更新它，返回服务端的表示，出错时返回错误。
func (e *EventSinkImpl) Update(ctx context.Context, event *eventsv1.Event) (*eventsv1.Event, error) {
	if event.Namespace == "" {
		return nil, fmt.Errorf("can't update an event with empty namespace")
	}
	return e.Interface.Events(event.Namespace).Update(ctx, event, metav1.UpdateOptions{})
}

// Patch 应用补丁并返回打补丁后的事件，出错时返回错误。
//
// 固定使用 strategic merge patch：Series 字段的计数合并依赖服务端的合并语义。
func (e *EventSinkImpl) Patch(ctx context.Context, event *eventsv1.Event, data []byte) (*eventsv1.Event, error) {
	if event.Namespace == "" {
		return nil, fmt.Errorf("can't patch an event with empty namespace")
	}
	return e.Interface.Events(event.Namespace).Patch(ctx, event.Name, types.StrategicMergePatchType, data, metav1.PatchOptions{})
}

// NewBroadcaster 创建一个新的 event broadcaster。
func NewBroadcaster(sink EventSink) EventBroadcaster {
	return newBroadcaster(sink, defaultSleepDuration, map[eventKey]*eventsv1.Event{})
}

// newBroadcaster 创建一个新的 event broadcaster，供测试使用。
//
// 与 NewBroadcaster 的唯一区别是允许注入 sleepDuration 与初始缓存，
// 便于测试控制重试节奏和预置聚合状态。
func newBroadcaster(sink EventSink, sleepDuration time.Duration, eventCache map[eventKey]*eventsv1.Event) EventBroadcaster {
	return &eventBroadcasterImpl{
		Broadcaster:   watch.NewBroadcaster(maxQueuedEvents, watch.DropIfChannelFull),
		eventCache:    eventCache,
		sleepDuration: sleepDuration,
		sink:          sink,
	}
}

// Shutdown 停止广播并等待后台 goroutine 退出。
func (e *eventBroadcasterImpl) Shutdown() {
	e.Broadcaster.Shutdown()
}

// refreshExistingEventSeries 刷新事件的 TTL
//
// 服务端会清理长期未更新的事件；仍活跃的 Series 需要周期性 Patch 以刷新
// TTL，否则连续事件的计数可能因对象被删除而丢失。
func (e *eventBroadcasterImpl) refreshExistingEventSeries(ctx context.Context) {
	// TODO: 调查锁竞争是否会成为问题
	e.mu.Lock()
	defer e.mu.Unlock()
	for isomorphicKey, event := range e.eventCache {
		if event.Series != nil {
			if recordedEvent, retry := recordEvent(ctx, e.sink, event); !retry {
				if recordedEvent != nil {
					e.eventCache[isomorphicKey] = recordedEvent
				}
			}
		}
	}
}

// finishSeries 检查某个事件序列是否已结束，并：
// - 把最终计数写入 apiserver
// - 从缓存中删除单例事件（即 Series 字段为 nil）
//
// 只有写入成功（或不可重试地失败）才把条目移出缓存，否则保留到下一轮。
func (e *eventBroadcasterImpl) finishSeries(ctx context.Context) {
	// TODO: 调查锁竞争是否会成为问题
	e.mu.Lock()
	defer e.mu.Unlock()
	for isomorphicKey, event := range e.eventCache {
		eventSerie := event.Series
		if eventSerie != nil {
			if eventSerie.LastObservedTime.Time.Before(time.Now().Add(-finishTime)) {
				if _, retry := recordEvent(ctx, e.sink, event); !retry {
					delete(e.eventCache, isomorphicKey)
				}
			}
		} else if event.EventTime.Time.Before(time.Now().Add(-finishTime)) {
			delete(e.eventCache, isomorphicKey)
		}
	}
}

// NewRecorder 返回一个 EventRecorder，用给定的上报组件记录事件。
//
// reportingInstance 由 controller 名与主机名拼接，便于区分同一组件在不同
// 节点上的实例。
func (e *eventBroadcasterImpl) NewRecorder(scheme *runtime.Scheme, reportingController string) EventRecorderLogger {
	hostname, _ := os.Hostname()
	reportingInstance := reportingController + "-" + hostname
	return &recorderImplLogger{recorderImpl: &recorderImpl{scheme, reportingController, reportingInstance, e.Broadcaster, clock.RealClock{}}, logger: klog.Background()}
}

// recordToSink 把事件异步写入 sink，并在内存中做同构事件聚合。
//
// 聚合规则（均在 e.mu 保护下）：
//   - 首次出现：缓存对象并立即创建服务端事件；
//   - 第二次出现：把缓存对象升级为 Series（InitialCount=2）并 Patch 服务端；
//   - 之后每出现一次：仅递增 Count 与 LastObservedTime，不再写服务端。
//
// 返回 nil 表示本次无需写服务端。异步执行是为了不阻塞事件生产者。
func (e *eventBroadcasterImpl) recordToSink(ctx context.Context, event *eventsv1.Event, clock clock.Clock) {
	// 修改前先复制一份，因为可能有多个监听器。
	eventCopy := event.DeepCopy()
	go func() {
		evToRecord := func() *eventsv1.Event {
			e.mu.Lock()
			defer e.mu.Unlock()
			eventKey := getKey(eventCopy)
			isomorphicEvent, isIsomorphic := e.eventCache[eventKey]
			if isIsomorphic {
				if isomorphicEvent.Series != nil {
					isomorphicEvent.Series.Count++
					isomorphicEvent.Series.LastObservedTime = metav1.MicroTime{Time: clock.Now()}
					return nil
				}
				isomorphicEvent.Series = &eventsv1.EventSeries{
					Count:            2,
					LastObservedTime: metav1.MicroTime{Time: clock.Now()},
				}
				// 复制一份 Event，确保记录事件不会影响
				// 缓存中存放的对象。
				return isomorphicEvent.DeepCopy()
			}
			e.eventCache[eventKey] = eventCopy
			// 复制一份 Event，确保记录事件不会影响缓存中存放的对象。
			return eventCopy.DeepCopy()
		}()
		if evToRecord != nil {
			// TODO: 增加一个统计记录尝试次数的指标
			e.attemptRecording(ctx, evToRecord)
			// 不希望新记录的事件反映到客户端缓存中，
			// 因为服务端的修改可能扰乱客户端的聚合机制。
		}
	}()
}

// attemptRecording 按重试上限反复写入事件，直到成功、不可重试或上下文取消。
func (e *eventBroadcasterImpl) attemptRecording(ctx context.Context, event *eventsv1.Event) {
	tries := 0
	for {
		if _, retry := recordEvent(ctx, e.sink, event); !retry {
			return
		}
		tries++
		if tries >= maxTriesPerEvent {
			klog.FromContext(ctx).Error(nil, "Unable to write event (retry limit exceeded!)", "event", event)
			return
		}
		// 随机化休眠时长，避免 master 宕机时所有客户端节奏一致；
		// context 取消时放弃。
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait.Jitter(e.sleepDuration, 0.25)):
		}
	}
}

// recordEvent 执行一次写入，返回（服务端对象, 是否应重试）。
//
// 对 Series 事件优先 Patch；若对象已不存在（KeyNotFound）则退化为 Create。
// 错误分类决定是否重试：请求构造错误和多数 StatusError 不重试，网络类
// 错误与 UnexpectedObjectError 重试。
func recordEvent(ctx context.Context, sink EventSink, event *eventsv1.Event) (*eventsv1.Event, bool) {
	var newEvent *eventsv1.Event
	var err error
	isEventSeries := event.Series != nil
	if isEventSeries {
		patch, patchBytesErr := createPatchBytesForSeries(event)
		if patchBytesErr != nil {
			klog.FromContext(ctx).Error(patchBytesErr, "Unable to calculate diff, no merge is possible")
			return nil, false
		}
		newEvent, err = sink.Patch(ctx, event, patch)
	}
	// Update 可能失败，因为事件可能已被删除、不再存在。
	if !isEventSeries || util.IsKeyNotFoundError(err) {
		// 确保创建时 ResourceVersion 为空
		event.ResourceVersion = ""
		newEvent, err = sink.Create(ctx, event)
	}
	if err == nil {
		return newEvent, false
	}
	// 如果无法联系服务端，就保留一切并重试。
	// 否则说明事件本身有问题，应当放弃它。
	switch err.(type) {
	case *restclient.RequestConstructionError:
		// 下次构造的请求也会一样，因此不再重试。
		klog.FromContext(ctx).Error(err, "Unable to construct event (will not retry!)", "event", event)
		return nil, false
	case *errors.StatusError:
		if errors.IsAlreadyExists(err) {
			// 如果是在从 EventSeries 创建 Event，说明原来的 Patch 请求
			// 因为目标 Event 不存在而失败；若创建又因该 Event 已存在而失败，
			// 重试是安全的。这种情况发生在一个新事件在极短时间内被发出两次时。
			if isEventSeries {
				return nil, true
			}
			klog.FromContext(ctx).V(5).Info("Server rejected event (will not retry!)", "event", event, "err", err)
		} else {
			klog.FromContext(ctx).Error(err, "Server rejected event (will not retry!)", "event", event)
		}
		return nil, false
	case *errors.UnexpectedObjectError:
		// 这种情况不常见：说明服务端的响应不符合已知模式。继续重试。
	default:
		// 该分支包含实际的 HTTP 传输错误。继续重试。
	}
	klog.FromContext(ctx).Error(err, "Unable to write event (may retry after sleeping)")
	return nil, true
}

// createPatchBytesForSeries 计算“把事件从非 Series 状态变为带 Series 状态”
// 的两路合并 patch。旧对象复制后清空 Series，使 patch 只包含 Series 差异。
func createPatchBytesForSeries(event *eventsv1.Event) ([]byte, error) {
	oldEvent := event.DeepCopy()
	oldEvent.Series = nil
	oldData, err := json.Marshal(oldEvent)
	if err != nil {
		return nil, err
	}
	newData, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	return strategicpatch.CreateTwoWayMergePatch(oldData, newData, eventsv1.Event{})
}

// getKey 从事件中提取聚合键；Related 为 nil 时使用零值引用。
func getKey(event *eventsv1.Event) eventKey {
	key := eventKey{
		eventType:           event.Type,
		action:              event.Action,
		reason:              event.Reason,
		reportingController: event.ReportingController,
		reportingInstance:   event.ReportingInstance,
		regarding:           event.Regarding,
	}
	if event.Related != nil {
		key.related = *event.Related
	}
	return key
}

// StartStructuredLogging 开始把该 EventBroadcaster 收到的事件发送给结构化日志函数。
// 返回值可以忽略，也可用于按需停止记录。
// TODO: 该函数也应返回 error。
//
// Deprecated: 请改用 StartLogging，它支持传入带上下文的 logger。
func (e *eventBroadcasterImpl) StartStructuredLogging(verbosity klog.Level) func() {
	logger := klog.Background().V(int(verbosity))
	stopWatcher, err := e.StartLogging(logger)
	if err != nil {
		logger.Error(err, "Failed to start event watcher")
		return func() {}
	}
	return stopWatcher
}

// StartLogging 开始把该 EventBroadcaster 收到的事件发送给结构化 logger。
// 若要调整日志级别，请使用 logger 的 V 方法（即传入 `logger.V(3)` 而不是 `logger`）。
// 返回的函数可以忽略，也可用于按需停止记录。
//
// 日志输出不会阻塞事件处理；stop 函数用于显式停止监听。
func (e *eventBroadcasterImpl) StartLogging(logger klog.Logger) (func(), error) {
	return e.startEventWatcher(logger,
		func(obj runtime.Object) {
			event, ok := obj.(*eventsv1.Event)
			if !ok {
				logger.Error(nil, "unexpected type, expected eventsv1.Event")
				return
			}
			logger.Info("Event occurred", "object", klog.KRef(event.Regarding.Namespace, event.Regarding.Name), "kind", event.Regarding.Kind, "apiVersion", event.Regarding.APIVersion, "type", event.Type, "reason", event.Reason, "action", event.Action, "note", event.Note)
		})
}

// StartEventWatcher 开始把该 EventBroadcaster 收到的事件发送给指定的事件处理函数。
// 返回值用于停止记录
func (e *eventBroadcasterImpl) StartEventWatcher(eventHandler func(event runtime.Object)) (func(), error) {
	return e.startEventWatcher(klog.Background(), eventHandler)
}

// startEventWatcher 启动一个后台 goroutine 消费广播通道，直到通道关闭。
//
// 通过 watch.Broadcaster 实现一对多分发；每个 watcher 独立消费，因此
// 慢消费者只会丢自己的事件，不影响其他 watcher。
func (e *eventBroadcasterImpl) startEventWatcher(logger klog.Logger, eventHandler func(event runtime.Object)) (func(), error) {
	watcher, err := e.Watch()
	if err != nil {
		return nil, err
	}
	go func() {
		defer utilruntime.HandleCrashWithLogger(logger)
		for {
			watchEvent, ok := <-watcher.ResultChan()
			if !ok {
				return
			}
			eventHandler(watchEvent.Object)
		}
	}()
	return watcher.Stop, nil
}

// startRecordingEvents 订阅广播并把事件投递给 recordToSink。
//
// 停止订阅的 goroutine 绑定到 ctx，而不是 stopCh，便于统一用上下文取消。
func (e *eventBroadcasterImpl) startRecordingEvents(ctx context.Context) error {
	eventHandler := func(obj runtime.Object) {
		event, ok := obj.(*eventsv1.Event)
		if !ok {
			klog.FromContext(ctx).Error(nil, "unexpected type, expected eventsv1.Event")
			return
		}
		e.recordToSink(ctx, event, clock.RealClock{})
	}
	stopWatcher, err := e.StartEventWatcher(eventHandler)
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		stopWatcher()
	}()
	return nil
}

// StartRecordingToSink 开始把指定 eventBroadcaster 收到的事件发送给给定的 sink。
// Deprecated: 请改用 StartRecordingToSinkWithContext。
func (e *eventBroadcasterImpl) StartRecordingToSink(stopCh <-chan struct{}) {
	err := e.StartRecordingToSinkWithContext(wait.ContextForChannel(stopCh))
	if err != nil {
		klog.Background().Error(err, "Failed to start recording to sink")
	}
}

// StartRecordingToSinkWithContext 开始把指定 eventBroadcaster 收到的事件发送给给定的 sink。
//
// 同时启动两个周期任务：刷新活跃 Series 的 TTL，以及结束过期的 Series；
// 它们在 ctx 取消时自动停止。
func (e *eventBroadcasterImpl) StartRecordingToSinkWithContext(ctx context.Context) error {
	go wait.UntilWithContext(ctx, e.refreshExistingEventSeries, refreshTime)
	go wait.UntilWithContext(ctx, e.finishSeries, finishTime)
	return e.startRecordingEvents(ctx)
}

// eventBroadcasterAdapterImpl 同时持有 events/v1 与 core/v1 两套广播器，
// 在 API 迁移期间按服务端能力选择可用的实现。
type eventBroadcasterAdapterImpl struct {
	coreClient          typedv1core.EventsGetter
	coreBroadcaster     record.EventBroadcaster
	eventsv1Client      typedeventsv1.EventsV1Interface
	eventsv1Broadcaster EventBroadcaster
}

// NewEventBroadcasterAdapter 包装新旧两个广播器，
// 简化各组件向新 Event API 的迁移。
//
//logcheck:context // NewEventBroadcasterAdapterWithContext should be used instead because record.NewBroadcaster is called and works better when a context is supplied (contextual logging, cancellation).
func NewEventBroadcasterAdapter(client clientset.Interface) EventBroadcasterAdapter {
	return NewEventBroadcasterAdapterWithContext(context.Background(), client)
}

// NewEventBroadcasterAdapterWithContext 包装新旧两个广播器，
// 简化各组件向新 Event API 的迁移。
//
// 通过发现接口探测 events/v1 是否可用：可用时同时启用新广播器；
// legacy core/v1 广播器始终创建，以兼容尚未迁移的调用方。
func NewEventBroadcasterAdapterWithContext(ctx context.Context, client clientset.Interface) EventBroadcasterAdapter {
	eventClient := &eventBroadcasterAdapterImpl{}
	if _, err := client.Discovery().ServerResourcesForGroupVersionWithContext(ctx, eventsv1.SchemeGroupVersion.String()); err == nil {
		eventClient.eventsv1Client = client.EventsV1()
		eventClient.eventsv1Broadcaster = NewBroadcaster(&EventSinkImpl{Interface: eventClient.eventsv1Client})
	}
	// 尽管很快就会出现 coreBroadcaster 不再被需要的场景，
	// 这里仍无条件创建它：开销很小，且能让各组件使用该库的常见模式更简单。
	eventClient.coreClient = client.CoreV1()
	eventClient.coreBroadcaster = record.NewBroadcaster(record.WithContext(ctx))
	return eventClient
}

// StartRecordingToSink 开始把指定 eventBroadcaster 收到的事件发送给给定的 sink。
//
// 两套广播器都会启动，这样新 API 与 legacy API 的事件都能被记录。
func (e *eventBroadcasterAdapterImpl) StartRecordingToSink(stopCh <-chan struct{}) {
	if e.eventsv1Broadcaster != nil && e.eventsv1Client != nil {
		e.eventsv1Broadcaster.StartRecordingToSink(stopCh)
	}
	if e.coreBroadcaster != nil && e.coreClient != nil {
		e.coreBroadcaster.StartRecordingToSink(&typedv1core.EventSinkImpl{Interface: e.coreClient.Events("")})
	}
}

// NewRecorder 返回优先使用 events/v1 的记录器；events/v1 不可用时回退到
// legacy 记录器的适配实现。
func (e *eventBroadcasterAdapterImpl) NewRecorder(name string) EventRecorderLogger {
	if e.eventsv1Broadcaster != nil && e.eventsv1Client != nil {
		return e.eventsv1Broadcaster.NewRecorder(scheme.Scheme, name)
	}
	return record.NewEventRecorderAdapter(e.DeprecatedNewLegacyRecorder(name))
}

// DeprecatedNewLegacyRecorder 返回直接使用 core/v1 Events 的旧记录器。
func (e *eventBroadcasterAdapterImpl) DeprecatedNewLegacyRecorder(name string) record.EventRecorderLogger {
	return e.coreBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: name})
}

// Shutdown 依次关闭两套广播器；两者都存在时都会被关闭。
func (e *eventBroadcasterAdapterImpl) Shutdown() {
	if e.coreBroadcaster != nil {
		e.coreBroadcaster.Shutdown()
	}
	if e.eventsv1Broadcaster != nil {
		e.eventsv1Broadcaster.Shutdown()
	}
}
