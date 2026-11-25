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
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/utils/clock"
	"k8s.io/utils/lru"
)

const (
	maxLruCacheEntries = 4096

	// 若仅 message 不同的相同事件在 10 分钟周期内出现超过 10 次，则聚合该事件
	// 10 分钟内相同事件（仅 message 不同）出现超过 10 次即合并，避免事件风暴。
	defaultAggregateMaxEvents         = 10
	defaultAggregateIntervalInSeconds = 600

	// 默认允许某个 source 就某个对象发送 25 条事件，
	// 但把回填速率控制在每 5 分钟 1 条新事件，
	// 以抑制长期不健康对象造成的事件长尾
	// 令牌桶：允许突发 25 条，但每 5 分钟只回填 1 条，抑制长期不健康对象的刷屏。
	defaultSpamBurst = 25
	defaultSpamQPS   = 1. / 300.
)

// getEventKey 基于 source、involvedObject、reason、message 构造唯一事件 key。
// 该 key 用于精确去重：message 也参与计算，因此只要消息不同就视为不同事件。
func getEventKey(event *v1.Event) string {
	return strings.Join([]string{
		event.Source.Component,
		event.Source.Host,
		event.InvolvedObject.Kind,
		event.InvolvedObject.Namespace,
		event.InvolvedObject.Name,
		event.InvolvedObject.FieldPath,
		string(event.InvolvedObject.UID),
		event.InvolvedObject.APIVersion,
		event.Type,
		event.Reason,
		event.Message,
	},
		"")
}

// getSpamKey 基于 source、involvedObject 构造唯一事件 key。
// 该 key 刻意不含 message/reason，用于按"来源+对象"维度统计频率以做限流。
func getSpamKey(event *v1.Event) string {
	return strings.Join([]string{
		event.Source.Component,
		event.Source.Host,
		event.InvolvedObject.Kind,
		event.InvolvedObject.Namespace,
		event.InvolvedObject.Name,
		string(event.InvolvedObject.UID),
		event.InvolvedObject.APIVersion,
		event.Type,
	},
		"")
}

// EventSpamKeyFunc 是一个函数，基于所提供的事件返回唯一 key
type EventSpamKeyFunc func(event *v1.Event) string

// EventFilterFunc 是一个函数，当事件应被跳过时返回 true
type EventFilterFunc func(event *v1.Event) bool

// EventSourceObjectSpamFilter 负责限制某个 source 和对象能产生的事件数量。
// 以"来源+对象"为粒度做令牌桶限流；LRU 保证缓存条目数有上界。
type EventSourceObjectSpamFilter struct {
	// cache 管理最近同步的状态
	cache *lru.Cache

	// burst 是每个 source + object 允许的事件数量
	burst int

	// qps 是令牌桶的回填速率（每秒查询数）
	qps float32

	// clock 用于在模拟的时间区间上进行测试
	clock clock.PassiveClock

	// spamKeyFunc 是根据事件生成 key 的函数，该 key 后续用于过滤刷屏事件。
	spamKeyFunc EventSpamKeyFunc
}

// NewEventSourceObjectSpamFilter 允许 source 就某个对象突发 burst 条事件，并按指定的 qps 回填。
func NewEventSourceObjectSpamFilter(lruCacheSize, burst int, qps float32, clock clock.PassiveClock, spamKeyFunc EventSpamKeyFunc) *EventSourceObjectSpamFilter {
	return &EventSourceObjectSpamFilter{
		cache:       lru.New(lruCacheSize),
		burst:       burst,
		qps:         qps,
		clock:       clock,
		spamKeyFunc: spamKeyFunc,
	}
}

// spamRecord 保存用于刷屏过滤决策的数据。
type spamRecord struct {
	// rateLimiter 控制该对象相关事件的速率
	rateLimiter flowcontrol.PassiveRateLimiter
}

// Filter 用于确保给定 source+object 不超过允许的速率。
// event 参数不得为 nil。
// 返回 true 表示该事件应被丢弃（超过配额）。
func (f *EventSourceObjectSpamFilter) Filter(event *v1.Event) bool {
	var record spamRecord

	// 缓存该事件的已知信息
	eventKey := f.spamKeyFunc(event)

	// 缓存中是否有相似事件的记录？
	value, found := f.cache.Get(eventKey)
	if found {
		record = value.(spamRecord)
	}

	// 确保该记录有对应的限流器
	// 首次见到该 key 时才创建令牌桶；此后复用同一个桶以保持配额状态。
	if record.rateLimiter == nil {
		record.rateLimiter = flowcontrol.NewTokenBucketPassiveRateLimiterWithClock(f.qps, f.burst, f.clock)
	}

	// 确认速率配额可用
	filter := !record.rateLimiter.TryAccept()

	// 更新缓存
	f.cache.Add(eventKey, record)

	return filter
}

// EventAggregatorKeyFunc 负责为聚合对事件进行分组。
// 它返回一个二元组：
// aggregateKey - 标识该事件应归入哪个聚合分组的 key
// localKey - 标识该事件在本地分组内的 key
type EventAggregatorKeyFunc func(event *v1.Event) (aggregateKey string, localKey string)

// EventAggregatorByReasonFunc 依据 event.Source、event.InvolvedObject、event.Type、
// event.Reason、event.ReportingController 和 event.ReportingInstance 的精确匹配来聚合事件。
// event 参数不得为 nil。
func EventAggregatorByReasonFunc(event *v1.Event) (string, string) {
	// aggregateKey 不含 message，使仅消息不同的事件归为同一组；localKey 用 message 区分组内成员。
	return strings.Join([]string{
		event.Source.Component,
		event.Source.Host,
		event.InvolvedObject.Kind,
		event.InvolvedObject.Namespace,
		event.InvolvedObject.Name,
		string(event.InvolvedObject.UID),
		event.InvolvedObject.APIVersion,
		event.Type,
		event.Reason,
		event.ReportingController,
		event.ReportingInstance,
	},
		""), event.Message
}

// EventAggregatorMessageFunc 负责生成聚合消息
type EventAggregatorMessageFunc func(event *v1.Event) string

// EventAggregatorByReasonMessageFunc 通过在原消息前添加前缀生成聚合消息
func EventAggregatorByReasonMessageFunc(event *v1.Event) string {
	return "(combined from similar events): " + event.Message
}

// EventAggregator 识别相似事件并将它们聚合为单个事件
// 内嵌 RWMutex 保护 cache 的并发读写。
type EventAggregator struct {
	sync.RWMutex

	// cache 是管理聚合状态的缓存
	cache *lru.Cache

	// keyFunc 是对事件进行聚合分组的函数
	keyFunc EventAggregatorKeyFunc

	// messageFunc 是生成聚合事件消息的函数
	messageFunc EventAggregatorMessageFunc

	// maxEvents 是触发聚合前，指定区间内的最大事件数
	maxEvents uint

	// maxIntervalInSeconds 是距上次相似事件出现后，将其视为新事件需要经过的秒数
	maxIntervalInSeconds uint

	// clock 用于在模拟的时间区间上进行测试
	clock clock.PassiveClock
}

// NewEventAggregator 返回一个新的 EventAggregator 实例
func NewEventAggregator(lruCacheSize int, keyFunc EventAggregatorKeyFunc, messageFunc EventAggregatorMessageFunc,
	maxEvents int, maxIntervalInSeconds int, clock clock.PassiveClock) *EventAggregator {
	return &EventAggregator{
		cache:                lru.New(lruCacheSize),
		keyFunc:              keyFunc,
		messageFunc:          messageFunc,
		maxEvents:            uint(maxEvents),
		maxIntervalInSeconds: uint(maxIntervalInSeconds),
		clock:                clock,
	}
}

// aggregateRecord 保存用于聚合决策的数据
type aggregateRecord struct {
	// 跟踪聚合集中已出现过的唯一 localKey 数量，以决定何时真正聚合；
	// 若该集合大小超过 max，就需要聚合
	localKeys sets.Set[string]
	// 上次记录该聚合的时间
	lastTimestamp metav1.Time
}

// EventAggregate 根据聚合配置（最大事件数、最大间隔等）检查是否已出现过相似事件，并返回：
//
//   - 应被创建（可能已被修改）的事件
//   - 该事件的缓存 key，用于关联。普通事件使用完整 key，
//     聚合事件使用 EventAggregatorMessageFunc 的结果。
//
// newEvent 参数不得为 nil。
func (e *EventAggregator) EventAggregate(newEvent *v1.Event) (*v1.Event, string) {
	now := metav1.NewTime(e.clock.Now())
	var record aggregateRecord
	// eventKey 是该事件的完整缓存 key
	eventKey := getEventKey(newEvent)
	// aggregateKey 用于聚合事件（如需要时）。
	aggregateKey, localKey := e.keyFunc(newEvent)

	// 缓存中是否有相似事件的记录？
	e.Lock()
	defer e.Unlock()
	value, found := e.cache.Get(aggregateKey)
	if found {
		record = value.(aggregateRecord)
	}

	// 之前的记录是否太旧？若是则新建一条。注意：若没找到相似记录，
	// 其 lastTimestamp 为零值，因此这种情况下也会创建新记录。
	maxInterval := time.Duration(e.maxIntervalInSeconds) * time.Second
	interval := now.Time.Sub(record.lastTimestamp.Time)
	if interval > maxInterval {
		record = aggregateRecord{localKeys: sets.New[string]()}
	}

	// 将新事件写入聚合记录并放入缓存
	record.localKeys.Insert(localKey)
	record.lastTimestamp = now
	e.cache.Add(aggregateKey, record)

	// 尚未超过唯一事件阈值时，不对它们做关联
	// 未达到阈值前返回原事件本身；达到阈值后才开始生成聚合事件。
	if uint(record.localKeys.Len()) < e.maxEvents {
		return newEvent, eventKey
	}

	// 控制 localKey 集合的增长不超过 max
	// 防止集合无限增长：每次聚合后移除一个任意元素，把大小维持在阈值内。
	record.localKeys.PopAny()

	// 创建新的聚合事件，并将 aggregateKey 作为缓存 key 返回
	// （以便它可以被覆盖）。
	// 聚合事件的名字带纳秒时间戳，保证每次聚合生成的事件名唯一。
	eventCopy := &v1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%v.%x", newEvent.InvolvedObject.Name, now.UnixNano()),
			Namespace: newEvent.Namespace,
		},
		Count:          1,
		FirstTimestamp: now,
		InvolvedObject: newEvent.InvolvedObject,
		LastTimestamp:  now,
		Message:        e.messageFunc(newEvent),
		Type:           newEvent.Type,
		Reason:         newEvent.Reason,
		Source:         newEvent.Source,
	}
	return eventCopy, aggregateKey
}

// eventLog 记录事件被观测到的相关数据
type eventLog struct {
	// count 是该事件自首次出现以来的发生次数。
	count uint

	// firstTimestamp 是首次记录该事件的时间。
	firstTimestamp metav1.Time

	// name 是该事件首次出现的唯一名称
	name string

	// resourceVersion 是上次与服务端交互返回的资源版本
	resourceVersion string
}

// eventLogger 记录事件的发生情况
type eventLogger struct {
	sync.RWMutex
	cache *lru.Cache
	clock clock.PassiveClock
}

// newEventLogger 观测事件并统计其出现频率
func newEventLogger(lruCacheEntries int, clock clock.PassiveClock) *eventLogger {
	return &eventLogger{cache: lru.New(lruCacheEntries), clock: clock}
}

// eventObserve 记录一个事件；若 key 命中缓存则更新已有事件
func (e *eventLogger) eventObserve(newEvent *v1.Event, key string) (*v1.Event, []byte, error) {
	var (
		patch []byte
		err   error
	)
	eventCopy := *newEvent
	event := &eventCopy

	e.Lock()
	defer e.Unlock()

	// 检查是否存在应更新的已有事件
	lastObservation := e.lastEventObservationFromCache(key)

	// 若找到结果，准备补丁
	if lastObservation.count > 0 {
		// 基于上次观测更新事件，使补丁按预期生效
		// 复用上次事件的 name/resourceVersion/firstTimestamp，使服务端把它识别为同一事件的更新。
		event.Name = lastObservation.name
		event.ResourceVersion = lastObservation.resourceVersion
		event.FirstTimestamp = lastObservation.firstTimestamp
		event.Count = int32(lastObservation.count) + 1

		// 构造用于补丁的"旧值"：把每次都会变化的字段清零，避免它们进入 patch。
		eventCopy2 := *event
		eventCopy2.Count = 0
		eventCopy2.LastTimestamp = metav1.NewTime(time.Unix(0, 0))
		eventCopy2.Message = ""

		newData, _ := json.Marshal(event)
		oldData, _ := json.Marshal(eventCopy2)
		patch, err = strategicpatch.CreateTwoWayMergePatch(oldData, newData, event)
	}

	// 记录本次新的观测
	e.cache.Add(
		key,
		eventLog{
			count:           uint(event.Count),
			firstTimestamp:  event.FirstTimestamp,
			name:            event.Name,
			resourceVersion: event.ResourceVersion,
		},
	)
	return event, patch, err
}

// updateState 根据最新的服务器状态更新内部跟踪信息
func (e *eventLogger) updateState(event *v1.Event) {
	key := getEventKey(event)
	e.Lock()
	defer e.Unlock()
	// 记录本次新的观测
	e.cache.Add(
		key,
		eventLog{
			count:           uint(event.Count),
			firstTimestamp:  event.FirstTimestamp,
			name:            event.Name,
			resourceVersion: event.ResourceVersion,
		},
	)
}

// lastEventObservationFromCache 从缓存返回事件；读操作需由外部锁保护
func (e *eventLogger) lastEventObservationFromCache(key string) eventLog {
	value, ok := e.cache.Get(key)
	if ok {
		observationValue, ok := value.(eventLog)
		if ok {
			return observationValue
		}
	}
	return eventLog{}
}

// EventCorrelator 处理所有进入的事件并进行分析，以避免压垮系统。它可以过滤所有进入的
// 事件，决定是否跳过后续处理；可以对频繁出现的相似事件做聚合，避免用户难以区分的
// 刷屏事件；还会做去重，将多次观测到的事件压缩为计数递增的单个事件。
type EventCorrelator struct {
	// filterFunc 是过滤事件的函数
	filterFunc EventFilterFunc
	// aggregator 是执行事件聚合的对象
	aggregator *EventAggregator
	// logger 是随事件到来持续观测的对象
	logger *eventLogger
}

// EventCorrelateResult 是 Correlate 的结果
type EventCorrelateResult struct {
	// Event 是经过关联后的事件
	Event *v1.Event
	// Patch 若提供，更新服务端记录时使用策略补丁
	Patch []byte
	// Skip 为 true 时不再继续处理该事件
	Skip bool
}

// NewEventCorrelator 返回一个使用默认值配置的 EventCorrelator。
//
// EventCorrelator 负责在与 API 服务器交互记录事件之前，进行事件过滤、聚合与计数。
//
// 默认行为如下：
//   - 当相似事件在 10 分钟的滚动区间内被记录 10 次时执行聚合。
//     相似事件指仅 Event.Message 字段不同的事件。聚合不会记录原始事件，
//     而是创建一个新事件，其消息表明已合并相同原因的事件。
//   - 完全相同的事件多次出现时进行增量计数。
//   - 某个 source 可以就某个对象突发发送 25 条事件，但每个对象的回填预算
//     为每 5 分钟 1 条事件，以控制刷屏长尾。
func NewEventCorrelator(clock clock.PassiveClock) *EventCorrelator {
	cacheSize := maxLruCacheEntries
	spamFilter := NewEventSourceObjectSpamFilter(cacheSize, defaultSpamBurst, defaultSpamQPS, clock, getSpamKey)
	return &EventCorrelator{
		filterFunc: spamFilter.Filter,
		aggregator: NewEventAggregator(
			cacheSize,
			EventAggregatorByReasonFunc,
			EventAggregatorByReasonMessageFunc,
			defaultAggregateMaxEvents,
			defaultAggregateIntervalInSeconds,
			clock),

		logger: newEventLogger(cacheSize, clock),
	}
}

// NewEventCorrelatorWithOptions 使用调用方提供的选项构造 EventCorrelator，零值字段由 populateDefaults 补齐。
func NewEventCorrelatorWithOptions(options CorrelatorOptions) *EventCorrelator {
	optionsWithDefaults := populateDefaults(options)
	spamFilter := NewEventSourceObjectSpamFilter(
		optionsWithDefaults.LRUCacheSize,
		optionsWithDefaults.BurstSize,
		optionsWithDefaults.QPS,
		optionsWithDefaults.Clock,
		optionsWithDefaults.SpamKeyFunc)
	return &EventCorrelator{
		filterFunc: spamFilter.Filter,
		aggregator: NewEventAggregator(
			optionsWithDefaults.LRUCacheSize,
			optionsWithDefaults.KeyFunc,
			optionsWithDefaults.MessageFunc,
			optionsWithDefaults.MaxEvents,
			optionsWithDefaults.MaxIntervalInSeconds,
			optionsWithDefaults.Clock),
		logger: newEventLogger(optionsWithDefaults.LRUCacheSize, optionsWithDefaults.Clock),
	}
}

// populateDefaults 用默认值填充零值选项
func populateDefaults(options CorrelatorOptions) CorrelatorOptions {
	if options.LRUCacheSize == 0 {
		options.LRUCacheSize = maxLruCacheEntries
	}
	if options.BurstSize == 0 {
		options.BurstSize = defaultSpamBurst
	}
	if options.QPS == 0 {
		options.QPS = defaultSpamQPS
	}
	if options.KeyFunc == nil {
		options.KeyFunc = EventAggregatorByReasonFunc
	}
	if options.MessageFunc == nil {
		options.MessageFunc = EventAggregatorByReasonMessageFunc
	}
	if options.MaxEvents == 0 {
		options.MaxEvents = defaultAggregateMaxEvents
	}
	if options.MaxIntervalInSeconds == 0 {
		options.MaxIntervalInSeconds = defaultAggregateIntervalInSeconds
	}
	if options.Clock == nil {
		options.Clock = clock.RealClock{}
	}
	if options.SpamKeyFunc == nil {
		options.SpamKeyFunc = getSpamKey
	}
	return options
}

// EventCorrelate 对所有进入的事件进行过滤、聚合、计数与去重
func (c *EventCorrelator) EventCorrelate(newEvent *v1.Event) (*EventCorrelateResult, error) {
	if newEvent == nil {
		return nil, fmt.Errorf("event is nil")
	}
	// 顺序很重要：先聚合（可能缩减为单个聚合事件），再对聚合结果做去重计数，最后做限流。
	aggregateEvent, ckey := c.aggregator.EventAggregate(newEvent)
	observedEvent, patch, err := c.logger.eventObserve(aggregateEvent, ckey)
	if c.filterFunc(observedEvent) {
		return &EventCorrelateResult{Skip: true}, nil
	}
	return &EventCorrelateResult{Event: observedEvent, Patch: patch}, err
}

// UpdateState 根据服务端最新观测到的状态更新。
// event 参数不得为 nil。
func (c *EventCorrelator) UpdateState(event *v1.Event) {
	c.logger.updateState(event)
}
