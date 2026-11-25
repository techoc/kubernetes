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

package handlers

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"golang.org/x/net/websocket"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/endpoints/handlers/negotiation"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	"k8s.io/apiserver/pkg/endpoints/metrics"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/server/httplog"
	"k8s.io/apiserver/pkg/storage"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	compbasemetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/tracing"
	"k8s.io/klog/v2"
	"k8s.io/streaming/pkg/httpstream/wsstream"
)

// timeoutFactory 抽象 watch 的超时逻辑，便于测试。
type TimeoutFactory interface {
	TimeoutCh() (<-chan time.Time, func() bool)
}

// realTimeoutFactory 使用真实的 time.Timer 实现 timeoutFactory。
type realTimeoutFactory struct {
	timeout time.Duration
}

// TimeoutCh 返回一个通道，watch 超时时会从中收到数据，
// 同时返回一个用于届时调用的清理函数。
//
// timeout == 0 时返回 nil channel：在 select 中永久阻塞，正好表达“不超时”，
// 且无需额外分配 timer。
func (w *realTimeoutFactory) TimeoutCh() (<-chan time.Time, func() bool) {
	if w.timeout == 0 {
		// 该通道永远不会收到任何数据。
		return nil, func() bool { return false }
	}
	t := time.NewTimer(w.timeout)
	return t.C, t.Stop
}

// serveWatchHandler 返回一个用于服务 watch 响应的句柄。
// TODO: 本方法与 WatchServer.Serve 的职责划分尚不清晰。
//
// 该函数负责“协商与装配”（编码器、framing、内存分配器、超时），
// WatchServer 负责实际的流式循环；两者尚未完全解耦。
func serveWatchHandler(watcher watch.Interface, scope *RequestScope, mediaTypeOptions negotiation.MediaTypeOptions, req *http.Request, w http.ResponseWriter, timeout time.Duration, metricsScope string, isWatchListRequest bool, completeHook WatchListCompleteHook) (http.Handler, error) {
	options, err := optionsForTransform(mediaTypeOptions, req)
	if err != nil {
		return nil, err
	}

	// 从 scope 的序列化器协商出流式序列化器。
	serializer, err := negotiation.NegotiateOutputMediaTypeStream(req, scope.Serializer, scope)
	if err != nil {
		return nil, err
	}
	framer := serializer.StreamSerializer.Framer
	// CBOR 的非确定性编码允许在 map 顺序等细节上自由选择，换取更快的序列化；
	// 对 watch 流而言每个事件独立解码，顺序无关紧要。
	var encoder runtime.Encoder
	if utilfeature.DefaultFeatureGate.Enabled(features.CBORServingAndStorage) {
		encoder = scope.Serializer.EncoderForVersion(runtime.UseNondeterministicEncoding(serializer.StreamSerializer.Serializer), scope.Kind.GroupVersion())
	} else {
		encoder = scope.Serializer.EncoderForVersion(serializer.StreamSerializer.Serializer, scope.Kind.GroupVersion())
	}
	useTextFraming := serializer.EncodesAsText
	if framer == nil {
		return nil, fmt.Errorf("no framer defined for %q available for embedded encoding", serializer.MediaType)
	}
	// TODO: 下一步应从 negotiate 拿回 mediaTypeOptions，并在此返回准确的值。
	mediaType := serializer.MediaType
	switch mediaType {
	case runtime.ContentTypeJSON:
		// 保持不变。
	case runtime.ContentTypeCBOR:
		// 如果客户端在 watch 请求中声明只接受 application/cbor（单个数据项），
		// 则把 watch 响应的媒体类型设为符合规范的 application/cbor-seq。
		// RFC 9110 允许源服务器偏离客户端声明的偏好，而不返回 406（Not Acceptable）
		// （见 https://www.rfc-editor.org/rfc/rfc9110.html#section-12.1-5）。
		mediaType = runtime.ContentTypeCBORSequence
	default:
		mediaType += ";stream=watch"
	}

	ctx := req.Context()

	// 根据 transform 定位合适的内嵌编码器。
	var negotiatedEncoder runtime.Encoder
	contentKind, contentSerializer, transform := targetEncodingForTransform(scope, mediaTypeOptions, req)
	if transform {
		info, ok := runtime.SerializerInfoForMediaType(contentSerializer.SupportedMediaTypes(), serializer.MediaType)
		if !ok {
			return nil, fmt.Errorf("no encoder for %q exists in the requested target %#v", serializer.MediaType, contentSerializer)
		}
		if utilfeature.DefaultFeatureGate.Enabled(features.CBORServingAndStorage) {
			negotiatedEncoder = contentSerializer.EncoderForVersion(runtime.UseNondeterministicEncoding(info.Serializer), contentKind.GroupVersion())
		} else {
			negotiatedEncoder = contentSerializer.EncoderForVersion(info.Serializer, contentKind.GroupVersion())
		}
	} else {
		if utilfeature.DefaultFeatureGate.Enabled(features.CBORServingAndStorage) {
			negotiatedEncoder = scope.Serializer.EncoderForVersion(runtime.UseNondeterministicEncoding(serializer.Serializer), contentKind.GroupVersion())
		} else {
			negotiatedEncoder = scope.Serializer.EncoderForVersion(serializer.Serializer, contentKind.GroupVersion())
		}
	}

	var memoryAllocator runtime.MemoryAllocator

	if encoderWithAllocator, supportsAllocator := negotiatedEncoder.(runtime.EncoderWithAllocator); supportsAllocator {
		// 不要把 allocator 放进 embeddedEncodeFn，否则每次调用都会分配内存；
		// 应该为整个 watch 会话分配一次 buffer，并在关闭 checker 时释放。
		// 每个 watch 连接复用一个分配器，显著降低长连接的 GC 压力。
		memoryAllocator = runtime.AllocatorPool.Get().(*runtime.Allocator)
		negotiatedEncoder = runtime.NewEncoderWithAllocator(encoderWithAllocator, memoryAllocator)
	}
	var tableOptions *metav1.TableOptions
	if options != nil {
		if passedOptions, ok := options.(*metav1.TableOptions); ok {
			tableOptions = passedOptions
		} else {
			return nil, fmt.Errorf("unexpected options type: %T", options)
		}
	}
	embeddedEncoder := newWatchEmbeddedEncoder(ctx, negotiatedEncoder, mediaTypeOptions.Convert, tableOptions, scope)

	if encoderWithAllocator, supportsAllocator := encoder.(runtime.EncoderWithAllocator); supportsAllocator {
		if memoryAllocator == nil {
			// 不要把 allocator 放进 embeddedEncodeFn，否则每次调用都会分配内存；
			// 应该为整个 watch 会话分配一次 buffer，并在关闭 checker 时释放。
			memoryAllocator = runtime.AllocatorPool.Get().(*runtime.Allocator)
		}
		encoder = runtime.NewEncoderWithAllocator(encoderWithAllocator, memoryAllocator)
	}

	var serverShuttingDownCh <-chan struct{}
	if signals := apirequest.ServerShutdownSignalFrom(req.Context()); signals != nil {
		serverShuttingDownCh = signals.ShuttingDown()
	}

	server := &WatchServer{
		Watching: watcher,
		Scope:    scope,

		UseTextFraming:  useTextFraming,
		MediaType:       mediaType,
		Framer:          framer,
		Encoder:         encoder,
		EmbeddedEncoder: embeddedEncoder,

		MemoryAllocator:      memoryAllocator,
		TimeoutFactory:       &realTimeoutFactory{timeout},
		ServerShuttingDownCh: serverShuttingDownCh,

		metricsScope:          metricsScope,
		isWatchListRequest:    isWatchListRequest,
		watchListCompleteHook: completeHook,
	}

	// WebSocket 与普通 HTTP 分块流共用同一 WatchServer，只是传输层不同。
	if wsstream.IsWebSocketRequest(req) {
		w.Header().Set("Content-Type", server.MediaType)
		return websocket.Handler(server.HandleWS), nil
	}
	return http.HandlerFunc(server.HandleHTTP), nil
}

// WatchServer 通过 websocket 或普通 HTTP 提供 watch.Interface 的服务。
type WatchServer struct {
	Watching watch.Interface
	Scope    *RequestScope

	// websocket 消息是否使用文本帧（相对于二进制帧）
	UseTextFraming bool
	// 本次 watch 所使用的媒体类型
	MediaType string
	// 用于对 watch 流做分帧
	Framer runtime.Framer
	// 用于编码 watch 流事件本身
	Encoder runtime.Encoder
	// 用于编码 watch 流中嵌套的对象
	EmbeddedEncoder runtime.Encoder

	MemoryAllocator      runtime.MemoryAllocator
	TimeoutFactory       TimeoutFactory
	ServerShuttingDownCh <-chan struct{}

	metricsScope          string
	isWatchListRequest    bool
	watchListCompleteHook WatchListCompleteHook
}

type WatchListCompleteHook func()

// watchEventMetricsRecorder 供调用方统计写出的字节数，并上报每个事件的大小。
// 只要底层 io.Writer 是线程安全的，它就是线程安全的。
// 某个 watch 事件的所有 Write 调用结束后，必须调用 RecordEvent。
//
// 一个事件可能被 Write 多次（例如编码器分块写），因此用计数器累积，
// 由 RecordEvent 一次性上报并清零。
type watchEventMetricsRecorder struct {
	writer      io.Writer
	countMetric compbasemetrics.CounterMetric
	sizeMetric  compbasemetrics.ObserverMetric
	byteCount   atomic.Int64
}

// Write 实现 io.Writer。
func (c *watchEventMetricsRecorder) Write(p []byte) (n int, err error) {
	n, err = c.writer.Write(p)
	c.byteCount.Add(int64(n))
	return
}

// Record 上报指标并重置字节计数。
func (c *watchEventMetricsRecorder) RecordEvent() {
	c.countMetric.Inc()
	c.sizeMetric.Observe(float64(c.byteCount.Swap(0)))
}

// 说明：sync.Pool 在首次 Get 之前是空池（https://pkg.go.dev/sync#Pool.Get）。
var watchGzipPool = responsewriters.NewGzipWriterPoolOrDie()

// watchStreamWriter 统一了明文与 gzip 两种流写入方式，供 watchResponseWriter 使用。
type watchStreamWriter interface {
	io.WriteCloser
	Flush() error
}

var _ watchStreamWriter = &plainResponseWriter{}

// plainResponseWriter 直接写底层 ResponseWriter，仅补充 Flush。
type plainResponseWriter struct {
	w       http.ResponseWriter
	flusher http.Flusher
}

func (p *plainResponseWriter) Write(b []byte) (int, error) {
	return p.w.Write(b)
}

func (p *plainResponseWriter) Flush() error {
	p.flusher.Flush()
	return nil
}

// Close 是空操作，因为 http.ResponseWriter（p.w）没有实现 io.Closer。
func (p *plainResponseWriter) Close() error {
	return nil
}

var _ watchStreamWriter = &perFlushGzipWriter{}

// perFlushGzipWriter 在每次 Flush 后关闭 gzip writer 并归还对象池：
// 对 watch 流而言，尽早结束一个 gzip member 能让客户端立即解压，
// 而不是等待整个连接结束。
type perFlushGzipWriter struct {
	delegateRW http.ResponseWriter
	flusher    http.Flusher
	gw         *gzip.Writer
}

func (p *perFlushGzipWriter) Write(b []byte) (int, error) {
	if p.gw == nil {
		p.gw = watchGzipPool.Get().(*gzip.Writer)
		p.gw.Reset(p.delegateRW)
	}
	return p.gw.Write(b)
}

// Flush 把压缩数据写给客户端，并把 gzip.Writer 归还给 pool。
func (p *perFlushGzipWriter) Flush() error {
	// 没有 writer 说明 Write 从未被调用，无需 flush
	if p.gw == nil {
		return nil
	}
	if err := p.gw.Flush(); err != nil {
		return err
	}
	err := p.Close()
	p.flusher.Flush()
	return err
}

func (p *perFlushGzipWriter) Close() error {
	if p.gw == nil {
		return nil
	}
	err := p.gw.Close()
	p.gw.Reset(nil)
	watchGzipPool.Put(p.gw)
	// 防止重复 close 把同一个 writer 归还给 pool 两次
	// 置 nil 是必要的：同一 writer 可能因错误路径被重复 Close，
	// 重复归还对象池会导致池中出现同一对象的多个副本。
	p.gw = nil
	return err
}

var _ io.Writer = &watchResponseWriter{}

// watchResponseWriter 根据内容协商结果选择明文或 gzip 输出。
type watchResponseWriter struct {
	delegateRW         http.ResponseWriter
	flusher            http.Flusher
	contentEncoding    string
	isWatchListRequest bool
	writer             watchStreamWriter
}

func newWatchResponseWriter(delegateRW http.ResponseWriter, flusher http.Flusher, contentEncoding string, isWatchListRequest bool) *watchResponseWriter {
	return &watchResponseWriter{
		delegateRW:         delegateRW,
		flusher:            flusher,
		contentEncoding:    contentEncoding,
		isWatchListRequest: isWatchListRequest,
		writer:             &plainResponseWriter{w: delegateRW, flusher: flusher},
	}
}

// BeginStream 写出响应头并立即 flush，让客户端尽快进入流式读取状态。
func (w *watchResponseWriter) BeginStream(mediaType string) {
	w.delegateRW.Header().Set("Content-Type", mediaType)
	w.delegateRW.Header().Set("Transfer-Encoding", "chunked")
	if w.contentEncoding == "gzip" && w.isWatchListRequest {
		w.delegateRW.Header().Set("Content-Encoding", "gzip")
		w.delegateRW.Header().Add("Vary", "Accept-Encoding")
		w.writer = &perFlushGzipWriter{delegateRW: w.delegateRW, flusher: w.flusher}
	}
	w.delegateRW.WriteHeader(http.StatusOK)
	// 只 flush HTTP 头；gzip 只作用于 body，不作用于 header。
	w.flusher.Flush()
}

func (w *watchResponseWriter) Write(p []byte) (int, error) {
	return w.writer.Write(p)
}

func (w *watchResponseWriter) Flush() error {
	return w.writer.Flush()
}

func (w *watchResponseWriter) Close() error {
	return w.writer.Close()
}

// HandleHTTP 通过 HTTP 的 Transfer-Encoding: chunked，或者 websocket 连接，
// 输出一串已编码的事件。
func (s *WatchServer) HandleHTTP(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	ctx, span := tracing.Start(ctx, "WatchServer.HandleHTTP",
		attribute.String("audit-id", audit.GetAuditIDTruncated(ctx)),
		attribute.String("method", req.Method),
		attribute.String("url", req.URL.Path),
		attribute.String("protocol", req.Proto),
		attribute.String("mediaType", s.MediaType),
		attribute.String("encoder", string(s.Encoder.Identifier())))
	req = req.WithContext(ctx)
	defer func() {
		if s.MemoryAllocator != nil {
			runtime.AllocatorPool.Put(s.MemoryAllocator)
		}
	}()

	flusher, ok := w.(http.Flusher)
	if !ok {
		err := fmt.Errorf("unable to start watch - can't get http.Flusher: %#v", w)
		utilruntime.HandleErrorWithContext(req.Context(), err, "Unable to start watch")
		s.Scope.err(errors.NewInternalError(err), w, req)
		return
	}

	contentEncoding := responsewriters.ContentEncodingSupported(req, features.WatchListCompression)
	rw := newWatchResponseWriter(w, flusher, contentEncoding, s.isWatchListRequest)
	defer func() {
		if err := rw.Close(); err != nil {
			utilruntime.HandleErrorWithContext(req.Context(), err, "Failed to close watch response writer")
		}
	}()

	// framer 负责把编码后的事件切分成流式帧；构造失败属于编程错误。
	framer := s.Framer.NewFrameWriter(rw)
	if framer == nil {
		// 这是编程错误。
		err := fmt.Errorf("no stream framing support is available for media type %q", s.MediaType)
		utilruntime.HandleErrorWithContext(req.Context(), err, "No stream framing support available")
		s.Scope.err(errors.NewBadRequest(err.Error()), w, req)
		return
	}

	// 确保连接会超时。
	// 超时后直接返回，让客户端按自身策略重连（通常是带 resourceVersion 续传）。
	timeoutCh, cleanup := s.TimeoutFactory.TimeoutCh()
	defer cleanup()

	// 开始输出流。
	rw.BeginStream(s.MediaType)

	gvr := s.Scope.Resource

	recorder := &watchEventMetricsRecorder{
		writer:      framer,
		countMetric: metrics.WatchEvents.WithContext(req.Context()).WithLabelValues(gvr.Group, gvr.Version, gvr.Resource),
		sizeMetric:  metrics.WatchEventsSizes.WithContext(req.Context()).WithLabelValues(gvr.Group, gvr.Version, gvr.Resource),
	}

	watchEncoder := newWatchEncoder(req.Context(), gvr, s.EmbeddedEncoder, s.Encoder, recorder)
	ch := s.Watching.ResultChan()
	done := req.Context().Done()

	span.AddEvent("About to start writing response")
	for {
		select {
		case <-s.ServerShuttingDownCh:
			// 服务器已发出正在关闭（不再接收新请求）的信号时，
			// 所有活跃的 watch 请求应立即在此返回。
			// WithWatchTerminationDuringShutdown 服务端过滤器会确保对客户端的
			// 响应做限速，避免客户端在其他可用的 apiserver 实例上重建 WATCH 时
			// 造成惊群效应。
			return
		case <-done:
			return
		case <-timeoutCh:
			return
		case event, ok := <-ch:
			if !ok {
				// 结果结束。
				return
			}
			// watch-list 的首批事件以带注解的 Bookmark 结束，借此记录初始化延迟。
			isWatchListLatencyRecordingRequired := shouldRecordWatchListLatency(req.Context(), event)

			if err := watchEncoder.Encode(event); err != nil {
				utilruntime.HandleErrorWithContext(req.Context(), err, "Failed to encode watch event")
				// 客户端断开。
				return
			}
			recorder.RecordEvent()

			// 只在队列排空时 flush：积压时连续写可以减少 syscall 与分块开销。
			if len(ch) == 0 {
				if err := rw.Flush(); err != nil {
					utilruntime.HandleErrorWithContext(req.Context(), err, "Failed to flush watch response")
					return
				}
			}
			if isWatchListLatencyRecordingRequired {
				// 记录 WatchList 初始列表阶段的完成。
				receivedTimestamp, ok := apirequest.ReceivedTimestampFrom(req.Context())
				if !ok {
					utilruntime.HandleErrorWithContext(req.Context(), nil, "Unable to measure watchlist latency, no received timestamp found in the context", "gvr", s.Scope.Resource)
				} else {
					initLatency := time.Since(receivedTimestamp)
					metrics.RecordWatchListLatency(req.Context(), s.Scope.Resource, s.metricsScope, initLatency)
					auditID := audit.GetAuditIDTruncated(req.Context())
					klog.V(3).InfoS("WatchList initial events sent", "path", req.URL.Path, "auditID", auditID, "initLatency", initLatency)
					httplog.AddKeyValue(req.Context(), "watchlist_init_latency", initLatency)
					span.AddEvent("Writing initial events done")
					span.End(5 * time.Second)
					s.watchListCompleteHook()
				}
				// 把 gzip writer 归还给 pool，避免空闲 watch 长期占用 gzip 状态。
				// 空闲的 watch 可能持续数小时，长期占用 gzip 状态是内存浪费。
				if err := rw.Flush(); err != nil {
					utilruntime.HandleErrorWithContext(req.Context(), err, "Failed to flush watch response after initial events")
					return
				}
			}
		}
	}
}

// HandleWS 通过 websocket 连接输出一串已编码的事件。
func (s *WatchServer) HandleWS(ws *websocket.Conn) {
	ctx := ws.Request().Context()
	logger := klog.FromContext(ctx)

	defer func() {
		if s.MemoryAllocator != nil {
			runtime.AllocatorPool.Put(s.MemoryAllocator)
		}
	}()

	defer ws.Close()
	done := make(chan struct{})
	// 确保连接会超时。
	timeoutCh, cleanup := s.TimeoutFactory.TimeoutCh()
	defer cleanup()

	// 单独一个 goroutine 读取客户端数据只是为了感知断连；watch 协议中
	// 客户端不应发送任何内容，收到的一律丢弃。
	go func() {
		defer utilruntime.HandleCrashWithLogger(logger)
		// 此处会阻塞直到连接关闭。
		// 客户端不应该发送任何内容。
		wsstream.IgnoreReceivesWithLogger(logger, ws, 0)
		// 客户端关闭后，我们也应当关闭
		close(done)
	}()

	framer := newWebsocketFramer(ws, s.UseTextFraming)

	gvr := s.Scope.Resource
	watchEncoder := newWatchEncoder(ctx, gvr, s.EmbeddedEncoder, s.Encoder, framer)
	ch := s.Watching.ResultChan()

	for {
		select {
		case <-done:
			return
		case <-timeoutCh:
			return
		case event, ok := <-ch:
			if !ok {
				// 结果结束。
				return
			}

			if err := watchEncoder.Encode(event); err != nil {
				utilruntime.HandleErrorWithLogger(logger, err, "Failed to encode watch event")
				// 客户端断开。
				return
			}
		}
	}
}

// websocketFramer 把编码后的事件作为单条 WebSocket 消息发送。
type websocketFramer struct {
	ws             *websocket.Conn
	useTextFraming bool
}

func newWebsocketFramer(ws *websocket.Conn, useTextFraming bool) io.Writer {
	return &websocketFramer{
		ws:             ws,
		useTextFraming: useTextFraming,
	}
}

func (w *websocketFramer) Write(p []byte) (int, error) {
	// JSON 等文本媒体类型使用 text frame，二进制格式使用 binary frame；
	// 浏览器端依赖这一区分来正确解码。
	if w.useTextFraming {
		// bytes.Buffer::String() 对 nil 有特殊处理，但由于写入的是序列化后的 watch 事件，
		// 这里不会出现该情况。
		if err := websocket.Message.Send(w.ws, string(p)); err != nil {
			return 0, err
		}
		return len(p), nil
	}
	if err := websocket.Message.Send(w.ws, p); err != nil {
		return 0, err
	}
	return len(p), nil
}

var _ io.Writer = &websocketFramer{}

// shouldRecordWatchListLatency 判断该事件是否标记了 watch-list 初始事件阶段结束。
// 只有 cacher 在满足条件时才会给唯一的 Bookmark 事件加该注解。
func shouldRecordWatchListLatency(ctx context.Context, event watch.Event) bool {
	if event.Type != watch.Bookmark || !utilfeature.DefaultFeatureGate.Enabled(features.WatchList) {
		return false
	}
	// 目前 initial-events-end 注解只会由 watch cache 添加到单个事件上，
	// 且只在满足特定条件时添加。
	// 更多背景请阅读 https://github.com/kubernetes/enhancements/tree/master/keps/sig-api-machinery/3157-watch-list#design
	hasAnnotation, err := storage.HasInitialEventsEndBookmarkAnnotation(event.Object)
	if err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Unable to determine if the object has the required annotation for measuring watchlist latency", "object", fmt.Sprintf("%T", event.Object))
		return false
	}
	return hasAnnotation
}
