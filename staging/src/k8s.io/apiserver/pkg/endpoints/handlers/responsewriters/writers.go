/*
Copyright 2016 The Kubernetes Authors.

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

package responsewriters

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"k8s.io/apiserver/pkg/features"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/endpoints/handlers/negotiation"
	"k8s.io/apiserver/pkg/endpoints/metrics"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/apiserver/pkg/util/flushwriter"
	"k8s.io/component-base/tracing"
	"k8s.io/streaming/pkg/httpstream/wsstream"
)

// StreamObject 从 ResourceStreamer 做输入流协商，并把流写到 response 上。
// 若客户端请求 websocket 升级，则协商 websocket reader 协议（因为浏览器
// 难以处理二进制流协议）。
//
// 流式响应不经过 SerializeObject，因此也不参与 gzip 压缩与响应体审计；
// 适用场景（logs/exec/attach/portforward）本身已有各自的协议约定。
func StreamObject(statusCode int, gv schema.GroupVersion, s runtime.NegotiatedSerializer, stream rest.ResourceStreamer, w http.ResponseWriter, req *http.Request) {
	out, flush, contentType, err := stream.InputStream(req.Context(), gv.String(), req.Header.Get("Accept"))
	if err != nil {
		ErrorNegotiated(err, s, gv, w, req)
		return
	}
	if out == nil {
		// 没有任何输出内容 —— 返回 StatusNoContent。
		w.WriteHeader(http.StatusNoContent)
		return
	}
	defer out.Close()

	// websocket 分支必须早于普通写路径：升级后不能再写 Content-Type/状态行。
	if wsstream.IsWebSocketRequest(req) {
		r := wsstream.NewReader(out, true, wsstream.NewDefaultReaderProtocols())
		if err := r.Copy(w, req); err != nil {
			utilruntime.HandleError(fmt.Errorf("error encountered while streaming results via websocket: %v", err))
		}
		return
	}

	if len(contentType) == 0 {
		contentType = "application/octet-stream"
	}
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(statusCode)
	// 尽可能 flush 响应头。
	//
	// 先刷出头部，让客户端尽早知道连接已建立，而不是等首块数据。
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
	writer := w.(io.Writer)
	// 需要逐块刷新时用 flushwriter 包装，保证每块数据立即到达客户端（如日志流）。
	if flush {
		writer = flushwriter.Wrap(w)
	}
	io.Copy(writer, out)
}

// SerializeObject 用给定序列化器按客户端协商的内容类型渲染对象。
// context 可选，可以为 nil。若客户端支持且开启 APIResponseCompression 特性门控，
// 本方法会执行可选的响应内容压缩。
//
// 延迟写入：编码成功后才真正写出响应，因此编码中途失败时可以丢弃已缓冲内容并改写为错误状态。
func SerializeObject(mediaType string, encoder runtime.Encoder, hw http.ResponseWriter, req *http.Request, statusCode int, object runtime.Object) {
	ctx := req.Context()
	ctx, span := tracing.Start(ctx, "SerializeObject",
		attribute.String("audit-id", audit.GetAuditIDTruncated(ctx)),
		attribute.String("method", req.Method),
		attribute.String("url", req.URL.Path),
		attribute.String("protocol", req.Proto),
		attribute.String("mediaType", mediaType),
		attribute.String("encoder", string(encoder.Identifier())))
	req = req.WithContext(ctx)
	defer span.End(5 * time.Second)

	w := &deferredResponseWriter{
		mediaType:       mediaType,
		statusCode:      statusCode,
		contentEncoding: responseContentEncodingSupported(req),
		hw:              hw,
		ctx:             ctx,
	}

	// 编码器若支持分配器，复用池中的缓冲以减少大对象序列化时的分配压力。
	var memoryAllocator runtime.MemoryAllocator
	if encoderWithAllocator, supportsAllocator := encoder.(runtime.EncoderWithAllocator); supportsAllocator {
		memoryAllocator = runtime.AllocatorPool.Get().(*runtime.Allocator)
		encoder = runtime.NewEncoderWithAllocator(encoderWithAllocator, memoryAllocator)
	}
	if memoryAllocator != nil {
		defer runtime.AllocatorPool.Put(memoryAllocator)
	}

	err := encoder.Encode(object, w)
	if err == nil {
		err = w.Close()
		if err != nil {
			// Encode 调用已成功，此时再也无法向 writer 写错误了。
			utilruntime.HandleError(fmt.Errorf("apiserver was unable to close cleanly the response writer: %v", err))
		}
		return
	}

	// 检测到失败时，尽力把对象写出去。
	//
	// 尽力而为：丢弃已缓冲内容，把编码错误转换为 Status 响应，避免客户端收到半截 JSON。
	utilruntime.HandleError(fmt.Errorf("apiserver was unable to write a %s response: %w", w.mediaType, err))
	w.discardBufferedResponse()
	status := ErrorToAPIStatus(err)
	candidateStatusCode := int(status.Code)
	// 若当前状态码是成功的，则允许用错误中的状态码覆盖它。
	if statusCode >= http.StatusOK && statusCode < http.StatusBadRequest {
		w.statusCode = candidateStatusCode
	}
	output, err := runtime.Encode(encoder, status)
	if err != nil {
		// 连错误对象都无法编码时退化为纯文本，保证至少返回可读信息。
		w.mediaType = "text/plain"
		output = []byte(fmt.Sprintf("%s: %s", status.Reason, status.Message))
	}
	if _, err := w.Write(output); err != nil {
		utilruntime.HandleError(fmt.Errorf("apiserver was unable to write a fallback %s response: %w", w.mediaType, err))
	}
	w.Close()
}

// gzipPool 复用 gzip.Writer，避免每个响应都分配压缩状态；启动期构造失败直接 panic。
var gzipPool = NewGzipWriterPoolOrDie()

const (
	// defaultGzipThresholdBytes 与流的首次写入长度（通常是整个对象）比较，
	// 若小于该阈值且客户端要求 gzip，也不做压缩。
	//
	// 阈值的存在是为了避免压缩小响应反而增加 CPU 与延迟。
	defaultGzipThresholdBytes = 128 * 1024
	// 用首次写入的长度识别流式实现：
	// 流式 JSON 的第一次写入是 "{"，而 Kubernetes protobuf 以唯一的 4 字节头开始。
	//
	// 首个写入不超过 4 字节通常意味着流式/分块输出，此时不应缓冲等待压缩。
	firstWriteStreamingThresholdBytes = 4
)

// deferredResponseWriter 延迟决定是否 gzip、以及何时写状态行：
// 只有确认首个数据块超过阈值时才启用压缩，否则直接透传。
type deferredResponseWriter struct {
	mediaType       string
	statusCode      int
	contentEncoding string

	hasBuffered bool
	buffer      []byte
	hasWritten  bool
	hw          http.ResponseWriter
	w           io.Writer
	// totalBytes 是已写入 `w` 的字节数，不含 buffer 中尚未写出的部分。
	totalBytes int
	// lastWriteErr 保存最近一次向 `w` 写入的错误结果（若有）。
	lastWriteErr error

	ctx context.Context
}

// Write 实现 io.Writer，按情况选择缓冲、透传或 gzip。
func (w *deferredResponseWriter) Write(p []byte) (n int, err error) {
	switch {
	case w.hasWritten:
		// 已经写出过，无法再缓冲。
		return w.unbufferedWrite(p)

	case w.contentEncoding != "gzip":
		// 非 gzip 场景，无需缓冲。
		return w.unbufferedWrite(p)

	case !w.hasBuffered && len(p) > defaultGzipThresholdBytes:
		// 尚未缓冲：首次写入已足够长会触发 gzip，无需缓冲。
		return w.unbufferedWrite(p)

	case !w.hasBuffered && len(p) > firstWriteStreamingThresholdBytes:
		// 尚未缓冲：首次写入长度超过需要缓冲的流式场景预期，直接写出。
		return w.unbufferedWrite(p)

	default:
		if !w.hasBuffered {
			w.hasBuffered = true
			// 从 80 字节起步，避免 buffer 频繁重新分配。
			// 空列表（0 项）序列化后的最小长度就是 80 字节：
			// {"kind":"List","apiVersion":"v1","metadata":{"resourceVersion":"1"},"items":[]}\n
			w.buffer = make([]byte, 0, max(80, len(p)))
		}
		w.buffer = append(w.buffer, p...)
		var err error
		if len(w.buffer) > defaultGzipThresholdBytes {
			// 已累计到足以触发 gzip 的量，写出并清空 buffer。
			_, err = w.unbufferedWrite(w.buffer)
			w.buffer = nil
		}
		return len(p), err
	}
}

// discardBufferedResponse 丢弃尚未写出的缓冲；已写出时无操作（无法撤回）。
func (w *deferredResponseWriter) discardBufferedResponse() {
	if w.hasWritten {
		return
	}
	w.hasBuffered = false
	w.buffer = nil
}

// unbufferedWrite 在首次写入时确定编码方式、写头部并锁定 Content-Type；
// 后续写入直接复用已选定的 writer。
func (w *deferredResponseWriter) unbufferedWrite(p []byte) (n int, err error) {
	defer func() {
		w.totalBytes += n
		w.lastWriteErr = err
	}()

	if w.hasWritten {
		return w.w.Write(p)
	}
	w.hasWritten = true

	hw := w.hw
	header := hw.Header()
	switch {
	case w.contentEncoding == "gzip" && len(p) > defaultGzipThresholdBytes:
		header.Set("Content-Encoding", "gzip")
		// Vary 告诉缓存层：同一 URL 因 Accept-Encoding 不同而内容不同，避免缓存串味。
		header.Add("Vary", "Accept-Encoding")

		gw := gzipPool.Get().(*gzip.Writer)
		gw.Reset(hw)

		w.w = gw
	default:
		w.w = hw
	}

	span := tracing.SpanFromContext(w.ctx)
	span.AddEvent("About to start writing response",
		attribute.String("writer", fmt.Sprintf("%T", w.w)),
		attribute.Int("size", len(p)),
	)

	header.Set("Content-Type", w.mediaType)
	hw.WriteHeader(w.statusCode)
	return w.w.Write(p)
}

// Close 在未写出任何数据时把缓冲内容刷出；使用 gzip 时关闭并归还 writer。
func (w *deferredResponseWriter) Close() (err error) {
	defer func() {
		if !w.hasWritten {
			return
		}

		span := tracing.SpanFromContext(w.ctx)

		if w.lastWriteErr != nil {
			span.AddEvent("Write call failed",
				attribute.Int("size", w.totalBytes),
				attribute.String("err", w.lastWriteErr.Error()))
		} else {
			span.AddEvent("Write call succeeded",
				attribute.Int("size", w.totalBytes))
		}
	}()

	if !w.hasWritten {
		if !w.hasBuffered {
			return nil
		}
		// 始终未达到 defaultGzipThresholdBytes，无需清理 gzip writer。
		_, err := w.unbufferedWrite(w.buffer)
		w.buffer = nil
		return err
	}

	switch t := w.w.(type) {
	case *gzip.Writer:
		err = t.Close()
		// Reset(nil) 断开对底层连接的引用，防止连接被池中的 writer 长期持有。
		t.Reset(nil)
		gzipPool.Put(t)
	}
	return err
}

// WriteObjectNegotiated 按客户端协商出的内容类型渲染对象。
//
// 若对象实现了 ResourceStreamer，则走流式路径并计入长耗时请求指标；
// 否则按内容协商结果选择序列化器。协商失败时若原状态码本身是错误，优先保留原错误。
func WriteObjectNegotiated(s runtime.NegotiatedSerializer, restrictions negotiation.EndpointRestrictions, gv schema.GroupVersion, w http.ResponseWriter, req *http.Request, statusCode int, object runtime.Object, listGVKInContentType bool) {
	stream, ok := object.(rest.ResourceStreamer)
	if ok {
		requestInfo, _ := request.RequestInfoFrom(req.Context())
		metrics.RecordLongRunning(req, requestInfo, metrics.APIServerComponent, func() {
			StreamObject(statusCode, gv, s, stream, w, req)
		})
		return
	}

	mediaType, serializer, err := negotiation.NegotiateOutputMediaType(req, s, restrictions)
	if err != nil {
		// 若原始 statusCode 不是成功，需要返回原始错误，
		// 不能把它隐藏在协商问题之后。
		if statusCode < http.StatusOK || statusCode >= http.StatusBadRequest {
			WriteRawJSON(int(statusCode), object, w)
			return
		}
		status := ErrorToAPIStatus(err)
		WriteRawJSON(int(status.Code), status, w)
		return
	}

	// 审计响应体：必须在编码前记录，否则对象可能已被编码器修改。
	audit.LogResponseObject(req.Context(), object, gv, s)

	var encoder runtime.Encoder
	// CBOR 特性开启时使用非确定性编码：CBOR map 顺序无关，避免为稳定顺序付出排序成本。
	if utilfeature.DefaultFeatureGate.Enabled(features.CBORServingAndStorage) {
		encoder = s.EncoderForVersion(runtime.UseNondeterministicEncoding(serializer.Serializer), gv)
	} else {
		encoder = s.EncoderForVersion(serializer.Serializer, gv)
	}
	request.TrackSerializeResponseObjectLatency(req.Context(), func() {
		if listGVKInContentType {
			SerializeObject(generateMediaTypeWithGVK(serializer.MediaType, mediaType.Convert), encoder, w, req, statusCode, object)
		} else {
			SerializeObject(serializer.MediaType, encoder, w, req, statusCode, object)
		}
	})
}

// generateMediaTypeWithGVK 把转换目标 GVK 附加到 media type 参数中，
// 供客户端（尤其是表格/部分对象场景）知道响应体实际是哪种类型。
func generateMediaTypeWithGVK(mediaType string, gvk *schema.GroupVersionKind) string {
	if gvk == nil {
		return mediaType
	}
	if gvk.Group != "" {
		mediaType += ";g=" + gvk.Group
	}
	if gvk.Version != "" {
		mediaType += ";v=" + gvk.Version
	}
	if gvk.Kind != "" {
		mediaType += ";as=" + gvk.Kind
	}
	return mediaType
}

// ErrorNegotiated 把错误渲染到响应中，并返回错误的 HTTP 状态码。
// context 可选，可以为 nil。
//
// 返回状态码供调用方记录指标或做后续判断；204 无响应体，直接写状态行。
func ErrorNegotiated(err error, s runtime.NegotiatedSerializer, gv schema.GroupVersion, w http.ResponseWriter, req *http.Request) int {
	status := ErrorToAPIStatus(err)
	code := int(status.Code)
	// 写错误时检查 status 中是否带有重试间隔提示。
	if status.Details != nil && status.Details.RetryAfterSeconds > 0 {
		delay := strconv.Itoa(int(status.Details.RetryAfterSeconds))
		w.Header().Set("Retry-After", delay)
	}

	if code == http.StatusNoContent {
		w.WriteHeader(code)
		return code
	}

	WriteObjectNegotiated(s, negotiation.DefaultEndpointRestrictions, gv, w, req, code, status, false)
	return code
}

// WriteRawJSON 以 JSON 写出一个非 API 对象。
//
// 用于内容协商失败等无法走正常序列化器的兜底路径；缩进格式便于人阅读。
func WriteRawJSON(statusCode int, object interface{}, w http.ResponseWriter) {
	output, err := json.MarshalIndent(object, "", "  ")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	w.Write(output)
}
