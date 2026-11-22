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

// StreamObject performs input stream negotiation from a ResourceStreamer and writes that to the response.
// If the client requests a websocket upgrade, negotiate for a websocket reader protocol (because many
// browser clients cannot easily handle binary streaming protocols).
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
		// No output provided - return StatusNoContent
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
	// Flush headers, if possible
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

// SerializeObject renders an object in the content type negotiated by the client using the provided encoder.
// The context is optional and can be nil. This method will perform optional content compression if requested by
// a client and the feature gate for APIResponseCompression is enabled.
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
			// we cannot write an error to the writer anymore as the Encode call was successful.
			utilruntime.HandleError(fmt.Errorf("apiserver was unable to close cleanly the response writer: %v", err))
		}
		return
	}

	// make a best effort to write the object if a failure is detected
	//
	// 尽力而为：丢弃已缓冲内容，把编码错误转换为 Status 响应，避免客户端收到半截 JSON。
	utilruntime.HandleError(fmt.Errorf("apiserver was unable to write a %s response: %w", w.mediaType, err))
	w.discardBufferedResponse()
	status := ErrorToAPIStatus(err)
	candidateStatusCode := int(status.Code)
	// if the current status code is successful, allow the error's status code to overwrite it
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
	// defaultGzipThresholdBytes is compared to the size of the first write from the stream
	// (usually the entire object), and if the size is smaller no gzipping will be performed
	// if the client requests it.
	//
	// 阈值的存在是为了避免压缩小响应反而增加 CPU 与延迟。
	defaultGzipThresholdBytes = 128 * 1024
	// Use the length of the first write to recognize streaming implementations.
	// When streaming JSON first write is "{", while Kubernetes protobuf starts unique 4 byte header.
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
	// totalBytes is the number of bytes written to `w` and does not include buffered bytes
	totalBytes int
	// lastWriteErr holds the error result (if any) of the last write attempt to `w`
	lastWriteErr error

	ctx context.Context
}

// Write 实现 io.Writer，按情况选择缓冲、透传或 gzip。
func (w *deferredResponseWriter) Write(p []byte) (n int, err error) {
	switch {
	case w.hasWritten:
		// already written, cannot buffer
		return w.unbufferedWrite(p)

	case w.contentEncoding != "gzip":
		// non-gzip, no need to buffer
		return w.unbufferedWrite(p)

	case !w.hasBuffered && len(p) > defaultGzipThresholdBytes:
		// not yet buffered, first write is long enough to trigger gzip, no need to buffer
		return w.unbufferedWrite(p)

	case !w.hasBuffered && len(p) > firstWriteStreamingThresholdBytes:
		// not yet buffered, first write is longer than expected for streaming scenarios that would require buffering, no need to buffer
		return w.unbufferedWrite(p)

	default:
		if !w.hasBuffered {
			w.hasBuffered = true
			// Start at 80 bytes to avoid rapid reallocation of the buffer.
			// The minimum size of a 0-item serialized list object is 80 bytes:
			// {"kind":"List","apiVersion":"v1","metadata":{"resourceVersion":"1"},"items":[]}\n
			w.buffer = make([]byte, 0, max(80, len(p)))
		}
		w.buffer = append(w.buffer, p...)
		var err error
		if len(w.buffer) > defaultGzipThresholdBytes {
			// we've accumulated enough to trigger gzip, write and clear buffer
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
		// never reached defaultGzipThresholdBytes, no need to do the gzip writer cleanup
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

// WriteObjectNegotiated renders an object in the content type negotiated by the client.
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
		// if original statusCode was not successful we need to return the original error
		// we cannot hide it behind negotiation problems
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

// ErrorNegotiated renders an error to the response. Returns the HTTP status code of the error.
// The context is optional and may be nil.
//
// 返回状态码供调用方记录指标或做后续判断；204 无响应体，直接写状态行。
func ErrorNegotiated(err error, s runtime.NegotiatedSerializer, gv schema.GroupVersion, w http.ResponseWriter, req *http.Request) int {
	status := ErrorToAPIStatus(err)
	code := int(status.Code)
	// when writing an error, check to see if the status indicates a retry after period
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

// WriteRawJSON writes a non-API object in JSON.
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
