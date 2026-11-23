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

package portforward

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"k8s.io/streaming/pkg/httpstream"
	"k8s.io/streaming/pkg/httpstream/spdy"
	utilruntime "k8s.io/streaming/pkg/runtime"

	"k8s.io/klog/v2"
)

// handleHTTPStreams 处理基于 HTTP Upgrade（SPDY 风格 httpstream）的端口转发请求。
// 它先完成协议协商并建立升级后的连接，再由主循环处理该连接上的所有流。
func handleHTTPStreams(req *http.Request, w http.ResponseWriter, portForwarder PortForwarder, podName string, uid string, supportedPortForwardProtocols []string, idleTimeout, streamCreationTimeout time.Duration) error {
	// 先做协议协商：只有客户端与服务端支持同一种端口转发子协议才能继续。
	_, err := httpstream.Handshake(req, w, supportedPortForwardProtocols)
	// negotiated protocol isn't currently used server side, but could be in the future
	if err != nil {
		// Handshake writes the error to the client
		// 握手失败时错误响应已由 Handshake 写给客户端，这里只需把错误上抛。
		return err
	}
	// 用带缓冲的通道接收新建的流，避免流回调阻塞在协议层的接收 goroutine 中。
	streamChan := make(chan httpstream.Stream, 1)

	klog.V(5).InfoS("Upgrading port forward response")
	upgrader := spdy.NewResponseUpgrader()
	// 将 HTTP 响应升级为 SPDY 连接，后续新建的流都会被送入 streamChan。
	conn := upgrader.UpgradeResponse(w, req, httpStreamReceived(streamChan))
	if conn == nil {
		return errors.New("unable to upgrade httpstream connection")
	}
	// 函数返回时关闭连接，确保底层资源总是被释放。
	defer conn.Close()

	klog.V(5).InfoS("Connection setting port forwarding streaming connection idle timeout", "connection", conn, "idleTimeout", idleTimeout)
	// 设置空闲超时，防止长期无数据的连接一直占用资源。
	conn.SetIdleTimeout(idleTimeout)

	// 构造处理器，集中管理该连接上的流对以及真正执行转发的 forwarder。
	h := &httpStreamHandler{
		conn:                  conn,
		streamChan:            streamChan,
		streamPairs:           make(map[string]*httpStreamPair),
		streamCreationTimeout: streamCreationTimeout,
		pod:                   podName,
		uid:                   uid,
		forwarder:             portForwarder,
	}
	// 进入主循环，直到连接被关闭才返回。
	h.run()

	return nil
}

// httpStreamReceived is the httpstream.NewStreamHandler for port
// forward streams. It checks each stream's port and stream type headers,
// rejecting any streams that with missing or invalid values. Each valid
// stream is sent to the streams channel.
// httpStreamReceived 返回端口转发流使用的 httpstream.NewStreamHandler 回调。
// 它逐条校验流携带的端口和流类型头，拒绝缺失或取值非法的流；合法的流会被送入 streams 通道。
func httpStreamReceived(streams chan httpstream.Stream) func(httpstream.Stream, <-chan struct{}) error {
	return func(stream httpstream.Stream, replySent <-chan struct{}) error {
		// make sure it has a valid port header
		// 端口头必须存在且能解析为 1 到 65535 之间的十进制整数。
		portString := stream.Headers().Get(PortHeader)
		if len(portString) == 0 {
			return fmt.Errorf("%q header is required", PortHeader)
		}
		port, err := strconv.ParseUint(portString, 10, 16)
		if err != nil {
			return fmt.Errorf("unable to parse %q as a port: %v", portString, err)
		}
		if port < 1 {
			return fmt.Errorf("port %q must be > 0", portString)
		}

		// make sure it has a valid stream type header
		// 流类型头决定该流承担数据通道还是错误通道的角色，取值只能是 data 或 error。
		streamType := stream.Headers().Get(StreamType)
		if len(streamType) == 0 {
			return fmt.Errorf("%q header is required", StreamType)
		}
		if streamType != StreamTypeError && streamType != StreamTypeData {
			return fmt.Errorf("invalid stream type %q", streamType)
		}

		// 校验通过后才把流交给主循环；replySent 在本实现中无需等待。
		streams <- stream
		return nil
	}
}

// httpStreamHandler is capable of processing multiple port forward
// requests over a single httpstream.Connection.
// httpStreamHandler 负责在单条 httpstream.Connection 上处理多个端口转发请求。
// 字段含义：conn 是已升级的连接；streamChan 接收新建的流；streamPairsLock 保护并发访问的流对表；
// streamPairs 按 requestID 索引每个请求的数据/错误流对；streamCreationTimeout 是等待一对流建立的最长时间；
// pod 与 uid 标识目标 Pod；forwarder 是真正执行端口转发的组件。
type httpStreamHandler struct {
	conn                  httpstream.Connection
	streamChan            chan httpstream.Stream
	streamPairsLock       sync.RWMutex
	streamPairs           map[string]*httpStreamPair
	streamCreationTimeout time.Duration
	pod                   string
	uid                   string
	forwarder             PortForwarder
}

// getStreamPair returns a httpStreamPair for requestID. This creates a
// new pair if one does not yet exist for the requestID. The returned bool is
// true if the pair was created.
// getStreamPair 返回 requestID 对应的流对；不存在时新建一个。返回的布尔值表示流对是否为本次新建。
func (h *httpStreamHandler) getStreamPair(requestID string) (*httpStreamPair, bool) {
	// 流对表会被主循环和监控协程并发访问，必须加锁保护。
	h.streamPairsLock.Lock()
	defer h.streamPairsLock.Unlock()

	// 同一请求的数据流与错误流可能先后到达，先查看是否已有记录。
	if p, ok := h.streamPairs[requestID]; ok {
		klog.V(5).InfoS("Connection request found existing stream pair", "connection", h.conn, "request", requestID)
		return p, false
	}

	klog.V(5).InfoS("Connection request creating new stream pair", "connection", h.conn, "request", requestID)

	// 首次见到该 requestID，创建流对并登记到表中。
	p := newPortForwardPair(requestID)
	h.streamPairs[requestID] = p

	return p, true
}

// monitorStreamPair waits for the pair to receive both its error and data
// streams, or for the timeout to expire (whichever happens first), and then
// removes the pair.
// monitorStreamPair 等待流对收齐错误流和数据流，或等待超时（以先到者为准），随后移除该流对。
func (h *httpStreamHandler) monitorStreamPair(p *httpStreamPair, timeout <-chan time.Time) {
	// 收不齐两条流就说明客户端已放弃或出错，需要把超时错误回写到错误流。
	select {
	case <-timeout:
		err := fmt.Errorf("(conn=%p, request=%s) timed out waiting for streams", h.conn, p.requestID)
		utilruntime.HandleError(err)
		p.printError(err.Error())
	case <-p.complete:
		klog.V(5).InfoS("Connection request successfully received error and data streams", "connection", h.conn, "request", p.requestID)
	}
	// 无论成功还是超时，都要清理表中记录，避免流对长期滞留。
	h.removeStreamPair(p.requestID)
}

// hasStreamPair returns a bool indicating if a stream pair for requestID
// exists.
// hasStreamPair 报告 requestID 对应的流对是否仍存在于表中。
func (h *httpStreamHandler) hasStreamPair(requestID string) bool {
	h.streamPairsLock.RLock()
	defer h.streamPairsLock.RUnlock()

	_, ok := h.streamPairs[requestID]
	return ok
}

// removeStreamPair removes the stream pair identified by requestID from streamPairs.
// removeStreamPair 从流对表中删除 requestID 对应的流对，并注销其底层流。
func (h *httpStreamHandler) removeStreamPair(requestID string) {
	h.streamPairsLock.Lock()
	defer h.streamPairsLock.Unlock()

	if h.conn != nil {
		pair := h.streamPairs[requestID]
		// 从连接中注销这对流，避免后续新流被误判为重复流。
		h.conn.RemoveStreams(pair.dataStream, pair.errorStream)
	}
	delete(h.streamPairs, requestID)
}

// requestID returns the request id for stream.
// requestID 返回流所属的端口转发请求 ID。
func (h *httpStreamHandler) requestID(stream httpstream.Stream) string {
	requestID := stream.Headers().Get(PortForwardRequestIDHeader)
	if len(requestID) == 0 {
		klog.V(5).InfoS("Connection stream received without requestID header", "connection", h.conn)
		// If we get here, it's because the connection came from an older client
		// that isn't generating the request id header
		// (https://github.com/kubernetes/kubernetes/blob/843134885e7e0b360eb5441e85b1410a8b1a7a0c/pkg/client/unversioned/portforward/portforward.go#L258-L287)
		//
		// This is a best-effort attempt at supporting older clients.
		//
		// When there aren't concurrent new forwarded connections, each connection
		// will have a pair of streams (data, error), and the stream IDs will be
		// consecutive odd numbers, e.g. 1 and 3 for the first connection. Convert
		// the stream ID into a pseudo-request id by taking the stream type and
		// using id = stream.Identifier() when the stream type is error,
		// and id = stream.Identifier() - 2 when it's data.
		//
		// NOTE: this only works when there are not concurrent new streams from
		// multiple forwarded connections; it's a best-effort attempt at supporting
		// old clients that don't generate request ids.  If there are concurrent
		// new connections, it's possible that 1 connection gets streams whose IDs
		// are not consecutive (e.g. 5 and 9 instead of 5 and 7).
		// 兼容旧客户端：从流类型和流 ID 推导伪 requestID，让同一请求的两条流得到相同的值。
		streamType := stream.Headers().Get(StreamType)
		switch streamType {
		case StreamTypeError:
			// 错误流的流 ID 直接作为请求 ID。
			requestID = strconv.Itoa(int(stream.Identifier()))
		case StreamTypeData:
			// 数据流的流 ID 比错误流小 2，减去 2 后与错误流对齐。
			requestID = strconv.Itoa(int(stream.Identifier()) - 2)
		}

		klog.V(5).InfoS("Connection automatically assigning request ID from stream type and stream ID", "connection", h.conn, "request", requestID, "streamType", streamType, "stream", stream.Identifier())
	}
	return requestID
}

// run is the main loop for the httpStreamHandler. It processes new
// streams, invoking portForward for each complete stream pair. The loop exits
// when the httpstream.Connection is closed.
// run 是主循环：处理新到达的流，并在流对收齐后调用 portForward；连接关闭时循环退出。
func (h *httpStreamHandler) run() {
	klog.V(5).InfoS("Connection waiting for port forward streams", "connection", h.conn)
Loop:
	for {
		// 同时等待连接关闭和新流到达两类事件。
		select {
		case <-h.conn.CloseChan():
			klog.V(5).InfoS("Connection upgraded connection closed", "connection", h.conn)
			break Loop
		case stream := <-h.streamChan:
			// 根据流头算出所属请求，再把流归入对应的流对。
			requestID := h.requestID(stream)
			streamType := stream.Headers().Get(StreamType)
			klog.V(5).InfoS("Connection request received new type of stream", "connection", h.conn, "request", requestID, "streamType", streamType)

			p, created := h.getStreamPair(requestID)
			if created {
				// 首次创建流对时启动监控协程，防止只收到一条流却一直挂着。
				go h.monitorStreamPair(p, time.After(h.streamCreationTimeout))
			}
			// 加入流对后若两条流已收齐就启动转发；加入失败则把错误写回错误流。
			if complete, err := p.add(stream); err != nil {
				msg := fmt.Sprintf("error processing stream for request %s: %v", requestID, err)
				utilruntime.HandleError(errors.New(msg))
				p.printError(msg)
			} else if complete {
				go h.portForward(p)
			}
		}
	}
}

// portForward invokes the httpStreamHandler's forwarder.PortForward
// function for the given stream pair.
// portForward 为给定流对调用 forwarder.PortForward，把实际的数据转发委派给 forwarder。
func (h *httpStreamHandler) portForward(p *httpStreamPair) {
	ctx := context.Background()
	// 转发结束后无论成功与否都要关闭两条流。
	defer p.dataStream.Close()
	defer p.errorStream.Close()

	portString := p.dataStream.Headers().Get(PortHeader)
	// 端口头已在 httpStreamReceived 中校验过，这里直接解析。
	port, _ := strconv.ParseInt(portString, 10, 32)

	klog.V(5).InfoS("Connection request invoking forwarder.PortForward for port", "connection", h.conn, "request", p.requestID, "port", portString)
	// 阻塞执行端口转发，直到客户端断开或转发结束。
	err := h.forwarder.PortForward(ctx, h.pod, h.uid, int32(port), p.dataStream)
	klog.V(5).InfoS("Connection request done invoking forwarder.PortForward for port", "connection", h.conn, "request", p.requestID, "port", portString)

	if err != nil {
		msg := fmt.Errorf("error forwarding port %d to pod %s, uid %v: %v", port, h.pod, h.uid, err)
		utilruntime.HandleError(msg)
		// 把错误文本写入错误流，让客户端知道转发失败。
		fmt.Fprint(p.errorStream, msg.Error())
		// receiving an error from a port-forward operation indicates a problem
		// with data stream most probably, thus we want to reset the streams
		// indicating there was a problem and allow a new set of streams be
		// created to mitigate the problem
		// 同时重置两条流，通知客户端需要重新建立流对才能继续转发。
		p.dataStream.Reset()  // nolint:errcheck
		p.errorStream.Reset() // nolint:errcheck
	}
}

// httpStreamPair represents the error and data streams for a port
// forwarding request.
// httpStreamPair 表示一个端口转发请求对应的数据/错误流对。
// 字段含义：lock 保护并发读写；requestID 标识所属请求；dataStream 承载转发数据；
// errorStream 回传错误信息；complete 在两条流都到齐时关闭，用于通知监控协程。
type httpStreamPair struct {
	lock        sync.RWMutex
	requestID   string
	dataStream  httpstream.Stream
	errorStream httpstream.Stream
	complete    chan struct{}
}

// newPortForwardPair creates a new httpStreamPair.
// newPortForwardPair 为指定请求创建一个新的流对，并初始化完成通知通道。
func newPortForwardPair(requestID string) *httpStreamPair {
	return &httpStreamPair{
		requestID: requestID,
		complete:  make(chan struct{}),
	}
}

// add adds the stream to the httpStreamPair. If the pair already
// contains a stream for the new stream's type, an error is returned. add
// returns true if both the data and error streams for this pair have been
// received.
// add 把一条流登记到流对中；同类型的流重复到达时返回错误。返回值表示数据/错误流是否都已收齐。
func (p *httpStreamPair) add(stream httpstream.Stream) (bool, error) {
	// 流对由主循环写入、监控协程读取，需要加锁。
	p.lock.Lock()
	defer p.lock.Unlock()

	// 按流类型把流放到对应的槽位，重复放置视为协议错误。
	switch stream.Headers().Get(StreamType) {
	case StreamTypeError:
		if p.errorStream != nil {
			return false, errors.New("error stream already assigned")
		}
		p.errorStream = stream
	case StreamTypeData:
		if p.dataStream != nil {
			return false, errors.New("data stream already assigned")
		}
		p.dataStream = stream
	}

	complete := p.errorStream != nil && p.dataStream != nil
	if complete {
		// 两条流齐了就关闭通道，唤醒等待中的监控协程。
		close(p.complete)
	}
	return complete, nil
}

// printError writes s to p.errorStream if p.errorStream has been set.
// printError 在错误流已经就位时把错误文本 s 写入该流，否则静默丢弃。
func (p *httpStreamPair) printError(s string) {
	p.lock.RLock()
	defer p.lock.RUnlock()
	if p.errorStream != nil {
		fmt.Fprint(p.errorStream, s)
	}
}
