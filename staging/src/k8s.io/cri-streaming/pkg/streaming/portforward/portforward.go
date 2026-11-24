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
	"io"
	"net/http"
	"time"

	"k8s.io/streaming/pkg/httpstream/wsstream"
	"k8s.io/streaming/pkg/runtime"
)

// PortForwarder 知道如何在数据流与 Pod 端口之间转发内容。
type PortForwarder interface {
	// PortForward 在数据流与指定 Pod 的端口之间双向拷贝数据。
	PortForward(ctx context.Context, name string, uid string, port int32, stream io.ReadWriteCloser) error
}

// ServePortForward 处理端口转发请求。只要客户端存活且连接未因空闲而超时，该请求就会一直保持；
// 单次调用可服务多个转发连接，例如多个 curl http://localhost:8888/ 请求由同一次 ServePortForward 调用处理。
func ServePortForward(w http.ResponseWriter, req *http.Request, portForwarder PortForwarder, podName string, uid string, portForwardOptions *V4Options, idleTimeout time.Duration, streamCreationTimeout time.Duration, supportedProtocols []string) {
	var err error
	// 按请求头区分协议：WebSocket 请求走 wsstream 路径，其余走 HTTP Upgrade（SPDY）路径。
	if wsstream.IsWebSocketRequest(req) {
		err = handleWebSocketStreams(req, w, portForwarder, podName, uid, portForwardOptions, supportedProtocols, idleTimeout, streamCreationTimeout)
	} else {
		err = handleHTTPStreams(req, w, portForwarder, podName, uid, supportedProtocols, idleTimeout, streamCreationTimeout)
	}

	// 两种路径的错误统一在这里记录，保证错误不会静默丢失。
	if err != nil {
		runtime.HandleError(err)
		return
	}
}
