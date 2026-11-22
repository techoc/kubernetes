/*
Copyright The Kubernetes Authors.

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

package transport

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"sync"
	"time"

	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/client-go/tools/metrics"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

var _ utilnet.RoundTripperWrapper = &atomicTransportHolder{}

// atomicTransportHolder holds a transport that can be atomically updated
// when CA files change, enabling graceful CA rotation without cache complexity
//
// 与重建整个 REST 客户端不同，这里只替换持有的 *http.Transport，
// 已有请求继续使用旧 transport 完成，新请求则走新的根证书池。
type atomicTransportHolder struct {
	caFile        string
	currentCAData []byte // Track the actual CA data currently in use
	// clock and caRefreshDuration are used to allow for testing time-based logic.
	clock             clock.Clock
	caRefreshDuration time.Duration
	// mu covers transport and transportLastChecked
	mu                   sync.RWMutex
	transport            *http.Transport
	transportLastChecked time.Time
}

func (h *atomicTransportHolder) RoundTrip(req *http.Request) (*http.Response, error) {
	return h.getTransport(req.Context()).RoundTrip(req)
}

func (h *atomicTransportHolder) WrappedRoundTripper() http.RoundTripper {
	h.mu.RLock()
	defer h.mu.RUnlock()

	return h.transport
}

// getTransport 返回当前应使用的 transport，必要时刷新 CA。
func (h *atomicTransportHolder) getTransport(ctx context.Context) *http.Transport {
	// 常见情况：仍在刷新窗口内，读锁直接返回，避免每个请求都抢写锁。
	if rt := h.getTransportIfFresh(); rt != nil {
		return rt
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	// 拿到写锁后需要二次检查，可能其他 goroutine 已完成刷新。
	h.tryRefreshTransportLocked(ctx)
	return h.transport
}

// getTransportIfFresh 在刷新窗口内返回当前 transport，否则返回 nil。
func (h *atomicTransportHolder) getTransportIfFresh() *http.Transport {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.clock.Since(h.transportLastChecked) < h.caRefreshDuration {
		return h.transport
	}
	return nil
}

// tryRefreshTransportLocked 在持写锁的前提下检查并替换 transport。
func (h *atomicTransportHolder) tryRefreshTransportLocked(ctx context.Context) {
	// If some other goroutine already checked/updated the CA
	if h.clock.Since(h.transportLastChecked) < h.caRefreshDuration {
		return
	}

	// only attempt CA reload once per caRefreshDuration, even if the reload fails
	// 失败也推进检查时间，防止文件持续不可读时每个请求都触发 stat/read。
	h.transportLastChecked = h.clock.Now()

	logger := klog.FromContext(ctx).WithValues("caFile", h.caFile)

	logger.V(4).Info("Checking CA file content")

	// Load new CA data from file
	newCAData, err := os.ReadFile(h.caFile)
	// Return old transport on read error
	// 文件暂时不可读（如 kubelet 原子替换 CA 卷的瞬间）时保留旧 transport，
	// 保证连接不会因短暂的 IO 错误中断。
	if err != nil {
		logger.Error(err, "Failed to read CA data from file")
		metrics.TransportCAReloads.Increment("failure", "read_error")
		return
	}

	// 空文件通常表示写入未完成，用空池替换会导致所有 TLS 校验失败。
	if len(newCAData) == 0 {
		logger.Info("CA file empty, skipping transport rotation")
		metrics.TransportCAReloads.Increment("failure", "empty")
		return
	}

	// 内容未变则无谓地 clone transport 只会浪费连接池，直接跳过。
	if bytes.Equal(h.currentCAData, newCAData) {
		logger.V(4).Info("CA file unchanged, skipping transport rotation")
		metrics.TransportCAReloads.Increment("success", "unchanged")
		return
	}

	logger.V(4).Info("CA content changed, updating transport")

	// Load new CA pool
	newCAs, err := rootCertPool(newCAData)
	// Return old transport on parse error
	if err != nil {
		logger.Error(err, "Failed to parse CA data from file")
		metrics.TransportCAReloads.Increment("failure", "ca_parse_error")
		return
	}
	// Clone 复制连接池配置，只替换根证书，其余 TLS/代理设置保持不变。
	newTransport := h.transport.Clone()
	newTransport.TLSClientConfig.RootCAs = newCAs
	oldTransport := h.transport
	h.transport = newTransport
	// Update our tracking of current CA data
	h.currentCAData = newCAData

	// Close idle connections on the old transport to encourage migration
	// 旧连接仍被进行中的请求使用，只能关闭空闲连接，使其自然淘汰。
	oldTransport.CloseIdleConnections()

	logger.V(4).Info("Transport updated for CA rotation")
	metrics.TransportCAReloads.Increment("success", "updated")
}

// newAtomicTransportHolder creates a new holder for CA file reloading scenarios.
// The caFile must be specified.
// caData may be empty but should correspond to the contents of caFile.
// transport must have a TLS config and its root CAs should match caData.
func newAtomicTransportHolder(caFile string, caData []byte, transport *http.Transport) *atomicTransportHolder {
	c := clock.RealClock{}
	return &atomicTransportHolder{
		caFile:               caFile,
		currentCAData:        caData,
		clock:                c,
		caRefreshDuration:    5 * time.Minute,
		transport:            transport,
		transportLastChecked: c.Now(),
	}
}
