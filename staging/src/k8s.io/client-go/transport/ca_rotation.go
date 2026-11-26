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

// atomicTransportHolder 持有可在 CA 文件变化时原子更新的 transport。
// 从而在不引入缓存复杂度的前提下完成 CA 平滑轮换。
//
// 与重建整个 REST 客户端不同，这里只替换持有的 *http.Transport，
// 已有请求继续使用旧 transport 完成，新请求则走新的根证书池。
type atomicTransportHolder struct {
	caFile        string
	currentCAData []byte // 记录当前实际使用的 CA 数据。
	// clock 与 caRefreshDuration 便于测试基于时间的逻辑。
	clock             clock.Clock
	caRefreshDuration time.Duration
	// mu 保护 transport 与 transportLastChecked。
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
	// 若其他 goroutine 已检查/更新过 CA，则直接返回。
	if h.clock.Since(h.transportLastChecked) < h.caRefreshDuration {
		return
	}

	// 即使重新加载失败，也每个 caRefreshDuration 只尝试一次：
	// 失败也推进检查时间，防止文件持续不可读时每个请求都触发 stat/read。
	h.transportLastChecked = h.clock.Now()

	logger := klog.FromContext(ctx).WithValues("caFile", h.caFile)

	logger.V(4).Info("Checking CA file content")

	// 从文件加载新的 CA 数据。
	newCAData, err := os.ReadFile(h.caFile)
	// 读取出错时返回旧 transport：
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

	// 加载新的 CA pool。
	newCAs, err := rootCertPool(newCAData)
	// 解析出错时返回旧 transport，避免因为一次坏文件中断连接。
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
	// 更新当前 CA 数据的记录。
	h.currentCAData = newCAData

	// 关闭旧 transport 上的空闲连接，促进流量迁移到新 transport：
	// 旧连接仍被进行中的请求使用，只能关闭空闲连接，使其自然淘汰。
	oldTransport.CloseIdleConnections()

	logger.V(4).Info("Transport updated for CA rotation")
	metrics.TransportCAReloads.Increment("success", "updated")
}

// newAtomicTransportHolder 为 CA 文件重载场景创建 holder。必须指定 caFile；
// caData 可为空，但应与 caFile 内容对应；transport 必须带 TLS 配置且根 CA 与
// caData 一致。
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
