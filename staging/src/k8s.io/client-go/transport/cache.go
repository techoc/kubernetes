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

package transport

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"
	"weak"

	utilnet "k8s.io/apimachinery/pkg/util/net"
	clientgofeaturegate "k8s.io/client-go/features"
	"k8s.io/client-go/tools/metrics"
	"k8s.io/klog/v2"
)

// TlsTransportCache 按不同配置缓存 TLS http.RoundTripper；
// TLS 选项相同的配置会返回同一个 RoundTripper。
// 若配置无自定义 TLS 选项，则返回 http.DefaultTransport。
//
// 单例缓存：相同 TLS 配置共享连接池，避免每个 clientset 各建一套连接。
// GC 开启时用 weak.Pointer，transport 不再被引用后条目可被回收。
type tlsTransportCache struct {
	mu               sync.Mutex
	transports       map[tlsCacheKey]weak.Pointer[trackedTransport] // 启用 GC 时使用的弱引用表。
	strongTransports map[tlsCacheKey]http.RoundTripper              // GC-disabled
}

// idleConnsPerHost 与 http.DefaultTransport 的默认值一致，避免共享 transport 时连接数骤降。
const idleConnsPerHost = 25

var tlsCache = newTLSCache()

// newTLSCache 初始化两张缓存表；实际使用哪张由 feature gate 决定。
func newTLSCache() *tlsTransportCache {
	return &tlsTransportCache{
		transports:       make(map[tlsCacheKey]weak.Pointer[trackedTransport]),
		strongTransports: make(map[tlsCacheKey]http.RoundTripper),
	}
}

// tlsCacheKey 是 TLS 配置的等值判断依据；字段必须严格可比较，
// 因此函数使用可比较的 holder 包装。
type tlsCacheKey struct {
	insecure           bool
	caData             string
	caFile             string
	certData           string
	keyData            string `datapolicy:"security-key"`
	certFile           string
	keyFile            string
	serverName         string
	nextProtos         string
	disableCompression bool
	// 包装这些函数使其可作为 map 键。
	getCert *GetCertHolder
	dial    *DialHolder
}

func (t tlsCacheKey) String() string {
	// 私钥内容不能出现在日志或错误信息中，只标记是否存在。
	keyText := "<none>"
	if len(t.keyData) > 0 {
		keyText = "<redacted>"
	}
	return fmt.Sprintf("insecure:%v, caData:%#v, caFile:%s, certData:%#v, keyData:%s, serverName:%s, disableCompression:%t, getCert:%p, dial:%p",
		t.insecure, t.caData, t.caFile, t.certData, keyText, t.serverName, t.disableCompression, t.getCert, t.dial)
}

// get 返回（或创建）与 config 对应的 RoundTripper。
// 缓存命中时直接复用；不可缓存（如自定义 Proxy）时每次新建。
func (c *tlsTransportCache) get(config *Config) (http.RoundTripper, error) {
	key, canCache, err := tlsConfigKey(config)
	if err != nil {
		return nil, err
	}

	if canCache {
		// 确保给定 TLS 选项只创建一个 transport。
		c.mu.Lock()
		defer c.mu.Unlock()
		defer func() { metrics.TransportCacheEntries.Observe(c.lenLocked()) }()

		// 查看该配置是否已有自定义 transport。
		if t, ok := c.getLocked(key); ok {
			if t != nil {
				metrics.TransportCreateCalls.Increment("hit")
				return t, nil
			}
			// key 存在但弱引用已被 GC 清空，视为 miss 并重建。
			metrics.TransportCreateCalls.Increment("miss-gc")
		} else {
			metrics.TransportCreateCalls.Increment("miss")
		}
	} else {
		metrics.TransportCreateCalls.Increment("uncacheable")
	}

	// 获取该客户端配置的 TLS 选项。
	tlsConfig, err := TLSConfigFor(config)
	if err != nil {
		return nil, err
	}
	// 这些选项不需要自定义 TLS 配置：
	// 没有自定义 TLS/Dial/Proxy 时复用全局 DefaultTransport，最大化连接复用。
	if tlsConfig == nil && config.DialHolder == nil && config.Proxy == nil {
		return http.DefaultTransport, nil
	}

	var dial func(ctx context.Context, network, address string) (net.Conn, error)
	if config.DialHolder != nil {
		dial = config.DialHolder.Dial
	} else {
		dial = (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext
	}

	// 若启用了文件重载，需要正确处理证书轮换。
	// TODO(jackkleeman): config.HasCertCallback() 为 true 时也可在此加入轮换逻辑。
	var cancel context.CancelFunc
	// ReloadTLSFiles 表示证书来自磁盘且可能被外部轮换；启动后台协程持续重载。
	if config.TLS.ReloadTLSFiles && tlsConfig != nil && tlsConfig.GetClientCertificate != nil {
		// TLS 缓存是单例，后台活动共用一个名字是可以接受的。
		logger := klog.Background().WithName("tls-transport-cache")
		dynamicCertDialer := certRotatingDialer(logger, tlsConfig.GetClientCertificate, dial)
		tlsConfig.GetClientCertificate = dynamicCertDialer.GetClientCertificate
		dial = dynamicCertDialer.connDialer.DialContext
		var ctx context.Context
		ctx, cancel = context.WithCancel(context.Background())
		go dynamicCertDialer.run(ctx.Done())
	}

	proxy := http.ProxyFromEnvironment
	if config.Proxy != nil {
		proxy = config.Proxy
	}

	httpTransport := utilnet.SetTransportDefaults(&http.Transport{
		Proxy:               proxy,
		TLSHandshakeTimeout: 10 * time.Second,
		TLSClientConfig:     tlsConfig,
		MaxIdleConnsPerHost: idleConnsPerHost,
		DialContext:         dial,
		DisableCompression:  config.DisableCompression,
	})
	var transport http.RoundTripper = httpTransport

	if config.TLS.ReloadCAFiles && tlsConfig != nil && tlsConfig.RootCAs != nil && len(config.TLS.CAFile) > 0 {
		// CA 文件可能在连接存活期间被替换；原子 holder 在下次 TLS 握手时读取新 CA。
		transport = newAtomicTransportHolder(config.TLS.CAFile, config.TLS.CAData, httpTransport)
	}

	if !canCache && cancel == nil {
		// 既不可缓存又没有后台协程，返回后无需任何清理。
		return transport, nil // uncacheable config with no cert rotation - nothing to GC
	}

	// 未开启 GC feature gate：缓存强引用，且故意不取消轮换协程以保持历史行为。
	if !clientgofeaturegate.FeatureGates().Enabled(clientgofeaturegate.ClientsAllowTLSCacheGC) {
		if canCache {
			c.strongTransports[key] = transport
		}
		return transport, nil // cancel is intentionally discarded and the cert rotation go routine leaks
	}

	transportWithGC := &trackedTransport{rt: transport}

	if cancel != nil {
		// 把 metric 捕获为局部变量，这样清理回调就不会通过全局变量影响其他测试：
		// 清理回调在对象被回收时才运行，因此不能依赖 cache 全局状态以外的可变状态。
		transportCertRotationGCCalls := metrics.TransportCertRotationGCCalls
		runtime.AddCleanup(transportWithGC, func(_ struct{}) {
			cancel()
			transportCertRotationGCCalls.Increment()
		}, struct{}{})
	}

	if canCache {
		wp := weak.Make(transportWithGC)
		c.transports[key] = wp
		// 把 metrics 捕获为局部变量，避免清理函数通过全局变量影响其他测试。
		transportCacheGCCalls := metrics.TransportCacheGCCalls
		transportCacheEntries := metrics.TransportCacheEntries
		runtime.AddCleanup(transportWithGC, func(key tlsCacheKey) {
			c.mu.Lock()
			defer c.mu.Unlock()

			// 确保只删除本次 setLocked 调用创建的弱指针：
			// 若同 key 已被新 transport 覆盖，旧条目的清理不能误删新条目。
			if c.transports[key] != wp {
				transportCacheGCCalls.Increment("skipped")
				return
			}
			delete(c.transports, key)
			transportCacheGCCalls.Increment("deleted")
			transportCacheEntries.Observe(c.lenLocked())
		}, key)
	}

	return transportWithGC, nil
}

// getLocked 按 feature gate 选择强/弱引用缓存查询；调用方必须持有 c.mu。
func (c *tlsTransportCache) getLocked(key tlsCacheKey) (http.RoundTripper, bool) {
	if !clientgofeaturegate.FeatureGates().Enabled(clientgofeaturegate.ClientsAllowTLSCacheGC) {
		v, ok := c.strongTransports[key]
		return v, ok
	}

	wp, ok := c.transports[key]
	if !ok {
		return nil, false
	}

	v := wp.Value()

	if v == nil { // 避免类型化 nil。
		return nil, true // 键存在但值已被 GC 回收。
	}

	return v, true
}

// lenLocked 返回当前缓存条目数；关闭 GC 时统计强引用表。
func (c *tlsTransportCache) lenLocked() int {
	if !clientgofeaturegate.FeatureGates().Enabled(clientgofeaturegate.ClientsAllowTLSCacheGC) {
		return len(c.strongTransports)
	}
	return len(c.transports)
}

// trackedTransport 包装 http.RoundTripper，作为 weak.Pointer 的目标。
// 这是 TLS transport 缓存中的目标对象；丢弃对它的所有引用会触发缓存条目和
// 证书轮换 goroutine 的 GC 清理。
type trackedTransport struct {
	rt http.RoundTripper
}

var _ http.RoundTripper = &trackedTransport{}
var _ utilnet.RoundTripperWrapper = &trackedTransport{}

// RoundTrip 透传到被包装的 transport。
func (v *trackedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return v.rt.RoundTrip(req)
}

// WrappedRoundTripper 暴露被包装的 transport，供 utilnet 识别连接复用等能力。
func (v *trackedTransport) WrappedRoundTripper() http.RoundTripper {
	return v.rt
}

// tlsConfigKey 为 TLSConfigFor 返回的 tls.Config 生成唯一键。
func tlsConfigKey(c *Config) (tlsCacheKey, bool, error) {
	// 确保 ca/key/cert 内容已加载。
	if err := loadTLSFiles(c); err != nil {
		return tlsCacheKey{}, false, err
	}

	if c.Proxy != nil {
		// 无法判断两个函数是否相等，只能放弃缓存。
		return tlsCacheKey{}, false, nil
	}

	k := tlsCacheKey{
		insecure:           c.TLS.Insecure,
		serverName:         c.TLS.ServerName,
		nextProtos:         strings.Join(c.TLS.NextProtos, ","),
		disableCompression: c.DisableCompression,
		getCert:            c.TLS.GetCertHolder,
		dial:               c.DialHolder,
	}

	if c.TLS.ReloadTLSFiles {
		// 重载模式下文件内容可能变化，用路径而不是内容做 key，保证轮换后仍命中同一 transport。
		k.certFile = c.TLS.CertFile
		k.keyFile = c.TLS.KeyFile
	} else {
		k.certData = string(c.TLS.CertData)
		k.keyData = string(c.TLS.KeyData)
	}

	if c.TLS.ReloadCAFiles {
		// 重载 CA 文件时，缓存键使用 CA 文件路径而非内容，这样每次创建 transport
		// 都能从磁盘重新加载 CA。
		k.caFile = c.TLS.CAFile
	} else {
		// 不重载时直接缓存 CA 内容。
		k.caData = string(c.TLS.CAData)
	}

	return k, true, nil
}
