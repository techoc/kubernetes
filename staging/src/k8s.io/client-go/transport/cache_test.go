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
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"weak"

	"k8s.io/apimachinery/pkg/util/wait"
	clientgofeaturegate "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"
	"k8s.io/client-go/tools/metrics"
)

// TestTLSConfigKey 验证缓存键只由影响 TLS 的字段决定：
// 凭据/UA 等不影响 TLS 的配置应共享空键；代理、拨号器、CA、证书、ServerName 等
// 必须产生互不相同的键，否则会错误复用 transport。
func TestTLSConfigKey(t *testing.T) {

	clientfeaturestesting.SetFeatureDuringTest(t, clientgofeaturegate.ClientsAllowCARotation, true)
	// 确认不影响 tls 配置的字段也不影响缓存键。
	identicalConfigurations := map[string]*Config{
		"empty":          {},
		"basic":          {Username: "bob", Password: "password"},
		"bearer":         {BearerToken: "token"},
		"user agent":     {UserAgent: "useragent"},
		"transport":      {Transport: http.DefaultTransport},
		"wrap transport": {WrapTransport: func(http.RoundTripper) http.RoundTripper { return nil }},
	}
	for nameA, valueA := range identicalConfigurations {
		for nameB, valueB := range identicalConfigurations {
			keyA, canCache, err := tlsConfigKey(valueA)
			if err != nil {
				t.Errorf("Unexpected error for %q: %v", nameA, err)
				continue
			}
			if !canCache {
				t.Errorf("Unexpected canCache=false")
				continue
			}
			keyB, canCache, err := tlsConfigKey(valueB)
			if err != nil {
				t.Errorf("Unexpected error for %q: %v", nameB, err)
				continue
			}
			if !canCache {
				t.Errorf("Unexpected canCache=false")
				continue
			}
			if keyA != keyB {
				t.Errorf("Expected identical cache keys for %q and %q, got:\n\t%s\n\t%s", nameA, nameB, keyA, keyB)
				continue
			}
			if keyA != (tlsCacheKey{}) {
				t.Errorf("Expected empty cache keys for %q and %q, got:\n\t%s\n\t%s", nameA, nameB, keyA, keyB)
				continue
			}
		}
	}

	// 确认影响 tls 配置的字段也会影响缓存键。
	dialer := net.Dialer{}
	getCert := &GetCertHolder{GetCert: func() (*tls.Certificate, error) { return nil, nil }}
	caFile := writeCAFile(t, []byte(testCACert1))
	uniqueConfigurations := map[string]*Config{
		"proxy":                         {Proxy: func(request *http.Request) (*url.URL, error) { return nil, nil }},
		"no tls":                        {},
		"dialer":                        {DialHolder: &DialHolder{Dial: dialer.DialContext}},
		"dialer2":                       {DialHolder: &DialHolder{Dial: func(ctx context.Context, network, address string) (net.Conn, error) { return nil, nil }}},
		"insecure":                      {TLS: TLSConfig{Insecure: true}},
		"cadata 1":                      {TLS: TLSConfig{CAData: []byte{1}}},
		"cadata 2":                      {TLS: TLSConfig{CAData: []byte{2}}},
		"with only ca file":             {TLS: TLSConfig{CAFile: caFile}},
		"with both ca file and ca data": {TLS: TLSConfig{CAFile: caFile, CAData: []byte(testCACert1)}},
		"cert 1, key 1": {
			TLS: TLSConfig{
				CertData: []byte{1},
				KeyData:  []byte{1},
			},
		},
		"cert 1, key 1, servername 1": {
			TLS: TLSConfig{
				CertData:   []byte{1},
				KeyData:    []byte{1},
				ServerName: "1",
			},
		},
		"cert 1, key 1, servername 2": {
			TLS: TLSConfig{
				CertData:   []byte{1},
				KeyData:    []byte{1},
				ServerName: "2",
			},
		},
		"cert 1, key 2": {
			TLS: TLSConfig{
				CertData: []byte{1},
				KeyData:  []byte{2},
			},
		},
		"cert 2, key 1": {
			TLS: TLSConfig{
				CertData: []byte{2},
				KeyData:  []byte{1},
			},
		},
		"cert 2, key 2": {
			TLS: TLSConfig{
				CertData: []byte{2},
				KeyData:  []byte{2},
			},
		},
		"cadata 1, cert 1, key 1": {
			TLS: TLSConfig{
				CAData:   []byte{1},
				CertData: []byte{1},
				KeyData:  []byte{1},
			},
		},
		"getCert1": {
			TLS: TLSConfig{
				KeyData:       []byte{1},
				GetCertHolder: getCert,
			},
		},
		"getCert2": {
			TLS: TLSConfig{
				KeyData:       []byte{1},
				GetCertHolder: &GetCertHolder{GetCert: func() (*tls.Certificate, error) { return nil, nil }},
			},
		},
		"getCert1, key 2": {
			TLS: TLSConfig{
				KeyData:       []byte{2},
				GetCertHolder: getCert,
			},
		},
		"http2, http1.1": {TLS: TLSConfig{NextProtos: []string{"h2", "http/1.1"}}},
		"http1.1-only":   {TLS: TLSConfig{NextProtos: []string{"http/1.1"}}},
	}
	for nameA, valueA := range uniqueConfigurations {
		for nameB, valueB := range uniqueConfigurations {
			keyA, canCacheA, err := tlsConfigKey(valueA)
			if err != nil {
				t.Errorf("Unexpected error for %q: %v", nameA, err)
				continue
			}
			keyB, canCacheB, err := tlsConfigKey(valueB)
			if err != nil {
				t.Errorf("Unexpected error for %q: %v", nameB, err)
				continue
			}

			shouldCacheA := valueA.Proxy == nil
			if shouldCacheA != canCacheA {
				t.Error("Unexpected canCache=false for " + nameA)
			}

			configIsNotEmpty := !reflect.DeepEqual(*valueA, Config{})
			if keyA == (tlsCacheKey{}) && shouldCacheA && configIsNotEmpty {
				t.Errorf("Expected non-empty cache keys for %q and %q, got:\n\t%s\n\t%s", nameA, nameB, keyA, keyB)
				continue
			}

			// 确认相同配置得到相同的键。
			if nameA == nameB {
				if keyA != keyB {
					t.Errorf("Expected identical cache keys for %q and %q, got:\n\t%s\n\t%s", nameA, nameB, keyA, keyB)
				}
				if canCacheA != canCacheB {
					t.Errorf("Expected identical canCache %q and %q, got:\n\t%v\n\t%v", nameA, nameB, canCacheA, canCacheB)
				}
				continue
			}

			if canCacheA && canCacheB {
				if keyA == keyB {
					t.Errorf("Expected unique cache keys for %q and %q, got:\n\t%s\n\t%s", nameA, nameB, keyA, keyB)
					continue
				}
			}
		}
	}
}

// TestTLSConfigKeyCARotationDisabled 验证门控关闭时 CAFile 会被解析成 CAData，
// 因此仅文件配置与等价的数据配置应得到相同缓存键，且不开启 ReloadCAFiles。
func TestTLSConfigKeyCARotationDisabled(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientgofeaturegate.ClientsAllowCARotation, false)

	caFile := writeCAFile(t, []byte(testCACert1))

	// 特性关闭时，仅配置 CAFile 的场景会通过 CAData 解析。
	// loadTLSFiles 会读取文件内容，因此相同文件内容得到相同键。
	config1 := &Config{TLS: TLSConfig{CAFile: caFile}}
	config2 := &Config{TLS: TLSConfig{CAData: []byte(testCACert1)}}

	if err := loadTLSFiles(config1); err != nil {
		t.Fatal(err)
	}
	if err := loadTLSFiles(config2); err != nil {
		t.Fatal(err)
	}

	key1, canCache1, err := tlsConfigKey(config1)
	if err != nil || !canCache1 {
		t.Fatalf("unexpected: err=%v, canCache=%v", err, canCache1)
	}

	key2, canCache2, err := tlsConfigKey(config2)
	if err != nil || !canCache2 {
		t.Fatalf("unexpected: err=%v, canCache=%v", err, canCache2)
	}

	if key1 != key2 {
		t.Error("Expected same cache key when feature is disabled (CAFile resolved to CAData)")
	}
	if config1.TLS.ReloadCAFiles {
		t.Error("Expected ReloadCAFiles=false when feature gate is disabled")
	}
}

// TestTLSTransportCacheCARotation 测试 CA 轮换时的 transport 缓存行为。
// 覆盖四种配置：仅 CAFile 时应包装为可轮换 transport；同时给出 CAData 或仅给
// CAData 时不包装；完全无 TLS 配置时返回 DefaultTransport 且不缓存。
func TestTLSTransportCacheCARotation(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientgofeaturegate.ClientsAllowCARotation, true)
	caFile := writeCAFile(t, []byte(testCACert1))
	testCases := []struct {
		name            string
		config          *Config
		expectWrapper   bool
		expectCacheable bool
	}{
		{
			name: "CA rotation should be enabled when only the CAFile is set",
			config: &Config{
				TLS: TLSConfig{
					CAFile: caFile,
				},
			},
			expectWrapper:   true,
			expectCacheable: true,
		},
		{
			name: "CA rotation should be disabled when both CAFile and CAData are set",
			config: &Config{
				TLS: TLSConfig{
					CAFile: caFile,
					CAData: []byte(testCACert1),
				},
			},
			expectWrapper:   false,
			expectCacheable: true,
		},
		{
			name: "CA rotation should be disabled when only the CAData is set",
			config: &Config{
				TLS: TLSConfig{
					CAData: []byte(testCACert1),
				},
			},
			expectWrapper:   false,
			expectCacheable: true,
		},
		{
			name: "no TLS config",
			config: &Config{
				TLS: TLSConfig{},
			},
			expectWrapper:   false,
			expectCacheable: false, // No TLS config means default transport
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			createCalls, _, _, _ := installFakeMetrics(t)
			tlsCaches := newTLSCache()

			rt, err := tlsCaches.get(tc.config)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if !tc.expectCacheable {
				// 应返回默认 transport。
				if rt != http.DefaultTransport {
					t.Errorf("Expected default transport, got %T", rt)
				}
				// canCache=true 使 getLocked 判定为“未命中”，但 DefaultTransport
				// 在 setLocked 之前就返回，条目从未被存储。
				if calls := createCalls.reset(); len(calls) != 1 || calls[0] != "miss" {
					t.Errorf("expected [miss], got %v", calls)
				}

				// 第二次调用同样是“未命中”，因为没有存储任何内容。
				rt2, err := tlsCaches.get(tc.config)
				if err != nil {
					t.Fatal(err)
				}
				if rt2 != http.DefaultTransport {
					t.Errorf("second call: expected default transport, got %T", rt2)
				}
				if calls := createCalls.reset(); len(calls) != 1 || calls[0] != "miss" {
					t.Errorf("expected [miss] on second call (nothing stored), got %v", calls)
				}

				requireCacheLen(t, tlsCaches, 0)
				return
			}

			if calls := createCalls.reset(); len(calls) != 1 || calls[0] != "miss" {
				t.Errorf("expected [miss] on first get, got %v", calls)
			}

			if rt := rt.(*trackedTransport).rt; tc.expectWrapper {
				// 应被包装在 atomicTransportHolder 中。
				if _, ok := rt.(*atomicTransportHolder); !ok {
					t.Errorf("Expected atomicTransportHolder, got %T", rt)
				}
				if !tc.config.TLS.ReloadCAFiles {
					t.Errorf("Expected ReloadCAFiles to be true, got %v", tc.config.TLS.ReloadCAFiles)
				}
			} else {
				// 应是普通的 http.Transport。
				if _, ok := rt.(*http.Transport); !ok {
					t.Errorf("Expected *http.Transport, got %T", rt)
				}
			}

			// 测试缓存：第二次调用应返回同一个实例。
			rt2, err := tlsCaches.get(tc.config)
			if err != nil {
				t.Fatalf("Unexpected error on second call: %v", err)
			}
			if rt != rt2 {
				t.Error("Expected same transport instance from cache")
			}
			if calls := createCalls.reset(); len(calls) != 1 || calls[0] != "hit" {
				t.Errorf("expected [hit] on second get, got %v", calls)
			}

			requireCacheLen(t, tlsCaches, 1)
		})
	}
}

// TestTLSTransportCacheCARotationDisabled 验证门控关闭时 CAFile 走普通 transport，
// 不会生成 atomicTransportHolder。
func TestTLSTransportCacheCARotationDisabled(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientgofeaturegate.ClientsAllowCARotation, false)

	caFile := writeCAFile(t, []byte(testCACert1))
	cache := newTLSCache()

	rt, err := cache.get(&Config{TLS: TLSConfig{CAFile: caFile}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	rt = rt.(*trackedTransport).rt
	if _, ok := rt.(*atomicTransportHolder); ok {
		t.Error("Expected plain *http.Transport when feature gate is disabled, got atomicTransportHolder")
	}
	if _, ok := rt.(*http.Transport); !ok {
		t.Errorf("Expected *http.Transport, got %T", rt)
	}
}

// TestEmptyCAFileRotationLifecycle 验证空 CA 文件启动时得到空根池，
// 后续写入有效证书并在刷新窗口后即可被加载，无需重建客户端。
func TestEmptyCAFileRotationLifecycle(t *testing.T) {
	// 测试期间启用该特性门控。
	clientfeaturestesting.SetFeatureDuringTest(t, clientgofeaturegate.ClientsAllowCARotation, true)

	// 创建合法文件路径，但证书数据为空。
	emptyFile := writeCAFile(t, []byte{})

	config := &Config{
		TLS: TLSConfig{
			CAFile: emptyFile,
		},
	}

	tlsCaches := newTLSCache()

	rt, err := tlsCaches.get(config)
	if err != nil {
		t.Fatalf("Unexpected error getting transport: %v", err)
	}

	// 验证 newAtomicTransportHolder 被成功创建。
	holder, ok := rt.(*trackedTransport).rt.(*atomicTransportHolder)
	if !ok {
		t.Fatalf("Expected atomicTransportHolder, got %T", rt)
	}

	// 验证初始状态：RootCAs 非 nil 但为空。
	initialTransport := holder.getTransport(context.Background())

	if initialTransport.TLSClientConfig == nil || initialTransport.TLSClientConfig.RootCAs == nil {
		t.Fatal("Expected RootCAs to be non-nil for an empty CA file (should be an empty CertPool)")
	}
	emptyPool := x509.NewCertPool()
	if !initialTransport.TLSClientConfig.RootCAs.Equal(emptyPool) {
		t.Fatal("Expected initially empty RootCAs")
	}

	// 向 CA 文件写入合法证书数据。
	if err := os.WriteFile(emptyFile, []byte(testCACert1), 0644); err != nil {
		t.Fatalf("Failed to write to CA file: %v", err)
	}

	holder.mu.Lock()
	// 把上次检查时间设为很久以前，强制下次 getTransport 时刷新。
	holder.transportLastChecked = time.Now().Add(-time.Hour)
	holder.mu.Unlock()

	refreshedTransport := holder.getTransport(context.Background())

	// 验证刷新成功且 cert pool 已填充。
	if refreshedTransport.TLSClientConfig.RootCAs.Equal(emptyPool) {
		t.Fatal("Expected RootCAs to be populated after writing valid cert data and refreshing")
	}
}

// TestCacheHoldAfterCARotation 验证持有 *atomicTransportHolder 能让缓存条目在 CA 轮换
// 替换内部 transport 后依然存活。
func TestCacheHoldAfterCARotation(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientgofeaturegate.ClientsAllowTLSCacheGC, true)
	clientfeaturestesting.SetFeatureDuringTest(t, clientgofeaturegate.ClientsAllowCARotation, true)
	_, gcCalls, _, _ := installFakeMetrics(t)

	caFile := writeCAFile(t, []byte(testCACert1))

	cache := newTLSCache()

	rt, err := cache.get(&Config{TLS: TLSConfig{ServerName: "reload-test", CAFile: caFile}})
	if err != nil {
		t.Fatal(err)
	}
	requireCacheLen(t, cache, 1)

	holder := rt.(*trackedTransport).rt.(*atomicTransportHolder)

	originalInner := holder.getTransport(context.Background())
	if originalInner == nil {
		t.Fatal("expected non-nil transport")
	}

	// Simulate CA rotation.
	if err := os.WriteFile(caFile, []byte(testCACert2), 0644); err != nil {
		t.Fatal(err)
	}
	holder.mu.Lock()
	holder.transportLastChecked = time.Now().Add(-time.Hour)
	holder.mu.Unlock()

	newInner := holder.getTransport(context.Background())
	if newInner == nil {
		t.Fatal("expected non-nil transport after rotation")
	}
	if newInner == originalInner {
		t.Fatal("expected transport to change after CA rotation")
	}

	// holder 仍存活，缓存条目必须保留。
	for range 3 {
		runtime.GC()
	}
	requireCacheLen(t, cache, 1)
	runtime.KeepAlive(rt)

	pollCacheSizeWithGC(t, cache, 0)
	if calls := gcCalls.reset(); len(calls) != 1 || calls[0] != "deleted" {
		t.Errorf("expected [deleted] after eviction, got %v", calls)
	}
}

// TestCacheGCDisabledNoEviction 验证 GC 特性门控关闭时，缓存条目存入 strongTransports
// 且永不淘汰。
func TestCacheGCDisabledNoEviction(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientgofeaturegate.ClientsAllowTLSCacheGC, false)
	createCalls, gcCalls, _, cacheEntries := installFakeMetrics(t)

	cache := newTLSCache()

	rt, err := cache.get(&Config{TLS: TLSConfig{ServerName: "gc-disabled-test"}})
	if err != nil {
		t.Fatal(err)
	}

	if calls := createCalls.reset(); len(calls) != 1 || calls[0] != "miss" {
		t.Errorf("expected [miss], got %v", calls)
	}

	if _, ok := rt.(*trackedTransport); ok {
		t.Error("expected plain transport, not *trackedTransport, when GC is disabled")
	}

	rt2, err := cache.get(&Config{TLS: TLSConfig{ServerName: "gc-disabled-test"}})
	if err != nil {
		t.Fatal(err)
	}
	if rt != rt2 {
		t.Error("expected cache hit to return same transport")
	}
	if calls := createCalls.reset(); len(calls) != 1 || calls[0] != "hit" {
		t.Errorf("expected [hit], got %v", calls)
	}

	requireCacheLen(t, cache, 1)

	runtime.KeepAlive(rt)
	for range 10 {
		runtime.GC()
	}
	requireCacheLen(t, cache, 1)

	if calls := gcCalls.reset(); len(calls) != 0 {
		t.Errorf("expected no GC cache calls when GC is disabled, got %v", calls)
	}

	// 两次 get() 都观察到 1 —— defer 在 setLocked 存入后才执行 lenLocked()。
	if values := cacheEntries.reset(); len(values) != 2 || values[0] != 1 || values[1] != 1 {
		t.Errorf("expected cache entries observations [1 1], got %v", values)
	}
}

// TestCacheReviveAfterDrop 验证 rt1 仍存活（通过 KeepAlive）时的场景。
// 相同键的第二次 get() 是确定性的缓存命中。
func TestCacheReviveAfterDrop(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientgofeaturegate.ClientsAllowTLSCacheGC, true)
	createCalls, _, _, _ := installFakeMetrics(t)

	cache := newTLSCache()

	config := &Config{TLS: TLSConfig{ServerName: "keep-alive-test"}}

	rt1, err := cache.get(config)
	if err != nil {
		t.Fatal(err)
	}
	requireCacheLen(t, cache, 1)
	createCalls.reset()

	// rt1 此时仍存活，弱引用必须仍能解析。
	rt2, err := cache.get(config)
	if err != nil {
		t.Fatal(err)
	}
	// rt1 与 rt2 是同一对象（缓存命中）。
	if rt1 != rt2 {
		t.Error("expected same transport (cache hit) since rt1 is still alive")
	}
	if calls := createCalls.reset(); len(calls) != 1 || calls[0] != "hit" {
		t.Errorf("expected [hit], got %v", calls)
	}
	requireCacheLen(t, cache, 1)

	runtime.KeepAlive(rt1)
	pollCacheSizeWithGC(t, cache, 0)
}

// TestUncacheableCertRotationLeak 验证证书轮换 goroutine 的泄漏场景。
// 在不可缓存的 transport 被 GC 时停止。
func TestUncacheableCertRotationLeak(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientgofeaturegate.ClientsAllowTLSCacheGC, true)
	_, _, rotationGCCalls, _ := installFakeMetrics(t)

	certFile := writeCAFile(t, []byte(certData))
	keyFile := writeCAFile(t, []byte(keyData))

	cache := newTLSCache()

	baseline := runtime.NumGoroutine()

	rt, err := cache.get(&Config{
		TLS: TLSConfig{
			CertFile: certFile,
			KeyFile:  keyFile,
		},
		Proxy: func(*http.Request) (*url.URL, error) { return nil, nil }, // force uncacheable
	})
	if err != nil {
		t.Fatal(err)
	}

	requireCacheLen(t, cache, 0)

	afterCreate := runtime.NumGoroutine()
	if afterCreate <= baseline {
		t.Fatalf("expected goroutine count to increase after creating transport with cert rotation, got baseline=%d after=%d", baseline, afterCreate)
	}

	runtime.KeepAlive(rt)

	err = wait.PollUntilContextTimeout(t.Context(), 10*time.Millisecond, 10*time.Second, true, func(_ context.Context) (bool, error) {
		runtime.GC()
		return runtime.NumGoroutine() <= baseline, nil
	})
	if err != nil {
		t.Errorf("goroutine leak: cert rotation goroutine was not stopped for uncacheable transport (baseline=%d current=%d)", baseline, runtime.NumGoroutine())
	}

	if n := rotationGCCalls.count.Load(); n != 1 {
		t.Errorf("expected TransportCertRotationGCCalls=1, got %d", n)
	}
}

// TestCacheableCertRotationLeak 验证证书轮换 goroutine 的场景。
// 在带证书轮换的可缓存 transport 被 GC 时停止。
// 这里覆盖 setLocked 中 canCache=true && cancel != nil 的路径。
func TestCacheableCertRotationLeak(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientgofeaturegate.ClientsAllowTLSCacheGC, true)
	_, gcCalls, rotationGCCalls, _ := installFakeMetrics(t)

	certFile := writeCAFile(t, []byte(certData))
	keyFile := writeCAFile(t, []byte(keyData))

	cache := newTLSCache()

	baseline := runtime.NumGoroutine()

	// CertFile+KeyFile 会触发 ReloadTLSFiles 与证书轮换 goroutine。
	// 没有 Proxy 时 canCache=true。
	rt, err := cache.get(&Config{
		TLS: TLSConfig{
			CertFile:   certFile,
			KeyFile:    keyFile,
			ServerName: "cacheable-cert-rotation-test",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	requireCacheLen(t, cache, 1)

	afterCreate := runtime.NumGoroutine()
	if afterCreate <= baseline {
		t.Fatalf("expected goroutine count to increase after creating transport with cert rotation, got baseline=%d after=%d", baseline, afterCreate)
	}

	runtime.KeepAlive(rt)

	gcCalls.reset()
	pollCacheSizeWithGC(t, cache, 0)

	err = wait.PollUntilContextTimeout(t.Context(), 10*time.Millisecond, 10*time.Second, true, func(_ context.Context) (bool, error) {
		runtime.GC()
		return runtime.NumGoroutine() <= baseline, nil
	})
	if err != nil {
		t.Errorf("goroutine leak: cert rotation goroutine was not stopped for cacheable transport (baseline=%d current=%d)", baseline, runtime.NumGoroutine())
	}

	if n := rotationGCCalls.count.Load(); n != 1 {
		t.Errorf("expected TransportCertRotationGCCalls=1, got %d", n)
	}
	if calls := gcCalls.reset(); len(calls) != 1 || calls[0] != "deleted" {
		t.Errorf("expected [deleted], got %v", calls)
	}
}

// TestCacheGCDisabledCertRotationNoCancel 验证 GC 特性门控关闭时的场景。
// 关闭时不会停止证书轮换 goroutine（GC 之前的行为）。
func TestCacheGCDisabledCertRotationNoCancel(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientgofeaturegate.ClientsAllowTLSCacheGC, false)
	_, _, rotationGCCalls, _ := installFakeMetrics(t)

	certFile := writeCAFile(t, []byte(certData))
	keyFile := writeCAFile(t, []byte(keyData))

	cache := newTLSCache()

	baseline := runtime.NumGoroutine()

	rt, err := cache.get(&Config{
		TLS: TLSConfig{
			CertFile:   certFile,
			KeyFile:    keyFile,
			ServerName: "gc-disabled-cert-rotation",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	afterCreate := runtime.NumGoroutine()
	if afterCreate <= baseline {
		t.Fatalf("expected cert rotation goroutine to start, got baseline=%d after=%d", baseline, afterCreate)
	}

	// 此后丢弃 rt。
	// 若启用 GC 清理，cancel 会触发且 goroutine 会停止。
	runtime.KeepAlive(rt)

	for range 10 {
		runtime.GC()
		runtime.Gosched()
	}
	time.Sleep(50 * time.Millisecond) // let any potential cleanup callbacks run

	if n := rotationGCCalls.count.Load(); n != 0 {
		t.Errorf("expected TransportCertRotationGCCalls=0 when GC disabled, got %d", n)
	}

	// goroutine 必须仍在运行 —— cancel 被有意丢弃。
	if runtime.NumGoroutine() <= baseline {
		t.Error("cert rotation goroutine was stopped — it should run indefinitely when GC is disabled")
	}
}

// TestCacheStaleEvictionSkipped 验证 GC 清理触发时，若旧条目的 map 槽位已被替换，
// 删除操作会被跳过。
// 通过替换 map 条目来触发：旧的 trackedTransport 仍存活时被替换，然后丢弃它，
// 其清理函数就会针对新条目执行。
func TestCacheStaleEvictionSkipped(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientgofeaturegate.ClientsAllowTLSCacheGC, true)
	_, gcCalls, _, _ := installFakeMetrics(t)

	cache := newTLSCache()

	cfg := &Config{TLS: TLSConfig{ServerName: "stale-test"}}
	rt1, err := cache.get(cfg)
	if err != nil {
		t.Fatal(err)
	}
	requireCacheLen(t, cache, 1)

	// 在 rt1 仍存活时替换 map 条目，模拟并发 get() 为相同键创建新条目。
	key, _, _ := tlsConfigKey(cfg)
	cache.mu.Lock()
	replacement := &trackedTransport{rt: &http.Transport{}}
	cache.transports[key] = weak.Make(replacement)
	cache.mu.Unlock()

	// 此后丢弃 rt1。其清理函数发现 c.transports[key] != wp -> "skipped"。
	runtime.KeepAlive(rt1)
	gcCalls.reset()

	err = wait.PollUntilContextTimeout(t.Context(), 10*time.Millisecond, 10*time.Second, true, func(_ context.Context) (bool, error) {
		runtime.GC()
		gcCalls.mu.Lock()
		n := len(gcCalls.calls)
		gcCalls.mu.Unlock()
		return n > 0, nil
	})
	if err != nil {
		t.Fatal("timed out waiting for stale cleanup to fire")
	}

	calls := gcCalls.reset()
	if len(calls) != 1 || calls[0] != "skipped" {
		t.Errorf("expected [skipped], got %v", calls)
	}

	// 替换后的条目因 keep alive 必须仍在 map 中。
	requireCacheLen(t, cache, 1)
	runtime.KeepAlive(replacement)
}

// TestCacheLeak 从 New 出发对 TLS 缓存 GC 做黑盒测试。
// 它使用并行生成的大量缓存条目。
//
// 通过弱引用缓存与大量并发创建，验证无强引用后条目会被 GC 清理，
// 且每次清理都会上报 cache entries 指标。
func TestCacheLeak(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientgofeaturegate.ClientsAllowTLSCacheGC, true)
	_, gcCalls, _, cacheEntries := installFakeMetrics(t)

	pollCacheSizeWithGC(t, tlsCache, 0) // clean start with global cache

	rt1, err := New(&Config{TLS: TLSConfig{ServerName: "1"}})
	if err != nil {
		t.Fatal(err)
	}
	rt2, err := New(&Config{TLS: TLSConfig{ServerName: "2"}})
	if err != nil {
		t.Fatal(err)
	}
	rt3, err := New(&Config{TLS: TLSConfig{ServerName: "1"}})
	if err != nil {
		t.Fatal(err)
	}

	requireCacheLen(t, tlsCache, 2) // rt1 and rt2 (rt3 is the same as rt1)

	var wg wait.Group
	var d net.Dialer
	var rts []http.RoundTripper
	var rtsLock sync.Mutex
	for i := range 1_000 { // outer loop forces cache miss via dialer
		dh := &DialHolder{Dial: d.DialContext}
		for range i%7 + 1 { // inner loop exercises each cache value having 1 to N references
			wg.Start(func() {
				rt, err := New(&Config{DialHolder: dh})
				if err != nil {
					panic(err)
				}
				rtsLock.Lock()
				rts = append(rts, rt) // keep a live reference to the round tripper
				rtsLock.Unlock()
			})
		}
	}
	wg.Wait()

	requireCacheLen(t, tlsCache, 1_000+2) // rts and rt1 and rt2 (rt3 is the same as rt1)

	runtime.KeepAlive(rts) // prevent round trippers from being GC'd too early

	// 在要测量的淘汰操作之前先重置计数。
	gcCalls.reset()
	cacheEntries.reset()

	pollCacheSizeWithGC(t, tlsCache, 2) // rt1 and rt2 (rt3 is the same as rt1)

	calls := gcCalls.reset()
	deletedCount := 0
	for _, c := range calls {
		if c == "deleted" {
			deletedCount++
		} else {
			t.Errorf("expected deleted call, got %s", c)
		}
	}
	if deletedCount != 1_000 {
		t.Errorf("expected 1000 deleted calls, got %d (total calls: %d)", deletedCount, len(calls))
	}

	// 每次“删除”清理都调用 Observe(lenLocked())。最后一次观察值应为 2（rt1 与 rt2 仍存在）。
	evictionObservations := cacheEntries.reset()
	if len(evictionObservations) != 1000 {
		t.Errorf("expected exactly 1000 cache entries observations from GC cleanup, got %d", len(evictionObservations))
	} else if last := evictionObservations[len(evictionObservations)-1]; last != 2 {
		t.Errorf("expected last cache entries observation to be 2 (rt1+rt2 remain), got %d", last)
	}

	runtime.KeepAlive(rt1)
	runtime.KeepAlive(rt2)
	runtime.KeepAlive(rt3)

	pollCacheSizeWithGC(t, tlsCache, 0)

	calls = gcCalls.reset()
	if len(calls) != 2 || calls[0] != "deleted" || calls[1] != "deleted" {
		t.Errorf("expected 2 deleted calls for rt1/rt2, got %v", calls)
	}
}

// requireCacheLen 断言缓存条目数，失败时立即终止测试。
func requireCacheLen(t *testing.T, c *tlsTransportCache, want int) {
	t.Helper()

	if cacheLen(t, c) != want {
		t.Fatalf("cache len %d, want %d", cacheLen(t, c), want)
	}
}

// cacheLen 返回当前缓存条目数，并顺带校验两种存储模式互斥：
// 开启 GC 时使用弱引用表，关闭时使用强引用表，两者不能同时非空。
func cacheLen(t *testing.T, c *tlsTransportCache) int {
	t.Helper()

	c.mu.Lock()
	defer c.mu.Unlock()

	if !clientgofeaturegate.FeatureGates().Enabled(clientgofeaturegate.ClientsAllowTLSCacheGC) {
		if len(c.transports) != 0 {
			t.Fatalf("transports len %d must be 0", len(c.transports))
		}
		return len(c.strongTransports)
	}

	if len(c.strongTransports) != 0 {
		t.Fatalf("strongTransports len %d must be 0", len(c.strongTransports))
	}
	return len(c.transports)
}

// pollCacheSizeWithGC 反复强制 GC 直到缓存大小达到期望值，
// 再额外 GC 几次确认大小稳定，以捕获“清理滞后/重复清理”类竞争问题。
func pollCacheSizeWithGC(t *testing.T, c *tlsTransportCache, want int) {
	t.Helper()

	if err := wait.PollUntilContextTimeout(t.Context(), 10*time.Millisecond, 10*time.Second, true, func(_ context.Context) (done bool, _ error) {
		runtime.GC() // run the garbage collector so the cleanups run
		return cacheLen(t, c) == want, nil
	}); err != nil {
		t.Fatalf("cache len %d, want %d: %v", cacheLen(t, c), want, err)
	}

	// 确保即使发生更多 GC，缓存大小也稳定；重复三次足以让有问题的实现暴露 flaky。
	for range 3 {
		runtime.GC()
	}
	requireCacheLen(t, c, want)
}

// recordingCreateCalls 记录 transport 创建指标的 hit/miss 序列，线程安全。
type recordingCreateCalls struct {
	mu    sync.Mutex
	calls []string
}

func (r *recordingCreateCalls) Increment(result string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, result)
}

func (r *recordingCreateCalls) reset() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := r.calls
	r.calls = nil
	return c
}

// recordingCacheGCCalls 记录缓存清理指标（deleted/skipped）。
type recordingCacheGCCalls struct {
	mu    sync.Mutex
	calls []string
}

func (r *recordingCacheGCCalls) Increment(result string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, result)
}

func (r *recordingCacheGCCalls) reset() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	c := r.calls
	r.calls = nil
	return c
}

// recordingCertRotationGCCalls 记录证书轮换 goroutine 被 GC 清理的次数。
type recordingCertRotationGCCalls struct {
	count atomic.Int64
}

func (r *recordingCertRotationGCCalls) Increment() {
	r.count.Add(1)
}

// recordingCacheEntries 记录每次观察到的缓存条目数。
type recordingCacheEntries struct {
	mu     sync.Mutex
	values []int
}

func (r *recordingCacheEntries) Observe(n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.values = append(r.values, n)
}

func (r *recordingCacheEntries) reset() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	v := r.values
	r.values = nil
	return v
}

// installFakeMetrics 用内存记录器替换四个 transport 指标，并在测试结束时恢复原值。
func installFakeMetrics(t *testing.T) (*recordingCreateCalls, *recordingCacheGCCalls, *recordingCertRotationGCCalls, *recordingCacheEntries) {
	createCalls := &recordingCreateCalls{}
	gcCalls := &recordingCacheGCCalls{}
	rotationGCCalls := &recordingCertRotationGCCalls{}
	cacheEntries := &recordingCacheEntries{}

	origCreate := metrics.TransportCreateCalls
	origGC := metrics.TransportCacheGCCalls
	origRotationGC := metrics.TransportCertRotationGCCalls
	origEntries := metrics.TransportCacheEntries
	metrics.TransportCreateCalls = createCalls
	metrics.TransportCacheGCCalls = gcCalls
	metrics.TransportCertRotationGCCalls = rotationGCCalls
	metrics.TransportCacheEntries = cacheEntries
	t.Cleanup(func() {
		metrics.TransportCreateCalls = origCreate
		metrics.TransportCacheGCCalls = origGC
		metrics.TransportCertRotationGCCalls = origRotationGC
		metrics.TransportCacheEntries = origEntries
	})
	return createCalls, gcCalls, rotationGCCalls, cacheEntries
}
