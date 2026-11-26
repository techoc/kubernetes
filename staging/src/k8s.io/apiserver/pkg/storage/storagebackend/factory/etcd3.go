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

// Package factory 根据 storagebackend.Config 构造具体的存储实现（目前为 etcd3），
// 并负责客户端连接、健康检查、压缩器复用以及资源释放。
package factory

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/url"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"go.etcd.io/etcd/client/pkg/v3/logutil"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/kubernetes"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	"k8s.io/apimachinery/pkg/runtime"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	genericfeatures "k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/server/egressselector"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/etcd3"
	"k8s.io/apiserver/pkg/storage/etcd3/metrics"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/value/encrypt/identity"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/metrics/legacyregistry"
	tracing "k8s.io/component-base/tracing"
)

const (
	// 选用较短的 keepalive 超时与间隔，是为了在没有太大开销的前提下
	// 尽快发现 etcd server 故障。
	keepaliveTime    = 30 * time.Second
	keepaliveTimeout = 10 * time.Second

	// dialTimeout 是建立连接失败的超时时间。
	// 设为 20 秒，因为更短的值会导致高负载 arm64 CPU 上 TLS 连接失败
	//
	dialTimeout = 20 * time.Second
)

// TODO(negz): 停止使用包级 logger。当前我们为每个 CRD 创建一个 etcd client，
// 需要给每个 client 传入 logger，否则它各自创建，
// 在数百个 CRD 时会带来显著内存开销（约占 apiserver 内存 20%）。
//
// https://github.com/kubernetes/kubernetes/issues/111476 for more.
var etcd3ClientLogger *zap.Logger

// grpcpromClientMetrics 是 grpc client prometheus 指标的单例。
var grpcpromClientMetrics = grpcprom.NewClientMetrics()

func init() {
	// 由于不使用全局 prometheus registry，而是自包装的 registry，
	//
	// 参考：https://github.com/kubernetes/kubernetes/pull/81387
	legacyregistry.RawMustRegister(grpcpromClientMetrics)

	l, err := logutil.CreateDefaultZapLogger(etcdClientDebugLevel())
	if err != nil {
		l = zap.NewNop()
	}
	etcd3ClientLogger = l.Named("etcd-client")
}

// etcdClientDebugLevel 把 ETCD_CLIENT_DEBUG 转换为 zap 日志级别。
// NOTE(negz): 以下复制自 etcd client 的私有函数：
// https://github.com/etcd-io/etcd/blob/v3.5.4/client/v3/logger.go#L47
//
// 环境变量为空或 "true" 时使用 Info 级别；非法取值回退到 Info 并打印弃用提示。
func etcdClientDebugLevel() zapcore.Level {
	envLevel := os.Getenv("ETCD_CLIENT_DEBUG")
	if envLevel == "" || envLevel == "true" {
		return zapcore.InfoLevel
	}
	var l zapcore.Level
	if err := l.Set(envLevel); err == nil {
		log.Printf("Deprecated env ETCD_CLIENT_DEBUG value. Using default level: 'info'")
		return zapcore.InfoLevel
	}
	return l
}

// newETCD3HealthCheck 构造 healthz 探针，超时使用配置值或默认值。
func newETCD3HealthCheck(c storagebackend.Config, stopCh <-chan struct{}) (func() error, error) {
	timeout := storagebackend.DefaultHealthcheckTimeout
	if c.HealthcheckTimeout != time.Duration(0) {
		timeout = c.HealthcheckTimeout
	}
	return newETCD3Check(c, timeout, stopCh)
}

// newETCD3ReadyCheck 构造 readyz 探针；读就绪的超时通常比 healthz 更严格。
func newETCD3ReadyCheck(c storagebackend.Config, stopCh <-chan struct{}) (func() error, error) {
	timeout := storagebackend.DefaultReadinessTimeout
	if c.ReadycheckTimeout != time.Duration(0) {
		timeout = c.ReadycheckTimeout
	}
	return newETCD3Check(c, timeout, stopCh)
}

// atomic error 作为原子存储错误的缓存
// 仅当时间戳更新时才更新错误。
type atomicLastError struct {
	mu        sync.RWMutex
	err       error
	timestamp time.Time
}

// Store 仅在时间戳更新时覆盖错误，防止旧请求的结果覆盖新请求。
func (a *atomicLastError) Store(err error, t time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.timestamp.IsZero() || a.timestamp.Before(t) {
		a.err = err
		a.timestamp = t
	}
}

// Load 返回最近缓存的错误。
func (a *atomicLastError) Load() error {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.err
}

// newETCD3Check 返回健康检查函数。
// 客户端构造可能阻塞，故在后台循环重试，健康检查只需读取当前状态即可。
func newETCD3Check(c storagebackend.Config, timeout time.Duration, stopCh <-chan struct{}) (func() error, error) {
	// etcd 不可用时构造 v3 client 会阻塞并超时。
	// 在后台循环重试直到成功创建 client，并保存 client 或遇到的错误。

	lock := sync.RWMutex{}
	var prober *etcd3ProberMonitor
	clientErr := fmt.Errorf("etcd client connection not yet established")

	go wait.PollImmediateUntil(time.Second, func() (bool, error) {
		lock.Lock()
		defer lock.Unlock()
		newProber, err := newETCD3ProberMonitor(c)
		// 确认服务端此时尚未进入关闭流程。
		// 关闭竞态：若 stopCh 已关闭而本次连接成功，需要立即释放，避免泄漏。
		select {
		case <-stopCh:
			if err == nil {
				newProber.Close()
			}
			return true, nil
		default:
		}
		if err != nil {
			clientErr = err
			return false, nil
		}
		prober = newProber
		clientErr = nil
		return true, nil
	}, stopCh)

	// 关停时关闭 client。
	go func() {
		defer utilruntime.HandleCrash()
		<-stopCh

		lock.Lock()
		defer lock.Unlock()
		if prober != nil {
			prober.Close()
			clientErr = fmt.Errorf("server is shutting down")
		}
	}()

	// 限速为每半个配置的超时时间一次请求，最大突发量为 1；被限流的调用会收到
	// 最近一次已发送请求的错误（注意：不是最近一次收到的响应）。
	// 限速避免健康检查本身给 etcd 造成压力；被限流的调用返回最近一次结果。
	limiter := rate.NewLimiter(rate.Every(timeout/2), 1)
	// 初始状态为 clientErr
	lastError := &atomicLastError{err: fmt.Errorf("etcd client connection not yet established")}

	return func() error {
		// client 会在关停时关闭，因此整个健康检查调用期间都持锁，
		//
		// 健康检查有 2 秒超时，最坏情况下关停多阻塞 2 秒可以接受。
		//
		lock.RLock()
		defer lock.RUnlock()

		if clientErr != nil {
			return clientErr
		}
		if limiter.Allow() == false {
			return lastError.Load()
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		now := time.Now()
		err := prober.Probe(ctx)
		lastError.Store(err, now)
		return err
	}, nil
}

// newETCD3ProberMonitor 为健康检查与存储指标创建独立客户端。
func newETCD3ProberMonitor(c storagebackend.Config) (*etcd3ProberMonitor, error) {
	client, err := newETCD3Client(c.Transport)
	if err != nil {
		return nil, err
	}
	return &etcd3ProberMonitor{
		client:    client.Client,
		prefix:    c.Prefix,
		endpoints: c.Transport.ServerList,
	}, nil
}

// etcd3ProberMonitor 持有健康检查专用客户端，并提供 Probe/Monitor 两种操作。
type etcd3ProberMonitor struct {
	prefix    string
	endpoints []string

	mux    sync.RWMutex
	client *clientv3.Client
	closed bool
}

// Close 关闭底层客户端；重复关闭返回错误而非 panic。
func (t *etcd3ProberMonitor) Close() error {
	t.mux.Lock()
	defer t.mux.Unlock()
	if !t.closed {
		t.closed = true
		return t.client.Close()
	}
	return fmt.Errorf("closed")
}

// Probe 对 etcd 执行一次读操作来判定其可用性；只有读成功才算健康。
func (t *etcd3ProberMonitor) Probe(ctx context.Context) error {
	t.mux.RLock()
	defer t.mux.RUnlock()
	if t.closed {
		return fmt.Errorf("closed")
	}
	// 见 https://github.com/etcd-io/etcd/blob/c57f8b3af865d1b531b979889c602ba14377420e/etcdctl/ctlv3/command/ep_command.go#L118
	_, err := t.client.Get(ctx, path.Join("/", t.prefix, "health"))
	if err != nil {
		return fmt.Errorf("error getting data from etcd: %w", err)
	}
	return nil
}

// Monitor 随机选择一个 endpoint 查询状态，返回数据库物理大小。
// 随机选取可避免总打同一个成员，导致指标无法反映整个集群的分布。
func (t *etcd3ProberMonitor) Monitor(ctx context.Context) (metrics.StorageMetrics, error) {
	t.mux.RLock()
	defer t.mux.RUnlock()
	if t.closed {
		return metrics.StorageMetrics{}, fmt.Errorf("closed")
	}
	status, err := t.client.Status(ctx, t.endpoints[rand.Int()%len(t.endpoints)])
	if err != nil {
		return metrics.StorageMetrics{}, err
	}
	return metrics.StorageMetrics{
		Size: status.DbSize,
	}, nil
}

// newETCD3Client 构造 etcd 客户端并等待连接就绪。
// 可通过变量替换以便测试注入假客户端。
var newETCD3Client = func(c storagebackend.TransportConfig) (*kubernetes.Client, error) {
	tlsInfo := transport.TLSInfo{
		CertFile:      c.CertFile,
		KeyFile:       c.KeyFile,
		TrustedCAFile: c.TrustedCAFile,
	}
	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		return nil, err
	}
	// 注意：client 依赖 tlsConfig 为 nil 来表示非安全（明文）连接，
	// 因此这里要更新这个隐式变量。
	// 未配置任何证书文件时显式置 nil，表示使用明文连接。
	if len(c.CertFile) == 0 && len(c.KeyFile) == 0 && len(c.TrustedCAFile) == 0 {
		tlsConfig = nil
	}
	networkContext := egressselector.Etcd.AsNetworkContext()
	var egressDialer utilnet.DialFunc
	if c.EgressLookup != nil {
		// EgressSelector 允许把 etcd 流量交给 konnectivity 等代理转发。
		egressDialer, err = c.EgressLookup(networkContext)
		if err != nil {
			return nil, err
		}
	}
	dialOptions := []grpc.DialOption{
		// 使用串联的 interceptor，以保留默认的重试与退避 interceptor，
		// 否则它们会被指标 interceptor 覆盖。
		//
		// 这些可选 interceptor 放在默认 ones 之后，
		//
		grpc.WithChainUnaryInterceptor(grpcpromClientMetrics.UnaryClientInterceptor()),
		grpc.WithChainStreamInterceptor(grpcpromClientMetrics.StreamClientInterceptor()),
	}
	tracingOpts := []otelgrpc.Option{
		otelgrpc.WithMessageEvents(otelgrpc.ReceivedEvents, otelgrpc.SentEvents),
		otelgrpc.WithPropagators(tracing.Propagators()),
		otelgrpc.WithTracerProvider(c.TracerProvider),
	}
	// 即便使用 Noop TracerProvider，otelgrpc 仍会处理 context 传播。
	// 见 https://github.com/open-telemetry/opentelemetry-go/tree/main/example/passthrough
	dialOptions = append(dialOptions,
		grpc.WithStatsHandler(otelgrpc.NewClientHandler(tracingOpts...)))
	if egressDialer != nil {
		dialer := func(ctx context.Context, addr string) (net.Conn, error) {
			// 兼容旧版 etcd 客户端传入 URL 而非 host:port 的行为。
			if strings.Contains(addr, "//") {
				// 3.5 之前的 etcd client 会把 URL 传给 dialer，这里归一化为地址。
				u, err := url.Parse(addr)
				if err != nil {
					return nil, err
				}
				addr = u.Host
			}
			return egressDialer(ctx, "tcp", addr)
		}
		dialOptions = append(dialOptions, grpc.WithContextDialer(dialer))
	}

	cfg := clientv3.Config{
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    keepaliveTime,
		DialKeepAliveTimeout: keepaliveTimeout,
		DialOptions:          dialOptions,
		Endpoints:            c.ServerList,
		TLS:                  tlsConfig,
		Logger:               etcd3ClientLogger,
	}

	kClient, err := kubernetes.New(cfg)
	if err != nil {
		return nil, err
	}

	if err := blockUntilReady(dialTimeout, kClient); err != nil {
		return nil, err
	}

	return kClient, nil
}

// blockUntilReady 等待 gRPC 连接进入 Ready 状态，或超时返回错误。
// 显式 Connect 处理 Idle 状态，否则 WaitForStateChange 可能一直等不到变化。
func blockUntilReady(timeout time.Duration, client *kubernetes.Client) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	conn := client.Client.ActiveConnection()
	if conn == nil {
		return fmt.Errorf("no grpc connection")
	}

	for {
		currentState := conn.GetState()
		if currentState == connectivity.Ready {
			// 就绪，返回
			return nil
		}
		if currentState == connectivity.Idle {
			// 尝试连接
			conn.Connect()
		}
		// 等待状态从 currentState 变化，直到 context 超时
		if !conn.WaitForStateChange(ctx, currentState) {
			return fmt.Errorf("etcd grpc connection not ready: %w", ctx.Err())
		}
	}
}

// runningCompactor 表示一个按传输配置共享的压缩器；refs 记录引用它的存储数量。
type runningCompactor struct {
	interval  time.Duration
	client    *clientv3.Client
	compactor etcd3.Compactor
	cancel    DestroyFunc
	refs      int
}

var (
	// compactorsMu 保护对 compactors map 的访问
	compactorsMu sync.Mutex
	compactors   = map[string]*runningCompactor{}
)

// startCompactorOnce 为每种 transport 启动一个 compactor。重复调用若间隔变短，
// 则替换 compactor。返回 destroy 函数；同一 transport 的所有 destroy 都被调用时
//
// 同一 etcd 传输配置只跑一个压缩器，避免多个资源各自压缩造成 etcd 压力；
// interval 变为更小值时替换为更高频的压缩器。
func startCompactorOnce(c storagebackend.TransportConfig, interval time.Duration) (etcd3.Compactor, func(), error) {
	compactorsMu.Lock()
	defer compactorsMu.Unlock()

	if interval == 0 {
		// apiserver 禁用压缩请求时直接短路
		return nil, func() {}, nil
	}
	key := fmt.Sprintf("%v", c) // 给出：{[server1 server2] keyFile certFile caFile}
	// 复用条件：不存在，或新间隔更小需要更频繁压缩。
	if compactor, foundBefore := compactors[key]; !foundBefore || compactor.interval > interval {
		client, err := newETCD3Client(c)
		if err != nil {
			return nil, nil, err
		}
		compactorClient := client.Client

		if foundBefore {
			// 替换 compactor
			compactor.cancel()
		} else {
			// 启动新 compactor
			compactor = &runningCompactor{}
			compactors[key] = compactor
		}

		compactor.interval = interval
		compactor.client = compactorClient
		c := etcd3.StartCompactorPerEndpoint(compactorClient, interval)
		compactor.compactor = c
		compactor.cancel = c.Stop
	}

	compactors[key].refs++

	// 每个调用方拿到独立 destroy 闭包，引用计数归零时才真正停止并关闭客户端。
	return compactors[key].compactor, func() {
		compactorsMu.Lock()
		defer compactorsMu.Unlock()

		compactor := compactors[key]
		compactor.refs--
		if compactor.refs == 0 {
			compactor.cancel()
			compactor.client.Close()
			delete(compactors, key)
		}
	}, nil
}

// newETCD3Storage 构造 etcd3 存储，并把压缩器、客户端与存储的生命周期绑定到返回的 destroy 函数。
func newETCD3Storage(c storagebackend.ConfigForResource, newFunc, newListFunc func() runtime.Object, resourcePrefix string) (storage.Interface, DestroyFunc, error) {
	compactor, stopCompactor, err := startCompactorOnce(c.Transport, c.CompactionInterval)
	if err != nil {
		return nil, nil, err
	}

	client, err := newETCD3Client(c.Transport)
	if err != nil {
		stopCompactor()
		return nil, nil, err
	}

	// 装饰 KV 实例，以便我们按请求跟踪 etcd 延迟。
	// 指标按 GVR 记录延迟，装饰器把请求级信息注入到 etcd 调用中。
	client.KV = etcd3.NewETCDLatencyTracker(client.KV)

	transformer := c.Transformer
	if transformer == nil {
		// 未配置加密时使用 identity，同时完成密文转换的合法性检查。
		transformer = identity.NewEncryptCheckTransformer()
	}

	versioner := storage.APIObjectVersioner{}
	decoder := etcd3.NewDefaultDecoder(c.Codec, versioner)

	// 允许在特性开启时删除损坏对象：需要 transformer/decoder 对损坏数据给出可识别的错误。
	if utilfeature.DefaultFeatureGate.Enabled(genericfeatures.AllowUnsafeMalformedObjectDeletion) {
		transformer = etcd3.WithCorruptObjErrorHandlingTransformer(transformer)
		decoder = etcd3.WithCorruptObjErrorHandlingDecoder(decoder)
	}
	store, err := etcd3.New(client, compactor, c.Codec, newFunc, newListFunc, c.Prefix, resourcePrefix, c.GroupResource, transformer, c.LeaseManagerConfig, decoder, versioner)
	if err != nil {
		stopCompactor()
		_ = client.Close()
		return nil, nil, err
	}
	var once sync.Once
	destroyFunc := func() {
		// 已知 storage destroy 函数会被多次调用（因子资源复用），
		// 因此只销毁一次。
		// TODO: 在更高层修复重复的 storage destroy 调用
		once.Do(func() {
			stopCompactor()
			store.Close()
			_ = client.Close()
		})
	}
	var storage storage.Interface = store
	if utilfeature.DefaultFeatureGate.Enabled(genericfeatures.AllowUnsafeMalformedObjectDeletion) {
		// 包装层额外允许对无法解码的对象执行删除，便于修复污染数据。
		storage = etcd3.NewStoreWithUnsafeCorruptObjectDeletion(storage, c.GroupResource)
	}
	return storage, destroyFunc, nil
}
