/*
Copyright 2021 The Kubernetes Authors.

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

package testserver

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/kubernetes"
	"go.etcd.io/etcd/server/v3/embed"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	storagetesting "k8s.io/apiserver/pkg/storage/testing"
)

// getAvailablePort returns a TCP port that is available for binding.
//
// 注意这是"先探测后使用"的 TOCTOU 模式：探测到的端口在真正 bind 前可能被抢走。
// 调用方需容忍偶发失败（RunEtcd 中用锁串行化来降低概率）。
func getAvailablePorts(count int) ([]int, error) {
	ports := []int{}
	for i := 0; i < count; i++ {
		l, err := net.Listen("tcp", ":0")
		if err != nil {
			return nil, fmt.Errorf("could not bind to a port: %v", err)
		}
		// It is possible but unlikely that someone else will bind this port before we get a chance to use it.
		defer l.Close()
		ports = append(ports, l.Addr().(*net.TCPAddr).Port)
	}
	return ports, nil
}

// NewTestConfig returns a configuration for an embedded etcd server.
// The configuration is based on embed.NewConfig(), with the following adjustments:
//   - sets UnsafeNoFsync = true to improve test performance (only reasonable in a test-only
//     single-member server we never intend to restart or keep data from)
//   - uses free ports for client and peer listeners
//   - cleans up the data directory on test termination
//   - silences server logs other than errors
func NewTestConfig(t testing.TB) *embed.Config {
	cfg := embed.NewConfig()

	// 关闭 fsync 换取速度；仅因单成员、用完即弃的测试实例才安全。
	cfg.UnsafeNoFsync = true

	ports, err := getAvailablePorts(2)
	if err != nil {
		t.Fatal(err)
	}
	clientURL := url.URL{Scheme: "http", Host: net.JoinHostPort("localhost", strconv.Itoa(ports[0]))}
	peerURL := url.URL{Scheme: "http", Host: net.JoinHostPort("localhost", strconv.Itoa(ports[1]))}

	cfg.ListenPeerUrls = []url.URL{peerURL}
	cfg.AdvertisePeerUrls = []url.URL{peerURL}
	cfg.ListenClientUrls = []url.URL{clientURL}
	cfg.AdvertiseClientUrls = []url.URL{clientURL}
	// 单节点集群必须让 InitialCluster 与自身名称一致，否则 etcd 会等待其他成员。
	cfg.InitialCluster = cfg.InitialClusterFromName(cfg.Name)

	// 只保留 error 级别日志，避免测试输出被 etcd 的例行信息淹没。
	cfg.ZapLoggerBuilder = embed.NewZapLoggerBuilder(zaptest.NewLogger(t, zaptest.Level(zapcore.ErrorLevel)).Named("etcd-server"))
	cfg.Dir = t.TempDir()
	os.Chmod(cfg.Dir, 0700)
	return cfg
}

// autoPortLock 串行化"自动选端口并启动"的过程，缩小端口被抢占的窗口。
var autoPortLock sync.Mutex

// RunEtcd starts an embedded etcd server with the provided config
// (or NewTestConfig(t) if nil), and returns a client connected to the server.
// The server is terminated when the test ends.
func RunEtcd(t testing.TB, cfg *embed.Config) *kubernetes.Client {
	t.Helper()

	if cfg == nil {
		// if we have to autopick free ports, lock until we successfully start the server on the ports we chose
		autoPortLock.Lock()
		defer autoPortLock.Unlock()
		cfg = NewTestConfig(t)
	}

	e, err := embed.StartEtcd(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// 用 Cleanup 而非 defer，保证测试无论成功失败都会关闭服务器。
	t.Cleanup(e.Close)

	select {
	case <-e.Server.ReadyNotify():
	case <-time.After(60 * time.Second):
		e.Server.Stop() // trigger a shutdown
		t.Fatal("server took too long to start")
	}
	// etcd 的运行时错误通过 channel 上报，这里转成测试错误，避免被静默吞掉。
	go func() {
		err := <-e.Err()
		if err != nil {
			t.Error(err)
		}
	}()

	tlsConfig, err := cfg.ClientTLSInfo.ClientConfig()
	if err != nil {
		t.Fatal(err)
	}

	client, err := kubernetes.New(clientv3.Config{
		TLS:         tlsConfig,
		Endpoints:   e.Server.Cluster().ClientURLs(),
		DialTimeout: 10 * time.Second,
		Logger:      zaptest.NewLogger(t, zaptest.Level(zapcore.ErrorLevel)).Named("etcd-client"),
	})
	if err != nil {
		t.Fatal(err)
	}
	// 包一层记录器，使测试可以断言实际发出的 etcd 请求，而不只是观察最终状态。
	kubernetesRecorder := storagetesting.NewKubernetesRecorder(client.Kubernetes)
	client.KV = storagetesting.NewKVRecorder(client.KV, kubernetesRecorder)
	client.Kubernetes = kubernetesRecorder
	return client
}
