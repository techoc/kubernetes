/*
Copyright 2022 The Kubernetes Authors.

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

package factory

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/kubernetes"
	"k8s.io/apiserver/pkg/storage/etcd3/testserver"
	"k8s.io/apiserver/pkg/storage/storagebackend"
)

// mockKV 是 clientv3.KV 的替身：只有 Get 可定制，其它方法返回零值，
// 足以模拟健康检查所依赖的最小读写面。
type mockKV struct {
	get func(ctx context.Context) (*clientv3.GetResponse, error)
}

func (mkv mockKV) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	return nil, nil
}
func (mkv mockKV) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	return mkv.get(ctx)
}
func (mockKV) GetStream(ctx context.Context, key string, opts ...clientv3.OpOption) (clientv3.GetStreamChan, error) {
	return nil, nil
}
func (mockKV) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	return nil, nil
}
func (mockKV) Compact(ctx context.Context, rev int64, opts ...clientv3.CompactOption) (*clientv3.CompactResponse, error) {
	return nil, nil
}
func (mockKV) Do(ctx context.Context, op clientv3.Op) (clientv3.OpResponse, error) {
	return clientv3.OpResponse{}, nil
}
func (mockKV) Txn(ctx context.Context) clientv3.Txn {
	return nil
}

// TestCreateHealthcheck 验证健康检查遵守 HealthcheckTimeout：默认超时为
// 2 秒，可由配置覆盖；etcd 响应过慢时应返回 DeadlineExceeded。
func TestCreateHealthcheck(t *testing.T) {
	etcdConfig := testserver.NewTestConfig(t)
	client := testserver.RunEtcd(t, etcdConfig)
	newETCD3ClientFn := newETCD3Client
	defer func() {
		newETCD3Client = newETCD3ClientFn
	}()
	tests := []struct {
		name         string
		cfg          storagebackend.Config
		want         error
		responseTime time.Duration
	}{
		{
			name: "ok if response time lower than default timeout",
			cfg: storagebackend.Config{
				Type:      storagebackend.StorageTypeETCD3,
				Transport: storagebackend.TransportConfig{},
			},
			responseTime: 1 * time.Second,
			want:         nil,
		},
		{
			name: "ok if response time lower than custom timeout",
			cfg: storagebackend.Config{
				Type:               storagebackend.StorageTypeETCD3,
				Transport:          storagebackend.TransportConfig{},
				HealthcheckTimeout: 5 * time.Second,
			},
			responseTime: 3 * time.Second,
			want:         nil,
		},
		{
			name: "timeouts if response time higher than default timeout",
			cfg: storagebackend.Config{
				Type:      storagebackend.StorageTypeETCD3,
				Transport: storagebackend.TransportConfig{},
			},
			responseTime: 3 * time.Second,
			want:         context.DeadlineExceeded,
		},
		{
			name: "timeouts if response time higher than custom timeout",
			cfg: storagebackend.Config{
				Type:               storagebackend.StorageTypeETCD3,
				Transport:          storagebackend.TransportConfig{},
				HealthcheckTimeout: 3 * time.Second,
			},
			responseTime: 5 * time.Second,
			want:         context.DeadlineExceeded,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ready := make(chan struct{})
			tc.cfg.Transport.ServerList = client.Endpoints()
			newETCD3Client = func(c storagebackend.TransportConfig) (*kubernetes.Client, error) {
				defer close(ready)
				dummyKV := mockKV{
					get: func(ctx context.Context) (*clientv3.GetResponse, error) {
						select {
						case <-ctx.Done():
							return nil, ctx.Err()
						case <-time.After(tc.responseTime):
							return nil, nil
						}
					},
				}
				client.KV = dummyKV
				return client, nil
			}
			stop := make(chan struct{})
			defer close(stop)

			healthcheck, err := CreateHealthCheck(tc.cfg, stop)
			if err != nil {
				t.Fatal(err)
			}
			// 等待健康检查建立连接。
			<-ready
			got := healthcheck()

			if !errors.Is(got, tc.want) {
				t.Errorf("healthcheck() missmatch want %v got %v", tc.want, got)
			}
		})
	}
}

// TestCreateReadycheck 验证就绪检查使用 ReadycheckTimeout（而不是
// HealthcheckTimeout）：即便 healthcheck 超时被调大，就绪检查仍按自己的
// 超时判定。
func TestCreateReadycheck(t *testing.T) {
	etcdConfig := testserver.NewTestConfig(t)
	client := testserver.RunEtcd(t, etcdConfig)
	newETCD3ClientFn := newETCD3Client
	defer func() {
		newETCD3Client = newETCD3ClientFn
	}()
	tests := []struct {
		name         string
		cfg          storagebackend.Config
		want         error
		responseTime time.Duration
	}{
		{
			name: "ok if response time lower than default timeout",
			cfg: storagebackend.Config{
				Type:      storagebackend.StorageTypeETCD3,
				Transport: storagebackend.TransportConfig{},
			},
			responseTime: 1 * time.Second,
			want:         nil,
		},
		{
			name: "ok if response time lower than custom timeout",
			cfg: storagebackend.Config{
				Type:              storagebackend.StorageTypeETCD3,
				Transport:         storagebackend.TransportConfig{},
				ReadycheckTimeout: 5 * time.Second,
			},
			responseTime: 3 * time.Second,
			want:         nil,
		},
		{
			name: "timeouts if response time higher than default timeout",
			cfg: storagebackend.Config{
				Type:      storagebackend.StorageTypeETCD3,
				Transport: storagebackend.TransportConfig{},
			},
			responseTime: 3 * time.Second,
			want:         context.DeadlineExceeded,
		},
		{
			name: "timeouts if response time higher than custom timeout",
			cfg: storagebackend.Config{
				Type:              storagebackend.StorageTypeETCD3,
				Transport:         storagebackend.TransportConfig{},
				ReadycheckTimeout: 3 * time.Second,
			},
			responseTime: 5 * time.Second,
			want:         context.DeadlineExceeded,
		},
		{
			// 最后一个用例专门确认 readycheck 不会被 HealthcheckTimeout 影响。
			name: "timeouts if response time higher than default timeout with custom healthcheck timeout",
			cfg: storagebackend.Config{
				Type:               storagebackend.StorageTypeETCD3,
				Transport:          storagebackend.TransportConfig{},
				HealthcheckTimeout: 10 * time.Second,
			},
			responseTime: 3 * time.Second,
			want:         context.DeadlineExceeded,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ready := make(chan struct{})
			tc.cfg.Transport.ServerList = client.Endpoints()
			newETCD3Client = func(c storagebackend.TransportConfig) (*kubernetes.Client, error) {
				defer close(ready)
				dummyKV := mockKV{
					get: func(ctx context.Context) (*clientv3.GetResponse, error) {
						select {
						case <-ctx.Done():
							return nil, ctx.Err()
						case <-time.After(tc.responseTime):
							return nil, nil
						}
					},
				}
				client.KV = dummyKV
				return client, nil
			}
			stop := make(chan struct{})
			defer close(stop)

			healthcheck, err := CreateReadyCheck(tc.cfg, stop)
			if err != nil {
				t.Fatal(err)
			}
			// 等待健康检查建立连接。
			<-ready

			got := healthcheck()

			if !errors.Is(got, tc.want) {
				t.Errorf("healthcheck() missmatch want %v got %v", tc.want, got)
			}
		})
	}
}

// TestRateLimitHealthcheck 验证健康检查的节流：在 HealthcheckTimeout/2 时间内，
// 并发调用应复用同一结果，最多只访问一次 etcd；超时窗口过后才允许再次访问。
func TestRateLimitHealthcheck(t *testing.T) {
	etcdConfig := testserver.NewTestConfig(t)
	client := testserver.RunEtcd(t, etcdConfig)
	newETCD3ClientFn := newETCD3Client
	defer func() {
		newETCD3Client = newETCD3ClientFn
	}()

	cfg := storagebackend.Config{
		Type:               storagebackend.StorageTypeETCD3,
		Transport:          storagebackend.TransportConfig{},
		HealthcheckTimeout: 5 * time.Second,
	}
	cfg.Transport.ServerList = client.Endpoints()
	tests := []struct {
		name string
		want error
	}{
		{
			name: "etcd ok",
		},
		{
			name: "etcd down",
			want: errors.New("etcd down"),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {

			ready := make(chan struct{})

			var counter uint64
			newETCD3Client = func(c storagebackend.TransportConfig) (*kubernetes.Client, error) {
				defer close(ready)
				dummyKV := mockKV{
					get: func(ctx context.Context) (*clientv3.GetResponse, error) {
						atomic.AddUint64(&counter, 1)
						select {
						case <-ctx.Done():
							return nil, ctx.Err()
						default:
							return nil, tc.want
						}
					},
				}
				client.KV = dummyKV
				return client, nil
			}

			stop := make(chan struct{})
			defer close(stop)
			healthcheck, err := CreateHealthCheck(cfg, stop)
			if err != nil {
				t.Fatal(err)
			}
			// 等待健康检查建立连接。
			<-ready
			// 先发一个请求取得初始状态。
			err = healthcheck()
			if !errors.Is(err, tc.want) {
				t.Errorf("healthcheck() mismatch want %v got %v", tc.want, err)
			}

			// 并发发多个请求，状态应与第一个请求一致。
			var wg sync.WaitGroup
			for i := 0; i < 100; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					err := healthcheck()
					if !errors.Is(err, tc.want) {
						t.Errorf("healthcheck() mismatch want %v got %v", tc.want, err)
					}

				}()
			}

			// 所有请求结束后检查计数器。
			wg.Wait()
			if counter != 1 {
				t.Errorf("healthcheck() called etcd %d times, expected only one call", counter)
			}

			// 等待限速器允许新连接。
			time.Sleep(cfg.HealthcheckTimeout / 2)

			// 新请求应只让计数器加 1。
			// 并发发多个请求，状态应与第一个请求一致。
			for i := 0; i < 100; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					err := healthcheck()
					if !errors.Is(err, tc.want) {
						t.Errorf("healthcheck() mismatch want %v got %v", tc.want, err)
					}

				}()
			}
			wg.Wait()

			if counter != 2 {
				t.Errorf("healthcheck() called etcd %d times, expected only two calls", counter)
			}
		})
	}

}

// TestTimeTravelHealthcheck 覆盖“慢响应晚于新请求返回”的竞态：
// 旧请求的错误结果不得覆盖已缓存的新结果。
func TestTimeTravelHealthcheck(t *testing.T) {
	etcdConfig := testserver.NewTestConfig(t)
	client := testserver.RunEtcd(t, etcdConfig)
	newETCD3ClientFn := newETCD3Client
	defer func() {
		newETCD3Client = newETCD3ClientFn
	}()

	cfg := storagebackend.Config{
		Type:               storagebackend.StorageTypeETCD3,
		Transport:          storagebackend.TransportConfig{},
		HealthcheckTimeout: 5 * time.Second,
	}
	cfg.Transport.ServerList = client.Endpoints()

	ready := make(chan struct{})
	signal := make(chan struct{})

	var counter uint64
	newETCD3Client = func(c storagebackend.TransportConfig) (*kubernetes.Client, error) {
		defer close(ready)
		dummyKV := mockKV{
			get: func(ctx context.Context) (*clientv3.GetResponse, error) {
				atomic.AddUint64(&counter, 1)
				val := atomic.LoadUint64(&counter)
				// 第一个请求等一个自定义超时后报错。不用 context 超时，是为了验证缓存结果不会被覆盖：
				// 限速基于 cfg.HealthcheckTimeout/2，该超时会与限速器竞争——
				// 究竟由缓存响应新请求，还是放行它走后端，取决于谁先到。
				//
				// 这里用 2/3 的超时时间制造“旧请求仍在途、新请求已开始”的窗口；
				// 随后断言缓存被新结果更新、旧错误被丢弃。
				if val == 1 {
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-time.After((2 * cfg.HealthcheckTimeout) / 3):
						return nil, fmt.Errorf("etcd down")
					}
				}
				// 后续请求总能成功。
				return nil, nil
			},
		}
		client.KV = dummyKV
		return client, nil
	}

	stop := make(chan struct{})
	defer close(stop)
	healthcheck, err := CreateHealthCheck(cfg, stop)
	if err != nil {
		t.Fatal(err)
	}
	// 等待健康检查建立连接。
	<-ready
	// 发一个 2 秒后失败的请求。
	go func() {
		err := healthcheck()
		if !strings.Contains(err.Error(), "etcd down") {
			t.Errorf("healthcheck() mismatch want %v got %v", fmt.Errorf("etcd down"), err)
		}
		close(signal)
	}()

	// 等待限速器允许新连接。
	time.Sleep(cfg.HealthcheckTimeout / 2)

	select {
	case <-signal:
		t.Errorf("first request should not return yet")
	default:
	}

	// 新请求应成功并让计数器加 1。
	err = healthcheck()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	c := atomic.LoadUint64(&counter)
	if c != 2 {
		t.Errorf("healthcheck() called etcd %d times, expected only two calls", c)
	}

	// 缓存中的请求应保持成功，不被迟到的错误覆盖。
	<-signal
	err = healthcheck()
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	c = atomic.LoadUint64(&counter)
	if c != 2 {
		t.Errorf("healthcheck() called etcd %d times, expected only two calls", c)
	}

}
