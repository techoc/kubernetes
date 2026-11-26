//go:build !windows

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

// Package kmsv2 用 Envelope v2 provider 对静态存储的数据做加解密转换。
package kmsv2

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apiserver/pkg/storage/value/encrypt/envelope/metrics"
	mock "k8s.io/apiserver/pkg/storage/value/encrypt/envelope/testing/v2"
	"k8s.io/component-base/metrics/testutil"
	kmsservice "k8s.io/kms/pkg/service"
)

const (
	testProviderName = "providerName"
)

// testSocket 保存抽象 unix socket 的路径与 endpoint URL 两种形式。
type testSocket struct {
	path     string
	endpoint string
}

// newEndpoint 为测试中使用的 Linux 抽象 socket 构造唯一名称。
// 本包使用 Linux 域 socket，从而无需清理 socket 文件。
func newEndpoint() *testSocket {
	p := fmt.Sprintf("@%s.sock", uuid.NewUUID())

	return &testSocket{
		path:     p,
		endpoint: fmt.Sprintf("unix:///%s", p),
	}
}

// TestKMSPluginLateStart 测试 kms-plugin pod/容器晚于 kube-apiserver 启动的场景。
// 由于对 kms-plugin 的 Dial 是非阻塞的，即便 kms-plugin 尚未起来，gRPC service 的构造也应成功——
// 拨号在后台进行。
func TestKMSPluginLateStart(t *testing.T) {
	t.Parallel()
	callTimeout := 3 * time.Second
	s := newEndpoint()

	ctx := testContext(t)

	// 插件尚未启动时也应能构造 service，这是"非阻塞 Dial"的核心契约。
	service, err := NewGRPCService(ctx, s.endpoint, testProviderName, callTimeout)
	if err != nil {
		t.Fatalf("failed to create envelope service, error: %v", err)
	}
	defer destroyService(service)

	// 在调用超时窗口内启动插件，后续 Encrypt 应能成功。
	time.Sleep(callTimeout / 2)
	_ = mock.NewBase64Plugin(t, s.path)

	data := []byte("test data")
	uid := string(uuid.NewUUID())
	_, err = service.Encrypt(ctx, uid, data)
	if err != nil {
		t.Fatalf("failed when execute encrypt, error: %v", err)
	}
}

// TestTimeouts 覆盖调用超时与插件/服务端启动先后顺序的组合：
// 零超时永远失败（无效配置），超时大于插件延迟才成功。
func TestTimeouts(t *testing.T) {
	t.Parallel()
	var testCases = []struct {
		desc               string
		callTimeout        time.Duration
		pluginDelay        time.Duration
		kubeAPIServerDelay time.Duration
		wantErr            string
	}{
		{
			desc:        "timeout zero - expect failure when call from kube-apiserver arrives before plugin starts",
			callTimeout: 0 * time.Second,
			pluginDelay: 3 * time.Second,
			wantErr:     "rpc error: code = DeadlineExceeded desc = context deadline exceeded",
		},
		{
			desc:               "timeout zero but kms-plugin already up - still failure - zero timeout is an invalid value",
			callTimeout:        0 * time.Second,
			pluginDelay:        0 * time.Second,
			kubeAPIServerDelay: 2 * time.Second,
			wantErr:            "rpc error: code = DeadlineExceeded desc = context deadline exceeded",
		},
		{
			desc:        "timeout greater than kms-plugin delay - expect success",
			callTimeout: 6 * time.Second,
			pluginDelay: 3 * time.Second,
		},
		{
			desc:        "timeout less than kms-plugin delay - expect failure",
			callTimeout: 3 * time.Second,
			pluginDelay: 6 * time.Second,
			wantErr:     "rpc error: code = DeadlineExceeded desc = context deadline exceeded",
		},
	}

	for _, tt := range testCases {
		t.Run(tt.desc, func(t *testing.T) {
			t.Parallel()
			var (
				service         kmsservice.Service
				err             error
				data            = []byte("test data")
				uid             = string(uuid.NewUUID())
				kubeAPIServerWG sync.WaitGroup
				kmsPluginWG     sync.WaitGroup
				testCompletedWG sync.WaitGroup
				socketName      = newEndpoint()
			)

			testCompletedWG.Add(1)
			defer testCompletedWG.Done()

			ctx := testContext(t)

			kubeAPIServerWG.Add(1)
			go func() {
				// 模拟 kube-apiserver 晚启动：按用例要求，插件先于 kube-apiserver 启动。
				time.Sleep(tt.kubeAPIServerDelay)

				service, err = NewGRPCService(ctx, socketName.endpoint, testProviderName, tt.callTimeout)
				if err != nil {
					t.Errorf("failed to create envelope service, error: %v", err)
					return
				}
				defer destroyService(service)
				kubeAPIServerWG.Done()
				// 保持 kube-apiserver 运行以处理请求。
				testCompletedWG.Wait()
			}()

			kmsPluginWG.Add(1)
			go func() {
				// 模拟 kms-plugin 延迟启动：按用例要求，kube-apiserver 先于插件启动。
				time.Sleep(tt.pluginDelay)

				_ = mock.NewBase64Plugin(t, socketName.path)
				kmsPluginWG.Done()
				// 保持插件运行以处理请求。
				testCompletedWG.Wait()
			}()

			kubeAPIServerWG.Wait()
			if t.Failed() {
				return
			}
			_, err = service.Encrypt(ctx, uid, data)

			if err == nil && tt.wantErr != "" {
				t.Fatalf("got nil, want %s", tt.wantErr)
			}

			if err != nil && tt.wantErr == "" {
				t.Fatalf("got %q, want nil", err.Error())
			}

			// 收集 kms-plugin，让插件得以清理。
			kmsPluginWG.Wait()
		})
	}
}

// TestIntermittentConnectionLoss 测试与 kms-plugin 的连接间歇性断开的场景。
//
// 期望 gRPC 客户端在插件重启后自动重连，期间发起的调用最终成功，
// 验证的是客户端连接管理而非插件实现。
func TestIntermittentConnectionLoss(t *testing.T) {
	t.Parallel()
	var (
		wg1        sync.WaitGroup
		wg2        sync.WaitGroup
		timeout    = 30 * time.Second
		blackOut   = 1 * time.Second
		data       = []byte("test data")
		uid        = string(uuid.NewUUID())
		endpoint   = newEndpoint()
		encryptErr error
	)
	// 启动 KMS 插件。
	f := mock.NewBase64Plugin(t, endpoint.path)

	ctx := testContext(t)

	// 连接 kms 插件。
	service, err := NewGRPCService(ctx, endpoint.endpoint, testProviderName, timeout)
	if err != nil {
		t.Fatalf("failed to create envelope service, error: %v", err)
	}
	defer destroyService(service)

	_, err = service.Encrypt(ctx, uid, data)
	if err != nil {
		t.Fatalf("failed when execute encrypt, error: %v", err)
	}
	t.Log("Connected to KMSPlugin")

	// 停止 KMS 插件，模拟连接断开。
	t.Log("KMS Plugin is stopping")
	f.CleanUp()
	time.Sleep(2 * time.Second)

	wg1.Add(1)
	wg2.Add(1)
	go func() {
		defer wg2.Done()
		// 调用 service 加密数据。
		t.Log("Sending encrypt request")
		wg1.Done()
		_, err := service.Encrypt(ctx, uid, data)
		if err != nil {
			encryptErr = fmt.Errorf("failed when executing encrypt, error: %v", err)
		}
	}()

	wg1.Wait()
	time.Sleep(blackOut)
	// 启动 KMS 插件。
	_ = mock.NewBase64Plugin(t, endpoint.path)
	t.Log("Restarted KMS Plugin")

	wg2.Wait()

	if encryptErr != nil {
		t.Error(encryptErr)
	}
}

// 正常的加解密操作。
func TestGRPCService(t *testing.T) {
	t.Parallel()
	// 启动测试 gRPC server。
	endpoint := newEndpoint()
	_ = mock.NewBase64Plugin(t, endpoint.path)

	ctx := testContext(t)

	// 创建 gRPC 客户端 service。
	service, err := NewGRPCService(ctx, endpoint.endpoint, testProviderName, 1*time.Second)
	if err != nil {
		t.Fatalf("failed to create envelope service, error: %v", err)
	}
	defer destroyService(service)

	// 调用 service 加密数据。
	data := []byte("test data")
	uid := string(uuid.NewUUID())
	resp, err := service.Encrypt(ctx, uid, data)
	if err != nil {
		t.Fatalf("failed when execute encrypt, error: %v", err)
	}

	keyID := "1"
	// 调用 service 解密数据。
	result, err := service.Decrypt(ctx, uid, &kmsservice.DecryptRequest{Ciphertext: resp.Ciphertext, KeyID: keyID})
	if err != nil {
		t.Fatalf("failed when execute decrypt, error: %v", err)
	}

	if !reflect.DeepEqual(data, result) {
		t.Errorf("expect: %v, but: %v", data, result)
	}
}

// 多 goroutine 下的正常加解密操作。
func TestGRPCServiceConcurrentAccess(t *testing.T) {
	t.Parallel()
	// 启动测试 gRPC server。
	endpoint := newEndpoint()
	_ = mock.NewBase64Plugin(t, endpoint.path)

	ctx := testContext(t)

	// 创建 gRPC 客户端 service。
	service, err := NewGRPCService(ctx, endpoint.endpoint, testProviderName, 15*time.Second)
	if err != nil {
		t.Fatalf("failed to create envelope service, error: %v", err)
	}
	defer destroyService(service)

	var wg sync.WaitGroup
	n := 100
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			// 调用 service 加密数据。
			data := []byte("test data")
			uid := string(uuid.NewUUID())
			resp, err := service.Encrypt(ctx, uid, data)
			if err != nil {
				t.Errorf("failed when execute encrypt, error: %v", err)
			}

			keyID := "1"
			// 调用 service 解密数据。
			result, err := service.Decrypt(ctx, uid, &kmsservice.DecryptRequest{Ciphertext: resp.Ciphertext, KeyID: keyID})
			if err != nil {
				t.Errorf("failed when execute decrypt, error: %v", err)
			}

			if !reflect.DeepEqual(data, result) {
				t.Errorf("expect: %v, but: %v", data, result)
			}
		}()
	}

	wg.Wait()
}

// destroyService 关闭底层 gRPC 连接，避免测试泄漏连接。
func destroyService(service kmsservice.Service) {
	if service != nil {
		s := service.(*gRPCService)
		s.connection.Close()
	}
}

// 测试 KMS provider 的各种无效配置。
func TestInvalidConfiguration(t *testing.T) {
	t.Parallel()
	// 启动测试 gRPC server。
	_ = mock.NewBase64Plugin(t, newEndpoint().path)

	ctx := testContext(t)

	invalidConfigs := []struct {
		name     string
		endpoint string
	}{
		{"emptyConfiguration", ""},
		{"invalidScheme", "tcp://localhost:6060"},
	}

	for _, testCase := range invalidConfigs {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := NewGRPCService(ctx, testCase.endpoint, testProviderName, 1*time.Second)
			if err == nil {
				t.Fatalf("should fail to create envelope service for %s.", testCase.name)
			}
		})
	}
}

// TestKMSOperationsMetric 验证 Encrypt/Decrypt/Status 各自写入带方法名与状态码
// 标签的延迟直方图；Reset 是为了支持 go test -count X 重复运行。
func TestKMSOperationsMetric(t *testing.T) {
	endpoint := newEndpoint()
	_ = mock.NewBase64Plugin(t, endpoint.path)

	ctx := testContext(t)

	service, err := NewGRPCService(ctx, endpoint.endpoint, testProviderName, 15*time.Second)
	if err != nil {
		t.Fatalf("failed to create envelope service, error: %v", err)
	}
	defer destroyService(service)
	metrics.RegisterMetrics()
	metrics.KMSOperationsLatencyMetric.Reset() // 支持 `go test -count X` 重复运行。

	testCases := []struct {
		name        string
		operation   func()
		labelValues []string
		wantCount   uint64
	}{
		{
			name: "encrypt",
			operation: func() {
				if _, err = service.Encrypt(ctx, "1", []byte("test data")); err != nil {
					t.Fatalf("failed when execute encrypt, error: %v", err)
				}
			},
			labelValues: []string{testProviderName, "/v2.KeyManagementService/Encrypt", "OK"},
			wantCount:   1,
		},
		{
			name: "decrypt",
			operation: func() {
				if _, err = service.Decrypt(ctx, "1", &kmsservice.DecryptRequest{Ciphertext: []byte("testdata"), KeyID: "1"}); err != nil {
					t.Fatalf("failed when execute decrypt, error: %v", err)
				}
			},
			labelValues: []string{testProviderName, "/v2.KeyManagementService/Decrypt", "OK"},
			wantCount:   1,
		},
		{
			name: "status",
			operation: func() {
				if _, err = service.Status(ctx); err != nil {
					t.Fatalf("failed when execute status, error: %v", err)
				}
			},
			labelValues: []string{testProviderName, "/v2.KeyManagementService/Status", "OK"},
			wantCount:   1,
		},
		{
			name: "multiple status calls",
			operation: func() {
				for i := 0; i < 10; i++ {
					if _, err = service.Status(ctx); err != nil {
						t.Fatalf("failed when execute status, error: %v", err)
					}
				}
			},
			labelValues: []string{testProviderName, "/v2.KeyManagementService/Status", "OK"},
			wantCount:   10,
		},
	}

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			tt.operation()
			defer metrics.KMSOperationsLatencyMetric.Reset()
			sampleSum, err := testutil.GetHistogramMetricValue(metrics.KMSOperationsLatencyMetric.WithLabelValues(tt.labelValues...))
			if err != nil {
				t.Fatalf("failed to get metric value, error: %v", err)
			}
			// apiserver_envelope_encryption_kms_operations_latency_seconds_sum{grpc_status_code="OK",method_name="/v2alpha1.KeyManagementService/Encrypt",provider_name="providerName"} 0.000881432
			if sampleSum == 0 {
				t.Fatalf("expected metric value to be greater than 0, got %v", sampleSum)
			}
			count, err := testutil.GetHistogramMetricCount(metrics.KMSOperationsLatencyMetric.WithLabelValues(tt.labelValues...))
			if err != nil {
				t.Fatalf("failed to get metric count, error: %v", err)
			}
			// apiserver_envelope_encryption_kms_operations_latency_seconds_count{grpc_status_code="OK",method_name="/v2alpha1.KeyManagementService/Encrypt",provider_name="providerName"} 1
			if count != tt.wantCount {
				t.Fatalf("expected metric count to be %v, got %v", tt.wantCount, count)
			}
		})
	}
}

// testContext 返回一个随测试结束自动取消的 context。
func testContext(t *testing.T) context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return ctx
}
