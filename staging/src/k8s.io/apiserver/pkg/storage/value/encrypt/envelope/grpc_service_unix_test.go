//go:build !windows

/*
Copyright 2017 The Kubernetes Authors.

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

// Package envelope 使用 Envelope provider 转换静态存储的值
package envelope

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	mock "k8s.io/apiserver/pkg/storage/value/encrypt/envelope/testing/v1beta1"

	"k8s.io/apimachinery/pkg/util/uuid"
)

// testSocket 描述测试用的 unix socket：path 是 Linux abstract socket 名，
// endpoint 是传给 KMS 客户端的 URL。
type testSocket struct {
	path     string
	endpoint string
}

// newEndpoint 为测试构造唯一的 Linux Abstract Socket 名称。
// 使用 abstract socket 可避免测试结束后遗留 socket 文件需要清理。
func newEndpoint() *testSocket {
	p := fmt.Sprintf("@%s.sock", uuid.NewUUID())

	return &testSocket{
		path:     p,
		endpoint: fmt.Sprintf("unix:///%s", p),
	}
}

// TestKMSPluginLateStart 模拟 kms-plugin 在 kube-apiserver 之后启动：
// 由于 Dial 是非阻塞的，即使插件尚未就绪，gRPC 服务也应能构造成功，
// 实际连接在后台建立；插件启动后 Encrypt 应正常工作。
func TestKMSPluginLateStart(t *testing.T) {
	t.Parallel()
	callTimeout := 3 * time.Second
	s := newEndpoint()

	ctx := testContext(t)

	service, err := NewGRPCService(ctx, s.endpoint, callTimeout)
	if err != nil {
		t.Fatalf("failed to create envelope service, error: %v", err)
	}
	defer destroyService(service)

	// 等一半超时时间后再启动插件，确保构造阶段插件确实不在。
	time.Sleep(callTimeout / 2)
	_ = mock.NewBase64Plugin(t, s.path)

	data := []byte("test data")
	_, err = service.Encrypt(data)
	if err != nil {
		t.Fatalf("failed when execute encrypt, error: %v", err)
	}
}

// TestTimeouts 验证在插件启动延迟与调用超时的不同组合下，apiserver 的行为：
// 超时为 0 属于非法配置；超时小于插件延迟时调用失败；超时足够时成功。
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
			// 超时为 0 且插件未启动：调用必然超时
			desc:        "timeout zero - expect failure when call from kube-apiserver arrives before plugin starts",
			callTimeout: 0 * time.Second,
			pluginDelay: 3 * time.Second,
			wantErr:     "rpc error: code = DeadlineExceeded desc = context deadline exceeded",
		},
		{
			// 插件已就绪，但超时为 0 仍是非法值，调用失败
			desc:               "timeout zero but kms-plugin already up - still failure - zero timeout is an invalid value",
			callTimeout:        0 * time.Second,
			pluginDelay:        0 * time.Second,
			kubeAPIServerDelay: 2 * time.Second,
			wantErr:            "rpc error: code = DeadlineExceeded desc = context deadline exceeded",
		},
		{
			// 超时大于插件延迟：成功
			desc:        "timeout greater than kms-plugin delay - expect success",
			callTimeout: 6 * time.Second,
			pluginDelay: 3 * time.Second,
		},
		{
			// 超时小于插件延迟：失败
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
				service         Service
				err             error
				data            = []byte("test data")
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
				// 模拟 kube-apiserver 晚启动：如用例要求，插件会先于 apiserver 就绪。
				time.Sleep(tt.kubeAPIServerDelay)

				service, err = NewGRPCService(ctx, socketName.endpoint, tt.callTimeout)
				if err != nil {
					t.Errorf("failed to create envelope service, error: %v", err)
					return
				}
				defer destroyService(service)
				kubeAPIServerWG.Done()
				// 保持 kube-apiserver 存活以处理请求。
				testCompletedWG.Wait()
			}()

			kmsPluginWG.Add(1)
			go func() {
				// 模拟 kms-plugin 延迟启动：如用例要求，apiserver 会先于插件就绪。
				time.Sleep(tt.pluginDelay)

				_ = mock.NewBase64Plugin(t, socketName.path)

				kmsPluginWG.Done()
				// 保持插件存活以处理请求。
				testCompletedWG.Wait()
			}()

			kubeAPIServerWG.Wait()
			if t.Failed() {
				return
			}
			_, err = service.Encrypt(data)

			if err == nil && tt.wantErr != "" {
				t.Fatalf("got nil, want %s", tt.wantErr)
			}

			if err != nil && tt.wantErr == "" {
				t.Fatalf("got %q, want nil", err.Error())
			}

			// 收集 kms-plugin，允许其完成清理。
			kmsPluginWG.Wait()
		})
	}
}

// TestIntermittentConnectionLoss 模拟与 kms-plugin 的连接间歇性中断：
// 插件停止后发起 Encrypt（调用会阻塞等待重连），随后重启插件，请求应最终成功。
func TestIntermittentConnectionLoss(t *testing.T) {
	t.Parallel()
	var (
		wg1        sync.WaitGroup
		wg2        sync.WaitGroup
		timeout    = 30 * time.Second
		blackOut   = 1 * time.Second
		data       = []byte("test data")
		endpoint   = newEndpoint()
		encryptErr error
	)
	// 启动 KMS Plugin
	f := mock.NewBase64Plugin(t, endpoint.path)

	ctx := testContext(t)

	// 连接到 kms plugin
	service, err := NewGRPCService(ctx, endpoint.endpoint, timeout)
	if err != nil {
		t.Fatalf("failed to create envelope service, error: %v", err)
	}
	defer destroyService(service)

	_, err = service.Encrypt(data)
	if err != nil {
		t.Fatalf("failed when execute encrypt, error: %v", err)
	}
	t.Log("Connected to KMSPlugin")
	f.CleanUp()

	// 停止 KMS Plugin，模拟连接中断
	t.Log("KMS Plugin is stopping")
	time.Sleep(2 * time.Second)

	wg1.Add(1)
	wg2.Add(1)
	go func() {
		defer wg2.Done()
		// 调用 service 加密数据。
		t.Log("Sending encrypt request")
		wg1.Done()
		_, err := service.Encrypt(data)
		if err != nil {
			encryptErr = fmt.Errorf("failed when executing encrypt, error: %v", err)
		}
	}()

	wg1.Wait()
	// 在请求挂起期间让连接中断持续一段时间，再恢复插件。
	time.Sleep(blackOut)
	// 重启 KMS Plugin
	_ = mock.NewBase64Plugin(t, endpoint.path)
	t.Log("Restarted KMS Plugin")

	wg2.Wait()

	if encryptErr != nil {
		t.Error(encryptErr)
	}
}

// TestUnsupportedVersion 验证插件返回不支持的 KMS API 版本时，
// Encrypt 与 Decrypt 都返回版本不匹配错误。
func TestUnsupportedVersion(t *testing.T) {
	t.Parallel()
	ver := "invalid"
	data := []byte("test data")
	wantErr := fmt.Errorf(versionErrorf, ver, kmsapiVersion)
	endpoint := newEndpoint()

	f := mock.NewBase64Plugin(t, endpoint.path)
	f.SetVersion(ver)

	ctx := testContext(t)

	s, err := NewGRPCService(ctx, endpoint.endpoint, 1*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer destroyService(s)

	// Encrypt
	_, err = s.Encrypt(data)
	if err == nil || err.Error() != wantErr.Error() {
		t.Errorf("got err: %ver, want: %ver", err, wantErr)
	}

	destroyService(s)

	s, err = NewGRPCService(ctx, endpoint.endpoint, 1*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer destroyService(s)

	// Decrypt
	_, err = s.Decrypt(data)
	if err == nil || err.Error() != wantErr.Error() {
		t.Errorf("got err: %ver, want: %ver", err, wantErr)
	}
}

// TestGRPCService 验证常规的加密再解密往返。
func TestGRPCService(t *testing.T) {
	t.Parallel()
	// 启动测试用 gRPC server。
	endpoint := newEndpoint()
	_ = mock.NewBase64Plugin(t, endpoint.path)

	ctx := testContext(t)

	// 创建 gRPC 客户端服务。
	service, err := NewGRPCService(ctx, endpoint.endpoint, 1*time.Second)
	if err != nil {
		t.Fatalf("failed to create envelope service, error: %v", err)
	}
	defer destroyService(service)

	// 调用 service 加密数据。
	data := []byte("test data")
	cipher, err := service.Encrypt(data)
	if err != nil {
		t.Fatalf("failed when execute encrypt, error: %v", err)
	}

	// 调用 service 解密数据。
	result, err := service.Decrypt(cipher)
	if err != nil {
		t.Fatalf("failed when execute decrypt, error: %v", err)
	}

	if !reflect.DeepEqual(data, result) {
		t.Errorf("expect: %v, but: %v", data, result)
	}
}

// TestGRPCServiceConcurrentAccess 验证多个 goroutine 并发加解密时的正确性，
// 用于发现客户端连接复用上的竞态。
func TestGRPCServiceConcurrentAccess(t *testing.T) {
	t.Parallel()
	// 启动测试用 gRPC server。
	endpoint := newEndpoint()
	_ = mock.NewBase64Plugin(t, endpoint.path)

	ctx := testContext(t)

	// 创建 gRPC 客户端服务。
	service, err := NewGRPCService(ctx, endpoint.endpoint, 15*time.Second)
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
			cipher, err := service.Encrypt(data)
			if err != nil {
				t.Errorf("failed when execute encrypt, error: %v", err)
			}

			// 调用 service 解密数据。
			result, err := service.Decrypt(cipher)
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

// destroyService 关闭底层 gRPC 连接；service 为 nil 时安全返回。
func destroyService(service Service) {
	if service != nil {
		s := service.(*gRPCService)
		s.connection.Close()
	}
}

// TestInvalidConfiguration 验证 KMS provider 的非法配置：
// 空 endpoint 与非 unix scheme 都应在构造阶段失败。
func TestInvalidConfiguration(t *testing.T) {
	t.Parallel()
	// 启动测试用 gRPC server。
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
			_, err := NewGRPCService(ctx, testCase.endpoint, 1*time.Second)
			if err == nil {
				t.Fatalf("should fail to create envelope service for %s.", testCase.name)
			}
		})
	}
}

// testContext 返回带测试清理的可取消 context。
func testContext(t *testing.T) context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return ctx
}
