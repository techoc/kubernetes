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

package controller

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/apiserver/pkg/server/options/encryptionconfig"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/util/workqueue"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
)

// TestController 验证动态加密配置控制器单次 worker 迭代的决策：
// 只有在文件哈希变化、配置能成功加载、且所有健康检查通过时才替换
// 现有转换器；任何失败都必须保留旧配置、关闭新加载的转换器、计入
// failure 指标，并按速率限制重新入队。
//
// 测试把 minKMSPluginCloseGracePeriod 调小以缩短关闭等待，并用 mock
// 队列精确控制"只执行一轮"。
func TestController(t *testing.T) {
	origMinKMSPluginCloseGracePeriod := minKMSPluginCloseGracePeriod
	t.Cleanup(func() { minKMSPluginCloseGracePeriod = origMinKMSPluginCloseGracePeriod })
	minKMSPluginCloseGracePeriod = 300 * time.Millisecond

	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.KMSv1, true)

	const expectedSuccessMetricValue = `
# HELP apiserver_encryption_config_controller_automatic_reloads_total [ALPHA] Total number of reload successes and failures of encryption configuration split by apiserver identity.
# TYPE apiserver_encryption_config_controller_automatic_reloads_total counter
apiserver_encryption_config_controller_automatic_reloads_total{apiserver_id_hash="sha256:cd8a60cec6134082e9f37e7a4146b4bc14a0bf8a863237c36ec8fdb658c3e027",status="success"} 1
# HELP apiserver_encryption_config_controller_last_config_info [ALPHA] Information about the last applied encryption configuration with hash as label, split by apiserver identity.
# TYPE apiserver_encryption_config_controller_last_config_info gauge
apiserver_encryption_config_controller_last_config_info{apiserver_id_hash="sha256:cd8a60cec6134082e9f37e7a4146b4bc14a0bf8a863237c36ec8fdb658c3e027",hash="sha256:6f6af143a5aec5c4056d759f2bdf8b6ffe218a2bf3846c4fd42b6baef037c5ef"} 1
`
	const expectedFailureMetricValue = `
# HELP apiserver_encryption_config_controller_automatic_reloads_total [ALPHA] Total number of reload successes and failures of encryption configuration split by apiserver identity.
# TYPE apiserver_encryption_config_controller_automatic_reloads_total counter
apiserver_encryption_config_controller_automatic_reloads_total{apiserver_id_hash="sha256:cd8a60cec6134082e9f37e7a4146b4bc14a0bf8a863237c36ec8fdb658c3e027",status="failure"} 1
`

	tests := []struct {
		name                        string
		wantECFileHash              string
		wantTransformerClosed       bool
		wantLoadCalls               int
		wantHashCalls               int
		wantAddRateLimitedCount     uint64
		wantMetrics                 string
		mockLoadEncryptionConfig    func(ctx context.Context, filepath string, reload bool, apiServerID string) (*encryptionconfig.EncryptionConfiguration, error)
		mockGetEncryptionConfigHash func(ctx context.Context, filepath string) (string, error)
	}{
		// 加载失败：旧配置保留、新转换器关闭、重新入队。
		{
			name:                    "when invalid config is provided previous config shouldn't be changed",
			wantECFileHash:          "sha256:6bc9f4aa2e5587afbb96074e1809550cbc4de3cc3a35717dac8ff2800a147fd3",
			wantLoadCalls:           1,
			wantHashCalls:           1,
			wantTransformerClosed:   true,
			wantMetrics:             expectedFailureMetricValue,
			wantAddRateLimitedCount: 1,
			mockGetEncryptionConfigHash: func(ctx context.Context, filepath string) (string, error) {
				return "always changes and never errors", nil
			},
			mockLoadEncryptionConfig: func(ctx context.Context, filepath string, reload bool, apiServerID string) (*encryptionconfig.EncryptionConfiguration, error) {
				return nil, fmt.Errorf("empty config file")
			},
		},
		// 成功路径：哈希更新、success 指标 +1、无需重试。
		{
			name:                    "when new valid config is provided it should be updated",
			wantECFileHash:          "sha256:6f6af143a5aec5c4056d759f2bdf8b6ffe218a2bf3846c4fd42b6baef037c5ef", // some new config hash
			wantLoadCalls:           1,
			wantHashCalls:           1,
			wantMetrics:             expectedSuccessMetricValue,
			wantAddRateLimitedCount: 0,
			mockGetEncryptionConfigHash: func(ctx context.Context, filepath string) (string, error) {
				return "always changes and never errors", nil
			},
			mockLoadEncryptionConfig: func(ctx context.Context, filepath string, reload bool, apiServerID string) (*encryptionconfig.EncryptionConfiguration, error) {
				return &encryptionconfig.EncryptionConfiguration{
					HealthChecks: []healthz.HealthChecker{
						&mockHealthChecker{
							pluginName: "valid-plugin",
							err:        nil,
						},
					},
					EncryptionFileContentHash: "sha256:6f6af143a5aec5c4056d759f2bdf8b6ffe218a2bf3846c4fd42b6baef037c5ef", // some new config hash
				}, nil
			},
		},
		// 内容哈希与已加载的一致，视为无变化：不产生指标、不重试。
		{
			name:                    "when same valid config is provided previous config shouldn't be changed",
			wantECFileHash:          "sha256:6bc9f4aa2e5587afbb96074e1809550cbc4de3cc3a35717dac8ff2800a147fd3",
			wantLoadCalls:           1,
			wantHashCalls:           1,
			wantTransformerClosed:   true,
			wantMetrics:             "",
			wantAddRateLimitedCount: 0,
			mockGetEncryptionConfigHash: func(ctx context.Context, filepath string) (string, error) {
				return "always changes and never errors", nil
			},
			mockLoadEncryptionConfig: func(ctx context.Context, filepath string, reload bool, apiServerID string) (*encryptionconfig.EncryptionConfiguration, error) {
				return &encryptionconfig.EncryptionConfiguration{
					HealthChecks: []healthz.HealthChecker{
						&mockHealthChecker{
							pluginName: "valid-plugin",
							err:        nil,
						},
					},
					// hash of initial "testdata/ec_config.yaml" config file before reloading
					EncryptionFileContentHash: "sha256:6bc9f4aa2e5587afbb96074e1809550cbc4de3cc3a35717dac8ff2800a147fd3",
				}, nil
			},
		},
		// 健康检查失败同样不能切换，否则可能启用一个不可用的 KMS。
		{
			name:                    "when transformer's health check fails previous config shouldn't be changed",
			wantECFileHash:          "sha256:6bc9f4aa2e5587afbb96074e1809550cbc4de3cc3a35717dac8ff2800a147fd3",
			wantLoadCalls:           1,
			wantHashCalls:           1,
			wantTransformerClosed:   true,
			wantMetrics:             expectedFailureMetricValue,
			wantAddRateLimitedCount: 1,
			mockGetEncryptionConfigHash: func(ctx context.Context, filepath string) (string, error) {
				return "always changes and never errors", nil
			},
			mockLoadEncryptionConfig: func(ctx context.Context, filepath string, reload bool, apiServerID string) (*encryptionconfig.EncryptionConfiguration, error) {
				return &encryptionconfig.EncryptionConfiguration{
					HealthChecks: []healthz.HealthChecker{
						&mockHealthChecker{
							pluginName: "invalid-plugin",
							err:        fmt.Errorf("mockingly failing"),
						},
					},
					KMSCloseGracePeriod:       0, // use minKMSPluginCloseGracePeriod
					EncryptionFileContentHash: "anything different",
				}, nil
			},
		},
		// 当前实现要求恰好一个健康检查；多个检查视为非法配置。
		{
			name:                    "when multiple health checks are present previous config shouldn't be changed",
			wantECFileHash:          "sha256:6bc9f4aa2e5587afbb96074e1809550cbc4de3cc3a35717dac8ff2800a147fd3",
			wantLoadCalls:           1,
			wantHashCalls:           1,
			wantTransformerClosed:   true,
			wantMetrics:             expectedFailureMetricValue,
			wantAddRateLimitedCount: 1,
			mockGetEncryptionConfigHash: func(ctx context.Context, filepath string) (string, error) {
				return "always changes and never errors", nil
			},
			mockLoadEncryptionConfig: func(ctx context.Context, filepath string, reload bool, apiServerID string) (*encryptionconfig.EncryptionConfiguration, error) {
				return &encryptionconfig.EncryptionConfiguration{
					HealthChecks: []healthz.HealthChecker{
						&mockHealthChecker{
							pluginName: "valid-plugin",
							err:        nil,
						},
						&mockHealthChecker{
							pluginName: "another-valid-plugin",
							err:        nil,
						},
					},
					EncryptionFileContentHash: "anything different",
				}, nil
			},
		},
		// 健康检查名称会进入 /healthz 路径，含换行等非法字符必须拒绝。
		{
			name:                    "when invalid health check URL is provided previous config shouldn't be changed",
			wantECFileHash:          "sha256:6bc9f4aa2e5587afbb96074e1809550cbc4de3cc3a35717dac8ff2800a147fd3",
			wantLoadCalls:           1,
			wantHashCalls:           1,
			wantTransformerClosed:   true,
			wantMetrics:             expectedFailureMetricValue,
			wantAddRateLimitedCount: 1,
			mockGetEncryptionConfigHash: func(ctx context.Context, filepath string) (string, error) {
				return "always changes and never errors", nil
			},
			mockLoadEncryptionConfig: func(ctx context.Context, filepath string, reload bool, apiServerID string) (*encryptionconfig.EncryptionConfiguration, error) {
				return &encryptionconfig.EncryptionConfiguration{
					HealthChecks: []healthz.HealthChecker{
						&mockHealthChecker{
							pluginName: "invalid\nname",
							err:        nil,
						},
					},
					EncryptionFileContentHash: "anything different",
				}, nil
			},
		},
		// 与"same valid config"重复，强调即使不切换也必须关闭新转换器，
		// 防止 KMS 连接泄漏。
		{
			name:                    "when config is not updated transformers are closed correctly",
			wantECFileHash:          "sha256:6bc9f4aa2e5587afbb96074e1809550cbc4de3cc3a35717dac8ff2800a147fd3",
			wantLoadCalls:           1,
			wantHashCalls:           1,
			wantTransformerClosed:   true,
			wantMetrics:             "",
			wantAddRateLimitedCount: 0,
			mockGetEncryptionConfigHash: func(ctx context.Context, filepath string) (string, error) {
				return "always changes and never errors", nil
			},
			mockLoadEncryptionConfig: func(ctx context.Context, filepath string, reload bool, apiServerID string) (*encryptionconfig.EncryptionConfiguration, error) {
				return &encryptionconfig.EncryptionConfiguration{
					HealthChecks: []healthz.HealthChecker{
						&mockHealthChecker{
							pluginName: "valid-plugin",
							err:        nil,
						},
					},
					// hash of initial "testdata/ec_config.yaml" config file before reloading
					EncryptionFileContentHash: "sha256:6bc9f4aa2e5587afbb96074e1809550cbc4de3cc3a35717dac8ff2800a147fd3",
				}, nil
			},
		},
		// 文件哈希未变时不应重新加载配置（loadCalls=0），但仍会关闭
		// 预先构造的转换器。
		{
			name:                    "when config hash is not updated transformers are closed correctly",
			wantECFileHash:          "sha256:6bc9f4aa2e5587afbb96074e1809550cbc4de3cc3a35717dac8ff2800a147fd3",
			wantLoadCalls:           0,
			wantHashCalls:           1,
			wantTransformerClosed:   true,
			wantMetrics:             "",
			wantAddRateLimitedCount: 0,
			mockGetEncryptionConfigHash: func(ctx context.Context, filepath string) (string, error) {
				// hash of initial "testdata/ec_config.yaml" config file before reloading
				return "sha256:6bc9f4aa2e5587afbb96074e1809550cbc4de3cc3a35717dac8ff2800a147fd3", nil
			},
			mockLoadEncryptionConfig: func(ctx context.Context, filepath string, reload bool, apiServerID string) (*encryptionconfig.EncryptionConfiguration, error) {
				return nil, fmt.Errorf("should not be called")
			},
		},
		// 读取哈希失败视为一次失败重载，需要重试；配置加载不应被调用。
		{
			name:                    "when config hash errors transformers are closed correctly",
			wantECFileHash:          "sha256:6bc9f4aa2e5587afbb96074e1809550cbc4de3cc3a35717dac8ff2800a147fd3",
			wantLoadCalls:           0,
			wantHashCalls:           1,
			wantTransformerClosed:   true,
			wantMetrics:             expectedFailureMetricValue,
			wantAddRateLimitedCount: 1,
			mockGetEncryptionConfigHash: func(ctx context.Context, filepath string) (string, error) {
				return "", fmt.Errorf("some io error")
			},
			mockLoadEncryptionConfig: func(ctx context.Context, filepath string, reload bool, apiServerID string) (*encryptionconfig.EncryptionConfiguration, error) {
				return nil, fmt.Errorf("should not be called")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctxServer, closeServer := context.WithCancel(context.Background())
			ctxTransformers, closeTransformers := context.WithCancel(ctxServer)
			t.Cleanup(closeServer)
			t.Cleanup(closeTransformers)

			legacyregistry.Reset()

			// load initial encryption config
			encryptionConfiguration, err := encryptionconfig.LoadEncryptionConfig(
				ctxTransformers,
				"testdata/ec_config.yaml",
				true,
				"test-apiserver",
			)
			if err != nil {
				t.Fatalf("failed to load encryption config: %v", err)
			}

			d := NewDynamicEncryptionConfiguration(
				"test-controller",
				"does not matter",
				encryptionconfig.NewDynamicTransformers(
					encryptionConfiguration.Transformers,
					encryptionConfiguration.HealthChecks[0],
					closeTransformers,
					0, // set grace period to 0 so that the time.Sleep in DynamicTransformers.Set finishes quickly
				),
				encryptionConfiguration.EncryptionFileContentHash,
				"test-apiserver",
			)
			d.queue.ShutDown() // we do not use the real queue during tests

			// mock 队列让 Run 只处理一项后即返回，避免测试依赖真实定时器。
			queue := &mockWorkQueue{
				addCalled: make(chan struct{}),
				cancel:    closeServer,
			}
			d.queue = queue

			var hashCalls, loadCalls int
			d.loadEncryptionConfig = func(ctx context.Context, filepath string, reload bool, apiServerID string) (*encryptionconfig.EncryptionConfiguration, error) {
				loadCalls++
				queue.ctx = ctx
				return test.mockLoadEncryptionConfig(ctx, filepath, reload, apiServerID)
			}
			d.getEncryptionConfigHash = func(ctx context.Context, filepath string) (string, error) {
				hashCalls++
				queue.ctx = ctx
				return test.mockGetEncryptionConfigHash(ctx, filepath)
			}

			d.Run(ctxServer) // this should block and run exactly one iteration of the worker loop

			if test.wantECFileHash != d.lastLoadedEncryptionConfigHash {
				t.Errorf("expected encryption config hash %q but got %q", test.wantECFileHash, d.lastLoadedEncryptionConfigHash)
			}

			if test.wantLoadCalls != loadCalls {
				t.Errorf("load calls does not match: want=%v, got=%v", test.wantLoadCalls, loadCalls)
			}

			if test.wantHashCalls != hashCalls {
				t.Errorf("hash calls does not match: want=%v, got=%v", test.wantHashCalls, hashCalls)
			}

			if test.wantTransformerClosed != queue.wasCanceled {
				t.Errorf("transformer closed does not match: want=%v, got=%v", test.wantTransformerClosed, queue.wasCanceled)
			}

			if test.wantAddRateLimitedCount != queue.addRateLimitedCount.Load() {
				t.Errorf("queue addRateLimitedCount does not match: want=%v, got=%v", test.wantAddRateLimitedCount, queue.addRateLimitedCount.Load())
			}

			if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(test.wantMetrics),
				"apiserver_encryption_config_controller_automatic_reloads_total", "apiserver_encryption_config_controller_automatic_reload_last_config_info",
			); err != nil {
				t.Errorf("failed to validate metrics: %v", err)
			}
		})
	}
}

// mockWorkQueue 通过嵌入接口保证未实现的方法会 panic，从而暴露控制器
// 对队列 API 的意外使用；只覆写测试需要的方法。
type mockWorkQueue struct {
	workqueue.TypedRateLimitingInterface[string] // will panic if any unexpected method is called

	closeOnce sync.Once
	addCalled chan struct{}

	count       atomic.Uint64
	ctx         context.Context
	wasCanceled bool
	cancel      func()

	addRateLimitedCount atomic.Uint64
}

// Done 记录 worker 完成一次处理，并检查此时上下文是否已被取消（用于
// 断言转换器是否按预期关闭），随后主动结束 Run。
func (m *mockWorkQueue) Done(item string) {
	m.count.Add(1)
	m.wasCanceled = m.ctx.Err() != nil
	m.cancel()
}

// Get 在首次 Add 之前阻塞；第一次返回待处理项，第二次报告 shutdown，
// 使 Run 恰好执行一轮；第三次调用属于实现错误。
func (m *mockWorkQueue) Get() (item string, shutdown bool) {
	<-m.addCalled

	switch m.count.Load() {
	case 0:
		return "", false
	case 1:
		return "", true
	default:
		panic("too many calls to Get")
	}
}

// Add 只用于释放 Get 的阻塞，之后不再接受新项。
func (m *mockWorkQueue) Add(item string) {
	m.closeOnce.Do(func() {
		close(m.addCalled)
	})
}

// ShutDown 与 AddRateLimited 记录重试次数；其余行为由测试控制。
func (m *mockWorkQueue) ShutDown()                  {}
func (m *mockWorkQueue) AddRateLimited(item string) { m.addRateLimitedCount.Add(1) }

// mockHealthChecker 返回预设结果，用于模拟 KMS 插件健康状态。
type mockHealthChecker struct {
	pluginName string
	err        error
}

func (m *mockHealthChecker) Check(req *http.Request) error {
	return m.err
}

func (m *mockHealthChecker) Name() string {
	return m.pluginName
}
