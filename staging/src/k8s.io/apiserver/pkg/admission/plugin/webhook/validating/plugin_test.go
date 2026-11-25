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

package validating

import (
	"context"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"k8s.io/apimachinery/pkg/api/errors"
	admissionmetrics "k8s.io/apiserver/pkg/admission/metrics"
	webhooktesting "k8s.io/apiserver/pkg/admission/plugin/webhook/testing"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/component-base/metrics/testutil"
	clocktesting "k8s.io/utils/clock/testing"
)

// BenchmarkValidate 测试 ValidatingWebhook#Validate 是否按预期工作。
// 可通过 WEBHOOK_TEST_SERVER_URL 指向独立 webhook 服务，避免把 webhook 自身开销计入基准。
func BenchmarkValidate(b *testing.B) {
	testServerURL := os.Getenv("WEBHOOK_TEST_SERVER_URL")
	if len(testServerURL) == 0 {
		// 未提供外部服务时退化到进程内 TLS 服务器；此时基准包含 webhook 处理成本，仅供相对比较。
		b.Log("warning, WEBHOOK_TEST_SERVER_URL not set, starting in-process server, benchmarks will include webhook cost.")
		b.Log("to run a standalone server, run:")
		b.Log("go run k8s.io/apiserver/pkg/admission/plugin/webhook/testing/main/main.go")
		testServer := webhooktesting.NewTestServer(b)
		testServer.StartTLS()
		defer testServer.Close()
		testServerURL = testServer.URL
	}

	objectInterfaces := webhooktesting.NewObjectInterfacesForTest()

	serverURL, err := url.ParseRequestURI(testServerURL)
	if err != nil {
		b.Fatalf("this should never happen? %v", err)
	}

	// 所有子基准共享同一个停止信号；defer close 保证 informer 协程退出。
	stopCh := make(chan struct{})
	defer close(stopCh)

	for _, tt := range webhooktesting.NewNonMutatingTestCases(serverURL) {
		// 暂时跳过失败用例或显式跳过基准测试的用例
		// 失败路径涉及错误构造与重试，不能代表正常校验的吞吐。
		if !tt.ExpectAllow || tt.SkipBenchmark {
			continue
		}

		b.Run(tt.Name, func(b *testing.B) {
			wh, err := NewValidatingAdmissionWebhook(nil)
			if err != nil {
				b.Errorf("%s: failed to create validating webhook: %v", tt.Name, err)
				return
			}

			ns := "webhook-test"
			client, informer := webhooktesting.NewFakeValidatingDataSource(ns, tt.Webhooks, stopCh)

			// 注入 fake 依赖，使基准只测量插件逻辑与 HTTP 往返。
			wh.SetAuthenticationInfoResolverWrapper(webhooktesting.Wrapper(webhooktesting.NewAuthenticationInfoResolver(new(int32))))
			wh.SetServiceResolver(webhooktesting.NewServiceResolver(*serverURL))
			wh.SetExternalKubeClientSet(client)
			wh.SetExternalKubeInformerFactory(informer)

			if err = wh.ValidateInitialization(); err != nil {
				b.Errorf("%s: failed to validate initialization: %v", tt.Name, err)
				return
			}

			// 等 informer 缓存同步完成，否则首个请求可能因配置尚未加载而失败。
			informer.Start(stopCh)
			informer.WaitForCacheSync(stopCh)

			attr := webhooktesting.NewAttribute(ns, nil, tt.IsDryRun)

			b.ResetTimer()
			// 并行执行，模拟 apiserver 多个请求处理协程同时调用准入插件。
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					wh.Validate(context.TODO(), attr, objectInterfaces)
				}
			})
		})
	}
}

// TestValidate 测试 ValidatingWebhook#Validate 是否按预期工作。
// 覆盖允许/拒绝、dry-run、错误信息以及 webhook 拒绝指标和审计注解。
func TestValidate(t *testing.T) {
	testServer := webhooktesting.NewTestServer(t)
	testServer.StartTLS()
	defer testServer.Close()

	objectInterfaces := webhooktesting.NewObjectInterfacesForTest()

	serverURL, err := url.ParseRequestURI(testServer.URL)
	if err != nil {
		t.Fatalf("this should never happen? %v", err)
	}

	stopCh := make(chan struct{})
	defer close(stopCh)

	for _, tt := range webhooktesting.NewNonMutatingTestCases(serverURL) {
		wh, err := NewValidatingAdmissionWebhook(nil)
		if err != nil {
			t.Errorf("%s: failed to create validating webhook: %v", tt.Name, err)
			continue
		}

		ns := "webhook-test"
		client, informer := webhooktesting.NewFakeValidatingDataSource(ns, tt.Webhooks, stopCh)

		wh.SetAuthenticationInfoResolverWrapper(webhooktesting.Wrapper(webhooktesting.NewAuthenticationInfoResolver(new(int32))))
		wh.SetServiceResolver(webhooktesting.NewServiceResolver(*serverURL))
		wh.SetExternalKubeClientSet(client)
		wh.SetExternalKubeInformerFactory(informer)

		if err = wh.ValidateInitialization(); err != nil {
			t.Errorf("%s: failed to validate initialization: %v", tt.Name, err)
			continue
		}

		informer.Start(stopCh)
		informer.WaitForCacheSync(stopCh)

		// 指标是全局状态，用例声明了期望指标时先清空，避免前序用例污染。
		if len(tt.ExpectRejectionMetrics) > 0 {
			admissionmetrics.Metrics.WebhookRejectionGathererForTest().Reset()
		}

		attr := webhooktesting.NewAttribute(ns, nil, tt.IsDryRun)
		err = wh.Validate(context.TODO(), attr, objectInterfaces)
		// 约定：err == nil 等价于允许；拒绝、调用失败均表现为非 nil 错误。
		if tt.ExpectAllow != (err == nil) {
			t.Errorf("%s: expected allowed=%v, but got err=%v", tt.Name, tt.ExpectAllow, err)
		}
		// ErrWebhookRejected 对我们而言不算错误
		// webhook 主动拒绝属于正常业务结果，仍需校验错误文案是否透传。
		if tt.ErrorContains != "" {
			if err == nil || !strings.Contains(err.Error(), tt.ErrorContains) {
				t.Errorf("%s: expected an error saying %q, but got %v", tt.Name, tt.ErrorContains, err)
			}
		}
		// 所有失败都必须以 StatusError 形式返回，客户端才能拿到规范的状态码与原因。
		if _, isStatusErr := err.(*errors.StatusError); err != nil && !isStatusErr {
			t.Errorf("%s: expected a StatusError, got %T", tt.Name, err)
		}
		if len(tt.ExpectRejectionMetrics) > 0 {
			expectedMetrics := `
# HELP apiserver_admission_webhook_rejection_count [ALPHA] Admission webhook rejection count, identified by name and broken out for each admission type (validating or admit) and operation. Additional labels specify an error type (calling_webhook_error or apiserver_internal_error if an error occurred; no_error otherwise) and optionally a non-zero rejection code if the webhook rejects the request with an HTTP status code (honored by the apiserver when the code is greater or equal to 400). Codes greater than 600 are truncated to 600, to keep the metrics cardinality bounded.
# TYPE apiserver_admission_webhook_rejection_count counter
` + tt.ExpectRejectionMetrics + "\n"
			if err := testutil.CollectAndCompare(admissionmetrics.Metrics.WebhookRejectionGathererForTest(), strings.NewReader(expectedMetrics), "apiserver_admission_webhook_rejection_count"); err != nil {
				t.Errorf("unexpected collecting result:\n%s", err)
			}
		}
		// webhook 返回的审计注解应被合并进属性，供后续审计阶段记录。
		fakeAttr, ok := attr.(*webhooktesting.FakeAttributes)
		if !ok {
			t.Errorf("Unexpected error, failed to convert attr to webhooktesting.FakeAttributes")
			continue
		}
		if len(tt.ExpectAnnotations) == 0 {
			assert.Empty(t, fakeAttr.GetAnnotations(auditinternal.LevelMetadata), tt.Name+": annotations not set as expected.")
		} else {
			assert.Equal(t, tt.ExpectAnnotations, fakeAttr.GetAnnotations(auditinternal.LevelMetadata), tt.Name+": annotations not set as expected.")
		}
	}
}

// TestValidateCachedClient 测试 ValidatingWebhook#Validate 应当缓存 restClient。
// 同一个 webhook 反复校验时，认证信息解析结果应命中缓存，避免每个请求都访问 kube-apiserver。
func TestValidateCachedClient(t *testing.T) {
	testServer := webhooktesting.NewTestServer(t)
	testServer.StartTLS()
	defer testServer.Close()
	serverURL, err := url.ParseRequestURI(testServer.URL)
	if err != nil {
		t.Fatalf("this should never happen? %v", err)
	}

	objectInterfaces := webhooktesting.NewObjectInterfacesForTest()

	stopCh := make(chan struct{})
	defer close(stopCh)

	wh, err := NewValidatingAdmissionWebhook(nil)
	if err != nil {
		t.Fatalf("Failed to create validating webhook: %v", err)
	}
	wh.SetServiceResolver(webhooktesting.NewServiceResolver(*serverURL))

	for _, tt := range webhooktesting.NewCachedClientTestcases(serverURL) {
		ns := "webhook-test"
		client, informer := webhooktesting.NewFakeValidatingDataSource(ns, tt.Webhooks, stopCh)

		// 覆盖 webhook source。客户端缓存保持不变。
		// 用计数器记录解析器被调用次数：为 0 说明命中了缓存。
		cacheMisses := new(int32)
		wh.SetAuthenticationInfoResolverWrapper(webhooktesting.Wrapper(webhooktesting.NewAuthenticationInfoResolver(cacheMisses)))
		wh.SetExternalKubeClientSet(client)
		wh.SetExternalKubeInformerFactory(informer)

		if err = wh.ValidateInitialization(); err != nil {
			t.Errorf("%s: failed to validate initialization: %v", tt.Name, err)
			continue
		}

		informer.Start(stopCh)
		informer.WaitForCacheSync(stopCh)

		err = wh.Validate(context.TODO(), webhooktesting.NewAttribute(ns, nil, false), objectInterfaces)
		if tt.ExpectAllow != (err == nil) {
			t.Errorf("%s: expected allowed=%v, but got err=%v", tt.Name, tt.ExpectAllow, err)
		}

		// 期望缓存未命中却从未调用解析器，说明配置未生效。
		if tt.ExpectCacheMiss && *cacheMisses == 0 {
			t.Errorf("%s: expected cache miss, but got no AuthenticationInfoResolver call", tt.Name)
		}

		// 期望命中缓存却调用了解析器，说明缓存键或失效逻辑有问题。
		if !tt.ExpectCacheMiss && *cacheMisses > 0 {
			t.Errorf("%s: expected client to be cached, but got %d AuthenticationInfoResolver calls", tt.Name, *cacheMisses)
		}
	}
}

// TestValidateWebhookDuration 测试 ValidatingWebhook#Validate 是否正确地把
// webhook 耗时写入 context。
// 延迟追踪器只在请求上下文已初始化时写入；未初始化时不应凭空创建。
func TestValidateWebhookDuration(ts *testing.T) {
	clk := clocktesting.FakeClock{}
	testServer := webhooktesting.NewTestServerWithHandler(ts, webhooktesting.ClockSteppingWebhookHandler(ts, &clk))
	testServer.StartTLS()
	defer testServer.Close()
	serverURL, err := url.ParseRequestURI(testServer.URL)
	if err != nil {
		ts.Fatalf("this should never happen? %v", err)
	}

	objectInterfaces := webhooktesting.NewObjectInterfacesForTest()

	stopCh := make(chan struct{})
	defer close(stopCh)

	for _, test := range webhooktesting.NewValidationDurationTestCases(serverURL) {
		ts.Run(test.Name, func(t *testing.T) {
			ctx := context.TODO()
			// 只有 InitContext 的用例才注入延迟追踪器，用于区分“未初始化”与“已初始化但为零”。
			if test.InitContext {
				ctx = request.WithLatencyTrackersAndCustomClock(ctx, &clk)
			}
			wh, err := NewValidatingAdmissionWebhook(nil)
			if err != nil {
				t.Errorf("failed to create mutating webhook: %v", err)
				return
			}

			ns := "webhook-test"
			client, informer := webhooktesting.NewFakeValidatingDataSource(ns, test.Webhooks, stopCh)

			wh.SetAuthenticationInfoResolverWrapper(webhooktesting.Wrapper(webhooktesting.NewAuthenticationInfoResolver(new(int32))))
			wh.SetServiceResolver(webhooktesting.NewServiceResolver(*serverURL))
			wh.SetExternalKubeClientSet(client)
			wh.SetExternalKubeInformerFactory(informer)

			if err = wh.ValidateInitialization(); err != nil {
				t.Errorf("failed to validate initialization: %v", err)
				return
			}

			informer.Start(stopCh)
			informer.WaitForCacheSync(stopCh)

			_ = wh.Validate(ctx, webhooktesting.NewAttribute(ns, nil, test.IsDryRun), objectInterfaces)
			wd, ok := request.LatencyTrackersFrom(ctx)
			if !ok {
				// 上下文未注入追踪器时，插件不应擅自创建。
				if test.InitContext {
					t.Errorf("expected webhook duration to be initialized")
				}
				return
			}
			if !test.InitContext {
				t.Errorf("expected webhook duration to not be initialized")
				return
			}
			// 校验型 webhook 只应更新校验耗时，变更型耗时保持为零。
			if wd.MutatingWebhookTracker.GetLatency() != 0 {
				t.Errorf("expected admit duration to be equal to 0 got %q", wd.MutatingWebhookTracker.GetLatency())
			}
			if wd.ValidatingWebhookTracker.GetLatency() < test.ExpectedDurationMax {
				t.Errorf("expected validate duraion to be greater or equal to %q got %q", test.ExpectedDurationMax, wd.ValidatingWebhookTracker.GetLatency())
			}
		})
	}
}

// TestValidatePanicHandling 测试 panic 不应逃出 dispatcher。
// webhook 调用链中的 panic 必须被转换为 StatusError，否则会打垮整个 apiserver 进程。
func TestValidatePanicHandling(t *testing.T) {
	testServer := webhooktesting.NewTestServer(t)
	testServer.StartTLS()
	defer testServer.Close()

	objectInterfaces := webhooktesting.NewObjectInterfacesForTest()

	serverURL, err := url.ParseRequestURI(testServer.URL)
	if err != nil {
		t.Fatalf("this should never happen? %v", err)
	}

	stopCh := make(chan struct{})
	defer close(stopCh)

	for _, tt := range webhooktesting.NewNonMutatingPanicTestCases(serverURL) {
		wh, err := NewValidatingAdmissionWebhook(nil)
		if err != nil {
			t.Errorf("%s: failed to create validating webhook: %v", tt.Name, err)
			continue
		}

		ns := "webhook-test"
		client, informer := webhooktesting.NewFakeValidatingDataSource(ns, tt.Webhooks, stopCh)

		// 故意使用会 panic 的解析器，验证 dispatcher 的 recover 逻辑。
		wh.SetAuthenticationInfoResolverWrapper(webhooktesting.Wrapper(webhooktesting.NewPanickingAuthenticationInfoResolver("Start panicking!"))) // 参见阿拉丁，妙极了
		wh.SetServiceResolver(webhooktesting.NewServiceResolver(*serverURL))
		wh.SetExternalKubeClientSet(client)
		wh.SetExternalKubeInformerFactory(informer)

		if err = wh.ValidateInitialization(); err != nil {
			t.Errorf("%s: failed to validate initialization: %v", tt.Name, err)
			continue
		}

		informer.Start(stopCh)
		informer.WaitForCacheSync(stopCh)

		attr := webhooktesting.NewAttribute(ns, nil, tt.IsDryRun)
		err = wh.Validate(context.TODO(), attr, objectInterfaces)
		if tt.ExpectAllow != (err == nil) {
			t.Errorf("%s: expected allowed=%v, but got err=%v", tt.Name, tt.ExpectAllow, err)
		}
		// ErrWebhookRejected 对我们而言不算错误
		if tt.ErrorContains != "" {
			if err == nil || !strings.Contains(err.Error(), tt.ErrorContains) {
				t.Errorf("%s: expected an error saying %q, but got %v", tt.Name, tt.ErrorContains, err)
			}
		}
		if _, isStatusErr := err.(*errors.StatusError); err != nil && !isStatusErr {
			t.Errorf("%s: expected a StatusError, got %T", tt.Name, err)
		}
		fakeAttr, ok := attr.(*webhooktesting.FakeAttributes)
		if !ok {
			t.Errorf("Unexpected error, failed to convert attr to webhooktesting.FakeAttributes")
			continue
		}
		if len(tt.ExpectAnnotations) == 0 {
			assert.Empty(t, fakeAttr.GetAnnotations(auditinternal.LevelMetadata), tt.Name+": annotations not set as expected.")
		} else {
			assert.Equal(t, tt.ExpectAnnotations, fakeAttr.GetAnnotations(auditinternal.LevelMetadata), tt.Name+": annotations not set as expected.")
		}
	}
}
