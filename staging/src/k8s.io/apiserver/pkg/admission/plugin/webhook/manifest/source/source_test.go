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

package source

import (
	"testing"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// mockLoadFunc 是 manifest 加载器的测试替身：它记录被调用的次数，
// 以便测试断言重载确实被触发，并返回预置的配置/哈希/错误。
type mockLoadFunc struct {
	configs   []*admissionregistrationv1.ValidatingWebhookConfiguration
	hash      string
	err       error
	callCount int
}

func (m *mockLoadFunc) load(_ string) ([]*admissionregistrationv1.ValidatingWebhookConfiguration, string, error) {
	m.callCount++
	return m.configs, m.hash, m.err
}

// validVWC 构造一个最小的合法 ValidatingWebhookConfiguration。webhook
// 必须带有 URL 形式的 client config、一个 AdmissionReview 版本，以及显式的
// side-effect 类别，才能通过校验。
func validVWC(name string) *admissionregistrationv1.ValidatingWebhookConfiguration {
	return &admissionregistrationv1.ValidatingWebhookConfiguration{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Webhooks: []admissionregistrationv1.ValidatingWebhook{{
			Name: "test.webhook.io",
			ClientConfig: admissionregistrationv1.WebhookClientConfig{
				URL: new("https://example.com"),
			},
			AdmissionReviewVersions: []string{"v1"},
			SideEffects: func() *admissionregistrationv1.SideEffectClass {
				s := admissionregistrationv1.SideEffectClassNone
				return &s
			}(),
		}},
	}
}

// invalidSelectorVWC 是一个合法配置，但其 namespace selector 使用了
// 无法识别的操作符，校验必须拒绝它。
func invalidSelectorVWC(name string) *admissionregistrationv1.ValidatingWebhookConfiguration {
	vwc := validVWC(name)
	vwc.Webhooks[0].NamespaceSelector = &metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{{
			Key:      "key",
			Operator: "InvalidOperator",
		}},
	}
	return vwc
}

// TestValidatingSource_ValidateBeforeStore 验证 LoadInitial 会拒绝
// 带非法 selector 的配置且不予存储（Bug #1）。
//
// 关键不变量：校验必须在写入缓存之前完成，否则一次坏配置会污染后续所有请求。
func TestValidatingSource_ValidateBeforeStore(t *testing.T) {
	mock := &mockLoadFunc{
		configs: []*admissionregistrationv1.ValidatingWebhookConfiguration{invalidSelectorVWC("bad")},
		hash:    "h1",
	}

	src := NewValidatingSource("/tmp/test", "test-server", mock.load)

	// 初始加载必须失败，且失败后不得留下任何半成品状态。
	if err := src.LoadInitial(); err == nil {
		t.Fatal("LoadInitial should have returned an error for invalid selector")
	}

	if webhooks := src.Webhooks(); webhooks != nil {
		t.Errorf("Webhooks() = %v, want nil (bad config should not be stored)", webhooks)
	}
}

// TestValidatingSource_ReloadKeepsPreviousOnValidationFailure 验证当新配置
// 校验失败时，checkAndReload 会保留之前的合法配置
// （Bug #1 在重载路径上）。
func TestValidatingSource_ReloadKeepsPreviousOnValidationFailure(t *testing.T) {
	mock := &mockLoadFunc{
		configs: []*admissionregistrationv1.ValidatingWebhookConfiguration{validVWC("good")},
		hash:    "h1",
	}

	src := NewValidatingSource("/tmp/test", "test-server", mock.load)

	// 先加载一份有效配置作为基线。
	if err := src.LoadInitial(); err != nil {
		t.Fatalf("LoadInitial failed: %v", err)
	}

	initialWebhooks := src.Webhooks()
	if len(initialWebhooks) != 1 {
		t.Fatalf("expected 1 webhook after LoadInitial, got %d", len(initialWebhooks))
	}

	// 切换为非法配置以触发重载
	mock.configs = []*admissionregistrationv1.ValidatingWebhookConfiguration{invalidSelectorVWC("bad")}
	mock.hash = "h2"

	// 新配置非法时，reload 必须整体失败并保留旧配置，而不是清空。
	src.checkAndReload()

	reloadedWebhooks := src.Webhooks()
	if len(reloadedWebhooks) != 1 {
		t.Fatalf("expected 1 webhook after failed reload, got %d", len(reloadedWebhooks))
	}
	// 应当仍是原来的 accessor
	if &initialWebhooks[0] != &reloadedWebhooks[0] {
		t.Error("Webhooks() returned different slice after failed reload; previous config should be preserved")
	}
}

// TestValidatingSource_CachedAccessorSlice 验证 Webhooks() 在重复调用时
// 返回同一个切片（Bug #2 —— 使 composite source 的缓存成为可能）。
//
// 复用同一底层数组可避免上层组合源每次调用都重新分配，从而让缓存命中稳定。
func TestValidatingSource_CachedAccessorSlice(t *testing.T) {
	mock := &mockLoadFunc{
		configs: []*admissionregistrationv1.ValidatingWebhookConfiguration{validVWC("cfg")},
		hash:    "h1",
	}

	src := NewValidatingSource("/tmp/test", "test-server", mock.load)

	if err := src.LoadInitial(); err != nil {
		t.Fatalf("LoadInitial failed: %v", err)
	}

	result1 := src.Webhooks()
	result2 := src.Webhooks()

	if len(result1) == 0 || len(result2) == 0 {
		t.Fatal("expected non-empty webhook slices")
	}

	if &result1[0] != &result2[0] {
		t.Error("Webhooks() returned different slices; expected pointer-equal backing array for caching")
	}
}

// TestValidatingSource_PreviousHashSkipsReload 验证当 checkAndReload
// 被调用且哈希未变化时，source 保留原有的 accessors
// （哈希比较由 source 自己负责）。
func TestValidatingSource_PreviousHashSkipsReload(t *testing.T) {
	mock := &mockLoadFunc{
		configs: []*admissionregistrationv1.ValidatingWebhookConfiguration{validVWC("cfg")},
		hash:    "h1",
	}

	src := NewValidatingSource("/tmp/test", "test-server", mock.load)

	if err := src.LoadInitial(); err != nil {
		t.Fatalf("LoadInitial failed: %v", err)
	}
	if mock.callCount != 1 {
		t.Fatalf("expected 1 call after LoadInitial, got %d", mock.callCount)
	}

	initialWebhooks := src.Webhooks()

	// loadFunc 返回相同的哈希 —— source 应检测到无变化
	// 注意：loadFunc 仍会被调用（哈希由 loadFunc 计算），但结果不应替换缓存。
	src.checkAndReload()

	if mock.callCount != 2 {
		t.Fatalf("expected 2 calls after checkAndReload, got %d", mock.callCount)
	}

	afterReload := src.Webhooks()
	if len(afterReload) != len(initialWebhooks) {
		t.Fatalf("expected %d webhooks after no-op reload, got %d", len(initialWebhooks), len(afterReload))
	}
	if &afterReload[0] != &initialWebhooks[0] {
		t.Error("Webhooks() returned different slice after no-op reload; original should be preserved")
	}
}

// TestValidatingSource_FilesDeletionClearsConfigs 验证当 loadFunc
// 返回空配置且哈希变化时，存储的配置会被更新为空。
//
// 删除清单文件是合法的配置变更，必须真正清空，而不是被当作校验失败保留旧值。
func TestValidatingSource_FilesDeletionClearsConfigs(t *testing.T) {
	mock := &mockLoadFunc{
		configs: []*admissionregistrationv1.ValidatingWebhookConfiguration{validVWC("cfg")},
		hash:    "h1",
	}

	src := NewValidatingSource("/tmp/test", "test-server", mock.load)

	if err := src.LoadInitial(); err != nil {
		t.Fatalf("LoadInitial failed: %v", err)
	}
	if len(src.Webhooks()) != 1 {
		t.Fatalf("expected 1 webhook after LoadInitial, got %d", len(src.Webhooks()))
	}

	// 模拟文件被删除：空配置、新哈希
	mock.configs = []*admissionregistrationv1.ValidatingWebhookConfiguration{}
	mock.hash = "h2"

	// 哈希变化 → 源必须接受新的空配置集。
	src.checkAndReload()

	webhooks := src.Webhooks()
	if len(webhooks) != 0 {
		t.Errorf("expected 0 webhooks after files deletion, got %d", len(webhooks))
	}
}
