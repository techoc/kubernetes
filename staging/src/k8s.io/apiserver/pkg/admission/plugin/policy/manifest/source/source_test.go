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
	"fmt"
	"testing"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/admission/plugin/manifest/metrics"
)

// mockEvaluator 是编译器产物的占位类型；source 只关心其存在与数量，不关心内容。
type mockEvaluator struct{}

// mockPolicyLoadFunc 是可注入结果的策略加载函数，并记录被调用次数，
// 用于断言“哈希未变时不应重复构建”。
type mockPolicyLoadFunc struct {
	policies  []*admissionregistrationv1.ValidatingAdmissionPolicy
	bindings  []*admissionregistrationv1.ValidatingAdmissionPolicyBinding
	hash      string
	err       error
	callCount int
}

// load 返回预置结果并累加调用计数。
func (m *mockPolicyLoadFunc) load(dir string) ([]*admissionregistrationv1.ValidatingAdmissionPolicy, []*admissionregistrationv1.ValidatingAdmissionPolicyBinding, string, error) {
	m.callCount++
	return m.policies, m.bindings, m.hash, m.err
}

func validVAP(name string) *admissionregistrationv1.ValidatingAdmissionPolicy {
	return &admissionregistrationv1.ValidatingAdmissionPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}
}

func validVAPB(name, policyName string) *admissionregistrationv1.ValidatingAdmissionPolicyBinding {
	return &admissionregistrationv1.ValidatingAdmissionPolicyBinding{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       admissionregistrationv1.ValidatingAdmissionPolicyBindingSpec{PolicyName: policyName},
	}
}

// mockCompiler 模拟编译成功；测试不覆盖编译失败路径。
func mockCompiler(p *admissionregistrationv1.ValidatingAdmissionPolicy) (mockEvaluator, error) {
	return mockEvaluator{}, nil
}

// getBindingPolicyName 提取绑定指向的策略名，用于把 policy 与 binding 配对。
func getBindingPolicyName(b *admissionregistrationv1.ValidatingAdmissionPolicyBinding) string {
	return b.Spec.PolicyName
}

// newTestSource 构造使用固定目录与 mock 加载函数的静态策略源。
func newTestSource(loadFunc PolicyLoadFunc[*admissionregistrationv1.ValidatingAdmissionPolicy, *admissionregistrationv1.ValidatingAdmissionPolicyBinding]) *StaticPolicySource[*admissionregistrationv1.ValidatingAdmissionPolicy, *admissionregistrationv1.ValidatingAdmissionPolicyBinding, mockEvaluator] {
	return NewStaticPolicySource[*admissionregistrationv1.ValidatingAdmissionPolicy, *admissionregistrationv1.ValidatingAdmissionPolicyBinding, mockEvaluator](
		"/tmp/test-manifests",
		"test-server",
		mockCompiler,
		loadFunc,
		getBindingPolicyName,
		metrics.VAPManifestType,
	)
}

// TestStaticPolicySource_LoadInitial 验证首次加载：策略与绑定被正确配对，
// 加载后 HasSynced 为 true，且加载函数只被调用一次。
func TestStaticPolicySource_LoadInitial(t *testing.T) {
	mock := &mockPolicyLoadFunc{
		policies: []*admissionregistrationv1.ValidatingAdmissionPolicy{validVAP("policy1")},
		bindings: []*admissionregistrationv1.ValidatingAdmissionPolicyBinding{validVAPB("binding1", "policy1")},
		hash:     "h1",
	}
	src := newTestSource(mock.load)

	if err := src.LoadInitial(); err != nil {
		t.Fatalf("LoadInitial() returned unexpected error: %v", err)
	}
	if !src.HasSynced() {
		t.Error("HasSynced() = false, want true after LoadInitial")
	}

	hooks := src.Hooks()
	if len(hooks) != 1 {
		t.Fatalf("Hooks() returned %d hooks, want 1", len(hooks))
	}
	if hooks[0].Policy.Name != "policy1" {
		t.Errorf("hook policy name = %q, want %q", hooks[0].Policy.Name, "policy1")
	}
	if len(hooks[0].Bindings) != 1 {
		t.Fatalf("hook has %d bindings, want 1", len(hooks[0].Bindings))
	}
	if hooks[0].Bindings[0].Name != "binding1" {
		t.Errorf("hook binding name = %q, want %q", hooks[0].Bindings[0].Name, "binding1")
	}
	if mock.callCount != 1 {
		t.Errorf("loadFunc called %d times, want 1", mock.callCount)
	}
}

// TestStaticPolicySource_HashSkipsReload 验证内容哈希未变时跳过重建：
// 加载函数仍会被调用（需要读取内容计算哈希），但 hooks 切片保持同一实例，
// 避免无谓地重新编译 CEL 表达式。
func TestStaticPolicySource_HashSkipsReload(t *testing.T) {
	mock := &mockPolicyLoadFunc{
		policies: []*admissionregistrationv1.ValidatingAdmissionPolicy{validVAP("policy1")},
		bindings: []*admissionregistrationv1.ValidatingAdmissionPolicyBinding{validVAPB("binding1", "policy1")},
		hash:     "h1",
	}
	src := newTestSource(mock.load)

	if err := src.LoadInitial(); err != nil {
		t.Fatalf("LoadInitial() returned unexpected error: %v", err)
	}
	hooksBefore := src.Hooks()

	// 使用相同哈希调用 checkAndReload 应跳过更新。
	src.checkAndReload()

	if mock.callCount != 2 {
		t.Errorf("loadFunc called %d times, want 2", mock.callCount)
	}
	hooksAfter := src.Hooks()
	if len(hooksAfter) != len(hooksBefore) {
		t.Errorf("Hooks() length changed from %d to %d after no-op reload", len(hooksBefore), len(hooksAfter))
	}
	// 校验指针未变（仍是同一个底层切片）。
	if &hooksAfter[0] != &hooksBefore[0] {
		t.Error("Hooks() returned a different slice after hash-matched reload; expected same slice")
	}
}

// TestStaticPolicySource_FilesDeletionClearsHooks 验证清单文件被删除后 hooks 清空：
// 否则已删除的策略会继续生效，形成“幽灵规则”。
func TestStaticPolicySource_FilesDeletionClearsHooks(t *testing.T) {
	mock := &mockPolicyLoadFunc{
		policies: []*admissionregistrationv1.ValidatingAdmissionPolicy{validVAP("policy1")},
		bindings: []*admissionregistrationv1.ValidatingAdmissionPolicyBinding{validVAPB("binding1", "policy1")},
		hash:     "h1",
	}
	src := newTestSource(mock.load)

	if err := src.LoadInitial(); err != nil {
		t.Fatalf("LoadInitial() returned unexpected error: %v", err)
	}
	if len(src.Hooks()) != 1 {
		t.Fatalf("Hooks() returned %d hooks after initial load, want 1", len(src.Hooks()))
	}

	// 模拟文件被删除：策略/绑定为空且哈希变化。
	mock.policies = nil
	mock.bindings = nil
	mock.hash = "h2"

	src.checkAndReload()

	hooks := src.Hooks()
	if len(hooks) != 0 {
		t.Errorf("Hooks() returned %d hooks after deletion reload, want 0", len(hooks))
	}
}

// TestStaticPolicySource_ReloadKeepsPreviousOnError 验证加载失败时保留上一份可用配置：
// 临时文件错误不应导致策略全部失效，宁可继续使用旧规则。
func TestStaticPolicySource_ReloadKeepsPreviousOnError(t *testing.T) {
	mock := &mockPolicyLoadFunc{
		policies: []*admissionregistrationv1.ValidatingAdmissionPolicy{validVAP("policy1")},
		bindings: []*admissionregistrationv1.ValidatingAdmissionPolicyBinding{validVAPB("binding1", "policy1")},
		hash:     "h1",
	}
	src := newTestSource(mock.load)

	if err := src.LoadInitial(); err != nil {
		t.Fatalf("LoadInitial() returned unexpected error: %v", err)
	}

	// 让 loadFunc 在后续调用中返回错误。
	mock.err = fmt.Errorf("simulated load error")

	src.checkAndReload()

	hooks := src.Hooks()
	if len(hooks) != 1 {
		t.Fatalf("Hooks() returned %d hooks after error reload, want 1", len(hooks))
	}
	if hooks[0].Policy.Name != "policy1" {
		t.Errorf("hook policy name = %q, want %q", hooks[0].Policy.Name, "policy1")
	}
}
