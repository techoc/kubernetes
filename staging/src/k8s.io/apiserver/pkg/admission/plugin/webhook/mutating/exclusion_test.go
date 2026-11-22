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

package mutating

import (
	"context"
	"net/url"
	"testing"

	registrationv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission/initializer"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/testcerts"
	webhooktesting "k8s.io/apiserver/pkg/admission/plugin/webhook/testing"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/component-base/featuregate"
)

// TestExcludeVirtualResources verifies the MutatingAdmissionWebhook plugin implements the
// WantsExcludedAdmissionResources initializer and, when ExcludeAdmissionWebhookVirtualResources is
// enabled, skips dispatch for excluded resources (so a rejecting webhook is never called); when the
// gate is disabled it dispatches as before. The production excluded set is auth/authz virtual
// resources; here a core resource is marked excluded so the mechanism can be exercised with the
// fixtures the webhook test harness supports.
//
// 中文说明：用例核心是"排除生效时拒绝型 webhook 根本不会被调用"——通过同一个
// 必然拒绝的 webhook，在门控开/关两种状态下的结果差异来证明请求被跳过而非被放行。
func TestExcludeVirtualResources(t *testing.T) {
	testServer := webhooktesting.NewTestServer(t)
	testServer.StartTLS()
	defer testServer.Close()
	serverURL, err := url.ParseRequestURI(testServer.URL)
	if err != nil {
		t.Fatal(err)
	}

	stopCh := make(chan struct{})
	defer close(stopCh)

	fail := registrationv1.Fail
	none := registrationv1.SideEffectClassNone
	path := "disallow"
	webhooks := []registrationv1.MutatingWebhook{{
		Name: "test-webhook.k8s.io",
		Rules: []registrationv1.RuleWithOperations{{
			Operations: []registrationv1.OperationType{registrationv1.OperationAll},
			Rule:       registrationv1.Rule{APIGroups: []string{"*"}, APIVersions: []string{"*"}, Resources: []string{"*/*"}},
		}},
		ClientConfig: registrationv1.WebhookClientConfig{
			Service:  &registrationv1.ServiceReference{Name: "webhook-test", Namespace: "default", Path: &path},
			CABundle: testcerts.CACert,
		},
		FailurePolicy: &fail,
		SideEffects:   &none,
		// Empty (non-nil) selectors match everything; nil would parse to "matches nothing".
		// 中文说明：区分 nil 与空结构体是常见陷阱，注释保留以提醒后来者。
		NamespaceSelector:       &metav1.LabelSelector{},
		ObjectSelector:          &metav1.LabelSelector{},
		AdmissionReviewVersions: []string{"v1"},
	}}

	wh, err := NewMutatingWebhook(nil)
	if err != nil {
		t.Fatalf("failed to create mutating webhook: %v", err)
	}

	// WantsExcludedAdmissionResources wiring. NewAttribute issues a request for the core "pod"
	// resource, so mark that excluded.
	// 中文说明：用生产中的可选接口做断言，顺带验证插件确实实现了该接口。
	wants, ok := interface{}(wh).(initializer.WantsExcludedAdmissionResources)
	if !ok {
		t.Fatal("plugin does not implement WantsExcludedAdmissionResources")
	}
	wants.SetExcludedAdmissionResources([]schema.GroupResource{{Group: "", Resource: "pod"}})

	wantsFeatures, ok := interface{}(wh).(initializer.WantsFeatures)
	if !ok {
		t.Fatal("plugin does not implement WantsFeatures")
	}

	client, informer := webhooktesting.NewFakeMutatingDataSource("default", webhooks, stopCh)
	wh.SetAuthenticationInfoResolverWrapper(webhooktesting.Wrapper(webhooktesting.NewAuthenticationInfoResolver(new(int32))))
	wh.SetServiceResolver(webhooktesting.NewServiceResolver(*serverURL))
	wh.SetExternalKubeClientSet(client)
	wh.SetExternalKubeInformerFactory(informer)
	if err := wh.ValidateInitialization(); err != nil {
		t.Fatalf("failed to validate initialization: %v", err)
	}

	informer.Start(stopCh)
	informer.WaitForCacheSync(stopCh)

	attr := webhooktesting.NewAttribute("default", nil, false)
	o := webhooktesting.NewObjectInterfacesForTest()

	// Gate enabled: the excluded resource is skipped, so the rejecting webhook is never called.
	// 中文说明：nil 结果证明请求被跳过；若实现退化为"放行"，本用例无法区分，
	// 因此还需下面的门控关闭分支共同构成证据。
	wantsFeatures.InspectFeatureGates(newExcludeGate(t, true))
	if err := wh.Admit(context.TODO(), attr, o); err != nil {
		t.Errorf("with gate enabled, expected the excluded resource to be skipped (nil), got: %v", err)
	}

	// Gate disabled: the webhook is dispatched and rejects the request.
	// 中文说明：门控关闭后排除列表不再生效，期望拿到错误。
	wantsFeatures.InspectFeatureGates(newExcludeGate(t, false))
	if err := wh.Admit(context.TODO(), attr, o); err == nil {
		t.Error("with gate disabled, expected the webhook to reject the request, got nil")
	}
}

// newExcludeGate 构造只包含被测门控的独立 FeatureGate。
//
// 中文说明：使用私有 gate 而非全局 DefaultFeatureGate，避免测试并行时互相干扰。
func newExcludeGate(t *testing.T, enabled bool) featuregate.FeatureGate {
	t.Helper()
	gate := featuregate.NewFeatureGate()
	if err := gate.Add(map[featuregate.Feature]featuregate.FeatureSpec{
		features.ExcludeAdmissionWebhookVirtualResources: {Default: enabled, PreRelease: featuregate.Beta},
	}); err != nil {
		t.Fatalf("failed to build feature gate: %v", err)
	}
	return gate
}
