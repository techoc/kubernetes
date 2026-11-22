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

package validating

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

// TestExcludeVirtualResources verifies the ValidatingAdmissionWebhook plugin implements the
// WantsExcludedAdmissionResources initializer and, when ExcludeAdmissionWebhookVirtualResources is
// enabled, skips dispatch for excluded resources (so a rejecting webhook is never called); when the
// gate is disabled it dispatches as before. The production excluded set is auth/authz virtual
// resources; here a core resource is marked excluded so the mechanism can be exercised with the
// fixtures the webhook test harness supports.
//
// 测试思路：同一套断言在开关开/关两种配置下各跑一次，只有“是否调用 webhook”这一变量不同。
// 由于 webhook 配置为 fail policy 且总是拒绝，开关生效时 Validate 返回 nil 即证明未分发。
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
	webhooks := []registrationv1.ValidatingWebhook{{
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
		NamespaceSelector:       &metav1.LabelSelector{},
		ObjectSelector:          &metav1.LabelSelector{},
		AdmissionReviewVersions: []string{"v1"},
	}}

	wh, err := NewValidatingAdmissionWebhook(nil)
	if err != nil {
		t.Fatalf("failed to create validating webhook: %v", err)
	}

	// WantsExcludedAdmissionResources wiring. NewAttribute issues a request for the core "pod"
	// resource, so mark that excluded.
	wants, ok := interface{}(wh).(initializer.WantsExcludedAdmissionResources)
	if !ok {
		t.Fatal("plugin does not implement WantsExcludedAdmissionResources")
	}
	wants.SetExcludedAdmissionResources([]schema.GroupResource{{Group: "", Resource: "pod"}})

	wantsFeatures, ok := interface{}(wh).(initializer.WantsFeatures)
	if !ok {
		t.Fatal("plugin does not implement WantsFeatures")
	}

	client, informer := webhooktesting.NewFakeValidatingDataSource("default", webhooks, stopCh)
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
	wantsFeatures.InspectFeatureGates(newExcludeGate(t, true))
	if err := wh.Validate(context.TODO(), attr, o); err != nil {
		t.Errorf("with gate enabled, expected the excluded resource to be skipped (nil), got: %v", err)
	}

	// Gate disabled: the webhook is dispatched and rejects the request.
	wantsFeatures.InspectFeatureGates(newExcludeGate(t, false))
	if err := wh.Validate(context.TODO(), attr, o); err == nil {
		t.Error("with gate disabled, expected the webhook to reject the request, got nil")
	}
}

// newExcludeGate 构造一个只包含目标开关的独立 FeatureGate。
// 用独立实例而非全局 DefaultFeatureGate，避免与其他并行测试相互污染。
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
