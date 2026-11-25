/*
Copyright 2019 The Kubernetes Authors.

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

package admissionwebhook

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/api/admission/v1beta1"
	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	auditv1 "k8s.io/apiserver/pkg/apis/audit/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	utiltesting "k8s.io/client-go/util/testing"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/kubernetes/test/utils"
)

const (
	testReinvocationClientUsername = "webhook-reinvocation-integration-client"
	auditPolicy                    = `
apiVersion: audit.k8s.io/v1
kind: Policy
rules:
  - level: Request
    resources:
      - group: "" # core
        resources: ["pods"]
`
)

// TestWebhookReinvocationPolicyWithWatchCache 确保启用 watch cache 时，admission webhook 的重新调用策略被正确应用。
func TestWebhookReinvocationPolicyWithWatchCache(t *testing.T) {
	testWebhookReinvocationPolicy(t, true)
}

// TestWebhookReinvocationPolicyWithoutWatchCache 确保未启用 watch cache 时，admission webhook 的重新调用策略被正确应用。
func TestWebhookReinvocationPolicyWithoutWatchCache(t *testing.T) {
	testWebhookReinvocationPolicy(t, false)
}

func mutationAnnotationValue(configuration, webhook string, mutated bool) string {
	return fmt.Sprintf(`{"configuration":"%s","webhook":"%s","mutated":%t}`, configuration, webhook, mutated)
}

func patchAnnotationValue(configuration, webhook string, patch string) string {
	return strings.Replace(fmt.Sprintf(`{"configuration": "%s", "webhook": "%s", "patch": %s, "patchType": "JSONPatch"}`, configuration, webhook, patch), " ", "", -1)
}

// testWebhookReinvocationPolicy 确保 admission webhook 的重新调用策略被正确应用。
func testWebhookReinvocationPolicy(t *testing.T, watchCache bool) {
	reinvokeNever := admissionregistrationv1.NeverReinvocationPolicy
	reinvokeIfNeeded := admissionregistrationv1.IfNeededReinvocationPolicy

	type testWebhook struct {
		path           string
		policy         *admissionregistrationv1.ReinvocationPolicyType
		objectSelector *metav1.LabelSelector
	}

	testCases := []struct {
		name                           string
		initialPriorityClass           string
		webhooks                       []testWebhook
		expectLabels                   map[string]string
		expectInvocations              map[string]int
		expectError                    bool
		errorContains                  string
		expectAuditMutationAnnotations map[string]string
		expectAuditPatchAnnotations    map[string]string
	}{
		{ // in-tree（变更）、webhook（无变更），无需重新调用
			name:                 "no reinvocation for in-tree only mutation",
			initialPriorityClass: "low-priority", // 触发初始的 in-tree 变更
			webhooks: []testWebhook{
				{path: "/noop", policy: &reinvokeIfNeeded},
			},
			expectInvocations: map[string]int{"/noop": 1},
			expectAuditMutationAnnotations: map[string]string{
				"mutation.webhook.admission.k8s.io/round_0_index_0": mutationAnnotationValue("admission.integration.test-0", "admission.integration.test.0.noop", false),
			},
		},
		{ // in-tree（变更）、webhook（变更）、重新调用 in-tree（无变更），无需重新调用 webhook
			name:                 "no webhook reinvocation for webhook when no in-tree reinvocation mutations",
			initialPriorityClass: "low-priority", // 触发初始的 in-tree 变更
			webhooks: []testWebhook{
				{path: "/addlabel", policy: &reinvokeIfNeeded},
			},
			expectInvocations: map[string]int{"/addlabel": 1},
			expectAuditPatchAnnotations: map[string]string{
				"patch.webhook.admission.k8s.io/round_0_index_0": patchAnnotationValue("admission.integration.test-1", "admission.integration.test.0.addlabel", `[{"op": "add", "path": "/metadata/labels/a", "value": "true"}]`),
			},
			expectAuditMutationAnnotations: map[string]string{
				"mutation.webhook.admission.k8s.io/round_0_index_0": mutationAnnotationValue("admission.integration.test-1", "admission.integration.test.0.addlabel", true),
			},
		},
		{ // in-tree（变更）、webhook（变更）、重新调用 in-tree（变更）、webhook（无变更），两者都被重新调用
			name:                 "webhook is reinvoked after in-tree reinvocation",
			initialPriorityClass: "low-priority", // 触发初始的 in-tree 变更
			webhooks: []testWebhook{
				// Priority 插件被安排在 mutating webhook 之前运行
				{path: "/setpriority", policy: &reinvokeIfNeeded}, // 触发 in-tree 重新调用时的变更
			},
			expectInvocations: map[string]int{"/setpriority": 2},
			expectAuditPatchAnnotations: map[string]string{
				"patch.webhook.admission.k8s.io/round_0_index_0": patchAnnotationValue("admission.integration.test-2", "admission.integration.test.0.setpriority", `[{"op": "add", "path": "/spec/priorityClassName", "value": "high-priority"},{"op": "remove", "path": "/spec/priority"}]`),
			},
			expectAuditMutationAnnotations: map[string]string{
				"mutation.webhook.admission.k8s.io/round_0_index_0": mutationAnnotationValue("admission.integration.test-2", "admission.integration.test.0.setpriority", true),
				"mutation.webhook.admission.k8s.io/round_1_index_0": mutationAnnotationValue("admission.integration.test-2", "admission.integration.test.0.setpriority", false),
			},
		},
		{ // in-tree（变更）、webhook A（变更）、webhook B（变更）、重新调用 in-tree（无变更）、重新调用 webhook A（无变更），无需重新调用 webhook B
			name:                 "no reinvocation of webhook B when in-tree or prior webhook mutations",
			initialPriorityClass: "low-priority", // 触发初始的 in-tree 变更
			webhooks: []testWebhook{
				{path: "/addlabel", policy: &reinvokeIfNeeded},
				{path: "/conditionaladdlabel", policy: &reinvokeIfNeeded},
			},
			expectLabels:      map[string]string{"x": "true", "a": "true", "b": "true"},
			expectInvocations: map[string]int{"/addlabel": 2, "/conditionaladdlabel": 1},
			expectAuditPatchAnnotations: map[string]string{
				"patch.webhook.admission.k8s.io/round_0_index_0": patchAnnotationValue("admission.integration.test-3", "admission.integration.test.0.addlabel", `[{"op": "add", "path": "/metadata/labels/a", "value": "true"}]`),
				"patch.webhook.admission.k8s.io/round_0_index_1": patchAnnotationValue("admission.integration.test-3", "admission.integration.test.1.conditionaladdlabel", `[{"op": "add", "path": "/metadata/labels/b", "value": "true"}]`),
			},
			expectAuditMutationAnnotations: map[string]string{
				"mutation.webhook.admission.k8s.io/round_0_index_0": mutationAnnotationValue("admission.integration.test-3", "admission.integration.test.0.addlabel", true),
				"mutation.webhook.admission.k8s.io/round_0_index_1": mutationAnnotationValue("admission.integration.test-3", "admission.integration.test.1.conditionaladdlabel", true),
				"mutation.webhook.admission.k8s.io/round_1_index_0": mutationAnnotationValue("admission.integration.test-3", "admission.integration.test.0.addlabel", false),
			},
		},
		{ // in-tree（变更）、webhook A（变更）、webhook B（变更）、重新调用 in-tree（无变更）、重新调用 webhook A（变更）、重新调用 webhook B（变更），两个 webhook 都被重新调用
			name:                 "all webhooks reinvoked when any webhook reinvocation causes mutation",
			initialPriorityClass: "low-priority", // 触发初始的 in-tree 变更
			webhooks: []testWebhook{
				{path: "/settrue", policy: &reinvokeIfNeeded},
				{path: "/setfalse", policy: &reinvokeIfNeeded},
			},
			expectLabels:      map[string]string{"x": "true", "fight": "false"},
			expectInvocations: map[string]int{"/settrue": 2, "/setfalse": 2},
			expectAuditPatchAnnotations: map[string]string{
				"patch.webhook.admission.k8s.io/round_0_index_0": patchAnnotationValue("admission.integration.test-4", "admission.integration.test.0.settrue", `[{"op": "replace", "path": "/metadata/labels/fight", "value": "true"}]`),
				"patch.webhook.admission.k8s.io/round_0_index_1": patchAnnotationValue("admission.integration.test-4", "admission.integration.test.1.setfalse", `[{"op": "replace", "path": "/metadata/labels/fight", "value": "false"}]`),
				"patch.webhook.admission.k8s.io/round_1_index_0": patchAnnotationValue("admission.integration.test-4", "admission.integration.test.0.settrue", `[{"op": "replace", "path": "/metadata/labels/fight", "value": "true"}]`),
				"patch.webhook.admission.k8s.io/round_1_index_1": patchAnnotationValue("admission.integration.test-4", "admission.integration.test.1.setfalse", `[{"op": "replace", "path": "/metadata/labels/fight", "value": "false"}]`),
			},
			expectAuditMutationAnnotations: map[string]string{
				"mutation.webhook.admission.k8s.io/round_0_index_0": mutationAnnotationValue("admission.integration.test-4", "admission.integration.test.0.settrue", true),
				"mutation.webhook.admission.k8s.io/round_0_index_1": mutationAnnotationValue("admission.integration.test-4", "admission.integration.test.1.setfalse", true),
				"mutation.webhook.admission.k8s.io/round_1_index_0": mutationAnnotationValue("admission.integration.test-4", "admission.integration.test.0.settrue", true),
				"mutation.webhook.admission.k8s.io/round_1_index_1": mutationAnnotationValue("admission.integration.test-4", "admission.integration.test.1.setfalse", true),
			},
		},
		{ // in-tree（变更）、webhook A 因 objectSelector 不匹配而被跳过、webhook B（变更）、重新调用 in-tree（无变更）、webhook A 即使现在标签匹配也会被跳过（因为第一轮没有调用它），无需重新调用 webhook B
			name:                 "no reinvocation of webhook B when in-tree or prior webhook mutations",
			initialPriorityClass: "low-priority", // 触发初始的 in-tree 变更
			webhooks: []testWebhook{
				{path: "/conditionaladdlabel", policy: &reinvokeIfNeeded, objectSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"a": "true"}}},
				{path: "/addlabel", policy: &reinvokeIfNeeded},
			},
			expectLabels:      map[string]string{"x": "true", "a": "true"},
			expectInvocations: map[string]int{"/addlabel": 1, "/conditionaladdlabel": 0},
			expectAuditPatchAnnotations: map[string]string{
				"patch.webhook.admission.k8s.io/round_0_index_1": patchAnnotationValue("admission.integration.test-5", "admission.integration.test.1.addlabel", `[{"op": "add", "path": "/metadata/labels/a", "value": "true"}]`),
			},
			expectAuditMutationAnnotations: map[string]string{
				"mutation.webhook.admission.k8s.io/round_0_index_1": mutationAnnotationValue("admission.integration.test-5", "admission.integration.test.1.addlabel", true),
			},
		},
		{
			name: "invalid priority class set by webhook should result in error from in-tree priority plugin",
			webhooks: []testWebhook{
				// Priority 插件被安排在 mutating webhook 之前运行
				{path: "/setinvalidpriority", policy: &reinvokeIfNeeded},
			},
			expectError:       true,
			errorContains:     "no PriorityClass with name invalid was found",
			expectInvocations: map[string]int{"/setinvalidpriority": 1},
		},
		{
			name: "'reinvoke never' policy respected",
			webhooks: []testWebhook{
				{path: "/conditionaladdlabel", policy: &reinvokeNever},
				{path: "/addlabel", policy: &reinvokeNever},
			},
			expectLabels:      map[string]string{"x": "true", "a": "true"},
			expectInvocations: map[string]int{"/conditionaladdlabel": 1, "/addlabel": 1},
			expectAuditPatchAnnotations: map[string]string{
				"patch.webhook.admission.k8s.io/round_0_index_1": patchAnnotationValue("admission.integration.test-7", "admission.integration.test.1.addlabel", `[{"op": "add", "path": "/metadata/labels/a", "value": "true"}]`),
			},
			expectAuditMutationAnnotations: map[string]string{
				"mutation.webhook.admission.k8s.io/round_0_index_0": mutationAnnotationValue("admission.integration.test-7", "admission.integration.test.0.conditionaladdlabel", false),
				"mutation.webhook.admission.k8s.io/round_0_index_1": mutationAnnotationValue("admission.integration.test-7", "admission.integration.test.1.addlabel", true),
			},
		},
		{
			name: "'reinvoke never' (by default) policy respected",
			webhooks: []testWebhook{
				{path: "/conditionaladdlabel", policy: nil},
				{path: "/addlabel", policy: nil},
			},
			expectLabels:      map[string]string{"x": "true", "a": "true"},
			expectInvocations: map[string]int{"/conditionaladdlabel": 1, "/addlabel": 1},
			expectAuditPatchAnnotations: map[string]string{
				"patch.webhook.admission.k8s.io/round_0_index_1": patchAnnotationValue("admission.integration.test-8", "admission.integration.test.1.addlabel", `[{"op": "add", "path": "/metadata/labels/a", "value": "true"}]`),
			},
			expectAuditMutationAnnotations: map[string]string{
				"mutation.webhook.admission.k8s.io/round_0_index_0": mutationAnnotationValue("admission.integration.test-8", "admission.integration.test.0.conditionaladdlabel", false),
				"mutation.webhook.admission.k8s.io/round_0_index_1": mutationAnnotationValue("admission.integration.test-8", "admission.integration.test.1.addlabel", true),
			},
		},
	}

	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(localhostCert) {
		t.Fatal("Failed to append Cert from PEM")
	}
	cert, err := tls.X509KeyPair(localhostCert, localhostKey)
	if err != nil {
		t.Fatalf("Failed to build cert with error: %+v", err)
	}

	recorder := &invocationRecorder{counts: map[string]int{}}
	webhookServer := httptest.NewUnstartedServer(newReinvokeWebhookHandler(recorder))
	webhookServer.TLS = &tls.Config{

		RootCAs:      roots,
		Certificates: []tls.Certificate{cert},
	}
	webhookServer.StartTLS()
	defer webhookServer.Close()

	// 准备审计策略文件
	policyFile, err := os.CreateTemp("", "audit-policy.yaml")
	if err != nil {
		t.Fatalf("Failed to create audit policy file: %v", err)
	}
	defer os.Remove(policyFile.Name())
	if _, err := policyFile.Write([]byte(auditPolicy)); err != nil {
		t.Fatalf("Failed to write audit policy file: %v", err)
	}
	if err := policyFile.Close(); err != nil {
		t.Fatalf("Failed to close audit policy file: %v", err)
	}

	// 准备审计日志文件
	logFile, err := os.CreateTemp("", "audit.log")
	if err != nil {
		t.Fatalf("Failed to create audit log file: %v", err)
	}
	defer utiltesting.CloseAndRemove(t, logFile)

	s := kubeapiservertesting.StartTestServerOrDie(t, kubeapiservertesting.NewDefaultTestServerOptions(), []string{
		"--disable-admission-plugins=ServiceAccount",
		fmt.Sprintf("--watch-cache=%v", watchCache),
		"--audit-policy-file", policyFile.Name(),
		"--audit-log-version", "audit.k8s.io/v1",
		"--audit-log-mode", "blocking",
		"--audit-log-path", logFile.Name(),
	}, framework.SharedEtcd())
	defer s.TearDownFn()

	// 使用一个独特的用户名配置客户端，便于区分该客户端发起的请求
	// 和控制器发起的请求。我们用它在记录请求之前先进行过滤，
	// 以免不小心把控制器的请求误认为
	// 是该客户端发起的请求。
	clientConfig := rest.CopyConfig(s.ClientConfig)
	clientConfig.Impersonate.UserName = testReinvocationClientUsername
	clientConfig.Impersonate.Groups = []string{"system:masters", "system:authenticated"}
	client, err := clientset.NewForConfig(clientConfig)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	for priorityClass, priority := range map[string]int{"low-priority": 1, "high-priority": 10} {
		_, err = client.SchedulingV1().PriorityClasses().Create(context.TODO(), &schedulingv1.PriorityClass{ObjectMeta: metav1.ObjectMeta{Name: priorityClass}, Value: int32(priority)}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
	}

	for i, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			upCh := recorder.Reset()
			testCaseID := strconv.Itoa(i)
			ns := "reinvoke-" + testCaseID
			nsLabels := map[string]string{"test-case": testCaseID}
			_, err = client.CoreV1().Namespaces().Create(context.TODO(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns, Labels: nsLabels}}, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}

			// 将标记写入独立的命名空间，避免相互干扰
			markerNs := ns + "-markers"
			markerNsLabels := map[string]string{"test-markers": testCaseID}
			_, err = client.CoreV1().Namespaces().Create(context.TODO(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: markerNs, Labels: markerNsLabels}}, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}

			// 创建一个标记对象，用于检查 webhook 配置是否已经就绪。
			marker, err := client.CoreV1().Pods(markerNs).Create(context.TODO(), newReinvocationMarkerFixture(markerNs), metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}

			fail := admissionregistrationv1.Fail
			webhooks := []admissionregistrationv1.MutatingWebhook{}
			for j, webhook := range tt.webhooks {
				endpoint := webhookServer.URL + webhook.path
				name := fmt.Sprintf("admission.integration.test.%d.%s", j, strings.TrimPrefix(webhook.path, "/"))
				webhooks = append(webhooks, admissionregistrationv1.MutatingWebhook{
					Name: name,
					ClientConfig: admissionregistrationv1.WebhookClientConfig{
						URL:      &endpoint,
						CABundle: localhostCert,
					},
					Rules: []admissionregistrationv1.RuleWithOperations{{
						Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.OperationAll},
						Rule:       admissionregistrationv1.Rule{APIGroups: []string{""}, APIVersions: []string{"v1"}, Resources: []string{"pods"}},
					}},
					ObjectSelector:          webhook.objectSelector,
					NamespaceSelector:       &metav1.LabelSelector{MatchLabels: nsLabels},
					FailurePolicy:           &fail,
					ReinvocationPolicy:      webhook.policy,
					AdmissionReviewVersions: []string{"v1beta1"},
					SideEffects:             &noSideEffects,
				})
			}
			// 为每组 webhook 配置注册一个用于检查标记的 webhook
			markerEndpoint := webhookServer.URL + "/marker"
			webhooks = append(webhooks, admissionregistrationv1.MutatingWebhook{
				Name: "admission.integration.test.marker",
				ClientConfig: admissionregistrationv1.WebhookClientConfig{
					URL:      &markerEndpoint,
					CABundle: localhostCert,
				},
				Rules: []admissionregistrationv1.RuleWithOperations{{
					Operations: []admissionregistrationv1.OperationType{admissionregistrationv1.OperationAll},
					Rule:       admissionregistrationv1.Rule{APIGroups: []string{""}, APIVersions: []string{"v1"}, Resources: []string{"pods"}},
				}},
				NamespaceSelector:       &metav1.LabelSelector{MatchLabels: markerNsLabels},
				ObjectSelector:          &metav1.LabelSelector{MatchLabels: map[string]string{"marker": "true"}},
				AdmissionReviewVersions: []string{"v1beta1"},
				SideEffects:             &noSideEffects,
			})

			cfg, err := client.AdmissionregistrationV1().MutatingWebhookConfigurations().Create(context.TODO(), &admissionregistrationv1.MutatingWebhookConfiguration{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("admission.integration.test-%d", i)},
				Webhooks:   webhooks,
			}, metav1.CreateOptions{})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				err := client.AdmissionregistrationV1().MutatingWebhookConfigurations().Delete(context.TODO(), cfg.GetName(), metav1.DeleteOptions{})
				if err != nil {
					t.Fatal(err)
				}
			}()

			// 等待新 webhook 第一次被调用
			if err := wait.PollImmediate(time.Millisecond*5, wait.ForeverTestTimeout, func() (bool, error) {
				_, err = client.CoreV1().Pods(markerNs).Patch(context.TODO(), marker.Name, types.JSONPatchType, []byte("[]"), metav1.PatchOptions{})
				select {
				case <-upCh:
					return true, nil
				default:
					t.Logf("Waiting for webhook to become effective, getting marker object: %v", err)
					return false, nil
				}
			}); err != nil {
				t.Fatal(err)
			}

			pod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: ns,
					Name:      "labeled",
					Labels:    map[string]string{"x": "true"},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "fake-name",
						Image: "fakeimage",
					}},
				},
			}
			if tt.initialPriorityClass != "" {
				pod.Spec.PriorityClassName = tt.initialPriorityClass
			}
			obj, err := client.CoreV1().Pods(ns).Create(context.TODO(), pod, metav1.CreateOptions{})

			if tt.expectError {
				if err == nil {
					t.Fatalf("expected error but got none")
				}
				if tt.errorContains != "" {
					if !strings.Contains(err.Error(), tt.errorContains) {
						t.Errorf("expected an error saying %q, but got: %v", tt.errorContains, err)
					}
				}
				return
			}

			if err != nil {
				t.Fatal(err)
			}

			if tt.expectLabels != nil {
				labels := obj.GetLabels()
				if !reflect.DeepEqual(tt.expectLabels, labels) {
					t.Errorf("expected labels '%v', but got '%v'", tt.expectLabels, labels)
				}
			}

			for k, v := range tt.expectInvocations {
				if recorder.GetCount(k) != v {
					t.Errorf("expected %d invocations of %s, but got %d", v, k, recorder.GetCount(k))
				}
			}

			stream, err := os.OpenFile(logFile.Name(), os.O_RDWR, 0600)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			defer stream.Close()
			missing, err := utils.CheckAuditLines(stream, expectedAuditEvents(tt.expectAuditMutationAnnotations, tt.expectAuditPatchAnnotations, ns), auditv1.SchemeGroupVersion)
			if err != nil {
				t.Errorf("unexpected error checking audit lines: %v", err)
			}
			if len(missing.MissingEvents) > 0 {
				t.Errorf("failed to get expected events -- missing: %s", missing)
			}
			if err := stream.Truncate(0); err != nil {
				t.Errorf("unexpected error truncate file: %v", err)
			}
			if _, err := stream.Seek(0, 0); err != nil {
				t.Errorf("unexpected error reset offset: %v", err)
			}
		})
	}
}

type invocationRecorder struct {
	mu     sync.Mutex
	upCh   chan struct{}
	upOnce sync.Once
	counts map[string]int
}

// Reset 将所有计数清零，并返回一个 channel；当收到标记对象的
// 第一次 admission 请求时，该 channel 会被关闭。
func (i *invocationRecorder) Reset() chan struct{} {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.counts = map[string]int{}
	i.upCh = make(chan struct{})
	i.upOnce = sync.Once{}
	return i.upCh
}

func (i *invocationRecorder) MarkerReceived() {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.upOnce.Do(func() {
		close(i.upCh)
	})
}

func (i *invocationRecorder) GetCount(path string) int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.counts[path]
}

func (i *invocationRecorder) IncrementCount(path string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.counts[path]++
}

func newReinvokeWebhookHandler(recorder *invocationRecorder) http.Handler {
	patch := func(w http.ResponseWriter, patch string) {
		w.Header().Set("Content-Type", "application/json")
		pt := v1beta1.PatchTypeJSONPatch
		json.NewEncoder(w).Encode(&v1beta1.AdmissionReview{
			Response: &v1beta1.AdmissionResponse{
				Allowed:   true,
				PatchType: &pt,
				Patch:     []byte(patch),
			},
		})
	}
	allow := func(w http.ResponseWriter) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(&v1beta1.AdmissionReview{
			Response: &v1beta1.AdmissionResponse{
				Allowed: true,
			},
		})
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		data, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), 400)
		}
		review := v1beta1.AdmissionReview{}
		if err := json.Unmarshal(data, &review); err != nil {
			http.Error(w, err.Error(), 400)
		}
		if review.Request.UserInfo.Username != testReinvocationClientUsername {
			// 跳过并非来自本集成测试客户端的请求
			allow(w)
			return
		}

		if len(review.Request.Object.Raw) == 0 {
			http.Error(w, err.Error(), 400)
		}
		pod := &corev1.Pod{}
		if err := json.Unmarshal(review.Request.Object.Raw, pod); err != nil {
			http.Error(w, err.Error(), 400)
		}

		recorder.IncrementCount(r.URL.Path)

		switch r.URL.Path {
		case "/marker":
			// 在测试之间重置时，会不断 patch 标记对象，直到该 webhook 观察到它，
			// 此时即认为 webhook 已就绪。
			recorder.MarkerReceived()
			allow(w)
			return
		case "/noop":
			allow(w)
		case "/settrue":
			patch(w, `[{"op": "replace", "path": "/metadata/labels/fight", "value": "true"}]`)
		case "/setfalse":
			patch(w, `[{"op": "replace", "path": "/metadata/labels/fight", "value": "false"}]`)
		case "/addlabel":
			labels := pod.GetLabels()
			if a, ok := labels["a"]; !ok || a != "true" {
				patch(w, `[{"op": "add", "path": "/metadata/labels/a", "value": "true"}]`)
				return
			}
			allow(w)
		case "/conditionaladdlabel": // 如果设置了 'a'，则将 'b' 设为 true
			labels := pod.GetLabels()
			if _, ok := labels["a"]; ok {
				patch(w, `[{"op": "add", "path": "/metadata/labels/b", "value": "true"}]`)
				return
			}
			allow(w)
		case "/setpriority": // 如果尚未设置 /spec/priorityClassName，则将其设为 high-priority
			if pod.Spec.PriorityClassName != "high-priority" {
				if pod.Spec.Priority != nil {
					patch(w, `[{"op": "add", "path": "/spec/priorityClassName", "value": "high-priority"},{"op": "remove", "path": "/spec/priority"}]`)
				} else {
					patch(w, `[{"op": "add", "path": "/spec/priorityClassName", "value": "high-priority"}]`)
				}
				return
			}
			allow(w)
		case "/setinvalidpriority":
			patch(w, `[{"op": "add", "path": "/spec/priorityClassName", "value": "invalid"}]`)
		default:
			http.NotFound(w, r)
		}
	})
}

func expectedAuditEvents(webhookMutationAnnotations, webhookPatchAnnotations map[string]string, namespace string) []utils.AuditEvent {
	return []utils.AuditEvent{
		{
			Level:                               auditinternal.LevelRequest,
			Stage:                               auditinternal.StageResponseComplete,
			RequestURI:                          fmt.Sprintf("/api/v1/namespaces/%s/pods", namespace),
			Verb:                                "create",
			Code:                                201,
			User:                                "system:apiserver",
			ImpersonatedUser:                    testReinvocationClientUsername,
			ImpersonatedGroups:                  "system:authenticated,system:masters",
			Resource:                            "pods",
			Namespace:                           namespace,
			AuthorizeDecision:                   "allow",
			RequestObject:                       true,
			ResponseObject:                      false,
			AdmissionWebhookMutationAnnotations: webhookMutationAnnotations,
			AdmissionWebhookPatchAnnotations:    webhookPatchAnnotations,
		},
	}
}

func newReinvocationMarkerFixture(namespace string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      "marker",
			Labels: map[string]string{
				"marker": "true",
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "fake-name",
				Image: "fakeimage",
			}},
		},
	}
}
