/*
Copyright 2023 The Kubernetes Authors.

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

package cel

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	v1 "k8s.io/api/admission/v1"
	appsv1beta1 "k8s.io/api/apps/v1beta1"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	extensionsv1beta1 "k8s.io/api/extensions/v1beta1"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"
	"k8s.io/kubernetes/test/integration/etcd"
)

// Admission 测试框架复制自 test/integration/apiserver/admissionwebhook/admission_test.go。
// 两者的差异都很小，并以 "DIFF" 前缀注释标出。

const (
	testNamespace      = "webhook-integration"
	testClientUsername = "webhook-integration-client"

	mutation   = "mutation"
	validation = "validation"

	// VAP v1beta1 最后可模拟的版本。VAP v1beta1 在 1.34+ 已移除，
	// 但集群仍可能模拟 1.33，因此需继续测试该场景。v1.37 时删除。
	vapV1beta1LastEmulatableVersion = "1.33"
)

// DIFF: 引入接口替代 testContext 中直接使用 *holder，以便注入策略专属的 holder。
type admissionTestExpectationHolder interface {
	reset(t *testing.T)
	expect(gvr schema.GroupVersionResource, gvk, optionsGVK schema.GroupVersionKind, operation v1.Operation, name, namespace string, object, oldObject, options bool)
	verify(t *testing.T)
}

type testContext struct {
	t *testing.T

	emulateV1beta1Version bool
	// DIFF: 由 *holder 改为接口。
	admissionHolder admissionTestExpectationHolder

	client    dynamic.Interface
	clientset clientset.Interface
	verb      string
	gvr       schema.GroupVersionResource
	resource  metav1.APIResource
	resources map[schema.GroupVersionResource]metav1.APIResource
}

type testFunc func(*testContext)

var (
	// defaultResourceFuncs 保存默认测试函数，特定资源可通过 customTestFuncs 覆盖。
	defaultResourceFuncs = map[string]testFunc{
		"create":           testResourceCreate,
		"update":           testResourceUpdate,
		"patch":            testResourcePatch,
		"delete":           testResourceDelete,
		"deletecollection": testResourceDeletecollection,
	}

	// defaultSubresourceFuncs 保存默认的子资源测试函数，特定资源可通过 customTestFuncs 覆盖。
	defaultSubresourceFuncs = map[string]testFunc{
		"update": testSubresourceUpdate,
		"patch":  testSubresourcePatch,
	}

	// customTestFuncs 按资源和 verb 保存自定义测试函数。
	customTestFuncs = map[schema.GroupVersionResource]map[string]testFunc{
		gvr("", "v1", "namespaces"): {"delete": testNamespaceDelete},

		gvr("apps", "v1beta1", "deployments/rollback"):       {"create": testDeploymentRollback},
		gvr("extensions", "v1beta1", "deployments/rollback"): {"create": testDeploymentRollback},

		gvr("", "v1", "pods/attach"):      {"create": testPodConnectSubresource},
		gvr("", "v1", "pods/exec"):        {"create": testPodConnectSubresource},
		gvr("", "v1", "pods/portforward"): {"create": testPodConnectSubresource},

		gvr("", "v1", "bindings"):      {"create": testPodBindingEviction},
		gvr("", "v1", "pods/binding"):  {"create": testPodBindingEviction},
		gvr("", "v1", "pods/eviction"): {"create": testPodBindingEviction},

		gvr("", "v1", "nodes/proxy"):    {"*": testSubresourceProxy},
		gvr("", "v1", "pods/proxy"):     {"*": testSubresourceProxy},
		gvr("", "v1", "services/proxy"): {"*": testSubresourceProxy},

		gvr("", "v1", "serviceaccounts/token"): {"create": testTokenCreate},

		gvr("random.numbers.com", "v1", "integers"): {"create": testPruningRandomNumbers},

		// DIFF: 该测试原本用于 webhook，这里禁用，直到有 mutating admission policy 能写入 "foo" 字段。
		// gvr("custom.fancy.com", "v2", "pants"):      {"create": testNoPruningCustomFancy},
	}

	// admissionExemptResources 列出免于 admission 校验/变更的对象，
	// 只应包含被 apiserver 豁免 admission 处理的资源。
	admissionExemptResources = map[schema.GroupVersionResource]bool{
		// DIFF: WebhookConfigurations 对 webhook admission 豁免，但对 policy admission 不豁免。
		// gvr("admissionregistration.k8s.io", "v1beta1", "mutatingwebhookconfigurations"):       true,
		// gvr("admissionregistration.k8s.io", "v1beta1", "validatingwebhookconfigurations"):     true,
		// gvr("admissionregistration.k8s.io", "v1", "mutatingwebhookconfigurations"):            true,
		// gvr("admissionregistration.k8s.io", "v1", "validatingwebhookconfigurations"):          true,
		// gvr("admissionregistration.k8s.io", "v1alpha1", "validatingadmissionpolicies"):        true,
		// gvr("admissionregistration.k8s.io", "v1alpha1", "validatingadmissionpolicies/status"): true,
		// gvr("admissionregistration.k8s.io", "v1alpha1", "validatingadmissionpolicybindings"):  true,
		gvr("admissionregistration.k8s.io", "v1beta1", "validatingadmissionpolicies"):        true,
		gvr("admissionregistration.k8s.io", "v1beta1", "validatingadmissionpolicies/status"): true,
		gvr("admissionregistration.k8s.io", "v1beta1", "validatingadmissionpolicybindings"):  true,
		gvr("admissionregistration.k8s.io", "v1", "validatingadmissionpolicies"):             true,
		gvr("admissionregistration.k8s.io", "v1", "validatingadmissionpolicies/status"):      true,
		gvr("admissionregistration.k8s.io", "v1", "validatingadmissionpolicybindings"):       true,
		gvr("admissionregistration.k8s.io", "v1", "mutatingadmissionpolicies"):               true,
		gvr("admissionregistration.k8s.io", "v1", "mutatingadmissionpolicybindings"):         true,
		gvr("admissionregistration.k8s.io", "v1alpha1", "mutatingadmissionpolicies"):         true,
		gvr("admissionregistration.k8s.io", "v1alpha1", "mutatingadmissionpolicybindings"):   true,
		gvr("admissionregistration.k8s.io", "v1beta1", "mutatingadmissionpolicies"):          true,
		gvr("admissionregistration.k8s.io", "v1beta1", "mutatingadmissionpolicybindings"):    true,
		// 临时资源豁免
		gvr("authentication.k8s.io", "v1", "selfsubjectreviews"):       true,
		gvr("authentication.k8s.io", "v1beta1", "selfsubjectreviews"):  true,
		gvr("authentication.k8s.io", "v1alpha1", "selfsubjectreviews"): true,
		gvr("authentication.k8s.io", "v1", "tokenreviews"):             true,
		gvr("authorization.k8s.io", "v1", "localsubjectaccessreviews"): true,
		gvr("authorization.k8s.io", "v1", "selfsubjectaccessreviews"):  true,
		gvr("authorization.k8s.io", "v1", "selfsubjectrulesreviews"):   true,
		gvr("authorization.k8s.io", "v1", "subjectaccessreviews"):      true,
	}

	parentResources = map[schema.GroupVersionResource]schema.GroupVersionResource{
		gvr("extensions", "v1beta1", "replicationcontrollers/scale"): gvr("", "v1", "replicationcontrollers"),
	}

	// stubDataOverrides 保存非持久化资源的定义，或需要覆盖默认 stub 的资源。
	stubDataOverrides = map[schema.GroupVersionResource]string{
		// 非持久化 Reviews 资源
		gvr("authentication.k8s.io", "v1", "tokenreviews"):                  `{"metadata": {"name": "tokenreview"}, "spec": {"token": "token", "audience": ["audience1","audience2"]}}`,
		gvr("authentication.k8s.io", "v1beta1", "tokenreviews"):             `{"metadata": {"name": "tokenreview"}, "spec": {"token": "token", "audience": ["audience1","audience2"]}}`,
		gvr("authentication.k8s.io", "v1alpha1", "selfsubjectreviews"):      `{"metadata": {"name": "SelfSubjectReview"},"status":{"userInfo":{}}}`,
		gvr("authentication.k8s.io", "v1beta1", "selfsubjectreviews"):       `{"metadata": {"name": "SelfSubjectReview"},"status":{"userInfo":{}}}`,
		gvr("authentication.k8s.io", "v1", "selfsubjectreviews"):            `{"metadata": {"name": "SelfSubjectReview"},"status":{"userInfo":{}}}`,
		gvr("authorization.k8s.io", "v1", "localsubjectaccessreviews"):      `{"metadata": {"name": "", "namespace":"` + testNamespace + `"}, "spec": {"uid": "token", "user": "user1","groups": ["group1","group2"],"resourceAttributes": {"name":"name1","namespace":"` + testNamespace + `"}}}`,
		gvr("authorization.k8s.io", "v1", "subjectaccessreviews"):           `{"metadata": {"name": "", "namespace":""}, "spec": {"user":"user1","resourceAttributes": {"name":"name1", "namespace":"` + testNamespace + `"}}}`,
		gvr("authorization.k8s.io", "v1", "selfsubjectaccessreviews"):       `{"metadata": {"name": "", "namespace":""}, "spec": {"resourceAttributes": {"name":"name1", "namespace":""}}}`,
		gvr("authorization.k8s.io", "v1", "selfsubjectrulesreviews"):        `{"metadata": {"name": "", "namespace":"` + testNamespace + `"}, "spec": {"namespace":"` + testNamespace + `"}}`,
		gvr("authorization.k8s.io", "v1beta1", "localsubjectaccessreviews"): `{"metadata": {"name": "", "namespace":"` + testNamespace + `"}, "spec": {"uid": "token", "user": "user1","groups": ["group1","group2"],"resourceAttributes": {"name":"name1","namespace":"` + testNamespace + `"}}}`,
		gvr("authorization.k8s.io", "v1beta1", "subjectaccessreviews"):      `{"metadata": {"name": "", "namespace":""}, "spec": {"user":"user1","resourceAttributes": {"name":"name1", "namespace":"` + testNamespace + `"}}}`,
		gvr("authorization.k8s.io", "v1beta1", "selfsubjectaccessreviews"):  `{"metadata": {"name": "", "namespace":""}, "spec": {"resourceAttributes": {"name":"name1", "namespace":""}}}`,
		gvr("authorization.k8s.io", "v1beta1", "selfsubjectrulesreviews"):   `{"metadata": {"name": "", "namespace":"` + testNamespace + `"}, "spec": {"namespace":"` + testNamespace + `"}}`,

		// 其他非持久化资源
	}
)

type webhookOptions struct {
	version string

	// phase 表示 webhook 是 mutating 还是 validating。
	phase string
	// converted 表示 webhook 是否使用 matchPolicy:equivalent 并期望转换。
	// 为 true 时，recordGVR 和 expectGVK 通过 gvrToConvertedGVR/gvrToConvertedGVK 映射；
	// 为 false 时直接与 admission review 比较。
	converted bool
}

type holder struct {
	lock sync.RWMutex

	t *testing.T

	supportedVersions []string
	// DIFF: policy 测试中移除了 warning handler。
	// warningHandler *warningHandler

	recordGVR       metav1.GroupVersionResource
	recordOperation string
	recordNamespace string
	recordName      string

	expectGVK        schema.GroupVersionKind
	expectObject     bool
	expectOldObject  bool
	expectOptionsGVK schema.GroupVersionKind
	expectOptions    bool

	// gvrToConvertedGVR 将提交给 apiserver 的 GVR 映射到转换为 webhook 可识别资源后的期望 GVR。
	// 记录转换后的请求时，会将 gvrToConvertedGVR[recordGVR] 与 webhook 看到的 GVR 比较。
	gvrToConvertedGVR map[metav1.GroupVersionResource]metav1.GroupVersionResource
	// gvrToConvertedGVK 将提交给 apiserver 的 GVR 映射到转换为 webhook 可识别资源后的期望 GVK。
	// 记录转换后的请求时，会将 gvrToConvertedGVK[recordGVR] 与 webhook 看到的 GVK 比较。
	gvrToConvertedGVK map[metav1.GroupVersionResource]schema.GroupVersionKind

	recorded map[webhookOptions]*admissionRequest
}

func (h *holder) reset(t *testing.T) {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.t = t
	h.recordGVR = metav1.GroupVersionResource{}
	h.expectGVK = schema.GroupVersionKind{}
	h.recordOperation = ""
	h.recordName = ""
	h.recordNamespace = ""
	h.expectObject = false
	h.expectOldObject = false
	h.expectOptionsGVK = schema.GroupVersionKind{}
	h.expectOptions = false
	// DIFF: 移除 warning handler。
	// h.warningHandler.reset()

	// 为所有组合预置 nil 记录。
	h.recorded = map[webhookOptions]*admissionRequest{}
	for _, phase := range []string{mutation, validation} {
		for _, converted := range []bool{true, false} {
			for _, version := range h.supportedVersions {
				h.recorded[webhookOptions{version: version, phase: phase, converted: converted}] = nil
			}
		}
	}
}
func (h *holder) expect(gvr schema.GroupVersionResource, gvk, optionsGVK schema.GroupVersionKind, operation v1.Operation, name, namespace string, object, oldObject, options bool) {
	// Namespace 需特殊处理，因为对象名会出现在 request attributes 中。
	if len(namespace) == 0 && gvk.Group == "" && gvk.Version == "v1" && gvk.Kind == "Namespace" {
		namespace = name
	}

	h.lock.Lock()
	defer h.lock.Unlock()
	h.recordGVR = metav1.GroupVersionResource{Group: gvr.Group, Version: gvr.Version, Resource: gvr.Resource}
	h.expectGVK = gvk
	h.recordOperation = string(operation)
	h.recordName = name
	h.recordNamespace = namespace
	h.expectObject = object
	h.expectOldObject = oldObject
	h.expectOptionsGVK = optionsGVK
	h.expectOptions = options
	// DIFF: 移除 warning handler。
	// h.warningHandler.reset()

	// 为所有组合预置 nil 记录。
	h.recorded = map[webhookOptions]*admissionRequest{}
	for _, phase := range []string{mutation, validation} {
		for _, converted := range []bool{true, false} {
			for _, version := range h.supportedVersions {
				h.recorded[webhookOptions{version: version, phase: phase, converted: converted}] = nil
			}
		}
	}
}

type admissionRequest struct {
	Operation   string
	Resource    metav1.GroupVersionResource
	SubResource string
	Namespace   string
	Name        string
	Object      runtime.RawExtension
	OldObject   runtime.RawExtension
	Options     runtime.RawExtension
}

func (h *holder) record(version string, phase string, converted bool, request *admissionRequest) {
	h.lock.Lock()
	defer h.lock.Unlock()

	// 排查请求未被记录的原因时可打开此调试开关。
	debug := false
	if debug {
		h.t.Logf("%s %#v %v", request.Operation, request.Resource, request.SubResource)
	}

	resource := request.Resource
	if len(request.SubResource) > 0 {
		resource.Resource += "/" + request.SubResource
	}

	// 判断这条请求是否应记录。
	gvrToRecord := h.recordGVR
	if converted {
		// 对于转换型 webhook，映射到期望它看到的 GVR。
		gvrToRecord = h.gvrToConvertedGVR[h.recordGVR]
	}
	if resource != gvrToRecord {
		if debug {
			h.t.Log(resource, "!=", gvrToRecord)
		}
		return
	}

	if request.Operation != h.recordOperation {
		if debug {
			h.t.Log(request.Operation, "!=", h.recordOperation)
		}
		return
	}
	if request.Namespace != h.recordNamespace {
		if debug {
			h.t.Log(request.Namespace, "!=", h.recordNamespace)
		}
		return
	}

	name := request.Name
	if name != h.recordName {
		if debug {
			h.t.Log(name, "!=", h.recordName)
		}
		return
	}

	if debug {
		h.t.Logf("recording: %#v = %s %#v %v", webhookOptions{version: version, phase: phase, converted: converted}, request.Operation, request.Resource, request.SubResource)
	}
	h.recorded[webhookOptions{version: version, phase: phase, converted: converted}] = request
}

func (h *holder) verify(t *testing.T) {
	h.lock.Lock()
	defer h.lock.Unlock()

	for options, value := range h.recorded {
		if err := h.verifyRequest(options, value); err != nil {
			t.Errorf("version: %v, phase:%v, converted:%v error: %v", options.version, options.phase, options.converted, err)
		}
	}
}

func (h *holder) verifyRequest(webhookOptions webhookOptions, request *admissionRequest) error {
	converted := webhookOptions.converted

	// 检查当前资源是否应豁免 admission 处理。
	if admissionExemptResources[gvr(h.recordGVR.Group, h.recordGVR.Version, h.recordGVR.Resource)] {
		if request == nil {
			return nil
		}
		return fmt.Errorf("admission webhook was called, but not supposed to")
	}

	if request == nil {
		return fmt.Errorf("no request received")
	}

	if h.expectObject {
		if err := h.verifyObject(converted, request.Object.Object); err != nil {
			return fmt.Errorf("object error: %v", err)
		}
	} else if request.Object.Object != nil {
		return fmt.Errorf("unexpected object: %#v", request.Object.Object)
	}

	if h.expectOldObject {
		if err := h.verifyObject(converted, request.OldObject.Object); err != nil {
			return fmt.Errorf("old object error: %v", err)
		}
	} else if request.OldObject.Object != nil {
		return fmt.Errorf("unexpected old object: %#v", request.OldObject.Object)
	}

	if h.expectOptions {
		if err := h.verifyOptions(request.Options.Object); err != nil {
			return fmt.Errorf("options error: %v", err)
		}
	} else if request.Options.Object != nil {
		return fmt.Errorf("unexpected options: %#v", request.Options.Object)
	}

	// DIFF: 该检查仅适用于 webhook，policy 测试中已移除。
	// if !h.warningHandler.hasWarning(makeWarning(webhookOptions.version, webhookOptions.phase, webhookOptions.converted)) {
	// 	return fmt.Errorf("no warning received from webhook")
	// }

	return nil
}

func (h *holder) verifyObject(converted bool, obj runtime.Object) error {
	if obj == nil {
		return fmt.Errorf("no object sent")
	}
	expectGVK := h.expectGVK
	if converted {
		expectGVK = h.gvrToConvertedGVK[h.recordGVR]
	}
	if obj.GetObjectKind().GroupVersionKind() != expectGVK {
		return fmt.Errorf("expected %#v, got %#v", expectGVK, obj.GetObjectKind().GroupVersionKind())
	}
	return nil
}

func (h *holder) verifyOptions(options runtime.Object) error {
	if options == nil {
		return fmt.Errorf("no options sent")
	}
	if options.GetObjectKind().GroupVersionKind() != h.expectOptionsGVK {
		return fmt.Errorf("expected %#v, got %#v", h.expectOptionsGVK, options.GetObjectKind().GroupVersionKind())
	}
	return nil
}

func getTestFunc(gvr schema.GroupVersionResource, verb string) testFunc {
	if f, found := customTestFuncs[gvr][verb]; found {
		return f
	}
	if f, found := customTestFuncs[gvr]["*"]; found {
		return f
	}
	if strings.Contains(gvr.Resource, "/") {
		if f, found := defaultSubresourceFuncs[verb]; found {
			return f
		}
		return unimplemented
	}
	if f, found := defaultResourceFuncs[verb]; found {
		return f
	}
	return unimplemented
}

func getStubObj(gvr schema.GroupVersionResource, resource metav1.APIResource, emulateV1Beta1Version bool) (*unstructured.Unstructured, error) {
	stub := ""
	if emulateV1Beta1Version {
		// v1.37 移除 TestPolicyAdmissionV1beta1 后此逻辑也应删除。
		if data, ok := etcd.GetEtcdStorageDataForNamespaceServedAt(testNamespace, vapV1beta1LastEmulatableVersion, false)[gvr]; ok {
			stub = data.Stub
		}
	} else {
		if data, ok := etcd.GetEtcdStorageDataForNamespace(testNamespace)[gvr]; ok {
			stub = data.Stub
		}
	}
	if data, ok := stubDataOverrides[gvr]; ok {
		stub = data
	}
	if len(stub) == 0 {
		return nil, fmt.Errorf("no stub data for %#v", gvr)
	}

	stubObj := &unstructured.Unstructured{Object: map[string]interface{}{}}
	if err := json.Unmarshal([]byte(stub), &stubObj.Object); err != nil {
		return nil, fmt.Errorf("error unmarshaling stub for %#v: %v", gvr, err)
	}
	return stubObj, nil
}

func createOrGetResource(client dynamic.Interface, gvr schema.GroupVersionResource, resource metav1.APIResource, emulateV1Beta1Version bool) (*unstructured.Unstructured, error) {
	stubObj, err := getStubObj(gvr, resource, emulateV1Beta1Version)
	if err != nil {
		return nil, err
	}
	ns := ""
	if resource.Namespaced {
		ns = testNamespace
	}
	obj, err := client.Resource(gvr).Namespace(ns).Get(context.TODO(), stubObj.GetName(), metav1.GetOptions{})
	if err == nil {
		return obj, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}
	return client.Resource(gvr).Namespace(ns).Create(context.TODO(), stubObj, metav1.CreateOptions{})
}

func gvr(group, version, resource string) schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: group, Version: version, Resource: resource}
}
func gvk(group, version, kind string) schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: group, Version: version, Kind: kind}
}

var (
	gvkCreateOptions = metav1.SchemeGroupVersion.WithKind("CreateOptions")
	gvkUpdateOptions = metav1.SchemeGroupVersion.WithKind("UpdateOptions")
	gvkDeleteOptions = metav1.SchemeGroupVersion.WithKind("DeleteOptions")
)

func shouldTestResource(gvr schema.GroupVersionResource, resource metav1.APIResource) bool {
	return sets.NewString(resource.Verbs...).HasAny("create", "update", "patch", "connect", "delete", "deletecollection")
}

func shouldTestResourceVerb(gvr schema.GroupVersionResource, resource metav1.APIResource, verb string) bool {
	return sets.NewString(resource.Verbs...).Has(verb)
}

//
// 通用资源测试
//

func testResourceCreate(c *testContext) {
	stubObj, err := getStubObj(c.gvr, c.resource, c.emulateV1beta1Version)
	if err != nil {
		c.t.Error(err)
		return
	}
	ns := ""
	if c.resource.Namespaced {
		ns = testNamespace
	}
	c.admissionHolder.expect(c.gvr, gvk(c.resource.Group, c.resource.Version, c.resource.Kind), gvkCreateOptions, v1.Create, stubObj.GetName(), ns, true, false, true)
	_, err = c.client.Resource(c.gvr).Namespace(ns).Create(context.TODO(), stubObj, metav1.CreateOptions{})
	if err != nil {
		c.t.Error(err)
		return
	}
}

func testResourceUpdate(c *testContext) {
	if err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		obj, err := createOrGetResource(c.client, c.gvr, c.resource, c.emulateV1beta1Version)
		if err != nil {
			return err
		}
		obj.SetAnnotations(map[string]string{"update": "true"})
		c.admissionHolder.expect(c.gvr, gvk(c.resource.Group, c.resource.Version, c.resource.Kind), gvkUpdateOptions, v1.Update, obj.GetName(), obj.GetNamespace(), true, true, true)
		_, err = c.client.Resource(c.gvr).Namespace(obj.GetNamespace()).Update(context.TODO(), obj, metav1.UpdateOptions{})
		return err
	}); err != nil {
		c.t.Error(err)
		return
	}
}

func testResourcePatch(c *testContext) {
	obj, err := createOrGetResource(c.client, c.gvr, c.resource, c.emulateV1beta1Version)
	if err != nil {
		c.t.Error(err)
		return
	}
	c.admissionHolder.expect(c.gvr, gvk(c.resource.Group, c.resource.Version, c.resource.Kind), gvkUpdateOptions, v1.Update, obj.GetName(), obj.GetNamespace(), true, true, true)
	_, err = c.client.Resource(c.gvr).Namespace(obj.GetNamespace()).Patch(
		context.TODO(),
		obj.GetName(),
		types.MergePatchType,
		[]byte(`{"metadata":{"annotations":{"patch":"true"}}}`),
		metav1.PatchOptions{})
	if err != nil {
		c.t.Error(err)
		return
	}
}

func testResourceDelete(c *testContext) {
	// 验证立即删除会触发 webhook 并填充 admissionRequest.oldObject。
	obj, err := createOrGetResource(c.client, c.gvr, c.resource, c.emulateV1beta1Version)
	if err != nil {
		c.t.Error(err)
		return
	}
	background := metav1.DeletePropagationBackground
	zero := int64(0)
	c.admissionHolder.expect(c.gvr, gvk(c.resource.Group, c.resource.Version, c.resource.Kind), gvkDeleteOptions, v1.Delete, obj.GetName(), obj.GetNamespace(), false, true, true)
	err = c.client.Resource(c.gvr).Namespace(obj.GetNamespace()).Delete(context.TODO(), obj.GetName(), metav1.DeleteOptions{GracePeriodSeconds: &zero, PropagationPolicy: &background})
	if err != nil {
		c.t.Error(err)
		return
	}
	c.admissionHolder.verify(c.t)

	// 等待对象消失。
	err = wait.PollImmediate(100*time.Millisecond, 10*time.Second, func() (bool, error) {
		obj, err := c.client.Resource(c.gvr).Namespace(obj.GetNamespace()).Get(context.TODO(), obj.GetName(), metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err == nil {
			c.t.Logf("waiting for %#v to be deleted (name: %s, finalizers: %v)...\n", c.gvr, obj.GetName(), obj.GetFinalizers())
			return false, nil
		}
		return false, err
	})
	if err != nil {
		c.t.Error(err)
		return
	}

	// 验证删除时的更新操作会触发 webhook 并填充 admissionRequest.oldObject。
	obj, err = createOrGetResource(c.client, c.gvr, c.resource, c.emulateV1beta1Version)
	if err != nil {
		c.t.Error(err)
		return
	}
	// 先给对象添加 finalizer 再删除。
	// 不通过 DeleteOptions.PropagationPolicy 添加 finalizer，
	// 因为某些资源（如 events）不支持垃圾回收器 finalizer。
	_, err = c.client.Resource(c.gvr).Namespace(obj.GetNamespace()).Patch(
		context.TODO(),
		obj.GetName(),
		types.MergePatchType,
		[]byte(`{"metadata":{"finalizers":["test/k8s.io"]}}`),
		metav1.PatchOptions{})
	if err != nil {
		c.t.Error(err)
		return
	}
	c.admissionHolder.expect(c.gvr, gvk(c.resource.Group, c.resource.Version, c.resource.Kind), gvkDeleteOptions, v1.Delete, obj.GetName(), obj.GetNamespace(), false, true, true)
	err = c.client.Resource(c.gvr).Namespace(obj.GetNamespace()).Delete(context.TODO(), obj.GetName(), metav1.DeleteOptions{GracePeriodSeconds: &zero, PropagationPolicy: &background})
	if err != nil {
		c.t.Error(err)
		return
	}
	c.admissionHolder.verify(c.t)

	// 等待其他 finalizer（如 CRD 的 customresourcecleanup finalizer）被移除。
	err = wait.PollImmediate(100*time.Millisecond, 10*time.Second, func() (bool, error) {
		obj, err := c.client.Resource(c.gvr).Namespace(obj.GetNamespace()).Get(context.TODO(), obj.GetName(), metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		finalizers := obj.GetFinalizers()
		if len(finalizers) != 1 {
			c.t.Logf("waiting for other finalizers on %#v %s to be removed, existing finalizers are %v", c.gvr, obj.GetName(), obj.GetFinalizers())
			return false, nil
		}
		if finalizers[0] != "test/k8s.io" {
			return false, fmt.Errorf("expected the single finalizer on %#v %s to be test/k8s.io, got %v", c.gvr, obj.GetName(), obj.GetFinalizers())
		}
		return true, nil
	})
	if err != nil {
		c.t.Error(err)
		return
	}

	// 移除 finalizer。
	_, err = c.client.Resource(c.gvr).Namespace(obj.GetNamespace()).Patch(
		context.TODO(),
		obj.GetName(),
		types.MergePatchType,
		[]byte(`{"metadata":{"finalizers":[]}}`),
		metav1.PatchOptions{})
	if err != nil {
		c.t.Error(err)
		return
	}
	// 等待对象消失。
	err = wait.PollImmediate(100*time.Millisecond, 10*time.Second, func() (bool, error) {
		obj, err := c.client.Resource(c.gvr).Namespace(obj.GetNamespace()).Get(context.TODO(), obj.GetName(), metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err == nil {
			c.t.Logf("waiting for %#v to be deleted (name: %s, finalizers: %v)...\n", c.gvr, obj.GetName(), obj.GetFinalizers())
			return false, nil
		}
		return false, err
	})
	if err != nil {
		c.t.Error(err)
		return
	}
}

func testResourceDeletecollection(c *testContext) {
	obj, err := createOrGetResource(c.client, c.gvr, c.resource, c.emulateV1beta1Version)
	if err != nil {
		c.t.Error(err)
		return
	}
	background := metav1.DeletePropagationBackground
	zero := int64(0)

	// 给对象打上与 selector 匹配的 label。
	_, err = c.client.Resource(c.gvr).Namespace(obj.GetNamespace()).Patch(
		context.TODO(),
		obj.GetName(),
		types.MergePatchType,
		[]byte(`{"metadata":{"labels":{"webhooktest":"true"}}}`),
		metav1.PatchOptions{})
	if err != nil {
		c.t.Error(err)
		return
	}

	// 设置预期。
	c.admissionHolder.expect(c.gvr, gvk(c.resource.Group, c.resource.Version, c.resource.Kind), gvkDeleteOptions, v1.Delete, "", obj.GetNamespace(), false, true, true)

	// 删除。
	err = c.client.Resource(c.gvr).Namespace(obj.GetNamespace()).DeleteCollection(context.TODO(), metav1.DeleteOptions{GracePeriodSeconds: &zero, PropagationPolicy: &background}, metav1.ListOptions{LabelSelector: "webhooktest=true"})
	if err != nil {
		c.t.Error(err)
		return
	}

	// 等待对象消失。
	err = wait.PollImmediate(100*time.Millisecond, 10*time.Second, func() (bool, error) {
		obj, err := c.client.Resource(c.gvr).Namespace(obj.GetNamespace()).Get(context.TODO(), obj.GetName(), metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		if err == nil {
			c.t.Logf("waiting for %#v to be deleted (name: %s, finalizers: %v)...\n", c.gvr, obj.GetName(), obj.GetFinalizers())
			return false, nil
		}
		return false, err
	})
	if err != nil {
		c.t.Error(err)
		return
	}
}

func getParentGVR(gvr schema.GroupVersionResource) schema.GroupVersionResource {
	parentGVR, found := parentResources[gvr]
	// 没有特殊覆盖时，直接去掉子资源部分。
	if !found {
		parentGVR = gvr
		parentGVR.Resource = strings.Split(parentGVR.Resource, "/")[0]
	}
	return parentGVR
}

func testTokenCreate(c *testContext) {
	saGVR := gvr("", "v1", "serviceaccounts")
	sa, err := createOrGetResource(c.client, saGVR, c.resources[saGVR], c.emulateV1beta1Version)
	if err != nil {
		c.t.Error(err)
		return
	}

	c.admissionHolder.expect(c.gvr, gvk(c.resource.Group, c.resource.Version, c.resource.Kind), gvkCreateOptions, v1.Create, sa.GetName(), sa.GetNamespace(), true, false, true)
	if err = c.clientset.CoreV1().RESTClient().Post().Namespace(sa.GetNamespace()).Resource("serviceaccounts").Name(sa.GetName()).SubResource("token").Body(&authenticationv1.TokenRequest{
		ObjectMeta: metav1.ObjectMeta{Name: sa.GetName()},
		Spec: authenticationv1.TokenRequestSpec{
			Audiences: []string{"api"},
		},
	}).Do(context.TODO()).Error(); err != nil {
		c.t.Error(err)
		return
	}
	c.admissionHolder.verify(c.t)
}

func testSubresourceUpdate(c *testContext) {
	if err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		parentGVR := getParentGVR(c.gvr)
		parentResource := c.resources[parentGVR]
		obj, err := createOrGetResource(c.client, parentGVR, parentResource, c.emulateV1beta1Version)
		if err != nil {
			return err
		}

		// 将父对象保存为待提交对象。
		submitObj := obj

		gvrWithoutSubresources := c.gvr
		gvrWithoutSubresources.Resource = strings.Split(gvrWithoutSubresources.Resource, "/")[0]
		subresources := strings.Split(c.gvr.Resource, "/")[1:]

		// 若子资源支持 get，则获取它作为提交对象（如 namespaces/finalize、*/scale 等）。
		if sets.NewString(c.resource.Verbs...).Has("get") {
			submitObj, err = c.client.Resource(gvrWithoutSubresources).Namespace(obj.GetNamespace()).Get(context.TODO(), obj.GetName(), metav1.GetOptions{}, subresources...)
			if err != nil {
				return err
			}
		}

		// 修改对象。
		submitObj.SetAnnotations(map[string]string{"subresourceupdate": "true"})

		// 设置预期。
		c.admissionHolder.expect(c.gvr, gvk(c.resource.Group, c.resource.Version, c.resource.Kind), gvkUpdateOptions, v1.Update, obj.GetName(), obj.GetNamespace(), true, true, true)

		_, err = c.client.Resource(gvrWithoutSubresources).Namespace(obj.GetNamespace()).Update(
			context.TODO(),
			submitObj,
			metav1.UpdateOptions{},
			subresources...,
		)
		return err
	}); err != nil {
		c.t.Error(err)
	}
}

func testSubresourcePatch(c *testContext) {
	parentGVR := getParentGVR(c.gvr)
	parentResource := c.resources[parentGVR]
	obj, err := createOrGetResource(c.client, parentGVR, parentResource, c.emulateV1beta1Version)
	if err != nil {
		c.t.Error(err)
		return
	}

	gvrWithoutSubresources := c.gvr
	gvrWithoutSubresources.Resource = strings.Split(gvrWithoutSubresources.Resource, "/")[0]
	subresources := strings.Split(c.gvr.Resource, "/")[1:]

	// 设置预期。
	c.admissionHolder.expect(c.gvr, gvk(c.resource.Group, c.resource.Version, c.resource.Kind), gvkUpdateOptions, v1.Update, obj.GetName(), obj.GetNamespace(), true, true, true)

	_, err = c.client.Resource(gvrWithoutSubresources).Namespace(obj.GetNamespace()).Patch(
		context.TODO(),
		obj.GetName(),
		types.MergePatchType,
		[]byte(`{"metadata":{"annotations":{"subresourcepatch":"true"}}}`),
		metav1.PatchOptions{},
		subresources...,
	)
	if err != nil {
		c.t.Error(err)
		return
	}
}

func unimplemented(c *testContext) {
	c.t.Errorf("Test function for %+v has not been implemented...", c.gvr)
}

//
// 自定义测试方法
//

// testNamespaceDelete 验证 Namespace 特有的删除行为：
// - 首次删除（仅设置 deletionTimestamp 和 terminating 状态）会触发 admission
// - 移除 Namespace 的 finalizer
// - finalizer 移除后的最终删除也会触发 admission
func testNamespaceDelete(c *testContext) {
	obj, err := createOrGetResource(c.client, c.gvr, c.resource, c.emulateV1beta1Version)
	if err != nil {
		c.t.Error(err)
		return
	}
	background := metav1.DeletePropagationBackground
	zero := int64(0)

	c.admissionHolder.expect(c.gvr, gvk(c.resource.Group, c.resource.Version, c.resource.Kind), gvkDeleteOptions, v1.Delete, obj.GetName(), obj.GetNamespace(), false, true, true)
	err = c.client.Resource(c.gvr).Namespace(obj.GetNamespace()).Delete(context.TODO(), obj.GetName(), metav1.DeleteOptions{GracePeriodSeconds: &zero, PropagationPolicy: &background})
	if err != nil {
		c.t.Error(err)
		return
	}
	c.admissionHolder.verify(c.t)

	// 完成 finalization，使 Namespace 可被删除。
	obj, err = c.client.Resource(c.gvr).Namespace(obj.GetNamespace()).Get(context.TODO(), obj.GetName(), metav1.GetOptions{})
	if err != nil {
		c.t.Error(err)
		return
	}
	err = unstructured.SetNestedField(obj.Object, nil, "spec", "finalizers")
	if err != nil {
		c.t.Error(err)
		return
	}
	_, err = c.client.Resource(c.gvr).Namespace(obj.GetNamespace()).Update(context.TODO(), obj, metav1.UpdateOptions{}, "finalize")
	if err != nil {
		c.t.Error(err)
		return
	}
	// 验证 Namespace 已消失。
	obj, err = c.client.Resource(c.gvr).Namespace(obj.GetNamespace()).Get(context.TODO(), obj.GetName(), metav1.GetOptions{})
	if err == nil || !apierrors.IsNotFound(err) {
		c.t.Errorf("expected namespace to be gone, got %#v, %v", obj, err)
	}
}

// testDeploymentRollback 验证 rollback 特有的行为：
// - 创建父 Deployment
// - 创建 rollback 对象并提交
func testDeploymentRollback(c *testContext) {
	deploymentGVR := gvr("apps", "v1", "deployments")
	obj, err := createOrGetResource(c.client, deploymentGVR, c.resources[deploymentGVR], c.emulateV1beta1Version)
	if err != nil {
		c.t.Error(err)
		return
	}

	gvrWithoutSubresources := c.gvr
	gvrWithoutSubresources.Resource = strings.Split(gvrWithoutSubresources.Resource, "/")[0]
	subresources := strings.Split(c.gvr.Resource, "/")[1:]

	c.admissionHolder.expect(c.gvr, gvk(c.resource.Group, c.resource.Version, c.resource.Kind), gvkCreateOptions, v1.Create, obj.GetName(), obj.GetNamespace(), true, false, true)

	var rollbackObj runtime.Object
	switch c.gvr {
	case gvr("apps", "v1beta1", "deployments/rollback"):
		rollbackObj = &appsv1beta1.DeploymentRollback{
			TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1beta1", Kind: "DeploymentRollback"},
			Name:       obj.GetName(),
			RollbackTo: appsv1beta1.RollbackConfig{Revision: 0},
		}
	case gvr("extensions", "v1beta1", "deployments/rollback"):
		rollbackObj = &extensionsv1beta1.DeploymentRollback{
			TypeMeta:   metav1.TypeMeta{APIVersion: "extensions/v1beta1", Kind: "DeploymentRollback"},
			Name:       obj.GetName(),
			RollbackTo: extensionsv1beta1.RollbackConfig{Revision: 0},
		}
	default:
		c.t.Errorf("unknown rollback resource %#v", c.gvr)
		return
	}

	rollbackUnstructuredBody, err := runtime.DefaultUnstructuredConverter.ToUnstructured(rollbackObj)
	if err != nil {
		c.t.Errorf("ToUnstructured failed: %v", err)
		return
	}
	rollbackUnstructuredObj := &unstructured.Unstructured{Object: rollbackUnstructuredBody}
	rollbackUnstructuredObj.SetName(obj.GetName())

	_, err = c.client.Resource(gvrWithoutSubresources).Namespace(obj.GetNamespace()).Create(context.TODO(), rollbackUnstructuredObj, metav1.CreateOptions{}, subresources...)
	if err != nil {
		c.t.Error(err)
		return
	}
}

// testPodConnectSubresource 验证 connect 子资源。
func testPodConnectSubresource(c *testContext) {
	podGVR := gvr("", "v1", "pods")
	pod, err := createOrGetResource(c.client, podGVR, c.resources[podGVR], c.emulateV1beta1Version)
	if err != nil {
		c.t.Error(err)
		return
	}

	// 检查所有可升级的 verb。
	for _, httpMethod := range []string{"GET", "POST"} {
		c.t.Logf("verifying %v", httpMethod)

		c.admissionHolder.expect(c.gvr, gvk(c.resource.Group, c.resource.Version, c.resource.Kind), schema.GroupVersionKind{}, v1.Connect, pod.GetName(), pod.GetNamespace(), true, false, false)
		var err error
		switch c.gvr {
		case gvr("", "v1", "pods/exec"):
			err = c.clientset.CoreV1().RESTClient().Verb(httpMethod).Namespace(pod.GetNamespace()).Resource("pods").Name(pod.GetName()).SubResource("exec").Do(context.TODO()).Error()
		case gvr("", "v1", "pods/attach"):
			err = c.clientset.CoreV1().RESTClient().Verb(httpMethod).Namespace(pod.GetNamespace()).Resource("pods").Name(pod.GetName()).SubResource("attach").Do(context.TODO()).Error()
		case gvr("", "v1", "pods/portforward"):
			err = c.clientset.CoreV1().RESTClient().Verb(httpMethod).Namespace(pod.GetNamespace()).Resource("pods").Name(pod.GetName()).SubResource("portforward").Do(context.TODO()).Error()
		default:
			c.t.Errorf("unknown subresource %#v", c.gvr)
			return
		}

		if err != nil {
			c.t.Logf("debug: result of subresource connect: %v", err)
		}
		c.admissionHolder.verify(c.t)

	}
}

// testPodBindingEviction 验证 Pod binding 和 eviction 的 admission。
func testPodBindingEviction(c *testContext) {
	podGVR := gvr("", "v1", "pods")
	pod, err := createOrGetResource(c.client, podGVR, c.resources[podGVR], c.emulateV1beta1Version)
	if err != nil {
		c.t.Error(err)
		return
	}

	background := metav1.DeletePropagationBackground
	zero := int64(0)
	forceDelete := metav1.DeleteOptions{GracePeriodSeconds: &zero, PropagationPolicy: &background}
	defer func() {
		err := c.clientset.CoreV1().Pods(pod.GetNamespace()).Delete(context.TODO(), pod.GetName(), forceDelete)
		if err != nil && !apierrors.IsNotFound(err) {
			c.t.Error(err)
			return
		}
	}()

	c.admissionHolder.expect(c.gvr, gvk(c.resource.Group, c.resource.Version, c.resource.Kind), gvkCreateOptions, v1.Create, pod.GetName(), pod.GetNamespace(), true, false, true)

	switch c.gvr {
	case gvr("", "v1", "bindings"):
		err = c.clientset.CoreV1().RESTClient().Post().Namespace(pod.GetNamespace()).Resource("bindings").Body(&corev1.Binding{
			ObjectMeta: metav1.ObjectMeta{Name: pod.GetName()},
			Target:     corev1.ObjectReference{Name: "foo", Kind: "Node", APIVersion: "v1"},
		}).Do(context.TODO()).Error()

	case gvr("", "v1", "pods/binding"):
		err = c.clientset.CoreV1().RESTClient().Post().Namespace(pod.GetNamespace()).Resource("pods").Name(pod.GetName()).SubResource("binding").Body(&corev1.Binding{
			ObjectMeta: metav1.ObjectMeta{Name: pod.GetName()},
			Target:     corev1.ObjectReference{Name: "foo", Kind: "Node", APIVersion: "v1"},
		}).Do(context.TODO()).Error()

	case gvr("", "v1", "pods/eviction"):
		err = c.clientset.CoreV1().RESTClient().Post().Namespace(pod.GetNamespace()).Resource("pods").Name(pod.GetName()).SubResource("eviction").Body(&policyv1.Eviction{
			ObjectMeta:    metav1.ObjectMeta{Name: pod.GetName()},
			DeleteOptions: &forceDelete,
		}).Do(context.TODO()).Error()

	default:
		c.t.Errorf("unhandled resource %#v", c.gvr)
		return
	}

	if err != nil {
		c.t.Error(err)
		return
	}
}

// testSubresourceProxy 验证 proxy 子资源。
func testSubresourceProxy(c *testContext) {
	parentGVR := getParentGVR(c.gvr)
	parentResource := c.resources[parentGVR]
	obj, err := createOrGetResource(c.client, parentGVR, parentResource, c.emulateV1beta1Version)
	if err != nil {
		c.t.Error(err)
		return
	}

	gvrWithoutSubresources := c.gvr
	gvrWithoutSubresources.Resource = strings.Split(gvrWithoutSubresources.Resource, "/")[0]
	subresources := strings.Split(c.gvr.Resource, "/")[1:]

	verbToHTTPMethods := map[string][]string{
		"create": {"POST", "GET", "HEAD", "OPTIONS"}, // 同时验证只读 verb 也映射到 Connect admission
		"update": {"PUT"},
		"patch":  {"PATCH"},
		"delete": {"DELETE"},
	}
	httpMethodsToTest, ok := verbToHTTPMethods[c.verb]
	if !ok {
		c.t.Errorf("unknown verb %v", c.verb)
		return
	}

	for _, httpMethod := range httpMethodsToTest {
		c.t.Logf("testing %v", httpMethod)
		request := c.clientset.CoreV1().RESTClient().Verb(httpMethod)

		// 需要时加上 namespace。
		if len(obj.GetNamespace()) > 0 {
			request = request.Namespace(obj.GetNamespace())
		}

		// 设置预期。
		c.admissionHolder.expect(c.gvr, gvk(c.resource.Group, c.resource.Version, c.resource.Kind), schema.GroupVersionKind{}, v1.Connect, obj.GetName(), obj.GetNamespace(), true, false, false)
		// 发起请求；只关心 admission 是否按预期被调用，不关心请求是否成功。
		err = request.Resource(gvrWithoutSubresources.Resource).Name(obj.GetName()).SubResource(subresources...).Do(context.TODO()).Error()
		if err != nil {
			c.t.Logf("debug: result of subresource proxy (error expected): %v", err)
		}
		// 验证结果。
		c.admissionHolder.verify(c.t)
	}
}

func testPruningRandomNumbers(c *testContext) {
	testResourceCreate(c)

	cr2pant, err := c.client.Resource(c.gvr).Get(context.TODO(), "fortytwo", metav1.GetOptions{})
	if err != nil {
		c.t.Error(err)
		return
	}

	foo, found, err := unstructured.NestedString(cr2pant.Object, "foo")
	if err != nil {
		c.t.Error(err)
		return
	}
	if found {
		c.t.Errorf("expected .foo to be pruned, but got: %s", foo)
	}
}

// DIFF: policy 测试中注释掉，待 mutating policy 测试时恢复。
// 该测试依赖 admission webhook/policy 将 "foo" 设为 test。
// func testNoPruningCustomFancy(c *testContext) {
// 	testResourceCreate(c)

// 	cr2pant, err := c.client.Resource(c.gvr).Get(context.TODO(), "cr2pant", metav1.GetOptions{})
// 	if err != nil {
// 		c.t.Error(err)
// 		return
// 	}

// 	foo, _, err := unstructured.NestedString(cr2pant.Object, "foo")
// 	if err != nil {
// 		c.t.Error(err)
// 		return
// 	}

// 	// 检查未发生字段裁剪。
// 	if expected, got := "test", foo; expected != got {
// 		c.t.Errorf("expected /foo to be %q, got: %q", expected, got)
// 	}
// }
