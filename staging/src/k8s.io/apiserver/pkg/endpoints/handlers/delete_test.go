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

package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metainternalversionscheme "k8s.io/apimachinery/pkg/apis/meta/internalversion/scheme"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	jsonserializer "k8s.io/apimachinery/pkg/runtime/serializer/json"
	"k8s.io/apiserver/pkg/admission"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/endpoints/handlers/negotiation"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/registry/rest"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"

	"k8s.io/utils/ptr"

	"github.com/google/go-cmp/cmp"
)

// mockCodecs 包装真实 codec 工厂，并把最近一次 Encode 的错误记录下来，
// 让测试无需解析输出即可断言审计日志对象是否编码成功。
type mockCodecs struct {
	serializer.CodecFactory
	err error
}

// mockCodec 只拦截 Encode，把错误回写到共享的 mockCodecs。
type mockCodec struct {
	runtime.Codec
	codecs *mockCodecs
}

func (p mockCodec) Encode(obj runtime.Object, w io.Writer) error {
	err := p.Codec.Encode(obj, w)
	p.codecs.err = err
	return err
}

// EncoderForVersion 返回带错误记录的编码器；下游审计日志写入走这条路径。
func (s *mockCodecs) EncoderForVersion(encoder runtime.Encoder, gv runtime.GroupVersioner) runtime.Encoder {
	out := s.CodecFactory.CodecForVersions(encoder, nil, gv, nil)
	return &mockCodec{
		Codec:  out,
		codecs: s,
	}
}

// TestDeleteResourceAuditLogRequestObject 验证审计日志序列化 DeleteOptions
// 时使用的 codec：只有注册了对应 group 的 codec 才能成功，否则
// LogRequestObject 应静默降级（不 panic、不阻塞删除）。
func TestDeleteResourceAuditLogRequestObject(t *testing.T) {

	ctx := audit.WithAuditContext(context.TODO())
	ac := audit.AuditContextFrom(ctx)
	if err := ac.Init(audit.RequestAuditConfig{Level: auditinternal.LevelRequestResponse}, nil); err != nil {
		t.Fatal(err)
	}

	policy := metav1.DeletePropagationBackground
	deleteOption := &metav1.DeleteOptions{
		GracePeriodSeconds: ptr.To[int64](30),
		PropagationPolicy:  &policy,
	}

	fakeCorev1GroupVersion := schema.GroupVersion{
		Group:   "",
		Version: "v1",
	}
	testScheme := runtime.NewScheme()
	metav1.AddToGroupVersion(testScheme, fakeCorev1GroupVersion)
	testCodec := serializer.NewCodecFactory(testScheme)

	tests := []struct {
		name       string
		object     runtime.Object
		gv         schema.GroupVersion
		serializer serializer.CodecFactory
		ok         bool
	}{
		{
			name: "meta built-in Codec encode v1.DeleteOptions",
			object: &metav1.DeleteOptions{
				GracePeriodSeconds: ptr.To[int64](30),
				PropagationPolicy:  &policy,
			},
			gv:         metav1.SchemeGroupVersion,
			serializer: metainternalversionscheme.Codecs,
			ok:         true,
		},
		{
			name: "fake corev1 registered codec encode v1 DeleteOptions",
			object: &metav1.DeleteOptions{
				GracePeriodSeconds: ptr.To[int64](30),
				PropagationPolicy:  &policy,
			},
			gv:         metav1.SchemeGroupVersion,
			serializer: testCodec,
			ok:         false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			codecs := &mockCodecs{}
			codecs.CodecFactory = test.serializer

			audit.LogRequestObject(ctx, deleteOption, test.gv, schema.GroupVersionResource{
				Group:    "",
				Version:  "v1",
				Resource: "pods",
			}, "", codecs)

			err := codecs.err
			if err != nil {
				if test.ok {
					t.Errorf("expect nil but got %#v", err)
				}
				t.Logf("encode object: %#v", err)
			} else {
				if !test.ok {
					t.Errorf("expect err but got nil")
				}
			}
		})
	}
}

// For issue https://github.com/kubernetes/kubernetes/issues/132359
// 回归测试：DELETE 请求体中的类型错误（字段类型不符、apiVersion/kind 不匹配）
// 必须返回 400，而不能被忽略或以 500 收场。
func TestDeleteResourceDeleteOptions(t *testing.T) {
	ctx := t.Context()
	fakeDeleterFn := func(ctx context.Context, _ rest.ValidateObjectFunc, _ *metav1.DeleteOptions) (runtime.Object, bool, error) {
		return nil, false, nil
	}
	js := jsonserializer.NewSerializerWithOptions(jsonserializer.DefaultMetaFactory, nil, nil, jsonserializer.SerializerOptions{})
	scope := &RequestScope{
		Namer: &mockNamer{},
		Serializer: &fakeSerializer{
			serializer: runtime.NewCodec(js, js),
		},
	}
	handler := DeleteResource(fakeDeleterFunc(fakeDeleterFn), true, scope, nil)

	tests := []struct {
		name       string
		body       string
		expectCode int
	}{
		{
			name:       "valid delete options",
			body:       "{}",
			expectCode: 200,
		},
		{
			name:       "orphanDependents invalid Go type",
			body:       `{"orphanDependents": "randomString"}`,
			expectCode: 400,
		},
		{
			name:       "apiVersion/kind mismatch",
			body:       `{ "apiVersion": "v1", "kind": "APIResourceList" }`,
			expectCode: 400,
		},
		{
			name:       "apiVersion invalid type",
			body:       `{ "apiVersion": false, "kind": "DeleteOptions" }`,
			expectCode: 400,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(ctx, request.MethodDelete, "/api/v1/namespaces/default/pods/testpod", strings.NewReader(test.body))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Accept", "*/*")

			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, req)
			gotCode := recorder.Code
			if gotCode != test.expectCode {
				t.Fatalf("expected status %v but got %v", test.expectCode, gotCode)
			}
		})
	}
}

// fakeDeleterFunc 让普通函数满足 rest.GracefulDeleter，便于注入测试行为。
type fakeDeleterFunc func(ctx context.Context, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions) (runtime.Object, bool, error)

func (f fakeDeleterFunc) Delete(ctx context.Context, name string, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions) (runtime.Object, bool, error) {
	return f(ctx, deleteValidation, options)
}

// TestDeleteCollection 验证 DeleteOptions 解码对 codec 注册的依赖：
// 未注册 DeleteOptions 的 codec 必须返回明确错误，而标准内部 scheme 应能
// 接受任意 apiVersion 的 DeleteOptions（版本转换后再解码）。
func TestDeleteCollection(t *testing.T) {
	req := &http.Request{
		Header: http.Header{},
	}
	req.Header.Set("Content-Type", "application/json")

	fakeCorev1GroupVersion := schema.GroupVersion{
		Group:   "",
		Version: "v1",
	}
	fakeCorev1Scheme := runtime.NewScheme()
	fakeCorev1Scheme.AddKnownTypes(fakeCorev1GroupVersion, &metav1.DeleteOptions{})
	fakeCorev1Codec := serializer.NewCodecFactory(fakeCorev1Scheme)

	tests := []struct {
		name         string
		codecFactory serializer.CodecFactory
		data         []byte
		expectErr    string
	}{
		//  for issue: https://github.com/kubernetes/kubernetes/issues/111985
		{
			name:         "decode '{}' to metav1.DeleteOptions with fakeCorev1Codecs",
			codecFactory: fakeCorev1Codec,
			data:         []byte("{}"),
			expectErr:    "no kind \"DeleteOptions\" is registered",
		},
		{
			name:         "decode '{}' to metav1.DeleteOptions with metainternalversionscheme.Codecs",
			codecFactory: metainternalversionscheme.Codecs,
			data:         []byte("{}"),
			expectErr:    "",
		},
		{
			name:         "decode versioned (corev1) DeleteOptions with metainternalversionscheme.Codecs",
			codecFactory: metainternalversionscheme.Codecs,
			data:         []byte(`{"apiVersion":"v1","kind":"DeleteOptions","gracePeriodSeconds":123}`),
			expectErr:    "",
		},
		{
			name:         "decode versioned (foo) DeleteOptions with metainternalversionscheme.Codecs",
			codecFactory: metainternalversionscheme.Codecs,
			data:         []byte(`{"apiVersion":"foo/v1","kind":"DeleteOptions","gracePeriodSeconds":123}`),
			expectErr:    "",
		},
	}

	defaultGVK := metav1.SchemeGroupVersion.WithKind("DeleteOptions")
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s, err := negotiation.NegotiateInputSerializer(req, false, test.codecFactory)
			if err != nil {
				t.Fatal(err)
			}

			options := &metav1.DeleteOptions{}
			_, _, err = metainternalversionscheme.Codecs.DecoderToVersion(s.Serializer, defaultGVK.GroupVersion()).Decode(test.data, &defaultGVK, options)
			if test.expectErr != "" {
				if err == nil {
					t.Fatalf("expect %s but got nil", test.expectErr)
				}
				if !strings.Contains(err.Error(), test.expectErr) {
					t.Fatalf("expect %s but got %s", test.expectErr, err.Error())
				}
			}
		})
	}
}

// For issue https://github.com/kubernetes/kubernetes/issues/132359
// deletecollection 与单对象删除共用同一解析路径，同样需把请求体类型错误
// 映射为 400。
func TestDeleteCollectionDeleteOptions(t *testing.T) {
	ctx := t.Context()
	fakeDeleterFn := func(ctx context.Context, _ rest.ValidateObjectFunc, _ *metav1.DeleteOptions, _ *metainternalversion.ListOptions) (runtime.Object, error) {
		return nil, nil
	}
	js := jsonserializer.NewSerializerWithOptions(jsonserializer.DefaultMetaFactory, nil, nil, jsonserializer.SerializerOptions{})
	scope := &RequestScope{
		Namer: &mockNamer{},
		Serializer: &fakeSerializer{
			serializer: runtime.NewCodec(js, js),
		},
	}
	handler := DeleteCollection(fakeCollectionDeleterFunc(fakeDeleterFn), true, scope, nil)

	tests := []struct {
		name       string
		body       string
		expectCode int
	}{
		{
			name:       "valid delete options",
			body:       "{}",
			expectCode: 200,
		},
		{
			name:       "orphanDependents invalid Go type",
			body:       `{"orphanDependents": "randomString"}`,
			expectCode: 400,
		},
		{
			name:       "apiVersion/kind mismatch",
			body:       `{ "apiVersion": "v1", "kind": "APIResourceList" }`,
			expectCode: 400,
		},
		{
			name:       "apiVersion invalid type",
			body:       `{ "apiVersion": false, "kind": "DeleteOptions" }`,
			expectCode: 400,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(ctx, request.MethodDelete, "/api/v1/namespaces/default/pods/testpod", strings.NewReader(test.body))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Accept", "*/*")

			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, req)
			gotCode := recorder.Code
			if gotCode != test.expectCode {
				t.Fatalf("expected status %v but got %v", test.expectCode, gotCode)
			}
		})
	}
}

// TestDeleteCollectionWithNoContextDeadlineEnforced 验证 deletecollection 不会
// 给下层注入额外 deadline：请求本身无截止时间时，deleter 看到的 ctx 也不应有。
func TestDeleteCollectionWithNoContextDeadlineEnforced(t *testing.T) {
	ctx := t.Context()
	var invokedGot, hasDeadlineGot int32
	fakeDeleterFn := func(ctx context.Context, _ rest.ValidateObjectFunc, _ *metav1.DeleteOptions, _ *metainternalversion.ListOptions) (runtime.Object, error) {
		// we expect CollectionDeleter to be executed once
		atomic.AddInt32(&invokedGot, 1)

		// we don't expect any context deadline to be set
		if _, hasDeadline := ctx.Deadline(); hasDeadline {
			atomic.AddInt32(&hasDeadlineGot, 1)
		}
		return nil, nil
	}

	// do the minimum setup to ensure that it gets as far as CollectionDeleter
	scope := &RequestScope{
		Namer: &mockNamer{},
		Serializer: &fakeSerializer{
			serializer: runtime.NewCodec(runtime.NoopEncoder{}, runtime.NoopDecoder{}),
		},
	}
	handler := DeleteCollection(fakeCollectionDeleterFunc(fakeDeleterFn), false, scope, nil)

	request, err := http.NewRequestWithContext(ctx, request.MethodGet, "/test", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// the request context should not have any deadline by default
	if _, hasDeadline := request.Context().Deadline(); hasDeadline {
		t.Fatalf("expected request context to not have any deadline")
	}

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if atomic.LoadInt32(&invokedGot) != 1 {
		t.Errorf("expected collection deleter to be invoked")
	}
	if atomic.LoadInt32(&hasDeadlineGot) > 0 {
		t.Errorf("expected context to not have any deadline")
	}
}

// fakeCollectionDeleterFunc 让普通函数满足 rest.CollectionDeleter。
type fakeCollectionDeleterFunc func(ctx context.Context, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions, listOptions *metainternalversion.ListOptions) (runtime.Object, error)

func (f fakeCollectionDeleterFunc) DeleteCollection(ctx context.Context, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions, listOptions *metainternalversion.ListOptions) (runtime.Object, error) {
	return f(ctx, deleteValidation, options, listOptions)
}

// fakeSerializer 把编解码固定为单一 JSON serializer，简化 handler 测试。
type fakeSerializer struct {
	serializer runtime.Serializer
}

func (n *fakeSerializer) SupportedMediaTypes() []runtime.SerializerInfo {
	return []runtime.SerializerInfo{
		{
			MediaType:        "application/json",
			MediaTypeType:    "application",
			MediaTypeSubType: "json",
		},
	}
}
func (n *fakeSerializer) EncoderForVersion(serializer runtime.Encoder, gv runtime.GroupVersioner) runtime.Encoder {
	return n.serializer
}
func (n *fakeSerializer) DecoderToVersion(serializer runtime.Decoder, gv runtime.GroupVersioner) runtime.Decoder {
	return n.serializer
}

// TestAuthorizeUnsafeDelete 覆盖 unsafe-delete 的授权闸门：只有删除操作、
// 选项为 DeleteOptions 且 ignoreStoreReadError 为 true 时才调用授权器；
// 子资源/非资源请求一律拒绝，授权器报错、弃权或拒绝都映射为 Forbidden。
func TestAuthorizeUnsafeDelete(t *testing.T) {
	const verbWant = "unsafe-delete-ignore-read-errors"
	tests := []struct {
		name    string
		reqInfo *request.RequestInfo
		attr    admission.Attributes
		authz   authorizer.UnconditionalAuthorizer
		err     func(admission.Attributes) error
	}{
		{
			name:  "operation is not delete, admit",
			attr:  newAttributes(attributes{operation: admission.Update}),
			authz: nil, // Authorize should not be invoked
		},
		{
			name: "feature enabled, delete, operation option is nil, admit",
			attr: newAttributes(attributes{
				operation:        admission.Delete,
				operationOptions: nil,
			}),
			authz: nil, // Authorize should not be invoked
		},
		{
			name: "delete, operation option is not a match, forbid",
			attr: newAttributes(attributes{
				operation:        admission.Delete,
				operationOptions: &metav1.PatchOptions{},
			}),
			authz: nil, // Authorize should not be invoked
			err: func(admission.Attributes) error {
				return errors.NewInternalError(fmt.Errorf("expected an option of type: %T, but got: %T", &metav1.DeleteOptions{}, &metav1.PatchOptions{}))
			},
		},
		{
			name: "delete, IgnoreStoreReadErrorWithClusterBreakingPotential is nil, admit",
			attr: newAttributes(attributes{
				operation: admission.Delete,
				operationOptions: &metav1.DeleteOptions{
					IgnoreStoreReadErrorWithClusterBreakingPotential: nil,
				},
			}),
			authz: nil, // Authorize should not be invoked
		},
		{
			name: "delete, IgnoreStoreReadErrorWithClusterBreakingPotential is false, admit",
			attr: newAttributes(attributes{
				operation: admission.Delete,
				operationOptions: &metav1.DeleteOptions{
					IgnoreStoreReadErrorWithClusterBreakingPotential: ptr.To[bool](false),
				},
			}),
			authz: nil, // Authorize should not be invoked
		},
		{
			name:    "feature enabled, delete, IgnoreStoreReadErrorWithClusterBreakingPotential is true, no RequestInfo in request context, forbid",
			reqInfo: nil,
			attr: newAttributes(attributes{
				operation: admission.Delete,
				operationOptions: &metav1.DeleteOptions{
					IgnoreStoreReadErrorWithClusterBreakingPotential: ptr.To[bool](true),
				},
			}),
			authz: nil,
			err: func(attr admission.Attributes) error {
				return admission.NewForbidden(attr, fmt.Errorf("no RequestInfo found in the context"))
			},
		},
		{
			name:    "delete, IgnoreStoreReadErrorWithClusterBreakingPotential is true, subresource request, forbid",
			reqInfo: &request.RequestInfo{IsResourceRequest: true},
			attr: newAttributes(attributes{
				operation:   admission.Delete,
				subresource: "foo",
				operationOptions: &metav1.DeleteOptions{
					IgnoreStoreReadErrorWithClusterBreakingPotential: ptr.To[bool](true),
				},
			}),
			authz: nil,
			err: func(attr admission.Attributes) error {
				return admission.NewForbidden(attr, fmt.Errorf("ignoreStoreReadErrorWithClusterBreakingPotential delete option is not allowed on a subresource or non-resource request"))
			},
		},
		{
			name:    "delete, IgnoreStoreReadErrorWithClusterBreakingPotential is true, subresource request, forbid",
			reqInfo: &request.RequestInfo{IsResourceRequest: false},
			attr: newAttributes(attributes{
				operation:   admission.Delete,
				subresource: "",
				operationOptions: &metav1.DeleteOptions{
					IgnoreStoreReadErrorWithClusterBreakingPotential: ptr.To[bool](true),
				},
			}),
			authz: nil,
			err: func(attr admission.Attributes) error {
				return admission.NewForbidden(attr, fmt.Errorf("ignoreStoreReadErrorWithClusterBreakingPotential delete option is not allowed on a subresource or non-resource request"))
			},
		},
		{
			name:    "delete, IgnoreStoreReadErrorWithClusterBreakingPotential is true, authorizer returns error, forbid",
			reqInfo: &request.RequestInfo{IsResourceRequest: true},
			attr: newAttributes(attributes{
				subresource: "",
				operation:   admission.Delete,
				operationOptions: &metav1.DeleteOptions{
					IgnoreStoreReadErrorWithClusterBreakingPotential: ptr.To[bool](true),
				},
			}),
			authz: &fakeAuthorizer{err: fmt.Errorf("unexpected error")},
			err: func(attr admission.Attributes) error {
				return admission.NewForbidden(attr, fmt.Errorf("error while checking permission for %q, %w", verbWant, fmt.Errorf("unexpected error")))
			},
		},
		{
			name:    "delete, IgnoreStoreReadErrorWithClusterBreakingPotential is true, user does not have permission, forbid",
			reqInfo: &request.RequestInfo{IsResourceRequest: true},
			attr: newAttributes(attributes{
				operation:   admission.Delete,
				subresource: "",
				operationOptions: &metav1.DeleteOptions{
					IgnoreStoreReadErrorWithClusterBreakingPotential: ptr.To[bool](true),
				},
			}),
			authz: &fakeAuthorizer{
				decision: authorizer.DecisionDeny,
				reason:   "does not have permission",
			},
			err: func(attr admission.Attributes) error {
				return admission.NewForbidden(attr, fmt.Errorf("not permitted to do %q, reason: %s", verbWant, "does not have permission"))
			},
		},
		{
			name:    "delete, IgnoreStoreReadErrorWithClusterBreakingPotential is true, authorizer gives no opinion, forbid",
			reqInfo: &request.RequestInfo{IsResourceRequest: true},
			attr: newAttributes(attributes{
				operation:   admission.Delete,
				subresource: "",
				operationOptions: &metav1.DeleteOptions{
					IgnoreStoreReadErrorWithClusterBreakingPotential: ptr.To[bool](true),
				},
			}),
			authz: &fakeAuthorizer{
				decision: authorizer.DecisionNoOpinion,
				reason:   "no opinion",
			},
			err: func(attr admission.Attributes) error {
				return admission.NewForbidden(attr, fmt.Errorf("not permitted to do %q, reason: %s", verbWant, "no opinion"))
			},
		},
		{
			name:    "delete, IgnoreStoreReadErrorWithClusterBreakingPotential is true, user has permission, admit",
			reqInfo: &request.RequestInfo{IsResourceRequest: true},
			attr: newAttributes(attributes{
				operation:   admission.Delete,
				subresource: "",
				operationOptions: &metav1.DeleteOptions{
					IgnoreStoreReadErrorWithClusterBreakingPotential: ptr.To[bool](true),
				},
				userInfo: &user.DefaultInfo{Name: "foo"},
			}),
			authz: &fakeAuthorizer{
				decision: authorizer.DecisionAllow,
				reason:   "permitted",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var want error
			if test.err != nil {
				want = test.err(test.attr)
			}

			ctx := context.Background()
			if test.reqInfo != nil {
				ctx = request.WithRequestInfo(ctx, test.reqInfo)
			}

			// wrap the attributes so we can access the annotations set during admission
			attrs := &fakeAttributes{Attributes: test.attr}
			got := authorizeUnsafeDelete(ctx, attrs, test.authz)
			switch {
			case want != nil:
				if got == nil || want.Error() != got.Error() {
					t.Errorf("expected error: %v, but got: %v", want, got)
				}
			default:
				if got != nil {
					t.Errorf("expected no error, but got: %v", got)
				}
			}
		})
	}
}

// attributes of interest for this test
// 只构造本插件关心的字段；对象、kind 等保持零值以证明它们不会被读取。
type attributes struct {
	operation        admission.Operation
	operationOptions runtime.Object
	userInfo         user.Info
	subresource      string
}

func newAttributes(attr attributes) admission.Attributes {
	return admission.NewAttributesRecord(
		nil,                           // this plugin should never inspect the object
		nil,                           // old object, this plugin should never inspect it
		schema.GroupVersionKind{},     // this plugin should never inspect kind
		"",                            // namespace, leave it empty, this plugin only passes it along to the authorizer
		"",                            // name, leave it empty, this plugin only passes it along to the authorizer
		schema.GroupVersionResource{}, // resource, leave it empty, this plugin only passes it along to the authorizer
		attr.subresource,
		attr.operation,
		attr.operationOptions,
		false, // dryRun, this plugin should never inspect this attribute
		attr.userInfo)
}

// fakeAttributes 记录通过 AddAnnotation 写入的审计注解，便于断言
// unsafe-delete 是否被标记。
type fakeAttributes struct {
	admission.Attributes
	annotations map[string]string
}

func (f *fakeAttributes) AddAnnotation(key, value string) error {
	if err := f.Attributes.AddAnnotation(key, value); err != nil {
		return err
	}

	if len(f.annotations) == 0 {
		f.annotations = map[string]string{}
	}
	f.annotations[key] = value
	return nil
}

// fakeAuthorizer 返回预设的授权决定，用于驱动 authorizeUnsafeDelete 的
// 各失败分支。
type fakeAuthorizer struct {
	decision authorizer.Decision
	reason   string
	err      error
}

func (authorizer fakeAuthorizer) Authorize(ctx context.Context, a authorizer.Attributes) (authorized authorizer.Decision, reason string, err error) {
	return authorizer.decision, authorizer.reason, authorizer.err
}

// TestDeleteResourceWithUnsafeDeletionFlow 是 unsafe delete 的矩阵测试：
// 组合 feature gate、registry 是否提供 corrupt-object deleter、用户是否
// 传入 ignore 选项以及授权结果，断言实际走了普通还是 unsafe 删除流程、
// 是否写入审计注解以及返回的 HTTP 状态。
func TestDeleteResourceWithUnsafeDeletionFlow(t *testing.T) {
	success := metav1.Status{Status: "Success", Code: 200, Details: &metav1.StatusDetails{}}
	newInternalError := func(err error) metav1.Status {
		status := errors.NewInternalError(err).ErrStatus
		status.Kind = "Status"
		status.APIVersion = "v1"
		return status
	}
	newForbiddenError := func() metav1.Status {
		status := errors.NewForbidden(schema.GroupResource{}, "Unknown",
			fmt.Errorf("not permitted to do %q, reason: %s", "unsafe-delete-ignore-read-errors", "not permitted")).ErrStatus
		status.Kind = "Status"
		status.APIVersion = "v1"
		return status
	}

	tests := []struct {
		name           string
		featureEnabled bool
		// whether the registry implements rest.CorruptObjectDeleterProvider, and provides an unsafe deleter:
		//  nil: it does not implement the rest.CorruptObjectDeleterProvider interface
		//  true: it implements CorruptObjectDeleterProvider, and returns a valid unsafe deleter
		//  false: it implements CorruptObjectDeleterProvider, but returns nil when asked for an unsafe deleter
		registryHasUnsafeDeleter *bool
		// what the user passes in the delete options for ignoreStoreReadErrorWithClusterBreakingPotential
		ignoreReadErr *bool
		authorizer    authorizer.UnconditionalAuthorizer
		// want
		normalFlowWant       *deletionFlowTracker // what the normal deletion flow should observe
		unsafeFlowWant       *deletionFlowTracker // what the unsafe deletion flow should observe
		unsafeAnnotationWant bool                 // whether the unsafe audit annotation should be added
		status               metav1.Status        // status we expect from the HTTP response
	}{
		{
			name:                     "feature disabled, registry does not implement CorruptObjectDeleterProvider, ignore is false, should invoke the normal deletion flow",
			featureEnabled:           false,
			registryHasUnsafeDeleter: nil,
			ignoreReadErr:            ptr.To(false),
			normalFlowWant:           &deletionFlowTracker{Invoked: 1, Ignore: nil},
			unsafeFlowWant:           &deletionFlowTracker{},
			status:                   success,
		},
		{
			name:                     "feature disabled, registry does not implement CorruptObjectDeleterProvider, ignore is true, should invoke the normal deletion flow",
			featureEnabled:           false,
			registryHasUnsafeDeleter: nil,
			ignoreReadErr:            ptr.To(true),
			normalFlowWant:           &deletionFlowTracker{Invoked: 1, Ignore: nil},
			unsafeFlowWant:           &deletionFlowTracker{},
			status:                   success,
		},
		{
			name:                     "feature disabled, registry provides a nil unsafe deleter, ignore is false, should invoke the normal deletion flow",
			featureEnabled:           false,
			registryHasUnsafeDeleter: ptr.To(false),
			ignoreReadErr:            ptr.To(false),
			normalFlowWant:           &deletionFlowTracker{Invoked: 1, Ignore: nil},
			unsafeFlowWant:           &deletionFlowTracker{},
			status:                   success,
		},
		{
			name:                     "feature disabled, registry provides a nil unsafe deleter, ignore is true, should invoke the normal deletion flow",
			featureEnabled:           false,
			registryHasUnsafeDeleter: ptr.To(false),
			ignoreReadErr:            ptr.To(true),
			normalFlowWant:           &deletionFlowTracker{Invoked: 1, Ignore: nil},
			unsafeFlowWant:           &deletionFlowTracker{},
			status:                   success,
		},
		{
			name:                     "feature disabled, registry provides a valid unsafe deleter, ignore is false, should invoke the normal deletion flow",
			featureEnabled:           false,
			registryHasUnsafeDeleter: ptr.To(true),
			ignoreReadErr:            ptr.To(false),
			normalFlowWant:           &deletionFlowTracker{Invoked: 1, Ignore: nil},
			unsafeFlowWant:           &deletionFlowTracker{},
			status:                   success,
		},
		{
			name:                     "feature disabled, registry provides a valid unsafe deleter, ignore is true, should invoke the normal deletion flow",
			featureEnabled:           false,
			registryHasUnsafeDeleter: ptr.To(true),
			ignoreReadErr:            ptr.To(true),
			normalFlowWant:           &deletionFlowTracker{Invoked: 1, Ignore: nil},
			unsafeFlowWant:           &deletionFlowTracker{},
			status:                   success,
		},

		// feature enabled
		{
			name:                     "feature enabled, registry does not implement CorruptObjectDeleterProvider, ignore not set, should invoke the normal deletion flow",
			featureEnabled:           true,
			registryHasUnsafeDeleter: nil,
			ignoreReadErr:            nil,
			normalFlowWant:           &deletionFlowTracker{Invoked: 1, Ignore: nil},
			unsafeFlowWant:           &deletionFlowTracker{},
			status:                   success,
		},
		{
			name:                     "feature enabled, registry does not implement CorruptObjectDeleterProvider, ignore is false, should invoke the normal deletion flow",
			featureEnabled:           true,
			registryHasUnsafeDeleter: nil,
			ignoreReadErr:            ptr.To(false),
			normalFlowWant:           &deletionFlowTracker{Invoked: 1, Ignore: ptr.To(false)},
			unsafeFlowWant:           &deletionFlowTracker{},
			status:                   success,
		},
		{
			name:                     "feature enabled, registry does not implement CorruptObjectDeleterProvider, ignore is true, should invoke the normal deletion flow",
			featureEnabled:           true,
			registryHasUnsafeDeleter: nil,
			ignoreReadErr:            ptr.To(true),
			normalFlowWant:           &deletionFlowTracker{},
			unsafeFlowWant:           &deletionFlowTracker{},
			unsafeAnnotationWant:     true,
			status:                   newInternalError(fmt.Errorf("no unsafe deleter provided, can not honor ignoreStoreReadErrorWithClusterBreakingPotential")),
		},
		{
			name:                     "feature enabled, registry provides a nil unsafe deleter, ignore not set, should invoke the normal deletion flow",
			featureEnabled:           true,
			registryHasUnsafeDeleter: ptr.To(false),
			ignoreReadErr:            nil,
			normalFlowWant:           &deletionFlowTracker{Invoked: 1, Ignore: nil},
			unsafeFlowWant:           &deletionFlowTracker{},
			status:                   success,
		},
		{
			name:                     "feature enabled, registry provides a nil unsafe deleter, ignore is false, should invoke the normal deletion flow",
			featureEnabled:           true,
			registryHasUnsafeDeleter: ptr.To(false),
			ignoreReadErr:            ptr.To(false),
			normalFlowWant:           &deletionFlowTracker{Invoked: 1, Ignore: ptr.To(false)},
			unsafeFlowWant:           &deletionFlowTracker{},
			status:                   success,
		},
		{
			name:                     "feature enabled, registry provides a nil unsafe deleter, ignore is true, error expected",
			featureEnabled:           true,
			registryHasUnsafeDeleter: ptr.To(false),
			ignoreReadErr:            ptr.To(true),
			normalFlowWant:           &deletionFlowTracker{},
			unsafeFlowWant:           &deletionFlowTracker{},
			unsafeAnnotationWant:     true,
			status:                   newInternalError(fmt.Errorf("no unsafe deleter provided, can not honor ignoreStoreReadErrorWithClusterBreakingPotential")),
		},
		{
			name:                     "feature enabled, registry provides a valid unsafe deleter, ignore is not set, should invoke the normal deletion flow",
			featureEnabled:           true,
			registryHasUnsafeDeleter: ptr.To(true),
			ignoreReadErr:            nil,
			authorizer:               &fakeAuthorizer{decision: authorizer.DecisionAllow, reason: "permitted"},
			normalFlowWant:           &deletionFlowTracker{Invoked: 1, Ignore: nil},
			unsafeFlowWant:           &deletionFlowTracker{},
			status:                   success,
		},
		{
			name:                     "feature enabled, registry provides a valid unsafe deleter, ignore is false, should invoke the normal deletion flow",
			featureEnabled:           true,
			registryHasUnsafeDeleter: ptr.To(true),
			ignoreReadErr:            ptr.To(false),
			authorizer:               &fakeAuthorizer{decision: authorizer.DecisionAllow, reason: "permitted"},
			normalFlowWant:           &deletionFlowTracker{Invoked: 1, Ignore: ptr.To(false)},
			unsafeFlowWant:           &deletionFlowTracker{},
			status:                   success,
		},
		{
			name:                     "feature enabled, registry provides a valid unsafe deleter, ignore is true, no authorizer, error expected",
			featureEnabled:           true,
			registryHasUnsafeDeleter: ptr.To(true),
			ignoreReadErr:            ptr.To(true),
			normalFlowWant:           &deletionFlowTracker{},
			unsafeFlowWant:           &deletionFlowTracker{},
			unsafeAnnotationWant:     true,
			status:                   newInternalError(fmt.Errorf("no authorizer provided, unable to authorize unsafe delete")),
		},
		{
			name:                     "feature enabled, registry provides a valid unsafe deleter, ignore is true, not authorized, error expected",
			featureEnabled:           true,
			registryHasUnsafeDeleter: ptr.To(true),
			ignoreReadErr:            ptr.To(true),
			authorizer:               &fakeAuthorizer{decision: authorizer.DecisionDeny, reason: "not permitted"},
			normalFlowWant:           &deletionFlowTracker{},
			unsafeFlowWant:           &deletionFlowTracker{},
			unsafeAnnotationWant:     true,
			status:                   newForbiddenError(),
		},
		{
			name:                     "feature enabled, registry provides a valid unsafe deleter, ignore is true, authorized, should invoke the unsafe deletion flow",
			featureEnabled:           true,
			registryHasUnsafeDeleter: ptr.To(true),
			ignoreReadErr:            ptr.To(true),
			authorizer:               &fakeAuthorizer{decision: authorizer.DecisionAllow, reason: "permitted"},
			normalFlowWant:           &deletionFlowTracker{},
			unsafeFlowWant:           &deletionFlowTracker{Invoked: 1, Ignore: ptr.To(true)},
			unsafeAnnotationWant:     true,
			status:                   success,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.AllowUnsafeMalformedObjectDeletion, test.featureEnabled)

			scope := &RequestScope{
				Namer:            &mockNamer{},
				Serializer:       newMetav1Serializer(t),
				Authorizer:       test.authorizer,
				MetaGroupVersion: metav1.SchemeGroupVersion,
			}

			// create a fake deleter that will be used for the normal deletion
			// flow, and it will record the normal deletion activities
			// registryHasUnsafeDeleter 为 nil 表示不实现 provider 接口；
			// 为 false 表示实现但返回 nil，二者语义不同，需分别覆盖。
			normalFlowObserved, unsafeFlowObserved := &deletionFlowTracker{}, &deletionFlowTracker{}
			var deleter rest.GracefulDeleter = &fakeRegistry{tracker: normalFlowObserved}
			if provider := test.registryHasUnsafeDeleter; provider != nil {
				r := &fakeRegistryWithCorruptObjDeleter{GracefulDeleter: deleter}
				if *provider {
					// create a fake deleter that will be used for the unsafe deletion
					// flow, and it will record the unsafe deletion activities
					r.unsafeDeleter = &fakeRegistry{tracker: unsafeFlowObserved}
				}
				deleter = r
			}

			handler := DeleteResource(deleter, true, scope, &fakeAdmitter{})
			req := newRequest(t, test.ignoreReadErr)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, req)

			// the status we have received in the response should match expectation
			resp := recorder.Result()
			status := toStatus(t, resp)
			if want, got := test.status, status; !cmp.Equal(want, got) {
				t.Errorf("expected the unsafe deletion flow observation to match, diff: %s", cmp.Diff(want, got))
			}

			// normal deletion flow
			if want, got := test.normalFlowWant, normalFlowObserved; !cmp.Equal(want, got) {
				t.Errorf("expected the normal deletion flow observation to match, diff: %s", cmp.Diff(want, got))
			}
			// this is an invariant; if the feature is disabled the normal deletion
			// flow should never see the ignore option set.
			if got := normalFlowObserved; !test.featureEnabled && got.Invoked == 1 && got.Ignore != nil {
				t.Errorf("IgnoreStoreReadErrorWithClusterBreakingPotential should always be nil when the feature is disabled, but got: %t", *got.Ignore)
			}

			// unsafe deletion flow
			if want, got := test.unsafeFlowWant, unsafeFlowObserved; !cmp.Equal(want, got) {
				t.Errorf("expected the unsafe deletion flow observation to match, diff: %s", cmp.Diff(want, got))
			}
			// this is an invariant; when invoked, the unsafe deletion flow should always see the option enabled
			if got := ptr.Deref(unsafeFlowObserved.Ignore, false); unsafeFlowObserved.Invoked == 1 && !got {
				t.Errorf("Invariant failed, IgnoreStoreReadErrorWithClusterBreakingPotential should always be set to %t for the unsafe deletion flow, but got: %t", true, got)
			}

			// certain annotation should be added in unsafe delete
			const keyWant = "apiserver.k8s.io/unsafe-delete-ignore-read-error"
			ac := audit.AuditContextFrom(req.Context())
			switch test.unsafeAnnotationWant {
			case true:
				if value, ok := ac.GetEventAnnotation(keyWant); !ok || len(value) > 0 {
					t.Errorf("expected annotation %q to exist with an empty value, but got exists: %t, value: %q", keyWant, ok, value)
				}
			default:
				if value, ok := ac.GetEventAnnotation(keyWant); ok {
					t.Errorf("did not expect annotation %q to exist, but got exists: %t, value: %q", keyWant, ok, value)
				}
			}
		})
	}
}

// TestDeleteCollectionWithUnsafeDeletionFlow 验证 deletecollection 明确禁止
// ignoreStoreReadErrorWithClusterBreakingPotential：开启该选项返回 422，
// deleter 不会被调用，也不会产生 unsafe 审计注解。
func TestDeleteCollectionWithUnsafeDeletionFlow(t *testing.T) {
	success := metav1.Status{Status: "Success", Code: 200, Details: &metav1.StatusDetails{}}

	tests := []struct {
		name           string
		featureEnabled bool
		// what the user passes in the delete options for ignoreStoreReadErrorWithClusterBreakingPotential
		ignoreReadErr *bool
		// want
		deleterInvoked int           // how many times the deleter should be invoked
		status         metav1.Status // status we expect from the HTTP response
	}{
		{
			name:           "feature disabled, ignore not set, should invoke the deleter",
			featureEnabled: false,
			ignoreReadErr:  nil,
			deleterInvoked: 1,
			status:         success,
		},
		{
			name:           "feature disabled, ignore is false, should invoke the deleter",
			featureEnabled: false,
			ignoreReadErr:  ptr.To(false),
			deleterInvoked: 1,
			status:         success,
		},
		{
			name:           "feature disabled, ignore is true, should invoke the deleter",
			featureEnabled: false,
			ignoreReadErr:  ptr.To(true),
			deleterInvoked: 1,
			status:         success,
		},
		{
			name:           "feature enabled, ignore not set, should invoke deleter",
			featureEnabled: true,
			ignoreReadErr:  nil,
			deleterInvoked: 1,
			status:         success,
		},
		{
			name:           "feature enabled, ignore is false, should invoke deleter",
			featureEnabled: true,
			ignoreReadErr:  ptr.To(false),
			deleterInvoked: 1,
			status:         success,
		},
		{
			name:           "feature enabled, ignore is true, invalid error expected",
			featureEnabled: true,
			ignoreReadErr:  ptr.To(true),
			status: metav1.Status{
				TypeMeta: metav1.TypeMeta{Kind: "Status", APIVersion: "v1"},
				Status:   "Failure",
				Reason:   "Invalid",
				Code:     422,
				Details: &metav1.StatusDetails{
					Group: "meta.k8s.io",
					Kind:  "DeleteOptions",
					Causes: []metav1.StatusCause{
						{
							Type:    "FieldValueInvalid",
							Field:   "ignoreStoreReadErrorWithClusterBreakingPotential",
							Message: "Invalid value: true: is not allowed with DELETECOLLECTION, try again after removing the option",
						},
					},
				},
				Message: `DeleteOptions.meta.k8s.io "" is invalid: ignoreStoreReadErrorWithClusterBreakingPotential: Invalid value: true: is not allowed with DELETECOLLECTION, try again after removing the option`,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.AllowUnsafeMalformedObjectDeletion, test.featureEnabled)

			scope := &RequestScope{
				Namer:            &mockNamer{},
				Serializer:       newMetav1Serializer(t),
				ParameterCodec:   runtime.NewParameterCodec(scheme),
				MetaGroupVersion: metav1.SchemeGroupVersion,
			}

			// the deleter should never see the ignore option enabled,
			// initializing it to true, so we can verify that it is reset to nil
			observed := &deletionFlowTracker{}
			deleter := func(ctx context.Context, _ rest.ValidateObjectFunc, options *metav1.DeleteOptions, _ *metainternalversion.ListOptions) (runtime.Object, error) {
				observed.Invoked++
				observed.Ignore = nil
				if ignore := options.IgnoreStoreReadErrorWithClusterBreakingPotential; ignore != nil {
					observed.Ignore = ptr.To(*ignore)
				}
				return nil, nil
			}

			handler := DeleteCollection(fakeCollectionDeleterFunc(deleter), true, scope, &fakeAdmitter{})
			req := newRequest(t, test.ignoreReadErr)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, req)

			// the status we have received in the response should match expectation
			resp := recorder.Result()
			status := toStatus(t, resp)
			if want, got := test.status, status; !cmp.Equal(want, got) {
				t.Errorf("expected the unsafe deletion flow observation to match, diff: %s", cmp.Diff(want, got))
			}

			if want, got := test.deleterInvoked, observed.Invoked; want != got {
				t.Errorf("expected the deleter to be invoked %d times, but got: %d", want, got)
			}
			// if invoked, the deleter should never see the ignore option enabled
			if got := ptr.Deref(observed.Ignore, false); test.deleterInvoked == 1 && got {
				t.Errorf("expected IgnoreStoreReadErrorWithClusterBreakingPotential to be nil for the deleter, but got: %t", got)
			}
			// we never expect the annotation to be added
			const keyNeverWant = "apiserver.k8s.io/unsafe-delete-ignore-read-error"
			ac := audit.AuditContextFrom(req.Context())
			if value, ok := ac.GetEventAnnotation(keyNeverWant); ok || len(value) > 0 {
				t.Errorf("did not expect annotation %q to exist, but got exists: %t, value: %q", keyNeverWant, ok, value)
			}
		})
	}
}

// newRequest 构造带 RequestInfo 与审计上下文的 DELETE 请求；ignoreReadErr
// 非 nil 时以查询参数传入，模拟真实 API 的请求形态。
func newRequest(t *testing.T, ignoreReadErr *bool) *http.Request {
	t.Helper()

	req, err := http.NewRequest(http.MethodDelete, "/test", io.NopCloser(strings.NewReader("")))
	if err != nil {
		t.Fatalf("unexpected error creating a new http.Request: %v", err)
	}

	reqInfo := &request.RequestInfo{IsResourceRequest: true}
	req = req.WithContext(request.WithRequestInfo(req.Context(), reqInfo))

	ctx := audit.WithAuditContext(req.Context())
	ac := audit.AuditContextFrom(ctx)
	if err := ac.Init(audit.RequestAuditConfig{Level: auditinternal.LevelMetadata}, nil); err != nil {
		t.Fatal(err)
	}
	req = req.WithContext(ctx)

	if ignoreReadErr != nil {
		q := req.URL.Query()
		q.Add("ignoreStoreReadErrorWithClusterBreakingPotential", strconv.FormatBool(*ignoreReadErr))
		req.URL.RawQuery = q.Encode()
	}

	req.Header.Set("Content-Type", "application/json")
	return req
}

// newMetav1Serializer 构造只含 metav1 与内部 meta 类型的 serializer，确保
// 测试不依赖完整 API scheme。
func newMetav1Serializer(t *testing.T) runtime.NegotiatedSerializer {
	t.Helper()

	scheme := runtime.NewScheme()
	metav1.AddToGroupVersion(scheme, metav1.SchemeGroupVersion)
	if err := metainternalversion.AddToScheme(scheme); err != nil {
		t.Fatalf("metainternalversion.AddToScheme failed - err: %v", err)
	}
	codecs := serializer.NewCodecFactory(scheme)
	info, ok := runtime.SerializerInfoForMediaType(codecs.SupportedMediaTypes(), runtime.ContentTypeJSON)
	if !ok {
		t.Fatalf("failed to setup serializer")
	}
	return runtime.NewSimpleNegotiatedSerializer(info)
}

// toStatus 把响应体解析为 metav1.Status，测试直接比较完整状态对象。
func toStatus(t *testing.T, resp *http.Response) metav1.Status {
	t.Helper()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("unexpected error reading response body: %v", err)
	}
	var status metav1.Status
	if err := json.Unmarshal(body, &status); err != nil {
		t.Fatalf("unexpected error unmarshaling to metav1.Status: %v", err)
	}
	return status
}

// to keep track of what a deletion flow (GracefulDeleter) observes
// Ignore 保存最后一次看到的 ignore 选项，用于区分普通/unsafe 流程。
type deletionFlowTracker struct {
	Invoked int   // how many times Delete has been invoked
	Ignore  *bool // the ignore option passed to Delete
}

// implements a fake GracefulDeleter.
// TODO: this is not thread safe if concurrent Delete calls are made by a
// test, tests are expected to invoke Delete serially, if that is to chage
// we need to implement locking to keept it concurrency safe.
// 记录调用次数与最后一次看到的 ignore 选项；测试串行调用，故无需加锁。
type fakeRegistry struct {
	tracker *deletionFlowTracker
}

func (f *fakeRegistry) Delete(ctx context.Context, name string, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions) (runtime.Object, bool, error) {
	f.tracker.Invoked++
	f.tracker.Ignore = nil
	// the following is on the basis that invoking the deleter more than once is an
	// error condition, so we are just storing the final ignore option provided
	if ignore := options.IgnoreStoreReadErrorWithClusterBreakingPotential; ignore != nil {
		f.tracker.Ignore = ptr.To(*ignore)
	}
	return nil, true, nil
}

var _ rest.CorruptObjectDeleterProvider = &fakeRegistryWithCorruptObjDeleter{}

// fake registry implementation with support of the unsafe deletion flow
// fakeRegistryWithCorruptObjDeleter 同时提供普通删器与 unsafe 删器；
// handler 通过 CorruptObjectDeleterProvider 接口决定是否切换流程。
type fakeRegistryWithCorruptObjDeleter struct {
	// the default GracefulDeleter in use for the normal deletion flow
	rest.GracefulDeleter
	// used for the unsafe deletion flow
	unsafeDeleter rest.GracefulDeleter
}

// the delete handler switches to the unsafe deletion flow by using GetCorruptObjDeleter
func (p *fakeRegistryWithCorruptObjDeleter) GetCorruptObjDeleter() rest.GracefulDeleter {
	return p.unsafeDeleter
}

// fakeAdmitter 对所有操作都声明不处理，确保测试不受准入插件干扰。
type fakeAdmitter struct{}

func (f *fakeAdmitter) Handles(_ admission.Operation) bool { return false }
