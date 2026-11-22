/*
Copyright 2020 The Kubernetes Authors.

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

package signing

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"

	certificatesapi "k8s.io/kubernetes/pkg/apis/certificates"
)

// TestPlugin_Validate 验证 status 子资源更新时，仅证书或条件发生变化才需要签名权限。
func TestPlugin_Validate(t *testing.T) {
	tests := map[string]struct {
		attributes  admission.Attributes
		allowedName string
		allowed     bool
		authzErr    error
	}{
		"wrong type": {
			attributes: &testAttributes{
				resource:    certificatesapi.Resource("certificatesigningrequests"),
				subresource: "status",
				oldObj:      &certificatesapi.CertificateSigningRequestList{},
				obj:         &certificatesapi.CertificateSigningRequestList{},
				operation:   admission.Update,
			},
			allowed: false,
		},
		"allowed if the 'certificate' and conditions field has not changed": {
			attributes: &testAttributes{
				resource:    certificatesapi.Resource("certificatesigningrequests"),
				subresource: "status",
				oldObj: &certificatesapi.CertificateSigningRequest{Status: certificatesapi.CertificateSigningRequestStatus{
					Certificate: []byte("data"),
				}},
				obj: &certificatesapi.CertificateSigningRequest{Status: certificatesapi.CertificateSigningRequestStatus{
					Certificate: []byte("data"),
				}},
				operation: admission.Update,
			},
			// 证书与条件都没变时无需查授权，因此即使注入授权错误也应放行。
			allowed:  true,
			authzErr: errors.New("faked error"),
		},
		"deny request if authz lookup fails on certificate change": {
			allowedName: "abc.com/xyz",
			attributes: &testAttributes{
				resource:    certificatesapi.Resource("certificatesigningrequests"),
				subresource: "status",
				oldObj: &certificatesapi.CertificateSigningRequest{Spec: certificatesapi.CertificateSigningRequestSpec{
					SignerName: "abc.com/xyz",
				}},
				obj: &certificatesapi.CertificateSigningRequest{
					Spec: certificatesapi.CertificateSigningRequestSpec{
						SignerName: "abc.com/xyz",
					},
					Status: certificatesapi.CertificateSigningRequestStatus{
						Certificate: []byte("data"),
					},
				},
				operation: admission.Update,
			},
			authzErr: errors.New("test"),
			allowed:  false,
		},
		"deny request if authz lookup fails on condition change": {
			allowedName: "abc.com/xyz",
			attributes: &testAttributes{
				resource:    certificatesapi.Resource("certificatesigningrequests"),
				subresource: "status",
				oldObj: &certificatesapi.CertificateSigningRequest{Spec: certificatesapi.CertificateSigningRequestSpec{
					SignerName: "abc.com/xyz",
				}},
				obj: &certificatesapi.CertificateSigningRequest{
					Spec: certificatesapi.CertificateSigningRequestSpec{
						SignerName: "abc.com/xyz",
					},
					Status: certificatesapi.CertificateSigningRequestStatus{
						Conditions: []certificatesapi.CertificateSigningRequestCondition{{Type: certificatesapi.CertificateFailed}},
					},
				},
				operation: admission.Update,
			},
			authzErr: errors.New("test"),
			allowed:  false,
		},
		"allow request if user is authorized for specific signerName": {
			allowedName: "abc.com/xyz",
			attributes: &testAttributes{
				resource:    certificatesapi.Resource("certificatesigningrequests"),
				subresource: "status",
				oldObj: &certificatesapi.CertificateSigningRequest{Spec: certificatesapi.CertificateSigningRequestSpec{
					SignerName: "abc.com/xyz",
				}},
				obj: &certificatesapi.CertificateSigningRequest{
					Spec: certificatesapi.CertificateSigningRequestSpec{
						SignerName: "abc.com/xyz",
					},
					Status: certificatesapi.CertificateSigningRequestStatus{
						Certificate: []byte("data"),
					},
				},
				operation: admission.Update,
			},
			allowed: true,
		},
		"allow request if user is authorized with wildcard": {
			allowedName: "abc.com/*",
			attributes: &testAttributes{
				resource:    certificatesapi.Resource("certificatesigningrequests"),
				subresource: "status",
				oldObj: &certificatesapi.CertificateSigningRequest{Spec: certificatesapi.CertificateSigningRequestSpec{
					SignerName: "abc.com/xyz",
				}},
				obj: &certificatesapi.CertificateSigningRequest{
					Spec: certificatesapi.CertificateSigningRequestSpec{
						SignerName: "abc.com/xyz",
					},
					Status: certificatesapi.CertificateSigningRequestStatus{
						Certificate: []byte("data"),
					},
				},
				operation: admission.Update,
			},
			allowed: true,
		},
		"should deny request if user does not have permission for this signerName": {
			allowedName: "notabc.com/xyz",
			attributes: &testAttributes{
				resource:    certificatesapi.Resource("certificatesigningrequests"),
				subresource: "status",
				oldObj: &certificatesapi.CertificateSigningRequest{Spec: certificatesapi.CertificateSigningRequestSpec{
					SignerName: "abc.com/xyz",
				}},
				obj: &certificatesapi.CertificateSigningRequest{
					Spec: certificatesapi.CertificateSigningRequestSpec{
						SignerName: "abc.com/xyz",
					},
					Status: certificatesapi.CertificateSigningRequestStatus{
						Certificate: []byte("data"),
					},
				},
				operation: admission.Update,
			},
			allowed: false,
		},
		"should deny request if user attempts to update signerName to a new value they *do* have permission to sign for": {
			allowedName: "allowed.com/xyz",
			attributes: &testAttributes{
				resource:    certificatesapi.Resource("certificatesigningrequests"),
				subresource: "status",
				oldObj: &certificatesapi.CertificateSigningRequest{Spec: certificatesapi.CertificateSigningRequestSpec{
					SignerName: "notallowed.com/xyz",
				}},
				obj: &certificatesapi.CertificateSigningRequest{
					Spec: certificatesapi.CertificateSigningRequestSpec{
						SignerName: "allowed.com/xyz",
					},
					Status: certificatesapi.CertificateSigningRequestStatus{
						Certificate: []byte("data"),
					},
				},
				operation: admission.Update,
			},
			allowed: false,
		},
	}

	for n, test := range tests {
		t.Run(n, func(t *testing.T) {
			p := Plugin{
				authz: fakeAuthorizer{
					t:           t,
					verb:        "sign",
					allowedName: test.allowedName,
					decision:    authorizer.DecisionAllow,
					err:         test.authzErr,
				},
			}
			// 只按 err 是否为 nil 判断结果，避免依赖具体拒绝原因。
			err := p.Validate(context.Background(), test.attributes, nil)
			if err == nil && !test.allowed {
				t.Errorf("Expected authorization policy to reject CSR but it was allowed")
			}
			if err != nil && test.allowed {
				t.Errorf("Expected authorization policy to accept CSR but it was rejected: %v", err)
			}
		})
	}
}

// fakeAuthorizer 校验插件构造出的授权请求属性，并允许按用例注入决策与错误。
type fakeAuthorizer struct {
	t           *testing.T
	verb        string
	allowedName string
	decision    authorizer.Decision
	err         error
}

// Authorize 实现 authorizer.Authorizer：先返回注入的错误，再逐项比对预期的请求属性。
func (f fakeAuthorizer) Authorize(ctx context.Context, a authorizer.Attributes) (authorizer.Decision, string, error) {
	if f.err != nil {
		return f.decision, "forced error", f.err
	}
	if a.GetVerb() != f.verb {
		return authorizer.DecisionDeny, fmt.Sprintf("unrecognised verb '%s'", a.GetVerb()), nil
	}
	if a.GetAPIGroup() != "certificates.k8s.io" {
		return authorizer.DecisionDeny, fmt.Sprintf("unrecognised groupName '%s'", a.GetAPIGroup()), nil
	}
	if a.GetAPIVersion() != "*" {
		return authorizer.DecisionDeny, fmt.Sprintf("unrecognised apiVersion '%s'", a.GetAPIVersion()), nil
	}
	if a.GetResource() != "signers" {
		return authorizer.DecisionDeny, fmt.Sprintf("unrecognised resource '%s'", a.GetResource()), nil
	}
	if a.GetName() != f.allowedName {
		return authorizer.DecisionDeny, fmt.Sprintf("unrecognised resource name '%s'", a.GetName()), nil
	}
	if !a.IsResourceRequest() {
		return authorizer.DecisionDeny, fmt.Sprintf("unrecognised IsResourceRequest '%t'", a.IsResourceRequest()), nil
	}
	return f.decision, "", nil
}

// testAttributes 只实现插件实际读取的 admission.Attributes 方法，其余保持 nil。
type testAttributes struct {
	resource    schema.GroupResource
	subresource string
	operation   admission.Operation
	oldObj, obj runtime.Object
	name        string

	admission.Attributes // 其余方法未被覆盖，调用时会因 nil 而 panic
}

// GetResource 返回测试用组资源，版本固定为占位值（插件不关心版本）。
func (t *testAttributes) GetResource() schema.GroupVersionResource {
	return t.resource.WithVersion("ignored")
}

// GetSubresource 返回子资源名，通常为 status。
func (t *testAttributes) GetSubresource() string {
	return t.subresource
}

// GetOldObject 返回更新前的对象，供插件判断 status 是否发生变化。
func (t *testAttributes) GetOldObject() runtime.Object {
	return t.oldObj
}

// GetObject 返回提交的对象，供插件读取拟写入的证书与条件。
func (t *testAttributes) GetObject() runtime.Object {
	return t.obj
}

// GetName 返回资源名称，插件在本测试中不使用该值。
func (t *testAttributes) GetName() string {
	return t.name
}

// GetOperation 返回准入操作类型，各用例均为 Update。
func (t *testAttributes) GetOperation() admission.Operation {
	return t.operation
}

// GetUserInfo 返回占位用户信息，插件在本测试中不使用该值。
func (t *testAttributes) GetUserInfo() user.Info {
	return &user.DefaultInfo{Name: "ignored"}
}
