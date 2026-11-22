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

package ctbattest

import (
	"context"
	"errors"
	"fmt"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	certificatesapi "k8s.io/kubernetes/pkg/apis/certificates"
	"k8s.io/kubernetes/pkg/features"
)

// TestPluginValidate 验证 ClusterTrustBundle 准入插件的校验逻辑。
func TestPluginValidate(t *testing.T) {
	tests := []struct {
		description                      string
		clusterTrustBundleFeatureEnabled bool
		attributes                       admission.Attributes
		allowedName                      string
		allowed                          bool
		authzErr                         error
	}{
		{
			description:                      "wrong type on create",
			clusterTrustBundleFeatureEnabled: true,
			attributes: &testAttributes{
				resource:  certificatesapi.Resource("clustertrustbundles"),
				obj:       &certificatesapi.ClusterTrustBundleList{},
				operation: admission.Create,
			},
			allowed: false,
		},
		{
			description:                      "wrong type on update",
			clusterTrustBundleFeatureEnabled: true,
			attributes: &testAttributes{
				resource:  certificatesapi.Resource("clustertrustbundles"),
				obj:       &certificatesapi.ClusterTrustBundleList{},
				operation: admission.Update,
			},
			allowed: false,
		},
		{
			description:                      "reject requests if looking up permissions fails",
			clusterTrustBundleFeatureEnabled: true,
			attributes: &testAttributes{
				resource: certificatesapi.Resource("clustertrustbundles"),
				obj: &certificatesapi.ClusterTrustBundle{
					Spec: certificatesapi.ClusterTrustBundleSpec{
						SignerName: "abc.com/xyz",
					},
				},
				operation: admission.Update,
			},
			authzErr: errors.New("forced error"),
			allowed:  false,
		},
		{
			description:                      "should allow create if no signer name is specified",
			clusterTrustBundleFeatureEnabled: true,
			allowedName:                      "abc.com/xyz",
			attributes: &testAttributes{
				resource: certificatesapi.Resource("clustertrustbundles"),
				obj: &certificatesapi.ClusterTrustBundle{
					Spec: certificatesapi.ClusterTrustBundleSpec{},
				},
				operation: admission.Create,
			},
			allowed: true,
		},
		{
			description:                      "should allow update if no signer name is specified",
			clusterTrustBundleFeatureEnabled: true,
			allowedName:                      "abc.com/xyz",
			attributes: &testAttributes{
				resource: certificatesapi.Resource("clustertrustbundles"),
				oldObj: &certificatesapi.ClusterTrustBundle{
					Spec: certificatesapi.ClusterTrustBundleSpec{},
				},
				obj: &certificatesapi.ClusterTrustBundle{
					Spec: certificatesapi.ClusterTrustBundleSpec{},
				},
				operation: admission.Update,
			},
			allowed: true,
		},
		{
			description:                      "should allow create if user is authorized for specific signerName",
			clusterTrustBundleFeatureEnabled: true,
			allowedName:                      "abc.com/xyz",
			attributes: &testAttributes{
				resource: certificatesapi.Resource("clustertrustbundles"),
				obj: &certificatesapi.ClusterTrustBundle{
					Spec: certificatesapi.ClusterTrustBundleSpec{
						SignerName: "abc.com/xyz",
					},
				},
				operation: admission.Create,
			},
			allowed: true,
		},
		{
			description:                      "should allow update if user is authorized for specific signerName",
			clusterTrustBundleFeatureEnabled: true,
			allowedName:                      "abc.com/xyz",
			attributes: &testAttributes{
				resource: certificatesapi.Resource("clustertrustbundles"),
				oldObj: &certificatesapi.ClusterTrustBundle{
					Spec: certificatesapi.ClusterTrustBundleSpec{
						SignerName: "abc.com/xyz",
					},
				},
				obj: &certificatesapi.ClusterTrustBundle{
					Spec: certificatesapi.ClusterTrustBundleSpec{
						SignerName: "abc.com/xyz",
					},
				},
				operation: admission.Update,
			},
			allowed: true,
		},
		{
			description:                      "should allow create if user is authorized with wildcard",
			clusterTrustBundleFeatureEnabled: true,
			allowedName:                      "abc.com/*",
			attributes: &testAttributes{
				resource: certificatesapi.Resource("clustertrustbundles"),
				obj: &certificatesapi.ClusterTrustBundle{
					Spec: certificatesapi.ClusterTrustBundleSpec{
						SignerName: "abc.com/xyz",
					},
				},
				operation: admission.Create,
			},
			allowed: true,
		},
		{
			description:                      "should allow update if user is authorized with wildcard",
			clusterTrustBundleFeatureEnabled: true,
			allowedName:                      "abc.com/*",
			attributes: &testAttributes{
				resource: certificatesapi.Resource("clustertrustbundles"),
				oldObj: &certificatesapi.ClusterTrustBundle{
					Spec: certificatesapi.ClusterTrustBundleSpec{
						SignerName: "abc.com/xyz",
					},
				},
				obj: &certificatesapi.ClusterTrustBundle{
					Spec: certificatesapi.ClusterTrustBundleSpec{
						SignerName: "abc.com/xyz",
					},
				},
				operation: admission.Update,
			},
			allowed: true,
		},
		{
			description:                      "should deny create if user does not have permission for this signerName",
			clusterTrustBundleFeatureEnabled: true,
			allowedName:                      "notabc.com/xyz",
			attributes: &testAttributes{
				resource: certificatesapi.Resource("clustertrustbundles"),
				obj: &certificatesapi.ClusterTrustBundle{
					Spec: certificatesapi.ClusterTrustBundleSpec{
						SignerName: "abc.com/xyz",
					},
				},
				operation: admission.Create,
			},
			allowed: false,
		},
		{
			description:                      "should deny update if user does not have permission for this signerName",
			clusterTrustBundleFeatureEnabled: true,
			allowedName:                      "notabc.com/xyz",
			attributes: &testAttributes{
				resource: certificatesapi.Resource("clustertrustbundles"),
				obj: &certificatesapi.ClusterTrustBundle{
					Spec: certificatesapi.ClusterTrustBundleSpec{
						SignerName: "abc.com/xyz",
					},
				},
				operation: admission.Update,
			},
			allowed: false,
		},
		{
			description:                      "should always allow no-op update",
			clusterTrustBundleFeatureEnabled: true,
			authzErr:                         errors.New("broken"),
			attributes: &testAttributes{
				resource: certificatesapi.Resource("clustertrustbundles"),
				oldObj: &certificatesapi.ClusterTrustBundle{
					Spec: certificatesapi.ClusterTrustBundleSpec{
						SignerName: "panda.com/foo",
					},
				},
				obj: &certificatesapi.ClusterTrustBundle{
					Spec: certificatesapi.ClusterTrustBundleSpec{
						SignerName: "panda.com/foo",
					},
				},
				operation: admission.Update,
			},
			allowed: true,
		},
		{
			description:                      "should always allow finalizer update",
			clusterTrustBundleFeatureEnabled: true,
			authzErr:                         errors.New("broken"),
			attributes: &testAttributes{
				resource: certificatesapi.Resource("clustertrustbundles"),
				oldObj: &certificatesapi.ClusterTrustBundle{
					Spec: certificatesapi.ClusterTrustBundleSpec{
						SignerName: "panda.com/foo",
					},
				},
				obj: &certificatesapi.ClusterTrustBundle{
					ObjectMeta: metav1.ObjectMeta{
						OwnerReferences: []metav1.OwnerReference{
							{APIVersion: "something"},
						},
					},
					Spec: certificatesapi.ClusterTrustBundleSpec{
						SignerName: "panda.com/foo",
					},
				},
				operation: admission.Update,
			},
			allowed: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			p := Plugin{
				authz: fakeAuthorizer{
					t:           t,
					verb:        "attest",
					allowedName: tc.allowedName,
					decision:    authorizer.DecisionAllow,
					err:         tc.authzErr,
				},
			}

			featuregatetesting.SetFeatureGateDuringTest(t, feature.DefaultFeatureGate, features.ClusterTrustBundle, tc.clusterTrustBundleFeatureEnabled)
			p.InspectFeatureGates(feature.DefaultFeatureGate)

			err := p.Validate(context.Background(), tc.attributes, nil)
			if err == nil && !tc.allowed {
				t.Errorf("Expected authorization policy to reject ClusterTrustBundle but it was allowed")
			}
			if err != nil && tc.allowed {
				t.Errorf("Expected authorization policy to accept ClusterTrustBundle but it was rejected: %v", err)
			}
		})
	}
}

// fakeAuthorizer 模拟授权器，校验请求属性并返回预设决定。
type fakeAuthorizer struct {
	t           *testing.T
	verb        string
	allowedName string
	decision    authorizer.Decision
	err         error
}

// Authorize 校验请求属性，匹配预设条件后返回设定的授权决定。
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

// testAttributes 提供测试所需的最小准入属性实现，其余方法由内嵌接口占位，调用会 panic。
type testAttributes struct {
	resource    schema.GroupResource
	subresource string
	operation   admission.Operation
	obj, oldObj runtime.Object
	name        string

	admission.Attributes // 其他方法未被覆盖，一旦被调用会因 nil 而 panic
}

// GetResource 返回测试设定的资源。
func (t *testAttributes) GetResource() schema.GroupVersionResource {
	return t.resource.WithVersion("ignored")
}

// GetSubresource 返回测试设定的子资源。
func (t *testAttributes) GetSubresource() string {
	return t.subresource
}

// GetObject 返回测试设定的对象。
func (t *testAttributes) GetObject() runtime.Object {
	return t.obj
}

// GetOldObject 返回测试设定的旧对象。
func (t *testAttributes) GetOldObject() runtime.Object {
	return t.oldObj
}

// GetName 返回测试设定的名称。
func (t *testAttributes) GetName() string {
	return t.name
}

// GetOperation 返回测试设定的操作类型。
func (t *testAttributes) GetOperation() admission.Operation {
	return t.operation
}

// GetUserInfo 返回占位用户信息。
func (t *testAttributes) GetUserInfo() user.Info {
	return &user.DefaultInfo{Name: "ignored"}
}
