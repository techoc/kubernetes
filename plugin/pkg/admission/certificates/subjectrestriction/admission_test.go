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

package subjectrestriction

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"testing"

	certificatesv1 "k8s.io/api/certificates/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	certificatesapi "k8s.io/kubernetes/pkg/apis/certificates"
)

// TestPlugin_Validate 校验插件在不同资源、子资源、signerName 和 CSR 内容下的准入判断。
func TestPlugin_Validate(t *testing.T) {
	tests := []struct {
		name    string
		a       admission.Attributes
		wantErr string
	}{
		{
			name: "ignored resource",
			a: &testAttributes{
				resource: schema.GroupResource{
					Group:    "foo",
					Resource: "bar",
				},
			},
			wantErr: "",
		},
		{
			name: "ignored subresource",
			a: &testAttributes{
				resource:    certificatesapi.Resource("certificatesigningrequests"),
				subresource: "approve",
			},
			wantErr: "",
		},
		{
			name: "wrong type",
			a: &testAttributes{
				resource: certificatesapi.Resource("certificatesigningrequests"),
				obj:      &certificatesapi.CertificateSigningRequestList{},
				name:     "panda",
			},
			wantErr: `certificatesigningrequests.certificates.k8s.io "panda" is forbidden: expected type CertificateSigningRequest, got: *certificates.CertificateSigningRequestList`,
		},
		{
			name: "some other signer",
			a: &testAttributes{
				resource: certificatesapi.Resource("certificatesigningrequests"),
				obj: &certificatesapi.CertificateSigningRequest{Spec: certificatesapi.CertificateSigningRequestSpec{
					Request:    pemWithGroup("system:masters"),
					SignerName: certificatesv1.KubeAPIServerClientKubeletSignerName,
				}},
			},
			wantErr: "",
		},
		{
			name: "invalid request",
			a: &testAttributes{
				resource: certificatesapi.Resource("certificatesigningrequests"),
				obj: &certificatesapi.CertificateSigningRequest{Spec: certificatesapi.CertificateSigningRequestSpec{
					Request:    []byte("this is not a CSR"),
					SignerName: certificatesv1.KubeAPIServerClientSignerName,
				}},
				name: "bear",
			},
			wantErr: `certificatesigningrequests.certificates.k8s.io "bear" is forbidden: failed to parse CSR: PEM block type must be CERTIFICATE REQUEST`,
		},
		{
			name: "some other group",
			a: &testAttributes{
				resource: certificatesapi.Resource("certificatesigningrequests"),
				obj: &certificatesapi.CertificateSigningRequest{Spec: certificatesapi.CertificateSigningRequestSpec{
					Request:    pemWithGroup("system:admin"),
					SignerName: certificatesv1.KubeAPIServerClientSignerName,
				}},
			},
			wantErr: "",
		},
		{
			name: "request for system:masters",
			a: &testAttributes{
				resource: certificatesapi.Resource("certificatesigningrequests"),
				obj: &certificatesapi.CertificateSigningRequest{Spec: certificatesapi.CertificateSigningRequestSpec{
					Request:    pemWithGroup("system:masters"),
					SignerName: certificatesv1.KubeAPIServerClientSignerName,
				}},
				name: "pooh",
			},
			wantErr: `certificatesigningrequests.certificates.k8s.io "pooh" is forbidden: use of kubernetes.io/kube-apiserver-client signer with system:masters group is not allowed`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 空 Plugin 即可：Validate 不依赖任何初始化状态。
			p := &Plugin{}
			// 错误统一转字符串比较，避免依赖具体错误类型。
			if err := p.Validate(context.TODO(), tt.a, nil); errStr(err) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// testAttributes 是 admission.Attributes 的最小实现，只覆盖 Validate 会调用的方法。
type testAttributes struct {
	resource    schema.GroupResource
	subresource string
	obj         runtime.Object
	name        string

	// 嵌入 nil 接口：若调用了未实现的方法，直接 panic 暴露出来。
	admission.Attributes
}

// GetResource 返回测试用的资源，版本字段对校验逻辑无影响。
func (t *testAttributes) GetResource() schema.GroupVersionResource {
	return t.resource.WithVersion("ignored")
}

// GetSubresource 返回测试用的子资源名。
func (t *testAttributes) GetSubresource() string {
	return t.subresource
}

// GetObject 返回测试用的对象。
func (t *testAttributes) GetObject() runtime.Object {
	return t.obj
}

// GetName 返回测试用的对象名。
func (t *testAttributes) GetName() string {
	return t.name
}

// errStr 将错误转为字符串；空错误返回空串，便于直接和期望值比较。
func errStr(err error) string {
	if err == nil {
		return ""
	}
	es := err.Error()
	// 空错误消息无法与期望值区分，视为无效测试输入。
	if len(es) == 0 {
		panic("invalid empty error")
	}
	return es
}

// pemWithGroup 生成 Subject.Organization 为指定组的 PEM 编码 CSR。
func pemWithGroup(group string) []byte {
	template := &x509.CertificateRequest{
		Subject: pkix.Name{
			Organization: []string{group},
		},
	}

	_, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		panic(err)
	}

	csrDER, err := x509.CreateCertificateRequest(rand.Reader, template, key)
	if err != nil {
		panic(err)
	}

	csrPemBlock := &pem.Block{
		Type:  "CERTIFICATE REQUEST",
		Bytes: csrDER,
	}

	p := pem.EncodeToMemory(csrPemBlock)
	if p == nil {
		panic("invalid pem block")
	}

	return p
}
