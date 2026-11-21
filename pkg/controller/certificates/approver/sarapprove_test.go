/*
Copyright 2017 The Kubernetes Authors.

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

package approver

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/rand"
	"net"
	"testing"

	authorization "k8s.io/api/authorization/v1"
	capi "k8s.io/api/certificates/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	testclient "k8s.io/client-go/testing"
	k8s_certificates_v1 "k8s.io/kubernetes/pkg/apis/certificates/v1"
)

// TestHandle 测试 sarApprover.handle 根据识别结果与授权结果决定如何处理 CSR。
// 只有既被识别又获得授权的 CSR 才会被批准；未被识别的 CSR 不应产生任何客户端调用。
func TestHandle(t *testing.T) {
	cases := []struct {
		allowed    bool
		recognized bool
		err        bool
		verify     func(*testing.T, []testclient.Action)
	}{
		{
			// 未识别且未授权：不应触发任何 API 调用。
			recognized: false,
			allowed:    false,
			verify: func(t *testing.T, as []testclient.Action) {
				if len(as) != 0 {
					t.Errorf("expected no client calls but got: %#v", as)
				}
			},
		},
		{
			// 未识别但授权：识别器不认得，仍不应触发任何 API 调用。
			recognized: false,
			allowed:    true,
			verify: func(t *testing.T, as []testclient.Action) {
				if len(as) != 0 {
					t.Errorf("expected no client calls but got: %#v", as)
				}
			},
		},
		{
			// 已识别但未授权：会创建 SubjectAccessReview，但不会批准 CSR，并返回错误。
			recognized: true,
			allowed:    false,
			verify: func(t *testing.T, as []testclient.Action) {
				if len(as) != 1 {
					t.Errorf("expected 1 call but got: %#v", as)
					return
				}
				_ = as[0].(testclient.CreateActionImpl)
			},
			err: true,
		},
		{
			// 已识别且已授权：先创建 SubjectAccessReview，再更新 CSR 的 approval 子资源。
			recognized: true,
			allowed:    true,
			verify: func(t *testing.T, as []testclient.Action) {
				if len(as) != 2 {
					t.Errorf("expected two calls but got: %#v", as)
					return
				}
				_ = as[0].(testclient.CreateActionImpl)
				a := as[1].(testclient.UpdateActionImpl)
				if got, expected := a.Verb, "update"; got != expected {
					t.Errorf("got: %v, expected: %v", got, expected)
				}
				if got, expected := a.Resource, (schema.GroupVersionResource{Group: "certificates.k8s.io", Version: "v1", Resource: "certificatesigningrequests"}); got != expected {
					t.Errorf("got: %v, expected: %v", got, expected)
				}
				if got, expected := a.Subresource, "approval"; got != expected {
					t.Errorf("got: %v, expected: %v", got, expected)
				}
				csr := a.Object.(*capi.CertificateSigningRequest)
				if len(csr.Status.Conditions) != 1 {
					t.Errorf("expected CSR to have approved condition: %#v", csr)
				}
				c := csr.Status.Conditions[0]
				if got, expected := c.Type, capi.CertificateApproved; got != expected {
					t.Errorf("got: %v, expected: %v", got, expected)
				}
				if got, expected := c.Reason, "AutoApproved"; got != expected {
					t.Errorf("got: %v, expected: %v", got, expected)
				}
			},
		},
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("recognized:%v,allowed: %v,err: %v", c.recognized, c.allowed, c.err), func(t *testing.T) {
			client := &fake.Clientset{}
			// 用 reactor 固定 SubjectAccessReview 的授权结果，使测试不依赖真实鉴权。
			client.AddReactor("create", "subjectaccessreviews", func(action testclient.Action) (handled bool, ret runtime.Object, err error) {
				return true, &authorization.SubjectAccessReview{
					Status: authorization.SubjectAccessReviewStatus{
						Allowed: c.allowed,
					},
				}, nil
			})
			approver := sarApprover{
				client: client,
				recognizers: []csrRecognizer{
					{
						successMessage: "tester",
						permission:     authorization.ResourceAttributes{Group: "foo", Resource: "bar", Subresource: "baz"},
						recognize: func(csr *capi.CertificateSigningRequest, x509cr *x509.CertificateRequest) bool {
							return c.recognized
						},
					},
				},
			}
			csr := makeTestCsr()
			ctx := context.TODO()
			if err := approver.handle(ctx, csr); err != nil && !c.err {
				t.Errorf("unexpected err: %v", err)
			}
			c.verify(t, client.Actions())
		})
	}
}

// TestRecognizers 测试 isNodeClientCert 与 isSelfNodeClientCert 两个识别器。
// 通过构造不同的 CSR 字段组合，验证合法用例被接受、非法用例被拒绝，
// 以及请求者与 CN 不一致时两个识别器行为的差异。
func TestRecognizers(t *testing.T) {
	// 合法 CSR 只需默认字段，无需额外修改。
	goodCases := []func(b *csrBuilder){
		func(b *csrBuilder) {
		},
	}

	testRecognizer(t, goodCases, isNodeClientCert, true)
	testRecognizer(t, goodCases, isSelfNodeClientCert, true)

	// 非法用例逐一破坏某个必要条件：CN、组织、签名者名称或用途任一不符都应被拒绝。
	badCases := []func(b *csrBuilder){
		func(b *csrBuilder) {
			b.cn = "mike"
		},
		func(b *csrBuilder) {
			b.orgs = nil
		},
		func(b *csrBuilder) {
			b.orgs = []string{"system:master"}
		},
		func(b *csrBuilder) {
			b.usages = append(b.usages, capi.UsageServerAuth)
		},
		func(b *csrBuilder) {
			b.signerName = "example.com/not-correct"
		},
		func(b *csrBuilder) {
			b.signerName = capi.KubeletServingSignerName
		},
	}

	testRecognizer(t, badCases, isNodeClientCert, false)
	testRecognizer(t, badCases, isSelfNodeClientCert, false)

	// CN 与请求者不同：节点客户端证书仍可识别，自签名节点客户端证书则要求两者一致。
	differentCN := []func(b *csrBuilder){
		func(b *csrBuilder) {
			b.requestor = "joe"
		},
		func(b *csrBuilder) {
			b.cn = "system:node:bar"
		},
	}

	testRecognizer(t, differentCN, isNodeClientCert, true)
	testRecognizer(t, differentCN, isSelfNodeClientCert, false)
}

// testRecognizer 对一组 CSR 变更函数逐一构造 CSR，验证识别器的判定结果。
// 每个用例都会运行两遍：一遍包含 UsageKeyEncipherment，一遍不包含，
// 因为某些识别逻辑会区分这两种密钥用途。
func testRecognizer(t *testing.T, cases []func(b *csrBuilder), recognizeFunc func(csr *capi.CertificateSigningRequest, x509cr *x509.CertificateRequest) bool, shouldRecognize bool) {
	for _, c := range cases {
		b := csrBuilder{
			signerName: capi.KubeAPIServerClientKubeletSignerName,
			cn:         "system:node:foo",
			orgs:       []string{"system:nodes"},
			requestor:  "system:node:foo",
			usages: []capi.KeyUsage{
				capi.UsageKeyEncipherment,
				capi.UsageDigitalSignature,
				capi.UsageClientAuth,
			},
		}
		c(&b)
		t.Run(fmt.Sprintf("csr:%#v", b), func(t *testing.T) {
			csr := makeFancyTestCsr(b)
			x509cr, err := k8s_certificates_v1.ParseCSR(csr.Spec.Request)
			if err != nil {
				t.Errorf("unexpected err: %v", err)
			}
			if recognizeFunc(csr, x509cr) != shouldRecognize {
				t.Errorf("expected recognized to be %v", shouldRecognize)
			}
		})
		// 重置 builder，再运行一遍不含 UsageKeyEncipherment 的用例。
		d := csrBuilder{
			signerName: capi.KubeAPIServerClientKubeletSignerName,
			cn:         "system:node:foo",
			orgs:       []string{"system:nodes"},
			requestor:  "system:node:foo",
			usages: []capi.KeyUsage{
				capi.UsageDigitalSignature,
				capi.UsageClientAuth,
			},
		}
		c(&d)
		t.Run(fmt.Sprintf("csr:%#v", d), func(t *testing.T) {
			csr := makeFancyTestCsr(d)
			x509cr, err := k8s_certificates_v1.ParseCSR(csr.Spec.Request)
			if err != nil {
				t.Errorf("unexpected err: %v", err)
			}
			if recognizeFunc(csr, x509cr) != shouldRecognize {
				t.Errorf("expected recognized to be %v", shouldRecognize)
			}
		})
	}
}

// 非密码学安全的随机数，仅用于加速测试。
// DO NOT COPY THIS CODE
var insecureRand = rand.New(rand.NewSource(0))

// makeTestCsr 构造一个仅包含 CN 的默认测试 CSR。
func makeTestCsr() *capi.CertificateSigningRequest {
	return makeFancyTestCsr(csrBuilder{cn: "test-cert"})
}

// csrBuilder 汇总构造测试 CSR 所需的字段，便于各用例只覆盖关心的部分。
type csrBuilder struct {
	cn         string
	orgs       []string
	requestor  string
	usages     []capi.KeyUsage
	dns        []string
	emails     []string
	ips        []net.IP
	signerName string
}

// makeFancyTestCsr 根据 csrBuilder 生成带真实 PEM 编码签名请求的 CSR 对象。
// 使用固定随机源仅为速度考虑，生成的密钥不具备安全性，不能用于生产。
func makeFancyTestCsr(b csrBuilder) *capi.CertificateSigningRequest {
	pk, err := ecdsa.GenerateKey(elliptic.P256(), insecureRand)
	if err != nil {
		panic(err)
	}
	csrb, err := x509.CreateCertificateRequest(insecureRand, &x509.CertificateRequest{
		Subject: pkix.Name{
			CommonName:   b.cn,
			Organization: b.orgs,
		},
		DNSNames:       b.dns,
		EmailAddresses: b.emails,
		IPAddresses:    b.ips,
	}, pk)
	if err != nil {
		panic(err)
	}
	return &capi.CertificateSigningRequest{
		Spec: capi.CertificateSigningRequestSpec{
			Username:   b.requestor,
			Usages:     b.usages,
			SignerName: b.signerName,
			Request:    pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrb}),
		},
	}
}
