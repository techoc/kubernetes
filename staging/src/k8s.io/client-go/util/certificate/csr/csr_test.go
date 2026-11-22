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

package csr

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"testing"

	certificates "k8s.io/api/certificates/v1"
)

// TestEnsureCompatible 验证 ensureCompatible 的 signerName 规则：
// 空值视为通配，两个非空值必须完全一致；所有用例还隐含验证 CSR 与私钥的签名匹配。
func TestEnsureCompatible(t *testing.T) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	req := pemWithPrivateKey(privateKey)

	tests := map[string]struct {
		new, orig  *certificates.CertificateSigningRequest
		privateKey interface{}
		err        string
	}{
		// 空值通常来自旧版对象或未指定 signer 的请求，因此视为兼容。
		"nil signerName on 'new' matches any signerName on 'orig'": {
			new: &certificates.CertificateSigningRequest{
				Spec: certificates.CertificateSigningRequestSpec{
					Request: req,
				},
			},
			orig: &certificates.CertificateSigningRequest{
				Spec: certificates.CertificateSigningRequestSpec{
					Request:    req,
					SignerName: "example.com/test",
				},
			},
			privateKey: privateKey,
		},
		// 旧对象未记录 signerName 时同样按通配处理。
		"nil signerName on 'orig' matches any signerName on 'new'": {
			new: &certificates.CertificateSigningRequest{
				Spec: certificates.CertificateSigningRequestSpec{
					Request:    req,
					SignerName: "example.com/test",
				},
			},
			orig: &certificates.CertificateSigningRequest{
				Spec: certificates.CertificateSigningRequestSpec{
					Request: req,
				},
			},
			privateKey: privateKey,
		},
		"signerName on 'orig' matches signerName on 'new'": {
			new: &certificates.CertificateSigningRequest{
				Spec: certificates.CertificateSigningRequestSpec{
					Request:    req,
					SignerName: "example.com/test",
				},
			},
			orig: &certificates.CertificateSigningRequest{
				Spec: certificates.CertificateSigningRequestSpec{
					Request:    req,
					SignerName: "example.com/test",
				},
			},
			privateKey: privateKey,
		},
		// 两个非空 signerName 不同说明请求已不属于同一签发者，必须拒绝复用。
		"signerName on 'orig' does not match signerName on 'new'": {
			new: &certificates.CertificateSigningRequest{
				Spec: certificates.CertificateSigningRequestSpec{
					Request:    req,
					SignerName: "example.com/test",
				},
			},
			orig: &certificates.CertificateSigningRequest{
				Spec: certificates.CertificateSigningRequestSpec{
					Request:    req,
					SignerName: "example.com/not-test",
				},
			},
			privateKey: privateKey,
			err:        `csr signerNames differ: new "example.com/test", orig: "example.com/not-test"`,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := ensureCompatible(test.new, test.orig, test.privateKey)
			if err != nil && test.err == "" {
				t.Errorf("expected no error, but got: %v", err)
			} else if err != nil && test.err != err.Error() {
				t.Errorf("error did not match as expected, got=%v, exp=%s", err, test.err)
			}
			if err == nil && test.err != "" {
				t.Errorf("expected to get an error but got none")
			}
		})
	}
}

// pemWithPrivateKey 用给定私钥生成一个带有固定 Subject 的 CSR（PEM 编码）。
func pemWithPrivateKey(pk crypto.PrivateKey) []byte {
	template := &x509.CertificateRequest{
		Subject: pkix.Name{
			CommonName:   "something",
			Organization: []string{"test"},
		},
	}
	return pemWithTemplate(template, pk)
}

// pemWithTemplate 按模板生成 DER 再编码为 PEM；失败说明测试夹具本身有问题，直接 panic。
func pemWithTemplate(template *x509.CertificateRequest, key crypto.PrivateKey) []byte {
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
