/*
Copyright 2021 The Kubernetes Authors.

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

package cert_test

import (
	cryptorand "crypto/rand"
	"crypto/rsa"
	"testing"

	"k8s.io/client-go/util/cert"
)

const COMMON_NAME = "foo.example.com"

// TestSelfSignedCertHasSAN 验证自签证书中存在 SAN。
// 在生成的自签证书中加入 SAN。Go 1.15+ 默认要求 SAN 才认为证书有效，因此它
// 默认关闭回退到 Common Name 的行为。
//
// 回归点：Go 1.15 起不再回退到 Common Name 校验，因此自签证书必须带 SAN，
// 否则 TLS 校验会失败。
func TestSelfSignedCertHasSAN(t *testing.T) {
	key, err := rsa.GenerateKey(cryptorand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa key failed to generate: %v", err)
	}
	selfSignedCert, err := cert.NewSelfSignedCACert(cert.Config{CommonName: COMMON_NAME}, key)
	if err != nil {
		t.Fatalf("self signed certificate failed to generate: %v", err)
	}
	if len(selfSignedCert.DNSNames) == 0 {
		t.Fatalf("self signed certificate has zero DNS names.")
	}
}
