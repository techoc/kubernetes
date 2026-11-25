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

package pubkeypin

import (
	"crypto/x509"
	_ "embed"
	"encoding/pem"
	"strings"
	"testing"
)

// testCertPEM 是使用 openssl CLI 签发的简单自签名测试证书：
// openssl req -new -newkey rsa:2048 -days 36500 -nodes -x509 -keyout /dev/null -out test.crt
//
//go:embed testdata/test-cert.pem
var testCertPEM string

// expectedHash 可使用 openssl CLI 验证。
const expectedHash = `sha256:345959acb2c3b2feb87d281961c893f62a314207ef02599f1cc4a5fb255480b3`

// testCert2PEM 是按 testCertPEM 相同方式生成的第二个测试证书。
//
//go:embed testdata/test-cert2.pem
var testCert2PEM string

// testCert 从 PEM 常量获取测试用 x509.Certificate。
func testCert(t *testing.T, pemString string) *x509.Certificate {
	// 将示例证书从 PEM 文件解码为 PEM 块。
	pemBlock, _ := pem.Decode([]byte(pemString))
	if pemBlock == nil {
		t.Fatal("failed to parse test certificate PEM")
		return nil
	}

	// 将 PEM 块解析为 x509.Certificate。
	result, err := x509.ParseCertificate(pemBlock.Bytes)
	if err != nil {
		t.Fatalf("failed to parse test certificate: %v", err)
		return nil
	}
	return result
}

func TestSet(t *testing.T) {
	s := NewSet()
	if !s.Empty() {
		t.Error("expected a new set to be empty")
		return
	}
	err := s.Allow("xyz")
	if err == nil || !s.Empty() {
		t.Error("expected allowing junk to fail")
		return
	}

	err = s.Allow("0011223344")
	if err == nil || !s.Empty() {
		t.Error("expected allowing something too short to fail")
		return
	}

	err = s.Allow(expectedHash + expectedHash)
	if err == nil || !s.Empty() {
		t.Error("expected allowing something too long to fail")
		return
	}

	err = s.CheckAny([]*x509.Certificate{testCert(t, testCertPEM)})
	if err == nil {
		t.Error("expected test cert to not be allowed (yet)")
		return
	}

	err = s.Allow(strings.ToUpper(expectedHash))
	if err != nil || s.Empty() {
		t.Error("expected allowing uppercase expectedHash to succeed")
		return
	}

	err = s.CheckAny([]*x509.Certificate{testCert(t, testCertPEM)})
	if err != nil {
		t.Errorf("expected test cert to be allowed, but got back: %v", err)
		return
	}

	err = s.CheckAny([]*x509.Certificate{testCert(t, testCert2PEM)})
	if err == nil {
		t.Error("expected the second test cert to be disallowed")
		return
	}

	s = NewSet() // s = NewSet() // 保持集合为空
	hashes := []string{
		`sha256:0000000000000000000000000000000000000000000000000000000000000000`,
		`sha256:0000000000000000000000000000000000000000000000000000000000000001`,
	}
	err = s.Allow(hashes...)
	if err != nil || len(s.sha256Hashes) != 2 {
		t.Error("expected allowing multiple hashes to succeed")
		return
	}
}

func TestHash(t *testing.T) {
	actualHash := Hash(testCert(t, testCertPEM))
	if actualHash != expectedHash {
		t.Errorf(
			"failed to Hash() to the expected value\n\texpected: %q\n\t  actual: %q",
			expectedHash,
			actualHash,
		)
	}
}
