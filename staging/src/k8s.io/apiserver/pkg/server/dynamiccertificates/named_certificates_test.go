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

package dynamiccertificates

import (
	"bytes"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"testing"
	"time"

	netutils "k8s.io/utils/net"

	"github.com/stretchr/testify/assert"
)

// testCertSpec 描述测试证书的自签名参数：host 作为 CN，names/ips 作为 SAN。
type testCertSpec struct {
	host       string
	names, ips []string // 证书中的名字。
}

// namedtestCertSpec 在证书参数之外附带“显式 SNI 名称”（对应
// --tls-sni-cert-key 的显式名字列表）。
type namedtestCertSpec struct {
	testCertSpec
	explicitNames []string // 作为 --tls-sni-cert-key 的显式名字。
}

// TestBuiltNamedCertificates 验证 SNI 证书映射的构建规则：DNS SAN 与显式
// 名称进入映射，IP 地址被忽略；同名时显式名称优先，且映射值必须指向正确
// 的证书（通过证书签名索引校验）。
func TestBuiltNamedCertificates(t *testing.T) {
	tests := []struct {
		certs         []namedtestCertSpec
		explicitNames []string
		expected      map[string]int // 名字到 certs[*] 索引的映射。
		errorString   string
	}{
		{
			// empty certs
			expected: map[string]int{},
		},
		{
			// 只有一个证书。
			certs: []namedtestCertSpec{
				{
					testCertSpec: testCertSpec{
						host: "test.com",
					},
				},
			},
			expected: map[string]int{
				"test.com": 0,
			},
		},
		{
			// 用 IP 作为 CN 会被忽略。
			certs: []namedtestCertSpec{
				{
					testCertSpec: testCertSpec{
						host:  "1.2.3.4",
						names: []string{"test.com"},
					},
				},
			},
			expected: map[string]int{
				"test.com": 0,
			},
		},
		{
			// IP 会被忽略。
			certs: []namedtestCertSpec{
				{
					testCertSpec: testCertSpec{
						host: "test.com",
						ips:  []string{"1.2.3.4"},
					},
				},
			},
			expected: map[string]int{
				"test.com": 0,
			},
		},
		{
			// 两个同名的证书。
			certs: []namedtestCertSpec{
				{
					testCertSpec: testCertSpec{
						host: "test.com",
					},
				},
				{
					testCertSpec: testCertSpec{
						host: "test.com",
					},
				},
			},
			expected: map[string]int{
				"test.com": 0,
			},
		},
		{
			// 两个不同名的证书。
			certs: []namedtestCertSpec{
				{
					testCertSpec: testCertSpec{
						host: "test2.com",
					},
				},
				{
					testCertSpec: testCertSpec{
						host: "test1.com",
					},
				},
			},
			expected: map[string]int{
				"test1.com": 1,
				"test2.com": 0,
			},
		},
		{
			// 显式名称优先于证书内 SAN：同一名称出现冲突时后者胜出。
			// 两个同名证书，显式名字优先。
			certs: []namedtestCertSpec{
				{
					testCertSpec: testCertSpec{
						host: "test.com",
					},
				},
				{
					testCertSpec: testCertSpec{
						host: "test.com",
					},
					explicitNames: []string{"test.com"},
				},
			},
			expected: map[string]int{
				"test.com": 1,
			},
		},
		{
			// 证书部分重叠；IP 会被忽略。
			certs: []namedtestCertSpec{
				{
					testCertSpec: testCertSpec{
						host:  "a",
						names: []string{"a.test.com", "test.com"},
					},
				},
				{
					testCertSpec: testCertSpec{
						host:  "b",
						names: []string{"b.test.com", "test.com"},
					},
				},
			},
			expected: map[string]int{
				"a": 0, "b": 1,
				"a.test.com": 0, "b.test.com": 1,
				"test.com": 0,
			},
		},
		{
			// 通配符与普通名称可共存；映射键保留用户书写形式，不做规范化。
			// wildcards
			certs: []namedtestCertSpec{
				{
					testCertSpec: testCertSpec{
						host:  "a",
						names: []string{"a.test.com", "test.com"},
					},
					explicitNames: []string{"*.test.com", "test.com"},
				},
				{
					testCertSpec: testCertSpec{
						host:  "b",
						names: []string{"b.test.com", "test.com"},
					},
					explicitNames: []string{"dev.test.com", "test.com"},
				}},
			expected: map[string]int{
				"test.com":     0,
				"*.test.com":   0,
				"dev.test.com": 1,
			},
		},
	}

NextTest:
	for i, test := range tests {
		var sniCerts []SNICertKeyContentProvider
		bySignature := map[string]int{} // 按证书签名在 test.certs 中的索引。
		for j, c := range test.certs {
			certProvider, err := createTestTLSCerts(c.testCertSpec, c.explicitNames)
			if err != nil {
				t.Errorf("%d - failed to create cert %d: %v", i, j, err)
				continue NextTest
			}

			sniCerts = append(sniCerts, certProvider)

			sig, err := certSignature(certProvider)
			if err != nil {
				t.Errorf("%d - failed to get signature for %d: %v", i, j, err)
				continue NextTest
			}
			bySignature[sig] = j
		}

		c := DynamicServingCertificateController{sniCerts: sniCerts}
		content, err := c.newTLSContent()
		assert.NoError(t, err)

		certMap, err := c.BuildNamedCertificates(content.sniCerts)
		if err == nil && len(test.errorString) != 0 {
			t.Errorf("%d - expected no error, got: %v", i, err)
		} else if err != nil && err.Error() != test.errorString {
			t.Errorf("%d - expected error %q, got: %v", i, test.errorString, err)
		} else {
			got := map[string]int{}
			for name, cert := range certMap {
				x509Certs, err := x509.ParseCertificates(cert.Certificate[0])
				assert.NoError(t, err, "%d - invalid certificate for %q", i, name)
				assert.NotEmpty(t, x509Certs, "%d - expected at least one x509 cert in tls cert for %q", i, name)
				got[name] = bySignature[x509CertSignature(x509Certs[0])]
			}

			assert.EqualValues(t, test.expected, got, "%d - wrong certificate map", i)
		}
	}
}

// parseIPList 把字符串 IP 转为 net.IP；使用 ParseIPSloppy 是为了与
// Kubernetes 其他组件对历史遗留地址写法的处理保持一致。
func parseIPList(ips []string) []net.IP {
	var netIPs []net.IP
	for _, ip := range ips {
		netIPs = append(netIPs, netutils.ParseIPSloppy(ip))
	}
	return netIPs
}

// createTestTLSCerts 生成自签名证书并包装成静态 SNI 内容提供者。
func createTestTLSCerts(spec testCertSpec, names []string) (certProvider SNICertKeyContentProvider, err error) {
	certPem, keyPem, err := generateSelfSignedCertKey(spec.host, parseIPList(spec.ips), spec.names)
	if err != nil {
		return nil, err
	}

	return NewStaticSNICertKeyContent("test-cert", certPem, keyPem, names...)
}

// x509CertSignature 用证书签名字节作为测试内的唯一标识，便于把映射结果
// 反查回输入证书的下标。
func x509CertSignature(cert *x509.Certificate) string {
	return base64.StdEncoding.EncodeToString(cert.Signature)
}

// certSignature 从 provider 当前内容解析出 x509 证书并返回其签名。
func certSignature(certProvider CertKeyContentProvider) (string, error) {
	currentCert, currentKey := certProvider.CurrentCertKeyContent()

	tlsCert, err := tls.X509KeyPair(currentCert, currentKey)
	if err != nil {
		return "", err
	}

	x509Certs, err := x509.ParseCertificates(tlsCert.Certificate[0])
	if err != nil {
		return "", err
	}
	return x509CertSignature(x509Certs[0]), nil
}

// generateSelfSignedCertKey 为给定 host 创建自签名证书与私钥。
// Host 可以是 IP 或 DNS 名。
// 还可为证书指定额外的 subject alt name（IP 或 DNS 名）。
// 有效期 100 年、IsCA=true 仅为了让测试证书无需额外信任链即可使用；
// 这不是生产证书生成器。
func generateSelfSignedCertKey(host string, alternateIPs []net.IP, alternateDNS []string) ([]byte, []byte, error) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, nil, err
	}

	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: fmt.Sprintf("%s@%d", host, time.Now().Unix()),
		},
		NotBefore: time.Unix(0, 0),
		NotAfter:  time.Now().Add(time.Hour * 24 * 365 * 100),

		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	if ip := netutils.ParseIPSloppy(host); ip != nil {
		template.IPAddresses = append(template.IPAddresses, ip)
	} else {
		template.DNSNames = append(template.DNSNames, host)
	}

	template.IPAddresses = append(template.IPAddresses, alternateIPs...)
	template.DNSNames = append(template.DNSNames, alternateDNS...)

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return nil, nil, err
	}

	// Generate cert
	certBuffer := bytes.Buffer{}
	if err := pem.Encode(&certBuffer, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}); err != nil {
		return nil, nil, err
	}

	// Generate key
	keyBuffer := bytes.Buffer{}
	if err := pem.Encode(&keyBuffer, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)}); err != nil {
		return nil, nil, err
	}

	return certBuffer.Bytes(), keyBuffer.Bytes(), nil
}
