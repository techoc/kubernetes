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

package authority

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"math/big"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	capi "k8s.io/api/certificates/v1"
)

// TestCertificateAuthority 表驱动验证 CertificateAuthority.Sign 的证书签发行为：
// 覆盖颁发者信息、用途映射、有效期回退与截断、URI SAN 透传，以及签名器过期的拒绝路径。
// 每个用例都通过比较签发证书的关键字段或错误消息，锁定对外可见的签发契约。
func TestCertificateAuthority(t *testing.T) {
	// 生成测试用 CA 密钥对；后续所有用例共享同一 CA，便于断言 Issuer 等字段。
	caKey, err := ecdsa.GenerateKey(elliptic.P224(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	// 固定 now，避免有效期断言因时钟推进产生偶发抖动。
	now := time.Now()
	nowFunc := func() time.Time { return now }
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(42),
		Subject: pkix.Name{
			CommonName: "test-ca",
		},
		NotBefore:             now.Add(-24 * time.Hour),
		NotAfter:              now.Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	// 自签名生成 CA 证书，再解析回结构体以获得真实的 Subject/SubjectKeyId 等字段。
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, caKey.Public(), caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}

	uri, err := url.Parse("help://me@what:8080/where/when?why=true")
	if err != nil {
		t.Fatal(err)
	}

	// cr 允许注入 CSR 字段（如 URI SAN）；mutateCA 用于构造异常 CA 状态（如已过期）。
	// want 是期望的证书字段子集——ignoreUnset 会让未设置的字段不参与比较，
	// 因此断言表达的是“签发证书是期望值的衍生”，而非逐字段全等。
	// wantErr 非空表示该用例应被策略拒绝，且错误消息需精确匹配。
	tests := []struct {
		name     string
		cr       x509.CertificateRequest
		policy   SigningPolicy
		mutateCA func(ca *CertificateAuthority)

		want    x509.Certificate
		wantErr string
	}{
		{
			// 验证签发证书继承 CA 的 Issuer/AuthorityKeyId，并应用 TTL 得到有效期。
			name:   "ca info",
			policy: PermissiveSigningPolicy{TTL: time.Hour, Now: nowFunc},
			want: x509.Certificate{
				Issuer:                caCert.Subject,
				AuthorityKeyId:        caCert.SubjectKeyId,
				NotBefore:             now,
				NotAfter:              now.Add(1 * time.Hour),
				BasicConstraintsValid: true,
			},
		},
		{
			// 验证 signing 用法被映射为数字签名位，防止签出权限不符的证书。
			name:   "key usage",
			policy: PermissiveSigningPolicy{TTL: time.Hour, Usages: []capi.KeyUsage{"signing"}, Now: nowFunc},
			want: x509.Certificate{
				NotBefore:             now,
				NotAfter:              now.Add(1 * time.Hour),
				BasicConstraintsValid: true,
				KeyUsage:              x509.KeyUsageDigitalSignature,
			},
		},
		{
			// 验证 client auth 映射为扩展用途而非普通 KeyUsage。
			name:   "ext key usage",
			policy: PermissiveSigningPolicy{TTL: time.Hour, Usages: []capi.KeyUsage{"client auth"}, Now: nowFunc},
			want: x509.Certificate{
				NotBefore:             now,
				NotAfter:              now.Add(1 * time.Hour),
				BasicConstraintsValid: true,
				ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
			},
		},
		{
			// 无 Short 时回退时间缩短有效期窗口，防止时钟偏差导致证书提前失效。
			name:   "backdate without short",
			policy: PermissiveSigningPolicy{TTL: time.Hour, Backdate: 5 * time.Minute, Now: nowFunc},
			want: x509.Certificate{
				NotBefore:             now.Add(-5 * time.Minute),
				NotAfter:              now.Add(55 * time.Minute),
				BasicConstraintsValid: true,
			},
		},
		{
			// TTL 小于 Backdate 时允许过期时间在 now 之前，锁定这一边界行为。
			name:   "backdate without short and super small ttl",
			policy: PermissiveSigningPolicy{TTL: time.Minute, Backdate: 5 * time.Minute, Now: nowFunc},
			want: x509.Certificate{
				NotBefore:             now.Add(-5 * time.Minute),
				NotAfter:              now.Add(-4 * time.Minute),
				BasicConstraintsValid: true,
			},
		},
		{
			// 存在 Short 时短证书不受回退影响，有效期仍为完整 TTL。
			name:   "backdate with short",
			policy: PermissiveSigningPolicy{TTL: time.Hour, Backdate: 5 * time.Minute, Short: 8 * time.Hour, Now: nowFunc},
			want: x509.Certificate{
				NotBefore:             now.Add(-5 * time.Minute),
				NotAfter:              now.Add(time.Hour),
				BasicConstraintsValid: true,
			},
		},
		{
			// 同上，验证极小 TTL 与 Short 共存时不会错误截断。
			name:   "backdate with short and super small ttl",
			policy: PermissiveSigningPolicy{TTL: time.Minute, Backdate: 5 * time.Minute, Short: 8 * time.Hour, Now: nowFunc},
			want: x509.Certificate{
				NotBefore:             now.Add(-5 * time.Minute),
				NotAfter:              now.Add(time.Minute),
				BasicConstraintsValid: true,
			},
		},
		{
			// TTL 超过 Short 阈值时回退生效，验证阈值分界两侧的行为差异。
			name:   "backdate with short but longer ttl",
			policy: PermissiveSigningPolicy{TTL: 24 * time.Hour, Backdate: 5 * time.Minute, Short: 8 * time.Hour, Now: nowFunc},
			want: x509.Certificate{
				NotBefore:             now.Add(-5 * time.Minute),
				NotAfter:              now.Add(24*time.Hour - 5*time.Minute),
				BasicConstraintsValid: true,
			},
		},
		{
			// 证书有效期被 CA 剩余寿命截断，防止签出的证书比 CA 更晚过期。
			name:   "truncate expiration",
			policy: PermissiveSigningPolicy{TTL: 48 * time.Hour, Now: nowFunc},
			want: x509.Certificate{
				NotBefore:             now,
				NotAfter:              now.Add(24 * time.Hour),
				BasicConstraintsValid: true,
			},
		},
		{
			// CSR 中的 URI SAN 应原样透传到签发证书，保证身份信息不丢失。
			name:   "uri sans",
			policy: PermissiveSigningPolicy{TTL: time.Hour, Now: nowFunc},
			cr: x509.CertificateRequest{
				URIs: []*url.URL{uri},
			},
			want: x509.Certificate{
				URIs:                  []*url.URL{uri},
				NotBefore:             now,
				NotAfter:              now.Add(1 * time.Hour),
				BasicConstraintsValid: true,
			},
		},
		{
			// CA 已过期时必须拒绝签名，避免链校验失败。
			name:   "expired ca",
			policy: PermissiveSigningPolicy{TTL: time.Hour, Now: nowFunc},
			mutateCA: func(ca *CertificateAuthority) {
				ca.Certificate.NotAfter = now // 模拟 CA 已过期
			},
			wantErr: "the signer has expired: NotAfter=" + now.String(),
		},
		{
			// CA 刚过期但有回退时，签发结果也会落在过去，策略应给出不同的拒绝原因。
			name:   "expired ca with backdate",
			policy: PermissiveSigningPolicy{TTL: time.Hour, Backdate: 5 * time.Minute, Now: nowFunc},
			mutateCA: func(ca *CertificateAuthority) {
				ca.Certificate.NotAfter = now // 模拟 CA 已过期
			},
			wantErr: "refusing to sign a certificate that expired in the past: NotAfter=" + now.String(),
		},
	}

	crKey, err := ecdsa.GenerateKey(elliptic.P224(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// 浅拷贝 CA 证书，使 mutateCA 的修改仅作用于当前用例，避免污染其他用例。
			caCertShallowCopy := *caCert

			ca := &CertificateAuthority{
				Certificate: &caCertShallowCopy,
				PrivateKey:  caKey,
			}

			if test.mutateCA != nil {
				test.mutateCA(ca)
			}

			// 用注入的 CSR 模板生成真实请求，确保 URIs 等字段进入签名流程。
			csr, err := x509.CreateCertificateRequest(rand.Reader, &test.cr, crKey)
			if err != nil {
				t.Fatal(err)
			}

			certDER, err := ca.Sign(csr, test.policy)
			// 期望失败的用例只比对错误字符串，精确的错误信息有助于定位拒绝原因。
			if len(test.wantErr) > 0 {
				if errStr := errString(err); test.wantErr != errStr {
					t.Fatalf("expected error %s but got %s", test.wantErr, errStr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}

			cert, err := x509.ParseCertificate(certDER)
			if err != nil {
				t.Fatal(err)
			}

			opts := cmp.Options{
				// 这些字段由底层 x509 库生成，不属于本测试关注的签发策略输出。
				cmpopts.IgnoreFields(x509.Certificate{},
					"SignatureAlgorithm",
					"PublicKeyAlgorithm",
					"Version",
					"MaxPathLen",
				),
				// 忽略 want 中未设置的字段，使断言聚焦于显式声明的期望子集。
				ignoreUnset(),
				// DER 编码只保留秒级精度，比较前统一截断以免纳秒差异造成误报。
				cmp.Transformer("RoundTime", func(x time.Time) time.Time {
					return x.Truncate(time.Second)
				}),
				// URL 指针不同但语义相同的 URI 应视为相等。
				cmp.Comparer(func(x, y *url.URL) bool {
					return ((x == nil) && (y == nil)) || x.String() == y.String()
				}),
			}
			// 全量比较证书字段，任何未预期的字段差异都会导致失败。
			if !cmp.Equal(*cert, test.want, opts) {
				t.Errorf("unexpected diff: %v", cmp.Diff(*cert, test.want, opts))
			}
		})
	}
}

// ignoreUnset 返回一个 cmp 选项，忽略比较右侧未设置的字段。
// 测试中用它断言“实际对象是期望值的衍生”：只校验显式声明的字段，
// 其余由底层库自动填充的字段不参与比较。
func ignoreUnset() cmp.Option {
	return cmp.Options{
		// 忽略 v2（期望值）中未设置的字段：空切片/空 map、空字符串和 nil 指针等。
		cmp.FilterPath(func(path cmp.Path) bool {
			_, v2 := path.Last().Values()
			switch v2.Kind() {
			case reflect.Slice, reflect.Map:
				if v2.IsNil() || v2.Len() == 0 {
					return true
				}
			case reflect.String:
				if v2.Len() == 0 {
					return true
				}
			case reflect.Interface, reflect.Pointer:
				if v2.IsNil() {
					return true
				}
			}
			return false
		}, cmp.Ignore()),
		// 忽略期望值中不存在的 map 条目，允许实际对象包含额外的键。
		cmp.FilterPath(func(path cmp.Path) bool {
			switch i := path.Last().(type) {
			case cmp.MapIndex:
				if _, v2 := i.Values(); !v2.IsValid() {
					fmt.Println("E")
					return true
				}
			}
			return false
		}, cmp.Ignore()),
	}
}

// errString 将错误转换为用于断言的字符串，nil 错误返回空串。
// 这样期望失败的用例可以直接与 wantErr 做字符串比较，无需分支处理 nil。
func errString(err error) string {
	if err == nil {
		return ""
	}

	return err.Error()
}
