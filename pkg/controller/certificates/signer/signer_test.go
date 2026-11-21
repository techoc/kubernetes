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

package signer

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"os"

	"math/rand"
	"reflect"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	capi "k8s.io/api/certificates/v1"
	"k8s.io/client-go/kubernetes/fake"
	testclient "k8s.io/client-go/testing"
	"k8s.io/client-go/util/cert"
	"k8s.io/client-go/util/certificate/csr"
	capihelper "k8s.io/kubernetes/pkg/apis/certificates/v1"
	"k8s.io/kubernetes/pkg/controller/certificates"
	testingclock "k8s.io/utils/clock/testing"
)

// ignoreUnset 返回一个比较选项，用于忽略比较右侧（v2）未设置的字段。
// 测试中可用它断言某个对象是另一对象的派生结果：派生对象可能未填全原对象的
// 所有字段，此时只比对左侧明确设置的内容更有意义。
func ignoreUnset() cmp.Option {
	return cmp.Options{
		// 忽略 v2 中未设置的字段：空切片/空 map、空字符串、nil 接口/指针
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
		// 忽略 v2 中不存在的 map 条目（索引值为无效值，说明右侧没有该键）
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

// TestSigner 验证 signer 能基于测试 CA 和 CSR 签发证书：
// 使用假时钟固定时间，断言签发出的证书主体、密钥用法、扩展密钥用法和时间边界
// 都符合预期（包括 -5 分钟的回拨和 1 小时的 TTL）。
func TestSigner(t *testing.T) {
	fakeClock := testingclock.FakeClock{}

	s, err := newSigner("kubernetes.io/legacy-unknown", "./testdata/ca.crt", "./testdata/ca.key", nil, 1*time.Hour)
	if err != nil {
		t.Fatalf("failed to create signer: %v", err)
	}

	csrb, err := os.ReadFile("./testdata/kubelet.csr")
	if err != nil {
		t.Fatalf("failed to read CSR: %v", err)
	}
	x509cr, err := capihelper.ParseCSR(csrb)
	if err != nil {
		t.Fatalf("failed to parse CSR: %v", err)
	}

	certData, err := s.sign(x509cr, []capi.KeyUsage{
		capi.UsageSigning,
		capi.UsageKeyEncipherment,
		capi.UsageServerAuth,
		capi.UsageClientAuth,
	},
		// 请求超过 TTL 的时长会被忽略，签发的证书仍按配置的 1 小时 TTL 计算过期时间。
		csr.DurationToExpirationSeconds(3*time.Hour),
		fakeClock.Now,
	)
	if err != nil {
		t.Fatalf("failed to sign CSR: %v", err)
	}
	if len(certData) == 0 {
		t.Fatalf("expected a certificate after signing")
	}

	certs, err := cert.ParseCertsPEM(certData)
	if err != nil {
		t.Fatalf("failed to parse certificate: %v", err)
	}
	if len(certs) != 1 {
		t.Fatalf("expected one certificate")
	}

	want := x509.Certificate{
		Version: 3,
		Subject: pkix.Name{
			CommonName:   "system:node:k-a-node-s36b",
			Organization: []string{"system:nodes"},
		},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
		NotBefore:             fakeClock.Now().Add(-5 * time.Minute),
		NotAfter:              fakeClock.Now().Add(1 * time.Hour),
		PublicKeyAlgorithm:    x509.ECDSA,
		SignatureAlgorithm:    x509.SHA256WithRSA,
		MaxPathLen:            -1,
	}

	if !cmp.Equal(*certs[0], want, ignoreUnset()) {
		t.Errorf("unexpected diff: %v", cmp.Diff(certs[0], want, ignoreUnset()))
	}
}

// TestHandle 覆盖 signer.handle 针对不同 signerName 和 CSR 状态的处理：
// 合法且已批准的 CSR 会被签发（证书写入 status），非法用法、已失败、未批准
// 或无法识别的 signer 则不会产生任何 Update 动作。每个用例通过 verify 对
// fake client 记录到的 Action 做断言。
func TestHandle(t *testing.T) {
	cases := []struct {
		name string
		// 生成 CSR 时使用的主体与用法参数
		commonName string
		dnsNames   []string
		org        []string
		usages     []capi.KeyUsage
		// 生成的 CSR 是否标记为已批准（未批准的 CSR 不应被签发）
		approved bool
		// 生成的 CSR 是否标记为失败（已失败的 CSR 不应被签发）
		failed bool
		// 生成 CSR 上设置的 signerName
		signerName string
		// 为 true 时期望 handle 返回错误
		err bool
		// 为 true 时期望构造 signer 阶段就失败
		constructionErr bool
		// 额外的校验函数，接收 fake client 记录到的所有 Action
		verify func(*testing.T, []testclient.Action)
	}{
		{
			name:       "should sign if signerName is kubernetes.io/kube-apiserver-client",
			signerName: "kubernetes.io/kube-apiserver-client",
			commonName: "hello-world",
			org:        []string{"some-org"},
			usages:     []capi.KeyUsage{capi.UsageClientAuth, capi.UsageDigitalSignature, capi.UsageKeyEncipherment},
			approved:   true,
			// 合法客户端 CSR 应恰好产生一次 Update（写回签发的证书）。
			verify: func(t *testing.T, as []testclient.Action) {
				if len(as) != 1 {
					t.Errorf("expected one Update action but got %d", len(as))
					return
				}
				csr := as[0].(testclient.UpdateAction).GetObject().(*capi.CertificateSigningRequest)
				// 以 status.Certificate 是否非空判断是否真的签发。
				if len(csr.Status.Certificate) == 0 {
					t.Errorf("expected certificate to be issued but it was not")
				}
			},
		},
		{
			name:       "should sign without key encipherment if signerName is kubernetes.io/kube-apiserver-client",
			signerName: "kubernetes.io/kube-apiserver-client",
			commonName: "hello-world",
			org:        []string{"some-org"},
			usages:     []capi.KeyUsage{capi.UsageClientAuth, capi.UsageDigitalSignature},
			approved:   true,
			verify: func(t *testing.T, as []testclient.Action) {
				if len(as) != 1 {
					t.Errorf("expected one Update action but got %d", len(as))
					return
				}
				csr := as[0].(testclient.UpdateAction).GetObject().(*capi.CertificateSigningRequest)
				// 不含 keyEncipherment 的客户端 CSR 同样应被签发。
				if len(csr.Status.Certificate) == 0 {
					t.Errorf("expected certificate to be issued but it was not")
				}
			},
		},
		{
			name:       "should refuse to sign if signerName is kubernetes.io/kube-apiserver-client and contains an unexpected usage",
			signerName: "kubernetes.io/kube-apiserver-client",
			commonName: "hello-world",
			org:        []string{"some-org"},
			usages:     []capi.KeyUsage{capi.UsageServerAuth, capi.UsageClientAuth, capi.UsageDigitalSignature, capi.UsageKeyEncipherment},
			approved:   true,
			// 含非法用法（server auth）的 CSR 不应签发，而应写入 Failed condition。
			verify: func(t *testing.T, as []testclient.Action) {
				if len(as) != 1 {
					t.Errorf("expected one Update action but got %d", len(as))
					return
				}
				csr := as[0].(testclient.UpdateAction).GetObject().(*capi.CertificateSigningRequest)
				// 未签发时 status.Certificate 必须为空。
				if len(csr.Status.Certificate) != 0 {
					t.Errorf("expected no certificate to be issued")
				}
				// 通过 Failed condition 而非错误返回值来标记处理失败。
				if !certificates.HasTrueCondition(csr, capi.CertificateFailed) {
					t.Errorf("expected Failed condition")
				}
			},
		},
		{
			name:       "should sign if signerName is kubernetes.io/kube-apiserver-client-kubelet",
			signerName: "kubernetes.io/kube-apiserver-client-kubelet",
			commonName: "system:node:hello-world",
			org:        []string{"system:nodes"},
			usages:     []capi.KeyUsage{capi.UsageClientAuth, capi.UsageDigitalSignature, capi.UsageKeyEncipherment},
			approved:   true,
			// 合法 kubelet 客户端 CSR 应被签发。
			verify: func(t *testing.T, as []testclient.Action) {
				if len(as) != 1 {
					t.Errorf("expected one Update action but got %d", len(as))
					return
				}
				csr := as[0].(testclient.UpdateAction).GetObject().(*capi.CertificateSigningRequest)
				if len(csr.Status.Certificate) == 0 {
					t.Errorf("expected certificate to be issued but it was not")
				}
			},
		},
		{
			name:       "should sign without usage key encipherment if signerName is kubernetes.io/kube-apiserver-client-kubelet",
			signerName: "kubernetes.io/kube-apiserver-client-kubelet",
			commonName: "system:node:hello-world",
			org:        []string{"system:nodes"},
			usages:     []capi.KeyUsage{capi.UsageClientAuth, capi.UsageDigitalSignature},
			approved:   true,
			// 不含 keyEncipherment 的 kubelet 客户端 CSR 同样应被签发。
			verify: func(t *testing.T, as []testclient.Action) {
				if len(as) != 1 {
					t.Errorf("expected one Update action but got %d", len(as))
					return
				}
				csr := as[0].(testclient.UpdateAction).GetObject().(*capi.CertificateSigningRequest)
				if len(csr.Status.Certificate) == 0 {
					t.Errorf("expected certificate to be issued but it was not")
				}
			},
		},
		{
			name:       "should sign if signerName is kubernetes.io/legacy-unknown",
			signerName: "kubernetes.io/legacy-unknown",
			approved:   true,
			// legacy-unknown 对内容不做严格校验，合法 CSR 应被签发。
			verify: func(t *testing.T, as []testclient.Action) {
				if len(as) != 1 {
					t.Errorf("expected one Update action but got %d", len(as))
					return
				}
				csr := as[0].(testclient.UpdateAction).GetObject().(*capi.CertificateSigningRequest)
				if len(csr.Status.Certificate) == 0 {
					t.Errorf("expected certificate to be issued but it was not")
				}
			},
		},
		{
			name:       "should sign if signerName is kubernetes.io/kubelet-serving",
			signerName: "kubernetes.io/kubelet-serving",
			commonName: "system:node:testnode",
			org:        []string{"system:nodes"},
			usages:     []capi.KeyUsage{capi.UsageServerAuth, capi.UsageDigitalSignature, capi.UsageKeyEncipherment},
			dnsNames:   []string{"example.com"},
			approved:   true,
			// 合法 kubelet serving CSR 应被签发。
			verify: func(t *testing.T, as []testclient.Action) {
				if len(as) != 1 {
					t.Errorf("expected one Update action but got %d", len(as))
					return
				}
				csr := as[0].(testclient.UpdateAction).GetObject().(*capi.CertificateSigningRequest)
				if len(csr.Status.Certificate) == 0 {
					t.Errorf("expected certificate to be issued but it was not")
				}
			},
		},
		{
			name:       "should sign without usage key encipherment if signerName is kubernetes.io/kubelet-serving",
			signerName: "kubernetes.io/kubelet-serving",
			commonName: "system:node:testnode",
			org:        []string{"system:nodes"},
			usages:     []capi.KeyUsage{capi.UsageServerAuth, capi.UsageDigitalSignature},
			dnsNames:   []string{"example.com"},
			approved:   true,
			// 不含 keyEncipherment 的 serving CSR 同样应被签发。
			verify: func(t *testing.T, as []testclient.Action) {
				if len(as) != 1 {
					t.Errorf("expected one Update action but got %d", len(as))
					return
				}
				csr := as[0].(testclient.UpdateAction).GetObject().(*capi.CertificateSigningRequest)
				if len(csr.Status.Certificate) == 0 {
					t.Errorf("expected certificate to be issued but it was not")
				}
			},
		},
		{
			name:       "should do nothing if failed",
			signerName: "kubernetes.io/kubelet-serving",
			commonName: "system:node:testnode",
			org:        []string{"system:nodes"},
			usages:     []capi.KeyUsage{capi.UsageServerAuth, capi.UsageDigitalSignature, capi.UsageKeyEncipherment},
			dnsNames:   []string{"example.com"},
			approved:   true,
			failed:     true,
			// 已失败的 CSR 即使已批准也不处理，不应产生任何 Action。
			verify: func(t *testing.T, as []testclient.Action) {
				if len(as) != 0 {
					t.Errorf("expected no action to be taken")
				}
			},
		},
		{
			name:            "should do nothing if an unrecognised signerName is used",
			signerName:      "kubernetes.io/not-recognised",
			constructionErr: true,
			approved:        true,
			// 无法识别的 signerName 在构造阶段就失败，因而不会有 Action 产生。
			verify: func(t *testing.T, as []testclient.Action) {
				if len(as) != 0 {
					t.Errorf("expected no action to be taken")
				}
			},
		},
		{
			name:       "should do nothing if not approved",
			signerName: "kubernetes.io/kubelet-serving",
			// 未批准的 CSR 直接跳过，不签发也不写入任何 condition。
			verify: func(t *testing.T, as []testclient.Action) {
				if len(as) != 0 {
					t.Errorf("expected no action to be taken")
				}
			},
		},
		{
			name:            "should do nothing if signerName does not start with kubernetes.io",
			signerName:      "example.com/sample-name",
			constructionErr: true,
			approved:        true,
			// 无法识别的 signerName 在构造阶段就失败，因而不会有 Action 产生。
			verify: func(t *testing.T, as []testclient.Action) {
				if len(as) != 0 {
					t.Errorf("expected no action to be taken")
				}
			},
		},
		{
			name:            "should do nothing if signerName starts with kubernetes.io but is unrecognised",
			signerName:      "kubernetes.io/not-a-real-signer",
			constructionErr: true,
			approved:        true,
			// 无法识别的 signerName 在构造阶段就失败，因而不会有 Action 产生。
			verify: func(t *testing.T, as []testclient.Action) {
				if len(as) != 0 {
					t.Errorf("expected no action to be taken")
				}
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			client := &fake.Clientset{}
			// 用 fake client 构造 signer，便于事后检查其产生的 Action。
			s, err := newSigner(c.signerName, "./testdata/ca.crt", "./testdata/ca.key", client, 1*time.Hour)
			switch {
			// 期望构造失败且确实失败：构造阶段没有 client 交互，直接结束该子用例。
			case c.constructionErr && err != nil:
				return
			case c.constructionErr && err == nil:
				t.Fatalf("expected failure during construction of controller")
			case !c.constructionErr && err != nil:
				t.Fatalf("failed to create signer: %v", err)

			case !c.constructionErr && err == nil:
				// 构造符合预期，继续执行后续测试逻辑。
			}

			csr := makeTestCSR(csrBuilder{cn: c.commonName, signerName: c.signerName, approved: c.approved, failed: c.failed, usages: c.usages, org: c.org, dnsNames: c.dnsNames})
			ctx := context.TODO()
			if err := s.handle(ctx, csr); err != nil && !c.err {
				t.Errorf("unexpected err: %v", err)
			}
			// 通过 fake client 记录的 Action 校验副作用，而非仅依赖返回错误。
			c.verify(t, client.Actions())
		})
	}
}

// insecureRand 使用固定种子的非密码学随机数，以加快测试并保证结果可复现。
// 仅供测试使用，DO NOT COPY THIS CODE。
var insecureRand = rand.New(rand.NewSource(0))

// csrBuilder 汇总构造测试 CSR 所需的字段，方便表驱动用例声明。
type csrBuilder struct {
	cn         string          // CommonName，节点场景需符合 system:node:<name> 格式
	dnsNames   []string        // DNS SAN 列表
	org        []string        // Organization 列表
	signerName string          // CSR 上声明的 signerName
	approved   bool            // 是否追加 CertificateApproved condition
	failed     bool            // 是否追加 CertificateFailed condition
	usages     []capi.KeyUsage // 请求的密钥用法
}

// makeTestCSR 根据 csrBuilder 生成测试用 CSR：用固定随机源生成 P-256 密钥，
// 创建真实的 x509 证书请求并 PEM 编码，再按需要添加 signerName 和状态 condition。
func makeTestCSR(b csrBuilder) *capi.CertificateSigningRequest {
	pk, err := ecdsa.GenerateKey(elliptic.P256(), insecureRand)
	if err != nil {
		panic(err)
	}
	csrb, err := x509.CreateCertificateRequest(insecureRand, &x509.CertificateRequest{
		Subject: pkix.Name{
			CommonName:   b.cn,
			Organization: b.org,
		},
		DNSNames: b.dnsNames,
	}, pk)
	if err != nil {
		panic(err)
	}
	csr := &capi.CertificateSigningRequest{
		Spec: capi.CertificateSigningRequestSpec{
			Request: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrb}),
			Usages:  b.usages,
		},
	}
	if b.signerName != "" {
		csr.Spec.SignerName = b.signerName
	}
	if b.approved {
		csr.Status.Conditions = append(csr.Status.Conditions, capi.CertificateSigningRequestCondition{
			Type: capi.CertificateApproved,
		})
	}
	if b.failed {
		csr.Status.Conditions = append(csr.Status.Conditions, capi.CertificateSigningRequestCondition{
			Type: capi.CertificateFailed,
		})
	}
	return csr
}

// Test_signer_duration 验证 duration 对 expirationSeconds 的边界处理：
// 请求时长可被接受（短于 TTL），但不得超过 TTL，也不能低于 10 分钟的硬下限；
// 未指定 expirationSeconds 时回退到配置的 certTTL。
func Test_signer_duration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		certTTL           time.Duration
		expirationSeconds *int32 // CSR 请求的过期秒数，nil 表示未指定
		want              time.Duration
	}{
		{
			name:              "can request shorter duration than TTL",
			certTTL:           time.Hour,
			expirationSeconds: csr.DurationToExpirationSeconds(30 * time.Minute),
			want:              30 * time.Minute,
		},
		{
			name:              "cannot request longer duration than TTL",
			certTTL:           time.Hour,
			expirationSeconds: csr.DurationToExpirationSeconds(3 * time.Hour),
			want:              time.Hour,
		},
		{
			name:              "cannot request negative duration",
			certTTL:           time.Hour,
			expirationSeconds: csr.DurationToExpirationSeconds(-time.Minute),
			want:              10 * time.Minute,
		},
		{
			name:              "cannot request duration less than 10 mins",
			certTTL:           time.Hour,
			expirationSeconds: csr.DurationToExpirationSeconds(10*time.Minute - time.Second),
			want:              10 * time.Minute,
		},
		{
			name:              "can request duration of exactly 10 mins",
			certTTL:           time.Hour,
			expirationSeconds: csr.DurationToExpirationSeconds(10 * time.Minute),
			want:              10 * time.Minute,
		},
		{
			name:              "can request duration equal to the default",
			certTTL:           time.Hour,
			expirationSeconds: csr.DurationToExpirationSeconds(time.Hour),
			want:              time.Hour,
		},
		{
			name:              "can choose not to request a duration to get the default",
			certTTL:           time.Hour,
			expirationSeconds: nil,
			want:              time.Hour,
		},
	}
	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// 直接构造 signer，只关注 duration 对 certTTL 与请求值的处理。
			s := &signer{
				certTTL: tt.certTTL,
			}
			if got := s.duration(tt.expirationSeconds); got != tt.want {
				t.Errorf("duration() = %v, want %v", got, tt.want)
			}
		})
	}
}
