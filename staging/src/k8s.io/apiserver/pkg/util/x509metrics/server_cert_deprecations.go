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

// Package x509metrics 通过包装 http.RoundTripper 观测对端证书中已弃用的属性，
// 并计入指标与审计注解。
//
// 目标是在 Go 默认收紧 x509 校验前，提前发现集群中仍依赖 CN 或 SHA-1
// 的证书，避免升级后突然出现连接失败。
package x509metrics

import (
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"

	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/component-base/metrics"
	"k8s.io/klog/v2"
)

var _ utilnet.RoundTripperWrapper = &x509DeprecatedCertificateMetricsRTWrapper{}

type x509DeprecatedCertificateMetricsRTWrapper struct {
	rt http.RoundTripper

	// 每个 checker 对应一种已弃用的证书属性；新增检查项只需追加到切片。
	checkers []deprecatedCertificateAttributeChecker
}

// deprecatedCertificateAttributeChecker 同时覆盖两条检测路径：握手失败（错误）
// 与握手成功但证书本身已弃用（peer certificates）。两条路径都需要，
// 否则 GODEBUG 开关不同时只能观测到其中一半。
type deprecatedCertificateAttributeChecker interface {
	// err 是与本弃用证书属性相关的错误时，CheckRoundTripError 返回 true
	//
	CheckRoundTripError(err error) bool
	// 在给定证书的 http.Response.TLS.PeerCertificates bundle 中发现
	// 该弃用属性/值对时，CheckPeerCertificates 返回 true
	CheckPeerCertificates(certs []*x509.Certificate) bool
	// IncreaseCounter 增加该接口内部的计数器
	// 用 req 派生并记录有助于排查证书问题的信息
	IncreaseMetricsCounter(req *http.Request)
}

// counterRaiser 是可嵌入到证书弃用检查器中的辅助结构。
// 它实现 IncreaseMetricsCounter() 方法，这样嵌入后
// 就不必重复实现。
type counterRaiser struct {
	counter *metrics.Counter
	// 用于日志与审计注解前缀的编程 ID
	id string
	// 人类可读的说明
	reason string
}

func (c *counterRaiser) IncreaseMetricsCounter(req *http.Request) {
	// 日志与审计注解都带 hostname，便于定位是哪台后端证书有问题；
	// 但 hostname 不进指标标签，避免基数爆炸。
	if req != nil && req.URL != nil {
		if hostname := req.URL.Hostname(); len(hostname) > 0 {
			prefix := fmt.Sprintf("%s.invalid-cert.kubernetes.io", c.id)
			klog.Infof("%s: invalid certificate detected connecting to %q: %s", prefix, hostname, c.reason)
			audit.AddAuditAnnotation(req.Context(), prefix+"/"+hostname, c.reason)
		}
	}
	c.counter.Inc()
}

// NewDeprecatedCertificateRoundTripperWrapperConstructor 返回一个可在 ClientConfig.Wrap
// 中使用的 RoundTripper 包装器。
//
// 以下任一情况都会使 `missingSAN` 计数器增加：
//  1. 出现包含 `x509: certificate relies on legacy Common Name field` 字符串的
//     x509.HostnameError，说明校验远端主机名时因 Common Name 字段被弃用而报错；
//  2. 响应中的服务端证书不含 SAN，说明该二进制运行在 GODEBUG=x509ignoreCN=0 环境下。
//
// 以下任一情况都会使 `sha1` 计数器增加：
//  1. 出现字符串为 `SHA1` 的 x509.InsecureAlgorithmError，即不安全 SHA1 签名导致的错误；
//  2. 响应中的服务端证书使用 SHA1WithRSA 或 ECDSAWithSHA1 签名，
//     说明该二进制运行在 GODEBUG=x509sha1=1 环境下。
//
// 包装器只观测、不改变握手结果，因此可以安全地挂在任意 client 上；
// 目的是在 Go 收紧 x509 校验前，提前发现集群里依赖已弃用证书的组件。
func NewDeprecatedCertificateRoundTripperWrapperConstructor(missingSAN, sha1 *metrics.Counter) func(rt http.RoundTripper) http.RoundTripper {
	return func(rt http.RoundTripper) http.RoundTripper {
		return &x509DeprecatedCertificateMetricsRTWrapper{
			rt: rt,
			checkers: []deprecatedCertificateAttributeChecker{
				NewSANDeprecatedChecker(missingSAN),
				NewSHA1SignatureDeprecatedChecker(sha1),
			},
		}
	}
}

// RoundTrip 先完成真实请求，再分别从“错误”和“成功响应的证书”两个维度检查。
// 成功的响应也检查，因为某些环境通过 GODEBUG 暂时放过了不安全的证书，
// 此时只能从 PeerCertificates 发现风险。
func (w *x509DeprecatedCertificateMetricsRTWrapper) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := w.rt.RoundTrip(req)

	if err != nil {
		for _, checker := range w.checkers {
			if checker.CheckRoundTripError(err) {
				checker.IncreaseMetricsCounter(req)
			}
		}
	} else if resp != nil {
		if resp.TLS != nil && len(resp.TLS.PeerCertificates) > 0 {
			for _, checker := range w.checkers {
				if checker.CheckPeerCertificates(resp.TLS.PeerCertificates) {
					checker.IncreaseMetricsCounter(req)
				}
			}
		}
	}

	return resp, err
}

func (w *x509DeprecatedCertificateMetricsRTWrapper) WrappedRoundTripper() http.RoundTripper {
	return w.rt
}

var _ deprecatedCertificateAttributeChecker = &missingSANChecker{}

// missingSANChecker 检测依赖 CN（Common Name）而不带 SAN 的证书。
type missingSANChecker struct {
	counterRaiser
}

func NewSANDeprecatedChecker(counter *metrics.Counter) *missingSANChecker {
	return &missingSANChecker{
		counterRaiser: counterRaiser{
			counter: counter,
			id:      "missing-san",
			reason:  "relies on a legacy Common Name field instead of the SAN extension for subject validation",
		},
	}
}

// 在未设置 GODEBUG=x509ignoreCN=0 的情况下运行时，CheckRoundTripError 返回 true，
// 且客户端报告了关于旧 CN 字段的 HostnameError
func (c *missingSANChecker) CheckRoundTripError(err error) bool {
	if err != nil && errors.As(err, &x509.HostnameError{}) && strings.Contains(err.Error(), "x509: certificate relies on legacy Common Name field") {
		// 增加因 Go 1.15 x509 证书 Common Name 弃用导致的登记失败计数
		return true
	}

	return false
}

// 服务端响应包含
// 缺少 SAN 扩展的叶子证书时，CheckPeerCertificates 返回 true
func (c *missingSANChecker) CheckPeerCertificates(peerCertificates []*x509.Certificate) bool {
	if len(peerCertificates) > 0 {
		if serverCert := peerCertificates[0]; !hasSAN(serverCert) {
			return true
		}
	}

	return false
}

// hasSAN 通过 OID 2.5.29.17 判断证书是否含 Subject Alternative Name 扩展。
// 不依赖解析后的 DNSNames，因为 SAN 可能只包含 IP 或 email。
func hasSAN(c *x509.Certificate) bool {
	sanOID := []int{2, 5, 29, 17}

	for _, e := range c.Extensions {
		if e.Id.Equal(sanOID) {
			return true
		}
	}
	return false
}

// sha1SignatureChecker 检测使用 SHA-1 签名的非根证书。
type sha1SignatureChecker struct {
	*counterRaiser
}

func NewSHA1SignatureDeprecatedChecker(counter *metrics.Counter) *sha1SignatureChecker {
	return &sha1SignatureChecker{
		counterRaiser: &counterRaiser{
			counter: counter,
			id:      "insecure-sha1",
			reason:  "uses an insecure SHA-1 signature",
		},
	}
}

// 在未设置 GODEBUG=x509sha1=1 的情况下运行时，CheckRoundTripError 返回 true，
// 且客户端报告了关于 SHA1 签名的 InsecureAlgorithmError
func (c *sha1SignatureChecker) CheckRoundTripError(err error) bool {
	var unknownAuthorityError x509.UnknownAuthorityError
	if err == nil {
		return false
	}
	if !errors.As(err, &unknownAuthorityError) {
		return false
	}

	errMsg := err.Error()
	if strIdx := strings.Index(errMsg, "x509: cannot verify signature: insecure algorithm"); strIdx != -1 && strings.Contains(errMsg[strIdx:], "SHA1") {
		// 增加因 Go 1.18 x509 SHA1 签名弃用导致的登记失败计数
		return true
	}

	return false
}

// 服务端响应包含
// 使用已弃用 SHA1 签名的非根、非自签证书时返回 true
func (c *sha1SignatureChecker) CheckPeerCertificates(peerCertificates []*x509.Certificate) bool {
	// 检查所有收到的非自签名证书是否使用了弃用的签名算法。自签名根证书使用
	// SHA1 不在弃用范围内（无法由 CA 重新签发），因此用 Issuer==Subject 排除。
	for _, cert := range peerCertificates {
		if cert.SignatureAlgorithm == x509.SHA1WithRSA || cert.SignatureAlgorithm == x509.ECDSAWithSHA1 {
			// SHA-1 弃用不涉及自签名的根证书
			if !reflect.DeepEqual(cert.Issuer, cert.Subject) {
				return true
			}
		}
	}

	return false
}
