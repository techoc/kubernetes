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
// 中文说明：目标是在 Go 默认收紧 x509 校验前，提前发现集群中仍依赖 CN 或 SHA-1
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

	// 中文说明：每个 checker 对应一种已弃用的证书属性；新增检查项只需追加到切片。
	checkers []deprecatedCertificateAttributeChecker
}

// deprecatedCertificateAttributeChecker 同时覆盖两条检测路径：握手失败（错误）
// 与握手成功但证书本身已弃用（peer certificates）。
// 中文说明：两条路径都需要，否则 GODEBUG 开关不同时只能观测到其中一半。
type deprecatedCertificateAttributeChecker interface {
	// CheckRoundTripError returns true if the err is an error specific
	// to this deprecated certificate attribute
	CheckRoundTripError(err error) bool
	// CheckPeerCertificates returns true if the deprecated attribute/value pair
	// was found in a given certificate in the http.Response.TLS.PeerCertificates bundle
	CheckPeerCertificates(certs []*x509.Certificate) bool
	// IncreaseCounter increases the counter internal to this interface
	// Use the req to derive and log information useful for troubleshooting the certificate issue
	IncreaseMetricsCounter(req *http.Request)
}

// counterRaiser is a helper structure to include in certificate deprecation checkers.
// It implements the IncreaseMetricsCounter() method so that, when included in the checker,
// it does not have to be reimplemented.
type counterRaiser struct {
	counter *metrics.Counter
	// programmatic id used in log and audit annotations prefixes
	id string
	// human readable explanation
	reason string
}

func (c *counterRaiser) IncreaseMetricsCounter(req *http.Request) {
	// 中文说明：日志与审计注解都带 hostname，便于定位是哪台后端证书有问题；
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

// NewDeprecatedCertificateRoundTripperWrapperConstructor returns a RoundTripper wrapper that's usable within ClientConfig.Wrap.
//
// It increases the `missingSAN` counter whenever:
//  1. we get a x509.HostnameError with string `x509: certificate relies on legacy Common Name field`
//     which indicates an error caused by the deprecation of Common Name field when veryfing remote
//     hostname
//  2. the server certificate in response contains no SAN. This indicates that this binary run
//     with the GODEBUG=x509ignoreCN=0 in env
//
// It increases the `sha1` counter whenever:
//  1. we get a x509.InsecureAlgorithmError with string `SHA1`
//     which indicates an error caused by an insecure SHA1 signature
//  2. the server certificate in response contains a SHA1WithRSA or ECDSAWithSHA1 signature.
//     This indicates that this binary run with the GODEBUG=x509sha1=1 in env
//
// 中文说明：包装器只观测、不改变握手结果，因此可以安全地挂在任意 client 上；
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
// 中文说明：成功的响应也检查，因为某些环境通过 GODEBUG 暂时放过了不安全的证书，
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

// CheckRoundTripError returns true when we're running w/o GODEBUG=x509ignoreCN=0
// and the client reports a HostnameError about the legacy CN fields
func (c *missingSANChecker) CheckRoundTripError(err error) bool {
	if err != nil && errors.As(err, &x509.HostnameError{}) && strings.Contains(err.Error(), "x509: certificate relies on legacy Common Name field") {
		// increase the count of registered failures due to Go 1.15 x509 cert Common Name deprecation
		return true
	}

	return false
}

// CheckPeerCertificates returns true when the server response contains
// a leaf certificate w/o the SAN extension
func (c *missingSANChecker) CheckPeerCertificates(peerCertificates []*x509.Certificate) bool {
	if len(peerCertificates) > 0 {
		if serverCert := peerCertificates[0]; !hasSAN(serverCert) {
			return true
		}
	}

	return false
}

// hasSAN 通过 OID 2.5.29.17 判断证书是否含 Subject Alternative Name 扩展。
// 中文说明：不依赖解析后的 DNSNames，因为 SAN 可能只包含 IP 或 email。
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

// CheckRoundTripError returns true when we're running w/o GODEBUG=x509sha1=1
// and the client reports an InsecureAlgorithmError about a SHA1 signature
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
		// increase the count of registered failures due to Go 1.18 x509 sha1 signature deprecation
		return true
	}

	return false
}

// CheckPeerCertificates returns true when the server response contains
// a non-root non-self-signed  certificate with a deprecated SHA1 signature
func (c *sha1SignatureChecker) CheckPeerCertificates(peerCertificates []*x509.Certificate) bool {
	// check all received non-self-signed certificates for deprecated signing algorithms
	// 中文说明：自签名根证书使用 SHA1 不在弃用范围内（无法由 CA 重新签发），
	// 因此用 Issuer==Subject 排除。
	for _, cert := range peerCertificates {
		if cert.SignatureAlgorithm == x509.SHA1WithRSA || cert.SignatureAlgorithm == x509.ECDSAWithSHA1 {
			// the SHA-1 deprecation does not involve self-signed root certificates
			if !reflect.DeepEqual(cert.Issuer, cert.Subject) {
				return true
			}
		}
	}

	return false
}
