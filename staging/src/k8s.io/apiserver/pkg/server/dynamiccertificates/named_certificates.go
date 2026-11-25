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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/klog/v2"
	netutils "k8s.io/utils/net"
)

// BuildNamedCertificates 返回按名称索引的 *tls.Certificate map。
// 它适合用于 tls.Config#NamedCertificates。任一证书非法时返回错误；
// len(certs) == 0 时返回 nil。
// 任何一张证书非法都会整体失败，而不是跳过——服务端证书集不完整
// 可能导致部分 SNI 名握手失败，静默忽略更难排查。
func (c *DynamicServingCertificateController) BuildNamedCertificates(sniCerts []sniCertKeyContent) (map[string]*tls.Certificate, error) {
	nameToCertificate := map[string]*tls.Certificate{}
	byNameExplicit := map[string]*tls.Certificate{}

	// 倒序遍历，使靠前的证书在 names map 中取得优先权。
	// 倒序遍历 + 正向覆盖，实现“先声明的证书优先”；配置中靠前的
	// 证书通常是更具体、更新的那张。
	for i := len(sniCerts) - 1; i >= 0; i-- {
		cert, err := tls.X509KeyPair(sniCerts[i].cert, sniCerts[i].key)
		if err != nil {
			return nil, fmt.Errorf("invalid SNI cert keypair [%d/%q]: %v", i, c.sniCerts[i].Name(), err)
		}

		// error is not possible given above call to X509KeyPair
		x509Cert, _ := x509.ParseCertificate(cert.Certificate[0])

		// 显式声明的 SNI 名单独收集，最后统一覆盖自动推导出的名字。
		names := sniCerts[i].sniNames
		for _, name := range names {
			byNameExplicit[name] = &cert
		}

		klog.V(2).InfoS("Loaded SNI cert", "index", i, "certName", c.sniCerts[i].Name(), "certDetail", GetHumanCertDetail(x509Cert))
		if c.eventRecorder != nil {
			c.eventRecorder.Eventf(&corev1.ObjectReference{Name: c.sniCerts[i].Name()}, nil, corev1.EventTypeWarning, "TLSConfigChanged", "SNICertificateReload", "loaded SNI cert [%d/%q]: %s with explicit names %v", i, c.sniCerts[i].Name(), GetHumanCertDetail(x509Cert), names)
		}

		// 未显式配置名字时，从证书的 CN/SAN 推导；显式配置优先于推导值。
		if len(names) == 0 {
			names = getCertificateNames(x509Cert)
			for _, name := range names {
				nameToCertificate[name] = &cert
			}
		}
	}

	// Explicitly set names must override
	for k, v := range byNameExplicit {
		nameToCertificate[k] = v
	}

	return nameToCertificate, nil
}

// getCertificateNames returns names for an x509.Certificate. The names are
// suitable for use in tls.Config#NamedCertificates.
func getCertificateNames(cert *x509.Certificate) []string {
	var names []string

	cn := cert.Subject.CommonName
	cnIsIP := netutils.ParseIPSloppy(cn) != nil
	cnIsValidDomain := cn == "*" || len(validation.IsDNS1123Subdomain(strings.TrimPrefix(cn, "*."))) == 0
	// don't use the CN if it is a valid IP because our IP serving detection may unexpectedly use it to terminate the connection.
	if !cnIsIP && cnIsValidDomain {
		names = append(names, cn)
	}
	names = append(names, cert.DNSNames...)
	// 有意忽略证书中的所有 IP，因为 SNI 禁止用 IP 来选择证书。
	// Go 1.6 之前，tls 包会乐意把 IP 当作 SNI 值传入。
	// SAN 中的 IP 同样被忽略——SNI 扩展本身不允许 IP 字面量，
	// 保留它们只会造成“配置了却永远匹配不上”的困惑。

	return names
}
