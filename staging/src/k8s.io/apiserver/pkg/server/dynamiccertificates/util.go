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
	"crypto/x509"
	"fmt"
	"strings"
	"time"
)

// GetHumanCertDetail is a convenient method for printing compact details of certificate that helps when debugging
// kube-apiserver usage of certs.
//
// 输出刻意保持单行紧凑，便于在日志与错误信息中直接阅读；时间统一用 UTC，
// 避免不同节点的本地时区造成排查时的困惑。
func GetHumanCertDetail(certificate *x509.Certificate) string {
	humanName := certificate.Subject.CommonName
	signerHumanName := certificate.Issuer.CommonName
	// 自签名证书的 subject 与 issuer 相同，单独标记为 <self> 更直观。
	if certificate.Subject.CommonName == certificate.Issuer.CommonName {
		signerHumanName = "<self>"
	}

	// 只把最常见的两种用途翻译成可读文本，其余按数字输出，保证信息不丢失。
	usages := []string{}
	for _, curr := range certificate.ExtKeyUsage {
		if curr == x509.ExtKeyUsageClientAuth {
			usages = append(usages, "client")
			continue
		}
		if curr == x509.ExtKeyUsageServerAuth {
			usages = append(usages, "serving")
			continue
		}

		usages = append(usages, fmt.Sprintf("%d", curr))
	}

	// SAN 中 IP 与 DNS 混排，能帮助快速确认证书是否覆盖当前服务的访问地址。
	validServingNames := []string{}
	for _, ip := range certificate.IPAddresses {
		validServingNames = append(validServingNames, ip.String())
	}
	validServingNames = append(validServingNames, certificate.DNSNames...)
	servingString := ""
	if len(validServingNames) > 0 {
		servingString = fmt.Sprintf(" validServingFor=[%s]", strings.Join(validServingNames, ","))
	}

	// Organization 在客户端证书里通常承载 RBAC group，调试认证问题时有价值。
	groupString := ""
	if len(certificate.Subject.Organization) > 0 {
		groupString = fmt.Sprintf(" groups=[%s]", strings.Join(certificate.Subject.Organization, ","))
	}

	return fmt.Sprintf("%q [%s]%s%s issuer=%q (%v to %v (now=%v))", humanName, strings.Join(usages, ","), groupString, servingString, signerHumanName, certificate.NotBefore.UTC(), certificate.NotAfter.UTC(),
		time.Now().UTC())
}
