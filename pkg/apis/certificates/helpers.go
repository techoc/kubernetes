/*
Copyright 2016 The Kubernetes Authors.

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

package certificates

import (
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
)

// ParseCSR 从 PEM 字节中解析并解码证书签名请求（CSR）。
// 仅接受类型为 CERTIFICATE REQUEST 的 PEM 块，防止误把证书等其他内容当作 CSR。
func ParseCSR(pemBytes []byte) (*x509.CertificateRequest, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil || block.Type != "CERTIFICATE REQUEST" {
		return nil, errors.New("PEM block type must be CERTIFICATE REQUEST")
	}
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		return nil, err
	}
	return csr, nil
}

// 预定义的校验错误：CSR 必须满足一组固定的主体与 SAN 约束，
// 才能被判定为 kubelet 客户端/服务端 CSR。
var (
	organizationNotSystemNodesErr = fmt.Errorf("subject organization is not system:nodes")
	commonNameNotSystemNode       = fmt.Errorf("subject common name does not begin with system:node:")
	dnsOrIPSANRequiredErr         = fmt.Errorf("DNS or IP subjectAltName is required")
	dnsSANNotAllowedErr           = fmt.Errorf("DNS subjectAltNames are not allowed")
	emailSANNotAllowedErr         = fmt.Errorf("Email subjectAltNames are not allowed")
	ipSANNotAllowedErr            = fmt.Errorf("IP subjectAltNames are not allowed")
	uriSANNotAllowedErr           = fmt.Errorf("URI subjectAltNames are not allowed")
)

// kubelet 服务端证书允许的 usages 集合：
// 非 RSA 私钥不需要密钥加密用途，因此两种情况都视为合法。
var (
	kubeletServingRequiredUsages = sets.NewString(
		string(UsageDigitalSignature),
		string(UsageKeyEncipherment),
		string(UsageServerAuth),
	)
	kubeletServingRequiredUsagesNoRSA = sets.NewString(
		string(UsageDigitalSignature),
		string(UsageServerAuth),
	)
)

// IsKubeletServingCSR 判断 CSR 是否形似 kubelet 服务端证书请求，
// 即 ValidateKubeletServingCSR 的布尔封装。
func IsKubeletServingCSR(req *x509.CertificateRequest, usages sets.String) bool {
	return ValidateKubeletServingCSR(req, usages) == nil
}

// ValidateKubeletServingCSR 校验 CSR 是否可作为 kubelet 服务端证书：
// 主体组织必须是 system:nodes，CommonName 必须以 system:node: 开头，
// 且必须带有 DNS 或 IP SAN（客户端通过地址访问 kubelet），
// 禁止 Email/URI SAN，usages 必须与已知组合完全匹配。
func ValidateKubeletServingCSR(req *x509.CertificateRequest, usages sets.String) error {
	// 组织字段必须精确等于 system:nodes，不能是包含该值的多元素列表。
	if !reflect.DeepEqual([]string{"system:nodes"}, req.Subject.Organization) {
		return organizationNotSystemNodesErr
	}

	// 服务端证书至少要有一个 DNS 或 IP SAN，否则客户端无法按地址校验证书。
	if len(req.DNSNames) == 0 && len(req.IPAddresses) == 0 {
		return dnsOrIPSANRequiredErr
	}

	// kubelet 服务端身份不通过邮箱或 URI 标识，出现这类 SAN 一律拒绝。
	if len(req.EmailAddresses) > 0 {
		return emailSANNotAllowedErr
	}
	if len(req.URIs) > 0 {
		return uriSANNotAllowedErr
	}

	// usages 必须完全等于已知集合，多余或缺失的用途都会改变证书语义。
	if !kubeletServingRequiredUsages.Equal(usages) && !kubeletServingRequiredUsagesNoRSA.Equal(usages) {
		return fmt.Errorf("usages did not match %v", kubeletServingRequiredUsages.List())
	}

	// CommonName 前缀是 kubelet 节点身份的约定，用于与用户名映射对应。
	if !strings.HasPrefix(req.Subject.CommonName, "system:node:") {
		return commonNameNotSystemNode
	}

	return nil
}

// kubelet 客户端证书允许的 usages 集合：
// 非 RSA 私钥同样不需要密钥加密用途。
var (
	kubeletClientRequiredUsagesNoRSA = sets.NewString(
		string(UsageDigitalSignature),
		string(UsageClientAuth),
	)
	kubeletClientRequiredUsages = sets.NewString(
		string(UsageDigitalSignature),
		string(UsageKeyEncipherment),
		string(UsageClientAuth),
	)
)

// IsKubeletClientCSR 判断 CSR 是否形似 kubelet 客户端证书请求，
// 即 ValidateKubeletClientCSR 的布尔封装。
func IsKubeletClientCSR(req *x509.CertificateRequest, usages sets.String) bool {
	return ValidateKubeletClientCSR(req, usages) == nil
}

// ValidateKubeletClientCSR 校验 CSR 是否可作为 kubelet 客户端证书：
// 主体组织必须是 system:nodes，CommonName 以 system:node: 开头，
// 且不能携带任何 SAN（客户端身份仅由主体字段表达）。
func ValidateKubeletClientCSR(req *x509.CertificateRequest, usages sets.String) error {
	// 组织字段必须精确等于 system:nodes，不能是包含该值的多元素列表。
	if !reflect.DeepEqual([]string{"system:nodes"}, req.Subject.Organization) {
		return organizationNotSystemNodesErr
	}

	// 客户端身份不依赖地址，禁止所有 SAN，避免身份含义被扩展。
	if len(req.DNSNames) > 0 {
		return dnsSANNotAllowedErr
	}
	if len(req.EmailAddresses) > 0 {
		return emailSANNotAllowedErr
	}
	if len(req.IPAddresses) > 0 {
		return ipSANNotAllowedErr
	}
	if len(req.URIs) > 0 {
		return uriSANNotAllowedErr
	}

	// CommonName 前缀用于匹配 system:nodes 组的用户名约定。
	if !strings.HasPrefix(req.Subject.CommonName, "system:node:") {
		return commonNameNotSystemNode
	}

	// usages 必须完全匹配已知集合，防止客户端证书被挪作他用。
	if !kubeletClientRequiredUsages.Equal(usages) && !kubeletClientRequiredUsagesNoRSA.Equal(usages) {
		return fmt.Errorf("usages did not match %v", kubeletClientRequiredUsages.List())
	}

	return nil
}
