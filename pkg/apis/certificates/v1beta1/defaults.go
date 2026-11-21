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

package v1beta1

import (
	"crypto/x509"

	certificatesv1beta1 "k8s.io/api/certificates/v1beta1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	certificates "k8s.io/kubernetes/pkg/apis/certificates"
)

// addDefaultingFuncs 向 Scheme 注册 v1beta1 版本的默认值处理函数。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_CertificateSigningRequestSpec 为 CSR 规格设置默认值：
// usages 为空时补齐数字签名和密钥加密，signerName 为空时按请求内容推断。
func SetDefaults_CertificateSigningRequestSpec(obj *certificatesv1beta1.CertificateSigningRequestSpec) {
	// 未指定用途时保留旧版兼容行为：数字签名 + 密钥加密。
	if obj.Usages == nil {
		obj.Usages = []certificatesv1beta1.KeyUsage{certificatesv1beta1.UsageDigitalSignature, certificatesv1beta1.UsageKeyEncipherment}
	}

	// 兼容旧客户端：不指定 signerName 时，从 CSR 内容推断最合适的签名者。
	if obj.SignerName == nil {
		signerName := DefaultSignerNameFromSpec(obj)
		obj.SignerName = &signerName
	}
}

// SetDefaults_CertificateSigningRequestCondition 为 CSR 状态条件设置默认值，
// 空状态视为 True，与常见条件语义保持一致。
func SetDefaults_CertificateSigningRequestCondition(obj *certificatesv1beta1.CertificateSigningRequestCondition) {
	if len(obj.Status) == 0 {
		obj.Status = v1.ConditionTrue
	}
}

// DefaultSignerNameFromSpec 根据 CSR 请求内容和 usages 推断应设置的 signerName。
// 只有能明确识别为 kubelet 客户端/服务端 CSR 时才使用专用签名者，
// 其余情况一律回退到 legacy-unknown，避免误判签名者。
func DefaultSignerNameFromSpec(obj *certificatesv1beta1.CertificateSigningRequestSpec) string {
	csr, err := ParseCSR(obj.Request)
	switch {
	case err != nil:
		// 无法解析的 CSR 不能安全推断签名者，统一回退到 legacy-unknown。
		return certificatesv1beta1.LegacyUnknownSignerName
	case IsKubeletClientCSR(csr, obj.Usages):
		return certificatesv1beta1.KubeAPIServerClientKubeletSignerName
	case IsKubeletServingCSR(csr, obj.Usages):
		return certificatesv1beta1.KubeletServingSignerName
	default:
		return certificatesv1beta1.LegacyUnknownSignerName
	}
}

// IsKubeletServingCSR 判断 CSR 是否符合 kubelet 服务端证书的要求，
// 复用内部版本的校验逻辑，仅把 usages 从切片转换为集合。
func IsKubeletServingCSR(req *x509.CertificateRequest, usages []certificatesv1beta1.KeyUsage) bool {
	return certificates.IsKubeletServingCSR(req, usagesToSet(usages))
}

// IsKubeletClientCSR 判断 CSR 是否符合 kubelet 客户端证书的要求，
// 复用内部版本的校验逻辑，仅把 usages 从切片转换为集合。
func IsKubeletClientCSR(req *x509.CertificateRequest, usages []certificatesv1beta1.KeyUsage) bool {
	return certificates.IsKubeletClientCSR(req, usagesToSet(usages))
}

// usagesToSet 把 v1beta1 的 usages 列表转换为集合，
// 以满足内部校验函数对键集合（而非有序列表）的比较要求。
func usagesToSet(usages []certificatesv1beta1.KeyUsage) sets.String {
	result := sets.NewString()
	for _, usage := range usages {
		result.Insert(string(usage))
	}
	return result
}
