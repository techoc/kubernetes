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

package v1

import (
	"fmt"

	certificatesv1 "k8s.io/api/certificates/v1"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/apis/certificates"
)

// addConversionFuncs 向 scheme 注册字段标签（field label）的转换规则。
// 字段标签转换用于把对外 API 中使用的字段路径映射为内部表示，
// 从而使 v1 版本的列表/监听请求能按这些字段进行筛选。
func addConversionFuncs(scheme *runtime.Scheme) error {
	// 注册各资源类型的字段标签转换函数；每种资源的白名单独立维护，
	// 未被显式列出的字段一律拒绝，避免暴露未受支持的筛选语义。
	err := scheme.AddFieldLabelConversionFunc(SchemeGroupVersion.WithKind("CertificateSigningRequest"),
		func(label, value string) (string, string, error) {
			switch label {
			// 仅 metadata.name 与 spec.signerName 是 CSR 对外支持的字段标签。
			case "metadata.name",
				"spec.signerName":
				return label, value, nil
			default:
				// 未知标签直接报错，由 API 层转换为 400 响应返回给客户端。
				return "", "", fmt.Errorf("field label not supported: %s", label)
			}
		},
	)
	if err != nil {
		// 包装注册失败的错误，标明是哪一类资源的注册出了问题。
		return fmt.Errorf("while adding CertificateSigningRequest field label conversion func: %w", err)
	}

	err = scheme.AddFieldLabelConversionFunc(
		SchemeGroupVersion.WithKind("ClusterTrustBundle"),
		func(label, value string) (string, string, error) {
			switch label {
			// ClusterTrustBundle 的可筛选字段与 CSR 相同。
			case "metadata.name", "spec.signerName":
				return label, value, nil
			default:
				return "", "", fmt.Errorf("field label not supported: %s", label)
			}
		},
	)
	if err != nil {
		return fmt.Errorf("while adding ClusterTrustBundle field label conversion func: %w", err)
	}

	err = scheme.AddFieldLabelConversionFunc(SchemeGroupVersion.WithKind("PodCertificateRequest"),
		func(label, value string) (string, string, error) {
			switch label {
			// PodCertificateRequest 额外支持按 podName 与 nodeName 筛选，
			// 因为 Pod 证书请求天然与特定 Pod 和节点关联。
			case "metadata.name", "spec.signerName", "spec.podName", "spec.nodeName":
				return label, value, nil
			default:
				return "", "", fmt.Errorf("field label not supported: %s", label)
			}
		},
	)
	if err != nil {
		return fmt.Errorf("while adding PodCertificateRequest field label conversion func: %w", err)
	}

	return nil
}

// Convert_certificates_PodCertificateRequestSpec_To_v1_PodCertificateRequestSpec
// 把内部版本的 PodCertificateRequestSpec 转换为 v1 外部版本。
// 当前两个版本字段一致，直接委托给生成的自动转换函数即可；
// 保留手写入口是为了将来版本出现差异时有扩展点。
func Convert_certificates_PodCertificateRequestSpec_To_v1_PodCertificateRequestSpec(in *certificates.PodCertificateRequestSpec, out *certificatesv1.PodCertificateRequestSpec, s conversion.Scope) error {
	return autoConvert_certificates_PodCertificateRequestSpec_To_v1_PodCertificateRequestSpec(in, out, s)
}
