/*
Copyright 2022 The Kubernetes Authors.

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

package v1alpha1

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
)

// addConversionFuncs 向 scheme 注册字段标签（field label）的转换规则。
// 字段标签转换让外部客户端可以按指定字段筛选/监听资源，
// 这里为 ClusterTrustBundle 与 PodCertificateRequest 两种资源分别注册。
func addConversionFuncs(scheme *runtime.Scheme) error {
	err := scheme.AddFieldLabelConversionFunc(
		SchemeGroupVersion.WithKind("ClusterTrustBundle"),
		func(label, value string) (string, string, error) {
			switch label {
			// 仅支持按名称与签名者名称筛选 ClusterTrustBundle。
			case "metadata.name", "spec.signerName":
				return label, value, nil
			default:
				// 未列入白名单的标签直接拒绝，防止误用不支持的筛选条件。
				return "", "", fmt.Errorf("field label not supported: %s", label)
			}
		},
	)
	if err != nil {
		// 包装错误以标明注册失败对应的资源类型。
		return fmt.Errorf("while adding ClusterTrustBundle field label conversion func: %w", err)
	}

	err = scheme.AddFieldLabelConversionFunc(
		SchemeGroupVersion.WithKind("PodCertificateRequest"),
		func(label, value string) (string, string, error) {
			switch label {
			// PodCertificateRequest 额外支持按 podName 与 nodeName 筛选，
			// 这类请求总是与特定的 Pod 和节点绑定。
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
