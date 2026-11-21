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

package v1beta1

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
)

// addConversionFuncs 向 scheme 注册各资源类型的字段标签转换函数。
// 字段标签转换用于把 API 层的 field selector 映射到内部表示，
// 这里为每个 Kind 白名单式地声明哪些标签可被选择。
func addConversionFuncs(scheme *runtime.Scheme) error {
	// 注册字段标签转换函数。
	err := scheme.AddFieldLabelConversionFunc(SchemeGroupVersion.WithKind("CertificateSigningRequest"),
		func(label, value string) (string, string, error) {
			switch label {
			case "metadata.name",
				"spec.signerName":
				return label, value, nil
			default:
				return "", "", fmt.Errorf("field label not supported: %s", label)
			}
		},
	)
	if err != nil {
		return err
	}

	// ClusterTrustBundle 同样只允许按名称和 signerName 选择。
	err = scheme.AddFieldLabelConversionFunc(
		SchemeGroupVersion.WithKind("ClusterTrustBundle"),
		func(label, value string) (string, string, error) {
			switch label {
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

	// PodCertificateRequest 额外支持按 podName 和 nodeName 选择，便于按工作负载定位请求。
	err = scheme.AddFieldLabelConversionFunc(
		SchemeGroupVersion.WithKind("PodCertificateRequest"),
		func(label, value string) (string, string, error) {
			switch label {
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
