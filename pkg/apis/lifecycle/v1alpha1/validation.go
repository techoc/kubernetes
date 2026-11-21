/*
Copyright The Kubernetes Authors.

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
	"context"

	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	apiscorev1 "k8s.io/kubernetes/pkg/apis/core/v1"
)

// ValidateCustom_EvictionRequestSpec_Requester 通过 corev1.EvictionRequestSpec.Requester 上的
// +k8s:customValidation 标记接入生成的声明式校验链。它的作用是确保 k8s 前缀标签键
// （k8s-prefixed-label-key）的值不以 k8s.io 或 kubernetes.io 域名作为前缀，
// 防止用户伪造 Kubernetes 保留的命名空间。
func ValidateCustom_EvictionRequestSpec_Requester(ctx context.Context, op operation.Operation, fldPath *field.Path, value, oldValue *string) field.ErrorList {
	if value != nil {
		// 将来出现 k8s 内置的 requester 时，需要维护一个可以绕过该校验的受支持 requester 集合。
		return apiscorev1.ValidateForbiddenReservedDomainSuffixes(fldPath, *value, []string{".k8s.io", ".kubernetes.io"})

	}
	return nil
}
