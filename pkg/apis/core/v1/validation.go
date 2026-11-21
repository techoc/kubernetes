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

package v1

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// ValidateCustom_EvictionResponder_Name 通过 corev1.EvictionResponder.Name 上的
// +k8s:customValidation 指令接入生成的声明式校验。它强制要求 k8s 前缀的 label key 值
// 不得以 k8s.io 或 kubernetes.io 域名作为前缀。
func ValidateCustom_EvictionResponder_Name(ctx context.Context, op operation.Operation, fldPath *field.Path, value, oldValue *string) field.ErrorList {
	if value != nil {
		return ValidateForbiddenReservedDomainSuffixes(fldPath, *value, []string{".k8s.io", ".kubernetes.io"})

	}
	return nil
}

// ValidateCustom_EvictionResponder_Priority 通过 corev1.EvictionResponder.Priority 上的
// +k8s:customValidation 指令接入生成的声明式校验。它确保 k8s 优先级区间保留给 k8s 响应器使用。
func ValidateCustom_EvictionResponder_Priority(ctx context.Context, op operation.Operation, fldPath *field.Path, value, oldValue *int32) field.ErrorList {
	const reservedKubernetesRespondersPriority = 1000
	if value != nil && *value >= 0 && *value < reservedKubernetesRespondersPriority {
		// 这里无需检查响应器名称，因为目前尚不允许 k8s 响应器。
		// 默认的命令式驱逐响应器直接设置在 Eviction 对象上。
		// 待允许核心响应器后，需要为其分配相应的优先级。
		return field.ErrorList{field.Invalid(fldPath, *value, "priorities 0-999 are reserved for responders with *.k8s.io suffix")}
	}
	return nil
}

// ValidateForbiddenReservedDomainSuffixes 校验 value 未使用保留的域名后缀 reservedSuffixes。
// 校验时对域名部分做小写化、去首尾空白和去尾随点处理，避免通过大小写或点号绕过。
func ValidateForbiddenReservedDomainSuffixes(fldPath *field.Path, value string, reservedSuffixes []string) field.ErrorList {
	var allErrors field.ErrorList
	segments := strings.SplitN(value, "/", 2)
	if len(segments) == 0 {
		return allErrors
	}
	cleanValue := strings.TrimRight(strings.TrimSpace(strings.ToLower(segments[0])), ".")
	for _, suffix := range reservedSuffixes {
		if strings.HasSuffix(cleanValue, suffix) || cleanValue == strings.Trim(suffix, ".") {
			var userFormattedPrefixes []string
			for _, reservedSuffix := range reservedSuffixes {
				userFormattedPrefixes = append(userFormattedPrefixes, "*"+reservedSuffix)
			}
			return append(allErrors, field.Invalid(fldPath, value, fmt.Sprintf("domain names %s are reserved", strings.Join(userFormattedPrefixes, ", "))))
		}
	}
	return allErrors
}
