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

package validation

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// StaticConfigSuffix 是基于清单（manifest）的准入配置保留的后缀。
// 名称以该后缀结尾的资源只能通过 API Server 启动时加载的静态清单文件创建，
// 不能通过 REST API 创建或修改。
// 注意：这个常量在 staging/src/k8s.io/apiserver/pkg/admission/plugin/manifest/validation.go 中有一份副本，
// 因为当前包不能从 staging 导入，修改时必须两处保持同步。
const StaticConfigSuffix = ".static.k8s.io"

// ValidateStaticSuffix 校验名称没有使用保留的静态配置后缀。
// 当名称以该后缀结尾时返回错误，防止通过 API 覆盖启动时加载的静态配置。
func ValidateStaticSuffix(name string, fldPath *field.Path) field.ErrorList {
	var allErrors field.ErrorList
	if strings.HasSuffix(name, StaticConfigSuffix) {
		allErrors = append(allErrors, field.Invalid(fldPath, name,
			fmt.Sprintf("names ending with %q are reserved for static manifest-based configurations and cannot be managed via the API", StaticConfigSuffix)))
	}
	return allErrors
}

// WarningsForStaticSuffix 在名称使用了保留的静态配置后缀时返回警告信息，
// 用于校验之外的提示场景（例如 dry-run）。
func WarningsForStaticSuffix(name string) []string {
	if strings.HasSuffix(name, StaticConfigSuffix) {
		return []string{
			fmt.Sprintf("metadata.name: names ending with %q are reserved for static manifest-based configurations and cannot be managed via the API", StaticConfigSuffix),
		}
	}
	return nil
}
