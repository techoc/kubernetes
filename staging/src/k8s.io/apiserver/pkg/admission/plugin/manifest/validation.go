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

// Package manifest 提供从静态 manifest 文件加载准入配置的公共工具。
package manifest

import (
	"fmt"
	"os"
	"path"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
)

// StaticConfigSuffix 是基于 manifest 的准入配置保留的后缀。
// 以此后缀命名的资源只能通过 apiserver 启动时加载的静态 manifest 文件创建，
// 不能经 REST API 创建。
// NOTE: 该常量在 pkg/apis/admissionregistration/validation/static_suffix.go 中重复定义，
// 因为那个包无法从 staging 导入。修改时必须同步两处。
//
// 后缀是安全边界——REST API 拒绝创建以此结尾的对象，确保这些对象
// 只能来自 apiserver 启动时读取的受信文件，无法被 API 客户端伪造或篡改。
const StaticConfigSuffix = ".static.k8s.io"

// ValidateStaticManifestsDir 校验 staticManifestsDir 配置字段。
// 检查特性门控是否启用、路径是否为绝对路径、是否存在且为目录。
//
// 空字符串视为未配置且合法；要求绝对路径是因为 apiserver 的工作目录
// 不可控，相对路径会在不同启动方式下解析到不同位置。
func ValidateStaticManifestsDir(staticManifestsDir string) error {
	if len(staticManifestsDir) > 0 {
		if !utilfeature.DefaultFeatureGate.Enabled(features.ManifestBasedAdmissionControlConfig) {
			return field.Forbidden(field.NewPath("staticManifestsDir"), "staticManifestsDir requires the ManifestBasedAdmissionControlConfig feature gate to be enabled")
		}
		if !path.IsAbs(staticManifestsDir) {
			return field.Invalid(field.NewPath("staticManifestsDir"), staticManifestsDir, "must be an absolute file path")
		}
		info, err := os.Stat(staticManifestsDir)
		if err != nil {
			return field.Invalid(field.NewPath("staticManifestsDir"), staticManifestsDir, fmt.Sprintf("unable to read: %v", err))
		}
		if !info.IsDir() {
			return field.Invalid(field.NewPath("staticManifestsDir"), staticManifestsDir, "must be a directory")
		}
	}
	return nil
}

// ValidateManifestName 检查对象名非空、带有必需的
// .static.k8s.io 后缀，且在 manifest 集合内唯一。
//
// seenNames 由调用方跨文件累积，保证不同文件中的同名对象也被拒绝；
// 名称重复会让后续按名查找产生歧义，必须在加载期就失败。
func ValidateManifestName(name, filePath string, seenNames map[string]string) error {
	if len(name) == 0 {
		return fmt.Errorf("resource in file %q must have a name", filePath)
	}
	if !strings.HasSuffix(name, StaticConfigSuffix) {
		return fmt.Errorf("%q in file %q must have a name ending with %q", name, filePath, StaticConfigSuffix)
	}
	if prevFile, ok := seenNames[name]; ok {
		return fmt.Errorf("duplicate name %q found in file %q (previously seen in %q)", name, filePath, prevFile)
	}
	seenNames[name] = filePath
	return nil
}
