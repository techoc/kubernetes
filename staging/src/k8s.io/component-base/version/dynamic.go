/*
Copyright 2023 The Kubernetes Authors.

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

package version

import (
	"fmt"
	"sync/atomic"

	utilversion "k8s.io/apimachinery/pkg/util/version"
)

// dynamicGitVersion 使用 atomic.Value 存储可覆盖的版本字符串，
// 使 Get() 能无锁读取，同时允许测试或包装组件在启动时修改。
var dynamicGitVersion atomic.Value

func init() {
	// initialize to static gitVersion
	dynamicGitVersion.Store(gitVersion)
}

// SetDynamicVersion overrides the version returned as the GitVersion from Get().
// The specified version must be non-empty, a valid semantic version, and must
// match the major/minor/patch version of the default gitVersion.
//
// 只允许改预发布/构建元数据等后缀；major.minor.patch 必须与二进制一致，
// 防止上报虚假版本破坏版本偏差判定。
func SetDynamicVersion(dynamicVersion string) error {
	if err := ValidateDynamicVersion(dynamicVersion); err != nil {
		return err
	}
	dynamicGitVersion.Store(dynamicVersion)
	return nil
}

// ValidateDynamicVersion ensures the given version is non-empty, a valid semantic version,
// and matched the major/minor/patch version of the default gitVersion.
func ValidateDynamicVersion(dynamicVersion string) error {
	return validateDynamicVersion(dynamicVersion, gitVersion)
}

// validateDynamicVersion 是 ValidateDynamicVersion 的实现，显式传入默认版本以便测试。
func validateDynamicVersion(dynamicVersion, defaultVersion string) error {
	if len(dynamicVersion) == 0 {
		return fmt.Errorf("version must not be empty")
	}
	if dynamicVersion == defaultVersion {
		// allow no-op
		// 与默认值相同直接放行，避免对未由 make 注入的版本做解析。
		return nil
	}
	vRuntime, err := utilversion.ParseSemantic(dynamicVersion)
	if err != nil {
		return err
	}
	// must match major/minor/patch of default version
	var vDefault *utilversion.Version
	if defaultVersion == "v0.0.0-master+$Format:%H$" {
		// special-case the placeholder value which doesn't parse as a semantic version
		// 开发构建中的占位符不是合法 semver，按 v0.0.0-master 处理。
		vDefault, err = utilversion.ParseSemantic("v0.0.0-master")
	} else {
		vDefault, err = utilversion.ParseSemantic(defaultVersion)
	}
	if err != nil {
		return err
	}
	// 只比较核心版本号，忽略预发布与构建元数据。
	if vRuntime.Major() != vDefault.Major() || vRuntime.Minor() != vDefault.Minor() || vRuntime.Patch() != vDefault.Patch() {
		return fmt.Errorf("version %q must match major/minor/patch of default version %q", dynamicVersion, defaultVersion)
	}
	return nil
}
