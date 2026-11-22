/*
Copyright 2025 The Kubernetes Authors.

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

// Package compatibility 构造 apiserver 使用的“有效版本”（effective version）：
// 二进制版本、模拟版本（emulationVersion）与最低兼容版本，并维护 N-3 的
// 版本下限，使新二进制可以按旧版本语义运行以支持平滑升级。
package compatibility

import (
	"k8s.io/apimachinery/pkg/util/version"
	basecompatibility "k8s.io/component-base/compatibility"
	baseversion "k8s.io/component-base/version"
)

// minimumKubeEmulationVersion is the first release emulation version is introduced,
// so the emulation version cannot go lower than that.
var minimumKubeEmulationVersion *version.Version = version.MajorMinor(1, 31)

// DefaultBuildEffectiveVersion returns the MutableEffectiveVersion based on the
// current build information.
// 优先使用构建时注入的 git 版本；本地无 tag 的单元测试会得到 0.0，此时回退
// 到硬编码的默认版本，避免测试因缺少构建信息而失败。
func DefaultBuildEffectiveVersion() basecompatibility.MutableEffectiveVersion {
	binaryVersion := defaultBuildBinaryVersion()
	useDefaultBuildBinaryVersion := true
	// fall back to the hard coded kube version only when the git tag is not available for local unit tests.
	if binaryVersion.Major() == 0 && binaryVersion.Minor() == 0 {
		useDefaultBuildBinaryVersion = false
		binaryVersion = version.MustParse(baseversion.DefaultKubeBinaryVersion)
	}
	versionFloor := kubeEffectiveVersionFloors(binaryVersion)
	return basecompatibility.NewEffectiveVersion(binaryVersion, useDefaultBuildBinaryVersion, versionFloor, versionFloor)
}

// kubeEffectiveVersionFloors 计算允许的最低模拟/兼容版本：二进制次版本减 3，
// 且不低于 emulation 机制引入的 1.31，防止声明一个无法表达该机制的历史版本。
func kubeEffectiveVersionFloors(binaryVersion *version.Version) *version.Version {
	// both emulationVersion and minCompatibilityVersion can be set to binaryVersion - 3
	versionFloor := binaryVersion.WithPatch(0).SubtractMinor(3)
	if versionFloor.LessThan(minimumKubeEmulationVersion) {
		versionFloor = minimumKubeEmulationVersion
	}
	return versionFloor
}

// DefaultKubeEffectiveVersionForTest returns the MutableEffectiveVersion based on the
// latest K8s release hardcoded in DefaultKubeBinaryVersion.
// DefaultKubeBinaryVersion is hard coded because defaultBuildBinaryVersion would return 0.0 when test is run without a git tag.
// We do not enforce the N-3..N emulation version range in tests so that the tests would not automatically fail when there is a version bump.
// Only used in tests.
// 下限设为 0.0 即关闭范围校验，测试不受版本 bump 影响。
func DefaultKubeEffectiveVersionForTest() basecompatibility.MutableEffectiveVersion {
	binaryVersion := version.MustParse(baseversion.DefaultKubeBinaryVersion)
	return basecompatibility.NewEffectiveVersion(binaryVersion, false, version.MustParse("0.0"), version.MustParse("0.0"))
}

// defaultBuildBinaryVersion 把构建信息转换为 version.Version；Get() 在无
// 构建元数据时返回零值版本，由调用方决定是否回退。
func defaultBuildBinaryVersion() *version.Version {
	verInfo := baseversion.Get()
	return version.MustParse(verInfo.String())
}
