/*
Copyright 2016 The Kubernetes Authors.

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

package types

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"k8s.io/gengo/v2/namer"
)

// ToGroupVersion 把 "group/version" 字符串解析为 GroupVersion。
//
// 空串和 "/" 都表示内部版本（core 组的 internal 版本），此时 Group 与 Version 均为空。
// 超过一个 "/" 视为非法，避免把 "a/b/c" 误解析成组名里带斜杠。
func ToGroupVersion(gv string) (GroupVersion, error) {
	// 空串对应旧 kube 类型的 internal 版本；等字符串用法清理干净后应移除该特例。
	if (len(gv) == 0) || (gv == "/") {
		return GroupVersion{}, nil
	}

	switch strings.Count(gv, "/") {
	case 0:
		return GroupVersion{Group(gv), ""}, nil
	case 1:
		i := strings.Index(gv, "/")
		return GroupVersion{Group(gv[:i]), Version(gv[i+1:])}, nil
	default:
		return GroupVersion{}, fmt.Errorf("unexpected GroupVersion string: %v", gv)
	}
}

// sortableSliceOfVersions 实现按 Kubernetes 版本优先级排序：v2 > v1beta2 > v1beta1 > v1alpha2。
// 注意它排序的是去掉 "v" 前缀后的字符串，依赖版本号只含数字与 alpha/beta 后缀这一约定。
type sortableSliceOfVersions []string

func (a sortableSliceOfVersions) Len() int      { return len(a) }
func (a sortableSliceOfVersions) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a sortableSliceOfVersions) Less(i, j int) bool {
	vi, vj := strings.TrimLeft(a[i], "v"), strings.TrimLeft(a[j], "v")
	major := regexp.MustCompile("^[0-9]+")
	viMajor, vjMajor := major.FindString(vi), major.FindString(vj)
	viRemaining, vjRemaining := strings.TrimLeft(vi, viMajor), strings.TrimLeft(vj, vjMajor)
	switch {
	case len(viRemaining) == 0 && len(vjRemaining) == 0:
		return viMajor < vjMajor
	case len(viRemaining) == 0 && len(vjRemaining) != 0:
		return false
	case len(viRemaining) != 0 && len(vjRemaining) == 0:
		return true
	}
	// 两边都是不稳定版本。
	if viMajor != vjMajor {
		return viMajor < vjMajor
	}
	// 假定最多只有一个 alpha 或 beta：含 alpha 的版本优先级更低。
	return strings.Contains(viRemaining, "alpha")
}

// defaultVersion 选出一组版本中的默认版本，供未显式指定版本的 group client 使用。
//
// 取排序后的最后一个，即最高优先级版本；group client 的默认构造函数使用该版本。
func defaultVersion(versions []PackageVersion) Version {
	var versionStrings []string
	for _, version := range versions {
		versionStrings = append(versionStrings, version.Version.String())
	}
	sort.Sort(sortableSliceOfVersions(versionStrings))
	return Version(versionStrings[len(versionStrings)-1])
}

// ToGroupVersionInfo 为各 group 生成器整理 group-version 元信息。
//
// PackageAlias 由 groupGoName 与版本拼接后转小写，用于生成 import 别名，
// 保证不同 group-version 的别名唯一且可预测。
func ToGroupVersionInfo(groups []GroupVersions, groupGoNames map[GroupVersion]string) []GroupVersionInfo {
	var groupVersionPackages []GroupVersionInfo
	for _, group := range groups {
		for _, version := range group.Versions {
			groupGoName := groupGoNames[GroupVersion{Group: group.Group, Version: version.Version}]
			groupVersionPackages = append(groupVersionPackages, GroupVersionInfo{
				Group:                Group(namer.IC(group.Group.NonEmpty())),
				Version:              Version(namer.IC(version.Version.String())),
				PackageAlias:         strings.ToLower(groupGoName + version.Version.NonEmpty()),
				GroupGoName:          groupGoName,
				LowerCaseGroupGoName: namer.IL(groupGoName),
			})
		}
	}
	return groupVersionPackages
}

// ToGroupInstallPackages 为每个 group 生成一个安装包信息，版本取该 group 的默认版本。
// scheme 注册时每个 group 只需一个安装入口，因此不按 version 展开。
func ToGroupInstallPackages(groups []GroupVersions, groupGoNames map[GroupVersion]string) []GroupInstallPackage {
	var groupInstallPackages []GroupInstallPackage
	for _, group := range groups {
		defaultVersion := defaultVersion(group.Versions)
		groupGoName := groupGoNames[GroupVersion{Group: group.Group, Version: defaultVersion}]
		groupInstallPackages = append(groupInstallPackages, GroupInstallPackage{
			Group:               Group(namer.IC(group.Group.NonEmpty())),
			InstallPackageAlias: strings.ToLower(groupGoName),
		})
	}
	return groupInstallPackages
}

// NormalizeGroupVersion 规范化 GroupVersion。
// func NormalizeGroupVersion(gv GroupVersion) GroupVersion {
// 	 return GroupVersion{Group: gv.Group.NonEmpty(), Version: gv.Version, NonEmptyVersion: normalization.Version(gv.Version)}
// }
