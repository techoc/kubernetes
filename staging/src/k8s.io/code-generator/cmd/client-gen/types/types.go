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

// Package types 定义 client-gen 与 applyconfiguration-gen 共享的 API 群组、
// 版本与类型标识，并规定它们到 Go 包名/目录名的映射方式。这些映射是
// 生成代码跨包引用与目录布局的基础，必须保持稳定。
package types

import "strings"

// Version 是 API 版本字符串（如 v1、v1beta1），空值表示内部版本。
type Version string

// String 返回版本原始字符串。
func (v Version) String() string {
	return string(v)
}

// NonEmpty 在版本为空时返回 "internalVersion"，用于日志与目录命名，
// 避免生成空路径段。
func (v Version) NonEmpty() string {
	if v == "" {
		return "internalVersion"
	}
	return v.String()
}

// PackageName 返回版本对应的小写 Go 包名。
func (v Version) PackageName() string {
	return strings.ToLower(v.NonEmpty())
}

// Group 是 API 群组名，空值表示 core 群组。
type Group string

// String 返回群组原始字符串。
func (g Group) String() string {
	return string(g)
}

// NonEmpty 在群组为空时返回 "core"。
func (g Group) NonEmpty() string {
	if g == "" {
		return "core"
	}
	return string(g)
}

// PackageName 计算群组对应的 Go 包名：
// 普通群组取首段（如 apps.foo.io -> apps）；
// internal 开头的群组取第二段加 internal（如 internal.foo -> foointernals），
// 避免与同名普通群组冲突。
func (g Group) PackageName() string {
	parts := strings.Split(g.NonEmpty(), ".")
	if parts[0] == "internal" && len(parts) > 1 {
		return strings.ToLower(parts[1] + parts[0])
	}
	return strings.ToLower(parts[0])
}

// Kind 是 API 类型（资源）名称。
type Kind string

// PackageVersion 是版本加上其所在源码包路径的组合。
type PackageVersion struct {
	Version
	// The fully qualified package, e.g. k8s.io/kubernetes/pkg/apis/apps, where the types.go is found.
	Package string
}

// GroupVersion 是群组与版本的组合，不含类型。
type GroupVersion struct {
	Group   Group
	Version Version
}

// GroupVersionKind 是完整的 GVK 标识。
type GroupVersionKind struct {
	Group   Group
	Version Version
	Kind    Kind
}

// ToAPIVersion 返回 "group/version"，core 群组（空 Group）则仅有 version。
func (gv GroupVersion) ToAPIVersion() string {
	if len(gv.Group) > 0 && gv.Group != "" {
		return gv.Group.String() + "/" + gv.Version.String()
	} else {
		return gv.Version.String()
	}
}

// WithKind 将当前 GroupVersion 与 Kind 组合为 GVK。
func (gv GroupVersion) WithKind(kind Kind) GroupVersionKind {
	return GroupVersionKind{Group: gv.Group, Version: gv.Version, Kind: kind}
}

// GroupVersions 是一个群组下所有版本及其 Go 包名的集合。
type GroupVersions struct {
	// The name of the package for this group, e.g. apps.
	PackageName string
	Group       Group
	Versions    []PackageVersion
}

// GroupVersionInfo contains all the info around a group version.
// GroupGoName 与 LowerCaseGroupGoName 用于生成包别名，需与 client-gen 输出一致。
type GroupVersionInfo struct {
	Group                Group
	Version              Version
	PackageAlias         string
	GroupGoName          string
	LowerCaseGroupGoName string
}

// GroupInstallPackage 描述群组的 install 包及其生成别名。
type GroupInstallPackage struct {
	Group               Group
	InstallPackageAlias string
}
