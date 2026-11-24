/*
Copyright 2014 The Kubernetes Authors.

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

// Info 保存组件的构建版本、模拟版本和兼容性版本信息。
// TODO：是否增加 []string 表示支持的 API 版本仍未确定，需先明确该信息的分发方式。
//
// Info 包含版本信息。
type Info struct {
	// Major is the major version of the binary version
	//
	// Major 是二进制版本的主版本号。
	Major string `json:"major"`
	// Minor is the minor version of the binary version
	//
	// Minor 是二进制版本的次版本号。
	Minor string `json:"minor"`
	// EmulationMajor is the major version of the emulation version
	//
	// EmulationMajor 是模拟版本的主版本号。
	EmulationMajor string `json:"emulationMajor,omitempty"`
	// EmulationMinor is the minor version of the emulation version
	//
	// EmulationMinor 是模拟版本的次版本号。
	EmulationMinor string `json:"emulationMinor,omitempty"`
	// MinCompatibilityMajor is the major version of the minimum compatibility version
	//
	// MinCompatibilityMajor 是最低兼容版本的主版本号。
	MinCompatibilityMajor string `json:"minCompatibilityMajor,omitempty"`
	// MinCompatibilityMinor is the minor version of the minimum compatibility version
	//
	// MinCompatibilityMinor 是最低兼容版本的次版本号。
	MinCompatibilityMinor string `json:"minCompatibilityMinor,omitempty"`
	// GitVersion 是完整的 git 版本字符串（如 v1.37.0-alpha.0）。
	GitVersion string `json:"gitVersion"`
	// GitCommit 是构建所用的 git commit。
	GitCommit string `json:"gitCommit"`
	// GitTreeState 是构建时工作区状态（clean/dirty）。
	GitTreeState string `json:"gitTreeState"`
	// BuildDate 是构建时间。
	BuildDate string `json:"buildDate"`
	// GoVersion 是构建所用的 Go 版本。
	GoVersion string `json:"goVersion"`
	// Compiler 是构建所用的编译器。
	Compiler string `json:"compiler"`
	// Platform 是构建目标平台（os/arch）。
	Platform string `json:"platform"`
}

// String 返回便于人阅读的版本字符串，当前使用 GitVersion。
//
// String 以人类友好的形式返回版本信息（即 GitVersion）。
func (info Info) String() string {
	return info.GitVersion
}
