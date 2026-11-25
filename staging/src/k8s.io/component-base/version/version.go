/*
Copyright 2019 The Kubernetes Authors.

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

// Package version 汇总 Kubernetes 二进制的版本信息。
//
// 大部分字段由构建系统通过 -ldflags -X 注入，源码中的值只是占位；
// dynamicGitVersion 是唯一可在运行期被 --version=vX.Y.Z 覆盖的字段。
package version

import (
	"fmt"
	"runtime"

	apimachineryversion "k8s.io/apimachinery/pkg/version"
)

// Get 返回整个代码库的版本信息，供调用方判断二进制是由哪份代码构建的。
// 判断二进制版本应使用 BinaryMajor/BinaryMinor；Major/Minor 仍沿用 git
// 版本中的值，仅为兼容旧调用方。
func Get() apimachineryversion.Info {
	// 这些变量通常来自 -ldflags -X；未注入时回退到 base.go 中的占位值。
	// dynamicGitVersion 用 atomic.Value 存储，因为它可能被 --version 标志改写。
	return apimachineryversion.Info{
		Major:        gitMajor,
		Minor:        gitMinor,
		GitVersion:   dynamicGitVersion.Load().(string),
		GitCommit:    gitCommit,
		GitTreeState: gitTreeState,
		BuildDate:    buildDate,
		GoVersion:    runtime.Version(),
		Compiler:     runtime.Compiler,
		Platform:     fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
	}
}
