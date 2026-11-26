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

import (
	"fmt"
	"runtime"

	apimachineryversion "k8s.io/apimachinery/pkg/version"
)

// Get 返回代码库的整体版本，用于判断二进制文件由哪份代码构建。
//
// 返回值在启动时即已固定：git 相关字段由构建脚本通过 -ldflags 注入，
// 未注入时回退到 base.go 中的占位值。该结果常被用于版本协商与问题上报。
func Get() apimachineryversion.Info {
	// 这些变量通常由 -ldflags 注入，缺省时回退到 pkg/version/base.go 中的占位值。
	// GoVersion/Compiler/Platform 取自当前运行时，而不是构建时的快照。
	return apimachineryversion.Info{
		Major:        gitMajor,
		Minor:        gitMinor,
		GitVersion:   gitVersion,
		GitCommit:    gitCommit,
		GitTreeState: gitTreeState,
		BuildDate:    buildDate,
		GoVersion:    runtime.Version(),
		Compiler:     runtime.Compiler,
		Platform:     fmt.Sprintf("%s/%s", runtime.GOOS, runtime.GOARCH),
	}
}
