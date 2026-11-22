//go:build !linux

/*
Copyright 2024 The Kubernetes Authors.

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

package nfacct

import (
	"fmt"
	"runtime"
)

// 该实现依赖 Linux 的 netfilter/nfacct 接口，非 Linux 平台无法使用；
// 因此构造时直接返回带操作系统与架构信息的错误，便于定位不支持的环境。
var unsupportedError = fmt.Errorf(runtime.GOOS + "/" + runtime.GOARCH + " is unsupported")

// New 在非 Linux 平台上返回错误：nfacct 功能依赖 Linux netfilter，此处仅作为平台桩实现，
// 让其他平台仍能编译通过，而调用方会在运行时得到明确的“不支持”错误。
func New() (Interface, error) {
	return nil, unsupportedError
}
