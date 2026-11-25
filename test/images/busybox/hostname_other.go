//go:build !windows

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

package main

import "errors"

// getFQDN 仅在 Windows 上实现，因为该辅助二进制文件只会
// 被复制到 Windows 的 busybox 镜像中。镜像构建仍会在选择
// Dockerfile 之前针对 Linux 平台编译此包，
// 因此保留这个桩实现以满足跨平台构建。
func getFQDN() (string, error) {
	return "", errors.New("hostname -f is only supported by this helper on Windows")
}
