//go:build !linux

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

// Package apparmor 提供 AppArmor 配置文件的验证和辅助功能。
// 此文件仅在非 Linux 平台编译时生效（build tag: !linux）。
// AppArmor 是 Linux 内核特有的安全模块，其他平台无法支持。
package apparmor

func init() {
	// 非 Linux 平台编译时，直接禁用 AppArmor 支持。
	// 这样 validateHost() 会立即返回错误，无需检查内核。
	isDisabledBuild = true
}
