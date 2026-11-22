//go:build !windows

/*
Copyright 2022 The Kubernetes Authors.

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

package routes

import "syscall"

// fileNameTooLong 对应非 Windows 平台（Linux/macOS 等 Unix 系统）的文件名过长错误。
// 用于 logFileNameIsTooLong() 函数中区分"文件名过长"和"真正的服务器内部错误"。
// Unix 系统文件名限制通常为 255 字节（NAME_MAX），超过此限制会返回 ENAMETOOLONG。
const (
	fileNameTooLong = syscall.ENAMETOOLONG
)
