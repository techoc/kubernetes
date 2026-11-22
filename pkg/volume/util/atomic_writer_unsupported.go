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

// Package util 提供卷相关的通用工具。
// 本文件为非 Linux 平台提供 AtomicWriter 的 lchown 空实现。
package util

import (
	"runtime"

	"k8s.io/klog/v2"
)

// lchown 修改指定文件的数字 uid 和 gid。
// 若文件是符号链接，修改的是链接本身而非目标文件。
// 在不支持的平台上为空操作：仅记录警告并返回 nil，
// 因为配置文件无法设置 Linux 所有权时不该阻断整个写入流程。
func (w *AtomicWriter) lchown(name string, uid, _ /* gid */ int) error {
	klog.Warningf("%s: skipping change of Linux owner %v for file %s; unsupported on %s", w.logContext, uid, name, runtime.GOOS)
	return nil
}
