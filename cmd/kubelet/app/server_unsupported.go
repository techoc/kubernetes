//go:build !linux

/*
Copyright 2015 The Kubernetes Authors.

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

package app

import (
	"context"
	"errors"
)

// 非 Linux 平台没有此处使用的 inotify 实现，显式返回不支持错误，避免误认为锁竞争监视已经启用。
func watchForLockfileContention(ctx context.Context, path string, done chan struct{}) error {
	return errors.New("kubelet unsupported in this build")
}

// 当前构建目标不提供 Linux cgroup v2 检测；返回 false 让调用方按非统一层级模式处理。
func isCgroup2UnifiedMode() bool {
	return false
}
