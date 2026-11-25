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

package initoption

import (
	"time"

	"k8s.io/kubernetes/test/utils/ktesting/internal"
)

// InitOption 是 Init 和 InitCtx 的函数式选项。
type InitOption func(c *internal.InitConfig)

// PerTestOutput 控制 Init 是否为每个测试设置 logger。
// 对 InitCtx 无效。
func PerTestOutput(enabled bool) InitOption {
	return func(c *internal.InitConfig) {
		c.PerTestOutput = enabled
	}
}

// BufferLogs 控制除了打印日志之外，是否还在内存中捕获日志条目。默认关闭。
// 希望验证日志条目是否按预期输出的单元测试可以打开此选项，
// 然后通过 Underlier LogSink 接口获取捕获到的日志。
func BufferLogs(enabled bool) InitOption {
	return func(c *internal.InitConfig) {
		c.BufferLogs = enabled
	}
}

// WithCleanupGracePeriod 覆盖 [Init] 使用的默认清理宽限期。
// 清理宽限期是在测试套件 deadline 之前预留的一段时间，
// 以便清理回调能够在测试二进制文件被终止之前完成。
//
// 非正值会被忽略，改用默认值。
//
// 当使用 Ginkgo 管理测试套件时，此选项无效，因为 Ginkgo 自身管理超时。
func WithCleanupGracePeriod(d time.Duration) InitOption {
	return func(c *internal.InitConfig) {
		c.CleanupGracePeriod = d
	}
}
