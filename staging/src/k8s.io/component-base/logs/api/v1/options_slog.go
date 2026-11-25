//go:build go1.21

/*
Copyright 2023 The Kubernetes Authors.

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

package v1

import (
	"log/slog"

	"github.com/go-logr/logr"
	"k8s.io/klog/v2"
)

// setSlogDefaultLogger 将全局 slog 默认 logger 设置为 klog 当前使用的默认 logger。
//
// 让直接使用 log/slog 的第三方库与 klog 输出到同一目标、遵循同一格式，
// 避免出现两套互不相干的日志管道。
func setSlogDefaultLogger() {
	// 无论日志如何配置，klog.Background() 总会返回有效的 logr.Logger。
	// 只需将其转换为 slog.Handler，再用 slog.Logger 包装并调用 SetDefault。
	// 经 logr 桥接而非重新构造 handler，确保动态级别与输出目标保持一致。
	handler := logr.ToSlogHandler(klog.Background())
	slog.SetDefault(slog.New(handler))
}
