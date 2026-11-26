/*
Copyright 2021 The Kubernetes Authors.

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

package webhook

import (
	"context"
)

// AuthenticatorMetrics 定义一组方法，用于注册 webhook 的各种指标。
//
// AuthenticatorMetrics 把指标上报抽象为两个函数，使 webhook 认证器
// 不直接依赖 metrics 包，便于测试与复用。
type AuthenticatorMetrics struct {
	// RecordRequestTotal 增加 webhook 的总请求数。
	RecordRequestTotal func(ctx context.Context, code string)

	// RecordRequestLatency 以秒为单位记录 webhook 请求延迟，并按状态码拆分。
	RecordRequestLatency func(ctx context.Context, code string, latency float64)
}

// noopMetrics 是空实现，用于未配置指标的场景，避免调用点到处判空。
type noopMetrics struct{}

func (noopMetrics) RequestTotal(context.Context, string)            {}
func (noopMetrics) RequestLatency(context.Context, string, float64) {}
