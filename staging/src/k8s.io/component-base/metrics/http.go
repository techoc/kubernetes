/*
Copyright 2019 The Kubernetes Authors.

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

package metrics

import (
	"io"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// processStartedAt 用于 promhttp 计算进程启动相关指标；
	// 在包初始化时取一次，之后保持不变。
	processStartedAt time.Time
)

func init() {
	processStartedAt = time.Now()
}

// 这些常量决定提供指标服务的 handler 在遇到错误时的行为。
const (
	// HTTPErrorOnError 遇到首个错误时返回 HTTP 500 状态码，并在响应体中报告错误信息。
	HTTPErrorOnError promhttp.HandlerErrorHandling = iota

	// ContinueOnError 忽略错误并尽量提供更多指标；若无法提供任何指标，则返回 HTTP 500 状态码，
	// 并在响应体中报告最后一个错误。仅用于有意采用“尽力而为”策略的指标收集场景。
	// 强烈建议同时提供其他错误检测手段：在 HandlerOpts 中设置 ErrorLog 可记录错误；
	// 提供 Registry 可暴露错误计数器 "promhttp_metric_handler_errors_total"，用于告警。
	ContinueOnError

	// PanicOnError 遇到首个错误时 panic，适用于“出错即崩溃”的应用。
	PanicOnError
)

// HandlerOpts 指定通过 http.Handler 提供指标服务的选项，其零值是合理的默认配置。
type HandlerOpts promhttp.HandlerOpts

// toPromhttpHandlerOpts 把内部类型转回 promhttp 类型，并注入进程启动时间。
func (ho *HandlerOpts) toPromhttpHandlerOpts() promhttp.HandlerOpts {
	ho.ProcessStartTime = processStartedAt
	return promhttp.HandlerOpts(*ho)
}

// HandlerFor 为给定 Gatherer 返回未插桩的 http.Handler，其行为由 HandlerOpts 决定。
// 适用于自定义 Gatherer、非默认 HandlerOpts、自定义插桩或不插桩的场景；
// 可使用 InstrumentMetricHandler 添加与 Handler 函数相同的插桩。
func HandlerFor(reg Gatherer, opts HandlerOpts) http.Handler {
	return promhttp.HandlerFor(reg, opts.toPromhttpHandlerOpts())
}

// HandlerWithReset 返回支持 Reset 的 http.Handler。
// DELETE 请求会清空注册表中的指标，仅用于测试或调试；生产端点通常不应暴露此能力。
func HandlerWithReset(reg KubeRegistry, opts HandlerOpts) http.Handler {
	defaultHandler := promhttp.HandlerFor(reg, opts.toPromhttpHandlerOpts())
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			reg.Reset()
			// 返回固定文本，便于测试断言重置确实发生。
			io.WriteString(w, "metrics reset\n")
			return
		}
		defaultHandler.ServeHTTP(w, r)
	})
}
