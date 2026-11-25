/*
Copyright 2020 The Kubernetes Authors.

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

// Package testutil 提供指标相关的测试辅助：抓取对比、Prometheus 规范 lint 等。
package testutil

import (
	"fmt"
	"io"
	"strings"

	"github.com/prometheus/client_golang/prometheus/testutil/promlint"
)

// exceptionMetrics 是 promlint 规则的豁免清单。
//
// 清单最初收录引入 promlint 时已存在的违规指标，用于放行存量问题、避免 CI 失败。
// 新增指标应直接修正问题，而不是加入清单。
var exceptionMetrics = []string{
	// k8s.io/apiserver/pkg/server/egressselector
	"apiserver_egress_dialer_dial_failure_count", // counter 指标应使用 "_total" 后缀

	// k8s.io/apiserver/pkg/endpoints/filters
	"authenticated_user_requests", // counter 指标应使用 "_total" 后缀
	"authentication_attempts",     // counter 指标应使用 "_total" 后缀

	// kube-apiserver
	"aggregator_openapi_v2_regeneration_count",                    // counter 指标应使用 "_total" 后缀；非 histogram 和 summary 指标不应使用 "_count" 后缀
	"apiserver_admission_step_admission_duration_seconds_summary", // 指标名称不应包含类型 'summary'
	"apiserver_admission_webhook_fail_open_count",                 // counter 指标应使用 "_total" 后缀；非 histogram 和 summary 指标不应使用 "_count" 后缀
	"apiserver_admission_webhook_rejection_count",                 // counter 指标应使用 "_total" 后缀；非 histogram 和 summary 指标不应使用 "_count" 后缀
	"apiserver_flowcontrol_latest_s",                              // 指标名称不应使用缩写单位
	"apiserver_flowcontrol_next_discounted_s_bounds",              // 指标名称不应使用缩写单位
	"apiserver_flowcontrol_next_s_bounds",                         // 指标名称不应使用缩写单位
	"authentication_token_cache_active_fetch_count",               // 缺少 HELP 文本；非 histogram 和 summary 指标不应使用 "_count" 后缀
	"authentication_token_cache_fetch_total",                      // 缺少 HELP 文本
	"authentication_token_cache_request_duration_seconds",         // 缺少 HELP 文本
	"authentication_token_cache_request_total",                    // 缺少 HELP 文本
	"apiserver_watch_shards_total",                                // 非 counter 指标不应使用 "_total" 后缀

	// apiextensions-apiserver
	"apiextensions_openapi_v2_regeneration_count", // counter 指标应使用 "_total" 后缀；非 histogram 和 summary 指标不应使用 "_count" 后缀
	"apiextensions_openapi_v3_regeneration_count", // counter 指标应使用 "_total" 后缀；非 histogram 和 summary 指标不应使用 "_count" 后缀

	// attach-detach 控制器
	"attach_detach_controller_attachdetach_controller_forced_detaches", // counter 指标应使用 "_total" 后缀
	"endpoint_slice_controller_changes",                                // counter 指标应使用 "_total" 后缀
	"endpoint_slice_controller_syncs",                                  // counter 指标应使用 "_total" 后缀
	"endpoint_slice_mirroring_controller_changes",                      // counter 指标应使用 "_total" 后缀

	// kubelet
	"kubelet_container_aligned_compute_resources_count",         // counter 指标应使用 "_total" 后缀；非 histogram 和 summary 指标不应使用 "_count" 后缀
	"kubelet_container_aligned_compute_resources_failure_count", // counter 指标应使用 "_total" 后缀；非 histogram 和 summary 指标不应使用 "_count" 后缀
	"kubelet_cpu_manager_exclusive_cpu_allocation_count",        // 非 histogram 和 summary 指标不应使用 "_count" 后缀
	"kubelet_evented_pleg_connection_error_count",               // counter 指标应使用 "_total" 后缀；非 histogram 和 summary 指标不应使用 "_count" 后缀
	"kubelet_evented_pleg_connection_success_count",             // counter 指标应使用 "_total" 后缀；非 histogram 和 summary 指标不应使用 "_count" 后缀
	"kubelet_evictions",                                          // counter 指标应使用 "_total" 后缀
	"kubelet_pleg_discard_events",                                // counter 指标应使用 "_total" 后缀
	"kubelet_pod_resources_endpoint_errors_get",                  // counter 指标应使用 "_total" 后缀
	"kubelet_pod_resources_endpoint_errors_get_allocatable",      // counter 指标应使用 "_total" 后缀
	"kubelet_pod_resources_endpoint_errors_list",                 // counter 指标应使用 "_total" 后缀
	"kubelet_pod_resources_endpoint_requests_get",                // counter 指标应使用 "_total" 后缀
	"kubelet_pod_resources_endpoint_requests_get_allocatable",    // counter 指标应使用 "_total" 后缀
	"kubelet_pod_resources_endpoint_requests_list",               // counter 指标应使用 "_total" 后缀
	"kubelet_preemptions",                                        // counter 指标应使用 "_total" 后缀
	"kubelet_server_expiration_renew_errors",                     // counter 指标应使用 "_total" 后缀
	"kubelet_topology_manager_admission_duration_ms",             // 指标名称不应使用缩写单位
	"kubelet_certificate_manager_client_expiration_renew_errors", // counter 指标应使用 "_total" 后缀
	"kubelet_pod_resize_duration_milliseconds",                   // 应使用基本单位 "seconds" 而非 "milliseconds"

	// kube-proxy
	"kubeproxy_sync_proxy_rules_iptables_total",           // 非 counter 指标不应使用 "_total" 后缀
	"kubeproxy_sync_proxy_rules_no_local_endpoints_total", // 非 counter 指标不应使用 "_total" 后缀

	// volume-manager
	"volume_manager_selinux_container_errors_total",                 // 非 counter 指标不应使用 "_total" 后缀
	"volume_manager_selinux_container_warnings_total",               // 非 counter 指标不应使用 "_total" 后缀
	"volume_manager_selinux_pod_context_mismatch_errors_total",      // 非 counter 指标不应使用 "_total" 后缀
	"volume_manager_selinux_pod_context_mismatch_warnings_total",    // 非 counter 指标不应使用 "_total" 后缀
	"volume_manager_selinux_volume_context_mismatch_errors_total",   // 非 counter 指标不应使用 "_total" 后缀
	"volume_manager_selinux_volume_context_mismatch_warnings_total", // 非 counter 指标不应使用 "_total" 后缀
	"volume_manager_selinux_volumes_admitted_total",                 // 非 counter 指标不应使用 "_total" 后缀

	// persistent-volume-controller
	"volume_operation_total_errors", // counter 指标应使用 "_total" 后缀
}

// Problem 表示 Linter 检测到的问题。
//
// 本类型是对 promlint.Problem 的别名式包装，便于统一格式化输出。
type Problem promlint.Problem

// String 输出 "指标名:问题描述"，方便直接拼进错误信息。
func (p *Problem) String() string {
	return fmt.Sprintf("%s:%s", p.Metric, p.Text)
}

// Linter 是 Prometheus 指标检查器，检测指标名称、类型和元数据中的问题并报告给调用方。
//
// Linter 在 promlint 之上叠加 kube 的豁免清单。
type Linter struct {
	promLinter *promlint.Linter
}

// Lint 检查指标流并返回发现的问题；结果按指标名称和问题描述排序。
//
// 过滤掉豁免指标后再返回，调用方只需关注真正需要修的问题。
func (l *Linter) Lint() ([]Problem, error) {
	promProblems, err := l.promLinter.Lint()
	if err != nil {
		return nil, err
	}

	// 忽略例外清单中的问题
	problems := make([]Problem, 0, len(promProblems))
	for i := range promProblems {
		if !l.shouldIgnore(promProblems[i].Metric) {
			problems = append(problems, Problem(promProblems[i]))
		}
	}

	return problems, nil
}

// shouldIgnore 判断指标是否位于例外清单中。
func (l *Linter) shouldIgnore(metricName string) bool {
	for i := range exceptionMetrics {
		if metricName == exceptionMetrics[i] {
			return true
		}
	}

	return false
}

// NewPromLinter 创建一个读取 Prometheus 指标输入流的 Linter，仅支持文本展示格式。
func NewPromLinter(r io.Reader) *Linter {
	return &Linter{
		promLinter: promlint.New(r),
	}
}

// mergeProblems 把多个问题拼成单行，用作错误消息主体。
func mergeProblems(problems []Problem) string {
	var problemsMsg []string

	for index := range problems {
		problemsMsg = append(problemsMsg, problems[index].String())
	}

	return strings.Join(problemsMsg, ",")
}

// shouldIgnore 判断指标是否位于例外清单中。
//
// 包级版本供没有 Linter 实例的调用路径使用。
func shouldIgnore(metricName string) bool {
	for i := range exceptionMetrics {
		if metricName == exceptionMetrics[i] {
			return true
		}
	}

	return false
}

// GetLintError 忽略例外清单中的指标，并将其余检查问题转换为错误。
//
// GetLintError 把过滤后的剩余问题聚合成一个 error；没有问题时返回 nil。
func GetLintError(problems []promlint.Problem) error {
	var filteredProblems []Problem
	for _, problem := range problems {
		if shouldIgnore(problem.Metric) {
			continue
		}

		filteredProblems = append(filteredProblems, Problem(problem))
	}

	if len(filteredProblems) == 0 {
		return nil
	}

	return fmt.Errorf("lint error: %s", mergeProblems(filteredProblems))
}

// CheckUnusedExceptions 在例外清单中的任何指标均无检查问题时返回错误。
//
// 若某个豁免项已不再违规，说明可以把它从清单中删除；该检查防止清单无限膨胀。
func CheckUnusedExceptions(problems []promlint.Problem) error {
	usedExceptions := make(map[string]bool)
	for _, p := range problems {
		if shouldIgnore(p.Metric) {
			usedExceptions[p.Metric] = true
		}
	}

	var unused []string
	for _, exc := range exceptionMetrics {
		if !usedExceptions[exc] {
			unused = append(unused, exc)
		}
	}

	if len(unused) > 0 {
		return fmt.Errorf("metrics in exception list but have no violations: %s", strings.Join(unused, ", "))
	}
	return nil
}
