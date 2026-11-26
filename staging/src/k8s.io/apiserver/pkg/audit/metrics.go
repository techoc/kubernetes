/*
Copyright 2017 The Kubernetes Authors.

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

package audit

import (
	"context"
	"fmt"

	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/klog/v2"
)

const (
	// subsystem 作为所有审计指标名的统一前缀 apiserver_audit_，指标名一旦发布即成为
	// 外部契约（告警、仪表盘依赖），不能随意更名。
	subsystem = "apiserver_audit"
)

/*
 * 默认地，以下所有指标的稳定性等级均为 ALPHA：https://github.com/kubernetes/enhancements/blob/master/keps/sig-instrumentation/1209-metrics-stability/kubernetes-control-plane-metrics-stability.md#stability-classes)
 *
 * 提升指标稳定性等级是组件所有者的责任，因为按照指标稳定性策略，这涉及明确承诺在多个发布版本中支持该指标。
 */
var (
	eventCounter = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      subsystem,
			Name:           "event_total",
			Help:           "Counter of audit events generated and sent to the audit backend.",
			StabilityLevel: metrics.ALPHA,
		})
	errorCounter = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem: subsystem,
			Name:      "error_total",
			Help: "Counter of audit events that failed to be audited properly. " +
				"Plugin identifies the plugin affected by the error.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"plugin"},
	)
	levelCounter = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      subsystem,
			Name:           "level_total",
			Help:           "Counter of policy levels for audit events (1 per request).",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"level"},
	)

	ApiserverAuditDroppedCounter = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem: subsystem,
			Name:      "requests_rejected_total",
			Help: "Counter of apiserver requests rejected due to an error " +
				"in audit logging backend.",
			StabilityLevel: metrics.ALPHA,
		},
	)
)

func init() {
	// 直接在 init 中注册：审计指标不依赖 feature gate，注册失败属于编程错误，
	// 用 MustRegister 让进程尽早失败而不是静默缺失指标。
	legacyregistry.MustRegister(eventCounter)
	legacyregistry.MustRegister(errorCounter)
	legacyregistry.MustRegister(levelCounter)
	legacyregistry.MustRegister(ApiserverAuditDroppedCounter)
}

// ObserveEvent 为产生的审计事件更新相应的 prometheus 指标。
// 每个真正产生并交给后端的审计事件计一次。
func ObserveEvent(ctx context.Context) {
	eventCounter.WithContext(ctx).Inc()
}

// ObservePolicyLevel 按请求的审计级别更新相应的 prometheus 指标。
// 每个请求只调用一次，因此该计数可用来估算不同审计级别下的请求量。
func ObservePolicyLevel(ctx context.Context, level auditinternal.Level) {
	levelCounter.WithContext(ctx).WithLabelValues(string(level)).Inc()
}

// HandlePluginError 处理审计插件中发生的错误。仅当该错误可能导致审计事件未被正确记录时才应调用。
// 相关事件会写入 debug 日志。
// 相关事件会写入 debug 日志。
//
// 计数按受影响事件数累加（而非按错误次数），这样能直观看出丢失了多少条审计记录；
// 事件内容打到日志里是为了事后取证，可能包含敏感信息，日志级别应保持可控。
func HandlePluginError(plugin string, err error, impacted ...*auditinternal.Event) {
	// 对错误计数。
	errorCounter.WithLabelValues(plugin).Add(float64(len(impacted)))

	// 将审计事件写入 debug 日志。
	msg := fmt.Sprintf("Error in audit plugin '%s' affecting %d audit events: %v\nImpacted events:\n",
		plugin, len(impacted), err)
	for _, ev := range impacted {
		msg = msg + EventString(ev) + "\n"
	}
	klog.Error(msg)
}
