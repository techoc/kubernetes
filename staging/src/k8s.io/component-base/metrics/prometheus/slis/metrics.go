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

// Package slis 暴露 Kubernetes SLI（服务级别指示器）健康检查指标。
//
// 两个指标都是 STABLE：名称与 label 属于对外契约，监控系统依赖其稳定性。
package slis

import (
	"context"

	k8smetrics "k8s.io/component-base/metrics"
)

// HealthcheckStatus 是健康检查结果，取值直接作为 counter 的 label，
// 因此只允许 success/error 两种，避免基数失控。
type HealthcheckStatus string

const (
	// Success 表示健康检查成功。
	Success HealthcheckStatus = "success"
	// Error 表示健康检查失败。
	Error HealthcheckStatus = "error"
)

var (
	// healthcheck 是记录 Kubernetes 单次健康检查结果的 Prometheus Gauge。
	healthcheck = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Namespace:      "kubernetes",
			Name:           "healthcheck",
			Help:           "This metric records the result of a single healthcheck.",
			StabilityLevel: k8smetrics.STABLE,
		},
		[]string{"name", "type"},
	)

	// healthchecksTotal 是累计 Kubernetes 健康检查结果的 Prometheus Counter。
	healthchecksTotal = k8smetrics.NewCounterVec(
		&k8smetrics.CounterOpts{
			Namespace:      "kubernetes",
			Name:           "healthchecks_total",
			Help:           "This metric records the results of all healthcheck.",
			StabilityLevel: k8smetrics.STABLE,
		},
		[]string{"name", "type", "status"},
	)
)

// Register 把健康检查指标注册到给定 registry，并顺带注册进程启动时间，
// 后者是计算 SLI 速率所需的分母。
func Register(registry k8smetrics.KubeRegistry) {
	registry.Register(healthcheck)
	registry.Register(healthchecksTotal)
	_ = k8smetrics.RegisterProcessStartTime(registry.Register)
}

// ResetHealthMetrics 清空两个指标的序列；仅供测试做隔离使用。
func ResetHealthMetrics() {
	healthcheck.Reset()
	healthchecksTotal.Reset()
}

// ObserveHealthcheck 记录一次健康检查：gauge 反映当前状态（success=1，
// 其余为 0），counter 累加该状态的次数，便于同时看瞬时状态与历史比例。
func ObserveHealthcheck(ctx context.Context, name string, healthcheckType string, status HealthcheckStatus) error {
	if status == Success {
		healthcheck.WithContext(ctx).WithLabelValues(name, healthcheckType).Set(1)
	} else {
		healthcheck.WithContext(ctx).WithLabelValues(name, healthcheckType).Set(0)
	}

	healthchecksTotal.WithContext(ctx).WithLabelValues(name, healthcheckType, string(status)).Inc()
	return nil
}
