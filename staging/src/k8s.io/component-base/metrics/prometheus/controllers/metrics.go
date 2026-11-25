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

// Package controllers 提供描述受管控制器运行位置与数量的指标，
// 用于在多个管理器中判断某个控制器实际由谁运行。
package controllers

import (
	"sync"

	k8smetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

var (
	// once 保证指标只注册一次，多个组件调用 Register 不会重复注册同一 GaugeVec。
	once                    sync.Once
	controllerInstanceCount = k8smetrics.NewGaugeVec(
		&k8smetrics.GaugeOpts{
			Name:           "running_managed_controllers",
			Help:           "Indicates where instances of a controller are currently running",
			StabilityLevel: k8smetrics.BETA,
		},
		[]string{"name", "manager"},
	)
)

// ControllerManagerMetrics 是设置特定 controller manager 指标的代理。
type ControllerManagerMetrics struct {
	// manager 标识指标来源（如 kube-controller-manager 或 cloud controller 管理器）。
	manager string
}

// NewControllerManagerMetrics 创建具有指定管理器名称的 ControllerManagerMetrics。
func NewControllerManagerMetrics(manager string) *ControllerManagerMetrics {
	controllerMetrics := &ControllerManagerMetrics{
		manager: manager,
	}
	return controllerMetrics
}

// Register 注册 controller manager 指标。
//
// 由组件在初始化时调用；实际注册动作只执行一次。
func Register() {
	once.Do(func() {
		legacyregistry.MustRegister(controllerInstanceCount)
	})
}

// ControllerStarted 将 controllerInstanceCount 设置为 1。
// 使用 Set 而非 Inc/Dec，避免控制器启动后异常退出且未正确上报时被重复计数。
func (a *ControllerManagerMetrics) ControllerStarted(name string) {
	controllerInstanceCount.With(k8smetrics.Labels{"name": name, "manager": a.manager}).Set(float64(1))
}

// ControllerStopped 将 controllerInstanceCount 设置为 0。
//
// 同样使用 Set 而非 Dec：异常退出可能没有对应的 Started/Stopped 配对，直接归零更可靠。
func (a *ControllerManagerMetrics) ControllerStopped(name string) {
	controllerInstanceCount.With(k8smetrics.Labels{"name": name, "manager": a.manager}).Set(float64(0))
}
