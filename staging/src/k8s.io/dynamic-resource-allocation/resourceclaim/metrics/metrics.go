/*
Copyright The Kubernetes Authors.

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
	"sync"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

// subsystem 为指标子系统名称。这里故意使用通用名称，
// 因为该指标会同时由 kube-controller-manager 与 kube-scheduler 暴露。
const subsystem = "dynamic_resource_allocation"

var (
	// ResourceClaimCreate 记录 ResourceClaim 创建请求的总数，
	// 按创建状态、是否管理员访问、所属 API 组与类型（Pod 或 scheduling.k8s.io/PodGroup）分类。
	// 由于它由 kube-controller-manager 与 kube-scheduler 共同采集，采集组件本身构成另一个区分维度。
	ResourceClaimCreate = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      subsystem,
			Name:           "resourceclaim_creates_total",
			Help:           "Number of ResourceClaims creation requests, categorized by creation status, admin access, and owning API group and kind (either Pod or scheduling.k8s.io/PodGroup)",
			StabilityLevel: metrics.ALPHA,
		},
		// 标签维度：创建状态、管理员访问、所有者 API 组与所有者类型。
		[]string{"status", "admin_access", "owner_api_group", "owner_api_kind"},
	)
)

// registerMetrics 保证指标只注册一次，避免重复注册导致 panic。
var registerMetrics sync.Once

// RegisterMetrics 将 ResourceClaim 创建计数指标注册到 legacyregistry。
// 借助 sync.Once，即使并发调用也只会执行一次注册。
func RegisterMetrics() {
	// 仅第一次调用时执行注册逻辑。
	registerMetrics.Do(func() {
		legacyregistry.MustRegister(ResourceClaimCreate)
	})
}
