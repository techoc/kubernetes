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

package metrics

import (
	"sync"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

// subsystem 故意使用通用名称，因为类似的指标在别处也存在。
const subsystem = "dynamic_resource_allocation"

// NumResourceClaimLabels 定义 ResourceClaim 数量指标的标签维度。
type NumResourceClaimLabels struct {
	// Allocated 表示 ResourceClaim 是否已分配。
	Allocated string
	// AdminAccess 表示 ResourceClaim 是否用于管理员访问。
	AdminAccess string
	// Source 表示 ResourceClaim 的创建来源。
	Source string
}

var (
	// NumResourceClaimsDesc 记录 ResourceClaim 的数量，
	// 按分配状态、管理员访问权限和来源分类。
	// 来源可以是 'resource_claim_template'（由模板创建）、
	// 'extended_resource'（扩展资源），或为空（用户手动创建）。
	NumResourceClaimsDesc = metrics.NewDesc(
		metrics.BuildFQName("", subsystem, "resource_claims"),
		"Number of ResourceClaims, categorized by allocation status, admin access, and source. "+
			"Source can be 'resource_claim_template' (created from a template), "+
			"'extended_resource' (extended resources), or empty (manually created by a user).",
		[]string{"allocated", "admin_access", "source"}, nil,
		metrics.ALPHA, "")
)

// registerMetrics 保证指标只注册一次。
var registerMetrics sync.Once

// testMode 表示是否运行在测试模式。
// 测试模式下不向全局注册表注册自定义 collector，避免测试间相互干扰。
var testMode bool

// SetTestMode 开启或关闭测试模式。
func SetTestMode(enabled bool) {
	testMode = enabled
}

// RegisterMetrics 注册 ResourceClaim 指标。
func RegisterMetrics(collector metrics.StableCollector) {
	registerMetrics.Do(func() {
		if !testMode && collector != nil {
			// 仅非测试模式下注册，避免测试中重复注册导致 panic。
			legacyregistry.CustomMustRegister(collector)
		}
	})
}
