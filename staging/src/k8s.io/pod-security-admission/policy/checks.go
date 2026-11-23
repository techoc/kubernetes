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

package policy

import (
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/pod-security-admission/api"
)

// 把多条版本化检查打包成一个具名检查，版本按递增顺序排列，求值时取适用的最后一条。
type Check struct {
	// ID is the unique ID of the check.
	// ID 同时是覆盖关系的引用键，必须全局唯一。
	ID CheckID
	// Level is the policy level this check belongs to.
	// Must be Baseline or Restricted.
	// Baseline checks are evaluated for baseline and restricted namespaces.
	// Restricted checks are only evaluated for restricted namespaces.
	// 等级决定适用范围：受限策略会连带跑基线检查，但基线策略不跑受限检查。
	Level api.Level
	// Versions contains one or more revisions of the check that apply to different versions.
	// If the check is not yet assigned to a version, this must be a single-item list with a MinimumVersion of "".
	// Otherwise, MinimumVersion of items must represent strictly increasing versions.
	// 用递增起点而非区间，可让未来版本自动继承最近的一条实现。
	Versions []VersionedCheck
}

// 同一检查在不同策略版本可有不同的实现，例如在后续版本放宽 user namespace 场景。
type VersionedCheck struct {
	// MinimumVersion is the first policy version this check applies to.
	// If unset, this check is not yet assigned to a policy version.
	// If set, must not be "latest".
	// 留空表示尚未纳入版本管理，求值时应归入实验性检查。
	MinimumVersion api.Version
	// CheckPod determines if the pod is allowed.
	// 求值函数保持无状态，同一 Pod 重复调用结果一致。
	CheckPod CheckPodFn
	// OverrideCheckIDs is an optional list of checks that should be skipped when this check is run.
	// Overrides may only be set on restricted checks, and may only override baseline checks.
	// 受限等级借此收紧基线中已放宽的场景，避免出现双重宽松判定。
	OverrideCheckIDs []CheckID
}

// 只传元数据与 PodSpec，避免求值函数依赖完整对象及其客户端状态。
type CheckPodFn func(*metav1.ObjectMeta, *corev1.PodSpec) CheckResult

// 用字符串类型而非结构体，便于在覆盖列表和注册键中直接比较。
type CheckID string

// CheckResult contains the result of checking a pod and indicates whether the pod is allowed,
// and if not, why it was forbidden.
//
// Example output for (false, "host ports", "8080, 9090"):
//
//	When checking all pods in a namespace:
//	  disallowed by policy "baseline": host ports, privileged containers, non-default capabilities
//	When checking an individual pod:
//	  disallowed by policy "baseline": host ports (8080, 9090), privileged containers, non-default capabilities (CAP_NET_RAW)
//
// 原因与详情分层：原因可跨 Pod 合并统计，详情只在检查单个对象时展开具体值。
type CheckResult struct {
	// Allowed indicates if the check allowed the pod.
	Allowed bool
	// ForbiddenReason must be set if Allowed is false.
	// ForbiddenReason should be as succinct as possible and is always output.
	// 简短且不含具体值，才能在命名空间预检中按原因聚合警告。
	// Examples:
	// - "host ports"
	// - "privileged containers"
	// - "non-default capabilities"
	ForbiddenReason string
	// ForbiddenDetail should only be set if Allowed is false, and is optional.
	// ForbiddenDetail can include specific values that were disallowed and is used when checking an individual object.
	// 可以带上被禁止的具体值；允许时可留空，避免无意义的细节输出。
	// Examples:
	// - list specific invalid host ports: "8080, 9090"
	// - list specific invalid containers: "container1, container2"
	// - list specific non-default capabilities: "CAP_NET_RAW"
	ForbiddenDetail string
}

// AggergateCheckResult holds the aggregate result of running CheckPod across multiple checks.
// 两个切片按下标一一对应，格式化详情时才能把值与原因配对。
type AggregateCheckResult struct {
	// Allowed indicates if all checks allowed the pod.
	Allowed bool
	// ForbiddenReasons is a slice of the forbidden reasons from all the forbidden checks. It should not include empty strings.
	// ForbiddenReasons and ForbiddenDetails must have the same number of elements, and the indexes are for the same check.
	// 聚合后可能跨多条检查合并展示，但不在此处排序，由调用方决定顺序。
	ForbiddenReasons []string
	// ForbiddenDetails is a slice of the forbidden details from all the forbidden checks. It may include empty strings.
	// ForbiddenReasons and ForbiddenDetails must have the same number of elements, and the indexes are for the same check.
	// 允许为空串表示该检查没有可展示的具体值。
	ForbiddenDetails []string
}

// ForbiddenReason returns a comma-separated string of of the forbidden reasons.
// Example: host ports, privileged containers, non-default capabilities
// 命名空间预检按原因汇总，因此这里只拼接原因，不带具体值。
func (a *AggregateCheckResult) ForbiddenReason() string {
	return strings.Join(a.ForbiddenReasons, ", ")
}

// ForbiddenDetail returns a detailed forbidden message, with non-empty details formatted in
// parentheses with the associated reason.
// Example: host ports (8080, 9090), privileged containers, non-default capabilities (NET_RAW)
// 仅把非空的详情放进括号，空串时保持原因简洁。
func (a *AggregateCheckResult) ForbiddenDetail() string {
	var b strings.Builder
	for i := 0; i < len(a.ForbiddenReasons); i++ {
		b.WriteString(a.ForbiddenReasons[i])
		if a.ForbiddenDetails[i] != "" {
			b.WriteString(" (")
			b.WriteString(a.ForbiddenDetails[i])
			b.WriteString(")")
		}
		if i != len(a.ForbiddenReasons)-1 {
			b.WriteString(", ")
		}
	}
	return b.String()
}

// UnknownForbiddenReason is used as the placeholder forbidden reason for checks that incorrectly disallow without providing a reason.
// 检查实现漏填原因时仍能产出可聚合的结果，避免静默丢弃该条违规。
const UnknownForbiddenReason = "unknown forbidden reason"

// AggregateCheckPod runs all the checks and aggregates the forbidden results into a single CheckResult.
// The aggregated reason is a comma-separated
// 聚合顺序即传入顺序，调用方负责稳定排序以便输出可复现。
func AggregateCheckResults(results []CheckResult) AggregateCheckResult {
	var (
		reasons []string
		details []string
	)
	for _, result := range results {
		if !result.Allowed {
			if len(result.ForbiddenReason) == 0 {
				reasons = append(reasons, UnknownForbiddenReason)
			} else {
				reasons = append(reasons, result.ForbiddenReason)
			}
			details = append(details, result.ForbiddenDetail)
		}
	}
	return AggregateCheckResult{
		Allowed:          len(reasons) == 0,
		ForbiddenReasons: reasons,
		ForbiddenDetails: details,
	}
}

// 两组互斥：未分配版本的检查默认不生效，需显式开启实验性策略才会启用。
var (
	defaultChecks      []func() Check
	experimentalChecks []func() Check
)

// 各检查在 init 中由此注册；存构造函数而非实例，便于每次取到全新副本。
func addCheck(f func() Check) {
	// add to experimental or versioned list
	c := f()
	if len(c.Versions) == 1 && c.Versions[0].MinimumVersion == (api.Version{}) {
		experimentalChecks = append(experimentalChecks, f)
	} else {
		defaultChecks = append(defaultChecks, f)
	}
}

// DefaultChecks returns checks that are expected to be enabled by default.
// The results are mutually exclusive with ExperimentalChecks.
// It returns a new copy of checks on each invocation and is expected to be called once at setup time.
// 返回副本可防止调用方修改共享注册表中的构造函数。
func DefaultChecks() []Check {
	retval := make([]Check, 0, len(defaultChecks))
	for _, f := range defaultChecks {
		retval = append(retval, f())
	}
	return retval
}

// ExperimentalChecks returns checks that have not yet been assigned to policy versions.
// The results are mutually exclusive with DefaultChecks.
// It returns a new copy of checks on each invocation and is expected to be called once at setup time.
// 与 DefaultChecks 返回互斥集合，避免同一检查被重复注册而产出重复警告。
func ExperimentalChecks() []Check {
	retval := make([]Check, 0, len(experimentalChecks))
	for _, f := range experimentalChecks {
		retval = append(retval, f())
	}
	return retval
}
