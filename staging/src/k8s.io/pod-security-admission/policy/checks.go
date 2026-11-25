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
// Check 描述一项策略检查及其版本化实现和覆盖关系。
type Check struct {
	// ID 是检查的唯一标识，也是覆盖关系的引用键，必须全局唯一。
	ID CheckID
	// Level 是检查所属等级，只能为 Baseline 或 Restricted。
	// Baseline 检查用于 baseline 和 restricted 命名空间；Restricted 检查只用于 restricted 命名空间。

	Level api.Level
	// Versions 保存适用于不同策略版本的实现。
	// 尚未分配版本时只能有一项，且 MinimumVersion 为 ""；
	// 否则 MinimumVersion 必须严格递增。
	// 用递增起点而非区间，可让未来版本自动继承最近的一条实现。
	Versions []VersionedCheck
}

// VersionedCheck 描述检查在单个策略版本中的实现；不同版本可分别放宽 user namespace 等场景。
type VersionedCheck struct {
	// MinimumVersion 是检查首次适用的策略版本。
	// 留空表示尚未分配策略版本，归入实验性检查；设置时不能为 "latest"。
	MinimumVersion api.Version
	// CheckPod 判定 Pod 是否允许通过。
	// 求值函数保持无状态，同一 Pod 重复调用结果一致。
	CheckPod CheckPodFn
	// OverrideCheckIDs 是运行本检查时应跳过的检查 ID 列表。
	// 只有 Restricted 检查可设置覆盖，且只能覆盖 Baseline 检查。
	// 受限等级借此收紧基线中已放宽的场景，避免出现双重宽松判定。
	OverrideCheckIDs []CheckID
}

// CheckPodFn 是无状态的 Pod 检查函数，只依赖元数据与 PodSpec，避免耦合完整对象及客户端状态。
type CheckPodFn func(*metav1.ObjectMeta, *corev1.PodSpec) CheckResult

// CheckID 是检查的字符串标识，便于在覆盖列表和注册键中直接比较。
type CheckID string

// CheckResult 保存 Pod 检查结果，说明是否允许通过以及拒绝原因。
//
// 当 Allowed 为 false、ForbiddenReason 为 "host ports"、ForbiddenDetail 为 "8080, 9090" 时，输出示例：
//
//	检查命名空间内全部 Pod 时：
//	  disallowed by policy "baseline": host ports, privileged containers, non-default capabilities
//	检查单个 Pod 时：
//	  disallowed by policy "baseline": host ports (8080, 9090), privileged containers, non-default capabilities (CAP_NET_RAW)
//
// 原因与详情分层：原因可跨 Pod 合并统计，详情只在检查单个对象时展开具体值。
type CheckResult struct {
	// Allowed 表示检查是否允许该 Pod 通过。
	Allowed bool
	// Allowed 为 false 时必须设置 ForbiddenReason。它应简短且不含具体值，
	// 因为始终输出，并用于命名空间预检的按原因聚合。
	// 示例：
	// - "host ports"
	// - "privileged containers"
	// - "non-default capabilities"
	ForbiddenReason string
	// ForbiddenDetail 可选，只应在 Allowed 为 false 时设置；可包含被拒绝的具体值，
	// 仅在检查单个对象时使用，允许时留空以避免无意义输出。
	// 示例：
	// - 列出非法宿主机端口："8080, 9090"
	// - 列出非法容器："container1, container2"
	// - 列出非默认能力："CAP_NET_RAW"
	ForbiddenDetail string
}

// AggregateCheckResult 保存多项 CheckPod 检查的聚合结果。
// 两个切片按下标一一对应，格式化详情时才能把值与原因配对。
type AggregateCheckResult struct {
	// Allowed 表示是否所有检查都允许该 Pod 通过。
	Allowed bool
	// ForbiddenReasons 保存所有拒绝检查的原因，不能包含空串。
	// 两个切片数量必须相同，相同下标对应同一检查。
	// 聚合后可能跨多条检查合并展示，但不在此处排序，由调用方决定顺序。
	ForbiddenReasons []string
	// ForbiddenDetails 保存所有拒绝检查的详情，允许包含空串。
	// 两个切片数量必须相同，相同下标对应同一检查。
	// 允许为空串表示该检查没有可展示的具体值。
	ForbiddenDetails []string
}

// ForbiddenReason 返回以逗号分隔的原因，供命名空间预检聚合，不带具体值。
// 示例：host ports, privileged containers, non-default capabilities
func (a *AggregateCheckResult) ForbiddenReason() string {
	return strings.Join(a.ForbiddenReasons, ", ")
}

// ForbiddenDetail 返回详细拒绝信息，仅将非空详情用括号附在对应原因后。
// 示例：host ports (8080, 9090), privileged containers, non-default capabilities (NET_RAW)
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

// UnknownForbiddenReason 是检查拒绝却漏填原因时的占位值，确保违规仍可聚合。
const UnknownForbiddenReason = "unknown forbidden reason"

// AggregateCheckResults 将全部检查的拒绝结果聚合为一个 CheckResult。
// 聚合原因是逗号分隔的字符串。
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
	// 按是否分配版本，分别加入实验性或版本化列表。
	c := f()
	if len(c.Versions) == 1 && c.Versions[0].MinimumVersion == (api.Version{}) {
		experimentalChecks = append(experimentalChecks, f)
	} else {
		defaultChecks = append(defaultChecks, f)
	}
}

// DefaultChecks 返回默认启用的检查。
// 与 ExperimentalChecks 互斥。
// 每次调用返回新副本，供初始化阶段注册，避免修改共享注册表。
func DefaultChecks() []Check {
	retval := make([]Check, 0, len(defaultChecks))
	for _, f := range defaultChecks {
		retval = append(retval, f())
	}
	return retval
}

// ExperimentalChecks 返回尚未分配策略版本的实验性检查。
// 与 DefaultChecks 互斥。
// 每次调用返回新副本，供初始化阶段注册，避免重复注册和共享状态。
func ExperimentalChecks() []Check {
	retval := make([]Check, 0, len(experimentalChecks))
	for _, f := range experimentalChecks {
		retval = append(retval, f())
	}
	return retval
}
