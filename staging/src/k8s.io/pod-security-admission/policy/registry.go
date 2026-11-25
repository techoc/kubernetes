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
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/pod-security-admission/api"
)

// Evaluator 暴露策略求值接口，使准入控制器不依赖具体实现，便于替换实现或测试。
type Evaluator interface {
	// EvaluatePod 按给定等级与版本评估 Pod，返回检查结果列表，由调用方决定警告或拒绝。
	EvaluatePod(lv api.LevelVersion, podMetadata *metav1.ObjectMeta, podSpec *corev1.PodSpec) []CheckResult
}

// checkRegistry 是 Evaluator 的默认实现，按版本预展开检查表以免逐次遍历规则。
type checkRegistry struct {
	// 检查表按策略版本缓存函数，因为同一检查在不同版本可能有不同实现。
	baselineChecks, restrictedChecks map[api.Version][]CheckPodFn
	// maxVersion 是缓存的最大版本，至少为所有检查的最大 MinimumVersion；
	// 更新版本回落到此值，避免缓存无限膨胀。
	maxVersion api.Version
}

// NewEvaluator 从检查列表构造 Evaluator；列表非法时返回错误。合法列表须满足：
// 1. Check.ID 在列表内唯一
// 2. Check.Level 只能为 Baseline 或 Restricted
// 3. 版本集合非空，且 MinimumVersion 严格递增
// 4. Check.Versions 不能包含 'latest'
// 构造时先校验再展开，防止非法检查表在准入请求到来时才暴露问题。
// emulationVersion 用于模拟旧版本行为，便于在不降级集群的情况下复现历史策略表现。
func NewEvaluator(checks []Check, emulationVersion *api.Version) (*checkRegistry, error) {
	if err := validateChecks(checks); err != nil {
		return nil, err
	}
	r := &checkRegistry{
		baselineChecks:   map[api.Version][]CheckPodFn{},
		restrictedChecks: map[api.Version][]CheckPodFn{},
	}
	populate(r, checks)

	// 模拟旧次版本时只下调缓存上限，避免查到未展开的版本。
	if emulationVersion != nil && (*emulationVersion).Older(r.maxVersion) {
		r.maxVersion = *emulationVersion
	}

	return r, nil
}

// EvaluatePod 按请求的级别与版本取出预展开的检查函数并逐个执行。
// privileged 档不设限制，直接返回避免无谓计算。
func (r *checkRegistry) EvaluatePod(lv api.LevelVersion, podMetadata *metav1.ObjectMeta, podSpec *corev1.PodSpec) []CheckResult {
	if lv.Level == api.LevelPrivileged {
		return nil
	}
	// 未来版本沿用最近已知版本的规则，新版本上线前策略行为可预测。
	if r.maxVersion.Older(lv.Version) {
		lv.Version = r.maxVersion
	}

	var checks []CheckPodFn
	if lv.Level == api.LevelBaseline {
		checks = r.baselineChecks[lv.Version]
	} else {
		// restricted 包含未被覆盖的 baseline 检查，确保它是基线的超集。
		checks = r.restrictedChecks[lv.Version]
	}

	var results []CheckResult
	for _, check := range checks {
		results = append(results, check(podMetadata, podSpec))
	}
	return results
}

// validateChecks 在建表前一次性校验检查定义，错误在启动阶段暴露而不是等到准入请求。
// 第一遍检查 ID 唯一性、级别与版本序列；第二遍单独校验覆盖关系。
func validateChecks(checks []Check) error {
	ids := map[CheckID]api.Level{}
	for _, check := range checks {
		if _, ok := ids[check.ID]; ok {
			return fmt.Errorf("multiple checks registered for ID %s", check.ID)
		}
		ids[check.ID] = check.Level
		if check.Level != api.LevelBaseline && check.Level != api.LevelRestricted {
			return fmt.Errorf("check %s: invalid level %s", check.ID, check.Level)
		}
		if len(check.Versions) == 0 {
			return fmt.Errorf("check %s: empty", check.ID)
		}
		maxVersion := api.Version{}
		for _, c := range check.Versions {
			if c.MinimumVersion == (api.Version{}) {
				return fmt.Errorf("check %s: undefined version found", check.ID)
			}
			if c.MinimumVersion.Latest() {
				return fmt.Errorf("check %s: version cannot be 'latest'", check.ID)
			}
			if maxVersion == c.MinimumVersion {
				return fmt.Errorf("check %s: duplicate version %s", check.ID, c.MinimumVersion)
			}
			if !maxVersion.Older(c.MinimumVersion) {
				return fmt.Errorf("check %s: versions must be strictly increasing", check.ID)
			}
			maxVersion = c.MinimumVersion
		}
	}
	// 第二遍校验覆盖关系；此时全部 ID 已知，才能判断目标是否存在及等级是否合法。
	for _, check := range checks {
		for _, c := range check.Versions {
			if len(c.OverrideCheckIDs) == 0 {
				continue
			}

			if check.Level != api.LevelRestricted {
				return fmt.Errorf("check %s: only restricted checks may set overrides", check.ID)
			}
			for _, override := range c.OverrideCheckIDs {
				if overriddenLevel, ok := ids[override]; ok && overriddenLevel != api.LevelBaseline {
					return fmt.Errorf("check %s: overrides %s check %s", check.ID, overriddenLevel, override)
				}
			}
		}
	}
	return nil
}

// populate 把稀疏的“起始版本 -> 检查实现”定义展开成每个版本的完整检查表。
func populate(r *checkRegistry, validChecks []Check) {
	// 找出所有检查的最大 MinimumVersion，将全部检查展开到同一上限。
	for _, c := range validChecks {
		lastVersion := c.Versions[len(c.Versions)-1].MinimumVersion
		if r.maxVersion.Older(lastVersion) {
			r.maxVersion = lastVersion
		}
	}

	var (
		restrictedVersionedChecks = map[api.Version]map[CheckID]VersionedCheck{}
		baselineVersionedChecks   = map[api.Version]map[CheckID]VersionedCheck{}

		baselineIDs, restrictedIDs []CheckID
	)
	for _, c := range validChecks {
		if c.Level == api.LevelRestricted {
			restrictedIDs = append(restrictedIDs, c.ID)
			inflateVersions(c, restrictedVersionedChecks, r.maxVersion)
		} else {
			baselineIDs = append(baselineIDs, c.ID)
			inflateVersions(c, baselineVersionedChecks, r.maxVersion)
		}
	}

	// 对 ID 排序，避免 map 遍历顺序导致结果与错误信息不稳定。
	slices.Sort(restrictedIDs)
	slices.Sort(baselineIDs)
	orderedIDs := append(baselineIDs, restrictedIDs...) // 先放 Baseline 检查，再放 Restricted 检查。

	for v := api.MajorMinorVersion(1, 0); v.Older(nextMinor(r.maxVersion)); v = nextMinor(v) {
		// 汇总被覆盖的 Baseline 检查 ID，剔除以免同一条规则重复报告。
		overrides := map[CheckID]bool{}
		for _, c := range restrictedVersionedChecks[v] {
			for _, override := range c.OverrideCheckIDs {
				overrides[override] = true
			}
		}
		// 将过滤后的 Baseline 检查加入 Restricted，保持其基线超集语义。
		for id, c := range baselineVersionedChecks[v] {
			if overrides[id] {
				continue // 已被覆盖的检查跳过。
			}
			if restrictedVersionedChecks[v] == nil {
				restrictedVersionedChecks[v] = map[CheckID]VersionedCheck{}
			}
			restrictedVersionedChecks[v][id] = c
		}

		r.restrictedChecks[v] = mapCheckPodFns(restrictedVersionedChecks[v], orderedIDs)
		r.baselineChecks[v] = mapCheckPodFns(baselineVersionedChecks[v], orderedIDs)
	}
}

// inflateVersions 把一条检查的多个版本区间填进按版本索引的表里。
func inflateVersions(check Check, versions map[api.Version]map[CheckID]VersionedCheck, maxVersion api.Version) {
	for i, c := range check.Versions {
		var nextVersion api.Version
		if i+1 < len(check.Versions) {
			nextVersion = check.Versions[i+1].MinimumVersion
		} else {
			// 假设只处理一个主版本；最后一个区间延伸到上限。
			nextVersion = nextMinor(maxVersion)
		}
		// 遍历当前检查最小版本至下一检查最小版本（不含）的区间；无下一检查时遍历到 maxVersion（含）。
		// 右开区间确保新旧实现不同时命中。
		for v := c.MinimumVersion; v.Older(nextVersion); v = nextMinor(v) {
			if versions[v] == nil {
				versions[v] = map[CheckID]VersionedCheck{}
			}
			versions[v][check.ID] = check.Versions[i]
		}
	}
}

// mapCheckPodFns 按 orderedIDs 将检查表转换为有序 CheckPodFn 切片，保证结果顺序稳定；
// 所有检查都必须在 orderedIDs 中。
func mapCheckPodFns(checks map[CheckID]VersionedCheck, orderedIDs []CheckID) []CheckPodFn {
	fns := make([]CheckPodFn, 0, len(checks))
	for _, id := range orderedIDs {
		if check, ok := checks[id]; ok {
			fns = append(fns, check.CheckPod)
		}
	}
	return fns
}

// nextMinor 将次版本号加一；latest 已无上限，不能再递增。
func nextMinor(v api.Version) api.Version {
	if v.Latest() {
		return v
	}
	return api.MajorMinorVersion(v.Major(), v.Minor()+1)
}
