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

// Evaluator holds the Checks that are used to validate a policy.
// 把策略执行抽象成接口，准入控制器只依赖它，便于替换实现或用假对象做测试。
type Evaluator interface {
	// EvaluatePod evaluates the pod against the policy for the given level & version.
	// 调用方只需给出级别与版本，拿到的是命中的检查结果列表，由调用方决定警告还是拒绝。
	EvaluatePod(lv api.LevelVersion, podMetadata *metav1.ObjectMeta, podSpec *corev1.PodSpec) []CheckResult
}

// checkRegistry provides a default implementation of an Evaluator.
// 按版本预先展开检查表，评估时直接查表，避免每次准入请求都遍历全部版本规则。
type checkRegistry struct {
	// The checks are a map policy version to a slice of checks registered for that version.
	// 逐个版本缓存检查函数，是因为同一检查在不同版本可能对应不同实现。
	baselineChecks, restrictedChecks map[api.Version][]CheckPodFn
	// maxVersion is the maximum version that is cached, guaranteed to be at least
	// the max MinimumVersion of all registered checks.
	// 比它更新的版本一律回落到该版本，缓存才不会随集群版本无限膨胀。
	maxVersion api.Version
}

// NewEvaluator constructs a new Evaluator instance from the list of checks. If the provided checks are invalid,
// an error is returned. A valid list of checks must meet the following requirements:
// 1. Check.ID is unique in the list
// 2. Check.Level must be either Baseline or Restricted
// 3. Checks must have a non-empty set of versions, sorted in a strictly increasing order
// 4. Check.Versions cannot include 'latest'
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

	// lower the max version if we're emulating an older minor
	// 模拟版本只允许下调上限，调高会让缓存里不存在的版本被查到。
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
		// includes non-overridden baseline checks
		// restricted 是在 baseline 之上叠加更严的规则，未被引用的 baseline 检查仍需执行。
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
	// Second pass to validate overrides.
	// 覆盖关系要等所有 ID 都记录完再校验，否则无法判断被覆盖的检查是否存在及其级别。
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
	// Find the max(MinimumVersion) across all checks.
	// 所有检查都展开到同一上限，查表时才能统一处理未来版本。
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

	// Sort the IDs to maintain consistent error messages.
	// map 遍历顺序不稳定，固定顺序才能给出确定的检查结果与错误信息。
	slices.Sort(restrictedIDs)
	slices.Sort(baselineIDs)
	orderedIDs := append(baselineIDs, restrictedIDs...) // Baseline checks first, then restricted.

	for v := api.MajorMinorVersion(1, 0); v.Older(nextMinor(r.maxVersion)); v = nextMinor(v) {
		// Aggregate all the overridden baseline check ids.
		// 被 restricted 覆盖的 baseline 检查必须剔除，避免同一条规则报两次且以宽松结果为准。
		overrides := map[CheckID]bool{}
		for _, c := range restrictedVersionedChecks[v] {
			for _, override := range c.OverrideCheckIDs {
				overrides[override] = true
			}
		}
		// Add the filtered baseline checks to restricted.
		// 未被覆盖的 baseline 检查保留下来，restricted 才是真正的 baseline 超集。
		for id, c := range baselineVersionedChecks[v] {
			if overrides[id] {
				continue // Overridden check: skip it.
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
			// Assumes only 1 Major version.
			// 最后一个区间延伸到上限，超出上限的版本由 EvaluatePod 回落处理。
			nextVersion = nextMinor(maxVersion)
		}
		// Iterate over all versions from the minimum of the current check, to the minimum of the
		// next check, or the maxVersion++.
		// 区间右开：新版本生效时旧实现立即退出，避免两个版本的实现同时命中。
		for v := c.MinimumVersion; v.Older(nextVersion); v = nextMinor(v) {
			if versions[v] == nil {
				versions[v] = map[CheckID]VersionedCheck{}
			}
			versions[v][check.ID] = check.Versions[i]
		}
	}
}

// mapCheckPodFns converts the versioned check map to an ordered slice of CheckPodFn,
// using the order specified by orderedIDs. All checks must have a corresponding ID in orderedIDs.
// 按固定 ID 顺序生成切片，保证不同请求得到的检查结果顺序一致、便于比对。
func mapCheckPodFns(checks map[CheckID]VersionedCheck, orderedIDs []CheckID) []CheckPodFn {
	fns := make([]CheckPodFn, 0, len(checks))
	for _, id := range orderedIDs {
		if check, ok := checks[id]; ok {
			fns = append(fns, check.CheckPod)
		}
	}
	return fns
}

// nextMinor increments the minor version
// latest 表示无上限，不能再递增，否则会越过边界。
func nextMinor(v api.Version) api.Version {
	if v.Latest() {
		return v
	}
	return api.MajorMinorVersion(v.Major(), v.Minor()+1)
}
