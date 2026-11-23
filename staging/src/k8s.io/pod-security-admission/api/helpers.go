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

package api

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/component-base/version"
)

// 策略版本要么是一个具体的 1.x，要么是 latest；latest 单独用布尔位表示，
// 避免把“跟随最新”伪装成一个永远追不上的数字。
type Version struct {
	major  int
	minor  int
	latest bool
}

// String 的返回值会出现在审计事件和错误信息里，latest 用字面量比展开成某个具体版本号更诚实。
func (v Version) String() string {
	if v.latest {
		return "latest"
	}
	return fmt.Sprintf("v%d.%d", v.major, v.minor)
}

// Older returns true if this version v is older than the other.
func (v *Version) Older(other Version) bool {
	if v.latest { // Latest is always consider newer, even than future versions.
		return false
	}
	if other.latest {
		return true
	}
	if v.major != other.major {
		return v.major < other.major
	}
	return v.minor < other.minor
}

// 以下访问器只暴露读取能力：Version 的比较语义由本包集中控制，外部不能拼出半合法的版本值。
func (v *Version) Major() int {
	return v.major
}
func (v *Version) Minor() int {
	return v.minor
}
func (v *Version) Latest() bool {
	return v.latest
}

func MajorMinorVersion(major, minor int) Version {
	return Version{major: major, minor: minor}
}

// GetAPIVersion get the version of apiServer and return the version major and minor
// 解析失败时返回零值而非 latest：宁可退回最保守的 v0.0，也不能把不认识的版本当成“永远最新”。
func GetAPIVersion() Version {
	var err error
	v := Version{}
	apiVersion := version.Get()
	major, err := strconv.Atoi(apiVersion.Major)
	if err != nil {
		return v
	}
	// split the "normal" + and - for semver stuff to get the leading minor number
	minorString := strings.FieldsFunc(apiVersion.Minor, func(r rune) bool {
		return !unicode.IsDigit(r)
	})[0]
	minor, err := strconv.Atoi(minorString)
	if err != nil {
		return v
	}
	v = MajorMinorVersion(major, minor)
	return v
}

// latest 跟随的是策略实现所能提供的最新规则，而非调用方构建时写死的版本号。
func LatestVersion() Version {
	return Version{latest: true}
}

// ParseLevel returns the level that should be evaluated.
// level must be "privileged", "baseline", or "restricted".
// if level does not match one of those strings, "restricted" and an error is returned.
// 返回值始终是可用级别：出错时收紧到 restricted，避免解析失败反而放宽准入。
func ParseLevel(level string) (Level, error) {
	switch Level(level) {
	case LevelPrivileged, LevelBaseline, LevelRestricted:
		return Level(level), nil
	default:
		return LevelRestricted, fmt.Errorf(`must be one of %s`, strings.Join(validLevels, ", "))
	}
}

// Valid checks whether the level l is a valid level.
// 未识别的级别必须视为无效，这样上游校验不会悄悄把它当成某个已知级别处理。
func (l *Level) Valid() bool {
	switch *l {
	case LevelPrivileged, LevelBaseline, LevelRestricted:
		return true
	default:
		return false
	}
}

// 限制为 1.x 且 x 无前导零：同一版本只能有一种写法，缓存键 comparisons 才不会把 v1.01 当成两个版本。
var versionRegexp = regexp.MustCompile(`^v1\.([0-9]|[1-9][0-9]*)$`)

// ParseVersion returns the policy version that should be evaluated.
// version must be "latest" or "v1.x".
// If version does not match one of those patterns, the latest version and an error is returned.
// 这里出错回退到 latest 而非零值：策略解析失败时按当前规则执行，行为可预测且不会误判所有检查都不适用。
func ParseVersion(version string) (Version, error) {
	if version == "latest" {
		return Version{latest: true}, nil
	}
	match := versionRegexp.FindStringSubmatch(version)
	if len(match) != 2 {
		return Version{latest: true}, fmt.Errorf(`must be "latest" or "v1.x"`)
	}
	versionNumber, err := strconv.Atoi(match[1])
	if err != nil || versionNumber < 0 {
		return Version{latest: true}, fmt.Errorf(`must be "latest" or "v1.x"`)
	}
	return Version{major: 1, minor: versionNumber}, nil
}

// 匿名嵌入让 LevelVersion 可直接当 Level/Version 使用，同时保证比较时两者必须同时相等。
type LevelVersion struct {
	Level
	Version
}

func (lv LevelVersion) String() string {
	return fmt.Sprintf("%s:%s", lv.Level, lv.Version)
}

// Equivalent determines whether two LevelVersions are functionally equivalent. LevelVersions are
// considered equivalent if both are privileged, or both levels & versions are equal.
// privileged 模式不执行任何检查，因此版本差异没有意义，无需比较。
func (lv *LevelVersion) Equivalent(other *LevelVersion) bool {
	return (lv.Level == LevelPrivileged && other.Level == LevelPrivileged) ||
		(lv.Level == other.Level && lv.Version == other.Version)
}

type Policy struct {
	Enforce LevelVersion
	Audit   LevelVersion
	Warn    LevelVersion
}

func (p *Policy) String() string {
	return fmt.Sprintf("enforce=%#v, audit=%#v, warn=%#v", p.Enforce, p.Audit, p.Warn)
}

// Equivalent determines whether two policies are functionally equivalent. Policies are considered
// equivalent if all 3 modes are considered equivalent.
// 三种模式逐一看待，避免只比 enforce 就断言策略相同，而忽略 audit/warn 的配置漂移。
func (p *Policy) Equivalent(other *Policy) bool {
	return p.Enforce.Equivalent(&other.Enforce) && p.Audit.Equivalent(&other.Audit) && p.Warn.Equivalent(&other.Warn)
}

// FullyPrivileged returns true if all 3 policy modes are privileged.
func (p *Policy) FullyPrivileged() bool {
	return p.Enforce.Level == LevelPrivileged &&
		p.Audit.Level == LevelPrivileged &&
		p.Warn.Level == LevelPrivileged
}

// PolicyToEvaluate resolves the PodSecurity namespace labels to the policy for that namespace,
// falling back to the provided defaults when a label is unspecified. A valid policy is always
// returned, even when an error is returned. If labels cannot be parsed correctly, the values of
// "restricted" and "latest" are used for level and version respectively.
// audit/warn 的非法级别分别回退到 privileged，与 enforce 的收紧回退不同：它们不阻塞准入，
// 按宽松处理可以避免用错误配置产生大量误导性的审计告警。
func PolicyToEvaluate(labels map[string]string, defaults Policy) (Policy, field.ErrorList) {
	var (
		err  error
		errs field.ErrorList

		p = defaults

		hasEnforceLevel              bool
		hasWarnLevel, hasWarnVersion bool
	)
	if len(labels) == 0 {
		return p, nil
	}
	if level, ok := labels[EnforceLevelLabel]; ok {
		p.Enforce.Level, err = ParseLevel(level)
		hasEnforceLevel = (err == nil) // Don't default warn in case of error
		errs = appendErr(errs, err, EnforceLevelLabel, level)
	}
	if version, ok := labels[EnforceVersionLabel]; ok {
		p.Enforce.Version, err = ParseVersion(version)
		errs = appendErr(errs, err, EnforceVersionLabel, version)
	}
	if level, ok := labels[AuditLevelLabel]; ok {
		p.Audit.Level, err = ParseLevel(level)
		errs = appendErr(errs, err, AuditLevelLabel, level)
		if err != nil {
			p.Audit.Level = LevelPrivileged // Fail open for audit.
		}
	}
	if version, ok := labels[AuditVersionLabel]; ok {
		p.Audit.Version, err = ParseVersion(version)
		errs = appendErr(errs, err, AuditVersionLabel, version)
	}
	if level, ok := labels[WarnLevelLabel]; ok {
		hasWarnLevel = true
		p.Warn.Level, err = ParseLevel(level)
		errs = appendErr(errs, err, WarnLevelLabel, level)
		if err != nil {
			p.Warn.Level = LevelPrivileged // Fail open for warn.
		}
	}
	if version, ok := labels[WarnVersionLabel]; ok {
		hasWarnVersion = true
		p.Warn.Version, err = ParseVersion(version)
		errs = appendErr(errs, err, WarnVersionLabel, version)
	}

	// Default warn to the enforce level when explicitly set to a more restrictive level.
	if !hasWarnLevel && hasEnforceLevel && CompareLevels(p.Enforce.Level, p.Warn.Level) > 0 {
		p.Warn.Level = p.Enforce.Level
		if !hasWarnVersion {
			p.Warn.Version = p.Enforce.Version
		}
	}

	return p, errs
}

// CompareLevels returns an integer comparing two levels by strictness. The result will be 0 if
// a==b, -1 if a is less strict than b, and +1 if a is more strict than b.
func CompareLevels(a, b Level) int {
	if a == b {
		return 0
	}
	switch a {
	case LevelPrivileged:
		return -1
	case LevelRestricted:
		return 1
	default:
		if b == LevelPrivileged {
			return 1
		} else if b == LevelRestricted {
			return -1
		}
	}
	// This should only happen if both a & b are invalid levels.
	return 0
}

var labelsPath = field.NewPath("metadata", "labels")

// appendErr is a helper function to collect label-specific errors.
func appendErr(errs field.ErrorList, err error, label, value string) field.ErrorList {
	if err != nil {
		return append(errs, field.Invalid(labelsPath.Key(label), value, err.Error()))
	}
	return errs
}
