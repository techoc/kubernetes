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

// Older 判断 v 是否比 other 旧。
func (v *Version) Older(other Version) bool {
	if v.latest { // latest 始终视为最新，甚至比未来版本更新。
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

// 以下访问器只暴露读取能力，避免外部依赖 Version 的内部字段布局。

// Major 返回主版本号；latest 没有具体版本号，返回 0。
func (v *Version) Major() int {
	return v.major
}

// Minor 返回次版本号；latest 没有具体版本号，返回 0。
func (v *Version) Minor() int {
	return v.minor
}

// Latest 返回该版本是否表示跟随最新策略。
func (v *Version) Latest() bool {
	return v.latest
}

// MajorMinorVersion 构造具体的策略版本；需要跟随最新规则时使用 LatestVersion。
func MajorMinorVersion(major, minor int) Version {
	return Version{major: major, minor: minor}
}

// GetAPIVersion 返回 apiServer 版本的主、次版本号。
// 数字解析失败时返回零值而非 latest，避免把不认识的版本误当成“永远最新”。
func GetAPIVersion() Version {
	var err error
	v := Version{}
	apiVersion := version.Get()
	major, err := strconv.Atoi(apiVersion.Major)
	if err != nil {
		return v
	}
	// 提取 semver 中 normal 后缀前的数字部分，得到次版本号。
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

// LatestVersion 返回跟随策略实现所能提供的最新规则的版本，而非调用方构建时写死的版本号。
func LatestVersion() Version {
	return Version{latest: true}
}

// ParseLevel 返回待评估的安全级别，只接受 privileged、baseline 或 restricted。
// 输入无效时返回 restricted 和错误，避免解析失败反而放宽准入。
func ParseLevel(level string) (Level, error) {
	switch Level(level) {
	case LevelPrivileged, LevelBaseline, LevelRestricted:
		return Level(level), nil
	default:
		return LevelRestricted, fmt.Errorf(`must be one of %s`, strings.Join(validLevels, ", "))
	}
}

// Valid 判断 Level 是否为已支持的三个安全级别之一。
// 未识别的级别必须视为无效，避免上游校验悄悄把它当成某个已知级别处理。
func (l *Level) Valid() bool {
	switch *l {
	case LevelPrivileged, LevelBaseline, LevelRestricted:
		return true
	default:
		return false
	}
}

// 仅接受 1.x 且次版本号无前导零，确保同一版本只有一种规范写法。
var versionRegexp = regexp.MustCompile(`^v1\.([0-9]|[1-9][0-9]*)$`)

// ParseVersion 返回待评估的策略版本，只接受 latest 或 v1.x。
// 输入无效时返回 latest 和错误，使策略解析失败时仍按当前规则执行。
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

// LevelVersion 组合安全级别与策略版本，匿名嵌入允许分别访问两者。
type LevelVersion struct {
	Level
	Version
}

// String 返回形如 level:version 的摘要，供日志与审计信息使用。
func (lv LevelVersion) String() string {
	return fmt.Sprintf("%s:%s", lv.Level, lv.Version)
}

// Equivalent 判断两个 LevelVersion 的功能是否等价。
// 两者都为 privileged 时等价；否则级别与版本必须同时相等，因为 privileged 不执行检查。
func (lv *LevelVersion) Equivalent(other *LevelVersion) bool {
	return (lv.Level == LevelPrivileged && other.Level == LevelPrivileged) ||
		(lv.Level == other.Level && lv.Version == other.Version)
}

// Policy 保存 enforce、audit、warn 三种模式各自采用的级别与版本。
type Policy struct {
	Enforce LevelVersion
	Audit   LevelVersion
	Warn    LevelVersion
}

// String 返回三种模式的配置摘要，便于日志与错误信息定位策略。
func (p *Policy) String() string {
	return fmt.Sprintf("enforce=%#v, audit=%#v, warn=%#v", p.Enforce, p.Audit, p.Warn)
}

// Equivalent 判断两条策略的功能是否等价。
// 三种模式必须逐一等价，避免只比较 enforce 而忽略 audit 或 warn 的配置漂移。
func (p *Policy) Equivalent(other *Policy) bool {
	return p.Enforce.Equivalent(&other.Enforce) && p.Audit.Equivalent(&other.Audit) && p.Warn.Equivalent(&other.Warn)
}

// FullyPrivileged 判断三种模式是否全部为 privileged；版本号不影响该判断。
func (p *Policy) FullyPrivileged() bool {
	return p.Enforce.Level == LevelPrivileged &&
		p.Audit.Level == LevelPrivileged &&
		p.Warn.Level == LevelPrivileged
}

// PolicyToEvaluate 根据命名空间标签生成待评估策略，未指定的维度沿用 defaults。
// 标签解析出错时仍返回回退策略：非法级别在 enforce 中回退 restricted，在 audit/warn 中回退 privileged；
// 非法版本回退 latest。enforce 显式收紧时，未设置的 warn 级别跟随 enforce；warn 版本也未设置时一并跟随。
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
		hasEnforceLevel = (err == nil) // enforce 级别无效时不触发 warn 的自动跟随。
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
			p.Audit.Level = LevelPrivileged // audit 不阻断准入，非法配置按宽松处理。
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
			p.Warn.Level = LevelPrivileged // warn 不阻断准入，非法配置按宽松处理。
		}
	}
	if version, ok := labels[WarnVersionLabel]; ok {
		hasWarnVersion = true
		p.Warn.Version, err = ParseVersion(version)
		errs = appendErr(errs, err, WarnVersionLabel, version)
	}

	// enforce 显式收紧且 warn 未显式指定时，让 warn 至少跟随 enforce。
	if !hasWarnLevel && hasEnforceLevel && CompareLevels(p.Enforce.Level, p.Warn.Level) > 0 {
		p.Warn.Level = p.Enforce.Level
		if !hasWarnVersion {
			p.Warn.Version = p.Enforce.Version
		}
	}

	return p, errs
}

// CompareLevels 按严格程度比较两个级别：相等返回 0，a 更宽松返回 -1，a 更严格返回 1。
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
	// 仅当两个级别都无效时才会走到这里，此时视为同等严格程度。
	return 0
}

var labelsPath = field.NewPath("metadata", "labels")

// appendErr 收集指定标签的校验错误，供 PolicyToEvaluate 统一返回。
func appendErr(errs field.ErrorList, err error, label, value string) field.ErrorList {
	if err != nil {
		return append(errs, field.Invalid(labelsPath.Key(label), value, err.Error()))
	}
	return errs
}
