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

// Level 表示 Pod 安全标准的三个级别。
// 用字符串别名而非枚举常量，是为了直接对应命名空间标签上的取值，便于序列化与校验。
type Level string

// 三个级别按严格程度递增。
const (
	// LevelPrivileged 不施加 Pod 安全限制。
	LevelPrivileged Level = "privileged"
	// LevelBaseline 阻挡已知的提权路径。
	LevelBaseline Level = "baseline"
	// LevelRestricted 在 baseline 之上进一步收紧容器配置。
	LevelRestricted Level = "restricted"
)

// validLevels 保留字符串形式，供配置校验时对照标签值使用；
// 与上面的常量分开维护，避免把字符串切片暴露为可变的公开契约。
var validLevels = []string{
	string(LevelPrivileged),
	string(LevelBaseline),
	string(LevelRestricted),
}

// VersionLatest 表示使用当前策略的最新版本，不锁定到某个具体小版本。
// 选择它会随集群升级自动获得最新限制，适合希望策略持续跟进的用户。
const VersionLatest = "latest"

// AuditAnnotationPrefix 复用 labelPrefix 的值：审计注解与命名空间标签共用同一命名空间前缀，
// 便于从审计记录直接对应回触发策略的标签配置。
const AuditAnnotationPrefix = labelPrefix

// 所有命名空间标签统一收敛到同一前缀，避免各处硬编码字符串时拼错。
// 三个动作（enforce/audit/warn）各配一个级别与一个版本标签，成对出现。
const (
	labelPrefix = "pod-security.kubernetes.io/"

	// EnforceLevelLabel 指定阻断准入时采用的 Pod 安全级别。
	EnforceLevelLabel = labelPrefix + "enforce"
	// EnforceVersionLabel 指定 enforce 模式使用的策略版本。
	EnforceVersionLabel = labelPrefix + "enforce-version"
	// AuditLevelLabel 指定只记录审计违规、不阻断准入的 Pod 安全级别。
	AuditLevelLabel = labelPrefix + "audit"
	// AuditVersionLabel 指定 audit 模式使用的策略版本。
	AuditVersionLabel = labelPrefix + "audit-version"
	// WarnLevelLabel 指定向请求方返回警告、不阻断准入的 Pod 安全级别。
	WarnLevelLabel = labelPrefix + "warn"
	// WarnVersionLabel 指定 warn 模式使用的策略版本。
	WarnVersionLabel = labelPrefix + "warn-version"

	// 审计注解只写进审计记录，不回写对象本身，避免改变 Pod 或命名空间的配置语义。
	// ExemptionReasonAnnotationKey 记录请求获得豁免的原因。
	ExemptionReasonAnnotationKey = "exempt"
	// AuditViolationsAnnotationKey 记录审计模式发现的违规详情。
	AuditViolationsAnnotationKey = "audit-violations"
	// EnforcedPolicyAnnotationKey 记录实际生效的 enforce 策略，便于审计追溯。
	EnforcedPolicyAnnotationKey = "enforce-policy"
)
