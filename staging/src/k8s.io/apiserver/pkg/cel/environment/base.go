/*
Copyright 2023 The Kubernetes Authors.

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

package environment

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker"
	"github.com/google/cel-go/ext"
	"github.com/google/cel-go/interpreter"
	"golang.org/x/sync/singleflight"

	"k8s.io/apimachinery/pkg/util/version"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	"k8s.io/apiserver/pkg/cel/library"
	"k8s.io/apiserver/pkg/util/compatibility"
	basecompatibility "k8s.io/component-base/compatibility"
)

// DefaultCompatibilityVersion 返回供 EnvSet 使用的默认兼容版本，
// 保证与 apiserver 最低兼容版本能理解的 CEL 特性、库与参数兼容。
//
// 默认值不会高于当前 Kubernetes major.minor 版本。
//
// 默认值若低于当前 major.minor 的前一个小版本，表示兼容范围比回滚所需更宽。
//
// 宽兼容范围让 CEL 表达式可跨更多 Kubernetes 版本使用，因此是可取的。
//
// 默认值等于当前 major.minor 版本，表示可以快进使用新 CEL 特性——
// 前提是已不再需要回滚。
func DefaultCompatibilityVersion() *version.Version {
	effectiveVer := compatibility.DefaultComponentGlobalsRegistry.EffectiveVersionFor(basecompatibility.DefaultKubeComponent)
	// 未初始化时退回构建时版本，避免在测试或独立组件中 panic。
	if effectiveVer == nil {
		effectiveVer = compatibility.DefaultBuildEffectiveVersion()
	}
	return effectiveVer.MinCompatibilityVersion()
}

// baseOpts 是所有兼容版本共享的 CEL 环境选项列表。
//
// 每一项都带有 IntroducedVersion/RemovedVersion，EnvSet 在构造时根据目标兼容
// 版本筛选出生效的选项。顺序有意义：选项按声明顺序叠加，同一库的新版本会
// 覆盖旧版本。
var baseOpts = []VersionedOptions{
	{
		// CEL 纪元实际是 1.23，但这里人为设为 1.0，因为这些选项始终应存在。
		//
		IntroducedVersion: version.MajorMinor(1, 0),
		EnvOptions: []cel.EnvOption{
			cel.HomogeneousAggregateLiterals(),
			// 在基础环境初始化时一次性校验函数声明，
			// 避免每次编译 CEL 规则时重复校验。
			// 校验开销相对较大。
			cel.EagerlyValidateDeclarations(true),
			cel.DefaultUTCTimeZone(true),

			UnversionedLib(library.URLs),
			UnversionedLib(library.Regex),

			// cel-go v0.17.7 把 has() 开销从 0 改为 1，同时提供 CostEstimatorOptions 保留旧行为；升级版本时一并启用。
			// 这是回归修复，对所有使用 v0.17.7 的代码统一生效。
			cel.CostEstimatorOptions(checker.PresenceTestHasCost(false)),
		},
		ProgramOptions: []cel.ProgramOption{
			cel.EvalOptions(cel.OptOptimize, cel.OptTrackCost),
			cel.CostLimit(celconfig.PerCallLimit),

			// cel-go v0.17.7 把 has() 开销从 0 改为 1，同时提供 CostEstimatorOptions 保留旧行为；升级版本时一并启用。
			// 这是回归修复，对所有使用 v0.17.7 的代码统一生效。
			cel.CostTrackerOptions(interpreter.PresenceTestHasCost(false)),
		},
	},
	{
		// 旧版 lists 库在 1.37 移除，由下面新版本接替；保留 RemovedVersion 是为了
		// 让旧兼容版本仍能编译历史表达式。
		IntroducedVersion: version.MajorMinor(1, 0),
		RemovedVersion:    version.MajorMinor(1, 37),
		EnvOptions: []cel.EnvOption{
			library.Lists(library.ListsVersion(0)),
		},
	},
	{
		IntroducedVersion: version.MajorMinor(1, 27),
		EnvOptions: []cel.EnvOption{
			UnversionedLib(library.Authz),
		},
	},
	{
		IntroducedVersion: version.MajorMinor(1, 28),
		EnvOptions: []cel.EnvOption{
			cel.CrossTypeNumericComparisons(true),
			cel.OptionalTypes(),
			UnversionedLib(library.Quantity),
		},
	},
	// 在 1.29 中添加新的校验器
	{
		IntroducedVersion: version.MajorMinor(1, 29),
		EnvOptions: []cel.EnvOption{
			cel.ASTValidators(
				cel.ValidateDurationLiterals(),
				cel.ValidateTimestampLiterals(),
				cel.ValidateRegexLiterals(),
				cel.ValidateHomogeneousAggregateLiterals(),
			),
		},
	},
	// String 库
	{
		IntroducedVersion: version.MajorMinor(1, 0),
		RemovedVersion:    version.MajorMinor(1, 29),
		EnvOptions: []cel.EnvOption{
			ext.Strings(ext.StringsVersion(0)),
		},
	},
	{
		IntroducedVersion: version.MajorMinor(1, 29),
		EnvOptions: []cel.EnvOption{
			ext.Strings(ext.StringsVersion(2)),
		},
	},
	// Set 库
	{
		IntroducedVersion: version.MajorMinor(1, 29),
		EnvOptions: []cel.EnvOption{
			ext.Sets(),
		},
	},
	{
		IntroducedVersion: version.MajorMinor(1, 30),
		EnvOptions: []cel.EnvOption{
			UnversionedLib(library.IP),
			UnversionedLib(library.CIDR),
		},
	},
	// Format 库
	{
		IntroducedVersion: version.MajorMinor(1, 31),
		EnvOptions: []cel.EnvOption{
			UnversionedLib(library.Format),
		},
	},
	// Authz selectors
	{
		IntroducedVersion: version.MajorMinor(1, 31),
		EnvOptions: []cel.EnvOption{
			UnversionedLib(library.AuthzSelectors),
		},
	},
	// 双变量推导式
	{
		IntroducedVersion: version.MajorMinor(1, 32),
		EnvOptions: []cel.EnvOption{
			ext.TwoVarComprehensions(),
		},
	},
	// Semver
	{
		IntroducedVersion: version.MajorMinor(1, 33),
		EnvOptions: []cel.EnvOption{
			library.SemverLib(library.SemverVersion(1)),
		},
	},
	// List 库
	{
		IntroducedVersion: version.MajorMinor(1, 34),
		EnvOptions: []cel.EnvOption{
			ext.Lists(ext.ListsVersion(3)),
		},
	},
	{
		IntroducedVersion: version.MajorMinor(1, 37),
		EnvOptions: []cel.EnvOption{
			library.Lists(library.ListsVersion(1)),
		},
	},
	StrictCostOpt,
}

// StrictCostOpt 为 Kubernetes 扩展库注册严格的成本估算器，防止 CEL 表达式通过
// 扩展函数绕过内置的成本上限。
var StrictCostOpt = VersionedOptions{
	// 用于配置扩展库的开销计算
	IntroducedVersion: version.MajorMinor(1, 0),
	ProgramOptions: []cel.ProgramOption{
		cel.CostTracking(&library.CostEstimator{}),
	},
}

// cacheBaseEnvs 控制是否缓存 MustBaseEnvSet 的调用结果。
// 默认开启；测试可调用 DisableBaseEnvSetCachingForTests 关闭。
var cacheBaseEnvs = true

// DisableBaseEnvSetCachingForTests 清空并禁用基础环境缓存。
// 仅供单元测试使用：以不同启用选项直接调用 MustBaseEnvSet。
// 它不会清理其他可能缓存 MustBaseEnvSet 结果的初始化路径。
func DisableBaseEnvSetCachingForTests() {
	cacheBaseEnvs = false
	baseEnvs.Clear()
}

// MustBaseEnvSet 返回指定 Version 的 Kubernetes 通用 CEL 基础环境；
// Version 为 nil 或缺少 major/minor 时 panic。
//
// 返回的环境包含函数库、语言设置、优化项，以及适配 Kubernetes 中 CEL 使用方式的
// 运行时开销上限。
//
// 返回的环境不含 CEL 变量定义与自定义类型声明，需要进一步扩展，
// 才能构造出带相应变量、类型声明及其他配置的环境。
func MustBaseEnvSet(ver *version.Version) *EnvSet {
	if ver == nil {
		panic("version must be non-nil")
	}
	if len(ver.Components()) < 2 {
		panic(fmt.Sprintf("version must contain an major and minor component, but got: %s", ver.String()))
	}
	// 缓存键只取 major.minor：同一兼容版本下 patch 号不影响环境内容。
	key := strconv.FormatUint(uint64(ver.Major()), 10) + "." + strconv.FormatUint(uint64(ver.Minor()), 10)
	var entry interface{}
	if entry, ok := baseEnvs.Load(key); ok {
		return entry.(*EnvSet)
	}
	// singleflight 保证并发首次构造时只构建一次，避免重复的昂贵初始化。
	entry, _, _ = baseEnvsSingleflight.Do(key, func() (interface{}, error) {
		entry := mustNewEnvSet(ver, baseOpts)
		if cacheBaseEnvs {
			baseEnvs.Store(key, entry)
		}
		return entry, nil
	})

	return entry.(*EnvSet)
}

var (
	baseEnvs             = sync.Map{}
	baseEnvsSingleflight = &singleflight.Group{}
)

// UnversionedLib 包装 ext.Sets()、library.IP() 等库初始化调用，
// 若将来这些调用加入可变参数选项，就强制编译失败。
//
// 这能自动发现代码审查中难以察觉的问题：
// 若 Kubernetes 使用的某个 CEL 库原本无版本，后来引入版本，而我们
// 未显式指定版本，库会默认使用最新版，从而绕过可控发布流程改变 CEL 环境，
//
// 违背基础环境设计的初衷。
//
// 若此函数的使用处编译失败：给所有失败调用点加上 version=1 参数，
//
// 并去掉 UnversionedLib 包装。然后审查高版本库中的变化，必要时通过 VersionedOptions
// 在基础环境中安全 roll out 到新版本。
func UnversionedLib(initializer func() cel.EnvOption) cel.EnvOption {
	return initializer()
}
