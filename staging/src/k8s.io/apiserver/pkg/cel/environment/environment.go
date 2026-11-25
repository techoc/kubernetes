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

// Package environment 管理 Kubernetes 中 CEL 环境的构建与版本演进。
// 每个 EnvSet 同时维护“写入时校验用”的 NewExpressions 环境和“读取历史
// 表达式用”的 StoredExpressions 环境，二者的差异是滚动升级兼容性的关键。
package environment

import (
	"fmt"
	"math"

	"github.com/google/cel-go/cel"

	"k8s.io/apimachinery/pkg/util/version"
	apiservercel "k8s.io/apiserver/pkg/cel"
)

// Type 定义 Kubernetes 中使用的不同 CEL 环境类型。
// CEL 环境用于编译和求值 CEL 表达式。
// 环境包含：
//   - 函数库
//   - 变量
//   - 类型（CEL 核心类型与 Kubernetes 类型）
//   - 其他 CEL 环境与程序选项
type Type string

const (
	// NewExpressions 用于校验在写入表达式的请求中新增或修改的表达式。
	//
	// 该环境类型与特定的 Kubernetes 主/次版本兼容。为了保证安全回滚，
	// 该环境类型可能不包含 StoredExpressions 环境类型中可用的全部函数库、
	// 变量、类型声明和 CEL 语言设置。
	//
	// 所有新增或修改的 CEL 表达式在写入存储之前，必须使用 NewExpressions
	// 进行校验（解析、编译、类型检查）。
	NewExpressions Type = "NewExpressions"

	// StoredExpressions 用于编译和运行已持久化到存储的 CEL 表达式。
	//
	// 该环境类型与所有已知 Kubernetes 版本持久化到存储的 CEL 表达式兼容，
	// 是可用环境中最为宽松的一个。
	//
	// StoredExpressions 适合用于配置文件中的 CEL 表达式。
	StoredExpressions Type = "StoredExpressions"
)

// EnvSet 管理 CEL 环境的创建与扩展。每个 EnvSet 同时包含一个
// NewExpressions 和一个 StoredExpressions 环境。EnvSet 的创建和扩展
// 通过 VersionedOptions 完成，使 EnvSet 能够按照各选项在哪个版本引入来准备环境。
//
// 每个 EnvSet 在创建时会被给定一个兼容版本（compatibility version），
// 并据此准备与该版本兼容的 NewExpressions 环境；同时 EnvSet 还会准备
// 与所有已知 Kubernetes 版本兼容的 StoredExpressions 环境。
type EnvSet struct {
	// compatibilityVersion 是 NewExpressions 环境中所有配置都必须兼容的版本。
	compatibilityVersion *version.Version

	// newExpressions 是只包含本 EnvSet 中在该 compatibilityVersion 上已启用的
	// 配置的环境。
	newExpressions *cel.Env

	// storedExpressions 是包含本 EnvSet 中最新配置的环境。
	storedExpressions *cel.Env
}

// newEnvSet 以空 CEL 环境为基底，再叠加版本化选项。两个环境从同一基底
// 分叉，确保差异只来自 VersionedOptions 的版本过滤。
func newEnvSet(compatibilityVersion *version.Version, opts []VersionedOptions) (*EnvSet, error) {
	base, err := cel.NewEnv()
	if err != nil {
		return nil, err
	}
	baseSet := EnvSet{compatibilityVersion: compatibilityVersion, newExpressions: base, storedExpressions: base}
	return baseSet.Extend(opts...)
}

// mustNewEnvSet 用于包内静态初始化；配置错误属于编程错误，直接 panic
// 而不是让 apiserver 带着残缺的 CEL 环境启动。
func mustNewEnvSet(ver *version.Version, opts []VersionedOptions) *EnvSet {
	envSet, err := newEnvSet(ver, opts)
	if err != nil {
		panic(fmt.Sprintf("Default environment misconfigured: %v", err))
	}
	return envSet
}

// NewExpressionsEnv 返回本 EnvSet 的 NewExpressions 环境（Type）。
// 详见 NewExpressions 的说明。
func (e *EnvSet) NewExpressionsEnv() *cel.Env {
	return e.newExpressions
}

// StoredExpressionsEnv 返回本 EnvSet 的 StoredExpressions 环境（Type）。
// 详见 StoredExpressions 的说明。
func (e *EnvSet) StoredExpressionsEnv() *cel.Env {
	return e.storedExpressions
}

// Env 返回给定 Type 对应的 CEL 环境。
func (e *EnvSet) Env(envType Type) (*cel.Env, error) {
	switch envType {
	case NewExpressions:
		return e.newExpressions, nil
	case StoredExpressions:
		return e.storedExpressions, nil
	default:
		return nil, fmt.Errorf("unsupported environment type: %v", envType)
	}
}

// VersionedOptions 提供一组 CEL 配置选项，以及选项引入的版本，并可选地
// 提供选项被移除的版本。
type VersionedOptions struct {
	// IntroducedVersion 是这些选项被引入的版本。
	// NewExpressions 环境只包含在 EnvSet 的兼容版本当时或之前引入的选项。
	//
	// 例如，要配置一个把 "object" 变量绑定到某个资源 Kind 的 CEL 环境，
	// 先由资源的 groupVersionKind 创建 DeclType，再把变量和类型填入 VersionedOptions：
	//
	//    schema := schemaResolver.ResolveSchema(groupVersionKind)
	//    objectType := apiservercel.SchemaDeclType(schema, true)
	//    ...
	//    VersionOptions{
	//      IntroducedVersion: version.MajorMinor(1, 26),
	//      DeclTypes: []*apiservercel.DeclType{ objectType },
	//      EnvOptions: []cel.EnvOption{ cel.Variable("object", objectType.CelType()) },
	//    },
	//
	// 要从 CRD 创建 DeclType，请使用 structural schema，例如：
	//
	//    schema := structuralschema.NewStructural(crdJSONProps)
	//    objectType := apiservercel.SchemaDeclType(schema, true)
	//
	// 必填。
	IntroducedVersion *version.Version
	// RemovedVersion 是这些选项被移除的版本。
	// NewExpressions 环境不会包含在 EnvSet 的兼容版本当时或之前被移除的选项。
	//
	// 所有选项移除都必须保持向后兼容；移除要么与同版本引入的兼容替代项配对，
	// 要么本身不破坏兼容性。
	// StoredExpressions 环境不会包含已被移除的选项。
	//
	// 升级函数库的方式：把旧库的 RemovedVersion 设为与新库的 IntroducedVersion
	// 相同的值。新库必须与旧库向后兼容。
	//
	// 例如：
	//
	//    VersionOptions{
	//      IntroducedVersion: version.MajorMinor(1, 26), RemovedVersion: version.MajorMinor(1, 27),
	//      EnvOptions: []cel.EnvOption{ libraries.Example(libraries.ExampleVersion(1)) },
	//    },
	//    VersionOptions{
	//      IntroducedVersion: version.MajorMinor(1, 27),
	//      EnvOptions: []EnvOptions{ libraries.Example(libraries.ExampleVersion(2)) },
	//    },
	//
	// 可选。
	RemovedVersion *version.Version
	// FeatureEnabled 在这些选项因特性门控而启用时返回 true，
	// 因特性门控未启用而关闭时返回 false。
	//
	// 对 NewExpressions 环境而言，它的优先级高于 IntroducedVersion / RemovedVersion。
	//
	// StoredExpressions 环境会忽略该函数。
	//
	// 可选。
	FeatureEnabled func() bool
	// EnvOptions 提供 CEL EnvOptions，可用于添加 cel.Variable、cel.Library，
	// 或启用其他 CEL EnvOptions（如语言设置）。
	//
	// 若添加的 cel.Variable 带有 OpenAPI 类型，该类型必须包含在 DeclTypes 中。
	EnvOptions []cel.EnvOption
	// ProgramOptions 提供 CEL ProgramOptions，可用于设置 cel.CostLimit、启用
	// 优化项，以及设置应对该环境下所有程序生效的其他程序级选项。
	ProgramOptions []cel.ProgramOption
	// DeclTypes 提供要注册到环境的 OpenAPI 类型声明。
	//
	// 若添加到 EnvOptions 的 cel.Variables 引用了某个 OpenAPI 类型，
	// 该类型必须包含在 DeclTypes 中。
	DeclTypes []*apiservercel.DeclType
}

// Extend 返回一个基于当前 EnvSet 并用给定 VersionedOptions 扩展出的新 EnvSet。
// 当前 EnvSet 不会被修改。
// 返回的 EnvSet 与被扩展的 EnvSet 具有相同的兼容版本。
//
// 两个环境分别扩展：NewExpressions 用本 EnvSet 的兼容版本过滤，且尊重
// feature gate；StoredExpressions 用“最大版本”过滤且忽略 feature gate，
// 以保证任何历史表达式都能被编译。
//
// Extend 是开销较大的操作，而且每次 Extend 添加 DeclTypes 都会加深一层
// resolver 链。因此应将 Extend 的调用次数控制在最少。
//
// 一些最佳实践：
//
//   - 处理 API 请求时尽量减少 Extend 调用；尽可能在初始化组件时调用 Extend。
//   - 若 Extend 返回的 EnvSet 可用于编译多个 CEL 程序，则调用一次 Extend
//     并复用返回的 EnvSet。
//   - 优先用一次 Extend 传入完整的 VersionedOptions 列表，而不是多次调用 Extend。
func (e *EnvSet) Extend(options ...VersionedOptions) (*EnvSet, error) {
	if len(options) > 0 {
		newExprOpts, err := e.filterAndBuildOpts(e.newExpressions, e.compatibilityVersion, true, options)
		if err != nil {
			return nil, err
		}
		p, err := e.newExpressions.Extend(newExprOpts)
		if err != nil {
			return nil, err
		}
		storedExprOpt, err := e.filterAndBuildOpts(e.storedExpressions, version.MajorMinor(math.MaxUint, math.MaxUint), false, options)
		if err != nil {
			return nil, err
		}
		s, err := e.storedExpressions.Extend(storedExprOpt)
		if err != nil {
			return nil, err
		}
		return &EnvSet{compatibilityVersion: e.compatibilityVersion, newExpressions: p, storedExpressions: s}, nil
	}
	return e, nil
}

// filterAndBuildOpts 按版本/feature gate 过滤选项，并合并成一个 cel.Lib。
//
// 合并成单个 Library 而非逐个 Extend，是为了减少 resolver 链深度：
// 每次 Extend 引入 DeclType 都会叠加一层 resolver，深链会拖慢编译。
func (e *EnvSet) filterAndBuildOpts(base *cel.Env, compatVer *version.Version, honorFeatureGateEnablement bool, opts []VersionedOptions) (cel.EnvOption, error) {
	var envOpts []cel.EnvOption
	var progOpts []cel.ProgramOption
	var declTypes []*apiservercel.DeclType

	for _, opt := range opts {
		var allowedByFeatureGate, allowedByVersion bool
		if opt.FeatureEnabled != nil && honorFeatureGateEnablement {
			// 由特性门控启用的库必须遵循兼容的默认启用状态。
			// 在其首个发布版本中就启用 alpha 特性，会启用旧版 API server 并不知晓的库。
			allowedByFeatureGate = opt.FeatureEnabled()
			if !allowedByFeatureGate {
				continue
			}
		}
		if compatVer.AtLeast(opt.IntroducedVersion) && (opt.RemovedVersion == nil || compatVer.LessThan(opt.RemovedVersion)) {
			allowedByVersion = true
		}

		// 版本区间与 feature gate 是“或”关系：只要任一维度允许，就纳入
		// 该环境；两者都拒绝时才跳过。
		if allowedByFeatureGate || allowedByVersion {
			envOpts = append(envOpts, opt.EnvOptions...)
			progOpts = append(progOpts, opt.ProgramOptions...)
			declTypes = append(declTypes, opt.DeclTypes...)
		}
	}

	if len(declTypes) > 0 {
		provider := apiservercel.NewDeclTypeProvider(declTypes...)
		// 1.31 起允许把 CEL 关键字当作字段名（如对象里真有名为 "true"
		// 的字段），这是向后兼容的语法放宽，因此按版本门控。
		if compatVer.AtLeast(version.MajorMinor(1, 31)) {
			provider.SetRecognizeKeywordAsFieldName(true)
		}
		providerOpts, err := provider.EnvOptions(base.CELTypeProvider())
		if err != nil {
			return nil, err
		}
		envOpts = append(envOpts, providerOpts...)
	}

	combined := cel.Lib(&envLoader{
		envOpts:  envOpts,
		progOpts: progOpts,
	})
	return combined, nil
}

// envLoader 把 envOptions 与 programOptions 打包成一个 cel.Lib，
// 使一次 Extend 就能同时应用编译期与运行期配置。
type envLoader struct {
	envOpts  []cel.EnvOption
	progOpts []cel.ProgramOption
}

// CompileOptions 返回编译期选项。
func (e *envLoader) CompileOptions() []cel.EnvOption {
	return e.envOpts
}

// ProgramOptions 返回运行期选项（如 cost limit）。
func (e *envLoader) ProgramOptions() []cel.ProgramOption {
	return e.progOpts
}
