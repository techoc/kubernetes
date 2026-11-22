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

// Type defines the different types of CEL environments used in Kubernetes.
// CEL environments are used to compile and evaluate CEL expressions.
// Environments include:
//   - Function libraries
//   - Variables
//   - Types (both core CEL types and Kubernetes types)
//   - Other CEL environment and program options
type Type string

const (
	// NewExpressions is used to validate new or modified expressions in
	// requests that write expressions to API resources.
	//
	// This environment type is compatible with a specific Kubernetes
	// major/minor version. To ensure safe rollback, this environment type
	// may not include all the function libraries, variables, type declarations, and CEL
	// language settings available in the StoredExpressions environment type.
	//
	// NewExpressions must be used to validate (parse, compile, type check)
	// all new or modified CEL expressions before they are written to storage.
	NewExpressions Type = "NewExpressions"

	// StoredExpressions is used to compile and run CEL expressions that have been
	// persisted to storage.
	//
	// This environment type is compatible with CEL expressions that have been
	// persisted to storage by all known versions of Kubernetes. This is the most
	// permissive environment available.
	//
	// StoredExpressions is appropriate for use with CEL expressions in
	// configuration files.
	StoredExpressions Type = "StoredExpressions"
)

// EnvSet manages the creation and extension of CEL environments. Each EnvSet contains
// both an NewExpressions and StoredExpressions environment. EnvSets are created
// and extended using VersionedOptions so that the EnvSet can prepare environments according
// to what options were introduced at which versions.
//
// Each EnvSet is given a compatibility version when it is created, and prepares the
// NewExpressions environment to be compatible with that version. The EnvSet also
// prepares StoredExpressions to be compatible with all known versions of Kubernetes.
type EnvSet struct {
	// compatibilityVersion is the version that all configuration in
	// the NewExpressions environment is compatible with.
	compatibilityVersion *version.Version

	// newExpressions is an environment containing only configuration
	// in this EnvSet that is enabled at this compatibilityVersion.
	newExpressions *cel.Env

	// storedExpressions is an environment containing the latest configuration
	// in this EnvSet.
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

// NewExpressionsEnv returns the NewExpressions environment Type for this EnvSet.
// See NewExpressions for details.
func (e *EnvSet) NewExpressionsEnv() *cel.Env {
	return e.newExpressions
}

// StoredExpressionsEnv returns the StoredExpressions environment Type for this EnvSet.
// See StoredExpressions for details.
func (e *EnvSet) StoredExpressionsEnv() *cel.Env {
	return e.storedExpressions
}

// Env returns the CEL environment for the given Type.
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

// VersionedOptions provides a set of CEL configuration options as well as the version the
// options were introduced and, optionally, the version the options were removed.
type VersionedOptions struct {
	// IntroducedVersion is the version at which these options were introduced.
	// The NewExpressions environment will only include options introduced at or before the
	// compatibility version of the EnvSet.
	//
	// For example, to configure a CEL environment with an "object" variable bound to a
	// resource kind, first create a DeclType from the groupVersionKind of the resource and then
	// populate a VersionedOptions with the variable and the type:
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
	// To create an DeclType from a CRD, use a structural schema. For example:
	//
	//    schema := structuralschema.NewStructural(crdJSONProps)
	//    objectType := apiservercel.SchemaDeclType(schema, true)
	//
	// Required.
	IntroducedVersion *version.Version
	// RemovedVersion is the version at which these options were removed.
	// The NewExpressions environment will not include options removed at or before the
	// compatibility version of the EnvSet.
	//
	// All option removals must be backward compatible; the removal must either be paired
	// with a compatible replacement introduced at the same version, or the removal must be non-breaking.
	// The StoredExpressions environment will not include removed options.
	//
	// A function library may be upgraded by setting the RemovedVersion of the old library
	// to the same value as the IntroducedVersion of the new library. The new library must
	// be backward compatible with the old library.
	//
	// For example:
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
	// Optional.
	RemovedVersion *version.Version
	// FeatureEnabled returns true if these options are enabled by feature gates,
	// and returns false if these options are not enabled due to feature gates.
	//
	// This takes priority over IntroducedVersion / RemovedVersion for the NewExpressions environment.
	//
	// The StoredExpressions environment ignores this function.
	//
	// Optional.
	FeatureEnabled func() bool
	// EnvOptions provides CEL EnvOptions. This may be used to add a cel.Variable, a
	// cel.Library, or to enable other CEL EnvOptions such as language settings.
	//
	// If an added cel.Variable has an OpenAPI type, the type must be included in DeclTypes.
	EnvOptions []cel.EnvOption
	// ProgramOptions provides CEL ProgramOptions. This may be used to set a cel.CostLimit,
	// enable optimizations, and set other program level options that should be enabled
	// for all programs using this environment.
	ProgramOptions []cel.ProgramOption
	// DeclTypes provides OpenAPI type declarations to register with the environment.
	//
	// If cel.Variables added to EnvOptions refer to a OpenAPI type, the type must be included in
	// DeclTypes.
	DeclTypes []*apiservercel.DeclType
}

// Extend returns an EnvSet based on this EnvSet but extended with given VersionedOptions.
// This EnvSet is not mutated.
// The returned EnvSet has the same compatibility version as the EnvSet that was extended.
//
// 两个环境分别扩展：NewExpressions 用本 EnvSet 的兼容版本过滤，且尊重
// feature gate；StoredExpressions 用“最大版本”过滤且忽略 feature gate，
// 以保证任何历史表达式都能被编译。
//
// Extend is an expensive operation and each call to Extend that adds DeclTypes increases
// the depth of a chain of resolvers. For these reasons, calls to Extend should be kept
// to a minimum.
//
// Some best practices:
//
//   - Minimize calls Extend when handling API requests. Where possible, call Extend
//     when initializing components.
//   - If an EnvSets returned by Extend can be used to compile multiple CEL programs,
//     call Extend once and reuse the returned EnvSets.
//   - Prefer a single call to Extend with a full list of VersionedOptions over
//     making multiple calls to Extend.
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
			// Feature-gate-enabled libraries must follow compatible default feature enablement.
			// Enabling alpha features in their first release enables libraries the previous API server is unaware of.
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
