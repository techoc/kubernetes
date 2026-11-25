/*
Copyright 2024 The Kubernetes Authors.

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

// Package mutating 实现 MutatingAdmissionPolicy 准入插件：
// 用 CEL 表达式声明式地修改请求对象，等价于一个可编程的 mutating webhook，
// 但策略以 API 对象形式存放在 apiserver 内，无需外部服务。
package mutating

import (
	"context"
	"io"

	celgo "github.com/google/cel-go/cel"

	v1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/managedfields"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/admission/initializer"
	"k8s.io/apiserver/pkg/admission/plugin/cel"
	"k8s.io/apiserver/pkg/admission/plugin/manifest/metrics"
	"k8s.io/apiserver/pkg/admission/plugin/policy/config"
	"k8s.io/apiserver/pkg/admission/plugin/policy/generic"
	"k8s.io/apiserver/pkg/admission/plugin/policy/manifest/source"
	"k8s.io/apiserver/pkg/admission/plugin/policy/matching"
	"k8s.io/apiserver/pkg/admission/plugin/policy/mutating/patch"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/matchconditions"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/component-base/featuregate"
)

const (
	// PluginName 表示准入插件的名字。
	PluginName = "MutatingAdmissionPolicy"
)

// Register 注册一个准入插件。
//
// 准入插件的注册入口：名字必须与 --enable-admission-plugins 中使用的值一致。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(configFile io.Reader) (admission.Interface, error) {
		return NewPlugin(configFile)
	})
}

// Policy / PolicyBinding 是 MutatingAdmissionPolicy 及其绑定的类型别名，
// 便于在泛型代码中复用同一套 source/dispatcher 实现。
type Policy = v1.MutatingAdmissionPolicy
type PolicyBinding = v1.MutatingAdmissionPolicyBinding

// PolicyHook 把策略、绑定与求值器组合成泛型插件所需的钩子类型。
type PolicyHook = generic.PolicyHook[*Policy, *PolicyBinding, PolicyEvaluator]

// Mutator 预留给未来的可插拔变更实现，当前为空。
type Mutator struct {
}

// MutationEvaluationFunc 描述一次策略求值的输入：匹配到的资源、版本化属性、
// 参数对象、namespace、类型转换器、CEL 成本预算与授权器。
type MutationEvaluationFunc func(
	ctx context.Context,
	matchedResource schema.GroupVersionResource,
	versionedAttr *admission.VersionedAttributes,
	o admission.ObjectInterfaces,
	versionedParams runtime.Object,
	namespace *corev1.Namespace,
	typeConverter managedfields.TypeConverter,
	runtimeCELCostBudget int64,
	authorizer authorizer.UnconditionalAuthorizer,
) (runtime.Object, error)

// PolicyEvaluator 是策略编译后的产物：matchConditions 匹配器、补丁器列表与共享的 CEL 编译器。
// Error 非空表示编译失败，此时该策略应被视为不可用而不是静默生效。
type PolicyEvaluator struct {
	Matcher            matchconditions.Matcher
	Mutators           []patch.Patcher
	CompositedCompiler *cel.CompositedCompiler
	Error              error
}

// Plugin 是 admission.Interface 的一种实现。
//
// 大部分逻辑委托给泛型 Plugin：本类型只负责 MutatingAdmissionPolicy 特有的
// 编译、分发与特性门控。
type Plugin struct {
	*generic.Plugin[PolicyHook]
}

// 编译期断言：插件需同时满足 admission.Interface、MutationInterface，
// 并从 initializer 获取 manifest 加载器。
var _ admission.Interface = &Plugin{}
var _ admission.MutationInterface = &Plugin{}
var _ initializer.WantsManifestLoaders = &Plugin{}

// SetManifestLoaders 注入基于 scheme 的默认化与校验所需的清单加载函数。
//
// 静态 manifest 模式用于在没有 API server 存储的情况下从目录加载策略；
// 这里注入加载函数，使 source 包无需直接依赖 initializer。
func (a *Plugin) SetManifestLoaders(loaders *initializer.ManifestLoaders) {
	// 未启用静态 manifest 时直接返回，插件仍走 informer 数据源。
	if loaders == nil || loaders.LoadMutatingPolicyManifests == nil {
		return
	}
	loadFunc := loaders.LoadMutatingPolicyManifests
	a.SetStaticSourceFactory(func(manifestsDir string) (generic.ReloadableSource[PolicyHook], error) {
		staticSource := source.NewStaticPolicySource(manifestsDir, a.GetAPIServerID(),
			func(p *v1.MutatingAdmissionPolicy) (PolicyEvaluator, error) {
				e := compilePolicy(p)
				if e.Error != nil {
					return PolicyEvaluator{}, e.Error
				}
				return e, nil
			},
			func(dir string) ([]*v1.MutatingAdmissionPolicy, []*v1.MutatingAdmissionPolicyBinding, string, error) {
				return loadFunc(dir)
			},
			// 绑定通过 spec.policyName 关联到策略，而不是 ownerReference。
			func(b *v1.MutatingAdmissionPolicyBinding) string { return b.Spec.PolicyName },
			metrics.MAPManifestType,
		)
		// 首次加载失败必须报错：否则插件会以“无策略”状态静默放行所有请求。
		if err := staticSource.LoadInitial(); err != nil {
			return nil, err
		}
		return staticSource, nil
	})
}

// NewPlugin 返回一个泛型准入插件。
//
// 读取插件配置后，装配数据源（informer 或静态 manifest）与分发器。
func NewPlugin(configFile io.Reader) (*Plugin, error) {
	cfg, err := config.LoadMutatingConfig(configFile)
	if err != nil {
		return nil, err
	}

	// DELETE 没有可供变更的请求体，因此本插件从不处理该操作。
	//
	// CONNECT 也需要处理，因为 exec/portforward 等子资源可能被策略改写或校验。
	handler := admission.NewHandler(admission.Create, admission.Update, admission.Connect)
	res := &Plugin{}
	res.Plugin = generic.NewPlugin(
		handler,
		func(f informers.SharedInformerFactory, client kubernetes.Interface, dynamicClient dynamic.Interface, restMapper meta.RESTMapper) generic.Source[PolicyHook] {
			return generic.NewPolicySource(
				f.Admissionregistration().V1().MutatingAdmissionPolicies().Informer(),
				f.Admissionregistration().V1().MutatingAdmissionPolicyBindings().Informer(),
				NewMutatingAdmissionPolicyAccessor,
				NewMutatingAdmissionPolicyBindingAccessor,
				compilePolicy,
				//!TODO: 提供一种在 mutating/validating 插件之间
				// 共享 param informer 的机制
				f,
				dynamicClient,
				restMapper,
			)
		},
		func(a authorizer.UnconditionalAuthorizer, m *matching.Matcher, client kubernetes.Interface) generic.Dispatcher[PolicyHook] {
			// TypeConverterManager 基于 OpenAPI v3 做结构化合并，nil 表示使用默认实现。
			return NewDispatcher(a, m, patch.NewTypeConverterManager(nil, client.Discovery().OpenAPIV3()))
		},
	)
	res.SetStaticManifestsDir(cfg.StaticManifestsDir)
	return res, nil
}

// Admit 根据请求属性做出准入决策。
func (a *Plugin) Admit(ctx context.Context, attr admission.Attributes, o admission.ObjectInterfaces) error {
	return a.Plugin.Dispatch(ctx, attr, o)
}

// InspectFeatureGates 根据 MutatingAdmissionPolicy 特性门控决定插件是否生效；
// 关闭时插件被禁用，所有请求原样放行。
func (a *Plugin) InspectFeatureGates(featureGates featuregate.FeatureGate) {
	a.Plugin.SetEnabled(featureGates.Enabled(features.MutatingAdmissionPolicy))
}

// Variable 是用于组合复用的具名表达式。
//
// 用于 CEL 组合表达式（composited variables），允许一个表达式引用另一个命名表达式。
type Variable struct {
	Name       string
	Expression string
}

// GetExpression 返回变量对应的 CEL 源文本。
func (v *Variable) GetExpression() string {
	return v.Expression
}

// ReturnTypes 声明变量允许的返回类型：Any 与 Dyn，
// 因为策略作者可能返回任意 JSON 兼容值。
func (v *Variable) ReturnTypes() []*celgo.Type {
	return []*celgo.Type{celgo.AnyType, celgo.DynType}
}

// GetName 返回变量名，供 CEL 环境注册与引用。
func (v *Variable) GetName() string {
	return v.Name
}
