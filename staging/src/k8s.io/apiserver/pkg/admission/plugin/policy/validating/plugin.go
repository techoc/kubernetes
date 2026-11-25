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

package validating

import (
	"context"
	"io"
	"sync"

	v1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/admission/initializer"
	"k8s.io/apiserver/pkg/admission/plugin/cel"
	"k8s.io/apiserver/pkg/admission/plugin/manifest/metrics"
	"k8s.io/apiserver/pkg/admission/plugin/policy/config"
	"k8s.io/apiserver/pkg/admission/plugin/policy/generic"
	"k8s.io/apiserver/pkg/admission/plugin/policy/manifest/source"
	"k8s.io/apiserver/pkg/admission/plugin/policy/matching"
	"k8s.io/apiserver/pkg/admission/plugin/webhook/matchconditions"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/cel/environment"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
)

const (
	// PluginName 表示准入插件的名字。
	//
	// 此名称同时用于 --enable-admission-plugins 与指标标签，不可随意更改。
	PluginName = "ValidatingAdmissionPolicy"
)

var (
	// CEL 环境构造开销较大，且与策略无关，因此进程内只构造一次；
	// sync.Once 保证并发编译策略时不会重复初始化。
	lazyCompositionEnvTemplateWithStrictCostInit sync.Once
	lazyCompositionEnvTemplateWithStrictCost     *environment.EnvSet
)

// getCompositionEnvTemplateWithStrictCost 返回共享的、启用严格成本评估的 CEL 环境模板。
//
// 模板只读，各策略编译时在其基础上派生，避免跨策略共享可变状态。
func getCompositionEnvTemplateWithStrictCost() *environment.EnvSet {
	lazyCompositionEnvTemplateWithStrictCostInit.Do(func() {
		lazyCompositionEnvTemplateWithStrictCost = environment.MustBaseEnvSet(environment.DefaultCompatibilityVersion())
	})
	return lazyCompositionEnvTemplateWithStrictCost
}

// Register 注册一个准入插件。
//
// 注册工厂仅保存构造函数，真正的实例化发生在 apiserver 组装
// 插件链时，此时才读取组件配置文件。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(configFile io.Reader) (admission.Interface, error) {
		return NewPlugin(configFile)
	})
}

// Plugin 是 admission.Interface 的一种实现。
//
// 这些类型别名把 API 类型与 generic 框架的泛型参数绑定在一起，
// 使 Plugin 能以最少的代码复用策略分发、匹配与静态源逻辑。
type Policy = v1.ValidatingAdmissionPolicy
type PolicyBinding = v1.ValidatingAdmissionPolicyBinding
type PolicyEvaluator = Validator
type PolicyHook = generic.PolicyHook[*Policy, *PolicyBinding, PolicyEvaluator]

// Plugin 是 ValidatingAdmissionPolicy 准入插件。
//
// 内嵌泛型插件，只额外实现 SetManifestLoaders 与 Validate，
// 其余生命周期（启动 informer、关闭）由 generic.Plugin 负责。
type Plugin struct {
	*generic.Plugin[PolicyHook]
}

var _ admission.Interface = &Plugin{}
var _ admission.ValidationInterface = &Plugin{}
var _ initializer.WantsExcludedAdmissionResources = &Plugin{}
var _ initializer.WantsManifestLoaders = &Plugin{}

// SetManifestLoaders 注入基于 scheme 的默认化与校验所需的清单加载函数。
//
// 仅在静态清单目录存在时才会被真正调用；此处注册一个工厂，
// generic.Plugin 在得知目录路径后再按需创建静态源，实现配置与加载解耦。
func (a *Plugin) SetManifestLoaders(loaders *initializer.ManifestLoaders) {
	// 未启用静态配置能力时保持默认（仅 informer 源）行为。
	if loaders == nil || loaders.LoadValidatingPolicyManifests == nil {
		return
	}
	loadFunc := loaders.LoadValidatingPolicyManifests
	a.SetStaticSourceFactory(func(manifestsDir string) (generic.ReloadableSource[PolicyHook], error) {
		staticSource := source.NewStaticPolicySource(manifestsDir, a.GetAPIServerID(),
			// 静态策略在加载期即编译，编译失败会让 apiserver 启动失败，
			// 从而在部署阶段暴露错误而不是在首个请求时。
			func(p *v1.ValidatingAdmissionPolicy) (Validator, error) {
				v := compilePolicy(p)
				if err := v.CompileError(); err != nil {
					return nil, err
				}
				return v, nil
			},
			func(dir string) ([]*v1.ValidatingAdmissionPolicy, []*v1.ValidatingAdmissionPolicyBinding, string, error) {
				return loadFunc(dir)
			},
			// 绑定通过 Spec.PolicyName 引用策略，静态源据此建立索引。
			func(b *v1.ValidatingAdmissionPolicyBinding) string { return b.Spec.PolicyName },
			metrics.VAPManifestType,
		)
		if err := staticSource.LoadInitial(); err != nil {
			return nil, err
		}
		return staticSource, nil
	})
}

// NewPlugin 构造插件：加载配置、选择 CEL 版本并装配 informer 源与分发器。
//
// 分发器使用 UnconditionalAuthorizer，因为策略中的 authorizer
// 变量只关心是否放行，不能容忍带条件的延迟决定。
func NewPlugin(configFile io.Reader) (*Plugin, error) {
	cfg, err := config.LoadValidatingConfig(configFile)
	if err != nil {
		return nil, err
	}

	handler := admission.NewHandler(admission.Connect, admission.Create, admission.Delete, admission.Update)

	// Connect 也被纳入，是因为 exec/portforward 等子资源同样需要被策略约束。
	p := &Plugin{
		Plugin: generic.NewPlugin(
			handler,
			// informer 源随 informer 启动而生效，是动态配置（API 创建的策略）的主路径。
			func(f informers.SharedInformerFactory, client kubernetes.Interface, dynamicClient dynamic.Interface, restMapper meta.RESTMapper) generic.Source[PolicyHook] {
				return generic.NewPolicySource(
					f.Admissionregistration().V1().ValidatingAdmissionPolicies().Informer(),
					f.Admissionregistration().V1().ValidatingAdmissionPolicyBindings().Informer(),
					NewValidatingAdmissionPolicyAccessor,
					NewValidatingAdmissionPolicyBindingAccessor,
					compilePolicy,
					f,
					dynamicClient,
					restMapper,
				)
			},
			func(a authorizer.UnconditionalAuthorizer, m *matching.Matcher, client kubernetes.Interface) generic.Dispatcher[PolicyHook] {
				return NewDispatcher(a, generic.NewPolicyMatcher(m))
			},
		),
	}
	// 插件始终启用；是否真正生效取决于是否存在匹配的策略与绑定。
	p.SetEnabled(true)
	p.SetStaticManifestsDir(cfg.StaticManifestsDir)
	return p, nil
}

// Validate 根据请求属性做出准入决策。
//
// 只做转发；分发器内部按匹配结果决定执行哪些策略，
// 无匹配策略时开销极小。
func (a *Plugin) Validate(ctx context.Context, attr admission.Attributes, o admission.ObjectInterfaces) error {
	return a.Plugin.Dispatch(ctx, attr, o)
}

// compilePolicy 把策略规格编译为可执行的 Validator。
//
// 编译错误不会在此返回 error，而是存入 Validator，以便按
// failurePolicy 决定拒绝请求还是放行——配置错误不应无条件阻断整个集群。
func compilePolicy(policy *Policy) Validator {
	hasParam := false
	if policy.Spec.ParamKind != nil {
		hasParam = true
	}
	// 校验与匹配条件可见 authorizer，而 messageExpression 不可见：
	// 后者用于生成面向用户的文案，暴露授权结果可能泄露权限信息。
	optionalVars := cel.OptionalVariableDeclarations{HasParams: hasParam, HasAuthorizer: true}
	expressionOptionalVars := cel.OptionalVariableDeclarations{HasParams: hasParam, HasAuthorizer: false}
	failurePolicy := policy.Spec.FailurePolicy
	var matcher matchconditions.Matcher = nil
	matchConditions := policy.Spec.MatchConditions
	compositionEnvTemplate := getCompositionEnvTemplateWithStrictCost()
	filterCompiler, err := cel.NewCompositedCompiler(compositionEnvTemplate)
	if err != nil {
		return NewValidator(nil, nil, nil, nil, failurePolicy, err)
	}
	// 变量按声明顺序编译并存入编译器，后续表达式可引用它们。
	filterCompiler.CompileAndStoreVariables(convertv1beta1Variables(policy.Spec.Variables), optionalVars, environment.StoredExpressions)

	if len(matchConditions) > 0 {
		matchExpressionAccessors := make([]cel.ExpressionAccessor, len(matchConditions))
		for i := range matchConditions {
			matchExpressionAccessors[i] = (*matchconditions.MatchCondition)(&matchConditions[i])
		}
		// 匹配条件出错时同样遵循 failurePolicy，保持策略整体语义一致。
		matcher = matchconditions.NewMatcher(filterCompiler.CompileCondition(matchExpressionAccessors, optionalVars, environment.StoredExpressions), failurePolicy, "policy", "validate", policy.Name)
	}
	res := NewValidator(
		filterCompiler.CompileCondition(convertv1Validations(policy.Spec.Validations), optionalVars, environment.StoredExpressions),
		matcher,
		filterCompiler.CompileCondition(convertv1AuditAnnotations(policy.Spec.AuditAnnotations), optionalVars, environment.StoredExpressions),
		filterCompiler.CompileCondition(convertv1MessageExpressions(policy.Spec.Validations), expressionOptionalVars, environment.StoredExpressions),
		failurePolicy,
		nil,
	)

	return res
}

// convertv1Validations 把 API 校验规则适配为 CEL 表达式访问器。
func convertv1Validations(inputValidations []v1.Validation) []cel.ExpressionAccessor {
	celExpressionAccessor := make([]cel.ExpressionAccessor, len(inputValidations))
	for i, validation := range inputValidations {
		validation := ValidationCondition{
			Expression: validation.Expression,
			Message:    validation.Message,
			Reason:     validation.Reason,
		}
		celExpressionAccessor[i] = &validation
	}
	return celExpressionAccessor
}

// convertv1MessageExpressions 提取带 messageExpression 的校验规则。
//
// 保持与 Validations 相同的下标，使编译结果与原始规则一一对应；
// 未设置该字段的位置保留 nil，执行时会跳过。
func convertv1MessageExpressions(inputValidations []v1.Validation) []cel.ExpressionAccessor {
	celExpressionAccessor := make([]cel.ExpressionAccessor, len(inputValidations))
	for i, validation := range inputValidations {
		if validation.MessageExpression != "" {
			condition := MessageExpressionCondition{
				MessageExpression: validation.MessageExpression,
			}
			celExpressionAccessor[i] = &condition
		}
	}
	return celExpressionAccessor
}

// convertv1AuditAnnotations 把审计注解规则适配为 CEL 表达式访问器。
func convertv1AuditAnnotations(inputValidations []v1.AuditAnnotation) []cel.ExpressionAccessor {
	celExpressionAccessor := make([]cel.ExpressionAccessor, len(inputValidations))
	for i, validation := range inputValidations {
		validation := AuditAnnotationCondition{
			Key:             validation.Key,
			ValueExpression: validation.ValueExpression,
		}
		celExpressionAccessor[i] = &validation
	}
	return celExpressionAccessor
}

// convertv1beta1Variables 把变量声明适配为具名 CEL 表达式。
//
// 函数名保留 v1beta1 是历史原因（变量特性最初在 v1beta1 引入），
// 当前输入已统一为 v1 类型。
func convertv1beta1Variables(variables []v1.Variable) []cel.NamedExpressionAccessor {
	namedExpressions := make([]cel.NamedExpressionAccessor, len(variables))
	for i, variable := range variables {
		namedExpressions[i] = &Variable{Name: variable.Name, Expression: variable.Expression}
	}
	return namedExpressions
}
