/*
Copyright 2022 The Kubernetes Authors.

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

package cel

import (
	"context"

	admissionv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/cel/environment"
)

// conditionCompiler 实现 ConditionCompiler 接口。
// 它把编译职责与求值职责分离：编译在策略加载/重载时进行，求值在每次准入请求时进行。
type conditionCompiler struct {
	compiler Compiler
}

// NewConditionCompiler 基于给定的 CEL 环境集合创建条件编译器。
// 复用同一个 Compiler 实例可以共享环境缓存，避免每条策略重复构建类型检查器。
func NewConditionCompiler(env *environment.EnvSet) ConditionCompiler {
	return &conditionCompiler{compiler: NewCompiler(env)}
}

// CompileCondition 将 ExpressionAccessors 中定义的 CEL 表达式编译为 ConditionEvaluator。
// 编译结果按输入顺序保存在切片中，求值时依靠同样的下标对应回原始表达式。
// 单个表达式编译失败不会中断其余表达式的编译，错误统一在 CompilationErrors 中汇总上报。
func (c *conditionCompiler) CompileCondition(expressionAccessors []ExpressionAccessor, options OptionalVariableDeclarations, mode environment.Type) ConditionEvaluator {
	compilationResults := make([]CompilationResult, len(expressionAccessors))
	for i, expressionAccessor := range expressionAccessors {
		// nil 访问器代表该位置没有条件，保留零值结果，避免下标错位。
		if expressionAccessor == nil {
			continue
		}
		compilationResults[i] = c.compiler.CompileCELExpression(expressionAccessor, options, mode)
	}
	return NewCondition(compilationResults)
}

// condition 实现 ConditionEvaluator 接口。
// 它是编译结果的无状态容器，因此可被多个请求并发复用。
type condition struct {
	compilationResults []CompilationResult
}

// NewCondition 用已编译的表达式结果构造条件求值器。
func NewCondition(compilationResults []CompilationResult) ConditionEvaluator {
	return &condition{
		compilationResults,
	}
}

// ForInput 求值已编译的 CEL 表达式，将其转换为 CELEvaluations。
// 每次求值的错误都会通过 Evaluation 对象返回。
// runtimeCELCostBudget 仅用于测试。调用方应始终使用 k8s.io/apiserver/pkg/apis/cel/config.go 中的常量 RuntimeCELCostBudget 作为输入。
// 预算在多个表达式之间顺序递减，任一表达式耗尽预算都会返回错误，
// 从而限制单次准入请求中 CEL 计算的总开销，防止策略被用来拖垮 apiserver。
func (c *condition) ForInput(ctx context.Context, versionedAttr *admission.VersionedAttributes, request *admissionv1.AdmissionRequest, inputs OptionalVariableBindings, namespace *v1.Namespace, runtimeCELCostBudget int64) ([]EvaluationResult, int64, error) {
	// TODO: 待原生类型支持可用后，用 ref.Val 替换 CEL 变量的 unstructured 形式
	evaluations := make([]EvaluationResult, len(c.compilationResults))
	if len(c.compilationResults) == 0 {
		return evaluations, runtimeCELCostBudget, nil
	}

	var err error

	// 若该 activation 支持组合（composition），就需要 compositionCtx，它可能为 nil。
	// 类型断言失败时 compositionCtx 为 nil，此时退化为普通的单策略求值路径。
	compositionCtx, _ := ctx.(CompositionContext)

	// 每次求值都新建 activation：它持有本次请求的变量绑定，不能跨请求共享。
	activation, err := newActivation(compositionCtx, versionedAttr, request, inputs, namespace, useSchemalessTypeRef())
	if err != nil {
		return nil, -1, err
	}

	remainingBudget := runtimeCELCostBudget
	for i, compilationResult := range c.compilationResults {
		// 注意这里把剩余预算透传给下一个表达式，实现全局预算而非每表达式独立预算。
		evaluations[i], remainingBudget, err = activation.Evaluate(ctx, compositionCtx, compilationResult, remainingBudget)
		if err != nil {
			return nil, -1, err
		}
	}

	return evaluations, remainingBudget, nil
}

// TODO: to reuse https://github.com/kubernetes/kubernetes/blob/master/staging/src/k8s.io/apiserver/pkg/admission/plugin/webhook/request/admissionreview.go#L154
// CreateAdmissionRequest 依据准入属性构造 CEL 表达式可见的 AdmissionRequest。
// 关键点：Kind/Resource 使用匹配器判定等价的 GVK/GVR，而 RequestKind/RequestResource
// 保留原始请求值。这样策略对同一资源的别名（如不同版本）只写一份即可，
// 同时仍能通过 request* 字段观察到调用方真正请求的版本。
func CreateAdmissionRequest(attr admission.Attributes, equivalentGVR metav1.GroupVersionResource, equivalentKind metav1.GroupVersionKind) *admissionv1.AdmissionRequest {
	// 尽量复用与 webhook 相同的逻辑来构造资源的
	// GVK、GVR、subresource。
	// 使用 matcher 判定为与请求等价的 GVK、GVR。
	// https://github.com/kubernetes/kubernetes/blob/90c362b3430bcbbf8f245fadbcd521dab39f1d7c/staging/src/k8s.io/apiserver/pkg/admission/plugin/webhook/generic/webhook.go#L182-L210
	gvk := equivalentKind
	gvr := equivalentGVR
	subresource := attr.GetSubresource()

	requestGVK := attr.GetKind()
	requestGVR := attr.GetResource()
	requestSubResource := attr.GetSubresource()

	// 将认证层的 UserInfo 转成 API 类型；Extra 需要逐键深拷贝，
	// 避免表达式或后续代码修改到认证缓存中共享的 map。
	aUserInfo := attr.GetUserInfo()
	var userInfo authenticationv1.UserInfo
	if aUserInfo != nil {
		userInfo = authenticationv1.UserInfo{
			Extra:    make(map[string]authenticationv1.ExtraValue),
			Groups:   aUserInfo.GetGroups(),
			UID:      aUserInfo.GetUID(),
			Username: aUserInfo.GetName(),
		}
		// 转换 user 对象中的 extra 信息
		for key, val := range aUserInfo.GetExtra() {
			userInfo.Extra[key] = authenticationv1.ExtraValue(val)
		}
	}

	dryRun := attr.IsDryRun()

	return &admissionv1.AdmissionRequest{
		Kind: metav1.GroupVersionKind{
			Group:   gvk.Group,
			Kind:    gvk.Kind,
			Version: gvk.Version,
		},
		Resource: metav1.GroupVersionResource{
			Group:    gvr.Group,
			Resource: gvr.Resource,
			Version:  gvr.Version,
		},
		SubResource: subresource,
		RequestKind: &metav1.GroupVersionKind{
			Group:   requestGVK.Group,
			Kind:    requestGVK.Kind,
			Version: requestGVK.Version,
		},
		RequestResource: &metav1.GroupVersionResource{
			Group:    requestGVR.Group,
			Resource: requestGVR.Resource,
			Version:  requestGVR.Version,
		},
		RequestSubResource: requestSubResource,
		Name:               attr.GetName(),
		Namespace:          attr.GetNamespace(),
		Operation:          admissionv1.Operation(attr.GetOperation()),
		UserInfo:           userInfo,
		// 保持 Object 与 OldObject 不设置，因为我们不通过 request 提供对它们的访问
		// 策略表达式通过 object/oldObject 变量直接访问对象，request 变量刻意不暴露它们，
		// 以避免同一数据出现两条访问路径、造成语义分叉。
		DryRun: &dryRun,
		Options: runtime.RawExtension{
			Object: attr.GetOperationOptions(),
		},
	}
}

// CreateNamespaceObject 创建适合 CEL 求值的 Namespace 对象。
// 若 namespace 为 nil，CreateNamespaceObject 返回 nil。
// 只复制 CEL 表达式可能用到的字段，属于显式的浅拷贝白名单：
// 新增需要暴露的字段时必须同步修改这里，否则表达式将看不到它。
func CreateNamespaceObject(namespace *v1.Namespace) *v1.Namespace {
	if namespace == nil {
		return nil
	}

	return &v1.Namespace{
		Status: namespace.Status,
		Spec:   namespace.Spec,
		ObjectMeta: metav1.ObjectMeta{
			Name:                       namespace.Name,
			GenerateName:               namespace.GenerateName,
			Namespace:                  namespace.Namespace,
			UID:                        namespace.UID,
			ResourceVersion:            namespace.ResourceVersion,
			Generation:                 namespace.Generation,
			CreationTimestamp:          namespace.CreationTimestamp,
			DeletionTimestamp:          namespace.DeletionTimestamp,
			DeletionGracePeriodSeconds: namespace.DeletionGracePeriodSeconds,
			Labels:                     namespace.Labels,
			Annotations:                namespace.Annotations,
			Finalizers:                 namespace.Finalizers,
		},
	}
}

// CompilationErrors 返回 mutatingEvaluator 编译产生的全部错误列表。
// 与 ForInput 不同，这里不涉及运行时错误；仅用于策略加载阶段判断表达式是否可用。
func (c *condition) CompilationErrors() []error {
	compilationErrors := []error{}
	for _, result := range c.compilationResults {
		if result.Error != nil {
			compilationErrors = append(compilationErrors, result.Error)
		}
	}
	return compilationErrors
}
