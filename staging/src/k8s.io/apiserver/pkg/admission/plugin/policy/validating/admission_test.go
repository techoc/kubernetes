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

// 本文件是 ValidatingAdmissionPolicy 插件的端到端行为测试。
// 使用 external test package（validating_test）通过公开 API 驱动插件，
// 并用 fakeCompiler/fakeMatcher 替换 CEL 编译与匹配逻辑，
// 从而把测试焦点放在“策略/绑定/参数的生命周期与分发语义”上，而不是 CEL 求值本身。
package validating_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	admissionregistrationv1 "k8s.io/api/admissionregistration/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utiljson "k8s.io/apimachinery/pkg/util/json"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/admission/plugin/policy/generic"
	"k8s.io/apiserver/pkg/admission/plugin/policy/matching"
	"k8s.io/apiserver/pkg/admission/plugin/policy/validating"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/warning"
	"k8s.io/client-go/kubernetes"
	"k8s.io/utils/ptr"
)

var (
	// 命名空间级参数类型：用于验证 paramRef 的命名空间解析与跨命名空间隔离。
	clusterScopedParamsGVK schema.GroupVersionKind = schema.GroupVersionKind{
		Group:   "example.com",
		Version: "v1",
		Kind:    "ClusterScopedParamsConfig",
	}

	paramsGVK schema.GroupVersionKind = schema.GroupVersionKind{
		Group:   "example.com",
		Version: "v1",
		Kind:    "ParamsConfig",
	}

	// Common objects
	// denyPolicy 是一个最小可用的策略：校验表达式内容不重要，因为 fakeCompiler 会忽略它，
	// 真正决定结果的是各测试注册的 validateFunc。
	denyPolicy *admissionregistrationv1.ValidatingAdmissionPolicy = &admissionregistrationv1.ValidatingAdmissionPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "denypolicy.example.com",
			ResourceVersion: "1",
		},
		Spec: admissionregistrationv1.ValidatingAdmissionPolicySpec{
			ParamKind: &admissionregistrationv1.ParamKind{
				APIVersion: paramsGVK.GroupVersion().String(),
				Kind:       paramsGVK.Kind,
			},
			FailurePolicy: ptr.To(admissionregistrationv1.Fail),
			Validations: []admissionregistrationv1.Validation{
				{
					Expression: "messageId for deny policy",
				},
			},
		},
	}

	// fakeParams 是策略参数对象（非结构化），其内容不会被真实求值，仅用于验证参数解析链路。
	fakeParams *unstructured.Unstructured = &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": paramsGVK.GroupVersion().String(),
			"kind":       paramsGVK.Kind,
			"metadata": map[string]interface{}{
				"name":            "replicas-test.example.com",
				"namespace":       "default",
				"resourceVersion": "1",
			},
			"maxReplicas": int64(3),
		},
	}

	// 以下绑定变体只改变 ValidationActions，用于分别验证 Deny/Warn/Audit 三种动作的分发与副作用。
	denyBinding *admissionregistrationv1.ValidatingAdmissionPolicyBinding = &admissionregistrationv1.ValidatingAdmissionPolicyBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "denybinding.example.com",
			ResourceVersion: "1",
		},
		Spec: admissionregistrationv1.ValidatingAdmissionPolicyBindingSpec{
			PolicyName: denyPolicy.Name,
			ParamRef: &admissionregistrationv1.ParamRef{
				Name:      fakeParams.GetName(),
				Namespace: fakeParams.GetNamespace(),
				// fake object tracker does not populate defaults
				ParameterNotFoundAction: ptr.To(admissionregistrationv1.DenyAction),
			},
			ValidationActions: []admissionregistrationv1.ValidationAction{admissionregistrationv1.Deny},
		},
	}
	denyBindingWithNoParamRef *admissionregistrationv1.ValidatingAdmissionPolicyBinding = &admissionregistrationv1.ValidatingAdmissionPolicyBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "denybinding.example.com",
			ResourceVersion: "1",
		},
		Spec: admissionregistrationv1.ValidatingAdmissionPolicyBindingSpec{
			PolicyName:        denyPolicy.Name,
			ValidationActions: []admissionregistrationv1.ValidationAction{admissionregistrationv1.Deny},
		},
	}

	denyBindingWithAudit = &admissionregistrationv1.ValidatingAdmissionPolicyBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "denybinding.example.com",
			ResourceVersion: "1",
		},
		Spec: admissionregistrationv1.ValidatingAdmissionPolicyBindingSpec{
			PolicyName:        denyPolicy.Name,
			ValidationActions: []admissionregistrationv1.ValidationAction{admissionregistrationv1.Audit},
		},
	}
	denyBindingWithWarn = &admissionregistrationv1.ValidatingAdmissionPolicyBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "denybinding.example.com",
			ResourceVersion: "1",
		},
		Spec: admissionregistrationv1.ValidatingAdmissionPolicyBindingSpec{
			PolicyName:        denyPolicy.Name,
			ValidationActions: []admissionregistrationv1.ValidationAction{admissionregistrationv1.Warn},
		},
	}
	denyBindingWithAll = &admissionregistrationv1.ValidatingAdmissionPolicyBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "denybinding.example.com",
			ResourceVersion: "1",
		},
		Spec: admissionregistrationv1.ValidatingAdmissionPolicyBindingSpec{
			PolicyName:        denyPolicy.Name,
			ValidationActions: []admissionregistrationv1.ValidationAction{admissionregistrationv1.Deny, admissionregistrationv1.Warn, admissionregistrationv1.Audit},
		},
	}
)

// newParam 构造命名空间级参数对象；namespace 为空时落到 default，
// 因为 fake object tracker 不会自动填充默认命名空间。
func newParam(name, namespace string, labels map[string]string) *unstructured.Unstructured {
	if len(namespace) == 0 {
		namespace = metav1.NamespaceDefault
	}
	res := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": paramsGVK.GroupVersion().String(),
			"kind":       paramsGVK.Kind,
			"metadata": map[string]interface{}{
				"name":            name,
				"namespace":       namespace,
				"resourceVersion": "1",
			},
		},
	}
	res.SetLabels(labels)
	return res
}

// newClusterScopedParam 构造集群级参数对象，用于验证集群级 paramKind 的解析路径。
func newClusterScopedParam(name string, labels map[string]string) *unstructured.Unstructured {
	res := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": clusterScopedParamsGVK.GroupVersion().String(),
			"kind":       clusterScopedParamsGVK.Kind,
			"metadata": map[string]interface{}{
				"name":            name,
				"resourceVersion": "1",
			},
		},
	}
	res.SetLabels(labels)
	return res
}

// 编译期断言：validateFunc 必须满足 Validator 接口，避免接口演进后测试桩静默失效。
var _ validating.Validator = validateFunc(nil)

// validateFunc 是 Validator 的函数式适配器，让每个测试用闭包直接给出校验结果。
type validateFunc func(
	ctx context.Context,
	matchResource schema.GroupVersionResource,
	versionedAttr *admission.VersionedAttributes,
	versionedParams runtime.Object,
	namespace *v1.Namespace,
	runtimeCELCostBudget int64,
	authz authorizer.UnconditionalAuthorizer) validating.ValidateResult

// CompileError 固定返回 nil：这些测试关注运行时行为，不模拟编译失败。
func (f validateFunc) CompileError() error { return nil }

// fakeCompiler 用“策略名 -> 预置校验函数”的映射替代真实的 CEL 编译，
// 同时统计每条策略的编译次数，用于断言编译缓存是否生效（避免每次请求都重编译）。
type fakeCompiler struct {
	ValidateFuncs map[types.NamespacedName]validating.Validator

	// lock 保护 NumCompiles：编译可能发生在多个 informer/工作协程中。
	lock        sync.Mutex
	NumCompiles map[types.NamespacedName]int
}

// getNumCompiles 读取某策略的累计编译次数，加锁以兼容并发访问。
func (f *fakeCompiler) getNumCompiles(p *validating.Policy) int {
	f.lock.Lock()
	defer f.lock.Unlock()
	return f.NumCompiles[types.NamespacedName{
		Name:      p.Name,
		Namespace: p.Namespace,
	}]
}

// RegisterDefinition 为指定策略注册校验函数；按 namespacedName 索引以支持同名不同命名空间。
func (f *fakeCompiler) RegisterDefinition(definition *validating.Policy, vf validateFunc) {
	if f.ValidateFuncs == nil {
		f.ValidateFuncs = make(map[types.NamespacedName]validating.Validator)
	}

	f.ValidateFuncs[types.NamespacedName{
		Name:      definition.Name,
		Namespace: definition.Namespace,
	}] = vf
}

// CompilePolicy 返回预置的校验函数并记录一次编译。
// 未注册的策略返回 nil，插件应据此把策略视为不可用。
func (f *fakeCompiler) CompilePolicy(policy *validating.Policy) validating.Validator {
	nn := types.NamespacedName{
		Name:      policy.Name,
		Namespace: policy.Namespace,
	}

	defer func() {
		f.lock.Lock()
		defer f.lock.Unlock()
		if f.NumCompiles == nil {
			f.NumCompiles = make(map[types.NamespacedName]int)
		}
		f.NumCompiles[nn]++
	}()
	return f.ValidateFuncs[nn]
}

// Validate 把接口调用转发到函数体，实现 Validator 接口。
func (f validateFunc) Validate(
	ctx context.Context,
	matchResource schema.GroupVersionResource,
	versionedAttr *admission.VersionedAttributes,
	versionedParams runtime.Object,
	namespace *v1.Namespace,
	runtimeCELCostBudget int64,
	authz authorizer.UnconditionalAuthorizer,
) validating.ValidateResult {
	return f(
		ctx,
		matchResource,
		versionedAttr,
		versionedParams,
		namespace,
		runtimeCELCostBudget,
		authz,
	)
}

// fakeMatcher 用“名称 -> 匹配函数”的映射替代真实的 CEL 匹配器，
// 使测试可以精确控制某个策略/绑定是否命中，而不必构造完整的匹配配置。
var _ generic.PolicyMatcher = &fakeMatcher{}

// ValidateInitialization 恒成功：fakeMatcher 没有需要初始化的外部依赖。
func (f *fakeMatcher) ValidateInitialization() error {
	return nil
}

// GetNamespace 返回 nil 表示测试环境不提供真实命名空间对象。
func (f *fakeMatcher) GetNamespace(ctx context.Context, name string) (*v1.Namespace, error) {
	return nil, nil
}

// fakeMatcher 是 PolicyMatcher 的测试实现。
// DefaultMatch 决定未注册策略的默认匹配结果；
// 两个 map 分别按 namespacedName 覆盖策略定义与绑定的匹配逻辑。
type fakeMatcher struct {
	DefaultMatch         bool
	DefinitionMatchFuncs map[types.NamespacedName]func(generic.PolicyAccessor, admission.Attributes) bool
	BindingMatchFuncs    map[types.NamespacedName]func(generic.BindingAccessor, admission.Attributes) bool
}

// RegisterDefinition 为策略注册自定义匹配函数；nil 表示保持默认行为。
func (f *fakeMatcher) RegisterDefinition(definition *admissionregistrationv1.ValidatingAdmissionPolicy, matchFunc func(generic.PolicyAccessor, admission.Attributes) bool) {
	namespace, name := definition.Namespace, definition.Name
	key := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}

	if matchFunc != nil {
		if f.DefinitionMatchFuncs == nil {
			f.DefinitionMatchFuncs = make(map[types.NamespacedName]func(generic.PolicyAccessor, admission.Attributes) bool)
		}
		f.DefinitionMatchFuncs[key] = matchFunc
	}
}

// RegisterBinding 为绑定注册自定义匹配函数；nil 表示保持默认行为。
func (f *fakeMatcher) RegisterBinding(binding *admissionregistrationv1.ValidatingAdmissionPolicyBinding, matchFunc func(generic.BindingAccessor, admission.Attributes) bool) {
	namespace, name := binding.Namespace, binding.Name
	key := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}

	if matchFunc != nil {
		if f.BindingMatchFuncs == nil {
			f.BindingMatchFuncs = make(map[types.NamespacedName]func(generic.BindingAccessor, admission.Attributes) bool)
		}
		f.BindingMatchFuncs[key] = matchFunc
	}
}

// Matches says whether this policy definition matches the provided admission
// resource request
// 等价 GVK/GVR 直接回填请求自身的值，因为 fakeMatcher 不做版本转换。
func (f *fakeMatcher) DefinitionMatches(a admission.Attributes, o admission.ObjectInterfaces, definition generic.PolicyAccessor) (bool, schema.GroupVersionResource, schema.GroupVersionKind, error) {
	namespace, name := definition.GetNamespace(), definition.GetName()
	key := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}
	if fun, ok := f.DefinitionMatchFuncs[key]; ok {
		return fun(definition, a), a.GetResource(), a.GetKind(), nil
	}

	// Default is match everything
	return f.DefaultMatch, a.GetResource(), a.GetKind(), nil
}

// Matches says whether this policy definition matches the provided admission
// resource request
// 与 DefinitionMatches 对称：未注册的绑定回退到 DefaultMatch。
func (f *fakeMatcher) BindingMatches(a admission.Attributes, o admission.ObjectInterfaces, binding generic.BindingAccessor) (bool, error) {
	namespace, name := binding.GetNamespace(), binding.GetName()
	key := types.NamespacedName{
		Name:      name,
		Namespace: namespace,
	}
	if fun, ok := f.BindingMatchFuncs[key]; ok {
		return fun(binding, a), nil
	}

	// Default is match everything
	return f.DefaultMatch, nil
}

// setupFakeTest 是 setupTestCommon 的便捷封装：默认启动 informer，
// 大多数用例需要插件处于就绪状态才能分发请求。
func setupFakeTest(t *testing.T, comp *fakeCompiler, match *fakeMatcher) *generic.PolicyTestContext[*validating.Policy, *validating.PolicyBinding, validating.Validator] {
	return setupTestCommon(t, comp, match, true)
}

// Starts CEL admission controller and sets up a plugin configured with it as well
// as object trackers for manipulating the objects available to the system
//
// ParamTracker only knows the gvk `paramGVK`. If in the future we need to
// support multiple types of params this function needs to be augmented
//
// PolicyTracker expects FakePolicyDefinition and FakePolicyBinding types
// !TODO: refactor this test/framework to remove startInformers argument and
// clean up the return args, and in general make it more accessible.
//
// 实现细节：
//   - 缩短策略刷新间隔，避免测试等待默认的较长周期；
//   - RESTMapper 中显式登记参数类型与 VAP 自身，供参数解析使用；
//   - 通过 t.Cleanup 注册取消函数，保证 informer 协程随测试结束退出。
func setupTestCommon(
	t *testing.T,
	compiler *fakeCompiler,
	matcher generic.PolicyMatcher,
	shouldStartInformers bool,
) *generic.PolicyTestContext[*validating.Policy, *validating.PolicyBinding, validating.Validator] {
	t.Cleanup(generic.SetPolicyRefreshIntervalForTests(10 * time.Millisecond))
	testContext, testContextCancel, err := generic.NewPolicyTestContext(
		t,
		validating.NewValidatingAdmissionPolicyAccessor,
		validating.NewValidatingAdmissionPolicyBindingAccessor,
		func(p *validating.Policy) validating.Validator {
			return compiler.CompilePolicy(p)
		},
		func(a authorizer.UnconditionalAuthorizer, m *matching.Matcher, client kubernetes.Interface) generic.Dispatcher[validating.PolicyHook] {
			coolMatcher := matcher
			if coolMatcher == nil {
				coolMatcher = generic.NewPolicyMatcher(m)
			}
			return validating.NewDispatcher(a, coolMatcher)
		},
		nil,
		[]meta.RESTMapping{
			{
				Resource:         paramsGVK.GroupVersion().WithResource("paramsconfigs"),
				GroupVersionKind: paramsGVK,
				Scope:            meta.RESTScopeNamespace,
			},
			{
				Resource:         clusterScopedParamsGVK.GroupVersion().WithResource("clusterscopedparamsconfigs"),
				GroupVersionKind: clusterScopedParamsGVK,
				Scope:            meta.RESTScopeRoot,
			},
			{
				Resource:         schema.GroupVersionResource{Group: "admissionregistration.k8s.io", Version: "v1beta1", Resource: "validatingadmissionpolicies"},
				GroupVersionKind: schema.GroupVersionKind{Group: "admissionregistration.k8s.io", Version: "v1beta1", Kind: "ValidatingAdmissionPolicy"},
				Scope:            meta.RESTScopeRoot,
			},
		},
	)
	require.NoError(t, err)
	t.Cleanup(testContextCancel)

	if shouldStartInformers {
		require.NoError(t, testContext.Start())
	}

	return testContext
}

// attributeRecord 用 old/new 之一构造 FakeAttributes；两者都为 nil 属于测试编写错误，直接 panic。
func attributeRecord(
	old, new runtime.Object,
	operation admission.Operation,
) *FakeAttributes {
	if old == nil && new == nil {
		panic("both `old` and `new` may not be nil")
	}

	// one of old/new may be nil, but not both
	example := new
	if example == nil {
		example = old
	}

	accessor, err := meta.Accessor(example)
	if err != nil {
		panic(err)
	}

	return &FakeAttributes{
		Attributes: admission.NewAttributesRecord(
			new,
			old,
			example.GetObjectKind().GroupVersionKind(),
			accessor.GetNamespace(),
			accessor.GetName(),
			schema.GroupVersionResource{},
			"",
			operation,
			nil,
			false,
			nil,
		),
	}
}

// //////////////////////////////////////////////////////////////////////////////
// Functionality Tests
// //////////////////////////////////////////////////////////////////////////////

// TestPluginNotReady 验证 informer 未启动（或初次 list/watch 失败）时插件拒绝处理请求，
// 启动后同一请求可正常通过——这是“未就绪即失败关闭”的保障，避免在策略未加载时放行流量。
func TestPluginNotReady(t *testing.T) {
	compiler := &fakeCompiler{}
	matcher := &fakeMatcher{
		DefaultMatch: true,
	}

	// Show that an unstarted informer (or one that has failed its listwatch)
	// will show proper error from plugin
	ctx := setupTestCommon(t, compiler, matcher, false)
	err := ctx.Plugin.Dispatch(
		context.Background(),
		// Object is irrelevant/unchecked for this test. Just test that
		// the evaluator is executed, and returns a denial
		attributeRecord(nil, fakeParams, admission.Create),
		&admission.RuntimeObjectInterfaces{},
	)

	require.ErrorContains(t, err, "not yet ready to handle request")

	// Show that by now starting the informer, the error is dissipated
	ctx = setupTestCommon(t, compiler, matcher, true)
	err = ctx.Plugin.Dispatch(
		context.Background(),
		// Object is irrelevant/unchecked for this test. Just test that
		// the evaluator is executed, and returns a denial
		attributeRecord(nil, fakeParams, admission.Create),
		&admission.RuntimeObjectInterfaces{},
	)

	require.NoError(t, err)
}

// TestBasicPolicyDefinitionFailure 验证最简链路：策略命中并返回 Deny 时请求被拒绝，
// 且 Deny 动作不会产生告警或审计注解（这些只属于 Warn/Audit 动作）。
func TestBasicPolicyDefinitionFailure(t *testing.T) {
	datalock := sync.Mutex{}
	numCompiles := 0

	compiler := &fakeCompiler{}
	matcher := &fakeMatcher{
		DefaultMatch: true,
	}

	compiler.RegisterDefinition(denyPolicy, func(ctx context.Context, matchedResource schema.GroupVersionResource, versionedAttr *admission.VersionedAttributes, versionedParams runtime.Object, namespace *v1.Namespace, runtimeCELCostBudget int64, authz authorizer.UnconditionalAuthorizer) validating.ValidateResult {
		datalock.Lock()
		numCompiles += 1
		datalock.Unlock()
		return validating.ValidateResult{
			Decisions: []validating.PolicyDecision{
				{
					Action:  validating.ActionDeny,
					Message: "Denied",
				},
			},
		}
	})

	testContext := setupFakeTest(t, compiler, matcher)
	require.NoError(t, testContext.UpdateAndWait(fakeParams, denyPolicy, denyBinding))

	warningRecorder := newWarningRecorder()
	warnCtx := warning.WithWarningRecorder(testContext, warningRecorder)
	attr := attributeRecord(nil, fakeParams, admission.Create)
	err := testContext.Plugin.Dispatch(
		warnCtx,
		// Object is irrelevant/unchecked for this test. Just test that
		// the evaluator is executed, and returns a denial
		attr,
		&admission.RuntimeObjectInterfaces{},
	)

	require.Equal(t, 0, warningRecorder.len())

	annotations := attr.GetAnnotations(auditinternal.LevelMetadata)
	require.Empty(t, annotations)

	require.ErrorContains(t, err, `Denied`)
}

// Shows that if a definition does not match the input, it will not be used.
// But with a different input it will be used.
// 这里用“对象名长度为偶数”作为匹配条件，构造奇偶两个输入验证匹配器的分流。
func TestDefinitionDoesntMatch(t *testing.T) {
	compiler := &fakeCompiler{}
	matcher := &fakeMatcher{
		DefaultMatch: true,
	}

	testContext := setupFakeTest(t, compiler, matcher)

	datalock := sync.Mutex{}
	passedParams := []*unstructured.Unstructured{}
	numCompiles := 0

	compiler.RegisterDefinition(denyPolicy, func(ctx context.Context, matchedResource schema.GroupVersionResource, versionedAttr *admission.VersionedAttributes, versionedParams runtime.Object, namespace *v1.Namespace, runtimeCELCostBudget int64, authz authorizer.UnconditionalAuthorizer) validating.ValidateResult {
		datalock.Lock()
		numCompiles += 1
		datalock.Unlock()
		return validating.ValidateResult{
			Decisions: []validating.PolicyDecision{
				{
					Action:  validating.ActionDeny,
					Message: "Denied",
				},
			},
		}
	})

	matcher.RegisterDefinition(denyPolicy, func(vap generic.PolicyAccessor, a admission.Attributes) bool {
		// Match names with even-numbered length
		obj := a.GetObject()

		accessor, err := meta.Accessor(obj)
		if err != nil {
			t.Fatal(err)
			return false
		}

		return len(accessor.GetName())%2 == 0
	})

	require.NoError(t, testContext.UpdateAndWait(fakeParams, denyPolicy, denyBinding))

	// Validate a non-matching input.
	// Should pass validation with no error.

	nonMatchingParams := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": paramsGVK.GroupVersion().String(),
			"kind":       paramsGVK.Kind,
			"metadata": map[string]interface{}{
				"name":            "oddlength",
				"resourceVersion": "1",
			},
		},
	}
	require.NoError(t,
		testContext.Plugin.Dispatch(testContext,
			attributeRecord(
				nil, nonMatchingParams,
				admission.Create), &admission.RuntimeObjectInterfaces{}))
	require.Empty(t, passedParams)

	// Validate a matching input.
	// Should match and be denied.
	matchingParams := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": paramsGVK.GroupVersion().String(),
			"kind":       paramsGVK.Kind,
			"metadata": map[string]interface{}{
				"name":            "evenlength",
				"resourceVersion": "1",
			},
		},
	}
	require.ErrorContains(t,
		testContext.Plugin.Dispatch(testContext,
			attributeRecord(
				nil, matchingParams,
				admission.Create), &admission.RuntimeObjectInterfaces{}),
		`Denied`)
	require.Equal(t, 1, numCompiles)
}

// TestReconfigureBinding 验证绑定参数变化时的重编译行为：
// 指向不存在的参数应报配置错误，补上参数后恢复校验；改变绑定的参数引用会触发一次重编译。
func TestReconfigureBinding(t *testing.T) {
	compiler := &fakeCompiler{}
	matcher := &fakeMatcher{
		DefaultMatch: true,
	}

	testContext := setupFakeTest(t, compiler, matcher)

	datalock := sync.Mutex{}
	numCompiles := 0

	fakeParams2 := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": paramsGVK.GroupVersion().String(),
			"kind":       paramsGVK.Kind,
			"metadata": map[string]interface{}{
				"name": "replicas-test2.example.com",
				// fake object tracker does not populate missing namespace
				"namespace":       "default",
				"resourceVersion": "2",
			},
			"maxReplicas": int64(35),
		},
	}

	compiler.RegisterDefinition(denyPolicy, func(ctx context.Context, matchedResource schema.GroupVersionResource, versionedAttr *admission.VersionedAttributes, versionedParams runtime.Object, namespace *v1.Namespace, runtimeCELCostBudget int64, authz authorizer.UnconditionalAuthorizer) validating.ValidateResult {
		datalock.Lock()
		numCompiles += 1
		datalock.Unlock()
		return validating.ValidateResult{
			Decisions: []validating.PolicyDecision{
				{
					Action:  validating.ActionDeny,
					Message: "Denied",
				},
			},
		}
	})

	denyBinding2 := &admissionregistrationv1.ValidatingAdmissionPolicyBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "denybinding.example.com",
			ResourceVersion: "2",
		},
		Spec: admissionregistrationv1.ValidatingAdmissionPolicyBindingSpec{
			PolicyName: denyPolicy.Name,
			ParamRef: &admissionregistrationv1.ParamRef{
				Name:                    fakeParams2.GetName(),
				Namespace:               fakeParams2.GetNamespace(),
				ParameterNotFoundAction: ptr.To(admissionregistrationv1.DenyAction),
			},
			ValidationActions: []admissionregistrationv1.ValidationAction{admissionregistrationv1.Deny},
		},
	}

	require.NoError(t, testContext.UpdateAndWait(fakeParams, denyPolicy, denyBinding))

	err := testContext.Plugin.Dispatch(
		testContext,
		attributeRecord(nil, fakeParams, admission.Create),
		&admission.RuntimeObjectInterfaces{},
	)

	// Expect validation to fail for first time due to binding unconditionally
	// failing
	require.ErrorContains(t, err, `Denied`, "expect policy validation error")

	// Expect `Compile` only called once
	require.Equal(t, 1, numCompiles, "expect `Compile` to be called only once")

	// Update the tracker to point at different params
	require.NoError(t, testContext.UpdateAndWait(denyBinding2))

	err = testContext.Plugin.Dispatch(
		testContext,
		attributeRecord(nil, fakeParams, admission.Create),
		&admission.RuntimeObjectInterfaces{},
	)

	require.ErrorContains(t, err, "no params found for policy binding with `Deny` parameterNotFoundAction")

	// Add the missing params
	require.NoError(t, testContext.UpdateAndWait(fakeParams2))

	// Expect validation to now fail again.
	err = testContext.Plugin.Dispatch(
		testContext,
		attributeRecord(nil, fakeParams, admission.Create),
		&admission.RuntimeObjectInterfaces{},
	)

	// Expect validation to fail the third time due to validation failure
	require.ErrorContains(t, err, `Denied`, "expected a true policy failure, not a configuration error")
	// require.Equal(t, []*unstructured.Unstructured{fakeParams, fakeParams2}, passedParams, "expected call to `Validate` to cause call to evaluator")
	require.Equal(t, 2, numCompiles, "expect changing binding causes a recompile")
}

// Shows that a policy which is in effect will stop being in effect when removed
func TestRemoveDefinition(t *testing.T) {
	// 删除策略后，同一个请求应从“被拒绝”变为“无策略处理而放行”，验证缓存能及时失效。
	compiler := &fakeCompiler{}
	matcher := &fakeMatcher{
		DefaultMatch: true,
	}

	testContext := setupFakeTest(t, compiler, matcher)

	datalock := sync.Mutex{}
	numCompiles := 0

	compiler.RegisterDefinition(denyPolicy, func(ctx context.Context, matchedResource schema.GroupVersionResource, versionedAttr *admission.VersionedAttributes, versionedParams runtime.Object, namespace *v1.Namespace, runtimeCELCostBudget int64, authz authorizer.UnconditionalAuthorizer) validating.ValidateResult {
		datalock.Lock()
		numCompiles += 1
		datalock.Unlock()

		return validating.ValidateResult{
			Decisions: []validating.PolicyDecision{
				{
					Action:  validating.ActionDeny,
					Message: "Denied",
				},
			},
		}
	})

	require.NoError(t, testContext.UpdateAndWait(fakeParams, denyPolicy, denyBinding))

	record := attributeRecord(nil, fakeParams, admission.Create)
	require.ErrorContains(t,
		testContext.Plugin.Dispatch(
			testContext,
			record,
			&admission.RuntimeObjectInterfaces{},
		),
		`Denied`)

	require.NoError(t, testContext.DeleteAndWait(denyPolicy))

	require.NoError(t, testContext.Plugin.Dispatch(
		testContext,
		// Object is irrelevant/unchecked for this test. Just test that
		// the evaluator is executed, and returns a denial
		record,
		&admission.RuntimeObjectInterfaces{},
	))
}

// Shows that a binding which is in effect will stop being in effect when removed
func TestRemoveBinding(t *testing.T) {
	// 与 TestRemoveDefinition 对称：策略仍在，但失去绑定后不应再被分发。
	compiler := &fakeCompiler{}
	matcher := &fakeMatcher{
		DefaultMatch: true,
	}

	testContext := setupFakeTest(t, compiler, matcher)

	datalock := sync.Mutex{}
	numCompiles := 0

	compiler.RegisterDefinition(denyPolicy, func(ctx context.Context, matchedResource schema.GroupVersionResource, versionedAttr *admission.VersionedAttributes, versionedParams runtime.Object, namespace *v1.Namespace, runtimeCELCostBudget int64, authz authorizer.UnconditionalAuthorizer) validating.ValidateResult {
		datalock.Lock()
		numCompiles += 1
		datalock.Unlock()

		return validating.ValidateResult{
			Decisions: []validating.PolicyDecision{
				{
					Action:  validating.ActionDeny,
					Message: "Denied",
				},
			},
		}
	})

	require.NoError(t, testContext.UpdateAndWait(fakeParams, denyPolicy, denyBinding))

	record := attributeRecord(nil, fakeParams, admission.Create)

	require.ErrorContains(t,
		testContext.Plugin.Dispatch(
			testContext,
			record,
			&admission.RuntimeObjectInterfaces{},
		),
		`Denied`)

	require.NoError(t, testContext.DeleteAndWait(denyBinding))
}

// Shows that an error is surfaced if a paramSource specified in a binding does
// not actually exist
// 此处 paramKind 指向一个未注册的 Kind，错误应明确指向“找不到 paramKind 引用的资源”，
// 而不是笼统的 CRD 不存在，便于集群管理员定位配置问题。
func TestInvalidParamSourceGVK(t *testing.T) {
	compiler := &fakeCompiler{}
	matcher := &fakeMatcher{
		DefaultMatch: true,
	}

	testContext := setupFakeTest(t, compiler, matcher)
	passedParams := make(chan *unstructured.Unstructured)

	badPolicy := *denyPolicy
	badPolicy.Spec.ParamKind = &admissionregistrationv1.ParamKind{
		APIVersion: paramsGVK.GroupVersion().String(),
		Kind:       "BadParamKind",
	}

	require.NoError(t, testContext.UpdateAndWait(&badPolicy, denyBinding))

	err := testContext.Plugin.Dispatch(
		testContext,
		attributeRecord(nil, fakeParams, admission.Create),
		&admission.RuntimeObjectInterfaces{},
	)

	// expect the specific error to be that the param was not found, not that CRD
	// is not existing
	require.ErrorContains(t, err,
		`failed to configure policy: failed to find resource referenced by paramKind: 'example.com/v1, Kind=BadParamKind'`)

	close(passedParams)
	require.Empty(t, passedParams)
}

// Shows that an error is surfaced if a param specified in a binding does not
// actually exist
// 参数类型合法但实例缺失，错误取决于 ParameterNotFoundAction（此处为 Deny）。
func TestInvalidParamSourceInstanceName(t *testing.T) {
	compiler := &fakeCompiler{}
	matcher := &fakeMatcher{
		DefaultMatch: true,
	}

	testContext := setupFakeTest(t, compiler, matcher)

	datalock := sync.Mutex{}
	passedParams := []*unstructured.Unstructured{}
	numCompiles := 0

	compiler.RegisterDefinition(denyPolicy, func(ctx context.Context, matchedResource schema.GroupVersionResource, versionedAttr *admission.VersionedAttributes, versionedParams runtime.Object, namespace *v1.Namespace, runtimeCELCostBudget int64, authz authorizer.UnconditionalAuthorizer) validating.ValidateResult {
		datalock.Lock()
		numCompiles += 1
		datalock.Unlock()

		return validating.ValidateResult{
			Decisions: []validating.PolicyDecision{
				{
					Action:  validating.ActionDeny,
					Message: "Denied",
				},
			},
		}
	})

	require.NoError(t, testContext.UpdateAndWait(denyPolicy, denyBinding))

	err := testContext.Plugin.Dispatch(
		testContext,
		attributeRecord(nil, fakeParams, admission.Create),
		&admission.RuntimeObjectInterfaces{},
	)

	// expect the specific error to be that the param was not found, not that CRD
	// is not existing
	require.ErrorContains(t, err,
		"no params found for policy binding with `Deny` parameterNotFoundAction")
	require.Empty(t, passedParams)
}

// Show that policy still gets evaluated with `nil` param if paramRef & namespaceParamRef
// are both unset
// 策略声明了 paramKind 但绑定未引用任何参数实例时，求值仍应发生，只是 params 为 nil；
// 策略作者需在 CEL 中自行处理 params 缺失的情况。
func TestEmptyParamRef(t *testing.T) {
	compiler := &fakeCompiler{}
	matcher := &fakeMatcher{
		DefaultMatch: true,
	}

	testContext := setupFakeTest(t, compiler, matcher)

	datalock := sync.Mutex{}
	numCompiles := 0

	compiler.RegisterDefinition(denyPolicy, func(ctx context.Context, matchedResource schema.GroupVersionResource, versionedAttr *admission.VersionedAttributes, versionedParams runtime.Object, namespace *v1.Namespace, runtimeCELCostBudget int64, authz authorizer.UnconditionalAuthorizer) validating.ValidateResult {
		datalock.Lock()
		numCompiles += 1
		datalock.Unlock()

		// Versioned params must be nil to pass the test
		if versionedParams != nil {
			return validating.ValidateResult{
				Decisions: []validating.PolicyDecision{
					{
						Action: validating.ActionAdmit,
					},
				},
			}
		}
		return validating.ValidateResult{
			Decisions: []validating.PolicyDecision{
				{
					Action:  validating.ActionDeny,
					Message: "Denied",
				},
			},
		}
	})

	require.NoError(t, testContext.UpdateAndWait(denyPolicy, denyBindingWithNoParamRef))

	err := testContext.Plugin.Dispatch(
		testContext,
		// Object is irrelevant/unchecked for this test. Just test that
		// the evaluator is executed, and returns a denial
		attributeRecord(nil, fakeParams, admission.Create),
		&admission.RuntimeObjectInterfaces{},
	)

	require.ErrorContains(t, err, `Denied`)
	require.Equal(t, 1, numCompiles)
}

// Shows that a definition with no param source works just fine, and has
// nil params passed to its evaluator.
//
// Also shows that if binding has specified params in this instance then they
// are silently ignored.
// 策略没有 paramKind 时，绑定上的 paramRef 不生效；这是有意的宽松处理，
// 避免策略作者删除 paramKind 后旧绑定立刻变成配置错误。
func TestEmptyParamSource(t *testing.T) {
	compiler := &fakeCompiler{}
	matcher := &fakeMatcher{
		DefaultMatch: true,
	}

	testContext := setupFakeTest(t, compiler, matcher)

	datalock := sync.Mutex{}
	numCompiles := 0

	// Push some fake
	noParamSourcePolicy := *denyPolicy
	noParamSourcePolicy.Spec.ParamKind = nil

	compiler.RegisterDefinition(denyPolicy, func(ctx context.Context, matchedResource schema.GroupVersionResource, versionedAttr *admission.VersionedAttributes, versionedParams runtime.Object, namespace *v1.Namespace, runtimeCELCostBudget int64, authz authorizer.UnconditionalAuthorizer) validating.ValidateResult {
		datalock.Lock()
		numCompiles += 1
		datalock.Unlock()
		return validating.ValidateResult{
			Decisions: []validating.PolicyDecision{
				{
					Action:  validating.ActionDeny,
					Message: "Denied",
				},
			},
		}
	})

	require.NoError(t, testContext.UpdateAndWait(&noParamSourcePolicy, denyBindingWithNoParamRef))

	err := testContext.Plugin.Dispatch(
		testContext,
		// Object is irrelevant/unchecked for this test. Just test that
		// the evaluator is executed, and returns a denial
		attributeRecord(nil, fakeParams, admission.Create),
		&admission.RuntimeObjectInterfaces{},
	)

	require.ErrorContains(t, err, `Denied`)
	require.Equal(t, 1, numCompiles)
}

// Shows what happens when multiple policies share one param type, then
// one policy stops using the param. The expectation is the second policy
// keeps behaving normally
// 关键回归点：共享同一参数类型的策略不应因另一条策略修改 paramKind 而丢失参数，
// 即参数 informer 的生命周期必须按“引用计数”而非“单一策略”管理。
func TestMultiplePoliciesSharedParamType(t *testing.T) {
	compiler := &fakeCompiler{}
	matcher := &fakeMatcher{
		DefaultMatch: true,
	}

	testContext := setupFakeTest(t, compiler, matcher)

	// Use ConfigMap native-typed param
	policy1 := *denyPolicy
	policy1.Name = "denypolicy1.example.com"
	policy1.Spec = admissionregistrationv1.ValidatingAdmissionPolicySpec{
		ParamKind: &admissionregistrationv1.ParamKind{
			APIVersion: paramsGVK.GroupVersion().String(),
			Kind:       paramsGVK.Kind,
		},
		FailurePolicy: ptr.To(admissionregistrationv1.Fail),
		Validations: []admissionregistrationv1.Validation{
			{
				Expression: "policy1",
			},
		},
	}

	policy2 := *denyPolicy
	policy2.Name = "denypolicy2.example.com"
	policy2.Spec = admissionregistrationv1.ValidatingAdmissionPolicySpec{
		ParamKind: &admissionregistrationv1.ParamKind{
			APIVersion: paramsGVK.GroupVersion().String(),
			Kind:       paramsGVK.Kind,
		},
		FailurePolicy: ptr.To(admissionregistrationv1.Fail),
		Validations: []admissionregistrationv1.Validation{
			{
				Expression: "policy2",
			},
		},
	}

	binding1 := *denyBinding
	binding2 := *denyBinding

	binding1.Name = "denybinding1.example.com"
	binding1.Spec.PolicyName = policy1.Name
	binding2.Name = "denybinding2.example.com"
	binding2.Spec.PolicyName = policy2.Name

	evaluations1 := atomic.Int64{}
	evaluations2 := atomic.Int64{}

	compiler.RegisterDefinition(&policy1, func(ctx context.Context, matchedResource schema.GroupVersionResource, versionedAttr *admission.VersionedAttributes, versionedParams runtime.Object, namespace *v1.Namespace, runtimeCELCostBudget int64, authz authorizer.UnconditionalAuthorizer) validating.ValidateResult {
		evaluations1.Add(1)

		return validating.ValidateResult{
			Decisions: []validating.PolicyDecision{
				{
					Action: validating.ActionAdmit,
				},
			},
		}
	})

	compiler.RegisterDefinition(&policy2, func(ctx context.Context, matchedResource schema.GroupVersionResource, versionedAttr *admission.VersionedAttributes, versionedParams runtime.Object, namespace *v1.Namespace, runtimeCELCostBudget int64, authz authorizer.UnconditionalAuthorizer) validating.ValidateResult {
		evaluations2.Add(1)
		return validating.ValidateResult{
			Decisions: []validating.PolicyDecision{
				{
					Action:  validating.ActionDeny,
					Message: "Policy2Denied",
				},
			},
		}
	})

	require.NoError(t, testContext.UpdateAndWait(fakeParams, &policy1, &binding1))

	// Make sure policy 1 is created and bound to the params type first
	require.NoError(t, testContext.UpdateAndWait(&policy2, &binding2))

	err := testContext.Plugin.Dispatch(
		testContext,
		// Object is irrelevant/unchecked for this test. Just test that
		// the evaluator is executed, and returns admit meaning the params
		// passed was a configmap
		attributeRecord(nil, fakeParams, admission.Create),
		&admission.RuntimeObjectInterfaces{},
	)

	require.ErrorContains(t, err, `Denied`)
	require.EqualValues(t, 1, compiler.getNumCompiles(&policy1))
	require.EqualValues(t, 1, evaluations1.Load())
	require.EqualValues(t, 1, compiler.getNumCompiles(&policy2))
	require.EqualValues(t, 1, evaluations2.Load())

	// Remove param type from policy1
	// Show that policy2 evaluator is still being passed the configmaps
	policy1.Spec.ParamKind = nil
	policy1.ResourceVersion = "2"

	binding1.Spec.ParamRef = nil
	binding1.ResourceVersion = "2"

	require.NoError(t, testContext.UpdateAndWait(&policy1, &binding1))

	err = testContext.Plugin.Dispatch(
		testContext,
		// Object is irrelevant/unchecked for this test. Just test that
		// the evaluator is executed, and returns admit meaning the params
		// passed was a configmap
		attributeRecord(nil, fakeParams, admission.Create),
		&admission.RuntimeObjectInterfaces{},
	)

	require.ErrorContains(t, err, `Policy2Denied`)
	require.EqualValues(t, 2, compiler.getNumCompiles(&policy1))
	require.EqualValues(t, 2, evaluations1.Load())
	require.EqualValues(t, 1, compiler.getNumCompiles(&policy2))
	require.EqualValues(t, 2, evaluations2.Load())
}

// Shows that we can refer to native-typed params just fine
// (as opposed to CRD params)
// 原生类型参数（ConfigMap）走的是 typed client 路径，与 CRD 的 unstructured 路径不同，
// 这里断言求值器收到的是 *v1.ConfigMap 而非 unstructured。
func TestNativeTypeParam(t *testing.T) {
	compiler := &fakeCompiler{}
	matcher := &fakeMatcher{
		DefaultMatch: true,
	}
	testContext := setupFakeTest(t, compiler, matcher)
	evaluations := atomic.Int64{}

	// Use ConfigMap native-typed param
	nativeTypeParamPolicy := *denyPolicy
	nativeTypeParamPolicy.Spec.ParamKind = &admissionregistrationv1.ParamKind{
		APIVersion: "v1",
		Kind:       "ConfigMap",
	}

	compiler.RegisterDefinition(&nativeTypeParamPolicy, func(ctx context.Context, matchedResource schema.GroupVersionResource, versionedAttr *admission.VersionedAttributes, versionedParams runtime.Object, namespace *v1.Namespace, runtimeCELCostBudget int64, authz authorizer.UnconditionalAuthorizer) validating.ValidateResult {
		evaluations.Add(1)
		if _, ok := versionedParams.(*v1.ConfigMap); ok {
			return validating.ValidateResult{
				Decisions: []validating.PolicyDecision{
					{
						Action:  validating.ActionDeny,
						Message: "correct type",
					},
				},
			}
		}
		return validating.ValidateResult{
			Decisions: []validating.PolicyDecision{
				{
					Action:  validating.ActionDeny,
					Message: "Incorrect param type",
				},
			},
		}
	})

	configMapParam := &v1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ConfigMap",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "replicas-test.example.com",
			Namespace:       "default",
			ResourceVersion: "1",
		},
		Data: map[string]string{
			"coolkey": "coolvalue",
		},
	}
	require.NoError(t, testContext.UpdateAndWait(&nativeTypeParamPolicy, denyBinding, configMapParam))

	err := testContext.Plugin.Dispatch(
		testContext,
		// Object is irrelevant/unchecked for this test. Just test that
		// the evaluator is executed, and returns admit meaning the params
		// passed was a configmap
		attributeRecord(nil, fakeParams, admission.Create),
		&admission.RuntimeObjectInterfaces{},
	)

	require.ErrorContains(t, err, "correct type")
	require.EqualValues(t, 1, compiler.getNumCompiles(&nativeTypeParamPolicy))
	require.EqualValues(t, 1, evaluations.Load())
}

// TestAuditValidationAction 验证 Audit 动作：多个绑定的失败结果应被合并成一条审计注解，
// 请求本身不被拒绝，也不产生告警。
func TestAuditValidationAction(t *testing.T) {
	compiler := &fakeCompiler{}
	matcher := &fakeMatcher{
		DefaultMatch: true,
	}
	testContext := setupFakeTest(t, compiler, matcher)

	policy1 := *denyPolicy
	policy1.Name = "denypolicy1.example.com"
	policy1.Spec.ParamKind = nil
	binding1 := *denyBindingWithAudit
	binding1.Name = "denybinding1.example.com"
	binding1.Spec.PolicyName = policy1.Name

	policy2 := *denyPolicy
	policy2.Name = "denypolicy2.example.com"
	policy2.Spec.ParamKind = nil
	binding2 := *denyBindingWithAudit
	binding2.Name = "denybinding2.example.com"
	binding2.Spec.PolicyName = policy2.Name

	compiler.RegisterDefinition(&policy1, func(ctx context.Context, matchedResource schema.GroupVersionResource, versionedAttr *admission.VersionedAttributes, versionedParams runtime.Object, namespace *v1.Namespace, runtimeCELCostBudget int64, authz authorizer.UnconditionalAuthorizer) validating.ValidateResult {
		return validating.ValidateResult{
			Decisions: []validating.PolicyDecision{
				{
					Action:  validating.ActionDeny,
					Message: "I'm sorry Dave",
				},
			},
		}
	})
	compiler.RegisterDefinition(&policy2, func(ctx context.Context, matchedResource schema.GroupVersionResource, versionedAttr *admission.VersionedAttributes, versionedParams runtime.Object, namespace *v1.Namespace, runtimeCELCostBudget int64, authz authorizer.UnconditionalAuthorizer) validating.ValidateResult {
		return validating.ValidateResult{
			Decisions: []validating.PolicyDecision{
				{
					Action:  validating.ActionDeny,
					Message: "I can't do that",
				},
			},
		}
	})

	require.NoError(t, testContext.UpdateAndWait(&policy1, &policy2, &binding1, &binding2))

	attr := attributeRecord(nil, fakeParams, admission.Create)
	warningRecorder := newWarningRecorder()
	warnCtx := warning.WithWarningRecorder(testContext, warningRecorder)
	err := testContext.Plugin.Dispatch(
		warnCtx,
		attr,
		&admission.RuntimeObjectInterfaces{},
	)

	require.Equal(t, 0, warningRecorder.len())

	annotations := attr.GetAnnotations(auditinternal.LevelMetadata)
	require.Len(t, annotations, 1)
	valueJson, ok := annotations["validation.policy.admission.k8s.io/validation_failure"]
	require.True(t, ok)
	var value []validating.ValidationFailureValue
	jsonErr := utiljson.Unmarshal([]byte(valueJson), &value)
	require.NoError(t, jsonErr)
	expected := []validating.ValidationFailureValue{
		{
			ExpressionIndex:   0,
			Message:           "I'm sorry Dave",
			ValidationActions: []admissionregistrationv1.ValidationAction{admissionregistrationv1.Audit},
			Binding:           "denybinding1.example.com",
			Policy:            policy1.Name,
		},
		{
			ExpressionIndex:   0,
			Message:           "I can't do that",
			ValidationActions: []admissionregistrationv1.ValidationAction{admissionregistrationv1.Audit},
			Binding:           "denybinding2.example.com",
			Policy:            policy2.Name,
		},
	}
	require.ElementsMatch(t, expected, value)

	require.NoError(t, err)
}

// TestWarnValidationAction 验证 Warn 动作：失败信息以 HTTP Warning 头返回，
// 不写入审计注解，请求正常放行。
func TestWarnValidationAction(t *testing.T) {
	compiler := &fakeCompiler{}
	matcher := &fakeMatcher{
		DefaultMatch: true,
	}
	testContext := setupFakeTest(t, compiler, matcher)

	// Push some fake
	noParamSourcePolicy := *denyPolicy
	noParamSourcePolicy.Spec.ParamKind = nil

	compiler.RegisterDefinition(denyPolicy, func(ctx context.Context, matchedResource schema.GroupVersionResource, versionedAttr *admission.VersionedAttributes, versionedParams runtime.Object, namespace *v1.Namespace, runtimeCELCostBudget int64, authz authorizer.UnconditionalAuthorizer) validating.ValidateResult {
		return validating.ValidateResult{
			Decisions: []validating.PolicyDecision{
				{
					Action:  validating.ActionDeny,
					Message: "I'm sorry Dave",
				},
			},
		}
	})

	require.NoError(t, testContext.UpdateAndWait(&noParamSourcePolicy, denyBindingWithWarn))

	attr := attributeRecord(nil, fakeParams, admission.Create)
	warningRecorder := newWarningRecorder()
	warnCtx := warning.WithWarningRecorder(testContext, warningRecorder)
	err := testContext.Plugin.Dispatch(
		warnCtx,
		attr,
		&admission.RuntimeObjectInterfaces{},
	)

	require.Equal(t, 1, warningRecorder.len())
	require.True(t, warningRecorder.hasWarning("Validation failed for ValidatingAdmissionPolicy 'denypolicy.example.com' with binding 'denybinding.example.com': I'm sorry Dave"))

	annotations := attr.GetAnnotations(auditinternal.LevelMetadata)
	require.Empty(t, annotations)

	require.NoError(t, err)
}

// TestAllValidationActions 验证 Deny+Warn+Audit 同时配置时三种副作用都会发生：
// 请求被拒绝、产生告警、写入审计注解。
func TestAllValidationActions(t *testing.T) {
	compiler := &fakeCompiler{}
	matcher := &fakeMatcher{
		DefaultMatch: true,
	}
	testContext := setupFakeTest(t, compiler, matcher)

	// Push some fake
	noParamSourcePolicy := *denyPolicy
	noParamSourcePolicy.Spec.ParamKind = nil

	compiler.RegisterDefinition(denyPolicy, func(ctx context.Context, matchedResource schema.GroupVersionResource, versionedAttr *admission.VersionedAttributes, versionedParams runtime.Object, namespace *v1.Namespace, runtimeCELCostBudget int64, authz authorizer.UnconditionalAuthorizer) validating.ValidateResult {
		return validating.ValidateResult{
			Decisions: []validating.PolicyDecision{
				{
					Action:  validating.ActionDeny,
					Message: "I'm sorry Dave",
				},
			},
		}
	})

	require.NoError(t, testContext.UpdateAndWait(&noParamSourcePolicy, denyBindingWithAll))

	attr := attributeRecord(nil, fakeParams, admission.Create)
	warningRecorder := newWarningRecorder()
	warnCtx := warning.WithWarningRecorder(testContext, warningRecorder)
	err := testContext.Plugin.Dispatch(
		warnCtx,
		attr,
		&admission.RuntimeObjectInterfaces{},
	)

	require.Equal(t, 1, warningRecorder.len())
	require.True(t, warningRecorder.hasWarning("Validation failed for ValidatingAdmissionPolicy 'denypolicy.example.com' with binding 'denybinding.example.com': I'm sorry Dave"))

	annotations := attr.GetAnnotations(auditinternal.LevelMetadata)
	require.Len(t, annotations, 1)
	valueJson, ok := annotations["validation.policy.admission.k8s.io/validation_failure"]
	require.True(t, ok)
	var value []validating.ValidationFailureValue
	jsonErr := utiljson.Unmarshal([]byte(valueJson), &value)
	require.NoError(t, jsonErr)
	expected := []validating.ValidationFailureValue{{
		ExpressionIndex:   0,
		Message:           "I'm sorry Dave",
		ValidationActions: []admissionregistrationv1.ValidationAction{admissionregistrationv1.Deny, admissionregistrationv1.Warn, admissionregistrationv1.Audit},
		Binding:           "denybinding.example.com",
		Policy:            noParamSourcePolicy.Name,
	}}
	require.Equal(t, expected, value)

	require.ErrorContains(t, err, "I'm sorry Dave")
}

// TestNamespaceParamRefName 验证未显式指定命名空间时，paramRef 会随请求对象所属命名空间解析：
// 不同命名空间的同名 ConfigMap 应分别被选中，且策略只编译一次。
func TestNamespaceParamRefName(t *testing.T) {
	compiler := &fakeCompiler{}
	matcher := &fakeMatcher{
		DefaultMatch: true,
	}
	testContext := setupFakeTest(t, compiler, matcher)

	evaluations := atomic.Int64{}

	// Use ConfigMap native-typed param
	nativeTypeParamPolicy := *denyPolicy
	nativeTypeParamPolicy.Spec.ParamKind = &admissionregistrationv1.ParamKind{
		APIVersion: "v1",
		Kind:       "ConfigMap",
	}

	namespaceParamBinding := *denyBinding
	namespaceParamBinding.Spec.ParamRef = &admissionregistrationv1.ParamRef{
		Name: "replicas-test.example.com",
	}
	lock := sync.Mutex{}
	observedParamNamespaces := []string{}
	compiler.RegisterDefinition(&nativeTypeParamPolicy, func(ctx context.Context, matchedResource schema.GroupVersionResource, versionedAttr *admission.VersionedAttributes, versionedParams runtime.Object, namespace *v1.Namespace, runtimeCELCostBudget int64, authz authorizer.UnconditionalAuthorizer) validating.ValidateResult {
		lock.Lock()
		defer lock.Unlock()

		evaluations.Add(1)
		if p, ok := versionedParams.(*v1.ConfigMap); ok {
			observedParamNamespaces = append(observedParamNamespaces, p.Namespace)
			return validating.ValidateResult{
				Decisions: []validating.PolicyDecision{
					{
						Action:  validating.ActionDeny,
						Message: "correct type",
					},
				},
			}
		}
		return validating.ValidateResult{
			Decisions: []validating.PolicyDecision{
				{
					Action:  validating.ActionDeny,
					Message: "Incorrect param type",
				},
			},
		}
	})

	configMapParam := &v1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ConfigMap",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "replicas-test.example.com",
			Namespace:       "default",
			ResourceVersion: "1",
		},
		Data: map[string]string{
			"coolkey": "default",
		},
	}
	configMapParam2 := &v1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ConfigMap",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "replicas-test.example.com",
			Namespace:       "mynamespace",
			ResourceVersion: "1",
		},
		Data: map[string]string{
			"coolkey": "mynamespace",
		},
	}
	configMapParam3 := &v1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ConfigMap",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            "replicas-test.example.com",
			Namespace:       "othernamespace",
			ResourceVersion: "1",
		},
		Data: map[string]string{
			"coolkey": "othernamespace",
		},
	}
	require.NoError(t, testContext.UpdateAndWait(&nativeTypeParamPolicy, &namespaceParamBinding, configMapParam, configMapParam2, configMapParam3))

	// Object is irrelevant/unchecked for this test. Just test that
	// the evaluator is executed with correct namespace, and returns admit
	// meaning the params passed was a configmap
	err := testContext.Plugin.Dispatch(
		testContext,
		attributeRecord(nil, configMapParam, admission.Create),
		&admission.RuntimeObjectInterfaces{},
	)

	func() {
		lock.Lock()
		defer lock.Unlock()
		require.ErrorContains(t, err, "correct type")
		require.EqualValues(t, 1, compiler.getNumCompiles(&nativeTypeParamPolicy))
		require.EqualValues(t, 1, evaluations.Load())
	}()

	err = testContext.Plugin.Dispatch(
		testContext,
		attributeRecord(nil, configMapParam2, admission.Create),
		&admission.RuntimeObjectInterfaces{},
	)

	func() {
		lock.Lock()
		defer lock.Unlock()
		require.ErrorContains(t, err, "correct type")
		require.EqualValues(t, 1, compiler.getNumCompiles(&nativeTypeParamPolicy))
		require.EqualValues(t, 2, evaluations.Load())
	}()

	err = testContext.Plugin.Dispatch(
		testContext,
		attributeRecord(nil, configMapParam3, admission.Create),
		&admission.RuntimeObjectInterfaces{},
	)

	func() {
		lock.Lock()
		defer lock.Unlock()
		require.ErrorContains(t, err, "correct type")
		require.EqualValues(t, 1, compiler.getNumCompiles(&nativeTypeParamPolicy))
		require.EqualValues(t, 3, evaluations.Load())
	}()

	err = testContext.Plugin.Dispatch(
		testContext,
		attributeRecord(nil, configMapParam, admission.Create),
		&admission.RuntimeObjectInterfaces{},
	)

	func() {
		lock.Lock()
		defer lock.Unlock()
		require.ErrorContains(t, err, "correct type")
		require.EqualValues(t, []string{"default", "mynamespace", "othernamespace", "default"}, observedParamNamespaces)
		require.EqualValues(t, 1, compiler.getNumCompiles(&nativeTypeParamPolicy))
		require.EqualValues(t, 4, evaluations.Load())
	}()
}

// TestParamRef 遍历 ParamRef 配置空间：集群级/命名空间级参数、name/selector 二选一、
// 是否指定命名空间、ParameterNotFoundAction 两种取值。跳过语义上非法的组合
// （集群级参数 + namespace、同时设置 name 与 selector）。
func TestParamRef(t *testing.T) {
	for _, paramIsClusterScoped := range []bool{false, true} {
		for _, nameIsSet := range []bool{false, true} {
			for _, namespaceIsSet := range []bool{false, true} {
				if paramIsClusterScoped && namespaceIsSet {
					// Skip invalid configuration
					continue
				}

				for _, selectorIsSet := range []bool{false, true} {
					if selectorIsSet && nameIsSet {
						// SKip invalid configuration
						continue
					}

					for _, denyNotFound := range []bool{false, true} {

						name := "ParamRef"

						if paramIsClusterScoped {
							name = "ClusterScoped" + name
						}

						if nameIsSet {
							name = name + "WithName"
						} else if selectorIsSet {
							name = name + "WithLabelSelector"
						} else {
							name = name + "WithEverythingSelector"
						}

						if namespaceIsSet {
							name = name + "WithNamespace"
						}

						if denyNotFound {
							name = name + "DenyNotFound"
						} else {
							name = name + "AllowNotFound"
						}

						t.Run(name, func(t *testing.T) {
							// Test creating a policy with a cluster or namesapce-scoped param
							// and binding with the provided configuration. Test will ensure
							// that the provided configuration is capable of matching
							// params as expected, and not matching params when not expected.
							// Also ensures the NotFound setting works as expected with this particular
							// configuration of ParamRef when all the previously
							// matched params are deleted.
							testParamRefCase(t, paramIsClusterScoped, nameIsSet, namespaceIsSet, selectorIsSet, denyNotFound)
						})
					}
				}
			}
		}
	}
}

// testParamRefCase constructs a ParamRef and policy with appropriate ParamKind
// for the given parameters, then constructs a scenario with several matching/non-matching params
// of varying names, namespaces, labels.
//
// Test then selects subset of params that should match provided configuration
// and ensuers those params are the only ones used.
//
// Also ensures NotFound action is enforced correctly by deleting all found
// params and ensuring the Action is used.
//
// This test is not meant to test every possible scenario of matching/not matching:
// only that each ParamRef CAN be evaluated correctly for both cluster scoped
// and namespace-scoped request kinds, and that the failure action is correctly
// applied.
// 命名空间级参数且 paramRef 未指定命名空间时，命名空间级请求可解析，
// 但集群级请求无法确定命名空间，属于配置错误。
func testParamRefCase(t *testing.T, paramIsClusterScoped, nameIsSet, namespaceIsSet, selectorIsSet, denyNotFound bool) {
	// Create a cluster scoped and a namespace scoped CRD
	policy := *denyPolicy
	binding := *denyBinding
	binding.Spec.ParamRef = &admissionregistrationv1.ParamRef{}
	paramRef := binding.Spec.ParamRef

	shouldErrorOnClusterScopedRequests := !namespaceIsSet && !paramIsClusterScoped

	matchingParamName := "replicas-test.example.com"
	matchingNamespace := "mynamespace"
	nonMatchingNamespace := "othernamespace"

	matchingLabels := labels.Set{"doesitmatch": "yes"}
	nonmatchingLabels := labels.Set{"doesitmatch": "no"}
	otherNonmatchingLabels := labels.Set{"notaffiliated": "no"}

	if paramIsClusterScoped {
		policy.Spec.ParamKind = &admissionregistrationv1.ParamKind{
			APIVersion: clusterScopedParamsGVK.GroupVersion().String(),
			Kind:       clusterScopedParamsGVK.Kind,
		}
	} else {
		policy.Spec.ParamKind = &admissionregistrationv1.ParamKind{
			APIVersion: paramsGVK.GroupVersion().String(),
			Kind:       paramsGVK.Kind,
		}
	}

	if nameIsSet {
		paramRef.Name = matchingParamName
	} else if selectorIsSet {
		paramRef.Selector = metav1.SetAsLabelSelector(matchingLabels)
	} else {
		paramRef.Selector = &metav1.LabelSelector{}
	}

	if namespaceIsSet {
		paramRef.Namespace = matchingNamespace
	}

	if denyNotFound {
		paramRef.ParameterNotFoundAction = ptr.To(admissionregistrationv1.DenyAction)
	} else {
		paramRef.ParameterNotFoundAction = ptr.To(admissionregistrationv1.AllowAction)
	}

	compiler := &fakeCompiler{}
	matcher := &fakeMatcher{
		DefaultMatch: true,
	}

	var matchedParams []runtime.Object
	paramLock := sync.Mutex{}
	observeParam := func(p runtime.Object) {
		paramLock.Lock()
		defer paramLock.Unlock()
		matchedParams = append(matchedParams, p)
	}
	getAndResetObservedParams := func() []runtime.Object {
		paramLock.Lock()
		defer paramLock.Unlock()
		oldParams := matchedParams
		matchedParams = nil
		return oldParams
	}

	compiler.RegisterDefinition(&policy, func(ctx context.Context, matchedResource schema.GroupVersionResource, versionedAttr *admission.VersionedAttributes, versionedParams runtime.Object, namespace *v1.Namespace, runtimeCELCostBudget int64, authz authorizer.UnconditionalAuthorizer) validating.ValidateResult {
		observeParam(versionedParams)
		return validating.ValidateResult{
			Decisions: []validating.PolicyDecision{
				{
					Action:  validating.ActionDeny,
					Message: "Denied by policy",
				},
			},
		}
	})

	testContext := setupFakeTest(t, compiler, matcher)

	// Create library of params to try to fool the controller
	// 参数矩阵覆盖：匹配/不匹配名称、匹配/不匹配命名空间、匹配/不匹配标签，
	// 以及命名空间级与集群级两类对象，用来确认 ParamRef 只选中应选中的那些。
	params := []*unstructured.Unstructured{
		newParam(matchingParamName, v1.NamespaceDefault, nonmatchingLabels),
		newParam(matchingParamName, matchingNamespace, nonmatchingLabels),
		newParam(matchingParamName, nonMatchingNamespace, nonmatchingLabels),

		newParam(matchingParamName+"1", v1.NamespaceDefault, matchingLabels),
		newParam(matchingParamName+"1", matchingNamespace, matchingLabels),
		newParam(matchingParamName+"1", nonMatchingNamespace, matchingLabels),

		newParam(matchingParamName+"2", v1.NamespaceDefault, otherNonmatchingLabels),
		newParam(matchingParamName+"2", matchingNamespace, otherNonmatchingLabels),
		newParam(matchingParamName+"2", nonMatchingNamespace, otherNonmatchingLabels),

		newParam(matchingParamName+"3", v1.NamespaceDefault, otherNonmatchingLabels),
		newParam(matchingParamName+"3", matchingNamespace, matchingLabels),
		newParam(matchingParamName+"3", nonMatchingNamespace, matchingLabels),

		newClusterScopedParam(matchingParamName, matchingLabels),
		newClusterScopedParam(matchingParamName+"1", nonmatchingLabels),
		newClusterScopedParam(matchingParamName+"2", otherNonmatchingLabels),
		newClusterScopedParam(matchingParamName+"3", matchingLabels),
		newClusterScopedParam(matchingParamName+"4", nonmatchingLabels),
		newClusterScopedParam(matchingParamName+"5", otherNonmatchingLabels),
	}

	for _, p := range params {
		// Don't wait for these sync the informers would not have been
		// created unless bound to a policy
		require.NoError(t, testContext.Update(p))
	}

	require.NoError(t, testContext.UpdateAndWait(&policy, &binding))

	namespacedRequestObject := newParam("some param", nonMatchingNamespace, nil)
	clusterScopedRequestObject := newClusterScopedParam("other param", nil)

	// Validate a namespaced object, and verify that the params being validated
	// are the ones we would expect
	// 期望集合由测试自身独立推导（与插件实现无关），再与插件实际传给求值器的参数比较，
	// 这样即使两边共享同一错误逻辑也能暴露问题。
	timeoutCtx, timeoutCancel := context.WithTimeout(testContext, 5*time.Second)
	defer timeoutCancel()
	var expectedParamsForNamespacedRequest []*unstructured.Unstructured
	for _, p := range params {
		if p.GetAPIVersion() != policy.Spec.ParamKind.APIVersion || p.GetKind() != policy.Spec.ParamKind.Kind {
			continue
		} else if len(paramRef.Name) > 0 && p.GetName() != paramRef.Name {
			continue
		} else if len(paramRef.Namespace) > 0 && p.GetNamespace() != paramRef.Namespace {
			continue
		}

		if !paramIsClusterScoped {
			// If the paramRef has empty namespace and the kind is
			// namespaced-scoped, then it only matches params of the same
			// namespace
			if len(paramRef.Namespace) == 0 && p.GetNamespace() != namespacedRequestObject.GetNamespace() {
				continue
			}
		}

		if paramRef.Selector != nil {
			ls := p.GetLabels()
			matched := true

			for k, v := range paramRef.Selector.MatchLabels {
				if l, hasLabel := ls[k]; !hasLabel {
					matched = false
					break
				} else if l != v {
					matched = false
					break
				}
			}

			// Empty selector matches everything
			if len(paramRef.Selector.MatchExpressions) == 0 && len(paramRef.Selector.MatchLabels) == 0 {
				matched = true
			}

			if !matched {
				continue
			}
		}

		expectedParamsForNamespacedRequest = append(expectedParamsForNamespacedRequest, p)
		require.NoError(t, testContext.WaitForReconcile(timeoutCtx, p))
	}
	require.NotEmpty(t, expectedParamsForNamespacedRequest, "all test cases should match at least one param")
	require.ErrorContains(t, testContext.Plugin.Dispatch(context.TODO(), attributeRecord(nil, namespacedRequestObject, admission.Create), &admission.RuntimeObjectInterfaces{}), "Denied by policy")
	require.ElementsMatch(t, expectedParamsForNamespacedRequest, getAndResetObservedParams(), "should exactly match expected params")

	// Validate a cluster-scoped object, and verify that the params being validated
	// are the ones we would expect
	var expectedParamsForClusterScopedRequest []*unstructured.Unstructured
	timeoutCtx, timeoutCancel = context.WithTimeout(testContext, 5*time.Second)
	defer timeoutCancel()
	for _, p := range params {
		if shouldErrorOnClusterScopedRequests {
			continue
		} else if p.GetAPIVersion() != policy.Spec.ParamKind.APIVersion || p.GetKind() != policy.Spec.ParamKind.Kind {
			continue
		} else if len(paramRef.Name) > 0 && p.GetName() != paramRef.Name {
			continue
		} else if len(paramRef.Namespace) > 0 && p.GetNamespace() != paramRef.Namespace {
			continue
		} else if !paramIsClusterScoped && len(paramRef.Namespace) == 0 && p.GetNamespace() != v1.NamespaceDefault {
			continue
		}

		if paramRef.Selector != nil {
			ls := p.GetLabels()
			matched := true
			for k, v := range paramRef.Selector.MatchLabels {
				if l, hasLabel := ls[k]; !hasLabel {
					matched = false
					break
				} else if l != v {
					matched = false
					break
				}
			}

			// Empty selector matches everything
			if len(paramRef.Selector.MatchExpressions) == 0 && len(paramRef.Selector.MatchLabels) == 0 {
				matched = true
			}

			if !matched {
				continue
			}
		}

		expectedParamsForClusterScopedRequest = append(expectedParamsForClusterScopedRequest, p)
		require.NoError(t, testContext.WaitForReconcile(timeoutCtx, p))

	}

	err := testContext.Plugin.Dispatch(context.TODO(), attributeRecord(nil, clusterScopedRequestObject, admission.Create), &admission.RuntimeObjectInterfaces{})
	if shouldErrorOnClusterScopedRequests {
		// Cannot validate cliuster-scoped resources against a paramRef that sets namespace
		require.ErrorContains(t, err, "failed to configure binding: cannot use namespaced paramRef in policy binding that matches cluster-scoped resources")
	} else {
		require.NotEmpty(t, expectedParamsForClusterScopedRequest, "all test cases should match at least one param")
		require.ErrorContains(t, err, "Denied by policy")
	}
	require.ElementsMatch(t, expectedParamsForClusterScopedRequest, getAndResetObservedParams(), "should exactly match expected params")

	// Remove all params matched by namespaced, and cluster-scoped validation.
	// Validate again to make sure NotFoundAction is respected
	// 删除全部匹配参数后：Deny 应返回错误，Allow 应静默跳过且不调用求值器。
	var deleted []runtime.Object
	for _, p := range expectedParamsForNamespacedRequest {
		deleted = append(deleted, p)
	}

	for _, p := range expectedParamsForClusterScopedRequest {
		deleted = append(deleted, p)
	}

	require.NoError(t, testContext.DeleteAndWait(deleted...))

	// Check that NotFound is working correctly for both namespaeed & non-namespaced
	// request object
	err = testContext.Plugin.Dispatch(context.TODO(), attributeRecord(nil, namespacedRequestObject, admission.Create), &admission.RuntimeObjectInterfaces{})
	if denyNotFound {
		require.ErrorContains(t, err, "no params found for policy binding with `Deny` parameterNotFoundAction")
	} else {
		require.NoError(t, err, "Allow not found expects no error when no params found. Policy should have been skipped")
	}
	require.Empty(t, getAndResetObservedParams(), "policy should not have been evaluated")

	err = testContext.Plugin.Dispatch(context.TODO(), attributeRecord(nil, clusterScopedRequestObject, admission.Create), &admission.RuntimeObjectInterfaces{})
	if shouldErrorOnClusterScopedRequests {
		require.ErrorContains(t, err, "failed to configure binding: cannot use namespaced paramRef in policy binding that matches cluster-scoped resources")

	} else if denyNotFound {
		require.ErrorContains(t, err, "no params found for policy binding with `Deny` parameterNotFoundAction")
	} else {
		require.NoError(t, err, "Allow not found expects no error when no params found. Policy should have been skipped")
	}
	require.Empty(t, getAndResetObservedParams(), "policy should not have been evaluated")
}

// If the ParamKind is ClusterScoped, and namespace param is used.
// This is a Configuration Error of the policy
// 配置错误应在分发阶段被拒绝，且不进入求值（evaluations 保持 0），
// 以免用错误配置静默放行或拒绝流量。
func TestNamespaceParamRefClusterScopedParamError(t *testing.T) {
	compiler := &fakeCompiler{}
	matcher := &fakeMatcher{
		DefaultMatch: true,
	}
	testContext := setupFakeTest(t, compiler, matcher)

	evaluations := atomic.Int64{}

	// Use ValidatingAdmissionPolicy for param type since it is cluster-scoped
	nativeTypeParamPolicy := *denyPolicy
	nativeTypeParamPolicy.Spec.ParamKind = &admissionregistrationv1.ParamKind{
		APIVersion: "admissionregistration.k8s.io/v1beta1",
		Kind:       "ValidatingAdmissionPolicy",
	}

	namespaceParamBinding := *denyBinding
	namespaceParamBinding.Spec.ParamRef = &admissionregistrationv1.ParamRef{
		Name:      "other-param-to-use-with-no-label.example.com",
		Namespace: "mynamespace",
	}

	compiler.RegisterDefinition(&nativeTypeParamPolicy, func(ctx context.Context, matchedResource schema.GroupVersionResource, versionedAttr *admission.VersionedAttributes, versionedParams runtime.Object, namespace *v1.Namespace, runtimeCELCostBudget int64, authz authorizer.UnconditionalAuthorizer) validating.ValidateResult {
		evaluations.Add(1)
		if _, ok := versionedParams.(*admissionregistrationv1.ValidatingAdmissionPolicy); ok {
			return validating.ValidateResult{
				Decisions: []validating.PolicyDecision{
					{
						Action:  validating.ActionAdmit,
						Message: "correct type",
					},
				},
			}
		}
		return validating.ValidateResult{
			Decisions: []validating.PolicyDecision{
				{
					Action:  validating.ActionDeny,
					Message: fmt.Sprintf("Incorrect param type %T", versionedParams),
				},
			},
		}
	})

	require.NoError(t, testContext.UpdateAndWait(&nativeTypeParamPolicy, &namespaceParamBinding))

	// Object is irrelevant/unchecked for this test. Just test that
	// the evaluator is executed with correct namespace, and returns admit
	// meaning the params passed was a configmap
	err := testContext.Plugin.Dispatch(
		testContext,
		attributeRecord(nil, fakeParams, admission.Create),
		&admission.RuntimeObjectInterfaces{},
	)

	require.ErrorContains(t, err, "paramRef.namespace must not be provided for a cluster-scoped `paramKind`")
	require.EqualValues(t, 1, compiler.getNumCompiles(&nativeTypeParamPolicy))
	require.EqualValues(t, 0, evaluations.Load())
}

// TestAuditAnnotations 验证策略产生的审计注解：Publish 动作写入注解、
// Exclude 动作不写入、Error 动作转为错误；多个绑定的值以逗号合并。
func TestAuditAnnotations(t *testing.T) {
	compiler := &fakeCompiler{}
	matcher := &fakeMatcher{
		DefaultMatch: true,
	}
	testContext := setupFakeTest(t, compiler, matcher)

	// Push some fake
	policy := *denyPolicy
	compiler.RegisterDefinition(denyPolicy, func(ctx context.Context, matchedResource schema.GroupVersionResource, versionedAttr *admission.VersionedAttributes, versionedParams runtime.Object, namespace *v1.Namespace, runtimeCELCostBudget int64, authz authorizer.UnconditionalAuthorizer) validating.ValidateResult {
		o, err := meta.Accessor(versionedParams)
		if err != nil {
			t.Fatal(err)
		}
		exampleValue := "normal-value"
		if o.GetName() == "replicas-test2.example.com" {
			exampleValue = "special-value"
		}
		return validating.ValidateResult{
			AuditAnnotations: []validating.PolicyAuditAnnotation{
				{
					Key:    "example-key",
					Value:  exampleValue,
					Action: validating.AuditAnnotationActionPublish,
				},
				{
					Key:    "excluded-key",
					Value:  "excluded-value",
					Action: validating.AuditAnnotationActionExclude,
				},
				{
					Key:    "error-key",
					Action: validating.AuditAnnotationActionError,
					Error:  "example error",
				},
			},
		}
	})

	fakeParams2 := fakeParams.DeepCopy()
	fakeParams2.SetName("replicas-test2.example.com")
	denyBinding2 := denyBinding.DeepCopy()
	denyBinding2.SetName("denybinding2.example.com")
	denyBinding2.Spec.ParamRef.Name = fakeParams2.GetName()

	fakeParams3 := fakeParams.DeepCopy()
	fakeParams3.SetName("replicas-test3.example.com")
	denyBinding3 := denyBinding.DeepCopy()
	denyBinding3.SetName("denybinding3.example.com")
	denyBinding3.Spec.ParamRef.Name = fakeParams3.GetName()

	require.NoError(t, testContext.UpdateAndWait(fakeParams, fakeParams2, fakeParams3, &policy, denyBinding, denyBinding2, denyBinding3))

	attr := attributeRecord(nil, fakeParams, admission.Create)
	err := testContext.Plugin.Dispatch(
		testContext,
		attr,
		&admission.RuntimeObjectInterfaces{},
	)

	annotations := attr.GetAnnotations(auditinternal.LevelMetadata)
	require.Len(t, annotations, 1)
	value := annotations[policy.Name+"/example-key"]
	parts := strings.Split(value, ", ")
	require.Len(t, parts, 2)
	require.Contains(t, parts, "normal-value", "special-value")

	require.ErrorContains(t, err, "example error")
}

// FakeAttributes decorates admission.Attributes. It's used to trace the added annotations.
// 内嵌 Attributes 以复用全部默认行为，仅覆盖注解相关方法；互斥锁保护 map，
// 因为分发过程中可能有多个校验器并发写入。
type FakeAttributes struct {
	admission.Attributes
	annotations map[string]string
	mutex       sync.Mutex
}

// AddAnnotation adds an annotation key value pair to FakeAttributes
func (f *FakeAttributes) AddAnnotation(k, v string) error {
	return f.AddAnnotationWithLevel(k, v, auditinternal.LevelMetadata)
}

// AddAnnotationWithLevel 先写入底层 Attributes（以便触发其去重/校验逻辑），
// 再记录到本地 map 供测试断言；底层报错时不同步本地副本，保持两者一致。
func (f *FakeAttributes) AddAnnotationWithLevel(k, v string, _ auditinternal.Level) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	if err := f.Attributes.AddAnnotation(k, v); err != nil {
		return err
	}
	if f.annotations == nil {
		f.annotations = make(map[string]string)
	}
	f.annotations[k] = v
	return nil
}

// GetAnnotations reads annotations from FakeAttributes
// 返回副本而非内部 map，避免调用方在锁外修改导致数据竞争。
func (f *FakeAttributes) GetAnnotations(_ auditinternal.Level) map[string]string {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	annotations := make(map[string]string, len(f.annotations))
	for k, v := range f.annotations {
		annotations[k] = v
	}
	return annotations
}

// warningRecorder 实现 warning.WarningRecorder，按集合记录告警文本，
// 重复告警只计一次，便于断言“是否产生过某条告警”而不受次数影响。
type warningRecorder struct {
	sync.Mutex
	warnings sets.Set[string]
}

// newWarningRecorder 创建一个空的告警记录器。
func newWarningRecorder() *warningRecorder {
	return &warningRecorder{warnings: sets.New[string]()}
}

// AddWarning 记录告警文本，忽略告警来源字段。
func (r *warningRecorder) AddWarning(_, text string) {
	r.Lock()
	defer r.Unlock()
	r.warnings.Insert(text)
}

// hasWarning 判断是否记录过指定文本的告警。
func (r *warningRecorder) hasWarning(text string) bool {
	r.Lock()
	defer r.Unlock()
	return r.warnings.Has(text)
}

// len 返回去重后的告警条数。
func (r *warningRecorder) len() int {
	r.Lock()
	defer r.Unlock()
	return len(r.warnings)
}
