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

package generic_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "k8s.io/api/admissionregistration/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/admission/plugin/policy/generic"
	"k8s.io/apiserver/pkg/admission/plugin/policy/matching"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// fakeDispatcher 是测试用的空 Dispatcher：本文件的测试只关心 PolicySource 的
// 缓存与绑定行为，不关心请求分发。
type fakeDispatcher struct{}

// Dispatch 始终放行。
func (fd *fakeDispatcher) Dispatch(context.Context, admission.Attributes, admission.ObjectInterfaces, []generic.PolicyHook[*FakePolicy, *FakeBinding, generic.Evaluator]) error {
	return nil
}

// Start 无后台工作。
func (fd *fakeDispatcher) Start(context.Context) error {
	return nil
}

// makeTestDispatcher 作为工厂注入 PolicyTestContext。
func makeTestDispatcher(authorizer.UnconditionalAuthorizer, *matching.Matcher, kubernetes.Interface) generic.Dispatcher[generic.PolicyHook[*FakePolicy, *FakeBinding, generic.Evaluator]] {
	return &fakeDispatcher{}
}

// TestPolicySourceHasSyncedEmpty 验证在没有初始对象时，informer 启动后
// HasSynced 能在上下文取消前完成同步。
func TestPolicySourceHasSyncedEmpty(t *testing.T) {
	testContext, testCancel, err := generic.NewPolicyTestContext(
		t,
		func(fp *FakePolicy) generic.PolicyAccessor { return fp },
		func(fb *FakeBinding) generic.BindingAccessor { return fb },
		func(fp *FakePolicy) generic.Evaluator { return nil },
		makeTestDispatcher,
		nil,
		nil,
	)
	require.NoError(t, err)
	defer testCancel()
	require.NoError(t, testContext.Start())

	// Should be able to wait for cache sync
	require.True(t, cache.WaitForCacheSync(testContext.Done(), testContext.Source.HasSynced), "cache should sync after informer running")
}

// TestPolicySourceHasSyncedInitialList 验证从 informer 初始列表构造 Hook，
// 并验证后续增量更新（新增 policy+binding、更新已有 policy）会反映到 Hooks 中。
func TestPolicySourceHasSyncedInitialList(t *testing.T) {
	// Create a list of fake policies
	initialObjects := []runtime.Object{
		&FakePolicy{
			ObjectMeta: metav1.ObjectMeta{
				Name: "policy1",
			},
		},
		&FakeBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name: "binding1",
			},
			PolicyName: "policy1",
		},
	}

	testContext, testCancel, err := generic.NewPolicyTestContext(
		t,
		func(fp *FakePolicy) generic.PolicyAccessor { return fp },
		func(fb *FakeBinding) generic.BindingAccessor { return fb },
		func(fp *FakePolicy) generic.Evaluator { return nil },
		makeTestDispatcher,
		initialObjects,
		nil,
	)
	require.NoError(t, err)
	defer testCancel()
	require.NoError(t, testContext.Start())
	// 应能等待缓存同步；初始列表中的 policy1 已经有一条 binding，应形成一个 Hook。
	require.Len(t, testContext.Source.Hooks(), 1, "should have one policy")
	require.NoError(t, testContext.UpdateAndWait(
		&FakePolicy{
			ObjectMeta: metav1.ObjectMeta{
				Name: "policy2",
			},
		},
		&FakeBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name: "binding2",
			},
			PolicyName: "policy2",
		},
	))
	require.Len(t, testContext.Source.Hooks(), 2, "should have two policies")
	// 同一批次中同时新增 policy3 并更新 policy2（为其加上 ParamKind），
	// 验证 Hook 数量按 policy 去重计数，而不是按事件计数。
	require.NoError(t, testContext.UpdateAndWait(
		&FakePolicy{
			ObjectMeta: metav1.ObjectMeta{
				Name: "policy3",
			},
		},
		&FakeBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name: "binding3",
			},
			PolicyName: "policy3",
		},
		&FakePolicy{
			ObjectMeta: metav1.ObjectMeta{
				Name: "policy2",
			},
			ParamKind: &v1.ParamKind{
				APIVersion: "policy.example.com/v1",
				Kind:       "FakeParam",
			},
		},
	))
	require.Len(t, testContext.Source.Hooks(), 3, "should have 3 policies")

}

// TestPolicySourceBindsToPolicies 验证 Binding 的归属：
// 无 binding 的 policy 不会出现在 Hooks 中；binding 改绑到另一个 policy 后，
// Hook 应随之迁移到新 policy，旧 policy 因失去 binding 而被移除。
func TestPolicySourceBindsToPolicies(t *testing.T) {
	// Create a list of fake policies
	initialObjects := []runtime.Object{
		&FakePolicy{
			ObjectMeta: metav1.ObjectMeta{
				Name: "policy1",
			},
		},
		&FakeBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name: "binding1",
			},
			PolicyName: "policy1",
		},
	}

	testContext, testCancel, err := generic.NewPolicyTestContext(
		t,
		func(fp *FakePolicy) generic.PolicyAccessor { return fp },
		func(fb *FakeBinding) generic.BindingAccessor { return fb },
		func(fp *FakePolicy) generic.Evaluator { return nil },
		makeTestDispatcher,
		initialObjects,
		nil,
	)
	require.NoError(t, err)
	defer testCancel()
	require.NoError(t, testContext.Start())

	require.Len(t, testContext.Source.Hooks(), 1, "should have one policy")
	require.Len(t, testContext.Source.Hooks()[0].Bindings, 1, "should have one binding")
	require.Equal(t, "binding1", testContext.Source.Hooks()[0].Bindings[0].GetName(), "should have one binding")

	// 把 binding 改绑到另一个 policy（没有 binding 的 policy 会被忽略，
	// 因此原来的 policy1 应被移除）。
	require.NoError(t, testContext.UpdateAndWait(
		&FakePolicy{
			ObjectMeta: metav1.ObjectMeta{
				Name: "policy2",
			},
		},
		&FakeBinding{
			ObjectMeta: metav1.ObjectMeta{
				Name: "binding1",
			},
			PolicyName: "policy2",
		}))
	require.Len(t, testContext.Source.Hooks(), 1, "should have one policy")
	require.Equal(t, "policy2", testContext.Source.Hooks()[0].Policy.GetName(), "policy name should be policy2")
	require.Len(t, testContext.Source.Hooks()[0].Bindings, 1, "should have one binding")
	require.Equal(t, "binding1", testContext.Source.Hooks()[0].Bindings[0].GetName(), "binding name should be binding1")

}

// FakePolicy 是测试用的最小 PolicyAccessor 实现，
// 只保留 generic.PolicySource 需要的字段。
type FakePolicy struct {
	metav1.TypeMeta
	metav1.ObjectMeta

	ParamKind *v1.ParamKind
}

// 编译期断言：FakePolicy 满足 PolicyAccessor。
var _ generic.PolicyAccessor = &FakePolicy{}

// FakeBinding 是测试用的最小 BindingAccessor 实现。
type FakeBinding struct {
	metav1.TypeMeta
	metav1.ObjectMeta

	PolicyName string
	ParamRef   *v1.ParamRef
}

// 编译期断言：FakeBinding 满足 BindingAccessor。
var _ generic.BindingAccessor = &FakeBinding{}

// GetName 返回 policy 名称。
func (fp *FakePolicy) GetName() string {
	return fp.Name
}

// GetNamespace 返回 policy 所在命名空间（集群级策略为空）。
func (fp *FakePolicy) GetNamespace() string {
	return fp.Namespace
}

// GetParamKind 返回参数类型声明。
func (fp *FakePolicy) GetParamKind() *v1.ParamKind {
	return fp.ParamKind
}

// GetMatchConstraints 始终返回 nil：测试不涉及匹配逻辑。
func (fb *FakePolicy) GetMatchConstraints() *v1.MatchResources {
	return nil
}

// GetFailurePolicy 始终返回 nil：测试不涉及失败策略。
func (fb *FakePolicy) GetFailurePolicy() *v1.FailurePolicyType {
	return nil
}

// GetName 返回 binding 名称。
func (fb *FakeBinding) GetName() string {
	return fb.Name
}

// GetNamespace 返回 binding 所在命名空间。
func (fb *FakeBinding) GetNamespace() string {
	return fb.Namespace
}

// GetPolicyName 返回 binding 指向的 policy；测试中只使用名称，忽略命名空间。
func (fb *FakeBinding) GetPolicyName() types.NamespacedName {
	return types.NamespacedName{
		Name: fb.PolicyName,
	}
}

// GetMatchResources 始终返回 nil。
func (fb *FakeBinding) GetMatchResources() *v1.MatchResources {
	return nil
}

// GetParamRef 返回参数引用。
func (fb *FakeBinding) GetParamRef() *v1.ParamRef {
	return fb.ParamRef
}

// DeepCopyObject 做浅拷贝即可：测试对象没有需要深拷贝的嵌套指针。
func (fp *FakePolicy) DeepCopyObject() runtime.Object {
	// 刻意简化的 deepcopy
	newFP := &FakePolicy{}
	*newFP = *fp
	return newFP
}

// DeepCopyObject 同样做浅拷贝。
func (fb *FakeBinding) DeepCopyObject() runtime.Object {
	// 刻意简化的 deepcopy
	newFB := &FakeBinding{}
	*newFB = *fb
	return newFB
}

// TestCollectParamsHandlesCacheMiss 测试参数存在于 apiserver 但尚未传播到 informer 缓存的竞态：
// 当 Policy、Binding 与 Param（如 ConfigMap）被快速连续创建时会出现这种情况。
// 验证缓存返回 NotFound 时 CollectParams 会回退到直接 API 调用。
func TestCollectParamsHandlesCacheMiss(t *testing.T) {
	testContext, testCancel, err := generic.NewPolicyTestContext(
		t,
		func(fp *FakePolicy) generic.PolicyAccessor { return fp },
		func(fb *FakeBinding) generic.BindingAccessor { return fb },
		func(fp *FakePolicy) generic.Evaluator { return nil },
		makeTestDispatcher,
		nil,
		nil,
	)
	require.NoError(t, err)
	defer testCancel()
	require.NoError(t, testContext.Start())

	// 直接写入 dynamic client（模拟 etcd）而不经过 informer，以复现竞态。
	configMap := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]interface{}{
				"name":      "test-param",
				"namespace": "default",
			},
			"data": map[string]interface{}{
				"maxReplicas": "3",
			},
		},
	}

	// 直接把 ConfigMap 写入 dynamic client 的 tracker（绕过 informer），
	// 模拟“对象已在 etcd 中，但 watch 事件尚未到达 informer 缓存”的状态。
	ctx := context.Background()
	_, err = testContext.DynamicClient.Resource(schema.GroupVersionResource{
		Group:    "",
		Version:  "v1",
		Resource: "configmaps",
	}).Namespace("default").Create(ctx, configMap, metav1.CreateOptions{})
	require.NoError(t, err)

	// Now create policy and binding that reference this ConfigMap
	policy := &FakePolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-policy",
		},
		ParamKind: &v1.ParamKind{
			APIVersion: "v1",
			Kind:       "ConfigMap",
		},
	}

	binding := &FakeBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-binding",
		},
		PolicyName: "test-policy",
		ParamRef: &v1.ParamRef{
			Name:      "test-param",
			Namespace: "default",
		},
	}

	// 通过正常路径创建 policy 与 binding（它们会进入 informer）。
	require.NoError(t, testContext.UpdateAndWait(policy, binding))

	// 取得 ConfigMap 对应的 param informer。
	hooks := testContext.Source.Hooks()
	require.Len(t, hooks, 1, "should have one policy hook")

	hook := hooks[0]
	require.NotNil(t, hook.ParamInformer, "param informer should be set")

	// 调用 CollectParams：即使 ConfigMap 不在 informer 缓存中，
	// 也应通过 client 回退成功返回。
	params, err := generic.CollectParams(
		policy.GetParamKind(),
		hook.ParamInformer,
		hook.ParamScope,
		binding.GetParamRef(),
		"default",
		hook.DynamicClient,
		hook.RESTMapper,
	)

	// 有 client 回退修复时：应成功
	require.NoError(t, err, "CollectParams should succeed by falling back to direct client Get")
	require.Len(t, params, 1, "should have retrieved the param")
	require.NotNil(t, params[0], "param should not be nil")

	// 回退查询返回的表示必须与 informer 缓存对同一对象的表示一致，
	// 否则同一策略在不同时机求值会得到不同结果。
	cmParam, ok := params[0].(*corev1.ConfigMap)
	require.Truef(t, ok, "expected fallback to return *corev1.ConfigMap, got %T", params[0])
	require.Empty(t, cmParam.APIVersion, "typed param should have empty apiVersion, like informer-cached objects")
	require.Empty(t, cmParam.Kind, "typed param should have empty kind, like informer-cached objects")
	require.Equal(t, "test-param", cmParam.Name)
	require.Equal(t, map[string]string{"maxReplicas": "3"}, cmParam.Data)

	// 没有 client 回退修复时：上面会因 informer 缓存中尚无 ConfigMap 而以 NotFound 失败。
}

// TestCollectParamsHandlesMissingCRD 验证当 paramKind 指向 RESTMapper 不认识的 CRD 时，
// CollectParams 应把参数视为缺失并交给 ParameterNotFoundAction 处理，而不是返回内部错误。
func TestCollectParamsHandlesMissingCRD(t *testing.T) {
	testContext, testCancel, err := generic.NewPolicyTestContext(
		t,
		func(fp *FakePolicy) generic.PolicyAccessor { return fp },
		func(fb *FakeBinding) generic.BindingAccessor { return fb },
		func(fp *FakePolicy) generic.Evaluator { return nil },
		makeTestDispatcher,
		nil,
		nil,
	)
	require.NoError(t, err)
	defer testCancel()
	require.NoError(t, testContext.Start())

	// 先用一个已知的 paramKind（ConfigMap）引导出 Hook，以便获得非 nil 的
	// ParamInformer/RESTMapper/DynamicClient。下面的 CollectParams 调用会换成
	// RESTMapper 无法解析的 paramKind，这正是被测场景。
	policy := &FakePolicy{
		ObjectMeta: metav1.ObjectMeta{Name: "test-policy"},
		ParamKind: &v1.ParamKind{
			APIVersion: "v1",
			Kind:       "ConfigMap",
		},
	}
	binding := &FakeBinding{
		ObjectMeta: metav1.ObjectMeta{Name: "test-binding"},
		PolicyName: "test-policy",
		ParamRef: &v1.ParamRef{
			Name:      "ignored",
			Namespace: "default",
		},
	}
	require.NoError(t, testContext.UpdateAndWait(policy, binding))

	hooks := testContext.Source.Hooks()
	require.Len(t, hooks, 1, "should have one policy hook")
	hook := hooks[0]
	require.NotNil(t, hook.ParamInformer, "param informer should be set")
	require.NotNil(t, hook.RESTMapper, "restMapper should be set")
	require.NotNil(t, hook.DynamicClient, "dynamicClient should be set")

	missingParamKind := &v1.ParamKind{
		APIVersion: "missing.example.com/v1",
		Kind:       "MissingCRD",
	}

	// 未指定 ParameterNotFoundAction：视为参数缺失但不算错误，返回空列表。
	t.Run("no ParameterNotFoundAction returns no params and no error", func(t *testing.T) {
		paramRef := &v1.ParamRef{
			Name:      "some-param",
			Namespace: "default",
		}

		params, err := generic.CollectParams(
			missingParamKind,
			hook.ParamInformer,
			meta.RESTScopeNamespace,
			paramRef,
			"default",
			hook.DynamicClient,
			hook.RESTMapper,
		)

		require.NoError(t, err, "an unknown paramKind must not surface as an internal error")
		require.Empty(t, params, "no params should be returned when the paramKind cannot be resolved")
	})

	// ParameterNotFoundAction=Deny：参数缺失应转为拒绝错误。
	t.Run("ParameterNotFoundAction=Deny returns deny error", func(t *testing.T) {
		denyAction := v1.DenyAction
		paramRef := &v1.ParamRef{
			Name:                    "some-param",
			Namespace:               "default",
			ParameterNotFoundAction: &denyAction,
		}

		params, err := generic.CollectParams(
			missingParamKind,
			hook.ParamInformer,
			meta.RESTScopeNamespace,
			paramRef,
			"default",
			hook.DynamicClient,
			hook.RESTMapper,
		)

		require.Error(t, err, "Deny action should produce an error when the param is missing")
		require.Contains(t, err.Error(), "no params found")
		require.Empty(t, params)
	})

	// ParameterNotFoundAction=Allow：参数缺失视为通过，不产生错误。
	t.Run("ParameterNotFoundAction=Allow returns no params and no error", func(t *testing.T) {
		allowAction := v1.AllowAction
		paramRef := &v1.ParamRef{
			Name:                    "some-param",
			Namespace:               "default",
			ParameterNotFoundAction: &allowAction,
		}

		params, err := generic.CollectParams(
			missingParamKind,
			hook.ParamInformer,
			meta.RESTScopeNamespace,
			paramRef,
			"default",
			hook.DynamicClient,
			hook.RESTMapper,
		)

		require.NoError(t, err, "Allow action should not produce an error when the param is missing")
		require.Empty(t, params)
	})
}
