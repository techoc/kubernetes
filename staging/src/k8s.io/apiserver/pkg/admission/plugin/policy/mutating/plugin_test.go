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

package mutating_test

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
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/admission/plugin/policy/generic"
	"k8s.io/apiserver/pkg/admission/plugin/policy/matching"
	"k8s.io/apiserver/pkg/admission/plugin/policy/mutating"
	"k8s.io/apiserver/pkg/admission/plugin/policy/mutating/patch"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/openapi/openapitest"
	"k8s.io/utils/ptr"
)

// setupTest 构造 MutatingAdmissionPolicy 的端到端测试环境：
// 真实的 PolicySource/Dispatcher，加上注入的 compiler（用于绕过 CEL 编译），
// 并注册 Pod 的 RESTMapping，使匹配逻辑可以解析资源。
func setupTest(
	t *testing.T,
	compiler func(*v1.MutatingAdmissionPolicy) mutating.PolicyEvaluator,
) *generic.PolicyTestContext[*v1.MutatingAdmissionPolicy, *v1.MutatingAdmissionPolicyBinding, mutating.PolicyEvaluator] {

	testContext, testCancel, err := generic.NewPolicyTestContext[*v1.MutatingAdmissionPolicy, *v1.MutatingAdmissionPolicyBinding, mutating.PolicyEvaluator](
		t,
		mutating.NewMutatingAdmissionPolicyAccessor,
		mutating.NewMutatingAdmissionPolicyBindingAccessor,
		compiler,
		func(a authorizer.UnconditionalAuthorizer, m *matching.Matcher, i kubernetes.Interface) generic.Dispatcher[mutating.PolicyHook] {
			// 使用内嵌 schema 而不是 discovery schema，避免测试依赖真实集群。
			return mutating.NewDispatcher(a, m, patch.NewTypeConverterManager(nil, openapitest.NewEmbeddedFileClient()))
		},
		nil,
		[]meta.RESTMapping{
			{
				Resource: schema.GroupVersionResource{
					Group:    "",
					Version:  "v1",
					Resource: "pods",
				},
				GroupVersionKind: schema.GroupVersionKind{
					Group:   "",
					Version: "v1",
					Kind:    "Pod",
				},
				Scope: meta.RESTScopeNamespace,
			},
		})
	require.NoError(t, err)
	t.Cleanup(testCancel)
	require.NoError(t, testContext.Start())
	return testContext
}

// TestBasicPatch 展示一个总是设置注解的 compiler 能正常工作：
// 用 annotationPatcher 替代真实 CEL 求值，验证分发链路会把补丁写回对象。
func TestBasicPatch(t *testing.T) {
	expectedAnnotations := map[string]string{"foo": "bar"}

	// 把所有 policy 都当作“将 foo 注解设为 bar”处理
	testContext := setupTest(t, func(p *v1.MutatingAdmissionPolicy) mutating.PolicyEvaluator {
		return mutating.PolicyEvaluator{Mutators: []patch.Patcher{annotationPatcher{expectedAnnotations}}}
	})

	// 构造能匹配的 policy 与 binding，不带参数
	require.NoError(t, testContext.UpdateAndWait(
		&v1.MutatingAdmissionPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "policy"},
			Spec: v1.MutatingAdmissionPolicySpec{
				MatchConstraints: &v1.MatchResources{
					MatchPolicy:       ptr.To(v1.Equivalent),
					NamespaceSelector: &metav1.LabelSelector{},
					ObjectSelector:    &metav1.LabelSelector{},
				},
				Mutations: []v1.Mutation{
					{
						ApplyConfiguration: &v1.ApplyConfiguration{
							Expression: "ignored, but required",
						},
						PatchType: v1.PatchTypeApplyConfiguration,
					},
				},
			},
		},
		&v1.MutatingAdmissionPolicyBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "binding"},
			Spec: v1.MutatingAdmissionPolicyBindingSpec{
				PolicyName: "policy",
			},
		},
	))

	// 验证对象经过 policy 后获得该注解
	testObject := &corev1.ConfigMap{}
	err := testContext.Dispatch(testObject, nil, admission.Create)
	require.NoError(t, err)
	require.Equal(t, expectedAnnotations, testObject.Annotations)
}

// TestJSONPatch 验证 JSONPatch 类型的变更：smdPatcher 直接返回目标对象，
// 因此结果等价于用补丁对象整体覆盖。
func TestJSONPatch(t *testing.T) {
	patchObj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]interface{}{
				"annotations": map[string]interface{}{
					"foo": "bar",
				},
			},
			"data": map[string]interface{}{
				"myfield": "myvalue",
			},
		},
	}

	testContext := setupTest(t, func(p *v1.MutatingAdmissionPolicy) mutating.PolicyEvaluator {
		return mutating.PolicyEvaluator{
			Mutators: []patch.Patcher{smdPatcher{patch: patchObj}},
		}
	})

	// Set up a policy and binding that match, no params
	require.NoError(t, testContext.UpdateAndWait(
		&v1.MutatingAdmissionPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "policy"},
			Spec: v1.MutatingAdmissionPolicySpec{
				MatchConstraints: &v1.MatchResources{
					MatchPolicy:       ptr.To(v1.Equivalent),
					NamespaceSelector: &metav1.LabelSelector{},
					ObjectSelector:    &metav1.LabelSelector{},
				},
				Mutations: []v1.Mutation{
					{
						JSONPatch: &v1.JSONPatch{
							Expression: "ignored, but required",
						},
						PatchType: v1.PatchTypeApplyConfiguration,
					},
				},
			},
		},
		&v1.MutatingAdmissionPolicyBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "binding"},
			Spec: v1.MutatingAdmissionPolicyBindingSpec{
				PolicyName: "policy",
			},
		},
	))

	// 验证对象经过 policy 后获得补丁中的注解与 data
	testObject := &corev1.ConfigMap{}
	err := testContext.Dispatch(testObject, nil, admission.Create)
	require.NoError(t, err)
	require.Equal(t, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{"foo": "bar"},
		},
		Data: map[string]string{"myfield": "myvalue"},
	}, testObject)
}

// TestSSAPatch 验证服务端应用（SSA）补丁路径：smdPatcher 走 ApplyStructuredMergeDiff，
// 结果应与 JSONPatch 用例一致，说明两种补丁类型最终都落到同一对象表示。
func TestSSAPatch(t *testing.T) {
	patchObj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "ConfigMap",
			"metadata": map[string]interface{}{
				"annotations": map[string]interface{}{
					"foo": "bar",
				},
			},
			"data": map[string]interface{}{
				"myfield": "myvalue",
			},
		},
	}

	testContext := setupTest(t, func(p *v1.MutatingAdmissionPolicy) mutating.PolicyEvaluator {
		return mutating.PolicyEvaluator{
			Mutators: []patch.Patcher{smdPatcher{patch: patchObj}},
		}
	})

	// Set up a policy and binding that match, no params
	require.NoError(t, testContext.UpdateAndWait(
		&v1.MutatingAdmissionPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "policy"},
			Spec: v1.MutatingAdmissionPolicySpec{
				MatchConstraints: &v1.MatchResources{
					MatchPolicy:       ptr.To(v1.Equivalent),
					NamespaceSelector: &metav1.LabelSelector{},
					ObjectSelector:    &metav1.LabelSelector{},
				},
				Mutations: []v1.Mutation{
					{
						ApplyConfiguration: &v1.ApplyConfiguration{
							Expression: "ignored, but required",
						},
						PatchType: v1.PatchTypeApplyConfiguration,
					},
				},
			},
		},
		&v1.MutatingAdmissionPolicyBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "binding"},
			Spec: v1.MutatingAdmissionPolicyBindingSpec{
				PolicyName: "policy",
			},
		},
	))

	// 验证对象经过 policy 后获得补丁中的注解与 data
	testObject := &corev1.ConfigMap{}
	err := testContext.Dispatch(testObject, nil, admission.Create)
	require.NoError(t, err)
	require.Equal(t, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{"foo": "bar"},
		},
		Data: map[string]string{"myfield": "myvalue"},
	}, testObject)
}

// TestSSAMapList 验证 SSA 合并语义作用于 map list（这里用 initContainers）时
// 会追加而非替换已有元素：注入的 init 容器应排在原有容器之后。
func TestSSAMapList(t *testing.T) {
	patchObj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]interface{}{
				"annotations": map[string]interface{}{
					"foo": "bar",
				},
			},
			"spec": map[string]interface{}{
				"initContainers": []interface{}{
					map[string]interface{}{
						"name":  "injected-init-container",
						"image": "injected-image",
					},
				},
			},
		},
	}

	testContext := setupTest(t, func(p *v1.MutatingAdmissionPolicy) mutating.PolicyEvaluator {
		return mutating.PolicyEvaluator{
			Mutators: []patch.Patcher{smdPatcher{patch: patchObj}},
		}
	})

	// Set up a policy and binding that match, no params
	require.NoError(t, testContext.UpdateAndWait(
		&v1.MutatingAdmissionPolicy{
			ObjectMeta: metav1.ObjectMeta{Name: "policy"},
			Spec: v1.MutatingAdmissionPolicySpec{
				MatchConstraints: &v1.MatchResources{
					MatchPolicy:       ptr.To(v1.Equivalent),
					NamespaceSelector: &metav1.LabelSelector{},
					ObjectSelector:    &metav1.LabelSelector{},
				},
				Mutations: []v1.Mutation{
					{
						ApplyConfiguration: &v1.ApplyConfiguration{
							Expression: "ignored, but required",
						},
						PatchType: v1.PatchTypeApplyConfiguration,
					},
				},
			},
		},
		&v1.MutatingAdmissionPolicyBinding{
			ObjectMeta: metav1.ObjectMeta{Name: "binding"},
			Spec: v1.MutatingAdmissionPolicyBindingSpec{
				PolicyName: "policy",
			},
		},
	))

	// 验证注入的 init 容器追加在原有 init 容器之后
	testObject := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{},
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{
				{
					Name:  "init-container",
					Image: "image",
				},
			},
		},
	}
	err := testContext.Dispatch(testObject, nil, admission.Create)
	require.NoError(t, err)
	require.Equal(t, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{"foo": "bar"},
		},
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{
				{
					Name:  "init-container",
					Image: "image",
				},
				{
					Name:  "injected-init-container",
					Image: "injected-image",
				},
			},
		},
	}, testObject)
}

// annotationPatcher 是测试用 Patcher：直接设置注解，不做 CEL 求值。
type annotationPatcher struct {
	annotations map[string]string
}

// Patch 在版本化对象的副本上设置注解后返回。
func (ap annotationPatcher) Patch(ctx context.Context, request patch.Request, runtimeCELCostBudget int64) (runtime.Object, error) {
	obj := request.VersionedAttributes.VersionedObject.Object().DeepCopyObject()
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}
	accessor.SetAnnotations(ap.annotations)
	return obj, nil
}

// smdPatcher 是测试用 Patcher：把预置的结构化合并补丁应用到对象上。
type smdPatcher struct {
	patch *unstructured.Unstructured
}

// Patch 委托给 ApplyStructuredMergeDiff，走真实的 SSA 合并逻辑。
func (sp smdPatcher) Patch(ctx context.Context, request patch.Request, runtimeCELCostBudget int64) (runtime.Object, error) {
	return patch.ApplyStructuredMergeDiff(request.TypeConverter, request.VersionedAttributes.VersionedObject.Object(), sp.patch)
}
