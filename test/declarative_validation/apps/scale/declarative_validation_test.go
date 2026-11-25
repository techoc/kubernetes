/*
Copyright 2025 The Kubernetes Authors.

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

// Package scale 对 apps/v1beta1.Scale 和 apps/v1beta2.Scale 子资源
// 执行声明式校验。与 test/declarative_validation 下的其他目录不同，
// Scale 子资源没有 RESTUpdateStrategy：每个父资源的 ScaleREST.Update
// 都在内部组合了 ValidateScale + rest.DeclarativeValidation。
// 本测试通过 VerifyUpdateValidationEquivalenceFunc 模拟这种组合。
package scale

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	apitesting "k8s.io/kubernetes/pkg/api/testing"
	"k8s.io/kubernetes/pkg/apis/autoscaling"
	autoscalingvalidation "k8s.io/kubernetes/pkg/apis/autoscaling/validation"
	deploymentstorage "k8s.io/kubernetes/pkg/registry/apps/deployment/storage"
)

func TestDeclarativeValidateUpdate(t *testing.T) {
	testCases := map[string]struct {
		old          autoscaling.Scale
		update       autoscaling.Scale
		expectedErrs field.ErrorList
	}{
		"replicas: 0":        {old: mkScale(), update: mkScale(setReplicas(0))},
		"replicas: positive": {old: mkScale(), update: mkScale(setReplicas(100))},
		"replicas: negative": {
			old:    mkScale(),
			update: mkScale(setReplicas(-1)),
			expectedErrs: field.ErrorList{
				field.Invalid(field.NewPath("spec.replicas"), nil, "").WithOrigin("minimum").MarkBeta(),
			},
		},
	}

	for _, v := range apiVersions {
		t.Run(v, func(t *testing.T) {
			ctx := genericapirequest.WithRequestInfo(genericapirequest.NewDefaultContext(),
				&genericapirequest.RequestInfo{APIGroup: "apps", APIVersion: v, Subresource: "scale"})
			for k, tc := range testCases {
				t.Run(k, func(t *testing.T) {
					tc.old.ResourceVersion = "1"
					tc.update.ResourceVersion = "1"
					apitesting.VerifyUpdateValidationEquivalenceFunc(t, ctx, &tc.update, &tc.old, validateScale, tc.expectedErrs, apitesting.WithSubResources("scale"))
				})
			}
		})
	}
}

// validateScale 调用生产环境中每个 ScaleREST.UpdatedObject
// 所使用的同一个 ValidateScaleUpdate 辅助函数（例如
// pkg/registry/apps/deployment/storage/storage.go），并传入
// deployment ScaleREST 所使用的真实的类型专用 GroupVersionKindProvider
// （它可代表 apps 的 Scale 子资源 -- replicaset 和 statefulset
// 使用相同的 apps/v1beta{1,2}.Scale 映射），从而让测试中的 GVK
// 映射与生产环境一致。
func validateScale(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	scale := obj.(*autoscaling.Scale)
	var oldScale *autoscaling.Scale
	if old != nil {
		oldScale = old.(*autoscaling.Scale)
	}
	return autoscalingvalidation.ValidateScaleUpdate(ctx, scale, oldScale, legacyscheme.Scheme, &deploymentstorage.ScaleREST{})
}

func mkScale(tweaks ...func(*autoscaling.Scale)) autoscaling.Scale {
	s := autoscaling.Scale{
		ObjectMeta: metav1.ObjectMeta{Name: "abc", Namespace: metav1.NamespaceDefault},
		Spec:       autoscaling.ScaleSpec{Replicas: 1},
	}
	for _, tweak := range tweaks {
		tweak(&s)
	}
	return s
}

func setReplicas(val int32) func(*autoscaling.Scale) {
	return func(s *autoscaling.Scale) { s.Spec.Replicas = val }
}
