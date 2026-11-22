/*
Copyright 2019 The Kubernetes Authors.

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

package defaulting

import (
	"context"
	"fmt"
	"reflect"

	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel"
	schemaobjectmeta "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/objectmeta"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning"
	apiservervalidation "k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	apiextensionsfeatures "k8s.io/apiextensions-apiserver/pkg/features"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
)

// ValidateDefaults 检查默认值是否通过校验且被正确 prune。
// 传入 context 是为了在 CEL 校验期间支持取消。
func ValidateDefaults(ctx context.Context, pth *field.Path, s *structuralschema.Structural, isResourceRoot, requirePrunedDefaults bool) (field.ErrorList, error) {
	f := NewRootObjectFunc().WithTypeMeta(metav1.TypeMeta{APIVersion: "validation/v1", Kind: "Validation"})

	if isResourceRoot {
		// 资源根节点始终按内嵌资源处理，以便 metadata/apiVersion/kind 走元数据校验路径。
		if s == nil {
			s = &structuralschema.Structural{}
		}
		if !s.XEmbeddedResource {
			clone := *s
			clone.XEmbeddedResource = true
			s = &clone
		}
	}

	allErr, error, _ := validate(ctx, pth, s, s, f, false, requirePrunedDefaults, celconfig.RuntimeCELCostBudget)
	return allErr, error
}

// validate 是校验的递归步骤。insideMeta 为 true 表示 s 描述的是 TypeMeta 或 ObjectMeta。
// SurroundingObjectFunc f 用于校验 TypeMeta 或 ObjectMeta 字段的默认值。
// 传入 context 是为了在 CEL 校验期间支持取消。
func validate(ctx context.Context, pth *field.Path, s *structuralschema.Structural, rootSchema *structuralschema.Structural, f SurroundingObjectFunc, insideMeta, requirePrunedDefaults bool, costBudget int64) (allErrs field.ErrorList, error error, remainingCost int64) {
	remainingCost = costBudget
	if s == nil {
		return nil, nil, remainingCost
	}

	if s.XEmbeddedResource {
		// 进入内嵌资源后元数据上下文重置，默认值在资源根上校验。
		insideMeta = false
		f = NewRootObjectFunc().WithTypeMeta(metav1.TypeMeta{APIVersion: "validation/v1", Kind: "Validation"})
		rootSchema = s
	}

	isResourceRoot := s == rootSchema

	if s.Default.Object != nil {
		validator := apiservervalidation.NewSchemaValidatorFromOpenAPI(s.ToKubeOpenAPI())

		if insideMeta {
			obj, _, err := f(runtime.DeepCopyJSONValue(s.Default.Object))
			if err != nil {
				// 理论上不会发生：f(s.Default.Object) 只有在其为根对象函数且默认值
				// 不是 map 时才报错，而那种情况不会进入这里。
				return nil, fmt.Errorf("failed to validate default value inside metadata: %v", err), remainingCost
			}

			// 校验 ObjectMeta/TypeMeta 及其余内容
			if err := schemaobjectmeta.Coerce(nil, obj, rootSchema, true, false); err != nil {
				allErrs = append(allErrs, field.Invalid(pth.Child("default"), s.Default.Object, fmt.Sprintf("must result in valid metadata: %v", err)))
			} else if errs := schemaobjectmeta.Validate(ctx, nil, obj, rootSchema, true); len(errs) > 0 {
				allErrs = append(allErrs, field.Invalid(pth.Child("default"), s.Default.Object, fmt.Sprintf("must result in valid metadata: %v", errs.ToAggregate())))
			} else if errs := apiservervalidation.ValidateCustomResource(pth.Child("default"), s.Default.Object, validator); len(errs) > 0 {
				allErrs = append(allErrs, errs...)
			} else if celValidator := cel.NewValidator(s, isResourceRoot, celconfig.PerCallLimit); celValidator != nil {
				celErrs, rmCost := celValidator.Validate(ctx, pth.Child("default"), s, s.Default.Object, s.Default.Object, remainingCost)
				allErrs = append(allErrs, celErrs...)

				if len(celErrs) == 0 && utilfeature.DefaultFeatureGate.Enabled(apiextensionsfeatures.CRDValidationRatcheting) {
					// 启用 ratcheting 时，某些 CEL 规则可能使用 optionalOldSelf。
					// 对这种规则，上面的校验不足以判断默认值是否能通过 create
					// 或无关联 update 引入。
					//
					// 因此再从 nil 到默认值模拟一次 update，确保默认值能通过。
					celErrs, rmCostWithoutOldObject := celValidator.Validate(ctx, pth.Child("default"), s, s.Default.Object, nil, remainingCost)
					allErrs = append(allErrs, celErrs...)

					// 取两次运行中剩余预算较小的那个
					if rmCostWithoutOldObject < rmCost {
						rmCost = rmCostWithoutOldObject
					}
				}

				remainingCost = rmCost
				if remainingCost < 0 {
					return allErrs, nil, remainingCost
				}
			}
		} else {
			// 检查默认值是否被 prune
			if requirePrunedDefaults {
				pruned := runtime.DeepCopyJSONValue(s.Default.Object)
				pruning.Prune(pruned, s, s.XEmbeddedResource)
				if !reflect.DeepEqual(pruned, s.Default.Object) {
					allErrs = append(allErrs, field.Invalid(pth.Child("default"), s.Default.Object, "must not have unknown fields"))
				}
			}

			// 校验 ObjectMeta/TypeMeta 及其余内容
			if err := schemaobjectmeta.Coerce(pth.Child("default"), s.Default.Object, s, s.XEmbeddedResource, false); err != nil {
				allErrs = append(allErrs, err)
			} else if errs := schemaobjectmeta.Validate(ctx, pth.Child("default"), s.Default.Object, s, s.XEmbeddedResource); len(errs) > 0 {
				allErrs = append(allErrs, errs...)
			} else if errs := apiservervalidation.ValidateCustomResource(pth.Child("default"), s.Default.Object, validator); len(errs) > 0 {
				allErrs = append(allErrs, errs...)
			} else if celValidator := cel.NewValidator(s, isResourceRoot, celconfig.PerCallLimit); celValidator != nil {
				celErrs, rmCost := celValidator.Validate(ctx, pth.Child("default"), s, s.Default.Object, s.Default.Object, remainingCost)
				allErrs = append(allErrs, celErrs...)

				if len(celErrs) == 0 && utilfeature.DefaultFeatureGate.Enabled(apiextensionsfeatures.CRDValidationRatcheting) {
					// 启用 ratcheting 时，某些 CEL 规则可能使用 optionalOldSelf。
					// 对这种规则，上面的校验不足以判断默认值是否能通过 create
					// 或无关联 update 引入。
					//
					// 因此再从 nil 到默认值模拟一次 update，确保默认值能通过。
					celErrs, rmCostWithoutOldObject := celValidator.Validate(ctx, pth.Child("default"), s, s.Default.Object, nil, remainingCost)
					allErrs = append(allErrs, celErrs...)

					// 取两次运行中剩余预算较小的那个
					if rmCostWithoutOldObject < rmCost {
						rmCost = rmCostWithoutOldObject
					}
				}

				remainingCost = rmCost
				if remainingCost < 0 {
					return allErrs, nil, remainingCost
				}
			}
		}
	}

	// 不递归 additionalProperties，因为那里禁止默认值

	if s.Items != nil {
		errs, err, rCost := validate(ctx, pth.Child("items"), s.Items, rootSchema, f.Index(), insideMeta, requirePrunedDefaults, remainingCost)
		remainingCost = rCost
		allErrs = append(allErrs, errs...)
		if err != nil {
			return nil, err, remainingCost
		}
		if remainingCost < 0 {
			return allErrs, nil, remainingCost
		}
	}

	for k, subSchema := range s.Properties {
		subInsideMeta := insideMeta
		if s.XEmbeddedResource && (k == "metadata" || k == "apiVersion" || k == "kind") {
			subInsideMeta = true
		}
		errs, err, rCost := validate(ctx, pth.Child("properties").Key(k), &subSchema, rootSchema, f.Child(k), subInsideMeta, requirePrunedDefaults, remainingCost)
		remainingCost = rCost
		allErrs = append(allErrs, errs...)
		if err != nil {
			return nil, err, remainingCost
		}
		if remainingCost < 0 {
			return allErrs, nil, remainingCost
		}
	}

	return allErrs, nil, remainingCost
}
