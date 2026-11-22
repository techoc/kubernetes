/*
Copyright 2018 The Kubernetes Authors.

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

package customresource

import (
	"context"
	"fmt"

	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel/model"
	structurallisttype "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/listtype"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	apiextensionsfeatures "k8s.io/apiextensions-apiserver/pkg/features"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	"k8s.io/apiserver/pkg/cel/common"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
)

// statusStrategy 实现 status 子资源的更新策略，只允许修改 status。
type statusStrategy struct {
	customResourceStrategy
}

// NewStatusStrategy 基于通用策略构造 status 子资源策略。
func NewStatusStrategy(strategy customResourceStrategy) statusStrategy {
	return statusStrategy{strategy}
}

// GetResetFields 返回会被该策略重置、因而用户不应修改的字段集合。
func (a statusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		fieldpath.APIVersion(a.customResourceStrategy.kind.GroupVersion().String()): fieldpath.NewSet(
			// 注意：若还有其他 CRD 独有的顶层字段，它们在 apiserver 持久化前
			// 同样会被移除，但不会加入 resetFields 集合。

			// 目前这不成问题；如果将来成为问题，可能需要一个与 resetFields
			// 相反的机制：只指定要保留的字段而非要清除的字段，
			// 那样就可以在这种场景下清除除 status 之外的一切。
			fieldpath.MakePathOrDie("metadata"),
			fieldpath.MakePathOrDie("spec"),
		),
	}

	return fields
}

// PrepareForUpdate 用旧对象覆盖新对象，只保留新对象中的 status。
func (a statusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	// update 只允许设置 status
	newCustomResourceObject := obj.(*unstructured.Unstructured)
	newCustomResource := newCustomResourceObject.UnstructuredContent()
	status, ok := newCustomResource["status"]

	// managedFields 必须保留：它在 status 更新过程中已被修改，
	// 用于跟踪变更字段。
	managedFields := newCustomResourceObject.GetManagedFields()

	// 把旧对象复制到新对象
	oldCustomResourceObject := old.(*unstructured.Unstructured)
	// 在此覆盖 metadata 中的 resourceVersion 是安全的：
	// 前面已确认新旧对象的 resourceVersion 相同。
	*newCustomResourceObject = *oldCustomResourceObject.DeepCopy()

	// 设置 status
	newCustomResourceObject.SetManagedFields(managedFields)
	newCustomResource = newCustomResourceObject.UnstructuredContent()
	if ok {
		newCustomResource["status"] = status
	} else {
		delete(newCustomResource, "status")
	}
}

// ValidateUpdate 是终端用户更新 status 时的默认更新校验。
func (a statusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	uNew, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return field.ErrorList{field.Invalid(field.NewPath(""), obj, fmt.Sprintf("has type %T. Must be a pointer to an Unstructured type", obj))}
	}
	uOld, ok := old.(*unstructured.Unstructured)
	if !ok {
		return field.ErrorList{field.Invalid(field.NewPath(""), old, fmt.Sprintf("has type %T. Must be a pointer to an Unstructured type", old))}
	}

	var options []validation.ValidationOption
	var celOptions []cel.Option
	var correlatedObject *common.CorrelatedObject
	if utilfeature.DefaultFeatureGate.Enabled(apiextensionsfeatures.CRDValidationRatcheting) {
		correlatedObject = common.NewCorrelatedObject(uNew.Object, uOld.Object, &model.Structural{Structural: a.structuralSchema})
		if a.structuralSchema != nil {
			options = append(options, validation.WithRatcheting(correlatedObject.Key("status")))
		}
		celOptions = append(celOptions, cel.WithRatcheting(correlatedObject))
	}

	var errs field.ErrorList
	errs = append(errs, a.customResourceStrategy.validator.ValidateStatusUpdate(ctx, uNew, uOld, a.scale, options...)...)

	// x-kubernetes-list-type 的 map 与 set 值采用 ratcheting 校验
	if newErrs := structurallisttype.ValidateListSetsAndMaps(nil, a.structuralSchema, uNew.Object); len(newErrs) > 0 {
		if oldErrs := structurallisttype.ValidateListSetsAndMaps(nil, a.structuralSchema, uOld.Object); len(oldErrs) == 0 {
			errs = append(errs, newErrs...)
		}
	}

	// 校验 x-kubernetes-validations 规则
	if celValidator := a.customResourceStrategy.celValidator; celValidator != nil {
		if has, err := hasBlockingErr(errs); has {
			errs = append(errs, err)
		} else {
			err, _ := celValidator.Validate(ctx, nil, a.customResourceStrategy.structuralSchema, uNew.Object, uOld.Object, celconfig.RuntimeCELCostBudget, celOptions...)
			errs = append(errs, err...)
		}
	}

	// 未挂载到 context 时为空操作
	if utilfeature.DefaultFeatureGate.Enabled(apiextensionsfeatures.CRDValidationRatcheting) {
		validation.Metrics.ObserveRatchetingTime(*correlatedObject.Duration)
	}
	return errs
}

// WarningsOnUpdate 返回给定更新对应的警告。
func (statusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}
