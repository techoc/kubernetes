/*
Copyright 2014 The Kubernetes Authors.

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

package validation

import (
	"context"
	"fmt"
	"strings"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/api/validate"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1validation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// FieldImmutableErrorMsg 是字段不可变时使用的错误消息。
const FieldImmutableErrorMsg string = `field is immutable`

// TotalAnnotationSizeLimitB 是对象所有 annotation 的键值总长度上限（256 kB）。
const TotalAnnotationSizeLimitB int = 256 * (1 << 10) // 256 kB

// BannedOwners 是不允许作为 owner 的对象黑名单。
var BannedOwners = map[schema.GroupVersionKind]struct{}{
	{Group: "", Version: "v1", Kind: "Event"}: {},
}

// ValidateAnnotations 校验一组 annotation 的定义是否合法：
// 键需为 QualifiedName（不区分大小写），且总大小不超过限制。
func ValidateAnnotations(annotations map[string]string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for k := range annotations {
		// 规则是 QualifiedName，但不区分大小写，因此先转小写再校验。
		for _, msg := range validation.IsQualifiedName(strings.ToLower(k)) {
			allErrs = append(allErrs, field.Invalid(fldPath, k, msg).WithOrigin("format=k8s-label-key"))
		}
	}
	if err := ValidateAnnotationsSize(annotations); err != nil {
		allErrs = append(allErrs, field.TooLong(fldPath, "" /*unused*/, TotalAnnotationSizeLimitB))
	}
	return allErrs
}

// ValidateAnnotationsSize 检查所有 annotation 键值总长度是否超过上限。
func ValidateAnnotationsSize(annotations map[string]string) error {
	var totalSize int64
	for k, v := range annotations {
		totalSize += (int64)(len(k)) + (int64)(len(v))
	}
	if totalSize > (int64)(TotalAnnotationSizeLimitB) {
		return fmt.Errorf("annotations size %d is larger than limit %d", totalSize, TotalAnnotationSizeLimitB)
	}
	return nil
}

// validateOwnerReference 校验单个 ownerReference 的 apiVersion/kind/name/uid
// 是否齐全且合法，并检查 owner 是否在黑名单中。
func validateOwnerReference(ownerReference metav1.OwnerReference, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	gv, err := schema.ParseGroupVersion(ownerReference.APIVersion)
	// 对遗留组而言 gvk.Group 为空。
	if len(ownerReference.APIVersion) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("apiVersion"), "must not be empty").MarkCoveredByDeclarative())
	} else if err != nil || len(gv.Version) == 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("apiVersion"), ownerReference.APIVersion, "must be <group>/<version> or <version>"))
	}
	if len(ownerReference.Kind) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("kind"), "must not be empty").MarkCoveredByDeclarative())
	}
	if len(ownerReference.Name) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("name"), "must not be empty").MarkCoveredByDeclarative())
	}
	if len(ownerReference.UID) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("uid"), "must not be empty").MarkCoveredByDeclarative())
	}
	gvk := gv.WithKind(ownerReference.Kind)
	if _, ok := BannedOwners[gvk]; ok {
		allErrs = append(allErrs, field.Invalid(fldPath, ownerReference, fmt.Sprintf("%s is disallowed from being an owner", gvk)))
	}
	return allErrs
}

// ValidateOwnerReferences 校验一组 ownerReference 的定义是否合法，
// 并确保最多只有一个引用的 Controller 为 true。
func ValidateOwnerReferences(ownerReferences []metav1.OwnerReference, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	firstControllerName := ""
	for idx, ref := range ownerReferences {
		allErrs = append(allErrs, validateOwnerReference(ref, fldPath.Index(idx))...)
		if ref.Controller != nil && *ref.Controller {
			curControllerName := ref.Kind + "/" + ref.Name
			if firstControllerName != "" {
				allErrs = append(allErrs, field.Invalid(fldPath, ownerReferences,
					fmt.Sprintf("Only one reference can have Controller set to true. Found \"true\" in references for %v and %v", firstControllerName, curControllerName)))
			} else {
				firstControllerName = curControllerName
			}
		}
	}
	return allErrs
}

// ValidateFinalizerName 校验 finalizer 名称。
func ValidateFinalizerName(stringValue string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for _, msg := range validation.IsQualifiedName(stringValue) {
		allErrs = append(allErrs, field.Invalid(fldPath, stringValue, msg))
	}

	return allErrs
}

// ValidateNoNewFinalizers 校验对象正在删除时没有新增 finalizer。
func ValidateNoNewFinalizers(newFinalizers []string, oldFinalizers []string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	extra := sets.NewString(newFinalizers...).Difference(sets.NewString(oldFinalizers...))
	if len(extra) != 0 {
		allErrs = append(allErrs, field.Forbidden(fldPath, fmt.Sprintf("no new finalizers can be added if the object is being deleted, found new finalizers %#v", extra.List())))
	}
	return allErrs
}

// ValidateImmutableField 校验新值与旧值深度相等（即字段不可变）。
func ValidateImmutableField(newVal, oldVal interface{}, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if !apiequality.Semantic.DeepEqual(oldVal, newVal) {
		allErrs = append(allErrs, field.Invalid(fldPath, newVal, FieldImmutableErrorMsg))
	}
	return allErrs
}

// ValidateObjectMetaDeclaratively 以声明式方式校验 ObjectMeta，并去重手写校验产生的错误。
// betaEnabled 控制是否强制执行 Beta 稳定级别的声明式校验规则。
// 注意：该方法应用于尚未启用或无法启用声明式校验的类型。已启用声明式校验且校验代码
// 由生成器生成的类型必须使用 ValidateObjectMeta 与 ValidateObjectMetaUpdate。
func ValidateObjectMetaDeclaratively(ctx context.Context, op operation.Type, obj, oldObj *metav1.ObjectMeta, requiresNamespace bool, nameFn ValidateNameFunc, fldPath *field.Path, betaEnabled bool) field.ErrorList {
	var errs field.ErrorList
	switch op {
	case operation.Create:
		errs = ValidateObjectMeta(obj, requiresNamespace, nameFn, fldPath)
	case operation.Update:
		errs = ValidateObjectMetaUpdate(obj, oldObj, fldPath)
	}
	// 先运行声明式校验，再根据稳定性级别过滤，最后剔除被声明式覆盖的手写错误，
	// 避免同一问题报两次。
	dvErrs := v1validation.Validate_ObjectMeta(ctx, operation.Operation{Type: op}, fldPath, obj, oldObj)
	enforcedDVErrs := validate.FilterEnforcedDeclarativeErrors(ctx, dvErrs, betaEnabled)
	errs = errs.MarkFromImperative()
	errs = validate.FilterCoveredHandwrittenErrors(ctx, errs, enforcedDVErrs, betaEnabled)
	return append(errs, enforcedDVErrs...)
}

// ValidateObjectMeta 在创建时校验对象元数据。它假定 name 生成已经完成。
func ValidateObjectMeta(objMeta *metav1.ObjectMeta, requiresNamespace bool, nameFn ValidateNameFunc, fldPath *field.Path) field.ErrorList {
	metadata, err := meta.Accessor(objMeta)
	if err != nil {
		var allErrs field.ErrorList
		allErrs = append(allErrs, field.Invalid(fldPath, objMeta, err.Error()))
		return allErrs
	}
	return ValidateObjectMetaAccessor(metadata, requiresNamespace, nameFn, fldPath)
}

// objectMetaValidationOptions 定义校验 ObjectMeta 时的行为修改项。
type objectMetaValidationOptions struct {
	/* nothing here yet */
}

// ObjectMetaValidationOption 指定 ValidateObjectMetaWithOpts 与
// ValidateObjectMetaAccessorWithOpts 的行为修改器。
type ObjectMetaValidationOption func(opts *objectMetaValidationOptions)

// ValidateObjectMetaWithOpts 在创建时校验对象元数据。它假定 name 生成已经完成，
// 因此总是执行 name 校验。
//
// 该函数与 ValidateObjectMeta 类似，但通过 options 提供未来扩展能力，并使用
// 不同签名的 name 校验函数。它也不直接校验 generateName 字段，因为 name 生成
// 应已完成，需要符合 nameFn 的是生成结果。
func ValidateObjectMetaWithOpts(objMeta *metav1.ObjectMeta, isNamespaced bool, nameFn ValidateNameFuncWithErrors, fldPath *field.Path, options ...ObjectMetaValidationOption) field.ErrorList {
	metadata, err := meta.Accessor(objMeta)
	if err != nil {
		var allErrs field.ErrorList
		allErrs = append(allErrs, field.InternalError(fldPath, err))
		return allErrs
	}
	return ValidateObjectMetaAccessorWithOpts(metadata, isNamespaced, nameFn, fldPath, options...)
}

// ValidateObjectMetaAccessor 在创建时校验对象元数据。它假定 name 生成已经完成。
func ValidateObjectMetaAccessor(meta metav1.Object, requiresNamespace bool, nameFn ValidateNameFunc, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	if len(meta.GetGenerateName()) != 0 {
		for _, msg := range nameFn(meta.GetGenerateName(), true) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("generateName"), meta.GetGenerateName(), msg))
		}
	}
	// 若 generateName 校验通过但计算出的 name 不通过，说明生成逻辑有问题，
	// 仍需在此报告。这可能让用户困惑，但代表程序缺陷，必须校验。
	// 若有多个字段满足其一即可，则用 or 作为分隔符。
	if len(meta.GetName()) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("name"), "name or generateName is required"))
	} else {
		for _, msg := range nameFn(meta.GetName(), false) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("name"), meta.GetName(), msg))
		}
	}

	return append(allErrs, validateObjectMetaAccessorWithOptsCommon(meta, requiresNamespace, fldPath, nil)...)
}

// ValidateObjectMetaAccessorWithOpts 在创建时校验对象元数据。它假定 name 生成
// 已经完成，因此总是执行 name 校验。
//
// 该函数与 ValidateObjectMetaAccessor 类似，但通过 options 提供未来扩展能力，
// 并使用不同签名的 name 校验函数。它也不直接校验 generateName 字段，因为
// name 生成应已完成，需要符合 nameFn 的是生成结果。
func ValidateObjectMetaAccessorWithOpts(meta metav1.Object, isNamespaced bool, nameFn ValidateNameFuncWithErrors, fldPath *field.Path, options ...ObjectMetaValidationOption) field.ErrorList {
	opts := objectMetaValidationOptions{}
	for _, opt := range options {
		opt(&opts)
	}

	var allErrs field.ErrorList

	// 此处不直接校验 generateName。不同类型可能有不同的 name 生成规则，
	// 而 nameFn 校验的是生成后的数据而非输入。过去我们假定 name 生成总是
	// “追加 5 个随机字符”，但这并非必然。另外 nameFn 应始终考虑 name 最大长度，
	// 而它并不了解 name 生成细节。若 generateName 非法，用户会同时收到
	// generateName 与 name 两个字段的错误；把校验集中在 name 字段上整体体验更好。
	// TODO(thockin): should we do a max-length check here? e.g. 1K or 4K?

	if len(meta.GetGenerateName()) != 0 && len(meta.GetName()) == 0 {
		allErrs = append(allErrs,
			field.InternalError(fldPath.Child("name"), fmt.Errorf("generateName was specified (%q), but no name was generated", meta.GetGenerateName())))
	}
	if len(meta.GetName()) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("name"), "name or generateName is required"))
	} else {
		allErrs = append(allErrs, nameFn(fldPath.Child("name"), meta.GetName())...)
	}

	return append(allErrs, validateObjectMetaAccessorWithOptsCommon(meta, isNamespaced, fldPath, &opts)...)
}

// validateObjectMetaAccessorWithOptsCommon 是两条校验路径共享的实现，
// 负责校验 namespace、generation、labels、annotations、ownerReferences、
// finalizers 与 managedFields 等通用字段。
func validateObjectMetaAccessorWithOptsCommon(meta metav1.Object, isNamespaced bool, fldPath *field.Path, _ *objectMetaValidationOptions) field.ErrorList {
	var allErrs field.ErrorList

	if isNamespaced {
		if len(meta.GetNamespace()) == 0 {
			allErrs = append(allErrs, field.Required(fldPath.Child("namespace"), ""))
		} else {
			for _, msg := range ValidateNamespaceName(meta.GetNamespace(), false) {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("namespace"), meta.GetNamespace(), msg))
			}
		}
	} else {
		if len(meta.GetNamespace()) != 0 {
			// TODO(thockin): change to "may not be specified on this type" or something
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("namespace"), "not allowed on this type"))
		}
	}

	allErrs = append(allErrs, ValidateNonnegativeField(meta.GetGeneration(), fldPath.Child("generation")).MarkCoveredByDeclarative()...)
	allErrs = append(allErrs, v1validation.ValidateLabels(meta.GetLabels(), fldPath.Child("labels"))...)
	allErrs = append(allErrs, ValidateAnnotations(meta.GetAnnotations(), fldPath.Child("annotations"))...)
	allErrs = append(allErrs, ValidateOwnerReferences(meta.GetOwnerReferences(), fldPath.Child("ownerReferences"))...)
	allErrs = append(allErrs, ValidateFinalizers(meta.GetFinalizers(), fldPath.Child("finalizers"))...)
	allErrs = append(allErrs, v1validation.ValidateManagedFields(meta.GetManagedFields(), fldPath.Child("managedFields"), v1validation.CoveredByDeclarative)...)
	return allErrs
}

// ValidateFinalizers 校验 finalizer 名称是否合法，并检查 finalizer 之间是否冲突
// （OrphanDependents 与 DeleteDependents 不能同时设置）。
func ValidateFinalizers(finalizers []string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	hasFinalizerOrphanDependents := false
	hasFinalizerDeleteDependents := false
	for _, finalizer := range finalizers {
		allErrs = append(allErrs, ValidateFinalizerName(finalizer, fldPath)...)
		if finalizer == metav1.FinalizerOrphanDependents {
			hasFinalizerOrphanDependents = true
		}
		if finalizer == metav1.FinalizerDeleteDependents {
			hasFinalizerDeleteDependents = true
		}
	}
	if hasFinalizerDeleteDependents && hasFinalizerOrphanDependents {
		allErrs = append(allErrs, field.Invalid(fldPath, finalizers, fmt.Sprintf("finalizer %s and %s cannot be both set", metav1.FinalizerOrphanDependents, metav1.FinalizerDeleteDependents)))
	}
	return allErrs
}

// ValidateObjectMetaUpdate 在更新时校验对象元数据。
func ValidateObjectMetaUpdate(newMeta, oldMeta *metav1.ObjectMeta, fldPath *field.Path) field.ErrorList {
	newMetadata, err := meta.Accessor(newMeta)
	if err != nil {
		allErrs := field.ErrorList{}
		allErrs = append(allErrs, field.Invalid(fldPath, newMeta, err.Error()))
		return allErrs
	}
	oldMetadata, err := meta.Accessor(oldMeta)
	if err != nil {
		allErrs := field.ErrorList{}
		allErrs = append(allErrs, field.Invalid(fldPath, oldMeta, err.Error()))
		return allErrs
	}
	return ValidateObjectMetaAccessorUpdate(newMetadata, oldMetadata, fldPath)
}

// ValidateObjectMetaAccessorUpdate 在更新时校验对象元数据。
func ValidateObjectMetaAccessorUpdate(newMeta, oldMeta metav1.Object, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	// 对象已在删除中时，不允许新增 finalizer。
	if oldMeta.GetDeletionTimestamp() != nil {
		allErrs = append(allErrs, ValidateNoNewFinalizers(newMeta.GetFinalizers(), oldMeta.GetFinalizers(), fldPath.Child("finalizers"))...)
	}

	// 拒绝未指定 resourceVersion 的更新（无法做乐观并发控制）。
	if len(newMeta.GetResourceVersion()) == 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("resourceVersion"), newMeta.GetResourceVersion(), "must be specified for an update"))
	}

	// generation 不应被递减。
	allErrs = append(allErrs, ValidateNonnegativeField(newMeta.GetGeneration(), fldPath.Child("generation")).MarkCoveredByDeclarative()...)
	if newMeta.GetGeneration() < oldMeta.GetGeneration() {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("generation"), newMeta.GetGeneration(), "must not be decremented"))
	}

	// 以下字段在更新时不可变。
	allErrs = append(allErrs, ValidateImmutableField(newMeta.GetName(), oldMeta.GetName(), fldPath.Child("name"))...)
	allErrs = append(allErrs, ValidateImmutableField(newMeta.GetNamespace(), oldMeta.GetNamespace(), fldPath.Child("namespace"))...)
	allErrs = append(allErrs, ValidateImmutableField(newMeta.GetUID(), oldMeta.GetUID(), fldPath.Child("uid")).WithOrigin("immutable").MarkCoveredByDeclarative()...)
	allErrs = append(allErrs, ValidateImmutableField(newMeta.GetCreationTimestamp(), oldMeta.GetCreationTimestamp(), fldPath.Child("creationTimestamp")).WithOrigin("immutable").MarkCoveredByDeclarative()...)
	allErrs = append(allErrs, ValidateImmutableField(newMeta.GetDeletionTimestamp(), oldMeta.GetDeletionTimestamp(), fldPath.Child("deletionTimestamp")).WithOrigin("immutable").MarkCoveredByDeclarative()...)
	allErrs = append(allErrs, ValidateImmutableField(newMeta.GetDeletionGracePeriodSeconds(), oldMeta.GetDeletionGracePeriodSeconds(), fldPath.Child("deletionGracePeriodSeconds")).WithOrigin("immutable").MarkCoveredByDeclarative()...)

	allErrs = append(allErrs, v1validation.ValidateLabels(newMeta.GetLabels(), fldPath.Child("labels"))...)
	allErrs = append(allErrs, ValidateAnnotations(newMeta.GetAnnotations(), fldPath.Child("annotations"))...)
	allErrs = append(allErrs, ValidateOwnerReferences(newMeta.GetOwnerReferences(), fldPath.Child("ownerReferences"))...)
	allErrs = append(allErrs, v1validation.ValidateManagedFields(newMeta.GetManagedFields(), fldPath.Child("managedFields"), v1validation.CoveredByDeclarative)...)

	return allErrs
}
