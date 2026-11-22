/*
Copyright 2017 The Kubernetes Authors.

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
	"strings"

	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	v1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel/model"
	structurallisttype "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/listtype"
	schemaobjectmeta "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/objectmeta"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	apiextensionsfeatures "k8s.io/apiextensions-apiserver/pkg/features"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	"k8s.io/apiserver/pkg/cel/common"
	"k8s.io/apiserver/pkg/registry/generic"
	apiserverstorage "k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/names"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/util/jsonpath"
)

// customResourceStrategy 实现单个版本的 CustomResource 的存储策略行为。
type customResourceStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator

	namespaceScoped    bool
	validator          customResourceValidator
	structuralSchema   *structuralschema.Structural
	celValidator       *cel.Validator
	status             *apiextensions.CustomResourceSubresourceStatus
	scale              *apiextensions.CustomResourceSubresourceScale
	kind               schema.GroupVersionKind
	selectableFieldSet []selectableField
}

// selectableField 是已解析（或解析失败）的 selectableField 配置项。
type selectableField struct {
	name      string
	fieldPath *jsonpath.JSONPath
	err       error
}

// NewStrategy 创建 CustomResource 的存储策略。
func NewStrategy(typer runtime.ObjectTyper, namespaceScoped bool, kind schema.GroupVersionKind, schemaValidator, statusSchemaValidator validation.SchemaValidator, structuralSchema *structuralschema.Structural, status *apiextensions.CustomResourceSubresourceStatus, scale *apiextensions.CustomResourceSubresourceScale, selectableFields []v1.SelectableField) customResourceStrategy {
	var celValidator *cel.Validator
	celValidator = cel.NewValidator(structuralSchema, true, celconfig.PerCallLimit) // CEL 程序在这里编译并缓存

	strategy := customResourceStrategy{
		ObjectTyper:     typer,
		NameGenerator:   names.SimpleNameGenerator,
		namespaceScoped: namespaceScoped,
		status:          status,
		scale:           scale,
		validator: customResourceValidator{
			namespaceScoped:       namespaceScoped,
			kind:                  kind,
			schemaValidator:       schemaValidator,
			statusSchemaValidator: statusSchemaValidator,
		},
		structuralSchema: structuralSchema,
		celValidator:     celValidator,
		kind:             kind,
	}
	if utilfeature.DefaultFeatureGate.Enabled(apiextensionsfeatures.CustomResourceFieldSelectors) {
		strategy.selectableFieldSet = prepareSelectableFields(selectableFields)
	}
	return strategy
}

// prepareSelectableFields 预解析 selectable field 的 JSONPath，避免每次取属性时重新解析。
func prepareSelectableFields(selectableFields []v1.SelectableField) []selectableField {
	result := make([]selectableField, len(selectableFields))
	for i, sf := range selectableFields {
		name := strings.TrimPrefix(sf.JSONPath, ".")

		parser := jsonpath.New("selectableField")
		// 缺失字段应视为空值而不是错误。
		parser.AllowMissingKeys(true)
		err := parser.Parse("{" + sf.JSONPath + "}")
		if err == nil {
			result[i] = selectableField{
				name:      name,
				fieldPath: parser,
			}
		} else {
			result[i] = selectableField{
				name: name,
				err:  err,
			}
		}
	}

	return result
}

// NamespaceScoped 返回资源是否为命名空间级别。
func (a customResourceStrategy) NamespaceScoped() bool {
	return a.namespaceScoped
}

// GetResetFields 返回被策略重置、用户不应修改的字段集合。
func (a customResourceStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{}

	if a.status != nil {
		fields[fieldpath.APIVersion(a.kind.GroupVersion().String())] = fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		)
	}

	return fields
}

// PrepareForCreate 在创建前清空 CustomResource 的 status。
func (a customResourceStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	if a.status != nil {
		customResourceObject := obj.(*unstructured.Unstructured)
		customResource := customResourceObject.UnstructuredContent()

		// 创建时不允许设置 status
		delete(customResource, "status")
	}

	accessor, _ := meta.Accessor(obj)
	accessor.SetGeneration(1)
}

// PrepareForUpdate 清除终端用户更新时不允许设置的字段。
func (a customResourceStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newCustomResourceObject := obj.(*unstructured.Unstructured)
	oldCustomResourceObject := old.(*unstructured.Unstructured)

	newCustomResource := newCustomResourceObject.UnstructuredContent()
	oldCustomResource := oldCustomResourceObject.UnstructuredContent()

	// 安装了 /status 子资源端点时，普通 update 不允许改 status。
	if a.status != nil {
		_, ok1 := newCustomResource["status"]
		_, ok2 := oldCustomResource["status"]
		switch {
		case ok2:
			// 旧对象有 status：无条件沿用旧值。
			newCustomResource["status"] = oldCustomResource["status"]
		case ok1:
			// 旧对象没有 status：丢弃新对象里擅自添加的值。
			delete(newCustomResource, "status")
		}
	}

	// 除 `metadata` 外的任何变化都会让 generation 递增。
	newCopyContent := copyNonMetadata(newCustomResource)
	oldCopyContent := copyNonMetadata(oldCustomResource)
	if !apiequality.Semantic.DeepEqual(newCopyContent, oldCopyContent) {
		oldAccessor, _ := meta.Accessor(oldCustomResourceObject)
		newAccessor, _ := meta.Accessor(newCustomResourceObject)
		newAccessor.SetGeneration(oldAccessor.GetGeneration() + 1)
	}
}

// copyNonMetadata 复制对象内容，但跳过 metadata 字段。
func copyNonMetadata(original map[string]interface{}) map[string]interface{} {
	ret := make(map[string]interface{})
	for key, val := range original {
		if key == "metadata" {
			continue
		}
		ret[key] = val
	}
	return ret
}

// Validate 校验新建的 CustomResource。
func (a customResourceStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	u, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return field.ErrorList{field.Invalid(field.NewPath(""), u, fmt.Sprintf("has type %T. Must be a pointer to an Unstructured type", obj))}
	}

	var errs field.ErrorList
	errs = append(errs, a.validator.Validate(ctx, u, a.scale)...)

	// 校验内嵌资源
	errs = append(errs, schemaobjectmeta.Validate(ctx, nil, u.Object, a.structuralSchema, false)...)

	// 校验 x-kubernetes-list-type 的 "map" 和 "set" 不变式
	errs = append(errs, structurallisttype.ValidateListSetsAndMaps(nil, a.structuralSchema, u.Object)...)

	// 校验 x-kubernetes-validations 规则
	if celValidator := a.celValidator; celValidator != nil {
		// 基础校验已失败时不再跑 CEL，否则错误会淹没根因。
		if has, err := hasBlockingErr(errs); has {
			errs = append(errs, err)
		} else {
			err, _ := celValidator.Validate(ctx, nil, a.structuralSchema, u.Object, nil, celconfig.RuntimeCELCostBudget)
			errs = append(errs, err...)
		}
	}

	return errs
}

// WarningsOnCreate 返回创建对象时的警告。
func (a customResourceStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return generateWarningsFromObj(obj, nil)
}

// generateWarningsFromObj 针对新出现的 finalizer 生成警告；old 为 nil 时视为新建。
func generateWarningsFromObj(obj, old runtime.Object) []string {
	var allWarnings []string
	fldPath := field.NewPath("metadata", "finalizers")
	newObjAccessor, err := meta.Accessor(obj)
	if err != nil {
		return allWarnings
	}

	// 只对本次新增的 finalizer 生成警告，避免更新时重复告警。
	newAdded := sets.NewString(newObjAccessor.GetFinalizers()...)
	if old != nil {
		oldObjAccessor, err := meta.Accessor(old)
		if err != nil {
			return allWarnings
		}
		newAdded = newAdded.Difference(sets.NewString(oldObjAccessor.GetFinalizers()...))
	}

	for _, finalizer := range newAdded.List() {
		allWarnings = append(allWarnings, validateKubeFinalizerName(finalizer, fldPath)...)
	}

	return allWarnings
}

// Canonicalize 在校验后规范化对象。CustomResource 无需额外规范化。
func (customResourceStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 对 CustomResource 为 false，即必须通过 POST 创建。
func (customResourceStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// AllowUnconditionalUpdate 是 CustomResource 的默认更新策略。
func (customResourceStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate 是终端用户更新 status 时的默认更新校验。
func (a customResourceStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
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
		// old/new 配对后，校验器可对既有非法值放行。
		correlatedObject = common.NewCorrelatedObject(uNew.Object, uOld.Object, &model.Structural{Structural: a.structuralSchema})
		options = append(options, validation.WithRatcheting(correlatedObject))
		celOptions = append(celOptions, cel.WithRatcheting(correlatedObject))
	}

	var errs field.ErrorList
	errs = append(errs, a.validator.ValidateUpdate(ctx, uNew, uOld, a.scale, options...)...)

	// 检查内嵌对象；这里不区分 update 与 create。
	errs = append(errs, schemaobjectmeta.Validate(ctx, nil, uNew.Object, a.structuralSchema, false)...)

	// x-kubernetes-list-type map/set 的 ratcheting 校验：
	// 旧对象本身已违规时跳过，避免用户被卡住无法修复。
	if oldErrs := structurallisttype.ValidateListSetsAndMaps(nil, a.structuralSchema, uOld.Object); len(oldErrs) == 0 {
		errs = append(errs, structurallisttype.ValidateListSetsAndMaps(nil, a.structuralSchema, uNew.Object)...)
	}

	// 校验 x-kubernetes-validations 规则
	if celValidator := a.celValidator; celValidator != nil {
		if has, err := hasBlockingErr(errs); has {
			errs = append(errs, err)
		} else {
			err, _ := celValidator.Validate(ctx, nil, a.structuralSchema, uNew.Object, uOld.Object, celconfig.RuntimeCELCostBudget, celOptions...)
			errs = append(errs, err...)
		}
	}

	// 未挂到 context 上时为空操作
	if utilfeature.DefaultFeatureGate.Enabled(apiextensionsfeatures.CRDValidationRatcheting) {
		validation.Metrics.ObserveRatchetingTime(*correlatedObject.Duration)
	}
	return errs
}

// WarningsOnUpdate 返回更新对象时的警告。
func (a customResourceStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return generateWarningsFromObj(obj, old)
}

// GetAttrs 返回对象的标签和字段，用于过滤。
func (a customResourceStrategy) GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return nil, nil, err
	}
	sFields, err := a.selectableFields(obj, accessor)
	if err != nil {
		return nil, nil, err
	}
	return accessor.GetLabels(), sFields, nil
}

// selectableFields 返回可用于过滤选择的字段集合。
// 包含 metadata.name、metadata.namespace 以及所有自定义可选字段。
func (a customResourceStrategy) selectableFields(obj runtime.Object, objectMeta metav1.Object) (fields.Set, error) {
	objectMetaFields := objectMetaFieldsSet(objectMeta, a.namespaceScoped)
	var selectableFieldsSet fields.Set

	if utilfeature.DefaultFeatureGate.Enabled(apiextensionsfeatures.CustomResourceFieldSelectors) && len(a.selectableFieldSet) > 0 {
		us, ok := obj.(runtime.Unstructured)
		if !ok {
			return nil, fmt.Errorf("unexpected error casting a custom resource to unstructured")
		}
		uc := us.UnstructuredContent()

		selectableFieldsSet = fields.Set{}
		for _, sf := range a.selectableFieldSet {
			if sf.err != nil {
				return nil, fmt.Errorf("unexpected error parsing jsonPath: %w", sf.err)
			}
			results, err := sf.fieldPath.FindResults(uc)
			if err != nil {
				return nil, fmt.Errorf("unexpected error finding value with jsonPath: %w", err)
			}
			var value any

			if len(results) > 0 && len(results[0]) > 0 {
				// 每个字段配置只允许定位到唯一值，否则视为配置错误。
				if len(results) > 1 || len(results[0]) > 1 {
					return nil, fmt.Errorf("unexpectedly received more than one JSON path result")
				}
				value = results[0][0].Interface()
			}

			// 字段缺失或为 null 时当作空串参与选择，保证 set 中键齐全。
			if value != nil {
				selectableFieldsSet[sf.name] = fmt.Sprint(value)
			} else {
				selectableFieldsSet[sf.name] = ""
			}
		}
	}
	return generic.MergeFieldsSets(objectMetaFields, selectableFieldsSet), nil
}

// objectMetaFieldsSet 返回表示 ObjectMeta 的字段集合。
func objectMetaFieldsSet(objectMeta metav1.Object, namespaceScoped bool) fields.Set {
	if namespaceScoped {
		return fields.Set{
			"metadata.name":      objectMeta.GetName(),
			"metadata.namespace": objectMeta.GetNamespace(),
		}
	}
	return fields.Set{
		"metadata.name": objectMeta.GetName(),
	}
}

// MatchCustomResourceDefinitionStorage 是通用 etcd 后端使用的过滤器，用于把 etcd 的
// watch 事件路由给只关心特定 labels/fields 的 apiserver 客户端。
func (a customResourceStrategy) MatchCustomResourceDefinitionStorage(label labels.Selector, field fields.Selector) apiserverstorage.SelectionPredicate {
	return apiserverstorage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: a.GetAttrs,
	}
}

// OpenAPIv3 的 type/maxLength/maxItems/MaxProperties/required/enum 违规以及类型错误
// 都会被视为阻塞 CEL 校验的错误。
func hasBlockingErr(errs field.ErrorList) (bool, *field.Error) {
	for _, err := range errs {
		if err.Type == field.ErrorTypeNotSupported || err.Type == field.ErrorTypeRequired || err.Type == field.ErrorTypeTooLong || err.Type == field.ErrorTypeTooMany || err.Type == field.ErrorTypeTypeInvalid {
			return true, field.Invalid(nil, nil, "some validation rules were not checked because the object was invalid; correct the existing errors to complete validation")
		}
	}
	return false, nil
}
