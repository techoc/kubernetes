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

package customresourcedefinition

import (
	"context"
	"fmt"

	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"

	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/validation"
	apiservervalidation "k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	apiextensionsfeatures "k8s.io/apiextensions-apiserver/pkg/features"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apiserver/pkg/cel/environment"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/names"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// strategy 实现 CustomResourceDefinition 的 REST 行为。
type strategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

// NewStrategy 返回 CRD 的策略，名称生成沿用通用实现。
func NewStrategy(typer runtime.ObjectTyper) strategy {
	return strategy{typer, names.SimpleNameGenerator}
}

// NamespaceScoped 返回 false：CRD 是集群级资源。
func (strategy) NamespaceScoped() bool {
	return false
}

// GetResetFields 返回服务端会重置、用户不应修改的字段集合。
func (strategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"apiextensions.k8s.io/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
		"apiextensions.k8s.io/v1beta1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}

	return fields
}

// PrepareForCreate 在创建前清空 status；generation 从 1 开始。
func (strategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	crd := obj.(*apiextensions.CustomResourceDefinition)
	crd.Status = apiextensions.CustomResourceDefinitionStatus{}
	crd.Generation = 1

	// 创建时即记录存储版本，避免首次写入后 status 与存储状态不一致。
	for _, v := range crd.Spec.Versions {
		if v.Storage {
			if !apiextensions.IsStoredVersion(crd, v.Name) {
				crd.Status.StoredVersions = append(crd.Status.StoredVersions, v.Name)
			}
			break
		}
	}
	dropDisabledFields(crd, nil)
}

// PrepareForUpdate 在更新前丢弃用户不可设置的字段（status）。
func (strategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newCRD := obj.(*apiextensions.CustomResourceDefinition)
	oldCRD := old.(*apiextensions.CustomResourceDefinition)
	newCRD.Status = oldCRD.Status

	// spec 的任何变化都递增 generation；status 变化应反映对应的 generation。
	// 状态管理交给客户端：服务端无法知道写 status 的一方看到的是哪一版 spec。
	// 看起来 obj 里就带着 spec，但写操作可能来自 read-update-write 循环，
	// 其中的 spec 未必是控制器“看到”的那份；将来 status 也可能拆成独立对象。
	if !apiequality.Semantic.DeepEqual(oldCRD.Spec, newCRD.Spec) {
		newCRD.Generation = oldCRD.Generation + 1
	}

	// 切换存储版本时，把新版本追加进 StoredVersions 以便后续迁移。
	for _, v := range newCRD.Spec.Versions {
		if v.Storage {
			if !apiextensions.IsStoredVersion(newCRD, v.Name) {
				newCRD.Status.StoredVersions = append(newCRD.Status.StoredVersions, v.Name)
			}
			break
		}
	}
	dropDisabledFields(newCRD, oldCRD)
}

// Validate 校验新建的 CustomResourceDefinition。
func (strategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	return validation.ValidateCustomResourceDefinition(ctx, obj.(*apiextensions.CustomResourceDefinition))
}

// WarningsOnCreate 返回创建 CRD 时给用户的警告。
func (strategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	newCRD := obj.(*apiextensions.CustomResourceDefinition)

	var warnings []string

	// 用系统的默认兼容版本判断 format 是否被识别。
	compatibilityVersion := environment.DefaultCompatibilityVersion()

	// 收集新 CRD 中无法识别的 format。
	unrecognizedFormats := getUnrecognizedFormatsInCRD(&newCRD.Spec, compatibilityVersion)

	// 为每个无法识别的 format 生成警告。
	for _, format := range unrecognizedFormats {
		warnings = append(warnings, fmt.Sprintf("unrecognized format %q", format))
	}

	warnings = append(warnings, getNonScalarListTypeSetWarnings(&newCRD.Spec)...)

	return warnings
}

// AllowCreateOnUpdate 返回 false：CRD 只能通过 POST 创建，不允许 update 时隐式创建。
func (strategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// AllowUnconditionalUpdate 返回 false：更新必须携带 resourceVersion。
func (strategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}

// Canonicalize 在校验后规范化对象；CRD 无可规范化的内容。
func (strategy) Canonicalize(obj runtime.Object) {
}

// ValidateUpdate 是更新 CRD 时的默认校验。
func (strategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateCustomResourceDefinitionUpdate(ctx, obj.(*apiextensions.CustomResourceDefinition), old.(*apiextensions.CustomResourceDefinition))
}

// WarningsOnUpdate 返回更新 CRD 时给用户的警告。
func (strategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	newCRD := obj.(*apiextensions.CustomResourceDefinition)
	oldCRD := old.(*apiextensions.CustomResourceDefinition)

	var warnings []string

	// 用系统的默认兼容版本判断 format 是否被识别。
	compatibilityVersion := environment.DefaultCompatibilityVersion()

	// 同时收集新旧 CRD 中无法识别的 format。
	oldUnrecognizedFormats := getUnrecognizedFormatsInCRD(&oldCRD.Spec, compatibilityVersion)
	newUnrecognizedFormats := getUnrecognizedFormatsInCRD(&newCRD.Spec, compatibilityVersion)

	// 只对本次更新新引入的 format 告警（ratcheting），旧告警不重复。
	oldFormatsSet := make(map[string]bool)
	for _, format := range oldUnrecognizedFormats {
		oldFormatsSet[format] = true
	}

	var newlyIntroducedFormats []string
	for _, format := range newUnrecognizedFormats {
		if !oldFormatsSet[format] {
			newlyIntroducedFormats = append(newlyIntroducedFormats, format)
		}
	}

	// 为新引入的无法识别 format 生成警告。
	for _, format := range newlyIntroducedFormats {
		warnings = append(warnings, fmt.Sprintf("unrecognized format %q", format))
	}

	// 同样只报告新出现的 list-type: set 警告。
	seen := sets.New[string]()
	seen.Insert(getNonScalarListTypeSetWarnings(&oldCRD.Spec)...)
	for _, w := range getNonScalarListTypeSetWarnings(&newCRD.Spec) {
		if !seen.Has(w) {
			warnings = append(warnings, w)
		}
	}

	return warnings
}

// getUnrecognizedFormatsInCRD 返回 CRD schema 中所有无法识别的 format。
func getUnrecognizedFormatsInCRD(spec *apiextensions.CustomResourceDefinitionSpec, compatibilityVersion *version.Version) []string {
	var unrecognizedFormats []string

	// 检查顶层校验 schema（v1 已废弃，但为向后兼容仍支持）。
	if spec.Validation != nil && spec.Validation.OpenAPIV3Schema != nil {
		unrecognizedFormats = append(unrecognizedFormats, getUnrecognizedFormatsInSchema(spec.Validation.OpenAPIV3Schema, compatibilityVersion)...)
	}

	// 检查每个版本的 schema。
	for _, v := range spec.Versions {
		if v.Schema != nil && v.Schema.OpenAPIV3Schema != nil {
			unrecognizedFormats = append(unrecognizedFormats, getUnrecognizedFormatsInSchema(v.Schema.OpenAPIV3Schema, compatibilityVersion)...)
		}
	}

	return unrecognizedFormats
}

// getNonScalarListTypeSetWarnings 返回 CRD schema 中所有
// x-kubernetes-list-type: set 搭配非标量元素时的警告；输出排序保证稳定。
func getNonScalarListTypeSetWarnings(spec *apiextensions.CustomResourceDefinitionSpec) []string {
	seen := sets.New[string]()
	check := func(s *apiextensions.JSONSchemaProps) bool {
		if s.XListType != nil && *s.XListType == "set" && s.Items != nil && s.Items.Schema != nil {
			switch s.Items.Schema.Type {
			case "object", "array":
				// SSA 与 CEL 校验规则无法正确处理对象/数组元素的 set 语义。
				seen.Insert(fmt.Sprintf("x-kubernetes-list-type: set for items of type %q is not supported by server-side apply or CEL validation rules", s.Items.Schema.Type))
			}
		}
		return false // 继续遍历
	}
	if spec.Validation != nil && spec.Validation.OpenAPIV3Schema != nil {
		validation.SchemaHas(spec.Validation.OpenAPIV3Schema, check)
	}
	for _, v := range spec.Versions {
		if v.Schema != nil && v.Schema.OpenAPIV3Schema != nil {
			validation.SchemaHas(v.Schema.OpenAPIV3Schema, check)
		}
	}
	return sets.List(seen)
}

// getUnrecognizedFormatsInSchema 递归遍历 schema，收集无法识别的 format。
func getUnrecognizedFormatsInSchema(schema *apiextensions.JSONSchemaProps, compatibilityVersion *version.Version) []string {
	var unrecognizedFormats []string

	// 复用 SchemaHas 遍历整棵 schema 树。
	validation.SchemaHas(schema, func(s *apiextensions.JSONSchemaProps) bool {
		if len(s.Format) > 0 {
			// 转成 spec.Schema 才能调用 format 校验。
			specSchema := &spec.Schema{SchemaProps: spec.SchemaProps{Format: s.Format, Type: []string{s.Type}}}
			if formats := apiservervalidation.GetUnrecognizedFormats(specSchema, compatibilityVersion); len(formats) > 0 {
				unrecognizedFormats = append(unrecognizedFormats, formats...)
			}
		}
		return false // 继续遍历
	})

	return unrecognizedFormats
}

// statusStrategy 实现 CRD status 子资源的 REST 行为。
type statusStrategy struct {
	runtime.ObjectTyper
	names.NameGenerator
}

// NewStatusStrategy 返回 CRD status 子资源的策略。
func NewStatusStrategy(typer runtime.ObjectTyper) statusStrategy {
	return statusStrategy{typer, names.SimpleNameGenerator}
}

// NamespaceScoped 返回 false：CRD 是集群级资源。
func (statusStrategy) NamespaceScoped() bool {
	return false
}

// GetResetFields 返回服务端会重置、用户不应修改的字段集合。
func (statusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"apiextensions.k8s.io/v1": fieldpath.NewSet(
			// 与 PrepareForUpdate 保持一致，不重置 metadata
			// https://github.com/kubernetes/kubernetes/issues/137681
			// fieldpath.MakePathOrDie("metadata"),

			fieldpath.MakePathOrDie("spec"),
		),
		"apiextensions.k8s.io/v1beta1": fieldpath.NewSet(
			// 与 PrepareForUpdate 保持一致，不重置 metadata
			// https://github.com/kubernetes/kubernetes/issues/137681
			// fieldpath.MakePathOrDie("metadata"),

			fieldpath.MakePathOrDie("spec"),
		),
	}

	return fields
}

// PrepareForUpdate 丢弃对 spec 的修改，只允许更新 status。
func (statusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newObj := obj.(*apiextensions.CustomResourceDefinition)
	oldObj := old.(*apiextensions.CustomResourceDefinition)
	newObj.Spec = oldObj.Spec

	// status 子资源的更新只针对 status，不允许借机修改 objectmeta。
	metav1.ResetObjectMetaForStatus(&newObj.ObjectMeta, &newObj.ObjectMeta)
}

// AllowCreateOnUpdate 返回 false：status 更新不能隐式创建对象。
func (statusStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// AllowUnconditionalUpdate 返回 false：更新必须携带 resourceVersion。
func (statusStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}

// Canonicalize 在校验后规范化对象；status 无可规范化的内容。
func (statusStrategy) Canonicalize(obj runtime.Object) {
}

// ValidateUpdate 是更新 CRD status 时的默认校验。
func (statusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateUpdateCustomResourceDefinitionStatus(ctx, obj.(*apiextensions.CustomResourceDefinition), old.(*apiextensions.CustomResourceDefinition))
}

// WarningsOnUpdate 返回更新 CRD status 时的警告；status 更新不产生警告。
func (statusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// GetAttrs 返回对象的 labels 与 fields，供过滤使用。
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	apiserver, ok := obj.(*apiextensions.CustomResourceDefinition)
	if !ok {
		return nil, nil, fmt.Errorf("given object is not a CustomResourceDefinition")
	}
	return labels.Set(apiserver.ObjectMeta.Labels), CustomResourceDefinitionToSelectableFields(apiserver), nil
}

// MatchCustomResourceDefinition 返回 etcd 后端使用的过滤谓词，
// 只把匹配 label/field 选择器的事件推送给 apiserver 客户端。
func MatchCustomResourceDefinition(label labels.Selector, field fields.Selector) storage.SelectionPredicate {
	return storage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// CustomResourceDefinitionToSelectableFields 返回对象可被字段选择器匹配的字段集合。
func CustomResourceDefinitionToSelectableFields(obj *apiextensions.CustomResourceDefinition) fields.Set {
	return generic.ObjectMetaFieldsSet(&obj.ObjectMeta, true)
}

// dropDisabledFields 丢弃 feature gate 未开启的字段。
// 如果旧对象已经在使用这些字段，则保留——否则关闭 feature gate 会抹掉已有数据。
func dropDisabledFields(newCRD *apiextensions.CustomResourceDefinition, oldCRD *apiextensions.CustomResourceDefinition) {
	if !utilfeature.DefaultFeatureGate.Enabled(apiextensionsfeatures.CRDValidationRatcheting) && (oldCRD == nil || !specHasOptionalOldSelf(&oldCRD.Spec)) {
		if newCRD.Spec.Validation != nil {
			dropOptionalOldSelfField(newCRD.Spec.Validation.OpenAPIV3Schema)
		}

		for _, v := range newCRD.Spec.Versions {
			if v.Schema != nil {
				dropOptionalOldSelfField(v.Schema.OpenAPIV3Schema)
			}
		}
	}
	if !utilfeature.DefaultFeatureGate.Enabled(apiextensionsfeatures.CustomResourceFieldSelectors) && (oldCRD == nil || !specHasSelectableFields(&oldCRD.Spec)) {
		dropSelectableFields(&newCRD.Spec)
	}
	if !utilfeature.DefaultFeatureGate.Enabled(apiextensionsfeatures.CRDObservedGenerationTracking) && (oldCRD == nil || !observedGenerationTrackingInUse(&oldCRD.Status)) {
		dropObservedGeneration(&newCRD.Status)
	}
}

// dropOptionalOldSelfField 从 CRD schema 中清除 optionalOldSelf 字段。
func dropOptionalOldSelfField(schema *apiextensions.JSONSchemaProps) {
	if schema == nil {
		return
	}
	for i := range schema.XValidations {
		schema.XValidations[i].OptionalOldSelf = nil
	}

	// 递归处理嵌套 schema；map 元素需写回，slice 元素可直接取址。
	if schema.AdditionalProperties != nil {
		dropOptionalOldSelfField(schema.AdditionalProperties.Schema)
	}
	for def, jsonSchema := range schema.Properties {
		dropOptionalOldSelfField(&jsonSchema)
		schema.Properties[def] = jsonSchema
	}
	if schema.Items != nil {
		dropOptionalOldSelfField(schema.Items.Schema)
		for i, jsonSchema := range schema.Items.JSONSchemas {
			dropOptionalOldSelfField(&jsonSchema)
			schema.Items.JSONSchemas[i] = jsonSchema
		}
	}
}

// specHasOptionalOldSelf 判断 CRD spec 的 schema 是否使用了 optionalOldSelf。
func specHasOptionalOldSelf(spec *apiextensions.CustomResourceDefinitionSpec) bool {
	return validation.HasSchemaWith(spec, schemaHasOptionalOldSelf)
}

// schemaHasOptionalOldSelf 判断单个 schema 是否使用了 optionalOldSelf。
func schemaHasOptionalOldSelf(s *apiextensions.JSONSchemaProps) bool {
	return validation.SchemaHas(s, func(s *apiextensions.JSONSchemaProps) bool {
		for _, v := range s.XValidations {
			if v.OptionalOldSelf != nil {
				return true
			}

		}
		return false
	})
}

// dropSelectableFields 清除顶层和各版本上的 selectableFields。
func dropSelectableFields(spec *apiextensions.CustomResourceDefinitionSpec) {
	spec.SelectableFields = nil
	for i := range spec.Versions {
		spec.Versions[i].SelectableFields = nil
	}
}

// dropObservedGeneration 清除 status 与 conditions 中的 observedGeneration。
func dropObservedGeneration(status *apiextensions.CustomResourceDefinitionStatus) {
	status.ObservedGeneration = 0
	for i := range status.Conditions {
		status.Conditions[i].ObservedGeneration = 0
	}
}

// observedGenerationTrackingInUse 判断 status 是否已经在记录 observedGeneration。
func observedGenerationTrackingInUse(status *apiextensions.CustomResourceDefinitionStatus) bool {
	if status.ObservedGeneration != 0 {
		return true
	}
	for i := range status.Conditions {
		if status.Conditions[i].ObservedGeneration != 0 {
			return true
		}
	}
	return false
}

// specHasSelectableFields 判断顶层或任一版本是否配置了 selectableFields。
func specHasSelectableFields(spec *apiextensions.CustomResourceDefinitionSpec) bool {
	if spec.SelectableFields != nil {
		return true
	}
	for _, v := range spec.Versions {
		if v.SelectableFields != nil {
			return true
		}
	}

	return false
}
