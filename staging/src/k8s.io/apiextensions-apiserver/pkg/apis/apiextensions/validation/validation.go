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

// Package validation 提供 CRD 的静态验证逻辑。
// 在 CRD 创建和更新时，验证 CRD 的 spec 是否合法，包括：
//   - 名称和组名的 DNS 子域名格式
//   - 版本配置（必须有且仅有一个 storage 版本）
//   - OpenAPI v3.0 schema 的合法性
//   - CEL 表达式的语法和成本限制
//   - 打印列、子资源、conversion webhook 等配置
//   - API 审批 annotation（受保护的组需要审批）
package validation

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"unicode"
	"unicode/utf8"

	celgo "github.com/google/cel-go/cel"

	"k8s.io/apiextensions-apiserver/pkg/apihelpers"
	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel"
	structuraldefaulting "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/defaulting"
	apiservervalidation "k8s.io/apiextensions-apiserver/pkg/apiserver/validation"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/operation"
	genericvalidation "k8s.io/apimachinery/pkg/api/validation"
	"k8s.io/apimachinery/pkg/util/sets"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
	apiservercel "k8s.io/apiserver/pkg/cel"
	"k8s.io/apiserver/pkg/cel/environment"
	"k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/apiserver/pkg/util/webhook"
)

var (
	// printerColumnDatatypes 是打印列支持的数据类型。
	printerColumnDatatypes = sets.NewString("integer", "number", "string", "boolean", "date")
	// customResourceColumnDefinitionFormats 是打印列支持的格式（OpenAPI format 子集）。
	customResourceColumnDefinitionFormats = sets.NewString("int32", "int64", "float", "double", "byte", "date", "date-time", "password")
	// openapiV3Types 是 OpenAPI v3.0 支持的基本类型。
	openapiV3Types = sets.NewString("string", "number", "integer", "boolean", "array", "object")
)

const (
	// StaticEstimatedCostLimit 是单个 CEL 表达式的最大静态成本估算值。
	// 超过此值的表达式会被拒绝，防止 CEL 表达式消耗过多计算资源。
	StaticEstimatedCostLimit = 10000000
	// StaticEstimatedCRDCostLimit 是整个 CRD 的所有 CEL 验证规则的总成本上限。
	// 限制单个 CRD 的 CEL 表达式总成本，防止大量复杂表达式影响 API Server 性能。
	StaticEstimatedCRDCostLimit = 100000000

	// MaxSelectableFields 是 CRD 允许的最大字段选择器数量。
	// 限制字段选择器数量，避免索引过多导致性能问题。
	MaxSelectableFields = 8
)

var supportedValidationReason = sets.NewString(
	string(apiextensions.FieldValueRequired),
	string(apiextensions.FieldValueForbidden),
	string(apiextensions.FieldValueInvalid),
	string(apiextensions.FieldValueDuplicate),
)

// ValidateCustomResourceDefinition 对新建的 CRD 进行完整的静态验证。
// 包括 metadata、spec、status 和 API 审批 annotation 的验证。
// 这是 CRD 创建时的入口验证函数。
func ValidateCustomResourceDefinition(ctx context.Context, obj *apiextensions.CustomResourceDefinition) field.ErrorList {
	nameValidationFn := func(name string, prefix bool) []string {
		ret := genericvalidation.NameIsDNSSubdomain(name, prefix)
		requiredName := obj.Spec.Names.Plural + "." + obj.Spec.Group
		if name != requiredName {
			ret = append(ret, fmt.Sprintf(`must be spec.names.plural+"."+spec.group`))
		}
		return ret
	}

	opts := validationOptions{
		allowDefaults:                            true,
		requireRecognizedConversionReviewVersion: true,
		requireImmutableNames:                    false,
		requireOpenAPISchema:                     true,
		requireValidPropertyType:                 true,
		requireStructuralSchema:                  true,
		requirePrunedDefaults:                    true,
		requireAtomicSetType:                     true,
		requireMapListKeysMapSetValidation:       true,
		celEnvironmentSet:                        environment.MustBaseEnvSet(environment.DefaultCompatibilityVersion()),
		// CRD 尚未 Established，因此创建时允许暂时存在无效的 Conversion Webhook CABundle。
		allowInvalidCABundle: true,
	}

	allErrs := genericvalidation.ValidateObjectMetaDeclaratively(ctx, operation.Create, &obj.ObjectMeta, nil, false, nameValidationFn, field.NewPath("metadata"), utilfeature.DefaultFeatureGate.Enabled(features.DeclarativeValidationBeta))
	allErrs = append(allErrs, validateCustomResourceDefinitionSpec(ctx, &obj.Spec, opts, field.NewPath("spec"))...)
	allErrs = append(allErrs, ValidateCustomResourceDefinitionStatus(&obj.Status, field.NewPath("status"))...)
	allErrs = append(allErrs, ValidateCustomResourceDefinitionStoredVersions(obj.Status.StoredVersions, obj.Spec.Versions, field.NewPath("status").Child("storedVersions"))...)
	allErrs = append(allErrs, validateAPIApproval(obj, nil)...)
	allErrs = append(allErrs, validatePreserveUnknownFields(obj, nil)...)
	return allErrs
}

// validationOptions 将多个校验选项集中封装，避免在方法之间传递大量布尔参数。
type validationOptions struct {
	// allowDefaults 表示是否允许校验 schema 中包含 default 属性。
	allowDefaults bool
	// disallowDefaultsReason 说明为何不允许 default，便于向用户提供更明确的错误信息。
	disallowDefaultsReason string
	// requireRecognizedConversionReviewVersion 表示是否要求 Webhook ConversionReviewVersions 至少包含一个已识别版本。
	requireRecognizedConversionReviewVersion bool
	// requireImmutableNames 表示是否禁止修改 spec.names 中的不可变名称字段。
	requireImmutableNames bool
	// requireOpenAPISchema 表示是否必须提供 OpenAPI v3 schema。
	requireOpenAPISchema bool
	// requireValidPropertyType 表示是否要求校验 schema 中的属性类型属于合法的 OpenAPI v3 类型。
	requireValidPropertyType bool
	// requireStructuralSchema 表示所有已提供的 schema 都必须满足 Structural Schema 约束。
	requireStructuralSchema bool
	// requirePrunedDefaults 表示是否要求默认值经过 pruning 处理。
	requirePrunedDefaults bool
	// requireAtomicSetType 表示 x-kubernetes-list-type=set 列表的元素类型必须为 atomic。
	requireAtomicSetType bool
	// requireMapListKeysMapSetValidation 控制 map/set 列表的额外校验：
	// 1. map 列表的键字段不能为 nullable，且必须必填或提供默认值。
	// 2. map/set 列表的整个元素不能为 nullable。
	requireMapListKeysMapSetValidation bool
	// preexistingExpressions 记录对象更新前已经存在的 CEL 表达式；创建操作时可以为 nil。
	preexistingExpressions preexistingExpressions
	// versionsWithUnchangedSchemas 记录 CRD 更新时 schema 未发生变化的版本；创建和删除操作不使用该集合。
	// 部分校验会利用它，避免控制平面升级或降级时拒绝此前已接受的版本。
	versionsWithUnchangedSchemas sets.Set[string]
	// suppressPerExpressionCost 表示是否跳过 CEL 单表达式成本上限检查。
	// 若某个版本位于 versionsWithUnchangedSchemas 中，版本校验时会自动设置该选项。
	suppressPerExpressionCost bool

	celEnvironmentSet *environment.EnvSet
	// allowInvalidCABundle 控制是否允许无效的 Conversion Webhook CABundle：更新时仅当旧值本身无效才允许保留；
	// 创建阶段以及 CRD 尚未进入 Established=True 状态前，也允许暂时存在无效值。
	allowInvalidCABundle bool

	// allowTooManySelectableFields 仅在更新时允许字段选择器数量超过 MaxSelectableFields，
	// 且必须满足旧对象已超限并且本次更新没有改变字段选择器集合。
	allowTooManySelectableFields map[string]bool
}

type preexistingExpressions struct {
	rules              sets.Set[string]
	messageExpressions sets.Set[string]
}

func (pe preexistingExpressions) RuleEnv(envSet *environment.EnvSet, expression string) *celgo.Env {
	if pe.rules.Has(expression) {
		return envSet.StoredExpressionsEnv()
	}
	return envSet.NewExpressionsEnv()
}

func (pe preexistingExpressions) MessageExpressionEnv(envSet *environment.EnvSet, expression string) *celgo.Env {
	if pe.messageExpressions.Has(expression) {
		return envSet.StoredExpressionsEnv()
	}
	return envSet.NewExpressionsEnv()
}

func findPreexistingExpressions(spec *apiextensions.CustomResourceDefinitionSpec) preexistingExpressions {
	expressions := preexistingExpressions{rules: sets.New[string](), messageExpressions: sets.New[string]()}
	if spec.Validation != nil && spec.Validation.OpenAPIV3Schema != nil {
		findPreexistingExpressionsInSchema(spec.Validation.OpenAPIV3Schema, expressions)
	}
	for _, v := range spec.Versions {
		if v.Schema != nil && v.Schema.OpenAPIV3Schema != nil {
			findPreexistingExpressionsInSchema(v.Schema.OpenAPIV3Schema, expressions)
		}
	}
	return expressions
}

func findPreexistingExpressionsInSchema(schema *apiextensions.JSONSchemaProps, expressions preexistingExpressions) {
	SchemaHas(schema, func(s *apiextensions.JSONSchemaProps) bool {
		for _, v := range s.XValidations {
			expressions.rules.Insert(v.Rule)
			if len(v.MessageExpression) > 0 {
				expressions.messageExpressions.Insert(v.MessageExpression)
			}
		}
		return false
	})
}

// findVersionsWithUnchangedSchemas 找出新旧 CRD 对象中 schema 保持不变的版本，并返回这些版本名称的集合。
// 本函数不检查重复版本、顶层版本是否包含在 versions 中，也不检查顶层与逐版本 schema 是否同时存在；
// 这些问题会由后续校验步骤统一检查。
func findVersionsWithUnchangedSchemas(obj, oldObject *apiextensions.CustomResourceDefinition) sets.Set[string] {
	versionsWithUnchangedSchemas := sets.New[string]()
	for _, version := range obj.Spec.Versions {
		newSchema, err := apiextensions.GetSchemaForVersion(obj, version.Name)
		if err != nil {
			continue
		}
		oldSchema, err := apiextensions.GetSchemaForVersion(oldObject, version.Name)
		if err != nil {
			continue
		}
		if apiequality.Semantic.DeepEqual(newSchema, oldSchema) {
			versionsWithUnchangedSchemas.Insert(version.Name)
		}
	}
	return versionsWithUnchangedSchemas
}

// suppressExpressionCostForUnchangedSchema 在指定版本的 schema 未变化时，返回 suppressPerExpressionCost=true 的选项副本。
func suppressExpressionCostForUnchangedSchema(opts validationOptions, version string) validationOptions {
	if opts.versionsWithUnchangedSchemas.Has(version) {
		opts.suppressPerExpressionCost = true
	}
	return opts
}

// ValidateCustomResourceDefinitionUpdate 对更新的 CRD 进行静态验证。
// 与 ValidateCustomResourceDefinition 不同，更新验证需要比较新旧对象，
// 检查不可变字段是否被修改、版本迁移是否合法等。
// 传入 context，以便校验默认值中的 CEL 表达式时支持取消操作。
func ValidateCustomResourceDefinitionUpdate(ctx context.Context, obj, oldObj *apiextensions.CustomResourceDefinition) field.ErrorList {
	opts := validationOptions{
		allowDefaults:                            true,
		requireRecognizedConversionReviewVersion: oldObj.Spec.Conversion == nil || hasValidConversionReviewVersionOrEmpty(oldObj.Spec.Conversion.ConversionReviewVersions),
		requireImmutableNames:                    apiextensions.IsCRDConditionTrue(oldObj, apiextensions.Established),
		requireOpenAPISchema:                     requireOpenAPISchema(&oldObj.Spec),
		requireValidPropertyType:                 requireValidPropertyType(&oldObj.Spec),
		requireStructuralSchema:                  requireStructuralSchema(&oldObj.Spec),
		requirePrunedDefaults:                    requirePrunedDefaults(&oldObj.Spec),
		requireAtomicSetType:                     requireAtomicSetType(&oldObj.Spec),
		requireMapListKeysMapSetValidation:       requireMapListKeysMapSetValidation(&oldObj.Spec),
		preexistingExpressions:                   findPreexistingExpressions(&oldObj.Spec),
		versionsWithUnchangedSchemas:             findVersionsWithUnchangedSchemas(obj, oldObj),
		celEnvironmentSet:                        environment.MustBaseEnvSet(environment.DefaultCompatibilityVersion()),
		allowInvalidCABundle:                     allowInvalidCABundle(oldObj),
		allowTooManySelectableFields:             findTooManySelectableFieldsAllowed(obj, oldObj),
	}
	return validateCustomResourceDefinitionUpdate(ctx, obj, oldObj, opts)
}

func validateCustomResourceDefinitionUpdate(ctx context.Context, obj, oldObj *apiextensions.CustomResourceDefinition, opts validationOptions) field.ErrorList {
	allErrs := genericvalidation.ValidateObjectMetaDeclaratively(ctx, operation.Update, &obj.ObjectMeta, &oldObj.ObjectMeta, false, nil, field.NewPath("metadata"), utilfeature.DefaultFeatureGate.Enabled(features.DeclarativeValidationBeta))
	allErrs = append(allErrs, validateCustomResourceDefinitionSpecUpdate(ctx, &obj.Spec, &oldObj.Spec, opts, field.NewPath("spec"))...)
	allErrs = append(allErrs, ValidateCustomResourceDefinitionStatus(&obj.Status, field.NewPath("status"))...)
	allErrs = append(allErrs, ValidateCustomResourceDefinitionStoredVersions(obj.Status.StoredVersions, obj.Spec.Versions, field.NewPath("status").Child("storedVersions"))...)
	allErrs = append(allErrs, validateAPIApproval(obj, oldObj)...)
	allErrs = append(allErrs, validatePreserveUnknownFields(obj, oldObj)...)
	return allErrs
}

// ValidateCustomResourceDefinitionStoredVersions 静态校验 CRD 的 storedVersions 与当前 Spec 版本配置是否一致。
func ValidateCustomResourceDefinitionStoredVersions(storedVersions []string, versions []apiextensions.CustomResourceDefinitionVersion, fldPath *field.Path) field.ErrorList {
	if len(storedVersions) == 0 {
		return field.ErrorList{field.Invalid(fldPath, storedVersions, "must have at least one stored version")}
	}
	allErrs := field.ErrorList{}
	storedVersionsMap := map[string]int{}
	for i, v := range storedVersions {
		storedVersionsMap[v] = i
	}
	for _, v := range versions {
		_, ok := storedVersionsMap[v.Name]
		if v.Storage && !ok {
			allErrs = append(allErrs, field.Invalid(fldPath, storedVersions, "must have the storage version "+v.Name))
		}
		if ok {
			delete(storedVersionsMap, v.Name)
		}
	}

	for v, i := range storedVersionsMap {
		allErrs = append(allErrs, field.Invalid(fldPath.Index(i), v, fmt.Sprintf("missing from spec.versions; %[1]s was previously a storage version, and must remain in spec.versions until a storage migration ensures no data remains persisted in %[1]s and removes %[1]s from status.storedVersions", v)))
	}

	return allErrs
}

// ValidateUpdateCustomResourceDefinitionStatus 静态校验 CRD Status 更新是否合法。
func ValidateUpdateCustomResourceDefinitionStatus(ctx context.Context, obj, oldObj *apiextensions.CustomResourceDefinition) field.ErrorList {
	allErrs := genericvalidation.ValidateObjectMetaDeclaratively(ctx, operation.Update, &obj.ObjectMeta, &oldObj.ObjectMeta, false, nil, field.NewPath("metadata"), utilfeature.DefaultFeatureGate.Enabled(features.DeclarativeValidationBeta))
	allErrs = append(allErrs, ValidateCustomResourceDefinitionStatus(&obj.Status, field.NewPath("status"))...)
	return allErrs
}

// validateCustomResourceDefinitionVersion 静态校验 CRD 的单个版本配置。
// 传入 context，以便校验默认值中的 CEL 表达式时支持取消操作。
func validateCustomResourceDefinitionVersion(ctx context.Context, version *apiextensions.CustomResourceDefinitionVersion, fldPath *field.Path, statusEnabled bool, opts validationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	for _, err := range validateDeprecationWarning(version.Deprecated, version.DeprecationWarning) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("deprecationWarning"), version.DeprecationWarning, err))
	}
	opts = suppressExpressionCostForUnchangedSchema(opts, version.Name)
	allErrs = append(allErrs, validateCustomResourceDefinitionValidation(ctx, version.Schema, statusEnabled, opts, fldPath.Child("schema"))...)
	allErrs = append(allErrs, ValidateCustomResourceDefinitionSubresources(version.Subresources, fldPath.Child("subresources"))...)
	for i := range version.AdditionalPrinterColumns {
		allErrs = append(allErrs, ValidateCustomResourceColumnDefinition(&version.AdditionalPrinterColumns[i], fldPath.Child("additionalPrinterColumns").Index(i))...)
	}

	if len(version.SelectableFields) > 0 {
		if version.Schema == nil || version.Schema.OpenAPIV3Schema == nil {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("selectableFields"), "", "may only be set when `version.schema.openAPIV3Schema` is not included"))
		} else {
			schema, err := structuralschema.NewStructural(version.Schema.OpenAPIV3Schema)
			if err != nil {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("schema.openAPIV3Schema"), "", err.Error()))
			}
			allErrs = append(allErrs, ValidateCustomResourceSelectableFields(version.SelectableFields, schema, fldPath.Child("selectableFields"), opts, version.Name)...)
		}
	}
	return allErrs
}

func validateDeprecationWarning(deprecated bool, deprecationWarning *string) []string {
	if !deprecated && deprecationWarning != nil {
		return []string{"can only be set for deprecated versions"}
	}
	if deprecationWarning == nil {
		return nil
	}
	var errors []string
	if len(*deprecationWarning) > 256 {
		errors = append(errors, "must be <= 256 characters long")
	}
	if len(*deprecationWarning) == 0 {
		errors = append(errors, "must not be an empty string")
	}
	for i, r := range *deprecationWarning {
		if !unicode.IsPrint(r) {
			errors = append(errors, fmt.Sprintf("must only contain printable UTF-8 characters; non-printable character found at index %d", i))
			break
		}
		if unicode.IsControl(r) {
			errors = append(errors, fmt.Sprintf("must only contain printable UTF-8 characters; control character found at index %d", i))
			break
		}
	}
	if !utf8.ValidString(*deprecationWarning) {
		errors = append(errors, "must only contain printable UTF-8 characters")
	}
	return errors
}

// validateCustomResourceDefinitionSpec 验证 CRD 的 spec 部分。
// 包括组名、版本、名称、schema、打印列、子资源、conversion webhook 等。
// 传入 context，以便校验默认值中的 CEL 表达式时支持取消操作。
func validateCustomResourceDefinitionSpec(ctx context.Context, spec *apiextensions.CustomResourceDefinitionSpec, opts validationOptions, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if len(spec.Group) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("group"), ""))
	} else if errs := utilvalidation.IsDNS1123Subdomain(spec.Group); len(errs) > 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("group"), spec.Group, strings.Join(errs, ",")))
	} else if len(strings.Split(spec.Group, ".")) < 2 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("group"), spec.Group, "should be a domain with at least one dot"))
	}

	allErrs = append(allErrs, validateEnumStrings(fldPath.Child("scope"), string(spec.Scope), []string{string(apiextensions.ClusterScoped), string(apiextensions.NamespaceScoped)}, true)...)

	// 启用字段裁剪（pruning）要求使用 Structural Schema。
	if spec.PreserveUnknownFields == nil || *spec.PreserveUnknownFields == false {
		opts.requireStructuralSchema = true
	}

	if opts.requireOpenAPISchema {
		// 检查是否为所有版本提供了顶层共享 schema，或分别提供逐版本 schema。
		if spec.Validation == nil || spec.Validation.OpenAPIV3Schema == nil {
			for i, v := range spec.Versions {
				if v.Schema == nil || v.Schema.OpenAPIV3Schema == nil {
					allErrs = append(allErrs, field.Required(fldPath.Child("versions").Index(i).Child("schema").Child("openAPIV3Schema"), ""))
				}
			}
		}
	} else if spec.PreserveUnknownFields == nil || *spec.PreserveUnknownFields == false {
		// 检查是否为所有对外服务的版本提供了顶层共享 schema，或分别提供逐版本 schema。
		if spec.Validation == nil || spec.Validation.OpenAPIV3Schema == nil {
			for i, v := range spec.Versions {
				schemaPath := fldPath.Child("versions").Index(i).Child("schema", "openAPIV3Schema")
				if v.Served && (v.Schema == nil || v.Schema.OpenAPIV3Schema == nil) {
					allErrs = append(allErrs, field.Required(schemaPath, "because otherwise all fields are pruned"))
				}
			}
		}
	}
	if opts.allowDefaults && specHasDefaults(spec) {
		opts.requireStructuralSchema = true
		if spec.PreserveUnknownFields == nil || *spec.PreserveUnknownFields {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("preserveUnknownFields"), true, "must be false in order to use defaults in the schema"))
		}
	}
	if specHasKubernetesExtensions(spec) {
		opts.requireStructuralSchema = true
	}

	storageFlagCount := 0
	versionsMap := map[string]bool{}
	uniqueNames := true
	for i, version := range spec.Versions {
		if version.Storage {
			storageFlagCount++
		}
		if versionsMap[version.Name] {
			uniqueNames = false
		} else {
			versionsMap[version.Name] = true
		}
		if errs := utilvalidation.IsDNS1035Label(version.Name); len(errs) > 0 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("versions").Index(i).Child("name"), spec.Versions[i].Name, strings.Join(errs, ",")))
		}
		subresources := getSubresourcesForVersion(spec, version.Name)
		allErrs = append(allErrs, validateCustomResourceDefinitionVersion(ctx, &version, fldPath.Child("versions").Index(i), hasStatusEnabled(subresources), opts)...)
	}

	// 顶层字段与逐版本字段不能同时设置。
	if spec.Validation != nil && hasPerVersionSchema(spec.Versions) {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("validation"), "top-level and per-version schemas are mutually exclusive"))
	}
	if spec.Subresources != nil && hasPerVersionSubresources(spec.Versions) {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("subresources"), "top-level and per-version subresources are mutually exclusive"))
	}
	if len(spec.AdditionalPrinterColumns) > 0 && hasPerVersionColumns(spec.Versions) {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("additionalPrinterColumns"), "top-level and per-version additionalPrinterColumns are mutually exclusive"))
	}

	// 不应将所有版本的字段都设置为完全相同的值；这种情况应改用顶层共享字段。
	if hasIdenticalPerVersionSchema(spec.Versions) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("versions"), spec.Versions, "per-version schemas may not all be set to identical values (top-level validation should be used instead)"))
	}
	if hasIdenticalPerVersionSubresources(spec.Versions) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("versions"), spec.Versions, "per-version subresources may not all be set to identical values (top-level subresources should be used instead)"))
	}
	if hasIdenticalPerVersionColumns(spec.Versions) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("versions"), spec.Versions, "per-version additionalPrinterColumns may not all be set to identical values (top-level additionalPrinterColumns should be used instead)"))
	}

	if !uniqueNames {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("versions"), spec.Versions, "must contain unique version names"))
	}
	if storageFlagCount != 1 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("versions"), spec.Versions, "must have exactly one version marked as storage version"))
	}
	if len(spec.Version) != 0 {
		if errs := utilvalidation.IsDNS1035Label(spec.Version); len(errs) > 0 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("version"), spec.Version, strings.Join(errs, ",")))
		}
		if len(spec.Versions) >= 1 && spec.Versions[0].Name != spec.Version {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("version"), spec.Version, "must match the first version in spec.versions"))
		}
	}

	// 除了基本名称格式限制外，Spec 中的部分名称字段为必填项，而 Status 校验不要求这些字段。
	if len(spec.Names.Plural) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("names", "plural"), ""))
	}
	if len(spec.Names.Singular) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("names", "singular"), ""))
	}
	if len(spec.Names.Kind) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("names", "kind"), ""))
	}
	if len(spec.Names.ListKind) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("names", "listKind"), ""))
	}

	allErrs = append(allErrs, ValidateCustomResourceDefinitionNames(&spec.Names, fldPath.Child("names"))...)
	allErrs = append(allErrs, validateCustomResourceDefinitionValidation(ctx, spec.Validation, hasAnyStatusEnabled(spec), suppressExpressionCostForUnchangedSchema(opts, spec.Version), fldPath.Child("validation"))...)
	allErrs = append(allErrs, ValidateCustomResourceDefinitionSubresources(spec.Subresources, fldPath.Child("subresources"))...)

	for i := range spec.AdditionalPrinterColumns {
		if errs := ValidateCustomResourceColumnDefinition(&spec.AdditionalPrinterColumns[i], fldPath.Child("additionalPrinterColumns").Index(i)); len(errs) > 0 {
			allErrs = append(allErrs, errs...)
		}
	}

	if len(spec.SelectableFields) > 0 {
		if spec.Validation == nil {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("selectableFields"), "", "may only be set when validations.schema is included"))
		} else {
			schema, err := structuralschema.NewStructural(spec.Validation.OpenAPIV3Schema)
			if err != nil {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("schema.openAPIV3Schema"), "", err.Error()))
			}

			allErrs = append(allErrs, ValidateCustomResourceSelectableFields(spec.SelectableFields, schema, fldPath.Child("selectableFields"), opts, "")...)
		}
	}

	if (spec.Conversion != nil && spec.Conversion.Strategy != apiextensions.NoneConverter) && (spec.PreserveUnknownFields == nil || *spec.PreserveUnknownFields) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("conversion").Child("strategy"), spec.Conversion.Strategy, "must be None if spec.preserveUnknownFields is true"))
	}
	allErrs = append(allErrs, validateCustomResourceConversion(spec.Conversion, opts.requireRecognizedConversionReviewVersion, fldPath.Child("conversion"), opts)...)

	return allErrs
}

func validateEnumStrings(fldPath *field.Path, value string, accepted []string, required bool) field.ErrorList {
	if value == "" {
		if required {
			return field.ErrorList{field.Required(fldPath, "")}
		}
		return field.ErrorList{}
	}
	for _, a := range accepted {
		if a == value {
			return field.ErrorList{}
		}
	}
	return field.ErrorList{field.NotSupported(fldPath, value, accepted)}
}

// AcceptedConversionReviewVersions 列出上一版本 API Server 能够理解的 ConversionReview 版本。
// 1.15：服务器支持 v1beta1，接受的版本为 ["v1beta1"]。
// 1.16：服务器支持 v1 和 v1beta1，接受的版本仍为 ["v1beta1"]。
// 1.17 及以后：服务器支持 v1 和 v1beta1，接受的版本为 ["v1","v1beta1"]。
var acceptedConversionReviewVersions = sets.NewString(apiextensionsv1.SchemeGroupVersion.Version, apiextensionsv1beta1.SchemeGroupVersion.Version)

func isAcceptedConversionReviewVersion(v string) bool {
	return acceptedConversionReviewVersions.Has(v)
}

func validateConversionReviewVersions(versions []string, requireRecognizedVersion bool, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(versions) < 1 {
		allErrs = append(allErrs, field.Required(fldPath, ""))
	} else {
		seen := map[string]bool{}
		hasAcceptedVersion := false
		for i, v := range versions {
			if seen[v] {
				allErrs = append(allErrs, field.Invalid(fldPath.Index(i), v, "duplicate version"))
				continue
			}
			seen[v] = true
			for _, errString := range utilvalidation.IsDNS1035Label(v) {
				allErrs = append(allErrs, field.Invalid(fldPath.Index(i), v, errString))
			}
			if isAcceptedConversionReviewVersion(v) {
				hasAcceptedVersion = true
			}
		}
		if requireRecognizedVersion && !hasAcceptedVersion {
			allErrs = append(allErrs, field.Invalid(
				fldPath, versions,
				fmt.Sprintf("must include at least one of %v",
					strings.Join(acceptedConversionReviewVersions.List(), ", "))))
		}
	}
	return allErrs
}

// 仅当旧 CABundle 本身无效，或 CRD 尚未 Established 时，才允许继续指定无效的 CA Bundle。
func allowInvalidCABundle(oldCRD *apiextensions.CustomResourceDefinition) bool {
	if !apiextensions.IsCRDConditionTrue(oldCRD, apiextensions.Established) {
		return true
	}
	oldConversion := oldCRD.Spec.Conversion
	if oldConversion == nil || oldConversion.WebhookClientConfig == nil ||
		len(oldConversion.WebhookClientConfig.CABundle) == 0 {
		return false
	}
	return len(webhook.ValidateCABundle(field.NewPath("caBundle"), oldConversion.WebhookClientConfig.CABundle)) > 0
}

// findTooManySelectableFieldsAllowed 返回各版本字段选择器集合的兼容标记。
// 只有旧 CRD 的字段选择器数量已超过 MaxSelectableFields，
// 且本次更新未改变该集合时，才允许保留超限状态。
func findTooManySelectableFieldsAllowed(obj *apiextensions.CustomResourceDefinition, oldCRD *apiextensions.CustomResourceDefinition) map[string]bool {
	result := map[string]bool{}

	if len(oldCRD.Spec.SelectableFields) > MaxSelectableFields && apiequality.Semantic.DeepEqual(obj.Spec.SelectableFields, oldCRD.Spec.SelectableFields) {
		result[""] = true
	}

	oldVersions := make(map[string]*apiextensions.CustomResourceDefinitionVersion, len(oldCRD.Spec.Versions))
	for _, v := range oldCRD.Spec.Versions {
		oldVersions[v.Name] = &v // +k8s:verify-mutation:reason=clone
	}

	for _, v := range obj.Spec.Versions {
		oldV, ok := oldVersions[v.Name]
		if !ok {
			continue
		}
		if len(oldV.SelectableFields) > MaxSelectableFields && apiequality.Semantic.DeepEqual(v.SelectableFields, oldV.SelectableFields) {
			result[v.Name] = true
		}
	}

	return result
}

// hasValidConversionReviewVersionOrEmpty 在列表为空或至少包含一个受支持的版本时返回 true。
func hasValidConversionReviewVersionOrEmpty(versions []string) bool {
	if len(versions) < 1 {
		return true
	}
	for _, v := range versions {
		if isAcceptedConversionReviewVersion(v) {
			return true
		}
	}
	return false
}

func validateCustomResourceConversion(conversion *apiextensions.CustomResourceConversion, requireRecognizedVersion bool, fldPath *field.Path, opts validationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	if conversion == nil {
		return allErrs
	}
	allErrs = append(allErrs, validateEnumStrings(fldPath.Child("strategy"), string(conversion.Strategy), []string{string(apiextensions.NoneConverter), string(apiextensions.WebhookConverter)}, true)...)
	if conversion.Strategy == apiextensions.WebhookConverter {
		if conversion.WebhookClientConfig == nil {
			allErrs = append(allErrs, field.Required(fldPath.Child("webhookClientConfig"), "required when strategy is set to Webhook"))
		} else {
			cc := conversion.WebhookClientConfig
			switch {
			case (cc.URL == nil) == (cc.Service == nil):
				allErrs = append(allErrs, field.Required(fldPath.Child("webhookClientConfig"), "exactly one of url or service is required"))
			case cc.URL != nil:
				allErrs = append(allErrs, webhook.ValidateWebhookURL(fldPath.Child("webhookClientConfig").Child("url"), *cc.URL, true)...)
			case cc.Service != nil:
				allErrs = append(allErrs, webhook.ValidateWebhookService(fldPath.Child("webhookClientConfig").Child("service"), cc.Service.Name, cc.Service.Namespace, cc.Service.Path, cc.Service.Port)...)
			}
			if len(cc.CABundle) > 0 && !opts.allowInvalidCABundle {
				allErrs = append(allErrs, webhook.ValidateCABundle(fldPath.Child("webhookClientConfig").Child("caBundle"), cc.CABundle)...)
			}
		}
		allErrs = append(allErrs, validateConversionReviewVersions(conversion.ConversionReviewVersions, requireRecognizedVersion, fldPath.Child("conversionReviewVersions"))...)
	} else {
		if conversion.WebhookClientConfig != nil {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("webhookClientConfig"), "should not be set when strategy is not set to Webhook"))
		}
		if len(conversion.ConversionReviewVersions) > 0 {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("conversionReviewVersions"), "should not be set when strategy is not set to Webhook"))
		}
	}
	return allErrs
}

// validateCustomResourceDefinitionSpecUpdate 验证 CRD spec 的更新。
// 先执行完整 spec 验证，然后额外检查不可变字段和版本变更的合法性。
// 传入 context，以便校验默认值中的 CEL 表达式时支持取消操作。
func validateCustomResourceDefinitionSpecUpdate(ctx context.Context, spec, oldSpec *apiextensions.CustomResourceDefinitionSpec, opts validationOptions, fldPath *field.Path) field.ErrorList {
	allErrs := validateCustomResourceDefinitionSpec(ctx, spec, opts, fldPath)

	if opts.requireImmutableNames {
		// 这些字段会影响资源存储方式，因此一旦 CRD 建立便不可修改。
		allErrs = append(allErrs, genericvalidation.ValidateImmutableField(spec.Scope, oldSpec.Scope, fldPath.Child("scope"))...)
		allErrs = append(allErrs, genericvalidation.ValidateImmutableField(spec.Names.Kind, oldSpec.Names.Kind, fldPath.Child("names", "kind"))...)
	}

	// 这些字段决定资源名称，资源名称始终不可变，因此不允许更新。
	allErrs = append(allErrs, genericvalidation.ValidateImmutableField(spec.Group, oldSpec.Group, fldPath.Child("group"))...)
	allErrs = append(allErrs, genericvalidation.ValidateImmutableField(spec.Names.Plural, oldSpec.Names.Plural, fldPath.Child("names", "plural"))...)

	return allErrs
}

// getSubresourcesForVersion 返回 CRD Spec 中指定版本的子资源配置。
// 注意：调用方遍历的是已经存在的版本，因此本函数假定指定版本一定存在。
func getSubresourcesForVersion(crd *apiextensions.CustomResourceDefinitionSpec, version string) *apiextensions.CustomResourceSubresources {
	if !hasPerVersionSubresources(crd.Versions) {
		return crd.Subresources
	}
	for _, v := range crd.Versions {
		if version == v.Name {
			return v.Subresources
		}
	}
	return nil
}

// hasAnyStatusEnabled 检查 CRD 顶层或任一版本的 Subresources 是否启用了 Status 子资源。
func hasAnyStatusEnabled(crd *apiextensions.CustomResourceDefinitionSpec) bool {
	if hasStatusEnabled(crd.Subresources) {
		return true
	}
	for _, v := range crd.Versions {
		if hasStatusEnabled(v.Subresources) {
			return true
		}
	}
	return false
}

// hasStatusEnabled 检查给定 Subresources 是否配置了非 nil 的 Status 子资源。
func hasStatusEnabled(subresources *apiextensions.CustomResourceSubresources) bool {
	if subresources != nil && subresources.Status != nil {
		return true
	}
	return false
}

// hasPerVersionSchema 判断 CRD 是否使用逐版本 schema。
func hasPerVersionSchema(versions []apiextensions.CustomResourceDefinitionVersion) bool {
	for _, v := range versions {
		if v.Schema != nil {
			return true
		}
	}
	return false
}

// hasPerVersionSubresources 判断 CRD 是否使用逐版本子资源配置。
func hasPerVersionSubresources(versions []apiextensions.CustomResourceDefinitionVersion) bool {
	for _, v := range versions {
		if v.Subresources != nil {
			return true
		}
	}
	return false
}

// hasPerVersionColumns 判断 CRD 是否使用逐版本打印列配置。
func hasPerVersionColumns(versions []apiextensions.CustomResourceDefinitionVersion) bool {
	for _, v := range versions {
		if len(v.AdditionalPrinterColumns) > 0 {
			return true
		}
	}
	return false
}

// hasIdenticalPerVersionSchema 判断 CRD 的所有版本是否都设置了完全相同的非 nil schema。
func hasIdenticalPerVersionSchema(versions []apiextensions.CustomResourceDefinitionVersion) bool {
	if len(versions) == 0 {
		return false
	}
	value := versions[0].Schema
	for _, v := range versions {
		if v.Schema == nil || !apiequality.Semantic.DeepEqual(v.Schema, value) {
			return false
		}
	}
	return true
}

// hasIdenticalPerVersionSubresources 判断 CRD 的所有版本是否都设置了完全相同的非 nil 子资源配置。
func hasIdenticalPerVersionSubresources(versions []apiextensions.CustomResourceDefinitionVersion) bool {
	if len(versions) == 0 {
		return false
	}
	value := versions[0].Subresources
	for _, v := range versions {
		if v.Subresources == nil || !apiequality.Semantic.DeepEqual(v.Subresources, value) {
			return false
		}
	}
	return true
}

// hasIdenticalPerVersionColumns 判断 CRD 的所有版本是否都设置了完全相同的非 nil 打印列配置。
func hasIdenticalPerVersionColumns(versions []apiextensions.CustomResourceDefinitionVersion) bool {
	if len(versions) == 0 {
		return false
	}
	value := versions[0].AdditionalPrinterColumns
	for _, v := range versions {
		if len(v.AdditionalPrinterColumns) == 0 || !apiequality.Semantic.DeepEqual(v.AdditionalPrinterColumns, value) {
			return false
		}
	}
	return true
}

// ValidateCustomResourceDefinitionStatus 验证 CRD 的 status 部分。
// 主要检查 AcceptedNames 的格式和 conditions 的合法性。
func ValidateCustomResourceDefinitionStatus(status *apiextensions.CustomResourceDefinitionStatus, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, ValidateCustomResourceDefinitionNames(&status.AcceptedNames, fldPath.Child("acceptedNames"))...)
	return allErrs
}

// ValidateCustomResourceDefinitionNames 静态校验 CRD 的名称字段。
func ValidateCustomResourceDefinitionNames(names *apiextensions.CustomResourceDefinitionNames, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if errs := utilvalidation.IsDNS1035Label(names.Plural); len(names.Plural) > 0 && len(errs) > 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("plural"), names.Plural, strings.Join(errs, ",")))
	}
	if errs := utilvalidation.IsDNS1035Label(names.Singular); len(names.Singular) > 0 && len(errs) > 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("singular"), names.Singular, strings.Join(errs, ",")))
	}
	if errs := utilvalidation.IsDNS1035Label(strings.ToLower(names.Kind)); len(names.Kind) > 0 && len(errs) > 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("kind"), names.Kind, "may have mixed case, but should otherwise match: "+strings.Join(errs, ",")))
	}
	if errs := utilvalidation.IsDNS1035Label(strings.ToLower(names.ListKind)); len(names.ListKind) > 0 && len(errs) > 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("listKind"), names.ListKind, "may have mixed case, but should otherwise match: "+strings.Join(errs, ",")))
	}

	for i, shortName := range names.ShortNames {
		if errs := utilvalidation.IsDNS1035Label(shortName); len(errs) > 0 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("shortNames").Index(i), shortName, strings.Join(errs, ",")))
		}
	}

	// kind 与 listKind 不能相同，否则资源对象和列表对象的解析会产生歧义。
	if len(names.Kind) > 0 && names.Kind == names.ListKind {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("listKind"), names.ListKind, "kind and listKind may not be the same"))
	}

	for i, category := range names.Categories {
		if errs := utilvalidation.IsDNS1035Label(category); len(errs) > 0 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("categories").Index(i), category, strings.Join(errs, ",")))
		}
	}

	return allErrs
}

// ValidateCustomResourceColumnDefinition 静态校验一条 AdditionalPrinterColumn 打印列定义。
func ValidateCustomResourceColumnDefinition(col *apiextensions.CustomResourceColumnDefinition, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if len(col.Name) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("name"), ""))
	}

	if len(col.Type) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("type"), fmt.Sprintf("must be one of %s", strings.Join(printerColumnDatatypes.List(), ","))))
	} else if !printerColumnDatatypes.Has(col.Type) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("type"), col.Type, fmt.Sprintf("must be one of %s", strings.Join(printerColumnDatatypes.List(), ","))))
	}

	if len(col.Format) > 0 && !customResourceColumnDefinitionFormats.Has(col.Format) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("format"), col.Format, fmt.Sprintf("must be one of %s", strings.Join(customResourceColumnDefinitionFormats.List(), ","))))
	}

	if len(col.JSONPath) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("JSONPath"), ""))
	} else if errs := validateSimpleJSONPath(col.JSONPath, fldPath.Child("JSONPath")); len(errs) > 0 {
		allErrs = append(allErrs, errs...)
	}

	return allErrs
}

func ValidateCustomResourceSelectableFields(selectableFields []apiextensions.SelectableField, schema *structuralschema.Structural, fldPath *field.Path, opts validationOptions, version string) (allErrs field.ErrorList) {
	uniqueSelectableFields := sets.New[string]()
	for i, selectableField := range selectableFields {
		indexFldPath := fldPath.Index(i)
		if len(selectableField.JSONPath) == 0 {
			allErrs = append(allErrs, field.Required(indexFldPath.Child("jsonPath"), ""))
			continue
		}
		// 复用最初为 CEL 功能构建的字段路径校验逻辑。
		path, foundSchema, err := cel.ValidFieldPath(selectableField.JSONPath, schema, cel.WithFieldPathAllowArrayNotation(false))
		if err != nil {
			allErrs = append(allErrs, field.Invalid(indexFldPath.Child("jsonPath"), selectableField.JSONPath, fmt.Sprintf("is an invalid path: %v", err)))
			continue
		}
		if path.Root().String() == "metadata" {
			allErrs = append(allErrs, field.Invalid(indexFldPath.Child("jsonPath"), selectableField.JSONPath, "must not point to fields in metadata"))
		}
		if !allowedSelectableFieldSchema(foundSchema) {
			allErrs = append(allErrs, field.Invalid(indexFldPath.Child("jsonPath"), selectableField.JSONPath, "must point to a field of type string, boolean or integer. Enum string fields and strings with formats are allowed."))
		}
		if uniqueSelectableFields.Has(path.String()) {
			allErrs = append(allErrs, field.Duplicate(indexFldPath.Child("jsonPath"), selectableField.JSONPath))
		} else {
			uniqueSelectableFields.Insert(path.String())
		}
	}
	uniqueSelectableFieldCount := uniqueSelectableFields.Len()
	if uniqueSelectableFieldCount > MaxSelectableFields && !opts.allowTooManySelectableFields[version] {
		allErrs = append(allErrs, field.TooMany(fldPath, uniqueSelectableFieldCount, MaxSelectableFields))
	}
	return allErrs
}

func allowedSelectableFieldSchema(schema *structuralschema.Structural) bool {
	if schema == nil {
		return false
	}
	switch schema.Type {
	case "string", "boolean", "integer":
		return true
	default:
		return false
	}
}

// specStandardValidator 根据不同 OpenAPI 规范版本应用相应的校验规则。
type specStandardValidator interface {
	validate(spec *apiextensions.JSONSchemaProps, fldPath *field.Path) field.ErrorList
	withForbiddenDefaults(reason string) specStandardValidator

	// insideResourceMeta 判断当前校验路径是否位于嵌入资源或顶层资源的 TypeMeta/ObjectMeta 中。
	insideResourceMeta() bool
	withInsideResourceMeta() specStandardValidator

	// forbidOldSelfValidations 返回无法在对象两个版本之间安全关联的最上层祖先路径；若不存在此类路径则返回 nil。
	forbidOldSelfValidations() *field.Path
	withForbidOldSelfValidations(path *field.Path) specStandardValidator
}

// validateCustomResourceDefinitionValidation 静态校验 CRD 的 OpenAPI schema 和 CEL 规则。
// 传入 context，以便校验默认值中的 CEL 表达式时支持取消操作。
func validateCustomResourceDefinitionValidation(ctx context.Context, customResourceValidation *apiextensions.CustomResourceValidation, statusSubresourceEnabled bool, opts validationOptions, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if customResourceValidation == nil {
		return allErrs
	}

	if schema := customResourceValidation.OpenAPIV3Schema; schema != nil {
		// 启用 Status 子资源时，根 schema 中只允许出现特定字段。
		// 这些字段的选择确保将 status 提取为 properties["status"] 后，不会丢失对应的校验规则。
		if statusSubresourceEnabled {
			v := reflect.ValueOf(schema).Elem()
			for i := 0; i < v.NumField(); i++ {
				// 跳过零值字段。
				if value := v.Field(i).Interface(); reflect.DeepEqual(value, reflect.Zero(reflect.TypeOf(value)).Interface()) {
					continue
				}

				fieldName := v.Type().Field(i).Name

				// 根 schema 只允许 object 类型，因为 Status 校验 schema 会从 properties["status"] 中单独提取。
				if fieldName == "Type" {
					if schema.Type != "object" {
						allErrs = append(allErrs, field.Invalid(fldPath.Child("openAPIV3Schema.type"), schema.Type, fmt.Sprintf(`only "object" is allowed as the type at the root of the schema if the status subresource is enabled`)))
						break
					}
					continue
				}

				if !allowedAtRootSchema(fieldName) {
					allErrs = append(allErrs, field.Invalid(fldPath.Child("openAPIV3Schema"), *schema, fmt.Sprintf(`only %v fields are allowed at the root of the schema if the status subresource is enabled`, allowedFieldsAtRootSchema)))
					break
				}
			}
		}

		if schema.Nullable {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("openAPIV3Schema.nullable"), fmt.Sprintf(`nullable cannot be true at the root`)))
		}

		openAPIV3Schema := &specStandardValidatorV3{
			allowDefaults:            opts.allowDefaults,
			disallowDefaultsReason:   opts.disallowDefaultsReason,
			requireValidPropertyType: opts.requireValidPropertyType,
		}

		var celContext *CELSchemaContext
		var structuralSchemaInitErrs field.ErrorList
		if opts.requireStructuralSchema {
			if ss, err := structuralschema.NewStructural(schema); err != nil {
				// 这些错误与 OpenAPISchema 校验错误存在重叠，因此单独记录；
				// 只有 OpenAPISchema 校验未报告错误时才展示。
				structuralSchemaInitErrs = append(structuralSchemaInitErrs, field.Invalid(fldPath.Child("openAPIV3Schema"), "", err.Error()))
			} else if validationErrors := structuralschema.ValidateStructural(fldPath.Child("openAPIV3Schema"), ss); len(validationErrors) > 0 {
				allErrs = append(allErrs, validationErrors...)
			} else if validationErrors, err := structuraldefaulting.ValidateDefaults(ctx, fldPath.Child("openAPIV3Schema"), ss, true, opts.requirePrunedDefaults); err != nil {
				// 正常情况下不应进入此分支。
				allErrs = append(allErrs, field.Invalid(fldPath.Child("openAPIV3Schema"), "", err.Error()))
			} else if len(validationErrors) > 0 {
				allErrs = append(allErrs, validationErrors...)
			} else {
				// 只有 Structural Schema 合法时才初始化 CEL 规则校验上下文。
				// CELSchemaContext 为 nil 表示不应尝试执行 CEL 校验。
				celContext = RootCELContext(schema)
			}
		}
		allErrs = append(allErrs, ValidateCustomResourceDefinitionOpenAPISchema(schema, fldPath.Child("openAPIV3Schema"), openAPIV3Schema, true, &opts, celContext).AllErrors()...)

		if len(allErrs) == 0 && len(structuralSchemaInitErrs) > 0 {
			// Structural Schema 初始化错误可能与 OpenAPISchema 校验错误重叠，
			// 因此仅在没有 OpenAPISchema 校验错误时展示。
			allErrs = append(allErrs, structuralSchemaInitErrs...)
		}

		if celContext != nil && celContext.TotalCost != nil {
			if celContext.TotalCost.Total > StaticEstimatedCRDCostLimit {
				for _, expensive := range celContext.TotalCost.MostExpensive {
					costErrorMsg := fmt.Sprintf("contributed to estimated rule cost total exceeding cost limit for entire OpenAPIv3 schema")
					allErrs = append(allErrs, field.Forbidden(expensive.Path, costErrorMsg))
				}

				costErrorMsg := getCostErrorMessage("x-kubernetes-validations estimated rule cost total for entire OpenAPIv3 schema", celContext.TotalCost.Total, StaticEstimatedCRDCostLimit)
				allErrs = append(allErrs, field.Forbidden(fldPath.Child("openAPIV3Schema"), costErrorMsg))
			}
		}
	}

	// 如果前面的校验均通过，还需确认能够根据该自定义资源校验配置成功构建 schema validator。
	if len(allErrs) == 0 {
		if _, _, err := apiservervalidation.NewSchemaValidator(customResourceValidation.OpenAPIV3Schema); err != nil {
			allErrs = append(allErrs, field.Invalid(fldPath, "", fmt.Sprintf("error building validator: %v", err)))
		}
	}
	return allErrs
}

var metaFields = sets.NewString("metadata", "kind", "apiVersion")

// OpenAPISchemaErrorList 汇总 ValidateCustomResourceDefinitionOpenAPISchema 报告的所有错误，
// 并将 CEL 相关错误与 schema 结构错误分别保存。
type OpenAPISchemaErrorList struct {
	SchemaErrors field.ErrorList
	CELErrors    field.ErrorList
}

// AppendErrors 将传入错误列表中的错误追加到当前列表。
func (o *OpenAPISchemaErrorList) AppendErrors(list *OpenAPISchemaErrorList) {
	if o == nil || list == nil {
		return
	}
	o.SchemaErrors = append(o.SchemaErrors, list.SchemaErrors...)
	o.CELErrors = append(o.CELErrors, list.CELErrors...)
}

// AllErrors 返回同时包含 schema 错误和 CEL 错误的列表。
func (o *OpenAPISchemaErrorList) AllErrors() field.ErrorList {
	if o == nil {
		return field.ErrorList{}
	}
	return append(o.SchemaErrors, o.CELErrors...)
}

// ValidateCustomResourceDefinitionOpenAPISchema 静态校验 CRD 的 OpenAPI schema。
func ValidateCustomResourceDefinitionOpenAPISchema(schema *apiextensions.JSONSchemaProps, fldPath *field.Path, ssv specStandardValidator, isRoot bool, opts *validationOptions, celContext *CELSchemaContext) *OpenAPISchemaErrorList {
	allErrs := &OpenAPISchemaErrorList{SchemaErrors: field.ErrorList{}, CELErrors: field.ErrorList{}}

	if schema == nil {
		return allErrs
	}
	allErrs.SchemaErrors = append(allErrs.SchemaErrors, ssv.validate(schema, fldPath)...)

	if schema.UniqueItems {
		allErrs.SchemaErrors = append(allErrs.SchemaErrors, field.Forbidden(fldPath.Child("uniqueItems"), "uniqueItems cannot be set to true since the runtime complexity becomes quadratic"))
	}

	// additionalProperties 与 properties 不能同时设置，
	// 否则会与 Kubernetes 忽略未知字段的 API 约定冲突。
	//
	// 换言之：
	// - properties 用于描述结构体字段；
	// - additionalProperties 用于描述 map[string]interface{}。
	//
	// 注意：未来若 OpenAPI 增加 patternProperties，也需要
	// 像 additionalProperties 一样限制其使用范围。
	if schema.AdditionalProperties != nil {
		if len(schema.Properties) != 0 {
			if !schema.AdditionalProperties.Allows || schema.AdditionalProperties.Schema != nil {
				allErrs.SchemaErrors = append(allErrs.SchemaErrors, field.Forbidden(fldPath.Child("additionalProperties"), "additionalProperties and properties are mutual exclusive"))
			}
		}
		// 注意：无论嵌入资源还是顶层资源，根节点都禁止使用 additionalProperties。
		// 但在更深层的字段中仍可使用，例如 labels 或 annotations。
		subSsv := ssv
		if ssv.insideResourceMeta() {
			// additionalProperties 内必须禁止设置默认值，因为缺少实际值时无法明确执行字段裁剪。
			subSsv = ssv.withForbiddenDefaults("inside additionalProperties applying to object metadata")
		}
		allErrs.AppendErrors(ValidateCustomResourceDefinitionOpenAPISchema(schema.AdditionalProperties.Schema, fldPath.Child("additionalProperties"), subSsv, false, opts, celContext.ChildAdditionalPropertiesContext(schema.AdditionalProperties.Schema)))
	}

	if len(schema.Properties) != 0 {
		for property, jsonSchema := range schema.Properties {
			subSsv := ssv

			if !cel.MapIsCorrelatable(schema.XMapType) {
				subSsv = subSsv.withForbidOldSelfValidations(fldPath)
			}

			if (isRoot || schema.XEmbeddedResource) && metaFields.Has(property) {
				// 递归校验适用于 ObjectMeta 的 schema。
				subSsv = subSsv.withInsideResourceMeta()
				if isRoot {
					subSsv = subSsv.withForbiddenDefaults(fmt.Sprintf("in top-level %s", property))
				}
			}
			propertySchema := jsonSchema
			allErrs.AppendErrors(ValidateCustomResourceDefinitionOpenAPISchema(&propertySchema, fldPath.Child("properties").Key(property), subSsv, false, opts, celContext.ChildPropertyContext(&propertySchema, property)))
		}
	}

	allErrs.AppendErrors(ValidateCustomResourceDefinitionOpenAPISchema(schema.Not, fldPath.Child("not"), ssv, false, opts, nil))

	if len(schema.AllOf) != 0 {
		for i, jsonSchema := range schema.AllOf {
			allOfSchema := jsonSchema
			allErrs.AppendErrors(ValidateCustomResourceDefinitionOpenAPISchema(&allOfSchema, fldPath.Child("allOf").Index(i), ssv, false, opts, nil))
		}
	}

	if len(schema.OneOf) != 0 {
		for i, jsonSchema := range schema.OneOf {
			oneOfSchema := jsonSchema
			allErrs.AppendErrors(ValidateCustomResourceDefinitionOpenAPISchema(&oneOfSchema, fldPath.Child("oneOf").Index(i), ssv, false, opts, nil))
		}
	}

	if len(schema.AnyOf) != 0 {
		for i, jsonSchema := range schema.AnyOf {
			anyOfSchema := jsonSchema
			allErrs.AppendErrors(ValidateCustomResourceDefinitionOpenAPISchema(&anyOfSchema, fldPath.Child("anyOf").Index(i), ssv, false, opts, nil))
		}
	}

	if len(schema.Definitions) != 0 {
		for definition, jsonSchema := range schema.Definitions {
			definitionSchema := jsonSchema
			allErrs.AppendErrors(ValidateCustomResourceDefinitionOpenAPISchema(&definitionSchema, fldPath.Child("definitions").Key(definition), ssv, false, opts, nil))
		}
	}

	if schema.Items != nil {
		subSsv := ssv

		// 只有 map 和 set 列表能够关联新旧对象中的元素；
		// CEL（x-kubernetes-validations）规则不支持按身份关联 set 元素。
		// 未设置列表类型时，默认按 atomic 处理。
		if schema.XListType == nil || *schema.XListType != "map" {
			subSsv = subSsv.withForbidOldSelfValidations(fldPath)
		}

		allErrs.AppendErrors(ValidateCustomResourceDefinitionOpenAPISchema(schema.Items.Schema, fldPath.Child("items"), subSsv, false, opts, celContext.ChildItemsContext(schema.Items.Schema)))
		if len(schema.Items.JSONSchemas) != 0 {
			for i, jsonSchema := range schema.Items.JSONSchemas {
				itemsSchema := jsonSchema
				allErrs.AppendErrors(ValidateCustomResourceDefinitionOpenAPISchema(&itemsSchema, fldPath.Child("items").Index(i), subSsv, false, opts, celContext.ChildItemsContext(&itemsSchema)))
			}
		}
	}

	if schema.Dependencies != nil {
		for dependency, jsonSchemaPropsOrStringArray := range schema.Dependencies {
			allErrs.AppendErrors(ValidateCustomResourceDefinitionOpenAPISchema(jsonSchemaPropsOrStringArray.Schema, fldPath.Child("dependencies").Key(dependency), ssv, false, opts, nil))
		}
	}

	if schema.XPreserveUnknownFields != nil && !*schema.XPreserveUnknownFields {
		allErrs.SchemaErrors = append(allErrs.SchemaErrors, field.Invalid(fldPath.Child("x-kubernetes-preserve-unknown-fields"), *schema.XPreserveUnknownFields, "must be true or undefined"))
	}

	if schema.XMapType != nil && schema.Type != "object" {
		if len(schema.Type) == 0 {
			allErrs.SchemaErrors = append(allErrs.SchemaErrors, field.Required(fldPath.Child("type"), "must be object if x-kubernetes-map-type is specified"))
		} else {
			allErrs.SchemaErrors = append(allErrs.SchemaErrors, field.Invalid(fldPath.Child("type"), schema.Type, "must be object if x-kubernetes-map-type is specified"))
		}
	}

	if schema.XMapType != nil && *schema.XMapType != "atomic" && *schema.XMapType != "granular" {
		allErrs.SchemaErrors = append(allErrs.SchemaErrors, field.NotSupported(fldPath.Child("x-kubernetes-map-type"), *schema.XMapType, []string{"atomic", "granular"}))
	}

	if schema.XListType != nil && schema.Type != "array" {
		if len(schema.Type) == 0 {
			allErrs.SchemaErrors = append(allErrs.SchemaErrors, field.Required(fldPath.Child("type"), "must be array if x-kubernetes-list-type is specified"))
		} else {
			allErrs.SchemaErrors = append(allErrs.SchemaErrors, field.Invalid(fldPath.Child("type"), schema.Type, "must be array if x-kubernetes-list-type is specified"))
		}
	} else if opts.requireAtomicSetType && schema.XListType != nil && *schema.XListType == "set" && schema.Items != nil && schema.Items.Schema != nil { // by structural schema items are present
		is := schema.Items.Schema
		switch is.Type {
		case "array":
			if is.XListType != nil && *is.XListType != "atomic" { // atomic is the implicit default behaviour if unset, hence != atomic is wrong
				allErrs.SchemaErrors = append(allErrs.SchemaErrors, field.Invalid(fldPath.Child("items").Child("x-kubernetes-list-type"), is.XListType, "must be atomic as item of a list with x-kubernetes-list-type=set"))
			}
		case "object":
			if is.XMapType == nil || *is.XMapType != "atomic" { // granular is the implicit default behaviour if unset, hence nil and != atomic are wrong
				allErrs.SchemaErrors = append(allErrs.SchemaErrors, field.Invalid(fldPath.Child("items").Child("x-kubernetes-map-type"), is.XListType, "must be atomic as item of a list with x-kubernetes-list-type=set"))
			}
		}
	}

	if schema.XListType != nil && *schema.XListType != "atomic" && *schema.XListType != "set" && *schema.XListType != "map" {
		allErrs.SchemaErrors = append(allErrs.SchemaErrors, field.NotSupported(fldPath.Child("x-kubernetes-list-type"), *schema.XListType, []string{"atomic", "set", "map"}))
	}

	if len(schema.XListMapKeys) > 0 {
		if schema.XListType == nil {
			allErrs.SchemaErrors = append(allErrs.SchemaErrors, field.Required(fldPath.Child("x-kubernetes-list-type"), "must be map if x-kubernetes-list-map-keys is non-empty"))
		} else if *schema.XListType != "map" {
			allErrs.SchemaErrors = append(allErrs.SchemaErrors, field.Invalid(fldPath.Child("x-kubernetes-list-type"), *schema.XListType, "must be map if x-kubernetes-list-map-keys is non-empty"))
		}
	}

	if schema.XListType != nil && *schema.XListType == "map" {
		if len(schema.XListMapKeys) == 0 {
			allErrs.SchemaErrors = append(allErrs.SchemaErrors, field.Required(fldPath.Child("x-kubernetes-list-map-keys"), "must not be empty if x-kubernetes-list-type is map"))
		}

		if schema.Items == nil {
			allErrs.SchemaErrors = append(allErrs.SchemaErrors, field.Required(fldPath.Child("items"), "must have a schema if x-kubernetes-list-type is map"))
		}

		if schema.Items != nil && schema.Items.Schema == nil {
			allErrs.SchemaErrors = append(allErrs.SchemaErrors, field.Invalid(fldPath.Child("items"), schema.Items, "must only have a single schema if x-kubernetes-list-type is map"))
		}

		if schema.Items != nil && schema.Items.Schema != nil && schema.Items.Schema.Type != "object" {
			allErrs.SchemaErrors = append(allErrs.SchemaErrors, field.Invalid(fldPath.Child("items").Child("type"), schema.Items.Schema.Type, "must be object if parent array's x-kubernetes-list-type is map"))
		}

		if schema.Items != nil && schema.Items.Schema != nil && schema.Items.Schema.Type == "object" {
			keys := map[string]struct{}{}
			for _, k := range schema.XListMapKeys {
				if s, ok := schema.Items.Schema.Properties[k]; ok {
					if s.Type == "array" || s.Type == "object" {
						allErrs.SchemaErrors = append(allErrs.SchemaErrors, field.Invalid(fldPath.Child("items").Child("properties").Key(k).Child("type"), schema.Items.Schema.Type, "must be a scalar type if parent array's x-kubernetes-list-type is map"))
					}
				} else {
					allErrs.SchemaErrors = append(allErrs.SchemaErrors, field.Invalid(fldPath.Child("x-kubernetes-list-map-keys"), schema.XListMapKeys, "entries must all be names of item properties"))
				}
				if _, ok := keys[k]; ok {
					allErrs.SchemaErrors = append(allErrs.SchemaErrors, field.Invalid(fldPath.Child("x-kubernetes-list-map-keys"), schema.XListMapKeys, "must not contain duplicate entries"))
				}
				keys[k] = struct{}{}
			}
		}
	}

	if opts.requireMapListKeysMapSetValidation {
		allErrs.SchemaErrors = append(allErrs.SchemaErrors, validateMapListKeysMapSet(schema, fldPath)...)
	}
	if len(schema.XValidations) > 0 {
		for i, rule := range schema.XValidations {
			trimmedRule := strings.TrimSpace(rule.Rule)
			trimmedMsg := strings.TrimSpace(rule.Message)
			trimmedMsgExpr := strings.TrimSpace(rule.MessageExpression)
			if len(trimmedRule) == 0 {
				allErrs.SchemaErrors = append(allErrs.SchemaErrors, field.Required(fldPath.Child("x-kubernetes-validations").Index(i).Child("rule"), "rule is not specified"))
			} else if len(rule.Message) > 0 && len(trimmedMsg) == 0 {
				allErrs.SchemaErrors = append(allErrs.SchemaErrors, field.Invalid(fldPath.Child("x-kubernetes-validations").Index(i).Child("message"), rule.Message, "must be non-empty if specified"))
			} else if hasNewlines(trimmedMsg) {
				allErrs.SchemaErrors = append(allErrs.SchemaErrors, field.Invalid(fldPath.Child("x-kubernetes-validations").Index(i).Child("message"), rule.Message, "must not contain line breaks"))
			} else if hasNewlines(trimmedRule) && len(trimmedMsg) == 0 {
				allErrs.SchemaErrors = append(allErrs.SchemaErrors, field.Required(fldPath.Child("x-kubernetes-validations").Index(i).Child("message"), "message must be specified if rule contains line breaks"))
			}
			if len(rule.MessageExpression) > 0 && len(trimmedMsgExpr) == 0 {
				allErrs.SchemaErrors = append(allErrs.SchemaErrors, field.Required(fldPath.Child("x-kubernetes-validations").Index(i).Child("messageExpression"), "messageExpression must be non-empty if specified"))
			}
			if rule.Reason != nil && !supportedValidationReason.Has(string(*rule.Reason)) {
				allErrs.SchemaErrors = append(allErrs.SchemaErrors, field.NotSupported(fldPath.Child("x-kubernetes-validations").Index(i).Child("reason"), *rule.Reason, supportedValidationReason.List()))
			}
			trimmedFieldPath := strings.TrimSpace(rule.FieldPath)
			if len(rule.FieldPath) > 0 && len(trimmedFieldPath) == 0 {
				allErrs.SchemaErrors = append(allErrs.SchemaErrors, field.Invalid(fldPath.Child("x-kubernetes-validations").Index(i).Child("fieldPath"), rule.FieldPath, "must be non-empty if specified"))
			}
			if hasNewlines(rule.FieldPath) {
				allErrs.SchemaErrors = append(allErrs.SchemaErrors, field.Invalid(fldPath.Child("x-kubernetes-validations").Index(i).Child("fieldPath"), rule.FieldPath, "must not contain line breaks"))
			}
			if len(rule.FieldPath) > 0 {
				if !pathValid(schema, rule.FieldPath) {
					allErrs.SchemaErrors = append(allErrs.SchemaErrors, field.Invalid(fldPath.Child("x-kubernetes-validations").Index(i).Child("fieldPath"), rule.FieldPath, "must be a valid path"))
				}

			}
		}

		// 如果当前层级或更深层级已经发现 schema 校验错误，则跳过 CEL 表达式校验。
		// 无效的 OpenAPI schema 不一定能转换为合法的 CEL DeclTypes；继续校验可能产生
		// 无法采取有效措施的错误（修复 schema 问题后这些错误就会消失），
		// 也会增加 CEL 表达式编写者的理解难度。
		if len(allErrs.SchemaErrors) == 0 && celContext != nil {
			typeInfo, err := celContext.TypeInfo()
			if err != nil {
				allErrs.CELErrors = append(allErrs.CELErrors, field.InternalError(fldPath.Child("x-kubernetes-validations"), fmt.Errorf("internal error: failed to construct type information for x-kubernetes-validations rules: %s", err)))
			} else if typeInfo == nil {
				allErrs.CELErrors = append(allErrs.CELErrors, field.InternalError(fldPath.Child("x-kubernetes-validations"), fmt.Errorf("internal error: failed to retrieve type information for x-kubernetes-validations")))
			} else {
				compResults, err := cel.Compile(typeInfo.Schema, typeInfo.DeclType, celconfig.PerCallLimit, opts.celEnvironmentSet, opts.preexistingExpressions)
				if err != nil {
					allErrs.CELErrors = append(allErrs.CELErrors, field.InternalError(fldPath.Child("x-kubernetes-validations"), err))
				} else {
					for i, cr := range compResults {
						expressionCost := getExpressionCost(cr, celContext)
						if !opts.suppressPerExpressionCost && expressionCost > StaticEstimatedCostLimit {
							costErrorMsg := getCostErrorMessage("estimated rule cost", expressionCost, StaticEstimatedCostLimit)
							allErrs.CELErrors = append(allErrs.CELErrors, field.Forbidden(fldPath.Child("x-kubernetes-validations").Index(i).Child("rule"), costErrorMsg))
						}
						if celContext.TotalCost != nil {
							celContext.TotalCost.ObserveExpressionCost(fldPath.Child("x-kubernetes-validations").Index(i).Child("rule"), expressionCost)
						}
						if cr.Error != nil {
							if cr.Error.Type == apiservercel.ErrorTypeRequired {
								allErrs.CELErrors = append(allErrs.CELErrors, field.Required(fldPath.Child("x-kubernetes-validations").Index(i).Child("rule"), cr.Error.Detail))
							} else {
								allErrs.CELErrors = append(allErrs.CELErrors, field.Invalid(fldPath.Child("x-kubernetes-validations").Index(i).Child("rule"), schema.XValidations[i], cr.Error.Detail))
							}
						}
						if cr.MessageExpressionError != nil {
							allErrs.CELErrors = append(allErrs.CELErrors, field.Invalid(fldPath.Child("x-kubernetes-validations").Index(i).Child("messageExpression"), schema.XValidations[i], cr.MessageExpressionError.Detail))
						} else {
							if cr.MessageExpression != nil {
								if !opts.suppressPerExpressionCost && cr.MessageExpressionMaxCost > StaticEstimatedCostLimit {
									costErrorMsg := getCostErrorMessage("estimated messageExpression cost", cr.MessageExpressionMaxCost, StaticEstimatedCostLimit)
									allErrs.CELErrors = append(allErrs.CELErrors, field.Forbidden(fldPath.Child("x-kubernetes-validations").Index(i).Child("messageExpression"), costErrorMsg))
								}
								if celContext.TotalCost != nil {
									celContext.TotalCost.ObserveExpressionCost(fldPath.Child("x-kubernetes-validations").Index(i).Child("messageExpression"), cr.MessageExpressionMaxCost)
								}
							}
						}
						if cr.UsesOldSelf {
							if uncorrelatablePath := ssv.forbidOldSelfValidations(); uncorrelatablePath != nil {
								allErrs.CELErrors = append(allErrs.CELErrors, field.Invalid(fldPath.Child("x-kubernetes-validations").Index(i).Child("rule"), schema.XValidations[i].Rule, fmt.Sprintf("oldSelf cannot be used on the uncorrelatable portion of the schema within %v", uncorrelatablePath)))
							}
						} else if schema.XValidations[i].OptionalOldSelf != nil {
							allErrs.CELErrors = append(allErrs.CELErrors, field.Invalid(fldPath.Child("x-kubernetes-validations").Index(i).Child("optionalOldSelf"), *schema.XValidations[i].OptionalOldSelf, "may not be set if oldSelf is not used in rule"))
						}
					}
				}
			}
		}
	}

	return allErrs
}

func pathValid(schema *apiextensions.JSONSchemaProps, path string) bool {
	// 为避免重复代码并便于维护，统一使用 ValidateFieldPath 函数检查路径是否合法。
	if ss, err := structuralschema.NewStructural(schema); err == nil {
		_, _, err := cel.ValidFieldPath(path, ss)
		return err == nil
	}
	return true
}

// multiplyWithOverflowGuard 返回 baseCost 与 cardinality 的乘积；
// 若乘积会超过 math.MaxUint，则返回 math.MaxUint。
func multiplyWithOverflowGuard(baseCost, cardinality uint64) uint64 {
	if baseCost == 0 {
		// 空规则可能返回 0，因此在此处进行保护处理。
		return 0
	} else if math.MaxUint/baseCost < cardinality {
		return math.MaxUint
	}
	return baseCost * cardinality
}

func getExpressionCost(cr cel.CompilationResult, cardinalityCost *CELSchemaContext) uint64 {
	if cardinalityCost.MaxCardinality != unbounded {
		return multiplyWithOverflowGuard(cr.MaxCost, *cardinalityCost.MaxCardinality)
	}
	return multiplyWithOverflowGuard(cr.MaxCost, cr.MaxCardinality)
}

func getCostErrorMessage(costName string, expressionCost, costLimit uint64) string {
	exceedFactor := float64(expressionCost) / float64(costLimit)
	var factor string
	if exceedFactor > 100.0 {
		// 如果 exceedFactor 超过两个数量级，该规则很可能达到 O(n²) 或更高复杂度，
		// 若不设置限制通常无法通过校验。
		// 在这种情况下，成本估算值通常已经足够大，进一步估算也不会带来额外价值。
		factor = fmt.Sprintf("more than 100x")
	} else if exceedFactor < 1.5 {
		factor = fmt.Sprintf("%fx", exceedFactor) // avoid reporting "exceeds budge by a factor of 1.0x"
	} else {
		factor = fmt.Sprintf("%.1fx", exceedFactor)
	}
	return fmt.Sprintf("%s exceeds budget by factor of %s (try simplifying the rule, or adding maxItems, maxProperties, and maxLength where arrays, maps, and strings are declared)", costName, factor)
}

var newlineMatcher = regexp.MustCompile(`[\n\r]+`) // valid newline chars in CEL grammar
func hasNewlines(s string) bool {
	return newlineMatcher.MatchString(s)
}

func validateMapListKeysMapSet(schema *apiextensions.JSONSchemaProps, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if schema.Items == nil || schema.Items.Schema == nil {
		return nil
	}
	if schema.XListType == nil {
		return nil
	}
	if *schema.XListType != "set" && *schema.XListType != "map" {
		return nil
	}

	// set 和 map 列表的元素不能为 nullable。
	if schema.Items.Schema.Nullable {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("items").Child("nullable"), "cannot be nullable when x-kubernetes-list-type is "+*schema.XListType))
	}

	switch *schema.XListType {
	case "map":
		// 确保所有 map 键字段均为必填项或提供了默认值。
		isRequired := make(map[string]bool, len(schema.Items.Schema.Required))
		for _, required := range schema.Items.Schema.Required {
			isRequired[required] = true
		}

		for _, k := range schema.XListMapKeys {
			obj, ok := schema.Items.Schema.Properties[k]
			if !ok {
				// ValidateCustomResourceDefinitionOpenAPISchema 已检查所有 XListMapKeys 是否对应现有属性，因此这里可以跳过重复检查。
				continue
			}

			if isRequired[k] == false && obj.Default == nil {
				allErrs = append(allErrs, field.Required(fldPath.Child("items").Child("properties").Key(k).Child("default"), "this property is in x-kubernetes-list-map-keys, so it must have a default or be a required property"))
			}

			if obj.Nullable {
				allErrs = append(allErrs, field.Forbidden(fldPath.Child("items").Child("properties").Key(k).Child("nullable"), "this property is in x-kubernetes-list-map-keys, so it cannot be nullable"))
			}
		}
	case "set":
		// 没有其他针对 set 列表的专属校验。
	}

	return allErrs
}

type specStandardValidatorV3 struct {
	allowDefaults                       bool
	disallowDefaultsReason              string
	isInsideResourceMeta                bool
	requireValidPropertyType            bool
	uncorrelatableOldSelfValidationPath *field.Path
}

func (v *specStandardValidatorV3) withForbiddenDefaults(reason string) specStandardValidator {
	clone := *v
	clone.disallowDefaultsReason = reason
	clone.allowDefaults = false
	return &clone
}

func (v *specStandardValidatorV3) withInsideResourceMeta() specStandardValidator {
	clone := *v
	clone.isInsideResourceMeta = true
	return &clone
}

func (v *specStandardValidatorV3) insideResourceMeta() bool {
	return v.isInsideResourceMeta
}

func (v *specStandardValidatorV3) withForbidOldSelfValidations(path *field.Path) specStandardValidator {
	if v.uncorrelatableOldSelfValidationPath != nil {
		// oldSelf 校验已被禁止；保留导致该限制生效的最高层级路径。
		return v
	}
	clone := *v
	clone.uncorrelatableOldSelfValidationPath = path
	return &clone
}

func (v *specStandardValidatorV3) forbidOldSelfValidations() *field.Path {
	return v.uncorrelatableOldSelfValidationPath
}

// validate 根据 OpenAPI Schema v3 执行校验。
func (v *specStandardValidatorV3) validate(schema *apiextensions.JSONSchemaProps, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if schema == nil {
		return allErrs
	}

	//
	// WARNING：如果在下方新增允许的字段或结构，必须同步调整 NewStructural 以支持这些内容。
	//

	if v.requireValidPropertyType && len(schema.Type) > 0 && !openapiV3Types.Has(schema.Type) {
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("type"), schema.Type, openapiV3Types.List()))
	}

	if schema.Default != nil && !v.allowDefaults {
		detail := "must not be set"
		if len(v.disallowDefaultsReason) > 0 {
			detail += " " + v.disallowDefaultsReason
		}
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("default"), detail))
	}

	if schema.ID != "" {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("id"), "id is not supported"))
	}

	if schema.AdditionalItems != nil {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("additionalItems"), "additionalItems is not supported"))
	}

	if len(schema.PatternProperties) != 0 {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("patternProperties"), "patternProperties is not supported"))
	}

	if len(schema.Definitions) != 0 {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("definitions"), "definitions is not supported"))
	}

	if schema.Dependencies != nil {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("dependencies"), "dependencies is not supported"))
	}

	if schema.Ref != nil {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("$ref"), "$ref is not supported"))
	}

	if schema.Type == "null" {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("type"), "type cannot be set to null, use nullable as an alternative"))
	}

	if schema.Items != nil && len(schema.Items.JSONSchemas) != 0 {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("items"), "items must be a schema object and not an array"))
	}

	if v.isInsideResourceMeta && schema.XEmbeddedResource {
		allErrs = append(allErrs, field.Forbidden(fldPath.Child("x-kubernetes-embedded-resource"), "must not be used inside of resource meta"))
	}

	return allErrs
}

// ValidateCustomResourceDefinitionSubresources 静态校验 CRD 的 Status、Scale 等子资源配置。
func ValidateCustomResourceDefinitionSubresources(subresources *apiextensions.CustomResourceSubresources, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if subresources == nil {
		return allErrs
	}

	if subresources.Scale != nil {
		if len(subresources.Scale.SpecReplicasPath) == 0 {
			allErrs = append(allErrs, field.Required(fldPath.Child("scale.specReplicasPath"), ""))
		} else {
			// 必须是 .spec 下受约束的 JSON 路径。
			if errs := validateSimpleJSONPath(subresources.Scale.SpecReplicasPath, fldPath.Child("scale.specReplicasPath")); len(errs) > 0 {
				allErrs = append(allErrs, errs...)
			} else if !strings.HasPrefix(subresources.Scale.SpecReplicasPath, ".spec.") {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("scale.specReplicasPath"), subresources.Scale.SpecReplicasPath, "should be a json path under .spec"))
			}
		}

		if len(subresources.Scale.StatusReplicasPath) == 0 {
			allErrs = append(allErrs, field.Required(fldPath.Child("scale.statusReplicasPath"), ""))
		} else {
			// 必须是 .status 下受约束的 JSON 路径。
			if errs := validateSimpleJSONPath(subresources.Scale.StatusReplicasPath, fldPath.Child("scale.statusReplicasPath")); len(errs) > 0 {
				allErrs = append(allErrs, errs...)
			} else if !strings.HasPrefix(subresources.Scale.StatusReplicasPath, ".status.") {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("scale.statusReplicasPath"), subresources.Scale.StatusReplicasPath, "should be a json path under .status"))
			}
		}

		// 如果设置了 labelSelectorPath，则必须是 .status 下受约束的 JSON 路径。
		if subresources.Scale.LabelSelectorPath != nil && len(*subresources.Scale.LabelSelectorPath) > 0 {
			if errs := validateSimpleJSONPath(*subresources.Scale.LabelSelectorPath, fldPath.Child("scale.labelSelectorPath")); len(errs) > 0 {
				allErrs = append(allErrs, errs...)
			} else if !strings.HasPrefix(*subresources.Scale.LabelSelectorPath, ".spec.") && !strings.HasPrefix(*subresources.Scale.LabelSelectorPath, ".status.") {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("scale.labelSelectorPath"), subresources.Scale.LabelSelectorPath, "should be a json path under either .spec or .status"))
			}
		}
	}

	return allErrs
}

func validateSimpleJSONPath(s string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	switch {
	case len(s) == 0:
		allErrs = append(allErrs, field.Invalid(fldPath, s, "must not be empty"))
	case s[0] != '.':
		allErrs = append(allErrs, field.Invalid(fldPath, s, "must be a simple json path starting with ."))
	case s != ".":
		if cs := strings.Split(s[1:], "."); len(cs) < 1 {
			allErrs = append(allErrs, field.Invalid(fldPath, s, "must be a json path in the dot notation"))
		}
	}

	return allErrs
}

var allowedFieldsAtRootSchema = []string{"Description", "Type", "Format", "Title", "Maximum", "ExclusiveMaximum", "Minimum", "ExclusiveMinimum", "MaxLength", "MinLength", "Pattern", "MaxItems", "MinItems", "UniqueItems", "MultipleOf", "Required", "Items", "Properties", "ExternalDocs", "Example", "XPreserveUnknownFields", "XValidations"}

func allowedAtRootSchema(field string) bool {
	for _, v := range allowedFieldsAtRootSchema {
		if field == v {
			return true
		}
	}
	return false
}

// requireOpenAPISchema 判断请求对应的 API Group/Version 是否必须提供 schema。
func requireOpenAPISchema(oldCRDSpec *apiextensions.CustomResourceDefinitionSpec) bool {
	if oldCRDSpec != nil && !allVersionsSpecifyOpenAPISchema(oldCRDSpec) {
		// 不要收紧对已经持久化数据的校验要求，以保持升级兼容性。
		return false
	}
	return true
}
func allVersionsSpecifyOpenAPISchema(spec *apiextensions.CustomResourceDefinitionSpec) bool {
	if spec.Validation != nil && spec.Validation.OpenAPIV3Schema != nil {
		return true
	}
	for _, v := range spec.Versions {
		if v.Schema == nil || v.Schema.OpenAPIV3Schema == nil {
			return false
		}
	}
	return true
}

func specHasDefaults(spec *apiextensions.CustomResourceDefinitionSpec) bool {
	return HasSchemaWith(spec, schemaHasDefaults)
}

func schemaHasDefaults(s *apiextensions.JSONSchemaProps) bool {
	return SchemaHas(s, func(s *apiextensions.JSONSchemaProps) bool {
		return s.Default != nil
	})
}

func HasSchemaWith(spec *apiextensions.CustomResourceDefinitionSpec, pred func(s *apiextensions.JSONSchemaProps) bool) bool {
	if spec.Validation != nil && spec.Validation.OpenAPIV3Schema != nil && pred(spec.Validation.OpenAPIV3Schema) {
		return true
	}
	for _, v := range spec.Versions {
		if v.Schema != nil && v.Schema.OpenAPIV3Schema != nil && pred(v.Schema.OpenAPIV3Schema) {
			return true
		}
	}
	return false
}

var schemaPool = sync.Pool{
	New: func() any {
		return new(apiextensions.JSONSchemaProps)
	},
}

func schemaHasRecurse(s *apiextensions.JSONSchemaProps, pred func(s *apiextensions.JSONSchemaProps) bool) bool {
	if s == nil {
		return false
	}
	schema := schemaPool.Get().(*apiextensions.JSONSchemaProps)
	defer schemaPool.Put(schema)
	*schema = *s
	return SchemaHas(schema, pred)
}

// SchemaHas 递归遍历 Schema，并调用 pred
// 判断 schema 是否包含指定内容。
//
// pred 不得保留 JSON Schema 的副本，也不得修改传入的
// schema。
func SchemaHas(s *apiextensions.JSONSchemaProps, pred func(s *apiextensions.JSONSchemaProps) bool) bool {
	if s == nil {
		return false
	}

	if pred(s) {
		return true
	}

	if s.Items != nil {
		if s.Items != nil && schemaHasRecurse(s.Items.Schema, pred) {
			return true
		}
		for i := range s.Items.JSONSchemas {
			if schemaHasRecurse(&s.Items.JSONSchemas[i], pred) {
				return true
			}
		}
	}
	for i := range s.AllOf {
		if schemaHasRecurse(&s.AllOf[i], pred) {
			return true
		}
	}
	for i := range s.AnyOf {
		if schemaHasRecurse(&s.AnyOf[i], pred) {
			return true
		}
	}
	for i := range s.OneOf {
		if schemaHasRecurse(&s.OneOf[i], pred) {
			return true
		}
	}
	if schemaHasRecurse(s.Not, pred) {
		return true
	}
	for _, s := range s.Properties {
		if schemaHasRecurse(&s, pred) {
			return true
		}
	}
	if s.AdditionalProperties != nil {
		if schemaHasRecurse(s.AdditionalProperties.Schema, pred) {
			return true
		}
	}
	for _, s := range s.PatternProperties {
		if schemaHasRecurse(&s, pred) {
			return true
		}
	}
	if s.AdditionalItems != nil {
		if schemaHasRecurse(s.AdditionalItems.Schema, pred) {
			return true
		}
	}
	for _, s := range s.Definitions {
		if schemaHasRecurse(&s, pred) {
			return true
		}
	}
	for _, d := range s.Dependencies {
		if schemaHasRecurse(d.Schema, pred) {
			return true
		}
	}

	return false
}

func specHasKubernetesExtensions(spec *apiextensions.CustomResourceDefinitionSpec) bool {
	if spec.Validation != nil && schemaHasKubernetesExtensions(spec.Validation.OpenAPIV3Schema) {
		return true
	}
	for _, v := range spec.Versions {
		if v.Schema != nil && schemaHasKubernetesExtensions(v.Schema.OpenAPIV3Schema) {
			return true
		}
	}
	return false
}

func schemaHasKubernetesExtensions(s *apiextensions.JSONSchemaProps) bool {
	return SchemaHas(s, func(s *apiextensions.JSONSchemaProps) bool {
		return s.XEmbeddedResource || s.XPreserveUnknownFields != nil || s.XIntOrString || len(s.XListMapKeys) > 0 || s.XListType != nil || len(s.XValidations) > 0
	})
}

// requireStructuralSchema 判断指定 API 版本是否要求 schema 必须满足 Structural Schema 约束。
func requireStructuralSchema(oldCRDSpec *apiextensions.CustomResourceDefinitionSpec) bool {
	if oldCRDSpec != nil && specHasNonStructuralSchema(oldCRDSpec) {
		// 不要收紧对已经持久化数据的校验要求，以保持升级兼容性。
		return false
	}
	return true
}

func specHasNonStructuralSchema(spec *apiextensions.CustomResourceDefinitionSpec) bool {
	if spec.Validation != nil && schemaIsNonStructural(spec.Validation.OpenAPIV3Schema) {
		return true
	}
	for _, v := range spec.Versions {
		if v.Schema != nil && schemaIsNonStructural(v.Schema.OpenAPIV3Schema) {
			return true
		}
	}
	return false
}
func schemaIsNonStructural(schema *apiextensions.JSONSchemaProps) bool {
	if schema == nil {
		return false
	}
	ss, err := structuralschema.NewStructural(schema)
	if err != nil {
		return true
	}
	return len(structuralschema.ValidateStructural(nil, ss)) > 0
}

// requirePrunedDefaults：如果旧 CRD Spec 中存在尚未裁剪的默认值则返回 false，否则返回 true。
func requirePrunedDefaults(oldCRDSpec *apiextensions.CustomResourceDefinitionSpec) bool {
	if oldCRDSpec.Validation != nil {
		if has, err := schemaHasUnprunedDefaults(oldCRDSpec.Validation.OpenAPIV3Schema); err == nil && has {
			return false
		}
	}
	for _, v := range oldCRDSpec.Versions {
		if v.Schema == nil {
			continue
		}
		if has, err := schemaHasUnprunedDefaults(v.Schema.OpenAPIV3Schema); err == nil && has {
			return false
		}
	}
	return true
}
func schemaHasUnprunedDefaults(schema *apiextensions.JSONSchemaProps) (bool, error) {
	if schema == nil || !schemaHasDefaults(schema) {
		return false, nil
	}
	ss, err := structuralschema.NewStructural(schema)
	if err != nil {
		return false, err
	}
	if errs := structuralschema.ValidateStructural(nil, ss); len(errs) > 0 {
		return false, errs.ToAggregate()
	}
	pruned := ss.DeepCopy()
	if err := structuraldefaulting.PruneDefaults(pruned); err != nil {
		return false, err
	}
	return !reflect.DeepEqual(ss, pruned), nil
}

// requireAtomicSetType 判断旧 CRD Spec 是否包含至少一个元素类型非 atomic 的 x-kubernetes-list-type=set 列表。
func requireAtomicSetType(oldCRDSpec *apiextensions.CustomResourceDefinitionSpec) bool {
	return !HasSchemaWith(oldCRDSpec, hasNonAtomicSetType)
}

// hasNonAtomicSetType 递归遍历 schema，判断是否存在元素类型非 atomic 的 set 列表。
func hasNonAtomicSetType(schema *apiextensions.JSONSchemaProps) bool {
	return SchemaHas(schema, func(schema *apiextensions.JSONSchemaProps) bool {
		if schema.XListType != nil && *schema.XListType == "set" && schema.Items != nil && schema.Items.Schema != nil { // we don't support schema.Items.JSONSchemas
			is := schema.Items.Schema
			switch is.Type {
			case "array":
				return is.XListType != nil && *is.XListType != "atomic" // atomic is the implicit default behaviour if unset, hence != atomic is wrong
			case "object":
				return is.XMapType == nil || *is.XMapType != "atomic" // granular is the implicit default behaviour if unset, hence nil and != atomic are wrong
			default:
				return false // scalar types are always atomic
			}
		}
		return false
	})
}

func requireMapListKeysMapSetValidation(oldCRDSpec *apiextensions.CustomResourceDefinitionSpec) bool {
	return !HasSchemaWith(oldCRDSpec, hasInvalidMapListKeysMapSet)
}

func hasInvalidMapListKeysMapSet(schema *apiextensions.JSONSchemaProps) bool {
	return SchemaHas(schema, func(schema *apiextensions.JSONSchemaProps) bool {
		return len(validateMapListKeysMapSet(schema, field.NewPath(""))) > 0
	})
}

// requireValidPropertyType 判断指定 API 版本是否要求属性类型必须属于合法的 OpenAPI v3 类型。
func requireValidPropertyType(oldCRDSpec *apiextensions.CustomResourceDefinitionSpec) bool {
	if oldCRDSpec != nil && specHasInvalidTypes(oldCRDSpec) {
		// 不要收紧对已经持久化数据的校验要求，以保持升级兼容性。
		return false
	}
	return true
}

// validateAPIApproval 在 API 审批 annotation 不合法时返回对应的校验错误。
func validateAPIApproval(newCRD, oldCRD *apiextensions.CustomResourceDefinition) field.ErrorList {
	// 检查当前 API Group 是否需要执行 Kubernetes API 审批。
	if !apihelpers.IsProtectedCommunityGroup(newCRD.Spec.Group) {
		// 非受保护的 API Group 无需执行审批校验。
		return nil
	}

	// 默认采用允许缺失值继续保持缺失的状态，以兼容已有对象。
	var oldApprovalState *apihelpers.APIApprovalState
	if oldCRD != nil {
		t, _ := apihelpers.GetAPIApprovalState(oldCRD.Annotations)
		oldApprovalState = &t // +k8s:verify-mutation:reason=clone
	}
	newApprovalState, reason := apihelpers.GetAPIApprovalState(newCRD.Annotations)

	// 如果审批状态没有变化，则不因审批校验而拒绝更新。
	// 这样仅更新 Spec、没有修改该字段的 v1 客户端不会被拒绝，例如负责持续调谐 CRD Spec 的控制器。
	if oldApprovalState != nil && *oldApprovalState == newApprovalState {
		return nil
	}

	// 对于 v1 API，审批 annotation 必须是合法字符串。
	switch newApprovalState {
	case apihelpers.APIApprovalInvalid:
		return field.ErrorList{field.Invalid(field.NewPath("metadata", "annotations").Key(apiextensionsv1beta1.KubeAPIApprovedAnnotation), newCRD.Annotations[apiextensionsv1beta1.KubeAPIApprovedAnnotation], reason)}
	case apihelpers.APIApprovalMissing:
		return field.ErrorList{field.Required(field.NewPath("metadata", "annotations").Key(apiextensionsv1beta1.KubeAPIApprovedAnnotation), reason)}
	case apihelpers.APIApproved, apihelpers.APIApprovalBypassed:
		// 审批校验通过。
		return nil
	default:
		return field.ErrorList{field.Invalid(field.NewPath("metadata", "annotations").Key(apiextensionsv1beta1.KubeAPIApprovedAnnotation), newCRD.Annotations[apiextensionsv1beta1.KubeAPIApprovedAnnotation], reason)}
	}
}

func validatePreserveUnknownFields(crd, oldCRD *apiextensions.CustomResourceDefinition) field.ErrorList {
	if oldCRD != nil && oldCRD.Spec.PreserveUnknownFields != nil && *oldCRD.Spec.PreserveUnknownFields {
		// 为兼容已有数据，不执行额外处理。
		return nil
	}

	var errs field.ErrorList
	if crd != nil && crd.Spec.PreserveUnknownFields != nil && *crd.Spec.PreserveUnknownFields {
		// 禁止将 spec.preserveUnknownFields 从 false 改为 true。
		errs = append(errs, field.Invalid(field.NewPath("spec").Child("preserveUnknownFields"), crd.Spec.PreserveUnknownFields, "cannot set to true, set x-kubernetes-preserve-unknown-fields to true in spec.versions[*].schema instead"))
	}
	return errs
}

func specHasInvalidTypes(spec *apiextensions.CustomResourceDefinitionSpec) bool {
	if spec.Validation != nil && SchemaHasInvalidTypes(spec.Validation.OpenAPIV3Schema) {
		return true
	}
	for _, v := range spec.Versions {
		if v.Schema != nil && SchemaHasInvalidTypes(v.Schema.OpenAPIV3Schema) {
			return true
		}
	}
	return false
}

// SchemaHasInvalidTypes 判断 schema 是否包含不合法的 OpenAPI v3 类型定义。
func SchemaHasInvalidTypes(s *apiextensions.JSONSchemaProps) bool {
	return SchemaHas(s, func(s *apiextensions.JSONSchemaProps) bool {
		return len(s.Type) > 0 && !openapiV3Types.Has(s.Type)
	})
}
