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

package validation

import (
	"encoding/json"
	"strings"

	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/features"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apiserver/pkg/cel/common"
	"k8s.io/apiserver/pkg/cel/environment"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	openapierrors "k8s.io/kube-openapi/pkg/validation/errors"
	"k8s.io/kube-openapi/pkg/validation/spec"
	"k8s.io/kube-openapi/pkg/validation/strfmt"
	"k8s.io/kube-openapi/pkg/validation/validate"
)

// SchemaValidator 在创建校验之外还支持带旧值的更新校验。
type SchemaValidator interface {
	SchemaCreateValidator
	ValidateUpdate(new, old interface{}, options ...ValidationOption) *validate.Result
}

// SchemaCreateValidator 校验新建对象，并接受可选的校验选项。
type SchemaCreateValidator interface {
	Validate(value interface{}, options ...ValidationOption) *validate.Result
}

// ValidationOptions 汇总一次校验调用的可选行为。
type ValidationOptions struct {
	// Ratcheting 表示对 schema 中未变更部分产生的错误是否执行 ratcheting（沿用旧值放行）。
	// Validate 会忽略该字段。
	Ratcheting bool

	// CorrelatedObject 表示新旧参数之间的对应关系。
	// 若设置，它应当是 ValidateUpdate 的 `new` 与 `old` 参数之间的对应关系，
	// 并且 `new`、`old` 的取值将从该对应关系中获取。
	//
	// Validate 会忽略该字段。
	//
	// 目前用于 ratcheting，但单独保留字段以便将来复用于其他目的。
	CorrelatedObject *common.CorrelatedObject
}

// ValidationOption 修改 ValidationOptions。
type ValidationOption func(*ValidationOptions)

// NewValidationOptions 依次应用选项并返回最终配置。
func NewValidationOptions(opts ...ValidationOption) ValidationOptions {
	options := ValidationOptions{}
	for _, opt := range opts {
		opt(&options)
	}
	return options
}

// WithRatcheting 开启 ratcheting 并设置新旧对象对应关系。
func WithRatcheting(correlation *common.CorrelatedObject) ValidationOption {
	return func(options *ValidationOptions) {
		options.Ratcheting = true
		options.CorrelatedObject = correlation
	}
}

// basicSchemaValidator 包装 kube-openapi 的 SchemaCreateValidator 以支持 ValidateUpdate。
// 它通过 kube-openapi 校验新值来实现 ValidateUpdate，忽略旧值。
type basicSchemaValidator struct {
	*validate.SchemaValidator
}

// Validate 用 kube-openapi 校验给定值。
func (s basicSchemaValidator) Validate(new interface{}, options ...ValidationOption) *validate.Result {
	return s.SchemaValidator.Validate(new)
}

// ValidateUpdate 忽略旧值，等价于校验新值。
func (s basicSchemaValidator) ValidateUpdate(new, old interface{}, options ...ValidationOption) *validate.Result {
	return s.Validate(new, options...)
}

// NewSchemaValidator 使用 environment.DefaultCompatibilityVersion() 为给定 CRD 校验构造 openapi schema 校验器。
func NewSchemaValidator(customResourceValidation *apiextensions.JSONSchemaProps) (SchemaValidator, *spec.Schema, error) {
	return NewSchemaValidatorForVersion(customResourceValidation, environment.DefaultCompatibilityVersion())
}

// NewSchemaValidatorForVersion 为给定 CRD 校验和兼容版本构造 openapi schema 校验器。
//
// 若 `CRDValidationRatcheting` 特性关闭，返回的校验器把所有 `Update` 和 `Create`
// 都当作 `Create` 校验，不参考旧值。
//
// 若 `CRDValidationRatcheting` 特性开启，返回的校验器支持在更新时对未变更的、
// 可关联字段执行 ratcheting。
func NewSchemaValidatorForVersion(customResourceValidation *apiextensions.JSONSchemaProps, compatibilityVersion *version.Version) (SchemaValidator, *spec.Schema, error) {
	// 把 CRD schema 转换为 openapi schema
	openapiSchema := &spec.Schema{}
	if customResourceValidation != nil {
		// TODO: 改用 NewStructural(...).ToGoOpenAPI 替换
		formatPostProcessor := StripUnsupportedFormatsPostProcessorForVersion(compatibilityVersion)
		if err := ConvertJSONSchemaPropsWithPostProcess(customResourceValidation, openapiSchema, formatPostProcessor); err != nil {
			return nil, nil, err
		}
	}
	return NewSchemaValidatorFromOpenAPI(openapiSchema), openapiSchema, nil
}

// NewSchemaValidatorFromOpenAPI 根据是否启用 ratcheting 特性，从 openapi schema 构造对应校验器。
func NewSchemaValidatorFromOpenAPI(openapiSchema *spec.Schema) SchemaValidator {
	if utilfeature.DefaultFeatureGate.Enabled(features.CRDValidationRatcheting) {
		return NewRatchetingSchemaValidator(openapiSchema, nil, "", strfmt.Default)
	}
	return basicSchemaValidator{validate.NewSchemaValidator(openapiSchema, nil, "", strfmt.Default)}

}

// ValidateCustomResourceUpdate 校验自定义资源从 `old` 到 `new` 的转换是否满足
// CustomResourceDefinition 中的 schema。customResource 和 old 都表示 JSON 数据结构。
//
// 若 `CRDValidationRatcheting` 特性关闭，行为与 ValidateCustomResource(customResource) 完全相同。
func ValidateCustomResourceUpdate(fldPath *field.Path, customResource, old interface{}, validator SchemaValidator, options ...ValidationOption) field.ErrorList {
	// 额外的特性开关检查，仅作防御
	if !utilfeature.DefaultFeatureGate.Enabled(features.CRDValidationRatcheting) {
		return ValidateCustomResource(nil, customResource, validator)
	} else if validator == nil {
		return nil
	}

	result := validator.ValidateUpdate(customResource, old, options...)
	if result.IsValid() {
		return nil
	}

	return kubeOpenAPIResultToFieldErrors(fldPath, result)
}

// ValidateCustomResource 依据 CustomResourceDefinition 中的 schema 校验自定义资源。
// customResource 是 JSON 数据结构。
func ValidateCustomResource(fldPath *field.Path, customResource interface{}, validator SchemaCreateValidator, options ...ValidationOption) field.ErrorList {
	if validator == nil {
		return nil
	}

	result := validator.Validate(customResource, options...)
	if result.IsValid() {
		return nil
	}

	return kubeOpenAPIResultToFieldErrors(fldPath, result)
}

// kubeOpenAPIResultToFieldErrors 把 kube-openapi 的校验结果转换为 field.ErrorList。
func kubeOpenAPIResultToFieldErrors(fldPath *field.Path, result *validate.Result) field.ErrorList {
	var allErrs field.ErrorList
	for _, err := range result.Errors {
		switch err := err.(type) {

		case *openapierrors.Validation:
			errPath := fldPath
			if len(err.Name) > 0 && err.Name != "." {
				errPath = errPath.Child(strings.TrimPrefix(err.Name, "."))
			}

			switch err.Code() {
			case openapierrors.RequiredFailCode:
				allErrs = append(allErrs, field.Required(errPath, ""))

			case openapierrors.EnumFailCode:
				values := []string{}
				for _, allowedValue := range err.Values {
					if s, ok := allowedValue.(string); ok {
						values = append(values, s)
					} else {
						allowedJSON, _ := json.Marshal(allowedValue)
						values = append(values, string(allowedJSON))
					}
				}
				allErrs = append(allErrs, field.NotSupported(errPath, err.Value, values))

			case openapierrors.TooLongFailCode:
				max := int64(-1)
				if i, ok := err.Valid.(int64); ok {
					max = i
				}
				allErrs = append(allErrs, field.TooLong(errPath, "" /*未使用*/, int(max)))

			case openapierrors.MaxItemsFailCode:
				actual := int64(-1)
				if i, ok := err.Value.(int64); ok {
					actual = i
				}
				max := int64(-1)
				if i, ok := err.Valid.(int64); ok {
					max = i
				}
				allErrs = append(allErrs, field.TooMany(errPath, int(actual), int(max)))

			case openapierrors.TooManyPropertiesCode:
				actual := int64(-1)
				if i, ok := err.Value.(int64); ok {
					actual = i
				}
				max := int64(-1)
				if i, ok := err.Valid.(int64); ok {
					max = i
				}
				allErrs = append(allErrs, field.TooMany(errPath, int(actual), int(max)))

			case openapierrors.InvalidTypeCode:
				value := interface{}("")
				if err.Value != nil {
					value = err.Value
				}
				allErrs = append(allErrs, field.TypeInvalid(errPath, value, err.Error()))

			default:
				value := interface{}("")
				if err.Value != nil {
					value = err.Value
				}
				allErrs = append(allErrs, field.Invalid(errPath, value, err.Error()))
			}

		default:
			allErrs = append(allErrs, field.Invalid(fldPath, "", err.Error()))
		}
	}
	return allErrs
}

// ConvertJSONSchemaProps 把 apiextensions.JSONSchemaProps 转换为 go-openapi/spec.Schema。
func ConvertJSONSchemaProps(in *apiextensions.JSONSchemaProps, out *spec.Schema) error {
	return ConvertJSONSchemaPropsWithPostProcess(in, out, nil)
}

// PostProcessFunc 对 spec.Schema 的一个节点做后处理。
type PostProcessFunc func(*spec.Schema) error

// ConvertJSONSchemaPropsWithPostProcess 把 apiextensions.JSONSchemaProps 转换为
// go-openapi/spec.Schema，并对每个 JSONSchemaProps 节点执行后处理步骤。
// nil schema 不会调用 postProcess。
func ConvertJSONSchemaPropsWithPostProcess(in *apiextensions.JSONSchemaProps, out *spec.Schema, postProcess PostProcessFunc) error {
	if in == nil {
		return nil
	}

	out.ID = in.ID
	out.Schema = spec.SchemaURL(in.Schema)
	out.Description = in.Description
	if in.Type != "" {
		out.Type = spec.StringOrArray([]string{in.Type})
	}
	if in.XIntOrString {
		out.VendorExtensible.AddExtension("x-kubernetes-int-or-string", true)
		out.Type = spec.StringOrArray{"integer", "string"}
	}
	out.Nullable = in.Nullable
	out.Format = in.Format
	out.Title = in.Title
	out.Maximum = in.Maximum
	out.ExclusiveMaximum = in.ExclusiveMaximum
	out.Minimum = in.Minimum
	out.ExclusiveMinimum = in.ExclusiveMinimum
	out.MaxLength = in.MaxLength
	out.MinLength = in.MinLength
	out.Pattern = in.Pattern
	out.MaxItems = in.MaxItems
	out.MinItems = in.MinItems
	out.UniqueItems = in.UniqueItems
	out.MultipleOf = in.MultipleOf
	out.MaxProperties = in.MaxProperties
	out.MinProperties = in.MinProperties
	out.Required = in.Required

	if in.Default != nil {
		out.Default = *(in.Default)
	}
	if in.Example != nil {
		out.Example = *(in.Example)
	}

	if in.Enum != nil {
		out.Enum = make([]interface{}, len(in.Enum))
		for k, v := range in.Enum {
			out.Enum[k] = v
		}
	}

	if err := convertSliceOfJSONSchemaProps(&in.AllOf, &out.AllOf, postProcess); err != nil {
		return err
	}
	if err := convertSliceOfJSONSchemaProps(&in.OneOf, &out.OneOf, postProcess); err != nil {
		return err
	}
	if err := convertSliceOfJSONSchemaProps(&in.AnyOf, &out.AnyOf, postProcess); err != nil {
		return err
	}

	if in.Not != nil {
		in, out := &in.Not, &out.Not
		*out = new(spec.Schema)
		if err := ConvertJSONSchemaPropsWithPostProcess(*in, *out, postProcess); err != nil {
			return err
		}
	}

	var err error
	out.Properties, err = convertMapOfJSONSchemaProps(in.Properties, postProcess)
	if err != nil {
		return err
	}

	out.PatternProperties, err = convertMapOfJSONSchemaProps(in.PatternProperties, postProcess)
	if err != nil {
		return err
	}

	out.Definitions, err = convertMapOfJSONSchemaProps(in.Definitions, postProcess)
	if err != nil {
		return err
	}

	if in.Ref != nil {
		out.Ref, err = spec.NewRef(*in.Ref)
		if err != nil {
			return err
		}
	}

	if in.AdditionalProperties != nil {
		in, out := &in.AdditionalProperties, &out.AdditionalProperties
		*out = new(spec.SchemaOrBool)
		if err := convertJSONSchemaPropsorBool(*in, *out, postProcess); err != nil {
			return err
		}
	}

	if in.AdditionalItems != nil {
		in, out := &in.AdditionalItems, &out.AdditionalItems
		*out = new(spec.SchemaOrBool)
		if err := convertJSONSchemaPropsorBool(*in, *out, postProcess); err != nil {
			return err
		}
	}

	if in.Items != nil {
		in, out := &in.Items, &out.Items
		*out = new(spec.SchemaOrArray)
		if err := convertJSONSchemaPropsOrArray(*in, *out, postProcess); err != nil {
			return err
		}
	}

	if in.Dependencies != nil {
		in, out := &in.Dependencies, &out.Dependencies
		*out = make(spec.Dependencies, len(*in))
		for key, val := range *in {
			newVal := new(spec.SchemaOrStringArray)
			if err := convertJSONSchemaPropsOrStringArray(&val, newVal, postProcess); err != nil {
				return err
			}
			(*out)[key] = *newVal
		}
	}

	if in.ExternalDocs != nil {
		out.ExternalDocs = &spec.ExternalDocumentation{}
		out.ExternalDocs.Description = in.ExternalDocs.Description
		out.ExternalDocs.URL = in.ExternalDocs.URL
	}

	if postProcess != nil {
		if err := postProcess(out); err != nil {
			return err
		}
	}

	if in.XPreserveUnknownFields != nil {
		out.VendorExtensible.AddExtension("x-kubernetes-preserve-unknown-fields", *in.XPreserveUnknownFields)
	}
	if in.XEmbeddedResource {
		out.VendorExtensible.AddExtension("x-kubernetes-embedded-resource", true)
	}
	if len(in.XListMapKeys) != 0 {
		out.VendorExtensible.AddExtension("x-kubernetes-list-map-keys", convertSliceToInterfaceSlice(in.XListMapKeys))
	}
	if in.XListType != nil {
		out.VendorExtensible.AddExtension("x-kubernetes-list-type", *in.XListType)
	}
	if in.XMapType != nil {
		out.VendorExtensible.AddExtension("x-kubernetes-map-type", *in.XMapType)
	}
	if len(in.XValidations) != 0 {
		var serializationValidationRules apiextensionsv1.ValidationRules
		if err := apiextensionsv1.Convert_apiextensions_ValidationRules_To_v1_ValidationRules(&in.XValidations, &serializationValidationRules, nil); err != nil {
			return err
		}
		out.VendorExtensible.AddExtension("x-kubernetes-validations", convertSliceToInterfaceSlice(serializationValidationRules))
	}
	return nil
}

// convertSliceToInterfaceSlice 把泛型切片转为 []interface{}。
func convertSliceToInterfaceSlice[T any](in []T) []interface{} {
	var res []interface{}
	for _, v := range in {
		res = append(res, v)
	}
	return res
}

// convertSliceOfJSONSchemaProps 转换 JSONSchemaProps 切片，并对每个元素执行后处理。
func convertSliceOfJSONSchemaProps(in *[]apiextensions.JSONSchemaProps, out *[]spec.Schema, postProcess PostProcessFunc) error {
	if in != nil {
		for _, jsonSchemaProps := range *in {
			schema := spec.Schema{}
			if err := ConvertJSONSchemaPropsWithPostProcess(&jsonSchemaProps, &schema, postProcess); err != nil {
				return err
			}
			*out = append(*out, schema)
		}
	}
	return nil
}

// convertMapOfJSONSchemaProps 转换 JSONSchemaProps 映射，并对每个值执行后处理。
func convertMapOfJSONSchemaProps(in map[string]apiextensions.JSONSchemaProps, postProcess PostProcessFunc) (map[string]spec.Schema, error) {
	if in == nil {
		return nil, nil
	}

	out := make(map[string]spec.Schema)
	for k, jsonSchemaProps := range in {
		schema := spec.Schema{}
		if err := ConvertJSONSchemaPropsWithPostProcess(&jsonSchemaProps, &schema, postProcess); err != nil {
			return nil, err
		}
		out[k] = schema
	}
	return out, nil
}

// convertJSONSchemaPropsOrArray 转换 schema 或 schema 数组。
func convertJSONSchemaPropsOrArray(in *apiextensions.JSONSchemaPropsOrArray, out *spec.SchemaOrArray, postProcess PostProcessFunc) error {
	if in.Schema != nil {
		in, out := &in.Schema, &out.Schema
		*out = new(spec.Schema)
		if err := ConvertJSONSchemaPropsWithPostProcess(*in, *out, postProcess); err != nil {
			return err
		}
	}
	if in.JSONSchemas != nil {
		in, out := &in.JSONSchemas, &out.Schemas
		*out = make([]spec.Schema, len(*in))
		for i := range *in {
			if err := ConvertJSONSchemaPropsWithPostProcess(&(*in)[i], &(*out)[i], postProcess); err != nil {
				return err
			}
		}
	}
	return nil
}

// convertJSONSchemaPropsorBool 转换 schema 或布尔值。
func convertJSONSchemaPropsorBool(in *apiextensions.JSONSchemaPropsOrBool, out *spec.SchemaOrBool, postProcess PostProcessFunc) error {
	out.Allows = in.Allows
	if in.Schema != nil {
		in, out := &in.Schema, &out.Schema
		*out = new(spec.Schema)
		if err := ConvertJSONSchemaPropsWithPostProcess(*in, *out, postProcess); err != nil {
			return err
		}
	}
	return nil
}

// convertJSONSchemaPropsOrStringArray 转换 schema 或字符串数组。
func convertJSONSchemaPropsOrStringArray(in *apiextensions.JSONSchemaPropsOrStringArray, out *spec.SchemaOrStringArray, postProcess PostProcessFunc) error {
	out.Property = in.Property
	if in.Schema != nil {
		in, out := &in.Schema, &out.Schema
		*out = new(spec.Schema)
		if err := ConvertJSONSchemaPropsWithPostProcess(*in, *out, postProcess); err != nil {
			return err
		}
	}
	return nil
}
