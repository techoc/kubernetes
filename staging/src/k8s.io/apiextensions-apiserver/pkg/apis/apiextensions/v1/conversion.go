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

// 本文件定义 v1 版本与内部版本之间的自定义转换逻辑。
// 大部分字段转换由代码生成（zz_generated.conversion.go），
// 但以下字段需要特殊处理：
//   - JSON 类型：内部版本使用 interface{}，v1 使用 raw JSON 字节
//   - Spec 的 per-version 字段：内部版本使用顶层字段，v1 支持 per-version
//   - Conversion webhook：内部版本和 v1 的结构不同
package v1

import (
	"bytes"
	unsafe "unsafe"

	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/util/json"
)

// Convert_apiextensions_JSONSchemaProps_To_v1_JSONSchemaProps 处理 JSONSchemaProps 的 Default 和 Example 字段。
// 内部版本中，nil 默认值可能表示为 *JSON{(*interface{})(nil)}，需要转换为 v1 中的 nil。
func Convert_apiextensions_JSONSchemaProps_To_v1_JSONSchemaProps(in *apiextensions.JSONSchemaProps, out *JSONSchemaProps, s conversion.Scope) error {
	if err := autoConvert_apiextensions_JSONSchemaProps_To_v1_JSONSchemaProps(in, out, s); err != nil {
		return err
	}
	if in.Default != nil && *(in.Default) == nil {
		out.Default = nil
	}
	if in.Example != nil && *(in.Example) == nil {
		out.Example = nil
	}
	return nil
}

var nullLiteral = []byte(`null`)

// Convert_apiextensions_JSON_To_v1_JSON 将内部版本的 JSON（interface{}）序列化为 v1 的 raw JSON 字节。
// 内部版本使用 interface{} 存储任意 JSON 值，v1 使用 []byte 存储原始 JSON。
func Convert_apiextensions_JSON_To_v1_JSON(in *apiextensions.JSON, out *JSON, s conversion.Scope) error {
	raw, err := json.Marshal(*in)
	if err != nil {
		return err
	}
	if len(raw) == 0 || bytes.Equal(raw, nullLiteral) {
		// 与 JSON.UnmarshalJSON 对字面 null 的处理保持一致。
		out.Raw = nil
	} else {
		out.Raw = raw
	}
	return nil
}

// Convert_v1_JSON_To_apiextensions_JSON 将 v1 的 raw JSON 字节反序列化为内部版本的 interface{}。
// 空字节或字面量 null 转换为 nil interface{}。
func Convert_v1_JSON_To_apiextensions_JSON(in *JSON, out *apiextensions.JSON, s conversion.Scope) error {
	if in != nil {
		var i interface{}
		if len(in.Raw) > 0 && !bytes.Equal(in.Raw, nullLiteral) {
			if err := json.Unmarshal(in.Raw, &i); err != nil {
				return err
			}
		}
		*out = i
	} else {
		*out = nil
	}
	return nil
}

// Convert_apiextensions_CustomResourceDefinitionSpec_To_v1_CustomResourceDefinitionSpec 将内部版本的 CustomResourceDefinitionSpec 转换为 v1 版本。
// 内部版本使用顶层字段（Version、Subresources、Validation 等），
// v1 版本使用 per-version 结构。转换时将顶层字段复制到所有版本。
func Convert_apiextensions_CustomResourceDefinitionSpec_To_v1_CustomResourceDefinitionSpec(in *apiextensions.CustomResourceDefinitionSpec, out *CustomResourceDefinitionSpec, s conversion.Scope) error {
	if err := autoConvert_apiextensions_CustomResourceDefinitionSpec_To_v1_CustomResourceDefinitionSpec(in, out, s); err != nil {
		return err
	}

	// 兼容旧版本：如果内部版本有 Version 字段但没有 Versions，创建一个单元素 Versions
	if len(out.Versions) == 0 && len(in.Version) > 0 {
		out.Versions = []CustomResourceDefinitionVersion{{Name: in.Version, Served: true, Storage: true}}
	}

	// 将顶层的 subresources、validation、additionalPrinterColumns、selectableFields
	// 复制到每个版本中，因为 v1 API 使用 per-version 结构
	if in.Subresources != nil {
		subresources := &CustomResourceSubresources{}
		if err := Convert_apiextensions_CustomResourceSubresources_To_v1_CustomResourceSubresources(in.Subresources, subresources, s); err != nil {
			return err
		}
		for i := range out.Versions {
			out.Versions[i].Subresources = subresources
		}
	}
	if in.Validation != nil {
		schema := &CustomResourceValidation{}
		if err := Convert_apiextensions_CustomResourceValidation_To_v1_CustomResourceValidation(in.Validation, schema, s); err != nil {
			return err
		}
		for i := range out.Versions {
			out.Versions[i].Schema = schema
		}
	}
	if in.AdditionalPrinterColumns != nil {
		additionalPrinterColumns := make([]CustomResourceColumnDefinition, len(in.AdditionalPrinterColumns))
		for i := range in.AdditionalPrinterColumns {
			if err := Convert_apiextensions_CustomResourceColumnDefinition_To_v1_CustomResourceColumnDefinition(&in.AdditionalPrinterColumns[i], &additionalPrinterColumns[i], s); err != nil {
				return err
			}
		}
		for i := range out.Versions {
			out.Versions[i].AdditionalPrinterColumns = additionalPrinterColumns
		}
	}
	if in.SelectableFields != nil {
		selectableFields := make([]SelectableField, len(in.SelectableFields))
		for i := range in.SelectableFields {
			if err := Convert_apiextensions_SelectableField_To_v1_SelectableField(&in.SelectableFields[i], &selectableFields[i], s); err != nil {
				return err
			}
		}
		for i := range out.Versions {
			out.Versions[i].SelectableFields = selectableFields
		}
	}
	return nil
}

// Convert_v1_CustomResourceDefinitionSpec_To_apiextensions_CustomResourceDefinitionSpec 将 v1 版本的 CustomResourceDefinitionSpec 转换为内部版本。
// 如果所有版本的 per-version 字段相同，提取到顶层字段以节省存储空间。
// 如果各版本不同，保留 per-version 结构。
func Convert_v1_CustomResourceDefinitionSpec_To_apiextensions_CustomResourceDefinitionSpec(in *CustomResourceDefinitionSpec, out *apiextensions.CustomResourceDefinitionSpec, s conversion.Scope) error {
	if err := autoConvert_v1_CustomResourceDefinitionSpec_To_apiextensions_CustomResourceDefinitionSpec(in, out, s); err != nil {
		return err
	}

	if len(out.Versions) == 0 {
		return nil
	}

	// 内部版本的 Version 字段对应 v1 的第一个版本名称
	out.Version = out.Versions[0].Name

	// 检查所有版本的 per-version 字段是否相同。
	// 如果相同，提取到顶层字段（节省存储空间）；如果不同，保留 per-version 结构
	subresources := out.Versions[0].Subresources
	subresourcesIdentical := true
	validation := out.Versions[0].Schema
	validationIdentical := true
	additionalPrinterColumns := out.Versions[0].AdditionalPrinterColumns
	additionalPrinterColumnsIdentical := true
	selectableFields := out.Versions[0].SelectableFields
	selectableFieldsIdentical := true

	// 遍历所有版本，检测 per-version 字段是否完全相同
	for _, v := range out.Versions {
		if subresourcesIdentical && !apiequality.Semantic.DeepEqual(v.Subresources, subresources) {
			subresourcesIdentical = false
		}
		if validationIdentical && !apiequality.Semantic.DeepEqual(v.Schema, validation) {
			validationIdentical = false
		}
		if additionalPrinterColumnsIdentical && !apiequality.Semantic.DeepEqual(v.AdditionalPrinterColumns, additionalPrinterColumns) {
			additionalPrinterColumnsIdentical = false
		}
		if selectableFieldsIdentical && !apiequality.Semantic.DeepEqual(v.SelectableFields, selectableFields) {
			selectableFieldsIdentical = false
		}
	}

	// 如果所有版本的某个字段相同，提取到顶层并清空 per-version 字段
	if subresourcesIdentical {
		out.Subresources = subresources
	}
	if validationIdentical {
		out.Validation = validation
	}
	if additionalPrinterColumnsIdentical {
		out.AdditionalPrinterColumns = additionalPrinterColumns
	}
	if selectableFieldsIdentical {
		out.SelectableFields = selectableFields
	}
	for i := range out.Versions {
		if subresourcesIdentical {
			out.Versions[i].Subresources = nil
		}
		if validationIdentical {
			out.Versions[i].Schema = nil
		}
		if additionalPrinterColumnsIdentical {
			out.Versions[i].AdditionalPrinterColumns = nil
		}
		if selectableFieldsIdentical {
			out.Versions[i].SelectableFields = nil
		}
	}

	return nil
}

// Convert_v1_CustomResourceConversion_To_apiextensions_CustomResourceConversion 将 v1 版本的 CustomResourceConversion 转换为内部版本：
// v1 版本的 Conversion 结构将 Webhook 配置嵌套在 Webhook 子对象中，
// 内部版本将其展平为顶层字段。此函数处理结构差异。
func Convert_v1_CustomResourceConversion_To_apiextensions_CustomResourceConversion(in *CustomResourceConversion, out *apiextensions.CustomResourceConversion, s conversion.Scope) error {
	if err := autoConvert_v1_CustomResourceConversion_To_apiextensions_CustomResourceConversion(in, out, s); err != nil {
		return err
	}

	out.WebhookClientConfig = nil
	out.ConversionReviewVersions = nil
	if in.Webhook != nil {
		out.ConversionReviewVersions = in.Webhook.ConversionReviewVersions
		if in.Webhook.ClientConfig != nil {
			out.WebhookClientConfig = &apiextensions.WebhookClientConfig{}
			if err := Convert_v1_WebhookClientConfig_To_apiextensions_WebhookClientConfig(in.Webhook.ClientConfig, out.WebhookClientConfig, s); err != nil {
				return err
			}
		}
	}
	return nil
}

// Convert_apiextensions_CustomResourceConversion_To_v1_CustomResourceConversion 将内部版本的扁平化 webhook 配置重新嵌套到 v1 的 Webhook 子对象中。
func Convert_apiextensions_CustomResourceConversion_To_v1_CustomResourceConversion(in *apiextensions.CustomResourceConversion, out *CustomResourceConversion, s conversion.Scope) error {
	if err := autoConvert_apiextensions_CustomResourceConversion_To_v1_CustomResourceConversion(in, out, s); err != nil {
		return err
	}

	out.Webhook = nil
	if in.WebhookClientConfig != nil || in.ConversionReviewVersions != nil {
		out.Webhook = &WebhookConversion{}
		out.Webhook.ConversionReviewVersions = in.ConversionReviewVersions
		if in.WebhookClientConfig != nil {
			out.Webhook.ClientConfig = &WebhookClientConfig{}
			if err := Convert_apiextensions_WebhookClientConfig_To_v1_WebhookClientConfig(in.WebhookClientConfig, out.Webhook.ClientConfig, s); err != nil {
				return err
			}
		}
	}
	return nil
}

// Convert_apiextensions_ValidationRules_To_v1_ValidationRules 将内部版本的 ValidationRules 转换为 v1 版本：
// 使用 unsafe.Pointer 直接复制切片，因为两个版本的 ValidationRules 类型完全相同。
// 这是一种性能优化，避免逐个元素复制。
func Convert_apiextensions_ValidationRules_To_v1_ValidationRules(in *apiextensions.ValidationRules, out *ValidationRules, s conversion.Scope) error {
	*out = *(*ValidationRules)(unsafe.Pointer(in))
	return nil
}
