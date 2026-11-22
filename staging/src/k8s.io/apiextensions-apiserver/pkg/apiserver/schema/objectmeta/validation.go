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

// Package objectmeta 校验通过 x-kubernetes-embedded-resource 嵌入的对象元数据，
// 复用 ObjectMeta 与 TypeMeta 的既有校验规则。
package objectmeta

import (
	"context"
	"strings"

	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/api/validate/content"
	metavalidation "k8s.io/apimachinery/pkg/api/validation"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
)

// Validate 校验内嵌的 ObjectMeta 与 TypeMeta。
// 当 isResourceRoot 为 true 时，根对象也按内嵌资源处理：根对象自身没有
// x-kubernetes-embedded-resource 标记，但其 apiVersion/kind/metadata 同样需要校验，
// 因此复制 schema 并临时打开该标记，避免改动调用方的 schema。
func Validate(ctx context.Context, pth *field.Path, obj interface{}, s *structuralschema.Structural, isResourceRoot bool) field.ErrorList {
	if isResourceRoot {
		if s == nil {
			s = &structuralschema.Structural{}
		}
		if !s.XEmbeddedResource {
			clone := *s
			clone.XEmbeddedResource = true
			s = &clone
		}
	}
	return validate(ctx, pth, obj, s)
}

// validate 递归遍历对象，按 schema 的 properties/items/additionalProperties 下钻，
// 并在标记了内嵌资源的节点上执行额外校验。
// s 为 nil 表示该子树没有可校验的 schema，直接跳过。
func validate(ctx context.Context, pth *field.Path, x interface{}, s *structuralschema.Structural) field.ErrorList {
	if s == nil {
		return nil
	}

	var allErrs field.ErrorList

	switch x := x.(type) {
	case map[string]interface{}:
		if s.XEmbeddedResource {
			allErrs = append(allErrs, validateEmbeddedResource(ctx, pth, x, s)...)
		}

		for k, v := range x {
			prop, ok := s.Properties[k]
			if ok {
				allErrs = append(allErrs, validate(ctx, pth.Child(k), v, &prop)...)
			} else if s.AdditionalProperties != nil {
				// 未在 properties 中声明的字段按 additionalProperties 的 schema 校验。
				allErrs = append(allErrs, validate(ctx, pth.Key(k), v, s.AdditionalProperties.Structural)...)
			}
		}
	case []interface{}:
		for i, v := range x {
			allErrs = append(allErrs, validate(ctx, pth.Index(i), v, s.Items)...)
		}
	default:
		// 标量没有可继续下钻的结构，无需处理。
	}

	return allErrs
}

// validatePathSegment 校验路径分段；prefix 为 true 时按前缀规则校验（generateName），
// 否则按完整名称规则校验。
func validatePathSegment(name string, prefix bool) []string {
	if prefix {
		return content.IsPathSegmentPrefix(name)
	}
	return content.IsPathSegmentName(name)
}

// validateEmbeddedResource 校验单个内嵌资源，重点是 apiVersion、kind 与 metadata。
func validateEmbeddedResource(ctx context.Context, pth *field.Path, x map[string]interface{}, s *structuralschema.Structural) field.ErrorList {
	var allErrs field.ErrorList

	// apiVersion 与 kind 必填；metadata 可省略。
	if _, found := x["apiVersion"]; !found {
		allErrs = append(allErrs, field.Required(pth.Child("apiVersion"), ""))
	}
	if _, found := x["kind"]; !found {
		allErrs = append(allErrs, field.Required(pth.Child("kind"), ""))
	}

	for k, v := range x {
		switch k {
		case "apiVersion":
			if apiVersion, ok := v.(string); !ok {
				allErrs = append(allErrs, field.Invalid(pth.Child("apiVersion"), v, "must be a string"))
			} else if len(apiVersion) == 0 {
				allErrs = append(allErrs, field.Invalid(pth.Child("apiVersion"), apiVersion, "must not be empty"))
			} else if _, err := schema.ParseGroupVersion(apiVersion); err != nil {
				allErrs = append(allErrs, field.Invalid(pth.Child("apiVersion"), apiVersion, err.Error()))
			}
		case "kind":
			if kind, ok := v.(string); !ok {
				allErrs = append(allErrs, field.Invalid(pth.Child("kind"), v, "must be a string"))
			} else if len(kind) == 0 {
				allErrs = append(allErrs, field.Invalid(pth.Child("kind"), kind, "must not be empty"))
			} else if errs := utilvalidation.IsDNS1035Label(strings.ToLower(kind)); len(errs) > 0 {
				allErrs = append(allErrs, field.Invalid(pth.Child("kind"), kind, "may have mixed case, but should otherwise match: "+strings.Join(errs, ",")))
			}
		case "metadata":
			meta, _, err := GetObjectMeta(x, false)
			if err != nil {
				allErrs = append(allErrs, field.Invalid(pth.Child("metadata"), v, err.Error()))
			} else {
				if len(meta.Name) == 0 {
					meta.Name = "fakename" // 必须填个名字，否则名称校验会直接报错
				}
				allErrs = append(allErrs, metavalidation.ValidateObjectMetaDeclaratively(ctx, operation.Create, meta, nil, len(meta.Namespace) > 0, validatePathSegment, pth.Child("metadata"), utilfeature.DefaultFeatureGate.Enabled(features.DeclarativeValidationBeta))...)
			}
		}
	}

	return allErrs
}
