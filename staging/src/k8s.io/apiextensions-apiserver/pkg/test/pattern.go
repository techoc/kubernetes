/*
Copyright 2024 The Kubernetes Authors.

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

package test

import (
	"errors"
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// PatternValidators 从 CRD 中按版本和 JSONPath 提取 pattern 校验器，
// 返回可用于对样本做测试的校验函数。
func PatternValidators(t *testing.T, crd *apiextensionsv1.CustomResourceDefinition) (validatorsByVersionByJSONPath map[string]map[string]PatternValidateFunc) {
	ret := map[string]map[string]PatternValidateFunc{}
	for _, v := range crd.Spec.Versions {
		var internalSchema apiextensions.JSONSchemaProps
		err := apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(v.Schema.OpenAPIV3Schema, &internalSchema, nil)
		require.NoError(t, err, "failed to convert JSONSchemaProps for version %s: %v", v.Name, err)
		structuralSchema, err := schema.NewStructural(&internalSchema)
		require.NoError(t, err, "failed to create StructuralSchema for version %s: %v", v.Name, err)

		versionVals, err := findPattern(t, structuralSchema, field.NewPath("openAPIV3Schema"))
		require.NoError(t, err, "failed to find CEL for version %s: %v", v.Name, err)
		ret[v.Name] = versionVals
	}

	return ret
}

// PatternValidateFunc 针对单个对象校验 pattern，失败时返回错误。
type PatternValidateFunc func(obj interface{}) error

// findPattern 递归遍历结构化 schema，收集其中所有非空 pattern 及其路径。
// 返回的 map 以 JSONPath 字符串为键，便于测试按字段定位。
func findPattern(t *testing.T, s *schema.Structural, pth *field.Path) (map[string]PatternValidateFunc, error) {
	ret := map[string]PatternValidateFunc{}

	if len(s.ValueValidation.Pattern) > 0 {
		s := *s
		pth := *pth
		ret[pth.String()] = func(obj interface{}) error {
			// 在闭包内编译，避免把编译结果暴露给调用方，也保证错误能就地返回。
			p, err := regexp.Compile(s.ValueValidation.Pattern)
			if err != nil {
				return err
			}
			if p.MatchString(obj.(string)) {
				return nil
			}
			return errors.New("pattern mismatch")
		}
	}

	for k, v := range s.Properties {
		sub, err := findPattern(t, &v, pth.Child("properties").Child(k))
		if err != nil {
			return nil, err
		}

		for pth, val := range sub {
			ret[pth] = val
		}
	}
	if s.Items != nil {
		sub, err := findPattern(t, s.Items, pth.Child("items"))
		if err != nil {
			return nil, err
		}
		for pth, val := range sub {
			ret[pth] = val
		}
	}
	if s.AdditionalProperties != nil && s.AdditionalProperties.Structural != nil {
		sub, err := findPattern(t, s.AdditionalProperties.Structural, pth.Child("additionalProperties"))
		if err != nil {
			return nil, err
		}
		for pth, val := range sub {
			ret[pth] = val
		}
	}

	return ret, nil
}
