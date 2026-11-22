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

// Package test 提供 CEL 校验相关的测试辅助函数。
package test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apimachinery/pkg/util/yaml"
	celconfig "k8s.io/apiserver/pkg/apis/cel"
)

// MustLoadManifest 从文件加载 CRD，出错时 panic。
func MustLoadManifest[T any](t *testing.T, pth string) *T {
	data, err := os.ReadFile(pth)
	require.NoError(t, err)

	var crd T
	err = yaml.Unmarshal(data, &crd)
	require.NoError(t, err)

	return &crd
}

// FieldValidators 从 CRD 中按版本与 JSONPath 提取 CEL 校验器，
// 返回可用于对样本对象做校验的函数。
func FieldValidators(t *testing.T, crd *apiextensionsv1.CustomResourceDefinition) (validatorsByVersionByJSONPath map[string]map[string]CELValidateFunc) {
	ret := map[string]map[string]CELValidateFunc{}
	for _, v := range crd.Spec.Versions {
		// 先转为内部类型，再构造结构化 schema
		var internalSchema apiextensions.JSONSchemaProps
		err := apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(v.Schema.OpenAPIV3Schema, &internalSchema, nil)
		require.NoError(t, err, "failed to convert JSONSchemaProps for version %s: %v", v.Name, err)
		structuralSchema, err := schema.NewStructural(&internalSchema)
		require.NoError(t, err, "failed to create StructuralSchema for version %s: %v", v.Name, err)

		versionVals, err := findCEL(t, structuralSchema, true, field.NewPath("openAPIV3Schema"))
		require.NoError(t, err, "failed to find CEL for version %s: %v", v.Name, err)
		ret[v.Name] = versionVals
	}

	return ret
}

// VersionValidatorsFromFile 从 CRD 文件中按版本提取 CEL 校验器，
// 返回可用于对样本对象做校验的函数。
func VersionValidatorsFromFile(t *testing.T, crdFilePath string) map[string]CELValidateFunc {
	data, err := os.ReadFile(crdFilePath)
	require.NoError(t, err)

	var crd apiextensionsv1.CustomResourceDefinition
	err = yaml.Unmarshal(data, &crd)
	require.NoError(t, err)

	ret := map[string]CELValidateFunc{}
	for _, v := range crd.Spec.Versions {
		var internalSchema apiextensions.JSONSchemaProps
		err := apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(v.Schema.OpenAPIV3Schema, &internalSchema, nil)
		require.NoError(t, err, "failed to convert JSONSchemaProps for version %s: %v", v.Name, err)
		structuralSchema, err := schema.NewStructural(&internalSchema)
		require.NoError(t, err, "failed to create StructuralSchema for version %s: %v", v.Name, err)
		// 以结构化的新旧对象运行 CEL 校验，忽略返回的成本值
		ret[v.Name] = func(obj, old interface{}) field.ErrorList {
			errs, _ := cel.NewValidator(structuralSchema, true, celconfig.RuntimeCELCostBudget).Validate(context.TODO(), nil, structuralSchema, obj, old, celconfig.PerCallLimit)
			return errs
		}
	}

	return ret
}

// VersionValidatorFromFile 从 CRD 文件中提取指定版本的 CEL 校验器，
// 返回可用于对样本对象做校验的函数。
func VersionValidatorFromFile(t *testing.T, crdFilePath string, version string) (CELValidateFunc, error) {
	vals := VersionValidatorsFromFile(t, crdFilePath)
	// 找不到指定版本时显式报错，避免调用方拿到 nil 校验器
	if val, ok := vals[version]; ok {
		return val, nil
	}
	return nil, fmt.Errorf("version %s not found", version)
}

// CELValidateFunc 用 CEL 校验器校验样本对象（可带旧对象）。
type CELValidateFunc func(obj, old interface{}) field.ErrorList

// findCEL 递归遍历结构化 schema，收集所有带 XValidations 的节点路径及其校验器。
func findCEL(t *testing.T, s *schema.Structural, root bool, pth *field.Path) (map[string]CELValidateFunc, error) {
	ret := map[string]CELValidateFunc{}

	if len(s.XValidations) > 0 {
		// 复制一份，避免闭包捕获循环变量或调用方后续修改
		s := *s
		pth := *pth
		ret[pth.String()] = func(obj, old interface{}) field.ErrorList {
			errs, _ := cel.NewValidator(&s, root, celconfig.RuntimeCELCostBudget).Validate(context.TODO(), &pth, &s, obj, old, celconfig.PerCallLimit)
			return errs
		}
	}

	for k, v := range s.Properties {
		sub, err := findCEL(t, &v, false, pth.Child("properties").Child(k))
		if err != nil {
			return nil, err
		}

		for pth, val := range sub {
			ret[pth] = val
		}
	}
	if s.Items != nil {
		sub, err := findCEL(t, s.Items, false, pth.Child("items"))
		if err != nil {
			return nil, err
		}
		for pth, val := range sub {
			ret[pth] = val
		}
	}
	if s.AdditionalProperties != nil && s.AdditionalProperties.Structural != nil {
		sub, err := findCEL(t, s.AdditionalProperties.Structural, false, pth.Child("additionalProperties"))
		if err != nil {
			return nil, err
		}
		for pth, val := range sub {
			ret[pth] = val
		}
	}

	return ret, nil
}
