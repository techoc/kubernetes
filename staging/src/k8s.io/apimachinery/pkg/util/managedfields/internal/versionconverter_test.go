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

package internal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/kube-openapi/pkg/validation/spec"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
	"sigs.k8s.io/structured-merge-diff/v6/typed"
)

// testTypeConverter 从 testdata/swagger.json 构建类型转换器，供本包多个测试共用。
// 初始化失败直接 panic：测试环境缺少数据属于配置错误，应尽早暴露。
var testTypeConverter = func() TypeConverter {
	data, err := os.ReadFile(filepath.Join("testdata", "swagger.json"))
	if err != nil {
		panic(err)
	}
	swag := spec.Swagger{}
	if err := json.Unmarshal(data, &swag); err != nil {
		panic(err)
	}

	convertedDefs := map[string]*spec.Schema{}
	for k, v := range swag.Definitions {
		vCopy := v
		convertedDefs[k] = &vCopy
	}
	typeConverter, err := NewTypeConverter(convertedDefs, false)
	if err != nil {
		panic(err)
	}
	return typeConverter
}()

// TestVersionConverter tests the version converter
//
// 验证 v1beta1 的 TypedValue 经转换后与 v1 对象语义一致。
func TestVersionConverter(t *testing.T) {
	oc := fakeObjectConvertorForTestSchema{
		gvkForVersion("v1beta1"): objForGroupVersion("apps/v1beta1"),
		gvkForVersion("v1"):      objForGroupVersion("apps/v1"),
	}
	vc := newVersionConverter(testTypeConverter, oc, schema.GroupVersion{Group: "apps", Version: runtime.APIVersionInternal})

	input, err := testTypeConverter.ObjectToTyped(objForGroupVersion("apps/v1beta1"))
	if err != nil {
		t.Fatalf("error creating converting input object to a typed value: %v", err)
	}
	expected := objForGroupVersion("apps/v1")
	output, err := vc.Convert(input, fieldpath.APIVersion("apps/v1"))
	if err != nil {
		t.Fatalf("expected err to be nil but got %v", err)
	}
	actual, err := testTypeConverter.TypedToObject(output)
	if err != nil {
		t.Fatalf("error converting output typed value to an object %v", err)
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("expected to get %v but got %v", expected, actual)
	}
}

// TestVersionConverterSameVersion 验证同版本转换走快速路径：
// 直接返回输入，不调用 TypedToObject 或 ConvertToVersion。
func TestVersionConverterSameVersion(t *testing.T) {
	tc := &countingTypeConverter{TypeConverter: testTypeConverter}
	oc := &countingObjectConvertor{ObjectConvertor: fakeObjectConvertorForTestSchema{}}
	vc := newVersionConverter(tc, oc, schema.GroupVersion{Group: "apps", Version: runtime.APIVersionInternal})

	input, err := testTypeConverter.ObjectToTyped(objForGroupVersion("apps/v1beta1"))
	if err != nil {
		t.Fatalf("error converting input object to a typed value: %v", err)
	}
	output, err := vc.Convert(input, fieldpath.APIVersion("apps/v1beta1"))
	if err != nil {
		t.Fatalf("expected err to be nil but got %v", err)
	}
	if output != input {
		t.Errorf("expected same-version conversion to return the input unchanged")
	}
	if tc.typedToObjectCalls != 0 {
		t.Errorf("expected no TypedToObject calls for same-version conversion, got %d", tc.typedToObjectCalls)
	}
	if oc.convertToVersionCalls != 0 {
		t.Errorf("expected no ConvertToVersion calls for same-version conversion, got %d", oc.convertToVersionCalls)
	}
}

// countingTypeConverter 记录 TypedToObject 调用次数，用于断言快速路径未被触发。
type countingTypeConverter struct {
	TypeConverter
	typedToObjectCalls int
}

// TypedToObject 计数后委托给内嵌的 TypeConverter。
func (c *countingTypeConverter) TypedToObject(value *typed.TypedValue) (runtime.Object, error) {
	c.typedToObjectCalls++
	return c.TypeConverter.TypedToObject(value)
}

// countingObjectConvertor 记录 ConvertToVersion 调用次数。
type countingObjectConvertor struct {
	runtime.ObjectConvertor
	convertToVersionCalls int
}

// ConvertToVersion 计数后委托给内嵌的 ObjectConvertor。
func (c *countingObjectConvertor) ConvertToVersion(in runtime.Object, gv runtime.GroupVersioner) (runtime.Object, error) {
	c.convertToVersionCalls++
	return c.ObjectConvertor.ConvertToVersion(in, gv)
}

// gvkForVersion 构造 apps 组 Deployment 的 GVK。
func gvkForVersion(v string) schema.GroupVersionKind {
	return schema.GroupVersionKind{
		Group:   "apps",
		Version: v,
		Kind:    "Deployment",
	}
}

// objForGroupVersion 构造指定 apiVersion 的空 Deployment。
func objForGroupVersion(gv string) runtime.Object {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": gv,
			"kind":       "Deployment",
		},
	}
}

// fakeObjectConvertorForTestSchema 用 GVK->对象 的映射模拟版本转换。
type fakeObjectConvertorForTestSchema map[schema.GroupVersionKind]runtime.Object

var _ runtime.ObjectConvertor = fakeObjectConvertorForTestSchema{}

// ConvertToVersion 通过 GroupVersioner 在已知 GVK 中选择目标，再返回对应对象。
func (c fakeObjectConvertorForTestSchema) ConvertToVersion(_ runtime.Object, gv runtime.GroupVersioner) (runtime.Object, error) {
	allKinds := make([]schema.GroupVersionKind, 0)
	for kind := range c {
		allKinds = append(allKinds, kind)
	}
	gvk, _ := gv.KindForGroupVersionKinds(allKinds)
	return c[gvk], nil
}

// Convert 未实现，仅用于满足接口。
func (fakeObjectConvertorForTestSchema) Convert(_, _, _ interface{}) error {
	return fmt.Errorf("function not implemented")
}

// ConvertFieldLabel 未实现，仅用于满足接口。
func (fakeObjectConvertorForTestSchema) ConvertFieldLabel(_ schema.GroupVersionKind, _, _ string) (string, string, error) {
	return "", "", fmt.Errorf("function not implemented")
}
