/*
Copyright 2016 The Kubernetes Authors.

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

package openapi

import (
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	openapitesting "k8s.io/apiserver/pkg/endpoints/openapi/testing"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// assertEqual 是测试内的小工具：nil 与非 nil 一律视为不等，
// 避免 reflect.DeepEqual(nil, typed-nil) 的陷阱导致误判。
func assertEqual(t *testing.T, expected, actual interface{}) {
	var equal bool
	if expected == nil || actual == nil {
		equal = expected == actual
	} else {
		equal = reflect.DeepEqual(expected, actual)
	}
	if !equal {
		t.Errorf("%v != %v", expected, actual)
	}
}

// TestGetDefinitionName 验证 OpenAPI 定义名的生成与扩展字段。
// 注册类型后，namer 应返回稳定的 friendly name，并附带
// x-kubernetes-group-version-kind 扩展，客户端据此把 schema 关联到 GVK。
// 未注册的类型则原样返回名称且不带扩展，保证未知类型不会破坏文档生成。
func TestGetDefinitionName(t *testing.T) {
	testType := openapitesting.TestType{}
	// 在生产环境中，名称在传给 GetDefinitionName 之前已去除 ".*vendor/" 前缀，
	// 因此这里的 typePkgName 不带 "k8s.io/kubernetes/vendor" 前缀。
	//
	// 这条注释解释了测试为何不构造 vendor 前缀——生产路径已先剥离，
	// 在此重复构造只会测试到已被移除的旧行为。
	typeFriendlyName := "io.k8s.apiserver.pkg.endpoints.openapi.testing.TestType"
	s := runtime.NewScheme()
	s.AddKnownTypeWithName(testType.GroupVersionKind(), &testType)
	namer := NewDefinitionNamer(s)
	n, e := namer.GetDefinitionName(typeFriendlyName)
	assertEqual(t, typeFriendlyName, n)
	assertEqual(t, []interface{}{
		map[string]interface{}{
			"group":   "test",
			"version": "v1",
			"kind":    "TestType",
		},
	}, e["x-kubernetes-group-version-kind"])
	// 未注册类型：名称保持不变，且没有 GVK 扩展。
	n, e2 := namer.GetDefinitionName("com.test.another.Type")
	assertEqual(t, "com.test.another.Type", n)
	assertEqual(t, e2, spec.Extensions(nil))
}

// TestToValidOperationID 验证把任意字符串规范化为合法 OpenAPI operationId 的规则。
// operationId 必须能安全嵌入生成的客户端方法名，因此非法字符被删除、
// 可选地首字母大写；数字开头的处理由调用方通过 capitalizeFirstLetter 控制。
func TestToValidOperationID(t *testing.T) {
	scenarios := []struct {
		s                     string
		capitalizeFirstLetter bool
		expectedResult        string
	}{
		{
			s:                     "test_operation",
			capitalizeFirstLetter: true,
			expectedResult:        "Test_operation",
		},
		{
			s:                     "test operation& test",
			capitalizeFirstLetter: true,
			expectedResult:        "TestOperationTest",
		},
		{
			s:                     "test78operation",
			capitalizeFirstLetter: false,
			expectedResult:        "test78operation",
		},
	}
	for _, tt := range scenarios {
		result := ToValidOperationID(tt.s, tt.capitalizeFirstLetter)
		if result != tt.expectedResult {
			t.Errorf("expected result: %s, got: %s", tt.expectedResult, result)
		}
	}
}
