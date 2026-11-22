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
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	smdschema "sigs.k8s.io/structured-merge-diff/v6/schema"
	"sigs.k8s.io/structured-merge-diff/v6/typed"
	"sigs.k8s.io/yaml"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// TestTypeConverter 用若干真实对象（Deployment、Pod）验证两种 TypeConverter
// （测试用 schema 转换器与 DeducedTypeConverter）都能把 unstructured 转成 typed 再转回。
func TestTypeConverter(t *testing.T) {
	dtc := NewDeducedTypeConverter()

	testCases := []struct {
		name string
		yaml string
	}{
		{
			name: "apps/v1.Deployment",
			yaml: `
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nginx-deployment
  labels:
    app: nginx
spec:
  selector:
    matchLabels:
      app: nginx
  template:
    metadata:
      labels:
        app: nginx
    spec:
      containers:
      - name: nginx
        image: nginx:1.15.4
`,
		}, {
			name: "extensions/v1beta1.Deployment",
			yaml: `
apiVersion: extensions/v1beta1
kind: Deployment
metadata:
  name: nginx-deployment
  labels:
    app: nginx
spec:
  selector:
    matchLabels:
      app: nginx
  template:
    metadata:
      labels:
        app: nginx
    spec:
      containers:
      - name: nginx
        image: nginx:1.15.4
`,
		}, {
			name: "v1.Pod",
			yaml: `
apiVersion: v1
kind: Pod
metadata:
  name: nginx-pod
  labels:
    app: nginx
spec:
  containers:
  - name: nginx
    image: nginx:1.15.4
`,
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("%v ObjectToTyped with TypeConverter", testCase.name), func(t *testing.T) {
			testObjectToTyped(t, testTypeConverter, testCase.yaml)
		})
		t.Run(fmt.Sprintf("%v ObjectToTyped with DeducedTypeConverter", testCase.name), func(t *testing.T) {
			testObjectToTyped(t, dtc, testCase.yaml)
		})
	}
}

// testObjectToTyped 执行 unstructured → typed → unstructured 往返并断言结果与原对象深度相等。
func testObjectToTyped(t *testing.T, tc TypeConverter, y string) {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
	if err := yaml.Unmarshal([]byte(y), &obj.Object); err != nil {
		t.Fatalf("Failed to parse yaml object: %v", err)
	}
	typed, err := tc.ObjectToTyped(obj)
	if err != nil {
		t.Fatalf("Failed to convert object to typed: %v", err)
	}
	newObj, err := tc.TypedToObject(typed)
	if err != nil {
		t.Fatalf("Failed to convert typed to object: %v", err)
	}
	if !reflect.DeepEqual(obj, newObj) {
		t.Errorf(`Round-trip failed:
Original object:
%#v
Final object:
%#v`, obj, newObj)
	}
}

// result 接收基准测试的最终输出，防止编译器优化掉被测调用。
var result typed.TypedValue

// BenchmarkObjectToTyped 测量 ObjectToTyped 的开销；选用 extensions/v1beta1 Deployment
// 是因为其嵌套 schema 足够复杂、能反映真实转换成本。
func BenchmarkObjectToTyped(b *testing.B) {
	y := `
apiVersion: extensions/v1beta1
kind: Deployment
metadata:
  name: nginx-deployment
  labels:
    app: nginx
spec:
  selector:
    matchLabels:
      app: nginx
  template:
    metadata:
      labels:
        app: nginx
    spec:
      containers:
      - name: nginx
        image: nginx:1.15.4
`
	obj := &unstructured.Unstructured{Object: map[string]interface{}{}}
	if err := yaml.Unmarshal([]byte(y), &obj.Object); err != nil {
		b.Fatalf("Failed to parse yaml object: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	var r *typed.TypedValue
	for i := 0; i < b.N; i++ {
		var err error
		r, err = testTypeConverter.ObjectToTyped(obj)
		if err != nil {
			b.Fatalf("Failed to convert object to typed: %v", err)
		}
	}
	result = *r
}

// TestIndexModels 验证依据 OpenAPI 的 x-kubernetes-group-version-kind 扩展建立
// GVK → schema 定义的索引：空 GVK、nil GVK 被忽略，单/多版本绑定都正确，
// 且 map[string]interface{} 与 map[interface{}]interface{} 两种解码形态都支持。
func TestIndexModels(t *testing.T) {
	myDefs := map[string]*spec.Schema{
		// 验证空的 GVK 扩展会被忽略
		"def0": {
			VendorExtensible: spec.VendorExtensible{
				Extensions: spec.Extensions{
					"x-kubernetes-group-version-kind": []interface{}{},
				},
			},
		},
		// 验证 nil GVK 会被忽略
		"def0.0": {
			VendorExtensible: spec.VendorExtensible{
				Extensions: spec.Extensions{
					"x-kubernetes-group-version-kind": nil,
				},
			},
		},
		// 验证该定义会被忽略（无 GVK 扩展）
		"def0.1": {},
		// 验证可以绑定单个 GVK
		"def1": {
			VendorExtensible: spec.VendorExtensible{
				Extensions: spec.Extensions{
					"x-kubernetes-group-version-kind": []interface{}{
						map[string]interface{}{
							"group":   "mygroup",
							"version": "v1",
							"kind":    "MyKind",
						},
					},
				},
			},
		},
		// 验证可以绑定两个版本
		"def2": {
			VendorExtensible: spec.VendorExtensible{
				Extensions: spec.Extensions{
					"x-kubernetes-group-version-kind": []interface{}{
						map[string]interface{}{
							"group":   "mygroup",
							"version": "v1",
							"kind":    "MyOtherKind",
						},
						map[string]interface{}{
							"group":   "mygroup",
							"version": "v2",
							"kind":    "MyOtherKind",
						},
					},
				},
			},
		},
		// 验证可以混用来自其它定义的 GVK，且 map[interface{}]interface{} 与
		// map[string]interface{} 两种形态都允许
		"def3": {
			VendorExtensible: spec.VendorExtensible{
				Extensions: spec.Extensions{
					"x-kubernetes-group-version-kind": []interface{}{
						map[string]interface{}{
							"group":   "mygroup",
							"version": "v3",
							"kind":    "MyKind",
						},
						map[interface{}]interface{}{
							"group":   "mygroup",
							"version": "v3",
							"kind":    "MyOtherKind",
						},
					},
				},
			},
		},
	}

	myTypes := []smdschema.TypeDef{
		{
			Name: "def0",
			Atom: smdschema.Atom{},
		},
		{
			Name: "def0.1",
			Atom: smdschema.Atom{},
		},
		{
			Name: "def0.2",
			Atom: smdschema.Atom{},
		},
		{
			Name: "def1",
			Atom: smdschema.Atom{},
		},
		{
			Name: "def2",
			Atom: smdschema.Atom{},
		},
		{
			Name: "def3",
			Atom: smdschema.Atom{},
		},
	}

	parser := typed.Parser{Schema: smdschema.Schema{Types: myTypes}}
	gvkIndex := indexModels(&parser, myDefs)

	require.Len(t, gvkIndex, 5)

	resultNames := map[schema.GroupVersionKind]string{}
	for k, v := range gvkIndex {
		require.NotNil(t, v.TypeRef.NamedType)
		resultNames[k] = *v.TypeRef.NamedType
	}

	require.Equal(t, map[schema.GroupVersionKind]string{
		{
			Group:   "mygroup",
			Version: "v1",
			Kind:    "MyKind",
		}: "def1",
		{
			Group:   "mygroup",
			Version: "v1",
			Kind:    "MyOtherKind",
		}: "def2",
		{
			Group:   "mygroup",
			Version: "v2",
			Kind:    "MyOtherKind",
		}: "def2",
		{
			Group:   "mygroup",
			Version: "v3",
			Kind:    "MyKind",
		}: "def3",
		{
			Group:   "mygroup",
			Version: "v3",
			Kind:    "MyOtherKind",
		}: "def3",
	}, resultNames)
}
