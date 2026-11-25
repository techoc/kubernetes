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

package unstructured_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/apis/testapigroup"
	testapigroupv1 "k8s.io/apimachinery/pkg/apis/testapigroup/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/test"
)

// TestObjectToUnstructuredConversion 覆盖 scheme 将具体对象转换为
// Unstructured 的路径。关键约束：带版本的对外对象可直接转换；Internal
// （hub）对象因为没有唯一的首选版本，必须显式指定目标版本，否则报错。
func TestObjectToUnstructuredConversion(t *testing.T) {
	scheme, _ := test.TestScheme()
	testCases := []struct {
		name                          string
		objectToConvert               runtime.Object
		expectedErr                   error
		expectedConvertedUnstructured *unstructured.Unstructured
	}{
		{
			// 空指针没有类型信息，无法映射到任何 GVK，因此必须失败。
			name:                          "convert nil object to unstructured should fail",
			objectToConvert:               nil,
			expectedErr:                   fmt.Errorf("unable to convert object type <nil> to Unstructured, must be a runtime.Object"),
			expectedConvertedUnstructured: &unstructured.Unstructured{},
		},
		{
			// 空对象也要保留 metadata/spec/status 三个字段，因为类型元数据
			// 来自 Go 类型，而不是值本身。
			name:            "convert versioned empty object to unstructured should work",
			objectToConvert: &testapigroupv1.Carp{},
			expectedConvertedUnstructured: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Carp",
					"metadata":   map[string]interface{}{},
					"spec":       map[string]interface{}{},
					"status":     map[string]interface{}{},
				},
			},
		},
		{
			name: "convert valid versioned object to unstructured should work",
			objectToConvert: &testapigroupv1.Carp{
				ObjectMeta: metav1.ObjectMeta{
					Name: "noxu",
				},
				Spec: testapigroupv1.CarpSpec{
					Hostname: "example.com",
				},
			},
			expectedConvertedUnstructured: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Carp",
					"metadata": map[string]interface{}{
						"name": "noxu",
					},
					"spec": map[string]interface{}{
						"hostname": "example.com",
					},
					"status": map[string]interface{}{},
				},
			},
		},
		{
			// Internal 类型不属于任何确定版本，缺少 preferred version 时
			// 无法决定 apiVersion，故报错。
			name:            "convert hub-versioned object to unstructured should fail",
			objectToConvert: &testapigroup.Carp{},
			expectedErr:     fmt.Errorf("unable to convert the internal object type *testapigroup.Carp to Unstructured without providing a preferred version to convert to"),
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// nil target version 表示“以 Out 的类型为准”，在此场景下不参与决策。
			outUnstructured := &unstructured.Unstructured{}
			err := scheme.Convert(testCase.objectToConvert, outUnstructured, nil)
			if err != nil {
				assert.Equal(t, testCase.expectedErr, err)
				return
			}
			assert.Equal(t, testCase.expectedConvertedUnstructured, outUnstructured)
		})
	}
}

// TestUnstructuredToObjectConversion 覆盖反向路径：把 Unstructured 解码进
// 指定对象。对象类型决定目标 GVK，因此目标与 map 中的 kind 必须匹配。
func TestUnstructuredToObjectConversion(t *testing.T) {
	scheme, _ := test.TestScheme()
	testCases := []struct {
		name                    string
		unstructuredToConvert   *unstructured.Unstructured
		convertingObject        runtime.Object
		expectPanic             bool
		expectedErrFunc         func(err error) bool
		expectedConvertedObject runtime.Object
	}{
		{
			// 没有 kind 就无法定位注册类型，这是最常见的输入错误。
			name: "convert empty unstructured w/o gvk to versioned object should fail",
			unstructuredToConvert: &unstructured.Unstructured{
				Object: map[string]interface{}{},
			},
			convertingObject: &testapigroupv1.Carp{},
			expectedErrFunc: func(err error) bool {
				return reflect.DeepEqual(err, runtime.NewMissingKindErr("unstructured object has no kind"))
			},
		},
		{
			// 只有 GVK 没有字段时解码结果就是目标对象的零值。
			name: "convert empty versioned unstructured to versioned object should work",
			unstructuredToConvert: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Carp",
				},
			},
			convertingObject:        &testapigroupv1.Carp{},
			expectedConvertedObject: &testapigroupv1.Carp{},
		},
		{
			// 与前面重复，保留以固定错误信息行为不变。
			name: "convert empty unstructured w/o gvk to versioned object should fail",
			unstructuredToConvert: &unstructured.Unstructured{
				Object: map[string]interface{}{},
			},
			convertingObject: &testapigroupv1.Carp{},
			expectedErrFunc: func(err error) bool {
				return reflect.DeepEqual(err, runtime.NewMissingKindErr("unstructured object has no kind"))
			},
		},
		{
			// 逐字段解码：metadata.name 与 spec.hostname 应被还原。
			name: "convert valid versioned unstructured to versioned object should work",
			unstructuredToConvert: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Carp",
					"metadata": map[string]interface{}{
						"name": "noxu",
					},
					"spec": map[string]interface{}{
						"hostname": "example.com",
					},
					"status": map[string]interface{}{},
				},
			},
			convertingObject: &testapigroupv1.Carp{},
			expectedConvertedObject: &testapigroupv1.Carp{
				ObjectMeta: metav1.ObjectMeta{
					Name: "noxu",
				},
				Spec: testapigroupv1.CarpSpec{
					Hostname: "example.com",
				},
			},
		},
		{
			// 目标是 Internal 类型时会经过转换：先把 v1 解码，再转成 hub 版本。
			name: "convert valid versioned unstructured to hub-versioned object should work",
			unstructuredToConvert: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Carp",
					"metadata": map[string]interface{}{
						"name": "noxu",
					},
					"spec": map[string]interface{}{
						"hostname": "example.com",
					},
					"status": map[string]interface{}{},
				},
			},
			convertingObject: &testapigroup.Carp{},
			expectedConvertedObject: &testapigroup.Carp{
				ObjectMeta: metav1.ObjectMeta{
					Name: "noxu",
				},
				Spec: testapigroup.CarpSpec{
					Hostname: "example.com",
				},
			},
		},
		{
			// v9 未在 scheme 中注册，属于未知版本。
			name: "convert unexisting-versioned unstructured to hub-versioned object should fail",
			unstructuredToConvert: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v9",
					"kind":       "Carp",
					"metadata": map[string]interface{}{
						"name": "noxu",
					},
					"spec": map[string]interface{}{
						"hostname": "example.com",
					},
					"status": map[string]interface{}{},
				},
			},
			convertingObject: &testapigroup.Carp{},
			expectedErrFunc: func(err error) bool {
				return reflect.DeepEqual(err, runtime.NewNotRegisteredGVKErrForTarget(
					scheme.Name(),
					schema.GroupVersionKind{Group: "", Version: "v9", Kind: "Carp"},
					nil,
				))
			},
		},
		{
			// 源是 Carp、目标是 CreateOptions：kind 与目标类型不匹配。
			name: "convert valid versioned unstructured to object w/ a mismatching kind should fail",
			unstructuredToConvert: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Carp",
					"metadata": map[string]interface{}{
						"name": "noxu",
					},
					"spec": map[string]interface{}{
						"hostname": "example.com",
					},
					"status": map[string]interface{}{},
				},
			},
			convertingObject: &metav1.CreateOptions{},
			expectedErrFunc: func(err error) bool {
				return strings.HasPrefix(err.Error(), "converting (v1.Carp) to (v1.CreateOptions):")
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// 部分畸形输入可能触发 panic；用 recover 把“是否 panic”也纳入
			// 期望值，避免测试进程直接崩溃。
			defer func() {
				v := recover()
				assert.Equal(t, testCase.expectPanic, v != nil, "unexpected panic")
			}()
			outObject := testCase.convertingObject.DeepCopyObject()
			// 通过指定目标对象进行转换。
			err := scheme.Convert(testCase.unstructuredToConvert, outObject, nil)
			if err != nil {
				if testCase.expectedErrFunc != nil {
					if !testCase.expectedErrFunc(err) {
						t.Errorf("error mismatched: %v", err)
					}
				}
				return
			}
			assert.Equal(t, testCase.expectedConvertedObject, outObject)
		})
	}
}

// TestUnstructuredToGVConversion 验证 ConvertToVersion 的目标版本选择逻辑：
// 调用方只给 GroupVersion，scheme 必须自行找到该版本对应的 Go 类型。
func TestUnstructuredToGVConversion(t *testing.T) {
	scheme, _ := test.TestScheme()
	// 注册伪造的 foo 组版本，用于构造“目标版本存在但类型不对”的场景；
	// 这不对应真实的 API 组，只服务本测试。
	scheme.AddKnownTypes(schema.GroupVersion{Group: "foo", Version: "v1beta1"}, &testapigroup.Carp{})
	scheme.AddKnownTypes(schema.GroupVersion{Group: "foo", Version: "__internal"}, &testapigroup.Carp{})

	testCases := []struct {
		name                    string
		unstructuredToConvert   *unstructured.Unstructured
		targetGV                schema.GroupVersion
		expectPanic             bool
		expectedErrFunc         func(err error) bool
		expectedConvertedObject runtime.Object
	}{
		{
			// 目标版本与源相同，无需做实际转换，但仍应回填 TypeMeta。
			name: "convert versioned unstructured to valid external version should work",
			unstructuredToConvert: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Carp",
				},
			},
			targetGV: schema.GroupVersion{Group: "", Version: "v1"},
			expectedConvertedObject: &testapigroupv1.Carp{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "v1",
					Kind:       "Carp",
				},
			},
		},
		{
			// Internal 版本没有 apiVersion 的概念，转换后 TypeMeta 为空。
			name: "convert hub-versioned unstructured to hub version should work",
			unstructuredToConvert: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "__internal",
					"kind":       "Carp",
				},
			},
			targetGV:                schema.GroupVersion{Group: "", Version: "__internal"},
			expectedConvertedObject: &testapigroup.Carp{},
		},
		{
			name: "convert empty unstructured w/o gvk to versioned should fail",
			unstructuredToConvert: &unstructured.Unstructured{
				Object: map[string]interface{}{},
			},
			targetGV: schema.GroupVersion{Group: "", Version: "v1"},
			expectedErrFunc: func(err error) bool {
				return reflect.DeepEqual(err, runtime.NewMissingKindErr("unstructured object has no kind"))
			},
			expectedConvertedObject: nil,
		},
		{
			// 目标版本可用但类型别名的 group 不匹配（Carp 属于默认组）。
			name: "convert versioned unstructured to mismatching external version should fail",
			unstructuredToConvert: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Carp",
				},
			},
			targetGV: schema.GroupVersion{Group: "foo", Version: "v1beta1"},
			expectedErrFunc: func(err error) bool {
				return reflect.DeepEqual(err, runtime.NewNotRegisteredErrForTarget(
					scheme.Name(), reflect.TypeOf(testapigroupv1.Carp{}), schema.GroupVersion{Group: "foo", Version: "v1beta1"}))
			},
			expectedConvertedObject: nil,
		},
		{
			// 同样是伪造的 foo 组，确认 Internal 版本也走相同的类型注册检查。
			name: "convert versioned unstructured to mismatching internal version should fail",
			unstructuredToConvert: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Carp",
				},
			},
			targetGV: schema.GroupVersion{Group: "foo", Version: "__internal"},
			expectedErrFunc: func(err error) bool {
				return reflect.DeepEqual(err, runtime.NewNotRegisteredErrForTarget(
					scheme.Name(), reflect.TypeOf(testapigroupv1.Carp{}), schema.GroupVersion{Group: "foo", Version: "__internal"}))
			},
			expectedConvertedObject: nil,
		},
		{
			// 同版本转换必须保留全部字段。
			name: "convert valid versioned unstructured to its own version should work",
			unstructuredToConvert: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Carp",
					"metadata": map[string]interface{}{
						"name": "noxu",
					},
					"spec": map[string]interface{}{
						"hostname": "example.com",
					},
					"status": map[string]interface{}{},
				},
			},
			targetGV: schema.GroupVersion{Group: "", Version: "v1"},
			expectedConvertedObject: &testapigroupv1.Carp{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "v1",
					Kind:       "Carp",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name: "noxu",
				},
				Spec: testapigroupv1.CarpSpec{
					Hostname: "example.com",
				},
			},
		},
		{
			// 转入 Internal 时丢弃 TypeMeta，这是 Internal 类型的约定。
			name: "convert valid versioned unstructured to hub-version should work ignoring type meta",
			unstructuredToConvert: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Carp",
					"metadata": map[string]interface{}{
						"name": "noxu",
					},
					"spec": map[string]interface{}{
						"hostname": "example.com",
					},
					"status": map[string]interface{}{},
				},
			},
			targetGV: schema.GroupVersion{Group: "", Version: "__internal"},
			expectedConvertedObject: &testapigroup.Carp{
				ObjectMeta: metav1.ObjectMeta{
					Name: "noxu",
				},
				Spec: testapigroup.CarpSpec{
					Hostname: "example.com",
				},
			},
		},
		{
			// v9 不在 scheme 中，报 NotRegisteredGVK。
			name: "convert valid versioned unstructured to unexisting version should fail",
			unstructuredToConvert: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Carp",
					"metadata": map[string]interface{}{
						"name": "noxu",
					},
					"spec": map[string]interface{}{
						"hostname": "example.com",
					},
					"status": map[string]interface{}{},
				},
			},
			targetGV: schema.GroupVersion{Group: "", Version: "v9"},
			expectedErrFunc: func(err error) bool {
				return reflect.DeepEqual(err, runtime.NewNotRegisteredGVKErrForTarget(
					scheme.Name(),
					schema.GroupVersionKind{Group: "", Version: "v9", Kind: "Carp"},
					nil,
				))
			},
			expectedConvertedObject: nil,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// 与上一个测试类似，panic 也算一种受控的结果。
			defer func() {
				v := recover()
				assert.Equal(t, testCase.expectPanic, v != nil, "unexpected panic")
			}()
			// 通过指定目标对象进行转换。
			outObject, err := scheme.ConvertToVersion(testCase.unstructuredToConvert, testCase.targetGV)
			if testCase.expectedErrFunc != nil {
				if !testCase.expectedErrFunc(err) {
					t.Errorf("error mismatched: %v", err)
				}
			}
			assert.Equal(t, testCase.expectedConvertedObject, outObject)
		})
	}
}

// TestUnstructuredToUnstructuredConversion 固定当前的宽松行为：两个
// Unstructured 之间的转换只是拷贝，不做任何跨版本语义转换。未来可能
// 收紧为报错，届时需同步调整此测试。
func TestUnstructuredToUnstructuredConversion(t *testing.T) {
	// 最终我们不应进行非结构化对象之间的转换，但目前该转换
	// 只是复制/粘贴。
	scheme, _ := test.TestScheme()
	inUnstructured := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Carp",
		},
	}
	outUnstructured := &unstructured.Unstructured{}
	err := scheme.Convert(inUnstructured, outUnstructured, nil)
	assert.NoError(t, err)
	assert.Equal(t, inUnstructured, outUnstructured)
}

// benchmarkCarp 构造一个字段覆盖较全的 Carp，作为 benchmark 的固定输入，
// 避免把对象构造时间计入计时。
func benchmarkCarp() *testapigroupv1.Carp {
	t := metav1.Date(2015, 1, 1, 12, 0, 0, 0, time.UTC)
	return &testapigroupv1.Carp{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "name",
			Namespace: "namespace",
		},
		Spec: testapigroupv1.CarpSpec{
			RestartPolicy: "restart",
			NodeSelector: map[string]string{
				"label1": "value1",
				"label2": "value2",
			},
			ServiceAccountName: "service-account",
			HostNetwork:        false,
			HostPID:            true,
			Subdomain:          "hostname.subdomain.namespace.svc.domain",
		},
		Status: testapigroupv1.CarpStatus{
			Phase: "phase",
			Conditions: []testapigroupv1.CarpCondition{
				{
					Type:               "condition1",
					Status:             "true",
					LastProbeTime:      t,
					LastTransitionTime: t,
					Reason:             "reason",
					Message:            "message",
				},
			},
			Message: "message",
			Reason:  "reason",
			HostIP:  "1.2.3.4",
		},
	}
}

// BenchmarkToUnstructured 测量有类型对象到 map 的转换。
// 结果始终恰好包含 apiVersion、kind 和 metadata/spec/status 键。
func BenchmarkToUnstructured(b *testing.B) {
	carp := benchmarkCarp()
	converter := runtime.DefaultUnstructuredConverter
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result, err := converter.ToUnstructured(carp)
		if err != nil {
			b.Fatalf("Unexpected conversion error: %v", err)
		}
		if len(result) != 3 {
			b.Errorf("Unexpected conversion result: %#v", result)
		}
	}
}

// BenchmarkFromUnstructured 测量反向转换；关于以下内容的断言
// Status.Phase 用于防止编译器优化掉结果。
func BenchmarkFromUnstructured(b *testing.B) {
	carp := benchmarkCarp()
	converter := runtime.DefaultUnstructuredConverter
	unstr, err := converter.ToUnstructured(carp)
	if err != nil {
		b.Fatalf("Unexpected conversion error: %v", err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		result := testapigroupv1.Carp{}
		if err := converter.FromUnstructured(unstr, &result); err != nil {
			b.Fatalf("Unexpected conversion error: %v", err)
		}
		if result.Status.Phase != "phase" {
			b.Errorf("Unexpected conversion result: %#v", result)
		}
	}
}
