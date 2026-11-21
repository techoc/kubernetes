/*
Copyright 2022 The Kubernetes Authors.

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
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/kubernetes/pkg/apis/resource"
	"k8s.io/utils/ptr"
)

// testClass 构造一个只带合法名称的 DeviceClass，供各场景复用。
func testClass(name string) *resource.DeviceClass {
	return &resource.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
	}
}

// TestValidateClass 验证 DeviceClass 的元数据、扩展资源名、CEL selector 和配置项。
// 同时覆盖声明式校验覆盖标记（MarkCoveredByDeclarative）与命令式校验的对应关系。
func TestValidateClass(t *testing.T) {
	goodName := "foo"
	now := metav1.Now()
	badName := "!@#$%^"
	badValue := "spaces not allowed"

	scenarios := map[string]struct {
		class        *resource.DeviceClass
		wantFailures field.ErrorList
	}{
		"good-class": {
			class: testClass(goodName),
		},
		"missing-name": {
			wantFailures: field.ErrorList{field.Required(field.NewPath("metadata", "name"), "name or generateName is required")},
			class:        testClass(""),
		},
		"bad-name": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("metadata", "name"), badName, "a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')").MarkCoveredByDeclarative()},
			class:        testClass(badName),
		},
		"generate-name": {
			class: func() *resource.DeviceClass {
				class := testClass(goodName)
				class.GenerateName = "pvc-"
				return class
			}(),
		},
		"uid": {
			class: func() *resource.DeviceClass {
				class := testClass(goodName)
				class.UID = "ac051fac-2ead-46d9-b8b4-4e0fbeb7455d"
				return class
			}(),
		},
		"resource-version": {
			class: func() *resource.DeviceClass {
				class := testClass(goodName)
				class.ResourceVersion = "1"
				return class
			}(),
		},
		"generation": {
			class: func() *resource.DeviceClass {
				class := testClass(goodName)
				class.Generation = 100
				return class
			}(),
		},
		"creation-timestamp": {
			class: func() *resource.DeviceClass {
				class := testClass(goodName)
				class.CreationTimestamp = now
				return class
			}(),
		},
		"deletion-grace-period-seconds": {
			class: func() *resource.DeviceClass {
				class := testClass(goodName)
				class.DeletionGracePeriodSeconds = ptr.To(int64(10))
				return class
			}(),
		},
		"owner-references": {
			class: func() *resource.DeviceClass {
				class := testClass(goodName)
				class.OwnerReferences = []metav1.OwnerReference{
					{
						APIVersion: "v1",
						Kind:       "pod",
						Name:       "foo",
						UID:        "ac051fac-2ead-46d9-b8b4-4e0fbeb7455d",
					},
				}
				return class
			}(),
		},
		"finalizers": {
			class: func() *resource.DeviceClass {
				class := testClass(goodName)
				class.Finalizers = []string{
					"example.com/foo",
				}
				return class
			}(),
		},
		"managed-fields": {
			class: func() *resource.DeviceClass {
				class := testClass(goodName)
				class.ManagedFields = []metav1.ManagedFieldsEntry{
					{
						FieldsType: "FieldsV1",
						Operation:  "Apply",
						APIVersion: "apps/v1",
						Manager:    "foo",
					},
				}
				return class
			}(),
		},
		"good-labels": {
			class: func() *resource.DeviceClass {
				class := testClass(goodName)
				class.Labels = map[string]string{
					"apps.kubernetes.io/name": "test",
				}
				return class
			}(),
		},
		"bad-labels": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("metadata", "labels"), badValue, "a valid label must be an empty string or consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyValue',  or 'my_value',  or '12345', regex used for validation is '(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?')")},
			class: func() *resource.DeviceClass {
				class := testClass(goodName)
				class.Labels = map[string]string{
					"hello-world": badValue,
				}
				return class
			}(),
		},
		"good-annotations": {
			class: func() *resource.DeviceClass {
				class := testClass(goodName)
				class.Annotations = map[string]string{
					"foo": "bar",
				}
				return class
			}(),
		},
		"bad-annotations": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("metadata", "annotations"), badName, "name part must consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyName',  or 'my.name',  or '123-abc', regex used for validation is '([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]')")},
			class: func() *resource.DeviceClass {
				class := testClass(goodName)
				class.Annotations = map[string]string{
					badName: "hello world",
				}
				return class
			}(),
		},
		"good-extended-resource-name": {
			class: func() *resource.DeviceClass {
				class := testClass(goodName)
				class.Spec.ExtendedResourceName = ptr.To(string("example.com/gpu"))
				return class
			}(),
		},
		"bad-extended-resource-name": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("spec", "extendedResourceName"), "example.com", "must be a valid extended resource name").MarkCoveredByDeclarative().WithOrigin("format=k8s-extended-resource-name")},
			class: func() *resource.DeviceClass {
				class := testClass(goodName)
				class.Spec.ExtendedResourceName = ptr.To(string("example.com"))
				return class
			}(),
		},
		"selectors": {
			wantFailures: field.ErrorList{
				field.Required(field.NewPath("spec", "selectors").Index(1).Child("cel"), ""),
				field.Invalid(field.NewPath("spec", "selectors").Index(2).Child("cel", "expression"), "noSuchVar", "compilation failed: ERROR: <input>:1:1: undeclared reference to 'noSuchVar' (in container '')\n | noSuchVar\n | ^"),
			},
			class: func() *resource.DeviceClass {
				class := testClass(goodName)
				validSelector := resource.DeviceSelector{
					CEL: &resource.CELDeviceSelector{
						Expression: "true",
					},
				}
				// 三个 selector 分别覆盖：合法、缺少 CEL、CEL 表达式引用未定义变量。
				// 末尾补满到上限，确保只报告预期的三条错误，不会混入 TooMany。
				class.Spec.Selectors = []resource.DeviceSelector{
					validSelector,
					{
						/* 缺少 CEL 字段。 */
					},
					{
						CEL: &resource.CELDeviceSelector{
							Expression: "noSuchVar",
						},
					},
				}
				// 补到数量上限，隔离出“字段内容非法”这一类错误，避免 TooMany 干扰断言。
				for i := len(class.Spec.Selectors); i < resource.DeviceSelectorsMaxSize; i++ {
					class.Spec.Selectors = append(class.Spec.Selectors, validSelector)
				}
				return class
			}(),
		},
		"too-many-selectors": {
			wantFailures: field.ErrorList{
				field.TooMany(field.NewPath("spec", "selectors"), resource.DeviceSelectorsMaxSize+1, resource.DeviceSelectorsMaxSize).WithOrigin("maxItems").MarkCoveredByDeclarative(),
			},
			class: func() *resource.DeviceClass {
				class := testClass(goodName)
				validSelector := resource.DeviceSelector{
					CEL: &resource.CELDeviceSelector{
						Expression: "true",
					},
				}
				// 超过数量上限，验证 TooMany 错误带有 maxItems origin 和声明式覆盖标记。
				for i := 0; i < resource.DeviceSelectorsMaxSize+1; i++ {
					class.Spec.Selectors = append(class.Spec.Selectors, validSelector)
				}
				return class
			}(),
		},
		"configuration": {
			wantFailures: field.ErrorList{
				field.Required(field.NewPath("spec", "config").Index(1).Child("opaque", "driver"), "").MarkCoveredByDeclarative(),
				field.Required(field.NewPath("spec", "config").Index(1).Child("opaque", "parameters"), ""),
				field.Invalid(field.NewPath("spec", "config").Index(2).Child("opaque", "parameters"), "<value omitted>", "error parsing data as JSON: invalid character 'x' looking for beginning of value"),
				field.Invalid(field.NewPath("spec", "config").Index(3).Child("opaque", "parameters"), "<value omitted>", "must be a valid JSON object"),
				field.Required(field.NewPath("spec", "config").Index(4).Child("opaque", "parameters"), ""),
				field.Required(field.NewPath("spec", "config").Index(5).Child("opaque"), ""),
				field.TooLong(field.NewPath("spec", "config").Index(7).Child("opaque", "parameters"), "" /* 该参数未被使用，仅占位。 */, resource.OpaqueParametersMaxLength),
			},
			class: func() *resource.DeviceClass {
				class := testClass(goodName)
				validConfig := resource.DeviceClassConfiguration{
					DeviceConfiguration: resource.DeviceConfiguration{
						Opaque: &resource.OpaqueDeviceConfiguration{
							Driver:     goodName,
							Parameters: runtime.RawExtension{Raw: []byte(`{"foo":42}`)},
						},
					},
				}
				// 逐个构造非法配置：缺字段、非法 JSON、非对象、nil 对象、空配置、超长参数。
				class.Spec.Config = []resource.DeviceClassConfiguration{
					validConfig,
					{
						DeviceConfiguration: resource.DeviceConfiguration{
							Opaque: &resource.OpaqueDeviceConfiguration{ /* 非法：两个字段都是必填项。 */ },
						},
					},
					{
						DeviceConfiguration: resource.DeviceConfiguration{
							Opaque: &resource.OpaqueDeviceConfiguration{
								Driver:     goodName,
								Parameters: runtime.RawExtension{Raw: []byte(`xxx`)}, /* 非法：不是合法 JSON。 */
							},
						},
					},
					{
						DeviceConfiguration: resource.DeviceConfiguration{
							Opaque: &resource.OpaqueDeviceConfiguration{
								Driver:     goodName,
								Parameters: runtime.RawExtension{Raw: []byte(`"hello-world"`)}, /* 非法：不是 JSON 对象。 */
							},
						},
					},
					{
						DeviceConfiguration: resource.DeviceConfiguration{
							Opaque: &resource.OpaqueDeviceConfiguration{
								Driver:     goodName,
								Parameters: runtime.RawExtension{Raw: []byte(`null`)}, /* 非法：nil 对象。 */
							},
						},
					},
					{
						DeviceConfiguration: resource.DeviceConfiguration{ /* 非法：内容为空。 */ },
					},
					{
						DeviceConfiguration: resource.DeviceConfiguration{
							Opaque: &resource.OpaqueDeviceConfiguration{
								Driver:     goodName,
								Parameters: runtime.RawExtension{Raw: []byte(`{"str": "` + strings.Repeat("x", resource.OpaqueParametersMaxLength-9-2) + `"}`)},
							},
						},
					},
					{
						DeviceConfiguration: resource.DeviceConfiguration{
							Opaque: &resource.OpaqueDeviceConfiguration{
								Driver:     goodName,
								Parameters: runtime.RawExtension{Raw: []byte(`{"str": "` + strings.Repeat("x", resource.OpaqueParametersMaxLength-9-2+1 /* 超出一个字节 */) + `"}`)},
							},
						},
					},
				}
				// 补到数量上限，使其他用例只暴露配置内容错误，不被 TooMany 干扰。
				for i := len(class.Spec.Config); i < resource.DeviceConfigMaxSize; i++ {
					class.Spec.Config = append(class.Spec.Config, validConfig)
				}
				return class
			}(),
		},
		"too-many-configs": {
			wantFailures: field.ErrorList{
				field.TooMany(field.NewPath("spec", "config"), resource.DeviceConfigMaxSize+1, resource.DeviceConfigMaxSize).WithOrigin("maxItems").MarkCoveredByDeclarative(),
			},
			class: func() *resource.DeviceClass {
				class := testClass(goodName)
				validConfig := resource.DeviceClassConfiguration{
					DeviceConfiguration: resource.DeviceConfiguration{
						Opaque: &resource.OpaqueDeviceConfiguration{
							Driver:     goodName,
							Parameters: runtime.RawExtension{Raw: []byte(`{"foo":42}`)},
						},
					},
				}
				// 超过数量上限，验证配置列表的 maxItems 限制。
				for i := len(class.Spec.Config); i < resource.DeviceConfigMaxSize+1; i++ {
					class.Spec.Config = append(class.Spec.Config, validConfig)
				}
				return class
			}(),
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			errs := ValidateDeviceClass(scenario.class)
			assertFailures(t, scenario.wantFailures, errs)
		})
	}
}

// TestValidateClassUpdate 验证更新时的配置大小限制：
// 新增超大配置应被拒绝，但历史遗留的超大配置在未被修改时允许通过。
func TestValidateClassUpdate(t *testing.T) {
	validClass := testClass(goodName)

	scenarios := map[string]struct {
		oldClass     *resource.DeviceClass
		update       func(class *resource.DeviceClass) *resource.DeviceClass
		wantFailures field.ErrorList
	}{
		"valid-no-op-update": {
			oldClass: validClass,
			update:   func(class *resource.DeviceClass) *resource.DeviceClass { return class },
		},
		"valid-config-large": {
			oldClass: validClass,
			update: func(class *resource.DeviceClass) *resource.DeviceClass {
				class.Spec.Config = []resource.DeviceClassConfiguration{{
					DeviceConfiguration: resource.DeviceConfiguration{
						Opaque: &resource.OpaqueDeviceConfiguration{
							Driver:     goodName,
							Parameters: runtime.RawExtension{Raw: []byte(`{"str": "` + strings.Repeat("x", resource.OpaqueParametersMaxLength-9-2) + `"}`)},
						},
					},
				}}
				return class
			},
		},
		"invalid-config-too-large": {
			wantFailures: field.ErrorList{
				field.TooLong(field.NewPath("spec", "config").Index(0).Child("opaque", "parameters"), "" /* 该参数未被使用，仅占位。 */, resource.OpaqueParametersMaxLength),
			},
			oldClass: validClass,
			update: func(class *resource.DeviceClass) *resource.DeviceClass {
				class.Spec.Config = []resource.DeviceClassConfiguration{{
					DeviceConfiguration: resource.DeviceConfiguration{
						Opaque: &resource.OpaqueDeviceConfiguration{
							Driver:     goodName,
							Parameters: runtime.RawExtension{Raw: []byte(`{"str": "` + strings.Repeat("x", resource.OpaqueParametersMaxLength-9-2+1 /* 超出一个字节 */) + `"}`)},
						},
					},
				}}
				return class
			},
		},
		"too-large-config-valid-if-stored": {
			oldClass: func() *resource.DeviceClass {
				class := validClass.DeepCopy()
				class.Spec.Config = []resource.DeviceClassConfiguration{{
					DeviceConfiguration: resource.DeviceConfiguration{
						Opaque: &resource.OpaqueDeviceConfiguration{
							Driver:     goodName,
							Parameters: runtime.RawExtension{Raw: []byte(`{"str": "` + strings.Repeat("x", resource.OpaqueParametersMaxLength-9-2+1 /* 超出一个字节 */) + `"}`)},
						},
					},
				}}
				return class
			}(),
			update: func(class *resource.DeviceClass) *resource.DeviceClass {
				// 没有任何改动，应仍然合法。
				return class
			},
		},
	}

	// 每个场景都基于深拷贝执行，避免元数据字段的修改泄漏到其他场景。
	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			// 更新校验不关心 ResourceVersion，只要 spec/status 没有非法变更即可。
			scenario.oldClass.ResourceVersion = "1"
			errs := ValidateDeviceClassUpdate(scenario.update(scenario.oldClass.DeepCopy()), scenario.oldClass)
			assertFailures(t, scenario.wantFailures, errs)
		})
	}
}
