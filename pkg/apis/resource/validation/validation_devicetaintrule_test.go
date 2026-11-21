/*
Copyright 2025 The Kubernetes Authors.

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
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	resourceapi "k8s.io/kubernetes/pkg/apis/resource"
	"k8s.io/utils/ptr"
)

// testDeviceTaintRule 是测试辅助函数：根据给定的 name 与 spec 构造一个 DeviceTaintRule。
// 对 spec 做深拷贝可避免各测试用例共享同一份内存，防止某个用例对 Spec 的修改污染其他用例。
func testDeviceTaintRule(name string, spec resourceapi.DeviceTaintRuleSpec) *resourceapi.DeviceTaintRule {
	return &resourceapi.DeviceTaintRule{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		// 深拷贝隔离各用例的 spec 修改。
		Spec: *spec.DeepCopy(),
	}
}

// validDeviceTaintRuleSpec 提供一个完整合法的基线 spec，各场景在此基础上做最小化修改，
// 这样每次只引入一个变化，便于定位是哪个字段触发了校验错误。
var validDeviceTaintRuleSpec = resourceapi.DeviceTaintRuleSpec{
	DeviceSelector: &resourceapi.DeviceTaintSelector{
		Driver: ptr.To("test.example.com"),
		Pool:   ptr.To(goodName),
		Device: ptr.To(goodName),
	},
	Taint: resourceapi.DeviceTaint{
		Key:    "example.com/taint",
		Value:  "tainted",
		Effect: resourceapi.DeviceTaintEffectNoSchedule,
	},
}

// TestValidateDeviceTaint 以表驱动方式验证 ValidateDeviceTaintRule 对 DeviceTaintRule 的整体校验。
// 表格以 validDeviceTaintRuleSpec 为基线，逐项覆盖：元数据（name、labels、annotations 的合法性，
// 缺失 name 时 generateName 可替代）、服务器管理的元数据字段（uid、resourceVersion、generation、
// creationTimestamp 等应被忽略而不报错），以及 spec 中 deviceSelector 各字段与 taint 的校验。
// 各错误用例都只改动一个字段，以精确定位错误来源；代表性地，bad-name 验证 DNS 子域名约束，
// bad-driver/bad-pool 验证子域名格式，bad-device 验证 DNS label 格式，required-taint 与
// invalid-taint 验证 taint 必填字段与 effect 枚举值。
func TestValidateDeviceTaint(t *testing.T) {
	goodName := "foo"
	now := metav1.Now()
	// 含特殊字符，用作各类"格式非法"场景的输入。
	badName := "!@#$%^"
	// 含空格，用于验证 label value 不允许空格。
	badValue := "spaces not allowed"

	scenarios := map[string]struct {
		taintRule    *resourceapi.DeviceTaintRule
		wantFailures field.ErrorList
	}{
		// 完全合法的对象应无任何错误。
		"good": {
			taintRule: testDeviceTaintRule(goodName, validDeviceTaintRuleSpec),
		},
		// name 与 generateName 均为空时必填校验失败。
		"missing-name": {
			wantFailures: field.ErrorList{field.Required(field.NewPath("metadata", "name"), "name or generateName is required")},
			taintRule:    testDeviceTaintRule("", validDeviceTaintRuleSpec),
		},
		// name 必须为合法的小写 RFC 1123 子域名，特殊字符应被拒绝。
		"bad-name": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("metadata", "name"), badName, "a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')")},
			taintRule:    testDeviceTaintRule(badName, validDeviceTaintRuleSpec),
		},
		// 有 generateName 时可省略 name，这属于合法的元数据。
		"generate-name": {
			taintRule: func() *resourceapi.DeviceTaintRule {
				taintRule := testDeviceTaintRule(goodName, validDeviceTaintRuleSpec)
				taintRule.GenerateName = "pvc-"
				return taintRule
			}(),
		},
		// 以下 uid/resource-version/generation/creation-timestamp 等属于服务端管理的元数据，
		// 用户不能修改，校验时应忽略这些字段而不报错。
		"uid": {
			taintRule: func() *resourceapi.DeviceTaintRule {
				taintRule := testDeviceTaintRule(goodName, validDeviceTaintRuleSpec)
				taintRule.UID = "ac051fac-2ead-46d9-b8b4-4e0fbeb7455d"
				return taintRule
			}(),
		},
		"resource-version": {
			taintRule: func() *resourceapi.DeviceTaintRule {
				taintRule := testDeviceTaintRule(goodName, validDeviceTaintRuleSpec)
				taintRule.ResourceVersion = "1"
				return taintRule
			}(),
		},
		"generation": {
			taintRule: func() *resourceapi.DeviceTaintRule {
				taintRule := testDeviceTaintRule(goodName, validDeviceTaintRuleSpec)
				taintRule.Generation = 100
				return taintRule
			}(),
		},
		"creation-timestamp": {
			taintRule: func() *resourceapi.DeviceTaintRule {
				taintRule := testDeviceTaintRule(goodName, validDeviceTaintRuleSpec)
				taintRule.CreationTimestamp = now
				return taintRule
			}(),
		},
		"deletion-grace-period-seconds": {
			taintRule: func() *resourceapi.DeviceTaintRule {
				taintRule := testDeviceTaintRule(goodName, validDeviceTaintRuleSpec)
				taintRule.DeletionGracePeriodSeconds = ptr.To(int64(10))
				return taintRule
			}(),
		},
		// ownerReferences、finalizers、managedFields 同为服务端管理的元数据，
		// 即使包含非空值也不应触发用户输入校验错误。
		"owner-references": {
			taintRule: func() *resourceapi.DeviceTaintRule {
				taintRule := testDeviceTaintRule(goodName, validDeviceTaintRuleSpec)
				taintRule.OwnerReferences = []metav1.OwnerReference{
					{
						APIVersion: "v1",
						Kind:       "pod",
						Name:       "foo",
						UID:        "ac051fac-2ead-46d9-b8b4-4e0fbeb7455d",
					},
				}
				return taintRule
			}(),
		},
		"finalizers": {
			taintRule: func() *resourceapi.DeviceTaintRule {
				taintRule := testDeviceTaintRule(goodName, validDeviceTaintRuleSpec)
				taintRule.Finalizers = []string{
					"example.com/foo",
				}
				return taintRule
			}(),
		},
		"managed-fields": {
			taintRule: func() *resourceapi.DeviceTaintRule {
				taintRule := testDeviceTaintRule(goodName, validDeviceTaintRuleSpec)
				taintRule.ManagedFields = []metav1.ManagedFieldsEntry{
					{
						FieldsType: "FieldsV1",
						Operation:  "Apply",
						APIVersion: "apps/v1",
						Manager:    "foo",
					},
				}
				return taintRule
			}(),
		},
		// 用户可设置的 labels/annotations 才需要校验：合法值通过，非法值报错。
		"good-labels": {
			taintRule: func() *resourceapi.DeviceTaintRule {
				taintRule := testDeviceTaintRule(goodName, validDeviceTaintRuleSpec)
				taintRule.Labels = map[string]string{
					"apps.kubernetes.io/name": "test",
				}
				return taintRule
			}(),
		},
		// label value 含空格，不符合 label 值格式，应报错。
		"bad-labels": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("metadata", "labels"), badValue, "a valid label must be an empty string or consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyValue',  or 'my_value',  or '12345', regex used for validation is '(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?')")},
			taintRule: func() *resourceapi.DeviceTaintRule {
				taintRule := testDeviceTaintRule(goodName, validDeviceTaintRuleSpec)
				taintRule.Labels = map[string]string{
					"hello-world": badValue,
				}
				return taintRule
			}(),
		},
		"good-annotations": {
			taintRule: func() *resourceapi.DeviceTaintRule {
				taintRule := testDeviceTaintRule(goodName, validDeviceTaintRuleSpec)
				taintRule.Annotations = map[string]string{
					"foo": "bar",
				}
				return taintRule
			}(),
		},
		// annotation 的 key 含非法字符，应报错。
		"bad-annotations": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("metadata", "annotations"), badName, "name part must consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyName',  or 'my.name',  or '123-abc', regex used for validation is '([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]')")},
			taintRule: func() *resourceapi.DeviceTaintRule {
				taintRule := testDeviceTaintRule(goodName, validDeviceTaintRuleSpec)
				taintRule.Annotations = map[string]string{
					badName: "hello world",
				}
				return taintRule
			}(),
		},
		// deviceSelector 的 driver/pool 须为 DNS 子域名，device 须为 DNS label，
		// 特殊字符均应被拒绝。
		"bad-driver": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("spec", "deviceSelector", "driver"), badName, "a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')")},
			taintRule: func() *resourceapi.DeviceTaintRule {
				taintRule := testDeviceTaintRule(goodName, validDeviceTaintRuleSpec)
				taintRule.Spec.DeviceSelector.Driver = ptr.To(badName)
				return taintRule
			}(),
		},
		// pool 字段含非法字符，应报错。
		"bad-pool": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("spec", "deviceSelector", "pool"), badName, "a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')")},
			taintRule: func() *resourceapi.DeviceTaintRule {
				taintRule := testDeviceTaintRule(goodName, validDeviceTaintRuleSpec)
				taintRule.Spec.DeviceSelector.Pool = ptr.To(badName)
				return taintRule
			}(),
		},
		// device 字段含非法字符，应报错。
		"bad-device": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("spec", "deviceSelector", "device"), badName, "a lowercase RFC 1123 label must consist of lower case alphanumeric characters or '-', and must start and end with an alphanumeric character (e.g. 'my-name',  or '123-abc', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?')")},
			taintRule: func() *resourceapi.DeviceTaintRule {
				taintRule := testDeviceTaintRule(goodName, validDeviceTaintRuleSpec)
				taintRule.Spec.DeviceSelector.Device = ptr.To(badName)
				return taintRule
			}(),
		},
		// 对 DeviceTaint 仅做最小化测试；validateDeviceTaint 的完整覆盖在 ResourceSlice 测试中。
		"valid-taint": {
			taintRule: func() *resourceapi.DeviceTaintRule {
				claim := testDeviceTaintRule(goodName, validDeviceTaintRuleSpec)
				claim.Spec.Taint = resourceapi.DeviceTaint{
					Key:    goodName,
					Value:  goodName,
					Effect: resourceapi.DeviceTaintEffectNoExecute,
				}
				return claim
			}(),
		},
		// effect 为必填字段，缺失时报错；该错误由声明式校验覆盖。
		"required-taint": {
			wantFailures: field.ErrorList{
				field.Required(field.NewPath("spec", "taint", "effect"), "").MarkCoveredByDeclarative(),
			},
			taintRule: func() *resourceapi.DeviceTaintRule {
				claim := testDeviceTaintRule(goodName, validDeviceTaintRuleSpec)
				claim.Spec.Taint = resourceapi.DeviceTaint{
					Key:   goodName,
					Value: goodName,
				}
				return claim
			}(),
		},
		// effect 必须为 NoExecute/NoSchedule/None 之一，"some-other-effect" 不受支持，应报错。
		"invalid-taint": {
			wantFailures: field.ErrorList{
				field.NotSupported(field.NewPath("spec", "taint", "effect"), resourceapi.DeviceTaintEffect("some-other-effect"), []resourceapi.DeviceTaintEffect{resourceapi.DeviceTaintEffectNoExecute, resourceapi.DeviceTaintEffectNoSchedule, resourceapi.DeviceTaintEffectNone}).MarkCoveredByDeclarative(),
			},
			taintRule: func() *resourceapi.DeviceTaintRule {
				claim := testDeviceTaintRule(goodName, validDeviceTaintRuleSpec)
				claim.Spec.Taint = resourceapi.DeviceTaint{
					Effect: "some-other-effect",
					Key:    goodName,
					Value:  goodName,
				}
				return claim
			}(),
		},
	}

	for name, scenario := range scenarios {
		// 每个场景作为子测试独立运行，便于定位失败的具体用例。
		t.Run(name, func(t *testing.T) {
			errs := ValidateDeviceTaintRule(scenario.taintRule)
			// 将实际错误与期望错误逐一比对。
			assertFailures(t, scenario.wantFailures, errs)
		})
	}
}

// TestValidateDeviceTaintUpdate 验证 DeviceTaintRule 更新时的校验规则。
// 表格覆盖四个场景：无操作更新（合法）、修改 name（不可变字段，应报错）、
// 对原本 effect 非法的旧对象做无关字段更新（合法，因为错误是既有的、非本次引入）、
// 以及本次更新引入非法 effect（应报错）。
// 核心不变量是：name 一经创建不可修改；只有本次更新新引入的非法值才应拒绝，
// 已存在的旧非法值在无关更新中应被容忍，以免阻断修复其他字段。
func TestValidateDeviceTaintUpdate(t *testing.T) {
	name := "valid"
	validTaintRule := testDeviceTaintRule(name, validDeviceTaintRuleSpec)
	invalidTaintEffectRule := validTaintRule.DeepCopy()
	invalidTaintEffectRule.Spec.Taint.Effect = "some-other-effect"

	scenarios := map[string]struct {
		old          *resourceapi.DeviceTaintRule
		update       func(patch *resourceapi.DeviceTaintRule) *resourceapi.DeviceTaintRule
		wantFailures field.ErrorList
	}{
		// 未作任何改动的更新应直接通过。
		"valid-no-op-update": {
			old:    validTaintRule,
			update: func(taintRule *resourceapi.DeviceTaintRule) *resourceapi.DeviceTaintRule { return taintRule },
		},
		// name 为不可变字段，修改后应报 "field is immutable"。
		"invalid-name-update": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("metadata", "name"), name+"-update", "field is immutable")},
			old:          validTaintRule,
			update: func(taintRule *resourceapi.DeviceTaintRule) *resourceapi.DeviceTaintRule {
				taintRule.Name += "-update"
				return taintRule
			},
		},
		// 旧对象 effect 已非法，本次只改 labels：既有的非法值不应阻断无关更新。
		"valid-existing-unknown-effect": {
			old: invalidTaintEffectRule,
			update: func(taintRule *resourceapi.DeviceTaintRule) *resourceapi.DeviceTaintRule {
				taintRule.Labels = map[string]string{"a": "b"}
				return taintRule
			},
		},
		// 本次更新新引入非法 effect，应报错。
		"invalid-new-unknown-effect": {
			wantFailures: field.ErrorList{field.NotSupported(field.NewPath("spec", "taint", "effect"), resourceapi.DeviceTaintEffect("some-other-effect"), []resourceapi.DeviceTaintEffect{resourceapi.DeviceTaintEffectNoExecute, resourceapi.DeviceTaintEffectNoSchedule, resourceapi.DeviceTaintEffectNone})}.MarkCoveredByDeclarative(),
			old:          validTaintRule,
			update: func(taintRule *resourceapi.DeviceTaintRule) *resourceapi.DeviceTaintRule {
				taintRule.Spec.Taint.Effect = "some-other-effect"
				return taintRule
			},
		},
	}

	for name, scenario := range scenarios {
		// 每个场景作为子测试独立运行，便于定位失败的具体用例。
		t.Run(name, func(t *testing.T) {
			// 设置相同的 ResourceVersion 并深拷贝旧对象作为更新基线，
			// 确保 update 函数只改动它想验证的字段，不污染共享的 old 对象。
			scenario.old.ResourceVersion = "1"
			errs := ValidateDeviceTaintRuleUpdate(scenario.update(scenario.old.DeepCopy()), scenario.old)
			// 将实际错误与期望错误逐一比对。
			assertFailures(t, scenario.wantFailures, errs)
		})
	}
}
