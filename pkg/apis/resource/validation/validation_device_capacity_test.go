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
	"math"
	"testing"

	apiresource "k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation/field"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/apis/resource"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/utils/ptr"
)

// testDeviceCapacity 构造一个 DeviceCapacity，供表驱动测试简洁表达“设备容量 + 其请求策略”。
// value 是容量，policy 为可选策略（nil 表示不设置策略）。
func testDeviceCapacity(value apiresource.Quantity, policy *resource.CapacityRequestPolicy) resource.DeviceCapacity {
	return resource.DeviceCapacity{
		Value:         value,
		RequestPolicy: policy,
	}
}

// testCapacityRequestPolicy 把默认值、候选值列表与取值范围打包成 CapacityRequestPolicy。
// 三个参数均可选：只传 defaultValue 表示“仅设默认值”的空约束策略；
// 同时传 validValues 与 validRange 则用于触发两者的互斥校验错误。
func testCapacityRequestPolicy(defaultValue *apiresource.Quantity,
	validValues []apiresource.Quantity,
	validRange *resource.CapacityRequestPolicyRange) *resource.CapacityRequestPolicy {
	return &resource.CapacityRequestPolicy{
		Default:     defaultValue,
		ValidValues: validValues,
		ValidRange:  validRange,
	}
}

// testValidRange 构造 CapacityRequestPolicyRange（min/max/step 三元组）。
// 任一字段传 nil 表示不设置该约束，借此覆盖“缺 min”“缺 max”等不同错误场景。
func testValidRange(min *apiresource.Quantity, max, step *apiresource.Quantity) *resource.CapacityRequestPolicyRange {
	return &resource.CapacityRequestPolicyRange{
		Min:  min,
		Max:  max,
		Step: step,
	}
}

// TestValidateDeviceCapacity 用表驱动方式覆盖 validateMultiAllocatableDeviceCapacity 的各校验分支：
// 请求策略的必填与互斥规则、默认值与候选值的合法性、取值范围的上下界与步长对齐，
// 以及与 DRAFractionalCapacityRange 特性开关相关的小数（毫级）与 int64 溢出边界行为。
func TestValidateDeviceCapacity(t *testing.T) {
	// 同一个数量 1024 的两种写法：缩写 "1Ki" 与展开 "1024"。
	// 用于验证重复值检测基于归一化后的数值（AsDec），而非原始字面量。
	oneKIAbbreviated := apiresource.MustParse("1Ki")
	oneKIUnabbreviated := apiresource.MustParse("1024")

	// 小数值 0.1 / 0.2 / 1.0，用于小数容量与小数步长的对齐计算。
	hundredMilli := apiresource.MustParse("100m")
	twoHundredMilli := apiresource.MustParse("200m")
	oneUnit := apiresource.MustParse("1")

	// 以 Gi 为单位的常规取值：1、2、容量上限 10Gi、超限 20Gi。
	one := apiresource.MustParse("1Gi")
	two := apiresource.MustParse("2Gi")
	maxCapacity := apiresource.MustParse("10Gi")
	overCapacity := apiresource.MustParse("20Gi")

	zero := apiresource.MustParse("0")
	negOne := apiresource.MustParse("-1")
	// 以下三个量用于探测步长校验里的 int64 边界。
	maxInt64Q := *apiresource.NewQuantity(math.MaxInt64, apiresource.DecimalSI) // Value() 能忠实读出的最大上界
	maxInt64Plus1 := maxInt64Q.DeepCopy()                                       // 首个放不进 int64 的上界
	maxInt64Plus1.Add(*apiresource.NewQuantity(1, apiresource.DecimalSI))
	twoPow64 := apiresource.MustParse("18446744073709551616") // 2^64 的正整数倍，Value() 会将其读成 0（曾导致除零）

	// 预构造错误路径，避免每个场景重复拼字符串；Child 逐级追加字段段。
	capacityField := field.NewPath("spec", "devices", "capacity")
	policyField := capacityField.Child("requestPolicy")
	validValuesField := policyField.Child("validValues")
	validRangeField := policyField.Child("validRange")

	// 每个场景声明：待测容量、是否开启小数容量范围特性开关、以及期望的错误列表。
	// wantFailures 为 nil 表示该场景应当校验通过。
	scenarios := map[string]struct {
		capacity                    resource.DeviceCapacity
		fractionalCapacityRangeGate bool
		wantFailures                field.ErrorList
	}{
		// 完全不设置策略：合法，没有任何约束。
		"no-policy": {
			capacity: testDeviceCapacity(one, nil),
		},
		// 策略对象存在但字段全空：等价于无约束，default 不是必填。
		"policy-without-default": {
			capacity: testDeviceCapacity(one, testCapacityRequestPolicy(nil, nil, nil)),
		},
		// 定义了 validValues 就必须给出 default，否则报 Required。
		"policy-with-valid-values-without-default": {
			capacity: testDeviceCapacity(one, testCapacityRequestPolicy(nil, []apiresource.Quantity{one}, nil)),
			wantFailures: field.ErrorList{
				field.Required(policyField.Child("default"), "required when validValues is defined"),
			},
		},
		// 同理，定义了 validRange 也必须给出 default。
		"policy-with-valid-range-without-default": {
			capacity: testDeviceCapacity(one, testCapacityRequestPolicy(nil, nil, testValidRange(ptr.To(one), nil, nil))),
			wantFailures: field.ErrorList{
				field.Required(policyField.Child("default"), "required when validRange is defined"),
			},
		},
		// validRange 一旦出现，min 就是必填字段。
		"policy-with-valid-range-without-min": {
			capacity: testDeviceCapacity(one, testCapacityRequestPolicy(&one, nil, testValidRange(nil, nil, nil))),
			wantFailures: field.ErrorList{
				field.Required(validRangeField.Child("min"), "required when validRange is defined"),
			},
		},
		// 只设 min，default 等于 min，合法。
		"valid-simple-range": {
			capacity: testDeviceCapacity(maxCapacity, testCapacityRequestPolicy(&one, nil, testValidRange(ptr.To(one), nil, nil))),
		},
		// min + max，且 default 与 max 均不超过容量，合法。
		"valid-range-with-max": {
			capacity: testDeviceCapacity(maxCapacity, testCapacityRequestPolicy(&one, nil, testValidRange(ptr.To(one), ptr.To(maxCapacity), nil))),
		},
		// min + step，default 落在 min 上且与步长对齐，合法。
		"valid-range-with-step": {
			capacity: testDeviceCapacity(maxCapacity, testCapacityRequestPolicy(&one, nil, testValidRange(ptr.To(one), nil, ptr.To(one)))),
		},
		// min + max + step 三者齐全的常规合法配置。
		"valid-range-with-max-and-step": {
			capacity: testDeviceCapacity(maxCapacity, testCapacityRequestPolicy(&one, nil, testValidRange(ptr.To(one), ptr.To(maxCapacity), ptr.To(one)))),
		},
		// 候选值只有一个且等于 default，合法。
		"valid-single-option": {
			capacity: testDeviceCapacity(maxCapacity, testCapacityRequestPolicy(&one, []apiresource.Quantity{one}, nil)),
		},
		// 两个升序候选值且包含 default，合法。
		"valid-two-options": {
			capacity: testDeviceCapacity(maxCapacity, testCapacityRequestPolicy(&one, []apiresource.Quantity{one, maxCapacity}, nil)),
		},
		// 策略里只设 default、无其他约束，合法。
		"default-without-policy": {
			capacity: testDeviceCapacity(maxCapacity, testCapacityRequestPolicy(&one, nil, nil)),
		},
		// validValues 与 validRange 互斥，同时出现报 Forbidden。
		"more-than-one-policy": {
			capacity: testDeviceCapacity(maxCapacity, testCapacityRequestPolicy(&one,
				[]apiresource.Quantity{one}, testValidRange(ptr.To(one), nil, nil))),
			wantFailures: field.ErrorList{
				field.Forbidden(policyField, `exactly one policy can be specified, cannot specify "validValues" and "validRange" at the same time`),
			},
		},
		// 候选值 20Gi 超过容量 10Gi；同时 default(1Gi) 不在候选列表中，触发两条错误。
		"invalid-options": {
			capacity: testDeviceCapacity(maxCapacity, testCapacityRequestPolicy(&one, []apiresource.Quantity{overCapacity}, nil)),
			wantFailures: field.ErrorList{
				field.Invalid(validValuesField.Index(0), "20Gi", "option is larger than capacity value: 10Gi"),
				field.Invalid(validValuesField, "1Gi", "default value is not valid according to the requestPolicy"),
			},
		},
		"valid-fractional-values": {
			// 100m(0.1)、200m(0.2)、1 严格递增且都不超过 maxCapacity；AsDec 会把它们规范化为互不相等的值
			capacity:                    testDeviceCapacity(maxCapacity, testCapacityRequestPolicy(&hundredMilli, []apiresource.Quantity{hundredMilli, twoHundredMilli, oneUnit}, nil)),
			fractionalCapacityRangeGate: true,
		},
		// 两个完全相同的值，第二个被视为重复。
		"invalid-options-duplicate": {
			capacity: testDeviceCapacity(maxCapacity, testCapacityRequestPolicy(&one, []apiresource.Quantity{one, one}, nil)),
			wantFailures: field.ErrorList{
				field.Duplicate(validValuesField.Index(1), "1073741824"), // 1Gi 的十进制展开值
			},
		},
		// "1Ki" 与 "1024" 字面量不同但归一化后相等，重复检测必须基于数值。
		"invalid-options-duplicate-normalized": {
			capacity: testDeviceCapacity(maxCapacity, testCapacityRequestPolicy(&oneKIAbbreviated, []apiresource.Quantity{oneKIAbbreviated, oneKIUnabbreviated}, nil)),
			wantFailures: field.ErrorList{
				field.Duplicate(validValuesField.Index(1), "1024"),
			},
		},
		// 候选值必须升序：2Gi 在前、1Gi 在后，Index(1) 报错。
		"invalid-options-unsort": {
			capacity: testDeviceCapacity(maxCapacity, testCapacityRequestPolicy(&one, []apiresource.Quantity{two, one}, nil)),
			wantFailures: field.ErrorList{
				field.Invalid(validValuesField.Index(1), one.String(), "values must be sorted in ascending order"),
			},
		},
		// min 超容量、default 小于 min、min 大于 max、default 大于 max，四条错误同时报出。
		"invalid-range-large-min-small-max": {
			capacity: testDeviceCapacity(maxCapacity, testCapacityRequestPolicy(&two, nil, testValidRange(ptr.To(overCapacity), ptr.To(one), nil))),
			wantFailures: field.ErrorList{
				field.Invalid(validRangeField.Child("min"), "20Gi", "min is larger than capacity value: 10Gi"),
				field.Invalid(validRangeField.Child("default"), "2Gi", "default is less than min: 20Gi"),
				field.Invalid(validRangeField.Child("max"), "20Gi", "min is larger than max: 1Gi"),
				field.Invalid(validRangeField.Child("default"), "2Gi", "default is more than max: 1Gi"),
			},
		},
		// max 超过容量，报错。
		"invalid-range-large-max": {
			capacity: testDeviceCapacity(maxCapacity, testCapacityRequestPolicy(&one, nil, testValidRange(ptr.To(one), ptr.To(overCapacity), nil))),
			wantFailures: field.ErrorList{
				field.Invalid(validRangeField.Child("max"), "20Gi", "max is larger than capacity value: 10Gi"),
			},
		},
		// default 与 max 都不在从 min 出发、按 step 递增的序列上，报两条对齐错误。
		"invalid-range-multiple-of-step": {
			capacity: testDeviceCapacity(maxCapacity, testCapacityRequestPolicy(&two, nil, testValidRange(ptr.To(one), ptr.To(maxCapacity), ptr.To(two)))),
			wantFailures: field.ErrorList{
				field.Invalid(validRangeField.Child("step"), "2Gi", "value is not a multiple of a given step (2Gi) from (1Gi)"),
				field.Invalid(validRangeField.Child("step"), "10Gi", "value is not a multiple of a given step (2Gi) from (1Gi)"),
			},
		},
		// step 是 2^64 的倍数，Value() 读成 0 曾导致除零；须在倍数计算前拒绝。
		"range-step-multiple-of-2^64": {
			// step 是 2^64 的正整数倍，Value() 会把它读成 0；
			// 步长倍数检查曾用它作除数导致除零，必须在进入该检查前拒绝。
			capacity: testDeviceCapacity(twoPow64, testCapacityRequestPolicy(ptr.To(zero), nil, testValidRange(ptr.To(zero), nil, ptr.To(twoPow64)))),
			wantFailures: field.ErrorList{
				field.Invalid(validRangeField.Child("step"), "18446744073709551616", "must not be larger than 9223372036854775807"),
			},
		},
		// step 恰为 int64 最大值，Value() 可忠实读出，应当接受。
		"range-step-at-int64-max": {
			// MaxInt64 是 Value() 能忠实读出的最大步长，应当被接受。
			capacity: testDeviceCapacity(maxInt64Q, testCapacityRequestPolicy(ptr.To(zero), nil, testValidRange(ptr.To(zero), nil, ptr.To(maxInt64Q)))),
		},
		// 比 int64 最大值大 1，放不进 int64，应当拒绝。
		"range-step-int64-max-plus-one": {
			// 比 MaxInt64 大 1 是首个放不进 int64 的步长，应当被拒绝。
			capacity: testDeviceCapacity(maxInt64Plus1, testCapacityRequestPolicy(ptr.To(zero), nil, testValidRange(ptr.To(zero), nil, ptr.To(maxInt64Plus1)))),
			wantFailures: field.ErrorList{
				field.Invalid(validRangeField.Child("step"), "9223372036854775808", "must not be larger than 9223372036854775807"),
			},
		},
		// 带 step 时负的 min 会进入 int64 运算，十进制负数可能被读回正数，需提前拒绝。
		"range-negative-min-with-step": {
			// 带 step 时负的 min 会进入 int64 运算；
			// 十进制存储的负数可能被读回正数，因此提前拒绝。
			capacity: testDeviceCapacity(maxCapacity, testCapacityRequestPolicy(ptr.To(zero), nil, testValidRange(ptr.To(negOne), nil, ptr.To(one)))),
			wantFailures: field.ErrorList{
				field.Invalid(validRangeField.Child("min"), "-1", "must be greater than or equal to 0"),
			},
		},
		// 范围为整数但 default 是小数：必须走毫级分支，否则整数分支会取整而漏报未对齐。
		"range-fractional-default-integer-range": {
			// 范围是整数但 default 是小数时，仍必须走毫级分支；
			// 整数分支会把 default 取整，从而错误地接受未对齐的值。
			capacity:                    testDeviceCapacity(maxCapacity, testCapacityRequestPolicy(ptr.To(hundredMilli), nil, testValidRange(ptr.To(zero), nil, ptr.To(oneUnit)))),
			fractionalCapacityRangeGate: true,
			wantFailures: field.ErrorList{
				field.Invalid(validRangeField.Child("step"), "100m", "value is not a multiple of a given step (1) from (0)"),
			},
		},
		// 没有 step 时不会以 int64 读取边界，负的 min 与 default 沿用旧行为被接受。
		"range-no-step-negative-bounds-accepted": {
			// 没有 step 时不会以 int64 读取边界，因此负的 min 和
			// default 会沿用既有行为被接受。
			capacity: testDeviceCapacity(maxCapacity, testCapacityRequestPolicy(ptr.To(negOne), nil, testValidRange(ptr.To(negOne), nil, nil))),
		},
		// min=0.2、step=0.1、max=1、default=0.2：均落在 0.2+k*0.1 上，合法。
		"valid-range-fractional-step": {
			// min=0.2、step=0.1、max=1、default=0.2、capacity=1：0.2 = min+0*0.1，1.0 = min+8*0.1
			capacity: testDeviceCapacity(
				apiresource.MustParse("1"),
				testCapacityRequestPolicy(
					ptr.To(apiresource.MustParse("200m")),
					nil,
					testValidRange(
						ptr.To(apiresource.MustParse("200m")),
						ptr.To(apiresource.MustParse("1")),
						ptr.To(apiresource.MustParse("100m")),
					),
				),
			),
			fractionalCapacityRangeGate: true,
		},
		// default=0.25 不是从 0.2 起、步长 0.1 的整数倍；开关开启才走毫级运算并报错。
		"invalid-range-fractional-step-not-aligned": {
			// default=0.25 不是从 0.2 起、步长 0.1 的整数倍；需开启开关才走毫级运算并报错
			capacity: testDeviceCapacity(
				apiresource.MustParse("1"),
				testCapacityRequestPolicy(
					ptr.To(apiresource.MustParse("250m")),
					nil,
					testValidRange(
						ptr.To(apiresource.MustParse("200m")),
						ptr.To(apiresource.MustParse("1")),
						ptr.To(apiresource.MustParse("100m")),
					),
				),
			),
			fractionalCapacityRangeGate: true,
			wantFailures: field.ErrorList{
				field.Invalid(validRangeField.Child("step"), "250m", "value is not a multiple of a given step (100m) from (200m)"),
			},
		},
		// 从 min(1Gi) 走一步 10Gi 得 11Gi，超过容量 10Gi，报错。
		"invalid-range-large-step": {
			capacity: testDeviceCapacity(maxCapacity, testCapacityRequestPolicy(&one, nil, testValidRange(ptr.To(one), nil, ptr.To(maxCapacity)))),
			wantFailures: field.ErrorList{
				field.Invalid(validRangeField.Child("step"), "10Gi", "one step 11Gi is larger than capacity value: 10Gi"),
			},
		},
		// 开关关闭：虽有小数，既不做溢出检查，也不做毫级对齐校验。
		"fractional-range-gate-disabled-valid": {
			// 开关关闭，尽管存在小数值：既不做溢出检查，也不做毫级对齐校验
			capacity: testDeviceCapacity(
				apiresource.MustParse("1"),
				testCapacityRequestPolicy(
					ptr.To(apiresource.MustParse("200m")),
					nil,
					testValidRange(
						ptr.To(apiresource.MustParse("200m")),
						ptr.To(apiresource.MustParse("1")),
						ptr.To(apiresource.MustParse("100m")),
					),
				),
			),
			// 此处 fractionalCapacityRangeGate 为 false（默认值）
		},
		// 开关关闭时 hasFractional 为 false，整数运算把小数都当 1，检测不到未对齐。
		"fractional-range-gate-disabled-misaligned": {
			// 开关关闭，存在小数值时 hasFractional 仍为 false，整数运算
			// 把所有小于 1 的值都当作 1，因此检测不到未对齐
			capacity: testDeviceCapacity(
				apiresource.MustParse("1"),
				testCapacityRequestPolicy(
					ptr.To(apiresource.MustParse("250m")),
					nil,
					testValidRange(
						ptr.To(apiresource.MustParse("200m")),
						ptr.To(apiresource.MustParse("1")),
						ptr.To(apiresource.MustParse("100m")),
					),
				),
			),
			// 此处 fractionalCapacityRangeGate 为 false（默认值）—— 期望无错误
		},
		// default=20P、min=10P 超出 MaxMilliValue，开关开启且范围为小数时必须拒绝。
		"fractional-range-overflow-min": {
			// min 超过 MaxMilliValue：开关开启且范围为小数时必须拒绝
			capacity: testDeviceCapacity(
				apiresource.MustParse("100P"),
				testCapacityRequestPolicy(
					ptr.To(apiresource.MustParse("20P")),
					nil,
					testValidRange(ptr.To(apiresource.MustParse("10P")), nil, ptr.To(apiresource.MustParse("100m"))),
				),
			),
			fractionalCapacityRangeGate: true,
			wantFailures: field.ErrorList{
				field.Invalid(validRangeField.Child("default"), "20P", "value cannot be represented as a milli value"),
				field.Invalid(validRangeField.Child("min"), "10P", "value cannot be represented as a milli value"),
			},
		},
		// 微级精度（10u/20u）无法用毫表示，同样报错。
		"fractional-too-fine-precision": {
			capacity: testDeviceCapacity(
				apiresource.MustParse("1"),
				testCapacityRequestPolicy(
					ptr.To(apiresource.MustParse("20u")),
					nil,
					testValidRange(ptr.To(apiresource.MustParse("10u")), nil, nil),
				),
			),
			fractionalCapacityRangeGate: true,
			wantFailures: field.ErrorList{
				field.Invalid(validRangeField.Child("default"), "20u", "value cannot be represented as a milli value"),
				field.Invalid(validRangeField.Child("min"), "10u", "value cannot be represented as a milli value"),
			},
		},
	}
	// 逐个场景：按场景设置特性开关，再调用校验函数并比对期望错误。
	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DRAFractionalCapacityRange, scenario.fractionalCapacityRangeGate)
			// 被测函数：校验单个多分配设备容量的请求策略。
			errs := validateMultiAllocatableDeviceCapacity(scenario.capacity, capacityField)
			// 断言实际错误列表与期望列表一致。
			assertFailures(t, scenario.wantFailures, errs)
		})
	}
}
