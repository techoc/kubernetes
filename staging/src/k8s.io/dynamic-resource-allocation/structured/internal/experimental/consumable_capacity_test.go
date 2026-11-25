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

package experimental

import (
	"math"
	"testing"

	. "github.com/onsi/gomega"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// 测试用的固定标识符：驱动名、设备池名、设备名以及两个容量名。
const (
	driverA   = "driver-a"
	pool1     = "pool-1"
	device1   = "device-1"
	capacity0 = "capacity-0"
	capacity1 = "capacity-1"
)

// 测试用的 Quantity 常量：整数边界值与毫值小数，覆盖可表示与不可表示的边界。
var (
	one        = resource.MustParse("1")
	two        = resource.MustParse("2")
	three      = resource.MustParse("3")
	zero       = resource.MustParse("0")
	maxInt64Q  = resource.MustParse("9223372036854775807")
	maxInt64P1 = resource.MustParse("9223372036854775808")
	twoPow64P3 = resource.MustParse("18446744073709551619")
	maxInt64M1 = resource.MustParse("9223372036854775806")

	pointTwoFive = resource.MustParse("250m")
	pointFour    = resource.MustParse("400m")
	pointThree   = resource.MustParse("300m")
	pointTwo     = resource.MustParse("200m")
	pointOne     = resource.MustParse("100m")

	// tooBigForMilli 的 MilliValue() 会溢出 int64，用于测试毫值转换失败的拒绝路径。
	tooBigForMilli = resource.MustParse("9224372036854776E3") // 约 9.22e21，远高于 MaxInt64（9.22e18）
)

// deviceConsumedCapacity 为指定设备构造仅含 capacity0=1 的 DeviceConsumedCapacity，
// 供消费容量集合的插入/删除测试复用。
func deviceConsumedCapacity(deviceID DeviceID) DeviceConsumedCapacity {
	capaicty := map[resourceapi.QualifiedName]resource.Quantity{
		capacity0: one,
	}
	return NewDeviceConsumedCapacity(deviceID, capaicty)
}

// TestConsumableCapacity 汇总消费容量的增删及请求解析相关子测试。
func TestConsumableCapacity(t *testing.T) {

	// 子测试：验证 ConsumedCapacity 的 Add/Sub 行为——从空开始，加一次变为非空，减回零后再次为空。
	t.Run("add-sub-allocating-consumed-capacity", func(t *testing.T) {
		g := NewWithT(t)
		allocatedCapacity := NewConsumedCapacity()
		g.Expect(allocatedCapacity.Empty()).To(BeTrueBecause("allocated capacity should start from zero"))
		oneAllocated := ConsumedCapacity{
			capacity0: &one,
		}
		allocatedCapacity.Add(oneAllocated)
		g.Expect(allocatedCapacity.Empty()).To(BeFalseBecause("capacity is added"))
		allocatedCapacity.Sub(oneAllocated)
		g.Expect(allocatedCapacity.Empty()).To(BeTrueBecause("capacity is subtracted to zero"))
	})

	// 子测试：验证 ConsumedCapacityCollection 的 Insert/Remove——同一设备插入两次容量累加为 2，
	// 删除一次后回落为 1。
	t.Run("insert-remove-allocating-consumed-capacity-collection", func(t *testing.T) {
		g := NewWithT(t)
		deviceID := MakeDeviceID(driverA, pool1, device1)
		aggregatedCapacity := NewConsumedCapacityCollection()
		aggregatedCapacity.Insert(deviceConsumedCapacity(deviceID))
		aggregatedCapacity.Insert(deviceConsumedCapacity(deviceID))
		allocatedCapacity, found := aggregatedCapacity[deviceID]
		g.Expect(found).To(BeTrueBecause("expected deviceID to be found"))
		g.Expect(allocatedCapacity[capacity0].Cmp(two)).To(BeZero())
		aggregatedCapacity.Remove(deviceConsumedCapacity(deviceID))
		g.Expect(allocatedCapacity[capacity0].Cmp(one)).To(BeZero())
	})

	// 子测试：验证 GetConsumedCapacityFromRequest 在有请求值、仅有 default、无策略三种情况下的取值：
	// 有请求取请求值，无请求取 default，无策略取容量值；断言三者都得到 1。
	t.Run("get-consumed-capacity-from-request", func(t *testing.T) {
		requestedCapacity := &resourceapi.CapacityRequirements{
			Requests: map[resourceapi.QualifiedName]resource.Quantity{
				capacity0: one,
				"dummy":   one,
			},
		}
		consumableCapacity := map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
			capacity0: { // 有 request 且有默认值时，期望使用请求值
				// 有请求且有 default：期望取请求值（此处请求值为 1）。
				Value: two,
				RequestPolicy: &resourceapi.CapacityRequestPolicy{
					Default:    &two,
					ValidRange: &resourceapi.CapacityRequestPolicyRange{Min: &one},
				},
			},
			capacity1: { // 无 request 但有默认值时，期望使用默认值
				// 无请求但有 default：期望取 default（此处为 1）。
				Value: two,
				RequestPolicy: &resourceapi.CapacityRequestPolicy{
					Default:    &one,
					ValidRange: &resourceapi.CapacityRequestPolicyRange{Min: &one},
				},
			},
			"dummy": {
				// 无请求且无策略（无 default）：期望取容量值（此处为 1）。
				Value: one, // 无 request 且无策略（无默认值）时，期望使用容量值
			},
		}
		g := NewWithT(t)
		consumedCapacity, err := GetConsumedCapacityFromRequest(requestedCapacity, consumableCapacity, false)
		// 断言无错误，且返回 3 项容量，每项名字合法且值为 1。
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(consumedCapacity).To(HaveLen(3))
		for name, val := range consumedCapacity {
			g.Expect(string(name)).Should(BeElementOf([]string{capacity0, capacity1, "dummy"}))
			g.Expect(val.Cmp(one)).To(BeZero())
		}
	})

	// 以下子测试分别委托给对应的表驱动测试函数。
	t.Run("violate-capacity-sharing", testViolateCapacityRequestPolicy)

	t.Run("calculate-consumed-capacity", testCalculateConsumedCapacity)

	t.Run("safe-milli-value", testSafeMilliValue)

	t.Run("use-milli", testUseMilli)

	t.Run("cmp-request-over-capacity-fatal-beats-soft", testCmpRequestOverCapacityFatalBeatsSoft)
}

// testCmpRequestOverCapacityFatalBeatsSoft 验证“致命错误优先于软失败”：同一设备上既有违反策略/
// 超容量的软不匹配，也有请求不可表示的致命错误时，应返回致命错误。输入中 capacity0 的请求 2
// 超出容量值 1（软），capacity1 的请求 MaxInt64 按步长 2 向上取整会溢出（致命）。断言重复 64 次
// 结果稳定为 ok=false 且 err 为 errCapacityRequestNotRepresentable，规避 Go map 遍历顺序的不确定性。
func testCmpRequestOverCapacityFatalBeatsSoft(t *testing.T) {
	g := NewWithT(t)
	capacity := map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
		capacity0: {Value: one}, // 无策略；请求值 2 超过值 1（soft 模式）
		capacity1: {
			Value: maxInt64P1,
			RequestPolicy: &resourceapi.CapacityRequestPolicy{
				ValidRange: &resourceapi.CapacityRequestPolicyRange{Min: &zero, Step: &two},
			},
		},
	}
	request := &resourceapi.CapacityRequirements{
		Requests: map[resourceapi.QualifiedName]resource.Quantity{
			capacity0: two,       // 超过 capacity0 的值 1：soft 模式，跳过该设备
			capacity1: maxInt64Q, // 将 MaxInt64 向上取整到步长 2 会超过 MaxInt64：致命错误
		},
	}
	// 重复 64 次，确保结果不依赖 map 遍历顺序。
	for range 64 {
		ok, err := CmpRequestOverCapacity(NewConsumedCapacity(), request, nil, capacity, NewConsumedCapacity(), false)
		// 断言不可满足，且错误为可表示性错误而非软失败。
		g.Expect(ok).To(BeFalseBecause("an unrepresentable request must not be considered satisfiable"))
		g.Expect(err).To(MatchError(errCapacityRequestNotRepresentable), "a representability error must take precedence over the soft over-capacity mismatch")
	}
}

// testSafeMilliValue 表驱动测试 safeMilliValue：输入为整数 1、毫值 100m 和超出毫值范围的超大值，
// 断言前两者无损转换成 1000 与 100，超大值返回错误。
func testSafeMilliValue(t *testing.T) {
	// q 为输入 Quantity，expectMilli 为期望毫值，expectErr 表示是否期望转换失败。
	testcases := map[string]struct {
		q           resource.Quantity
		expectMilli int64
		expectErr   bool
	}{
		"whole number": {
			q:           one,
			expectMilli: 1000,
		},
		"fractional milli": {
			q:           pointOne,
			expectMilli: 100,
		},
		"too large for milli (overflows int64)": {
			q:         tooBigForMilli,
			expectErr: true,
		},
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			g := NewWithT(t)
			got, err := safeMilliValue(tc.q)
			// 期望失败时只断言有错误；否则断言无错误且毫值等于期望值。
			if tc.expectErr {
				g.Expect(err).To(HaveOccurred())
			} else {
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(got).To(Equal(tc.expectMilli))
			}
		})
	}
}

// testUseMilli 表驱动测试 useMilli 的判定条件：特性关闭、全为毫值小数、全为整数、Min 溢出毫值、
// Step 小于 1m 等场景，断言是否正确选择（或不选择）毫值算术路径。
func testUseMilli(t *testing.T) {
	// validRange 为区间输入，fractionalCapacityRange 为特性开关，expectUseMilli 为期望判定。
	testcases := map[string]struct {
		validRange              resourceapi.CapacityRequestPolicyRange
		fractionalCapacityRange bool
		expectUseMilli          bool
	}{
		// 特性关闭：即便区间是小数且可表示，也返回 false。
		"fractional disabled": {
			validRange: resourceapi.CapacityRequestPolicyRange{
				Min: &pointTwo, Max: &one, Step: &pointOne,
			},
			fractionalCapacityRange: false,
			expectUseMilli:          false,
		},
		// 全为小数且都在毫值范围内：使用毫值算术。
		"all fractional, fits in milli": {
			validRange: resourceapi.CapacityRequestPolicyRange{
				Min: &pointTwo, Max: &one, Step: &pointOne,
			},
			fractionalCapacityRange: true,
			expectUseMilli:          true,
		},
		// 全为整数但可表示为毫值：当前实现不再要求“必须有小数”，返回 true。
		"all integer, fits in milli (no hasFractional gate any more)": {
			validRange: resourceapi.CapacityRequestPolicyRange{
				Min: &one, Max: &three, Step: &one,
			},
			fractionalCapacityRange: true,
			expectUseMilli:          true,
		},
		// Min 超出毫值范围：无法无损转换，退回整数路径。
		"min overflows milli, falls back to integer": {
			validRange: resourceapi.CapacityRequestPolicyRange{
				Min: &tooBigForMilli, Step: &one,
			},
			fractionalCapacityRange: true,
			expectUseMilli:          false,
		},
		// Step 小于 1m：即便 safeMilliValue 成功（可能舍入为 0），也不满足最小步长要求。
		"step too small (sub-milli)": {
			validRange: resourceapi.CapacityRequestPolicyRange{
				Min:  &one,
				Step: func() *resource.Quantity { q := resource.MustParse("0"); return &q }(),
			},
			fractionalCapacityRange: true,
			expectUseMilli:          false,
		},
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			g := NewWithT(t)
			// 断言 useMilli 的返回值与期望一致。
			got := useMilli(&tc.validRange, tc.fractionalCapacityRange)
			g.Expect(got).To(Equal(tc.expectUseMilli))
		})
	}
}

// testViolateCapacityRequestPolicy 表驱动测试 violatesPolicy：覆盖无约束、区间上下界、合法值集合、
// 小数步长对齐、请求值无法转成毫值等场景，断言是否正确判定违反策略。
func testViolateCapacityRequestPolicy(t *testing.T) {
	// requestedVal 为请求值，requestPolicy 为策略，fractionalCapacityRange 为特性开关，
	// expectResult 为期望的“违反”结果。
	testcases := map[string]struct {
		requestedVal            resource.Quantity
		requestPolicy           *resourceapi.CapacityRequestPolicy
		fractionalCapacityRange bool
		expectResult            bool
	}{
		// 无策略：不违反。
		"no constraint": {requestedVal: one, expectResult: false},
		// 请求值在 [Min, Max] 范围内：不违反。
		"less than maximum": {
			requestedVal: one,
			requestPolicy: &resourceapi.CapacityRequestPolicy{
				Default:    &one,
				ValidRange: &resourceapi.CapacityRequestPolicyRange{Min: &one, Max: &two},
			},
			expectResult: false,
		},
		// 请求值超过 Max：违反区间策略。
		"more than maximum": {
			requestedVal: two,
			requestPolicy: &resourceapi.CapacityRequestPolicy{
				Default:    &one,
				ValidRange: &resourceapi.CapacityRequestPolicyRange{Min: &one, Max: &one},
			},
			expectResult: true,
		},
		// 请求值在合法值集合中：不违反。
		"in set": {
			requestedVal: one,
			requestPolicy: &resourceapi.CapacityRequestPolicy{
				Default:     &one,
				ValidValues: []resource.Quantity{one},
			},
			expectResult: false,
		},
		// 请求值不在合法值集合中：违反。
		"not in set": {
			requestedVal: two,
			requestPolicy: &resourceapi.CapacityRequestPolicy{
				Default:     &one,
				ValidValues: []resource.Quantity{one},
			},
			expectResult: true,
		},
		// 小数步长对齐：0.3 = 0.2 + 1*0.1，不违反。
		"fractional step aligned (0.3 = min+1*step)": {
			requestedVal: pointThree,
			requestPolicy: &resourceapi.CapacityRequestPolicy{
				Default: &pointTwo,
				ValidRange: &resourceapi.CapacityRequestPolicyRange{
					Min:  &pointTwo,
					Max:  &one,
					Step: &pointOne,
				},
			},
			fractionalCapacityRange: true,
			expectResult:            false,
		},
		// 小数步长未对齐：0.25 不是从 0.2 出发的 0.1 整数倍，违反。
		"fractional step not aligned (0.25 is not a multiple of 0.1 from 0.2)": {
			requestedVal: pointTwoFive,
			requestPolicy: &resourceapi.CapacityRequestPolicy{
				Default: &pointTwo,
				ValidRange: &resourceapi.CapacityRequestPolicyRange{
					Min:  &pointTwo,
					Max:  &one,
					Step: &pointOne,
				},
			},
			fractionalCapacityRange: true,
			expectResult:            true,
		},
		// 请求值无法无损转成毫值：useMilli 判定使用毫值路径后，safeMilliValue 报错，判为违反。
		"requested value overflows milli representation": {
			requestedVal: tooBigForMilli,
			requestPolicy: &resourceapi.CapacityRequestPolicy{
				Default: &one,
				ValidRange: &resourceapi.CapacityRequestPolicyRange{
					Min:  &one,
					Step: &one,
				},
			},
			fractionalCapacityRange: true,
			expectResult:            true,
		},
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			g := NewWithT(t)
			// 断言 violatesPolicy 的判定结果与期望一致。
			violate := violatesPolicy(tc.requestedVal, tc.requestPolicy, tc.fractionalCapacityRange)
			g.Expect(violate).To(BeEquivalentTo(tc.expectResult))
		})
	}
}

// testCalculateConsumedCapacity 表驱动测试 calculateConsumedCapacity：覆盖无请求、无策略、
// Min/Default/Step/合法值集合、小数取整以及整数与毫值的溢出边界，断言返回的消费量或致命错误。
func testCalculateConsumedCapacity(t *testing.T) {
	// requestedVal 为请求值（nil 表示无请求），capacityValue 为设备容量，requestPolicy 为策略，
	// fractionalCapacityRange 为特性开关，expectResult 为期望消费量，expectErr 表示是否期望不可表示错误。
	testcases := map[string]struct {
		requestedVal            *resource.Quantity
		capacityValue           resource.Quantity
		requestPolicy           *resourceapi.CapacityRequestPolicy
		fractionalCapacityRange bool
		expectResult            resource.Quantity
		expectErr               bool
	}{
		// 无请求且无 default：期望返回容量值。
		"empty": {requestedVal: nil, capacityValue: one, requestPolicy: &resourceapi.CapacityRequestPolicy{}, expectResult: one},
		// 请求超过 MaxInt64：无法用整数算术安全读取，期望致命错误。
		"request-above-maxint64-is-rejected": {
			requestedVal:  &twoPow64P3,
			capacityValue: twoPow64P3,
			requestPolicy: &resourceapi.CapacityRequestPolicy{Default: &zero, ValidRange: &resourceapi.CapacityRequestPolicyRange{Min: &zero, Step: &three}},
			expectErr:     true,
		},
		// 取整后超过 MaxInt64：期望致命错误而非回绕值。
		"rounded-value-above-maxint64-is-rejected": {
			requestedVal:  &maxInt64Q,
			capacityValue: maxInt64P1,
			requestPolicy: &resourceapi.CapacityRequestPolicy{Default: &zero, ValidRange: &resourceapi.CapacityRequestPolicyRange{Min: &zero, Step: &two}},
			expectErr:     true,
		},
		// 安全边界：取整结果恰好等于 MaxInt64，仍可表示，应被接受。
		"max-safe-boundary-rounds-to-maxint64": {
			requestedVal:  &maxInt64M1,
			capacityValue: maxInt64Q,
			requestPolicy: &resourceapi.CapacityRequestPolicy{Default: &one, ValidRange: &resourceapi.CapacityRequestPolicyRange{Min: &one, Step: &two}},
			expectResult:  maxInt64Q,
		},
		// 无请求、有 default：期望返回 default。
		"min in range": {
			requestedVal:  nil,
			capacityValue: two,
			requestPolicy: &resourceapi.CapacityRequestPolicy{Default: &one, ValidRange: &resourceapi.CapacityRequestPolicyRange{Min: &one}},
			expectResult:  one,
		},
		// 无请求、Default 在合法值集合中：期望返回 Default。
		"default in set": {
			requestedVal:  nil,
			capacityValue: two,
			requestPolicy: &resourceapi.CapacityRequestPolicy{Default: &one, ValidValues: []resource.Quantity{one}},
			expectResult:  one,
		},
		// 请求大于 Min 且无步长：原样返回请求值。
		"more than min in range": {
			requestedVal:  &two,
			capacityValue: two,
			requestPolicy: &resourceapi.CapacityRequestPolicy{Default: &one, ValidRange: &resourceapi.CapacityRequestPolicyRange{Min: &one}},
			expectResult:  two,
		},
		// 请求小于 Min：返回下界 Min。
		"less than min in range": {
			requestedVal:  &one,
			capacityValue: two,
			requestPolicy: &resourceapi.CapacityRequestPolicy{Default: &one, ValidRange: &resourceapi.CapacityRequestPolicyRange{Min: &two}},
			expectResult:  two,
		},
		// 指定步长：请求向上取整到下一个步长倍数。
		"with step (round up)": {
			requestedVal:  &two,
			capacityValue: three,
			requestPolicy: &resourceapi.CapacityRequestPolicy{Default: &one, ValidRange: &resourceapi.CapacityRequestPolicyRange{Min: &one, Step: &two}},
			expectResult:  three,
		},
		// 指定步长但请求已对齐：无需再取整。
		"with step (no remaining)": {
			requestedVal:  &two,
			capacityValue: two,
			requestPolicy: &resourceapi.CapacityRequestPolicy{Default: &one, ValidRange: &resourceapi.CapacityRequestPolicyRange{Min: &one, Step: &one}},
			expectResult:  two,
		},
		// 小数取整：0.25 向上取整到 0.3（300m）。
		"fractional step round up (0.25 to 0.3)": {
			requestedVal:  &pointTwoFive,
			capacityValue: resource.MustParse("1"),
			requestPolicy: &resourceapi.CapacityRequestPolicy{
				Default: &pointTwo,
				ValidRange: &resourceapi.CapacityRequestPolicyRange{
					Min:  &pointTwo,
					Max:  &one,
					Step: &pointOne,
				},
			},
			fractionalCapacityRange: true,
			expectResult:            resource.MustParse("300m"),
		},
		// 小数请求已对齐步长：原样返回 0.4（400m）。
		"fractional step already aligned (0.4 = min+2*step)": {
			requestedVal:  &pointFour,
			capacityValue: resource.MustParse("1"),
			requestPolicy: &resourceapi.CapacityRequestPolicy{
				Default: &pointTwo,
				ValidRange: &resourceapi.CapacityRequestPolicyRange{
					Min:  &pointTwo,
					Max:  &one,
					Step: &pointOne,
				},
			},
			fractionalCapacityRange: true,
			expectResult:            resource.MustParse("400m"),
		},
		// 合法值集合：请求值本身在集合中，原样返回。
		"valid value in set": {
			requestedVal:  &two,
			capacityValue: three,
			requestPolicy: &resourceapi.CapacityRequestPolicy{Default: &one, ValidValues: []resource.Quantity{one, two, three}},
			expectResult:  two,
		},
		// 合法值集合：请求值不在集合中，向上取到更大的合法值。
		"set (round up)": {
			requestedVal:  &two,
			capacityValue: three,
			requestPolicy: &resourceapi.CapacityRequestPolicy{Default: &one, ValidValues: []resource.Quantity{one, three}},
			expectResult:  three,
		},
		// 请求值大于所有合法值：原样返回请求值。
		"larger than set": {
			requestedVal:  &three,
			capacityValue: three,
			requestPolicy: &resourceapi.CapacityRequestPolicy{Default: &one, ValidValues: []resource.Quantity{one, two}},
			expectResult:  three,
		},
		// 小数步长下请求无法转成毫值：期望致命错误。
		"fractional-step-request-not-milli-representable-is-rejected": {
			requestedVal:  &tooBigForMilli,
			capacityValue: tooBigForMilli,
			requestPolicy: &resourceapi.CapacityRequestPolicy{
				Default: &one,
				ValidRange: &resourceapi.CapacityRequestPolicyRange{
					Min:  &pointOne,
					Step: &pointOne,
				},
			},
			fractionalCapacityRange: true,
			expectErr:               true,
		},
		// 请求可表示为毫值，但取整结果溢出毫值范围：期望致命错误而非截断值。
		"fractional-step-rounded-value-passes-milli-range-is-rejected": {
			requestedVal: func() *resource.Quantity {
				q := resource.NewMilliQuantity(math.MaxInt64-1, resource.DecimalSI)
				return q
			}(),
			capacityValue: *resource.NewMilliQuantity(math.MaxInt64, resource.DecimalSI),
			requestPolicy: &resourceapi.CapacityRequestPolicy{
				Default: &pointOne,
				ValidRange: &resourceapi.CapacityRequestPolicyRange{
					Min:  &pointOne,
					Step: &pointOne,
				},
			},
			fractionalCapacityRange: true,
			expectErr:               true,
		},
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			g := NewWithT(t)
			// 用用例输入的容量值与策略构造 DeviceCapacity。
			capacity := resourceapi.DeviceCapacity{
				Value:         tc.capacityValue,
				RequestPolicy: tc.requestPolicy,
			}
			consumedCapacity, err := calculateConsumedCapacity(tc.requestedVal, capacity, tc.fractionalCapacityRange)
			// 期望错误时断言为不可表示错误；否则断言无错误且消费量等于期望值。
			if tc.expectErr {
				g.Expect(err).To(MatchError(errCapacityRequestNotRepresentable))
			} else {
				g.Expect(err).NotTo(HaveOccurred())
				g.Expect(consumedCapacity.Cmp(tc.expectResult)).To(BeZero())
			}
		})
	}
}
