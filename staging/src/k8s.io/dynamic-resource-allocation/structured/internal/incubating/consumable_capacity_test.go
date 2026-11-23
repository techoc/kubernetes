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

// Package incubating 包含孵化阶段的容量与池逻辑，本文件是其容量相关的单元测试。
package incubating

import (
	"math"
	"testing"

	. "github.com/onsi/gomega"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// driverA、pool1、device1、capacity0、capacity1 是测试用例共用的名称常量。
//   - driverA 是测试用的驱动名；
//   - pool1 是测试用的池名；
//   - device1 是测试用的设备名；
//   - capacity0 是测试用的第一个容量名；
//   - capacity1 是测试用的第二个容量名。
const (
	driverA   = "driver-a"
	pool1     = "pool-1"
	device1   = "device-1"
	capacity0 = "capacity-0"
	capacity1 = "capacity-1"
)

// one、two、three、zero、maxInt64Q、maxInt64P1、twoPow64P3、maxInt64M1、pointTwoFive、pointFour、pointThree、pointTwo、pointOne、tooBigForMilli 是测试中使用的共享 Quantity 值，涵盖整数、边界值与小数（毫值）。
//   - one 表示数量 1；two 表示数量 2；three 表示数量 3；zero 表示数量 0；
//   - maxInt64Q 是恰好等于 MaxInt64 的数量；maxInt64P1 是 MaxInt64+1；
//   - twoPow64P3 是 2^64+3，远超 MaxInt64；maxInt64M1 是 MaxInt64-1；
//   - pointTwoFive 是 250m（0.25）；pointFour 是 400m（0.4）；pointThree 是 300m（0.3）；
//   - pointTwo 是 200m（0.2）；pointOne 是 100m（0.1）；
//   - tooBigForMilli 的 MilliValue() 会溢出 int64，用于测试毫值表示失败的分支。
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

	// tooBigForMilli is a value whose MilliValue() overflows int64 (> MaxInt64/1000).
	// resource.MustParse uses DecimalSI by default for large integers.
	// tooBigForMilli 的毫值表示会溢出 int64，用于覆盖毫值转换失败的分支。
	tooBigForMilli = resource.MustParse("9224372036854776E3") // ~9.22e21, well above MaxInt64 (9.22e18)
)

// deviceConsumedCapacity 构造某设备已消耗容量（capacity0=1），供集合的插入/移除测试使用。
func deviceConsumedCapacity(deviceID DeviceID) DeviceConsumedCapacity {
	// 构造包含 capacity0=1 的容量映射。
	capaicty := map[resourceapi.QualifiedName]resource.Quantity{
		capacity0: one,
	}
	// 生成该设备对应的已消耗容量对象。
	return NewDeviceConsumedCapacity(deviceID, capaicty)
}

// TestConsumableCapacity 汇总可消费容量的子测试：加减、集合插入移除、由请求计算以及委托的子测试集。
func TestConsumableCapacity(t *testing.T) {

	// 子测试：验证 ConsumedCapacity 的 Add/Sub 与 Empty 判定。
	t.Run("add-sub-allocating-consumed-capacity", func(t *testing.T) {
		g := NewWithT(t)
		allocatedCapacity := NewConsumedCapacity()
		// 初始容量应为空。
		g.Expect(allocatedCapacity.Empty()).To(BeTrueBecause("allocated capacity should start from zero"))
		oneAllocated := ConsumedCapacity{
			capacity0: &one,
		}
		// 加入 capacity0=1。
		allocatedCapacity.Add(oneAllocated)
		// 加入后不再为空。
		g.Expect(allocatedCapacity.Empty()).To(BeFalseBecause("capacity is added"))
		// 减去相同容量。
		allocatedCapacity.Sub(oneAllocated)
		// 减回零后应重新为空。
		g.Expect(allocatedCapacity.Empty()).To(BeTrueBecause("capacity is subtracted to zero"))
	})

	// 子测试：验证 ConsumedCapacityCollection 按设备聚合，Insert 累加、Remove 扣减容量。
	t.Run("insert-remove-allocating-consumed-capacity-collection", func(t *testing.T) {
		g := NewWithT(t)
		deviceID := MakeDeviceID(driverA, pool1, device1)
		// 创建空的已消耗容量集合。
		aggregatedCapacity := NewConsumedCapacityCollection()
		// 第一次插入：容量应为 1。
		aggregatedCapacity.Insert(deviceConsumedCapacity(deviceID))
		// 第二次插入同一设备：容量应累加为 2。
		aggregatedCapacity.Insert(deviceConsumedCapacity(deviceID))
		// 按设备 ID 查询聚合结果。
		allocatedCapacity, found := aggregatedCapacity[deviceID]
		// 设备应能被查到。
		g.Expect(found).To(BeTrueBecause("expected deviceID to be found"))
		// 断言累加结果为 2。
		g.Expect(allocatedCapacity[capacity0].Cmp(two)).To(BeZero())
		// 移除一次后容量应回到 1。
		aggregatedCapacity.Remove(deviceConsumedCapacity(deviceID))
		// 断言扣减结果为 1。
		g.Expect(allocatedCapacity[capacity0].Cmp(one)).To(BeZero())
	})

	// 子测试：验证 GetConsumedCapacityFromRequest 的取值优先级：请求值 > 策略默认值 > 容量值。
	t.Run("get-consumed-capacity-from-request", func(t *testing.T) {
		// 请求 capacity0=1，并附带设备未定义的 dummy 容量。
		requestedCapacity := &resourceapi.CapacityRequirements{
			Requests: map[resourceapi.QualifiedName]resource.Quantity{
				capacity0: one,
				"dummy":   one,
			},
		}
		// 定义三个容量及其请求策略。
		consumableCapacity := map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
			// 有请求且有默认值时期望采用请求值。
			capacity0: { // with request and with default, expect requested value
				Value: two,
				RequestPolicy: &resourceapi.CapacityRequestPolicy{
					Default:    &two,
					ValidRange: &resourceapi.CapacityRequestPolicyRange{Min: &one},
				},
			},
			// 无请求但有默认值时期望采用默认值。
			capacity1: { // no request but with default, expect default
				Value: two,
				RequestPolicy: &resourceapi.CapacityRequestPolicy{
					Default:    &one,
					ValidRange: &resourceapi.CapacityRequestPolicyRange{Min: &one},
				},
			},
			// 无请求且无策略时期望采用容量值。
			"dummy": {
				Value: one, // no request and no policy (no default), expect capacity value
			},
		}
		g := NewWithT(t)
		// 由请求计算各容量的消耗值。
		consumedCapacity, err := GetConsumedCapacityFromRequest(requestedCapacity, consumableCapacity, false)
		// 计算不应出错。
		g.Expect(err).NotTo(HaveOccurred())
		// 应返回全部三个容量。
		g.Expect(consumedCapacity).To(HaveLen(3))
		// 逐一校验容量名与取值。
		for name, val := range consumedCapacity {
			// 名称应属于预期的三个之一。
			g.Expect(string(name)).Should(BeElementOf([]string{capacity0, capacity1, "dummy"}))
			// 三种情况下结果都应为 1。
			g.Expect(val.Cmp(one)).To(BeZero())
		}
	})

	// 子测试：委托 testViolateCapacityRequestPolicy 校验请求策略的违反判定。
	t.Run("violate-capacity-sharing", testViolateCapacityRequestPolicy)

	// 子测试：委托 testCalculateConsumedCapacity 校验消耗容量计算与不可表示错误。
	t.Run("calculate-consumed-capacity", testCalculateConsumedCapacity)

	// 子测试：委托 testSafeMilliValue 校验毫值转换的无损性判断。
	t.Run("safe-milli-value", testSafeMilliValue)

	// 子测试：委托 testUseMilli 校验毫值算术的启用条件。
	t.Run("use-milli", testUseMilli)

	// 子测试：委托 testCmpRequestOverCapacityFatalBeatsSoft 校验致命错误优先于软失败。
	t.Run("cmp-request-over-capacity-fatal-beats-soft", testCmpRequestOverCapacityFatalBeatsSoft)
}

// testCmpRequestOverCapacityFatalBeatsSoft pins that a representability error takes
// precedence over a soft policy or capacity mismatch on the same device. The allocator
// enforces that precedence by resolving representability for all capacities before it
// runs the soft checks, so the outcome does not depend on Go's unspecified map order.
// testCmpRequestOverCapacityFatalBeatsSoft 验证不可表示错误优先于同一设备上的软失败；通过重复运行规避 Go map 遍历顺序的不确定性。
func testCmpRequestOverCapacityFatalBeatsSoft(t *testing.T) {
	g := NewWithT(t)
	// capacity0 的请求 2 超过其值 1 属软失败；capacity1 的取整会越过 MaxInt64 属致命错误。
	capacity := map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
		capacity0: {Value: one}, // no policy; the request of 2 over-fills the value of 1 (soft)
		capacity1: {
			Value: maxInt64P1,
			RequestPolicy: &resourceapi.CapacityRequestPolicy{
				ValidRange: &resourceapi.CapacityRequestPolicyRange{Min: &zero, Step: &two},
			},
		},
	}
	// 请求中 capacity0 超容量（软），capacity1 触发不可表示（致命）。
	request := &resourceapi.CapacityRequirements{
		Requests: map[resourceapi.QualifiedName]resource.Quantity{
			capacity0: two,       // over capacity0's value of 1: soft, skip this device
			capacity1: maxInt64Q, // rounding MaxInt64 up to the next step of 2 passes MaxInt64: fatal
		},
	}
	// Go's map order is unspecified, so run the check repeatedly to make an
	// order-dependent regression very likely to surface rather than to rely on one order.
	// 重复 64 次，使顺序相关的回归更可能暴露。
	for range 64 {
		// 执行容量比较。
		ok, err := CmpRequestOverCapacity(NewConsumedCapacity(), request, nil, capacity, NewConsumedCapacity(), false)
		// 不可表示的请求不能被认为可满足。
		g.Expect(ok).To(BeFalseBecause("an unrepresentable request must not be considered satisfiable"))
		// 必须返回不可表示错误，证明致命错误优先于软失败。
		g.Expect(err).To(MatchError(errCapacityRequestNotRepresentable), "a representability error must take precedence over the soft over-capacity mismatch")
	}
}

// testSafeMilliValue 表驱动测试 safeMilliValue：整数、可无损表示的小数与溢出 int64 的毫值。
func testSafeMilliValue(t *testing.T) {
	// 每个用例包含输入 Quantity、期望毫值与是否期望错误。
	testcases := map[string]struct {
		q           resource.Quantity
		expectMilli int64
		expectErr   bool
	}{
		// 整数 1 对应 1000m。
		"whole number": {
			q:           one,
			expectMilli: 1000,
		},
		// 100m 对应毫值 100，转换无损。
		"fractional milli": {
			q:           pointOne,
			expectMilli: 100,
		},
		// 超过 MaxInt64/1000 的值无法无损转为毫值，期望错误。
		"too large for milli (overflows int64)": {
			q:         tooBigForMilli,
			expectErr: true,
		},
	}
	// 以用例名作为子测试名运行。
	for name, tc := range testcases {
		// 每个用例独立运行。
		t.Run(name, func(t *testing.T) {
			g := NewWithT(t)
			// 执行毫值转换。
			got, err := safeMilliValue(tc.q)
			// 期望错误时只断言错误发生。
			if tc.expectErr {
				g.Expect(err).To(HaveOccurred())
			} else {
				// 否则先断言无错误。
				g.Expect(err).NotTo(HaveOccurred())
				// 再断言毫值等于期望。
				g.Expect(got).To(Equal(tc.expectMilli))
			}
		})
	}
}

// testUseMilli 表驱动测试 useMilli 的启用条件：特性开关、无损表示与步长至少 1m。
func testUseMilli(t *testing.T) {
	// 每个用例包含合法区间、特性开关与是否期望使用毫值算术。
	testcases := map[string]struct {
		validRange              resourceapi.CapacityRequestPolicyRange
		fractionalCapacityRange bool
		expectUseMilli          bool
	}{
		// 特性关闭时即使区间为小数也不使用毫值。
		"fractional disabled": {
			validRange: resourceapi.CapacityRequestPolicyRange{
				Min: &pointTwo, Max: &one, Step: &pointOne,
			},
			fractionalCapacityRange: false,
			expectUseMilli:          false,
		},
		// 全为小数且可无损表示时使用毫值。
		"all fractional, fits in milli": {
			validRange: resourceapi.CapacityRequestPolicyRange{
				Min: &pointTwo, Max: &one, Step: &pointOne,
			},
			fractionalCapacityRange: true,
			expectUseMilli:          true,
		},
		// 全整数也可无损表示，特性开启时使用毫值。
		"all integer, fits in milli (no hasFractional gate any more)": {
			validRange: resourceapi.CapacityRequestPolicyRange{
				Min: &one, Max: &three, Step: &one,
			},
			fractionalCapacityRange: true,
			expectUseMilli:          true,
		},
		// 下限超出毫值范围，回退到整数路径。
		"min overflows milli, falls back to integer": {
			validRange: resourceapi.CapacityRequestPolicyRange{
				Min: &tooBigForMilli, Step: &one,
			},
			fractionalCapacityRange: true,
			expectUseMilli:          false,
		},
		// 步长小于 1m（此处为 0）时，即使安全转换成功也判定不使用毫值。
		"step too small (sub-milli)": {
			// 1u = 1 micro = 0.001m; safeMilliValue succeeds (rounds to 0) but step<1m
			validRange: resourceapi.CapacityRequestPolicyRange{
				Min:  &one,
				Step: func() *resource.Quantity { q := resource.MustParse("0"); return &q }(),
			},
			fractionalCapacityRange: true,
			expectUseMilli:          false,
		},
	}
	// 以用例名作为子测试名运行。
	for name, tc := range testcases {
		// 每个用例独立运行。
		t.Run(name, func(t *testing.T) {
			g := NewWithT(t)
			// 执行 useMilli 判定。
			got := useMilli(&tc.validRange, tc.fractionalCapacityRange)
			// 断言判定结果与期望一致。
			g.Expect(got).To(Equal(tc.expectUseMilli))
		})
	}
}

// testViolateCapacityRequestPolicy 表驱动测试 violatesPolicy：覆盖无约束、区间上限、合法值集合与小数步长对齐。
func testViolateCapacityRequestPolicy(t *testing.T) {
	// 每个用例包含请求值、请求策略、特性开关与期望的违反结果。
	testcases := map[string]struct {
		requestedVal            resource.Quantity
		requestPolicy           *resourceapi.CapacityRequestPolicy
		fractionalCapacityRange bool
		expectResult            bool
	}{
		// 无策略时不违反。
		"no constraint": {requestedVal: one, expectResult: false},
		// 请求值小于上限，不违反。
		"less than maximum": {
			requestedVal: one,
			requestPolicy: &resourceapi.CapacityRequestPolicy{
				Default:    &one,
				ValidRange: &resourceapi.CapacityRequestPolicyRange{Min: &one, Max: &two},
			},
			expectResult: false,
		},
		// 请求值超过上限，违反。
		"more than maximum": {
			requestedVal: two,
			requestPolicy: &resourceapi.CapacityRequestPolicy{
				Default:    &one,
				ValidRange: &resourceapi.CapacityRequestPolicyRange{Min: &one, Max: &one},
			},
			expectResult: true,
		},
		// 请求值在合法集合中，不违反。
		"in set": {
			requestedVal: one,
			requestPolicy: &resourceapi.CapacityRequestPolicy{
				Default:     &one,
				ValidValues: []resource.Quantity{one},
			},
			expectResult: false,
		},
		// 请求值不在合法集合中，违反。
		"not in set": {
			requestedVal: two,
			requestPolicy: &resourceapi.CapacityRequestPolicy{
				Default:     &one,
				ValidValues: []resource.Quantity{one},
			},
			expectResult: true,
		},
		// fractional step: min=0.2, step=0.1, max=1
		// 小数步长场景：下限 0.2、步长 0.1、上限 1。
		// 0.3 = 0.2 + 1*0.1，按步长对齐，不违反。
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
		// 0.25 不是从 0.2 起 0.1 的整数倍，违反。
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
		// requested value cannot be losslessly converted to milli, treated as violation.
		// Range values are all milli-representable (no Max) so useMilli returns true; the
		// oversized requested value then hits the safeMilliValue error path in violateValidRange.
		// 请求值无法无损转为毫值时，即使区间字段都可表示，也会在 violateValidRange 中命中错误分支并判为违反。
		// 超大请求值判为违反。
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
	// 以用例名作为子测试名运行。
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			g := NewWithT(t)
			// 执行策略违反判定。
			violate := violatesPolicy(tc.requestedVal, tc.requestPolicy, tc.fractionalCapacityRange)
			// 断言判定结果与期望一致。
			g.Expect(violate).To(BeEquivalentTo(tc.expectResult))
		})
	}
}

// testCalculateConsumedCapacity 表驱动测试 calculateConsumedCapacity：覆盖空请求、默认值、区间/集合取整、小数步长及不可表示错误。
func testCalculateConsumedCapacity(t *testing.T) {
	// 每个用例包含请求值、容量值、请求策略、特性开关、期望结果与是否期望错误。
	testcases := map[string]struct {
		requestedVal            *resource.Quantity
		capacityValue           resource.Quantity
		requestPolicy           *resourceapi.CapacityRequestPolicy
		fractionalCapacityRange bool
		expectResult            resource.Quantity
		expectErr               bool
	}{
		// 无请求值时按策略默认值填充。
		"empty": {requestedVal: nil, capacityValue: one, requestPolicy: &resourceapi.CapacityRequestPolicy{}, expectResult: one},
		// A request above MaxInt64 cannot be read with Value() without wrapping, so
		// calculateConsumedCapacity returns a fatal error rather than acting on a
		// wrapped read (#140441).
		// 超过 MaxInt64 的请求无法用 Value() 无损读取，期望致命错误（#140441）。
		// 请求为 2^64+3，期望不可表示错误。
		"request-above-maxint64-is-rejected": {
			requestedVal:  &twoPow64P3,
			capacityValue: twoPow64P3,
			requestPolicy: &resourceapi.CapacityRequestPolicy{Default: &zero, ValidRange: &resourceapi.CapacityRequestPolicyRange{Min: &zero, Step: &three}},
			expectErr:     true,
		},
		// Rounding MaxInt64 up to the next step passes MaxInt64 and would wrap; return a
		// fatal error instead of a wrapped value.
		// MaxInt64 向上取整到步长 2 的倍数会越过 MaxInt64，期望致命错误。
		// 请求为 MaxInt64、步长为 2，期望不可表示错误。
		"rounded-value-above-maxint64-is-rejected": {
			requestedVal:  &maxInt64Q,
			capacityValue: maxInt64P1,
			requestPolicy: &resourceapi.CapacityRequestPolicy{Default: &zero, ValidRange: &resourceapi.CapacityRequestPolicyRange{Min: &zero, Step: &two}},
			expectErr:     true,
		},
		// Maximum safe boundary: MaxInt64-1 with min=1, step=2 rounds to exactly MaxInt64,
		// which still fits, so it is accepted. Guards on > (not >=) so this is not rejected.
		// 安全边界：MaxInt64-1 按 min=1、step=2 取整恰好为 MaxInt64，仍可接受（守卫用 > 而非 >=）。
		// 期望取整结果为 MaxInt64 且无错误。
		"max-safe-boundary-rounds-to-maxint64": {
			requestedVal:  &maxInt64M1,
			capacityValue: maxInt64Q,
			requestPolicy: &resourceapi.CapacityRequestPolicy{Default: &one, ValidRange: &resourceapi.CapacityRequestPolicyRange{Min: &one, Step: &two}},
			expectResult:  maxInt64Q,
		},
		// 无请求、策略有默认值时取默认值。
		"min in range": {
			requestedVal:  nil,
			capacityValue: two,
			requestPolicy: &resourceapi.CapacityRequestPolicy{Default: &one, ValidRange: &resourceapi.CapacityRequestPolicyRange{Min: &one}},
			expectResult:  one,
		},
		// 默认值在合法集合中时取默认值。
		"default in set": {
			requestedVal:  nil,
			capacityValue: two,
			requestPolicy: &resourceapi.CapacityRequestPolicy{Default: &one, ValidValues: []resource.Quantity{one}},
			expectResult:  one,
		},
		// 请求大于下限且无步长时原样返回。
		"more than min in range": {
			requestedVal:  &two,
			capacityValue: two,
			requestPolicy: &resourceapi.CapacityRequestPolicy{Default: &one, ValidRange: &resourceapi.CapacityRequestPolicyRange{Min: &one}},
			expectResult:  two,
		},
		// 请求小于下限时按下限取整。
		"less than min in range": {
			requestedVal:  &one,
			capacityValue: two,
			requestPolicy: &resourceapi.CapacityRequestPolicy{Default: &one, ValidRange: &resourceapi.CapacityRequestPolicyRange{Min: &two}},
			expectResult:  two,
		},
		// 按步长向上取整。
		"with step (round up)": {
			requestedVal:  &two,
			capacityValue: three,
			requestPolicy: &resourceapi.CapacityRequestPolicy{Default: &one, ValidRange: &resourceapi.CapacityRequestPolicyRange{Min: &one, Step: &two}},
			expectResult:  three,
		},
		// 请求已对齐步长时原样返回。
		"with step (no remaining)": {
			requestedVal:  &two,
			capacityValue: two,
			requestPolicy: &resourceapi.CapacityRequestPolicy{Default: &one, ValidRange: &resourceapi.CapacityRequestPolicyRange{Min: &one, Step: &one}},
			expectResult:  two,
		},
		// fractional step: min=0.2, step=0.1, max=1; request=0.25; rounds up to 0.3
		// 小数步长：下限 0.2、步长 0.1，请求 0.25 应向上取整到 0.3。
		// 期望结果为 300m。
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
		// fractional step: request already aligned; no rounding
		// 请求已对齐步长时无需取整。
		// 期望结果保持 400m。
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
		// 请求值本身在合法集合中时原样返回。
		"valid value in set": {
			requestedVal:  &two,
			capacityValue: three,
			requestPolicy: &resourceapi.CapacityRequestPolicy{Default: &one, ValidValues: []resource.Quantity{one, two, three}},
			expectResult:  two,
		},
		// 向上取整到集合中第一个不小于请求的值。
		"set (round up)": {
			requestedVal:  &two,
			capacityValue: three,
			requestPolicy: &resourceapi.CapacityRequestPolicy{Default: &one, ValidValues: []resource.Quantity{one, three}},
			expectResult:  three,
		},
		// 请求大于集合中所有值时原样返回。
		"larger than set": {
			requestedVal:  &three,
			capacityValue: three,
			requestPolicy: &resourceapi.CapacityRequestPolicy{Default: &one, ValidValues: []resource.Quantity{one, two}},
			expectResult:  three,
		},
		// A request that cannot be converted to a milli value safely is rejected with a
		// fatal error rather than passed through unrounded.
		// 无法无损转为毫值的请求被拒绝，而不是未经取整直接通过。
		// 期望不可表示错误。
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
		// A milli-representable request whose rounded value passes the milli-value range is
		// rejected with a fatal error rather than silently capped.
		// min=100m, step=100m, request=MaxInt64-1 milli:
		//   added = MaxInt64-1 - 100 = MaxInt64-101
		//   n     = (MaxInt64-101) / 100 = 92233720368547757  (added%100 == 6, so n++)
		//   n     = 92233720368547758
		//   guard = (MaxInt64-100)/100 = 92233720368547757  → n > guard → not representable
		// 毫值可表示但取整后越过毫值范围时同样拒绝，而非静默封顶。
		// 请求为 MaxInt64-1 毫值，期望不可表示错误。
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
	// 以用例名作为子测试名运行。
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			g := NewWithT(t)
			// 用用例数据构造设备容量。
			capacity := resourceapi.DeviceCapacity{
				Value:         tc.capacityValue,
				RequestPolicy: tc.requestPolicy,
			}
			// 执行消耗容量计算。
			consumedCapacity, err := calculateConsumedCapacity(tc.requestedVal, capacity, tc.fractionalCapacityRange)
			// 期望错误时断言为不可表示错误。
			if tc.expectErr {
				g.Expect(err).To(MatchError(errCapacityRequestNotRepresentable))
			} else {
				// 否则先断言无错误。
				g.Expect(err).NotTo(HaveOccurred())
				// 再断言计算结果等于期望值。
				g.Expect(consumedCapacity.Cmp(tc.expectResult)).To(BeZero())
			}
		})
	}
}
