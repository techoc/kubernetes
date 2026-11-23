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

// Package incubating 提供动态资源分配中处于孵化阶段的容量计算与资源池处理逻辑，
// 包括可消费容量的推导、请求策略校验以及资源池的收集与校验。
package incubating

import (
	"errors"
	"fmt"
	"math"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"
)

// errCapacityRequestNotRepresentable signals that a capacity request, or the
// value it rounds up to, cannot be represented by a device's range arithmetic
// (it passes MaxInt64 on the integer path, or the milli-value range on the
// fractional path). The allocator treats this as a fatal error that aborts the
// allocation rather than a per-device mismatch to skip, so an unrepresentable
// request is rejected outright instead of driving a device-by-device retry.
// errCapacityRequestNotRepresentable 表示容量请求或其向上取整后的值无法用设备的区间算术表示
// （整数路径超过 MaxInt64，小数路径超过毫值范围）。分配器将其视为中止分配的致命错误，
// 而不是可跳过的单设备不匹配，因此不可表示的请求会被直接拒绝，而非触发逐设备重试。
var errCapacityRequestNotRepresentable = errors.New("capacity request cannot be represented in the device's range arithmetic")

// errNegativeCapacity signals that a resolved consumed capacity is negative. A negative
// value is not valid capacity and would under-count the device and over-allocate it. It
// can reach the allocator from a stored ResourceSlice or ResourceClaim created before
// stricter validation, which admission does not re-check, so the allocator rejects it
// rather than recording it.
// errNegativeCapacity 表示解析出的已消费容量为负值。负值不是合法容量，会导致设备被少计而过度分配。
// 它可能来自更严格校验之前存储的 ResourceSlice 或 ResourceClaim，而准入不会重新校验这些对象，
// 因此分配器拒绝记录该值。
var errNegativeCapacity = errors.New("capacity value is negative")

// CmpRequestOverCapacity checks whether the new capacity request can be added within the given capacity,
// and checks whether the requested value is against the capacity requestPolicy.
// CmpRequestOverCapacity 判断当前已消费容量之上能否追加新的容量请求，并检查请求值是否符合 requestPolicy。
// 请求不可表示的致命错误优先于策略/容量不满足这类软失败，且该优先级不受 map 遍历顺序影响。
func CmpRequestOverCapacity(currentConsumedCapacity ConsumedCapacity, deviceRequestCapacity *resourceapi.CapacityRequirements,
	allowMultipleAllocations *bool, capacity map[resourceapi.QualifiedName]resourceapi.DeviceCapacity, allocatingCapacity ConsumedCapacity, fractionalCapacityRange bool) (bool, error) {
	if requestsContainNonExistCapacity(deviceRequestCapacity, capacity) {
		// This device does not define a requested capacity. A different device may,
		// so report it as not satisfiable rather than a fatal error: false with a nil
		// error means "skip this device", a non-nil error means "abort allocation".
		// 返回 false + nil 表示“跳过此设备”而非致命错误：其他设备可能定义了该容量。
		return false, nil
	}
	// First resolve every requested capacity. A request that cannot be represented is a
	// fatal error that must abort regardless of map iteration order, so representability
	// is checked for all capacities before any soft "not satisfiable" return below.
	// Interleaving the two in one loop would let the outcome, skip or abort, depend on
	// which capacity the map happens to visit first.
	// 先解析全部请求容量；不可表示的请求必须在任何软失败返回之前被发现，
	// 否则“跳过”还是“中止”将取决于 map 的遍历顺序。
	consumed := make(map[resourceapi.QualifiedName]resource.Quantity, len(capacity))
	for name, cap := range capacity {
		var requestedValPtr *resource.Quantity
		// 仅当请求中显式给出该容量名时才取请求值。
		if deviceRequestCapacity != nil && deviceRequestCapacity.Requests != nil {
			if requestedVal, requestedFound := deviceRequestCapacity.Requests[name]; requestedFound {
				requestedValPtr = &requestedVal
			}
		}
		// 按请求值与请求策略计算该容量的实际消耗值。
		consumedCapacity, err := calculateConsumedCapacity(requestedValPtr, cap, fractionalCapacityRange)
		if err != nil {
			// The request is not representable in this capacity's range arithmetic.
			// Surface a fatal error so the allocator aborts instead of retrying other
			// devices, which a user-controlled out-of-range request could abuse.
			// 请求在该容量的区间算术中不可表示，返回致命错误让分配器中止，
			// 避免用户可控的超范围请求借机反复重试其他设备。
			return false, fmt.Errorf("capacity %q: %w", name, err)
		}
		if consumedCapacity.Sign() < 0 {
			// A negative capacity cannot come from a request the allocator should honor;
			// recording it would over-allocate the device. Abort rather than skip, since a
			// stored object from before stricter validation is a data problem, not a
			// per-device mismatch that another device could satisfy.
			// 负容量记录后会过度分配设备；这是数据问题而非单设备不匹配，因此中止而非跳过。
			return false, fmt.Errorf("capacity %q: %w", name, errNegativeCapacity)
		}
		// 保存该容量解析后的消耗值，供后续策略与容量检查使用。
		consumed[name] = consumedCapacity
	}
	// The policy and capacity checks are soft: they skip this device rather than abort.
	// 以下策略与容量检查属于软失败：不满足时跳过本设备，不中止整体分配。
	clone := currentConsumedCapacity.Clone()
	for name, cap := range capacity {
		// 取出该容量预先解析好的消耗值。
		consumedCapacity := consumed[name]
		// 请求值违反请求策略时跳过本设备。
		if violatesPolicy(consumedCapacity, cap.RequestPolicy, fractionalCapacityRange) {
			return false, nil
		}
		// If the current clone already contains an entry for this capacity, add the consumedCapacity to it.
		// Otherwise, initialize it with calculated consumedCapacity.
		// 已有条目则累加，否则用解析值深拷贝初始化该条目。
		if _, allocatedFound := clone[name]; allocatedFound {
			clone[name].Add(consumedCapacity)
		} else {
			clone[name] = ptr.To(consumedCapacity.DeepCopy())
		}
		// If allocatingCapacity contains an entry for this capacity, add its value to clone as well.
		// 若本轮还在为同一次分配预留容量（allocatingCapacity），也一并累加进去。
		if allocatingVal, allocatingFound := allocatingCapacity[name]; allocatingFound {
			clone[name].Add(*allocatingVal)
		}
		// 累加后的总量超过设备声明的容量，则本设备无法满足。
		if clone[name].Cmp(cap.Value) > 0 {
			return false, nil
		}
	}
	return true, nil
}

// requestsNonExistCapacity returns true if requests contain non-exist capacity.
// requestsContainNonExistCapacity 检查请求中是否包含设备未定义的容量名，只要有一个不存在即返回 true。
func requestsContainNonExistCapacity(deviceRequestCapacity *resourceapi.CapacityRequirements,
	capacity map[resourceapi.QualifiedName]resourceapi.DeviceCapacity) bool {
	if deviceRequestCapacity == nil || deviceRequestCapacity.Requests == nil {
		// 请求或请求表为空时，自然不存在“请求了不存在的容量”。
		return false
	}
	// 逐个检查请求中的容量名是否在设备容量表中定义。
	for name := range deviceRequestCapacity.Requests {
		if _, found := capacity[name]; !found {
			// 发现未定义的容量名，立即返回 true。
			return true
		}
	}
	// 所有请求容量都有定义。
	return false
}

// calculateConsumedCapacity returns valid capacity to be consumed regarding the requested capacity and device capacity policy.
//
// If no requestPolicy, return capacity.Value.
// If no requestVal, fill the quantity by fillEmptyRequest function
// Otherwise, use requestPolicy to calculate the consumed capacity from request if applicable.
// calculateConsumedCapacity 根据请求值与设备请求策略计算本次应消耗的容量。
// 无请求值时按默认值/容量值填充；无策略时采用请求值；有区间则按步长向上取整，有合法值集合则取整到第一个不小于请求的元素。
func calculateConsumedCapacity(requestedVal *resource.Quantity, capacity resourceapi.DeviceCapacity, fractionalCapacityRange bool) (resource.Quantity, error) {
	// 无请求值：由 fillEmptyRequest 决定默认值或容量总值。
	if requestedVal == nil {
		return fillEmptyRequest(capacity), nil
	}
	// 无请求策略时不调整请求值，直接返回其深拷贝。
	if capacity.RequestPolicy == nil {
		return requestedVal.DeepCopy(), nil
	}
	switch {
	// 带最小值的合法区间：按区间步长向上取整。
	case capacity.RequestPolicy.ValidRange != nil && capacity.RequestPolicy.ValidRange.Min != nil:
		return roundUpRange(requestedVal, capacity.RequestPolicy.ValidRange, fractionalCapacityRange)
	// 合法值集合：向上取整到集合中第一个不小于请求的值。
	case capacity.RequestPolicy.ValidValues != nil:
		return roundUpValidValues(requestedVal, capacity.RequestPolicy.ValidValues), nil
	}
	// 其余策略组合无需调整，原样返回请求值。
	return *requestedVal, nil
}

// fillEmptyRequest
// return requestPolicy.default if defined.
// Otherwise, return capacity value.
// fillEmptyRequest 在请求为空时确定消耗值：优先使用策略默认值，否则回退到设备容量值。
func fillEmptyRequest(capacity resourceapi.DeviceCapacity) resource.Quantity {
	// 定义了默认值时返回默认值的深拷贝。
	if capacity.RequestPolicy != nil && capacity.RequestPolicy.Default != nil {
		return capacity.RequestPolicy.Default.DeepCopy()
	}
	// 否则使用设备声明的容量总值。
	return capacity.Value.DeepCopy()
}

// roundUpRange rounds the requestedVal up to fit within the specified validRange.
//   - If requestedVal is less than Min, it returns Min.
//   - If Step is specified, it rounds requestedVal up to the nearest multiple of Step
//     starting from Min.
//   - If no Step is specified and requestedVal >= Min, it returns requestedVal as is.
//
// When fractionalCapacityRange is true and any of min/max/step are fractional and all
// fit within the milli-value int64 range and step >= 1m, milli-value arithmetic is used.
// Otherwise Value() arithmetic is used.
//
// It returns errCapacityRequestNotRepresentable when the request, or the value it
// rounds up to, cannot be represented: past MaxInt64 on the integer path, or past
// the milli-value range on the fractional path. The caller aborts allocation with
// that error rather than acting on a wrapped read or a silently capped value.
// roundUpRange 将请求值向上取整到合法区间：低于下限取下限，指定步长则取从下限起第一个不小于请求的步长倍数。
// 小数场景下按毫值算术计算，并在结果或中间值超出可表示范围时返回致命错误。
func roundUpRange(requestedVal *resource.Quantity, validRange *resourceapi.CapacityRequestPolicyRange, fractionalCapacityRange bool) (resource.Quantity, error) {
	// 请求低于下限时直接按下限计费。
	if requestedVal.Cmp(*validRange.Min) < 0 {
		return validRange.Min.DeepCopy(), nil
	}
	// 未指定步长时无需取整。
	if validRange.Step == nil {
		return *requestedVal, nil
	}
	// 小数路径：使用毫值算术以保留小数精度。
	if useMilli(validRange, fractionalCapacityRange) {
		requestedMilli, err := safeMilliValue(*requestedVal)
		// 保持结果与步长相同的 Quantity 格式。
		format := validRange.Step.Format
		if err != nil {
			// 请求无法无损转换为毫值，返回不可表示错误。
			return resource.Quantity{}, fmt.Errorf("%w: request %s is not a representable milli value", errCapacityRequestNotRepresentable, requestedVal.String())
		}
		// 步长与下限的毫值表示。
		stepMilli := validRange.Step.MilliValue()
		minMilli := validRange.Min.MilliValue()
		// 计算请求相对下限需要跨越的毫值差。
		added := requestedMilli - minMilli
		// 计算需要跨越的整数个步长。
		n := added / stepMilli
		// 有余数说明未对齐，需再进一个步长。
		if added%stepMilli != 0 {
			n++
		}
		// 取整后的毫值会超过 int64 范围，报告不可表示而非静默截断。
		if n > (math.MaxInt64-minMilli)/stepMilli {
			return resource.Quantity{}, fmt.Errorf("%w: rounding request %s up to the next step passes the milli-value range", errCapacityRequestNotRepresentable, requestedVal.String())
		}
		// 得到向上取整后的毫值结果。
		valMilli := minMilli + stepMilli*n
		// Return in the same format as the step quantity. If the result is a
		// whole number, use NewQuantity to keep the representation compact and
		// compatible with quantities parsed from whole-number strings.
		// 结果为整数量时用 NewQuantity 保持表示紧凑并与整数字面量解析结果兼容。
		if valMilli%1000 == 0 {
			return *resource.NewQuantity(valMilli/1000, format), nil
		}
		return *resource.NewMilliQuantity(valMilli, format), nil
	}
	// Integer arithmetic path.
	// A request above MaxInt64 cannot be read with Value() without wrapping, so it
	// cannot be rounded in int64. Report that it is not representable rather than
	// acting on the wrapped read.
	// 整数路径：先确认请求可用 Value() 无损读取，超过 MaxInt64 会回绕。
	if requestedVal.CmpInt64(math.MaxInt64) > 0 {
		// 请求本身超过 MaxInt64，报告不可表示。
		return resource.Quantity{}, fmt.Errorf("%w: request %s exceeds MaxInt64", errCapacityRequestNotRepresentable, requestedVal.String())
	}
	// 读取请求、步长与下限的整数值（Value() 向下取整）。
	requestedInt := requestedVal.Value()
	stepInt := validRange.Step.Value()
	minInt := validRange.Min.Value()
	// 计算相对下限的差值与步长倍数。
	added := requestedInt - minInt
	n := added / stepInt
	// 未整除则再进一个步长，完成向上取整。
	if added%stepInt != 0 {
		n++
	}
	// minInt+stepInt*n overflows int64 once the rounded value passes MaxInt64. It cannot
	// be represented or compared reliably, so report the request as not representable. The
	// guard needs minInt >= 0 so that MaxInt64-minInt does not itself overflow; a negative
	// min is out of the non-negative API contract and keeps the existing arithmetic here.
	// 取整结果超过 MaxInt64 时无法可靠表示或比较，报告不可表示；下限非负保证 MaxInt64-minInt 不溢出。
	if minInt >= 0 && n > (math.MaxInt64-minInt)/stepInt {
		return resource.Quantity{}, fmt.Errorf("%w: rounding request %s up to the next step passes MaxInt64", errCapacityRequestNotRepresentable, requestedVal.String())
	}
	// 按步长的 Quantity 格式返回取整后的整数值。
	return *resource.NewQuantity(minInt+stepInt*n, validRange.Step.Format), nil
}

// roundUpValidValues returns the first value in validValues that is greater than or equal to requestedVal.
// If no such value exists, it returns requestedVal itself.
// roundUpValidValues 返回合法值集合中第一个不小于请求值的元素；不存在这样的元素时返回请求值本身。
func roundUpValidValues(requestedVal *resource.Quantity, validValues []resource.Quantity) resource.Quantity {
	// Simple sequential search is used as the maximum entry of validValues is finite and small (≤10),
	// and the list must already be sorted in ascending order, ensured by API validation.
	// Note: A binary search could alternatively be used for better efficiency if the list grows larger.
	// 合法值集合有限（≤10）且由 API 校验保证升序，顺序查找已足够。
	for _, validValue := range validValues {
		// 找到第一个不小于请求值的合法值，返回其深拷贝。
		if requestedVal.Cmp(validValue) <= 0 {
			return validValue.DeepCopy()
		}
	}
	// 所有合法值都小于请求值时，原样返回请求值。
	return *requestedVal
}

// GetConsumedCapacityFromRequest returns valid consumed capacity,
// according to claim request and defined capacity.
// GetConsumedCapacityFromRequest 根据 claim 请求与设备声明计算每个容量应消耗的值。
func GetConsumedCapacityFromRequest(requestedCapacity *resourceapi.CapacityRequirements,
	consumableCapacity map[resourceapi.QualifiedName]resourceapi.DeviceCapacity, fractionalCapacityRange bool) (map[resourceapi.QualifiedName]resource.Quantity, error) {
	consumedCapacity := make(map[resourceapi.QualifiedName]resource.Quantity)
	// 遍历设备的每个可消费容量。
	for name, cap := range consumableCapacity {
		var requestedValPtr *resource.Quantity
		// 仅当请求显式给出该容量名时才使用请求值。
		if requestedCapacity != nil && requestedCapacity.Requests != nil {
			if requestedVal, requestedFound := requestedCapacity.Requests[name]; requestedFound {
				requestedValPtr = &requestedVal
			}
		}
		// 计算该容量的实际消耗值。
		capacity, err := calculateConsumedCapacity(requestedValPtr, cap, fractionalCapacityRange)
		if err != nil {
			// Feasibility already ran, so this should not happen. Return the error
			// rather than a raw request so a future caller cannot record an
			// unrepresentable value as consumed capacity.
			// 可行性检查阶段本应拦住此类错误；这里返回错误，防止后续调用方记录不可表示的消耗值。
			return nil, fmt.Errorf("capacity %q: %w", name, err)
		}
		// 记录该容量的消耗值。
		consumedCapacity[name] = capacity
	}
	return consumedCapacity, nil
}

// violatesPolicy checks whether the request violate the requestPolicy.
// violatesPolicy 检查请求值是否违反容量请求策略；无策略、等于默认值或满足区间/集合约束时均视为不违反。
func violatesPolicy(requestedVal resource.Quantity, policy *resourceapi.CapacityRequestPolicy, fractionalCapacityRange bool) bool {
	if policy == nil {
		// no policy to check
		// 没有策略时自然无从违反。
		return false
	}
	// 等于默认值视为合法，即使默认值落在区间之外。
	if policy.Default != nil && requestedVal == *policy.Default {
		return false
	}
	switch {
	// 区间策略：检查上限以及是否按步长对齐。
	case policy.ValidRange != nil:
		return violateValidRange(requestedVal, *policy.ValidRange, fractionalCapacityRange)
	// 合法值集合策略：检查请求值是否属于集合。
	case len(policy.ValidValues) > 0:
		return violateValidValues(requestedVal, policy.ValidValues)
	}
	// no policy violated through to completion.
	// 所有检查通过，未违反任何策略。
	return false
}

// violateValidRange 检查请求值是否超过区间上限，或未能从下限按步长对齐。
func violateValidRange(requestedVal resource.Quantity, validRange resourceapi.CapacityRequestPolicyRange, fractionalCapacityRange bool) bool {
	// 超过上限即违反。
	if validRange.Max != nil &&
		requestedVal.Cmp(*validRange.Max) > 0 {
		return true
	}
	// 指定步长时，请求值必须是从下限开始的整数个步长。
	if validRange.Step != nil {
		// 根据是否启用毫值算术选择对应的整数表示。
		var requested, step, min int64
		var err error
		// 毫值路径：安全转换请求、步长与下限。
		if useMilli(&validRange, fractionalCapacityRange) {
			// 请求无法无损转为毫值，视为违反。
			if requested, err = safeMilliValue(requestedVal); err != nil {
				return true
			}
			// 步长与下限的毫值。
			step = validRange.Step.MilliValue()
			min = validRange.Min.MilliValue()
		} else {
			// 整数路径：使用 Value() 的向下取整表示。
			requested = requestedVal.Value()
			step = validRange.Step.Value()
			min = validRange.Min.Value()
		}
		// must be a multiple of step from min
		// 相对下限的差值必须能被步长整除，否则未按步长对齐。
		if (requested-min)%step != 0 {
			return true
		}
	}
	return false
}

// useMilli reports whether milli-value arithmetic should be used for the given range.
// Conditions: fractionalCapacityRange enabled AND any of min/max/step is fractional
// AND all non-nil fields fit within the milli-value int64 range AND step >= 1m (i.e.
// non-zero after MilliValue()).
// useMilli 判断是否使用毫值算术：特性开关开启、区间字段可无损转为毫值且步长不小于 1m。
func useMilli(validRange *resourceapi.CapacityRequestPolicyRange, fractionalCapacityRange bool) bool {
	// 特性未开启时始终走整数路径。
	if !fractionalCapacityRange {
		return false
	}
	for i, q := range []*resource.Quantity{validRange.Min, validRange.Max, validRange.Step} {
		// 空字段不参与判断。
		if q == nil {
			continue
		}
		// 任一字段无法无损转为毫值，回退到整数路径。
		if _, err := safeMilliValue(*q); err != nil {
			return false
		}
		// 第三个字段是步长。
		if i == 2 {
			// Step must be at least 1m.
			// 步长必须至少为 1m，否则毫值取整没有意义。
			if step, err := safeMilliValue(*validRange.Step); err == nil && step < 1 {
				return false
			}
		}
	}
	return true
}

// violateValidValues 检查请求值是否不在合法值集合中；不在集合中返回 true。
func violateValidValues(requestedVal resource.Quantity, validValues []resource.Quantity) bool {
	// 找到相等值即说明请求合法。
	for _, validVal := range validValues {
		if requestedVal.Cmp(validVal) == 0 {
			return false
		}
	}
	// 遍历完仍无匹配，说明请求值不在集合中。
	return true
}

// safeMilliValue returns q as a milli value if the conversion is lossless.
// safeMilliValue 在转换无损的前提下返回 q 的毫值，否则返回错误。
func safeMilliValue(q resource.Quantity) (int64, error) {
	// 先取毫值整数，再构造对应的毫值 Quantity 用于比较。
	milli := q.MilliValue()
	milliQuantity := resource.NewMilliQuantity(milli, q.Format)
	if q.Cmp(*milliQuantity) != 0 {
		// 两者不等说明转换丢失了精度。
		return 0, errors.New("value must be representable in milli units")
	}
	return milli, nil
}
