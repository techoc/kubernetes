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
	"errors"
	"fmt"
	"math"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"
)

// errCapacityRequestNotRepresentable 表示容量请求（或其向上取整后的值）无法用设备的区间算术表示：
// 整数路径上超过 MaxInt64，或小数路径上超出毫值范围。分配器将其视为中止分配的致命错误，
// 而不是可跳过的单设备不匹配，因此不可表示的请求会被直接拒绝，而不会触发逐设备重试。
var errCapacityRequestNotRepresentable = errors.New("capacity request cannot be represented in the device's range arithmetic")

// errNegativeCapacity 表示解析出的已消费容量为负数。负值不是有效容量，会导致设备被少计而过度分配。
// 它可能来自在更严格校验之前创建、且准入阶段不会重新校验的已存储 ResourceSlice 或 ResourceClaim，
// 因此分配器拒绝它而不是记录它。
var errNegativeCapacity = errors.New("capacity value is negative")

// CmpRequestOverCapacity 检查新的容量请求能否在给定容量之内被追加，并校验请求值是否符合容量的
// requestPolicy。返回 (true, nil) 表示可分配；(false, nil) 表示跳过该设备（软失败）；
// (false, err) 表示致命错误，应中止整个分配。
func CmpRequestOverCapacity(currentConsumedCapacity ConsumedCapacity, deviceRequestCapacity *resourceapi.CapacityRequirements,
	allowMultipleAllocations *bool, capacity map[resourceapi.QualifiedName]resourceapi.DeviceCapacity, allocatingCapacity ConsumedCapacity, fractionalCapacityRange bool) (bool, error) {
	// 请求中包含设备未定义的容量：其他设备可能支持，故按“不可满足”软失败处理，而非致命错误。
	// false + nil error 表示“跳过该设备”，非 nil error 表示“中止分配”。
	if requestsContainNonExistCapacity(deviceRequestCapacity, capacity) {
		return false, nil
	}
	// 第一遍：预先解析每种容量的实际消费量，并先完成可表示性检查。
	consumed := make(map[resourceapi.QualifiedName]resource.Quantity, len(capacity))
	for name, cap := range capacity {
		// 取出该容量对应的请求值；不存在则保持 nil，交给 calculateConsumedCapacity 走默认值逻辑。
		var requestedValPtr *resource.Quantity
		if deviceRequestCapacity != nil && deviceRequestCapacity.Requests != nil {
			if requestedVal, requestedFound := deviceRequestCapacity.Requests[name]; requestedFound {
				requestedValPtr = &requestedVal
			}
		}
		// 根据请求值与 requestPolicy 计算实际消费量，并在此处检测到不可表示的错误。
		consumedCapacity, err := calculateConsumedCapacity(requestedValPtr, cap, fractionalCapacityRange)
		if err != nil {
			return false, fmt.Errorf("capacity %q: %w", name, err)
		}
		if consumedCapacity.Sign() < 0 {
			return false, fmt.Errorf("capacity %q: %w", name, errNegativeCapacity)
		}
		// 缓存解析结果，供第二遍策略与容量检查使用。
		consumed[name] = consumedCapacity
	}
	// 第二遍：策略与容量检查属于软失败，只会跳过该设备而不会中止分配。先在克隆上累加，避免污染原状态。
	clone := currentConsumedCapacity.Clone()
	for name, cap := range capacity {
		consumedCapacity := consumed[name]
		// 请求值违反 requestPolicy（如越界或不在步长上）：跳过该设备。
		if violatesPolicy(consumedCapacity, cap.RequestPolicy, fractionalCapacityRange) {
			return false, nil
		}
		if _, allocatedFound := clone[name]; allocatedFound {
			clone[name].Add(consumedCapacity)
		} else {
			clone[name] = ptr.To(consumedCapacity.DeepCopy())
		}
		if allocatingVal, allocatingFound := allocatingCapacity[name]; allocatingFound {
			clone[name].Add(*allocatingVal)
		}
		// 累计消费量超过设备容量：跳过该设备。
		if clone[name].Cmp(cap.Value) > 0 {
			return false, nil
		}
	}
	// 所有容量都通过检查：该设备可分配。
	return true, nil
}

// requestsContainNonExistCapacity 在请求包含设备未定义的容量名时返回 true。请求为空或 nil 时返回 false；
// 只要有任一请求名在设备容量 map 中找不到，就立即返回 true。
func requestsContainNonExistCapacity(deviceRequestCapacity *resourceapi.CapacityRequirements,
	capacity map[resourceapi.QualifiedName]resourceapi.DeviceCapacity) bool {
	// 无请求或请求 map 为 nil：不存在不匹配的容量名。
	if deviceRequestCapacity == nil || deviceRequestCapacity.Requests == nil {
		return false
	}
	// 遍历请求名，发现设备未定义的容量即返回 true。
	for name := range deviceRequestCapacity.Requests {
		if _, found := capacity[name]; !found {
			return true
		}
	}
	return false
}

// calculateConsumedCapacity 根据请求值与设备容量策略返回有效的消费量：无请求值时由 fillEmptyRequest
// 填充；无 requestPolicy 时原样返回请求值；否则按 ValidRange 或 ValidValues 策略计算消费量。
func calculateConsumedCapacity(requestedVal *resource.Quantity, capacity resourceapi.DeviceCapacity, fractionalCapacityRange bool) (resource.Quantity, error) {
	// 没有请求值：使用 default 或容量值填充。
	if requestedVal == nil {
		return fillEmptyRequest(capacity), nil
	}
	// 没有策略：请求值即消费量，返回其深拷贝。
	if capacity.RequestPolicy == nil {
		return requestedVal.DeepCopy(), nil
	}
	// 有策略：按范围向上取整，或按合法值集合向上取值。
	switch {
	// 区间策略（含 Min）：调用 roundUpRange，可返回不可表示错误。
	case capacity.RequestPolicy.ValidRange != nil && capacity.RequestPolicy.ValidRange.Min != nil:
		return roundUpRange(requestedVal, capacity.RequestPolicy.ValidRange, fractionalCapacityRange)
	// 合法值集合策略：调用 roundUpValidValues。
	case capacity.RequestPolicy.ValidValues != nil:
		return roundUpValidValues(requestedVal, capacity.RequestPolicy.ValidValues), nil
	}
	// 策略未定义可用规则：原样返回请求值。
	return *requestedVal, nil
}

// fillEmptyRequest 在请求值为空时使用策略中的 default（若定义），否则使用容量自身的值。
// 返回深拷贝以避免调用方修改共享的 Quantity。
func fillEmptyRequest(capacity resourceapi.DeviceCapacity) resource.Quantity {
	// 定义了 default：返回其深拷贝。
	if capacity.RequestPolicy != nil && capacity.RequestPolicy.Default != nil {
		return capacity.RequestPolicy.Default.DeepCopy()
	}
	// 未定义 default：退化为容量值。
	return capacity.Value.DeepCopy()
}

// roundUpRange 把请求值向上取整到 validRange 允许的取值：低于 Min 时返回 Min；指定了 Step 时
// 向上取整到从 Min 起 Step 的最近倍数；无 Step 且请求 >= Min 时原样返回。当
// fractionalCapacityRange 开启且 min/max/step 中存在小数、都能无损表示为毫值且 step >= 1m 时，
// 使用毫值算术，否则使用 Value() 整数算术。请求或取整结果无法表示时返回
// errCapacityRequestNotRepresentable，调用方据此中止分配而非使用回绕或截断值。
func roundUpRange(requestedVal *resource.Quantity, validRange *resourceapi.CapacityRequestPolicyRange,
	enableFractionalCapacityRange bool) (resource.Quantity, error) {
	// 请求低于下界：直接返回 Min。
	if requestedVal.Cmp(*validRange.Min) < 0 {
		return validRange.Min.DeepCopy(), nil
	}
	// 未指定步长：请求已 >= Min，原样返回。
	if validRange.Step == nil {
		return *requestedVal, nil
	}
	// 满足小数条件时使用毫值整数算术，避免浮点误差。
	if useMilli(validRange, enableFractionalCapacityRange) {
		// 先确认请求本身可无损转换成毫值，否则致命错误。
		requestedMilli, err := safeMilliValue(*requestedVal)
		format := validRange.Step.Format
		if err != nil {
			return resource.Quantity{}, fmt.Errorf("%w: request %s is not a representable milli value", errCapacityRequestNotRepresentable, requestedVal.String())
		}
		// 计算相对 Min 的偏移量，向上取整得到需要跨过的步长数 n。
		stepMilli := validRange.Step.MilliValue()
		minMilli := validRange.Min.MilliValue()
		added := requestedMilli - minMilli
		n := added / stepMilli
		if added%stepMilli != 0 {
			n++
		}
		// 取整后的值若超出 int64 毫值范围则报致命错误，避免溢出。
		if n > (math.MaxInt64-minMilli)/stepMilli {
			return resource.Quantity{}, fmt.Errorf("%w: rounding request %s up to the next step passes the milli-value range", errCapacityRequestNotRepresentable, requestedVal.String())
		}
		// 得到最终的毫值结果：min + step * n。
		valMilli := minMilli + stepMilli*n
		// 结果为整数时用 NewQuantity 保持紧凑表示，否则用毫值表示。
		if valMilli%1000 == 0 {
			return *resource.NewQuantity(valMilli/1000, format), nil
		}
		return *resource.NewMilliQuantity(valMilli, format), nil
	}
	// 整数算术路径：使用 Value() 读取整数值；超过 MaxInt64 的请求无法安全读取。
	if requestedVal.CmpInt64(math.MaxInt64) > 0 {
		return resource.Quantity{}, fmt.Errorf("%w: request %s exceeds MaxInt64", errCapacityRequestNotRepresentable, requestedVal.String())
	}
	// 读取整数值，计算相对 Min 的偏移量并向上取整得到步长数 n。
	requestedInt := requestedVal.Value()
	stepInt := validRange.Step.Value()
	minInt := validRange.Min.Value()
	added := requestedInt - minInt
	n := added / stepInt
	if added%stepInt != 0 {
		n++
	}
	// 取整结果溢出 int64 时报致命错误；minInt >= 0 的前提保证 MaxInt64-minInt 本身不溢出。
	if minInt >= 0 && n > (math.MaxInt64-minInt)/stepInt {
		return resource.Quantity{}, fmt.Errorf("%w: rounding request %s up to the next step passes MaxInt64", errCapacityRequestNotRepresentable, requestedVal.String())
	}
	// 返回按步长格式构造的取整整数值。
	return *resource.NewQuantity(minInt+stepInt*n, validRange.Step.Format), nil
}

// roundUpValidValues 返回合法值集合中第一个大于等于请求值的值；若不存在这样的值，则原样返回请求值。
func roundUpValidValues(requestedVal *resource.Quantity, validValues []resource.Quantity) resource.Quantity {
	// 由于合法值集合由 API 校验保证升序且条目很少（≤10），使用简单顺序查找即可。
	for _, validValue := range validValues {
		// 第一个 >= 请求值的合法值即为向上取整结果。
		if requestedVal.Cmp(validValue) <= 0 {
			return validValue.DeepCopy()
		}
	}
	// 请求值大于所有合法值：原样返回。
	return *requestedVal
}

// GetConsumedCapacityFromRequest 根据 claim 请求与设备定义的容量，返回每种容量的有效消费量。
// 不可表示的请求会作为致命错误返回，不会记录未经处理的原始请求值。
func GetConsumedCapacityFromRequest(requestedCapacity *resourceapi.CapacityRequirements,
	consumableCapacity map[resourceapi.QualifiedName]resourceapi.DeviceCapacity, fractionalCapacityRange bool) (map[resourceapi.QualifiedName]resource.Quantity, error) {
	consumedCapacity := make(map[resourceapi.QualifiedName]resource.Quantity)
	for name, cap := range consumableCapacity {
		// 取出该容量的请求值，不存在则交给 calculateConsumedCapacity 使用默认/容量值。
		var requestedValPtr *resource.Quantity
		if requestedCapacity != nil && requestedCapacity.Requests != nil {
			if requestedVal, requestedFound := requestedCapacity.Requests[name]; requestedFound {
				requestedValPtr = &requestedVal
			}
		}
		capacity, err := calculateConsumedCapacity(requestedValPtr, cap, fractionalCapacityRange)
		if err != nil {
			return nil, fmt.Errorf("capacity %q: %w", name, err)
		}
		consumedCapacity[name] = capacity
	}
	return consumedCapacity, nil
}

// violatesPolicy 检查请求值是否违反 requestPolicy：策略为 nil、或请求值恰为 default 时均视为不违反；
// 否则按 ValidRange 或 ValidValues 规则判断。返回 true 表示违反策略。
func violatesPolicy(requestedVal resource.Quantity, policy *resourceapi.CapacityRequestPolicy, fractionalCapacityRange bool) bool {
	if policy == nil {
		// 无策略可检查，自然不违反。
		return false
	}
	// 请求值就是 default：允许通过，即使它可能不满足其他约束。
	if policy.Default != nil && requestedVal == *policy.Default {
		return false
	}
	// 按区间或合法值集合校验；两者分别委托给 violateValidRange / violateValidValues。
	switch {
	case policy.ValidRange != nil:
		return violateValidRange(requestedVal, *policy.ValidRange, fractionalCapacityRange)
	case len(policy.ValidValues) > 0:
		return violateValidValues(requestedVal, policy.ValidValues)
	}
	// 策略中没有任何可用约束，视为不违反。
	return false
}

// violateValidRange 检查请求值是否违反区间策略：超过 Max 视为违反；指定了 Step 时，
// 请求值必须落在从 Min 出发的整数倍步长上，否则视为违反。小数场景下使用毫值算术，
// 请求值无法无损转成毫值时直接判为违反。
func violateValidRange(requestedVal resource.Quantity, validRange resourceapi.CapacityRequestPolicyRange, fractionalCapacityRange bool) bool {
	// 超过上界：违反策略。
	if validRange.Max != nil &&
		requestedVal.Cmp(*validRange.Max) > 0 {
		return true
	}
	if validRange.Step != nil {
		var requested, step, min int64
		var err error
		if useMilli(&validRange, fractionalCapacityRange) {
			// 毫值路径：请求值必须可无损转换，否则判为违反。
			if requested, err = safeMilliValue(requestedVal); err != nil {
				return true
			}
			step = validRange.Step.MilliValue()
			min = validRange.Min.MilliValue()
		} else {
			// 整数路径：使用 Value() 读取整数值。
			requested = requestedVal.Value()
			step = validRange.Step.Value()
			min = validRange.Min.Value()
		}
		// 必须是相对 Min 的 Step 整数倍，否则违反策略。
		if (requested-min)%step != 0 {
			return true
		}
	}
	return false
}

// useMilli 报告给定区间是否应使用毫值算术。需要 fractionalCapacityRange 开启、各字段均可无损
// 表示成毫值，且 Step 至少为 1m（MilliValue 后非零）；特性关闭时直接返回 false。
func useMilli(validRange *resourceapi.CapacityRequestPolicyRange, enableFractionalCapacityRange bool) bool {
	// 小数容量区间特性未开启：始终走整数路径。
	if !enableFractionalCapacityRange {
		return false
	}
	// 依次检查 Min、Max、Step：nil 跳过；不能无损转成毫值则退回整数路径。
	for i, q := range []*resource.Quantity{validRange.Min, validRange.Max, validRange.Step} {
		if q == nil {
			continue
		}
		if _, err := safeMilliValue(*q); err != nil {
			return false
		}
		// 第三个字段是 Step：必须至少 1m，否则取整无意义，退回整数路径。
		if i == 2 {
			if step, err := safeMilliValue(*validRange.Step); err == nil && step < 1 {
				return false
			}
		}
	}
	// 所有字段都通过检查：可以使用毫值算术。
	return true
}

// violateValidValues 检查请求值是否不在合法值集合中。只要有一个合法值与请求值相等即视为不违反；
// 全部不匹配则返回 true（违反）。
func violateValidValues(requestedVal resource.Quantity, validValues []resource.Quantity) bool {
	for _, validVal := range validValues {
		// 命中任一合法值：不违反。
		if requestedVal.Cmp(validVal) == 0 {
			return false
		}
	}
	// 没有任何合法值匹配：违反策略。
	return true
}

// safeMilliValue 在转换无损时返回 q 的毫值整数表示。它把 MilliValue() 的结果重新构造成
// 毫值 Quantity 并与原值比较；不一致说明原值无法用毫单位精确表示，返回错误。
func safeMilliValue(q resource.Quantity) (int64, error) {
	milli := q.MilliValue()
	milliQuantity := resource.NewMilliQuantity(milli, q.Format)
	// 往返比较，检测是否因超出毫值范围而丢失精度。
	if q.Cmp(*milliQuantity) != 0 {
		return 0, errors.New("value must be representable in milli units")
	}
	return milli, nil
}
