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

package v1beta1

import (
	unsafe "unsafe"

	corev1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	resourcev1beta1 "k8s.io/api/resource/v1beta1"
	conversion "k8s.io/apimachinery/pkg/conversion"
)

// Convert_v1beta1_DeviceRequest_To_v1_DeviceRequest 把 v1beta1 的 DeviceRequest 转换为 v1 版本。
// 先执行自动生成的字段转换，再把 v1beta1 的“主请求”字段打包进 v1 新增的 Exactly 字段。
func Convert_v1beta1_DeviceRequest_To_v1_DeviceRequest(in *resourcev1beta1.DeviceRequest, out *resourceapi.DeviceRequest, s conversion.Scope) error {
	if err := autoConvert_v1beta1_DeviceRequest_To_v1_DeviceRequest(in, out, s); err != nil {
		return err
	}
	// If any fields on the main request is set, we create a ExactDeviceRequest
	// and set the Exactly field. It might be invalid but that will be caught in validation.
	// 只要任一主请求字段有设置就构造 Exactly；字段组合是否合法由后续校验负责。
	if hasAnyMainRequestFieldsSet(in) {
		var exactDeviceRequest resourceapi.ExactDeviceRequest
		// DeviceClassName 是纯字符串字段，可直接赋值。
		exactDeviceRequest.DeviceClassName = in.DeviceClassName
		if in.Selectors != nil {
			// 选择器是另一个版本的类型，必须逐项调用生成的转换函数。
			selectors := make([]resourceapi.DeviceSelector, 0, len(in.Selectors))
			for i := range in.Selectors {
				var selector resourceapi.DeviceSelector
				err := Convert_v1beta1_DeviceSelector_To_v1_DeviceSelector(&in.Selectors[i], &selector, s)
				if err != nil {
					return err
				}
				selectors = append(selectors, selector)
			}
			exactDeviceRequest.Selectors = selectors
		}
		// 分配模式两个版本底层类型一致，直接做显式类型转换。
		exactDeviceRequest.AllocationMode = resourceapi.DeviceAllocationMode(in.AllocationMode)
		// 计数与管理员访问标志直接透传。
		exactDeviceRequest.Count = in.Count
		exactDeviceRequest.AdminAccess = in.AdminAccess
		var tolerations []resourceapi.DeviceToleration
		// 容忍规则是另一版本类型，逐项转换并在失败时立即返回错误。
		for _, e := range in.Tolerations {
			var toleration resourceapi.DeviceToleration
			if err := Convert_v1beta1_DeviceToleration_To_v1_DeviceToleration(&e, &toleration, s); err != nil {
				return err
			}
			tolerations = append(tolerations, toleration)
		}
		exactDeviceRequest.Tolerations = tolerations
		// CapacityRequirements 两版本内存布局相同，用 unsafe 指针转换避免逐字段拷贝。
		exactDeviceRequest.Capacity = (*resourceapi.CapacityRequirements)(unsafe.Pointer(in.Capacity))
		if in.DerivedAttributes != nil {
			// 派生属性逐项转换，保持与顶层属性相同的转换规则。
			derivedAttributes := make([]resourceapi.DeviceDerivedAttribute, 0, len(in.DerivedAttributes))
			for i := range in.DerivedAttributes {
				var derivedAttribute resourceapi.DeviceDerivedAttribute
				err := Convert_v1beta1_DeviceDerivedAttribute_To_v1_DeviceDerivedAttribute(&in.DerivedAttributes[i], &derivedAttribute, s)
				if err != nil {
					return err
				}
				derivedAttributes = append(derivedAttributes, derivedAttribute)
			}
			exactDeviceRequest.DerivedAttributes = derivedAttributes
		}
		// 打包完成后挂到 v1 请求的 Exactly 字段上。
		out.Exactly = &exactDeviceRequest
	}
	return nil
}

// hasAnyMainRequestFieldsSet 判断 v1beta1 请求中是否至少设置了一个主请求字段。
// 这些字段在 v1 中被收拢进 Exactly，全部未设置时 Exactly 保持为 nil。
func hasAnyMainRequestFieldsSet(deviceRequest *resourcev1beta1.DeviceRequest) bool {
	// 标量判断零值、指针与切片判断 nil，任一命中即说明存在主请求配置。
	return deviceRequest.DeviceClassName != "" ||
		deviceRequest.Selectors != nil ||
		deviceRequest.AllocationMode != "" ||
		deviceRequest.Count != 0 ||
		deviceRequest.AdminAccess != nil ||
		deviceRequest.Tolerations != nil ||
		deviceRequest.Capacity != nil ||
		deviceRequest.DerivedAttributes != nil
}

// Convert_v1_DeviceRequest_To_v1beta1_DeviceRequest 把 v1 的 DeviceRequest 转换为 v1beta1 版本。
// 与正向转换相反：把 v1 的 Exactly 子结构拆回 v1beta1 的顶层主请求字段。
func Convert_v1_DeviceRequest_To_v1beta1_DeviceRequest(in *resourceapi.DeviceRequest, out *resourcev1beta1.DeviceRequest, s conversion.Scope) error {
	if err := autoConvert_v1_DeviceRequest_To_v1beta1_DeviceRequest(in, out, s); err != nil {
		return err
	}
	// Exactly 为空说明是 FirstAvailable 请求，主请求字段保持零值即可。
	if in.Exactly != nil {
		// 类名、分配模式、计数与管理员标志直接从 Exactly 拆回顶层。
		out.DeviceClassName = in.Exactly.DeviceClassName
		if in.Exactly.Selectors != nil {
			// 选择器需要逐项反向转换到 v1beta1 类型。
			selectors := make([]resourcev1beta1.DeviceSelector, 0, len(in.Exactly.Selectors))
			for i := range in.Exactly.Selectors {
				var selector resourcev1beta1.DeviceSelector
				err := Convert_v1_DeviceSelector_To_v1beta1_DeviceSelector(&in.Exactly.Selectors[i], &selector, s)
				if err != nil {
					return err
				}
				selectors = append(selectors, selector)
			}
			out.Selectors = selectors
		}
		// 分配模式底层类型一致，显式转换即可。
		out.AllocationMode = resourcev1beta1.DeviceAllocationMode(in.Exactly.AllocationMode)
		out.Count = in.Exactly.Count
		out.AdminAccess = in.Exactly.AdminAccess
		var tolerations []resourcev1beta1.DeviceToleration
		// 容忍规则逐项反向转换。
		for _, e := range in.Exactly.Tolerations {
			var toleration resourcev1beta1.DeviceToleration
			if err := Convert_v1_DeviceToleration_To_v1beta1_DeviceToleration(&e, &toleration, s); err != nil {
				return err
			}
			tolerations = append(tolerations, toleration)
		}
		out.Tolerations = tolerations
		// CapacityRequirements 布局相同，unsafe 指针反向转换。
		out.Capacity = (*resourcev1beta1.CapacityRequirements)(unsafe.Pointer(in.Exactly.Capacity))
		if in.Exactly.DerivedAttributes != nil {
			// 派生属性逐项反向转换。
			derivedAttributes := make([]resourcev1beta1.DeviceDerivedAttribute, 0, len(in.Exactly.DerivedAttributes))
			for i := range in.Exactly.DerivedAttributes {
				var derivedAttribute resourcev1beta1.DeviceDerivedAttribute
				err := Convert_v1_DeviceDerivedAttribute_To_v1beta1_DeviceDerivedAttribute(&in.Exactly.DerivedAttributes[i], &derivedAttribute, s)
				if err != nil {
					return err
				}
				derivedAttributes = append(derivedAttributes, derivedAttribute)
			}
			out.DerivedAttributes = derivedAttributes
		}
	}
	return nil
}

// Convert_v1beta1_ResourceSliceSpec_To_v1_ResourceSliceSpec 把 v1beta1 的 ResourceSliceSpec 转为 v1 版本。
// 关键差异是 NodeName 与 AllNodes 在 v1 中改为指针，用 nil 表示“未设置”。
func Convert_v1beta1_ResourceSliceSpec_To_v1_ResourceSliceSpec(in *resourcev1beta1.ResourceSliceSpec, out *resourceapi.ResourceSliceSpec, s conversion.Scope) error {
	if err := autoConvert_v1beta1_ResourceSliceSpec_To_v1_ResourceSliceSpec(in, out, s); err != nil {
		return err
	}
	// 空字符串表示未指定节点，映射为 nil；否则取字符串地址。
	if in.NodeName == "" {
		out.NodeName = nil
	} else {
		out.NodeName = &in.NodeName
	}
	// false 表示未启用全节点，映射为 nil；true 则取地址表示显式设置。
	if !in.AllNodes {
		out.AllNodes = nil
	} else {
		out.AllNodes = &in.AllNodes
	}
	return nil
}

// Convert_v1_ResourceSliceSpec_To_v1beta1_ResourceSliceSpec 把 v1 的 ResourceSliceSpec 转为 v1beta1 版本。
// 把 v1 中可空的 NodeName/AllNodes 指针还原为 v1beta1 的标量零值。
func Convert_v1_ResourceSliceSpec_To_v1beta1_ResourceSliceSpec(in *resourceapi.ResourceSliceSpec, out *resourcev1beta1.ResourceSliceSpec, s conversion.Scope) error {
	if err := autoConvert_v1_ResourceSliceSpec_To_v1beta1_ResourceSliceSpec(in, out, s); err != nil {
		return err
	}
	// nil 指针还原为空字符串，否则解引用取值。
	if in.NodeName == nil {
		out.NodeName = ""
	} else {
		out.NodeName = *in.NodeName
	}
	// nil 指针还原为 false，否则解引用取布尔值。
	if in.AllNodes == nil {
		out.AllNodes = false
	} else {
		out.AllNodes = *in.AllNodes
	}
	return nil
}

// Convert_v1beta1_Device_To_v1_Device 把 v1beta1 的 Device 转换为 v1 版本。
// v1beta1 把大部分字段放在 BasicDevice 子结构中，v1 则把它们平铺到 Device 顶层，
// 因此需要手工搬运这些字段，并对 map/slice 中的元素逐项转换。
func Convert_v1beta1_Device_To_v1_Device(in *resourcev1beta1.Device, out *resourceapi.Device, s conversion.Scope) error {
	if err := autoConvert_v1beta1_Device_To_v1_Device(in, out, s); err != nil {
		return err
	}
	// Basic 为空说明没有基本设备信息，平铺字段保持零值。
	if in.Basic != nil {
		basic := in.Basic
		if len(basic.Attributes) > 0 {
			// 属性 map 的键与值类型都不同，必须逐项转换。
			attributes := make(map[resourceapi.QualifiedName]resourceapi.DeviceAttribute)
			if err := convert_v1beta1_Attributes_To_v1_Attributes(basic.Attributes, attributes, s); err != nil {
				return err
			}
			out.Attributes = attributes
		}
		if len(basic.Capacity) > 0 {
			// 容量 map 同样需要逐项转换限定名与容量值。
			capacity := make(map[resourceapi.QualifiedName]resourceapi.DeviceCapacity)
			if err := convert_v1beta1_Capacity_To_v1_Capacity(basic.Capacity, capacity, s); err != nil {
				return err
			}
			out.Capacity = capacity
		}
		var consumesCounters []resourceapi.DeviceCounterConsumption
		// 计数器消费规则逐项转换，遇到错误立即返回。
		for _, e := range basic.ConsumesCounters {
			var deviceCounterConsumption resourceapi.DeviceCounterConsumption
			if err := Convert_v1beta1_DeviceCounterConsumption_To_v1_DeviceCounterConsumption(&e, &deviceCounterConsumption, s); err != nil {
				return err
			}
			consumesCounters = append(consumesCounters, deviceCounterConsumption)
		}
		out.ConsumesCounters = consumesCounters
		// 节点名、节点选择器与全节点标记从 Basic 平铺到顶层。
		out.NodeName = basic.NodeName
		out.NodeSelector = basic.NodeSelector
		out.AllNodes = basic.AllNodes
		var taints []resourceapi.DeviceTaint
		// 设备污点逐项转换。
		for _, e := range basic.Taints {
			var taint resourceapi.DeviceTaint
			if err := Convert_v1beta1_DeviceTaint_To_v1_DeviceTaint(&e, &taint, s); err != nil {
				return err
			}
			taints = append(taints, taint)
		}
		out.Taints = taints
		// 以下绑定与分配相关标志在两版本中类型相同，直接搬运。
		out.BindsToNode = basic.BindsToNode
		out.BindingConditions = basic.BindingConditions
		out.BindingFailureConditions = basic.BindingFailureConditions
		out.AllowMultipleAllocations = basic.AllowMultipleAllocations
		if basic.NodeAllocatableResources != nil {
			// 可分配资源 map 的键相同但值类型分属两个版本，逐项调用自动转换函数。
			out.NodeAllocatableResources = make(map[corev1.ResourceName]resourceapi.NodeAllocatableResource)
			for key, value := range basic.NodeAllocatableResources {
				var outVal resourceapi.NodeAllocatableResource
				if err := autoConvert_v1beta1_NodeAllocatableResource_To_v1_NodeAllocatableResource(&value, &outVal, s); err != nil {
					return err
				}
				out.NodeAllocatableResources[key] = outVal
			}
		} else {
			// 源为 nil 时保持 nil，避免产生“非 nil 空 map”的不对称结果。
			out.NodeAllocatableResources = nil
		}
	}
	return nil
}

// Convert_v1_Device_To_v1beta1_Device 把 v1 的 Device 转换为 v1beta1 版本。
// 与正向转换相反：把 v1 顶层的字段收拢进 v1beta1 的 BasicDevice 子结构。
func Convert_v1_Device_To_v1beta1_Device(in *resourceapi.Device, out *resourcev1beta1.Device, s conversion.Scope) error {
	if err := autoConvert_v1_Device_To_v1beta1_Device(in, out, s); err != nil {
		return err
	}
	// 始终创建 Basic，保证 v1beta1 对象具备完整的基本设备信息结构。
	out.Basic = &resourcev1beta1.BasicDevice{}
	if len(in.Attributes) > 0 {
		// 属性 map 逐项反向转换。
		attributes := make(map[resourcev1beta1.QualifiedName]resourcev1beta1.DeviceAttribute)
		if err := convert_v1_Attributes_To_v1beta1_Attributes(in.Attributes, attributes, s); err != nil {
			return err
		}
		out.Basic.Attributes = attributes
	}

	if len(in.Capacity) > 0 {
		// 容量 map 逐项反向转换。
		capacity := make(map[resourcev1beta1.QualifiedName]resourcev1beta1.DeviceCapacity)
		if err := convert_v1_Capacity_To_v1beta1_Capacity(in.Capacity, capacity, s); err != nil {
			return err
		}
		out.Basic.Capacity = capacity
	}
	var consumesCounters []resourcev1beta1.DeviceCounterConsumption
	// 计数器消费规则逐项反向转换。
	for _, e := range in.ConsumesCounters {
		var deviceCounterConsumption resourcev1beta1.DeviceCounterConsumption
		if err := Convert_v1_DeviceCounterConsumption_To_v1beta1_DeviceCounterConsumption(&e, &deviceCounterConsumption, s); err != nil {
			return err
		}
		consumesCounters = append(consumesCounters, deviceCounterConsumption)
	}
	out.Basic.ConsumesCounters = consumesCounters
	out.Basic.NodeName = in.NodeName
	// NodeSelector 两版本定义一致，使用 unsafe 指针转换避免逐字段拷贝。
	out.Basic.NodeSelector = (*corev1.NodeSelector)(unsafe.Pointer(in.NodeSelector))
	out.Basic.AllNodes = in.AllNodes
	var taints []resourcev1beta1.DeviceTaint
	// 设备污点逐项反向转换。
	for _, e := range in.Taints {
		var taint resourcev1beta1.DeviceTaint
		if err := Convert_v1_DeviceTaint_To_v1beta1_DeviceTaint(&e, &taint, s); err != nil {
			return err
		}
		taints = append(taints, taint)
	}
	out.Basic.Taints = taints
	// 绑定与分配相关标志直接搬运到 Basic。
	out.Basic.BindsToNode = in.BindsToNode
	out.Basic.BindingConditions = in.BindingConditions
	out.Basic.BindingFailureConditions = in.BindingFailureConditions
	out.Basic.AllowMultipleAllocations = in.AllowMultipleAllocations
	if in.NodeAllocatableResources != nil {
		// 可分配资源 map 逐项调用自动转换函数。
		out.Basic.NodeAllocatableResources = make(map[corev1.ResourceName]resourcev1beta1.NodeAllocatableResource)
		for key, value := range in.NodeAllocatableResources {
			var outVal resourcev1beta1.NodeAllocatableResource
			if err := autoConvert_v1_NodeAllocatableResource_To_v1beta1_NodeAllocatableResource(&value, &outVal, s); err != nil {
				return err
			}
			out.Basic.NodeAllocatableResources[key] = outVal
		}
	} else {
		// 源为 nil 时目标也保持 nil。
		out.Basic.NodeAllocatableResources = nil
	}

	return nil
}

// convert_v1_Attributes_To_v1beta1_Attributes 把 v1 的设备属性 map 转换为 v1beta1 版本。
// 键类型分属两个包，需要显式转换；值通过生成的 DeviceAttribute 转换函数处理。
func convert_v1_Attributes_To_v1beta1_Attributes(in map[resourceapi.QualifiedName]resourceapi.DeviceAttribute, out map[resourcev1beta1.QualifiedName]resourcev1beta1.DeviceAttribute, s conversion.Scope) error {
	for k, v := range in {
		var a resourcev1beta1.DeviceAttribute
		// 属性值是 union 类型，必须由生成的转换函数处理。
		if err := Convert_v1_DeviceAttribute_To_v1beta1_DeviceAttribute(&v, &a, s); err != nil {
			return err
		}
		// 键从 v1 的限定名显式转换为 v1beta1 的限定名。
		out[resourcev1beta1.QualifiedName(k)] = a
	}
	return nil
}

// convert_v1_Capacity_To_v1beta1_Capacity 把 v1 的设备容量 map 转换为 v1beta1 版本。
// 与属性 map 的处理方式一致：键显式转换，值逐项调用生成函数。
func convert_v1_Capacity_To_v1beta1_Capacity(in map[resourceapi.QualifiedName]resourceapi.DeviceCapacity, out map[resourcev1beta1.QualifiedName]resourcev1beta1.DeviceCapacity, s conversion.Scope) error {
	for k, v := range in {
		var c resourcev1beta1.DeviceCapacity
		// 容量值逐项反向转换。
		if err := Convert_v1_DeviceCapacity_To_v1beta1_DeviceCapacity(&v, &c, s); err != nil {
			return err
		}
		// 键显式转换后写入目标 map。
		out[resourcev1beta1.QualifiedName(k)] = c
	}
	return nil
}

// convert_v1beta1_Attributes_To_v1_Attributes 把 v1beta1 的设备属性 map 转换为 v1 版本。
func convert_v1beta1_Attributes_To_v1_Attributes(in map[resourcev1beta1.QualifiedName]resourcev1beta1.DeviceAttribute, out map[resourceapi.QualifiedName]resourceapi.DeviceAttribute, s conversion.Scope) error {
	for k, v := range in {
		var a resourceapi.DeviceAttribute
		// 属性值由生成的转换函数处理，失败则终止整个转换。
		if err := Convert_v1beta1_DeviceAttribute_To_v1_DeviceAttribute(&v, &a, s); err != nil {
			return err
		}
		// 键从 v1beta1 显式转换为 v1 的限定名类型。
		out[resourceapi.QualifiedName(k)] = a
	}
	return nil
}

// convert_v1beta1_Capacity_To_v1_Capacity 把 v1beta1 的设备容量 map 转换为 v1 版本。
func convert_v1beta1_Capacity_To_v1_Capacity(in map[resourcev1beta1.QualifiedName]resourcev1beta1.DeviceCapacity, out map[resourceapi.QualifiedName]resourceapi.DeviceCapacity, s conversion.Scope) error {
	for k, v := range in {
		var c resourceapi.DeviceCapacity
		// 容量值逐项正向转换。
		if err := Convert_v1beta1_DeviceCapacity_To_v1_DeviceCapacity(&v, &c, s); err != nil {
			return err
		}
		// 键显式转换后写入目标 map。
		out[resourceapi.QualifiedName(k)] = c
	}
	return nil
}
