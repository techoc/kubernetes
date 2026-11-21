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

package resourceclaimspec

import (
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/apis/resource"
	"k8s.io/kubernetes/pkg/features"
)

// DropDisabledFields 从 ResourceClaimSpec 中移除被特性开关禁用的字段，
// 但保留旧 spec 中已经在用的字段。
//
// 理论上某些特性可能只在旧 ResourceClaim 的 status 中使用而未出现在 spec 中。
// 这只可能发生在 spec 更新场景，而目前整个 spec 是不可变的，所以不会出现。
// 即便将来允许更新，禁止向 spec 添加被禁用的字段也是正确的做法，
// 无需考虑 status 里曾经有什么。
func DropDisabledFields(new, old *resource.ResourceClaimSpec) {
	dropDisabledDRAPrioritizedListFields(new, old)
	// 注意顺序：DeviceTaints 的清理必须放在 PrioritizedList 之后，
	// 否则会遍历即将被剔除的 FirstAvailable 切片。
	dropDisabledDRADeviceTaintsFields(new, old)
	dropDisabledDRAAdminAccessFields(new, old)
	dropDisabledDRAResourceClaimConsumableCapacityFields(new, old)
	dropDisabledDRADerivedAttributesFields(new, old)
}

// dropDisabledDRADeviceTaintsFields 在 DRADeviceTaints 特性关闭且旧对象未使用时，
// 清除请求中的 Tolerations（包括 Exactly 和各个 FirstAvailable 子请求）。
func dropDisabledDRADeviceTaintsFields(new, old *resource.ResourceClaimSpec) {
	if utilfeature.DefaultFeatureGate.Enabled(features.DRADeviceTaints) ||
		draDeviceTaintsInUse(old) {
		return
	}

	for i, req := range new.Devices.Requests {
		if exactly := req.Exactly; exactly != nil {
			exactly.Tolerations = nil
		}
		for e := range req.FirstAvailable {
			new.Devices.Requests[i].FirstAvailable[e].Tolerations = nil
		}
	}
}

// draDeviceTaintsInUse 检查旧 spec 中是否已使用 device taints（即 Tolerations 非空）。
func draDeviceTaintsInUse(spec *resource.ResourceClaimSpec) bool {
	if spec == nil {
		return false
	}

	for _, req := range spec.Devices.Requests {
		if exactly := req.Exactly; exactly != nil && len(exactly.Tolerations) > 0 {
			return true
		}
		for _, sub := range req.FirstAvailable {
			if len(sub.Tolerations) > 0 {
				return true
			}
		}
	}

	return false
}

// dropDisabledDRAPrioritizedListFields 在 DRAPrioritizedList 特性关闭且旧对象未使用时，
// 清除所有请求中的 FirstAvailable 列表。
func dropDisabledDRAPrioritizedListFields(new, old *resource.ResourceClaimSpec) {
	if utilfeature.DefaultFeatureGate.Enabled(features.DRAPrioritizedList) {
		return
	}
	if draPrioritizedListFeatureInUse(old) {
		return
	}

	for i := range new.Devices.Requests {
		new.Devices.Requests[i].FirstAvailable = nil
	}
}

// draPrioritizedListFeatureInUse 检查旧 spec 中是否有请求使用了 FirstAvailable。
func draPrioritizedListFeatureInUse(spec *resource.ResourceClaimSpec) bool {
	if spec == nil {
		return false
	}

	for _, request := range spec.Devices.Requests {
		if len(request.FirstAvailable) > 0 {
			return true
		}
	}

	return false
}

// dropDisabledDRAAdminAccessFields 在 DRAAdminAccess 特性关闭且旧对象未使用时，
// 清除请求中的 AdminAccess 字段。
func dropDisabledDRAAdminAccessFields(new, old *resource.ResourceClaimSpec) {
	if utilfeature.DefaultFeatureGate.Enabled(features.DRAAdminAccess) ||
		DRAAdminAccessFeatureInUse(old) {
		// 无需清理。
		return
	}

	for i := range new.Devices.Requests {
		if new.Devices.Requests[i].Exactly != nil {
			new.Devices.Requests[i].Exactly.AdminAccess = nil
		}
	}
}

// DRAAdminAccessFeatureInUse 检查旧 spec 中是否已使用 AdminAccess 字段。
func DRAAdminAccessFeatureInUse(spec *resource.ResourceClaimSpec) bool {
	if spec == nil {
		return false
	}

	for _, request := range spec.Devices.Requests {
		if request.Exactly != nil && request.Exactly.AdminAccess != nil {
			return true
		}
	}

	return false
}

// dropDisabledDRAResourceClaimConsumableCapacityFields 在 DRAConsumableCapacity 特性关闭
// 且旧对象未使用时，清除 Constraints 的 DistinctAttribute 以及请求中的 Capacity 字段。
func dropDisabledDRAResourceClaimConsumableCapacityFields(new, old *resource.ResourceClaimSpec) {
	if utilfeature.DefaultFeatureGate.Enabled(features.DRAConsumableCapacity) ||
		DRAConsumableCapacityFeatureInUse(old) {
		// 无需清理。
		return
	}

	for i := range new.Devices.Constraints {
		new.Devices.Constraints[i].DistinctAttribute = nil
	}

	for i := range new.Devices.Requests {
		if new.Devices.Requests[i].Exactly != nil {
			new.Devices.Requests[i].Exactly.Capacity = nil
		}
		request := new.Devices.Requests[i]
		for j := range request.FirstAvailable {
			new.Devices.Requests[i].FirstAvailable[j].Capacity = nil
		}
	}
}

// DRAConsumableCapacityFeatureInUse 检查旧 spec 中是否已使用可消费容量相关字段。
func DRAConsumableCapacityFeatureInUse(spec *resource.ResourceClaimSpec) bool {
	if spec == nil {
		return false
	}

	for _, constaint := range spec.Devices.Constraints {
		if constaint.DistinctAttribute != nil {
			return true
		}
	}

	for _, request := range spec.Devices.Requests {
		if request.Exactly != nil && request.Exactly.Capacity != nil {
			return true
		}
		for _, subRequest := range request.FirstAvailable {
			if subRequest.Capacity != nil {
				return true
			}
		}
	}

	return false
}

// dropDisabledDRADerivedAttributesFields 在 DRADerivedAttributes 特性关闭且旧对象未使用时，
// 清除请求中的 DerivedAttributes 字段。
func dropDisabledDRADerivedAttributesFields(new, old *resource.ResourceClaimSpec) {
	if utilfeature.DefaultFeatureGate.Enabled(features.DRADerivedAttributes) ||
		draDerivedAttributesInUse(old) {
		return
	}

	for i, req := range new.Devices.Requests {
		if exactly := req.Exactly; exactly != nil {
			exactly.DerivedAttributes = nil
		}
		for e := range req.FirstAvailable {
			new.Devices.Requests[i].FirstAvailable[e].DerivedAttributes = nil
		}
	}
}

// draDerivedAttributesInUse 检查旧 spec 中是否已使用 DerivedAttributes 字段。
func draDerivedAttributesInUse(spec *resource.ResourceClaimSpec) bool {
	if spec == nil {
		return false
	}

	for _, req := range spec.Devices.Requests {
		if exactly := req.Exactly; exactly != nil && len(exactly.DerivedAttributes) > 0 {
			return true
		}
		for _, sub := range req.FirstAvailable {
			if len(sub.DerivedAttributes) > 0 {
				return true
			}
		}
	}

	return false
}
