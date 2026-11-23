/*
Copyright The Kubernetes Authors.

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
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

// compatibilityGroupIntersection is the rolling state of the
// DRADeviceCompatibilityGroups constraint for a single counter set. It captures
// the intersection of the compatibilityGroups declared by every device placed
// on that counter set so far - both already-allocated peers (their groups read
// live from the slice) and devices being allocated in the current attempt.
//
// The KEP nil-semantics are encoded explicitly: a device that declares no
// groups is only co-allocatable with sibling devices that likewise declare no
// groups. That cannot be represented by an ordinary set intersection (the empty
// set would wrongly reject a second no-group device), so the "all members had
// no groups" state is tracked separately via allEmpty.
//
// compatibilityGroupIntersection 是单个计数器集合上 DRADeviceCompatibilityGroups
// 约束的滚动状态。它记录到目前为止放置在该计数器集合上的所有设备所声明兼容组的交集，
// 既包括已分配的同伴设备（其组列表从切片中实时读取），也包括当前分配尝试中正在分配的设备。
//
// KEP 的 nil 语义在此被显式编码：未声明任何兼容组的设备，只能与同样未声明兼容组的兄弟设备
// 共同分配。这无法用普通的集合交集表达（空集会错误地拒绝第二个无组设备），因此用 allEmpty
// 单独跟踪“所有成员都无组”这一状态。
type compatibilityGroupIntersection struct {
	// hasMembers is false until the first device is placed on the counter set.
	// While false any candidate is admissible.
	// hasMembers 在第一个设备被放置到计数器集合之前为 false；为 false 时任何候选设备都可接纳。
	hasMembers bool
	// allEmpty is true while every device placed so far declared no groups. In
	// that state only further no-group devices are admissible. Only meaningful
	// when hasMembers is true.
	// allEmpty 在到目前为止放置的每个设备都未声明兼容组时为 true；此时只允许继续接纳无组设备。
	// 仅当 hasMembers 为 true 时该字段才有意义。
	allEmpty bool
	// groups is the running intersection of the (non-empty) group lists of the
	// devices placed so far. Only meaningful when hasMembers && !allEmpty.
	// groups 是到目前为止已放置设备（非空）组列表的滚动交集。仅当 hasMembers 为 true
	// 且 allEmpty 为 false 时该字段才有意义。
	groups sets.Set[string]
}

// admits reports whether a device declaring the given groups may be co-allocated
// on the counter set, given the devices already folded into the intersection.
//
// admits 判断在已有交集状态的前提下，声明了给定兼容组的设备是否可以与该计数器集合上的
// 设备共同分配。
func (i compatibilityGroupIntersection) admits(groups sets.Set[string]) bool {
	// 尚无成员时交集为空，任何候选设备都可接纳。
	if !i.hasMembers {
		return true
	}
	// 候选设备无组：只有已有成员也全部无组时才兼容。
	if groups.Len() == 0 {
		// A device with no groups only co-allocates with other no-group devices.
		return i.allEmpty
	}
	// 已有成员全部无组，而有组的候选设备与之不兼容。
	if i.allEmpty {
		// Existing members declared no groups; a grouped device is incompatible.
		return false
	}
	// 双方都有组：只要交集非空即可共同分配。
	return i.groups.Intersection(groups).Len() > 0
}

// add folds a device's groups into the intersection and returns the updated
// value. It never mutates the receiver's set in place (it only ever assigns a
// freshly created set), so a value copy taken before add is a safe snapshot for
// backtracking. The caller is expected to have verified admits(groups) first;
// add stays defensive for the inconsistent states that slice mutation could
// otherwise produce.
//
// add 把一个设备的兼容组折叠进当前交集并返回更新后的值。它从不在原地修改接收者的集合
// （只赋新创建的集合），因此 add 之前取的值副本可安全地用于回溯。调用方应先通过 admits
// 校验；add 对切片变更可能导致的不一致状态仍保持防御性。
func (i compatibilityGroupIntersection) add(groups sets.Set[string]) compatibilityGroupIntersection {
	// 第一个成员：据此确定 allEmpty 并用其组列表（克隆）初始化交集。
	if !i.hasMembers {
		i.hasMembers = true
		// 首个设备无组：进入 allEmpty 状态，交集置空。
		if groups.Len() == 0 {
			i.allEmpty = true
			i.groups = nil
		} else {
			// 首个设备有组：克隆其组列表作为交集初值，避免与调用方共享底层 map。
			i.allEmpty = false
			i.groups = groups.Clone()
		}
		return i
	}
	// 已有成员或新设备任一方无组；admits() 已保证另一方匹配，状态无需变化。
	if i.allEmpty || groups.Len() == 0 {
		// Either existing members or the new device declared no groups; admits()
		// guarantees the other side matched, so the state is unchanged.
		return i
	}
	// 双方都有组：计算交集，进一步收窄可共同分配的范围。
	i.groups = i.groups.Intersection(groups)
	return i
}

// compatibilityGroupSet builds a set from a device's declared groups. A nil or
// empty list yields a nil set, which the intersection logic treats as "no
// groups".
//
// compatibilityGroupSet 根据设备声明的兼容组构建集合。nil 或空列表返回 nil 集合，
// 交集逻辑将其视为“无兼容组”。
func compatibilityGroupSet(groups []string) sets.Set[string] {
	// 空列表与 nil 一视同仁，都返回 nil 集合。
	if len(groups) == 0 {
		return nil
	}
	return sets.New[string](groups...)
}

// sliceUsesCompatibilityGroups reports whether any device in the slice declares
// compatibility groups on one of its counter consumptions.
//
// While the DRADeviceCompatibilityGroups feature is disabled, GatherPools
// ignores such slices entirely: this allocator cannot validate co-allocation
// against compatibility groups, so those devices must not be allocated, and
// ignoring the slice makes the pool incomplete, which prevents allocating the
// pool's other devices, too. Whether any of the devices are already allocated
// is deliberately irrelevant, keeping the outcome independent of allocation
// order. This lets the feature be enabled later without deleting pods.
// Enforcement (feature on) uses checkAndConsumeCompatibilityGroups and the
// per-pool baseline instead.
//
// sliceUsesCompatibilityGroups 报告切片中是否有任何设备在其某次计数器消费上声明了兼容组。
// 只要发现一个非空的兼容组列表就立即返回 true，否则返回 false。
func sliceUsesCompatibilityGroups(slice *resourceapi.ResourceSlice) bool {
	// 遍历切片中的每个设备。
	for _, device := range slice.Spec.Devices {
		// 再遍历该设备的每一次计数器消费声明。
		for _, deviceCounterConsumption := range device.ConsumesCounters {
			// 任意一处声明了兼容组即视为该切片使用了兼容组。
			if len(deviceCounterConsumption.CompatibilityGroups) > 0 {
				return true
			}
		}
	}
	return false
}

// slicesWithoutCompatibilityGroups returns the slices which do not match
// sliceUsesCompatibilityGroups, preserving their order.
//
// slicesWithoutCompatibilityGroups 返回不使用兼容组的切片（即 sliceUsesCompatibilityGroups
// 为 false 的切片），并保持原有顺序。
func slicesWithoutCompatibilityGroups(slices []*resourceapi.ResourceSlice) []*resourceapi.ResourceSlice {
	filtered := make([]*resourceapi.ResourceSlice, 0, len(slices))
	for _, slice := range slices {
		// 跳过使用兼容组的切片，保留其余切片及其相对顺序。
		if sliceUsesCompatibilityGroups(slice) {
			continue
		}
		filtered = append(filtered, slice)
	}
	return filtered
}
