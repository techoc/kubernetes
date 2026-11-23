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

package internal

import (
	"context"
	"errors"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

// ErrFailedAllocationOnNode is an empty sentinel for errors.Is.
// See allocator.go for details.
// ErrFailedAllocationOnNode 是一个空文本的哨兵错误，供 errors.Is 使用；详见 allocator.go。
var ErrFailedAllocationOnNode = errors.New("")

// DeviceClassLister 提供对 DeviceClass 的查询能力，供分配器读取设备类定义。
type DeviceClassLister interface {
	// List returns a list of all DeviceClasses.
	// List 返回所有 DeviceClass。
	List() ([]*resourceapi.DeviceClass, error)
	// Get returns the DeviceClass with the given className.
	// Get 返回指定类名 className 对应的 DeviceClass。
	Get(className string) (*resourceapi.DeviceClass, error)
}

// Allocator is intentionally not documented here. See the main package for docs.
//
// This interface is also broader than the public one.
// Allocator 有意不在此处写文档，说明请参见主包。
// 该接口比公开接口更宽：额外暴露了 Channel 方法。
type Allocator interface {
	// Allocate 为指定节点计算分配方案，返回每个未分配 claim 的分配结果。
	Allocate(ctx context.Context, node *v1.Node, claims []*resourceapi.ResourceClaim) (finalResult []resourceapi.AllocationResult, finalErr error)
	// Channel 返回该分配器所属的实现通道标识。
	Channel() AllocatorChannel
}

// AllocatorExtended is an optional interface. Not all variants implement it.
// AllocatorExtended 是可选接口，并非所有分配器实现都支持它。
type AllocatorExtended interface {
	// Stats shows statistics from the allocation process.
	// May return nil if not implemented.
	// Stats 展示分配过程的统计信息；未实现时可能返回 nil。
	// GetStats 返回分配过程统计信息。
	GetStats() Stats
}

// Stats shows statistics from the allocation process.
// Stats 记录分配过程相关的统计信息。
type Stats struct {
	// NumAllocateOneInvocations counts the number of times the allocateOne function
	// got called.
	// NumAllocateOneInvocations 统计 allocateOne 函数被调用的次数。
	NumAllocateOneInvocations int64
}

// AllocatorChannel 表示分配器实现所属的通道（stable/incubating/experimental）。
type AllocatorChannel string

const (
	// Experimental 对应全新实现通道。
	// Stable 对应最稳定实现通道。
	// Incubating 对应即将稳定化的实现通道。
	Experimental = "experimental"
	Stable       = "stable"
	Incubating   = "incubating"
)

// Features control optional functionality during ResourceClaim allocation.
// Each entry must correspond to at least one control flow change. Entries can
// be removed when the control flow change is no longer necessary (= feature is
// considered stable and always enabled).
//
// This often corresponds to feature gates, but not always: if a KEP implementation
// depends on a set of feature gates, then a single entry here should control whether
// that implementation is active.
// Features 控制 ResourceClaim 分配过程中的可选功能。
// 每个条目必须至少对应一处控制流分支；当功能稳定且始终启用时可移除该条目。
// 这通常与 feature gate 对应，但不总是如此：若某个 KEP 的实现依赖多个 feature gate，则用一个条目统一控制。
type Features struct {
	// Sorted alphabetically. When adding a new entry, also extend Set and FeaturesAll.
	// 字段按字母顺序排列；新增条目时须同步更新 Set 与 FeaturesAll。

	// AdminAccess：管理员访问权限（DRAAdminAccess）。
	// CompatibilityGroups：设备兼容性组（DRADeviceCompatibilityGroups）。
	// ConsumableCapacity：可消费容量（DRAConsumableCapacity）。
	// DerivedAttributes：派生属性（DRADerivedAttributes）。
	// DeviceBindingAndStatus：设备绑定条件与 ResourceClaim 设备状态。
	// DeviceTaints：设备污点（DRADeviceTaints）。
	// FractionalCapacityRange：分数容量范围（DRAFractionalCapacityRange）。
	// ListTypeAttributes：列表类型属性（DRAListTypeAttributes）。
	// OptionalNodeOperations：可选节点操作（DRAOptionalNodeOperations）。
	// PartitionableDevices：可分区设备（DRAPartitionableDevices）。
	// PrioritizedList：优先级列表（DRAPrioritizedList）。
	AdminAccess             bool
	CompatibilityGroups     bool
	ConsumableCapacity      bool
	DerivedAttributes       bool
	DeviceBindingAndStatus  bool
	DeviceTaints            bool
	FractionalCapacityRange bool
	ListTypeAttributes      bool
	OptionalNodeOperations  bool
	PartitionableDevices    bool
	PrioritizedList         bool
}

// Set returns all features which are set to true.
// The names of the features happen to match the Kubernetes
// feature gates where applicable. Plain strings are used
// because not all allocator features necessarily have to
// be Kubernetes feature gates and this package must not
// depend on those definitions.
// Set 返回所有设为 true 的特性名称集合。
// 名称在适用时与 Kubernetes feature gate 一致；这里使用纯字符串，因为并非所有分配器特性都必须是 feature gate，且本包不得依赖那些定义。
func (f Features) Set() sets.Set[string] {
	// 先创建空集合。
	enabled := sets.New[string]()
	// 依次把已启用的特性映射到对应的字符串名称。
	if f.AdminAccess {
		enabled.Insert("DRAAdminAccess")
	}
	if f.CompatibilityGroups {
		enabled.Insert("DRADeviceCompatibilityGroups")
	}
	if f.ConsumableCapacity {
		enabled.Insert("DRAConsumableCapacity")
	}
	if f.DerivedAttributes {
		enabled.Insert("DRADerivedAttributes")
	}
	if f.DeviceTaints {
		enabled.Insert("DRADeviceTaints")
	}
	if f.FractionalCapacityRange {
		enabled.Insert("DRAFractionalCapacityRange")
	}
	if f.ListTypeAttributes {
		enabled.Insert("DRAListTypeAttributes")
	}
	if f.OptionalNodeOperations {
		enabled.Insert("DRAOptionalNodeOperations")
	}
	if f.PartitionableDevices {
		enabled.Insert("DRAPartitionableDevices")
	}
	if f.PrioritizedList {
		enabled.Insert("DRAPrioritizedList")
	}
	// 设备绑定条件与设备状态由两个 feature gate 共同控制，映射为一个组合名称。
	if f.DeviceBindingAndStatus {
		enabled.Insert("DRADeviceBindingConditions+DRAResourceClaimDeviceStatus")
	}
	// 返回收集到的特性名称集合。
	return enabled
}

// FeaturesAll 是启用了全部可选特性的配置，供测试使用。
var FeaturesAll = Features{
	AdminAccess:             true,
	CompatibilityGroups:     true,
	ConsumableCapacity:      true,
	DerivedAttributes:       true,
	DeviceBindingAndStatus:  true,
	DeviceTaints:            true,
	FractionalCapacityRange: true,
	ListTypeAttributes:      true,
	OptionalNodeOperations:  true,
	PartitionableDevices:    true,
	PrioritizedList:         true,
}
