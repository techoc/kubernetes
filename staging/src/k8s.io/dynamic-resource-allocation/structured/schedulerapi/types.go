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

package schedulerapi

import (
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	draapi "k8s.io/dynamic-resource-allocation/api"
)

// DeviceID represents a unique identifier for a device in the DRA system.
// This type is used in the scheduler and autoscaler contract.
// DeviceID 表示 DRA 系统中设备的唯一标识，由驱动器、资源池和设备三级名称组成，
// 该类型属于调度器与自动扩缩容器契约的一部分。
type DeviceID struct {
	// Driver、Pool、Device 分别为驱动器名、资源池名和设备名，均以 UniqueString 包装以避免字符串混淆。
	Driver, Pool, Device draapi.UniqueString
}

// String 以“驱动器/资源池/设备”的形式返回设备标识字符串。
func (d DeviceID) String() string {
	return d.Driver.String() + "/" + d.Pool.String() + "/" + d.Device.String()
}

// MakeDeviceID creates a new DeviceID from driver, pool, and device names.
// This function is used in the scheduler and autoscaler contract.
// MakeDeviceID 由驱动器名、资源池名和设备名构造 DeviceID，属于调度器与自动扩缩容器契约的一部分。
func MakeDeviceID(driver, pool, device string) DeviceID {
	// 把三个名称分别包装成 UniqueString。
	return DeviceID{
		Driver: draapi.MakeUniqueString(driver),
		Pool:   draapi.MakeUniqueString(pool),
		Device: draapi.MakeUniqueString(device),
	}
}

// SharedDeviceID represents a shared device allocation.
// This type is used in consumable capacity features and the scheduler.
// SharedDeviceID 表示一次共享设备分配，
// 在可消费容量特性与调度器中用于区分同一设备上的不同共享份额。
type SharedDeviceID struct {
	// Driver、Pool、Device 标识设备，ShareID 标识该设备上的共享份额，可以为空。
	Driver, Pool, Device, ShareID draapi.UniqueString
}

// String 返回共享设备标识字符串；ShareID 非空时将其追加到末尾。
func (d SharedDeviceID) String() string {
	// 先拼出基础的设备 ID 字符串。
	deviceIDStr := d.Driver.String() + "/" + d.Pool.String() + "/" + d.Device.String()
	// 存在共享 ID 时追加到字符串末尾。
	if d.ShareID.String() != "" {
		deviceIDStr += "/" + d.ShareID.String()
	}
	return deviceIDStr
}

// GetDeviceID 去掉共享 ID，返回对应的基础 DeviceID。
func (d SharedDeviceID) GetDeviceID() DeviceID {
	return DeviceID{d.Driver, d.Pool, d.Device}
}

// MakeSharedDeviceID creates a new SharedDeviceID from a DeviceID and share ID.
// This function is used in consumable capacity features and the scheduler.
// MakeSharedDeviceID 由 DeviceID 与可选的共享 ID 构造 SharedDeviceID，用于可消费容量特性与调度器。
func MakeSharedDeviceID(deviceID DeviceID, shareID *types.UID) SharedDeviceID {
	// This function avoids disruptive changes to MakeDeviceID
	// while enabling ShareID as part of the device key.
	// 这样既避免对 MakeDeviceID 的破坏性改动，又把 ShareID 纳入设备键。
	// 共享 ID 为 nil 时按空字符串处理。
	var shareIDStr string
	if shareID != nil {
		shareIDStr = string(*shareID)
	}
	// 复用 deviceID 的三级名称，仅新增 ShareID 字段。
	return SharedDeviceID{
		Driver:  deviceID.Driver,
		Pool:    deviceID.Pool,
		Device:  deviceID.Device,
		ShareID: draapi.MakeUniqueString(shareIDStr),
	}
}

// AllocatedState represents the current state of allocated resources.
// This type is used in the scheduler and autoscaler contract.
// AllocatedState packs information of allocated devices which is gathered from allocated resource claims.
// AllocatedState 描述已分配资源的当前状态，由已分配的 ResourceClaim 汇总而来；
// 属于调度器与自动扩缩容器契约的一部分。
type AllocatedState struct {
	// AllocatedDevices 记录被整体分配的设备集合。
	// AllocatedSharedDeviceIDs 记录被共享分配（部分消费）的设备份额集合。
	// AggregatedCapacity 记录按设备聚合的已消费容量。
	AllocatedDevices         sets.Set[DeviceID]
	AllocatedSharedDeviceIDs sets.Set[SharedDeviceID]
	AggregatedCapacity       ConsumedCapacityCollection
}

// ConsumedCapacity represents the consumed capacity of a specific resource.
// This type is used in consumable capacity features and the scheduler.
// ConsumedCapacity defines consumable capacity values
// ConsumedCapacity 表示某设备上各可消费容量名称对应的已消费数量，用于可消费容量特性与调度器。
type ConsumedCapacity map[resourceapi.QualifiedName]*resource.Quantity

// NewConsumedCapacity creates a new ConsumedCapacity.
// This function is used in consumable capacity features and the scheduler.
// NewConsumedCapacity initiates a new map of consumable capacity values
// NewConsumedCapacity 创建一个空的已消费容量映射。
func NewConsumedCapacity() ConsumedCapacity {
	return make(ConsumedCapacity)
}

// Clone 深拷贝已消费容量映射，其中的 Quantity 也逐个复制。
// Clone makes a copy of consumed capacity values
func (s ConsumedCapacity) Clone() ConsumedCapacity {
	// 创建结果映射。
	clone := make(ConsumedCapacity)
	// 对每个数量做深拷贝，避免共享底层对象。
	for name, quantity := range s {
		q := quantity.DeepCopy()
		clone[name] = &q
	}
	return clone
}

// Add adds quantity to corresponding consumable capacity,
// and creates a new entry if no capacity created yet.
// Add 将另一份已消费容量累加进来；若某个名称尚不存在则新建条目。
func (s ConsumedCapacity) Add(addedCapacity ConsumedCapacity) {
	// 逐项累加；数量先做深拷贝以免出现别名。
	for name, quantity := range addedCapacity {
		val := quantity.DeepCopy()
		// 名称已存在则相加，否则直接写入副本。
		if _, found := s[name]; found {
			s[name].Add(val)
		} else {
			s[name] = &val
		}
	}
}

// Sub subtracts quantity,
// and ignore if no capacity entry found.
// Sub 从当前容量中减去指定的已消费容量；若对应条目不存在则忽略。
func (s ConsumedCapacity) Sub(subtractedCapacity ConsumedCapacity) {
	// 逐项相减，缺失条目直接跳过。
	for name, quantity := range subtractedCapacity {
		if _, found := s[name]; found {
			s[name].Sub(*quantity)
		}
	}
}

// Empty return true if all quantity is zero.
// Empty 在所有数量均为零时返回 true。
func (s ConsumedCapacity) Empty() bool {
	// 发现任一非零数量即返回 false。
	for _, quantity := range s {
		if !quantity.IsZero() {
			return false
		}
	}
	return true
}

// ConsumedCapacityCollection represents a collection of consumed capacities.
// This type is used in consumable capacity features and the scheduler.
// ConsumedCapacityCollection collects consumable capacity values of each device
// ConsumedCapacityCollection 按设备 ID 汇总每台设备的已消费容量，
// 用于可消费容量特性与调度器。
type ConsumedCapacityCollection map[DeviceID]ConsumedCapacity

// NewConsumedCapacityCollection creates a new ConsumedCapacityCollection.
// This function is used in consumable capacity features and the scheduler.
// NewConsumedCapacityCollection initiates a new map of device's consumable capacity values
// NewConsumedCapacityCollection 创建一个空的已消费容量集合。
func NewConsumedCapacityCollection() ConsumedCapacityCollection {
	return make(ConsumedCapacityCollection)
}

// Clone makes a copy of ConsumedCapacity of each capacity.
// Clone 深拷贝集合中每台设备的已消费容量。
func (c ConsumedCapacityCollection) Clone() ConsumedCapacityCollection {
	clone := NewConsumedCapacityCollection()
	for deviceID, share := range c {
		clone[deviceID] = share.Clone()
	}
	return clone
}

// Insert adds a new allocated capacity to the collection.
// Insert 将一份设备已消费容量加入集合；若该设备已有记录则累加。
func (c ConsumedCapacityCollection) Insert(cap DeviceConsumedCapacity) {
	// 取出待插入的容量映射。
	consumedCapacity := cap.ConsumedCapacity
	// 已有记录则累加，否则写入深拷贝。
	if _, found := c[cap.DeviceID]; found {
		c[cap.DeviceID].Add(consumedCapacity)
	} else {
		c[cap.DeviceID] = consumedCapacity.Clone()
	}
}

// Remove removes an allocated capacity from the collection.
// Remove 从集合中扣除一份设备已消费容量。
func (c ConsumedCapacityCollection) Remove(cap DeviceConsumedCapacity) {
	// 仅当该设备存在记录时才处理。
	if _, found := c[cap.DeviceID]; found {
		c[cap.DeviceID].Sub(cap.ConsumedCapacity)
		// 扣除后若容量归零，则移除该设备条目。
		if c[cap.DeviceID].Empty() {
			delete(c, cap.DeviceID)
		}
	}
}

// DeviceConsumedCapacity represents the consumed capacity of a device.
// This type is used in consumable capacity features and the scheduler.
// DeviceConsumedCapacity contains consumed capacity result within device allocation.
// DeviceConsumedCapacity 表示一次设备分配中已消费的容量，
// 内嵌 DeviceID 与 ConsumedCapacity 两个字段。
type DeviceConsumedCapacity struct {
	DeviceID
	ConsumedCapacity
}

// NewDeviceConsumedCapacity creates a new DeviceConsumedCapacity.
// This function is used in consumable capacity features and the scheduler.
// NewDeviceConsumedCapacity creates DeviceConsumedCapacity instance from device ID and its consumed capacity.
// NewDeviceConsumedCapacity 由设备 ID 与容量映射构造 DeviceConsumedCapacity。
func NewDeviceConsumedCapacity(deviceID DeviceID, consumedCapacity map[resourceapi.QualifiedName]resource.Quantity) DeviceConsumedCapacity {
	// 构造容量映射并逐个复制数量。
	allocatedCapacity := NewConsumedCapacity()
	for name, quantity := range consumedCapacity {
		// 把每个容量数量写入映射。
		allocatedCapacity[name] = &quantity
	}
	return DeviceConsumedCapacity{
		DeviceID:         deviceID,
		ConsumedCapacity: allocatedCapacity,
	}
}

// Clone makes a copy of DeviceConsumedCapacity.
// Clone 深拷贝 DeviceConsumedCapacity。
func (a DeviceConsumedCapacity) Clone() DeviceConsumedCapacity {
	return DeviceConsumedCapacity{
		DeviceID:         a.DeviceID,
		ConsumedCapacity: a.ConsumedCapacity.Clone(),
	}
}

// String returns formatted device ID.
// String 返回格式化后的设备 ID，不包含已消费容量。
func (a DeviceConsumedCapacity) String() string {
	return a.DeviceID.String()
}
