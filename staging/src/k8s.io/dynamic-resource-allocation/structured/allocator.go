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

package structured

import (
	"context"
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/dynamic-resource-allocation/cel"
	"k8s.io/dynamic-resource-allocation/structured/internal"
	"k8s.io/dynamic-resource-allocation/structured/internal/experimental"
	"k8s.io/dynamic-resource-allocation/structured/internal/incubating"
	"k8s.io/dynamic-resource-allocation/structured/internal/stable"
	"k8s.io/dynamic-resource-allocation/structured/schedulerapi"
)

// ErrFailedAllocationOnNode 是 Allocate 返回错误的基错误（哨兵错误），
// 与其余错误不同，它只影响单个节点，而不会影响整个调度尝试。
// 调度器应当用 errors.Is 检测该错误，并把错误字符串作为节点不可调度的原因上报。
// 该错误本身没有任何文本，可通过 fmt.Errorf("%w其他错误", ErrFailedAllocationOnNode) 这种方式使用。
var ErrFailedAllocationOnNode = internal.ErrFailedAllocationOnNode

// 为简化不同包中的代码，这里到处使用类型别名。
// 函数采用包装函数而不是变量，以便编译器优化。
// Allocator 接口被有意定义了两次：这样本包的文档会更有用。

// DeviceClassLister 是 DeviceClass 查询接口的别名，实际定义位于 internal 包。
// Features 是可选特性开关集合的别名，实际定义位于 internal 包。
type DeviceClassLister = internal.DeviceClassLister
type Features = internal.Features

// 以下类型别名指向 schedulerapi 包，这些类型属于调度器与自动扩缩容器之间的契约。
// 这样做可确保对这些类型的修改需要自动扩缩容器批准。
// DeviceID 是设备唯一标识（驱动器/资源池/设备）的别名。
// AllocatedState 描述集群中已分配资源的当前状态，是调度器与自动扩缩容器契约的一部分。
// SharedDeviceID 表示共享设备的分配标识（含共享 ID），用于可消费容量场景。
// DeviceConsumedCapacity 表示单个设备已消费的容量。
// ConsumedCapacityCollection 是按设备聚合的已消费容量集合。
// ConsumedCapacity 是容量名称到已消费数量的映射。
type DeviceID = schedulerapi.DeviceID
type AllocatedState = schedulerapi.AllocatedState
type SharedDeviceID = schedulerapi.SharedDeviceID
type DeviceConsumedCapacity = schedulerapi.DeviceConsumedCapacity
type ConsumedCapacityCollection = schedulerapi.ConsumedCapacityCollection
type ConsumedCapacity = schedulerapi.ConsumedCapacity

// MakeDeviceID 根据驱动器名、资源池名和设备名构造 DeviceID，委托给 schedulerapi 包实现。
func MakeDeviceID(driver, pool, device string) DeviceID {
	return schedulerapi.MakeDeviceID(driver, pool, device)
}

// MakeSharedDeviceID 由设备 ID 与可选的共享 ID 构造 SharedDeviceID，委托给 schedulerapi 包实现。
func MakeSharedDeviceID(deviceID DeviceID, shareID *types.UID) SharedDeviceID {
	return schedulerapi.MakeSharedDeviceID(deviceID, shareID)
}

// NewConsumedCapacityCollection 创建一个空的已消费容量集合，委托给 schedulerapi 包实现。
func NewConsumedCapacityCollection() ConsumedCapacityCollection {
	return schedulerapi.NewConsumedCapacityCollection()
}

// NewDeviceConsumedCapacity 由设备 ID 和已消费容量映射构造 DeviceConsumedCapacity，委托给 schedulerapi 包实现。
func NewDeviceConsumedCapacity(deviceID DeviceID,
	consumedCapacity map[resourceapi.QualifiedName]resource.Quantity) DeviceConsumedCapacity {
	return schedulerapi.NewDeviceConsumedCapacity(deviceID, consumedCapacity)
}

// Allocator 负责计算一组使用结构化参数的未分配 ResourceClaim 的分配方案。
// 输入包括分配结果要生效的目标节点，以及集群当前状态（claim、class、resource slice）。
// 返回的 Allocator 可重复使用，并且是线程安全的。
type Allocator interface {
	// Allocate 为某个具体节点计算分配方案。
	// 仅在发生致命问题时才返回错误，例如输入数据非法、CEL 选择器出错；调度器应中止并上报，而不是换节点重试。
	// 若 claim 无法分配则返回 nil，其中包括资源切片当前不完整的情形。
	// 若能分配，则为每个未分配的 claim 准备一份 AllocationResult；是否持久化由调用方决定。
	// Allocate 是线程安全的。若希望日志中包含节点名，可用 contextual logging 把节点作为附加字段；V(5) 用于一次性日志，V(6) 用于重要进度，V(7) 用于详细调试。
	// 支持上下文取消：取消时返回包装了 context 错误的错误。
	Allocate(ctx context.Context, node *v1.Node, claims []*resourceapi.ResourceClaim) (finalResult []resourceapi.AllocationResult, finalErr error)
}

// NewAllocator 根据特性集合、已分配状态、设备类查询器、资源切片与 CEL 缓存创建分配器；
// 若检测到导致无法分配的问题则返回错误。返回的 Allocator 可多次使用，并且是线程安全的。
func NewAllocator(ctx context.Context,
	features Features,
	allocatedState AllocatedState,
	classLister DeviceClassLister,
	slices []*resourceapi.ResourceSlice,
	celCache *cel.Cache,
) (Allocator, error) {
	// 上述设计要点：stable、incubating、experimental 三套实现彼此完全独立，支持的特性逐级扩展，
	// 大致对应 GA/beta/alpha，但有意不这样命名，因为 stable 支持的特性未必都已 GA。
	// 每套实现位于独立包中，文件名后缀便于从日志追溯来源，公共函数有意重复而非共享，
	// 便于逐套比较并整体拷贝升级。单元测试在三套实现之间共享。
	// 记录当前已启用的分配器名称，用于在错误中给出提示。
	var enabledAllocators []string
	// 按优先级（最稳定优先）遍历所有可用的分配器实现。
	for _, allocator := range availableAllocators {
		// 该实现是否被禁用？
		if !allocatorEnabled(allocator.name) {
			continue
		}
		enabledAllocators = append(enabledAllocators, allocator.name)
		// 记录该分配器处于启用状态。

		// 该实现是否支持所需的全部特性？
		if allocator.supportedFeatures.Set().IsSuperset(features.Set()) {
			// 支持则采用该实现并立即返回。
			return allocator.newAllocator(ctx, features, allocatedState, classLister, slices, celCache)
		}
	}
	// 没有任何启用的分配器支持该特性集合，返回内部错误。
	return nil, fmt.Errorf("internal error: no allocator available for feature set %+v, enabled allocators: %s", features, strings.Join(enabledAllocators, ", "))
}

// EnableAllocators 在传入非空列表时控制 NewAllocator 可以选择哪些分配器。
// 列表项为实现包名（stable、incubating、experimental）。
// 该函数不是线程安全的，仅供测试使用。
func EnableAllocators(names ...string) {
	// 把显式启用的名称列表保存为集合。
	explicitlyEnabledAllocators = sets.New(names...)
}

// explicitlyEnabledAllocators 保存 EnableAllocators 的设置；默认为空，表示所有可用分配器均启用。
var explicitlyEnabledAllocators sets.Set[string]

// allocatorEnabled 判断名为 name 的分配器是否启用：未显式设置时全部启用。
func allocatorEnabled(name string) bool {
	// 未显式设置，或集合中包含该名称时返回 true。
	return len(explicitlyEnabledAllocators) == 0 || explicitlyEnabledAllocators.Has(name)
}

// availableAllocators 按稳定性从高到低列出所有可用实现，以及各自支持的特性集合、构造函数和节点匹配函数。
var availableAllocators = []struct {
	// name 为实现包名：stable、incubating 或 experimental。
	// supportedFeatures 为该实现支持的特性集合。
	// newAllocator 用于构造该实现的分配器实例。
	// nodeMatches 用于判断节点是否匹配给定选择器/节点名。
	name              string
	supportedFeatures Features
	newAllocator      func(ctx context.Context,
		features Features,
		allocatedState AllocatedState,
		classLister DeviceClassLister,
		slices []*resourceapi.ResourceSlice,
		celCache *cel.Cache,
	) (Allocator, error)
	nodeMatches func(node *v1.Node,
		nodeNameToMatch string,
		allNodesMatch bool,
		nodeSelector *v1.NodeSelector,
	) (bool, error)
}{
	// 最稳定的实现放在最前面。
	// stable：最旧、最稳定的实现。
	{
		name:              "stable",
		supportedFeatures: stable.SupportedFeatures,
		newAllocator: func(ctx context.Context,
			features Features,
			allocatedState AllocatedState,
			classLister DeviceClassLister,
			slices []*resourceapi.ResourceSlice,
			celCache *cel.Cache,
		) (Allocator, error) {
			// stable 版本只接收已分配设备集合，不处理共享设备与已消费容量。
			return stable.NewAllocator(ctx, features, allocatedState.AllocatedDevices, classLister, slices, celCache)
		},
		nodeMatches: stable.NodeMatches,
	},
	// incubating：目标是成为下一个 stable 的实现。
	{
		name:              "incubating",
		supportedFeatures: incubating.SupportedFeatures,
		newAllocator: func(ctx context.Context,
			features Features,
			allocatedState AllocatedState,
			classLister DeviceClassLister,
			slices []*resourceapi.ResourceSlice,
			celCache *cel.Cache,
		) (Allocator, error) {
			// incubating 版本接收完整的 AllocatedState。
			return incubating.NewAllocator(ctx, features, allocatedState, classLister, slices, celCache)
		},
		nodeMatches: incubating.NodeMatches,
	},
	// experimental：全新的实现。
	{
		name:              "experimental",
		supportedFeatures: experimental.SupportedFeatures,
		newAllocator: func(ctx context.Context,
			features Features,
			allocateState AllocatedState,
			classLister DeviceClassLister,
			slices []*resourceapi.ResourceSlice,
			celCache *cel.Cache,
		) (Allocator, error) {
			// experimental 版本接收完整的 AllocatedState。
			return experimental.NewAllocator(ctx, features, allocateState, classLister, slices, celCache)
		},
		nodeMatches: experimental.NodeMatches,
	},
}

// NodeMatches 判断给定 Kubernetes 节点是否符合指定条件。
// 根据传入的 DRA 特性集合，选择 stable、incubating、experimental 中的一种实现来完成匹配。
func NodeMatches(features Features, node *v1.Node, nodeNameToMatch string, allNodesMatch bool, nodeSelector *v1.NodeSelector) (bool, error) {
	// 按优先级选择第一个支持当前特性集合的实现。
	for _, allocator := range availableAllocators {
		if allocator.supportedFeatures.Set().IsSuperset(features.Set()) {
			return allocator.nodeMatches(node, nodeNameToMatch, allNodesMatch, nodeSelector)
			// 找到支持的实现后委托其进行匹配。
		}
	}

	// 没有任何实现支持该特性集合时返回内部错误。
	return false, fmt.Errorf("internal error: no NodeMatches implementation available for feature set %v", features)
}

// IsDeviceAllocated 判断某个设备是否已提交分配状态，委托给 internal 包实现。
func IsDeviceAllocated(deviceID DeviceID, allocatedState *AllocatedState) bool {
	return internal.IsDeviceAllocated(deviceID, allocatedState)
}
