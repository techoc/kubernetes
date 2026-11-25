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

package stable

import (
	"context"
	"fmt"
	"slices"

	"github.com/go-logr/logr"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	draapi "k8s.io/dynamic-resource-allocation/api"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
)

// NodeMatches 判断给定节点是否满足切片或设备声明的节点选择条件。
// 参数按优先级依次为节点名、allNodes 与节点选择器；三者都未设置时返回 false。
// 返回值为是否匹配，以及解析节点选择器时可能产生的错误。
func NodeMatches(node *v1.Node, nodeNameToMatch string, allNodesMatch bool, nodeSelector *v1.NodeSelector) (bool, error) {
	// 按优先级依次判断三种节点选择方式。
	switch {
	// 指定节点名：要求节点存在且名称完全一致。
	case nodeNameToMatch != "":
		return node != nil && node.Name == nodeNameToMatch, nil
	// 声明匹配所有节点时直接返回 true。
	case allNodesMatch:
		return true, nil
	// 解析并检查节点选择器。
	case nodeSelector != nil:
		// 将 API 的 NodeSelector 转换为可匹配的选择器。
		selector, err := nodeaffinity.NewNodeSelector(nodeSelector)
		// 选择器非法时报告具体错误。
		if err != nil {
			return false, fmt.Errorf("failed to parse node selector %s: %w", nodeSelector.String(), err)
		}
		// 返回节点与选择器的匹配结果。
		return selector.Match(node), nil
	}

	// 未指定任何选择条件，视为不匹配。
	return false, nil
}

// GatherPools 收集当前节点可访问的全部资源池信息。
// 过时切片会被静默忽略；池可能因缺少切片而不完整，或因数据冲突而非法，两者都会记录在结果中。
func GatherPools(ctx context.Context, slices []*resourceapi.ResourceSlice, node *v1.Node, features Features) ([]*Pool, error) {
	// 按池 ID 聚合切片，后续再转换为内部表示。
	pools := make(map[PoolID][]*draapi.ResourceSlice)

	// 逐个处理输入切片。
	for _, slice := range slices {
		// 分区设备特性关闭时，忽略带逐设备节点选择或共享计数器的切片。
		if !features.PartitionableDevices && (slice.Spec.PerDeviceNodeSelection != nil || len(slice.Spec.SharedCounters) > 0) {
			continue
		}

		// 兼容组特性关闭时忽略相关切片，使所属池变为不完整，从而阻止分配该池设备。
		if !features.CompatibilityGroups && sliceUsesCompatibilityGroups(slice) {
			// 记录忽略原因，便于排查。
			klog.FromContext(ctx).V(5).Info("Ignoring resource slice because it uses compatibility groups while the DRADeviceCompatibilityGroups feature is disabled; its pool will be treated as incomplete", "resourceslice", klog.KObj(slice))
			continue
		}

		// 共享计数器切片对所有节点均可见，无条件加入池。
		if len(slice.Spec.SharedCounters) > 0 {
			if err := addSlice(pools, slice); err != nil {
				return nil, fmt.Errorf("failed to add node slice %s: %w", slice.Name, err)
			}
		} else if nodeName, allNodes := ptr.Deref(slice.Spec.NodeName, ""), ptr.Deref(slice.Spec.AllNodes, false); nodeName != "" || allNodes || slice.Spec.NodeSelector != nil {
			// 切片级别指定节点条件时，按切片条件匹配当前节点。
			match, err := NodeMatches(node, nodeName, allNodes, slice.Spec.NodeSelector)
			if err != nil {
				return nil, fmt.Errorf("failed to perform node selection for slice %s: %w", slice.Name, err)
			}
			// 匹配成功才把切片纳入池。
			if match {
				if err := addSlice(pools, slice); err != nil {
					return nil, fmt.Errorf("failed to add node slice %s: %w", slice.Name, err)
				}
			}
		} else if ptr.Deref(slice.Spec.PerDeviceNodeSelection, false) {
			// 逐设备节点选择模式下，只要有一个设备匹配当前节点就纳入切片。
			for _, device := range slice.Spec.Devices {
				// 用设备级别的节点选择条件进行判断。
				match, err := NodeMatches(node, ptr.Deref(device.NodeName, ""), ptr.Deref(device.AllNodes, false), device.NodeSelector)
				if err != nil {
					return nil, fmt.Errorf("failed to perform node selection for device %s in slice %s: %w",
						device.String(), slice.Name, err)
				}
				// 找到匹配设备后纳入切片。
				if match {
					if err := addSlice(pools, slice); err != nil {
						return nil, fmt.Errorf("failed to add node slice %s: %w", slice.Name, err)
					}
					// 同一切片只需加入一次。
					break
				}
			}
		} else {
			// 没有设置任何已知选择条件，视作未来扩展而忽略。
			// TODO（后续）：由调用方决定如何向用户报告此警告。
			continue
		}

	}

	// 遍历聚合后的池，识别不完整或过时的池并构造结果。
	result := make([]*Pool, 0, len(pools))
	for poolID, slicesForPool := range pools {
		// 切片数量与期望数量一致时认为池完整。
		isComplete := int64(len(slicesForPool)) == slicesForPool[0].Spec.Pool.ResourceSliceCount
		if isComplete {
			// 池完整，直接构造池对象。
			pool, err := buildPool(poolID, slicesForPool, features, nil)
			if err != nil {
				return nil, err
			}
			result = append(result, pool)
			continue
		}
		// 切片数不足时重新检查被节点选择过滤掉的切片，以区分真正不完整与数据过时。
		isObsolete, allSlicesForPool := checkSlicesInPool(slices, poolID, slicesForPool[0].Spec.Pool.Generation)
		// 发现更新代际说明驱动正在替换旧数据，忽略该池。
		if isObsolete {
			continue
		}
		// 与前面的忽略逻辑保持一致，避免不完整的计数影响完整性判断。
		if !features.CompatibilityGroups {
			allSlicesForPool = slicesWithoutCompatibilityGroups(allSlicesForPool)
		}
		// 用更完整的切片列表重新判定完整性，但返回给调用方的仍是节点相关切片。
		isComplete = int64(len(allSlicesForPool)) == slicesForPool[0].Spec.Pool.ResourceSliceCount
		// 不完整的池不允许分配，但需保留在结果中供 allocate all 模式判断。
		if !isComplete {
			result = append(result, &Pool{
				PoolID:       poolID,
				IsIncomplete: true,
			})
			continue
		}
		// 构造池对象，并传入完整切片列表用于校验计数器消费。
		pool, err := buildPool(poolID, slicesForPool, features, allSlicesForPool)
		if err != nil {
			return nil, err
		}
		result = append(result, pool)
	}

	// 返回所有池信息。
	return result, nil
}

// addSlice 将 v1 版本的 ResourceSlice 转换后加入对应资源池的切片列表。
// 比较 generation：更旧的切片被丢弃，更新的切片替换该池的全部旧切片，同代际则追加。
func addSlice(pools map[PoolID][]*draapi.ResourceSlice, s *resourceapi.ResourceSlice) error {
	// 转换为内部 API 表示。
	var slice draapi.ResourceSlice
	// 转换失败时返回带上下文的错误。
	if err := draapi.Convert_v1_ResourceSlice_To_api_ResourceSlice(s, &slice, nil); err != nil {
		return fmt.Errorf("convert ResourceSlice: %w", err)
	}

	// 由驱动名与池名构成池 ID。
	id := PoolID{Driver: slice.Spec.Driver, Pool: slice.Spec.Pool.Name}
	slicesForPool := pools[id]
	// 该池首次出现。
	if slicesForPool == nil {
		pools[id] = []*draapi.ResourceSlice{&slice}
		return nil
	}

	// 旧于当前代际，丢弃。
	if slice.Spec.Pool.Generation < slicesForPool[0].Spec.Pool.Generation {
		return nil
	}

	// 新于当前代际，替换该池的全部旧切片。
	if slice.Spec.Pool.Generation > slicesForPool[0].Spec.Pool.Generation {
		pools[id] = []*draapi.ResourceSlice{&slice}
		return nil
	}

	// 同代际切片追加到池中。
	slicesForPool = append(slicesForPool, &slice)
	pools[id] = slicesForPool
	return nil
}

// buildPool 根据同一池的切片构造 Pool 对象，并按需校验设备名与计数器。
// allSlicesForPool 是完整代际的切片列表，用于校验不面向当前节点但仍消费计数器的设备。
func buildPool(id PoolID, slices []*draapi.ResourceSlice, features Features, allSlicesForPool []*resourceapi.ResourceSlice) (*Pool, error) {
	// 按是否携带共享计数器对切片分组。
	var deviceSlices []*draapi.ResourceSlice
	var counterSetSlices []*draapi.ResourceSlice
	// 仅当分区设备特性开启时才区分计数器切片。
	if features.PartitionableDevices {
		// 按切片内容分流。
		for _, slice := range slices {
			if len(slice.Spec.SharedCounters) > 0 {
				counterSetSlices = append(counterSetSlices, slice)
			} else {
				deviceSlices = append(deviceSlices, slice)
			}
		}
	} else {
		deviceSlices = slices
	}
	// 设备名必须唯一，否则将池标记为无效。
	if err := validateDeviceNames(deviceSlices); err != nil {
		return &Pool{
			PoolID:        id,
			IsInvalid:     true,
			InvalidReason: err.Error(),
		}, nil
	}
	// 分区特性关闭时无需校验计数器，直接返回结果。
	if !features.PartitionableDevices {
		return &Pool{
			PoolID:                    id,
			DeviceSlicesTargetingNode: deviceSlices,
		}, nil
	}

	// 校验计数器集合的合法性，例如名称唯一。
	counterSets, err := getAndValidateCounterSets(counterSetSlices)
	if err != nil {
		return &Pool{
			PoolID:        id,
			IsInvalid:     true,
			InvalidReason: err.Error(),
		}, nil
	}

	// 校验设备引用的计数器集合与计数器确实存在。
	if err := validateDeviceCounterConsumption(counterSets, slices); err != nil {
		return &Pool{
			PoolID:        id,
			IsInvalid:     true,
			InvalidReason: err.Error(),
		}, nil
	}
	// 若已看到全部切片，无需再校验不面向节点的切片。
	if allSlicesForPool == nil || len(slices) == len(allSlicesForPool) {
		return &Pool{
			PoolID:                    id,
			DeviceSlicesTargetingNode: deviceSlices,
			CounterSets:               counterSets,
		}, nil
	}

	// 记录已转换切片的名称，避免重复转换。
	slicesTargetingNodeNames := sets.New[string]()
	for _, slice := range slices {
		slicesTargetingNodeNames.Insert(slice.Name)
	}
	// 保存不面向当前节点的切片的内部表示。
	var slicesNotTargetingNode []*draapi.ResourceSlice
	// 转换那些尚未转换过的切片。
	for _, slice := range allSlicesForPool {
		// 已转换过的切片跳过。
		if slicesTargetingNodeNames.Has(slice.Name) {
			continue
		}
		var convertedSlice draapi.ResourceSlice
		if err := draapi.Convert_v1_ResourceSlice_To_api_ResourceSlice(slice, &convertedSlice, nil); err != nil {
			return nil, fmt.Errorf("convert ResourceSlice: %w", err)
		}
		slicesNotTargetingNode = append(slicesNotTargetingNode, &convertedSlice)
	}
	// 这些切片的设备消费计数器必须合法；这里不校验设备名，与既有行为一致。
	if err := validateDeviceCounterConsumption(counterSets, slicesNotTargetingNode); err != nil {
		return &Pool{
			PoolID:        id,
			IsInvalid:     true,
			InvalidReason: err.Error(),
		}, nil
	}
	// 返回包含全部相关切片与计数器信息的池对象。
	return &Pool{
		PoolID:                       id,
		DeviceSlicesTargetingNode:    deviceSlices,
		DeviceSlicesNotTargetingNode: slicesNotTargetingNode,
		CounterSets:                  counterSets,
	}, nil
}

// getAndValidateCounterSets 从切片中提取共享计数器集合，并保证集合名称唯一。
// 只返回遇到的第一个错误。
func getAndValidateCounterSets(slices []*draapi.ResourceSlice) (map[draapi.UniqueString]*draapi.CounterSet, error) {
	// 以计数器集合名为键建立映射。
	counterSets := make(map[draapi.UniqueString]*draapi.CounterSet)
	// 遍历切片中的共享计数器。
	for _, slice := range slices {
		for i := range slice.Spec.SharedCounters {
			counterSet := slice.Spec.SharedCounters[i]
			// 名称重复则视为无效。
			if _, found := counterSets[counterSet.Name]; found {
				return nil, fmt.Errorf("duplicate counter set name %s", counterSet.Name)
			}
			// 记录首次出现的计数器集合。
			counterSets[counterSet.Name] = &counterSet
		}
	}
	return counterSets, nil
}

// validateDeviceNames 校验同一资源池内设备名称是否唯一。
func validateDeviceNames(resourceSlices []*draapi.ResourceSlice) error {
	// 用集合记录已出现的设备名。
	deviceNames := sets.New[draapi.UniqueString]()
	for _, slice := range resourceSlices {
		for _, device := range slice.Spec.Devices {
			// 发现重复名称立即报错。
			if deviceNames.Has(device.Name) {
				return fmt.Errorf("duplicate device name %s", device.Name)
			}
			// 记录新出现的设备名。
			deviceNames.Insert(device.Name)
		}
	}
	return nil
}

// validateDeviceCounterConsumption 校验设备消费的计数器集合与计数器都真实存在。
func validateDeviceCounterConsumption(counterSets map[draapi.UniqueString]*draapi.CounterSet, resourceSlices []*draapi.ResourceSlice) error {
	// 遍历所有切片与设备。
	for _, slice := range resourceSlices {
		for _, device := range slice.Spec.Devices {
			for _, deviceCounterConsumption := range device.ConsumesCounters {
				// 查找被引用的计数器集合。
				counterSet, found := counterSets[deviceCounterConsumption.CounterSet]
				// 集合不存在，报错。
				if !found {
					return fmt.Errorf("counter set %s not found", deviceCounterConsumption.CounterSet)
				}

				// 继续检查集合内的具体计数器是否存在。
				for counterName := range deviceCounterConsumption.Counters {
					// 计数器不存在，报错。
					if _, found := counterSet.Counters[counterName]; !found {
						return fmt.Errorf("counter %s not found in counter set %s", counterName, counterSet.Name)
					}
				}
			}
		}
	}
	return nil
}

// checkSlicesInPool 全量扫描池中的切片，返回调用方关心的代际是否已过时以及该代际的全部切片。
// 当发现比目标代际更新的切片时，说明调用方遗漏了数据，应立即中止。
func checkSlicesInPool(slices []*resourceapi.ResourceSlice, poolID PoolID, generation int64) (bool, []*resourceapi.ResourceSlice) {
	// 缓存索引可提升效率。
	var allSlicesForPool []*resourceapi.ResourceSlice
	// 扫描全部切片，按驱动与池名过滤。
	for i := range slices {
		slice := slices[i]
		// 不属于目标池的切片跳过。
		if slice.Spec.Driver != poolID.Driver.String() ||
			slice.Spec.Pool.Name != poolID.Pool.String() {
			continue
		}
		// 按代际与目标代际的关系分类。
		switch {
		// 代际一致，纳入结果。
		case slice.Spec.Pool.Generation == generation:
			allSlicesForPool = append(allSlicesForPool, slice)
		// 发现更新代际，说明数据不一致，中止检查。
		case slice.Spec.Pool.Generation > generation:
			return true, nil
		// 更旧的代际，忽略。
		default:
		}
	}
	// 未发现更新代际，返回该代际的全部切片。
	return false, allSlicesForPool
}

// Pool 描述一个资源池的汇总信息，包括完整性、合法性以及相关切片与计数器集合。
type Pool struct {
	// 嵌入的 PoolID 为池标识，包含驱动名与池名。
	// IsIncomplete 标记池是否缺少部分切片。
	// IsInvalid 标记池是否因数据非法而不可用。
	// InvalidReason 记录池被判定为非法的原因。
	// DeviceSlicesTargetingNode 为面向当前节点的设备切片。
	// DeviceSlicesNotTargetingNode 为不面向当前节点但仍需用于计数器校验的切片。
	// CounterSets 为池中定义的共享计数器集合。
	PoolID
	IsIncomplete                 bool
	IsInvalid                    bool
	InvalidReason                string
	DeviceSlicesTargetingNode    []*draapi.ResourceSlice
	DeviceSlicesNotTargetingNode []*draapi.ResourceSlice
	CounterSets                  map[draapi.UniqueString]*draapi.CounterSet
}

// PoolID 由驱动名与池名唯一标识一个资源池。
type PoolID struct {
	// Driver 为驱动名称，Pool 为资源池名称。
	Driver, Pool draapi.UniqueString
}

// String 返回「驱动名/池名」形式的字符串表示。
func (p PoolID) String() string {
	return p.Driver.String() + "/" + p.Pool.String()
}

// maxDevicesLevel6 限制 V(6) 日志中打印的设备数量，避免日志过大。
const maxDevicesLevel6 = 10

// logPools 返回一个用于结构化日志的值句柄，按日志详细级别输出不同粒度的池信息。
func logPools(logger klog.Logger, pools []*Pool) any {
	// 在此检查详细级别，因为调用方位置对 -vmodule 有意义。
	helper, logger := logger.WithCallStackHelper()
	helper()

	// 默认详细级别为 5。
	verbosity := 5
	// V<=5 输出固定内容，6 增加摘要，7 输出完整内容；从高到低寻找已启用的级别。
	for i := 7; i > verbosity; i-- {
		if loggerV := logger.V(i); loggerV.Enabled() {
			verbosity = i
			break
		}
	}
	// 用确定的详细级别包装池列表。
	return &poolsLogger{verbosity, pools}
}

// poolsLogger 按详细级别延迟生成资源池的日志内容。
type poolsLogger struct {
	// verbosity 为选定的日志详细级别。
	// pools 为待输出的资源池列表。
	verbosity int
	pools     []*Pool
}

// 确保 poolsLogger 实现 logr.Marshaler，以便结构化日志按自定义格式输出。
var _ logr.Marshaler = &poolsLogger{}

// MarshalLog 根据详细级别生成池信息的日志表示：6 级含摘要与受限设备列表，7 级输出完整内容。
func (p *poolsLogger) MarshalLog() any {
	// 始终包含池数量。
	info := map[string]any{"count": len(p.pools)}
	// 6 级附加每个池的元信息与受限的设备列表。
	if p.verbosity == 6 {
		meta := make([]map[string]any, len(p.pools))
		for i, pool := range p.pools {
			meta[i] = map[string]any{
				"id":            pool.PoolID.String(),
				"isIncomplete":  pool.IsIncomplete,
				"isInvalid":     pool.IsInvalid,
				"InvalidReason": pool.InvalidReason,
			}
		}
		info["meta"] = meta
		info["devices"] = p.listDevices(maxDevicesLevel6)
	}
	// 7 级输出全部设备与完整池内容。
	if p.verbosity >= 7 {
		info["devices"] = p.listDevices(-1)
		info["content"] = p.pools
	}
	return info
}

// listDevices 汇总所有池中的设备 ID 字符串，maxDevices 为 -1 时不限制数量。
func (p *poolsLogger) listDevices(maxDevices int) []string {
	// 按池顺序收集设备。
	var devices []string
	for _, pool := range p.pools {
		// 先收集面向当前节点的切片，再收集其余切片。
		devices = p.addDevicesInSlices(devices, pool.PoolID, pool.DeviceSlicesTargetingNode, maxDevices)
		devices = p.addDevicesInSlices(devices, pool.PoolID, pool.DeviceSlicesNotTargetingNode, maxDevices)
	}
	return devices
}

// addDevicesInSlices 将给定切片中的设备 ID 追加到结果列表，达到上限后以省略号代替。
func (p *poolsLogger) addDevicesInSlices(devices []string, poolID PoolID, slices []*draapi.ResourceSlice, maxDevices int) []string {
	// 按顺序遍历切片与设备。
	for _, slice := range slices {
		for _, device := range slice.Spec.Devices {
			// 达到上限时用省略号标记截断。
			if maxDevices != -1 && len(devices) >= maxDevices {
				devices = append(devices, "...")
				return devices
			}
			// 生成完整的设备 ID 字符串。
			devices = append(devices, DeviceID{Driver: poolID.Driver, Pool: poolID.Pool, Device: device.Name}.String())
		}
	}
	return devices
}

// logAllocatedDevices 返回一个用于结构化日志的值句柄，按详细级别输出已分配设备信息。
func logAllocatedDevices(logger klog.Logger, allocatedDevices sets.Set[DeviceID]) any {
	// 检查详细级别，保留调用方位置信息。
	helper, logger := logger.WithCallStackHelper()
	helper()

	// 默认详细级别为 5；V<=5 仅输出数量，6 级起输出设备 ID。
	verbosity := 5
	// 从 7 级向下寻找已启用的级别。
	for i := 7; i > verbosity; i-- {
		if loggerV := logger.V(i); loggerV.Enabled() {
			verbosity = i
			break
		}
	}

	// 用确定的级别包装已分配设备集合。
	return &allocatedDevicesLogger{verbosity, allocatedDevices}
}

// allocatedDevicesLogger 按详细级别延迟生成已分配设备的日志内容。
type allocatedDevicesLogger struct {
	// verbosity 为选定的日志详细级别。
	// devices 为已分配设备的 ID 集合。
	verbosity int
	devices   sets.Set[DeviceID]
}

// 确保 allocatedDevicesLogger 实现 logr.Marshaler。
var _ logr.Marshaler = &allocatedDevicesLogger{}

// MarshalLog 生成已分配设备的日志表示：始终包含数量，6 级起包含设备 ID 列表。
func (a *allocatedDevicesLogger) MarshalLog() any {
	// 始终记录设备数量。
	info := map[string]any{"count": len(a.devices)}
	// 6 级及以上输出设备 ID。
	if a.verbosity >= 6 {
		// 预分配切片容量。
		ids := make([]string, 0, len(a.devices))
		// 遍历集合中的设备 ID。
		for id := range a.devices {
			// 6 级时限制数量，超出则用省略号并停止。
			if a.verbosity == 6 && len(ids) >= maxDevicesLevel6 {
				ids = append(ids, "...")
				break
			}
			// 记录设备 ID 字符串。
			ids = append(ids, id.String())
		}
		// 排序以保证输出稳定。
		slices.Sort(ids)
		info["devices"] = ids

	}
	return info
}
