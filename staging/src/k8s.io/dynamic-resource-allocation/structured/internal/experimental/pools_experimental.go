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
	"cmp"
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

// NodeMatches 判断给定节点是否匹配三种节点选择方式之一：按节点名精确匹配、匹配所有节点、
// 或按 NodeSelector 匹配。优先级依次为 nodeNameToMatch、allNodesMatch、nodeSelector；
// 三者都未设置时返回 false。解析 NodeSelector 失败时返回错误。
func NodeMatches(node *v1.Node, nodeNameToMatch string, allNodesMatch bool, nodeSelector *v1.NodeSelector) (bool, error) {
	// 按优先级依次判断三种选择方式。
	switch {
	// 指定了节点名：节点非 nil 且名字相等才匹配。
	case nodeNameToMatch != "":
		return node != nil && node.Name == nodeNameToMatch, nil
	// 匹配所有节点：直接返回 true。
	case allNodesMatch:
		return true, nil
	// 指定了 NodeSelector：先解析再匹配节点；解析失败返回包装后的错误。
	case nodeSelector != nil:
		selector, err := nodeaffinity.NewNodeSelector(nodeSelector)
		if err != nil {
			return false, fmt.Errorf("failed to parse node selector %s: %w", nodeSelector.String(), err)
		}
		return selector.Match(node), nil
	}

	// 没有任何选择条件：不匹配。
	return false, nil
}

// GatherPools 收集所有能从给定节点访问的资源池信息。过时的切片会被静默忽略；池可能不完整
// （缺少部分所需切片）或无效（如设备名重复），这两种情况都会记录在返回的 Pool 结果中。
// slicesForNode 是与节点相关的切片，allSlices 是全量切片，用于补全不完整的池。
func GatherPools(ctx context.Context, slicesForNode []*resourceapi.ResourceSlice, node *v1.Node, features Features, allSlices []*resourceapi.ResourceSlice) ([]*Pool, error) {
	// 先按 PoolID 把切片分组，稍后逐池判定完整性并构建 Pool。
	pools := make(map[PoolID][]*draapi.ResourceSlice)

	for _, slice := range slicesForNode {
		// 可分区设备特性关闭时，忽略依赖该特性的切片（按设备选择节点或声明共享计数器）。
		if !features.PartitionableDevices && (slice.Spec.PerDeviceNodeSelection != nil || len(slice.Spec.SharedCounters) > 0) {
			continue
		}

		// 兼容组特性关闭时，忽略声明了兼容组的切片；这会使所在的池不完整，从而禁止分配该池设备。
		if !features.CompatibilityGroups && sliceUsesCompatibilityGroups(slice) {
			klog.FromContext(ctx).V(5).Info("Ignoring resource slice because it uses compatibility groups while the DRADeviceCompatibilityGroups feature is disabled; its pool will be treated as incomplete", "resourceslice", klog.KObj(slice))
			continue
		}

		// 判定该切片是否与当前节点相关。
		relevant := false
		// 含共享计数器的切片可能因不指向本节点而被排除；若本节点设备引用了其中的计数器，
		// 后面的完整性检查会发现缺失，并通过 checkSlicesInPool 取回全池切片，确保补全所需计数器。
		// 切片级节点选择：NodeName / AllNodes / NodeSelector 任一设置即按切片整体判定。
		if nodeName, allNodes := ptr.Deref(slice.Spec.NodeName, ""), ptr.Deref(slice.Spec.AllNodes, false); nodeName != "" || allNodes || slice.Spec.NodeSelector != nil {
			match, err := NodeMatches(node, nodeName, allNodes, slice.Spec.NodeSelector)
			// 节点选择解析失败：中止并返回带切片名的错误。
			if err != nil {
				return nil, fmt.Errorf("failed to perform node selection for slice %s: %w", slice.Name, err)
			}
			relevant = match
			// 切片未指定节点选择，但启用了按设备选择节点：逐设备判定，任一匹配即视为相关。
		} else if ptr.Deref(slice.Spec.PerDeviceNodeSelection, false) {
			for _, device := range slice.Spec.Devices {
				match, err := NodeMatches(node, ptr.Deref(device.NodeName, ""), ptr.Deref(device.AllNodes, false), device.NodeSelector)
				// 设备节点选择解析失败：中止并返回带设备与切片名的错误。
				if err != nil {
					return nil, fmt.Errorf("failed to perform node selection for device %s in slice %s: %w",
						device.String(), slice.Name, err)
				}
				// 任一设备匹配节点即认为该切片相关，随后跳出循环。
				if match {
					relevant = true
					break
				}
			}
		} else {
			// TODO（后续）：由调用方决定如何向用户报告此警告。
			continue
		}

		// 相关切片按池分组加入，转换或版本处理失败则中止。
		if relevant {
			if err := addSlice(pools, slice); err != nil {
				return nil, fmt.Errorf("failed to add node slice %s: %w", slice.Name, err)
			}
		}
	}

	// 找出不完整的池：此时只包含与节点相关的切片，且至少有一个。
	// 注意：更高版本但与节点无关的切片可能已被跳过，因此完整性检查需谨慎处理。
	result := make([]*Pool, 0, len(pools))
	var resultWithBindingConditions []*Pool
	for poolID, slicesForPool := range pools {
		// 切片数量达到声明的 ResourceSliceCount 即认为池完整，直接构建。
		isComplete := int64(len(slicesForPool)) == slicesForPool[0].Spec.Pool.ResourceSliceCount
		if isComplete {
			pool, err := buildPool(poolID, slicesForPool, features, nil)
			if err != nil {
				return nil, err
			}
			// 带绑定条件的池延后追加，排在其他池之后。
			if poolHasBindingConditions(*pool) {
				resultWithBindingConditions = append(resultWithBindingConditions, pool)
				continue
			}
			result = append(result, pool)
			continue
		}
		// 在全量切片中重新查找该池：判断是否已过时，并取回该代的所有切片。
		isObsolete, allSlicesForPool := checkSlicesInPool(allSlices, poolID, slicesForPool[0].Spec.Pool.Generation)
		if isObsolete {
			// 当前代已被驱动替换，忽略旧的池设备信息。
			continue
		}
		// 与前面的过滤保持一致：特性关闭时，带兼容组的切片不计入完整性。
		if !features.CompatibilityGroups {
			allSlicesForPool = slicesWithoutCompatibilityGroups(allSlicesForPool)
		}
		// 用全池切片数量判断完整性，但构建池时仍只返回节点相关设备，避免调用方重复检查节点选择器。
		isComplete = int64(len(allSlicesForPool)) == slicesForPool[0].Spec.Pool.ResourceSliceCount
		// 池不完整：无需校验，记录一个 IsIncomplete 的 Pool，防止 allocate-all 遗漏设备。
		if !isComplete {
			result = append(result, &Pool{
				PoolID:       poolID,
				IsIncomplete: true,
			})
			continue
		}
		// 池完整且未过时：构建池，allSlicesForPool 用于纳入不针对当前节点的共享计数器切片。
		pool, err := buildPool(poolID, slicesForPool, features, allSlicesForPool)
		if err != nil {
			return nil, err
		}
		if poolHasBindingConditions(*pool) {
			resultWithBindingConditions = append(resultWithBindingConditions, pool)
			continue
		}
		result = append(result, pool)
	}

	// 按 PoolID 排序保证分配顺序确定；first-fit 搜索让驱动可通过命名影响优先级。
	sortPoolsByID(result)
	sortPoolsByID(resultWithBindingConditions)

	// 带绑定条件的池统一追加到末尾。
	if len(resultWithBindingConditions) != 0 {
		result = append(result, resultWithBindingConditions...)
	}

	return result, nil
}

// sortSlicesByName 按切片名称升序排序，保证切片处理顺序确定，便于驱动通过命名影响优先级。
func sortSlicesByName(slicesToSort []*draapi.ResourceSlice) {
	slices.SortFunc(slicesToSort, func(a, b *draapi.ResourceSlice) int {
		return cmp.Compare(a.Name, b.Name)
	})
}

// sortPoolsByID 先按驱动名、再按池名对 Pool 切片排序，确保分配顺序确定。
func sortPoolsByID(pools []*Pool) {
	slices.SortFunc(pools, func(a, b *Pool) int {
		// 先比较驱动名，不同则直接返回结果。
		if cmp := cmp.Compare(a.PoolID.Driver.String(), b.PoolID.Driver.String()); cmp != 0 {
			return cmp
		}
		// 驱动名相同再比较池名。
		return cmp.Compare(a.PoolID.Pool.String(), b.PoolID.Pool.String())
	})
}

// addSlice 把 v1 ResourceSlice 转换后加入按 PoolID 分组的 map。若到来的是过时代则忽略；
// 若是更新代则替换该池所有旧切片；同代则追加。
func addSlice(pools map[PoolID][]*draapi.ResourceSlice, s *resourceapi.ResourceSlice) error {
	// 先把外部 v1 类型转换为内部 DRA API 类型，转换失败返回包装错误。
	var slice draapi.ResourceSlice
	if err := draapi.Convert_v1_ResourceSlice_To_api_ResourceSlice(s, &slice, nil); err != nil {
		return fmt.Errorf("convert ResourceSlice: %w", err)
	}

	// 由驱动名与池名组成 PoolID。
	id := PoolID{Driver: slice.Spec.Driver, Pool: slice.Spec.Pool.Name}
	slicesForPool := pools[id]
	if slicesForPool == nil {
		// 首次见到该池：新建分组。
		pools[id] = []*draapi.ResourceSlice{&slice}
		return nil
	}

	// 版本低于已有切片：过时，直接忽略。
	if slice.Spec.Pool.Generation < slicesForPool[0].Spec.Pool.Generation {
		return nil
	}

	// 版本更高：新代替换该池所有旧切片。
	if slice.Spec.Pool.Generation > slicesForPool[0].Spec.Pool.Generation {
		pools[id] = []*draapi.ResourceSlice{&slice}
		return nil
	}

	// 同代：追加到该池切片列表。
	slicesForPool = append(slicesForPool, &slice)
	pools[id] = slicesForPool
	return nil
}

// buildPool 根据已转换的相关切片构建 Pool：先按名称排序，再把切片分为设备切片与共享计数器切片，
// 补充不针对当前节点但被设备引用的切片，并依次校验设备名、计数器集合及计数器消费。校验失败时
// 返回 IsInvalid 的 Pool 而非错误。allSlicesForPool 用于补全不针对当前节点的切片。
func buildPool(id PoolID, slices []*draapi.ResourceSlice, features Features, allSlicesForPool []*resourceapi.ResourceSlice) (*Pool, error) {
	// 按名称排序保证 first-fit 分配顺序确定。
	sortSlicesByName(slices)

	// 分类收集：设备切片、共享计数器切片、以及不针对当前节点的切片。
	var deviceSlices []*draapi.ResourceSlice
	var counterSetSlices []*draapi.ResourceSlice
	var slicesNotTargetingNode []*draapi.ResourceSlice

	for _, slice := range slices {
		// 特性开启且声明了共享计数器时归入计数器切片，否则视为设备切片。
		if features.PartitionableDevices && len(slice.Spec.SharedCounters) > 0 {
			counterSetSlices = append(counterSetSlices, slice)
		} else {
			deviceSlices = append(deviceSlices, slice)
		}
	}

	// 存在未转换的切片时，补进去：这些切片虽不针对当前节点，但可能被本节点设备引用的计数器需要。
	if len(slices) < len(allSlicesForPool) {
		// 用名称集合去重，避免重复转换已处理的切片。
		slicesTargetingNodeNames := sets.New[string]()
		for _, slice := range slices {
			slicesTargetingNodeNames.Insert(slice.Name)
		}
		for _, slice := range allSlicesForPool {
			// 已转换过的切片跳过。
			if slicesTargetingNodeNames.Has(slice.Name) {
				continue
			}
			// 转换补充切片；失败则中止。
			var convertedSlice draapi.ResourceSlice
			if err := draapi.Convert_v1_ResourceSlice_To_api_ResourceSlice(slice, &convertedSlice, nil); err != nil {
				return nil, fmt.Errorf("convert ResourceSlice: %w", err)
			}
			// 同样按是否含共享计数器分类：计数器切片用于引用校验，其余归入不针对节点的设备切片。
			if features.PartitionableDevices && len(convertedSlice.Spec.SharedCounters) > 0 {
				counterSetSlices = append(counterSetSlices, &convertedSlice)
			} else {
				slicesNotTargetingNode = append(slicesNotTargetingNode, &convertedSlice)
			}
		}
	}

	// 校验设备名唯一性；失败返回标记 IsInvalid 的 Pool。
	if err := validateDeviceNames(deviceSlices); err != nil {
		return &Pool{
			PoolID:        id,
			IsInvalid:     true,
			InvalidReason: err.Error(),
		}, nil
	}

	// 可分区设备特性关闭：无需校验计数器，直接返回仅含针对节点设备切片的 Pool。
	if !features.PartitionableDevices {
		return &Pool{
			PoolID:                    id,
			DeviceSlicesTargetingNode: deviceSlices,
		}, nil
	}

	// 收集并校验计数器集合（名称唯一）。
	counterSets, err := getAndValidateCounterSets(counterSetSlices)
	if err != nil {
		return &Pool{
			PoolID:        id,
			IsInvalid:     true,
			InvalidReason: err.Error(),
		}, nil
	}

	// 校验针对当前节点的设备：其消费的计数器集合与计数器必须存在。
	if err := validateDeviceCounterConsumption(counterSets, deviceSlices); err != nil {
		return &Pool{
			PoolID:        id,
			IsInvalid:     true,
			InvalidReason: err.Error(),
		}, nil
	}

	// 同样校验不针对当前节点的设备，保证其计数器引用有效。
	if err := validateDeviceCounterConsumption(counterSets, slicesNotTargetingNode); err != nil {
		return &Pool{
			PoolID:        id,
			IsInvalid:     true,
			InvalidReason: err.Error(),
		}, nil
	}

	// 全部校验通过：返回完整的 Pool。
	return &Pool{
		PoolID:                       id,
		DeviceSlicesTargetingNode:    deviceSlices,
		DeviceSlicesNotTargetingNode: slicesNotTargetingNode,
		CounterSets:                  counterSets,
	}, nil
}

// getAndValidateCounterSets 从所有计数器切片中收集共享计数器集合，并以名称为键建索引。
// 发现重复的计数器集合名称时立即返回错误（只报告第一个错误）。
func getAndValidateCounterSets(slices []*draapi.ResourceSlice) (map[draapi.UniqueString]*draapi.CounterSet, error) {
	counterSets := make(map[draapi.UniqueString]*draapi.CounterSet)
	// 遍历所有共享计数器；名称重复视为无效。
	for _, slice := range slices {
		for i := range slice.Spec.SharedCounters {
			counterSet := slice.Spec.SharedCounters[i]
			// 重复名称：立即报错。
			if _, found := counterSets[counterSet.Name]; found {
				return nil, fmt.Errorf("duplicate counter set name %s", counterSet.Name)
			}
			counterSets[counterSet.Name] = &counterSet
		}
	}
	return counterSets, nil
}

// validateDeviceNames 校验给定切片中的设备名是否唯一；出现重名立即返回错误。
func validateDeviceNames(resourceSlices []*draapi.ResourceSlice) error {
	deviceNames := sets.New[draapi.UniqueString]()
	for _, slice := range resourceSlices {
		for _, device := range slice.Spec.Devices {
			// 设备名已出现：说明存在重复，报错。
			if deviceNames.Has(device.Name) {
				return fmt.Errorf("duplicate device name %s", device.Name)
			}
			// 首次出现：记录到已见集合。
			deviceNames.Insert(device.Name)
		}
	}
	return nil
}

// validateDeviceCounterConsumption 校验设备消费的计数器：其引用的计数器集合必须存在，且消费的
// 每个计数器都必须存在于该集合中。任一不满足即返回错误。
func validateDeviceCounterConsumption(counterSets map[draapi.UniqueString]*draapi.CounterSet, resourceSlices []*draapi.ResourceSlice) error {
	for _, slice := range resourceSlices {
		for _, device := range slice.Spec.Devices {
			// 逐条检查设备的计数器消费声明。
			for _, deviceCounterConsumption := range device.ConsumesCounters {
				// 引用的计数器集合不存在：报错。
				counterSet, found := counterSets[deviceCounterConsumption.CounterSet]
				if !found {
					return fmt.Errorf("counter set %s not found", deviceCounterConsumption.CounterSet)
				}

				// 消费的计数器名不在该集合中：报错。
				for counterName := range deviceCounterConsumption.Counters {
					if _, found := counterSet.Counters[counterName]; !found {
						return fmt.Errorf("counter %s not found in counter set %s", counterName, counterSet.Name)
					}
				}
			}
		}
	}
	return nil
}

// poolHasBindingConditions 报告池中是否有任何针对当前节点的设备声明了 BindingConditions。
// 带绑定条件的池会被 GatherPools 延后排序，避免优先分配。
func poolHasBindingConditions(pool Pool) bool {
	// 只检查针对当前节点的设备切片。
	for _, slice := range pool.DeviceSlicesTargetingNode {
		for _, device := range slice.Spec.Devices {
			// 任一设备声明了绑定条件即返回 true。
			if device.BindingConditions != nil {
				return true
			}
		}
	}
	return false
}

func checkSlicesInPool(slices []*resourceapi.ResourceSlice, poolID PoolID, generation int64) (bool, []*resourceapi.ResourceSlice) {
	// 全量扫描开销较大，未来可考虑按 PoolID 建索引缓存。
	var allSlicesForPool []*resourceapi.ResourceSlice
	for i := range slices {
		slice := slices[i]
		// 只关心属于指定池的切片。
		if slice.Spec.Driver != poolID.Driver.String() ||
			slice.Spec.Pool.Name != poolID.Pool.String() {
			continue
		}
		// 按代次处理：同代收集，更高代说明调用方遗漏了切片（当前代已过时），更老代忽略。
		switch {
		case slice.Spec.Pool.Generation == generation:
			allSlicesForPool = append(allSlicesForPool, slice)
		case slice.Spec.Pool.Generation > generation:
			// 存在更新代切片：判定当前代过时，返回 true。
			return true, nil
		default:
		}
	}
	// 未过时，返回该代的全部切片。
	return false, allSlicesForPool
}

// Pool 描述一个资源池的汇总信息：嵌入 PoolID（便于直接访问驱动名与池名）；IsIncomplete 表示
// 池切片不完整、不允许分配；IsInvalid 表示校验失败（如设备重名或计数器引用错误），InvalidReason
// 记录原因；DeviceSlicesTargetingNode 是目标节点可访问的设备切片，DeviceSlicesNotTargetingNode
// 是为计数器引用校验而保留的不针对当前节点的切片；CounterSets 以名称为键保存共享计数器集合。
type Pool struct {
	PoolID
	IsIncomplete                 bool
	IsInvalid                    bool
	InvalidReason                string
	DeviceSlicesTargetingNode    []*draapi.ResourceSlice
	DeviceSlicesNotTargetingNode []*draapi.ResourceSlice
	CounterSets                  map[draapi.UniqueString]*draapi.CounterSet
}

// PoolID 由驱动名和池名唯一标识一个资源池。
type PoolID struct {
	Driver, Pool draapi.UniqueString
}

// String 返回 PoolID 的可读形式 "驱动名/池名"。
func (p PoolID) String() string {
	return p.Driver.String() + "/" + p.Pool.String()
}

// maxDevicesLevel6 限制 V(6) 日志中最多输出 10 个设备，避免日志过大。
const maxDevicesLevel6 = 10

// logPools 返回一个可用于结构化日志的值句柄，根据日志详细级别决定输出多少池信息：
// V<=5 只输出数量，V=6 增加池元信息及有限数量的设备，V>=7 输出全部设备与池内容。
func logPools(logger klog.Logger, pools []*Pool) any {
	// 在此调整调用栈助手，保证日志中的源码位置指向调用方（-vmodule 需要）。
	helper, logger := logger.WithCallStackHelper()
	helper()

	// 从高到低探测启用的最大详细级别，决定输出内容。
	verbosity := 5
	for i := 7; i > verbosity; i-- {
		if loggerV := logger.V(i); loggerV.Enabled() {
			verbosity = i
			break
		}
	}
	return &poolsLogger{verbosity, pools}
}

// poolsLogger 实现 logr.Marshaler，按 verbosity 懒加载地生成池日志内容，
// 避免在低详细级别下付出序列化全部池的开销。verbosity 为选定的日志详细级别，
// pools 为待输出的设备池列表。
type poolsLogger struct {
	verbosity int
	pools     []*Pool
}

// 编译期断言：poolsLogger 实现 logr.Marshaler 接口。
var _ logr.Marshaler = &poolsLogger{}

// MarshalLog 按详细级别构造日志内容：始终包含池数量；V=6 附加每个池的元信息和最多
// maxDevicesLevel6 个设备；V>=7 附加全部设备与完整池内容。
func (p *poolsLogger) MarshalLog() any {
	info := map[string]any{"count": len(p.pools)}
	// V=6：输出每个池的元信息以及截断到 10 个设备的列表。
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
	// V>=7：输出全部设备和完整池内容（maxDevices=-1 表示不截断）。
	if p.verbosity >= 7 {
		info["devices"] = p.listDevices(-1)
		info["content"] = p.pools
	}
	return info
}

// listDevices 汇总所有池（含针对与不针对节点的两类切片）中的设备名；maxDevices 为 -1 时不截断。
func (p *poolsLogger) listDevices(maxDevices int) []string {
	var devices []string
	for _, pool := range p.pools {
		// 先收集针对节点的设备切片，再收集不针对节点的切片。
		devices = p.addDevicesInSlices(devices, pool.PoolID, pool.DeviceSlicesTargetingNode, maxDevices)
		devices = p.addDevicesInSlices(devices, pool.PoolID, pool.DeviceSlicesNotTargetingNode, maxDevices)
	}
	return devices
}

// addDevicesInSlices 把给定切片中的设备以 DeviceID 字符串追加到 devices；
// 当达到 maxDevices 上限（非负）时追加 "..." 并提前返回。
func (p *poolsLogger) addDevicesInSlices(devices []string, poolID PoolID, slices []*draapi.ResourceSlice, maxDevices int) []string {
	for _, slice := range slices {
		for _, device := range slice.Spec.Devices {
			// 达到输出上限：追加省略号并停止。
			if maxDevices != -1 && len(devices) >= maxDevices {
				devices = append(devices, "...")
				return devices
			}
			// 用池标识与设备名构造 DeviceID 字符串。
			devices = append(devices, DeviceID{Driver: poolID.Driver, Pool: poolID.Pool, Device: device.Name}.String())
		}
	}
	return devices
}
