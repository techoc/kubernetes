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

// Package incubating 提供动态资源分配中处于孵化阶段的资源池收集、校验与日志能力，
// 并定义池、池 ID 等核心数据结构。
package incubating

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

// NodeMatches 判断节点是否匹配给定的节点名、全部节点或节点选择器约束；三种约束都未设置时返回 false。
func NodeMatches(node *v1.Node, nodeNameToMatch string, allNodesMatch bool, nodeSelector *v1.NodeSelector) (bool, error) {
	switch {
	// 按节点名精确匹配：节点必须存在且名字一致。
	case nodeNameToMatch != "":
		return node != nil && node.Name == nodeNameToMatch, nil
	// 匹配所有节点。
	case allNodesMatch:
		return true, nil
	// 解析节点选择器后进行匹配。
	case nodeSelector != nil:
		selector, err := nodeaffinity.NewNodeSelector(nodeSelector)
		if err != nil {
			// 选择器解析失败时返回错误，由调用方包装上下文。
			return false, fmt.Errorf("failed to parse node selector %s: %w", nodeSelector.String(), err)
		}
		return selector.Match(node), nil
	}

	// 没有任何匹配约束时视为不匹配。
	return false, nil
}

// GatherPools 收集所有可从给定节点访问的设备资源池；过期 slice 被静默忽略，池可能不完整或非法，结果中记录这些状态。
// 带绑定条件的池会被放在结果末尾，普通池按池 ID 排序。
func GatherPools(ctx context.Context, slicesForNode []*resourceapi.ResourceSlice, node *v1.Node, features Features, allSlices []*resourceapi.ResourceSlice) ([]*Pool, error) {
	// 按池 ID 聚合与当前节点相关的 slice。
	pools := make(map[PoolID][]*draapi.ResourceSlice)

	// 遍历调用方给出的 slice，过滤出与本节点相关的部分。
	for _, slice := range slicesForNode {
		// 可分区设备特性未开启时，忽略使用共享计数器或逐设备节点选择的 slice。
		if !features.PartitionableDevices && (slice.Spec.PerDeviceNodeSelection != nil || len(slice.Spec.SharedCounters) > 0) {
			continue
		}

		// 兼容性组特性未开启时忽略声明了兼容性组的 slice，使其所在池被视为不完整，从而阻止分配该池的其他设备。
		if !features.CompatibilityGroups && sliceUsesCompatibilityGroups(slice) {
			// 记录忽略原因，便于诊断池为何不完整。
			klog.FromContext(ctx).V(5).Info("Ignoring resource slice because it uses compatibility groups while the DRADeviceCompatibilityGroups feature is disabled; its pool will be treated as incomplete", "resourceslice", klog.KObj(slice))
			continue
		}

		// relevant 标记该 slice 是否与当前节点相关。
		relevant := false
		// 含共享计数器的 slice 可能因不指向本节点而被排除；若本节点设备引用了其中的计数器，
		// 后面的完整性检查会发现缺失并借助 checkSlicesInPool 重新拉取全池 slice。
		// slice 级别设置了节点名/全部节点/节点选择器时，按 slice 维度做节点匹配。
		if nodeName, allNodes := ptr.Deref(slice.Spec.NodeName, ""), ptr.Deref(slice.Spec.AllNodes, false); nodeName != "" || allNodes || slice.Spec.NodeSelector != nil {
			match, err := NodeMatches(node, nodeName, allNodes, slice.Spec.NodeSelector)
			if err != nil {
				// 节点选择器解析失败，返回带 slice 名的错误。
				return nil, fmt.Errorf("failed to perform node selection for slice %s: %w", slice.Name, err)
			}
			relevant = match
			// 逐设备节点选择：任一设备匹配当前节点即认为 slice 相关。
		} else if ptr.Deref(slice.Spec.PerDeviceNodeSelection, false) {
			for _, device := range slice.Spec.Devices {
				// 按设备自身的节点选择字段做匹配。
				match, err := NodeMatches(node, ptr.Deref(device.NodeName, ""), ptr.Deref(device.AllNodes, false), device.NodeSelector)
				if err != nil {
					// 设备节点选择器解析失败，返回带设备与 slice 信息的错误。
					return nil, fmt.Errorf("failed to perform node selection for device %s in slice %s: %w",
						device.String(), slice.Name, err)
				}
				// 找到第一个匹配设备即可停止遍历。
				if match {
					relevant = true
					break
				}
			}
		} else {
			// TODO（后续）：由调用方决定如何向用户报告此警告。
			// 未设置任何已知的节点选择字段，可能是未来的未知扩展；跳过该 slice 但仍可尝试从其他池分配。
			continue
		}

		// 仅将相关 slice 按池聚合。
		if relevant {
			if err := addSlice(pools, slice); err != nil {
				// slice 转换或聚合失败，返回错误。
				return nil, fmt.Errorf("failed to add node slice %s: %w", slice.Name, err)
			}
		}
	}

	// 以下找出不完整的池：此时只保留与节点相关的 slice，且可能跳过了更高版本但对本节点无关的 slice，需谨慎判断完整性。
	result := make([]*Pool, 0, len(pools))
	// result 为普通池；带绑定条件的池单独收集，稍后追加到末尾。
	var resultWithBindingConditions []*Pool
	// 遍历每个池，判断完整性并构建。
	for poolID, slicesForPool := range pools {
		// slice 数量达到池声明的 ResourceSliceCount 即认为完整。
		isComplete := int64(len(slicesForPool)) == slicesForPool[0].Spec.Pool.ResourceSliceCount
		if isComplete {
			// 池完整：直接构建并校验。
			pool, err := buildPool(poolID, slicesForPool, features, nil)
			if err != nil {
				// 构建或校验失败，向上返回错误。
				return nil, err
			}
			// 带绑定条件的池放入单独列表，稍后追加到结果末尾。
			if poolHasBindingConditions(*pool) {
				resultWithBindingConditions = append(resultWithBindingConditions, pool)
				continue
			}
			// 普通池直接加入结果。
			result = append(result, pool)
			continue
		}
		// 池不完整时，需要重新审视此前因节点选择被过滤的 slice；allocate all 模式会拒绝不完整的池。
		isObsolete, allSlicesForPool := checkSlicesInPool(allSlices, poolID, slicesForPool[0].Spec.Pool.Generation)
		// 检查全池 slice，同时判断当前代际是否已过时。
		if isObsolete {
			// 驱动正在替换当前代际且新版没有面向本节点的设备，忽略旧池。
			continue
		}
		if !features.CompatibilityGroups {
			// 与前面的过滤保持一致：兼容性组特性关闭时相关 slice 不计入完整性。
			allSlicesForPool = slicesWithoutCompatibilityGroups(allSlicesForPool)
		}
		// 用更完整的 slice 数量判断完整性；返回给调用方的仍是与节点相关的子集，避免调用方重复检查节点选择器。
		// 按全池 slice 数量重新判定完整性。
		isComplete = int64(len(allSlicesForPool)) == slicesForPool[0].Spec.Pool.ResourceSliceCount
		// 不完整的池不允许分配，也无需校验；但仍需记录，以确保 allocate all 模式不会遗漏设备而误判成功。
		// 记录不完整池并跳过后续构建。
		if !isComplete {
			result = append(result, &Pool{
				PoolID:       poolID,
				IsIncomplete: true,
			})
			continue
		}
		// 池完整：结合全池 slice 构建。
		pool, err := buildPool(poolID, slicesForPool, features, allSlicesForPool)
		if err != nil {
			// 构建失败，向上返回错误。
			return nil, err
		}
		// 带绑定条件的池延后追加，避免改变普通池的优先顺序。
		if poolHasBindingConditions(*pool) {
			resultWithBindingConditions = append(resultWithBindingConditions, pool)
			continue
		}
		result = append(result, pool)
	}

	// 按池 ID 排序保证分配顺序确定；分配器采用首次适配，驱动可通过命名影响优先级。
	// 分别对两组池排序。
	sortPoolsByID(result)
	sortPoolsByID(resultWithBindingConditions)

	// 带绑定条件的池统一追加到结果末尾。
	if len(resultWithBindingConditions) != 0 {
		result = append(result, resultWithBindingConditions...)
	}

	return result, nil
}

// sortSlicesByName 按 slice 名称升序排序，使分配顺序确定。
func sortSlicesByName(slicesToSort []*draapi.ResourceSlice) {
	slices.SortFunc(slicesToSort, func(a, b *draapi.ResourceSlice) int {
		// 比较两个 slice 的名称。
		return cmp.Compare(a.Name, b.Name)
	})
}

// sortPoolsByID 先按驱动名、再按池名排序，保证池的遍历顺序确定。
func sortPoolsByID(pools []*Pool) {
	slices.SortFunc(pools, func(a, b *Pool) int {
		// 驱动名不同时先按驱动名排序。
		if cmp := cmp.Compare(a.PoolID.Driver.String(), b.PoolID.Driver.String()); cmp != 0 {
			return cmp
		}
		// 驱动相同时按池名排序。
		return cmp.Compare(a.PoolID.Pool.String(), b.PoolID.Pool.String())
	})
}

// addSlice 将 v1 ResourceSlice 转换后按池 ID 聚合：更旧代际被丢弃，更新代际会替换同池的全部旧 slice。
func addSlice(pools map[PoolID][]*draapi.ResourceSlice, s *resourceapi.ResourceSlice) error {
	// 将外部 v1 类型转换为内部 DRA API 类型。
	var slice draapi.ResourceSlice
	if err := draapi.Convert_v1_ResourceSlice_To_api_ResourceSlice(s, &slice, nil); err != nil {
		// 转换失败返回错误。
		return fmt.Errorf("convert ResourceSlice: %w", err)
	}

	// 用驱动名与池名构造池 ID。
	id := PoolID{Driver: slice.Spec.Driver, Pool: slice.Spec.Pool.Name}
	slicesForPool := pools[id]
	// 新池：直接放入该 slice。
	if slicesForPool == nil {
		pools[id] = []*draapi.ResourceSlice{&slice}
		return nil
	}

	// 代际更旧，属于过期数据，忽略。
	if slice.Spec.Pool.Generation < slicesForPool[0].Spec.Pool.Generation {
		return nil
	}

	// 代际更新，替换该池此前的全部 slice。
	if slice.Spec.Pool.Generation > slicesForPool[0].Spec.Pool.Generation {
		pools[id] = []*draapi.ResourceSlice{&slice}
		return nil
	}

	// 代际相同，追加到该池的 slice 列表。
	slicesForPool = append(slicesForPool, &slice)
	pools[id] = slicesForPool
	return nil
}

// buildPool 校验并构建池：先按名称排序 slice，区分设备 slice 与共享计数器 slice，转换未指向本节点的 slice，再校验设备名与计数器引用。
func buildPool(id PoolID, slices []*draapi.ResourceSlice, features Features, allSlicesForPool []*resourceapi.ResourceSlice) (*Pool, error) {
	// 按名称排序 slice，保证分配顺序确定。
	sortSlicesByName(slices)

	// 将 slice 分为设备 slice、共享计数器 slice，以及不指向当前节点的 slice。
	var deviceSlices []*draapi.ResourceSlice
	var counterSetSlices []*draapi.ResourceSlice
	var slicesNotTargetingNode []*draapi.ResourceSlice

	// 遍历池中 slice 进行分类。
	for _, slice := range slices {
		// 可分区特性开启时，含共享计数器的 slice 单独归类。
		if features.PartitionableDevices && len(slice.Spec.SharedCounters) > 0 {
			counterSetSlices = append(counterSetSlices, slice)
		} else {
			deviceSlices = append(deviceSlices, slice)
		}
	}

	// 处理此前因不指向当前节点而被排除的 slice。
	// 存在未转换的 slice 时才需要处理。
	if len(slices) < len(allSlicesForPool) {
		// 用名称集合快速识别已转换的 slice，避免重复处理。
		slicesTargetingNodeNames := sets.New[string]()
		// 记录已转换（指向本节点）的 slice 名称。
		for _, slice := range slices {
			slicesTargetingNodeNames.Insert(slice.Name)
		}
		// 遍历全池 slice，找出尚未转换的那些。
		for _, slice := range allSlicesForPool {
			// 已转换的 slice 跳过。
			if slicesTargetingNodeNames.Has(slice.Name) {
				continue
			}
			// 转换此前被排除的 slice。
			var convertedSlice draapi.ResourceSlice
			if err := draapi.Convert_v1_ResourceSlice_To_api_ResourceSlice(slice, &convertedSlice, nil); err != nil {
				// 转换失败返回错误。
				return nil, fmt.Errorf("convert ResourceSlice: %w", err)
			}
			// 共享计数器 slice 单独归类。
			if features.PartitionableDevices && len(convertedSlice.Spec.SharedCounters) > 0 {
				counterSetSlices = append(counterSetSlices, &convertedSlice)
			} else {
				// 记录不指向本节点的设备 slice，供后续计数器消费校验。
				slicesNotTargetingNode = append(slicesNotTargetingNode, &convertedSlice)
			}
		}
	}

	// 设备名必须唯一，重复则将池标记为非法。
	if err := validateDeviceNames(deviceSlices); err != nil {
		return &Pool{
			PoolID:        id,
			IsInvalid:     true,
			InvalidReason: err.Error(),
		}, nil
	}

	// 可分区特性未开启时无需校验计数器，构造只含本节点设备 slice 的池后返回。
	if !features.PartitionableDevices {
		return &Pool{
			PoolID:                    id,
			DeviceSlicesTargetingNode: deviceSlices,
		}, nil
	}

	// 汇总并校验共享计数器集合。
	counterSets, err := getAndValidateCounterSets(counterSetSlices)
	// 计数器集合名字重复等错误使池非法。
	if err != nil {
		return &Pool{
			PoolID:        id,
			IsInvalid:     true,
			InvalidReason: err.Error(),
		}, nil
	}

	// 校验指向本节点的设备所引用的计数器集合与计数器是否存在，不通过则将池标记为非法。
	if err := validateDeviceCounterConsumption(counterSets, deviceSlices); err != nil {
		return &Pool{
			PoolID:        id,
			IsInvalid:     true,
			InvalidReason: err.Error(),
		}, nil
	}

	// 同样校验不指向本节点的设备：它们也可能引用池内共享计数器。
	if err := validateDeviceCounterConsumption(counterSets, slicesNotTargetingNode); err != nil {
		return &Pool{
			PoolID:        id,
			IsInvalid:     true,
			InvalidReason: err.Error(),
		}, nil
	}

	// 校验通过：构造包含本节点/非本节点设备 slice 与计数器集合的完整池。
	return &Pool{
		PoolID:                       id,
		DeviceSlicesTargetingNode:    deviceSlices,
		DeviceSlicesNotTargetingNode: slicesNotTargetingNode,
		CounterSets:                  counterSets,
	}, nil
}

// getAndValidateCounterSets 将 slice 中的共享计数器集合按名称汇总，名称重复时返回错误。
func getAndValidateCounterSets(slices []*draapi.ResourceSlice) (map[draapi.UniqueString]*draapi.CounterSet, error) {
	// 按名称索引计数器集合。
	counterSets := make(map[draapi.UniqueString]*draapi.CounterSet)
	// 遇到第一个错误即返回，不做全量收集。
	for _, slice := range slices {
		// 遍历每个 slice 及其共享计数器。
		for i := range slice.Spec.SharedCounters {
			counterSet := slice.Spec.SharedCounters[i]
			if _, found := counterSets[counterSet.Name]; found {
				// 计数器集合名重复，池非法。
				return nil, fmt.Errorf("duplicate counter set name %s", counterSet.Name)
			}
			// 记录该计数器集合。
			counterSets[counterSet.Name] = &counterSet
		}
	}
	return counterSets, nil
}

// validateDeviceNames 检查池中所有设备名是否唯一。
func validateDeviceNames(resourceSlices []*draapi.ResourceSlice) error {
	// 用集合记录已出现的设备名。
	deviceNames := sets.New[draapi.UniqueString]()
	// 遍历所有 slice 中的设备。
	for _, slice := range resourceSlices {
		for _, device := range slice.Spec.Devices {
			// 发现重复设备名立即报错。
			if deviceNames.Has(device.Name) {
				return fmt.Errorf("duplicate device name %s", device.Name)
			}
			// 记录已出现的设备名。
			deviceNames.Insert(device.Name)
		}
	}
	return nil
}

// validateDeviceCounterConsumption 校验设备引用的计数器集合及其中的计数器确实存在。
func validateDeviceCounterConsumption(counterSets map[draapi.UniqueString]*draapi.CounterSet, resourceSlices []*draapi.ResourceSlice) error {
	// 遍历设备 slice。
	for _, slice := range resourceSlices {
		for _, device := range slice.Spec.Devices {
			// 逐个检查设备声明的计数器消费，确保引用的集合与计数器都存在。
			// 遍历设备的每一项计数器消费声明。
			for _, deviceCounterConsumption := range device.ConsumesCounters {
				// 查找被引用的计数器集合。
				counterSet, found := counterSets[deviceCounterConsumption.CounterSet]
				if !found {
					// 引用的计数器集合不存在。
					return fmt.Errorf("counter set %s not found", deviceCounterConsumption.CounterSet)
				}

				for counterName := range deviceCounterConsumption.Counters {
					// 检查声明消费的每个计数器是否属于该集合。
					if _, found := counterSet.Counters[counterName]; !found {
						// 引用的计数器在集合中不存在。
						return fmt.Errorf("counter %s not found in counter set %s", counterName, counterSet.Name)
					}
				}
			}
		}
	}
	return nil
}

// poolHasBindingConditions 判断池中是否存在任一设备声明了绑定条件。
func poolHasBindingConditions(pool Pool) bool {
	// 只检查指向本节点的设备 slice。
	for _, slice := range pool.DeviceSlicesTargetingNode {
		for _, device := range slice.Spec.Devices {
			// 发现绑定条件即返回 true。
			if device.BindingConditions != nil {
				return true
			}
		}
	}
	return false
}

// checkSlicesInPool 对池中所有 slice 做一次开销较高的扫描：判断调用方要推进的代际是否已过时，并返回该代际的全部 slice。
func checkSlicesInPool(slices []*resourceapi.ResourceSlice, poolID PoolID, generation int64) (bool, []*resourceapi.ResourceSlice) {
	// 按池 ID 建缓存索引可提升效率；未来支持始终需要考虑全部 slice 的特性时可能必需。
	var allSlicesForPool []*resourceapi.ResourceSlice
	// 收集与目标池、目标代际一致的 slice。
	for i := range slices {
		// 遍历全量 slice。
		slice := slices[i]
		if slice.Spec.Driver != poolID.Driver.String() ||
			slice.Spec.Pool.Name != poolID.Pool.String() {
			// 不属于目标池，跳过。
			continue
		}
		// 按代际与目标代际的关系分别处理。
		switch {
		case slice.Spec.Pool.Generation == generation:
			// 代际一致，收集该 slice。
			allSlicesForPool = append(allSlicesForPool, slice)
		case slice.Spec.Pool.Generation > generation:
			// 发现更高代际说明调用方漏看了同池的其他 slice，中止检查。
			return true, nil
		// 更旧代际忽略。
		default:
		}
	}
	// 当前代际未过时，返回该代际的全部 slice。
	return false, allSlicesForPool
}

// Pool 描述一个资源池的状态与内容。
//   - PoolID 是池的唯一标识（驱动名 + 池名）；
//   - IsIncomplete 标记池是否缺少部分 slice，不完整的池不允许分配；
//   - IsInvalid 标记池是否因设备名或计数器引用非法而不可用；
//   - InvalidReason 记录池被判非法的原因；
//   - DeviceSlicesTargetingNode 是面向当前节点的设备 slice；
//   - DeviceSlicesNotTargetingNode 是不面向当前节点、但可能引用池内共享计数器的设备 slice；
//   - CounterSets 是池内按名称索引的共享计数器集合。
type Pool struct {
	PoolID
	IsIncomplete                 bool
	IsInvalid                    bool
	InvalidReason                string
	DeviceSlicesTargetingNode    []*draapi.ResourceSlice
	DeviceSlicesNotTargetingNode []*draapi.ResourceSlice
	CounterSets                  map[draapi.UniqueString]*draapi.CounterSet
}

// PoolID 由驱动名与池名组成，唯一标识一个资源池。
//   - Driver 是资源驱动名；
//   - Pool 是驱动内的池名。
type PoolID struct {
	Driver, Pool draapi.UniqueString
}

// String 以“驱动名/池名”的形式返回池 ID 的字符串表示。
func (p PoolID) String() string {
	return p.Driver.String() + "/" + p.Pool.String()
}

// maxDevicesLevel6 限制 V(6) 日志输出的设备数量，避免日志膨胀；多数 E2E 测试 10 个已足够。
const maxDevicesLevel6 = 10

// logPools 返回一个结构化日志句柄，根据日志详细级别输出不同量的池信息。
func logPools(logger klog.Logger, pools []*Pool) any {
	// 在此检查详细级别，因为调用方的源码位置可能对 -vmodule 判定有意义。
	helper, logger := logger.WithCallStackHelper()
	// 使用调用栈助手，使日志位置指向真正的调用方。
	helper()

	// V<=5 输出固定内容，V=6 增加摘要，V=7 输出完整信息。
	verbosity := 5
	// 从 7 向下探测已启用的最高详细级别。
	for i := 7; i > verbosity; i-- {
		if loggerV := logger.V(i); loggerV.Enabled() {
			verbosity = i
			break
		}
	}
	// 按生效的详细级别构造日志句柄。
	return &poolsLogger{verbosity, pools}
}

// poolsLogger 是按详细级别惰性组织池信息的结构化日志对象。
//   - verbosity 是实际生效的日志详细级别；
//   - pools 是待输出的池列表。
type poolsLogger struct {
	verbosity int
	pools     []*Pool
}

// 编译期断言：poolsLogger 实现 logr.Marshaler 接口。
var _ logr.Marshaler = &poolsLogger{}

// MarshalLog 实现 logr.Marshaler，按详细级别生成要输出的字段映射。
func (p *poolsLogger) MarshalLog() any {
	// 任何级别都输出池的总数。
	info := map[string]any{"count": len(p.pools)}
	// V(6)：输出每个池的元信息及有限数量的设备。
	if p.verbosity == 6 {
		meta := make([]map[string]any, len(p.pools))
		// 收集每个池的 ID、完整性、合法性与非法原因。
		for i, pool := range p.pools {
			meta[i] = map[string]any{
				"id":            pool.PoolID.String(),
				"isIncomplete":  pool.IsIncomplete,
				"isInvalid":     pool.IsInvalid,
				"InvalidReason": pool.InvalidReason,
			}
		}
		info["meta"] = meta
		// 设备列表受 maxDevicesLevel6 限制。
		info["devices"] = p.listDevices(maxDevicesLevel6)
	}
	// V(7)：输出全部设备及池的完整内容。
	if p.verbosity >= 7 {
		info["devices"] = p.listDevices(-1)
		info["content"] = p.pools
	}
	return info
}

// listDevices 汇总所有池中指向与不指向当前节点的设备 ID。
func (p *poolsLogger) listDevices(maxDevices int) []string {
	var devices []string
	// 先收集指向本节点的设备，再收集不指向本节点的设备。
	for _, pool := range p.pools {
		devices = p.addDevicesInSlices(devices, pool.PoolID, pool.DeviceSlicesTargetingNode, maxDevices)
		devices = p.addDevicesInSlices(devices, pool.PoolID, pool.DeviceSlicesNotTargetingNode, maxDevices)
	}
	return devices
}

// addDevicesInSlices 将 slice 中的设备以“驱动/池/设备”形式的 ID 追加到结果，达到上限时用省略号标记截断。
func (p *poolsLogger) addDevicesInSlices(devices []string, poolID PoolID, slices []*draapi.ResourceSlice, maxDevices int) []string {
	for _, slice := range slices {
		// 遍历 slice 中的设备。
		for _, device := range slice.Spec.Devices {
			// 达到数量上限时追加省略号并停止。
			if maxDevices != -1 && len(devices) >= maxDevices {
				devices = append(devices, "...")
				return devices
			}
			// 记录设备的完整 ID。
			devices = append(devices, DeviceID{Driver: poolID.Driver, Pool: poolID.Pool, Device: device.Name}.String())
		}
	}
	return devices
}
