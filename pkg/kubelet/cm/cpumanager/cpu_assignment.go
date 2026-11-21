/*
Copyright 2017 The Kubernetes Authors.

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

// cpu_assignment.go 实现 CPU 的拓扑感知分配算法，包括紧凑（packed）与分散（distributed）两种策略。
package cpumanager

import (
	"fmt"
	"maps"
	"math"
	"sort"

	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager/topology"
	"k8s.io/utils/cpuset"
)

// LoopControl 控制 CPU 累加器组合遍历循环的行为。
type LoopControl int

// 循环控制结果：Continue 继续遍历，Break 提前退出。
const (
	Continue LoopControl = iota
	Break
)

// mapIntInt 是 int 到 int 的辅助映射类型，用于统计每个 NUMA 节点的可用 CPU 数。
type mapIntInt map[int]int

// Clone 返回映射的浅拷贝。
func (m mapIntInt) Clone() mapIntInt {
	return maps.Clone(m)
}

// Keys 返回所有键（顺序不保证）。
func (m mapIntInt) Keys() []int {
	var keys []int
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// Values 返回指定键的值；未指定键时返回全部值。
func (m mapIntInt) Values(keys ...int) []int {
	if keys == nil {
		keys = m.Keys()
	}
	var values []int
	for _, k := range keys {
		values = append(values, m[k])
	}
	return values
}

// sum 返回整数切片之和。
func sum(xs []int) int {
	var s int
	for _, x := range xs {
		s += x
	}
	return s
}

// mean 返回平均值，结果保留三位小数。
func mean(xs []int) float64 {
	var sum float64
	for _, x := range xs {
		sum += float64(x)
	}
	m := sum / float64(len(xs))
	return math.Round(m*1000) / 1000
}

// standardDeviation 返回总体标准差，结果保留三位小数。
func standardDeviation(xs []int) float64 {
	m := mean(xs)
	var sum float64
	for _, x := range xs {
		sum += (float64(x) - m) * (float64(x) - m)
	}
	s := math.Sqrt(sum / float64(len(xs)))
	return math.Round(s*1000) / 1000
}

// numaOrSocketsFirstFuncs 抽象“NUMA 优先”与“socket 优先”两种拓扑遍历顺序。
type numaOrSocketsFirstFuncs interface {
	takeFullFirstLevel()
	takeFullSecondLevel()
	sortAvailableNUMANodes() []int
	sortAvailableSockets() []int
	sortAvailableCores() []int
}

// numaFirst 表示 NUMA 节点在内存层级中高于 socket 的拓扑。
type numaFirst struct{ acc *cpuAccumulator }

// socketsFirst 表示 socket 在内存层级中高于 NUMA 节点的拓扑。
type socketsFirst struct{ acc *cpuAccumulator }

// 确保两种实现都满足接口。
var _ numaOrSocketsFirstFuncs = (*numaFirst)(nil)
var _ numaOrSocketsFirstFuncs = (*socketsFirst)(nil)

// NUMA 节点高于 socket 时，第一层优先整块取 NUMA 节点。
func (n *numaFirst) takeFullFirstLevel() {
	n.acc.takeFullNUMANodes()
}

// 第二层再整块取 socket。
func (n *numaFirst) takeFullSecondLevel() {
	n.acc.takeFullSockets()
}

// sortAvailableUncoreCaches 在每个 NUMA 节点内部对 UncoreCache 排序。
func (a *cpuAccumulator) sortAvailableUncoreCaches() []int {
	var result []int
	for _, numa := range a.sortAvailableNUMANodes() {
		uncore := a.details.UncoreInNUMANodes(numa).UnsortedList()
		a.sort(uncore, a.details.CPUsInUncoreCaches)
		result = append(result, uncore...)
	}
	return result
}

// NUMA 节点优先时直接对 NUMA 节点排序并返回。
func (n *numaFirst) sortAvailableNUMANodes() []int {
	numas := n.acc.details.NUMANodes().UnsortedList()
	n.acc.sort(numas, n.acc.details.CPUsInNUMANodes)
	return numas
}

// NUMA 节点优先时，从每个已排序的 NUMA 节点中取出 socket，拼接成全局顺序。
func (n *numaFirst) sortAvailableSockets() []int {
	var result []int
	for _, numa := range n.sortAvailableNUMANodes() {
		sockets := n.acc.details.SocketsInNUMANodes(numa).UnsortedList()
		n.acc.sort(sockets, n.acc.details.CPUsInSockets)
		result = append(result, sockets...)
	}
	return result
}

// NUMA 节点优先时，core 直接位于 socket 之下。
func (n *numaFirst) sortAvailableCores() []int {
	var result []int
	for _, socket := range n.acc.sortAvailableSockets() {
		cores := n.acc.details.CoresInSockets(socket).UnsortedList()
		n.acc.sort(cores, n.acc.details.CPUsInCores)
		result = append(result, cores...)
	}
	return result
}

// socket 优先时，第一层优先整块取 socket。
func (s *socketsFirst) takeFullFirstLevel() {
	s.acc.takeFullSockets()
}

// 第二层再整块取 NUMA 节点。
func (s *socketsFirst) takeFullSecondLevel() {
	s.acc.takeFullNUMANodes()
}

// socket 优先时，从每个已排序的 socket 中取出 NUMA 节点，拼接成全局顺序。
func (s *socketsFirst) sortAvailableNUMANodes() []int {
	var result []int
	for _, socket := range s.sortAvailableSockets() {
		numas := s.acc.details.NUMANodesInSockets(socket).UnsortedList()
		s.acc.sort(numas, s.acc.details.CPUsInNUMANodes)
		result = append(result, numas...)
	}
	return result
}

// socket 优先时直接对 socket 排序并返回。
func (s *socketsFirst) sortAvailableSockets() []int {
	sockets := s.acc.details.Sockets().UnsortedList()
	s.acc.sort(sockets, s.acc.details.CPUsInSockets)
	return sockets
}

// socket 优先时，core 直接位于 NUMA 节点之下。
func (s *socketsFirst) sortAvailableCores() []int {
	var result []int
	for _, numa := range s.acc.sortAvailableNUMANodes() {
		cores := s.acc.details.CoresInNUMANodes(numa).UnsortedList()
		s.acc.sort(cores, s.acc.details.CPUsInCores)
		result = append(result, cores...)
	}
	return result
}

// availableCPUSorter 抽象 CPU 排序策略（紧凑或分散）。
type availableCPUSorter interface {
	sort() []int
}

// sortCPUsPacked 使用紧凑排序：尽量填满整个核再使用下一个核。
type sortCPUsPacked struct{ acc *cpuAccumulator }

// sortCPUsSpread 使用分散排序：先在 socket 内按 NUMA 分散。
type sortCPUsSpread struct{ acc *cpuAccumulator }

// 确保两种排序实现都满足接口。
var _ availableCPUSorter = (*sortCPUsPacked)(nil)
var _ availableCPUSorter = (*sortCPUsSpread)(nil)

// sort 返回紧凑排序后的 CPU 列表。
func (s sortCPUsPacked) sort() []int {
	return s.acc.sortAvailableCPUsPacked()
}

// sort 返回分散排序后的 CPU 列表。
func (s sortCPUsSpread) sort() []int {
	return s.acc.sortAvailableCPUsSpread()
}

// CPUSortingStrategy 描述 socket 范围内的 CPU 排序方式。
// 以双 socket、6 核、开启超线程的 12 CPU 拓扑为例：
//
//	CPUDetails: map[int]topology.CPUInfo{
//		0:  {CoreID: 0, SocketID: 0, NUMANodeID: 0},
//		1:  {CoreID: 1, SocketID: 1, NUMANodeID: 1},
//		2:  {CoreID: 2, SocketID: 0, NUMANodeID: 0},
//		3:  {CoreID: 3, SocketID: 1, NUMANodeID: 1},
//		4:  {CoreID: 4, SocketID: 0, NUMANodeID: 0},
//		5:  {CoreID: 5, SocketID: 1, NUMANodeID: 1},
//		6:  {CoreID: 0, SocketID: 0, NUMANodeID: 0},
//		7:  {CoreID: 1, SocketID: 1, NUMANodeID: 1},
//		8:  {CoreID: 2, SocketID: 0, NUMANodeID: 0},
//		9:  {CoreID: 3, SocketID: 1, NUMANodeID: 1},
//		10: {CoreID: 4, SocketID: 0, NUMANodeID: 0},
//		11: {CoreID: 5, SocketID: 1, NUMANodeID: 1},
//	},
//
// - CPUSortingOptionPacked 按核分组后再使用下一个核，结果为：
// 0, 2, 4, 6, 8, 10, 1, 3, 5, 7, 9, 11
// - CPUSortingOptionSpread 先跨核分散再使用下一个 CPU，结果为：
// 0, 6, 2, 8, 4, 10, 1, 7, 3, 9, 5, 11
//
// 默认使用 Packed；只有用户显式设置 DistributeCPUsAcrossCores 策略选项时才使用 Spread。
type CPUSortingStrategy string

const (
	// 两种排序策略的取值。
	CPUSortingStrategyPacked CPUSortingStrategy = "packed"
	CPUSortingStrategySpread CPUSortingStrategy = "spread"
)

// cpuAccumulator 在拓扑约束下逐步累加 CPU，直到满足请求数量。
type cpuAccumulator struct {
	// logger 内部保存：cpuAccumulator 的生命周期完全包含在一次 takeByTopology 调用内，
	// 因此保存它与每次从调用中获取没有功能差异，同时还能减少 API 参数。
	logger klog.Logger

	// topo 描述 CPU（开启超线程时为线程）在核、NUMA 节点与 socket 之间的布局。
	// topo 不会被修改，仅作为原始拓扑的只读参考，用于查询各层级包含多少 CPU。
	topo *topology.CPUTopology

	// details 是累加器可认领的空闲 CPU 集合；CPU 被认领后会从中移除。
	details topology.CPUDetails

	// numCPUsNeeded 是仍需累加的 CPU 数：创建时等于请求总数，每认领一个减 1。
	// 减到 0 表示成功；仍大于 0 但已无可用 CPU 则表示失败。
	numCPUsNeeded int

	// result 是已累加的 CPU 集合；其大小等于请求总数时任务完成。
	result cpuset.CPUSet

	numaOrSocketsFirst numaOrSocketsFirstFuncs

	// availableCPUSorter 控制 CPU 排序结果：默认按 packed 排序，否则按 spread 排序。
	availableCPUSorter availableCPUSorter
}

// newCPUAccumulator 创建累加器，并根据拓扑层级与排序策略选择对应实现。
func newCPUAccumulator(logger klog.Logger, topo *topology.CPUTopology, availableCPUs cpuset.CPUSet, numCPUs int, cpuSortingStrategy CPUSortingStrategy) *cpuAccumulator {
	acc := &cpuAccumulator{
		logger:        logger,
		topo:          topo,
		details:       topo.CPUDetails.KeepOnly(availableCPUs),
		numCPUsNeeded: numCPUs,
		result:        cpuset.New(),
	}

	if topo.NumSockets >= topo.NumNUMANodes {
		acc.numaOrSocketsFirst = &numaFirst{acc}
	} else {
		acc.numaOrSocketsFirst = &socketsFirst{acc}
	}

	if cpuSortingStrategy == CPUSortingStrategyPacked {
		acc.availableCPUSorter = &sortCPUsPacked{acc}
	} else {
		acc.availableCPUSorter = &sortCPUsSpread{acc}
	}

	return acc
}

// isNUMANodeFree 判断 NUMA 节点是否完全空闲（其中所有 CPU 都可用）。
func (a *cpuAccumulator) isNUMANodeFree(numaID int) bool {
	return a.details.CPUsInNUMANodes(numaID).Size() == a.topo.CPUDetails.CPUsInNUMANodes(numaID).Size()
}

// isSocketFree 判断 socket 是否完全空闲。
func (a *cpuAccumulator) isSocketFree(socketID int) bool {
	return a.details.CPUsInSockets(socketID).Size() == a.topo.CPUsPerSocket()
}

// isUncoreCacheFree 判断 UncoreCache 是否完全空闲。
func (a *cpuAccumulator) isUncoreCacheFree(uncoreID int) bool {
	return a.details.CPUsInUncoreCaches(uncoreID).Size() == a.topo.CPUDetails.CPUsInUncoreCaches(uncoreID).Size()
}

// isCoreFree 判断物理核是否完全空闲。
func (a *cpuAccumulator) isCoreFree(coreID int) bool {
	return a.details.CPUsInCores(coreID).Size() == a.topo.CPUsPerCore()
}

// freeNUMANodes 返回完全空闲的 NUMA 节点，按 sortAvailableNUMANodes 排序。
func (a *cpuAccumulator) freeNUMANodes() []int {
	free := []int{}
	for _, numa := range a.sortAvailableNUMANodes() {
		if a.isNUMANodeFree(numa) {
			free = append(free, numa)
		}
	}
	return free
}

// freeSockets 返回完全空闲的 socket，按 sortAvailableSockets 排序。
func (a *cpuAccumulator) freeSockets() []int {
	free := []int{}
	for _, socket := range a.sortAvailableSockets() {
		if a.isSocketFree(socket) {
			free = append(free, socket)
		}
	}
	return free
}

// freeUncoreCache 返回完全空闲的 UncoreCache，按排序后的顺序返回。
func (a *cpuAccumulator) freeUncoreCache() []int {
	free := []int{}
	for _, uncore := range a.sortAvailableUncoreCaches() {
		if a.isUncoreCacheFree(uncore) {
			free = append(free, uncore)
		}
	}
	return free
}

// freeCores 返回完全空闲的物理核，按 sortAvailableCores 排序。
func (a *cpuAccumulator) freeCores() []int {
	free := []int{}
	for _, core := range a.sortAvailableCores() {
		if a.isCoreFree(core) {
			free = append(free, core)
		}
	}
	return free
}

// freeCPUs 返回空闲 CPU，按当前排序策略排序。
func (a *cpuAccumulator) freeCPUs() []int {
	return a.availableCPUSorter.sort()
}

// sort 按各元素包含的可用 CPU 数升序排序；数量相同时按 id 升序。
// getCPUs 用于获取指定元素下的 CPU 集合。
func (a *cpuAccumulator) sort(ids []int, getCPUs func(ids ...int) cpuset.CPUSet) {
	sort.Slice(ids,
		func(i, j int) bool {
			iCPUs := getCPUs(ids[i])
			jCPUs := getCPUs(ids[j])
			if iCPUs.Size() < jCPUs.Size() {
				return true
			}
			if iCPUs.Size() > jCPUs.Size() {
				return false
			}
			return ids[i] < ids[j]
		})
}

// sortAvailableNUMANodes 对所有含空闲 CPU 的 NUMA 节点排序。
//
// NUMA 节点高于 socket 时，按包含的空闲 CPU 数升序排列。
//
// NUMA 节点低于 socket 时：先按所属 socket 的空闲 CPU 数排序，再按节点自身空闲 CPU 数排序。
// 即：不同 socket 的节点，所属 socket 空闲 CPU 少者在前；同一 socket 内，空闲 CPU 少者在前。
func (a *cpuAccumulator) sortAvailableNUMANodes() []int {
	return a.numaOrSocketsFirst.sortAvailableNUMANodes()
}

// sortAvailableSockets 对所有含空闲 CPU 的 socket 排序。
//
// socket 高于 NUMA 节点时，按包含的空闲 CPU 数升序排列。
//
// socket 低于 NUMA 节点时：先按所属 NUMA 节点的空闲 CPU 数排序，再按自身空闲 CPU 数排序。
func (a *cpuAccumulator) sortAvailableSockets() []int {
	return a.numaOrSocketsFirst.sortAvailableSockets()
}

// sortAvailableCores 对所有含空闲 CPU 的物理核排序。
//
// socket 优先时：先按 socket 空闲 CPU 数、再按 NUMA 节点空闲 CPU 数、最后按核自身空闲 CPU 数升序排列。
// 即：不同 socket 的核，socket 空闲 CPU 少者在前；同 socket 不同 NUMA 节点，节点空闲 CPU 少者在前；
// 同 socket 同 NUMA 节点，核空闲 CPU 少者在前。
//
// NUMA 优先时排序逻辑相同，只是 NUMA 节点与 socket 的优先级互换。
func (a *cpuAccumulator) sortAvailableCores() []int {
	return a.numaOrSocketsFirst.sortAvailableCores()
}

// sortAvailableCPUsPacked 对全部空闲 CPU 做紧凑排序。
//
// 依次按 socket、NUMA 节点、核的空闲 CPU 数升序排列，最后按 CPU ID 升序。
// 即：不同 socket 的 CPU，socket 空闲 CPU 少者在前；同 socket 不同 NUMA 节点，节点空闲 CPU 少者在前；
// 同节点不同核，核空闲 CPU 少者在前；同核内按 CPU ID 升序。
//
// NUMA 优先时排序逻辑相同，只是 NUMA 节点与 socket 的优先级互换。
func (a *cpuAccumulator) sortAvailableCPUsPacked() []int {
	var result []int
	for _, core := range a.sortAvailableCores() {
		cpus := a.details.CPUsInCores(core).UnsortedList()
		sort.Ints(cpus)
		result = append(result, cpus...)
	}
	return result
}

// sortAvailableCPUsSpread 对全部可用 CPU 做分散排序：
// 先按 socket 排序，再在每个 socket 内直接按 CPU 排序。
func (a *cpuAccumulator) sortAvailableCPUsSpread() []int {
	var result []int
	for _, socket := range a.sortAvailableSockets() {
		cpus := a.details.CPUsInSockets(socket).UnsortedList()
		sort.Ints(cpus)
		result = append(result, cpus...)
	}
	return result
}

// take 认领一组 CPU：加入结果、从可用集合移除并减少待分配数量。
func (a *cpuAccumulator) take(cpus cpuset.CPUSet) {
	a.result = a.result.Union(cpus)
	a.details = a.details.KeepOnly(a.details.CPUs().Difference(a.result))
	a.numCPUsNeeded -= cpus.Size()
}

// takeFullNUMANodes 整块认领完全空闲且不超出剩余需求的 NUMA 节点。
func (a *cpuAccumulator) takeFullNUMANodes() {
	for _, numa := range a.freeNUMANodes() {
		cpusInNUMANode := a.topo.CPUDetails.CPUsInNUMANodes(numa)
		if !a.needsAtLeast(cpusInNUMANode.Size()) {
			continue
		}
		a.logger.V(4).Info("takeFullNUMANodes: claiming NUMA node", "numa", numa)
		a.take(cpusInNUMANode)
	}
}

// takeFullSockets 整块认领完全空闲且不超出剩余需求的 socket。
func (a *cpuAccumulator) takeFullSockets() {
	for _, socket := range a.freeSockets() {
		cpusInSocket := a.topo.CPUDetails.CPUsInSockets(socket)
		if !a.needsAtLeast(cpusInSocket.Size()) {
			continue
		}
		a.logger.V(4).Info("takeFullSockets: claiming socket", "socket", socket)
		a.take(cpusInSocket)
	}
}

// takeFullUncore 整块认领完全空闲且不超出剩余需求的 UncoreCache。
func (a *cpuAccumulator) takeFullUncore() {
	for _, uncore := range a.freeUncoreCache() {
		cpusInUncore := a.topo.CPUDetails.CPUsInUncoreCaches(uncore)
		if !a.needsAtLeast(cpusInUncore.Size()) {
			continue
		}
		a.logger.V(4).Info("takeFullUncore: claiming uncore", "uncore", uncore)
		a.take(cpusInUncore)
	}
}

// takePartialUncore 尝试从指定 UncoreCache 中部分认领 CPU。
func (a *cpuAccumulator) takePartialUncore(uncoreID int) {
	// 无论是否开启超线程，都按每核 CPU 数向上取整计算所需核数。
	numCoresNeeded := (a.numCPUsNeeded + a.topo.CPUsPerCore() - 1) / a.topo.CPUsPerCore()

	// 先确定 UncoreCache 内的空闲核，再取这些核对应的空闲 CPU。
	// 这样能保证尽量整核分配，减少 SMT 不对齐。
	freeCores := a.details.CoresNeededInUncoreCache(numCoresNeeded, uncoreID)
	freeCPUs := a.details.CPUsInCores(freeCores.UnsortedList()...)

	// 开启超线程且剩余需求为奇数时，需要去掉最后一个核的一半 CPU：
	// 先排序空闲 CPU，保证紧凑分配。
	// 空闲数多于需求时丢弃最后一个 CPU，避免多分。
	// 奇数需求只需该核两个线程中的一个。
	// 这样分配结果仍保持核内对齐。
	if a.numCPUsNeeded%2 != 0 && a.topo.CPUsPerCore() > 1 {
		// 排序后尽量整核分配，减少 SMT 不对齐。
		// 排序顺序决定丢弃哪个线程。
		sortFreeCPUs := freeCPUs.List()
		if len(sortFreeCPUs) > a.numCPUsNeeded {
			// 进入部分分配说明整块分配后仍未满足需求，因此空闲数必然大于 0。
			// 因此截断到 Size()-1 是安全的。
			sortFreeCPUs = sortFreeCPUs[:freeCPUs.Size()-1]
		}
		freeCPUs = cpuset.New(sortFreeCPUs...)
	}

	// 只有恰好满足剩余需求时才认领，避免超出。
	claimed := (a.numCPUsNeeded == freeCPUs.Size())
	a.logger.V(4).Info("takePartialUncore: trying to claim partial uncore",
		"uncore", uncoreID,
		"claimed", claimed,
		"needed", a.numCPUsNeeded,
		"cores", freeCores.String(),
		"cpus", freeCPUs.String())
	if !claimed {
		return

	}
	a.take(freeCPUs)
}

// takeUncoreCache 先尝试整块认领 UncoreCache，再尝试部分认领能容纳请求的 UncoreCache。
func (a *cpuAccumulator) takeUncoreCache() {
	numCPUsInUncore := a.topo.CPUsPerUncore()
	for _, uncore := range a.sortAvailableUncoreCaches() {
		// 需求大于等于 UncoreCache 大小时整块认领。
		if a.needsAtLeast(numCPUsInUncore) {
			a.takeFullUncore()
		}

		if a.isSatisfied() {
			return
		}

		// 否则尝试在单个 UncoreCache 内部分分配。
		a.takePartialUncore(uncore)
		if a.isSatisfied() {
			return
		}
	}
}

// takeFullCores 整块认领完全空闲且不超出剩余需求的物理核。
func (a *cpuAccumulator) takeFullCores() {
	for _, core := range a.freeCores() {
		cpusInCore := a.topo.CPUDetails.CPUsInCores(core)
		if !a.needsAtLeast(cpusInCore.Size()) {
			continue
		}
		a.logger.V(4).Info("takeFullCores: claiming core", "core", core)
		a.take(cpusInCore)
	}
}

// takeRemainingCPUs 按排序顺序逐个认领剩余 CPU，直到满足需求。
func (a *cpuAccumulator) takeRemainingCPUs() {
	for _, cpu := range a.availableCPUSorter.sort() {
		a.logger.V(4).Info("takeRemainingCPUs: claiming CPU", "cpu", cpu)
		a.take(cpuset.New(cpu))
		if a.isSatisfied() {
			return
		}
	}
}

// rangeNUMANodesNeededToSatisfy 返回满足 numCPUsNeeded 所需的 NUMA 节点数范围（最小值、最大值），
// 假设 CPU 以 cpuGroupSize 为一组分配。
func (a *cpuAccumulator) rangeNUMANodesNeededToSatisfy(cpuGroupSize int) (minNumNUMAs, maxNumNUMAs int) {
	// 系统中的 NUMA 节点总数。
	numNUMANodes := a.topo.CPUDetails.NUMANodes().Size()

	// 有可用 CPU 的 NUMA 节点数。
	numNUMANodesAvailable := a.details.NUMANodes().Size()

	// 系统 CPU 总数。
	numCPUs := a.topo.CPUDetails.CPUs().Size()

	// 系统可划分的 CPU 组总数（向上取整）。
	numCPUGroups := (numCPUs-1)/cpuGroupSize + 1

	// 每个 NUMA 节点平均可分到的 CPU 组数（向上取整）。
	numCPUGroupsPerNUMANode := (numCPUGroups-1)/numNUMANodes + 1

	// 计算需要的 CPU 组数（向上取整）。
	// 组数决定所需节点数。
	numCPUGroupsNeeded := (a.numCPUsNeeded-1)/cpuGroupSize + 1

	// 最少节点数：按每节点可提供的组数向上取整。
	// 这是理论下界。
	minNumNUMAs = (numCPUGroupsNeeded-1)/numCPUGroupsPerNUMANode + 1

	// 最多节点数：不超过所需组数与可用节点数。
	maxNumNUMAs = min(numCPUGroupsNeeded, numNUMANodesAvailable)

	return
}

// needsAtLeast 判断累加器是否还需要至少 n 个 CPU。
// 需要更多时同样返回 true。
func (a *cpuAccumulator) needsAtLeast(n int) bool {
	return a.numCPUsNeeded >= n
}

// isSatisfied 判断累加器是否已满足需求。
func (a *cpuAccumulator) isSatisfied() bool {
	return a.numCPUsNeeded < 1
}

// isFailed 判断系统可用 CPU 是否已不足以满足需求。
// 例如需要 4 个但只剩 3 个可用。
func (a *cpuAccumulator) isFailed() bool {
	return a.numCPUsNeeded > a.details.CPUs().Size()
}

// iterateCombinations 遍历 n 中所有大小为 k 的组合，并对每个组合调用 f。
// 例如 n={0,1,2}、k=2 时会依次调用 {0,1}、{0,2}、{1,2}；f 返回 Break 时提前结束。
func (a *cpuAccumulator) iterateCombinations(n []int, k int, f func([]int) LoopControl) {
	if k < 1 {
		return
	}

	var helper func(n []int, k int, start int, accum []int, f func([]int) LoopControl) LoopControl
	helper = func(n []int, k int, start int, accum []int, f func([]int) LoopControl) LoopControl {
		if k == 0 {
			return f(accum)
		}
		for i := start; i <= len(n)-k; i++ {
			control := helper(n, k-1, i+1, append(accum, n[i]), f)
			if control == Break {
				return Break
			}
		}
		return Continue
	}

	helper(n, k, 0, []int{}, f)
}

// takeByTopologyNUMAPacked 从 availableCPUs 中取 numCPUs 个 CPU 并返回。
// topo 描述 CPU 在 socket、NUMA 节点与物理核之间的布局（开启超线程时 CPU 是线程而非完整物理核）。
//
// socket 高于 NUMA 节点时，选择顺序如下：
//  1. 若 numCPUs 超过单个 socket 的 CPU 数且存在完全空闲的 socket，尽量整块取空闲 socket；
//     否则尝试整块取 NUMA 节点；
//  2. 若开启 PreferAlignByUncoreCache，尽量整块取空闲 UncoreCache，或在单个 UncoreCache 内分配；
//  3. 尽量整块取空闲物理核；
//  4. 最后逐个取 CPU，优先填补同一 socket 上已部分分配的核。
//
// NUMA 节点高于 socket 时，选择顺序相同，只是整块取 socket 与整块取 NUMA 节点的顺序互换，
// 底层元素的排序优先级也相应互换。
func takeByTopologyNUMAPacked(logger klog.Logger, topo *topology.CPUTopology, availableCPUs cpuset.CPUSet, numCPUs int, cpuSortingStrategy CPUSortingStrategy, preferAlignByUncoreCache bool) (cpuset.CPUSet, error) {
	acc := newCPUAccumulator(logger, topo, availableCPUs, numCPUs, cpuSortingStrategy)
	if acc.isSatisfied() {
		return acc.result, nil
	}
	if acc.isFailed() {
		return cpuset.New(), fmt.Errorf("not enough cpus available to satisfy request: requested=%d, available=%d", numCPUs, availableCPUs.Size())
	}

	// 算法：拓扑感知的最佳适配（topology-aware best-fit）。
	// 1. 需求超过一个 NUMA 节点或 socket 时，优先整块认领。
	//    NUMA 节点包含多个 socket 时先取 NUMA 节点，
	//    否则先取 socket。
	//    这一顺序由 numaOrSocketsFirst 决定。
	acc.numaOrSocketsFirst.takeFullFirstLevel()
	if acc.isSatisfied() {
		return acc.result, nil
	}
	acc.numaOrSocketsFirst.takeFullSecondLevel()
	if acc.isSatisfied() {
		return acc.result, nil
	}

	// 2. 开启 PreferAlignByUncoreCache 时，优先整块或部分认领 UncoreCache。
	//    这样可提升 L3 缓存局部性。
	//    未开启则跳过该层级。
	if preferAlignByUncoreCache {
		acc.takeUncoreCache()
		if acc.isSatisfied() {
			return acc.result, nil
		}
	}

	// 3. 需求超过一个核时，尽量整块认领物理核。
	//    这样可减少 SMT 不对齐。
	//    分散策略下跳过整核认领，直接逐个取 CPU。
	if cpuSortingStrategy != CPUSortingStrategySpread {
		acc.takeFullCores()
		if acc.isSatisfied() {
			return acc.result, nil
		}
	}

	// 4. 最后逐个取 CPU，优先填补同一 socket 上已部分分配的核，
	//    保持亲和性尽量集中。
	//    这是尽力而为的兜底。
	acc.takeRemainingCPUs()
	if acc.isSatisfied() {
		return acc.result, nil
	}

	return cpuset.New(), fmt.Errorf("failed to allocate cpus")
}

// takeByTopologyNUMADistributed 返回大小为 numCPUs 的 CPU 集合。
//
// 按 KEP-2902 描述的算法在多个 NUMA 节点之间均匀分布 CPU：
// https://github.com/kubernetes/enhancements/tree/e7f51ffbe2ee398ffd1fba4a6d854f276bfad9fb/keps/sig-node/2902-cpumanager-distribute-cpus-policy-option
//
// 与 takeByTopologyNUMAPacked 尽量填满 NUMA 节点的做法相反，本算法在需要多个 NUMA 节点时均匀分摊。
//
// 高层流程：
//
// 对每个 NUMA 节点：
//   - 若请求可全部由该节点满足，则在该节点内运行 takeByTopologyNUMAPacked 并返回；
//
// 否则对每两个 NUMA 节点：
//   - 若请求数（模 2）可被两节点均分，且剩余 CPU 可在节点子集间条带化分配，
//     则在两个节点内运行 takeByTopologyNUMAPacked 并返回；
//
// 否则对每三个 NUMA 节点：
//   - 若请求数（模 3）可被三节点均分，且剩余 CPU 可条带化分配，则同样处理；
//
// ...
//
// 否则对所有 NUMA 节点：
//   - 若请求数（模节点数）可均分，且剩余 CPU 可条带化分配，则同样处理。
//
// 若以上条件都不满足，退回在所有可用 CPU 上调用 takeByTopologyNUMAPacked 做最佳适配。
//
// 注意：当请求无法在所选节点集合上均分时，会用“平衡分”选择分配剩余 CPU 的节点子集。
// 平衡分是分配完成后各 NUMA 节点可用 CPU 数的标准差，标准差最小者获得剩余 CPU，
// 以保持整体分配尽量均衡。
//
// 注意：算法支持 cpuGroupSize 参数，保证 CPU 始终按该粒度成组分配；
// 例如开启超线程时必须把同一物理核的所有线程一起分配。
func takeByTopologyNUMADistributed(logger klog.Logger, topo *topology.CPUTopology, availableCPUs cpuset.CPUSet, numCPUs int, cpuGroupSize int, cpuSortingStrategy CPUSortingStrategy, alignBySocket bool) (cpuset.CPUSet, error) {
	// 请求数无法按 cpuGroupSize 整除时无法均分，直接退回紧凑算法。
	// 这是保证成组分配的前提。
	// 否则均分可能破坏核对齐。
	// PreferAlignByUncoreCache 尚未在此算法中实现，固定传 false。
	// 计划在 beta 阶段补齐该能力。
	if (numCPUs % cpuGroupSize) != 0 {
		return takeByTopologyNUMAPacked(logger, topo, availableCPUs, numCPUs, cpuSortingStrategy, false)
	}

	// 构建累加器，用于跟踪可用 CPU 与已分配结果。
	acc := newCPUAccumulator(logger, topo, availableCPUs, numCPUs, cpuSortingStrategy)
	if acc.isSatisfied() {
		return acc.result, nil
	}
	if acc.isFailed() {
		return cpuset.New(), fmt.Errorf("not enough cpus available to satisfy request: requested=%d, available=%d", numCPUs, availableCPUs.Size())
	}

	// 获取可用 CPU 涉及的 NUMA 节点，并按拓扑排序。
	numas := acc.sortAvailableNUMANodes()

	// 计算所需 NUMA 节点数范围，减少组合遍历次数。
	// 该范围是算法的关键优化。
	// 避免从 1 遍历到全部节点数。
	minNUMAs, maxNUMAs := acc.rangeNUMANodesNeededToSatisfy(cpuGroupSize)

	// 从最少节点数开始尝试不同节点组合，寻找可均分的最佳组合。
	// 只遍历 minNUMAs..maxNUMAs，
	// 减少无效组合。
	// 组合数随节点数指数增长，必须限制范围。
	for k := minNUMAs; k <= maxNUMAs; k++ {
		// 遍历所有 n 选 k 的节点组合，寻找平衡分最低者。
		// 平衡分越低表示分配后各节点越均衡。
		// 这是均分策略的核心。
		var bestBalance float64 = math.MaxFloat64
		var bestRemainder []int = nil
		var bestCombo []int = nil
		var bestBalanceInOneSocket = false
		acc.iterateCombinations(numas, k, func(combo []int) LoopControl {
			// 已找到完美平衡的组合时无需继续搜索。
			// alignBySocket 下还需确认该组合在同一 socket 内。
			if bestBalance == 0 && (!alignBySocket || bestBalanceInOneSocket) {
				return Break
			}

			// 组合内 CPU 总数不足则跳过。
			// 这是可行性检查。
			cpus := acc.details.CPUsInNUMANodes(combo...)
			if cpus.Size() < numCPUs {
				return Continue
			}
			// 还要保证能按 cpuGroupSize 成组分配。
			// 否则无法保持核对齐。
			numCPUGroups := 0
			for _, numa := range combo {
				numCPUGroups += (acc.details.CPUsInNUMANodes(numa).Size() / cpuGroupSize)
			}
			if (numCPUGroups * cpuGroupSize) < numCPUs {
				return Continue
			}

			// 计算每个节点应分到的 CPU 数（按组取整）。
			// 这是均分的基础。
			distribution := (numCPUs / len(combo) / cpuGroupSize) * cpuGroupSize
			if alignBySocket {
				for _, numa := range combo {
					// alignBySocket 下均分量不能超过节点实际可用量。
					// 否则会分配失败。
					availableCPUsInNUMA := acc.details.CPUsInNUMANodes(numa).Size() / cpuGroupSize * cpuGroupSize
					if distribution > availableCPUsInNUMA {
						distribution = availableCPUsInNUMA
					}
				}
			}
			// 每个节点都必须能提供均分量。
			// 任一节点不足则跳过该组合。
			for _, numa := range combo {
				cpus := acc.details.CPUsInNUMANodes(numa)
				if cpus.Size() < distribution {
					return Continue
				}
			}
			// 计算均分后各节点的剩余可用量，用于计算平衡分。
			// 这决定了组合的均衡程度。
			// 剩余越均匀，平衡分越低。
			// 平衡分用于最终选择。
			// 这是算法的评分依据。
			availableAfterAllocation := make(mapIntInt, len(numas))
			for _, numa := range numas {
				availableAfterAllocation[numa] = acc.details.CPUsInNUMANodes(numa).Size()
			}
			for _, numa := range combo {
				availableAfterAllocation[numa] -= distribution
			}

			// 均分后可能还有剩余 CPU 需要条带化分配。
			// 剩余量小于节点数乘以组大小。
			// 这部分需要额外的子集选择。
			remainder := numCPUs - (distribution * len(combo))

			// 只有均分后仍能提供一个组的节点才能承接剩余 CPU。
			// 这样可以保证成组分配。
			// 否则剩余分配会失败。
			// 因此先过滤候选节点。
			var remainderCombo []int
			for _, numa := range combo {
				if availableAfterAllocation[numa] >= cpuGroupSize {
					remainderCombo = append(remainderCombo, numa)
				}
			}

			// 局部变量用于在剩余 CPU 的子集选择中记录最优平衡分。
			// 每个子集都会计算一次平衡分。
			// 最优者被保留。
			var bestLocalBalance float64 = math.MaxFloat64
			var bestLocalRemainder []int = nil

			// 无剩余时直接按均分后的可用量计算平衡分。
			// 标准差越小越均衡。
			// 这是组合评估的基准。
			if remainder == 0 {
				bestLocalBalance = standardDeviation(availableAfterAllocation.Values())
				bestLocalRemainder = nil
			}

			// 有剩余时，在候选节点子集中搜索最佳平衡分。
			// 子集越大，剩余 CPU 分布越广。
			// 仍按 cpuGroupSize 成组分配。
			// 从大子集向小子集搜索，优先把剩余 CPU 分散到更多节点。
			// 这符合“尽量分散”的语义。
			// 只有找不到时才退到更小集合。
			for k := len(remainderCombo); remainder > 0 && k >= 1; k-- {
				acc.iterateCombinations(remainderCombo, k, func(subset []int) LoopControl {
					// 使用局部副本，避免修改外层循环状态。
					remainder := remainder

					// 同样复制可用量映射，子集尝试互不影响。
					availableAfterAllocation := availableAfterAllocation.Clone()

					// 子集容量不足则跳过。
					// 这是可行性检查。
					if sum(availableAfterAllocation.Values(subset...)) < remainder {
						return Continue
					}

					// 在子集内轮转分配，每次每个节点取一个 CPU 组。
					// 轮转可保证剩余 CPU 尽量均匀。
					// 剩余量归零即完成。
					for remainder > 0 {
						for _, numa := range subset {
							if remainder == 0 {
								break
							}
							if availableAfterAllocation[numa] < cpuGroupSize {
								continue
							}
							availableAfterAllocation[numa] -= cpuGroupSize
							remainder -= cpuGroupSize
						}
					}

					// 分配完成后计算各节点可用量的标准差。
					// 越小表示越均衡。
					// 用于选择最优子集。
					// 这是评分逻辑。
					balance := standardDeviation(availableAfterAllocation.Values())
					if balance < bestLocalBalance {
						bestLocalBalance = balance
						bestLocalRemainder = subset
					}

					return Continue
				})
			}

			// alignBySocket 时优先选择节点位于同一 socket 的组合。
			// 同 socket 内再按平衡分比较。
			// 这样可减少跨 socket 访问。
			// 这是 socket 对齐的保证。
			inSameSocket := false
			if alignBySocket {
				inSameSocket = topo.CPUDetails.AreNUMANodesInSameSocket(combo)
			}
			isBetter := bestLocalBalance < bestBalance
			if alignBySocket && inSameSocket != bestBalanceInOneSocket {
				isBetter = inSameSocket
			}

			if isBetter {
				bestBalance = bestLocalBalance
				bestRemainder = bestLocalRemainder
				bestCombo = combo
				if alignBySocket {
					bestBalanceInOneSocket = inSameSocket
				}
			}

			return Continue
		})

		// 该 k 值下没有可行组合时，尝试更大的节点集合。
		// 更大的集合更容易满足均分条件。
		// 这是逐层放宽的过程。
		if bestCombo == nil {
			continue
		}

		// 找到组合后，先在每个节点内均分。
		// 均分量按组取整，并受节点实际可用量限制。
		// 这是分配的第一阶段。
		distribution := (numCPUs / len(bestCombo) / cpuGroupSize) * cpuGroupSize
		// 节点选择已完成，这里只需保证不向节点索取超过其可用量。
		// alignBySocket 的影响已在选择阶段体现。
		// 因此这里只做容量裁剪。
		for _, numa := range bestCombo {
			availableCPUsInNUMA := acc.details.CPUsInNUMANodes(numa).Size() / cpuGroupSize * cpuGroupSize
			if distribution > availableCPUsInNUMA {
				distribution = availableCPUsInNUMA
			}
		}
		for _, numa := range bestCombo {
			cpus, _ := takeByTopologyNUMAPacked(logger, acc.topo, acc.details.CPUsInNUMANodes(numa), distribution, cpuSortingStrategy, false)
			acc.take(cpus)
		}

		// 再按组把剩余 CPU 条带化到所选节点。
		// 每个节点每次取一组，直到剩余量归零。
		remainder := numCPUs - (distribution * len(bestCombo))
		for remainder > 0 {
			for _, numa := range bestRemainder {
				if remainder == 0 {
					break
				}
				if acc.details.CPUsInNUMANodes(numa).Size() < cpuGroupSize {
					continue
				}
				cpus, _ := takeByTopologyNUMAPacked(logger, acc.topo, acc.details.CPUsInNUMANodes(numa), cpuGroupSize, cpuSortingStrategy, false)
				acc.take(cpus)
				remainder -= cpuGroupSize
			}
		}

		// 分配数量不足说明记账出现错误，直接报错。
		// 这种情况不应发生。
		if acc.numCPUsNeeded > 0 {
			return cpuset.New(), fmt.Errorf("accounting error, not enough CPUs allocated, remaining: %v", acc.numCPUsNeeded)
		}

		// 分配过多同样是记账错误。
		// 宁可报错也不能返回错误的 CPU 集合。
		if acc.numCPUsNeeded < 0 {
			return cpuset.New(), fmt.Errorf("accounting error, too many CPUs allocated, remaining: %v", acc.numCPUsNeeded)
		}

		// 分配数量正确时返回结果。
		return acc.result, nil
	}

	// 找不到可均分的组合时退回紧凑算法。
	// 保证在极端拓扑下仍能完成分配。
	return takeByTopologyNUMAPacked(logger, topo, availableCPUs, numCPUs, cpuSortingStrategy, false)
}
