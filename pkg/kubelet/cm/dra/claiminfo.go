/*
Copyright 2022 The Kubernetes Authors.

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

package dra

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/go-logr/logr"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/metrics"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/cm/dra/state"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	kubeletmetrics "k8s.io/kubernetes/pkg/kubelet/metrics"
)

// ClaimInfo 保存准备和取消准备一个资源声明所需的信息。
// +k8s:deepcopy-gen=true
type ClaimInfo struct {
	state.ClaimInfoState
	prepared bool
	// unpreparing 从 unprepareResources 决定为最后一个引用 pod 调用 NodeUnprepareResources
	// 的时刻起，到缓存条目被删除为止，一直为 true。置位期间 prepareResources 不得再为
	// 该声明挂载新 pod：声明资源正在被取消准备，新 pod 必须等其完全取消准备后才能准备该声明。
	unpreparing bool
}

// claimInfoCache 是已处理资源声明的缓存，以 namespace/claimname 作为键。
type claimInfoCache struct {
	sync.RWMutex
	checkpointer state.Checkpointer
	claimInfo    map[string]*ClaimInfo
}

// newClaimInfoFromClaim 从资源声明创建新的 claim 信息，并校验 kubelet 能否处理该声明。
func newClaimInfoFromClaim(claim *resourceapi.ResourceClaim) (*ClaimInfo, error) {
	// 尚未分配的声明没有可准备的设备，kubelet 直接拒绝处理。
	if claim.Status.Allocation == nil {
		return nil, errors.New("not allocated")
	}

	driverState, err := computeDriverStates(claim)
	if err != nil {
		return nil, err
	}

	info := &ClaimInfo{
		ClaimInfoState: state.ClaimInfoState{
			ClaimUID:    claim.UID,
			ClaimName:   claim.Name,
			Namespace:   claim.Namespace,
			PodUIDs:     sets.New[string](),
			DriverState: driverState,
		},
		prepared: false,
	}
	return info, nil
}

// computeDriverStates 统计声明分配结果中各驱动的设备数量与跳过节点操作（SkipNodeOperation），
// 汇总为每个驱动的 DriverState。
func computeDriverStates(claim *resourceapi.ResourceClaim) (map[string]state.DriverState, error) {
	devicesPerDriver := make(map[string]int)
	opCountsPerDriver := make(map[string]map[resourceapi.SkipNodeOperation]int)

	for _, result := range claim.Status.Allocation.Devices.Results {
		devicesPerDriver[result.Driver]++
		if len(result.SkipNodeOperations) == 0 {
			continue
		}
		if opCountsPerDriver[result.Driver] == nil {
			opCountsPerDriver[result.Driver] = make(map[resourceapi.SkipNodeOperation]int)
		}
		// SkipNodeOperationAll 已经涵盖该结果的所有单个操作，无需再逐个统计。
		if slices.Contains(result.SkipNodeOperations, resourceapi.SkipNodeOperationAll) {
			opCountsPerDriver[result.Driver][resourceapi.SkipNodeOperationAll]++
			continue
		}
		// API 校验（+listType=set）保证切片元素不重复，可以直接按出现次数计数。
		for _, op := range result.SkipNodeOperations {
			opCountsPerDriver[result.Driver][op]++
		}
	}

	driverStates := make(map[string]state.DriverState, len(devicesPerDriver))
	for driver, total := range devicesPerDriver {
		var skippedOps []resourceapi.SkipNodeOperation
		driverOpCounts := opCountsPerDriver[driver]
		if driverOpCounts != nil {
			// 跳过某个操作的条件是其覆盖了该驱动的全部设备，避免只跳过部分设备却声明整体跳过。
			allCount := driverOpCounts[resourceapi.SkipNodeOperationAll]
			if allCount == total {
				skippedOps = []resourceapi.SkipNodeOperation{resourceapi.SkipNodeOperationAll}
			} else {
				for op, count := range driverOpCounts {
					if op == resourceapi.SkipNodeOperationAll {
						continue
					}
					if count+allCount == total {
						skippedOps = append(skippedOps, op)
					}
				}
				slices.Sort(skippedOps)
			}
		}

		// 声明要求跳过节点操作，但特性门控关闭时 kubelet 无法执行这些操作，直接拒绝处理。
		if len(skippedOps) > 0 && !utilfeature.DefaultFeatureGate.Enabled(features.DRAOptionalNodeOperations) {
			return nil, errors.New("DRAOptionalNodeOperations feature gate is disabled on kubelet")
		}

		driverStates[driver] = state.DriverState{
			SkipNodeOperations: skippedOps,
		}
	}

	return driverStates, nil
}

// newClaimInfoFromState 从已检查点（checkpoint）的 claim 状态对象创建新的 claim 信息。
func newClaimInfoFromState(state *state.ClaimInfoState) *ClaimInfo {
	// 深拷贝状态对象，避免与外部缓存共享底层数据；新条目一律视为未准备，需重新执行准备流程。
	info := &ClaimInfo{
		ClaimInfoState: *state.DeepCopy(),
		prepared:       false,
	}
	return info
}

// addDevice 向 claim 信息中添加一个 CDI 设备。
func (info *ClaimInfo) addDevice(driverName string, deviceState state.Device) {
	// DriverState 可能尚未初始化，先建好映射再追加设备，避免 nil map 写入。
	if info.DriverState == nil {
		info.DriverState = make(map[string]state.DriverState)
	}
	driverState := info.DriverState[driverName]
	driverState.Devices = append(driverState.Devices, deviceState)
	info.DriverState[driverName] = driverState
}

// resetDevices 清空指定驱动的设备列表，但保留其在 DriverState 中的条目。
// 用于在应用新的 NodePrepareResources 响应前，丢弃上一次尝试遗留的陈旧设备。
func (info *ClaimInfo) resetDevices(driverName string) {
	if info.DriverState == nil {
		return
	}
	driverState, ok := info.DriverState[driverName]
	if !ok {
		return
	}
	driverState.Devices = nil
	info.DriverState[driverName] = driverState
}

// addPodReference 为 claim 信息添加一条 pod 引用。
func (info *ClaimInfo) addPodReference(podUID types.UID) {
	info.PodUIDs.Insert(string(podUID))
}

// hasPodReference 检查 claim 信息中是否存在指定 pod 的引用。
func (info *ClaimInfo) hasPodReference(podUID types.UID) bool {
	return info.PodUIDs.Has(string(podUID))
}

// deletePodReference 从 claim 信息中删除指定 pod 的引用。
func (info *ClaimInfo) deletePodReference(podUID types.UID) {
	info.PodUIDs.Delete(string(podUID))
}

// setPrepared 把 claim 信息标记为已准备。
func (info *ClaimInfo) setPrepared() {
	info.prepared = true
}

// isPrepared 判断 claim 信息是否已准备。
func (info *ClaimInfo) isPrepared() bool {
	return info.prepared
}

// setUnpreparing 把 claim 信息标记为正在拆除，使 prepareResources 拒绝再为它挂载新 pod。
func (info *ClaimInfo) setUnpreparing() {
	info.unpreparing = true
}

// isUnpreparing 报告 unprepareResources 是否正在对该声明调用 NodeUnprepareResources。
func (info *ClaimInfo) isUnpreparing() bool {
	return info.unpreparing
}

// cdiDevicesAsList 返回从该 claim 信息中提取的 CDI 设备列表。
// 当请求名非空时，只返回与该请求相关的设备。
func (info *ClaimInfo) cdiDevicesAsList(requestName string) []kubecontainer.CDIDevice {
	var cdiDevices []kubecontainer.CDIDevice
	for _, driverData := range info.DriverState {
		for _, device := range driverData.Devices {
			// 请求名为空时返回全部设备；设备未声明请求名时视为适用于所有请求，否则按请求名过滤。
			if requestName == "" || len(device.RequestNames) == 0 || slices.Contains(device.RequestNames, requestName) {
				for _, cdiDeviceID := range device.CDIDeviceIDs {
					cdiDevices = append(cdiDevices, kubecontainer.CDIDevice{Name: cdiDeviceID})
				}
			}
		}
	}
	return cdiDevices
}

// newClaimInfoCache 创建新的 claim 信息缓存对象，若存在检查点文件则预先填充其中的数据。
func newClaimInfoCache(stateDir, checkpointName string) (*claimInfoCache, error) {
	checkpointer, err := state.NewCheckpointer(stateDir, checkpointName)
	if err != nil {
		return nil, fmt.Errorf("could not initialize checkpoint manager, please drain node and remove DRA state file, err: %w", err)
	}

	// 获取或新建检查点状态：kubelet 重启后靠它恢复此前已处理声明的缓存内容。
	checkpoint, err := checkpointer.GetOrCreate()
	if err != nil {
		return nil, fmt.Errorf("GetOrCreate() on checkpoint state: %w", err)
	}

	cache := &claimInfoCache{
		checkpointer: checkpointer,
		claimInfo:    make(map[string]*ClaimInfo),
	}

	entries, err := checkpoint.GetClaimInfoStateList()
	if err != nil {
		return nil, fmt.Errorf("GetEntries() on checkpoint: %w", err)

	}
	// 按 namespace/claimname 复建缓存键，与运行期写入保持同一格式，保证查找一致。
	for _, entry := range entries {
		info := newClaimInfoFromState(&entry)
		cache.claimInfo[info.Namespace+"/"+info.ClaimName] = info
	}

	return cache, nil
}

// withLock 在持有 claimInfoCache 写锁的状态下执行函数，并记录缓存中声明的使用变化。
func (cache *claimInfoCache) withLock(logger klog.Logger, f func() error) error {
	logger = logger.WithName("dra-claiminfo")
	cache.Lock()
	defer cache.Unlock()

	// 仅在 V(5) 级别开启时统计使用量变化，避免每次加锁都做无谓的遍历。
	if loggerV := logger.V(5); loggerV.Enabled() {
		claimsInUseBefore := cache.claimsInUse()
		defer func() {
			claimsInUseAfter := cache.claimsInUse()
			delta := claimsInUseDelta(claimsInUseBefore, claimsInUseAfter)

			changed := false
			for _, inUse := range delta {
				if inUse.Delta != 0 {
					changed = true
					break
				}
			}

			// 只有存在非零变化时才记录日志，过滤掉空操作。
			if changed {
				logger.V(5).Info("ResourceClaim usage changed", "claimsInUse", delta)
			}
		}()
	}

	return f()
}

// withRLock 在持有 claimInfoCache 读锁的状态下执行函数。
func (cache *claimInfoCache) withRLock(f func() error) error {
	cache.RLock()
	defer cache.RUnlock()
	return f()
}

// add 把新的 claim 信息对象加入缓存。
func (cache *claimInfoCache) add(info *ClaimInfo) {
	cache.claimInfo[info.Namespace+"/"+info.ClaimName] = info
}

// contains 检查指定声明是否已存在于缓存中。
func (cache *claimInfoCache) contains(claimName, namespace string) bool {
	_, exists := cache.claimInfo[namespace+"/"+claimName]
	return exists
}

// get 从缓存中获取指定声明的 claim 信息对象。
func (cache *claimInfoCache) get(claimName, namespace string) (*ClaimInfo, bool) {
	info, exists := cache.claimInfo[namespace+"/"+claimName]
	return info, exists
}

// delete 从缓存中删除指定声明的 claim 信息对象。
func (cache *claimInfoCache) delete(claimName, namespace string) {
	delete(cache.claimInfo, namespace+"/"+claimName)
}

// hasPodReference 检查是否存在至少一个被指定 UID 的 pod 引用的声明。
// 该函数被状态管理器间接调用，用于判断 pod 能否进入终止状态。
func (cache *claimInfoCache) hasPodReference(uid types.UID) bool {
	for _, claimInfo := range cache.claimInfo {
		if claimInfo.hasPodReference(uid) {
			return true
		}
	}
	return false
}

// claimNamesForPod 返回指定 UID 的 pod 所引用的全部声明名称。
// claim 信息缓存是"为该 pod 准备了什么"的权威记录（经过检查点，可跨 kubelet 重启保留），
// 因此该函数不依赖 pod.Status.ResourceClaimStatuses 是否存在。
//
// 必须在持有读锁时调用。
func (cache *claimInfoCache) claimNamesForPod(uid types.UID) []string {
	var claimNames []string
	for _, claimInfo := range cache.claimInfo {
		if claimInfo.hasPodReference(uid) {
			claimNames = append(claimNames, claimInfo.ClaimName)
		}
	}
	return claimNames
}

// syncToCheckpoint 把 claim 信息缓存的完整状态同步到检查点。
func (cache *claimInfoCache) syncToCheckpoint() error {
	// 预分配容量与缓存条目数一致，避免 append 过程中反复扩容。
	claimInfoStateList := make(state.ClaimInfoStateList, 0, len(cache.claimInfo))
	for _, infoClaim := range cache.claimInfo {
		claimInfoStateList = append(claimInfoStateList, infoClaim.ClaimInfoState)
	}
	checkpoint, err := state.NewCheckpoint(claimInfoStateList)
	if err != nil {
		return err
	}
	return cache.checkpointer.Store(checkpoint)
}

// claimsInUse 计算 DRAResourceClaimsInUse 指标当前的计数向量。
// 返回从驱动名到"使用该驱动完成准备的声明数"的映射；
// [kubeletmetrics.DRAResourceClaimsInUseAnyDriver] 键表示全部已准备声明。
//
// 必须在持有读锁时调用。
func (cache *claimInfoCache) claimsInUse() map[string]int {
	counts := make(map[string]int)
	total := 0
	for _, claimInfo := range cache.claimInfo {
		if !claimInfo.isPrepared() {
			continue
		}
		total++

		for driverName := range claimInfo.DriverState {
			counts[driverName]++
		}
	}
	counts[kubeletmetrics.DRAResourceClaimsInUseAnyDriver] = total
	return counts
}

// claimsInUseDelta 比较 claimsInUse 返回的两个映射，生成各驱动的使用量变化。
// 该类型可直接用作结构化日志的值。
func claimsInUseDelta(before, after map[string]int) ClaimsInUseDelta {
	var delta ClaimsInUseDelta
	for driverName, count := range before {
		if _, stillSet := after[driverName]; !stillSet {
			delta = append(delta, ClaimsInUse{DriverName: driverName, Count: 0, Delta: -count})
		}
	}
	for driverName, count := range after {
		delta = append(delta, ClaimsInUse{DriverName: driverName, Count: count, Delta: count - before[driverName]})
	}
	return delta
}

// ClaimsInUseDelta 提供 String（用于文本日志）和 MarshalLog（用于结构化日志）方法。
type ClaimsInUseDelta []ClaimsInUse

// 编译期校验 ClaimsInUseDelta 实现了 fmt.Stringer 接口。
var _ fmt.Stringer = ClaimsInUseDelta{}

// 编译期校验 ClaimsInUseDelta 实现了 logr.Marshaler 接口，可输出结构化日志。
var _ logr.Marshaler = ClaimsInUseDelta{}

// String 把变化列表格式化为多行文本，每行形如 "驱动名: 当前值 (变化量)"。
func (d ClaimsInUseDelta) String() string {
	d = d.sort()
	var buffer strings.Builder
	for i, inUse := range d {
		if i > 0 {
			buffer.WriteByte('\n')
		}
		buffer.WriteString(fmt.Sprintf("%s: %d (%+d)", inUse.DriverName, inUse.Count, inUse.Delta))
	}
	return buffer.String()
}

// MarshalLog 返回排序后的切片，供结构化日志直接序列化。
func (d ClaimsInUseDelta) MarshalLog() any {
	d = d.sort()
	return []ClaimsInUse(d)
}

// sort 返回按驱动名排序的切片副本，保证日志输出顺序稳定可复现。
func (d ClaimsInUseDelta) sort() ClaimsInUseDelta {
	// 先克隆再排序，避免改动调用方持有的原始切片。
	d = slices.Clone(d)
	slices.SortFunc(d, func(a, b ClaimsInUse) int {
		return strings.Compare(a.DriverName, b.DriverName)
	})
	return d
}

// ClaimsInUse 记录单个驱动在某次快照中的使用量及其与上一次快照的变化量。
type ClaimsInUse struct {
	DriverName string
	Count      int
	Delta      int
}

// claimInfoCollector 为 claimInfoCache 提供指标采集。
type claimInfoCollector struct {
	metrics.BaseStableCollector
	cache *claimInfoCache
}

// 编译期校验 claimInfoCollector 实现了 StableCollector 接口。
var _ metrics.StableCollector = &claimInfoCollector{}

// DescribeWithStability 实现 metrics.StableCollector 接口，声明本采集器导出的指标描述符。
func (collector *claimInfoCollector) DescribeWithStability(ch chan<- *metrics.Desc) {
	ch <- kubeletmetrics.DRAResourceClaimsInUseDesc
}

// CollectWithStability 实现 metrics.StableCollector 接口，采集当前各驱动的声明使用量并上报。
func (collector *claimInfoCollector) CollectWithStability(ch chan<- metrics.Metric) {
	var claimsInUse map[string]int
	// 通过读锁读取使用量，保证采集期间缓存不被并发修改。
	_ = collector.cache.withRLock(func() error {
		claimsInUse = collector.cache.claimsInUse()
		return nil
	})

	// 只上报当前已知的驱动。若某驱动过去有活跃声明、后来不再使用并被卸载，
	// 它就不会再出现。这避免了普通 GaugeVec 随时间膨胀、必须主动删除过期驱动的内存泄漏问题。
	//
	// 空驱动名对应所有已准备 ResourceClaim 的总计数，与驱动无关。
	for driverName, count := range claimsInUse {
		ch <- metrics.NewLazyConstMetric(kubeletmetrics.DRAResourceClaimsInUseDesc, metrics.GaugeValue, float64(count), driverName)
	}
}
