/*
Copyright 2016 The Kubernetes Authors.

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

// Package eviction 实现 kubelet 的节点资源压力驱逐：解析阈值配置、采集信号观测、排序待驱逐 Pod 并执行资源回收。
package eviction

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	resourcehelper "k8s.io/component-helpers/resource"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/klog/v2"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	v1resource "k8s.io/kubernetes/pkg/api/v1/resource"
	"k8s.io/kubernetes/pkg/features"
	evictionapi "k8s.io/kubernetes/pkg/kubelet/eviction/api"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
	volumeutils "k8s.io/kubernetes/pkg/volume/util"
)

const (
	unsupportedEvictionSignal = "unsupported eviction signal %v"
	// Reason 是 Pod 因驱逐而失败时写入 status.reason 的原因字符串。
	Reason = "Evicted"
	// nodeLowMessageFmt 是资源压力驱逐消息的开头部分。
	nodeLowMessageFmt = "The node was low on resource: %v. "
	// nodeConditionMessageFmt 是节点进入某种状态导致驱逐时的消息格式。
	nodeConditionMessageFmt = "The node had condition: %v. "
	// containerMessageFmt 在容器用量超过 request 时追加明细，便于事后定位问题容器。
	containerMessageFmt = "Container %s was using %s, request is %s, has larger consumption of %v. "
	// podMessageFmt 在 Pod 级别用量超过 request 时追加明细。
	podMessageFmt = "Pod %s was using %s, request is %s, has larger consumption of %v. "
	// containerEphemeralStorageMessageFmt 用于说明容器超出临时存储上限。
	containerEphemeralStorageMessageFmt = "Container %s exceeded its local ephemeral storage limit %q. "
	// podEphemeralStorageMessageFmt 用于说明 Pod 临时存储总用量超出容器限额之和。
	podEphemeralStorageMessageFmt = "Pod ephemeral local storage usage exceeds the total limit of containers %s. "
	// emptyDirMessageFmt 用于说明 emptyDir 卷超出其 sizeLimit。
	emptyDirMessageFmt = "Usage of EmptyDir volume %q exceeds the limit %q. "
	// resourceInodes 是模块内部使用的资源名，用于统计本地磁盘 inode 消耗。
	resourceInodes v1.ResourceName = "inodes"
	// resourcePids 是模块内部使用的资源名，用于统计本地 PID 消耗。
	resourcePids v1.ResourceName = "pids"
	// OffendingContainersKey 是驱逐事件注解中记录超限容器名的键。
	OffendingContainersKey = "offending_containers"
	// OffendingContainersUsageKey 是驱逐事件注解中记录超限容器用量的键，与容器名顺序一一对应。
	OffendingContainersUsageKey = "offending_containers_usage"
	// OffendingPodKey 是驱逐事件注解中记录 Pod 级超限 Pod 名的键。
	OffendingPodKey = "offending_pod"
	// OffendingPodUsageKey 是驱逐事件注解中记录 Pod 级超限用量的键。
	OffendingPodUsageKey = "offending_pod_usage"
	// StarvedResourceKey 是驱逐事件注解中记录引发驱逐的资源名的键。
	StarvedResourceKey = "starved_resource"
	// thresholdMetMessageFmt 在驱逐消息中给出阈值量和当前可用量。
	thresholdMetMessageFmt = "Threshold quantity: %v, available: %v. "
)

var (
	// signalToNodeCondition 将驱逐信号映射到阈值触发时需要上报的 NodeCondition。
	signalToNodeCondition map[evictionapi.Signal]v1.NodeConditionType
	// signalToResource 将驱逐信号映射到对应的资源名，同时用作“信号是否受支持”的判定依据。
	signalToResource map[evictionapi.Signal]v1.ResourceName
)

// init 建立驱逐信号到节点状态和资源名的静态映射表。
func init() {
	// 每个信号对应一个 NodeCondition；同一资源上的多个信号会映射到同一状态。
	signalToNodeCondition = map[evictionapi.Signal]v1.NodeConditionType{}
	signalToNodeCondition[evictionapi.SignalMemoryAvailable] = v1.NodeMemoryPressure
	signalToNodeCondition[evictionapi.SignalAllocatableMemoryAvailable] = v1.NodeMemoryPressure
	signalToNodeCondition[evictionapi.SignalImageFsAvailable] = v1.NodeDiskPressure
	signalToNodeCondition[evictionapi.SignalContainerFsAvailable] = v1.NodeDiskPressure
	signalToNodeCondition[evictionapi.SignalNodeFsAvailable] = v1.NodeDiskPressure
	signalToNodeCondition[evictionapi.SignalImageFsInodesFree] = v1.NodeDiskPressure
	signalToNodeCondition[evictionapi.SignalNodeFsInodesFree] = v1.NodeDiskPressure
	signalToNodeCondition[evictionapi.SignalContainerFsInodesFree] = v1.NodeDiskPressure
	signalToNodeCondition[evictionapi.SignalPIDAvailable] = v1.NodePIDPressure

	// 信号到资源的映射是驱逐决策的基础：没有对应资源即视为不支持该信号。
	signalToResource = map[evictionapi.Signal]v1.ResourceName{}
	signalToResource[evictionapi.SignalMemoryAvailable] = v1.ResourceMemory
	signalToResource[evictionapi.SignalAllocatableMemoryAvailable] = v1.ResourceMemory
	signalToResource[evictionapi.SignalImageFsAvailable] = v1.ResourceEphemeralStorage
	signalToResource[evictionapi.SignalImageFsInodesFree] = resourceInodes
	signalToResource[evictionapi.SignalContainerFsAvailable] = v1.ResourceEphemeralStorage
	signalToResource[evictionapi.SignalContainerFsInodesFree] = resourceInodes
	signalToResource[evictionapi.SignalNodeFsAvailable] = v1.ResourceEphemeralStorage
	signalToResource[evictionapi.SignalNodeFsInodesFree] = resourceInodes
	signalToResource[evictionapi.SignalPIDAvailable] = resourcePids
}

// validSignal 判断信号是否受支持。
func validSignal(signal evictionapi.Signal) bool {
	_, found := signalToResource[signal]
	return found
}

// getReclaimableThreshold 在已越过的阈值中找出第一个具备回收能力的阈值及其资源。
func getReclaimableThreshold(logger klog.Logger, thresholds []evictionapi.Threshold) (evictionapi.Threshold, v1.ResourceName, bool) {
	for _, thresholdToReclaim := range thresholds {
		if resourceToReclaim, ok := signalToResource[thresholdToReclaim.Signal]; ok {
			return thresholdToReclaim, resourceToReclaim, true
		}
		// 已越过但无法回收的信号不触发驱逐，仅记录以便排查配置问题。
		logger.V(3).Info("Eviction manager: threshold was crossed, but reclaim is not implemented for this threshold.", "threshold", thresholdToReclaim.Signal)
	}
	return evictionapi.Threshold{}, "", false
}

// ParseThresholdConfig 将命令行/配置中的硬阈值、软阈值、宽限期和最小回收量解析为 Threshold 列表。
func ParseThresholdConfig(allocatableConfig []string, evictionHard, evictionSoft, evictionSoftGracePeriod, evictionMinimumReclaim map[string]string) ([]evictionapi.Threshold, error) {
	results := []evictionapi.Threshold{}
	hardThresholds, err := parseThresholdStatements(evictionHard)
	if err != nil {
		return nil, err
	}
	results = append(results, hardThresholds...)
	// 硬阈值一旦越过立即驱逐，软阈值则需满足宽限期后才驱逐。
	softThresholds, err := parseThresholdStatements(evictionSoft)
	if err != nil {
		return nil, err
	}
	gracePeriods, err := parseGracePeriods(evictionSoftGracePeriod)
	if err != nil {
		return nil, err
	}
	minReclaims, err := parseMinimumReclaims(evictionMinimumReclaim)
	if err != nil {
		return nil, err
	}
	// 软阈值必须显式配置宽限期，否则无法判断何时触发驱逐。
	for i := range softThresholds {
		signal := softThresholds[i].Signal
		period, found := gracePeriods[signal]
		if !found {
			return nil, fmt.Errorf("grace period must be specified for the soft eviction threshold %v", signal)
		}
		softThresholds[i].GracePeriod = period
	}
	results = append(results, softThresholds...)
	for i := range results {
		if minReclaim, ok := minReclaims[results[i].Signal]; ok {
			results[i].MinReclaim = &minReclaim
		}
	}
	// 启用 NodeAllocatable 强制约束时，额外派生可分配内存阈值。
	for _, key := range allocatableConfig {
		if key == kubetypes.NodeAllocatableEnforcementKey {
			results = addAllocatableThresholds(results)
			break
		}
	}
	return results, nil
}

// addAllocatableThresholds 基于硬性 memory.available 阈值派生 memory.available<allocatable> 阈值。
func addAllocatableThresholds(thresholds []evictionapi.Threshold) []evictionapi.Threshold {
	additionalThresholds := []evictionapi.Threshold{}
	for _, threshold := range thresholds {
		// 只派生硬阈值：软阈值的宽限期语义无法直接复制到可分配内存信号上。
		if threshold.Signal == evictionapi.SignalMemoryAvailable && isHardEvictionThreshold(threshold) {
			// 复制操作符、取值和最小回收量，仅将信号替换为 SignalAllocatableMemoryAvailable。
			additionalThresholds = append(additionalThresholds, evictionapi.Threshold{
				Signal:     evictionapi.SignalAllocatableMemoryAvailable,
				Operator:   threshold.Operator,
				Value:      threshold.Value,
				MinReclaim: threshold.MinReclaim,
			})
		}
	}
	return append(append([]evictionapi.Threshold{}, thresholds...), additionalThresholds...)
}

// UpdateContainerFsThresholds 依据容器运行时的文件系统布局补充 containerfs 的硬/软驱逐阈值。
// 阈值从 evictionHard/evictionSoft 解析而来，因此对 containerfs 是覆盖式设置。
// 若只有一个文件系统，containerfs 设置等同于 nodefs。
// 若容器与镜像共用独立文件系统，containerfs 设置等同于 imagefs。
// 收集 nodefs/imagefs 的硬软阈值以及 containerfs 阈值的下标，稍后统一覆盖。
func UpdateContainerFsThresholds(thresholds []evictionapi.Threshold, imageFs, separateContainerImageFs bool) ([]evictionapi.Threshold, error) {
	hardNodeFsDisk := evictionapi.Threshold{}
	softNodeFsDisk := evictionapi.Threshold{}
	hardNodeINodeDisk := evictionapi.Threshold{}
	softNodeINodeDisk := evictionapi.Threshold{}
	hardImageFsDisk := evictionapi.Threshold{}
	softImageFsDisk := evictionapi.Threshold{}
	hardImageINodeDisk := evictionapi.Threshold{}
	softImageINodeDisk := evictionapi.Threshold{}

	hardContainerFsDisk := -1
	softContainerFsDisk := -1
	hardContainerFsINodes := -1
	softContainerFsINodes := -1
	// 记录 imagefs 与 nodefs 的各信号阈值，后面按文件系统布局重新计算 containerfs 阈值。
	var err error = nil
	for idx, threshold := range thresholds {
		if threshold.Signal == evictionapi.SignalImageFsAvailable && isHardEvictionThreshold(threshold) {
			hardImageFsDisk = threshold
		}
		if threshold.Signal == evictionapi.SignalImageFsAvailable && !isHardEvictionThreshold(threshold) {
			softImageFsDisk = threshold
		}
		if threshold.Signal == evictionapi.SignalImageFsInodesFree && isHardEvictionThreshold(threshold) {
			hardImageINodeDisk = threshold
		}
		if threshold.Signal == evictionapi.SignalImageFsInodesFree && !isHardEvictionThreshold(threshold) {
			softImageINodeDisk = threshold
		}
		if threshold.Signal == evictionapi.SignalNodeFsAvailable && isHardEvictionThreshold(threshold) {
			hardNodeFsDisk = threshold
		}
		if threshold.Signal == evictionapi.SignalNodeFsAvailable && !isHardEvictionThreshold(threshold) {
			softNodeFsDisk = threshold
		}
		if threshold.Signal == evictionapi.SignalNodeFsInodesFree && isHardEvictionThreshold(threshold) {
			hardNodeINodeDisk = threshold
		}
		if threshold.Signal == evictionapi.SignalNodeFsInodesFree && !isHardEvictionThreshold(threshold) {
			softNodeINodeDisk = threshold
		}
		// 用户显式配置的 containerfs 阈值会被覆盖：我们只支持其与 nodefs 或 imagefs 保持一致。
		// 因此记录告警并保存下标，而不是直接报错中断启动。
		// 下面按布局选择容器文件系统阈值来源。
		if threshold.Signal == evictionapi.SignalContainerFsAvailable && isHardEvictionThreshold(threshold) {
			err = errors.Join(fmt.Errorf("found containerfs.available for hard eviction. ignoring"))
			hardContainerFsDisk = idx
		}
		if threshold.Signal == evictionapi.SignalContainerFsAvailable && !isHardEvictionThreshold(threshold) {
			err = errors.Join(fmt.Errorf("found containerfs.available for soft eviction. ignoring"))
			softContainerFsDisk = idx
		}
		if threshold.Signal == evictionapi.SignalContainerFsInodesFree && isHardEvictionThreshold(threshold) {
			err = errors.Join(fmt.Errorf("found containerfs.inodesFree for hard eviction. ignoring"))
			hardContainerFsINodes = idx
		}
		if threshold.Signal == evictionapi.SignalContainerFsInodesFree && !isHardEvictionThreshold(threshold) {
			err = errors.Join(fmt.Errorf("found containerfs.inodesFree for soft eviction. ignoring"))
			softContainerFsINodes = idx
		}
	}
	// 容器可写层与 nodefs 同盘，或整机只有一个文件系统：containerfs 复用 nodefs 阈值。
	if (imageFs && separateContainerImageFs) || (!imageFs && !separateContainerImageFs) {
		if hardContainerFsDisk != -1 {
			thresholds[hardContainerFsDisk] = evictionapi.Threshold{
				Signal: evictionapi.SignalContainerFsAvailable, Operator: hardNodeFsDisk.Operator, Value: hardNodeFsDisk.Value, MinReclaim: hardNodeFsDisk.MinReclaim,
			}
		} else {
			thresholds = append(thresholds, evictionapi.Threshold{
				Signal:     evictionapi.SignalContainerFsAvailable,
				Operator:   hardNodeFsDisk.Operator,
				Value:      hardNodeFsDisk.Value,
				MinReclaim: hardNodeFsDisk.MinReclaim,
			})
		}
		if softContainerFsDisk != -1 {
			thresholds[softContainerFsDisk] = evictionapi.Threshold{
				Signal: evictionapi.SignalContainerFsAvailable, GracePeriod: softNodeFsDisk.GracePeriod, Operator: softNodeFsDisk.Operator, Value: softNodeFsDisk.Value, MinReclaim: softNodeFsDisk.MinReclaim,
			}
		} else {
			thresholds = append(thresholds, evictionapi.Threshold{
				Signal:      evictionapi.SignalContainerFsAvailable,
				Operator:    softNodeFsDisk.Operator,
				Value:       softNodeFsDisk.Value,
				MinReclaim:  softNodeFsDisk.MinReclaim,
				GracePeriod: softNodeFsDisk.GracePeriod,
			})
		}
		if hardContainerFsINodes != -1 {
			thresholds[hardContainerFsINodes] = evictionapi.Threshold{
				Signal: evictionapi.SignalContainerFsInodesFree, Operator: hardNodeINodeDisk.Operator, Value: hardNodeINodeDisk.Value, MinReclaim: hardNodeINodeDisk.MinReclaim,
			}
		} else {
			thresholds = append(thresholds, evictionapi.Threshold{
				Signal:     evictionapi.SignalContainerFsInodesFree,
				Operator:   hardNodeINodeDisk.Operator,
				Value:      hardNodeINodeDisk.Value,
				MinReclaim: hardNodeINodeDisk.MinReclaim,
			})
		}
		if softContainerFsINodes != -1 {
			thresholds[softContainerFsINodes] = evictionapi.Threshold{
				Signal: evictionapi.SignalContainerFsInodesFree, GracePeriod: softNodeINodeDisk.GracePeriod, Operator: softNodeINodeDisk.Operator, Value: softNodeINodeDisk.Value, MinReclaim: softNodeINodeDisk.MinReclaim,
			}
		} else {
			thresholds = append(thresholds, evictionapi.Threshold{
				Signal:      evictionapi.SignalContainerFsInodesFree,
				Operator:    softNodeINodeDisk.Operator,
				Value:       softNodeINodeDisk.Value,
				MinReclaim:  softNodeINodeDisk.MinReclaim,
				GracePeriod: softNodeINodeDisk.GracePeriod,
			})
		}
	}
	// 镜像单独占盘：containerfs 复用 imagefs 阈值。
	if imageFs && !separateContainerImageFs {
		if hardContainerFsDisk != -1 {
			thresholds[hardContainerFsDisk] = evictionapi.Threshold{
				Signal: evictionapi.SignalContainerFsAvailable, Operator: hardImageFsDisk.Operator, Value: hardImageFsDisk.Value, MinReclaim: hardImageFsDisk.MinReclaim,
			}
		} else {
			thresholds = append(thresholds, evictionapi.Threshold{
				Signal:     evictionapi.SignalContainerFsAvailable,
				Operator:   hardImageFsDisk.Operator,
				Value:      hardImageFsDisk.Value,
				MinReclaim: hardImageFsDisk.MinReclaim,
			})
		}
		if softContainerFsDisk != -1 {
			thresholds[softContainerFsDisk] = evictionapi.Threshold{
				Signal: evictionapi.SignalContainerFsAvailable, GracePeriod: softImageFsDisk.GracePeriod, Operator: softImageFsDisk.Operator, Value: softImageFsDisk.Value, MinReclaim: softImageFsDisk.MinReclaim,
			}
		} else {
			thresholds = append(thresholds, evictionapi.Threshold{
				Signal:      evictionapi.SignalContainerFsAvailable,
				Operator:    softImageFsDisk.Operator,
				Value:       softImageFsDisk.Value,
				MinReclaim:  softImageFsDisk.MinReclaim,
				GracePeriod: softImageFsDisk.GracePeriod,
			})
		}
		if hardContainerFsINodes != -1 {
			thresholds[hardContainerFsINodes] = evictionapi.Threshold{
				Signal: evictionapi.SignalContainerFsInodesFree, GracePeriod: hardImageINodeDisk.GracePeriod, Operator: hardImageINodeDisk.Operator, Value: hardImageINodeDisk.Value, MinReclaim: hardImageINodeDisk.MinReclaim,
			}
		} else {
			thresholds = append(thresholds, evictionapi.Threshold{
				Signal:     evictionapi.SignalContainerFsInodesFree,
				Operator:   hardImageINodeDisk.Operator,
				Value:      hardImageINodeDisk.Value,
				MinReclaim: hardImageINodeDisk.MinReclaim,
			})
		}
		if softContainerFsINodes != -1 {
			thresholds[softContainerFsINodes] = evictionapi.Threshold{
				Signal: evictionapi.SignalContainerFsInodesFree, GracePeriod: softImageINodeDisk.GracePeriod, Operator: softImageINodeDisk.Operator, Value: softImageINodeDisk.Value, MinReclaim: softImageINodeDisk.MinReclaim,
			}
		} else {
			thresholds = append(thresholds, evictionapi.Threshold{
				Signal:      evictionapi.SignalContainerFsInodesFree,
				Operator:    softImageINodeDisk.Operator,
				Value:       softImageINodeDisk.Value,
				MinReclaim:  softImageINodeDisk.MinReclaim,
				GracePeriod: softImageINodeDisk.GracePeriod,
			})
		}
	}
	return thresholds, err
}

// parseThresholdStatements 将“信号->取值”形式的配置解析为 Threshold 列表。
func parseThresholdStatements(statements map[string]string) ([]evictionapi.Threshold, error) {
	if len(statements) == 0 {
		return nil, nil
	}
	results := []evictionapi.Threshold{}
	for signal, val := range statements {
		result, err := parseThresholdStatement(evictionapi.Signal(signal), val)
		if err != nil {
			return nil, err
		}
		if result != nil {
			results = append(results, *result)
		}
	}
	return results, nil
}

// parseThresholdStatement 解析单个阈值配置，返回 nil 表示该配置应被忽略（如 0% 和 100%）。
// 取值支持百分比和 resource.Quantity 两种形式。
func parseThresholdStatement(signal evictionapi.Signal, val string) (*evictionapi.Threshold, error) {
	if !validSignal(signal) {
		return nil, fmt.Errorf(unsupportedEvictionSignal, signal)
	}
	operator := evictionapi.OpForSignal[signal]
	if strings.HasSuffix(val, "%") {
		// 0% 表示禁用该信号，100% 永远无法触发，二者都直接忽略。
		if val == "0%" || val == "100%" {
			return nil, nil
		}
		percentage, err := parsePercentage(val)
		if err != nil {
			return nil, err
		}
		if percentage < 0 {
			return nil, fmt.Errorf("eviction percentage threshold %v must be >= 0%%: %s", signal, val)
		}
		// parsePercentage 返回 0~1 的比例，超过 1 说明配置非法。
		if percentage > 1 {
			return nil, fmt.Errorf("eviction percentage threshold %v must be <= 100%%: %s", signal, val)
		}
		return &evictionapi.Threshold{
			Signal:   signal,
			Operator: operator,
			Value: evictionapi.ThresholdValue{
				Percentage: percentage,
			},
		}, nil
	}
	quantity, err := resource.ParseQuantity(val)
	if err != nil {
		return nil, err
	}
	if quantity.Sign() < 0 || quantity.IsZero() {
		return nil, fmt.Errorf("eviction threshold %v must be positive: %s", signal, &quantity)
	}
	return &evictionapi.Threshold{
		Signal:   signal,
		Operator: operator,
		Value: evictionapi.ThresholdValue{
			Quantity: &quantity,
		},
	}, nil
}

// parsePercentage 将 “12%” 之类的字符串解析为 0~1 的比例。
func parsePercentage(input string) (float32, error) {
	value, err := strconv.ParseFloat(strings.TrimRight(input, "%"), 32)
	if err != nil {
		return 0, err
	}
	return float32(value) / 100, nil
}

// parseGracePeriods 解析软阈值的宽限期配置。
func parseGracePeriods(statements map[string]string) (map[evictionapi.Signal]time.Duration, error) {
	if len(statements) == 0 {
		return nil, nil
	}
	results := map[evictionapi.Signal]time.Duration{}
	for signal, val := range statements {
		signal := evictionapi.Signal(signal)
		if !validSignal(signal) {
			return nil, fmt.Errorf(unsupportedEvictionSignal, signal)
		}
		gracePeriod, err := time.ParseDuration(val)
		if err != nil {
			return nil, err
		}
		if gracePeriod < 0 {
			return nil, fmt.Errorf("invalid eviction grace period specified: %v, must be a positive value", val)
		}
		results[signal] = gracePeriod
	}
	return results, nil
}

// parseMinimumReclaims 解析最小回收量配置，用于避免刚触发阈值就立即再次驱逐。
func parseMinimumReclaims(statements map[string]string) (map[evictionapi.Signal]evictionapi.ThresholdValue, error) {
	if len(statements) == 0 {
		return nil, nil
	}
	results := map[evictionapi.Signal]evictionapi.ThresholdValue{}
	for signal, val := range statements {
		signal := evictionapi.Signal(signal)
		if !validSignal(signal) {
			return nil, fmt.Errorf(unsupportedEvictionSignal, signal)
		}
		if strings.HasSuffix(val, "%") {
			percentage, err := parsePercentage(val)
			if err != nil {
				return nil, err
			}
			if percentage <= 0 {
				return nil, fmt.Errorf("eviction percentage minimum reclaim %v must be positive: %s", signal, val)
			}
			results[signal] = evictionapi.ThresholdValue{
				Percentage: percentage,
			}
			continue
		}
		quantity, err := resource.ParseQuantity(val)
		if err != nil {
			return nil, err
		}
		if quantity.Sign() < 0 {
			return nil, fmt.Errorf("negative eviction minimum reclaim specified for %v", signal)
		}
		results[signal] = evictionapi.ThresholdValue{
			Quantity: &quantity,
		}
	}
	return results, nil
}

// diskUsage 将已用字节数转换为资源量；stats 缺失时返回零值。
func diskUsage(fsStats *statsapi.FsStats) *resource.Quantity {
	if fsStats == nil || fsStats.UsedBytes == nil {
		return &resource.Quantity{Format: resource.BinarySI}
	}
	usage := int64(*fsStats.UsedBytes)
	return resource.NewQuantity(usage, resource.BinarySI)
}

// inodeUsage 将已用 inode 数转换为资源量；stats 缺失时返回零值。
func inodeUsage(fsStats *statsapi.FsStats) *resource.Quantity {
	if fsStats == nil || fsStats.InodesUsed == nil {
		return &resource.Quantity{Format: resource.DecimalSI}
	}
	usage := int64(*fsStats.InodesUsed)
	return resource.NewQuantity(usage, resource.DecimalSI)
}

// memoryUsage 将工作集（working set）字节数转换为资源量；stats 缺失时返回零值。
func memoryUsage(memStats *statsapi.MemoryStats) *resource.Quantity {
	if memStats == nil || memStats.WorkingSetBytes == nil {
		return &resource.Quantity{Format: resource.BinarySI}
	}
	usage := int64(*memStats.WorkingSetBytes)
	return resource.NewQuantity(usage, resource.BinarySI)
}

// processUsage 返回进程数；stats 缺失时返回 0。
func processUsage(processStats *statsapi.ProcessStats) uint64 {
	if processStats == nil || processStats.ProcessCount == nil {
		return 0
	}
	usage := uint64(*processStats.ProcessCount)
	return usage
}

// localVolumeNames 返回 Pod 中会消耗本地存储的卷名，包括 hostPath 和本地临时卷。
// TODO: 汇总 API 应报告哪些卷消耗本地存储，而非在此硬编码。
func localVolumeNames(pod *v1.Pod) []string {
	result := []string{}
	for _, volume := range pod.Spec.Volumes {
		if volume.HostPath != nil ||
			volumeutils.IsLocalEphemeralVolume(volume) {
			result = append(result, volume.Name)
		}
	}
	return result
}

// containerUsage 汇总 Pod 内所有容器在指定 stats 维度上的磁盘与 inode 消耗。
func containerUsage(podStats statsapi.PodStats, statsToMeasure []fsStatsType) v1.ResourceList {
	disk := resource.Quantity{Format: resource.BinarySI}
	inodes := resource.Quantity{Format: resource.DecimalSI}
	for _, container := range podStats.Containers {
		if hasFsStatsType(statsToMeasure, fsStatsRoot) {
			disk.Add(*diskUsage(container.Rootfs))
			inodes.Add(*inodeUsage(container.Rootfs))
		}
		if hasFsStatsType(statsToMeasure, fsStatsLogs) {
			disk.Add(*diskUsage(container.Logs))
			inodes.Add(*inodeUsage(container.Logs))
		}
	}
	return v1.ResourceList{
		v1.ResourceEphemeralStorage: disk,
		resourceInodes:              inodes,
	}
}

// podLocalVolumeUsage 汇总指定本地卷的磁盘与 inode 消耗。
func podLocalVolumeUsage(volumeNames []string, podStats statsapi.PodStats) v1.ResourceList {
	disk := resource.Quantity{Format: resource.BinarySI}
	inodes := resource.Quantity{Format: resource.DecimalSI}
	for _, volumeName := range volumeNames {
		for _, volumeStats := range podStats.VolumeStats {
			if volumeStats.Name == volumeName {
				disk.Add(*diskUsage(&volumeStats.FsStats))
				inodes.Add(*inodeUsage(&volumeStats.FsStats))
				break
			}
		}
	}
	return v1.ResourceList{
		v1.ResourceEphemeralStorage: disk,
		resourceInodes:              inodes,
	}
}

// podDiskUsage 汇总 Pod 的容器与本地卷磁盘、inode 消耗。
func podDiskUsage(podStats statsapi.PodStats, pod *v1.Pod, statsToMeasure []fsStatsType) (v1.ResourceList, error) {
	disk := resource.Quantity{Format: resource.BinarySI}
	inodes := resource.Quantity{Format: resource.DecimalSI}

	containerUsageList := containerUsage(podStats, statsToMeasure)
	disk.Add(containerUsageList[v1.ResourceEphemeralStorage])
	inodes.Add(containerUsageList[resourceInodes])

	// 仅当需要统计本地卷时才遍历卷，避免无谓开销。
	if hasFsStatsType(statsToMeasure, fsStatsLocalVolumeSource) {
		volumeNames := localVolumeNames(pod)
		podLocalVolumeUsageList := podLocalVolumeUsage(volumeNames, podStats)
		disk.Add(podLocalVolumeUsageList[v1.ResourceEphemeralStorage])
		inodes.Add(podLocalVolumeUsageList[resourceInodes])
	}
	return v1.ResourceList{
		v1.ResourceEphemeralStorage: disk,
		resourceInodes:              inodes,
	}, nil
}

// formatThreshold 将阈值格式化为便于日志阅读的字符串。
func formatThreshold(threshold evictionapi.Threshold) string {
	return fmt.Sprintf("threshold(signal=%v, operator=%v, value=%v, gracePeriod=%v)", threshold.Signal, threshold.Operator, evictionapi.ThresholdValue(threshold.Value), threshold.GracePeriod)
}

// cachedStatsFunc 按 Pod UID 建立统计缓存，返回可反复查询的 statsFunc。
func cachedStatsFunc(podStats []statsapi.PodStats) statsFunc {
	uid2PodStats := map[string]statsapi.PodStats{}
	for i := range podStats {
		uid2PodStats[podStats[i].PodRef.UID] = podStats[i]
	}
	return func(pod *v1.Pod) (statsapi.PodStats, bool) {
		stats, found := uid2PodStats[string(pod.UID)]
		return stats, found
	}
}

// Cmp 比较 p1 与 p2，返回 -1/0/+1 表示小于/等于/大于。
//
//	-1 表示 p1 < p2
//	 0 表示 p1 == p2
//	+1 表示 p1 > p2
type cmpFunc func(p1, p2 *v1.Pod) int

// multiSorter 按给定的比较函数序列依次比较，实现多级排序。
type multiSorter struct {
	pods []*v1.Pod
	cmp  []cmpFunc
}

// Sort 使用 OrderedBy 传入的比较函数对切片排序。
func (ms *multiSorter) Sort(pods []*v1.Pod) {
	ms.pods = pods
	sort.Sort(ms)
}

// orderedBy 构造按比较函数序列依次生效的多级排序器。
// 调用其 Sort 方法即可完成排序。
func orderedBy(cmp ...cmpFunc) *multiSorter {
	return &multiSorter{
		cmp: cmp,
	}
}

// Len 实现 sort.Interface。
func (ms *multiSorter) Len() int {
	return len(ms.pods)
}

// Swap 实现 sort.Interface。
func (ms *multiSorter) Swap(i, j int) {
	ms.pods[i], ms.pods[j] = ms.pods[j], ms.pods[i]
}

// Less 实现 sort.Interface：前面的比较函数相等时才用后一个判定。
func (ms *multiSorter) Less(i, j int) bool {
	p1, p2 := ms.pods[i], ms.pods[j]
	var k int
	for k = 0; k < len(ms.cmp)-1; k++ {
		cmpResult := ms.cmp[k](p1, p2)
		// p1 小于 p2。
		if cmpResult < 0 {
			return true
		}
		// p1 大于 p2。
		if cmpResult > 0 {
			return false
		}
		// 尚无法判定，继续比较下一级。
	}
	// 前面全部相等时，由最后一个比较函数决定顺序。
	return ms.cmp[k](p1, p2) < 0
}

// priority 按 Pod 优先级比较；未设置时视为 0。
func priority(p1, p2 *v1.Pod) int {
	priority1 := corev1helpers.PodPriority(p1)
	priority2 := corev1helpers.PodPriority(p2)
	if priority1 == priority2 {
		return 0
	}
	if priority1 > priority2 {
		return 1
	}
	return -1
}

// exceedMemoryRequests 优先判定内存用量是否超过 request。
func exceedMemoryRequests(stats statsFunc) cmpFunc {
	return func(p1, p2 *v1.Pod) int {
		p1Stats, p1Found := stats(p1)
		p2Stats, p2Found := stats(p2)
		if !p1Found || !p2Found {
			// 缺少统计数据的 Pod 优先驱逐，因为无法判断其真实用量。
			return cmpBool(!p1Found, !p2Found)
		}

		p1Memory := memoryUsage(p1Stats.Memory)
		p2Memory := memoryUsage(p2Stats.Memory)
		p1ExceedsRequests := p1Memory.Cmp(v1resource.GetResourceRequestQuantity(p1, v1.ResourceMemory)) == 1
		p2ExceedsRequests := p2Memory.Cmp(v1resource.GetResourceRequestQuantity(p2, v1.ResourceMemory)) == 1
		// 超过 request 的 Pod 排在前面。
		return cmpBool(p1ExceedsRequests, p2ExceedsRequests)
	}
}

// memory 按“内存用量减去 request”的差值从大到小排序。
func memory(stats statsFunc) cmpFunc {
	return func(p1, p2 *v1.Pod) int {
		p1Stats, p1Found := stats(p1)
		p2Stats, p2Found := stats(p2)
		if !p1Found || !p2Found {
			// 缺少统计数据的 Pod 优先驱逐。
			return cmpBool(!p1Found, !p2Found)
		}

		// 减去 request 后比较，突出超出申请量的部分。
		p1Memory := memoryUsage(p1Stats.Memory)
		p1Request := v1resource.GetResourceRequestQuantity(p1, v1.ResourceMemory)
		p1Memory.Sub(p1Request)

		p2Memory := memoryUsage(p2Stats.Memory)
		p2Request := v1resource.GetResourceRequestQuantity(p2, v1.ResourceMemory)
		p2Memory.Sub(p2Request)

		// 注意参数顺序反转：Cmp 结果使消耗更大者排在前面。
		return p2Memory.Cmp(*p1Memory)
	}
}

// process 按进程数从多到少排序。
func process(stats statsFunc) cmpFunc {
	return func(p1, p2 *v1.Pod) int {
		p1Stats, p1Found := stats(p1)
		p2Stats, p2Found := stats(p2)
		if !p1Found || !p2Found {
			// 缺少统计数据的 Pod 优先驱逐。
			return cmpBool(!p1Found, !p2Found)
		}

		p1Process := processUsage(p1Stats.ProcessStats)
		p2Process := processUsage(p2Stats.ProcessStats)
		return int(p2Process - p1Process)
	}
}

// exceedDiskRequests 优先判定磁盘/inode 用量是否超过 request。
func exceedDiskRequests(stats statsFunc, fsStatsToMeasure []fsStatsType, diskResource v1.ResourceName) cmpFunc {
	return func(p1, p2 *v1.Pod) int {
		p1Stats, p1Found := stats(p1)
		p2Stats, p2Found := stats(p2)
		if !p1Found || !p2Found {
			// 缺少统计数据的 Pod 优先驱逐。
			return cmpBool(!p1Found, !p2Found)
		}

		p1Usage, p1Err := podDiskUsage(p1Stats, p1, fsStatsToMeasure)
		p2Usage, p2Err := podDiskUsage(p2Stats, p2, fsStatsToMeasure)
		if p1Err != nil || p2Err != nil {
			// 取统计失败者优先驱逐，避免因统计异常而无法决策。
			return cmpBool(p1Err != nil, p2Err != nil)
		}

		p1Disk := p1Usage[diskResource]
		p2Disk := p2Usage[diskResource]
		p1ExceedsRequests := p1Disk.Cmp(v1resource.GetResourceRequestQuantity(p1, diskResource)) == 1
		p2ExceedsRequests := p2Disk.Cmp(v1resource.GetResourceRequestQuantity(p2, diskResource)) == 1
		// 超过 request 的 Pod 优先驱逐。
		return cmpBool(p1ExceedsRequests, p2ExceedsRequests)
	}
}

// disk 按“磁盘用量减去 request”的差值从大到小排序。
func disk(stats statsFunc, fsStatsToMeasure []fsStatsType, diskResource v1.ResourceName) cmpFunc {
	return func(p1, p2 *v1.Pod) int {
		p1Stats, p1Found := stats(p1)
		p2Stats, p2Found := stats(p2)
		if !p1Found || !p2Found {
			// 缺少统计数据的 Pod 优先驱逐。
			return cmpBool(!p1Found, !p2Found)
		}
		p1Usage, p1Err := podDiskUsage(p1Stats, p1, fsStatsToMeasure)
		p2Usage, p2Err := podDiskUsage(p2Stats, p2, fsStatsToMeasure)
		if p1Err != nil || p2Err != nil {
			// 获取统计数据出错的 Pod 优先驱逐。
			return cmpBool(p1Err != nil, p2Err != nil)
		}

		// 按 request（如有）调整 p1、p2 的用量。
		p1Disk := p1Usage[diskResource]
		p2Disk := p2Usage[diskResource]
		p1Request := v1resource.GetResourceRequestQuantity(p1, v1.ResourceEphemeralStorage)
		p1Disk.Sub(p1Request)
		p2Request := v1resource.GetResourceRequestQuantity(p2, v1.ResourceEphemeralStorage)
		p2Disk.Sub(p2Request)
		// 参数顺序反转，使用量更大者排在前面。
		return p2Disk.Cmp(p1Disk)
	}
}

// cmpBool 比较布尔值，true 排在 false 之前。
func cmpBool(a, b bool) int {
	if a == b {
		return 0
	}
	if !b {
		return -1
	}
	return 1
}

// rankMemoryPressure 在内存压力下排序待驱逐 Pod：先看是否超 request，再看优先级，最后看超出量。
// 该顺序保证低优先级、超量大的 Pod 先被驱逐。
// （多级排序的具体顺序由 orderedBy 参数顺序决定。）
func rankMemoryPressure(pods []*v1.Pod, stats statsFunc) {
	orderedBy(exceedMemoryRequests(stats), priority, memory(stats)).Sort(pods)
}

// rankPIDPressure 在 PID 压力下先按优先级、再按进程数排序。
func rankPIDPressure(pods []*v1.Pod, stats statsFunc) {
	orderedBy(priority, process(stats)).Sort(pods)
}

// rankDiskPressureFunc 返回针对指定 fs 统计维度和资源名的磁盘压力排序函数。
func rankDiskPressureFunc(fsStatsToMeasure []fsStatsType, diskResource v1.ResourceName) rankFunc {
	return func(pods []*v1.Pod, stats statsFunc) {
		orderedBy(exceedDiskRequests(stats, fsStatsToMeasure, diskResource), priority, disk(stats, fsStatsToMeasure, diskResource)).Sort(pods)
	}
}

// byEvictionPriority 实现 sort.Interface，用于对阈值按回收优先级排序。
type byEvictionPriority []evictionapi.Threshold

// Len 实现 sort.Interface。
func (a byEvictionPriority) Len() int { return len(a) }

// Swap 实现 sort.Interface。
func (a byEvictionPriority) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Less 将内存相关阈值排在最前，并将无法回收资源的阈值排在最后。
func (a byEvictionPriority) Less(i, j int) bool {
	_, jSignalHasResource := signalToResource[a[j].Signal]
	return a[i].Signal == evictionapi.SignalMemoryAvailable || a[i].Signal == evictionapi.SignalAllocatableMemoryAvailable || !jSignalHasResource
}

// makeSignalObservations 从节点 summary 中提取各驱逐信号的当前可用量与容量。
func makeSignalObservations(logger klog.Logger, summary *statsapi.Summary) (signalObservations, statsFunc) {
	// Pod 统计按 UID 缓存，供后续排序和生成驱逐消息复用。
	statsFunc := cachedStatsFunc(summary.Pods)
	// 逐类采集信号观测，任何字段缺失都跳过该信号而不是写入错误值。
	result := signalObservations{}

	memoryAvailableSignal := makeMemoryAvailableSignalObservation(logger, summary)
	if memoryAvailableSignal != nil {
		result[evictionapi.SignalMemoryAvailable] = *memoryAvailableSignal
	}

	// 可分配内存信号来自 pods 系统容器：可用量 + 工作集近似为可分配上限。
	if allocatableContainer, err := getSysContainer(summary.Node.SystemContainers, statsapi.SystemContainerPods); err != nil {
		logger.Error(err, "Eviction manager: failed to construct signal", "signal", evictionapi.SignalAllocatableMemoryAvailable)
	} else {
		if memory := allocatableContainer.Memory; memory != nil && memory.AvailableBytes != nil && memory.WorkingSetBytes != nil {
			result[evictionapi.SignalAllocatableMemoryAvailable] = signalObservation{
				available: resource.NewQuantity(int64(*memory.AvailableBytes), resource.BinarySI),
				capacity:  resource.NewQuantity(int64(*memory.AvailableBytes+*memory.WorkingSetBytes), resource.BinarySI),
				time:      memory.Time,
			}
		}
	}
	// nodefs 信号：节点根文件系统的可用空间与 inode。
	if nodeFs := summary.Node.Fs; nodeFs != nil {
		if nodeFs.AvailableBytes != nil && nodeFs.CapacityBytes != nil {
			result[evictionapi.SignalNodeFsAvailable] = signalObservation{
				available: resource.NewQuantity(int64(*nodeFs.AvailableBytes), resource.BinarySI),
				capacity:  resource.NewQuantity(int64(*nodeFs.CapacityBytes), resource.BinarySI),
				time:      nodeFs.Time,
			}
		}
		if nodeFs.InodesFree != nil && nodeFs.Inodes != nil {
			result[evictionapi.SignalNodeFsInodesFree] = signalObservation{
				available: resource.NewQuantity(int64(*nodeFs.InodesFree), resource.DecimalSI),
				capacity:  resource.NewQuantity(int64(*nodeFs.Inodes), resource.DecimalSI),
				time:      nodeFs.Time,
			}
		}
	}
	// 运行时提供 imagefs 与 containerfs 的统计。
	if summary.Node.Runtime != nil {
		if imageFs := summary.Node.Runtime.ImageFs; imageFs != nil {
			if imageFs.AvailableBytes != nil && imageFs.CapacityBytes != nil {
				result[evictionapi.SignalImageFsAvailable] = signalObservation{
					available: resource.NewQuantity(int64(*imageFs.AvailableBytes), resource.BinarySI),
					capacity:  resource.NewQuantity(int64(*imageFs.CapacityBytes), resource.BinarySI),
					time:      imageFs.Time,
				}
			}
			if imageFs.InodesFree != nil && imageFs.Inodes != nil {
				result[evictionapi.SignalImageFsInodesFree] = signalObservation{
					available: resource.NewQuantity(int64(*imageFs.InodesFree), resource.DecimalSI),
					capacity:  resource.NewQuantity(int64(*imageFs.Inodes), resource.DecimalSI),
					time:      imageFs.Time,
				}
			}
		}
		if containerFs := summary.Node.Runtime.ContainerFs; containerFs != nil {
			if containerFs.AvailableBytes != nil && containerFs.CapacityBytes != nil {
				result[evictionapi.SignalContainerFsAvailable] = signalObservation{
					available: resource.NewQuantity(int64(*containerFs.AvailableBytes), resource.BinarySI),
					capacity:  resource.NewQuantity(int64(*containerFs.CapacityBytes), resource.BinarySI),
					time:      containerFs.Time,
				}
			}
			if containerFs.InodesFree != nil && containerFs.Inodes != nil {
				result[evictionapi.SignalContainerFsInodesFree] = signalObservation{
					available: resource.NewQuantity(int64(*containerFs.InodesFree), resource.DecimalSI),
					capacity:  resource.NewQuantity(int64(*containerFs.Inodes), resource.DecimalSI),
					time:      containerFs.Time,
				}
			}
		}
	}
	// PID 可用量 = 最大 PID 数 - 当前运行进程数。
	if rlimit := summary.Node.Rlimit; rlimit != nil {
		if rlimit.NumOfRunningProcesses != nil && rlimit.MaxPID != nil {
			available := int64(*rlimit.MaxPID) - int64(*rlimit.NumOfRunningProcesses)
			result[evictionapi.SignalPIDAvailable] = signalObservation{
				available: resource.NewQuantity(available, resource.DecimalSI),
				capacity:  resource.NewQuantity(int64(*rlimit.MaxPID), resource.DecimalSI),
				time:      rlimit.Time,
			}
		}
	}
	return result, statsFunc
}

// getSysContainer 按名称查找系统容器统计，找不到时报错。
func getSysContainer(sysContainers []statsapi.ContainerStats, name string) (*statsapi.ContainerStats, error) {
	for _, cont := range sysContainers {
		if cont.Name == name {
			return &cont, nil
		}
	}
	return nil, fmt.Errorf("system container %q not found in metrics", name)
}

// thresholdsMet 返回已越过但尚未考虑宽限期的阈值集合。
func thresholdsMet(logger klog.Logger, thresholds []evictionapi.Threshold, observations signalObservations, enforceMinReclaim bool) []evictionapi.Threshold {
	results := []evictionapi.Threshold{}
	for i := range thresholds {
		threshold := thresholds[i]
		observed, found := observations[threshold.Signal]
		if !found {
			logger.Info("Eviction manager: no observation found for eviction signal", "signal", threshold.Signal)
			continue
		}
		// 阈值量按容量换算为绝对值，再与当前可用量比较。
		thresholdMet := false
		quantity := evictionapi.GetThresholdQuantity(threshold.Value, observed.capacity)
		// 启用最小回收量时，按“阈值 + 最小回收量”比较，避免回收量不足导致反复触发。
		if enforceMinReclaim && threshold.MinReclaim != nil {
			quantity.Add(*evictionapi.GetThresholdQuantity(*threshold.MinReclaim, observed.capacity))
		}
		thresholdResult := quantity.Cmp(*observed.available)
		// 目前只支持小于操作符：可用量低于阈值即视为越过。
		switch threshold.Operator {
		case evictionapi.OpLessThan:
			thresholdMet = thresholdResult > 0
		}
		if thresholdMet {
			results = append(results, threshold)
		}
	}
	return results
}

// debugLogObservations 在 V(3) 级别输出所有信号观测，便于排查驱逐决策。
func debugLogObservations(logger klog.Logger, logPrefix string, observations signalObservations) {
	if !logger.V(3).Enabled() {
		return
	}
	for k, v := range observations {
		if !v.time.IsZero() {
			logger.V(3).Info("Eviction manager:", "log", logPrefix, "signal", k, "resourceName", signalToResource[k], "available", v.available, "capacity", v.capacity, "time", v.time)
		} else {
			logger.V(3).Info("Eviction manager:", "log", logPrefix, "signal", k, "resourceName", signalToResource[k], "available", v.available, "capacity", v.capacity)
		}
	}
}

// debugLogThresholdsWithObservation 在 V(3) 级别输出阈值及其观测到的量。
func debugLogThresholdsWithObservation(logger klog.Logger, logPrefix string, thresholds []evictionapi.Threshold, observations signalObservations) {
	if !logger.V(3).Enabled() {
		return
	}
	for i := range thresholds {
		threshold := thresholds[i]
		observed, found := observations[threshold.Signal]
		if found {
			quantity := evictionapi.GetThresholdQuantity(threshold.Value, observed.capacity)
			logger.V(3).Info("Eviction manager: threshold observed resource", "log", logPrefix, "signal", threshold.Signal, "resourceName", signalToResource[threshold.Signal], "quantity", quantity, "available", observed.available)
		} else {
			logger.V(3).Info("Eviction manager: threshold had no observation", "log", logPrefix, "signal", threshold.Signal)
		}
	}
}

// thresholdsUpdatedStats 过滤出观测数据比上次更新的阈值，避免用陈旧统计做决策。
func thresholdsUpdatedStats(logger klog.Logger, thresholds []evictionapi.Threshold, observations, lastObservations signalObservations) []evictionapi.Threshold {
	results := []evictionapi.Threshold{}
	for i := range thresholds {
		threshold := thresholds[i]
		observed, found := observations[threshold.Signal]
		if !found {
			logger.Info("Eviction manager: no observation found for eviction signal", "signal", threshold.Signal)
			continue
		}
		last, found := lastObservations[threshold.Signal]
		if !found || observed.time.IsZero() || observed.time.After(last.time.Time) {
			results = append(results, threshold)
		}
	}
	return results
}

// thresholdsFirstObservedAt 记录每个阈值首次被观测到越过的时刻，用于计算宽限期。
func thresholdsFirstObservedAt(thresholds []evictionapi.Threshold, lastObservedAt thresholdsObservedAt, now time.Time) thresholdsObservedAt {
	results := thresholdsObservedAt{}
	for i := range thresholds {
		observedAt, found := lastObservedAt[thresholds[i]]
		if !found {
			observedAt = now
		}
		results[thresholds[i]] = observedAt
	}
	return results
}

// thresholdsMetGracePeriod 返回已满足宽限期的阈值。
func thresholdsMetGracePeriod(logger klog.Logger, observedAt thresholdsObservedAt, now time.Time) []evictionapi.Threshold {
	results := []evictionapi.Threshold{}
	for threshold, at := range observedAt {
		duration := now.Sub(at)
		if duration < threshold.GracePeriod {
			logger.V(2).Info("Eviction manager: eviction criteria not yet met", "threshold", formatThreshold(threshold), "duration", duration)
			continue
		}
		results = append(results, threshold)
	}
	return results
}

// nodeConditions 返回这些阈值对应的 NodeCondition 集合（去重）。
func nodeConditions(thresholds []evictionapi.Threshold) []v1.NodeConditionType {
	results := []v1.NodeConditionType{}
	for _, threshold := range thresholds {
		if nodeCondition, found := signalToNodeCondition[threshold.Signal]; found {
			if !hasNodeCondition(results, nodeCondition) {
				results = append(results, nodeCondition)
			}
		}
	}
	return results
}

// nodeConditionsLastObservedAt 合并新旧状态观测，记录每个状态最近一次被观测到的时间。
func nodeConditionsLastObservedAt(nodeConditions []v1.NodeConditionType, lastObservedAt nodeConditionsObservedAt, now time.Time) nodeConditionsObservedAt {
	results := nodeConditionsObservedAt{}
	// 本次观测到的状态时间记为当前时刻。
	for i := range nodeConditions {
		results[nodeConditions[i]] = now
	}
	// 本次未观测到的状态保留上次的时间，以便判断其是否仍在持续期内。
	for key, value := range lastObservedAt {
		_, found := results[key]
		if !found {
			results[key] = value
		}
	}
	return results
}

// nodeConditionsObservedSince 返回在指定时间段内被观测到过的状态。
func nodeConditionsObservedSince(observedAt nodeConditionsObservedAt, period time.Duration, now time.Time) []v1.NodeConditionType {
	results := []v1.NodeConditionType{}
	for nodeCondition, at := range observedAt {
		duration := now.Sub(at)
		if duration < period {
			results = append(results, nodeCondition)
		}
	}
	return results
}

// hasFsStatsType 判断 fsStatsType 是否在列表中。
func hasFsStatsType(inputs []fsStatsType, item fsStatsType) bool {
	for _, input := range inputs {
		if input == item {
			return true
		}
	}
	return false
}

// hasNodeCondition 判断 NodeCondition 是否在列表中。
func hasNodeCondition(inputs []v1.NodeConditionType, item v1.NodeConditionType) bool {
	for _, input := range inputs {
		if input == item {
			return true
		}
	}
	return false
}

// mergeThresholds 合并两个阈值列表并去重。
func mergeThresholds(inputsA []evictionapi.Threshold, inputsB []evictionapi.Threshold) []evictionapi.Threshold {
	results := inputsA
	for _, threshold := range inputsB {
		if !hasThreshold(results, threshold) {
			results = append(results, threshold)
		}
	}
	return results
}

// hasThreshold 判断阈值是否已存在（信号、操作符、取值、宽限期均相同）。
func hasThreshold(inputs []evictionapi.Threshold, item evictionapi.Threshold) bool {
	for _, input := range inputs {
		if input.GracePeriod == item.GracePeriod && input.Operator == item.Operator && input.Signal == item.Signal && compareThresholdValue(input.Value, item.Value) {
			return true
		}
	}
	return false
}

// compareThresholdValue 判断两个阈值取值是否等价（Quantity 或百分比）。
func compareThresholdValue(a evictionapi.ThresholdValue, b evictionapi.ThresholdValue) bool {
	if a.Quantity != nil {
		if b.Quantity == nil {
			return false
		}
		return a.Quantity.Cmp(*b.Quantity) == 0
	}
	if b.Quantity != nil {
		return false
	}
	return a.Percentage == b.Percentage
}

// isHardEvictionThreshold 判断是否为硬阈值：宽限期为 0 即立即驱逐。
func isHardEvictionThreshold(threshold evictionapi.Threshold) bool {
	return threshold.GracePeriod == time.Duration(0)
}

// isAllocatableEvictionThreshold 判断阈值是否针对可分配内存信号。
func isAllocatableEvictionThreshold(threshold evictionapi.Threshold) bool {
	return threshold.Signal == evictionapi.SignalAllocatableMemoryAvailable
}

// buildSignalToRankFunc 按文件系统布局为各信号选择排序函数。
func buildSignalToRankFunc(withImageFs bool, imageContainerSplitFs bool) map[evictionapi.Signal]rankFunc {
	signalToRankFunc := map[evictionapi.Signal]rankFunc{
		evictionapi.SignalMemoryAvailable:            rankMemoryPressure,
		evictionapi.SignalAllocatableMemoryAvailable: rankMemoryPressure,
		evictionapi.SignalPIDAvailable:               rankPIDPressure,
	}
	// 排序函数决定压力下先驱逐哪些 Pod，因此必须与回收函数覆盖的统计维度一致。
	// 镜像与容器共用一块独立磁盘：imagefs 压力只统计 rootfs 与镜像层。
	// nodefs 压力只统计日志和本地卷。
	if withImageFs && !imageContainerSplitFs {
		// 有独立 imagefs 时，nodefs 压力只统计日志与本地卷。
		signalToRankFunc[evictionapi.SignalNodeFsAvailable] = rankDiskPressureFunc([]fsStatsType{fsStatsLogs, fsStatsLocalVolumeSource}, v1.ResourceEphemeralStorage)
		signalToRankFunc[evictionapi.SignalNodeFsInodesFree] = rankDiskPressureFunc([]fsStatsType{fsStatsLogs, fsStatsLocalVolumeSource}, resourceInodes)
		// imagefs 压力统计容器可写层与镜像层。
		signalToRankFunc[evictionapi.SignalImageFsAvailable] = rankDiskPressureFunc([]fsStatsType{fsStatsRoot, fsStatsImages}, v1.ResourceEphemeralStorage)
		signalToRankFunc[evictionapi.SignalImageFsInodesFree] = rankDiskPressureFunc([]fsStatsType{fsStatsRoot, fsStatsImages}, resourceInodes)
		signalToRankFunc[evictionapi.SignalContainerFsAvailable] = signalToRankFunc[evictionapi.SignalImageFsAvailable]
		signalToRankFunc[evictionapi.SignalContainerFsInodesFree] = signalToRankFunc[evictionapi.SignalImageFsInodesFree]

		// 镜像与容器可写层分处不同磁盘时，containerfs 只关心可写层。
		// 因此 containerfs 信号复用 nodefs 的排序函数。
	} else if withImageFs && imageContainerSplitFs {
		// nodefs 压力统计日志、本地卷和可写层。
		signalToRankFunc[evictionapi.SignalNodeFsAvailable] = rankDiskPressureFunc([]fsStatsType{fsStatsLogs, fsStatsLocalVolumeSource, fsStatsRoot}, v1.ResourceEphemeralStorage)
		signalToRankFunc[evictionapi.SignalNodeFsInodesFree] = rankDiskPressureFunc([]fsStatsType{fsStatsLogs, fsStatsLocalVolumeSource, fsStatsRoot}, resourceInodes)
		signalToRankFunc[evictionapi.SignalContainerFsAvailable] = signalToRankFunc[evictionapi.SignalNodeFsAvailable]
		signalToRankFunc[evictionapi.SignalContainerFsInodesFree] = signalToRankFunc[evictionapi.SignalNodeFsInodesFree]
		// imagefs 压力只统计镜像层。

		signalToRankFunc[evictionapi.SignalImageFsAvailable] = rankDiskPressureFunc([]fsStatsType{fsStatsImages}, v1.ResourceEphemeralStorage)
		signalToRankFunc[evictionapi.SignalImageFsInodesFree] = rankDiskPressureFunc([]fsStatsType{fsStatsImages}, resourceInodes)
		// 镜像与 nodefs 同盘：imagefs、nodefs 共用全部统计维度。
	} else {
		// 所有文件系统同盘时，各信号排序函数完全一致。
		// 这样任意信号触发都能正确评估 Pod 的总体磁盘占用。
		signalToRankFunc[evictionapi.SignalNodeFsAvailable] = rankDiskPressureFunc([]fsStatsType{fsStatsImages, fsStatsRoot, fsStatsLogs, fsStatsLocalVolumeSource}, v1.ResourceEphemeralStorage)
		signalToRankFunc[evictionapi.SignalNodeFsInodesFree] = rankDiskPressureFunc([]fsStatsType{fsStatsImages, fsStatsRoot, fsStatsLogs, fsStatsLocalVolumeSource}, resourceInodes)
		signalToRankFunc[evictionapi.SignalImageFsAvailable] = rankDiskPressureFunc([]fsStatsType{fsStatsImages, fsStatsRoot, fsStatsLogs, fsStatsLocalVolumeSource}, v1.ResourceEphemeralStorage)
		signalToRankFunc[evictionapi.SignalImageFsInodesFree] = rankDiskPressureFunc([]fsStatsType{fsStatsImages, fsStatsRoot, fsStatsLogs, fsStatsLocalVolumeSource}, resourceInodes)
		signalToRankFunc[evictionapi.SignalContainerFsAvailable] = signalToRankFunc[evictionapi.SignalNodeFsAvailable]
		signalToRankFunc[evictionapi.SignalContainerFsInodesFree] = signalToRankFunc[evictionapi.SignalNodeFsInodesFree]
	}
	return signalToRankFunc
}

// PodIsEvicted 判断 Pod 是否因驱逐而失败。
func PodIsEvicted(podStatus v1.PodStatus) bool {
	return podStatus.Phase == v1.PodFailed && podStatus.Reason == Reason
}

// buildSignalToNodeReclaimFuncs 按文件系统布局为各信号选择节点级资源回收动作。
func buildSignalToNodeReclaimFuncs(imageGC ImageGC, containerGC ContainerGC, withImageFs bool, splitContainerImageFs bool) map[evictionapi.Signal]nodeReclaimFuncs {
	signalToReclaimFunc := map[evictionapi.Signal]nodeReclaimFuncs{}
	// 回收函数需要与排序函数覆盖的统计维度匹配，否则可能回收无效。
	if withImageFs && !splitContainerImageFs {
		// 有独立 imagefs 时，nodefs 压力无需删除镜像或容器。
		signalToReclaimFunc[evictionapi.SignalNodeFsAvailable] = nodeReclaimFuncs{}
		signalToReclaimFunc[evictionapi.SignalNodeFsInodesFree] = nodeReclaimFuncs{}
		// imagefs 压力下删除未使用的容器和镜像。
		signalToReclaimFunc[evictionapi.SignalImageFsAvailable] = nodeReclaimFuncs{containerGC.DeleteAllUnusedContainers, imageGC.DeleteUnusedImages}
		signalToReclaimFunc[evictionapi.SignalImageFsInodesFree] = nodeReclaimFuncs{containerGC.DeleteAllUnusedContainers, imageGC.DeleteUnusedImages}
		signalToReclaimFunc[evictionapi.SignalContainerFsAvailable] = signalToReclaimFunc[evictionapi.SignalImageFsAvailable]
		signalToReclaimFunc[evictionapi.SignalContainerFsInodesFree] = signalToReclaimFunc[evictionapi.SignalImageFsInodesFree]
		// 镜像与容器可写层分盘：各自回收自己磁盘上的对象。
		// containerfs/nodefs 压力下回收容器可写层。
		// imagefs 压力下回收镜像。
	} else if withImageFs && splitContainerImageFs {
		// imagefs 压力下只回收未使用镜像。
		signalToReclaimFunc[evictionapi.SignalImageFsAvailable] = nodeReclaimFuncs{imageGC.DeleteUnusedImages}
		signalToReclaimFunc[evictionapi.SignalImageFsInodesFree] = nodeReclaimFuncs{imageGC.DeleteUnusedImages}
		// 分盘场景下 containerfs 压力回收未使用容器。
		signalToReclaimFunc[evictionapi.SignalNodeFsAvailable] = nodeReclaimFuncs{containerGC.DeleteAllUnusedContainers}
		signalToReclaimFunc[evictionapi.SignalNodeFsInodesFree] = nodeReclaimFuncs{containerGC.DeleteAllUnusedContainers}
		signalToReclaimFunc[evictionapi.SignalContainerFsAvailable] = signalToReclaimFunc[evictionapi.SignalNodeFsAvailable]
		signalToReclaimFunc[evictionapi.SignalContainerFsInodesFree] = signalToReclaimFunc[evictionapi.SignalNodeFsInodesFree]
	} else {
		// 无独立 imagefs 时所有文件系统同盘，任意压力都回收容器和镜像。
		// 因此各信号共用同一组回收函数。
		signalToReclaimFunc[evictionapi.SignalNodeFsAvailable] = nodeReclaimFuncs{containerGC.DeleteAllUnusedContainers, imageGC.DeleteUnusedImages}
		signalToReclaimFunc[evictionapi.SignalNodeFsInodesFree] = nodeReclaimFuncs{containerGC.DeleteAllUnusedContainers, imageGC.DeleteUnusedImages}
		signalToReclaimFunc[evictionapi.SignalImageFsAvailable] = nodeReclaimFuncs{containerGC.DeleteAllUnusedContainers, imageGC.DeleteUnusedImages}
		signalToReclaimFunc[evictionapi.SignalImageFsInodesFree] = nodeReclaimFuncs{containerGC.DeleteAllUnusedContainers, imageGC.DeleteUnusedImages}
		signalToReclaimFunc[evictionapi.SignalContainerFsAvailable] = signalToReclaimFunc[evictionapi.SignalNodeFsAvailable]
		signalToReclaimFunc[evictionapi.SignalContainerFsInodesFree] = signalToReclaimFunc[evictionapi.SignalNodeFsInodesFree]
	}
	return signalToReclaimFunc
}

// evictionMessage 生成驱逐原因消息，并附带记录超限容器/Pod 的注解。
func evictionMessage(resourceToReclaim v1.ResourceName, pod *v1.Pod, stats statsFunc, thresholds []evictionapi.Threshold, observations signalObservations) (message string, annotations map[string]string) {
	annotations = make(map[string]string)
	message = fmt.Sprintf(nodeLowMessageFmt, resourceToReclaim)
	quantity, available := getThresholdMetInfo(resourceToReclaim, thresholds, observations)
	// 阈值信息仅在能找到对应观测时追加。
	if quantity != nil && available != nil {
		message += fmt.Sprintf(thresholdMetMessageFmt, quantity, available)
	}
	exceededContainers := []string{}
	containerUsage := []string{}
	podStats, ok := stats(pod)
	if !ok {
		return
	}

	// 启用 Pod 级资源且已设置时，同时报告 Pod 级别的超限情况。
	if resourceToReclaim == v1.ResourceMemory && utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResources) && resourcehelper.IsPodLevelResourcesSet(pod) {
		if podRequest, ok := pod.Spec.Resources.Requests[resourceToReclaim]; ok && podStats.Memory != nil {
			podUsage := memoryUsage(podStats.Memory)
			message += fmt.Sprintf(podMessageFmt, pod.Name, podUsage.String(), podRequest.String(), resourceToReclaim)

			annotations[OffendingPodKey] = pod.Name
			annotations[OffendingPodUsageKey] = podUsage.String()
		}
	}

	// 临时容器无法声明 resources，若把它们算进来会导致误判超限，故只统计常规与 init 容器。
	// 重启式 init 容器（restartable init）计入常规容器列表，因此一并参与统计。
	// 遍历统计时按容器名匹配，保证只比较实际存在的容器。
	// 用量按驱逐资源类型分别计算：临时存储取 rootfs+logs，内存取工作集。
	containers := pod.Spec.Containers
	if len(pod.Spec.InitContainers) != 0 {
		containers = append(containers, pod.Spec.InitContainers...)
	}
	for _, containerStats := range podStats.Containers {
		for _, container := range containers {
			if container.Name == containerStats.Name {
				requests := container.Resources.Requests[resourceToReclaim]
				var usage *resource.Quantity
				switch resourceToReclaim {
				case v1.ResourceEphemeralStorage:
					if containerStats.Rootfs != nil && containerStats.Rootfs.UsedBytes != nil && containerStats.Logs != nil && containerStats.Logs.UsedBytes != nil {
						usage = resource.NewQuantity(int64(*containerStats.Rootfs.UsedBytes+*containerStats.Logs.UsedBytes), resource.BinarySI)
					}
				case v1.ResourceMemory:
					if containerStats.Memory != nil && containerStats.Memory.WorkingSetBytes != nil {
						usage = resource.NewQuantity(int64(*containerStats.Memory.WorkingSetBytes), resource.BinarySI)
					}
				}
				if usage != nil && usage.Cmp(requests) > 0 {
					message += fmt.Sprintf(containerMessageFmt, container.Name, usage.String(), requests.String(), resourceToReclaim)
					exceededContainers = append(exceededContainers, container.Name)
					containerUsage = append(containerUsage, usage.String())
				}
				// 已找到同名容器完成比较，跳出内层循环，避免重复统计。
				// 每个统计项最多匹配一个容器，因此可安全跳出。
				break
			}
		}
	}
	annotations[OffendingContainersKey] = strings.Join(exceededContainers, ",")
	annotations[OffendingContainersUsageKey] = strings.Join(containerUsage, ",")
	annotations[StarvedResourceKey] = string(resourceToReclaim)
	return
}

// getThresholdMetInfo 返回触发驱逐的资源对应的阈值量和当前可用量。
func getThresholdMetInfo(resourceToReclaim v1.ResourceName, thresholds []evictionapi.Threshold, observations signalObservations) (quantity *resource.Quantity, available *resource.Quantity) {
	for i := range thresholds {
		threshold := thresholds[i]
		if signalToResource[threshold.Signal] == resourceToReclaim {
			observed, found := observations[threshold.Signal]
			if found {
				quantity := evictionapi.GetThresholdQuantity(threshold.Value, observed.capacity)
				return quantity, observed.available
			}
		}
	}
	return nil, nil
}
