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

package cache

import (
	"slices"
	"sort"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller/volume/selinuxwarning/internal/parse"
	"k8s.io/kubernetes/pkg/controller/volume/selinuxwarning/translator"
)

const (
	// 每次变更后打印卷缓存所使用的日志级别。
	dumpLogLevel = 10
)

// VolumeCache 维护 Pod 使用的卷及 SELinux 标签、变更策略等冲突检测所需状态。
type VolumeCache interface {
	// AddVolume 向缓存添加一个卷，并返回本次新增或更新引发的冲突列表。
	AddVolume(logger klog.Logger, volumeName v1.UniqueVolumeName, podKey cache.ObjectName, seLinuxLabel string, changePolicy v1.PodSELinuxChangePolicy, csiDriver string) []Conflict

	// DeletePod 从缓存中移除指定 Pod，并清理因此变空的卷、索引和冲突记录。
	DeletePod(logger klog.Logger, podKey cache.ObjectName)

	// GetPodsForCSIDriver 返回使用指定 CSI 驱动卷的所有 Pod。
	// 当 CSIDriver 修改 spec.seLinuxMount 时，控制器需要重新评估使用该驱动卷的所有 Pod。
	// 无需跟踪 in-tree 卷插件，因为它们的 SELinux 支持不会动态变化。
	GetPodsForCSIDriver(driverName string) []cache.ObjectName

	// GetConflicts 返回当前所有活跃的冲突（双向成对）。
	GetConflicts(logger klog.Logger) []Conflict
}

// volumeCache 保存 Pod 使用的所有卷及控制器需要跟踪的属性，
// 例如 SELinux 标签和 SELinuxChangePolicy。
type volumeCache struct {
	mutex             sync.RWMutex
	seLinuxTranslator *translator.ControllerSELinuxTranslator
	// 所有现存 Pod 使用的全部卷。
	volumes map[v1.UniqueVolumeName]usedVolume
	// 反向索引：记录每个 Pod 使用的卷列表。
	// 删除 Pod 时借助该索引只遍历相关的卷。
	podToVolumes map[cache.ObjectName]sets.Set[v1.UniqueVolumeName]
	// 每个卷当前的活跃冲突（双向、成对对称）。
	conflicts map[v1.UniqueVolumeName][]Conflict
}

var _ VolumeCache = &volumeCache{}

// NewVolumeLabelCache 创建一个新的 VolumeCache 实例。
func NewVolumeLabelCache(seLinuxTranslator *translator.ControllerSELinuxTranslator) VolumeCache {
	return &volumeCache{
		seLinuxTranslator: seLinuxTranslator,
		volumes:           make(map[v1.UniqueVolumeName]usedVolume),
		podToVolumes:      make(map[cache.ObjectName]sets.Set[v1.UniqueVolumeName]),
		conflicts:         make(map[v1.UniqueVolumeName][]Conflict),
	}
}

// usedVolume 表示被一个或多个现存 Pod 使用的卷。
// 其中保存这些 Pod 的信息，用于检测冲突并生成事件。
type usedVolume struct {
	csiDriver string
	// 使用该卷的 Pod 列表，以 Pod key 为索引以便快速删除。
	pods map[cache.ObjectName]podInfo
}

// podInfo 记录使用某个卷的 Pod 的相关信息。
type podInfo struct {
	// 要在 Pod 中应用到卷上的 SELinux 标签，
	// 既可以作为 mount option 传入，也可以由容器运行时递归设置。
	seLinuxLabel string
	// 预先解析的 SELinux 标签各部分，用于快速检测冲突。
	seLinuxParts [4]string
	// Pod 的 SELinuxChangePolicy。
	changePolicy v1.PodSELinuxChangePolicy
}

// newPodInfoListForPod 为一个 Pod 构造仅含单个条目的 podInfo map。
func newPodInfoListForPod(podKey cache.ObjectName, seLinuxLabel string, changePolicy v1.PodSELinuxChangePolicy) map[cache.ObjectName]podInfo {
	return map[cache.ObjectName]podInfo{
		podKey: {
			seLinuxLabel: seLinuxLabel,
			seLinuxParts: parse.ParseSELinuxLabel(seLinuxLabel),
			changePolicy: changePolicy,
		},
	}
}

// AddVolume 向缓存添加一个卷，并返回本次新增或更新引发的冲突列表。
func (c *volumeCache) AddVolume(logger klog.Logger, volumeName v1.UniqueVolumeName, podKey cache.ObjectName, label string, changePolicy v1.PodSELinuxChangePolicy, csiDriver string) []Conflict {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	defer c.dump(logger)

	conflicts := make([]Conflict, 0)

	volume, found := c.volumes[volumeName]
	if !found {
		// 首次见到该卷，尚无其他 Pod 使用，因此不可能产生冲突。
		volume = usedVolume{
			csiDriver: csiDriver,
			pods:      newPodInfoListForPod(podKey, label, changePolicy),
		}
		c.volumes[volumeName] = volume

		// 维护反向索引，供后续 Pod 删除时快速定位卷。
		c.registerPodVolume(podKey, volumeName)
		return conflicts
	}

	// 卷已存在：需要与卷上已有 Pod 逐一比较以检测冲突。
	podInfo := podInfo{
		seLinuxLabel: label,
		seLinuxParts: parse.ParseSELinuxLabel(label),
		changePolicy: changePolicy,
	}
	oldPodInfo, found := volume.pods[podKey]
	if found && oldPodInfo == podInfo {
		// Pod 也在缓存中且属性未变化，说明上次加入或更新时已报告过冲突。
		// 直接返回可避免重复上报相同事件。
		return conflicts
	}

	// 用最新信息覆盖缓存中该 Pod 的记录。
	volume.pods[podKey] = podInfo

	// 确保反向索引包含该 Pod 与卷的关联。
	c.registerPodVolume(podKey, volumeName)

	// 与卷上其他 Pod 比较，生成冲突事件。
	for otherPodKey, otherPodInfo := range volume.pods {
		if otherPodInfo.changePolicy != changePolicy {
			// 冲突是双向的，需为两个 Pod 各生成一条事件。
			conflicts = append(conflicts, Conflict{
				PropertyName:       "SELinuxChangePolicy",
				EventReason:        "SELinuxChangePolicyConflict",
				Pod:                podKey,
				PropertyValue:      string(changePolicy),
				OtherPod:           otherPodKey,
				OtherPropertyValue: string(otherPodInfo.changePolicy),
			}, Conflict{
				PropertyName:       "SELinuxChangePolicy",
				EventReason:        "SELinuxChangePolicyConflict",
				Pod:                otherPodKey,
				PropertyValue:      string(otherPodInfo.changePolicy),
				OtherPod:           podKey,
				OtherPropertyValue: string(changePolicy),
			})

		}
		if c.seLinuxTranslator.ConflictsParsed(otherPodInfo.seLinuxParts, podInfo.seLinuxParts) {
			// 冲突是双向的，需为两个 Pod 各生成一条事件。
			conflicts = append(conflicts, Conflict{
				PropertyName:       "SELinuxLabel",
				EventReason:        "SELinuxLabelConflict",
				Pod:                podKey,
				PropertyValue:      label,
				OtherPod:           otherPodKey,
				OtherPropertyValue: otherPodInfo.seLinuxLabel,
			}, Conflict{
				PropertyName:       "SELinuxLabel",
				EventReason:        "SELinuxLabelConflict",
				Pod:                otherPodKey,
				PropertyValue:      otherPodInfo.seLinuxLabel,
				OtherPod:           podKey,
				OtherPropertyValue: label,
			})
		}
	}
	// 更新该卷的冲突缓存：先清除该 Pod 的过期冲突，再写入本次检测结果。
	volumeConflicts := c.conflicts[volumeName]
	updated := make([]Conflict, 0, len(volumeConflicts))
	for _, existing := range volumeConflicts {
		if existing.Pod != podKey && existing.OtherPod != podKey {
			updated = append(updated, existing)
		}
	}
	updated = append(updated, conflicts...)
	if len(updated) == 0 {
		delete(c.conflicts, volumeName)
	} else {
		c.conflicts[volumeName] = updated
	}

	return conflicts
}

// DeletePod 从缓存中移除指定 Pod，并清理因此变空的卷、索引和冲突记录。
func (c *volumeCache) DeletePod(logger klog.Logger, podKey cache.ObjectName) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	defer c.dump(logger)

	for volumeName := range c.podToVolumes[podKey] {
		conflicts, found := c.conflicts[volumeName]
		if !found {
			continue
		}
		updated := make([]Conflict, 0, len(conflicts))
		for _, existing := range conflicts {
			// 仅保留与待删除 Pod 无关的冲突。
			if existing.Pod != podKey && existing.OtherPod != podKey {
				updated = append(updated, existing)
			}
		}
		if len(updated) == 0 {
			delete(c.conflicts, volumeName)
		} else {
			c.conflicts[volumeName] = updated
		}
	}

	// 借助反向索引只遍历该 Pod 真正使用的卷，避免全表扫描。
	for volumeName := range c.podToVolumes[podKey] {
		volume, found := c.volumes[volumeName]
		if !found {
			continue
		}
		delete(volume.pods, podKey)
		if len(volume.pods) == 0 {
			delete(c.volumes, volumeName)
		}
	}
	delete(c.podToVolumes, podKey)
}

// registerPodVolume 将卷加入 Pod 的卷索引。
// 调用方必须持有 c.mutex，函数内部不负责加锁。
func (c *volumeCache) registerPodVolume(podKey cache.ObjectName, volumeName v1.UniqueVolumeName) {
	if podVolumes, ok := c.podToVolumes[podKey]; ok {
		podVolumes.Insert(volumeName)
	} else {
		c.podToVolumes[podKey] = sets.New(volumeName)
	}
}

// dump 在调试日志级别开启时按稳定顺序打印卷缓存与反向索引，便于比对不同时刻的状态。
func (c *volumeCache) dump(logger klog.Logger) {
	if !logger.V(dumpLogLevel).Enabled() {
		return
	}
	logger.Info("VolumeCache dump:")

	// 排序卷 ID，保证日志输出稳定可比对。
	volumeIDs := make([]v1.UniqueVolumeName, 0, len(c.volumes))
	for volumeID := range c.volumes {
		volumeIDs = append(volumeIDs, volumeID)
	}
	slices.Sort(volumeIDs)
	for _, volumeID := range volumeIDs {
		volume := c.volumes[volumeID]
		logger.Info("Cached volume", "volume", volumeID, "csiDriver", volume.csiDriver)

		// 排序 Pod key，保证日志输出顺序稳定。
		podKeys := make([]cache.ObjectName, 0, len(volume.pods))
		for podKey := range volume.pods {
			podKeys = append(podKeys, podKey)
		}
		sort.Slice(podKeys, func(i, j int) bool {
			return podKeys[i].String() < podKeys[j].String()
		})
		for _, podKey := range podKeys {
			podInfo := volume.pods[podKey]
			logger.Info("  pod", "pod", podKey, "seLinuxLabel", podInfo.seLinuxLabel, "changePolicy", podInfo.changePolicy)
		}
	}

	// 收集全部 Pod 并排序，然后打印各自关联的卷。
	podKeys := make([]cache.ObjectName, 0, len(c.podToVolumes))
	for podKey := range c.podToVolumes {
		podKeys = append(podKeys, podKey)
	}
	sort.Slice(podKeys, func(i, j int) bool {
		return podKeys[i].String() < podKeys[j].String()
	})

	logger.Info("VolumeCache reverse index dump:")
	for _, podKey := range podKeys {
		podVolumes := sets.List(c.podToVolumes[podKey])
		slices.Sort(podVolumes)
		logger.Info("  pod", "pod", podKey, "volumes", podVolumes)
	}
}

// GetPodsForCSIDriver 返回使用指定 CSI 驱动卷的所有 Pod。
func (c *volumeCache) GetPodsForCSIDriver(driverName string) []cache.ObjectName {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	var pods []cache.ObjectName
	for _, volume := range c.volumes {
		if volume.csiDriver != driverName {
			continue
		}
		for podKey := range volume.pods {
			pods = append(pods, podKey)
		}
	}
	return pods
}

// GetConflicts 返回当前所有活跃的冲突（双向、成对对称）。
func (c *volumeCache) GetConflicts(logger klog.Logger) []Conflict {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	logger.V(4).Info("Scraping conflicts")
	c.dump(logger)

	// 用 set 去重，避免同一冲突因多个卷被重复收集。
	result := sets.New[Conflict]()
	for _, volConflicts := range c.conflicts {
		result.Insert(volConflicts...)
	}
	return result.UnsortedList()
}
