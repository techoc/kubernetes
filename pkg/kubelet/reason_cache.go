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

package kubelet

import (
	"fmt"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/lru"

	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
)

// ReasonCache 以 <pod_UID>_<container_name> 为键，
// 缓存最近一次容器启动失败的原因，用于传播到容器状态。
// 该缓存是「尽力而为」的，原因有二：
//  1. 缓存不持久化；
//  2. 使用 LRU 缓存避免额外的垃圾回收工作，
//     因此部分条目可能在 Pod 删除前就被回收。
//
// TODO(random-liu): 使用更可靠的缓存，以便回收失败 Pod 的条目。
// TODO(random-liu): 将原因缓存迁移到更合适的位置。
type ReasonCache struct {
	cache *lru.Cache
}

// ReasonItem 是 ReasonCache 中缓存的条目。
type ReasonItem struct {
	// Err 是启动失败的错误。
	Err error
	// Message 是失败原因的描述。
	Message string
}

// maxReasonCacheEntries 是 LRU 缓存的条目上限。
// 以每节点 100 个 Pod 为目标时 1000 足够；
// 若未来支持更多 Pod，可能需要调大该值。
const maxReasonCacheEntries = 1000

// NewReasonCache 创建 ReasonCache 实例。
func NewReasonCache() *ReasonCache {
	return &ReasonCache{cache: lru.New(maxReasonCacheEntries)}
}

// composeKey 拼接 Pod UID 与容器名作为缓存键。
func (c *ReasonCache) composeKey(uid types.UID, name string) string {
	return fmt.Sprintf("%s_%s", uid, name)
}

// add 把失败原因写入缓存。
func (c *ReasonCache) add(uid types.UID, name string, reason error, message string) {
	c.cache.Add(c.composeKey(uid, name), ReasonItem{reason, message})
}

// Update 根据 SyncPodResult 更新原因缓存。
// 只有 StartContainer 动作的结果会影响缓存。
func (c *ReasonCache) Update(uid types.UID, result kubecontainer.PodSyncResult) {
	// 只关心 StartContainer 动作的结果。
	for _, r := range result.SyncResults {
		if r.Action != kubecontainer.StartContainer {
			continue
		}
		// Target 是容器名，由调用方保证类型。
		name := r.Target.(string)
		// 启动失败记录原因，成功则清除旧记录。
		if r.Error != nil {
			c.add(uid, name, r.Error, r.Message)
		} else {
			c.Remove(uid, name)
		}
	}
}

// Remove 从缓存中删除失败原因。
func (c *ReasonCache) Remove(uid types.UID, name string) {
	c.cache.Remove(c.composeKey(uid, name))
}

// Get 从缓存读取失败原因。
// 返回缓存条目以及是否命中；未命中时返回 nil 和 false。
func (c *ReasonCache) Get(uid types.UID, name string) (*ReasonItem, bool) {
	value, ok := c.cache.Get(c.composeKey(uid, name))
	if !ok {
		return nil, false
	}
	info := value.(ReasonItem)
	return &info, true
}
