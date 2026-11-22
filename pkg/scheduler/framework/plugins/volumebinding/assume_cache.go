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

// Package volumebinding 实现卷绑定调度插件，在调度过程中处理 Pod 的持久卷声明（PVC）。
//
// 插件在 Filter 阶段评估节点是否满足 Pod 的卷需求，在 Reserve 阶段通过 assume cache
// 临时记录 PV/PVC 的绑定意图（避免同一卷被其他 Pod 重复绑定），最后在 PreBind 阶段
// 真正执行绑定并等待 PV 控制器完成。若绑定失败或超时，Unreserve 会回滚 assume 状态，
// 使 Pod 可以被重新调度。
package volumebinding

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
	storagehelpers "k8s.io/component-helpers/storage/volume"
	"k8s.io/klog/v2"
)

// PVAssumeCache 是 PersistentVolume 对象的 AssumeCache。
// 它在 informer 缓存之上叠加一层"已假定绑定"的本地视图，使调度器在同一调度周期内
// 能立即看到刚假定绑定的 PV，无需等待 API server 的实际更新。
type PVAssumeCache struct {
	*passiveAssumeCache[*v1.PersistentVolume]
}

// pvStorageClassIndexFunc 为 PV 建立按 StorageClass 名称索引的键值函数。
// 索引用于快速查找某个 StorageClass 下可用的 PV；传入对象类型不符时返回错误，
// 以便尽早暴露 informer 数据异常。
func pvStorageClassIndexFunc(obj interface{}) ([]string, error) {
	if pv, ok := obj.(*v1.PersistentVolume); ok {
		return []string{storagehelpers.GetPersistentVolumeClass(pv)}, nil
	}
	return nil, fmt.Errorf("object is not a v1.PersistentVolume: %v", obj)
}

// storageClassIndex 是 PV 按 StorageClass 名称索引的索引名。
const storageClassIndex = "storageclass"

// NewPVAssumeCache 创建 PV assume cache，并为 informer 注册 StorageClass 索引。
func NewPVAssumeCache(logger klog.Logger, informer informer) (PVAssumeCache, error) {
	logger = klog.LoggerWithName(logger, "pv-cache")
	err := informer.GetIndexer().AddIndexers(map[string]cache.IndexFunc{
		storageClassIndex: pvStorageClassIndexFunc,
	})
	if err != nil {
		// 同一个 informer 可能被多个 profile 的 PVAssumeCache 共享，索引重复注册属正常情况。
		// 仅当索引确实不存在时才视为真正的错误返回。
		if informer.GetIndexer().GetIndexers()[storageClassIndex] == nil {
			return PVAssumeCache{}, err
		}
	}
	cache, err := newAssumeCache[*v1.PersistentVolume](logger, informer, schema.GroupResource{Resource: "persistentvolumes"})
	return PVAssumeCache{cache}, err
}

// ListPVs 列出指定 StorageClass 下的 PV（包含已假定绑定的 PV）。
// 调度器不会修改 PV 的 StorageClass，因此索引键始终可靠；将假定 PV 一并列出，
// 可防止同一个 PVC 在下一个调度周期被绑定到另一个 PV 上。
func (c PVAssumeCache) ListPVs(storageClassName string) ([]*v1.PersistentVolume, error) {
	// 调度器不会修改 PV 的 StorageClass，因此按类名查询的结果始终可靠。
	// 必须把假定绑定的 PV 也包含进来，否则同一 PVC 可能在下一个调度周期被绑定到其他 PV。
	return c.ByIndex(storageClassIndex, storageClassName)
}

// PVCAssumeCache 是 PersistentVolumeClaim 对象的 AssumeCache。
// 与 PVAssumeCache 类似，让调度器立即看到本周期内假定绑定的 PVC 状态。
type PVCAssumeCache struct {
	*passiveAssumeCache[*v1.PersistentVolumeClaim]
}

// NewPVCAssumeCache 创建 PVC assume cache。
func NewPVCAssumeCache(logger klog.Logger, informer informer) (PVCAssumeCache, error) {
	logger = klog.LoggerWithName(logger, "pvc-cache")
	cache, err := newAssumeCache[*v1.PersistentVolumeClaim](logger, informer, schema.GroupResource{Resource: "persistentvolumeclaims"})
	return PVCAssumeCache{cache}, err
}
