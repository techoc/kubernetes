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

package fuzzer

import (
	"fmt"

	"sigs.k8s.io/randfill"

	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/storage"
)

// Funcs 返回 storage API 组使用的 fuzzer 函数集合。
// 随机字节难以命中枚举常量的合法取值，因此这里针对性覆盖枚举字段和默认值字段。
var Funcs = func(codecs runtimeserializer.CodecFactory) []interface{} {
	return []interface{}{
		func(obj *storage.StorageClass, c randfill.Continue) {
			c.FillNoCustom(obj) // 填充除本函数外的其它字段，避免递归调用自身
			// 回收策略和绑定模式都是枚举指针，从合法常量中随机选取而不是依赖随机字节。
			reclamationPolicies := []api.PersistentVolumeReclaimPolicy{api.PersistentVolumeReclaimDelete, api.PersistentVolumeReclaimRetain}
			obj.ReclaimPolicy = &reclamationPolicies[c.Rand.Intn(len(reclamationPolicies))]
			bindingModes := []storage.VolumeBindingMode{storage.VolumeBindingImmediate, storage.VolumeBindingWaitForFirstConsumer}
			obj.VolumeBindingMode = &bindingModes[c.Rand.Intn(len(bindingModes))]
		},
		func(obj *storage.CSIDriver, c randfill.Continue) {
			c.FillNoCustom(obj) // 填充除本函数外的其它字段，避免递归调用自身

			// 覆盖卷生命周期模式的各种组合（含 nil、空列表、非法值），以测试默认填充与校验路径。
			switch c.Rand.Intn(7) {
			case 0:
				obj.Spec.VolumeLifecycleModes = nil
			case 1:
				obj.Spec.VolumeLifecycleModes = []storage.VolumeLifecycleMode{}
			case 2:
				// 故意生成非法模式，用于验证校验器能识别并拒绝未知取值。
				obj.Spec.VolumeLifecycleModes = []storage.VolumeLifecycleMode{
					storage.VolumeLifecycleMode(fmt.Sprintf("%d", c.Rand.Int31())),
				}
			case 3:
				obj.Spec.VolumeLifecycleModes = []storage.VolumeLifecycleMode{
					storage.VolumeLifecyclePersistent,
				}
			case 4:
				obj.Spec.VolumeLifecycleModes = []storage.VolumeLifecycleMode{
					storage.VolumeLifecycleEphemeral,
				}
			case 5:
				obj.Spec.VolumeLifecycleModes = []storage.VolumeLifecycleMode{
					storage.VolumeLifecyclePersistent,
					storage.VolumeLifecycleEphemeral,
				}
			case 6:
				obj.Spec.VolumeLifecycleModes = []storage.VolumeLifecycleMode{
					storage.VolumeLifecycleEphemeral,
					storage.VolumeLifecyclePersistent,
				}
			}

			// 未设置的字段按默认值填充，保持模糊生成对象与经过默认化处理的对象语义一致。
			// match defaulting
			if obj.Spec.AttachRequired == nil {
				obj.Spec.AttachRequired = new(bool)
				*(obj.Spec.AttachRequired) = true
			}
			if obj.Spec.PodInfoOnMount == nil {
				obj.Spec.PodInfoOnMount = new(bool)
				*(obj.Spec.PodInfoOnMount) = false
			}
			if obj.Spec.StorageCapacity == nil {
				obj.Spec.StorageCapacity = new(bool)
				*(obj.Spec.StorageCapacity) = false
			}
			if obj.Spec.FSGroupPolicy == nil {
				obj.Spec.FSGroupPolicy = new(storage.FSGroupPolicy)
				*obj.Spec.FSGroupPolicy = storage.ReadWriteOnceWithFSTypeFSGroupPolicy
			}
			if obj.Spec.RequiresRepublish == nil {
				obj.Spec.RequiresRepublish = new(bool)
				*(obj.Spec.RequiresRepublish) = false
			}
			if len(obj.Spec.VolumeLifecycleModes) == 0 {
				obj.Spec.VolumeLifecycleModes = []storage.VolumeLifecycleMode{
					storage.VolumeLifecyclePersistent,
				}
			}
			if obj.Spec.SELinuxMount == nil {
				obj.Spec.SELinuxMount = new(bool)
				*(obj.Spec.SELinuxMount) = false
			}
			if obj.Spec.PreventPodSchedulingIfMissing == nil {
				obj.Spec.PreventPodSchedulingIfMissing = new(bool)
				*(obj.Spec.PreventPodSchedulingIfMissing) = false
			}
		},
	}
}
