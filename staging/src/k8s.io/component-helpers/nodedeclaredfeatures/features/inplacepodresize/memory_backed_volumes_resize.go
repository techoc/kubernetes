/*
Copyright The Kubernetes Authors.

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

// 本文件声明 InPlacePodVerticalScalingMemoryBackedVolumes 特性：
// 判断节点是否支持原地调整 memory-backed emptyDir 的容量。
// 与传输类能力不同，它取决于具体 pod 的卷定义，因此只在“更新推断”中返回 true。
package inplacepodresize

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/component-helpers/nodedeclaredfeatures/types"
)

// 编译期断言，确保实现仍满足统一的 Feature 接口。
// 这里引用的是同包 podLevelResourcesResizeFeature 类型（接口一致性检查），
// 接口一旦新增方法就会在此报错，避免漏改后到运行时才暴露。
var _ types.Feature = &podLevelResourcesResizeFeature{}

// IPPRMemoryBackedVolumesFeatureGate 是内存卷原地扩缩容对应的 feature gate。
//
// gate 名直接作为对外声明的特性名，省去维护两个常量产生不一致的风险。
const IPPRMemoryBackedVolumesFeatureGate = "InPlacePodVerticalScalingMemoryBackedVolumes"

// MemoryBackedVolumesResizeFeature 是内存卷原地扩缩容特性的实现。
//
// 导出为无状态单例：该特性的所有方法都只依赖参数，不需要为每次调用分配对象。
var MemoryBackedVolumesResizeFeature = &memoryBackedVolumesResizeFeature{}

// 结构体不携带字段：节点级开关从 NodeConfiguration 读，pod 级需求从 PodInfo 推导。
type memoryBackedVolumesResizeFeature struct{}

// Name 返回特性名，与 feature gate 保持一致，便于在需求声明和日志中互相对照。
func (f *memoryBackedVolumesResizeFeature) Name() string {
	return IPPRMemoryBackedVolumesFeatureGate
}

// Discover 只看 gate：节点是否支持该能力与具体 runtime feature 无关，
// 只要 kubelet 打开了特性开关就认为可用。
func (f *memoryBackedVolumesResizeFeature) Discover(cfg *types.NodeConfiguration) bool {
	return cfg.FeatureGates.Enabled(IPPRMemoryBackedVolumesFeatureGate)
}

// Requirements 把 gate 列入必要条件：调度侧据此判断节点是否声明了该能力。
func (f *memoryBackedVolumesResizeFeature) Requirements() *types.FeatureRequirements {
	return &types.FeatureRequirements{
		EnabledFeatureGates: []string{IPPRMemoryBackedVolumesFeatureGate},
	}
}

// InferForScheduling 恒返回 false：该特性只在 Pod 更新时才有意义。
// 新建 Pod 时卷大小由正常的调度与绑定流程决定，不需要“原地扩容”能力。
func (f *memoryBackedVolumesResizeFeature) InferForScheduling(podInfo *types.PodInfo) bool {
	return false
}

// InferForUpdate 仅识别一种情形：同名 memory-backed emptyDir 的 SizeLimit 发生变化。
// 其他不合法改动（增删卷、改名、改介质）交给常规校验报错，这里刻意不返回 true，
// 避免把非法请求误判成“需要该特性”而掩盖真正的校验错误。
func (f *memoryBackedVolumesResizeFeature) InferForUpdate(oldPodInfo, newPodInfo *types.PodInfo) bool {
	if len(oldPodInfo.Spec.Volumes) != len(newPodInfo.Spec.Volumes) {
		// 原地扩缩容只能改大小，不能增删卷；数量不等属于非法 patch，
		// 交给标准校验流程拒绝，本特性不对此声明需求。
		return false
	}

	for i := range newPodInfo.Spec.Volumes {
		// 按位置配对新旧卷：数量已在上面对齐，卷顺序由 PodSpec 保证稳定。
		newVol := &newPodInfo.Spec.Volumes[i]
		oldVol := &oldPodInfo.Spec.Volumes[i]

		if newVol.Name != oldVol.Name {
			// 卷名变了属于非法修改，交给标准校验处理。
			continue
		}
		if newVol.EmptyDir == nil || newVol.EmptyDir.Medium != v1.StorageMediumMemory {
			// 不是 memory-backed emptyDir；改卷类型同样由标准校验拒绝。
			continue
		}
		if newVol.EmptyDir.SizeLimit == nil || oldVol.EmptyDir.SizeLimit == nil {
			// 任一侧没有 SizeLimit，说明属于“增删限制”而非扩容，仍由标准校验处理。
			continue
		}
		// 只有两侧都明确设置了 SizeLimit 且值不同，才真正需要该能力：
		// 上面的分支已经排除了卷类型、名称、有无 sizeLimit 的非扩容改动。
		if newVol.EmptyDir.SizeLimit.Cmp(*oldVol.EmptyDir.SizeLimit) != 0 {
			return true
		}
	}

	return false
}

// MaxVersion 返回 nil：原地扩容能力不因 kubelet 版本而有差异。
func (f *memoryBackedVolumesResizeFeature) MaxVersion() *version.Version {
	return nil
}
