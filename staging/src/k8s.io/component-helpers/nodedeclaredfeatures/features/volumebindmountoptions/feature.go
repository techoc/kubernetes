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

// Package volumebindmountoptions 实现“卷绑定挂载选项”这一节点声明特性：
// 依据特性门控与容器运行时上报的挂载选项能力，判断节点是否支持该能力，
// 并在 Pod 使用相关卷时给出调度/更新期的能力要求。
package volumebindmountoptions

import (
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/component-helpers/nodedeclaredfeatures/types"
)

// 编译期检查实现是否仍满足 Feature 接口，签名漂移在本包就能发现。
var _ types.Feature = &volumeBindMountOptionsFeature{}

const (
	// VolumeBindMountOptionsFeatureGate 是卷绑定挂载选项能力的特性门名。
	VolumeBindMountOptionsFeatureGate = "VolumeBindMountOptions"
	// VolumeBindMountOptionsFeatureName 是节点对外声明的能力名称。
	VolumeBindMountOptionsFeatureName = "VolumeBindMountOptions"
)

// Feature 是卷绑定挂载选项特性的实现单例。
// 无状态单例，供框架注册；所有调用方共享同一份实现，避免各自构造出现偏差。
var Feature = &volumeBindMountOptionsFeature{}

// volumeBindMountOptionsFeature 为空结构体，所有判断都只依赖传入参数。
type volumeBindMountOptionsFeature struct{}

// Name 返回对外声明的特性名。
func (f *volumeBindMountOptionsFeature) Name() string {
	return VolumeBindMountOptionsFeatureName
}

// Requirements 返回节点必要前提：特性门必须打开，且 CRI runtime 必须支持挂载选项。
// 运行时能力是硬依赖：光打开 feature gate、CRI 却不支持挂载选项，
// 这个能力也不能对外声明，否则调度会把 Pod 送到执行不了的机器上。
func (f *volumeBindMountOptionsFeature) Requirements() *types.FeatureRequirements {
	return &types.FeatureRequirements{
		EnabledFeatureGates: []string{VolumeBindMountOptionsFeatureGate},
		RequiredRuntimeFeatures: &types.RuntimeFeatures{
			MountOptions: true,
		},
	}
}

// Discover 判断节点是否声明该能力。cfg 携带开关与运行时能力。
// gate 与运行时能力必须同时成立；先查 gate 还能让关闭特性时
// 完全不依赖 RuntimeFeatures 的探测结果。
func (f *volumeBindMountOptionsFeature) Discover(cfg *types.NodeConfiguration) bool {
	if !cfg.FeatureGates.Enabled(VolumeBindMountOptionsFeatureGate) {
		return false
	}
	return cfg.RuntimeFeatures.MountOptions
}

// InferForScheduling 扫描 Pod 中所有容器的 VolumeMounts：
// 只要任一挂载配置了 BindMountOptions 就说明 Pod 依赖该能力。
// 三类容器都要检查：ephemeral container 同样能带挂选项，
// 漏掉它会导致调试容器被调度到不支持该能力的节点上。
func (f *volumeBindMountOptionsFeature) InferForScheduling(podInfo *types.PodInfo) bool {
	for i := range podInfo.Spec.Containers {
		for j := range podInfo.Spec.Containers[i].VolumeMounts {
			if len(podInfo.Spec.Containers[i].VolumeMounts[j].BindMountOptions) > 0 {
				return true
			}
		}
	}
	for i := range podInfo.Spec.InitContainers {
		for j := range podInfo.Spec.InitContainers[i].VolumeMounts {
			if len(podInfo.Spec.InitContainers[i].VolumeMounts[j].BindMountOptions) > 0 {
				return true
			}
		}
	}
	for i := range podInfo.Spec.EphemeralContainers {
		for j := range podInfo.Spec.EphemeralContainers[i].VolumeMounts {
			if len(podInfo.Spec.EphemeralContainers[i].VolumeMounts[j].BindMountOptions) > 0 {
				return true
			}
		}
	}
	return false
}

// InferForUpdate 恒返回 false：VolumeMounts 是不可变字段，
// 更新不会引入新的挂载选项需求。
func (f *volumeBindMountOptionsFeature) InferForUpdate(oldPodInfo, newPodInfo *types.PodInfo) bool {
	return false
}

// MaxVersion 返回 nil：此能力没有 GA 上界，始终需要向调度器声明。
func (f *volumeBindMountOptionsFeature) MaxVersion() *version.Version {
	return nil
}
