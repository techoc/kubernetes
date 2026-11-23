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

package inplacepodresize

import (
	"reflect"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/component-helpers/nodedeclaredfeatures/types"
)

// 编译期断言让接口变动立刻暴露为编译错误，避免某个实现被漏改后
// 直到节点声明特性时才在运行时失败。
// 注：下面这个断言引用的是同包另一个特性实现，不要误以为是当前文件的类型。
var _ types.Feature = &podLevelResourcesResizeFeature{}

// IPPRInitContainersFeatureGate 是 init container 原地扩缩容对应的 feature gate。
// 特性门控名必须与 kubelet 侧使用的字符串完全一致，否则节点声明的能力
// 和调度器/控制器判断的依据会对不上。这里作为单一常量来源避免拼写漂移。
const IPPRInitContainersFeatureGate = "InPlacePodVerticalScalingInitContainers"

// NonSidecarInitContainerResizeFeature 是非 sidecar init container 原地扩缩容特性的实现。
// 导出一个无状态单例即可：该特性的判断只依赖传入的配置和 Pod 信息，
// 没有需要按实例保存的状态，省去调用方反复构造对象。
var NonSidecarInitContainerResizeFeature = &nonSidecarInitContainerResizeFeature{}

// 类型本身不导出，是为了让使用者只能拿到上面的单例，
// 避免各自构造实现导致行为不一致。
type nonSidecarInitContainerResizeFeature struct{}

// Name 返回对外声明的特性名，与 init container 扩缩容的 feature gate 一致。
func (f *nonSidecarInitContainerResizeFeature) Name() string {
	return IPPRInitContainersFeatureGate
}

// Discover 直接以节点上的特性门控作为能力判定：这个特性的支持范围
// 完全由 kubelet 启动时是否打开门控决定，没有必要再引入额外的探测机制。
func (f *nonSidecarInitContainerResizeFeature) Discover(cfg *types.NodeConfiguration) bool {
	return cfg.FeatureGates.Enabled(IPPRInitContainersFeatureGate)
}

// Requirements 把门控列成显式依赖，便于上层据此判断节点是否满足
// 该特性所需的配置，而不是到 Discover 里再去解析一遍门控状态。
func (f *nonSidecarInitContainerResizeFeature) Requirements() *types.FeatureRequirements {
	return &types.FeatureRequirements{
		EnabledFeatureGates: []string{IPPRInitContainersFeatureGate},
	}
}

// InferForScheduling 恒返回 false：该特性只在 Pod 更新时才有意义，
// 调度新 Pod 时不存在需要原地调整的 init container。
func (f *nonSidecarInitContainerResizeFeature) InferForScheduling(podInfo *types.PodInfo) bool {
	return false
}

// InferForUpdate 按位置对齐新旧 InitContainers：Pod 更新时顺序是稳定的，
// 所以不需要按名字建索引。sidecar 由 kubelet 单独管理，不在本特性范围内，
// 因此只比较非 sidecar 的资源字段；其他字段变化与本特性无关。
func (f *nonSidecarInitContainerResizeFeature) InferForUpdate(oldPodInfo, newPodInfo *types.PodInfo) bool {
	for i, ctr := range oldPodInfo.Spec.InitContainers {
		if !isSidecar(ctr) && !reflect.DeepEqual(ctr.Resources, newPodInfo.Spec.InitContainers[i].Resources) {
			return true
		}
	}
	return false
}

// MaxVersion 返回 nil 表示没有版本上限：该特性由门控开关决定，
// 不随某个具体 Kubernetes 版本自动失效。
func (f *nonSidecarInitContainerResizeFeature) MaxVersion() *version.Version {
	return nil
}

// isSidecar 以 RestartPolicy=Always 判定 sidecar，这是 Kubernetes 对 sidecar
// 容器的既有约定；这类 init container 的资源调整走另一套流程。
func isSidecar(initContainer core.Container) bool {
	return initContainer.RestartPolicy != nil && *initContainer.RestartPolicy == core.ContainerRestartPolicyAlways
}
