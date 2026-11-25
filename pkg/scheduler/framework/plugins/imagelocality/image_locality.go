/*
Copyright 2019 The Kubernetes Authors.

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

package imagelocality

import (
	"context"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
)

// 两个阈值界定镜像打分的范围。它们对应镜像仓库中压缩存储容器的常见大小区间，
// dockerhub 上约 90% 的镜像落在此范围内。
const (
	mb                    int64 = 1024 * 1024
	minThreshold          int64 = 23 * mb
	maxContainerThreshold int64 = 1000 * mb
)

// ImageLocality 是一个打分插件，倾向选择已经缓存了 Pod 所需容器镜像的节点。
type ImageLocality struct {
	handle fwk.Handle
}

var _ fwk.ScorePlugin = &ImageLocality{}
var _ fwk.SignPlugin = &ImageLocality{}

// Name 是该插件在插件注册表和配置文件中使用的名称。
const Name = names.ImageLocality

// Name 返回插件名称，用于日志等场景。
func (pl *ImageLocality) Name() string {
	return Name
}

// SignPod 基于容器镜像名称生成签名片段。
func (pl *ImageLocality) SignPod(ctx context.Context, pod *v1.Pod) ([]fwk.SignFragment, *fwk.Status) {
	nameSet := sets.New[string]()

	// 普通容器和 init 容器的镜像都要计入，去重后再签名。
	containers := []v1.Container{}
	containers = append(containers, pod.Spec.Containers...)
	containers = append(containers, pod.Spec.InitContainers...)

	for _, container := range containers {
		nameSet.Insert(normalizedImageName(container.Image))
	}
	// sets.List 会排序，保证签名结果稳定。
	names := sets.List(nameSet)
	return []fwk.SignFragment{{Key: fwk.ImageNamesSignerName, Value: names}}, nil
}

// Score 在 Score 扩展点调用，按节点上已有镜像的情况打分。
func (pl *ImageLocality) Score(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	// 节点总数是镜像分布度的分母，每次从快照获取以反映最新集群规模。
	nodeInfos, err := pl.handle.SnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return 0, fwk.AsStatus(err)
	}
	totalNumNodes := len(nodeInfos)

	imageScores, imageCount := sumImageScores(nodeInfo, pod, totalNumNodes)
	score := calculatePriority(imageScores, imageCount)

	return score, nil
}

// ScoreExtensions 返回打分插件的扩展；本插件不需要归一化等后处理。
func (pl *ImageLocality) ScoreExtensions() fwk.ScoreExtensions {
	return nil
}

// New 初始化并返回一个 ImageLocality 插件实例。
func New(_ context.Context, _ runtime.Object, h fwk.Handle) (fwk.Plugin, error) {
	return &ImageLocality{handle: h}, nil
}

// calculatePriority 根据节点上已有镜像的得分总和与 Pod 镜像数计算节点得分。
// 先把得分截断到阈值区间，再按比例映射到最大分值，避免个别超大镜像主导结果。
func calculatePriority(sumScores int64, imageCount int) int64 {
	maxThreshold := maxContainerThreshold * int64(imageCount)
	if sumScores < minThreshold {
		sumScores = minThreshold
	} else if sumScores > maxThreshold {
		sumScores = maxThreshold
	}

	return fwk.MaxScore * (sumScores - minThreshold) / (maxThreshold - minThreshold)
}

// sumImageScores 返回 Pod spec 中所有镜像来源（普通容器、init 容器和镜像卷）的得分总和与数量。
// 节点上已存在的镜像按其大小计原始分，再经 scaledImageScore 按分布度缩放。
// 原始分和镜像数量随后用于计算最终得分。
func sumImageScores(nodeInfo fwk.NodeInfo, pod *v1.Pod, totalNumNodes int) (int64, int) {
	var sum int64

	for _, container := range pod.Spec.InitContainers {
		if state, ok := nodeInfo.GetImageStates()[normalizedImageName(container.Image)]; ok {
			sum += scaledImageScore(state, totalNumNodes)
		}
	}

	for _, container := range pod.Spec.Containers {
		if state, ok := nodeInfo.GetImageStates()[normalizedImageName(container.Image)]; ok {
			sum += scaledImageScore(state, totalNumNodes)
		}
	}

	imageCount := len(pod.Spec.InitContainers) + len(pod.Spec.Containers)

	for _, volume := range pod.Spec.Volumes {
		if volume.Image == nil {
			continue
		}
		if state, ok := nodeInfo.GetImageStates()[normalizedImageName(volume.Image.Reference)]; ok {
			sum += scaledImageScore(state, totalNumNodes)
		}
		imageCount++
	}
	return sum, imageCount
}

// scaledImageScore 按镜像分布度自适应地缩放镜像得分。
// 以镜像大小为基准分，再乘以镜像“扩散”到的节点比例。
// 这样可缓解镜像亲和性导致的“热点节点”问题：Pod 都涌向缓存了镜像的少数节点。
func scaledImageScore(imageState *fwk.ImageStateSummary, totalNumNodes int) int64 {
	spread := float64(imageState.NumNodes) / float64(totalNumNodes)
	return int64(float64(imageState.Size) * spread)
}

// normalizedImageName 返回符合 CRI 规范的镜像名（缺少 tag 时补 :latest）。
// TODO: 覆盖镜像名无法匹配的边界情况，例如：
// 1. Pod 规范使用 Docker 运行时，镜像写为 docker.io/library/test:tag，而节点状态只有 test:tag。
// 2. Pod 规范使用隐式 registry，镜像写为 test:tag 或 library/test:tag，而节点状态只有
// docker.io/library/test:tag。用户始终使用同一种 registry 格式时不会出现这些问题。
func normalizedImageName(name string) string {
	if strings.LastIndex(name, ":") <= strings.LastIndex(name, "/") {
		name = name + ":latest"
	}
	return name
}
