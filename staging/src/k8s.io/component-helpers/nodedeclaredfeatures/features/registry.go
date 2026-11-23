/*
Copyright 2025 The Kubernetes Authors.

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

// Package features 汇总所有已实现的节点声明特性，并对外提供统一的注册表。
// 发现节点能力、推断 Pod 调度/更新所需能力都遍历同一份列表，
// 新增特性只需实现 types.Feature 并加入 AllFeatures 即可接入全部流程。
package features

import (
	"k8s.io/component-helpers/nodedeclaredfeatures/features/dranodeallocatableresources"
	"k8s.io/component-helpers/nodedeclaredfeatures/features/draoptionalnodeoperations"
	"k8s.io/component-helpers/nodedeclaredfeatures/features/extendwebsocketstokubelet"
	"k8s.io/component-helpers/nodedeclaredfeatures/features/inplacepodresize"
	"k8s.io/component-helpers/nodedeclaredfeatures/features/restartallcontainers"
	"k8s.io/component-helpers/nodedeclaredfeatures/features/usernamespaceshostnetwork"
	"k8s.io/component-helpers/nodedeclaredfeatures/features/volumebindmountoptions"
	"k8s.io/component-helpers/nodedeclaredfeatures/types"
)

// AllFeatures 是所有已声明特性的中心注册表。
// 新增特性只要加入这份列表，就会自动被纳入“节点发现”和“Pod 推断”两条逻辑链。
//
// 发现、调度推断和更新推断都遍历这一份列表，新增特性只需实现 Feature 接口并挂到这里，
// 不必改动各调用方。列表中保存接口值，也让统一的一致性测试能覆盖每个实现。
// 顺序不重要：位下标由 FeatureMapper 另行按名字排序决定，这里按语义分组便于阅读。
var AllFeatures = []types.Feature{
	restartallcontainers.Feature,
	inplacepodresize.PodLevelResourcesResizeFeature,
	extendwebsocketstokubelet.Feature,
	inplacepodresize.NonSidecarInitContainerResizeFeature,
	inplacepodresize.MemoryBackedVolumesResizeFeature,
	usernamespaceshostnetwork.Feature,
	dranodeallocatableresources.Feature,
	volumebindmountoptions.Feature,
	draoptionalnodeoperations.Feature,
}
