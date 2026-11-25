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

package nodedeclaredfeatures

import (
	"fmt"
	"slices"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/component-helpers/nodedeclaredfeatures/features"
	"k8s.io/component-helpers/nodedeclaredfeatures/types"
)

// Framework 把"节点有哪些能力"和"Pod 需要哪些能力"两侧的判定统一到一份特性注册表上。
// 构造时按名称排序并建立名称到位图的映射，之后发现、推断和匹配都能复用同一套编码，
// 避免各处都用字符串列表导致顺序与比较规则各自为政。
// Framework 本身是有状态的：它持有特性注册表，并提供节点特性发现与 Pod 特性推断能力。
type Framework struct {
	// 内嵌 FeatureMapper，使 Framework 直接具备名字到位图的映射能力。
	*FeatureMapper
	// registry 保存所有特性定义，遍历顺序与位图下标一致。
	registry []types.Feature
}

// DefaultFramework 是默认框架实例，注册了全部内置特性；
// 调用方若有特殊注册表可自行 New，两者互不干扰。
var DefaultFramework = New(features.AllFeatures)

// New 按特性名排序以确保位图下标稳定：
// 排序后的索引就是 FeatureSet 的位序号，换了注册顺序也不会改变编码含义。
func New(registry []types.Feature) *Framework {
	// 确保特性按名字排序，位图下标才有稳定含义。
	slices.SortFunc(registry, func(a, b types.Feature) int {
		return strings.Compare(a.Name(), b.Name())
	})
	featureNames := make([]string, len(registry))
	for i, f := range registry {
		featureNames[i] = f.Name()
	}
	return &Framework{
		registry:      registry,
		FeatureMapper: NewFeatureMapper(featureNames),
	}
}

// DiscoverNodeFeatures 根据节点配置和注册表，判定该节点实际具备哪些特性。
// 返回值是按字典序排列、去重后的特性名列表。
func (f *Framework) DiscoverNodeFeatures(cfg *types.NodeConfiguration) []string {
	var enabledFeatures []string
	for _, f := range f.registry {
		if f.Discover(cfg) {
			// 特性已过时则视为不可用：即便开关打开，节点版本超出上限时
			// 也不能对外声明该能力，防止调度器把需要的 Pod 放上来。
			if cfg.Version != nil && f.MaxVersion() != nil && cfg.Version.GreaterThan(f.MaxVersion()) {
				continue
			}
			enabledFeatures = append(enabledFeatures, f.Name())
		}
	}
	return enabledFeatures
}

// InferForPodScheduling 以目标版本为参照，推断调度一个 Pod 需要哪些特性。
// 目标版本必填：它决定某些特性是否已普遍可用、无需再声明为节点约束；
// 返回的特征集合可直接交给匹配阶段使用。
func (f *Framework) InferForPodScheduling(podInfo *types.PodInfo, targetVersion *version.Version) (FeatureSet, error) {
	if targetVersion == nil {
		return FeatureSet{}, fmt.Errorf("target version cannot be nil")
	}
	reqs := f.NewFeatureSet()
	for i, f := range f.registry {
		if f.MaxVersion() != nil && targetVersion.GreaterThan(f.MaxVersion()) {
			// 目标版本已超过特性上限，说明该能力在所有目标节点上必然具备，
			// 不必写进 Pod 的要求，否则会把本可匹配的节点排除掉。
			continue
		}
		if f.InferForScheduling(podInfo) {
			reqs.Set(i)
		}
	}
	return reqs, nil
}

// InferForPodUpdate 与 InferForPodScheduling 同理，但判定依据是更新前后 Pod 的差异。
// 用同一套 FeatureSet 编码，保证匹配阶段无需区分要求来自调度还是更新；
// targetVersion 同样必填，nil 时直接报错。
func (f *Framework) InferForPodUpdate(oldPodInfo, newPodInfo *types.PodInfo, targetVersion *version.Version) (FeatureSet, error) {
	if targetVersion == nil {
		return FeatureSet{}, fmt.Errorf("target version cannot be nil")
	}
	reqs := f.NewFeatureSet()
	for i, f := range f.registry {
		if f.MaxVersion() != nil && targetVersion.GreaterThan(f.MaxVersion()) {
			// 超过版本上限的特性无需列为更新前提，理由同调度推断。
			continue
		}
		if f.InferForUpdate(oldPodInfo, newPodInfo) {
			reqs.Set(i)
		}
	}
	return reqs, nil
}

// MatchResult 封装一次特性匹配检查的结果，并列出未满足的特性名，
// 让调度失败时能直接说明“节点缺什么”，而不是只报不匹配。
type MatchResult struct {
	// IsMatch 为 true 表示节点满足了全部特性要求。
	IsMatch bool
	// UnsatisfiedRequirements 列出未满足的特性名；仅当 IsMatch 为 false 时才有值。
	UnsatisfiedRequirements []string
}

// MatchNode 检查节点声明的特性是否满足 Pod 预先计算出的要求。
// node 为 nil 时返回错误。
// 返回的 MatchResult 中：
//   - IsMatch 为 true 表示 requiredFeatures 全部出现在 node.status.declaredFeatures 中；
//   - UnsatisfiedRequirements 列出要求里有、但节点未声明的特性。
func (f *Framework) MatchNode(requiredFeatures FeatureSet, node *v1.Node) (*MatchResult, error) {
	if node == nil {
		return nil, fmt.Errorf("node cannot be nil")
	}
	// 节点声明的特性可能含未知名字（来自更新版本的节点），
	// 所以用 TryMap 忽略它们，而不是因为不认识就判为不匹配。
	fs := f.TryMap(node.Status.DeclaredFeatures)
	return f.MatchNodeFeatureSet(requiredFeatures, fs)
}

// MatchNodeFeatureSet 直接比较两个 FeatureSet：要求集合是否为节点集合的子集。
// 返回的 MatchResult 中，IsMatch 表示全部要求都被满足；
// UnsatisfiedRequirements 列出要求里有但节点没有的特性。
func (f *Framework) MatchNodeFeatureSet(requiredFeatures FeatureSet, nodeFeatures FeatureSet) (*MatchResult, error) {
	if requiredFeatures.IsEmpty() {
		return &MatchResult{IsMatch: true}, nil // 没有要求，直接匹配成功。
	}
	// 匹配成功是常见路径，先走无分配的子集判断；只有失败时才计算差集生成失败原因。
	isMatch, err := requiredFeatures.IsSubset(nodeFeatures)
	if err != nil {
		return nil, err
	}
	if isMatch {
		return &MatchResult{IsMatch: true}, nil
	}

	// 上一步的 IsSubset 已经排除尺寸不符的情况，此处 Difference 不会报错，可安全忽略。
	diff, _ := requiredFeatures.Difference(nodeFeatures)
	unsatisfiedRequirements, err := f.Unmap(diff)
	if err != nil {
		return nil, err
	}
	return &MatchResult{IsMatch: false, UnsatisfiedRequirements: unsatisfiedRequirements}, nil
}

// GetFeatureRequirements 按名字返回特性依赖的 feature gate 等条件，
// 供外部在声明节点能力前核对应该打开哪些开关；名字未注册时返回错误。
func (f *Framework) GetFeatureRequirements(name string) (*FeatureRequirements, error) {
	for _, r := range f.registry {
		if r.Name() == name {
			return r.Requirements(), nil
		}
	}
	return nil, fmt.Errorf("feature '%s' not registered", name)
}
