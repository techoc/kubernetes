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

package workloadbuilder

import (
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
)

// mapWorkloadInput 把叶子节点上的 scheduling.k8s.io building block 映射成
// WorkloadItem 配置的 IR。它始终返回非 nil 的配置；当某个输入为空时对应字段
// 保持 nil，使 resolveSchedulingConfig 能按字段粒度回退到默认值。
// 这里区分“字段没设置”和“显式空设置”很关键：每个字段独立判断，
// 让 DefaultConfig 能按字段粒度兜底；如果把整个 Input 的缺省当成显式覆盖，
// 默认配置就会被意外冲掉。返回值始终非 nil，是为了让后续合并逻辑不必到处判空，
// 统一处理“有壳无内容”的中间态。
func mapWorkloadInput(input WorkloadInput) *SchedulingConfig {
	cfg := &SchedulingConfig{}

	if input.Policy.PodGroupData != nil {
		if p := mapSchedulingPolicy(input.Policy.PodGroupData); p != nil {
			cfg.Policy = p
		}
	}
	if input.Constraints.PodGroupData != nil {
		if c := mapTopologyConstraints(input.Constraints.PodGroupData); c != nil {
			cfg.Constraints = c
		}
	}
	if input.DisruptionMode.PodGroupData != nil {
		if d := mapDisruptionMode(input.DisruptionMode.PodGroupData); d != nil {
			cfg.DisruptionMode = d
		}
	}
	if len(input.ResourceClaims.PodGroupData) > 0 {
		cfg.ResourceClaims = mapResourceClaims(input.ResourceClaims.PodGroupData)
	}

	return cfg
}

// mapCompositeGroupInput 把复合节点上的 scheduling.k8s.io building block 映射成
// 复合 WorkloadItem 配置的 IR，包括 group-of-groups 调度策略、拓扑约束和中断模式。
// CompositePodGroupTemplate 的其余字段（PriorityClassName 与 Priority）在解析阶段
// 由 DefaultConfig/回调填充，不来自 WorkloadInput。它始终返回非 nil 配置；
// 当某个输入为空时对应字段保持 nil，使 resolveSchedulingConfig 能按字段粒度回退到默认值。
// 组合节点不处理 ResourceClaims，那是叶子节点的概念；优先级相关字段也不在这里翻译，
// 因为它们来自控制器默认值或回调。这样输入层只负责表达“用户显式设置了什么”。
func mapCompositeGroupInput(input WorkloadInput) *SchedulingConfig {
	cfg := &SchedulingConfig{}

	if input.Policy.CompositePodGroupData != nil {
		if p := mapCompositeSchedulingPolicy(input.Policy.CompositePodGroupData); p != nil {
			cfg.Policy = p
		}
	}
	if input.Constraints.CompositePodGroupData != nil {
		if c := mapCompositeTopologyConstraints(input.Constraints.CompositePodGroupData); c != nil {
			cfg.Constraints = c
		}
	}
	if input.DisruptionMode.CompositePodGroupData != nil {
		if d := mapCompositeDisruptionMode(input.DisruptionMode.CompositePodGroupData); d != nil {
			cfg.DisruptionMode = d
		}
	}

	return cfg
}

// mapSchedulingPolicy 对空策略返回 nil，使 resolveSchedulingConfig 回退到控制器默认值，
// 而不是把用户未填写的 block 当成一次显式覆盖。
// Basic 与 Gang 互斥，switch 的顺序决定两者同时出现时以 Basic 为准；
// 把 API 的 union 收敛成 IR 里的单个非空指针，后续合并逻辑只需判断一个字段。
func mapSchedulingPolicy(p *schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy) *SchedulingPolicy {
	switch {
	case p.Basic != nil:
		return &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}}
	case p.Gang != nil:
		gang := &GangSchedulingPolicy{}
		// 复制指针值，避免回调在解析后的配置上改 MinCount 时，
		// 反过来污染调用方传入的输入 building block。
		if p.Gang.MinCount != nil {
			mc := *p.Gang.MinCount
			gang.MinCount = &mc
		}
		return &SchedulingPolicy{Gang: gang}
	default:
		return nil
	}
}

// mapCompositeSchedulingPolicy 把复合节点的 building block 策略映射到共享的 IR 策略。
// 复合 gang 的 minGroupCount 借用 IR 的 GangSchedulingPolicy.MinCount 承载，
// 编译复合节点时再还原为 CompositePodGroupSchedulingPolicy.MinGroupCount。
// 空策略返回 nil，使 resolveSchedulingConfig 回退到控制器默认值，
// 而不是把用户未填写的 block 当成一次显式覆盖。
// 组合策略与叶子策略共用同一份 IR，minGroupCount 复用 MinCount 字段，
// 编回 CompositePodGroupTemplate 时再还原名字；复用避免为两种节点维护两套合并代码。
func mapCompositeSchedulingPolicy(p *schedulingv1alpha3.WorkloadCompositePodGroupSchedulingPolicy) *SchedulingPolicy {
	switch {
	case p.Basic != nil:
		return &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}}
	case p.Gang != nil:
		gang := &GangSchedulingPolicy{}
		// 复制指针值，避免回调在解析后的配置上改值，
		// 反过来污染调用方传入的输入 building block。
		if p.Gang.MinGroupCount != nil {
			mgc := *p.Gang.MinGroupCount
			gang.MinCount = &mgc
		}
		return &SchedulingPolicy{Gang: gang}
	default:
		return nil
	}
}

// mapTopologyConstraints 只保留 Key 而丢掉 v1alpha3 里的其他元信息，
// 因为调度器消费的 IR 只需要约束键；空列表映射成 nil，同样是为了让默认值能生效。
func mapTopologyConstraints(c *schedulingv1alpha3.WorkloadPodGroupSchedulingConstraints) *SchedulingConstraints {
	if len(c.Topology) == 0 {
		return nil
	}
	topology := make([]schedulingv1beta1.TopologyConstraint, len(c.Topology))
	for i, t := range c.Topology {
		topology[i] = schedulingv1beta1.TopologyConstraint{Key: t.Key}
	}
	return &SchedulingConstraints{Topology: topology}
}

// mapCompositeTopologyConstraints 与叶子节点的约束映射分开写，是因为输入 API 类型不同，
// 但都收敛到同一个 SchedulingConstraints IR；保持双向一一对应便于未来扩展时
// 明确哪些字段需要补上，而不是用反射做隐式转换。
func mapCompositeTopologyConstraints(c *schedulingv1alpha3.WorkloadCompositePodGroupSchedulingConstraints) *SchedulingConstraints {
	if len(c.Topology) == 0 {
		return nil
	}
	topology := make([]schedulingv1beta1.TopologyConstraint, len(c.Topology))
	for i, t := range c.Topology {
		topology[i] = schedulingv1beta1.TopologyConstraint{Key: t.Key}
	}
	return &SchedulingConstraints{Topology: topology}
}

// mapDisruptionMode 把叶子节点的中断模式映射到 IR。disruption mode 也是 union 语义，
// 默认分支返回 nil 表示“用户没指定”，与明确指定 Single/All 区分开，
// 后者才会在校验阶段参与交叉规则检查。
func mapDisruptionMode(d *schedulingv1alpha3.WorkloadPodGroupDisruptionMode) *DisruptionMode {
	switch {
	case d.Single != nil:
		return &DisruptionMode{Single: &SingleDisruptionMode{}}
	case d.All != nil:
		return &DisruptionMode{All: &AllDisruptionMode{}}
	default:
		return nil
	}
}

// mapCompositeDisruptionMode 把复合节点的中断模式映射到 IR。组合节点与叶子节点
// 在 IR 层共用 DisruptionMode 类型，所以映射结果完全同构；独立的函数只是为了
// 保持 API 类型到 IR 的映射显式且可导航。
func mapCompositeDisruptionMode(d *schedulingv1alpha3.WorkloadCompositePodGroupDisruptionMode) *DisruptionMode {
	switch {
	case d.Single != nil:
		return &DisruptionMode{Single: &SingleDisruptionMode{}}
	case d.All != nil:
		return &DisruptionMode{All: &AllDisruptionMode{}}
	default:
		return nil
	}
}

// mapResourceClaims 逐个复制 name 指针而不是直接赋值，是为了让后续回调修改 IR 时
// 不会写穿到 controller 传入的 API 对象；空切片映射成 nil，
// 表示“没有声明任何 claim”，从而允许默认值/其他来源填充。
func mapResourceClaims(claims []schedulingv1alpha3.WorkloadPodGroupResourceClaim) []ResourceClaim {
	result := make([]ResourceClaim, len(claims))
	for i := range claims {
		rc := ResourceClaim{Name: claims[i].Name}
		if claims[i].ResourceClaimName != nil {
			v := *claims[i].ResourceClaimName
			rc.ResourceClaimName = &v
		}
		if claims[i].ResourceClaimTemplateName != nil {
			v := *claims[i].ResourceClaimTemplateName
			rc.ResourceClaimTemplateName = &v
		}
		result[i] = rc
	}
	return result
}
