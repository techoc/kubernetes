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
	"testing"

	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

// TestMapWorkloadInput 验证 leaf 形态的版本化 building block 到 IR 的映射。
// 重点不在“字段被复制”，而在两个语义：空对象必须映射成 nil 以保留默认值；
// 以及指针字段必须拷贝出独立副本，不能与输入对象共享存储。
func TestMapWorkloadInput(t *testing.T) {
	tests := []struct {
		name        string
		policy      *schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy
		constraints *schedulingv1alpha3.WorkloadPodGroupSchedulingConstraints
		disruption  *schedulingv1alpha3.WorkloadPodGroupDisruptionMode
		claims      []schedulingv1alpha3.WorkloadPodGroupResourceClaim
		check       func(t *testing.T, cfg *SchedulingConfig)
	}{
		{
			// 所有输入均为 nil：各字段应保持 nil，这是默认值能够生效的前提。
			name: "all nil leaves fields unset",
			check: func(t *testing.T, cfg *SchedulingConfig) {
				if cfg.Policy != nil || cfg.Constraints != nil ||
					cfg.DisruptionMode != nil ||
					cfg.ResourceClaims != nil {
					t.Errorf("expected all fields nil, got %+v", cfg)
				}
			},
		},
		{
			// 空策略对象映射为 nil，避免把“未填写”误当成显式覆盖而冲掉默认配置。
			name:   "empty policy maps to nil so the default survives",
			policy: &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{},
			check: func(t *testing.T, cfg *SchedulingConfig) {
				if cfg.Policy != nil {
					t.Errorf("expected nil policy for empty input, got %+v", cfg.Policy)
				}
			},
		},
		{
			// 空约束对象同样应映射为 nil。
			name:        "empty constraints maps to nil so the default survives",
			constraints: &schedulingv1alpha3.WorkloadPodGroupSchedulingConstraints{},
			check: func(t *testing.T, cfg *SchedulingConfig) {
				if cfg.Constraints != nil {
					t.Errorf("expected nil constraints for empty input, got %+v", cfg.Constraints)
				}
			},
		},
		{
			// 空中断模式对象映射为 nil，保留调度器的默认行为。
			name:       "empty disruption maps to nil so the default survives",
			disruption: &schedulingv1alpha3.WorkloadPodGroupDisruptionMode{},
			check: func(t *testing.T, cfg *SchedulingConfig) {
				if cfg.DisruptionMode != nil {
					t.Errorf("expected nil disruption for empty input, got %+v", cfg.DisruptionMode)
				}
			},
		},
		{
			// Basic 策略能被正确识别并映射到 IR。
			name:   "basic policy",
			policy: &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{Basic: &schedulingv1alpha3.WorkloadPodGroupBasicSchedulingPolicy{}},
			check: func(t *testing.T, cfg *SchedulingConfig) {
				if cfg.Policy == nil || cfg.Policy.Basic == nil {
					t.Error("expected Basic policy")
				}
			},
		},
		{
			// Gang 策略连同 minCount 一起映射，数值需原样保留。
			name:   "gang policy with minCount",
			policy: &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{Gang: &schedulingv1alpha3.WorkloadPodGroupGangSchedulingPolicy{MinCount: ptr.To[int32](4)}},
			check: func(t *testing.T, cfg *SchedulingConfig) {
				if cfg.Policy == nil || cfg.Policy.Gang == nil {
					t.Fatal("expected Gang policy")
				}
				if cfg.Policy.Gang.MinCount == nil || *cfg.Policy.Gang.MinCount != 4 {
					t.Errorf("expected MinCount=4, got %v", cfg.Policy.Gang.MinCount)
				}
			},
		},
		{
			// Gang 策略未设置 minCount 时，IR 中仍保持 nil，留给回调按控制器语义补默认。
			name:   "gang policy preserves nil minCount",
			policy: &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{Gang: &schedulingv1alpha3.WorkloadPodGroupGangSchedulingPolicy{}},
			check: func(t *testing.T, cfg *SchedulingConfig) {
				if cfg.Policy == nil || cfg.Policy.Gang == nil {
					t.Fatal("expected Gang policy")
				}
				if cfg.Policy.Gang.MinCount != nil {
					t.Errorf("expected nil MinCount to be preserved, got %d", *cfg.Policy.Gang.MinCount)
				}
			},
		},
		{
			// 拓扑约束逐项映射，Key 必须原样带到 IR。
			name: "topology constraints",
			constraints: &schedulingv1alpha3.WorkloadPodGroupSchedulingConstraints{
				Topology: []schedulingv1alpha3.TopologyConstraint{{Key: "topology.kubernetes.io/rack"}},
			},
			check: func(t *testing.T, cfg *SchedulingConfig) {
				if cfg.Constraints == nil || len(cfg.Constraints.Topology) != 1 {
					t.Fatal("expected 1 topology constraint")
				}
				if cfg.Constraints.Topology[0].Key != "topology.kubernetes.io/rack" {
					t.Errorf("unexpected key: %s", cfg.Constraints.Topology[0].Key)
				}
			},
		},
		{
			// All 中断模式能被正确映射到 IR。
			name:       "disruption mode all",
			disruption: &schedulingv1alpha3.WorkloadPodGroupDisruptionMode{All: &schedulingv1alpha3.WorkloadPodGroupAllDisruptionMode{}},
			check: func(t *testing.T, cfg *SchedulingConfig) {
				if cfg.DisruptionMode == nil || cfg.DisruptionMode.All == nil {
					t.Error("expected All disruption mode")
				}
			},
		},
		{
			// 资源声明的名称与 ResourceClaimName 都要被复制。
			name:   "resource claims",
			claims: []schedulingv1alpha3.WorkloadPodGroupResourceClaim{{Name: "gpu", ResourceClaimName: new("shared-gpu")}},
			check: func(t *testing.T, cfg *SchedulingConfig) {
				if len(cfg.ResourceClaims) != 1 {
					t.Fatalf("expected 1 claim, got %d", len(cfg.ResourceClaims))
				}
				if cfg.ResourceClaims[0].Name != "gpu" {
					t.Errorf("expected claim name 'gpu', got %q", cfg.ResourceClaims[0].Name)
				}
				if cfg.ResourceClaims[0].ResourceClaimName == nil || *cfg.ResourceClaims[0].ResourceClaimName != "shared-gpu" {
					t.Error("expected ResourceClaimName 'shared-gpu'")
				}
			},
		},
		{
			// 所有字段同时设置：验证四类 building block 的映射互不干扰。
			name:   "all fields populated",
			policy: &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{Gang: &schedulingv1alpha3.WorkloadPodGroupGangSchedulingPolicy{MinCount: ptr.To[int32](2)}},
			constraints: &schedulingv1alpha3.WorkloadPodGroupSchedulingConstraints{
				Topology: []schedulingv1alpha3.TopologyConstraint{{Key: "zone"}},
			},
			disruption: &schedulingv1alpha3.WorkloadPodGroupDisruptionMode{Single: &schedulingv1alpha3.WorkloadPodGroupSingleDisruptionMode{}},
			claims:     []schedulingv1alpha3.WorkloadPodGroupResourceClaim{{Name: "net", ResourceClaimTemplateName: new("tpl")}},
			check: func(t *testing.T, cfg *SchedulingConfig) {
				if cfg.Policy == nil || cfg.Policy.Gang == nil {
					t.Error("expected Gang policy")
				}
				if cfg.Constraints == nil || len(cfg.Constraints.Topology) != 1 {
					t.Error("expected topology")
				}
				if cfg.DisruptionMode == nil || cfg.DisruptionMode.Single == nil {
					t.Error("expected Single disruption")
				}
				if len(cfg.ResourceClaims) != 1 || cfg.ResourceClaims[0].ResourceClaimTemplateName == nil {
					t.Error("expected resource claim with template name")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := mapWorkloadInput(WorkloadInput{
				Policy:         PolicyInput{PodGroupData: tt.policy},
				Constraints:    ConstraintsInput{PodGroupData: tt.constraints},
				DisruptionMode: DisruptionModeInput{PodGroupData: tt.disruption},
				ResourceClaims: ResourceClaimsInput{PodGroupData: tt.claims},
			})
			// 前置断言：映射函数必须始终返回非 nil 配置，后续按字段检查才有意义。
			if cfg == nil {
				t.Fatal("expected non-nil config")
			}
			tt.check(t, cfg)
		})
	}
}

// TestMapCompositeGroupInput 验证 composite 形态的映射，并额外确认 resourceClaims
// 只在 leaf 侧存在：composite 不直接拥有 Pod，因此没有共享资源声明的概念。
func TestMapCompositeGroupInput(t *testing.T) {
	inputPolicy := &schedulingv1alpha3.WorkloadCompositePodGroupSchedulingPolicy{
		Gang: &schedulingv1alpha3.WorkloadCompositePodGroupGangSchedulingPolicy{
			MinGroupCount: ptr.To[int32](2),
		},
	}

	tests := []struct {
		name        string
		policy      *schedulingv1alpha3.WorkloadCompositePodGroupSchedulingPolicy
		constraints *schedulingv1alpha3.WorkloadCompositePodGroupSchedulingConstraints
		disruption  *schedulingv1alpha3.WorkloadCompositePodGroupDisruptionMode
		check       func(t *testing.T, cfg *SchedulingConfig)
	}{
		{
			// 策略输入为 nil：Policy 字段应保持未设置。
			name: "nil policy leaves Policy unset",
			check: func(t *testing.T, cfg *SchedulingConfig) {
				if cfg.Policy != nil {
					t.Errorf("expected nil policy for nil input, got %+v", cfg.Policy)
				}
			},
		},
		{
			// 复合拓扑约束按 Key 映射到共享的 SchedulingConstraints IR。
			name:        "composite topology constraints",
			constraints: &schedulingv1alpha3.WorkloadCompositePodGroupSchedulingConstraints{Topology: []schedulingv1alpha3.TopologyConstraint{{Key: "zone"}}},
			check: func(t *testing.T, cfg *SchedulingConfig) {
				if cfg.Constraints == nil || len(cfg.Constraints.Topology) != 1 || cfg.Constraints.Topology[0].Key != "zone" {
					t.Errorf("expected 1 composite topology constraint, got %+v", cfg.Constraints)
				}
			},
		},
		{
			// 复合 All 中断模式能被正确映射到 IR。
			name:       "composite disruption mode all",
			disruption: &schedulingv1alpha3.WorkloadCompositePodGroupDisruptionMode{All: &schedulingv1alpha3.WorkloadCompositePodGroupAllDisruptionMode{}},
			check: func(t *testing.T, cfg *SchedulingConfig) {
				if cfg.DisruptionMode == nil || cfg.DisruptionMode.All == nil {
					t.Error("expected composite All disruption mode")
				}
			},
		},
		{
			// 空的复合策略对象映射为 nil，保留控制器默认值。
			name:   "empty policy maps to nil so the default survives",
			policy: &schedulingv1alpha3.WorkloadCompositePodGroupSchedulingPolicy{},
			check: func(t *testing.T, cfg *SchedulingConfig) {
				if cfg.Policy != nil {
					t.Errorf("expected nil policy for empty input, got %+v", cfg.Policy)
				}
			},
		},
		{
			// 复合 Basic 策略能被正确识别。
			name:   "basic policy",
			policy: &schedulingv1alpha3.WorkloadCompositePodGroupSchedulingPolicy{Basic: &schedulingv1alpha3.WorkloadCompositePodGroupBasicSchedulingPolicy{}},
			check: func(t *testing.T, cfg *SchedulingConfig) {
				if cfg.Policy == nil || cfg.Policy.Basic == nil {
					t.Error("expected Basic policy")
				}
			},
		},
		{
			// 复合 gang 的 minGroupCount 借用 IR 的 MinCount 字段承载，值需保持一致。
			name:   "gang policy carries minGroupCount in the IR MinCount",
			policy: &schedulingv1alpha3.WorkloadCompositePodGroupSchedulingPolicy{Gang: &schedulingv1alpha3.WorkloadCompositePodGroupGangSchedulingPolicy{MinGroupCount: ptr.To[int32](3)}},
			check: func(t *testing.T, cfg *SchedulingConfig) {
				if cfg.Policy == nil || cfg.Policy.Gang == nil {
					t.Fatal("expected Gang policy")
				}
				if cfg.Policy.Gang.MinCount == nil || *cfg.Policy.Gang.MinCount != 3 {
					t.Errorf("expected MinCount=3 (from minGroupCount), got %v", cfg.Policy.Gang.MinCount)
				}
			},
		},
		{
			// 复合 gang 未设置 minGroupCount 时，IR 中也保持 nil 供回调补默认。
			name:   "gang policy preserves nil minGroupCount",
			policy: &schedulingv1alpha3.WorkloadCompositePodGroupSchedulingPolicy{Gang: &schedulingv1alpha3.WorkloadCompositePodGroupGangSchedulingPolicy{}},
			check: func(t *testing.T, cfg *SchedulingConfig) {
				if cfg.Policy == nil || cfg.Policy.Gang == nil {
					t.Fatal("expected Gang policy")
				}
				if cfg.Policy.Gang.MinCount != nil {
					t.Errorf("expected nil MinCount to be preserved, got %d", *cfg.Policy.Gang.MinCount)
				}
			},
		},
		{
			// 映射必须复制指针值而非直接别名输入，否则回调会污染控制器 API 对象。
			name:   "gang policy copies minGroupCount instead of aliasing the input",
			policy: inputPolicy,
			check: func(t *testing.T, cfg *SchedulingConfig) {
				if cfg.Policy == nil || cfg.Policy.Gang == nil || cfg.Policy.Gang.MinCount == nil {
					t.Fatal("expected Gang policy with MinCount")
				}
				// 修改解析后的值不得回写到调用方的 building block。
				// 这里故意改 IR 里的值再看输入有没有变，专门防止映射实现直接把
				// 输入对象的指针挂到 IR 上——那会让后续 callback 的修改污染控制器 API 对象。
				*cfg.Policy.Gang.MinCount = 99
				if *inputPolicy.Gang.MinGroupCount != 2 {
					t.Errorf("mapping aliased the input; minGroupCount mutated to %d", *inputPolicy.Gang.MinGroupCount)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := mapCompositeGroupInput(WorkloadInput{
				Policy:         PolicyInput{CompositePodGroupData: tt.policy},
				Constraints:    ConstraintsInput{CompositePodGroupData: tt.constraints},
				DisruptionMode: DisruptionModeInput{CompositePodGroupData: tt.disruption},
			})
			// 前置断言：映射函数必须始终返回非 nil 配置。
			if cfg == nil {
				t.Fatal("expected non-nil config")
			}
			// resourceClaims 只在叶子侧存在；复合节点永远不会映射它。
			if cfg.ResourceClaims != nil {
				t.Errorf("expected no resourceClaims for a composite, got %+v", cfg.ResourceClaims)
			}
			tt.check(t, cfg)
		})
	}
}

// TestMapConfigEndToEnd 走通文档所述的控制器集成链路：先把公开的 building block
// 映射成 IR，再通过 builder 编译，leaf 与 composite 两种形态都覆盖。
//
// 前面的单测只验证映射或编译的一半，这里把完整链路串起来，
// 确保 building block 经映射、默认值合并、callback 处理后，最终生成的 Workload 模板
// 里确实带着正确的值。leaf 和 composite 各一条用例，覆盖两种模板编译路径。
func TestMapConfigEndToEnd(t *testing.T) {
	tests := []struct {
		name   string
		root   *WorkloadItem
		verify func(t *testing.T, wl *schedulingv1beta1.Workload)
	}{
		{
			// 叶子组端到端：映射 + 编译，验证四类 building block 的值都进入最终模板。
			name: "leaf group maps and compiles every building block",
			root: &WorkloadItem{
				Name:          "job-root",
				DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}}},
				Input: WorkloadInput{
					Policy: PolicyInput{PodGroupData: &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{
						Gang: &schedulingv1alpha3.WorkloadPodGroupGangSchedulingPolicy{},
					}},
					Constraints: ConstraintsInput{PodGroupData: &schedulingv1alpha3.WorkloadPodGroupSchedulingConstraints{
						Topology: []schedulingv1alpha3.TopologyConstraint{{Key: "topology.kubernetes.io/zone"}},
					}},
					DisruptionMode: DisruptionModeInput{PodGroupData: &schedulingv1alpha3.WorkloadPodGroupDisruptionMode{
						All: &schedulingv1alpha3.WorkloadPodGroupAllDisruptionMode{},
					}},
				},
				Callbacks: []SchedulingConfigFunc{defaultGangMinCount(4)},
			},
			verify: func(t *testing.T, wl *schedulingv1beta1.Workload) {
				tmpl := wl.Spec.PodGroupTemplates[0]
				if tmpl.SchedulingPolicy.Gang == nil || tmpl.SchedulingPolicy.Gang.MinCount != 4 {
					t.Error("expected Gang policy with defaulted MinCount=4")
				}
				if tmpl.SchedulingConstraints == nil || len(tmpl.SchedulingConstraints.Topology) != 1 {
					t.Error("expected topology constraint to pass through")
				}
				if tmpl.DisruptionMode == nil || tmpl.DisruptionMode.All == nil {
					t.Error("expected All disruption mode to pass through")
				}
			},
		},
		{
			// 复合组端到端：验证 group-of-groups 策略映射，以及叶子子节点被递归编译。
			name: "composite group maps and compiles the group-of-groups policy",
			root: &WorkloadItem{
				Name: "cpg-root",
				DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{
					Basic: &BasicSchedulingPolicy{},
				},
				},
				Input: WorkloadInput{
					Policy: PolicyInput{CompositePodGroupData: &schedulingv1alpha3.WorkloadCompositePodGroupSchedulingPolicy{
						Gang: &schedulingv1alpha3.WorkloadCompositePodGroupGangSchedulingPolicy{},
					}},
				},
				Callbacks: []SchedulingConfigFunc{defaultGangMinCount(4)},
				Children: []*WorkloadItem{
					{Name: "workers", DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}}}},
				},
			},
			verify: func(t *testing.T, wl *schedulingv1beta1.Workload) {
				cpg := wl.Spec.CompositePodGroupTemplates[0]
				if cpg.SchedulingPolicy.Gang == nil || cpg.SchedulingPolicy.Gang.MinGroupCount != 4 {
					t.Errorf("expected composite Gang policy with defaulted MinGroupCount=4, got %+v", cpg.SchedulingPolicy)
				}
				if len(cpg.PodGroupTemplates) != 1 || cpg.PodGroupTemplates[0].Name != "workers" {
					t.Errorf("expected the leaf child to compile, got %+v", cpg.PodGroupTemplates)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 通过 NewBuilder 走完整编译链路；Owner 是编译身份的硬性要求。
			wl, err := NewBuilder(tt.root, BuildOptions{
				Name:      "job",
				Namespace: "ns",
				Owner: &metav1.OwnerReference{
					APIVersion: "batch/v1",
					Kind:       "Job",
					Name:       "test-job",
					UID:        "12345",
				},
			}).BuildWorkload()
			// 构建失败时后续字段断言无从谈起，直接终止当前子测试。
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			tt.verify(t, wl)
		})
	}
}
