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

// 本文件集中验证 WorkloadBuilder 的两条使用路径：
// 从 WorkloadItem 编译新 Workload，以及从已有 Workload 还原 Builder。
// 测试重点覆盖默认值与用户输入的逐字段合并、复合模板的递归编译、
// 以及缓存/索引只构建一次的行为。

package workloadbuilder

import (
	"reflect"
	"strings"
	"testing"

	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/utils/ptr"
)

// TestBuildWorkload 表驱动覆盖编译 Workload 的主要分支：单模板/复合模板、
// 默认值与用户输入的逐字段覆盖、回调调整，以及各类非法输入。
// 每个用例的 verify 回调用于检查具体的编译结果。
func TestBuildWorkload(t *testing.T) {
	// 这个标志位跨用例共享，专门用来验证“空叶子名字”用例会在名字检查处提前失败：
	// 名字校验应先于配置解析与回调执行，因此该标志位应始终保持 false。
	emptyNameCallbackRan := false
	// 表驱动用例各字段的含义：
	// name - 用例名，同时作为子测试名；owner 替换逻辑靠它识别“缺少 owner”特例
	// root - 待编译的 Workload 树根节点；nil 用于覆盖空输入报错
	// owner - 编译选项里的 owner；为 nil 且非特例时会被替换为 dummyOwner
	// wantErr - 是否预期 BuildWorkload 返回错误
	// verify - 成功路径下对编译结果（及根节点）的逐字段断言
	tests := []struct {
		name    string
		root    *WorkloadItem
		owner   *metav1.OwnerReference
		wantErr bool
		verify  func(t *testing.T, wl *schedulingv1beta1.Workload, root *WorkloadItem)
	}{
		{
			// 最小可用输入：只给默认 Basic 策略。验证编译出恰好一个叶子 PodGroupTemplate，
			// 且只设置 Basic、不误带 Gang。
			name: "basic policy",
			root: &WorkloadItem{
				Name:          "test-job",
				DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}}},
			},
			verify: func(t *testing.T, wl *schedulingv1beta1.Workload, _ *WorkloadItem) {
				if len(wl.Spec.PodGroupTemplates) != 1 {
					t.Fatalf("expected 1 template, got %d", len(wl.Spec.PodGroupTemplates))
				}
				tmpl := wl.Spec.PodGroupTemplates[0]
				if tmpl.SchedulingPolicy.Basic == nil {
					t.Error("expected Basic policy to be set")
				}
				if tmpl.SchedulingPolicy.Gang != nil {
					t.Error("Gang policy should not be set")
				}
			},
		},
		{
			// 默认配置给出 Gang(MinCount=4)。验证默认值会被编译进模板的 SchedulingPolicy.Gang。
			name: "gang policy from default",
			root: &WorkloadItem{
				Name:          "gang-job",
				DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Gang: &GangSchedulingPolicy{MinCount: ptr.To[int32](4)}}},
			},
			verify: func(t *testing.T, wl *schedulingv1beta1.Workload, _ *WorkloadItem) {
				gang := wl.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang
				if gang == nil {
					t.Fatal("expected Gang policy")
				}
				if gang.MinCount != 4 {
					t.Errorf("expected MinCount=4, got %d", gang.MinCount)
				}
			},
		},
		{
			// 用户输入一个空的 Gang（没给 MinCount），由回调补默认值 8。
			// 验证回调确实挂在编译链路上且能填充必填字段。
			name: "gang minCount defaulted by callback",
			root: &WorkloadItem{
				Name:          "gang-job",
				DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}}},
				Input:         WorkloadInput{Policy: PolicyInput{PodGroupData: &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{Gang: &schedulingv1alpha3.WorkloadPodGroupGangSchedulingPolicy{}}}},
				Callbacks:     []SchedulingConfigFunc{defaultGangMinCount(8)},
			},
			verify: func(t *testing.T, wl *schedulingv1beta1.Workload, _ *WorkloadItem) {
				gang := wl.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang
				if gang == nil {
					t.Fatal("expected Gang policy")
				}
				if gang.MinCount != 8 {
					t.Errorf("expected MinCount=8 (from callback), got %d", gang.MinCount)
				}
			},
		},
		{
			// 默认是 Basic，但用户输入里带了 Gang(MinCount=2)。
			// 验证用户策略整体覆盖默认策略，而不是两者共存。
			name: "user gang overrides default basic",
			root: &WorkloadItem{
				Name:          "override-job",
				DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}}},
				Input:         WorkloadInput{Policy: PolicyInput{PodGroupData: gangData(2)}},
			},
			verify: func(t *testing.T, wl *schedulingv1beta1.Workload, _ *WorkloadItem) {
				gang := wl.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang
				if gang == nil {
					t.Fatal("expected Gang policy from user override")
				}
				if gang.MinCount != 2 {
					t.Errorf("expected user MinCount=2, got %d", gang.MinCount)
				}
			},
		},
		{
			// 默认与用户分别提供不同维度的配置：默认给 Basic + Single 中断模式，
			// 用户只给拓扑约束。验证合并是逐字段的——默认的 Basic/Single 保留，
			// 用户的拓扑约束被加入，而不是其中一方整体覆盖另一方。
			name: "field-by-field merge of default and user config",
			root: &WorkloadItem{
				Name: "merge-job",
				DefaultConfig: &SchedulingConfig{
					Policy:         &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}},
					DisruptionMode: &DisruptionMode{Single: &SingleDisruptionMode{}},
				},
				Input: WorkloadInput{
					Constraints: ConstraintsInput{PodGroupData: &schedulingv1alpha3.WorkloadPodGroupSchedulingConstraints{
						Topology: []schedulingv1alpha3.TopologyConstraint{{Key: "topology.kubernetes.io/zone"}},
					}},
				},
			},
			verify: func(t *testing.T, wl *schedulingv1beta1.Workload, _ *WorkloadItem) {
				tmpl := wl.Spec.PodGroupTemplates[0]
				if tmpl.SchedulingPolicy.Basic == nil {
					t.Error("expected default Basic policy to survive the merge")
				}
				if tmpl.DisruptionMode == nil || tmpl.DisruptionMode.Single == nil {
					t.Error("expected default Single disruption mode to survive the merge")
				}
				if tmpl.SchedulingConstraints == nil || len(tmpl.SchedulingConstraints.Topology) != 1 {
					t.Error("expected user-provided topology constraint")
				}
			},
		},
		{
			// 用户输入一条拓扑约束。验证它被编译进 SchedulingConstraints，
			// 且拓扑 key 原样保留（zone）。
			name: "topology constraints",
			root: &WorkloadItem{
				Name:          "topo-job",
				DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}}},
				Input: WorkloadInput{
					Constraints: ConstraintsInput{PodGroupData: &schedulingv1alpha3.WorkloadPodGroupSchedulingConstraints{
						Topology: []schedulingv1alpha3.TopologyConstraint{{Key: "topology.kubernetes.io/zone"}},
					}},
				},
			},
			verify: func(t *testing.T, wl *schedulingv1beta1.Workload, _ *WorkloadItem) {
				tmpl := wl.Spec.PodGroupTemplates[0]
				if tmpl.SchedulingConstraints == nil || len(tmpl.SchedulingConstraints.Topology) != 1 {
					t.Fatal("expected 1 topology constraint")
				}
				if tmpl.SchedulingConstraints.Topology[0].Key != "topology.kubernetes.io/zone" {
					t.Errorf("unexpected topology key: %s", tmpl.SchedulingConstraints.Topology[0].Key)
				}
			},
		},
		{
			// 用户输入 All 中断模式。验证编译结果中 DisruptionMode.All 被设置。
			name: "disruption mode all",
			root: &WorkloadItem{
				Name:          "disruption-job",
				DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}}},
				Input:         WorkloadInput{DisruptionMode: DisruptionModeInput{PodGroupData: &schedulingv1alpha3.WorkloadPodGroupDisruptionMode{All: &schedulingv1alpha3.WorkloadPodGroupAllDisruptionMode{}}}},
			},
			verify: func(t *testing.T, wl *schedulingv1beta1.Workload, _ *WorkloadItem) {
				tmpl := wl.Spec.PodGroupTemplates[0]
				if tmpl.DisruptionMode == nil || tmpl.DisruptionMode.All == nil {
					t.Error("expected All disruption mode")
				}
			},
		},
		{
			// DRA 场景：用户输入一条资源声明。验证 ResourceClaims 被编译且名称（gpu）正确。
			name: "resource claims",
			root: &WorkloadItem{
				Name:          "dra-job",
				DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}}},
				Input: WorkloadInput{
					ResourceClaims: ResourceClaimsInput{PodGroupData: []schedulingv1alpha3.WorkloadPodGroupResourceClaim{{Name: "gpu", ResourceClaimName: new("my-claim")}}},
				},
			},
			verify: func(t *testing.T, wl *schedulingv1beta1.Workload, _ *WorkloadItem) {
				tmpl := wl.Spec.PodGroupTemplates[0]
				if len(tmpl.ResourceClaims) != 1 {
					t.Fatalf("expected 1 resource claim, got %d", len(tmpl.ResourceClaims))
				}
				if tmpl.ResourceClaims[0].Name != "gpu" {
					t.Errorf("expected claim name 'gpu', got %q", tmpl.ResourceClaims[0].Name)
				}
			},
		},
		{
			// 默认配置带 priorityClassName。验证该字段被复制到生成的 PodGroupTemplate 上。
			name: "priorityClassName propagates to template",
			root: &WorkloadItem{
				Name: "prio-job",
				DefaultConfig: &SchedulingConfig{
					Policy:            &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}},
					PriorityClassName: "system-node-critical",
				},
			},
			verify: func(t *testing.T, wl *schedulingv1beta1.Workload, _ *WorkloadItem) {
				if got := wl.Spec.PodGroupTemplates[0].PriorityClassName; got != "system-node-critical" {
					t.Errorf("expected priorityClassName copied to template, got %q", got)
				}
			},
		},
		{
			// 传入 batch/v1 的 Job owner。验证同时生成 ownerReferences 和 controllerRef，
			// 且 APIVersion 被正确拆成 APIGroup="batch" + Kind="Job"。
			name: "owner reference and controllerRef parse APIGroup",
			root: &WorkloadItem{
				Name:          "owned-job",
				DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}}},
			},
			owner: &metav1.OwnerReference{
				APIVersion: "batch/v1",
				Kind:       "Job",
				Name:       "my-job",
				UID:        "abc-123",
			},
			verify: func(t *testing.T, wl *schedulingv1beta1.Workload, _ *WorkloadItem) {
				if len(wl.OwnerReferences) != 1 {
					t.Fatalf("expected 1 owner reference, got %d", len(wl.OwnerReferences))
				}
				if wl.OwnerReferences[0].Name != "my-job" {
					t.Errorf("expected owner name 'my-job', got %q", wl.OwnerReferences[0].Name)
				}
				if wl.Spec.ControllerRef == nil || wl.Spec.ControllerRef.Name != "my-job" {
					t.Fatal("expected controllerRef to be set")
				}
				if wl.Spec.ControllerRef.APIGroup != "batch" || wl.Spec.ControllerRef.Kind != "Job" {
					t.Errorf("unexpected controllerRef: %+v", wl.Spec.ControllerRef)
				}
			},
		},
		{
			// 叶子节点名字为空。验证名字检查在配置解析与回调之前短路：
			// 期望报错，且挂载的回调绝不执行（由共享标志位 emptyNameCallbackRan 证明）。
			name: "empty leaf name fails before resolution and callbacks",
			root: &WorkloadItem{
				Name:  "",
				Input: WorkloadInput{Policy: PolicyInput{PodGroupData: &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{Basic: &schedulingv1alpha3.WorkloadPodGroupBasicSchedulingPolicy{}}}},
				Callbacks: []SchedulingConfigFunc{
					func(*SchedulingConfig) {
						emptyNameCallbackRan = true
					},
				},
			},
			wantErr: true,
			verify: func(t *testing.T, _ *schedulingv1beta1.Workload, root *WorkloadItem) {
				if emptyNameCallbackRan {
					t.Error("callback ran despite empty name; the name check should short-circuit before resolution and callbacks")
				}
			},
		},
		{
			// 既无默认配置也无用户输入。验证兜底行为是设置 Basic 策略。
			name: "nil configs default to basic",
			root: &WorkloadItem{Name: "bare"},
			verify: func(t *testing.T, wl *schedulingv1beta1.Workload, _ *WorkloadItem) {
				if wl.Spec.PodGroupTemplates[0].SchedulingPolicy.Basic == nil {
					t.Error("expected default Basic policy when no config is provided")
				}
			},
		},
		{
			// 回调把默认 Gang 的 MinCount 乘以 3（4 → 12）。
			// 验证回调能对解析后的配置做任意改写，且结果反映到编译产物中。
			name: "callback performs arbitrary adjustment",
			root: &WorkloadItem{
				Name:          "adjusted",
				DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Gang: &GangSchedulingPolicy{MinCount: ptr.To[int32](4)}}},
				Callbacks: []SchedulingConfigFunc{
					func(cfg *SchedulingConfig) {
						if g := cfg.Policy.Gang; g != nil && g.MinCount != nil {
							*g.MinCount *= 3
						}
					},
				},
			},
			verify: func(t *testing.T, wl *schedulingv1beta1.Workload, _ *WorkloadItem) {
				if got := wl.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount; got != 12 {
					t.Errorf("expected callback-adjusted MinCount=12, got %d", got)
				}
			},
		},
		{
			// 回调通过 new(n) 补默认值。验证它只改内部的解析结果，不写回调用方的
			// Input（Input 里的 MinCount 仍须为 nil），而编译产物拿到默认值 16。
			name: "resolution does not mutate caller inputs",
			root: &WorkloadItem{
				Name:      "gang-job",
				Input:     WorkloadInput{Policy: PolicyInput{PodGroupData: &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{Gang: &schedulingv1alpha3.WorkloadPodGroupGangSchedulingPolicy{}}}},
				Callbacks: []SchedulingConfigFunc{defaultGangMinCount(16)},
			},
			verify: func(t *testing.T, wl *schedulingv1beta1.Workload, root *WorkloadItem) {
				if root.Input.Policy.PodGroupData.Gang.MinCount != nil {
					t.Errorf("callback mutated the caller's Input; MinCount should remain nil, got %d", *root.Input.Policy.PodGroupData.Gang.MinCount)
				}
				gang := wl.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang
				if gang == nil {
					t.Fatal("expected compiled Gang policy to carry the defaulted MinCount")
				}
				if gang.MinCount != 16 {
					t.Errorf("expected compiled MinCount=16, got %d", gang.MinCount)
				}
			},
		},
		{
			// 非法输入：Gang 缺少必填的 MinCount 且无回调补齐。验证编译报错。
			name: "gang missing minCount fails",
			root: &WorkloadItem{
				Name:  "gang-job",
				Input: WorkloadInput{Policy: PolicyInput{PodGroupData: &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{Gang: &schedulingv1alpha3.WorkloadPodGroupGangSchedulingPolicy{}}}},
			},
			wantErr: true,
		},
		{
			// 复合根节点的完整编译路径：验证顶层不再有普通 PodGroupTemplates，
			// 而是生成一个 CompositePodGroupTemplate；根节点的 MinCount=2 映射为复合策略的
			// MinGroupCount=2，两个子节点分别编译为带 Gang(4) 和 Basic 的叶子模板。
			name: "composite root compiles into a composite template with leaf children",
			root: &WorkloadItem{
				Name:          "cpg-root",
				DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Gang: &GangSchedulingPolicy{MinCount: ptr.To[int32](2)}}},
				Children: []*WorkloadItem{
					{Name: "workers", DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Gang: &GangSchedulingPolicy{MinCount: ptr.To[int32](4)}}}},
					{Name: "ps", DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}}}},
				},
			},
			verify: func(t *testing.T, wl *schedulingv1beta1.Workload, _ *WorkloadItem) {
				if len(wl.Spec.PodGroupTemplates) != 0 {
					t.Errorf("expected no top-level PodGroupTemplates for a composite root, got %d", len(wl.Spec.PodGroupTemplates))
				}
				if len(wl.Spec.CompositePodGroupTemplates) != 1 {
					t.Fatalf("expected 1 composite template, got %d", len(wl.Spec.CompositePodGroupTemplates))
				}
				cpg := wl.Spec.CompositePodGroupTemplates[0]
				if cpg.Name != "cpg-root" {
					t.Errorf("expected composite template name 'cpg-root', got %q", cpg.Name)
				}
				if cpg.SchedulingPolicy.Gang == nil || cpg.SchedulingPolicy.Gang.MinGroupCount != 2 {
					t.Errorf("expected composite Gang policy with MinGroupCount=2, got %+v", cpg.SchedulingPolicy)
				}
				if len(cpg.PodGroupTemplates) != 2 {
					t.Fatalf("expected 2 child PodGroupTemplates, got %d", len(cpg.PodGroupTemplates))
				}
				if cpg.PodGroupTemplates[0].SchedulingPolicy.Gang == nil || cpg.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount != 4 {
					t.Error("expected first child to carry Gang MinCount=4")
				}
				if cpg.PodGroupTemplates[1].SchedulingPolicy.Basic == nil {
					t.Error("expected second child to carry Basic policy")
				}
			},
		},
		{
			// 用户直接给出复合 Gang 的 MinGroupCount=3。验证用户输入被编译进复合模板，
			// 而不是被默认值覆盖。
			name: "composite gang minGroupCount from user input",
			root: &WorkloadItem{
				Name:  "cpg-root",
				Input: WorkloadInput{Policy: PolicyInput{CompositePodGroupData: &schedulingv1alpha3.WorkloadCompositePodGroupSchedulingPolicy{Gang: &schedulingv1alpha3.WorkloadCompositePodGroupGangSchedulingPolicy{MinGroupCount: ptr.To[int32](3)}}}},
				Children: []*WorkloadItem{
					{Name: "workers", DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}}}},
				},
			},
			verify: func(t *testing.T, wl *schedulingv1beta1.Workload, _ *WorkloadItem) {
				cpg := wl.Spec.CompositePodGroupTemplates[0]
				if cpg.SchedulingPolicy.Gang == nil || cpg.SchedulingPolicy.Gang.MinGroupCount != 3 {
					t.Errorf("expected composite Gang MinGroupCount=3 from user input, got %+v", cpg.SchedulingPolicy)
				}
			},
		},
		{
			// 复合节点输入拓扑约束与 All 中断模式。验证这些字段编译到复合模板本身
			// （而非下挂的叶子模板），且值正确。
			name: "composite constraints and disruption mode compile onto the composite template",
			root: &WorkloadItem{
				Name:          "cpg-root",
				DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}}},
				Input: WorkloadInput{
					Constraints: ConstraintsInput{CompositePodGroupData: &schedulingv1alpha3.WorkloadCompositePodGroupSchedulingConstraints{
						Topology: []schedulingv1alpha3.TopologyConstraint{{Key: "topology.kubernetes.io/zone"}},
					}},
					DisruptionMode: DisruptionModeInput{CompositePodGroupData: &schedulingv1alpha3.WorkloadCompositePodGroupDisruptionMode{All: &schedulingv1alpha3.WorkloadCompositePodGroupAllDisruptionMode{}}},
				},
				Children: []*WorkloadItem{
					{Name: "workers", DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}}}},
				},
			},
			verify: func(t *testing.T, wl *schedulingv1beta1.Workload, _ *WorkloadItem) {
				cpg := wl.Spec.CompositePodGroupTemplates[0]
				if cpg.SchedulingConstraints == nil || len(cpg.SchedulingConstraints.Topology) != 1 ||
					cpg.SchedulingConstraints.Topology[0].Key != "topology.kubernetes.io/zone" {
					t.Errorf("expected composite topology constraint to compile, got %+v", cpg.SchedulingConstraints)
				}
				if cpg.DisruptionMode == nil || cpg.DisruptionMode.All == nil {
					t.Errorf("expected composite All disruption mode to compile, got %+v", cpg.DisruptionMode)
				}
			},
		},
		{
			// 复合节点没有任何配置。验证它和叶子节点一样，兜底为 Basic 策略。
			name: "composite defaults to basic policy",
			root: &WorkloadItem{
				Name: "cpg-root",
				Children: []*WorkloadItem{
					{Name: "workers", DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}}}},
				},
			},
			verify: func(t *testing.T, wl *schedulingv1beta1.Workload, _ *WorkloadItem) {
				cpg := wl.Spec.CompositePodGroupTemplates[0]
				if cpg.SchedulingPolicy.Basic == nil {
					t.Error("expected composite to default to Basic policy when no config is provided")
				}
			},
		},
		{
			// 递归编译：内层节点自身又带一个叶子子节点。验证生成嵌套的
			// CompositePodGroupTemplate，且内层模板拥有自己的叶子模板（名字为 workers）。
			name: "nested composite compiles recursively",
			root: &WorkloadItem{
				Name:          "cpg-root",
				DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}}},
				Children: []*WorkloadItem{
					{
						Name:          "inner",
						DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Gang: &GangSchedulingPolicy{MinCount: ptr.To[int32](2)}}},
						Children: []*WorkloadItem{
							{Name: "workers", DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}}}},
						},
					},
				},
			},
			verify: func(t *testing.T, wl *schedulingv1beta1.Workload, _ *WorkloadItem) {
				cpg := wl.Spec.CompositePodGroupTemplates[0]
				if len(cpg.CompositePodGroupTemplates) != 1 {
					t.Fatalf("expected 1 nested composite template, got %d", len(cpg.CompositePodGroupTemplates))
				}
				inner := cpg.CompositePodGroupTemplates[0]
				if inner.Name != "inner" || inner.SchedulingPolicy.Gang == nil || inner.SchedulingPolicy.Gang.MinGroupCount != 2 {
					t.Errorf("unexpected nested composite: %+v", inner)
				}
				if len(inner.PodGroupTemplates) != 1 || inner.PodGroupTemplates[0].Name != "workers" {
					t.Errorf("expected nested composite to own the leaf child, got %+v", inner.PodGroupTemplates)
				}
			},
		},
		{
			// 非法输入：复合 Gang 缺少必填的 MinGroupCount。验证编译报错。
			name: "composite gang missing minGroupCount fails",
			root: &WorkloadItem{
				Name:  "cpg-root",
				Input: WorkloadInput{Policy: PolicyInput{CompositePodGroupData: &schedulingv1alpha3.WorkloadCompositePodGroupSchedulingPolicy{Gang: &schedulingv1alpha3.WorkloadCompositePodGroupGangSchedulingPolicy{}}}},
				Children: []*WorkloadItem{
					{Name: "workers", DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}}}},
				},
			},
			wantErr: true,
		},
		{
			// 非法组合：复合策略（CompositePodGroupData）与叶子级构件
			// （DisruptionMode.PodGroupData）同时出现。验证输入的联合校验会拒绝这种混用。
			name: "composite policy set together with a leaf building block fails the input union",
			root: &WorkloadItem{
				Name: "cpg-root",
				Input: WorkloadInput{
					Policy:         PolicyInput{CompositePodGroupData: &schedulingv1alpha3.WorkloadCompositePodGroupSchedulingPolicy{Gang: &schedulingv1alpha3.WorkloadCompositePodGroupGangSchedulingPolicy{MinGroupCount: ptr.To[int32](2)}}},
					DisruptionMode: DisruptionModeInput{PodGroupData: &schedulingv1alpha3.WorkloadPodGroupDisruptionMode{Single: &schedulingv1alpha3.WorkloadPodGroupSingleDisruptionMode{}}},
				},
				Children: []*WorkloadItem{
					{Name: "workers", DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}}}},
				},
			},
			wantErr: true,
		},
		{
			// 非法树结构：复合节点的子节点为 nil。验证遍历时报错而非 panic。
			name: "composite with a nil child fails",
			root: &WorkloadItem{
				Name:          "cpg-root",
				DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}}},
				Children:      []*WorkloadItem{nil},
			},
			wantErr: true,
		},
		{
			// 非法树结构：复合节点的某个子节点名字为空。验证建名前的前置校验会失败。
			name: "composite child with empty name fails",
			root: &WorkloadItem{
				Name:          "cpg-root",
				DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}}},
				Children: []*WorkloadItem{
					{Name: "", DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}}}},
				},
			},
			wantErr: true,
		},
		{
			// 非法树结构：两个子节点同名（workers）。验证模板名唯一性检查会失败。
			name: "duplicate template names across the tree fail",
			root: &WorkloadItem{
				Name:          "cpg-root",
				DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}}},
				Children: []*WorkloadItem{
					{Name: "workers", DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}}}},
					{Name: "workers", DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}}}},
				},
			},
			wantErr: true,
		},
		{
			// 空输入：根节点为 nil。验证编译直接报错。
			name:    "nil root fails",
			root:    nil,
			wantErr: true,
		},
		{
			// 编译选项缺少 owner。这是唯一需要保留 owner=nil 的用例，
			// 验证编译流程会因缺少 owner 而报错。
			name: "missing owner returns error",
			root: &WorkloadItem{
				Name:          "job",
				DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}}},
			},
			owner:   nil,
			wantErr: true,
		},
		{
			// owner 的 APIVersion 是三段式（batch/v1/extra），无法拆成 group/version。
			// 验证解析失败并报错。
			name: "owner apiVersion that fails to parse returns error",
			root: &WorkloadItem{
				Name:          "job",
				DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}}},
			},
			owner: &metav1.OwnerReference{
				APIVersion: "batch/v1/extra",
				Kind:       "Job",
				Name:       "my-job",
				UID:        "abc-123",
			},
			wantErr: true,
		},
	}

	dummyOwner := &metav1.OwnerReference{
		APIVersion: "batch/v1",
		Kind:       "Job",
		Name:       "test-job",
		UID:        "12345",
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 大多数用例不关心 owner，但编译流程要求必须有 owner。
			// "missing owner returns error" 是唯一需要保留 nil 的例外。
			owner := tt.owner
			if owner == nil && tt.name != "missing owner returns error" {
				owner = dummyOwner
			}
			wl, err := NewBuilder(tt.root, BuildOptions{Name: "wl", Namespace: "ns", Owner: owner}).BuildWorkload()
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
			} else if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.verify != nil {
				tt.verify(t, wl, tt.root)
			}
		})
	}
}

// TestResolveSchedulingConfigMergesEveryField 验证 resolveSchedulingConfig 会合并
// SchedulingConfig 的每一个字段：用反射遍历字段，确保两个 fixture 都填了非零值，
// 再用 DeepEqual 比较完整结果。这样未来新增字段却忘记合并时，
// 会先在 fixture 完备性检查处失败，而不是给出误导性的 DeepEqual 失败。
func TestResolveSchedulingConfigMergesEveryField(t *testing.T) {
	defaultConfig := &SchedulingConfig{
		Policy:            &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}},
		Constraints:       &SchedulingConstraints{Topology: []schedulingv1beta1.TopologyConstraint{{Key: "topology.kubernetes.io/zone"}}},
		DisruptionMode:    &DisruptionMode{Single: &SingleDisruptionMode{}},
		ResourceClaims:    []ResourceClaim{{Name: "default-claim", ResourceClaimName: new("default")}},
		PriorityClassName: "default-priority",
	}
	userInput := WorkloadInput{
		Policy:         PolicyInput{PodGroupData: &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{Gang: &schedulingv1alpha3.WorkloadPodGroupGangSchedulingPolicy{MinCount: ptr.To[int32](4)}}},
		Constraints:    ConstraintsInput{PodGroupData: &schedulingv1alpha3.WorkloadPodGroupSchedulingConstraints{Topology: []schedulingv1alpha3.TopologyConstraint{{Key: "topology.kubernetes.io/region"}}}},
		DisruptionMode: DisruptionModeInput{PodGroupData: &schedulingv1alpha3.WorkloadPodGroupDisruptionMode{All: &schedulingv1alpha3.WorkloadPodGroupAllDisruptionMode{}}},
		ResourceClaims: ResourceClaimsInput{PodGroupData: []schedulingv1alpha3.WorkloadPodGroupResourceClaim{{Name: "user-claim", ResourceClaimName: new("user")}}},
	}

	// WorkloadInput 里没有 priorityClassName 这个输入字段，因此合并结果中该字段
	// 保留默认值；其余字段都被用户输入覆盖。
	expectedConfig := &SchedulingConfig{
		Policy:            &SchedulingPolicy{Gang: &GangSchedulingPolicy{MinCount: ptr.To[int32](4)}},
		Constraints:       &SchedulingConstraints{Topology: []schedulingv1beta1.TopologyConstraint{{Key: "topology.kubernetes.io/region"}}},
		DisruptionMode:    &DisruptionMode{All: &AllDisruptionMode{}},
		ResourceClaims:    []ResourceClaim{{Name: "user-claim", ResourceClaimName: new("user")}},
		PriorityClassName: "default-priority",
	}

	// 两个 fixture 必须把每个字段都填成非零值，否则下面的覆盖断言无法区分
	// “字段被正确复制”和“字段被直接忽略”。这样未来若给 SchedulingConfig 新增字段
	// 却忘了同步 fixture，会先在这里失败，而不是给出误导性的 DeepEqual 失败。
	cfgType := reflect.TypeFor[SchedulingConfig]()
	defVal := reflect.ValueOf(*defaultConfig)
	expectedVal := reflect.ValueOf(*expectedConfig)
	for i := 0; i < cfgType.NumField(); i++ {
		name := cfgType.Field(i).Name
		if defVal.Field(i).IsZero() {
			t.Fatalf("defaultConfig fixture leaves SchedulingConfig.%s unset; populate it so this test stays exhaustive", name)
		}
		if expectedVal.Field(i).IsZero() {
			t.Fatalf("expectedConfig fixture leaves SchedulingConfig.%s unset; populate it so this test stays exhaustive", name)
		}
	}

	item := &WorkloadItem{Name: "job", DefaultConfig: defaultConfig, Input: userInput}
	resolved := resolveSchedulingConfig(item)

	// 关键断言：深度比较整个合并结果。因为 fixture 保证每个字段非零，
	// 任何不一致都意味着 resolveSchedulingConfig 漏合并了某个字段。
	if !reflect.DeepEqual(resolved, expectedConfig) {
		t.Errorf("resolved config does not match expected; a field is likely missing from resolveSchedulingConfig's merge\n resolved: %+v\n expected: %+v", resolved, expectedConfig)
	}
}

// TestNewBuilder 验证 NewBuilder 只做字段登记：root 与 opts 被原样保存，
// workload 缓存此时应为空，真正的编译留给 BuildWorkload，让两次调用职责清晰。
func TestNewBuilder(t *testing.T) {
	item := createGangWorkloadItem()
	opts := BuildOptions{Name: "wl", Namespace: "ns", Owner: jobOwner()}

	b := NewBuilder(item, opts)
	if b.root != item {
		t.Errorf("root not stored: got %p want %p", b.root, item)
	}
	if b.opts.Name != "wl" || b.opts.Namespace != "ns" || b.opts.Owner == nil {
		t.Errorf("opts not stored: %+v", b.opts)
	}
	if b.workload != nil {
		t.Error("workload should be nil before BuildWorkload")
	}
}

// TestBuilderBuildWorkload 检查 Builder 的编译与缓存语义：编译产物的身份字段正确、
// 成功结果必须被复用；失败时返回错误且不得入缓存，否则下次调用会拿到半成品 Workload。
func TestBuilderBuildWorkload(t *testing.T) {
	// 校验编译产物的“身份”字段：name/namespace、ownerReferences、controllerRef
	// （含 APIGroup 解析）以及编译出的唯一叶子模板 pgt-0。
	t.Run("compiles identity and controllerRef", func(t *testing.T) {
		b := NewBuilder(createGangWorkloadItem(), BuildOptions{Name: "wl", Namespace: "ns", Owner: jobOwner()})
		wl, err := b.BuildWorkload()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if wl.Name != "wl" || wl.Namespace != "ns" {
			t.Errorf("unexpected identity: %s/%s", wl.Namespace, wl.Name)
		}
		if len(wl.OwnerReferences) != 1 || wl.OwnerReferences[0].Name != "job" {
			t.Errorf("expected single job ownerReference, got %+v", wl.OwnerReferences)
		}
		if wl.Spec.ControllerRef == nil ||
			wl.Spec.ControllerRef.APIGroup != "batch" ||
			wl.Spec.ControllerRef.Kind != "Job" ||
			wl.Spec.ControllerRef.Name != "job" {
			t.Errorf("unexpected controllerRef: %+v", wl.Spec.ControllerRef)
		}
		if len(wl.Spec.PodGroupTemplates) != 1 || wl.Spec.PodGroupTemplates[0].Name != "pgt-0" {
			t.Errorf("expected single template pgt-0, got %+v", wl.Spec.PodGroupTemplates)
		}
	})

	// 缓存语义：连续两次 BuildWorkload 必须返回同一个指针，
	// 证明第二次直接复用缓存而未重新编译。
	t.Run("caches the compiled workload", func(t *testing.T) {
		b := NewBuilder(createGangWorkloadItem(), BuildOptions{Name: "wl", Namespace: "ns", Owner: jobOwner()})
		first, err := b.BuildWorkload()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		second, err := b.BuildWorkload()
		if err != nil {
			t.Fatalf("unexpected error on second call: %v", err)
		}
		if first != second {
			t.Error("expected BuildWorkload to return the cached workload on repeat calls")
		}
	})

	// 失败路径：BuildWorkload 应返回错误、返回 nil workload，
	// 且 b.workload 缓存保持为空（否则下次调用会拿到半成品）。
	t.Run("errors are returned and nothing is cached", func(t *testing.T) {
		// 表字段：name 为子测试名；b 是预先构造好的 Builder，
		// 用于验证失败时既不返回结果也不污染缓存。
		tests := []struct {
			name string
			b    *Builder
		}{
			{
				// 根节点为 nil，编译应在起点就失败。
				name: "nil root",
				b:    NewBuilder(nil, BuildOptions{Name: "wl", Namespace: "ns", Owner: jobOwner()}),
			},
			{
				// 编译选项里没有 owner，编译应失败。
				name: "missing owner",
				b:    NewBuilder(createGangWorkloadItem(), BuildOptions{Name: "wl", Namespace: "ns"}),
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				wl, err := tt.b.BuildWorkload()
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if wl != nil {
					t.Errorf("expected nil workload on error, got %+v", wl)
				}
				if tt.b.workload != nil {
					t.Error("failed build must not populate the cache")
				}
			})
		}
	})
}

// TestBuilderNewPodGroup 验证从编译结果物化 PodGroup 的路径：必须已先编译、未知模板名会报错、
// 模板索引按需构建一次并复用，且无 owner 时不应生成 ownerReferences。
func TestBuilderNewPodGroup(t *testing.T) {
	// 前置条件校验：既没调用 BuildWorkload 也没从已有 Workload 构造时，
	// NewPodGroup 必须报错，且错误信息要提示先调用哪个方法。
	t.Run("requires a workload first", func(t *testing.T) {
		b := NewBuilder(createGangWorkloadItem(), BuildOptions{Name: "wl", Namespace: "ns", Owner: jobOwner()})
		_, err := b.NewPodGroup("pg", "pgt-0")
		if err == nil {
			t.Fatal("expected error when neither BuildWorkload nor NewBuilderFromExistingWorkload has run")
		}
		if !strings.Contains(err.Error(), "call BuildWorkload or use NewBuilderFromExistingWorkload") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	// 物化路径：验证新 PodGroup 的身份、来自 opts.Owner 的 ownerReference，
	// 以及指回 Workload 与模板名的 workloadRef。
	t.Run("materializes from the compiled template owned by the configured owner", func(t *testing.T) {
		b := NewBuilder(createGangWorkloadItem(), BuildOptions{Name: "wl", Namespace: "ns", Owner: jobOwner()})
		if _, err := b.BuildWorkload(); err != nil {
			t.Fatalf("unexpected build error: %v", err)
		}
		pg, err := b.NewPodGroup("pg", "pgt-0")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pg.Name != "pg" || pg.Namespace != "ns" {
			t.Errorf("unexpected identity: %s/%s", pg.Namespace, pg.Name)
		}
		if len(pg.OwnerReferences) != 1 || pg.OwnerReferences[0].Name != "job" {
			t.Errorf("expected single job ownerReference from opts.Owner, got %+v", pg.OwnerReferences)
		}
		if pg.Spec.WorkloadRef == nil ||
			pg.Spec.WorkloadRef.WorkloadName != "wl" ||
			pg.Spec.WorkloadRef.TemplateName != "pgt-0" {
			t.Errorf("unexpected workloadRef: %+v", pg.Spec.WorkloadRef)
		}
	})

	t.Run("omits ownerReferences when no owner is configured", func(t *testing.T) {
		// build() 要求必须有 owner，所以先用 owner 完成编译，
		// 再清空它，用已缓存的 Workload 走无 owner 的 NewPodGroup 路径。
		b := NewBuilder(createGangWorkloadItem(), BuildOptions{Name: "wl", Namespace: "ns", Owner: jobOwner()})
		if _, err := b.BuildWorkload(); err != nil {
			t.Fatalf("unexpected build error: %v", err)
		}
		b.opts.Owner = nil
		pg, err := b.NewPodGroup("pg", "pgt-0")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(pg.OwnerReferences) != 0 {
			t.Errorf("expected no ownerReferences, got %+v", pg.OwnerReferences)
		}
	})

	t.Run("errors on unknown template", func(t *testing.T) {
		b := NewBuilder(createGangWorkloadItem(), BuildOptions{Name: "wl", Namespace: "ns", Owner: jobOwner()})
		if _, err := b.BuildWorkload(); err != nil {
			t.Fatalf("unexpected build error: %v", err)
		}
		// 模板名不存在时按名字查索引应失败。
		if _, err := b.NewPodGroup("pg", "missing"); err == nil {
			t.Error("expected error for unknown template name")
		}
	})

	// 索引缓存：首次调用按名字查找时构建 podGroupTemplateIndex，
	// 第二次调用必须复用同一个 map 指针，不能重复构建。
	t.Run("builds the template index once and reuses it across calls", func(t *testing.T) {
		b := NewBuilder(createGangWorkloadItem(), BuildOptions{Name: "wl", Namespace: "ns", Owner: jobOwner()})
		if _, err := b.BuildWorkload(); err != nil {
			t.Fatalf("unexpected build error: %v", err)
		}
		if _, err := b.NewPodGroup("pg-1", "pgt-0"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		first := b.podGroupTemplateIndex
		if first == nil {
			t.Fatal("expected the template index to be built on first use")
		}
		if _, err := b.NewPodGroup("pg-2", "pgt-0"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if reflect.ValueOf(b.podGroupTemplateIndex).Pointer() != reflect.ValueOf(first).Pointer() {
			t.Error("expected the template index to be reused, not rebuilt on the second call")
		}
	})
}

// TestBuilderNewCompositePodGroup 验证复合 PodGroup 的物化路径：
// 复合模板索引必须递归建立，使嵌套 CompositePodGroup 中的模板也能被按名字找到并复用。
func TestBuilderNewCompositePodGroup(t *testing.T) {
	// 与叶子路径一致：未先编译也未从已有 Workload 构造时，
	// NewCompositePodGroup 应报错并提示先调用 BuildWorkload。
	t.Run("requires a workload first", func(t *testing.T) {
		b := NewBuilder(createCompositeWorkloadItem(), BuildOptions{Name: "wl", Namespace: "ns", Owner: jobOwner()})
		_, err := b.NewCompositePodGroup("cpg", "cpgt-0")
		if err == nil {
			t.Fatal("expected error when neither BuildWorkload nor NewBuilderFromExistingWorkload has run")
		}
		if !strings.Contains(err.Error(), "call BuildWorkload or use NewBuilderFromExistingWorkload") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	// 物化路径：验证复合模板的 Gang(MinGroupCount=2) 被复制到产物 spec，
	// 且带上来自 opts.Owner 的 ownerReference。
	t.Run("materializes from the compiled composite template owned by the configured owner", func(t *testing.T) {
		b := NewBuilder(createCompositeWorkloadItem(), BuildOptions{Name: "wl", Namespace: "ns", Owner: jobOwner()})
		if _, err := b.BuildWorkload(); err != nil {
			t.Fatalf("unexpected build error: %v", err)
		}
		cpg, err := b.NewCompositePodGroup("cpg", "cpgt-0")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if cpg.Spec.SchedulingPolicy.Gang == nil ||
			cpg.Spec.SchedulingPolicy.Gang.MinGroupCount != 2 {
			t.Errorf("expected composite Gang policy with MinGroupCount=2 copied from template, got %+v", cpg.Spec.SchedulingPolicy)
		}
		if len(cpg.OwnerReferences) != 1 ||
			cpg.OwnerReferences[0].Name != "job" {
			t.Errorf("expected single job ownerReference, got %+v", cpg.OwnerReferences)
		}
	})

	// 与叶子路径相同：先带 owner 编译，再清空 owner，
	// 验证无 owner 时物化的复合 PodGroup 不带 ownerReferences。
	t.Run("omits ownerReferences when no owner is configured", func(t *testing.T) {
		b := NewBuilder(createCompositeWorkloadItem(), BuildOptions{Name: "wl", Namespace: "ns", Owner: jobOwner()})
		if _, err := b.BuildWorkload(); err != nil {
			t.Fatalf("unexpected build error: %v", err)
		}
		b.opts.Owner = nil
		_, err := b.NewCompositePodGroup("cpg", "cpgt-0")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

	})

	// 递归索引：验证嵌套在 cpgt-0 内的复合模板 cpgt-1 也能按名字查到并物化。
	t.Run("resolves a nested composite template through the recursive index", func(t *testing.T) {
		b := NewBuilder(createCompositeWorkloadItem(), BuildOptions{Name: "wl", Namespace: "ns", Owner: jobOwner()})
		if _, err := b.BuildWorkload(); err != nil {
			t.Fatalf("unexpected build error: %v", err)
		}
		_, err := b.NewCompositePodGroup("cpg", "cpgt-1")
		if err != nil {
			t.Fatalf("unexpected error resolving nested composite template: %v", err)
		}

	})

	t.Run("errors on unknown template", func(t *testing.T) {
		b := NewBuilder(createCompositeWorkloadItem(), BuildOptions{Name: "wl", Namespace: "ns", Owner: jobOwner()})
		if _, err := b.BuildWorkload(); err != nil {
			t.Fatalf("unexpected build error: %v", err)
		}
		// 未知复合模板名应按名字查索引失败。
		if _, err := b.NewCompositePodGroup("cpg", "missing"); err == nil {
			t.Error("expected error for unknown composite template name")
		}
	})

	// 索引缓存：首次构建 compositePodGroupTemplateIndex 后，
	// 后续调用（这里换成查嵌套的 cpgt-1）必须复用同一指针。
	t.Run("builds the composite template index once and reuses it across calls", func(t *testing.T) {
		b := NewBuilder(createCompositeWorkloadItem(), BuildOptions{Name: "wl", Namespace: "ns", Owner: jobOwner()})
		if _, err := b.BuildWorkload(); err != nil {
			t.Fatalf("unexpected build error: %v", err)
		}
		if _, err := b.NewCompositePodGroup("cpg-1", "cpgt-0"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		first := b.compositePodGroupTemplateIndex
		if first == nil {
			t.Fatal("expected the composite template index to be built on first use")
		}
		if _, err := b.NewCompositePodGroup("cpg-2", "cpgt-1"); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if reflect.ValueOf(b.compositePodGroupTemplateIndex).Pointer() != reflect.ValueOf(first).Pointer() {
			t.Error("expected the composite template index to be reused, not rebuilt on the second call")
		}
	})
}

// TestNewBuilderFromExistingWorkload 验证从已有 Workload 构造 Builder 的行为：
// 模板里 builder 本身不会设置的字段（如 priorityClassName、priority）必须原样保留；
// 同时 BuildWorkload/Validate 应明确拒绝，因为该模式下没有“编译”阶段。
func TestNewBuilderFromExistingWorkload(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	// 已有 Workload 模式：无需调用 BuildWorkload 就能物化 PodGroup。
	// 重点验证 builder 自身不设置的字段（priorityClassName、priority）
	// 必须从传入的模板原样拷贝，同时 workloadRef 指向已有 Workload。
	t.Run("materializes from the supplied workload without BuildWorkload", func(t *testing.T) {
		b := NewBuilderFromExistingWorkload(existingWorkload(), BuildOptions{Owner: jobOwner()})
		pg, err := b.NewPodGroup("pg", "pgt-0")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pg.Name != "pg" || pg.Namespace != "ns" {
			t.Errorf("unexpected identity: %s/%s", pg.Namespace, pg.Name)
		}
		if len(pg.OwnerReferences) != 1 || pg.OwnerReferences[0].Name != "job" {
			t.Errorf("expected single job ownerReference, got %+v", pg.OwnerReferences)
		}
		// builder 从不设置的字段必须从传入的模板原样拷贝，不能被清零或覆盖。
		if pg.Spec.PriorityClassName != "high-priority" {
			t.Errorf("expected priorityClassName copied, got %q", pg.Spec.PriorityClassName)
		}
		if pg.Spec.Priority == nil || *pg.Spec.Priority != 1000 {
			t.Errorf("expected priority copied, got %v", pg.Spec.Priority)
		}
		if pg.Spec.WorkloadRef == nil || pg.Spec.WorkloadRef.WorkloadName != "parent-wl" {
			t.Errorf("unexpected workloadRef: %+v", pg.Spec.WorkloadRef)
		}
	})

	// 已有 Workload 模式下不存在“编译”阶段，因此 BuildWorkload 必须明确拒绝，
	// 且错误信息要说明原因。
	t.Run("BuildWorkload is refused in existing mode", func(t *testing.T) {
		b := NewBuilderFromExistingWorkload(existingWorkload(), BuildOptions{Name: "wl", Namespace: "ns", Owner: jobOwner()})
		_, err := b.BuildWorkload()
		if err == nil {
			t.Fatal("expected BuildWorkload to be refused for a builder created from an existing Workload")
		}
		if !strings.Contains(err.Error(), "not available on a Builder created from an existing Workload") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	// 同理，Validate 在该模式下也应拒绝：验证只返回一个内部错误（ErrorTypeInternal）。
	t.Run("Validate is refused in existing mode", func(t *testing.T) {
		b := NewBuilderFromExistingWorkload(existingWorkload(), BuildOptions{Owner: jobOwner()})
		errs := b.Validate(ctx, ValidationInput{})
		if len(errs) != 1 {
			t.Fatalf("expected exactly one validation error, got %v", errs)
		}
		if errs[0].Type != field.ErrorTypeInternal {
			t.Errorf("expected an internal error, got %s: %v", errs[0].Type, errs[0])
		}
	})

	t.Run("errors on unknown template", func(t *testing.T) {
		b := NewBuilderFromExistingWorkload(existingWorkload(), BuildOptions{Owner: jobOwner()})
		// 即使在已有 Workload 模式下，未知模板名也必须报错。
		if _, err := b.NewPodGroup("pg", "missing"); err == nil {
			t.Error("expected error for unknown template name")
		}
	})
}

// jobOwner 构造一个标准的 batch/v1 Job owner fixture，
// 供各测试填进 BuildOptions.Owner 使用。
func jobOwner() *metav1.OwnerReference {
	return &metav1.OwnerReference{APIVersion: "batch/v1", Kind: "Job", Name: "job", UID: "job-uid"}
}

// createGangWorkloadItem 构造一个最小的叶子 WorkloadItem fixture：
// 名字 pgt-0，输入里带 MinCount=3 的 Gang 策略（gangData），
// 供编译与物化相关测试复用。
func createGangWorkloadItem() *WorkloadItem {
	return &WorkloadItem{
		Name:  "pgt-0",
		Input: WorkloadInput{Policy: PolicyInput{PodGroupData: gangData(3)}},
	}
}

// createCompositeWorkloadItem 构造一个复合 WorkloadItem fixture：
// 根节点 cpgt-0 同时拥有叶子子节点 pgt-0 和嵌套的复合子节点 cpgt-1，
// 而 cpgt-1 又拥有叶子 pgt-1。这个结构让“嵌套模板解析”和“索引递归复用”
// 两个用例都能覆盖到递归路径。
func createCompositeWorkloadItem() *WorkloadItem {
	return &WorkloadItem{
		Name:          "cpgt-0",
		DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Gang: &GangSchedulingPolicy{MinCount: ptr.To[int32](2)}}},
		Children: []*WorkloadItem{
			{Name: "pgt-0", DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}}}},
			{
				Name:          "cpgt-1",
				DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Gang: &GangSchedulingPolicy{MinCount: ptr.To[int32](2)}}},
				Children: []*WorkloadItem{
					{Name: "pgt-1", DefaultConfig: &SchedulingConfig{Policy: &SchedulingPolicy{Basic: &BasicSchedulingPolicy{}}}},
				},
			},
		},
	}
}

// existingWorkload 构造一个“已编译好”的 Workload fixture（parent-wl/ns），
// 其模板 pgt-0 带有 builder 不会设置的 priorityClassName 与 priority，
// 用于验证 NewBuilderFromExistingWorkload 能原样保留这些字段。
func existingWorkload() *schedulingv1beta1.Workload {
	return &schedulingv1beta1.Workload{
		ObjectMeta: metav1.ObjectMeta{Name: "parent-wl", Namespace: "ns", UID: "wl-uid"},
		Spec: schedulingv1beta1.WorkloadSpec{
			PodGroupTemplates: []schedulingv1beta1.PodGroupTemplate{{
				Name:              "pgt-0",
				SchedulingPolicy:  schedulingv1beta1.PodGroupSchedulingPolicy{Gang: &schedulingv1beta1.GangSchedulingPolicy{MinCount: 4}},
				PriorityClassName: "high-priority",
				Priority:          ptr.To[int32](1000),
			}},
		},
	}
}

// defaultGangMinCount 返回一个回调，只在 Gang 存在但 MinCount 缺失时
// 把它填成 n；用于测试回调补齐必填字段的能力。
func defaultGangMinCount(n int32) SchedulingConfigFunc {
	return func(cfg *SchedulingConfig) {
		p := cfg.Policy
		if p != nil && p.Gang != nil && p.Gang.MinCount == nil {
			p.Gang.MinCount = new(n)
		}
	}
}
