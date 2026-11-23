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

// gangWorkload 构造一份字段齐全的 Workload，作为“模板应被完整复制”的基准样本。
func gangWorkload() *schedulingv1beta1.Workload {
	return &schedulingv1beta1.Workload{
		ObjectMeta: metav1.ObjectMeta{Name: "job-abc", Namespace: "training"},
		Spec: schedulingv1beta1.WorkloadSpec{
			PodGroupTemplates: []schedulingv1beta1.PodGroupTemplate{
				{
					Name:             "workers",
					SchedulingPolicy: schedulingv1beta1.PodGroupSchedulingPolicy{Gang: &schedulingv1beta1.GangSchedulingPolicy{MinCount: 4}},
					SchedulingConstraints: &schedulingv1beta1.PodGroupSchedulingConstraints{
						Topology: []schedulingv1beta1.TopologyConstraint{{Key: "topology.kubernetes.io/zone"}},
					},
					DisruptionMode:    &schedulingv1beta1.DisruptionMode{All: &schedulingv1beta1.AllDisruptionMode{}},
					ResourceClaims:    []schedulingv1beta1.PodGroupResourceClaim{{Name: "gpu", ResourceClaimName: new("shared-gpu")}},
					PriorityClassName: "high",
					Priority:          ptr.To[int32](1000),
				},
			},
		},
	}
}

// TestNewPodGroup 既检查模板字段是否完整复制到 PodGroup，
// 也检查生成对象是否与模板共享底层内存（深拷贝是否到位）。
func TestNewPodGroup(t *testing.T) {
	tests := []struct {
		name         string
		workload     *schedulingv1beta1.Workload
		pickTemplate func(*schedulingv1beta1.Workload) *schedulingv1beta1.PodGroupTemplate
		podGroupName string
		owners       []metav1.OwnerReference
		verify       func(t *testing.T, wl *schedulingv1beta1.Workload, pg *schedulingv1beta1.PodGroup)
	}{
		{
			// 完整复制用例：名字、命名空间、ownerRef、workloadRef 以及全部调度字段都要核对。
			name:     "materializes every field from the template",
			workload: gangWorkload(),
			pickTemplate: func(wl *schedulingv1beta1.Workload) *schedulingv1beta1.PodGroupTemplate {
				return &wl.Spec.PodGroupTemplates[0]
			},
			podGroupName: "job-abc-workers-xyz",
			owners: []metav1.OwnerReference{
				{APIVersion: "batch/v1", Kind: "Job", Name: "job", Controller: new(true)},
				{APIVersion: "scheduling.k8s.io/v1alpha3", Kind: "Workload", Name: "job-abc"},
			},
			verify: func(t *testing.T, _ *schedulingv1beta1.Workload, pg *schedulingv1beta1.PodGroup) {
				if pg.Name != "job-abc-workers-xyz" {
					t.Errorf("unexpected name: %q", pg.Name)
				}
				if pg.Namespace != "training" {
					t.Errorf("expected namespace inherited from workload, got %q", pg.Namespace)
				}
				if len(pg.OwnerReferences) != 2 {
					t.Errorf("expected 2 owner references, got %d", len(pg.OwnerReferences))
				}
				ref := pg.Spec.WorkloadRef
				if ref == nil || ref.WorkloadName != "job-abc" || ref.TemplateName != "workers" {
					t.Errorf("unexpected template ref: %+v", ref)
				}
				if pg.Spec.SchedulingPolicy.Gang == nil || pg.Spec.SchedulingPolicy.Gang.MinCount != 4 {
					t.Error("expected gang policy with MinCount=4 copied from template")
				}
				if pg.Spec.SchedulingConstraints == nil || len(pg.Spec.SchedulingConstraints.Topology) != 1 {
					t.Error("expected topology constraint copied from template")
				}
				if pg.Spec.DisruptionMode == nil || pg.Spec.DisruptionMode.All == nil {
					t.Error("expected All disruption mode copied from template")
				}
				if len(pg.Spec.ResourceClaims) != 1 || pg.Spec.ResourceClaims[0].Name != "gpu" {
					t.Error("expected resource claim copied from template")
				}
				if pg.Spec.PriorityClassName != "high" {
					t.Errorf("expected priorityClassName copied, got %q", pg.Spec.PriorityClassName)
				}
				if pg.Spec.Priority == nil || *pg.Spec.Priority != 1000 {
					t.Error("expected priority copied from template")
				}
			},
		},
		{
			// 深拷贝用例：改动生成对象后，模板原值不能被改写。
			name:     "does not alias the workload template",
			workload: gangWorkload(),
			pickTemplate: func(wl *schedulingv1beta1.Workload) *schedulingv1beta1.PodGroupTemplate {
				return &wl.Spec.PodGroupTemplates[0]
			},
			podGroupName: "pg",
			verify: func(t *testing.T, wl *schedulingv1beta1.Workload, pg *schedulingv1beta1.PodGroup) {
				// 修改 PodGroup 不得回写到模板。
				// 逐个改写指针指向的值和切片元素，确认生成对象与模板没有共享底层内存。
				pg.Spec.SchedulingPolicy.Gang.MinCount = 99
				pg.Spec.SchedulingConstraints.Topology[0].Key = "changed"
				pg.Spec.ResourceClaims[0].Name = "changed"
				*pg.Spec.Priority = 1

				tmpl := wl.Spec.PodGroupTemplates[0]
				if tmpl.SchedulingPolicy.Gang.MinCount != 4 {
					t.Error("template MinCount mutated through the PodGroup")
				}
				if tmpl.SchedulingConstraints.Topology[0].Key != "topology.kubernetes.io/zone" {
					t.Error("template topology mutated through the PodGroup")
				}
				if tmpl.ResourceClaims[0].Name != "gpu" {
					t.Error("template resource claim mutated through the PodGroup")
				}
				if *tmpl.Priority != 1000 {
					t.Error("template priority mutated through the PodGroup")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pg := newPodGroup(tt.workload, tt.pickTemplate(tt.workload), tt.podGroupName, tt.owners)
			tt.verify(t, tt.workload, pg)
		})
	}
}

// compositeWorkload 构造含有嵌套子节点的复合模板样本，用于验证递归结构的复制行为。
func compositeWorkload() *schedulingv1beta1.Workload {
	return &schedulingv1beta1.Workload{
		ObjectMeta: metav1.ObjectMeta{Name: "job-abc", Namespace: "training"},
		Spec: schedulingv1beta1.WorkloadSpec{
			CompositePodGroupTemplates: []schedulingv1beta1.CompositePodGroupTemplate{
				{
					Name:              "top",
					SchedulingPolicy:  schedulingv1beta1.CompositePodGroupSchedulingPolicy{Gang: &schedulingv1beta1.CompositeGangSchedulingPolicy{MinGroupCount: 2}},
					PriorityClassName: "high",
					Priority:          ptr.To[int32](1000),
					CompositePodGroupTemplates: []schedulingv1beta1.CompositePodGroupTemplate{
						{
							Name:             "nested",
							SchedulingPolicy: schedulingv1beta1.CompositePodGroupSchedulingPolicy{Basic: &schedulingv1beta1.CompositeBasicSchedulingPolicy{}},
						},
					},
				},
			},
		},
	}
}

// TestNewCompositePodGroup 覆盖顶层与嵌套模板的物化、owner 可选性，
// 以及复合对象同样不能通过修改反过来污染模板。
func TestNewCompositePodGroup(t *testing.T) {
	owners := []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: "job", UID: "job-uid"}}
	tests := []struct {
		name         string
		workload     *schedulingv1beta1.Workload
		pickTemplate func(*schedulingv1beta1.Workload) *schedulingv1beta1.CompositePodGroupTemplate
		cpgName      string
		owners       []metav1.OwnerReference

		verify func(t *testing.T, wl *schedulingv1beta1.Workload, cpg *schedulingv1alpha3.CompositePodGroup)
	}{
		{
			// 顶层复合模板：核对身份、ownerRef、workloadRef 与调度字段是否完整复制。
			name:     "materializes a top-level composite template",
			workload: compositeWorkload(),
			pickTemplate: func(wl *schedulingv1beta1.Workload) *schedulingv1beta1.CompositePodGroupTemplate {
				return &wl.Spec.CompositePodGroupTemplates[0]
			},
			cpgName: "job-abc-top-xyz",
			owners:  owners,
			verify: func(t *testing.T, wl *schedulingv1beta1.Workload, cpg *schedulingv1alpha3.CompositePodGroup) {
				if cpg.Name != "job-abc-top-xyz" {
					t.Errorf("unexpected name: %q", cpg.Name)
				}
				if cpg.Namespace != wl.Namespace {
					t.Errorf("expected namespace %q, got %q", wl.Namespace, cpg.Namespace)
				}
				if len(cpg.OwnerReferences) != 1 || cpg.OwnerReferences[0].Name != "job" {
					t.Errorf("expected single job ownerReference, got %+v", cpg.OwnerReferences)
				}
				if cpg.Spec.WorkloadRef == nil || cpg.Spec.WorkloadRef.WorkloadName != wl.Name || cpg.Spec.WorkloadRef.TemplateName != "top" {
					t.Errorf("unexpected workloadRef: %+v", cpg.Spec.WorkloadRef)
				}
				if cpg.Spec.SchedulingPolicy.Gang == nil || cpg.Spec.SchedulingPolicy.Gang.MinGroupCount != 2 {
					t.Error("expected gang policy with MinGroupCount=2 copied from template")
				}
				if cpg.Spec.PriorityClassName != "high" {
					t.Errorf("expected priorityClassName copied, got %q", cpg.Spec.PriorityClassName)
				}
				if cpg.Spec.Priority == nil || *cpg.Spec.Priority != 1000 {
					t.Error("expected priority copied from template")
				}
			},
		},
		{
			// 嵌套复合模板：验证递归结构中的子节点也能被正确物化（此处为 Basic 策略）。
			name:     "materializes a nested composite template",
			workload: compositeWorkload(),
			pickTemplate: func(wl *schedulingv1beta1.Workload) *schedulingv1beta1.CompositePodGroupTemplate {
				return &wl.Spec.CompositePodGroupTemplates[0].CompositePodGroupTemplates[0]
			},
			cpgName: "job-abc-nested-xyz",
			verify: func(t *testing.T, _ *schedulingv1beta1.Workload, cpg *schedulingv1alpha3.CompositePodGroup) {

				if cpg.Spec.SchedulingPolicy.Basic == nil {
					t.Error("expected basic policy copied from the nested template")
				}
			},
		},
		{
			// 未配置 owner 时：生成对象不应带任何 ownerReferences。
			name:     "omits ownerReferences when none are configured",
			workload: compositeWorkload(),
			pickTemplate: func(wl *schedulingv1beta1.Workload) *schedulingv1beta1.CompositePodGroupTemplate {
				return &wl.Spec.CompositePodGroupTemplates[0]
			},
			cpgName: "cpg",
			verify: func(t *testing.T, _ *schedulingv1beta1.Workload, cpg *schedulingv1alpha3.CompositePodGroup) {
				if cpg.OwnerReferences != nil {
					t.Errorf("expected no ownerReferences, got %+v", cpg.OwnerReferences)
				}
			},
		},
		{
			// 深拷贝用例：改动 CompositePodGroup 后，模板原值必须保持不变。
			name:     "does not alias the workload template",
			workload: compositeWorkload(),
			pickTemplate: func(wl *schedulingv1beta1.Workload) *schedulingv1beta1.CompositePodGroupTemplate {
				return &wl.Spec.CompositePodGroupTemplates[0]
			},
			cpgName: "cpg",
			verify: func(t *testing.T, wl *schedulingv1beta1.Workload, cpg *schedulingv1alpha3.CompositePodGroup) {
				// 修改 CompositePodGroup 不得回写到模板。
				// 与 PodGroup 用例同理，改动生成对象不应污染复合模板。
				cpg.Spec.SchedulingPolicy.Gang.MinGroupCount = 99
				*cpg.Spec.Priority = 1

				tmpl := wl.Spec.CompositePodGroupTemplates[0]
				if tmpl.SchedulingPolicy.Gang.MinGroupCount != 2 {
					t.Error("template MinGroupCount mutated through the CompositePodGroup")
				}
				if *tmpl.Priority != 1000 {
					t.Error("template priority mutated through the CompositePodGroup")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 转换函数可能报错，先确保物化成功再执行字段检查。
			cpg, err := newCompositePodGroup(tt.workload, tt.pickTemplate(tt.workload), tt.cpgName, tt.owners)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			tt.verify(t, tt.workload, cpg)
		})
	}
}
