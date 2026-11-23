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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// newPodGroup 根据给定的 PodGroupTemplate 物化出一个运行时 PodGroup
// （BuildWorkload 本身不会创建 PodGroup）。模板的调度字段会被深拷贝进 spec，
// workloadRef 指回模板本身，PodGroup 落在 Workload 所在的命名空间。
// 模板查找由调用方负责（Builder 通过缓存的名字索引完成）。
// 模板字段一律深拷贝，是因为 Workload 缓存在 Builder 里会被反复使用；
// 运行时 PodGroup 如果被外部改动，不能反过来改坏模板。
func newPodGroup(workload *schedulingv1beta1.Workload, tmpl *schedulingv1beta1.PodGroupTemplate, podGroupName string, owners []metav1.OwnerReference) *schedulingv1beta1.PodGroup {
	spec := schedulingv1beta1.PodGroupSpec{
		WorkloadRef: &schedulingv1beta1.WorkloadReference{
			WorkloadName: workload.Name,
			TemplateName: tmpl.Name,
		},
		SchedulingPolicy:      *tmpl.SchedulingPolicy.DeepCopy(),
		SchedulingConstraints: tmpl.SchedulingConstraints.DeepCopy(),
		DisruptionMode:        tmpl.DisruptionMode.DeepCopy(),
		PriorityClassName:     tmpl.PriorityClassName,
	}
	if len(tmpl.ResourceClaims) > 0 {
		spec.ResourceClaims = make([]schedulingv1beta1.PodGroupResourceClaim, len(tmpl.ResourceClaims))
		for i := range tmpl.ResourceClaims {
			tmpl.ResourceClaims[i].DeepCopyInto(&spec.ResourceClaims[i])
		}
	}
	// 指针字段重新分配而不是共享地址，避免运行时对象与模板共享同一个可写位置。
	if tmpl.Priority != nil {
		spec.Priority = new(*tmpl.Priority)
	}

	return &schedulingv1beta1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:            podGroupName,
			Namespace:       workload.Namespace,
			OwnerReferences: owners,
		},
		Spec: spec,
	}
}

// newCompositePodGroup 根据给定的 CompositePodGroupTemplate 物化出一个运行时
// CompositePodGroup（BuildWorkload 本身不会创建它）。
// 这里要把 v1beta1 模板转换成 v1alpha3 运行时对象，是因为 CompositePodGroup
// 目前在 alpha 版本提供；转换函数的错误必须向上传播，否则会物化出缺字段的对象。
func newCompositePodGroup(workload *schedulingv1beta1.Workload, tmpl *schedulingv1beta1.CompositePodGroupTemplate,
	compositePodGroupName string, owners []metav1.OwnerReference) (*schedulingv1alpha3.CompositePodGroup, error) {
	spec := schedulingv1alpha3.CompositePodGroupSpec{
		WorkloadRef: &schedulingv1alpha3.WorkloadReference{
			WorkloadName: workload.Name,
			TemplateName: tmpl.Name,
		},
		PriorityClassName: tmpl.PriorityClassName,
	}

	if err := Convert_v1beta1_CompositePodGroupSchedulingPolicy_To_v1alpha3_CompositePodGroupSchedulingPolicy(&tmpl.SchedulingPolicy, &spec.SchedulingPolicy, nil); err != nil {
		return nil, err
	}
	if tmpl.SchedulingConstraints != nil {
		spec.SchedulingConstraints = &schedulingv1alpha3.CompositePodGroupSchedulingConstraints{}
		if err := Convert_v1beta1_CompositePodGroupSchedulingConstraints_To_v1alpha3_CompositePodGroupSchedulingConstraints(tmpl.SchedulingConstraints, spec.SchedulingConstraints, nil); err != nil {
			return nil, err
		}
	}
	if tmpl.DisruptionMode != nil {
		spec.DisruptionMode = &schedulingv1alpha3.CompositeDisruptionMode{}
		if err := Convert_v1beta1_CompositeDisruptionMode_To_v1alpha3_CompositeDisruptionMode(tmpl.DisruptionMode, spec.DisruptionMode, nil); err != nil {
			return nil, err
		}
	}
	if tmpl.Priority != nil {
		spec.Priority = new(*tmpl.Priority)
	}

	cpg := &schedulingv1alpha3.CompositePodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:            compositePodGroupName,
			Namespace:       workload.Namespace,
			OwnerReferences: owners,
		},
		Spec: spec,
	}
	// 生成的 Convert_* 辅助函数只做浅拷贝（unsafe.Pointer），
	// 因此 spec 仍与模板的 Gang/Topology 指针共享存储。返回前再 DeepCopy 一次，
	// 这样调用方修改 CompositePodGroup 时不会破坏缓存在 Builder 中的 Workload 模板。
	return cpg.DeepCopy(), nil
}

// indexPodGroupTemplates 按名字为每个叶子 PodGroupTemplate 建立索引，实现 O(1) 查找。
// Builder 只构建一次并在多次物化间复用，避免每次调用 NewPodGroup 都重新扫描切片。
func indexPodGroupTemplates(workload *schedulingv1beta1.Workload) map[string]*schedulingv1beta1.PodGroupTemplate {
	index := make(map[string]*schedulingv1beta1.PodGroupTemplate, len(workload.Spec.PodGroupTemplates))
	for i := range workload.Spec.PodGroupTemplates {
		index[workload.Spec.PodGroupTemplates[i].Name] = &workload.Spec.PodGroupTemplates[i]
	}
	return index
}

// podGroupTemplateNames 收集所有叶子模板名，给错误信息提供候选名字列表，
// 让拼错模板名的用户能直接看到可用值，而不是只收到一句“模板不存在”。
func podGroupTemplateNames(workload *schedulingv1beta1.Workload) []string {
	names := make([]string, len(workload.Spec.PodGroupTemplates))
	for i := range workload.Spec.PodGroupTemplates {
		names[i] = workload.Spec.PodGroupTemplates[i].Name
	}
	return names
}

// walkCompositePodGroupTemplates 以先序遍历的方式对复合模板树中的每个模板调用 visit。
func walkCompositePodGroupTemplates(tmpls []schedulingv1beta1.CompositePodGroupTemplate,
	visit func(*schedulingv1beta1.CompositePodGroupTemplate)) {
	for i := range tmpls {
		visit(&tmpls[i])
		walkCompositePodGroupTemplates(tmpls[i].CompositePodGroupTemplates, visit)
	}
}

// indexCompositePodGroupTemplates 按名字为 Workload 复合模板树中的每个
// CompositePodGroupTemplate 建立索引。模板名在整个 Workload 内唯一，
// 因此用扁平的 map 不会产生歧义。Builder 只构建一次并在多次物化间复用，
// 避免每次调用 NewCompositePodGroup 都重新遍历整棵树。
func indexCompositePodGroupTemplates(workload *schedulingv1beta1.Workload) map[string]*schedulingv1beta1.CompositePodGroupTemplate {
	index := map[string]*schedulingv1beta1.CompositePodGroupTemplate{}
	walkCompositePodGroupTemplates(workload.Spec.CompositePodGroupTemplates,
		func(tmpl *schedulingv1beta1.CompositePodGroupTemplate) {
			index[tmpl.Name] = tmpl
		})
	return index
}

// compositePodGroupTemplateNames 列出 Workload 复合模板树中每个
// CompositePodGroupTemplate 的名字，用于拼装错误信息中的候选列表。
func compositePodGroupTemplateNames(workload *schedulingv1beta1.Workload) []string {
	var names []string
	walkCompositePodGroupTemplates(workload.Spec.CompositePodGroupTemplates,
		func(tmpl *schedulingv1beta1.CompositePodGroupTemplate) {
			names = append(names, tmpl.Name)
		})
	return names
}
