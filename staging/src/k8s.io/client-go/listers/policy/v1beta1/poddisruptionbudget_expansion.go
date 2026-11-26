/*
Copyright 2017 The Kubernetes Authors.

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

package v1beta1

import (
	"fmt"

	"k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// PodDisruptionBudgetListerExpansion 允许向 PodDisruptionBudgetLister 添加自定义方法。
// 这里是 lister 的扩展点：生成代码保留该方法，手写的扩展逻辑通过内嵌接口实现。
type PodDisruptionBudgetListerExpansion interface {
	// GetPodPodDisruptionBudgets 返回与给定 pod 标签匹配的所有 PDB。
	GetPodPodDisruptionBudgets(pod *v1.Pod) ([]*policy.PodDisruptionBudget, error)
}

// PodDisruptionBudgetNamespaceListerExpansion 允许向 PodDisruptionBudgetNamespaceLister 添加自定义方法。
// 目前没有命名空间级扩展方法，仅保留占位接口，便于日后在不破坏兼容性的前提下新增。
type PodDisruptionBudgetNamespaceListerExpansion interface{}

// GetPodPodDisruptionBudgets 返回与 Pod 匹配的 PDB 列表，找不到任何候选时才返回错误。
func (s *podDisruptionBudgetLister) GetPodPodDisruptionBudgets(pod *v1.Pod) ([]*policy.PodDisruptionBudget, error) {
	var selector labels.Selector

	// 只关心与 pod 同命名空间下的 PDB；PDB 的选择器不能跨命名空间匹配。
	list, err := s.PodDisruptionBudgets(pod.Namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	var pdbList []*policy.PodDisruptionBudget
	for i := range list {
		pdb := list[i]
		selector, err = metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
		if err != nil {
			// selector 无效即视为与 Pod 不匹配。
			continue
		}

		// 若混入 nil 或空选择器的 PDB，它应匹配零个对象而非全部。
		// 这一点与 ReplicaSet 等控制器不同：空选择器会匹配所有 pod，而 PDB 的空选择器应视为不匹配。
		if selector.Empty() || !selector.Matches(labels.Set(pod.Labels)) {
			continue
		}
		pdbList = append(pdbList, pdb)
	}

	if len(pdbList) == 0 {
		return nil, fmt.Errorf("could not find PodDisruptionBudget for pod %s in namespace %s with labels: %v", pod.Name, pod.Namespace, pod.Labels)
	}

	return pdbList, nil
}
