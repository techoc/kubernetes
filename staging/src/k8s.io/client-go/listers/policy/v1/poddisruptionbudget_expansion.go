/*
Copyright 2021 The Kubernetes Authors.

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

package v1

import (
	"fmt"

	"k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// PodDisruptionBudgetListerExpansion allows custom methods to be added to
// PodDisruptionBudgetLister.
//
// PodDisruptionBudgetListerExpansion 为生成的 PDB Lister 补充跨对象查询方法。
type PodDisruptionBudgetListerExpansion interface {
	GetPodPodDisruptionBudgets(pod *v1.Pod) ([]*policy.PodDisruptionBudget, error)
}

// PodDisruptionBudgetNamespaceListerExpansion allows custom methods to be added to
// PodDisruptionBudgetNamespaceLister.
//
// 当前没有命名空间级扩展方法，保留空接口是为了与代码生成器的扩展点约定一致。
type PodDisruptionBudgetNamespaceListerExpansion interface{}

// GetPodPodDisruptionBudgets returns a list of PodDisruptionBudgets matching a pod.
//
// 返回结果可能包含多个 PDB：v1 语义允许一个 Pod 同时被多个 PDB 选中，
// 调用方需要按各自的 disruptionsAllowed 综合判断能否驱逐。
func (s *podDisruptionBudgetLister) GetPodPodDisruptionBudgets(pod *v1.Pod) ([]*policy.PodDisruptionBudget, error) {
	var selector labels.Selector

	// 先按命名空间取出全部 PDB，再在客户端做标签匹配；Lister 缓存本身不做选择器过滤。
	list, err := s.PodDisruptionBudgets(pod.Namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	var pdbList []*policy.PodDisruptionBudget
	for i := range list {
		pdb := list[i]
		selector, err = metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
		if err != nil {
			// 该对象的 selector 非法，视为不匹配该 pod，避免因为单个坏对象影响整体结果。
			continue
		}

		// Unlike the v1beta version, here we let an empty selector match everything.
		if !selector.Matches(labels.Set(pod.Labels)) {
			continue
		}
		pdbList = append(pdbList, pdb)
	}

	if len(pdbList) == 0 {
		return nil, fmt.Errorf("could not find PodDisruptionBudget for pod %s in namespace %s with labels: %v", pod.Name, pod.Namespace, pod.Labels)
	}

	return pdbList, nil
}
