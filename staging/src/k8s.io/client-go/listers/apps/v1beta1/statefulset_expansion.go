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

// Package v1beta1 提供 apps/v1beta1 资源基于本地缓存索引的只读访问。
//
// 该包只读：所有 Lister 都直接查询 informer 的 Indexer，不访问 API Server。
// 返回的对象是共享缓存中的指针，调用方只应读取，不得修改。
package v1beta1

import (
	"fmt"

	apps "k8s.io/api/apps/v1beta1"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// StatefulSetListerExpansion 为 StatefulSetLister 提供扩展点。
//
// 通过独立的 Expansion 接口扩展生成的 Lister，避免手工修改生成代码。
type StatefulSetListerExpansion interface {
	GetPodStatefulSets(pod *v1.Pod) ([]*apps.StatefulSet, error)
}

// StatefulSetNamespaceListerExpansion 为 StatefulSetNamespaceLister 提供扩展点。
type StatefulSetNamespaceListerExpansion interface{}

// GetPodStatefulSets 返回可能与 Pod 匹配的 StatefulSet 列表；真正管理该 Pod 的只有
// ControllerRef 指向的那一个。找不到任何候选时才返回错误。
//
// 潜在匹配意味着同一 Pod 可能被多个 selector 命中，调用方仍需检查
// ControllerRef 才能确定真正的属主。
func (s *statefulSetLister) GetPodStatefulSets(pod *v1.Pod) ([]*apps.StatefulSet, error) {
	var selector labels.Selector
	var ps *apps.StatefulSet

	// 没有标签的 Pod 不可能匹配任何 selector，提前返回避免无意义遍历。
	if len(pod.Labels) == 0 {
		return nil, fmt.Errorf("no StatefulSets found for pod %v because it has no labels", pod.Name)
	}

	list, err := s.StatefulSets(pod.Namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	var psList []*apps.StatefulSet
	for i := range list {
		ps = list[i]
		if ps.Namespace != pod.Namespace {
			continue
		}
		selector, err = metav1.LabelSelectorAsSelector(ps.Spec.Selector)
		if err != nil {
			// selector 无效即视为与 Pod 不匹配。
			continue
		}

		// 若混入 selector 为 nil 或空的 StatefulSet，应视为不匹配任何对象，而不是匹配全部。
		//
		// 空 selector 在 labels.Selector 语义下会匹配所有标签，这里必须显式排除，
		// 防止错误配置的 StatefulSet 抢占所有 Pod。
		if selector.Empty() || !selector.Matches(labels.Set(pod.Labels)) {
			continue
		}
		psList = append(psList, ps)
	}

	if len(psList) == 0 {
		return nil, fmt.Errorf("could not find StatefulSet for pod %s in namespace %s with labels: %v", pod.Name, pod.Namespace, pod.Labels)
	}

	return psList, nil
}
