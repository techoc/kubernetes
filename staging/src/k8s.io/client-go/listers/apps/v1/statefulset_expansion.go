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

package v1

import (
	"fmt"

	apps "k8s.io/api/apps/v1"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// StatefulSetListerExpansion 允许向 StatefulSetLister 添加自定义方法。
// 扩展接口由生成的 StatefulSetLister 嵌入，避免手写方法被 lister-gen 覆盖。
type StatefulSetListerExpansion interface {
	GetPodStatefulSets(pod *v1.Pod) ([]*apps.StatefulSet, error)
}

// StatefulSetNamespaceListerExpansion 允许向 StatefulSetNamespaceLister 添加自定义方法。
// 当前没有命名空间级扩展方法，保留空接口以支持后续增量添加而不破坏 API。
type StatefulSetNamespaceListerExpansion interface{}

// GetPodStatefulSets 返回可能匹配某个 Pod 的 StatefulSet 列表；只有 Pod 的
// ControllerRef 指定的那个才会真正管理它。找不到匹配项时返回错误。
//
// 只做标签选择器的粗筛，调用方需再检查 ControllerRef 才能确定真正管理该 Pod 的对象。
func (s *statefulSetLister) GetPodStatefulSets(pod *v1.Pod) ([]*apps.StatefulSet, error) {
	var selector labels.Selector
	var ps *apps.StatefulSet

	// 没有标签的 Pod 不可能匹配任何选择器，直接返回错误而不是空列表，
	// 便于调用方把"无候选"与"查询失败"区分开。
	if len(pod.Labels) == 0 {
		return nil, fmt.Errorf("no StatefulSets found for pod %v because it has no labels", pod.Name)
	}

	// 先按命名空间缩小范围；StatefulSet 只能管理同命名空间的 Pod。
	list, err := s.StatefulSets(pod.Namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	var psList []*apps.StatefulSet
	for i := range list {
		ps = list[i]
		// 缓存中可能混入其他命名空间的对象，这里再校验一次。
		if ps.Namespace != pod.Namespace {
			continue
		}
		selector, err = metav1.LabelSelectorAsSelector(ps.Spec.Selector)
		if err != nil {
			// 该对象的 selector 非法，视为不匹配该 pod，避免单个坏对象让整个查询失败。
			continue
		}

		// 若混入 nil 或空选择器的 StatefulSet，它应匹配零个对象而非全部：
		// labels.Everything() 的语义与空选择器不同，必须显式排除，否则会误匹配全部 Pod。
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
