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

package v1beta2

import (
	"fmt"

	apps "k8s.io/api/apps/v1beta2"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// StatefulSetListerExpansion 允许向 StatefulSetLister 添加自定义方法。
//
// 该接口由 lister-gen 生成的 StatefulSetLister 嵌入，用来挂接手写的扩展方法。
type StatefulSetListerExpansion interface {
	GetPodStatefulSets(pod *v1.Pod) ([]*apps.StatefulSet, error)
}

// StatefulSetNamespaceListerExpansion 允许向 StatefulSetNamespaceLister 添加自定义方法。
//
// 有意保持为空：命名空间级 lister 目前没有额外的扩展方法，但保留该接口
// 可以让未来添加方法时不破坏既有嵌入关系。
type StatefulSetNamespaceListerExpansion interface{}

// GetPodStatefulSets 返回可能匹配某个 Pod 的 StatefulSet 列表；只有 Pod 的
// ControllerRef 指定的那个才会真正管理它。找不到匹配项时返回错误。
//
// 返回的是“候选”集合：调用方仍需通过 pod 的 ControllerRef 判定真正管理它的那个。
// 无标签的 Pod 不可能被选择器命中，故直接报错，避免做一次无意义的全量 List。
func (s *statefulSetLister) GetPodStatefulSets(pod *v1.Pod) ([]*apps.StatefulSet, error) {
	var selector labels.Selector
	var ps *apps.StatefulSet

	if len(pod.Labels) == 0 {
		return nil, fmt.Errorf("no StatefulSets found for pod %v because it has no labels", pod.Name)
	}

	// lister 的缓存按命名空间索引，因此只需扫描 Pod 所在命名空间。
	list, err := s.StatefulSets(pod.Namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	var psList []*apps.StatefulSet
	for i := range list {
		ps = list[i]
		// StatefulSet 不能跨命名空间选择 Pod，这里再校验一次以防索引异常。
		if ps.Namespace != pod.Namespace {
			continue
		}
		selector, err = metav1.LabelSelectorAsSelector(ps.Spec.Selector)
		if err != nil {
			// 选择器无法转换说明对象已损坏；跳过它，而不是让整个查询失败。
			// 该对象的 selector 非法，视为不匹配该 pod；跳过它而不让整个查询失败。
			continue
		}

		// 若混入 nil 或空 selector 的 StatefulSet，应视为不匹配任何对象，而非匹配全部。
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
