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
	extensions "k8s.io/api/extensions/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// ReplicaSetListerExpansion 允许向 ReplicaSetLister 添加自定义方法。
//
// 生成器无法预知这种按 Pod 反查归属者集合的定制方法，故通过扩展接口注入。
type ReplicaSetListerExpansion interface {
	GetPodReplicaSets(pod *v1.Pod) ([]*extensions.ReplicaSet, error)
}

// ReplicaSetNamespaceListerExpansion 允许向 ReplicaSetNamespaceLister 添加自定义方法。
//
// 当前没有命名空间级别的定制方法，接口保留为空以维持生成代码的扩展点。
type ReplicaSetNamespaceListerExpansion interface{}

// GetPodReplicaSets 返回可能匹配某个 Pod 的 ReplicaSet 列表；只有 Pod 的
// ControllerRef 指定的那个才会真正管理它。找不到匹配项时返回错误。
func (s *replicaSetLister) GetPodReplicaSets(pod *v1.Pod) ([]*extensions.ReplicaSet, error) {
	// 没有标签的 Pod 不可能匹配任何 selector，提前返回可给出更明确的错误。
	if len(pod.Labels) == 0 {
		return nil, fmt.Errorf("no ReplicaSets found for pod %v because it has no labels", pod.Name)
	}

	// 先在命名空间内全量取出，再按 selector 过滤；注意这里不能直接用
	// selector 查询，因为需要返回所有“潜在”匹配者供调用方判断归属。
	list, err := s.ReplicaSets(pod.Namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	var rss []*extensions.ReplicaSet
	for _, rs := range list {
		if rs.Namespace != pod.Namespace {
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(rs.Spec.Selector)
		if err != nil {
			// 该对象的 selector 非法，视为不匹配该 pod；跳过它而不让整个查询失败。
			continue
		}

		// 若混入 nil 或空 selector 的 ReplicaSet，应视为不匹配任何对象，而非匹配全部。
		if selector.Empty() || !selector.Matches(labels.Set(pod.Labels)) {
			continue
		}
		rss = append(rss, rs)
	}

	if len(rss) == 0 {
		return nil, fmt.Errorf("could not find ReplicaSet for pod %s in namespace %s with labels: %v", pod.Name, pod.Namespace, pod.Labels)
	}

	return rss, nil
}
