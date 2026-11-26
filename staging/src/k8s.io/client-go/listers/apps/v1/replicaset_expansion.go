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

// Package v1 提供 apps/v1 ReplicaSet 等资源的只读 informer 缓存 lister。
package v1

import (
	"fmt"

	apps "k8s.io/api/apps/v1"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// ReplicaSetListerExpansion 为 ReplicaSetLister 提供扩展点，用于追加自定义方法。
//
// 通过扩展接口注入（而非直接修改生成代码），以便 lister-gen 重新生成时不丢失手写方法。
type ReplicaSetListerExpansion interface {
	GetPodReplicaSets(pod *v1.Pod) ([]*apps.ReplicaSet, error)
}

// ReplicaSetNamespaceListerExpansion 为 ReplicaSetNamespaceLister 提供扩展点。
type ReplicaSetNamespaceListerExpansion interface{}

// GetPodReplicaSets 返回可能与 Pod 匹配的 ReplicaSet 列表；真正管理该 Pod 的只有
// Pod 的 ControllerRef 指向的那一个。找不到任何候选时才返回错误。
//
// 中文：基于选择器在缓存中做粗匹配；ControllerRef 的精确认领由调用方判断。
func (s *replicaSetLister) GetPodReplicaSets(pod *v1.Pod) ([]*apps.ReplicaSet, error) {
	// 没有标签的 Pod 不可能被任何选择器匹配，直接报错以避免无意义的全量扫描。
	if len(pod.Labels) == 0 {
		return nil, fmt.Errorf("no ReplicaSets found for pod %v because it has no labels", pod.Name)
	}

	// 先按命名空间缩小范围；缓存 lister 不访问 API server。
	list, err := s.ReplicaSets(pod.Namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	var rss []*apps.ReplicaSet
	for _, rs := range list {
		if rs.Namespace != pod.Namespace {
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(rs.Spec.Selector)
		if err != nil {
			// selector 无效即视为与 Pod 不匹配。
			continue
		}

		// 若混入 nil 或空选择器的 ReplicaSet，它应匹配零个对象而非全部：
		// 空选择器按 Kubernetes 约定匹配零个对象，不能退化为匹配全部。
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
