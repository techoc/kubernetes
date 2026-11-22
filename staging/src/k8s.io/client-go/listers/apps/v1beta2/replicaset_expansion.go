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

// ReplicaSetListerExpansion allows custom methods to be added to
// ReplicaSetLister.
//
// 为生成的 ReplicaSetLister 补充反查能力，使调用方免于遍历整个 namespace。
type ReplicaSetListerExpansion interface {
	GetPodReplicaSets(pod *v1.Pod) ([]*apps.ReplicaSet, error)
}

// ReplicaSetNamespaceListerExpansion allows custom methods to be added to
// ReplicaSetNamespaceLister.
//
// 目前无需为 namespace lister 添加方法，保留空接口占位以维持生成代码的
// 组合结构稳定。
type ReplicaSetNamespaceListerExpansion interface{}

// GetPodReplicaSets returns a list of ReplicaSets that potentially match a pod.
// Only the one specified in the Pod's ControllerRef will actually manage it.
// Returns an error only if no matching ReplicaSets are found.
//
// 该方法是基于缓存的启发式匹配，可能返回多个候选（同名标签重叠时），
// 调用方必须再用 ControllerRef 判定真正拥有 Pod 的 ReplicaSet。
func (s *replicaSetLister) GetPodReplicaSets(pod *v1.Pod) ([]*apps.ReplicaSet, error) {
	// 没有标签的 Pod 不可能被任何带 selector 的 ReplicaSet 选中，
	// 直接返回错误，省去一次全量扫描。
	if len(pod.Labels) == 0 {
		return nil, fmt.Errorf("no ReplicaSets found for pod %v because it has no labels", pod.Name)
	}

	// 只查 Pod 所在 namespace，缩小扫描范围；selector 过滤在循环里做。
	list, err := s.ReplicaSets(pod.Namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	var rss []*apps.ReplicaSet
	for _, rs := range list {
		// namespace lister 理论上只返回该 namespace 的对象，这里再做一次防御性检查。
		if rs.Namespace != pod.Namespace {
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(rs.Spec.Selector)
		if err != nil {
			// This object has an invalid selector, it does not match the pod
			continue
		}

		// If a ReplicaSet with a nil or empty selector creeps in, it should match nothing, not everything.
		if selector.Empty() || !selector.Matches(labels.Set(pod.Labels)) {
			continue
		}
		rss = append(rss, rs)
	}

	if len(rss) == 0 {
		// 没有任何候选时返回错误而非空列表，便于调用方与“存在但不匹配”区分。
		return nil, fmt.Errorf("could not find ReplicaSet for pod %s in namespace %s with labels: %v", pod.Name, pod.Namespace, pod.Labels)
	}

	return rss, nil
}
