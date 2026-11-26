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

// Package v1beta1 提供 extensions/v1beta1 资源基于本地缓存索引的只读访问。
//
// 该包只读：所有 Lister 都直接查询 informer 的 Indexer，不访问 API Server。
// extensions/v1beta1 是已废弃的旧 API 组，保留这些 Lister 仅为兼容存量控制器。
package v1beta1

import (
	"fmt"

	apps "k8s.io/api/apps/v1beta1"
	"k8s.io/api/core/v1"
	"k8s.io/api/extensions/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// DaemonSetListerExpansion 允许向 DaemonSetLister 添加自定义方法。
//
// 通过独立的 Expansion 接口扩展生成的 Lister，避免手工修改生成代码。
type DaemonSetListerExpansion interface {
	GetPodDaemonSets(pod *v1.Pod) ([]*v1beta1.DaemonSet, error)
	GetHistoryDaemonSets(history *apps.ControllerRevision) ([]*v1beta1.DaemonSet, error)
}

// DaemonSetNamespaceListerExpansion 允许向 DaemonSetNamespaceLister 添加自定义方法。
type DaemonSetNamespaceListerExpansion interface{}

// GetPodDaemonSets 返回可能匹配某个 Pod 的 DaemonSet 列表；只有 Pod 的
// ControllerRef 指定的那个才会真正管理它。找不到匹配项时返回错误。
//
// 潜在匹配意味着同一 Pod 可能被多个 selector 命中，调用方仍需检查
// ControllerRef 才能确定真正的属主。
func (s *daemonSetLister) GetPodDaemonSets(pod *v1.Pod) ([]*v1beta1.DaemonSet, error) {
	var selector labels.Selector
	var daemonSet *v1beta1.DaemonSet

	// 没有标签的 Pod 不可能匹配任何 selector，提前返回避免无意义遍历。
	if len(pod.Labels) == 0 {
		return nil, fmt.Errorf("no daemon sets found for pod %v because it has no labels", pod.Name)
	}

	list, err := s.DaemonSets(pod.Namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	var daemonSets []*v1beta1.DaemonSet
	for i := range list {
		daemonSet = list[i]
		if daemonSet.Namespace != pod.Namespace {
			continue
		}
		selector, err = metav1.LabelSelectorAsSelector(daemonSet.Spec.Selector)
		if err != nil {
			// 该对象的 selector 非法，视为不匹配该 pod；跳过它而不让整个查询失败。
			continue
		}

		// 若混入 nil 或空 selector 的 DaemonSet，应视为不匹配任何对象，而非匹配全部：
		//
		// 空 selector 在 labels.Selector 语义下会匹配所有标签，必须显式排除。
		if selector.Empty() || !selector.Matches(labels.Set(pod.Labels)) {
			continue
		}
		daemonSets = append(daemonSets, daemonSet)
	}

	if len(daemonSets) == 0 {
		return nil, fmt.Errorf("could not find daemon set for pod %s in namespace %s with labels: %v", pod.Name, pod.Namespace, pod.Labels)
	}

	return daemonSets, nil
}

// GetHistoryDaemonSets 返回可能匹配某个 ControllerRevision 的 DaemonSet 列表；
// 只有 ControllerRevision 的 ControllerRef 指定的那个才会真正管理它。
// 找不到匹配项时返回错误。
//
// 用于由 ControllerRevision 反查所属 DaemonSet，例如回滚时确定历史版本归谁所有。
func (s *daemonSetLister) GetHistoryDaemonSets(history *apps.ControllerRevision) ([]*v1beta1.DaemonSet, error) {
	// 没有标签的 ControllerRevision 无法与 selector 比较。
	if len(history.Labels) == 0 {
		return nil, fmt.Errorf("no DaemonSet found for ControllerRevision %s because it has no labels", history.Name)
	}

	list, err := s.DaemonSets(history.Namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	var daemonSets []*v1beta1.DaemonSet
	for _, ds := range list {
		selector, err := metav1.LabelSelectorAsSelector(ds.Spec.Selector)
		if err != nil {
			// 该对象的 selector 非法，视为不匹配该历史对象，跳过以免单个坏对象破坏结果。
			continue
		}
		// 若混入 nil 或空 selector 的 DaemonSet，应视为不匹配任何对象，而非匹配全部。
		if selector.Empty() || !selector.Matches(labels.Set(history.Labels)) {
			continue
		}
		daemonSets = append(daemonSets, ds)
	}

	if len(daemonSets) == 0 {
		return nil, fmt.Errorf("could not find DaemonSets for ControllerRevision %s in namespace %s with labels: %v", history.Name, history.Namespace, history.Labels)
	}

	return daemonSets, nil
}
