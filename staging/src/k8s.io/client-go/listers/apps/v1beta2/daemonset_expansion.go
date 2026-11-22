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

// Package v1beta2 提供 apps/v1beta2 资源基于本地缓存索引的只读访问。
//
// 该包只读：所有 Lister 都直接查询 informer 的 Indexer，不访问 API Server。
// 返回的对象是共享缓存中的指针，调用方只应读取，不得修改。
package v1beta2

import (
	"fmt"

	apps "k8s.io/api/apps/v1beta2"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// DaemonSetListerExpansion allows custom methods to be added to
// DaemonSetLister.
//
// 通过独立的 Expansion 接口扩展生成的 Lister，避免手工修改生成代码。
type DaemonSetListerExpansion interface {
	GetPodDaemonSets(pod *v1.Pod) ([]*apps.DaemonSet, error)
	GetHistoryDaemonSets(history *apps.ControllerRevision) ([]*apps.DaemonSet, error)
}

// DaemonSetNamespaceListerExpansion allows custom methods to be added to
// DaemonSetNamespaceLister.
type DaemonSetNamespaceListerExpansion interface{}

// GetPodDaemonSets returns a list of DaemonSets that potentially match a pod.
// Only the one specified in the Pod's ControllerRef will actually manage it.
// Returns an error only if no matching DaemonSets are found.
//
// 潜在匹配意味着同一 Pod 可能被多个 selector 命中，调用方仍需检查
// ControllerRef 才能确定真正的属主。
func (s *daemonSetLister) GetPodDaemonSets(pod *v1.Pod) ([]*apps.DaemonSet, error) {
	var selector labels.Selector
	var daemonSet *apps.DaemonSet

	// 没有标签的 Pod 不可能匹配任何 selector，提前返回避免无意义遍历。
	if len(pod.Labels) == 0 {
		return nil, fmt.Errorf("no daemon sets found for pod %v because it has no labels", pod.Name)
	}

	list, err := s.DaemonSets(pod.Namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	var daemonSets []*apps.DaemonSet
	for i := range list {
		daemonSet = list[i]
		if daemonSet.Namespace != pod.Namespace {
			continue
		}
		selector, err = metav1.LabelSelectorAsSelector(daemonSet.Spec.Selector)
		if err != nil {
			// This object has an invalid selector, it does not match the pod
			continue
		}

		// If a daemonSet with a nil or empty selector creeps in, it should match nothing, not everything.
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

// GetHistoryDaemonSets returns a list of DaemonSets that potentially
// match a ControllerRevision. Only the one specified in the ControllerRevision's ControllerRef
// will actually manage it.
// Returns an error only if no matching DaemonSets are found.
//
// 用于由 ControllerRevision 反查所属 DaemonSet，例如回滚时确定历史版本归谁所有。
func (s *daemonSetLister) GetHistoryDaemonSets(history *apps.ControllerRevision) ([]*apps.DaemonSet, error) {
	// 没有标签的 ControllerRevision 无法与 selector 比较。
	if len(history.Labels) == 0 {
		return nil, fmt.Errorf("no DaemonSet found for ControllerRevision %s because it has no labels", history.Name)
	}

	list, err := s.DaemonSets(history.Namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	var daemonSets []*apps.DaemonSet
	for _, ds := range list {
		selector, err := metav1.LabelSelectorAsSelector(ds.Spec.Selector)
		if err != nil {
			// This object has an invalid selector, it does not match the history object
			continue
		}
		// If a DaemonSet with a nil or empty selector creeps in, it should match nothing, not everything.
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
