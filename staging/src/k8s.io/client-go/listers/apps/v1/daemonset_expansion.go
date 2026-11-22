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

// DaemonSetListerExpansion allows custom methods to be added to
// DaemonSetLister.
//
// 中文说明：这是手写的扩展接口，属于生成 lister 的一部分；生成器不会覆盖。
type DaemonSetListerExpansion interface {
	// GetPodDaemonSets 返回 selector 可能匹配该 Pod 的 DaemonSet。
	GetPodDaemonSets(pod *v1.Pod) ([]*apps.DaemonSet, error)
	// GetHistoryDaemonSets 返回 selector 可能匹配该 ControllerRevision 的 DaemonSet。
	GetHistoryDaemonSets(history *apps.ControllerRevision) ([]*apps.DaemonSet, error)
}

// DaemonSetNamespaceListerExpansion allows custom methods to be added to
// DaemonSetNamespaceLister.
//
// 中文说明：当前无命名空间级扩展方法，仅作为预留钩子。
type DaemonSetNamespaceListerExpansion interface{}

// GetPodDaemonSets returns a list of DaemonSets that potentially match a pod.
// Only the one specified in the Pod's ControllerRef will actually manage it.
// Returns an error only if no matching DaemonSets are found.
//
// 中文说明：这里返回的是“可能匹配”的候选集合，调用方仍需根据 ControllerRef 判定真正的
// owner。返回 error 只用于“找不到任何候选”这一情形，便于调用方区分。
func (s *daemonSetLister) GetPodDaemonSets(pod *v1.Pod) ([]*apps.DaemonSet, error) {
	var selector labels.Selector
	var daemonSet *apps.DaemonSet

	// 中文：没有标签的 Pod 不可能被基于 selector 的 DaemonSet 选中，提前返回。
	if len(pod.Labels) == 0 {
		return nil, fmt.Errorf("no daemon sets found for pod %v because it has no labels", pod.Name)
	}

	// 中文：先在 Pod 所在命名空间内列举全部 DaemonSet，再用 label selector 过滤。
	list, err := s.DaemonSets(pod.Namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	var daemonSets []*apps.DaemonSet
	for i := range list {
		daemonSet = list[i]
		// 中文：命名空间 lister 理论上已过滤，这里再次校验以防御缓存异常或跨命名空间复用。
		if daemonSet.Namespace != pod.Namespace {
			continue
		}
		selector, err = metav1.LabelSelectorAsSelector(daemonSet.Spec.Selector)
		if err != nil {
			// This object has an invalid selector, it does not match the pod
			// 中文：selector 非法时无法判定，保守地视为不匹配。
			continue
		}

		// If a daemonSet with a nil or empty selector creeps in, it should match nothing, not everything.
		// 中文：空 selector 按“匹配空集”处理，绝不能退化为“匹配全部”。
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
// 中文说明：用于从 ControllerRevision 反查其所属 DaemonSet（例如解析当前 revision 的
// 模板差异）。同样只是候选集合，真正的 owner 由 ControllerRef 决定。
func (s *daemonSetLister) GetHistoryDaemonSets(history *apps.ControllerRevision) ([]*apps.DaemonSet, error) {
	// 中文：ControllerRevision 通常带有 DaemonSet 模板的标签；无标签则无法匹配。
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
			// This object has an invalid selector, it does not match the history
			// 中文：非法 selector 保守视为不匹配。
			continue
		}
		// If a DaemonSet with a nil or empty selector creeps in, it should match nothing, not everything.
		// 中文：空 selector 不得匹配一切。
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
