/*
Copyright The Kubernetes Authors.

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

package helper

import (
	"fmt"
	"iter"

	v1 "k8s.io/api/core/v1"
	schedulingapi "k8s.io/api/scheduling/v1alpha3"
	fwk "k8s.io/kube-scheduler/framework"
)

// MatchingSchedulingGroup 判断两个 Pod 是否属于同一个调度组。
// 要求命名空间相同，且双方都设置了 SchedulingGroup 并且组名一致。
func MatchingSchedulingGroup(pod1, pod2 *v1.Pod) bool {
	return pod1.Namespace == pod2.Namespace &&
		pod1.Spec.SchedulingGroup != nil &&
		pod2.Spec.SchedulingGroup != nil &&
		*pod1.Spec.SchedulingGroup.PodGroupName == *pod2.Spec.SchedulingGroup.PodGroupName
}

// GetPodGroupStates 返回一个迭代器序列，遍历给定 rootEntityKey 下所有叶子 PodGroupState。
// 若 rootEntityKey 是 CompositePodGroupKeyType，则递归遍历其子实体；
// 若是 PodGroupKeyType，则直接产出对应的 PodGroupState。
// 遍历深度受 WorkloadMaxTreeDepth 限制，防止组合组形成环时无限递归。
func GetPodGroupStates(sharedLister fwk.SharedLister, rootEntityKey fwk.EntityKey) iter.Seq2[fwk.PodGroupState, error] {
	return func(yield func(fwk.PodGroupState, error) bool) {
		var walk func(entityKey fwk.EntityKey, depth int) bool
		walk = func(entityKey fwk.EntityKey, depth int) bool {
			if depth >= schedulingapi.WorkloadMaxTreeDepth {
				return yield(nil, fmt.Errorf("exceeded maximum hierarchy depth at %v", entityKey))
			}
			if entityKey.GetType() == fwk.CompositePodGroupKeyType {
				cpgState, err := sharedLister.CompositePodGroupStates().Get(entityKey.GetNamespace(), entityKey.GetName())
				if err != nil {
					return yield(nil, err)
				}
				for _, childEntityKey := range cpgState.GetChildren() {
					if !walk(childEntityKey, depth+1) {
						return false
					}
				}
				return true
			}
			pgState, err := sharedLister.PodGroupStates().Get(entityKey.GetNamespace(), entityKey.GetName())
			if err != nil {
				return yield(nil, err)
			}
			return yield(pgState, nil)
		}
		walk(rootEntityKey, 0)
	}
}
