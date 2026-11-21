/*
Copyright 2018 The Kubernetes Authors.

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

package types

import (
	v1 "k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/features"
)

// PodConditionsByKubelet 是 kubelet 负责维护的 Pod condition 列表。
var PodConditionsByKubelet = []v1.PodConditionType{
	v1.PodScheduled,
	v1.PodReady,
	v1.PodInitialized,
	v1.ContainersReady,
	v1.PodResizeInProgress,
	v1.PodResizePending,
}

// PodConditionByKubelet 判断该 Pod condition 类型是否由 kubelet 负责。
func PodConditionByKubelet(conditionType v1.PodConditionType) bool {
	for _, c := range PodConditionsByKubelet {
		if c == conditionType {
			return true
		}
	}
	// PodReadyToStartContainers 条件受特性门控控制，关闭时不算 kubelet 所有。
	if utilfeature.DefaultFeatureGate.Enabled(features.PodReadyToStartContainersCondition) {
		if conditionType == v1.PodReadyToStartContainers {
			return true
		}
	}
	// AllContainersRestarting 条件受 RestartAllContainersOnContainerExits 特性门控控制。
	if utilfeature.DefaultFeatureGate.Enabled(features.RestartAllContainersOnContainerExits) {
		if conditionType == v1.AllContainersRestarting {
			return true
		}
	}
	return false
}

// PodConditionSharedByKubelet 判断该 Pod condition 类型是否由 kubelet 与其他组件共同维护。
func PodConditionSharedByKubelet(conditionType v1.PodConditionType) bool {
	return conditionType == v1.DisruptionTarget
}
