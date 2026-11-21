/*
Copyright 2015 The Kubernetes Authors.

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

// 如果修改本文件，也应同步修改 ReplicaSet 中对应的代码。
// 两者共享 condition 处理语义，漂移会导致两个控制器行为不一致。

package replication

import (
	"k8s.io/api/core/v1"
)

// GetCondition 返回副本控制器（ReplicationController）状态中指定类型的 Condition，不存在则返回 nil。
func GetCondition(status v1.ReplicationControllerStatus, condType v1.ReplicationControllerConditionType) *v1.ReplicationControllerCondition {
	for i := range status.Conditions {
		c := status.Conditions[i]
		if c.Type == condType {
			return &c
		}
	}
	return nil
}

// SetCondition 在副本控制器状态中添加或替换指定的 Condition。
// 当状态和原因都未变化时会提前返回，避免无意义的更新造成额外的 API 写入与事件。
func SetCondition(status *v1.ReplicationControllerStatus, condition v1.ReplicationControllerCondition) {
	currentCond := GetCondition(*status, condition.Type)
	// 仅 Status 和 Reason 相同才跳过；消息或时间戳变化仍需要反映到状态中。
	if currentCond != nil && currentCond.Status == condition.Status && currentCond.Reason == condition.Reason {
		return
	}
	newConditions := filterOutCondition(status.Conditions, condition.Type)
	status.Conditions = append(newConditions, condition)
}

// RemoveCondition 从副本控制器状态中移除指定类型的 Condition。
func RemoveCondition(status *v1.ReplicationControllerStatus, condType v1.ReplicationControllerConditionType) {
	status.Conditions = filterOutCondition(status.Conditions, condType)
}

// filterOutCondition 返回一个新的 Condition 切片，其中排除了指定类型的 Condition。
// 返回新切片而非原地修改，避免与状态对象的其他引用共享底层数组。
func filterOutCondition(conditions []v1.ReplicationControllerCondition, condType v1.ReplicationControllerConditionType) []v1.ReplicationControllerCondition {
	var newConditions []v1.ReplicationControllerCondition
	for _, c := range conditions {
		// 只剔除目标类型，保留其他 Condition 及其顺序。
		if c.Type == condType {
			continue
		}
		newConditions = append(newConditions, c)
	}
	return newConditions
}
