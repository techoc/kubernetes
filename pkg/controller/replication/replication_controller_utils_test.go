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

package replication

import (
	"reflect"
	"testing"

	"k8s.io/api/core/v1"
)

var (
	// imagePullBackOff 是自定义的条件下标类型，用于验证不在预期集合中的条件。
	imagePullBackOff v1.ReplicationControllerConditionType = "ImagePullBackOff"

	// 以下工厂函数每次返回新的条件值，避免多个用例共享可变对象而相互污染。
	condImagePullBackOff = func() v1.ReplicationControllerCondition {
		return v1.ReplicationControllerCondition{
			Type:   imagePullBackOff,
			Status: v1.ConditionTrue,
			Reason: "NonExistentImage",
		}
	}

	// condReplicaFailure 构造一个副本失败条件，Reason 为 OtherFailure。
	condReplicaFailure = func() v1.ReplicationControllerCondition {
		return v1.ReplicationControllerCondition{
			Type:   v1.ReplicationControllerReplicaFailure,
			Status: v1.ConditionTrue,
			Reason: "OtherFailure",
		}
	}

	// condReplicaFailure2 与 condReplicaFailure 类型相同但 Reason 不同，用于覆盖“覆盖已有条件”场景。
	condReplicaFailure2 = func() v1.ReplicationControllerCondition {
		return v1.ReplicationControllerCondition{
			Type:   v1.ReplicationControllerReplicaFailure,
			Status: v1.ConditionTrue,
			Reason: "AnotherFailure",
		}
	}

	// status 返回一个仅包含一个副本失败条件的状态对象。
	status = func() *v1.ReplicationControllerStatus {
		return &v1.ReplicationControllerStatus{
			Conditions: []v1.ReplicationControllerCondition{condReplicaFailure()},
		}
	}
)

// TestGetCondition 验证 GetCondition 仅按条件类型查找，不关心 Reason 等字段。
func TestGetCondition(t *testing.T) {
	exampleStatus := status()

	tests := []struct {
		name string

		status     v1.ReplicationControllerStatus
		condType   v1.ReplicationControllerConditionType
		condStatus v1.ConditionStatus
		condReason string

		expected bool
	}{
		{
			name: "condition exists",

			status:   *exampleStatus,
			condType: v1.ReplicationControllerReplicaFailure,

			expected: true,
		},
		{
			name: "condition does not exist",

			status:   *exampleStatus,
			condType: imagePullBackOff,

			expected: false,
		},
	}

	for _, test := range tests {
		// 用返回值是否为 nil 判断条件是否存在；状态中未含该类型时应返回 nil。
		cond := GetCondition(test.status, test.condType)
		exists := cond != nil
		if exists != test.expected {
			t.Errorf("%s: expected condition to exist: %t, got: %t", test.name, test.expected, exists)
		}
	}
}

// TestSetCondition 验证 SetCondition 的三种语义：首次添加、追加不同类型、覆盖同类型的已有条件。
func TestSetCondition(t *testing.T) {
	tests := []struct {
		name string

		status *v1.ReplicationControllerStatus
		cond   v1.ReplicationControllerCondition

		expectedStatus *v1.ReplicationControllerStatus
	}{
		{
			name: "set for the first time",

			status: &v1.ReplicationControllerStatus{},
			cond:   condReplicaFailure(),

			expectedStatus: &v1.ReplicationControllerStatus{Conditions: []v1.ReplicationControllerCondition{condReplicaFailure()}},
		},
		{
			name: "simple set",

			status: &v1.ReplicationControllerStatus{Conditions: []v1.ReplicationControllerCondition{condImagePullBackOff()}},
			cond:   condReplicaFailure(),

			expectedStatus: &v1.ReplicationControllerStatus{Conditions: []v1.ReplicationControllerCondition{condImagePullBackOff(), condReplicaFailure()}},
		},
		{
			name: "overwrite",

			status: &v1.ReplicationControllerStatus{Conditions: []v1.ReplicationControllerCondition{condReplicaFailure()}},
			cond:   condReplicaFailure2(),

			expectedStatus: &v1.ReplicationControllerStatus{Conditions: []v1.ReplicationControllerCondition{condReplicaFailure2()}},
		},
	}

	for _, test := range tests {
		SetCondition(test.status, test.cond)
		// 直接比较整个状态，确保条件顺序与内容都符合预期，不只是目标条件被写入。
		if !reflect.DeepEqual(test.status, test.expectedStatus) {
			t.Errorf("%s: expected status: %v, got: %v", test.name, test.expectedStatus, test.status)
		}
	}
}

// TestRemoveCondition 验证 RemoveCondition 仅删除指定类型的条件，不存在的类型应保持状态不变。
func TestRemoveCondition(t *testing.T) {
	tests := []struct {
		name string

		status   *v1.ReplicationControllerStatus
		condType v1.ReplicationControllerConditionType

		expectedStatus *v1.ReplicationControllerStatus
	}{
		{
			name: "remove from empty status",

			status:   &v1.ReplicationControllerStatus{},
			condType: v1.ReplicationControllerReplicaFailure,

			expectedStatus: &v1.ReplicationControllerStatus{},
		},
		{
			name: "simple remove",

			status:   &v1.ReplicationControllerStatus{Conditions: []v1.ReplicationControllerCondition{condReplicaFailure()}},
			condType: v1.ReplicationControllerReplicaFailure,

			expectedStatus: &v1.ReplicationControllerStatus{},
		},
		{
			name: "doesn't remove anything",

			status:   status(),
			condType: imagePullBackOff,

			expectedStatus: status(),
		},
	}

	for _, test := range tests {
		RemoveCondition(test.status, test.condType)
		// 比较完整状态，确认删除操作的副作用范围：只移除目标类型，不影响其它条件。
		if !reflect.DeepEqual(test.status, test.expectedStatus) {
			t.Errorf("%s: expected status: %v, got: %v", test.name, test.expectedStatus, test.status)
		}
	}
}
