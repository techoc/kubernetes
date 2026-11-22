/*
Copyright 2020 The Kubernetes Authors.

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

package meta

import (
	"reflect"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestSetStatusCondition 覆盖 SetStatusCondition 的插入/更新语义：
// 新类型追加到末尾；已存在的类型原位替换，并按状态是否变化决定是否保留
// 原有的 LastTransitionTime。
func TestSetStatusCondition(t *testing.T) {
	oneHourBefore := time.Now().Add(-1 * time.Hour)
	oneHourAfter := time.Now().Add(1 * time.Hour)

	tests := []struct {
		name          string
		conditions    []metav1.Condition
		toAdd         metav1.Condition
		expectChanged bool
		expected      []metav1.Condition
	}{
		{
			// 新条件应追加到列表末尾，并返回 changed=true。
			name: "should-add",
			conditions: []metav1.Condition{
				{Type: "first"},
				{Type: "third"},
			},
			toAdd:         metav1.Condition{Type: "second", Status: metav1.ConditionTrue, LastTransitionTime: metav1.Time{Time: oneHourBefore}, Reason: "reason", Message: "message"},
			expectChanged: true,
			expected: []metav1.Condition{
				{Type: "first"},
				{Type: "third"},
				{Type: "second", Status: metav1.ConditionTrue, LastTransitionTime: metav1.Time{Time: oneHourBefore}, Reason: "reason", Message: "message"},
			},
		},
		{
			// 状态发生变化时，采用调用方提供的 LastTransitionTime。
			name: "use-supplied-time",
			conditions: []metav1.Condition{
				{Type: "first"},
				{Type: "second", Status: metav1.ConditionFalse},
				{Type: "third"},
			},
			toAdd:         metav1.Condition{Type: "second", Status: metav1.ConditionTrue, LastTransitionTime: metav1.Time{Time: oneHourBefore}, Reason: "reason", Message: "message"},
			expectChanged: true,
			expected: []metav1.Condition{
				{Type: "first"},
				{Type: "second", Status: metav1.ConditionTrue, LastTransitionTime: metav1.Time{Time: oneHourBefore}, Reason: "reason", Message: "message"},
				{Type: "third"},
			},
		},
		{
			// 状态未变时，LastTransitionTime 保留旧值，其余字段（Reason/Message/
			// ObservedGeneration）更新为新值。
			name: "update-fields",
			conditions: []metav1.Condition{
				{Type: "first"},
				{Type: "second", Status: metav1.ConditionTrue, LastTransitionTime: metav1.Time{Time: oneHourBefore}},
				{Type: "third"},
			},
			toAdd:         metav1.Condition{Type: "second", Status: metav1.ConditionTrue, LastTransitionTime: metav1.Time{Time: oneHourAfter}, Reason: "reason", Message: "message", ObservedGeneration: 3},
			expectChanged: true,
			expected: []metav1.Condition{
				{Type: "first"},
				{Type: "second", Status: metav1.ConditionTrue, LastTransitionTime: metav1.Time{Time: oneHourBefore}, Reason: "reason", Message: "message", ObservedGeneration: 3},
				{Type: "third"},
			},
		},
		{
			// 所有字段都相同则视为无变化，changed=false。
			name: "nothing changes",
			conditions: []metav1.Condition{{
				Type:               "type",
				Status:             metav1.ConditionTrue,
				LastTransitionTime: metav1.Time{Time: oneHourBefore},
			}},
			toAdd: metav1.Condition{Type: "type", Status: metav1.ConditionTrue, LastTransitionTime: metav1.Time{Time: oneHourBefore}},
			expected: []metav1.Condition{{
				Type:               "type",
				Status:             metav1.ConditionTrue,
				LastTransitionTime: metav1.Time{Time: oneHourBefore},
			}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := SetStatusCondition(&test.conditions, test.toAdd)
			if test.expectChanged != changed {
				t.Errorf("expectChanged=%t != changed=%t", test.expectChanged, changed)
			}
			if !reflect.DeepEqual(test.conditions, test.expected) {
				t.Error(test.conditions)
			}
		})
	}
}

// TestRemoveStatusCondition 覆盖 RemoveStatusCondition：存在则删除并返回 true，
// 不存在或列表为空时保持原样并返回 false。
func TestRemoveStatusCondition(t *testing.T) {
	tests := []struct {
		name          string
		conditions    []metav1.Condition
		conditionType string
		expectRemoval bool
		expected      []metav1.Condition
	}{
		{
			name: "present",
			conditions: []metav1.Condition{
				{Type: "first"},
				{Type: "second"},
				{Type: "third"},
			},
			conditionType: "second",
			expectRemoval: true,
			expected: []metav1.Condition{
				{Type: "first"},
				{Type: "third"},
			},
		},
		{
			name: "not-present",
			conditions: []metav1.Condition{
				{Type: "first"},
				{Type: "second"},
				{Type: "third"},
			},
			conditionType: "fourth",
			expected: []metav1.Condition{
				{Type: "first"},
				{Type: "second"},
				{Type: "third"},
			},
		},
		{
			name:          "empty_conditions",
			conditions:    []metav1.Condition{},
			conditionType: "Foo",
			expected:      []metav1.Condition{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			removed := RemoveStatusCondition(&test.conditions, test.conditionType)
			if test.expectRemoval != removed {
				t.Errorf("expectRemoval=%t != removal=%t", test.expectRemoval, removed)
			}
			if !reflect.DeepEqual(test.conditions, test.expected) {
				t.Error(test.conditions)
			}
		})
	}
}

// TestFindStatusCondition 覆盖 FindStatusCondition 的查找结果：
// 不存在返回 nil，存在返回指向该条件的指针。
func TestFindStatusCondition(t *testing.T) {
	tests := []struct {
		name          string
		conditions    []metav1.Condition
		conditionType string
		expected      *metav1.Condition
	}{
		{
			name: "not-present",
			conditions: []metav1.Condition{
				{Type: "first"},
			},
			conditionType: "second",
			expected:      nil,
		},
		{
			name: "present",
			conditions: []metav1.Condition{
				{Type: "first"},
				{Type: "second"},
			},
			conditionType: "second",
			expected:      &metav1.Condition{Type: "second"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := FindStatusCondition(test.conditions, test.conditionType)
			if !reflect.DeepEqual(actual, test.expected) {
				t.Error(actual)
			}
		})
	}
}

// TestIsStatusConditionPresentAndEqual 覆盖“条件存在且状态相等”的组合判定。
func TestIsStatusConditionPresentAndEqual(t *testing.T) {
	tests := []struct {
		name            string
		conditions      []metav1.Condition
		conditionType   string
		conditionStatus metav1.ConditionStatus
		expected        bool
	}{
		{
			name: "doesnt-match-true",
			conditions: []metav1.Condition{
				{Type: "first", Status: metav1.ConditionUnknown},
			},
			conditionType:   "first",
			conditionStatus: metav1.ConditionTrue,
			expected:        false,
		},
		{
			name: "does-match-true",
			conditions: []metav1.Condition{
				{Type: "first", Status: metav1.ConditionTrue},
			},
			conditionType:   "first",
			conditionStatus: metav1.ConditionTrue,
			expected:        true,
		},
		{
			name: "does-match-false",
			conditions: []metav1.Condition{
				{Type: "first", Status: metav1.ConditionFalse},
			},
			conditionType:   "first",
			conditionStatus: metav1.ConditionFalse,
			expected:        true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := IsStatusConditionPresentAndEqual(test.conditions, test.conditionType, test.conditionStatus)
			if actual != test.expected {
				t.Error(actual)
			}

		})
	}
}
