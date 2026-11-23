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

package helper

import (
	"reflect"
	"testing"

	"k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
)

// 用短名字的哨兵条件类型，避免测试中反复写长常量，同时保证各用例互不干扰。
var (
	a v1.APIServiceConditionType = "A"
	b v1.APIServiceConditionType = "B"
)

// 表驱动覆盖“条件缺失/存在但为 False/存在且为 True”三种情况，
// 因为 IsAPIServiceConditionTrue 必须能区分“不存在”和“存在但非 True”。
func TestIsAPIServiceConditionTrue(t *testing.T) {
	conditionATrue := makeNewAPIServiceCondition(a, "a reason", "a message", v1.ConditionTrue)
	conditionAFalse := makeNewAPIServiceCondition(a, "a reason", "a message", v1.ConditionFalse)
	tests := []*struct {
		name          string
		apiService    *v1.APIService
		conditionType v1.APIServiceConditionType
		expected      bool
	}{
		{
			name:          "Should return false when condition of type is not present",
			apiService:    makeNewAPIService("v1", 100),
			conditionType: a,
			expected:      false,
		},
		{
			name:          "Should return false when condition of type is present but status is not ConditionTrue",
			apiService:    makeNewAPIService("v1", 100, conditionAFalse),
			conditionType: a,
			expected:      false,
		},
		{
			name:          "Should return false when condition of type is present but status is not ConditionTrue",
			apiService:    makeNewAPIService("v1", 100, conditionATrue),
			conditionType: a,
			expected:      true,
		},
	}

	for _, tc := range tests {
		if isConditionTrue := IsAPIServiceConditionTrue(tc.apiService, tc.conditionType); isConditionTrue != tc.expected {
			t.Errorf("expected condition of type %v to be %v, actually was %v",
				tc.conditionType, isConditionTrue, tc.expected)

		}
	}
}

// 验证同类型条件被覆盖、不同类型条件互不影响，
// 这依赖 GetAPIServiceConditionByType 按 Type 精确匹配的语义。
func TestSetAPIServiceCondition(t *testing.T) {
	conditionA1 := makeNewAPIServiceCondition(a, "a1 reason", "a1 message", v1.ConditionTrue)
	conditionA2 := makeNewAPIServiceCondition(a, "a2 reason", "a2 message", v1.ConditionTrue)
	tests := []*struct {
		name              string
		apiService        *v1.APIService
		conditionType     v1.APIServiceConditionType
		initialCondition  *v1.APIServiceCondition
		setCondition      v1.APIServiceCondition
		expectedCondition *v1.APIServiceCondition
	}{
		{
			name:              "Should set a new condition with type where previously there was no condition of that type",
			apiService:        makeNewAPIService("v1", 100),
			conditionType:     a,
			initialCondition:  nil,
			setCondition:      conditionA1,
			expectedCondition: &conditionA1,
		},
		{
			name:              "Should override a condition of type, when a condition of that type existed previously",
			apiService:        makeNewAPIService("v1", 100, conditionA1),
			conditionType:     a,
			initialCondition:  &conditionA1,
			setCondition:      conditionA2,
			expectedCondition: &conditionA2,
		},
	}

	for _, tc := range tests {
		startingCondition := GetAPIServiceConditionByType(tc.apiService, tc.conditionType)
		if !reflect.DeepEqual(startingCondition, tc.initialCondition) {
			t.Errorf("expected to find condition %s initially, actual was %s", tc.initialCondition, startingCondition)

		}
		SetAPIServiceCondition(tc.apiService, tc.setCondition)
		actual := GetAPIServiceConditionByType(tc.apiService, tc.setCondition.Type)
		if !reflect.DeepEqual(actual, tc.expectedCondition) {
			t.Errorf("expected %s, actual %s", tc.expectedCondition, actual)
		}
	}
}

// case5 直接照抄文档里的排序示例，确保实现与对外承诺的优先级规则一致；
// 其他 case 则分别覆盖纯数字比较、alpha/beta/GA 顺序和非 kube 风格版本号。
func TestSortedAPIServicesByVersion(t *testing.T) {
	tests := []*struct {
		name     string
		versions []string
		expected []string
	}{
		{
			name:     "case1",
			versions: []string{"v1", "v2"},
			expected: []string{"v2", "v1"},
		},
		{
			name:     "case2",
			versions: []string{"v2", "v10"},
			expected: []string{"v10", "v2"},
		},
		{
			name:     "case3",
			versions: []string{"v2", "v2beta1", "v10beta2", "v10beta1", "v10alpha1", "v1"},
			expected: []string{"v2", "v1", "v10beta2", "v10beta1", "v2beta1", "v10alpha1"},
		},
		{
			name:     "case4",
			versions: []string{"v1", "v2", "test", "foo10", "final", "foo2", "foo1"},
			expected: []string{"v2", "v1", "final", "foo1", "foo10", "foo2", "test"},
		},
		{
			name:     "case5_from_documentation",
			versions: []string{"v12alpha1", "v10", "v11beta2", "v10beta3", "v3beta1", "v2", "v11alpha2", "foo1", "v1", "foo10"},
			expected: []string{"v10", "v2", "v1", "v11beta2", "v10beta3", "v3beta1", "v12alpha1", "v11alpha2", "foo1", "foo10"},
		},
	}

	for _, tc := range tests {
		apiServices := []*v1.APIService{}
		for _, v := range tc.versions {
			apiServices = append(apiServices, makeNewAPIService(v, 100))
		}
		sortedServices := SortedByGroupAndVersion(apiServices)
		actual := []string{}
		for _, s := range sortedServices[0] {
			actual = append(actual, s.Spec.Version)
		}
		if !reflect.DeepEqual(tc.expected, actual) {
			t.Errorf("expected %s, actual %s", tc.expected, actual)
		}
	}
}

// 查找函数的契约是“找不到返回 nil”，这里同时验证命中与未命中，
// 防止实现把零值条件当成有效结果返回。
func TestGetAPIServiceConditionByType(t *testing.T) {
	conditionA := makeNewAPIServiceCondition(a, "a reason", "a message", v1.ConditionTrue)
	conditionB := makeNewAPIServiceCondition(b, "b reason", "b message", v1.ConditionTrue)
	tests := []*struct {
		name              string
		apiService        *v1.APIService
		conditionType     v1.APIServiceConditionType
		expectedCondition *v1.APIServiceCondition
	}{
		{
			name:              "Should find a matching condition from apiService",
			apiService:        makeNewAPIService("v1", 100, conditionA, conditionB),
			conditionType:     a,
			expectedCondition: &conditionA,
		},
		{
			name:              "Should not find a matching condition",
			apiService:        makeNewAPIService("v1", 100, conditionA),
			conditionType:     b,
			expectedCondition: nil,
		},
	}

	for _, tc := range tests {
		actual := GetAPIServiceConditionByType(tc.apiService, tc.conditionType)
		if !reflect.DeepEqual(tc.expectedCondition, actual) {
			t.Errorf("expected %s, actual %s", tc.expectedCondition, actual)
		}
	}
}

// 测试辅助函数只填充与排序/条件相关的字段，其余保持零值，
// 让用例意图一眼可见，也减少无关字段变化带来的噪音。
func makeNewAPIService(version string, priority int32, conditions ...v1.APIServiceCondition) *v1.APIService {
	status := v1.APIServiceStatus{Conditions: conditions}
	return &v1.APIService{Spec: v1.APIServiceSpec{Version: version, VersionPriority: priority}, Status: status}
}

// 条件构造函数按参数逐项赋值，不做任何默认处理，
// 这样测试用例显式写出每个字段，断言失败时更容易定位是哪一项不符。
func makeNewAPIServiceCondition(conditionType v1.APIServiceConditionType, reason string, message string, status v1.ConditionStatus) v1.APIServiceCondition {
	return v1.APIServiceCondition{Type: conditionType, Reason: reason, Message: message, Status: status}
}
