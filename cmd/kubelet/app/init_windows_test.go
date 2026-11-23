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

package app

import "testing"

// TestIsValidPriorityClass 覆盖 Windows 优先级名称映射，确认合法名称得到系统常量、非法名称返回 0。
func TestIsValidPriorityClass(t *testing.T) {
	testCases := []struct {
		description           string
		priorityClassName     string
		expectedPriorityValue uint32
	}{
		{
			description:           "Invalid Priority Class",
			priorityClassName:     "myPriorityClass",
			expectedPriorityValue: 0,
		},
		{
			description:           "Valid Priority Class",
			priorityClassName:     "IDLE_PRIORITY_CLASS",
			expectedPriorityValue: uint32(64),
		},
	}
	for _, test := range testCases {
		actualPriorityValue := getPriorityValue(test.priorityClassName)
		if test.expectedPriorityValue != actualPriorityValue {
			t.Fatalf("unexpected error for %s", test.description)
		}
	}
}
