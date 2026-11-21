/*
Copyright 2016 The Kubernetes Authors.

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
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/api/core/v1"
)

// TestConvertToTimestamp 验证时间字符串转换后保持不变。
func TestConvertToTimestamp(t *testing.T) {
	timestamp := "2017-02-17T15:34:49.830882016+08:00"
	convertedTimeStamp := ConvertToTimestamp(timestamp).GetString()
	assert.Equal(t, timestamp, convertedTimeStamp)
}

// TestLen 验证 SortedContainerStatuses 的 Len 返回元素个数。
func TestLen(t *testing.T) {
	// 用例覆盖单元素与多元素切片。
	var cases = []struct {
		statuses SortedContainerStatuses
		expected int
	}{
		{
			statuses: SortedContainerStatuses{{Name: "first"}},
			expected: 1,
		},
		{
			statuses: SortedContainerStatuses{{Name: "first"}, {Name: "second"}},
			expected: 2,
		},
	}
	// 逐用例比较长度。
	for _, data := range cases {
		assert.Equal(t, data.expected, data.statuses.Len())
	}
}

// TestSwap 验证 Swap 交换两个状态的位置。
func TestSwap(t *testing.T) {
	// 用例为两个状态，交换后顺序应反转。
	var cases = []struct {
		statuses SortedContainerStatuses
		expected SortedContainerStatuses
	}{
		{
			statuses: SortedContainerStatuses{{Name: "first"}, {Name: "second"}},
			expected: SortedContainerStatuses{{Name: "second"}, {Name: "first"}},
		},
	}
	for _, data := range cases {
		// 交换后应与期望顺序一致。
		data.statuses.Swap(0, 1)
		if !reflect.DeepEqual(data.statuses, data.expected) {
			t.Errorf(
				"failed Swap:\n\texpected: %v\n\t  actual: %v",
				data.expected,
				data.statuses,
			)
		}
	}
}

// TestLess 验证 Less 按名称排序。
func TestLess(t *testing.T) {
	var cases = []struct {
		statuses SortedContainerStatuses
		expected bool
	}{
		{
			statuses: SortedContainerStatuses{{Name: "first"}, {Name: "second"}},
			expected: true,
		},
		{
			statuses: SortedContainerStatuses{{Name: "second"}, {Name: "first"}},
			expected: false,
		},
	}
	for _, data := range cases {
		// 逐用例比较 Less 结果。
		actual := data.statuses.Less(0, 1)
		if actual != data.expected {
			t.Errorf(
				"failed Less:\n\texpected: %t\n\t  actual: %t",
				data.expected,
				actual,
			)
		}
	}
}

// TestSortInitContainerStatuses 验证 init 容器状态按容器定义顺序排序。
func TestSortInitContainerStatuses(t *testing.T) {
	pod := v1.Pod{
		Spec: v1.PodSpec{},
	}
	// 覆盖状态顺序错乱、缺失状态等场景。
	var cases = []struct {
		containers     []v1.Container
		statuses       []v1.ContainerStatus
		sortedStatuses []v1.ContainerStatus
	}{
		{
			containers:     []v1.Container{{Name: "first"}, {Name: "second"}, {Name: "third"}, {Name: "fourth"}},
			statuses:       []v1.ContainerStatus{{Name: "first"}, {Name: "second"}, {Name: "third"}, {Name: "fourth"}},
			sortedStatuses: []v1.ContainerStatus{{Name: "first"}, {Name: "second"}, {Name: "third"}, {Name: "fourth"}},
		},
		{
			containers:     []v1.Container{{Name: "first"}, {Name: "second"}, {Name: "third"}, {Name: "fourth"}},
			statuses:       []v1.ContainerStatus{{Name: "second"}, {Name: "first"}, {Name: "fourth"}, {Name: "third"}},
			sortedStatuses: []v1.ContainerStatus{{Name: "first"}, {Name: "second"}, {Name: "third"}, {Name: "fourth"}},
		},
		{
			containers:     []v1.Container{{Name: "first"}, {Name: "second"}, {Name: "third"}, {Name: "fourth"}},
			statuses:       []v1.ContainerStatus{{Name: "fourth"}, {Name: "first"}},
			sortedStatuses: []v1.ContainerStatus{{Name: "first"}, {Name: "fourth"}},
		},
		{
			containers:     []v1.Container{{Name: "first"}, {Name: "second"}, {Name: "third"}, {Name: "fourth"}},
			statuses:       []v1.ContainerStatus{{Name: "first"}, {Name: "third"}},
			sortedStatuses: []v1.ContainerStatus{{Name: "first"}, {Name: "third"}},
		},
	}
	for _, data := range cases {
		// 设置容器定义后调用排序并比较结果。
		pod.Spec.InitContainers = data.containers
		SortInitContainerStatuses(&pod, data.statuses)
		if !reflect.DeepEqual(data.statuses, data.sortedStatuses) {
			t.Errorf("SortInitContainerStatuses result wrong:\nContainers order: %v\nExpected order: %v\nReturne order: %v",
				data.containers, data.sortedStatuses, data.statuses)
		}
	}
}

// TestSortStatusesOfInitContainers 验证按状态 map 生成有序的 init 容器状态列表。
func TestSortStatusesOfInitContainers(t *testing.T) {
	pod := v1.Pod{
		Spec: v1.PodSpec{},
	}
	// 覆盖完整、乱序与部分状态的情况。
	var tests = []struct {
		containers     []v1.Container
		statusMap      map[string]*v1.ContainerStatus
		expectStatuses []v1.ContainerStatus
	}{
		{
			containers:     []v1.Container{{Name: "first"}, {Name: "second"}, {Name: "third"}, {Name: "fourth"}},
			expectStatuses: []v1.ContainerStatus{{Name: "first"}, {Name: "second"}, {Name: "third"}, {Name: "fourth"}},
			statusMap:      map[string]*v1.ContainerStatus{"first": {Name: "first"}, "second": {Name: "second"}, "third": {Name: "third"}, "fourth": {Name: "fourth"}},
		},
		{
			containers:     []v1.Container{{Name: "first"}, {Name: "second"}, {Name: "third"}, {Name: "fourth"}},
			expectStatuses: []v1.ContainerStatus{{Name: "first"}, {Name: "second"}, {Name: "third"}, {Name: "fourth"}},
			statusMap:      map[string]*v1.ContainerStatus{"second": {Name: "second"}, "third": {Name: "third"}, "first": {Name: "first"}, "fourth": {Name: "fourth"}},
		},
		{
			containers:     []v1.Container{{Name: "first"}, {Name: "second"}, {Name: "third"}, {Name: "fourth"}},
			expectStatuses: []v1.ContainerStatus{{Name: "first"}, {Name: "third"}, {Name: "fourth"}},
			statusMap:      map[string]*v1.ContainerStatus{"third": {Name: "third"}, "first": {Name: "first"}, "fourth": {Name: "fourth"}},
		},
		{
			containers:     []v1.Container{{Name: "first"}, {Name: "second"}, {Name: "third"}, {Name: "fourth"}},
			expectStatuses: []v1.ContainerStatus{{Name: "first"}, {Name: "third"}, {Name: "fourth"}},
			statusMap:      map[string]*v1.ContainerStatus{"first": {Name: "first"}, "third": {Name: "third"}, "fourth": {Name: "fourth"}},
		},
	}
	for _, data := range tests {
		// 逐用例比较排序结果。
		pod.Spec.InitContainers = data.containers
		result := SortStatusesOfInitContainers(&pod, data.statusMap)
		require.Equal(t, result, data.expectStatuses, "Unexpected result from SortStatusesOfInitContainers: %v", result)
	}
}

// TestNewTimestamp 验证 NewTimestamp 返回当前时间。
func TestNewTimestamp(t *testing.T) {
	timeStart := time.Now()
	timestamp := NewTimestamp()
	timeEnd := time.Now()
	// 生成的时间应落在调用前后的时间区间内。
	assert.WithinDuration(t, timestamp.Get(), timeStart, timeEnd.Sub(timeStart))
}
