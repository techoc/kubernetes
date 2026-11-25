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

package results

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
)

// TestCacheOperations 验证缓存管理器 Set/Get/Remove 的基本行为。
func TestCacheOperations(t *testing.T) {
	m := NewManager()

	unsetID := kubecontainer.ContainerID{Type: "test", ID: "unset"}
	setID := kubecontainer.ContainerID{Type: "test", ID: "set"}

	_, found := m.Get(unsetID)
	assert.False(t, found, "unset result found")

	// 设置结果后应能按容器 ID 读回。
	m.Set(setID, Success, &corev1.Pod{})
	result, found := m.Get(setID)
	assert.Equal(t, Success, result, "set result")
	assert.True(t, found, "set result found")

	// 删除后查询应返回未找到。
	m.Remove(setID)
	_, found = m.Get(setID)
	assert.False(t, found, "removed result found")
}

// TestUpdates 验证仅在结果发生变化时推送更新。
func TestUpdates(t *testing.T) {
	m := NewManager()

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test-pod"}}
	fooID := kubecontainer.ContainerID{Type: "test", ID: "foo"}
	barID := kubecontainer.ContainerID{Type: "test", ID: "bar"}

	// expectUpdate 等待一次更新并校验内容，超时即失败。
	expectUpdate := func(expected Update, msg string) {
		select {
		case u := <-m.Updates():
			if expected != u {
				t.Errorf("Expected update %v, received %v: %s", expected, u, msg)
			}
		case <-time.After(wait.ForeverTestTimeout):
			t.Errorf("Timed out waiting for update %v: %s", expected, msg)
		}
	}

	// expectNoUpdate 断言当前没有待消费的更新。
	expectNoUpdate := func(msg string) {
		// 注意：更新是异步累积的，本方法不保证在应当失败时一定失败；
		// 若漏报，后续 expectUpdate 仍会失败。
		select {
		case u := <-m.Updates():
			t.Errorf("Unexpected update %v: %s", u, msg)
		default:
			// 通过。
		}
	}

	// 新结果必须推送更新。
	m.Set(fooID, Success, pod)
	expectUpdate(Update{fooID, Success, pod.UID}, "new success")

	m.Set(barID, Failure, pod)
	expectUpdate(Update{barID, Failure, pod.UID}, "new failure")

	// 结果未变化时不应推送更新。
	m.Set(fooID, Success, pod)
	expectNoUpdate("unchanged foo")

	m.Set(barID, Failure, pod)
	expectNoUpdate("unchanged bar")

	// 结果变化时必须推送更新。
	m.Set(fooID, Failure, pod)
	expectUpdate(Update{fooID, Failure, pod.UID}, "changed foo")

	m.Set(barID, Success, pod)
	expectUpdate(Update{barID, Success, pod.UID}, "changed bar")
}

// TestResult_ToPrometheusType 验证 Result 到 Prometheus 数值的映射：
// Success=0、Failure=1、其他=-1。
func TestResult_ToPrometheusType(t *testing.T) {
	tests := []struct {
		name     string
		result   Result
		expected float64
	}{
		{
			name:     "result is Success",
			result:   Success,
			expected: 0,
		},
		{
			name:     "result is Failure",
			result:   Failure,
			expected: 1,
		},
		{
			name:     "result is other",
			result:   123,
			expected: -1,
		},
	}
	// 逐用例比较映射值。
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.result.ToPrometheusType(); got != test.expected {
				t.Errorf("Result.ToPrometheusType() = %v, expected %v", got, test.expected)
			}
		})
	}
}

// TestResult_String 验证 Result 的字符串表示。
func TestResult_String(t *testing.T) {
	tests := []struct {
		name     string
		result   Result
		expected string
	}{
		{
			name:     "result is Success",
			result:   Success,
			expected: "Success",
		},
		{
			name:     "result is Failure",
			result:   Failure,
			expected: "Failure",
		},
		{
			name:     "result is other",
			result:   -123,
			expected: "UNKNOWN",
		},
	}
	// 逐用例比较字符串输出。
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.result.String(); got != test.expected {
				t.Errorf("Result.String() = %v, expected %v", got, test.expected)
			}
		})
	}
}
