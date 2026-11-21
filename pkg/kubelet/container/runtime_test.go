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

package container

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestParseContainerID 校验 "type://id" 解析：带引号、无引号、缺少分隔符与空串。
func TestParseContainerID(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	tests := []struct {
		name     string
		input    string
		expected ContainerID
	}{
		{
			name:     "valid docker container id",
			input:    `"docker://abc123"`,
			expected: ContainerID{Type: "docker", ID: "abc123"},
		},
		{
			name:     "valid containerd container id",
			input:    `"containerd://def456"`,
			expected: ContainerID{Type: "containerd", ID: "def456"},
		},
		{
			name:     "valid format - no quotes",
			input:    "docker://abc123",
			expected: ContainerID{Type: "docker", ID: "abc123"},
		},
		{
			name:     "invalid format - missing separator",
			input:    `"dockerabc123"`,
			expected: ContainerID{},
		},
		{
			name:     "empty string",
			input:    `""`,
			expected: ContainerID{},
		},
	}

	// 解析失败只记录日志并返回零值，因此无效输入统一期望空 ContainerID。
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseContainerID(logger, tt.input)
			assert.Equal(t, tt.expected, result, "ParseContainerID(%q)", tt.input)
		})
	}
}

// TestContainerIDString 校验 ContainerID 的字符串化结果。
func TestContainerIDString(t *testing.T) {
	tests := []struct {
		name     string
		cid      ContainerID
		expected string
	}{
		{
			name:     "docker container",
			cid:      ContainerID{Type: "docker", ID: "abc123"},
			expected: "docker://abc123",
		},
		{
			name:     "containerd container",
			cid:      ContainerID{Type: "containerd", ID: "def456"},
			expected: "containerd://def456",
		},
		{
			name:     "empty container id",
			cid:      ContainerID{},
			expected: "://",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.cid.String()
			assert.Equal(t, tt.expected, result, "ContainerID.String()")
		})
	}
}

// TestPodStatusFindContainerStatusByName 校验按名称查找容器状态，同名时返回第一个。
func TestPodStatusFindContainerStatusByName(t *testing.T) {
	podStatus := &PodStatus{
		ContainerStatuses: []*Status{
			{Name: "container1", State: ContainerStateRunning},
			{Name: "container2", State: ContainerStateExited},
			{Name: "container1", State: ContainerStateCreated}, // 同名条目，用于验证返回第一个匹配
		},
	}

	tests := []struct {
		name           string
		containerName  string
		expectedStatus *Status
	}{
		{
			name:           "find existing container",
			containerName:  "container1",
			expectedStatus: podStatus.ContainerStatuses[0], // 应返回第一个匹配
		},
		{
			name:           "find another existing container",
			containerName:  "container2",
			expectedStatus: podStatus.ContainerStatuses[1],
		},
		{
			name:           "find non-existing container",
			containerName:  "nonexistent",
			expectedStatus: nil,
		},
		{
			name:           "empty container name",
			containerName:  "",
			expectedStatus: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := podStatus.FindContainerStatusByName(tt.containerName)
			assert.Equal(t, tt.expectedStatus, result, "FindContainerStatusByName(%q)", tt.containerName)
		})
	}
}

// TestPodStatusFindActiveContainerStatusByName 校验在活跃容器状态中按名称查找。
func TestPodStatusFindActiveContainerStatusByName(t *testing.T) {
	podStatus := &PodStatus{
		ActiveContainerStatuses: []*Status{
			{Name: "container1", State: ContainerStateRunning},
			{Name: "container2", State: ContainerStateExited},
			{Name: "container1", State: ContainerStateCreated}, // 同名条目，用于验证返回第一个匹配
		},
	}

	tests := []struct {
		name           string
		containerName  string
		expectedStatus *Status
	}{
		{
			name:           "find existing container",
			containerName:  "container1",
			expectedStatus: podStatus.ActiveContainerStatuses[0], // 应返回第一个匹配
		},
		{
			name:           "find another existing container",
			containerName:  "container2",
			expectedStatus: podStatus.ActiveContainerStatuses[1],
		},
		{
			name:           "find non-existing container",
			containerName:  "nonexistent",
			expectedStatus: nil,
		},
		{
			name:           "empty container name",
			containerName:  "",
			expectedStatus: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := podStatus.FindActiveContainerStatusByName(tt.containerName)
			assert.Equal(t, tt.expectedStatus, result, "FindActiveContainerStatusByName(%q)", tt.containerName)
		})
	}
}

// TestPodStatusGetRunningContainerStatuses 校验只返回运行中的容器状态。
func TestPodStatusGetRunningContainerStatuses(t *testing.T) {
	podStatus := &PodStatus{
		ContainerStatuses: []*Status{
			{Name: "container1", State: ContainerStateRunning},
			{Name: "container2", State: ContainerStateExited},
			{Name: "container3", State: ContainerStateRunning},
			{Name: "container4", State: ContainerStateCreated},
		},
	}

	expected := []*Status{
		podStatus.ContainerStatuses[0], // container1，运行中
		podStatus.ContainerStatuses[2], // container3，运行中
	}

	result := podStatus.GetRunningContainerStatuses()
	assert.Equal(t, expected, result, "GetRunningContainerStatuses()")
}

// TestGetPodFullName 校验由 Pod 对象生成全名（name_namespace）。
func TestGetPodFullName(t *testing.T) {
	tests := []struct {
		name     string
		pod      *v1.Pod
		expected string
	}{
		{
			name: "normal pod",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-pod",
					Namespace: "test-namespace",
				},
			},
			expected: "test-pod_test-namespace",
		},
		{
			name: "pod with empty name and namespace",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "",
					Namespace: "",
				},
			},
			expected: "_",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetPodFullName(tt.pod)
			assert.Equal(t, tt.expected, result, "GetPodFullName()")
		})
	}
}

// TestBuildPodFullName 校验由名称与命名空间拼接全名的边界情况。
func TestBuildPodFullName(t *testing.T) {
	tests := []struct {
		name      string
		podName   string
		namespace string
		expected  string
	}{
		{
			name:      "normal pod",
			podName:   "test-pod",
			namespace: "test-namespace",
			expected:  "test-pod_test-namespace",
		},
		{
			name:      "empty name and namespace",
			podName:   "",
			namespace: "",
			expected:  "_",
		},
		{
			name:      "empty name only",
			podName:   "",
			namespace: "test-namespace",
			expected:  "_test-namespace",
		},
		{
			name:      "empty namespace only",
			podName:   "test-pod",
			namespace: "",
			expected:  "test-pod_",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BuildPodFullName(tt.podName, tt.namespace)
			assert.Equal(t, tt.expected, result, "BuildPodFullName(%q, %q)", tt.podName, tt.namespace)
		})
	}
}

// TestParsePodFullName 校验 Pod 全名解析，空段或多下划线均应报错。
func TestParsePodFullName(t *testing.T) {
	tests := []struct {
		name              string
		podFullName       string
		expectedName      string
		expectedNamespace string
		expectError       bool
	}{
		{
			name:              "valid pod full name",
			podFullName:       "test-pod_test-namespace",
			expectedName:      "test-pod",
			expectedNamespace: "test-namespace",
			expectError:       false,
		},
		{
			name:              "invalid format - no underscore",
			podFullName:       "test-pod",
			expectedName:      "",
			expectedNamespace: "",
			expectError:       true,
		},
		{
			name:              "invalid format - multiple underscores",
			podFullName:       "test_pod_namespace",
			expectedName:      "",
			expectedNamespace: "",
			expectError:       true,
		},
		{
			name:              "invalid format - empty parts",
			podFullName:       "_",
			expectedName:      "",
			expectedNamespace: "",
			expectError:       true,
		},
		{
			name:              "invalid format - empty name",
			podFullName:       "_namespace",
			expectedName:      "",
			expectedNamespace: "",
			expectError:       true,
		},
		{
			name:              "invalid format - empty namespace",
			podFullName:       "pod_",
			expectedName:      "",
			expectedNamespace: "",
			expectError:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, namespace, err := ParsePodFullName(tt.podFullName)

			if tt.expectError {
				require.Error(t, err, "ParsePodFullName(%q) expected error", tt.podFullName)
			} else {
				require.NoError(t, err, "ParsePodFullName(%q) unexpected error", tt.podFullName)
			}
			assert.Equal(t, tt.expectedName, name, "ParsePodFullName(%q) name", tt.podFullName)
			assert.Equal(t, tt.expectedNamespace, namespace, "ParsePodFullName(%q) namespace", tt.podFullName)
		})
	}
}

// TestPodFindContainerByName 校验按名称查找容器，同名时返回第一个。
func TestPodFindContainerByName(t *testing.T) {
	pod := &Pod{
		Containers: []*Container{
			{Name: "container1", ID: ContainerID{Type: "docker", ID: "abc123"}},
			{Name: "container2", ID: ContainerID{Type: "docker", ID: "def456"}},
			{Name: "container1", ID: ContainerID{Type: "docker", ID: "ghi789"}}, // 同名条目，用于验证返回第一个匹配
		},
	}

	tests := []struct {
		name              string
		containerName     string
		expectedContainer *Container
	}{
		{
			name:              "find existing container",
			containerName:     "container1",
			expectedContainer: pod.Containers[0], // 应返回第一个匹配
		},
		{
			name:              "find another existing container",
			containerName:     "container2",
			expectedContainer: pod.Containers[1],
		},
		{
			name:              "find non-existing container",
			containerName:     "nonexistent",
			expectedContainer: nil,
		},
		{
			name:              "empty container name",
			containerName:     "",
			expectedContainer: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pod.FindContainerByName(tt.containerName)
			assert.Equal(t, tt.expectedContainer, result, "FindContainerByName(%q)", tt.containerName)
		})
	}
}

// TestPodFindContainerByID 校验按 ContainerID（类型 + ID）精确查找容器。
func TestPodFindContainerByID(t *testing.T) {
	pod := &Pod{
		Containers: []*Container{
			{Name: "container1", ID: ContainerID{Type: "docker", ID: "abc123"}},
			{Name: "container2", ID: ContainerID{Type: "containerd", ID: "def456"}},
		},
	}

	tests := []struct {
		name              string
		containerID       ContainerID
		expectedContainer *Container
	}{
		{
			name:              "find existing container",
			containerID:       ContainerID{Type: "docker", ID: "abc123"},
			expectedContainer: pod.Containers[0],
		},
		{
			name:              "find another existing container",
			containerID:       ContainerID{Type: "containerd", ID: "def456"},
			expectedContainer: pod.Containers[1],
		},
		{
			name:              "find non-existing container",
			containerID:       ContainerID{Type: "docker", ID: "nonexistent"},
			expectedContainer: nil,
		},
		{
			name:              "empty container id",
			containerID:       ContainerID{},
			expectedContainer: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pod.FindContainerByID(tt.containerID)
			assert.Equal(t, tt.expectedContainer, result, "FindContainerByID(%v)", tt.containerID)
		})
	}
}

// TestPodFindSandboxByID 校验按 ContainerID 查找 sandbox。
func TestPodFindSandboxByID(t *testing.T) {
	pod := &Pod{
		Sandboxes: []*Container{
			{Name: "sandbox1", ID: ContainerID{Type: "docker", ID: "abc123"}},
			{Name: "sandbox2", ID: ContainerID{Type: "containerd", ID: "def456"}},
		},
	}

	tests := []struct {
		name            string
		sandboxID       ContainerID
		expectedSandbox *Container
	}{
		{
			name:            "find existing sandbox",
			sandboxID:       ContainerID{Type: "docker", ID: "abc123"},
			expectedSandbox: pod.Sandboxes[0],
		},
		{
			name:            "find another existing sandbox",
			sandboxID:       ContainerID{Type: "containerd", ID: "def456"},
			expectedSandbox: pod.Sandboxes[1],
		},
		{
			name:            "find non-existing sandbox",
			sandboxID:       ContainerID{Type: "docker", ID: "nonexistent"},
			expectedSandbox: nil,
		},
		{
			name:            "empty sandbox id",
			sandboxID:       ContainerID{},
			expectedSandbox: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pod.FindSandboxByID(tt.sandboxID)
			assert.Equal(t, tt.expectedSandbox, result, "FindSandboxByID(%v)", tt.sandboxID)
		})
	}
}

// TestPodToAPIPod 校验运行时 Pod 到 v1.Pod 的最小字段转换。
func TestPodToAPIPod(t *testing.T) {
	pod := &Pod{
		ID:        "test-uid",
		Name:      "test-pod",
		Namespace: "test-namespace",
		Containers: []*Container{
			{Name: "container1", Image: "nginx:latest"},
			{Name: "container2", Image: "redis:latest"},
		},
	}

	expected := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "test-uid",
			Name:      "test-pod",
			Namespace: "test-namespace",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{Name: "container1", Image: "nginx:latest"},
				{Name: "container2", Image: "redis:latest"},
			},
		},
	}

	result := pod.ToAPIPod()
	assert.Equal(t, expected, result, "ToAPIPod()")
}

// TestPodIsEmpty 校验空 Pod 判定：只有完全零值才视为空。
func TestPodIsEmpty(t *testing.T) {
	tests := []struct {
		name     string
		pod      *Pod
		expected bool
	}{
		{
			name:     "empty pod",
			pod:      &Pod{},
			expected: true,
		},
		{
			name: "non-empty pod",
			pod: &Pod{
				ID:        "test-uid",
				Name:      "test-pod",
				Namespace: "test-namespace",
			},
			expected: false,
		},
		{
			name: "pod with containers",
			pod: &Pod{
				Containers: []*Container{{Name: "container1"}},
			},
			expected: false,
		},
		{
			name: "pod with sandboxes",
			pod: &Pod{
				Sandboxes: []*Container{{Name: "sandbox1"}},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.pod.IsEmpty()
			assert.Equal(t, tt.expected, result, "IsEmpty()")
		})
	}
}

// TestPodsFindPod 校验全名优先、UID 兜底的 Pod 查找语义。
func TestPodsFindPod(t *testing.T) {
	pods := Pods{
		{ID: "uid1", Name: "pod1", Namespace: "ns1"},
		{ID: "uid2", Name: "pod2", Namespace: "ns2"},
	}

	tests := []struct {
		name        string
		podFullName string
		podUID      types.UID
		expected    Pod
	}{
		{
			name:        "find by full name",
			podFullName: "pod1_ns1",
			podUID:      "",
			expected:    *pods[0],
		},
		{
			name:        "find by uid when full name is empty",
			podFullName: "",
			podUID:      "uid2",
			expected:    *pods[1],
		},
		{
			name:        "find by full name takes precedence",
			podFullName: "pod1_ns1",
			podUID:      "uid2",
			expected:    *pods[0],
		},
		{
			name:        "find non-existing pod",
			podFullName: "pod3_ns3",
			podUID:      "uid3",
			expected:    Pod{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := pods.FindPod(tt.podFullName, tt.podUID)
			assert.Equal(t, tt.expected, result, "FindPod(%q, %q)", tt.podFullName, tt.podUID)
		})
	}
}

// TestRuntimeStatusGetRuntimeCondition 校验按类型获取运行时条件，未知类型返回 nil。
func TestRuntimeStatusGetRuntimeCondition(t *testing.T) {
	status := &RuntimeStatus{
		Conditions: []RuntimeCondition{
			{Type: RuntimeReady, Status: true, Reason: "ready", Message: "runtime is ready"},
			{Type: NetworkReady, Status: false, Reason: "not ready", Message: "network is not ready"},
		},
	}

	tests := []struct {
		name          string
		conditionType RuntimeConditionType
		expected      *RuntimeCondition
	}{
		{
			name:          "find existing condition",
			conditionType: RuntimeReady,
			expected:      &status.Conditions[0],
		},
		{
			name:          "find another existing condition",
			conditionType: NetworkReady,
			expected:      &status.Conditions[1],
		},
		{
			name:          "find non-existing condition",
			conditionType: "NonExistent",
			expected:      nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := status.GetRuntimeCondition(tt.conditionType)
			assert.Equal(t, tt.expected, result, "GetRuntimeCondition(%q)", tt.conditionType)
		})
	}
}

// TestRuntimeStatusString 校验运行时状态的字符串格式（条件、handler、特性）。
func TestRuntimeStatusString(t *testing.T) {
	status := &RuntimeStatus{
		Conditions: []RuntimeCondition{
			{Type: RuntimeReady, Status: true, Reason: "ready", Message: "runtime is ready"},
			{Type: NetworkReady, Status: false, Reason: "not ready", Message: "network is not ready"},
		},
		Handlers: []RuntimeHandler{
			{Name: "handler1", SupportsRecursiveReadOnlyMounts: true, SupportsUserNamespaces: false},
			{Name: "handler2", SupportsRecursiveReadOnlyMounts: false, SupportsUserNamespaces: true},
		},
		Features: &RuntimeFeatures{SupplementalGroupsPolicy: true, UserNamespacesHostNetwork: true},
	}

	result := status.String()
	expected := "Runtime Conditions: RuntimeReady=true reason:ready message:runtime is ready, NetworkReady=false reason:not ready message:network is not ready; Handlers: Name=handler1 SupportsRecursiveReadOnlyMounts: true SupportsUserNamespaces: false, Name=handler2 SupportsRecursiveReadOnlyMounts: false SupportsUserNamespaces: true, Features: SupplementalGroupsPolicy: true UserNamespacesHostNetwork: true MountOptions: false"
	assert.Equal(t, expected, result, "String()")
}

// TestRuntimeHandlerString 校验 runtime handler 的字符串格式。
func TestRuntimeHandlerString(t *testing.T) {
	tests := []struct {
		name     string
		handler  RuntimeHandler
		expected string
	}{
		{
			name: "handler with all features",
			handler: RuntimeHandler{
				Name:                            "test-handler",
				SupportsRecursiveReadOnlyMounts: true,
				SupportsUserNamespaces:          true,
			},
			expected: "Name=test-handler SupportsRecursiveReadOnlyMounts: true SupportsUserNamespaces: true",
		},
		{
			name: "handler with no features",
			handler: RuntimeHandler{
				Name:                            "test-handler",
				SupportsRecursiveReadOnlyMounts: false,
				SupportsUserNamespaces:          false,
			},
			expected: "Name=test-handler SupportsRecursiveReadOnlyMounts: false SupportsUserNamespaces: false",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.handler.String()
			assert.Equal(t, tt.expected, result, "String()")
		})
	}
}

// TestRuntimeConditionString 校验运行时条件的字符串格式。
func TestRuntimeConditionString(t *testing.T) {
	tests := []struct {
		name      string
		condition RuntimeCondition
		expected  string
	}{
		{
			name: "true condition",
			condition: RuntimeCondition{
				Type:    RuntimeReady,
				Status:  true,
				Reason:  "ready",
				Message: "runtime is ready",
			},
			expected: "RuntimeReady=true reason:ready message:runtime is ready",
		},
		{
			name: "false condition",
			condition: RuntimeCondition{
				Type:    NetworkReady,
				Status:  false,
				Reason:  "not ready",
				Message: "network is not ready",
			},
			expected: "NetworkReady=false reason:not ready message:network is not ready",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.condition.String()
			assert.Equal(t, tt.expected, result, "String()")
		})
	}
}

// TestRuntimeFeaturesString 校验运行时特性的字符串格式，nil 时返回 "nil"。
func TestRuntimeFeaturesString(t *testing.T) {
	tests := []struct {
		name     string
		features *RuntimeFeatures
		expected string
	}{
		{
			name: "features with both flags true",
			features: &RuntimeFeatures{
				SupplementalGroupsPolicy:  true,
				UserNamespacesHostNetwork: true,
			},
			expected: "SupplementalGroupsPolicy: true UserNamespacesHostNetwork: true MountOptions: false",
		},
		{
			name: "features with both flags false",
			features: &RuntimeFeatures{
				SupplementalGroupsPolicy:  false,
				UserNamespacesHostNetwork: false,
			},
			expected: "SupplementalGroupsPolicy: false UserNamespacesHostNetwork: false MountOptions: false",
		},
		{
			name:     "nil features",
			features: nil,
			expected: "nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.features.String()
			assert.Equal(t, tt.expected, result, "String()")
		})
	}
}

// TestSortContainerStatusesByCreationTime 逐一校验排序接口的 Len、Swap、Less。
func TestSortContainerStatusesByCreationTime(t *testing.T) {
	// 创建时间刻意乱序，Swap 后再用 Less 检查顺序关系。
	now := time.Now()
	statuses := SortContainerStatusesByCreationTime{
		{CreatedAt: now.Add(2 * time.Hour)},
		{CreatedAt: now},
		{CreatedAt: now.Add(1 * time.Hour)},
	}

	// 验证 Len。
	assert.Len(t, statuses, 3, "Len()")

	// 验证 Swap。
	original1 := statuses[1]
	original2 := statuses[2]
	statuses.Swap(1, 2)
	assert.Equal(t, original1, statuses[2], "Swap(0, 1) did not work correctly (0)")
	assert.Equal(t, original2, statuses[1], "Swap(0, 1) did not work correctly (1)")

	// 验证 Less：按创建时间升序。
	assert.True(t, statuses.Less(1, 0), "Less(1, 0) should be true")
	assert.False(t, statuses.Less(0, 1), "Less(0, 1) should be false")
}
