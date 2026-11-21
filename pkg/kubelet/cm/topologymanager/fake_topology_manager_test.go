/*
Copyright 2019 The Kubernetes Authors.

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

package topologymanager

import (
	"reflect"
	"testing"

	"k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestNewFakeManager 验证 NewFakeManager 返回的对象实现了 Manager 接口。
func TestNewFakeManager(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	fm := NewFakeManager(logger)

	// 返回值应满足 Manager 接口。
	if _, ok := fm.(Manager); !ok {
		t.Errorf("Result is not Manager type")

	}
}

// TestFakeGetAffinity 验证 fake manager 对任意容器返回空的拓扑提示。
func TestFakeGetAffinity(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	tcases := []struct {
		name          string
		containerName string
		podUID        string
		expected      TopologyHint
	}{
		{
			name:          "Case1",
			containerName: "nginx",
			podUID:        "0aafa4c4-38e8-11e9-bcb1-a4bf01040474",
			expected:      TopologyHint{},
		},
	}
	for _, tc := range tcases {
		fm := fakeManager{}
		actual := fm.GetAffinity(logger, tc.podUID, tc.containerName)
		if !reflect.DeepEqual(actual, tc.expected) {
			t.Errorf("Expected Affinity in result to be %v, got %v", tc.expected, actual)
		}
	}
}

// TestFakeRemoveContainer 验证 fake manager 的 RemoveContainer 始终返回成功。
func TestFakeRemoveContainer(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	testCases := []struct {
		name        string
		containerID string
		podUID      string
	}{
		{
			name:        "Case1",
			containerID: "nginx",
			podUID:      "0aafa4c4-38e8-11e9-bcb1-a4bf01040474",
		},
		{
			name:        "Case2",
			containerID: "Busy_Box",
			podUID:      "b3ee37fc-39a5-11e9-bcb1-a4bf01040474",
		},
	}
	fm := fakeManager{}
	for _, tc := range testCases {
		err := fm.RemoveContainer(logger, tc.containerID)
		if err != nil {
			t.Errorf("Expected error to be nil but got: %v", err)
		}

	}

}

// TestFakeAdmit 验证 fake manager 对三种 QoS 类别的 Pod 都返回准入。
func TestFakeAdmit(t *testing.T) {
	tCtx := ktesting.Init(t)
	tcases := []struct {
		name     string
		result   lifecycle.PodAdmitResult
		qosClass v1.PodQOSClass
		expected bool
	}{
		{
			name:     "QOSClass set as Guaranteed",
			result:   lifecycle.PodAdmitResult{},
			qosClass: v1.PodQOSGuaranteed,
			expected: true,
		},
		{
			name:     "QOSClass set as Burstable",
			result:   lifecycle.PodAdmitResult{},
			qosClass: v1.PodQOSBurstable,
			expected: true,
		},
		{
			name:     "QOSClass set as BestEffort",
			result:   lifecycle.PodAdmitResult{},
			qosClass: v1.PodQOSBestEffort,
			expected: true,
		},
	}
	fm := fakeManager{}
	// fake manager 对所有 QoS 都返回准入，因此结果不应等于零值。
	for _, tc := range tcases {
		podAttr := lifecycle.PodAdmitAttributes{}
		pod := v1.Pod{}
		// 设置 Pod 的 QoS 类别后调用 Admit。
		pod.Status.QOSClass = tc.qosClass
		podAttr.Pod = &pod
		actual := fm.Admit(tCtx, &podAttr)
		if reflect.DeepEqual(actual, tc.result) {
			t.Errorf("Error occurred, expected Admit in result to be %v got %v", tc.result, actual.Admit)
		}
	}
}
