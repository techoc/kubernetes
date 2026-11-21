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

package kubelet

import (
	"testing"

	"github.com/stretchr/testify/assert"

	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	kubefeatures "k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestPodResourceLimitsDefaulting 验证 downward API 的 Pod 资源限制默认化逻辑：
// 未设置限制的维度应回退到节点可分配量；启用 PodLevelResources 时优先使用 Pod 级限制。
func TestPodResourceLimitsDefaulting(t *testing.T) {
	tCtx := ktesting.Init(t)
	tk := newTestKubelet(t, true)
	defer tk.Cleanup()
	// 构造一个可分配 6 CPU / 4Gi 内存的节点，作为限制回退的上限。
	tk.kubelet.nodeLister = &testNodeLister{
		nodes: []*v1.Node{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: string(tk.kubelet.nodeName),
				},
				Status: v1.NodeStatus{
					Allocatable: v1.ResourceList{
						v1.ResourceCPU:    resource.MustParse("6"),
						v1.ResourceMemory: resource.MustParse("4Gi"),
					},
				},
			},
		},
	}
	// 用例覆盖容器级限制为空/部分设置，以及 Pod 级限制与容器级限制的组合。
	cases := []struct {
		pod                      *v1.Pod
		expected                 *v1.Pod
		podLevelResourcesEnabled bool
	}{
		{
			pod:      getPod("0", "0"),
			expected: getPod("6", "4Gi"),
		},
		{
			pod:      getPod("1", "0"),
			expected: getPod("1", "4Gi"),
		},
		{
			pod:      getPod("", ""),
			expected: getPod("6", "4Gi"),
		},
		{
			pod:      getPod("0", "1Mi"),
			expected: getPod("6", "1Mi"),
		},
		{
			pod:                      getPodWithPodLevelResources("0", "1Mi", "0", "0"),
			expected:                 getPodWithPodLevelResources("0", "1Mi", "6", "1Mi"),
			podLevelResourcesEnabled: true,
		},
		{
			pod:                      getPodWithPodLevelResources("1", "0", "0", "0"),
			expected:                 getPodWithPodLevelResources("1", "0", "1", "4Gi"),
			podLevelResourcesEnabled: true,
		},
		{
			pod:                      getPodWithPodLevelResources("1", "1Mi", "", ""),
			expected:                 getPodWithPodLevelResources("1", "1Mi", "1", "1Mi"),
			podLevelResourcesEnabled: true,
		},
		{
			pod:                      getPodWithPodLevelResources("1", "5Mi", "0", "1Mi"),
			expected:                 getPodWithPodLevelResources("1", "5Mi", "1", "1Mi"),
			podLevelResourcesEnabled: true,
		},
		{
			pod:                      getPodWithPodLevelResources("1", "5Mi", "1", "1Mi"),
			expected:                 getPodWithPodLevelResources("1", "5Mi", "1", "1Mi"),
			podLevelResourcesEnabled: true,
		},
	}
	// 逐用例切换特性门控后调用 defaultPodLimitsForDownwardAPI，并做语义深比较。
	as := assert.New(t)
	for idx, tc := range cases {
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, kubefeatures.PodLevelResources, tc.podLevelResourcesEnabled)
		actual, _, err := tk.kubelet.defaultPodLimitsForDownwardAPI(tCtx, tc.pod, nil)
		as.NoError(err, "failed to default pod limits: %v", err)
		if !apiequality.Semantic.DeepEqual(tc.expected, actual) {
			as.Fail("test case [%d] failed.  Expected: %+v, Got: %+v", idx, tc.expected, actual)
		}
	}
}

// getPod 构造只设置容器级限制的 Pod；空字符串表示该维度不设置限制。
func getPod(cpuLimit, memoryLimit string) *v1.Pod {
	resources := v1.ResourceRequirements{}
	// 至少一个维度有值时才创建 Limits map，避免空 map 影响默认化判断。
	if cpuLimit != "" || memoryLimit != "" {
		resources.Limits = make(v1.ResourceList)
	}
	if cpuLimit != "" {
		resources.Limits[v1.ResourceCPU] = resource.MustParse(cpuLimit)
	}
	if memoryLimit != "" {
		resources.Limits[v1.ResourceMemory] = resource.MustParse(memoryLimit)
	}
	return &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:      "foo",
					Resources: resources,
				},
			},
		},
	}
}

// getPodWithPodLevelResources 在 getPod 基础上补充 Pod 级限制，
// 参数依次为 Pod 级与容器级的 CPU/内存限制。
func getPodWithPodLevelResources(plCPULimit, plMemoryLimit, clCPULimit, clMemoryLimit string) *v1.Pod {
	pod := getPod(clCPULimit, clMemoryLimit)
	resources := v1.ResourceRequirements{}
	if plCPULimit != "" || plMemoryLimit != "" {
		resources.Limits = make(v1.ResourceList)
	}
	if plCPULimit != "" {
		resources.Limits[v1.ResourceCPU] = resource.MustParse(plCPULimit)
	}
	if plMemoryLimit != "" {
		resources.Limits[v1.ResourceMemory] = resource.MustParse(plMemoryLimit)
	}
	pod.Spec.Resources = &resources
	return pod
}
