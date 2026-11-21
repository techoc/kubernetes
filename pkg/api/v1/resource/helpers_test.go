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

package resource

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/features"
)

const (
	// 测试 Pod 中主容器与 init 容器的共享名称；用例和辅助函数都引用它们，
	// 避免查询容器时因名称不一致而误判为“找不到容器”。
	containerName     = "test-container"
	initContainerName = "init-test-container"
)

// TestResourceHelpers 验证 ResourceList 的便捷访问器（Cpu/Memory）。
// 既覆盖资源存在时返回正确数量，也覆盖资源缺失时返回零值的情况，
// 因为调用方依赖“缺失即零”的语义来省略显式的存在性检查。
func TestResourceHelpers(t *testing.T) {
	cpuLimit := resource.MustParse("10")
	memoryLimit := resource.MustParse("10G")
	resourceSpec := v1.ResourceRequirements{
		Limits: v1.ResourceList{
			v1.ResourceCPU:    cpuLimit,
			v1.ResourceMemory: memoryLimit,
		},
	}
	// Cmp 返回 0 才表示两个 Quantity 相等；不直接用 == 是因为 Quantity 内部含不可比较字段。
	if res := resourceSpec.Limits.Cpu(); res.Cmp(cpuLimit) != 0 {
		t.Errorf("expected cpulimit %v, got %v", cpuLimit, res)
	}
	if res := resourceSpec.Limits.Memory(); res.Cmp(memoryLimit) != 0 {
		t.Errorf("expected memorylimit %v, got %v", memoryLimit, res)
	}
	resourceSpec = v1.ResourceRequirements{
		Limits: v1.ResourceList{
			v1.ResourceMemory: memoryLimit,
		},
	}
	// 只设置内存时，Cpu() 必须返回零值，这样调用方无需先判断 key 是否存在。
	if res := resourceSpec.Limits.Cpu(); res.Value() != 0 {
		t.Errorf("expected cpulimit %v, got %v", 0, res)
	}
	if res := resourceSpec.Limits.Memory(); res.Cmp(memoryLimit) != 0 {
		t.Errorf("expected memorylimit %v, got %v", memoryLimit, res)
	}
}

// TestDefaultResourceHelpers 验证空的 ResourceList 上访问器返回的 Quantity 仍带有正确的默认格式：
// CPU 使用 DecimalSI（十进制），内存使用 BinarySI（二进制）。
// 格式会影响资源值的字符串展示与取整方式，因此即使数量为零也必须正确。
func TestDefaultResourceHelpers(t *testing.T) {
	resourceList := v1.ResourceList{}
	if resourceList.Cpu().Format != resource.DecimalSI {
		t.Errorf("expected %v, actual %v", resource.DecimalSI, resourceList.Cpu().Format)
	}
	if resourceList.Memory().Format != resource.BinarySI {
		t.Errorf("expected %v, actual %v", resource.BinarySI, resourceList.Memory().Format)
	}
}

// TestGetResourceRequest 验证 GetResourceRequest 对容器级 request、Pod 级 request、
// Pod overhead 以及 PodLevelResources 特性开关的各种组合的取值行为。
// 关键预期：Pod overhead 不归到单个容器名下；开启特性且设置了 Pod 级资源时，
// Pod 级资源优先于容器级资源；特性开启但 Pod 没有设置 Pod 级资源时回退到容器级资源。
func TestGetResourceRequest(t *testing.T) {
	cases := []struct {
		pod                      *v1.Pod
		cName                    string // 仅用于断言失败信息中标识用例，不参与被测函数调用。
		resourceName             v1.ResourceName
		expectedValue            int64
		podLevelResourcesEnabled bool // 每条用例单独控制特性开关，因为被测行为随开关变化。
	}{
		// 第一组：仅有容器级 request，验证基本取值（CPU 以毫核计，内存以字节计）。
		{
			pod:           getPod(containerName, resources{cpuRequest: "9"}),
			resourceName:  v1.ResourceCPU,
			expectedValue: 9000,
		},
		{
			pod:           getPod(containerName, resources{memoryRequest: "90Mi"}),
			resourceName:  v1.ResourceMemory,
			expectedValue: 94371840,
		},
		// overhead 不属于任何单个容器，所以仅设置 overhead 时容器的 request 仍为 0。
		{
			cName:         "just-overhead for cpu",
			pod:           getPod(containerName, resources{cpuOverhead: "5", memoryOverhead: "5"}),
			resourceName:  v1.ResourceCPU,
			expectedValue: 0,
		},
		{
			cName:         "just-overhead for memory",
			pod:           getPod(containerName, resources{memoryOverhead: "5"}),
			resourceName:  v1.ResourceMemory,
			expectedValue: 0,
		},
		// 同时有 request 和 overhead 时，两者相加：2 + 5 = 7 核。
		{
			cName:         "cpu overhead and req",
			pod:           getPod(containerName, resources{cpuRequest: "2", cpuOverhead: "5", memoryOverhead: "5"}),
			resourceName:  v1.ResourceCPU,
			expectedValue: 7000,
		},
		// 内存同理：1024 字节 request + 5 字节 overhead = 1029。
		{
			cName:         "mem overhead and req",
			pod:           getPod(containerName, resources{cpuRequest: "2", memoryRequest: "1024", cpuOverhead: "5", memoryOverhead: "5"}),
			resourceName:  v1.ResourceMemory,
			expectedValue: 1029,
		},
		// 第二组：开启 PodLevelResources 且设置了 Pod 级资源。
		// 即使容器也有自己的 request，也应返回 Pod 级资源的值。
		{
			cName:                    "pod level resources cpu req, container cpu req",
			pod:                      getPodWithPodLevelResources(containerName, resources{cpuRequest: "10"}, resources{cpuRequest: "8"}),
			resourceName:             v1.ResourceCPU,
			expectedValue:            10000,
			podLevelResourcesEnabled: true,
		},
		{
			cName:                    "pod level resources mem req, container mem req",
			pod:                      getPodWithPodLevelResources(containerName, resources{memoryRequest: "100Mi"}, resources{memoryRequest: "80Mi"}),
			resourceName:             v1.ResourceMemory,
			expectedValue:            104857600,
			podLevelResourcesEnabled: true,
		},
		// Pod 级资源与容器级资源针对不同资源时，按查询的资源名各取对应来源。
		{
			cName:                    "pod level resources cpu req, container mem req",
			pod:                      getPodWithPodLevelResources(containerName, resources{cpuRequest: "5"}, resources{memoryRequest: "50Mi"}),
			resourceName:             v1.ResourceCPU,
			expectedValue:            5000,
			podLevelResourcesEnabled: true,
		},
		{
			cName:                    "pod level resources mem req, container cpu req",
			pod:                      getPodWithPodLevelResources(containerName, resources{memoryRequest: "100Mi"}, resources{cpuRequest: "8"}),
			resourceName:             v1.ResourceMemory,
			expectedValue:            104857600,
			podLevelResourcesEnabled: true,
		},
		// 容器未声明该资源时，Pod 级资源作为唯一来源生效。
		{
			cName:                    "pod level resources cpu req, container no req",
			pod:                      getPodWithPodLevelResources(containerName, resources{cpuRequest: "10"}, resources{}),
			resourceName:             v1.ResourceCPU,
			expectedValue:            10000,
			podLevelResourcesEnabled: true,
		},
		{
			cName:                    "pod level resources mem req, container no req",
			pod:                      getPodWithPodLevelResources(containerName, resources{memoryRequest: "100Mi"}, resources{}),
			resourceName:             v1.ResourceMemory,
			expectedValue:            104857600,
			podLevelResourcesEnabled: true,
		},
		// 第三组：特性开启但 Pod 没有 Pod 级资源时，必须保持旧行为（只看容器级 request），
		// 否则开启特性会意外改变存量 Pod 的计算结果。
		{
			cName:                    "cpu pod without pod level resources but pod level resources feature enabled",
			pod:                      getPod(containerName, resources{cpuRequest: "9"}),
			resourceName:             v1.ResourceCPU,
			expectedValue:            9000,
			podLevelResourcesEnabled: true,
		},
		{
			cName:                    "mem pod without pod level resources but pod level resources feature enabled",
			pod:                      getPod(containerName, resources{memoryRequest: "90Mi"}),
			resourceName:             v1.ResourceMemory,
			expectedValue:            94371840,
			podLevelResourcesEnabled: true,
		},
	}
	as := assert.New(t)
	for idx, tc := range cases {
		// 每条用例都要按自身需求切换特性开关；该辅助函数会在用例结束后恢复全局默认值，
		// 避免用例之间互相污染（Go 的 table-driven 测试按序执行但共享同一个进程状态）。
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodLevelResources, tc.podLevelResourcesEnabled)

		actual := GetResourceRequest(tc.pod, tc.resourceName)
		// 断言信息带上用例下标和 cName，失败时能直接定位是哪条表项，而不是只看到一堆数字。
		as.Equal(tc.expectedValue, actual, "expected test case [%d] %v: to return %q; got %q instead", idx, tc.cName, tc.expectedValue, actual)
	}
}

// TestExtractResourceValue 验证 ExtractResourceValueByContainerName：
// 按容器名提取 requests/limits 中的资源值，处理不存在资源（返回 "0"）、
// 数量取整（如 100m 向上取整为 1）、divisor 换算，以及 init 容器的同名路径。
func TestExtractResourceValue(t *testing.T) {
	cases := []struct {
		fs            *v1.ResourceFieldSelector
		pod           *v1.Pod
		cName         string
		expectedValue string
		expectedError error // 预留错误断言入口；当前用例均不期望出错。
	}{
		// 主容器：limits.cpu 直接取值。
		{
			fs: &v1.ResourceFieldSelector{
				Resource: "limits.cpu",
			},
			cName:         containerName,
			pod:           getPod(containerName, resources{cpuLimit: "9"}),
			expectedValue: "9",
		},
		// 容器没有该 request 时返回 "0"，而不是报错：下游模板常依赖这一默认值语义。
		{
			fs: &v1.ResourceFieldSelector{
				Resource: "requests.cpu",
			},
			cName:         containerName,
			pod:           getPod(containerName, resources{}),
			expectedValue: "0",
		},
		{
			fs: &v1.ResourceFieldSelector{
				Resource: "requests.cpu",
			},
			cName:         containerName,
			pod:           getPod(containerName, resources{cpuRequest: "8"}),
			expectedValue: "8",
		},
		// 100m（0.1 核）按整核输出时向上取整为 1。
		{
			fs: &v1.ResourceFieldSelector{
				Resource: "requests.cpu",
			},
			cName:         containerName,
			pod:           getPod(containerName, resources{cpuRequest: "100m"}),
			expectedValue: "1",
		},
		// 指定 divisor 后先除以它再输出：1200m / 100m = 12。
		{
			fs: &v1.ResourceFieldSelector{
				Resource: "requests.cpu",
				Divisor:  resource.MustParse("100m"),
			},
			cName:         containerName,
			pod:           getPod(containerName, resources{cpuRequest: "1200m"}),
			expectedValue: "12",
		},
		// 内存默认以字节数返回，100Mi = 104857600 字节。
		{
			fs: &v1.ResourceFieldSelector{
				Resource: "requests.memory",
			},
			cName:         containerName,
			pod:           getPod(containerName, resources{memoryRequest: "100Mi"}),
			expectedValue: "104857600",
		},
		// 同样支持内存的 divisor：100Mi / 1Mi = 100。
		{
			fs: &v1.ResourceFieldSelector{
				Resource: "requests.memory",
				Divisor:  resource.MustParse("1Mi"),
			},
			cName:         containerName,
			pod:           getPod(containerName, resources{memoryRequest: "100Mi", memoryLimit: "1Gi"}),
			expectedValue: "100",
		},
		{
			fs: &v1.ResourceFieldSelector{
				Resource: "limits.memory",
			},
			cName:         containerName,
			pod:           getPod(containerName, resources{memoryRequest: "10Mi", memoryLimit: "100Mi"}),
			expectedValue: "104857600",
		},
		// 以下用例与上面一一对应，但容器名换成 init 容器，验证名称查找不会失败，
		// 且 init 容器同样读取自身的 requests/limits（辅助函数给它与主容器相同的资源）。
		{
			fs: &v1.ResourceFieldSelector{
				Resource: "limits.cpu",
			},
			cName:         initContainerName,
			pod:           getPod(containerName, resources{cpuLimit: "9"}),
			expectedValue: "9",
		},
		{
			fs: &v1.ResourceFieldSelector{
				Resource: "requests.cpu",
			},
			cName:         initContainerName,
			pod:           getPod(containerName, resources{}),
			expectedValue: "0",
		},
		{
			fs: &v1.ResourceFieldSelector{
				Resource: "requests.cpu",
			},
			cName:         initContainerName,
			pod:           getPod(containerName, resources{cpuRequest: "8"}),
			expectedValue: "8",
		},
		{
			fs: &v1.ResourceFieldSelector{
				Resource: "requests.cpu",
			},
			cName:         initContainerName,
			pod:           getPod(containerName, resources{cpuRequest: "100m"}),
			expectedValue: "1",
		},
		{
			fs: &v1.ResourceFieldSelector{
				Resource: "requests.cpu",
				Divisor:  resource.MustParse("100m"),
			},
			cName:         initContainerName,
			pod:           getPod(containerName, resources{cpuRequest: "1200m"}),
			expectedValue: "12",
		},
		{
			fs: &v1.ResourceFieldSelector{
				Resource: "requests.memory",
			},
			cName:         initContainerName,
			pod:           getPod(containerName, resources{memoryRequest: "100Mi"}),
			expectedValue: "104857600",
		},
		{
			fs: &v1.ResourceFieldSelector{
				Resource: "requests.memory",
				Divisor:  resource.MustParse("1Mi"),
			},
			cName:         initContainerName,
			pod:           getPod(containerName, resources{memoryRequest: "100Mi", memoryLimit: "1Gi"}),
			expectedValue: "100",
		},
		{
			fs: &v1.ResourceFieldSelector{
				Resource: "limits.memory",
			},
			cName: initContainerName,
			pod:   getPod(containerName, resources{memoryRequest: "10Mi", memoryLimit: "100Mi"}),

			expectedValue: "104857600",
		},
	}
	as := assert.New(t)
	for idx, tc := range cases {
		actual, err := ExtractResourceValueByContainerName(tc.fs, tc.pod, tc.cName)
		if tc.expectedError != nil {
			// require.* 在失败时立即终止当前用例，避免后续断言在错误的结果上产生误导性失败。
			require.EqualError(t, err, tc.expectedError.Error(), "expected test case [%d] to fail with error %v; got %v", idx, tc.expectedError, err)
		} else {
			require.NoError(t, err, "expected test case [%d] to not return an error; got %v", idx, err)
			// 只断言字符串结果：资源字段选择器面向模板展开，输出格式本身就是契约的一部分。
			as.Equal(tc.expectedValue, actual, "expected test case [%d] to return %q; got %q instead", idx, tc.expectedValue, actual)
		}
	}
}

// resources 是表驱动用例的简写输入：空字符串表示“不设置该资源”，
// 这样每条用例可以只写出与自身相关的字段。
type resources struct {
	cpuRequest, cpuLimit, memoryRequest, memoryLimit, cpuOverhead, memoryOverhead string
}

// defineResources 把简写输入转换成 ResourceRequirements。
// 只对非空字段调用 MustParse：空字符串解析会 panic，而“不设置”是有意义的用例状态。
func defineResources(resources resources) v1.ResourceRequirements {
	r := v1.ResourceRequirements{
		Limits:   make(v1.ResourceList),
		Requests: make(v1.ResourceList),
	}

	if resources.cpuLimit != "" {
		r.Limits[v1.ResourceCPU] = resource.MustParse(resources.cpuLimit)
	}
	if resources.memoryLimit != "" {
		r.Limits[v1.ResourceMemory] = resource.MustParse(resources.memoryLimit)
	}
	if resources.cpuRequest != "" {
		r.Requests[v1.ResourceCPU] = resource.MustParse(resources.cpuRequest)
	}
	if resources.memoryRequest != "" {
		r.Requests[v1.ResourceMemory] = resource.MustParse(resources.memoryRequest)
	}

	return r
}

// getPod 构造一个包含主容器和 init 容器的测试 Pod，并带上可选的 Overhead。
// 两个容器使用同一份 ResourceRequirements，便于用不同容器名重复验证同一套取值逻辑；
// Overhead 单独存放，因为它属于 Pod 级别，不应混进容器的 requests/limits。
func getPod(cname string, resources resources) *v1.Pod {
	r := defineResources(resources)

	overhead := make(v1.ResourceList)

	if resources.cpuOverhead != "" {
		overhead[v1.ResourceCPU] = resource.MustParse(resources.cpuOverhead)
	}
	if resources.memoryOverhead != "" {
		overhead[v1.ResourceMemory] = resource.MustParse(resources.memoryOverhead)
	}

	return &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:      cname,
					Resources: r,
				},
			},
			InitContainers: []v1.Container{
				{
					Name:      "init-" + cname,
					Resources: r,
				},
			},
			Overhead: overhead,
		},
	}
}

// getPodWithPodLevelResources 在 getPod 的基础上设置 Pod 级 Resources 字段。
// 该字段受 PodLevelResources 特性门控保护，所以只在对应用例中启用该开关。
func getPodWithPodLevelResources(cname string, podResources resources, resources resources) *v1.Pod {
	pod := getPod(cname, resources)

	r := defineResources(podResources)
	pod.Spec.Resources = &r

	return pod
}
