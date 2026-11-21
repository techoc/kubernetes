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

package devicemanager

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/util/sets"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
	"k8s.io/kubernetes/pkg/kubelet/cm/devicemanager/checkpoint"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestGetContainerDevices 验证按 Pod/容器查询设备时，返回结果包含正确的设备与 NUMA 拓扑。
func TestGetContainerDevices(t *testing.T) {
	podDevices := newPodDevices()
	resourceName1 := "domain1.com/resource1"
	podID := "pod1"
	contID := "con1"
	devices := checkpoint.DevicesPerNUMA{0: []string{"dev1"}, 1: []string{"dev1"}}

	podDevices.insert(podID, contID, resourceName1,
		devices,
		newContainerAllocateResponse(
			withDevices(map[string]string{"/dev/r1dev1": "/dev/r1dev1", "/dev/r1dev2": "/dev/r1dev2"}),
			withMounts(map[string]string{"/home/r1lib1": "/usr/r1lib1"}),
		),
	)

	// 查询结果应以资源名为键，值为设备 ID 到插件信息的映射。
	resContDevices := podDevices.getContainerDevices(podID, contID)
	contDevices, ok := resContDevices[resourceName1]
	require.True(t, ok, "resource %q not present", resourceName1)

	for devID, plugInfo := range contDevices {
		nodes := plugInfo.GetTopology().GetNodes()
		require.Equal(t, len(nodes), len(devices), "Incorrect container devices: %v - %v (nodes %v)", devices, contDevices, nodes)

		for _, node := range plugInfo.GetTopology().GetNodes() {
			dev, ok := devices[node.ID]
			require.True(t, ok, "NUMA id %v doesn't exist in result", node.ID)
			require.Equal(t, devID, dev[0], "Can't find device %s in result", dev[0])
		}
	}
}

// TestResourceDeviceInstanceFilter 验证按资源/设备 ID 过滤设备实例：
// 空条件、部分命中、全部命中、包含不存在 ID 等情况。
func TestResourceDeviceInstanceFilter(t *testing.T) {
	var expected string
	var cond map[string]sets.Set[string]
	var resp ResourceDeviceInstances
	devs := ResourceDeviceInstances{
		"foo": DeviceInstances{
			"dev-foo1": &pluginapi.Device{
				ID: "foo1",
			},
			"dev-foo2": &pluginapi.Device{
				ID: "foo2",
			},
			"dev-foo3": &pluginapi.Device{
				ID: "foo3",
			},
		},
		"bar": DeviceInstances{
			"dev-bar1": &pluginapi.Device{
				ID: "bar1",
			},
			"dev-bar2": &pluginapi.Device{
				ID: "bar2",
			},
			"dev-bar3": &pluginapi.Device{
				ID: "bar3",
			},
		},
		"baz": DeviceInstances{
			"dev-baz1": &pluginapi.Device{
				ID: "baz1",
			},
			"dev-baz2": &pluginapi.Device{
				ID: "baz2",
			},
			"dev-baz3": &pluginapi.Device{
				ID: "baz3",
			},
		},
	}

	// 空过滤条件返回空集合。
	resp = devs.Filter(map[string]sets.Set[string]{})
	expected = `{}`
	expectResourceDeviceInstances(t, resp, expected)

	cond = map[string]sets.Set[string]{
		"foo": sets.New[string]("dev-foo1", "dev-foo2"),
		"bar": sets.New[string]("dev-bar1"),
	}
	resp = devs.Filter(cond)
	expected = `{"bar":{"dev-bar1":{"ID":"bar1"}},"foo":{"dev-foo1":{"ID":"foo1"},"dev-foo2":{"ID":"foo2"}}}`
	expectResourceDeviceInstances(t, resp, expected)

	cond = map[string]sets.Set[string]{
		"foo": sets.New[string]("dev-foo1", "dev-foo2", "dev-foo3"),
		"bar": sets.New[string]("dev-bar1", "dev-bar2", "dev-bar3"),
		"baz": sets.New[string]("dev-baz1", "dev-baz2", "dev-baz3"),
	}
	resp = devs.Filter(cond)
	expected = `{"bar":{"dev-bar1":{"ID":"bar1"},"dev-bar2":{"ID":"bar2"},"dev-bar3":{"ID":"bar3"}},"baz":{"dev-baz1":{"ID":"baz1"},"dev-baz2":{"ID":"baz2"},"dev-baz3":{"ID":"baz3"}},"foo":{"dev-foo1":{"ID":"foo1"},"dev-foo2":{"ID":"foo2"},"dev-foo3":{"ID":"foo3"}}}`
	expectResourceDeviceInstances(t, resp, expected)

	cond = map[string]sets.Set[string]{
		"foo": sets.New[string]("dev-foo1", "dev-foo2", "dev-foo3", "dev-foo4"),
		"bar": sets.New[string]("dev-bar1", "dev-bar2", "dev-bar3", "dev-bar4"),
		"baz": sets.New[string]("dev-baz1", "dev-baz2", "dev-baz3", "dev-bar4"),
	}
	resp = devs.Filter(cond)
	expected = `{"bar":{"dev-bar1":{"ID":"bar1"},"dev-bar2":{"ID":"bar2"},"dev-bar3":{"ID":"bar3"}},"baz":{"dev-baz1":{"ID":"baz1"},"dev-baz2":{"ID":"baz2"},"dev-baz3":{"ID":"baz3"}},"foo":{"dev-foo1":{"ID":"foo1"},"dev-foo2":{"ID":"foo2"},"dev-foo3":{"ID":"foo3"}}}`
	expectResourceDeviceInstances(t, resp, expected)

	cond = map[string]sets.Set[string]{
		"foo": sets.New[string]("dev-foo1", "dev-foo4", "dev-foo7"),
		"bar": sets.New[string]("dev-bar1", "dev-bar4", "dev-bar7"),
		"baz": sets.New[string]("dev-baz1", "dev-baz4", "dev-baz7"),
	}
	resp = devs.Filter(cond)
	expected = `{"bar":{"dev-bar1":{"ID":"bar1"}},"baz":{"dev-baz1":{"ID":"baz1"}},"foo":{"dev-foo1":{"ID":"foo1"}}}`
	expectResourceDeviceInstances(t, resp, expected)

}

// expectResourceDeviceInstances 把结果序列化为 JSON 并与期望字符串比较。
func expectResourceDeviceInstances(t *testing.T, resp ResourceDeviceInstances, expected string) {
	// encoding/json 保证 map 键按字典序输出（见 Marshal 文档），
	// 因此这里的字符串比较是稳定的，不会偶发失败。
	data, err := json.Marshal(resp)
	if err != nil {
		t.Fatalf("unexpected JSON marshalling error: %v", err)
	}
	got := string(data)
	if got != expected {
		t.Errorf("expected %q got %q", expected, got)
	}
}

// TestDeviceRunContainerOptions 验证把各资源的分配响应汇总为容器运行选项，
// 包括 CDI 设备去重与多资源合并。
func TestDeviceRunContainerOptions(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	const (
		podUID        = "pod"
		containerName = "container"
		resource1     = "example1.com/resource1"
		resource2     = "example2.com/resource2"
	)
	testCases := []struct {
		description          string
		responsesPerResource map[string]*pluginapi.ContainerAllocateResponse
		expected             *DeviceRunContainerOptions
	}{
		{
			description: "empty response",
			responsesPerResource: map[string]*pluginapi.ContainerAllocateResponse{
				resource1: newContainerAllocateResponse(),
			},
			expected: &DeviceRunContainerOptions{},
		},
		{
			description: "cdi devices are handled",
			responsesPerResource: map[string]*pluginapi.ContainerAllocateResponse{
				resource1: newContainerAllocateResponse(
					withCDIDevices("vendor1.com/class1=device1", "vendor2.com/class2=device2"),
				),
			},
			expected: &DeviceRunContainerOptions{
				CDIDevices: []kubecontainer.CDIDevice{
					{Name: "vendor1.com/class1=device1"},
					{Name: "vendor2.com/class2=device2"},
				},
			},
		},
		{
			description: "cdi devices from multiple resources are handled",
			responsesPerResource: map[string]*pluginapi.ContainerAllocateResponse{
				resource1: newContainerAllocateResponse(
					withCDIDevices("vendor1.com/class1=device1", "vendor2.com/class2=device2"),
				),
				resource2: newContainerAllocateResponse(
					withCDIDevices("vendor3.com/class3=device3", "vendor4.com/class4=device4"),
				),
			},
			expected: &DeviceRunContainerOptions{
				CDIDevices: []kubecontainer.CDIDevice{
					{Name: "vendor1.com/class1=device1"},
					{Name: "vendor2.com/class2=device2"},
					{Name: "vendor3.com/class3=device3"},
					{Name: "vendor4.com/class4=device4"},
				},
			},
		},
		{
			description: "duplicate cdi devices are skipped",
			responsesPerResource: map[string]*pluginapi.ContainerAllocateResponse{
				resource1: newContainerAllocateResponse(
					withCDIDevices("vendor1.com/class1=device1", "vendor2.com/class2=device2"),
				),
				resource2: newContainerAllocateResponse(
					withCDIDevices("vendor2.com/class2=device2", "vendor3.com/class3=device3"),
				),
			},
			expected: &DeviceRunContainerOptions{
				CDIDevices: []kubecontainer.CDIDevice{
					{Name: "vendor1.com/class1=device1"},
					{Name: "vendor2.com/class2=device2"},
					{Name: "vendor3.com/class3=device3"},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			as := assert.New(t)

			podDevices := newPodDevices()
			for resourceName, response := range tc.responsesPerResource {
				podDevices.insert("pod", "container", resourceName,
					nil,
					response,
				)
			}
			// 从各资源的分配响应汇总注解、CDI 设备、设备、环境变量和挂载。
			opts := podDevices.deviceRunContainerOptions(logger, podUID, containerName)

			// 选项顺序取决于 map 遍历顺序，因此成员切片用 ElementsMatch 而非 Equal。
			as.ElementsMatch(tc.expected.Annotations, opts.Annotations)
			as.ElementsMatch(tc.expected.CDIDevices, opts.CDIDevices)
			as.ElementsMatch(tc.expected.Devices, opts.Devices)
			as.ElementsMatch(tc.expected.Envs, opts.Envs)
			as.ElementsMatch(tc.expected.Mounts, opts.Mounts)
		})
	}
}

// TestGetPodAndContainerForDevice 验证按资源名和设备 ID 反查所属 Pod/容器。
func TestGetPodAndContainerForDevice(t *testing.T) {
	podDevices := newPodDevices()
	resourceName1 := "domain1.com/resource1"
	podID := "pod1"
	contID := "con1"
	devices := checkpoint.DevicesPerNUMA{0: []string{"dev1"}, 1: []string{"dev1"}}

	podDevices.insert(podID, contID, resourceName1,
		devices,
		newContainerAllocateResponse(
			withDevices(map[string]string{"/dev/r1dev1": "/dev/r1dev1", "/dev/r1dev2": "/dev/r1dev2"}),
			withMounts(map[string]string{"/home/r1lib1": "/usr/r1lib1"}),
		),
	)

	// dev2 是未分配的新设备，应查不到 Pod。
	podUID, _ := podDevices.getPodAndContainerForDevice(resourceName1, "dev2")
	assert.Equal(t, "", podUID)

	// dev1 已分配给 pod1，应能反查到。
	podUID, _ = podDevices.getPodAndContainerForDevice(resourceName1, "dev1")
	assert.Equal(t, "pod1", podUID)

	// 设备 ID 只在资源内唯一：另一个资源下同名 ID 的设备属于不同 Pod，不能混淆。
	resourceName2 := "domain2.com/resource2"
	podDevices.insert("pod2", contID, resourceName2,
		devices,
		newContainerAllocateResponse(
			withDevices(map[string]string{"/dev/r2dev1": "/dev/r2dev1"}),
		),
	)

	podUID, _ = podDevices.getPodAndContainerForDevice(resourceName2, "dev1")
	assert.Equal(t, "pod2", podUID)

	podUID, _ = podDevices.getPodAndContainerForDevice(resourceName1, "dev1")
	assert.Equal(t, "pod1", podUID)

	podUID, _ = podDevices.getPodAndContainerForDevice("domain3.com/unknown", "dev1")
	assert.Empty(t, podUID)
}
