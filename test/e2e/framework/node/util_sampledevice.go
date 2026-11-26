/*
Copyright 2023 The Kubernetes Authors.

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

package node

import (
	v1 "k8s.io/api/core/v1"
)

const (
	// SampleDevicePluginDSYAML 是 sample device plugin 的 daemonset 模板路径。TODO: 把它做成 TestFramework 的一个特性来参数化。
	SampleDevicePluginDSYAML                    = "test/e2e/testing-manifests/sample-device-plugin/sample-device-plugin.yaml"
	SampleDevicePluginControlRegistrationDSYAML = "test/e2e/testing-manifests/sample-device-plugin/sample-device-plugin-control-registration.yaml"

	// SampleDevicePluginName 是 device plugin pod 的名称
	SampleDevicePluginName = "sample-device-plugin"

	// SampleDeviceResourceName 是 sample device plugin 提供的资源名称
	SampleDeviceResourceName = "example.com/resource"

	SampleDeviceEnvVarNamePluginSockDir = "PLUGIN_SOCK_DIR"

	// SampleDevsAmount 是 sample device plugin 提供的设备数量：
	// https://github.com/kubernetes/kubernetes/blob/111a2a0d2dfe13639724506f674bc4f342ccfbab/test/images/sample-device-plugin/sampledeviceplugin.go#L100-L101
	SampleDevsAmount int64 = 2
)

// CountSampleDeviceCapacity 返回节点容量中通告的 SampleDeviceResourceName 设备数量
func CountSampleDeviceCapacity(node *v1.Node) int64 {
	val, ok := node.Status.Capacity[v1.ResourceName(SampleDeviceResourceName)]
	if !ok {
		return 0
	}
	return val.Value()
}

// CountSampleDeviceAllocatable 返回节点可分配量中通告的 SampleDeviceResourceName 设备数量
func CountSampleDeviceAllocatable(node *v1.Node) int64 {
	val, ok := node.Status.Allocatable[v1.ResourceName(SampleDeviceResourceName)]
	if !ok {
		return 0
	}
	return val.Value()
}
