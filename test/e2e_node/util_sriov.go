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

package e2enode

import (
	"k8s.io/kubernetes/test/e2e/framework"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
)

const (
	// SRIOVDevicePluginCMYAML 是用于配置 SR-IOV 设备插件的 ConfigMap 路径。
	SRIOVDevicePluginCMYAML = "test/e2e_node/testing-manifests/sriovdp-cm.yaml"
	// SRIOVDevicePluginDSYAML 是 SR-IOV 设备插件的 DaemonSet 模板路径。// TODO: 将其作为 TestFramework 的特性参数化。
	SRIOVDevicePluginDSYAML = "test/e2e_node/testing-manifests/sriovdp-ds.yaml"
	// SRIOVDevicePluginSAYAML 是 SR-IOV 设备插件运行所需 ServiceAccount 的路径。
	SRIOVDevicePluginSAYAML = "test/e2e_node/testing-manifests/sriovdp-sa.yaml"
	// SRIOVDevicePluginName 是设备插件 Pod 的名称
	SRIOVDevicePluginName = "sriov-device-plugin"
)

func requireSRIOVDevices() {
	sriovdevCount, err := countSRIOVDevices()
	framework.ExpectNoError(err)

	if sriovdevCount > 0 {
		return // all good
	}

	msg := "this test is meant to run on a system with at least one configured VF from SRIOV device"
	if framework.TestContext.RequireDevices {
		framework.Fail(msg)
	} else {
		e2eskipper.Skip(msg)
	}
}
