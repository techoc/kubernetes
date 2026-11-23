/*
Copyright 2017 The Kubernetes Authors.

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

// 本包保留设备插件 API 的旧版常量，供老插件与 kubelet 做版本协商时使用。
// 注意 Version 的字符串仍是 v1alpha2，这是历史遗留值，不能按包名“纠正”。
package v1alpha

// Healthy/Unhealthy 是插件上报设备状态的固定字符串，
// 不需要类型区分，因为两版协议都直接按字符串比较。
const (
	// Healthy means that the device is healthy
	Healthy = "Healthy"
	// Unhealthy means that the device is unhealthy
	Unhealthy = "Unhealthy"

	// Version is the current version of the API supported by kubelet
	//
	// Version 的字符串值必须与历史协议一致（v1alpha2），
	// 改动它会导致老插件在握手阶段被拒。
	Version = "v1alpha2"
	// DevicePluginPath is the folder the Device Plugin is expecting sockets to be on
	// Only privileged pods have access to this path
	// Note: Placeholder until we find a "standard path"
	//
	// DevicePluginPath 与 v1beta1 相同：两个版本的插件共用同一个 socket 目录，
	// 靠文件名而不是目录区分协议版本。
	DevicePluginPath = "/var/lib/kubelet/device-plugins/"
	// KubeletSocket is the path of the Kubelet registry socket
	KubeletSocket = DevicePluginPath + "kubelet.sock"
)
