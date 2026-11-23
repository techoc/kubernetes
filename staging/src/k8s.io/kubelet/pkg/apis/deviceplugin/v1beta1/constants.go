/*
Copyright 2018 The Kubernetes Authors.

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

// 本包定义设备插件与 kubelet 通信的版本标识、状态字符串与 socket 路径。
// 这些值是双方握手的协议常量：路径写错会导致插件注册失败，状态字符串写错会导致设备被误判为不可用。
package v1beta1

// Healthy/Unhealthy 是插件上报设备状态的固定取值，
// 不用 bool 是为了给将来扩展第三种状态留出空间。
const (
	// Healthy means that the device is healthy
	Healthy = "Healthy"
	// Unhealthy means that the device is unhealthy
	Unhealthy = "Unhealthy"

	// Version means current version of the API supported by kubelet
	//
	// Version 用于插件注册时的版本协商；必须与 SupportedVersions 保持一致，
	// 否则插件会认为自己支持的版本与 kubelet 实际接受的版本不符。
	Version = "v1beta1"
	// DevicePluginPath is the folder the Device Plugin is expecting sockets to be on
	// Only privileged pods have access to this path
	// Note: Placeholder until we find a "standard path"
	//
	// DevicePluginPath 是插件放 socket 的约定目录；只有特权 Pod 能访问，
	// 因此这里同时充当一条隐式的安全边界。
	DevicePluginPath = "/var/lib/kubelet/device-plugins/"
	// KubeletSocket is the path of the Kubelet registry socket
	KubeletSocket = DevicePluginPath + "kubelet.sock"

	// DevicePluginPathWindows Avoid failed to run Kubelet: bad socketPath,
	// must be an absolute path: /var/lib/kubelet/device-plugins/kubelet.sock
	// https://github.com/kubernetes/kubernetes/issues/93262
	// https://github.com/kubernetes/kubernetes/pull/93285#discussion_r458140701
	//
	// 反斜杠开头的 Windows 路径是为了通过 gRPC 的绝对路径校验；
	// 去掉第一个反斜杠时会被判为相对路径，kubelet 直接启动失败（见上面 issue）。
	DevicePluginPathWindows = "\\var\\lib\\kubelet\\device-plugins\\"
	// KubeletSocketWindows is the path of the Kubelet registry socket on windows
	KubeletSocketWindows = DevicePluginPathWindows + "kubelet.sock"

	// KubeletPreStartContainerRPCTimeoutInSecs is the timeout duration in secs for PreStartContainer RPC
	// Timeout duration in secs for PreStartContainer RPC
	//
	// PreStartContainer 会调用外部插件，超时是为了避免单个慢插件阻塞容器启动流程。
	KubeletPreStartContainerRPCTimeoutInSecs = 30
)

// SupportedVersions provides a list of supported version
//
// SupportedVersions 是版本协商的权威列表；注册握手时插件从中选一个版本，
// 新增版本必须同时更新它与 Version，避免两者说法不一致。
var SupportedVersions = [...]string{"v1beta1"}
