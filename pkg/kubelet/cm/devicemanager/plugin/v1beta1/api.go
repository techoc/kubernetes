/*
Copyright 2022 The Kubernetes Authors.

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

package v1beta1

import (
	"context"

	"k8s.io/klog/v2"
	api "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

// RegistrationHandler 定义处理设备插件注册与插件目录清理的接口。
type RegistrationHandler interface {
	// CleanupPluginDirectory 清理插件目录中已失效的 socket 文件。
	CleanupPluginDirectory(klog.Logger, string) error
}

// ClientHandler 定义处理设备插件连接的接口。
type ClientHandler interface {
	// PluginConnected 在设备插件连接成功时回调。
	PluginConnected(context.Context, string, DevicePlugin) error
	// PluginDisconnected 在设备插件断开连接时回调。
	PluginDisconnected(klog.Logger, string, string)
	// PluginListAndWatchReceiver 接收设备插件 ListAndWatch 推送的资源更新。
	PluginListAndWatchReceiver(klog.Logger, string, *api.ListAndWatchResponse)
}

// TODO：评估是否需要这些错误定义。
const (
	// errFailedToDialDevicePlugin 表示无法通过注册的 socket 连接设备插件。
	errFailedToDialDevicePlugin = "failed to dial device plugin:"
	// errUnsupportedVersion 表示设备插件请求了 kubelet 注册表不支持的 API 版本。
	errUnsupportedVersion = "requested API version %q is not supported by kubelet. Supported version is %q"
	// errInvalidResourceName 表示设备插件注册时使用了非法的 ResourceName。
	errInvalidResourceName = "the ResourceName %q is invalid"
	// errBadSocket 表示注册使用的 socket 路径不是绝对路径。
	errBadSocket = "bad socketPath, must be an absolute path:"
)
