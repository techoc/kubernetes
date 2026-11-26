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

package v1beta1

// 本文件定义注册协议中插件类型标识：kubelet 用它区分 API 版本与注册流程，
// 字符串值属于线上协议契约，不能随意改名。
const (
	// 标识 CSI 存储插件，值为历史协议约定，改动会破坏与存量插件的握手。
	CSIPlugin = "CSIPlugin"
	// 标识设备插件（GPU、SR-IOV 等），与 CSI 共用同一套注册服务但类型不同。
	DevicePlugin = "DevicePlugin"
)
