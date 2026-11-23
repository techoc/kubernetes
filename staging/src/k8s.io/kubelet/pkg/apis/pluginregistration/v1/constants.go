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

package v1

// 本文件定义插件注册协议中用于标识插件类型的常量。
// 这些字符串会作为插件注册时上报的 name 字段值，kubelet 据此区分插件类别并走对应的处理逻辑。
const (
	// CSIPlugin identifier for registered CSI plugins
	// CSIPlugin 标识 CSI（Container Storage Interface）存储插件。
	CSIPlugin = "CSIPlugin"
	// DevicePlugin identifier for registered device plugins
	// DevicePlugin 标识传统的 Device Plugin（如 GPU、FPGA 设备插件）。
	DevicePlugin = "DevicePlugin"
	// DRAPlugin identifier for registered Dynamic Resourc Allocation plugins
	// DRAPlugin 标识 DRA（Dynamic Resource Allocation）插件，是设备资源分配的新一代机制。
	DRAPlugin = "DRAPlugin"
)
