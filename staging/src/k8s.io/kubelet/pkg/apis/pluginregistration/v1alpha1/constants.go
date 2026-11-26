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

package v1alpha1

// 本文件定义插件注册协议 v1alpha1 版本中的插件类型标识。
// 与 v1 版本相比，这里只有 CSI 和 Device Plugin 两种，因为 DRA 是后来才加入的。
const (
	// CSIPlugin 标识 CSI（Container Storage Interface）存储插件。
	CSIPlugin = "CSIPlugin"
	// DevicePlugin 标识传统的 Device Plugin（如 GPU、FPGA 设备插件）。
	DevicePlugin = "DevicePlugin"
)
