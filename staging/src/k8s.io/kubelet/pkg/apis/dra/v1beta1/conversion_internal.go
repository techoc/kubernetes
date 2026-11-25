/*
Copyright 2025 The Kubernetes Authors.

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

// 本文件集中放置需手写的 DRA v1beta1 与 v1 转换函数。
// 两版消息由 protobuf 生成，内部带 state/unknownFields 等未导出字段，
// 自动生成转换无法处理，故在此手写包装，把实际字段拷贝委托给生成函数。
package v1beta1

import (
	conversion "k8s.io/apimachinery/pkg/conversion"
	v1 "k8s.io/kubelet/pkg/apis/dra/v1"
)

// Convert_v1beta1_Claim_To_v1_Claim 是手工创建的转换函数，因为该对象包含无法自动转换的私有字段。
// 它把 v1beta1 Claim 转成 v1 Claim；
// 只做字段拷贝，protobuf 内部状态字段交给生成代码处理。
func Convert_v1beta1_Claim_To_v1_Claim(in *Claim, out *v1.Claim, s conversion.Scope) error {
	return autoConvert_v1beta1_Claim_To_v1_Claim(in, out, s)
}

// Convert_v1_Claim_To_v1beta1_Claim 是手工创建的转换函数，因为该对象包含无法自动转换的私有字段。
// Convert_v1_Claim_To_v1beta1_Claim 是上一函数的反向路径，
// 用同一套生成代码保证两个方向的字段映射对称。
func Convert_v1_Claim_To_v1beta1_Claim(in *v1.Claim, out *Claim, s conversion.Scope) error {
	return autoConvert_v1_Claim_To_v1beta1_Claim(in, out, s)
}

// Convert_v1beta1_Device_To_v1_Device 是手工创建的转换函数，因为该对象包含无法自动转换的私有字段。
// 它把 v1beta1 Device 转成 v1 Device；
// 设备标识与属性属值语义，可直接逐字段拷贝。
func Convert_v1beta1_Device_To_v1_Device(in *Device, out *v1.Device, s conversion.Scope) error {
	return autoConvert_v1beta1_Device_To_v1_Device(in, out, s)
}

// Convert_v1_Device_To_v1beta1_Device 是手工创建的转换函数，因为该对象包含无法自动转换的私有字段。
// Convert_v1_Device_To_v1beta1_Device 是 Device 转换的反向路径，
// 保持与前向转换一致，避免版本往返后字段丢失。
func Convert_v1_Device_To_v1beta1_Device(in *v1.Device, out *Device, s conversion.Scope) error {
	return autoConvert_v1_Device_To_v1beta1_Device(in, out, s)
}

// Convert_v1beta1_NodePrepareResourceResponse_To_v1_NodePrepareResourceResponse 是手工创建的转换函数，因为该对象包含无法自动转换的私有字段。
// 它转换 Prepare 的单资源响应，
// 保留生成函数对设备列表的深拷贝语义。
func Convert_v1beta1_NodePrepareResourceResponse_To_v1_NodePrepareResourceResponse(in *NodePrepareResourceResponse, out *v1.NodePrepareResourceResponse, s conversion.Scope) error {
	return autoConvert_v1beta1_NodePrepareResourceResponse_To_v1_NodePrepareResourceResponse(in, out, s)
}

// Convert_v1_NodePrepareResourceResponse_To_v1beta1_NodePrepareResourceResponse 是手工创建的转换函数，因为该对象包含无法自动转换的私有字段。
// Convert_v1_NodePrepareResourceResponse_To_v1beta1_NodePrepareResourceResponse 是 Prepare 单资源响应的反向转换，
// 供 kubelet 用老版本协议回复驱动时使用。
func Convert_v1_NodePrepareResourceResponse_To_v1beta1_NodePrepareResourceResponse(in *v1.NodePrepareResourceResponse, out *NodePrepareResourceResponse, s conversion.Scope) error {
	return autoConvert_v1_NodePrepareResourceResponse_To_v1beta1_NodePrepareResourceResponse(in, out, s)
}

// Convert_v1beta1_NodePrepareResourcesRequest_To_v1_NodePrepareResourcesRequest 是手工创建的转换函数，因为该对象包含无法自动转换的私有字段。
// 它转换 Prepare 请求；
// 请求中的 Claim 列表由生成函数递归处理。
func Convert_v1beta1_NodePrepareResourcesRequest_To_v1_NodePrepareResourcesRequest(in *NodePrepareResourcesRequest, out *v1.NodePrepareResourcesRequest, s conversion.Scope) error {
	return autoConvert_v1beta1_NodePrepareResourcesRequest_To_v1_NodePrepareResourcesRequest(in, out, s)
}

// Convert_v1_NodePrepareResourcesRequest_To_v1beta1_NodePrepareResourcesRequest 是手工创建的转换函数，因为该对象包含无法自动转换的私有字段。
// Convert_v1_NodePrepareResourcesRequest_To_v1beta1_NodePrepareResourcesRequest 是 Prepare 请求的反向转换，
// 用于把 v1 请求降级为旧版协议发给驱动。
func Convert_v1_NodePrepareResourcesRequest_To_v1beta1_NodePrepareResourcesRequest(in *v1.NodePrepareResourcesRequest, out *NodePrepareResourcesRequest, s conversion.Scope) error {
	return autoConvert_v1_NodePrepareResourcesRequest_To_v1beta1_NodePrepareResourcesRequest(in, out, s)
}

// Convert_v1beta1_NodePrepareResourcesResponse_To_v1_NodePrepareResourcesResponse 是手工创建的转换函数，因为该对象包含无法自动转换的私有字段。
// 它转换批量 Prepare 响应；
// 设备结果列表按元素逐个深拷贝，避免两版本共享可变对象。
func Convert_v1beta1_NodePrepareResourcesResponse_To_v1_NodePrepareResourcesResponse(in *NodePrepareResourcesResponse, out *v1.NodePrepareResourcesResponse, s conversion.Scope) error {
	return autoConvert_v1beta1_NodePrepareResourcesResponse_To_v1_NodePrepareResourcesResponse(in, out, s)
}

// Convert_v1_NodePrepareResourcesResponse_To_v1beta1_NodePrepareResourcesResponse 是手工创建的转换函数，因为该对象包含无法自动转换的私有字段。
// Convert_v1_NodePrepareResourcesResponse_To_v1beta1_NodePrepareResourcesResponse 是批量 Prepare 响应的反向转换，
// 保证旧驱动返回的结果能被 kubelet 正常消费。
func Convert_v1_NodePrepareResourcesResponse_To_v1beta1_NodePrepareResourcesResponse(in *v1.NodePrepareResourcesResponse, out *NodePrepareResourcesResponse, s conversion.Scope) error {
	return autoConvert_v1_NodePrepareResourcesResponse_To_v1beta1_NodePrepareResourcesResponse(in, out, s)
}

// Convert_v1beta1_NodeUnprepareResourceResponse_To_v1_NodeUnprepareResourceResponse 是手工创建的转换函数，因为该对象包含无法自动转换的私有字段。
// 它转换 Unprepare 的单资源响应；
// 空响应也是合法结果，表示资源已清理完毕。
func Convert_v1beta1_NodeUnprepareResourceResponse_To_v1_NodeUnprepareResourceResponse(in *NodeUnprepareResourceResponse, out *v1.NodeUnprepareResourceResponse, s conversion.Scope) error {
	return autoConvert_v1beta1_NodeUnprepareResourceResponse_To_v1_NodeUnprepareResourceResponse(in, out, s)
}

// Convert_v1_NodeUnprepareResourceResponse_To_v1beta1_NodeUnprepareResourceResponse 是手工创建的转换函数，因为该对象包含无法自动转换的私有字段。
// Convert_v1_NodeUnprepareResourceResponse_To_v1beta1_NodeUnprepareResourceResponse 是 Unprepare 单资源响应的反向转换。
func Convert_v1_NodeUnprepareResourceResponse_To_v1beta1_NodeUnprepareResourceResponse(in *v1.NodeUnprepareResourceResponse, out *NodeUnprepareResourceResponse, s conversion.Scope) error {
	return autoConvert_v1_NodeUnprepareResourceResponse_To_v1beta1_NodeUnprepareResourceResponse(in, out, s)
}

// Convert_v1beta1_NodeUnprepareResourcesRequest_To_v1_NodeUnprepareResourcesRequest 是手工创建的转换函数，因为该对象包含无法自动转换的私有字段。
// 它转换 Unprepare 请求；
// 请求只携带 Claim 坐标，转换不涉及运行时状态。
func Convert_v1beta1_NodeUnprepareResourcesRequest_To_v1_NodeUnprepareResourcesRequest(in *NodeUnprepareResourcesRequest, out *v1.NodeUnprepareResourcesRequest, s conversion.Scope) error {
	return autoConvert_v1beta1_NodeUnprepareResourcesRequest_To_v1_NodeUnprepareResourcesRequest(in, out, s)
}

// Convert_v1_NodeUnprepareResourcesRequest_To_v1beta1_NodeUnprepareResourcesRequest 是手工创建的转换函数，因为该对象包含无法自动转换的私有字段。
// Convert_v1_NodeUnprepareResourcesRequest_To_v1beta1_NodeUnprepareResourcesRequest 是 Unprepare 请求的反向转换，
// 使 kubelet 能向只支持旧版的驱动发起清理调用。
func Convert_v1_NodeUnprepareResourcesRequest_To_v1beta1_NodeUnprepareResourcesRequest(in *v1.NodeUnprepareResourcesRequest, out *NodeUnprepareResourcesRequest, s conversion.Scope) error {
	return autoConvert_v1_NodeUnprepareResourcesRequest_To_v1beta1_NodeUnprepareResourcesRequest(in, out, s)
}

// Convert_v1beta1_NodeUnprepareResourcesResponse_To_v1_NodeUnprepareResourcesResponse 是手工创建的转换函数，因为该对象包含无法自动转换的私有字段。
// 它转换批量 Unprepare 响应，
// 与单资源版本走同一套生成逻辑，避免两处字段映射出现偏差。
func Convert_v1beta1_NodeUnprepareResourcesResponse_To_v1_NodeUnprepareResourcesResponse(in *NodeUnprepareResourcesResponse, out *v1.NodeUnprepareResourcesResponse, s conversion.Scope) error {
	return autoConvert_v1beta1_NodeUnprepareResourcesResponse_To_v1_NodeUnprepareResourcesResponse(in, out, s)
}

// Convert_v1_NodeUnprepareResourcesResponse_To_v1beta1_NodeUnprepareResourcesResponse 是手工创建的转换函数，因为该对象包含无法自动转换的私有字段。
// Convert_v1_NodeUnprepareResourcesResponse_To_v1beta1_NodeUnprepareResourcesResponse 是批量 Unprepare 响应的反向转换，
// 完成本文件中所有消息类型的双向映射闭环。
func Convert_v1_NodeUnprepareResourcesResponse_To_v1beta1_NodeUnprepareResourcesResponse(in *v1.NodeUnprepareResourcesResponse, out *NodeUnprepareResourcesResponse, s conversion.Scope) error {
	return autoConvert_v1_NodeUnprepareResourcesResponse_To_v1beta1_NodeUnprepareResourcesResponse(in, out, s)
}
