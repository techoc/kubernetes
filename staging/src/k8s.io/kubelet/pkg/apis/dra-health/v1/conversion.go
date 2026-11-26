/*
Copyright The Kubernetes Authors.

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

// 本文件提供 v1 与 v1alpha1 DRAResourceHealth gRPC API 之间的
// 转换及薄包装层，写法参照主 DRA gRPC API
// 参照 k8s.io/kubelet/pkg/apis/dra/v1beta1/conversion.go 的写法。
//
// v1 与 v1alpha1 的 proto 完全一致，这些转换只是机械的字段拷贝。
// 它们让驱动只需实现最新的
// v1 健康服务，同时由 kubelet 辅助包继续为旧版 kubelet 提供
// v1alpha1 API；反过来也让 kubelet 能消费那些在 v1 出现前就发布的、
// 只带 v1alpha1 健康服务的驱动的健康信息。
// 两个方向的兼容代码都会在 1.40 前后移除：
// 客户端回退保留三个版本过渡期，
// 届时也不再支持没有 v1 的老 kubelet。

// 本文件只在版本过渡期存在：v1 与 v1alpha1 的 proto 完全一致，
// 因此转换只是逐字段拷贝；真正重要的是两侧的 server/client 包装，
// 让驱动只实现新版本、kubelet 只处理新版本类型，同时仍能与旧版本对等端通信。
package v1

import (
	"context"

	grpc "google.golang.org/grpc"

	v1alpha1 "k8s.io/kubelet/pkg/apis/dra-health/v1alpha1"
)

// --- 消息转换 ---

// deviceIdentifierToV1Alpha1 把 v1 设备标识转换为 v1alpha1。
// 两版字段一一对应，nil 输入原样返回 nil，避免把“未指定”变成空对象。
func deviceIdentifierToV1Alpha1(in *DeviceIdentifier) *v1alpha1.DeviceIdentifier {
	if in == nil {
		return nil
	}
	return &v1alpha1.DeviceIdentifier{
		PoolName:   in.PoolName,
		DeviceName: in.DeviceName,
	}
}

// deviceIdentifierFromV1Alpha1 是 deviceIdentifierToV1Alpha1 的反向转换，
// 同样保持 nil 语义，供 client 包装处理旧驱动的响应。
func deviceIdentifierFromV1Alpha1(in *v1alpha1.DeviceIdentifier) *DeviceIdentifier {
	if in == nil {
		return nil
	}
	return &DeviceIdentifier{
		PoolName:   in.PoolName,
		DeviceName: in.DeviceName,
	}
}

// deviceHealthToV1Alpha1 把单个设备的健康状态转换为旧版。
// HealthStatus 虽是不同包中的类型，但底层枚举值一致，可直接强转。
func deviceHealthToV1Alpha1(in *DeviceHealth) *v1alpha1.DeviceHealth {
	if in == nil {
		return nil
	}
	return &v1alpha1.DeviceHealth{
		Device:                    deviceIdentifierToV1Alpha1(in.Device),
		Health:                    v1alpha1.HealthStatus(in.Health),
		LastUpdatedTime:           in.LastUpdatedTime,
		HealthCheckTimeoutSeconds: in.HealthCheckTimeoutSeconds,
		Message:                   in.Message,
	}
}

// deviceHealthFromV1Alpha1 是 deviceHealthToV1Alpha1 的反向转换，
// 用于把旧驱动的响应喂给只认识 v1 类型的 kubelet 代码。
func deviceHealthFromV1Alpha1(in *v1alpha1.DeviceHealth) *DeviceHealth {
	if in == nil {
		return nil
	}
	return &DeviceHealth{
		Device:                    deviceIdentifierFromV1Alpha1(in.Device),
		Health:                    HealthStatus(in.Health),
		LastUpdatedTime:           in.LastUpdatedTime,
		HealthCheckTimeoutSeconds: in.HealthCheckTimeoutSeconds,
		Message:                   in.Message,
	}
}

// NodeWatchResourcesResponseToV1Alpha1 把 v1 响应转为 v1alpha1。
//
// NodeWatchResourcesResponseToV1Alpha1 逐元素转换设备列表；
// 保留 nil 切片与空切片的区分，因为二者对“无设备”和“未上报”含义不同。
func NodeWatchResourcesResponseToV1Alpha1(in *NodeWatchResourcesResponse) *v1alpha1.NodeWatchResourcesResponse {
	if in == nil {
		return nil
	}
	out := &v1alpha1.NodeWatchResourcesResponse{}
	if in.Devices != nil {
		out.Devices = make([]*v1alpha1.DeviceHealth, len(in.Devices))
		for i, d := range in.Devices {
			out.Devices[i] = deviceHealthToV1Alpha1(d)
		}
	}
	return out
}

// NodeWatchResourcesResponseFromV1Alpha1 把 v1alpha1 响应转为 v1。
//
// NodeWatchResourcesResponseFromV1Alpha1 是响应转换的反向路径，
// 与 server 包装配合让 kubelet 的客户端不必感知驱动实际使用的版本。
func NodeWatchResourcesResponseFromV1Alpha1(in *v1alpha1.NodeWatchResourcesResponse) *NodeWatchResourcesResponse {
	if in == nil {
		return nil
	}
	out := &NodeWatchResourcesResponse{}
	if in.Devices != nil {
		out.Devices = make([]*DeviceHealth, len(in.Devices))
		for i, d := range in.Devices {
			out.Devices[i] = deviceHealthFromV1Alpha1(d)
		}
	}
	return out
}

// --- 服务端包装：用 v1 实现提供 v1alpha1 API ---

// V1ServerWrapper 包装 v1 的 [DRAResourceHealthServer]，
// 接口：包装一个 v1 的 [DRAResourceHealthServer]。kubelet 辅助包
// 注册它后，只实现 v1 健康服务的驱动
// 也能提供旧版 v1alpha1 API。
//
// V1ServerWrapper 是适配层：老 kubelet 连上来时仍看到 v1alpha1 接口，
// 而驱动侧只需实现 v1，过渡期结束后可整体删除。
type V1ServerWrapper struct {
	// 嵌入 Unsafe 接口使未来新增 RPC 时本类型仍满足接口约束，
	// 未实现的方法走 protobuf 默认行为，而不是编译期失败。
	v1alpha1.UnsafeDRAResourceHealthServer
	// Server 是 DRA 驱动提供的 v1 健康服务。
	Server DRAResourceHealthServer
}

// 编译期断言：包装器必须满足 v1alpha1 服务端接口，避免接口演进时悄然失效。
var _ v1alpha1.DRAResourceHealthServer = V1ServerWrapper{}

// NodeWatchResources 忽略旧版请求内容（两版请求均为空消息），
// 把 v1 服务的响应经流适配器转换后写回旧版连接。
func (w V1ServerWrapper) NodeWatchResources(_ *v1alpha1.NodeWatchResourcesRequest, stream v1alpha1.DRAResourceHealth_NodeWatchResourcesServer) error {
	return w.Server.NodeWatchResources(&NodeWatchResourcesRequest{}, &v1ToV1alpha1ServerStream{ServerStream: stream})
}

// v1ToV1alpha1ServerStream 适配 v1alpha1 服务端流，
// 使 v1 服务端实现发送的 v1 响应在写入网络前
// 转换为 v1alpha1。
//
// v1ToV1alpha1ServerStream 用组合而非重新实现整个 gRPC 流接口：
// 只覆盖 Send 做类型转换，其余方法委托给底层流。
type v1ToV1alpha1ServerStream struct {
	grpc.ServerStream
}

// 编译期断言：适配器必须满足 v1 服务端流接口。
var _ DRAResourceHealth_NodeWatchResourcesServer = &v1ToV1alpha1ServerStream{}

// Send 在写 wire 前把 v1 响应转换为 v1alpha1，
// 这样驱动代码无需知道自己正被老 kubelet 调用。
func (s *v1ToV1alpha1ServerStream) Send(resp *NodeWatchResourcesResponse) error {
	return s.ServerStream.SendMsg(NodeWatchResourcesResponseToV1Alpha1(resp))
}

// --- 客户端包装：通过 v1 客户端 API 消费 v1alpha1 插件 ---

// V1Alpha1ClientWrapper 包装 [v1alpha1.DRAResourceHealthClient]，
// 接口：包装 [v1alpha1.DRAResourceHealthClient]。kubelet 用它
// 消费只实现 v1alpha1 健康服务的驱动的上报，
// 而 kubelet 其余部分只接触 v1 类型。
//
// TODO(harche): 1.40 中随 kubelet 的 v1alpha1 客户端支持
// 一并移除；这是为 v1 出现前就发布 v1alpha1 健康服务的驱动
// 保留的三个版本（1.37~1.39）过渡期。
//
// V1Alpha1ClientWrapper 把旧版客户端包装成 v1 客户端：
// 上层 kubelet 逻辑只写 v1 代码，版本差异被限制在这个适配层内。
type V1Alpha1ClientWrapper struct {
	// Client 连接只实现 v1alpha1 健康服务的插件。
	Client v1alpha1.DRAResourceHealthClient
}

// 编译期断言：包装器必须满足 v1 客户端接口。
var _ DRAResourceHealthClient = V1Alpha1ClientWrapper{}

// NodeWatchResources 用空的旧版请求建立流（两版请求都无字段），
// 再把返回的流包装成 v1 视图；错误原样透传，避免丢失 gRPC 状态信息。
func (w V1Alpha1ClientWrapper) NodeWatchResources(ctx context.Context, _ *NodeWatchResourcesRequest, opts ...grpc.CallOption) (DRAResourceHealth_NodeWatchResourcesClient, error) {
	stream, err := w.Client.NodeWatchResources(ctx, &v1alpha1.NodeWatchResourcesRequest{}, opts...)
	if err != nil {
		return nil, err
	}
	return &v1alpha1ToV1ClientStream{ClientStream: stream}, nil
}

// v1alpha1ToV1ClientStream 适配 v1alpha1 客户端流，
// 使 kubelet 能收到由插件发来的 v1alpha1 消息转换成的 v1 响应。
// 由插件发来的 v1alpha1 消息。
//
// v1alpha1ToV1ClientStream 只覆盖 Recv 做消息转换，
// 其余 gRPC 流操作沿用嵌入的 ClientStream。
type v1alpha1ToV1ClientStream struct {
	grpc.ClientStream
}

// 编译期断言：适配器必须满足 v1 客户端流接口。
var _ DRAResourceHealth_NodeWatchResourcesClient = &v1alpha1ToV1ClientStream{}

// Recv 读取一条旧版消息并转换为 v1 响应；
// 流结束或出错时直接返回原错误，让调用方正常处理 EOF。
func (s *v1alpha1ToV1ClientStream) Recv() (*NodeWatchResourcesResponse, error) {
	var msg v1alpha1.NodeWatchResourcesResponse
	if err := s.ClientStream.RecvMsg(&msg); err != nil {
		return nil, err
	}
	return NodeWatchResourcesResponseFromV1Alpha1(&msg), nil
}
