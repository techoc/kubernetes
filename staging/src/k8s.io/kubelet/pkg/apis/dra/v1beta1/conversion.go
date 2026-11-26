/*
Copyright 2024 The Kubernetes Authors.

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

// 本文件提供四个转换包装：让只实现 v1 或只实现 v1beta1 的 DRA 插件，
// 都能被另一版本的 kubelet 通过 gRPC 调用。转换失败一律当作内部错误处理，
// 因为请求/响应结构在两版本间应一一对应，失败意味着代码有 bug 而不是用户输入有问题。

package v1beta1

import (
	context "context"
	fmt "fmt"

	grpc "google.golang.org/grpc"
	"k8s.io/apimachinery/pkg/runtime"

	v1 "k8s.io/kubelet/pkg/apis/dra/v1"
)

var (
	localSchemeBuilder runtime.SchemeBuilder
	AddToScheme        = localSchemeBuilder.AddToScheme
)

// AddToScheme 暴露给调用方注册本版本的类型，底层 builder 保持私有以便集中管理。

// V1ServerWrapper 包装 [v1.DRAPluginServer] 以实现 [NodeServer] 接口。
//
// 把 v1beta1 服务端“伪装”成 v1 服务端：kubelet 说 v1beta1，插件说 v1，
// 包装层在两次调用之间翻译请求和响应。嵌入 UnsafeDRAPluginServer 是为了补齐
// mustEmbedUnimplemented 方法，避免 gRPC 生成代码拒绝注册。
type V1ServerWrapper struct {
	UnsafeDRAPluginServer
	v1.DRAPluginServer
}

// 编译期断言：包装器必须满足 v1beta1 服务端接口，接口一旦变动此处立即报错。
var _ DRAPluginServer = V1ServerWrapper{}

// NodePrepareResources 先把 v1beta1 请求转成 v1，再翻译回 v1beta1 响应。
// 插件返回的 RPC 错误原样透传，避免包装层掩盖插件语义。
func (w V1ServerWrapper) NodePrepareResources(ctx context.Context, req *NodePrepareResourcesRequest) (*NodePrepareResourcesResponse, error) {
	var convertedReq v1.NodePrepareResourcesRequest
	if err := Convert_v1beta1_NodePrepareResourcesRequest_To_v1_NodePrepareResourcesRequest(req, &convertedReq, nil); err != nil {
		return nil, fmt.Errorf("internal error converting NodePrepareResourcesRequest from v1beta1 to v1: %w", err)
	}
	resp, err := w.DRAPluginServer.NodePrepareResources(ctx, &convertedReq)
	if err != nil {
		return nil, err
	}
	var convertedResp NodePrepareResourcesResponse
	if err := Convert_v1_NodePrepareResourcesResponse_To_v1beta1_NodePrepareResourcesResponse(resp, &convertedResp, nil); err != nil {
		return nil, fmt.Errorf("internal error converting NodePrepareResourcesResponse from v1 to v1beta1: %w", err)
	}
	return &convertedResp, nil
}

// NodeUnprepareResources 同样做双向翻译，保证资源清理路径也跨版本可用。
func (w V1ServerWrapper) NodeUnprepareResources(ctx context.Context, req *NodeUnprepareResourcesRequest) (*NodeUnprepareResourcesResponse, error) {
	var convertedReq v1.NodeUnprepareResourcesRequest
	if err := Convert_v1beta1_NodeUnprepareResourcesRequest_To_v1_NodeUnprepareResourcesRequest(req, &convertedReq, nil); err != nil {
		return nil, fmt.Errorf("internal error converting NodeUnprepareResourcesRequest from v1beta1 to v1: %w", err)
	}
	resp, err := w.DRAPluginServer.NodeUnprepareResources(ctx, &convertedReq)
	if err != nil {
		return nil, err
	}
	var convertedResp NodeUnprepareResourcesResponse
	if err := Convert_v1_NodeUnprepareResourcesResponse_To_v1beta1_NodeUnprepareResourcesResponse(resp, &convertedResp, nil); err != nil {
		return nil, fmt.Errorf("internal error converting NodeUnprepareResourcesResponse from v1 to v1beta1: %w", err)
	}
	return &convertedResp, nil
}

// V1Beta1ServerWrapper 包装 [NodeServer] 以实现 [v1.DRAPluginServer] 接口。
//
// 反方向的翻译器：插件只实现 v1beta1，却要接住 v1 kubelet 的请求。
type V1Beta1ServerWrapper struct {
	v1.UnsafeDRAPluginServer
	DRAPluginServer
}

// 编译期断言：确保包装器对 v1 接口保持可赋值。
var _ v1.DRAPluginServer = V1Beta1ServerWrapper{}

// NodePrepareResources 把 v1 请求翻成 v1beta1 交给插件，再把结果翻回 v1。
func (w V1Beta1ServerWrapper) NodePrepareResources(ctx context.Context, req *v1.NodePrepareResourcesRequest) (*v1.NodePrepareResourcesResponse, error) {
	var convertedReq NodePrepareResourcesRequest
	if err := Convert_v1_NodePrepareResourcesRequest_To_v1beta1_NodePrepareResourcesRequest(req, &convertedReq, nil); err != nil {
		return nil, fmt.Errorf("internal error converting NodePrepareResourcesRequest from v1 to v1beta1: %w", err)
	}
	resp, err := w.DRAPluginServer.NodePrepareResources(ctx, &convertedReq)
	if err != nil {
		return nil, err
	}
	var convertedResp v1.NodePrepareResourcesResponse
	if err := Convert_v1beta1_NodePrepareResourcesResponse_To_v1_NodePrepareResourcesResponse(resp, &convertedResp, nil); err != nil {
		return nil, fmt.Errorf("internal error converting NodePrepareResourcesResponse from v1beta1 to v1: %w", err)
	}
	return &convertedResp, nil
}

// NodeUnprepareResources 是清理路径上的 v1 → v1beta1 双向翻译。
func (w V1Beta1ServerWrapper) NodeUnprepareResources(ctx context.Context, req *v1.NodeUnprepareResourcesRequest) (*v1.NodeUnprepareResourcesResponse, error) {
	var convertedReq NodeUnprepareResourcesRequest
	if err := Convert_v1_NodeUnprepareResourcesRequest_To_v1beta1_NodeUnprepareResourcesRequest(req, &convertedReq, nil); err != nil {
		return nil, fmt.Errorf("internal error converting NodeUnprepareResourcesRequest from v1 to v1beta1: %w", err)
	}
	resp, err := w.DRAPluginServer.NodeUnprepareResources(ctx, &convertedReq)
	if err != nil {
		return nil, err
	}
	var convertedResp v1.NodeUnprepareResourcesResponse
	if err := Convert_v1beta1_NodeUnprepareResourcesResponse_To_v1_NodeUnprepareResourcesResponse(resp, &convertedResp, nil); err != nil {
		return nil, fmt.Errorf("internal error converting NodeUnprepareResourcesResponse from v1beta1 to v1: %w", err)
	}
	return &convertedResp, nil
}

// V1ClientWrapper 包装 [v1.DRAPluginClient] 以实现 [NodeClient] 接口。
//
// 客户端侧的适配：用 v1beta1 的接口调用一个远端只说 v1 的插件。grpc.CallOption
// 原样透传，调用方设置的超时、认证等选项不能因版本翻译而丢失。
type V1ClientWrapper struct {
	v1.DRAPluginClient
}

// 编译期断言：包装器满足 v1beta1 客户端接口。
var _ DRAPluginClient = V1ClientWrapper{}

// NodePrepareResources 将请求翻成 v1 后调用远端，并把响应翻回 v1beta1。
func (w V1ClientWrapper) NodePrepareResources(ctx context.Context, req *NodePrepareResourcesRequest, options ...grpc.CallOption) (*NodePrepareResourcesResponse, error) {
	var convertedReq v1.NodePrepareResourcesRequest
	if err := Convert_v1beta1_NodePrepareResourcesRequest_To_v1_NodePrepareResourcesRequest(req, &convertedReq, nil); err != nil {
		return nil, fmt.Errorf("internal error converting NodePrepareResourcesRequest from v1beta1 to v1: %w", err)
	}
	resp, err := w.DRAPluginClient.NodePrepareResources(ctx, &convertedReq, options...)
	if err != nil {
		return nil, err
	}
	var convertedResp NodePrepareResourcesResponse
	if err := Convert_v1_NodePrepareResourcesResponse_To_v1beta1_NodePrepareResourcesResponse(resp, &convertedResp, nil); err != nil {
		return nil, fmt.Errorf("internal error converting NodePrepareResourcesResponse from v1 to v1beta1: %w", err)
	}
	return &convertedResp, nil
}

// NodeUnprepareResources 是客户端清理路径上的版本适配。
func (w V1ClientWrapper) NodeUnprepareResources(ctx context.Context, req *NodeUnprepareResourcesRequest, options ...grpc.CallOption) (*NodeUnprepareResourcesResponse, error) {
	var convertedReq v1.NodeUnprepareResourcesRequest
	if err := Convert_v1beta1_NodeUnprepareResourcesRequest_To_v1_NodeUnprepareResourcesRequest(req, &convertedReq, nil); err != nil {
		return nil, fmt.Errorf("internal error converting NodeUnprepareResourcesRequest from v1beta1 to v1: %w", err)
	}
	resp, err := w.DRAPluginClient.NodeUnprepareResources(ctx, &convertedReq, options...)
	if err != nil {
		return nil, err
	}
	var convertedResp NodeUnprepareResourcesResponse
	if err := Convert_v1_NodeUnprepareResourcesResponse_To_v1beta1_NodeUnprepareResourcesResponse(resp, &convertedResp, nil); err != nil {
		return nil, fmt.Errorf("internal error converting NodeUnprepareResourcesResponse from v1 to v1beta1: %w", err)
	}
	return &convertedResp, nil
}

// V1Beta1ClientWrapper 包装 [NodeClient] 以实现 [v1.DRAPluginClient] 接口。
//
// 让只封装了 v1beta1 协议的客户端也能接入 v1 调用链，方向与前三个包装器相反。
type V1Beta1ClientWrapper struct {
	DRAPluginClient
}

// 编译期断言：包装器满足 v1 客户端接口。
var _ v1.DRAPluginClient = V1Beta1ClientWrapper{}

// NodePrepareResources 把 v1 请求翻成 v1beta1，响应再翻回 v1。
func (w V1Beta1ClientWrapper) NodePrepareResources(ctx context.Context, req *v1.NodePrepareResourcesRequest, options ...grpc.CallOption) (*v1.NodePrepareResourcesResponse, error) {
	var convertedReq NodePrepareResourcesRequest
	if err := Convert_v1_NodePrepareResourcesRequest_To_v1beta1_NodePrepareResourcesRequest(req, &convertedReq, nil); err != nil {
		return nil, fmt.Errorf("internal error converting NodePrepareResourcesRequest from v1 to v1beta1: %w", err)
	}
	resp, err := w.DRAPluginClient.NodePrepareResources(ctx, &convertedReq, options...)
	if err != nil {
		return nil, err
	}
	var convertedResp v1.NodePrepareResourcesResponse
	if err := Convert_v1beta1_NodePrepareResourcesResponse_To_v1_NodePrepareResourcesResponse(resp, &convertedResp, nil); err != nil {
		return nil, fmt.Errorf("internal error converting NodePrepareResourcesResponse from v1beta1 to v1: %w", err)
	}
	return &convertedResp, nil
}

// NodeUnprepareResources 是 v1 客户端调用 v1beta1 插件时的清理路径适配。
func (w V1Beta1ClientWrapper) NodeUnprepareResources(ctx context.Context, req *v1.NodeUnprepareResourcesRequest, options ...grpc.CallOption) (*v1.NodeUnprepareResourcesResponse, error) {
	var convertedReq NodeUnprepareResourcesRequest
	if err := Convert_v1_NodeUnprepareResourcesRequest_To_v1beta1_NodeUnprepareResourcesRequest(req, &convertedReq, nil); err != nil {
		return nil, fmt.Errorf("internal error converting NodeUnprepareResourcesRequest from v1 to v1beta1: %w", err)
	}
	resp, err := w.DRAPluginClient.NodeUnprepareResources(ctx, &convertedReq, options...)
	if err != nil {
		return nil, err
	}
	var convertedResp v1.NodeUnprepareResourcesResponse
	if err := Convert_v1beta1_NodeUnprepareResourcesResponse_To_v1_NodeUnprepareResourcesResponse(resp, &convertedResp, nil); err != nil {
		return nil, fmt.Errorf("internal error converting NodeUnprepareResourcesResponse from v1beta1 to v1: %w", err)
	}
	return &convertedResp, nil
}
