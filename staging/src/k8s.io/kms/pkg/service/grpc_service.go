/*
Copyright 2023 The Kubernetes Authors.

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

package service

import (
	"context"
	"net"
	"time"

	"google.golang.org/grpc"

	kmsapi "k8s.io/kms/apis/v2"
)

// GRPCService 是承载 KMS v2 API 的 gRPC 服务端，负责把 protobuf 请求转换为
// 内部 Service 接口调用，并把业务结果包装回 protobuf 响应。
type GRPCService struct {
	// addr 是监听的 Unix socket 路径。
	addr string
	// timeout 是 gRPC 连接超时时间。
	timeout time.Duration
	// server 是底层 gRPC server，启动后赋值；Shutdown/Close 通过它停止服务。
	server *grpc.Server

	// kmsService 是真正的加解密业务实现。
	kmsService Service
	// 嵌入 UnsafeKeyManagementServiceServer 以避免 future 新增方法时破坏编译：
	// 未实现的 RPC 会由嵌入的 nil 接口提供默认 panic 行为。
	kmsapi.UnsafeKeyManagementServiceServer
}

// 编译期断言：GRPCService 必须完整实现 KMS gRPC 服务端接口。
var _ kmsapi.KeyManagementServiceServer = (*GRPCService)(nil)

// NewGRPCService 构造一个 GRPCService，参数分别为监听地址、连接超时与业务实现。
func NewGRPCService(
	address string,
	timeout time.Duration,

	kmsService Service,
) *GRPCService {
	return &GRPCService{
		addr:       address,
		timeout:    timeout,
		kmsService: kmsService,
	}
}

// ListenAndServe 在 Unix socket 上监听并阻塞处理请求。
// 流程：
//  1. 在指定地址建立 Unix socket 监听；
//  2. 创建带连接超时配置的 gRPC server；
//  3. 把自身注册为 KeyManagementService 的服务端实现；
//  4. 调用 Serve 开始阻塞服务，正常关闭时返回 nil。
func (s *GRPCService) ListenAndServe() error {
	ln, err := net.Listen("unix", s.addr)
	if err != nil {
		return err
	}
	// 函数退出时关闭监听器。
	defer ln.Close()

	// 设置连接超时，避免慢客户端长期占用资源。
	gs := grpc.NewServer(
		grpc.ConnectionTimeout(s.timeout),
	)
	s.server = gs

	// 把本对象注册为 KMS 服务端实现。
	kmsapi.RegisterKeyManagementServiceServer(gs, s)

	return gs.Serve(ln)
}

// Shutdown 优雅关闭：停止接收新连接，并阻塞等待所有进行中的 RPC 完成。
func (s *GRPCService) Shutdown() {
	if s.server != nil {
		s.server.GracefulStop()
	}
}

// Close 立即关闭：直接断开所有连接并取消所有活跃 RPC（与 Shutdown 的优雅关闭相对）。
func (s *GRPCService) Close() {
	if s.server != nil {
		s.server.Stop()
	}
}

// Status 转发状态查询请求给底层 KMS 服务，并把结果转换为 protobuf 响应。
func (s *GRPCService) Status(ctx context.Context, _ *kmsapi.StatusRequest) (*kmsapi.StatusResponse, error) {
	res, err := s.kmsService.Status(ctx)
	if err != nil {
		return nil, err
	}

	return &kmsapi.StatusResponse{
		Version: res.Version,
		Healthz: res.Healthz,
		KeyId:   res.KeyID,
	}, nil
}

// Decrypt 把 protobuf 解密请求转换为内部 DecryptRequest，调用业务实现后返回明文。
func (s *GRPCService) Decrypt(ctx context.Context, req *kmsapi.DecryptRequest) (*kmsapi.DecryptResponse, error) {
	// 把 protobuf 的 KeyId 字段（生成代码命名）映射到内部的 KeyID。
	plaintext, err := s.kmsService.Decrypt(ctx, req.Uid, &DecryptRequest{
		Ciphertext:  req.Ciphertext,
		KeyID:       req.KeyId,
		Annotations: req.Annotations,
	})
	if err != nil {
		return nil, err
	}

	return &kmsapi.DecryptResponse{
		Plaintext: plaintext,
	}, nil
}

// Encrypt 调用业务实现加密明文，并把密文、key ID 与 annotations 包装为 protobuf 响应。
func (s *GRPCService) Encrypt(ctx context.Context, req *kmsapi.EncryptRequest) (*kmsapi.EncryptResponse, error) {
	encRes, err := s.kmsService.Encrypt(ctx, req.Uid, req.Plaintext)
	if err != nil {
		return nil, err
	}

	return &kmsapi.EncryptResponse{
		Ciphertext:  encRes.Ciphertext,
		KeyId:       encRes.KeyID,
		Annotations: encRes.Annotations,
	}, nil
}
