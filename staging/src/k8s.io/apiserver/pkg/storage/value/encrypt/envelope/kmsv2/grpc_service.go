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

// Package kmsv2 使用 Envelope provider 对静态存储的数据做加解密。
//
// 通过 gRPC 调用外部 KMS 插件完成信封加密：数据密钥由本地生成，
// KMS 只负责包裹/解包该密钥，避免把大量数据送进 KMS。
package kmsv2

import (
	"context"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/storage/value/encrypt/envelope/metrics"
	"k8s.io/klog/v2"
	kmsapi "k8s.io/kms/apis/v2"
	kmsservice "k8s.io/kms/pkg/service"
	"k8s.io/kms/pkg/util"
)

const (
	// 远程 KMS 只支持 unix 协议。
	unixProtocol = "unix"
)

// envelope.Service 的 gRPC 实现。
type gRPCService struct {
	kmsClient   kmsapi.KeyManagementServiceClient
	connection  *grpc.ClientConn
	callTimeout time.Duration
}

// NewGRPCService 返回一个通过 gRPC 与远程 KMS 通信的 envelope.Service。
//
// 只支持 unix socket：KMS 插件与 apiserver 同机部署，避免把密钥材料暴露到网络。
// callTimeout 约束每次 RPC，防止 KMS 卡住拖垮所有存储请求。
func NewGRPCService(ctx context.Context, endpoint, providerName string, callTimeout time.Duration) (kmsservice.Service, error) {
	klog.V(4).InfoS("Configure KMS provider", "endpoint", endpoint)

	addr, err := util.ParseEndpoint(endpoint)
	if err != nil {
		return nil, err
	}

	s := &gRPCService{callTimeout: callTimeout}
	s.connection, err = grpc.Dial(
		addr,
		grpc.WithAuthority("localhost"),
		// unix socket 本身不经过网络，因此使用 insecure 凭据；安全性由文件权限保证。
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
		grpc.WithContextDialer(
			func(context.Context, string) (net.Conn, error) {
				// 忽略 addr 与 timeout 参数：addr 来自闭包。
				c, err := net.DialUnix(unixProtocol, nil, &net.UnixAddr{Name: addr})
				if err != nil {
					klog.ErrorS(err, "failed to create connection to unix socket", "addr", addr)
				} else {
					klog.V(4).InfoS("Successfully dialed Unix socket", "addr", addr)
				}
				return c, err
			}),
		grpc.WithChainUnaryInterceptor(recordMetricsInterceptor(providerName)),
	)

	if err != nil {
		return nil, fmt.Errorf("failed to create connection to %s, error: %v", endpoint, err)
	}

	s.kmsClient = kmsapi.NewKeyManagementServiceClient(s.connection)

	// ctx 结束（apiserver 关闭）时关闭连接，避免泄漏 socket。
	go func() {
		defer utilruntime.HandleCrashWithContext(ctx)

		<-ctx.Done()
		_ = s.connection.Close()
	}()

	return s, nil
}

// Decrypt 解密给定的数据，返回原始字节。
func (g *gRPCService) Decrypt(ctx context.Context, uid string, req *kmsservice.DecryptRequest) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, g.callTimeout)
	defer cancel()

	request := &kmsapi.DecryptRequest{
		Ciphertext:  req.Ciphertext,
		Uid:         uid,
		KeyId:       req.KeyID,
		Annotations: req.Annotations,
	}
	response, err := g.kmsClient.Decrypt(ctx, request)
	if err != nil {
		return nil, err
	}
	return response.Plaintext, nil
}

// Encrypt 把字节数据加密为字符串形式的密文。
func (g *gRPCService) Encrypt(ctx context.Context, uid string, plaintext []byte) (*kmsservice.EncryptResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, g.callTimeout)
	defer cancel()

	request := &kmsapi.EncryptRequest{
		Plaintext: plaintext,
		Uid:       uid,
	}
	response, err := g.kmsClient.Encrypt(ctx, request)
	if err != nil {
		return nil, err
	}
	return &kmsservice.EncryptResponse{
		Ciphertext:  response.Ciphertext,
		KeyID:       response.KeyId,
		Annotations: response.Annotations,
	}, nil
}

// Status 返回 KMSv2 provider 的状态。
//
// 用于健康检查与密钥轮换判断：KeyID 变化意味着需要重新包裹数据密钥。
func (g *gRPCService) Status(ctx context.Context) (*kmsservice.StatusResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, g.callTimeout)
	defer cancel()

	request := &kmsapi.StatusRequest{}
	response, err := g.kmsClient.Status(ctx, request)
	if err != nil {
		return nil, err
	}
	return &kmsservice.StatusResponse{Version: response.Version, Healthz: response.Healthz, KeyID: response.KeyId}, nil
}

// recordMetricsInterceptor 记录每次 KMS RPC 的耗时与结果，
// 便于区分是 KMS 慢还是存储层慢。
func recordMetricsInterceptor(providerName string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		nowFunc := GetNowFunc(providerName)
		start := nowFunc()
		respErr := invoker(ctx, method, req, reply, cc, opts...)
		elapsed := nowFunc().Sub(start)
		metrics.RecordKMSOperationLatency(providerName, method, elapsed, respErr)
		return respErr
	}
}
