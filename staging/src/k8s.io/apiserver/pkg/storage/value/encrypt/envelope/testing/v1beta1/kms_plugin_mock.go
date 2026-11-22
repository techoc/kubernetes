//go:build !windows

/*
Copyright 2017 The Kubernetes Authors.

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
	"encoding/base64"
	"fmt"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	kmsapi "k8s.io/kms/apis/v1beta1"
)

const (
	// Now only supported unix domain socket.
	unixProtocol = "unix"

	// Current version for the protocol interface definition.
	kmsapiVersion = "v1beta1"
)

// Base64Plugin gRPC sever for a mock KMS provider.
// Uses base64 to simulate encrypt and decrypt.
//
// 这是测试替身而非真正的加密实现：base64 只做可逆编码，用来验证 KMS v2
// 客户端的协议交互（重载、健康检查、失败状态）而不引入真实密钥管理。
// 用互斥锁保护共享状态，因为 gRPC 服务端会并发调用各 RPC。
type Base64Plugin struct {
	grpcServer         *grpc.Server
	listener           net.Listener
	mu                 *sync.Mutex
	lastEncryptRequest *kmsapi.EncryptRequest
	inFailedState      bool
	ver                string
	socketPath         string
	kmsapi.UnsafeKeyManagementServiceServer
}

// NewBase64Plugin is a constructor for Base64Plugin.
func NewBase64Plugin(t *testing.T, socketPath string) *Base64Plugin {
	server := grpc.NewServer()
	result := &Base64Plugin{
		grpcServer: server,
		mu:         &sync.Mutex{},
		ver:        kmsapiVersion,
		socketPath: socketPath,
	}

	// Make sure we don't have a leftover socket.
	// 上一次测试异常退出可能残留 socket 文件，listen 会因此失败。
	_ = os.Remove(socketPath)
	kmsapi.RegisterKeyManagementServiceServer(server, result)
	if err := result.start(); err != nil {
		t.Fatalf("failed to start KMS plugin, err: %v", err)
	}
	// 注册清理，确保测试结束或失败时关闭 server 并删除 socket。
	t.Cleanup(result.CleanUp)
	if err := waitForBase64PluginToBeUp(result); err != nil {
		t.Fatalf("failed to start KMS plugin: err: %v", err)
	}
	return result
}

// waitForBase64PluginToBeUp waits until the plugin is ready to serve requests.
//
// Serve 在独立 goroutine 中启动，监听建立后仍可能尚未可服务，因此用一次
// Encrypt 探活作为就绪信号。
func waitForBase64PluginToBeUp(plugin *Base64Plugin) error {
	var gRPCErr error
	pollErr := wait.PollImmediate(1*time.Second, wait.ForeverTestTimeout, func() (bool, error) {
		_, gRPCErr = plugin.Encrypt(context.Background(), &kmsapi.EncryptRequest{Plain: []byte("foo")})
		return gRPCErr == nil, nil
	})

	if pollErr != nil {
		return fmt.Errorf("failed to start KMS plugin, gRPC error: %v, poll error: %v", gRPCErr, pollErr)
	}

	return nil
}

// LastEncryptRequest returns the last EncryptRequest.Plain sent to the plugin.
func (s *Base64Plugin) LastEncryptRequest() []byte {
	return s.lastEncryptRequest.Plain
}

// SetVersion sets the version of kms-plugin.
func (s *Base64Plugin) SetVersion(ver string) {
	s.ver = ver
}

// start starts plugin's gRPC service.
func (s *Base64Plugin) start() error {
	var err error
	s.listener, err = net.Listen(unixProtocol, s.socketPath)
	if err != nil {
		return fmt.Errorf("failed to listen on the unix socket, error: %v", err)
	}
	klog.InfoS("Starting KMS Plugin", "socketPath", s.socketPath)

	// Serve 阻塞直到 Stop，必须放在 goroutine 中。
	go s.grpcServer.Serve(s.listener)
	return nil
}

// CleanUp stops gRPC server and the underlying listener.
//
// 同时删除 socket 文件，否则下一次测试可能复用到已失效的 socket。
func (s *Base64Plugin) CleanUp() {
	s.grpcServer.Stop()
	_ = s.listener.Close()
	_ = os.Remove(s.socketPath)
}

// EnterFailedState places the plugin into failed state.
//
// 进入失败态后所有加解密返回 FailedPrecondition，用于模拟 KMS 密钥被禁用。
func (s *Base64Plugin) EnterFailedState() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inFailedState = true
}

// ExitFailedState removes the plugin from the failed state.
func (s *Base64Plugin) ExitFailedState() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inFailedState = false
}

// Version returns the version of the kms-plugin.
func (s *Base64Plugin) Version(ctx context.Context, request *kmsapi.VersionRequest) (*kmsapi.VersionResponse, error) {
	klog.V(3).InfoS("Received request for Version", "request", request)
	return &kmsapi.VersionResponse{Version: s.ver, RuntimeName: "testKMS", RuntimeVersion: "0.0.1"}, nil
}

// Decrypt performs base64 decoding of the payload of kms.DecryptRequest.
func (s *Base64Plugin) Decrypt(ctx context.Context, request *kmsapi.DecryptRequest) (*kmsapi.DecryptResponse, error) {
	klog.V(3).InfoS("Received Decrypt Request", "cipher", string(request.Cipher))

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inFailedState {
		return nil, status.Error(codes.FailedPrecondition, "failed precondition - key disabled")
	}

	// 先按编码长度分配缓冲，再以实际解码长度切片返回。
	buf := make([]byte, base64.StdEncoding.DecodedLen(len(request.Cipher)))
	n, err := base64.StdEncoding.Decode(buf, request.Cipher)
	if err != nil {
		return nil, err
	}

	return &kmsapi.DecryptResponse{Plain: buf[:n]}, nil
}

// Encrypt performs base64 encoding of the payload of kms.EncryptRequest.
func (s *Base64Plugin) Encrypt(ctx context.Context, request *kmsapi.EncryptRequest) (*kmsapi.EncryptResponse, error) {
	klog.V(3).InfoS("Received Encrypt Request", "plain", string(request.Plain))
	s.mu.Lock()
	defer s.mu.Unlock()
	// 记录最后一次请求，供测试断言客户端传入的明文。
	s.lastEncryptRequest = request

	if s.inFailedState {
		return nil, status.Error(codes.FailedPrecondition, "failed precondition - key disabled")
	}

	buf := make([]byte, base64.StdEncoding.EncodedLen(len(request.Plain)))
	base64.StdEncoding.Encode(buf, request.Plain)

	return &kmsapi.EncryptResponse{Cipher: buf}, nil
}
