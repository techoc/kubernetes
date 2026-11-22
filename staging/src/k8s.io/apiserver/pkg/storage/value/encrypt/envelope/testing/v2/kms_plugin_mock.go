//go:build !windows

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

package v2

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
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	kmsapi "k8s.io/kms/apis/v2"
)

const (
	// Now only supported unix domain socket.
	unixProtocol = "unix"

	// Current version for the protocol interface definition.
	kmsapiVersion = "v2beta1"
)

// Base64Plugin gRPC sever for a mock KMS provider.
// Uses base64 to simulate encrypt and decrypt.
//
// 中文说明：用 base64 而非真实加密，是为了让测试能验证「密文与明文不同」
// 且可逆，而无需管理密钥材料。所有可变字段由 mu 保护：
// gRPC 服务端并发处理请求，而测试协程会并发修改 inFailedState/keyID，
// 因此读写都必须加锁，否则 race detector 会报错。
type Base64Plugin struct {
	grpcServer         *grpc.Server
	listener           net.Listener
	mu                 *sync.Mutex
	lastEncryptRequest *kmsapi.EncryptRequest
	inFailedState      bool
	ver                string
	socketPath         string
	keyID              string
	kmsapi.UnsafeKeyManagementServiceServer
}

// NewBase64Plugin is a constructor for Base64Plugin.
//
// 中文说明：拦截器校验 :authority 必须为 localhost，用来确保 apiserver 的
// KMS 客户端在 unix socket 上设置了正确的主机名（否则 TLS/路由行为可能异常）。
// 启动后轮询 Status 直到就绪，避免测试在服务端尚未监听时发起请求。
// CleanUp 通过 t.Cleanup 注册，保证用例失败时也会移除 socket 文件。
func NewBase64Plugin(t testing.TB, socketPath string) *Base64Plugin {
	server := grpc.NewServer(
		grpc.UnaryInterceptor(
			func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
				if val := metadata.ValueFromIncomingContext(ctx, ":authority"); len(val) != 1 || val[0] != "localhost" {
					t.Errorf("wanted localhost authority, got: %v", val)
				}
				return handler(ctx, req)
			},
		),
	)
	result := &Base64Plugin{
		grpcServer: server,
		mu:         &sync.Mutex{},
		ver:        kmsapiVersion,
		socketPath: socketPath,
		keyID:      "1",
	}

	kmsapi.RegisterKeyManagementServiceServer(server, result)

	if err := result.start(); err != nil {
		t.Fatalf("failed to start KMS plugin, err: %v", err)
	}
	t.Cleanup(result.CleanUp)
	if err := waitForBase64PluginToBeUp(result); err != nil {
		t.Fatalf("failed to start KMS plugin: err: %v", err)
	}
	return result
}

// waitForBase64PluginToBeUp waits until the plugin is ready to serve requests.
//
// 中文说明：以 Healthz=="ok" 作为就绪判据，而不是仅依赖监听成功——
// gRPC Serve 是异步的，监听建立不代表可以处理 RPC。
func waitForBase64PluginToBeUp(plugin *Base64Plugin) error {
	var gRPCErr error
	var resp *kmsapi.StatusResponse
	pollErr := wait.PollImmediate(1*time.Second, wait.ForeverTestTimeout, func() (bool, error) {
		resp, gRPCErr = plugin.Status(context.Background(), &kmsapi.StatusRequest{})
		return gRPCErr == nil && resp.Healthz == "ok", nil
	})

	if pollErr != nil {
		return fmt.Errorf("failed to start kms-plugin, gRPC error: %v, poll error: %v", gRPCErr, pollErr)
	}

	return nil
}

// waitForBase64PluginToBeUpdated waits until the plugin updates keyID.
//
// 中文说明：用于验证 apiserver 能感知 KMS 的 KEK 轮换——轮换后插件报告新 keyID，
// apiserver 应重新加密后续数据。轮询等待 keyID 变为 "2" 即表示变更已生效。
func WaitForBase64PluginToBeUpdated(plugin *Base64Plugin) error {
	var gRPCErr error
	var resp *kmsapi.StatusResponse

	updatePollErr := wait.PollImmediate(1*time.Second, wait.ForeverTestTimeout, func() (bool, error) {
		resp, gRPCErr = plugin.Status(context.Background(), &kmsapi.StatusRequest{})
		klog.InfoS("WaitForBase64PluginToBeUpdated", "keyID", resp.KeyId)
		return gRPCErr == nil && resp.Healthz == "ok" && resp.KeyId == "2", nil
	})

	if updatePollErr != nil {
		return fmt.Errorf("failed to update keyID for kmsv2-plugin, gRPC error: %w, updatePoll error: %w", gRPCErr, updatePollErr)
	}

	return nil
}

// LastEncryptRequest returns the last EncryptRequest.Plain sent to the plugin.
//
// 中文说明：测试借此断言 apiserver 传入的明文与存储的内容一致。
// 未加锁读取是刻意的简化：调用前测试通常已同步等待过 Encrypt 完成。
func (s *Base64Plugin) LastEncryptRequest() []byte {
	return s.lastEncryptRequest.Plaintext
}

// SetVersion sets the version of kms-plugin.
//
// 中文说明：用于模拟协议版本不匹配，验证 apiserver 会拒绝不兼容的插件。
func (s *Base64Plugin) SetVersion(ver string) {
	s.ver = ver
}

// start starts plugin's gRPC service.
//
// 中文说明：Serve 在独立 goroutine 中运行，因此这里不会阻塞构造流程；
// 调用方需自行等待就绪。
func (s *Base64Plugin) start() error {
	var err error
	s.listener, err = net.Listen(unixProtocol, s.socketPath)
	if err != nil {
		return fmt.Errorf("failed to listen on the unix socket, error: %v", err)
	}
	klog.InfoS("Starting KMS Plugin", "socketPath", s.socketPath)

	go s.grpcServer.Serve(s.listener)
	return nil
}

// CleanUp stops gRPC server and the underlying listener.
//
// 中文说明：除了关闭监听器，还要删除 socket 文件——unix socket 是文件系统实体，
// 不删除会残留并在下次 Listen 时返回 address already in use。
func (s *Base64Plugin) CleanUp() {
	s.grpcServer.Stop()
	_ = s.listener.Close()
	_ = os.Remove(s.socketPath)
}

// EnterFailedState places the plugin into failed state.
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

// Update keyID for the plugin.
//
// 中文说明：模拟 KEK 轮换，把 keyID 从 "1" 切到 "2"。
func (s *Base64Plugin) UpdateKeyID() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keyID = "2"
}

// Status returns the status of the kms-plugin.
//
// 中文说明：失败状态下返回 FailedPrecondition，与真实 KMS 插件在密钥被禁用时的
// 行为一致，从而让 apiserver 走「KMS 不可用」的降级/拒绝路径。
func (s *Base64Plugin) Status(ctx context.Context, request *kmsapi.StatusRequest) (*kmsapi.StatusResponse, error) {
	klog.V(3).InfoS("Received request for Status", "request", request)
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.inFailedState {
		return nil, status.Error(codes.FailedPrecondition, "failed precondition - key disabled")
	}

	return &kmsapi.StatusResponse{Version: s.ver, Healthz: "ok", KeyId: s.keyID}, nil
}

// Decrypt performs base64 decoding of the payload of kms.DecryptRequest.
//
// 中文说明：要求 Uid 非空——这是 KMSv2 的关联数据完整性检查，
// 缺少 UID 的请求可能来自旧客户端或被篡改，必须拒绝。
func (s *Base64Plugin) Decrypt(ctx context.Context, request *kmsapi.DecryptRequest) (*kmsapi.DecryptResponse, error) {
	klog.V(3).InfoS("Received Decrypt Request", "ciphertext", string(request.Ciphertext))

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inFailedState {
		return nil, status.Error(codes.FailedPrecondition, "failed precondition - key disabled")
	}
	if len(request.Uid) == 0 {
		return nil, status.Error(codes.InvalidArgument, "uid is required")
	}

	buf := make([]byte, base64.StdEncoding.DecodedLen(len(request.Ciphertext)))
	n, err := base64.StdEncoding.Decode(buf, request.Ciphertext)
	if err != nil {
		return nil, err
	}

	return &kmsapi.DecryptResponse{Plaintext: buf[:n]}, nil
}

// Encrypt performs base64 encoding of the payload of kms.EncryptRequest.
//
// 中文说明：记录最后一次请求供测试断言；返回的 Annotations 模拟本地 KEK 场景，
// 用于验证注解会被原样写入存储而不会被 apiserver 丢弃。
func (s *Base64Plugin) Encrypt(ctx context.Context, request *kmsapi.EncryptRequest) (*kmsapi.EncryptResponse, error) {
	klog.V(3).InfoS("Received Encrypt Request", "plaintext", string(request.Plaintext))
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lastEncryptRequest = request

	if s.inFailedState {
		return nil, status.Error(codes.FailedPrecondition, "failed precondition - key disabled")
	}
	if len(request.Uid) == 0 {
		return nil, status.Error(codes.InvalidArgument, "uid is required")
	}

	buf := make([]byte, base64.StdEncoding.EncodedLen(len(request.Plaintext)))
	base64.StdEncoding.Encode(buf, request.Plaintext)

	return &kmsapi.EncryptResponse{Ciphertext: buf, KeyId: s.keyID, Annotations: map[string][]byte{"local-kek.kms.kubernetes.io": []byte("encrypted-local-kek")}}, nil
}
