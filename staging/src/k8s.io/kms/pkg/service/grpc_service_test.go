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
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	kmsapi "k8s.io/kms/apis/v2"
)

// version 是测试用 KMS 服务上报的 API 版本。
const version = "v2"

// TestGRPCService 是端到端测试：在真实 Unix socket 上启动 GRPCService，
// 用 gRPC 客户端验证加解密往返与状态查询是否正常工作。
func TestGRPCService(t *testing.T) {
	t.Parallel()

	// 设置整体超时，防止服务启动失败时测试永久挂起。
	defaultTimeout := 30 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), defaultTimeout)
	t.Cleanup(cancel)

	// 准备 socket 地址、明文与随机 key ID。
	address := filepath.Join(os.TempDir(), "kmsv2.sock")
	plaintext := []byte("lorem ipsum dolor sit amet")
	r := rand.New(rand.NewSource(time.Now().Unix()))
	id, err := makeID(r.Read)
	if err != nil {
		t.Fatal(err)
	}

	// 用 base64 服务作为后端，启动 gRPC 服务端（在 goroutine 中阻塞运行）。
	kmsService := newBase64Service(id)
	server := NewGRPCService(address, defaultTimeout, kmsService)
	go func() {
		if err := server.ListenAndServe(); err != nil {
			panic(err)
		}
	}()
	// 测试结束后优雅关闭服务端。
	t.Cleanup(server.Shutdown)

	// 创建连接到该 socket 的 gRPC 客户端。
	client := newClient(t, address)

	// 等待服务端就绪：循环调用 Status 探测，直到成功或整体超时。
ready:
	for {
		select {
		case <-ctx.Done():
			t.Fatalf("server failed to start in time: %v", ctx.Err())

		default:
			// 每次探测单独设置 3 秒子超时，避免单次调用卡住。
			if done := func() bool {
				ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
				defer cancel()

				_, err := client.Status(ctx, &kmsapi.StatusRequest{})
				if err != nil {
					t.Logf("failed to get kms status: %v", err)
				}

				return err == nil
			}(); done {
				break ready
			}
			// 尚未就绪，稍后重试。
			time.Sleep(time.Second)
		}
	}

	// 子测试一：通过 Unix socket 完成加密再解密的往返，验证密文不同于明文且最终还原。
	t.Run("should be able to encrypt and decrypt through unix domain sockets", func(t *testing.T) {
		t.Parallel()

		// 先加密。
		encRes, err := client.Encrypt(ctx, &kmsapi.EncryptRequest{
			Plaintext: plaintext,
			Uid:       id,
		})
		if err != nil {
			t.Fatal(err)
		}

		// 密文不应与明文相同。
		if bytes.Equal(plaintext, encRes.Ciphertext) {
			t.Fatal("plaintext and ciphertext shouldn't be equal!")
		}

		// 带上加密时返回的 key ID 与 annotations 解密。
		decRes, err := client.Decrypt(ctx, &kmsapi.DecryptRequest{
			Ciphertext:  encRes.Ciphertext,
			KeyId:       encRes.KeyId,
			Annotations: encRes.Annotations,
			Uid:         id,
		})
		if err != nil {
			t.Fatal(err)
		}

		// 解密结果应与原始明文一致。
		if !bytes.Equal(decRes.Plaintext, plaintext) {
			t.Errorf("want: %q, have: %q", plaintext, decRes.Plaintext)
		}
	})

	// 子测试二：验证 Status RPC 返回的健康状态、key ID 与版本号。
	t.Run("should return status data", func(t *testing.T) {
		t.Parallel()

		status, err := client.Status(ctx, &kmsapi.StatusRequest{})
		if err != nil {
			t.Fatal(err)
		}

		if status.Healthz != "ok" {
			t.Errorf("want: %q, have: %q", "ok", status.Healthz)
		}
		if len(status.KeyId) == 0 {
			t.Errorf("want: len(keyID) > 0, have: %d", len(status.KeyId))
		}
		if status.Version != version {
			t.Errorf("want %q, have: %q", version, status.Version)
		}
	})
}

// newClient 建立到指定 Unix socket 地址的 gRPC 客户端连接，并注册测试结束时的清理。
// 使用 insecure 凭证，因为本地 socket 不依赖 TLS。
func newClient(t *testing.T, address string) kmsapi.KeyManagementServiceClient {
	t.Helper()

	cnn, err := grpc.Dial(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDialer(func(addr string, t time.Duration) (net.Conn, error) {
			return net.Dial("unix", addr)
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() { _ = cnn.Close() })

	return kmsapi.NewKeyManagementServiceClient(cnn)
}

// testService 是 Service 接口的可注入实现：把三个方法都委托给函数字段，
// 便于测试中快速构造不同的行为。
type testService struct {
	decrypt func(ctx context.Context, uid string, req *DecryptRequest) ([]byte, error)
	encrypt func(ctx context.Context, uid string, data []byte) (*EncryptResponse, error)
	status  func(ctx context.Context) (*StatusResponse, error)
}

// 编译期断言：testService 必须实现 Service 接口。
var _ Service = (*testService)(nil)

func (s *testService) Decrypt(ctx context.Context, uid string, req *DecryptRequest) ([]byte, error) {
	return s.decrypt(ctx, uid, req)
}

func (s *testService) Encrypt(ctx context.Context, uid string, data []byte) (*EncryptResponse, error) {
	return s.encrypt(ctx, uid, data)
}

func (s *testService) Status(ctx context.Context) (*StatusResponse, error) {
	return s.status(ctx)
}

// makeID 用传入的随机源生成 10 字节随机数，并以标准 base64 编码为字符串 key ID。
// 把随机函数作为参数传入，便于测试注入确定性的随机源。
func makeID(rand func([]byte) (int, error)) (string, error) {
	b := make([]byte, 10)
	if _, err := rand(b); err != nil {
		return "", err
	}

	return base64.StdEncoding.EncodeToString(b), nil
}

// newBase64Service 构造一个测试用 KMS 服务：
//   - Encrypt：用 base64 编码明文作为"密文"，并记录 key ID；
//   - Decrypt：校验 key ID 后 base64 解码还原明文；
//   - Status：返回固定版本与 "ok" 健康状态。
//
// 这里只用于验证 gRPC 链路与字段转换，不涉及真实加密强度。
func newBase64Service(keyID string) *testService {
	// 解密：先校验 key ID 是否匹配，再做 base64 解码。
	decrypt := func(_ context.Context, _ string, req *DecryptRequest) ([]byte, error) {
		if req.KeyID != keyID {
			return nil, fmt.Errorf("keyID mismatch. want: %q, have: %q", keyID, req.KeyID)
		}

		return base64.StdEncoding.DecodeString(string(req.Ciphertext))
	}

	// 加密：把明文 base64 编码后作为密文返回。
	encrypt := func(_ context.Context, _ string, data []byte) (*EncryptResponse, error) {
		return &EncryptResponse{
			Ciphertext: []byte(base64.StdEncoding.EncodeToString(data)),
			KeyID:      keyID,
		}, nil
	}

	// 状态：返回固定版本与健康信息。
	status := func(_ context.Context) (*StatusResponse, error) {
		return &StatusResponse{
			Version: version,
			Healthz: "ok",
			KeyID:   keyID,
		}, nil
	}

	return &testService{
		decrypt: decrypt,
		encrypt: encrypt,
		status:  status,
	}
}
