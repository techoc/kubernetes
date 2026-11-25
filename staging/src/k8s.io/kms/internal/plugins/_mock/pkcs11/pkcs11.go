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

package pkcs11

import (
	"context"
	"crypto/cipher"
	"crypto/rand"
	"fmt"

	crypot11 "github.com/ThalesIgnite/crypto11"

	"k8s.io/kms/pkg/service"
)

const (
	// mockAnnotationKey 是 mock 插件写入 annotations 的版本标识键，解密时会校验它。
	mockAnnotationKey = "version.encryption.remote.io"
)

// 编译期断言：pkcs11RemoteService 必须实现 service.Service 接口。
var _ service.Service = &pkcs11RemoteService{}

// pkcs11RemoteService 是基于 PKCS#11（SoftHSMv2）的 KMS 服务实现。
type pkcs11RemoteService struct {
	// keyID 是 HSM 中密钥的标识，同时作为 AEAD 的附加认证数据（AAD）。
	keyID string
	// aead 是 AES-GCM 认证加密实例，由 HSM 中的密钥创建。
	aead cipher.AEAD
}

// NewPKCS11RemoteService 从 SoftHSMv2 配置文件加载上下文，按 keyID 查找密钥并创建 GCM AEAD 实例。
// 流程：
//  1. 用配置文件初始化 PKCS#11 上下文；
//  2. 校验 keyID 非空；
//  3. 在 HSM 中查找该 keyID 对应的密钥，未找到则报错；
//  4. 用密钥创建 GCM 模式 AEAD，失败则返回错误。
func NewPKCS11RemoteService(configFilePath, keyID string) (service.Service, error) {
	ctx, err := crypot11.ConfigureFromFile(configFilePath)
	if err != nil {
		return nil, err
	}

	if len(keyID) == 0 {
		return nil, fmt.Errorf("invalid keyID")
	}

	remoteService := &pkcs11RemoteService{
		keyID: keyID,
	}

	key, err := ctx.FindKey(nil, []byte(keyID))
	if err != nil {
		return nil, err
	}
	if key == nil {
		return nil, fmt.Errorf("key not found")
	}
	if remoteService.aead, err = key.NewGCM(); err != nil {
		return nil, err
	}

	return remoteService, nil
}

// Encrypt 使用 HSM 密钥以 AES-GCM 加密明文，并把 nonce 前置到密文一起返回。
// 返回的 annotations 中写入版本号，供 Decrypt 校验。
func (s *pkcs11RemoteService) Encrypt(ctx context.Context, uid string, plaintext []byte) (*service.EncryptResponse, error) {
	// 预留 nonce + 认证标签 + 密文所需的总空间。
	nonceSize := s.aead.NonceSize()
	result := make([]byte, nonceSize+s.aead.Overhead()+len(plaintext))
	// 填充随机 nonce。
	n, err := rand.Read(result[:nonceSize])
	if err != nil {
		return nil, err
	}
	// 确认随机数读取完整。
	if n != nonceSize {
		return nil, fmt.Errorf("unable to read sufficient random bytes")
	}
	// 用 nonce 作为 Seal 的前缀写入密文；keyID 作为附加认证数据参与认证。
	cipherText := s.aead.Seal(result[nonceSize:nonceSize], result[:nonceSize], plaintext, []byte(s.keyID))

	return &service.EncryptResponse{
		// 最终密文 = nonce + Seal 输出。
		Ciphertext: result[:nonceSize+len(cipherText)],
		KeyID:      s.keyID,
		// 写入版本标记，解密时据此校验数据格式。
		Annotations: map[string][]byte{
			mockAnnotationKey: []byte("1"),
		},
	}, nil
}

// Decrypt 校验 annotations 与 key ID 后，用 HSM 密钥打开 AES-GCM 密文。
func (s *pkcs11RemoteService) Decrypt(ctx context.Context, uid string, req *service.DecryptRequest) ([]byte, error) {
	// 只允许恰好一个 annotation（版本标记）。
	if len(req.Annotations) != 1 {
		return nil, fmt.Errorf("invalid annotations")
	}
	if v, ok := req.Annotations[mockAnnotationKey]; !ok || string(v) != "1" {
		return nil, fmt.Errorf("invalid version in annotations")
	}

	// key ID 必须匹配，否则 AAD 校验也会失败，提前给出明确错误。
	if req.KeyID != s.keyID {
		return nil, fmt.Errorf("invalid keyID")
	}

	// 密文至少要包含完整 nonce。
	nonceSize := s.aead.NonceSize()
	data := req.Ciphertext
	if len(data) < nonceSize {
		return nil, fmt.Errorf("the stored data was shorter than the required size")
	}

	// 拆分 nonce 与密文，用与加密时相同的 keyID 作为 AAD 解密。
	return s.aead.Open(nil, data[:nonceSize], data[nonceSize:], []byte(s.keyID))
}

// Status 返回 mock 插件固定状态：版本 v2、健康、当前 key ID。
func (s *pkcs11RemoteService) Status(ctx context.Context) (*service.StatusResponse, error) {
	return &service.StatusResponse{
		Version: "v2",
		Healthz: "ok",
		KeyID:   s.keyID,
	}, nil
}
