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

import "context"

// Service allows encrypting and decrypting data using an external Key Management Service.
// Service 是外部 KMS 插件需要实现的业务接口，与具体的 gRPC 传输层解耦。
// GRPCService 负责把 gRPC 请求转换为本接口调用，并把结果包装回 protobuf 响应。
// uid 是请求的唯一标识，可用于插件侧的请求追踪与幂等控制。
type Service interface {
	// Decrypt a given bytearray to obtain the original data as bytes.
	// Decrypt 解密密文，返回原始明文。
	// req 中包含密文、加密时使用的 key ID 以及附加的 annotations。
	Decrypt(ctx context.Context, uid string, req *DecryptRequest) ([]byte, error)
	// Encrypt bytes to a ciphertext.
	// Encrypt 加密明文，返回密文、使用的 key ID 与 annotations。
	Encrypt(ctx context.Context, uid string, data []byte) (*EncryptResponse, error)
	// Status returns the status of the KMS.
	// Status 返回插件的健康状态与当前写入密钥信息，供 apiserver 轮询。
	Status(ctx context.Context) (*StatusResponse, error)
}

// EncryptResponse is the response from the Envelope service when encrypting data.
// EncryptResponse 是加密结果：密文、使用的 KMS KEK 标识，以及随密文一起存储的附加元数据。
type EncryptResponse struct {
	// Ciphertext 是加密后的密文字节。
	Ciphertext []byte
	// KeyID 是本次加密使用的 KMS 密钥标识（必须是 KEK，而非本地密钥）。
	KeyID string
	// Annotations 是随密文存储的附加元数据，以明文存在 etcd 中，敏感数据需由插件自行预加密。
	Annotations map[string][]byte
}

// DecryptRequest is the request to the Envelope service when decrypting data.
// DecryptRequest 是解密请求，携带待解密密文及加密时保存的 key ID 与 annotations。
type DecryptRequest struct {
	// Ciphertext 是待解密的密文。
	Ciphertext []byte
	// KeyID 是加密时记录在该数据上的密钥标识。
	KeyID string
	// Annotations 是加密时一同存储的附加元数据。
	Annotations map[string][]byte
}

// StatusResponse is the response from the Envelope service when getting the status of the service.
// StatusResponse 是 KMS 插件的状态信息，用于健康检查和判断数据是否因密钥轮换而过期。
type StatusResponse struct {
	// Version 是插件实现的 KMS gRPC API 版本。
	Version string
	// Healthz 为 "ok" 表示健康，其他值会被 apiserver 写入 healthz 错误信息。
	Healthz string
	// KeyID 是当前的写入密钥，用于判断经 TransformFromStorage 读取的数据是否过期。
	KeyID string
}
