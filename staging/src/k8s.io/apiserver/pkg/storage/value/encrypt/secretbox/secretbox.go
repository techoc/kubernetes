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

// Package secretbox 使用 XSalsa20 与 Poly1305 对静态存储的数据做加解密。
package secretbox

import (
	"context"
	"crypto/rand"
	"fmt"

	"golang.org/x/crypto/nacl/secretbox"

	"k8s.io/apiserver/pkg/storage/value"
)

// secretbox 在给定 32 字节密钥下实现数据的静态加密。
// 使用 crypto/rand 生成的标准 24 字节 nonce（置于密文开头），
// 不对静态数据做与存储上下文的认证绑定。
// 密文格式为 nonce || ciphertext，secretbox 自身保证完整性，
// 但这里不做与存储上下文（资源/键）的绑定，因此不同资源的密文可互换。
type secretboxTransformer struct {
	key [32]byte
}

const nonceSize = 24

// NewSecretboxTransformer 用给定密钥构造可对数据加解密的 transformer。
func NewSecretboxTransformer(key [32]byte) value.Transformer {
	return &secretboxTransformer{key: key}
}

// TransformFromStorage 从密文头部取出 nonce 后解密；长度不足或认证失败
// 都返回错误，且第二个返回值恒为 false（不涉及密钥轮换/重写提示）。
func (t *secretboxTransformer) TransformFromStorage(ctx context.Context, data []byte, dataCtx value.Context) ([]byte, bool, error) {
	if len(data) < (secretbox.Overhead + nonceSize) {
		return nil, false, fmt.Errorf("the stored data was shorter than the required size")
	}
	var nonce [nonceSize]byte
	copy(nonce[:], data[:nonceSize])
	data = data[nonceSize:]
	out := make([]byte, 0, len(data)-secretbox.Overhead)
	result, ok := secretbox.Open(out, data, &nonce, &t.key)
	if !ok {
		return nil, false, fmt.Errorf("output array was not large enough for encryption")
	}
	return result, false, nil
}

// TransformToStorage 为每条数据生成新的随机 nonce，并输出 nonce || 密文。
// nonce 绝不可复用，否则会破坏 secretbox 的机密性保证。
func (t *secretboxTransformer) TransformToStorage(ctx context.Context, data []byte, dataCtx value.Context) ([]byte, error) {
	var nonce [nonceSize]byte
	n, err := rand.Read(nonce[:])
	if err != nil {
		return nil, err
	}
	// crypto/rand.Read 理论上可能返回短读，必须显式校验。
	if n != nonceSize {
		return nil, fmt.Errorf("unable to read sufficient random bytes")
	}
	return secretbox.Seal(nonce[:], data, &nonce, &t.key), nil
}
