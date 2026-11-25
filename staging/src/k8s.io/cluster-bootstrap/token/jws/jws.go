/*
Copyright 2016 The Kubernetes Authors.

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

// Package jws 实现 detached JWS（RFC 7515 附录 F）签名，用于让新节点在
// 信任集群 CA 之前验证 cluster-info ConfigMap 未被中间人篡改。
//
// 签名以 bootstrap token 的 ID 作为 JWK KeyID、以 token Secret 作为
// HMAC 密钥（对称密钥），因此只有持有同一 token 的节点才能验证签名。
// “detached” 指签名载荷省略内容部分，签名结果仍随 ConfigMap 发布，
// 而内容本身由配置的 Data 提供。
package jws

import (
	"fmt"
	"strings"

	jose "gopkg.in/go-jose/go-jose.v2"
)

// ComputeDetachedSignature 按 RFC 7515 附录 F 为内容生成 detached JWS 签名，
// 返回 "<protected>..<signature>" 形式的紧凑序列化。
// tokenSecret 是对称 HMAC 密钥，必须保持机密；实现和错误路径都不得泄露它。
func ComputeDetachedSignature(content, tokenID, tokenSecret string) (string, error) {
	// 把 Secret 包装成 JSONWebKey，并用公开的 token ID 作为 KeyID，
	// 使验证方无需额外信息即可定位对应 token。
	jwk := &jose.JSONWebKey{
		Key:   []byte(tokenSecret),
		KeyID: tokenID,
	}

	opts := &jose.SignerOptions{
		// go-jose 不会为对称密钥自动写入 KeyID，必须显式加入受保护头，
		// 否则验证方无法把签名对应到 bootstrap token ID。
		ExtraHeaders: map[jose.HeaderKey]interface{}{
			"kid": tokenID,
		},
	}

	// HS256 是对称算法，验证方必须持有相同的 Secret，符合 bootstrap
	// token 的共享密钥模型。
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.HS256, Key: jwk}, opts)
	if err != nil {
		return "", fmt.Errorf("can't make a HS256 signer from the given token: %v", err)
	}

	jws, err := signer.Sign([]byte(content))
	if err != nil {
		return "", fmt.Errorf("can't HS256-sign the given token: %v", err)
	}

	// 先做完整的紧凑序列化，再剥掉内容段得到 detached 形式。
	fullSig, err := jws.CompactSerialize()
	if err != nil {
		return "", fmt.Errorf("can't serialize the given token: %v", err)
	}
	return stripContent(fullSig)
}

// stripContent 移除紧凑 JWS 的载荷段。
// go-jose 不直接生成 detached 签名，因此先完整序列化再拆分为
// protected.payload.signature 三段，最后以 "<protected>..<signature>" 重组。
func stripContent(fullSig string) (string, error) {
	parts := strings.Split(fullSig, ".")
	if len(parts) != 3 {
		return "", fmt.Errorf("compact JWS format must have three parts")
	}

	return parts[0] + ".." + parts[2], nil
}

// DetachedTokenIsValid 重新计算签名并比较，判断 detached JWS 是否与给定内容和 token 匹配。
// 比较对象是公开签名，不是 token Secret；重新计算失败时按无效处理。
func DetachedTokenIsValid(detachedToken, content, tokenID, tokenSecret string) bool {
	newToken, err := ComputeDetachedSignature(content, tokenID, tokenSecret)
	if err != nil {
		return false
	}
	return detachedToken == newToken
}
