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

// ComputeDetachedSignature takes content and token details and computes a detached
// JWS signature.  This is described in Appendix F of RFC 7515.  Basically, this
// is a regular JWS with the content part of the signature elided.
//
// 返回值为 "<protected>..<signature>" 形式的紧凑序列化。tokenSecret 是
// 对称 HMAC 密钥，必须保持机密；任何能把 Secret 泄露到日志或错误中的
// 改动都会削弱引导信任。
func ComputeDetachedSignature(content, tokenID, tokenSecret string) (string, error) {
	// 把 Secret 包装成 JSONWebKey，并用公开的 token ID 作为 KeyID，
	// 使验证方无需额外信息即可定位对应 token。
	jwk := &jose.JSONWebKey{
		Key:   []byte(tokenSecret),
		KeyID: tokenID,
	}

	opts := &jose.SignerOptions{
		// Since this is a symmetric key, go-jose doesn't automatically include
		// the KeyID as part of the protected header. We have to pass it here
		// explicitly.
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

// stripContent will remove the content part of a compact JWS
//
// The `go-jose` library doesn't support generating signatures with "detached"
// content. To make up for this we take the full compact signature, break it
// apart and put it back together without the content section.
//
// 紧凑 JWS 由三部分组成（protected.payload.signature）；detached 形式把
// 中间的 payload 置空，从而保持 "<protected>..<signature>" 的合法结构。
func stripContent(fullSig string) (string, error) {
	parts := strings.Split(fullSig, ".")
	if len(parts) != 3 {
		return "", fmt.Errorf("compact JWS format must have three parts")
	}

	return parts[0] + ".." + parts[2], nil
}

// DetachedTokenIsValid checks whether a given detached JWS-encoded token matches JWS output of the given content and token
//
// 通过重新计算签名再做字符串比较来验证；因为签名由 HMAC 决定，未知密钥
// 无法构造出匹配值。这里的比较本身不是常量时间的，但只涉及公开的签名，
// 不泄露 Secret。
func DetachedTokenIsValid(detachedToken, content, tokenID, tokenSecret string) bool {
	newToken, err := ComputeDetachedSignature(content, tokenID, tokenSecret)
	if err != nil {
		return false
	}
	return detachedToken == newToken
}
