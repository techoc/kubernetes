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

package aes

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"time"

	"golang.org/x/crypto/hkdf"

	"k8s.io/apiserver/pkg/storage/value"
	"k8s.io/utils/clock"
)

const (
	// cacheTTL is the TTL of KDF cache entries.  We assume that the value.Context.AuthenticatedData
	// for every call is the etcd storage path of the associated resource, and use that as the primary
	// cache key (with a secondary check that confirms that the info matches).  Thus if a client
	// is constantly creating resources with new names (and thus new paths), they will keep adding new
	// entries to the cache for up to this TTL before the GC logic starts deleting old entries.  Each
	// entry is ~300 bytes in size, so even a malicious client will be bounded in the overall memory
	// it can consume.
	// KDF 派生结果的缓存 TTL；以认证数据（通常是 etcd 存储路径）为主键，条目很小。
	cacheTTL = 10 * time.Minute

	// 下面三个常量都等于 commonSize（32 字节）：派生密钥长度、KDF info 长度与 seed 的最小长度。
	derivedKeySizeExtendedNonceGCM = commonSize
	infoSizeExtendedNonceGCM
	MinSeedSizeExtendedNonceGCM
)

// NewHKDFExtendedNonceGCMTransformer is the same as NewGCMTransformer but trades storage,
// memory and CPU to work around the limitations of AES-GCM's 12 byte nonce size.  The input seed
// is assumed to be a cryptographically strong slice of MinSeedSizeExtendedNonceGCM+ random bytes.
// Unlike NewGCMTransformer, this function is immune to the birthday attack because a new key is generated
// per encryption via a key derivation function: KDF(seed, random_bytes) -> key.  The derived key is
// only used once as an AES-GCM key with a random 12 byte nonce.  This avoids any concerns around
// cryptographic wear out (by either number of encryptions or the amount of data being encrypted).
// Speaking on the cryptographic safety, the limit on the number of operations that can be preformed
// with a single seed with derived keys and randomly generated nonces is not practically reachable.
// Thus, the scheme does not impose any specific requirements on the seed rotation schedule.
// Reusing the same seed is safe to do over time and across process restarts.  Whenever a new
// seed is needed, the caller should generate it via GenerateKey(MinSeedSizeExtendedNonceGCM).
// In regard to KMSv2, organization standards or compliance policies around rotation may require
// that the seed be rotated at some interval.  This can be implemented externally by rotating
// the key encryption key via a key ID change.
// NewHKDFExtendedNonceGCMTransformer 用 KDF 为每次加密派生一次性密钥，绕开 AES-GCM
// 12 字节 nonce 的生日界限制。seed 必须至少为 MinSeedSizeExtendedNonceGCM 字节。
func NewHKDFExtendedNonceGCMTransformer(seed []byte) (value.Transformer, error) {
	if seedLen := len(seed); seedLen < MinSeedSizeExtendedNonceGCM {
		// seed 过短会削弱 KDF 的安全边际，直接拒绝。
		return nil, fmt.Errorf("invalid seed length %d used for key generation", seedLen)
	}
	return &extendedNonceGCM{
		seed:  seed,
		cache: newSimpleCache(clock.RealClock{}, cacheTTL), // 缓存派生结果，避免重复读时重算 KDF；写路径总是派生新密钥。
	}, nil
}

// extendedNonceGCM 用固定 seed 加上每次加密的随机 info 派生一次性 AES-GCM 密钥。
type extendedNonceGCM struct {
	seed  []byte
	cache *simpleCache
}

// TransformFromStorage 从数据头部取回 KDF info，派生读密钥后解密。
// 前 infoSizeExtendedNonceGCM 字节是明文 info，不属于密文。
func (e *extendedNonceGCM) TransformFromStorage(ctx context.Context, data []byte, dataCtx value.Context) ([]byte, bool, error) {
	if len(data) < infoSizeExtendedNonceGCM {
		// 连 info 都不完整，无法派生密钥。
		return nil, false, errors.New("the stored data was shorter than the required size")
	}

	info := data[:infoSizeExtendedNonceGCM]

	// 读路径允许命中缓存，同一 info 的 transformer 可以复用。
	transformer, err := e.derivedKeyTransformer(info, dataCtx, false)
	if err != nil {
		return nil, false, fmt.Errorf("failed to derive read key from KDF: %w", err)
	}

	return transformer.TransformFromStorage(ctx, data, dataCtx)
}

// TransformToStorage 生成随机 info 并派生新密钥；写路径不复用缓存，
// 保证每次加密都使用独立密钥与随机 nonce。
func (e *extendedNonceGCM) TransformToStorage(ctx context.Context, data []byte, dataCtx value.Context) ([]byte, error) {
	info := make([]byte, infoSizeExtendedNonceGCM)
	if err := randomNonce(info); err != nil {
		// info 必须具备密码学随机性，否则派生密钥可能重复。
		return nil, fmt.Errorf("failed to generate info for KDF: %w", err)
	}

	transformer, err := e.derivedKeyTransformer(info, dataCtx, true)
	if err != nil {
		return nil, fmt.Errorf("failed to derive write key from KDF: %w", err)
	}

	return transformer.TransformToStorage(ctx, data, dataCtx)
}

// derivedKeyTransformer 返回与 info 对应的 transformer；读路径优先查缓存，写路径总是新建。
func (e *extendedNonceGCM) derivedKeyTransformer(info []byte, dataCtx value.Context, write bool) (value.Transformer, error) {
	if !write { // no need to check cache on write since we always generate a new transformer
		// 读命中缓存可省去 KDF 与 AES 初始化开销。
		if transformer := e.cache.get(info, dataCtx); transformer != nil {
			return transformer, nil
		}

		// on read, this is a subslice of a much larger slice and we do not want to hold onto that larger slice
		// 克隆后缓存才不会拖住底层整块数据，避免内存泄漏。
		info = bytes.Clone(info)
	}

	// 只用 HKDF-Expand：seed 已由调用方保证是强随机，无需 Extract。
	key, err := e.sha256KDFExpandOnly(info)
	if err != nil {
		return nil, fmt.Errorf("failed to KDF expand seed with info: %w", err)
	}

	// 用派生密钥构建 GCM transformer，并把 info 作为前缀一并写入。
	transformer, err := newGCMTransformerWithInfo(key, info)
	if err != nil {
		return nil, fmt.Errorf("failed to build transformer with KDF derived key: %w", err)
	}

	e.cache.set(dataCtx, transformer)

	return transformer, nil
}

// sha256KDFExpandOnly 以 info 作为 HKDF info，从 seed 扩展出固定长度密钥。
// seed 已是强随机，故省略 Extract 步骤。
func (e *extendedNonceGCM) sha256KDFExpandOnly(info []byte) ([]byte, error) {
	kdf := hkdf.Expand(sha256.New, e.seed, info)

	derivedKey := make([]byte, derivedKeySizeExtendedNonceGCM)
	if _, err := io.ReadFull(kdf, derivedKey); err != nil {
		// HKDF 是确定性流，正常不会失败；保留错误以防御实现异常。
		return nil, fmt.Errorf("failed to read a derived key from KDF: %w", err)
	}

	return derivedKey, nil
}

// newGCMTransformerWithInfo 用派生密钥构建 GCM transformer，并记录 info 供写入时加前缀。
func newGCMTransformerWithInfo(key, info []byte) (*transformerWithInfo, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	transformer, err := NewGCMTransformer(block)
	if err != nil {
		return nil, err
	}

	return &transformerWithInfo{transformer: transformer, info: info}, nil
}

// transformerWithInfo 把 info 前缀与底层 GCM transformer 组合：
// 写入先写 info 再写密文，读取时校验并剥离前缀。
type transformerWithInfo struct {
	transformer value.Transformer
	// info are extra opaque bytes prepended to the writes from transformer and stripped from reads.
	// currently info is used to generate a key via KDF(seed, info) -> key
	// and transformer is the output of NewGCMTransformer(aes.NewCipher(key))
	// info 是可透传的附加字节，当前用于 KDF(seed, info) 派生密钥。
	info []byte
}

// TransformFromStorage 校验并剥离 info 前缀后交给底层 transformer 解密。
func (t *transformerWithInfo) TransformFromStorage(ctx context.Context, data []byte, dataCtx value.Context) ([]byte, bool, error) {
	if !bytes.HasPrefix(data, t.info) {
		// 前缀不符说明数据由不同的 info（因而不同的密钥）加密，无法解密。
		return nil, false, errors.New("the stored data is missing the required info prefix")
	}

	return t.transformer.TransformFromStorage(ctx, data[len(t.info):], dataCtx)
}

// TransformToStorage 先加密，再把 info 前缀拼到密文前，供读取端派生同一密钥。
func (t *transformerWithInfo) TransformToStorage(ctx context.Context, data []byte, dataCtx value.Context) ([]byte, error) {
	out, err := t.transformer.TransformToStorage(ctx, data, dataCtx)
	if err != nil {
		return nil, err
	}

	// 预分配 info+密文总长度，避免 append 过程中反复扩容。
	outWithInfo := make([]byte, 0, len(out)+len(t.info))
	outWithInfo = append(outWithInfo, t.info...)
	outWithInfo = append(outWithInfo, out...)

	return outWithInfo, nil
}
