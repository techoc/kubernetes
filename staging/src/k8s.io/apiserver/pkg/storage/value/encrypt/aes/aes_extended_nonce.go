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
	// cacheTTL 是 KDF 缓存条目的存活时间。我们假设每次调用
	// value.Context.AuthenticatedData 得到的都是对应资源的 etcd 存储路径，并把它作为
	// 主缓存键（另有二次检查确认 info 与之匹配）。因此若客户端不断创建新名称（从而
	// 新路径）的资源，就会持续向缓存添加新条目，直到达到该 TTL 后 GC 逻辑才开始删除
	// 旧条目。每个条目约 300 字节，所以即使恶意客户端所能消耗的总内存也有上界。
	cacheTTL = 10 * time.Minute

	// 下面三个常量都等于 commonSize（32 字节）：派生密钥长度、KDF info 长度与 seed 的最小长度。
	derivedKeySizeExtendedNonceGCM = commonSize
	infoSizeExtendedNonceGCM
	MinSeedSizeExtendedNonceGCM
)

// NewHKDFExtendedNonceGCMTransformer 与 NewGCMTransformer 相同，但以存储、内存和 CPU
// 开销为代价来绕开 AES-GCM 12 字节 nonce 的限制。输入 seed 应当是密码学强随机的、长度
// 至少为 MinSeedSizeExtendedNonceGCM 字节的随机切片。与 NewGCMTransformer 不同，本函数
// 不受生日攻击影响，因为每次加密都会通过密钥派生函数生成新密钥：
// KDF(seed, random_bytes) -> key。派生出的密钥只作为 AES-GCM 密钥使用一次，并配合随机
// 生成的 12 字节 nonce，因此不存在密码学损耗（无论是加密次数还是加密数据量）方面的顾虑。
// 就密码学安全性而言，用单个 seed 派生密钥并配合随机 nonce 可执行的操作次数上限在实践中
// 无法达到，所以该方案不对 seed 的轮换周期提出特定要求：随时间推移以及进程重启之间重复
// 使用同一个 seed 是安全的。需要新 seed 时，调用方应通过
// GenerateKey(MinSeedSizeExtendedNonceGCM) 生成。就 KMSv2 而言，组织规范或合规策略可能
// 要求按一定周期轮换 seed，这可以在外部通过变更 keyID 来轮换密钥加密密钥实现。
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

		// 读取时 info 只是一个更大切片的子切片，我们不想继续持有那个更大的切片，
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
	// info 是附加的不透明字节：写入时拼接在 transformer 的输出之前，读取时被剥离。
	// 目前 info 用于通过 KDF(seed, info) -> key 派生密钥，而 transformer 则是
	// NewGCMTransformer(aes.NewCipher(key)) 的输出。
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
