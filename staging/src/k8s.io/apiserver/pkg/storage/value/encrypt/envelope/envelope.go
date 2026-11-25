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

// Package envelope 使用 Envelope provider 转换落盘存储的值。
// envelope 实现 KEK/DEK 信封加密：KMS 保管 KEK，每次写入生成随机 DEK，
// 用 KEK 加密 DEK 后与密文拼接存储；读取时先解出 DEK 再解密数据。DEK
// 以密文形式作为缓存键，避免把明文密钥长期留在进程内存。
package envelope

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"time"

	"k8s.io/apiserver/pkg/storage/value"
	"k8s.io/apiserver/pkg/storage/value/encrypt/envelope/metrics"
	"k8s.io/utils/lru"

	"golang.org/x/crypto/cryptobyte"
)

func init() {
	// 数据变换层与信封加密层各自注册指标；init 保证即使未显式初始化，
	// 指标也已存在，避免运行期重复注册 panic。
	value.RegisterMetrics()
	metrics.RegisterMetrics()
}

// Service 表示借助外部 Key Management Service 对数据加解密的能力。
// Service 是 KMS 的最小接口：只做 KEK 加解密，DEK 的生成与数据加解密由
// transformer 负责。错误信息需保留原始状态码供指标分类，调用方不应包装。
type Service interface {
	// Decrypt a given bytearray to obtain the original data as bytes.
	Decrypt(data []byte) ([]byte, error)
	// Encrypt bytes to a ciphertext.
	Encrypt(data []byte) ([]byte, error)
}

// envelopeTransformer 实现 value.Transformer：密文布局为
// [uint16 长度][加密后的 DEK][AES-GCM 密文]。
type envelopeTransformer struct {
	envelopeService Service

	// transformers 是线程安全的 LRU 缓存，以 DEK 的加密形式为索引缓存已解密的 DEK。
	// 键是加密后的 DEK，因此无需担心缓存键泄露明文密钥。
	transformers *lru.Cache

	// baseTransformerFunc creates a new transformer for encrypting the data with the DEK.
	baseTransformerFunc func(cipher.Block) (value.Transformer, error)

	cacheSize    int
	cacheEnabled bool
}

// NewEnvelopeTransformer 返回实现 KEK-DEK 信封加密方案的 transformer。它使用
// envelopeService 加解密 DEK，并把相应的 DEK（加密形式）拼接在所加密数据项之前；
// 同时维护一个大小为 cacheSize 的缓存，在内存中保存最近使用过的已解密 DEK。
// cacheSize<=0 时禁用缓存，每次读取都要调用 KMS 解密 DEK（可用但延迟高）。
func NewEnvelopeTransformer(envelopeService Service, cacheSize int, baseTransformerFunc func(cipher.Block) (value.Transformer, error)) value.Transformer {
	var (
		cache *lru.Cache
	)

	if cacheSize > 0 {
		cache = lru.New(cacheSize)
	}
	return &envelopeTransformer{
		envelopeService:     envelopeService,
		transformers:        cache,
		baseTransformerFunc: baseTransformerFunc,
		cacheEnabled:        cacheSize > 0,
		cacheSize:           cacheSize,
	}
}

// TransformFromStorage 使用信封解密本 transformer 所加密的数据。
// 先解析长度前缀得到加密 DEK，再查缓存或请求 KMS，最后用 DEK 解数据；
// 任何一步失败都返回 false 表示“未成功解密”，由上层决定是否回退。
func (t *envelopeTransformer) TransformFromStorage(ctx context.Context, data []byte, dataCtx value.Context) ([]byte, bool, error) {
	metrics.RecordArrival(metrics.FromStorageLabel, time.Now())

	// Read the 16 bit length-of-DEK encoded at the start of the encrypted DEK. 16 bits can
	// represent a maximum key length of 65536 bytes. We are using a 256 bit key, whose
	// length cannot fit in 8 bits (1 byte). Thus, we use 16 bits (2 bytes) to store the length.
	var encKey cryptobyte.String
	s := cryptobyte.String(data)
	if ok := s.ReadUint16LengthPrefixed(&encKey); !ok {
		return nil, false, fmt.Errorf("invalid data encountered by envelope transformer: failed to read uint16 length prefixed data")
	}

	// 长度前缀之后的全部字节即 DEK 加密的数据密文。
	encData := []byte(s)

	// Look up the decrypted DEK from cache or Envelope.
	transformer := t.getTransformer(encKey)
	if transformer == nil {
		if t.cacheEnabled {
			value.RecordCacheMiss()
		}
		key, err := t.envelopeService.Decrypt(encKey)
		if err != nil {
			// Do NOT wrap this err using fmt.Errorf() or similar functions
			// because this gRPC status error has useful error code when
			// record the metric.
			return nil, false, err
		}

		transformer, err = t.addTransformer(encKey, key)
		if err != nil {
			return nil, false, err
		}
	}

	return transformer.TransformFromStorage(ctx, encData, dataCtx)
}

// TransformToStorage 使用信封加密对待写入磁盘的数据加密。
// 每次写入使用全新的随机 DEK，因此相同明文得到不同密文（避免等值泄露），
// 也无需维护 nonce 计数器跨重启的状态。
func (t *envelopeTransformer) TransformToStorage(ctx context.Context, data []byte, dataCtx value.Context) ([]byte, error) {
	metrics.RecordArrival(metrics.ToStorageLabel, time.Now())
	newKey, err := generateKey(32)
	if err != nil {
		return nil, err
	}

	encKey, err := t.envelopeService.Encrypt(newKey)
	if err != nil {
		// Do NOT wrap this err using fmt.Errorf() or similar functions
		// because this gRPC status error has useful error code when
		// record the metric.
		return nil, err
	}

	transformer, err := t.addTransformer(encKey, newKey)
	if err != nil {
		return nil, err
	}

	result, err := transformer.TransformToStorage(ctx, data, dataCtx)
	if err != nil {
		return nil, err
	}
	// 把加密后 DEK 的长度作为前 2 个字节追加进去。
	// 布局与 TransformFromStorage 的解析逻辑严格对应，改动需两边同步。
	b := cryptobyte.NewBuilder(nil)
	b.AddUint16LengthPrefixed(func(b *cryptobyte.Builder) {
		b.AddBytes([]byte(encKey))
	})
	b.AddBytes(result)

	return b.Bytes()
}

// 编译期断言：信封转换器必须满足 value.Transformer。
var _ value.Transformer = &envelopeTransformer{}

// addTransformer inserts a new transformer to the Envelope cache of DEKs for future reads.
func (t *envelopeTransformer) addTransformer(encKey []byte, key []byte) (value.Transformer, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	transformer, err := t.baseTransformerFunc(block)
	if err != nil {
		return nil, err
	}

	// 使用 encKey 的 base64 编码作为缓存键，因为 hashicorp/golang-lru 无法对
	// []uint8 计算哈希。缓存填充率用于观察缓存是否过小导致频繁回源 KMS。
	if t.cacheEnabled {
		t.transformers.Add(base64.StdEncoding.EncodeToString(encKey), transformer)
		metrics.RecordDekCacheFillPercent(float64(t.transformers.Len()) / float64(t.cacheSize))
	}
	return transformer, nil
}

// getTransformer 若缓存中存在与 encKey 对应的 transformer 则将其取出。
// 未启用缓存直接返回 nil，让调用方走 KMS 解密路径。
func (t *envelopeTransformer) getTransformer(encKey []byte) value.Transformer {
	if !t.cacheEnabled {
		return nil
	}

	_transformer, found := t.transformers.Get(base64.StdEncoding.EncodeToString(encKey))
	if found {
		return _transformer.(value.Transformer)
	}
	return nil
}

// generateKey 使用系统随机源生成长度随机的密钥。
// 使用 crypto/rand 而非伪随机数：DEK 的不可预测性是信封加密安全性的基础。
func generateKey(length int) (key []byte, err error) {
	defer func(start time.Time) {
		value.RecordDataKeyGeneration(start, err)
	}(time.Now())
	key = make([]byte, length)
	if _, err = rand.Read(key); err != nil {
		return nil, err
	}

	return key, nil
}
