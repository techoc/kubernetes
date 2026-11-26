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

// Package aes 用 AES-GCM 对静态存储的数据做加解密转换。
//
// 同时保留 CBC 实现，仅用于读取历史数据；新写入一律使用 GCM。
package aes

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"k8s.io/apiserver/pkg/storage/value"
	"k8s.io/klog/v2"
)

// commonSize 是各类安全敏感字节切片（如加密密钥）的长度。
// 不要改动此值，否则会破坏向后兼容。
//
// 32 字节即 AES-256 密钥长度；改动会导致已加密数据无法解密。
const commonSize = 32

// keySizeCounterNonceGCM 是计数器型 nonce 模式要求的密钥长度，与 commonSize 保持一致。
const keySizeCounterNonceGCM = commonSize

// NewGCMTransformerWithUniqueKeyUnsafe 与 NewGCMTransformer 功能相同，但不适合通用场景：
// 它对底层块密码的密钥作了假设。具体而言，它使用 96 位 nonce，
// 前 32 位随机、后 64 位为单调递增的原子计数器。这意味着密钥必须在进程启动时随机生成，
// 且绝不能在进程生命周期之外用于加密。
// 与 NewGCMTransformer 不同，该函数不受生日攻击影响，因此密钥可
// 用于 2^64-1 次写入而无需轮换。此外，顺序 nonce 下 AES-GCM 的密码学磨损出现在
// 2^64 次加密之后，对我们的用例不构成问题。
// 即便发生，nonce 计数器也会回绕并使进程崩溃。所有存储项都很小（<2 MB），
// 因此不担心明文长度。为防止块密码被意外复用，它不作为输入传入；
// 每次调用都会生成一个新的随机 key 并返回。该 key 用作块密码的输入。
// 若把 key 保存下来，日后可传给 NewGCMTransformer(aes.NewCipher(key))
// 构造能解密本 transformer 加密数据的转换器（但那个转换器不得用于加密）。
//
// 关键安全性质：nonce 由随机前缀 + 单调递增计数器组成，因此不存在随机 nonce 的生日碰撞问题，
// 同一密钥可安全使用至 2^64 次写入。代价是密钥不能持久化后复用，只能随进程生命周期存在。
//
// 关键安全性质：nonce 由随机前缀 + 单调递增计数器组成，因此不存在随机 nonce 的生日碰撞问题，
// 同一密钥可安全使用至 2^64 次写入。代价是密钥不能持久化后复用，只能随进程生命周期存在。
// 返回的 key 供持久化以便日后解密（不可再用于加密）。
func NewGCMTransformerWithUniqueKeyUnsafe() (value.Transformer, []byte, error) {
	key, err := GenerateKey(keySizeCounterNonceGCM)
	if err != nil {
		return nil, nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, nil, err
	}

	nonceGen := &nonceGenerator{
		// 计数器从十亿开始，以便在不同 goroutine 之间
		// 可靠检测回绕。
		//
		// 起点远高于 0，使计数器回绕时必然落到 <= zero 的区间从而被检测到。
		zero:  1_000_000_000,
		fatal: die,
	}
	nonceGen.nonce.Add(nonceGen.zero)

	transformer, err := newGCMTransformerWithUniqueKeyUnsafe(block, nonceGen)
	if err != nil {
		return nil, nil, err
	}
	return transformer, key, nil
}

// newGCMTransformerWithUniqueKeyUnsafe 用给定的块密码与 nonce 生成器构造 GCM 转换器。
// 分离出来是为了让测试可以注入自定义 nonceGenerator（例如模拟溢出）。
func newGCMTransformerWithUniqueKeyUnsafe(block cipher.Block, nonceGen *nonceGenerator) (value.Transformer, error) {
	aead, err := newGCM(block)
	if err != nil {
		return nil, err
	}

	nonceFunc := func(b []byte) error {
		// 只需 8 字节存储 64 位递增 nonce；
		// 把未使用的字节设为随机位而不是零，
		// 主要防范 VM 恢复导致原子计数器回退之类的诡异边界情况。
		//
		// 随机前缀 + 递增后缀：即便计数器因 VM 回滚而重复，随机部分也能避免 nonce 完全碰撞。
		randNonceSize := len(b) - 8

		if err := randomNonce(b[:randNonceSize]); err != nil {
			return err
		}

		nonceGen.next(b[randNonceSize:])

		return nil
	}

	return &gcm{aead: aead, nonceFunc: nonceFunc}, nil
}

// randomNonce 用系统熵源填充 nonce 前缀。
func randomNonce(b []byte) error {
	_, err := rand.Read(b)
	return err
}

// nonceGenerator 生成 64 位单调递增 nonce，并在回绕时终止进程。
type nonceGenerator struct {
	// 即便每秒一百万次加密，该计数器也够用五十万年。
	// 使用本结构体是为了避免对齐问题：https://pkg.go.dev/sync/atomic#pkg-note-BUG
	//
	// 用 atomic.Uint64 而非裸 uint64 + atomic 包函数，避免 32 位平台上的对齐问题。
	nonce atomic.Uint64
	zero  uint64
	fatal func(msg string)
}

// next 自增并把新值以小端序写入 b；回绕意味着密码学寿命耗尽，必须致命退出。
func (n *nonceGenerator) next(b []byte) {
	incrementingNonce := n.nonce.Add(1)
	if incrementingNonce <= n.zero {
		// 这不应发生，一旦发生便无法恢复。
		n.fatal("aes-gcm detected nonce overflow - cryptographic wear out has occurred")
	}
	binary.LittleEndian.PutUint64(b, incrementingNonce)
}

// die 以 Fatal 级别终止进程；保留 klog.FatalDepth 以便输出可读的调用栈。
func die(msg string) {
	// nolint:logcheck // we want the stack traces, log flushing, and process exiting logic from FatalDepth
	klog.FatalDepth(1, msg)
}

// GenerateKey 用系统随机源生成一个随机 key。
//
// 用 defer 记录密钥生成耗时与错误，供 KMS/加密相关指标使用。
func GenerateKey(length int) (key []byte, err error) {
	defer func(start time.Time) {
		value.RecordDataKeyGeneration(start, err)
	}(time.Now())
	key = make([]byte, length)
	if _, err = rand.Read(key); err != nil {
		return nil, err
	}

	return key, nil
}

// NewGCMTransformer 用给定的块密码对数据进行加解密。
// 它基于 cipher.Block 算法实现 AEAD 加密。
// 通过 value.Context 提供的认证数据在对象写入与读取时必须一致。为确保攻击者无法从
// 自己控制的位置复制值，应使用存储位置的特征（如 etcd key）作为认证数据的一部分。
//
// 该模式需要生成 IV，而 IV 重用是 AES-GCM 的已知弱点，因此必须在生日攻击可行之前
// 轮换密钥。NIST SP 800-38D
// (http://csrc.nist.gov/publications/nistpubs/800-38D/SP-800-38D.pdf) 建议同一个 key 配合
// 随机 96 位 nonce（默认长度）使用不超过 2^32 次，因此采用本实现的转换器必须允许频繁轮换密钥。
// 后续工作应研究用 AES-GCM-SIV 替代随机 nonce。
//
// 因此采用本实现的转换器必须允许频繁轮换密钥。
// 后续应研究用 AES-GCM-SIV 替代随机 nonce。
// random nonces.
//
// 与计数器版本不同，这里的 nonce 完全随机，因此密钥必须频繁轮换（NIST 建议 ≤2^32 次），
// 否则 IV 重用会同时破坏机密性与完整性。
func NewGCMTransformer(block cipher.Block) (value.Transformer, error) {
	aead, err := newGCM(block)
	if err != nil {
		return nil, err
	}

	return &gcm{aead: aead, nonceFunc: randomNonce}, nil
}

// newGCM 创建 GCM AEAD 并强制校验 nonce 长度为 12。
// 该断言保护存量数据：nonce 长度改变会让所有已写入 etcd 的密文无法解密。
func newGCM(block cipher.Block) (cipher.AEAD, error) {
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if nonceSize := aead.NonceSize(); nonceSize != 12 { // 若改动此值，etcd 中的所有数据都会失效。
		return nil, fmt.Errorf("crypto/cipher.NewGCM returned unexpected nonce size: %d", nonceSize)
	}
	return aead, nil
}

// gcm 是 GCM 模式的 value.Transformer；nonceFunc 决定 nonce 的生成策略。
type gcm struct {
	aead      cipher.AEAD
	nonceFunc func([]byte) error
}

// TransformFromStorage 解密存储中的密文。密文布局为 nonce || ciphertext+tag。
// 返回的 bool 恒为 false：本实现不提供“数据是否已过时/需重写”的语义。
func (t *gcm) TransformFromStorage(ctx context.Context, data []byte, dataCtx value.Context) ([]byte, bool, error) {
	nonceSize := t.aead.NonceSize()
	if len(data) < nonceSize {
		return nil, false, errors.New("the stored data was shorter than the required size")
	}
	result, err := t.aead.Open(nil, data[:nonceSize], data[nonceSize:], dataCtx.AuthenticatedData())
	return result, false, err
}

// TransformToStorage 加密数据，输出 nonce || ciphertext+tag。
// 一次性分配完整缓冲区，nonce 与密文写入同一底层数组，减少拷贝。
func (t *gcm) TransformToStorage(ctx context.Context, data []byte, dataCtx value.Context) ([]byte, error) {
	nonceSize := t.aead.NonceSize()
	result := make([]byte, nonceSize+t.aead.Overhead()+len(data))

	if err := t.nonceFunc(result[:nonceSize]); err != nil {
		return nil, fmt.Errorf("failed to write nonce for AES-GCM: %w", err)
	}

	cipherText := t.aead.Seal(result[nonceSize:nonceSize], result[:nonceSize], data, dataCtx.AuthenticatedData())
	return result[:nonceSize+len(cipherText)], nil
}

// cbc 用给定的块密码对存储值做静态加密。
//
// 仅用于向后兼容：读取早期版本写入的 CBC 数据。CBC 无认证，故不作为写入格式。
type cbc struct {
	block cipher.Block
}

// NewCBCTransformer 用给定的块密码对数据进行加解密。
// data.
func NewCBCTransformer(block cipher.Block) value.Transformer {
	return &cbc{block: block}
}

// CBC 解码过程中的哨兵错误，便于调用方区分“数据损坏”与“密钥错误”。
var (
	errInvalidBlockSize    = errors.New("the stored data is not a multiple of the block size")
	errInvalidPKCS7Data    = errors.New("invalid PKCS7 data (empty or not padded)")
	errInvalidPKCS7Padding = errors.New("invalid padding on input")
)

// TransformFromStorage 解密 CBC 密文（IV || ciphertext），并校验 PKCS#7 填充。
// 填充校验逐字节比较，避免把填充错误误判为数据损坏。
func (t *cbc) TransformFromStorage(ctx context.Context, data []byte, dataCtx value.Context) ([]byte, bool, error) {
	blockSize := aes.BlockSize
	if len(data) < blockSize {
		return nil, false, errors.New("the stored data was shorter than the required size")
	}
	iv := data[:blockSize]
	data = data[blockSize:]

	if len(data)%blockSize != 0 {
		return nil, false, errInvalidBlockSize
	}

	result := make([]byte, len(data))
	copy(result, data)
	mode := cipher.NewCBCDecrypter(t.block, iv)
	mode.CryptBlocks(result, result)

	// 移除并校验 CBC 的 PKCS#7 填充。
	c := result[len(result)-1]
	paddingSize := int(c)
	size := len(result) - paddingSize
	if paddingSize == 0 || paddingSize > len(result) {
		return nil, false, errInvalidPKCS7Data
	}
	for i := 0; i < paddingSize; i++ {
		if result[size+i] != c {
			return nil, false, errInvalidPKCS7Padding
		}
	}

	return result[:size], false, nil
}

// TransformToStorage 加密数据为 IV || ciphertext，并按 PKCS#7 填充。
// 每次写入都生成新的随机 IV；IV 不保密，直接前置在密文前。
func (t *cbc) TransformToStorage(ctx context.Context, data []byte, dataCtx value.Context) ([]byte, error) {
	blockSize := aes.BlockSize
	paddingSize := blockSize - (len(data) % blockSize)
	result := make([]byte, blockSize+len(data)+paddingSize)
	iv := result[:blockSize]
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, errors.New("unable to read sufficient random bytes")
	}
	copy(result[blockSize:], data)

	// 为 CBC 添加 PKCS#7 填充。
	copy(result[blockSize+len(data):], bytes.Repeat([]byte{byte(paddingSize)}, paddingSize))

	mode := cipher.NewCBCEncrypter(t.block, iv)
	mode.CryptBlocks(result[blockSize:], result[blockSize:])
	return result, nil
}
