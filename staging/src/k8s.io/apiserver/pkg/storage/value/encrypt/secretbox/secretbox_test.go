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

package secretbox

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"reflect"
	"testing"

	"k8s.io/apiserver/pkg/storage/value"
)

var (
	key1 = [32]byte{0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01}
	key2 = [32]byte{0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02, 0x02}
)

// TestSecretboxKeyRotation 验证前缀转换器在密钥轮换时的读取语义：
// 加密时总是用第一个（当前）密钥并写入对应前缀；读取时必须能识别
// 旧前缀解密旧数据，并把"用了非首密钥"标记为 stale，触发上层回写。
// 同时确认 secretbox 不是认证加密存储——上下文不同也仍能解密。
func TestSecretboxKeyRotation(t *testing.T) {
	testErr := fmt.Errorf("test error")
	ctx := context.Background()
	dataCtx := value.DefaultContext([]byte("authenticated_data"))

	p := value.NewPrefixTransformers(testErr,
		value.PrefixTransformer{Prefix: []byte("first:"), Transformer: NewSecretboxTransformer(key1)},
		value.PrefixTransformer{Prefix: []byte("second:"), Transformer: NewSecretboxTransformer(key2)},
	)
	out, err := p.TransformToStorage(ctx, []byte("firstvalue"), dataCtx)
	if err != nil {
		t.Fatal(err)
	}
	// 写入必须使用首个转换器的前缀，否则轮换顺序就失去意义。
	if !bytes.HasPrefix(out, []byte("first:")) {
		t.Fatalf("unexpected prefix: %q", out)
	}
	from, stale, err := p.TransformFromStorage(ctx, out, dataCtx)
	if err != nil {
		t.Fatal(err)
	}
	if stale || !bytes.Equal([]byte("firstvalue"), from) {
		t.Fatalf("unexpected data: %t %q", stale, from)
	}

	// verify changing the context does not fails storage
	// Secretbox is not currently an authenticating store
	// 这记录的是当前实现的已知属性：secretbox 不绑定 dataCtx，
	// 因此改变上下文仍可解密（不同于 AES-GCM 等认证模式）。
	_, _, err = p.TransformFromStorage(ctx, out, value.DefaultContext([]byte("incorrect_context")))
	if err != nil {
		t.Fatalf("secretbox is not authenticated")
	}

	// reverse the order, use the second key
	// 调换顺序模拟"新密钥成为当前密钥"：旧数据仍能读，但必须报告
	// stale 以便上层用新密钥重写。
	p = value.NewPrefixTransformers(testErr,
		value.PrefixTransformer{Prefix: []byte("second:"), Transformer: NewSecretboxTransformer(key2)},
		value.PrefixTransformer{Prefix: []byte("first:"), Transformer: NewSecretboxTransformer(key1)},
	)
	from, stale, err = p.TransformFromStorage(ctx, out, dataCtx)
	if err != nil {
		t.Fatal(err)
	}
	if !stale || !bytes.Equal([]byte("firstvalue"), from) {
		t.Fatalf("unexpected data: %t %q", stale, from)
	}
}

// BenchmarkSecretboxRead 测量不同值大小与是否命中旧密钥（stale）时的
// 解密吞吐；解码路径与密钥选择逻辑都会影响结果。
func BenchmarkSecretboxRead(b *testing.B) {
	tests := []struct {
		keyLength   int
		valueLength int
		expectStale bool
	}{
		{keyLength: 32, valueLength: 1024, expectStale: false},
		{keyLength: 32, valueLength: 16384, expectStale: false},
		{keyLength: 32, valueLength: 16384, expectStale: true},
	}
	for _, t := range tests {
		name := fmt.Sprintf("%vKeyLength/%vValueLength/%vExpectStale", t.keyLength, t.valueLength, t.expectStale)
		b.Run(name, func(b *testing.B) {
			benchmarkSecretboxRead(b, t.keyLength, t.valueLength, t.expectStale)
		})
	}
}

// BenchmarkSecretboxWrite 测量不同值大小下的加密吞吐。
func BenchmarkSecretboxWrite(b *testing.B) {
	tests := []struct {
		keyLength   int
		valueLength int
	}{
		{keyLength: 32, valueLength: 1024},
		{keyLength: 32, valueLength: 16384},
	}
	for _, t := range tests {
		name := fmt.Sprintf("%vKeyLength/%vValueLength", t.keyLength, t.valueLength)
		b.Run(name, func(b *testing.B) {
			benchmarkSecretboxWrite(b, t.keyLength, t.valueLength)
		})
	}
}

// benchmarkSecretboxRead 先写入一次，再重复读取。expectStale 时把密钥
// 顺序反过来，使读取必须走非首转换器，从而测量该路径的成本并确认
// stale 标志正确。
func benchmarkSecretboxRead(b *testing.B, keyLength int, valueLength int, expectStale bool) {
	p := value.NewPrefixTransformers(nil,
		value.PrefixTransformer{Prefix: []byte("first:"), Transformer: NewSecretboxTransformer(key1)},
		value.PrefixTransformer{Prefix: []byte("second:"), Transformer: NewSecretboxTransformer(key2)},
	)

	ctx := context.Background()
	dataCtx := value.DefaultContext([]byte("authenticated_data"))
	v := bytes.Repeat([]byte("0123456789abcdef"), valueLength/16)

	out, err := p.TransformToStorage(ctx, v, dataCtx)
	if err != nil {
		b.Fatal(err)
	}
	// reverse the key order if expecting stale
	if expectStale {
		p = value.NewPrefixTransformers(nil,
			value.PrefixTransformer{Prefix: []byte("second:"), Transformer: NewSecretboxTransformer(key2)},
			value.PrefixTransformer{Prefix: []byte("first:"), Transformer: NewSecretboxTransformer(key1)},
		)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		from, stale, err := p.TransformFromStorage(ctx, out, dataCtx)
		if err != nil {
			b.Fatal(err)
		}
		if expectStale != stale {
			b.Fatalf("unexpected data: %q, expect stale %t but got %t", from, expectStale, stale)
		}
	}
	b.StopTimer()
}

// benchmarkSecretboxWrite 重复加密固定内容，测量纯写入路径。
func benchmarkSecretboxWrite(b *testing.B, keyLength int, valueLength int) {
	p := value.NewPrefixTransformers(nil,
		value.PrefixTransformer{Prefix: []byte("first:"), Transformer: NewSecretboxTransformer(key1)},
		value.PrefixTransformer{Prefix: []byte("second:"), Transformer: NewSecretboxTransformer(key2)},
	)

	ctx := context.Background()
	dataCtx := value.DefaultContext([]byte("authenticated_data"))
	v := bytes.Repeat([]byte("0123456789abcdef"), valueLength/16)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := p.TransformToStorage(ctx, v, dataCtx)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// TestRoundTrip 用 0..17、128、1024 字节的随机数据验证加解密可逆。
// 特别覆盖 0 字节与 1..17 字节：这些长度落在 secretbox 的块边界与
// nonce 开销附近，是编码实现最容易出错的区间。
func TestRoundTrip(t *testing.T) {
	lengths := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 128, 1024}

	ctx := context.Background()
	tests := []struct {
		name    string
		dataCtx value.Context
		t       value.Transformer
	}{
		{name: "Secretbox 32 byte key", t: NewSecretboxTransformer(key1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dataCtx := tt.dataCtx
			if dataCtx == nil {
				dataCtx = value.DefaultContext([]byte(""))
			}
			for _, l := range lengths {
				data := make([]byte, l)
				if _, err := io.ReadFull(rand.Reader, data); err != nil {
					t.Fatalf("unable to read sufficient random bytes: %v", err)
				}
				original := append([]byte{}, data...)

				ciphertext, err := tt.t.TransformToStorage(ctx, data, dataCtx)
				if err != nil {
					t.Errorf("TransformToStorage error = %v", err)
					continue
				}

				result, stale, err := tt.t.TransformFromStorage(ctx, ciphertext, dataCtx)
				if err != nil {
					t.Errorf("TransformFromStorage error = %v", err)
					continue
				}
				if stale {
					t.Errorf("unexpected stale output")
					continue
				}

				switch {
				case l == 0:
					if len(result) != 0 {
						t.Errorf("Round trip failed len=%d\noriginal:\n%s\nresult:\n%s", l, hex.Dump(original), hex.Dump(result))
					}
				case !reflect.DeepEqual(original, result):
					t.Errorf("Round trip failed len=%d\noriginal:\n%s\nresult:\n%s", l, hex.Dump(original), hex.Dump(result))
				}
			}
		})
	}
}
