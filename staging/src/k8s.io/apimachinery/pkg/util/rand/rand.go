/*
Copyright 2015 The Kubernetes Authors.

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

// Package rand 提供随机数、随机排列和随机字符串生成工具。
package rand

import (
	"math/rand"
	"sync"
	"time"
)

// rng 是全局随机源：math/rand 的 Rand 不是并发安全的，因此用互斥锁串行化
// 所有访问。种子取自启动时间。
var rng = struct {
	sync.Mutex
	rand *rand.Rand
}{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

// Int 返回一个非负伪随机整数。
func Int() int {
	rng.Lock()
	defer rng.Unlock()
	return rng.rand.Int()
}

// Intn 返回区间 [0, max) 内的伪随机整数。max 小于或等于 0 时按设计触发 panic，
// 与标准库 math/rand.Intn 保持一致。
//
// 与 math/rand.Intn 一致：max <= 0 时 panic 而不是返回错误。
func Intn(max int) int {
	rng.Lock()
	defer rng.Unlock()
	return rng.rand.Intn(max)
}

// IntnRange 返回区间 [min, max) 内的伪随机整数。要求 max 大于 min；
// 参数不合法时由底层 Intn 按设计触发 panic。
func IntnRange(min, max int) int {
	rng.Lock()
	defer rng.Unlock()
	return rng.rand.Intn(max-min) + min
}

// Int63nRange 返回区间 [min, max) 内的伪随机 int64。要求 max 大于 min；
// 参数不合法时由底层 Int63n 按设计触发 panic。
func Int63nRange(min, max int64) int64 {
	rng.Lock()
	defer rng.Unlock()
	return rng.rand.Int63n(max-min) + min
}

// Seed 使用给定种子重建全局随机源；调用期间会持锁，避免并发读写随机源状态。
//
// 替换整个 Rand 实例而不是调用 Seed，保证并发下状态切换是原子的。
func Seed(seed int64) {
	rng.Lock()
	defer rng.Unlock()

	rng.rand = rand.New(rand.NewSource(seed))
}

// Perm 返回 [0, n) 的伪随机排列，结果以整数切片表示。
func Perm(n int) []int {
	rng.Lock()
	defer rng.Unlock()
	return rng.rand.Perm(n)
}

const (
	// 字符表中去掉元音，以降低随机字符串意外组成不雅词语的概率。
	alphanums = "bcdfghjklmnpqrstvwxz2456789"
	// 索引 alphanums 字符表所需的位数。
	alphanumsIdxBits = 5
	// 用于提取随机整数最低 alphanumsIdxBits 位的掩码。
	alphanumsIdxMask = 1<<alphanumsIdxBits - 1
	// 单个 int63 最多可提取的字符数量。
	maxAlphanumsPerInt = 63 / alphanumsIdxBits
)

// String 生成长度为 n 的随机字母数字字符串，字符表不包含元音。n 为负数时会 panic。
// 生成过程：先生成随机 int63，再通过移位和掩码从每个随机数中提取多个字符索引；
// 如果索引超出字符表范围，则丢弃该位段并继续提取。
//
// 位切片（bit slicing）复用每个 int63 的低位，减少对随机源的调用次数。
func String(n int) string {
	b := make([]byte, n)
	rng.Lock()
	defer rng.Unlock()

	randomInt63 := rng.rand.Int63()
	remaining := maxAlphanumsPerInt
	for i := 0; i < n; {
		if remaining == 0 {
			// 当前 int63 的可用位耗尽，取新的随机数。
			randomInt63, remaining = rng.rand.Int63(), maxAlphanumsPerInt
		}
		// 索引越界（29..31）时丢弃该位段，重试。
		if idx := int(randomInt63 & alphanumsIdxMask); idx < len(alphanums) {
			b[i] = alphanums[idx]
			i++
		}
		randomInt63 >>= alphanumsIdxBits
		remaining--
	}
	return string(b)
}

// SafeEncodeString 使用与 rand.String 相同的字符表对 s 编码，降低生成不雅词语的概率，
// 并确保 API 中由哈希值派生的字符串具有一致的字符集合。
func SafeEncodeString(s string) string {
	r := make([]byte, len(s))
	for i, b := range []rune(s) {
		r[i] = alphanums[(int(b) % len(alphanums))]
	}
	return string(r)
}
