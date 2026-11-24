/*
Copyright 2022 The Kubernetes Authors.

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

package runtime

import (
	"sync"
)

// AllocatorPool 使用 sync.Pool 缓存暂时闲置的 Allocator，减少重复分配及垃圾回收压力。Get 获取可复用实例，使用完毕后必须 Put 归还。池可能随时丢弃缓存对象，因此不能依赖对象一定被复用。
var AllocatorPool = sync.Pool{
	New: func() interface{} {
		return &Allocator{}
	},
}

// Allocator 为对象序列化提供可复用的字节缓冲区。容量足够时复用原底层数组，容量不足时扩容；这样可减少频繁分配和 GC 扫描压力。
type Allocator struct {
	buf []byte
}

var _ MemoryAllocator = &Allocator{}

// 申请至少 n 字节的缓冲区。若现有底层数组容量足够，则直接扩展切片长度并复用；否则按“原容量的两倍加本次需求”分配新数组。返回的字节不会清零，调用方必须覆盖将要读取的区域。
func (a *Allocator) Allocate(n uint64) []byte {
	// 如果缓存数组容量足够，直接复用。这里不会清零，因为序列化器会覆盖实际使用的每个字节。
	if uint64(cap(a.buf)) >= n {
		a.buf = a.buf[:n]
		return a.buf
	}
	// 扩大缓冲区。
	// 按“当前容量的两倍 + 本次请求长度”扩容，使连续增长时的总复制成本保持摊销线性。
	size := uint64(2*cap(a.buf)) + n
	a.buf = make([]byte, size)
	a.buf = a.buf[:n]
	return a.buf
}

// SimpleAllocator 是 MemoryAllocator 的简单实现，每次请求都直接 make 新切片，不缓存内存。实现简单且无共享状态，适合不需要复用缓冲区的场景。
type SimpleAllocator struct{}

var _ MemoryAllocator = &SimpleAllocator{}

// Allocate 每次都创建长度为 n 且内容清零的新缓冲区。与 Allocator 不同，SimpleAllocator 不保存状态，因此可并发使用。
func (sa *SimpleAllocator) Allocate(n uint64) []byte {
	return make([]byte, n)
}
