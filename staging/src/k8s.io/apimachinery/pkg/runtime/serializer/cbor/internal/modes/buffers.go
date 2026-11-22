/*
Copyright 2024 The Kubernetes Authors.

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

package modes

import (
	"bytes"
	"sync"
)

// buffers 是包级共享的缓冲区提供者。用 sync.Pool 复用缓冲区以降低 GC 压力；
// CBOR 编码在 apiserver 热路径上，分配开销不可忽视。
var buffers = BufferProvider{p: new(sync.Pool)}

// buffer 包装 bytes.Buffer，以便与标准库类型区分，避免误用。
type buffer struct {
	bytes.Buffer
}

// pool 抽象 sync.Pool 的最小接口，便于测试替换。
type pool interface {
	Get() interface{}
	Put(interface{})
}

// BufferProvider 从池中借用/归还 *buffer。
type BufferProvider struct {
	p pool
}

// Get 返回一个已重置的缓冲区；池为空时新建。
func (b *BufferProvider) Get() *buffer {
	if buf, ok := b.p.Get().(*buffer); ok {
		return buf
	}
	return &buffer{}
}

// Put 归还缓冲区。超过阈值的大缓冲区直接丢弃。
func (b *BufferProvider) Put(buf *buffer) {
	if buf.Cap() > 3*1024*1024 /* Default MaxRequestBodyBytes */ {
		// Objects in a sync.Pool are assumed to be fungible. This is not a good assumption
		// for pools of *bytes.Buffer because a *bytes.Buffer's underlying array grows as
		// needed to accommodate writes. In Kubernetes, apiservers tend to encode "small"
		// objects very frequently and much larger objects (especially large lists) only
		// occasionally. Under steady load, pooled buffers tend to be borrowed frequently
		// enough to prevent them from being released. Over time, each buffer is used to
		// encode a large object and its capacity increases accordingly. The result is that
		// practically all buffers in the pool retain much more capacity than needed to
		// encode most objects.

		// As a basic mitigation for the worst case, buffers with more capacity than the
		// default max request body size are never returned to the pool.
		// TODO: Optimize for higher buffer utilization.
		//
		// 阈值取 3MiB，与默认 MaxRequestBodyBytes 同量级：再大的缓冲区留在池里
		// 会长期占用内存，而命中率又很低，不如交给 GC。
		return
	}
	buf.Reset()
	b.p.Put(buf)
}
