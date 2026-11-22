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
	"math/rand"
	"testing"
)

// TestAllocatorRandomInputs 用随机大小（最多 5MB）反复调用 Allocate，
// 验证返回缓冲区的 cap 足够大、len 恰好等于请求字节数。
func TestAllocatorRandomInputs(t *testing.T) {
	maxBytes := 5 * 1000000 // 5 MB
	iterations := rand.Intn(10000) + 10
	target := &Allocator{}

	for i := 0; i < iterations; i++ {
		bytesToAllocate := rand.Intn(maxBytes)
		buff := target.Allocate(uint64(bytesToAllocate))
		if cap(buff) < bytesToAllocate {
			t.Fatalf("expected the buffer to allocate: %v bytes whereas it allocated: %v bytes", bytesToAllocate, cap(buff))
		}
		if len(buff) != bytesToAllocate {
			t.Fatalf("unexpected length of the buffer, expected: %v, got: %v", bytesToAllocate, len(buff))
		}
	}
}

// TestAllocatorNeverShrinks 验证分配器是单调增长的：后续较小的请求仍复用
// 之前的大缓冲区（cap 不缩小），只是 len 相应减小。
func TestAllocatorNeverShrinks(t *testing.T) {
	target := &Allocator{}
	initialSize := 1000000 // 1MB
	initialBuff := target.Allocate(uint64(initialSize))
	if cap(initialBuff) < initialSize {
		t.Fatalf("unexpected size of the buffer, expected at least 1MB, got: %v", cap(initialBuff))
	}

	for i := initialSize; i > 0; i = i / 10 {
		newBuff := target.Allocate(uint64(i))
		if cap(newBuff) < initialSize {
			t.Fatalf("allocator is now allowed to shrink memory")
		}
		if len(newBuff) != i {
			t.Fatalf("unexpected length of the buffer, expected: %v, got: %v", i, len(newBuff))
		}
	}
}

// TestAllocatorZero 验证分配 0 字节时返回 len=0 但仍保留已有容量，
// 避免调用方拿到 nil 缓冲区而丢失复用能力。
func TestAllocatorZero(t *testing.T) {
	target := &Allocator{}
	initialSize := 1000000 // 1MB
	buff := target.Allocate(uint64(initialSize))
	if cap(buff) < initialSize {
		t.Fatalf("unexpected size of the buffer, expected at least 1MB, got: %v", cap(buff))
	}
	if len(buff) != initialSize {
		t.Fatalf("unexpected length of the buffer, expected: %v, got: %v", initialSize, len(buff))
	}

	buff = target.Allocate(0)
	if cap(buff) < initialSize {
		t.Fatalf("unexpected size of the buffer, expected at least 1MB, got: %v", cap(buff))
	}
	if len(buff) != 0 {
		t.Fatalf("unexpected length of the buffer, expected: 0, got: %v", len(buff))
	}
}
