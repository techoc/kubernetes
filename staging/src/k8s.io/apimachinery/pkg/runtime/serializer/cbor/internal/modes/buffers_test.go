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
	"testing"
)

// mockPool 记录最后一次 Put 进来的值，用于断言缓冲区是否被归还到池中。
type mockPool struct {
	v interface{}
}

// Get 始终返回 nil：本测试只关心 Put 的路径，无需模拟从池中取出的对象。
func (*mockPool) Get() interface{} {
	return nil
}

// Put 记录归还的对象，便于测试结束后检查。
func (p *mockPool) Put(v interface{}) {
	p.v = v
}

// TestBufferProviderPut 验证 BufferProvider.Put 的归还策略：
// 容量不超过阈值（3MiB）的缓冲区应被重置后归还到池中，超过阈值的则直接丢弃，
// 以免少数超大缓冲区长期占用内存、拖高整个进程的常驻内存。
func TestBufferProviderPut(t *testing.T) {
	{
		p := new(mockPool)
		bp := &BufferProvider{p: p}
		small := new(buffer)
		small.Grow(3 * 1024 * 1024)
		small.WriteString("hello world")
		bp.Put(small)
		// 容量在阈值内的缓冲区应被归还。
		if p.v != small {
			t.Errorf("expected buf with capacity %d to be returned to pool", small.Cap())
		}
		// 归还前必须清空内容，否则下一位使用者会读到上一次残留的数据。
		if small.Len() != 0 {
			t.Errorf("expected buf to be reset before returning to pool")
		}
	}

	{
		p := new(mockPool)
		bp := &BufferProvider{p: p}
		big := new(buffer)
		// 超出阈值 1 字节即应被丢弃，验证边界是严格大于阈值而非大于等于。
		big.Grow(3*1024*1024 + 1)
		bp.Put(big)
		if p.v != nil {
			t.Errorf("expected buf with capacity %d not to be returned to pool", big.Cap())
		}
	}
}
