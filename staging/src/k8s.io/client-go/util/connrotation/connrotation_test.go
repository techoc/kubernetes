/*
Copyright 2018 The Kubernetes Authors.

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

package connrotation

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestCloseAll 验证 CloseAll 能关闭所有已建立的连接，并且 Dialer 在 CloseAll 之后仍可复用。
func TestCloseAll(t *testing.T) {
	// 每个连接关闭时向 channel 发送信号，缓冲足够容纳一轮的全部连接。
	closed := make(chan struct{}, 50)
	dialFn := func(ctx context.Context, network, address string) (net.Conn, error) {
		return closeOnlyConn{onClose: func() { closed <- struct{}{} }}, nil
	}
	dialer := NewDialer(dialFn)

	const numConns = 10

	// Outer loop to ensure Dialer is re-usable after CloseAll.
	for i := 0; i < 5; i++ {
		for j := 0; j < numConns; j++ {
			if _, err := dialer.Dial("", ""); err != nil {
				t.Fatal(err)
			}
		}
		dialer.CloseAll()
		// 1 秒内必须收到全部关闭通知，否则说明有连接被遗漏。
		deadline := time.After(time.Second)
		for j := 0; j < numConns; j++ {
			select {
			case <-closed:
			case <-deadline:
				t.Fatalf("iteration %d: 1s after CloseAll only %d/%d connections closed", i, j, numConns)
			}
		}
	}
}

// TestCloseAllRace 确保 CloseAll 与正在并发建立的连接能协同工作。
// 压测 Dial 与 CloseAll 的并发：两者都在频繁修改跟踪集合，必须无竞态且不丢连接。
func TestCloseAllRace(t *testing.T) {
	conns := int64(0)
	dialer := NewDialer(func(ctx context.Context, network, address string) (net.Conn, error) {
		return closeOnlyConn{onClose: func() { atomic.AddInt64(&conns, -1) }}, nil
	})

	const raceCount = 5000
	begin := &sync.WaitGroup{}
	begin.Add(1)

	wg := &sync.WaitGroup{}

	// Close all as fast as we can
	wg.Add(1)
	go func() {
		begin.Wait()
		defer wg.Done()
		for i := 0; i < raceCount; i++ {
			dialer.CloseAll()
		}
	}()

	// Dial as fast as we can
	wg.Add(1)
	go func() {
		begin.Wait()
		defer wg.Done()
		for i := 0; i < raceCount; i++ {
			if _, err := dialer.Dial("", ""); err != nil {
				t.Error(err)
				return
			}
			atomic.AddInt64(&conns, 1)
		}
	}()

	// 尽可能同时放行两个 goroutine，制造最大并发窗口。
	begin.Done()

	// Wait for goroutines
	wg.Wait()

	// Ensure CloseAll ran after all dials
	dialer.CloseAll()

	// Expect all connections to close within 5 seconds
	for start := time.Now(); time.Since(start) < 5*time.Second; time.Sleep(10 * time.Millisecond) {
		// Ensure all connections were closed
		if c := atomic.LoadInt64(&conns); c == 0 {
			break
		} else {
			t.Logf("got %d open connections, want 0, will retry", c)
		}
	}
	// Ensure all connections were closed
	if c := atomic.LoadInt64(&conns); c != 0 {
		t.Fatalf("got %d open connections, want 0", c)
	}
}

// closeOnlyConn 是只实现 Close 的最小 net.Conn，用于统计连接是否被关闭。
// Close 在独立 goroutine 中回调，避免在锁内执行测试逻辑造成死锁。
type closeOnlyConn struct {
	net.Conn
	onClose func()
}

// Close 触发回调并返回 nil；nil net.Conn 嵌入后其他方法不应被调用。
func (c closeOnlyConn) Close() error {
	go c.onClose()
	return nil
}
