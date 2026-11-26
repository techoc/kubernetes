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

// Package connrotation 实现了一个能追踪并可关闭所有已创建连接的拨号器。
// 这用于长连接的凭证轮换场景——此时无法在活动连接上重新认证：
// 凭证轮换时，旧连接仍会使用旧凭证存活，只能强制关闭并让客户端重新拨号。
// 该包记录所有已建立的连接，便于在轮换时一次性关闭。
package connrotation

import (
	"context"
	"net"
	"sync"
)

// DialFunc 是 net.DialContext 签名的简写。
type DialFunc func(ctx context.Context, network, address string) (net.Conn, error)

// Dialer 通过 Dial 建立连接并追踪它们。
//
// 中文：嵌入 ConnectionTracker，使所有经此 Dialer 建立的连接都可被统一关闭。
type Dialer struct {
	dial DialFunc
	*ConnectionTracker
}

// NewDialer 创建 Dialer 实例，等价于 NewDialerWithTracker(dial, nil)。
func NewDialer(dial DialFunc) *Dialer {
	return NewDialerWithTracker(dial, nil)
}

// NewDialerWithTracker 创建一个新的 Dialer 实例。
// 若 dial 不为 nil，则用它创建底层连接；否则使用 net.DialContext。
// 若 tracker 不为 nil，则用它追踪新的底层连接；否则使用 NewConnectionTracker()。
// tracker 允许与其它 Dialer 共享连接集合，从而一起轮换。注意当前实现
// 并未在 dial 为 nil 时回退到 net.DialContext，传入 nil 会在首次拨号时 panic。
func NewDialerWithTracker(dial DialFunc, tracker *ConnectionTracker) *Dialer {
	if tracker == nil {
		tracker = NewConnectionTracker()
	}
	return &Dialer{
		dial:              dial,
		ConnectionTracker: tracker,
	}
}

// ConnectionTracker 追踪已建立的连接。
//
// 中文：用 map 而非切片，便于关闭时以 O(1) 删除单条记录。
type ConnectionTracker struct {
	mu    sync.Mutex
	conns map[*closableConn]struct{}
}

// NewConnectionTracker 创建供 NewDialerWithTracker 使用的连接追踪器。
func NewConnectionTracker() *ConnectionTracker {
	return &ConnectionTracker{
		conns: make(map[*closableConn]struct{}),
	}
}

// CloseAll 强制关闭所有被追踪的连接。
// 注意：在 CloseAll 返回之前，新连接也可能被建立。
// 先在锁内替换为新的空 map，再在锁外逐个关闭；这样既缩短临界区，也避免
// 关闭动作里的回调（delete）与持锁路径相互纠缠。关闭期间新建立的连接不会被本轮
// 关闭，这是刻意接受的竞态，调用方应通过轮换后的凭证重新建连。
func (c *ConnectionTracker) CloseAll() {
	c.mu.Lock()
	conns := c.conns
	c.conns = make(map[*closableConn]struct{})
	c.mu.Unlock()

	for conn := range conns {
		conn.Close()
	}
}

// Track 把连接加入追踪列表，并返回包装后的连接：关闭时自动停止追踪。
func (c *ConnectionTracker) Track(conn net.Conn) net.Conn {
	closable := &closableConn{Conn: conn}

	// 连接关闭时从 map 中移除；若已被 CloseAll 清空，这里是无操作。
	// 中文：正常关闭时及时清理，防止 map 随连接建立无界增长；CloseAll 已清空时
	// delete 是安全的空操作。
	closable.onClose = func() {
		c.mu.Lock()
		delete(c.conns, closable)
		c.mu.Unlock()
	}

	// 开始追踪该连接。
	c.mu.Lock()
	c.conns[closable] = struct{}{}
	c.mu.Unlock()

	return closable
}

// Dial 创建一条被追踪的连接。
//
// 中文：无 context 版本，内部使用 Background。
func (d *Dialer) Dial(network, address string) (net.Conn, error) {
	return d.DialContext(context.Background(), network, address)
}

// DialContext 创建一条被追踪的连接。
//
// 中文：拨号成功后立即登记，确保 CloseAll 能看到该连接。
func (d *Dialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	conn, err := d.dial(ctx, network, address)
	if err != nil {
		return nil, err
	}
	return d.ConnectionTracker.Track(conn), nil
}

// closableConn 在底层连接外再包一层，以便 Close 时触发取消跟踪的回调。
type closableConn struct {
	onClose func()
	net.Conn
}

// Close 关闭底层连接，并在后台执行取消跟踪。
//
// 中文：使用 goroutine 是为了保证 Close 立即返回，避免回调中的锁等待阻塞调用方；
// 回调只做一次 map 删除，开销很小。
func (c *closableConn) Close() error {
	go c.onClose()
	return c.Conn.Close()
}
