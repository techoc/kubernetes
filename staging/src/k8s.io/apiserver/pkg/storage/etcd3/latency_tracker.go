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

package etcd3

import (
	"context"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	endpointsrequest "k8s.io/apiserver/pkg/endpoints/request"
)

// NewETCDLatencyTracker 返回一个 clientv3.KV 实现，
// 它对指定 'delegate' KV 实例的调用计时，
// 以追踪所产生的延迟。
//
// 通过装饰器而非修改调用点来采集延迟，保证所有经此 KV 的调用都被覆盖。
func NewETCDLatencyTracker(delegate clientv3.KV) clientv3.KV {
	return &clientV3KVLatencyTracker{KV: delegate}
}

// clientV3KVLatencyTracker 装饰 clientv3.KV 实例，对每次调用计时，
// 以追踪一个 API 请求在 etcd 往返中产生的延迟
// （把数据发送到 etcd 并收到完整响应所需的时间）。
//
//
// 若一个 API 请求涉及 N 次（N>=1）etcd 往返，则累加
// 每次往返产生的延迟。

// 它使用进行中请求关联的 context，因此各请求之间不共享状态，
// 没有并发开销。
//
// 若处理请求的 goroutine 并发调用底层存储，也没有问题，
// 因为延迟追踪函数 TrackStorageLatency 是线程安全的。
// 追踪函数 TrackStorageLatency 是线程安全的。
//
// 注意：Compact 是异步过程，不与任何请求关联，
// 因此不追踪它的延迟。
//
// 只嵌入 clientv3.KV 而非另存字段，未覆盖的方法也能透明透传。
type clientV3KVLatencyTracker struct {
	clientv3.KV
}

// Put 计时并透传到底层 KV；用 defer 确保出错路径也记入延迟。
func (c *clientV3KVLatencyTracker) Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	startedAt := time.Now()
	defer func() {
		endpointsrequest.TrackStorageLatency(ctx, time.Since(startedAt))
	}()

	return c.KV.Put(ctx, key, val, opts...)
}

// Get 计时并透传到底层 KV。
func (c *clientV3KVLatencyTracker) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	startedAt := time.Now()
	defer func() {
		endpointsrequest.TrackStorageLatency(ctx, time.Since(startedAt))
	}()

	return c.KV.Get(ctx, key, opts...)
}

// Delete 计时并透传到底层 KV。
func (c *clientV3KVLatencyTracker) Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	startedAt := time.Now()
	defer func() {
		endpointsrequest.TrackStorageLatency(ctx, time.Since(startedAt))
	}()

	return c.KV.Delete(ctx, key, opts...)
}

// Do 计时并透传到底层 KV。
func (c *clientV3KVLatencyTracker) Do(ctx context.Context, op clientv3.Op) (clientv3.OpResponse, error) {
	startedAt := time.Now()
	defer func() {
		endpointsrequest.TrackStorageLatency(ctx, time.Since(startedAt))
	}()

	return c.KV.Do(ctx, op)
}

// Txn 返回包装后的 Txn；只对 Commit 计时，因为构建事务项本身不发网络请求。
func (c *clientV3KVLatencyTracker) Txn(ctx context.Context) clientv3.Txn {
	return &clientV3TxnTracker{ctx: ctx, Txn: c.KV.Txn(ctx)}
}

// clientV3TxnTracker 包装事务，保存 ctx 以便在 Commit 时上报延迟。
type clientV3TxnTracker struct {
	ctx context.Context
	clientv3.Txn
}

// Commit 计时事务提交并透传到底层 Txn。
func (t *clientV3TxnTracker) Commit() (*clientv3.TxnResponse, error) {
	startedAt := time.Now()
	defer func() {
		endpointsrequest.TrackStorageLatency(t.ctx, time.Since(startedAt))
	}()

	return t.Txn.Commit()
}
