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

// NewETCDLatencyTracker returns an implementation of
// clientv3.KV that times the calls from the specified
// 'delegate' KV instance in order to track latency incurred.
//
// 通过装饰器而非修改调用点来采集延迟，保证所有经此 KV 的调用都被覆盖。
func NewETCDLatencyTracker(delegate clientv3.KV) clientv3.KV {
	return &clientV3KVLatencyTracker{KV: delegate}
}

// clientV3KVLatencyTracker decorates a clientv3.KV instance and times
// each call so we can track the latency an API request incurs in etcd
// round trips (the time it takes to send data to etcd and get the
// complete response back)
//
// If an API request involves N (N>=1) round trips to etcd, then we will sum
// up the latenciy incurred in each roundtrip.

// It uses the context associated with the request in flight, so there
// are no states shared among the requests in flight, and so there is no
// concurrency overhead.
// If the goroutine executing the request handler makes concurrent calls
// to the underlying storage layer, that is protected since the latency
// tracking function TrackStorageLatency is thread safe.
//
// NOTE: Compact is an asynchronous process and is not associated with
// any request, so we will not be tracking its latency.
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
