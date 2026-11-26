/*
Copyright 2025 The Kubernetes Authors.

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

package testing

import (
	"context"
	"sync"
	"sync/atomic"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/kubernetes"
)

// KVRecorder 包装 clientv3.KV，统计读取次数并记录分页/流式读请求。
// 计数使用 atomic，因为被测存储可能从多个 goroutine 并发读取。
type KVRecorder struct {
	clientv3.KV

	reads       uint64
	streamReads uint64
	lists       *KubernetesRecorder
}

// NewKVRecorder 构造 KV 记录器；lists 可为 nil，此时仅计数不记录请求详情。
func NewKVRecorder(kv clientv3.KV, lists *KubernetesRecorder) *KVRecorder {
	return &KVRecorder{KV: kv, lists: lists}
}

// Get 计数后透传。
func (r *KVRecorder) Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	atomic.AddUint64(&r.reads, 1)
	return r.KV.Get(ctx, key, opts...)
}

// GetReadsAndReset 返回累计 Get 次数并清零，便于按阶段断言读放大。
func (r *KVRecorder) GetReadsAndReset() uint64 {
	return atomic.SwapUint64(&r.reads, 0)
}

// GetStream 计数并记录流式读取的 key/revision/limit 后透传。
func (r *KVRecorder) GetStream(ctx context.Context, key string, opts ...clientv3.OpOption) (clientv3.GetStreamChan, error) {
	atomic.AddUint64(&r.streamReads, 1)
	if r.lists != nil {
		op := clientv3.OpGet(key, opts...)
		r.lists.record(ctx, RecordedList{Key: key, Revision: op.Rev(), Limit: op.Limit()})
	}
	return r.KV.GetStream(ctx, key, opts...)
}

// GetStreamReadsAndReset 返回累计流式读取次数并清零。
func (r *KVRecorder) GetStreamReadsAndReset() uint64 {
	return atomic.SwapUint64(&r.streamReads, 0)
}

// KubernetesRecorder 记录 kubernetes.Interface 的 List 调用，按测试上下文中的
// RecorderContextKey 分组，使并发测试互不干扰。
type KubernetesRecorder struct {
	kubernetes.Interface

	mux         sync.Mutex
	listsPerKey map[string][]RecordedList
}

// NewKubernetesRecorder 构造 List 记录器。
func NewKubernetesRecorder(client kubernetes.Interface) *KubernetesRecorder {
	return &KubernetesRecorder{
		listsPerKey: make(map[string][]RecordedList),
		Interface:   client,
	}
}

// List 记录本次 List 的起始位置与分页参数后透传。
func (r *KubernetesRecorder) List(ctx context.Context, key string, opts kubernetes.ListOptions) (kubernetes.ListResponse, error) {
	// Continue 若设置，表示 range 实际开始的位置（见 kubernetes.Client.List），因此折叠进 Key。
	// 折叠后，RecordedList.Key 始终表示“真实读取起点”，便于断言分页行为。
	rangeStart := key
	if opts.Continue != "" {
		rangeStart = opts.Continue
	}
	r.record(ctx, RecordedList{Key: rangeStart, Revision: opts.Revision, Limit: opts.Limit})
	return r.Interface.List(ctx, key, opts)
}

// record 仅在 ctx 携带 RecorderContextKey 时记录；无键时静默跳过，
// 让同一 recorder 可安全地服务不关心记录的测试。
func (r *KubernetesRecorder) record(ctx context.Context, list RecordedList) {
	recorderKey, ok := ctx.Value(RecorderContextKey).(string)
	if !ok {
		return
	}
	r.mux.Lock()
	defer r.mux.Unlock()
	r.listsPerKey[recorderKey] = append(r.listsPerKey[recorderKey], list)
}

// ListRequestForKey 返回指定 recorderKey 下捕获的全部 List 请求。
func (r *KubernetesRecorder) ListRequestForKey(key string) []RecordedList {
	r.mux.Lock()
	defer r.mux.Unlock()
	return r.listsPerKey[key]
}

// RecordedList 是 recorder 捕获的 LIST 请求。分页与流式
// 读取都把续读点折叠进 Key。
type RecordedList struct {
	Key      string
	Revision int64
	Limit    int64
}

// RecorderContextKey 是 ctx 中存放 recorderKey 的键。
var RecorderContextKey recorderKeyType

type recorderKeyType struct{}
