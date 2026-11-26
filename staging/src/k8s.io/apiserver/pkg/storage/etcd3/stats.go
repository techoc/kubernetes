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

package etcd3

import (
	"context"
	"strings"
	"sync"

	"sync/atomic"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/klog/v2"
)

// sizerRefreshInterval 是后台清理已删除 key 的基准周期；实际间隔带
// 抖动，避免所有 apiserver 同时扫描 etcd。
const sizerRefreshInterval = time.Minute

// newResourceSizeEstimator 创建估算器并启动后台清理 goroutine。prefix
// 统一补尾斜杠，使后续 `prefix + key` 的拼接结果稳定。
func newResourceSizeEstimator(prefix string, getKeys storage.KeysFunc) *resourceSizeEstimator {
	if prefix[len(prefix)-1] != '/' {
		prefix += "/"
	}
	sc := &resourceSizeEstimator{
		prefix:  prefix,
		getKeys: getKeys,
		stop:    make(chan struct{}),
		keys:    make(map[string]sizeRevision),
	}
	sc.wg.Add(1)
	go func() {
		defer sc.wg.Done()
		sc.run()
	}()
	return sc
}

// resourceSizeEstimator 根据各键最近观察到的状态，
// 高效估算对象平均大小。
// 把它接入 GetList 与 Watch，无需额外请求底层存储
// 即可维护相当准确的对象大小估算。
//
// 为处理可能的乱序或缺失数据，
// 它用每个键的 revision 判断哪个状态更新。
// 若漏掉删除事件，这种方式可能残留键，
// 因此有后台 goroutine 定期清理。
//
// resourceSizeEstimator 通过 GetList/Watch 顺带观察到的 key/value 大小
// 维护估算，无需额外请求存储。用 ModRevision 解决事件乱序/重复；未观察
// 到删除事件时会残留 key，故由后台 goroutine 定期按真实 key 列表清理。
type resourceSizeEstimator struct {
	prefix         string
	stop           chan struct{}
	wg             sync.WaitGroup
	lastKeyCleanup atomic.Pointer[time.Time]
	getKeys        storage.KeysFunc

	keysLock sync.Mutex
	keys     map[string]sizeRevision
}

// sizeRevision 记录某 key 最近一次观察到的值大小与修订号；revision 用于
// 丢弃过期更新。
type sizeRevision struct {
	sizeBytes int64
	revision  int64
}

// Stats 返回对象数量与平均对象大小估算。对象数量来自存储的真实 key
// 列表，平均大小仅基于已观察到的 key；未观察到任何 key 时不提供估算，
// 避免用样本不足的数据误导调用方。
func (sc *resourceSizeEstimator) Stats(ctx context.Context) (storage.Stats, error) {
	keys, err := sc.getKeys(ctx)
	if err != nil {
		return storage.Stats{}, err
	}
	stats := storage.Stats{
		ObjectCount: int64(len(keys)),
	}
	sc.keysLock.Lock()
	defer sc.keysLock.Unlock()
	sc.cleanKeys(keys)
	if len(sc.keys) != 0 {
		stats.EstimatedAverageObjectSizeBytes = sc.keySizes() / int64(len(sc.keys))
	}
	return stats, nil
}

// Close 停止后台清理并等待其退出；重复调用会 panic（关闭已关闭的
// channel），调用方应保证只关闭一次。
func (sc *resourceSizeEstimator) Close() {
	close(sc.stop)
	sc.wg.Wait()
}

// run 首次清理延迟一个抖动周期，因为构造时可能还没有任何观察数据；
// 之后以带抖动的周期持续清理。
func (sc *resourceSizeEstimator) run() {
	jitter := 0.5 // 间隔范围为 [interval, interval * (1.0 + jitter)]
	sliding := true
	// wait.JitterUntilWithContext 会立即执行，所以先等待。
	select {
	case <-time.After(wait.Jitter(sizerRefreshInterval, jitter)):
	case <-sc.stop:
	}
	wait.JitterUntilWithContext(wait.ContextForChannel(sc.stop), sc.cleanKeysIfNeeded, sizerRefreshInterval, jitter, sliding)
}

// cleanKeysIfNeeded 在距离上次清理不足一个周期时跳过，防止 Stats 与
// 后台任务重复触发全量 key 扫描。
func (sc *resourceSizeEstimator) cleanKeysIfNeeded(ctx context.Context) {
	lastKeyCleanup := sc.lastKeyCleanup.Load()
	if lastKeyCleanup != nil && time.Since(*lastKeyCleanup) < sizerRefreshInterval {
		return
	}
	keys, err := sc.getKeys(ctx)
	if err != nil {
		klog.InfoS("Error getting keys", "err", err)
		return
	}
	sc.keysLock.Lock()
	defer sc.keysLock.Unlock()
	sc.cleanKeys(keys)
}

// cleanKeys 用真实 key 列表重建观察表，从而丢弃已不存在的 key。缓存
// 层的 key 可能不带完整前缀（仅以 / 开头），这里统一补全以便匹配。
func (sc *resourceSizeEstimator) cleanKeys(keepKeys []string) {
	newKeys := make(map[string]sizeRevision, len(keepKeys))
	for _, key := range keepKeys {
		// 处理 cacher 中不带前缀的键。
		if !strings.HasPrefix(key, sc.prefix) {
			startIndex := 0
			if key[0] == '/' {
				startIndex = 1
			}
			key = sc.prefix + key[startIndex:]
		}
		keySizeRevision, ok := sc.keys[key]
		if !ok {
			continue
		}
		newKeys[key] = keySizeRevision
	}
	sc.keys = newKeys
	now := time.Now()
	sc.lastKeyCleanup.Store(&now)
}

// keySizes 汇总所有已观察 key 的大小。
func (sc *resourceSizeEstimator) keySizes() (totalSize int64) {
	for _, sizeRevision := range sc.keys {
		totalSize += sizeRevision.sizeBytes
	}
	return totalSize
}

// Update 批量吸收一组 key/value，通常在 GetList 返回时调用。
func (sc *resourceSizeEstimator) Update(kvs []*mvccpb.KeyValue) {
	sc.keysLock.Lock()
	defer sc.keysLock.Unlock()
	for _, kv := range kvs {
		sc.updateKey(kv)
	}
}

// UpdateKey 吸收单个 key/value，通常在 Watch 事件中调用。
func (sc *resourceSizeEstimator) UpdateKey(kv *mvccpb.KeyValue) {
	sc.keysLock.Lock()
	defer sc.keysLock.Unlock()

	sc.updateKey(kv)
}

// updateKey 仅在事件修订号更新时覆盖，避免乱序事件把新大小换成旧值。
// 删除事件表示为空 Value，这里仍按普通更新处理，随后由其修订号参与
// 后续删除判断。
func (sc *resourceSizeEstimator) updateKey(kv *mvccpb.KeyValue) {
	key := string(kv.Key)
	keySizeRevision := sc.keys[key]
	if keySizeRevision.revision >= kv.ModRevision {
		return
	}

	sc.keys[key] = sizeRevision{
		sizeBytes: int64(len(kv.Value)),
		revision:  kv.ModRevision,
	}
}

// DeleteKey 在删除事件的修订号更新时移除 key，防止旧删除事件误删后续
// 重新创建的 key。
func (sc *resourceSizeEstimator) DeleteKey(kv *mvccpb.KeyValue) {
	sc.keysLock.Lock()
	defer sc.keysLock.Unlock()

	key := string(kv.Key)
	keySizeRevision := sc.keys[key]
	if keySizeRevision.revision >= kv.ModRevision {
		return
	}

	delete(sc.keys, key)
}
