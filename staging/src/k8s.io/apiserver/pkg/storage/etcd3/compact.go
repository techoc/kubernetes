/*
Copyright 2016 The Kubernetes Authors.

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
	"fmt"
	"strconv"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

// compactRevKey 是 etcd 中记录“已压缩到哪个 revision”的特殊键。
// 它的 value 是 revision 数字，Version 被用作逻辑时钟，实现跨实例的压缩仲裁。
const (
	compactRevKey = "compact_rev_key"
)

// endpointsMap 以 etcd endpoint 为键去重 compactor：同一进程对同一集群只运行一个。
var (
	endpointsMapMu sync.Mutex
	endpointsMap   map[string]*compactor
)

func init() {
	endpointsMap = make(map[string]*compactor)
}

// StartCompactorPerEndpoint 启动一个 compactor，并防止同一客户端 endpoint 出现多个 compactor。
// 对同一个 endpoint 调用两次会返回同一个 compactor。
// Compactor 从 etcd 中移除旧版本的 key。
// 默认保留最近 5 分钟的数据，压缩更早的版本。
// 这足以应付较慢的 watcher 并容忍突发流量。
func StartCompactorPerEndpoint(client *clientv3.Client, compactInterval time.Duration) *compactor {
	endpointsMapMu.Lock()
	defer endpointsMapMu.Unlock()

	// 在一个进程中，一个集群只能有一个 compactor。
	// 目前依靠 endpoint 来区分不同集群。
	for _, ep := range client.Endpoints() {
		if c, ok := endpointsMap[ep]; ok {
			klog.V(4).Infof("compactor already exists for endpoints %v", client.Endpoints())
			return c
		}
	}
	// onClose 在 Stop 时把该集群的所有 endpoint 从注册表中移除，
	// 允许之后重新创建 compactor（例如测试或重建存储）。
	c := NewCompactor(client, compactInterval, clock.RealClock{}, func() {
		endpointsMapMu.Lock()
		defer endpointsMapMu.Unlock()
		for _, ep := range client.Endpoints() {
			delete(endpointsMap, ep)
		}
	})
	for _, ep := range client.Endpoints() {
		endpointsMap[ep] = c
	}
	return c
}

// Compactor 对外暴露当前已知的压缩 revision，供 watch cache 判断历史数据是否已被压缩。
type Compactor interface {
	CompactRevision() int64
}

// NewCompactor 运行独立的 compactor，从 etcd 中移除旧版本的 key。
// 优先调用 StartCompactorPerEndpoint，因为本函数不防止同一 endpoint 上运行多个 compactor。
func NewCompactor(client *clientv3.Client, compactInterval time.Duration, clock clock.Clock, onClose func()) *compactor {
	c := &compactor{
		client:   client,
		interval: compactInterval,
		stop:     make(chan struct{}),
		clock:    clock,
		onClose:  onClose,
	}
	// interval 为 0 表示只创建不后台压缩（例如只需要读取 revision）。
	if compactInterval != 0 {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.runCompactLoop(c.stop)
		}()
	}
	// ListFromCacheSnapshot 需要及时感知其他实例的压缩进度，因此额外运行 watch 循环。
	if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.runWatchLoop(c.stop)
		}()
	}
	return c
}

// compactor 封装压缩循环及其生命周期控制。
type compactor struct {
	client  *clientv3.Client
	wg      sync.WaitGroup
	clock   clock.Clock
	onClose func()

	stopOnce sync.Once
	stop     chan struct{}

	mux             sync.Mutex
	compactRevision int64
	interval        time.Duration
}

// Stop 幂等地停止后台循环；stopOnce 让集成测试重复调用 Stop 不会 panic。
func (c *compactor) Stop() {
	// 防止集成测试重复停止 compactor。
	c.stopOnce.Do(func() {
		if c.onClose != nil {
			c.onClose()
		}
		close(c.stop)
		c.wg.Wait()
	})
}

// CompactRevision 返回当前已知的最大压缩 revision。
func (c *compactor) CompactRevision() int64 {
	c.mux.Lock()
	defer c.mux.Unlock()
	return c.compactRevision
}

// UpdateCompactRevision 只允许 revision 单调递增，避免乱序响应导致回退。
func (c *compactor) UpdateCompactRevision(rev int64) {
	if rev == 0 {
		return
	}
	c.mux.Lock()
	defer c.mux.Unlock()
	c.compactRevision = max(c.compactRevision, rev)
}

// compactor 定期压缩 etcd 中 key 的历史版本。
// 它会压缩早于给定时间间隔的 key 版本。
// 也就是说，压缩后只保留最近一个时间间隔内设置的 key。
// 任何读取更旧版本的 API 调用都会返回错误。
// Interval 是两次压缩之间的时间间隔。第一次压缩在 "interval" 之后发生。
func (c *compactor) runCompactLoop(stopCh chan struct{}) {
	// 技术定义：
	// 在 etcd 中有一个特殊 key，定义为 *compactRevKey*。
	// compactRevKey 的值会被设为上次压缩 revision 的字符串。
	// compactRevKey 的版本用作比较的逻辑时间，称为压缩时间。
	// 最初该 key 不存在，因此压缩时间（版本）为 0。
	//
	// 算法：
	// - 比较本地 compact_time 与远端 compact_time 是否相等。
	// - 相等则同时递增本地与远端的 compact_time，并执行一次压缩。
	// - 不相等则把本地值设为远端的 compact_time。
	//
	// 技术细节/要点：
	//
	// 这里的协议基于 lease。若一个 compactor CAS 成功，其他 compactor 会在
	// 之后的 CAS 失败时得知，并在 5 分钟后重试。若某个 APIServer 崩溃，另一个会"接管"这个 lease。
	//
	// 例如在下图中，compactor C1 在 t1、t2 执行压缩。另一个 compactor C2
	// 在 t1'（t1 < t1' < t2）CAS 失败，把已知的 oldRev 设为 t1' 时的 rev，并在 t2'（t2' > t2）重试。
	// 若 C1 崩溃、未在 t2 压缩，C2 会在 t2' CAS 成功。
	//
	//                 oldRev(t2)     curRev(t2)
	//                                  +
	//   oldRev        curRev           |
	//     +             +              |
	//     |             |              |
	//     |             |    t1'       |     t2'
	// +---v-------------v----^---------v------^---->
	//     t0           t1             t2
	//
	// 保证：
	// - 正常情况下，间隔为 5 分钟。
	// - 故障转移时，间隔 >5 分钟且 <10 分钟
	//
	// 常见问题：
	// - 时间不准确怎么办？只要有人完成了压缩就无所谓。原子性由 etcd API 保证。
	//   etcd API.
	// - 高负载下会怎样？最初每个 apiserver 每 5 分钟只做一次压缩，
	//   几乎不会影响服务器负载，也不太受其影响。

	ctx := wait.ContextForChannel(stopCh)
	var compactTime int64
	var rev int64
	var compactRev int64
	var err error
	for {
		select {
		case <-c.clock.After(c.interval):
		case <-ctx.Done():
			return
		}

		compactTime, rev, compactRev, err = Compact(ctx, c.client, compactTime, rev)
		if err != nil {
			// 失败不退出：下一轮重试，保证压缩最终发生。
			klog.Errorf("etcd: endpoint (%v) compact failed: %v", c.client.Endpoints(), err)
			continue
		}
		if compactRev != 0 {
			c.UpdateCompactRevision(compactRev)
		}
	}
}

// Compact 压缩 etcd 存储并返回当前 revision。
// 无错误时返回当前压缩时间与全局 revision。
// 注意 CAS 失败不会产生错误。
//
// 通过 compactRevKey 的 Version 做 CAS 实现跨实例互斥：只有 CAS 成功者执行实际压缩，
// 其余实例读取对方写入的 revision 后等到下一轮。
func Compact(ctx context.Context, client *clientv3.Client, expectVersion, rev int64) (currentVersion, currentRev, compactRev int64, err error) {
	resp, err := client.KV.Txn(ctx).If(
		clientv3.Compare(clientv3.Version(compactRevKey), "=", expectVersion),
	).Then(
		clientv3.OpPut(compactRevKey, strconv.FormatInt(rev, 10)), // Expect side effect: increment Version
	).Else(
		clientv3.OpGet(compactRevKey),
	).Commit()
	if err != nil {
		return expectVersion, rev, 0, err
	}

	currentRev = resp.Header.Revision

	if !resp.Succeeded {
		// CAS 失败：说明其他实例已推进 compact time，同步本地状态即可。
		currentVersion = resp.Responses[0].GetResponseRange().Kvs[0].Version
		compactRev, err = strconv.ParseInt(string(resp.Responses[0].GetResponseRange().Kvs[0].Value), 10, 64)
		if err != nil {
			return currentVersion, currentRev, 0, nil
		}
		return currentVersion, currentRev, compactRev, nil
	}
	currentVersion = expectVersion + 1

	if rev == 0 {
		// 启动时不做压缩。
		return currentVersion, currentRev, 0, nil
	}
	if _, err = client.Compact(ctx, rev); err != nil {
		return currentVersion, currentRev, 0, err
	}
	klog.V(4).Infof("etcd: compacted rev (%d), endpoints (%v)", rev, client.Endpoints())
	return currentVersion, currentRev, rev, nil
}

// runWatchLoops 获取并监听 compact_rev_key 的变化，据此更新压缩 revision。
//
// watch 断开后按秒重试，先重新读取当前值再继续 watch，避免漏掉断连期间的变化。
func (c *compactor) runWatchLoop(stopCh chan struct{}) {
	ctx := wait.ContextForChannel(stopCh)
	for {
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return
		}
		compactRev, currentRev, err := c.getCompactRev(ctx)
		if err != nil {
			klog.Errorf("etcd: endpoint (%v): %v", c.client.Endpoints(), err)
			continue
		}
		c.UpdateCompactRevision(compactRev)
		// 从 currentRev 开始 watch，保证不会跳过断连窗口内的更新。
		watch := c.client.Watch(ctx, compactRevKey, clientv3.WithRev(currentRev))
		for resp := range watch {
			compactRev, err := c.fromWatchResponse(resp)
			if err != nil {
				klog.Errorf("etcd: endpoint (%v): %v", c.client.Endpoints(), err)
				continue
			}
			c.UpdateCompactRevision(compactRev)
		}
	}
}

// getCompactRev 通过读取 compact_rev_key 下的值返回 etcd 的压缩 revision。
func (c *compactor) getCompactRev(ctx context.Context) (compactRev int64, currentRev int64, err error) {
	resp, err := c.client.Get(ctx, compactRevKey)
	if err != nil {
		return compactRev, currentRev, fmt.Errorf("get %q failed: %w", compactRevKey, err)
	}
	if len(resp.Kvs) != 0 {
		compactRev, err = strconv.ParseInt(string(resp.Kvs[0].Value), 10, 64)
		if err != nil {
			return compactRev, currentRev, fmt.Errorf("failed to parse compact revision: %w", err)
		}
	}
	if resp.Header == nil {
		return compactRev, currentRev, fmt.Errorf("empty response header")
	}
	currentRev = resp.Header.Revision
	return compactRev, currentRev, nil
}

// fromWatchResponse 从 watch 事件的最后一个值解析 compact revision；
// 空事件（如进度通知）返回 0 表示无需更新。
func (c *compactor) fromWatchResponse(resp clientv3.WatchResponse) (compactRev int64, err error) {
	if resp.Err() != nil {
		return 0, resp.Err()
	}
	if len(resp.Events) == 0 {
		return 0, nil
	}
	lastEvent := resp.Events[len(resp.Events)-1]
	compactRev, err = strconv.ParseInt(string(lastEvent.Kv.Value), 10, 64)
	if err != nil {
		return compactRev, fmt.Errorf("failed to parse compact revision: %w", err)
	}
	return compactRev, nil
}
