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

// StartCompactorPerEndpoint starts a compactor but prevents multiple compactors per client endpoint.
// Calling the function twice on single endpoint will return the same compactor.
// Compactor removes old version of keys from etcd.
// By default, we save the most recent 5 minutes data and compact versions > 5minutes ago.
// It should be enough for slow watchers and to tolerate burst.
func StartCompactorPerEndpoint(client *clientv3.Client, compactInterval time.Duration) *compactor {
	endpointsMapMu.Lock()
	defer endpointsMapMu.Unlock()

	// In one process, we can have only one compactor for one cluster.
	// Currently we rely on endpoints to differentiate clusters.
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

// NewCompactor runs dedicated compactor, that removes old versions of keys from etcd.
// Prefer calling StartCompactorPerEndpoint as this function doesn't prevent multiple compactors running per single endpoint.
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
	// Prevent integration tests stopping compactor twice.
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

// compactor periodically compacts historical versions of keys in etcd.
// It will compact keys with versions older than given interval.
// In other words, after compaction, it will only contain keys set during last interval.
// Any API call for the older versions of keys will return error.
// Interval is the time interval between each compaction. The first compaction happens after "interval".
func (c *compactor) runCompactLoop(stopCh chan struct{}) {
	// Technical definitions:
	// We have a special key in etcd defined as *compactRevKey*.
	// compactRevKey's value will be set to the string of last compacted revision.
	// compactRevKey's version will be used as logical time for comparison. THe version is referred as compact time.
	// Initially, because the key doesn't exist, the compact time (version) is 0.
	//
	// Algorithm:
	// - Compare to see if (local compact_time) = (remote compact_time).
	// - If yes, increment both local and remote compact_time, and do a compaction.
	// - If not, set local to remote compact_time.
	//
	// Technical details/insights:
	//
	// The protocol here is lease based. If one compactor CAS successfully, the others would know it when they fail in
	// CAS later and would try again in 5 minutes. If an APIServer crashed, another one would "take over" the lease.
	//
	// For example, in the following diagram, we have a compactor C1 doing compaction in t1, t2. Another compactor C2
	// at t1' (t1 < t1' < t2) would CAS fail, set its known oldRev to rev at t1', and try again in t2' (t2' > t2).
	// If C1 crashed and wouldn't compact at t2, C2 would CAS successfully at t2'.
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
	// We have the guarantees:
	// - in normal cases, the interval is 5 minutes.
	// - in failover, the interval is >5m and <10m
	//
	// FAQ:
	// - What if time is not accurate? We don't care as long as someone did the compaction. Atomicity is ensured using
	//   etcd API.
	// - What happened under heavy load scenarios? Initially, each apiserver will do only one compaction
	//   every 5 minutes. This is very unlikely affecting or affected w.r.t. server load.

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

// Compact compacts etcd store and returns current rev.
// It will return the current Compact time and global revision if no error occurred.
// Note that CAS fail will not incur any error.
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
		// We don't compact on bootstrap.
		return currentVersion, currentRev, 0, nil
	}
	if _, err = client.Compact(ctx, rev); err != nil {
		return currentVersion, currentRev, 0, err
	}
	klog.V(4).Infof("etcd: compacted rev (%d), endpoints (%v)", rev, client.Endpoints())
	return currentVersion, currentRev, rev, nil
}

// runWatchLoops gets and watches changes to compact_rev_key and updates the compaction revision based on it.
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

// getCompactRev returns the etcd compaction revision by reading the value stored under compact_rev_key.
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
