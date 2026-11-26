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

// Package etcd3 实现基于 etcd v3 的存储后端，包括 lease 复用、watcher
// 缓存与对象大小估算等。
package etcd3

import (
	"context"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/apiserver/pkg/storage/etcd3/metrics"
)

const (
	// 默认复用窗口 60 秒；在一个 lease 的生命周期内尽量合并写入，
	// 减少昂贵的 etcd LeaseGrant 调用。单个 lease 最多承载 1000 个
	// 对象，避免一个 lease 过期造成过大的批量删除风暴。
	defaultLeaseReuseDurationSeconds = 60
	defaultLeaseMaxObjectCount       = 1000
)

// LeaseManagerConfig 是创建 lease manager 的配置。
type LeaseManagerConfig struct {
	// ReuseDurationSeconds 指定每个 lease 复用的秒数
	ReuseDurationSeconds int64
	// MaxObjectCount 指定一个 lease 可关联的对象数
	MaxObjectCount int64
}

// NewDefaultLeaseManagerConfig 创建使用默认值的 LeaseManagerConfig
func NewDefaultLeaseManagerConfig() LeaseManagerConfig {
	return LeaseManagerConfig{
		ReuseDurationSeconds: defaultLeaseReuseDurationSeconds,
		MaxObjectCount:       defaultLeaseMaxObjectCount,
	}
}

// leaseManager 管理从 etcd 请求的 lease。若新写入
// 需要一个过期时间与上一次相近的 lease，就复用旧的，
// 以降低 etcd 开销——lease 操作很昂贵。实现中只保存一个旧 lease，
// 因为所有事件的 ttl 都相同。
//
// leaseManager 管理 etcd lease。所有事件的 TTL 相同，因此只需缓存上一个
// lease；复用可以显著降低 LeaseGrant 调用频率。leaseMu 保护以下所有
// 可变字段，GetLease 是唯一入口。
type leaseManager struct {
	client                  *clientv3.Client // etcd client used to grant leases
	leaseMu                 sync.Mutex
	prevLeaseID             clientv3.LeaseID
	prevLeaseExpirationTime time.Time
	// 每个 lease 的复用时长（秒）与 TTL 百分比。
	// 取两者中较小值，避免出现不合理的大数。
	leaseReuseDurationSeconds   int64
	leaseReuseDurationPercent   float64
	leaseMaxAttachedObjectCount int64
	leaseAttachedObjectCount    int64
}

// newDefaultLeaseManager 用默认设置创建一个新的 lease manager。
//
// 默认复用策略为 TTL 的 5%，同时受配置的秒数上限约束；MaxObjectCount
// 非正时回退到默认值，避免配置错误导致 lease 被无限复用。
func newDefaultLeaseManager(client *clientv3.Client, config LeaseManagerConfig) *leaseManager {
	if config.MaxObjectCount <= 0 {
		config.MaxObjectCount = defaultLeaseMaxObjectCount
	}
	return newLeaseManager(client, config.ReuseDurationSeconds, 0.05, config.MaxObjectCount)
}

// newLeaseManager 用指定的缓存 lease 数、复用秒数与百分比创建新的 lease manager。
//
// 百分比值 x 表示 x*100%。
func newLeaseManager(client *clientv3.Client, leaseReuseDurationSeconds int64, leaseReuseDurationPercent float64, maxObjectCount int64) *leaseManager {
	return &leaseManager{
		client:                      client,
		leaseReuseDurationSeconds:   leaseReuseDurationSeconds,
		leaseReuseDurationPercent:   leaseReuseDurationPercent,
		leaseMaxAttachedObjectCount: maxObjectCount,
	}
}

// GetLease 根据请求的 ttl 返回 lease：缓存的旧 lease 可复用就复用，
// 否则向 etcd 请求新的。
//
// GetLease 的复用条件必须同时满足：
//  1. 旧 lease 在 now+ttl 时仍未过期（valid），保证写入的对象活到预期
//     时间；
//  2. 旧 lease 不会"太晚"过期（sufficient），避免为短 TTL 写入复用
//     一个即将到期的长 lease 导致对象提前消失；
//  3. 已附着对象数未超过上限。
//
// 持锁执行是为了让计数与 lease 缓存保持一致，代价是同一时刻只有一个
// LeaseGrant 在途。
func (l *leaseManager) GetLease(ctx context.Context, ttl int64) (clientv3.LeaseID, error) {
	now := time.Now()
	l.leaseMu.Lock()
	defer l.leaseMu.Unlock()
	// 检查旧 lease 是否可复用
	reuseDurationSeconds := l.getReuseDurationSecondsLocked(ttl)
	valid := now.Add(time.Duration(ttl) * time.Second).Before(l.prevLeaseExpirationTime)
	sufficient := now.Add(time.Duration(ttl+reuseDurationSeconds) * time.Second).After(l.prevLeaseExpirationTime)

	// 统计同一 lease 中的所有操作，无论成功或失败。
	// 目前每次 GetLease 调用只关联 1 个对象
	l.leaseAttachedObjectCount++

	if valid && sufficient && l.leaseAttachedObjectCount <= l.leaseMaxAttachedObjectCount {
		return l.prevLeaseID, nil
	}

	// 向 etcd 请求一个稍长 ttl 的 lease
	// 多要一个复用窗口，使 lease 在最后一个对象 TTL 到期后仍短暂存活。
	ttl += reuseDurationSeconds
	lcr, err := l.client.Lease.Grant(ctx, ttl)
	if err != nil {
		return clientv3.LeaseID(0), err
	}
	// 缓存新的 lease id
	l.prevLeaseID = lcr.ID
	l.prevLeaseExpirationTime = now.Add(time.Duration(ttl) * time.Second)
	// 刷新计数
	metrics.UpdateLeaseObjectCount(l.leaseAttachedObjectCount)
	l.leaseAttachedObjectCount = 1
	return lcr.ID, nil
}

// getReuseDurationSecondsLocked 根据配置返回可复用的秒数。
// 调用前必须持有锁。
//
// 取 TTL 百分比与绝对秒数上限中的较小值，防止 TTL 很大时复用窗口随之
// 膨胀。
func (l *leaseManager) getReuseDurationSecondsLocked(ttl int64) int64 {
	reuseDurationSeconds := int64(l.leaseReuseDurationPercent * float64(ttl))
	if reuseDurationSeconds > l.leaseReuseDurationSeconds {
		reuseDurationSeconds = l.leaseReuseDurationSeconds
	}
	return reuseDurationSeconds
}
