/*
Copyright 2019 The Kubernetes Authors.

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

// Package resourcelock 提供领导者选举（leader election）所用的锁后端抽象，
// 目前实际可用的是基于 coordination.k8s.io Lease 的 LeaseLock。
package resourcelock

import (
	"bytes"
	"context"
	"encoding/json"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

const (
	// Deprecated: UnknownLeader 仅被（同样已弃用的）MultiLock 使用。
	//
	// 已弃用：仅在 MultiLock 中作为“两个锁的持有者不一致”的占位值。
	UnknownLeader = "leaderelection.k8s.io/unknown"
)

// Deprecated: MultiLock 曾用于从非 Lease 锁向 Lease 锁的迁移。
// Kubernetes 1.28 移除了对非 Lease 锁的支持，MultiLock 已失去作用，
// 请直接使用 LeaseLock。
//
// 已弃用：MultiLock 仅用于从非 Lease 锁迁移到 Lease 锁的过渡期。1.28 起非 Lease
// 锁已被移除，MultiLock 实际上已不可用，请直接使用 LeaseLock。
type MultiLock struct {
	Primary   Interface
	Secondary Interface
}

// Get 返回该锁中较旧的选举记录
func (ml *MultiLock) Get(ctx context.Context) (*LeaderElectionRecord, []byte, error) {
	primary, primaryRaw, err := ml.Primary.Get(ctx)
	if err != nil {
		return nil, nil, err
	}

	secondary, secondaryRaw, err := ml.Secondary.Get(ctx)
	if err != nil {
		// 锁被旧客户端持有
		// Secondary 锁不存在且 Primary 由他人持有：说明是老客户端只写入了 Primary，
		// 此时以 Primary 记录为准，避免误判为无主。
		if apierrors.IsNotFound(err) && primary.HolderIdentity != ml.Identity() {
			return primary, primaryRaw, nil
		}
		return nil, nil, err
	}

	// 两把锁的持有者不一致属于迁移中的中间态，标记为 UnknownLeader 让上层按“无主”处理。
	if primary.HolderIdentity != secondary.HolderIdentity {
		primary.HolderIdentity = UnknownLeader
		primaryRaw, err = json.Marshal(primary)
		if err != nil {
			return nil, nil, err
		}
	}
	return primary, ConcatRawRecord(primaryRaw, secondaryRaw), nil
}

// Create 尝试同时创建主锁和次锁
func (ml *MultiLock) Create(ctx context.Context, ler LeaderElectionRecord) error {
	// Primary 已存在不算失败，继续创建 Secondary，以兼容迁移期间已存在的旧锁。
	err := ml.Primary.Create(ctx, ler)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return ml.Secondary.Create(ctx, ler)
}

// Update 会更新两个资源上已有的注解。
func (ml *MultiLock) Update(ctx context.Context, ler LeaderElectionRecord) error {
	err := ml.Primary.Update(ctx, ler)
	if err != nil {
		return err
	}
	// Secondary 可能尚未创建（老集群只有 Primary），此时补齐创建而非直接更新。
	_, _, err = ml.Secondary.Get(ctx)
	if err != nil && apierrors.IsNotFound(err) {
		return ml.Secondary.Create(ctx, ler)
	}
	return ml.Secondary.Update(ctx, ler)
}

// RecordEvent 记录领导者选举事件，并附加元数据
func (ml *MultiLock) RecordEvent(s string) {
	ml.Primary.RecordEvent(s)
	ml.Secondary.RecordEvent(s)
}

// Describe 把当前资源锁的细节转换为字符串
func (ml *MultiLock) Describe() string {
	return ml.Primary.Describe()
}

// Identity 返回锁的 Identity
func (ml *MultiLock) Identity() string {
	return ml.Primary.Identity()
}

// Deprecated: ConcatRawRecord 仅被（同样已弃用的）MultiLock 使用。
//
// 已弃用：仅被 MultiLock 用来把两把锁的原始记录拼接成一个诊断用字节串。
func ConcatRawRecord(primaryRaw, secondaryRaw []byte) []byte {
	return bytes.Join([][]byte{primaryRaw, secondaryRaw}, []byte(","))
}
