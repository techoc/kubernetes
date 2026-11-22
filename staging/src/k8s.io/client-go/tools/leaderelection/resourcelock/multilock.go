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

// Package resourcelock 提供主节点选举（leader election）所用的锁后端抽象，
// 目前实际可用的是基于 coordination.k8s.io Lease 的 LeaseLock。
package resourcelock

import (
	"bytes"
	"context"
	"encoding/json"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

const (
	// Deprecated: UnknownLeader is only used by MultiLock, which is deprecated.
	//
	// 已弃用：仅在 MultiLock 中作为“两个锁的持有者不一致”的占位值。
	UnknownLeader = "leaderelection.k8s.io/unknown"
)

// Deprecated: MultiLock was used to facilitate migration from non-lease
// based leader election to lease-based leader election. Support for
// non-lease locks was removed in Kubernetes 1.28, making MultiLock
// non-functional. Use LeaseLock directly instead.
//
// 已弃用：MultiLock 仅用于从非 Lease 锁迁移到 Lease 锁的过渡期。1.28 起非 Lease
// 锁已被移除，MultiLock 实际上已不可用，请直接使用 LeaseLock。
type MultiLock struct {
	Primary   Interface
	Secondary Interface
}

// Get returns the older election record of the lock
func (ml *MultiLock) Get(ctx context.Context) (*LeaderElectionRecord, []byte, error) {
	primary, primaryRaw, err := ml.Primary.Get(ctx)
	if err != nil {
		return nil, nil, err
	}

	secondary, secondaryRaw, err := ml.Secondary.Get(ctx)
	if err != nil {
		// Lock is held by old client
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

// Create attempts to create both primary lock and secondary lock
func (ml *MultiLock) Create(ctx context.Context, ler LeaderElectionRecord) error {
	// Primary 已存在不算失败，继续创建 Secondary，以兼容迁移期间已存在的旧锁。
	err := ml.Primary.Create(ctx, ler)
	if err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return ml.Secondary.Create(ctx, ler)
}

// Update will update and existing annotation on both two resources.
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

// RecordEvent in leader election while adding meta-data
func (ml *MultiLock) RecordEvent(s string) {
	ml.Primary.RecordEvent(s)
	ml.Secondary.RecordEvent(s)
}

// Describe is used to convert details on current resource lock
// into a string
func (ml *MultiLock) Describe() string {
	return ml.Primary.Describe()
}

// Identity returns the Identity of the lock
func (ml *MultiLock) Identity() string {
	return ml.Primary.Identity()
}

// Deprecated: ConcatRawRecord is only used by MultiLock, which is deprecated.
//
// 已弃用：仅被 MultiLock 用来把两把锁的原始记录拼接成一个诊断用字节串。
func ConcatRawRecord(primaryRaw, secondaryRaw []byte) []byte {
	return bytes.Join([][]byte{primaryRaw, secondaryRaw}, []byte(","))
}
