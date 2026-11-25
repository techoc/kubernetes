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

package resourcelock

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coordinationv1client "k8s.io/client-go/kubernetes/typed/coordination/v1"
)

// LeaseLock 基于 coordination.k8s.io/v1 Lease 对象实现 LeaderElection 的资源锁。
type LeaseLock struct {
	// LeaseMeta 应包含 LeaderElector 尝试持有的 Lease 对象的
	// Name 与 Namespace。
	LeaseMeta  metav1.ObjectMeta
	Client     coordinationv1client.LeasesGetter
	LockConfig ResourceLockConfig
	// lease 缓存最近一次 Get/Create/Update 得到的对象，Update 依赖它携带 resourceVersion。
	lease  *coordinationv1.Lease
	Labels map[string]string
}

// Get 从 Lease spec 中返回选举记录
//
// Get 读取 Lease 并转换为内部的 LeaderElectionRecord；
// 同时把对象缓存到 ll.lease，供后续 Update 使用。
func (ll *LeaseLock) Get(ctx context.Context) (*LeaderElectionRecord, []byte, error) {
	lease, err := ll.Client.Leases(ll.LeaseMeta.Namespace).Get(ctx, ll.LeaseMeta.Name, metav1.GetOptions{})
	if err != nil {
		return nil, nil, err
	}
	ll.lease = lease
	record := LeaseSpecToLeaderElectionRecord(&ll.lease.Spec)
	recordByte, err := json.Marshal(*record)
	if err != nil {
		return nil, nil, err
	}
	return record, recordByte, nil
}

// Create 尝试创建 Lease
//
// Create 用给定的选举记录创建 Lease；只有当前没有 holder 时才会成功。
func (ll *LeaseLock) Create(ctx context.Context, ler LeaderElectionRecord) error {
	var err error
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ll.LeaseMeta.Name,
			Namespace: ll.LeaseMeta.Namespace,
			Labels:    ll.Labels,
		},
		Spec: LeaderElectionRecordToLeaseSpec(&ler),
	}

	ll.lease, err = ll.Client.Leases(ll.LeaseMeta.Namespace).Create(ctx, lease, metav1.CreateOptions{})
	return err
}

// Update 更新已有 Lease 的 spec。
//
// Update 更新已有 Lease；必须先前调用过 Get 或 Create，否则无法确定 resourceVersion。
func (ll *LeaseLock) Update(ctx context.Context, ler LeaderElectionRecord) error {
	if ll.lease == nil {
		return errors.New("lease not initialized, call get or create first")
	}
	ll.lease.Spec = LeaderElectionRecordToLeaseSpec(&ler)

	if ll.Labels != nil {
		if ll.lease.Labels == nil {
			ll.lease.Labels = map[string]string{}
		}
		// 只覆盖明确设置的标签
		// 保留未在 ll.Labels 中声明的既有标签，避免覆盖其他控制器写入的元数据。
		for k, v := range ll.Labels {
			ll.lease.Labels[k] = v
		}
	}

	lease, err := ll.Client.Leases(ll.LeaseMeta.Namespace).Update(ctx, ll.lease, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	ll.lease = lease
	return nil
}

// RecordEvent 记录领导者选举事件，并附加元数据
//
// RecordEvent 以 Lease 为 involved object 记录一条 LeaderElection 事件。
func (ll *LeaseLock) RecordEvent(s string) {
	if ll.LockConfig.EventRecorder == nil {
		return
	}
	events := fmt.Sprintf("%v %v", ll.LockConfig.Identity, s)
	subject := &coordinationv1.Lease{ObjectMeta: ll.lease.ObjectMeta}
	// 填充 type meta，这样就不必从 schema 中获取
	subject.Kind = "Lease"
	subject.APIVersion = coordinationv1.SchemeGroupVersion.String()
	ll.LockConfig.EventRecorder.Eventf(subject, corev1.EventTypeNormal, "LeaderElection", "%s", events)
}

// Describe 把当前资源锁的细节转换为字符串
//
// Describe 返回 "namespace/name" 形式的锁描述，用于日志。
func (ll *LeaseLock) Describe() string {
	return fmt.Sprintf("%v/%v", ll.LeaseMeta.Namespace, ll.LeaseMeta.Name)
}

// Identity 返回锁的 Identity
//
// Identity 返回持有者标识。
func (ll *LeaseLock) Identity() string {
	return ll.LockConfig.Identity
}

// LeaseSpecToLeaderElectionRecord 把 LeaseSpec 转成内部选举记录；
// 指针字段为 nil 时保持零值。
func LeaseSpecToLeaderElectionRecord(spec *coordinationv1.LeaseSpec) *LeaderElectionRecord {
	var r LeaderElectionRecord
	if spec.HolderIdentity != nil {
		r.HolderIdentity = *spec.HolderIdentity
	}
	if spec.LeaseDurationSeconds != nil {
		r.LeaseDurationSeconds = int(*spec.LeaseDurationSeconds)
	}
	if spec.LeaseTransitions != nil {
		r.LeaderTransitions = int(*spec.LeaseTransitions)
	}
	if spec.AcquireTime != nil {
		r.AcquireTime = metav1.Time{Time: spec.AcquireTime.Time}
	}
	if spec.RenewTime != nil {
		r.RenewTime = metav1.Time{Time: spec.RenewTime.Time}
	}
	if spec.PreferredHolder != nil {
		r.PreferredHolder = *spec.PreferredHolder
	}
	if spec.Strategy != nil {
		r.Strategy = *spec.Strategy
	}
	return &r

}

// LeaderElectionRecordToLeaseSpec 把内部选举记录转成 LeaseSpec；
// 空字符串的可选字段不设置指针，避免写入无意义的值。
func LeaderElectionRecordToLeaseSpec(ler *LeaderElectionRecord) coordinationv1.LeaseSpec {
	leaseDurationSeconds := int32(ler.LeaseDurationSeconds)
	leaseTransitions := int32(ler.LeaderTransitions)
	spec := coordinationv1.LeaseSpec{
		HolderIdentity:       &ler.HolderIdentity,
		LeaseDurationSeconds: &leaseDurationSeconds,
		AcquireTime:          &metav1.MicroTime{Time: ler.AcquireTime.Time},
		RenewTime:            &metav1.MicroTime{Time: ler.RenewTime.Time},
		LeaseTransitions:     &leaseTransitions,
	}
	if ler.PreferredHolder != "" {
		spec.PreferredHolder = &ler.PreferredHolder
	}
	if ler.Strategy != "" {
		spec.Strategy = &ler.Strategy
	}
	return spec
}
