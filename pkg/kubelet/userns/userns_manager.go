//go:build !windows

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

package userns

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/kubeletconfig"
	utilstore "k8s.io/kubernetes/pkg/kubelet/util/store"
	"k8s.io/kubernetes/pkg/registry/core/service/allocator"
	utilfs "k8s.io/kubernetes/pkg/util/filesystem"
)

const (
	// 移除足够多的 Pod 后重建 map，避免内存泄漏，因为 Go 的 map 永远不会释放内存。
	mapReInitializeThreshold = 1000

	// userNsUnitLength 是用户命名空间的单位长度。
	userNsUnitLength = 65536
)

// UsernsManager 管理节点上的用户命名空间分配：把 Pod 映射到互不重叠的 ID 范围，
// 并负责映射的持久化、恢复与回收。
type UsernsManager struct {
	used    *allocator.AllocationBitmap
	usedBy  map[types.UID]uint32 // 将 pod.UID 映射到其占用的范围
	removed int

	off int
	len int

	userNsLength uint32

	kl userNsPodsManager
	// 该锁保护除 kl 内部管理状态之外的所有成员。
	lock sync.Mutex
}

// userNamespace 保存用户命名空间的配置。
type userNamespace struct {
	// 用户命名空间的 UID 映射。
	UIDMappings []idMapping `json:"uidMappings"`
	// 用户命名空间的 GID 映射。
	GIDMappings []idMapping `json:"gidMappings"`
}

// idMapping 描述 Pod 用户命名空间中的一条 ID 映射。
type idMapping struct {
	// 必填。
	HostId uint32 `json:"hostId"`
	// 必填。
	ContainerId uint32 `json:"containerId"`
	// 必填。
	Length uint32 `json:"length"`
}

// mappingsFile 是持久化用户命名空间映射的文件名。
const mappingsFile = "userns"

// writeMappingsToFile 把指定的用户命名空间配置写入 Pod 目录。
func (m *UsernsManager) writeMappingsToFile(pod types.UID, userNs userNamespace) error {
	dir := m.kl.GetPodDir(pod)

	data, err := json.Marshal(userNs)
	if err != nil {
		return err
	}

	fstore, err := utilstore.NewFileStore(dir, &utilfs.DefaultFs{})
	if err != nil {
		return fmt.Errorf("create user namespace store: %w", err)
	}
	if err := fstore.Write(mappingsFile, data); err != nil {
		return err
	}

	// 需要 fsync 父目录才能保证文件确实存在。fstore 只保证原子写入，这里还需要持久性。
	parentDir, err := os.Open(dir)
	if err != nil {
		return err
	}

	if err = parentDir.Sync(); err != nil {
		// 这里忽略返回值，因为已经有一个错误需要上报。
		parentDir.Close()
		return err
	}

	return parentDir.Close()
}

// readMappingsFromFile 从 Pod 目录读取用户命名空间配置。
func (m *UsernsManager) readMappingsFromFile(pod types.UID) ([]byte, error) {
	dir := m.kl.GetPodDir(pod)
	fstore, err := utilstore.NewFileStore(dir, &utilfs.DefaultFs{})
	if err != nil {
		return nil, fmt.Errorf("create user namespace store: %w", err)
	}
	return fstore.Read(mappingsFile)
}

// MakeUserNsManager 创建 UsernsManager：校验 kubelet 的 ID 映射配置，初始化分配位图，
// 并从磁盘恢复已有 Pod 的用户命名空间映射。
func MakeUserNsManager(logger klog.Logger, kl userNsPodsManager, idsPerPod *int64) (*UsernsManager, error) {
	if !utilfeature.DefaultFeatureGate.Enabled(features.UserNamespacesSupport) {
		return nil, nil
	}

	userNsLength := uint32(kubeletconfig.DefaultKubeletUserNamespacesIDsPerPod)
	if idsPerPod != nil {
		// 该值已在 kubelet 配置校验阶段验证过，因此可以安全地转换。
		userNsLength = uint32(*idsPerPod)
	}
	kubeletMappingID, kubeletMappingLen, err := kl.GetKubeletMappings(logger, userNsLength)
	if err != nil {
		return nil, fmt.Errorf("kubelet mappings: %w", err)
	}

	if userNsLength%userNsUnitLength != 0 {
		return nil, fmt.Errorf("kubelet user namespace length %v is not a multiple of %d", userNsLength, userNsUnitLength)
	}
	if kubeletMappingID%userNsLength != 0 {
		return nil, fmt.Errorf("kubelet user assigned ID %v is not a multiple of %v", kubeletMappingID, userNsLength)
	}
	if kubeletMappingID < userNsLength {
		// 不允许映射 0，否则安全机制会被绕过。
		return nil, fmt.Errorf("kubelet user assigned ID %v must be greater or equal to %v", kubeletMappingID, userNsLength)
	}
	if uint64(kubeletMappingID)+uint64(kubeletMappingLen) > math.MaxUint32 {
		// 2^32-1 在内核中是 INVALID_UID，无法被映射，必须排除在范围之外。
		// 越过它更糟：某些子范围（由 idsPerPod 控制）会回绕，意外包含比预期更低的 uid。
		return nil, fmt.Errorf("kubelet user assigned ID %v with assigned IDs length %v ends past the last mappable ID %v",
			kubeletMappingID, kubeletMappingLen, uint64(math.MaxUint32)-1)
	}
	if kubeletMappingLen%userNsLength != 0 {
		return nil, fmt.Errorf("kubelet user assigned IDs length %v is not a multiple of %v", kubeletMappingLen, userNsLength)
	}
	if kubeletMappingLen/userNsLength < uint32(kl.GetMaxPods()) {
		return nil, fmt.Errorf("kubelet user assigned IDs are not enough to support %v pods", kl.GetMaxPods())
	}
	off := int(kubeletMappingID / userNsLength)
	len := int(kubeletMappingLen / userNsLength)
	logger.V(5).Info("User namespace manager mapping", "offset", off, "length", len, "idsPerPod", userNsLength)

	m := UsernsManager{
		used:         allocator.NewAllocationMap(len, "user namespaces"),
		usedBy:       make(map[types.UID]uint32),
		kl:           kl,
		off:          off,
		len:          len,
		userNsLength: userNsLength,
	}

	found, err := kl.ListPodsFromDisk()
	if err != nil {
		if os.IsNotExist(err) {
			// Pod 目录尚不存在，说明还没有任何 Pod，返回空的管理器即可。
			return &m, nil
		}
		return nil, fmt.Errorf("read pods from disk: %w", err)

	}
	for _, podUID := range found {
		logger.V(5).Info("reading pod from disk for user namespace", "podUID", podUID)
		if err := m.recordPodMappings(logger, podUID); err != nil {
			return nil, fmt.Errorf("record pod mappings for existing pod %q: %w", podUID, err)
		}
	}

	return &m, nil
}

// recordPodMappings 在 Pod 目录中存在用户命名空间配置文件时，登记该文件使用的 ID 范围。
func (m *UsernsManager) recordPodMappings(logger klog.Logger, pod types.UID) error {
	content, err := m.readMappingsFromFile(pod)
	if err != nil && err != utilstore.ErrKeyNotFound {
		return err
	}

	// 内容为空说明该 Pod 没有用户命名空间，无需处理。
	if len(content) == 0 {
		return nil
	}

	_, err = m.parseUserNsFileAndRecord(logger, pod, content)
	return err
}

// isSet 检查指定索引是否已被占用。
func (m *UsernsManager) isSet(v uint32) bool {
	index := int(v/m.userNsLength) - m.off
	if index < 0 || index >= m.len {
		return true
	}
	return m.used.Has(index)
}

// allocateOne 找到一段空闲的用户命名空间并分配给指定 Pod。
// 第一个返回值是该用户命名空间的起始 ID，第二个返回值是范围长度。
func (m *UsernsManager) allocateOne(logger klog.Logger, pod types.UID) (firstID uint32, length uint32, err error) {
	firstZero, found, err := m.used.AllocateNext()
	if err != nil {
		return 0, 0, err
	}
	if !found {
		return 0, 0, fmt.Errorf("could not find an empty slot to allocate a user namespace")
	}

	logger.V(5).Info("new pod user namespace allocation", "podUID", pod)

	firstID = uint32((firstZero + m.off)) * m.userNsLength
	m.usedBy[pod] = firstID
	return firstID, m.userNsLength, nil
}

// record 把用户命名空间范围 [from, from+length] 登记给指定 Pod。
func (m *UsernsManager) record(logger klog.Logger, pod types.UID, from, length uint32) (err error) {
	if length != m.userNsLength {
		return fmt.Errorf("wrong user namespace length %v", length)
	}
	if from%m.userNsLength != 0 {
		return fmt.Errorf("wrong user namespace offset specified %v", from)
	}
	prevFrom, found := m.usedBy[pod]
	if found && prevFrom != from {
		return fmt.Errorf("different user namespace range already used by pod %q", pod)
	}
	index := int(from/m.userNsLength) - m.off
	if index < 0 || index >= m.len {
		return fmt.Errorf("id %v is out of range", from)
	}
	// Pod 未登记时，需确认该范围尚未被占用。
	if !found && m.used.Has(index) {
		return fmt.Errorf("range picked for pod %q already taken", pod)
	}
	// Pod 已登记且范围一致，无需处理。
	if found && prevFrom == from {
		return nil
	}

	logger.V(5).Info("new pod user namespace allocation", "podUID", pod)

	// from 是一个 ID（UID/GID），在位图中把对应的大小为 userNsLength 的 userns 标记为已占用。
	m.used.Allocate(index)
	m.usedBy[pod] = from
	return nil
}

// Release 释放分配给指定 Pod 的用户命名空间。
func (m *UsernsManager) Release(logger klog.Logger, podUID types.UID) {
	if !utilfeature.DefaultFeatureGate.Enabled(features.UserNamespacesSupport) {
		return
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	m.releaseWithLock(logger, podUID)
}

// podAllocated 判断该 Pod 是否已分配用户命名空间。
func (m *UsernsManager) podAllocated(podUID types.UID) bool {
	if !utilfeature.DefaultFeatureGate.Enabled(features.UserNamespacesSupport) {
		return false
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	_, ok := m.usedBy[podUID]
	return ok
}

// releaseWithLock 释放 Pod 的用户命名空间，调用方必须已持有 m.lock。
func (m *UsernsManager) releaseWithLock(logger klog.Logger, pod types.UID) {
	v, ok := m.usedBy[pod]
	if !ok {
		logger.V(5).Info("pod user namespace allocation not present", "podUID", pod)
		return
	}
	delete(m.usedBy, pod)

	logger.V(5).Info("releasing pod user namespace allocation", "podUID", pod)
	m.removed++

	// 删除落盘的映射文件，避免残留配置在下次启动时被误读。
	_ = os.Remove(filepath.Join(m.kl.GetPodDir(pod), mappingsFile))

	// Go 的 map 不会释放内存，累计移除足够多后重建一张新 map，避免内存持续增长。
	if m.removed%mapReInitializeThreshold == 0 {
		n := make(map[types.UID]uint32)
		for k, v := range m.usedBy {
			n[k] = v
		}
		m.usedBy = n
		m.removed = 0
	}
	_ = m.used.Release(int(v/m.userNsLength) - m.off)
}

// parseUserNsFileAndRecord 解析用户命名空间映射文件，校验格式后登记到分配状态中。
func (m *UsernsManager) parseUserNsFileAndRecord(logger klog.Logger, pod types.UID, content []byte) (userNs userNamespace, err error) {
	if err = json.Unmarshal([]byte(content), &userNs); err != nil {
		err = fmt.Errorf("invalid user namespace mappings file: %w", err)
		return
	}

	if len(userNs.UIDMappings) != 1 {
		err = fmt.Errorf("invalid user namespace configuration: no more than one mapping allowed.")
		return
	}

	if len(userNs.UIDMappings) != len(userNs.GIDMappings) {
		err = fmt.Errorf("invalid user namespace configuration: GID and UID mappings should be identical.")
		return
	}

	if userNs.UIDMappings[0] != userNs.GIDMappings[0] {
		err = fmt.Errorf("invalid user namespace configuration: GID and UID mapping should be identical")
		return
	}

	// 我们不会生成未映射 root 的配置，且部分运行时假设 root 已被映射。
	// 这里校验文件内容确实由我们生成、且可以安全解析。
	if userNs.UIDMappings[0].ContainerId != 0 {
		err = fmt.Errorf("invalid user namespace configuration: UID 0 must be mapped")
		return
	}

	if userNs.GIDMappings[0].ContainerId != 0 {
		err = fmt.Errorf("invalid user namespace configuration: GID 0 must be mapped")
		return
	}

	hostId := userNs.UIDMappings[0].HostId
	length := userNs.UIDMappings[0].Length

	err = m.record(logger, pod, hostId, length)
	return
}

// createUserNs 为 Pod 分配一段用户命名空间，生成映射配置并持久化到 Pod 目录。
func (m *UsernsManager) createUserNs(logger klog.Logger, pod *v1.Pod) (userNs userNamespace, err error) {
	firstID, length, err := m.allocateOne(logger, pod.UID)
	if err != nil {
		return
	}

	// 分配成功但后续写入失败时回滚，避免命名空间范围泄漏。
	defer func() {
		if err != nil {
			m.releaseWithLock(logger, pod.UID)
		}
	}()

	userNs = userNamespace{
		UIDMappings: []idMapping{
			{
				ContainerId: 0,
				HostId:      firstID,
				Length:      length,
			},
		},
		GIDMappings: []idMapping{
			{
				ContainerId: 0,
				HostId:      firstID,
				Length:      length,
			},
		},
	}

	return userNs, m.writeMappingsToFile(pod.UID, userNs)
}

// GetOrCreateUserNamespaceMappings 返回 sandbox 用户命名空间的配置，必要时为 Pod 创建并持久化映射。
func (m *UsernsManager) GetOrCreateUserNamespaceMappings(logger klog.Logger, pod *v1.Pod, runtimeHandler string) (*runtimeapi.UserNamespace, error) {
	featureEnabled := utilfeature.DefaultFeatureGate.Enabled(features.UserNamespacesSupport)

	// TODO: If the default value for hostUsers ever changes, change the default value of
	// userNamespacesEnabled as well
	if pod == nil || pod.Spec.HostUsers == nil {
		// 特性开启时，显式指定使用 node 模式……
		if featureEnabled {
			return &runtimeapi.UserNamespace{
				Mode: runtimeapi.NamespaceMode_NODE,
			}, nil
		}
		// ……否则连该字段都不指定
		return nil, nil
	}
	// 此时 pod.Spec.HostUsers 已被显式设置为 true/false
	if !featureEnabled {
		return nil, fmt.Errorf("the feature gate %q is disabled: can't set spec.HostUsers", features.UserNamespacesSupport)
	}
	if *pod.Spec.HostUsers {
		return &runtimeapi.UserNamespace{
			Mode: runtimeapi.NamespaceMode_NODE,
		}, nil
	}

	// 从这里开始，hostUsers=false 且特性门控已开启。

	// Pod 请求了用户命名空间，但运行时不支持时，返回错误。
	if handlerSupportsUserns, err := m.kl.HandlerSupportsUserNamespaces(runtimeHandler); err != nil || !handlerSupportsUserns {
		msg := "can't set `spec.hostUsers: false`, runtime does not support user namespaces"
		if runtimeHandler != "" {
			msg = fmt.Sprintf("can't set `spec.hostUsers: false`, RuntimeClass handler %q does not support user namespaces", runtimeHandler)
		}
		if err != nil {
			return nil, fmt.Errorf("%v: %w", msg, err)
		}
		return nil, fmt.Errorf("%v", msg)
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	content, err := m.readMappingsFromFile(pod.UID)
	if err != nil && err != utilstore.ErrKeyNotFound {
		return nil, err
	}

	var userNs userNamespace
	if string(content) != "" {
		userNs, err = m.parseUserNsFileAndRecord(logger, pod.UID, content)
		if err != nil {
			return nil, fmt.Errorf("user namespace: %w", err)
		}
	} else {
		userNs, err = m.createUserNs(logger, pod)
		if err != nil {
			return nil, fmt.Errorf("create user namespace: %w", err)
		}
	}

	var uids []*runtimeapi.IDMapping
	var gids []*runtimeapi.IDMapping

	for _, u := range userNs.UIDMappings {
		uids = append(uids, &runtimeapi.IDMapping{
			HostId:      u.HostId,
			ContainerId: u.ContainerId,
			Length:      u.Length,
		})
	}
	for _, g := range userNs.GIDMappings {
		gids = append(gids, &runtimeapi.IDMapping{
			HostId:      g.HostId,
			ContainerId: g.ContainerId,
			Length:      g.Length,
		})
	}

	return &runtimeapi.UserNamespace{
		Mode: runtimeapi.NamespaceMode_POD,
		Uids: uids,
		Gids: gids,
	}, nil
}

// CleanupOrphanedPodUsernsAllocations 将用户命名空间分配状态与实际运行的 Pod 对账，
// 释放所有孤立 Pod 的分配。
func (m *UsernsManager) CleanupOrphanedPodUsernsAllocations(ctx context.Context, pods []*v1.Pod, runningPods []*kubecontainer.Pod) error {
	if !utilfeature.DefaultFeatureGate.Enabled(features.UserNamespacesSupport) {
		return nil
	}
	logger := klog.FromContext(ctx)

	m.lock.Lock()
	defer m.lock.Unlock()

	allPods := sets.New[string]()
	for _, pod := range pods {
		allPods.Insert(string(pod.UID))
	}
	for _, pod := range runningPods {
		allPods.Insert(string(pod.ID))
	}

	allFound := sets.New[string]()
	found, err := m.kl.ListPodsFromDisk()
	if err != nil {
		return fmt.Errorf("user namespace: read pods from disk: %w", err)
	}

	for _, podUID := range found {
		allFound.Insert(string(podUID))
	}

	// 释放磁盘上“发现”但已不存在的 Pod 的分配。
	for _, podUID := range found {
		if allPods.Has(string(podUID)) {
			continue
		}

		logger.V(5).Info("Clean up orphaned pod user namespace possible allocation", "podUID", podUID)
		m.releaseWithLock(logger, podUID)
	}

	// 释放内存中仍登记、但磁盘上已不存在的 Pod 的分配。
	for podUID := range m.usedBy {
		if allFound.Has(string(podUID)) {
			continue
		}

		logger.V(5).Info("Clean up orphaned pod user namespace possible allocation", "podUID", podUID)
		m.releaseWithLock(logger, podUID)
	}

	return nil
}

// EnabledUserNamespacesSupport 返回 UserNamespacesSupport 特性门控是否开启。
func EnabledUserNamespacesSupport() bool {
	return utilfeature.DefaultFeatureGate.Enabled(features.UserNamespacesSupport)
}
