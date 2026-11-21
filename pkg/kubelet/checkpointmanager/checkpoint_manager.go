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

package checkpointmanager

import (
	"fmt"
	"sync"

	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager/errors"
	utilstore "k8s.io/kubernetes/pkg/kubelet/util/store"
	utilfs "k8s.io/kubernetes/pkg/util/filesystem"
)

// Checkpoint 定义进程 checkpoint 数据的序列化与校验接口。
type Checkpoint interface {
	// MarshalCheckpoint 把 checkpoint 序列化为字节。
	MarshalCheckpoint() ([]byte, error)
	// UnmarshalCheckpoint 从字节反序列化 checkpoint。
	UnmarshalCheckpoint(blob []byte) error
	// VerifyChecksum 校验 checkpoint 的 checksum。
	VerifyChecksum() error
}

// CheckpointManager 提供 checkpoint 管理接口。
type CheckpointManager interface {
	// CreateCheckpoint 把 checkpoint 持久化到 CheckpointStore。
	// checkpointKey 是 utilstore 定位 checkpoint 的键；
	// 对基于文件的 utilstore 而言就是写入的文件名。
	CreateCheckpoint(checkpointKey string, checkpoint Checkpoint) error
	// GetCheckpoint 从 CheckpointStore 读取 checkpoint。
	GetCheckpoint(checkpointKey string, checkpoint Checkpoint) error
	// 警告：checkpoint 不存在时 RemoveCheckpoint 不会返回错误。
	RemoveCheckpoint(checkpointKey string) error
	// ListCheckpoints 返回现有 checkpoint 的列表。
	ListCheckpoints() ([]string, error)
}

// impl 是 CheckpointManager 的实现，把 checkpoint 持久化到 CheckpointStore。
type impl struct {
	// path 是 checkpoint 目录。
	path string
	// store 是底层存储实现。
	store utilstore.Store
	// mutex 保护底层存储的并发访问。
	mutex sync.Mutex
}

// NewCheckpointManager 创建 checkpoint 管理器。
func NewCheckpointManager(checkpointDir string) (CheckpointManager, error) {
	fstore, err := utilstore.NewFileStore(checkpointDir, &utilfs.DefaultFs{})
	if err != nil {
		return nil, err
	}

	return &impl{path: checkpointDir, store: fstore}, nil
}

// CreateCheckpoint 把 checkpoint 持久化到 CheckpointStore。
func (manager *impl) CreateCheckpoint(checkpointKey string, checkpoint Checkpoint) error {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	// 先序列化再写入存储。
	blob, err := checkpoint.MarshalCheckpoint()
	if err != nil {
		return err
	}
	return manager.store.Write(checkpointKey, blob)
}

// GetCheckpoint 从 CheckpointStore 读取 checkpoint。
func (manager *impl) GetCheckpoint(checkpointKey string, checkpoint Checkpoint) error {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	blob, err := manager.store.Read(checkpointKey)
	if err != nil {
		// 统一转换为 checkpoint 不存在的专用错误。
		if err == utilstore.ErrKeyNotFound {
			return errors.ErrCheckpointNotFound
		}
		return err
	}
	// 反序列化后必须校验 checksum，防止数据损坏。
	err = checkpoint.UnmarshalCheckpoint(blob)
	if err == nil {
		err = checkpoint.VerifyChecksum()
	}
	return err
}

// RemoveCheckpoint 删除 checkpoint；不存在时不返回错误。
func (manager *impl) RemoveCheckpoint(checkpointKey string) error {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	return manager.store.Delete(checkpointKey)
}

// ListCheckpoints 返回现有 checkpoint 的列表。
func (manager *impl) ListCheckpoints() ([]string, error) {
	manager.mutex.Lock()
	defer manager.mutex.Unlock()
	keys, err := manager.store.List()
	if err != nil {
		return []string{}, fmt.Errorf("failed to list checkpoint store: %v", err)
	}
	return keys, nil
}
