/*
Copyright 2023 The Kubernetes Authors.

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

package state

import (
	"errors"
	"fmt"
	"sync"

	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager"
	checkpointerrors "k8s.io/kubernetes/pkg/kubelet/checkpointmanager/errors"
)

// Checkpointer 负责读取和保存 DRA 状态的 checkpoint。
type Checkpointer interface {
	// GetOrCreate 读取 checkpoint；不存在时创建空 checkpoint。
	GetOrCreate() (*Checkpoint, error)
	// Store 保存 checkpoint。
	Store(*Checkpoint) error
}

// checkpointer 是基于 checkpointmanager 的 Checkpointer 实现。
type checkpointer struct {
	// RWMutex 保护 checkpoint 文件的读写。
	sync.RWMutex
	// checkpointManager 是底层的 checkpoint 管理器。
	checkpointManager checkpointmanager.CheckpointManager
	// checkpointName 是 checkpoint 文件名。
	checkpointName string
}

// NewCheckpointer 创建使用 checkpoint 持久化 Claim 信息的 Checkpointer。
func NewCheckpointer(stateDir, checkpointName string) (Checkpointer, error) {
	// checkpoint 名称为空时无法定位文件，直接报错。
	if len(checkpointName) == 0 {
		return nil, fmt.Errorf("received empty string instead of checkpointName")
	}

	checkpointManager, err := checkpointmanager.NewCheckpointManager(stateDir)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize checkpoint manager: %w", err)
	}

	checkpointer := &checkpointer{
		checkpointManager: checkpointManager,
		checkpointName:    checkpointName,
	}

	return checkpointer, nil
}

// GetOrCreate 从 checkpoint 读取 Claim 状态列表；
// checkpoint 不存在时创建空列表。
func (sc *checkpointer) GetOrCreate() (*Checkpoint, error) {
	// 加写锁，因为不存在时可能需要创建并写入 checkpoint。
	sc.Lock()
	defer sc.Unlock()

	checkpoint, err := NewCheckpoint(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create new checkpoint: %w", err)
	}

	err = sc.checkpointManager.GetCheckpoint(sc.checkpointName, checkpoint)
	if errors.Is(err, checkpointerrors.ErrCheckpointNotFound) {
		// checkpoint 不存在时创建空 checkpoint 并落盘。
		err = sc.store(checkpoint)
		if err != nil {
			return nil, fmt.Errorf("failed to store checkpoint %v: %w", sc.checkpointName, err)
		}
		return checkpoint, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get checkpoint %v: %w", sc.checkpointName, err)
	}

	return checkpoint, nil
}

// Store 把 checkpoint 写入文件。
func (sc *checkpointer) Store(checkpoint *Checkpoint) error {
	sc.Lock()
	defer sc.Unlock()

	return sc.store(checkpoint)
}

// store 保存 checkpoint，调用方需自行加锁。
func (sc *checkpointer) store(checkpoint *Checkpoint) error {
	if err := sc.checkpointManager.CreateCheckpoint(sc.checkpointName, checkpoint); err != nil {
		return fmt.Errorf("could not save checkpoint %s: %w", sc.checkpointName, err)
	}
	return nil
}
