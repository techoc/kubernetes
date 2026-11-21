/*
Copyright 2021 The Kubernetes Authors.

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
	"encoding/json"
	"fmt"

	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager/checksum"
)

var _ checkpointmanager.Checkpoint = &Checkpoint{}

// PodResourceCheckpointInfo 是写入 checkpoint 的 Pod 资源分配信息。
type PodResourceCheckpointInfo struct {
	Entries PodResourceInfoMap `json:"entries,omitempty"`
}

// Checkpoint 用于存储 Pod 资源分配 checkpoint 数据。
type Checkpoint struct {
	// Data 是序列化后的 PodResourceAllocationInfo。
	Data string `json:"data"`
	// Checksum 是 Data 的校验和。
	Checksum checksum.Checksum `json:"checksum"`
}

// NewCheckpoint 根据 Pod 资源分配信息创建新的 checkpoint。
func NewCheckpoint(allocations *PodResourceCheckpointInfo) (*Checkpoint, error) {

	serializedAllocations, err := json.Marshal(allocations)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize allocations for checkpointing: %w", err)
	}

	cp := &Checkpoint{
		Data: string(serializedAllocations),
	}
	cp.Checksum = checksum.New(cp.Data)
	return cp, nil
}

// MarshalCheckpoint 把 Checkpoint 序列化为 JSON。
func (cp *Checkpoint) MarshalCheckpoint() ([]byte, error) {
	return json.Marshal(cp)
}

// UnmarshalCheckpoint 从 JSON 反序列化 checkpoint。
func (cp *Checkpoint) UnmarshalCheckpoint(blob []byte) error {
	return json.Unmarshal(blob, cp)
}

// VerifyChecksum 校验 checkpoint 中 Data 的当前校验和是否有效。
func (cp *Checkpoint) VerifyChecksum() error {
	return cp.Checksum.Verify(cp.Data)
}

// GetPodResourceCheckpointInfo 从 checkpoint 中解析 Pod 资源分配信息。
func (cp *Checkpoint) GetPodResourceCheckpointInfo() (*PodResourceCheckpointInfo, error) {
	var data PodResourceCheckpointInfo
	if err := json.Unmarshal([]byte(cp.Data), &data); err != nil {
		return nil, err
	}

	return &data, nil
}
