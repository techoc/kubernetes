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
	"encoding/json"
	"hash/crc32"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager/errors"
)

const (
	// DRA checkpoint 的 API 组、类型与版本。
	CheckpointAPIGroup   = "checkpoint.dra.kubelet.k8s.io"
	CheckpointKind       = "DRACheckpoint"
	CheckpointAPIVersion = CheckpointAPIGroup + "/v1"
)

// Checkpoint 保存 DRA checkpoint 数据。
type Checkpoint struct {
	// Data 是 JSON 序列化的 checkpoint 数据。
	Data string
	// Checksum 是 Data 的校验和。
	Checksum uint32
}

// CheckpointData 是写入 JSON 的 DRA checkpoint 结构。
type CheckpointData struct {
	// TypeMeta 包含类型与版本信息。
	metav1.TypeMeta
	// ClaimInfoStateList 是 Claim 状态列表。
	ClaimInfoStateList ClaimInfoStateList
}

// NewCheckpoint 根据 Claim 状态列表创建 checkpoint。
func NewCheckpoint(data ClaimInfoStateList) (*Checkpoint, error) {
	cpData := &CheckpointData{
		TypeMeta: metav1.TypeMeta{
			Kind:       CheckpointKind,
			APIVersion: CheckpointAPIVersion,
		},
		ClaimInfoStateList: data,
	}

	cpDataBytes, err := json.Marshal(cpData)
	if err != nil {
		return nil, err
	}

	// 同时记录序列化数据与 CRC32 校验和。
	cp := &Checkpoint{
		Data:     string(cpDataBytes),
		Checksum: crc32.ChecksumIEEE(cpDataBytes),
	}

	return cp, nil
}

// MarshalCheckpoint 把 checkpoint 序列化为 JSON。
func (cp *Checkpoint) MarshalCheckpoint() ([]byte, error) {
	return json.Marshal(cp)
}

// UnmarshalCheckpoint 从 JSON 反序列化 checkpoint 并校验 checksum。
func (cp *Checkpoint) UnmarshalCheckpoint(blob []byte) error {
	if err := json.Unmarshal(blob, cp); err != nil {
		return err
	}

	// 校验 checksum。
	// 反序列化后立即校验，防止加载损坏数据。
	if err := cp.VerifyChecksum(); err != nil {
		return err
	}

	return nil
}

// VerifyChecksum 校验 checkpoint 中 Data 的 checksum 是否有效。
func (cp *Checkpoint) VerifyChecksum() error {
	// 重新计算 Data 的 CRC32 并与存储值比较。
	expectedCS := crc32.ChecksumIEEE([]byte(cp.Data))
	if expectedCS != cp.Checksum {
		return &errors.CorruptCheckpointError{ActualCS: uint64(cp.Checksum), ExpectedCS: uint64(expectedCS)}
	}
	return nil
}

// GetClaimInfoStateList 返回 checkpoint 中的 Claim 状态列表。
func (cp *Checkpoint) GetClaimInfoStateList() (ClaimInfoStateList, error) {
	var data CheckpointData
	if err := json.Unmarshal([]byte(cp.Data), &data); err != nil {
		return nil, err
	}

	return data.ClaimInfoStateList, nil
}
