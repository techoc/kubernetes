/*
Copyright 2020 The Kubernetes Authors.

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
	"hash/fnv"
	"strings"

	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager/checksum"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager/errors"
	"k8s.io/utils/dump"
)

// MemoryManagerCheckpointV3 将 checkpoint 数据序列化为字符串，采用与 allocation manager
// 相同的方案（见 pkg/kubelet/allocation/state/checkpoint.go）。
// 内嵌的 V1 版本用于保持向前兼容。
// 编译期断言：各版本的 checkpoint 都实现 checkpointmanager.Checkpoint 接口。
var _ checkpointmanager.Checkpoint = &MemoryManagerCheckpointV1{}
var _ checkpointmanager.Checkpoint = &MemoryManagerCheckpointV2{}
var _ checkpointmanager.Checkpoint = &MemoryManagerCheckpointV3{}
var _ checkpointmanager.Checkpoint = &MemoryManagerCheckpoint{}

// MemoryManagerCheckpointData 保存内存/Pod 分配信息，是 v3 格式 checkpoint 的一部分。
type MemoryManagerCheckpointData struct {
	PolicyName   string                     `json:"policyName"`
	MachineState NUMANodeMap                `json:"machineState"`
	Entries      ContainerMemoryAssignments `json:"entries,omitempty"`
	PodEntries   PodMemoryAssignments       `json:"podEntries,omitempty"`
}

// MemoryManagerCheckpoint 是保存内存/Pod 分配 checkpoint 数据的结构。
// 采用双校验和格式以保持向后兼容：Data 字符串与 DataChecksum 是权威的 V3 载荷。
type MemoryManagerCheckpoint struct {
	// 向后兼容：内嵌的 V1 checkpoint 不得再增强；当支持的最旧版本也能理解 V3 后，
	// 应尽快移除该内嵌字段。
	MemoryManagerCheckpointV1 `json:",inline"`

	// Data 是序列化后的 MemoryManagerCheckpointData。
	Data string `json:"data"`
	// DataChecksum 是 Data 字符串的校验和。
	DataChecksum checksum.Checksum `json:"dataChecksum"`

	// CheckpointData 保存实际数据，不会被直接序列化。
	CheckpointData MemoryManagerCheckpointData `json:"-"`
}

// MemoryManagerCheckpointV3 是 v3 格式 checkpoint 的类型别名，用于保存内存/Pod 分配。
type MemoryManagerCheckpointV3 = MemoryManagerCheckpoint

// MemoryManagerCheckpointV2 是 v2 格式 checkpoint，用于保存内存/Pod 分配。
type MemoryManagerCheckpointV2 struct {
	PolicyName   string                     `json:"policyName"`
	MachineState NUMANodeMap                `json:"machineState"`
	Entries      ContainerMemoryAssignments `json:"entries,omitempty"`
	PodEntries   PodMemoryAssignments       `json:"podEntries,omitempty"`
	Checksum     checksum.Checksum          `json:"checksum"`
}

// MemoryManagerCheckpointV1 是 v1 格式 checkpoint，用于保存内存/Pod 分配。
type MemoryManagerCheckpointV1 struct {
	PolicyName   string                     `json:"policyName"`
	MachineState NUMANodeMap                `json:"machineState"`
	Entries      ContainerMemoryAssignments `json:"entries,omitempty"`
	Checksum     checksum.Checksum          `json:"checksum"`
}

// newMemoryManagerCheckpoint 返回 Checkpoint 实例（当前为 v3 格式）。
func newMemoryManagerCheckpoint() *MemoryManagerCheckpoint {
	//nolint:staticcheck // unexported-type-in-api user-facing error message
	return newMemoryManagerCheckpointV3()
}

// newMemoryManagerCheckpointV1 创建空的 v1 格式 checkpoint。
func newMemoryManagerCheckpointV1() *MemoryManagerCheckpointV1 {
	return &MemoryManagerCheckpointV1{
		Entries:      ContainerMemoryAssignments{},
		MachineState: NUMANodeMap{},
	}
}

// newMemoryManagerCheckpointV2 创建空的 v2 格式 checkpoint。
func newMemoryManagerCheckpointV2() *MemoryManagerCheckpointV2 {
	return &MemoryManagerCheckpointV2{
		Entries:      ContainerMemoryAssignments{},
		PodEntries:   PodMemoryAssignments{},
		MachineState: NUMANodeMap{},
	}
}

// newMemoryManagerCheckpointV3 创建空的 v3 格式 checkpoint。
func newMemoryManagerCheckpointV3() *MemoryManagerCheckpointV3 {
	return &MemoryManagerCheckpoint{
		MemoryManagerCheckpointV1: MemoryManagerCheckpointV1{
			Entries:      ContainerMemoryAssignments{},
			MachineState: NUMANodeMap{},
		},
		CheckpointData: MemoryManagerCheckpointData{
			Entries:      ContainerMemoryAssignments{},
			PodEntries:   PodMemoryAssignments{},
			MachineState: NUMANodeMap{},
		},
	}
}

// MarshalCheckpoint 以 v1 格式序列化 checkpoint。
func (mp *MemoryManagerCheckpointV1) MarshalCheckpoint() ([]byte, error) {
	// 先清零校验和，避免旧值影响输出校验和的计算。
	mp.Checksum = 0

	// 为了保持回滚兼容，必须使用旧结构名 "MemoryManagerCheckpoint"（而不是
	// "MemoryManagerCheckpointV1"）计算校验和。旧版本 kubelet 没有字符串替换逻辑，
	// 只认原始结构名。
	object := dump.ForHash(mp)
	object = strings.Replace(object, "MemoryManagerCheckpointV1", "MemoryManagerCheckpoint", 1)
	hash := fnv.New32a()
	_, _ = fmt.Fprintf(hash, "%v", object)
	mp.Checksum = checksum.Checksum(hash.Sum32())

	return json.Marshal(*mp)
}

// MarshalCheckpoint 以 v2 格式序列化 checkpoint。
func (mp *MemoryManagerCheckpointV2) MarshalCheckpoint() ([]byte, error) {
	// 确保此前未设置校验和，以免影响输出校验和。
	mp.Checksum = 0

	// 为了保持回滚兼容，必须使用旧结构名 "MemoryManagerCheckpoint"（而不是
	// "MemoryManagerCheckpointV2"）计算校验和。旧版本 kubelet 没有字符串替换逻辑，
	// 只认原始结构名。
	object := dump.ForHash(mp)
	object = strings.Replace(object, "MemoryManagerCheckpointV2", "MemoryManagerCheckpoint", 1)
	hash := fnv.New32a()
	_, _ = fmt.Fprintf(hash, "%v", object)
	mp.Checksum = checksum.Checksum(hash.Sum32())

	return json.Marshal(*mp)
}

// MarshalCheckpoint 以 v3 格式序列化 checkpoint，并写入双校验和以保持向后兼容。
func (mp *MemoryManagerCheckpointV3) MarshalCheckpoint() ([]byte, error) {
	// 先把实际数据序列化到 Data，并计算其校验和（权威载荷）。
	data, err := json.Marshal(mp.CheckpointData)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize memory manager checkpoint data: %w", err)
	}
	mp.Data = string(data)
	mp.DataChecksum = checksum.New(mp.Data)

	// 同时回填 V1 内嵌字段，保证旧版本 kubelet 仍能读取该 checkpoint。
	mp.PolicyName = mp.CheckpointData.PolicyName
	mp.MachineState = mp.CheckpointData.MachineState
	mp.Entries = mp.CheckpointData.Entries

	// V1 校验和按旧结构名计算，以兼容不支持字符串替换逻辑的旧 kubelet。
	mp.Checksum = 0
	object := dump.ForHash(&mp.MemoryManagerCheckpointV1)
	object = strings.Replace(object, "MemoryManagerCheckpointV1", "MemoryManagerCheckpoint", 1)
	hash := fnv.New32a()
	_, _ = fmt.Fprintf(hash, "%v", object)
	mp.Checksum = checksum.Checksum(hash.Sum32())

	return json.Marshal(*mp)
}

// UnmarshalCheckpoint 将给定字节反序列化为 v1 格式 checkpoint。
func (mp *MemoryManagerCheckpointV1) UnmarshalCheckpoint(blob []byte) error {
	return json.Unmarshal(blob, mp)
}

// UnmarshalCheckpoint 将给定字节反序列化为 v2 格式 checkpoint。
func (mp *MemoryManagerCheckpointV2) UnmarshalCheckpoint(blob []byte) error {
	return json.Unmarshal(blob, mp)
}

// UnmarshalCheckpoint 将给定字节反序列化为 v3 格式 checkpoint。
func (mp *MemoryManagerCheckpointV3) UnmarshalCheckpoint(blob []byte) error {
	if err := json.Unmarshal(blob, mp); err != nil {
		return fmt.Errorf("failed to deserialize memory manager checkpoint: %w", err)
	}
	// 先反序列化外层结构，再解析 Data 中的实际数据。
	if err := json.Unmarshal([]byte(mp.Data), &mp.CheckpointData); err != nil {
		return fmt.Errorf("failed to deserialize memory manager checkpoint data: %w", err)
	}
	return nil
}

// VerifyChecksum 校验 v1 格式 checkpoint 的校验和。
func (mp *MemoryManagerCheckpointV1) VerifyChecksum() error {
	if mp.Checksum == 0 {
		// 兼容旧文件后端：校验和为零时视为通过。
		return nil
	}

	ck := mp.Checksum
	mp.Checksum = 0
	object := dump.ForHash(mp)
	object = strings.Replace(object, "MemoryManagerCheckpointV1", "MemoryManagerCheckpoint", 1)
	mp.Checksum = ck

	hash := fnv.New32a()
	_, _ = fmt.Fprintf(hash, "%v", object)
	actualCS := checksum.Checksum(hash.Sum32())
	if mp.Checksum != actualCS {
		return &errors.CorruptCheckpointError{
			ActualCS:   uint64(actualCS),
			ExpectedCS: uint64(mp.Checksum),
		}
	}

	return nil
}

// VerifyChecksum 校验 v2 格式 checkpoint 的校验和。
func (mp *MemoryManagerCheckpointV2) VerifyChecksum() error {
	ck := mp.Checksum
	mp.Checksum = 0
	object := dump.ForHash(mp)
	object = strings.Replace(object, "MemoryManagerCheckpointV2", "MemoryManagerCheckpoint", 1)
	mp.Checksum = ck

	hash := fnv.New32a()
	_, _ = fmt.Fprintf(hash, "%v", object)
	actualCS := checksum.Checksum(hash.Sum32())
	if mp.Checksum != actualCS {
		return &errors.CorruptCheckpointError{
			ActualCS:   uint64(actualCS),
			ExpectedCS: uint64(mp.Checksum),
		}
	}

	return nil
}

// VerifyChecksum 校验 v3 格式 checkpoint；DataChecksum 是 Data 的权威校验和。
func (mp *MemoryManagerCheckpoint) VerifyChecksum() error {
	return mp.DataChecksum.Verify(mp.Data)
}
