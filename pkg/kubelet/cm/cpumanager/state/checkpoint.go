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

package state

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"strings"

	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager/checksum"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager/errors"
	"k8s.io/utils/dump"
)

// CPUManagerCheckpointV4 将 checkpoint 数据序列化为字符串，采用与 allocation manager
// 相同的方案（见 pkg/kubelet/allocation/state/checkpoint.go）。
// 内嵌的 V2 版本用于保持向前兼容。
// 编译期断言：各版本的 checkpoint 都实现 checkpointmanager.Checkpoint 接口。
var _ checkpointmanager.Checkpoint = &CPUManagerCheckpointV2{}
var _ checkpointmanager.Checkpoint = &CPUManagerCheckpointV3{}
var _ checkpointmanager.Checkpoint = &CPUManagerCheckpointV4{}
var _ checkpointmanager.Checkpoint = &CPUManagerCheckpoint{}

// ContainerCPUs 是 v4 格式 checkpoint 中的结构，
// 用于在静态 CPU Manager 策略下支持 Pod 资源的原地更新。
type ContainerCPUs struct {
	Baseline string `json:"baseline"`
}

// CPUManagerCheckpointData 保存 CPU/Pod 分配信息，是 v4 格式 checkpoint 的一部分。
// 准入时记录的 CPU 分配用于原地纵向扩缩容，保存在 Baselines 中。
type CPUManagerCheckpointData struct {
	PolicyName    string                              `json:"policyName"`
	DefaultCPUSet string                              `json:"defaultCpuSet"`
	Entries       map[string]map[string]string        `json:"entries,omitempty"`
	PodEntries    PodCPUAssignments                   `json:"podEntries,omitempty"`
	Baselines     map[string]map[string]ContainerCPUs `json:"baselines,omitempty"`
}

// CPUManagerCheckpoint 是保存 CPU manager checkpoint 数据的结构。
// 采用双校验和格式以保持向后兼容：Data 字符串与 DataChecksum 是权威的 V4 载荷。
type CPUManagerCheckpoint struct {
	// 向后兼容：内嵌的 V2 checkpoint 不得再增强；当支持的最旧版本也能理解 V4 后，
	// 应尽快移除该内嵌字段。
	CPUManagerCheckpointV2 `json:",inline"`

	// Data 是序列化后的 CPUManagerCheckpointData。
	Data string `json:"data"`
	// DataChecksum 是 Data 字符串的校验和。
	DataChecksum checksum.Checksum `json:"dataChecksum"`

	// CheckpointData 保存实际数据，不会被直接序列化。
	CheckpointData CPUManagerCheckpointData `json:"-"`
}

// CPUManagerCheckpointV4 是 v4 格式 checkpoint 的类型别名，用于保存 CPU/Pod 分配。
type CPUManagerCheckpointV4 = CPUManagerCheckpoint

// CPUManagerCheckpointV3 是 v3 格式 checkpoint，用于保存 CPU/Pod 分配。
type CPUManagerCheckpointV3 struct {
	PolicyName    string                       `json:"policyName"`
	DefaultCPUSet string                       `json:"defaultCpuSet"`
	Entries       map[string]map[string]string `json:"entries,omitempty"`
	PodEntries    PodCPUAssignments            `json:"podEntries,omitempty"`
	Checksum      checksum.Checksum            `json:"checksum"`
}

// CPUManagerCheckpointV2 是 v2 格式 checkpoint，用于保存 CPU/Pod 分配。
type CPUManagerCheckpointV2 struct {
	PolicyName    string                       `json:"policyName"`
	DefaultCPUSet string                       `json:"defaultCpuSet"`
	Entries       map[string]map[string]string `json:"entries,omitempty"`
	Checksum      checksum.Checksum            `json:"checksum"`
}

// newCPUManagerCheckpoint 返回 Checkpoint 实例（当前为 v4 格式）。
func newCPUManagerCheckpoint() *CPUManagerCheckpoint {
	//nolint:staticcheck // unexported-type-in-api user-facing error message
	return newCPUManagerCheckpointV4()
}

// newCPUManagerCheckpointV2 创建空的 v2 格式 checkpoint。
func newCPUManagerCheckpointV2() *CPUManagerCheckpointV2 {
	return &CPUManagerCheckpointV2{
		Entries: make(map[string]map[string]string),
	}
}

// newCPUManagerCheckpointV3 创建空的 v3 格式 checkpoint。
func newCPUManagerCheckpointV3() *CPUManagerCheckpointV3 {
	return &CPUManagerCheckpointV3{
		Entries:    make(map[string]map[string]string),
		PodEntries: make(PodCPUAssignments),
	}
}

// newCPUManagerCheckpointV4 创建空的 v4 格式 checkpoint。
func newCPUManagerCheckpointV4() *CPUManagerCheckpointV4 {
	return &CPUManagerCheckpoint{
		CPUManagerCheckpointV2: CPUManagerCheckpointV2{
			Entries: make(map[string]map[string]string),
		},
		CheckpointData: CPUManagerCheckpointData{
			Entries:    make(map[string]map[string]string),
			PodEntries: make(PodCPUAssignments),
			Baselines:  make(map[string]map[string]ContainerCPUs),
		},
	}
}

// MarshalCheckpoint 以 v2 格式序列化 checkpoint。
func (cp *CPUManagerCheckpointV2) MarshalCheckpoint() ([]byte, error) {
	// 先清零校验和，避免旧值影响输出校验和的计算。
	cp.Checksum = 0

	// 为了在特性门控关闭时保持回滚兼容，必须使用旧结构名 "CPUManagerCheckpoint"
	// （而不是 "CPUManagerCheckpointV2"）计算校验和。旧版本 kubelet 没有字符串替换逻辑，
	// 只认原始结构名。
	object := dump.ForHash(cp)
	object = strings.Replace(object, "CPUManagerCheckpointV2", "CPUManagerCheckpoint", 1)
	hash := fnv.New32a()
	_, _ = fmt.Fprintf(hash, "%v", object)
	cp.Checksum = checksum.Checksum(hash.Sum32())

	return json.Marshal(*cp)
}

// MarshalCheckpoint 以 v3 格式序列化 checkpoint。
func (cp *CPUManagerCheckpointV3) MarshalCheckpoint() ([]byte, error) {
	// make sure checksum wasn't set before so it doesn't affect output checksum
	cp.Checksum = 0

	// 为了保持回滚兼容，必须使用旧结构名 "CPUManagerCheckpoint"（而不是
	// "CPUManagerCheckpointV3"）计算校验和。旧版本 kubelet 没有字符串替换逻辑，
	// 只认原始结构名。
	object := dump.ForHash(cp)
	object = strings.Replace(object, "CPUManagerCheckpointV3", "CPUManagerCheckpoint", 1)
	hash := fnv.New32a()
	_, _ = fmt.Fprintf(hash, "%v", object)
	cp.Checksum = checksum.Checksum(hash.Sum32())

	return json.Marshal(*cp)
}

// MarshalCheckpoint 以 v4 格式序列化 checkpoint，并写入双校验和以保持向后兼容。
func (cp *CPUManagerCheckpoint) MarshalCheckpoint() ([]byte, error) {
	// 先把实际数据序列化到 Data，并计算其校验和（权威载荷）。
	data, err := json.Marshal(cp.CheckpointData)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize cpu manager checkpoint data: %w", err)
	}
	cp.Data = string(data)
	cp.DataChecksum = checksum.New(cp.Data)

	// 同时回填 V2 内嵌字段，保证旧版本 kubelet 仍能读取该 checkpoint。
	cp.PolicyName = cp.CheckpointData.PolicyName
	cp.DefaultCPUSet = cp.CheckpointData.DefaultCPUSet
	cp.Entries = cp.CheckpointData.Entries

	// V2 校验和按旧结构名计算，以兼容不支持字符串替换逻辑的旧 kubelet。
	cp.Checksum = 0
	object := dump.ForHash(&cp.CPUManagerCheckpointV2)
	object = strings.Replace(object, "CPUManagerCheckpointV2", "CPUManagerCheckpoint", 1)
	hash := fnv.New32a()
	_, _ = fmt.Fprintf(hash, "%v", object)
	cp.Checksum = checksum.Checksum(hash.Sum32())

	return json.Marshal(*cp)
}

// UnmarshalCheckpoint 将给定字节反序列化为 v2 格式 checkpoint。
func (cp *CPUManagerCheckpointV2) UnmarshalCheckpoint(blob []byte) error {
	return json.Unmarshal(blob, cp)
}

// UnmarshalCheckpoint 将给定字节反序列化为 v3 格式 checkpoint。
func (cp *CPUManagerCheckpointV3) UnmarshalCheckpoint(blob []byte) error {
	return json.Unmarshal(blob, cp)
}

// UnmarshalCheckpoint 将给定字节反序列化为 v4 格式 checkpoint。
func (cp *CPUManagerCheckpoint) UnmarshalCheckpoint(blob []byte) error {
	if err := json.Unmarshal(blob, cp); err != nil {
		return fmt.Errorf("failed to deserialize cpu manager checkpoint: %w", err)
	}
	// 先反序列化外层结构，再解析 Data 中的实际数据。
	if err := json.Unmarshal([]byte(cp.Data), &cp.CheckpointData); err != nil {
		return fmt.Errorf("failed to deserialize cpu manager checkpoint data: %w", err)
	}
	return nil
}

// VerifyChecksum 校验 v2 格式 checkpoint 的校验和。
func (cp *CPUManagerCheckpointV2) VerifyChecksum() error {
	if cp.Checksum == 0 {
		// 兼容旧文件后端：校验和为零时视为通过。
		return nil
	}

	ck := cp.Checksum
	cp.Checksum = 0

	if !utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResourceManagers) {
		err := ck.Verify(cp)
		if err == nil {
			cp.Checksum = ck
			return nil
		}
		// 标准校验失败时，可能来自旧版 checkpoint，回退到旧版校验方式
		// （V3 是当时的最新版本）。
	}

	object := dump.ForHash(cp)
	object = strings.Replace(object, "CPUManagerCheckpointV2", "CPUManagerCheckpoint", 1)
	cp.Checksum = ck

	hash := fnv.New32a()
	_, _ = fmt.Fprintf(hash, "%v", object)
	actualCS := checksum.Checksum(hash.Sum32())
	if cp.Checksum != actualCS {
		return &errors.CorruptCheckpointError{
			ActualCS:   uint64(actualCS),
			ExpectedCS: uint64(cp.Checksum),
		}
	}
	return nil
}

// VerifyChecksum 校验 v3 格式 checkpoint 的校验和。
func (cp *CPUManagerCheckpointV3) VerifyChecksum() error {
	ck := cp.Checksum
	cp.Checksum = 0
	// 先按当前结构名做标准校验；失败再回退到旧结构名校验。
	err := ck.Verify(cp)
	if err == nil {
		cp.Checksum = ck
		return nil
	}

	object := dump.ForHash(cp)
	object = strings.Replace(object, "CPUManagerCheckpointV3", "CPUManagerCheckpoint", 1)
	cp.Checksum = ck

	hash := fnv.New32a()
	_, _ = fmt.Fprintf(hash, "%v", object)
	actualCS := checksum.Checksum(hash.Sum32())
	if cp.Checksum != actualCS {
		return &errors.CorruptCheckpointError{
			ActualCS:   uint64(actualCS),
			ExpectedCS: uint64(cp.Checksum),
		}
	}
	return nil
}

// VerifyChecksum 校验 v4 格式 checkpoint；DataChecksum 是 Data 的权威校验和。
func (cp *CPUManagerCheckpoint) VerifyChecksum() error {
	return cp.DataChecksum.Verify(cp.Data)
}
