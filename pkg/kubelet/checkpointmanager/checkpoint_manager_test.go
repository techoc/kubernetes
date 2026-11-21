/*
Copyright 2017 The Kubernetes Authors.

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
	"encoding/json"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager/checksum"
	utilstore "k8s.io/kubernetes/pkg/kubelet/checkpointmanager/testing"
	v1 "k8s.io/kubernetes/pkg/kubelet/checkpointmanager/testing/example_checkpoint_formats/v1"
)

// testStore 是所有测试共享的内存存储。
var testStore *utilstore.MemStore

// FakeCheckpoint 是测试用的 checkpoint 接口，额外提供读取数据的方法。
type FakeCheckpoint interface {
	Checkpoint
	GetData() ([]*PortMapping, bool)
}

// Data 包含可存入 checkpoint 的所有类型的数据。
type Data struct {
	PortMappings []*PortMapping `json:"port_mappings,omitempty"`
	HostNetwork  bool           `json:"host_network,omitempty"`
}

// CheckpointDataV2 是 v2 格式的测试数据，新增 V2Field 以区分版本。
type CheckpointDataV2 struct {
	PortMappings []*PortMapping `json:"port_mappings,omitempty"`
	HostNetwork  bool           `json:"host_network,omitempty"`
	V2Field      string         `json:"v2field"`
}

// protocol 是端口协议类型。
type protocol string

// PortMapping 是 sandbox 的端口映射配置。
type PortMapping struct {
	// Protocol 是端口映射的协议。
	Protocol *protocol
	// ContainerPort 是容器内的端口号。
	ContainerPort *int32
	// HostPort 是宿主机上的端口号。
	HostPort *int32
	// HostIP 是暴露的宿主机 IP。
	HostIP string
}

// CheckpointData 是用于 checkpoint 测试的示例结构。
type CheckpointData struct {
	Version  string
	Name     string
	Data     *Data
	Checksum checksum.Checksum
}

// newFakeCheckpointV1 构造 v1 格式的假 checkpoint。
func newFakeCheckpointV1(name string, portMappings []*PortMapping, hostNetwork bool) FakeCheckpoint {
	return &CheckpointData{
		Version: "v1",
		Name:    name,
		Data: &Data{
			PortMappings: portMappings,
			HostNetwork:  hostNetwork,
		},
	}
}

// MarshalCheckpoint 计算并写入校验和后序列化。
func (cp *CheckpointData) MarshalCheckpoint() ([]byte, error) {
	cp.Checksum = checksum.New(*cp.Data)
	return json.Marshal(*cp)
}

// UnmarshalCheckpoint 反序列化 checkpoint。
func (cp *CheckpointData) UnmarshalCheckpoint(blob []byte) error {
	return json.Unmarshal(blob, cp)
}

// VerifyChecksum 校验数据校验和。
func (cp *CheckpointData) VerifyChecksum() error {
	return cp.Checksum.Verify(*cp.Data)
}

// GetData 返回端口映射与 hostNetwork 标志。
func (cp *CheckpointData) GetData() ([]*PortMapping, bool) {
	return cp.Data.PortMappings, cp.Data.HostNetwork
}

// checkpointDataV2 是 v2 格式的假 checkpoint 实现。
type checkpointDataV2 struct {
	Version  string
	Name     string
	Data     *CheckpointDataV2
	Checksum checksum.Checksum
}

// newFakeCheckpointV2 构造 v2 格式的假 checkpoint。
func newFakeCheckpointV2(name string, portMappings []*PortMapping, hostNetwork bool) FakeCheckpoint {
	return &checkpointDataV2{
		Version: "v2",
		Name:    name,
		Data: &CheckpointDataV2{
			PortMappings: portMappings,
			HostNetwork:  hostNetwork,
		},
	}
}

// newFakeCheckpointRemoteV1 构造定义在其他包中的 v1 checkpoint，
// 用于验证结构名与包路径参与校验和计算。
func newFakeCheckpointRemoteV1(name string, portMappings []*v1.PortMapping, hostNetwork bool) Checkpoint {
	return &v1.CheckpointData{
		Version: "v1",
		Name:    name,
		Data: &v1.Data{
			PortMappings: portMappings,
			HostNetwork:  hostNetwork,
		},
	}
}

// MarshalCheckpoint 计算并写入校验和后序列化。
func (cp *checkpointDataV2) MarshalCheckpoint() ([]byte, error) {
	cp.Checksum = checksum.New(*cp.Data)
	return json.Marshal(*cp)
}

// UnmarshalCheckpoint 反序列化 checkpoint。
func (cp *checkpointDataV2) UnmarshalCheckpoint(blob []byte) error {
	return json.Unmarshal(blob, cp)
}

// VerifyChecksum 校验数据校验和。
func (cp *checkpointDataV2) VerifyChecksum() error {
	return cp.Checksum.Verify(*cp.Data)
}

// GetData 返回端口映射与 hostNetwork 标志。
func (cp *checkpointDataV2) GetData() ([]*PortMapping, bool) {
	return cp.Data.PortMappings, cp.Data.HostNetwork
}

// newTestCheckpointManager 返回使用共享内存存储的 checkpoint 管理器。
func newTestCheckpointManager() CheckpointManager {
	return &impl{store: testStore}
}

// TestCheckpointManager 覆盖 checkpoint 的创建、读取、版本不匹配、
// 校验和损坏、列举与删除等场景。
func TestCheckpointManager(t *testing.T) {
	var err error
	testStore = utilstore.NewMemStore()
	manager := newTestCheckpointManager()
	port80 := int32(80)
	port443 := int32(443)
	proto := protocol("tcp")
	ip1234 := "1.2.3.4"

	portMappings := []*PortMapping{
		{
			&proto,
			&port80,
			&port80,
			ip1234,
		},
		{
			&proto,
			&port443,
			&port443,
			ip1234,
		},
	}
	// 构造端口映射与 v1 checkpoint，作为后续写入的数据。
	checkpoint1 := newFakeCheckpointV1("check1", portMappings, true)

	checkpoints := []struct {
		checkpointKey     string
		checkpoint        FakeCheckpoint
		expectHostNetwork bool
	}{
		{
			"key1",
			checkpoint1,
			true,
		},
		{
			"key2",
			newFakeCheckpointV1("check2", nil, false),
			false,
		},
	}

	// 逐个写入并读回 checkpoint，校验数据一致。
	for _, tc := range checkpoints {
		// 验证 CreateCheckpoint。
		err = manager.CreateCheckpoint(tc.checkpointKey, tc.checkpoint)
		assert.NoError(t, err)

		// 验证 GetCheckpoint 能读回写入的数据。
		checkpointOut := newFakeCheckpointV1("", nil, false)
		err := manager.GetCheckpoint(tc.checkpointKey, checkpointOut)
		assert.NoError(t, err)
		actualPortMappings, actualHostNetwork := checkpointOut.GetData()
		expPortMappings, expHostNetwork := tc.checkpoint.GetData()
		assert.Equal(t, expPortMappings, actualPortMappings)
		assert.Equal(t, expHostNetwork, actualHostNetwork)
	}
	// 用 V2 结构读取 V1 checkpoint 应失败：结构与写入时不同。
	checkpointV2 := newFakeCheckpointV2("", nil, false)
	err = manager.GetCheckpoint("key1", checkpointV2)
	assert.EqualError(t, err, "checkpoint is corrupted")

	// 用另一个包中定义的同名 V1 结构读取也应失败：包路径参与校验和计算。
	checkpointRemoteV1 := newFakeCheckpointRemoteV1("", nil, false)
	err = manager.GetCheckpoint("key1", checkpointRemoteV1)
	assert.EqualError(t, err, "checkpoint is corrupted")

	// 用新的 V1 结构读取同一格式的 checkpoint 应成功。
	checkpointV1 := newFakeCheckpointV1("", nil, false)
	err = manager.GetCheckpoint("key1", checkpointV1)
	assert.NoError(t, err)

	// 验证校验和被篡改时读取失败。
	checkpointOut := newFakeCheckpointV1("", nil, false)
	blob, err := checkpointOut.MarshalCheckpoint()
	assert.NoError(t, err)
	testStore.Write("key1", blob)
	err = manager.GetCheckpoint("key1", checkpoint1)
	assert.EqualError(t, err, "checkpoint is corrupted")

	// 验证 ListCheckpoints 返回全部 key。
	keys, err := manager.ListCheckpoints()
	assert.NoError(t, err)
	sort.Strings(keys)
	assert.Equal(t, []string{"key1", "key2"}, keys)

	// 验证 RemoveCheckpoint 能删除指定 key。
	err = manager.RemoveCheckpoint("key1")
	assert.NoError(t, err)
	// 删除不存在的 checkpoint 不应报错。
	err = manager.RemoveCheckpoint("key1")
	assert.NoError(t, err)

	// 删除后再次列举，只应剩下 key2。
	keys, err = manager.ListCheckpoints()
	assert.NoError(t, err)
	assert.Equal(t, []string{"key2"}, keys)

	// 读取不存在的 checkpoint 应返回错误。
	checkpointNE := newFakeCheckpointV1("NE", nil, false)
	err = manager.GetCheckpoint("key1", checkpointNE)
	assert.Error(t, err)
}
