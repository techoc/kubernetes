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

package v1

import (
	"encoding/json"

	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager/checksum"
)

// protocol 是端口映射的协议类型。
type protocol string

// PortMapping 描述 sandbox 的端口映射配置。
type PortMapping struct {
	// Protocol 是端口映射协议。
	Protocol *protocol
	// ContainerPort 是容器内的端口号。
	ContainerPort *int32
	// HostPort 是宿主机上的端口号。
	HostPort *int32
}

// Data 包含可存入 checkpoint 的所有数据类型。
type Data struct {
	PortMappings []*PortMapping `json:"port_mappings,omitempty"`
	HostNetwork  bool           `json:"host_network,omitempty"`
}

// CheckpointData 是 checkpoint 测试用例使用的示例结构。
type CheckpointData struct {
	Version  string
	Name     string
	Data     *Data
	Checksum checksum.Checksum
}

// MarshalCheckpoint 重新计算校验和并将 CheckpointData 序列化为 JSON。
func (cp *CheckpointData) MarshalCheckpoint() ([]byte, error) {
	cp.Checksum = checksum.New(*cp.Data)
	return json.Marshal(*cp)
}

// UnmarshalCheckpoint 把传入的 JSON 数据反序列化为 CheckpointData。
func (cp *CheckpointData) UnmarshalCheckpoint(blob []byte) error {
	return json.Unmarshal(blob, cp)
}

// VerifyChecksum 校验 CheckpointData 的校验和与根据其数据计算出的校验和一致。
func (cp *CheckpointData) VerifyChecksum() error {
	return cp.Checksum.Verify(*cp.Data)
}
