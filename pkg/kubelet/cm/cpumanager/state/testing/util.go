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

package testing

import "k8s.io/kubernetes/pkg/kubelet/checkpointmanager"

var _ checkpointmanager.Checkpoint = &MockCheckpoint{}

// MockCheckpoint 用于在测试中模拟 checkpoint 内容。
type MockCheckpoint struct {
	Content string
}

// MarshalCheckpoint 返回预设的假内容。
func (mc *MockCheckpoint) MarshalCheckpoint() ([]byte, error) {
	return []byte(mc.Content), nil
}

// UnmarshalCheckpoint 为空实现，模拟反序列化成功。
func (mc *MockCheckpoint) UnmarshalCheckpoint(blob []byte) error {
	return nil
}

// VerifyChecksum 为空实现，模拟校验和验证通过。
func (mc *MockCheckpoint) VerifyChecksum() error {
	return nil
}
