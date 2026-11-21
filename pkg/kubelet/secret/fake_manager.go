/*
Copyright 2016 The Kubernetes Authors.

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

package secret

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
)

// fakeManager 是测试用的 Manager 假实现，只返回初始化时给定的 Secret。
type fakeManager struct {
	secrets []*v1.Secret
}

// NewFakeManager 创建一个空的 Secret manager 假实现。
func NewFakeManager() Manager {
	return &fakeManager{}
}

// NewFakeManagerWithSecrets 创建包含指定 Secret 的假 manager。
func NewFakeManagerWithSecrets(secrets []*v1.Secret) Manager {
	return &fakeManager{
		secrets: secrets,
	}
}

// GetSecret 返回初始化时提供的同名 Secret；找不到时返回错误。
// 若初始化时未提供任何 Secret，则返回 nil。
func (s *fakeManager) GetSecret(namespace, name string) (*v1.Secret, error) {
	if s.secrets == nil {
		return nil, nil
	}

	for _, secret := range s.secrets {
		if secret.Name == name {
			return secret, nil
		}
	}

	return nil, fmt.Errorf("secret %s not found", name)
}

// RegisterPod 为空实现，假 manager 不跟踪 Pod。
func (s *fakeManager) RegisterPod(pod *v1.Pod) {
}

// UnregisterPod 为空实现，假 manager 不跟踪 Pod。
func (s *fakeManager) UnregisterPod(pod *v1.Pod) {
}
