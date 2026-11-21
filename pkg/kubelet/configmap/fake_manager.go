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

package configmap

import v1 "k8s.io/api/core/v1"

// fakeManager 是测试用的 Manager 接口假实现，不访问 apiserver。
type fakeManager struct {
}

// NewFakeManager 创建一个空的 ConfigMap manager 假实现。
func NewFakeManager() Manager {
	return &fakeManager{}
}

// GetConfigMap 返回 nil，假实现不提供真实 ConfigMap 数据。
func (s *fakeManager) GetConfigMap(namespace, name string) (*v1.ConfigMap, error) {
	return nil, nil
}

// RegisterPod 为空实现，假实现不跟踪 Pod。
func (s *fakeManager) RegisterPod(pod *v1.Pod) {
}

// UnregisterPod 为空实现，假实现不跟踪 Pod。
func (s *fakeManager) UnregisterPod(pod *v1.Pod) {
}
