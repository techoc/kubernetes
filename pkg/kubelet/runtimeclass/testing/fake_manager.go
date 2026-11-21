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

import (
	nodev1 "k8s.io/api/node/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/kubernetes/pkg/kubelet/runtimeclass"
)

const (
	// SandboxRuntimeClass 是预置在 fake client 中的有效 RuntimeClass。
	SandboxRuntimeClass = "sandbox"
	// SandboxRuntimeHandler 是 SandboxRuntimeClass 关联的 handler。
	SandboxRuntimeHandler = "kata-containers"

	// EmptyRuntimeClass 是预置在 fake client 中、没有 handler 的有效 RuntimeClass。
	EmptyRuntimeClass = "native"
)

// NewPopulatedClient 创建供 runtimeclass.Manager 使用的 fake client，
// 并预置若干测试 RuntimeClass 对象。
func NewPopulatedClient() clientset.Interface {
	return fake.NewSimpleClientset(
		NewRuntimeClass(EmptyRuntimeClass, ""),
		NewRuntimeClass(SandboxRuntimeClass, SandboxRuntimeHandler),
	)
}

// StartManagerSync 启动 manager 并等待 informer 缓存同步完成。
// 返回停止 manager 的函数，应通过 defer 调用：
//
//	defer StartManagerSync(t, m)()
func StartManagerSync(m *runtimeclass.Manager) func() {
	stopCh := make(chan struct{})
	m.Start(stopCh)
	m.WaitForCacheSync(stopCh)
	return func() {
		close(stopCh)
	}
}

// NewRuntimeClass 生成指定名称与 handler 的 RuntimeClass 资源，便于测试使用。
func NewRuntimeClass(name, handler string) *nodev1.RuntimeClass {
	return &nodev1.RuntimeClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Handler: handler,
	}
}
