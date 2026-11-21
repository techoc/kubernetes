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

package cm

import (
	"k8s.io/api/core/v1"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
)

// NewFakeInternalContainerLifecycle 返回测试用的容器生命周期假实现。
func NewFakeInternalContainerLifecycle() *fakeInternalContainerLifecycle {
	return &fakeInternalContainerLifecycle{}
}

// fakeInternalContainerLifecycle 是所有生命周期钩子均为空实现的假对象。
type fakeInternalContainerLifecycle struct{}

// PreCreateContainer 为空实现，不修改容器配置。
func (f *fakeInternalContainerLifecycle) PreCreateContainer(logger klog.Logger, pod *v1.Pod, container *v1.Container, containerConfig *runtimeapi.ContainerConfig) error {
	return nil
}

// PreStartContainer 为空实现，不执行启动前操作。
func (f *fakeInternalContainerLifecycle) PreStartContainer(logger klog.Logger, pod *v1.Pod, container *v1.Container, containerID string) error {
	return nil
}

// PostStopContainer 为空实现，不执行停止后清理。
func (f *fakeInternalContainerLifecycle) PostStopContainer(logger klog.Logger, containerID string) error {
	return nil
}
