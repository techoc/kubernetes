//go:build !linux && !windows

/*
Copyright 2015 The Kubernetes Authors.

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
	"context"
	"fmt"

	"k8s.io/mount-utils"

	v1 "k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	internalapi "k8s.io/cri-api/pkg/apis"
	"k8s.io/kubernetes/pkg/kubelet/cadvisor"
	"k8s.io/kubernetes/pkg/kubelet/config"
	"k8s.io/kubernetes/pkg/kubelet/status"
)

// unsupportedContainerManager 是不支持容器管理的平台上的空实现。
type unsupportedContainerManager struct {
	containerManagerStub
}

var _ ContainerManager = &unsupportedContainerManager{}

// Start 在不支持的平台上直接返回错误。
func (unsupportedContainerManager) Start(_ context.Context, _ *v1.Node, _ ActivePodsFunc, _ GetNodeFunc, _ config.SourcesReady, _ status.PodStatusProvider, _ internalapi.RuntimeService, _ bool) error {
	return fmt.Errorf("Container Manager is unsupported in this build")
}

// NewContainerManager 在不支持的平台上返回空实现的容器管理器。
func NewContainerManager(_ context.Context, _ mount.Interface, _ cadvisor.Interface, _ NodeConfig, failSwapOn bool, recorder record.EventRecorder, kubeClient clientset.Interface) (ContainerManager, error) {
	return &unsupportedContainerManager{}, nil
}
