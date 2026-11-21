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

package testing

import (
	"context"

	cadvisorapi "github.com/google/cadvisor/lib/model"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/cadvisor"
)

// Fake 是 cadvisor.Interface 的测试实现。
type Fake struct {
	// NodeName 是模拟节点名称。
	NodeName string
}

const (
	// FakeKernelVersion 是测试用的内核版本。
	FakeKernelVersion = "3.16.0-0.bpo.4-amd64"
	// FakeContainerOSVersion 是测试用的操作系统版本。
	FakeContainerOSVersion = "Debian GNU/Linux 7 (wheezy)"

	// 以下为测试用的固定机器参数。
	fakeNumCores       = 1
	fakeMemoryCapacity = 4026531840
	fakeDockerVersion  = "1.13.1"
)

// 编译期断言：Fake 实现了 cadvisor.Interface 接口。
var _ cadvisor.Interface = new(Fake)

// Start 是 Interface.Start 的伪实现。
func (c *Fake) Start() error {
	return nil
}

// ContainerInfoV2 是 Interface.ContainerInfoV2 的伪实现，返回空映射。
func (c *Fake) ContainerInfoV2(name string, options cadvisorapi.RequestOptions) (map[string]cadvisorapi.ContainerInfo, error) {
	return map[string]cadvisorapi.ContainerInfo{}, nil
}

// GetRequestedContainersInfo 是 Interface.GetRequestedContainersInfo 的伪实现。
func (c *Fake) GetRequestedContainersInfo(containerName string, options cadvisorapi.RequestOptions) (map[string]*cadvisorapi.ContainerInfo, error) {
	return map[string]*cadvisorapi.ContainerInfo{}, nil
}

// MachineInfo 是 Interface.MachineInfo 的伪实现。
func (c *Fake) MachineInfo(logger klog.Logger) (*cadvisorapi.MachineInfo, error) {
	// 模拟 1 核、3.75GB 内存的机器。
	// 使用非零值是为了让 Kubemark 中的机器容量也不为零。
	return &cadvisorapi.MachineInfo{
		NumCores:       fakeNumCores,
		InstanceID:     cadvisorapi.InstanceID(c.NodeName),
		MemoryCapacity: fakeMemoryCapacity,
	}, nil
}

// VersionInfo 是 Interface.VersionInfo 的伪实现。
func (c *Fake) VersionInfo() (*cadvisorapi.VersionInfo, error) {
	return &cadvisorapi.VersionInfo{
		KernelVersion:      FakeKernelVersion,
		ContainerOsVersion: FakeContainerOSVersion,
		DockerVersion:      fakeDockerVersion,
	}, nil
}

// ImagesFsInfo 是 Interface.ImagesFsInfo 的伪实现。
func (c *Fake) ImagesFsInfo(context.Context) (cadvisorapi.FsInfo, error) {
	return cadvisorapi.FsInfo{}, nil
}

// RootFsInfo 是 Interface.RootFsInfo 的伪实现。
func (c *Fake) RootFsInfo() (cadvisorapi.FsInfo, error) {
	return cadvisorapi.FsInfo{}, nil
}

// ContainerFsInfo 是 Interface.ContainerFsInfo 的伪实现。
func (c *Fake) ContainerFsInfo(context.Context) (cadvisorapi.FsInfo, error) {
	return cadvisorapi.FsInfo{}, nil
}

// GetDirFsInfo 是 Interface.GetDirFsInfo 的伪实现。
func (c *Fake) GetDirFsInfo(path string) (cadvisorapi.FsInfo, error) {
	return cadvisorapi.FsInfo{}, nil
}
