//go:build windows

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

package cadvisor

import (
	"context"

	cadvisorapi "github.com/google/cadvisor/lib/model"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/winstats"
)

// cadvisorClient 是 Windows 上的 cAdvisor 接口实现。
type cadvisorClient struct {
	// rootPath 是根文件系统路径。
	rootPath string
	// winStatsClient 提供 Windows 性能统计。
	winStatsClient winstats.Client
}

// 编译期断言：cadvisorClient 实现了 Interface 接口。
var _ Interface = new(cadvisorClient)

// New 创建 Windows 上的 cAdvisor 实现。
func New(logger klog.Logger, imageFsInfoProvider ImageFsInfoProvider, rootPath string, cgroupRoots []string, usingLegacyStats, localStorageCapacityIsolation, disableContainerDiscovery bool) (Interface, error) {
	// 使用 Windows 性能计数器客户端采集统计。
	client, err := winstats.NewPerfCounterClient(logger)
	return &cadvisorClient{
		rootPath:       rootPath,
		winStatsClient: client,
	}, err
}

// Start 是空实现。
func (cu *cadvisorClient) Start() error {
	return nil
}

// ContainerInfoV2 预期只用于根容器，返回节点上所有容器的信息。
func (cu *cadvisorClient) ContainerInfoV2(name string, options cadvisorapi.RequestOptions) (map[string]cadvisorapi.ContainerInfo, error) {
	return cu.winStatsClient.WinContainerInfos()
}

// GetRequestedContainersInfo 未实现，返回 nil。
func (cu *cadvisorClient) GetRequestedContainersInfo(containerName string, options cadvisorapi.RequestOptions) (map[string]*cadvisorapi.ContainerInfo, error) {
	return nil, nil
}

// MachineInfo 返回 Windows 机器信息。
func (cu *cadvisorClient) MachineInfo(logger klog.Logger) (*cadvisorapi.MachineInfo, error) {
	return cu.winStatsClient.WinMachineInfo(logger)
}

// VersionInfo 返回 cAdvisor 版本信息。
func (cu *cadvisorClient) VersionInfo() (*cadvisorapi.VersionInfo, error) {
	return cu.winStatsClient.WinVersionInfo()
}

// ImagesFsInfo 在 Windows 上返回空统计。
func (cu *cadvisorClient) ImagesFsInfo(context.Context) (cadvisorapi.FsInfo, error) {
	return cadvisorapi.FsInfo{}, nil
}

// ContainerFsInfo 在 Windows 上返回空统计。
func (cu *cadvisorClient) ContainerFsInfo(context.Context) (cadvisorapi.FsInfo, error) {
	return cadvisorapi.FsInfo{}, nil
}

// RootFsInfo 返回根路径的文件系统统计。
func (cu *cadvisorClient) RootFsInfo() (cadvisorapi.FsInfo, error) {
	return cu.GetDirFsInfo(cu.rootPath)
}

// GetDirFsInfo 返回指定目录的文件系统统计。
func (cu *cadvisorClient) GetDirFsInfo(path string) (cadvisorapi.FsInfo, error) {
	return cu.winStatsClient.GetDirFsInfo(path)
}

// IsPsiEnabled 在 Windows 上始终返回 false。
func IsPsiEnabled(_ klog.Logger) bool {
	return false
}
