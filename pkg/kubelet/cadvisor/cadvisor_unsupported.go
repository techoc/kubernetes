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

package cadvisor

import (
	"context"
	"errors"

	cadvisorapi "github.com/google/cadvisor/lib/model"
	"k8s.io/klog/v2"
)

// cadvisorUnsupported 是不支持 cAdvisor 的构建中的空实现。
type cadvisorUnsupported struct {
}

var _ Interface = new(cadvisorUnsupported)

// New 在不支持 cAdvisor 的系统上返回空实现。
func New(_ klog.Logger, imageFsInfoProvider ImageFsInfoProvider, rootPath string, cgroupsRoots []string, usingLegacyStats, localStorageCapacityIsolation, disableContainerDiscovery bool) (Interface, error) {
	return &cadvisorUnsupported{}, nil
}

// errUnsupported 表示当前构建不支持 cAdvisor。
var errUnsupported = errors.New("cAdvisor is unsupported in this build")

// Start 直接返回不支持错误。
func (cu *cadvisorUnsupported) Start() error {
	return errUnsupported
}

// ContainerInfoV2 直接返回不支持错误。
func (cu *cadvisorUnsupported) ContainerInfoV2(name string, options cadvisorapi.RequestOptions) (map[string]cadvisorapi.ContainerInfo, error) {
	return nil, errUnsupported
}

// GetRequestedContainersInfo 直接返回不支持错误。
func (cu *cadvisorUnsupported) GetRequestedContainersInfo(containerName string, options cadvisorapi.RequestOptions) (map[string]*cadvisorapi.ContainerInfo, error) {
	return nil, errUnsupported
}

// MachineInfo 直接返回不支持错误。
func (cu *cadvisorUnsupported) MachineInfo(logger klog.Logger) (*cadvisorapi.MachineInfo, error) {
	return nil, errUnsupported
}

// VersionInfo 直接返回不支持错误。
func (cu *cadvisorUnsupported) VersionInfo() (*cadvisorapi.VersionInfo, error) {
	return nil, errUnsupported
}

// ImagesFsInfo 直接返回不支持错误。
func (cu *cadvisorUnsupported) ImagesFsInfo(context.Context) (cadvisorapi.FsInfo, error) {
	return cadvisorapi.FsInfo{}, errUnsupported
}

// RootFsInfo 直接返回不支持错误。
func (cu *cadvisorUnsupported) RootFsInfo() (cadvisorapi.FsInfo, error) {
	return cadvisorapi.FsInfo{}, errUnsupported
}

// ContainerFsInfo 直接返回不支持错误。
func (cu *cadvisorUnsupported) ContainerFsInfo(context.Context) (cadvisorapi.FsInfo, error) {
	return cadvisorapi.FsInfo{}, errUnsupported
}

// GetDirFsInfo 在不受支持的构建中返回空的 FsInfo。
func (cu *cadvisorUnsupported) GetDirFsInfo(path string) (cadvisorapi.FsInfo, error) {
	return cadvisorapi.FsInfo{}, nil
}

// IsPsiEnabled 在不受支持的构建中始终返回 false。
func IsPsiEnabled(_ klog.Logger) bool {
	return false
}
