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

//go:generate mockery
package cadvisor

import (
	"context"

	cadvisorapi "github.com/google/cadvisor/lib/model"
	"k8s.io/klog/v2"
)

// Interface 抽象 cAdvisor 的接口，便于测试替换。
type Interface interface {
	// Start 启动 cAdvisor。
	Start() error
	// ContainerInfoV2 返回指定容器的信息（cAdvisor v2 接口）。
	ContainerInfoV2(name string, options cadvisorapi.RequestOptions) (map[string]cadvisorapi.ContainerInfo, error)
	// GetRequestedContainersInfo 返回指定容器的详细信息。
	GetRequestedContainersInfo(containerName string, options cadvisorapi.RequestOptions) (map[string]*cadvisorapi.ContainerInfo, error)
	// MachineInfo 返回节点机器信息。
	MachineInfo(logger klog.Logger) (*cadvisorapi.MachineInfo, error)

	// VersionInfo 返回 cAdvisor 的版本信息。
	VersionInfo() (*cadvisorapi.VersionInfo, error)

	// ImagesFsInfo 返回存放容器镜像的文件系统使用信息。
	ImagesFsInfo(context.Context) (cadvisorapi.FsInfo, error)

	// RootFsInfo 返回根文件系统的使用信息。
	RootFsInfo() (cadvisorapi.FsInfo, error)

	// ContainerFsInfo 返回可写层的使用信息；KEP 4191 支持把镜像文件系统独立出来。
	ContainerFsInfo(context.Context) (cadvisorapi.FsInfo, error)

	// GetDirFsInfo 返回包含指定文件的文件系统信息。
	GetDirFsInfo(path string) (cadvisorapi.FsInfo, error)
}

// ImageFsInfoProvider 告知 cAdvisor 如何定位容器镜像所在的文件系统。
type ImageFsInfoProvider interface {
	// ImageFsInfoLabel 返回 cAdvisor 用于查找镜像文件系统的标签。
	ImageFsInfoLabel() (string, error)
	// ContainerFsInfoLabel 在镜像文件系统拆分时与 ImageFsInfoLabel 不同，返回可写层文件系统的标签。
	ContainerFsInfoLabel() (string, error)
}
