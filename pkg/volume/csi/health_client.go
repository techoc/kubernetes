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

package csi

import (
	"context"

	api "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/kubernetes/pkg/volume/csi/nodeinfomanager"
)

// HealthClient 是 CSI 节点 RPC 的子集，用于探测卷与存储的健康状态。
// 通过 Supports 方法先判断驱动是否支持，避免产生不支持的 RPC 调用。
type HealthClient interface {
	NodeGetVolumeHealth(ctx context.Context, volID, stagingTargetPath, volumePublishPath string) ([]api.VolumeHealthCondition, error)
	NodeGetStorageHealth(ctx context.Context, secrets map[string]string) ([]storagev1.StorageHealthCondition, error)
	NodeSupportsVolumeHealth(ctx context.Context) (bool, error)
	NodeSupportsStorageHealth(ctx context.Context) (bool, error)
}

// NewHealthClient 为指定的已注册 CSI 驱动创建 HealthClient。
func NewHealthClient(driverName string) HealthClient {
	c := &CSIHealthClient{}
	c.csiClientGetter.driverName = csiDriverName(driverName)
	return c
}

// 编译时断言 CSIHealthClient 实现 HealthClient 接口。
var _ HealthClient = &CSIHealthClient{}

// ListRegisteredDrivers 返回当前已向 kubelet 注册的 CSI 驱动名称列表。
func ListRegisteredDrivers() []string {
	return csiDrivers.List()
}

// GetNodeInfoManager 返回用于更新 CSINode 对象的节点信息管理器。
// CSI 插件初始化前可能返回 nil。
func GetNodeInfoManager() nodeinfomanager.Interface {
	return nim
}

// CSIHealthClient 包装带缓存的 CSI 客户端，并为每次调用附加超时控制。
// 健康检查不应无限期阻塞 kubelet，故所有 RPC 都使用统一的 csiTimeout。
type CSIHealthClient struct {
	csiClientGetter
}

// NodeSupportsVolumeHealth 查询驱动是否支持卷健康状态上报。
func (c *CSIHealthClient) NodeSupportsVolumeHealth(ctx context.Context) (bool, error) {
	client, err := c.csiClientGetter.Get()
	if err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(ctx, csiTimeout)
	defer cancel()
	return client.NodeSupportsVolumeHealth(ctx)
}

// NodeSupportsStorageHealth 查询驱动是否支持存储健康状态上报。
func (c *CSIHealthClient) NodeSupportsStorageHealth(ctx context.Context) (bool, error) {
	client, err := c.csiClientGetter.Get()
	if err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(ctx, csiTimeout)
	defer cancel()
	return client.NodeSupportsStorageHealth(ctx)
}

// NodeGetVolumeHealth 返回指定卷的健康状况列表。
// stagingTargetPath 与 volumePublishPath 帮助驱动定位卷的不同挂载阶段。
func (c *CSIHealthClient) NodeGetVolumeHealth(ctx context.Context, volID, stagingTargetPath, volumePublishPath string) ([]api.VolumeHealthCondition, error) {
	client, err := c.csiClientGetter.Get()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, csiTimeout)
	defer cancel()
	return client.NodeGetVolumeHealth(ctx, volID, stagingTargetPath, volumePublishPath)
}

// NodeGetStorageHealth 返回存储后端的健康状况列表，secrets 用于驱动认证。
func (c *CSIHealthClient) NodeGetStorageHealth(ctx context.Context, secrets map[string]string) ([]storagev1.StorageHealthCondition, error) {
	client, err := c.csiClientGetter.Get()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, csiTimeout)
	defer cancel()
	return client.NodeGetStorageHealth(ctx, secrets)
}
