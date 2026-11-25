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

package persistentvolume

import (
	"fmt"

	"k8s.io/klog/v2"
	"k8s.io/mount-utils"

	authenticationv1 "k8s.io/api/authentication/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	vol "k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util/subpath"
)

// 本文件为 PersistentVolumeController 提供 vol.VolumeHost 接口的实现。
// PV 控制器只负责卷的供给/绑定/回收，并不在节点上挂载卷，因此与节点路径相关的
// 方法均返回空值或空实现；编译期断言确保接口变更能被及时发现。

var _ vol.VolumeHost = &PersistentVolumeController{}

// GetPluginDir 返回插件目录；PV 控制器不在节点上执行挂载，故无需返回路径。
func (ctrl *PersistentVolumeController) GetPluginDir(pluginName string) string {
	return ""
}

// GetVolumeDevicePluginDir 返回设备插件目录；PV 控制器不使用该路径。
func (ctrl *PersistentVolumeController) GetVolumeDevicePluginDir(pluginName string) string {
	return ""
}

// GetPodsDir 返回 Pod 目录；PV 控制器不使用该路径。
func (ctrl *PersistentVolumeController) GetPodsDir() string {
	return ""
}

// GetPodVolumeDir 返回 Pod 的卷目录；PV 控制器不使用该路径。
func (ctrl *PersistentVolumeController) GetPodVolumeDir(podUID types.UID, pluginName string, volumeName string) string {
	return ""
}

// GetPodPluginDir 返回 Pod 的插件目录；PV 控制器不使用该路径。
func (ctrl *PersistentVolumeController) GetPodPluginDir(podUID types.UID, pluginName string) string {
	return ""
}

// GetPodVolumeDeviceDir 返回 Pod 的卷设备目录；PV 控制器不使用该路径。
func (ctrl *PersistentVolumeController) GetPodVolumeDeviceDir(ppodUID types.UID, pluginName string) string {
	return ""
}

// GetKubeClient 返回控制器使用的 Kubernetes 客户端。
func (ctrl *PersistentVolumeController) GetKubeClient() clientset.Interface {
	return ctrl.kubeClient
}

// NewWrapperMounter 构造带包装的 Mounter；PV 控制器不需要此能力。
func (ctrl *PersistentVolumeController) NewWrapperMounter(volName string, spec vol.Spec, pod *v1.Pod) (vol.Mounter, error) {
	return nil, fmt.Errorf("PersistentVolumeController.NewWrapperMounter is not implemented")
}

// NewWrapperUnmounter 构造带包装的 Unmounter；PV 控制器不需要此能力。
func (ctrl *PersistentVolumeController) NewWrapperUnmounter(volName string, spec vol.Spec, podUID types.UID) (vol.Unmounter, error) {
	return nil, fmt.Errorf("PersistentVolumeController.NewWrapperMounter is not implemented")
}

// GetMounter 返回挂载器；PV 控制器不执行挂载操作。
func (ctrl *PersistentVolumeController) GetMounter() mount.Interface {
	return nil
}

// GetNodeAllocatable 返回节点可分配资源；PV 控制器无节点上下文，返回空列表。
func (ctrl *PersistentVolumeController) GetNodeAllocatable() (v1.ResourceList, error) {
	return v1.ResourceList{}, nil
}

// GetAttachedVolumesFromNodeStatus 返回节点状态中已挂载的卷；PV 控制器不维护该信息。
func (ctrl *PersistentVolumeController) GetAttachedVolumesFromNodeStatus() (map[v1.UniqueVolumeName]string, error) {
	return map[v1.UniqueVolumeName]string{}, nil
}

// GetSecretFunc 返回读取 Secret 的函数；该能力仅节点侧挂载流程需要。
func (ctrl *PersistentVolumeController) GetSecretFunc() func(namespace, name string) (*v1.Secret, error) {
	return func(_, _ string) (*v1.Secret, error) {
		return nil, fmt.Errorf("GetSecret unsupported in PersistentVolumeController")
	}
}

// GetConfigMapFunc 返回读取 ConfigMap 的函数；该能力仅节点侧挂载流程需要。
func (ctrl *PersistentVolumeController) GetConfigMapFunc() func(namespace, name string) (*v1.ConfigMap, error) {
	return func(_, _ string) (*v1.ConfigMap, error) {
		return nil, fmt.Errorf("GetConfigMap unsupported in PersistentVolumeController")
	}
}

// GetServiceAccountTokenFunc 返回申请 ServiceAccount Token 的函数；PV 控制器不支持。
func (ctrl *PersistentVolumeController) GetServiceAccountTokenFunc() func(_, _ string, _ *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error) {
	return func(_, _ string, _ *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error) {
		return nil, fmt.Errorf("GetServiceAccountToken unsupported in PersistentVolumeController")
	}
}

// DeleteServiceAccountTokenFunc 返回删除 ServiceAccount Token 的函数；PV 控制器仅记录错误。
func (ctrl *PersistentVolumeController) DeleteServiceAccountTokenFunc() func(types.UID) {
	return func(types.UID) {
		//nolint:logcheck
		klog.ErrorS(nil, "DeleteServiceAccountToken unsupported in PersistentVolumeController")
	}
}

// GetNodeLabels 返回节点标签；PV 控制器无节点上下文，不支持此操作。
func (ctrl *PersistentVolumeController) GetNodeLabels() (map[string]string, error) {
	return nil, fmt.Errorf("GetNodeLabels() unsupported in PersistentVolumeController")
}

// GetNodeName 返回节点名；PV 控制器运行在控制面而非特定节点上。
func (ctrl *PersistentVolumeController) GetNodeName() types.NodeName {
	return ""
}

// GetEventRecorder 返回控制器用于记录事件的 EventRecorder。
func (ctrl *PersistentVolumeController) GetEventRecorder() record.EventRecorder {
	return ctrl.eventRecorder
}

// GetSubpather 返回 subpath 操作接口；PV 控制器中没有任何卷插件需要 subpath。
func (ctrl *PersistentVolumeController) GetSubpather() subpath.Interface {
	// PV 控制器没有需要 subpath 的卷插件。
	return nil
}
