/*
Copyright 2024 The Kubernetes Authors.

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

package selinuxwarning

import (
	"fmt"

	authenticationv1 "k8s.io/api/authentication/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	storagelisters "k8s.io/client-go/listers/storage/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util/subpath"
	"k8s.io/mount-utils"
)

var _ volume.VolumeHost = &Controller{}
var _ volume.CSIDriverVolumeHost = &Controller{}

// 以下方法是 volume.VolumeHost 接口的实现。
// 该接口包含大量 kubelet 专有的方法，但 SELinux 警告控制器并不使用它们；
// 因此这里返回空值或明确的 "unsupported" 错误，而不是尝试实现真实的卷挂载逻辑。
// GetPluginDir 未在此控制器中使用，返回空路径。
func (c *Controller) GetPluginDir(podUID string) string {
	return ""
}

// GetVolumeDevicePluginDir 未在此控制器中使用，返回空路径。
func (c *Controller) GetVolumeDevicePluginDir(podUID string) string {
	return ""
}

// GetPodsDir 未在此控制器中使用，返回空路径。
func (c *Controller) GetPodsDir() string {
	return ""
}

// GetPodVolumeDir 未在此控制器中使用，返回空路径。
func (c *Controller) GetPodVolumeDir(podUID types.UID, pluginName, volumeName string) string {
	return ""
}

// GetPodPluginDir 未在此控制器中使用，返回空路径。
func (c *Controller) GetPodPluginDir(podUID types.UID, pluginName string) string {
	return ""
}

// GetPodVolumeDeviceDir 未在此控制器中使用，返回空路径。
func (c *Controller) GetPodVolumeDeviceDir(podUID types.UID, pluginName string) string {
	return ""
}

// GetKubeClient 返回控制器持有的 kube client，供卷插件在需要时访问 API。
func (c *Controller) GetKubeClient() clientset.Interface {
	return c.kubeClient
}

// NewWrapperMounter 明确返回不支持错误：控制器不需要真正挂载卷。
func (c *Controller) NewWrapperMounter(volName string, spec volume.Spec, pod *v1.Pod) (volume.Mounter, error) {
	return nil, fmt.Errorf("NewWrapperMounter not supported by SELinux controller VolumeHost implementation")
}

// NewWrapperUnmounter 明确返回不支持错误：控制器不需要真正卸载卷。
func (c *Controller) NewWrapperUnmounter(volName string, spec volume.Spec, podUID types.UID) (volume.Unmounter, error) {
	return nil, fmt.Errorf("NewWrapperUnmounter not supported by SELinux controller VolumeHost implementation")
}

// GetMounter 未在此控制器中使用，返回 nil。
func (c *Controller) GetMounter() mount.Interface {
	return nil
}

// GetNodeAllocatable 返回空的资源列表：控制器不关心节点可分配资源。
func (c *Controller) GetNodeAllocatable() (v1.ResourceList, error) {
	return v1.ResourceList{}, nil
}

// GetAttachedVolumesFromNodeStatus 返回空的卷映射：控制器不读取节点状态中的挂载卷。
func (c *Controller) GetAttachedVolumesFromNodeStatus() (map[v1.UniqueVolumeName]string, error) {
	return map[v1.UniqueVolumeName]string{}, nil
}

// GetSecretFunc 返回一个明确报错的取 Secret 函数，防止卷插件意外依赖未实现的能力。
func (c *Controller) GetSecretFunc() func(namespace, name string) (*v1.Secret, error) {
	return func(_, _ string) (*v1.Secret, error) {
		return nil, fmt.Errorf("GetSecret unsupported in SELinux controller")
	}
}

// GetConfigMapFunc 返回一个明确报错的取 ConfigMap 函数，原因同 GetSecretFunc。
func (c *Controller) GetConfigMapFunc() func(namespace, name string) (*v1.ConfigMap, error) {
	return func(_, _ string) (*v1.ConfigMap, error) {
		return nil, fmt.Errorf("GetConfigMap unsupported in SELinux controller")
	}
}

// GetServiceAccountTokenFunc 返回一个明确报错的令牌请求函数：控制器不需要签发 ServiceAccount token。
func (c *Controller) GetServiceAccountTokenFunc() func(_, _ string, _ *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error) {
	return func(_, _ string, _ *authenticationv1.TokenRequest) (*authenticationv1.TokenRequest, error) {
		return nil, fmt.Errorf("GetServiceAccountToken unsupported in SELinux controller")
	}
}

// DeleteServiceAccountTokenFunc 返回一个仅记录错误的函数：此操作在控制器中不受支持。
func (c *Controller) DeleteServiceAccountTokenFunc() func(types.UID) {
	return func(types.UID) {
		// nolint:logcheck
		klog.ErrorS(nil, "DeleteServiceAccountToken unsupported in SELinux controller")
	}
}

// GetNodeLabels 明确返回不支持错误：控制器不使用节点标签参与卷处理。
func (c *Controller) GetNodeLabels() (map[string]string, error) {
	return nil, fmt.Errorf("GetNodeLabels() unsupported in SELinux controller")
}

// GetNodeName 返回空节点名：控制器没有绑定具体 kubelet 节点。
func (c *Controller) GetNodeName() types.NodeName {
	return ""
}

// GetEventRecorder 返回 nil：告警通过控制器自身的机制发布，而非卷插件事件。
func (c *Controller) GetEventRecorder() record.EventRecorder {
	return nil
}

// GetSubpather 返回 nil：控制器不执行 subpath 挂载操作。
func (c *Controller) GetSubpather() subpath.Interface {
	return nil
}

// CSIDriverLister 返回控制器的 CSI 驱动 lister，供卷插件查询 CSI 驱动信息。
func (c *Controller) CSIDriverLister() storagelisters.CSIDriverLister {
	return c.csiDriverLister
}
