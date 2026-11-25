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

package utils

import (
	"fmt"
	"path"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	e2eframework "k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
)

// PatchCSIDeployment 修改 CSI driver 的 Deployment：
//   - 替换 provisioner 名称
//   - 强制 Pod 运行在指定节点上
//
// 这些都是可选项，参见 PatchCSIOptions。但要注意，不重命名 CSI driver 的 Deployment 可能引发问题：
//   - 多个测试部署同一 driver 时，必须串行运行
//   - 可能与手动部署冲突
//
// 本函数适用于遵循以下约定的 CSI driver Deployment：
//   - driver 名称与 provisioner 名称相同
//   - driver 二进制接受 --drivername 参数
//   - 容器内的路径要么固定且无需 patch（例如 --csi-address=/csi/csi.sock），
//     要么直接在参数中指定（例如
//     --kubelet-registration-path=/var/lib/kubelet/plugins/csi-hostpath/csi.sock）
//
// 不符合这些约定的 driver Deployment 只能自行 patch，或完全跳过 patch。
func PatchCSIDeployment(f *e2eframework.Framework, o PatchCSIOptions, object interface{}) error {
	rename := o.OldDriverName != "" && o.NewDriverName != "" &&
		o.OldDriverName != o.NewDriverName

	substKubeletRootDir := func(s string) string {
		return strings.ReplaceAll(s, "/var/lib/kubelet/", e2eframework.TestContext.KubeletRootDir+"/")
	}

	patchVolumes := func(volumes []v1.Volume) {
		if !rename {
			return
		}
		for i := range volumes {
			volume := &volumes[i]
			if volume.HostPath != nil {
				// 更新形如 /var/lib/kubelet/plugins/<provisioner> 的路径。
				p := &volume.HostPath.Path
				dir, file := path.Split(*p)
				if file == o.OldDriverName {
					*p = path.Join(dir, o.NewDriverName)
				}
				// 注入非标准的 kubelet 路径。
				*p = substKubeletRootDir(*p)
			}
		}
	}

	patchContainers := func(containers []v1.Container) {
		for i := range containers {
			container := &containers[i]
			if rename {
				for e := range container.Args {
					// 将测试专用的 provider 名称注入如下路径：
					// --kubelet-registration-path=/var/lib/kubelet/plugins/csi-hostpath/csi.sock
					container.Args[e] = strings.Replace(container.Args[e], "/"+o.OldDriverName+"/", "/"+o.NewDriverName+"/", 1)
				}
			}

			// 修改 --kubelet-registration-path。
			for e := range container.Args {
				container.Args[e] = substKubeletRootDir(container.Args[e])
			}
			for e := range container.VolumeMounts {
				container.VolumeMounts[e].MountPath = substKubeletRootDir(container.VolumeMounts[e].MountPath)
			}

			if len(o.Features) > 0 && len(o.Features[container.Name]) > 0 {
				featuregateString := strings.Join(o.Features[container.Name], ",")
				container.Args = append(container.Args, fmt.Sprintf("--feature-gates=%s", featuregateString))
			}

			// 通过追加带有正确值的参数，覆盖 driver 名称或 provider 名称。
			switch container.Name {
			case o.DriverContainerName:
				container.Args = append(container.Args, o.DriverContainerArguments...)
			}
		}
	}

	patchPodSpec := func(spec *v1.PodSpec) {
		patchContainers(spec.Containers)
		patchVolumes(spec.Volumes)
		if o.NodeName != "" {
			e2epod.SetNodeSelection(spec, e2epod.NodeSelection{Name: o.NodeName})
		}
	}

	switch object := object.(type) {
	case *appsv1.ReplicaSet:
		patchPodSpec(&object.Spec.Template.Spec)
	case *appsv1.DaemonSet:
		patchPodSpec(&object.Spec.Template.Spec)
	case *appsv1.StatefulSet:
		patchPodSpec(&object.Spec.Template.Spec)
	case *appsv1.Deployment:
		patchPodSpec(&object.Spec.Template.Spec)
	case *storagev1.StorageClass:
		if o.NewDriverName != "" {
			// 这里假定 driver 名称与 provisioner 名称相同。
			object.Provisioner = o.NewDriverName
		}
	case *storagev1.CSIDriver:
		if o.NewDriverName != "" {
			object.Name = o.NewDriverName
		}
		if o.PodInfo != nil {
			object.Spec.PodInfoOnMount = o.PodInfo
		}
		if o.StorageCapacity != nil {
			object.Spec.StorageCapacity = o.StorageCapacity
		}
		if o.CanAttach != nil {
			object.Spec.AttachRequired = o.CanAttach
		}
		if o.VolumeLifecycleModes != nil {
			object.Spec.VolumeLifecycleModes = *o.VolumeLifecycleModes
		}
		if o.TokenRequests != nil {
			object.Spec.TokenRequests = o.TokenRequests
		}
		if o.RequiresRepublish != nil {
			object.Spec.RequiresRepublish = o.RequiresRepublish
		}
		if o.ServiceAccountTokenInSecrets != nil {
			object.Spec.ServiceAccountTokenInSecrets = o.ServiceAccountTokenInSecrets
		}
		if o.FSGroupPolicy != nil {
			object.Spec.FSGroupPolicy = o.FSGroupPolicy
		}
		if o.SELinuxMount != nil {
			object.Spec.SELinuxMount = o.SELinuxMount
		}
	}

	return nil
}

// PatchCSIOptions 控制 PatchCSIDeployment 如何修改对象。
type PatchCSIOptions struct {
	// 原始的 driver 名称。
	OldDriverName string
	// 替换原始名称的 driver 名称。
	// 可以为空（完全不使用），或等于 OldDriverName
	// （此时会在合适的位置补上该名称，而不改写已有字段）。
	NewDriverName string
	// 包含 CSI driver 二进制文件的容器名称。
	// 非空时，会将 DriverContainerArguments 追加到该容器的参数列表中。
	DriverContainerName string
	// 要追加到 DriverContainerName 对应容器的参数列表。
	DriverContainerArguments []string
	// 包含 provisioner 二进制文件的容器名称。
	// 非空时，会将带新名称的 --provisioner 追加到参数列表中。
	ProvisionerContainerName string
	// 包含 snapshotter 二进制文件的容器名称。
	// 非空时，会将带新名称的 --snapshotter 追加到参数列表中。
	SnapshotterContainerName string
	// 非空时，强制所有 Pod 运行在该节点上。
	NodeName string
	// 非 nil 时，*如果* driver 部署了 CSIDriver 对象，该值将用于
	// CSIDriver.Spec.PodInfo 字段；否则忽略。
	PodInfo *bool
	// 非 nil 时，*如果* driver 部署了 CSIDriver 对象，该值将用于
	// CSIDriver.Spec.CanAttach 字段；否则忽略。
	CanAttach *bool
	// 非 nil 时，*如果* driver 部署了 CSIDriver 对象，该值将用于
	// CSIDriver.Spec.StorageCapacity 字段；否则忽略。
	StorageCapacity *bool
	// 非 nil 时，*如果* driver 部署了 CSIDriver 对象，该值将用于
	// CSIDriver.Spec.VolumeLifecycleModes 字段；否则忽略。
	VolumeLifecycleModes *[]storagev1.VolumeLifecycleMode
	// 非 nil 时，*如果* driver 部署了 CSIDriver 对象，该值将用于
	// CSIDriver.Spec.TokenRequests 字段；否则忽略。
	TokenRequests []storagev1.TokenRequest
	// 非 nil 时，*如果* driver 部署了 CSIDriver 对象，该值将用于
	// CSIDriver.Spec.RequiresRepublish 字段；否则忽略。
	RequiresRepublish *bool
	// 非 nil 时，*如果* driver 部署了 CSIDriver 对象，该值将用于
	// CSIDriver.Spec.ServiceAccountTokenInSecrets 字段；否则忽略。
	ServiceAccountTokenInSecrets *bool
	// 非 nil 时，*如果* driver 部署了 CSIDriver 对象，该值将用于
	// CSIDriver.Spec.FSGroupPolicy 字段；否则忽略。
	FSGroupPolicy *storagev1.FSGroupPolicy
	// 非 nil 时，*如果* driver 部署了 CSIDriver 对象，该值将用于
	// CSIDriver.Spec.SELinuxMount 字段；否则忽略。
	SELinuxMount *bool
	// 非 nil 时，这些值用于为特定 sidecar 设置 feature 参数。
	// Feature 是一个 map，key 是 sidecar 名称，例如：
	//	-- key: resizer
	//	-- value: []string{feature-gates}
	Features map[string][]string
}
