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

package flexvolume

import (
	"encoding/base64"
	"fmt"
	"os"

	"k8s.io/klog/v2"
	"k8s.io/mount-utils"

	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util"
)

// addSecretsToOptions 获取 spec 引用的 Secret，并以 base64 编码写入驱动选项。
// 未指定 Secret 时直接返回；Secret 必须能被当前 kube 客户端读取。
func addSecretsToOptions(options map[string]string, spec *volume.Spec, namespace string, driverName string, host volume.VolumeHost) error {
	secretName, secretNamespace, err := getSecretNameAndNamespace(spec, namespace)
	if err != nil {
		return err
	}

	if len(secretName) == 0 || len(secretNamespace) == 0 {
		return nil
	}

	kubeClient := host.GetKubeClient()
	if kubeClient == nil {
		return fmt.Errorf("cannot get kube client")
	}

	secrets, err := util.GetSecretForPV(secretNamespace, secretName, driverName, host.GetKubeClient())
	if err != nil {
		err = fmt.Errorf("couldn't get secret %v/%v err: %w", secretNamespace, secretName, err)
		return err
	}
	for name, data := range secrets {
		// 以固定前缀写入选项，避免与用户自定义选项键冲突。
		options[optionKeySecret+"/"+name] = base64.StdEncoding.EncodeToString([]byte(data))
		klog.V(1).Infof("found flex volume secret info: %s", name)
	}

	return nil
}

// 共享错误：spec 不是 flexVolume 卷。
var errNotFlexVolume = fmt.Errorf("not a flex volume")

// getDriver 从内联卷或 PV 规格中提取 flexVolume 驱动名。
func getDriver(spec *volume.Spec) (string, error) {
	if spec.Volume != nil && spec.Volume.FlexVolume != nil {
		return spec.Volume.FlexVolume.Driver, nil
	}
	if spec.PersistentVolume != nil && spec.PersistentVolume.Spec.FlexVolume != nil {
		return spec.PersistentVolume.Spec.FlexVolume.Driver, nil
	}
	return "", errNotFlexVolume
}

// getFSType 从内联卷或 PV 规格中提取 flexVolume 指定的文件系统类型。
func getFSType(spec *volume.Spec) (string, error) {
	if spec.Volume != nil && spec.Volume.FlexVolume != nil {
		return spec.Volume.FlexVolume.FSType, nil
	}
	if spec.PersistentVolume != nil && spec.PersistentVolume.Spec.FlexVolume != nil {
		return spec.PersistentVolume.Spec.FlexVolume.FSType, nil
	}
	return "", errNotFlexVolume
}

// getSecretNameAndNamespace 解析 Secret 引用：内联卷使用 Pod 命名空间，
// PV 使用引用中指定的命名空间，缺省时回退到 Pod 命名空间。
func getSecretNameAndNamespace(spec *volume.Spec, podNamespace string) (string, string, error) {
	if spec.Volume != nil && spec.Volume.FlexVolume != nil {
		if spec.Volume.FlexVolume.SecretRef == nil {
			return "", "", nil
		}
		return spec.Volume.FlexVolume.SecretRef.Name, podNamespace, nil
	}
	if spec.PersistentVolume != nil && spec.PersistentVolume.Spec.FlexVolume != nil {
		if spec.PersistentVolume.Spec.FlexVolume.SecretRef == nil {
			return "", "", nil
		}
		secretName := spec.PersistentVolume.Spec.FlexVolume.SecretRef.Name
		secretNamespace := spec.PersistentVolume.Spec.FlexVolume.SecretRef.Namespace
		if len(secretNamespace) == 0 {
			secretNamespace = podNamespace
		}
		return secretName, secretNamespace, nil
	}
	return "", "", errNotFlexVolume
}

// getReadOnly 返回卷是否只读：内联卷取字段值，
// PV 场景只读属性由卷规格整体决定，而非 flexVolume 驱动字段。
func getReadOnly(spec *volume.Spec) (bool, error) {
	if spec.Volume != nil && spec.Volume.FlexVolume != nil {
		return spec.Volume.FlexVolume.ReadOnly, nil
	}
	if spec.PersistentVolume != nil && spec.PersistentVolume.Spec.FlexVolume != nil {
		// PV 场景下只读属性在卷规格层面指定。
		return spec.ReadOnly, nil
	}
	return false, errNotFlexVolume
}

// getOptions 提取 flexVolume 的自定义驱动选项。
func getOptions(spec *volume.Spec) (map[string]string, error) {
	if spec.Volume != nil && spec.Volume.FlexVolume != nil {
		return spec.Volume.FlexVolume.Options, nil
	}
	if spec.PersistentVolume != nil && spec.PersistentVolume.Spec.FlexVolume != nil {
		return spec.PersistentVolume.Spec.FlexVolume.Options, nil
	}
	return nil, errNotFlexVolume
}

// prepareForMount 确保挂载目录存在，并返回该路径当前是否已被挂载。
// 目录不存在时以 0750 创建：仅属主可写、同组可读，避免敏感数据泄露。
func prepareForMount(mounter mount.Interface, deviceMountPath string) (bool, error) {

	notMnt, err := mounter.IsLikelyNotMountPoint(deviceMountPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(deviceMountPath, 0750); err != nil {
				return false, err
			}
			notMnt = true
		} else {
			return false, err
		}
	}

	return !notMnt, nil
}

// doMount 将设备挂载到指定路径，调用前须先通过 prepareForMount 准备好目录。
func doMount(mounter mount.Interface, devicePath, deviceMountPath, fsType string, options []string) error {
	err := mounter.MountSensitiveWithoutSystemd(devicePath, deviceMountPath, fsType, options, nil)
	if err != nil {
		klog.Errorf("Failed to mount the volume at %s, device: %s, error: %s", deviceMountPath, devicePath, err.Error())
		return err
	}
	return nil
}

// isNotMounted 判断路径是否尚未挂载，检查失败时返回错误。
func isNotMounted(mounter mount.Interface, deviceMountPath string) (bool, error) {
	notmnt, err := mounter.IsLikelyNotMountPoint(deviceMountPath)
	if err != nil {
		klog.Errorf("Error checking mount point %s, error: %v", deviceMountPath, err)
		return false, err
	}
	return notmnt, nil
}
