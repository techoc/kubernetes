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

package csi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	api "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/volume"
	utilstrings "k8s.io/utils/strings"
)

const (
	// TestInformerSyncPeriod 是测试时使用的 informer 同步周期。
	TestInformerSyncPeriod = 100 * time.Millisecond
	// TestInformerSyncTimeout 是测试时使用的 informer 超时时长。
	TestInformerSyncTimeout = 30 * time.Second
)

// getCredentialsFromSecret 从 Secret 中读取 CSI 驱动所需的凭据。
func getCredentialsFromSecret(k8s kubernetes.Interface, secretRef *api.SecretReference) (map[string]string, error) {
	credentials := map[string]string{}
	secret, err := k8s.CoreV1().Secrets(secretRef.Namespace).Get(context.TODO(), secretRef.Name, meta.GetOptions{})
	if err != nil {
		return credentials, errors.New(log("failed to find the secret %s in the namespace %s with error: %v", secretRef.Name, secretRef.Namespace, err))
	}
	for key, value := range secret.Data {
		credentials[key] = string(value)
	}

	return credentials, nil
}

// saveVolumeData 将参数数据以 JSON 文件形式保存到指定位置。
func saveVolumeData(dir string, fileName string, data map[string]string) error {
	dataFilePath := filepath.Join(dir, fileName)
	klog.V(4).Info(log("saving volume data file [%s]", dataFilePath))
	file, err := os.Create(dataFilePath)
	if err != nil {
		return errors.New(log("failed to save volume data file %s: %v", dataFilePath, err))
	}
	defer file.Close()
	if err := json.NewEncoder(file).Encode(data); err != nil {
		return errors.New(log("failed to save volume data file %s: %v", dataFilePath, err))
	}
	klog.V(4).Info(log("volume data file saved successfully [%s]", dataFilePath))
	return nil
}

// loadVolumeData 从指定的 JSON 文件加载卷信息。
func loadVolumeData(dir string, fileName string) (map[string]string, error) {
	// 去掉末尾的 /mount。
	dataFileName := filepath.Join(dir, fileName)
	klog.V(4).Info(log("loading volume data file [%s]", dataFileName))

	file, err := os.Open(dataFileName)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", log("failed to open volume data file [%s]", dataFileName), err)
	}
	defer file.Close()
	data := map[string]string{}
	if err := json.NewDecoder(file).Decode(&data); err != nil {
		return nil, errors.New(log("failed to parse volume data file [%s]: %v", dataFileName, err))
	}

	return data, nil
}

// getCSISourceFromSpec 从卷规格中提取 CSI 持久卷来源。
func getCSISourceFromSpec(spec *volume.Spec) (*api.CSIPersistentVolumeSource, error) {
	return getPVSourceFromSpec(spec)
}

// getReadOnlyFromSpec 从卷规格中提取只读标志。
func getReadOnlyFromSpec(spec *volume.Spec) (bool, error) {
	if spec.PersistentVolume != nil &&
		spec.PersistentVolume.Spec.CSI != nil {
		return spec.ReadOnly, nil
	}

	return false, fmt.Errorf("CSIPersistentVolumeSource not defined in spec")
}

// log 在日志字符串前加上 `kubernetes.io/csi` 前缀。
func log(msg string, parts ...interface{}) string {
	return fmt.Sprintf(fmt.Sprintf("%s: %s", CSIPluginName, msg), parts...)
}

// getVolumePluginDir 返回 CSI 插件保存指定卷元数据的路径。
func getVolumePluginDir(specVolID string, host volume.VolumeHost) string {
	sanitizedSpecVolID := utilstrings.EscapeQualifiedName(specVolID)
	return filepath.Join(host.GetVolumeDevicePluginDir(CSIPluginName), sanitizedSpecVolID)
}

// getVolumeDevicePluginDir 返回 CSI 插件保存与指定 specVolumeID 关联的
// 块设备符号链接的路径。
// 路径格式：plugins/kubernetes.io/csi/volumeDevices/{specVolumeID}/dev
func getVolumeDevicePluginDir(specVolID string, host volume.VolumeHost) string {
	return filepath.Join(getVolumePluginDir(specVolID, host), "dev")
}

// getVolumeDeviceDataDir 返回 CSI 插件保存与指定 specVolumeID 关联的
// 块设备卷数据的路径。
// 路径格式：plugins/kubernetes.io/csi/volumeDevices/{specVolumeID}/data
func getVolumeDeviceDataDir(specVolID string, host volume.VolumeHost) string {
	return filepath.Join(getVolumePluginDir(specVolID, host), "data")
}

// hasReadWriteOnce 判断 modes 中是否包含 v1.ReadWriteOnce（或 ReadWriteOncePod）。
func hasReadWriteOnce(modes []api.PersistentVolumeAccessMode) bool {
	if modes == nil {
		return false
	}
	for _, mode := range modes {
		if mode == api.ReadWriteOnce ||
			mode == api.ReadWriteOncePod {
			return true
		}
	}
	return false
}

// getSourceFromSpec 从卷规格中返回 CSIVolumeSource 或 CSIPersistentVolumeSource 之一，二者不可同时存在。
func getSourceFromSpec(spec *volume.Spec) (*api.CSIVolumeSource, *api.CSIPersistentVolumeSource, error) {
	if spec == nil {
		return nil, nil, fmt.Errorf("volume.Spec nil")
	}
	if spec.Volume != nil && spec.PersistentVolume != nil {
		return nil, nil, fmt.Errorf("volume.Spec has both volume and persistent volume sources")
	}
	if spec.Volume != nil && spec.Volume.CSI != nil {
		return spec.Volume.CSI, nil, nil
	}
	if spec.PersistentVolume != nil &&
		spec.PersistentVolume.Spec.CSI != nil {
		return nil, spec.PersistentVolume.Spec.CSI, nil
	}

	return nil, nil, fmt.Errorf("volume source not found in volume.Spec")
}

// getPVSourceFromSpec 确保 volume.Spec 中只存在 CSIPersistentVolumeSource。
func getPVSourceFromSpec(spec *volume.Spec) (*api.CSIPersistentVolumeSource, error) {
	volSrc, pvSrc, err := getSourceFromSpec(spec)
	if err != nil {
		return nil, err
	}
	if volSrc != nil {
		return nil, fmt.Errorf("unexpected api.CSIVolumeSource found in volume.Spec")
	}
	return pvSrc, nil
}

// GetCSIMounterPath 根据基础路径返回挂载器路径。
func GetCSIMounterPath(path string) string {
	return filepath.Join(path, "/mount")
}

// GetCSIDriverName 返回 CSI 驱动名称。
func GetCSIDriverName(spec *volume.Spec) (string, error) {
	volSrc, pvSrc, err := getSourceFromSpec(spec)
	if err != nil {
		return "", err
	}

	switch {
	case volSrc != nil:
		return volSrc.Driver, nil
	case pvSrc != nil:
		return pvSrc.Driver, nil
	default:
		return "", errors.New(log("volume source not found in volume.Spec"))
	}
}

// createCSIOperationContext 创建带超时和迁移标记的 CSI 操作上下文。
func createCSIOperationContext(volumeSpec *volume.Spec, timeout time.Duration) (context.Context, context.CancelFunc) {
	migrated := false
	if volumeSpec != nil {
		migrated = volumeSpec.Migrated
	}
	ctx := context.WithValue(context.Background(), additionalInfoKey, additionalInfo{Migrated: strconv.FormatBool(migrated)})
	return context.WithTimeout(ctx, timeout)
}

// getPodInfoAttrs 返回 NodePublish 所需的 Pod 信息属性。
func getPodInfoAttrs(pod *api.Pod, volumeMode storage.VolumeLifecycleMode) map[string]string {
	attrs := map[string]string{
		"csi.storage.k8s.io/pod.name":            pod.Name,
		"csi.storage.k8s.io/pod.namespace":       pod.Namespace,
		"csi.storage.k8s.io/pod.uid":             string(pod.UID),
		"csi.storage.k8s.io/serviceAccount.name": pod.Spec.ServiceAccountName,
		"csi.storage.k8s.io/ephemeral":           strconv.FormatBool(volumeMode == storage.VolumeLifecycleEphemeral),
	}
	return attrs
}
