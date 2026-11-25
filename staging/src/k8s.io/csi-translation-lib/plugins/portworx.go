/*
Copyright 2021 The Kubernetes Authors.

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

package plugins

import (
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

// 以下常量定义 Portworx 的插件名、驱动名，以及认证 Secret 在树内与 CSI 两侧的键名。
const (
	// PortworxVolumePluginName 是树内插件名，PortworxDriverName 是取代它的 CSI 驱动名。
	PortworxVolumePluginName = "kubernetes.io/portworx-volume"
	PortworxDriverName       = "pxd.portworx.com"

	// OpenStorageAuthSecretNameKey 和其命名空间伙伴是 PV 上指定认证 Secret 的注解键。
	OpenStorageAuthSecretNameKey      = "openstorage.io/auth-secret-name"
	OpenStorageAuthSecretNamespaceKey = "openstorage.io/auth-secret-namespace"

	// csiParameterPrefix 是 CSI 外部组件（provisioner/attach 等）识别的参数键前缀。
	csiParameterPrefix = "csi.storage.k8s.io/"

	// prefixedProvisionerSecretNameKey 及其命名空间伙伴对应 CSI 置备阶段的 Secret 参数键。
	prefixedProvisionerSecretNameKey      = csiParameterPrefix + "provisioner-secret-name"
	prefixedProvisionerSecretNamespaceKey = csiParameterPrefix + "provisioner-secret-namespace"

	// prefixedControllerPublishSecretNameKey 及其命名空间伙伴对应 ControllerPublish 阶段的 Secret 参数键。
	prefixedControllerPublishSecretNameKey      = csiParameterPrefix + "controller-publish-secret-name"
	prefixedControllerPublishSecretNamespaceKey = csiParameterPrefix + "controller-publish-secret-namespace"

	// prefixedNodeStageSecretNameKey 及其命名空间伙伴对应 NodeStage 阶段的 Secret 参数键。
	prefixedNodeStageSecretNameKey      = csiParameterPrefix + "node-stage-secret-name"
	prefixedNodeStageSecretNamespaceKey = csiParameterPrefix + "node-stage-secret-namespace"

	// prefixedNodePublishSecretNameKey 及其命名空间伙伴对应 NodePublish 阶段的 Secret 参数键。
	prefixedNodePublishSecretNameKey      = csiParameterPrefix + "node-publish-secret-name"
	prefixedNodePublishSecretNamespaceKey = csiParameterPrefix + "node-publish-secret-namespace"

	// prefixedControllerExpandSecretNameKey 及其命名空间伙伴对应 ControllerExpand 阶段的 Secret 参数键。
	prefixedControllerExpandSecretNameKey      = csiParameterPrefix + "controller-expand-secret-name"
	prefixedControllerExpandSecretNamespaceKey = csiParameterPrefix + "controller-expand-secret-namespace"

	// prefixedNodeExpandSecretNameKey 及其命名空间伙伴对应 NodeExpand 阶段的 Secret 参数键。
	prefixedNodeExpandSecretNameKey      = csiParameterPrefix + "node-expand-secret-name"
	prefixedNodeExpandSecretNamespaceKey = csiParameterPrefix + "node-expand-secret-namespace"
)

// 编译期断言：确保 portworxCSITranslator 实现了 InTreePlugin 接口。
var _ InTreePlugin = &portworxCSITranslator{}

// portworxCSITranslator 承载 Portworx 插件从树内到 CSI 的转换实现。
type portworxCSITranslator struct{}

// NewPortworxCSITranslator 返回一个 Portworx CSI 转换器实例。
func NewPortworxCSITranslator() InTreePlugin {
	return &portworxCSITranslator{}
}

// TranslateInTreeStorageClassToCSI 将树内 StorageClass 的认证 Secret 参数展开为 CSI 各阶段所需的带前缀参数，
// 并把 provisioner 替换为 Portworx CSI 驱动名。
func (p portworxCSITranslator) TranslateInTreeStorageClassToCSI(logger klog.Logger, sc *storagev1.StorageClass) (*storagev1.StorageClass, error) {
	// 输入为空时直接报错，避免后续解引用。
	if sc == nil {
		return nil, fmt.Errorf("sc is nil")
	}

	// 用新 map 收集翻译结果，保持原参数表不被就地改写。
	var params = map[string]string{}
	// 键名按小写比较，以兼容大小写不规范的树内参数。
	for k, v := range sc.Parameters {
		switch strings.ToLower(k) {
		case OpenStorageAuthSecretNameKey:
			// 一个树内认证 Secret 名称需要扇出到 CSI 的全部操作阶段。
			params[prefixedProvisionerSecretNameKey] = v
			params[prefixedControllerPublishSecretNameKey] = v
			params[prefixedNodePublishSecretNameKey] = v
			params[prefixedNodeStageSecretNameKey] = v
			params[prefixedControllerExpandSecretNameKey] = v
			params[prefixedNodeExpandSecretNameKey] = v
		case OpenStorageAuthSecretNamespaceKey:
			// Secret 命名空间同样要扇出到全部操作阶段。
			params[prefixedProvisionerSecretNamespaceKey] = v
			params[prefixedControllerPublishSecretNamespaceKey] = v
			params[prefixedNodePublishSecretNamespaceKey] = v
			params[prefixedNodeStageSecretNamespaceKey] = v
			params[prefixedControllerExpandSecretNamespaceKey] = v
			params[prefixedNodeExpandSecretNamespaceKey] = v
		default:
			// 其余参数原样保留，交由 CSI 驱动处理。
			params[k] = v
		}
	}
	// 仅当翻译结果非空时才写回，避免覆盖原参数。
	if len(params) > 0 {
		sc.Parameters = params
	}
	// 替换 provisioner 是 CSI 迁移生效的关键标志。
	sc.Provisioner = PortworxDriverName

	return sc, nil
}

// TranslateInTreeInlineVolumeToCSI 把 Pod 中的 Portworx 内联卷转换为等价的 CSI PV 定义。
func (p portworxCSITranslator) TranslateInTreeInlineVolumeToCSI(logger klog.Logger, volume *v1.Volume, podNamespace string) (*v1.PersistentVolume, error) {
	// Volume 与 PortworxVolume 来源必须同时存在。
	if volume == nil || volume.PortworxVolume == nil {
		return nil, fmt.Errorf("volume is nil or PortworxVolume not defined on volume")
	}

	// ReadOnly 决定访问模式：只读用 ReadOnlyMany，否则用 ReadWriteOnce。
	var am v1.PersistentVolumeAccessMode
	if volume.PortworxVolume.ReadOnly {
		am = v1.ReadOnlyMany
	} else {
		am = v1.ReadWriteOnce
	}

	// PV 名称由驱动名与卷 ID 拼接，保证每个磁盘唯一（staging 路径依赖它）。
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("%s-%s", PortworxDriverName, volume.PortworxVolume.VolumeID),
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				// Portworx 不需要额外卷属性，CSI 来源中的 VolumeAttributes 保持为空。
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:           PortworxDriverName,
					VolumeHandle:     volume.PortworxVolume.VolumeID,
					FSType:           volume.PortworxVolume.FSType,
					VolumeAttributes: make(map[string]string),
				},
			},
			AccessModes: []v1.PersistentVolumeAccessMode{am},
		},
	}
	return pv, nil
}

// TranslateInTreePVToCSI 把树内 Portworx PV 就地转换为 CSI PV，
// 并将两处认证注解合并为各 CSI 阶段共享的 Secret 引用。
func (p portworxCSITranslator) TranslateInTreePVToCSI(logger klog.Logger, pv *v1.PersistentVolume) (*v1.PersistentVolume, error) {
	// 校验 PV 及其 Portworx 来源必须存在。
	if pv == nil || pv.Spec.PortworxVolume == nil {
		return nil, fmt.Errorf("pv is nil or PortworxVolume not defined on pv")
	}
	// 先置空；只有注解齐全时才构造 Secret 引用。
	var secretRef *v1.SecretReference

	// 名称和命名空间注解必须成对出现，单凭其一无法定位 Secret。
	if metav1.HasAnnotation(pv.ObjectMeta, OpenStorageAuthSecretNameKey) &&
		metav1.HasAnnotation(pv.ObjectMeta, OpenStorageAuthSecretNamespaceKey) {
		secretRef = &v1.SecretReference{
			Name:      pv.Annotations[OpenStorageAuthSecretNameKey],
			Namespace: pv.Annotations[OpenStorageAuthSecretNamespaceKey],
		}
	}

	// 同一个 Secret 引用被复用于所有 CSI 操作阶段。
	// VolumeAttributes 保持为空；访问模式仍在 PV Spec 的 AccessModes 中。
	csiSource := &v1.CSIPersistentVolumeSource{
		Driver:                     PortworxDriverName,
		VolumeHandle:               pv.Spec.PortworxVolume.VolumeID,
		FSType:                     pv.Spec.PortworxVolume.FSType,
		VolumeAttributes:           make(map[string]string),
		ControllerPublishSecretRef: secretRef,
		NodeStageSecretRef:         secretRef,
		NodePublishSecretRef:       secretRef,
		ControllerExpandSecretRef:  secretRef,
		NodeExpandSecretRef:        secretRef,
	}
	// 清除旧来源并挂上 CSI 来源，完成 PV 的原地转换。
	pv.Spec.PortworxVolume = nil
	pv.Spec.CSI = csiSource

	return pv, nil
}

// TranslateCSIPVToInTree 是反向转换：把 CSI PV 还原为树内 Portworx PV，用于回滚场景。
func (p portworxCSITranslator) TranslateCSIPVToInTree(pv *v1.PersistentVolume) (*v1.PersistentVolume, error) {
	// 没有 CSI 来源时无法反推，直接报错。
	if pv == nil || pv.Spec.CSI == nil {
		return nil, fmt.Errorf("pv is nil or CSI source not defined on pv")
	}
	csiSource := pv.Spec.CSI

	// VolumeHandle、FSType、ReadOnly 从 CSI 来源逐字段回填。
	portworxSource := &v1.PortworxVolumeSource{
		VolumeID: csiSource.VolumeHandle,
		FSType:   csiSource.FSType,
		ReadOnly: csiSource.ReadOnly,
	}
	// 清空 CSI 来源并恢复树内来源。
	pv.Spec.CSI = nil
	pv.Spec.PortworxVolume = portworxSource

	return pv, nil
}

// CanSupport 判断该 PV 是否为 Portworx 树内卷。
// 传入对象的 spec 应视为常量，不得修改。
func (p portworxCSITranslator) CanSupport(pv *v1.PersistentVolume) bool {
	return pv != nil && pv.Spec.PortworxVolume != nil
}

// CanSupportInline 判断该内联卷是否为 Portworx 卷。
// 传入对象的 spec 应视为常量，不得修改。
func (p portworxCSITranslator) CanSupportInline(volume *v1.Volume) bool {
	return volume != nil && volume.PortworxVolume != nil
}

// GetInTreePluginName 返回本转换器所迁移的树内插件名。
func (p portworxCSITranslator) GetInTreePluginName() string {
	return PortworxVolumePluginName
}

// GetCSIPluginName 返回取代树内插件的 CSI 驱动名。
func (p portworxCSITranslator) GetCSIPluginName() string {
	return PortworxDriverName
}

// RepairVolumeHandle 无需修正 Portworx 的卷句柄，原样返回即可。
func (p portworxCSITranslator) RepairVolumeHandle(volumeHandle, nodeID string) (string, error) {
	return volumeHandle, nil
}
