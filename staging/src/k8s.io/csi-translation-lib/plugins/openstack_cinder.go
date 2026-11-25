/*
Copyright 2019 The Kubernetes Authors.

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
	storage "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

const (
	// CinderDriverName 是 Cinder CSI 驱动的名称。
	CinderDriverName = "cinder.csi.openstack.org"
	// CinderTopologyKey 是 Cinder CSI 驱动表示可用区（zone）的拓扑键。
	CinderTopologyKey = "topology.cinder.csi.openstack.org/zone"
	// CinderInTreePluginName 是 Cinder 树内插件的名称。
	CinderInTreePluginName = "kubernetes.io/cinder"
)

// 编译期断言：确保 osCinderCSITranslator 实现了 InTreePlugin 接口。
var _ InTreePlugin = (*osCinderCSITranslator)(nil)

// osCinderCSITranslator 负责 Cinder 树内 PV 与 CSI PV 之间的双向转换。
type osCinderCSITranslator struct{}

// NewOpenStackCinderCSITranslator 返回一个 Cinder 转换器实例。
func NewOpenStackCinderCSITranslator() InTreePlugin {
	return &osCinderCSITranslator{}
}

// TranslateInTreeStorageClassToCSI 把树内 Cinder StorageClass 参数改写为 CSI 参数，
// 并翻译 AllowedTopologies 中的可用区键。
func (t *osCinderCSITranslator) TranslateInTreeStorageClassToCSI(logger klog.Logger, sc *storage.StorageClass) (*storage.StorageClass, error) {
	// 用新 map 收集翻译后的参数，避免原地修改原参数表。
	var (
		params = map[string]string{}
	)
	// 键名统一转小写比较，其中只有 fstype 需要改名。
	for k, v := range sc.Parameters {
		switch strings.ToLower(k) {
		case fsTypeKey:
			params[csiFsTypeKey] = v
		default:
			// CSI 驱动支持其余参数，因此 availability 等键保持原样，不转换为拓扑选择器。
			params[k] = v
		}
	}

	// 存在拓扑约束时，需要把树内可用区键改成 Cinder CSI 的拓扑键。
	if len(sc.AllowedTopologies) > 0 {
		newTopologies, err := translateAllowedTopologies(sc.AllowedTopologies, CinderTopologyKey)
		// 翻译失败时直接返回错误，避免产生半转换对象。
		if err != nil {
			return nil, fmt.Errorf("failed translating allowed topologies: %v", err)
		}
		sc.AllowedTopologies = newTopologies
	}

	// 用翻译后的参数表整体替换原参数。
	sc.Parameters = params

	return sc, nil
}

// TranslateInTreeInlineVolumeToCSI 把 Cinder 内联卷转换为等价的 CSI PV 定义。
func (t *osCinderCSITranslator) TranslateInTreeInlineVolumeToCSI(logger klog.Logger, volume *v1.Volume, podNamespace string) (*v1.PersistentVolume, error) {
	// 必须提供 Cinder 内联卷来源。
	if volume == nil || volume.Cinder == nil {
		return nil, fmt.Errorf("volume is nil or Cinder not defined on volume")
	}

	cinderSource := volume.Cinder
	// PV 名称由驱动名和卷 ID 拼接，保证每个磁盘唯一。
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			// 该名称是 staging 路径的唯一组成部分。
			Name: fmt.Sprintf("%s-%s", CinderDriverName, cinderSource.VolumeID),
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:           CinderDriverName,
					VolumeHandle:     cinderSource.VolumeID,
					ReadOnly:         cinderSource.ReadOnly,
					FSType:           cinderSource.FSType,
					VolumeAttributes: map[string]string{},
				},
			},
			// 内联卷固定使用 ReadWriteOnce 访问模式。
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
		},
	}
	return pv, nil
}

// TranslateInTreePVToCSI 把树内 Cinder PV 转换为 CSI PV，并同步翻译可用区拓扑。
func (t *osCinderCSITranslator) TranslateInTreePVToCSI(logger klog.Logger, pv *v1.PersistentVolume) (*v1.PersistentVolume, error) {
	// 校验 Cinder 来源存在。
	if pv == nil || pv.Spec.Cinder == nil {
		return nil, fmt.Errorf("pv is nil or Cinder not defined on pv")
	}

	cinderSource := pv.Spec.Cinder

	// 树内 Cinder 字段直接映射到 CSI 来源，卷属性留空。
	csiSource := &v1.CSIPersistentVolumeSource{
		Driver:           CinderDriverName,
		VolumeHandle:     cinderSource.VolumeID,
		ReadOnly:         cinderSource.ReadOnly,
		FSType:           cinderSource.FSType,
		VolumeAttributes: map[string]string{},
	}

	// 迁移兼容：把 PV 的可用区拓扑改成 CSI 拓扑键。
	if err := translateTopologyFromInTreeToCSI(pv, CinderTopologyKey); err != nil {
		return nil, fmt.Errorf("failed to translate topology: %v", err)
	}

	// 清除旧来源并挂上 CSI 来源。
	pv.Spec.Cinder = nil
	pv.Spec.CSI = csiSource
	return pv, nil
}

// TranslateCSIPVToInTree 是反向转换：把 Cinder CSI PV 还原为树内 PV，用于回滚。
func (t *osCinderCSITranslator) TranslateCSIPVToInTree(pv *v1.PersistentVolume) (*v1.PersistentVolume, error) {
	// 没有 CSI 来源时无法反推，直接报错。
	if pv == nil || pv.Spec.CSI == nil {
		return nil, fmt.Errorf("pv is nil or CSI source not defined on pv")
	}

	csiSource := pv.Spec.CSI

	// 从 CSI 来源逐字段回填 Cinder 来源。
	cinderSource := &v1.CinderPersistentVolumeSource{
		VolumeID: csiSource.VolumeHandle,
		FSType:   csiSource.FSType,
		ReadOnly: csiSource.ReadOnly,
	}

	// 无法从 Zone 可靠推导 Cinder Region，因此 Region 保持为空，只转换 Zone 拓扑。
	if err := translateTopologyFromCSIToInTree(pv, CinderTopologyKey, nil); err != nil {
		return nil, fmt.Errorf("failed to translate topology. PV:%+v. Error:%v", *pv, err)
	}

	// 清空 CSI 来源并恢复树内来源。
	pv.Spec.CSI = nil
	pv.Spec.Cinder = cinderSource
	return pv, nil
}

// CanSupport 判断该 PV 是否为 Cinder 树内卷。
// 传入对象的 spec 应视为常量，不得修改。
func (t *osCinderCSITranslator) CanSupport(pv *v1.PersistentVolume) bool {
	return pv != nil && pv.Spec.Cinder != nil
}

// CanSupportInline 判断该内联卷是否为 Cinder 卷。
// 传入对象的 spec 应视为常量，不得修改。
func (t *osCinderCSITranslator) CanSupportInline(volume *v1.Volume) bool {
	return volume != nil && volume.Cinder != nil
}

// GetInTreePluginName 返回 Cinder 树内插件名。
func (t *osCinderCSITranslator) GetInTreePluginName() string {
	return CinderInTreePluginName
}

// GetCSIPluginName 返回 Cinder CSI 驱动名。
func (t *osCinderCSITranslator) GetCSIPluginName() string {
	return CinderDriverName
}

// RepairVolumeHandle 无需修正 Cinder 的卷句柄，原样返回即可。
func (t *osCinderCSITranslator) RepairVolumeHandle(volumeHandle, nodeID string) (string, error) {
	return volumeHandle, nil
}
