/*
Copyright 2020 The Kubernetes Authors.

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
	// VSphereDriverName is the name of the CSI driver for vSphere Volume
	// VSphereDriverName 是 vSphere Volume 对应的 CSI 驱动名称。
	VSphereDriverName = "csi.vsphere.vmware.com"
	// VSphereInTreePluginName is the name of the in-tree plugin for vSphere Volume
	// VSphereInTreePluginName 是 vSphere Volume 树内插件的名称。
	VSphereInTreePluginName = "kubernetes.io/vsphere-volume"

	// vSphereCSITopologyZoneKey is the zonal topology key for vSphere CSI Driver
	// vSphereCSITopologyZoneKey 是 vSphere CSI 驱动表示可用区的拓扑键。
	vSphereCSITopologyZoneKey = "topology.csi.vmware.com/zone"

	// vSphereCSITopologyRegionKey is the region topology key for vSphere CSI Driver
	// vSphereCSITopologyRegionKey 是 vSphere CSI 驱动表示区域的拓扑键。
	vSphereCSITopologyRegionKey = "topology.csi.vmware.com/region"

	// paramStoragePolicyName used to supply SPBM Policy name for Volume provisioning
	// paramStoragePolicyName 是用于传入 SPBM 存储策略名的参数键。
	paramStoragePolicyName = "storagepolicyname"

	// This param is used to tell Driver to return volumePath and not VolumeID
	// in-tree vSphere plugin does not understand volume id, it uses volumePath
	// paramcsiMigration 通知驱动这是迁移请求：返回 volumePath 而非 VolumeID，因为树内插件只认识 volumePath。
	paramcsiMigration = "csimigration"

	// This param is used to supply datastore name for Volume provisioning
	// paramDatastore 是迁移时传入数据存储名称的参数键。
	paramDatastore = "datastore-migrationparam"

	// This param supplies disk foramt (thin, thick, zeoredthick) for Volume provisioning
	// paramDiskFormat 是迁移时传入磁盘格式（thin、thick、eagerzeroedthick）的参数键。
	paramDiskFormat = "diskformat-migrationparam"

	// vSAN Policy Parameters
	// 以下键对应 vSAN 策略参数（容错数、强制置备、缓存预留、条带数、空间预留、IOPS 上限），均带 -migrationparam 后缀。
	paramHostFailuresToTolerate = "hostfailurestotolerate-migrationparam"
	paramForceProvisioning      = "forceprovisioning-migrationparam"
	paramCacheReservation       = "cachereservation-migrationparam"
	paramDiskstripes            = "diskstripes-migrationparam"
	paramObjectspacereservation = "objectspacereservation-migrationparam"
	paramIopslimit              = "iopslimit-migrationparam"

	// AttributeInitialVolumeFilepath represents the path of volume where volume is created
	// AttributeInitialVolumeFilepath 是卷属性中记录初始卷文件路径的键，反向转换时用它还原 VolumePath。
	AttributeInitialVolumeFilepath = "initialvolumefilepath"
)

// 编译期断言：确保 vSphereCSITranslator 实现了 InTreePlugin 接口。
var _ InTreePlugin = &vSphereCSITranslator{}

// vSphereCSITranslator handles translation of PV spec from In-tree vSphere Volume to vSphere CSI
// vSphereCSITranslator 负责 vSphere Volume 树内 PV 与 CSI PV 之间的双向转换。
type vSphereCSITranslator struct{}

// NewvSphereCSITranslator returns a new instance of vSphereCSITranslator
// NewvSphereCSITranslator 返回一个 vSphere Volume 转换器实例。
func NewvSphereCSITranslator() InTreePlugin {
	return &vSphereCSITranslator{}
}

// TranslateInTreeStorageClassToCSI translates InTree vSphere storage class parameters to CSI storage class
// TranslateInTreeStorageClassToCSI 把树内 vSphere StorageClass 参数改写为 CSI 迁移参数，
// 补充迁移标志并翻译 AllowedTopologies 中的拓扑键。
func (t *vSphereCSITranslator) TranslateInTreeStorageClassToCSI(logger klog.Logger, sc *storage.StorageClass) (*storage.StorageClass, error) {
	// 输入为空时无法翻译。
	if sc == nil {
		return nil, fmt.Errorf("sc is nil")
	}
	// 新参数表只保留受支持的键。
	var params = map[string]string{}
	// 键名统一按小写匹配。
	for k, v := range sc.Parameters {
		switch strings.ToLower(k) {
		// 逐项把树内参数名改写为带 -migrationparam 后缀的 CSI 迁移参数名。
		case fsTypeKey:
			params[csiFsTypeKey] = v
		case paramStoragePolicyName:
			params[paramStoragePolicyName] = v
		case "datastore":
			params[paramDatastore] = v
		case "diskformat":
			params[paramDiskFormat] = v
		case "hostfailurestotolerate":
			params[paramHostFailuresToTolerate] = v
		case "forceprovisioning":
			params[paramForceProvisioning] = v
		case "cachereservation":
			params[paramCacheReservation] = v
		case "diskstripes":
			params[paramDiskstripes] = v
		case "objectspacereservation":
			params[paramObjectspacereservation] = v
		case "iopslimit":
			params[paramIopslimit] = v
		default:
			// 不支持的参数只记录日志，不写入结果。
			logger.V(2).Info("StorageClass parameter is not supported", "name", k, "value", v)
		}
	}

	// This helps vSphere CSI driver to identify in-tree provisioner request vs CSI provisioner request
	// When this is true, Driver returns initialvolumefilepath in the VolumeContext, which is
	// used in TranslateCSIPVToInTree
	// 该标志让驱动返回 initialvolumefilepath，供反向转换还原树内卷路径。
	params[paramcsiMigration] = "true"
	// translate AllowedTopologies to vSphere CSI Driver topology
	// 把 AllowedTopologies 中的 zone 键替换为 vSphere CSI 拓扑键。
	if len(sc.AllowedTopologies) > 0 {
		newTopologies, err := translateAllowedTopologies(sc.AllowedTopologies, vSphereCSITopologyZoneKey)
		if err != nil {
			return nil, fmt.Errorf("failed translating allowed topologies: %v", err)
		}
		sc.AllowedTopologies = newTopologies
	}
	// 用新参数表整体替换原参数。
	sc.Parameters = params
	return sc, nil
}

// TranslateInTreeInlineVolumeToCSI takes a Volume with VsphereVolume set from in-tree
// and converts the VsphereVolume source to a CSIPersistentVolumeSource
// TranslateInTreeInlineVolumeToCSI 把 vSphere 内联卷转换为 CSI PV，卷路径作为 VolumeHandle。
func (t *vSphereCSITranslator) TranslateInTreeInlineVolumeToCSI(logger klog.Logger, volume *v1.Volume, podNamespace string) (*v1.PersistentVolume, error) {
	// 必须提供 VsphereVolume 来源。
	if volume == nil || volume.VsphereVolume == nil {
		return nil, fmt.Errorf("volume is nil or VsphereVolume not defined on volume")
	}
	// PV 名称由驱动名与卷路径拼接，保证每个磁盘唯一。
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			// Must be unique per disk as it is used as the unique part of the
			// staging path
			Name: fmt.Sprintf("%s-%s", VSphereDriverName, volume.VsphereVolume.VolumePath),
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:           VSphereDriverName,
					VolumeHandle:     volume.VsphereVolume.VolumePath,
					FSType:           volume.VsphereVolume.FSType,
					VolumeAttributes: make(map[string]string),
				},
			},
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
		},
	}
	// 存储策略名非空时，尝试把它作为卷属性传给驱动。
	if volume.VsphereVolume.StoragePolicyName != "" {
		pv.Spec.CSI.VolumeAttributes[paramStoragePolicyName] = pv.Spec.VsphereVolume.StoragePolicyName
	}
	return pv, nil
}

// TranslateInTreePVToCSI takes a PV with VsphereVolume set from in-tree
// and converts the VsphereVolume source to a CSIPersistentVolumeSource
// TranslateInTreePVToCSI 把树内 vSphere PV 转换为 CSI PV，并翻译 zone/region 拓扑。
func (t *vSphereCSITranslator) TranslateInTreePVToCSI(logger klog.Logger, pv *v1.PersistentVolume) (*v1.PersistentVolume, error) {
	// 校验 VsphereVolume 来源存在。
	if pv == nil || pv.Spec.VsphereVolume == nil {
		return nil, fmt.Errorf("pv is nil or VsphereVolume not defined on pv")
	}
	// VolumePath 直接作为 CSI 的 VolumeHandle。
	csiSource := &v1.CSIPersistentVolumeSource{
		Driver:           VSphereDriverName,
		VolumeHandle:     pv.Spec.VsphereVolume.VolumePath,
		FSType:           pv.Spec.VsphereVolume.FSType,
		VolumeAttributes: make(map[string]string),
	}
	// 存储策略名写入卷属性，供驱动置备时参考。
	if pv.Spec.VsphereVolume.StoragePolicyName != "" {
		csiSource.VolumeAttributes[paramStoragePolicyName] = pv.Spec.VsphereVolume.StoragePolicyName
	}
	// translate in-tree topology to CSI topology for migration
	// 迁移兼容：把树内 zone/region 标签改写为 CSI 拓扑。
	if err := translateTopologyFromInTreevSphereToCSI(pv, vSphereCSITopologyZoneKey, vSphereCSITopologyRegionKey); err != nil {
		return nil, fmt.Errorf("failed to translate topology: %v", err)
	}
	// 清除旧来源并挂上 CSI 来源。
	pv.Spec.VsphereVolume = nil
	pv.Spec.CSI = csiSource
	return pv, nil
}

// TranslateCSIPVToInTree takes a PV with CSIPersistentVolumeSource set and
// translates the vSphere CSI source to a vSphereVolume source.
// TranslateCSIPVToInTree 是反向转换：把 vSphere CSI PV 还原为树内 VsphereVolume PV，用于回滚。
func (t *vSphereCSITranslator) TranslateCSIPVToInTree(pv *v1.PersistentVolume) (*v1.PersistentVolume, error) {
	// 没有 CSI 来源时无法反推。
	if pv == nil || pv.Spec.CSI == nil {
		return nil, fmt.Errorf("pv is nil or CSI source not defined on pv")
	}
	csiSource := pv.Spec.CSI
	// 先按 FSType 构造树内来源。
	vsphereVirtualDiskVolumeSource := &v1.VsphereVirtualDiskVolumeSource{
		FSType: csiSource.FSType,
	}
	// 卷路径不在 VolumeHandle 中，而由驱动放在 initialvolumefilepath 属性里。
	volumeFilePath, ok := csiSource.VolumeAttributes[AttributeInitialVolumeFilepath]
	if ok {
		vsphereVirtualDiskVolumeSource.VolumePath = volumeFilePath
	}
	// translate CSI topology to In-tree topology for rollback compatibility.
	// 回滚兼容：把 CSI 拓扑还原为树内 zone/region 标签与亲和性规则。
	if err := translateTopologyFromCSIToInTreevSphere(pv, vSphereCSITopologyZoneKey, vSphereCSITopologyRegionKey); err != nil {
		return nil, fmt.Errorf("failed to translate topology. PV:%+v. Error:%v", *pv, err)
	}
	// 恢复树内来源。
	pv.Spec.CSI = nil
	pv.Spec.VsphereVolume = vsphereVirtualDiskVolumeSource
	return pv, nil
}

// CanSupport tests whether the plugin supports a given persistent volume
// specification from the API.
// CanSupport 判断该 PV 是否为 vSphere 树内卷。
func (t *vSphereCSITranslator) CanSupport(pv *v1.PersistentVolume) bool {
	return pv != nil && pv.Spec.VsphereVolume != nil
}

// CanSupportInline tests whether the plugin supports a given inline volume
// specification from the API.
// CanSupportInline 判断该内联卷是否为 vSphere 卷。
func (t *vSphereCSITranslator) CanSupportInline(volume *v1.Volume) bool {
	return volume != nil && volume.VsphereVolume != nil
}

// GetInTreePluginName returns the name of the in-tree plugin driver
// GetInTreePluginName 返回 vSphere Volume 树内插件名。
func (t *vSphereCSITranslator) GetInTreePluginName() string {
	return VSphereInTreePluginName
}

// GetCSIPluginName returns the name of the CSI plugin
// GetCSIPluginName 返回 vSphere CSI 驱动名。
func (t *vSphereCSITranslator) GetCSIPluginName() string {
	return VSphereDriverName
}

// RepairVolumeHandle is needed in VerifyVolumesAttached on the external attacher when we need to do strict volume
// handle matching to check VolumeAttachment attached status.
// vSphere volume does not need patch to help verify whether that volume is attached.
// RepairVolumeHandle 无需修补 vSphere 的卷句柄，原样返回即可。
func (t *vSphereCSITranslator) RepairVolumeHandle(volumeHandle, nodeID string) (string, error) {
	return volumeHandle, nil
}

// translateTopologyFromInTreevSphereToCSI converts existing zone labels or in-tree vsphere topology to
// vSphere CSI topology.
// translateTopologyFromInTreevSphereToCSI 把 PV 上已有的 zone/region 树内拓扑改写为 vSphere CSI 的拓扑键。
func translateTopologyFromInTreevSphereToCSI(pv *v1.PersistentVolume, csiTopologyKeyZone string, csiTopologyKeyRegion string) error {
	// 先确定当前使用的 zone/region 标签键（GA 或 Beta）。
	zoneLabel, regionLabel := getTopologyLabel(pv)

	// If Zone kubernetes topology exist, replace it to use csiTopologyKeyZone
	// 读取 NodeAffinity 中 zone 表达式的取值。
	zones := getTopologyValues(pv, zoneLabel)
	if len(zones) > 0 {
		// NodeAffinity 里已有 zone 值时，直接替换键名。
		replaceTopology(pv, zoneLabel, csiTopologyKeyZone)
	} else {
		// if nothing is in the NodeAffinity, try to fetch the topology from PV labels
		// NodeAffinity 中没有值时，退回从 PV 标签读取。
		if label, ok := pv.Labels[zoneLabel]; ok {
			if len(label) > 0 {
				addTopology(pv, csiTopologyKeyZone, []string{label})
			}
		}
	}

	// If region kubernetes topology exist, replace it to use csiTopologyKeyRegion
	// region 的处理与 zone 相同。
	regions := getTopologyValues(pv, regionLabel)
	if len(regions) > 0 {
		// 把 region 键替换为 CSI 的 region 拓扑键。
		replaceTopology(pv, regionLabel, csiTopologyKeyRegion)
	} else {
		// if nothing is in the NodeAffinity, try to fetch the topology from PV labels
		// region 在 NodeAffinity 中没有值时，同样退回 PV 标签。
		if label, ok := pv.Labels[regionLabel]; ok {
			if len(label) > 0 {
				addTopology(pv, csiTopologyKeyRegion, []string{label})
			}
		}
	}
	return nil
}

// translateTopologyFromCSIToInTreevSphere converts CSI zone/region affinity rules to in-tree vSphere zone/region labels
// translateTopologyFromCSIToInTreevSphere 把 CSI 的 zone/region 亲和性规则还原为树内标签，供回滚使用。
func translateTopologyFromCSIToInTreevSphere(pv *v1.PersistentVolume,
	csiTopologyKeyZone string, csiTopologyKeyRegion string) error {
	// 先确定树内目标标签键。
	zoneLabel, regionLabel := getTopologyLabel(pv)

	// Replace all CSI topology to Kubernetes Zone label
	// 把 CSI 的 zone 键替换回树内 zone 标签。
	err := replaceTopology(pv, csiTopologyKeyZone, zoneLabel)
	if err != nil {
		return fmt.Errorf("failed to replace CSI topology to Kubernetes topology, error: %v", err)
	}

	// Replace all CSI topology to Kubernetes Region label
	// 再处理 region。
	err = replaceTopology(pv, csiTopologyKeyRegion, regionLabel)
	if err != nil {
		return fmt.Errorf("failed to replace CSI topology to Kubernetes topology, error: %v", err)
	}

	// 若 PV 标签缺失，从 NodeAffinity 中提取首个 zone 值补齐。
	zoneVals := getTopologyValues(pv, zoneLabel)
	if len(zoneVals) > 0 {
		if pv.Labels == nil {
			pv.Labels = make(map[string]string)
		}
		_, zoneOK := pv.Labels[zoneLabel]
		if !zoneOK {
			pv.Labels[zoneLabel] = zoneVals[0]
		}
	}
	// region 同理，补上 region 标签。
	regionVals := getTopologyValues(pv, regionLabel)
	if len(regionVals) > 0 {
		if pv.Labels == nil {
			pv.Labels = make(map[string]string)
		}
		_, regionOK := pv.Labels[regionLabel]
		if !regionOK {
			pv.Labels[regionLabel] = regionVals[0]
		}
	}
	return nil
}
