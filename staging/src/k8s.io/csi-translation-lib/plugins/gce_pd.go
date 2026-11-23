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
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
)

const (
	// GCEPDDriverName is the name of the CSI driver for GCE PD
	// GCEPDDriverName 是 GCE PD 对应的 CSI 驱动名称。
	GCEPDDriverName = "pd.csi.storage.gke.io"
	// GCEPDInTreePluginName is the name of the intree plugin for GCE PD
	// GCEPDInTreePluginName 是树内（in-tree）GCE PD 插件的名称。
	GCEPDInTreePluginName = "kubernetes.io/gce-pd"

	// GCEPDTopologyKey is the zonal topology key for GCE PD CSI Driver
	// GCEPDTopologyKey 是 CSI 驱动使用的可用区拓扑标签键。
	GCEPDTopologyKey = "topology.gke.io/zone"

	// Volume ID Expected Format
	// "projects/{projectName}/zones/{zoneName}/disks/{diskName}"
	// volIDZonalFmt 是区域级（zonal）磁盘的 volume handle 格式：项目、可用区、磁盘名。
	volIDZonalFmt = "projects/%s/zones/%s/disks/%s"
	// "projects/{projectName}/regions/{regionName}/disks/{diskName}"
	// 下面一组常量描述 region 级 handle 格式以及按 "/" 拆分后各字段的下标：
	// volIDRegionalFmt 是地区级（regional）磁盘的 handle 格式；
	// volIDProjectValue 等项目在 RepairVolumeHandle 中用于定位并修补 UNSPECIFIED 字段；
	// volIDTotalElements 是合法 handle 至少应包含的分段数。
	volIDRegionalFmt      = "projects/%s/regions/%s/disks/%s"
	volIDProjectValue     = 1
	volIDRegionalityValue = 2
	volIDZoneValue        = 3
	volIDDiskNameValue    = 5
	volIDTotalElements    = 6

	// nodeIDFmt 是节点 ID 的格式，修补 handle 时从中提取项目与可用区。
	nodeIDFmt = "projects/%s/zones/%s/instances/%s"

	// UnspecifiedValue is used for an unknown zone string
	// UnspecifiedValue 表示 handle 中尚未确定的项目/可用区，需要后续从节点信息推断。
	UnspecifiedValue = "UNSPECIFIED"
)

// 编译期断言：确保 gcePersistentDiskCSITranslator 实现 InTreePlugin 接口。
var _ InTreePlugin = &gcePersistentDiskCSITranslator{}

// gcePersistentDiskCSITranslator handles translation of PV spec from In-tree
// GCE PD to CSI GCE PD and vice versa
// gcePersistentDiskCSITranslator 负责树内 GCE PD 与 CSI GCE PD 之间 PV 配置的双向翻译。
type gcePersistentDiskCSITranslator struct{}

// NewGCEPersistentDiskCSITranslator returns a new instance of gcePersistentDiskTranslator
// NewGCEPersistentDiskCSITranslator 返回一个 GCE PD 翻译器实例。
func NewGCEPersistentDiskCSITranslator() InTreePlugin {
	return &gcePersistentDiskCSITranslator{}
}

// generateToplogySelectors 用给定的键和取值集合构造一个拓扑选择器，供 AllowedTopologies 使用。
func generateToplogySelectors(key string, values []string) []v1.TopologySelectorTerm {
	return []v1.TopologySelectorTerm{
		{
			MatchLabelExpressions: []v1.TopologySelectorLabelRequirement{
				{
					// 键值即为 CSI 驱动识别的拓扑标签键与对应取值列表。
					Key:    key,
					Values: values,
				},
			},
		},
	}
}

// TranslateInTreeStorageClassToCSI translates InTree GCE storage class parameters to CSI storage class
// TranslateInTreeStorageClassToCSI 把树内 StorageClass 参数翻译为 CSI 参数：重命名 fstype，并把 zone/zones 转成拓扑约束。
func (g *gcePersistentDiskCSITranslator) TranslateInTreeStorageClassToCSI(logger klog.Logger, sc *storage.StorageClass) (*storage.StorageClass, error) {
	var generatedTopologies []v1.TopologySelectorTerm

	// 用新 map 收集参数，避免直接修改原 map 时混入已废弃的键。
	np := map[string]string{}
	for k, v := range sc.Parameters {
		// 参数名大小写不敏感，统一转小写后匹配。
		switch strings.ToLower(k) {
		case fsTypeKey:
			// prefixed fstype parameter is stripped out by external provisioner
			// 旧键 fstype 被外部 provisioner 剥离，需改写成 CSI 识别的完整键名。
			np[csiFsTypeKey] = v
		// Strip out zone and zones parameters and translate them into topologies instead
		// zone/zones 不再作为参数传递，而是转换成 allowedTopologies。
		case zoneKey:
			generatedTopologies = generateToplogySelectors(GCEPDTopologyKey, []string{v})
		case zonesKey:
			// zones 是用逗号分隔的多个可用区。
			generatedTopologies = generateToplogySelectors(GCEPDTopologyKey, strings.Split(v, ","))
		default:
			// 其余参数原样保留。
			np[k] = v
		}
	}

	// zone/zones 与 allowedTopologies 语义冲突，不能同时指定。
	if len(generatedTopologies) > 0 && len(sc.AllowedTopologies) > 0 {
		return nil, fmt.Errorf("cannot simultaneously set allowed topologies and zone/zones parameters")
	} else if len(generatedTopologies) > 0 {
		// 由 zone/zones 生成的拓扑直接作为新的 allowedTopologies。
		sc.AllowedTopologies = generatedTopologies
	} else if len(sc.AllowedTopologies) > 0 {
		// 已有的拓扑也要把旧标签键翻译成 CSI 的拓扑键。
		newTopologies, err := translateAllowedTopologies(sc.AllowedTopologies, GCEPDTopologyKey)
		if err != nil {
			return nil, fmt.Errorf("failed translating allowed topologies: %v", err)
		}
		sc.AllowedTopologies = newTopologies
	}

	// 用清理后的参数集替换原参数。
	sc.Parameters = np

	return sc, nil
}

// backwardCompatibleAccessModes translates all instances of ReadWriteMany
// access mode from the in-tree plugin to ReadWriteOnce. This is because in-tree
// plugin never supported ReadWriteMany but also did not validate or enforce
// this access mode for pre-provisioned volumes. The GCE PD CSI Driver validates
// and enforces (fails) ReadWriteMany. Therefore we treat all in-tree
// ReadWriteMany as ReadWriteOnce volumes to not break legacy volumes. It also
// takes [ReadWriteOnce, ReadOnlyMany] and makes it ReadWriteOnce. This is
// because the in-tree plugin does not enforce access modes and just attaches
// the disk in ReadWriteOnce mode; however, the CSI external-attacher will fail
// this combination because technically [ReadWriteOnce, ReadOnlyMany] is not
// supportable on an attached volume
// See: https://github.com/kubernetes-csi/external-attacher/issues/153
// backwardCompatibleAccessModes 将树内遗留的访问模式规整为 CSI 能接受的形式：RWX 当作 RWO，[RWO, ROX] 也折叠为 RWO。
func backwardCompatibleAccessModes(ams []v1.PersistentVolumeAccessMode) []v1.PersistentVolumeAccessMode {
	if ams == nil {
		return nil
	}

	// 用 set 去重，避免重复模式影响后续判断。
	s := map[v1.PersistentVolumeAccessMode]bool{}
	var newAM []v1.PersistentVolumeAccessMode

	for _, am := range ams {
		if am == v1.ReadWriteMany {
			// ReadWriteMany is unsupported in CSI, but in-tree did no
			// validation and treated it as ReadWriteOnce
			// 树内插件从未真正支持 RWX，这里等价替换为 RWO 以保持旧卷可用。
			s[v1.ReadWriteOnce] = true
		} else {
			s[am] = true
		}
	}

	// 按优先级选出一个 CSI 可接受的访问模式。
	switch {
	case s[v1.ReadOnlyMany] && s[v1.ReadWriteOnce]:
		// ROX,RWO is unsupported in CSI, but in-tree did not validation and
		// treated it as ReadWriteOnce
		// ROX+RWO 组合在 CSI external-attacher 中会失败，统一降级为 RWO。
		newAM = []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}
	case s[v1.ReadWriteOnce]:
		newAM = []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}
	case s[v1.ReadOnlyMany]:
		newAM = []v1.PersistentVolumeAccessMode{v1.ReadOnlyMany}
	default:
		// 没有可识别模式时给出最保守的 RWO。
		newAM = []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}
	}

	return newAM
}

// TranslateInTreeInlineVolumeToCSI takes a Volume with GCEPersistentDisk set from in-tree
// and converts the GCEPersistentDisk source to a CSIPersistentVolumeSource
// TranslateInTreeInlineVolumeToCSI 把 Pod 中内联的 GCE PD 卷转换成内存中的 CSI PV。
func (g *gcePersistentDiskCSITranslator) TranslateInTreeInlineVolumeToCSI(logger klog.Logger, volume *v1.Volume, podNamespace string) (*v1.PersistentVolume, error) {
	// 内联卷必须带有 GCEPersistentDisk 来源。
	if volume == nil || volume.GCEPersistentDisk == nil {
		return nil, fmt.Errorf("volume is nil or GCE PD not defined on volume")
	}

	pdSource := volume.GCEPersistentDisk

	// 分区是数值类型，而 CSI 用字符串属性传递；0 表示整块磁盘，传空串。
	partition := ""
	if pdSource.Partition != 0 {
		partition = strconv.Itoa(int(pdSource.Partition))
	}

	// 树内内联卷的只读标志直接映射为相应的访问模式。
	var am v1.PersistentVolumeAccessMode
	if pdSource.ReadOnly {
		am = v1.ReadOnlyMany
	} else {
		am = v1.ReadWriteOnce
	}

	// 内联卷固定使用 Filesystem 卷模式。
	fsMode := v1.PersistentVolumeFilesystem
	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			// Must be unique per disk as it is used as the unique part of the
			// staging path
			// 用驱动名加磁盘名保证同名卷的 staging 路径稳定且全局唯一。
			Name: fmt.Sprintf("%s-%s", GCEPDDriverName, pdSource.PDName),
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					// 内联卷没有项目和可用区信息，全部填 UNSPECIFIED，由后续 RepairVolumeHandle 补全。
					Driver:       GCEPDDriverName,
					VolumeHandle: fmt.Sprintf(volIDZonalFmt, UnspecifiedValue, UnspecifiedValue, pdSource.PDName),
					ReadOnly:     pdSource.ReadOnly,
					FSType:       pdSource.FSType,
					// 分区信息通过 VolumeAttributes 传给 CSI 驱动。
					VolumeAttributes: map[string]string{
						"partition": partition,
					},
				},
			},
			// 访问模式和卷模式从内联来源推导而来。
			AccessModes: []v1.PersistentVolumeAccessMode{am},
			VolumeMode:  &fsMode,
		},
	}, nil
}

// TranslateInTreePVToCSI takes a PV with GCEPersistentDisk set from in-tree
// and converts the GCEPersistentDisk source to a CSIPersistentVolumeSource
// TranslateInTreePVToCSI 把树内 GCE PD PV 原地改造成 CSI PV，并按拓扑标签生成 volume handle。
func (g *gcePersistentDiskCSITranslator) TranslateInTreePVToCSI(logger klog.Logger, pv *v1.PersistentVolume) (*v1.PersistentVolume, error) {
	var volID string

	if pv == nil || pv.Spec.GCEPersistentDisk == nil {
		return nil, fmt.Errorf("pv is nil or GCE Persistent Disk source not defined on pv")
	}

	// depend on which version it migrates from, the label could be failuredomain beta or topology GA version
	// 旧版本用 beta 标签，新版本用 GA 标签，两者都要兼容。
	zonesLabel := pv.Labels[v1.LabelFailureDomainBetaZone]
	if zonesLabel == "" {
		zonesLabel = pv.Labels[v1.LabelTopologyZone]
	}

	// 多个可用区之间用 "__" 分隔，代表地区级（regional）磁盘。
	zones := strings.Split(zonesLabel, labelMultiZoneDelimiter)
	if len(zones) == 1 && len(zones[0]) != 0 {
		// Zonal
		// 单一可用区：生成区域级 handle，项目仍待推断。
		volID = fmt.Sprintf(volIDZonalFmt, UnspecifiedValue, zones[0], pv.Spec.GCEPersistentDisk.PDName)
	} else if len(zones) > 1 {
		// Regional
		// 多个可用区：先从区名推导出地区名，再生成地区级 handle。
		region, err := gceGetRegionFromZones(zones)
		if err != nil {
			return nil, fmt.Errorf("failed to get region from zones: %v", err)
		}
		volID = fmt.Sprintf(volIDRegionalFmt, UnspecifiedValue, region, pv.Spec.GCEPersistentDisk.PDName)
	} else {
		// Unspecified
		// 没有拓扑标签：项目和可用区都留待后续修复。
		volID = fmt.Sprintf(volIDZonalFmt, UnspecifiedValue, UnspecifiedValue, pv.Spec.GCEPersistentDisk.PDName)
	}

	gceSource := pv.Spec.PersistentVolumeSource.GCEPersistentDisk

	// 分区号转成字符串属性，0 表示不使用分区。
	partition := ""
	if gceSource.Partition != 0 {
		partition = strconv.Itoa(int(gceSource.Partition))
	}

	// 按 CSI 约定组装来源：驱动名、handle、只读、文件系统类型和分区。
	csiSource := &v1.CSIPersistentVolumeSource{
		Driver:       GCEPDDriverName,
		VolumeHandle: volID,
		ReadOnly:     gceSource.ReadOnly,
		FSType:       gceSource.FSType,
		VolumeAttributes: map[string]string{
			"partition": partition,
		},
	}

	// PV 上的拓扑标签也要从树内键转换成 CSI 键，供调度使用。
	if err := translateTopologyFromInTreeToCSI(pv, GCEPDTopologyKey); err != nil {
		return nil, fmt.Errorf("failed to translate topology: %v", err)
	}

	// 切换来源并规整访问模式，避免 CSI 侧校验失败。
	pv.Spec.PersistentVolumeSource.GCEPersistentDisk = nil
	pv.Spec.PersistentVolumeSource.CSI = csiSource
	pv.Spec.AccessModes = backwardCompatibleAccessModes(pv.Spec.AccessModes)

	return pv, nil
}

// TranslateCSIPVToInTree takes a PV with CSIPersistentVolumeSource set and
// translates the GCE PD CSI source to a GCEPersistentDisk source.
// TranslateCSIPVToInTree 是反向翻译：把 CSI GCE PD PV 还原成树内 GCEPersistentDisk PV，用于回滚。
func (g *gcePersistentDiskCSITranslator) TranslateCSIPVToInTree(pv *v1.PersistentVolume) (*v1.PersistentVolume, error) {
	if pv == nil || pv.Spec.CSI == nil {
		return nil, fmt.Errorf("pv is nil or CSI source not defined on pv")
	}
	csiSource := pv.Spec.CSI

	// 磁盘名保存在 handle 的最后一段，需要从中解析出来。
	pdName, err := pdNameFromVolumeID(csiSource.VolumeHandle)
	if err != nil {
		return nil, err
	}

	// 还原树内来源的基本字段。
	gceSource := &v1.GCEPersistentDiskVolumeSource{
		PDName:   pdName,
		FSType:   csiSource.FSType,
		ReadOnly: csiSource.ReadOnly,
	}
	// 分区在 CSI 侧是字符串属性，转回树内所需的 int32；空串表示不使用分区。
	if partition, ok := csiSource.VolumeAttributes["partition"]; ok && partition != "" {
		partInt, err := strconv.Atoi(partition)
		if err != nil {
			return nil, fmt.Errorf("Failed to convert partition %v to integer: %v", partition, err)
		}
		gceSource.Partition = int32(partInt)
	}

	// translate CSI topology to In-tree topology for rollback compatibility
	// 回滚时把 CSI 拓扑键还原为树内标签，地区级还需要从区名推导地区。
	if err := translateTopologyFromCSIToInTree(pv, GCEPDTopologyKey, gceGetRegionFromZones); err != nil {
		return nil, fmt.Errorf("failed to translate topology. PV:%+v. Error:%v", *pv, err)
	}

	// 切换来源字段。
	pv.Spec.CSI = nil
	pv.Spec.GCEPersistentDisk = gceSource

	return pv, nil
}

// CanSupport tests whether the plugin supports a given persistent volume
// specification from the API.  The spec pointer should be considered
// const.
// CanSupport 判断该 PV 是否为树内 GCE PD 卷。
func (g *gcePersistentDiskCSITranslator) CanSupport(pv *v1.PersistentVolume) bool {
	return pv != nil && pv.Spec.GCEPersistentDisk != nil
}

// CanSupportInline tests whether the plugin supports a given inline volume
// specification from the API.  The spec pointer should be considered
// const.
// CanSupportInline 判断内联 Volume 是否为 GCE PD 类型。
func (g *gcePersistentDiskCSITranslator) CanSupportInline(volume *v1.Volume) bool {
	return volume != nil && volume.GCEPersistentDisk != nil
}

// GetInTreePluginName returns the name of the intree plugin driver
// GetInTreePluginName 返回树内插件名称。
func (g *gcePersistentDiskCSITranslator) GetInTreePluginName() string {
	return GCEPDInTreePluginName
}

// GetCSIPluginName returns the name of the CSI plugin
// GetCSIPluginName 返回 CSI 驱动名称。
func (g *gcePersistentDiskCSITranslator) GetCSIPluginName() string {
	return GCEPDDriverName
}

// RepairVolumeHandle returns a fully specified volume handle by inferring
// project, zone/region from the node ID if the volume handle has UNSPECIFIED
// sections
// RepairVolumeHandle 用节点 ID 中的项目和可用区信息补全 handle 里的 UNSPECIFIED 字段。
func (g *gcePersistentDiskCSITranslator) RepairVolumeHandle(volumeHandle, nodeID string) (string, error) {
	var err error
	tok := strings.Split(volumeHandle, "/")
	// handle 至少要有 projects/{p}/zones|regions/{z}/disks/{d} 六段。
	if len(tok) < volIDTotalElements {
		return "", fmt.Errorf("volume handle has wrong number of elements; got %v, wanted %v or more", len(tok), volIDTotalElements)
	}
	// 项目已经确定时无需修补，直接返回。
	if tok[volIDProjectValue] != UnspecifiedValue {
		return volumeHandle, nil
	}

	// 节点 ID 同样按 "/" 拆分，用于补齐项目和可用区。
	nodeTok := strings.Split(nodeID, "/")
	if len(nodeTok) < volIDTotalElements {
		return "", fmt.Errorf("node handle has wrong number of elements; got %v, wanted %v or more", len(nodeTok), volIDTotalElements)
	}

	// 根据 handle 中是 zones 还是 regions 分别构造对应格式的 handle。
	switch tok[volIDRegionalityValue] {
	case "zones":
		zone := ""
		// 可用区未指定则取节点所在区，否则沿用 handle 中已有的区。
		if tok[volIDZoneValue] == UnspecifiedValue {
			zone = nodeTok[volIDZoneValue]
		} else {
			zone = tok[volIDZoneValue]
		}
		return fmt.Sprintf(volIDZonalFmt, nodeTok[volIDProjectValue], zone, tok[volIDDiskNameValue]), nil
	case "regions":
		region := ""
		if tok[volIDZoneValue] == UnspecifiedValue {
			// 地区未指定时先从节点的可用区推导出地区名。
			region, err = gceGetRegionFromZones([]string{nodeTok[volIDZoneValue]})
			if err != nil {
				return "", fmt.Errorf("failed to get region from zone %s: %v", nodeTok[volIDZoneValue], err)
			}
		} else {
			region = tok[volIDZoneValue]
		}
		return fmt.Sprintf(volIDRegionalFmt, nodeTok[volIDProjectValue], region, tok[volIDDiskNameValue]), nil
	default:
		return "", fmt.Errorf("expected volume handle to have zones or regions regionality value, got: %s", tok[volIDRegionalityValue])
	}
}

// pdNameFromVolumeID 从 volume handle 中解析出磁盘名（路径的最后一段）。
func pdNameFromVolumeID(id string) (string, error) {
	splitID := strings.Split(id, "/")
	if len(splitID) < volIDTotalElements {
		return "", fmt.Errorf("failed to get id components.Got: %v, wanted %v components or more. ", len(splitID), volIDTotalElements)
	}
	return splitID[volIDDiskNameValue], nil
}

// TODO: Replace this with the imported one from GCE PD CSI Driver when
// the driver removes all k8s/k8s dependencies
// gceGetRegionFromZones 从可用区名称列表中推导出唯一的地区名（取区名的前两段）。
func gceGetRegionFromZones(zones []string) (string, error) {
	regions := sets.String{}
	if len(zones) < 1 {
		return "", fmt.Errorf("no zones specified")
	}
	for _, zone := range zones {
		// Zone expected format {locale}-{region}-{zone}
		// 区名形如 us-central1-a，前两段拼起来就是地区名。
		splitZone := strings.Split(zone, "-")
		if len(splitZone) != 3 {
			return "", fmt.Errorf("zone in unexpected format, expected: {locale}-{region}-{zone}, got: %v", zone)
		}
		regions.Insert(strings.Join(splitZone[0:2], "-"))
	}
	// 所有可用区必须属于同一个地区，否则无法生成合法的地区级 handle。
	if regions.Len() != 1 {
		return "", fmt.Errorf("multiple or no regions gotten from zones, got: %v", regions)
	}
	return regions.UnsortedList()[0], nil
}
