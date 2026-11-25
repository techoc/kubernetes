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
	"regexp"
	"strings"

	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

const (
	// AzureDiskDriverName 是 Azure Disk CSI 驱动的名称。
	AzureDiskDriverName = "disk.csi.azure.com"
	// AzureDiskTopologyKey 是 Azure Disk CSI 驱动使用的区拓扑键。
	AzureDiskTopologyKey = "topology.disk.csi.azure.com/zone"
	// AzureDiskInTreePluginName 是 in-tree Azure Disk 插件的名称。
	AzureDiskInTreePluginName = "kubernetes.io/azure-disk"

	// 以下是 Azure Disk CSI 驱动定义的参数名，用于构造 VolumeAttributes。
	// kind：磁盘类型；cachingmode：缓存模式；fstype：文件系统类型。
	azureDiskKind        = "kind"
	azureDiskCachingMode = "cachingmode"
	azureDiskFSType      = "fstype"
)

// 包级正则与常量：前两个正则分别用于从托管磁盘资源 ID 和非托管 VHD URL 中
// 提取磁盘名；managed 是写入 CSI kind 属性的托管类型字符串；
// unzonedCSIRegionRE 匹配纯数字拓扑值，代表没有可用区的 region。
var (
	managedDiskPathRE   = regexp.MustCompile(`.*/subscriptions/(?:.*)/resourceGroups/(?:.*)/providers/Microsoft.Compute/disks/(.+)`)
	unmanagedDiskPathRE = regexp.MustCompile(`http(?:.*)://(?:.*)/vhds/(.+)`)
	managed             = string(v1.AzureManagedDisk)
	unzonedCSIRegionRE  = regexp.MustCompile(`^[0-9]+$`)
)

// 编译期断言：确保 azureDiskCSITranslator 实现了 InTreePlugin 接口。
var _ InTreePlugin = &azureDiskCSITranslator{}

// azureDiskCSITranslator 负责 in-tree Azure Disk 与 CSI Azure Disk 之间 PV spec 的双向翻译。
type azureDiskCSITranslator struct{}

// NewAzureDiskCSITranslator 返回一个新的 Azure Disk 翻译器实例。
func NewAzureDiskCSITranslator() InTreePlugin {
	return &azureDiskCSITranslator{}
}

// TranslateInTreeStorageClassToCSI 把 in-tree Azure Disk StorageClass 翻译为 CSI StorageClass：
// 把 zone/zones 转为拓扑选择器，并清洗无可用区 region 的拓扑值。
// 驱动参数说明：https://github.com/kubernetes-sigs/azuredisk-csi-driver/blob/master/docs/driver-parameters.md
func (t *azureDiskCSITranslator) TranslateInTreeStorageClassToCSI(logger klog.Logger, sc *storage.StorageClass) (*storage.StorageClass, error) {
	var (
		generatedTopologies []v1.TopologySelectorTerm
		params              = map[string]string{}
	)
	// 逐项遍历参数：zone/zones 转成拓扑，其余参数原样保留。
	for k, v := range sc.Parameters {
		switch strings.ToLower(k) {
		case zoneKey:
			generatedTopologies = generateToplogySelectors(AzureDiskTopologyKey, []string{v})
		case zonesKey:
			generatedTopologies = generateToplogySelectors(AzureDiskTopologyKey, strings.Split(v, ","))
		default:
			params[k] = v
		}
	}

	// zone/zones 与 AllowedTopologies 语义重叠，不能同时设置；
	// 否则按需写入新拓扑或把 in-tree 键翻译为 CSI 键。
	if len(generatedTopologies) > 0 && len(sc.AllowedTopologies) > 0 {
		return nil, fmt.Errorf("cannot simultaneously set allowed topologies and zone/zones parameters")
	} else if len(generatedTopologies) > 0 {
		sc.AllowedTopologies = generatedTopologies
	} else if len(sc.AllowedTopologies) > 0 {
		newTopologies, err := translateAllowedTopologies(sc.AllowedTopologies, AzureDiskTopologyKey)
		if err != nil {
			return nil, fmt.Errorf("failed translating allowed topologies: %v", err)
		}
		sc.AllowedTopologies = newTopologies
	}
	// Azure 中无可用区 region 的旧拓扑值是数字，需替换为 CSI 驱动期望的空字符串。
	sc.AllowedTopologies = t.replaceFailureDomainsToCSI(sc.AllowedTopologies)

	sc.Parameters = params

	return sc, nil
}

// TranslateInTreeInlineVolumeToCSI 把 Pod 中 inline 的 in-tree Azure Disk 卷转换为独立的 CSI PV。
func (t *azureDiskCSITranslator) TranslateInTreeInlineVolumeToCSI(logger klog.Logger, volume *v1.Volume, podNamespace string) (*v1.PersistentVolume, error) {
	// 入参必须是带 AzureDisk 的 Volume。
	if volume == nil || volume.AzureDisk == nil {
		return nil, fmt.Errorf("volume is nil or Azure Disk not defined on volume")
	}

	azureSource := volume.AzureDisk
	// CSI 迁移只支持托管磁盘，非托管（Shared Blob）类型直接拒绝。
	if azureSource.Kind != nil && !strings.EqualFold(string(*azureSource.Kind), managed) {
		return nil, fmt.Errorf("kind(%v) is not supported in csi migration", *azureSource.Kind)
	}
	pv := &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			// 以 DataDiskURI 作为 PV 名称，保证跨磁盘唯一；它也是 staging 路径的组成部分。
			Name: azureSource.DataDiskURI,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource: v1.PersistentVolumeSource{
				CSI: &v1.CSIPersistentVolumeSource{
					Driver:           AzureDiskDriverName,
					VolumeHandle:     azureSource.DataDiskURI,
					VolumeAttributes: map[string]string{azureDiskKind: managed},
				},
			},
			// inline 卷固定为单节点读写模式。
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
		},
	}
	// 以下字段均为指针，需判空后再解引用映射到 CSI 源。
	if azureSource.ReadOnly != nil {
		pv.Spec.PersistentVolumeSource.CSI.ReadOnly = *azureSource.ReadOnly
	}

	// 缓存模式作为 VolumeAttributes 传递，空值无需写入。
	if azureSource.CachingMode != nil && *azureSource.CachingMode != "" {
		pv.Spec.PersistentVolumeSource.CSI.VolumeAttributes[azureDiskCachingMode] = string(*azureSource.CachingMode)
	}
	// FSType 同时写入 CSI 的 FSType 字段和 VolumeAttributes，保持一致。
	if azureSource.FSType != nil {
		pv.Spec.PersistentVolumeSource.CSI.FSType = *azureSource.FSType
		pv.Spec.PersistentVolumeSource.CSI.VolumeAttributes[azureDiskFSType] = *azureSource.FSType
	}
	// 明确写入 kind=Managed，完成磁盘类型属性设置。
	pv.Spec.PersistentVolumeSource.CSI.VolumeAttributes[azureDiskKind] = managed

	return pv, nil
}

// TranslateInTreePVToCSI 把带 in-tree AzureDisk 源的 PV 原地转换为 CSI PV。
func (t *azureDiskCSITranslator) TranslateInTreePVToCSI(logger klog.Logger, pv *v1.PersistentVolume) (*v1.PersistentVolume, error) {
	// 校验 PV 非空且确实带有 AzureDisk 源。
	if pv == nil || pv.Spec.AzureDisk == nil {
		return nil, fmt.Errorf("pv is nil or Azure Disk source not defined on pv")
	}

	var (
		azureSource = pv.Spec.PersistentVolumeSource.AzureDisk

		// 按 CSI 驱动参数文档构造源：磁盘 URI 作为 volumeHandle，kind 固定为 Managed。
		csiSource = &v1.CSIPersistentVolumeSource{
			Driver:           AzureDiskDriverName,
			VolumeAttributes: map[string]string{azureDiskKind: managed},
			VolumeHandle:     azureSource.DataDiskURI,
		}
	)

	// 与 inline 路径一致，非托管磁盘不支持迁移。
	if azureSource.Kind != nil && !strings.EqualFold(string(*azureSource.Kind), managed) {
		return nil, fmt.Errorf("kind(%v) is not supported in csi migration", *azureSource.Kind)
	}

	// 缓存模式映射到 VolumeAttributes。
	if azureSource.CachingMode != nil {
		csiSource.VolumeAttributes[azureDiskCachingMode] = string(*azureSource.CachingMode)
	}

	// 文件系统类型同时写入 FSType 字段与 VolumeAttributes。
	if azureSource.FSType != nil {
		csiSource.FSType = *azureSource.FSType
		csiSource.VolumeAttributes[azureDiskFSType] = *azureSource.FSType
	}
	csiSource.VolumeAttributes[azureDiskKind] = managed

	// 只读标志直接映射。
	if azureSource.ReadOnly != nil {
		csiSource.ReadOnly = *azureSource.ReadOnly
	}

	// 清空旧源并挂载 CSI 源，避免 PV 上同时存在两种来源。
	pv.Spec.PersistentVolumeSource.AzureDisk = nil
	pv.Spec.PersistentVolumeSource.CSI = csiSource

	return pv, nil
}

// TranslateCSIPVToInTree 是反向翻译：把 Azure Disk CSI 源还原为 in-tree AzureDisk 源。
func (t *azureDiskCSITranslator) TranslateCSIPVToInTree(pv *v1.PersistentVolume) (*v1.PersistentVolume, error) {
	// 校验 PV 非空且带有 CSI 源。
	if pv == nil || pv.Spec.CSI == nil {
		return nil, fmt.Errorf("pv is nil or CSI source not defined on pv")
	}
	csiSource := pv.Spec.CSI

	// volumeHandle 就是磁盘 URI，从中解析出 in-tree 需要的 DiskName。
	diskURI := csiSource.VolumeHandle
	diskName, err := getDiskName(diskURI)
	if err != nil {
		return nil, err
	}

	// 先按直接对应关系构造 AzureDisk 源，kind 固定为 Managed。
	managed := v1.AzureManagedDisk
	azureSource := &v1.AzureDiskVolumeSource{
		DiskName:    diskName,
		DataDiskURI: diskURI,
		FSType:      &csiSource.FSType,
		ReadOnly:    &csiSource.ReadOnly,
		Kind:        &managed,
	}

	// VolumeAttributes 中的缓存模式与文件系统类型优先级更高，用于覆盖上面的默认值。
	if csiSource.VolumeAttributes != nil {
		for k, v := range csiSource.VolumeAttributes {
			// 键名大小写不敏感，便于兼容不同书写形式。
			switch strings.ToLower(k) {
			case azureDiskCachingMode:
				// 空字符串不设置，避免产生无效的缓存模式。
				if v != "" {
					mode := v1.AzureDataDiskCachingMode(v)
					azureSource.CachingMode = &mode
				}
			case azureDiskFSType:
				if v != "" {
					fsType := v
					azureSource.FSType = &fsType
				}
			}
		}
		azureSource.Kind = &managed
	}

	// 清空 CSI 源并挂回 in-tree 源。
	pv.Spec.CSI = nil
	pv.Spec.AzureDisk = azureSource

	return pv, nil
}

// CanSupport 判断该插件能否处理该 PV：即是否为 in-tree Azure Disk 源。
// 传入对象的 spec 应视为常量，不得修改。
func (t *azureDiskCSITranslator) CanSupport(pv *v1.PersistentVolume) bool {
	return pv != nil && pv.Spec.AzureDisk != nil
}

// CanSupportInline 判断该插件能否处理该 inline Volume：即是否带 AzureDisk 源。
// 传入对象的 spec 应视为常量，不得修改。
func (t *azureDiskCSITranslator) CanSupportInline(volume *v1.Volume) bool {
	return volume != nil && volume.AzureDisk != nil
}

// GetInTreePluginName 返回 in-tree 插件的名称。
func (t *azureDiskCSITranslator) GetInTreePluginName() string {
	return AzureDiskInTreePluginName
}

// GetCSIPluginName 返回对应 CSI 插件的名称。
func (t *azureDiskCSITranslator) GetCSIPluginName() string {
	return AzureDiskDriverName
}

// RepairVolumeHandle 对旧的 volumeHandle 做修复；Azure Disk 无需修复，原样返回。
func (t *azureDiskCSITranslator) RepairVolumeHandle(volumeHandle, nodeID string) (string, error) {
	return volumeHandle, nil
}

// isManagedDisk 根据 URI 是否以 http 开头判断磁盘是否为非托管磁盘（VHD URL）。
func isManagedDisk(diskURI string) bool {
	if len(diskURI) > 4 && strings.ToLower(diskURI[:4]) == "http" {
		return false
	}
	return true
}

// getDiskName 从磁盘 URI 中解析出磁盘名：
// 先判断托管/非托管类型，再选用对应正则提取捕获组。
func getDiskName(diskURI string) (string, error) {
	diskPathRE := managedDiskPathRE
	if !isManagedDisk(diskURI) {
		diskPathRE = unmanagedDiskPathRE
	}

	// 正则需恰好匹配一个捕获组，否则说明 URI 格式不符合预期。
	matches := diskPathRE.FindStringSubmatch(diskURI)
	if len(matches) != 2 {
		return "", fmt.Errorf("could not get disk name from %s, correct format: %s", diskURI, diskPathRE)
	}
	return matches[1], nil
}

// replaceFailureDomainsToCSI 把无可用区 region 的数字拓扑值（如 "0"）替换为空字符串，
// 因为 Azure Disk CSI 驱动期望用 "" 表示这种拓扑。
func (t *azureDiskCSITranslator) replaceFailureDomainsToCSI(terms []v1.TopologySelectorTerm) []v1.TopologySelectorTerm {
	// nil 输入保持 nil，避免产生空切片语义差异。
	if terms == nil {
		return nil
	}

	newTopologies := []v1.TopologySelectorTerm{}
	for _, term := range terms {
		// 深拷贝后再修改，避免改动调用方持有的原拓扑对象。
		newTerm := term.DeepCopy()
		for i := range newTerm.MatchLabelExpressions {
			exp := &newTerm.MatchLabelExpressions[i]
			// 只处理 Azure Disk CSI 的区拓扑键。
			if exp.Key == AzureDiskTopologyKey {
				for j := range exp.Values {
					if unzonedCSIRegionRE.Match([]byte(exp.Values[j])) {
						// 纯数字值代表无可用区，改写为空字符串。
						exp.Values[j] = ""
					}
				}
			}
		}
		newTopologies = append(newTopologies, *newTerm)
	}
	return newTopologies
}
