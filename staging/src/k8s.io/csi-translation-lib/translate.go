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

package csitranslation

import (
	"errors"
	"fmt"

	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	"k8s.io/csi-translation-lib/plugins"
	"k8s.io/klog/v2"
)

var (
	// inTreePlugins 维护 CSI 驱动名到树内插件翻译器的映射，是本包所有翻译与查询能力的注册表。
	// key 是 CSI 驱动名（如 ebs.csi.aws.com），value 是对应插件的翻译器实现。
	// 新增受支持插件时，只需在此处注册。
	inTreePlugins = map[string]plugins.InTreePlugin{
		plugins.GCEPDDriverName:     plugins.NewGCEPersistentDiskCSITranslator(),
		plugins.AWSEBSDriverName:    plugins.NewAWSElasticBlockStoreCSITranslator(),
		plugins.CinderDriverName:    plugins.NewOpenStackCinderCSITranslator(),
		plugins.AzureDiskDriverName: plugins.NewAzureDiskCSITranslator(),
		plugins.AzureFileDriverName: plugins.NewAzureFileCSITranslator(),
		plugins.VSphereDriverName:   plugins.NewvSphereCSITranslator(),
		plugins.PortworxDriverName:  plugins.NewPortworxCSITranslator(),
	}
)

// CSITranslator 是树内（in-tree）存储 API 对象到等价 CSI API 对象的翻译器。
// 它同时提供一组辅助方法，用于判断某种插件是否存在翻译逻辑，
// 以及"树内插件名 <-> CSI 驱动名"之间的双向映射。
// 本类型是无状态的，所有查询都基于包级的 inTreePlugins 注册表。
type CSITranslator struct{}

// New 创建一个执行真实翻译工作的 CSITranslator 实例。
func New() CSITranslator {
	return CSITranslator{}
}

// TranslateInTreeStorageClassToCSI 把树内 StorageClass 的 parameters 翻译为 CSI 插件可消费的参数。
// 处理流程：
//  1. 先对入参 StorageClass 做 DeepCopy，避免修改调用方对象；
//  2. 遍历注册表，按树内插件名找到对应翻译器；
//  3. 委托该插件完成具体参数转换；找不到则返回错误。
func (CSITranslator) TranslateInTreeStorageClassToCSI(logger klog.Logger, inTreePluginName string, sc *storage.StorageClass) (*storage.StorageClass, error) {
	// 深拷贝，保证输入对象不被修改。
	newSC := sc.DeepCopy()
	// 按树内插件名在注册表中查找翻译器。
	for _, curPlugin := range inTreePlugins {
		if inTreePluginName == curPlugin.GetInTreePluginName() {
			return curPlugin.TranslateInTreeStorageClassToCSI(logger, newSC)
		}
	}
	return nil, fmt.Errorf("could not find in-tree storage class parameter translation logic for %#v", inTreePluginName)
}

// TranslateInTreeInlineVolumeToCSI 把 Pod 中的内联卷（inline volume）翻译为一个包装好的 PV，
// 其 PersistentVolumeSource 为对应的 CSI 卷源。
// podNamespace 用于构造需要命名空间信息的卷（如 Azure File 的 Secret 引用）。
// 特殊处理：内联卷只支持 Filesystem 卷模式，不支持 Block。
// 由于内联卷不经过常规 PV 初始化流程（那一步会填充 VolumeMode），
// 因此这里需要显式地把未设置的 VolumeMode 补为 Filesystem。
func (CSITranslator) TranslateInTreeInlineVolumeToCSI(logger klog.Logger, volume *v1.Volume, podNamespace string) (*v1.PersistentVolume, error) {
	// 空指针防御。
	if volume == nil {
		return nil, fmt.Errorf("persistent volume was nil")
	}
	for _, curPlugin := range inTreePlugins {
		// 由各插件判断自己是否支持该内联卷。
		if curPlugin.CanSupportInline(volume) {
			pv, err := curPlugin.TranslateInTreeInlineVolumeToCSI(logger, volume, podNamespace)
			if err != nil {
				return nil, err
			}
			// 插件翻译器未显式设置 VolumeMode 时，在这里补上默认值 Filesystem。
			if pv.Spec.VolumeMode == nil {
				volumeMode := v1.PersistentVolumeFilesystem
				pv.Spec.VolumeMode = &volumeMode
			}
			return pv, nil
		}
	}
	return nil, fmt.Errorf("could not find in-tree plugin translation logic for %#v", volume.Name)
}

// TranslateInTreePVToCSI 把树内 PV 的卷源翻译为 CSI 卷源，返回新的 PV 对象。
// 入参 pv 不会被修改：方法内部先做 DeepCopy，再交给具体插件翻译。
// 流程：
//  1. 空指针检查；
//  2. 深拷贝 PV；
//  3. 遍历注册表，用 CanSupport 找到支持该 PV 的插件；
//  4. 委托插件完成翻译；找不到则返回错误。
func (CSITranslator) TranslateInTreePVToCSI(logger klog.Logger, pv *v1.PersistentVolume) (*v1.PersistentVolume, error) {
	if pv == nil {
		return nil, errors.New("persistent volume was nil")
	}
	// 深拷贝，保证输入 PV 不被修改。
	copiedPV := pv.DeepCopy()
	for _, curPlugin := range inTreePlugins {
		if curPlugin.CanSupport(copiedPV) {
			return curPlugin.TranslateInTreePVToCSI(logger, copiedPV)
		}
	}
	return nil, fmt.Errorf("could not find in-tree plugin translation logic for %#v", copiedPV.Name)
}

// TranslateCSIPVToInTree 是 TranslateInTreePVToCSI 的逆向操作：
// 根据 CSI 卷源中的 Driver 字段找到对应插件，把 CSI PV 翻译回树内 PV。
// 入参同样不会被修改（内部做 DeepCopy）。
// 流程：
//  1. 检查 pv 及其 CSI 卷源是否为空；
//  2. 深拷贝 PV；
//  3. 按 CSI.Driver 在注册表中查找插件并委托翻译；找不到则返回错误。
func (CSITranslator) TranslateCSIPVToInTree(pv *v1.PersistentVolume) (*v1.PersistentVolume, error) {
	if pv == nil || pv.Spec.CSI == nil {
		return nil, errors.New("CSI persistent volume was nil")
	}
	// 深拷贝，保证输入 PV 不被修改。
	copiedPV := pv.DeepCopy()
	// 这里用 map 的 key（CSI 驱动名）直接匹配，比遍历更高效。
	for driverName, curPlugin := range inTreePlugins {
		if copiedPV.Spec.CSI.Driver == driverName {
			return curPlugin.TranslateCSIPVToInTree(copiedPV)
		}
	}
	return nil, fmt.Errorf("could not find in-tree plugin translation logic for %s", copiedPV.Spec.CSI.Driver)
}

// IsMigratableIntreePluginByName 判断给定名称的树内插件是否存在对应的迁移（翻译）逻辑。
func (CSITranslator) IsMigratableIntreePluginByName(inTreePluginName string) bool {
	for _, curPlugin := range inTreePlugins {
		if curPlugin.GetInTreePluginName() == inTreePluginName {
			return true
		}
	}
	return false
}

// IsMigratedCSIDriverByName 判断是否存在某个树内插件可以迁移到给定名称的 CSI 驱动。
// 直接以驱动名查询注册表即可。
func (CSITranslator) IsMigratedCSIDriverByName(csiPluginName string) bool {
	if _, ok := inTreePlugins[csiPluginName]; ok {
		return true
	}
	return false
}

// GetInTreePluginNameFromSpec 从 PV 或内联 Volume 中识别其所属的树内插件名。
// pv 与 vol 至少提供一个：优先检查 pv，否则检查 vol；两者都为 nil 时返回错误。
// 识别方式与 Translate 系列一致：
//   - PV：调用插件的 CanSupport；
//   - 内联 Volume：调用插件的 CanSupportInline。
func (CSITranslator) GetInTreePluginNameFromSpec(pv *v1.PersistentVolume, vol *v1.Volume) (string, error) {
	if pv != nil {
		for _, curPlugin := range inTreePlugins {
			if curPlugin.CanSupport(pv) {
				return curPlugin.GetInTreePluginName(), nil
			}
		}
		return "", fmt.Errorf("could not find in-tree plugin name from persistent volume %s", pv.Name)
	} else if vol != nil {
		for _, curPlugin := range inTreePlugins {
			if curPlugin.CanSupportInline(vol) {
				return curPlugin.GetInTreePluginName(), nil
			}
		}
		return "", fmt.Errorf("could not find in-tree plugin name from volume %s", vol.Name)
	} else {
		return "", errors.New("both persistent volume and volume are nil")
	}
}

// GetCSINameFromInTreeName 根据树内插件名查询取代它的 CSI 驱动名。
// 由于注册表以 CSI 驱动名为 key，这里需要遍历 value 进行反向查找。
func (CSITranslator) GetCSINameFromInTreeName(pluginName string) (string, error) {
	for csiDriverName, curPlugin := range inTreePlugins {
		if curPlugin.GetInTreePluginName() == pluginName {
			return csiDriverName, nil
		}
	}
	return "", fmt.Errorf("could not find CSI Driver name for plugin %s", pluginName)
}

// GetInTreeNameFromCSIName 根据 CSI 驱动名查询被其取代的树内插件名。
// 可直接用驱动名在注册表中查表，再取插件的 GetInTreePluginName。
func (CSITranslator) GetInTreeNameFromCSIName(pluginName string) (string, error) {
	if plugin, ok := inTreePlugins[pluginName]; ok {
		return plugin.GetInTreePluginName(), nil
	}
	return "", fmt.Errorf("could not find In-Tree driver name for CSI plugin %s", pluginName)
}

// IsPVMigratable 判断给定 PV 是否是某个可迁移树内插件的卷。
func (CSITranslator) IsPVMigratable(pv *v1.PersistentVolume) bool {
	for _, curPlugin := range inTreePlugins {
		if curPlugin.CanSupport(pv) {
			return true
		}
	}
	return false
}

// IsInlineMigratable 判断给定内联 Volume 是否是某个可迁移树内插件的卷。
func (CSITranslator) IsInlineMigratable(vol *v1.Volume) bool {
	for _, curPlugin := range inTreePlugins {
		if curPlugin.CanSupportInline(vol) {
			return true
		}
	}
	return false
}

// RepairVolumeHandle 根据节点 ID 修复（生成）正确的 volume handle。
// 某些 CSI 驱动的 volume handle 中编码了节点信息，迁移时需要按当前节点重写；
// 具体规则由各插件自行实现，未注册的驱动返回错误。
func (CSITranslator) RepairVolumeHandle(driverName, volumeHandle, nodeID string) (string, error) {
	if plugin, ok := inTreePlugins[driverName]; ok {
		return plugin.RepairVolumeHandle(volumeHandle, nodeID)
	}
	return "", fmt.Errorf("could not find In-Tree driver name for CSI plugin %s", driverName)
}
