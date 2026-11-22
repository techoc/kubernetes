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

package csimigration

import (
	"errors"
	"fmt"

	v1 "k8s.io/api/core/v1"
	csilibplugins "k8s.io/csi-translation-lib/plugins"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/volume"
)

// PluginNameMapper 提供工具方法：从 spec 获取支持它的插件名，
// 在 in-tree 插件名与迁移后的 CSI 插件名之间映射等。
type PluginNameMapper interface {
	GetInTreePluginNameFromSpec(pv *v1.PersistentVolume, vol *v1.Volume) (string, error)
	GetCSINameFromInTreeName(pluginName string) (string, error)
}

// PluginManager 跟踪 in-tree 插件的迁移状态。
type PluginManager struct {
	PluginNameMapper
}

// NewPluginManager 返回一个新的 PluginManager 实例。
func NewPluginManager(m PluginNameMapper) PluginManager {
	return PluginManager{
		PluginNameMapper: m,
	}
}

// IsMigrationCompleteForPlugin 表示某个存储插件的 CSI 迁移是否已完成。
// 完成迁移需要：
// 1. 为该插件启用 CSIMigrationXX；
// 2. 通过设置 InTreePluginXXUnregister feature gate 注销 in-tree 插件。
func (pm PluginManager) IsMigrationCompleteForPlugin(pluginName string) bool {
	// 插件级迁移完成要生效，CSIMigration 与插件专属的 InTreePluginUnregister 开关都必须开启。
	if !pm.IsMigrationEnabledForPlugin(pluginName) {
		return false
	}

	switch pluginName {
	case csilibplugins.AWSEBSInTreePluginName:
		return true
	case csilibplugins.GCEPDInTreePluginName:
		return true
	case csilibplugins.AzureFileInTreePluginName:
		return true
	case csilibplugins.AzureDiskInTreePluginName:
		return true
	case csilibplugins.CinderInTreePluginName:
		return true
	case csilibplugins.VSphereInTreePluginName:
		return true
	case csilibplugins.PortworxVolumePluginName:
		return true
	default:
		return false
	}
}

// IsMigrationEnabledForPlugin 表示某个存储插件是否已启用 CSI 迁移。
func (pm PluginManager) IsMigrationEnabledForPlugin(pluginName string) bool {
	// CSIMigration 需要与插件专属开关一起启用。
	// CSIMigration 已 GA，默认开启。

	switch pluginName {
	case csilibplugins.AWSEBSInTreePluginName:
		return true
	case csilibplugins.GCEPDInTreePluginName:
		return true
	case csilibplugins.AzureFileInTreePluginName:
		return true
	case csilibplugins.AzureDiskInTreePluginName:
		return true
	case csilibplugins.CinderInTreePluginName:
		return true
	case csilibplugins.VSphereInTreePluginName:
		return true
	case csilibplugins.PortworxVolumePluginName:
		return true
	default:
		return false
	}
}

// IsMigratable 表示 spec 所指的卷插件是否已启用 CSI 迁移。
func (pm PluginManager) IsMigratable(spec *volume.Spec) (bool, error) {
	if spec == nil {
		return false, fmt.Errorf("could not find if plugin is migratable because volume spec is nil")
	}

	pluginName, _ := pm.GetInTreePluginNameFromSpec(spec.PersistentVolume, spec.Volume)
	if pluginName == "" {
		return false, nil
	}
	// 找到了支持该 spec 的 in-tree 插件
	return pm.IsMigrationEnabledForPlugin(pluginName), nil
}

// InTreeToCSITranslator 把 PV 和 Volume 对象中的卷来源从 in-tree 插件引用
// 翻译为迁移后的 CSI 插件引用。
type InTreeToCSITranslator interface {
	TranslateInTreePVToCSI(logger klog.Logger, pv *v1.PersistentVolume) (*v1.PersistentVolume, error)
	TranslateInTreeInlineVolumeToCSI(logger klog.Logger, volume *v1.Volume, podNamespace string) (*v1.PersistentVolume, error)
}

// TranslateInTreeSpecToCSI 把 in-tree 插件支持的卷 spec（PV 或 inline volume）翻译为 CSI。
func TranslateInTreeSpecToCSI(logger klog.Logger, spec *volume.Spec, podNamespace string, translator InTreeToCSITranslator) (*volume.Spec, error) {
	var csiPV *v1.PersistentVolume
	var err error
	inlineVolume := false
	if spec.PersistentVolume != nil {
		csiPV, err = translator.TranslateInTreePVToCSI(logger, spec.PersistentVolume)
	} else if spec.Volume != nil {
		csiPV, err = translator.TranslateInTreeInlineVolumeToCSI(logger, spec.Volume, podNamespace)
		inlineVolume = true
	} else {
		err = errors.New("not a valid volume spec")
	}
	if err != nil {
		return nil, fmt.Errorf("failed to translate in-tree pv to CSI: %v", err)
	}
	return &volume.Spec{
		Migrated:                        true,
		PersistentVolume:                csiPV,
		ReadOnly:                        spec.ReadOnly,
		InlineVolumeSpecForCSIMigration: inlineVolume,
	}, nil
}
