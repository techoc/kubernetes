/*
Copyright 2014 The Kubernetes Authors.

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

package app

import (
	// 通过导入强制链接所需插件实现；更合理的方式是由各二进制目标的构建配置决定。

	"context"
	"fmt"

	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/volume/csi"
	"k8s.io/kubernetes/pkg/volume/iscsi"

	// 存储卷插件
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/fc"
	"k8s.io/kubernetes/pkg/volume/flexvolume"
	"k8s.io/kubernetes/pkg/volume/hostpath"
	"k8s.io/kubernetes/pkg/volume/nfs"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"

	persistentvolumeconfig "k8s.io/kubernetes/pkg/controller/volume/persistentvolume/config"
	"k8s.io/utils/exec"
)

// ProbeAttachableVolumePlugins 收集挂载/卸载控制器所需的全部存储卷插件。
func ProbeAttachableVolumePlugins(logger klog.Logger, config persistentvolumeconfig.VolumeConfiguration) ([]volume.VolumePlugin, error) {
	return probeControllerVolumePlugins(logger, config, func(plugin volume.VolumePlugin) bool {
		_, ok := plugin.(volume.AttachableVolumePlugin)
		return ok
	})
}

// GetDynamicPluginProber 返回挂载/卸载控制器用于动态发现插件的探测器；目前仅 Flexvolume 支持。
func GetDynamicPluginProber(ctx context.Context, config persistentvolumeconfig.VolumeConfiguration) volume.DynamicPluginProber {
	return flexvolume.GetDynamicPluginProber(ctx, config.FlexVolumePluginDir, exec.New() /*exec.Interface*/)
}

// ProbeExpandableVolumePlugins 返回支持在线扩容的存储卷插件。
func ProbeExpandableVolumePlugins(logger klog.Logger, config persistentvolumeconfig.VolumeConfiguration) ([]volume.VolumePlugin, error) {
	return probeControllerVolumePlugins(logger, config, func(plugin volume.VolumePlugin) bool {
		_, ok := plugin.(volume.ExpandableVolumePlugin)
		return ok
	})
}

// ProbeProvisionableRecyclableVolumePlugins 收集支持动态供给、删除或回收的存储卷插件。
func ProbeProvisionableRecyclableVolumePlugins(logger klog.Logger, config persistentvolumeconfig.VolumeConfiguration) ([]volume.VolumePlugin, error) {
	return probeControllerVolumePlugins(logger, config, func(plugin volume.VolumePlugin) bool {
		if _, ok := plugin.(volume.ProvisionableVolumePlugin); ok {
			return true
		}
		if _, ok := plugin.(volume.DeletableVolumePlugin); ok {
			return true
		}
		if _, ok := plugin.(volume.RecyclableVolumePlugin); ok {
			return true
		}
		return false
	})
}

// ProbePersistentVolumePlugins 收集全部持久存储卷插件。
func ProbePersistentVolumePlugins(logger klog.Logger, config persistentvolumeconfig.VolumeConfiguration) ([]volume.VolumePlugin, error) {
	return probeControllerVolumePlugins(logger, config, nil)
}

// probeControllerVolumePlugins 按给定条件筛选并收集控制器管理器使用的持久存储卷插件。
func probeControllerVolumePlugins(logger klog.Logger, config persistentvolumeconfig.VolumeConfiguration, filter func(plugin volume.VolumePlugin) bool) ([]volume.VolumePlugin, error) {
	var allPlugins []volume.VolumePlugin

	// 待探测插件由本二进制静态决定，而非依赖动态链接；插件稍后再分析和初始化。

	// 每个插件都使用 VolumeConfig。本函数的单一入参汇总所有存储卷插件选项，
	// 需按插件拆分为独立的 VolumeConfig，再传给 ProbeVolumePlugins。

	// HostPath 回收仅用于测试和开发。
	hostPathConfig := volume.VolumeConfig{
		RecyclerMinimumTimeout:   int(config.PersistentVolumeRecyclerConfiguration.MinimumTimeoutHostPath),
		RecyclerTimeoutIncrement: int(config.PersistentVolumeRecyclerConfiguration.IncrementTimeoutHostPath),
		RecyclerPodTemplate:      volume.NewPersistentVolumeRecyclerPodTemplate(),
		ProvisioningEnabled:      config.EnableHostPathProvisioning,
	}
	if err := AttemptToLoadRecycler(config.PersistentVolumeRecyclerConfiguration.PodTemplateFilePathHostPath, &hostPathConfig); err != nil {
		logger.Error(err, "Could not create hostpath recycler pod from file", "path", config.PersistentVolumeRecyclerConfiguration.PodTemplateFilePathHostPath)
		return nil, err
	}
	allPlugins = append(allPlugins, hostpath.ProbeVolumePlugins(hostPathConfig)...)

	nfsConfig := volume.VolumeConfig{
		RecyclerMinimumTimeout:   int(config.PersistentVolumeRecyclerConfiguration.MinimumTimeoutNFS),
		RecyclerTimeoutIncrement: int(config.PersistentVolumeRecyclerConfiguration.IncrementTimeoutNFS),
		RecyclerPodTemplate:      volume.NewPersistentVolumeRecyclerPodTemplate(),
	}
	if err := AttemptToLoadRecycler(config.PersistentVolumeRecyclerConfiguration.PodTemplateFilePathNFS, &nfsConfig); err != nil {
		logger.Error(err, "Could not create NFS recycler pod from file", "path", config.PersistentVolumeRecyclerConfiguration.PodTemplateFilePathNFS)
		return nil, err
	}
	allPlugins = append(allPlugins, nfs.ProbeVolumePlugins(nfsConfig)...)
	allPlugins = append(allPlugins, fc.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, iscsi.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, csi.ProbeVolumePlugins()...)

	var filteredPlugins []volume.VolumePlugin
	if filter == nil {
		filteredPlugins = allPlugins
	} else {
		for _, plugin := range allPlugins {
			if filter(plugin) {
				filteredPlugins = append(filteredPlugins, plugin)
			}
		}
	}

	return filteredPlugins, nil
}

// AttemptToLoadRecycler 从文件解析 Pod 模板并设置为存储卷回收器；失败时返回错误。该函数供下游复用。
func AttemptToLoadRecycler(path string, config *volume.VolumeConfig) error {
	if path != "" {
		recyclerPod, err := volumeutil.LoadPodFromFile(path)
		if err != nil {
			return err
		}
		if err = volume.ValidateRecyclerPodTemplate(recyclerPod); err != nil {
			return fmt.Errorf("pod specification (%v): %v", path, err)
		}
		config.RecyclerPodTemplate = recyclerPod
	}
	return nil
}
