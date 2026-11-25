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

// 本文件通过显式 import 把内置卷插件链接进 Kubelet 二进制；插件不是运行时自动扫描出来的。
import (
	"context"

	"k8s.io/component-base/featuregate"
	"k8s.io/utils/exec"

	// 卷插件。
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/configmap"
	"k8s.io/kubernetes/pkg/volume/csi"
	"k8s.io/kubernetes/pkg/volume/downwardapi"
	"k8s.io/kubernetes/pkg/volume/emptydir"
	"k8s.io/kubernetes/pkg/volume/fc"
	"k8s.io/kubernetes/pkg/volume/flexvolume"
	"k8s.io/kubernetes/pkg/volume/git_repo"
	"k8s.io/kubernetes/pkg/volume/hostpath"
	"k8s.io/kubernetes/pkg/volume/image"
	"k8s.io/kubernetes/pkg/volume/iscsi"
	"k8s.io/kubernetes/pkg/volume/local"
	"k8s.io/kubernetes/pkg/volume/nfs"
	"k8s.io/kubernetes/pkg/volume/projected"
	"k8s.io/kubernetes/pkg/volume/secret"
)

// ProbeVolumePlugins 汇总当前 Kubelet 二进制支持的内置卷插件；这里只发现插件对象，实际初始化由卷管理器完成。
func ProbeVolumePlugins(ctx context.Context, featureGate featuregate.FeatureGate) ([]volume.VolumePlugin, error) {
	// 显式维护插件列表使编译进二进制的能力可审查，避免依赖动态链接或隐式注册。
	allPlugins := []volume.VolumePlugin{}

	// 当前 Kubelet 无需配置卷插件，后续由卷管理器分析和初始化。
	// 若将来需要配置，可参考 kube-controller-manager/app/plugins.go 中 volume.VolumeConfig 的用法。
	allPlugins = append(allPlugins, emptydir.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, git_repo.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, hostpath.ProbeVolumePlugins(volume.VolumeConfig{})...)
	allPlugins = append(allPlugins, nfs.ProbeVolumePlugins(volume.VolumeConfig{})...)
	allPlugins = append(allPlugins, secret.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, iscsi.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, downwardapi.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, fc.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, configmap.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, projected.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, local.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, csi.ProbeVolumePlugins()...)
	// Image Volume 受 Feature Gate 控制，未启用时不把插件暴露给 Kubelet。
	if featureGate.Enabled(features.ImageVolume) {
		allPlugins = append(allPlugins, image.ProbeVolumePlugins()...)
	}
	return allPlugins, nil
}

// GetDynamicPluginProber 返回动态卷插件发现器；当前动态发现机制主要用于 FlexVolume。
func GetDynamicPluginProber(ctx context.Context, pluginDir string, runner exec.Interface) volume.DynamicPluginProber {
	return flexvolume.GetDynamicPluginProber(ctx, pluginDir, runner)
}
