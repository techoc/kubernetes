/*
Copyright 2017 The Kubernetes Authors.

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

package flexvolume

import (
	"k8s.io/klog/v2"

	"k8s.io/kubernetes/pkg/volume"
)

// pluginDefaults 为未自行实现该方法的 flexVolume 插件提供默认行为。
type pluginDefaults flexVolumePlugin

// logPrefix 返回带驱动名的日志前缀，便于定位具体驱动。
func logPrefix(plugin *flexVolumePlugin) string {
	return "flexVolume driver " + plugin.driverName + ": "
}

// GetVolumeName 默认返回卷名本身；多数驱动无需自定义名称。
func (plugin *pluginDefaults) GetVolumeName(spec *volume.Spec) (string, error) {
	klog.V(4).Info(logPrefix((*flexVolumePlugin)(plugin)), "using default GetVolumeName for volume ", spec.Name())
	return spec.Name(), nil
}
