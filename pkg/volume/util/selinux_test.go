/*
Copyright 2024 The Kubernetes Authors.

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

package util

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/version"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/featuregate"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/volume"
	volumetesting "k8s.io/kubernetes/pkg/volume/testing"
	"k8s.io/utils/ptr"
)

// TestGetMountSELinuxLabel 验证不同特性开关、变更策略与插件支持情况下 SELinux 标签的计算结果。
func TestGetMountSELinuxLabel(t *testing.T) {
	pvRWOP := &v1.PersistentVolume{
		Spec: v1.PersistentVolumeSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteOncePod},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				HostPath: &v1.HostPathVolumeSource{},
			},
		},
	}
	pvRWX := &v1.PersistentVolume{
		Spec: v1.PersistentVolumeSpec{
			AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteMany},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				HostPath: &v1.HostPathVolumeSource{},
			},
		},
	}

	seLinuxOpts1 := v1.SELinuxOptions{
		Level: "s0:c123,c456",
	}
	seLinuxOpts2 := v1.SELinuxOptions{
		Level: "s0:c234,c567",
	}
	seLinuxOpts3 := v1.SELinuxOptions{
		Level: "s0:c345,c678",
	}
	label1 := "system_u:object_r:container_file_t:s0:c123,c456"

	tests := []struct {
		name                  string
		featureGates          []featuregate.Feature // SELinuxMountReadWriteOncePod 始终启用
		pluginSupportsSELinux bool
		volume                *volume.Spec
		podSecurityContext    *v1.PodSecurityContext
		seLinuxOptions        []*v1.SELinuxOptions
		expectError           bool
		expectedInfo          SELinuxLabelInfo
	}{
		// 无标签场景
		{
			name:                  "no label, no changePolicy",
			featureGates:          nil,
			pluginSupportsSELinux: true,
			volume:                &volume.Spec{PersistentVolume: pvRWOP},
			podSecurityContext:    nil,
			seLinuxOptions:        nil,
			expectError:           false,
			expectedInfo: SELinuxLabelInfo{
				SELinuxMountLabel:                 "", // 无 SELinuxOptions，且默认策略为 recursive
				SELinuxProcessLabel:               "", // 无 SELinuxOptions
				PluginSupportsSELinuxContextMount: true,
			},
		},
		{
			name:                  "no label, Recursive change policy",
			featureGates:          nil,
			pluginSupportsSELinux: true,
			volume:                &volume.Spec{PersistentVolume: pvRWOP},
			podSecurityContext:    &v1.PodSecurityContext{SELinuxChangePolicy: ptr.To(v1.SELinuxChangePolicyRecursive)},
			seLinuxOptions:        nil,
			expectError:           false,
			expectedInfo: SELinuxLabelInfo{
				SELinuxMountLabel:                 "", // 无 SELinuxOptions，且策略为 recursive
				SELinuxProcessLabel:               "", // 无 SELinuxOptions
				PluginSupportsSELinuxContextMount: true,
			},
		},
		// 单个标签 + RWOP 卷场景
		{
			name:                  "one label, Recursive change policy, no feature gate",
			featureGates:          nil,
			pluginSupportsSELinux: true,
			volume:                &volume.Spec{PersistentVolume: pvRWOP},
			podSecurityContext:    &v1.PodSecurityContext{SELinuxChangePolicy: ptr.To(v1.SELinuxChangePolicyRecursive)},
			seLinuxOptions:        []*v1.SELinuxOptions{&seLinuxOpts1},
			expectError:           false,
			expectedInfo: SELinuxLabelInfo{
				SELinuxMountLabel:                 label1, // SELinuxChangePolicy 关闭时 recursive 策略不生效
				SELinuxProcessLabel:               label1, // Pod 已分配标签
				PluginSupportsSELinuxContextMount: true,
			},
		},
		{
			name:                  "one label, Recursive change policy, SELinuxChangePolicy",
			featureGates:          []featuregate.Feature{features.SELinuxChangePolicy},
			pluginSupportsSELinux: true,
			volume:                &volume.Spec{PersistentVolume: pvRWOP},
			podSecurityContext:    &v1.PodSecurityContext{SELinuxChangePolicy: ptr.To(v1.SELinuxChangePolicyRecursive)},
			seLinuxOptions:        []*v1.SELinuxOptions{&seLinuxOpts1},
			expectError:           false,
			expectedInfo: SELinuxLabelInfo{
				SELinuxMountLabel:                 "",     // SELinuxChangePolicy 开启时 recursive 策略生效，同样影响 RWOP。
				SELinuxProcessLabel:               label1, // Pod 已分配标签
				PluginSupportsSELinuxContextMount: true,
			},
		},
		{
			name:                  "one label, no policy",
			featureGates:          nil,
			pluginSupportsSELinux: true,
			volume:                &volume.Spec{PersistentVolume: pvRWOP},
			podSecurityContext:    nil,
			seLinuxOptions:        []*v1.SELinuxOptions{&seLinuxOpts1},
			expectError:           false,
			expectedInfo: SELinuxLabelInfo{
				SELinuxMountLabel:                 label1, // 默认策略为 MountOption
				SELinuxProcessLabel:               label1, // Pod 已分配标签
				PluginSupportsSELinuxContextMount: true,
			},
		},
		{
			name:                  "one label, MountOption policy",
			featureGates:          nil,
			pluginSupportsSELinux: true,
			volume:                &volume.Spec{PersistentVolume: pvRWOP},
			podSecurityContext:    &v1.PodSecurityContext{SELinuxChangePolicy: ptr.To(v1.SELinuxChangePolicyMountOption)},
			seLinuxOptions:        []*v1.SELinuxOptions{&seLinuxOpts1},
			expectError:           false,
			expectedInfo: SELinuxLabelInfo{
				SELinuxMountLabel:                 label1, // SELinuxChangePolicy 特性虽关闭，但默认策略本就是 MountOption
				SELinuxProcessLabel:               label1, // Pod 已分配标签
				PluginSupportsSELinuxContextMount: true,
			},
		},
		// RWX 卷场景
		{
			name:                  "one label, no policy, RWX",
			featureGates:          nil,
			pluginSupportsSELinux: true,
			volume:                &volume.Spec{PersistentVolume: pvRWX},
			podSecurityContext:    nil,
			seLinuxOptions:        []*v1.SELinuxOptions{&seLinuxOpts1},
			expectError:           false,
			expectedInfo: SELinuxLabelInfo{
				SELinuxMountLabel:                 label1, // GetMountSELinuxLabel() 不检查访问模式，交由调用方判断
				SELinuxProcessLabel:               label1, // Pod 已分配标签
				PluginSupportsSELinuxContextMount: true,
			},
		},
		{
			name:                  "one label, no policy, RWX, SELinuxChangePolicy",
			featureGates:          []featuregate.Feature{features.SELinuxChangePolicy},
			pluginSupportsSELinux: true,
			volume:                &volume.Spec{PersistentVolume: pvRWX},
			podSecurityContext:    nil,
			seLinuxOptions:        []*v1.SELinuxOptions{&seLinuxOpts1},
			expectError:           false,
			expectedInfo: SELinuxLabelInfo{
				SELinuxMountLabel:                 label1, //  GetMountSELinuxLabel() 不检查访问模式，交由调用方判断
				SELinuxProcessLabel:               label1, // Pod 已分配标签
				PluginSupportsSELinuxContextMount: true,
			},
		},
		{
			name:                  "one label, MountOption policy, RWX, SELinuxChangePolicy",
			featureGates:          []featuregate.Feature{features.SELinuxChangePolicy},
			pluginSupportsSELinux: true,
			volume:                &volume.Spec{PersistentVolume: pvRWX},
			podSecurityContext:    &v1.PodSecurityContext{SELinuxChangePolicy: ptr.To(v1.SELinuxChangePolicyMountOption)},
			seLinuxOptions:        []*v1.SELinuxOptions{&seLinuxOpts1},
			expectError:           false,
			expectedInfo: SELinuxLabelInfo{
				SELinuxMountLabel:                 label1, // GetMountSELinuxLabel() 不检查访问模式，交由调用方判断
				SELinuxProcessLabel:               label1, // Pod 已分配标签
				PluginSupportsSELinuxContextMount: true,
			},
		},
		{
			name:                  "one label, no policy, RWX, SELinuxMount",
			featureGates:          []featuregate.Feature{features.SELinuxChangePolicy, features.SELinuxMount},
			pluginSupportsSELinux: true,
			volume:                &volume.Spec{PersistentVolume: pvRWX},
			podSecurityContext:    nil,
			seLinuxOptions:        []*v1.SELinuxOptions{&seLinuxOpts1},
			expectError:           false,
			expectedInfo: SELinuxLabelInfo{
				SELinuxMountLabel:                 label1, // SELinuxMount 特性开启 + MountOption 策略
				SELinuxProcessLabel:               label1, // Pod 已分配标签
				PluginSupportsSELinuxContextMount: true,
			},
		},
		// 插件不支持 SELinux 场景
		{
			name:                  "one label, Recursive change policy, SELinuxChangePolicy, no plugin support",
			featureGates:          []featuregate.Feature{features.SELinuxChangePolicy},
			pluginSupportsSELinux: false,
			volume:                &volume.Spec{PersistentVolume: pvRWOP},
			podSecurityContext:    &v1.PodSecurityContext{SELinuxChangePolicy: ptr.To(v1.SELinuxChangePolicyRecursive)},
			seLinuxOptions:        []*v1.SELinuxOptions{&seLinuxOpts1},
			expectError:           false,
			expectedInfo: SELinuxLabelInfo{
				SELinuxMountLabel:                 "",     // 插件不支持
				SELinuxProcessLabel:               label1, // Pod 已分配标签
				PluginSupportsSELinuxContextMount: false,
			},
		},
		{
			name:                  "one label, no policy, no plugin support",
			featureGates:          nil,
			pluginSupportsSELinux: false,
			volume:                &volume.Spec{PersistentVolume: pvRWOP},
			podSecurityContext:    nil,
			seLinuxOptions:        []*v1.SELinuxOptions{&seLinuxOpts1},
			expectError:           false,
			expectedInfo: SELinuxLabelInfo{
				SELinuxMountLabel:                 "",     // 插件不支持
				SELinuxProcessLabel:               label1, // Pod 已分配标签
				PluginSupportsSELinuxContextMount: false,
			},
		},
		{
			name:                  "one label, MountOption policy, no plugin support",
			featureGates:          nil,
			pluginSupportsSELinux: false,
			volume:                &volume.Spec{PersistentVolume: pvRWOP},
			podSecurityContext:    &v1.PodSecurityContext{SELinuxChangePolicy: ptr.To(v1.SELinuxChangePolicyMountOption)},
			seLinuxOptions:        []*v1.SELinuxOptions{&seLinuxOpts1},
			expectError:           false,
			expectedInfo: SELinuxLabelInfo{
				SELinuxMountLabel:                 "",     // 插件不支持
				SELinuxProcessLabel:               label1, // Pod 已分配标签
				PluginSupportsSELinuxContextMount: false,
			},
		},
		// 边界场景
		{
			name:                  "multiple same labels, no policy",
			featureGates:          nil,
			pluginSupportsSELinux: true,
			volume:                &volume.Spec{PersistentVolume: pvRWOP},
			podSecurityContext:    nil,
			seLinuxOptions:        []*v1.SELinuxOptions{&seLinuxOpts1, &seLinuxOpts1, &seLinuxOpts1, &seLinuxOpts1},
			expectError:           false,
			expectedInfo: SELinuxLabelInfo{
				SELinuxMountLabel:                 label1, // 默认策略为 MountOption
				SELinuxProcessLabel:               label1, // Pod 已分配标签
				PluginSupportsSELinuxContextMount: true,
			},
		},
		// 错误场景
		{
			name:                  "multiple different labels, no policy",
			featureGates:          nil,
			pluginSupportsSELinux: true,
			volume:                &volume.Spec{PersistentVolume: pvRWOP},
			podSecurityContext:    nil,
			seLinuxOptions:        []*v1.SELinuxOptions{&seLinuxOpts1, &seLinuxOpts2, &seLinuxOpts3},
			expectError:           true,
			expectedInfo: SELinuxLabelInfo{
				SELinuxMountLabel:                 "",
				SELinuxProcessLabel:               "",
				PluginSupportsSELinuxContextMount: true,
			},
		},
		{
			name:                  "multiple different labels, Recursive policy",
			featureGates:          nil,
			pluginSupportsSELinux: true,
			volume:                &volume.Spec{PersistentVolume: pvRWOP},
			podSecurityContext:    &v1.PodSecurityContext{SELinuxChangePolicy: ptr.To(v1.SELinuxChangePolicyRecursive)},
			seLinuxOptions:        []*v1.SELinuxOptions{&seLinuxOpts1, &seLinuxOpts2, &seLinuxOpts3},
			expectError:           true,
			expectedInfo: SELinuxLabelInfo{
				SELinuxMountLabel:                 "",
				SELinuxProcessLabel:               "",
				PluginSupportsSELinuxContextMount: true,
			},
		},
	}

	// 将已 GA 的特性门控模拟版本降到 1.35，使测试中能够关闭它们。
	featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.35"))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 准备：按用例设置特性门控。
			allGates := []featuregate.Feature{features.SELinuxChangePolicy, features.SELinuxMount}
			enabledGates := sets.New(tt.featureGates...)
			for _, fg := range allGates {
				enable := enabledGates.Has(fg)
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, fg, enable)
			}
			seLinuxTranslator := NewFakeSELinuxLabelTranslator()
			pluginMgr, plugin := volumetesting.GetTestKubeletVolumePluginMgr(t)
			plugin.SupportsSELinux = tt.pluginSupportsSELinux

			// 执行：计算标签。
			info, err := GetMountSELinuxLabel(tt.volume, tt.seLinuxOptions, tt.podSecurityContext, pluginMgr, seLinuxTranslator)

			// 断言：校验错误与结果。
			if err != nil {
				if !tt.expectError {
					t.Errorf("GetMountSELinuxLabel() unexpected error: %v", err)
				}
				return
			}
			if tt.expectError {
				t.Errorf("GetMountSELinuxLabel() expected error, got none")
				return
			}

			if info != tt.expectedInfo {
				t.Errorf("GetMountSELinuxLabel() expected %+v, got %+v", tt.expectedInfo, info)
			}
		})
	}
}
