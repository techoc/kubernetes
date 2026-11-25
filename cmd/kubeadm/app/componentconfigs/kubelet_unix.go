//go:build !windows

/*
Copyright 2021 The Kubernetes Authors.

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

package componentconfigs

import (
	"k8s.io/klog/v2"
	kubeletconfig "k8s.io/kubelet/config/v1beta1"
	"k8s.io/utils/ptr"

	"k8s.io/kubernetes/cmd/kubeadm/app/util/initsystem"
)

// Mutate 在配置序列化前应用预定义的修改。
func (kc *kubeletConfig) Mutate() error {
	if err := mutateResolverConfig(&kc.config, isServiceActive); err != nil {
		return err
	}
	return nil
}

// mutateResolverConfig 动态修改 kubeletConfig 中的 ResolverConfig。
func mutateResolverConfig(cfg *kubeletconfig.KubeletConfiguration, isServiceActiveFunc func(string) (bool, error)) error {
	ok, err := isServiceActiveFunc("systemd-resolved")
	if err != nil {
		klog.Warningf("cannot determine if systemd-resolved is active: %v", err)
	}
	if ok {
		if cfg.ResolverConfig == nil {
			cfg.ResolverConfig = ptr.To(kubeletSystemdResolverConfig)
		} else if *cfg.ResolverConfig != kubeletSystemdResolverConfig {
			warnDefaultComponentConfigValue("KubeletConfiguration", "resolvConf",
				kubeletSystemdResolverConfig, *cfg.ResolverConfig)
		}

	}
	return nil
}

// isServiceActive 检查给定服务是否存在且正在运行。
func isServiceActive(name string) (bool, error) {
	initSystem, err := initsystem.GetInitSystem()
	if err != nil {
		return false, err
	}
	return initSystem.ServiceIsActive(name), nil
}
