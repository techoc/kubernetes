//go:build linux

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

package kubelet

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	"k8s.io/kubernetes/pkg/kubelet/cm/util"
	"k8s.io/kubernetes/pkg/kubelet/events"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
)

// cgroupVersionCheck 检查节点的 cgroup 版本：v1 已废弃并返回错误，
// v2 下若缺少 cpu.stat 则提示内核配置问题。
func (kl *Kubelet) cgroupVersionCheck() error {
	cgroupVersion := kl.containerManager.GetNodeConfig().CgroupVersion
	metrics.CgroupVersion.Set(float64(cgroupVersion))
	switch cgroupVersion {
	case 1:
		// v1 已不推荐使用，发出事件并返回错误提示用户迁移。
		kl.recorder.Eventf(kl.nodeRefWithUID(), v1.EventTypeWarning, events.CgroupV1, cm.CgroupV1DeprecatedWarning)
		return errors.New(cm.CgroupV1DeprecatedWarning)
	case 2:
		cpustat := filepath.Join(util.CgroupRoot, "cpu.stat")
		if _, err := os.Stat(cpustat); os.IsNotExist(err) {
			// cpu.stat 缺失通常意味着内核未启用 CPU 统计，资源管理功能会受影响。
			return errors.New(cm.CgroupV2KernelWarning)
		}
	default:
		return fmt.Errorf("unsupported cgroup version: %d", cgroupVersion)
	}
	return nil
}
