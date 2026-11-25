//go:build windows

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
	"os"
	"path/filepath"
	"strings"

	"k8s.io/klog/v2"
	kubeletconfig "k8s.io/kubelet/config/v1beta1"
	"k8s.io/utils/ptr"

	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

// Mutate 将 KubeletConfiguration 中的绝对路径字段改为 Windows 兼容的绝对路径。
func (kc *kubeletConfig) Mutate() error {
	// 在 Windows 节点上，`kubeadm join` 从集群下载的 KubeletConfiguration
	// 可能包含不带盘符的绝对路径，因为配置可能生成于 Linux 控制平面节点。
	// Windows 上 Go 的 filepath.IsAbs() 仅在路径包含盘符时返回 true。
	// 这会使 client-go 和 kubelet 出错，并在 Windows 节点上引发问题。
	// 在 client-go 或 kubelet 中修复会破坏依赖相对路径的现有 Windows
	// 用户的兼容性：
	//   https://github.com/kubernetes/kubernetes/pull/77710#issuecomment-491989621
	//
	// 因此，这里通过调整 KubeletConfiguration 路径来兼容 Windows。
	// 注意：此处理目前绑定 KubeletConfiguration v1beta1。
	klog.V(2).Infoln("[componentconfig] Adapting the paths in the KubeletConfiguration for Windows...")

	// 获取调用 kubeadm 二进制文件时所在的盘符。
	exe, err := os.Executable()
	if err != nil {
		return errors.Wrap(err, "could not obtain information about the kubeadm executable")
	}
	drive := filepath.VolumeName(filepath.Dir(exe))
	klog.V(2).Infof("[componentconfig] Assuming Windows drive %q", drive)

	// 修改配置中的路径。
	mutatePaths(&kc.config, drive)
	return nil
}

func mutatePaths(cfg *kubeletconfig.KubeletConfiguration, drive string) {
	mutateStringField := func(name string, field *string) {
		// filepath.IsAbs() 在 Windows 运行时中不可靠，因此改为检查路径是否
		// 以 "/" 开头。这表示路径来自 Unix 节点，是绝对路径。
		if !strings.HasPrefix(*field, "/") {
			return
		}
		// 在路径前添加盘符并更新字段。
		*field = filepath.Join(drive, *field)
		klog.V(2).Infof("[componentconfig] kubelet/Windows: adapted path for field %q to %q", name, *field)
	}

	// 修改需要处理的字段。
	klog.V(2).Infof("[componentconfig] kubelet/Windows: changing field \"resolverConfig\" to empty")
	cfg.ResolverConfig = ptr.To("")
	mutateStringField("staticPodPath", &cfg.StaticPodPath)
	mutateStringField("authentication.x509.clientCAFile", &cfg.Authentication.X509.ClientCAFile)
}
