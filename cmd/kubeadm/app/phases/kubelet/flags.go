/*
Copyright 2018 The Kubernetes Authors.

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
	"fmt"
	"os"
	"path/filepath"
	"strings"

	nodeutil "k8s.io/component-helpers/node/util"
	"k8s.io/klog/v2"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	"k8s.io/kubernetes/cmd/kubeadm/app/constants"
	kubeadmutil "k8s.io/kubernetes/cmd/kubeadm/app/util"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

type kubeletFlagsOpts struct {
	nodeRegOpts              *kubeadmapi.NodeRegistrationOptions
	registerTaintsUsingFlags bool
}

// GetNodeNameAndHostname 按以下优先级（从低到高）获取节点名称：
// - 实际主机名
// - NodeRegistrationOptions.Name（等同于传给 "kubeadm init/join" 的 "--node-name"）
// - NodeRegistrationOptions.KubeletExtraArgs 中的 "hostname-override" 参数
// 同时返回主机名；获取主机名失败时返回错误。
func GetNodeNameAndHostname(cfg *kubeadmapi.NodeRegistrationOptions) (string, string, error) {
	hostname, err := nodeutil.GetHostname("")
	nodeName := hostname
	if cfg.Name != "" {
		nodeName = cfg.Name
	}
	if name, idx := kubeadmapi.GetArgValue(cfg.KubeletExtraArgs, "hostname-override", -1); idx > -1 {
		nodeName = name
	}
	return nodeName, hostname, err
}

// WriteKubeletDynamicEnvFile 向 kubelet 写入包含动态参数的环境文件。
// 在 "kubeadm init" 和 "kubeadm join" 时使用。
func WriteKubeletDynamicEnvFile(cfg *kubeadmapi.ClusterConfiguration, nodeReg *kubeadmapi.NodeRegistrationOptions, registerTaintsUsingFlags bool, kubeletDir string) error {
	flagOpts := kubeletFlagsOpts{
		nodeRegOpts:              nodeReg,
		registerTaintsUsingFlags: registerTaintsUsingFlags,
	}

	stringMap := buildKubeletArgs(flagOpts)
	return WriteKubeletArgsToFile(stringMap, nodeReg.KubeletExtraArgs, kubeletDir)
}

// WriteKubeletArgsToFile 将合并后的 kubelet 参数写入 kubeletDir 中的 KubeletEnvFile 文件。
func WriteKubeletArgsToFile(kubeletFlags, overridesFlags []kubeadmapi.Arg, kubeletDir string) error {
	argList := kubeadmutil.ArgumentsToCommand(kubeletFlags, overridesFlags)
	envFileContent := fmt.Sprintf("%s=%q\n", constants.KubeletEnvFileVariableName, strings.Join(argList, " "))

	return writeKubeletFlagBytesToDisk([]byte(envFileContent), kubeletDir)
}

// buildKubeletArgsCommon 根据 kubeletFlagsOpts 构建 Linux 和 Windows 通用的参数切片。
func buildKubeletArgsCommon(opts kubeletFlagsOpts) []kubeadmapi.Arg {
	kubeletFlags := []kubeadmapi.Arg{}

	if opts.registerTaintsUsingFlags && opts.nodeRegOpts.Taints != nil && len(opts.nodeRegOpts.Taints) > 0 {
		taintStrs := []string{}
		for _, taint := range opts.nodeRegOpts.Taints {
			taintStrs = append(taintStrs, taint.ToString())
		}
		kubeletFlags = append(kubeletFlags, kubeadmapi.Arg{Name: "register-with-taints", Value: strings.Join(taintStrs, ",")})
	}

	// 仅当 "--hostname-override" 与主机名不同时，才将其传给 kubelet。
	nodeName, hostname, err := GetNodeNameAndHostname(opts.nodeRegOpts)
	if err != nil {
		klog.Warning(err)
	}
	if nodeName != hostname {
		klog.V(1).Infof("setting kubelet hostname-override to %q", nodeName)
		kubeletFlags = append(kubeletFlags, kubeadmapi.Arg{Name: "hostname-override", Value: nodeName})
	}

	return kubeletFlags
}

// writeKubeletFlagBytesToDisk 将字节切片写入 kubelet 参数覆盖文件的指定位置。
func writeKubeletFlagBytesToDisk(b []byte, kubeletDir string) error {
	kubeletEnvFilePath := filepath.Join(kubeletDir, constants.KubeletEnvFileName)
	fmt.Printf("[kubelet-start] Writing kubelet environment file with flags to file %q\n", kubeletEnvFilePath)

	// 若目标目录不存在，则创建。
	if err := os.MkdirAll(kubeletDir, 0700); err != nil {
		return errors.Wrapf(err, "failed to create directory %q", kubeletDir)
	}
	if err := os.WriteFile(kubeletEnvFilePath, b, 0644); err != nil {
		return errors.Wrapf(err, "failed to write kubelet configuration to the file %q", kubeletEnvFilePath)
	}
	return nil
}

// buildKubeletArgs 根据 kubeletFlagsOpts 构建应传给本地 kubelet 守护进程的参数切片。
func buildKubeletArgs(opts kubeletFlagsOpts) []kubeadmapi.Arg {
	return buildKubeletArgsCommon(opts)
}

// ReadKubeletDynamicEnvFile 读取 kubelet 动态环境参数文件，并返回字符串切片。
func ReadKubeletDynamicEnvFile(kubeletEnvFilePath string) ([]string, error) {
	data, err := os.ReadFile(kubeletEnvFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	// 去除内容两端的空白。
	content := strings.TrimSpace(string(data))

	// 检查内容是否以预期的 kubelet 环境变量前缀开头。
	const prefix = constants.KubeletEnvFileVariableName + "="
	if !strings.HasPrefix(content, prefix) {
		return nil, errors.Errorf("the file %q does not contain the  expected prefix %q", kubeletEnvFilePath, prefix)
	}

	// 去除前缀和两端的双引号。
	flags := strings.TrimPrefix(content, prefix)
	flags = strings.Trim(flags, `"`)

	// 按空白拆分参数，得到各个独立参数。
	trimmedFlags := strings.Fields(flags)
	if len(trimmedFlags) == 0 {
		return nil, nil
	}

	var updatedFlags []string
	for i := 0; i < len(trimmedFlags); i++ {
		flag := trimmedFlags[i]
		if strings.Contains(flag, "=") {
			// 若参数包含 "="，则直接添加。
			updatedFlags = append(updatedFlags, flag)
		} else if i+1 < len(trimmedFlags) {
			// 若未找到 "="，则将参数与下一项组合为其值。
			combinedFlag := flag + "=" + trimmedFlags[i+1]
			updatedFlags = append(updatedFlags, combinedFlag)
			// 跳过已用作值的下一项。
			i++
		}
	}

	return updatedFlags, nil
}
