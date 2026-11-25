/*
Copyright 2016 The Kubernetes Authors.

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

package remote

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"k8s.io/klog/v2"

	"k8s.io/kubernetes/test/e2e_node/builder"
	"k8s.io/kubernetes/test/e2e_node/system"
	"k8s.io/kubernetes/test/utils"
)

// NodeE2ERemote 包含 node e2e 测试套件中的特定函数。
type NodeE2ERemote struct{}

// init 初始化 node e2e 测试套件。
func init() {
	RegisterTestSuite("default", &NodeE2ERemote{})
}

var ginkgoBin = CommandLine.String("ginkgo-binary", "", "Existing Ginkgo binary to be used on the target instead of building from source")
var kubeletBin = CommandLine.String("kubelet-binary", "", "Existing kubelet binary to be used on the target instead of building from source")
var e2eNodeBin = CommandLine.String("e2e-node-binary", "", "Existing e2e-node.test binary to be used on the target instead of building from source")

// SetupTestPackage 为测试包准备 node e2e 测试所需的 k8s 二进制文件
func (n *NodeE2ERemote) SetupTestPackage(tardir, systemSpecName string) error {
	requiredBins := map[string]struct {
		file   string
		target string
	}{
		"ginkgo":        {*ginkgoBin, "github.com/onsi/ginkgo/v2/ginkgo"},
		"kubelet":       {*kubeletBin, "cmd/kubelet"},
		"e2e_node.test": {*e2eNodeBin, "test/e2e_node/e2e_node.test"},
	}

	// 只构建尚无二进制文件的目标。
	var targets []string
	for _, entry := range requiredBins {
		if entry.file == "" {
			targets = append(targets, entry.target)
		}
	}
	if len(targets) > 0 {
		// 构建下面所需的缺失可执行文件。
		if err := builder.BuildTargets(targets...); err != nil {
			return fmt.Errorf("failed to build the dependencies: %w", err)
		}

		// 确保能找到新构建的二进制文件
		buildOutputDir, err := utils.GetK8sBuildOutputDir(builder.IsDockerizedBuild(), builder.GetTargetBuildArch())
		if err != nil {
			return fmt.Errorf("failed to locate kubernetes build output directory: %w", err)
		}
		for bin, entry := range requiredBins {
			if entry.file == "" {
				entry.file = filepath.Join(buildOutputDir, bin)
			}
			requiredBins[bin] = entry
		}
	}

	rootDir, err := utils.GetK8sRootDir()
	if err != nil {
		return fmt.Errorf("failed to locate kubernetes root directory: %w", err)
	}

	// 复制二进制文件
	for bin, entry := range requiredBins {
		source := entry.file
		klog.V(2).Infof("Copying binaries from %s", source)
		if _, err := os.Stat(source); err != nil {
			return fmt.Errorf("failed to locate test binary %s: %w", bin, err)
		}
		out, err := exec.Command("cp", source, filepath.Join(tardir, bin)).CombinedOutput()
		if err != nil {
			return fmt.Errorf("failed to copy %q: %v Output: %q", bin, err, out)
		}
	}

	// 通过这些符号链接调用 e2e_node.test 时，它的行为类似
	// 这些独立的二进制文件。
	e2eNodeBinary := "e2e_node.test"
	for _, alias := range []string{"mounter", "gcp-credential-provider"} {
		symlink := filepath.Join(tardir, alias)
		klog.V(2).Infof("Creating symlink %s -> %s", symlink, e2eNodeBinary)
		if err := os.Symlink(e2eNodeBinary, symlink); err != nil {
			return fmt.Errorf("failed to create symlink %q: %w", alias, err)
		}
	}

	// 创建 gcp-credential-provider 二进制文件的符号链接以用于测试
	// 供 credential provider 使用的 service account token。
	// feature-gate: KubeletServiceAccountTokenForCredentialProviders=true
	binary := "gcp-credential-provider" // 使用相对路径而非完整路径
	symlink := filepath.Join(tardir, "gcp-credential-provider-with-sa")
	if _, err := os.Lstat(symlink); err == nil {
		if err := os.Remove(symlink); err != nil {
			return fmt.Errorf("failed to remove symlink %q: %w", symlink, err)
		}
	}
	klog.V(2).Infof("Creating symlink %s -> %s", symlink, binary)
	if err := os.Symlink(binary, symlink); err != nil {
		return fmt.Errorf("failed to create symlink %q: %w", symlink, err)
	}

	if systemSpecName != "" {
		// 复制系统规格文件
		source := filepath.Join(rootDir, system.SystemSpecPath, systemSpecName+".yaml")
		if _, err := os.Stat(source); err != nil {
			return fmt.Errorf("failed to locate system spec %q: %w", source, err)
		}
		out, err := exec.Command("cp", source, tardir).CombinedOutput()
		if err != nil {
			return fmt.Errorf("failed to copy system spec %q: %v, output: %q", source, err, out)
		}
	}

	return nil
}

// prependMemcgNotificationFlag 将启用 memcg 通知的 flag
// 前置到 args 并返回结果。
func prependMemcgNotificationFlag(args string) string {
	return "--kubelet-flags=--kernel-memcg-notification=true " + args
}

// prependCredentialProviderFlag 将启用 credential provider 插件的
// flag 前置。
func prependCredentialProviderFlag(args, workspace string) string {
	credentialProviderConfig := filepath.Join(workspace, "credential-provider.yaml")
	featureGateFlag := "--kubelet-flags=--feature-gates=KubeletServiceAccountTokenForCredentialProviders=true,KubeletEnsureSecretPulledImages=true"
	configFlag := fmt.Sprintf("--kubelet-flags=--image-credential-provider-config=%s", credentialProviderConfig)
	binFlag := fmt.Sprintf("--kubelet-flags=--image-credential-provider-bin-dir=%s", workspace)
	return fmt.Sprintf("%s %s %s %s", featureGateFlag, configFlag, binFlag, args)
}

// osSpecificActions 执行节点测试所需的特定于 OS 的操作
func osSpecificActions(args, host, workspace string) (string, error) {
	output, err := getOSDistribution(host)
	if err != nil {
		return "", fmt.Errorf("issue detecting node's OS via node's /etc/os-release. Err: %v, Output:\n%s", err, output)
	}
	switch {
	case strings.Contains(output, "fedora"), strings.Contains(output, "rhcos"),
		strings.Contains(output, "centos"), strings.Contains(output, "rhel"):
		return args, setKubeletSELinuxLabels(host, workspace)
	case strings.Contains(output, "gci"), strings.Contains(output, "cos"):
		args = prependMemcgNotificationFlag(args)
		return prependCredentialProviderFlag(args, workspace), nil
	case strings.Contains(output, "ubuntu"):
		args = prependCredentialProviderFlag(args, workspace)
		return prependMemcgNotificationFlag(args), nil
	case strings.Contains(output, "amzn"):
		args = prependCredentialProviderFlag(args, workspace)
		return prependMemcgNotificationFlag(args), nil
	}
	return args, nil
}

// setKubeletSELinuxLabels 为 Fedora CoreOS 发行版上的 kubelet
// 设置适当的 SELinux 标签
func setKubeletSELinuxLabels(host, workspace string) error {
	cmd := getSSHCommand(" && ",
		fmt.Sprintf("/usr/bin/chcon -u system_u -r object_r -t kubelet_exec_t %s", filepath.Join(workspace, "kubelet")),
		fmt.Sprintf("/usr/bin/chcon -u system_u -r object_r -t bin_t %s", filepath.Join(workspace, "e2e_node.test")),
		fmt.Sprintf("/usr/bin/chcon -u system_u -r object_r -t bin_t %s", filepath.Join(workspace, "ginkgo")),
		fmt.Sprintf("/usr/bin/chcon -u system_u -r object_r -t bin_t %s", filepath.Join(workspace, "mounter")),
		fmt.Sprintf("/usr/bin/chcon -R -u system_u -r object_r -t bin_t %s", filepath.Join(workspace, "cni", "bin/")),
	)
	output, err := SSH(host, "sh", "-c", cmd)
	if err != nil {
		return fmt.Errorf("Unable to apply SELinux labels. Err: %v, Output:\n%s", err, output)
	}
	return nil
}

func getOSDistribution(host string) (string, error) {
	output, err := SSH(host, "cat", "/etc/os-release")
	if err != nil {
		return "", fmt.Errorf("issue detecting node's OS via node's /etc/os-release. Err: %v, Output:\n%s", err, output)
	}

	var re = regexp.MustCompile(`(?m)^ID="?(\w+)"?`)
	subMatch := re.FindStringSubmatch(output)
	if len(subMatch) > 0 {
		return subMatch[1], nil
	}

	return "", fmt.Errorf("Unable to parse os-release for the host, %s", host)
}

// RunTest 在节点上运行测试。
func (n *NodeE2ERemote) RunTest(host, workspace, results, imageDesc, junitFilePrefix, testArgs, ginkgoArgs, systemSpecName, extraEnvs, runtimeConfig string, timeout time.Duration) (string, error) {
	// 安装 CNI 插件并添加基本的 CNI 配置。
	// TODO(random-liu): 移除 containervm 测试后，在 cloud init 中执行此操作。
	if err := setupCNI(host, workspace); err != nil {
		return "", err
	}

	// 配置 iptables 防火墙规则
	if err := configureFirewall(host); err != nil {
		return "", err
	}

	// 安装 kubelet credential provider 插件
	if err := configureCredentialProvider(host, workspace); err != nil {
		return "", err
	}

	// 终止所有正在运行的节点进程
	cleanupNodeProcesses(host)

	testArgs, err := osSpecificActions(testArgs, host, workspace)
	if err != nil {
		return "", err
	}

	systemSpecFile := ""
	if systemSpecName != "" {
		systemSpecFile = systemSpecName + ".yaml"
	}

	outputGinkgoFile := filepath.Join(results, fmt.Sprintf("%s-ginkgo.log", host))

	// 运行测试
	klog.V(2).Infof("Starting tests on %q", host)
	cmd := getSSHCommand(" && ",
		fmt.Sprintf("cd %s", workspace),
		// 注意，这里需要 set -o pipefail，以确保返回 ginkgo 的正确退出码，而非 tee 的
		fmt.Sprintf("set -o pipefail; timeout -k 30s %fs ./ginkgo %s ./e2e_node.test -- --system-spec-name=%s --system-spec-file=%s --extra-envs=%s --runtime-config=%s --v 4 --node-name=%s --report-dir=%s --report-prefix=%s --image-description=\"%s\" %s 2>&1 | tee -i %s",
			timeout.Seconds(), ginkgoArgs, systemSpecName, systemSpecFile, extraEnvs, runtimeConfig, host, results, junitFilePrefix, imageDesc, testArgs, outputGinkgoFile),
	)
	return SSH(host, "/bin/bash", "-c", cmd)
}
