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
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"k8s.io/klog/v2"

	"k8s.io/kubernetes/test/e2e_node/builder"
	"k8s.io/kubernetes/test/utils"
)

// ConformanceRemote 包含 node 一致性测试套件中的特定函数。
type ConformanceRemote struct{}

func init() {
	RegisterTestSuite("conformance", &ConformanceRemote{})
}

// getConformanceDirectory 获取 node 一致性测试的 build 目录。
func getConformanceDirectory() (string, error) {
	k8sRoot, err := utils.GetK8sRootDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(k8sRoot, "test", "e2e_node", "conformance", "build"), nil
}

// commandToString 是把 command 格式化为字符串的辅助函数。
func commandToString(c *exec.Cmd) string {
	return strings.Join(append([]string{c.Path}, c.Args[1:]...), " ")
}

// 镜像路径常量。
const (
	conformanceRegistry         = "registry.k8s.io"
	conformanceArch             = runtime.GOARCH
	conformanceTarfile          = "node_conformance.tar"
	conformanceTestBinary       = "e2e_node.test"
	conformanceImageLoadTimeout = time.Duration(30) * time.Second
)

// timestamp 用作当前测试的唯一 id。
var timestamp = getTimestamp()

// getConformanceTestImageName 根据 system spec 名称返回一致性测试镜像名称。
func getConformanceTestImageName(systemSpecName string) string {
	if systemSpecName == "" {
		return fmt.Sprintf("%s/node-test-%s:%s", conformanceRegistry, conformanceArch, timestamp)
	}
	return fmt.Sprintf("%s/node-test-%s-%s:%s", conformanceRegistry, systemSpecName, conformanceArch, timestamp)
}

// buildConformanceTest 把 node 一致性测试镜像 tarball 构建到 binDir。
func buildConformanceTest(binDir, systemSpecName string) error {
	// 获取 node 一致性测试目录。
	conformancePath, err := getConformanceDirectory()
	if err != nil {
		return fmt.Errorf("failed to get node conformance directory: %w", err)
	}
	// 构建 docker 镜像。
	cmd := exec.Command("make", "-C", conformancePath, "BIN_DIR="+binDir,
		"REGISTRY="+conformanceRegistry,
		"ARCH="+conformanceArch,
		"VERSION="+timestamp,
		"SYSTEM_SPEC_NAME="+systemSpecName)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to build node conformance docker image: command - %q, error - %v, output - %q",
			commandToString(cmd), err, output)
	}
	// 把 docker 镜像保存为 tar 文件。
	cmd = exec.Command("docker", "save", "-o", filepath.Join(binDir, conformanceTarfile), getConformanceTestImageName(systemSpecName))
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("failed to save node conformance docker image into tar file: command - %q, error - %v, output - %q",
			commandToString(cmd), err, output)
	}
	return nil
}

// SetupTestPackage 用 node 一致性测试所需的 k8s 二进制文件设置测试包
func (c *ConformanceRemote) SetupTestPackage(tardir, systemSpecName string) error {
	// 构建可执行文件
	if err := builder.BuildGo(); err != nil {
		return fmt.Errorf("failed to build the dependencies: %w", err)
	}

	// 确保能找到新构建的二进制文件
	buildOutputDir, err := utils.GetK8sBuildOutputDir(builder.IsDockerizedBuild(), builder.GetTargetBuildArch())
	if err != nil {
		return fmt.Errorf("failed to locate kubernetes build output directory %v", err)
	}

	// 构建 node 一致性测试 tarball。
	if err := buildConformanceTest(buildOutputDir, systemSpecName); err != nil {
		return fmt.Errorf("failed to build node conformance test: %w", err)
	}

	// 复制文件
	requiredFiles := []string{"kubelet", conformanceTestBinary, conformanceTarfile}
	for _, file := range requiredFiles {
		source := filepath.Join(buildOutputDir, file)
		if _, err := os.Stat(source); err != nil {
			return fmt.Errorf("failed to locate test file %s: %w", file, err)
		}
		output, err := exec.Command("cp", source, filepath.Join(tardir, file)).CombinedOutput()
		if err != nil {
			return fmt.Errorf("failed to copy %q: error - %v output - %q", file, err, output)
		}
	}

	return nil
}

// loadConformanceImage 从 tar 文件加载 node 一致性测试镜像。
func loadConformanceImage(host, workspace string) error {
	klog.Info("Loading conformance image from tarfile")
	tarfile := filepath.Join(workspace, conformanceTarfile)
	if output, err := SSH(host, "timeout", conformanceImageLoadTimeout.String(),
		"docker", "load", "-i", tarfile); err != nil {
		return fmt.Errorf("failed to load node conformance image from tar file %q: error - %v output - %q",
			tarfile, err, output)
	}
	return nil
}

// kubeletLauncherLog 是 kubelet launcher 的日志。
const kubeletLauncherLog = "kubelet-launcher.log"

// kubeletPodPath 是固定的已知 pod spec 路径。不能用
// e2e_node.test 中随机生成的 pod manifest 目录，因为需要把该目录挂载到
// 一致性测试容器中，已知目录更方便。
// TODO(random-liu): 切换到 cluster e2e node bootstrap 脚本后去掉它。
var kubeletPodPath = "conformance-pod-manifest-" + timestamp

// getPodPath 返回 pod manifest 的完整路径。
func getPodPath(workspace string) string {
	return filepath.Join(workspace, kubeletPodPath)
}

// isSystemd 返回节点是否是 systemd 节点。
func isSystemd(host string) (bool, error) {
	// 找到 /run/systemd/system 则返回 "systemd"，否则返回空字符串。
	output, err := SSH(host, "test", "-e", "/run/systemd/system", "&&", "echo", "systemd", "||", "true")
	if err != nil {
		return false, fmt.Errorf("failed to check systemd: error - %v output - %q", err, output)
	}
	return strings.TrimSpace(output) != "", nil
}

// launchKubelet 通过以 run-kubelet-mode 运行 e2e_node.test 二进制来启动 kubelet。
// 这是临时方案，应让 node e2e 与 cluster e2e 使用相同的 node bootstrap，
// 并在常规 node e2e 和 node 一致性测试中都在测试之外
// 启动 kubelet。
// TODO(random-liu): 改用标准的 node bootstrap 脚本。
func launchKubelet(host, workspace, results, testArgs, bearerToken string) error {
	podManifestPath := getPodPath(workspace)
	if output, err := SSH(host, "mkdir", podManifestPath); err != nil {
		return fmt.Errorf("failed to create kubelet pod manifest path %q: error - %v output - %q",
			podManifestPath, err, output)
	}
	startKubeletCmd := fmt.Sprintf("./%s --run-kubelet-mode --node-name=%s"+
		" --bearer-token=%s"+
		" --report-dir=%s %s --kubelet-flags=--pod-manifest-path=%s > %s 2>&1",
		conformanceTestBinary, host, bearerToken, results, testArgs, podManifestPath, filepath.Join(results, kubeletLauncherLog))
	var cmd []string
	systemd, err := isSystemd(host)
	if err != nil {
		return fmt.Errorf("failed to check systemd: %w", err)
	}
	if systemd {
		cmd = []string{
			"systemd-run", "sh", "-c", getSSHCommand(" && ",
				// 切换到 workspace。
				fmt.Sprintf("cd %s", workspace),
				// 以 run-kubelet-mode 运行 e2e_node.test 启动 kubelet。
				startKubeletCmd,
			),
		}
	} else {
		cmd = []string{
			"sh", "-c", getSSHCommand(" && ",
				// 切换到 workspace。
				fmt.Sprintf("cd %s", workspace),
				// 用 nohup 以 run-kubelet-mode 运行 e2e_node.test 启动 kubelet。
				fmt.Sprintf("(nohup %s &)", startKubeletCmd),
			),
		}
	}
	klog.V(2).Infof("Launch kubelet with command: %v", cmd)
	output, err := SSH(host, cmd...)
	if err != nil {
		return fmt.Errorf("failed to launch kubelet with command %v: error - %v output - %q",
			cmd, err, output)
	}
	klog.Info("Successfully launch kubelet")
	return nil
}

// kubeletStopGracePeriod 是强制杀死 kubelet 前等待的 grace period。
const kubeletStopGracePeriod = 10 * time.Second

// stopKubelet 优雅地停止 kubelet launcher 和 kubelet。
func stopKubelet(host, workspace string) error {
	klog.Info("Gracefully stop kubelet launcher")
	if output, err := SSH(host, "pkill", conformanceTestBinary); err != nil {
		return fmt.Errorf("failed to gracefully stop kubelet launcher: error - %v output - %q",
			err, output)
	}
	klog.Info("Wait for kubelet launcher to stop")
	stopped := false
	for start := time.Now(); time.Since(start) < kubeletStopGracePeriod; time.Sleep(time.Second) {
		// 检查进程是否仍在运行。
		output, err := SSH(host, "pidof", conformanceTestBinary, "||", "true")
		if err != nil {
			return fmt.Errorf("failed to check kubelet stopping: error - %v output -%q",
				err, output)
		}
		// Kubelet 已停止
		if strings.TrimSpace(output) == "" {
			stopped = true
			break
		}
	}
	if !stopped {
		klog.Info("Forcibly stop kubelet")
		if output, err := SSH(host, "pkill", "-SIGKILL", conformanceTestBinary); err != nil {
			return fmt.Errorf("failed to forcibly stop kubelet: error - %v output - %q",
				err, output)
		}
	}
	klog.Info("Successfully stop kubelet")
	// 清理 pod manifest 路径
	podManifestPath := getPodPath(workspace)
	if output, err := SSH(host, "rm", "-f", filepath.Join(workspace, podManifestPath)); err != nil {
		return fmt.Errorf("failed to cleanup pod manifest directory %q: error - %v, output - %q",
			podManifestPath, err, output)
	}
	return nil
}

// RunTest 在节点上运行测试。
func (c *ConformanceRemote) RunTest(host, workspace, results, imageDesc, junitFilePrefix, testArgs, _, systemSpecName, extraEnvs, _ string, timeout time.Duration) (string, error) {
	// 安装 cni 插件并添加基础 CNI 配置。
	if err := setupCNI(host, workspace); err != nil {
		return "", err
	}

	// 配置 iptables 防火墙规则。
	if err := configureFirewall(host); err != nil {
		return "", err
	}

	// 杀死任何正在运行的 node 进程。
	cleanupNodeProcesses(host)

	// 加载 node 一致性测试镜像。
	if err := loadConformanceImage(host, workspace); err != nil {
		return "", err
	}

	bearerToken, err := generateSecureToken(16)
	if err != nil {
		return "", err
	}

	// 启动 kubelet。
	if err := launchKubelet(host, workspace, results, testArgs, bearerToken); err != nil {
		return "", err
	}
	// 停止 kubelet。
	defer func() {
		if err := stopKubelet(host, workspace); err != nil {
			// 只在停止 kubelet 失败时记录错误，因为这并不关键。
			klog.Errorf("failed to stop kubelet: %v", err)
		}
	}()

	// 运行测试
	klog.V(2).Infof("Starting tests on %q", host)
	podManifestPath := getPodPath(workspace)
	cmd := fmt.Sprintf("'timeout -k 30s %fs docker run --rm --privileged=true --net=host -v /:/rootfs -v %s:%s -v %s:/var/result -e TEST_ARGS=--report-prefix=%s -e EXTRA_ENVS=%s -e TEST_ARGS=--bearer-token=%s %s'",
		timeout.Seconds(), podManifestPath, podManifestPath, results, junitFilePrefix, extraEnvs, bearerToken, getConformanceTestImageName(systemSpecName))
	return SSH(host, "sh", "-c", cmd)
}

// generateSecureToken 返回一个长度为 tokenLen 的字符串，
// 由随机字节经 base64 编码组成，用作与 APIServer
// 通信时的 Bearer Token
func generateSecureToken(tokenLen int) (string, error) {
	// base64 编码后字节数应等于 tokenLen。
	tokenSize := math.Ceil(float64(tokenLen) * 6 / 8)
	rawToken := make([]byte, int(tokenSize))
	if _, err := rand.Read(rawToken); err != nil {
		return "", err
	}
	encoded := base64.RawURLEncoding.EncodeToString(rawToken)
	token := encoded[:tokenLen]
	return token, nil
}
