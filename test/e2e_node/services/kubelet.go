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

package services

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/klog/v2"
	kubeletconfigv1beta1 "k8s.io/kubelet/config/v1beta1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/cmd/kubelet/app/options"
	"k8s.io/kubernetes/pkg/cluster/ports"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/kubeletconfig/configfiles"
	kubeletconfigcodec "k8s.io/kubernetes/pkg/kubelet/kubeletconfig/util/codec"
	utilfs "k8s.io/kubernetes/pkg/util/filesystem"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/e2e_node/builder"
	"k8s.io/kubernetes/test/e2e_node/remote"
)

// TODO(random-liu): 用标准的 kubelet launcher 替换这里

// args 用于累积同名 flag 的参数
type args []string

// String 实现 flag.Value 的 String 方法
func (a *args) String() string {
	return fmt.Sprint(*a)
}

// Set 实现 flag.Value 的 Set 方法
func (a *args) Set(value string) error {
	// 这里假设 flag 字符串中的所有空白都是字段分隔符
	na := strings.Fields(value)
	*a = append(*a, na...)
	return nil
}

// kubeletArgs 是测试运行器指定的 kubelet 参数覆盖项
var kubeletArgs args
var kubeletConfigFile = "./kubeletconfig.yaml"

func init() {
	flag.Var(&kubeletArgs, "kubelet-flags", "Kubelet flags passed to kubelet, this will override default kubelet flags in the test. Flags specified in multiple kubelet-flags will be concatenate. Deprecated, see: --kubelet-config-file.")
	if flag.Lookup("kubelet-config-file") == nil {
		flag.StringVar(&kubeletConfigFile, "kubelet-config-file", kubeletConfigFile, "The base KubeletConfiguration to use when setting up the kubelet. This configuration will then be minimially modified to support requirements from the test suite.")
	}
}

// RunKubelet 启动 kubelet 并等待终止信号；收到信号后优雅停止 kubelet
func RunKubelet(ctx context.Context, featureGates map[string]bool) {
	var err error
	// 启用 monitorParent，确保测试进程退出时 kubelet 能收到终止信号
	e := NewE2EServices(true /* monitorParent */)
	defer e.Stop()
	e.kubelet, err = e.startKubelet(ctx, featureGates)
	if err != nil {
		klog.Fatalf("Failed to start kubelet: %v", err)
	}
	// 等待终止信号
	waitForTerminationSignal()
}

const (
	// KubeletRootDirectory 指定存放 kubelet 运行时信息的目录
	KubeletRootDirectory = "/var/lib/kubelet"
)

// kubelet 的健康检查 URL
var kubeletHealthCheckURL = fmt.Sprintf("http://127.0.0.1:%d/healthz", ports.KubeletHealthzPort)

func baseKubeConfiguration(ctx context.Context, cfgPath string) (*kubeletconfig.KubeletConfiguration, error) {
	cfgPath, err := filepath.Abs(cfgPath)
	if err != nil {
		return nil, err
	}

	_, err = os.Stat(cfgPath)
	if err != nil {
		// kubeletconfig 存在但由于某种原因无法读取时返回错误，避免静默跳过
		if !os.IsNotExist(err) {
			return nil, err
		}

		// kubeletconfig 文件不存在时，使用默认配置作为基础
		kc, err := options.NewKubeletConfiguration()
		if err != nil {
			return nil, err
		}

		// 以下值应与 test/e2e_node/jenkins/default-kubelet-config.yaml 的内容保持一致。
		// 默认配置位于并行目录中，因此这里无法用 go embed 作为回退。
		// TODO(endocrimes): 当所有 e2e_node 使用方都提供配置（或迁移到 kubetest2
		//                   并使用其默认配置）后，移除缺少 kubelet config 时的回退逻辑
		kc.CgroupRoot = "/"
		kc.VolumeStatsAggPeriod = metav1.Duration{Duration: 10 * time.Second}
		kc.SerializeImagePulls = false
		kc.FileCheckFrequency = metav1.Duration{Duration: 10 * time.Second}
		kc.PodCIDR = "10.100.0.0/24"
		kc.EvictionPressureTransitionPeriod = metav1.Duration{Duration: 30 * time.Second}
		kc.EvictionHard = map[string]string{
			"memory.available":  "250Mi",
			"nodefs.available":  "10%",
			"nodefs.inodesFree": "5%",
		}
		kc.EvictionMinimumReclaim = map[string]string{
			"nodefs.available":  "5%",
			"nodefs.inodesFree": "5%",
		}

		return kc, nil
	}

	loader, err := configfiles.NewFsLoader(&utilfs.DefaultFs{}, cfgPath)
	if err != nil {
		return nil, err
	}

	return loader.Load(ctx)
}

// startKubelet 在独立进程中启动 kubelet，启动失败时返回错误
func (e *E2EServices) startKubelet(ctx context.Context, featureGates map[string]bool) (*server, error) {
	klog.Info("Starting kubelet")

	framework.Logf("Standalone mode: %v", framework.TestContext.StandaloneMode)

	var kubeconfigPath string

	if !framework.TestContext.StandaloneMode {
		var err error
		// 生成 kubeconfig
		kubeconfigPath, err = createKubeconfigCWD()
		if err != nil {
			return nil, err
		}
	}

	// KubeletConfiguration 文件路径
	kubeletConfigPath, err := kubeletConfigCWDPath()
	if err != nil {
		return nil, err
	}

	// KubeletDropInConfiguration 目录路径
	framework.TestContext.KubeletConfigDropinDir, err = KubeletConfigDirCWDDir()
	if err != nil {
		return nil, err
	}

	// 创建 pod 目录
	podPath, err := createPodDirectory()
	if err != nil {
		return nil, err
	}
	e.rmDirs = append(e.rmDirs, podPath)
	err = createRootDirectory(KubeletRootDirectory)
	if err != nil {
		return nil, err
	}

	lookup := flag.Lookup("kubelet-config-file")
	if lookup != nil {
		kubeletConfigFile = lookup.Value.String()
	}
	kc, err := baseKubeConfiguration(ctx, kubeletConfigFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load base kubelet configuration: %w", err)
	}

	// 应用覆盖项，使测试套件可以访问 Kubelet API。
	// 这些设置不安全，通常不应在测试基础设施之外使用

	// --anonymous-auth
	kc.Authentication.Anonymous.Enabled = true
	// --authentication-token-webhook
	kc.Authentication.Webhook.Enabled = false
	// --authorization-mode
	kc.Authorization.Mode = kubeletconfig.KubeletAuthorizationModeAlwaysAllow
	// --read-only-port
	kc.ReadOnlyPort = ports.KubeletReadOnlyPort

	// static pod 位于每个测试各自的目录中，这里为测试覆盖该路径
	kc.StaticPodPath = podPath

	var killCommand, restartCommand *exec.Cmd
	var isSystemd bool
	var unitName string
	// 应用默认的 kubelet 参数
	cmdArgs := []string{}
	if systemdRun, err := exec.LookPath("systemd-run"); err == nil {
		// systemd service/unit 的检测是可靠的，而从 ssh 会话启动的进程无法可靠检测。
		// kubelet 通常以 service 方式运行，因此这样测试也更贴近实际
		isSystemd = true

		// systemd >=240 时，重启后可以追加到同一个日志文件
		logLocation := "StandardError=file:"
		if version, verr := exec.Command("systemd-run", "--version").Output(); verr == nil {
			// $ systemd-run --version 的输出示例
			// systemd 245 (245.4-4ubuntu3.13)
			re := regexp.MustCompile(`systemd (\d+)`)
			if match := re.FindSubmatch(version); len(match) > 1 {
				num, _ := strconv.Atoi(string(match[1]))
				if num >= 240 {
					logLocation = "StandardError=append:"
				}
			}
		}
		// 可以忽略错误，让 GetTimestampFromWorkspaceDir() 回退到当前时间
		cwd, _ := os.Getwd()
		// 用当前目录的时间戳命名 systemd unit
		unitTimestamp := remote.GetTimestampFromWorkspaceDir(cwd)
		unitName = fmt.Sprintf("kubelet-%s.service", unitTimestamp)
		cmdArgs = append(cmdArgs,
			systemdRun,
			"-p", "Delegate=true",
			"-p", logLocation+framework.TestContext.ReportDir+"/kubelet.log",
			"--unit="+unitName,
			"--slice=runtime.slice",
			"--remain-after-exit",
			builder.GetKubeletServerBin())

		killCommand = exec.Command("systemctl", "kill", unitName)
		restartCommand = exec.Command("systemctl", "restart", unitName)

		kc.KubeletCgroups = "/kubelet.slice"
	} else {
		cmdArgs = append(cmdArgs, builder.GetKubeletServerBin())
		// TODO(random-liu): 去掉这个 docker 特有的设置
		cmdArgs = append(cmdArgs, "--runtime-cgroups=/docker-daemon")

		kc.KubeletCgroups = "/kubelet"

		kc.SystemCgroups = "/system"
	}

	if !framework.TestContext.StandaloneMode {
		cmdArgs = append(cmdArgs,
			"--kubeconfig", kubeconfigPath,
		)
	}

	cmdArgs = append(cmdArgs,
		"--root-dir", KubeletRootDirectory,
		"--v", LogVerbosityLevel,
	)

	// 默认应用测试框架的 feature gate，也可以用 kubelet-flags 覆盖
	if len(featureGates) > 0 {
		cmdArgs = append(cmdArgs, "--feature-gates", cliflag.NewMapStringBool(&featureGates).String())
		kc.FeatureGates = featureGates
	}

	// 添加 KubeletDropinConfigDirectory 参数（如果已设置）
	cmdArgs = append(cmdArgs, "--config-dir", framework.TestContext.KubeletConfigDropinDir)

	// 为方便起见保留 hostname override
	if framework.TestContext.NodeName != "" { // 指定了 node name 时设置 hostname override
		cmdArgs = append(cmdArgs, "--hostname-override", framework.TestContext.NodeName)
	}

	if framework.TestContext.ContainerRuntimeEndpoint != "" {
		cmdArgs = append(cmdArgs, "--container-runtime-endpoint", framework.TestContext.ContainerRuntimeEndpoint)
	}

	if framework.TestContext.ImageServiceEndpoint != "" {
		cmdArgs = append(cmdArgs, "--image-service-endpoint", framework.TestContext.ImageServiceEndpoint)
	}

	if err := WriteKubeletConfigFile(kc, kubeletConfigPath); err != nil {
		return nil, err
	}
	// 添加从文件加载配置的 flag
	cmdArgs = append(cmdArgs, "--config", kubeletConfigPath)

	// 覆盖默认的 kubelet flag
	cmdArgs = append(cmdArgs, kubeletArgs...)

	// 以 systemd 方式运行 kubelet 时调整参数
	if isSystemd {
		adjustArgsForSystemd(cmdArgs)
	}

	cmd := exec.Command(cmdArgs[0], cmdArgs[1:]...)
	restartOnExit := framework.TestContext.RestartKubelet
	server := newServer(
		"kubelet",
		cmd,
		killCommand,
		restartCommand,
		[]string{kubeletHealthCheckURL},
		"kubelet.log",
		e.monitorParent,
		restartOnExit,
		unitName)
	return server, server.start()
}

// WriteKubeletConfigFile 根据参数写入 kubelet 配置文件
func WriteKubeletConfigFile(internal *kubeletconfig.KubeletConfiguration, path string) error {
	data, err := kubeletconfigcodec.EncodeKubeletConfig(internal, kubeletconfigv1beta1.SchemeGroupVersion)
	if err != nil {
		return err
	}
	// 目录不存在时创建目录
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	// 写入文件
	if err := os.WriteFile(path, data, 0755); err != nil {
		return err
	}
	return nil
}

// createPodDirectory 创建 pod 目录
func createPodDirectory() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("failed to get current working directory: %w", err)
	}
	path, err := os.MkdirTemp(cwd, "static-pods")
	if err != nil {
		return "", fmt.Errorf("failed to create static pod directory: %w", err)
	}
	return path, nil
}

// createKubeconfig 在绝对路径 `path` 处创建 kubeconfig 文件，父目录必须已存在
func createKubeconfig(path string) error {
	kubeconfig := []byte(fmt.Sprintf(`apiVersion: v1
kind: Config
users:
- name: kubelet
  user:
    token: %s
clusters:
- cluster:
    server: %s
    insecure-skip-tls-verify: true
  name: local
contexts:
- context:
    cluster: local
    user: kubelet
  name: local-context
current-context: local-context`, framework.TestContext.BearerToken, getAPIServerClientURL()))

	if err := os.WriteFile(path, kubeconfig, 0666); err != nil {
		return err
	}
	return nil
}

func createRootDirectory(path string) error {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return os.MkdirAll(path, os.FileMode(0755))
		}
		return err
	}
	return nil
}

func kubeconfigCWDPath() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("failed to get current working directory: %w", err)
	}
	return filepath.Join(cwd, "kubeconfig"), nil
}

func kubeletConfigCWDPath() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("failed to get current working directory: %w", err)
	}
	// 不要把这个文件命名为 "kubelet"，否则会覆盖 kubelet 二进制并造成困惑 :)
	return filepath.Join(cwd, "kubelet-config"), nil
}

func KubeletConfigDirCWDDir() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("failed to get current working directory: %w", err)
	}
	dir := filepath.Join(cwd, "kubelet.conf.d")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return "", err
	}
	return dir, nil
}

// 与 createKubeconfig 类似，但在当前工作目录下创建 kubeconfig；
// 返回 kubeconfig 文件的绝对路径
func createKubeconfigCWD() (string, error) {
	kubeconfigPath, err := kubeconfigCWDPath()
	if err != nil {
		return "", err
	}

	if err = createKubeconfig(kubeconfigPath); err != nil {
		return "", err
	}
	return kubeconfigPath, nil
}

// adjustArgsForSystemd 为 systemd 转义 kubelet 参数中的特殊字符，
// 否则 systemd 可能尝试自动展开
func adjustArgsForSystemd(args []string) {
	for i := range args {
		args[i] = strings.Replace(args[i], "%", "%%", -1)
		args[i] = strings.Replace(args[i], "$", "$$", -1)
	}
}
