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
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/klog/v2"
)

var testTimeout = CommandLine.Duration("test-timeout", 45*time.Minute, "How long (in golang duration format) to wait for ginkgo tests to complete.")
var resultsDir = CommandLine.String("results-dir", defaultResultsDir(), "Directory to scp test results to.")

func defaultResultsDir() string {
	if artifacts, ok := os.LookupEnv("ARTIFACTS"); ok {
		return artifacts
	}
	return "/tmp"
}

const archiveName = "e2e_node_test.tar.gz"

func copyKubeletConfigIfExists(kubeletConfigFile, dstDir string) error {
	srcStat, err := os.Stat(kubeletConfigFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		} else {
			return err
		}
	}

	if !srcStat.Mode().IsRegular() {
		return fmt.Errorf("%s is not a regular file", kubeletConfigFile)
	}

	source, err := os.Open(kubeletConfigFile)
	if err != nil {
		return err
	}
	defer source.Close()

	dst := filepath.Join(dstDir, "kubeletconfig.yaml")
	destination, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destination.Close()

	_, err = io.Copy(destination, source)
	return err
}

// CreateTestArchive 为 node e2e 测试创建归档包。
func CreateTestArchive(suite TestSuite, systemSpecName, kubeletConfigFile string) (string, error) {
	klog.V(2).Infof("Building archive...")
	tardir, err := os.MkdirTemp("", "node-e2e-archive")
	if err != nil {
		return "", fmt.Errorf("failed to create temporary directory %v", err)
	}
	defer os.RemoveAll(tardir)

	err = copyKubeletConfigIfExists(kubeletConfigFile, tardir)
	if err != nil {
		return "", fmt.Errorf("failed to copy kubelet config: %w", err)
	}

	// 调用 suite 函数设置测试包。
	err = suite.SetupTestPackage(tardir, systemSpecName)
	if err != nil {
		return "", fmt.Errorf("failed to setup test package %q: %w", tardir, err)
	}

	// 构建 tar
	out, err := exec.Command("tar", "-zcvf", archiveName, "-C", tardir, ".").CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed to build tar %v.  Output:\n%s", err, out)
	}

	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("failed to get working directory %v", err)
	}
	return filepath.Join(dir, archiveName), nil
}

// RunRemote 返回命令输出、退出是否正常以及任何错误
type RunRemoteConfig struct {
	Suite                                                                                    TestSuite
	Archive                                                                                  string
	Host                                                                                     string
	Cleanup                                                                                  bool
	ImageDesc, JunitFileName, TestArgs, GinkgoArgs, SystemSpecName, ExtraEnvs, RuntimeConfig string
}

func RunRemote(cfg RunRemoteConfig) (string, bool, error) {
	// 创建临时 staging 目录
	klog.V(2).Infof("Staging test binaries on %q", cfg.Host)
	workspace := newWorkspaceDir()
	// 这里不用 sudo，以便用 scp 把测试归档复制到该目录。
	if output, err := SSHNoSudo(cfg.Host, "mkdir", workspace); err != nil {
		// 出错则退出失败
		return "", false, fmt.Errorf("failed to create workspace directory %q on Host %q: %v Output: %q", workspace, cfg.Host, err, output)
	}
	if cfg.Cleanup {
		defer func() {
			output, err := SSH(cfg.Host, "rm", "-rf", workspace)
			if err != nil {
				klog.Errorf("failed to cleanup workspace %q on Host %q: %v.  Output:\n%s", workspace, cfg.Host, err, output)
			}
		}()
	}

	// 把归档复制到 staging 目录
	if output, err := runSSHCommand(cfg.Host, "scp", cfg.Archive, fmt.Sprintf("%s:%s/", GetHostnameOrIP(cfg.Host), workspace)); err != nil {
		// 出错则退出失败
		return "", false, fmt.Errorf("failed to copy test archive: %v, Output: %q", err, output)
	}

	// 解压归档
	cmd := getSSHCommand(" && ",
		fmt.Sprintf("cd %s", workspace),
		fmt.Sprintf("tar -xzvf ./%s", archiveName),
	)
	klog.V(2).Infof("Extracting tar on %q", cfg.Host)
	// 这里不用 sudo，因为 `sudo tar -x` 会恢复 tarball 中的文件属主，
	// 而我们希望解压出的文件属于当前用户。
	if output, err := SSHNoSudo(cfg.Host, "sh", "-c", cmd); err != nil {
		// 出错则退出失败
		return "", false, fmt.Errorf("failed to extract test archive: %v, Output: %q", err, output)
	}

	// 创建测试结果目录。
	resultDir := filepath.Join(workspace, "results")
	if output, err := SSHNoSudo(cfg.Host, "mkdir", resultDir); err != nil {
		// 出错则退出失败
		return "", false, fmt.Errorf("failed to create test result directory %q on Host %q: %v Output: %q", resultDir, cfg.Host, err, output)
	}

	allGinkgoFlags := cfg.GinkgoArgs
	if !strings.Contains(allGinkgoFlags, "-timeout") {
		klog.Warningf("ginkgo flags are missing explicit --timeout (ginkgo defaults to 60 minutes)")
		// see https://github.com/onsi/ginkgo/blob/master/docs/index.md#:~:text=ginkgo%20%2D%2Dtimeout%3Dduration
		// ginkgo suite 超时应大于默认值但小于
		// 完整测试超时，因此取两者的平均值。
		suiteTimeout := int(testTimeout.Minutes())
		suiteTimeout = (60 + suiteTimeout) / 2
		allGinkgoFlags = fmt.Sprintf("%s --timeout=%dm", allGinkgoFlags, suiteTimeout)
		klog.Infof("updated ginkgo flags: %s", allGinkgoFlags)
	}

	klog.V(2).Infof("Running test on %q", cfg.Host)
	output, err := cfg.Suite.RunTest(cfg.Host, workspace, resultDir, cfg.ImageDesc, cfg.JunitFileName, cfg.TestArgs,
		allGinkgoFlags, cfg.SystemSpecName, cfg.ExtraEnvs, cfg.RuntimeConfig, *testTimeout)

	var aggErrs []error
	// 这里不记录 Output，交给调用方处理测试 Output。
	if err != nil {
		aggErrs = append(aggErrs, err)
		collectSystemLog(cfg.Host)
	}

	klog.V(2).Infof("Copying test artifacts from %q", cfg.Host)
	scpErr := getTestArtifacts(cfg.Host, workspace)
	if scpErr != nil {
		aggErrs = append(aggErrs, scpErr)
	}

	return output, len(aggErrs) == 0, utilerrors.NewAggregate(aggErrs)
}

const (
	// workspaceDirPrefix 是 workspace 目录名中使用的字符串前缀。
	workspaceDirPrefix = "node-e2e-"
	// timestampFormat 是 node e2e 目录名中使用的 timestamp 格式。
	timestampFormat = "20060102T150405"
)

func getTimestamp() string {
	return fmt.Sprint(time.Now().Format(timestampFormat))
}

func newWorkspaceDir() string {
	return filepath.Join("/tmp", workspaceDirPrefix+getTimestamp())
}

// GetTimestampFromWorkspaceDir 解析 workspace 目录名并取出其中的 timestamp 部分。
// 之后可用它命名其他产物（例如用于启动
// kubelet-${instance}.service 的 systemd transient service），
// 使它们能互相关联。
func GetTimestampFromWorkspaceDir(dir string) string {
	dirTimestamp := strings.TrimPrefix(filepath.Base(dir), workspaceDirPrefix)
	re := regexp.MustCompile("^\\d{8}T\\d{6}$")
	if re.MatchString(dirTimestamp) {
		return dirTimestamp
	}
	// 回退：如果找不到 timestamp，默认用 Now()
	return getTimestamp()
}

func getTestArtifacts(host, testDir string) error {
	logPath := filepath.Join(*resultsDir, host)
	if err := os.MkdirAll(logPath, 0755); err != nil {
		return fmt.Errorf("failed to create log directory %q: %w", logPath, err)
	}
	// 把日志（如有）复制到 artifacts/hostname
	if _, err := SSH(host, "ls", fmt.Sprintf("%s/results/*.log", testDir)); err == nil {
		if _, err := runSSHCommand(host, "scp", "-r", fmt.Sprintf("%s:%s/results/*.log", GetHostnameOrIP(host), testDir), logPath); err != nil {
			return err
		}
	}
	// 把 json 文件（如有）复制到 artifacts。
	if _, err := SSH(host, "ls", fmt.Sprintf("%s/results/*.json", testDir)); err == nil {
		if _, err = runSSHCommand(host, "scp", "-r", fmt.Sprintf("%s:%s/results/*.json", GetHostnameOrIP(host), testDir), *resultsDir); err != nil {
			return err
		}
	}
	// 把 junit 结果（如有）复制到 artifacts
	if _, err := SSH(host, "ls", fmt.Sprintf("%s/results/junit*", testDir)); err == nil {
		// 把 junit（如有）复制到 artifacts 顶层
		if _, err = runSSHCommand(host, "scp", fmt.Sprintf("%s:%s/results/junit*", GetHostnameOrIP(host), testDir), *resultsDir); err != nil {
			return err
		}
	}
	// 把容器日志复制到 artifacts/hostname
	klog.V(4).Info("Add 'execute' permission to /var/log/pods to copy logs")
	if _, err := SSH(host, "chmod", "o+x", "/var/log/pods"); err == nil {
		if _, err := SSH(host, "chmod", "-R", "a+r", "/var/log/pods"); err == nil {
			if _, err = runSSHCommand(host, "scp", "-r", fmt.Sprintf("%s:/var/log/pods/", GetHostnameOrIP(host)), logPath); err != nil {
				return err
			}
		}
	}
	return nil
}

// collectSystemLog 是一个临时 hack，遇到
// 意外错误时收集系统日志。
func collectSystemLog(host string) {
	// 遇到意外错误。这种情况下远端测试框架可能
	// 还没完成所有日志的取回和存储。尝试
	// 获取一些日志以便调试。
	// TODO: 这是尽力而为的临时 hack，只对
	// journald 节点有效。应有更健壮的日志收集方式。
	var (
		logName  = "system.log"
		logPath  = fmt.Sprintf("/tmp/%s-%s", getTimestamp(), logName)
		destPath = fmt.Sprintf("%s/%s-%s", *resultsDir, host, logName)
	)
	klog.V(2).Infof("Test failed unexpectedly. Attempting to retrieving system logs (only works for nodes with journald)")
	// 尝试从 journald 获取系统日志并存到文件。
	// 不要复用远端主机上原来的测试目录，因为
	// 节点重启后它可能已被删除。
	if output, err := SSH(host, "sh", "-c", fmt.Sprintf("'journalctl --system --all > %s'", logPath)); err == nil {
		klog.V(2).Infof("Got the system logs from journald; copying it back...")
		if output, err := runSSHCommand(host, "scp", fmt.Sprintf("%s:%s", GetHostnameOrIP(host), logPath), destPath); err != nil {
			klog.V(2).Infof("Failed to copy the log: err: %v, output: %q", err, output)
		}
	} else {
		klog.V(2).Infof("Failed to run journactl (normal if it doesn't exist on the node): %v, output: %q", err, output)
	}
}

// WriteLog 是一个临时函数，让 runner 中
// 也能写日志，用于收集串口控制台日志。
// TODO(random-liu): 改用 cluster e2e 中的 log-dump 脚本。
func WriteLog(host, filename, content string) error {
	logPath := filepath.Join(*resultsDir, host)
	if err := os.MkdirAll(logPath, 0755); err != nil {
		return fmt.Errorf("failed to create log directory %q: %w", logPath, err)
	}
	f, err := os.Create(filepath.Join(logPath, filename))
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(content)
	return err
}
