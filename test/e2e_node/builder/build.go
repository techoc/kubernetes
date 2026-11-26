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

package builder

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"k8s.io/klog/v2"
	"k8s.io/kubernetes/test/utils"
)

var k8sBinDir = CommandLine.String("k8s-bin-dir", "", "Directory containing k8s kubelet binaries.")
var useDockerizedBuild = CommandLine.Bool("use-dockerized-build", false, "Use dockerized build for test artifacts")
var targetBuildArch = CommandLine.String("target-build-arch", "linux/amd64", "Target architecture for the test artifacts for dockerized build")

// buildTargets 是在通过 "make test-e2e-node" 调用时，
// `test/e2e_node/runners/remote` 用 `make WHAT=` 构建的内容。该模式下，
// 每个命令使用独立的二进制。
var buildTargets = []string{
	"cmd/kubelet",
	"test/e2e_node/e2e_node.test",
	"github.com/onsi/ginkgo/v2/ginkgo",
	"cluster/gce/gci/mounter",
	"test/e2e_node/plugins/gcp-credential-provider",
}

// BuildGo 构建一些默认的 k8s 二进制。
func BuildGo() error {
	return BuildTargets(buildTargets...)
}

// BuildTargets 构建指定的 k8s 二进制（= WHAT 目标）。
func BuildTargets(targets ...string) error {
	k8sRoot, err := utils.GetK8sRootDir()
	if err != nil {
		return fmt.Errorf("failed to locate kubernetes root directory %v", err)
	}
	arch := GetTargetBuildArch()
	klog.Infof("Building k8s binaries %v in %q for %s...", targets, k8sRoot, arch)
	what := strings.Join(targets, " ")
	cmd := exec.Command("make", "-C", k8sRoot,
		fmt.Sprintf("WHAT=%s", what))
	if IsDockerizedBuild() {
		klog.Infof("Building dockerized k8s binaries targets %s for architecture %s", targets, GetTargetBuildArch())
		// 多架构构建仅在 dockerized build 中支持
		cmd = exec.Command(filepath.Join(k8sRoot, "build/run.sh"), "make", fmt.Sprintf("WHAT=%s", what), fmt.Sprintf("KUBE_BUILD_PLATFORMS=%s", arch))
		// dockerized build 时确保在 k8s 根目录下执行该命令
		cmd.Dir = k8sRoot
	}
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	if err != nil {
		return fmt.Errorf("failed to build go packages %v", err)
	}
	return nil
}

// IsDockerizedBuild 返回测试是否需要使用 dockerized build
func IsDockerizedBuild() bool {
	return *useDockerizedBuild
}

// GetTargetBuildArch 返回 dockerized build 的目标构建架构
func GetTargetBuildArch() string {
	return *targetBuildArch
}

// IsTargetArchArm64 返回目标是否为 linux/arm64 平台
func IsTargetArchArm64() bool {
	return GetTargetBuildArch() == "linux/arm64"
}

func getK8sBin(bin string) (string, error) {
	// 使用命令行指定的路径
	if *k8sBinDir != "" {
		absPath, err := filepath.Abs(*k8sBinDir)
		if err != nil {
			return "", err
		}
		if _, err := os.Stat(filepath.Join(*k8sBinDir, bin)); err != nil {
			return "", fmt.Errorf("Could not find %s under directory %s", bin, absPath)
		}
		return filepath.Join(absPath, bin), nil
	}

	path, err := filepath.Abs(filepath.Dir(os.Args[0]))
	if err != nil {
		return "", fmt.Errorf("Could not find absolute path of directory containing the tests %s", filepath.Dir(os.Args[0]))
	}
	if _, err := os.Stat(filepath.Join(path, bin)); err == nil {
		return filepath.Join(path, bin), nil
	}

	buildOutputDir, err := utils.GetK8sBuildOutputDir(IsDockerizedBuild(), GetTargetBuildArch())
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(filepath.Join(buildOutputDir, bin)); err == nil {
		return filepath.Join(buildOutputDir, bin), nil
	}

	// 放弃并报错
	return "", fmt.Errorf("unable to locate %s, Can be defined using --k8s-path", bin)
}

// GetKubeletServerBin 返回 kubelet 二进制的路径。
func GetKubeletServerBin() string {
	bin, err := getK8sBin("kubelet")
	if err != nil {
		klog.Fatalf("Could not locate kubelet binary %v.", err)
	}
	return bin
}
