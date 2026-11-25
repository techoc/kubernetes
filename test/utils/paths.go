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

package utils

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// GetK8sRootDir 返回 gopath 中 kubernetes 的根目录（如果存在）。
func GetK8sRootDir() (string, error) {
	dir := os.Getenv("KUBE_ROOT")
	if len(dir) > 0 {
		return dir, nil
	}

	dir, err := RootDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, fmt.Sprintf("%s/", "k8s.io/kubernetes")), nil
}

// GetCAdvisorRootDir 返回 gopath 中 cAdvisor 的根目录（如果存在）。
func GetCAdvisorRootDir() (string, error) {
	dir, err := RootDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, fmt.Sprintf("%s/", "github.com/google/cadvisor")), nil
}

// RootDir 获取磁盘上的 kubernetes 源码目录，找不到时返回错误
func RootDir() (string, error) {
	// 获取当前可执行文件所在的目录
	_, testExec, _, _ := runtime.Caller(0)
	path := filepath.Dir(testExec)

	// 查找 kubernetes 源码根目录
	if strings.Contains(path, "k8s.io/kubernetes") {
		splitPath := strings.Split(path, "k8s.io/kubernetes")
		return splitPath[0], nil
	}

	return "", errors.New("could not find kubernetes source root directory")
}

// GetK8sBuildOutputDir 返回 k8s 的构建输出目录
// 对于 docker 化构建，必须显式指定 targetArch（例如 'linux/arm64'、'linux/amd64'）
// 对于非 docker 化构建，targetArch 会被忽略
func GetK8sBuildOutputDir(isDockerizedBuild bool, targetArch string) (string, error) {
	k8sRoot, err := GetK8sRootDir()
	if err != nil {
		return "", err
	}
	buildOutputDir := filepath.Join(k8sRoot, "_output/local/go/bin")
	if isDockerizedBuild {
		buildOutputDir = filepath.Join(k8sRoot, "_output/dockerized/bin/", targetArch)
	}
	if _, err := os.Stat(buildOutputDir); err != nil {
		return "", err
	}
	return buildOutputDir, nil
}
