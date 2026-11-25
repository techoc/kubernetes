/*
Copyright 2019 The Kubernetes Authors.

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

package main

import (
	"fmt"
	"io"
	"os/exec"
	"strings"
)

// getCmd 使用给定的环境拼装用于运行测试的 ginkgo 命令，并将
// stdout/stderr 设置为给定的 writer。
func getCmd(env Getenver, w io.Writer) *exec.Cmd {
	ginkgoArgs := []string{}

	// 并行环境变量影响 skip 值的逻辑决定了它必须放在
	// 其余 flag 解析之前处理。
	skip := env.Getenv(skipEnvKey)
	switch env.Getenv(parallelEnvKey) {
	case "y", "Y", "true":
		ginkgoArgs = append(ginkgoArgs, "--p")
		if len(skip) == 0 {
			skip = serialTestsRegexp
		}
	}

	ginkgoArgs = append(ginkgoArgs, []string{
		"--focus=" + env.Getenv(focusEnvKey),
		"--skip=" + skip,
		"--no-color=true",
	}...)

	extraArgs := []string{
		"--disable-log-dump",
		"--repo-root=/kubernetes",
		"--provider=" + env.Getenv(providerEnvKey),
		"--report-dir=" + env.Getenv(resultsDirEnvKey),
		"--kubeconfig=" + env.Getenv(kubeconfigEnvKey),
	}

	// 处理额外参数
	sep := " "
	if len(env.Getenv(extraArgsSeparaterEnvKey)) > 0 {
		sep = env.Getenv(extraArgsSeparaterEnvKey)
	}

	if len(env.Getenv(extraGinkgoArgsEnvKey)) > 0 {
		ginkgoArgs = append(ginkgoArgs, strings.Split(env.Getenv(extraGinkgoArgsEnvKey), sep)...)
	}

	if len(env.Getenv(extraArgsEnvKey)) > 0 {
		fmt.Printf("sep is %q args are %q", sep, env.Getenv(extraArgsEnvKey))
		fmt.Println("split", strings.Split(env.Getenv(extraArgsEnvKey), sep))
		extraArgs = append(extraArgs, strings.Split(env.Getenv(extraArgsEnvKey), sep)...)
	}

	if len(env.Getenv(dryRunEnvKey)) > 0 {
		ginkgoArgs = append(ginkgoArgs, "--dry-run=true")
	}
	// NOTE: Ginkgo V2 的默认超时已从 24h 缩短为 1h；若 extraGinkgoArgsEnvKey
	// 对应的环境未设置该项，则将其设为 "24h" 以保持向后兼容。
	exists := func(args []string) bool {
		for _, arg := range args {
			if strings.Contains(arg, "--timeout") {
				return true
			}
		}
		return false
	}(ginkgoArgs)

	if !exists {
		ginkgoArgs = append(ginkgoArgs, "--timeout=24h")
	}

	args := []string{}
	args = append(args, ginkgoArgs...)
	args = append(args, env.Getenv(testBinEnvKey))
	args = append(args, "--")
	args = append(args, extraArgs...)

	cmd := exec.Command(env.Getenv(ginkgoEnvKey), args...)
	cmd.Stdout = w
	cmd.Stderr = w
	return cmd
}

// cmdInfo 生成便于打印/调试的命令信息。
func cmdInfo(cmd *exec.Cmd) string {
	return fmt.Sprintf(
		`Command env: %v
Run from directory: %v
Executable path: %v
Args (comma-delimited): %v`, cmd.Env, cmd.Dir, cmd.Path, strings.Join(cmd.Args, ","),
	)
}
