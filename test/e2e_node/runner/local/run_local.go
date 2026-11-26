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

package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"

	"k8s.io/kubernetes/test/e2e_node/builder"
	"k8s.io/kubernetes/test/e2e_node/system"
	"k8s.io/kubernetes/test/utils"

	"k8s.io/klog/v2"
)

var buildDependencies = flag.Bool("build-dependencies", true, "If true, build all dependencies.")
var ginkgoFlags = flag.String("ginkgo-flags", "", "Space-separated list of arguments to pass to Ginkgo test runner, with shell-style quoting and escaping.")
var testFlags = flag.String("test-flags", "", "Space-separated list of arguments to pass to node e2e test, with shell-style quoting and escaping.")
var systemSpecName = flag.String("system-spec-name", "", fmt.Sprintf("The name of the system spec used for validating the image in the node conformance test. The specs are at %s. If unspecified, the default built-in spec (system.DefaultSpec) will be used.", system.SystemSpecPath))
var extraEnvs = flag.String("extra-envs", "", "The extra environment variables needed for node e2e tests. Format: a list of key=value pairs, e.g., env1=val1,env2=val2")
var runtimeConfig = flag.String("runtime-config", "", "The runtime configuration for the API server on the node e2e tests. Format: a list of key=value pairs, e.g., env1=val1,env2=val2")
var kubeletConfigFile = flag.String("kubelet-config-file", "", "The KubeletConfiguration file that should be applied to the kubelet")
var debugTool = flag.String("debug-tool", "", "'delve', 'dlv' or 'gdb': run e2e_node.test under that debugger")

func main() {
	copyFlags(builder.CommandLine, flag.CommandLine)
	klog.InitFlags(nil)
	flag.Parse()

	// 构建依赖 —— ginkgo、kubelet、e2e_node.test 和 mounter。
	if *buildDependencies {
		if err := builder.BuildGo(); err != nil {
			klog.Fatalf("Failed to build the dependencies: %v", err)
		}
	}

	// 运行 node e2e 测试
	outputDir, err := utils.GetK8sBuildOutputDir(builder.IsDockerizedBuild(), builder.GetTargetBuildArch())
	if err != nil {
		klog.Fatalf("Failed to get build output directory: %v", err)
	}
	klog.Infof("Got build output dir: %v", outputDir)
	test := filepath.Join(outputDir, "e2e_node.test")
	interactive := false
	var cmd string
	var args []string
	switch *debugTool {
	case "":
		// 没有 debugger，直接运行 ginkgo。
		cmd = filepath.Join(outputDir, "ginkgo")
		args = []string{*ginkgoFlags, test, "--"}
	case "delve", "dlv":
		dlv, err := exec.LookPath("dlv")
		if err != nil {
			klog.Fatalf("'dlv' not found: %v", err)
		}
		interactive = true
		cmd = dlv
		args = []string{"exec", test, "--", addGinkgoArgPrefix(*ginkgoFlags)}
	case "gdb":
		gdb, err := exec.LookPath("gdb")
		if err != nil {
			klog.Fatalf("'gdb' not found: %v", err)
		}
		interactive = true
		cmd = gdb
		args = []string{test, "--", addGinkgoArgPrefix(*ginkgoFlags)}
	}

	args = append(args, *testFlags, fmt.Sprintf("--runtime-config=%s", *runtimeConfig))
	if *systemSpecName != "" {
		rootDir, err := utils.GetK8sRootDir()
		if err != nil {
			klog.Fatalf("Failed to get k8s root directory: %v", err)
		}
		systemSpecFile := filepath.Join(rootDir, system.SystemSpecPath, *systemSpecName+".yaml")
		args = append(args, fmt.Sprintf("--system-spec-name=%s --system-spec-file=%s --extra-envs=%s", *systemSpecName, systemSpecFile, *extraEnvs))
	}
	if *kubeletConfigFile != "" {
		args = append(args, fmt.Sprintf("--kubelet-config-file=\"%s\"", *kubeletConfigFile))
	}
	if err := runCommand(interactive, cmd, args...); err != nil {
		klog.Exitf("Test failed: %v", err)
	}
	return
}

func copyFlags(from, to *flag.FlagSet) {
	from.VisitAll(func(f *flag.Flag) {
		to.Var(f.Value, f.Name, f.Usage)
	})
}

func addGinkgoArgPrefix(ginkgoFlags string) string {
	// 警告，hack！这个简单的搜索/替换假定参数值中不含连字符。
	//
	//
	// 正确的做法是用 github.com/anmitsu/go-shlex 把
	// -ginkgo-flags 和 -test-flags 字符串拆成单个参数，然后把得到的字符串切片
	// 传给 exec.Command，而不是把单个字符串交给 sh。但 github.com/anmitsu/go-shlex
	// 不是 Kubernetes 依赖，不值得为此添加。
	//

	ginkgoFlags = regexp.MustCompile(`(^| )--?`).ReplaceAllString(ginkgoFlags, `$1--ginkgo.`)
	return ginkgoFlags
}

func runCommand(interactive bool, name string, args ...string) error {
	klog.Infof("Running command: %v %v", name, strings.Join(args, " "))
	// 必须使用 sh，因为参数使用了 POSIX 引号。
	// sh 必须解析这些引号。
	cmd := exec.Command("sudo", "sh", "-c", strings.Join(append([]string{name}, args...), " "))
	if interactive {
		// stdin 必须是控制台。
		cmd.Stdin = os.Stdin
		cmd.Stdout = os.Stdin
		cmd.Stderr = os.Stdin
	} else {
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
	}

	return cmd.Run()
}
