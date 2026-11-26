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

package kubectl

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	testutils "k8s.io/kubernetes/test/utils"

	"github.com/onsi/ginkgo/v2"
)

const (
	maxKubectlExecRetries = 5
)

// TestKubeconfig 是一个结构体，包含 TestContext 和 Framework(Namespace) 中所需的属性。
type TestKubeconfig struct {
	CertDir     string
	Host        string
	KubeConfig  string
	KubeContext string
	KubectlPath string
	Namespace   string // 每个测试至少有一个命名空间，除非跳过创建
}

// NewTestKubeconfig 返回一个新的 Kubeconfig 结构体实例。
func NewTestKubeconfig(certdir, host, kubeconfig, kubecontext, kubectlpath, namespace string) *TestKubeconfig {
	return &TestKubeconfig{
		CertDir:     certdir,
		Host:        host,
		KubeConfig:  kubeconfig,
		KubeContext: kubecontext,
		KubectlPath: kubectlpath,
		Namespace:   namespace,
	}
}

// KubectlCmd 通过 wrapper 脚本运行 kubectl 可执行文件。
func (tk *TestKubeconfig) KubectlCmd(args ...string) *exec.Cmd {
	defaultArgs := []string{}

	// 指定 --server 参数，让测试可在任意位置运行。
	if tk.Host != "" {
		defaultArgs = append(defaultArgs, "--"+clientcmd.FlagAPIServer+"="+tk.Host)
	}
	if tk.KubeConfig != "" {
		defaultArgs = append(defaultArgs, "--"+clientcmd.RecommendedConfigPathFlag+"="+tk.KubeConfig)

		// 引用 KubeContext
		if tk.KubeContext != "" {
			defaultArgs = append(defaultArgs, "--"+clientcmd.FlagContext+"="+tk.KubeContext)
		}

	} else {
		if tk.CertDir != "" {
			defaultArgs = append(defaultArgs,
				fmt.Sprintf("--certificate-authority=%s", filepath.Join(tk.CertDir, "ca.crt")),
				fmt.Sprintf("--client-certificate=%s", filepath.Join(tk.CertDir, "kubecfg.crt")),
				fmt.Sprintf("--client-key=%s", filepath.Join(tk.CertDir, "kubecfg.key")))
		}
	}
	if tk.Namespace != "" {
		defaultArgs = append(defaultArgs, fmt.Sprintf("--namespace=%s", tk.Namespace))
	}
	kubectlArgs := append(defaultArgs, args...)

	// 允许用户指定 kubectl 路径，因此可以测试 "kubectl" 或 "cluster/kubectl.sh"
	// 等等。
	cmd := exec.Command(tk.KubectlPath, kubectlArgs...)

	// 调用方会执行该命令并等待其结束。
	return cmd
}

// LogFailedContainers 对失败的容器运行 `kubectl logs`。
func LogFailedContainers(ctx context.Context, c clientset.Interface, ns string, logFunc func(ftm string, args ...interface{})) {
	podList, err := c.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		logFunc("Error getting pods in namespace '%s': %v", ns, err)
		return
	}
	logFunc("Running kubectl logs on non-ready containers in %v", ns)
	for _, pod := range podList.Items {
		if res, err := testutils.PodRunningReady(&pod); !res || err != nil {
			kubectlLogPod(ctx, c, pod, "", framework.Logf)
		}
	}
}

func kubectlLogPod(ctx context.Context, c clientset.Interface, pod v1.Pod, containerNameSubstr string, logFunc func(ftm string, args ...interface{})) {
	for _, container := range pod.Spec.Containers {
		if strings.Contains(container.Name, containerNameSubstr) {
			// substr 为空时 Contains() 匹配所有字符串
			logs, err := e2epod.GetPodLogs(ctx, c, pod.Namespace, pod.Name, container.Name)
			if err != nil {
				logs, err = e2epod.GetPreviousPodLogs(ctx, c, pod.Namespace, pod.Name, container.Name)
				if err != nil {
					logFunc("Failed to get logs of pod %v, container %v, err: %v", pod.Name, container.Name, err)
				}
			}
			logFunc("Logs of %v/%v:%v on node %v", pod.Namespace, pod.Name, container.Name, pod.Spec.NodeName)
			logFunc("%s : STARTLOG\n%s\nENDLOG for container %v:%v:%v", containerNameSubstr, logs, pod.Namespace, pod.Name, container.Name)
		}
	}
}

// WriteFileViaContainer 通过指定容器执行 kubectl exec echo <contents> > <path> 来写文件。
// 由于这里使用的技术比较原始，只允许 ASCII 字母数字字符。
func (tk *TestKubeconfig) WriteFileViaContainer(podName, containerName string, path string, contents string) error {
	ginkgo.By("writing a file in the container")
	allowedCharacters := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	for _, c := range contents {
		if !strings.ContainsRune(allowedCharacters, c) {
			return fmt.Errorf("Unsupported character in string to write: %v", c)
		}
	}
	command := fmt.Sprintf("echo '%s' > '%s'; sync", contents, path)
	stdout, stderr, err := tk.kubectlExecWithRetry(tk.Namespace, podName, containerName, "--", "/bin/sh", "-c", command)
	if err != nil {
		framework.Logf("error running kubectl exec to write file: %v\nstdout=%v\nstderr=%v)", err, string(stdout), string(stderr))
	}
	return err
}

// ReadFileViaContainer 通过 kubectl exec cat <path> 读取文件。
func (tk *TestKubeconfig) ReadFileViaContainer(podName, containerName string, path string) (string, error) {
	ginkgo.By("reading a file in the container")

	stdout, stderr, err := tk.kubectlExecWithRetry(tk.Namespace, podName, containerName, "--", "cat", path)
	if err != nil {
		framework.Logf("error running kubectl exec to read file: %v\nstdout=%v\nstderr=%v)", err, string(stdout), string(stderr))
	}
	return string(stdout), err
}

func (tk *TestKubeconfig) kubectlExecWithRetry(namespace string, podName, containerName string, args ...string) ([]byte, []byte, error) {
	for numRetries := range maxKubectlExecRetries {
		if numRetries > 0 {
			framework.Logf("Retrying kubectl exec (retry count=%v/%v)", numRetries+1, maxKubectlExecRetries)
		}

		stdOutBytes, stdErrBytes, err := tk.kubectlExec(namespace, podName, containerName, args...)
		if err != nil {
			if strings.Contains(strings.ToLower(string(stdErrBytes)), "i/o timeout") {
				// 遇到 "i/o timeout" 错误时重试
				framework.Logf("Warning: kubectl exec encountered i/o timeout.\nerr=%v\nstdout=%v\nstderr=%v)", err, string(stdOutBytes), string(stdErrBytes))
				continue
			}
			if strings.Contains(strings.ToLower(string(stdErrBytes)), "container not found") {
				// 遇到 "container not found" 错误时重试
				framework.Logf("Warning: kubectl exec encountered container not found.\nerr=%v\nstdout=%v\nstderr=%v)", err, string(stdOutBytes), string(stdErrBytes))
				time.Sleep(2 * time.Second)
				continue
			}
		}

		return stdOutBytes, stdErrBytes, err
	}
	err := fmt.Errorf("Failed: kubectl exec failed %d times with \"i/o timeout\". Giving up", maxKubectlExecRetries)
	return nil, nil, err
}

func (tk *TestKubeconfig) kubectlExec(namespace string, podName, containerName string, args ...string) ([]byte, []byte, error) {
	var stdout, stderr bytes.Buffer
	cmdArgs := []string{
		"exec",
		fmt.Sprintf("--namespace=%v", namespace),
		podName,
		fmt.Sprintf("-c=%v", containerName),
	}
	cmdArgs = append(cmdArgs, args...)

	cmd := tk.KubectlCmd(cmdArgs...)
	cmd.Stdout, cmd.Stderr = &stdout, &stderr

	framework.Logf("Running '%s %s'", cmd.Path, strings.Join(cmdArgs, " "))
	err := cmd.Run()
	return stdout.Bytes(), stderr.Bytes(), err
}
