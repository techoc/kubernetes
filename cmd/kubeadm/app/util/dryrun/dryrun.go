/*
Copyright 2017 The Kubernetes Authors.

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

package dryrun

import (
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	errorsutil "k8s.io/apimachinery/pkg/util/errors"

	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/apiclient"
)

// FileToPrint 表示打印时可使用别名的磁盘临时文件。
// 例如从 /tmp/ 加载文件，却向用户展示 /etc/kubernetes/... 中的目标路径。
type FileToPrint struct {
	RealPath  string
	PrintPath string
}

// NewFileToPrint 使用指定参数创建 FileToPrint。
func NewFileToPrint(realPath, printPath string) FileToPrint {
	return FileToPrint{
		RealPath:  realPath,
		PrintPath: printPath,
	}
}

// PrintDryRunFile 是 PrintDryRunFiles 的辅助方法。
func PrintDryRunFile(fileName, realDir, printDir string, w io.Writer) error {
	return PrintDryRunFiles([]FileToPrint{
		NewFileToPrint(filepath.Join(realDir, fileName), filepath.Join(printDir, fileName)),
	}, w)
}

// PrintDryRunFiles 将给定 FileToPrint 的内容输出到 writer w。
func PrintDryRunFiles(files []FileToPrint, w io.Writer) error {
	errs := []error{}
	for _, file := range files {
		if len(file.RealPath) == 0 {
			continue
		}

		fileBytes, err := os.ReadFile(file.RealPath)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		// 可伪造文件路径：实际从 /tmp/kubeadm-dryrun/admin.conf 写入和加载，
		// 却向用户展示 /etc/kubernetes/admin.conf。未设置 PrintPath 时使用真实路径。
		outputFilePath := file.PrintPath
		if len(outputFilePath) == 0 {
			outputFilePath = file.RealPath
		}
		outputFilePath = filepath.ToSlash(outputFilePath)

		fmt.Fprintf(w, "[dryrun] Would write file %q with content:\n", outputFilePath)
		fmt.Fprintf(w, "%s", fileBytes)
	}
	return errorsutil.NewAggregate(errs)
}

// Waiter 是用于试运行的 apiclient.Waiter 实现。
type Waiter struct{}

// NewWaiter 返回与指定 Kubernetes 集群通信的新 Waiter。
func NewWaiter() apiclient.Waiter {
	return &Waiter{}
}

// WaitForControlPlaneComponents 仅返回占位的 nil，让流程继续。
func (w *Waiter) WaitForControlPlaneComponents(podsMap map[string]*v1.Pod, apiServerAddress string) error {
	return nil
}

// WaitForPodsWithLabel 仅返回占位的 nil，让流程继续。
func (w *Waiter) WaitForPodsWithLabel(kvLabel string) error {
	fmt.Printf("[dryrun] Would wait for the Pods with the label %q in the %s namespace to become Running\n", kvLabel, metav1.NamespaceSystem)
	return nil
}

// WaitForKubelet 阻塞等待 kubelet 的 /healthz 端点返回 "ok"。
func (w *Waiter) WaitForKubelet(healthzAddress string, healthzPort int32) error {
	var (
		addrPort        = net.JoinHostPort(healthzAddress, strconv.Itoa(int(healthzPort)))
		healthzEndpoint = fmt.Sprintf("http://%s/healthz", addrPort)
	)
	fmt.Printf("[dryrun] Would make sure the kubelet returns 'ok' at %s\n", healthzEndpoint)
	return nil
}

// SetTimeout 不执行任何操作；此实现不会等待。
func (w *Waiter) SetTimeout(_ time.Duration) {}

// WaitForStaticPodControlPlaneHashes 为所有控制平面镜像返回空哈希。
func (w *Waiter) WaitForStaticPodControlPlaneHashes(_ string) (map[string]string, error) {
	return map[string]string{
		kubeadmconstants.KubeAPIServer:         "",
		kubeadmconstants.KubeControllerManager: "",
		kubeadmconstants.KubeScheduler:         "",
	}, nil
}

// WaitForStaticPodSingleHash 返回空哈希；调用方需要这些空字符串。
func (w *Waiter) WaitForStaticPodSingleHash(_ string, _ string) (string, error) {
	return "", nil
}

// WaitForStaticPodHashChange 返回占位的 nil 错误，使试运行流程继续。
func (w *Waiter) WaitForStaticPodHashChange(_, _, _ string) error {
	return nil
}

// PrintFilesIfDryRunning 将静态 Pod 清单输出到标准输出，并提示试运行临时目录的位置。
func PrintFilesIfDryRunning(needPrintManifest bool, manifestDir string, outputWriter io.Writer) error {
	var files []FileToPrint
	// 控制平面节点输出静态 Pod 清单。
	if needPrintManifest {
		fmt.Printf("[dryrun] Wrote certificates, kubeconfig files and control plane manifests to the %q directory\n", manifestDir)
		for _, component := range kubeadmconstants.ControlPlaneComponents {
			realPath := kubeadmconstants.GetStaticPodFilepath(component, manifestDir)
			outputPath := kubeadmconstants.GetStaticPodFilepath(component, kubeadmconstants.GetStaticPodDirectory())
			files = append(files, NewFileToPrint(realPath, outputPath))
		}
	} else {
		fmt.Printf("[dryrun] Wrote certificates and kubeconfig files to the %q directory\n", manifestDir)
	}

	fmt.Println("[dryrun] The certificates or kubeconfig files would not be printed due to their sensitive nature")
	fmt.Printf("[dryrun] Please examine the %q directory for details about what would be written\n", manifestDir)

	// 输出 kubelet 配置清单。
	kubeletConfigFiles := []string{kubeadmconstants.KubeletConfigurationFileName, kubeadmconstants.KubeletEnvFileName}
	for _, filename := range kubeletConfigFiles {
		realPath := filepath.Join(manifestDir, filename)
		outputPath := filepath.Join(kubeadmconstants.KubeletRunDirectory, filename)
		files = append(files, NewFileToPrint(realPath, outputPath))
	}

	return PrintDryRunFiles(files, outputWriter)
}
