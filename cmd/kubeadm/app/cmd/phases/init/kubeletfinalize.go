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

package phases

import (
	"fmt"
	"os"
	"path/filepath"

	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/options"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/phases/workflow"
	cmdutil "k8s.io/kubernetes/cmd/kubeadm/app/cmd/util"
	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	kubeletphase "k8s.io/kubernetes/cmd/kubeadm/app/phases/kubelet"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

var (
	kubeletFinalizePhaseExample = cmdutil.Examples(`
		# Updates settings relevant to the kubelet after TLS bootstrap
		kubeadm init phase kubelet-finalize all --config
		`)
)

// NewKubeletFinalizePhase 创建 kubeadm 工作流阶段，更新 TLS 引导后与 kubelet 相关的设置。
func NewKubeletFinalizePhase() workflow.Phase {
	return workflow.Phase{
		Name:    "kubelet-finalize",
		Short:   "Updates settings relevant to the kubelet after TLS bootstrap",
		Example: kubeletFinalizePhaseExample,
		Phases: []workflow.Phase{
			{
				Name:           "all",
				Short:          "Run all kubelet-finalize phases",
				InheritFlags:   []string{options.CfgPath, options.CertificatesDir, options.DryRun},
				Example:        kubeletFinalizePhaseExample,
				RunAllSiblings: true,
			},
			{
				Name:         "enable-client-cert-rotation",
				Short:        "Enable kubelet client certificate rotation",
				InheritFlags: []string{options.CfgPath, options.CertificatesDir, options.DryRun},
				Run:          runKubeletFinalizeEnableClientCertRotation,
			},
		},
	}
}

// runKubeletFinalizeEnableClientCertRotation 检测是否启用 kubelet 证书轮换，
// 并更新 kubelet.conf，使 Node 用户指向可轮换的证书和密钥。
func runKubeletFinalizeEnableClientCertRotation(c workflow.RunData) error {
	data, ok := c.(InitData)
	if !ok {
		return errors.New("kubelet-finalize phase invoked with an invalid data struct")
	}

	// 检查用户是否添加 kubelet --cert-dir 参数。
	// 若已添加则使用该路径，否则使用 kubeadm 提供的值。
	cfg := data.Cfg()
	pkiPath := filepath.Join(data.KubeletDir(), "pki")
	val, idx := kubeadmapi.GetArgValue(cfg.NodeRegistration.KubeletExtraArgs, "cert-dir", -1)
	if idx > -1 {
		pkiPath = val
	}

	// 检查 kubelet 证书目录中是否存在 kubelet-client-current.pem。
	rotate := false
	pemPath := filepath.Join(pkiPath, "kubelet-client-current.pem")
	if _, err := os.Stat(pemPath); err == nil {
		klog.V(1).Infof("[kubelet-finalize] Assuming that kubelet client certificate rotation is enabled: found %q", pemPath)
		rotate = true
	} else {
		klog.V(1).Infof("[kubelet-finalize] Assuming that kubelet client certificate rotation is disabled: %v", err)
	}

	// 若未启用轮换，则提前退出。
	if !rotate {
		return nil
	}

	kubeconfigPath := filepath.Join(kubeadmconstants.KubernetesDir, kubeadmconstants.KubeletKubeConfigFileName)
	fmt.Printf("[kubelet-finalize] Updating %q to point to a rotatable kubelet client certificate and key\n", kubeconfigPath)

	// 若启用 dry-run，则提前退出。
	if data.DryRun() {
		return nil
	}

	// 从磁盘加载 kubeconfig。
	kubeconfig, err := clientcmd.LoadFromFile(kubeconfigPath)
	if err != nil {
		return errors.Wrapf(err, "could not load %q", kubeconfigPath)
	}

	// 执行基本校验。此处错误只可能在 kubelet.conf 损坏时发生。
	if len(kubeconfig.CurrentContext) == 0 {
		return errors.Errorf("the file %q does not have current context set", kubeconfigPath)
	}
	currentContext, ok := kubeconfig.Contexts[kubeconfig.CurrentContext]
	if !ok {
		return errors.Errorf("the file %q is not a valid kubeconfig: %q set as current-context, but not found in context list", kubeconfigPath, kubeconfig.CurrentContext)
	}
	userName := currentContext.AuthInfo
	if len(userName) == 0 {
		return errors.Errorf("the file %q is not a valid kubeconfig: empty username for current context", kubeconfigPath)
	}
	info, ok := kubeconfig.AuthInfos[userName]
	if !ok {
		return errors.Errorf("the file %q does not contain authentication for user %q", kubeconfigPath, cfg.NodeRegistration.Name)
	}

	// 更新节点授权器的客户端证书和密钥，使其指向 PEM 符号链接。
	info.ClientKeyData = []byte{}
	info.ClientCertificateData = []byte{}
	info.ClientKey = pemPath
	info.ClientCertificate = pemPath

	// 将 kubeconfig 写回磁盘。
	if err = clientcmd.WriteToFile(*kubeconfig, kubeconfigPath); err != nil {
		return errors.Wrapf(err, "failed to serialize %q", kubeconfigPath)
	}

	// 重启 kubelet。
	klog.V(1).Info("[kubelet-finalize] Restarting the kubelet to enable client certificate rotation")
	kubeletphase.TryRestartKubelet()

	return nil
}
