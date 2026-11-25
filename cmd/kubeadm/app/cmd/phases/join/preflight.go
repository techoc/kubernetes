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

package phases

import (
	"bytes"
	"fmt"
	"text/template"

	"github.com/lithammer/dedent"

	"k8s.io/klog/v2"
	utilsexec "k8s.io/utils/exec"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/options"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/phases/workflow"
	cmdutil "k8s.io/kubernetes/cmd/kubeadm/app/cmd/util"
	"k8s.io/kubernetes/cmd/kubeadm/app/phases/certs"
	"k8s.io/kubernetes/cmd/kubeadm/app/preflight"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

var (
	preflightExample = cmdutil.Examples(`
		# Run join pre-flight checks using a config file.
		kubeadm join phase preflight --config kubeadm-config.yaml
		`)

	notReadyToJoinControlPlaneTemp = template.Must(template.New("join").Parse(dedent.Dedent(`
		One or more conditions for hosting a new control plane instance is not satisfied.

		{{.Error}}

		Please ensure that:
		* The cluster has a stable controlPlaneEndpoint address.
		* The certificates that must be shared among control plane instances are provided.

		`)))
)

// NewPreflightPhase 创建为新节点加入执行预检的 kubeadm 工作流阶段。
func NewPreflightPhase() workflow.Phase {
	return workflow.Phase{
		Name:    "preflight [api-server-endpoint]",
		Short:   "Run join pre-flight checks",
		Long:    "Run pre-flight checks for kubeadm join.",
		Example: preflightExample,
		Run:     runPreflight,
		InheritFlags: []string{
			options.CfgPath,
			options.IgnorePreflightErrors,
			options.TLSBootstrapToken,
			options.TokenStr,
			options.ControlPlane,
			options.APIServerAdvertiseAddress,
			options.APIServerBindPort,
			options.NodeCRISocket,
			options.NodeName,
			options.FileDiscovery,
			options.TokenDiscovery,
			options.TokenDiscoveryCAHash,
			options.TokenDiscoverySkipCAHash,
			options.CertificateKey,
			options.DryRun,
		},
	}
}

// runPreflight 执行预检逻辑。
func runPreflight(c workflow.RunData) error {
	j, ok := c.(JoinData)
	if !ok {
		return errors.New("preflight phase invoked with an invalid data struct")
	}
	fmt.Println("[preflight] Running pre-flight checks")

	// 从通用检查开始。
	klog.V(1).Infoln("[preflight] Running general checks")
	// 首先单独检查是否以 root 身份运行，以便快速失败。
	if err := preflight.RunRootCheckOnly(j.IgnorePreflightErrors()); err != nil {
		return err
	}
	if err := preflight.RunJoinNodeChecks(utilsexec.New(), j.Cfg(), j.IgnorePreflightErrors()); err != nil {
		return err
	}

	initCfg, err := j.InitCfg()
	if err != nil {
		return err
	}

	// 根据 init 配置继续执行更具体的检查。
	klog.V(1).Infoln("[preflight] Running configuration dependant checks")
	if j.Cfg().ControlPlane != nil {
		// 检查集群配置是否支持加入新的控制平面实例，
		// 以及是否提供了全部必需证书。
		hasCertificateKey := len(j.CertificateKey()) > 0
		if err := checkIfReadyForAdditionalControlPlane(&initCfg.ClusterConfiguration, hasCertificateKey); err != nil {
			// 输出节点尚不适合承载新控制平面实例的消息。
			ctx := map[string]string{
				"Error": err.Error(),
			}

			var msg bytes.Buffer
			notReadyToJoinControlPlaneTemp.Execute(&msg, ctx)
			return errors.New(msg.String())
		}

		// 运行 kubeadm init 预检，检查全部前置条件。
		fmt.Println("[preflight] Running pre-flight checks before initializing the new control plane instance")

		if err := preflight.RunInitNodeChecks(utilsexec.New(), initCfg, j.IgnorePreflightErrors(), true, hasCertificateKey); err != nil {
			return err
		}

		if j.DryRun() {
			fmt.Println("[preflight] Would pull the required images (like 'kubeadm config images pull')")
			return nil
		}

		fmt.Println("[preflight] Pulling images required for setting up a Kubernetes cluster")
		fmt.Println("[preflight] This might take a minute or two, depending on the speed of your internet connection")
		fmt.Println("[preflight] You can also perform this action beforehand using 'kubeadm config images pull'")
		if err := preflight.RunPullImagesCheck(utilsexec.New(), initCfg, j.IgnorePreflightErrors()); err != nil {
			return err
		}
	}
	return nil
}

// checkIfReadyForAdditionalControlPlane 确保集群状态支持加入额外的控制平面实例，
// 且节点已准备好接受预检。
func checkIfReadyForAdditionalControlPlane(initConfiguration *kubeadmapi.ClusterConfiguration, hasCertificateKey bool) error {
	// 若集群创建时没有稳定控制平面端点，则阻塞。
	if initConfiguration.ControlPlaneEndpoint == "" {
		return errors.New("unable to add a new control plane instance to a cluster that doesn't have a stable controlPlaneEndpoint address")
	}

	if !hasCertificateKey {
		// 检查证书是否已提供且仍有效、尚未过期。
		if ret, err := certs.SharedCertificateExists(initConfiguration); !ret {
			return err
		}
	}

	return nil
}
