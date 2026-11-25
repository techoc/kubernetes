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

package phases

import (
	"fmt"
	"path/filepath"

	"github.com/spf13/cobra"

	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/options"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/phases/workflow"
	cmdutil "k8s.io/kubernetes/cmd/kubeadm/app/cmd/util"
	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	certsphase "k8s.io/kubernetes/cmd/kubeadm/app/phases/certs"
	"k8s.io/kubernetes/cmd/kubeadm/app/phases/controlplane"
	"k8s.io/kubernetes/cmd/kubeadm/app/phases/copycerts"
	kubeconfigphase "k8s.io/kubernetes/cmd/kubeadm/app/phases/kubeconfig"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	kubeconfigutil "k8s.io/kubernetes/cmd/kubeadm/app/util/kubeconfig"
)

var controlPlanePrepareExample = cmdutil.Examples(`
	# Prepares the machine for serving a control plane
	kubeadm join phase control-plane-prepare all
`)

// NewControlPlanePreparePhase 创建实现节点控制平面服务准备的 kubeadm 工作流阶段。
func NewControlPlanePreparePhase() workflow.Phase {
	return workflow.Phase{
		Name:    "control-plane-prepare",
		Short:   "Prepare the machine for serving a control plane",
		Example: controlPlanePrepareExample,
		Phases: []workflow.Phase{
			{
				Name:           "all [api-server-endpoint]",
				Short:          "Prepare the machine for serving a control plane",
				InheritFlags:   getControlPlanePreparePhaseFlags("all"),
				RunAllSiblings: true,
			},
			newControlPlanePrepareDownloadCertsSubphase(),
			newControlPlanePrepareCertsSubphase(),
			newControlPlanePrepareKubeconfigSubphase(),
			newControlPlanePrepareControlPlaneSubphase(),
		},
	}
}

func getControlPlanePreparePhaseFlags(name string) []string {
	var flags []string
	switch name {
	case "all":
		flags = []string{
			options.APIServerAdvertiseAddress,
			options.APIServerBindPort,
			options.CfgPath,
			options.ControlPlane,
			options.NodeName,
			options.FileDiscovery,
			options.TokenDiscovery,
			options.TokenDiscoveryCAHash,
			options.TokenDiscoverySkipCAHash,
			options.TLSBootstrapToken,
			options.TokenStr,
			options.CertificateKey,
			options.Patches,
			options.DryRun,
		}
	case "download-certs":
		flags = []string{
			options.CfgPath,
			options.ControlPlane,
			options.FileDiscovery,
			options.TokenDiscovery,
			options.TokenDiscoveryCAHash,
			options.TokenDiscoverySkipCAHash,
			options.TLSBootstrapToken,
			options.TokenStr,
			options.CertificateKey,
			options.DryRun,
		}
	case "certs":
		flags = []string{
			options.APIServerAdvertiseAddress,
			options.CfgPath,
			options.ControlPlane,
			options.NodeName,
			options.FileDiscovery,
			options.TokenDiscovery,
			options.TokenDiscoveryCAHash,
			options.TokenDiscoverySkipCAHash,
			options.TLSBootstrapToken,
			options.TokenStr,
			options.DryRun,
		}
	case "kubeconfig":
		flags = []string{
			options.CfgPath,
			options.ControlPlane,
			options.FileDiscovery,
			options.TokenDiscovery,
			options.TokenDiscoveryCAHash,
			options.TokenDiscoverySkipCAHash,
			options.TLSBootstrapToken,
			options.TokenStr,
			options.CertificateKey,
			options.DryRun,
		}
	case "control-plane":
		flags = []string{
			options.APIServerAdvertiseAddress,
			options.APIServerBindPort,
			options.CfgPath,
			options.ControlPlane,
			options.Patches,
			options.DryRun,
		}
	default:
		flags = []string{}
	}
	return flags
}

func newControlPlanePrepareDownloadCertsSubphase() workflow.Phase {
	return workflow.Phase{
		Name:         "download-certs [api-server-endpoint]",
		Short:        fmt.Sprintf("Download certificates shared among control-plane nodes from the %s Secret", kubeadmconstants.KubeadmCertsSecret),
		Run:          runControlPlanePrepareDownloadCertsPhaseLocal,
		InheritFlags: getControlPlanePreparePhaseFlags("download-certs"),
	}
}

func newControlPlanePrepareCertsSubphase() workflow.Phase {
	return workflow.Phase{
		Name:         "certs [api-server-endpoint]",
		Short:        "Generate the certificates for the new control plane components",
		Run:          runControlPlanePrepareCertsPhaseLocal, // 注意：未来可能按每张证书拆分为子阶段，或添加 --csr 参数。
		InheritFlags: getControlPlanePreparePhaseFlags("certs"),
	}
}

func newControlPlanePrepareKubeconfigSubphase() workflow.Phase {
	return workflow.Phase{
		Name:         "kubeconfig [api-server-endpoint]",
		Short:        "Generate the kubeconfig for the new control plane components",
		Run:          runControlPlanePrepareKubeconfigPhaseLocal, // 注意：未来可能按每个 kubeconfig 拆分为子阶段。
		InheritFlags: getControlPlanePreparePhaseFlags("kubeconfig"),
	}
}

func newControlPlanePrepareControlPlaneSubphase() workflow.Phase {
	return workflow.Phase{
		Name:          "control-plane",
		Short:         "Generate the manifests for the new control plane components",
		Run:           runControlPlanePrepareControlPlaneSubphase, // 注意：未来可能按每个组件拆分为子阶段。
		InheritFlags:  getControlPlanePreparePhaseFlags("control-plane"),
		ArgsValidator: cobra.NoArgs,
	}
}

func runControlPlanePrepareControlPlaneSubphase(c workflow.RunData) error {
	data, ok := c.(JoinData)
	if !ok {
		return errors.New("control-plane-prepare phase invoked with an invalid data struct")
	}

	// 若不是控制平面则跳过。
	if data.Cfg().ControlPlane == nil {
		return nil
	}

	cfg, err := data.InitCfg()
	if err != nil {
		return err
	}

	fmt.Printf("[control-plane] Using manifest folder %q\n", data.ManifestDir())

	// dry-run 时将 CertificatesDir 设为默认值，以在静态 Pod YAML 中获得正确的证书路径。
	if data.DryRun() {
		cfg.CertificatesDir = filepath.Join(kubeadmconstants.KubernetesDir, kubeadmconstants.DefaultCertificateDir)
	}

	for _, component := range kubeadmconstants.ControlPlaneComponents {
		fmt.Printf("[control-plane] Creating static Pod manifest for %q\n", component)
		err := controlplane.CreateStaticPodFiles(
			data.ManifestDir(),
			data.PatchesDir(),
			&cfg.ClusterConfiguration,
			&cfg.LocalAPIEndpoint,
			data.DryRun(),
			component,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func runControlPlanePrepareDownloadCertsPhaseLocal(c workflow.RunData) error {
	data, ok := c.(JoinData)
	if !ok {
		return errors.New("download-certs phase invoked with an invalid data struct")
	}

	if data.Cfg().ControlPlane == nil || len(data.CertificateKey()) == 0 {
		klog.V(1).Infoln("[download-certs] Skipping certs download")
		return nil
	}

	cfg, err := data.InitCfg()
	if err != nil {
		return err
	}

	// dry-run 时将证书下载到临时目录，并延迟恢复到用户最初指定的路径。
	certsDir := cfg.CertificatesDir
	cfg.CertificatesDir = data.CertificateWriteDir()
	defer func() { cfg.CertificatesDir = certsDir }()

	client, err := bootstrapClient(data)
	if err != nil {
		return err
	}

	if err := copycerts.DownloadCerts(client, cfg, data.CertificateKey()); err != nil {
		return errors.Wrap(err, "error downloading certs")
	}
	return nil
}

func runControlPlanePrepareCertsPhaseLocal(c workflow.RunData) error {
	data, ok := c.(JoinData)
	if !ok {
		return errors.New("control-plane-prepare phase invoked with an invalid data struct")
	}

	// 若不是控制平面则跳过。
	if data.Cfg().ControlPlane == nil {
		return nil
	}

	cfg, err := data.InitCfg()
	if err != nil {
		return err
	}

	fmt.Printf("[certs] Using certificateDir folder %q\n", cfg.CertificatesDir)

	// dry-run 时将证书文件写入临时目录，
	// 并延迟恢复到用户最初指定的路径。
	certsDir := cfg.CertificatesDir
	cfg.CertificatesDir = data.CertificateWriteDir()
	defer func() { cfg.CertificatesDir = certsDir }()
	// 生成缺失的证书（若有）。
	return certsphase.CreatePKIAssets(cfg)
}

func runControlPlanePrepareKubeconfigPhaseLocal(c workflow.RunData) error {
	data, ok := c.(JoinData)
	if !ok {
		return errors.New("control-plane-prepare phase invoked with an invalid data struct")
	}

	// 若不是控制平面则跳过。
	if data.Cfg().ControlPlane == nil {
		return nil
	}

	cfg, err := data.InitCfg()
	if err != nil {
		return err
	}

	fmt.Println("[kubeconfig] Generating kubeconfig files")
	fmt.Printf("[kubeconfig] Using kubeconfig folder %q\n", data.KubeConfigDir())

	// dry-run 时从临时目录加载证书，并延迟恢复到用户最初指定的路径。
	certsDir := cfg.CertificatesDir
	cfg.CertificatesDir = data.CertificateWriteDir()
	defer func() { cfg.CertificatesDir = certsDir }()

	// 为 controller manager、scheduler 以及 admin/kubeadm 自身生成 kubeconfig 文件。
	// 注意：kubelet 的 kubeconfig 文件将由后续 kubeadm join --control-plane 工作流中的 TLS 引导过程生成。
	if err := kubeconfigphase.CreateJoinControlPlaneKubeConfigFiles(data.KubeConfigDir(), cfg); err != nil {
		return errors.Wrap(err, "error generating kubeconfig files")
	}

	return nil
}

func bootstrapClient(data JoinData) (clientset.Interface, error) {
	if data.DryRun() {
		client, err := data.Client()
		if err != nil {
			return nil, err
		}
		return client, nil
	}

	tlsBootstrapCfg, err := data.TLSBootstrapCfg()
	if err != nil {
		return nil, errors.Wrap(err, "unable to access the cluster")
	}
	client, err := kubeconfigutil.ToClientSet(tlsBootstrapCfg)
	if err != nil {
		return nil, errors.Wrap(err, "unable to access the cluster")
	}
	return client, nil
}
