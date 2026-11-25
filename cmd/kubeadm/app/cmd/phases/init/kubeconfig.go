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

	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/options"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/phases/workflow"
	cmdutil "k8s.io/kubernetes/cmd/kubeadm/app/cmd/util"
	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	kubeconfigphase "k8s.io/kubernetes/cmd/kubeadm/app/phases/kubeconfig"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	filesutil "k8s.io/kubernetes/cmd/kubeadm/app/util/files"
)

var (
	kubeconfigFilePhaseProperties = map[string]struct {
		name  string
		short string
		long  string
	}{
		kubeadmconstants.AdminKubeConfigFileName: {
			name:  "admin",
			short: "Generate a kubeconfig file for the admin to use and for kubeadm itself",
			long:  "Generate the kubeconfig file for the admin and for kubeadm itself, and save it to %s file.",
		},
		kubeadmconstants.SuperAdminKubeConfigFileName: {
			name:  "super-admin",
			short: "Generate a kubeconfig file for the super-admin",
			long:  "Generate a kubeconfig file for the super-admin, and save it to %s file.",
		},
		kubeadmconstants.KubeletKubeConfigFileName: {
			name:  "kubelet",
			short: "Generate a kubeconfig file for the kubelet to use *only* for cluster bootstrapping purposes",
			long: cmdutil.LongDesc(`
					Generate the kubeconfig file for the kubelet to use and save it to %s file.

					Please note that this should *only* be used for cluster bootstrapping purposes. After your control plane is up,
					you should request all kubelet credentials from the CSR API.`),
		},
		kubeadmconstants.ControllerManagerKubeConfigFileName: {
			name:  "controller-manager",
			short: "Generate a kubeconfig file for the controller manager to use",
			long:  "Generate the kubeconfig file for the controller manager to use and save it to %s file",
		},
		kubeadmconstants.SchedulerKubeConfigFileName: {
			name:  "scheduler",
			short: "Generate a kubeconfig file for the scheduler to use",
			long:  "Generate the kubeconfig file for the scheduler to use and save it to %s file.",
		},
	}
)

// NewKubeConfigPhase 创建 kubeadm 工作流阶段，生成建立控制平面所需的全部 kubeconfig 文件和管理员 kubeconfig 文件。
func NewKubeConfigPhase() workflow.Phase {
	return workflow.Phase{
		Name:  "kubeconfig",
		Short: "Generate all kubeconfig files necessary to establish the control plane and the admin kubeconfig file",
		Phases: []workflow.Phase{
			{
				Name:           "all",
				Short:          "Generate all kubeconfig files",
				InheritFlags:   getKubeConfigPhaseFlags("all"),
				RunAllSiblings: true,
			},
			NewKubeConfigFilePhase(kubeadmconstants.AdminKubeConfigFileName),
			NewKubeConfigFilePhase(kubeadmconstants.SuperAdminKubeConfigFileName),
			NewKubeConfigFilePhase(kubeadmconstants.KubeletKubeConfigFileName),
			NewKubeConfigFilePhase(kubeadmconstants.ControllerManagerKubeConfigFileName),
			NewKubeConfigFilePhase(kubeadmconstants.SchedulerKubeConfigFileName),
		},
		Run: runKubeConfig,
	}
}

// NewKubeConfigFilePhase 创建用于生成 kubeconfig 文件的 kubeadm 工作流阶段。
func NewKubeConfigFilePhase(kubeConfigFileName string) workflow.Phase {
	return workflow.Phase{
		Name:         kubeconfigFilePhaseProperties[kubeConfigFileName].name,
		Short:        kubeconfigFilePhaseProperties[kubeConfigFileName].short,
		Long:         fmt.Sprintf(kubeconfigFilePhaseProperties[kubeConfigFileName].long, kubeConfigFileName),
		Run:          runKubeConfigFile(kubeConfigFileName),
		InheritFlags: getKubeConfigPhaseFlags(kubeConfigFileName),
	}
}

func getKubeConfigPhaseFlags(name string) []string {
	flags := []string{
		options.APIServerAdvertiseAddress,
		options.ControlPlaneEndpoint,
		options.APIServerBindPort,
		options.CertificatesDir,
		options.CfgPath,
		options.KubeconfigDir,
		options.KubernetesVersion,
		options.DryRun,
	}
	if name == "all" || name == kubeadmconstants.KubeletKubeConfigFileName {
		flags = append(flags,
			options.NodeName,
		)
	}
	return flags
}

func runKubeConfig(c workflow.RunData) error {
	data, ok := c.(InitData)
	if !ok {
		return errors.New("kubeconfig phase invoked with an invalid data struct")
	}

	fmt.Printf("[kubeconfig] Using kubeconfig folder %q\n", data.KubeConfigDir())
	return nil
}

// runKubeConfigFile 执行 kubeconfig 创建逻辑。
func runKubeConfigFile(kubeConfigFileName string) func(workflow.RunData) error {
	return func(c workflow.RunData) error {
		data, ok := c.(InitData)
		if !ok {
			return errors.New("kubeconfig phase invoked with an invalid data struct")
		}

		// 外部 CA 模式下跳过证书颁发机构生成。
		if data.ExternalCA() {
			fmt.Printf("[kubeconfig] External CA mode: Using user provided %s\n", kubeConfigFileName)
			// 使用外部 CA 且 dry-run 时，将 kubeconfig 文件复制到 dry-run 目录供后续使用。
			if data.DryRun() {
				err := filesutil.CopyFile(filepath.Join(kubeadmconstants.KubernetesDir, kubeConfigFileName), filepath.Join(data.KubeConfigDir(), kubeConfigFileName))
				if err != nil {
					return errors.Wrapf(err, "could not copy %s to dry run directory %s", kubeConfigFileName, data.KubeConfigDir())
				}
			}
			return nil
		}

		// dry-run 时从临时目录读取证书，
		// 并延迟恢复到用户最初指定的路径。
		cfg := data.Cfg()
		cfg.CertificatesDir = data.CertificateWriteDir()
		defer func() { cfg.CertificatesDir = data.CertificateDir() }()

		initConfiguration := data.Cfg().DeepCopy()

		if kubeConfigFileName == kubeadmconstants.KubeletKubeConfigFileName {
			// 取消设置 ControlPlaneEndpoint，使 kubelet 的 kubeconfig 回退到 LocalAPIEndpoint。
			initConfiguration.ControlPlaneEndpoint = ""
		}

		// 创建 KubeConfig 文件（或使用已有文件）。
		return kubeconfigphase.CreateKubeConfigFile(kubeConfigFileName, data.KubeConfigDir(), initConfiguration)
	}
}
