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

package upgrade

import (
	"io"
	"os"

	"github.com/pmezard/go-difflib/difflib"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/version"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	"k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/v1beta4"
	"k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/validation"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/options"
	cmdutil "k8s.io/kubernetes/cmd/kubeadm/app/cmd/util"
	"k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/phases/controlplane"
	kubeadmutil "k8s.io/kubernetes/cmd/kubeadm/app/util"
	configutil "k8s.io/kubernetes/cmd/kubeadm/app/util/config"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	kubeconfigutil "k8s.io/kubernetes/cmd/kubeadm/app/util/kubeconfig"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/output"
	staticpodutil "k8s.io/kubernetes/cmd/kubeadm/app/util/staticpod"
)

type diffFlags struct {
	apiServerManifestPath         string
	controllerManagerManifestPath string
	schedulerManifestPath         string
	newK8sVersionStr              string
	contextLines                  int
	kubeConfigPath                string
	cfgPath                       string
	out                           io.Writer
}

// newCmdDiff 返回 kubeadm upgrade diff 的 cobra 命令。
func newCmdDiff(out io.Writer) *cobra.Command {
	flags := &diffFlags{
		kubeConfigPath:                constants.GetAdminKubeConfigPath(),
		out:                           out,
		apiServerManifestPath:         constants.GetStaticPodFilepath(constants.KubeAPIServer, constants.GetStaticPodDirectory()),
		controllerManagerManifestPath: constants.GetStaticPodFilepath(constants.KubeControllerManager, constants.GetStaticPodDirectory()),
		schedulerManifestPath:         constants.GetStaticPodFilepath(constants.KubeScheduler, constants.GetStaticPodDirectory()),
	}

	cmd := &cobra.Command{
		Use:   "diff [version]",
		Short: "Show what differences would be applied to existing static pod manifests. See also: kubeadm upgrade apply --dry-run",
		RunE: func(cmd *cobra.Command, args []string) error {
			// 为 diff 运行预检，确认清单已存在。
			if err := validateManifestsPath(
				flags.apiServerManifestPath,
				flags.controllerManagerManifestPath,
				flags.schedulerManifestPath); err != nil {
				return err
			}

			if err := validation.ValidateMixedArguments(cmd.Flags()); err != nil {
				return err
			}
			return runDiff(cmd.Flags(), flags, args, configutil.FetchInitConfigurationFromCluster)
		},
	}

	options.AddKubeConfigFlag(cmd.Flags(), &flags.kubeConfigPath)
	options.AddConfigFlag(cmd.Flags(), &flags.cfgPath)
	cmd.Flags().IntVarP(&flags.contextLines, "context-lines", "c", 3, "How many lines of context in the diff")
	return cmd
}

func validateManifestsPath(manifests ...string) (err error) {
	for _, manifestPath := range manifests {
		s, err := os.Stat(manifestPath)
		if err != nil {
			if os.IsNotExist(err) {
				return errors.Wrapf(err, "the manifest file %q does not exist", manifestPath)
			}
			return errors.Wrapf(err, "error obtaining stats for manifest file %q", manifestPath)
		}
		if s.IsDir() {
			return errors.Errorf("%q is a directory", manifestPath)
		}
	}
	return nil
}

// FetchInitConfigurationFunc 定义从集群获取 InitConfiguration 的函数签名。
type FetchInitConfigurationFunc func(client clientset.Interface, printer output.Printer, logPrefix string, getNodeRegistration, getAPIEndpoint, getComponentConfigs, shortConfigMapGet bool) (*kubeadmapi.InitConfiguration, error)

func runDiff(fs *pflag.FlagSet, flags *diffFlags, args []string, fetchInitConfigurationFromCluster FetchInitConfigurationFunc) error {
	externalCfg := &v1beta4.UpgradeConfiguration{}
	opt := configutil.LoadOrDefaultConfigurationOptions{}
	upgradeCfg, err := configutil.LoadOrDefaultUpgradeConfiguration(flags.cfgPath, externalCfg, opt)
	if err != nil {
		return err
	}
	client, err := kubeconfigutil.ClientSetFromFile(flags.kubeConfigPath)
	if err != nil {
		return errors.Wrapf(err, "couldn't create a Kubernetes client from file %q", flags.kubeConfigPath)
	}
	getNodeRegistration := true
	getAPIEndpoint := staticpodutil.IsControlPlaneNode()
	getComponentConfigs := false
	initCfg, err := fetchInitConfigurationFromCluster(client, &output.TextPrinter{}, "upgrade/diff", getNodeRegistration, getAPIEndpoint, getComponentConfigs, false)
	if err != nil {
		return err
	}

	// 从 ClusterConfiguration 获取版本。
	if initCfg.KubernetesVersion != "" {
		flags.newK8sVersionStr = initCfg.KubernetesVersion
	}
	if upgradeCfg.Diff.KubernetesVersion != "" {
		flags.newK8sVersionStr = upgradeCfg.Diff.KubernetesVersion
	}

	// 若 ClusterConfiguration 未设置版本，则必须通过版本参数指定。
	if flags.newK8sVersionStr == "" {
		if err := cmdutil.ValidateExactArgNumber(args, []string{"version"}); err != nil {
			return err
		}
	}
	// 若参数和配置文件都指定了选项，则参数覆盖配置文件。
	if len(args) == 1 {
		flags.newK8sVersionStr = args[0]
	}
	_, err = version.ParseSemantic(flags.newK8sVersionStr)
	if err != nil {
		return err
	}

	initCfg.ClusterConfiguration.KubernetesVersion = flags.newK8sVersionStr

	specs := controlplane.GetStaticPodSpecs(&initCfg.ClusterConfiguration, &initCfg.LocalAPIEndpoint, nil)
	for spec, pod := range specs {
		var path string
		switch spec {
		case constants.KubeAPIServer:
			path = flags.apiServerManifestPath
		case constants.KubeControllerManager:
			path = flags.controllerManagerManifestPath
		case constants.KubeScheduler:
			path = flags.schedulerManifestPath
		default:
			klog.Errorf("[diff] unknown spec %v", spec)
			continue
		}
		newManifest, err := kubeadmutil.MarshalToYaml(&pod, corev1.SchemeGroupVersion)
		if err != nil {
			return err
		}
		if path == "" {
			return errors.New("empty manifest path")
		}
		existingManifest, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		// 填充并输出差异。
		diff := difflib.UnifiedDiff{
			A:        difflib.SplitLines(string(existingManifest)),
			B:        difflib.SplitLines(string(newManifest)),
			FromFile: path,
			ToFile:   "new manifest",
			Context:  cmdutil.ValueFromFlagsOrConfig(fs, "context-lines", upgradeCfg.Diff.DiffContextLines, flags.contextLines).(int),
		}

		if err = difflib.WriteUnifiedDiff(flags.out, diff); err != nil {
			return errors.Wrap(err, "error writing unified diff")
		}
	}
	return nil
}
