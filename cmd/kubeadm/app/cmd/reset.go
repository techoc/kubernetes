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

package cmd

import (
	"fmt"
	"io"
	"os"
	"path"

	"github.com/lithammer/dedent"
	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"

	"k8s.io/apimachinery/pkg/util/sets"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	kubeadmscheme "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/scheme"
	kubeadmapiv1 "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/v1beta4"
	"k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/validation"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/options"
	phases "k8s.io/kubernetes/cmd/kubeadm/app/cmd/phases/reset"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/phases/workflow"
	cmdutil "k8s.io/kubernetes/cmd/kubeadm/app/cmd/util"
	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/apiclient"
	configutil "k8s.io/kubernetes/cmd/kubeadm/app/util/config"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	kubeconfigutil "k8s.io/kubernetes/cmd/kubeadm/app/util/kubeconfig"
	utilruntime "k8s.io/kubernetes/cmd/kubeadm/app/util/runtime"
	staticpodutil "k8s.io/kubernetes/cmd/kubeadm/app/util/staticpod"
)

var (
	manualCleanupInstructions = dedent.Dedent(`
		The reset process does not perform cleanup of CNI plugin configuration,
		network filtering rules and kubeconfig files.

		For information on how to perform this cleanup manually, please see:
		    https://k8s.io/docs/reference/setup-tools/kubeadm/kubeadm-reset/

	`)
)

// resetOptions 定义 kubeadm reset 通过参数暴露的全部选项。
type resetOptions struct {
	kubeconfigPath        string
	cfgPath               string
	ignorePreflightErrors []string
	externalcfg           *kubeadmapiv1.ResetConfiguration
	skipCRIDetect         bool
}

// resetData 定义运行 kubeadm reset 工作流所需的全部运行时信息，
// 并供工作流包含的所有阶段共享。
type resetData struct {
	certificatesDir       string
	client                clientset.Interface
	criSocketPath         string
	forceReset            bool
	ignorePreflightErrors sets.Set[string]
	inputReader           io.Reader
	outputWriter          io.Writer
	cfg                   *kubeadmapi.InitConfiguration
	resetCfg              *kubeadmapi.ResetConfiguration
	dryRun                bool
	cleanupTmpDir         bool
}

// newResetOptions 返回可用于创建 reset 命令参数的结构体。
func newResetOptions() *resetOptions {
	// 应用默认值以初始化公开的 kubeadm 配置 API。
	externalcfg := &kubeadmapiv1.ResetConfiguration{}
	// 应用默认值。
	kubeadmscheme.Scheme.Default(externalcfg)
	return &resetOptions{
		kubeconfigPath: kubeadmconstants.GetAdminKubeConfigPath(),
		externalcfg:    externalcfg,
	}
}

// newResetData 返回用于执行 kubeadm reset 工作流的新 resetData。
func newResetData(cmd *cobra.Command, opts *resetOptions, in io.Reader, out io.Writer, allowExperimental bool) (*resetData, error) {
	// 校验 --config 与其他参数的组合，出错时提前返回。
	if err := validation.ValidateMixedArguments(cmd.Flags()); err != nil {
		return nil, err
	}

	var (
		initCfg *kubeadmapi.InitConfiguration
		client  clientset.Interface
	)

	// 若指定配置文件则使用它，否则将公开 kubeadm API 转换为内部 ResetConfiguration 并校验。
	resetCfg, err := configutil.LoadOrDefaultResetConfiguration(opts.cfgPath, opts.externalcfg, configutil.LoadOrDefaultConfigurationOptions{
		AllowExperimental: allowExperimental,
		SkipCRIDetect:     opts.skipCRIDetect,
	})
	if err != nil {
		return nil, err
	}

	dryRunFlag := cmdutil.ValueFromFlagsOrConfig(cmd.Flags(), options.DryRun, resetCfg.DryRun, opts.externalcfg.DryRun).(bool)
	if dryRunFlag {
		dryRun := apiclient.NewDryRun().WithDefaultMarshalFunction().WithWriter(os.Stdout)
		dryRun.AppendReactor(dryRun.GetKubeadmConfigReactor()).
			AppendReactor(dryRun.GetKubeletConfigReactor()).
			AppendReactor(dryRun.GetKubeProxyConfigReactor())
		client = dryRun.FakeClient()
		_, err = os.Stat(opts.kubeconfigPath)
		if err == nil {
			err = dryRun.WithKubeConfigFile(opts.kubeconfigPath)
		}
	} else {
		client, err = kubeconfigutil.ClientSetFromFile(opts.kubeconfigPath)
	}

	if err == nil {
		klog.V(1).Infof("[reset] Loaded client set from kubeconfig file: %s", opts.kubeconfigPath)
		getNodeRegistration := true
		getAPIEndpoint := staticpodutil.IsControlPlaneNode()
		getComponentConfigs := true
		initCfg, err = configutil.FetchInitConfigurationFromCluster(client, nil, "reset", getNodeRegistration, getAPIEndpoint, getComponentConfigs, true)
		if err != nil {
			klog.Warningf("[reset] Unable to fetch the kubeadm-config ConfigMap from cluster: %v", err)
		}
	} else {
		klog.V(1).Infof("[reset] Could not obtain a client set from the kubeconfig file: %s", opts.kubeconfigPath)
	}

	ignorePreflightErrorsSet, err := validation.ValidateIgnorePreflightErrors(opts.ignorePreflightErrors, resetCfg.IgnorePreflightErrors)
	if err != nil {
		return nil, err
	}
	if initCfg != nil {
		// 同时将预检错误的并集写入 InitConfiguration，使运行时配置视图保持一致：
		initCfg.NodeRegistration.IgnorePreflightErrors = sets.List(ignorePreflightErrorsSet)
	}

	criSocketPath := opts.externalcfg.CRISocket
	if criSocketPath == "" {
		criSocketPath, err = resetDetectCRISocket(resetCfg, initCfg)
		if err != nil {
			return nil, err
		}
		klog.V(1).Infof("[reset] Using specified CRI socket: %s", criSocketPath)
	}

	certificatesDir := kubeadmapiv1.DefaultCertificatesDir
	if cmd.Flags().Changed(options.CertificatesDir) { // 已指定参数。
		certificatesDir = opts.externalcfg.CertificatesDir
	} else if len(resetCfg.CertificatesDir) > 0 { // 已在 ResetConfiguration 中配置。
		certificatesDir = resetCfg.CertificatesDir
	} else if initCfg != nil && len(initCfg.ClusterConfiguration.CertificatesDir) > 0 { // 从集群获取。
		certificatesDir = initCfg.ClusterConfiguration.CertificatesDir
	}

	return &resetData{
		certificatesDir:       certificatesDir,
		client:                client,
		criSocketPath:         criSocketPath,
		ignorePreflightErrors: ignorePreflightErrorsSet,
		inputReader:           in,
		outputWriter:          out,
		cfg:                   initCfg,
		resetCfg:              resetCfg,
		dryRun:                dryRunFlag,
		forceReset:            cmdutil.ValueFromFlagsOrConfig(cmd.Flags(), options.Force, resetCfg.Force, opts.externalcfg.Force).(bool),
		cleanupTmpDir:         cmdutil.ValueFromFlagsOrConfig(cmd.Flags(), options.CleanupTmpDir, resetCfg.CleanupTmpDir, opts.externalcfg.CleanupTmpDir).(bool),
	}, nil
}

// AddResetFlags 添加 reset 参数。
func AddResetFlags(flagSet *flag.FlagSet, resetOptions *resetOptions) {
	flagSet.StringVar(
		&resetOptions.externalcfg.CertificatesDir, options.CertificatesDir, kubeadmapiv1.DefaultCertificatesDir,
		`The path to the directory where the certificates are stored. If specified, clean this directory.`,
	)
	flagSet.BoolVarP(
		&resetOptions.externalcfg.Force, options.Force, "f", resetOptions.externalcfg.Force,
		"Reset the node without prompting for confirmation.",
	)
	flagSet.BoolVar(
		&resetOptions.externalcfg.DryRun, options.DryRun, resetOptions.externalcfg.DryRun,
		"Don't apply any changes; just output what would be done.",
	)
	flagSet.BoolVar(
		&resetOptions.externalcfg.CleanupTmpDir, options.CleanupTmpDir, resetOptions.externalcfg.CleanupTmpDir,
		fmt.Sprintf("Cleanup the %q directory", path.Join(kubeadmconstants.KubernetesDir, kubeadmconstants.TempDir)),
	)
	options.AddKubeConfigFlag(flagSet, &resetOptions.kubeconfigPath)
	options.AddConfigFlag(flagSet, &resetOptions.cfgPath)
	options.AddIgnorePreflightErrorsFlag(flagSet, &resetOptions.ignorePreflightErrors)
	cmdutil.AddCRISocketFlag(flagSet, &resetOptions.externalcfg.CRISocket)
}

// newCmdReset 返回 kubeadm reset 命令。
func newCmdReset(in io.Reader, out io.Writer, resetOptions *resetOptions) *cobra.Command {
	if resetOptions == nil {
		resetOptions = newResetOptions()
	}
	resetRunner := workflow.NewRunner()

	cmd := &cobra.Command{
		Use:   "reset",
		Short: "Performs a best effort revert of changes made to this host by 'kubeadm init' or 'kubeadm join'",
		RunE: func(cmd *cobra.Command, args []string) error {
			data, err := resetRunner.InitData(args)
			if err != nil {
				return err
			}
			if _, ok := data.(*resetData); !ok {
				return errors.New("invalid data struct")
			}
			if err := resetRunner.Run(args); err != nil {
				return err
			}

			fmt.Print(manualCleanupInstructions)
			return nil
		},
	}

	AddResetFlags(cmd.Flags(), resetOptions)
	// 使用阶段列表初始化工作流运行器。
	resetRunner.AppendPhase(phases.NewPreflightPhase())
	resetRunner.AppendPhase(phases.NewRemoveETCDMemberPhase())
	resetRunner.AppendPhase(phases.NewCleanupNodePhase())

	// 为运行器设置数据构建函数，运行完整工作流或单个阶段时都会调用。
	resetRunner.SetDataInitializer(func(cmd *cobra.Command, args []string) (workflow.RunData, error) {
		if cmd.Flags().Lookup(options.NodeCRISocket) == nil {
			// 跳过 CRI 检测。
			// 未设置 --cri-socket 时，假定命令执行不依赖 CRISocket。
			resetOptions.skipCRIDetect = true
		}
		data, err := newResetData(cmd, resetOptions, in, out, true)
		if err != nil {
			return nil, err
		}
		// 若跳过阶段的参数为空，则使用配置中的值。
		if len(resetRunner.Options.SkipPhases) == 0 {
			resetRunner.Options.SkipPhases = data.resetCfg.SkipPhases
		}
		return data, nil
	})

	// 将 Runner 绑定到 kubeadm reset 命令：修改帮助信息、
	// 添加 --skip-phases 参数并添加阶段子命令。
	resetRunner.BindToCommand(cmd)

	return cmd
}

// ResetCfg 返回 ResetConfiguration。
func (r *resetData) ResetCfg() *kubeadmapi.ResetConfiguration {
	return r.resetCfg
}

// Cfg 返回 InitConfiguration。
func (r *resetData) Cfg() *kubeadmapi.InitConfiguration {
	return r.cfg
}

// DryRun 返回 dryRun 参数的值。
func (r *resetData) DryRun() bool {
	return r.dryRun
}

// CleanupTmpDir 返回 cleanupTmpDir 参数的值。
func (r *resetData) CleanupTmpDir() bool {
	return r.cleanupTmpDir
}

// CertificatesDir 返回 CertificatesDir。
func (r *resetData) CertificatesDir() string {
	return r.certificatesDir
}

// Client 返回用于访问集群的 Client。
func (r *resetData) Client() clientset.Interface {
	return r.client
}

// ForceReset 返回 forceReset 参数的值。
func (r *resetData) ForceReset() bool {
	return r.forceReset
}

// InputReader 返回用于读取消息的 io.Reader。
func (r *resetData) InputReader() io.Reader {
	return r.inputReader
}

// IgnorePreflightErrors 返回要忽略的预检错误列表。
func (r *resetData) IgnorePreflightErrors() sets.Set[string] {
	return r.ignorePreflightErrors
}

// CRISocketPath 返回 criSocketPath。
func (r *resetData) CRISocketPath() string {
	return r.criSocketPath
}

func resetDetectCRISocket(resetCfg *kubeadmapi.ResetConfiguration, initCfg *kubeadmapi.InitConfiguration) (string, error) {
	if resetCfg != nil && len(resetCfg.CRISocket) > 0 {
		return resetCfg.CRISocket, nil
	}
	if initCfg != nil && len(initCfg.NodeRegistration.CRISocket) > 0 {
		return initCfg.NodeRegistration.CRISocket, nil
	}

	// 尝试在主机上检测。
	return utilruntime.DetectCRISocket()
}
