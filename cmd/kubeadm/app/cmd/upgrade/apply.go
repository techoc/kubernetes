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

package upgrade

import (
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	"k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/v1beta4"
	"k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/validation"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/options"
	commonphases "k8s.io/kubernetes/cmd/kubeadm/app/cmd/phases/upgrade"
	phases "k8s.io/kubernetes/cmd/kubeadm/app/cmd/phases/upgrade/apply"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/phases/workflow"
	cmdutil "k8s.io/kubernetes/cmd/kubeadm/app/cmd/util"
	"k8s.io/kubernetes/cmd/kubeadm/app/constants"
	configutil "k8s.io/kubernetes/cmd/kubeadm/app/util/config"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/output"
)

// applyFlags 保存可传给 apply 的参数信息。
type applyFlags struct {
	*applyPlanFlags

	nonInteractiveMode bool
	force              bool
	dryRun             bool
	renewCerts         bool
	patchesDir         string
}

// 编译期断言本地数据对象满足阶段数据接口。
var _ phases.Data = &applyData{}

// applyData 定义运行 kubeadm upgrade apply 工作流所需的全部运行时信息，
// 并供工作流包含的所有阶段共享。
type applyData struct {
	nonInteractiveMode        bool
	force                     bool
	dryRun                    bool
	dryRunDir                 string
	etcdUpgrade               bool
	renewCerts                bool
	allowExperimentalUpgrades bool
	allowRCUpgrades           bool
	printConfig               bool
	cfg                       *kubeadmapi.UpgradeConfiguration
	initCfg                   *kubeadmapi.InitConfiguration
	client                    clientset.Interface
	patchesDir                string
	ignorePreflightErrors     sets.Set[string]
	outputWriter              io.Writer
}

// newCmdApply 返回 kubeadm upgrade apply 的 cobra 命令。
func newCmdApply(apf *applyPlanFlags) *cobra.Command {
	flags := &applyFlags{
		applyPlanFlags: apf,
		renewCerts:     true,
	}

	applyRunner := workflow.NewRunner()

	cmd := &cobra.Command{
		Use:                   "apply [version]",
		DisableFlagsInUseLine: true,
		Short:                 "Upgrade your Kubernetes cluster to the specified version",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := validation.ValidateMixedArguments(cmd.Flags()); err != nil {
				return err
			}

			data, err := applyRunner.InitData(args)
			if err != nil {
				return err
			}
			applyData, ok := data.(*applyData)
			if !ok {
				return errors.New("invalid data struct")
			}
			if err := applyRunner.Run(args); err != nil {
				return err
			}
			if applyData.DryRun() {
				fmt.Println("[upgrade/successful] Finished dryrunning successfully!")
				return nil
			}

			fmt.Println("")
			fmt.Printf("[upgrade] SUCCESS! A control plane node of your cluster was upgraded to %q.\n\n", applyData.InitCfg().KubernetesVersion)
			fmt.Println("[upgrade] Now please proceed with upgrading the rest of the nodes by following the right order.")

			return nil
		},
	}

	// 注册 apply 和 plan 的公共参数。
	addApplyPlanFlags(cmd.Flags(), flags.applyPlanFlags)
	// 指定 apply 特有的合法参数。
	cmd.Flags().BoolVarP(&flags.nonInteractiveMode, "yes", "y", flags.nonInteractiveMode, "Perform the upgrade and do not prompt for confirmation (non-interactive mode).")
	cmd.Flags().BoolVarP(&flags.force, options.Force, "f", flags.force, "Force upgrading although some requirements might not be met. This also implies non-interactive mode.")
	cmd.Flags().BoolVar(&flags.dryRun, options.DryRun, flags.dryRun, "Do not change any state, just output what actions would be performed.")
	cmd.Flags().BoolVar(&flags.renewCerts, options.CertificateRenewal, flags.renewCerts, "Perform the renewal of certificates used by component changed during upgrades.")
	options.AddPatchesFlag(cmd.Flags(), &flags.patchesDir)

	// 使用阶段列表初始化工作流运行器。
	applyRunner.AppendPhase(phases.NewPreflightPhase())
	applyRunner.AppendPhase(phases.NewControlPlanePhase())
	applyRunner.AppendPhase(phases.NewUploadConfigPhase())
	applyRunner.AppendPhase(phases.NewKubeconfigPhase())
	applyRunner.AppendPhase(commonphases.NewKubeletConfigPhase())
	applyRunner.AppendPhase(phases.NewBootstrapTokenPhase())
	applyRunner.AppendPhase(commonphases.NewAddonPhase())
	applyRunner.AppendPhase(commonphases.NewPostUpgradePhase())

	// 为运行器设置数据构建函数，运行完整工作流或单个阶段时都会调用。
	applyRunner.SetDataInitializer(func(cmd *cobra.Command, args []string) (workflow.RunData, error) {
		data, err := newApplyData(cmd, args, flags)
		if err != nil {
			return nil, err
		}
		// 若跳过阶段的参数为空，则使用配置中的值。
		if len(applyRunner.Options.SkipPhases) == 0 {
			applyRunner.Options.SkipPhases = data.cfg.Apply.SkipPhases
		}
		return data, nil
	})

	// 将 Runner 绑定到 kubeadm upgrade apply 命令：修改帮助信息、
	// 添加 --skip-phases 参数并添加阶段子命令。
	applyRunner.BindToCommand(cmd)

	return cmd
}

// newApplyData 返回用于执行 kubeadm upgrade apply 工作流的新 applyData。
func newApplyData(cmd *cobra.Command, args []string, applyFlags *applyFlags) (*applyData, error) {
	externalCfg := &v1beta4.UpgradeConfiguration{}
	opt := configutil.LoadOrDefaultConfigurationOptions{}
	upgradeCfg, err := configutil.LoadOrDefaultUpgradeConfiguration(applyFlags.cfgPath, externalCfg, opt)
	if err != nil {
		return nil, err
	}

	upgradeVersion := upgradeCfg.Apply.KubernetesVersion
	// 除非配置文件中已指定，否则版本参数为必填项。
	if upgradeVersion == "" {
		if err := cmdutil.ValidateExactArgNumber(args, []string{"version"}); err != nil {
			return nil, err
		}
	}

	// 若参数和配置文件都指定了版本，则参数覆盖配置文件。
	if len(args) == 1 {
		upgradeVersion = args[0]
	}

	ignorePreflightErrorsSet, err := validation.ValidateIgnorePreflightErrors(applyFlags.ignorePreflightErrors, upgradeCfg.Apply.IgnorePreflightErrors)
	if err != nil {
		return nil, err
	}

	force, ok := cmdutil.ValueFromFlagsOrConfig(cmd.Flags(), options.Force, upgradeCfg.Apply.ForceUpgrade, &applyFlags.force).(*bool)
	if !ok {
		return nil, cmdutil.TypeMismatchErr("forceUpgrade", "bool")
	}

	dryRun, ok := cmdutil.ValueFromFlagsOrConfig(cmd.Flags(), options.DryRun, upgradeCfg.Apply.DryRun, &applyFlags.dryRun).(*bool)
	if !ok {
		return nil, cmdutil.TypeMismatchErr("dryRun", "bool")
	}

	// 若为 dry-run，创建用于保存 kubeadm 生成文件的临时目录。
	dryRunDir := ""
	if *dryRun {
		if dryRunDir, err = constants.GetDryRunDir(constants.EnvVarUpgradeDryRunDir, "kubeadm-upgrade-apply-dryrun", klog.Warningf); err != nil {
			return nil, errors.Wrap(err, "could not create a temporary directory on dryrun")
		}
	}

	etcdUpgrade, ok := cmdutil.ValueFromFlagsOrConfig(cmd.Flags(), options.EtcdUpgrade, upgradeCfg.Apply.EtcdUpgrade, &applyFlags.etcdUpgrade).(*bool)
	if !ok {
		return nil, cmdutil.TypeMismatchErr("etcdUpgrade", "bool")
	}

	renewCerts, ok := cmdutil.ValueFromFlagsOrConfig(cmd.Flags(), options.CertificateRenewal, upgradeCfg.Apply.CertificateRenewal, &applyFlags.renewCerts).(*bool)
	if !ok {
		return nil, cmdutil.TypeMismatchErr("certificateRenewal", "bool")
	}

	allowExperimentalUpgrades, ok := cmdutil.ValueFromFlagsOrConfig(cmd.Flags(), "allow-experimental-upgrades", upgradeCfg.Apply.AllowExperimentalUpgrades, &applyFlags.allowExperimentalUpgrades).(*bool)
	if !ok {
		return nil, cmdutil.TypeMismatchErr("allowExperimentalUpgrades", "bool")
	}

	allowRCUpgrades, ok := cmdutil.ValueFromFlagsOrConfig(cmd.Flags(), "allow-release-candidate-upgrades", upgradeCfg.Apply.AllowRCUpgrades, &applyFlags.allowRCUpgrades).(*bool)
	if !ok {
		return nil, cmdutil.TypeMismatchErr("allowRCUpgrades", "bool")
	}

	printConfig, ok := cmdutil.ValueFromFlagsOrConfig(cmd.Flags(), "print-config", upgradeCfg.Apply.PrintConfig, &applyFlags.printConfig).(*bool)
	if !ok {
		return nil, cmdutil.TypeMismatchErr("printConfig", "bool")
	}

	printer := &output.TextPrinter{}

	client, err := getClient(applyFlags.kubeConfigPath, *dryRun, printer)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't create a Kubernetes client from file %q", applyFlags.kubeConfigPath)
	}

	// 获取集群配置。
	klog.V(1).Infoln("[upgrade] retrieving configuration from cluster")
	getNodeRegistration := true
	isControlPlaneNode := true
	getComponentConfigs := true
	initCfg, err := configutil.FetchInitConfigurationFromCluster(client, nil, "upgrade", getNodeRegistration, isControlPlaneNode, getComponentConfigs, false)
	if err != nil {
		if apierrors.IsNotFound(err) {
			_, _ = printer.Printf("[upgrade] In order to upgrade, a ConfigMap called %q in the %q namespace must exist.\n", constants.KubeadmConfigConfigMap, metav1.NamespaceSystem)
			_, _ = printer.Printf("[upgrade/config] Use 'kubeadm init phase upload-config kubeadm --config your-config-file' to re-upload it.\n")
			err = errors.Errorf("the ConfigMap %q in the %q namespace was not found", constants.KubeadmConfigConfigMap, metav1.NamespaceSystem)
		}
		return nil, errors.Wrap(err, "[upgrade] FATAL")
	}

	// 同时将预检错误的并集写入 InitConfiguration，使运行时配置视图保持一致：
	initCfg.NodeRegistration.IgnorePreflightErrors = sets.List(ignorePreflightErrorsSet)

	// 将 UpgradeApplyConfiguration 中的 ImagePullPolicy 和 ImagePullSerial
	// 写入 InitConfiguration，供运行 apply 时的 preflight.RunPullImagesCheck() 使用。
	initCfg.NodeRegistration.ImagePullPolicy = upgradeCfg.Apply.ImagePullPolicy
	initCfg.NodeRegistration.ImagePullSerial = upgradeCfg.Apply.ImagePullSerial

	// upgrade apply 的版本参数总会用目标版本覆盖返回 cfg 中的 KubernetesVersion。
	// upgrade plan 则不同，其 KubernetesVersion 应保持旧版本，
	// 因为 getComponentConfigVersionStates 需要当前已安装的版本。
	// 这也使 upgrade plan 返回的 KubernetesVersion 保持一致：该命令允许不指定目标版本，
	// 此时 KubernetesVersion 始终为当前已安装版本。
	initCfg.KubernetesVersion = upgradeVersion

	var patchesDir string
	if upgradeCfg.Apply.Patches != nil {
		patchesDir = cmdutil.ValueFromFlagsOrConfig(cmd.Flags(), options.Patches, upgradeCfg.Apply.Patches.Directory, applyFlags.patchesDir).(string)
	} else {
		patchesDir = applyFlags.patchesDir
	}

	if *printConfig {
		printConfiguration(&initCfg.ClusterConfiguration, os.Stdout, printer)
	}

	return &applyData{
		nonInteractiveMode:        applyFlags.nonInteractiveMode,
		force:                     *force,
		dryRun:                    *dryRun,
		dryRunDir:                 dryRunDir,
		etcdUpgrade:               *etcdUpgrade,
		renewCerts:                *renewCerts,
		allowExperimentalUpgrades: *allowExperimentalUpgrades,
		allowRCUpgrades:           *allowRCUpgrades,
		printConfig:               *printConfig,
		cfg:                       upgradeCfg,
		initCfg:                   initCfg,
		client:                    client,
		patchesDir:                patchesDir,
		ignorePreflightErrors:     ignorePreflightErrorsSet,
		outputWriter:              applyFlags.out,
	}, nil
}

// DryRun 返回 dryRun 参数的值。
func (d *applyData) DryRun() bool {
	return d.dryRun
}

// EtcdUpgrade 返回 etcdUpgrade 参数的值。
func (d *applyData) EtcdUpgrade() bool {
	return d.etcdUpgrade
}

// RenewCerts 返回 renewCerts 参数的值。
func (d *applyData) RenewCerts() bool {
	return d.renewCerts
}

// Cfg 返回 UpgradeConfiguration。
func (d *applyData) Cfg() *kubeadmapi.UpgradeConfiguration {
	return d.cfg
}

// InitCfg 返回 InitConfiguration。
func (d *applyData) InitCfg() *kubeadmapi.InitConfiguration {
	return d.initCfg
}

// Client 返回供 kubeadm 使用的 Kubernetes 客户端。
func (d *applyData) Client() clientset.Interface {
	return d.client
}

// PatchesDir 返回保存组件补丁的目录。
func (d *applyData) PatchesDir() string {
	return d.patchesDir
}

// IgnorePreflightErrors 返回要忽略的预检错误列表。
func (d *applyData) IgnorePreflightErrors() sets.Set[string] {
	return d.ignorePreflightErrors
}

// OutputWriter 返回供 kubeadm 使用的输出写入器。
func (d *applyData) OutputWriter() io.Writer {
	return d.outputWriter
}

// SessionIsInteractive 在会话为交互式时返回 true（默认如此，可通过 -y、-f 或 --dry-run 退出交互模式）。
func (d *applyData) SessionIsInteractive() bool {
	return !(d.nonInteractiveMode || d.dryRun || d.force)
}

// AllowExperimentalUpgrades 在允许升级到 Kubernetes 的 alpha、beta 或候选发布版本时返回 true。
func (d *applyData) AllowExperimentalUpgrades() bool {
	return d.allowExperimentalUpgrades
}

// AllowRCUpgrades 在允许升级到 Kubernetes 候选发布版本时返回 true。
func (d *applyData) AllowRCUpgrades() bool {
	return d.allowRCUpgrades
}

// ForceUpgrade 在启用强制升级时返回 true。
func (d *applyData) ForceUpgrade() bool {
	return d.force
}

// IsControlPlaneNode 返回节点是否为控制平面节点。
func (d *applyData) IsControlPlaneNode() bool {
	// kubeadm upgrade apply 应始终在控制平面节点上执行。
	return true
}

// KubeConfigDir 返回 Kubernetes 配置目录；dry-run 时返回临时目录。
func (j *applyData) KubeConfigDir() string {
	if j.dryRun {
		return j.dryRunDir
	}
	return constants.KubernetesDir
}

// KubeletDir 返回 kubelet 配置目录；dry-run 时返回临时目录。
func (j *applyData) KubeletDir() string {
	if j.dryRun {
		return j.dryRunDir
	}
	return constants.KubeletRunDirectory
}
