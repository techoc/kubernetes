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
	"fmt"
	"io"

	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"

	"k8s.io/apimachinery/pkg/util/sets"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	"k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/v1beta4"
	"k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/validation"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/options"
	commonphases "k8s.io/kubernetes/cmd/kubeadm/app/cmd/phases/upgrade"
	phases "k8s.io/kubernetes/cmd/kubeadm/app/cmd/phases/upgrade/node"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/phases/workflow"
	cmdutil "k8s.io/kubernetes/cmd/kubeadm/app/cmd/util"
	"k8s.io/kubernetes/cmd/kubeadm/app/constants"
	configutil "k8s.io/kubernetes/cmd/kubeadm/app/util/config"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/output"
	staticpodutil "k8s.io/kubernetes/cmd/kubeadm/app/util/staticpod"
)

// nodeOptions 定义 kubeadm upgrade node 通过参数暴露的全部选项。
// 该结构包含公开的 kubeadm 配置 API，但只有部分受支持的选项会暴露为参数。
type nodeOptions struct {
	cfgPath               string
	kubeConfigPath        string
	etcdUpgrade           bool
	renewCerts            bool
	dryRun                bool
	patchesDir            string
	ignorePreflightErrors []string
}

// 编译期断言本地数据对象满足阶段数据接口。
var _ phases.Data = &nodeData{}

// nodeData 定义运行 kubeadm upgrade node 工作流所需的全部运行时信息，
// 并供工作流包含的所有阶段共享。
type nodeData struct {
	etcdUpgrade           bool
	renewCerts            bool
	dryRun                bool
	dryRunDir             string
	cfg                   *kubeadmapi.UpgradeConfiguration
	initCfg               *kubeadmapi.InitConfiguration
	isControlPlaneNode    bool
	client                clientset.Interface
	patchesDir            string
	ignorePreflightErrors sets.Set[string]
	kubeConfigPath        string
	outputWriter          io.Writer
}

// newCmdNode 返回 kubeadm upgrade node 的 cobra 命令。
func newCmdNode(out io.Writer) *cobra.Command {
	nodeOptions := newNodeOptions()
	nodeRunner := workflow.NewRunner()

	cmd := &cobra.Command{
		Use:   "node",
		Short: "Upgrade commands for a node in the cluster",
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := validation.ValidateMixedArguments(cmd.Flags()); err != nil {
				return err
			}

			data, err := nodeRunner.InitData(args)
			if err != nil {
				return err
			}
			nodeData, ok := data.(*nodeData)
			if !ok {
				return errors.New("invalid data struct")
			}
			if err := nodeRunner.Run(args); err != nil {
				return err
			}
			if nodeData.DryRun() {
				fmt.Println("[upgrade/successful] Finished dryrunning successfully!")
				return nil
			}

			return nil
		},
		Args: cobra.NoArgs,
	}

	// 向 node 命令添加参数。
	// 这些参数最终可能由自动生成的阶段子命令继承。
	addUpgradeNodeFlags(cmd.Flags(), nodeOptions)
	options.AddConfigFlag(cmd.Flags(), &nodeOptions.cfgPath)
	options.AddPatchesFlag(cmd.Flags(), &nodeOptions.patchesDir)

	// 使用阶段列表初始化工作流运行器。
	nodeRunner.AppendPhase(phases.NewPreflightPhase())
	nodeRunner.AppendPhase(phases.NewControlPlane())
	nodeRunner.AppendPhase(phases.NewKubeconfigPhase())
	nodeRunner.AppendPhase(commonphases.NewKubeletConfigPhase())
	nodeRunner.AppendPhase(commonphases.NewAddonPhase())
	nodeRunner.AppendPhase(commonphases.NewPostUpgradePhase())

	// 为运行器设置数据构建函数，运行完整工作流或单个阶段时都会调用。
	nodeRunner.SetDataInitializer(func(cmd *cobra.Command, args []string) (workflow.RunData, error) {
		data, err := newNodeData(cmd, nodeOptions, out)
		if err != nil {
			return nil, err
		}
		// 若跳过阶段的参数为空，则使用配置中的值。
		if len(nodeRunner.Options.SkipPhases) == 0 {
			nodeRunner.Options.SkipPhases = data.cfg.Node.SkipPhases
		}
		return data, nil
	})

	// 将 Runner 绑定到 kubeadm upgrade node 命令：修改帮助信息、
	// 添加 --skip-phases 参数并添加阶段子命令。
	nodeRunner.BindToCommand(cmd)

	return cmd
}

// newNodeOptions 返回可用于创建 kubeadm upgrade node 命令参数的结构体。
func newNodeOptions() *nodeOptions {
	return &nodeOptions{
		kubeConfigPath: "", // 此值在运行时由 newNodeData() 填充。
		dryRun:         false,
		renewCerts:     true,
		etcdUpgrade:    true,
	}
}

func addUpgradeNodeFlags(flagSet *flag.FlagSet, nodeOptions *nodeOptions) {
	options.AddKubeConfigFlag(flagSet, &nodeOptions.kubeConfigPath)
	flagSet.BoolVar(&nodeOptions.dryRun, options.DryRun, nodeOptions.dryRun, "Do not change any state, just output the actions that would be performed.")
	flagSet.BoolVar(&nodeOptions.renewCerts, options.CertificateRenewal, nodeOptions.renewCerts, "Perform the renewal of certificates used by component changed during upgrades.")
	flagSet.BoolVar(&nodeOptions.etcdUpgrade, options.EtcdUpgrade, nodeOptions.etcdUpgrade, "Perform the upgrade of etcd.")
	flagSet.StringSliceVar(&nodeOptions.ignorePreflightErrors, options.IgnorePreflightErrors, nodeOptions.ignorePreflightErrors, "A list of checks whose errors will be shown as warnings. Example: 'IsPrivilegedUser,Swap'. Value 'all' ignores errors from all checks.")
}

// newNodeData 返回用于执行 kubeadm upgrade node 工作流的新 nodeData。
// 它校验传给命令的 nodeOptions，再将选项转换为内部 InitConfiguration，
// 作为 kubeadm upgrade node 工作流所有阶段的输入。
func newNodeData(cmd *cobra.Command, nodeOptions *nodeOptions, out io.Writer) (*nodeData, error) {
	isControlPlaneNode := staticpodutil.IsControlPlaneNode()

	if len(nodeOptions.kubeConfigPath) == 0 {
		// 根据节点是否为控制平面节点更新 kubeconfig 路径。
		nodeOptions.kubeConfigPath = constants.GetKubeletKubeConfigPath()
		if isControlPlaneNode {
			nodeOptions.kubeConfigPath = constants.GetAdminKubeConfigPath()
		}
	}

	externalCfg := &v1beta4.UpgradeConfiguration{}
	opt := configutil.LoadOrDefaultConfigurationOptions{}
	upgradeCfg, err := configutil.LoadOrDefaultUpgradeConfiguration(nodeOptions.cfgPath, externalCfg, opt)
	if err != nil {
		return nil, err
	}

	dryRun, ok := cmdutil.ValueFromFlagsOrConfig(cmd.Flags(), options.DryRun, upgradeCfg.Node.DryRun, &nodeOptions.dryRun).(*bool)
	if !ok {
		return nil, cmdutil.TypeMismatchErr("dryRun", "bool")
	}

	// 若为 dry-run，创建用于保存 kubeadm 生成文件的临时目录。
	dryRunDir := ""
	if *dryRun {
		if dryRunDir, err = constants.GetDryRunDir(constants.EnvVarUpgradeDryRunDir, "kubeadm-upgrade-node-dryrun", klog.Warningf); err != nil {
			return nil, errors.Wrap(err, "could not create a temporary directory on dryrun")
		}
	}

	printer := &output.TextPrinter{}
	client, err := getClient(nodeOptions.kubeConfigPath, *dryRun, printer)
	if err != nil {
		return nil, errors.Wrapf(err, "couldn't create a Kubernetes client from file %q", nodeOptions.kubeConfigPath)
	}

	// 获取集群配置。
	getNodeRegistration := true
	getAPIEndpoint := isControlPlaneNode
	getComponentConfigs := true
	initCfg, err := configutil.FetchInitConfigurationFromCluster(client, nil, "upgrade", getNodeRegistration, getAPIEndpoint, getComponentConfigs, false)
	if err != nil {
		return nil, errors.Wrap(err, "unable to fetch the kubeadm-config ConfigMap")
	}

	ignorePreflightErrorsSet, err := validation.ValidateIgnorePreflightErrors(nodeOptions.ignorePreflightErrors, upgradeCfg.Node.IgnorePreflightErrors)
	if err != nil {
		return nil, err
	}
	// 同时将预检错误的并集写入 InitConfiguration，使运行时配置视图保持一致：
	initCfg.NodeRegistration.IgnorePreflightErrors = sets.List(ignorePreflightErrorsSet)

	var patchesDir string
	if upgradeCfg.Node.Patches != nil {
		patchesDir = cmdutil.ValueFromFlagsOrConfig(cmd.Flags(), options.Patches, upgradeCfg.Node.Patches.Directory, nodeOptions.patchesDir).(string)
	} else {
		patchesDir = nodeOptions.patchesDir
	}

	return &nodeData{
		cfg:                   upgradeCfg,
		dryRun:                *dryRun,
		dryRunDir:             dryRunDir,
		initCfg:               initCfg,
		client:                client,
		isControlPlaneNode:    isControlPlaneNode,
		ignorePreflightErrors: ignorePreflightErrorsSet,
		kubeConfigPath:        nodeOptions.kubeConfigPath,
		outputWriter:          out,
		patchesDir:            patchesDir,
		etcdUpgrade:           *cmdutil.ValueFromFlagsOrConfig(cmd.Flags(), options.EtcdUpgrade, upgradeCfg.Node.EtcdUpgrade, &nodeOptions.etcdUpgrade).(*bool),
		renewCerts:            *cmdutil.ValueFromFlagsOrConfig(cmd.Flags(), options.CertificateRenewal, upgradeCfg.Node.CertificateRenewal, &nodeOptions.renewCerts).(*bool),
	}, nil
}

// DryRun 返回 dryRun 参数的值。
func (d *nodeData) DryRun() bool {
	return d.dryRun
}

// EtcdUpgrade 返回 etcdUpgrade 参数的值。
func (d *nodeData) EtcdUpgrade() bool {
	return d.etcdUpgrade
}

// RenewCerts 返回 renewCerts 参数的值。
func (d *nodeData) RenewCerts() bool {
	return d.renewCerts
}

// Cfg 返回 UpgradeConfiguration。
func (d *nodeData) Cfg() *kubeadmapi.UpgradeConfiguration {
	return d.cfg
}

// InitCfg 返回 InitConfiguration。
func (d *nodeData) InitCfg() *kubeadmapi.InitConfiguration {
	return d.initCfg
}

// IsControlPlaneNode 返回 isControlPlaneNode 参数的值。
func (d *nodeData) IsControlPlaneNode() bool {
	return d.isControlPlaneNode
}

// Client 返回供 kubeadm 使用的 Kubernetes 客户端。
func (d *nodeData) Client() clientset.Interface {
	return d.client
}

// PatchesDir 返回保存组件补丁的目录。
func (d *nodeData) PatchesDir() string {
	return d.patchesDir
}

// IgnorePreflightErrors 返回要忽略的预检错误列表。
func (d *nodeData) IgnorePreflightErrors() sets.Set[string] {
	return d.ignorePreflightErrors
}

// KubeConfigPath 返回用户 kubeconfig 文件的路径。
func (d *nodeData) KubeConfigPath() string {
	return d.kubeConfigPath
}

// OutputWriter 返回供 kubeadm 使用的输出写入器。
func (d *nodeData) OutputWriter() io.Writer {
	return d.outputWriter
}

// KubeConfigDir 返回 Kubernetes 配置目录；dry-run 时返回临时目录。
func (j *nodeData) KubeConfigDir() string {
	if j.dryRun {
		return j.dryRunDir
	}
	return constants.KubernetesDir
}

// KubeletDir 返回 kubelet 配置目录；dry-run 时返回临时目录。
func (j *nodeData) KubeletDir() string {
	if j.dryRun {
		return j.dryRunDir
	}
	return constants.KubeletRunDirectory
}
