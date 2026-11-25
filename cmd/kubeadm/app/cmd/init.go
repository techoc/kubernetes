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

package cmd

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"

	"github.com/spf13/cobra"
	flag "github.com/spf13/pflag"

	"k8s.io/apimachinery/pkg/util/sets"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/klog/v2"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	kubeadmscheme "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/scheme"
	kubeadmapiv1 "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/v1beta4"
	"k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/validation"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/options"
	phases "k8s.io/kubernetes/cmd/kubeadm/app/cmd/phases/init"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/phases/workflow"
	cmdutil "k8s.io/kubernetes/cmd/kubeadm/app/cmd/util"
	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/features"
	certsphase "k8s.io/kubernetes/cmd/kubeadm/app/phases/certs"
	kubeconfigphase "k8s.io/kubernetes/cmd/kubeadm/app/phases/kubeconfig"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/apiclient"
	configutil "k8s.io/kubernetes/cmd/kubeadm/app/util/config"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	kubeconfigutil "k8s.io/kubernetes/cmd/kubeadm/app/util/kubeconfig"
)

// initOptions 定义 kubeadm init 通过参数暴露的全部选项。
// 该结构包含公开的 kubeadm 配置 API，但只有部分受支持的选项会暴露为参数。
type initOptions struct {
	cfgPath                 string
	skipTokenPrint          bool
	dryRun                  bool
	kubeconfigDir           string
	kubeconfigPath          string
	featureGatesString      string
	ignorePreflightErrors   []string
	bto                     *options.BootstrapTokenOptions
	externalInitCfg         *kubeadmapiv1.InitConfiguration
	externalClusterCfg      *kubeadmapiv1.ClusterConfiguration
	uploadCerts             bool
	skipCertificateKeyPrint bool
	patchesDir              string
	skipCRIDetect           bool
}

const (
	// CoreDNSPhase 是 kubeadm init 中 CoreDNS 子阶段的名称。
	coreDNSPhase = "addon/coredns"

	// KubeProxyPhase 是 kubeadm init 中 kube-proxy 子阶段的名称。
	kubeProxyPhase = "addon/kube-proxy"

	// AddonPhase 是 kubeadm init 中插件阶段的名称。
	addonPhase = "addon"
)

// 编译期断言本地数据对象满足阶段数据接口。
var _ phases.InitData = &initData{}

// initData 定义运行 kubeadm init 工作流所需的全部运行时信息，
// 并供工作流包含的所有阶段共享。
type initData struct {
	cfg                         *kubeadmapi.InitConfiguration
	skipTokenPrint              bool
	dryRun                      bool
	kubeconfig                  *clientcmdapi.Config
	kubeconfigOriginal          *clientcmdapi.Config
	kubeconfigDir               string
	kubeconfigPath              string
	ignorePreflightErrors       sets.Set[string]
	certificatesDir             string
	dryRunDir                   string
	externalCA                  bool
	client                      clientset.Interface
	outputWriter                io.Writer
	uploadCerts                 bool
	skipCertificateKeyPrint     bool
	patchesDir                  string
	adminKubeConfigBootstrapped bool
}

// newCmdInit 返回 kubeadm init 命令。
// 注意：initOptions 作为参数暴露，以便单元测试实现全部命令选项校验逻辑的 newInitOptions 方法。
func newCmdInit(out io.Writer, initOptions *initOptions) *cobra.Command {
	if initOptions == nil {
		initOptions = newInitOptions()
	}
	initRunner := workflow.NewRunner()

	cmd := &cobra.Command{
		Use:   "init",
		Short: "Run this command in order to set up the Kubernetes control plane",
		RunE: func(cmd *cobra.Command, args []string) error {
			c, err := initRunner.InitData(args)
			if err != nil {
				return err
			}

			data, ok := c.(*initData)
			if !ok {
				return errors.New("invalid data struct")
			}

			fmt.Printf("[init] Using Kubernetes version: %s\n", data.cfg.KubernetesVersion)

			return initRunner.Run(args)
		},
		Args: cobra.NoArgs,
	}

	// 为 init 命令添加参数。
	// init 命令的本地参数最终可能由自动生成的阶段子命令继承。
	AddInitConfigFlags(cmd.Flags(), initOptions.externalInitCfg)
	AddClusterConfigFlags(cmd.Flags(), initOptions.externalClusterCfg, &initOptions.featureGatesString)
	AddInitOtherFlags(cmd.Flags(), initOptions)
	initOptions.bto.AddTokenFlag(cmd.Flags())
	initOptions.bto.AddTTLFlag(cmd.Flags())
	options.AddImageMetaFlags(cmd.Flags(), &initOptions.externalClusterCfg.ImageRepository)

	// 定义额外参数；init 命令本身不使用它们，但自动生成的阶段子命令可能使用。
	initRunner.SetAdditionalFlags(func(flags *flag.FlagSet) {
		options.AddKubeConfigFlag(flags, &initOptions.kubeconfigPath)
		options.AddKubeConfigDirFlag(flags, &initOptions.kubeconfigDir)
		options.AddControlPlanExtraArgsFlags(flags, &initOptions.externalClusterCfg.APIServer.ExtraArgs, &initOptions.externalClusterCfg.ControllerManager.ExtraArgs, &initOptions.externalClusterCfg.Scheduler.ExtraArgs)
	})

	// 使用阶段列表初始化工作流运行器。
	initRunner.AppendPhase(phases.NewPreflightPhase())
	initRunner.AppendPhase(phases.NewCertsPhase())
	initRunner.AppendPhase(phases.NewKubeConfigPhase())
	initRunner.AppendPhase(phases.NewEtcdPhase())
	initRunner.AppendPhase(phases.NewControlPlanePhase())
	initRunner.AppendPhase(phases.NewKubeletStartPhase())
	initRunner.AppendPhase(phases.NewWaitControlPlanePhase())
	initRunner.AppendPhase(phases.NewUploadConfigPhase())
	initRunner.AppendPhase(phases.NewUploadCertsPhase())
	initRunner.AppendPhase(phases.NewMarkControlPlanePhase())
	initRunner.AppendPhase(phases.NewBootstrapTokenPhase())
	initRunner.AppendPhase(phases.NewKubeletFinalizePhase())
	initRunner.AppendPhase(phases.NewAddonPhase())
	initRunner.AppendPhase(phases.NewShowJoinCommandPhase())

	// 为运行器设置数据构建函数，运行完整工作流或单个阶段时都会调用。
	initRunner.SetDataInitializer(func(cmd *cobra.Command, args []string) (workflow.RunData, error) {
		if cmd.Flags().Lookup(options.NodeCRISocket) == nil {
			// 跳过 CRI 检测。
			// 未设置 --cri-socket 时，假定命令执行不依赖 CRISocket。
			initOptions.skipCRIDetect = true
		}
		data, err := newInitData(cmd, args, initOptions, out)
		if err != nil {
			return nil, err
		}
		// 若跳过阶段的参数为空，则使用配置中的值。
		if len(initRunner.Options.SkipPhases) == 0 {
			initRunner.Options.SkipPhases = data.cfg.SkipPhases
		}

		initRunner.Options.SkipPhases = manageSkippedAddons(&data.cfg.ClusterConfiguration, initRunner.Options.SkipPhases)
		return data, nil
	})

	// 将 Runner 绑定到 kubeadm init 命令：修改帮助信息、
	// 添加 --skip-phases 参数并添加阶段子命令。
	initRunner.BindToCommand(cmd)

	return cmd
}

// AddInitConfigFlags 将绑定到配置的 init 参数添加到指定参数集。
func AddInitConfigFlags(flagSet *flag.FlagSet, cfg *kubeadmapiv1.InitConfiguration) {
	flagSet.StringVar(
		&cfg.LocalAPIEndpoint.AdvertiseAddress, options.APIServerAdvertiseAddress, cfg.LocalAPIEndpoint.AdvertiseAddress,
		"The IP address the API Server will advertise it's listening on. If not set the default network interface will be used.",
	)
	flagSet.Int32Var(
		&cfg.LocalAPIEndpoint.BindPort, options.APIServerBindPort, cfg.LocalAPIEndpoint.BindPort,
		"Port for the API Server to bind to.",
	)
	flagSet.StringVar(
		&cfg.NodeRegistration.Name, options.NodeName, cfg.NodeRegistration.Name,
		`Specify the node name.`,
	)
	flagSet.StringVar(
		&cfg.CertificateKey, options.CertificateKey, "",
		"Key used to encrypt the control-plane certificates in the kubeadm-certs Secret. The certificate key is a hex encoded string that is an AES key of size 32 bytes.",
	)
	cmdutil.AddCRISocketFlag(flagSet, &cfg.NodeRegistration.CRISocket)
}

// AddClusterConfigFlags 将绑定到配置的集群参数添加到指定参数集。
func AddClusterConfigFlags(flagSet *flag.FlagSet, cfg *kubeadmapiv1.ClusterConfiguration, featureGatesString *string) {
	flagSet.StringVar(
		&cfg.Networking.ServiceSubnet, options.NetworkingServiceSubnet, cfg.Networking.ServiceSubnet,
		"Use alternative range of IP address for service VIPs.",
	)
	flagSet.StringVar(
		&cfg.Networking.PodSubnet, options.NetworkingPodSubnet, cfg.Networking.PodSubnet,
		"Specify range of IP addresses for the pod network. If set, the control plane will automatically allocate CIDRs for every node.",
	)
	flagSet.StringVar(
		&cfg.Networking.DNSDomain, options.NetworkingDNSDomain, cfg.Networking.DNSDomain,
		`Use alternative domain for services, e.g. "myorg.internal".`,
	)

	flagSet.StringVar(
		&cfg.ControlPlaneEndpoint, options.ControlPlaneEndpoint, cfg.ControlPlaneEndpoint,
		`Specify a stable IP address or DNS name for the control plane.`,
	)

	options.AddKubernetesVersionFlag(flagSet, &cfg.KubernetesVersion)

	flagSet.StringVar(
		&cfg.CertificatesDir, options.CertificatesDir, cfg.CertificatesDir,
		`The path where to save and store the certificates.`,
	)
	flagSet.StringSliceVar(
		&cfg.APIServer.CertSANs, options.APIServerCertSANs, cfg.APIServer.CertSANs,
		`Optional extra Subject Alternative Names (SANs) to use for the API Server serving certificate. Can be both IP addresses and DNS names.`,
	)
	options.AddFeatureGatesStringFlag(flagSet, featureGatesString)
}

// AddInitOtherFlags 将不绑定到配置文件的 init 参数添加到指定参数集。
// 注意：所有不绑定到 cfg 对象的参数都应允许出现在 cmd/kubeadm/app/apis/kubeadm/validation/validation.go 中。
func AddInitOtherFlags(flagSet *flag.FlagSet, initOptions *initOptions) {
	options.AddConfigFlag(flagSet, &initOptions.cfgPath)
	flagSet.StringSliceVar(
		&initOptions.ignorePreflightErrors, options.IgnorePreflightErrors, initOptions.ignorePreflightErrors,
		"A list of checks whose errors will be shown as warnings. Example: 'IsPrivilegedUser,Swap'. Value 'all' ignores errors from all checks.",
	)
	flagSet.BoolVar(
		&initOptions.skipTokenPrint, options.SkipTokenPrint, initOptions.skipTokenPrint,
		"Skip printing of the default bootstrap token generated by 'kubeadm init'.",
	)
	flagSet.BoolVar(
		&initOptions.dryRun, options.DryRun, initOptions.dryRun,
		"Don't apply any changes; just output what would be done.",
	)
	flagSet.BoolVar(
		&initOptions.uploadCerts, options.UploadCerts, initOptions.uploadCerts,
		"Upload control-plane certificates to the kubeadm-certs Secret.",
	)
	flagSet.BoolVar(
		&initOptions.skipCertificateKeyPrint, options.SkipCertificateKeyPrint, initOptions.skipCertificateKeyPrint,
		"Don't print the key used to encrypt the control-plane certificates.",
	)
	options.AddPatchesFlag(flagSet, &initOptions.patchesDir)
}

// newInitOptions 返回可用于创建 init 命令参数的结构体。
func newInitOptions() *initOptions {
	// 应用默认值以初始化公开的 kubeadm 配置 API。
	externalInitCfg := &kubeadmapiv1.InitConfiguration{}
	kubeadmscheme.Scheme.Default(externalInitCfg)

	externalClusterCfg := &kubeadmapiv1.ClusterConfiguration{}
	kubeadmscheme.Scheme.Default(externalClusterCfg)

	// 创建引导令牌相关参数的选项对象，并覆盖 .Description 的默认值。
	bto := options.NewBootstrapTokenOptions()
	bto.Description = "The default bootstrap token generated by 'kubeadm init'."

	return &initOptions{
		externalInitCfg:    externalInitCfg,
		externalClusterCfg: externalClusterCfg,
		bto:                bto,
		kubeconfigDir:      kubeadmconstants.KubernetesDir,
		kubeconfigPath:     kubeadmconstants.GetAdminKubeConfigPath(),
		uploadCerts:        false,
	}
}

// newInitData 返回用于执行 kubeadm init 工作流的新 initData。
// 它校验传给命令的 initOptions，再将选项转换为内部 InitConfiguration，
// 作为 kubeadm init 工作流所有阶段的输入。
func newInitData(cmd *cobra.Command, args []string, initOptions *initOptions, out io.Writer) (*initData, error) {
	// 重新应用公开 kubeadm API 的默认值，仅设置未暴露为参数或未设置的值。
	kubeadmscheme.Scheme.Default(initOptions.externalInitCfg)
	kubeadmscheme.Scheme.Default(initOptions.externalClusterCfg)

	// 校验独立参数及其组合，并在适用时将校验后的值赋给公开的 kubeadm 配置 API。
	var err error
	if initOptions.externalClusterCfg.FeatureGates, err = features.NewFeatureGate(&features.InitFeatureGates, initOptions.featureGatesString); err != nil {
		return nil, err
	}

	if err = validation.ValidateMixedArguments(cmd.Flags()); err != nil {
		return nil, err
	}

	if err = initOptions.bto.ApplyTo(initOptions.externalInitCfg); err != nil {
		return nil, err
	}

	// 若指定配置文件则使用它，否则将公开 kubeadm API 转换为内部 InitConfiguration 并校验。
	cfg, err := configutil.LoadOrDefaultInitConfiguration(initOptions.cfgPath, initOptions.externalInitCfg, initOptions.externalClusterCfg, configutil.LoadOrDefaultConfigurationOptions{
		SkipCRIDetect: initOptions.skipCRIDetect,
	})
	if err != nil {
		return nil, err
	}

	ignorePreflightErrorsSet, err := validation.ValidateIgnorePreflightErrors(initOptions.ignorePreflightErrors, cfg.NodeRegistration.IgnorePreflightErrors)
	if err != nil {
		return nil, err
	}
	// 同时将预检错误的并集写入 InitConfiguration，使运行时配置视图保持一致：
	cfg.NodeRegistration.IgnorePreflightErrors = sets.List(ignorePreflightErrorsSet)

	// 用命令行选项覆盖节点名称。
	if initOptions.externalInitCfg.NodeRegistration.Name != "" {
		cfg.NodeRegistration.Name = initOptions.externalInitCfg.NodeRegistration.Name
	}

	if err := configutil.VerifyAPIServerBindAddress(cfg.LocalAPIEndpoint.AdvertiseAddress); err != nil {
		return nil, err
	}
	if err := features.ValidateVersion(features.InitFeatureGates, cfg.FeatureGates, cfg.KubernetesVersion); err != nil {
		return nil, err
	}

	// 若为 dry-run，创建用于保存 kubeadm 生成文件的临时目录。
	dryRunDir := ""
	if initOptions.dryRun || cfg.DryRun {
		if dryRunDir, err = kubeadmconstants.GetDryRunDir(kubeadmconstants.EnvVarInitDryRunDir, "kubeadm-init-dryrun", klog.Warningf); err != nil {
			return nil, errors.Wrap(err, "could not create a temporary directory on dryrun")
		}
	}

	// 检查用户是否提供了外部 CA（存在 CA 证书但不存在 CA 密钥）。
	externalCA, err := certsphase.UsingExternalCA(&cfg.ClusterConfiguration)
	if externalCA {
		// 若用户应提供的、由该 CA 签发的证书缺失或无效，则直接返回，
		// 因为 kubeadm 没有 CA 密钥无法重新生成。
		if err != nil {
			return nil, errors.Wrapf(err, "invalid or incomplete external CA")
		}

		// 校验必需的 kubeconfig 文件是否存在且有效；
		// 没有 CA 密钥时 kubeadm 无法重新生成它们。
		kubeconfigDir := initOptions.kubeconfigDir
		if err := kubeconfigphase.ValidateKubeconfigsForExternalCA(kubeconfigDir, cfg); err != nil {
			return nil, err
		}
	}

	// 检查用户是否提供了外部 Front-Proxy CA（存在证书但不存在密钥）。
	externalFrontProxyCA, err := certsphase.UsingExternalFrontProxyCA(&cfg.ClusterConfiguration)
	if externalFrontProxyCA {
		// 若用户应提供的、由 Front-Proxy CA 签发的证书缺失或无效，则直接返回，
		// 因为 kubeadm 没有 Front-Proxy CA 密钥无法重新生成。
		if err != nil {
			return nil, errors.Wrapf(err, "invalid or incomplete external front-proxy CA")
		}
	}

	if initOptions.uploadCerts && (externalCA || externalFrontProxyCA) {
		return nil, errors.New("can't use upload-certs with an external CA or an external front-proxy CA")
	}

	return &initData{
		cfg:                     cfg,
		certificatesDir:         cfg.CertificatesDir,
		skipTokenPrint:          initOptions.skipTokenPrint,
		dryRun:                  cmdutil.ValueFromFlagsOrConfig(cmd.Flags(), options.DryRun, cfg.DryRun, initOptions.dryRun).(bool),
		dryRunDir:               dryRunDir,
		kubeconfigDir:           initOptions.kubeconfigDir,
		kubeconfigPath:          initOptions.kubeconfigPath,
		ignorePreflightErrors:   ignorePreflightErrorsSet,
		externalCA:              externalCA,
		outputWriter:            out,
		uploadCerts:             initOptions.uploadCerts,
		skipCertificateKeyPrint: initOptions.skipCertificateKeyPrint,
		patchesDir:              initOptions.patchesDir,
	}, nil
}

// UploadCerts 返回 Uploadcerts 参数的值。
func (d *initData) UploadCerts() bool {
	return d.uploadCerts
}

// CertificateKey 返回用于加密证书的密钥。
func (d *initData) CertificateKey() string {
	return d.cfg.CertificateKey
}

// SetCertificateKey 设置用于加密证书的密钥。
func (d *initData) SetCertificateKey(key string) {
	d.cfg.CertificateKey = key
}

// SkipCertificateKeyPrint 返回 skipCertificateKeyPrint 参数的值。
func (d *initData) SkipCertificateKeyPrint() bool {
	return d.skipCertificateKeyPrint
}

// Cfg 返回 InitConfiguration。
func (d *initData) Cfg() *kubeadmapi.InitConfiguration {
	return d.cfg
}

// DryRun 返回 DryRun 参数的值。
func (d *initData) DryRun() bool {
	return d.dryRun
}

// SkipTokenPrint 返回 SkipTokenPrint 参数的值。
func (d *initData) SkipTokenPrint() bool {
	return d.skipTokenPrint
}

// IgnorePreflightErrors 返回 IgnorePreflightErrors 参数的值。
func (d *initData) IgnorePreflightErrors() sets.Set[string] {
	return d.ignorePreflightErrors
}

// CertificateWriteDir 返回证书目录；dry-run 时返回临时目录。
func (d *initData) CertificateWriteDir() string {
	if d.dryRun {
		return d.dryRunDir
	}
	return d.certificatesDir
}

// CertificateDir 返回用户最初指定的 CertificateDir。
func (d *initData) CertificateDir() string {
	return d.certificatesDir
}

// KubeConfig 返回从 KubeConfigPath() 加载的 kubeconfig。
// 若使用默认 kubeconfig 路径（admin.conf），不构造指向控制平面端点的配置，
// 而是指向 localAPIEndpoint，使 kubeadm init 只与本地 kube-apiserver 实例通信。
func (d *initData) KubeConfig() (*clientcmdapi.Config, error) {
	if d.kubeconfig != nil {
		return d.kubeconfig, nil
	}

	var err error
	d.kubeconfig, err = clientcmd.LoadFromFile(d.KubeConfigPath())
	if err != nil {
		return nil, err
	}
	d.kubeconfigOriginal = d.kubeconfig.DeepCopy()

	if d.kubeconfigPath == kubeadmconstants.GetAdminKubeConfigPath() {
		kubeconfigutil.PointKubeConfigToLocalAPIEndpoint(d.kubeconfig, &d.Cfg().LocalAPIEndpoint)
	}

	return d.kubeconfig, nil
}

// KubeConfigOriginal 返回从文件加载且未经修改的原始 kubeconfig。
func (d *initData) KubeConfigOriginal() (*clientcmdapi.Config, error) {
	if d.kubeconfigOriginal == nil {
		if _, err := d.KubeConfig(); err != nil {
			return nil, err
		}
	}

	return d.kubeconfigOriginal, nil
}

// KubeConfigDir 返回 Kubernetes 配置目录；dry-run 时返回临时目录。
func (d *initData) KubeConfigDir() string {
	if d.dryRun {
		return d.dryRunDir
	}
	return d.kubeconfigDir
}

// KubeConfigPath 返回用于连接 Kubernetes 的 kubeconfig 文件路径。
func (d *initData) KubeConfigPath() string {
	if d.dryRun {
		d.kubeconfigPath = filepath.Join(d.dryRunDir, kubeadmconstants.AdminKubeConfigFileName)
	}
	return d.kubeconfigPath
}

// ManifestDir 返回清单保存目录；dry-run 时返回临时目录。
func (d *initData) ManifestDir() string {
	if d.dryRun {
		return d.dryRunDir
	}
	return kubeadmconstants.GetStaticPodDirectory()
}

// KubeletDir 返回 kubelet 配置目录；dry-run 时返回临时目录。
func (d *initData) KubeletDir() string {
	if d.dryRun {
		return d.dryRunDir
	}
	return kubeadmconstants.KubeletRunDirectory
}

// ExternalCA 在用户提供外部 CA 时返回 true。
func (d *initData) ExternalCA() bool {
	return d.externalCA
}

// OutputWriter 返回此命令用于写入输出的 io.Writer。
func (d *initData) OutputWriter() io.Writer {
	return d.outputWriter
}

// getDryRunClient 创建伪客户端，响应部分 GET 请求，使 dry-run 模式能够执行完整 init 流程。
func getDryRunClient(d *initData) (clientset.Interface, error) {
	kubeconfig, err := d.KubeConfig()
	if err != nil {
		return nil, err
	}
	dryRun := apiclient.NewDryRun()
	if err := dryRun.WithKubeConfig(kubeconfig); err != nil {
		return nil, err
	}
	dryRun.WithDefaultMarshalFunction().
		WithWriter(os.Stdout).
		PrependReactor(dryRun.GetNodeReactor()).
		PrependReactor(dryRun.PatchNodeReactor())

	return dryRun.FakeClient(), nil
}

// Client 返回供 kubeadm 使用的 Kubernetes 客户端。
// 该函数采用单例实现，避免不同阶段重复创建客户端。
// 重要：必须在创建 admin.conf kubeconfig 文件之后调用此函数。
func (d *initData) Client() (clientset.Interface, error) {
	var err error
	if d.client == nil {
		if d.dryRun {
			d.client, err = getDryRunClient(d)
			if err != nil {
				return nil, err
			}
		} else { // 使用真实客户端。
			isDefaultKubeConfigPath := d.KubeConfigPath() == kubeadmconstants.GetAdminKubeConfigPath()
			// 仅在用户使用 admin.conf（即 --kubeconfig 为默认值）
			// 且尚未完成引导时，引导 admin.conf。
			if !d.adminKubeConfigBootstrapped && isDefaultKubeConfigPath {
				// 调用 EnsureAdminClusterRoleBinding()，获得可由 admin.conf 正常使用的客户端。
				d.client, err = kubeconfigphase.EnsureAdminClusterRoleBinding(
					kubeadmconstants.KubernetesDir,
					&d.Cfg().LocalAPIEndpoint,
					nil,
				)
				if err != nil {
					return nil, errors.Wrapf(err, "could not bootstrap the admin user in file %s", kubeadmconstants.AdminKubeConfigFileName)
				}
				d.adminKubeConfigBootstrapped = true
			} else {
				// 或者仅加载 --kubeconfig 路径指向的配置。
				cfg, err := d.KubeConfig()
				if err != nil {
					return nil, err
				}
				d.client, err = kubeconfigutil.ToClientSet(cfg)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return d.client, nil
}

// Tokens 返回令牌字符串数组。
func (d *initData) Tokens() []string {
	tokens := make([]string, 0, len(d.cfg.BootstrapTokens))
	for _, bt := range d.cfg.BootstrapTokens {
		tokens = append(tokens, bt.Token.String())
	}
	return tokens
}

// PatchesDir 返回保存组件补丁的目录。
func (d *initData) PatchesDir() string {
	// 若提供了参数值，则覆盖配置中的值。
	if len(d.patchesDir) > 0 {
		return d.patchesDir
	}
	if d.cfg.Patches != nil {
		return d.cfg.Patches.Directory
	}
	return ""
}

// manageSkippedAddons 同步 proxy 和 DNS 的 Disabled 状态与 skipPhases。
func manageSkippedAddons(cfg *kubeadmapi.ClusterConfiguration, skipPhases []string) []string {
	var (
		skipDNSPhase   = false
		skipProxyPhase = false
	)
	// 若 DNS 或 Proxy 插件被禁用，则跳过对应阶段；
	// 反之，根据跳过的插件阶段更新 proxy 和 DNS 的 Disabled 状态。
	if isPhaseInSkipPhases(addonPhase, skipPhases) {
		skipDNSPhase = true
		skipProxyPhase = true
		cfg.DNS.Disabled = true
		cfg.Proxy.Disabled = true
	}
	if isPhaseInSkipPhases(coreDNSPhase, skipPhases) {
		skipDNSPhase = true
		cfg.DNS.Disabled = true
	}
	if isPhaseInSkipPhases(kubeProxyPhase, skipPhases) {
		skipProxyPhase = true
		cfg.Proxy.Disabled = true
	}
	if cfg.DNS.Disabled && !skipDNSPhase {
		skipPhases = append(skipPhases, coreDNSPhase)
	}
	if cfg.Proxy.Disabled && !skipProxyPhase {
		skipPhases = append(skipPhases, kubeProxyPhase)
	}
	return skipPhases
}

func isPhaseInSkipPhases(phase string, skipPhases []string) bool {
	return slices.Contains(skipPhases, phase)
}
