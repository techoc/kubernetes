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
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/template"

	"github.com/lithammer/dedent"
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
	phases "k8s.io/kubernetes/cmd/kubeadm/app/cmd/phases/join"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/phases/workflow"
	cmdutil "k8s.io/kubernetes/cmd/kubeadm/app/cmd/util"
	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/discovery"
	"k8s.io/kubernetes/cmd/kubeadm/app/discovery/token"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/apiclient"
	configutil "k8s.io/kubernetes/cmd/kubeadm/app/util/config"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	kubeconfigutil "k8s.io/kubernetes/cmd/kubeadm/app/util/kubeconfig"
)

var (
	joinWorkerNodeDoneMsg = dedent.Dedent(`
		This node has joined the cluster:
		* Certificate signing request was sent to apiserver and a response was received.
		* The Kubelet was informed of the new secure connection details.

		Run 'kubectl get nodes' on the control-plane to see this node join the cluster.

		`)

	joinControlPlaneDoneTemp = template.Must(template.New("join").Parse(dedent.Dedent(`
		This node has joined the cluster and a new control plane instance was created:

		* Certificate signing request was sent to apiserver and approval was received.
		* The Kubelet was informed of the new secure connection details.
		* Control plane label and taint were applied to the new node.
		* The Kubernetes control plane instances scaled up.
		{{.etcdMessage}}

		To start administering your cluster from this node, you need to run the following as a regular user:

			mkdir -p $HOME/.kube
			sudo cp -i {{.KubeConfigPath}} $HOME/.kube/config
			sudo chown $(id -u):$(id -g) $HOME/.kube/config

		Run 'kubectl get nodes' to see this node join the cluster.

		`)))

	joinLongDescription = dedent.Dedent(`
		When joining a kubeadm initialized cluster, we need to establish
		bidirectional trust. This is split into discovery (having the Node
		trust the Kubernetes Control Plane) and TLS bootstrap (having the
		Kubernetes Control Plane trust the Node).

		There are 2 main schemes for discovery. The first is to use a shared
		token along with the IP address of the API server. The second is to
		provide a file - a subset of the standard kubeconfig file. The
		discovery/kubeconfig file supports token, client-go authentication
		plugins ("exec"), "tokenFile", and "authProvider". This file can be a
		local file or downloaded via an HTTPS URL. The forms are
		kubeadm join --discovery-token abcdef.1234567890abcdef 1.2.3.4:6443,
		kubeadm join --discovery-file path/to/file.conf, or kubeadm join
		--discovery-file https://url/file.conf. Only one form can be used. If
		the discovery information is loaded from a URL, HTTPS must be used.
		Also, in that case the host installed CA bundle is used to verify
		the connection.

		If you use a shared token for discovery, you should also pass the
		--discovery-token-ca-cert-hash flag to validate the public key of the
		root certificate authority (CA) presented by the Kubernetes Control Plane.
		The value of this flag is specified as "<hash-type>:<hex-encoded-value>",
		where the supported hash type is "sha256". The hash is calculated over
		the bytes of the Subject Public Key Info (SPKI) object (as in RFC7469).
		This value is available in the output of "kubeadm init" or can be
		calculated using standard tools. The --discovery-token-ca-cert-hash flag
		may be repeated multiple times to allow more than one public key.

		If you cannot know the CA public key hash ahead of time, you can pass
		the --discovery-token-unsafe-skip-ca-verification flag to disable this
		verification. This weakens the kubeadm security model since other nodes
		can potentially impersonate the Kubernetes Control Plane.

		The TLS bootstrap mechanism is also driven via a shared token. This is
		used to temporarily authenticate with the Kubernetes Control Plane to submit a
		certificate signing request (CSR) for a locally created key pair. By
		default, kubeadm will set up the Kubernetes Control Plane to automatically
		approve these signing requests. This token is passed in with the
		--tls-bootstrap-token abcdef.1234567890abcdef flag.

		Often times the same token is used for both parts. In this case, the
		--token flag can be used instead of specifying each token individually.
		`)
)

// joinOptions 定义 kubeadm join 通过参数暴露的全部选项。
// 该结构包含公开的 kubeadm 配置 API，但只有部分受支持的选项会暴露为参数。
type joinOptions struct {
	cfgPath               string
	token                 string `datapolicy:"token"`
	controlPlane          bool
	ignorePreflightErrors []string
	externalcfg           *kubeadmapiv1.JoinConfiguration
	patchesDir            string
	dryRun                bool
	skipCRIDetect         bool
}

// 编译期断言本地数据对象满足阶段数据接口。
var _ phases.JoinData = &joinData{}

// joinData 定义运行 kubeadm join 工作流所需的全部运行时信息，
// 并供工作流包含的所有阶段共享。
type joinData struct {
	cfg                   *kubeadmapi.JoinConfiguration
	initCfg               *kubeadmapi.InitConfiguration
	tlsBootstrapCfg       *clientcmdapi.Config
	client                clientset.Interface
	ignorePreflightErrors sets.Set[string]
	outputWriter          io.Writer
	patchesDir            string
	dryRun                bool
	dryRunDir             string
}

// newCmdJoin 返回 kubeadm join 命令。
// 注意：joinOptions 作为参数暴露，以便单元测试实现全部命令选项校验逻辑的 newJoinData 方法。
func newCmdJoin(out io.Writer, joinOptions *joinOptions) *cobra.Command {
	if joinOptions == nil {
		joinOptions = newJoinOptions()
	}
	joinRunner := workflow.NewRunner()

	cmd := &cobra.Command{
		Use:   "join [api-server-endpoint]",
		Short: "Run this on any machine you wish to join an existing cluster",
		Long:  joinLongDescription,
		RunE: func(cmd *cobra.Command, args []string) error {

			c, err := joinRunner.InitData(args)
			if err != nil {
				return err
			}

			data, ok := c.(*joinData)
			if !ok {
				return errors.New("invalid data struct")
			}

			if err := joinRunner.Run(args); err != nil {
				return err
			}

			// 若节点将承载新的控制平面实例，
			if data.cfg.ControlPlane != nil {
				// 输出控制平面加入完成消息并退出。
				etcdMessage := ""
				if data.initCfg.Etcd.External == nil {
					etcdMessage = "* A new etcd member was added to the local/stacked etcd cluster."
				}

				ctx := map[string]string{
					"KubeConfigPath": kubeadmconstants.GetAdminKubeConfigPath(),
					"etcdMessage":    etcdMessage,
				}
				if err := joinControlPlaneDoneTemp.Execute(data.outputWriter, ctx); err != nil {
					return err
				}

			} else {
				// 否则，若节点作为工作节点加入，
				// 输出加入完成消息并退出。
				fmt.Fprint(data.outputWriter, joinWorkerNodeDoneMsg)
			}

			return nil
		},
		// 控制平面地址作为可选的位置参数接收。
		Args: cobra.MaximumNArgs(1),
	}

	addJoinConfigFlags(cmd.Flags(), joinOptions.externalcfg)
	addJoinOtherFlags(cmd.Flags(), joinOptions)

	joinRunner.AppendPhase(phases.NewPreflightPhase())
	joinRunner.AppendPhase(phases.NewControlPlanePreparePhase())
	joinRunner.AppendPhase(phases.NewCheckEtcdPhase())
	joinRunner.AppendPhase(phases.NewKubeletStartPhase())
	joinRunner.AppendPhase(phases.NewEtcdJoinPhase())
	joinRunner.AppendPhase(phases.NewKubeletWaitBootstrapPhase())
	joinRunner.AppendPhase(phases.NewControlPlaneJoinPhase())
	joinRunner.AppendPhase(phases.NewWaitControlPlanePhase())

	// 为运行器设置数据构建函数，运行完整工作流或单个阶段时都会调用。
	joinRunner.SetDataInitializer(func(cmd *cobra.Command, args []string) (workflow.RunData, error) {
		if cmd.Flags().Lookup(options.NodeCRISocket) == nil {
			// 跳过 CRI 检测。
			// 未设置 --cri-socket 时，假定命令执行不依赖 CRISocket。
			joinOptions.skipCRIDetect = true
		}
		data, err := newJoinData(cmd, args, joinOptions, out, kubeadmconstants.GetAdminKubeConfigPath())
		if err != nil {
			return nil, err
		}
		// 若跳过阶段的参数为空，则使用配置中的值。
		if len(joinRunner.Options.SkipPhases) == 0 {
			joinRunner.Options.SkipPhases = data.cfg.SkipPhases
		}
		return data, nil
	})

	// 将 Runner 绑定到 kubeadm join 命令：修改帮助信息、
	// 添加 --skip-phases 参数并添加阶段子命令。
	joinRunner.BindToCommand(cmd)

	return cmd
}

// addJoinConfigFlags 将绑定到配置的 join 参数添加到指定参数集。
func addJoinConfigFlags(flagSet *flag.FlagSet, cfg *kubeadmapiv1.JoinConfiguration) {
	flagSet.StringVar(
		&cfg.NodeRegistration.Name, options.NodeName, cfg.NodeRegistration.Name,
		`Specify the node name.`,
	)
	flagSet.StringVar(
		&cfg.ControlPlane.CertificateKey, options.CertificateKey, cfg.ControlPlane.CertificateKey,
		"Use this key to decrypt the certificate secrets uploaded by init. The certificate key is a hex encoded string that is an AES key of size 32 bytes.",
	)
	// 将控制平面端点参数添加到指定参数集。
	flagSet.StringVar(
		&cfg.ControlPlane.LocalAPIEndpoint.AdvertiseAddress, options.APIServerAdvertiseAddress, cfg.ControlPlane.LocalAPIEndpoint.AdvertiseAddress,
		"If the node should host a new control plane instance, the IP address the API Server will advertise it's listening on. If not set the default network interface will be used.",
	)
	flagSet.Int32Var(
		&cfg.ControlPlane.LocalAPIEndpoint.BindPort, options.APIServerBindPort, cfg.ControlPlane.LocalAPIEndpoint.BindPort,
		"If the node should host a new control plane instance, the port for the API Server to bind to.",
	)
	// 将引导令牌发现相关参数添加到指定参数集。
	flagSet.StringVar(
		&cfg.Discovery.BootstrapToken.Token, options.TokenDiscovery, "",
		"For token-based discovery, the token used to validate cluster information fetched from the API server.",
	)
	flagSet.StringSliceVar(
		&cfg.Discovery.BootstrapToken.CACertHashes, options.TokenDiscoveryCAHash, []string{},
		"For token-based discovery, validate that the root CA public key matches this hash (format: \"<type>:<value>\").",
	)
	flagSet.BoolVar(
		&cfg.Discovery.BootstrapToken.UnsafeSkipCAVerification, options.TokenDiscoverySkipCAHash, false,
		"For token-based discovery, allow joining without --discovery-token-ca-cert-hash pinning.",
	)
	// 通过 kubeconfig 文件发现。
	flagSet.StringVar(
		&cfg.Discovery.File.KubeConfigPath, options.FileDiscovery, "",
		"For file-based discovery, a file or URL from which to load cluster information.",
	)
	flagSet.StringVar(
		&cfg.Discovery.TLSBootstrapToken, options.TLSBootstrapToken, cfg.Discovery.TLSBootstrapToken,
		`Specify the token used to temporarily authenticate with the Kubernetes Control Plane while joining the node.`,
	)
	cmdutil.AddCRISocketFlag(flagSet, &cfg.NodeRegistration.CRISocket)
}

// addJoinOtherFlags 将不绑定到配置文件的 join 参数添加到指定参数集。
func addJoinOtherFlags(flagSet *flag.FlagSet, joinOptions *joinOptions) {
	options.AddConfigFlag(flagSet, &joinOptions.cfgPath)
	flagSet.StringSliceVar(
		&joinOptions.ignorePreflightErrors, options.IgnorePreflightErrors, joinOptions.ignorePreflightErrors,
		"A list of checks whose errors will be shown as warnings. Example: 'IsPrivilegedUser,Swap'. Value 'all' ignores errors from all checks.",
	)
	flagSet.StringVar(
		&joinOptions.token, options.TokenStr, "",
		"Use this token for both discovery-token and tls-bootstrap-token when those values are not provided.",
	)
	flagSet.BoolVar(
		&joinOptions.controlPlane, options.ControlPlane, joinOptions.controlPlane,
		"Create a new control plane instance on this node",
	)
	flagSet.BoolVar(
		&joinOptions.dryRun, options.DryRun, joinOptions.dryRun,
		"Don't apply any changes; just output what would be done.",
	)
	options.AddPatchesFlag(flagSet, &joinOptions.patchesDir)
}

// newJoinOptions 返回可用于创建 join 命令参数的结构体。
func newJoinOptions() *joinOptions {
	// 应用默认值以初始化公开的 kubeadm 配置 API。
	externalcfg := &kubeadmapiv1.JoinConfiguration{}

	// 添加可选配置对象以承载参数。
	// 未设置的对象随后会在 newJoinData 中清理。
	externalcfg.Discovery.File = &kubeadmapiv1.FileDiscovery{}
	externalcfg.Discovery.BootstrapToken = &kubeadmapiv1.BootstrapTokenDiscovery{}
	externalcfg.ControlPlane = &kubeadmapiv1.JoinControlPlane{}

	// 此对象用于保存控制平面参数。
	joinControlPlane := &kubeadmapiv1.JoinControlPlane{}

	// 应用默认值。
	kubeadmscheme.Scheme.Default(externalcfg)
	kubeadmapiv1.SetDefaults_JoinControlPlane(joinControlPlane)

	return &joinOptions{
		externalcfg: externalcfg,
	}
}

// newJoinData 返回用于执行 kubeadm join 工作流的新 joinData。
// 它校验传给命令的 joinOptions，再将选项转换为内部 JoinConfiguration，
// 作为 kubeadm join 工作流所有阶段的输入。
func newJoinData(cmd *cobra.Command, args []string, opt *joinOptions, out io.Writer, adminKubeConfigPath string) (*joinData, error) {

	// 校验 --config 与其他参数的组合，出错时提前返回。
	if err := validation.ValidateMixedArguments(cmd.Flags()); err != nil {
		return nil, err
	}

	// 重新应用公开 kubeadm API 的默认值，仅设置未暴露为参数或未设置的值。
	kubeadmscheme.Scheme.Default(opt.externalcfg)

	// 校验独立参数及其组合，并在适用时将校验后的值赋给公开的 kubeadm 配置 API。

	// 若提供了令牌，在未显式提供对应值时，同时用于 discovery-token 和 tls-bootstrap-token。
	if len(opt.token) > 0 {
		if len(opt.externalcfg.Discovery.TLSBootstrapToken) == 0 {
			opt.externalcfg.Discovery.TLSBootstrapToken = opt.token
		}
		if len(opt.externalcfg.Discovery.BootstrapToken.Token) == 0 {
			opt.externalcfg.Discovery.BootstrapToken.Token = opt.token
		}
	}

	// 若未提供加载集群信息的文件或 URL，则取消设置 Discovery.File。
	if len(opt.externalcfg.Discovery.File.KubeConfigPath) == 0 {
		opt.externalcfg.Discovery.File = nil
	}

	// 若未提供用于获取集群信息的 APIServerEndpoint，则取消设置 Discovery.BootstrapToken。
	if len(args) == 0 {
		opt.externalcfg.Discovery.BootstrapToken = nil
	} else {
		if len(opt.cfgPath) == 0 && len(args) > 1 {
			klog.Warningf("[preflight] WARNING: More than one API server endpoint supplied on command line %v. Using the first one.", args)
		}
		opt.externalcfg.Discovery.BootstrapToken.APIServerEndpoint = args[0]
	}

	// 若未传入 --control-plane，则取消设置 ControlPlane。
	if !opt.controlPlane {
		// 使用应用默认值后的 JoinControlPlane 判断用户是否传入了其他控制平面相关参数。
		defaultJCP := &kubeadmapiv1.JoinControlPlane{}
		kubeadmapiv1.SetDefaults_JoinControlPlane(defaultJCP)

		// 此列表必须与 addJoinConfigFlags() 中的 JCP 参数一致。
		joinControlPlaneFlags := []string{
			options.CertificateKey,
			options.APIServerAdvertiseAddress,
			options.APIServerBindPort,
		}

		if *opt.externalcfg.ControlPlane != *defaultJCP {
			klog.Warningf("[preflight] WARNING: --%s is also required when passing control-plane "+
				"related flags such as [%s]", options.ControlPlane, strings.Join(joinControlPlaneFlags, ", "))
		}
		opt.externalcfg.ControlPlane = nil
	}

	// 若 admin.conf 已存在，使用它跳过发现过程。
	// 注意：加入控制平面节点且各阶段原子执行时可能发生这种情况。
	var tlsBootstrapCfg *clientcmdapi.Config
	if _, err := os.Stat(adminKubeConfigPath); err == nil && opt.controlPlane {
		// 将 admin.conf 作为 tlsBootstrapCfg，即在发现期间读取 kubeadm-config 的 kubeconfig 文件。
		klog.V(1).Infof("[preflight] found %s. Use it for skipping discovery", adminKubeConfigPath)
		tlsBootstrapCfg, err = clientcmd.LoadFromFile(adminKubeConfigPath)
		if err != nil {
			return nil, errors.Wrapf(err, "Error loading %s", adminKubeConfigPath)
		}
	}

	// 若指定配置文件则使用它，否则将公开 kubeadm API 转换为内部 JoinConfiguration 并校验。
	if opt.externalcfg.NodeRegistration.Name == "" {
		klog.V(1).Infoln("[preflight] found NodeName empty; using OS hostname as NodeName")
	}

	if opt.externalcfg.ControlPlane != nil && opt.externalcfg.ControlPlane.LocalAPIEndpoint.AdvertiseAddress == "" {
		klog.V(1).Infoln("[preflight] found advertiseAddress empty; using default interface's IP address as advertiseAddress")
	}

	// 命令没有发现相关参数时，使 join 配置能够通过发现校验。
	if cmd.Flags().Lookup(options.FileDiscovery) == nil {
		if _, err := os.Stat(adminKubeConfigPath); os.IsNotExist(err) {
			return nil, errors.Errorf("File %s does not exists. Please use 'kubeadm join phase control-plane-prepare' subcommands to generate it.", adminKubeConfigPath)
		}
		klog.V(1).Infof("[preflight] found discovery flags missing for this command. using FileDiscovery: %s", adminKubeConfigPath)
		opt.externalcfg.Discovery.File = &kubeadmapiv1.FileDiscovery{KubeConfigPath: adminKubeConfigPath}
		opt.externalcfg.Discovery.BootstrapToken = nil // 注意：更好地控制参数后可能移除此逻辑（例如无发现阶段的命令使用 NoArgs）。
	}

	cfg, err := configutil.LoadOrDefaultJoinConfiguration(opt.cfgPath, opt.externalcfg, configutil.LoadOrDefaultConfigurationOptions{
		SkipCRIDetect: opt.skipCRIDetect,
	})
	if err != nil {
		return nil, err
	}

	ignorePreflightErrorsSet, err := validation.ValidateIgnorePreflightErrors(opt.ignorePreflightErrors, cfg.NodeRegistration.IgnorePreflightErrors)
	if err != nil {
		return nil, err
	}
	// 同时将预检错误的并集写入 JoinConfiguration，使运行时配置视图保持一致：
	cfg.NodeRegistration.IgnorePreflightErrors = sets.List(ignorePreflightErrorsSet)

	// 用命令行选项覆盖节点名称和 CRI 套接字。
	if opt.externalcfg.NodeRegistration.Name != "" {
		cfg.NodeRegistration.Name = opt.externalcfg.NodeRegistration.Name
	}
	if opt.externalcfg.NodeRegistration.CRISocket != "" {
		cfg.NodeRegistration.CRISocket = opt.externalcfg.NodeRegistration.CRISocket
	}

	if cfg.ControlPlane != nil {
		if err := configutil.VerifyAPIServerBindAddress(cfg.ControlPlane.LocalAPIEndpoint.AdvertiseAddress); err != nil {
			return nil, err
		}
	}

	// 若为 dry-run，创建用于保存 kubeadm 生成文件的临时目录。
	dryRunDir := ""
	if opt.dryRun || cfg.DryRun {
		if dryRunDir, err = kubeadmconstants.GetDryRunDir(kubeadmconstants.EnvVarJoinDryRunDir, "kubeadm-join-dryrun", klog.Warningf); err != nil {
			return nil, errors.Wrap(err, "could not create a temporary directory on dryrun")
		}
	}

	return &joinData{
		cfg:                   cfg,
		dryRun:                cmdutil.ValueFromFlagsOrConfig(cmd.Flags(), options.DryRun, cfg.DryRun, opt.dryRun).(bool),
		tlsBootstrapCfg:       tlsBootstrapCfg,
		ignorePreflightErrors: ignorePreflightErrorsSet,
		outputWriter:          out,
		patchesDir:            opt.patchesDir,
		dryRunDir:             dryRunDir,
	}, nil
}

// CertificateKey 返回用于加密证书的密钥。
func (j *joinData) CertificateKey() string {
	if j.cfg.ControlPlane != nil {
		return j.cfg.ControlPlane.CertificateKey
	}
	return ""
}

// Cfg 返回 JoinConfiguration。
func (j *joinData) Cfg() *kubeadmapi.JoinConfiguration {
	return j.cfg
}

// DryRun 返回 DryRun 参数的值。
func (j *joinData) DryRun() bool {
	return j.dryRun
}

// KubeConfigDir 返回 Kubernetes 配置目录；dry-run 时返回临时目录。
func (j *joinData) KubeConfigDir() string {
	if j.dryRun {
		return j.dryRunDir
	}
	return kubeadmconstants.KubernetesDir
}

// KubeletDir 返回 kubelet 配置目录；dry-run 时返回临时目录。
func (j *joinData) KubeletDir() string {
	if j.dryRun {
		return j.dryRunDir
	}
	return kubeadmconstants.KubeletRunDirectory
}

// ManifestDir 返回清单保存目录；dry-run 时返回临时目录。
func (j *joinData) ManifestDir() string {
	if j.dryRun {
		return j.dryRunDir
	}
	return kubeadmconstants.GetStaticPodDirectory()
}

// CertificateWriteDir 返回证书保存目录；dry-run 时返回临时目录。
func (j *joinData) CertificateWriteDir() string {
	if j.dryRun {
		return j.dryRunDir
	}
	return j.initCfg.CertificatesDir
}

// TLSBootstrapCfg 返回 cluster-info（kubeconfig）。
func (j *joinData) TLSBootstrapCfg() (*clientcmdapi.Config, error) {
	if j.tlsBootstrapCfg != nil {
		return j.tlsBootstrapCfg, nil
	}

	var (
		client clientset.Interface
		err    error
	)
	if j.dryRun {
		client, err = j.Client()
		if err != nil {
			return nil, errors.Wrap(err, "could not create a client for TLS bootstrap")
		}
	}
	klog.V(1).Infoln("[preflight] Discovering cluster-info")
	tlsBootstrapCfg, err := discovery.For(client, j.cfg)
	j.tlsBootstrapCfg = tlsBootstrapCfg
	return tlsBootstrapCfg, err
}

// InitCfg 返回 InitConfiguration。
func (j *joinData) InitCfg() (*kubeadmapi.InitConfiguration, error) {
	if j.initCfg != nil {
		return j.initCfg, nil
	}
	if _, err := j.TLSBootstrapCfg(); err != nil {
		return nil, err
	}
	klog.V(1).Infoln("[preflight] Fetching init configuration")
	var client clientset.Interface
	if j.dryRun {
		var err error
		client, err = j.Client()
		if err != nil {
			return nil, errors.Wrap(err, "could not get dry-run client for fetching InitConfiguration")
		}
	}
	initCfg, err := fetchInitConfigurationFromJoinConfiguration(j.cfg, client, j.tlsBootstrapCfg)
	j.initCfg = initCfg
	return initCfg, err
}

// Client 返回以 admin.conf 中身份访问集群的客户端。
func (j *joinData) Client() (clientset.Interface, error) {
	pathAdmin := filepath.Join(j.KubeConfigDir(), kubeadmconstants.AdminKubeConfigFileName)

	if j.dryRun {
		dryRun := apiclient.NewDryRun()
		// 动态 dry-run 客户端仅在此 kubeconfig 存在时使用它，
		// 这种情况可能发生在 TLS 引导之后。
		if _, err := os.Stat(pathAdmin); err == nil {
			if err := dryRun.WithKubeConfigFile(pathAdmin); err != nil {
				return nil, err
			}
		} else if j.tlsBootstrapCfg != nil {
			if err := dryRun.WithKubeConfig(j.tlsBootstrapCfg); err != nil {
				return nil, err
			}
		} else if j.cfg.Discovery.BootstrapToken != nil {
			insecureConfig := token.BuildInsecureBootstrapKubeConfig(j.cfg.Discovery.BootstrapToken.APIServerEndpoint)
			resetConfig, err := clientcmd.NewDefaultClientConfig(*insecureConfig, &clientcmd.ConfigOverrides{}).ClientConfig()
			if err != nil {
				return nil, errors.Wrap(err, "failed to create API client configuration from kubeconfig")
			}
			if err := dryRun.WithRestConfig(resetConfig); err != nil {
				return nil, err
			}
		}

		dryRun.WithDefaultMarshalFunction().
			WithWriter(os.Stdout).
			AppendReactor(dryRun.GetClusterInfoReactor()).
			AppendReactor(dryRun.GetKubeadmConfigReactor()).
			AppendReactor(dryRun.GetKubeadmCertsReactor()).
			AppendReactor(dryRun.GetKubeProxyConfigReactor()).
			AppendReactor(dryRun.GetKubeletConfigReactor()).
			AppendReactor(dryRun.GetNodeReactor()).
			AppendReactor(dryRun.PatchNodeReactor())

		j.client = dryRun.FakeClient()
		return j.client, nil
	}

	client, err := kubeconfigutil.ClientSetFromFile(pathAdmin)
	if err != nil {
		return nil, errors.Wrap(err, "[preflight] couldn't create Kubernetes client")
	}
	j.client = client
	return client, nil
}

// WaitControlPlaneClient 返回用于等待控制平面组件在各自健康检查端点报告 ok 的基础客户端。
// 它以 admin.conf 为基础，但改为指向本地 API 服务器，而非控制平面端点。
func (j *joinData) WaitControlPlaneClient() (clientset.Interface, error) {
	pathAdmin := filepath.Join(j.KubeConfigDir(), kubeadmconstants.AdminKubeConfigFileName)
	config, err := clientcmd.LoadFromFile(pathAdmin)
	if err != nil {
		return nil, err
	}
	for _, v := range config.Clusters {
		v.Server = fmt.Sprintf("https://%s",
			net.JoinHostPort(
				j.Cfg().ControlPlane.LocalAPIEndpoint.AdvertiseAddress,
				strconv.Itoa(int(j.Cfg().ControlPlane.LocalAPIEndpoint.BindPort)),
			),
		)
	}
	client, err := kubeconfigutil.ToClientSet(config)
	if err != nil {
		return nil, err
	}
	return client, nil
}

// IgnorePreflightErrors 返回要忽略的预检错误列表。
func (j *joinData) IgnorePreflightErrors() sets.Set[string] {
	return j.ignorePreflightErrors
}

// OutputWriter 返回用于写入加入完成等消息的 io.Writer。
func (j *joinData) OutputWriter() io.Writer {
	return j.outputWriter
}

// PatchesDir 返回保存组件补丁的目录。
func (j *joinData) PatchesDir() string {
	// 若提供了参数值，则覆盖配置中的值。
	if len(j.patchesDir) > 0 {
		return j.patchesDir
	}
	if j.cfg.Patches != nil {
		return j.cfg.Patches.Directory
	}
	return ""
}

// fetchInitConfigurationFromJoinConfiguration 从 join 配置执行发现并获取 init 配置。
func fetchInitConfigurationFromJoinConfiguration(cfg *kubeadmapi.JoinConfiguration, client clientset.Interface, tlsBootstrapCfg *clientcmdapi.Config) (*kubeadmapi.InitConfiguration, error) {
	var err error

	klog.V(1).Infoln("[preflight] Retrieving KubeConfig objects")
	if client == nil {
		// 创建使用引导令牌身份访问集群的客户端。
		client, err = kubeconfigutil.ToClientSet(tlsBootstrapCfg)
		if err != nil {
			return nil, errors.Wrap(err, "unable to access the cluster")
		}
	}
	initConfiguration, err := fetchInitConfiguration(client)
	if err != nil {
		return nil, err
	}

	// 获取集群配置后，使用发现的集群名创建最终 KubeConfig 文件。
	_, clusterinfo, err := kubeconfigutil.GetClusterFromKubeConfig(tlsBootstrapCfg)
	if err != nil {
		return nil, errors.Wrap(err, "the TLS bootstrap kubeconfig is malformed")
	}
	tlsBootstrapCfg.Clusters = map[string]*clientcmdapi.Cluster{
		initConfiguration.ClusterName: clusterinfo,
	}
	currentContext, ok := tlsBootstrapCfg.Contexts[tlsBootstrapCfg.CurrentContext]
	if !ok || currentContext == nil {
		return nil, errors.Errorf("the TLS bootstrap kubeconfig is malformed: %q set as current-context, but not found in context list", tlsBootstrapCfg.CurrentContext)
	}
	currentContext.Cluster = initConfiguration.ClusterName

	// 将加入节点的信息注入 kubeadm 配置。
	initConfiguration.NodeRegistration = cfg.NodeRegistration
	if cfg.ControlPlane != nil {
		initConfiguration.LocalAPIEndpoint = cfg.ControlPlane.LocalAPIEndpoint
	}

	return initConfiguration, nil
}

// fetchInitConfiguration 从 kubeadm-admin ConfigMap 读取集群配置。
func fetchInitConfiguration(client clientset.Interface) (*kubeadmapi.InitConfiguration, error) {
	getNodeRegistration := false
	getAPIEndpoint := false
	getComponentConfigs := true
	initConfiguration, err := configutil.FetchInitConfigurationFromCluster(client, nil, "preflight", getNodeRegistration, getAPIEndpoint, getComponentConfigs, false)
	if err != nil {
		return nil, errors.Wrap(err, "unable to fetch the kubeadm-config ConfigMap")
	}

	return initConfiguration, nil
}
