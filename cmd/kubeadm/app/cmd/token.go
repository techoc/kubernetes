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
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/lithammer/dedent"
	"github.com/spf13/cobra"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/duration"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	clientset "k8s.io/client-go/kubernetes"
	bootstrapapi "k8s.io/cluster-bootstrap/token/api"
	bootstraputil "k8s.io/cluster-bootstrap/token/util"
	"k8s.io/klog/v2"

	bootstraptokenv1 "k8s.io/kubernetes/cmd/kubeadm/app/apis/bootstraptoken/v1"
	kubeadmscheme "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/scheme"
	kubeadmapiv1 "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/v1beta4"
	"k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/validation"
	outputapischeme "k8s.io/kubernetes/cmd/kubeadm/app/apis/output/scheme"
	outputapiv1alpha3 "k8s.io/kubernetes/cmd/kubeadm/app/apis/output/v1alpha3"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/options"
	cmdutil "k8s.io/kubernetes/cmd/kubeadm/app/cmd/util"
	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	tokenphase "k8s.io/kubernetes/cmd/kubeadm/app/phases/bootstraptoken/node"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/apiclient"
	configutil "k8s.io/kubernetes/cmd/kubeadm/app/util/config"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	kubeconfigutil "k8s.io/kubernetes/cmd/kubeadm/app/util/kubeconfig"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/output"
)

// newCmdToken 返回令牌管理命令。
func newCmdToken(out io.Writer, errW io.Writer) *cobra.Command {
	var kubeConfigFile string
	var dryRun bool
	tokenCmd := &cobra.Command{
		Use:   "token",
		Short: "Manage bootstrap tokens",
		Long: dedent.Dedent(`
			This command manages bootstrap tokens. It is optional and needed only for advanced use cases.

			In short, bootstrap tokens are used for establishing bidirectional trust between a client and a server.
			A bootstrap token can be used when a client (for example a node that is about to join the cluster) needs
			to trust the server it is talking to. Then a bootstrap token with the "signing" usage can be used.
			bootstrap tokens can also function as a way to allow short-lived authentication to the API Server
			(the token serves as a way for the API Server to trust the client), for example for doing the TLS Bootstrap.

			What is a bootstrap token more exactly?
			 - It is a Secret in the kube-system namespace of type "bootstrap.kubernetes.io/token".
			 - A bootstrap token must be of the form "[a-z0-9]{6}.[a-z0-9]{16}". The former part is the public token ID,
			   while the latter is the Token Secret and it must be kept private at all circumstances!
			 - The name of the Secret must be named "bootstrap-token-(token-id)".

			You can read more about bootstrap tokens here:
			  https://kubernetes.io/docs/admin/bootstrap-tokens/
		`),
	}
	cmdutil.RequireSubcommand(tokenCmd)

	options.AddKubeConfigFlag(tokenCmd.PersistentFlags(), &kubeConfigFile)
	tokenCmd.PersistentFlags().BoolVar(&dryRun,
		options.DryRun, dryRun, "Whether to enable dry-run mode or not")

	cfg := &kubeadmapiv1.InitConfiguration{}

	// cobra 帮助文本的默认值。
	kubeadmscheme.Scheme.Default(cfg)

	var cfgPath string
	var printJoinCommand bool
	var certificateKey string
	bto := options.NewBootstrapTokenOptions()

	createCmd := &cobra.Command{
		Use:                   "create [token]",
		DisableFlagsInUseLine: true,
		Short:                 "Create bootstrap tokens on the server",
		Long: dedent.Dedent(`
			This command will create a bootstrap token for you.
			You can specify the usages for this token, the "time to live" and an optional human friendly description.

			The [token] is the actual token to write.
			This should be a securely generated random token of the form "[a-z0-9]{6}.[a-z0-9]{16}".
			If no [token] is given, kubeadm will generate a random token instead.
		`),
		RunE: func(tokenCmd *cobra.Command, args []string) error {
			if len(args) > 0 {
				bto.TokenStr = args[0]
			}
			klog.V(1).Infoln("[token] validating mixed arguments")
			if err := validation.ValidateMixedArguments(tokenCmd.Flags()); err != nil {
				return err
			}

			if err := bto.ApplyTo(cfg); err != nil {
				return err
			}

			klog.V(1).Infoln("[token] getting Clientsets from kubeconfig file")
			kubeConfigFile = cmdutil.GetKubeConfigPath(kubeConfigFile)
			client, err := getClientForTokenCommands(kubeConfigFile, dryRun)
			if err != nil {
				return err
			}

			return RunCreateToken(out, client, cfgPath, cfg, printJoinCommand, certificateKey, kubeConfigFile)
		},
	}

	options.AddConfigFlag(createCmd.Flags(), &cfgPath)
	createCmd.Flags().BoolVar(&printJoinCommand,
		"print-join-command", false, "Instead of printing only the token, print the full 'kubeadm join' flag needed to join the cluster using the token.")
	createCmd.Flags().StringVar(&certificateKey,
		options.CertificateKey, "", "When used together with '--print-join-command', print the full 'kubeadm join' flag needed to join the cluster as a control-plane. To create a new certificate key you must use 'kubeadm init phase upload-certs --upload-certs'.")
	bto.AddTTLFlagWithName(createCmd.Flags(), "ttl")
	bto.AddUsagesFlag(createCmd.Flags())
	bto.AddGroupsFlag(createCmd.Flags())
	bto.AddDescriptionFlag(createCmd.Flags())

	tokenCmd.AddCommand(createCmd)
	tokenCmd.AddCommand(newCmdTokenGenerate(out))

	outputFlags := output.NewOutputFlags(&tokenTextPrintFlags{}).WithTypeSetter(outputapischeme.Scheme).WithDefaultOutput(output.TextOutput)

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List bootstrap tokens on the server",
		Long: dedent.Dedent(`
			This command will list all bootstrap tokens for you.
		`),
		RunE: func(tokenCmd *cobra.Command, args []string) error {
			kubeConfigFile = cmdutil.GetKubeConfigPath(kubeConfigFile)
			client, err := getClientForTokenCommands(kubeConfigFile, dryRun)
			if err != nil {
				return err
			}

			printer, err := outputFlags.ToPrinter()
			if err != nil {
				return errors.Wrap(err, "could not construct output printer")
			}

			return RunListTokens(out, errW, client, printer)
		},
		Args: cobra.NoArgs,
	}

	outputFlags.AddFlags(listCmd)

	tokenCmd.AddCommand(listCmd)

	deleteCmd := &cobra.Command{
		Use:                   "delete [token-value] ...",
		DisableFlagsInUseLine: true,
		Short:                 "Delete bootstrap tokens on the server",
		Long: dedent.Dedent(`
			This command will delete a list of bootstrap tokens for you.

			The [token-value] is the full Token of the form "[a-z0-9]{6}.[a-z0-9]{16}" or the
			Token ID of the form "[a-z0-9]{6}" to delete.
		`),
		RunE: func(tokenCmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return errors.Errorf("missing argument; 'token delete' is missing token of form %q or %q", bootstrapapi.BootstrapTokenPattern, bootstrapapi.BootstrapTokenIDPattern)
			}
			kubeConfigFile = cmdutil.GetKubeConfigPath(kubeConfigFile)
			client, err := getClientForTokenCommands(kubeConfigFile, dryRun)
			if err != nil {
				return err
			}

			return RunDeleteTokens(out, client, args)
		},
	}
	tokenCmd.AddCommand(deleteCmd)

	return tokenCmd
}

// newCmdTokenGenerate 返回生成新令牌的 cobra.Command。
func newCmdTokenGenerate(out io.Writer) *cobra.Command {
	return &cobra.Command{
		Use:   "generate",
		Short: "Generate and print a bootstrap token, but do not create it on the server",
		Long: dedent.Dedent(`
			This command will print out a randomly-generated bootstrap token that can be used with
			the "init" and "join" commands.

			You don't have to use this command in order to generate a token. You can do so
			yourself as long as it is in the format "[a-z0-9]{6}.[a-z0-9]{16}". This
			command is provided for convenience to generate tokens in the given format.

			You can also use "kubeadm init" without specifying a token and it will
			generate and print one for you.
		`),
		RunE: func(cmd *cobra.Command, args []string) error {
			return RunGenerateToken(out)
		},
		Args: cobra.NoArgs,
	}
}

// RunCreateToken 生成新的引导令牌，并将其作为 Secret 保存到服务器。
func RunCreateToken(out io.Writer, client clientset.Interface, cfgPath string, initCfg *kubeadmapiv1.InitConfiguration, printJoinCommand bool, certificateKey string, kubeConfigFile string) error {
	// ClusterConfiguration 仅用于调用 LoadOrDefaultInitConfiguration。
	clusterCfg := &kubeadmapiv1.ClusterConfiguration{
		// KubernetesVersion 未被使用，但显式设置它可避免执行
		// LoadOrDefaultInitConfiguration 时联网查询版本。
		KubernetesVersion: kubeadmconstants.CurrentKubernetesVersion.String(),
	}
	kubeadmscheme.Scheme.Default(clusterCfg)

	// 此调用根据可能存在或缺失的配置文件以及参数填充的默认配置，返回可直接使用的配置。
	klog.V(1).Infoln("[token] loading configurations")

	internalcfg, err := configutil.LoadOrDefaultInitConfiguration(cfgPath, initCfg, clusterCfg, configutil.LoadOrDefaultConfigurationOptions{
		SkipCRIDetect: true,
	})
	if err != nil {
		return err
	}

	klog.V(1).Infoln("[token] creating token")
	if err := tokenphase.CreateNewTokens(client, internalcfg.BootstrapTokens); err != nil {
		return err
	}

	// 若指定 --print-join-command，则打印可供机器读取的完整 kubeadm join 命令；
	// 否则仅打印令牌。
	if printJoinCommand {
		skipTokenPrint := false
		if certificateKey != "" {
			skipCertificateKeyPrint := false
			joinCommand, err := cmdutil.GetJoinControlPlaneCommand(kubeConfigFile, internalcfg.BootstrapTokens[0].Token.String(), certificateKey, skipTokenPrint, skipCertificateKeyPrint)
			if err != nil {
				return errors.Wrap(err, "failed to get join command")
			}
			joinCommand = strings.ReplaceAll(joinCommand, "\\\n", "")
			joinCommand = strings.ReplaceAll(joinCommand, "\t", "")
			fmt.Fprintln(out, joinCommand)
		} else {
			joinCommand, err := cmdutil.GetJoinWorkerCommand(kubeConfigFile, internalcfg.BootstrapTokens[0].Token.String(), skipTokenPrint)
			if err != nil {
				return errors.Wrap(err, "failed to get join command")
			}
			joinCommand = strings.ReplaceAll(joinCommand, "\\\n", "")
			joinCommand = strings.ReplaceAll(joinCommand, "\t", "")
			fmt.Fprintln(out, joinCommand)
		}
	} else {
		if certificateKey != "" {
			return errors.New("cannot use --certificate-key without --print-join-command")
		}
		fmt.Fprintln(out, internalcfg.BootstrapTokens[0].Token.String())
	}

	return nil
}

// RunGenerateToken 仅为用户生成随机令牌。
func RunGenerateToken(out io.Writer) error {
	klog.V(1).Infoln("[token] generating random token")
	token, err := bootstraputil.GenerateBootstrapToken()
	if err != nil {
		return err
	}

	fmt.Fprintln(out, token)
	return nil
}

func formatBootstrapToken(obj *outputapiv1alpha3.BootstrapToken) string {
	ttl := "<forever>"
	expires := "<never>"
	if obj.Expires != nil {
		ttl = duration.ShortHumanDuration(time.Until(obj.Expires.Time))
		expires = obj.Expires.Format(time.RFC3339)
	}
	ttl = fmt.Sprintf("%-9s", ttl)

	usages := strings.Join(obj.Usages, ",")
	if len(usages) == 0 {
		usages = "<none>"
	}
	usages = fmt.Sprintf("%-22s", usages)

	description := obj.Description
	if len(description) == 0 {
		description = "<none>"
	}
	description = fmt.Sprintf("%-56s", description)

	groups := strings.Join(obj.Groups, ",")
	if len(groups) == 0 {
		groups = "<none>"
	}

	return fmt.Sprintf("%s\t%s\t%s\t%s\t%s\t%s\n", obj.Token, ttl, expires, usages, description, groups)
}

// tokenTextPrinter 以文本形式打印引导令牌。
type tokenTextPrinter struct {
	output.TextPrinter
	columns         []string
	headerIsPrinted bool
}

// PrintObj 实现 ResourcePrinter.PrintObj，用于纯文本输出。
func (ttp *tokenTextPrinter) PrintObj(obj runtime.Object, writer io.Writer) error {
	tabw := tabwriter.NewWriter(writer, 10, 4, 3, ' ', 0)

	// 打印表头。
	if !ttp.headerIsPrinted {
		fmt.Fprintln(tabw, strings.Join(ttp.columns, "\t"))
		ttp.headerIsPrinted = true
	}

	// 打印令牌。
	fmt.Fprint(tabw, formatBootstrapToken(obj.(*outputapiv1alpha3.BootstrapToken)))

	return tabw.Flush()
}

// tokenTextPrintFlags 提供以文本形式打印引导令牌所需的参数。
type tokenTextPrintFlags struct{}

// ToPrinter 返回用于文本输出格式的 kubeadm 打印机。
func (tpf *tokenTextPrintFlags) ToPrinter(outputFormat string) (output.Printer, error) {
	if outputFormat == output.TextOutput {
		return &tokenTextPrinter{columns: []string{"TOKEN", "TTL", "EXPIRES", "USAGES", "DESCRIPTION", "EXTRA GROUPS"}}, nil
	}
	return nil, genericclioptions.NoCompatiblePrinterError{OutputFormat: &outputFormat, AllowedFormats: []string{output.TextOutput}}
}

// RunListTokens 列出服务器上全部现有引导令牌的详细信息。
func RunListTokens(out io.Writer, errW io.Writer, client clientset.Interface, printer output.Printer) error {
	// 首先构建仅用于引导令牌的选择器。
	klog.V(1).Infoln("[token] preparing selector for bootstrap token")
	tokenSelector := fields.SelectorFromSet(
		map[string]string{
			"type": string(bootstrapapi.SecretTypeBootstrapToken),
		},
	)
	listOptions := metav1.ListOptions{
		FieldSelector: tokenSelector.String(),
	}

	klog.V(1).Info("[token] retrieving list of bootstrap tokens")
	secrets, err := client.CoreV1().Secrets(metav1.NamespaceSystem).List(context.TODO(), listOptions)
	if err != nil {
		return errors.Wrap(err, "failed to list bootstrap tokens")
	}

	for _, secret := range secrets.Items {
		// 从 Secret 对象获取 BootstrapToken 结构表示。
		token, err := bootstraptokenv1.BootstrapTokenFromSecret(&secret)
		if err != nil {
			fmt.Fprintf(errW, "%v", err)
			continue
		}

		// 将令牌转换为带版本信息的输出结构。
		outputToken := outputapiv1alpha3.BootstrapToken{
			BootstrapToken: bootstraptokenv1.BootstrapToken{
				Token:       &bootstraptokenv1.BootstrapTokenString{ID: token.Token.ID, Secret: token.Token.Secret},
				Description: token.Description,
				TTL:         token.TTL,
				Expires:     token.Expires,
				Usages:      token.Usages,
				Groups:      token.Groups,
			},
		}

		if err := printer.PrintObj(&outputToken, out); err != nil {
			return errors.Wrapf(err, "unable to print token %s", token.Token)
		}
	}
	return nil
}

// RunDeleteTokens 从服务器删除引导令牌。
func RunDeleteTokens(out io.Writer, client clientset.Interface, tokenIDsOrTokens []string) error {
	for _, tokenIDOrToken := range tokenIDsOrTokens {
		// 假定这是令牌 ID，并尝试解析。
		tokenID := tokenIDOrToken
		klog.V(1).Info("[token] parsing token")
		if !bootstraputil.IsValidBootstrapTokenID(tokenIDOrToken) {
			// 用户可能传入了包含 ID 和密钥的完整令牌；解析并仅提取 ID。
			bts, err := bootstraptokenv1.NewBootstrapTokenString(tokenIDOrToken)
			if err != nil {
				return errors.Errorf("given token didn't match pattern %q or %q",
					bootstrapapi.BootstrapTokenPattern, bootstrapapi.BootstrapTokenIDPattern)
			}
			tokenID = bts.ID
		}

		tokenSecretName := bootstraputil.BootstrapTokenSecretName(tokenID)
		klog.V(1).Infof("[token] deleting token %q", tokenID)
		if err := client.CoreV1().Secrets(metav1.NamespaceSystem).Delete(context.TODO(), tokenSecretName, metav1.DeleteOptions{}); err != nil {
			return errors.Wrapf(err, "failed to delete bootstrap token %q", tokenID)
		}
		fmt.Fprintf(out, "bootstrap token %q deleted\n", tokenID)
	}
	return nil
}

// getClientForTokenCommands 返回供令牌命令使用的客户端。
// dry-run 时包含令牌专用的反应器。
func getClientForTokenCommands(file string, dryRun bool) (clientset.Interface, error) {
	if dryRun {
		dryRun := apiclient.NewDryRun().WithDefaultMarshalFunction().WithWriter(os.Stdout)
		dryRun.AppendReactor(dryRun.DeleteBootstrapTokenReactor())
		if err := dryRun.WithKubeConfigFile(file); err != nil {
			return nil, err
		}
		return dryRun.FakeClient(), nil
	}
	return kubeconfigutil.ClientSetFromFile(file)
}
