/*
Copyright 2014 The Kubernetes Authors.

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
	"net/http"
	"os"
	"strings"
	"sync/atomic"

	"github.com/spf13/cobra"

	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/klog/v2"
	"k8s.io/kubectl/pkg/cmd/annotate"
	"k8s.io/kubectl/pkg/cmd/apiresources"
	"k8s.io/kubectl/pkg/cmd/apply"
	"k8s.io/kubectl/pkg/cmd/attach"
	"k8s.io/kubectl/pkg/cmd/auth"
	"k8s.io/kubectl/pkg/cmd/autoscale"
	"k8s.io/kubectl/pkg/cmd/certificates"
	"k8s.io/kubectl/pkg/cmd/clusterinfo"
	"k8s.io/kubectl/pkg/cmd/completion"
	cmdconfig "k8s.io/kubectl/pkg/cmd/config"
	"k8s.io/kubectl/pkg/cmd/cp"
	"k8s.io/kubectl/pkg/cmd/create"
	"k8s.io/kubectl/pkg/cmd/debug"
	"k8s.io/kubectl/pkg/cmd/delete"
	"k8s.io/kubectl/pkg/cmd/describe"
	"k8s.io/kubectl/pkg/cmd/diff"
	"k8s.io/kubectl/pkg/cmd/drain"
	"k8s.io/kubectl/pkg/cmd/edit"
	"k8s.io/kubectl/pkg/cmd/events"
	cmdexec "k8s.io/kubectl/pkg/cmd/exec"
	"k8s.io/kubectl/pkg/cmd/explain"
	"k8s.io/kubectl/pkg/cmd/expose"
	"k8s.io/kubectl/pkg/cmd/get"
	kuberccmd "k8s.io/kubectl/pkg/cmd/kuberc"
	"k8s.io/kubectl/pkg/cmd/kustomize"
	"k8s.io/kubectl/pkg/cmd/label"
	"k8s.io/kubectl/pkg/cmd/logs"
	"k8s.io/kubectl/pkg/cmd/options"
	"k8s.io/kubectl/pkg/cmd/patch"
	"k8s.io/kubectl/pkg/cmd/plugin"
	"k8s.io/kubectl/pkg/cmd/portforward"
	"k8s.io/kubectl/pkg/cmd/proxy"
	"k8s.io/kubectl/pkg/cmd/replace"
	"k8s.io/kubectl/pkg/cmd/rollout"
	"k8s.io/kubectl/pkg/cmd/run"
	"k8s.io/kubectl/pkg/cmd/scale"
	"k8s.io/kubectl/pkg/cmd/set"
	"k8s.io/kubectl/pkg/cmd/taint"
	"k8s.io/kubectl/pkg/cmd/top"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/cmd/version"
	"k8s.io/kubectl/pkg/cmd/wait"
	"k8s.io/kubectl/pkg/kuberc"
	utilcomp "k8s.io/kubectl/pkg/util/completion"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/templates"
)

type KubectlOptions struct {
	PluginHandler PluginHandler
	Arguments     []string
	ConfigFlags   *genericclioptions.ConfigFlags

	genericiooptions.IOStreams
}

func defaultConfigFlags() *genericclioptions.ConfigFlags {
	return genericclioptions.NewConfigFlags(true).WithDeprecatedPasswordFlag().WithDiscoveryBurst(300).WithDiscoveryQPS(50.0)
}

// NewDefaultKubectlCommand 用默认参数创建 `kubectl` 命令。
func NewDefaultKubectlCommand() *cobra.Command {
	ioStreams := genericiooptions.IOStreams{In: os.Stdin, Out: os.Stdout, ErrOut: os.Stderr}
	return NewDefaultKubectlCommandWithArgs(KubectlOptions{
		PluginHandler: NewDefaultPluginHandler(plugin.ValidPluginFilenamePrefixes),
		Arguments:     os.Args,
		ConfigFlags:   defaultConfigFlags().WithWarningPrinter(ioStreams),
		IOStreams:     ioStreams,
	})
}

// NewDefaultKubectlCommandWithArgs 创建带有参数的 `kubectl` 命令
func NewDefaultKubectlCommandWithArgs(o KubectlOptions) *cobra.Command {
	cmd := NewKubectlCommand(o)

	if o.PluginHandler == nil || len(o.Arguments) <= 1 {
		return cmd
	}

	cmdPathPieces := o.Arguments[1:]
	// 只有指定命令本身不存在时
	// 才查找可作为扩展的可执行文件。
	foundCmd, foundArgs, err := cmd.Find(cmdPathPieces)
	if err != nil {
		// 还要检查 Cobra 稍后会注入的命令。
		// 这些命令要到 rootCmd.Execute() 时才注入，
		// 因此这里必须显式检查。
		var cmdName string // first "non-flag" arguments
		for _, arg := range cmdPathPieces {
			if !strings.HasPrefix(arg, "-") {
				cmdName = arg
				break
			}
		}

		switch cmdName {
		case "help", cobra.ShellCompRequestCmd, cobra.ShellCompNoDescRequestCmd:
			// 不要按插件查找。
		default:
			if err := HandlePluginCommand(o.PluginHandler, cmdPathPieces, 1); err != nil {
				fmt.Fprintf(o.IOStreams.ErrOut, "Error: %v\n", err)
				os.Exit(1)
			}
		}
	}
	// 命令存在（如 kubectl create），但不能确定
	// 子命令也存在（如 kubectl create networkpolicy）。
	// 还需排除 kubectl create -f 这类形式。
	if IsSubcommandPluginAllowed(foundCmd.Name()) && len(foundArgs) >= 1 && !strings.HasPrefix(foundArgs[0], "-") {
		subcommand := foundArgs[0]
		builtinSubcmdExist := false
		for _, subcmd := range foundCmd.Commands() {
			if subcmd.Name() == subcommand {
				builtinSubcmdExist = true
				break
			}
		}

		if !builtinSubcmdExist {
			if err := HandlePluginCommand(o.PluginHandler, cmdPathPieces, len(cmdPathPieces)-len(foundArgs)+1); err != nil {
				fmt.Fprintf(o.IOStreams.ErrOut, "Error: %v\n", err)
				os.Exit(1)
			}
		}
	}

	return cmd
}

// NewKubectlCommand 创建 `kubectl` 命令及其所有子命令。
func NewKubectlCommand(o KubectlOptions) *cobra.Command {
	warningHandler := rest.NewWarningWriter(o.IOStreams.ErrOut, rest.WarningWriterOptions{Deduplicate: true, Color: printers.AllowsColorOutput(o.IOStreams.ErrOut)})
	warningsAsErrors := false
	var finishProfiling func() error
	// 所有子命令都挂载到这个父命令上。
	cmds := &cobra.Command{
		Use:   "kubectl",
		Short: i18n.T("kubectl controls the Kubernetes cluster manager"),
		Long: templates.LongDesc(`
      kubectl controls the Kubernetes cluster manager.

      Find more information at:
            https://kubernetes.io/docs/reference/kubectl/`),
		Run: runHelp,
		// Run 前后的钩子分别初始化并落盘性能剖析数据。
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			rest.SetDefaultWarningHandler(warningHandler)

			if cmd.Name() == cobra.ShellCompRequestCmd {
				// 这是 __complete 或 __completeNoDesc 命令，
				// 表示请求了 shell 补全。
				plugin.SetupPluginCompletion(cmd, args)
			}

			var err error
			finishProfiling, err = initProfiling()
			return err
		},
		PersistentPostRunE: func(*cobra.Command, []string) error {
			if finishProfiling != nil {
				if err := finishProfiling(); err != nil {
					return err
				}
			}
			if warningsAsErrors {
				count := warningHandler.WarningCount()
				switch count {
				case 0:
					// 不输出警告。
				case 1:
					return fmt.Errorf("%d warning received", count)
				default:
					return fmt.Errorf("%d warnings received", count)
				}
			}
			return nil
		},
	}
	// 此后对含 "_" 分隔符的标志，若以连字符形式添加，
	// 会给出规范化警告。
	cmds.SetGlobalNormalizationFunc(cliflag.WarnWordSepNormalizeFunc)

	flags := cmds.PersistentFlags()

	addProfilingFlags(flags)

	flags.BoolVar(&warningsAsErrors, "warnings-as-errors", warningsAsErrors, "Treat warnings received from the server as errors and exit with a non-zero exit code")

	pref := kuberc.NewPreferences()
	if !cmdutil.KubeRC.IsDisabled() {
		pref.AddFlags(flags)
	}

	kubeConfigFlags := o.ConfigFlags
	if kubeConfigFlags == nil {
		kubeConfigFlags = defaultConfigFlags().WithWarningPrinter(o.IOStreams)
	}
	kubeConfigFlags.AddFlags(flags)
	matchVersionKubeConfigFlags := cmdutil.NewMatchVersionFlags(kubeConfigFlags)
	matchVersionKubeConfigFlags.AddFlags(flags)
	// 更新钩子以添加 kubectl 命令头（SIG CLI KEP 859）。
	var isProxyCmd atomic.Bool
	addCmdHeaderHooks(cmds, kubeConfigFlags, &isProxyCmd)

	f := cmdutil.NewFactory(matchVersionKubeConfigFlags)

	// proxy 命令与 CommandHeaderRoundTripper 设置的头不兼容，
	// 因此 `addCmdHeaderHooks` 中的 RoundTripper 钩子
	// 需要识别子命令是否为 `proxy`。
	proxyCmd := proxy.NewCmdProxy(f, o.IOStreams)
	proxyCmd.PreRun = func(cmd *cobra.Command, args []string) {
		isProxyCmd.Store(true)
	}

	// 在此处而非 NewCmdGet() 中设置 ValidArgsFunction，避免导入循环。
	getCmd := get.NewCmdGet("kubectl", f, o.IOStreams)
	getCmd.ValidArgsFunction = utilcomp.ResourceTypeAndNameCompletionFunc(f)
	debugCmd := debug.NewCmdDebug(f, o.IOStreams)
	debugCmd.ValidArgsFunction = utilcomp.ResourceTypeAndNameCompletionFunc(f)

	groups := templates.CommandGroups{
		{
			Message: "Basic Commands (Beginner):",
			Commands: []*cobra.Command{
				create.NewCmdCreate(f, o.IOStreams),
				expose.NewCmdExposeService(f, o.IOStreams),
				run.NewCmdRun(f, o.IOStreams),
				set.NewCmdSet(f, o.IOStreams),
			},
		},
		{
			Message: "Basic Commands (Intermediate):",
			Commands: []*cobra.Command{
				explain.NewCmdExplain("kubectl", f, o.IOStreams),
				getCmd,
				edit.NewCmdEdit(f, o.IOStreams),
				delete.NewCmdDelete(f, o.IOStreams),
			},
		},
		{
			Message: "Deploy Commands:",
			Commands: []*cobra.Command{
				rollout.NewCmdRollout("kubectl", f, o.IOStreams),
				scale.NewCmdScale(f, o.IOStreams),
				autoscale.NewCmdAutoscale(f, o.IOStreams),
			},
		},
		{
			Message: "Cluster Management Commands:",
			Commands: []*cobra.Command{
				certificates.NewCmdCertificate(f, o.IOStreams),
				clusterinfo.NewCmdClusterInfo(f, o.IOStreams),
				top.NewCmdTop(f, o.IOStreams),
				drain.NewCmdCordon(f, o.IOStreams),
				drain.NewCmdUncordon(f, o.IOStreams),
				drain.NewCmdDrain(f, o.IOStreams),
				taint.NewCmdTaint(f, o.IOStreams),
			},
		},
		{
			Message: "Troubleshooting and Debugging Commands:",
			Commands: []*cobra.Command{
				describe.NewCmdDescribe("kubectl", f, o.IOStreams),
				logs.NewCmdLogs(f, o.IOStreams),
				attach.NewCmdAttach(f, o.IOStreams),
				cmdexec.NewCmdExec(f, o.IOStreams),
				portforward.NewCmdPortForward(f, o.IOStreams),
				proxyCmd,
				cp.NewCmdCp(f, o.IOStreams),
				auth.NewCmdAuth(f, o.IOStreams),
				debugCmd,
				events.NewCmdEvents(f, o.IOStreams),
			},
		},
		{
			Message: "Advanced Commands:",
			Commands: []*cobra.Command{
				diff.NewCmdDiff(f, o.IOStreams),
				apply.NewCmdApply("kubectl", f, o.IOStreams),
				patch.NewCmdPatch(f, o.IOStreams),
				replace.NewCmdReplace(f, o.IOStreams),
				wait.NewCmdWait(f, o.IOStreams),
				kustomize.NewCmdKustomize(o.IOStreams),
			},
		},
		{
			Message: "Settings Commands:",
			Commands: []*cobra.Command{
				label.NewCmdLabel(f, o.IOStreams),
				annotate.NewCmdAnnotate("kubectl", f, o.IOStreams),
				completion.NewCmdCompletion(o.IOStreams.Out, ""),
			},
		},
	}
	groups.Add(cmds)

	filters := []string{"options"}

	// 本次构建没有 alpha 命令时隐藏 "alpha" 子命令。
	alpha := NewCmdAlpha(f, o.IOStreams)
	if !alpha.HasSubCommands() {
		filters = append(filters, alpha.Name())
	}

	// 把插件命令组加入命令组列表。
	// 这些命令只为帮助信息和补全而注入，不会
	// 被直接调用。
	pluginCommandGroup := plugin.GetPluginCommandGroup(cmds)
	groups = append(groups, pluginCommandGroup)

	templates.ActsAsRootCommand(cmds, filters, groups...)

	utilcomp.SetFactoryForCompletion(f)
	registerCompletionFuncForGlobalFlags(cmds, f)

	cmds.AddCommand(alpha)
	cmds.AddCommand(cmdconfig.NewCmdConfig(f, clientcmd.NewDefaultPathOptions(), o.IOStreams))
	cmds.AddCommand(plugin.NewCmdPlugin(o.IOStreams))
	cmds.AddCommand(version.NewCmdVersion(f, o.IOStreams))
	cmds.AddCommand(apiresources.NewCmdAPIVersions(f, o.IOStreams))
	cmds.AddCommand(apiresources.NewCmdAPIResources(f, o.IOStreams))
	cmds.AddCommand(options.NewCmdOptions(o.IOStreams.Out))
	if !cmdutil.KubeRC.IsDisabled() {
		cmds.AddCommand(kuberccmd.NewCmdKubeRC(o.IOStreams))
	}

	// 停止标志规范化的警告，以便之后
	// 追加 klog 标志。
	cmds.SetGlobalNormalizationFunc(cliflag.WordSepNormalizeFunc)

	if !cmdutil.KubeRC.IsDisabled() {
		existingPreRunE := cmds.PersistentPreRunE
		cmds.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
			if originalCommandArgs, ok := cmd.Annotations[kuberc.KubeRCOriginalCommandAnnotation]; ok {
				originalCommand := fmt.Sprintf("%s %s", cmd.Root().Name(), originalCommandArgs)
				klog.V(1).Info(fmt.Sprintf("original command: %q", originalCommand))
			}
			return existingPreRunE(cmd, args)
		}
		_, err := pref.Apply(cmds, kubeConfigFlags, o.Arguments, o.IOStreams.ErrOut)
		if err != nil {
			fmt.Fprintf(o.IOStreams.ErrOut, "error occurred while applying preferences %v\n", err)
			os.Exit(1)
		}
	}

	return cmds
}

// addCmdHeaderHooks 更新两个钩子：
// 1. 修改传入 "cmds" 的持久化 pre-run 函数，解析命令头。
// 这些头随后会作为 X-header 添加到每次
// REST 调用中。
// 2. 在标准 RoundTripper 外包装一层 CommandHeaderRoundTripper。
// CommandHeaderRoundTripper 添加 X-Header 后再委托给
// 标准 RoundTripper。
//
// 详见 SIG CLI KEP 859：
//
//	https://github.com/kubernetes/enhancements/tree/master/keps/sig-cli/859-kubectl-headers
func addCmdHeaderHooks(cmds *cobra.Command, kubeConfigFlags *genericclioptions.ConfigFlags, isProxyCmd *atomic.Bool) {
	crt := &genericclioptions.CommandHeaderRoundTripper{}
	existingPreRunE := cmds.PersistentPreRunE
	// 在已有持久化 pre-run 函数中加入命令头解析。
	cmds.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		crt.ParseCommandHeaders(cmd, args)
		return existingPreRunE(cmd, args)
	}
	wrapConfigFn := kubeConfigFlags.WrapConfigFn
	// 用 CommandHeaderRoundTripper 包装标准 RoundTripper。
	kubeConfigFlags.WithWrapConfigFn(func(c *rest.Config) *rest.Config {
		if wrapConfigFn != nil {
			c = wrapConfigFn(c)
		}
		c.Wrap(func(rt http.RoundTripper) http.RoundTripper {
			// 必须使用独立的 RoundTripper，不能复用 "crt" 闭包。
			// Fixes: https://github.com/kubernetes/kubectl/issues/1098
			return &genericclioptions.CommandHeaderRoundTripper{
				Delegate:    rt,
				Headers:     crt.Headers,
				SkipHeaders: isProxyCmd, // proxy command is incompatible with these headers
			}
		})
		return c
	})
}

func runHelp(cmd *cobra.Command, args []string) {
	cmd.Help()
}

func registerCompletionFuncForGlobalFlags(cmd *cobra.Command, f cmdutil.Factory) {
	cmdutil.CheckErr(cmd.RegisterFlagCompletionFunc(
		"namespace",
		func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			return utilcomp.CompGetResource(f, "namespace", toComplete), cobra.ShellCompDirectiveNoFileComp
		}))
	cmdutil.CheckErr(cmd.RegisterFlagCompletionFunc(
		"context",
		func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			return utilcomp.ListContextsInConfig(toComplete), cobra.ShellCompDirectiveNoFileComp
		}))
	cmdutil.CheckErr(cmd.RegisterFlagCompletionFunc(
		"cluster",
		func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			return utilcomp.ListClustersInConfig(toComplete), cobra.ShellCompDirectiveNoFileComp
		}))
	cmdutil.CheckErr(cmd.RegisterFlagCompletionFunc(
		"user",
		func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
			return utilcomp.ListUsersInConfig(toComplete), cobra.ShellCompDirectiveNoFileComp
		}))
}

// GetLogVerbosity 解析命令行参数，确定
// 日志详细级别；返回级别字符串，未指定详细级别标志时
// 返回 0。
func GetLogVerbosity(args []string) string {
	for i, arg := range args {
		if arg == "--" {
			// "--" 之后的参数不属于当前命令的任何标志，
			// 这里应直接中断遍历。
			break
		}

		if arg == "--v" || arg == "-v" {
			if i+1 < len(args) {
				return args[i+1]
			}
		} else if strings.Contains(arg, "--v=") || strings.Contains(arg, "-v=") {
			parg := strings.Split(arg, "=")
			if len(parg) > 1 && parg[1] != "" {
				return parg[1]
			}
		}
	}

	return "0"
}
