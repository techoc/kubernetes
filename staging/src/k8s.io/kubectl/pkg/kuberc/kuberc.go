/*
Copyright 2025 The Kubernetes Authors.

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

package kuberc

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/plugin/pkg/client/auth/exec"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/client-go/util/homedir"
	"k8s.io/kubectl/pkg/config"
)

const (
	RecommendedKubeRCFileName       = "kuberc"
	KubeRCOriginalCommandAnnotation = "kubectl.kubernetes.io/original-command"
)

var (
	RecommendedConfigDir  = filepath.Join(homedir.HomeDir(), clientcmd.RecommendedHomeDir)
	RecommendedKubeRCFile = filepath.Join(RecommendedConfigDir, RecommendedKubeRCFileName)

	aliasNameRegex = regexp.MustCompile("^[a-zA-Z]+$")
	shortHandRegex = regexp.MustCompile("^-[a-zA-Z]+$")
)

// 编译时检查，确保这两个类型一致。
var _ = clientcmdapi.AllowlistEntry(config.AllowlistEntry{})

// PreferencesHandler 负责根据用户的 kuberc 配置设置默认的
// 标志参数。
type PreferencesHandler interface {
	AddFlags(flags *pflag.FlagSet)
	Apply(rootCmd *cobra.Command, kubeConfigFlags *genericclioptions.ConfigFlags, args []string, errOut io.Writer) ([]string, error)
}

// Preferences 存储 kuberc 文件，该文件可能来自环境变量、
// 通过标志指定的文件，或默认的 kuberc 路径。
type Preferences struct {
	getPreferencesFunc func(kuberc string, errOut io.Writer) (*config.Preference, error)

	aliases map[string]struct{}
	policy  clientcmdapi.PluginPolicy
}

// NewPreferences 返回已初始化的 Preferences 对象。
func NewPreferences() PreferencesHandler {
	return &Preferences{
		getPreferencesFunc: DefaultGetPreferences,
		aliases:            make(map[string]struct{}),
	}
}

type aliasing struct {
	appendArgs      []string
	prependArgs     []string
	flags           []config.CommandOptionDefault
	command         *cobra.Command
	originalCommand bytes.Buffer
}

// AddFlags 将与 kuberc 相关的标志添加到命令中。
func (p *Preferences) AddFlags(flags *pflag.FlagSet) {
	flags.String("kuberc", "", "Path to the kuberc file to use for preferences. This can be disabled by exporting KUBECTL_KUBERC=false feature gate or turning off the feature KUBERC=off.")
}

// Apply 首先应用首选项文件中的别名，其次覆盖
// 标志的默认值。
func (p *Preferences) Apply(rootCmd *cobra.Command, kubeConfigFlags *genericclioptions.ConfigFlags, args []string, errOut io.Writer) ([]string, error) {
	if len(args) <= 1 {
		return args, nil
	}

	kubercPath, err := getExplicitKuberc(args)
	if err != nil {
		return args, err
	}
	kuberc, err := p.getPreferencesFunc(kubercPath, errOut)
	if err != nil {
		return args, fmt.Errorf("kuberc error %w", err)
	}

	if kuberc == nil {
		return args, nil
	}

	p.convertPluginPolicy(kuberc)

	err = p.validate(kuberc)
	if err != nil {
		return args, err
	}

	p.applyPluginPolicy(kubeConfigFlags, kuberc)

	args, err = p.applyAliases(rootCmd, kuberc, args, errOut)
	if err != nil {
		return args, err
	}
	err = p.applyOverrides(rootCmd, kuberc, args, errOut)
	if err != nil {
		return args, err
	}
	return args, nil
}

func (p *Preferences) convertPluginPolicy(kuberc *config.Preference) {
	var allowlist []clientcmdapi.AllowlistEntry
	if kuberc.CredentialPluginAllowlist != nil {
		allowlist = make([]clientcmdapi.AllowlistEntry, len(kuberc.CredentialPluginAllowlist))
		for i := range kuberc.CredentialPluginAllowlist {
			allowlist[i] = clientcmdapi.AllowlistEntry(kuberc.CredentialPluginAllowlist[i])
		}
	}
	p.policy = clientcmdapi.PluginPolicy{
		PolicyType: clientcmdapi.PolicyType(kuberc.CredentialPluginPolicy),
		Allowlist:  allowlist,
	}
}

// `applyPluginPolicy` 用一个包装器替换 rest 客户端获取器，
// 该包装器通过 rest 配置将允许列表传递给处理凭据 exec
// 插件的代码。
func (p *Preferences) applyPluginPolicy(kubeConfigFlags *genericclioptions.ConfigFlags, kuberc *config.Preference) {
	wrapConfigFn := kubeConfigFlags.WrapConfigFn
	kubeConfigFlags.WithWrapConfigFn(func(c *rest.Config) *rest.Config {
		if wrapConfigFn != nil {
			c = wrapConfigFn(c)
		}

		if c.ExecProvider != nil {
			c.ExecProvider.PluginPolicy = p.policy
		}

		return c
	})
}

// applyOverrides 查找命令，并设置 kuberc 中指定的标志默认值。
func (p *Preferences) applyOverrides(rootCmd *cobra.Command, kuberc *config.Preference, args []string, errOut io.Writer) error {
	args = args[1:]
	cmd, _, err := rootCmd.Find(args)
	if err != nil {
		return nil
	}
	originalCommand := bytes.Buffer{}
	for _, c := range kuberc.Defaults {
		parsedCmds := strings.Fields(c.Command)
		overrideCmd, _, err := rootCmd.Find(parsedCmds)
		if err != nil {
			fmt.Fprintf(errOut, "Warning: command %q not found to set kuberc override\n", c.Command)
			continue
		}
		if overrideCmd.Name() != cmd.Name() {
			continue
		}

		if _, ok := p.aliases[cmd.Name()]; ok {
			return fmt.Errorf("alias %s can not be overridden", cmd.Name())
		}

		// 调用此函数会触发合并父命令中的持久标志。
		_ = cmd.InheritedFlags()

		allShorthands := make(map[string]struct{})
		cmd.Flags().VisitAll(func(flag *pflag.Flag) {
			if flag.Shorthand != "" {
				allShorthands[flag.Shorthand] = struct{}{}
			}
		})

		for _, fl := range c.Options {
			existingFlag := cmd.Flag(fl.Name)
			if existingFlag == nil {
				return fmt.Errorf("invalid flag %s for command %s", fl.Name, c.Command)
			}
			if searchInArgs(existingFlag.Name, existingFlag.Shorthand, allShorthands, args) {
				// 如果参数中已显式传递该值，则不要隐式修改它。
				continue
			}
			err = cmd.Flags().Set(fl.Name, fl.Default)
			if err != nil {
				return fmt.Errorf("could not apply override value %s to flag %s in command %s err: %w", fl.Default, fl.Name, c.Command, err)
			}
			originalCommand.WriteString(fmt.Sprintf(" --%s=%s", fl.Name, fl.Default))
		}
		// 添加注解，以便追溯使用 kuberc 中所设默认值构建的命令。
		if cmd.Annotations == nil {
			cmd.Annotations = make(map[string]string, 1)
		}
		cmd.Annotations[KubeRCOriginalCommandAnnotation] = strings.Join(args, " ") + originalCommand.String()
		originalCommand.Reset()
	}

	return nil
}

// applyAliases 首先将 kuberc 文件中定义的所有别名追加到根命令。
// 由于可能有多个别名定义指向同一命令，它会从参数中
// 提取当前正在执行的别名。之后，它将别名中的标志定义设为
// 该命令的默认值。最后，将 kuberc 中作为参数传递的其他参数
// （如资源等）追加到命令参数中。
func (p *Preferences) applyAliases(rootCmd *cobra.Command, kuberc *config.Preference, args []string, errOut io.Writer) ([]string, error) {
	_, _, err := rootCmd.Find(args[1:])
	if err == nil {
		// 已找到命令，无需继续进行别名处理。
		return args, nil
	}

	var aliasArgs *aliasing
	var commandName string // 第一个“非标志”参数
	var commandIndex int
	for index, arg := range args[1:] {
		if !strings.HasPrefix(arg, "-") && !strings.HasPrefix(arg, cobra.ShellCompRequestCmd) {
			commandName = arg
			commandIndex = index + 1
			break
		}
	}

	for _, alias := range kuberc.Aliases {
		p.aliases[alias.Name] = struct{}{}
		if alias.Name != commandName {
			continue
		}

		// 不允许遮蔽内置命令。
		if _, _, err := rootCmd.Find([]string{alias.Name}); err == nil {
			fmt.Fprintf(errOut, "Warning: Setting alias %q to a built-in command is not supported\n", alias.Name)
			break
		}

		commands := strings.Fields(alias.Command)
		existingCmd, flags, err := rootCmd.Find(commands)
		if err != nil {
			return args, fmt.Errorf("command %q not found to set alias %q: %v", alias.Command, alias.Name, flags)
		}

		newCmd := *existingCmd
		newCmd.Use = alias.Name
		newCmd.Aliases = []string{}
		aliasCmd := &newCmd

		if alias.Name == commandName {
			aliasArgs = &aliasing{
				prependArgs:     alias.PrependArgs,
				appendArgs:      alias.AppendArgs,
				flags:           alias.Options,
				command:         aliasCmd,
				originalCommand: bytes.Buffer{},
			}
			aliasArgs.originalCommand.WriteString(alias.Command)
		}

		if aliasArgs == nil {
			// 沿用当前行为。
			// 这可能是内置命令、外部插件等。
			return args, nil
		}

		rootCmd.AddCommand(aliasArgs.command)

		foundAliasCmd, _, err := rootCmd.Find([]string{commandName})
		if err != nil {
			return args, nil
		}

		// 调用此函数会触发合并父命令中的持久标志。
		_ = foundAliasCmd.InheritedFlags()

		allShorthands := make(map[string]struct{})
		foundAliasCmd.Flags().VisitAll(func(flag *pflag.Flag) {
			if flag.Shorthand != "" {
				allShorthands[flag.Shorthand] = struct{}{}
			}
		})

		for _, fl := range aliasArgs.flags {
			existingFlag := foundAliasCmd.Flag(fl.Name)
			if existingFlag == nil {
				return args, fmt.Errorf("invalid alias flag %s in alias %s", fl.Name, args[0])
			}
			if searchInArgs(existingFlag.Name, existingFlag.Shorthand, allShorthands, args) {
				// 如果参数中已显式传递该值，则不要隐式修改它。
				continue
			}
			err = foundAliasCmd.Flags().Set(fl.Name, fl.Default)
			if err != nil {
				return args, fmt.Errorf("could not apply value %s to flag %s in alias %s err: %w", fl.Default, fl.Name, args[0], err)
			}
			aliasArgs.originalCommand.WriteString(fmt.Sprintf(" --%s=%s", fl.Name, fl.Default))
		}

		if len(aliasArgs.prependArgs) > 0 {
			// kuberc 中定义的 prependArgs 应插入到别名名称之后。
			if commandIndex+1 >= len(args) {
				// 命令是最后一项，直接像 appendArgs 一样追加即可。
				args = append(args, aliasArgs.prependArgs...)
			} else {
				args = append(args[:commandIndex+1], append(aliasArgs.prependArgs, args[commandIndex+1:]...)...)
			}
		}
		if len(aliasArgs.appendArgs) > 0 {
			// kuberc 中定义的 appendArgs 应追加到实际参数中。
			args = append(args, aliasArgs.appendArgs...)
		}
		// Cobra (command.go#L1078) 仅将根命令的参数追加到实际参数中，而忽略其他参数。
		// 我们在此处追加 kuberc 中定义的额外参数，
		// 期望它们会被一并传递给实际命令。
		rootCmd.SetArgs(args[1:])
		// 移除别名参数，以添加既不是标志也不属于首选项的其余参数。
		sanitizedArgs := strings.ReplaceAll(strings.Join(args[1:], " "), foundAliasCmd.Name(), "")
		if len(sanitizedArgs) > 0 {
			aliasArgs.originalCommand.WriteString(fmt.Sprintf(" %s", sanitizedArgs))
		}
		// 添加注解，以便追溯未应用别名时构建的命令。
		if aliasArgs.command.Annotations == nil {
			aliasArgs.command.Annotations = make(map[string]string, 1)
		}
		aliasArgs.command.Annotations[KubeRCOriginalCommandAnnotation] = aliasArgs.originalCommand.String()
		aliasArgs.originalCommand.Reset()
	}

	return args, nil
}

// LoadKuberc 返回正确的 kuberc 文件。显式指定的文件始终具有最高优先级。
// 如果未显式指定，则其次考虑 KUBERC 环境变量。
// 如果两者都未设置，则使用默认的 kuberc 位置。
func LoadKuberc(kuberc string) (string, bool, error) {
	if val := os.Getenv("KUBERC"); val == "off" {
		if kuberc != "" {
			return "", false, fmt.Errorf("disabling kuberc via KUBERC=off and passing kuberc flag are mutually exclusive")
		}
		return "", false, nil
	}
	kubeRCFile := RecommendedKubeRCFile
	explicitly := false
	if kuberc != "" {
		kubeRCFile = kuberc
		explicitly = true
	}

	if kubeRCFile == RecommendedKubeRCFile && os.Getenv("KUBERC") != "" {
		kubeRCFile = os.Getenv("KUBERC")
		explicitly = true
	}

	return kubeRCFile, explicitly, nil
}

// DefaultGetPreferences 返回 KubeRCConfiguration。
// 如果用户通过 --kuberc 标志显式设置了 kuberc 文件，
// 则该文件具有最高优先级。如果未指定，则查找 KUBERC 环境变量。
// 如果 KUBERC 也未设置，则回退到与 kubeconfig 默认位置
// 相同的目录下的默认 .kuberc 文件。
// 如果 KUBERC 设置为 "off"，则会关闭 kuberc，并应用 kubectl 的原有行为。
func DefaultGetPreferences(kuberc string, errOut io.Writer) (*config.Preference, error) {
	kubeRCFile, explicitly, err := LoadKuberc(kuberc)
	if err != nil {
		return nil, err
	}

	if kubeRCFile == "" {
		return nil, nil
	}

	preference, err := decodePreference(kubeRCFile)
	switch {
	case preference != nil && runtime.IsStrictDecodingError(err):
		// 如果已拿到可用的 Preference 对象，则仅对严格解码错误发出警告。
		fmt.Fprintf(errOut, "kuberc: ignoring strict decoding error in %s: %v", kubeRCFile, err) //nolint:errcheck
		return preference, nil

	case explicitly && err != nil:
		// 如果是显式请求的，则除 StrictDecodingError 外的任何错误都直接返回。
		return nil, fmt.Errorf("kuberc: %w", err)

	case !explicitly && os.IsNotExist(err):
		// 如果不是显式请求的，则静默忽略 kuberc 文件缺失。
		return nil, nil

	case !explicitly && err != nil:
		// 如果不是显式请求的，则对其他任何错误仅发出警告。
		fmt.Fprintf(errOut, "kuberc: no preferences loaded from %s: %v", kubeRCFile, err) //nolint:errcheck
		return nil, nil

	default:
		return preference, nil
	}
}

// 通常，我们应直接从 kuberc 标志中提取此值。
// 但是，标志值是在命令执行期间设置的，
// 而我们当前处于执行命令之前的非常早期的准备阶段。
// 此外，在这个阶段我们只需要 kuberc 标志的值。
func getExplicitKuberc(args []string) (string, error) {
	var kubercPath string
	for i, arg := range args {
		if arg == "--" {
			// "--" 之后的标志不代表命令的任何标志。
			// 我们应在此处提前结束遍历。
			break
		}
		if arg == "--kuberc" {
			if i+1 < len(args) {
				kubercPath = args[i+1]
				break
			}
			return "", fmt.Errorf("kuberc file is not found")
		} else if strings.Contains(arg, "--kuberc=") {
			parg := strings.Split(arg, "=")
			if len(parg) > 1 && parg[1] != "" {
				kubercPath = parg[1]
				break
			}
			return "", fmt.Errorf("kuberc file is not found")
		}
	}

	if kubercPath == "" {
		return "", nil
	}

	return kubercPath, nil
}

// searchInArgs 在参数中搜索给定的键，找到则返回 true，
// 否则返回 false。
func searchInArgs(flagName string, shorthand string, allShorthands map[string]struct{}, args []string) bool {
	for _, arg := range args {
		// 如果参数以 "--flag value" 或 "--flag=value" 格式设置了标志，
		// 则应将其视为已找到。
		if fmt.Sprintf("--%s", flagName) == arg || strings.HasPrefix(arg, fmt.Sprintf("--%s=", flagName)) {
			return true
		}
		if shorthand == "" {
			continue
		}
		// 简写可以是 "-n value" 或 "-nvalue" 格式。
		// 可以确定简写只有一个字母，因此仅检查前缀 -oyaml
		// 也能找到 --output。
		if strings.HasPrefix(arg, fmt.Sprintf("-%s", shorthand)) {
			return true
		}

		if !shortHandRegex.MatchString(arg) {
			continue
		}

		// 移除前缀 "-"。
		arg = arg[1:]
		// 简写可以组合成 "-abc" 格式。
		// 首先需要确保所有值都是简写，才能安全地搜索我们的目标。
		// 因为我们知道 "-abcvalue" 是无效的。所以需要确认，如果找到
		// "b"，它确实指的是简写 "b"，而不是任意值 "-cargb"。
		arbitraryFound := false
		for _, runeValue := range shorthand {
			if _, ok := allShorthands[string(runeValue)]; !ok {
				arbitraryFound = true
				break
			}
		}
		if arbitraryFound {
			continue
		}
		// 已确认所有值都是简写。现在搜索我们的目标。
		if strings.Contains(arg, shorthand) {
			return true
		}
	}
	return false
}

func (p *Preferences) validate(plugin *config.Preference) error {
	validateFlag := func(flags []config.CommandOptionDefault) error {
		for _, flag := range flags {
			if strings.HasPrefix(flag.Name, "-") {
				return fmt.Errorf("flag name %s should be in long form without dashes", flag.Name)
			}
		}
		return nil
	}
	aliases := make(map[string]struct{})
	for _, alias := range plugin.Aliases {
		if !aliasNameRegex.MatchString(alias.Name) {
			return fmt.Errorf("invalid alias name, can only include alphabetical characters")
		}

		if err := validateFlag(alias.Options); err != nil {
			return err
		}

		if _, ok := aliases[alias.Name]; ok {
			return fmt.Errorf("duplicate alias name %s", alias.Name)
		}
		aliases[alias.Name] = struct{}{}
	}

	for _, override := range plugin.Defaults {
		if err := validateFlag(override.Options); err != nil {
			return err
		}
	}

	if err := exec.ValidatePluginPolicy(p.policy); err != nil {
		return err
	}

	return nil
}
