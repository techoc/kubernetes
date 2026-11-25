/*
Copyright 2022 The Kubernetes Authors.

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

package plugin

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/templates"
)

func GetPluginCommandGroup(kubectl *cobra.Command) templates.CommandGroup {
	// 查找根层级
	return templates.CommandGroup{
		Message:  i18n.T("Subcommands provided by plugins:"),
		Commands: registerPluginCommands(kubectl, false),
	}
}

// SetupPluginCompletion 为每个插件向命令树添加一个
// Cobra 命令。仅在执行与插件相关的
// shell 补全时才会这样做。
func SetupPluginCompletion(cmd *cobra.Command, args []string) {
	kubectl := cmd.Root()
	if len(args) > 0 {
		if strings.HasPrefix(args[0], "-") {
			// 如果第一个参数是标志，则不支持插件，
			// 因此这种情况下无需添加插件。
			return
		}

		if len(args) == 1 {
			// 此时正在补全第一层的子命令，
			// 因此应包含所有插件名称。
			registerPluginCommands(kubectl, true)
			return
		}

		// 此时有多个参数。
		// 检查是否知道第一层子命令。
		// 如果不知道，它可能是插件，需要添加
		// 对应的插件命令才能使补全生效。
		found := false
		for _, subCmd := range kubectl.Commands() {
			if args[0] == subCmd.Name() {
				found = true
				break
			}
		}

		if !found {
			// 我们不知道调用补全时针对的是哪个子命令：
			// 它可能是插件。
			//
			// 使用插件时不支持 kubectl 全局标志。
			// 因此，执行补全时需要移除这些标志，
			// 以免它们出现在补全选项中。
			// 这必须在添加插件命令*之前*完成，
			// 这样创建这些插件命令时就不会存在这些标志。
			kubectl.ResetFlags()
			cobra.CompDebugln("Cleared global flags for plugin completion", true)

			registerPluginCommands(kubectl, true)
		}
	}
}

// registerPluginCommand 允许将 Cobra 命令添加到命令树，或提取出来用于
// 帮助函数或注册补全函数等场景
func registerPluginCommands(kubectl *cobra.Command, list bool) (cmds []*cobra.Command) {
	userDefinedCommands := []*cobra.Command{}

	streams := genericclioptions.IOStreams{
		In:     &bytes.Buffer{},
		Out:    io.Discard,
		ErrOut: io.Discard,
	}

	o := &PluginListOptions{IOStreams: streams}
	o.Complete(kubectl)
	plugins, _ := o.ListPlugins()

	for _, plugin := range plugins {
		plugin = filepath.Base(plugin)
		args := []string{}

		// 插件命名为 "kubectl-<name>"，也可能包含更多 -，例如
		// "kubectl-<name>-<subcmd1>..."
		rawPluginArgs := strings.Split(plugin, "-")[1:]
		pluginArgs := rawPluginArgs[:1]
		if list {
			pluginArgs = rawPluginArgs
		}

		// 遍历所有片段；对于 kubectl-my_plugin-sub_cmd，最终会进行
		// 两次迭代：一次处理 my_plugin，一次处理 sub_cmd。
		for _, arg := range pluginArgs {
			// 插件文件名中的下划线 (_) 会被替换为短横线 (-)
			// 例如 foo_bar -> foo-bar
			args = append(args, strings.ReplaceAll(arg, "_", "-"))
		}

		// 为避免同一个插件命令被重复添加，
		// 从根命令出发，根据这些参数找到层级最低的命令
		parentCmd, remainingArgs, _ := kubectl.Find(args)
		if parentCmd == nil {
			parentCmd = kubectl
		}

		for _, remainingArg := range remainingArgs {
			cmd := &cobra.Command{
				Use: remainingArg,
				// 添加将随补全选项一起显示的描述。
				// 让每个描述都包含插件名称以互不相同，
				// 避免 zsh 补全时把所有插件归到同一行。
				Short:              fmt.Sprintf(i18n.T("The command %s is a plugin installed by the user"), remainingArg),
				DisableFlagParsing: true,
				// 允许插件提供自己的补全选项
				ValidArgsFunction: pluginCompletion,
				// 必须提供 Run，命令才算有效
				Run: func(cmd *cobra.Command, args []string) {},
			}
			// 将插件命令添加到用户定义命令列表中
			userDefinedCommands = append(userDefinedCommands, cmd)

			if list {
				parentCmd.AddCommand(cmd)
				parentCmd = cmd
			}
		}
	}

	return userDefinedCommands
}

// pluginCompletion 处理插件名称之后的 shell 补全，可补全
// 插件的参数和标志。
// 它会在 $PATH 中查找某个特定的可执行文件，
// 该文件将为相应的插件提供补全。
//
// 调用时，该补全可执行文件应将补全选项打印到 stdout。
// 传给可执行文件的参数就是当前命令行中
// 插件的参数。例如，如果用户输入：
//
//	kubectl myplugin arg1 arg2 a<TAB>
//
// 则调用补全可执行文件时会传入参数："arg1" "arg2" "a"。
// 如果用户输入：
//
//	kubectl myplugin arg1 arg2 <TAB>
//
// 则调用补全可执行文件时会传入参数："arg1" "arg2" ""。
// 注意最后一个参数是空的，表示需要补全一个新词，
// 但用户尚未输入任何内容。
//
// Kubectl 的插件补全逻辑支持 Cobra 的 ShellCompDirective 机制。这意味着插件
// 可以选择在最后一行打印 :<value of a shell completion directive>，以向 shell
// 提供如何执行补全的指令。如果不存在该指令，则使用
// cobra.ShellCompDirectiveDefault。更多细节请参阅 Cobra 文档：
// https://github.com/spf13/cobra/blob/master/shell_completions.md#dynamic-completion-of-nouns
//
// 补全可执行文件应命名为 kubectl_complete-<plugin>。例如，对于名为
// kubectl-get_all 的插件，补全文件应命名为 kubectl_complete-get_all。补全
// 可执行文件必须设置可执行权限，并且必须位于 $PATH 中。
func pluginCompletion(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	// 从 commandPath 重建插件名称
	pluginName := strings.ReplaceAll(strings.ReplaceAll(cmd.CommandPath(), "-", "_"), " ", "-")

	path, found := lookupCompletionExec(pluginName)
	if !found {
		cobra.CompDebugln(fmt.Sprintf("Plugin %s does not provide a matching completion executable", pluginName), true)
		return nil, cobra.ShellCompDirectiveDefault
	}

	args = append(args, toComplete)
	cobra.CompDebugln(fmt.Sprintf("About to call: %s %s", path, strings.Join(args, " ")), true)
	return getPluginCompletions(path, args, os.Environ())
}

// lookupCompletionExec 查找是否存在这样的可执行文件：
// 它能为给定插件名称提供补全。
// 返回第一个匹配的文件路径；
// 如果找不到这样的可执行文件，则返回布尔值 false。
func lookupCompletionExec(pluginName string) (string, bool) {
	// 在第一个 - 前插入 "_complete"，将插件名称转换为插件补全名称。
	// 例如，将 kubectl-get_all 转换为 kubectl_complete-get_all
	pluginCompExec := strings.Replace(pluginName, "-", "_complete-", 1)
	cobra.CompDebugln(fmt.Sprintf("About to look for: %s", pluginCompExec), true)
	path, err := exec.LookPath(pluginCompExec)
	if err != nil || len(path) == 0 {
		return "", false
	}
	return path, true
}

// getPluginCompletions 接收可执行文件的路径、
// 一组参数，以及要传递给该可执行文件的
// 环境变量切片。
// 该可执行文件负责打印当前这组参数下
// 插件的补全结果。
func getPluginCompletions(executablePath string, cmdArgs, environment []string) ([]string, cobra.ShellCompDirective) {
	buf := new(bytes.Buffer)

	prog := exec.Command(executablePath, cmdArgs...)
	prog.Stdin = os.Stdin
	prog.Stdout = buf
	prog.Stderr = os.Stderr
	prog.Env = environment

	var comps []string
	directive := cobra.ShellCompDirectiveDefault
	if err := prog.Run(); err == nil {
		for _, comp := range strings.Split(buf.String(), "\n") {
			// 移除所有空行
			if len(comp) > 0 {
				comps = append(comps, comp)
			}
		}

		// 检查输出的最后一行是否为 :<integer> 形式，
		// 这表示 Cobra ShellCompDirective。这样做是为了兼容
		// 使用 Cobra 的插件，或希望借此指令向 shell
		// 传达特殊行为的插件。
		if len(comps) > 0 {
			lastLine := comps[len(comps)-1]
			if len(lastLine) > 1 && lastLine[0] == ':' {
				if strInt, err := strconv.Atoi(lastLine[1:]); err == nil {
					directive = cobra.ShellCompDirective(strInt)
					comps = comps[:len(comps)-1]
				}
			}
		}
	}
	return comps, directive
}
