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
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/spf13/cobra"

	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/kubectl/pkg/cmd/plugin"
)

func TestNormalizationFuncGlobalExistence(t *testing.T) {
	// 当不再支持多种 flag 格式时，可以安全地删除此测试
	root := NewKubectlCommand(KubectlOptions{IOStreams: genericiooptions.IOStreams{In: os.Stdin, Out: os.Stdout, ErrOut: os.Stderr}})

	if root.Parent() != nil {
		t.Fatal("We expect the root command to be returned")
	}
	if root.GlobalNormalizationFunc() == nil {
		t.Fatal("We expect that root command has a global normalization function")
	}

	if reflect.ValueOf(root.GlobalNormalizationFunc()).Pointer() != reflect.ValueOf(root.Flags().GetNormalizeFunc()).Pointer() {
		t.Fatal("root command seems to have a wrong normalization function")
	}

	sub := root
	for sub.HasSubCommands() {
		sub = sub.Commands()[0]
	}

	// 如果此测试失败，请查看这个 PR：spf13/cobra#110
	if reflect.ValueOf(sub.Flags().GetNormalizeFunc()).Pointer() != reflect.ValueOf(root.Flags().GetNormalizeFunc()).Pointer() {
		t.Fatal("child and root commands should have the same normalization functions")
	}
}

func TestKubectlSubcommandShadowPlugin(t *testing.T) {
	tests := []struct {
		name              string
		args              []string
		expectPlugin      string
		expectPluginArgs  []string
		expectLookupError string
	}{
		{
			name:             "test that a plugin executable is found based on command args when builtin subcommand does not exist",
			args:             []string{"kubectl", "create", "foo", "--bar", "--bar2", "--namespace", "test-namespace"},
			expectPlugin:     "plugin/testdata/kubectl-create-foo",
			expectPluginArgs: []string{"--bar", "--bar2", "--namespace", "test-namespace"},
		},
		{
			name:              "test that a plugin executable is not found based on command args when also builtin subcommand does not exist",
			args:              []string{"kubectl", "create", "foo2", "--bar", "--bar2", "--namespace", "test-namespace"},
			expectLookupError: "unable to find a plugin executable \"kubectl-create-foo2\"",
		},
		{
			name:             "test that normal commands are able to be executed, when builtin subcommand exists",
			args:             []string{"kubectl", "create", "job", "foo", "--image=busybox", "--dry-run=client", "--namespace", "test-namespace"},
			expectPlugin:     "",
			expectPluginArgs: []string{},
		},
		// 其余测试复制自 TestKubectlCommandHandlesPlugins 函数，
		// 以便在该特性启用时也重新运行这些测试。
		{
			name:             "test that normal commands are able to be executed, when no plugin overshadows them",
			args:             []string{"kubectl", "config", "get-clusters"},
			expectPlugin:     "",
			expectPluginArgs: []string{},
		},
		{
			name:             "test that a plugin executable is found based on command args",
			args:             []string{"kubectl", "foo", "--bar"},
			expectPlugin:     "plugin/testdata/kubectl-foo",
			expectPluginArgs: []string{"--bar"},
		},
		{
			name: "test that a plugin does not execute over an existing command by the same name",
			args: []string{"kubectl", "version", "--client=true"},
		},
		{
			name: "test that a plugin does not execute over Cobra's help command",
			args: []string{"kubectl", "help"},
		},
		{
			name: "test that a plugin does not execute over Cobra's __complete command",
			args: []string{"kubectl", cobra.ShellCompRequestCmd, "de"},
		},
		{
			name: "test that a plugin does not execute over Cobra's __completeNoDesc command",
			args: []string{"kubectl", cobra.ShellCompNoDescRequestCmd, "de"},
		},
		{
			name: "test that a flag does not break Cobra's help command",
			args: []string{"kubectl", "--kubeconfig=/path/to/kubeconfig", "help"},
		},
		{
			name: "test that a flag does not break Cobra's __complete command",
			args: []string{"kubectl", "--kubeconfig=/path/to/kubeconfig", cobra.ShellCompRequestCmd},
		},
		{
			name: "test that a flag does not break Cobra's __completeNoDesc command",
			args: []string{"kubectl", "--kubeconfig=/path/to/kubeconfig", cobra.ShellCompNoDescRequestCmd},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pluginsHandler := &testPluginHandler{
				pluginsDirectory: "plugin/testdata",
				validPrefixes:    plugin.ValidPluginFilenamePrefixes,
			}
			ioStreams, _, _, _ := genericiooptions.NewTestIOStreams()
			root := NewDefaultKubectlCommandWithArgs(KubectlOptions{PluginHandler: pluginsHandler, Arguments: test.args, IOStreams: ioStreams})
			// 原始的 plugin handler（DefaultPluginHandler）通过 exec 调用实现，因此若激活 plugin 流程，不应期望 cobra 命令上有额外的动作
			if !pluginsHandler.lookedup && !pluginsHandler.executed {
				// 必须设置 args，否则 Execute 会使用 os.Args（启动测试时使用的参数），
				// 导致 test.args 不会传给命令，可能只调用 "kubectl" 而没有附带其他参数，从而产生误判
				root.SetArgs(test.args[1:])
				// 重要提示！错误的命令或校验失败的命令可能会直接调用 os.Exit(1)，从而中断测试执行
				if err := root.Execute(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}

			if (pluginsHandler.lookupErr != nil && pluginsHandler.lookupErr.Error() != test.expectLookupError) ||
				(pluginsHandler.lookupErr == nil && len(test.expectLookupError) > 0) {
				t.Fatalf("unexpected error: expected %q to occur, but got %q", test.expectLookupError, pluginsHandler.lookupErr)
			}

			if pluginsHandler.lookedup && !pluginsHandler.executed && len(test.expectLookupError) == 0 {
				// 这里必须判定失败，因为我们已经找到了 plugin，但既没有执行该 plugin，也没有执行命令（这通常会导致 unknown command 错误）
				t.Fatalf("expected plugin execution, but did not occur")
			}

			if pluginsHandler.executedPlugin != test.expectPlugin {
				t.Fatalf("unexpected plugin execution: expected %q, got %q", test.expectPlugin, pluginsHandler.executedPlugin)
			}

			if pluginsHandler.executed && len(test.expectPlugin) == 0 {
				t.Fatalf("unexpected plugin execution: expected no plugin, got %q", pluginsHandler.executedPlugin)
			}

			if !cmp.Equal(pluginsHandler.withArgs, test.expectPluginArgs, cmpopts.EquateEmpty()) {
				t.Fatalf("unexpected plugin execution args: expected %q, got %q", test.expectPluginArgs, pluginsHandler.withArgs)
			}

			if pluginsHandler.executed {
				expectKubectlPathEnvironmentVariable(t, pluginsHandler.withEnv)
			}
		})
	}
}

func TestKubectlCommandHandlesPlugins(t *testing.T) {
	tests := []struct {
		name              string
		args              []string
		expectPlugin      string
		expectPluginArgs  []string
		expectLookupError string
	}{
		{
			name:             "test that normal commands are able to be executed, when no plugin overshadows them",
			args:             []string{"kubectl", "config", "get-clusters"},
			expectPlugin:     "",
			expectPluginArgs: []string{},
		},
		{
			name:             "test that normal commands are able to be executed, when no plugin overshadows them and shadowing feature is not enabled",
			args:             []string{"kubectl", "create", "job", "foo", "--image=busybox", "--dry-run=client"},
			expectPlugin:     "",
			expectPluginArgs: []string{},
		},
		{
			name:             "test that a plugin executable is found based on command args",
			args:             []string{"kubectl", "foo", "--bar"},
			expectPlugin:     "plugin/testdata/kubectl-foo",
			expectPluginArgs: []string{"--bar"},
		},
		{
			name:             "test that a plugin executable is found based on command args with positional argument",
			args:             []string{"kubectl", "foo", "positional", "--bar"},
			expectPlugin:     "plugin/testdata/kubectl-foo",
			expectPluginArgs: []string{"positional", "--bar"},
		},
		{
			name:             "test that an allowed subcommand plugin executable is found based on command args with positional argument",
			args:             []string{"kubectl", "create", "foo", "positional", "--bar"},
			expectPlugin:     "plugin/testdata/kubectl-create-foo",
			expectPluginArgs: []string{"positional", "--bar"},
		},
		{
			name: "test that a plugin does not execute over an existing command by the same name",
			args: []string{"kubectl", "version", "--client=true"},
		},
		// 以下测试确保由 Cobra 添加的命令不会被 plugin 遮蔽
		// 参见 https://github.com/kubernetes/kubectl/issues/1116
		{
			name: "test that a plugin does not execute over Cobra's help command",
			args: []string{"kubectl", "help"},
		},
		{
			name: "test that a plugin does not execute over Cobra's __complete command",
			args: []string{"kubectl", cobra.ShellCompRequestCmd, "de"},
		},
		{
			name: "test that a plugin does not execute over Cobra's __completeNoDesc command",
			args: []string{"kubectl", cobra.ShellCompNoDescRequestCmd, "de"},
		},
		// 以下测试确保即使先指定了 flag，由 Cobra 添加的命令也不会被 plugin 遮蔽。
		// 使用别名时可能出现这种情况。
		// 参见 https://github.com/kubernetes/kubectl/issues/1119
		{
			name: "test that a flag does not break Cobra's help command",
			args: []string{"kubectl", "--kubeconfig=/path/to/kubeconfig", "help"},
		},
		{
			name: "test that a flag does not break Cobra's __complete command",
			args: []string{"kubectl", "--kubeconfig=/path/to/kubeconfig", cobra.ShellCompRequestCmd},
		},
		{
			name: "test that a flag does not break Cobra's __completeNoDesc command",
			args: []string{"kubectl", "--kubeconfig=/path/to/kubeconfig", cobra.ShellCompNoDescRequestCmd},
		},
		// 与之前的测试类似，别名可以在不使用 = 形式的情况下添加 flag。
		// 我们不支持这种情况，因为解析这类 flag 会变得相当复杂（区分为
		// 需要取值的 flag 和不需要取值的 flag）
		// {
		// 	name: "test that a flag with a space does not break Cobra's help command",
		// 	args: []string{"kubectl", "--kubeconfig", "/path/to/kubeconfig", "help"},
		// },
		// {
		// 	name: "test that a flag with a space does not break Cobra's __complete command",
		// 	args: []string{"kubectl", "--kubeconfig", "/path/to/kubeconfig", cobra.ShellCompRequestCmd},
		// },
		// {
		// 	name: "test that a flag with a space does not break Cobra's __completeNoDesc command",
		// 	args: []string{"kubectl", "--kubeconfig", "/path/to/kubeconfig", cobra.ShellCompNoDescRequestCmd},
		// },
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			pluginsHandler := &testPluginHandler{
				pluginsDirectory: "plugin/testdata",
				validPrefixes:    plugin.ValidPluginFilenamePrefixes,
			}
			ioStreams, _, _, _ := genericiooptions.NewTestIOStreams()
			root := NewDefaultKubectlCommandWithArgs(KubectlOptions{PluginHandler: pluginsHandler, Arguments: test.args, IOStreams: ioStreams})
			// 原始的 plugin handler（DefaultPluginHandler）通过 exec 调用实现，因此若激活 plugin 流程，不应期望 cobra 命令上有额外的动作
			if !pluginsHandler.lookedup && !pluginsHandler.executed {
				// 必须设置 args，否则 Execute 会使用 os.Args（启动测试时使用的参数），
				// 导致 test.args 不会传给命令，可能只调用 "kubectl" 而没有附带其他参数，从而产生误判
				root.SetArgs(test.args[1:])
				// 重要提示！错误的命令或校验失败的命令可能会直接调用 os.Exit(1)，从而中断测试执行
				if err := root.Execute(); err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
			}

			if (pluginsHandler.lookupErr != nil && pluginsHandler.lookupErr.Error() != test.expectLookupError) ||
				(pluginsHandler.lookupErr == nil && len(test.expectLookupError) > 0) {
				t.Fatalf("unexpected error: expected %q to occur, but got %q", test.expectLookupError, pluginsHandler.lookupErr)
			}

			if pluginsHandler.lookedup && !pluginsHandler.executed && len(test.expectLookupError) == 0 {
				// 这里必须判定失败，因为我们已经找到了 plugin，但既没有执行该 plugin，也没有执行命令（这通常会导致 unknown command 错误）
				t.Fatalf("expected plugin execution, but did not occur")
			}

			if pluginsHandler.executedPlugin != test.expectPlugin {
				t.Fatalf("unexpected plugin execution: expected %q, got %q", test.expectPlugin, pluginsHandler.executedPlugin)
			}

			if pluginsHandler.executed && len(test.expectPlugin) == 0 {
				t.Fatalf("unexpected plugin execution: expected no plugin, got %q", pluginsHandler.executedPlugin)
			}

			if !cmp.Equal(pluginsHandler.withArgs, test.expectPluginArgs, cmpopts.EquateEmpty()) {
				t.Fatalf("unexpected plugin execution args: expected %q, got %q", test.expectPluginArgs, pluginsHandler.withArgs)
			}

			if pluginsHandler.executed {
				expectKubectlPathEnvironmentVariable(t, pluginsHandler.withEnv)
			}
		})
	}
}

func TestPluginEnvironmentReplacesKubectlPath(t *testing.T) {
	t.Setenv("KUBECTL_PATH", "some-other-value")
	env, err := pluginEnvironment()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	expectKubectlPathEnvironmentVariable(t, env)
}

func expectKubectlPathEnvironmentVariable(t *testing.T, env []string) {
	t.Helper()
	kubectlPaths := slices.DeleteFunc(env, func(e string) bool {
		return !strings.HasPrefix(e, "KUBECTL_PATH=")
	})
	if len(kubectlPaths) > 1 {
		t.Fatalf("unexpected multiple KUBECTL_PATH environment variables: got %q", kubectlPaths)
	}
	if len(kubectlPaths) == 0 {
		t.Fatalf("unexpected missing KUBECTL_PATH environment variable")
	}
	if executable, err := os.Executable(); err != nil {
		t.Fatalf("unexpected error when getting executable path: %v", err)
	} else if kubectlPaths[0] != fmt.Sprintf("KUBECTL_PATH=%s", executable) {
		t.Fatalf("unexpected KUBECTL_PATH environment variable value: expected %q, got %q", fmt.Sprintf("KUBECTL_PATH=%s", executable), kubectlPaths[0])
	}
}

type testPluginHandler struct {
	pluginsDirectory string
	validPrefixes    []string

	// 查找结果
	lookedup  bool
	lookupErr error

	// 执行结果
	executed       bool
	executedPlugin string
	withArgs       []string
	withEnv        []string
}

func (h *testPluginHandler) Lookup(filename string) (string, bool) {
	h.lookedup = true

	dir, err := os.Stat(h.pluginsDirectory)
	if err != nil {
		h.lookupErr = err
		return "", false
	}

	if !dir.IsDir() {
		h.lookupErr = fmt.Errorf("expected %q to be a directory", h.pluginsDirectory)
		return "", false
	}

	plugins, err := os.ReadDir(h.pluginsDirectory)
	if err != nil {
		h.lookupErr = err
		return "", false
	}

	filenameWithSuportedPrefix := ""
	for _, prefix := range h.validPrefixes {
		for _, p := range plugins {
			filenameWithSuportedPrefix = fmt.Sprintf("%s-%s", prefix, filename)
			if p.Name() == filenameWithSuportedPrefix {
				h.lookupErr = nil
				return fmt.Sprintf("%s/%s", h.pluginsDirectory, p.Name()), true
			}
		}
	}

	h.lookupErr = fmt.Errorf("unable to find a plugin executable %q", filenameWithSuportedPrefix)
	return "", false
}

func (h *testPluginHandler) Execute(executablePath string, cmdArgs, env []string) error {
	h.executed = true
	h.executedPlugin = executablePath
	h.withArgs = cmdArgs
	h.withEnv = env
	return nil
}
