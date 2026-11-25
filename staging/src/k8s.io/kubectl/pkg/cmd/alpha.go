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

package cmd

import (
	"github.com/spf13/cobra"

	"k8s.io/cli-runtime/pkg/genericiooptions"

	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/util/i18n"
	"k8s.io/kubectl/pkg/util/templates"
)

// NewCmdAlpha 创建一个命令，作为 alpha 阶段特性的备用根命令
func NewCmdAlpha(f cmdutil.Factory, streams genericiooptions.IOStreams) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "alpha",
		Short: i18n.T("Commands for features in alpha"),
		Long:  templates.LongDesc(i18n.T("These commands correspond to alpha features that are not enabled in Kubernetes clusters by default.")),
	}

	// 如果没有子命令，NewKubeletCommand() 会隐藏 alpha 命令。此处覆盖
	// help 函数，确保即使有人输入了被隐藏的命令，也能得到合理的提示。
	if !cmd.HasAvailableSubCommands() {
		cmd.SetHelpFunc(func(*cobra.Command, []string) {
			cmd.Println(i18n.T("No alpha commands are available in this version of kubectl"))
		})
	}

	return cmd
}
