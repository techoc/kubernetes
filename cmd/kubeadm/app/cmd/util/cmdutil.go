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

package util

import (
	"bufio"
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"

	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/options"
	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

// ErrorSubcommandRequired 是父命令无法执行时返回的错误。
// 它以换行开头，以便与帮助屏幕等先前输出分隔。
var ErrorSubcommandRequired = errors.New("\nerror: subcommand is required")

// RequireSubcommand 可为 cobra 命令设置空的 Run 函数和 NoArgs。
// 它处理父命令 c 必须指定子命令的情况；若未指定子命令，CLI 将以错误退出。
func RequireSubcommand(c *cobra.Command) {
	c.RunE = func(c *cobra.Command, args []string) error {
		_ = c.Help()
		return ErrorSubcommandRequired
	}
	c.Args = cobra.NoArgs
}

// ValidateExactArgNumber 校验是否指定了必需的顶层参数。
func ValidateExactArgNumber(args []string, supportedArgs []string) error {
	lenSupported := len(supportedArgs)
	validArgs := 0
	// 忽略可能的空字符串参数；它们无效。
	for _, arg := range args {
		if len(arg) > 0 {
			validArgs++
		}
		// 参数过多时提前退出。
		if validArgs > lenSupported {
			return errors.Errorf("too many arguments. Required arguments: %v", supportedArgs)
		}
	}

	if validArgs < lenSupported {
		return errors.Errorf("missing one or more required arguments. Required arguments: %v", supportedArgs)
	}
	return nil
}

// GetKubeConfigPath 在传入空字符串时搜索标准位置中的 kubeconfig；
// 若传入非空字符串，则原样返回该字符串。
func GetKubeConfigPath(file string) string {
	// 若提供了值，则尊重该值。
	if file != "" {
		return file
	}
	// 使用 DefaultClientConfigLoadingRules 在标准位置查找配置，
	// 同时考虑默认配置路径。
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	rules.Precedence = append(rules.Precedence, kubeadmconstants.GetAdminKubeConfigPath())
	file = rules.GetDefaultFilename()
	klog.V(1).Infof("Using kubeconfig file: %s", file)
	return file
}

// AddCRISocketFlag 将 cri-socket 参数添加到指定参数集。
func AddCRISocketFlag(flagSet *pflag.FlagSet, criSocket *string) {
	flagSet.StringVar(
		criSocket, options.NodeCRISocket, *criSocket,
		"Path to the CRI socket to connect. If empty kubeadm will try to auto-detect this value; use this option only if you have more than one CRI installed or if you have non-standard CRI socket.",
	)
}

// InteractivelyConfirmAction 询问用户是否确实要执行该操作。
func InteractivelyConfirmAction(action, question string, r io.Reader) error {
	fmt.Printf("[%s] %s [y/N]: ", action, question)
	scanner := bufio.NewScanner(r)
	scanner.Scan()
	if err := scanner.Err(); err != nil {
		return errors.Wrap(err, "couldn't read from standard input")
	}
	answer := scanner.Text()
	if strings.EqualFold(answer, "y") || strings.EqualFold(answer, "yes") {
		return nil
	}

	return errors.New("won't proceed; the user didn't answer (Y|y) in order to continue")
}

// ValueFromFlagsOrConfig 检查是否设置了 name 参数；若已设置则返回参数值，否则返回配置中的值。
func ValueFromFlagsOrConfig(flagSet *pflag.FlagSet, name string, cfgValue interface{}, flagValue interface{}) interface{} {
	if flagSet.Changed(name) {
		return flagValue
	}

	// 若为布尔值则将 nil 转为 false，避免空指针解引用错误。
	cfg, ok := cfgValue.(*bool)
	if ok && cfg == nil {
		return ptr.To(false)
	}

	// 假定配置已正确设置全部默认值。
	return cfgValue
}

// TypeMismatchErr 返回描述类型不匹配原因的错误。
func TypeMismatchErr(opt, rType string) error {
	return errors.Errorf("type mismatch, %s is expected to be a pointer to %s", opt, rType)
}
