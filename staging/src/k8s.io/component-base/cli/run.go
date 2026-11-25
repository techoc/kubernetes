/*
Copyright 2021 The Kubernetes Authors.

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

package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/logs"
	"k8s.io/klog/v2"
)

// Run 提供执行 cobra 命令的公共样板代码：确保正确初始化日志，在尚未添加时
// 补充日志标志，归一化标志名称以便帮助文本使用连字符，并允许用下划线代替连字符。
// 日志初始化前，命令返回的错误以纯文本写入 stderr，覆盖标志解析错误和未知命令；
// 初始化后则记录日志，覆盖运行时错误。kubectl 等运行输出通常不含日志的命令
// 应改用 RunNoErrOutput，自行处理返回的错误。
// Run 返回进程退出码：0 表示成功，1 表示解析或执行失败。
func Run(cmd *cobra.Command) int {
	if logsInitialized, err := run(cmd); err != nil {
		// 标志解析错误若带上 klog 的装饰（例如
		// "E0923 23:02:03.219216 4168816 run.go:61] unknown shorthand flag"）会更难读；
		// klog.Fatal 更糟，它还会输出与错误无关的堆栈。
		// 但多数命令的输出本身是日志事件流，运行时错误仍应作为错误记录。
		// 是否已执行 logs.InitLogs() 可用于区分这两类错误。
		// 对 kubectl 等精心控制输出、默认不写日志的命令行工具，这一启发式并不合适；
		// 它们应改用 RunNoErrOutput。
		// 此外，当前无法判断命令是否成功配置 klog；实现
		// https://github.com/kubernetes/enhancements/pull/3078 中的
		// "contextual logging"（上下文日志）提案后或许可以做到。
		if !logsInitialized {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		} else {
			klog.ErrorS(err, "command failed")
		}
		return 1
	}
	return 0
}

// RunNoErrOutput 是 Run 的变体，返回 cobra 命令错误而不打印它。
// 适用于自行控制输出格式的命令（如 kubectl）。
func RunNoErrOutput(cmd *cobra.Command) error {
	_, err := run(cmd)
	return err
}

// run 完成所有命令共用的启动准备：日志 flush、flag 归一化、
// 错误用法打印策略、注入日志初始化钩子，最后执行命令。
func run(cmd *cobra.Command) (logsInitialized bool, err error) {
	// 无论成功失败都要冲刷日志，避免进程退出时丢失尾部日志。
	defer logs.FlushLogs()

	// 允许用下划线代替连字符，兼容历史命令行写法。
	cmd.SetGlobalNormalizationFunc(cliflag.WordSepNormalizeFunc)

	// Cobra 默认先打印标志解析错误，再打印可能很长的 usage；屏幕末尾因而看不到错误。
	// 按 #sig-cli 的建议，这里统一先打印 usage 再打印错误。
	// 但非解析原因导致的执行失败不应打印 usage，因此通过 FlagParseError 回调判断。
	// kubectl 等已自行处理此问题的命令不受影响。
	if !cmd.SilenceUsage {
		cmd.SilenceUsage = true
		// 仅在 flag 解析失败时重新启用 usage，使“先 usage 后错误”的布局只发生在解析错误上。
		cmd.SetFlagErrorFunc(func(c *cobra.Command, err error) error {
			// 重新启用 usage 打印。
			c.SilenceUsage = false
			return err
		})
	}

	// 所有错误统一在下方打印。
	cmd.SilenceErrors = true

	// 重复调用安全；命令自行注册过同名 flag 时不会覆盖。
	logs.AddFlags(cmd.PersistentFlags())

	// 将 logs.InitLogs 注入某个 PersistentPre* 函数，在命令行解析后执行；
	// 若启用竞态检测，同时输出提示。保留命令已有的 PersistentPreRun(E)，
	// 在其前面插入日志初始化，确保日志配置在业务逻辑运行前生效。
	switch {
	case cmd.PersistentPreRun != nil:
		pre := cmd.PersistentPreRun
		cmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
			logs.InitLogs()
			logsInitialized = true
			logRaceDetection()
			pre(cmd, args)
		}
	case cmd.PersistentPreRunE != nil:
		pre := cmd.PersistentPreRunE
		cmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
			logs.InitLogs()
			logsInitialized = true
			logRaceDetection()
			return pre(cmd, args)
		}
	default:
		cmd.PersistentPreRun = func(cmd *cobra.Command, args []string) {
			logs.InitLogs()
			logsInitialized = true
			logRaceDetection()
		}
	}

	err = cmd.Execute()
	return
}
