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

// Run provides the common boilerplate code around executing a cobra command.
// For example, it ensures that logging is set up properly. Logging
// flags get added to the command line if not added already. Flags get normalized
// so that help texts show them with hyphens. Underscores are accepted
// as alternative for the command parameters.
//
// Run tries to be smart about how to print errors that are returned by the
// command: before logging is known to be set up, it prints them as plain text
// to stderr. This covers command line flag parse errors and unknown commands.
// Afterwards it logs them. This covers runtime errors.
//
// Commands like kubectl where logging is not normally part of the runtime output
// should use RunNoErrOutput instead and deal with the returned error themselves.
//
// Run 返回进程退出码：0 表示成功，1 表示解析或执行失败。
func Run(cmd *cobra.Command) int {
	if logsInitialized, err := run(cmd); err != nil {
		// If the error is about flag parsing, then printing that error
		// with the decoration that klog would add ("E0923
		// 23:02:03.219216 4168816 run.go:61] unknown shorthand flag")
		// is less readable. Using klog.Fatal is even worse because it
		// dumps a stack trace that isn't about the error.
		//
		// But if it is some other error encountered at runtime, then
		// we want to log it as error, at least in most commands because
		// their output is a log event stream.
		//
		// We can distinguish these two cases depending on whether
		// we got to logs.InitLogs() above.
		//
		// This heuristic might be problematic for command line
		// tools like kubectl where the output is carefully controlled
		// and not a log by default. They should use RunNoErrOutput
		// instead.
		//
		// The usage of klog is problematic also because we don't know
		// whether the command has managed to configure it. This cannot
		// be checked right now, but may become possible when the early
		// logging proposal from
		// https://github.com/kubernetes/enhancements/pull/3078
		// ("contextual logging") is implemented.
		if !logsInitialized {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		} else {
			klog.ErrorS(err, "command failed")
		}
		return 1
	}
	return 0
}

// RunNoErrOutput is a version of Run which returns the cobra command error
// instead of printing it.
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

	// When error printing is enabled for the Cobra command, a flag parse
	// error gets printed first, then optionally the often long usage
	// text. This is very unreadable in a console because the last few
	// lines that will be visible on screen don't include the error.
	//
	// The recommendation from #sig-cli was to print the usage text, then
	// the error. We implement this consistently for all commands here.
	// However, we don't want to print the usage text when command
	// execution fails for other reasons than parsing. We detect this via
	// the FlagParseError callback.
	//
	// Some commands, like kubectl, already deal with this themselves.
	// We don't change the behavior for those.
	if !cmd.SilenceUsage {
		cmd.SilenceUsage = true
		// 仅在 flag 解析失败时重新启用 usage，使“先 usage 后错误”的布局只发生在解析错误上。
		cmd.SetFlagErrorFunc(func(c *cobra.Command, err error) error {
			// Re-enable usage printing.
			c.SilenceUsage = false
			return err
		})
	}

	// In all cases error printing is done below.
	cmd.SilenceErrors = true

	// This is idempotent.
	// 重复调用安全；命令自行注册过同名 flag 时不会覆盖。
	logs.AddFlags(cmd.PersistentFlags())

	// Inject logs.InitLogs after command line parsing into one of the
	// PersistentPre* functions. Also inform about race detection, if enabled.
	// 保留命令已有的 PersistentPreRun(E)，在其前面插入日志初始化，
	// 从而确保日志配置在业务逻辑运行前生效。
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
