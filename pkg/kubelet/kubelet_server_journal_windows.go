//go:build windows

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

package kubelet

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
)

// powershellExe 是 Windows 上查询事件日志使用的可执行文件。
const powershellExe = "PowerShell.exe"

// getLoggingCmd 返回针对给定 nodeLogQuery 与服务列表执行日志查询的
// PowerShell 命令、参数与环境变量。
// 所有字符串输入都通过环境变量传递，防止子命令表达式被 PowerShell 解释执行。
// 返回值依次为：
//   - cmd：待执行的命令；
//   - args：命令参数；
//   - cmdEnv：命令执行时的环境变量。
func getLoggingCmd(n *nodeLogQuery, services []string) (cmd string, args []string, cmdEnv []string, err error) {
	cmdEnv = getLoggingCmdEnv(n, services)

	// 先计算哪些过滤条件需要拼进命令，避免生成多余子句。
	var includeSinceTime, includeUntilTime, includeTailLines, includePattern bool
	if n.SinceTime != nil {
		includeSinceTime = true
	}
	if n.UntilTime != nil {
		includeUntilTime = true
	}
	if n.TailLines != nil {
		includeTailLines = true
	}
	if len(n.Pattern) > 0 {
		includePattern = true
	}

	// 记录每个 service 是否非空；空 service 不应成为 provider 过滤项。
	var includeServices []bool
	for _, service := range services {
		includeServices = append(includeServices, len(service) > 0)
	}

	args = getLoggingCmdArgs(includeSinceTime, includeUntilTime, includeTailLines, includePattern, includeServices)

	return powershellExe, args, cmdEnv, nil
}

// getLoggingCmdArgs 返回需要传给 powershellExe 的参数。
func getLoggingCmdArgs(includeSinceTime, includeUntilTime, includeTailLines, includePattern bool, services []bool) (args []string) {
	// -NonInteractive 防止命令等待输入；Bypass 跳过执行策略限制，
	// 保证脚本在受限节点上也能运行。
	args = []string{
		"-NonInteractive",
		"-ExecutionPolicy", "Bypass",
		"-Command",
	}

	// 使用 FilterHashtable 从 Application 日志读取事件。
	psCmd := `Get-WinEvent -FilterHashtable @{LogName='Application'`

	if includeSinceTime {
		psCmd += fmt.Sprintf(`; StartTime="$Env:kubelet_sinceTime"`)
	}
	if includeUntilTime {
		psCmd += fmt.Sprintf(`; EndTime="$Env:kubelet_untilTime"`)
	}

	// 把非空 service 映射为环境变量引用，实际值通过环境传入。
	var providers []string
	for i := range services {
		if services[i] {
			providers = append(providers, fmt.Sprintf("$Env:kubelet_provider%d", i))
		}
	}

	if len(providers) > 0 {
		psCmd += fmt.Sprintf("; ProviderName=%s", strings.Join(providers, ","))
	}

	// 关闭哈希表；随后按创建时间排序并做格式化输出。
	psCmd += `}`
	if includeTailLines {
		psCmd += fmt.Sprint(` -MaxEvents $Env:kubelet_tailLines`)
	}
	psCmd += ` | Sort-Object TimeCreated`

	if includePattern {
		psCmd += fmt.Sprintf(` | Where-Object -Property Message -Match "$Env:kubelet_pattern"`)
	}
	psCmd += ` | Format-Table -AutoSize -Wrap`

	args = append(args, psCmd)

	return args
}

// getLoggingCmdEnv 返回 powershellExe 执行时可见的环境变量。
func getLoggingCmdEnv(n *nodeLogQuery, services []string) (cmdEnv []string) {
	// 时间统一用 dateLayout 格式化，避免受系统区域设置影响。
	if n.SinceTime != nil {
		cmdEnv = append(cmdEnv, fmt.Sprintf("kubelet_sinceTime=%s", n.SinceTime.Format(dateLayout)))
	}
	if n.UntilTime != nil {
		cmdEnv = append(cmdEnv, fmt.Sprintf("kubelet_untilTime=%s", n.UntilTime.Format(dateLayout)))
	}

	for i, service := range services {
		if len(service) > 0 {
			cmdEnv = append(cmdEnv, fmt.Sprintf("kubelet_provider%d=%s", i, service))
		}
	}

	if n.TailLines != nil {
		cmdEnv = append(cmdEnv, fmt.Sprintf("kubelet_tailLines=%d", *n.TailLines))
	}

	if len(n.Pattern) > 0 {
		cmdEnv = append(cmdEnv, fmt.Sprintf("kubelet_pattern=%s", n.Pattern))
	}

	return cmdEnv
}

// checkForNativeLogger 在 Windows 上始终返回 true。
func checkForNativeLogger(ctx context.Context, service string) bool {
	cmd := exec.CommandContext(ctx, powershellExe, []string{
		"-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command",
		fmt.Sprintf("Get-WinEvent -ListProvider %s | Format-Table -AutoSize", service)}...)

	// 用 Get-WinEvent -ListProvider 探测服务是否注册为事件提供者。
	_, err := cmd.CombinedOutput()
	if err != nil {
		// 服务未注册为事件提供者时 Get-WinEvent 返回 ExitError，说明它不写事件日志。
		if _, ok := err.(*exec.ExitError); ok {
			return false
		}
		// 其他错误说明命令未真正执行，此时按"有原生日志"处理，
		// 让调用方继续用 Get-WinEvent 取日志，而不是假设服务写文件日志。
	}
	return true
}
