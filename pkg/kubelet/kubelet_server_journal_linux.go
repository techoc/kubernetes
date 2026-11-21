//go:build linux

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

// getLoggingCmd 根据 nodeLogQuery 与 boot 参数构造 journalctl 命令及参数。
// services 由调用方传入，以便应用服务发现启发式。
func getLoggingCmd(n *nodeLogQuery, services []string) (cmd string, args []string, cmdEnv []string, err error) {
	// 固定使用 UTC、不分页、短精确时间格式，便于日志解析。
	args = []string{
		"--utc",
		"--no-pager",
		"--output=short-precise",
	}
	// 按查询条件追加时间范围与行数限制。
	if n.SinceTime != nil {
		args = append(args, fmt.Sprintf("--since=%s", n.SinceTime.Format(dateLayout)))
	}
	if n.UntilTime != nil {
		args = append(args, fmt.Sprintf("--until=%s", n.UntilTime.Format(dateLayout)))
	}
	if n.TailLines != nil {
		args = append(args, "--pager-end", fmt.Sprintf("--lines=%d", *n.TailLines))
	}
	// 只把非空服务名加入 --unit 过滤。
	for _, service := range services {
		if len(service) > 0 {
			args = append(args, "--unit="+service)
		}
	}
	if len(n.Pattern) > 0 {
		args = append(args, "--grep="+n.Pattern)
	}

	if n.Boot != nil {
		args = append(args, "--boot", fmt.Sprintf("%d", *n.Boot))
	}

	return "journalctl", args, nil, nil
}

// checkForNativeLogger 检查 journalctl 是否认识该服务（即服务是否使用 journald 记录日志）。
func checkForNativeLogger(ctx context.Context, service string) bool {
	// 该命令会列出 journald 已知的所有 unit。
	cmd := exec.CommandContext(ctx, "journalctl", []string{"--field", "_SYSTEMD_UNIT"}...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		// 返回 false，以便调用方继续检查服务是否把日志写入文件。
		return false
	}

	// journalctl 查询不存在的服务也不会报错，
	// 因此需要在它已知的服务列表中搜索该服务。
	return strings.Contains(string(output), service+".service")
}
