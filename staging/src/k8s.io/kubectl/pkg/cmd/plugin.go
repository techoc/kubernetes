/*
Copyright The Kubernetes Authors.

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
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
)

// PluginHandler 能够解析命令行参数，
// 并执行可执行文件名查找，以搜索
// 有效的插件文件，并执行找到的插件。
type PluginHandler interface {
	// 给定文件名处存在可执行文件，则返回后者；否则返回布尔值 false。
	// Lookup 会遍历给定的前缀列表，
	// 以识别有效的插件文件名。
	// 返回第一个匹配前缀的文件路径。
	Lookup(filename string) (string, bool)
	// Execute 接收可执行文件的路径、
	// 参数切片，以及要传递给该可执行文件
	// 的环境变量切片。
	Execute(executablePath string, cmdArgs, environment []string) error
}

// DefaultPluginHandler 实现 PluginHandler
type DefaultPluginHandler struct {
	ValidPrefixes []string
}

// NewDefaultPluginHandler 用给定的文件名前缀列表
// 实例化 DefaultPluginHandler，这些前缀用于识别有效的插件文件名。
func NewDefaultPluginHandler(validPrefixes []string) *DefaultPluginHandler {
	return &DefaultPluginHandler{
		ValidPrefixes: validPrefixes,
	}
}

// Lookup 实现 PluginHandler
func (h *DefaultPluginHandler) Lookup(filename string) (string, bool) {
	for _, prefix := range h.ValidPrefixes {
		path, err := exec.LookPath(fmt.Sprintf("%s-%s", prefix, filename))
		if shouldSkipOnLookPathErr(err) || len(path) == 0 {
			continue
		}
		return path, true
	}
	return "", false
}

// Execute 实现 PluginHandler
func (h *DefaultPluginHandler) Execute(executablePath string, cmdArgs, environment []string) error {
	// Windows 不支持 exec 系统调用。
	if runtime.GOOS == "windows" {
		cmd := command(executablePath, cmdArgs...)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Stdin = os.Stdin
		cmd.Env = environment
		err := cmd.Run()
		if err == nil {
			os.Exit(0)
		}
		return err
	}

	// 调用 cmd 二进制文件，并传递给定的环境和参数
	// 将 executablePath 追加到 cmdArgs，因为 execve 会把第一个参数当作"二进制名称"。
	return syscall.Exec(executablePath, append([]string{executablePath}, cmdArgs...), environment)
}

func command(name string, arg ...string) *exec.Cmd {
	cmd := &exec.Cmd{
		Path: name,
		Args: append([]string{name}, arg...),
	}
	if filepath.Base(name) == name {
		lp, err := exec.LookPath(name)
		if lp != "" && !shouldSkipOnLookPathErr(err) {
			// 即使 err 非 nil，也更新 cmd.Path。
			// 如果 err 是 ErrDot（尤其是在 Windows 上），lp 可能包含已解析的
			// 扩展名（如 .exe 或 .bat），应予以保留。
			cmd.Path = lp
		}
	}
	return cmd
}

// HandlePluginCommand 接收 pluginHandler 和命令行参数，并尝试在 PATH 上查找
// 满足给定参数的插件可执行文件。
func HandlePluginCommand(pluginHandler PluginHandler, cmdArgs []string, minArgs int) error {
	var remainingArgs []string // all "non-flag" arguments
	for _, arg := range cmdArgs {
		if strings.HasPrefix(arg, "-") {
			break
		}
		remainingArgs = append(remainingArgs, strings.Replace(arg, "-", "_", -1))
	}

	if len(remainingArgs) == 0 {
		// cmdArgs 的长度至少为 1
		return fmt.Errorf("flags cannot be placed before plugin name: %s", cmdArgs[0])
	}

	foundBinaryPath := ""

	// 尝试查找二进制文件，从使用给定 cmdArgs 组成的最长可能名称开始
	for len(remainingArgs) > 0 {
		path, found := pluginHandler.Lookup(strings.Join(remainingArgs, "-"))
		if !found {
			remainingArgs = remainingArgs[:len(remainingArgs)-1]
			if len(remainingArgs) < minArgs {
				// 不应继续用更短的名称搜索。
				// 这尤其用于避免在 kubectl-create-foo 插件未找到时
				// 继续搜索 kubectl-create 插件。
				break
			}

			continue
		}

		foundBinaryPath = path
		break
	}

	if len(foundBinaryPath) == 0 {
		return nil
	}

	pluginEnv, err := pluginEnvironment()
	if err != nil {
		return err
	}

	// 调用 cmd 二进制文件，并传递当前环境和给定参数
	if err := pluginHandler.Execute(foundBinaryPath, cmdArgs[len(remainingArgs):], pluginEnv); err != nil {
		return err
	}

	return nil
}

func pluginEnvironment() ([]string, error) {
	env := os.Environ()
	pluginEnv := make([]string, 0, len(env)+1)
	for _, e := range env {
		if !strings.HasPrefix(e, "KUBECTL_PATH=") {
			pluginEnv = append(pluginEnv, e)
		}
	}

	kubectlPath, err := os.Executable()
	if err != nil {
		return nil, err
	}

	return append(pluginEnv, "KUBECTL_PATH="+kubectlPath), nil
}

// 如果给定命令没有对应的内置子命令，IsSubcommandPluginAllowed 返回
// 是否允许使用插件作为该子命令。
func IsSubcommandPluginAllowed(foundCmd string) bool {
	allowedCmds := map[string]struct{}{"create": {}}
	_, ok := allowedCmds[foundCmd]
	return ok
}
