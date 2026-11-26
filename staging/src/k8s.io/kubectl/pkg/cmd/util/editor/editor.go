/*
Copyright 2015 The Kubernetes Authors.

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

package editor

import (
	"io"
	"os"
	"runtime"
	"strings"
)

const (
	// 原因见 Git 历史。
	// TODO: Windows 下可用 'start' 启动与文件类型关联的编辑器；
	// 但如果需要阻塞等待而不可用，
	// 可用带 'ftype' 和 'assoc' 的脚本检测。
	// 默认编辑器：Linux/macOS 使用 vi，Windows 使用 notepad
	defaultEditor = "vi"
	// 默认 shell：Linux/macOS 使用 bash，Windows 使用 cmd
	defaultShell  = "/bin/bash"
	windowsEditor = "notepad"
	windowsShell  = "cmd"
)

// Editor 编辑器配置，包含启动编辑器所需的命令行参数
// Shell 为 true 时，Args 会通过 shell 执行；否则直接执行
// 当编辑器路径包含空格或引号时，需要 shell 来解析参数
// 例如："vim -c 'set noswapfile'" 需要 shell 处理引号
// 而 "vim" 可以直接执行，无需 shell
// 这种设计允许用户配置复杂编辑器命令（如带参数的编辑器），
// 同时保持简单场景下直接执行的效率
type Editor struct {
	Args  []string // 编辑器命令及其参数
	Shell bool     // 是否通过 shell 执行（true 表示需要 shell 解析）
}

// NewDefaultEditor 创建 Editor 实例，从环境变量中查找编辑器程序
// 按 envs 列表的顺序查找（如 KUBE_EDITOR、EDITOR），找到第一个非空值即使用
// 如果编辑器命令不含空格和引号，直接执行（无需 shell）
// 如果包含引号，则通过 shell 执行以正确解析参数
// 例如："vim" 直接执行，"vim -c 'set noswapfile'" 通过 shell 执行
func NewDefaultEditor(envs []string) Editor {
	args, shell := defaultEnvEditor(envs)
	return Editor{
		Args:  args,
		Shell: shell,
	}
}

// defaultEnvShell 返回 shell 命令及其参数，用于执行编辑器
// 优先使用 SHELL 环境变量，未设置则根据操作系统选择默认 shell
// Linux/macOS 使用 bash -c，Windows 使用 cmd /C
// 返回的切片包含 shell 路径和执行标志，例如 ["/bin/bash", "-c"]
func defaultEnvShell() []string {
	shell := os.Getenv("SHELL")
	if len(shell) == 0 {
		shell = platformize(defaultShell, windowsShell)
	}
	// bash 使用 -c 标志执行命令，cmd 使用 /C
	flag := "-c"
	if shell == windowsShell {
		flag = "/C"
	}
	return []string{shell, flag}
}

// defaultEnvEditor 从环境变量中查找编辑器并解析为命令行参数
// 按 envs 顺序查找，找到第一个非空值即返回
// 返回编辑器参数列表和是否需要 shell 执行的标志
//
// 解析逻辑（按优先级）：
// 1. 无空格：直接执行，如 "vim" → ["vim"], false
// 2. 有空格但无引号：按空格分割，如 "vim -c set noswapfile" → ["vim", "-c", "set", "noswapfile"], false
// 3. 有引号：通过 shell 执行，如 "vim -c 'set noswapfile'" → ["/bin/bash", "-c", "vim -c 'set noswapfile'"], true
//
// 引号情况需要 shell 是因为 Go 的 strings.Split 无法正确处理引号内的空格，
// 而 shell 能正确解析引号、转义等复杂语法
func defaultEnvEditor(envs []string) ([]string, bool) {
	var editor string
	// 按优先级查找环境变量，找到第一个非空值即停止
	for _, env := range envs {
		if len(env) > 0 {
			editor = os.Getenv(env)
		}
		if len(editor) > 0 {
			break
		}
	}
	// 未设置任何编辑器环境变量，使用默认值
	if len(editor) == 0 {
		editor = platformize(defaultEditor, windowsEditor)
	}
	// 无空格：简单命令，直接执行
	if !strings.Contains(editor, " ") {
		return []string{editor}, false
	}
	// 有空格但无引号：按空格分割为命令和参数
	if !strings.ContainsAny(editor, "\"'\\") {
		return strings.Split(editor, " "), false
	}
	// 有引号或转义符：交给 shell 解析，避免自己处理复杂的 shell 语法
	shell := defaultEnvShell()
	return append(shell, editor), true
}

// LaunchTempFile 将内容写入临时文件并启动编辑器
// 创建临时文件后，等待用户编辑完成，返回编辑后的内容
// 返回编辑后的文件内容、临时文件路径（供调用者清理）和错误
//
// 流程：创建临时文件 → 写入内容 → 关闭文件描述符 → 启动编辑器 → 读取编辑后的内容
// 关闭文件描述符是因为某些编辑器（如 Windows 的 notepad）无法打开已被占用的文件
func (e Editor) LaunchTempFile(prefix, suffix string, r io.Reader) ([]byte, string, error) {
	f, err := os.CreateTemp("", prefix+"*"+suffix)
	if err != nil {
		return nil, "", err
	}
	defer f.Close()
	path := f.Name()
	// 将内容写入临时文件
	if _, err := io.Copy(f, r); err != nil {
		os.Remove(path)
		return nil, path, err
	}
	// 关闭文件描述符，让编辑器能独占打开该文件
	// Windows 上尤其重要：notepad 无法打开已被其他进程占用的文件
	f.Close()
	// 启动编辑器并阻塞等待用户保存退出
	if err := e.Launch(path); err != nil {
		return nil, path, err
	}
	// 读取编辑后的内容
	bytes, err := os.ReadFile(path)
	return bytes, path, err
}

// platformize 根据操作系统返回对应的值
// Windows 返回 windows 参数，其他系统返回 linux 参数
func platformize(linux, windows string) string {
	if runtime.GOOS == "windows" {
		return windows
	}
	return linux
}
