/*
Copyright 2018 The Kubernetes Authors.

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

// Package naming 从调用栈中推导出简短、可读的组件名称。
//
// 典型用途是给 reflector/controller 生成日志标识：同一段代码创建的多个实例
// 会得到相同的名称，便于在日志中区分来源。
package naming

import (
	"fmt"
	"regexp"
	goruntime "runtime"
	"runtime/debug"
	"strconv"
	"strings"
)

// GetNameFromCallsite 沿调用栈向上查找第一个不属于 ignoredPackages 的调用方。
// 返回简化后的文件路径和行号，便于 reflector 开始记录日志时识别其创建位置。
func GetNameFromCallsite(ignoredPackages ...string) string {
	name := "????"
	const maxStack = 10
	// 只向上看有限层，避免深层调用栈带来过大开销；超出后尝试从 goroutine
	// 创建位置（created by）提取。
	for i := 1; i < maxStack; i++ {
		_, file, line, ok := goruntime.Caller(i)
		if !ok {
			file, line, ok = extractStackCreator()
			if !ok {
				break
			}
			i += maxStack
		}
		// 跳过被忽略的包以及 runtime 汇编桩，它们对定位调用方没有帮助。
		if hasPackage(file, append(ignoredPackages, "/runtime/asm_")) {
			continue
		}

		file = trimPackagePrefix(file)
		name = fmt.Sprintf("%s:%d", file, line)
		break
	}
	return name
}

// hasPackage 判断文件路径是否包含任意一个需要忽略的包路径片段。
func hasPackage(file string, ignoredPackages []string) bool {
	for _, ignoredPackage := range ignoredPackages {
		if strings.Contains(file, ignoredPackage) {
			return true
		}
	}
	return false
}

// trimPackagePrefix 移除文件路径中重复且不稳定的前缀，保留更有辨识度的包内路径。
//
// 依次尝试 vendor/src/pkg 三种路径前缀，取最靠后的一种，使不同构建布局下
// 得到的名称尽量一致。
func trimPackagePrefix(file string) string {
	if l := strings.LastIndex(file, "/vendor/"); l >= 0 {
		return file[l+len("/vendor/"):]
	}
	if l := strings.LastIndex(file, "/src/"); l >= 0 {
		return file[l+5:]
	}
	if l := strings.LastIndex(file, "/pkg/"); l >= 0 {
		return file[l+1:]
	}
	return file
}

// stackCreator 匹配 debug.Stack() 末尾的 "created by func\n file:line +0x..."。
var stackCreator = regexp.MustCompile(`(?m)^created by (.*)\n\s+(.*):(\d+) \+0x[[:xdigit:]]+$`)

// extractStackCreator 从 debug.Stack() 中提取创建当前 goroutine 的文件路径和行号。
// 如果无法定位创建位置，则返回 false。
// TODO：Go runtime 尚未通过公开 API 暴露该信息，参见 https://github.com/golang/go/issues/11440。
func extractStackCreator() (string, int, bool) {
	stack := debug.Stack()
	matches := stackCreator.FindStringSubmatch(string(stack))
	if len(matches) != 4 {
		return "", 0, false
	}
	line, err := strconv.Atoi(matches[3])
	if err != nil {
		return "", 0, false
	}
	return matches[2], line, true
}
