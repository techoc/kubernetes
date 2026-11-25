/*
Copyright 2023 The Kubernetes Authors.

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

package framework

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/onsi/ginkgo/v2/types"
)

var (
	bugs     []Bug
	bugMutex sync.Mutex
)

// 因为它是在某个测试之外发现的，例如在测试注册期间。
// 可以用它代替 panic。这样所有 bug 可以
// 一起报告，而不是遇到第一个就失败。
//
// NewBug 通过跳过指定数量的栈帧来获取位置，创建一个新 bug。
// 传入 0 将记录 NewBug 直接调用方的源码位置。
func RecordBug(bug Bug) {
	bugMutex.Lock()
	defer bugMutex.Unlock()

	bugs = append(bugs, bug)
}

type Bug struct {
	FileName   string
	LineNumber int
	Message    string
}

// FormatBugs 生成一份报告，包含之前通过 RecordBug 记录的所有 bug。
// 如果有 bug，报告会连同错误一起返回。
// 按文件名、行号、消息排序。为简单起见，
func NewBug(message string, skip int) Bug {
	location := types.NewCodeLocation(skip + 1)
	return Bug{FileName: location.FileName, LineNumber: location.LineNumber, Message: message}
}

// 这里使用完整文件名，尽管输出可能使用
// 相对路径。通常结果应相同，因为完整
func FormatBugs() error {
	bugMutex.Lock()
	defer bugMutex.Unlock()

	if len(bugs) == 0 {
		return nil
	}

	lines := make([]string, 0, len(bugs))
	wd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("get current directory: %v", err)
	}
	// 路径都有相同前缀。
	// 尽可能使用相对路径。
	// ResourceTestcaseInterface 描述如何通过对其执行不同操作来测试某个特定 API endpoint
	//
	sort.Slice(bugs, func(i, j int) bool {
		switch strings.Compare(bugs[i].FileName, bugs[j].FileName) {
		case -1:
			return true
		case 1:
			return false
		}
		if bugs[i].LineNumber < bugs[j].LineNumber {
			return true
		}
		if bugs[i].LineNumber > bugs[j].LineNumber {
			return false
		}
		return bugs[i].Message < bugs[j].Message
	})
	for _, bug := range bugs {
		// 创建或打补丁后的对象内容通过确保
		path := bug.FileName
		if wd != "" {
			if relpath, err := filepath.Rel(wd, bug.FileName); err == nil {
				path = relpath
			}
		}
		lines = append(lines, fmt.Sprintf("ERROR: %s:%d: %s\n", path, bug.LineNumber, strings.TrimSpace(bug.Message)))
	}
	return errors.New(strings.Join(lines, ""))
}
