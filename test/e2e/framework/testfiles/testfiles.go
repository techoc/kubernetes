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

// Package testfiles 封装了多种获取测试运行所需额外文件的方式：
// - 内置 bindata
// - 文件系统访问
//
// 由于是自包含的包，test/e2e/framework 和 test/e2e/manifest 都可以使用它，
// 而不会产生循环依赖。
package testfiles

import (
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
)

var filesources []FileSource

// AddFileSource 注册一个运行时可能需要的文件提供者。
// 应在测试二进制初始化期间调用。
func AddFileSource(filesource FileSource) {
	filesources = append(filesources, filesource)
}

// FileSource 实现一种获取测试文件内容的方式。
// 例如，一个文件源可以从原始源码目录树读取，另一个则从编译进测试二进制的 bindata 读取。
type FileSource interface {
	// ReadTestFile 获取与测试源码一起维护的文件内容。
	// 文件通过其在包含测试的仓库中的相对路径标识，
	// 例如 kubernetes/kubernetes 中的 "cluster/gce/upgrade.sh"。
	//
	// 文件未找到时返回 nil slice；发生致命错误时返回 error。
	ReadTestFile(filePath string) ([]byte, error)

	// DescribeFiles 返回多行描述，说明该来源可以提供哪些文件。
	// 用于在找不到文件时拼进错误信息，帮助用户定位问题。
	DescribeFiles() string
}

// Read 尝试从已注册的文件源中获取所需文件内容。
func Read(filePath string) ([]byte, error) {
	if len(filesources) == 0 {
		return nil, fmt.Errorf("no file sources registered (yet?), cannot retrieve test file %s", filePath)
	}
	for _, filesource := range filesources {
		data, err := filesource.ReadTestFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("fatal error retrieving test file %s: %w", filePath, err)
		}
		if data != nil {
			return data, nil
		}
	}
	// 这里生成错误信息时尽量给出提示，帮助测试作者或用户找到解决问题的方向。
	err := fmt.Sprintf("Test file %q was not found.\n", filePath)
	for _, filesource := range filesources {
		err += filesource.DescribeFiles()
		err += "\n"
	}
	return nil, errors.New(err)
}

// Exists 检查文件是否可读取。意外错误由调用方的 fail 函数处理，
// 该函数应终止当前测试。
func Exists(filePath string) (bool, error) {
	for _, filesource := range filesources {
		data, err := filesource.ReadTestFile(filePath)
		if err != nil {
			return false, err
		}
		if data != nil {
			return true, nil
		}
	}
	return false, nil
}

// RootFileSource 相对于指定根目录查找文件。
type RootFileSource struct {
	Root string
}

// ReadTestFile 相对于配置的根目录查找文件。
// 如果路径已是绝对路径（例如测试自己有确定文件位置的方式），则直接使用该路径。
func (r RootFileSource) ReadTestFile(filePath string) ([]byte, error) {
	var fullPath string
	if path.IsAbs(filePath) {
		fullPath = filePath
	} else {
		fullPath = filepath.Join(r.Root, filePath)
	}
	data, err := os.ReadFile(fullPath)
	if os.IsNotExist(err) {
		// 暂时不算错误，其他文件源可能提供该文件。
		return nil, nil
	}
	return data, err
}

// DescribeFiles 说明它在指定根目录下查找文件。
func (r RootFileSource) DescribeFiles() string {
	description := fmt.Sprintf("Test files are expected in %q", r.Root)
	if !path.IsAbs(r.Root) {
		// test_context.go 中的默认值是相对路径 ../../，
		// 对定位实际位置帮助不大。因此必要时也补上绝对路径。
		abs, err := filepath.Abs(r.Root)
		if err == nil {
			description += fmt.Sprintf(" = %q", abs)
		}
	}
	description += "."
	return description
}

// EmbeddedFileSource 处理使用 bindata 生成的包中存储的文件。
type EmbeddedFileSource struct {
	EmbeddedFS embed.FS
	Root       string
	fileList   []string
}

// ReadTestFile 按给定路径查找内嵌文件。
func (e EmbeddedFileSource) ReadTestFile(filepath string) ([]byte, error) {
	relativePath := strings.TrimPrefix(filepath, fmt.Sprintf("%s/", e.Root))

	b, err := e.EmbeddedFS.ReadFile(relativePath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	return b, nil
}

// DescribeFiles 说明它在内嵌文件系统中查找
func (e EmbeddedFileSource) DescribeFiles() string {
	var lines []string
	lines = append(lines, "The following files are embedded into the test executable:")

	if len(e.fileList) == 0 {
		e.populateFileList()
	}
	lines = append(lines, e.fileList...)

	return strings.Join(lines, "\n\t")
}

func (e *EmbeddedFileSource) populateFileList() {
	fs.WalkDir(e.EmbeddedFS, ".", func(path string, d fs.DirEntry, err error) error {
		if !d.IsDir() {
			e.fileList = append(e.fileList, filepath.Join(e.Root, path))
		}

		return nil
	})
}
