/*
Copyright 2016 The Kubernetes Authors.

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
	"os"
	"path/filepath"
)

// IoUtil 把常用 IO 操作抽象成接口，便于在测试中替换（mock）。
type IoUtil interface {
	ReadFile(filename string) ([]byte, error)
	ReadDir(dirname string) ([]os.FileInfo, error)
	Lstat(name string) (os.FileInfo, error)
	EvalSymlinks(path string) (string, error)
}

// osIOHandler 是 IoUtil 的真实操作系统实现。
type osIOHandler struct{}

// NewIOHandler 创建一个 IoUtil 的操作系统实现。
func NewIOHandler() IoUtil {
	return &osIOHandler{}
}

// ReadFile 读取指定文件的全部内容。
func (handler *osIOHandler) ReadFile(filename string) ([]byte, error) {
	return os.ReadFile(filename)
}

// ReadDir 读取目录下的条目信息；os.ReadDir 返回的是 DirEntry，需逐个转成 FileInfo。
func (handler *osIOHandler) ReadDir(dirname string) ([]os.FileInfo, error) {
	entries, err := os.ReadDir(dirname)
	if err != nil {
		return nil, err
	}
	infos := make([]os.FileInfo, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return nil, err
		}
		infos = append(infos, info)
	}
	return infos, nil
}

// Lstat 获取文件或符号链接自身的信息（不跟随链接）。
func (handler *osIOHandler) Lstat(name string) (os.FileInfo, error) {
	return os.Lstat(name)
}

// EvalSymlinks 解析路径中的所有符号链接，返回最终路径。
func (handler *osIOHandler) EvalSymlinks(path string) (string, error) {
	return filepath.EvalSymlinks(path)
}
