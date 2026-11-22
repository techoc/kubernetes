/*
Copyright 2017 The Kubernetes Authors.

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

package filesystem

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

// DefaultFs 基于 "os" 和 "io" 中的同名函数实现 Filesystem 接口。
// root 非空时，所有路径都会加上该前缀，便于把文件操作限制在测试临时目录内。
type DefaultFs struct {
	root string
}

var _ Filesystem = &DefaultFs{}

// NewTempFs 返回一个以临时目录为根的 Filesystem，供单元测试使用，
// 避免测试操作污染真实文件系统。
func NewTempFs() Filesystem {
	path, _ := os.MkdirTemp("", "tmpfs")
	return &DefaultFs{
		root: path,
	}
}

func (fs *DefaultFs) prefix(path string) string {
	if len(fs.root) == 0 {
		return path
	}
	return filepath.Join(fs.root, path)
}

// Stat 调用 os.Stat 获取文件信息。
func (fs *DefaultFs) Stat(name string) (os.FileInfo, error) {
	return os.Stat(fs.prefix(name))
}

// Create 调用 os.Create 创建或截断文件。
func (fs *DefaultFs) Create(name string) (File, error) {
	file, err := os.Create(fs.prefix(name))
	if err != nil {
		return nil, err
	}
	return &defaultFile{file}, nil
}

// Rename 调用 os.Rename 重命名文件。
// 对已带 root 前缀的路径不重复添加前缀，避免双重拼接。
func (fs *DefaultFs) Rename(oldpath, newpath string) error {
	if !strings.HasPrefix(oldpath, fs.root) {
		oldpath = fs.prefix(oldpath)
	}
	if !strings.HasPrefix(newpath, fs.root) {
		newpath = fs.prefix(newpath)
	}
	return os.Rename(oldpath, newpath)
}

// MkdirAll 创建目录及其所有必要的父目录。
func (fs *DefaultFs) MkdirAll(path string, perm os.FileMode) error {
	return MkdirAll(fs.prefix(path), perm)
}

// MkdirAllWithPathCheck 先检查 path 是否已存在；若不存在，则创建该目录及所有必要的父目录。
// 创建时使用权限位 perm（umask 之前的值）。若 path 已经是目录，则不做任何操作并返回 nil。
// 注意：Windows NTFS 上挂载点通过重解析点（类似符号链接）实现，并非真正的目录，
// 因此在 Windows 上检查目录存在性时，还要接受符号链接或非常规文件类型。
func MkdirAllWithPathCheck(path string, perm os.FileMode) error {
	if dir, err := os.Lstat(path); err == nil {
		// 路径已存在时：
		// 1. Unix/Linux 下检查是否为目录；
		// 2. Windows NTFS 下挂载点是重解析点，需接受符号链接或非常规文件。
		if dir.IsDir() ||
			(runtime.GOOS == "windows" && (dir.Mode()&os.ModeSymlink != 0 || dir.Mode()&os.ModeIrregular != 0)) {
			return nil
		}
		return fmt.Errorf("path %v exists but is not a directory", path)
	}
	// Lstat 未找到该路径，说明目录尚不存在，尝试创建。
	if err := MkdirAll(path, perm); err != nil {
		return err
	}
	return nil
}

// Chtimes 调用 os.Chtimes 修改文件的访问和修改时间。
func (fs *DefaultFs) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return os.Chtimes(fs.prefix(name), atime, mtime)
}

// RemoveAll 调用 os.RemoveAll 递归删除路径及其内容。
func (fs *DefaultFs) RemoveAll(path string) error {
	return os.RemoveAll(fs.prefix(path))
}

// Remove 调用 os.Remove 删除文件或空目录。
func (fs *DefaultFs) Remove(name string) error {
	return os.Remove(fs.prefix(name))
}

// ReadFile 调用 os.ReadFile 读取文件的全部内容。
func (fs *DefaultFs) ReadFile(filename string) ([]byte, error) {
	return os.ReadFile(fs.prefix(filename))
}

// TempDir 调用 os.MkdirTemp 在指定目录下创建临时目录。
func (fs *DefaultFs) TempDir(dir, prefix string) (string, error) {
	return os.MkdirTemp(fs.prefix(dir), prefix)
}

// TempFile 调用 os.CreateTemp 在指定目录下创建临时文件。
func (fs *DefaultFs) TempFile(dir, prefix string) (File, error) {
	file, err := os.CreateTemp(fs.prefix(dir), prefix)
	if err != nil {
		return nil, err
	}
	return &defaultFile{file}, nil
}

// ReadDir 调用 os.ReadDir 读取目录下的条目。
func (fs *DefaultFs) ReadDir(dirname string) ([]os.DirEntry, error) {
	return os.ReadDir(fs.prefix(dirname))
}

// Walk 调用 filepath.Walk 遍历目录树。
func (fs *DefaultFs) Walk(root string, walkFn filepath.WalkFunc) error {
	return filepath.Walk(fs.prefix(root), walkFn)
}

// defaultFile 基于 *os.File 的同名方法实现 File 接口。
type defaultFile struct {
	file *os.File
}

// Name 返回文件名，委托给 os.File.Name。
func (file *defaultFile) Name() string {
	return file.file.Name()
}

// Write 写入数据，委托给 os.File.Write。
func (file *defaultFile) Write(b []byte) (n int, err error) {
	return file.file.Write(b)
}

// Sync 将文件内容同步落盘，委托给 os.File.Sync。
func (file *defaultFile) Sync() error {
	return file.file.Sync()
}

// Close 关闭文件，委托给 os.File.Close。
func (file *defaultFile) Close() error {
	return file.file.Close()
}
