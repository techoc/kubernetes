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

package container

import (
	"os"
	"path/filepath"
	"time"
)

// OSInterface 汇集需要在测试中模拟的系统级操作。
type OSInterface interface {
	MkdirAll(path string, perm os.FileMode) error
	Symlink(oldname string, newname string) error
	Stat(path string) (os.FileInfo, error)
	Remove(path string) error
	RemoveAll(path string) error
	Create(path string) (*os.File, error)
	Chmod(path string, perm os.FileMode) error
	Hostname() (name string, err error)
	Chtimes(path string, atime time.Time, mtime time.Time) error
	Pipe() (r *os.File, w *os.File, err error)
	ReadDir(dirname string) ([]os.DirEntry, error)
	Glob(pattern string) ([]string, error)
	Open(name string) (*os.File, error)
	OpenFile(name string, flag int, perm os.FileMode) (*os.File, error)
	Rename(oldpath, newpath string) error
}

// RealOS 把系统级操作转发给真实的 os 包实现。
type RealOS struct{}

// MkdirAll 调用 os.MkdirAll 创建目录。
func (RealOS) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

// Symlink 调用 os.Symlink 创建符号链接。
func (RealOS) Symlink(oldname string, newname string) error {
	return os.Symlink(oldname, newname)
}

// Stat 调用 os.Stat 获取指定路径的 FileInfo。
func (RealOS) Stat(path string) (os.FileInfo, error) {
	return os.Stat(path)
}

// Remove 调用 os.Remove 删除指定路径。
func (RealOS) Remove(path string) error {
	return os.Remove(path)
}

// RemoveAll 调用 os.RemoveAll 递归删除路径及其子项。
func (RealOS) RemoveAll(path string) error {
	return os.RemoveAll(path)
}

// Create 调用 os.Create 在指定路径创建并返回文件。
func (RealOS) Create(path string) (*os.File, error) {
	return os.Create(path)
}

// Chmod 修改指定路径的权限，失败时返回错误。
func (RealOS) Chmod(path string, perm os.FileMode) error {
	return os.Chmod(path, perm)
}

// Hostname 调用 os.Hostname 返回主机名。
func (RealOS) Hostname() (name string, err error) {
	return os.Hostname()
}

// Chtimes 调用 os.Chtimes 修改路径的访问时间和修改时间。
func (RealOS) Chtimes(path string, atime time.Time, mtime time.Time) error {
	return os.Chtimes(path, atime, mtime)
}

// Pipe 调用 os.Pipe 返回一对相连的管道文件。
func (RealOS) Pipe() (r *os.File, w *os.File, err error) {
	return os.Pipe()
}

// ReadDir 调用 os.ReadDir 返回目录下的条目。
func (RealOS) ReadDir(dirname string) ([]os.DirEntry, error) {
	return os.ReadDir(dirname)
}

// Glob 调用 filepath.Glob 返回匹配模式的所有文件名。
func (RealOS) Glob(pattern string) ([]string, error) {
	return filepath.Glob(pattern)
}

// Open 调用 os.Open 打开并返回文件。
func (RealOS) Open(name string) (*os.File, error) {
	return os.Open(name)
}

// OpenFile 调用 os.OpenFile 按指定标志与权限打开文件。
func (RealOS) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(name, flag, perm)
}

// Rename 调用 os.Rename 重命名文件。
func (RealOS) Rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}
