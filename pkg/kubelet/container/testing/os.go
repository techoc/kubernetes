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

package testing

import (
	"errors"
	"os"
	"sync"
	"time"
)

// FakeOS 模拟部分 OS 调用，避免测试扰动真实文件系统。
// 若某个 `*Fn` 字段被设置，则用它替代真实调用。
type FakeOS struct {
	StatFn     func(string) (os.FileInfo, error)
	ReadDirFn  func(string) ([]os.DirEntry, error)
	MkdirAllFn func(string, os.FileMode) error
	SymlinkFn  func(string, string) error
	GlobFn     func(string, string) bool
	HostName   string
	// Removes 记录被删除的路径，便于测试断言。
	Removes []string
	// Files 记录 Create 创建过的虚拟文件，供 Glob 匹配。
	Files     map[string][]*os.FileInfo
	FilesLock sync.RWMutex
}

// MkdirAll 是伪实现，默认返回 nil；设置 MkdirAllFn 时转调它。
func (f *FakeOS) MkdirAll(path string, perm os.FileMode) error {
	if f.MkdirAllFn != nil {
		return f.MkdirAllFn(path, perm)
	}
	return nil
}

// Symlink 是伪实现，默认返回 nil；设置 SymlinkFn 时转调它。
func (f *FakeOS) Symlink(oldname string, newname string) error {
	if f.SymlinkFn != nil {
		return f.SymlinkFn(oldname, newname)
	}
	return nil
}

// Stat 默认返回错误，除非设置了 StatFn。
func (f *FakeOS) Stat(path string) (os.FileInfo, error) {
	if f.StatFn != nil {
		return f.StatFn(path)
	}
	return nil, errors.New("unimplemented testing mock")
}

// Remove 记录删除路径并返回 nil。
func (f *FakeOS) Remove(path string) error {
	f.Removes = append(f.Removes, path)
	return nil
}

// RemoveAll 记录删除路径并返回 nil。
func (f *FakeOS) RemoveAll(path string) error {
	f.Removes = append(f.Removes, path)
	return nil
}

// Create 登记一个虚拟文件并返回 nil 句柄。
func (f *FakeOS) Create(path string) (*os.File, error) {
	f.FilesLock.Lock()
	defer f.FilesLock.Unlock()
	// 惰性初始化虚拟文件表。
	if f.Files == nil {
		f.Files = make(map[string][]*os.FileInfo)
	}
	f.Files[path] = []*os.FileInfo{}
	return nil, nil
}

// Chmod 是伪实现，直接返回 nil。
func (*FakeOS) Chmod(path string, perm os.FileMode) error {
	return nil
}

// Hostname 返回预置的主机名。
func (f *FakeOS) Hostname() (name string, err error) {
	return f.HostName, nil
}

// Chtimes 是伪实现，直接返回 nil。
func (*FakeOS) Chtimes(path string, atime time.Time, mtime time.Time) error {
	return nil
}

// Pipe 是伪实现，返回空句柄。
func (*FakeOS) Pipe() (r *os.File, w *os.File, err error) {
	return nil, nil, nil
}

// ReadDir 返回目录下的条目，默认返回空；设置 ReadDirFn 时转调它。
func (f *FakeOS) ReadDir(dirname string) ([]os.DirEntry, error) {
	if f.ReadDirFn != nil {
		return f.ReadDirFn(dirname)
	}
	return nil, nil
}

// Glob 按模式筛选虚拟文件表，返回匹配的路径。
func (f *FakeOS) Glob(pattern string) ([]string, error) {
	if f.GlobFn != nil {
		var res []string
		// 遍历虚拟文件表时加读锁，避免与 Create 的写入并发冲突。
		f.FilesLock.RLock()
		defer f.FilesLock.RUnlock()
		for k := range f.Files {
			if f.GlobFn(pattern, k) {
				res = append(res, k)
			}
		}
		return res, nil
	}
	return nil, nil
}

// Open 是伪实现，返回空句柄。
func (*FakeOS) Open(name string) (*os.File, error) {
	return nil, nil
}

// OpenFile 是伪实现，返回空句柄。
func (*FakeOS) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	return nil, nil
}

// Rename 是伪实现，直接返回 nil。
func (*FakeOS) Rename(oldpath, newpath string) error {
	return nil
}
