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
	"os"
	"path/filepath"
	"time"
)

// Filesystem 是对 os 包文件系统操作的抽象，便于在测试中 mock。
type Filesystem interface {
	// 以下方法来自 "os" 包
	Stat(name string) (os.FileInfo, error)
	Create(name string) (File, error)
	Rename(oldpath, newpath string) error
	MkdirAll(path string, perm os.FileMode) error
	Chtimes(name string, atime time.Time, mtime time.Time) error
	RemoveAll(path string) error
	Remove(name string) error

	// 以下方法来自 "os" 包
	ReadFile(filename string) ([]byte, error)
	TempDir(dir, prefix string) (string, error)
	TempFile(dir, prefix string) (File, error)
	ReadDir(dirname string) ([]os.DirEntry, error)
	Walk(root string, walkFn filepath.WalkFunc) error
}

// File 是对 os.File 常用方法的抽象，便于 mock 文件操作。
type File interface {
	// 目前只用到 os.File 的这些方法，有其他需求时再按需添加
	Name() string
	Write(b []byte) (n int, err error)
	Sync() error
	Close() error
}
