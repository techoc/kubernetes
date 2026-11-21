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

package store

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	utilfs "k8s.io/kubernetes/pkg/util/filesystem"
)

const (
	// 临时文件的前缀。
	tmpPrefix = "."
)

// FileStore 是 Store 接口的实现，把数据保存为文件。
type FileStore struct {
	// 数据文件根目录的绝对路径。
	directoryPath string

	// 使用的文件系统抽象。
	filesystem utilfs.Filesystem
}

// NewFileStore 创建 FileStore 实例，并确保根目录存在。
func NewFileStore(path string, fs utilfs.Filesystem) (Store, error) {
	if err := fs.MkdirAll(path, 0755); err != nil {
		return nil, err
	}
	return &FileStore{directoryPath: path, filesystem: fs}, nil
}

// Write 把数据写入名为 key 的文件。
func (f *FileStore) Write(key string, data []byte) error {
	// key 会拼进文件路径，必须先校验，防止路径穿越。
	if err := ValidateKey(key); err != nil {
		return err
	}
	// 写入前确保目录存在，兼容目录被外部删除的情况。
	if err := f.filesystem.MkdirAll(f.directoryPath, 0755); err != nil {
		return err
	}

	return writeFile(f.filesystem, f.getPathByKey(key), data)
}

// Read 读取名为 key 的文件内容。
func (f *FileStore) Read(key string) ([]byte, error) {
	if err := ValidateKey(key); err != nil {
		return nil, err
	}
	bytes, err := f.filesystem.ReadFile(f.getPathByKey(key))
	// 文件不存在时转换为统一的 ErrKeyNotFound，调用方无需关心底层错误类型。
	if os.IsNotExist(err) {
		return bytes, ErrKeyNotFound
	}
	return bytes, err
}

// Delete 删除 key 对应的文件。
func (f *FileStore) Delete(key string) error {
	if err := ValidateKey(key); err != nil {
		return err
	}
	return removePath(f.filesystem, f.getPathByKey(key))
}

// List 返回 store 中的所有键。
func (f *FileStore) List() ([]string, error) {
	keys := make([]string, 0)
	files, err := f.filesystem.ReadDir(f.directoryPath)
	if err != nil {
		return keys, err
	}
	for _, f := range files {
		// 过滤临时文件，避免把写入中的半成品暴露为有效键。
		if !strings.HasPrefix(f.Name(), tmpPrefix) {
			keys = append(keys, f.Name())
		}
	}
	return keys, nil
}

// getPathByKey 返回 key 对应的文件完整路径。
func (f *FileStore) getPathByKey(key string) string {
	return filepath.Join(f.directoryPath, key)
}

// writeFile 以单次事务方式把数据写入 path。
func writeFile(fs utilfs.Filesystem, path string, data []byte) (retErr error) {
	// 在目标目录创建临时文件，随后通过 rename 原子替换。
	tmpFile, err := fs.TempFile(filepath.Dir(path), tmpPrefix)
	if err != nil {
		return err
	}

	tmpPath := tmpFile.Name()
	shouldClose := true

	defer func() {
		// 未显式关闭时在这里兜底关闭，避免文件描述符泄漏。
		if shouldClose {
			if err := tmpFile.Close(); err != nil {
				if retErr == nil {
					retErr = fmt.Errorf("close error: %v", err)
				} else {
					retErr = fmt.Errorf("failed to close temp file after error %v; close error: %v", retErr, err)
				}
			}
		}

		// 出错时清理临时文件，避免留下垃圾。
		if retErr != nil && tmpPath != "" {
			if err := removePath(fs, tmpPath); err != nil {
				retErr = fmt.Errorf("failed to remove the temporary file (%q) after error %v; remove error: %v", tmpPath, retErr, err)
			}
		}
	}()

	// 写入数据。
	if _, err := tmpFile.Write(data); err != nil {
		return err
	}

	// 落盘后再 rename，保证崩溃时不会看到不完整内容。
	if err := tmpFile.Sync(); err != nil {
		return err
	}

	// 重命名前必须关闭文件，Windows 等平台不允许重命名打开中的文件。
	err = tmpFile.Close()
	shouldClose = false
	if err != nil {
		return err
	}

	return fs.Rename(tmpPath, path)
}

// removePath 删除文件，文件不存在时视为成功。
func removePath(fs utilfs.Filesystem, path string) error {
	if err := fs.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
