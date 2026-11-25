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

// Package files 包含管理文件的工具。
package files

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"k8s.io/klog/v2"

	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

// CopyFile 将文件从 src 复制到 dest。
func CopyFile(src, dest string) error {
	sourceFileInfo, err := os.Stat(src)
	if err != nil {
		return err
	}

	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() {
		_ = sourceFile.Close()
	}()

	destFile, err := os.OpenFile(dest, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, sourceFileInfo.Mode())
	if err != nil {
		return err
	}
	defer func() {
		_ = destFile.Close()
	}()

	_, err = io.Copy(destFile, sourceFile)

	return err
}

// CopyDir 将目录内容复制到 dest，必要时创建该目录。
func CopyDir(src, dest string) error {
	return filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dest, rel)
		if !d.IsDir() {
			return CopyFile(path, target)
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		return os.MkdirAll(target, info.Mode().Perm())
	})
}

// MoveFile 将文件从 src 移动到 dest。
func MoveFile(src, dest string) error {
	err := os.Rename(src, dest)
	if err != nil && strings.Contains(err.Error(), "invalid cross-device link") {
		// 源文件和目标文件位于不同文件系统时，os.Rename() 可能返回 "invalid cross-device link"。
		// 此时先复制再删除源文件，效率低于 os.Rename()。
		klog.V(4).Infof("cannot rename %v to %v due to %v, attempting an alternative method", src, dest, err)
		if err := CopyFile(src, dest); err != nil {
			return errors.Wrapf(err, "failed to copy file %v to %v", src, dest)
		}
		return os.Remove(src)
	}
	return err
}
