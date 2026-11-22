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

package removeall

import (
	"fmt"
	"io"
	"os"
	"syscall"

	"k8s.io/mount-utils"
)

// RemoveAllOneFilesystemCommon 使用传入的 remove 函数删除 path 及其所有子内容。
// 它会尽可能删除所有能删的内容，但只返回遇到的第一个错误。
// path 不存在时返回 nil（无错误）。
// 它会确保不跨越挂载边界，即*不会*删除其他文件系统上的文件，
// 类似 'rm -rf --one-file-system'。
// 实现拷贝自 RemoveAll()，额外加上了 IsLikelyNotMountPoint 检查。
func RemoveAllOneFilesystemCommon(mounter mount.Interface, path string, remove func(string) error) error {
	// 简单情况：直接删除成功就结束。
	err := remove(path)
	if err == nil || os.IsNotExist(err) {
		return nil
	}

	// 否则，判断这是否是我们需要递归处理的目录。
	dir, serr := os.Lstat(path)
	if serr != nil {
		if serr, ok := serr.(*os.PathError); ok && (os.IsNotExist(serr.Err) || serr.Err == syscall.ENOTDIR) {
			return nil
		}
		return serr
	}
	if !dir.IsDir() {
		// 不是目录，直接返回 remove 的错误。
		return err
	}

	// 是目录。先确认它不是挂载点，避免误删其他文件系统的内容。
	isNotMount, err := mounter.IsLikelyNotMountPoint(path)
	if err != nil {
		return err
	}
	if !isNotMount {
		return fmt.Errorf("cannot delete directory %s: it is a mount point", path)
	}

	fd, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			// 竞态：目录在 Lstat 与 Open 之间被删除了。
			// 按 RemoveAll 的语义返回 nil。
			return nil
		}
		return err
	}

	// 删除目录内容，只保留第一个错误。
	err = nil
	for {
		names, err1 := fd.Readdirnames(100)
		for _, name := range names {
			err1 := RemoveAllOneFilesystemCommon(mounter, path+string(os.PathSeparator)+name, remove)
			if err == nil {
				err = err1
			}
		}
		if err1 == io.EOF {
			break
		}
		// 如果 Readdirnames 返回了错误，记录它。
		if err == nil {
			err = err1
		}
		if len(names) == 0 {
			break
		}
	}

	// 先关闭目录，因为 Windows 上无法删除处于打开状态的目录。
	fd.Close()

	// 删除目录本身。
	err1 := remove(path)
	if err1 == nil || os.IsNotExist(err1) {
		return nil
	}
	if err == nil {
		err = err1
	}
	return err
}

// RemoveAllOneFilesystem 使用 os.Remove 删除 path 及其所有子内容。
// 它会确保不跨越挂载边界，即遇到其他文件系统的内容时返回错误而不是删除。
// 它会尽可能删除所有能删的内容，但只返回遇到的第一个错误。
// path 不存在时返回 nil（无错误）。
func RemoveAllOneFilesystem(mounter mount.Interface, path string) error {
	return RemoveAllOneFilesystemCommon(mounter, path, os.Remove)
}

// RemoveDirsOneFilesystem 使用 syscall.Rmdir 删除 path 及其包含的空子目录。
// 与 RemoveAllOneFilesystem 不同，它只删除目录；如果目录树中遇到任何文件就返回错误。
// 它会确保不跨越挂载边界，即遇到其他文件系统的目录时返回错误而不是删除。
// 它会尽可能删除所有能删的内容，但只返回遇到的第一个错误。
// path 不存在时返回 nil（无错误）。
func RemoveDirsOneFilesystem(mounter mount.Interface, path string) error {
	return RemoveAllOneFilesystemCommon(mounter, path, syscall.Rmdir)
}
