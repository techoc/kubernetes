//go:build freebsd || linux || darwin

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

package filesystem

import (
	"fmt"
	"os"
	"path/filepath"
)

// IsUnixDomainSocket 判断给定文件是否为 AF_UNIX 套接字文件。
func IsUnixDomainSocket(filePath string) (bool, error) {
	fi, err := os.Stat(filePath)
	if err != nil {
		return false, fmt.Errorf("stat file %s failed: %v", filePath, err)
	}
	if fi.Mode()&os.ModeSocket == 0 {
		return false, nil
	}
	return true, nil
}

// Chmod 在 Unix 上等同于 os.Chmod。
func Chmod(name string, mode os.FileMode) error {
	return os.Chmod(name, mode)
}

// MkdirAll 在 Unix 上等同于 os.MkdirAll。
func MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

// IsAbs 在 Unix 上等同于 filepath.IsAbs。
func IsAbs(path string) bool {
	return filepath.IsAbs(path)
}
