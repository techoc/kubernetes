//go:build windows

/*
Copyright 2019 The Kubernetes Authors.

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

package mounttest

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

func umask(mask int) int {
	// 在 Windows 上为空操作。
	return 0
}

func fileOwner(path string) error {
	// Windows 没有所有者 UID / GID，但有所有者 SID。
	// Kubernetes 目前尚未实现该功能，因此为空操作。
	return nil
}

func fileMode(path string) error {
	if path == "" {
		return nil
	}

	permissions, err := getFilePerm(path)
	if err != nil {
		return err
	}

	fmt.Printf("mode of Windows file %q: %v\n", path, permissions)
	return nil
}

func filePerm(path string) error {
	if path == "" {
		return nil
	}

	permissions, err := getFilePerm(path)
	if err != nil {
		return err
	}

	fmt.Printf("perms of Windows file %q: %v\n", path, permissions)
	return nil
}

func getFilePerm(path string) (os.FileMode, error) {
	var (
		out    bytes.Buffer
		errOut bytes.Buffer
	)

	// NOTE(claudiub): 符号链接的权限可能与目标文件的权限不同。
	// 我们要检查的是目标文件的权限，而不是符号链接本身的权限。
	info, err := os.Lstat(path)
	if err == nil {
		// go1.23 行为变更：https://github.com/golang/go/issues/63703#issuecomment-2535941458
		if info.Mode()&os.ModeSymlink != 0 || info.Mode()&os.ModeIrregular != 0 {
			evaluated, err := filepath.EvalSymlinks(path)
			if err != nil {
				return 0, err
			}
			path = evaluated
		}
	}

	cmd := exec.Command("powershell.exe", "-NonInteractive", "./filePermissions.ps1",
		"-FileName", path)
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	err = cmd.Run()

	if err != nil {
		fmt.Printf("error from PowerShell Script: %v, %v\n", err, errOut.String())
		return 0, err
	}

	output := strings.TrimSpace(out.String())
	val, err := strconv.ParseInt(output, 8, 32)
	if err != nil {
		fmt.Printf("error parsing string '%s' as int: %v\n", output, err)
		return 0, err
	}

	return os.FileMode(val), nil
}

func fsType(path string) error {
	// 目前仅支持 NTFS。
	return nil
}
