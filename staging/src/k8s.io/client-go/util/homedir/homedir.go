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

// Package homedir 以跨平台方式定位当前用户的主目录。
// 主要用于找到默认的 ~/.kube/config。
package homedir

import (
	"os"
	"path/filepath"
	"runtime"
)

// HomeDir 返回当前用户的主目录。
// Windows 平台：
// 1. 返回 %HOME%、%HOMEDRIVE%%HOMEPATH%、%USERPROFILE% 中第一个含 `.kube\config` 的路径。
// 2. 都没有时，返回三者中第一个存在且可写的路径。
// 3. 都不可写时，返回第一个存在的路径。
// 都不存在时，返回第一个已设置的路径。
//
// 非 Windows 平台直接返回 $HOME（可能为空），由调用方决定是否报错。
func HomeDir() string {
	if runtime.GOOS == "windows" {
		home := os.Getenv("HOME")
		homeDriveHomePath := ""
		if homeDrive, homePath := os.Getenv("HOMEDRIVE"), os.Getenv("HOMEPATH"); len(homeDrive) > 0 && len(homePath) > 0 {
			homeDriveHomePath = homeDrive + homePath
		}
		userProfile := os.Getenv("USERPROFILE")

		// 返回 %HOME%、%HOMEDRIVE%/%HOMEPATH%、%USERPROFILE% 中第一个含 `.kube\config` 的路径。
		// 为向后兼容，优先 %HOMEDRIVE%/%HOMEPATH% 而非 %USERPROFILE%。
		for _, p := range []string{home, homeDriveHomePath, userProfile} {
			if len(p) == 0 {
				continue
			}
			if _, err := os.Stat(filepath.Join(p, ".kube", "config")); err != nil {
				continue
			}
			return p
		}

		firstSetPath := ""
		firstExistingPath := ""

		// 优先 %USERPROFILE% 而非 %HOMEDRIVE%/%HOMEPATH%，以兼容其他写入认证信息的工具。
		// 本轮的顺序与上面不同：优先 USERPROFILE，历史原因见注释。
		for _, p := range []string{home, userProfile, homeDriveHomePath} {
			if len(p) == 0 {
				continue
			}
			if len(firstSetPath) == 0 {
				// 记录第一个已设置的路径。
				firstSetPath = p
			}
			info, err := os.Stat(p)
			if err != nil {
				continue
			}
			if len(firstExistingPath) == 0 {
				// 记录第一个存在的路径。
				firstExistingPath = p
			}
			// 权限位 0700 中的 owner-write 位；目录可写才能安全落盘 kubeconfig。
			if info.IsDir() && info.Mode().Perm()&(1<<(uint(7))) != 0 {
				// 返回第一个可写的路径。
				return p
			}
		}

		// 都不可写时，返回第一个存在的路径。
		if len(firstExistingPath) > 0 {
			return firstExistingPath
		}

		// 都不存在时，返回第一个已设置的路径。
		if len(firstSetPath) > 0 {
			return firstSetPath
		}

		// 无可用路径。
		return ""
	}
	return os.Getenv("HOME")
}
