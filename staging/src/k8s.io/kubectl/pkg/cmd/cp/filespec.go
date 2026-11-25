/*
Copyright 2021 The Kubernetes Authors.

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

package cp

import (
	"path"
	"path/filepath"
	"strings"
)

type fileSpec struct {
	PodName      string
	PodNamespace string
	File         pathSpec
}

type pathSpec interface {
	String() string
}

// localPath 表示客户端本地路径，其形式因
// 客户端操作系统而异；其方法使用 path/filepath 包，
// 该包依赖于操作系统
type localPath struct {
	file string
}

func newLocalPath(fileName string) localPath {
	file := stripTrailingSlash(fileName)
	return localPath{file: file}
}

func (p localPath) String() string {
	return p.file
}

func (p localPath) Dir() localPath {
	return newLocalPath(filepath.Dir(p.file))
}

func (p localPath) Base() localPath {
	return newLocalPath(filepath.Base(p.file))
}

func (p localPath) Clean() localPath {
	return newLocalPath(filepath.Clean(p.file))
}

func (p localPath) Join(elem pathSpec) localPath {
	return newLocalPath(filepath.Join(p.file, elem.String()))
}

func (p localPath) Glob() (matches []string, err error) {
	return filepath.Glob(p.file)
}

func (p localPath) StripSlashes() localPath {
	return newLocalPath(stripLeadingSlash(p.file))
}

func isRelative(base, target localPath) bool {
	baseNormalized := strings.ReplaceAll(stripTrailingSlash(base.String()), `\`, "/")
	targetNormalized := strings.ReplaceAll(stripTrailingSlash(target.String()), `\`, "/")
	relative, err := filepath.Rel(baseNormalized, targetNormalized)
	if err != nil {
		return false
	}
	return relative == "." || relative == stripPathShortcuts(relative)
}

// remotePath 始终表示 UNIX 路径，其方法使用 path
// 包，该包始终使用 `/`
type remotePath struct {
	file string
}

func newRemotePath(fileName string) remotePath {
	// 我们假设远端文件位于 Linux 容器中，但仍需将
	// Windows 路径分隔符转换为 Unix 风格，以便统一处理
	file := strings.ReplaceAll(stripTrailingSlash(fileName), `\`, "/")
	return remotePath{file: file}
}

func (p remotePath) String() string {
	return p.file
}

func (p remotePath) Dir() remotePath {
	return newRemotePath(path.Dir(p.file))
}

func (p remotePath) Base() remotePath {
	return newRemotePath(path.Base(p.file))
}

func (p remotePath) Clean() remotePath {
	return newRemotePath(path.Clean(p.file))
}

func (p remotePath) Join(elem pathSpec) remotePath {
	return newRemotePath(path.Join(p.file, elem.String()))
}

func (p remotePath) StripShortcuts() remotePath {
	p = p.Clean()
	return newRemotePath(stripPathShortcuts(p.file))
}

func (p remotePath) StripSlashes() remotePath {
	return newRemotePath(stripLeadingSlash(p.file))
}

// 去除 Unix 和 Windows 风格的末尾斜杠（如果有）
func stripTrailingSlash(file string) string {
	if len(file) == 0 {
		return file
	}
	if file != "/" && strings.HasSuffix(string(file[len(file)-1]), "/") {
		return file[:len(file)-1]
	}
	return file
}

func stripLeadingSlash(file string) string {
	// tar 会去除开头的 '/' 和 '\'（如果存在），我们也这样做
	return strings.TrimLeft(file, `/\`)
}

// stripPathShortcuts 移除给定路径开头或结尾的所有 "../"
func stripPathShortcuts(p string) string {
	newPath := p
	trimmedPosix := strings.TrimPrefix(newPath, "../")

	for trimmedPosix != newPath {
		newPath = trimmedPosix
		trimmedPosix = strings.TrimPrefix(newPath, "../")
	}

	trimmedWindows := strings.TrimPrefix(newPath, `..\`)

	for trimmedWindows != newPath {
		newPath = trimmedWindows
		trimmedWindows = strings.TrimPrefix(newPath, `..\`)
	}

	// 修剪剩余的 {"."、".."}
	if newPath == "." || newPath == ".." {
		newPath = ""
	}

	if len(newPath) > 0 && (string(newPath[0]) == "/" || string(newPath[0]) == `\`) {
		return newPath[1:]
	}

	return newPath
}
