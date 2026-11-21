//go:build freebsd || linux || darwin

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

package util

import (
	"net/url"
	"path/filepath"
)

const (
	// unixProtocol 是 unix socket 的网络协议名。
	unixProtocol = "unix"
)

// LocalEndpoint 返回指定端点下 unix socket 的完整路径。
func LocalEndpoint(path, file string) (string, error) {
	u := url.URL{
		Scheme: unixProtocol,
		Path:   path,
	}
	return filepath.Join(u.String(), file+".sock"), nil
}

// NormalizePath 在 Linux 上暂时是空操作，原样返回路径。
func NormalizePath(path string) string {
	return path
}
