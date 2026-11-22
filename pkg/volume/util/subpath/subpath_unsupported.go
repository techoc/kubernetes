//go:build !linux && !windows

/*
Copyright 2014 The Kubernetes Authors.

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

package subpath

import (
	"errors"
	"os"

	"k8s.io/mount-utils"
)

// subpath 是当前平台上 subpath 的空实现。
type subpath struct{}

var errUnsupported = errors.New("util/subpath on this platform is not supported")

// New 返回当前系统的 subpath.Interface 实现。
func New(mount.Interface) Interface {
	return &subpath{}
}

// PrepareSafeSubpath 为子路径做准备；本平台不支持，直接返回原路径和错误。
func (sp *subpath) PrepareSafeSubpath(subPath Subpath) (newHostPath string, cleanupAction func(), err error) {
	return subPath.Path, nil, errUnsupported
}

// CleanSubPaths 清理 pod 目录下某卷的子路径；本平台不支持。
func (sp *subpath) CleanSubPaths(podDir string, volumeName string) error {
	return errUnsupported
}

// SafeMakeDir 在 base 下安全地创建目录；本平台不支持。
func (sp *subpath) SafeMakeDir(pathname string, base string, perm os.FileMode) error {
	return errUnsupported
}
