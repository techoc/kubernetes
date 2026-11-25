//go:build windows

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

package preflight

import (
	"golang.org/x/sys/windows"

	utilsexec "k8s.io/utils/exec"

	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

// Check 校验用户是否具有提升后的（管理员）权限。
func (ipuc IsPrivilegedUserCheck) Check() (warnings, errorList []error) {
	hProcessToken := windows.GetCurrentProcessToken()
	if hProcessToken.IsElevated() {
		return nil, nil
	}
	return nil, []error{errors.New("the kubeadm process must be run by a user with elevated privileges")}
}

// Check 检查 kubeadm 所需的内存数量。
// 在 Windows 上为空操作。
func (mc MemCheck) Check() (warnings, errorList []error) {
	return nil, nil
}

// addExecChecks 添加检查，验证某些二进制文件是否存在于 PATH 中。
func addExecChecks(checks []Checker, execer utilsexec.Interface, _ string) []Checker {
	// kubeadm 要求 xcopy 存在于 PATH 中，以复制 etcd 目录。
	checks = append(checks, InPathCheck{executable: "xcopy", mandatory: true, exec: execer})
	return checks
}
