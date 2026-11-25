//go:build linux

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

package preflight

import (
	"syscall"

	utilversion "k8s.io/apimachinery/pkg/util/version"
	system "k8s.io/system-validators/validators"
	utilsexec "k8s.io/utils/exec"

	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

// Check 检查 kubeadm 所需的内存数量。
func (mc MemCheck) Check() (warnings, errorList []error) {
	info := syscall.Sysinfo_t{}
	err := syscall.Sysinfo(&info)
	if err != nil {
		errorList = append(errorList, errors.Wrapf(err, "failed to get system info"))
	}

	// Totalram 保存可用内存总量，Unit 保存内存单元的字节数；两者相乘后转换为 MB。
	actual := uint64(info.Totalram) * uint64(info.Unit) / 1024 / 1024
	if actual < mc.Mem {
		errorList = append(errorList, errors.Errorf("the system RAM (%d MB) is less than the minimum %d MB", actual, mc.Mem))
	}
	return warnings, errorList
}

// addOSValidator 添加一个新的 OSValidator。
func addOSValidator(validators []system.Validator, reporter *system.StreamReporter, kubeletVersion string) []system.Validator {
	validators = append(validators,
		&system.OSValidator{Reporter: reporter},
		&system.CgroupsValidator{Reporter: reporter, KubeletVersion: kubeletVersion},
	)
	return validators
}

// addIPv6Checks 添加 IPv6 相关检查。
func addIPv6Checks(checks []Checker) []Checker {
	checks = append(checks,
		FileContentCheck{Path: ipv6DefaultForwarding, Content: []byte{'1'}},
	)
	return checks
}

// addIPv4Checks 添加 IPv4 相关检查。
func addIPv4Checks(checks []Checker) []Checker {
	checks = append(checks,
		FileContentCheck{Path: ipv4Forward, Content: []byte{'1'}})
	return checks
}

// addSwapCheck 添加 swap 检查。
func addSwapCheck(checks []Checker) []Checker {
	checks = append(checks, SwapCheck{})
	return checks
}

// addExecChecks 添加检查，验证某些二进制文件是否存在于 PATH 中。
func addExecChecks(checks []Checker, execer utilsexec.Interface, k8sVersion string) []Checker {
	// 从 k8s >= 1.32.0 起，kube-proxy 不再依赖 conntrack 存在于 PATH 中。
	// (ref: https://github.com/kubernetes/kubernetes/pull/126952)
	if v, err := utilversion.ParseSemantic(k8sVersion); err == nil {
		if v.LessThan(utilversion.MustParseSemantic("1.32.0")) {
			checks = append(checks, InPathCheck{executable: "conntrack", mandatory: true, exec: execer})
		}
	}

	// 从 1.9.0 起，kubelet 要求 losetup 存在于 PATH 中以支持块卷。
	// (ref: https://github.com/kubernetes/kubernetes/pull/51494)
	checks = append(checks, InPathCheck{executable: "losetup", mandatory: true, exec: execer})

	// kubelet 要求 mount 存在于 PATH 中以支持树内卷插件。
	checks = append(checks, InPathCheck{executable: "mount", mandatory: true, exec: execer})

	// kubeadm 要求 cp 存在于 PATH 中以复制 etcd 目录。
	checks = append(checks, InPathCheck{executable: "cp", mandatory: true, exec: execer})
	return checks
}
