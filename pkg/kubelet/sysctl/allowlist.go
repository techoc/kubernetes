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

package sysctl

import (
	"context"
	"fmt"
	"strings"

	utilsysctl "k8s.io/component-helpers/node/util/sysctl"
	"k8s.io/kubernetes/pkg/apis/core/validation"
	policyvalidation "k8s.io/kubernetes/pkg/apis/policy/validation"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
)

const (
	// ForbiddenReason 是 sysctl 被准入拒绝时的原因标识。
	ForbiddenReason = "SysctlForbidden"
)

// patternAllowlist 保存 sysctl 完整名称与前缀到命名空间的映射，
// 用于拒绝那些未知命名空间的 sysctl 或 sysctl 模式（以 * 结尾）。
type patternAllowlist struct {
	sysctls  map[string]utilsysctl.Namespace
	prefixes map[string]utilsysctl.Namespace
}

// 编译期断言：patternAllowlist 实现了 PodAdmitHandler。
var _ lifecycle.PodAdmitHandler = &patternAllowlist{}

// NewAllowlist 根据 sysctl 列表及 sysctl 模式（以 * 结尾）创建允许列表。
func NewAllowlist(patterns []string) (*patternAllowlist, error) {
	w := &patternAllowlist{
		sysctls:  map[string]utilsysctl.Namespace{},
		prefixes: map[string]utilsysctl.Namespace{},
	}

	for _, s := range patterns {
		// 先做格式校验，避免把非法模式当作前缀处理。
		if !policyvalidation.IsValidSysctlPattern(s) {
			return nil, fmt.Errorf("sysctl %q must have at most %d characters and match regex %s",
				s,
				validation.SysctlMaxLength,
				policyvalidation.SysctlContainSlashPatternFmt,
			)
		}
		// 通过已知的 sysctl 前缀映射判断其所属命名空间。
		ns, sysctlOrPrefix, prefixed := utilsysctl.GetNamespace(s)
		if ns == utilsysctl.UnknownNamespace {
			return nil, fmt.Errorf("the sysctls %q are not known to be namespaced", sysctlOrPrefix)
		}
		// 以 * 结尾的模式按前缀存储，其余按完整名称存储。
		if prefixed {
			w.prefixes[sysctlOrPrefix] = ns
		} else {
			w.sysctls[sysctlOrPrefix] = ns
		}
	}
	return w, nil
}

// validateSysctl 检查 sysctl 是否在允许列表中（即已知被 Linux 内核命名空间隔离）。
// 在允许列表中只是必要条件：容器运行时可能有更严格的检查并拒绝启动 Pod。
//
// hostNet、hostIPC、hostUsers 用于禁止与宿主共享相应命名空间的 Pod 使用对应 sysctl。
// 该检查只适用于静态默认允许列表，不适用于管理员提供的自定义允许列表。
func (w *patternAllowlist) validateSysctl(sysctl string, hostNet, hostIPC, hostUsers bool) error {
	// 统一名称格式（例如把 / 转换为 .），保证匹配一致。
	sysctl = utilsysctl.NormalizeName(sysctl)
	nsErrorFmt := "%q not allowed with host %s enabled"
	// 先匹配完整名称，再回退到前缀匹配。
	if ns, found := w.sysctls[sysctl]; found {
		if ns == utilsysctl.IPCNamespace && hostIPC {
			return fmt.Errorf(nsErrorFmt, sysctl, ns)
		}
		if ns == utilsysctl.NetNamespace && hostNet {
			return fmt.Errorf(nsErrorFmt, sysctl, ns)
		}
		if ns == utilsysctl.UserNamespace && hostUsers {
			return fmt.Errorf(nsErrorFmt, sysctl, ns)
		}
		return nil
	}
	// 前缀匹配覆盖以 * 结尾的自定义 sysctl 模式。
	for p, ns := range w.prefixes {
		if strings.HasPrefix(sysctl, p) {
			if ns == utilsysctl.IPCNamespace && hostIPC {
				return fmt.Errorf(nsErrorFmt, sysctl, ns)
			}
			if ns == utilsysctl.NetNamespace && hostNet {
				return fmt.Errorf(nsErrorFmt, sysctl, ns)
			}
			if ns == utilsysctl.UserNamespace && hostUsers {
				return fmt.Errorf(nsErrorFmt, sysctl, ns)
			}
			return nil
		}
	}
	return fmt.Errorf("%q not allowlisted", sysctl)
}

// Admit 检查 Pod 安全上下文中的所有 sysctl 是否都符合允许列表。
func (w *patternAllowlist) Admit(_ context.Context, attrs *lifecycle.PodAdmitAttributes) lifecycle.PodAdmitResult {
	// 没有配置 sysctl 时直接准入。
	pod := attrs.Pod
	if pod.Spec.SecurityContext == nil || len(pod.Spec.SecurityContext.Sysctls) == 0 {
		return lifecycle.PodAdmitResult{
			Admit: true,
		}
	}

	// hostUsers 默认为 true，因此 nil 表示 Pod 与宿主共享用户命名空间。
	hostUsers := pod.Spec.HostUsers == nil || *pod.Spec.HostUsers
	// 任一 sysctl 不合法就拒绝整个 Pod。
	for _, s := range pod.Spec.SecurityContext.Sysctls {
		if err := w.validateSysctl(s.Name, pod.Spec.HostNetwork, pod.Spec.HostIPC, hostUsers); err != nil {
			return lifecycle.PodAdmitResult{
				Admit:   false,
				Reason:  ForbiddenReason,
				Message: fmt.Sprintf("forbidden sysctl: %v", err),
			}
		}
	}

	return lifecycle.PodAdmitResult{
		Admit: true,
	}
}
