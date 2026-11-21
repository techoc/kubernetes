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

package kuberuntime

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	runtimeutil "k8s.io/kubernetes/pkg/kubelet/kuberuntime/util"
	"k8s.io/kubernetes/pkg/securitycontext"
)

// determineEffectiveSecurityContext 由 Pod 与 Container 推导出容器最终生效的安全上下文。
func (m *kubeGenericRuntimeManager) determineEffectiveSecurityContext(ctx context.Context, pod *v1.Pod, container *v1.Container, uid *int64, username string) (*runtimeapi.LinuxContainerSecurityContext, error) {
	// 先合并 Pod 与容器的安全上下文，容器级配置优先。
	effectiveSc := securitycontext.DetermineEffectiveSecurityContext(pod, container)
	synthesized := convertToRuntimeSecurityContext(effectiveSc)
	// 没有可转换的字段时，仍需设置 ProcMount 相关的屏蔽路径与只读路径。
	if synthesized == nil {
		synthesized = &runtimeapi.LinuxContainerSecurityContext{
			MaskedPaths:   securitycontext.ConvertToRuntimeMaskedPaths(effectiveSc.ProcMount),
			ReadonlyPaths: securitycontext.ConvertToRuntimeReadonlyPaths(effectiveSc.ProcMount),
		}
	}
	var err error

	// 合并 Pod 注解、Pod 级与容器级配置，并应用 kubelet 默认 seccomp。
	synthesized.Seccomp, err = m.getSeccompProfile(pod.Annotations, container.Name, pod.Spec.SecurityContext, container.SecurityContext, m.seccompDefault)
	if err != nil {
		return nil, err
	}

	// 设置 AppArmor 配置。
	synthesized.Apparmor, synthesized.ApparmorProfile, err = getAppArmorProfile(pod, container)
	if err != nil {
		return nil, err
	}

	// 设置运行用户；显式 UID 优先于用户名。
	if synthesized.RunAsUser == nil {
		if uid != nil {
			synthesized.RunAsUser = &runtimeapi.Int64Value{Value: *uid}
		}
		synthesized.RunAsUsername = username
	}

	// 设置命名空间选项与补充组。
	namespaceOptions, err := runtimeutil.NamespacesForPod(ctx, pod, m.runtimeHelper, m.runtimeClassManager)
	if err != nil {
		return nil, err
	}
	synthesized.NamespaceOptions = namespaceOptions
	podSc := pod.Spec.SecurityContext
	// FSGroup 与补充组最终都并入 SupplementalGroups，供运行时设置进程组。
	if podSc != nil {
		if podSc.FSGroup != nil {
			synthesized.SupplementalGroups = append(synthesized.SupplementalGroups, int64(*podSc.FSGroup))
		}

		if podSc.SupplementalGroups != nil {
			for _, sg := range podSc.SupplementalGroups {
				synthesized.SupplementalGroups = append(synthesized.SupplementalGroups, int64(sg))
			}
		}

		if podSc.SupplementalGroupsPolicy != nil {
			policyValue, ok := runtimeapi.SupplementalGroupsPolicy_value[string(*podSc.SupplementalGroupsPolicy)]
			if !ok {
				return nil, fmt.Errorf("unsupported supplementalGroupsPolicy: %s", string(*podSc.SupplementalGroupsPolicy))
			}
			synthesized.SupplementalGroupsPolicy = runtimeapi.SupplementalGroupsPolicy(policyValue)
		}
	}
	// 追加运行时插件（如设备插件）要求的额外补充组。
	if groups := m.runtimeHelper.GetExtraSupplementalGroupsForPod(pod); len(groups) > 0 {
		synthesized.SupplementalGroups = append(synthesized.SupplementalGroups, groups...)
	}

	// 根据 allowPrivilegeEscalation 推导 no_new_privs。
	synthesized.NoNewPrivs = securitycontext.AddNoNewPrivileges(effectiveSc)

	synthesized.MaskedPaths = securitycontext.ConvertToRuntimeMaskedPaths(effectiveSc.ProcMount)
	synthesized.ReadonlyPaths = securitycontext.ConvertToRuntimeReadonlyPaths(effectiveSc.ProcMount)

	return synthesized, nil
}

// convertToRuntimeSecurityContext 把 v1.SecurityContext 转换为 runtimeapi.SecurityContext。
func convertToRuntimeSecurityContext(securityContext *v1.SecurityContext) *runtimeapi.LinuxContainerSecurityContext {
	// nil 安全上下文转换为 nil，交由上层补充默认值。
	if securityContext == nil {
		return nil
	}

	sc := &runtimeapi.LinuxContainerSecurityContext{
		Capabilities:   convertToRuntimeCapabilities(securityContext.Capabilities),
		SelinuxOptions: convertToRuntimeSELinuxOption(securityContext.SELinuxOptions),
	}
	if securityContext.RunAsUser != nil {
		sc.RunAsUser = &runtimeapi.Int64Value{Value: int64(*securityContext.RunAsUser)}
	}
	if securityContext.RunAsGroup != nil {
		sc.RunAsGroup = &runtimeapi.Int64Value{Value: int64(*securityContext.RunAsGroup)}
	}
	if securityContext.Privileged != nil {
		sc.Privileged = *securityContext.Privileged
	}
	if securityContext.ReadOnlyRootFilesystem != nil {
		sc.ReadonlyRootfs = *securityContext.ReadOnlyRootFilesystem
	}

	return sc
}

// convertToRuntimeSELinuxOption 把 v1.SELinuxOptions 转换为 runtimeapi.SELinuxOption。
func convertToRuntimeSELinuxOption(opts *v1.SELinuxOptions) *runtimeapi.SELinuxOption {
	// nil 输入保持 nil，避免伪造空的 SELinux 配置。
	if opts == nil {
		return nil
	}

	return &runtimeapi.SELinuxOption{
		User:  opts.User,
		Role:  opts.Role,
		Type:  opts.Type,
		Level: opts.Level,
	}
}

// convertToRuntimeCapabilities 把 v1.Capabilities 转换为 runtimeapi.Capability。
func convertToRuntimeCapabilities(opts *v1.Capabilities) *runtimeapi.Capability {
	if opts == nil {
		return nil
	}

	// 预分配目标切片长度，避免逐个 append 扩容。
	capabilities := &runtimeapi.Capability{
		AddCapabilities:  make([]string, len(opts.Add)),
		DropCapabilities: make([]string, len(opts.Drop)),
	}
	// 保留 add/drop 的原始顺序，便于调试与对比。
	for index, value := range opts.Add {
		capabilities.AddCapabilities[index] = string(value)
	}
	for index, value := range opts.Drop {
		capabilities.DropCapabilities[index] = string(value)
	}

	return capabilities
}
