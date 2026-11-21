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
	"net/url"
	"runtime"
	"sort"
	"strings"

	v1 "k8s.io/api/core/v1"
	kubetypes "k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	utilsysctl "k8s.io/component-helpers/node/util/sysctl"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	runtimeutil "k8s.io/kubernetes/pkg/kubelet/kuberuntime/util"
	"k8s.io/kubernetes/pkg/kubelet/util"
	"k8s.io/kubernetes/pkg/kubelet/util/format"
	netutils "k8s.io/utils/net"
)

// createPodSandbox 创建 Pod 沙箱，返回沙箱 ID、可读消息与错误。
func (m *kubeGenericRuntimeManager) createPodSandbox(ctx context.Context, pod *v1.Pod, attempt uint32) (string, string, error) {
	logger := klog.FromContext(ctx)
	podSandboxConfig, err := m.generatePodSandboxConfig(ctx, pod, attempt)
	if err != nil {
		message := fmt.Sprintf("Failed to generate sandbox config for pod %q: %v", format.Pod(pod), err)
		logger.Error(err, "Failed to generate sandbox config for pod", "pod", klog.KObj(pod))
		return "", message, err
	}

	// 提前创建 Pod 日志目录，容器运行时随后可直接写入日志。
	err = m.osInterface.MkdirAll(podSandboxConfig.LogDirectory, 0755)
	if err != nil {
		message := fmt.Sprintf("Failed to create log directory for pod %q: %v", format.Pod(pod), err)
		logger.Error(err, "Failed to create log directory for pod", "pod", klog.KObj(pod))
		return "", message, err
	}

	// 根据 RuntimeClass 解析出运行时处理器；未配置 RuntimeClass 时使用默认处理器。
	runtimeHandler := ""
	if m.runtimeClassManager != nil {
		runtimeHandler, err = m.runtimeClassManager.LookupRuntimeHandler(pod.Spec.RuntimeClassName)
		if err != nil {
			message := fmt.Sprintf("Failed to create sandbox for pod %q: %v", format.Pod(pod), err)
			return "", message, err
		}
		if runtimeHandler != "" {
			logger.V(2).Info("Running pod with runtime handler", "pod", klog.KObj(pod), "runtimeHandler", runtimeHandler)
		}
	}

	// 由运行时服务实际创建沙箱。
	podSandBoxID, err := m.runtimeService.RunPodSandbox(ctx, podSandboxConfig, runtimeHandler)
	if err != nil {
		message := fmt.Sprintf("Failed to create sandbox for pod %q: %v", format.Pod(pod), err)
		logger.Error(err, "Failed to create sandbox for pod", "pod", klog.KObj(pod))
		return "", message, err
	}

	return podSandBoxID, "", nil
}

// generatePodSandboxConfig 根据 v1.Pod 生成 Pod 沙箱配置。
func (m *kubeGenericRuntimeManager) generatePodSandboxConfig(ctx context.Context, pod *v1.Pod, attempt uint32) (*runtimeapi.PodSandboxConfig, error) {
	// TODO: 逐步弃用 podsandbox 级别的资源需求，改用 Pod 级 cgroup。
	// 参考 https://github.com/kubernetes/kubernetes/issues/29871
	logger := klog.FromContext(ctx)
	podUID := string(pod.UID)
	podSandboxConfig := &runtimeapi.PodSandboxConfig{
		Metadata: &runtimeapi.PodSandboxMetadata{
			Name:      pod.Name,
			Namespace: pod.Namespace,
			Uid:       podUID,
			Attempt:   attempt,
		},
		Labels:      newPodLabels(pod),
		Annotations: newPodAnnotations(pod),
	}

	dnsConfig, err := m.runtimeHelper.GetPodDNS(ctx, pod)
	if err != nil {
		return nil, err
	}
	podSandboxConfig.DnsConfig = dnsConfig

	// 使用主机网络的 Pod 不需要单独设置主机名。
	if !kubecontainer.IsHostNetworkPod(pod) {
		// TODO: 在新的运行时接口中加入域名支持。
		podHostname, podDomain, err := m.runtimeHelper.GeneratePodHostNameAndDomain(logger, pod)
		if err != nil {
			return nil, err
		}
		podHostname, err = util.GetNodenameForKernel(podHostname, podDomain, pod.Spec.SetHostnameAsFQDN)
		if err != nil {
			return nil, err
		}
		podSandboxConfig.Hostname = podHostname
	}

	logDir := BuildPodLogsDirectory(m.podLogsDirectory, pod.Namespace, pod.Name, pod.UID)
	podSandboxConfig.LogDirectory = logDir

	// 汇总所有容器的端口映射，交给沙箱网络统一配置。
	portMappings := []*runtimeapi.PortMapping{}
	for _, c := range pod.Spec.Containers {
		containerPortMappings := kubecontainer.MakePortMappings(logger, &c)

		for idx := range containerPortMappings {
			port := containerPortMappings[idx]
			hostPort := int32(port.HostPort)
			containerPort := int32(port.ContainerPort)
			protocol := toRuntimeProtocol(logger, port.Protocol)
			portMappings = append(portMappings, &runtimeapi.PortMapping{
				HostIp:        port.HostIP,
				HostPort:      hostPort,
				ContainerPort: containerPort,
				Protocol:      protocol,
			})
		}

	}
	if len(portMappings) > 0 {
		podSandboxConfig.PortMappings = portMappings
	}

	lc, err := m.generatePodSandboxLinuxConfig(ctx, pod)
	if err != nil {
		return nil, err
	}
	podSandboxConfig.Linux = lc

	if runtime.GOOS == "windows" {
		wc, err := m.generatePodSandboxWindowsConfig(pod)
		if err != nil {
			return nil, err
		}
		podSandboxConfig.Windows = wc
	}

	// 把 Pod 开销与沙箱级资源写入配置。
	if err := m.applySandboxResources(ctx, pod, podSandboxConfig); err != nil {
		return nil, err
	}
	return podSandboxConfig, nil
}

// generatePodSandboxLinuxConfig 根据 v1.Pod 生成 LinuxPodSandboxConfig。
// 无论底层操作系统是什么都必须调用该函数：securityContext 并不在
// podSandboxConfig 中，而是位于 LinuxPodSandboxConfig 里。
// 将来若 securityContext 被提升到 podSandboxConfig，便可按需调用。
func (m *kubeGenericRuntimeManager) generatePodSandboxLinuxConfig(ctx context.Context, pod *v1.Pod) (*runtimeapi.LinuxPodSandboxConfig, error) {
	cgroupParent := m.runtimeHelper.GetPodCgroupParent(pod)
	lc := &runtimeapi.LinuxPodSandboxConfig{
		CgroupParent: cgroupParent,
		SecurityContext: &runtimeapi.LinuxSandboxSecurityContext{
			Privileged: kubecontainer.HasPrivilegedContainer(pod),

			// 强制沙箱以 `runtime/default` 运行，使用户能在 Pod 级别
			// 使用最小权限的 seccomp 配置。参见 Issue #84623。
			Seccomp: &runtimeapi.SecurityProfile{
				ProfileType: runtimeapi.SecurityProfile_RuntimeDefault,
			},
		},
	}

	// 先应用默认 sysctl，再由 Pod 显式声明的 sysctl 覆盖。
	sysctls := make(map[string]string)
	if utilfeature.DefaultFeatureGate.Enabled(features.DefaultPodSysctls) {
		applyPodSysctls(sysctls, m.defaultPodSysctls, pod)
	}
	if pod.Spec.SecurityContext != nil {
		for _, c := range pod.Spec.SecurityContext.Sysctls {
			sysctls[c.Name] = c.Value
		}
	}

	lc.Sysctls = sysctls

	if pod.Spec.SecurityContext != nil {
		sc := pod.Spec.SecurityContext
		if sc.RunAsUser != nil && runtime.GOOS != "windows" {
			lc.SecurityContext.RunAsUser = &runtimeapi.Int64Value{Value: int64(*sc.RunAsUser)}
		}
		if sc.RunAsGroup != nil && runtime.GOOS != "windows" {
			lc.SecurityContext.RunAsGroup = &runtimeapi.Int64Value{Value: int64(*sc.RunAsGroup)}
		}
		namespaceOptions, err := runtimeutil.NamespacesForPod(ctx, pod, m.runtimeHelper, m.runtimeClassManager)
		if err != nil {
			return nil, err
		}
		lc.SecurityContext.NamespaceOptions = namespaceOptions

		if sc.FSGroup != nil && runtime.GOOS != "windows" {
			lc.SecurityContext.SupplementalGroups = append(lc.SecurityContext.SupplementalGroups, int64(*sc.FSGroup))
		}
		if groups := m.runtimeHelper.GetExtraSupplementalGroupsForPod(pod); len(groups) > 0 {
			lc.SecurityContext.SupplementalGroups = append(lc.SecurityContext.SupplementalGroups, groups...)
		}
		if sc.SupplementalGroups != nil {
			for _, sg := range sc.SupplementalGroups {
				lc.SecurityContext.SupplementalGroups = append(lc.SecurityContext.SupplementalGroups, int64(sg))
			}
		}
		if sc.SupplementalGroupsPolicy != nil {
			policyValue, ok := runtimeapi.SupplementalGroupsPolicy_value[string(*sc.SupplementalGroupsPolicy)]
			if !ok {
				return nil, fmt.Errorf("unsupported supplementalGroupsPolicy: %s", string(*sc.SupplementalGroupsPolicy))
			}
			lc.SecurityContext.SupplementalGroupsPolicy = runtimeapi.SupplementalGroupsPolicy(policyValue)
		}

		if sc.SELinuxOptions != nil && runtime.GOOS != "windows" {
			lc.SecurityContext.SelinuxOptions = &runtimeapi.SELinuxOption{
				User:  sc.SELinuxOptions.User,
				Role:  sc.SELinuxOptions.Role,
				Type:  sc.SELinuxOptions.Type,
				Level: sc.SELinuxOptions.Level,
			}
		}
	}

	return lc, nil
}

// generatePodSandboxWindowsConfig 根据 v1.Pod 生成 WindowsPodSandboxConfig。
// 在 Windows 上除 LinuxPodSandboxConfig 外还会调用该函数，
// 因为目前并非所有相关字段都已加入 WindowsPodSandboxConfig。
func (m *kubeGenericRuntimeManager) generatePodSandboxWindowsConfig(pod *v1.Pod) (*runtimeapi.WindowsPodSandboxConfig, error) {
	wc := &runtimeapi.WindowsPodSandboxConfig{
		SecurityContext: &runtimeapi.WindowsSandboxSecurityContext{},
	}

	// 若 Pod 中包含 HostProcess 容器，必须在创建沙箱时显式设置 HostProcess 字段，
	// 因为容器运行时需要在此时获得该信息。
	if kubecontainer.HasWindowsHostProcessContainer(pod) {
		// 目前 Windows Pod 中的所有容器必须同为 HostProcess 容器，
		// 且必须启用 HostNetwork。
		if !kubecontainer.AllContainersAreWindowsHostProcess(pod) {
			return nil, fmt.Errorf("pod must not contain both HostProcess and non-HostProcess containers")
		}

		if !kubecontainer.IsHostNetworkPod(pod) {
			return nil, fmt.Errorf("hostNetwork is required if Pod contains HostProcess containers")
		}

		wc.SecurityContext.HostProcess = true
	}

	sc := pod.Spec.SecurityContext
	if sc == nil || sc.WindowsOptions == nil {
		return wc, nil
	}

	wo := sc.WindowsOptions
	if wo.GMSACredentialSpec != nil {
		wc.SecurityContext.CredentialSpec = *wo.GMSACredentialSpec
	}

	if wo.RunAsUserName != nil {
		wc.SecurityContext.RunAsUsername = *wo.RunAsUserName
	}

	if kubecontainer.HasWindowsHostProcessContainer(pod) {

		if wo.HostProcess != nil && !*wo.HostProcess {
			return nil, fmt.Errorf("pod must not contain any HostProcess containers if Pod's WindowsOptions.HostProcess is set to false")
		}
	}

	return wc, nil
}

// determinePodSandboxIPs 解析给定 Pod 沙箱的 IP 地址列表。
func (m *kubeGenericRuntimeManager) determinePodSandboxIPs(ctx context.Context, podNamespace, podName string, podSandbox *runtimeapi.PodSandboxStatus) []string {
	logger := klog.FromContext(ctx)
	podIPs := make([]string, 0)
	if podSandbox.Network == nil {
		logger.Info("Pod Sandbox status doesn't have network information, cannot report IPs", "pod", klog.KRef(podNamespace, podName))
		return podIPs
	}

	// 若运行时并不负责分配 IP（例如主机网络），IP 可能为空字符串。

	// 取主 IP。
	if len(podSandbox.Network.Ip) != 0 {
		if netutils.ParseIPSloppy(podSandbox.Network.Ip) == nil {
			logger.Info("Pod Sandbox reported an unparseable primary IP", "pod", klog.KRef(podNamespace, podName), "IP", podSandbox.Network.Ip)
			return nil
		}
		podIPs = append(podIPs, podSandbox.Network.Ip)
	}

	// 若 CRI 上报了附加 IP，也一并收集。
	for _, podIP := range podSandbox.Network.AdditionalIps {
		if nil == netutils.ParseIPSloppy(podIP.Ip) {
			logger.Info("Pod Sandbox reported an unparseable additional IP", "pod", klog.KRef(podNamespace, podName), "IP", podIP.Ip)
			return nil
		}
		podIPs = append(podIPs, podIP.Ip)
	}

	return podIPs
}

// getSandboxIDByPodUID 按 podUID 获取其所有沙箱 ID 列表。
func (m *kubeGenericRuntimeManager) getSandboxIDByPodUID(ctx context.Context, podUID kubetypes.UID) ([]string, error) {
	logger := klog.FromContext(ctx)
	sandboxes, err := m.getSandboxes(ctx, listOptions{podUID: podUID})
	if err != nil {
		logger.Error(err, "Failed to list sandboxes for pod", "podUID", podUID)
		return nil, err
	}

	if len(sandboxes) == 0 {
		return nil, nil
	}

	// 按创建时间从新到旧排序，调用方通常使用第一个（最新的）沙箱。
	sandboxIDs := make([]string, len(sandboxes))
	sort.Sort(podSandboxByCreatedThenID(sandboxes))
	for i, s := range sandboxes {
		sandboxIDs[i] = s.Id
	}

	return sandboxIDs, nil
}

// GetPortForward 获取运行时为端口转发请求提供服务所用的端点 URL。
func (m *kubeGenericRuntimeManager) GetPortForward(ctx context.Context, podName, podNamespace string, podUID kubetypes.UID, ports []int32) (*url.URL, error) {
	sandboxIDs, err := m.getSandboxIDByPodUID(ctx, podUID)
	if err != nil {
		return nil, fmt.Errorf("failed to find sandboxID for pod %s: %v", format.PodDesc(podName, podNamespace, podUID), err)
	}
	if len(sandboxIDs) == 0 {
		return nil, fmt.Errorf("failed to find sandboxID for pod %s", format.PodDesc(podName, podNamespace, podUID))
	}
	// 使用最新的沙箱处理端口转发。
	req := &runtimeapi.PortForwardRequest{
		PodSandboxId: sandboxIDs[0],
		Port:         ports,
	}
	resp, err := m.runtimeService.PortForward(ctx, req)
	if err != nil {
		return nil, err
	}
	return url.Parse(resp.Url)
}

// validSysctlMap 记录无需额外判断即可安全设置的 sysctl 参数，
// 其余参数需根据 Pod 的命名空间隔离情况决定是否设置。
var validSysctlMap = map[string]bool{
	"kernel.msgmax":          true,
	"kernel.msgmnb":          true,
	"kernel.msgmni":          true,
	"kernel.sem":             true,
	"kernel.shmall":          true,
	"kernel.shmmax":          true,
	"kernel.shmmni":          true,
	"kernel.shm_rmid_forced": true,
}

// applyPodSysctls 将 Pod 级 sysctl 应用到沙箱配置中，
// 并根据 Pod 是否共享主机命名空间决定是否真正设置。
// 规则参考：https://github.com/opencontainers/runc/blob/master/libcontainer/configs/validate/validator.go
func applyPodSysctls(sysctls, podSysctls map[string]string, pod *v1.Pod) {
	for k, v := range podSysctls {
		normalizedKey := utilsysctl.NormalizeName(k)
		if validSysctlMap[normalizedKey] || strings.HasPrefix(normalizedKey, "fs.mqueue.") {
			if !pod.Spec.HostIPC {
				// 除非 Pod 运行在主机 IPC 命名空间，否则设置 IPC 参数。
				sysctls[normalizedKey] = v
			}
			continue
		}
		if strings.HasPrefix(normalizedKey, "net.") {
			if !pod.Spec.HostNetwork {
				// 除非 Pod 运行在主机网络命名空间，否则设置网络参数。
				sysctls[normalizedKey] = v
			}
			continue
		}
		if strings.HasPrefix(normalizedKey, "user.") {
			// HostUsers 默认为 true，因此 nil 表示 Pod 运行在主机用户命名空间。
			if pod.Spec.HostUsers != nil && !*pod.Spec.HostUsers {
				// 除非 Pod 运行在主机用户命名空间，否则设置用户参数。
				sysctls[normalizedKey] = v
			}
			continue
		}
		if !pod.Spec.HostNetwork {
			if normalizedKey == "kernel.domainname" {
				// 该参数按命名空间隔离，且 OCI 中没有对应的显式字段。
				sysctls[normalizedKey] = v
			}
			continue
		}
	}
}
