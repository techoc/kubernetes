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

package pod

import (
	"fmt"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/test/e2e/framework"
	imageutils "k8s.io/kubernetes/test/utils/image"
	psaapi "k8s.io/pod-security-admission/api"
	psapolicy "k8s.io/pod-security-admission/policy"
	"k8s.io/utils/ptr"
)

// 该命令执行无限循环，每轮 sleep 1 秒。
// 收到 TERM 信号时会设置 trap 优雅退出。
//
// 适用于测试容器以退出码 0 终止的场景。
const InfiniteSleepCommand = "trap exit TERM; while true; do sleep 1; done"

// 该命令会让 shell 无限期保持 sleep 状态，
// 除非收到 KILL 信号才会退出。
//
// 适用于测试容器以非 0 退出码终止的场景。
const InfiniteSleepCommandWithoutGracefulShutdown = "while true; do sleep 100000; done"

// GenerateScriptCmd 生成执行命令所需的命令行。
func GenerateScriptCmd(command string) []string {
	return []string{"/bin/sh", "-c", command}
}

// GetDefaultTestImage 根据 OS 返回默认测试镜像。
// 节点 OS 为 windows 时，由于
// #https://github.com/kubernetes-sigs/windows-testing/pull/35 的问题，当前返回 Windows 版 Agnhost 镜像。
// 节点 OS 为 linux 时返回 busybox 镜像。
func GetDefaultTestImage() string {
	return imageutils.GetE2EImage(GetDefaultTestImageID())
}

// GetDefaultTestImageID 根据 OS 返回默认测试镜像 ID。
// 节点 OS 为 windows 时，由于
// #https://github.com/kubernetes-sigs/windows-testing/pull/35 的问题，当前返回 Windows 版 Agnhost 镜像。
// 节点 OS 为 linux 时返回 busybox 镜像。
func GetDefaultTestImageID() imageutils.ImageID {
	if framework.NodeOSDistroIs("windows") {
		return GetTestImageID(imageutils.Agnhost)
	}
	return GetTestImageID(imageutils.BusyBox)
}

// GetTestImage 根据输入返回镜像名。
// 节点 OS 为 windows 时，由于
// #https://github.com/kubernetes-sigs/windows-testing/pull/35 的问题，当前返回 Windows 版 Agnhost 镜像。
func GetTestImage(id imageutils.ImageID) string {
	if framework.NodeOSDistroIs("windows") {
		return imageutils.GetE2EImage(imageutils.Agnhost)
	}
	return imageutils.GetE2EImage(id)
}

// GetTestImageID 根据输入返回镜像 ID。
// 节点 OS 为 windows 时，由于
// #https://github.com/kubernetes-sigs/windows-testing/pull/35 的问题，当前返回 Windows 版 Agnhost 镜像。
func GetTestImageID(id imageutils.ImageID) imageutils.ImageID {
	if framework.NodeOSDistroIs("windows") {
		return imageutils.Agnhost
	}
	return id
}

// GetDefaultNonRootUser 返回默认的非 root 用户。
// 节点 OS 为 windows 时返回 nil，因为 projected volume 的权限设置有问题
// https://github.com/kubernetes/kubernetes/issues/102849
func GetDefaultNonRootUser() *int64 {
	if framework.NodeOSDistroIs("windows") {
		return nil
	}
	return ptr.To[int64](DefaultNonRootUser)
}

// GeneratePodSecurityContext 根据输入生成相应的 Pod security context。
// 节点 OS 为 windows 时，当前忽略输入并返回 nil。
// TODO: 等 windows 有了自己的 security context 后再改。
func GeneratePodSecurityContext(fsGroup *int64, seLinuxOptions *v1.SELinuxOptions) *v1.PodSecurityContext {
	if framework.NodeOSDistroIs("windows") {
		return nil
	}
	return &v1.PodSecurityContext{
		FSGroup:        fsGroup,
		SELinuxOptions: seLinuxOptions,
	}
}

// GenerateContainerSecurityContext 根据输入生成相应的 container security context。
// 节点 OS 为 windows 时，当前忽略输入并返回 nil。
// TODO: 等 windows 有了自己的 security context 后再改。
func GenerateContainerSecurityContext(level psaapi.Level) *v1.SecurityContext {
	if framework.NodeOSDistroIs("windows") {
		return nil
	}

	switch level {
	case psaapi.LevelBaseline:
		return &v1.SecurityContext{
			Privileged: ptr.To(false),
		}
	case psaapi.LevelPrivileged:
		return &v1.SecurityContext{
			Privileged: ptr.To(true),
		}
	case psaapi.LevelRestricted:
		return GetRestrictedContainerSecurityContext()
	default:
		ginkgo.Fail(fmt.Sprintf("unknown k8s.io/pod-security-admission/policy.Level %q", level))
		panic("not reached")
	}
}

// GetLinuxLabel 根据 OS 返回默认的 SELinuxLabel。
// 节点 OS 为 windows 时返回 nil
func GetLinuxLabel() *v1.SELinuxOptions {
	if framework.NodeOSDistroIs("windows") {
		return nil
	}
	return &v1.SELinuxOptions{
		Level: "s0:c0,c1"}
}

// DefaultNonRootUser 是运行受限（非 root）容器使用的默认用户 ID。
const DefaultNonRootUser = 1000

// DefaultNonRootUserName 是 Windows 上运行受限（非 root）容器使用的默认用户名
const DefaultNonRootUserName = "ContainerUser"

// GetRestrictedPodSecurityContext 返回受限的 Pod security context。
// 为通过 RunAsNonRoot 检查，方便起见会设置 RunAsUser。
// 需要指定用户 ID 的测试应覆盖它。
func GetRestrictedPodSecurityContext() *v1.PodSecurityContext {
	psc := &v1.PodSecurityContext{
		RunAsNonRoot:   ptr.To(true),
		RunAsUser:      GetDefaultNonRootUser(),
		SeccompProfile: &v1.SeccompProfile{Type: v1.SeccompProfileTypeRuntimeDefault},
	}

	if framework.NodeOSDistroIs("windows") {
		psc.WindowsOptions = &v1.WindowsSecurityContextOptions{}
		psc.WindowsOptions.RunAsUserName = ptr.To(DefaultNonRootUserName)
	}

	return psc
}

// GetRestrictedContainerSecurityContext 返回最小化的受限 container security context。
func GetRestrictedContainerSecurityContext() *v1.SecurityContext {
	return &v1.SecurityContext{
		AllowPrivilegeEscalation: ptr.To(false),
		Capabilities:             &v1.Capabilities{Drop: []v1.Capability{"ALL"}},
	}
}

var psaEvaluator, _ = psapolicy.NewEvaluator(psapolicy.DefaultChecks(), nil)

// MustMixinRestrictedPodSecurity 让给定 Pod 符合 restricted pod security level。
// 若这样做会覆盖已有的不合规配置，则触发测试失败。
func MustMixinRestrictedPodSecurity(pod *v1.Pod) *v1.Pod {
	err := MixinRestrictedPodSecurity(pod)
	gomega.ExpectWithOffset(1, err).NotTo(gomega.HaveOccurred())
	return pod
}

// MixinRestrictedPodSecurity 让给定 Pod 符合 restricted pod security level。
// 若这样做会覆盖已有的不合规配置，则返回错误。
// 注意它会设置默认的 RunAsUser，参见 GetRestrictedPodSecurityContext。
// TODO(#105919): 处理 windows Pod 的 PodOS。
func MixinRestrictedPodSecurity(pod *v1.Pod) error {
	if pod.Spec.SecurityContext == nil {
		pod.Spec.SecurityContext = GetRestrictedPodSecurityContext()
	} else {
		if pod.Spec.SecurityContext.RunAsNonRoot == nil {
			pod.Spec.SecurityContext.RunAsNonRoot = ptr.To(true)
		}
		if pod.Spec.SecurityContext.RunAsUser == nil {
			pod.Spec.SecurityContext.RunAsUser = GetDefaultNonRootUser()
		}
		if pod.Spec.SecurityContext.SeccompProfile == nil {
			pod.Spec.SecurityContext.SeccompProfile = &v1.SeccompProfile{Type: v1.SeccompProfileTypeRuntimeDefault}
		}
		if framework.NodeOSDistroIs("windows") && pod.Spec.SecurityContext.WindowsOptions == nil {
			pod.Spec.SecurityContext.WindowsOptions = &v1.WindowsSecurityContextOptions{}
			pod.Spec.SecurityContext.WindowsOptions.RunAsUserName = ptr.To(DefaultNonRootUserName)
		}
	}
	for i := range pod.Spec.Containers {
		mixinRestrictedContainerSecurityContext(&pod.Spec.Containers[i])
	}
	for i := range pod.Spec.InitContainers {
		mixinRestrictedContainerSecurityContext(&pod.Spec.InitContainers[i])
	}

	// 用 restricted profile 校验生成的 Pod。
	restricted := psaapi.LevelVersion{
		Level:   psaapi.LevelRestricted,
		Version: psaapi.LatestVersion(),
	}
	if agg := psapolicy.AggregateCheckResults(psaEvaluator.EvaluatePod(restricted, &pod.ObjectMeta, &pod.Spec)); !agg.Allowed {
		return fmt.Errorf("failed to make pod %s restricted: %s", pod.Name, agg.ForbiddenDetail())
	}

	return nil
}

// mixinRestrictedContainerSecurityContext 添加所需的 container security context 选项，
// 以符合 restricted pod security level。不合规检查由调用方处理。
func mixinRestrictedContainerSecurityContext(container *v1.Container) {
	if container.SecurityContext == nil {
		container.SecurityContext = GetRestrictedContainerSecurityContext()
	} else {
		if container.SecurityContext.AllowPrivilegeEscalation == nil {
			container.SecurityContext.AllowPrivilegeEscalation = ptr.To(false)
		}
		if container.SecurityContext.Capabilities == nil {
			container.SecurityContext.Capabilities = &v1.Capabilities{}
		}
		if len(container.SecurityContext.Capabilities.Drop) == 0 {
			container.SecurityContext.Capabilities.Drop = []v1.Capability{"ALL"}
		}
	}
}

// FindPodConditionByType 遍历 pod status 中所有 pod condition，返回指定的 condition。
func FindPodConditionByType(podStatus *v1.PodStatus, conditionType v1.PodConditionType) *v1.PodCondition {
	for _, cond := range podStatus.Conditions {
		if cond.Type == conditionType {
			return &cond
		}
	}
	return nil
}

// FindContainerInPod 按名称查找 Pod 中的容器
func FindContainerInPod(pod *v1.Pod, containerName string) *v1.Container {
	for _, container := range pod.Spec.InitContainers {
		if container.Name == containerName {
			return &container
		}
	}
	for _, container := range pod.Spec.Containers {
		if container.Name == containerName {
			return &container
		}
	}
	return nil
}

// FindContainerStatusInPod 在给定 Pod 中按名称查找容器状态
func FindContainerStatusInPod(pod *v1.Pod, containerName string) *v1.ContainerStatus {
	for _, containerStatus := range pod.Status.InitContainerStatuses {
		if containerStatus.Name == containerName {
			return &containerStatus
		}
	}
	for _, containerStatus := range pod.Status.ContainerStatuses {
		if containerStatus.Name == containerName {
			return &containerStatus
		}
	}
	for _, containerStatus := range pod.Status.EphemeralContainerStatuses {
		if containerStatus.Name == containerName {
			return &containerStatus
		}
	}
	return nil
}
