/*
Copyright 2022 The Kubernetes Authors.

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

package debug

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubectl/pkg/util/podutils"
	"k8s.io/utils/ptr"
)

type debugStyle int

const (
	// 通过临时容器调试
	ephemeral debugStyle = iota
	// 通过 pod 副本调试
	podCopy
	// 调试节点
	node
	// 不支持的调试方式
	unsupported
)

const (
	// 注意：添加新的 profile 字符串时，记得同时将其加入
	// --profile flag 的帮助文本

	// ProfileLegacy 表示旧版调试 profile，与 1.23 的行为向后兼容。
	//
	// Deprecated: legacyProfile is planned to be removed in v1.39.
	ProfileLegacy = "legacy"
	// ProfileGeneral 包含针对每种调试过程量身定制的一组合理默认值。
	ProfileGeneral = "general"
	// ProfileBaseline 与 "general" 相同，但去掉了 baseline 安全 profile 下不允许的特权，
	// 例如 host 命名空间、host volume、mount 和 SYS_PTRACE。
	ProfileBaseline = "baseline"
	// ProfileRestricted 与 "baseline" 相同，但增加了 restricted 安全 profile 下所需的配置，
	// 例如要求非 root 用户并丢弃所有 capability。
	ProfileRestricted = "restricted"
	// ProfileNetadmin 为网络调试提供提升的特权。
	ProfileNetadmin = "netadmin"
	// ProfileSysadmin 为调试提供提升的特权。
	ProfileSysadmin = "sysadmin"
)

type ProfileApplier interface {
	// Apply 将此 profile 应用到 pod 中给定的容器。
	Apply(pod *corev1.Pod, containerName string, target runtime.Object) error
}

// NewProfileApplier 根据给定的 profile 名称返回一个新的 Options。
func NewProfileApplier(profile string, kflags KeepFlags) (ProfileApplier, error) {
	switch profile {
	case ProfileLegacy:
		return &legacyProfile{kflags}, nil
	case ProfileGeneral:
		return &generalProfile{kflags}, nil
	case ProfileBaseline:
		return &baselineProfile{kflags}, nil
	case ProfileRestricted:
		return &restrictedProfile{kflags}, nil
	case ProfileNetadmin:
		return &netadminProfile{kflags}, nil
	case ProfileSysadmin:
		return &sysadminProfile{kflags}, nil
	}
	return nil, fmt.Errorf("unknown profile: %s", profile)
}

type legacyProfile struct {
	KeepFlags
}

type generalProfile struct {
	KeepFlags
}

type baselineProfile struct {
	KeepFlags
}

type restrictedProfile struct {
	KeepFlags
}

type netadminProfile struct {
	KeepFlags
}

type sysadminProfile struct {
	KeepFlags
}

// KeepFlags 保存决定要在副本 pod 中保留哪些字段的 flag 集合。
type KeepFlags struct {
	Labels         bool
	Annotations    bool
	Liveness       bool
	Readiness      bool
	Startup        bool
	InitContainers bool
}

// RemoveLabels 从 pod 中移除 label。
func (kflags KeepFlags) RemoveLabels(p *corev1.Pod) {
	if !kflags.Labels {
		p.Labels = nil
	}
}

// RemoveAnnotations 从 pod 中移除 annotation。
func (kflags KeepFlags) RemoveAnnotations(p *corev1.Pod) {
	if !kflags.Annotations {
		p.Annotations = nil
	}
}

// RemoveProbes 从 pod 的所有容器中移除 probe。
func (kflags KeepFlags) RemoveProbes(p *corev1.Pod) {
	for i := range p.Spec.Containers {
		if !kflags.Liveness {
			p.Spec.Containers[i].LivenessProbe = nil
		}
		if !kflags.Readiness {
			p.Spec.Containers[i].ReadinessProbe = nil
		}
		if !kflags.Startup {
			p.Spec.Containers[i].StartupProbe = nil
		}
	}
}

// RemoveInitContainers 从 pod 中移除 initContainer。
func (kflags KeepFlags) RemoveInitContainers(p *corev1.Pod) {
	if !kflags.InitContainers {
		p.Spec.InitContainers = nil
	}
}

func getDebugStyle(pod *corev1.Pod, target runtime.Object) (debugStyle, error) {
	switch target.(type) {
	case *corev1.Pod:
		if asserted, ok := target.(*corev1.Pod); ok {
			if pod != asserted { // comparing addresses
				return podCopy, nil
			}
		}
		return ephemeral, nil
	case *corev1.Node:
		return node, nil
	}
	return unsupported, fmt.Errorf("objects of type %T are not supported", target)
}

func (p *legacyProfile) Apply(pod *corev1.Pod, containerName string, target runtime.Object) error {
	style, err := getDebugStyle(pod, target)
	if err != nil {
		return fmt.Errorf("legacy profile: %w", err)
	}

	switch style {
	case node:
		mountRootPartition(pod, containerName)
		useHostNamespaces(pod)

	case podCopy:
		p.Labels = false
		p.RemoveLabels(pod)

	case ephemeral:
		// 无需额外修改
	}

	return nil
}

func (p *generalProfile) Apply(pod *corev1.Pod, containerName string, target runtime.Object) error {
	style, err := getDebugStyle(pod, target)
	if err != nil {
		return fmt.Errorf("general profile: %w", err)
	}

	switch style {
	case node:
		mountRootPartition(pod, containerName)
		clearSecurityContext(pod, containerName)
		useHostNamespaces(pod)

	case podCopy:
		p.RemoveLabels(pod)
		p.RemoveAnnotations(pod)
		p.RemoveProbes(pod)
		p.RemoveInitContainers(pod)
		allowProcessTracing(pod, containerName)
		shareProcessNamespace(pod)

	case ephemeral:
		allowProcessTracing(pod, containerName)
	}

	return nil
}

func (p *baselineProfile) Apply(pod *corev1.Pod, containerName string, target runtime.Object) error {
	style, err := getDebugStyle(pod, target)
	if err != nil {
		return fmt.Errorf("baseline profile: %w", err)
	}

	clearSecurityContext(pod, containerName)

	switch style {
	case podCopy:
		p.RemoveLabels(pod)
		p.RemoveAnnotations(pod)
		p.RemoveProbes(pod)
		p.RemoveInitContainers(pod)
		shareProcessNamespace(pod)

	case ephemeral, node:
		// 无需额外修改
	}

	return nil
}

func (p *restrictedProfile) Apply(pod *corev1.Pod, containerName string, target runtime.Object) error {
	style, err := getDebugStyle(pod, target)
	if err != nil {
		return fmt.Errorf("restricted profile: %w", err)
	}

	clearSecurityContext(pod, containerName)
	disallowRoot(pod, containerName)
	dropCapabilities(pod, containerName)
	disallowPrivilegeEscalation(pod, containerName)
	setSeccompProfile(pod, containerName)

	switch style {
	case podCopy:
		p.RemoveLabels(pod)
		p.RemoveAnnotations(pod)
		p.RemoveProbes(pod)
		p.RemoveInitContainers(pod)
		shareProcessNamespace(pod)

	case ephemeral, node:
		// 无需额外修改
	}

	return nil
}

func (p *netadminProfile) Apply(pod *corev1.Pod, containerName string, target runtime.Object) error {
	style, err := getDebugStyle(pod, target)
	if err != nil {
		return fmt.Errorf("netadmin profile: %w", err)
	}

	allowNetadminCapability(pod, containerName)

	switch style {
	case node:
		useHostNamespaces(pod)

	case podCopy:
		p.RemoveLabels(pod)
		p.RemoveAnnotations(pod)
		p.RemoveProbes(pod)
		p.RemoveInitContainers(pod)
		shareProcessNamespace(pod)

	case ephemeral:
		// 无需额外修改
	}

	return nil
}

func (p *sysadminProfile) Apply(pod *corev1.Pod, containerName string, target runtime.Object) error {
	style, err := getDebugStyle(pod, target)
	if err != nil {
		return fmt.Errorf("sysadmin profile: %w", err)
	}

	setPrivileged(pod, containerName)

	switch style {
	case node:
		useHostNamespaces(pod)
		mountRootPartition(pod, containerName)

	case podCopy:
		// 模仿 general、default 和 baseline
		p.RemoveLabels(pod)
		p.RemoveAnnotations(pod)
		p.RemoveProbes(pod)
		p.RemoveInitContainers(pod)
		shareProcessNamespace(pod)

	case ephemeral:
		// 无需额外修改
	}

	return nil
}

// mountRootPartition 将宿主机的根路径挂载到容器中的 "/host"。
func mountRootPartition(p *corev1.Pod, containerName string) {
	const volumeName = "host-root"
	p.Spec.Volumes = append(p.Spec.Volumes, corev1.Volume{
		Name: volumeName,
		VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{Path: "/"},
		},
	})
	podutils.VisitContainers(&p.Spec, podutils.Containers, func(c *corev1.Container, _ podutils.ContainerType) bool {
		if c.Name != containerName {
			return true
		}
		c.VolumeMounts = append(c.VolumeMounts, corev1.VolumeMount{
			MountPath: "/host",
			Name:      volumeName,
		})
		return false
	})
}

// useHostNamespaces 将 pod 配置为使用宿主机的 network、PID 和 IPC
// 命名空间。
func useHostNamespaces(p *corev1.Pod) {
	p.Spec.HostNetwork = true
	p.Spec.HostPID = true
	p.Spec.HostIPC = true
}

// shareProcessNamespace 将 pod 中的所有容器配置为共享
// 进程命名空间。
func shareProcessNamespace(p *corev1.Pod) {
	if p.Spec.ShareProcessNamespace == nil {
		p.Spec.ShareProcessNamespace = ptr.To(true)
	}
}

// clearSecurityContext 清除容器的安全上下文。
func clearSecurityContext(p *corev1.Pod, containerName string) {
	podutils.VisitContainers(&p.Spec, podutils.AllContainers, func(c *corev1.Container, _ podutils.ContainerType) bool {
		if c.Name != containerName {
			return true
		}
		c.SecurityContext = nil
		return false
	})
}

// setPrivileged 将容器配置为特权容器。
func setPrivileged(p *corev1.Pod, containerName string) {
	podutils.VisitContainers(&p.Spec, podutils.AllContainers, func(c *corev1.Container, _ podutils.ContainerType) bool {
		if c.Name != containerName {
			return true
		}
		if c.SecurityContext == nil {
			c.SecurityContext = &corev1.SecurityContext{}
		}
		c.SecurityContext.Privileged = ptr.To(true)
		return false
	})
}

// disallowRoot 将容器配置为以非 root 用户运行。
func disallowRoot(p *corev1.Pod, containerName string) {
	podutils.VisitContainers(&p.Spec, podutils.AllContainers, func(c *corev1.Container, _ podutils.ContainerType) bool {
		if c.Name != containerName {
			return true
		}
		if c.SecurityContext == nil {
			c.SecurityContext = &corev1.SecurityContext{}
		}
		c.SecurityContext.RunAsNonRoot = ptr.To(true)
		return false
	})
}

// dropCapabilities 丢弃容器的所有 Capability
func dropCapabilities(p *corev1.Pod, containerName string) {
	podutils.VisitContainers(&p.Spec, podutils.AllContainers, func(c *corev1.Container, _ podutils.ContainerType) bool {
		if c.Name != containerName {
			return true
		}
		if c.SecurityContext == nil {
			c.SecurityContext = &corev1.SecurityContext{}
		}
		if c.SecurityContext.Capabilities == nil {
			c.SecurityContext.Capabilities = &corev1.Capabilities{}
		}
		c.SecurityContext.Capabilities.Drop = []corev1.Capability{"ALL"}
		c.SecurityContext.Capabilities.Add = nil
		return false
	})
}

// allowProcessTracing 为容器授予 SYS_PTRACE capability。
func allowProcessTracing(p *corev1.Pod, containerName string) {
	podutils.VisitContainers(&p.Spec, podutils.AllContainers, func(c *corev1.Container, _ podutils.ContainerType) bool {
		if c.Name != containerName {
			return true
		}
		addCapability(c, "SYS_PTRACE")
		return false
	})
}

// allowNetadminCapability 为容器授予 NET_ADMIN 和 NET_RAW capability。
func allowNetadminCapability(p *corev1.Pod, containerName string) {
	podutils.VisitContainers(&p.Spec, podutils.AllContainers, func(c *corev1.Container, _ podutils.ContainerType) bool {
		if c.Name != containerName {
			return true
		}
		addCapability(c, "NET_ADMIN")
		addCapability(c, "NET_RAW")
		return false
	})
}

func addCapability(c *corev1.Container, capability corev1.Capability) {
	if c.SecurityContext == nil {
		c.SecurityContext = &corev1.SecurityContext{}
	}
	if c.SecurityContext.Capabilities == nil {
		c.SecurityContext.Capabilities = &corev1.Capabilities{}
	}
	c.SecurityContext.Capabilities.Add = append(c.SecurityContext.Capabilities.Add, capability)
}

// disallowPrivilegeEscalation 将容器配置为不允许 PrivilegeEscalation
func disallowPrivilegeEscalation(p *corev1.Pod, containerName string) {
	podutils.VisitContainers(&p.Spec, podutils.AllContainers, func(c *corev1.Container, _ podutils.ContainerType) bool {
		if c.Name != containerName {
			return true
		}
		if c.SecurityContext == nil {
			c.SecurityContext = &corev1.SecurityContext{}
		}
		c.SecurityContext.AllowPrivilegeEscalation = ptr.To(false)
		return false
	})
}

// setSeccompProfile 将 SeccompProfile 应用于容器
func setSeccompProfile(p *corev1.Pod, containerName string) {
	podutils.VisitContainers(&p.Spec, podutils.AllContainers, func(c *corev1.Container, _ podutils.ContainerType) bool {
		if c.Name != containerName {
			return true
		}
		if c.SecurityContext == nil {
			c.SecurityContext = &corev1.SecurityContext{}
		}
		c.SecurityContext.SeccompProfile = &corev1.SeccompProfile{Type: "RuntimeDefault"}
		return false
	})
}
