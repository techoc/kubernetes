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

package test

import (
	corev1 "k8s.io/api/core/v1"
)

const (
	// 老的 alpha 注解已经被 securityContext.seccompProfile 取代，
	// 但测试仍需构造它们，用来验证向后兼容路径上的判定。
	annotationKeyPod             = "seccomp.security.alpha.kubernetes.io/pod"
	annotationKeyContainerPrefix = "container.seccomp.security.alpha.kubernetes.io/"
)

var (
	// RuntimeDefault seccomp profile；共享指针，避免每个夹具重复构造。
	seccompProfileRuntimeDefault *corev1.SeccompProfile = &corev1.SeccompProfile{
		Type: corev1.SeccompProfileTypeRuntimeDefault,
	}

	// Unconfined seccomp profile，与 RuntimeDefault 构成受约束和不受约束两个端点。
	seccompProfileUnconfined *corev1.SeccompProfile = &corev1.SeccompProfile{
		Type: corev1.SeccompProfileTypeUnconfined,
	}
)

// seccompProfileLocalhost 构造引用节点上指定 profile 的 Localhost 配置。
// 路径由调用方提供，以覆盖显式引用本地 profile 的场景。
func seccompProfileLocalhost(profile string) *corev1.SeccompProfile {
	return &corev1.SeccompProfile{
		Type:             corev1.SeccompProfileTypeLocalhost,
		LocalhostProfile: &profile,
	}
}

// annotationKeyContainer 按容器名拼接旧 seccomp 注解键，避免不同容器互相覆盖。
func annotationKeyContainer(c corev1.Container) string {
	return annotationKeyContainerPrefix + c.Name
}
