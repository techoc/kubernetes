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
	// the RuntimeDefault seccomp profile
	// 共享这些指针值，测试里就不用每次都写一个完整的 profile 结构。
	seccompProfileRuntimeDefault *corev1.SeccompProfile = &corev1.SeccompProfile{
		Type: corev1.SeccompProfileTypeRuntimeDefault,
	}

	// the Unconfined seccomp profile
	// 和 RuntimeDefault 互斥，分别代表「受约束」与「不受约束」两个判定端点。
	seccompProfileUnconfined *corev1.SeccompProfile = &corev1.SeccompProfile{
		Type: corev1.SeccompProfileTypeUnconfined,
	}
)

// the Localhost seccomp profile
// 路径由调用方给，才能测到「引用了节点上特定 profile」这种最宽松的情形。
func seccompProfileLocalhost(profile string) *corev1.SeccompProfile {
	return &corev1.SeccompProfile{
		Type:             corev1.SeccompProfileTypeLocalhost,
		LocalhostProfile: &profile,
	}
}

// annotationKeyContainer builds the annotation key for a specific container
// 老注解按容器名区分，所以拼接时不能漏掉名字，否则多个容器会互相覆盖。
func annotationKeyContainer(c corev1.Container) string {
	return annotationKeyContainerPrefix + c.Name
}
