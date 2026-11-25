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

// tweak 深拷贝 in，将副本交给 f 修改后返回，不改变入参。
// 各夹具从同一基准 Pod 派生，避免共享指针导致用例之间相互污染。
func tweak(in *corev1.Pod, f func(copy *corev1.Pod)) *corev1.Pod {
	out := in.DeepCopy()
	f(out)
	return out
}

// ensureSecurityContext 为 Pod、普通容器和 init 容器补齐安全上下文。
// 先区分“对象缺失”和“字段未设置”，夹具才能只改变目标字段。
func ensureSecurityContext(p *corev1.Pod) *corev1.Pod {
	p = p.DeepCopy()
	if p.Spec.SecurityContext == nil {
		p.Spec.SecurityContext = &corev1.PodSecurityContext{}
	}
	for i := range p.Spec.Containers {
		if p.Spec.Containers[i].SecurityContext == nil {
			p.Spec.Containers[i].SecurityContext = &corev1.SecurityContext{}
		}
	}
	for i := range p.Spec.InitContainers {
		if p.Spec.InitContainers[i].SecurityContext == nil {
			p.Spec.InitContainers[i].SecurityContext = &corev1.SecurityContext{}
		}
	}
	return p
}

// ensureSELinuxOptions 为 Pod、普通容器和 init 容器补齐 SELinux 选项对象。
// 与 ensureSecurityContext 组合，可在不同层级构造差异夹具。
func ensureSELinuxOptions(p *corev1.Pod) *corev1.Pod {
	p = ensureSecurityContext(p)
	if p.Spec.SecurityContext.SELinuxOptions == nil {
		p.Spec.SecurityContext.SELinuxOptions = &corev1.SELinuxOptions{}
	}
	for i := range p.Spec.Containers {
		if p.Spec.Containers[i].SecurityContext.SELinuxOptions == nil {
			p.Spec.Containers[i].SecurityContext.SELinuxOptions = &corev1.SELinuxOptions{}
		}
	}
	for i := range p.Spec.InitContainers {
		if p.Spec.InitContainers[i].SecurityContext.SELinuxOptions == nil {
			p.Spec.InitContainers[i].SecurityContext.SELinuxOptions = &corev1.SELinuxOptions{}
		}
	}
	return p
}

// ensureAnnotation 确保 pod.Annotations 不为 nil，避免写注解时因 map 未初始化而 panic。
func ensureAnnotation(pod *corev1.Pod) *corev1.Pod {
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	return pod
}
