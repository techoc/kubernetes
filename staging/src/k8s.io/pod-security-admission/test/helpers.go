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

// tweak makes a copy of in, passes it to f(), and returns the result.
// the input is not modified.
// 每个用例都从同一个基准 Pod 派生，改动互不污染；用深拷贝避免共享指针被 f 修改。
func tweak(in *corev1.Pod, f func(copy *corev1.Pod)) *corev1.Pod {
	out := in.DeepCopy()
	f(out)
	return out
}

// ensureSecurityContext ensures the pod and all initContainers and containers have a non-nil security context.
// 测试里要区分“字段没设置”和“整层对象缺失”，先补齐对象才能只测字段语义。
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

// ensureSELinuxOptions ensures the pod and all initContainers and containers have a non-nil seLinuxOptions.
// 与 ensureSecurityContext 组合使用，才能在不同层级的 SELinux 选项上构造差异用例。
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

// ensureAnnotation makes sure that pod.Annotations is never nil
// 注解夹具需要往里写键值，先初始化 map 才能避免赋值 panic。
func ensureAnnotation(pod *corev1.Pod) *corev1.Pod {
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	return pod
}
