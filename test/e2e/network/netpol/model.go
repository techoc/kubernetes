/*
Copyright 2020 The Kubernetes Authors.

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

package netpol

import (
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/test/e2e/framework"
	imageutils "k8s.io/kubernetes/test/utils/image"
)

// Model 定义 network policy 测试用例的 namespace、deployment、service、pod、container
// 及相关数据，是测试数据的唯一来源
type Model struct {
	Namespaces []*Namespace
	PodNames   []string
	Ports      []int32
	Protocols  []v1.Protocol
}

// NewModel 基于以下内容实例化 model：
// - namespaceNames
// - pods
// - 要监听的端口
// - 要监听的协议
// pod 总数为 namespace 数乘以每 namespace 的 pod 数。
// 每 pod 的容器数为端口数乘以协议数。
// 容器*总数*为 namespace x pod x 端口 x 协议。
func NewModel(namespaceNames []string, podNames []string, ports []int32, protocols []v1.Protocol) *Model {
	podNamesByNamespace := make(map[string]sets.Set[string], len(namespaceNames))
	for _, ns := range namespaceNames {
		podNamesByNamespace[ns] = sets.New(podNames...)
	}
	return newModelWithPerNamespacePodNames(namespaceNames, podNamesByNamespace, ports, protocols)
}

// newModelWithPerNamespacePodNames 实例化一个 model，每个 namespace 可定义不同的 pod 集合。
func newModelWithPerNamespacePodNames(namespaceNames []string, podNamesByNamespace map[string]sets.Set[string], ports []int32, protocols []v1.Protocol) *Model {
	model := &Model{
		Ports:     ports,
		Protocols: protocols,
	}
	allPodNames := sets.New[string]()

	// 构建整个测试的"模型"，也就是为每种协议构建
	// namespace、pod、container。
	for _, ns := range namespaceNames {
		var pods []*Pod
		podNames := sets.List(podNamesByNamespace[ns])
		for _, podName := range podNames {
			allPodNames.Insert(podName)
			pods = append(pods, &Pod{
				Name:      podName,
				Ports:     ports,
				Protocols: protocols,
			})
		}
		model.Namespaces = append(model.Namespaces, &Namespace{
			Name: ns,
			Pods: pods,
		})
	}
	model.PodNames = sets.List(allPodNames)
	return model
}

// Namespace 是对 network policy 测试所关心的 namespace
// 内容的抽象表示，忽略 kube 实现细节
type Namespace struct {
	Name string
	Pods []*Pod
}

// Pod 是对 network policy 测试所关心的 pod
// 内容的抽象表示，忽略 kube 实现细节
type Pod struct {
	Name      string
	Ports     []int32
	Protocols []v1.Protocol
}

func podNameLabelKey() string {
	return "pod"
}

// Labels 返回应放在 pod/deployment 上的默认标签，
// 以便通过 label selector 唯一选中它
func (p *Pod) Labels() map[string]string {
	return map[string]string{
		podNameLabelKey(): p.Name,
	}
}

// KubePod 返回 kube pod（必要时为 windows 添加 label selector）。
func (p *Pod) KubePod(namespace string) *v1.Pod {
	zero := int64(0)

	thePod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      p.Name,
			Labels:    p.Labels(),
			Namespace: namespace,
		},
		Spec: v1.PodSpec{
			TerminationGracePeriodSeconds: &zero,
			Containers:                    p.ContainerSpecs(),
		},
	}

	if framework.NodeOSDistroIs("windows") {
		thePod.Spec.NodeSelector = map[string]string{
			"kubernetes.io/os": "windows",
		}
	}
	return thePod
}

// QualifiedServiceAddress 返回可用于访问 service 的地址
func (p *Pod) QualifiedServiceAddress(namespace string, dnsDomain string) string {
	return fmt.Sprintf("%s.%s.svc.%s", p.ServiceName(namespace), namespace, dnsDomain)
}

// ServiceName 返回非限定 service 名称
func (p *Pod) ServiceName(namespace string) string {
	return fmt.Sprintf("s-%s-%s", namespace, p.Name)
}

// Service 返回 kube service spec
func (p *Pod) Service(namespace string) *v1.Service {
	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      p.ServiceName(namespace),
			Namespace: namespace,
		},
		Spec: v1.ServiceSpec{
			Selector: p.Labels(),
		},
	}
	for _, protocol := range p.Protocols {
		for _, port := range p.Ports {
			service.Spec.Ports = append(service.Spec.Ports, v1.ServicePort{
				Name:     fmt.Sprintf("service-port-%s-%d", strings.ToLower(string(protocol)), port),
				Protocol: protocol,
				Port:     port,
			})
		}
	}
	return service
}

// ContainerSpecs 为 pod 构建 kubernetes container spec
func (p *Pod) ContainerSpecs() []v1.Container {
	env := make([]v1.EnvVar, 0, len(p.Ports)*len(p.Protocols))
	ports := make([]v1.ContainerPort, 0, len(p.Ports)*len(p.Protocols))

	for _, protocol := range p.Protocols {
		for _, port := range p.Ports {
			env = append(env, v1.EnvVar{
				Name:  fmt.Sprintf("SERVE_%s_PORT_%d", protocol, port),
				Value: "foo",
			})
			ports = append(ports, v1.ContainerPort{
				Name:          fmt.Sprintf("serve-%d-%s", port, strings.ToLower(string(protocol))),
				Protocol:      protocol,
				ContainerPort: port,
			})
		}
	}

	return []v1.Container{{
		Name:            "agnhost",
		ImagePullPolicy: v1.PullIfNotPresent,
		Image:           imageutils.GetE2EImage(imageutils.Agnhost),
		Command:         []string{"/agnhost", "porter"},
		SecurityContext: &v1.SecurityContext{},
		Env:             env,
		Ports:           ports,
	}}
}
