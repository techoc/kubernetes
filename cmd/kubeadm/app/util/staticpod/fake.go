/*
Copyright The Kubernetes Authors.

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

package staticpod

import (
	"context"
	"fmt"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
)

// FakeStaticPod 表示测试用伪静态 Pod。
type FakeStaticPod struct {
	NodeName    string
	Component   string
	Annotations map[string]string
}

// Pod 返回带指定后缀、表示伪静态 Pod 的 Pod 结构。
func (p *FakeStaticPod) Pod(suffix string) *v1.Pod {
	pod := ComponentPod(
		v1.Container{
			Name:  p.Component,
			Image: fmt.Sprintf("%s-image:tag", p.Component),
		},
		map[string]v1.Volume{},
		p.Annotations,
	)
	if len(suffix) > 0 {
		pod.ObjectMeta.Name = fmt.Sprintf("%s-%s-%s", p.Component, p.NodeName, suffix)
	} else {
		pod.ObjectMeta.Name = fmt.Sprintf("%s-%s", p.Component, p.NodeName)
	}
	pod.Spec.NodeName = p.NodeName
	return &pod
}

// Create 使用给定客户端创建伪静态 Pod。
func (p *FakeStaticPod) Create(client clientset.Interface) error {
	return p.CreateWithPodSuffix(client, "")
}

// CreateWithPodSuffix 使用给定客户端和后缀创建伪静态 Pod。
func (p *FakeStaticPod) CreateWithPodSuffix(client clientset.Interface, suffix string) error {
	_, err := client.CoreV1().Pods(metav1.NamespaceSystem).Create(context.TODO(), p.Pod(suffix), metav1.CreateOptions{})
	return err
}
