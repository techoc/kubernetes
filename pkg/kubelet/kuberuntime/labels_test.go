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
	"reflect"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/features"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestContainerLabels 验证从容器标签能还原出正确的 labeledContainerInfo。
func TestContainerLabels(t *testing.T) {
	tCtx := ktesting.Init(t)
	deletionGracePeriod := int64(10)
	terminationGracePeriod := int64(10)
	lifecycle := &v1.Lifecycle{
		// PostStart 故意保持 nil，以验证可选字段的处理。
		PreStop: &v1.LifecycleHandler{
			Exec: &v1.ExecAction{
				Command: []string{"action1", "action2"},
			},
			HTTPGet: &v1.HTTPGetAction{
				Path:   "path",
				Host:   "host",
				Port:   intstr.FromInt32(8080),
				Scheme: "scheme",
			},
			TCPSocket: &v1.TCPSocketAction{
				Port: intstr.FromString("80"),
			},
		},
	}
	container := &v1.Container{
		Name:                   "test_container",
		TerminationMessagePath: "/somepath",
		Lifecycle:              lifecycle,
	}
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:                       "test_pod",
			Namespace:                  "test_pod_namespace",
			UID:                        "test_pod_uid",
			DeletionGracePeriodSeconds: &deletionGracePeriod,
		},
		Spec: v1.PodSpec{
			Containers:                    []v1.Container{*container},
			TerminationGracePeriodSeconds: &terminationGracePeriod,
		},
	}

	var tests = []struct {
		description string
		expected    *labeledContainerInfo
	}{
		{
			"Regular containers",
			&labeledContainerInfo{
				PodName:       pod.Name,
				PodNamespace:  pod.Namespace,
				PodUID:        pod.UID,
				ContainerName: container.Name,
			},
		},
	}

	// 验证能否从标签中还原出正确的信息。
	for _, test := range tests {
		labels := newContainerLabels(container, pod)
		containerInfo := getContainerInfoFromLabels(tCtx, labels)
		if !reflect.DeepEqual(containerInfo, test.expected) {
			t.Errorf("%v: expected %v, got %v", test.description, test.expected, containerInfo)
		}
	}
}

// TestContainerAnnotations 验证容器注解的写入与读取往返一致，
// 包括可选字段为 nil 以及特性门控开启时的哈希更新。
func TestContainerAnnotations(t *testing.T) {
	tCtx := ktesting.Init(t)
	restartCount := 5
	deletionGracePeriod := int64(10)
	terminationGracePeriod := int64(10)
	opts := &kubecontainer.RunContainerOptions{
		Annotations: []kubecontainer.Annotation{
			{Name: "Foo", Value: "bar"},
		},
	}
	lifecycle := &v1.Lifecycle{
		// PostStart 故意保持 nil，以验证可选字段的处理。
		PreStop: &v1.LifecycleHandler{
			Exec: &v1.ExecAction{
				Command: []string{"action1", "action2"},
			},
			HTTPGet: &v1.HTTPGetAction{
				Path:   "path",
				Host:   "host",
				Port:   intstr.FromInt32(8080),
				Scheme: "scheme",
			},
			TCPSocket: &v1.TCPSocketAction{
				Port: intstr.FromString("80"),
			},
		},
	}
	containerPorts := []v1.ContainerPort{
		{
			Name:          "http",
			HostPort:      80,
			ContainerPort: 8080,
			Protocol:      v1.ProtocolTCP,
		},
		{
			Name:          "https",
			HostPort:      443,
			ContainerPort: 6443,
			Protocol:      v1.ProtocolTCP,
		},
	}
	container := &v1.Container{
		Name:                   "test_container",
		Ports:                  containerPorts,
		TerminationMessagePath: "/somepath",
		Lifecycle:              lifecycle,
	}
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:                       "test_pod",
			Namespace:                  "test_pod_namespace",
			UID:                        "test_pod_uid",
			DeletionGracePeriodSeconds: &deletionGracePeriod,
		},
		Spec: v1.PodSpec{
			Containers:                    []v1.Container{*container},
			TerminationGracePeriodSeconds: &terminationGracePeriod,
		},
	}
	expected := &annotatedContainerInfo{
		ContainerPorts:            containerPorts,
		PodDeletionGracePeriod:    pod.DeletionGracePeriodSeconds,
		PodTerminationGracePeriod: pod.Spec.TerminationGracePeriodSeconds,
		Hash:                      kubecontainer.HashContainer(container),
		RestartCount:              restartCount,
		TerminationMessagePath:    container.TerminationMessagePath,
		PreStopHandler:            container.Lifecycle.PreStop,
	}

	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodVerticalScaling, true)
	// 验证能否从注解中还原出正确的信息。
	annotations := newContainerAnnotations(tCtx, container, pod, restartCount, opts)
	containerInfo := getContainerInfoFromAnnotations(tCtx, annotations)
	if !reflect.DeepEqual(containerInfo, expected) {
		t.Errorf("expected %v, got %v", expected, containerInfo)
	}
	if v, ok := annotations[opts.Annotations[0].Name]; !ok || v != opts.Annotations[0].Value {
		t.Errorf("expected annotation %s to exist got %v, %v", opts.Annotations[0].Name, ok, v)
	}

	// 当 DeletionGracePeriodSeconds、TerminationGracePeriodSeconds 和 Lifecycle 都为 nil 时，
	// 从注解还原出的对应信息也应当为 nil。
	container.Lifecycle = nil
	pod.DeletionGracePeriodSeconds = nil
	pod.Spec.TerminationGracePeriodSeconds = nil
	expected.PodDeletionGracePeriod = nil
	expected.PodTerminationGracePeriod = nil
	expected.PreStopHandler = nil
	// 容器内容已改变，哈希也应随之更新。
	expected.Hash = kubecontainer.HashContainer(container)
	annotations = newContainerAnnotations(tCtx, container, pod, restartCount, opts)
	containerInfo = getContainerInfoFromAnnotations(tCtx, annotations)
	if !reflect.DeepEqual(containerInfo, expected) {
		t.Errorf("expected %v, got %v", expected, containerInfo)
	}
	if v, ok := annotations[opts.Annotations[0].Name]; !ok || v != opts.Annotations[0].Value {
		t.Errorf("expected annotation %s to exist got %v, %v", opts.Annotations[0].Name, ok, v)
	}
}

// TestPodLabels 验证从 Pod sandbox 标签能还原出 Pod 的基本信息和自定义标签。
func TestPodLabels(t *testing.T) {
	tCtx := ktesting.Init(t)
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test_pod",
			Namespace: "test_pod_namespace",
			UID:       "test_pod_uid",
			Labels:    map[string]string{"foo": "bar"},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{},
		},
	}
	expected := &labeledPodSandboxInfo{
		Labels:       pod.Labels,
		PodName:      pod.Name,
		PodNamespace: pod.Namespace,
		PodUID:       pod.UID,
	}

	// 验证能否从标签中还原出正确的信息。
	labels := newPodLabels(pod)
	podSandboxInfo := getPodSandboxInfoFromLabels(tCtx, labels)
	if !reflect.DeepEqual(podSandboxInfo, expected) {
		t.Errorf("expected %v, got %v", expected, podSandboxInfo)
	}
}

// TestPodAnnotations 验证 Pod 注解能原样写入并从 sandbox 注解中还原。
func TestPodAnnotations(t *testing.T) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        "test_pod",
			Namespace:   "test_pod_namespace",
			UID:         "test_pod_uid",
			Annotations: map[string]string{"foo": "bar"},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{},
		},
	}
	expected := &annotatedPodSandboxInfo{
		Annotations: map[string]string{"foo": "bar"},
	}

	// 验证能否从注解中还原出正确的信息。
	annotations := newPodAnnotations(pod)
	podSandboxInfo := getPodSandboxInfoFromAnnotations(annotations)
	if !reflect.DeepEqual(podSandboxInfo, expected) {
		t.Errorf("expected %v, got %v", expected, podSandboxInfo)
	}
}
