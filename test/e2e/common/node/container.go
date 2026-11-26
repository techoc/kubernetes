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

package node

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
)

const (
	// ContainerStatusRetryTimeout 是放弃获取容器状态前的轮询阈值
	ContainerStatusRetryTimeout = time.Minute * 5
	// ContainerStatusPollInterval 是获取容器状态的轮询间隔
	ContainerStatusPollInterval = time.Second * 1
)

// ConformanceContainer 定义运行容器一致性测试用例的类型
// 一个 pod 一个容器
type ConformanceContainer struct {
	Container        v1.Container
	RestartPolicy    v1.RestartPolicy
	Volumes          []v1.Volume
	ImagePullSecrets []string
	NodeName         string

	PodClient          *e2epod.PodClient
	podName            string
	PodSecurityContext *v1.PodSecurityContext
}

// Create 创建定义的一致性容器
func (cc *ConformanceContainer) Create(ctx context.Context) *v1.Pod {
	cc.podName = cc.Container.Name + string(uuid.NewUUID())
	imagePullSecrets := []v1.LocalObjectReference{}
	for _, s := range cc.ImagePullSecrets {
		imagePullSecrets = append(imagePullSecrets, v1.LocalObjectReference{Name: s})
	}
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: cc.podName,
		},
		Spec: v1.PodSpec{
			RestartPolicy: cc.RestartPolicy,
			Containers: []v1.Container{
				cc.Container,
			},
			NodeName:         cc.NodeName,
			SecurityContext:  cc.PodSecurityContext,
			Volumes:          cc.Volumes,
			ImagePullSecrets: imagePullSecrets,
		},
	}
	return cc.PodClient.Create(ctx, pod)
}

// Delete 删除定义的一致性容器
func (cc *ConformanceContainer) Delete(ctx context.Context) error {
	return cc.PodClient.Delete(ctx, cc.podName, *metav1.NewDeleteOptions(0))
}

// IsReady 返回此容器是否就绪，以及可能的错误
func (cc *ConformanceContainer) IsReady(ctx context.Context) (bool, error) {
	pod, err := cc.PodClient.Get(ctx, cc.podName, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	return podutil.IsPodReady(pod), nil
}

// GetPhase 返回 pod 生命周期的阶段，以及可能的错误
func (cc *ConformanceContainer) GetPhase(ctx context.Context) (v1.PodPhase, error) {
	pod, err := cc.PodClient.Get(ctx, cc.podName, metav1.GetOptions{})
	if err != nil {
		// 返回哪个阶段都无所谓，因为错误不会是 nil
		return v1.PodSucceeded, err
	}
	return pod.Status.Phase, nil
}

// GetStatus 返回此容器当前状态的详情，以及可能的错误
func (cc *ConformanceContainer) GetStatus(ctx context.Context) (v1.ContainerStatus, error) {
	pod, err := cc.PodClient.Get(ctx, cc.podName, metav1.GetOptions{})
	if err != nil {
		return v1.ContainerStatus{}, err
	}
	statuses := pod.Status.ContainerStatuses
	if len(statuses) != 1 || statuses[0].Name != cc.Container.Name {
		return v1.ContainerStatus{}, fmt.Errorf("unexpected container statuses %v", statuses)
	}
	return statuses[0], nil
}

// Present 返回此 pod 是否存在，以及可能的错误
func (cc *ConformanceContainer) Present(ctx context.Context) (bool, error) {
	_, err := cc.PodClient.Get(ctx, cc.podName, metav1.GetOptions{})
	if err == nil {
		return true, nil
	}
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	return false, err
}

// ContainerState 表示其生命周期中的不同状态
type ContainerState string

const (
	// ContainerStateWaiting 表示 'Waiting' 容器状态
	ContainerStateWaiting ContainerState = "Waiting"
	// ContainerStateRunning 表示 'Running' 容器状态
	ContainerStateRunning ContainerState = "Running"
	// ContainerStateTerminated 表示 'Terminated' 容器状态
	ContainerStateTerminated ContainerState = "Terminated"
	// ContainerStateUnknown 表示 'Unknown' 容器状态
	ContainerStateUnknown ContainerState = "Unknown"
)

// GetContainerState 返回容器在生命周期中所处的当前状态
func GetContainerState(state v1.ContainerState) ContainerState {
	if state.Waiting != nil {
		return ContainerStateWaiting
	}
	if state.Running != nil {
		return ContainerStateRunning
	}
	if state.Terminated != nil {
		return ContainerStateTerminated
	}
	return ContainerStateUnknown
}
