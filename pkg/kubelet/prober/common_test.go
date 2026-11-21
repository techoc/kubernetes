/*
Copyright 2015 The Kubernetes Authors.

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

package prober

import (
	"reflect"
	"sync"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	kubepod "k8s.io/kubernetes/pkg/kubelet/pod"
	"k8s.io/kubernetes/pkg/kubelet/prober/results"
	"k8s.io/kubernetes/pkg/kubelet/status"
	statustest "k8s.io/kubernetes/pkg/kubelet/status/testing"
	kubeletutil "k8s.io/kubernetes/pkg/kubelet/util"
	"k8s.io/kubernetes/pkg/probe"
	"k8s.io/utils/exec"
)

// 测试用容器名与 Pod UID。
const (
	testContainerName = "cOnTaInEr_NaMe"
	testPodUID        = "pOd_UiD"
)

// testContainerID 是测试用容器 ID。
var testContainerID = kubecontainer.ContainerID{Type: "test", ID: "cOnTaInEr_Id"}

// getTestRunningStatus 返回容器处于 Running 且 started=true 的 Pod 状态。
func getTestRunningStatus() v1.PodStatus {
	return getTestRunningStatusWithStarted(true)
}

// getTestNotRunningStatus 返回容器处于 Running 但 started=false 的 Pod 状态。
func getTestNotRunningStatus() v1.PodStatus {
	return getTestRunningStatusWithStarted(false)
}

// getTestRunningStatusWithStarted 构造指定 started 标志的运行中 Pod 状态。
func getTestRunningStatusWithStarted(started bool) v1.PodStatus {
	containerStatus := v1.ContainerStatus{
		Name:        testContainerName,
		ContainerID: testContainerID.String(),
	}
	containerStatus.State.Running = &v1.ContainerStateRunning{StartedAt: metav1.Now()}
	containerStatus.Started = &started
	podStatus := v1.PodStatus{
		Phase:             v1.PodRunning,
		ContainerStatuses: []v1.ContainerStatus{containerStatus},
	}
	return podStatus
}

// getTestRunningStatusWithFailedContainer 返回容器已终止且退出码为 1 的 Pod 状态。
func getTestRunningStatusWithFailedContainer() v1.PodStatus {
	return v1.PodStatus{
		Phase: v1.PodRunning,
		ContainerStatuses: []v1.ContainerStatus{{
			Name:        testContainerName,
			ContainerID: testContainerID.String(),
			State: v1.ContainerState{
				Terminated: &v1.ContainerStateTerminated{
					ExitCode: 1,
				},
			},
		}},
	}
}

// getTestRunningStatusWithSucceededContainer 返回容器已终止且退出码为 0 的 Pod 状态。
func getTestRunningStatusWithSucceededContainer() v1.PodStatus {
	return v1.PodStatus{
		Phase: v1.PodRunning,
		ContainerStatuses: []v1.ContainerStatus{{
			Name:        testContainerName,
			ContainerID: testContainerID.String(),
			State: v1.ContainerState{
				Terminated: &v1.ContainerStateTerminated{
					ExitCode: 0,
				},
			},
		}},
	}
}

// getTestPendingStatus 返回容器处于 Waiting 的 Pending Pod 状态。
func getTestPendingStatus() v1.PodStatus {
	return v1.PodStatus{
		Phase: v1.PodPending,
		ContainerStatuses: []v1.ContainerStatus{{
			Name:        testContainerName,
			ContainerID: testContainerID.String(),
			State: v1.ContainerState{
				Waiting: &v1.ContainerStateWaiting{},
			},
		}},
	}
}

// getTestPod 返回带有单个容器、RestartPolicyNever 的测试 Pod。
func getTestPod() *v1.Pod {
	container := v1.Container{
		Name: testContainerName,
	}
	pod := v1.Pod{
		Spec: v1.PodSpec{
			Containers:    []v1.Container{container},
			RestartPolicy: v1.RestartPolicyNever,
		},
	}
	pod.Name = "testPod"
	pod.UID = testPodUID
	return &pod
}

// setTestProbe 把探针设置到对应容器字段，并填入加速测试的默认参数。
func setTestProbe(pod *v1.Pod, probeType probeType, probeSpec v1.Probe) {
	// 所有测试都依赖 fake exec prober。
	probeSpec.ProbeHandler = v1.ProbeHandler{
		Exec: &v1.ExecAction{},
	}

	// 应用测试默认值并调小，以加快测试速度。
	defaults := map[string]int64{
		"TimeoutSeconds":   1,
		"PeriodSeconds":    1,
		"SuccessThreshold": 1,
		"FailureThreshold": 1,
	}
	for field, value := range defaults {
		f := reflect.ValueOf(&probeSpec).Elem().FieldByName(field)
		if f.Int() == 0 {
			f.SetInt(value)
		}
	}

	switch probeType {
	case readiness:
		pod.Spec.Containers[0].ReadinessProbe = &probeSpec
	case liveness:
		pod.Spec.Containers[0].LivenessProbe = &probeSpec
	case startup:
		pod.Spec.Containers[0].StartupProbe = &probeSpec
	}
}

// newTestManager 构造使用 fake 组件与 fake exec prober 的 prober manager。
func newTestManager() *manager {
	podManager := kubepod.NewBasicPodManager()
	podStartupLatencyTracker := kubeletutil.NewPodStartupLatencyTracker()
	// 把测试 Pod 加入 pod manager，便于 status manager 按需获取。
	podManager.AddPod(getTestPod())
	m := NewManager(
		status.NewManager(&fake.Clientset{}, podManager, &statustest.FakePodDeletionSafetyProvider{}, podStartupLatencyTracker),
		results.NewManager(),
		results.NewManager(),
		results.NewManager(),
		nil, // runner
		&record.FakeRecorder{},
	).(*manager)
	// 不真正执行探针。
	m.prober.exec = fakeExecProber{probe.Success, nil}
	return m
}

// newTestWorker 创建绑定测试 Pod 与探针的 worker。
func newTestWorker(m *manager, probeType probeType, probeSpec v1.Probe) *worker {
	pod := getTestPod()
	setTestProbe(pod, probeType, probeSpec)
	return newWorker(m, probeType, pod, pod.Spec.Containers[0])
}

// fakeExecProber 返回预设探针结果。
type fakeExecProber struct {
	result probe.Result
	err    error
}

// Probe 返回预设结果与错误。
func (p fakeExecProber) Probe(c exec.Cmd) (probe.Result, string, error) {
	return p.result, "", p.err
}

// syncExecProber 是并发安全的 fakeExecProber。
type syncExecProber struct {
	sync.RWMutex
	fakeExecProber
}

// set 在锁保护下更新预设结果。
func (p *syncExecProber) set(result probe.Result, err error) {
	p.Lock()
	defer p.Unlock()
	p.result = result
	p.err = err
}

// Probe 在读锁保护下返回结果。
func (p *syncExecProber) Probe(cmd exec.Cmd) (probe.Result, string, error) {
	p.RLock()
	defer p.RUnlock()
	return p.fakeExecProber.Probe(cmd)
}
