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
	"context"
	"net/http"
	"time"

	cadvisorapi "github.com/google/cadvisor/lib/model"
	"go.opentelemetry.io/otel/trace"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/component-base/logs/logreduction"
	internalapi "k8s.io/cri-api/pkg/apis"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/credentialprovider"
	"k8s.io/kubernetes/pkg/kubelet/allocation/state"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/images"
	imagepullmanager "k8s.io/kubernetes/pkg/kubelet/images/pullmanager"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/logs"
	proberesults "k8s.io/kubernetes/pkg/kubelet/prober/results"
	"k8s.io/utils/ptr"
)

// 测试用常量：seccomp 根目录、节点可分配资源与 Pod 日志目录。
const (
	fakeSeccompProfileRoot = "/fakeSeccompProfileRoot"

	fakeNodeAllocatableMemory = "32Gi"
	fakeNodeAllocatableCPU    = "16"

	fakePodLogsDirectory = "/var/log/pods"
)

// fakeHTTP 是可注入错误的 HTTP 客户端桩。
type fakeHTTP struct {
	req *http.Request
	err error
}

// Do 记录请求并返回预设错误，模拟下载失败。
func (f *fakeHTTP) Do(req *http.Request) (*http.Response, error) {
	f.req = req
	return nil, f.err
}

// fakePodStateProvider 用集合模拟 Pod 终止/移除状态。
type fakePodStateProvider struct {
	terminated map[types.UID]struct{}
	removed    map[types.UID]struct{}
}

// newFakePodStateProvider 创建空的 fake 状态提供器。
func newFakePodStateProvider() *fakePodStateProvider {
	return &fakePodStateProvider{
		terminated: make(map[types.UID]struct{}),
		removed:    make(map[types.UID]struct{}),
	}
}

// IsPodTerminationRequested 判断 Pod 是否已被移除。
func (f *fakePodStateProvider) IsPodTerminationRequested(uid types.UID) bool {
	_, found := f.removed[uid]
	return found
}

// ShouldPodRuntimeBeRemoved 判断 Pod 是否已终止。
func (f *fakePodStateProvider) ShouldPodRuntimeBeRemoved(uid types.UID) bool {
	_, found := f.terminated[uid]
	return found
}

// ShouldPodContentBeRemoved 判断 Pod 内容是否应被清理。
func (f *fakePodStateProvider) ShouldPodContentBeRemoved(uid types.UID) bool {
	_, found := f.removed[uid]
	return found
}

// fakePodPullingTimeRecorder 是记录镜像拉取时间的空实现。
type fakePodPullingTimeRecorder struct{}

// RecordImageStartedPulling 空实现，测试中不记录时间。
func (f *fakePodPullingTimeRecorder) RecordImageStartedPulling(podUID types.UID) {}

// RecordImageFinishedPulling 空实现，测试中不记录时间。
func (f *fakePodPullingTimeRecorder) RecordImageFinishedPulling(podUID types.UID) {}

// newFakeKubeRuntimeManager 组装一个可用的 kubeGenericRuntimeManager 测试实例。
func newFakeKubeRuntimeManager(ctx context.Context, runtimeService internalapi.RuntimeService, imageService internalapi.ImageManagerService, machineInfo *cadvisorapi.MachineInfo, osInterface kubecontainer.OSInterface, runtimeHelper kubecontainer.RuntimeHelper, tracer trace.Tracer, recorder *record.FakeRecorder) (*kubeGenericRuntimeManager, error) {
	logger := klog.FromContext(ctx)
	// 用 fake 组件填充所有必需依赖，避免触碰真实容器运行时。
	kubeRuntimeManager := &kubeGenericRuntimeManager{
		recorder:           recorder,
		cpuCFSQuota:        false,
		cpuCFSQuotaPeriod:  metav1.Duration{Duration: time.Millisecond * 100},
		livenessManager:    proberesults.NewManager(),
		startupManager:     proberesults.NewManager(),
		machineInfo:        machineInfo,
		osInterface:        osInterface,
		containerManager:   cm.NewFakeContainerManager(logger),
		runtimeHelper:      runtimeHelper,
		runtimeService:     runtimeService,
		imageService:       imageService,
		seccompProfileRoot: fakeSeccompProfileRoot,
		internalLifecycle:  cm.NewFakeInternalContainerLifecycle(),
		logReduction:       logreduction.NewLogReduction(identicalErrorDelay),
		logManager:         logs.NewStubContainerLogManager(),
		podLogsDirectory:   fakePodLogsDirectory,
		actuatedState:      state.NewStateMemory(logger, nil),
	}

	// 初始化 swap 控制器可用性检查；测试中始终为 false。
	kubeRuntimeManager.getSwapControllerAvailable = func() bool { return false }

	// 从运行时服务读取版本，确定 runtimeName。
	typedVersion, err := runtimeService.Version(ctx, kubeRuntimeAPIVersion)
	if err != nil {
		return nil, err
	}

	// 注入 fake 状态提供器，让 GC 逻辑可预测。
	podStateProvider := newFakePodStateProvider()
	kubeRuntimeManager.containerGC = newContainerGC(runtimeService, podStateProvider, kubeRuntimeManager, tracer)
	kubeRuntimeManager.podStateProvider = podStateProvider
	kubeRuntimeManager.runtimeName = typedVersion.RuntimeName
	// 使用 Noop pull manager 与宽松限流，避免测试中真实拉取镜像。
	kubeRuntimeManager.imagePuller = images.NewImageManager(
		kubecontainer.FilterEventRecorder(recorder),
		&credentialprovider.BasicDockerKeyring{},
		kubeRuntimeManager,
		&imagepullmanager.NoopImagePullManager{},
		flowcontrol.NewBackOff(time.Second, 300*time.Second),
		false,
		ptr.To[int32](0), // No limit on max parallel image pulls,
		0,                // Disable image pull throttling by setting QPS to 0,
		0,
		&fakePodPullingTimeRecorder{},
	)
	kubeRuntimeManager.runner = lifecycle.NewHandlerRunner(
		&fakeHTTP{},
		kubeRuntimeManager,
		kubeRuntimeManager,
		recorder)

	// 固定节点可分配资源，保证驱逐/调度相关测试结果稳定。
	kubeRuntimeManager.getNodeAllocatable = func() v1.ResourceList {
		return v1.ResourceList{
			v1.ResourceMemory: resource.MustParse(fakeNodeAllocatableMemory),
			v1.ResourceCPU:    resource.MustParse(fakeNodeAllocatableCPU),
		}
	}

	return kubeRuntimeManager, nil
}
