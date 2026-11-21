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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	cadvisorapi "github.com/google/cadvisor/lib/model"
	"go.opentelemetry.io/otel/trace"
	grpcstatus "google.golang.org/grpc/status"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubetypes "k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	utilversion "k8s.io/apimachinery/pkg/util/version"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/record"
	ref "k8s.io/client-go/tools/reference"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/component-base/logs/logreduction"
	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	crierror "k8s.io/cri-api/pkg/errors"
	remote "k8s.io/cri-client/pkg"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/credentialprovider"
	"k8s.io/kubernetes/pkg/credentialprovider/plugin"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/allocation"
	"k8s.io/kubernetes/pkg/kubelet/allocation/state"
	kubeletconfiginternal "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/events"
	"k8s.io/kubernetes/pkg/kubelet/images"
	imagepullmanager "k8s.io/kubernetes/pkg/kubelet/images/pullmanager"
	runtimeutil "k8s.io/kubernetes/pkg/kubelet/kuberuntime/util"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/logs"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	proberesults "k8s.io/kubernetes/pkg/kubelet/prober/results"
	"k8s.io/kubernetes/pkg/kubelet/runtimeclass"
	"k8s.io/kubernetes/pkg/kubelet/sysctl"
	"k8s.io/kubernetes/pkg/kubelet/token"
	"k8s.io/kubernetes/pkg/kubelet/types"
	kubeutil "k8s.io/kubernetes/pkg/kubelet/util"
	"k8s.io/kubernetes/pkg/kubelet/util/cache"
	"k8s.io/kubernetes/pkg/kubelet/util/format"
	sc "k8s.io/kubernetes/pkg/securitycontext"
	"k8s.io/utils/ptr"
)

// kuberuntime 管理器使用的常量
const (
	// kubelet 运行时 API 的版本
	kubeRuntimeAPIVersion = "0.1.0"
	// 为避免不必要的 SIGKILL 而保留的最小关闭窗口
	minimumGracePeriodInSeconds = 2

	// 版本缓存的过期时间。
	versionCacheTTL = 60 * time.Second
	// 相同错误重复上报的最小间隔，用于抑制日志刷屏
	identicalErrorDelay = 1 * time.Minute
	// OpenTelemetry 插桩的 scope 名称
	instrumentationScope = "k8s.io/kubernetes/pkg/kubelet/kuberuntime"

	// 记录已 actuate（已实际下发到运行时）的 pod 资源状态的检查点文件名
	actuatedPodsStateFile = "actuated_pods_state"
)

// 包级错误定义
var (
	// 运行时接口的 API 版本不受支持时返回该错误
	ErrVersionNotSupported = errors.New("runtime api version is not supported")
)

// podStateProvider 用于判断 pod 内容（如 volume、日志）或运行时元素（容器、sandbox）
// 是否都已经无需保留，从而允许 GC 安全地清理。
type podStateProvider interface {
	IsPodTerminationRequested(kubetypes.UID) bool
	ShouldPodContentBeRemoved(kubetypes.UID) bool
	ShouldPodRuntimeBeRemoved(kubetypes.UID) bool
}

// PodInitContainerTimeRecorder 记录 pod 首个 init container 的启动时间与最后一个
// init container 的结束时间，用于计算 pod 启动耗时指标。
type PodInitContainerTimeRecorder interface {
	RecordInitContainerStarted(podUID kubetypes.UID, startedAt time.Time)
	RecordInitContainerFinished(podUID kubetypes.UID, finishedAt time.Time)
}

// kubeGenericRuntimeManager 是 kubecontainer.Runtime 基于 CRI 的实现，
// 把 pod 生命周期操作翻译成对 runtime service 与 image service 的 gRPC 调用。
type kubeGenericRuntimeManager struct {
	runtimeName string
	recorder    record.EventRecorderLogger
	osInterface kubecontainer.OSInterface

	// 机器信息（CPU、内存等），用于计算 pod 资源与状态。
	machineInfo *cadvisorapi.MachineInfo

	// 容器 GC 管理器
	containerGC *containerGC

	// 生命周期事件（postStart/preStop 等）的执行器。
	runner kubecontainer.HandlerRunner

	// 包装 kubelet 的 RuntimeHelper，用于生成运行时容器选项。
	runtimeHelper kubecontainer.RuntimeHelper

	// 健康检查结果。
	livenessManager  proberesults.Manager
	readinessManager proberesults.Manager
	startupManager   proberesults.Manager

	// 为 false 时在 cgroups v2 下向容器 cgroup 传入 "memory.oom.group"，
	// 使 OOM killer 把该 cgroup 内进程作为一个整体杀死。
	// 除 linux 外必须为 nil
	singleProcessOOMKill *bool

	// 为 true 时通过 CFS quota 强制执行容器 CPU limit
	cpuCFSQuota bool

	// CPUCFSQuotaPeriod 设置 CPU CFS quota 周期 cpu.cfs_period_us，默认 100ms
	cpuCFSQuotaPeriod metav1.Duration

	// 将应用到本节点上运行 pod 的 Linux 内核参数（sysctls）集合。
	defaultPodSysctls map[string]string

	// 包装后的镜像拉取器。
	imagePuller images.ImageManager

	// gRPC 服务客户端
	runtimeService internalapi.RuntimeService
	imageService   internalapi.ImageManagerService

	// 运行时守护进程的版本缓存，避免每次查询都走一次 CRI 调用。
	versionCache *cache.ObjectCache

	// seccomp profile 所在目录。
	seccompProfileRoot string

	// pod 容器的管理接口（cgroup、cpuset 等）。
	containerManager cm.ContainerManager

	// 用于容器资源管理的内部生命周期事件处理器。
	internalLifecycle cm.InternalContainerLifecycle

	// 管理容器日志。
	logManager logs.ContainerLogManager

	// 管理 RuntimeClass 资源。
	runtimeClassManager *runtimeclass.Manager

	// actuatedState 跟踪已实际下发到运行时的资源，用于原地扩缩容。
	actuatedState state.State

	// 缓存每个容器最近的错误信息，减少重复日志
	logReduction *logreduction.LogReduction

	// PodState provider 实例
	podStateProvider podStateProvider

	// 为所有工作负载使用 RuntimeDefault 作为默认 seccomp profile。
	seccompDefault bool

	// MemorySwapBehavior 定义 swap 的使用方式
	memorySwapBehavior string

	// 获取节点可分配资源的函数
	getNodeAllocatable func() v1.ResourceList

	// MemoryQoS 的内存限流系数
	// 为 nil 时不设置 memory.high（即关闭限流）。
	memoryThrottlingFactor *float64
	// MemoryQoS memory.min 行为对应的内存预留策略
	memoryReservationPolicy kubeletconfiginternal.MemoryReservationPolicy

	// 存放 pod 日志的根目录
	podLogsDirectory string

	// swap 控制器可用性检查函数（仅 Linux）
	// 使用 sync.OnceValue 做惰性初始化
	getSwapControllerAvailable func() bool

	// 记录首个 initContainer 启动时间与最后一个 initContainer 结束时间
	podInitContainerTimeRecorder PodInitContainerTimeRecorder
}

// KubeGenericRuntime 聚合了容器运行时与命令执行所需的接口。
type KubeGenericRuntime interface {
	kubecontainer.Runtime
	kubecontainer.StreamingRuntime
	kubecontainer.CommandRunner
}

// NewKubeGenericRuntimeManager 创建新的 kubeGenericRuntimeManager。
func NewKubeGenericRuntimeManager(
	ctx context.Context,
	recorder record.EventRecorderLogger,
	livenessManager proberesults.Manager,
	readinessManager proberesults.Manager,
	startupManager proberesults.Manager,
	rootDirectory string,
	podLogsDirectory string,
	machineInfo *cadvisorapi.MachineInfo,
	podStateProvider podStateProvider,
	maxPods int32,
	osInterface kubecontainer.OSInterface,
	runtimeHelper kubecontainer.RuntimeHelper,
	insecureContainerLifecycleHTTPClient types.HTTPDoer,
	imageBackOff *flowcontrol.Backoff,
	serializeImagePulls bool,
	maxParallelImagePulls *int32,
	imagePullQPS float32,
	imagePullBurst int,
	imagePullsCredentialVerificationPolicy string,
	preloadedImagesCredentialVerificationWhitelist []string,
	imageCredentialProviderConfigPath string,
	imageCredentialProviderBinDir string,
	singleProcessOOMKill *bool,
	cpuCFSQuota bool,
	cpuCFSQuotaPeriod metav1.Duration,
	defaultPodSysctls map[string]string,
	runtimeService internalapi.RuntimeService,
	imageService internalapi.ImageManagerService,
	containerManager cm.ContainerManager,
	logManager logs.ContainerLogManager,
	runtimeClassManager *runtimeclass.Manager,
	seccompDefault bool,
	memorySwapBehavior string,
	getNodeAllocatable func() v1.ResourceList,
	memoryThrottlingFactor *float64,
	memoryReservationPolicy kubeletconfiginternal.MemoryReservationPolicy,
	podPullingTimeRecorder images.ImagePodPullingTimeRecorder,
	tracerProvider trace.TracerProvider,
	tokenManager *token.Manager,
	getServiceAccount plugin.GetServiceAccountFunc,
	podInitContainerTimeRecorder PodInitContainerTimeRecorder,
) (KubeGenericRuntime, []images.PostImageGCHook, error) {
	logger := klog.FromContext(ctx)

	// 包装 CRI 客户端以采集指标与链路追踪，业务代码无需感知。
	runtimeService = newInstrumentedRuntimeService(runtimeService)
	imageService = newInstrumentedImageManagerService(imageService)
	tracer := tracerProvider.Tracer(instrumentationScope)
	kubeRuntimeManager := &kubeGenericRuntimeManager{
		recorder:                     recorder,
		singleProcessOOMKill:         singleProcessOOMKill,
		cpuCFSQuota:                  cpuCFSQuota,
		cpuCFSQuotaPeriod:            cpuCFSQuotaPeriod,
		defaultPodSysctls:            defaultPodSysctls,
		seccompProfileRoot:           filepath.Join(rootDirectory, "seccomp"),
		livenessManager:              livenessManager,
		readinessManager:             readinessManager,
		startupManager:               startupManager,
		machineInfo:                  machineInfo,
		osInterface:                  osInterface,
		runtimeHelper:                runtimeHelper,
		runtimeService:               runtimeService,
		imageService:                 imageService,
		containerManager:             containerManager,
		internalLifecycle:            containerManager.InternalContainerLifecycle(logger),
		logManager:                   logManager,
		runtimeClassManager:          runtimeClassManager,
		logReduction:                 logreduction.NewLogReduction(identicalErrorDelay),
		seccompDefault:               seccompDefault,
		memorySwapBehavior:           memorySwapBehavior,
		getNodeAllocatable:           getNodeAllocatable,
		memoryThrottlingFactor:       memoryThrottlingFactor,
		memoryReservationPolicy:      memoryReservationPolicy,
		podLogsDirectory:             podLogsDirectory,
		podInitContainerTimeRecorder: podInitContainerTimeRecorder,
	}

	// 用惰性求值初始化 swap 控制器可用性检查，避免启动时阻塞在 cgroup 探测上
	kubeRuntimeManager.getSwapControllerAvailable = initSwapControllerAvailabilityCheck(ctx)

	typedVersion, err := kubeRuntimeManager.getTypedVersion(ctx)
	if err != nil {
		logger.Error(err, "Get runtime version failed")
		return nil, nil, err
	}

	// 当前只支持完全匹配 kubeRuntimeAPIVersion 的运行时
	// TODO: Runtime API machinery is under discussion at https://github.com/kubernetes/kubernetes/issues/28642
	if typedVersion.Version != kubeRuntimeAPIVersion {
		logger.Error(err, "This runtime api version is not supported",
			"apiVersion", typedVersion.Version,
			"supportedAPIVersion", kubeRuntimeAPIVersion)
		return nil, nil, ErrVersionNotSupported
	}

	kubeRuntimeManager.runtimeName = typedVersion.RuntimeName
	logger.Info("Container runtime initialized",
		"containerRuntime", typedVersion.RuntimeName,
		"version", typedVersion.RuntimeVersion,
		"apiVersion", typedVersion.RuntimeApiVersion)

	if imageCredentialProviderConfigPath != "" || imageCredentialProviderBinDir != "" {
		// 凭据插件注册失败属于配置错误，无法在运行期恢复，直接退出
		if err := plugin.RegisterCredentialProviderPlugins(imageCredentialProviderConfigPath, imageCredentialProviderBinDir, tokenManager.GetServiceAccountToken, getServiceAccount); err != nil {
			logger.Error(err, "Failed to register CRI auth plugins")
			os.Exit(1)
		}
	}

	var imageGCHooks []images.PostImageGCHook
	// 默认使用空实现，只有开启 KubeletEnsureSecretPulledImages 时才启用真实的拉取管理器
	var imagePullManager imagepullmanager.ImagePullManager = &imagepullmanager.NoopImagePullManager{}
	if utilfeature.DefaultFeatureGate.Enabled(features.KubeletEnsureSecretPulledImages) {
		imagePullCredentialsVerificationPolicy, err := imagepullmanager.NewImagePullCredentialVerificationPolicy(
			kubeletconfiginternal.ImagePullCredentialsVerificationPolicy(imagePullsCredentialVerificationPolicy),
			preloadedImagesCredentialVerificationWhitelist)

		if err != nil {
			return nil, nil, err
		}

		fsRecordAccessor, err := imagepullmanager.NewFSPullRecordsAccessor(logger, rootDirectory)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to setup the FSPullRecordsAccessor: %w", err)
		}

		// 缓存与锁集合的容量按 Pod 数与并行拉取上限推算，避免无界增长。
		var (
			maxParallelPulls     = ptr.Deref(maxParallelImagePulls, 0)
			intentCacheSize      = max(2*maxPods, 2*maxParallelPulls)
			pullRecordsCacheSize = 5 * maxPods
		)

		memCacheRecordsAccessor := imagepullmanager.NewCachedPullRecordsAccessor(logger, fsRecordAccessor, intentCacheSize, pullRecordsCacheSize, maxParallelPulls)

		imagePullManager, err = imagepullmanager.NewImagePullManager(ctx, memCacheRecordsAccessor, imagePullCredentialsVerificationPolicy, kubeRuntimeManager, maxParallelPulls)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create image pull manager: %w", err)
		}

		// 镜像 GC 时顺带清理已无对应 pod 的拉取记录，防止记录无限增长
		imageGCHooks = append(imageGCHooks, imagePullManager.PruneUnknownRecords)
	}

	nodeKeyring := credentialprovider.NewDefaultDockerKeyring()
	kubeRuntimeManager.imagePuller = images.NewImageManager(
		kubecontainer.FilterEventRecorder(recorder),
		nodeKeyring,
		kubeRuntimeManager,
		imagePullManager,
		imageBackOff,
		serializeImagePulls,
		maxParallelImagePulls,
		imagePullQPS,
		imagePullBurst,
		podPullingTimeRecorder)
	kubeRuntimeManager.runner = lifecycle.NewHandlerRunner(insecureContainerLifecycleHTTPClient, kubeRuntimeManager, kubeRuntimeManager, recorder)
	kubeRuntimeManager.containerGC = newContainerGC(runtimeService, podStateProvider, kubeRuntimeManager, tracer)
	kubeRuntimeManager.podStateProvider = podStateProvider

	// 版本缓存基于机器 ID 作为 key，TTL 到期后才重新查询运行时
	kubeRuntimeManager.versionCache = cache.NewObjectCache(
		func() (interface{}, error) {
			return kubeRuntimeManager.getTypedVersion(ctx)
		},
		versionCacheTTL,
	)

	kubeRuntimeManager.actuatedState, err = state.NewStateCheckpoint(logger, rootDirectory, actuatedPodsStateFile)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize actuated state checkpoint: %w", err)
	}

	return kubeRuntimeManager, imageGCHooks, nil
}

// Type 返回容器运行时的类型名称。
func (m *kubeGenericRuntimeManager) Type() string {
	return m.runtimeName
}

// newRuntimeVersion 优先按语义化版本解析，失败后回退到宽松的通用解析，
// 以兼容形如 "cri-o://1.30" 这类非标准版本串。
func newRuntimeVersion(version string) (*utilversion.Version, error) {
	if ver, err := utilversion.ParseSemantic(version); err == nil {
		return ver, err
	}
	return utilversion.ParseGeneric(version)
}

// getTypedVersion 向运行时查询版本，并强制使用 kubeRuntimeAPIVersion 作为协商版本。
func (m *kubeGenericRuntimeManager) getTypedVersion(ctx context.Context) (*runtimeapi.VersionResponse, error) {
	typedVersion, err := m.runtimeService.Version(ctx, kubeRuntimeAPIVersion)
	if err != nil {
		return nil, fmt.Errorf("get remote runtime typed version failed: %v", err)
	}
	return typedVersion, nil
}

// Version 返回容器运行时的版本信息。
func (m *kubeGenericRuntimeManager) Version(ctx context.Context) (kubecontainer.Version, error) {
	typedVersion, err := m.getTypedVersion(ctx)
	if err != nil {
		return nil, err
	}

	return newRuntimeVersion(typedVersion.RuntimeVersion)
}

// APIVersion 返回缓存的运行时 API 版本信息。实现应定期刷新该缓存，
// 因此该值可能与运行时引擎自身的版本不同。
func (m *kubeGenericRuntimeManager) APIVersion() (kubecontainer.Version, error) {
	versionObject, err := m.versionCache.Get(m.machineInfo.MachineID)
	if err != nil {
		return nil, err
	}
	typedVersion := versionObject.(*runtimeapi.VersionResponse)

	return newRuntimeVersion(typedVersion.RuntimeApiVersion)
}

// Status 返回运行时状态。仅当 Status 调用本身失败时返回错误，否则为 nil。
func (m *kubeGenericRuntimeManager) Status(ctx context.Context) (*kubecontainer.RuntimeStatus, error) {
	resp, err := m.runtimeService.Status(ctx, false)
	if err != nil {
		return nil, err
	}
	if resp.GetStatus() == nil {
		return nil, errors.New("runtime status is nil")
	}
	return toKubeRuntimeStatus(resp.GetStatus(), resp.GetRuntimeHandlers(), resp.GetFeatures()), nil
}

// GetPod 获取指定 pod 当前的 sandbox 与容器列表。
func (m *kubeGenericRuntimeManager) GetPod(ctx context.Context, podUID kubetypes.UID) (*kubecontainer.Pod, error) {
	pods, err := m.getPods(ctx, listOptions{podUID: podUID})
	if err != nil {
		return nil, err
	}

	pod := pods[podUID]
	if pod == nil {
		return nil, kubecontainer.ErrPodNotFound
	}
	return pod, nil
}

// GetPods 返回按 pod 分组后的容器列表。all 参数决定是否包含已经退出、死亡的容器
// （垃圾回收会用到）。
func (m *kubeGenericRuntimeManager) GetPods(ctx context.Context, all bool) ([]*kubecontainer.Pod, error) {
	logger := klog.FromContext(ctx)
	pods, err := m.getPods(ctx, listOptions{onlyRunningReady: !all})
	if err != nil {
		return nil, err
	}

	// map 转 list。
	var result []*kubecontainer.Pod
	for _, pod := range pods {
		result = append(result, pod)
	}

	// 某些场景下多个同名 pod 会并行存在，因为其中一个尚未完全终止。
	// 为避免基于容器名查找时出现意外行为（例如调用 *Kubelet.findContainer()
	// 时未指定 pod ID），这里按创建时间排序返回。
	sort.SliceStable(result, func(i, j int) bool {
		return result[i].CreatedAt > result[j].CreatedAt
	})
	logger.V(4).Info("Retrieved pods from runtime", "all", all)
	return result, nil
}

// getPods 是 GetPod/GetPods 的公共实现：先列 sandbox 再列容器，并按 pod UID 聚合。
func (m *kubeGenericRuntimeManager) getPods(ctx context.Context, opts listOptions) (map[kubetypes.UID]*kubecontainer.Pod, error) {
	logger := klog.FromContext(ctx)
	pods := make(map[kubetypes.UID]*kubecontainer.Pod)
	timestamp := time.Now()
	sandboxes, err := m.getSandboxes(ctx, opts)
	if err != nil {
		return nil, err
	}
	// 按创建时间排序，最新的在前。
	sort.Sort(podSandboxByCreatedThenID(sandboxes))
	for i := range sandboxes {
		s := sandboxes[i]
		if s.Metadata == nil {
			logger.V(4).Info("Sandbox does not have metadata", "sandbox", s)
			continue
		}
		podUID := kubetypes.UID(s.Metadata.Uid)
		if _, ok := pods[podUID]; !ok {
			pods[podUID] = &kubecontainer.Pod{
				ID:        podUID,
				Name:      s.Metadata.Name,
				Namespace: s.Metadata.Namespace,
				Timestamp: timestamp,
			}
		}
		p := pods[podUID]
		converted, err := m.sandboxToKubeContainer(s)
		if err != nil {
			logger.V(4).Info("Convert sandbox of pod failed", "runtimeName", m.runtimeName, "sandbox", s, "podUID", podUID, "err", err)
			continue
		}
		p.Sandboxes = append(p.Sandboxes, converted)
		p.CreatedAt = uint64(s.GetCreatedAt())
	}

	containers, err := m.getContainers(ctx, opts)
	if err != nil {
		return nil, err
	}
	// 容器也按创建时间排序：最新在前，同一时刻再按容器 ID 保证稳定。
	// 某些场景下多个同名 pod 会并行存在，因为其中一个尚未完全终止。
	// 为避免基于容器名查找时出现意外行为（例如调用 *Kubelet.findContainer()
	// 时未指定 pod ID），这里按创建时间排序。
	sort.Sort(containerByCreatedThenID(containers))
	for i := range containers {
		c := containers[i]
		if c.Metadata == nil {
			logger.V(4).Info("Container does not have metadata", "container", c)
			continue
		}

		labelledInfo := getContainerInfoFromLabels(ctx, c.Labels)
		pod, found := pods[labelledInfo.PodUID]
		if !found {
			pod = &kubecontainer.Pod{
				ID:        labelledInfo.PodUID,
				Name:      labelledInfo.PodName,
				Namespace: labelledInfo.PodNamespace,
				Timestamp: timestamp,
			}
			pods[labelledInfo.PodUID] = pod
		}

		converted, err := m.toKubeContainer(ctx, c)
		if err != nil {
			logger.V(4).Info("Convert container of pod failed", "runtimeName", m.runtimeName, "container", c, "podUID", labelledInfo.PodUID, "err", err)
			continue
		}

		pod.Containers = append(pod.Containers, converted)
	}

	return pods, nil
}

// containerKillReason 说明容器被杀死的原因
type containerKillReason string

// 容器被杀原因的全部取值
const (
	// 启动探针失败导致重启
	reasonStartupProbe containerKillReason = "StartupProbe"
	// 存活探针失败导致重启
	reasonLivenessProbe containerKillReason = "LivenessProbe"
	// postStart 钩子失败
	reasonFailedPostStartHook containerKillReason = "FailedPostStartHook"
	// 因 RestartAllContainers 而重置
	reasonRestartAllContainers containerKillReason = "RestartAllContainers"
	// 原因未知
	reasonUnknown containerKillReason = "Unknown"
)

// containerToKillInfo 保存杀死某个容器所需的信息。
type containerToKillInfo struct {
	// 容器 spec。
	container *v1.Container
	// 容器名。
	name string
	// 说明容器为何将被杀死。
	message string
	// reason 是容器被杀原因更清晰的来源
	// TODO: replace message with reason?
	reason containerKillReason
}

// containerToUpdateInfo 保存更新容器资源所需的信息。
type containerToUpdateInfo struct {
	// 容器 spec。
	container *v1.Container
	// 需要更新资源的运行时容器 ID
	kubeContainerID kubecontainer.ContainerID
	// 运行中容器期望的资源
	desiredContainerResources resourceRequirements
	// 运行中容器最近一次配置的资源
	currentContainerResources *resourceRequirements
}

// containerToRemoveInfo 保存从运行时中移除容器所需的信息。
type containerToRemoveInfo struct {
	// 容器 ID。
	containerID kubecontainer.ContainerID
	// 容器 spec。
	container *v1.Container
	// 移除前是否先杀死容器。
	kill bool
}

// podActions 记录针对某个 pod 需要执行的动作。
type podActions struct {
	// 停止 pod 的所有运行中容器（普通、init、ephemeral）以及 sandbox。
	KillPod bool
	// 是否需要新建 sandbox。若需要先杀 pod 再新建 sandbox，
	// 则所有 init 容器都必须清除（purge）。
	CreateSandbox bool
	// 已存在 sandbox 的 ID，用于启动 ContainersToStart 中的容器。
	SandboxID string
	// 为该 pod 创建 sandbox 的尝试次数。
	Attempt uint32

	// InitContainersToStart 保存要启动的 init 容器索引列表，
	// 索引对应 pod.Spec.InitContainers 中的位置。
	InitContainersToStart []int
	// ContainersToStart 保存要启动的容器索引列表，
	// 索引对应 pod.Spec.Containers 中的位置。
	ContainersToStart []int
	// ContainersToKill 保存需要杀死的容器，key 是容器 ID，
	// value 是杀死该容器所需的信息。
	ContainersToKill map[kubecontainer.ContainerID]containerToKillInfo
	// EphemeralContainersToStart 保存要启动的 ephemeral 容器索引列表，
	// 索引对应 pod.Spec.EphemeralContainers 中的位置。
	EphemeralContainersToStart []int
	// ContainersToUpdate 保存需要更新资源的容器列表。
	// 容器资源更新仅适用于 CPU 和内存。
	ContainersToUpdate map[v1.ResourceName][]containerToUpdateInfo
	// VolumesToUpsize 保存需要扩容的 volume 列表。
	VolumesToUpsize []v1.Volume
	// VolumesToDownsize 保存需要缩容的 volume 列表。
	VolumesToDownsize []v1.Volume
	// UpdatePodResources 为 true 表示容器需要通过重启来更新资源。
	UpdatePodResources bool
	// ContainersToReset 保存为 RestartAllContainers 需要杀死（若在运行）
	// 并从运行时移除的容器。触发 RestartAllContainers 的容器最后才重置。
	ContainersToReset []containerToRemoveInfo
	// UpdatePodLevelResources 为 true 表示需要更新 pod 级别资源
	UpdatePodLevelResources bool
}

// resourceRequirements 归纳在 kuberuntime 语境下适用于运行中 pod 或容器的资源集合。
type resourceRequirements struct {
	memoryLimit   int64
	memoryRequest int64
	cpuLimit      int64
	cpuRequest    int64
}

// String 输出 podActions 的可读形式，便于日志排查。
func (p podActions) String() string {
	return fmt.Sprintf("KillPod: %t, CreateSandbox: %t, UpdatePodResources: %t, UpdatePodLevelResources: %t, Attempt: %d, InitContainersToStart: %v, ContainersToStart: %v, EphemeralContainersToStart: %v, ContainersToUpdate: %v, VolumesToUpsize: %v, VolumesToDownsize: %v, ContainersToKill: %v, ContainersToRemove: %v",
		p.KillPod, p.CreateSandbox, p.UpdatePodResources, p.UpdatePodLevelResources, p.Attempt, p.InitContainersToStart, p.ContainersToStart, p.EphemeralContainersToStart, p.ContainersToUpdate, p.VolumesToUpsize, p.VolumesToDownsize, p.ContainersToKill, p.ContainersToReset)
}

// containerChanged 根据会影响容器运行的字段判断容器是否发生变化。
// 目前只有 `image` 和 `name` 字段。
// 这里无需考虑 pod UID，因为我们是按 pod UID 查找 containerStatus 的：
// 一旦 pod UID 改变，就找不到对应的 containerStatus 做比较了。
func containerChanged(container *v1.Container, containerStatus *kubecontainer.Status) (uint64, uint64, bool) {
	expectedHash := kubecontainer.HashContainer(container)
	return expectedHash, containerStatus.Hash, containerStatus.Hash != expectedHash
}

// shouldRestartOnFailure 判断 pod 退出后是否应重启（Never 策略除外）。
func shouldRestartOnFailure(pod *v1.Pod) bool {
	return pod.Spec.RestartPolicy != v1.RestartPolicyNever
}

// containerSucceeded 判断容器是否已以退出码 0 正常结束。
func containerSucceeded(c *v1.Container, podStatus *kubecontainer.PodStatus) bool {
	cStatus := podStatus.FindContainerStatusByName(c.Name)
	if cStatus == nil {
		return false
	}
	// 容器已退出，且退出码为 0。
	return cStatus.State == kubecontainer.ContainerStateExited && cStatus.ExitCode == 0
}

// containerResourcesFromRequirements 把容器资源换算成 kuberuntime 内部使用的数值形式；
// 开启 pod 级资源特性时，limit 还要与 pod 级 limit 取较小值，保证容器不超过 pod 上限。
func containerResourcesFromRequirements(podRequirements, containerRequirements *v1.ResourceRequirements) resourceRequirements {
	resources := resourceRequirements{
		memoryLimit:   containerRequirements.Limits.Memory().Value(),
		memoryRequest: containerRequirements.Requests.Memory().Value(),
		cpuLimit:      containerRequirements.Limits.Cpu().MilliValue(),
		cpuRequest:    containerRequirements.Requests.Cpu().MilliValue(),
	}

	if !utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodLevelResourcesVerticalScaling) {
		return resources
	}

	// pod 级 limit 可能比容器自身 limit 更严格，取两者的有效值
	containerLimits := kubeutil.GetLimits(&kubeutil.ResourceOpts{PodResources: podRequirements, ContainerResources: containerRequirements})
	resources.memoryLimit = containerLimits.Memory().Value()
	resources.cpuLimit = containerLimits.Cpu().MilliValue()
	return resources
}

// podResourcesFromRequirements 把 pod 级资源换算成内部数值形式；nil 视为全零。
func podResourcesFromRequirements(requirements *v1.ResourceRequirements) resourceRequirements {
	if requirements == nil {
		return resourceRequirements{}
	}

	return resourceRequirements{
		memoryLimit:   requirements.Limits.Memory().Value(),
		memoryRequest: requirements.Requests.Memory().Value(),
		cpuLimit:      requirements.Limits.Cpu().MilliValue(),
		cpuRequest:    requirements.Requests.Cpu().MilliValue(),
	}
}

// computePodResizeAction 判断对指定容器做原地扩缩容需要哪些动作。
// 返回是否保留（true）或重启（false）该容器。
// TODO(vibansal): Make this function to be agnostic to whether it is dealing with a restartable init container or not (i.e. remove the argument `isRestartableInitContainer`).
func (m *kubeGenericRuntimeManager) computePodResizeAction(ctx context.Context, pod *v1.Pod, containerIdx int, initContainer bool, kubeContainerStatus *kubecontainer.Status, changes *podActions) (keepContainer bool) {
	logger := klog.FromContext(ctx)
	// 未开启原地扩缩容或 pod 不可调整时，直接保留容器
	if resizable, _, _ := allocation.IsInPlacePodVerticalScalingAllowed(pod); !resizable {
		return true
	}

	var container v1.Container
	if initContainer {
		container = pod.Spec.InitContainers[containerIdx]
	} else {
		container = pod.Spec.Containers[containerIdx]
	}

	// 通过比较 v1.Spec.Resources（期望值）与 v1.Status.Resources / runtime.Status.Resources
	// （最近一次已知实际值）来判断 *运行中* 容器是否需要更新资源。
	// 仅在 kubelet 已接受该 resize（即 v1.Spec.Resources.Requests == v1.Status.AllocatedResources）时继续。
	// 若运行时 containerID 与 pod.Status 中的 containerID 不一致（容器正在重启），则跳过。
	if kubeContainerStatus.State != kubecontainer.ContainerStateRunning {
		return true
	}

	actuatedContainerResources, found := m.actuatedState.GetContainerResources(pod.UID, container.Name)
	if !found {
		logger.Error(nil, "Missing actuated resource record", "pod", klog.KObj(pod), "container", container.Name)
		// 以零值 actuated 资源继续。对 restart NotRequired 来说可能多一次
		// UpdateContainerResources 调用，但该调用应当幂等。
		// 对 RestartContainer 来说可能会触发一次容器重启。
	}

	var actuatedPodResources *v1.ResourceRequirements
	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodLevelResourcesVerticalScaling) {
		actuatedPodResources, _ = m.actuatedState.GetPodLevelResources(pod.UID)
	}

	desiredResources := containerResourcesFromRequirements(pod.Spec.Resources, &container.Resources)
	currentResources := containerResourcesFromRequirements(actuatedPodResources, &actuatedContainerResources)

	if currentResources == desiredResources {
		// 无需 resize。
		return true
	}

	// 按容器 ResizePolicy 决定某类资源是否需要更新、以及是否需要重启容器。
	determineContainerResize := func(rName v1.ResourceName, desiredValue, currentValue int64) (resize, restart bool) {
		if desiredValue == currentValue {
			return false, false
		}
		for _, policy := range container.ResizePolicy {
			if policy.ResourceName == rName {
				return true, policy.RestartPolicy == v1.RestartContainer
			}
		}
		// 未设置 ResizePolicy 时，隐式默认为 NotRequired。
		return true, false
	}
	markContainerForUpdate := func(rName v1.ResourceName, desiredValue, currentValue int64) {
		cUpdateInfo := containerToUpdateInfo{
			container:                 &container,
			kubeContainerID:           kubeContainerStatus.ID,
			desiredContainerResources: desiredResources,
			currentContainerResources: &currentResources,
		}
		// 调整容器更新顺序，保证缩容先于扩容执行
		switch {
		// 扩容追加到尾部，缩容插入到头部，确保先缩小后扩大。
		case desiredValue > currentValue:
			changes.ContainersToUpdate[rName] = append(changes.ContainersToUpdate[rName], cUpdateInfo)
		case desiredValue < currentValue:
			changes.ContainersToUpdate[rName] = append(changes.ContainersToUpdate[rName], containerToUpdateInfo{})
			copy(changes.ContainersToUpdate[rName][1:], changes.ContainersToUpdate[rName])
			changes.ContainersToUpdate[rName][0] = cUpdateInfo
		}
	}
	resizeMemLim, restartMemLim := determineContainerResize(v1.ResourceMemory, desiredResources.memoryLimit, currentResources.memoryLimit)
	resizeMemReq, restartMemReq := determineContainerResize(v1.ResourceMemory, desiredResources.memoryRequest, currentResources.memoryRequest)
	resizeCPULim, restartCPULim := determineContainerResize(v1.ResourceCPU, desiredResources.cpuLimit, currentResources.cpuLimit)
	resizeCPUReq, restartCPUReq := determineContainerResize(v1.ResourceCPU, desiredResources.cpuRequest, currentResources.cpuRequest)
	if restartCPULim || restartCPUReq || restartMemLim || restartMemReq {
		// resize 策略要求该容器重启
		changes.ContainersToKill[kubeContainerStatus.ID] = containerToKillInfo{
			name:      kubeContainerStatus.Name,
			container: &container,
			message:   fmt.Sprintf("Container %s resize requires restart", container.Name),
		}
		if initContainer {
			// 校验逻辑会阻止把不可重启的 init 容器标记为重启，
			// 因此可以确定这个 init 容器是可重启的。
			changes.InitContainersToStart = append(changes.InitContainersToStart, containerIdx)
		} else {
			changes.ContainersToStart = append(changes.ContainersToStart, containerIdx)
		}
		changes.UpdatePodResources = true
		return false
	} else {
		if resizeMemLim {
			markContainerForUpdate(v1.ResourceMemory, desiredResources.memoryLimit, currentResources.memoryLimit)
		} else if resizeMemReq {
			markContainerForUpdate(v1.ResourceMemory, desiredResources.memoryRequest, currentResources.memoryRequest)
		}
		if resizeCPULim {
			markContainerForUpdate(v1.ResourceCPU, desiredResources.cpuLimit, currentResources.cpuLimit)
		} else if resizeCPUReq {
			markContainerForUpdate(v1.ResourceCPU, desiredResources.cpuRequest, currentResources.cpuRequest)
		}
	}
	return true
}

// InitializeActuatedPod 在首次调度时把已分配资源写入 actuated 检查点，
// 作为后续 resize 比较的基线；已有记录则不覆盖。
func (m *kubeGenericRuntimeManager) InitializeActuatedPod(logger klog.Logger, allocatedPod *v1.Pod) {
	if !utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling) {
		return
	}
	if _, ok := m.actuatedState.GetPodResourceInfo(allocatedPod.UID); ok {
		return
	}
	info := allocation.ResourceInfoForPod(allocatedPod)
	if err := m.actuatedState.SetPodResourceInfo(logger, allocatedPod.UID, info); err != nil {
		logger.Error(err, "Failed to initialize actuated pod resource info checkpoint", "pod", klog.KObj(allocatedPod))
	}
}

// getActuatedEmptyDirVolumeLimit 读取已 actuate 的 emptyDir 大小限制；缺失时返回 nil 并记录日志。
func (m *kubeGenericRuntimeManager) getActuatedEmptyDirVolumeLimit(logger klog.Logger, pod *v1.Pod, volName string) *resource.Quantity {
	if size, found := m.actuatedState.GetEmptyDirVolumeLimit(pod.UID, volName); found {
		return size
	}
	logger.Error(nil, "Failed to read actuated empty dir volume limit", "pod", klog.KObj(pod), "volume", volName)
	return nil
}

// computeVolumeResizeAction 判断 volume 需要执行哪些扩容/缩容动作。
func (m *kubeGenericRuntimeManager) computeVolumeResizeAction(ctx context.Context, pod *v1.Pod, changes *podActions) {
	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingMemoryBackedVolumes) {
		logger := klog.FromContext(ctx)
		for _, vol := range pod.Spec.Volumes {
			if !allocation.VolHasMemoryBackedEmptyDirSizeLimit(&vol) {
				continue
			}
			actuatedSize := m.getActuatedEmptyDirVolumeLimit(logger, pod, vol.Name)
			desiredSize := vol.EmptyDir.SizeLimit

			if actuatedSize == nil {
				// 若 actuation 检查点缺失，强制触发一次 sync 以写入 limit 与检查点。
				// 这里按扩容处理（顺序更安全），确保 cgroup limit 先被扩大。
				changes.VolumesToUpsize = append(changes.VolumesToUpsize, vol)
				continue
			}

			// 比较当前（已 actuate）大小与新大小
			cmp := desiredSize.Cmp(*actuatedSize)
			if cmp > 0 {
				changes.VolumesToUpsize = append(changes.VolumesToUpsize, vol)
			} else if cmp < 0 {
				changes.VolumesToDownsize = append(changes.VolumesToDownsize, vol)
			}
		}
	}
}

// doPodResizeAction 执行原地扩缩容：按依赖顺序调整 volume、内存 cgroup、CPU cgroup，
// 并把结果同步给运行时。任何一步失败都会中止，由后续 SyncPod 重试剩余部分。
func (m *kubeGenericRuntimeManager) doPodResizeAction(ctx context.Context, pod *v1.Pod, podStatus *kubecontainer.PodStatus, podContainerChanges podActions) *kubecontainer.SyncResult {
	logger := klog.FromContext(ctx)
	start := time.Now()
	success := false
	// 无论成功与否都记录耗时，便于观测 resize 的尾部延迟
	defer func() {
		metrics.PodResizeDurationMilliseconds.WithLabelValues(strconv.FormatBool(success)).Observe(float64(time.Since(start).Milliseconds()))
	}()

	resizeResult := kubecontainer.NewSyncResult(kubecontainer.ResizePodInPlace, format.Pod(pod))
	pcm := m.containerManager.NewPodContainerManager()
	enforceCPULimits := m.cpuCFSQuota
	// 独占 CPU 场景下 CFS quota 会干扰绑核，特性开启时关闭 quota
	if utilfeature.DefaultFeatureGate.Enabled(features.DisableCPUQuotaWithExclusiveCPUs) && m.containerManager.PodHasExclusiveCPUs(logger, pod) {
		enforceCPULimits = false
		logger.V(2).Info("Disabled CFS quota", "pod", klog.KObj(pod))
	}
	podResources := cm.ResourceConfigForPod(pod, enforceCPULimits, uint64((m.cpuCFSQuotaPeriod.Duration)/time.Microsecond), m.isMemoryQoSEnforced(), m.memoryReservationPolicy)
	if podResources == nil {
		logger.Error(nil, "Unable to get resource configuration", "pod", klog.KObj(pod))
		resizeResult.Fail(kubecontainer.ErrResizePodInPlace, fmt.Sprintf("unable to get resource configuration processing resize for pod %q", format.Pod(pod)))
		return resizeResult
	}
	if m.isMemoryQoSEnforced() {
		m.applyPodLevelMemoryHigh(pod, podResources)
	}
	currentPodMemoryConfig, err := pcm.GetPodCgroupConfig(pod, v1.ResourceMemory)
	if err != nil {
		logger.Error(err, "Unable to get pod cgroup memory config", "pod", klog.KObj(pod))
		resizeResult.Fail(kubecontainer.ErrResizePodInPlace, fmt.Sprintf("unable to get pod cgroup memory config for pod %q", format.Pod(pod)))
		return resizeResult
	}
	currentPodCPUConfig, err := pcm.GetPodCgroupConfig(pod, v1.ResourceCPU)
	if err != nil {
		logger.Error(err, "Unable to get pod cgroup cpu config", "pod", klog.KObj(pod))
		resizeResult.Fail(kubecontainer.ErrResizePodInPlace, fmt.Sprintf("unable to get pod cgroup cpu config for pod %q", format.Pod(pod)))
		return resizeResult
	}

	currentPodResources := podResources
	currentPodResources = mergeResourceConfig(currentPodResources, currentPodMemoryConfig)
	currentPodResources = mergeResourceConfig(currentPodResources, currentPodCPUConfig)

	// 真正执行 resize 前先做一次尽力而为的可行性校验，避免出现部分 resize 的中间状态。
	if err := m.validatePodResizeAction(ctx, pod, podStatus, currentPodResources, podResources, podContainerChanges); err != nil {
		logger.Error(err, "Allocated pod resize is not currently feasible", "pod", klog.KObj(pod))
		resizeResult.Fail(kubecontainer.ErrResizePodInPlace, err.Error())
		return resizeResult
	}

	updateActuatedPodLevelResources := func(resourceName v1.ResourceName) error {
		allocatedResources := pod.Spec.Resources
		if allocatedResources == nil {
			return nil
		}

		// allocated 资源不会为 nil
		actuatedPodResources, found := m.actuatedState.GetPodLevelResources(pod.UID)
		if !found || actuatedPodResources == nil {
			logger.Error(nil, "Missing actuated resource record", "pod", klog.KObj(pod), "pod", pod.Name)
			// 以零值 actuated 资源继续。
			actuatedPodResources = &v1.ResourceRequirements{}
		}

		defaultResourceListIfNil := func(rl v1.ResourceList) v1.ResourceList {
			if rl == nil {
				return make(v1.ResourceList)
			}

			return rl
		}

		switch resourceName {
		case v1.ResourceMemory:
			if allocatedResources.Requests != nil {
				actuatedPodResources.Requests = defaultResourceListIfNil(actuatedPodResources.Requests)
				actuatedPodResources.Requests[v1.ResourceMemory] = allocatedResources.Requests[v1.ResourceMemory]
			}
			if allocatedResources.Limits != nil {
				actuatedPodResources.Limits = defaultResourceListIfNil(actuatedPodResources.Limits)
				actuatedPodResources.Limits[v1.ResourceMemory] = allocatedResources.Limits[v1.ResourceMemory]
			}
		case v1.ResourceCPU:
			if allocatedResources.Requests != nil {
				actuatedPodResources.Requests = defaultResourceListIfNil(actuatedPodResources.Requests)
				actuatedPodResources.Requests[v1.ResourceCPU] = allocatedResources.Requests[v1.ResourceCPU]
			}
			if allocatedResources.Limits != nil {
				actuatedPodResources.Limits = defaultResourceListIfNil(actuatedPodResources.Limits)
				actuatedPodResources.Limits[v1.ResourceCPU] = allocatedResources.Limits[v1.ResourceCPU]
			}

		}
		if err = m.actuatedState.SetPodLevelResources(logger, pod.UID, actuatedPodResources); err != nil {
			logger.Error(err, "SetPodLevelResources failed", "pod", pod.Name, "UID", pod.UID,
				"pod", format.Pod(pod), "resourceName", resourceName)
			return err
		}
		return nil
	}

	setPodCgroupConfig := func(logger klog.Logger, rName v1.ResourceName, setLimitValue bool) error {
		var err error
		resizedResources := &cm.ResourceConfig{}
		switch rName {
		case v1.ResourceCPU:
			if setLimitValue {
				resizedResources.CPUPeriod = podResources.CPUPeriod
				resizedResources.CPUQuota = podResources.CPUQuota
			} else {
				resizedResources.CPUShares = podResources.CPUShares
			}
		case v1.ResourceMemory:
			if !setLimitValue {
				// memory request 不会写入 cgroup。
				return nil
			}
			resizedResources.Memory = podResources.Memory
			resizedResources.Unified = podResources.Unified
		}

		// 先通知运行时。若失败说明运行时拒绝了这次 resize。
		mergedPodResources := mergeResourceConfig(currentPodResources, resizedResources)
		if err = m.updatePodSandboxResources(ctx, podContainerChanges.SandboxID, pod, mergedPodResources); err != nil {
			return fmt.Errorf("failed to notify runtime for UpdatePodSandboxResources (resource=%s); resize rejected: %w", rName, err)
		}

		// 再实际调整 pod 级 cgroup。
		err = pcm.SetPodCgroupConfig(logger, pod, resizedResources)
		if err != nil {
			logger.Error(err, "Failed to set cgroup config", "resource", rName, "pod", klog.KObj(pod))
			// cgroup 调整失败时通知运行时回滚到之前的资源值，保持两侧一致。
			if rollbackErr := m.updatePodSandboxResources(ctx, podContainerChanges.SandboxID, pod, currentPodResources); rollbackErr != nil {
				logger.Error(rollbackErr, "Failed to notify runtime to rollback UpdatePodSandboxResources", "resource", rName, "pod", klog.KObj(pod))
			}
			return err
		}

		// 更新本地跟踪的当前状态。
		currentPodResources = mergedPodResources
		return nil
	}

	// 内存、CPU 与 volume 大小按协调后的顺序更新：
	// 1. Volume 缩容（先降低 tmpfs 占用，释放 page cache）。
	// 2. 内存 limit 与 request（调整内存 cgroup 配置）。
	// 3. Volume 扩容（在 cgroup 内存容量扩大后再安全提升 tmpfs 上限）。
	// 4. CPU limit 与 request（调整 cpu cgroup 配置）
	// 内存与 CPU 分开更新，因为二者的 resize 顺序可能不同。
	// 若 resize 导致 pod 资源净增，先设置 pod cgroup 再调整容器；
	// 若净减，则先调整容器再设置 pod cgroup。
	// 任一步出错即中止，未完成的部分留给后续 syncpod 迭代重试。
	updateVolumeSize := func(volumes []v1.Volume) error {
		if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingMemoryBackedVolumes) {
			for _, vol := range volumes {
				desiredSize := vol.EmptyDir.SizeLimit
				if err := m.runtimeHelper.ResizeEphemeralVolume(pod, vol.Name, desiredSize); err != nil {
					return fmt.Errorf("failed to resize volume %s: %w", vol.Name, err)
				}
				if err := m.actuatedState.SetEmptyDirVolumeLimit(pod.UID, vol.Name, desiredSize); err != nil {
					return fmt.Errorf("failed to update actuated emptyDir volume limit checkpoint for %s: %w", vol.Name, err)
				}
			}
		}
		return nil
	}

	resizeContainers := func(rName v1.ResourceName, currPodCgLimValue, newPodCgLimValue, currPodCgReqValue, newPodCgReqValue int64) error {
		var err error
		// 扩容时先扩 limit 再扩 request，保持 "requests <= limits"。
		if newPodCgLimValue > currPodCgLimValue {
			// TODO: Pass logger from context once contextual logging migration is complete
			if err = setPodCgroupConfig(klog.TODO(), rName, true); err != nil {
				return err
			}
		}
		if newPodCgReqValue > currPodCgReqValue {
			// TODO: Pass logger from context once contextual logging migration is complete
			if err = setPodCgroupConfig(klog.TODO(), rName, false); err != nil {
				return err
			}
		}
		if len(podContainerChanges.ContainersToUpdate[rName]) > 0 {
			if err = m.updatePodContainerResources(ctx, pod, rName, podContainerChanges.ContainersToUpdate[rName]); err != nil {
				logger.Error(err, "updatePodContainerResources failed", "pod", format.Pod(pod), "resource", rName)
				return err
			}
		}

		// 缩容时先缩 request 再缩 limit，保持 "requests <= limits"。
		if newPodCgReqValue < currPodCgReqValue {
			// TODO: Pass logger from context once contextual logging migration is complete
			if err = setPodCgroupConfig(klog.TODO(), rName, false); err != nil {
				return err
			}
		}
		if newPodCgLimValue < currPodCgLimValue {
			// TODO(#127825): Pass logger from context once contextual logging migration is complete
			if err = setPodCgroupConfig(klog.TODO(), rName, true); err != nil {
				return err
			}
		}

		if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodLevelResourcesVerticalScaling) && podContainerChanges.UpdatePodLevelResources {
			if err = updateActuatedPodLevelResources(rName); err != nil {
				logger.Error(err, "Failed to update pod-level actuated resources", "resource", rName, "pod", klog.KObj(pod))
			}
		}
		return err
	}

	// 无论是否有 resize 错误，都要请求一次 pod 状态重检，
	// 因为 resize 可能已被部分 actuate。
	defer m.runtimeHelper.RequestPodReinspect(pod.UID)

	// 1. Volume 缩容
	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingMemoryBackedVolumes) {
		if err := updateVolumeSize(podContainerChanges.VolumesToDownsize); err != nil {
			resizeResult.Fail(kubecontainer.ErrResizePodInPlace, err.Error())
			return resizeResult
		}
	}

	// 2. 内存 cgroup
	if len(podContainerChanges.ContainersToUpdate[v1.ResourceMemory]) > 0 || podContainerChanges.UpdatePodResources || podContainerChanges.UpdatePodLevelResources {
		if podResources.Memory == nil {
			// 未设置 pod 内存 limit 时沿用当前值，避免把它意外改成“无限制”。
			// TODO(#128675): This does not support removing limits.
			podResources.Memory = currentPodMemoryConfig.Memory
		}
		if errResize := resizeContainers(v1.ResourceMemory, int64(*currentPodMemoryConfig.Memory), *podResources.Memory, 0, 0); errResize != nil {
			resizeResult.Fail(kubecontainer.ErrResizePodInPlace, errResize.Error())
			return resizeResult
		}
	}

	// 3. Volume 扩容
	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingMemoryBackedVolumes) {
		if err := updateVolumeSize(podContainerChanges.VolumesToUpsize); err != nil {
			resizeResult.Fail(kubecontainer.ErrResizePodInPlace, err.Error())
			return resizeResult
		}
	}

	// 4. CPU cgroup
	if len(podContainerChanges.ContainersToUpdate[v1.ResourceCPU]) > 0 || podContainerChanges.UpdatePodResources || podContainerChanges.UpdatePodLevelResources {
		if podResources.CPUShares == nil {
			// 理论上不应发生：ResourceConfigForPod 总会为 CPUShares 返回非 nil 值。
			logger.Error(nil, "podResources.CPUShares is nil", "pod", pod.Name)
			resizeResult.Fail(kubecontainer.ErrResizePodInPlace, fmt.Sprintf("podResources.CPUShares is nil for pod %s", pod.Name))
			return resizeResult
		}

		// 未设置 pod CPU limit 时沿用当前 CPUQuota，避免 pod limit 被意外更新。
		// TODO(#128675): This does not support removing limits.
		if podResources.CPUQuota == nil {
			podResources.CPUQuota = currentPodCPUConfig.CPUQuota
		}
		if errResize := resizeContainers(v1.ResourceCPU, *currentPodCPUConfig.CPUQuota, *podResources.CPUQuota,
			int64(*currentPodCPUConfig.CPUShares), int64(*podResources.CPUShares)); errResize != nil {
			resizeResult.Fail(kubecontainer.ErrResizePodInPlace, errResize.Error())
			return resizeResult
		}
	}

	success = true
	return resizeResult
}

// validatePodResizeAction 检查待执行的 resize 动作当前是否可行。
func (m *kubeGenericRuntimeManager) validatePodResizeAction(
	ctx context.Context,
	pod *v1.Pod,
	podStatus *kubecontainer.PodStatus,
	currentPodResources, desiredPodResources *cm.ResourceConfig,
	podContainerChanges podActions,
) error {
	if len(podContainerChanges.ContainersToUpdate[v1.ResourceMemory]) > 0 || podContainerChanges.UpdatePodResources {
		return m.validateMemoryResizeAction(ctx, pod, podStatus, currentPodResources, desiredPodResources, podContainerChanges)
	}

	return nil
}

// validateMemoryResizeAction 校验内存 limit 的下调是否安全：
// 新的 limit 不能低于当前实际使用量，否则 cgroup 会立刻触发 OOM。
func (m *kubeGenericRuntimeManager) validateMemoryResizeAction(
	ctx context.Context,
	pod *v1.Pod,
	podStatus *kubecontainer.PodStatus,
	currentPodResources, desiredPodResources *cm.ResourceConfig,
	podContainerChanges podActions,
) error {
	// 找出哪些内存 limit 正在下降。新增 limit（原值为空或 0）也算作"下降"，
	// 因为从无限制变为有限制同样可能让内存用量瞬间越界。
	podLimitDecreasing := desiredPodResources.Memory != nil &&
		(currentPodResources.Memory == nil ||
			*desiredPodResources.Memory < *currentPodResources.Memory)

	// 容器名 -> 期望内存 limit。
	decreasingContainerLimits := map[string]int64{}
	for _, cUpdate := range podContainerChanges.ContainersToUpdate[v1.ResourceMemory] {
		if cUpdate.desiredContainerResources.memoryLimit != 0 {
			if cUpdate.currentContainerResources == nil || cUpdate.currentContainerResources.memoryLimit == 0 ||
				cUpdate.desiredContainerResources.memoryLimit < cUpdate.currentContainerResources.memoryLimit {
				decreasingContainerLimits[cUpdate.container.Name] = cUpdate.desiredContainerResources.memoryLimit
			}
		}
	}

	if !podLimitDecreasing && len(decreasingContainerLimits) == 0 {
		// 没有内存 limit 在下降，无需进一步检查。
		return nil
	}

	// 检查是否有新的内存 limit 低于当前内存使用量。
	podUsageStats, err := m.runtimeHelper.PodCPUAndMemoryStats(ctx, pod, podStatus)
	if err != nil {
		return fmt.Errorf("unable to read memory usage for pod %q", format.Pod(pod))
	}

	var errs []error
	if podLimitDecreasing {
		if podUsageStats.Memory == nil || podUsageStats.Memory.UsageBytes == nil {
			errs = append(errs, fmt.Errorf("missing pod memory usage"))
		} else if *podUsageStats.Memory.UsageBytes >= uint64(*desiredPodResources.Memory) {
			errs = append(errs, fmt.Errorf("attempting to set pod memory limit (%d) below current usage (%d)",
				*desiredPodResources.Memory, *podUsageStats.Memory.UsageBytes))
		}
	}
	for _, cStats := range podUsageStats.Containers {
		if desiredLimit, ok := decreasingContainerLimits[cStats.Name]; ok {
			if cStats.Memory == nil || cStats.Memory.UsageBytes == nil {
				errs = append(errs, fmt.Errorf("missing container %q memory usage", cStats.Name))
			} else if *cStats.Memory.UsageBytes >= uint64(desiredLimit) {
				errs = append(errs, fmt.Errorf("attempting to set container %q memory limit (%d) below current usage (%d)",
					cStats.Name, desiredLimit, *podUsageStats.Memory.UsageBytes))
			}
		}
	}

	if len(errs) > 0 {
		agg := utilerrors.NewAggregate(errs)
		return fmt.Errorf("cannot decrease memory limits: %w", agg)
	}

	return nil
}

// updatePodContainerResources 把指定资源的期望值下发给运行时。
// 更新某一类资源时，另一类资源沿用最近一次已知的配置值，
// 避免用过期字段覆盖掉已生效的配置。
func (m *kubeGenericRuntimeManager) updatePodContainerResources(ctx context.Context, pod *v1.Pod, resourceName v1.ResourceName, containersToUpdate []containerToUpdateInfo) error {
	logger := klog.FromContext(ctx)
	logger.V(5).Info("Updating container resources", "pod", klog.KObj(pod))

	for _, cInfo := range containersToUpdate {
		container := cInfo.container.DeepCopy()
		// 更新内存 limit 时，使用最近一次配置的 CPU request/limit；
		// 更新 CPU request/limit 时，使用最近一次配置的内存 request/limit。
		switch resourceName {
		case v1.ResourceMemory:
			container.Resources.Limits = v1.ResourceList{
				v1.ResourceCPU:    *resource.NewMilliQuantity(cInfo.currentContainerResources.cpuLimit, resource.DecimalSI),
				v1.ResourceMemory: *resource.NewQuantity(cInfo.desiredContainerResources.memoryLimit, resource.BinarySI),
			}
			container.Resources.Requests = v1.ResourceList{
				v1.ResourceCPU:    *resource.NewMilliQuantity(cInfo.currentContainerResources.cpuRequest, resource.DecimalSI),
				v1.ResourceMemory: *resource.NewQuantity(cInfo.desiredContainerResources.memoryRequest, resource.BinarySI),
			}
		case v1.ResourceCPU:
			container.Resources.Limits = v1.ResourceList{
				v1.ResourceCPU:    *resource.NewMilliQuantity(cInfo.desiredContainerResources.cpuLimit, resource.DecimalSI),
				v1.ResourceMemory: *resource.NewQuantity(cInfo.currentContainerResources.memoryLimit, resource.BinarySI),
			}
			container.Resources.Requests = v1.ResourceList{
				v1.ResourceCPU:    *resource.NewMilliQuantity(cInfo.desiredContainerResources.cpuRequest, resource.DecimalSI),
				v1.ResourceMemory: *resource.NewQuantity(cInfo.currentContainerResources.memoryRequest, resource.BinarySI),
			}
		}
		if err := m.updateContainerResources(ctx, pod, container, cInfo.kubeContainerID); err != nil {
			// 记录错误并中止，因为容器更新必须按 computePodResizeAction 确定的顺序执行。
			// 恢复路径是 SyncPod 在后续迭代中持续重试直到成功。
			logger.Error(err, "updateContainerResources failed", "container", container.Name, "cID", cInfo.kubeContainerID,
				"pod", format.Pod(pod), "resourceName", resourceName)
			return err
		}

		// UpdateContainerResources 无错误，说明运行时已接受 'resourceName' 的期望值。
		// 因此更新 currentContainerResources 中该类资源，它代表我们视角下最近一次配置的资源。
		// 注意：不能依赖 GetPodStatus，因为运行时可能滞后于它刚刚接受的值。
		switch resourceName {
		case v1.ResourceMemory:
			cInfo.currentContainerResources.memoryLimit = cInfo.desiredContainerResources.memoryLimit
			cInfo.currentContainerResources.memoryRequest = cInfo.desiredContainerResources.memoryRequest
		case v1.ResourceCPU:
			cInfo.currentContainerResources.cpuLimit = cInfo.desiredContainerResources.cpuLimit
			cInfo.currentContainerResources.cpuRequest = cInfo.desiredContainerResources.cpuRequest
		}
	}
	return nil
}

// computePodActions 检查 pod spec 与当前状态，返回需要执行的变更动作。
func (m *kubeGenericRuntimeManager) computePodActions(ctx context.Context, pod *v1.Pod, podStatus *kubecontainer.PodStatus, restartAllContainers bool) podActions {
	logger := klog.FromContext(ctx)
	logger.V(5).Info("Syncing Pod", "pod", klog.KObj(pod))

	createPodSandbox, attempt, sandboxID, _ := runtimeutil.PodSandboxChanged(pod, podStatus)
	changes := podActions{
		KillPod:           createPodSandbox,
		CreateSandbox:     createPodSandbox,
		SandboxID:         sandboxID,
		Attempt:           attempt,
		ContainersToStart: []int{},
		ContainersToKill:  make(map[kubecontainer.ContainerID]containerToKillInfo),
	}

	// pod 被标记为 RestartAllContainers 时，需要按逆序杀死并移除所有容器。
	if utilfeature.DefaultFeatureGate.Enabled(features.RestartAllContainersOnContainerExits) && restartAllContainers {
		logger.V(3).Info("Pod marked for RestartAllContainers", "pod", klog.KObj(pod))
		// 按逆序杀死并移除容器。触发 RestartAllContainers 的源容器最后移除。
		sourceInitContainers, targetInitContainers := m.getContainersToReset(pod.Spec.InitContainers, podStatus)
		sourceContainers, targetContainers := m.getContainersToReset(pod.Spec.Containers, podStatus)
		changes.ContainersToReset = append(changes.ContainersToReset, targetContainers...)
		changes.ContainersToReset = append(changes.ContainersToReset, targetInitContainers...)
		changes.ContainersToReset = append(changes.ContainersToReset, sourceContainers...)
		changes.ContainersToReset = append(changes.ContainersToReset, sourceInitContainers...)
		return changes
	}

	// 若需要（重新）创建 pod sandbox，则所有内容都要被杀死并重建，
	// 且 init 容器应被清除。
	if createPodSandbox {
		if !shouldRestartOnFailure(pod) && attempt != 0 && len(podStatus.ContainerStatuses) != 0 {
			// 不应重启 pod，直接返回。
			// 此时不应创建 sandbox：如果 pod 已完成就直接杀死它。
			// 若所有容器都已结束且无需再启动，就没必要新建 sandbox。
			// 这样可以避免在容器都有退出码的 pod 上先重建 sandbox 再终止它，造成日志混乱。
			//
			// 若 ContainerStatuses 为空，则认为从未成功创建过任何容器，
			// 此时应重试创建 sandbox。
			changes.CreateSandbox = false
			return changes
		}

		// 获取要启动的容器；RestartPolicy 为 OnFailure 时排除已成功的容器。
		var containersToStart []int
		for idx, c := range pod.Spec.Containers {
			runOnce := pod.Spec.RestartPolicy == v1.RestartPolicyOnFailure
			if utilfeature.DefaultFeatureGate.Enabled(features.ContainerRestartRules) {
				if c.RestartPolicy != nil {
					runOnce = *c.RestartPolicy == v1.ContainerRestartPolicyOnFailure
				}
			}
			if runOnce && containerSucceeded(&c, podStatus) {
				continue
			}
			if utilfeature.DefaultFeatureGate.Enabled(features.ContainerRestartRules) {
				if c.RestartPolicy != nil && *c.RestartPolicy == v1.ContainerRestartPolicyOnFailure && containerSucceeded(&c, podStatus) {
					continue
				}
			}
			containersToStart = append(containersToStart, idx)
		}

		// 初始化已完成且没有容器需要启动时，不应创建 sandbox，直接杀死 pod。
		if len(containersToStart) == 0 {
			hasInitialized := false

			// 只要存在任一普通容器，就说明所有 init 容器都已初始化完成。
			hasInitialized = HasAnyRegularContainerCreated(pod, podStatus)

			if hasInitialized {
				changes.CreateSandbox = false
				return changes
			}
		}

		// 既然要创建 pod sandbox，就应从初始状态重新开始。
		if len(pod.Spec.InitContainers) != 0 {
			// pod 有 init 容器，返回第一个。
			changes.InitContainersToStart = []int{0}

			return changes
		}
		changes.ContainersToStart = containersToStart
		return changes
	}

	// 即使初始化尚未完成，ephemeral 容器也可以启动。
	for i := range pod.Spec.EphemeralContainers {
		c := (*v1.Container)(&pod.Spec.EphemeralContainers[i].EphemeralContainerCommon)

		// ephemeral 容器从不重启
		if podStatus.FindContainerStatusByName(c.Name) == nil {
			changes.EphemeralContainersToStart = append(changes.EphemeralContainersToStart, i)
		}
	}

	// 只有允许原地扩缩容时才初始化资源更新表
	if resizable, _, _ := allocation.IsInPlacePodVerticalScalingAllowed(pod); resizable {
		changes.ContainersToUpdate = make(map[v1.ResourceName][]containerToUpdateInfo)
	}

	// 检查初始化进度。
	// TODO: Remove this code path as logically it is the subset of the next
	// code path.
	hasInitialized := m.computeInitContainerActions(ctx, pod, podStatus, &changes)
	if changes.KillPod || !hasInitialized {
		// 初始化失败或仍在进行中，跳过对普通容器的检查。
		return changes
	}

	// 需要保留的运行中容器数量。
	keepCount := 0
	// 检查各容器状态。
	for idx, container := range pod.Spec.Containers {
		containerStatus := podStatus.FindContainerStatusByName(container.Name)

		// 对任何非运行中容器调用内部 post-stop 生命周期钩子，让已分配的 CPU 立即释放。
		// 若容器随后被重启，CPU 会重新分配给它。
		if containerStatus != nil && containerStatus.State != kubecontainer.ContainerStateRunning {
			if err := m.internalLifecycle.PostStopContainer(logger, containerStatus.ID.ID); err != nil {
				logger.Error(err, "Internal container post-stop lifecycle hook failed for container in pod with error",
					"containerName", container.Name, "pod", klog.KObj(pod))
			}
		}

		// 若容器不存在或未运行，检查是否需要重启它。
		if containerStatus == nil || containerStatus.State != kubecontainer.ContainerStateRunning {
			if kubecontainer.ShouldContainerBeRestarted(logger, &container, pod, podStatus) {
				logger.V(3).Info("Container of pod is not in the desired state and shall be started", "containerName", container.Name, "pod", klog.KObj(pod))
				changes.ContainersToStart = append(changes.ContainersToStart, idx)
				if containerStatus != nil && containerStatus.State == kubecontainer.ContainerStateUnknown {
					// 容器状态未知时无法确定它是否仍在运行，
					// 重启前总是先尝试杀死它，避免出现同一容器的两个运行实例。
					changes.ContainersToKill[containerStatus.ID] = containerToKillInfo{
						name:      containerStatus.Name,
						container: &pod.Spec.Containers[idx],
						message: fmt.Sprintf("Container is in %q state, try killing it before restart",
							containerStatus.State),
						reason: reasonUnknown,
					}
				}
			}
			continue
		}
		// 容器正在运行，但满足以下任一条件时需要杀死它。
		var message string
		var reason containerKillReason
		restart := shouldRestartOnFailure(pod)
		if utilfeature.DefaultFeatureGate.Enabled(features.ContainerRestartRules) {
			// 探针失败只参考容器级重启策略。容器仍在运行，因此不评估容器级重启规则。
			if container.RestartPolicy != nil {
				restart = *container.RestartPolicy != v1.ContainerRestartPolicyNever
			}
		}
		if _, _, changed := containerChanged(&container, containerStatus); changed {
			message = fmt.Sprintf("Container %s definition changed", container.Name)
			// 容器 spec 变了就必须重启，与重启策略无关。
			restart = true
		} else if liveness, found := m.livenessManager.Get(containerStatus.ID); found && liveness == proberesults.Failure {
			// 容器存活探针失败，应杀死它。
			message = fmt.Sprintf("Container %s failed liveness probe", container.Name)
			reason = reasonLivenessProbe
		} else if startup, found := m.startupManager.Get(containerStatus.ID); found && startup == proberesults.Failure {
			// 容器启动探针失败，应杀死它。
			message = fmt.Sprintf("Container %s failed startup probe", container.Name)
			reason = reasonStartupProbe
		} else if !m.computePodResizeAction(ctx, pod, idx, false, containerStatus, &changes) {
			// resize 策略要求重启该容器时，computePodResizeAction 已更新 'changes'
			continue
		} else {
			// 保留该容器。
			keepCount++
			continue
		}

		// 需要杀死容器；若之后还要重启它，在 message 中明确表达这一意图。
		// 同时不要杀死整个 pod，因为我们期望容器最终能运行起来。
		if restart {
			message = fmt.Sprintf("%s, will be restarted", message)
			changes.ContainersToStart = append(changes.ContainersToStart, idx)
		}

		changes.ContainersToKill[containerStatus.ID] = containerToKillInfo{
			name:      containerStatus.Name,
			container: &pod.Spec.Containers[idx],
			message:   message,
			reason:    reason,
		}
		logger.V(2).Info("Message for Container of pod", "containerName", container.Name, "containerStatusID", containerStatus.ID, "pod", klog.KObj(pod), "containerMessage", message)
	}

	// 检查 volume 的 resize 动作
	m.computeVolumeResizeAction(ctx, pod, &changes)

	if keepCount == 0 && len(changes.ContainersToStart) == 0 {
		changes.KillPod = true
		// 为避免 restartable init 容器把 pod 一直撑活着，不应重启它们。
		changes.InitContainersToStart = nil
	}

	changes.UpdatePodLevelResources = m.computePodLevelResourcesResizeAction(ctx, pod)
	if !changes.UpdatePodLevelResources && !changes.UpdatePodResources {
		// 若有待启动容器资源已被调整，则需要同步调整 pod 资源。
		changes.UpdatePodResources = m.startingResizedContainer(logger, pod, pod.Spec.Containers, changes.ContainersToStart)
	}

	return changes
}

// startingResizedContainer 判断 containersToStart 中是否有容器的资源已发生变化。
func (m *kubeGenericRuntimeManager) startingResizedContainer(logger klog.Logger, pod *v1.Pod, containers []v1.Container, containersToStart []int) bool {
	for _, i := range containersToStart {
		c := containers[i]
		actuatedResources, ok := m.actuatedState.GetContainerResources(pod.UID, c.Name)
		if !ok {
			logger.V(4).Info("Actuated resources missing for container", "pod", format.Pod(pod), "container", c.Name)
		}
		var actuatedPodResources *v1.ResourceRequirements
		if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodLevelResourcesVerticalScaling) {
			actuatedPodResources, _ = m.actuatedState.GetPodLevelResources(pod.UID)
		}
		desired := containerResourcesFromRequirements(pod.Spec.Resources, &c.Resources)
		actuated := containerResourcesFromRequirements(actuatedPodResources, &actuatedResources)
		if desired != actuated {
			return true
		}
	}
	return false
}

// getContainersToReset 返回需要从运行时移除的容器信息。
// 第一个列表是触发 RestartAllContainers 的源容器；第二个列表是受其牵连的目标容器。
func (m *kubeGenericRuntimeManager) getContainersToReset(containers []v1.Container, podStatus *kubecontainer.PodStatus) (sources []containerToRemoveInfo, targets []containerToRemoveInfo) {
	for idx, c := range containers {
		// 不能使用 podStatus.FindContainerStatusByName，因为同一容器可能存在多个状态，
		// 而 RestartAllContainers 要求把所有容器都从运行时清除。
		for _, containerStatus := range podStatus.ContainerStatuses {
			if containerStatus.Name != c.Name {
				continue
			}
			info := containerToRemoveInfo{
				containerID: containerStatus.ID,
				container:   &containers[idx],
			}
			if containerStatus.State == kubecontainer.ContainerStateExited {
				exitCode := containerStatus.ExitCode
				rule, ok := podutil.FindMatchingContainerRestartRule(c, int32(exitCode))
				if ok && rule.Action == v1.ContainerRestartRuleActionRestartAllContainers {
					sources = append(sources, info)
				} else {
					targets = append(targets, info)
				}
			} else {
				info.kill = true
				targets = append(targets, info)
			}
		}
	}
	return
}

// computePodLevelResourcesResizeAction 判断 pod 级资源是否需要 resize（仅特性开启时有效）。
func (m *kubeGenericRuntimeManager) computePodLevelResourcesResizeAction(ctx context.Context, pod *v1.Pod) bool {
	if !utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodLevelResourcesVerticalScaling) {
		return false
	}
	logger := klog.FromContext(ctx)

	actuatedPodLevelResources, found := m.actuatedState.GetPodLevelResources(pod.UID)
	if !found {
		logger.Error(nil, "Missing actuated pod level resource record", "pod", klog.KObj(pod), "pod", pod.Name)
		// 以零值 actuated 资源继续。对 restart NotRequired 来说可能多一次
		// UpdateContainerResources 调用，但该调用应当幂等。
		// 对 RestartContainer 来说可能会触发一次容器重启。
	}

	desiredPodLevelResources := podResourcesFromRequirements(pod.Spec.Resources)
	currentPodLevelResources := podResourcesFromRequirements(actuatedPodLevelResources)
	return currentPodLevelResources != desiredPodLevelResources
}

// SyncPod 通过以下步骤把运行中的 pod 同步到期望状态：
//
//  1. 计算 sandbox 与容器的变更。
//  2. 必要时杀死 pod sandbox。
//  3. 杀死所有不应继续运行的容器。
//  4. 必要时创建 sandbox。
//  5. 调用 OnPodSandboxReady 通知 kubelet 更新 pod 状态。
//  6. 调整 pod 与运行中容器的大小
//  7. 创建 ephemeral 容器。
//  8. 创建 init 容器。
//  9. 创建普通容器。
func (m *kubeGenericRuntimeManager) SyncPod(ctx context.Context, pod *v1.Pod, podStatus *kubecontainer.PodStatus, pullSecrets []v1.Secret, backOff *flowcontrol.Backoff, restartAllContainers bool) (result kubecontainer.PodSyncResult) {
	logger := klog.FromContext(ctx)
	// 步骤 1：计算 sandbox 与容器变更。
	podContainerChanges := m.computePodActions(ctx, pod, podStatus, restartAllContainers)
	logger.V(3).Info("computePodActions got for pod", "podActions", podContainerChanges, "pod", klog.KObj(pod))
	if podContainerChanges.CreateSandbox {
		ref, err := ref.GetReference(legacyscheme.Scheme, pod)
		if err != nil {
			logger.Error(err, "Couldn't make a ref to pod", "pod", klog.KObj(pod))
		}
		if podContainerChanges.SandboxID != "" {
			m.recorder.WithLogger(logger).Eventf(ref, v1.EventTypeNormal, events.SandboxChanged, "Pod sandbox changed, it will be killed and re-created.")
		} else {
			logger.V(4).Info("SyncPod received new pod, will create a sandbox for it", "pod", klog.KObj(pod))
		}
	}

	// 步骤 2：若 sandbox 已变更则杀死 pod。
	if podContainerChanges.KillPod {
		if podContainerChanges.CreateSandbox {
			logger.V(4).Info("Stopping PodSandbox for pod, will start new one", "pod", klog.KObj(pod))
		} else {
			logger.V(4).Info("Stopping PodSandbox for pod, because all other containers are dead", "pod", klog.KObj(pod))
		}

		killResult := m.killPodWithSyncResult(ctx, pod, kubecontainer.ConvertPodStatusToRunningPod(m.runtimeName, podStatus), nil)
		result.AddPodSyncResult(killResult)
		if killResult.Error() != nil {
			logger.Error(killResult.Error(), "killPodWithSyncResult failed")
			return
		}

		if podContainerChanges.CreateSandbox {
			m.purgeInitContainers(ctx, pod, podStatus)
		}
	} else {
		// 步骤 3：杀死该 pod 中所有不应保留的运行中容器。
		for containerID, containerInfo := range podContainerChanges.ContainersToKill {
			logger.V(3).Info("Killing unwanted container for pod", "containerName", containerInfo.name, "containerID", containerID, "pod", klog.KObj(pod))
			killContainerResult := kubecontainer.NewSyncResult(kubecontainer.KillContainer, containerInfo.name)
			result.AddSyncResult(killContainerResult)
			if err := m.killContainer(ctx, pod, containerID, containerInfo.name, containerInfo.message, containerInfo.reason, nil, nil); err != nil {
				killContainerResult.Fail(kubecontainer.ErrKillContainer, err.Error())
				logger.Error(err, "killContainer for pod failed", "containerName", containerInfo.name, "containerID", containerID, "pod", klog.KObj(pod))
				return
			}
		}

		// 移除标记为待移除的容器（用于原地重启）
		if utilfeature.DefaultFeatureGate.Enabled(features.RestartAllContainersOnContainerExits) {
			for _, containerInfo := range podContainerChanges.ContainersToReset {
				cName := containerInfo.container.Name
				logger.V(3).Info("Removing container before pod restarts", "containerName", cName, "containerID", containerInfo.containerID, "pod", klog.KObj(pod))
				removeContainerResult := kubecontainer.NewSyncResult(kubecontainer.RemoveContainer, cName)
				result.AddSyncResult(removeContainerResult)
				if containerInfo.kill {
					logger.V(3).Info("Killing container before removal", "containerName", cName, "containerID", containerInfo.containerID, "pod", klog.KObj(pod))
					// 不设宽限期直接杀死容器。
					var gracePeriod int64 = 0
					if err := m.killContainer(ctx, pod, containerInfo.containerID, cName, "killing", reasonRestartAllContainers, &gracePeriod, nil); err != nil {
						removeContainerResult.Fail(kubecontainer.ErrKillContainer, err.Error())
						logger.Error(err, "killContainer for pod failed", "containerName", cName, "containerID", containerInfo.containerID, "pod", klog.KObj(pod))
						return
					}
				}
				// 被移除容器的日志会保留，直到 pod 被删除并触发 GC。
				if err := m.removeContainer(ctx, containerInfo.containerID.ID, true); err != nil {
					removeContainerResult.Fail(kubecontainer.ErrRemoveContainer, err.Error())
					logger.Error(err, "removeContainer for pod failed", "containerName", cName, "containerID", containerInfo.containerID, "pod", klog.KObj(pod))
					return
				}
			}
		}
	}

	// 较激进地清理已终止的 init 容器。
	// 这是一项优化，因为容器移除通常由容器垃圾回收器负责。
	m.pruneInitContainersBeforeStart(ctx, pod, podStatus)

	// 把 PRIMARY podIP 与 podIPs 列表传递到 generatePodSandboxConfig 和
	// generateContainerConfig，再由它们传给其他函数，以支持 hosts 文件和 downward API
	// 等依赖这些值的功能，并避免在容器需要重启但 status manager 中还没有 podIP 时
	// 产生竞争。podIPs 列表用于生成 hosts 文件。
	//
	// 默认使用传入 pod status 中的 IP，若 sandbox 需要（重）建则覆盖它们。
	var podIPs []string
	if podStatus != nil {
		podIPs = podStatus.IPs
	}

	// 步骤 4：必要时为 pod 创建 sandbox。
	podSandboxID := podContainerChanges.SandboxID
	if podContainerChanges.CreateSandbox {
		var msg string
		var err error

		logger.V(4).Info("Creating PodSandbox for pod", "pod", klog.KObj(pod))
		metrics.StartedPodsTotal.Inc()
		if utilfeature.DefaultFeatureGate.Enabled(features.UserNamespacesSupport) && pod.Spec.HostUsers != nil && !*pod.Spec.HostUsers {
			metrics.StartedUserNamespacedPodsTotal.Inc()
			// 用户命名空间创建的失败可能出现在 pod 生命周期的任意阶段，
			// 但通常会在容器创建时暴露。
			// 为避免逐一处理每种错误，在 sync 结束后统一检查结果。
			defer func() {
				// 捕获未被处理的错误
				for _, res := range result.SyncResults {
					if res.Error != nil {
						metrics.StartedUserNamespacedPodsErrorsTotal.Inc()
						return
					}
				}
				// 捕获已被处理的错误
				if result.SyncError != nil {
					metrics.StartedUserNamespacedPodsErrorsTotal.Inc()
				}
			}()
		}
		createSandboxResult := kubecontainer.NewSyncResult(kubecontainer.CreatePodSandbox, format.Pod(pod))
		result.AddSyncResult(createSandboxResult)

		// ConvertPodSysctlsVariableToDotsSeparator 会把 Pod.Spec.SecurityContext.Sysctls
		// 中 sysctl 变量的分隔符转换为点号。
		// runc 用点号作为分隔符来校验 sysctl 变量在独立命名空间中是否合法，
		// 因此当使用斜杠作为分隔符时，runc 会返回错误：
		// "sysctl is not in a separate kernel namespace"，导致 podSandbox 无法创建。
		// 所以在调用 runc 之前必须先转换 sysctl 变量，用点号分隔内核命名空间。
		// 待 runc 支持斜杠作为 sysctl 分隔符后，该函数即可移除。
		sysctl.ConvertPodSysctlsVariableToDotsSeparator(pod.Spec.SecurityContext)

		// 为 pod 准备由动态资源分配（DRA）特性分配的资源
		if utilfeature.DefaultFeatureGate.Enabled(features.DynamicResourceAllocation) {
			if err := m.runtimeHelper.PrepareDynamicResources(ctx, pod); err != nil {
				ref, referr := ref.GetReference(legacyscheme.Scheme, pod)
				if referr != nil {
					logger.Error(referr, "Couldn't make a ref to pod", "pod", klog.KObj(pod))
					return
				}
				m.recorder.WithLogger(logger).Eventf(ref, v1.EventTypeWarning, events.FailedPrepareDynamicResources, "Failed to prepare dynamic resources: %v", err)
				logger.Error(err, "Failed to prepare dynamic resources", "pod", klog.KObj(pod))
				return
			}
		}

		podSandboxID, msg, err = m.createPodSandbox(ctx, pod, podContainerChanges.Attempt)
		if err != nil {
			// 若 pod 在创建过程中被删除，createPodSandbox 可能返回来自 CNI、CSI
			// 或 CRI 的错误，此时这不算真正的错误。
			//
			// 我们走到这里时 SyncPod 仍可能正在运行，说明 PodWorker 尚未确认删除。
			if m.podStateProvider.IsPodTerminationRequested(pod.UID) {
				logger.V(4).Info("Pod was deleted and sandbox failed to be created", "pod", klog.KObj(pod), "podUID", pod.UID)
				return
			}
			metrics.StartedPodsErrorsTotal.Inc()
			createSandboxResult.Fail(kubecontainer.ErrCreatePodSandbox, msg)
			logger.Error(err, "CreatePodSandbox for pod failed", "pod", klog.KObj(pod))
			ref, referr := ref.GetReference(legacyscheme.Scheme, pod)
			if referr != nil {
				logger.Error(referr, "Couldn't make a ref to pod", "pod", klog.KObj(pod))
			}
			m.recorder.WithLogger(logger).Eventf(ref, v1.EventTypeWarning, events.FailedCreatePodSandBox, "Failed to create pod sandbox: %v", err)
			return
		}
		logger.V(4).Info("Created PodSandbox for pod", "podSandboxID", podSandboxID, "pod", klog.KObj(pod))

		resp, err := m.runtimeService.PodSandboxStatus(ctx, podSandboxID, false)
		if err != nil {
			ref, referr := ref.GetReference(legacyscheme.Scheme, pod)
			if referr != nil {
				logger.Error(referr, "Couldn't make a ref to pod", "pod", klog.KObj(pod))
			}
			m.recorder.WithLogger(logger).Eventf(ref, v1.EventTypeWarning, events.FailedStatusPodSandBox, "Unable to get pod sandbox status: %v", err)
			logger.Error(err, "Failed to get pod sandbox status; Skipping pod", "pod", klog.KObj(pod))
			result.Fail(err)
			return
		}
		if resp.GetStatus() == nil {
			result.Fail(errors.New("pod sandbox status is nil"))
			return
		}

		// 如果将来允许把 pod 从非 host 网络更新为 host 网络，这里可能用到过期的 IP。
		if !kubecontainer.IsHostNetworkPod(pod) {
			// 刚启动 sandbox，覆盖 pod status 中传入的 podIPs。
			podIPs = m.determinePodSandboxIPs(ctx, pod.Namespace, pod.Name, resp.GetStatus())
			logger.V(4).Info("Determined the ip for pod after sandbox changed", "IPs", podIPs, "pod", klog.KObj(pod))
		}

		// 步骤 5：在拉取镜像之前触发 sandbox ready 回调。
		// 此时动态资源已准备（见上面的 PrepareDynamicResources() 调用），
		// volume 也已挂载（在 kubelet 的 SyncPod() 层面完成），
		// 因此具备设置 `PodReadyToStartContainers=True` 条件的全部前提（sandbox、网络、volume）。
		logger.V(4).Info("Pod sandbox and network ready, invoking callback", "pod", klog.KObj(pod), "podIPs", podIPs)
		if err := m.runtimeHelper.OnPodSandboxReady(ctx, pod); err != nil {
			// 只记录错误并继续创建 pod，以保持既有行为
			logger.Error(err, "Failed to invoke sandbox ready callback, continuing with pod creation", "pod", klog.KObj(pod))
		}
	}

	// 启动容器的流程依赖 pod IP（即主 pod IP）。
	// 这里提前短路取出，避免每次都要判断 len(podIPs) > 0。
	podIP := ""
	if len(podIPs) != 0 {
		podIP = podIPs[0]
	}

	// 步骤 6：如有必要，调整 pod 与运行中容器的大小。
	if resizable, _, _ := allocation.IsInPlacePodVerticalScalingAllowed(pod); resizable {
		updateVolumes := utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingMemoryBackedVolumes) &&
			(len(podContainerChanges.VolumesToUpsize) > 0 || len(podContainerChanges.VolumesToDownsize) > 0)
		if len(podContainerChanges.ContainersToUpdate) > 0 || podContainerChanges.UpdatePodResources || podContainerChanges.UpdatePodLevelResources || updateVolumes {
			result.AddSyncResult(m.doPodResizeAction(ctx, pod, podStatus, podContainerChanges))
		}
	}

	// 包含启动各类容器所需样板逻辑的辅助函数。
	// typeName 用于日志中描述容器类型，目前取值："container"、"init container" 或 "ephemeral container"
	// metricLabel 用于监控指标中描述容器类型，目前取值："container"、"init_container" 或 "ephemeral_container"
	startWithInitState := func(ctx context.Context, typeName, metricLabel string, spec *startSpec, podSandboxConfig *runtimeapi.PodSandboxConfig, imageVolumePullResults imageVolumePulls) error {
		startContainerResult := kubecontainer.NewSyncResult(kubecontainer.StartContainer, spec.container.Name)
		result.AddSyncResult(startContainerResult)

		isInBackOff, msg, err := m.doBackOff(ctx, pod, spec.container, podStatus, backOff)
		if isInBackOff {
			startContainerResult.Fail(err, msg)
			logger.V(4).Info("Backing Off restarting container in pod", "containerType", typeName, "container", spec.container.Name, "pod", klog.KObj(pod))
			return err
		}

		metrics.StartedContainersTotal.WithLabelValues(metricLabel).Inc()
		if sc.HasWindowsHostProcessRequest(pod, spec.container) {
			metrics.StartedHostProcessContainersTotal.WithLabelValues(metricLabel).Inc()
		}
		logger.V(4).Info("Creating container in pod", "containerType", typeName, "container", spec.container.Name, "pod", klog.KObj(pod))

		// 在这里才失败是为了把 "ErrImagePull" 和 "ImagePullBackOff" 正确地反馈给最终用户。
		imageVolumes, err := m.toKubeContainerImageVolumes(ctx, imageVolumePullResults, spec.container, pod, startContainerResult)
		if err != nil {
			return err
		}

		// NOTE (aramase) podIPs 在单栈和双栈集群中都会被填充。这里只发送 podIPs。
		msg, err = m.startContainer(ctx, podSandboxID, podSandboxConfig, spec, pod, podStatus, pullSecrets, podIP, podIPs, imageVolumes)
		incrementImageVolumeMetrics(err, msg, spec.container, imageVolumes)
		if err != nil {
			// startContainer() 返回定义良好的错误码，基数可控，便于集群管理员
			// 区分 "server errors" 与 "user errors"。
			metrics.StartedContainersErrorsTotal.WithLabelValues(metricLabel, err.Error()).Inc()
			if sc.HasWindowsHostProcessRequest(pod, spec.container) {
				metrics.StartedHostProcessContainersErrorsTotal.WithLabelValues(metricLabel, err.Error()).Inc()
			}
			startContainerResult.Fail(err, msg)
			// 已在别处记录过的已知错误，这里用更高日志级别记录以避免重复刷屏
			switch {
			case err == images.ErrImagePullBackOff:
				logger.V(3).Info("Container start failed in pod", "containerType", typeName, "container", spec.container.Name, "pod", klog.KObj(pod), "containerMessage", msg, "err", err)
			default:
				utilruntime.HandleError(fmt.Errorf("%v %v start failed in pod %v: %w: %s", typeName, spec.container.Name, format.Pod(pod), err, msg))
			}
			return err
		}
		if typeName == "init container" {
			// 不统计 restartable init 容器（sidecar）
			if !podutil.IsRestartableInitContainer(spec.container) {
				if m.podInitContainerTimeRecorder != nil {
					m.podInitContainerTimeRecorder.RecordInitContainerStarted(pod.UID, time.Now())
				}
			}
		}

		return nil
	}

	// 仅当需要启动容器时才准备的状态
	var (
		podSandboxConfig       *runtimeapi.PodSandboxConfig
		imageVolumePullResults imageVolumePulls
	)

	// lazyStart 惰性初始化启动容器所需状态，并返回一个启动函数。
	// 首次调用时才生成 sandbox 配置、拉取 image volume，避免无容器可启动时做无用功。
	lazyStart := func() func(ctx context.Context, typeName, metricLabel string, spec *startSpec) error {
		var err error

		if podSandboxConfig == nil {
			podSandboxConfig, err = m.generatePodSandboxConfig(ctx, pod, podContainerChanges.Attempt)
			if err != nil {
				logger.Error(err, "GeneratePodSandboxConfig for pod failed", "pod", klog.KObj(pod))
				result.Fail(fmt.Errorf("GeneratePodSandboxConfig for pod %q failed: %w", format.Pod(pod), err))
				return nil
			}
		}

		if imageVolumePullResults == nil {
			imageVolumePullResults, err = m.getImageVolumes(ctx, pod, podSandboxConfig, pullSecrets)
			if err != nil {
				// 在拿到所有 image volume 之前不启动任何容器。
				logger.Error(err, "Get image volumes for pod failed", "pod", klog.KObj(pod))
				result.Fail(err)
				return nil
			}
		}

		return func(ctx context.Context, typeName, metricLabel string, spec *startSpec) error {
			return startWithInitState(ctx, typeName, metricLabel, spec, podSandboxConfig, imageVolumePullResults)
		}
	}

	// 步骤 7：启动 ephemeral 容器
	// 它们先于 init 容器启动，以便即使 init 容器启动失败也能运行 ephemeral 容器。
	// 实际上 init 容器通常先启动，因为 ephemeral 容器无法在 pod 创建时指定。
	for _, idx := range podContainerChanges.EphemeralContainersToStart {
		start := lazyStart()
		if start == nil {
			return
		}

		start(ctx, "ephemeral container", metrics.EphemeralContainer, ephemeralContainerStartSpec(&pod.Spec.EphemeralContainers[idx]))
	}

	// 步骤 8：启动 init 容器。
	for _, idx := range podContainerChanges.InitContainersToStart {
		start := lazyStart()
		if start == nil {
			return
		}

		container := &pod.Spec.InitContainers[idx]
		// 启动下一个 init 容器。
		if err := start(ctx, "init container", metrics.InitContainer, containerStartSpec(container)); err != nil {
			if podutil.IsRestartableInitContainer(container) {
				logger.V(4).Info("Failed to start the restartable init container for the pod, skipping", "initContainerName", container.Name, "pod", klog.KObj(pod))
				continue
			}
			logger.V(4).Info("Failed to initialize the pod, as the init container failed to start, aborting", "initContainerName", container.Name, "pod", klog.KObj(pod))
			return
		}

		// 容器已成功启动，清空失败记录
		logger.V(4).Info("Completed init container for pod", "containerName", container.Name, "pod", klog.KObj(pod))
	}

	for _, cs := range podStatus.ContainerStatuses {
		// 检查是否为 init 容器
		for _, init := range pod.Spec.InitContainers {
			// 不统计 restartable init 容器（sidecar）
			if podutil.IsRestartableInitContainer(&init) {
				continue
			}
			if cs.Name == init.Name && cs.State == kubecontainer.ContainerStateExited && !cs.FinishedAt.IsZero() {
				if m.podInitContainerTimeRecorder != nil {
					m.podInitContainerTimeRecorder.RecordInitContainerFinished(pod.UID, cs.FinishedAt)
				}
			}
		}
	}

	// 步骤 9：启动 podContainerChanges.ContainersToStart 中的容器。
	for _, idx := range podContainerChanges.ContainersToStart {
		start := lazyStart()
		if start == nil {
			return
		}

		start(ctx, "container", metrics.Container, containerStartSpec(&pod.Spec.Containers[idx]))
	}

	return result
}

// incrementImageVolumeMetrics 根据错误与容器对 image volume 的使用情况，
// 递增 image volume 挂载相关指标。
func incrementImageVolumeMetrics(err error, msg string, container *v1.Container, imageVolumes kubecontainer.ImageVolumes) {
	if !utilfeature.DefaultFeatureGate.Enabled(features.ImageVolume) {
		return
	}

	metrics.ImageVolumeRequestedTotal.Add(float64(len(imageVolumes)))

	for _, m := range container.VolumeMounts {
		if _, exists := imageVolumes[m.Name]; exists {
			if errors.Is(err, ErrCreateContainer) && strings.HasPrefix(msg, crierror.ErrImageVolumeMountFailed.Error()) {
				metrics.ImageVolumeMountedErrorsTotal.Inc()
			} else {
				metrics.ImageVolumeMountedSucceedTotal.Inc()
			}
		}
	}
}

// imageVolumePulls 保存各 image volume 名称对应的拉取结果。
type imageVolumePulls = map[string]imageVolumePullResult

// imageVolumePullResult 是单个 image volume 的拉取结果。
// 若 spec 为 nil，则 err 与 msg 应被设置。
// 若 err 为 nil，则 spec 应被设置。
type imageVolumePullResult struct {
	spec *runtimeapi.ImageSpec
	err  error
	msg  string
}

// toKubeContainerImageVolumes 把 image volume 拉取结果转换为容器可用的 image volume 集合；
// 只要有一个挂载的 image volume 拉取失败，就返回错误并让该容器启动失败。
func (m *kubeGenericRuntimeManager) toKubeContainerImageVolumes(ctx context.Context, imageVolumePullResults imageVolumePulls, container *v1.Container, pod *v1.Pod, syncResult *kubecontainer.SyncResult) (kubecontainer.ImageVolumes, error) {
	if len(imageVolumePullResults) == 0 {
		return nil, nil
	}

	imageVolumes := kubecontainer.ImageVolumes{}
	var (
		lastErr error
		lastMsg string
	)
	for _, v := range container.VolumeMounts {
		res, ok := imageVolumePullResults[v.Name]
		if !ok {
			continue
		}

		if res.err != nil {
			s, _ := grpcstatus.FromError(res.err)
			m.recordContainerEvent(ctx, pod, container, "", v1.EventTypeWarning, events.FailedToCreateContainer, "Error: %v", s.Message())
			lastErr = res.err
			lastMsg = res.msg
			continue
		}

		imageVolumes[v.Name] = res.spec
	}

	if lastErr != nil {
		syncResult.Fail(lastErr, lastMsg)
		return nil, lastErr
	}

	return imageVolumes, nil
}

// getImageVolumes 在启动容器前确保 pod 中所有 image volume 的镜像都已就绪。
// 返回每个 volume 的拉取结果，单个 volume 失败不影响其他 volume 的拉取。
func (m *kubeGenericRuntimeManager) getImageVolumes(ctx context.Context, pod *v1.Pod, podSandboxConfig *runtimeapi.PodSandboxConfig, pullSecrets []v1.Secret) (imageVolumePulls, error) {
	logger := klog.FromContext(ctx)
	if !utilfeature.DefaultFeatureGate.Enabled(features.ImageVolume) {
		return nil, nil
	}

	podRuntimeHandler, err := m.getPodRuntimeHandler(pod)
	if err != nil {
		logger.Error(err, "Failed to get pod runtime handler", "pod", klog.KObj(pod))
		return nil, err
	}

	res := make(imageVolumePulls)
	for _, volume := range pod.Spec.Volumes {
		if volume.Image == nil {
			continue
		}

		// objectRef 可能为 nil，此处无需检查错误。
		objectRef, _ := ref.GetReference(legacyscheme.Scheme, pod)
		ref, msg, err := m.imagePuller.EnsureImageExists(
			ctx, objectRef, pod, volume.Image.Reference, pullSecrets, podSandboxConfig, podRuntimeHandler, volume.Image.PullPolicy,
		)
		if err != nil {
			logger.Error(err, "Failed to ensure image", "pod", klog.KObj(pod))
			res[volume.Name] = imageVolumePullResult{err: err, msg: msg}
			continue
		}

		logger.V(4).Info("Pulled image", "ref", ref, "pod", klog.KObj(pod))
		res[volume.Name] = imageVolumePullResult{spec: &runtimeapi.ImageSpec{
			Image:              ref,
			UserSpecifiedImage: volume.Image.Reference,
			RuntimeHandler:     podRuntimeHandler,
			Annotations:        pod.Annotations,
		}}
	}

	return res, nil
}

// 若容器仍处于 backoff 中，函数返回一个简短的 backoff 错误和详细错误信息。
func (m *kubeGenericRuntimeManager) doBackOff(ctx context.Context, pod *v1.Pod, container *v1.Container, podStatus *kubecontainer.PodStatus, backOff *flowcontrol.Backoff) (bool, string, error) {
	logger := klog.FromContext(ctx)
	var cStatus *kubecontainer.Status
	for _, c := range podStatus.ContainerStatuses {
		if c.Name == container.Name && c.State == kubecontainer.ContainerStateExited {
			cStatus = c
			break
		}
	}

	if cStatus == nil {
		return false, "", nil
	}

	logger.V(3).Info("Checking backoff for container in pod", "containerName", container.Name, "pod", klog.KObj(pod))
	// 以最近一次退出的容器的结束时间作为起点，计算是否应进行 back-off。
	ts := cStatus.FinishedAt
	// backOff 需要一个唯一 key 来标识容器。
	key := GetBackoffKey(pod, container)
	if backOff.IsInBackOffSince(key, ts) {
		if containerRef, err := kubecontainer.GenerateContainerRef(pod, container); err == nil {
			m.recorder.WithLogger(logger).Eventf(containerRef, v1.EventTypeWarning, events.BackOffStartContainer,
				"Back-off restarting failed container %s in pod %s", container.Name, format.Pod(pod))
		}
		backoff := backOff.Get(key)
		err := fmt.Errorf("back-off %s restarting failed container=%s pod=%s", backoff, container.Name, format.Pod(pod))
		logger.V(3).Info("Back-off restarting failed container", "err", err.Error())
		return true, err.Error(), kubecontainer.NewBackoffError(kubecontainer.ErrCrashLoopBackOff, ts.Add(backoff))
	}

	backOff.Next(key, ts)
	return false, "", nil
}

// KillPod 杀死 pod 的所有容器。pod 可以为 nil，但 runningPod 不能为 nil。
// 若指定 gracePeriodOverride，调用方可以覆盖 pod 默认的宽限期。
// 在 kubelet 中只有硬杀路径允许指定 gracePeriodOverride，以免损坏用户数据：
// 例如硬驱逐场景下发送 SIGKILL，或软驱逐场景下使用最大宽限期。
func (m *kubeGenericRuntimeManager) KillPod(ctx context.Context, pod *v1.Pod, runningPod kubecontainer.Pod, gracePeriodOverride *int64) error {
	err := m.killPodWithSyncResult(ctx, pod, runningPod, gracePeriodOverride)
	return err.Error()
}

// killPodWithSyncResult 杀死 runningPod 并返回 SyncResult。
// 注意：kubelet 重启后传入的 pod 可能是 *nil*。
func (m *kubeGenericRuntimeManager) killPodWithSyncResult(ctx context.Context, pod *v1.Pod, runningPod kubecontainer.Pod, gracePeriodOverride *int64) (result kubecontainer.PodSyncResult) {
	logger := klog.FromContext(ctx)
	killContainerResults := m.killContainersWithSyncResult(ctx, pod, runningPod, gracePeriodOverride)
	for _, containerResult := range killContainerResults {
		result.AddSyncResult(containerResult)
	}

	// 停止 sandbox，sandbox 的删除由 GarbageCollect 负责
	killSandboxResult := kubecontainer.NewSyncResult(kubecontainer.KillPodSandbox, runningPod.ID)
	result.AddSyncResult(killSandboxResult)
	// 停止属于同一 pod 的所有 sandbox
	for _, podSandbox := range runningPod.Sandboxes {
		if err := m.runtimeService.StopPodSandbox(ctx, podSandbox.ID.ID); err != nil && !crierror.IsNotFound(err) {
			killSandboxResult.Fail(kubecontainer.ErrKillPodSandbox, err.Error())
			logger.Error(nil, "Failed to stop sandbox", "podSandboxID", podSandbox.ID)
		}
	}

	return
}

// GeneratePodStatus 根据 CRI 容器事件构建 PodStatus，用于 Evented PLEG 的快速状态更新。
func (m *kubeGenericRuntimeManager) GeneratePodStatus(event *runtimeapi.ContainerEventResponse) *kubecontainer.PodStatus {
	// 该 context 将来会改为由调用方传入。
	ctx := context.TODO()
	podUID := kubetypes.UID(event.PodSandboxStatus.Metadata.Uid)
	podIPs := m.determinePodSandboxIPs(ctx, event.PodSandboxStatus.Metadata.Namespace, event.PodSandboxStatus.Metadata.Name, event.PodSandboxStatus)

	kubeContainerStatuses := []*kubecontainer.Status{}
	for _, status := range event.ContainersStatuses {
		kubeContainerStatuses = append(kubeContainerStatuses, m.convertToKubeContainerStatus(ctx, podUID, status))
	}

	sort.Sort(containerStatusByCreated(kubeContainerStatuses))

	return &kubecontainer.PodStatus{
		ID:                kubetypes.UID(event.PodSandboxStatus.Metadata.Uid),
		Name:              event.PodSandboxStatus.Metadata.Name,
		Namespace:         event.PodSandboxStatus.Metadata.Namespace,
		IPs:               podIPs,
		SandboxStatuses:   []*runtimeapi.PodSandboxStatus{event.PodSandboxStatus},
		ContainerStatuses: kubeContainerStatuses,
	}
}

// GetPodStatus 获取 pod 状态，包括运行时中可见的所有容器信息。
func (m *kubeGenericRuntimeManager) GetPodStatus(ctx context.Context, pod *kubecontainer.Pod) (*kubecontainer.PodStatus, error) {
	logger := klog.FromContext(ctx)
	// 目前把容器的重启次数作为容器 label 保留。每次容器重启时，pod 会从已注册的
	// 死亡容器读取重启次数，加一得到新的重启次数，再把新次数作为 label 加到新启动的容器上。
	// 但该方法存在一些限制：
	//	1. 当所有死亡容器都被垃圾回收后，容器状态无法获得历史值，会 *不准确*。
	//	   好在这种概率极低。
	//	2. 对于没有重启次数 label 的旧版本容器，只能假定其重启次数为 0。
	// 无论如何，我们只承诺 "best-effort" 地报告重启次数，可以暂时忽略这些限制。
	// TODO: move this comment to SyncPod.
	podFullName := format.PodDesc(pod.Name, pod.Namespace, pod.ID)
	logger = logger.WithValues("pod", podFullName)
	ctx = klog.NewContext(ctx, logger)

	podSandboxIDs := make([]string, len(pod.Sandboxes))
	for i, s := range pod.Sandboxes {
		podSandboxIDs[i] = s.ID.ID
	}

	sandboxStatuses := []*runtimeapi.PodSandboxStatus{}
	containerStatuses := []*kubecontainer.Status{}
	activeContainerStatuses := []*kubecontainer.Status{}

	// 由于 kubecontainer.Pod 中的 sandbox 与容器 ID 是权威来源，必须保守地使用其
	// timestamp 作为状态时间戳。否则可能上报一个更新的 PodStatus 时间戳，
	// 却缺少当时本应存在的 sandbox 或容器。
	timestamp := pod.Timestamp

	podIPs := []string{}
	var activePodSandboxID string
	for idx, podSandboxID := range podSandboxIDs {
		resp, err := m.runtimeService.PodSandboxStatus(ctx, podSandboxID, false)
		// 在 List（ListPodSandbox）与检查（PodSandboxStatus）之间，另一个线程可能移除了容器，
		// 这是正常的。之前的调用（getSandboxIDByPodUID）不会因 sandbox 不存在而失败，
		// 因此本方法也不应失败，而应视作之前的调用失败一样忽略该错误。
		if crierror.IsNotFound(err) {
			continue
		}
		if err != nil {
			logger.Error(err, "PodSandboxStatus of sandbox for pod", "podSandboxID", podSandboxID)
			return nil, err
		}
		if resp.GetStatus() == nil {
			return nil, errors.New("pod sandbox status is nil")

		}
		sandboxStatuses = append(sandboxStatuses, resp.Status)
		// 只从最新的 sandbox 获取 pod IP
		if idx == 0 && resp.Status.State == runtimeapi.PodSandboxState_SANDBOX_READY {
			podIPs = m.determinePodSandboxIPs(ctx, pod.Namespace, pod.Name, resp.Status)
			activePodSandboxID = podSandboxID
		}

		if idx == 0 && utilfeature.DefaultFeatureGate.Enabled(features.EventedPLEG) {
			if resp.Timestamp == 0 {
				// kubelet 开启了 Evented PLEG 但运行时未开启时，拿到的 pod 状态不会有时间戳。
				// 例如 CI job 'pull-kubernetes-e2e-gce-alpha-features' 会开启该特性
				// （其中包含 Evented PLEG），但所用的运行时不支持 Evented PLEG。
				logger.V(4).Info("Runtime does not set pod status timestamp")
				containerStatuses, activeContainerStatuses, err = m.getPodContainerStatuses(ctx, pod, activePodSandboxID)
				if err != nil {
					if m.logReduction.ShouldMessageBePrinted(err.Error(), podFullName) {
						logger.Error(err, "getPodContainerStatuses for pod failed")
					}
					return nil, err
				}
			} else {
				// 获取 pod 可见的所有容器状态，并使用 sandboxStatus 中的时间戳。
				timestamp = time.Unix(0, resp.Timestamp)
				for _, cs := range resp.ContainersStatuses {
					cStatus := m.convertToKubeContainerStatus(ctx, pod.ID, cs)
					containerStatuses = append(containerStatuses, cStatus)
				}
			}
		}
	}

	if !utilfeature.DefaultFeatureGate.Enabled(features.EventedPLEG) {
		// 获取 pod 中可见的所有容器状态。
		var err error
		containerStatuses, activeContainerStatuses, err = m.getPodContainerStatuses(ctx, pod, activePodSandboxID)
		if err != nil {
			if m.logReduction.ShouldMessageBePrinted(err.Error(), podFullName) {
				logger.Error(err, "getPodContainerStatuses for pod failed")
			}
			return nil, err
		}
	}

	m.logReduction.ClearID(podFullName)
	return &kubecontainer.PodStatus{
		ID:                      pod.ID,
		Name:                    pod.Name,
		Namespace:               pod.Namespace,
		IPs:                     podIPs,
		SandboxStatuses:         sandboxStatuses,
		ContainerStatuses:       containerStatuses,
		ActiveContainerStatuses: activeContainerStatuses,
		TimeStamp:               timestamp,
	}, nil
}

// GetContainerStatus 查询单个容器的状态。
func (m *kubeGenericRuntimeManager) GetContainerStatus(ctx context.Context, podUID kubetypes.UID, id kubecontainer.ContainerID) (*kubecontainer.Status, error) {
	resp, err := m.runtimeService.ContainerStatus(ctx, id.ID, false)
	if err != nil {
		return nil, fmt.Errorf("runtime container status: %w", err)
	}

	status := resp.GetStatus()
	if status == nil {
		return nil, remote.ErrContainerStatusNil
	}

	return m.convertToKubeContainerStatus(ctx, podUID, status), nil
}

// GarbageCollect 按照给定的容器 GC 策略清理死亡容器。
func (m *kubeGenericRuntimeManager) GarbageCollect(ctx context.Context, gcPolicy kubecontainer.GCPolicy, allSourcesReady bool, evictNonDeletedPods bool) error {
	logger := klog.FromContext(ctx)
	// 从 actuated 状态中移除已终止的 pod，避免检查点无限增长
	for uid := range m.actuatedState.GetPodResourceInfoMap() {
		if m.podStateProvider.ShouldPodContentBeRemoved(uid) {
			if err := m.actuatedState.RemovePod(logger, uid); err != nil {
				// 除了记录日志，无需对该错误做其他处理。
				logger.Error(err, "Failed to remove pod from actuated state", "podUID", uid)
			}
		}
	}

	return m.containerGC.GarbageCollect(ctx, gcPolicy, allSourcesReady, evictNonDeletedPods)
}

// UpdatePodCIDR 是一个透传方法，把 kubelet 提供的 podCIDR 更新到 shim 的 runtimeConfig 中。
func (m *kubeGenericRuntimeManager) UpdatePodCIDR(ctx context.Context, podCIDR string) error {
	logger := klog.FromContext(ctx)
	// TODO(#35531): do we really want to write a method on this manager for each
	// field of the config?
	logger.Info("Updating runtime config through cri with podcidr", "CIDR", podCIDR)
	return m.runtimeService.UpdateRuntimeConfig(ctx,
		&runtimeapi.RuntimeConfig{
			NetworkConfig: &runtimeapi.NetworkConfig{
				PodCidr: podCIDR,
			},
		})
}

// CheckpointContainer 通过 CRI 触发容器检查点（CRIU）。
func (m *kubeGenericRuntimeManager) CheckpointContainer(ctx context.Context, options *runtimeapi.CheckpointContainerRequest) error {
	return m.runtimeService.CheckpointContainer(ctx, options)
}

// ListMetricDescriptors 列出运行时可提供的指标描述符。
func (m *kubeGenericRuntimeManager) ListMetricDescriptors(ctx context.Context) ([]*runtimeapi.MetricDescriptor, error) {
	return m.runtimeService.ListMetricDescriptors(ctx)
}

// ListPodSandboxMetrics 列出运行时中各个 pod sandbox 的指标。
func (m *kubeGenericRuntimeManager) ListPodSandboxMetrics(ctx context.Context) ([]*runtimeapi.PodSandboxMetrics, error) {
	return m.runtimeService.ListPodSandboxMetrics(ctx)
}

// UpdateActuatedPodLevelResources 把已 actuate 的 pod 级资源写入检查点。
// 仅在特性开启且 pod 声明了资源时才有实际动作。
func (m *kubeGenericRuntimeManager) UpdateActuatedPodLevelResources(logger klog.Logger, actuatedPod *v1.Pod) error {

	if !utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling) {
		return nil
	}

	if actuatedPod.Spec.Resources == nil {
		return nil
	}

	if actuatedPod.Spec.Resources.Requests == nil && actuatedPod.Spec.Resources.Limits == nil {
		return nil
	}

	return m.actuatedState.SetPodLevelResources(logger, actuatedPod.UID, actuatedPod.Spec.Resources)
}

// isPodResizeInProgress 检查已 actuate 的可调整资源是否与已分配资源不一致：
//   - 任何运行中容器：以下差异会被忽略：
//   - 不可调整的容器：不可重启的 init 容器、ephemeral 容器
//   - 不可调整的资源：仅 CPU 与内存可调整
//   - 未运行的容器：它们会在（重）启动时被正确设置大小
//   - 若开启 InPlacePodLevelResourcesVerticalScaling，还包括任何运行中 pod。
//   - 若开启 InPlacePodVerticalScalingMemoryBackedVolumes，还包括任何运行中 pod。
func (m *kubeGenericRuntimeManager) IsPodResizeInProgress(allocatedPod *v1.Pod, podStatus *kubecontainer.PodStatus) bool {
	if m.isContainerResourceResizeInProgress(allocatedPod, podStatus) {
		return true
	}

	if m.isMemoryBackedVolumeResizeInProgress(allocatedPod) {
		return true
	}

	return m.isPodLevelResourcesResizeInProgress(allocatedPod, podStatus)
}

// isMemoryBackedVolumeResizeInProgress 检查内存支撑的 emptyDir volume 大小限制是否尚未生效。
func (m *kubeGenericRuntimeManager) isMemoryBackedVolumeResizeInProgress(allocatedPod *v1.Pod) bool {
	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingMemoryBackedVolumes) {
		for _, volume := range allocatedPod.Spec.Volumes {
			if !allocation.VolHasMemoryBackedEmptyDirSizeLimit(&volume) {
				continue
			}

			desiredLimit := volume.EmptyDir.SizeLimit
			actuatedLimit, found := m.actuatedState.GetEmptyDirVolumeLimit(allocatedPod.UID, volume.Name)
			if !found {
				// 尚无 actuation 检查点。
				continue
			}

			if desiredLimit != nil && !desiredLimit.Equal(*actuatedLimit) {
				return true
			}
		}
	}
	return false
}

// isContainerResourceResizeInProgress 检查是否有运行中容器已分配资源与 actuated 资源不一致。
func (m *kubeGenericRuntimeManager) isContainerResourceResizeInProgress(allocatedPod *v1.Pod, podStatus *kubecontainer.PodStatus) bool {
	return !podutil.VisitContainers(&allocatedPod.Spec, podutil.InitContainers|podutil.Containers,
		func(allocatedContainer *v1.Container, containerType podutil.ContainerType) (shouldContinue bool) {
			if !isResizableContainer(allocatedContainer, containerType) {
				return true
			}

			containerStatus := podStatus.FindContainerStatusByName(allocatedContainer.Name)
			if containerStatus == nil || containerStatus.State != kubecontainer.ContainerStateRunning {
				// 容器未运行，无需调整大小。
				return true
			}

			actuatedResources, _ := m.actuatedState.GetContainerResources(allocatedPod.UID, allocatedContainer.Name)
			allocatedResources := allocatedContainer.Resources
			if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodLevelResourcesVerticalScaling) {
				allocatedResources.Limits = kubeutil.GetLimits(&kubeutil.ResourceOpts{PodResources: allocatedPod.Spec.Resources, ContainerResources: &allocatedContainer.Resources})
			}

			return allocatedResources.Requests[v1.ResourceCPU].Equal(actuatedResources.Requests[v1.ResourceCPU]) &&
				allocatedResources.Limits[v1.ResourceCPU].Equal(actuatedResources.Limits[v1.ResourceCPU]) &&
				allocatedResources.Requests[v1.ResourceMemory].Equal(actuatedResources.Requests[v1.ResourceMemory]) &&
				allocatedResources.Limits[v1.ResourceMemory].Equal(actuatedResources.Limits[v1.ResourceMemory])
		})
}

// isPodLevelResourcesResizeInProgress 检查 pod 级资源的 actuated 值与分配值是否不一致。
func (m *kubeGenericRuntimeManager) isPodLevelResourcesResizeInProgress(allocatedPod *v1.Pod, podStatus *kubecontainer.PodStatus) bool {
	if !utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodLevelResourcesVerticalScaling) {
		return false
	}

	if allocatedPod.Spec.Resources == nil || podStatus == nil {
		return false
	}

	actuatedPodResources, found := m.actuatedState.GetPodLevelResources(allocatedPod.UID)
	if !found || actuatedPodResources == nil {
		return false
	}

	allocatedPodResources := allocatedPod.Spec.Resources

	return !cpuMemoryResourcesEqual(actuatedPodResources, allocatedPodResources)
}

// cpuMemoryResourcesEqual 比较两组资源中 CPU 与内存的 request/limit 是否相等。
// nil 与空列表视为等价（取值时缺省为零值）。
func cpuMemoryResourcesEqual(actuatedPodResources, allocatedPodResources *v1.ResourceRequirements) bool {
	if actuatedPodResources == nil && allocatedPodResources == nil {
		return true
	}

	cmpResources := func(actuated, allocated v1.ResourceList) bool {
		return actuated[v1.ResourceCPU].Equal(allocated[v1.ResourceCPU]) && actuated[v1.ResourceMemory].Equal(allocated[v1.ResourceMemory])
	}

	if actuatedPodResources == nil {
		return cmpResources(nil, allocatedPodResources.Requests) && cmpResources(nil, allocatedPodResources.Limits)
	}

	if allocatedPodResources == nil {
		return cmpResources(actuatedPodResources.Requests, nil) && cmpResources(actuatedPodResources.Limits, nil)

	}
	return cmpResources(actuatedPodResources.Requests, allocatedPodResources.Requests) && cmpResources(actuatedPodResources.Limits, allocatedPodResources.Limits)
}

// isResizableContainer 判断给定类型的容器是否支持原地调整大小：
// 只有 restartable init 容器与普通容器可以，ephemeral 容器不行。
func isResizableContainer(container *v1.Container, containerType podutil.ContainerType) bool {
	switch containerType {
	case podutil.InitContainers:
		return podutil.IsRestartableInitContainer(container)
	case podutil.Containers:
		return true
	default:
		return false
	}
}
