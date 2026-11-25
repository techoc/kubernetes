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

package kubelet

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	sysruntime "runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	cadvisorapi "github.com/google/cadvisor/lib/model"
	inuserns "github.com/moby/sys/userns"
	"github.com/opencontainers/selinux/go-selinux"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
	"go.opentelemetry.io/otel/trace"

	"k8s.io/client-go/informers"
	ndf "k8s.io/component-helpers/nodedeclaredfeatures"
	"k8s.io/mount-utils"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	v1qos "k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/tainttoleration"
	utilfs "k8s.io/kubernetes/pkg/util/filesystem"
	utilpod "k8s.io/kubernetes/pkg/util/pod"
	netutils "k8s.io/utils/net"
	"k8s.io/utils/ptr"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	versionutil "k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	"k8s.io/apiserver/pkg/server/flagz"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	coreinformersv1 "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	certutil "k8s.io/client-go/util/cert"
	"k8s.io/client-go/util/certificate"
	"k8s.io/client-go/util/flowcontrol"
	cloudprovider "k8s.io/cloud-provider"
	"k8s.io/component-base/version"
	"k8s.io/component-helpers/apimachinery/lease"
	resourcehelper "k8s.io/component-helpers/resource"
	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	remote "k8s.io/cri-client/pkg"
	"k8s.io/klog/v2"
	pluginwatcherapi "k8s.io/kubelet/pkg/apis/pluginregistration/v1"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/allocation"
	kubeletconfiginternal "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/apis/config/v1beta1"
	"k8s.io/kubernetes/pkg/kubelet/apis/podresources"
	"k8s.io/kubernetes/pkg/kubelet/apis/pods"
	"k8s.io/kubernetes/pkg/kubelet/cadvisor"
	kubeletcertificate "k8s.io/kubernetes/pkg/kubelet/certificate"
	"k8s.io/kubernetes/pkg/kubelet/clustertrustbundle"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/config"
	"k8s.io/kubernetes/pkg/kubelet/configmap"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/events"
	"k8s.io/kubernetes/pkg/kubelet/eviction"
	"k8s.io/kubernetes/pkg/kubelet/images"
	"k8s.io/kubernetes/pkg/kubelet/kubeletconfig"
	"k8s.io/kubernetes/pkg/kubelet/kuberuntime"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/logs"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/kubernetes/pkg/kubelet/metrics/collectors"
	"k8s.io/kubernetes/pkg/kubelet/network/dns"
	"k8s.io/kubernetes/pkg/kubelet/nodeshutdown"
	oomwatcher "k8s.io/kubernetes/pkg/kubelet/oom"
	"k8s.io/kubernetes/pkg/kubelet/pleg"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager"
	plugincache "k8s.io/kubernetes/pkg/kubelet/pluginmanager/cache"
	kubepod "k8s.io/kubernetes/pkg/kubelet/pod"
	"k8s.io/kubernetes/pkg/kubelet/podcertificate"
	"k8s.io/kubernetes/pkg/kubelet/preemption"
	"k8s.io/kubernetes/pkg/kubelet/prober"
	proberesults "k8s.io/kubernetes/pkg/kubelet/prober/results"
	"k8s.io/kubernetes/pkg/kubelet/runtimeclass"
	"k8s.io/kubernetes/pkg/kubelet/secret"
	"k8s.io/kubernetes/pkg/kubelet/server"
	servermetrics "k8s.io/kubernetes/pkg/kubelet/server/metrics"
	serverstats "k8s.io/kubernetes/pkg/kubelet/server/stats"
	"k8s.io/kubernetes/pkg/kubelet/stats"
	"k8s.io/kubernetes/pkg/kubelet/status"
	"k8s.io/kubernetes/pkg/kubelet/sysctl"
	"k8s.io/kubernetes/pkg/kubelet/token"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
	"k8s.io/kubernetes/pkg/kubelet/userns"
	"k8s.io/kubernetes/pkg/kubelet/util"
	"k8s.io/kubernetes/pkg/kubelet/util/manager"
	"k8s.io/kubernetes/pkg/kubelet/util/queue"
	"k8s.io/kubernetes/pkg/kubelet/util/sliceutils"
	"k8s.io/kubernetes/pkg/kubelet/volumemanager"
	"k8s.io/kubernetes/pkg/kubelet/watchdog"
	httpprobe "k8s.io/kubernetes/pkg/probe/http"
	"k8s.io/kubernetes/pkg/security/apparmor"
	"k8s.io/kubernetes/pkg/util/oom"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/csi"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	"k8s.io/kubernetes/pkg/volume/util/subpath"
	"k8s.io/kubernetes/pkg/volume/util/volumepathhandler"
	"k8s.io/utils/clock"
)

const (
	// 等待容器运行时启动的最长时间。
	maxWaitForContainerRuntime = 30 * time.Second

	// nodeStatusUpdateRetry 指定上报节点状态失败时 kubelet 的重试次数。
	nodeStatusUpdateRetry = 5

	// nodeReadyGracePeriod 是快速状态更新被终止前的宽限期；超过该时间后，
	// 容器运行时未就绪会以不受 verbosity 限制的方式记录日志。
	nodeReadyGracePeriod = 120 * time.Second

	// DefaultContainerLogsDir 是容器日志的存放位置。
	DefaultContainerLogsDir = "/var/log/containers"

	// MaxCrashLoopBackOff 是容器重启退避的最大周期，导出供 e2e 测试使用。
	MaxCrashLoopBackOff = v1beta1.MaxContainerBackOff

	// 启用 alpha 特性门控 ReduceDefaultCrashLoopBackOffDecay 时，
	// 容器重启的默认最大退避周期。
	reducedMaxCrashLoopBackOff = 60 * time.Second

	// 容器重启指数退避的初始周期。
	initialCrashLoopBackOff = time.Second * 10

	// 启用 alpha 特性门控 ReduceDefaultCrashLoopBackOffDecay 时，
	// 容器重启的默认初始退避周期。
	reducedInitialCrashLoopBackOff = 1 * time.Second

	// MaxImageBackOff 是镜像拉取退避的最大周期，导出供 e2e 测试使用。
	MaxImageBackOff = 300 * time.Second

	// 执行全局清理任务的周期。
	housekeepingPeriod = time.Second * 2

	// housekeeping 耗时超过该值即视为违反不变量：housekeeping 必须足够快，
	// 以免阻塞 pod 配置（housekeeping 运行期间不会启动或删除任何 pod）。
	housekeepingWarningDuration = time.Second * 1

	// 运行时缓存过期周期——略长于两次 housekeeping 之间的预期间隔，
	// 而 housekeeping 本身会显式刷新缓存。
	runtimeCacheRefreshPeriod = housekeepingPeriod + housekeepingWarningDuration

	// 执行驱逐监控的周期。
	// 需与 cadvisor 内部 housekeeping 保持同步。
	evictionMonitoringPeriod = time.Second * 10

	// 容器文件系统中 hosts 文件的挂载路径。
	linuxEtcHostsPath   = "/etc/hosts"
	windowsEtcHostsPath = "C:\\Windows\\System32\\drivers\\etc\\hosts"

	// 接收 pod 生命周期事件的 channel 容量。该数值比较随意，未来可能调整。
	plegChannelCapacity = 1000

	// Generic PLEG 依靠 relist 发现容器事件。
	// 周期越长，kubelet 发现容器变化并更新 pod 状态就越慢；周期越短，
	// relist 越频繁（例如容器运行时操作），CPU 占用越高。
	// 注意：即使周期设为 1s，若容器运行时响应缓慢或单轮内容器变化很多，
	// relist 本身也可能耗时超过 1s。
	genericPlegRelistPeriod    = time.Second * 1
	genericPlegRelistThreshold = time.Minute * 3

	// Evented PLEG 的 CRI 事件流重试上限。
	eventedPlegMaxStreamRetries = 5

	// pod 同步出错时的退避周期。
	backOffPeriod = time.Second * 10

	// 镜像拉取指数退避的初始周期。
	imageBackOffPeriod = time.Second * 10

	// ContainerGCPeriod 是执行容器垃圾回收的周期。
	ContainerGCPeriod = time.Minute
	// ImageGCPeriod 是执行镜像垃圾回收的周期。
	ImageGCPeriod = 5 * time.Minute

	// 每个 pod 中至少保留的死容器数量。
	minDeadContainerInPod = 1

	// nodeLeaseRenewIntervalFraction 是续约租约所用的租约时长比例。
	nodeLeaseRenewIntervalFraction = 0.25

	// instrumentationScope 是 OpenTelemetry 插桩作用域的名称。
	instrumentationScope = "k8s.io/kubernetes/pkg/kubelet"
)

var (
	// ContainerLogsDir 可在测试中覆盖。
	ContainerLogsDir = DefaultContainerLogsDir
	etcHostsPath     = getContainerEtcHostsPath()

	admissionRejectionReasons = sets.New[string](
		lifecycle.AppArmorNotAdmittedReason,
		lifecycle.PodOSSelectorNodeLabelDoesNotMatch,
		lifecycle.PodOSNotSupported,
		lifecycle.InvalidNodeInfo,
		lifecycle.InitContainerRestartPolicyForbidden,
		lifecycle.SupplementalGroupsPolicyNotSupported,
		lifecycle.UnexpectedAdmissionError,
		lifecycle.UnknownReason,
		lifecycle.UnexpectedPredicateFailureType,
		lifecycle.OutOfCPU,
		lifecycle.OutOfMemory,
		lifecycle.OutOfEphemeralStorage,
		lifecycle.OutOfPods,
		lifecycle.PodLevelResourcesNotAdmittedReason,
		lifecycle.PodFeatureUnsupported,
		tainttoleration.ErrReasonNotMatch,
		eviction.Reason,
		sysctl.ForbiddenReason,
		topologymanager.ErrorTopologyAffinity,
		nodeshutdown.NodeShutdownNotAdmittedReason,
		volumemanager.VolumeAttachmentLimitExceededReason,
	)

	// 为单元测试而导出。
	goos = sysruntime.GOOS
)

// 返回容器内 hosts 文件的挂载路径：Windows 使用系统盘路径，其他系统使用 /etc/hosts。
func getContainerEtcHostsPath() string {
	// 按目标操作系统选择路径：Windows 与 Linux 的 hosts 文件位置不同。
	if goos == "windows" {
		return windowsEtcHostsPath
	}
	return linuxEtcHostsPath
}

// SyncHandler 由 Kubelet 实现，抽象出同步处理器以便测试。
type SyncHandler interface {
	HandlePodAdditions(ctx context.Context, pods []*v1.Pod)
	HandlePodUpdates(ctx context.Context, pods []*v1.Pod)
	HandlePodRemoves(ctx context.Context, pods []*v1.Pod)
	HandlePodReconcile(ctx context.Context, pods []*v1.Pod)
	HandlePodSyncs(ctx context.Context, pods []*v1.Pod)
	HandlePodCleanups(ctx context.Context) error
}

// Option 是用于配置 Kubelet 的函数式选项类型。
type Option func(*Kubelet)

// Bootstrap 是 kubelet 的引导接口，覆盖初始化协议所需的方法。
type Bootstrap interface {
	GetConfiguration() kubeletconfiginternal.KubeletConfiguration
	BirthCry()
	StartGarbageCollection(ctx context.Context)
	ListenAndServe(ctx context.Context, kubeCfg *kubeletconfiginternal.KubeletConfiguration, tlsConfig *tls.Config, auth server.AuthInterface, tp trace.TracerProvider)
	ListenAndServeReadOnly(ctx context.Context, address net.IP, port uint, tp trace.TracerProvider)
	ListenAndServePodResources(ctx context.Context)
	ListenAndServePods(ctx context.Context)
	Run(ctx context.Context, updates <-chan kubetypes.PodUpdate)
}

// Dependencies 是一组在运行时构建、运行 Kubelet 所必需的“注入依赖”。
// 将这些对象集中于此只是临时方案，待 Kubelet 的依赖注入方案完善后再调整。
type Dependencies struct {
	Options []Option

	// 注入的依赖。
	Flagz                     flagz.Reader
	Auth                      server.AuthInterface
	CAdvisorInterface         cadvisor.Interface
	ContainerManager          cm.ContainerManager
	EventClient               v1core.EventsGetter
	HeartbeatClient           clientset.Interface
	OnHeartbeatFailure        func()
	KubeClient                clientset.Interface
	Mounter                   mount.Interface
	HostUtil                  hostutil.HostUtils
	OOMAdjuster               *oom.OOMAdjuster
	OSInterface               kubecontainer.OSInterface
	PodConfig                 *config.PodConfig
	ProbeManager              prober.Manager
	Recorder                  record.EventRecorderLogger
	Subpather                 subpath.Interface
	TracerProvider            trace.TracerProvider
	VolumePlugins             []volume.VolumePlugin
	DynamicPluginProber       volume.DynamicPluginProber
	TLSOptions                *server.TLSOptions
	TLSConfig                 *tls.Config
	RemoteRuntimeService      internalapi.RuntimeService
	RemoteImageService        internalapi.ImageManagerService
	PodStartupLatencyTracker  util.PodStartupLatencyTracker
	NodeStartupLatencyTracker util.NodeStartupLatencyTracker
	HealthChecker             watchdog.HealthChecker
	// cadvisor.UsingLegacyCadvisorStats 移除后即可删除该字段。
	useLegacyCadvisorStats bool
	// 若 CRI 不支持 PodSandboxStats，初始化时会设置 PodSandboxStatsUnimplemented。
	// 待 PodAndContainerStatsFromCRI 进入 GA 且不再以 cadvisor 统计作为兜底后，
	// 即可删除该字段。
	PodSandboxStatsUnimplemented bool
}

// newCrashLoopBackOff 依据 alpha 特性门控与 kubelet 配置，计算容器重启退避的最大周期与初始周期。
func newCrashLoopBackOff(kubeCfg *kubeletconfiginternal.KubeletConfiguration) (time.Duration, time.Duration) {
	boMax := MaxCrashLoopBackOff
	boInitial := initialCrashLoopBackOff
	if utilfeature.DefaultFeatureGate.Enabled(features.ReduceDefaultCrashLoopBackOffDecay) {
		boMax = reducedMaxCrashLoopBackOff
		boInitial = reducedInitialCrashLoopBackOff
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.KubeletCrashLoopBackOffMax) {
		// 只要合法，运维方显式配置始终优先。
		boMax = kubeCfg.CrashLoopBackOff.MaxContainerRestartPeriod.Duration
		if boMax < boInitial {
			boInitial = boMax
		}
	}
	return boMax, boInitial
}

// makePodSourceConfig 根据 KubeletConfiguration 构造 config.PodConfig，聚合静态 Pod 文件、URL 与 apiserver 三类配置来源。
func makePodSourceConfig(ctx context.Context, kubeCfg *kubeletconfiginternal.KubeletConfiguration, kubeDeps *Dependencies, nodeName types.NodeName, nodeHasSynced func() bool) (*config.PodConfig, error) {
	logger := klog.FromContext(ctx)
	manifestURLHeader := make(http.Header)
	if len(kubeCfg.StaticPodURLHeader) > 0 {
		for k, v := range kubeCfg.StaticPodURLHeader {
			for i := range v {
				manifestURLHeader.Add(k, v[i])
			}
		}
	}

	// 所有配置的统一来源。
	cfg := config.NewPodConfig(kubeDeps.Recorder, kubeDeps.PodStartupLatencyTracker)

	// 定义文件配置来源。
	if kubeCfg.StaticPodPath != "" {
		logger.Info("Adding static pod path", "path", kubeCfg.StaticPodPath)
		config.NewSourceFile(logger, kubeCfg.StaticPodPath, nodeName, kubeCfg.FileCheckFrequency.Duration, cfg.Channel(ctx, kubetypes.FileSource))
	}

	// 定义 URL 配置来源。
	if kubeCfg.StaticPodURL != "" {
		logger.Info("Adding pod URL with HTTP header", "URL", kubeCfg.StaticPodURL, "header", manifestURLHeader)
		config.NewSourceURL(logger, kubeCfg.StaticPodURL, manifestURLHeader, nodeName, kubeCfg.HTTPCheckFrequency.Duration, cfg.Channel(ctx, kubetypes.HTTPSource))
	}

	if kubeDeps.KubeClient != nil {
		logger.Info("Adding apiserver pod source")
		config.NewSourceApiserver(logger, kubeDeps.KubeClient, nodeName, nodeHasSynced, cfg.Channel(ctx, kubetypes.ApiserverSource))
	}
	return cfg, nil
}

// PreInitRuntimeService 在 RunKubelet 之前初始化 CRI 运行时服务与镜像服务。
func PreInitRuntimeService(ctx context.Context, kubeCfg *kubeletconfiginternal.KubeletConfiguration, kubeDeps *Dependencies) error {
	remoteImageEndpoint := kubeCfg.ImageServiceEndpoint
	if remoteImageEndpoint == "" && kubeCfg.ContainerRuntimeEndpoint != "" {
		remoteImageEndpoint = kubeCfg.ContainerRuntimeEndpoint
	}
	var err error
	useStreaming := utilfeature.DefaultFeatureGate.Enabled(features.CRIListStreaming)
	if kubeDeps.RemoteRuntimeService, err = remote.NewRemoteRuntimeServiceBuilder().
		WithEndpoint(kubeCfg.ContainerRuntimeEndpoint).
		WithConnectionTimeout(kubeCfg.RuntimeRequestTimeout.Duration).
		WithTracerProvider(kubeDeps.TracerProvider).
		WithUseStreaming(useStreaming).
		Build(ctx); err != nil {
		return err
	}
	if kubeDeps.RemoteImageService, err = remote.NewRemoteImageServiceBuilder().
		WithEndpoint(remoteImageEndpoint).
		WithConnectionTimeout(kubeCfg.RuntimeRequestTimeout.Duration).
		WithTracerProvider(kubeDeps.TracerProvider).
		WithUseStreaming(useStreaming).
		Build(ctx); err != nil {
		return err
	}

	kubeDeps.useLegacyCadvisorStats = cadvisor.UsingLegacyCadvisorStats(kubeCfg.ContainerRuntimeEndpoint)

	return nil
}

// newNodeHasSyncedFunc 返回一个 cache.InformerSynced：仅当本 kubelet 对应的 Node 对象进入 informer 缓存后才报告 true。
func newNodeHasSyncedFunc(nodeLister corelisters.NodeLister, nodeName types.NodeName) cache.InformerSynced {
	var synced atomic.Bool
	return func() bool {
		if synced.Load() {
			return true
		}
		if _, err := nodeLister.Get(string(nodeName)); err == nil {
			synced.Store(true)
			return true
		}
		return false
	}
}

// NewMainKubelet 创建 Kubelet 及运行所需的全部内部模块。
// 这里只做构造，不对 Kubelet 及其模块执行启动初始化。
func NewMainKubelet(ctx context.Context,
	kubeCfg *kubeletconfiginternal.KubeletConfiguration,
	kubeDeps *Dependencies,
	crOptions *kubeletconfig.ContainerRuntimeOptions,
	hostname string,
	nodeName types.NodeName,
	nodeIPs []net.IP,
	providerID string,
	cloudProvider string,
	certDirectory string,
	rootDirectory string,
	podLogsDirectory string,
	imageCredentialProviderConfigPath string,
	imageCredentialProviderBinDir string,
	registerNode bool,
	registerWithTaints []v1.Taint,
	allowedUnsafeSysctls []string,
	experimentalMounterPath string,
	kernelMemcgNotification bool,
	experimentalNodeAllocatableIgnoreEvictionThreshold bool,
	minimumGCAge metav1.Duration,
	maxPerPodContainerCount int32,
	maxContainerCount int32,
	nodeLabels map[string]string,
	nodeStatusMaxImages int32,
	seccompDefault bool,
) (*Kubelet, error) {
	logger := klog.FromContext(ctx)

	if rootDirectory == "" {
		return nil, fmt.Errorf("invalid root directory %q", rootDirectory)
	}
	if podLogsDirectory == "" {
		return nil, errors.New("pod logs root directory is empty")
	}
	if kubeCfg.SyncFrequency.Duration <= 0 {
		return nil, fmt.Errorf("invalid sync frequency %d", kubeCfg.SyncFrequency.Duration)
	}

	if !cloudprovider.IsExternal(cloudProvider) && len(cloudProvider) != 0 {
		// 树内云厂商实现已被移除，这里显式报错，避免静默降级为无云厂商模式。
		cloudprovider.DisableWarningForProvider(cloudProvider)
		return nil, cloudprovider.ErrorForDisabledProvider(cloudProvider)
	}

	var nodeHasSynced cache.InformerSynced
	var nodeInformer coreinformersv1.NodeInformer
	var nodeLister corelisters.NodeLister

	// kubeClient == nil 表示以独立模式运行（没有 apiserver）；
	// 非 nil 表示运行在集群中，需要与 API 同步。
	if kubeDeps.KubeClient != nil {
		// 只 watch 本节点的 Node 对象，避免缓存整个集群的节点。
		kubeInformers := informers.NewSharedInformerFactoryWithOptions(kubeDeps.KubeClient, 0, informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.FieldSelector = fields.Set{metav1.ObjectNameField: string(nodeName)}.String()
		}))
		nodeInformer = kubeInformers.Core().V1().Nodes()
		nodeLister = nodeInformer.Lister()
		nodeHasSynced = newNodeHasSyncedFunc(nodeLister, nodeName)
		kubeInformers.Start(wait.NeverStop)
		logger.Info("Attempting to sync node with API server")
	} else {
		// 没有可用的客户端进行同步！
		nodeIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
		nodeLister = corelisters.NewNodeLister(nodeIndexer)
		nodeHasSynced = func() bool { return true }
		logger.Info("Kubelet is running in standalone mode, will skip API server sync")
	}

	if kubeDeps.PodConfig == nil {
		var err error
		kubeDeps.PodConfig, err = makePodSourceConfig(ctx, kubeCfg, kubeDeps, nodeName, nodeHasSynced)
		if err != nil {
			return nil, err
		}
	}

	containerGCPolicy := kubecontainer.GCPolicy{
		MinAge:             minimumGCAge.Duration,
		MaxPerPodContainer: int(maxPerPodContainerCount),
		MaxContainers:      int(maxContainerCount),
	}

	daemonEndpoints := &v1.NodeDaemonEndpoints{
		KubeletEndpoint: v1.DaemonEndpoint{Port: kubeCfg.Port},
	}

	imageGCPolicy := images.ImageGCPolicy{
		MinAge:               kubeCfg.ImageMinimumGCAge.Duration,
		HighThresholdPercent: int(kubeCfg.ImageGCHighThresholdPercent),
		LowThresholdPercent:  int(kubeCfg.ImageGCLowThresholdPercent),
	}

	imageGCPolicy.MaxAge = kubeCfg.ImageMaximumGCAge.Duration

	enforceNodeAllocatable := kubeCfg.EnforceNodeAllocatable
	if experimentalNodeAllocatableIgnoreEvictionThreshold {
		// 若不强制执行驱逐，则不要把 kubeCfg.EnforceNodeAllocatable 传给驱逐阈值解析。
		enforceNodeAllocatable = []string{}
	}
	thresholds, err := eviction.ParseThresholdConfig(enforceNodeAllocatable, kubeCfg.EvictionHard, kubeCfg.EvictionSoft, kubeCfg.EvictionSoftGracePeriod, kubeCfg.EvictionMinimumReclaim)
	if err != nil {
		return nil, err
	}
	evictionConfig := eviction.Config{
		PressureTransitionPeriod: kubeCfg.EvictionPressureTransitionPeriod.Duration,
		MaxPodGracePeriodSeconds: int64(kubeCfg.EvictionMaxPodGracePeriod),
		Thresholds:               thresholds,
		KernelMemcgNotification:  kernelMemcgNotification,
		PodCgroupRoot:            kubeDeps.ContainerManager.GetPodCgroupRoot(),
	}

	var serviceLister corelisters.ServiceLister
	var serviceHasSynced cache.InformerSynced
	if kubeDeps.KubeClient != nil {
		// 不监听 headless service：该 informer 只用于生成 pod 环境变量，不需要它们。
		// 参见 https://issues.k8s.io/122394
		kubeInformers := informers.NewSharedInformerFactoryWithOptions(kubeDeps.KubeClient, 0, informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.FieldSelector = fields.OneTermNotEqualSelector("spec.clusterIP", v1.ClusterIPNone).String()
		}))
		serviceLister = kubeInformers.Core().V1().Services().Lister()
		serviceHasSynced = kubeInformers.Core().V1().Services().Informer().HasSynced
		kubeInformers.Start(wait.NeverStop)
	} else {
		serviceIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
		serviceLister = corelisters.NewServiceLister(serviceIndexer)
		serviceHasSynced = func() bool { return true }
	}

	// 构造用于事件记录的 node 引用。
	nodeRef := &v1.ObjectReference{
		APIVersion: "v1",
		Kind:       "Node",
		Name:       string(nodeName),
		Namespace:  "",
	}

	oomWatcher, err := oomwatcher.NewWatcher(kubeDeps.Recorder)
	if err != nil {
		if inuserns.RunningInUserNS() {
			if utilfeature.DefaultFeatureGate.Enabled(features.KubeletInUserNamespace) {
				// 在启用 sysctl `kernel.dmesg_restrict=1` 的 user namespace 中运行时，
				// oomwatcher.NewWatcher 会返回 "open /dev/kmsg: operation not permitted" 错误。
				logger.V(2).Info("Failed to create an oomWatcher (running in UserNS, ignoring)", "err", err)
				oomWatcher = nil
			} else {
				logger.Error(err, "Failed to create an oomWatcher (running in UserNS, Hint: enable KubeletInUserNamespace feature flag to ignore the error)")
				return nil, err
			}
		} else {
			return nil, err
		}
	}

	clusterDNS := make([]net.IP, 0, len(kubeCfg.ClusterDNS))
	for _, ipEntry := range kubeCfg.ClusterDNS {
		ip := netutils.ParseIPSloppy(ipEntry)
		if ip == nil {
			logger.Info("Invalid clusterDNS IP", "IP", ipEntry)
		} else {
			clusterDNS = append(clusterDNS, ip)
		}
	}

	// 容器生命周期回调基于 HTTPS，因此需要 TLS transport，
	// 但我们没有做 TLS 校验所需的信息。
	//
	// 禁止给该客户端添加凭据：凭据绝不能从客户端泄露到任意主机，这一点至关重要。
	insecureContainerLifecycleHTTPClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
		CheckRedirect: httpprobe.RedirectChecker(false),
	}

	tracer := kubeDeps.TracerProvider.Tracer(instrumentationScope)

	klet := &Kubelet{
		hostname:                     hostname,
		nodeName:                     nodeName,
		kubeClient:                   kubeDeps.KubeClient,
		heartbeatClient:              kubeDeps.HeartbeatClient,
		onRepeatedHeartbeatFailure:   kubeDeps.OnHeartbeatFailure,
		rootDirectory:                filepath.Clean(rootDirectory),
		podLogsDirectory:             podLogsDirectory,
		resyncInterval:               kubeCfg.SyncFrequency.Duration,
		sourcesReady:                 config.NewSourcesReady(kubeDeps.PodConfig.SourcesReadyFn(logger)),
		registerNode:                 registerNode,
		registerWithTaints:           registerWithTaints,
		dnsConfigurer:                dns.NewConfigurer(kubeDeps.Recorder, nodeRef, nodeIPs, clusterDNS, kubeCfg.ClusterDomain, kubeCfg.ResolverConfig),
		serviceLister:                serviceLister,
		serviceHasSynced:             serviceHasSynced,
		nodeLister:                   nodeLister,
		nodeHasSynced:                nodeHasSynced,
		recorder:                     kubeDeps.Recorder,
		cadvisor:                     kubeDeps.CAdvisorInterface,
		externalCloudProvider:        cloudprovider.IsExternal(cloudProvider),
		providerID:                   providerID,
		nodeRef:                      nodeRef,
		nodeLabels:                   nodeLabels,
		nodeStatusUpdateFrequency:    kubeCfg.NodeStatusUpdateFrequency.Duration,
		nodeStatusReportFrequency:    kubeCfg.NodeStatusReportFrequency.Duration,
		os:                           kubeDeps.OSInterface,
		oomWatcher:                   oomWatcher,
		cgroupsPerQOS:                kubeCfg.CgroupsPerQOS,
		cgroupRoot:                   kubeCfg.CgroupRoot,
		mounter:                      kubeDeps.Mounter,
		hostutil:                     kubeDeps.HostUtil,
		subpather:                    kubeDeps.Subpather,
		maxPods:                      int(kubeCfg.MaxPods),
		podsPerCore:                  int(kubeCfg.PodsPerCore),
		syncLoopMonitor:              atomic.Value{},
		daemonEndpoints:              daemonEndpoints,
		containerManager:             kubeDeps.ContainerManager,
		nodeIPs:                      nodeIPs,
		nodeIPValidator:              validateNodeIP,
		clock:                        clock.RealClock{},
		enableControllerAttachDetach: kubeCfg.EnableControllerAttachDetach,
		makeIPTablesUtilChains:       kubeCfg.MakeIPTablesUtilChains,
		nodeStatusMaxImages:          nodeStatusMaxImages,
		pluginManagerStopCh:          wait.NeverStop,
		tracer:                       tracer,
		nodeStartupLatencyTracker:    kubeDeps.NodeStartupLatencyTracker,
		podStartupLatencyTracker:     kubeDeps.PodStartupLatencyTracker,
		healthChecker:                kubeDeps.HealthChecker,
		flagz:                        kubeDeps.Flagz,
	}

	var secretManager secret.Manager
	var configMapManager configmap.Manager
	if klet.kubeClient != nil {
		switch kubeCfg.ConfigMapAndSecretChangeDetectionStrategy {
		case kubeletconfiginternal.WatchChangeDetectionStrategy:
			secretManager = secret.NewWatchingSecretManager(klet.kubeClient, klet.resyncInterval)
			configMapManager = configmap.NewWatchingConfigMapManager(klet.kubeClient, klet.resyncInterval)
		case kubeletconfiginternal.TTLCacheChangeDetectionStrategy:
			secretManager = secret.NewCachingSecretManager(
				klet.kubeClient, manager.GetObjectTTLFromNodeFunc(ctx, klet.GetNode))
			configMapManager = configmap.NewCachingConfigMapManager(
				klet.kubeClient, manager.GetObjectTTLFromNodeFunc(ctx, klet.GetNode))
		case kubeletconfiginternal.GetChangeDetectionStrategy:
			secretManager = secret.NewSimpleSecretManager(klet.kubeClient)
			configMapManager = configmap.NewSimpleConfigMapManager(klet.kubeClient)
		default:
			return nil, fmt.Errorf("unknown configmap and secret manager mode: %v", kubeCfg.ConfigMapAndSecretChangeDetectionStrategy)
		}

		klet.secretManager = secretManager
		klet.configMapManager = configMapManager
	}

	machineInfo, err := klet.cadvisor.MachineInfo(logger)
	if err != nil {
		return nil, err
	}
	// 避免采集器把它当作带时间戳的指标。
	// 详见 PR #95210 与 #97006。
	machineInfo.Timestamp = time.Time{}
	klet.setCachedMachineInfo(machineInfo)

	imageBackOff := flowcontrol.NewBackOff(imageBackOffPeriod, MaxImageBackOff)

	klet.livenessManager = proberesults.NewManager()
	klet.readinessManager = proberesults.NewManager()
	klet.startupManager = proberesults.NewManager()
	podCache := kubecontainer.NewCache()
	klet.podCache = podCache

	klet.mirrorPodClient = kubepod.NewBasicMirrorClient(klet.kubeClient, string(nodeName), nodeLister)
	klet.podManager = kubepod.NewBasicPodManager()

	klet.statusManager = status.NewManager(klet.kubeClient, klet.podManager, klet, kubeDeps.PodStartupLatencyTracker)

	broadcaster := pods.NewBroadcaster(ctx)
	klet.podsServer = pods.NewPodsServer(broadcaster, klet.podManager, klet.statusManager, klet.sourcesReady)
	if utilfeature.DefaultFeatureGate.Enabled(features.PodsAPI) {
		klet.statusManager.AddPodUpdateNotifier(klet.podsServer)
	}
	klet.allocationManager = allocation.NewManager(
		klet.getRootDir(),
		klet.statusManager,
		klet.syncPodNow,
		klet.GetActivePods,
		klet.podManager.GetPodByUID,
		klet.sourcesReady,
		kubeDeps.Recorder,
		logger,
	)

	klet.resourceAnalyzer = serverstats.NewResourceAnalyzer(ctx, klet, kubeCfg.VolumeStatsAggPeriod.Duration, kubeDeps.Recorder)
	klet.runtimeService = kubeDeps.RemoteRuntimeService

	if kubeDeps.KubeClient != nil {
		klet.runtimeClassManager = runtimeclass.NewManager(kubeDeps.KubeClient)
	}

	// 为 CRI 容器运行时初始化 containerLogManager。
	containerLogManager, err := logs.NewContainerLogManager(
		klet.runtimeService,
		kubeDeps.OSInterface,
		kubeCfg.ContainerLogMaxSize,
		int(kubeCfg.ContainerLogMaxFiles),
		int(kubeCfg.ContainerLogMaxWorkers),
		kubeCfg.ContainerLogMonitorInterval,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize container log manager: %v", err)
	}
	klet.containerLogManager = containerLogManager

	klet.reasonCache = NewReasonCache()
	klet.workQueue = queue.NewBasicWorkQueue(klet.clock)
	klet.podWorkers = newPodWorkers(
		klet,
		kubeDeps.Recorder,
		klet.workQueue,
		klet.resyncInterval,
		backOffPeriod,
		klet.podCache,
		klet.allocationManager,
	)

	var singleProcessOOMKill *bool
	if sysruntime.GOOS == "linux" {
		if !util.IsCgroup2UnifiedMode() {
			// 这是 cgroups v1 下的默认行为。
			singleProcessOOMKill = ptr.To(true)
		} else {
			if kubeCfg.SingleProcessOOMKill == nil {
				singleProcessOOMKill = ptr.To(false)
			} else {
				singleProcessOOMKill = kubeCfg.SingleProcessOOMKill
			}
		}
	}

	tokenManager := token.NewManager(kubeDeps.KubeClient)
	getServiceAccount := func(namespace, name string) (*v1.ServiceAccount, error) {
		return nil, fmt.Errorf("get service account is not implemented")
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.KubeletServiceAccountTokenForCredentialProviders) {
		getServiceAccount = func(namespace, name string) (*v1.ServiceAccount, error) {
			if klet.kubeClient == nil {
				return nil, errors.New("cannot get ServiceAccounts when kubelet is in standalone mode")
			}
			return klet.kubeClient.CoreV1().ServiceAccounts(namespace).Get(ctx, name, metav1.GetOptions{})
		}
	}

	runtime, postImageGCHooks, err := kuberuntime.NewKubeGenericRuntimeManager(
		ctx,
		kubecontainer.FilterEventRecorder(kubeDeps.Recorder),
		klet.livenessManager,
		klet.readinessManager,
		klet.startupManager,
		rootDirectory,
		podLogsDirectory,
		machineInfo,
		klet.podWorkers,
		kubeCfg.MaxPods,
		kubeDeps.OSInterface,
		klet,
		insecureContainerLifecycleHTTPClient,
		imageBackOff,
		kubeCfg.SerializeImagePulls,
		kubeCfg.MaxParallelImagePulls,
		float32(kubeCfg.RegistryPullQPS),
		int(kubeCfg.RegistryBurst),
		kubeCfg.ImagePullCredentialsVerificationPolicy,
		kubeCfg.PreloadedImagesVerificationAllowlist,
		imageCredentialProviderConfigPath,
		imageCredentialProviderBinDir,
		singleProcessOOMKill,
		kubeCfg.CPUCFSQuota,
		kubeCfg.CPUCFSQuotaPeriod,
		kubeCfg.DefaultPodSysctls,
		kubeDeps.RemoteRuntimeService,
		kubeDeps.RemoteImageService,
		kubeDeps.ContainerManager,
		klet.containerLogManager,
		klet.runtimeClassManager,
		seccompDefault,
		kubeCfg.MemorySwap.SwapBehavior,
		kubeDeps.ContainerManager.GetNodeAllocatableAbsolute,
		kubeCfg.MemoryThrottlingFactor,
		kubeCfg.MemoryReservationPolicy,
		klet.podStartupLatencyTracker,
		kubeDeps.TracerProvider,
		tokenManager,
		getServiceAccount,
		klet.podStartupLatencyTracker,
	)
	if err != nil {
		return nil, err
	}
	klet.containerRuntime = runtime
	klet.streamingRuntime = runtime
	klet.runner = runtime
	resizeAdmitHandler := allocation.NewPodResizesAdmitHandler(klet.containerManager, runtime, klet.allocationManager)

	runtimeCache, err := kubecontainer.NewRuntimeCache(klet.containerRuntime, runtimeCacheRefreshPeriod)
	if err != nil {
		return nil, err
	}
	klet.runtimeCache = runtimeCache

	// 获取 kubelet 所管理 pod 关联的宿主文件系统用量的通用 provider。
	hostStatsProvider := stats.NewHostStatsProvider(kubecontainer.RealOS{}, func(podUID types.UID) string {
		return getEtcHostsPath(klet.getPodDir(podUID))
	}, podLogsDirectory)

	cadvisorStatsProvider := stats.NewCadvisorStatsProvider(
		klet.cadvisor,
		klet.resourceAnalyzer,
		klet.podManager,
		klet.containerRuntime,
		klet.statusManager,
		hostStatsProvider,
		kubeDeps.ContainerManager,
	)
	if kubeDeps.useLegacyCadvisorStats {
		klet.StatsProvider = cadvisorStatsProvider
	} else {
		klet.StatsProvider = stats.NewCRIStatsProvider(
			klet.cadvisor,
			klet.resourceAnalyzer,
			klet.podManager,
			kubeDeps.RemoteRuntimeService,
			kubeDeps.RemoteImageService,
			hostStatsProvider,
			utilfeature.DefaultFeatureGate.Enabled(features.PodAndContainerStatsFromCRI),
			kubeDeps.PodSandboxStatsUnimplemented,
			cadvisorStatsProvider,
		)
	}

	eventChannel := make(chan *pleg.PodLifecycleEvent, plegChannelCapacity)
	genericRelistDuration := &pleg.RelistDuration{
		RelistPeriod:    genericPlegRelistPeriod,
		RelistThreshold: genericPlegRelistThreshold,
	}
	klet.pleg = pleg.NewGenericPLEG(klet.containerRuntime, eventChannel, genericRelistDuration, podCache, clock.RealClock{})

	if utilfeature.DefaultFeatureGate.Enabled(features.EventedPLEG) {
		// EventedPLEG 只监听 CRI 事件流并触发 relist；
		// 缓存更新与生命周期事件仍由 GenericPLEG 负责。
		klet.eventedPlegWatcher = pleg.NewEventedPLEG(klet.runtimeService, klet.pleg, eventedPlegMaxStreamRetries)
	}

	klet.runtimeState = newRuntimeState(maxWaitForContainerRuntime)
	klet.runtimeState.addHealthCheck("PLEG", klet.pleg.Healthy)
	if _, err := klet.updatePodCIDR(ctx, kubeCfg.PodCIDR); err != nil {
		logger.Error(err, "Pod CIDR update failed")
	}

	// 初始化容器垃圾回收。
	containerGC, err := kubecontainer.NewContainerGC(klet.containerRuntime, containerGCPolicy, klet.sourcesReady)
	if err != nil {
		return nil, err
	}
	klet.containerGC = containerGC
	klet.containerDeletor = newPodContainerDeletor(logger, klet.containerRuntime, max(containerGCPolicy.MaxPerPodContainer, minDeadContainerInPod))

	// 初始化镜像管理器。
	imageManager, err := images.NewImageGCManager(klet.containerRuntime, klet.StatsProvider, postImageGCHooks, kubeDeps.Recorder, nodeRef, imageGCPolicy, kubeDeps.TracerProvider)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize image manager: %v", err)
	}
	klet.imageManager = imageManager

	if kubeDeps.TLSOptions != nil {
		kubeDeps.TLSConfig = &tls.Config{}

		kubeDeps.TLSConfig.MinVersion = kubeDeps.TLSOptions.MinVersion
		kubeDeps.TLSConfig.CipherSuites = kubeDeps.TLSOptions.CipherSuites
		kubeDeps.TLSConfig.CurvePreferences = kubeDeps.TLSOptions.CurvePreferences

		getServingCertificate := func() *tls.Certificate { return nil }

		// 自动轮换服务端证书的 kubelet 配置。
		if kubeCfg.ServerTLSBootstrap && utilfeature.DefaultFeatureGate.Enabled(features.RotateKubeletServerCertificate) {
			klet.serverCertificateManager, err = kubeletcertificate.NewKubeletServerCertificateManager(klet.kubeClient, kubeCfg, klet.nodeName, func() []v1.NodeAddress {
				return klet.getLastObservedNodeAddresses(ctx)
			}, certDirectory)
			if err != nil {
				return nil, fmt.Errorf("failed to initialize certificate manager: %w", err)
			}
			getServingCertificate = klet.serverCertificateManager.Current

		} else if kubeDeps.TLSOptions.CertFile != "" && kubeDeps.TLSOptions.KeyFile != "" {

			if utilfeature.DefaultFeatureGate.Enabled(features.ReloadKubeletServerCertificateFile) {
				// 证书更新时从磁盘重新加载服务端证书的 kubelet 配置。
				klet.serverCertificateManager, err = kubeletcertificate.NewKubeletServerCertificateDynamicFileManager(kubeDeps.TLSOptions.CertFile, kubeDeps.TLSOptions.KeyFile)
				if err != nil {
					return nil, fmt.Errorf("failed to initialize file based certificate manager: %w", err)
				}
				getServingCertificate = klet.serverCertificateManager.Current

			} else {
				// 使用静态服务端证书的 kubelet 配置。
				servingCert, err := tls.LoadX509KeyPair(kubeDeps.TLSOptions.CertFile, kubeDeps.TLSOptions.KeyFile)
				if err != nil {
					return nil, fmt.Errorf("failed to load certificate: %w", err)
				}
				getServingCertificate = func() *tls.Certificate { return &servingCert }
			}
		}

		kubeDeps.TLSConfig.GetCertificate = func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			cert := getServingCertificate()
			if cert == nil {
				return nil, fmt.Errorf("no serving certificate available for the kubelet")
			}
			return cert, nil
		}

		if kubeDeps.TLSOptions.ClientCAFile != "" {
			// 在请求中填充 PeerCertificates，但不拒绝未通过证书校验的连接。
			kubeDeps.TLSConfig.ClientAuth = tls.RequestClientCert

			if utilfeature.DefaultFeatureGate.Enabled(features.ReloadKubeletClientCAFile) {
				// 更新时从磁盘重新加载客户端 CA Bundle 的 kubelet 配置。
				klet.clientCAManager = dynamiccertificates.NewDynamicServingCertificateController(kubeDeps.TLSConfig.Clone(), kubeDeps.Auth, nil, nil, nil)

				err := klet.clientCAManager.RunOnce()
				if err != nil {
					return nil, fmt.Errorf("unable to load client CA file %s: %w", kubeDeps.TLSOptions.ClientCAFile, err)
				}

				kubeDeps.TLSConfig.GetConfigForClient = klet.clientCAManager.GetConfigForClient

			} else {
				// 使用静态客户端 CA Bundle 的 kubelet 配置。
				clientCAs, err := certutil.NewPool(kubeDeps.TLSOptions.ClientCAFile)
				if err != nil {
					return nil, fmt.Errorf("unable to load client CA file %s: %w", kubeDeps.TLSOptions.ClientCAFile, err)
				}
				kubeDeps.TLSConfig.ClientCAs = clientCAs
			}
		}
	}

	if kubeDeps.ProbeManager != nil {
		klet.probeManager = kubeDeps.ProbeManager
	} else {
		klet.probeManager = prober.NewManager(
			klet.statusManager,
			klet.livenessManager,
			klet.readinessManager,
			klet.startupManager,
			klet.runner,
			kubeDeps.Recorder)
	}

	var clusterTrustBundleManager clustertrustbundle.Manager = &clustertrustbundle.NoopManager{}
	if kubeDeps.KubeClient != nil && utilfeature.DefaultFeatureGate.Enabled(features.ClusterTrustBundleProjection) {
		clusterTrustBundleManager = clustertrustbundle.NewLazyInformerManager(ctx, kubeDeps.KubeClient, 2*int(kubeCfg.MaxPods))
		logger.Info("ClusterTrustBundle informer will be started eventually once a trust bundle is requested")
	} else {
		logger.Info("Not starting ClusterTrustBundle informer because we are in static kubelet mode or the ClusterTrustBundleProjection featuregate is disabled")
	}

	if kubeDeps.KubeClient != nil && utilfeature.DefaultFeatureGate.Enabled(features.PodCertificateRequest) {
		kubeInformers := informers.NewSharedInformerFactoryWithOptions(
			kubeDeps.KubeClient,
			0,
			informers.WithTweakListOptions(func(options *metav1.ListOptions) {
				options.FieldSelector = fields.OneTermEqualSelector("spec.nodeName", string(nodeName)).String()
			}),
		)
		podCertificateManager := podcertificate.NewIssuingManager(
			kubeDeps.KubeClient,
			klet.podManager,
			kubeDeps.Recorder,
			kubeInformers.Certificates().V1().PodCertificateRequests(),
			nodeInformer,
			nodeName,
			clock.RealClock{},
		)
		klet.podCertificateManager = podCertificateManager
		kubeInformers.Start(ctx.Done())
		go podCertificateManager.Run(ctx)

		metrics.RegisterCollectors(collectors.PodCertificateCollectorFor(podCertificateManager))
	} else {
		klet.podCertificateManager = &podcertificate.NoOpManager{}
		logger.Info("Not starting PodCertificateRequest manager because we are in static kubelet mode or the PodCertificateProjection feature gate is disabled")
	}

	// NewInitializedVolumePluginMgr 会在 Kubelet runtimeState 上设置存储错误（见 csi_plugin.go 的 init），
	// 这会影响节点就绪状态。必须在 Kubelet 初始化之前调用，以保证 Node ReadyState
	// 与存储状态一致。
	klet.volumePluginMgr, err = NewInitializedVolumePluginMgr(logger, klet, secretManager, configMapManager, tokenManager, clusterTrustBundleManager, kubeDeps.VolumePlugins, kubeDeps.DynamicPluginProber)
	if err != nil {
		return nil, err
	}
	klet.pluginManager = pluginmanager.NewPluginManager(
		klet.getPluginsRegistrationDir(), /* sockDir */
		kubeDeps.Recorder,
	)

	// 若设置了 experimentalMounterPathFlag，由于挂载路径已非默认值，
	// 不再检查节点能力。
	if len(experimentalMounterPath) != 0 {
		// 把容器化 mounter rootfs 中 /etc/resolv.conf 的 nameserver 替换为 kubelet.ClusterDNS，
		// 以便解析 service 名称。
		klet.dnsConfigurer.SetupDNSinContainerizedMounter(logger, experimentalMounterPath)
	}

	// 初始化卷管理器。
	var vmPodManager volumemanager.PodManager = klet.podManager
	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingMemoryBackedVolumes) {
		// 卷管理器只应感知处于 'allocated' 状态的 pod。
		vmPodManager = &allocatedPodManager{baseManager: klet.podManager, allocationManager: klet.allocationManager}
	}

	klet.volumeManager = volumemanager.NewVolumeManager(
		kubeCfg.EnableControllerAttachDetach,
		nodeName,
		vmPodManager,
		klet.podWorkers,
		klet.kubeClient,
		klet.volumePluginMgr,
		kubeDeps.Mounter,
		kubeDeps.HostUtil,
		klet.getPodsDir(),
		kubeDeps.Recorder,
		volumepathhandler.NewBlockVolumePathHandler(),
		klet.statusManager,
		kubeCfg.VolumeStatsAggPeriod.Duration)

	boMax, base := newCrashLoopBackOff(kubeCfg)

	klet.crashLoopBackOff = flowcontrol.NewBackOff(base, boMax)
	klet.crashLoopBackOff.HasExpiredFunc = func(eventTime time.Time, lastUpdate time.Time, maxDuration time.Duration) bool {
		return eventTime.Sub(lastUpdate) > 600*time.Second
	}

	// 初始化驱逐管理器。
	evictionManager, evictionAdmitHandler := eviction.NewManager(klet.resourceAnalyzer, evictionConfig,
		killPodNow(ctx, klet.podWorkers, kubeDeps.Recorder), klet.imageManager, klet.containerGC, kubeDeps.Recorder, nodeRef, klet.clock, kubeCfg.LocalStorageCapacityIsolation)

	klet.evictionManager = evictionManager
	handlers := []lifecycle.PodAdmitHandler{}
	handlers = append(handlers, evictionAdmitHandler)

	if utilfeature.DefaultFeatureGate.Enabled(features.NodeDeclaredFeatures) {
		if status, err := klet.containerRuntime.Status(ctx); err == nil && status != nil {
			klet.runtimeState.setRuntimeFeatures(status.Features)
		} else if err != nil {
			logger.V(4).Info("Unable to prefetch container runtime features for node declared features", "err", err)
		}
		v, err := versionutil.Parse(version.Get().String())
		if err != nil {
			return nil, fmt.Errorf("failed to parse version: %w", err)
		}
		framework := ndf.DefaultFramework
		klet.version = v
		klet.nodeDeclaredFeaturesFramework = framework
		klet.nodeDeclaredFeatures = klet.discoverNodeDeclaredFeatures()
		klet.nodeDeclaredFeaturesSet = framework.MustMapSorted(klet.nodeDeclaredFeatures)
	}

	// 安全允许列表中的 sysctl 一定可以作为不安全 sysctl 使用，
	// 因此把两个列表拼接起来。
	safeAndUnsafeSysctls := append(sysctl.SafeSysctlAllowlist(ctx), allowedUnsafeSysctls...)
	sysctlsAllowlist, err := sysctl.NewAllowlist(safeAndUnsafeSysctls)
	if err != nil {
		return nil, err
	}
	handlers = append(handlers, sysctlsAllowlist)

	// 启用 activeDeadline 处理器。
	activeDeadlineHandler, err := newActiveDeadlineHandler(klet.statusManager, kubeDeps.Recorder, klet.clock)
	if err != nil {
		return nil, err
	}
	klet.AddPodSyncLoopHandler(activeDeadlineHandler)
	klet.AddPodSyncHandler(activeDeadlineHandler)

	handlers = append(handlers, klet.containerManager.GetAllocateResourcesPodAdmitHandler(logger))

	criticalPodAdmissionHandler := preemption.NewCriticalPodAdmissionHandler(klet.getAllocatedPods, killPodNow(ctx, klet.podWorkers, kubeDeps.Recorder), kubeDeps.Recorder)
	handlers = append(handlers, lifecycle.NewPredicateAdmitHandler(klet.GetCachedNode, criticalPodAdmissionHandler, klet.containerManager.UpdatePluginResources))
	// 应用函数式 Option。
	for _, opt := range kubeDeps.Options {
		opt(klet)
	}

	if goos == "linux" {
		// AppArmor 是 Linux 内核安全模块，不支持其他操作系统。
		klet.appArmorValidator = apparmor.NewValidator()
		handlers = append(handlers, lifecycle.NewAppArmorAdmitHandler(klet.appArmorValidator))
	}

	handlers = append(handlers, lifecycle.NewPodFeaturesAdmitHandler())

	if utilfeature.DefaultFeatureGate.Enabled(features.NodeDeclaredFeatures) {
		handlers = append(handlers, lifecycle.NewDeclaredFeaturesAdmitHandler(klet.nodeDeclaredFeaturesFramework, klet.nodeDeclaredFeaturesSet, klet.version))
	}

	leaseDuration := time.Duration(kubeCfg.NodeLeaseDurationSeconds) * time.Second
	renewInterval := time.Duration(float64(leaseDuration) * nodeLeaseRenewIntervalFraction)
	klet.nodeLeaseController = lease.NewController(
		klet.clock,
		klet.heartbeatClient,
		string(klet.nodeName),
		kubeCfg.NodeLeaseDurationSeconds,
		klet.onRepeatedHeartbeatFailure,
		renewInterval,
		string(klet.nodeName),
		v1.NamespaceNodeLease,
		util.SetNodeOwnerFunc(ctx, klet.heartbeatClient, string(klet.nodeName)))

	// 初始化节点关机管理器。
	shutdownManager := nodeshutdown.NewManager(&nodeshutdown.Config{
		Logger:                           logger,
		VolumeManager:                    klet.volumeManager,
		Recorder:                         kubeDeps.Recorder,
		NodeRef:                          nodeRef,
		GetPodsFunc:                      klet.GetActivePods,
		KillPodFunc:                      killPodNow(ctx, klet.podWorkers, kubeDeps.Recorder),
		SyncNodeStatusFunc:               klet.syncNodeStatus,
		ShutdownGracePeriodRequested:     kubeCfg.ShutdownGracePeriod.Duration,
		ShutdownGracePeriodCriticalPods:  kubeCfg.ShutdownGracePeriodCriticalPods.Duration,
		ShutdownGracePeriodByPodPriority: kubeCfg.ShutdownGracePeriodByPodPriority,
		StateDirectory:                   rootDirectory,
	})
	klet.shutdownManager = shutdownManager
	handlers = append(handlers, shutdownManager)

	klet.allocationManager.AddPodAdmitHandlers(append([]lifecycle.PodAdmitHandler{resizeAdmitHandler}, handlers...))

	var usernsIDsPerPod *int64
	if kubeCfg.UserNamespaces != nil {
		usernsIDsPerPod = kubeCfg.UserNamespaces.IDsPerPod
	}
	klet.usernsManager, err = userns.MakeUserNsManager(logger, klet, usernsIDsPerPod)
	if err != nil {
		return nil, fmt.Errorf("create user namespace manager: %w", err)
	}

	// 最后把最新配置存到 Kubelet 上，便于查看实际生效的配置。
	klet.kubeletConfiguration = *kubeCfg

	// 生成状态函数必须放在最后，因为它依赖 Kubelet 其余部分已构造完成。
	klet.setNodeStatusFuncs = klet.defaultNodeStatusFuncs()

	return klet, nil
}

type serviceLister interface {
	List(labels.Selector) ([]*v1.Service, error)
}

// Kubelet 是 kubelet 的主体实现。
type Kubelet struct {
	kubeletConfiguration kubeletconfiginternal.KubeletConfiguration

	// kubelet 自行探测或通过 flag/config 指定的主机名。
	hostname string

	nodeName        types.NodeName
	cachedNode      *v1.Node
	runtimeCache    kubecontainer.RuntimeCache
	kubeClient      clientset.Interface
	heartbeatClient clientset.Interface
	// mirrorPodClient 用于在 API 中为 static pod 创建和删除 mirror pod。
	mirrorPodClient kubepod.MirrorClient

	rootDirectory    string
	podLogsDirectory string

	// onRepeatedHeartbeatFailure 在心跳操作失败超过一次时被调用，可选。
	onRepeatedHeartbeatFailure func()

	// podManager 保存 kubelet 应当运行的已准入 pod 与 mirror pod 的期望集合。
	// 实际运行中的 pod 集合记录在 podWorkers 中。该管理器由 kubelet 的配置循环填充，
	// 这些循环屏蔽了配置来源的差异（普通 pod 来自 api，static pod 来自本地文件系统或 http）。
	// 需要查看期望 pod 集合的其他组件可以查询它。注意：并非所有期望 pod 都在运行，
	// 也并非所有运行中的 pod 都在 podManager 中——例如从 apiserver 强制删除 pod 会把它
	// 从 podManager 中移除，但该 pod 可能仍在终止并由 podWorkers 跟踪。需要了解节点实际
	// 资源占用、或由 podWorkers 与 sync*Pod 方法驱动的组件（status、volume、stats），
	// reconcile 时也应查询 podWorkers。
	//
	// TODO: 审查所有需要实际 pod 集合（而非期望集合）的 kubelet 组件，
	// 把它们改为使用 podWorkers 而不是 podManager。这可能给部分方法带来延迟，
	// 但能避免竞态，并正确处理被强制删除的终止中 pod 或已更新的 static pod。
	// https://github.com/kubernetes/kubernetes/issues/116970
	podManager kubepod.Manager

	// podWorkers 负责驱动每个 pod 的生命周期状态机。配置变更、更新、周期性 reconcile、
	// 容器运行时更新以及所有期望 pod 的驱逐都会通知 worker，worker 在独立 goroutine 中
	// 对每个 pod 调用相应的 reconcile 方法。在 kubelet 中，哪些 pod 实际在运行及其当前状态
	// 以 podWorkers 为准：
	//
	// * syncing：pod 应处于运行状态（syncPod）
	// * terminating：pod 应被停止（syncTerminatingPod）
	// * terminated：pod 的所有资源应被清理（syncTerminatedPod）
	//
	// 并调用与各状态对应的 handler 方法。kubelet 内需要知道 pod 所处阶段以便正确建立或
	// 拆除资源的组件，必须查询 podWorkers。
	//
	// 一旦 pod 被 pod workers 接收，在首个 pod 完全终止并由 SyncKnownPods 清理之前，
	// 相同 UID（static pod 则为相同 name+namespace）的另一个 pod 不会启动。这意味着
	// pod 可能已处于期望状态（在 API 中）、已准入（在 pod manager 中）、也已请求启动
	// （通过 UpdatePod），但因前一个 pod 仍在终止而长时间无法启动。
	//
	// 作为事件驱动（由 UpdatePod 驱动）的控制器，podWorkers 必须由 kubelet 定期调用
	// SyncKnownPods（携带期望状态，即 podManager 中已准入的 pod）来重新同步。由于强制删除，
	// podManager 可能不知道某些仍在运行的 pod，因此 podWorkers 负责触发这些“已非期望但
	// 必须运行至完成”的 pod 的同步。
	podWorkers PodWorkers

	// evictionManager 观察节点状态，发现可能影响节点稳定的情况时驱逐 pod
	// （置为 Failed 阶段、原因为 Evicted）以缓解资源压力。驱逐管理器依据节点实际状态行事，
	// 并以 podWorker 为准。
	evictionManager eviction.Manager

	// probeManager 跟踪运行中的 pod 集合，确保按用户定义的周期检查探测各 pod 状态。
	// 探针管理器依据节点实际状态行事，由 podWorker 通知 pod 变化。探针管理器是最近探针
	// 状态的权威来源，并负责通知 status manager，由其合成完整的 pod 状态。
	probeManager prober.Manager

	// secretManager 缓存本节点运行中 pod 所使用的 secret。pod 启动与终止时由 podWorkers
	// 通知 secretManager，后者需在 secret 变化时保持所需内容为最新。
	secretManager secret.Manager

	// configMapManager 缓存本节点运行中 pod 所使用的 config map。pod 启动与终止时由
	// podWorkers 通知 configMapManager，后者需在 config map 变化时保持所需内容为最新。
	configMapManager configmap.Manager

	// volumeManager 观察运行中的 pod 集合，负责在 pod 生命周期中执行卷的 attach、mount、
	// unmount 与 detach。它周期性地把已知卷集合与实际期望卷集合对齐，并清理孤儿卷。
	// 卷管理器以 podWorker 作为“哪些 pod 在运行”的权威依据。
	volumeManager volumemanager.VolumeManager

	// statusManager 接收 podWorker 上报的 pod 状态更新，并把 API 中对应 pod 的状态更新为一致。
	// 从 kubelet 视角看，合成后的 pod 状态以 statusManager 为准（状态中的各独立元素由其他
	// 组件负责），组件应查询它而不是自行拼装状态。注意：status manager 位于 pod worker
	// 下游，需要判断 pod 是否仍在运行的组件应直接查询 pod worker。
	statusManager status.Manager

	// allocationManager 管理 pod 的已分配资源。
	allocationManager allocation.Manager

	// pod 在节点上增删时会把更新送入 podCertificateManager，
	// 它根据 pod 配置的证书卷为 pod 申请证书。
	podCertificateManager podcertificate.Manager

	// resyncInterval 是节点上 pod 周期性全量 reconcile 的间隔。
	resyncInterval time.Duration

	// sourcesReady 记录 kubelet 已见到的配置来源，线程安全。
	sourcesReady config.SourcesReady

	// 可选，默认为 /var/log 下的 /logs/。
	logServer http.Handler
	// 可选，默认为简单的 Docker 实现。
	runner kubecontainer.CommandRunner

	// 用于获取容器信息的 cAdvisor。
	cadvisor cadvisor.Interface

	// 设为 true 时节点会自行向 apiserver 注册。
	registerNode bool
	// kubelet 注册自身时添加到 Node 对象的污点列表。
	registerWithTaints []v1.Taint
	// 仅用于内部记账；只能在 registerWithApiserver 中访问。
	registrationCompleted bool

	// dnsConfigurer 用于在启动 pod 时设置 DNS 解析配置。
	dnsConfigurer *dns.Configurer

	// serviceLister 用于列出 service。
	serviceLister serviceLister
	// serviceHasSynced 表示 service 是否已完成至少一次同步。
	// 信任 lister 结果前应先检查它。
	serviceHasSynced cache.InformerSynced
	// nodeLister 用于列出 node。
	nodeLister corelisters.NodeLister
	// nodeHasSynced 表示 node 是否已完成至少一次同步。
	// 信任 node lister 结果前应先检查它。
	nodeHasSynced cache.InformerSynced
	// 注册时要写入的节点标签列表。
	nodeLabels map[string]string

	// nodeDeclaredFeatures 是启动时确定、并在节点状态中声明的有序静态特性列表。
	nodeDeclaredFeatures []string
	// nodeDeclaredFeaturesSet 与 nodeDeclaredFeatures 内容相同，但以集合形式提供以便更快推断。
	nodeDeclaredFeaturesSet ndf.FeatureSet
	// nodeDeclaredFeaturesFramework 提供特性发现与 pod 需求推断的共享逻辑。
	nodeDeclaredFeaturesFramework *ndf.Framework

	// kubelet 版本。
	version *versionutil.Version

	// 运行时最近一次响应 ping 的时间戳。
	// 该值由 Mutex 保护。
	runtimeState *runtimeState

	// 卷插件。
	volumePluginMgr *volume.VolumePluginMgr

	// 管理容器健康检查结果。
	livenessManager  proberesults.Manager
	readinessManager proberesults.Manager
	startupManager   proberesults.Manager

	// 使用的事件记录器。
	recorder record.EventRecorderLogger

	// 死容器垃圾回收策略。
	containerGC kubecontainer.GC

	// 镜像垃圾回收管理器。
	imageManager images.ImageGCManager

	// 容器日志管理器。
	containerLogManager logs.ContainerLogManager

	// cadvisor 返回的 MachineInfo 缓存。
	machineInfoLock sync.RWMutex
	machineInfo     *cadvisorapi.MachineInfo

	// 处理证书轮换。
	serverCertificateManager certificate.Manager

	// 处理从磁盘重新加载 ClientCA。
	clientCAManager *dynamiccertificates.DynamicServingCertificateController

	// 表示节点初始化由外部 cloud controller 完成。
	externalCloudProvider bool
	// 指向本节点的引用。
	nodeRef *v1.ObjectReference

	// cachedNodeRef 缓存已解析 Node UID 的 nodeRef 副本，使记录节点事件的常见路径
	// 无需分配、也无需查询 lister。UID 已知后才会惰性填充，见 nodeRefWithUID。
	cachedNodeRef atomic.Pointer[v1.ObjectReference]

	// 容器运行时。
	containerRuntime kubecontainer.Runtime

	// 流式运行时负责容器流式接口。
	streamingRuntime kubecontainer.StreamingRuntime

	// 容器运行时服务（容器运行时 Start() 需要）。
	runtimeService internalapi.RuntimeService

	// reasonCache 缓存所有容器最近一次创建失败的原因，用于生成 ContainerStatus。
	reasonCache *ReasonCache

	// containerRuntimeReadyExpected 表示当前是否预期容器运行时已就绪，从而让错误日志不受
	// verbosity 限制，避免节点启动期日志过多。在 nodeReadyGracePeriod 的节点初始化期间为
	// false，之后由 fastStatusUpdateOnce 退出时置为 true。
	containerRuntimeReadyExpected bool

	// nodeStatusUpdateFrequency 指定 kubelet 计算节点状态并检查是否需要上报 apiserver 的频率。
	// 每个 tick 都会重新计算节点状态，若状态发生变化、或距上次上报已超过
	// nodeStatusReportFrequency，则上报 apiserver。
	// 注意：节点存活主要通过 NodeLease 续约告知 node controller；nodeStatusUpdateFrequency
	// 控制节点状态变化（如 conditions）被检测并上报的速度。
	// 该值必须足够大，以留出生成节点状态的时间；若过小，kubelet 可能无法可靠更新节点状态。
	nodeStatusUpdateFrequency time.Duration

	// nodeStatusReportFrequency 是节点状态无变化时 kubelet 向 apiserver 上报的频率。
	// 一旦检测到变化，kubelet 会忽略该频率并立即上报。
	nodeStatusReportFrequency time.Duration

	// delayAfterNodeStatusChange 是一次性随机时长：每次节点状态真实变化或 kubelet 重启时，
	// 它会被加到下一次状态上报间隔上。此后不因真实状态变化触发的上报仍遵循
	// nodeStatusReportFrequency。该随机时长服从 [-0.5*nodeStatusReportFrequency,
	// 0.5*nodeStatusReportFrequency] 的均匀分布。
	delayAfterNodeStatusChange time.Duration

	// lastStatusReportTime 是最近一次上报节点状态的时间。
	lastStatusReportTime time.Time

	// syncNodeStatusMux 是更新节点状态的锁，因为该路径非线程安全。
	// 仅供 Kubelet.syncNodeStatus 与 Kubelet.fastNodeStatusUpdate 使用，不得用于其他地方。
	syncNodeStatusMux sync.Mutex

	// updatePodCIDRMux 是更新 pod CIDR 的锁，因为该路径非线程安全。
	// 仅供 Kubelet.updatePodCIDR 使用，不得用于其他地方。
	updatePodCIDRMux sync.Mutex

	// updateRuntimeMux 是更新运行时状态的锁，因为该路径非线程安全。
	// 仅供 Kubelet.updateRuntimeUp、Kubelet.fastNodeStatusUpdate 与
	// Kubelet.HandlerSupportsUserNamespaces 使用，不得用于其他地方。
	updateRuntimeMux sync.Mutex

	// nodeLeaseController 为本 Kubelet 申请并续约节点租约。
	nodeLeaseController lease.Controller

	// pleg 观察容器运行时状态，把容器变化通知给 kubelet，进而通知 podWorkers 对 pod 状态
	// 进行 reconcile（例如容器死亡需要重启）。
	pleg pleg.PodLifecycleEventGenerator

	// eventedPlegWatcher 借助 CRI 事件请求低延迟 relist，作为 pleg 的补充。
	eventedPlegWatcher *pleg.EventedPLEG

	// 缓存所有 pod 的 kubecontainer.PodStatus。
	podCache kubecontainer.ROCache

	// os 是若干系统调用的门面，便于测试时 mock。
	os kubecontainer.OSInterface

	// OOM 事件观察器。
	oomWatcher oomwatcher.Watcher

	// 监控资源使用。
	resourceAnalyzer serverstats.ResourceAnalyzer

	// 是否启用用于资源管理的 QOS cgroup 层级。
	cgroupsPerQOS bool

	// 非空时作为根 cgroup 传给容器运行时。
	cgroupRoot string

	// 卷使用的挂载器。
	mounter mount.Interface

	// 与文件系统交互的 hostutil。
	hostutil hostutil.HostUtils

	// 执行 subpath 操作的 subpather。
	subpather subpath.Interface

	// 非运行时容器管理器。
	containerManager cm.ContainerManager

	// 本 Kubelet 可运行的 pod 数量上限。
	maxPods int

	// 监控 Kubelet 的 sync loop。
	syncLoopMonitor atomic.Value

	// 容器重启退避。
	crashLoopBackOff *flowcontrol.Backoff

	// 本 Kubelet 服务器所在节点上守护进程开放的端口信息。
	daemonEndpoints *v1.NodeDaemonEndpoints

	// 用于触发 pod worker 的队列。
	workQueue queue.WorkQueue

	// oneTimeInitializer 用于初始化依赖运行时已启动的模块。
	oneTimeInitializer sync.Once

	// 若设置，节点使用该 IP 或这些 IP。
	nodeIPs []net.IP

	// 用该函数校验 kubelet 的 nodeIP。
	nodeIPValidator func(net.IP) error

	// 非 nil 时表示节点在外部数据库（如 cloudprovider）中的唯一标识。
	providerID string

	// clock 提供时间相关功能，并便于在测试中替换。
	clock clock.WithTicker

	// tryUpdateNodeStatus 周期中调用的 handler。
	setNodeStatusFuncs []func(context.Context, *v1.Node) error

	lastNodeUnschedulableLock sync.Mutex
	// 保存上一次 tryUpdateNodeStatus() 运行时的 Node.Spec.Unschedulable 值。
	lastNodeUnschedulable bool

	// Pod 同步循环期间要调用的处理器列表。
	lifecycle.PodSyncLoopHandlers

	// pod 同步期间要调用的 handler 列表。
	lifecycle.PodSyncHandlers

	// 每个 core 允许的 pod 数量。
	podsPerCore int

	// enableControllerAttachDetach 表示由 Attach/Detach controller 管理调度到本节点的卷
	// attach/detach，kubelet 不再执行任何 attach/detach 操作。
	enableControllerAttachDetach bool

	// 触发删除 pod 中的容器。
	containerDeletor *podContainerDeletor

	// 配置 iptables 工具链规则。
	makeIPTablesUtilChains bool

	// 用于检查是否支持 AppArmor 的校验器。
	appArmorValidator apparmor.Validator

	// StatsProvider 提供节点与容器统计。
	StatsProvider *stats.Provider

	// pluginmanager 运行一组异步循环，确定本节点需要注册/注销哪些插件并执行相应操作。
	pluginManager pluginmanager.PluginManager

	// pluginManagerStopCh 控制插件管理器退出。生产环境的 kubelet 使用 wait.NeverStop，
	// 测试可注入可关闭的 channel。
	pluginManagerStopCh <-chan struct{}

	// 该标志限制节点状态中上报的镜像数量上限。
	nodeStatusMaxImages int32

	// 为 Kubelet 处理 RuntimeClass 对象。
	runtimeClassManager *runtimeclass.Manager

	// 处理节点的关机事件。
	shutdownManager nodeshutdown.Manager

	// 管理 user namespace。
	usernsManager *userns.UsernsManager

	// OpenTelemetry 追踪器。
	tracer trace.Tracer

	// 跟踪节点启动延迟。
	nodeStartupLatencyTracker util.NodeStartupLatencyTracker

	// 跟踪 pod 启动延迟。
	podStartupLatencyTracker util.PodStartupLatencyTracker

	// kubelet 健康检查。
	healthChecker watchdog.HealthChecker

	// flagz 是用于为 flagz 页面获取 flag 的 Reader 接口。
	flagz flagz.Reader

	// podsServer 是提供 pods gRPC 服务的服务器。
	podsServer *pods.PodsServer
}

// ListPodStats 委托给实现了 stats.Provider 接口的 StatsProvider。
func (kl *Kubelet) ListPodStats(ctx context.Context) ([]statsapi.PodStats, error) {
	return kl.StatsProvider.ListPodStats(ctx)
}

// ListPodCPUAndMemoryStats 委托给实现了 stats.Provider 接口的 StatsProvider。
func (kl *Kubelet) ListPodCPUAndMemoryStats(ctx context.Context) ([]statsapi.PodStats, error) {
	return kl.StatsProvider.ListPodCPUAndMemoryStats(ctx)
}

// PodCPUAndMemoryStats 委托给 StatsProvider，返回指定 pod 的 CPU 与内存统计。
func (kl *Kubelet) PodCPUAndMemoryStats(ctx context.Context, pod *v1.Pod, podStatus *kubecontainer.PodStatus) (*statsapi.PodStats, error) {
	return kl.StatsProvider.PodCPUAndMemoryStats(ctx, pod, podStatus)
}

// ListPodStatsAndUpdateCPUNanoCoreUsage 委托给实现了 stats.Provider 接口的 StatsProvider，并在返回统计的同时更新 CPU nano-core 用量。
func (kl *Kubelet) ListPodStatsAndUpdateCPUNanoCoreUsage(ctx context.Context) ([]statsapi.PodStats, error) {
	return kl.StatsProvider.ListPodStatsAndUpdateCPUNanoCoreUsage(ctx)
}

// ImageFsStats 委托给实现了 stats.Provider 接口的 StatsProvider，返回镜像文件系统统计。
func (kl *Kubelet) ImageFsStats(ctx context.Context) (*statsapi.FsStats, *statsapi.FsStats, error) {
	return kl.StatsProvider.ImageFsStats(ctx)
}

// GetCgroupStats 委托给实现了 stats.Provider 接口的 StatsProvider，返回指定 cgroup 的容器与网络统计。
func (kl *Kubelet) GetCgroupStats(ctx context.Context, cgroupName string, updateStats bool) (*statsapi.ContainerStats, *statsapi.NetworkStats, error) {
	return kl.StatsProvider.GetCgroupStats(ctx, cgroupName, updateStats)
}

// GetCgroupCPUAndMemoryStats 委托给实现了 stats.Provider 接口的 StatsProvider，仅返回 CPU 与内存统计。
func (kl *Kubelet) GetCgroupCPUAndMemoryStats(cgroupName string, updateStats bool) (*statsapi.ContainerStats, error) {
	return kl.StatsProvider.GetCgroupCPUAndMemoryStats(cgroupName, updateStats)
}

// RootFsStats 委托给实现了 stats.Provider 接口的 StatsProvider，返回根文件系统统计。
func (kl *Kubelet) RootFsStats() (*statsapi.FsStats, error) {
	return kl.StatsProvider.RootFsStats()
}

// RlimitStats 委托给实现了 stats.Provider 接口的 StatsProvider，返回 rlimit 统计。
func (kl *Kubelet) RlimitStats() (*statsapi.RlimitStats, error) {
	return kl.StatsProvider.RlimitStats()
}

// setupDataDirs 创建 kubelet 运行所需的目录：
// 1. 根目录
// 2. pods 目录
// 3. plugins 目录
// 4. pod-resources 目录
// 5. checkpoint 目录
// 6. pod 日志根目录
func (kl *Kubelet) setupDataDirs(logger klog.Logger) error {
	if cleanedRoot := filepath.Clean(kl.rootDirectory); cleanedRoot != kl.rootDirectory {
		return fmt.Errorf("rootDirectory not in canonical form: expected %s, was %s", cleanedRoot, kl.rootDirectory)
	}
	pluginRegistrationDir := kl.getPluginsRegistrationDir()
	pluginsDir := kl.getPluginsDir()
	if err := os.MkdirAll(kl.getRootDir(), 0750); err != nil {
		return fmt.Errorf("error creating root directory: %v", err)
	}
	if err := utilfs.MkdirAll(kl.getPodLogsDir(), 0750); err != nil {
		return fmt.Errorf("error creating pod logs root directory %q: %w", kl.getPodLogsDir(), err)
	}
	if err := kl.hostutil.MakeRShared(kl.getRootDir()); err != nil {
		return fmt.Errorf("error configuring root directory: %v", err)
	}
	if err := os.MkdirAll(kl.getPodsDir(), 0750); err != nil {
		return fmt.Errorf("error creating pods directory: %v", err)
	}
	if err := utilfs.MkdirAll(kl.getPluginsDir(), 0750); err != nil {
		return fmt.Errorf("error creating plugins directory: %v", err)
	}
	if err := utilfs.MkdirAll(kl.getPluginsRegistrationDir(), 0750); err != nil {
		return fmt.Errorf("error creating plugins registry directory: %v", err)
	}
	if err := os.MkdirAll(kl.getPodResourcesDir(), 0750); err != nil {
		return fmt.Errorf("error creating podresources directory: %v", err)
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.ContainerCheckpoint) {
		if err := utilfs.MkdirAll(kl.getCheckpointsDir(), 0700); err != nil {
			return fmt.Errorf("error creating checkpoint directory: %v", err)
		}
	}
	if selinux.GetEnabled() {
		err := selinux.SetFileLabel(pluginRegistrationDir, kubeletconfig.KubeletPluginsDirSELinuxLabel)
		if err != nil {
			logger.Info("Unprivileged containerized plugins might not work, could not set selinux context on plugin registration dir", "path", pluginRegistrationDir, "err", err)
		}
		err = selinux.SetFileLabel(pluginsDir, kubeletconfig.KubeletPluginsDirSELinuxLabel)
		if err != nil {
			logger.Info("Unprivileged containerized plugins might not work, could not set selinux context on plugins dir", "path", pluginsDir, "err", err)
		}
	}
	return nil
}

// StartGarbageCollection 启动容器与镜像的垃圾回收协程。
func (kl *Kubelet) StartGarbageCollection(ctx context.Context) {
	logger := klog.FromContext(ctx)
	loggedContainerGCFailure := false
	go wait.UntilWithContext(ctx, func(ctx context.Context) {
		if err := kl.containerGC.GarbageCollect(ctx); err != nil {
			logger.Error(err, "Container garbage collection failed")
			kl.recorder.WithLogger(logger).Eventf(kl.nodeRefWithUID(), v1.EventTypeWarning, events.ContainerGCFailed, "%s", err.Error())
			loggedContainerGCFailure = true
		} else {
			var vLevel klog.Level = 4
			if loggedContainerGCFailure {
				vLevel = 1
				loggedContainerGCFailure = false
			}

			logger.V(int(vLevel)).Info("Container garbage collection succeeded")
		}
	}, ContainerGCPeriod)

	// 当高水位阈值为 100 且最大保留时长为 0（或最大保留时长特性被禁用）时，
	// 用空实现替换镜像 GC 管理器。
	if kl.kubeletConfiguration.ImageGCHighThresholdPercent == 100 && kl.kubeletConfiguration.ImageMaximumGCAge.Duration == 0 {
		logger.V(2).Info("ImageGCHighThresholdPercent is set 100 and ImageMaximumGCAge is 0, Disable image GC")
		return
	}

	prevImageGCFailed := false
	beganGC := time.Now()
	go wait.UntilWithContext(ctx, func(ctx context.Context) {
		if err := kl.imageManager.GarbageCollect(ctx, beganGC); err != nil {
			if prevImageGCFailed {
				logger.Error(err, "Image garbage collection failed multiple times in a row")
				// 仅在连续失败时才创建事件。
				kl.recorder.WithLogger(logger).Event(kl.nodeRefWithUID(), v1.EventTypeWarning, events.ImageGCFailed, err.Error())
			} else {
				logger.Error(err, "Image garbage collection failed once. Stats initialization may not have completed yet")
			}
			prevImageGCFailed = true
		} else {
			var vLevel klog.Level = 4
			if prevImageGCFailed {
				vLevel = 1
				prevImageGCFailed = false
			}

			logger.V(int(vLevel)).Info("Image garbage collection succeeded")
		}
	}, ImageGCPeriod)
}

// initializeModules 初始化不依赖容器运行时启动的内部模块。
// 注意：这里的模块不得依赖尚未在此初始化的模块。
func (kl *Kubelet) initializeModules(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	// 注册 Prometheus 指标。
	metrics.Register()
	metrics.RegisterCollectors(
		collectors.NewVolumeStatsCollector(kl),
		collectors.NewLogMetricsCollector(kl.StatsProvider.ListPodStats),
	)
	metrics.SetNodeName(kl.nodeName)
	servermetrics.Register()

	// 初始化文件系统目录。
	if err := kl.setupDataDirs(logger); err != nil {
		return err
	}

	// 容器日志目录不存在时创建它。
	if _, err := os.Stat(ContainerLogsDir); err != nil {
		if err := kl.os.MkdirAll(ContainerLogsDir, 0755); err != nil {
			return fmt.Errorf("failed to create directory %q: %v", ContainerLogsDir, err)
		}
	}

	if goos == "windows" {
		// Windows 上不允许其他用户读取日志目录，
		// 以免非 root 容器读取其他容器的日志。
		if err := utilfs.Chmod(ContainerLogsDir, 0750); err != nil {
			return fmt.Errorf("failed to set permissions on directory %q: %w", ContainerLogsDir, err)
		}
	}

	// 启动镜像管理器。
	kl.imageManager.Start(ctx)

	// 若已启用证书管理器则启动它。
	if kl.serverCertificateManager != nil {
		kl.serverCertificateManager.Start()
	}

	// 若已启用客户端 CA 管理器则启动它。
	if kl.clientCAManager != nil {
		go kl.clientCAManager.Run(1, ctx.Done())
	}

	// 启动 OOM 观察器。
	if kl.oomWatcher != nil {
		if err := kl.oomWatcher.Start(ctx, kl.nodeRef); err != nil {
			return fmt.Errorf("failed to start OOM watcher: %w", err)
		}
	}

	// 启动资源分析器。
	kl.resourceAnalyzer.Start(ctx)

	return nil
}

// initializeRuntimeDependentModules 初始化依赖容器运行时已启动的内部模块。
func (kl *Kubelet) initializeRuntimeDependentModules(ctx context.Context) {
	logger := klog.FromContext(ctx)
	if err := kl.cadvisor.Start(); err != nil {
		// 让 kubelet 失败，由 babysitter 负责重试启动。
		logger.Error(err, "Failed to start cAdvisor")
		os.Exit(1)
	}

	// 先触发一次按需统计采集，以获得 ephemeral storage 的容量信息。
	// 忽略错误：若统计采集失败，下面的容器管理器启动也会失败。
	_, _, _ = kl.StatsProvider.GetCgroupStats(ctx, "/", true)
	// 启动容器管理器。
	node, err := kl.getNodeAnyWay(ctx)
	if err != nil {
		// 让 kubelet 失败，由 babysitter 负责重试启动。
		logger.Error(err, "Kubelet failed to get node info")
		os.Exit(1)
	}
	// containerManager 必须在 cAdvisor 之后启动，因为它需要文件系统容量信息。
	if err := kl.containerManager.Start(ctx, node, kl.GetActivePods, kl.getNodeAnyWay, kl.sourcesReady, kl.statusManager, kl.runtimeService, kl.supportLocalStorageCapacityIsolation()); err != nil {
		// 让 kubelet 失败，由 babysitter 负责重试启动。
		logger.Error(err, "Failed to start ContainerManager")
		os.Exit(1)
	}
	// 驱逐管理器必须在 cadvisor 之后启动，因为它需要知道容器运行时是否有独立的 imagefs。
	// 驱逐决策基于已分配（而非期望）的 pod 资源。
	kl.evictionManager.Start(ctx, kl.StatsProvider, kl.getAllocatedPods, kl.PodIsFinished, evictionMonitoringPeriod)

	// 容器日志管理器必须在容器运行时启动后运行，以便从运行时获取信息，
	// 并在日志轮转后通知容器重新打开日志文件。
	kl.containerLogManager.Start(ctx)
	// 注册 CSI Driver 的注册回调函数。
	kl.pluginManager.AddHandler(pluginwatcherapi.CSIPlugin, plugincache.PluginHandler(csi.PluginHandler))
	// 注册 DRA 插件与 Device 插件的注册回调函数。
	for name, handler := range kl.containerManager.GetPluginRegistrationHandlers() {
		kl.pluginManager.AddHandler(name, handler)
	}

	// 启动插件管理器。
	logger.V(4).Info("Starting plugin manager")
	pluginManagerStopCh := kl.pluginManagerStopCh
	if pluginManagerStopCh == nil {
		pluginManagerStopCh = wait.NeverStop
	}
	go kl.pluginManager.Run(ctx, kl.sourcesReady, pluginManagerStopCh)

	err = kl.shutdownManager.Start(ctx)
	if err != nil {
		// 关机管理器对 kubelet 并非关键：启动失败只记录日志，不阻塞 Kubelet 启动。
		logger.Error(err, "Failed to start node shutdown manager")
	}
}

// Run 启动 kubelet，并开始响应配置更新。
func (kl *Kubelet) Run(ctx context.Context, updates <-chan kubetypes.PodUpdate) {
	logger := klog.FromContext(ctx)
	if kl.logServer == nil {
		file := http.FileServer(http.Dir(nodeLogDir))
		if utilfeature.DefaultFeatureGate.Enabled(features.NodeLogQuery) && kl.kubeletConfiguration.EnableSystemLogQuery {
			kl.logServer = http.StripPrefix("/logs/", http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				if nlq, errs := newNodeLogQuery(req.URL.Query()); len(errs) > 0 {
					http.Error(w, errs.ToAggregate().Error(), http.StatusBadRequest)
					return
				} else if nlq != nil {
					if req.URL.Path != "/" && req.URL.Path != "" {
						http.Error(w, "path not allowed in query mode", http.StatusNotAcceptable)
						return
					}
					if errs := nlq.validate(); len(errs) > 0 {
						http.Error(w, errs.ToAggregate().Error(), http.StatusNotAcceptable)
						return
					}
					// 校验已保证请求不会同时查询 service 和文件。
					if len(nlq.Services) > 0 {
						journal.ServeHTTP(w, req)
						return
					}
					// 校验已保证请求不会显式同时查询多个文件。
					if len(nlq.Files) == 1 {
						// 兼容 Windows 客户端使用的反斜杠。
						req.URL.Path = filepath.ToSlash(nlq.Files[0])
					}
				}
				// 兜底处理调用方直接查询文件的情况。
				// 例如：kubectl get --raw /api/v1/nodes/$name/proxy/logs/foo.log
				file.ServeHTTP(w, req)
			}))
		} else {
			kl.logServer = http.StripPrefix("/logs/", file)
		}
	}
	if kl.kubeClient == nil {
		logger.Info("No API server defined - no node status update will be sent")
	}

	if err := kl.initializeModules(ctx); err != nil {
		kl.recorder.WithLogger(logger).Eventf(kl.nodeRefWithUID(), v1.EventTypeWarning, events.KubeletSetupFailed, "%s", err.Error())
		logger.Error(err, "Failed to initialize internal modules")
		os.Exit(1)
	}

	if err := kl.cgroupVersionCheck(); err != nil {
		logger.V(2).Info("Warning: cgroup check", "error", err)
	}

	// 启动分配管理器。
	if kl.allocationManager != nil {
		kl.allocationManager.Run(ctx)
	}

	// 启动卷管理器。
	go kl.volumeManager.Run(ctx, kl.sourcesReady)

	if kl.kubeClient != nil {
		// 启动两个 goroutine 更新状态。
		//
		// 第一个按 nodeStatusUpdateFrequency 周期向 apiserver 上报，提供规律的状态间隔；
		// 第二个用于在初始化期间更及时地更新状态：节点变为就绪后向 apiserver 做一次
		// 一次性上报，随后退出。
		//
		// 加入少量抖动，避免随着时间推移各节点的请求因优先级与公平性效应几乎同时堆积。
		go func() {
			// 在 syncNodeStatus 之前先调用一次 updateRuntimeUp，确保 kubelet 已检查运行时状态；
			// 否则重启 kubelet 后，syncNodeStatus 会在第一个上报周期内把节点报为 NotReady。
			kl.updateRuntimeUp(ctx)
			wait.JitterUntil(func() { kl.syncNodeStatus(ctx) }, kl.nodeStatusUpdateFrequency, 0.04, true, wait.NeverStop)
		}()

		// 保持原先与 context.Background 脱离的行为，同时仍为上下文日志传递日志字段。
		go kl.fastStatusUpdateOnce(context.WithoutCancel(ctx))

		// 持续续约节点租约直到 kubelet 退出。
		// 这里刻意不使用 kubelet 的 context，以便优雅关闭期间租约仍能继续续约。
		go kl.nodeLeaseController.Run(context.Background())

		// 节点启动时，static pod 的 mirror pod 可能因节点注册或 informer 同步延迟而无法立即创建，
		// 最终会在 static pod 重新同步时（每 1-1.5 分钟）创建。
		// 为了让 kube-scheduler 更快感知 static pod 的资源占用，节点一完成注册就创建 mirror pod。
		go kl.fastStaticPodsRegistration(ctx)
	}
	go wait.UntilWithContext(ctx, kl.updateRuntimeUp, 5*time.Second)

	// 设置 iptables 工具链规则。
	if kl.makeIPTablesUtilChains {
		kl.initNetworkUtil(logger)
	}

	// 启动各组件同步循环。
	kl.statusManager.Start(ctx)

	// 若已启用则开始同步 RuntimeClass。
	if kl.runtimeClassManager != nil {
		kl.runtimeClassManager.Start(wait.NeverStop)
	}

	// 启动 pod 生命周期事件生成器。
	kl.pleg.Start(ctx)

	if kl.eventedPlegWatcher != nil {
		kl.eventedPlegWatcher.Start(ctx)
	}

	if kl.healthChecker != nil {
		kl.healthChecker.SetHealthCheckers(kl, kl.containerManager.GetHealthCheckers())
	}

	kl.syncLoop(ctx, updates, kl)
}

// SyncPod 是单个 pod 同步（建立）过程的入口脚本。
// 该方法可重入，负责将 pod 逐步收敛到 spec 期望状态；反向的拆除流程由
// SyncTerminatingPod 与 SyncTerminatedPod 负责。若 SyncPod 无错误返回，
// 则 pod 运行时状态与期望配置一致（pod 正在运行）。若 SyncPod 返回瞬时错误，
// 下一次调用会继续向期望状态推进。当检测到容器退出导致 pod 进入终态
// （RestartNever 或 RestartOnFailure）时，SyncPod 返回 isTerminal=true，
// 随后将调用 SyncTerminatingPod。若 pod 因其他原因终止，SyncPod 会收到
// context 取消，应尽快退出。
//
// 参数：
//
// updateType - 本次是创建（首次）还是更新；由于方法必须可重入，该值仅用于指标
//
// pod - 正在建立（同步）的 pod
//
// mirrorPod - kubelet 已知的该 pod 的 mirror pod（如有）
//
// podStatus - 观测到的最新 pod 状态，用于决定本轮 SyncPod 应执行的动作
//
// 工作流程：
//   - 若 pod 正在创建，记录 pod worker 启动延迟
//   - 调用 generateAPIPodStatus 生成 v1.PodStatus
//   - 若首次观测到 pod 进入运行状态，记录 pod 启动延迟
//   - 更新 status manager 中的 pod 状态
//   - 若因软准入不应继续运行，则停止 pod 容器
//   - 为可运行的 pod 启动后台跟踪
//   - 若为 static pod 且尚无 mirror pod，则创建 mirror pod
//   - 按需创建 pod 数据目录
//   - 等待卷 attach/mount 完成
//   - 获取 pod 的镜像拉取 secret
//   - 调用容器运行时的 SyncPod 回调
//   - 更新 pod 入向与出向限流的流量整形配置
//
// 若流程中任一步骤出错，错误会返回并在下次 SyncPod 调用中重试。
//
// 该方法会按顺序写入所有事件，以便尽可能准确地呈现错误现场、辅助排障。
// 因此调用方在收到错误时不应再额外写事件。
func (kl *Kubelet) SyncPod(ctx context.Context, updateType kubetypes.SyncPodType, pod, mirrorPod *v1.Pod, podStatus *kubecontainer.PodStatus) (isTerminal bool, postSync func(), err error) {
	ctx, otelSpan := kl.tracer.Start(ctx, "syncPod", trace.WithAttributes(
		semconv.K8SPodUIDKey.String(string(pod.UID)),
		attribute.String("k8s.pod", klog.KObj(pod).String()),
		semconv.K8SPodNameKey.String(pod.Name),
		attribute.String("k8s.pod.update_type", updateType.String()),
		semconv.K8SNamespaceNameKey.String(pod.Namespace),
	))
	logger := klog.FromContext(ctx)
	logger.V(4).Info("SyncPod enter", "pod", klog.KObj(pod), "podUID", pod.UID)
	defer func() {
		if err != nil {
			otelSpan.RecordError(err)
			otelSpan.SetStatus(codes.Error, err.Error())
		}
		logger.V(4).Info("SyncPod exit", "pod", klog.KObj(pod), "podUID", pod.UID, "isTerminal", isTerminal)
		otelSpan.End()
	}()

	// 主流程的延迟测量以 kubelet 首次看到该 pod 的时刻为基准。
	var firstSeenTime time.Time
	if firstSeenTimeStr, ok := pod.Annotations[kubetypes.ConfigFirstSeenAnnotationKey]; ok {
		firstSeenTime = kubetypes.ConvertToTimestamp(firstSeenTimeStr).Get()
	}

	// pod 创建时记录 pod worker 启动延迟。
	// TODO: 让 pod worker 自行记录延迟
	if updateType == kubetypes.SyncPodCreate {
		if !firstSeenTime.IsZero() {
			// 这是首次同步该 pod。若设置了 firstSeenTime，记录自 kubelet 首次看到该 pod 以来的延迟。
			metrics.PodWorkerStartDuration.Observe(metrics.SinceInSeconds(firstSeenTime))
		} else {
			logger.V(3).Info("First seen time not recorded for pod",
				"podUID", pod.UID,
				"pod", klog.KObj(pod))
		}
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling) {
		// 检查是否有 resize 正在进行，以便相应设置 PodResizeInProgressCondition。
		if kl.containerRuntime.IsPodResizeInProgress(pod, podStatus) {
			kl.statusManager.SetPodResizeInProgressCondition(pod.UID, "", "", pod.Generation)
		} else if generation, cleared := kl.statusManager.ClearPodResizeInProgressCondition(pod.UID); cleared {
			// (Allocated == Actual) 时清除 resize 进行中的状态。
			msg := events.PodResizeCompletedMsg(logger, pod, generation)
			kl.recorder.WithLogger(logger).Eventf(pod, v1.EventTypeNormal, events.ResizeCompleted, "%s", msg)
		}
		// TODO(natasha41575): 这里存在竞态：分配管理器中的 goroutine 可能在我们设置
		// 下面的状态之前，分配新的 resize 并无条件设置 PodResizeInProgressCondition。
	}

	// 结合 pod 与 status manager 的状态生成最终 API pod 状态。
	apiPodStatus := kl.generateAPIPodStatus(ctx, pod, podStatus, false)
	// 若 pod 使用 host network，generateAPIPodStatus 中可能改写 pod IP。（见 #24576）
	// TODO(random-liu): 把 pod spec 写入容器 label 后，检查 pod 是否使用 host network，
	// 并在 runtime.GetPodStatus 中直接把 pod IP 设为 hostIP
	podStatus.IPs = make([]string, 0, len(apiPodStatus.PodIPs))
	for _, ipInfo := range apiPodStatus.PodIPs {
		podStatus.IPs = append(podStatus.IPs, ipInfo.IP)
	}
	if len(podStatus.IPs) == 0 && len(apiPodStatus.PodIP) > 0 {
		podStatus.IPs = []string{apiPodStatus.PodIP}
	}

	// pod 已处于终态时无需继续建立 pod。
	if apiPodStatus.Phase == v1.PodSucceeded || apiPodStatus.Phase == v1.PodFailed {
		kl.statusManager.SetPodStatus(logger, pod, apiPodStatus)
		isTerminal = true
		return isTerminal, nil, nil
	}

	// 若设置了 firstSeenTime，记录自 kubelet 首次看到该 pod 到其变为 Running 的耗时。
	existingStatus, ok := kl.statusManager.GetPodStatus(pod.UID)
	if !ok || existingStatus.Phase == v1.PodPending && apiPodStatus.Phase == v1.PodRunning &&
		!firstSeenTime.IsZero() {
		metrics.PodStartDuration.Observe(metrics.SinceInSeconds(firstSeenTime))
	}

	kl.statusManager.SetPodStatus(logger, pod, apiPodStatus)

	// 网络插件未就绪时，只有使用 host network 的 pod 才会启动。
	if err := kl.runtimeState.networkErrors(); err != nil && !kubecontainer.IsHostNetworkPod(pod) {
		kl.recorder.WithLogger(logger).Eventf(pod, v1.EventTypeWarning, events.NetworkNotReady, "%s: %v", NetworkNotReadyErrorMsg, err)
		return false, nil, fmt.Errorf("%s: %v", NetworkNotReadyErrorMsg, err)
	}

	// 确保 kubelet 知道该 pod 引用的 secret 与 configmap。
	if !kl.podWorkers.IsPodTerminationRequested(pod.UID) {
		if kl.secretManager != nil {
			kl.secretManager.RegisterPod(pod)
		}
		if kl.configMapManager != nil {
			kl.configMapManager.RegisterPod(pod)
		}
	}

	// 为 pod 创建 cgroup；若启用了 cgroups-per-qos，还要把资源参数应用到 cgroup。
	pcm := kl.containerManager.NewPodContainerManager()
	// pod 已终止时无需创建或更新其 cgroup。
	// TODO: 加入 context 取消机制后可以移除此检查
	if !kl.podWorkers.IsPodTerminationRequested(pod.UID) {
		// kubelet 在启用 cgroups-per-qos 的情况下重启时，
		// pod 的所有运行中容器应被逐批杀掉并重新拉起，
		// 以纳入 qos cgroup 层级。
		// 这里判断是否为该 pod 的首次同步。
		firstSync := true
		for _, containerStatus := range apiPodStatus.ContainerStatuses {
			if containerStatus.State.Running != nil {
				firstSync = false
				break
			}
		}
		// 若 pod 的 cgroup 已存在、或 pod 是首次运行，则不杀容器。
		podKilled := false
		if !pcm.Exists(pod) && !firstSync {
			p := kubecontainer.ConvertPodStatusToRunningPod(kl.getRuntime().Type(), podStatus)
			if err := kl.killPod(ctx, pod, p, nil); err == nil {
				podKilled = true
			} else {
				if wait.Interrupted(err) {
					return false, nil, nil
				}
				logger.Error(err, "KillPod failed", "pod", klog.KObj(pod), "podStatus", podStatus)
			}
		}
		// 创建并更新 pod 的 cgroup。
		// 若上面的 run-once pod 已被杀掉，则不再为其创建 cgroup。
		// 当前策略是：kubelet 带新 flag 重启时不重启 run-once pod，
		// 因为这类 pod 只应运行一次，kubelet 重启后不应再次运行。
		// 因此，run-once pod 若已在上面被杀，就不再创建和更新其 cgroup。
		runOnce := pod.Spec.RestartPolicy == v1.RestartPolicyNever
		// 启用 ContainerRestartRules 时，只要任一容器可重启，整个 pod 就应重启。
		if utilfeature.DefaultFeatureGate.Enabled(features.ContainerRestartRules) {
			for _, c := range pod.Spec.Containers {
				if podutil.IsContainerRestartable(pod.Spec, c) {
					runOnce = false
				}
			}
		}
		if !podKilled || !runOnce {
			if !pcm.Exists(pod) {
				if err := kl.containerManager.UpdateQOSCgroups(logger); err != nil {
					logger.V(2).Info("Failed to update QoS cgroups while syncing pod", "pod", klog.KObj(pod), "err", err)
				}
				if err := pcm.EnsureExists(logger, pod); err != nil {
					kl.recorder.WithLogger(logger).Eventf(pod, v1.EventTypeWarning, events.FailedToCreatePodContainer, "unable to ensure pod container exists: %v", err)
					return false, nil, fmt.Errorf("failed to ensure that the pod: %v cgroups exist and are correctly applied: %v", pod.UID, err)
				}

				if err = kl.containerRuntime.UpdateActuatedPodLevelResources(logger, pod); err != nil {
					return false, nil, fmt.Errorf("failed to update the state of pod-level resources for the pod %v : %w", pod.UID, err)
				}
			}
		}
	}

	// static pod 尚无 mirror pod 时为其创建。
	kl.tryReconcileMirrorPods(ctx, pod, mirrorPod)

	// 创建 pod 的数据目录。
	if err := kl.makePodDataDirs(pod); err != nil {
		kl.recorder.WithLogger(logger).Eventf(pod, v1.EventTypeWarning, events.FailedToMakePodDataDirectories, "error making pod data directories: %v", err)
		logger.Error(err, "Unable to make pod data directories for pod", "pod", klog.KObj(pod))
		return false, nil, err
	}

	// 等待卷 attach/mount 完成。
	if err := kl.volumeManager.WaitForAttachAndMount(ctx, pod); err != nil {
		var volumeAttachLimitErr *volumemanager.VolumeAttachLimitExceededError
		if errors.As(err, &volumeAttachLimitErr) {
			kl.rejectPod(ctx, pod, volumemanager.VolumeAttachmentLimitExceededReason, volumeAttachLimitErr.Error())
			recordAdmissionRejection(volumemanager.VolumeAttachmentLimitExceededReason)
			return true, nil, nil
		}
		if !wait.Interrupted(err) {
			kl.recorder.WithLogger(logger).Eventf(pod, v1.EventTypeWarning, events.FailedMountVolume, "Unable to attach or mount volumes: %v", err)
			logger.Error(err, "Unable to attach or mount volumes for pod; skipping pod", "pod", klog.KObj(pod))
		}
		return false, nil, err
	}
	// 卷已挂载完成。确保实际生效状态（容器资源与卷限额）已初始化。
	kl.containerRuntime.InitializeActuatedPod(logger, pod)

	// 获取该 pod 的镜像拉取 secret。
	pullSecrets, missingPullSecretNames := kl.getPullSecretsForPod(logger, pod)

	// 确保该 pod 正在被探针检测。
	kl.probeManager.AddPod(ctx, pod)

	// TODO(#113606): 使用来自 pod worker 的入参 context 的取消能力。
	// 目前直接使用该 context 的取消会导致测试失败。要移除这里的 WithoutCancel，
	// 需要把 wait.Interrupted 错误从结果中过滤掉并绕过 reasonCache——因为取消 SyncPod
	// 的 context 是已知的、有意的错误，而非一般性错误。
	// 用 WithoutCancel 而非新的 context.TODO()，以保留 trace context。
	// 调用容器运行时的 SyncPod 回调。
	sctx := context.WithoutCancel(ctx)
	restartingAllContainers := false
	if utilfeature.DefaultFeatureGate.Enabled(features.RestartAllContainersOnContainerExits) {
		for _, cond := range apiPodStatus.Conditions {
			if cond.Type == v1.AllContainersRestarting && cond.Status == v1.ConditionTrue {
				restartingAllContainers = true
			}
		}
	}
	result := kl.containerRuntime.SyncPod(sctx, pod, podStatus, pullSecrets, kl.crashLoopBackOff, restartingAllContainers)
	// 缓存同步结果中的失败原因，供后续生成 ContainerStatus 使用。
	kl.reasonCache.Update(pod.UID, result)

	// 若刚执行过 RestartAllContainers 重置，应立即再触发一次同步来启动容器，
	// 而不是等待 PLEG 或周期性 resync。
	if utilfeature.DefaultFeatureGate.Enabled(features.RestartAllContainersOnContainerExits) &&
		restartingAllContainers && result.Error() == nil {
		shouldRequeue := false
		for _, r := range result.SyncResults {
			if r.Action == kubecontainer.RemoveContainer && r.Error == nil {
				shouldRequeue = true
				break
			}
		}
		if shouldRequeue {
			// 这不会造成死循环：UpdatePod 会合并同一 pod 的并发更新；且所有容器都已被移除，
			// 后续 SyncPod 会清除 AllContainersRestarting 条件，从而跳出循环。
			kl.podWorkers.UpdatePod(ctx, UpdatePodOptions{
				Pod:        pod,
				MirrorPod:  mirrorPod,
				UpdateType: kubetypes.SyncPodUpdate,
			})
		}
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling) {
		for _, r := range result.SyncResults {
			if r.Action == kubecontainer.ResizePodInPlace && r.Error != nil {
				// 若该条件已存在，则不会更新 observedGeneration。
				if generation, updated := kl.statusManager.SetPodResizeInProgressCondition(pod.UID, v1.PodReasonError, r.Message, pod.Generation); updated {
					msg := events.PodResizeErrorMsg(logger, pod, generation, r.Message)
					kl.recorder.WithLogger(logger).Eventf(pod, v1.EventTypeWarning, events.ResizeError, "%s", msg)
				}
			}
		}
	}

	// 若存在找不到的具名拉取 secret，且本次同步出现镜像拉取错误，则发出事件——这有助于排查配置错误。
	if len(missingPullSecretNames) > 0 {
		hasPullFailures := false
		for _, r := range result.SyncResults {
			// 仅在本次同步实际尝试过拉取时触发；若因退避而未拉取，则不会再次触发。
			if errors.Is(r.Error, images.ErrImagePull) {
				hasPullFailures = true
				break
			}
		}
		if hasPullFailures {
			kl.recorder.WithLogger(logger).Eventf(pod, v1.EventTypeWarning, "FailedToRetrieveImagePullSecret", "Unable to retrieve some image pull secrets (%s); attempting to pull the image may not succeed.", strings.Join(missingPullSecretNames, ", "))
		}
	}

	err = result.Error()
	if len(result.SyncResults) > 0 && err == nil {
		postSync = func() {
			kl.RequestPodRelist(klog.FromContext(ctx), pod.UID)
		}
	}

	return false, postSync, err
}

// SyncTerminatingPod 负责终止 pod 内所有运行中的容器。该方法无错误返回后，
// pod 即视为已终止，此时清理与容器生命周期绑定的 pod 状态是安全的，
// 下一个被调用的方法是 SyncTerminatedPod。方法应在给定宽限期内返回；
// 超时后传入的 context 可能被取消。若用户或 kubelet 缩短了宽限期
// （例如驱逐场景），方法也可能被 context 取消中断。若 pod 从配置中被强制删除
// 且 kubelet 随后重启，该方法不保证会被调用——遗留 pod 由
// SyncTerminatingRuntimePod 处理。
func (kl *Kubelet) SyncTerminatingPod(ctx context.Context, pod *v1.Pod, podStatus *kubecontainer.PodStatus, gracePeriod *int64, podStatusFn func(*v1.PodStatus)) (err error) {
	// TODO(#113606): 把这里与来自 pod worker 的入参 context 关联起来。
	// 目前使用该 context 会导致测试失败。
	logger := klog.FromContext(ctx)
	ctx = klog.NewContext(context.TODO(), logger)
	logger.V(4).Info("SyncTerminatingPod enter", "pod", klog.KObj(pod), "podUID", pod.UID)

	ctx, otelSpan := kl.tracer.Start(ctx, "syncTerminatingPod", trace.WithAttributes(
		semconv.K8SPodUIDKey.String(string(pod.UID)),
		attribute.String("k8s.pod", klog.KObj(pod).String()),
		semconv.K8SPodNameKey.String(pod.Name),
		semconv.K8SNamespaceNameKey.String(pod.Namespace),
	))
	defer func() {
		if err != nil {
			otelSpan.RecordError(err)
			otelSpan.SetStatus(codes.Error, err.Error())
		}
		otelSpan.End()
		logger.V(4).Info("SyncTerminatingPod exit", "pod", klog.KObj(pod), "podUID", pod.UID)
	}()

	apiPodStatus := kl.generateAPIPodStatus(ctx, pod, podStatus, false)
	if podStatusFn != nil {
		podStatusFn(&apiPodStatus)
	}
	kl.statusManager.SetPodStatus(logger, pod, apiPodStatus)

	if gracePeriod != nil {
		logger.V(4).Info("Pod terminating with grace period", "pod", klog.KObj(pod), "podUID", pod.UID, "gracePeriod", *gracePeriod)
	} else {
		logger.V(4).Info("Pod terminating with grace period", "pod", klog.KObj(pod), "podUID", pod.UID, "gracePeriod", nil)
	}

	kl.probeManager.StopLivenessAndStartup(pod)

	p := kubecontainer.ConvertPodStatusToRunningPod(kl.getRuntime().Type(), podStatus)
	if err := kl.killPod(ctx, pod, p, gracePeriod); err != nil {
		kl.recorder.WithLogger(logger).Eventf(pod, v1.EventTypeWarning, events.FailedToKillPod, "error killing pod: %v", err)
		// 杀掉 pod 时出错，因此直接返回该错误。
		return fmt.Errorf("error killing terminating pod: %w", err)
	}

	// 容器停止后即可停止 liveness 与 readiness 探针。
	// TODO: pod 进入终态后，某些探针（liveness exec）可以在检测到容器关闭后立即停止，
	// readiness 探针可在首次失败后停止。跟踪于
	// https://github.com/kubernetes/kubernetes/issues/107894，不过可能不值得优化。
	kl.probeManager.RemovePod(pod)

	// 通过检查是否还有运行中的容器，防范 KillPod 实现的一致性缺陷。该方法调用不频繁，
	// 开销基本可忽略，却能捕获调用方乱序更新 pod 状态引入的竞态。
	// TODO: 让 KillPod 返回已停止容器的终态，并立即写入缓存
	runtimePod, err := kl.containerRuntime.GetPod(ctx, pod.UID)
	if err != nil {
		if errors.Is(err, kubecontainer.ErrPodNotFound) {
			// 若 pod sandbox 已被清理，则用空的 runtimePod 继续处理。
			runtimePod = &kubecontainer.Pod{
				ID:        pod.UID,
				Name:      pod.Name,
				Namespace: pod.Namespace,
				Timestamp: kl.clock.Now(),
			}
		} else {
			return fmt.Errorf("unable to get pod prior to final pod termination: %w", err)
		}
	}
	stoppedPodStatus, err := kl.containerRuntime.GetPodStatus(ctx, runtimePod)
	if err != nil {
		return fmt.Errorf("unable to read pod status prior to final pod termination: %w", err)
	}
	preserveDataFromBeforeStopping(stoppedPodStatus, podStatus)
	var runningContainers []string
	type container struct {
		Name       string
		State      string
		ExitCode   int
		FinishedAt string
	}
	var containers []container
	loggerV := logger.V(4)
	loggerVEnabled := loggerV.Enabled()
	for _, s := range stoppedPodStatus.ContainerStatuses {
		if s.State == kubecontainer.ContainerStateRunning {
			runningContainers = append(runningContainers, s.ID.String())
		}
		if loggerVEnabled {
			containers = append(containers, container{Name: s.Name, State: string(s.State), ExitCode: s.ExitCode, FinishedAt: s.FinishedAt.UTC().Format(time.RFC3339Nano)})
		}
	}
	if loggerVEnabled {
		sort.Slice(containers, func(i, j int) bool { return containers[i].Name < containers[j].Name })
		logger.V(4).Info("Post-termination container state", "pod", klog.KObj(pod), "podUID", pod.UID, "containers", containers)
	}
	if len(runningContainers) > 0 {
		return fmt.Errorf("detected running containers after a successful KillPod, CRI violation: %v", runningContainers)
	}

	// 注意：资源必须在所有容器停止之后、且在上报 apiserver 的 pod 状态变更之前
	// 执行 unprepare，以避免与 kubernetes 核心中的资源释放逻辑产生竞态。
	if utilfeature.DefaultFeatureGate.Enabled(features.DynamicResourceAllocation) {
		if err := kl.UnprepareDynamicResources(ctx, pod); err != nil {
			return err
		}
	}

	// pod 不再运行后，计算并更新缓存中的状态。
	// 计算放在这里，是为了确保所用 pod 状态包含容器结束状态（含退出码）——等到
	// SyncTerminatedPod 被调用时，容器可能已被移除。
	apiPodStatus = kl.generateAPIPodStatus(ctx, pod, stoppedPodStatus, true)
	kl.statusManager.SetPodStatus(logger, pod, apiPodStatus)

	// 已成功停止所有容器，pod 正在终止，本阶段状态可视为“完成”。
	logger.V(4).Info("Pod termination stopped all running containers", "pod", klog.KObj(pod), "podUID", pod.UID)

	return nil
}

// preserveDataFromBeforeStopping 保留 IP 等在终止后仍需上报给 apiserver、
// 但 containerRuntime.GetPodStatus 对已停止 pod 不再返回的数据。
// 注意：pod 停止后若 kubelet 重启，这些数据仍可能丢失。
func preserveDataFromBeforeStopping(stoppedPodStatus, podStatus *kubecontainer.PodStatus) {
	stoppedPodStatus.IPs = podStatus.IPs
}

// SyncTerminatingRuntimePod 终止一个 kubelet 已无对应配置的 pod 中仍在运行的容器。
// 该方法无错误返回后，各子系统可在后台安全清理残留本地状态。与
// SyncTerminatingPod 不同，这里没有完整 pod spec，无法执行生命周期相关操作，
// 只能确保运行中的残留 pod 被终止，以便垃圾回收继续进行。由于配置来源已移除，
// 这里不更新 pod 状态。
func (kl *Kubelet) SyncTerminatingRuntimePod(ctx context.Context, runningPod *kubecontainer.Pod) error {
	// TODO(#113606): 把这里与来自 pod worker 的入参 context 关联起来。
	// 目前使用该 context 会导致测试失败。
	logger := klog.FromContext(ctx)
	ctx = klog.NewContext(context.TODO(), logger)
	pod := runningPod.ToAPIPod()
	logger.V(4).Info("SyncTerminatingRuntimePod enter", "pod", klog.KObj(pod), "podUID", pod.UID)
	defer logger.V(4).Info("SyncTerminatingRuntimePod exit", "pod", klog.KObj(pod), "podUID", pod.UID)

	// 由于已丢失该 pod 的其他所有信息，直接杀掉它。
	logger.V(4).Info("Orphaned running pod terminating without grace period", "pod", klog.KObj(pod), "podUID", pod.UID)
	// TODO: 这里可能应设为 0 以跳过等待（需要先修复容器运行时）。
	gracePeriod := int64(1)
	if err := kl.killPod(ctx, pod, *runningPod, &gracePeriod); err != nil {
		kl.recorder.WithLogger(logger).Eventf(pod, v1.EventTypeWarning, events.FailedToKillPod, "error killing pod: %v", err)
		// 杀掉 pod 时出错，因此直接返回该错误。
		utilruntime.HandleError(err)
		return err
	}
	logger.V(4).Info("Pod termination stopped all running orphaned containers", "pod", klog.KObj(pod), "podUID", pod.UID)
	return nil
}

// SyncTerminatedPod 清理已终止（没有运行中容器）的 pod，调用其中的操作来
// 拆除该 pod 的全部资源。方法退出后 pod 应可供清理。该方法缩短了 pod 清理延迟，
// 但不保证在所有场景都被调用。
//
// 由于 kubelet 本地不持久化信息，本方法中所有修改磁盘状态的操作都必须可重入，
// 并能由 HandlePodCleanups 或其他循环兜底回收。典型场景是：pod 从配置
// （本地磁盘或 API）中被强制删除，而 kubelet 在操作中途重启。
func (kl *Kubelet) SyncTerminatedPod(ctx context.Context, pod *v1.Pod, podStatus *kubecontainer.PodStatus) error {
	ctx, otelSpan := kl.tracer.Start(ctx, "syncTerminatedPod", trace.WithAttributes(
		semconv.K8SPodUIDKey.String(string(pod.UID)),
		attribute.String("k8s.pod", klog.KObj(pod).String()),
		semconv.K8SPodNameKey.String(pod.Name),
		semconv.K8SNamespaceNameKey.String(pod.Namespace),
	))
	logger := klog.FromContext(ctx)
	defer otelSpan.End()
	logger.V(4).Info("SyncTerminatedPod enter", "pod", klog.KObj(pod), "podUID", pod.UID)
	defer logger.V(4).Info("SyncTerminatedPod exit", "pod", klog.KObj(pod), "podUID", pod.UID)

	// 生成 pod 的最终状态。
	// TODO: 是否可以直接并入 TerminatePod？这样只需一次 pod 更新
	apiPodStatus := kl.generateAPIPodStatus(ctx, pod, podStatus, true)

	kl.statusManager.SetPodStatus(logger, pod, apiPodStatus)

	// 卷会在 pod worker 报告 ShouldPodRuntimeBeRemoved 之后卸载
	// （该条件在 syncTerminatedPod 被调用前即已满足）。
	if err := kl.volumeManager.WaitForUnmount(ctx, pod); err != nil {
		return err
	}
	logger.V(4).Info("Pod termination unmounted volumes", "pod", klog.KObj(pod), "podUID", pod.UID)

	// 该等待循环依赖后台清理：pod worker 对 ShouldPodRuntimeBeRemoved 返回 true 后
	// （发生在 `SyncTerminatingPod` 完成之后）后台清理才会启动。
	if err := wait.PollUntilContextCancel(ctx, 100*time.Millisecond, true, func(ctx context.Context) (bool, error) {
		volumesExist := kl.podVolumesExist(logger, pod.UID)
		if volumesExist {
			logger.V(3).Info("Pod is terminated, but some volumes have not been cleaned up", "pod", klog.KObj(pod), "podUID", pod.UID)
		}
		return !volumesExist, nil
	}); err != nil {
		return err
	}
	logger.V(3).Info("Pod termination cleaned up volume paths", "pod", klog.KObj(pod), "podUID", pod.UID)

	// 卷卸载完成后，通知 secret 与 configmap 管理器该 pod 已处理完毕。
	if kl.secretManager != nil {
		kl.secretManager.UnregisterPod(pod)
	}
	if kl.configMapManager != nil {
		kl.configMapManager.UnregisterPod(pod)
	}

	// 注意：pod 容器交由后台回收，因为 dockershim 需要容器来获取日志，
	// 且我们希望日志在 pod 被物理删除前始终可用。

	// 移除已不再运行的 pod 在层级中的 cgroup。
	if kl.cgroupsPerQOS {
		pcm := kl.containerManager.NewPodContainerManager()
		name, _ := pcm.GetPodContainerName(pod)
		if err := pcm.Destroy(logger, name); err != nil {
			return err
		}
		logger.V(4).Info("Pod termination removed cgroups", "pod", klog.KObj(pod), "podUID", pod.UID)
	}

	kl.usernsManager.Release(logger, pod.UID)

	// 标记 pod 的最终状态。
	kl.statusManager.TerminatePod(logger, pod)
	logger.V(4).Info("Pod is terminated and will need no more status updates", "pod", klog.KObj(pod), "podUID", pod.UID)

	return nil
}

// getPodsToSync 返回需要重新同步的 pod，目前包括：
//   - 工作队列中已有就绪工作的 pod。
//   - 请求同步该 pod 的内部模块。
//
// 该方法不返回孤立的 pod（仅 pod worker 知道、可能已从配置中删除的 pod）。
// 这些 pod 由 HandlePodCleanups 在推进状态机时同步。
//
// TODO: 考虑同步所有近期未被处理的 pod，以抵御可能导致更新无法送达的缺陷
// （例如此前孤立 pod 的缺陷）。不要向工作队列询问待处理工作，而是向
// PodWorker 询问哪些 pod 需要同步。
func (kl *Kubelet) getPodsToSync() []*v1.Pod {
	allPods := kl.podManager.GetPods()
	podUIDs := kl.workQueue.GetWork()
	podUIDSet := sets.New[string]()
	for _, podUID := range podUIDs {
		podUIDSet.Insert(string(podUID))
	}
	var podsToSync []*v1.Pod
	for _, pod := range allPods {
		if podUIDSet.Has(string(pod.UID)) {
			// 该 pod 已有就绪的工作。
			podsToSync = append(podsToSync, pod)
			continue
		}
		for _, podSyncLoopHandler := range kl.PodSyncLoopHandlers {
			if podSyncLoopHandler.ShouldSync(pod) {
				podsToSync = append(podsToSync, pod)
				break
			}
		}
	}
	return podsToSync
}

// deletePod 从 kubelet 内部状态中删除 pod：
// 1. 异步停止关联的 pod worker
// 2. 通过 podKillingCh 发出终止信号
//
// 若配置来源尚未全部就绪，或运行时缓存中找不到该 pod，则返回错误。
func (kl *Kubelet) deletePod(ctx context.Context, pod *v1.Pod) error {
	if pod == nil {
		return fmt.Errorf("deletePod does not allow nil pod")
	}
	if !kl.sourcesReady.AllReady() {
		// 配置来源未就绪时跳过删除，否则可能误删尚未上报的来源对应的 pod。
		return fmt.Errorf("skipping delete because sources aren't ready yet")
	}
	klog.FromContext(ctx).V(3).Info("Pod has been deleted and must be killed", "pod", klog.KObj(pod), "podUID", pod.UID)
	kl.podWorkers.UpdatePod(ctx, UpdatePodOptions{
		Pod:        pod,
		UpdateType: kubetypes.SyncPodKill,
	})
	// 卷与目录的清理由周期性清理流程负责。
	return nil
}

// rejectPod 以给定 reason 与 message 记录 pod 事件，并在 status manager 中把 pod 置为 Failed。
func (kl *Kubelet) rejectPod(ctx context.Context, pod *v1.Pod, reason, message string) {
	logger := klog.FromContext(ctx)
	kl.recorder.WithLogger(logger).Eventf(pod, v1.EventTypeWarning, reason, "%s", message)
	kl.statusManager.SetPodStatus(logger, pod, v1.PodStatus{
		QOSClass: v1qos.GetPodQOS(pod), // keep it as is
		Phase:    v1.PodFailed,
		Reason:   reason,
		Message:  "Pod was rejected: " + message})
}

// recordAdmissionRejection 记录准入拒绝指标；仅接受已知原因集合，避免标签基数失控。
func recordAdmissionRejection(reason string) {
	// "reason" 标签的基数可能很高。为避免指标膨胀，这里维护已知原因的允许列表，
	// 只记录列表中的原因；其余统一记为 "Other"。
	if admissionRejectionReasons.Has(reason) {
		metrics.AdmissionRejectionsTotal.WithLabelValues(reason).Inc()
	} else if strings.HasPrefix(reason, lifecycle.InsufficientResourcePrefix) {
		// 非扩展资源（如 cpu、memory、ephemeral-storage、pods）已包含在
		// admissionRejectionReasons 中。
		metrics.AdmissionRejectionsTotal.WithLabelValues("OutOfExtendedResources").Inc()
	} else {
		metrics.AdmissionRejectionsTotal.WithLabelValues("Other").Inc()
	}
}

// syncLoop 是处理变更的主循环。它监听三个通道（file、apiserver、http）的变更并合并处理。
// 发现新变更时，会针对期望状态与实际运行状态执行同步；若配置无变化，
// 则每隔 sync-frequency 秒重新同步最近已知的期望状态。该函数永不返回。
func (kl *Kubelet) syncLoop(ctx context.Context, updates <-chan kubetypes.PodUpdate, handler SyncHandler) {
	logger := klog.FromContext(ctx)
	logger.Info("Starting kubelet main sync loop")
	// syncTicker 唤醒 kubelet 检查是否有需要同步的 pod worker。
	// 一秒的周期足够，因为同步间隔默认是 10s。
	syncTicker := time.NewTicker(time.Second)
	defer syncTicker.Stop()
	housekeepingTicker := time.NewTicker(housekeepingPeriod)
	defer housekeepingTicker.Stop()
	plegCh := kl.pleg.Watch()
	const (
		base   = 100 * time.Millisecond
		max    = 5 * time.Second
		factor = 2
	)
	duration := base
	// 负责检查 resolv.conf 中的限制项。
	// 这些限制与单个 pod 无关。
	// 由于在 syncLoop 中调用，无需在其他地方再调用。
	if kl.dnsConfigurer != nil && kl.dnsConfigurer.ResolverConfig != "" {
		kl.dnsConfigurer.CheckLimitsForResolvConf(klog.FromContext(ctx))
	}

	for {
		// 运行时未就绪时不做 pod 同步，并用指数退避避免高频刷日志。
		if err := kl.runtimeState.runtimeErrors(); err != nil {
			logger.Error(err, "Skipping pod synchronization")
			// 指数退避。
			time.Sleep(duration)
			duration = time.Duration(math.Min(float64(max), factor*float64(duration)))
			continue
		}
		// 成功后重置退避。
		duration = base

		kl.syncLoopMonitor.Store(kl.clock.Now())
		if !kl.syncLoopIteration(ctx, updates, handler, syncTicker.C, housekeepingTicker.C, plegCh) {
			break
		}
		kl.syncLoopMonitor.Store(kl.clock.Now())
	}
}

// syncLoopIteration 从多个 channel 读取事件，并将 pod 分发给给定的 handler。
//
// 参数：
// 1.  configCh:       读取配置事件的 channel
// 2.  handler:        接收 pod 的 SyncHandler
// 3.  syncCh:         读取周期性同步事件的 channel
// 4.  housekeepingCh: 读取 housekeeping 事件的 channel
// 5.  plegCh:         读取 PLEG 更新的 channel
//
// 同时也会从 kubelet 存活管理器的更新通道读取事件。
//
// 流程是：读取一个通道、处理该事件，并更新同步循环监视器中的时间戳。
//
// 此处需要说明：尽管 select 的语法与 switch 相似，但当多个 channel 就绪时，
// select 中的 case 会以伪随机顺序求值。换言之，case 的顺序是随机的，
// 不能假设存在多个就绪 channel 时 case 会按书写顺序执行。
//
// 基于这一点，以下 channel 的处理顺序不分先后：
//
//   - configCh: 将配置变更的 pod 分发给对应事件类型的 handler 回调
//   - plegCh: 更新运行时缓存；同步 pod
//   - syncCh: 同步所有等待同步的 pod
//   - housekeepingCh: 触发 pod 清理
//   - health manager: 同步失败或存在容器健康检查失败的 pod
func (kl *Kubelet) syncLoopIteration(ctx context.Context, configCh <-chan kubetypes.PodUpdate, handler SyncHandler,
	syncCh <-chan time.Time, housekeepingCh <-chan time.Time, plegCh <-chan *pleg.PodLifecycleEvent) bool {
	logger := klog.FromContext(ctx)
	select {
	case u, open := <-configCh:
		// 来自配置来源的更新；分发给对应的 handler 回调。
		if !open {
			logger.Error(nil, "Update channel is closed, exiting the sync loop")
			return false
		}

		switch u.Op {
		case kubetypes.ADD:
			logger.V(2).Info("SyncLoop ADD", "source", u.Source, "pods", klog.KObjSlice(u.Pods))
			// 重启后，kubelet 会以 ADD 事件收到所有现存 pod，仿佛它们都是新 pod。
			// 这些 pod 会重新走准入流程，并*可能*被拒绝。待实现 checkpointing 后即可解决。
			handler.HandlePodAdditions(ctx, u.Pods)
		case kubetypes.UPDATE:
			logger.V(2).Info("SyncLoop UPDATE", "source", u.Source, "pods", klog.KObjSlice(u.Pods))
			handler.HandlePodUpdates(ctx, u.Pods)
		case kubetypes.REMOVE:
			logger.V(2).Info("SyncLoop REMOVE", "source", u.Source, "pods", klog.KObjSlice(u.Pods))
			handler.HandlePodRemoves(ctx, u.Pods)
		case kubetypes.RECONCILE:
			logger.V(4).Info("SyncLoop RECONCILE", "source", u.Source, "pods", klog.KObjSlice(u.Pods))
			handler.HandlePodReconcile(ctx, u.Pods)
		case kubetypes.DELETE:
			logger.V(2).Info("SyncLoop DELETE", "source", u.Source, "pods", klog.KObjSlice(u.Pods))
			// 由于是优雅删除，DELETE 按 UPDATE 处理。
			handler.HandlePodUpdates(ctx, u.Pods)
		default:
			logger.Error(nil, "Invalid operation type received", "operation", u.Op)
		}

		kl.sourcesReady.AddSource(u.Source)
	case e := <-plegCh:
		if isSyncPodWorthy(e) {
			// 某个 pod 的 PLEG 事件；同步该 pod。
			if pod, ok := kl.podManager.GetPodByUID(e.ID); ok {
				logger.V(2).Info("SyncLoop (PLEG): event for pod", "pod", klog.KObj(pod), "event", e)
				handler.HandlePodSyncs(ctx, []*v1.Pod{pod})
			} else {
				// pod 已不存在，忽略该事件。
				logger.V(4).Info("SyncLoop (PLEG): pod does not exist, ignore irrelevant event", "event", e)
			}
		}

		if e.Type == pleg.ContainerDied {
			if containerID, ok := e.Data.(string); ok {
				kl.cleanUpContainersInPod(ctx, e.ID, containerID)
			}
		}
	case <-syncCh:
		// 同步等待中的 pod。
		podsToSync := kl.getPodsToSync()
		if len(podsToSync) == 0 {
			break
		}
		logger.V(4).Info("SyncLoop (SYNC) pods", "total", len(podsToSync), "pods", klog.KObjSlice(podsToSync))
		handler.HandlePodSyncs(ctx, podsToSync)
	case update := <-kl.livenessManager.Updates():
		if update.Result == proberesults.Failure {
			handleProbeSync(ctx, kl, update, handler, "liveness", "unhealthy")
		}
	case update := <-kl.readinessManager.Updates():
		ready := update.Result == proberesults.Success
		kl.statusManager.SetContainerReadiness(logger, update.PodUID, update.ContainerID, ready)

		status := "not ready"
		if ready {
			status = "ready"
		}
		handleProbeSync(ctx, kl, update, handler, "readiness", status)
	case update := <-kl.startupManager.Updates():
		started := update.Result == proberesults.Success
		kl.statusManager.SetContainerStartup(logger, update.PodUID, update.ContainerID, started)

		status := "unhealthy"
		if started {
			status = "started"
		}
		handleProbeSync(ctx, kl, update, handler, "startup", status)
	case update := <-kl.containerManager.Updates():
		pods := []*v1.Pod{}
		for _, p := range update.PodUIDs {
			if pod, ok := kl.podManager.GetPodByUID(types.UID(p)); ok {
				logger.V(3).Info("SyncLoop (containermanager): event for pod", "pod", klog.KObj(pod), "event", update)
				pods = append(pods, pod)
			} else {
				// pod 已不存在，忽略该事件。
				logger.V(4).Info("SyncLoop (containermanager): pod does not exist, ignore devices updates", "event", update)
			}
		}
		if len(pods) > 0 {
			// 通过再次同步来更新该 pod。
			// 这里没有采用直接更新状态的优化，后续可以加上。
			handler.HandlePodSyncs(ctx, pods)
		}
	case <-housekeepingCh:
		if !kl.sourcesReady.AllReady() {
			// 若配置来源未就绪、或卷管理器尚未完成状态同步，则跳过 housekeeping，
			// 否则可能误删未就绪来源的 pod。
			logger.V(4).Info("SyncLoop (housekeeping, skipped): sources aren't ready yet")
		} else {
			start := time.Now()
			logger.V(4).Info("SyncLoop (housekeeping)")
			if err := handler.HandlePodCleanups(ctx); err != nil {
				logger.Error(err, "Failed cleaning pods")
			}
			duration := time.Since(start)
			if duration > housekeepingWarningDuration {
				logger.Error(fmt.Errorf("housekeeping took too long"), "Housekeeping took longer than expected", "expected", housekeepingWarningDuration, "actual", duration.Round(time.Millisecond))
			}
			logger.V(4).Info("SyncLoop (housekeeping) end", "duration", duration.Round(time.Millisecond))
		}
	}
	return true
}

// handleProbeSync 处理探针结果更新：找到对应 pod 后触发一次 pod 同步。
func handleProbeSync(ctx context.Context, kl *Kubelet, update proberesults.Update, handler SyncHandler, probe, status string) {
	logger := klog.FromContext(ctx)
	// 不应使用 manager 中的 pod，因为它在初始化后从不更新。
	pod, ok := kl.podManager.GetPodByUID(update.PodUID)
	if !ok {
		// pod 已不存在，忽略该更新。
		logger.V(4).Info("SyncLoop (probe): ignore irrelevant update", "probe", probe, "status", status, "update", update)
		return
	}
	logger.V(1).Info("SyncLoop (probe)", "probe", probe, "status", status, "pod", klog.KObj(pod))
	handler.HandlePodSyncs(ctx, []*v1.Pod{pod})
}

// HandlePodAdditions 是 SyncHandler 中处理配置来源新增 pod 的回调。
func (kl *Kubelet) HandlePodAdditions(ctx context.Context, pods []*v1.Pod) {
	start := kl.clock.Now()
	logger := klog.FromContext(ctx)
	sort.Sort(sliceutils.PodsByCreationTime(pods))
	var pendingResizes []types.UID
	for _, pod := range pods {
		// 始终把 pod 加入 pod manager。kubelet 以 pod manager 作为期望状态的唯一来源：
		// 若 pod 不在 pod manager 中，说明它已在 apiserver 中被删除，除清理外无需其他操作。
		kl.podManager.AddPod(pod)

		kl.podCertificateManager.TrackPod(ctx, pod)

		pod, mirrorPod, wasMirror := kl.podManager.GetPodAndMirrorPod(pod)
		if wasMirror {
			if pod == nil {
				logger.V(2).Info("Unable to find pod for mirror pod, skipping", "mirrorPod", klog.KObj(mirrorPod), "mirrorPodUID", mirrorPod.UID)
				continue
			}
			kl.podWorkers.UpdatePod(ctx, UpdatePodOptions{
				Pod:        pod,
				MirrorPod:  mirrorPod,
				UpdateType: kubetypes.SyncPodUpdate,
				StartTime:  start,
			})
			continue
		}

		// 仅当 kubelet 其他部分未请求终止该 pod 时才走准入流程。
		// 若 pod 已在使用资源（此前已准入），pod worker 会将其关闭；
		// 若 pod 尚未启动，pod worker 被调用时也不会建立该 pod，因此这里直接不做任何工作。
		// 另外，已处于终态的 pod 也不再尝试准入。
		if !kl.podWorkers.IsPodTerminationRequested(pod.UID) && !podutil.IsPodPhaseTerminal(pod.Status.Phase) {
			// 检查能否准入该 pod；不能则拒绝。
			// 被拒绝的 pod 会被置为 Failed，因此 activePods 只包含已准入且存活的 pod。
			if ok, reason, message := kl.allocationManager.AddPod(ctx, kl.GetActivePods(), pod); !ok {
				kl.rejectPod(ctx, pod, reason, message)
				// 不在 canAdmitPod 中记录该指标，因为 resize 期间它会反复调用，导致指标虚高。
				// 改为在 HandlePodAdditions 中为新 pod 记录指标，resize 事件则单独统计。
				recordAdmissionRejection(reason)
				continue
			}
			recordPodLevelResourcesAdmission(pod)

			if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling) {
				// 回填待处理 resize 队列，但必须在所有 pod 添加完成之后进行。
				// 这样可以保证在所有现存 pod 加入之前，任何 resize 都不会被处理。
				_, updatedFromAllocation := kl.allocationManager.UpdatePodFromAllocation(pod)
				if updatedFromAllocation {
					pendingResizes = append(pendingResizes, pod.UID)
				}
			}
		}
		kl.podWorkers.UpdatePod(ctx, UpdatePodOptions{
			Pod:        pod,
			MirrorPod:  mirrorPod,
			UpdateType: kubetypes.SyncPodCreate,
			StartTime:  start,
		})
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling) {
		kl.statusManager.BackfillPodResizeConditions(pods)
		for _, uid := range pendingResizes {
			kl.allocationManager.PushPendingResize(logger, uid)
		}
		if len(pendingResizes) > 0 {
			kl.allocationManager.RetryPendingResizes(ctx, allocation.TriggerReasonPodsAdded)
		}
	}
}

// HandlePodUpdates 是 SyncHandler 接口中处理配置来源 pod 更新的回调。
func (kl *Kubelet) HandlePodUpdates(ctx context.Context, pods []*v1.Pod) {
	start := kl.clock.Now()
	logger := klog.FromContext(ctx)
	for _, pod := range pods {
		// 取出旧 pod，用于后续比较 resize 操作与节点声明特性需求。
		oldPod, _ := kl.podManager.GetPodByUID(pod.UID)
		kl.podManager.UpdatePod(pod)

		if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling) {
			// 跳过尚未分配资源的 pod，避免在准入之前就计入节点容量。
			if !kl.allocationManager.HasPodAllocatedResources(pod.UID) {
				logger.V(4).Info("Skipping pod update for non-allocated pod", "pod", klog.KObj(pod), "podUID", pod.UID)
				continue
			}
		}

		pod, mirrorPod, wasMirror := kl.podManager.GetPodAndMirrorPod(pod)
		if wasMirror {
			if pod == nil {
				logger.V(2).Info("Unable to find pod for mirror pod, skipping", "mirrorPod", klog.KObj(mirrorPod), "mirrorPodUID", mirrorPod.UID)
				continue
			}
		}

		if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling) {
			if recordResizeOperations(oldPod, pod) {
				_, updatedFromAllocation := kl.allocationManager.UpdatePodFromAllocation(pod)
				if updatedFromAllocation {
					kl.allocationManager.PushPendingResize(logger, pod.UID)
					// TODO(natasha41575): 若 resize 立即生效，会触发一次 pod 同步，
					// 最终导致 UpdatePod 被调用两次。看看是否有办法避免。
					kl.allocationManager.RetryPendingResizes(ctx, allocation.TriggerReasonPodUpdated)
				} else {
					// 若待处理的 resize 被回退，会走到这里，因此需要清除待处理 resize 条件。
					kl.statusManager.ClearPodResizePendingCondition(pod.UID, metrics.DeferredResizeResolutionReverted)
				}
			}
		}

		if utilfeature.DefaultFeatureGate.Enabled(features.NodeDeclaredFeatures) && oldPod != nil {
			oldPodInfo := &ndf.PodInfo{Spec: &oldPod.Spec, Status: &oldPod.Status}
			newPodInfo := &ndf.PodInfo{Spec: &pod.Spec, Status: &pod.Status}
			reqs, err := kl.nodeDeclaredFeaturesFramework.InferForPodUpdate(oldPodInfo, newPodInfo, kl.version)
			if err != nil {
				logger.Error(err, "Failed to infer required features for pod update", "pod", klog.KObj(pod))
			}
			if !reqs.IsEmpty() {
				matchResult, err := kl.nodeDeclaredFeaturesFramework.MatchNodeFeatureSet(reqs, kl.nodeDeclaredFeaturesSet)
				if err != nil {
					logger.Error(err, "Failed to match pod features with the node", "pod", klog.KObj(pod))

				}
				if !matchResult.IsMatch {
					missingNodeDeclaredFeatures := strings.Join(matchResult.UnsatisfiedRequirements, ", ")
					logger.Error(nil, "Pod requires node features that are not available", "missingFeatures", missingNodeDeclaredFeatures)
					kl.recorder.WithLogger(logger).Eventf(pod, v1.EventTypeWarning, events.FailedNodeDeclaredFeaturesCheck, "Pod requires node features that are not available: %s", missingNodeDeclaredFeatures)
				}
			}
		}

		kl.podWorkers.UpdatePod(ctx, UpdatePodOptions{
			Pod:        pod,
			MirrorPod:  mirrorPod,
			UpdateType: kubetypes.SyncPodUpdate,
			StartTime:  start,
		})
	}
}

// recordResizeOperations 记录 pod 级资源、容器与卷是否需要 resize，并返回是否存在任一 resize。
func recordResizeOperations(oldPod, newPod *v1.Pod) bool {
	if oldPod == nil {
		// 不应发生。
		return true
	}

	hasContainerResize := recordContainerResizeOperations(oldPod, newPod)
	hasPodLevelResourceResize := recordPodLevelResourceResizeOperations(oldPod, newPod)
	hasVolumeResize := recordVolumeResizeOperations(oldPod, newPod)
	return hasContainerResize || hasPodLevelResourceResize || hasVolumeResize
}

// recordPodLevelResourceResizeOperations 判断 pod 级资源是否需要 resize，并返回结果。
func recordPodLevelResourceResizeOperations(oldPod, newPod *v1.Pod) bool {
	if !utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling) {
		return false
	}

	// TODO(ndixita): 为 pod 级资源 resize 添加指标。

	return !apiequality.Semantic.DeepEqual(oldPod.Spec.Resources, newPod.Spec.Resources)
}

// recordContainerResizeOperations 判断 pod 内任一容器是否需要 resize，并返回结果。
func recordContainerResizeOperations(oldPod, newPod *v1.Pod) bool {
	hasResize := false

	for oldContainer, containerType := range podutil.ContainerIter(&oldPod.Spec, podutil.InitContainers|podutil.Containers) {
		if !allocation.IsResizableContainer(oldContainer, containerType) {
			continue
		}

		var newContainer *v1.Container
		for new, newType := range podutil.ContainerIter(&newPod.Spec, podutil.InitContainers|podutil.Containers) {
			if !allocation.IsResizableContainer(new, newType) {
				continue
			}
			if new.Name == oldContainer.Name && containerType == newType {
				newContainer = new
			}
		}

		newResources := newContainer.Resources
		oldResources := oldContainer.Resources

		if op := resizeOperationForResources(newResources.Requests.Memory(), oldResources.Requests.Memory()); op != "" {
			hasResize = true
			metrics.ContainerRequestedResizes.WithLabelValues("memory", "requests", op).Inc()
		}
		if op := resizeOperationForResources(newResources.Limits.Memory(), oldResources.Limits.Memory()); op != "" {
			hasResize = true
			metrics.ContainerRequestedResizes.WithLabelValues("memory", "limits", op).Inc()
		}
		if op := resizeOperationForResources(newResources.Requests.Cpu(), oldResources.Requests.Cpu()); op != "" {
			hasResize = true
			metrics.ContainerRequestedResizes.WithLabelValues("cpu", "requests", op).Inc()
		}
		if op := resizeOperationForResources(newResources.Limits.Cpu(), oldResources.Limits.Cpu()); op != "" {
			hasResize = true
			metrics.ContainerRequestedResizes.WithLabelValues("cpu", "limits", op).Inc()
		}
	}

	return hasResize
}

// recordVolumeResizeOperations 判断 pod 的 memory-backed emptyDir 卷是否需要 resize，并返回结果。
func recordVolumeResizeOperations(oldPod, newPod *v1.Pod) bool {
	if !utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingMemoryBackedVolumes) {
		return false
	}
	return allocation.IsMemoryBackedVolumeResizeRequested(oldPod, newPod)
}

// resizeOperationForResources 比较新旧资源量，返回 remove、add、decrease、increase 或空字符串（无变化）。
func resizeOperationForResources(new, old *resource.Quantity) string {
	if new.IsZero() && !old.IsZero() {
		return "remove"
	}
	if old.IsZero() && !new.IsZero() {
		return "add"
	}
	if new.Cmp(*old) < 0 {
		return "decrease"
	}
	if new.Cmp(*old) > 0 {
		return "increase"
	}
	return ""
}

// HandlePodRemoves 是 SyncHandler 接口中处理配置来源移除 pod 的回调。
func (kl *Kubelet) HandlePodRemoves(ctx context.Context, pods []*v1.Pod) {
	start := kl.clock.Now()
	logger := klog.FromContext(ctx)
	for _, pod := range pods {
		// 先从各管理器中移除 pod，再走终止流程，避免后续仍把它当作期望状态。
		kl.podCertificateManager.ForgetPod(ctx, pod)
		kl.podManager.RemovePod(pod)
		kl.allocationManager.RemovePod(logger, pod.UID)

		pod, mirrorPod, wasMirror := kl.podManager.GetPodAndMirrorPod(pod)
		if wasMirror {
			if pod == nil {
				logger.V(2).Info("Unable to find pod for mirror pod, skipping", "mirrorPod", klog.KObj(mirrorPod), "mirrorPodUID", mirrorPod.UID)
				continue
			}
			kl.podWorkers.UpdatePod(ctx, UpdatePodOptions{
				Pod:        pod,
				MirrorPod:  mirrorPod,
				UpdateType: kubetypes.SyncPodUpdate,
				StartTime:  start,
			})
			continue
		}

		// 允许删除失败，因为周期性清理流程会再次触发删除。
		if err := kl.deletePod(ctx, pod); err != nil {
			logger.V(2).Info("Failed to delete pod", "pod", klog.KObj(pod), "err", err)
		}
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling) {
		kl.allocationManager.RetryPendingResizes(ctx, allocation.TriggerReasonPodsRemoved)
	}
}

// HandlePodReconcile 是 SyncHandler 接口中处理需要 reconcile 的 pod 的回调。
// 当 API 中仅更新了 pod 状态时，会触发 reconcile。
func (kl *Kubelet) HandlePodReconcile(ctx context.Context, pods []*v1.Pod) {
	start := kl.clock.Now()
	logger := klog.FromContext(ctx)
	retryPendingResizes := false
	var triggerReason string
	// 提前读取是否有待处理 resize，避免在 pod 循环中重复查询。
	hasPendingResizes := kl.allocationManager.HasPendingResizes()
	for _, pod := range pods {
		// 更新 pod manager 中的 pod；status manager 会依据 pod manager 周期性 reconcile。
		oldPod, _ := kl.podManager.GetPodByUID(pod.UID)
		kl.podManager.UpdatePod(pod)

		pod, mirrorPod, wasMirror := kl.podManager.GetPodAndMirrorPod(pod)
		if wasMirror {
			if pod == nil {
				logger.V(2).Info("Unable to find pod for mirror pod, skipping", "mirrorPod", klog.KObj(mirrorPod), "mirrorPodUID", mirrorPod.UID)
				continue
			}
			// static pod 应与普通 pod 以相同方式 reconcile。
		}

		if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling) {
			if hasPendingResizes && !retryPendingResizes && oldPod != nil {
				// pod 进入终态时，重试所有待处理 resize。
				// 即使分配记录已被清除，已终止的 pod 仍会释放容量，因此要在下面的“未分配则跳过”之前检查。
				if podutil.IsPodTerminal(pod) && !podutil.IsPodTerminal(oldPod) {
					retryPendingResizes = true
					triggerReason = allocation.TriggerReasonPodTerminated
				}

				// 若存在待处理 resize，检查请求量是否因状态中的资源变化而减少。
				opts := resourcehelper.PodResourcesOptions{
					UseStatusResources:                             true,
					SkipPodLevelResources:                          !utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResources),
					InPlacePodLevelResourcesVerticalScalingEnabled: utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodLevelResourcesVerticalScaling),
					UseDRANodeAllocatableResourceClaimStatus:       utilfeature.DefaultFeatureGate.Enabled(features.DRANodeAllocatableResources),
				}

				// 聚合资源时忽略期望资源。
				allocatedOldPod, _ := kl.allocationManager.UpdatePodFromAllocation(oldPod)
				allocatedPod, _ := kl.allocationManager.UpdatePodFromAllocation(pod)

				oldRequest := resourcehelper.PodRequests(allocatedOldPod, opts)
				newRequest := resourcehelper.PodRequests(allocatedPod, opts)

				// 若 cpu 或内存请求量减少，则重试待处理 resize。
				if newRequest.Memory().Cmp(*oldRequest.Memory()) < 0 ||
					newRequest.Cpu().Cmp(*oldRequest.Cpu()) < 0 {
					retryPendingResizes = true
					triggerReason = allocation.TriggerReasonPodResized
				}
			}

			// 尚未分配资源的 pod 跳过后续 reconcile。
			// 上面仍更新了 podManager，以保持状态与 apiserver 同步。
			if !kl.allocationManager.HasPodAllocatedResources(pod.UID) {
				logger.V(4).Info("Skipping pod reconcile operations for non-allocated pod", "pod", klog.KObj(pod), "podUID", pod.UID)
				continue
			}
		}

		// TODO: 由 config manager 计算 reconcile 是否合理存疑，避免额外同步可能已无必要。
		// 在解决下面两个 TODO 后，重新评估 Reconcile 与 Sync 能否合并。

		// 必要时 reconcile Pod 的 "Ready" 条件，并触发一次 pod 同步。
		// TODO: 如今这可能已无必要——需要弄清它与 Sync 的差异从何而来，或是否有更合适的位置。
		// 例如 kubelet/config、这里以及 status_manager 中都存在 needsReconcile。
		if status.NeedToReconcilePodReadiness(pod) {
			kl.podWorkers.UpdatePod(ctx, UpdatePodOptions{
				Pod:        pod,
				MirrorPod:  mirrorPod,
				UpdateType: kubetypes.SyncPodSync,
				StartTime:  start,
			})
		}

		// 被驱逐的 pod 同步完成后，可移除其所有死容器。
		// TODO: 这存疑——状态读取是异步的，且驱逐期间本就预期缺少部分容器信息。pod worker
		// 知道 pod 是否被驱逐，若目的是缩短对驱逐的响应时间，可以做得更好；若目的是保留 pod
		// 状态信息，同样可以做得更好。
		if eviction.PodIsEvicted(pod.Status) {
			if podStatus, err := kl.podCache.Get(pod.UID); err == nil {
				kl.containerDeletor.deleteContainersInPod(logger, "", podStatus, true)
			}
		}
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling) {
		if retryPendingResizes {
			kl.allocationManager.RetryPendingResizes(ctx, triggerReason)
		}
	}
}

// HandlePodSyncs 是 SyncHandler 接口中把待同步 pod 分发给 pod worker 的回调。
func (kl *Kubelet) HandlePodSyncs(ctx context.Context, pods []*v1.Pod) {
	start := kl.clock.Now()
	logger := klog.FromContext(ctx)
	for _, pod := range pods {
		pod, mirrorPod, wasMirror := kl.podManager.GetPodAndMirrorPod(pod)
		if wasMirror {
			if pod == nil {
				logger.V(2).Info("Unable to find pod for mirror pod, skipping", "mirrorPod", klog.KObj(mirrorPod), "mirrorPodUID", mirrorPod.UID)
				continue
			}
			// 同步 mirror pod 属于程序错误，因为同步的意图是批量通知所有待处理工作。
			// 应当从根本上杜绝重复同步，目前先记录“程序错误”日志，防止无意引入。
			logger.V(3).Info("Programmer error, HandlePodSyncs does not expect to receive mirror pods", "podUID", pod.UID, "mirrorPodUID", mirrorPod.UID)
			continue
		}
		kl.podWorkers.UpdatePod(ctx, UpdatePodOptions{
			Pod:        pod,
			MirrorPod:  mirrorPod,
			UpdateType: kubetypes.SyncPodSync,
			StartTime:  start,
		})
	}
}

// LatestLoopEntryTime 返回同步循环监视器中记录的最近一次进入时间。
func (kl *Kubelet) LatestLoopEntryTime() time.Time {
	val := kl.syncLoopMonitor.Load()
	if val == nil {
		return time.Time{}
	}
	return val.(time.Time)
}

// SyncLoopHealthCheck 检查 kubelet 更新容器的 sync loop 是否正常工作。
func (kl *Kubelet) SyncLoopHealthCheck(req *http.Request) error {
	duration := kl.resyncInterval * 2
	minDuration := time.Minute * 5
	if duration < minDuration {
		duration = minDuration
	}
	enterLoopTime := kl.LatestLoopEntryTime()
	if !enterLoopTime.IsZero() && time.Now().After(enterLoopTime.Add(duration)) {
		return fmt.Errorf("sync Loop took longer than expected")
	}
	return nil
}

// updateRuntimeUp 调用容器运行时状态回调；容器运行时首次就绪时初始化依赖运行时的模块。
// 状态检查失败时返回错误；检查通过则更新 kubelet runtimeState 中的运行时 uptime。
func (kl *Kubelet) updateRuntimeUp(ctx context.Context) {
	logger := klog.FromContext(ctx)
	// 串行化运行时状态更新，避免与 fastNodeStatusUpdate 并发修改同一状态。
	kl.updateRuntimeMux.Lock()
	defer kl.updateRuntimeMux.Unlock()

	s, err := kl.containerRuntime.Status(ctx)
	if err != nil {
		logger.Error(err, "Container runtime sanity check failed")
		return
	}
	if s == nil {
		logger.Error(nil, "Container runtime status is nil")
		return
	}
	// 周期性记录完整运行时状态以便调试。
	logger.V(4).Info("Container runtime status", "status", s)
	loggerInfo := logger.Info
	if !kl.containerRuntimeReadyExpected {
		loggerInfo = logger.V(4).Info
	}
	networkReady := s.GetRuntimeCondition(kubecontainer.NetworkReady)
	if networkReady == nil || !networkReady.Status {
		loggerInfo("Container runtime network not ready", "networkReady", networkReady)
		kl.runtimeState.setNetworkState(fmt.Errorf("container runtime network not ready: %v", networkReady))
	} else {
		// 容器运行时网络就绪时置为 nil。
		kl.runtimeState.setNetworkState(nil)
	}
	// RuntimeReady 条件中的信息会传播到 NodeReady 条件。
	runtimeReady := s.GetRuntimeCondition(kubecontainer.RuntimeReady)
	// 若 RuntimeReady 未设置或为 false，则报告错误。
	if runtimeReady == nil || !runtimeReady.Status {
		loggerInfo("Container runtime not ready", "runtimeReady", runtimeReady)
		kl.runtimeState.setRuntimeState(fmt.Errorf("container runtime not ready: %v", runtimeReady))
		return
	}

	kl.runtimeState.setRuntimeState(nil)
	kl.runtimeState.setRuntimeHandlers(s.Handlers)
	kl.runtimeState.setRuntimeFeatures(s.Features)
	kl.oneTimeInitializer.Do(func() {
		kl.initializeRuntimeDependentModules(ctx)
	})
	kl.runtimeState.setRuntimeSync(kl.clock.Now())
}

// GetConfiguration 返回用于配置 kubelet 的 KubeletConfiguration。
func (kl *Kubelet) GetConfiguration() kubeletconfiginternal.KubeletConfiguration {
	return kl.kubeletConfiguration
}

// BirthCry 发送 kubelet 已启动的事件。
func (kl *Kubelet) BirthCry() {
	// 生成 kubelet 已重启的事件。
	kl.recorder.Eventf(kl.nodeRefWithUID(), v1.EventTypeNormal, events.StartingKubelet, "Starting kubelet.")
}

// ListenAndServe 运行 kubelet 的 HTTP 服务。
func (kl *Kubelet) ListenAndServe(ctx context.Context, kubeCfg *kubeletconfiginternal.KubeletConfiguration, tlsConfig *tls.Config,
	auth server.AuthInterface, tp trace.TracerProvider) {
	server.ListenAndServeKubeletServer(ctx, kl, kl.resourceAnalyzer, kl.containerManager.GetHealthCheckers(), kl.flagz, kubeCfg, tlsConfig, auth, tp)
}

// ListenAndServeReadOnly 以只读模式运行 kubelet 的 HTTP 服务。
func (kl *Kubelet) ListenAndServeReadOnly(ctx context.Context, address net.IP, port uint, tp trace.TracerProvider) {
	server.ListenAndServeKubeletReadOnlyServer(ctx, kl, kl.resourceAnalyzer, kl.containerManager.GetHealthCheckers(), kl.flagz, address, port, tp)
}

// kubeletPodsProvider 为 podresources 服务提供 pod 查询能力。
type kubeletPodsProvider struct {
	kl *Kubelet
}

// GetActivePods 返回当前活跃（已准入且未终止）的 pod 列表。
func (pp *kubeletPodsProvider) GetActivePods() []*v1.Pod {
	return pp.kl.GetActivePods()
}

// GetPods 返回 pod manager 中记录的全部 pod。
func (pp *kubeletPodsProvider) GetPods() []*v1.Pod {
	return pp.kl.podManager.GetPods()
}

// GetPodByName 按 namespace/name 查找 pod；若 pod 已不活跃则视为不存在。
func (pp *kubeletPodsProvider) GetPodByName(namespace, name string) (*v1.Pod, bool) {
	pod, found := pp.kl.podManager.GetPodByName(namespace, name)
	if !found {
		return nil, false
	}
	// 使用与 GetActivePods 相同的过滤逻辑。
	if pp.kl.isPodInactive(pod) {
		return nil, false
	}
	return pod, true
}

// ListenAndServePodResources 运行 kubelet 的 podresources gRPC 服务。
func (kl *Kubelet) ListenAndServePodResources(ctx context.Context) {
	if utilfeature.DefaultFeatureGate.Enabled(features.KubeletPodResourcesGet) {
		endpoint, err := util.LocalEndpoint(kl.getPodResourcesDir(), podresources.Socket)
		if err != nil {
			klog.FromContext(ctx).V(2).Info("Failed to get local endpoint for PodResources endpoint", "err", err)
			return
		}

		providers := podresources.PodResourcesProviders{
			Pods:             &kubeletPodsProvider{kl: kl},
			Devices:          kl.containerManager,
			Cpus:             kl.containerManager,
			Memory:           kl.containerManager,
			DynamicResources: kl.containerManager,
		}

		server.ListenAndServePodResources(ctx, endpoint, providers)
	}
}

// ListenAndServePods 初始化 HTTP 服务以提供 Pod API。
func (kl *Kubelet) ListenAndServePods(ctx context.Context) {
	if utilfeature.DefaultFeatureGate.Enabled(features.PodsAPI) {
		endpoint, err := util.LocalEndpoint(kl.getPodsAPIDir(), pods.Socket)
		if err != nil {
			klog.FromContext(ctx).Error(err, "Failed to get local endpoint for pod api")
			return
		}
		server.ListenAndServePodsServer(
			ctx,
			endpoint,
			kl.podsServer,
		)
	}
}

// 删除 pod 中符合条件的已退出容器实例；根据配置可能保留最近的死容器。
func (kl *Kubelet) cleanUpContainersInPod(ctx context.Context, podID types.UID, exitedContainerID string) {
	if podStatus, err := kl.podCache.Get(podID); err == nil {
		// 被驱逐或删除的 pod 已完成同步时，可以移除其所有容器。
		removeAll := kl.podWorkers.ShouldPodContentBeRemoved(podID)
		kl.containerDeletor.deleteContainersInPod(klog.FromContext(ctx), exitedContainerID, podStatus, removeAll)
	}
}

// fastStatusUpdateOnce 启动一个循环，检查当前 kubelet 与容器运行时状态能否让节点变为 Ready，
// 并尽快把 Ready 状态同步到 apiserver。函数在状态更新成功后、或节点已 Ready 后返回。
// 该函数只在 kubelet 启动期间执行，通过尽快更新 kubelet 状态、运行时状态与节点状态来缩短节点就绪延迟。
func (kl *Kubelet) fastStatusUpdateOnce(ctx context.Context) {
	start := kl.clock.Now()
	stopCh := make(chan struct{})

	// 持续尝试快速更新节点状态，直到超时或更新成功。
	wait.Until(func() {
		// fastNodeStatusUpdate 在成功时、或宽限期已过（nodeReadyGracePeriod 内未完成状态更新，
		// 即下面第二个参数为 true）时返回 true，此时关闭 channel 并结束循环。
		if kl.fastNodeStatusUpdate(ctx, kl.clock.Since(start) >= nodeReadyGracePeriod) {
			close(stopCh)
		}
	}, 100*time.Millisecond, stopCh)
}

// CheckpointContainer 尝试对容器执行 checkpoint。参数用于定位指定容器；
// 找不到容器时返回错误。找到后请求容器引擎把容器 checkpoint 到 kubelet 默认的 checkpoint 目录。
func (kl *Kubelet) CheckpointContainer(
	ctx context.Context,
	podUID types.UID,
	podFullName,
	containerName string,
	options *runtimeapi.CheckpointContainerRequest,
) error {
	container, err := kl.findContainer(ctx, podFullName, podUID, containerName)
	if err != nil {
		return err
	}
	if container == nil {
		return fmt.Errorf("container %v not found", containerName)
	}

	// checkpoint 文件写入 kubelet 专属目录，并带上可读的时间戳便于定位。
	options.Location = filepath.Join(
		kl.getCheckpointsDir(),
		fmt.Sprintf(
			"checkpoint-%s-%s-%s.tar",
			podFullName,
			containerName,
			time.Now().Format(time.RFC3339),
		),
	)

	options.ContainerId = string(container.ID.ID)

	if err := kl.containerRuntime.CheckpointContainer(ctx, options); err != nil {
		return err
	}

	return nil
}

// ListMetricDescriptors 获取 ListPodSandboxMetrics 将返回的指标描述符。
func (kl *Kubelet) ListMetricDescriptors(ctx context.Context) ([]*runtimeapi.MetricDescriptor, error) {
	return kl.containerRuntime.ListMetricDescriptors(ctx)
}

// ListPodSandboxMetrics 获取所有 pod sandbox 的指标。
func (kl *Kubelet) ListPodSandboxMetrics(ctx context.Context) ([]*runtimeapi.PodSandboxMetrics, error) {
	return kl.containerRuntime.ListPodSandboxMetrics(ctx)
}

// supportLocalStorageCapacityIsolation 返回 kubelet 配置是否启用本地存储容量隔离。
func (kl *Kubelet) supportLocalStorageCapacityIsolation() bool {
	return kl.GetConfiguration().LocalStorageCapacityIsolation
}

// isSyncPodWorthy 过滤掉不值得触发 pod 同步的事件。
func isSyncPodWorthy(event *pleg.PodLifecycleEvent) bool {
	// ContainerRemoved 不影响 pod 状态。
	return event.Type != pleg.ContainerRemoved
}

// PrepareDynamicResources 调用容器管理器的 PrepareDynamicResources API。
// 该方法实现了 RuntimeHelper 接口。
func (kl *Kubelet) PrepareDynamicResources(ctx context.Context, pod *v1.Pod) error {
	return kl.containerManager.PrepareDynamicResources(ctx, pod)
}

// UnprepareDynamicResources 调用容器管理器的 UnprepareDynamicResources API。
// 该方法实现了 RuntimeHelper 接口。
func (kl *Kubelet) UnprepareDynamicResources(ctx context.Context, pod *v1.Pod) error {
	return kl.containerManager.UnprepareDynamicResources(ctx, pod)
}

// 确保 static pod 的 mirror pod 存在且与当前 pod 定义匹配。
// 函数内部记录并忽略所有错误。
func (kl *Kubelet) tryReconcileMirrorPods(ctx context.Context, staticPod, mirrorPod *v1.Pod) {
	logger := klog.FromContext(ctx)
	if !kubetypes.IsStaticPod(staticPod) {
		return
	}
	deleted := false
	if mirrorPod != nil {
		if mirrorPod.DeletionTimestamp != nil || !kubepod.IsMirrorPodOf(mirrorPod, staticPod) {
			// mirror pod 与 static pod 语义不一致，先删除它；mirror pod 之后会重新创建。
			logger.Info("Trying to delete pod", "pod", klog.KObj(mirrorPod), "podUID", mirrorPod.UID)
			podFullName := kubecontainer.GetPodFullName(staticPod)
			if ok, err := kl.mirrorPodClient.DeleteMirrorPod(ctx, podFullName, &mirrorPod.UID); err != nil {
				logger.Error(err, "Failed deleting mirror pod", "pod", klog.KObj(mirrorPod))
			} else if ok {
				deleted = ok
				logger.Info("Deleted mirror pod as it didn't match the static Pod", "pod", klog.KObj(mirrorPod))
			}
		}
	}
	if mirrorPod == nil || deleted {
		node, err := kl.GetNode(ctx)
		if err != nil {
			logger.Error(err, "No need to create a mirror pod, since failed to get node info from the cluster", "node", klog.KRef("", string(kl.nodeName)))
		} else if node.DeletionTimestamp != nil {
			logger.Info("No need to create a mirror pod, since node has been removed from the cluster", "node", klog.KRef("", string(kl.nodeName)))
		} else {
			logger.Info("Creating a mirror pod for static pod", "pod", klog.KObj(staticPod))
			if err := kl.mirrorPodClient.CreateMirrorPod(ctx, staticPod); err != nil {
				logger.Error(err, "Failed creating a mirror pod", "pod", klog.KObj(staticPod))
			}
		}
	}
}

// 节点注册完成后尽快确保 static pod 的 mirror pod 存在。
func (kl *Kubelet) fastStaticPodsRegistration(ctx context.Context) {
	logger := klog.FromContext(ctx)
	// 等待节点注册完成：注册完成前 API 不会接受 mirror pod 的创建请求。
	if err := wait.PollUntilContextCancel(ctx, 100*time.Millisecond, true, func(ctx context.Context) (bool, error) {
		_, err := kl.GetNode(ctx)
		if err == nil {
			return true, nil
		}

		logger.Error(err, "Unable to register mirror pod because node is not registered yet", "node", klog.KRef("", string(kl.nodeName)))
		return false, nil
	}); err != nil {
		logger.Error(err, "Failed to wait until node is registered", "node", klog.KRef("", string(kl.nodeName)))
	}

	staticPodToMirrorPodMap := kl.podManager.GetStaticPodToMirrorPodMap()
	for staticPod, mirrorPod := range staticPodToMirrorPodMap {
		kl.tryReconcileMirrorPods(ctx, staticPod, mirrorPod)
	}
}

// RequestPodReinspect 请求 PLEG 重新检查指定 pod。
func (kl *Kubelet) RequestPodReinspect(podUID types.UID) {
	kl.pleg.RequestReinspect(podUID)
}

// RequestPodRelist 请求 PLEG 对指定 pod 触发一次 relist。
func (kl *Kubelet) RequestPodRelist(logger klog.Logger, podUID types.UID) {
	kl.pleg.RequestRelist(logger, podUID)
}

// syncPodNow 立即触发一次 pod 同步，并请求 PLEG 重新列出该 pod。
func (kl *Kubelet) syncPodNow(ctx context.Context, pod *v1.Pod) {
	kl.HandlePodSyncs(ctx, []*v1.Pod{pod})
	kl.RequestPodRelist(klog.FromContext(ctx), pod.UID)
}

// OnPodSandboxReady 是容器运行时在 sandbox、网络与卷三项条件都就绪后调用的回调，
// 用于立即把 PodReadyToStartContainers 状态条件置为 True。
// 该方法实现了 RuntimeHelper 接口。
// 参考：https://github.com/kubernetes/kubernetes/issues/134460
func (kl *Kubelet) OnPodSandboxReady(ctx context.Context, pod *v1.Pod) error {
	// 特性门控关闭时保持原有状态更新路径，不提前设置该条件。
	if !utilfeature.DefaultFeatureGate.Enabled(features.PodReadyToStartContainersCondition) {
		return nil
	}

	logger := klog.FromContext(ctx)
	logger.V(3).Info("OnPodSandboxReady callback invoked", "pod", klog.KObj(pod), "podUID", pod.UID)

	// 异步更新状态，避免阻塞 SyncPod 流程。
	go func() {
		existingStatus, ok := kl.statusManager.GetPodStatus(pod.UID)
		if !ok {
			existingStatus = pod.Status
		}

		cachedStatus := existingStatus.DeepCopy()

		readySandboxCondition := v1.PodCondition{
			Type:               v1.PodReadyToStartContainers,
			Status:             v1.ConditionTrue,
			ObservedGeneration: podutil.CalculatePodConditionObservedGeneration(cachedStatus, pod.Generation, v1.PodReadyToStartContainers),
		}

		cachedStatus.Conditions = utilpod.ReplaceOrAppendPodCondition(cachedStatus.Conditions, &readySandboxCondition)

		kl.statusManager.SetPodStatus(logger, pod, *cachedStatus)

		logger.V(3).Info("Successfully updated PodReadyToStartContainers condition after sandbox creation",
			"pod", klog.KObj(pod),
			"podUID", pod.UID)
	}()

	return nil
}

// allocatedPodManager 装饰基础的 volumemanager.PodManager，拦截 pod 查询并返回
// allocationManager 计算出的当前分配状态。
//
// 任何只应感知“已分配 pod”的组件都应使用它作为 pod manager。
type allocatedPodManager struct {
	baseManager       volumemanager.PodManager
	allocationManager allocation.Manager
}

// getAllocatedPod 返回 pod 的“已分配”视图；特性门控关闭或无分配记录时原样返回 pod。
func (a *allocatedPodManager) getAllocatedPod(pod *v1.Pod) (*v1.Pod, bool) {
	// TODO: 这是否应作为分配管理器的公开方法？
	if pod == nil {
		return nil, false
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling) {
		if allocatedPod, ok := a.allocationManager.UpdatePodFromAllocation(pod); ok {
			return allocatedPod, true
		}
	}
	// 特性门控关闭、或该 pod 无分配记录时，原样返回 pod。
	return pod, true
}

// GetPodByUID 按 UID 查找 pod，并返回其已分配资源视图。
func (a *allocatedPodManager) GetPodByUID(uid types.UID) (*v1.Pod, bool) {
	pod, found := a.baseManager.GetPodByUID(uid)
	if !found || pod == nil {
		return pod, found
	}
	// 始终返回分配视图，确保卷管理等组件看到实际生效的资源而非期望值。
	return a.getAllocatedPod(pod)
}

// GetPods 返回全部 pod 的已分配资源视图。
func (a *allocatedPodManager) GetPods() []*v1.Pod {
	pods := a.baseManager.GetPods()
	var allocatedPods []*v1.Pod
	for _, pod := range pods {
		if allocatedPod, podFound := a.getAllocatedPod(pod); podFound {
			allocatedPods = append(allocatedPods, allocatedPod)
		}
	}
	return allocatedPods
}
