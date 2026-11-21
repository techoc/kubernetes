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

package kubemark

import (
	"context"
	"fmt"
	"time"

	noopoteltrace "go.opentelemetry.io/otel/trace/noop"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/mount-utils"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	internalapi "k8s.io/cri-api/pkg/apis"
	kubeletapp "k8s.io/kubernetes/cmd/kubelet/app"
	"k8s.io/kubernetes/cmd/kubelet/app/options"
	"k8s.io/kubernetes/pkg/kubelet"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/cadvisor"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	containertest "k8s.io/kubernetes/pkg/kubelet/container/testing"
	probetest "k8s.io/kubernetes/pkg/kubelet/prober/testing"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
	kubeletutil "k8s.io/kubernetes/pkg/kubelet/util"
	"k8s.io/kubernetes/pkg/util/oom"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/configmap"
	"k8s.io/kubernetes/pkg/volume/csi"
	"k8s.io/kubernetes/pkg/volume/downwardapi"
	"k8s.io/kubernetes/pkg/volume/emptydir"
	"k8s.io/kubernetes/pkg/volume/fc"
	"k8s.io/kubernetes/pkg/volume/git_repo"
	"k8s.io/kubernetes/pkg/volume/hostpath"
	"k8s.io/kubernetes/pkg/volume/iscsi"
	"k8s.io/kubernetes/pkg/volume/local"
	"k8s.io/kubernetes/pkg/volume/nfs"
	"k8s.io/kubernetes/pkg/volume/projected"
	"k8s.io/kubernetes/pkg/volume/secret"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	"k8s.io/kubernetes/pkg/volume/util/subpath"
	"k8s.io/kubernetes/test/utils"
)

// HollowKubelet 是 kubemark 场景下的简化版 Kubelet。
// 与真实 Kubelet 不同，它使用大量的 mock/fake 组件替代实际的系统交互，
// 因为空心节点不需要真正运行容器，只需向 API Server 注册并报告状态即可。
type HollowKubelet struct {
	// KubeletFlags 是 Kubelet 的命令行标志配置
	KubeletFlags *options.KubeletFlags
	// KubeletConfiguration 是 Kubelet 的详细配置参数
	KubeletConfiguration *kubeletconfig.KubeletConfiguration
	// KubeletDeps 是 Kubelet 运行所需的依赖注入，包含客户端、插件等
	KubeletDeps *kubelet.Dependencies
}

// volumePlugins 返回 hollow kubelet 所需的卷插件列表。
// 由于空心节点不真正运行容器，大部分插件使用 Fake 版本，但仍需注册以支持相关 Volume 类型。
func volumePlugins() []volume.VolumePlugin {
	allPlugins := []volume.VolumePlugin{}
	// 依次添加各类卷插件的探测结果
	allPlugins = append(allPlugins, emptydir.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, git_repo.ProbeVolumePlugins()...)
	// hostpath 使用 Fake 版本，因为空心节点没有真实的文件系统
	allPlugins = append(allPlugins, hostpath.FakeProbeVolumePlugins(volume.VolumeConfig{})...)
	allPlugins = append(allPlugins, nfs.ProbeVolumePlugins(volume.VolumeConfig{})...)
	allPlugins = append(allPlugins, secret.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, iscsi.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, downwardapi.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, fc.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, configmap.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, projected.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, local.ProbeVolumePlugins()...)
	allPlugins = append(allPlugins, csi.ProbeVolumePlugins()...)
	return allPlugins
}

// NewHollowKubelet 创建 HollowKubelet 实例，初始化 Kubelet 运行所需的所有依赖。
// 关键设计：使用 fake/mock 组件替代真实系统交互，因为空心节点不需要真正管理容器。
//
// 参数说明：
//   - flags: Kubelet 的命令行标志
//   - config: Kubelet 的配置参数
//   - client: 用于与 API Server 通信的客户端
//   - heartbeatClient: 用于发送节点心跳的客户端
//   - cadvisorInterface: 资源监控接口
//   - imageService: 镜像管理服务
//   - runtimeService: 容器运行时服务
//   - containerManager: 容器管理器
func NewHollowKubelet(
	flags *options.KubeletFlags,
	config *kubeletconfig.KubeletConfiguration,
	client *clientset.Clientset,
	heartbeatClient *clientset.Clientset,
	cadvisorInterface cadvisor.Interface,
	imageService internalapi.ImageManagerService,
	runtimeService internalapi.RuntimeService,
	containerManager cm.ContainerManager) *HollowKubelet {
	// Kubemark 没有上层的 logger，直接使用 klog.TODO()
	nodeStartupLatencyTracker := kubeletutil.NewNodeStartupLatencyTracker(klog.TODO())

	// 构建 Kubelet 依赖注入结构体
	d := &kubelet.Dependencies{
		KubeClient:      client,
		HeartbeatClient: heartbeatClient,
		// 使用 fake probe manager，因为空心节点不真正执行探针
		ProbeManager:         probetest.FakeManager{},
		RemoteRuntimeService: runtimeService,
		RemoteImageService:   imageService,
		CAdvisorInterface:    cadvisorInterface,
		// 使用 fake OS 接口，避免与真实操作系统交互
		OSInterface:      &containertest.FakeOS{},
		ContainerManager: containerManager,
		VolumePlugins:    volumePlugins(),
		// 空心节点不需要 TLS
		TLSOptions: nil,
		// 使用 fake OOM adjuster，因为不需要调整真实进程的 OOM score
		OOMAdjuster: oom.NewFakeOOMAdjuster(),
		// 使用 fake mounter，因为不执行真实的挂载操作
		Mounter: &mount.FakeMounter{},
		// 使用 fake subpath，因为不操作真实的卷子路径
		Subpather: &subpath.FakeSubpath{},
		// 使用 fake host util，避免执行真实的系统命令
		HostUtil:                  hostutil.NewFakeHostUtil(nil),
		PodStartupLatencyTracker:  kubeletutil.NewPodStartupLatencyTracker(),
		NodeStartupLatencyTracker: nodeStartupLatencyTracker,
		// 使用 no-op tracer，空心节点不需要分布式追踪
		TracerProvider: noopoteltrace.NewTracerProvider(),
		// 使用 fake recorder，避免尝试读取 /dev/kmsg
		Recorder: &record.FakeRecorder{},
	}

	return &HollowKubelet{
		KubeletFlags:         flags,
		KubeletConfiguration: config,
		KubeletDeps:          d,
	}
}

// Run 启动 HollowKubelet 并阻塞当前协程。
// 调用 kubeletapp.RunKubelet 启动 Kubelet 主循环，成功后进入永久阻塞状态。
func (hk *HollowKubelet) Run(ctx context.Context) {
	// 使用标准的 Kubelet 启动流程，传入配置和依赖
	if err := kubeletapp.RunKubelet(ctx, &options.KubeletServer{
		KubeletFlags:         *hk.KubeletFlags,
		KubeletConfiguration: *hk.KubeletConfiguration,
	}, hk.KubeletDeps); err != nil {
		klog.Fatalf("Failed to run HollowKubelet: %v. Exiting.", err)
	}
	// Kubelet 启动成功后永久阻塞，保持进程运行
	select {}
}

// HollowKubeletOptions 是 hollow kubelet 的可配置参数。
// 用于定制空心节点的行为，如节点名称、端口、资源限制等。
type HollowKubeletOptions struct {
	// NodeName 是空心节点在集群中的名称
	NodeName string
	// KubeletPort 是 Kubelet API 服务的端口
	KubeletPort int
	// KubeletReadOnlyPort 是 Kubelet 只读 API 的端口
	KubeletReadOnlyPort int
	// MaxPods 是该节点上允许的最大 Pod 数量
	MaxPods int
	// PodsPerCore 是每个 CPU 核心允许的 Pod 数量
	PodsPerCore int
	// NodeLabels 是节点的标签，用于节点选择和调度
	NodeLabels map[string]string
	// RegisterWithTaints 是节点注册时附加的污点
	RegisterWithTaints []v1.Taint
}

// GetHollowKubeletConfig 为 HollowKubelet 构建 Kubelet 配置。
// 基于传入的选项设置关键参数，同时保留 Kubelet 的默认值。
func GetHollowKubeletConfig(opt *HollowKubeletOptions) (*options.KubeletFlags, *kubeletconfig.KubeletConfiguration) {
	// 创建临时目录作为 hollow kubelet 的根目录
	testRootDir := utils.MakeTempDirOrDie("hollow-kubelet.", "")
	// 静态 Pod 配置文件目录
	podFilePath := utils.MakeTempDirOrDie("static-pods", testRootDir)
	// Pod 日志目录
	podLogsPath := utils.MakeTempDirOrDie("pod-logs", testRootDir)
	klog.Infof("Using %s as root dir for hollow-kubelet", testRootDir)

	// ---- Flags 结构体：命令行级别的配置 ----
	f := options.NewKubeletFlags()
	f.RootDirectory = testRootDir
	f.HostnameOverride = opt.NodeName
	// 容器 GC 的最小年龄：1 分钟
	f.MinimumGCAge = metav1.Duration{Duration: 1 * time.Minute}
	// 节点最大容器数
	f.MaxContainerCount = 100
	// 每个 Pod 最大容器数
	f.MaxPerPodContainerCount = 2
	f.NodeLabels = opt.NodeLabels

	// ---- Config 结构体：详细的 Kubelet 配置参数 ----
	c, err := options.NewKubeletConfiguration()
	if err != nil {
		panic(err)
	}

	// 镜像服务端点（containerd socket）
	c.ImageServiceEndpoint = "unix:///run/containerd/containerd.sock"
	// 不使用静态 Pod URL
	c.StaticPodURL = ""
	// 启用 Kubelet 的 HTTP 服务器
	c.EnableServer = true
	// 绑定所有接口
	c.Address = "0.0.0.0"
	c.Port = int32(opt.KubeletPort)
	c.ReadOnlyPort = int32(opt.KubeletReadOnlyPort)
	// 静态 Pod 配置路径
	c.StaticPodPath = podFilePath
	// 静态 Pod 文件检查频率
	c.FileCheckFrequency.Duration = 20 * time.Second
	// 静态 Pod HTTP 检查频率
	c.HTTPCheckFrequency.Duration = 20 * time.Second
	// 节点状态更新频率：每 10 秒向 API Server 报告一次
	c.NodeStatusUpdateFrequency.Duration = 10 * time.Second
	// 节点状态报告频率
	c.NodeStatusReportFrequency.Duration = 5 * time.Minute
	// Kubelet 同步频率：每 10 秒同步一次 Pod 状态
	c.SyncFrequency.Duration = 10 * time.Second
	// 驱逐压力转换周期
	c.EvictionPressureTransitionPeriod.Duration = 5 * time.Minute
	// 节点最大 Pod 数
	c.MaxPods = int32(opt.MaxPods)
	// 每核心 Pod 数
	c.PodsPerCore = int32(opt.PodsPerCore)
	// 空心节点不需要集群 DNS
	c.ClusterDNS = []string{}
	// 镜像 GC 高水位：90%
	c.ImageGCHighThresholdPercent = 90
	// 镜像 GC 低水位：80%
	c.ImageGCLowThresholdPercent = 80
	// 设置 provider ID，标识为 kubemark 节点
	c.ProviderID = fmt.Sprintf("kubemark://%v", opt.NodeName)
	// 卷统计聚合周期
	c.VolumeStatsAggPeriod.Duration = time.Minute
	// 空心节点不需要 cgroup
	c.CgroupRoot = ""
	c.CPUCFSQuota = true
	// 禁用控制器附加/分离，因为空心节点不管理真实的卷
	c.EnableControllerAttachDetach = false
	// 启用调试 handler，便于排查问题
	c.EnableDebuggingHandlers = true
	// 禁用 per-QoS cgroup，简化配置
	c.CgroupsPerQOS = false
	// hairpin-veth 用于允许 hairpin 数据包；与真实 kubelet 不同，
	// 因为无法在 docker0 上设置混杂模式
	c.HairpinMode = kubeletconfig.HairpinVeth
	// 最大打开文件数
	c.MaxOpenFiles = 1024
	// 镜像拉取突发限制
	c.RegistryBurst = 10
	// 镜像拉取 QPS 限制
	c.RegistryPullQPS = 5.0
	// DNS 解析器配置
	c.ResolverConfig = kubetypes.ResolvConfDefault
	// Kubelet 自身的 cgroup 路径
	c.KubeletCgroups = "/kubelet"
	// 串行拉取镜像，减少并发压力
	c.SerializeImagePulls = true
	// 空心节点不需要系统 cgroup
	c.SystemCgroups = ""
	// 不保护内核默认值
	c.ProtectKernelDefaults = false
	// 注册时附加的污点
	c.RegisterWithTaints = opt.RegisterWithTaints
	// 启用节点自动注册
	c.RegisterNode = true
	// 启用本地存储容量隔离
	c.LocalStorageCapacityIsolation = true
	// Pod 日志目录
	c.PodLogsDir = podLogsPath

	return f, c
}
