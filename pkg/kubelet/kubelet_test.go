/*
Copyright 2014 The Kubernetes Authors.

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
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	goruntime "runtime"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
	noopoteltrace "go.opentelemetry.io/otel/trace/noop"

	"k8s.io/component-base/metrics/legacyregistry"

	cadvisorapi "github.com/google/cadvisor/lib/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	core "k8s.io/client-go/testing"
	"k8s.io/mount-utils"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	utilversion "k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/cert"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/component-base/featuregate"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/component-base/metrics/testutil"
	ndf "k8s.io/component-helpers/nodedeclaredfeatures"
	ndftesting "k8s.io/component-helpers/nodedeclaredfeatures/testing"
	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	remote "k8s.io/cri-client/pkg"
	fakeremote "k8s.io/cri-client/pkg/fake"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/allocation"
	"k8s.io/kubernetes/pkg/kubelet/allocation/state"
	kubeletconfiginternal "k8s.io/kubernetes/pkg/kubelet/apis/config"
	cadvisortest "k8s.io/kubernetes/pkg/kubelet/cadvisor/testing"
	"k8s.io/kubernetes/pkg/kubelet/clustertrustbundle"
	"k8s.io/kubernetes/pkg/kubelet/cm"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
	"k8s.io/kubernetes/pkg/kubelet/config"
	"k8s.io/kubernetes/pkg/kubelet/configmap"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	containertest "k8s.io/kubernetes/pkg/kubelet/container/testing"
	"k8s.io/kubernetes/pkg/kubelet/eviction"
	"k8s.io/kubernetes/pkg/kubelet/images"
	"k8s.io/kubernetes/pkg/kubelet/kubeletconfig"
	"k8s.io/kubernetes/pkg/kubelet/kuberuntime"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/logs"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/kubernetes/pkg/kubelet/network/dns"
	"k8s.io/kubernetes/pkg/kubelet/nodeshutdown"
	"k8s.io/kubernetes/pkg/kubelet/pleg"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager"
	kubepod "k8s.io/kubernetes/pkg/kubelet/pod"
	podtest "k8s.io/kubernetes/pkg/kubelet/pod/testing"
	"k8s.io/kubernetes/pkg/kubelet/podcertificate"
	proberesults "k8s.io/kubernetes/pkg/kubelet/prober/results"
	probetest "k8s.io/kubernetes/pkg/kubelet/prober/testing"
	"k8s.io/kubernetes/pkg/kubelet/secret"
	"k8s.io/kubernetes/pkg/kubelet/server"
	serverstats "k8s.io/kubernetes/pkg/kubelet/server/stats"
	"k8s.io/kubernetes/pkg/kubelet/stats"
	"k8s.io/kubernetes/pkg/kubelet/status"
	statustest "k8s.io/kubernetes/pkg/kubelet/status/testing"
	"k8s.io/kubernetes/pkg/kubelet/sysctl"
	"k8s.io/kubernetes/pkg/kubelet/token"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
	"k8s.io/kubernetes/pkg/kubelet/userns"
	kubeletutil "k8s.io/kubernetes/pkg/kubelet/util"
	"k8s.io/kubernetes/pkg/kubelet/util/queue"
	kubeletvolume "k8s.io/kubernetes/pkg/kubelet/volumemanager"
	schedulerframework "k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/tainttoleration"
	"k8s.io/kubernetes/pkg/util/oom"
	"k8s.io/kubernetes/pkg/volume"
	_ "k8s.io/kubernetes/pkg/volume/hostpath"
	volumesecret "k8s.io/kubernetes/pkg/volume/secret"
	volumetest "k8s.io/kubernetes/pkg/volume/testing"
	"k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/pkg/volume/util/hostutil"
	"k8s.io/kubernetes/pkg/volume/util/subpath"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/utils/clock"
	testingclock "k8s.io/utils/clock/testing"
)

// init 目前为空，仅保留包级初始化入口；测试依赖都由各用例显式构造。
func init() {
}

const (
	testKubeletHostname = "127.0.0.1"
	testKubeletHostIP   = "127.0.0.1"
	testKubeletHostIPv6 = "::1"

	// TODO(harry) any global place for these two?
	// 所有容器镜像的合理大小范围；Docker Hub 上 90% 的镜像落在这个区间。
	minImgSize int64 = 23 * 1024 * 1024
	maxImgSize int64 = 1000 * 1024 * 1024
)

// fakeImageGCManager 是测试用的假镜像 GC 管理器：
// 直接从假运行时取镜像列表，而不经过缓存。
type fakeImageGCManager struct {
	fakeImageService kubecontainer.ImageService
	images.ImageGCManager
}

// GetImageList 透传给假镜像服务。
func (f *fakeImageGCManager) GetImageList() ([]kubecontainer.Image, error) {
	// ImageGCManager 接口没有 context 参数，所以只能用 context.TODO。
	return f.fakeImageService.ListImages(context.TODO())
}

// TestKubelet 汇总一个测试 kubelet 及其依赖的假实现，Cleanup 负责释放它们。
type TestKubelet struct {
	kubelet              *Kubelet
	fakeRuntime          *containertest.FakeRuntime
	fakeContainerManager *cm.FakeContainerManager
	fakeKubeClient       *fake.Clientset
	fakeMirrorClient     *podtest.FakeMirrorClient
	fakeClock            *testingclock.FakeClock
	pluginManagerStopCh  chan struct{}
	mounter              mount.Interface
	volumePlugin         *volumetest.FakeVolumePlugin
}

// Cleanup 停止插件管理器、删除 kubelet 根目录并清空引用，可安全重复调用。
func (tk *TestKubelet) Cleanup() {
	if tk.kubelet != nil {
		if tk.pluginManagerStopCh != nil {
			close(tk.pluginManagerStopCh)
			tk.pluginManagerStopCh = nil
			// Allow the plugin manager goroutines to observe stopCh before TempDir cleanup.
			time.Sleep(20 * time.Millisecond)
		}
		os.RemoveAll(tk.kubelet.rootDirectory)
		tk.kubelet = nil
	}
}

// newTestKubelet 构造带两张镜像的测试 kubelet，是大多数用例的默认配置。
func newTestKubelet(t *testing.T, controllerAttachDetachEnabled bool) *TestKubelet {
	imageList := []kubecontainer.Image{
		{
			ID:       "abc",
			RepoTags: []string{"registry.k8s.io:v1", "registry.k8s.io:v2"},
			Size:     123,
		},
		{
			ID:       "efg",
			RepoTags: []string{"registry.k8s.io:v3", "registry.k8s.io:v4"},
			Size:     456,
		},
	}
	return newTestKubeletWithImageList(t, imageList, controllerAttachDetachEnabled, true /*initFakeVolumePlugin*/, true /*localStorageCapacityIsolation*/, false /*excludePodAdmitHandlers*/, false /*enableResizing*/)
}

// newTestKubeletExcludeAdmitHandlers 构造不注册准入处理器的 kubelet，
// 让调用方能自行控制准入逻辑；sourcesReady 用于控制 sources 是否就绪。
func newTestKubeletExcludeAdmitHandlers(t *testing.T, controllerAttachDetachEnabled, sourcesReady bool) *TestKubelet {
	imageList := []kubecontainer.Image{
		{
			ID:       "abc",
			RepoTags: []string{"registry.k8s.io:v1", "registry.k8s.io:v2"},
			Size:     123,
		},
		{
			ID:       "efg",
			RepoTags: []string{"registry.k8s.io:v3", "registry.k8s.io:v4"},
			Size:     456,
		},
	}
	return newTestKubeletWithImageList(t, imageList, controllerAttachDetachEnabled, true /*initFakeVolumePlugin*/, true /*localStorageCapacityIsolation*/, true /*excludePodAdmitHandlers*/, sourcesReady)
}

// newTestKubeletWithImageList 组装测试 kubelet 的全部依赖：
// 运行时、状态管理器、卷管理器等均为假实现，并按参数决定是否注入
// 假卷插件、本地存储隔离、准入处理器以及 resize 支持。
func newTestKubeletWithImageList(
	t *testing.T,
	imageList []kubecontainer.Image,
	controllerAttachDetachEnabled bool,
	initFakeVolumePlugin bool,
	localStorageCapacityIsolation bool,
	excludeAdmitHandlers bool,
	enableResizing bool,
) *TestKubelet {
	logger, tCtx := ktesting.NewTestContext(t)

	fakeRuntime := &containertest.FakeRuntime{
		ImageList: imageList,
		// 默认把运行时就绪条件置为 true，省得每个用例再单独设置。
		RuntimeStatus: &kubecontainer.RuntimeStatus{
			Conditions: []kubecontainer.RuntimeCondition{
				{Type: "RuntimeReady", Status: true},
				{Type: "NetworkReady", Status: true},
			},
		},
		VersionInfo: "1.5.0",
		RuntimeType: "test",
		T:           t,
	}

	fakeRecorder := &record.FakeRecorder{}
	fakeKubeClient := &fake.Clientset{}
	kubelet := &Kubelet{}
	kubelet.recorder = fakeRecorder
	kubelet.kubeClient = fakeKubeClient
	kubelet.heartbeatClient = fakeKubeClient
	kubelet.os = &containertest.FakeOS{}
	kubelet.mounter = mount.NewFakeMounter(nil)
	kubelet.hostutil = hostutil.NewFakeHostUtil(nil)
	kubelet.subpather = &subpath.FakeSubpath{}

	kubelet.hostname = testKubeletHostname
	kubelet.nodeName = types.NodeName(testKubeletHostname)
	kubelet.runtimeState = newRuntimeState(maxWaitForContainerRuntime)
	kubelet.runtimeState.setNetworkState(nil)
	kubelet.rootDirectory = t.TempDir()
	kubelet.podLogsDirectory = t.TempDir()
	kubelet.sourcesReady = config.NewSourcesReady(func(_ sets.Set[string]) bool { return true })
	kubelet.serviceLister = testServiceLister{}
	kubelet.serviceHasSynced = func() bool { return true }
	kubelet.nodeHasSynced = func() bool { return true }
	kubelet.nodeLister = testNodeLister{
		nodes: []*v1.Node{
			{
				ObjectMeta: metav1.ObjectMeta{
					Name: string(kubelet.nodeName),
				},
				Status: v1.NodeStatus{
					Conditions: []v1.NodeCondition{
						{
							Type:    v1.NodeReady,
							Status:  v1.ConditionTrue,
							Reason:  "Ready",
							Message: "Node ready",
						},
					},
					Addresses: []v1.NodeAddress{
						{
							Type:    v1.NodeInternalIP,
							Address: testKubeletHostIP,
						},
						{
							Type:    v1.NodeInternalIP,
							Address: testKubeletHostIPv6,
						},
					},
					VolumesAttached: []v1.AttachedVolume{
						{
							Name:       "fake/fake-device",
							DevicePath: "fake/path",
						},
					},
				},
			},
		},
	}
	kubelet.recorder = fakeRecorder
	if err := kubelet.setupDataDirs(logger); err != nil {
		t.Fatalf("can't initialize kubelet data dirs: %v", err)
	}
	kubelet.daemonEndpoints = &v1.NodeDaemonEndpoints{}

	kubelet.cadvisor = &cadvisortest.Fake{}
	machineInfo, _ := kubelet.cadvisor.MachineInfo(logger)
	kubelet.setCachedMachineInfo(machineInfo)
	kubelet.tracer = noopoteltrace.NewTracerProvider().Tracer("")

	fakeMirrorClient := podtest.NewFakeMirrorClient()
	secretManager := secret.NewSimpleSecretManager(kubelet.kubeClient)
	kubelet.secretManager = secretManager
	configMapManager := configmap.NewSimpleConfigMapManager(kubelet.kubeClient)
	kubelet.configMapManager = configMapManager
	kubelet.mirrorPodClient = fakeMirrorClient
	kubelet.podManager = kubepod.NewBasicPodManager()
	kubelet.podStartupLatencyTracker = kubeletutil.NewPodStartupLatencyTracker()
	kubelet.statusManager = status.NewManager(fakeKubeClient, kubelet.podManager, &statustest.FakePodDeletionSafetyProvider{}, kubelet.podStartupLatencyTracker)
	kubelet.nodeStartupLatencyTracker = kubeletutil.NewNodeStartupLatencyTracker(logger)
	kubelet.podCertificateManager = &podcertificate.NoOpManager{}

	kubelet.containerRuntime = fakeRuntime
	kubelet.runtimeCache = containertest.NewFakeRuntimeCache(kubelet.containerRuntime)
	kubelet.reasonCache = NewReasonCache()
	podCache := containertest.NewFakeCache(kubelet.containerRuntime)
	kubelet.podCache = podCache
	kubelet.podWorkers = &fakePodWorkers{
		syncPodFn: kubelet.SyncPod,
		cache:     kubelet.podCache,
		t:         t,
	}

	kubelet.probeManager = probetest.FakeManager{}
	kubelet.livenessManager = proberesults.NewManager()
	kubelet.readinessManager = proberesults.NewManager()
	kubelet.startupManager = proberesults.NewManager()

	fakeContainerManager := cm.NewFakeContainerManager(logger)
	kubelet.containerManager = fakeContainerManager
	fakeNodeRef := &v1.ObjectReference{
		Kind:      "Node",
		Name:      testKubeletHostname,
		UID:       types.UID(testKubeletHostname),
		Namespace: "",
	}

	kubelet.allocationManager = allocation.NewInMemoryManager(
		logger,
		kubelet.statusManager,
		func(ctx context.Context, pod *v1.Pod) { kubelet.HandlePodSyncs(ctx, []*v1.Pod{pod}) },
		kubelet.GetActivePods,
		kubelet.podManager.GetPodByUID,
		config.NewSourcesReady(func(_ sets.Set[string]) bool { return enableResizing }),
		kubelet.recorder,
	)
	volumeStatsAggPeriod := time.Second * 10
	kubelet.resourceAnalyzer = serverstats.NewResourceAnalyzer(tCtx, kubelet, volumeStatsAggPeriod, kubelet.recorder)

	fakeHostStatsProvider := stats.NewFakeHostStatsProvider(&containertest.FakeOS{})

	kubelet.StatsProvider = stats.NewCadvisorStatsProvider(
		kubelet.cadvisor,
		kubelet.resourceAnalyzer,
		kubelet.podManager,
		fakeRuntime,
		kubelet.statusManager,
		fakeHostStatsProvider,
		kubelet.containerManager,
	)
	fakeImageGCPolicy := images.ImageGCPolicy{
		HighThresholdPercent: 90,
		LowThresholdPercent:  80,
	}
	imageGCManager, err := images.NewImageGCManager(fakeRuntime, kubelet.StatsProvider, nil, fakeRecorder, fakeNodeRef, fakeImageGCPolicy, noopoteltrace.NewTracerProvider())
	assert.NoError(t, err)
	kubelet.imageManager = &fakeImageGCManager{
		fakeImageService: fakeRuntime,
		ImageGCManager:   imageGCManager,
	}
	kubelet.containerLogManager = logs.NewStubContainerLogManager()
	containerGCPolicy := kubecontainer.GCPolicy{
		MinAge:             time.Duration(0),
		MaxPerPodContainer: 1,
		MaxContainers:      -1,
	}
	containerGC, err := kubecontainer.NewContainerGC(fakeRuntime, containerGCPolicy, kubelet.sourcesReady)
	assert.NoError(t, err)
	kubelet.containerGC = containerGC

	fakeClock := testingclock.NewFakeClock(time.Now())
	kubelet.crashLoopBackOff = flowcontrol.NewBackOff(time.Second, time.Minute)
	kubelet.crashLoopBackOff.Clock = fakeClock
	kubelet.resyncInterval = 10 * time.Second
	kubelet.workQueue = queue.NewBasicWorkQueue(fakeClock)
	// relist 周期与用例无关，取足够大的值避免后台反复刷新。
	kubelet.pleg = pleg.NewGenericPLEG(fakeRuntime, make(chan *pleg.PodLifecycleEvent, 100), &pleg.RelistDuration{RelistPeriod: time.Hour, RelistThreshold: genericPlegRelistThreshold}, podCache, clock.RealClock{})
	kubelet.clock = fakeClock

	nodeRef := &v1.ObjectReference{
		Kind:      "Node",
		Name:      string(kubelet.nodeName),
		UID:       types.UID(kubelet.nodeName),
		Namespace: "",
	}
	// 驱逐管理器同时充当准入处理器，在准入阶段就拦截会被驱逐的 Pod。
	evictionManager, evictionAdmitHandler := eviction.NewManager(kubelet.resourceAnalyzer, eviction.Config{},
		killPodNow(tCtx, kubelet.podWorkers, fakeRecorder), kubelet.imageManager, kubelet.containerGC, fakeRecorder, nodeRef, kubelet.clock, kubelet.supportLocalStorageCapacityIsolation())

	kubelet.evictionManager = evictionManager
	handlers := []lifecycle.PodAdmitHandler{}
	handlers = append(handlers, evictionAdmitHandler)
	handlers = append(handlers, allocation.NewPodResizesAdmitHandler(kubelet.containerManager, fakeRuntime, kubelet.allocationManager))

	// 关闭管理器同样实现 PodAdmitHandler，节点关闭期间拒绝新 Pod。
	shutdownManager := nodeshutdown.NewManager(&nodeshutdown.Config{
		Logger:                          logger,
		Recorder:                        fakeRecorder,
		NodeRef:                         nodeRef,
		GetPodsFunc:                     kubelet.podManager.GetPods,
		KillPodFunc:                     killPodNow(tCtx, kubelet.podWorkers, fakeRecorder),
		SyncNodeStatusFunc:              func(context.Context) {},
		ShutdownGracePeriodRequested:    0,
		ShutdownGracePeriodCriticalPods: 0,
	})
	kubelet.shutdownManager = shutdownManager
	kubelet.usernsManager, err = userns.MakeUserNsManager(logger, kubelet, nil)
	if err != nil {
		t.Fatalf("Failed to create UserNsManager: %v", err)
	}
	handlers = append(handlers, shutdownManager)

	// 最后注册通用谓词准入处理器，负责资源、污点等基础检查。
	handlers = append(handlers, lifecycle.NewPredicateAdmitHandler(kubelet.GetCachedNode, lifecycle.NewAdmissionFailureHandlerStub(), kubelet.containerManager.UpdatePluginResources))

	if !excludeAdmitHandlers {
		kubelet.allocationManager.AddPodAdmitHandlers(handlers)
	}

	allPlugins := []volume.VolumePlugin{}
	plug := &volumetest.FakeVolumePlugin{PluginName: "fake", Host: nil}
	if initFakeVolumePlugin {
		allPlugins = append(allPlugins, plug)
	} else {
		allPlugins = append(allPlugins, volumesecret.ProbeVolumePlugins()...)
	}

	var prober volume.DynamicPluginProber // TODO (#51147) inject mock
	kubelet.volumePluginMgr, err =
		NewInitializedVolumePluginMgr(logger, kubelet, kubelet.secretManager, kubelet.configMapManager, token.NewManager(kubelet.kubeClient), &clustertrustbundle.NoopManager{}, allPlugins, prober)
	require.NoError(t, err, "Failed to initialize VolumePluginMgr")

	kubelet.volumeManager = kubeletvolume.NewVolumeManager(
		controllerAttachDetachEnabled,
		kubelet.nodeName,
		kubelet.podManager,
		kubelet.podWorkers,
		fakeKubeClient,
		kubelet.volumePluginMgr,
		kubelet.mounter,
		kubelet.hostutil,
		kubelet.getPodsDir(),
		kubelet.recorder,
		volumetest.NewBlockVolumePathHandler(),
		kubelet.statusManager,
		0)

	kubelet.pluginManager = pluginmanager.NewPluginManager(
		kubelet.getPluginsRegistrationDir(), /* sockDir */
		kubelet.recorder,
	)
	pluginManagerStopCh := make(chan struct{})
	kubelet.pluginManagerStopCh = pluginManagerStopCh
	kubelet.setNodeStatusFuncs = kubelet.defaultNodeStatusFuncs()

	// 把超时处理器同时挂到 SyncLoop 和 Sync 两条路径，保证超时 Pod 都能被处理。
	activeDeadlineHandler, err := newActiveDeadlineHandler(kubelet.statusManager, kubelet.recorder, kubelet.clock)
	require.NoError(t, err, "Can't initialize active deadline handler")

	kubelet.AddPodSyncLoopHandler(activeDeadlineHandler)
	kubelet.AddPodSyncHandler(activeDeadlineHandler)
	kubelet.kubeletConfiguration.LocalStorageCapacityIsolation = localStorageCapacityIsolation
	return &TestKubelet{
		kubelet:              kubelet,
		fakeRuntime:          fakeRuntime,
		fakeContainerManager: fakeContainerManager,
		fakeKubeClient:       fakeKubeClient,
		fakeMirrorClient:     fakeMirrorClient,
		fakeClock:            fakeClock,
		pluginManagerStopCh:  pluginManagerStopCh,
		mounter:              nil,
		volumePlugin:         plug,
	}
}

// newTestPods 生成纯占位的测试 Pod：HostNetwork 打开，UID 和名字按序号生成。
func newTestPods(count int) []*v1.Pod {
	pods := make([]*v1.Pod, count)
	for i := 0; i < count; i++ {
		pods[i] = &v1.Pod{
			Spec: v1.PodSpec{
				HostNetwork: true,
			},
			ObjectMeta: metav1.ObjectMeta{
				UID:  types.UID(strconv.Itoa(10000 + i)),
				Name: fmt.Sprintf("pod%d", i),
			},
		}
	}
	return pods
}

// createDNSConfigurer 构造供测试使用的 DNS Configurer。
func createDNSConfigurer() *dns.Configurer {
	recorder := record.NewFakeRecorder(20)
	nodeRef := &v1.ObjectReference{
		Kind:      "Node",
		Name:      "testNode",
		UID:       types.UID("testNode"),
		Namespace: "",
	}
	return dns.NewConfigurer(recorder, nodeRef, nil, nil, "TEST", "")
}

// createGenericRuntimeManager 用测试 kubelet 的依赖构造真实实现的通用运行时管理器，
// 用于需要走完整运行时链路的用例。
func createGenericRuntimeManager(ctx context.Context, kubelet *Kubelet, kubeCfg kubeletconfiginternal.KubeletConfiguration, runtimeSvc internalapi.RuntimeService, imageSvc internalapi.ImageManagerService, tracerProvider oteltrace.TracerProvider) (kuberuntime.KubeGenericRuntime, []images.PostImageGCHook, error) {
	return kuberuntime.NewKubeGenericRuntimeManager(
		ctx,
		kubelet.recorder,
		kubelet.livenessManager,
		kubelet.readinessManager,
		kubelet.startupManager,
		kubelet.rootDirectory,
		kubelet.podLogsDirectory,
		kubelet.machineInfo,
		kubelet.podWorkers,
		kubeCfg.MaxPods,
		kubelet.os,
		kubelet,
		nil,
		kubelet.crashLoopBackOff,
		kubeCfg.SerializeImagePulls,
		kubeCfg.MaxParallelImagePulls,
		float32(kubeCfg.RegistryPullQPS),
		int(kubeCfg.RegistryBurst),
		string(kubeletconfiginternal.NeverVerify),
		nil,
		"",
		"",
		nil,
		kubeCfg.CPUCFSQuota,
		kubeCfg.CPUCFSQuotaPeriod,
		kubeCfg.DefaultPodSysctls,
		runtimeSvc,
		imageSvc,
		kubelet.containerManager,
		kubelet.containerLogManager,
		kubelet.runtimeClassManager,
		false,
		kubeCfg.MemorySwap.SwapBehavior,
		kubelet.containerManager.GetNodeAllocatableAbsolute,
		kubeCfg.MemoryThrottlingFactor,
		kubeCfg.MemoryReservationPolicy,
		kubelet.podStartupLatencyTracker,
		tracerProvider,
		token.NewManager(kubelet.kubeClient),
		func(string, string) (*v1.ServiceAccount, error) { return nil, nil },
		kubelet.podStartupLatencyTracker,
	)
}

// TestSyncLoopAbort 验证更新 channel 关闭后 syncLoop 会立即退出，不会挂起。
func TestSyncLoopAbort(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	kubelet := testKubelet.kubelet
	kubelet.runtimeState.setRuntimeSync(time.Now())
	// syncLoop 会等待 time.After(resyncInterval)，把周期调大以免与 channel 关闭竞争。
	kubelet.resyncInterval = time.Second * 30

	ch := make(chan kubetypes.PodUpdate)
	close(ch)

	// 先做一次健全性检查，同时避免下一步真的挂住。
	ok := kubelet.syncLoopIteration(tCtx, ch, kubelet, make(chan time.Time), make(chan time.Time), make(chan *pleg.PodLifecycleEvent, 1))
	require.False(t, ok, "Expected syncLoopIteration to return !ok since update chan was closed")

	// 正常情况下应立即返回；若挂起说明 syncLoopIteration 的退出路径有问题。
	kubelet.syncLoop(tCtx, ch, kubelet)
}

// TestSyncPodsStartPod 验证 HandlePodSyncs 会通过运行时启动 Pod 中的容器。
func TestSyncPodsStartPod(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	kubelet := testKubelet.kubelet
	fakeRuntime := testKubelet.fakeRuntime
	pods := []*v1.Pod{
		podWithUIDNameNsSpec("12345678", "foo", "new", v1.PodSpec{
			Containers: []v1.Container{
				{Name: "bar"},
			},
		}),
	}
	kubelet.podManager.SetPods(pods)
	kubelet.HandlePodSyncs(tCtx, pods)
	fakeRuntime.AssertStartedPods([]string{string(pods[0].UID)})
}

// TestHandlePodCleanupsPerQOS 验证开启 cgroupsPerQOS 后，多余 Pod 的 cgroup 会被清理。
func TestHandlePodCleanupsPerQOS(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()

	pod := &kubecontainer.Pod{
		ID:        "12345678",
		Name:      "foo",
		Namespace: "new",
		Containers: []*kubecontainer.Container{
			{Name: "bar"},
		},
	}

	fakeRuntime := testKubelet.fakeRuntime
	fakeContainerManager := testKubelet.fakeContainerManager
	// 把 Pod 注册进假 cgroup 管理器，同时让假运行时报告它仍在运行。
	fakeContainerManager.PodContainerManager.AddPodFromCgroups(pod) // add pod to mock cgroup
	fakeRuntime.PodList = []*containertest.FakePod{
		{Pod: pod},
	}
	kubelet := testKubelet.kubelet
	kubelet.cgroupsPerQOS = true // 打开 cgroupsPerQOS 才会进入 cgroup 清理路径

	// HandlePodCleanups 由 kubelet 的 housekeeping 每 2 秒调用一次。
	// 本用例先注册 Pod、再移除这个多余 Pod，然后多调用几次 HandlePodCleanups。
	// podKiller 在 goroutine 中运行，因此这里通过轮询等待 Destroy() 完成，
	// 而不是假设固定的延时一定够用。

	require.NoError(t, kubelet.HandlePodCleanups(tCtx))

	// 断言多余 Pod 已被排队删除。
	if actual, expected := kubelet.podWorkers.(*fakePodWorkers).triggeredDeletion, []types.UID{"12345678"}; !reflect.DeepEqual(actual, expected) {
		t.Fatalf("expected %v to be deleted, got %v", expected, actual)
	}
	fakeRuntime.AssertKilledPods([]string(nil))

	// 模拟 Runtime.KillPod 已经把 Pod 从运行时移除。
	fakeRuntime.PodList = nil

	require.NoError(t, kubelet.HandlePodCleanups(tCtx))
	require.NoError(t, kubelet.HandlePodCleanups(tCtx))
	require.NoError(t, kubelet.HandlePodCleanups(tCtx))

	destroyCount := 0
	err := wait.Poll(100*time.Millisecond, 10*time.Second, func() (bool, error) {
		fakeContainerManager.PodContainerManager.Lock()
		defer fakeContainerManager.PodContainerManager.Unlock()
		destroyCount = 0
		for _, functionName := range fakeContainerManager.PodContainerManager.CalledFunctions {
			if functionName == "Destroy" {
				destroyCount = destroyCount + 1
			}
		}
		return destroyCount >= 1, nil
	})

	assert.NoError(t, err, "wait should not return error")
	// housekeeping 可能被多次调用，且 cgroup 的 Destroy() 在 goroutine 中执行，
	// 因此 Destroy() 的调用次数并不固定，这里只要求至少出现一次。
	// https://github.com/kubernetes/kubernetes/blob/29fdbb065b5e0d195299eb2d260b975cbc554673/pkg/kubelet/kubelet_pods.go#L2006
	assert.GreaterOrEqual(t, destroyCount, 1, "Expect 1 or more destroys")
}

// TestDispatchWorkOfCompletedPod 验证已终止的 Pod 仍会交给 SyncPod 处理一次，不会被跳过。
func TestDispatchWorkOfCompletedPod(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	kubelet := testKubelet.kubelet
	var got bool
	kubelet.podWorkers = &fakePodWorkers{
		syncPodFn: func(ctx context.Context, updateType kubetypes.SyncPodType, pod, mirrorPod *v1.Pod, podStatus *kubecontainer.PodStatus) (bool, func(), error) {
			got = true
			return false, nil, nil
		},
		cache: kubelet.podCache,
		t:     t,
	}
	now := metav1.NewTime(time.Now())
	pods := []*v1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:         "1",
				Name:        "completed-pod1",
				Namespace:   "ns",
				Annotations: make(map[string]string),
			},
			Status: v1.PodStatus{
				Phase: v1.PodFailed,
				ContainerStatuses: []v1.ContainerStatus{
					{
						State: v1.ContainerState{
							Terminated: &v1.ContainerStateTerminated{},
						},
					},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:         "2",
				Name:        "completed-pod2",
				Namespace:   "ns",
				Annotations: make(map[string]string),
			},
			Status: v1.PodStatus{
				Phase: v1.PodSucceeded,
				ContainerStatuses: []v1.ContainerStatus{
					{
						State: v1.ContainerState{
							Terminated: &v1.ContainerStateTerminated{},
						},
					},
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:               "3",
				Name:              "completed-pod3",
				Namespace:         "ns",
				Annotations:       make(map[string]string),
				DeletionTimestamp: &now,
			},
			Status: v1.PodStatus{
				ContainerStatuses: []v1.ContainerStatus{
					{
						State: v1.ContainerState{
							Terminated: &v1.ContainerStateTerminated{},
						},
					},
				},
			},
		},
	}
	// 三个 Pod 的容器都已终止，其中一个还带有 DeletionTimestamp；
	// 无论哪种情况都应触发一次 SyncPod。
	for _, pod := range pods {
		kubelet.podWorkers.UpdatePod(tCtx, UpdatePodOptions{
			Pod:        pod,
			UpdateType: kubetypes.SyncPodSync,
			StartTime:  time.Now(),
		})
		if !got {
			t.Errorf("Should not skip completed pod %q", pod.Name)
		}
		got = false
	}
}

// TestDispatchWorkOfActivePod 验证仍在运行的 Pod 会交给 SyncPod 处理，
// 即使 API 状态显示为 Failed 也不例外。
func TestDispatchWorkOfActivePod(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	kubelet := testKubelet.kubelet
	var got bool
	kubelet.podWorkers = &fakePodWorkers{
		syncPodFn: func(ctx context.Context, updateType kubetypes.SyncPodType, pod, mirrorPod *v1.Pod, podStatus *kubecontainer.PodStatus) (bool, func(), error) {
			got = true
			return false, nil, nil
		},
		cache: kubelet.podCache,
		t:     t,
	}
	pods := []*v1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:         "1",
				Name:        "active-pod1",
				Namespace:   "ns",
				Annotations: make(map[string]string),
			},
			Status: v1.PodStatus{
				Phase: v1.PodRunning,
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:         "2",
				Name:        "active-pod2",
				Namespace:   "ns",
				Annotations: make(map[string]string),
			},
			Status: v1.PodStatus{
				Phase: v1.PodFailed,
				ContainerStatuses: []v1.ContainerStatus{
					{
						State: v1.ContainerState{
							Running: &v1.ContainerStateRunning{},
						},
					},
				},
			},
		},
	}

	for _, pod := range pods {
		kubelet.podWorkers.UpdatePod(tCtx, UpdatePodOptions{
			Pod:        pod,
			UpdateType: kubetypes.SyncPodSync,
			StartTime:  time.Now(),
		})
		if !got {
			t.Errorf("Should not skip active pod %q", pod.Name)
		}
		got = false
	}
}

// TestHandlePodCleanups 验证不再存在于 Pod 管理器中的 Pod 会被排队删除。
func TestHandlePodCleanups(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()

	pod := &kubecontainer.Pod{
		ID:        "12345678",
		Name:      "foo",
		Namespace: "new",
		Containers: []*kubecontainer.Container{
			{Name: "bar"},
		},
	}

	fakeRuntime := testKubelet.fakeRuntime
	fakeRuntime.PodList = []*containertest.FakePod{
		{Pod: pod},
	}
	kubelet := testKubelet.kubelet

	require.NoError(t, kubelet.HandlePodCleanups(tCtx))

	// 断言多余 Pod 已被排队等待杀死。
	if actual, expected := kubelet.podWorkers.(*fakePodWorkers).triggeredDeletion, []types.UID{"12345678"}; !reflect.DeepEqual(actual, expected) {
		t.Fatalf("expected %v to be deleted, got %v", expected, actual)
	}
	fakeRuntime.AssertKilledPods([]string(nil))
}

// TestVolumeAttachLimitExceededCleanup 验证卷挂载数超限时 Pod 会进入 Failed 状态，
// 并完成清理：容器级资源分配被释放，pod worker 也标记该 Pod 已处理完毕。
func TestVolumeAttachLimitExceededCleanup(t *testing.T) {
	tCtx := ktesting.Init(t)
	const podCount = 500
	tk := newTestKubelet(t, true /* controller-attach-detach enabled */)
	defer tk.Cleanup()
	kl := tk.kubelet

	kl.nodeLister = testNodeLister{nodes: []*v1.Node{{
		ObjectMeta: metav1.ObjectMeta{Name: string(kl.nodeName)},
		Status: v1.NodeStatus{
			Allocatable: v1.ResourceList{
				v1.ResourcePods:   *resource.NewQuantity(1000, resource.DecimalSI),
				v1.ResourceCPU:    resource.MustParse("10"),
				v1.ResourceMemory: resource.MustParse("20Gi"),
			},
		},
	}}}

	kl.workQueue = queue.NewBasicWorkQueue(clock.RealClock{})
	kl.podWorkers = newPodWorkers(
		kl, kl.recorder, kl.workQueue,
		kl.resyncInterval, backOffPeriod,
		kl.podCache, kl.allocationManager,
	)

	kl.volumeManager = kubeletvolume.NewFakeVolumeManager(nil, 0, nil, true /* volumeAttachLimitExceededError  */)

	pods, _ := newTestPodsWithResources(podCount)

	kl.podManager.SetPods(pods)
	kl.HandlePodSyncs(tCtx, pods)

	// 所有 Pod 都应因 VolumeAttachmentLimitExceeded 进入终态 Failed。
	if err := wait.PollUntilContextTimeout(
		tCtx, 200*time.Millisecond, 30*time.Second, true,
		func(ctx context.Context) (bool, error) {
			for _, p := range pods {
				st, ok := kl.statusManager.GetPodStatus(p.UID)
				if !ok || st.Phase != v1.PodFailed && st.Reason != "VolumeAttachmentLimitExceeded" {
					return false, nil
				}
			}
			return true, nil
		}); err != nil {
		t.Fatalf("pods did not reach a terminal, Failed state: %v", err)
	}

	// 确认每个 Pod 的 SyncTerminatedPod 都已成功完成。
	if err := wait.PollUntilContextTimeout(
		tCtx, 200*time.Millisecond, 30*time.Second, true,
		func(ctx context.Context) (bool, error) {
			for _, p := range pods {
				if !kl.podWorkers.ShouldPodBeFinished(p.UID) {
					return false, nil
				}
			}
			return true, nil
		}); err != nil {
		t.Fatalf("pod workers did not finish cleanup: %v", err)
	}

	// 确认容器级资源分配已释放，避免挂载失败后泄漏资源。
	for _, p := range pods {
		cn := p.Spec.Containers[0].Name
		if _, still := kl.allocationManager.GetContainerResourceAllocation(p.UID, cn); still {
			t.Fatalf("allocation for pod %q container %q not released", p.Name, cn)
		}
	}
}

// newTestPodsWithResources 生成带最小 CPU/内存请求的 Pod，并一并返回容器名。
func newTestPodsWithResources(count int) (pods []*v1.Pod, containerNames []string) {
	pods = make([]*v1.Pod, count)
	containerNames = make([]string, count)
	for i := 0; i < count; i++ {
		containerName := fmt.Sprintf("container%d", i)
		containerNames[i] = containerName
		pods[i] = &v1.Pod{
			Spec: v1.PodSpec{
				HostNetwork: true,
				Containers: []v1.Container{{
					Name: containerName,
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU:    resource.MustParse("1m"),
							v1.ResourceMemory: resource.MustParse("1Mi"),
						},
					},
				}},
			},
			ObjectMeta: metav1.ObjectMeta{
				UID:  types.UID(strconv.Itoa(10000 + i)),
				Name: fmt.Sprintf("pod%d", i),
			},
		}
	}
	return pods, containerNames
}

// TestHandlePodRemovesWhenSourcesAreReady 验证 sources 未就绪时不会删除 Pod，
// 就绪之后才会真正删除。
func TestHandlePodRemovesWhenSourcesAreReady(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	tCtx := ktesting.Init(t)

	ready := false

	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()

	fakePod := &kubecontainer.Pod{
		ID:        "1",
		Name:      "foo",
		Namespace: "new",
		Containers: []*kubecontainer.Container{
			{Name: "bar"},
		},
	}

	pods := []*v1.Pod{
		podWithUIDNameNs("1", "foo", "new"),
	}

	fakeRuntime := testKubelet.fakeRuntime
	fakeRuntime.PodList = []*containertest.FakePod{
		{Pod: fakePod},
	}
	kubelet := testKubelet.kubelet
	kubelet.sourcesReady = config.NewSourcesReady(func(_ sets.Set[string]) bool { return ready })

	kubelet.HandlePodRemoves(tCtx, pods)
	time.Sleep(2 * time.Second)

	// sources 尚未就绪，不应删除任何 Pod。
	if expect, actual := []types.UID(nil), kubelet.podWorkers.(*fakePodWorkers).triggeredDeletion; !reflect.DeepEqual(expect, actual) {
		t.Fatalf("expected %v kills, got %v", expect, actual)
	}

	ready = true
	kubelet.HandlePodRemoves(tCtx, pods)
	time.Sleep(2 * time.Second)

	// sources 已就绪，此时才删除多余的 Pod。
	if expect, actual := []types.UID{"1"}, kubelet.podWorkers.(*fakePodWorkers).triggeredDeletion; !reflect.DeepEqual(expect, actual) {
		t.Fatalf("expected %v kills, got %v", expect, actual)
	}
}

// testNodeLister 是只读的内存 Node lister，测试通过它注入节点列表。
type testNodeLister struct {
	nodes []*v1.Node
}

// Get 按名字查找节点，找不到时返回错误。
func (nl testNodeLister) Get(name string) (*v1.Node, error) {
	for _, node := range nl.nodes {
		if node.Name == name {
			return node, nil
		}
	}
	return nil, fmt.Errorf("Node with name: %s does not exist", name)
}

// List 忽略 selector，直接返回全部节点。
func (nl testNodeLister) List(_ labels.Selector) (ret []*v1.Node, err error) {
	return nl.nodes, nil
}

// checkPodStatus 断言状态管理器中记录的 Pod 阶段符合预期。
func checkPodStatus(t *testing.T, kl *Kubelet, pod *v1.Pod, phase v1.PodPhase) {
	t.Helper()
	status, found := kl.statusManager.GetPodStatus(pod.UID)
	require.True(t, found, "Status of pod %q is not found in the status map", pod.UID)
	require.Equal(t, phase, status.Phase)
}

// 验证端口冲突会被正确拒绝：冲突的新 Pod 在状态表中被标记为 Failed。
func TestHandlePortConflicts(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	kl := testKubelet.kubelet

	kl.nodeLister = testNodeLister{nodes: []*v1.Node{
		{
			ObjectMeta: metav1.ObjectMeta{Name: string(kl.nodeName)},
			Status: v1.NodeStatus{
				Allocatable: v1.ResourceList{
					v1.ResourcePods: *resource.NewQuantity(110, resource.DecimalSI),
				},
			},
		},
	}}

	kl.dnsConfigurer = createDNSConfigurer()

	spec := v1.PodSpec{NodeName: string(kl.nodeName), Containers: []v1.Container{{Ports: []v1.ContainerPort{{HostPort: 80}}}}}
	pods := []*v1.Pod{
		podWithUIDNameNsSpec("123456789", "newpod", "foo", spec),
		podWithUIDNameNsSpec("987654321", "oldpod", "foo", spec),
	}
	// 让 Pod 的创建时间呈倒序，较新创建的那个才是冲突的一方。
	pods[1].CreationTimestamp = metav1.NewTime(time.Now())
	pods[0].CreationTimestamp = metav1.NewTime(time.Now().Add(1 * time.Second))
	// 较新的 Pod 应被拒绝。
	notfittingPod := pods[0]
	fittingPod := pods[1]
	kl.podWorkers.(*fakePodWorkers).running = map[types.UID]bool{
		pods[0].UID: true,
		pods[1].UID: true,
	}

	kl.HandlePodAdditions(tCtx, pods)

	// 检查写入状态表的 Pod 状态。
	checkPodStatus(t, kl, notfittingPod, v1.PodFailed)
	checkPodStatus(t, kl, fittingPod, v1.PodPending)
}

// 验证主机名冲突会被正确拒绝：NodeName 与当前节点不一致的 Pod 被标记为 Failed。
func TestHandleHostNameConflicts(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	kl := testKubelet.kubelet

	kl.nodeLister = testNodeLister{nodes: []*v1.Node{
		{
			ObjectMeta: metav1.ObjectMeta{Name: "127.0.0.1"},
			Status: v1.NodeStatus{
				Allocatable: v1.ResourceList{
					v1.ResourcePods: *resource.NewQuantity(110, resource.DecimalSI),
				},
			},
		},
	}}

	kl.dnsConfigurer = createDNSConfigurer()

	// 测试环境的默认 NodeName 是 127.0.0.1。
	pods := []*v1.Pod{
		podWithUIDNameNsSpec("123456789", "notfittingpod", "foo", v1.PodSpec{NodeName: "127.0.0.2"}),
		podWithUIDNameNsSpec("987654321", "fittingpod", "foo", v1.PodSpec{NodeName: "127.0.0.1"}),
	}

	notfittingPod := pods[0]
	fittingPod := pods[1]

	kl.HandlePodAdditions(tCtx, pods)

	// Check pod status stored in the status map.
	checkPodStatus(t, kl, notfittingPod, v1.PodFailed)
	checkPodStatus(t, kl, fittingPod, v1.PodPending)
}

// 验证 nodeSelector 与节点标签不匹配时，Pod 被标记为 Failed。
func TestHandleNodeSelector(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	kl := testKubelet.kubelet
	nodes := []*v1.Node{
		{
			ObjectMeta: metav1.ObjectMeta{Name: testKubeletHostname, Labels: map[string]string{"key": "B"}},
			Status: v1.NodeStatus{
				Allocatable: v1.ResourceList{
					v1.ResourcePods: *resource.NewQuantity(110, resource.DecimalSI),
				},
			},
		},
	}
	kl.nodeLister = testNodeLister{nodes: nodes}

	kl.dnsConfigurer = createDNSConfigurer()

	pods := []*v1.Pod{
		podWithUIDNameNsSpec("123456789", "podA", "foo", v1.PodSpec{NodeSelector: map[string]string{"key": "A"}}),
		podWithUIDNameNsSpec("987654321", "podB", "foo", v1.PodSpec{NodeSelector: map[string]string{"key": "B"}}),
	}
	// 第一个 Pod 的 selector 不匹配，应被拒绝。
	notfittingPod := pods[0]
	fittingPod := pods[1]

	kl.HandlePodAdditions(tCtx, pods)

	// Check pod status stored in the status map.
	checkPodStatus(t, kl, notfittingPod, v1.PodFailed)
	checkPodStatus(t, kl, fittingPod, v1.PodPending)
}

// 验证基于 OS/Arch 标签的 nodeSelector 判定：
// 选择器错误时拒绝，选择器正确（含额外标签）时通过准入。
func TestHandleNodeSelectorBasedOnOS(t *testing.T) {
	tests := []struct {
		name        string
		nodeLabels  map[string]string
		podSelector map[string]string
		podStatus   v1.PodPhase
	}{
		{
			name:        "correct OS label, wrong pod selector, admission denied",
			nodeLabels:  map[string]string{v1.LabelOSStable: goruntime.GOOS, v1.LabelArchStable: goruntime.GOARCH},
			podSelector: map[string]string{v1.LabelOSStable: "dummyOS"},
			podStatus:   v1.PodFailed,
		},
		{
			name:        "correct OS label, correct pod selector, admission denied",
			nodeLabels:  map[string]string{v1.LabelOSStable: goruntime.GOOS, v1.LabelArchStable: goruntime.GOARCH},
			podSelector: map[string]string{v1.LabelOSStable: goruntime.GOOS},
			podStatus:   v1.PodPending,
		},
		{
			// 预期不会发生补丁操作：额外的标签 B 应被保留，仍可用于 nodeAffinity。
			name:        "new node label, correct pod selector, admitted",
			nodeLabels:  map[string]string{v1.LabelOSStable: goruntime.GOOS, v1.LabelArchStable: goruntime.GOARCH, "key": "B"},
			podSelector: map[string]string{"key": "B"},
			podStatus:   v1.PodPending,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
			defer testKubelet.Cleanup()
			kl := testKubelet.kubelet
			nodes := []*v1.Node{
				{
					ObjectMeta: metav1.ObjectMeta{Name: testKubeletHostname, Labels: test.nodeLabels},
					Status: v1.NodeStatus{
						Allocatable: v1.ResourceList{
							v1.ResourcePods: *resource.NewQuantity(110, resource.DecimalSI),
						},
					},
				},
			}
			kl.nodeLister = testNodeLister{nodes: nodes}

			kl.dnsConfigurer = createDNSConfigurer()

			pod := podWithUIDNameNsSpec("123456789", "podA", "foo", v1.PodSpec{NodeSelector: test.podSelector})

			kl.HandlePodAdditions(tCtx, []*v1.Pod{pod})

			// Check pod status stored in the status map.
			checkPodStatus(t, kl, pod, test.podStatus)
		})
	}
}

// 验证内存请求超过节点可分配量时，新 Pod 会被标记为 Failed。
func TestHandleMemExceeded(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	kl := testKubelet.kubelet
	nodes := []*v1.Node{
		{ObjectMeta: metav1.ObjectMeta{Name: testKubeletHostname},
			Status: v1.NodeStatus{Capacity: v1.ResourceList{}, Allocatable: v1.ResourceList{
				v1.ResourceCPU:    *resource.NewMilliQuantity(10, resource.DecimalSI),
				v1.ResourceMemory: *resource.NewQuantity(100, resource.BinarySI),
				v1.ResourcePods:   *resource.NewQuantity(40, resource.DecimalSI),
			}}},
	}
	kl.nodeLister = testNodeLister{nodes: nodes}

	kl.dnsConfigurer = createDNSConfigurer()

	spec := v1.PodSpec{NodeName: string(kl.nodeName),
		Containers: []v1.Container{{Resources: v1.ResourceRequirements{
			Requests: v1.ResourceList{
				v1.ResourceMemory: resource.MustParse("90"),
			},
		}}},
	}
	pods := []*v1.Pod{
		podWithUIDNameNsSpec("123456789", "newpod", "foo", spec),
		podWithUIDNameNsSpec("987654321", "oldpod", "foo", spec),
	}
	// 构造创建时间倒序的 Pod，使后创建者成为资源不足的一方。
	pods[1].CreationTimestamp = metav1.NewTime(time.Now())
	pods[0].CreationTimestamp = metav1.NewTime(time.Now().Add(1 * time.Second))
	// 较新的 Pod 应被拒绝。
	notfittingPod := pods[0]
	fittingPod := pods[1]
	kl.podWorkers.(*fakePodWorkers).running = map[types.UID]bool{
		pods[0].UID: true,
		pods[1].UID: true,
	}

	kl.HandlePodAdditions(tCtx, pods)

	// 检查写入状态表的 Pod 状态。
	checkPodStatus(t, kl, notfittingPod, v1.PodFailed)
	checkPodStatus(t, kl, fittingPod, v1.PodPending)
}

// 验证 UpdatePluginResources 的结果被正确处理：可调整的资源放行，
// 可分配量为 0 的资源拒绝，缺失的扩展资源视为通过，分配报错的资源使 Pod 失败。
func TestHandlePluginResources(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet := newTestKubeletExcludeAdmitHandlers(t, false /* controllerAttachDetachEnabled */, false /*enableResizing*/)
	defer testKubelet.Cleanup()
	kl := testKubelet.kubelet

	adjustedResource := v1.ResourceName("domain1.com/adjustedResource")
	emptyResource := v1.ResourceName("domain2.com/emptyResource")
	missingResource := v1.ResourceName("domain2.com/missingResource")
	failedResource := v1.ResourceName("domain2.com/failedResource")
	resourceQuantity0 := *resource.NewQuantity(int64(0), resource.DecimalSI)
	resourceQuantity1 := *resource.NewQuantity(int64(1), resource.DecimalSI)
	resourceQuantity2 := *resource.NewQuantity(int64(2), resource.DecimalSI)
	resourceQuantityInvalid := *resource.NewQuantity(int64(-1), resource.DecimalSI)
	allowedPodQuantity := *resource.NewQuantity(int64(10), resource.DecimalSI)
	nodes := []*v1.Node{
		{ObjectMeta: metav1.ObjectMeta{Name: testKubeletHostname},
			Status: v1.NodeStatus{Capacity: v1.ResourceList{}, Allocatable: v1.ResourceList{
				adjustedResource: resourceQuantity1,
				emptyResource:    resourceQuantity0,
				v1.ResourcePods:  allowedPodQuantity,
			}}},
	}
	kl.nodeLister = testNodeLister{nodes: nodes}

	updatePluginResourcesFunc := func(node *schedulerframework.NodeInfo, attrs *lifecycle.PodAdmitAttributes) error {
		// resourceName -> 用于覆盖 node.Allocatable[resourceName] 的值。
		// 负值会让函数返回错误，以此模拟资源分配失败。
		// 不在这个 map 里的资源保持可分配量不变。
		updateResourceMap := map[v1.ResourceName]resource.Quantity{
			adjustedResource: resourceQuantity2,
			emptyResource:    resourceQuantity0,
			failedResource:   resourceQuantityInvalid,
		}
		pod := attrs.Pod
		newAllocatableResource := node.Allocatable.Clone()
		for _, container := range pod.Spec.Containers {
			for resource := range container.Resources.Requests {
				newQuantity, exist := updateResourceMap[resource]
				if !exist {
					continue
				}
				if newQuantity.Value() < 0 {
					return fmt.Errorf("Allocation failed")
				}
				newAllocatableResource.ScalarResources[resource] = newQuantity.Value()
			}
		}
		node.Allocatable = newAllocatableResource
		return nil
	}

	// 把模拟的 updatePluginResources 注入准入处理器，用来观察它的行为。
	kl.allocationManager.AddPodAdmitHandlers(lifecycle.PodAdmitHandlers{lifecycle.NewPredicateAdmitHandler(kl.GetCachedNode, lifecycle.NewAdmissionFailureHandlerStub(), updatePluginResourcesFunc)})

	kl.dnsConfigurer = createDNSConfigurer()

	// 需要 adjustedResource 的 Pod 能通过，因为 updatePluginResourcesFunc
	// 会把这个资源的可分配量调整到足够大。
	fittingPodSpec := v1.PodSpec{NodeName: string(kl.nodeName),
		Containers: []v1.Container{{Resources: v1.ResourceRequirements{
			Limits: v1.ResourceList{
				adjustedResource: resourceQuantity2,
			},
			Requests: v1.ResourceList{
				adjustedResource: resourceQuantity2,
			},
		}}},
	}
	// 需要 emptyResource（可分配量为 0 的扩展资源）的 Pod 无法通过 PredicateAdmit。
	emptyPodSpec := v1.PodSpec{NodeName: string(kl.nodeName),
		Containers: []v1.Container{{Resources: v1.ResourceRequirements{
			Limits: v1.ResourceList{
				emptyResource: resourceQuantity2,
			},
			Requests: v1.ResourceList{
				emptyResource: resourceQuantity2,
			},
		}}},
	}
	// 需要 missingResource 的 Pod 会通过 PredicateAdmit。
	//
	// 节点状态里缺失的扩展资源在 PredicateAdmit 中会被忽略；
	// 这是为了支持不由 device plugin 管理的扩展资源（例如集群级资源）。
	missingPodSpec := v1.PodSpec{NodeName: string(kl.nodeName),
		Containers: []v1.Container{{Resources: v1.ResourceRequirements{
			Limits: v1.ResourceList{
				missingResource: resourceQuantity2,
			},
			Requests: v1.ResourceList{
				missingResource: resourceQuantity2,
			},
		}}},
	}
	// 需要 failedResource 的 Pod 会因资源分配失败而失败。
	failedPodSpec := v1.PodSpec{NodeName: string(kl.nodeName),
		Containers: []v1.Container{{Resources: v1.ResourceRequirements{
			Limits: v1.ResourceList{
				failedResource: resourceQuantity1,
			},
			Requests: v1.ResourceList{
				failedResource: resourceQuantity1,
			},
		}}},
	}

	fittingPod := podWithUIDNameNsSpec("1", "fittingpod", "foo", fittingPodSpec)
	emptyPod := podWithUIDNameNsSpec("2", "emptypod", "foo", emptyPodSpec)
	missingPod := podWithUIDNameNsSpec("3", "missingpod", "foo", missingPodSpec)
	failedPod := podWithUIDNameNsSpec("4", "failedpod", "foo", failedPodSpec)

	kl.HandlePodAdditions(tCtx, []*v1.Pod{fittingPod, emptyPod, missingPod, failedPod})

	// Check pod status stored in the status map.
	checkPodStatus(t, kl, fittingPod, v1.PodPending)
	checkPodStatus(t, kl, emptyPod, v1.PodFailed)
	checkPodStatus(t, kl, missingPod, v1.PodPending)
	checkPodStatus(t, kl, failedPod, v1.PodFailed)
}

// TODO(filipg): This test should be removed once StatusSyncer can do garbage collection without external signal.
// 验证清理流程会删除已不存在 Pod 的状态缓存条目。
func TestPurgingObsoleteStatusMapEntries(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()

	kl := testKubelet.kubelet
	pods := []*v1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "pod1", UID: "1234"}, Spec: v1.PodSpec{Containers: []v1.Container{{Ports: []v1.ContainerPort{{HostPort: 80}}}}}},
		{ObjectMeta: metav1.ObjectMeta{Name: "pod2", UID: "4567"}, Spec: v1.PodSpec{Containers: []v1.Container{{Ports: []v1.ContainerPort{{HostPort: 80}}}}}},
	}
	podToTest := pods[1]
	// 先跑一次，让状态表里写入这些 Pod 的条目。
	kl.HandlePodAdditions(tCtx, pods)
	if _, found := kl.statusManager.GetPodStatus(podToTest.UID); !found {
		t.Fatalf("expected to have status cached for pod2")
	}
	// 用空的 Pod 列表再同步一次，触发状态表中对应条目的删除。
	kl.podManager.SetPods([]*v1.Pod{})
	require.NoError(t, kl.HandlePodCleanups(tCtx))
	if _, found := kl.statusManager.GetPodStatus(podToTest.UID); found {
		t.Fatalf("expected to not have status cached for pod2")
	}
}

// 验证读取容器日志前的状态校验：只有存在有效容器 ID 的运行或终止状态才允许读取。
func TestValidateContainerLogStatus(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	kubelet := testKubelet.kubelet
	containerName := "x"
	testCases := []struct {
		statuses []v1.ContainerStatus
		success  bool // whether getting logs for the container should succeed.
		pSuccess bool // whether getting logs for the previous container should succeed.
	}{
		{
			statuses: []v1.ContainerStatus{
				{
					Name: containerName,
					State: v1.ContainerState{
						Running: &v1.ContainerStateRunning{},
					},
					LastTerminationState: v1.ContainerState{
						Terminated: &v1.ContainerStateTerminated{ContainerID: "docker://fakeid"},
					},
				},
			},
			success:  true,
			pSuccess: true,
		},
		{
			statuses: []v1.ContainerStatus{
				{
					Name: containerName,
					State: v1.ContainerState{
						Running: &v1.ContainerStateRunning{},
					},
				},
			},
			success:  true,
			pSuccess: false,
		},
		{
			statuses: []v1.ContainerStatus{
				{
					Name: containerName,
					State: v1.ContainerState{
						Terminated: &v1.ContainerStateTerminated{},
					},
				},
			},
			success:  false,
			pSuccess: false,
		},
		{
			statuses: []v1.ContainerStatus{
				{
					Name: containerName,
					State: v1.ContainerState{
						Terminated: &v1.ContainerStateTerminated{ContainerID: "docker://fakeid"},
					},
				},
			},
			success:  true,
			pSuccess: false,
		},
		{
			statuses: []v1.ContainerStatus{
				{
					Name: containerName,
					State: v1.ContainerState{
						Terminated: &v1.ContainerStateTerminated{},
					},
					LastTerminationState: v1.ContainerState{
						Terminated: &v1.ContainerStateTerminated{},
					},
				},
			},
			success:  false,
			pSuccess: false,
		},
		{
			statuses: []v1.ContainerStatus{
				{
					Name: containerName,
					State: v1.ContainerState{
						Terminated: &v1.ContainerStateTerminated{},
					},
					LastTerminationState: v1.ContainerState{
						Terminated: &v1.ContainerStateTerminated{ContainerID: "docker://fakeid"},
					},
				},
			},
			success:  true,
			pSuccess: true,
		},
		{
			statuses: []v1.ContainerStatus{
				{
					Name: containerName,
					State: v1.ContainerState{
						Waiting: &v1.ContainerStateWaiting{},
					},
				},
			},
			success:  false,
			pSuccess: false,
		},
		{
			statuses: []v1.ContainerStatus{
				{
					Name:  containerName,
					State: v1.ContainerState{Waiting: &v1.ContainerStateWaiting{Reason: "ErrImagePull"}},
				},
			},
			success:  false,
			pSuccess: false,
		},
		{
			statuses: []v1.ContainerStatus{
				{
					Name:  containerName,
					State: v1.ContainerState{Waiting: &v1.ContainerStateWaiting{Reason: "ErrImagePullBackOff"}},
				},
			},
			success:  false,
			pSuccess: false,
		},
	}

	for i, tc := range testCases {
		// 先校验最近一次容器。
		previous := false
		podStatus := &v1.PodStatus{ContainerStatuses: tc.statuses}
		_, err := kubelet.validateContainerLogStatus(logger, "podName", podStatus, containerName, previous)
		if !tc.success {
			assert.Errorf(t, err, "[case %d] error", i)
		} else {
			assert.NoErrorf(t, err, "[case %d] error", i)
		}
		// 再校验上一个已终止的容器。
		previous = true
		_, err = kubelet.validateContainerLogStatus(logger, "podName", podStatus, containerName, previous)
		if !tc.pSuccess {
			assert.Errorf(t, err, "[case %d] error", i)
		} else {
			assert.NoErrorf(t, err, "[case %d] error", i)
		}
		// 容器名不在 Pod 中时必须报错。
		_, err = kubelet.validateContainerLogStatus(logger, "podName", podStatus, "blah", false)
		assert.Errorf(t, err, "[case %d] invalid container name should cause an error", i)
	}
}

// 验证创建与更新两条路径都会为静态 Pod 创建镜像 Pod，且不会重复创建。
func TestCreateMirrorPod(t *testing.T) {
	tests := []struct {
		name       string
		updateType kubetypes.SyncPodType
	}{
		{
			name:       "SyncPodCreate",
			updateType: kubetypes.SyncPodCreate,
		},
		{
			name:       "SyncPodUpdate",
			updateType: kubetypes.SyncPodUpdate,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
			defer testKubelet.Cleanup()

			kl := testKubelet.kubelet
			manager := testKubelet.fakeMirrorClient
			pod := podWithUIDNameNs("12345678", "bar", "foo")
			pod.Annotations[kubetypes.ConfigSourceAnnotationKey] = "file"
			pods := []*v1.Pod{pod}
			kl.podManager.SetPods(pods)
			isTerminal, _, err := kl.SyncPod(tCtx, tt.updateType, pod, nil, &kubecontainer.PodStatus{})
			assert.NoError(t, err)
			if isTerminal {
				t.Fatalf("pod should not be terminal: %#v", pod)
			}
			podFullName := kubecontainer.GetPodFullName(pod)
			assert.True(t, manager.HasPod(podFullName), "Expected mirror pod %q to be created", podFullName)
			assert.Equal(t, 1, manager.NumOfPods(), "Expected only 1 mirror pod %q, got %+v", podFullName, manager.GetPods())
		})
	}
}

// 验证静态 Pod 变更后会删除过期的镜像 Pod 并重建。
func TestDeleteOutdatedMirrorPod(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()

	kl := testKubelet.kubelet
	manager := testKubelet.fakeMirrorClient
	pod := podWithUIDNameNsSpec("12345678", "foo", "ns", v1.PodSpec{
		Containers: []v1.Container{
			{Name: "1234", Image: "foo"},
		},
	})
	pod.Annotations[kubetypes.ConfigSourceAnnotationKey] = "file"

	// Mirror pod has an outdated spec.
	mirrorPod := podWithUIDNameNsSpec("11111111", "foo", "ns", v1.PodSpec{
		Containers: []v1.Container{
			{Name: "1234", Image: "bar"},
		},
	})
	mirrorPod.Annotations[kubetypes.ConfigSourceAnnotationKey] = "api"
	mirrorPod.Annotations[kubetypes.ConfigMirrorAnnotationKey] = "mirror"

	pods := []*v1.Pod{pod, mirrorPod}
	kl.podManager.SetPods(pods)
	isTerminal, _, err := kl.SyncPod(tCtx, kubetypes.SyncPodUpdate, pod, mirrorPod, &kubecontainer.PodStatus{})
	assert.NoError(t, err)
	if isTerminal {
		t.Fatalf("pod should not be terminal: %#v", pod)
	}
	name := kubecontainer.GetPodFullName(pod)
	creates, deletes := manager.GetCounts(name)
	if creates != 1 || deletes != 1 {
		t.Errorf("expected 1 creation and 1 deletion of %q, got %d, %d", name, creates, deletes)
	}
}

// 验证清理流程会删除没有对应静态 Pod 的孤儿镜像 Pod，
// 但正在终止的静态 Pod 对应的镜像 Pod 会被保留。
func TestDeleteOrphanedMirrorPods(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()

	kl := testKubelet.kubelet
	manager := testKubelet.fakeMirrorClient
	orphanPods := []*v1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:       "12345678",
				Name:      "pod1",
				Namespace: "ns",
				Annotations: map[string]string{
					kubetypes.ConfigSourceAnnotationKey: "api",
					kubetypes.ConfigMirrorAnnotationKey: "mirror",
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:       "12345679",
				Name:      "pod2",
				Namespace: "ns",
				Annotations: map[string]string{
					kubetypes.ConfigSourceAnnotationKey: "api",
					kubetypes.ConfigMirrorAnnotationKey: "mirror",
				},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:       "12345670",
				Name:      "pod3",
				Namespace: "ns",
				Annotations: map[string]string{
					kubetypes.ConfigSourceAnnotationKey: "api",
					kubetypes.ConfigMirrorAnnotationKey: "mirror",
				},
			},
		},
	}

	kl.podManager.SetPods(orphanPods)

	// 正在终止的静态 Pod，其镜像 Pod 不应被删除。
	kl.podWorkers.(*fakePodWorkers).terminatingStaticPods = map[string]bool{
		kubecontainer.GetPodFullName(orphanPods[2]): true,
	}

	// 用空的 Pod 列表同步，触发所有孤儿镜像 Pod 的删除。
	require.NoError(t, kl.HandlePodCleanups(tCtx))
	assert.Empty(t, manager.GetPods(), "Expected no mirror pods")
	for i, pod := range orphanPods {
		name := kubecontainer.GetPodFullName(pod)
		creates, deletes := manager.GetCounts(name)
		switch i {
		case 2:
			if creates != 0 || deletes != 0 {
				t.Errorf("expected 0 creation and 0 deletion of %q, got %d, %d", name, creates, deletes)
			}
		default:
			if creates != 0 || deletes != 1 {
				t.Errorf("expected 0 creation and one deletion of %q, got %d, %d", name, creates, deletes)
			}
		}
	}
}

// 验证网络组件异常时非 hostNetwork 的 Pod 同步失败，
// 而 hostNetwork 的 Pod 仍可继续同步。
func TestNetworkErrorsWithoutHostNetwork(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	kubelet := testKubelet.kubelet

	kubelet.runtimeState.setNetworkState(fmt.Errorf("simulated network error"))

	pod := podWithUIDNameNsSpec("12345678", "hostnetwork", "new", v1.PodSpec{
		HostNetwork: false,

		Containers: []v1.Container{
			{Name: "foo"},
		},
	})

	kubelet.podManager.SetPods([]*v1.Pod{pod})
	isTerminal, _, err := kubelet.SyncPod(tCtx, kubetypes.SyncPodUpdate, pod, nil, &kubecontainer.PodStatus{})
	assert.Error(t, err, "expected pod with hostNetwork=false to fail when network in error")
	if isTerminal {
		t.Fatalf("pod should not be terminal: %#v", pod)
	}

	pod.Annotations[kubetypes.ConfigSourceAnnotationKey] = kubetypes.FileSource
	pod.Spec.HostNetwork = true
	isTerminal, _, err = kubelet.SyncPod(tCtx, kubetypes.SyncPodUpdate, pod, nil, &kubecontainer.PodStatus{})
	assert.NoError(t, err, "expected pod with hostNetwork=true to succeed when network in error")
	if isTerminal {
		t.Fatalf("pod should not be terminal: %#v", pod)
	}
}

// 验证 filterOutInactivePods 的过滤规则：排除已终止、被准入拒绝、
// 已被判定终止的 Pod，保留其余仍在活动或仍在终止过程中的 Pod。
func TestFilterOutInactivePods(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	kubelet := testKubelet.kubelet
	pods := newTestPods(8)
	now := metav1.NewTime(time.Now())

	// terminal pods are excluded
	pods[0].Status.Phase = v1.PodFailed
	pods[1].Status.Phase = v1.PodSucceeded

	// deleted pod is included unless it's known to be terminated
	pods[2].Status.Phase = v1.PodRunning
	pods[2].DeletionTimestamp = &now
	pods[2].Status.ContainerStatuses = []v1.ContainerStatus{
		{State: v1.ContainerState{
			Running: &v1.ContainerStateRunning{
				StartedAt: now,
			},
		}},
	}

	// pending and running pods are included
	pods[3].Status.Phase = v1.PodPending
	pods[4].Status.Phase = v1.PodRunning

	// pod that is running but has been rejected by admission is excluded
	pods[5].Status.Phase = v1.PodRunning
	kubelet.statusManager.SetPodStatus(logger, pods[5], v1.PodStatus{Phase: v1.PodFailed})

	// pod that is running according to the api but is known terminated is excluded
	pods[6].Status.Phase = v1.PodRunning
	kubelet.podWorkers.(*fakePodWorkers).terminated = map[types.UID]bool{
		pods[6].UID: true,
	}

	// pod that is failed but still terminating is included (it may still be consuming
	// resources)
	pods[7].Status.Phase = v1.PodFailed
	kubelet.podWorkers.(*fakePodWorkers).terminationRequested = map[types.UID]bool{
		pods[7].UID: true,
	}

	expected := []*v1.Pod{pods[2], pods[3], pods[4], pods[7]}
	kubelet.podManager.SetPods(pods)
	actual := kubelet.filterOutInactivePods(pods)
	assert.Equal(t, expected, actual)
}

// 验证 CheckpointContainer 的容器名校验以及 checkpoint 目录的生成规则。
func TestCheckpointContainer(t *testing.T) {
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	kubelet := testKubelet.kubelet

	fakeRuntime := testKubelet.fakeRuntime
	containerID := kubecontainer.ContainerID{
		Type: "test",
		ID:   "abc1234",
	}

	fakePod := &containertest.FakePod{
		Pod: &kubecontainer.Pod{
			ID:        "12345678",
			Name:      "podFoo",
			Namespace: "nsFoo",
			Containers: []*kubecontainer.Container{
				{
					Name: "containerFoo",
					ID:   containerID,
				},
			},
		},
	}

	fakeRuntime.PodList = []*containertest.FakePod{fakePod}
	wrongContainerName := "wrongContainerName"

	tests := []struct {
		name               string
		containerName      string
		checkpointLocation string
		expectedStatus     error
		expectedLocation   string
	}{
		{
			name:               "Checkpoint with wrong container name",
			containerName:      wrongContainerName,
			checkpointLocation: "",
			expectedStatus:     fmt.Errorf("container %s not found", wrongContainerName),
			expectedLocation:   "",
		},
		{
			name:               "Checkpoint with default checkpoint location",
			containerName:      fakePod.Pod.Containers[0].Name,
			checkpointLocation: "",
			expectedStatus:     nil,
			expectedLocation: filepath.Join(
				kubelet.getCheckpointsDir(),
				fmt.Sprintf(
					"checkpoint-%s_%s-%s",
					fakePod.Pod.Name,
					fakePod.Pod.Namespace,
					fakePod.Pod.Containers[0].Name,
				),
			),
		},
		{
			name:               "Checkpoint with ignored location",
			containerName:      fakePod.Pod.Containers[0].Name,
			checkpointLocation: "somethingThatWillBeIgnored",
			expectedStatus:     nil,
			expectedLocation: filepath.Join(
				kubelet.getCheckpointsDir(),
				fmt.Sprintf(
					"checkpoint-%s_%s-%s",
					fakePod.Pod.Name,
					fakePod.Pod.Namespace,
					fakePod.Pod.Containers[0].Name,
				),
			),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			options := &runtimeapi.CheckpointContainerRequest{}
			if test.checkpointLocation != "" {
				options.Location = test.checkpointLocation
			}
			status := kubelet.CheckpointContainer(
				tCtx,
				fakePod.Pod.ID,
				fmt.Sprintf(
					"%s_%s",
					fakePod.Pod.Name,
					fakePod.Pod.Namespace,
				),
				test.containerName,
				options,
			)
			require.Equal(t, test.expectedStatus, status)

			if status != nil {
				return
			}

			require.True(
				t,
				strings.HasPrefix(
					options.Location,
					test.expectedLocation,
				),
			)
			require.Equal(
				t,
				options.ContainerId,
				containerID.ID,
			)

		})
	}
}

// 验证超过 ActiveDeadlineSeconds 的 Pod 会被标记为 Failed，
// 且其状态中包含容器状态信息。
func TestSyncPodsSetStatusToFailedForPodsThatRunTooLong(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	fakeRuntime := testKubelet.fakeRuntime
	kubelet := testKubelet.kubelet

	now := metav1.Now()
	startTime := metav1.NewTime(now.Time.Add(-1 * time.Minute))
	exceededActiveDeadlineSeconds := int64(30)

	pods := []*v1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:       "12345678",
				Name:      "bar",
				Namespace: "new",
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{Name: "foo"},
				},
				ActiveDeadlineSeconds: &exceededActiveDeadlineSeconds,
			},
			Status: v1.PodStatus{
				StartTime: &startTime,
			},
		},
	}

	fakeRuntime.PodList = []*containertest.FakePod{
		{Pod: &kubecontainer.Pod{
			ID:        "12345678",
			Name:      "bar",
			Namespace: "new",
			Containers: []*kubecontainer.Container{
				{Name: "foo"},
			},
		}},
	}

	// 在 HandlePodUpdates 之前先写入分配记录，避免更新路径因缺少分配而报错。
	err := kubelet.allocationManager.SetAllocatedResources(klog.FromContext(tCtx), pods[0])
	require.NoError(t, err)

	// 让 pod worker 在这次同步之后把状态置为失败。
	kubelet.HandlePodUpdates(tCtx, pods)
	status, found := kubelet.statusManager.GetPodStatus(pods[0].UID)
	assert.True(t, found, "expected to found status for pod %q", pods[0].UID)
	assert.Equal(t, v1.PodFailed, status.Phase)
	// 确认状态里带上了 ContainerStatuses 等明细。
	assert.NotNil(t, status.ContainerStatuses)
}

// 验证未超过 ActiveDeadlineSeconds 的 Pod 不会被标记为 Failed。
func TestSyncPodsDoesNotSetPodsThatDidNotRunTooLongToFailed(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	fakeRuntime := testKubelet.fakeRuntime

	kubelet := testKubelet.kubelet

	now := metav1.Now()
	startTime := metav1.NewTime(now.Time.Add(-1 * time.Minute))
	exceededActiveDeadlineSeconds := int64(300)

	pods := []*v1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:       "12345678",
				Name:      "bar",
				Namespace: "new",
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{Name: "foo"},
				},
				ActiveDeadlineSeconds: &exceededActiveDeadlineSeconds,
			},
			Status: v1.PodStatus{
				StartTime: &startTime,
			},
		},
	}

	fakeRuntime.PodList = []*containertest.FakePod{
		{Pod: &kubecontainer.Pod{
			ID:        "12345678",
			Name:      "bar",
			Namespace: "new",
			Containers: []*kubecontainer.Container{
				{Name: "foo"},
			},
		}},
	}

	kubelet.podManager.SetPods(pods)

	// 先写入分配记录，避免更新路径因缺少分配而报错。
	err := kubelet.allocationManager.SetAllocatedResources(klog.FromContext(tCtx), pods[0])
	require.NoError(t, err)

	kubelet.HandlePodUpdates(tCtx, pods)
	status, found := kubelet.statusManager.GetPodStatus(pods[0].UID)
	assert.True(t, found, "expected to found status for pod %q", pods[0].UID)
	assert.NotEqual(t, v1.PodFailed, status.Phase)
}

// podWithUIDNameNs 构造只带 UID、名字、命名空间和空注解的 Pod。
func podWithUIDNameNs(uid types.UID, name, namespace string) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:         uid,
			Name:        name,
			Namespace:   namespace,
			Annotations: map[string]string{},
		},
	}
}

// podWithUIDNameNsSpec 在 podWithUIDNameNs 的基础上设置 Pod 的 Spec。
func podWithUIDNameNsSpec(uid types.UID, name, namespace string, spec v1.PodSpec) *v1.Pod {
	pod := podWithUIDNameNs(uid, name, namespace)
	pod.Spec = spec
	return pod
}

// 验证已从 Pod 管理器删除的 Pod，其目录会在清理时被删除。
func TestDeletePodDirsForDeletedPods(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	kl := testKubelet.kubelet
	pods := []*v1.Pod{
		podWithUIDNameNs("12345678", "pod1", "ns"),
		podWithUIDNameNs("12345679", "pod2", "ns"),
	}

	kl.podManager.SetPods(pods)
	// 先同步一次，把 Pod 目录创建出来。
	kl.HandlePodSyncs(tCtx, kl.podManager.GetPods())
	for i := range pods {
		assert.True(t, dirExists(kl.getPodDir(pods[i].UID)), "Expected directory to exist for pod %d", i)
	}

	// Pod 1 已被删除，不再存在于 Pod 管理器中。
	kl.podManager.SetPods([]*v1.Pod{pods[0]})
	require.NoError(t, kl.HandlePodCleanups(tCtx))
	assert.True(t, dirExists(kl.getPodDir(pods[0].UID)), "Expected directory to exist for pod 0")
	assert.False(t, dirExists(kl.getPodDir(pods[1].UID)), "Expected directory to be deleted for pod 1")
}

// syncAndVerifyPodDir 同步给定 Pod 并执行清理，然后断言目标 Pod 目录是否存在。
func syncAndVerifyPodDir(t *testing.T, testKubelet *TestKubelet, pods []*v1.Pod, podsToCheck []*v1.Pod, shouldExist bool) {
	tCtx := ktesting.Init(t)
	t.Helper()
	kl := testKubelet.kubelet

	kl.podManager.SetPods(pods)
	kl.HandlePodSyncs(tCtx, pods)
	require.NoError(t, kl.HandlePodCleanups(tCtx))
	for i, pod := range podsToCheck {
		exist := dirExists(kl.getPodDir(pod.UID))
		assert.Equal(t, shouldExist, exist, "directory of pod %d", i)
	}
}

// 验证已终止（Failed/Succeeded）的 Pod 目录不会被删除。
func TestDoesNotDeletePodDirsForTerminatedPods(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	kl := testKubelet.kubelet
	pods := []*v1.Pod{
		podWithUIDNameNs("12345678", "pod1", "ns"),
		podWithUIDNameNs("12345679", "pod2", "ns"),
		podWithUIDNameNs("12345680", "pod3", "ns"),
	}
	syncAndVerifyPodDir(t, testKubelet, pods, pods, true)
	// Pod 1 失败、Pod 2 成功；这两个 Pod 的目录都不应被删除。
	kl.statusManager.SetPodStatus(logger, pods[1], v1.PodStatus{Phase: v1.PodFailed})
	kl.statusManager.SetPodStatus(logger, pods[2], v1.PodStatus{Phase: v1.PodSucceeded})
	syncAndVerifyPodDir(t, testKubelet, pods, pods, true)
}

// 验证容器仍在运行时，即使 Pod 已从 API 删除，其目录也应保留。
func TestDoesNotDeletePodDirsIfContainerIsRunning(t *testing.T) {
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	runningPod := &kubecontainer.Pod{
		ID:        "12345678",
		Name:      "pod1",
		Namespace: "ns",
	}
	apiPod := podWithUIDNameNs(runningPod.ID, runningPod.Name, runningPod.Namespace)

	// 先同步一次创建 Pod 目录，并确认目录已存在。
	pods := []*v1.Pod{apiPod}
	testKubelet.kubelet.podWorkers.(*fakePodWorkers).running = map[types.UID]bool{apiPod.UID: true}
	syncAndVerifyPodDir(t, testKubelet, pods, []*v1.Pod{apiPod}, true)

	// 假设 Pod 已从 apiserver 删除，但节点上仍在运行，
	// 此时 Pod 目录不应被删除。
	pods = []*v1.Pod{}
	testKubelet.fakeRuntime.PodList = []*containertest.FakePod{{Pod: runningPod, NetnsPath: ""}}
	syncAndVerifyPodDir(t, testKubelet, pods, []*v1.Pod{apiPod}, true)

	// Pod 已删除且节点上也不再活动，此时 Pod 目录应被删除。
	pods = []*v1.Pod{}
	testKubelet.fakeRuntime.PodList = []*containertest.FakePod{}
	testKubelet.kubelet.podWorkers.(*fakePodWorkers).running = nil
	syncAndVerifyPodDir(t, testKubelet, pods, []*v1.Pod{apiPod}, false)
}

// 验证 getPodsToSync 会合并工作队列中已到期的 Pod 与已超过 ActiveDeadlineSeconds 的 Pod。
func TestGetPodsToSync(t *testing.T) {
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	kubelet := testKubelet.kubelet
	clock := testKubelet.fakeClock
	pods := newTestPods(5)

	exceededActiveDeadlineSeconds := int64(30)
	// pods[0]/pods[2] 的 deadline 只有 30 秒，时间推进后即视为超时；
	// pods[1] 的 deadline 尚未到达，不应被同步。
	notYetActiveDeadlineSeconds := int64(120)
	startTime := metav1.NewTime(clock.Now())
	pods[0].Status.StartTime = &startTime
	pods[0].Spec.ActiveDeadlineSeconds = &exceededActiveDeadlineSeconds
	pods[1].Status.StartTime = &startTime
	pods[1].Spec.ActiveDeadlineSeconds = &notYetActiveDeadlineSeconds
	pods[2].Status.StartTime = &startTime
	pods[2].Spec.ActiveDeadlineSeconds = &exceededActiveDeadlineSeconds

	kubelet.podManager.SetPods(pods)
	kubelet.workQueue.Enqueue(pods[2].UID, 0)
	kubelet.workQueue.Enqueue(pods[3].UID, 30*time.Second)
	kubelet.workQueue.Enqueue(pods[4].UID, 2*time.Minute)

	clock.Step(1 * time.Minute)

	expected := []*v1.Pod{pods[2], pods[3], pods[0]}
	podsToSync := kubelet.getPodsToSync()
	sort.Sort(podsByUID(expected))
	sort.Sort(podsByUID(podsToSync))
	assert.Equal(t, expected, podsToSync)
}

// 验证 generateAPIPodStatus 会把容器状态按 Spec 中的容器顺序排序，
// 不依赖运行时返回的顺序。
func TestGenerateAPIPodStatusWithSortedContainers(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	kubelet := testKubelet.kubelet
	numContainers := 10
	expectedOrder := []string{}
	cStatuses := []*kubecontainer.Status{}
	specContainerList := []v1.Container{}
	for i := 0; i < numContainers; i++ {
		id := fmt.Sprintf("%v", i)
		containerName := fmt.Sprintf("%vcontainer", id)
		expectedOrder = append(expectedOrder, containerName)
		cStatus := &kubecontainer.Status{
			ID:   kubecontainer.BuildContainerID("test", id),
			Name: containerName,
		}
		// 奇偶交替地从头部或尾部插入，故意打乱状态顺序。
		if i%2 == 0 {
			cStatuses = append(cStatuses, cStatus)
		} else {
			cStatuses = append([]*kubecontainer.Status{cStatus}, cStatuses...)
		}
		specContainerList = append(specContainerList, v1.Container{Name: containerName})
	}
	pod := podWithUIDNameNs("uid1", "foo", "test")
	pod.Spec = v1.PodSpec{
		Containers: specContainerList,
	}

	status := &kubecontainer.PodStatus{
		ID:                pod.UID,
		Name:              pod.Name,
		Namespace:         pod.Namespace,
		ContainerStatuses: cStatuses,
	}
	for i := 0; i < 5; i++ {
		apiStatus := kubelet.generateAPIPodStatus(tCtx, pod, status, false)
		for i, c := range apiStatus.ContainerStatuses {
			if expectedOrder[i] != c.Name {
				t.Fatalf("Container status not sorted, expected %v at index %d, but found %v", expectedOrder[i], i, c.Name)
			}
		}
	}
}

// verifyContainerStatuses 逐个容器比较当前状态与上次终止状态。
func verifyContainerStatuses(t *testing.T, statuses []v1.ContainerStatus, expectedState, expectedLastTerminationState map[string]v1.ContainerState, message string) {
	for _, s := range statuses {
		assert.Equal(t, expectedState[s.Name], s.State, "%s: state", message)
		assert.Equal(t, expectedLastTerminationState[s.Name], s.LastTerminationState, "%s: last terminated state", message)
	}
}

// 验证 generateAPIPodStatus 在不同 reason 缓存与旧 API 状态下生成的状态。
func TestGenerateAPIPodStatusWithReasonCache(t *testing.T) {
	tCtx := ktesting.Init(t)
	// 下面的 waiting reason 与 message 由 convertStatusToAPIStatus() 生成。
	testTimestamp := time.Unix(123456789, 987654321)
	testErrorReason := fmt.Errorf("test-error")
	emptyContainerID := (&kubecontainer.ContainerID{}).String()
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	kubelet := testKubelet.kubelet
	pod := podWithUIDNameNs("12345678", "foo", "new")
	pod.Spec = v1.PodSpec{RestartPolicy: v1.RestartPolicyOnFailure}

	podStatus := &kubecontainer.PodStatus{
		ID:        pod.UID,
		Name:      pod.Name,
		Namespace: pod.Namespace,
	}
	tests := []struct {
		containers    []v1.Container
		statuses      []*kubecontainer.Status
		reasons       map[string]error
		oldStatuses   []v1.ContainerStatus
		expectedState map[string]v1.ContainerState
		// 只有 init 容器结果与普通容器不同时才单独设置 expectedInitState。
		expectedInitState            map[string]v1.ContainerState
		expectedLastTerminationState map[string]v1.ContainerState
	}{
		// 没有历史记录的容器：State 应为 Waiting，
		// LastTerminationState 取自 apiserver 中的旧状态。
		{
			containers: []v1.Container{{Name: "without-old-record"}, {Name: "with-old-record"}},
			statuses:   []*kubecontainer.Status{},
			reasons:    map[string]error{},
			oldStatuses: []v1.ContainerStatus{{
				Name:                 "with-old-record",
				LastTerminationState: v1.ContainerState{Terminated: &v1.ContainerStateTerminated{}},
			}},
			expectedState: map[string]v1.ContainerState{
				"without-old-record": {Waiting: &v1.ContainerStateWaiting{
					Reason: ContainerCreating,
				}},
				"with-old-record": {Waiting: &v1.ContainerStateWaiting{
					Reason: ContainerCreating,
				}},
			},
			expectedInitState: map[string]v1.ContainerState{
				"without-old-record": {Waiting: &v1.ContainerStateWaiting{
					Reason: PodInitializing,
				}},
				"with-old-record": {Waiting: &v1.ContainerStateWaiting{
					Reason: PodInitializing,
				}},
			},
			expectedLastTerminationState: map[string]v1.ContainerState{
				"with-old-record": {Terminated: &v1.ContainerStateTerminated{}},
			},
		},
		// 运行中的容器：State 为 Running，
		// LastTerminationState 取自最近一次已终止的状态。
		{
			containers: []v1.Container{{Name: "running"}},
			statuses: []*kubecontainer.Status{
				{
					Name:      "running",
					State:     kubecontainer.ContainerStateRunning,
					StartedAt: testTimestamp,
				},
				{
					Name:     "running",
					State:    kubecontainer.ContainerStateExited,
					ExitCode: 1,
				},
			},
			reasons:     map[string]error{},
			oldStatuses: []v1.ContainerStatus{},
			expectedState: map[string]v1.ContainerState{
				"running": {Running: &v1.ContainerStateRunning{
					StartedAt: metav1.NewTime(testTimestamp),
				}},
			},
			expectedLastTerminationState: map[string]v1.ContainerState{
				"running": {Terminated: &v1.ContainerStateTerminated{
					ExitCode:    1,
					ContainerID: emptyContainerID,
				}},
			},
		},
		// 已终止的容器：
		// * 没有最近的启动错误记录时，State 为 Terminated，
		//   LastTerminationState 取自第二新的已终止状态；
		// * 有最近的启动错误记录时，State 为 Waiting，
		//   LastTerminationState 取自最新的已终止状态；
		// * ExitCode 为 0 且重启策略为 RestartPolicyOnFailure 时容器不会重启，
		//   无论是否有启动错误，State 都是 Terminated，
		//   LastTerminationState 取自第二新的已终止状态。
		{
			containers: []v1.Container{{Name: "without-reason"}, {Name: "with-reason"}},
			statuses: []*kubecontainer.Status{
				{
					Name:     "without-reason",
					State:    kubecontainer.ContainerStateExited,
					ExitCode: 1,
				},
				{
					Name:     "with-reason",
					State:    kubecontainer.ContainerStateExited,
					ExitCode: 2,
				},
				{
					Name:     "without-reason",
					State:    kubecontainer.ContainerStateExited,
					ExitCode: 3,
				},
				{
					Name:     "with-reason",
					State:    kubecontainer.ContainerStateExited,
					ExitCode: 4,
				},
				{
					Name:     "succeed",
					State:    kubecontainer.ContainerStateExited,
					ExitCode: 0,
				},
				{
					Name:     "succeed",
					State:    kubecontainer.ContainerStateExited,
					ExitCode: 5,
				},
			},
			reasons:     map[string]error{"with-reason": testErrorReason, "succeed": testErrorReason},
			oldStatuses: []v1.ContainerStatus{},
			expectedState: map[string]v1.ContainerState{
				"without-reason": {Terminated: &v1.ContainerStateTerminated{
					ExitCode:    1,
					ContainerID: emptyContainerID,
				}},
				"with-reason": {Waiting: &v1.ContainerStateWaiting{Reason: testErrorReason.Error()}},
				"succeed": {Terminated: &v1.ContainerStateTerminated{
					ExitCode:    0,
					ContainerID: emptyContainerID,
				}},
			},
			expectedLastTerminationState: map[string]v1.ContainerState{
				"without-reason": {Terminated: &v1.ContainerStateTerminated{
					ExitCode:    3,
					ContainerID: emptyContainerID,
				}},
				"with-reason": {Terminated: &v1.ContainerStateTerminated{
					ExitCode:    2,
					ContainerID: emptyContainerID,
				}},
				"succeed": {Terminated: &v1.ContainerStateTerminated{
					ExitCode:    5,
					ContainerID: emptyContainerID,
				}},
			},
		},
		// 未知容器状态：
		// 某些情况下容器仍在运行但状态查询失败，会转入 Unknown 状态。
		// 修复前会出现 Running->Waiting 的非法转换；本用例验证
		// Running->Terminated 才是正确行为。
		{
			containers: []v1.Container{{Name: "unknown"}},
			statuses: []*kubecontainer.Status{
				{
					Name:  "unknown",
					State: kubecontainer.ContainerStateUnknown,
				},
				{
					Name:  "unknown",
					State: kubecontainer.ContainerStateRunning,
				},
			},
			reasons: map[string]error{},
			oldStatuses: []v1.ContainerStatus{{
				Name:  "unknown",
				State: v1.ContainerState{Running: &v1.ContainerStateRunning{}},
			}},
			expectedState: map[string]v1.ContainerState{
				"unknown": {Terminated: &v1.ContainerStateTerminated{
					ExitCode: 137,
					Message:  "The container could not be located when the pod was terminated",
					Reason:   kubecontainer.ContainerReasonStatusUnknown,
				}},
			},
			expectedLastTerminationState: map[string]v1.ContainerState{
				"unknown": {Running: &v1.ContainerStateRunning{}},
			},
		},
	}

	for i, test := range tests {
		kubelet.reasonCache = NewReasonCache()
		for n, e := range test.reasons {
			kubelet.reasonCache.add(pod.UID, n, e, "")
		}
		pod.Spec.Containers = test.containers
		pod.Status.ContainerStatuses = test.oldStatuses
		podStatus.ContainerStatuses = test.statuses
		apiStatus := kubelet.generateAPIPodStatus(tCtx, pod, podStatus, false)
		verifyContainerStatuses(t, apiStatus.ContainerStatuses, test.expectedState, test.expectedLastTerminationState, fmt.Sprintf("case %d", i))
	}

	// init 容器走同一套逻辑，只是状态字段换到 InitContainerStatuses。
	for i, test := range tests {
		kubelet.reasonCache = NewReasonCache()
		for n, e := range test.reasons {
			kubelet.reasonCache.add(pod.UID, n, e, "")
		}
		pod.Spec.InitContainers = test.containers
		pod.Status.InitContainerStatuses = test.oldStatuses
		podStatus.ContainerStatuses = test.statuses
		apiStatus := kubelet.generateAPIPodStatus(tCtx, pod, podStatus, false)
		expectedState := test.expectedState
		if test.expectedInitState != nil {
			expectedState = test.expectedInitState
		}
		verifyContainerStatuses(t, apiStatus.InitContainerStatuses, expectedState, test.expectedLastTerminationState, fmt.Sprintf("case %d", i))
	}
}

// 验证不同重启策略下 generateAPIPodStatus 生成的容器状态。
func TestGenerateAPIPodStatusWithDifferentRestartPolicies(t *testing.T) {
	tCtx := ktesting.Init(t)
	testErrorReason := fmt.Errorf("test-error")
	emptyContainerID := (&kubecontainer.ContainerID{}).String()
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	kubelet := testKubelet.kubelet
	pod := podWithUIDNameNs("12345678", "foo", "new")
	containers := []v1.Container{{Name: "succeed"}, {Name: "failed"}}
	podStatus := &kubecontainer.PodStatus{
		ID:        pod.UID,
		Name:      pod.Name,
		Namespace: pod.Namespace,
		ContainerStatuses: []*kubecontainer.Status{
			{
				Name:     "succeed",
				State:    kubecontainer.ContainerStateExited,
				ExitCode: 0,
			},
			{
				Name:     "failed",
				State:    kubecontainer.ContainerStateExited,
				ExitCode: 1,
			},
			{
				Name:     "succeed",
				State:    kubecontainer.ContainerStateExited,
				ExitCode: 2,
			},
			{
				Name:     "failed",
				State:    kubecontainer.ContainerStateExited,
				ExitCode: 3,
			},
		},
	}
	kubelet.reasonCache.add(pod.UID, "succeed", testErrorReason, "")
	kubelet.reasonCache.add(pod.UID, "failed", testErrorReason, "")
	for c, test := range []struct {
		restartPolicy                v1.RestartPolicy
		expectedState                map[string]v1.ContainerState
		expectedLastTerminationState map[string]v1.ContainerState
		// 只有 init 容器结果与普通容器不同时才单独设置 expectedInitState。
		expectedInitState map[string]v1.ContainerState
		// Only set expectedInitLastTerminationState when it is different from expectedLastTerminationState
		expectedInitLastTerminationState map[string]v1.ContainerState
	}{
		{
			restartPolicy: v1.RestartPolicyNever,
			expectedState: map[string]v1.ContainerState{
				"succeed": {Terminated: &v1.ContainerStateTerminated{
					ExitCode:    0,
					ContainerID: emptyContainerID,
				}},
				"failed": {Terminated: &v1.ContainerStateTerminated{
					ExitCode:    1,
					ContainerID: emptyContainerID,
				}},
			},
			expectedLastTerminationState: map[string]v1.ContainerState{
				"succeed": {Terminated: &v1.ContainerStateTerminated{
					ExitCode:    2,
					ContainerID: emptyContainerID,
				}},
				"failed": {Terminated: &v1.ContainerStateTerminated{
					ExitCode:    3,
					ContainerID: emptyContainerID,
				}},
			},
		},
		{
			restartPolicy: v1.RestartPolicyOnFailure,
			expectedState: map[string]v1.ContainerState{
				"succeed": {Terminated: &v1.ContainerStateTerminated{
					ExitCode:    0,
					ContainerID: emptyContainerID,
				}},
				"failed": {Waiting: &v1.ContainerStateWaiting{Reason: testErrorReason.Error()}},
			},
			expectedLastTerminationState: map[string]v1.ContainerState{
				"succeed": {Terminated: &v1.ContainerStateTerminated{
					ExitCode:    2,
					ContainerID: emptyContainerID,
				}},
				"failed": {Terminated: &v1.ContainerStateTerminated{
					ExitCode:    1,
					ContainerID: emptyContainerID,
				}},
			},
		},
		{
			restartPolicy: v1.RestartPolicyAlways,
			expectedState: map[string]v1.ContainerState{
				"succeed": {Waiting: &v1.ContainerStateWaiting{Reason: testErrorReason.Error()}},
				"failed":  {Waiting: &v1.ContainerStateWaiting{Reason: testErrorReason.Error()}},
			},
			expectedLastTerminationState: map[string]v1.ContainerState{
				"succeed": {Terminated: &v1.ContainerStateTerminated{
					ExitCode:    0,
					ContainerID: emptyContainerID,
				}},
				"failed": {Terminated: &v1.ContainerStateTerminated{
					ExitCode:    1,
					ContainerID: emptyContainerID,
				}},
			},
			// init 容器以退出码 0 终止后不会重启，即使策略是 RestartAlways。
			expectedInitState: map[string]v1.ContainerState{
				"succeed": {Terminated: &v1.ContainerStateTerminated{
					ExitCode:    0,
					ContainerID: emptyContainerID,
				}},
				"failed": {Waiting: &v1.ContainerStateWaiting{Reason: testErrorReason.Error()}},
			},
			expectedInitLastTerminationState: map[string]v1.ContainerState{
				"succeed": {Terminated: &v1.ContainerStateTerminated{
					ExitCode:    2,
					ContainerID: emptyContainerID,
				}},
				"failed": {Terminated: &v1.ContainerStateTerminated{
					ExitCode:    1,
					ContainerID: emptyContainerID,
				}},
			},
		},
	} {
		pod.Spec.RestartPolicy = test.restartPolicy
		// Test normal containers
		pod.Spec.Containers = containers
		apiStatus := kubelet.generateAPIPodStatus(tCtx, pod, podStatus, false)
		expectedState, expectedLastTerminationState := test.expectedState, test.expectedLastTerminationState
		verifyContainerStatuses(t, apiStatus.ContainerStatuses, expectedState, expectedLastTerminationState, fmt.Sprintf("case %d", c))
		pod.Spec.Containers = nil

		// Test init containers
		pod.Spec.InitContainers = containers
		apiStatus = kubelet.generateAPIPodStatus(tCtx, pod, podStatus, false)
		if test.expectedInitState != nil {
			expectedState = test.expectedInitState
		}
		if test.expectedInitLastTerminationState != nil {
			expectedLastTerminationState = test.expectedInitLastTerminationState
		}
		verifyContainerStatuses(t, apiStatus.InitContainerStatuses, expectedState, expectedLastTerminationState, fmt.Sprintf("case %d", c))
		pod.Spec.InitContainers = nil
	}
}

// 验证容器级重启策略（含重启规则）下 generateAPIPodStatus 生成的状态。
func TestGenerateAPIPodStatusWithContainerRestartPolicies(t *testing.T) {
	tCtx := ktesting.Init(t)
	var (
		containerRestartPolicyAlways    = v1.ContainerRestartPolicyAlways
		containerRestartPolicyOnFailure = v1.ContainerRestartPolicyOnFailure
		containerRestartPolicyNever     = v1.ContainerRestartPolicyNever
	)
	testErrorReason := fmt.Errorf("test-error")
	emptyContainerID := (&kubecontainer.ContainerID{}).String()
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	kubelet := testKubelet.kubelet
	pod := podWithUIDNameNs("12345678", "foo", "new")
	podStatus := &kubecontainer.PodStatus{
		ID:        pod.UID,
		Name:      pod.Name,
		Namespace: pod.Namespace,
		ContainerStatuses: []*kubecontainer.Status{
			{
				Name:     "succeed",
				State:    kubecontainer.ContainerStateExited,
				ExitCode: 0,
			},
			{
				Name:     "failed",
				State:    kubecontainer.ContainerStateExited,
				ExitCode: 1,
			},
			{
				Name:     "succeed",
				State:    kubecontainer.ContainerStateExited,
				ExitCode: 2,
			},
			{
				Name:     "failed",
				State:    kubecontainer.ContainerStateExited,
				ExitCode: 3,
			},
		},
	}
	kubelet.reasonCache.add(pod.UID, "succeed", testErrorReason, "")
	kubelet.reasonCache.add(pod.UID, "failed", testErrorReason, "")

	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ContainerRestartRules, true)
	for _, test := range []struct {
		desc                         string
		containers                   []v1.Container
		expectedState                map[string]v1.ContainerState
		expectedLastTerminationState map[string]v1.ContainerState
	}{
		{
			desc: "container restart policy rules match",
			containers: []v1.Container{
				{
					Name:          "failed",
					RestartPolicy: &containerRestartPolicyNever,
					RestartPolicyRules: []v1.ContainerRestartRule{{
						Action: v1.ContainerRestartRuleActionRestart,
						ExitCodes: &v1.ContainerRestartRuleOnExitCodes{
							Operator: v1.ContainerRestartRuleOnExitCodesOpIn,
							Values:   []int32{1},
						},
					}},
				},
			},
			expectedState: map[string]v1.ContainerState{
				"failed": {Waiting: &v1.ContainerStateWaiting{Reason: testErrorReason.Error()}},
			},
			expectedLastTerminationState: map[string]v1.ContainerState{
				"failed": {Terminated: &v1.ContainerStateTerminated{
					ExitCode:    1,
					ContainerID: emptyContainerID,
				}},
			},
		},
		{
			desc: "container restart policy rules not match",
			containers: []v1.Container{
				{
					Name:          "failed",
					RestartPolicy: &containerRestartPolicyNever,
					RestartPolicyRules: []v1.ContainerRestartRule{{
						Action: v1.ContainerRestartRuleActionRestart,
						ExitCodes: &v1.ContainerRestartRuleOnExitCodes{
							Operator: v1.ContainerRestartRuleOnExitCodesOpIn,
							Values:   []int32{2},
						},
					}},
				},
			},
			expectedState: map[string]v1.ContainerState{
				"failed": {Terminated: &v1.ContainerStateTerminated{
					ExitCode:    1,
					ContainerID: emptyContainerID,
				}},
			},
			expectedLastTerminationState: map[string]v1.ContainerState{
				"failed": {Terminated: &v1.ContainerStateTerminated{
					ExitCode:    3,
					ContainerID: emptyContainerID,
				}},
			},
		},
		{
			desc: "container restart policy never",
			containers: []v1.Container{
				{
					Name:          "succeed",
					RestartPolicy: &containerRestartPolicyNever,
				},
				{
					Name:          "failed",
					RestartPolicy: &containerRestartPolicyNever,
				},
			},
			expectedState: map[string]v1.ContainerState{
				"succeed": {Terminated: &v1.ContainerStateTerminated{
					ExitCode:    0,
					ContainerID: emptyContainerID,
				}},
				"failed": {Terminated: &v1.ContainerStateTerminated{
					ExitCode:    1,
					ContainerID: emptyContainerID,
				}},
			},
			expectedLastTerminationState: map[string]v1.ContainerState{
				"succeed": {Terminated: &v1.ContainerStateTerminated{
					ExitCode:    2,
					ContainerID: emptyContainerID,
				}},
				"failed": {Terminated: &v1.ContainerStateTerminated{
					ExitCode:    3,
					ContainerID: emptyContainerID,
				}},
			},
		},
		{
			desc: "container restart policy OnFailure",
			containers: []v1.Container{
				{
					Name:          "succeed",
					RestartPolicy: &containerRestartPolicyOnFailure,
				},
				{
					Name:          "failed",
					RestartPolicy: &containerRestartPolicyOnFailure,
				},
			},
			expectedState: map[string]v1.ContainerState{
				"succeed": {Terminated: &v1.ContainerStateTerminated{
					ExitCode:    0,
					ContainerID: emptyContainerID,
				}},
				"failed": {Waiting: &v1.ContainerStateWaiting{Reason: testErrorReason.Error()}},
			},
			expectedLastTerminationState: map[string]v1.ContainerState{
				"succeed": {Terminated: &v1.ContainerStateTerminated{
					ExitCode:    2,
					ContainerID: emptyContainerID,
				}},
				"failed": {Terminated: &v1.ContainerStateTerminated{
					ExitCode:    1,
					ContainerID: emptyContainerID,
				}},
			},
		},
		{
			desc: "container restart policy Always",
			containers: []v1.Container{
				{
					Name:          "succeed",
					RestartPolicy: &containerRestartPolicyAlways,
				},
				{
					Name:          "failed",
					RestartPolicy: &containerRestartPolicyAlways,
				},
			},
			expectedState: map[string]v1.ContainerState{
				"succeed": {Waiting: &v1.ContainerStateWaiting{Reason: testErrorReason.Error()}},
				"failed":  {Waiting: &v1.ContainerStateWaiting{Reason: testErrorReason.Error()}},
			},
			expectedLastTerminationState: map[string]v1.ContainerState{
				"succeed": {Terminated: &v1.ContainerStateTerminated{
					ExitCode:    0,
					ContainerID: emptyContainerID,
				}},
				"failed": {Terminated: &v1.ContainerStateTerminated{
					ExitCode:    1,
					ContainerID: emptyContainerID,
				}},
			},
		},
	} {
		pod.Spec.RestartPolicy = v1.RestartPolicyAlways
		// 先验证普通容器，容器级策略优先于 Pod 级策略。
		pod.Spec.Containers = test.containers
		apiStatus := kubelet.generateAPIPodStatus(tCtx, pod, podStatus, false)
		expectedState, expectedLastTerminationState := test.expectedState, test.expectedLastTerminationState
		verifyContainerStatuses(t, apiStatus.ContainerStatuses, expectedState, expectedLastTerminationState, test.desc)
		pod.Spec.Containers = nil
	}
}

// testPodAdmitHandler 是测试用的 lifecycle.PodAdmitHandler。
type testPodAdmitHandler struct {
	// 需要拒绝的 Pod 列表（按 UID 匹配）。
	podsToReject []*v1.Pod
}

// Admit 拒绝 podsToReject 中 UID 匹配的 Pod，其余一律放行。
func (a *testPodAdmitHandler) Admit(_ context.Context, attrs *lifecycle.PodAdmitAttributes) lifecycle.PodAdmitResult {
	for _, podToReject := range a.podsToReject {
		if podToReject.UID == attrs.Pod.UID {
			return lifecycle.PodAdmitResult{Admit: false, Reason: "Rejected", Message: "Pod is rejected"}
		}
	}
	return lifecycle.PodAdmitResult{Admit: true}
}

// 验证 kubelet 在 HandlePodAdditions 中确实会调用准入处理器。
func TestHandlePodAdditionsInvokesPodAdmitHandlers(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	kl := testKubelet.kubelet
	kl.nodeLister = testNodeLister{nodes: []*v1.Node{
		{
			ObjectMeta: metav1.ObjectMeta{Name: string(kl.nodeName)},
			Status: v1.NodeStatus{
				Allocatable: v1.ResourceList{
					v1.ResourcePods: *resource.NewQuantity(110, resource.DecimalSI),
				},
			},
		},
	}}

	pods := []*v1.Pod{
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:       "123456789",
				Name:      "podA",
				Namespace: "foo",
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				UID:       "987654321",
				Name:      "podB",
				Namespace: "foo",
			},
		},
	}
	podToReject := pods[0]
	podToAdmit := pods[1]
	podsToReject := []*v1.Pod{podToReject}

	kl.allocationManager.AddPodAdmitHandlers(lifecycle.PodAdmitHandlers{&testPodAdmitHandler{podsToReject: podsToReject}})

	kl.HandlePodAdditions(tCtx, pods)

	// Check pod status stored in the status map.
	checkPodStatus(t, kl, podToReject, v1.PodFailed)
	checkPodStatus(t, kl, podToAdmit, v1.PodPending)
}

// 验证 Pod 的资源分配在 kubelet 重启后会被保留，
// 或在没有分配记录时按 Pod 当前的请求重建。
func TestPodResourceAllocationReset(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)

	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodVerticalScaling, true)
	testKubelet := newTestKubelet(t, false)
	defer testKubelet.Cleanup()
	kubelet := testKubelet.kubelet

	// fakePodWorkers 在更新时会同步触发 syncPodFn；这里覆盖它以避免调用
	// kubelet.SyncPod，因为那里还会再执行一次 resize 资源分配。
	kubelet.podWorkers.(*fakePodWorkers).syncPodFn =
		func(_ context.Context, _ kubetypes.SyncPodType, _, _ *v1.Pod, _ *kubecontainer.PodStatus) (bool, func(), error) {
			return false, nil, nil
		}

	nodes := []*v1.Node{
		{
			ObjectMeta: metav1.ObjectMeta{Name: testKubeletHostname},
			Status: v1.NodeStatus{
				Capacity: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("8"),
					v1.ResourceMemory: resource.MustParse("8Gi"),
				},
				Allocatable: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("4"),
					v1.ResourceMemory: resource.MustParse("4Gi"),
					v1.ResourcePods:   *resource.NewQuantity(40, resource.DecimalSI),
				},
			},
		},
	}
	kubelet.nodeLister = testNodeLister{nodes: nodes}

	cpu500m := resource.MustParse("500m")
	cpu800m := resource.MustParse("800m")
	mem500M := resource.MustParse("500Mi")
	mem800M := resource.MustParse("800Mi")
	cpu500mMem500MPodSpec := &v1.PodSpec{
		Containers: []v1.Container{
			{
				Name: "c1",
				Resources: v1.ResourceRequirements{
					Requests: v1.ResourceList{v1.ResourceCPU: cpu500m, v1.ResourceMemory: mem500M},
				},
			},
		},
	}
	cpu800mMem800MPodSpec := cpu500mMem500MPodSpec.DeepCopy()
	cpu800mMem800MPodSpec.Containers[0].Resources.Requests = v1.ResourceList{v1.ResourceCPU: cpu800m, v1.ResourceMemory: mem800M}
	cpu800mPodSpec := cpu500mMem500MPodSpec.DeepCopy()
	cpu800mPodSpec.Containers[0].Resources.Requests = v1.ResourceList{v1.ResourceCPU: cpu800m}
	mem800MPodSpec := cpu500mMem500MPodSpec.DeepCopy()
	mem800MPodSpec.Containers[0].Resources.Requests = v1.ResourceList{v1.ResourceMemory: mem800M}

	cpu500mPodSpec := cpu500mMem500MPodSpec.DeepCopy()
	cpu500mPodSpec.Containers[0].Resources.Requests = v1.ResourceList{v1.ResourceCPU: cpu500m}
	mem500MPodSpec := cpu500mMem500MPodSpec.DeepCopy()
	mem500MPodSpec.Containers[0].Resources.Requests = v1.ResourceList{v1.ResourceMemory: mem500M}
	emptyPodSpec := cpu500mMem500MPodSpec.DeepCopy()
	emptyPodSpec.Containers[0].Resources.Requests = v1.ResourceList{}

	tests := []struct {
		name                       string
		pod                        *v1.Pod
		existingPodAllocation      *v1.Pod
		expectedPodResourceInfoMap state.PodResourceInfoMap
	}{
		{
			name: "Having both memory and cpu, resource allocation not exists",
			pod:  podWithUIDNameNsSpec("1", "pod1", "foo", *cpu500mMem500MPodSpec),
			expectedPodResourceInfoMap: state.PodResourceInfoMap{
				"1": {
					ContainerResources: map[string]v1.ResourceRequirements{
						cpu500mMem500MPodSpec.Containers[0].Name: cpu500mMem500MPodSpec.Containers[0].Resources,
					},
				},
			},
		},
		{
			name:                  "Having both memory and cpu, resource allocation exists",
			pod:                   podWithUIDNameNsSpec("2", "pod2", "foo", *cpu500mMem500MPodSpec),
			existingPodAllocation: podWithUIDNameNsSpec("2", "pod2", "foo", *cpu500mMem500MPodSpec),
			expectedPodResourceInfoMap: state.PodResourceInfoMap{
				"2": {
					ContainerResources: map[string]v1.ResourceRequirements{
						cpu500mMem500MPodSpec.Containers[0].Name: cpu500mMem500MPodSpec.Containers[0].Resources,
					},
				},
			},
		},
		{
			name:                  "Having both memory and cpu, resource allocation exists (with different value)",
			pod:                   podWithUIDNameNsSpec("3", "pod3", "foo", *cpu500mMem500MPodSpec),
			existingPodAllocation: podWithUIDNameNsSpec("3", "pod3", "foo", *cpu800mMem800MPodSpec),
			expectedPodResourceInfoMap: state.PodResourceInfoMap{
				"3": state.PodResourceInfo{
					ContainerResources: map[string]v1.ResourceRequirements{
						cpu800mMem800MPodSpec.Containers[0].Name: cpu800mMem800MPodSpec.Containers[0].Resources,
					},
				},
			},
		},
		{
			name: "Only has cpu, resource allocation not exists",
			pod:  podWithUIDNameNsSpec("4", "pod5", "foo", *cpu500mPodSpec),
			expectedPodResourceInfoMap: state.PodResourceInfoMap{
				"4": state.PodResourceInfo{
					ContainerResources: map[string]v1.ResourceRequirements{
						cpu500mPodSpec.Containers[0].Name: cpu500mPodSpec.Containers[0].Resources,
					},
				},
			},
		},
		{
			name:                  "Only has cpu, resource allocation exists",
			pod:                   podWithUIDNameNsSpec("5", "pod5", "foo", *cpu500mPodSpec),
			existingPodAllocation: podWithUIDNameNsSpec("5", "pod5", "foo", *cpu500mPodSpec),
			expectedPodResourceInfoMap: state.PodResourceInfoMap{
				"5": state.PodResourceInfo{
					ContainerResources: map[string]v1.ResourceRequirements{
						cpu500mPodSpec.Containers[0].Name: cpu500mPodSpec.Containers[0].Resources,
					},
				},
			},
		},
		{
			name:                  "Only has cpu, resource allocation exists (with different value)",
			pod:                   podWithUIDNameNsSpec("6", "pod6", "foo", *cpu500mPodSpec),
			existingPodAllocation: podWithUIDNameNsSpec("6", "pod6", "foo", *cpu800mPodSpec),
			expectedPodResourceInfoMap: state.PodResourceInfoMap{
				"6": state.PodResourceInfo{
					ContainerResources: map[string]v1.ResourceRequirements{
						cpu800mPodSpec.Containers[0].Name: cpu800mPodSpec.Containers[0].Resources,
					},
				},
			},
		},
		{
			name: "Only has memory, resource allocation not exists",
			pod:  podWithUIDNameNsSpec("7", "pod7", "foo", *mem500MPodSpec),
			expectedPodResourceInfoMap: state.PodResourceInfoMap{
				"7": state.PodResourceInfo{
					ContainerResources: map[string]v1.ResourceRequirements{
						mem500MPodSpec.Containers[0].Name: mem500MPodSpec.Containers[0].Resources,
					},
				},
			},
		},
		{
			name:                  "Only has memory, resource allocation exists",
			pod:                   podWithUIDNameNsSpec("8", "pod8", "foo", *mem500MPodSpec),
			existingPodAllocation: podWithUIDNameNsSpec("8", "pod8", "foo", *mem500MPodSpec),
			expectedPodResourceInfoMap: state.PodResourceInfoMap{
				"8": state.PodResourceInfo{
					ContainerResources: map[string]v1.ResourceRequirements{
						mem500MPodSpec.Containers[0].Name: mem500MPodSpec.Containers[0].Resources,
					},
				},
			},
		},
		{
			name:                  "Only has memory, resource allocation exists (with different value)",
			pod:                   podWithUIDNameNsSpec("9", "pod9", "foo", *mem500MPodSpec),
			existingPodAllocation: podWithUIDNameNsSpec("9", "pod9", "foo", *mem800MPodSpec),
			expectedPodResourceInfoMap: state.PodResourceInfoMap{
				"9": state.PodResourceInfo{
					ContainerResources: map[string]v1.ResourceRequirements{
						mem800MPodSpec.Containers[0].Name: mem800MPodSpec.Containers[0].Resources,
					},
				},
			},
		},
		{
			name: "No CPU and memory, resource allocation not exists",
			pod:  podWithUIDNameNsSpec("10", "pod10", "foo", *emptyPodSpec),
			expectedPodResourceInfoMap: state.PodResourceInfoMap{
				"10": state.PodResourceInfo{
					ContainerResources: map[string]v1.ResourceRequirements{
						emptyPodSpec.Containers[0].Name: emptyPodSpec.Containers[0].Resources,
					},
				},
			},
		},
		{
			name:                  "No CPU and memory, resource allocation exists",
			pod:                   podWithUIDNameNsSpec("11", "pod11", "foo", *emptyPodSpec),
			existingPodAllocation: podWithUIDNameNsSpec("11", "pod11", "foo", *emptyPodSpec),
			expectedPodResourceInfoMap: state.PodResourceInfoMap{
				"11": state.PodResourceInfo{
					ContainerResources: map[string]v1.ResourceRequirements{
						emptyPodSpec.Containers[0].Name: emptyPodSpec.Containers[0].Resources,
					},
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.existingPodAllocation != nil {
				// 模拟 kubelet 重启后、Pod 重新加入前分配记录已经存在的情况。
				err := kubelet.allocationManager.SetAllocatedResources(logger, tc.existingPodAllocation)
				if err != nil {
					t.Fatalf("failed to set pod allocation: %v", err)
				}
			}
			kubelet.HandlePodAdditions(tCtx, []*v1.Pod{tc.pod})

			allocatedResources, found := kubelet.allocationManager.GetContainerResourceAllocation(tc.pod.UID, tc.pod.Spec.Containers[0].Name)
			if !found {
				t.Fatalf("resource allocation should exist: (pod: %#v, container: %s)", tc.pod, tc.pod.Spec.Containers[0].Name)
			}
			assert.Equal(t, tc.expectedPodResourceInfoMap[tc.pod.UID].ContainerResources[tc.pod.Spec.Containers[0].Name], allocatedResources, tc.name)
		})
	}
}

// testPodSyncLoopHandler 是测试用的 lifecycle.PodSyncLoopHandler。
type testPodSyncLoopHandler struct {
	// 需要同步的 Pod 列表。
	podsToSync []*v1.Pod
}

// ShouldSync 判断给定 Pod 是否应被同步。
func (a *testPodSyncLoopHandler) ShouldSync(pod *v1.Pod) bool {
	for _, podToSync := range a.podsToSync {
		if podToSync.UID == pod.UID {
			return true
		}
	}
	return false
}

// 验证 getPodsToSync 会调用注册的 PodSyncLoopHandler。
func TestGetPodsToSyncInvokesPodSyncLoopHandlers(t *testing.T) {
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	kubelet := testKubelet.kubelet
	pods := newTestPods(5)
	expected := []*v1.Pod{pods[0]}
	kubelet.AddPodSyncLoopHandler(&testPodSyncLoopHandler{expected})
	kubelet.podManager.SetPods(pods)

	podsToSync := kubelet.getPodsToSync()
	sort.Sort(podsByUID(expected))
	sort.Sort(podsByUID(podsToSync))
	assert.Equal(t, expected, podsToSync)
}

// testPodSyncHandler 是测试用的 lifecycle.PodSyncHandler。
type testPodSyncHandler struct {
	// 需要驱逐的 Pod 列表。
	podsToEvict []*v1.Pod
	// 驱逐时使用的原因。
	reason string
	// 驱逐时使用的消息。
	message string
}

// ShouldEvict 判断给定 Pod 是否应被驱逐，并返回原因与消息。
func (a *testPodSyncHandler) ShouldEvict(pod *v1.Pod) lifecycle.ShouldEvictResponse {
	for _, podToEvict := range a.podsToEvict {
		if podToEvict.UID == pod.UID {
			return lifecycle.ShouldEvictResponse{Evict: true, Reason: a.reason, Message: a.message}
		}
	}
	return lifecycle.ShouldEvictResponse{Evict: false}
}

// 验证 generateAPIPodStatus 会调用 PodSyncHandler 并把驱逐结果写入 Pod 状态。
func TestGenerateAPIPodStatusInvokesPodSyncHandlers(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	kubelet := testKubelet.kubelet
	pod := newTestPods(1)[0]
	podsToEvict := []*v1.Pod{pod}
	kubelet.AddPodSyncHandler(&testPodSyncHandler{podsToEvict, "Evicted", "because"})
	status := &kubecontainer.PodStatus{
		ID:        pod.UID,
		Name:      pod.Name,
		Namespace: pod.Namespace,
	}
	apiStatus := kubelet.generateAPIPodStatus(tCtx, pod, status, false)
	require.Equal(t, v1.PodFailed, apiStatus.Phase)
	require.Equal(t, "Evicted", apiStatus.Reason)
	require.Equal(t, "because", apiStatus.Message)
}

// 验证 SyncTerminatingPod 的 kill 回调写入的 Pod 状态会被保存到状态管理器。
func TestSyncTerminatingPodKillPod(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	kl := testKubelet.kubelet
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "12345678",
			Name:      "bar",
			Namespace: "foo",
		},
	}
	pods := []*v1.Pod{pod}
	kl.podManager.SetPods(pods)
	podStatus := &kubecontainer.PodStatus{ID: pod.UID}
	gracePeriodOverride := int64(0)
	err := kl.SyncTerminatingPod(tCtx, pod, podStatus, &gracePeriodOverride, func(podStatus *v1.PodStatus) {
		podStatus.Phase = v1.PodFailed
		podStatus.Reason = "reason"
		podStatus.Message = "message"
	})
	require.NoError(t, err)

	// Check pod status stored in the status map.
	checkPodStatus(t, kl, pod, v1.PodFailed)
}

// 验证镜像拉取失败且缺失拉取 secret 时，会发出 FailedToRetrieveImagePullSecret 事件。
func TestPullErrorReportsMissingSecrets(t *testing.T) {
	tCtx := ktesting.Init(t)

	expectedEvent := "Warning FailedToRetrieveImagePullSecret Unable to retrieve some image pull secrets (missing); attempting to pull the image may not succeed."

	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	kubelet := testKubelet.kubelet

	// 换成可检查事件的假 recorder。
	fakeRecorder := record.NewFakeRecorder(10)
	kubelet.recorder = fakeRecorder

	// 换成查不到任何 secret 的假管理器，确保拉取 secret 缺失。
	kubelet.secretManager = secret.NewFakeManagerWithSecrets([]*v1.Secret{})

	// 使用能走到镜像拉取步骤的运行时。
	kubelet.dnsConfigurer = createDNSConfigurer()

	kubeCfg := kubeletconfiginternal.KubeletConfiguration{
		MaxPods:                10,
		MemoryThrottlingFactor: new(float64),
	}

	tracerProvider := noopoteltrace.NewTracerProvider()

	fakeRuntime, endpoint := createAndStartFakeRemoteRuntime(t)
	defer func() {
		fakeRuntime.Stop()
	}()
	runtimeSvc, err := remote.NewRemoteRuntimeServiceBuilder().
		WithEndpoint(endpoint).
		WithConnectionTimeout(15 * time.Second).
		WithTracerProvider(tracerProvider).
		Build(tCtx)
	require.NoError(t, err)
	kubelet.runtimeService = runtimeSvc

	fakeRuntime.ImageService.InjectError("PullImage", errors.New("Test pull failed"))
	imageSvc, err := remote.NewRemoteImageServiceBuilder().
		WithEndpoint(endpoint).
		WithConnectionTimeout(15 * time.Second).
		WithTracerProvider(tracerProvider).
		WithUseStreaming(false).
		Build(tCtx)
	require.NoError(t, err)

	kubelet.containerRuntime, _, err = createGenericRuntimeManager(tCtx, kubelet, kubeCfg, runtimeSvc, imageSvc, tracerProvider)
	require.NoError(t, err)

	pod := podWithUIDNameNsSpec("12345678", "foo", "new", v1.PodSpec{
		ImagePullSecrets: []v1.LocalObjectReference{
			{
				Name: "missing",
			},
		},
		Containers: []v1.Container{
			{
				Name:            "bar",
				Image:           "missing:latest",
				ImagePullPolicy: v1.PullAlways,
			},
		},
		EnableServiceLinks: new(false),
	})
	_, _, err = kubelet.SyncPod(tCtx, kubetypes.SyncPodCreate, pod, nil, &kubecontainer.PodStatus{})
	require.ErrorContains(t, err, "ErrImagePull", "Image pull must fail for missing pull secret event to be emitted")

	events := make([]string, 0, 10)
	for done := false; !done; {
		select {
		case event := <-fakeRecorder.Events:
			events = append(events, event)
		default:
			done = true
		}
	}
	assert.Contains(t, events, expectedEvent)
}

// 验证镜像拉取成功时，缺失的拉取 secret 不应被报告为事件。
func TestMissingSecretsNotReportedWithoutPullError(t *testing.T) {
	tCtx := ktesting.Init(t)

	unexpectedEvent := "FailedToRetrieveImagePullSecret"

	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	kubelet := testKubelet.kubelet

	// 换成可检查事件的假 recorder。
	fakeRecorder := record.NewFakeRecorder(10)
	kubelet.recorder = fakeRecorder

	// 换成查不到任何 secret 的假管理器，确保拉取 secret 缺失。
	kubelet.secretManager = secret.NewFakeManagerWithSecrets([]*v1.Secret{})

	// 使用能走到镜像拉取步骤的运行时。
	kubelet.dnsConfigurer = createDNSConfigurer()

	kubeCfg := kubeletconfiginternal.KubeletConfiguration{
		MaxPods:                10,
		MemoryThrottlingFactor: new(float64),
	}

	tracerProvider := noopoteltrace.NewTracerProvider()

	fakeRuntime, endpoint := createAndStartFakeRemoteRuntime(t)
	defer func() {
		fakeRuntime.Stop()
	}()
	runtimeSvc, err := remote.NewRemoteRuntimeServiceBuilder().
		WithEndpoint(endpoint).
		WithConnectionTimeout(15 * time.Second).
		WithTracerProvider(tracerProvider).
		Build(tCtx)
	require.NoError(t, err)
	kubelet.runtimeService = runtimeSvc

	imageSvc, err := remote.NewRemoteImageServiceBuilder().
		WithEndpoint(endpoint).
		WithConnectionTimeout(15 * time.Second).
		WithTracerProvider(tracerProvider).
		WithUseStreaming(false).
		Build(tCtx)
	require.NoError(t, err)

	kubelet.containerRuntime, _, err = createGenericRuntimeManager(tCtx, kubelet, kubeCfg, runtimeSvc, imageSvc, tracerProvider)
	require.NoError(t, err)

	pod := podWithUIDNameNsSpec("12345678", "foo", "new", v1.PodSpec{
		ImagePullSecrets: []v1.LocalObjectReference{
			{
				Name: "missing",
			},
		},
		Containers: []v1.Container{
			{
				Name:            "bar",
				Image:           "missing:latest",
				ImagePullPolicy: v1.PullAlways,
			},
		},
		EnableServiceLinks: new(false),
	})
	_, _, err = kubelet.SyncPod(tCtx, kubetypes.SyncPodCreate, pod, nil, &kubecontainer.PodStatus{})
	if err != nil {
		require.NotContains(t, err.Error(), "ErrImagePull", "Image pull must not fail to check missing pull secret event is not emitted")
	}

	events := make([]string, 0, 10)
	for done := false; !done; {
		select {
		case event := <-fakeRecorder.Events:
			events = append(events, event)
		default:
			done = true
		}
	}
	for _, event := range events {
		assert.NotContainsf(t, event, unexpectedEvent, "%s event must not be fired if the image pull succeeds, found event: %s", unexpectedEvent, event)
	}
}

// 验证 syncNodeStatus 会把 OS/Arch 标签同步到节点，必要时通过 patch 更新。
func TestSyncLabels(t *testing.T) {
	tests := []struct {
		name             string
		existingNode     *v1.Node
		isPatchingNeeded bool
	}{
		{
			name:             "no labels",
			existingNode:     &v1.Node{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{}}},
			isPatchingNeeded: true,
		},
		{
			name:             "wrong labels",
			existingNode:     &v1.Node{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.LabelOSStable: "dummyOS", v1.LabelArchStable: "dummyArch"}}},
			isPatchingNeeded: true,
		},
		{
			name:             "correct labels",
			existingNode:     &v1.Node{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.LabelOSStable: goruntime.GOOS, v1.LabelArchStable: goruntime.GOARCH}}},
			isPatchingNeeded: false,
		},
		{
			name:             "partially correct labels",
			existingNode:     &v1.Node{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{v1.LabelOSStable: goruntime.GOOS, v1.LabelArchStable: "dummyArch"}}},
			isPatchingNeeded: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			testKubelet := newTestKubelet(t, false)
			defer testKubelet.Cleanup()
			kl := testKubelet.kubelet
			kubeClient := testKubelet.fakeKubeClient

			test.existingNode.Name = string(kl.nodeName)

			kl.nodeLister = testNodeLister{nodes: []*v1.Node{test.existingNode}}
			go func() { kl.syncNodeStatus(tCtx) }()

			err := retryWithExponentialBackOff(
				100*time.Millisecond,
				func() (bool, error) {
					var savedNode *v1.Node
					if test.isPatchingNeeded {
						actions := kubeClient.Actions()
						if len(actions) == 0 {
							t.Logf("No action yet")
							return false, nil
						}
						for _, action := range actions {
							if action.GetVerb() == "patch" {
								var (
									err          error
									patchAction  = action.(core.PatchActionImpl)
									patchContent = patchAction.GetPatch()
								)
								savedNode, err = applyNodeStatusPatch(test.existingNode, patchContent)
								if err != nil {
									t.Logf("node patching failed, %v", err)
									return false, nil
								}
							}
						}
					} else {
						savedNode = test.existingNode
					}
					if savedNode == nil || savedNode.Labels == nil {
						t.Logf("savedNode.Labels should not be nil")
						return false, nil
					}
					val, ok := savedNode.Labels[v1.LabelOSStable]
					if !ok {
						t.Logf("expected kubernetes.io/os label to be present")
						return false, nil
					}
					if val != goruntime.GOOS {
						t.Logf("expected kubernetes.io/os to match runtime.GOOS but got %v", val)
						return false, nil
					}
					val, ok = savedNode.Labels[v1.LabelArchStable]
					if !ok {
						t.Logf("expected kubernetes.io/arch label to be present")
						return false, nil
					}
					if val != goruntime.GOARCH {
						t.Logf("expected kubernetes.io/arch to match runtime.GOARCH but got %v", val)
						return false, nil
					}
					return true, nil
				},
			)
			if err != nil {
				t.Fatalf("expected labels to be reconciled but it failed with %v", err)
			}
		})
	}
}

// waitForVolumeUnmount 轮询等待 Pod 的卷全部卸载完成。
func waitForVolumeUnmount(
	volumeManager kubeletvolume.VolumeManager,
	pod *v1.Pod) error {
	err := retryWithExponentialBackOff(
		time.Duration(50*time.Millisecond),
		func() (bool, error) {
			// Verify volumes detached
			hasVolumes := volumeManager.HasPossiblyMountedVolumesForPod(
				util.GetUniquePodName(pod))
			return !hasVolumes, nil
		},
	)

	if err != nil {
		return fmt.Errorf(
			"Expected volumes to be unmounted. But some volumes are still mounted")
	}

	return nil
}

// waitForVolumeDetach 轮询等待指定卷从节点摘除。
func waitForVolumeDetach(
	volumeName v1.UniqueVolumeName,
	volumeManager kubeletvolume.VolumeManager) error {
	attachedVolumes := []v1.UniqueVolumeName{}
	err := retryWithExponentialBackOff(
		time.Duration(50*time.Millisecond),
		func() (bool, error) {
			// Verify volumes detached
			volumeAttached := volumeManager.VolumeIsAttached(volumeName)
			return !volumeAttached, nil
		},
	)

	if err != nil {
		return fmt.Errorf(
			"Expected volumes to be detached. But some volumes are still attached: %#v", attachedVolumes)
	}

	return nil
}

// retryWithExponentialBackOff 以 3 倍递增、最多 6 步的退避重试条件函数。
func retryWithExponentialBackOff(initialDuration time.Duration, fn wait.ConditionFunc) error {
	backoff := wait.Backoff{
		Duration: initialDuration,
		Factor:   3,
		Jitter:   0,
		Steps:    6,
	}
	return wait.ExponentialBackoff(backoff, fn)
}

// simulateVolumeInUseUpdate 周期性把卷标记为 in-use，直到 stopCh 关闭。
func simulateVolumeInUseUpdate(
	volumeName v1.UniqueVolumeName,
	stopCh <-chan struct{},
	volumeManager kubeletvolume.VolumeManager) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			volumeManager.MarkVolumesAsReportedInUse(
				[]v1.UniqueVolumeName{volumeName})
		case <-stopCh:
			return
		}
	}
}

// dirExists 判断路径是否存在且是目录。
func dirExists(path string) bool {
	s, err := os.Stat(path)
	if err != nil {
		return false
	}
	return s.IsDir()
}

// podsByUID 按 UID 对 Pod 排序。
type podsByUID []*v1.Pod

// Len 返回 Pod 数量。
func (p podsByUID) Len() int { return len(p) }

// Swap 交换两个 Pod 的位置。
func (p podsByUID) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

// Less 按 UID 字典序比较，确保排序结果稳定可断言。
func (p podsByUID) Less(i, j int) bool { return p[i].UID < p[j].UID }

// createAndStartFakeRemoteRuntime 创建并启动 fakeremote.RemoteRuntime。
// 成功时返回 RemoteRuntime 与 endpoint；调用方需自行调用 Stop() 清理服务端。
func createAndStartFakeRemoteRuntime(t *testing.T) (*fakeremote.RemoteRuntime, string) {
	endpoint, err := fakeremote.GenerateEndpoint()
	require.NoError(t, err)

	fakeRuntime := fakeremote.NewFakeRemoteRuntime()
	fakeRuntime.Start(endpoint)

	return fakeRuntime, endpoint
}

// 验证 standalone 模式下 NewMainKubelet 可以成功构造：
// 不创建 configmap/secret 管理器，TLS 回调配置符合预期。
func TestNewMainKubeletStandAlone(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	tempDir, err := os.MkdirTemp("", "logs")
	require.NoError(t, err)
	containerLogsDir := ContainerLogsDir
	// 把包级日志目录指向本用例的临时目录，让 GC 路径使用隔离的目录，
	// 并在清理阶段恢复原值。
	ContainerLogsDir = tempDir
	// 用 t.Cleanup 而不是 defer：本用例第二次 ktesting.Init 注册的清理会先取消
	// GC 上下文，然后才执行 RemoveAll。Windows 上如果 GC goroutine 还在
	// os.ReadDir，会一直持有目录句柄，导致 os.RemoveAll 失败。
	t.Cleanup(func() {
		cleanUpTempDir(t, tempDir, containerLogsDir)
	})

	ca, cert, key, err := generateCAAndCertKeyWithOptions(
		"localhost",
		[]net.IP{{127, 0, 0, 1}},
		[]string{"localhost"},
	)
	require.NoError(t, err)

	caPath := filepath.Join(tempDir, "kubelet-client-ca.cert")
	certPath := filepath.Join(tempDir, "kubelet.cert")
	keyPath := filepath.Join(tempDir, "kubelet.key")

	err = os.WriteFile(caPath, ca, os.FileMode(0644))
	require.NoError(t, err)

	err = os.WriteFile(certPath, cert, os.FileMode(0644))
	require.NoError(t, err)

	err = os.WriteFile(keyPath, key, os.FileMode(0600))
	require.NoError(t, err)

	kubeCfg := &kubeletconfiginternal.KubeletConfiguration{
		SyncFrequency: metav1.Duration{Duration: time.Minute},
		ConfigMapAndSecretChangeDetectionStrategy: kubeletconfiginternal.WatchChangeDetectionStrategy,
		ContainerLogMaxSize:                       "10Mi",
		ContainerLogMaxFiles:                      5,
		ImagePullCredentialsVerificationPolicy:    "NeverVerifyPreloadedImages",
		MemoryThrottlingFactor:                    new(float64),
		CrashLoopBackOff: kubeletconfiginternal.CrashLoopBackOffConfig{
			MaxContainerRestartPeriod: &metav1.Duration{Duration: 5 * time.Minute},
		},
	}
	var prober volume.DynamicPluginProber
	tp := noopoteltrace.NewTracerProvider()
	cadvisor := cadvisortest.NewMockInterface(t)
	cadvisor.EXPECT().MachineInfo(logger).Return(&cadvisorapi.MachineInfo{}, nil).Maybe()
	cadvisor.EXPECT().ImagesFsInfo(tCtx).Return(cadvisorapi.FsInfo{
		Usage:     400,
		Capacity:  1000,
		Available: 600,
	}, nil).Maybe()
	tlsOptions := &server.TLSOptions{
		MinVersion:   0,
		CertFile:     certPath,
		KeyFile:      keyPath,
		ClientCAFile: caPath,
	}
	fakeRuntime, endpoint := createAndStartFakeRemoteRuntime(t)
	defer func() {
		fakeRuntime.Stop()
	}()
	fakeRecorder := &record.FakeRecorder{}
	rtSvc, err := remote.NewRemoteRuntimeServiceBuilder().
		WithEndpoint(endpoint).
		WithConnectionTimeout(15 * time.Second).
		Build(tCtx)
	require.NoError(t, err)
	kubeDep := &Dependencies{
		Auth:                 nil,
		CAdvisorInterface:    cadvisor,
		ContainerManager:     cm.NewStubContainerManager(),
		KubeClient:           nil, // standalone mode
		HeartbeatClient:      nil,
		EventClient:          nil,
		TracerProvider:       tp,
		HostUtil:             hostutil.NewFakeHostUtil(nil),
		Mounter:              mount.NewFakeMounter(nil),
		Recorder:             fakeRecorder,
		RemoteRuntimeService: rtSvc,
		RemoteImageService:   fakeRuntime.ImageService,
		Subpather:            &subpath.FakeSubpath{},
		OOMAdjuster:          oom.NewOOMAdjuster(),
		OSInterface:          kubecontainer.RealOS{},
		DynamicPluginProber:  prober,
		TLSOptions:           tlsOptions,
	}
	crOptions := &kubeletconfig.ContainerRuntimeOptions{}

	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ReloadKubeletServerCertificateFile, false)
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.RotateKubeletServerCertificate, false)
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ReloadKubeletClientCAFile, false)

	testMainKubelet, err := NewMainKubelet(
		tCtx,
		kubeCfg,
		kubeDep,
		crOptions,
		"hostname",
		"hostname",
		[]net.IP{},
		"",
		"external",
		"/tmp/cert",
		"/tmp/rootdir",
		tempDir,
		"",
		"",
		false,
		[]v1.Taint{},
		[]string{},
		"",
		false,
		false,
		metav1.Duration{Duration: time.Minute},
		1024,
		110,
		map[string]string{},
		1024,
		false,
	)
	require.NoError(t, err, "NewMainKubelet should succeed")
	require.NotNil(t, testMainKubelet, "testMainKubelet should not be nil")

	testMainKubelet.BirthCry()
	tCtx = ktesting.Init(t)
	testMainKubelet.StartGarbageCollection(tCtx)
	// 如果 configMapManager 非 nil，可以复现空指针 panic。
	// 参见 https://github.com/kubernetes/kubernetes/issues/113492
	// （下面保留的是复现用的 Pod 构造示例。）
	// pod := &v1.Pod{
	// 	ObjectMeta: metav1.ObjectMeta{
	// 		UID:       "12345678",
	// 		Name:      "bar",
	// 		Namespace: "foo",
	// 	},
	// 	Spec: v1.PodSpec{
	// 		Containers: []v1.Container{{
	// 			EnvFrom: []v1.EnvFromSource{{
	// 				ConfigMapRef: &v1.ConfigMapEnvSource{
	// 					LocalObjectReference: v1.LocalObjectReference{Name: "config-map"}}},
	// 			}}},
	// 		Volumes: []v1.Volume{{
	// 			VolumeSource: v1.VolumeSource{
	// 				ConfigMap: &v1.ConfigMapVolumeSource{
	// 					LocalObjectReference: v1.LocalObjectReference{
	// 						Name: "config-map"}}}}},
	// 	},
	// }
	// testMainKubelet.configMapManager.RegisterPod(pod)
	// testMainKubelet.secretManager.RegisterPod(pod)
	require.Nil(t, testMainKubelet.configMapManager, "configmap manager should be nil if kubelet is in standalone mode")
	require.Nil(t, testMainKubelet.secretManager, "secret manager should be nil if kubelet is in standalone mode")
	require.Nil(t, testMainKubelet.serverCertificateManager, "serverCertificateManager should be nil")
	require.NotNil(t, kubeDep.TLSConfig.GetCertificate, "GetCertificate should not be nil")
	require.Nil(t, kubeDep.TLSConfig.GetConfigForClient, "GetConfigForClient should be nil")
	require.NotNil(t, kubeDep.TLSConfig.ClientCAs, "ClientCAs should not be nil")
}

// 验证开启证书与 CA 重新加载时，TLS 配置使用 GetConfigForClient，
// 并且由动态 CA 取代静态 ClientCAs。
func TestNewMainKubeletWithCertAndCAReloadingEnabled(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	tempDir, err := os.MkdirTemp("", "logs")
	require.NoError(t, err)
	containerLogsDir := ContainerLogsDir
	// 把包级日志目录指向本用例的临时目录，让 GC 路径使用隔离的目录，
	// 并在清理阶段恢复原值。
	ContainerLogsDir = tempDir
	// 用 t.Cleanup 而不是 defer：本用例第二次 ktesting.Init 注册的清理会先取消
	// GC 上下文，然后才执行 RemoveAll。Windows 上如果 GC goroutine 还在
	// os.ReadDir，会一直持有目录句柄，导致 os.RemoveAll 失败。
	t.Cleanup(func() {
		cleanUpTempDir(t, tempDir, containerLogsDir)
	})

	ca, cert, key, err := generateCAAndCertKeyWithOptions(
		"localhost",
		[]net.IP{{127, 0, 0, 1}},
		[]string{"localhost"},
	)
	require.NoError(t, err)

	caPath := filepath.Join(tempDir, "kubelet-client-ca.cert")
	certPath := filepath.Join(tempDir, "kubelet.cert")
	keyPath := filepath.Join(tempDir, "kubelet.key")

	err = os.WriteFile(caPath, ca, os.FileMode(0644))
	require.NoError(t, err)

	err = os.WriteFile(certPath, cert, os.FileMode(0644))
	require.NoError(t, err)

	err = os.WriteFile(keyPath, key, os.FileMode(0600))
	require.NoError(t, err)

	kubeCfg := &kubeletconfiginternal.KubeletConfiguration{
		SyncFrequency: metav1.Duration{Duration: time.Minute},
		ConfigMapAndSecretChangeDetectionStrategy: kubeletconfiginternal.WatchChangeDetectionStrategy,
		ContainerLogMaxSize:                       "10Mi",
		ContainerLogMaxFiles:                      5,
		ImagePullCredentialsVerificationPolicy:    "NeverVerifyPreloadedImages",
		MemoryThrottlingFactor:                    new(float64),
		CrashLoopBackOff: kubeletconfiginternal.CrashLoopBackOffConfig{
			MaxContainerRestartPeriod: &metav1.Duration{Duration: 5 * time.Minute},
		},
	}
	var prober volume.DynamicPluginProber
	tp := noopoteltrace.NewTracerProvider()
	cadvisor := cadvisortest.NewMockInterface(t)
	cadvisor.EXPECT().MachineInfo(logger).Return(&cadvisorapi.MachineInfo{}, nil).Maybe()
	cadvisor.EXPECT().ImagesFsInfo(tCtx).Return(cadvisorapi.FsInfo{
		Usage:     400,
		Capacity:  1000,
		Available: 600,
	}, nil).Maybe()
	tlsOptions := &server.TLSOptions{
		MinVersion:   0,
		CertFile:     certPath,
		KeyFile:      keyPath,
		ClientCAFile: caPath,
	}
	fakeRuntime, endpoint := createAndStartFakeRemoteRuntime(t)
	defer func() {
		fakeRuntime.Stop()
	}()
	fakeRecorder := &record.FakeRecorder{}
	rtSvc, err := remote.NewRemoteRuntimeServiceBuilder().
		WithEndpoint(endpoint).
		WithConnectionTimeout(15 * time.Second).
		Build(tCtx)
	require.NoError(t, err)
	kubeDep := &Dependencies{
		Auth:                 nil,
		CAdvisorInterface:    cadvisor,
		ContainerManager:     cm.NewStubContainerManager(),
		KubeClient:           nil, // standalone mode
		HeartbeatClient:      nil,
		EventClient:          nil,
		TracerProvider:       tp,
		HostUtil:             hostutil.NewFakeHostUtil(nil),
		Mounter:              mount.NewFakeMounter(nil),
		Recorder:             fakeRecorder,
		RemoteRuntimeService: rtSvc,
		RemoteImageService:   fakeRuntime.ImageService,
		Subpather:            &subpath.FakeSubpath{},
		OOMAdjuster:          oom.NewOOMAdjuster(),
		OSInterface:          kubecontainer.RealOS{},
		DynamicPluginProber:  prober,
		TLSOptions:           tlsOptions,
	}
	crOptions := &kubeletconfig.ContainerRuntimeOptions{}

	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ReloadKubeletServerCertificateFile, true)
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.RotateKubeletServerCertificate, false)
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ReloadKubeletClientCAFile, true)

	testMainKubelet, err := NewMainKubelet(
		tCtx,
		kubeCfg,
		kubeDep,
		crOptions,
		"hostname",
		"hostname",
		[]net.IP{},
		"",
		"external",
		"/tmp/cert",
		"/tmp/rootdir",
		tempDir,
		"",
		"",
		false,
		[]v1.Taint{},
		[]string{},
		"",
		false,
		false,
		metav1.Duration{Duration: time.Minute},
		1024,
		110,
		map[string]string{},
		1024,
		false,
	)
	require.NoError(t, err, "NewMainKubelet should succeed")
	require.NotNil(t, testMainKubelet, "testMainKubelet should not be nil")

	testMainKubelet.BirthCry()
	ctx := ktesting.Init(t)
	testMainKubelet.StartGarbageCollection(ctx)
	require.Nil(t, testMainKubelet.configMapManager, "configmap manager should be nil if kubelet is in standalone mode")
	require.Nil(t, testMainKubelet.secretManager, "secret manager should be nil if kubelet is in standalone mode")
	require.NotNil(t, testMainKubelet.serverCertificateManager, "serverCertificateManager should not be nil")
	require.NotNil(t, kubeDep.TLSConfig.GetCertificate, "GetCertificate should not be nil")
	require.NotNil(t, kubeDep.TLSConfig.GetConfigForClient, "GetConfigForClient should be nil")
	require.Nil(t, kubeDep.TLSConfig.ClientCAs, "ClientCAs should be nil")
}

// 验证 SyncPod 会生成 syncPod 根 span，且镜像/运行时服务调用都挂在该 trace 下。
func TestSyncPodSpans(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet := newTestKubelet(t, false)
	kubelet := testKubelet.kubelet

	kubelet.dnsConfigurer = createDNSConfigurer()

	kubeCfg := kubeletconfiginternal.KubeletConfiguration{
		SyncFrequency: metav1.Duration{Duration: time.Minute},
		ConfigMapAndSecretChangeDetectionStrategy: kubeletconfiginternal.WatchChangeDetectionStrategy,
		ContainerLogMaxSize:                       "10Mi",
		ContainerLogMaxFiles:                      5,
		MemoryThrottlingFactor:                    new(float64),
		MaxPods:                                   110,
		MaxParallelImagePulls:                     new(int32(5)),
	}

	exp := tracetest.NewInMemoryExporter()
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exp),
	)
	kubelet.tracer = tracerProvider.Tracer(instrumentationScope)

	fakeRuntime, endpoint := createAndStartFakeRemoteRuntime(t)
	defer func() {
		fakeRuntime.Stop()
	}()
	runtimeSvc, err := remote.NewRemoteRuntimeServiceBuilder().
		WithEndpoint(endpoint).
		WithConnectionTimeout(15 * time.Second).
		WithTracerProvider(tracerProvider).
		Build(tCtx)
	require.NoError(t, err)
	kubelet.runtimeService = runtimeSvc

	fakeRuntime.ImageService.SetFakeImageSize(100)
	fakeRuntime.ImageService.SetFakeImages([]string{"test:latest"})
	imageSvc, err := remote.NewRemoteImageServiceBuilder().
		WithEndpoint(endpoint).
		WithConnectionTimeout(15 * time.Second).
		WithTracerProvider(tracerProvider).
		Build(tCtx)
	assert.NoError(t, err)

	kubelet.containerRuntime, _, err = createGenericRuntimeManager(tCtx, kubelet, kubeCfg, runtimeSvc, imageSvc, tracerProvider)
	assert.NoError(t, err)

	pod := podWithUIDNameNsSpec("12345678", "foo", "new", v1.PodSpec{
		Containers: []v1.Container{
			{
				Name:            "bar",
				Image:           "test:latest",
				ImagePullPolicy: v1.PullAlways,
			},
		},
		EnableServiceLinks: new(false),
	})

	_, _, err = kubelet.SyncPod(tCtx, kubetypes.SyncPodCreate, pod, nil, &kubecontainer.PodStatus{})
	require.NoError(t, err)

	assert.NotEmpty(t, exp.GetSpans())

	// 在所有 span 中找出 syncPod 根 span。
	var rootSpan *tracetest.SpanStub
	spans := exp.GetSpans()
	for i, span := range spans {
		if span.Name == "syncPod" {
			rootSpan = &spans[i]
			break
		}
	}
	assert.NotNil(t, rootSpan)

	imageServiceSpans := make([]tracetest.SpanStub, 0)
	runtimeServiceSpans := make([]tracetest.SpanStub, 0)
	for _, span := range exp.GetSpans() {
		if span.SpanContext.TraceID() == rootSpan.SpanContext.TraceID() {
			switch {
			case strings.HasPrefix(span.Name, "runtime.v1.ImageService"):
				imageServiceSpans = append(imageServiceSpans, span)
			case strings.HasPrefix(span.Name, "runtime.v1.RuntimeService"):
				runtimeServiceSpans = append(runtimeServiceSpans, span)
			}
		}
	}
	assert.NotEmpty(t, imageServiceSpans, "syncPod trace should have image service spans")
	assert.NotEmpty(t, runtimeServiceSpans, "syncPod trace should have runtime service spans")

	for _, span := range imageServiceSpans {
		assert.Equalf(t, span.Parent.SpanID(), rootSpan.SpanContext.SpanID(), "image service span %s %s should be child of root span", span.Name, span.Parent.SpanID())
	}

	for _, span := range runtimeServiceSpans {
		assert.Equalf(t, span.Parent.SpanID(), rootSpan.SpanContext.SpanID(), "runtime service span %s %s should be child of root span", span.Name, span.Parent.SpanID())
	}
}

// 验证不同准入拒绝原因会以预期标签记录到 kubelet_admission_rejections_total。
func TestRecordAdmissionRejection(t *testing.T) {
	metrics.Register()

	testCases := []struct {
		name   string
		reason string
		wants  string
	}{
		{
			name:   "AppArmor",
			reason: lifecycle.AppArmorNotAdmittedReason,
			wants: `
				# HELP kubelet_admission_rejections_total [ALPHA] Cumulative number pod admission rejections by the Kubelet.
				# TYPE kubelet_admission_rejections_total counter
				kubelet_admission_rejections_total{reason="AppArmor"} 1
			`,
		},
		{
			name:   "VolumeAttachmentLimitExceeded",
			reason: kubeletvolume.VolumeAttachmentLimitExceededReason,
			wants: `
				# HELP kubelet_admission_rejections_total [ALPHA] Cumulative number pod admission rejections by the Kubelet.
				# TYPE kubelet_admission_rejections_total counter
				kubelet_admission_rejections_total{reason="VolumeAttachmentLimitExceeded"} 1
			`,
		},
		{
			name:   "PodOSSelectorNodeLabelDoesNotMatch",
			reason: lifecycle.PodOSSelectorNodeLabelDoesNotMatch,
			wants: `
                # HELP kubelet_admission_rejections_total [ALPHA] Cumulative number pod admission rejections by the Kubelet.
                # TYPE kubelet_admission_rejections_total counter
                kubelet_admission_rejections_total{reason="PodOSSelectorNodeLabelDoesNotMatch"} 1
            `,
		},
		{
			name:   "PodOSNotSupported",
			reason: lifecycle.PodOSNotSupported,
			wants: `
                # HELP kubelet_admission_rejections_total [ALPHA] Cumulative number pod admission rejections by the Kubelet.
                # TYPE kubelet_admission_rejections_total counter
                kubelet_admission_rejections_total{reason="PodOSNotSupported"} 1
            `,
		},
		{
			name:   "InvalidNodeInfo",
			reason: lifecycle.InvalidNodeInfo,
			wants: `
                # HELP kubelet_admission_rejections_total [ALPHA] Cumulative number pod admission rejections by the Kubelet.
                # TYPE kubelet_admission_rejections_total counter
                kubelet_admission_rejections_total{reason="InvalidNodeInfo"} 1
            `,
		},
		{
			name:   "InitContainerRestartPolicyForbidden",
			reason: lifecycle.InitContainerRestartPolicyForbidden,
			wants: `
                # HELP kubelet_admission_rejections_total [ALPHA] Cumulative number pod admission rejections by the Kubelet.
                # TYPE kubelet_admission_rejections_total counter
                kubelet_admission_rejections_total{reason="InitContainerRestartPolicyForbidden"} 1
            `,
		},
		{
			name:   "SupplementalGroupsPolicyNotSupported",
			reason: lifecycle.SupplementalGroupsPolicyNotSupported,
			wants: `
                # HELP kubelet_admission_rejections_total [ALPHA] Cumulative number pod admission rejections by the Kubelet.
                # TYPE kubelet_admission_rejections_total counter
                kubelet_admission_rejections_total{reason="SupplementalGroupsPolicyNotSupported"} 1
            `,
		},
		{
			name:   "UnexpectedAdmissionError",
			reason: lifecycle.UnexpectedAdmissionError,
			wants: `
                # HELP kubelet_admission_rejections_total [ALPHA] Cumulative number pod admission rejections by the Kubelet.
                # TYPE kubelet_admission_rejections_total counter
                kubelet_admission_rejections_total{reason="UnexpectedAdmissionError"} 1
            `,
		},
		{
			name:   "UnknownReason",
			reason: lifecycle.UnknownReason,
			wants: `
                # HELP kubelet_admission_rejections_total [ALPHA] Cumulative number pod admission rejections by the Kubelet.
                # TYPE kubelet_admission_rejections_total counter
                kubelet_admission_rejections_total{reason="UnknownReason"} 1
            `,
		},
		{
			name:   "UnexpectedPredicateFailureType",
			reason: lifecycle.UnexpectedPredicateFailureType,
			wants: `
                # HELP kubelet_admission_rejections_total [ALPHA] Cumulative number pod admission rejections by the Kubelet.
                # TYPE kubelet_admission_rejections_total counter
                kubelet_admission_rejections_total{reason="UnexpectedPredicateFailureType"} 1
            `,
		},
		{
			name:   "node(s) had taints that the pod didn't tolerate",
			reason: tainttoleration.ErrReasonNotMatch,
			wants: `
                # HELP kubelet_admission_rejections_total [ALPHA] Cumulative number pod admission rejections by the Kubelet.
                # TYPE kubelet_admission_rejections_total counter
                kubelet_admission_rejections_total{reason="node(s) had taints that the pod didn't tolerate"} 1
            `,
		},
		{
			name:   "Evicted",
			reason: eviction.Reason,
			wants: `
                # HELP kubelet_admission_rejections_total [ALPHA] Cumulative number pod admission rejections by the Kubelet.
                # TYPE kubelet_admission_rejections_total counter
                kubelet_admission_rejections_total{reason="Evicted"} 1
            `,
		},
		{
			name:   "SysctlForbidden",
			reason: sysctl.ForbiddenReason,
			wants: `
                # HELP kubelet_admission_rejections_total [ALPHA] Cumulative number pod admission rejections by the Kubelet.
                # TYPE kubelet_admission_rejections_total counter
                kubelet_admission_rejections_total{reason="SysctlForbidden"} 1
            `,
		},
		{
			name:   "TopologyAffinityError",
			reason: topologymanager.ErrorTopologyAffinity,
			wants: `
                # HELP kubelet_admission_rejections_total [ALPHA] Cumulative number pod admission rejections by the Kubelet.
                # TYPE kubelet_admission_rejections_total counter
                kubelet_admission_rejections_total{reason="TopologyAffinityError"} 1
            `,
		},
		{
			name:   "NodeShutdown",
			reason: nodeshutdown.NodeShutdownNotAdmittedReason,
			wants: `
                # HELP kubelet_admission_rejections_total [ALPHA] Cumulative number pod admission rejections by the Kubelet.
                # TYPE kubelet_admission_rejections_total counter
                kubelet_admission_rejections_total{reason="NodeShutdown"} 1
            `,
		},
		{
			name:   "OutOfcpu",
			reason: "OutOfcpu",
			wants: `
                # HELP kubelet_admission_rejections_total [ALPHA] Cumulative number pod admission rejections by the Kubelet.
                # TYPE kubelet_admission_rejections_total counter
                kubelet_admission_rejections_total{reason="OutOfcpu"} 1
            `,
		},
		{
			name:   "OutOfmemory",
			reason: "OutOfmemory",
			wants: `
                # HELP kubelet_admission_rejections_total [ALPHA] Cumulative number pod admission rejections by the Kubelet.
                # TYPE kubelet_admission_rejections_total counter
                kubelet_admission_rejections_total{reason="OutOfmemory"} 1
            `,
		},
		{
			name:   "OutOfephemeral-storage",
			reason: "OutOfephemeral-storage",
			wants: `
                # HELP kubelet_admission_rejections_total [ALPHA] Cumulative number pod admission rejections by the Kubelet.
                # TYPE kubelet_admission_rejections_total counter
                kubelet_admission_rejections_total{reason="OutOfephemeral-storage"} 1
            `,
		},
		{
			name:   "OutOfpods",
			reason: "OutOfpods",
			wants: `
                # HELP kubelet_admission_rejections_total [ALPHA] Cumulative number pod admission rejections by the Kubelet.
                # TYPE kubelet_admission_rejections_total counter
                kubelet_admission_rejections_total{reason="OutOfpods"} 1
            `,
		},
		{
			name:   "OutOfgpu",
			reason: "OutOfgpu",
			wants: `
                # HELP kubelet_admission_rejections_total [ALPHA] Cumulative number pod admission rejections by the Kubelet.
                # TYPE kubelet_admission_rejections_total counter
                kubelet_admission_rejections_total{reason="OutOfExtendedResources"} 1
            `,
		},
		{
			name:   "PodLevelResources",
			reason: lifecycle.PodLevelResourcesNotAdmittedReason,
			wants: `
				# HELP kubelet_admission_rejections_total [ALPHA] Cumulative number pod admission rejections by the Kubelet.
				# TYPE kubelet_admission_rejections_total counter
				kubelet_admission_rejections_total{reason="PodLevelResourcesNotSupported"} 1
			`,
		},
		{
			name:   "OtherReason",
			reason: "OtherReason",
			wants: `
                # HELP kubelet_admission_rejections_total [ALPHA] Cumulative number pod admission rejections by the Kubelet.
                # TYPE kubelet_admission_rejections_total counter
                kubelet_admission_rejections_total{reason="Other"} 1
            `,
		},
	}

	// 逐个用例执行。
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Clear the metrics after the test.
			metrics.AdmissionRejectionsTotal.Reset()

			// Call the function.
			recordAdmissionRejection(tc.reason)

			if err := testutil.GatherAndCompare(metrics.GetGather(), strings.NewReader(tc.wants), "kubelet_admission_rejections_total"); err != nil {
				t.Error(err)
			}
		})
	}
}

// 验证 CrashLoopBackOff 的初始与最大退避时间随节点配置和特性开关变化。
func TestCrashLoopBackOffConfiguration(t *testing.T) {
	testCases := []struct {
		name            string
		featureGates    []featuregate.Feature
		configuredMax   metav1.Duration
		expectedInitial time.Duration
		expectedMax     time.Duration
	}{
		{
			name:            "Faster per node config; only node config configured",
			configuredMax:   metav1.Duration{Duration: 2 * time.Second},
			expectedMax:     time.Duration(2 * time.Second),
			expectedInitial: time.Duration(2 * time.Second),
		},
		{
			name:            "Faster per node config; new default and node config configured",
			featureGates:    []featuregate.Feature{features.ReduceDefaultCrashLoopBackOffDecay},
			configuredMax:   metav1.Duration{Duration: 2 * time.Second},
			expectedMax:     time.Duration(2 * time.Second),
			expectedInitial: time.Duration(1 * time.Second),
		},
		{
			name:            "Slower per node config; new default and node config configured, set A",
			featureGates:    []featuregate.Feature{features.ReduceDefaultCrashLoopBackOffDecay},
			configuredMax:   metav1.Duration{Duration: 10 * time.Second},
			expectedMax:     time.Duration(10 * time.Second),
			expectedInitial: time.Duration(1 * time.Second),
		},
		{
			name:            "Slower per node config; new default and node config configured, set B",
			featureGates:    []featuregate.Feature{features.ReduceDefaultCrashLoopBackOffDecay},
			configuredMax:   metav1.Duration{Duration: 300 * time.Second},
			expectedMax:     time.Duration(300 * time.Second),
			expectedInitial: time.Duration(1 * time.Second),
		},
		{
			name:            "Slower per node config; only node config configured, set A",
			configuredMax:   metav1.Duration{Duration: 11 * time.Second},
			expectedMax:     time.Duration(11 * time.Second),
			expectedInitial: time.Duration(10 * time.Second),
		},
		{
			name:            "Slower per node config; only node config configured, set B",
			configuredMax:   metav1.Duration{Duration: 300 * time.Second},
			expectedMax:     time.Duration(300 * time.Second),
			expectedInitial: time.Duration(10 * time.Second),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			kubeCfg := &kubeletconfiginternal.KubeletConfiguration{}

			for _, f := range tc.featureGates {
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, f, true)
			}
			if tc.configuredMax.Duration > 0 {
				kubeCfg.CrashLoopBackOff.MaxContainerRestartPeriod = &tc.configuredMax
			}

			resultMax, resultInitial := newCrashLoopBackOff(kubeCfg)

			assert.Equalf(t, tc.expectedMax, resultMax, "wrong max calculated, want: %v, got %v", tc.expectedMax, resultMax)
			assert.Equalf(t, tc.expectedInitial, resultInitial, "wrong base calculated, want: %v, got %v", tc.expectedInitial, resultInitial)
		})
	}
}

// 验证原地扩容相关的同步错误：错误被返回、Pod 条件被记录，
// 成功的扩容动作则会清除已有的错误条件。
func TestSyncPodWithErrorsDuringInPlacePodResize(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	defer testKubelet.Cleanup()
	kubelet := testKubelet.kubelet
	kubelet.recorder = record.NewFakeRecorder(20)

	pod := podWithUIDNameNsSpec("12345678", "foo", "new", v1.PodSpec{
		Containers: []v1.Container{
			{Name: "bar"},
		},
	})
	pod.Generation = 2
	kubelet.podManager.SetPods([]*v1.Pod{pod})
	kubelet.statusManager.SetPodResizeInProgressCondition(pod.UID, "", "", 1)

	testCases := []struct {
		name                     string
		syncResults              *kubecontainer.PodSyncResult
		podResizeInProgress      bool
		expectedErr              string
		expectedResizeConditions []*v1.PodCondition
		expectedEvent            string
	}{
		{
			name: "pod resize error returned from the runtime",
			syncResults: &kubecontainer.PodSyncResult{
				SyncResults: []*kubecontainer.SyncResult{{
					Action:  kubecontainer.ResizePodInPlace,
					Target:  pod.UID,
					Error:   kubecontainer.ErrResizePodInPlace,
					Message: "could not resize pod",
				}},
			},
			podResizeInProgress: true,
			expectedErr:         "failed to \"ResizePodInPlace\" for \"12345678\" with ResizePodInPlaceError: \"could not resize pod\"",
			expectedResizeConditions: []*v1.PodCondition{{
				Type:               v1.PodResizeInProgress,
				Status:             v1.ConditionTrue,
				Reason:             v1.PodReasonError,
				Message:            "could not resize pod",
				ObservedGeneration: 1,
			}},
			expectedEvent: "Warning ResizeError Pod resize error: {\"containers\":[{\"name\":\"bar\",\"resources\":{}}],\"generation\":1,\"error\":\"could not resize pod\"}",
		},
		{
			name: "pod resize error cleared upon successful run",
			syncResults: &kubecontainer.PodSyncResult{
				SyncResults: []*kubecontainer.SyncResult{{
					Action: kubecontainer.ResizePodInPlace,
					Target: pod.UID,
				}},
			},
			expectedResizeConditions: nil,
			expectedEvent:            "Normal ResizeCompleted Pod resize completed: {\"containers\":[{\"name\":\"bar\",\"resources\":{}}],\"generation\":1}",
		},
		{
			name: "sync results have a non-resize error",
			syncResults: &kubecontainer.PodSyncResult{
				SyncResults: []*kubecontainer.SyncResult{{
					Action:  kubecontainer.CreatePodSandbox,
					Target:  pod.UID,
					Error:   kubecontainer.ErrCreatePodSandbox,
					Message: "could not create pod sandbox",
				}},
			},
			expectedErr:              "failed to \"CreatePodSandbox\" for \"12345678\" with CreatePodSandboxError: \"could not create pod sandbox\"",
			expectedResizeConditions: nil,
		},
		{
			name: "sync results have a non-resize error and a successful pod resize action",
			syncResults: &kubecontainer.PodSyncResult{
				SyncResults: []*kubecontainer.SyncResult{
					{
						Action:  kubecontainer.CreatePodSandbox,
						Target:  pod.UID,
						Error:   kubecontainer.ErrCreatePodSandbox,
						Message: "could not create pod sandbox",
					},
					{
						Action: kubecontainer.ResizePodInPlace,
						Target: pod.UID,
					},
				},
			},
			expectedErr:              "failed to \"CreatePodSandbox\" for \"12345678\" with CreatePodSandboxError: \"could not create pod sandbox\"",
			expectedResizeConditions: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			testKubelet.fakeRuntime.SyncResults = tc.syncResults
			testKubelet.fakeRuntime.PodResizeInProgress = tc.podResizeInProgress
			isTerminal, _, err := kubelet.SyncPod(tCtx, kubetypes.SyncPodUpdate, pod, nil, &kubecontainer.PodStatus{})
			require.False(t, isTerminal)
			if tc.expectedErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Equal(t, tc.expectedErr, err.Error())
			}
			gotResizeConditions := kubelet.statusManager.GetPodResizeConditions(pod.UID)
			for _, c := range gotResizeConditions {
				// ignore last probe and transition times for comparison
				c.LastProbeTime = metav1.Time{}
				c.LastTransitionTime = metav1.Time{}
			}
			require.Equal(t, tc.expectedResizeConditions, gotResizeConditions)

			fakeRecorder := kubelet.recorder.(*record.FakeRecorder)
			found := false
			for len(fakeRecorder.Events) > 0 {
				event := <-fakeRecorder.Events
				if event == tc.expectedEvent {
					found = true
				} else if strings.Contains(event, "Resize") {
					t.Errorf("Received unexpected resize event: %q", event)
				}
			}
			if tc.expectedEvent != "" {
				require.True(t, found, "Expected event %q not found", tc.expectedEvent)
			}
		})
	}
}

// 验证扩容请求会按资源、请求/限制、增删升降等维度计入 requested resizes 指标。
func TestHandlePodUpdates_RecordContainerRequestedResizes(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	metrics.Register()
	metrics.ContainerRequestedResizes.Reset()

	type expectedMetricsStruct struct {
		memoryLimitsCounter   map[string]int
		memoryRequestsCounter map[string]int
		cpuLimitsCounter      map[string]int
		cpuRequestsCounter    map[string]int
	}
	expectedMetrics := expectedMetricsStruct{
		memoryLimitsCounter:   make(map[string]int),
		memoryRequestsCounter: make(map[string]int),
		cpuLimitsCounter:      make(map[string]int),
		cpuRequestsCounter:    make(map[string]int),
	}

	for _, tc := range []struct {
		name               string
		initialAllocation  *v1.Pod
		updatedPod         *v1.Pod
		updateExpectedFunc func(*expectedMetricsStruct)
	}{
		// Memory requests
		{
			name: "add memory requests",
			initialAllocation: &v1.Pod{
				Spec: v1.PodSpec{Containers: []v1.Container{{}}},
			},
			updatedPod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("90"),
							},
						},
					}},
				},
			},
			updateExpectedFunc: func(e *expectedMetricsStruct) {
				e.memoryRequestsCounter["add"]++
			},
		},
		{
			name: "remove memory requests",
			initialAllocation: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("90"),
							},
						},
					}},
				},
			},
			updatedPod: &v1.Pod{
				Spec: v1.PodSpec{Containers: []v1.Container{{}}},
			},
			updateExpectedFunc: func(e *expectedMetricsStruct) {
				e.memoryRequestsCounter["remove"]++
			},
		},
		{
			name: "increase memory requests",
			initialAllocation: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("90"),
							},
						}},
					},
				},
			},
			updatedPod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("110"),
							},
						},
					}},
				},
			},
			updateExpectedFunc: func(e *expectedMetricsStruct) {
				e.memoryRequestsCounter["increase"]++
			},
		},
		{
			name: "decrease memory requests",
			initialAllocation: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("110"),
							},
						},
					}},
				},
			},
			updatedPod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("90"),
							},
						},
					}},
				},
			},
			updateExpectedFunc: func(e *expectedMetricsStruct) {
				e.memoryRequestsCounter["decrease"]++
			},
		},

		// Memory limits
		{
			name: "add memory limits",
			initialAllocation: &v1.Pod{
				Spec: v1.PodSpec{Containers: []v1.Container{{}}},
			},
			updatedPod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Resources: v1.ResourceRequirements{
							Limits: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("90"),
							},
						},
					}},
				},
			},
			updateExpectedFunc: func(e *expectedMetricsStruct) {
				e.memoryLimitsCounter["add"]++
			},
		},
		{
			name: "remove memory limits",
			initialAllocation: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Resources: v1.ResourceRequirements{
							Limits: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("90"),
							},
						},
					}},
				},
			},
			updatedPod: &v1.Pod{
				Spec: v1.PodSpec{Containers: []v1.Container{{}}},
			},
			updateExpectedFunc: func(e *expectedMetricsStruct) {
				e.memoryLimitsCounter["remove"]++
			},
		},
		{
			name: "increase memory limits",
			initialAllocation: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Resources: v1.ResourceRequirements{
							Limits: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("90"),
							},
						},
					}},
				},
			},
			updatedPod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Resources: v1.ResourceRequirements{
							Limits: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("110"),
							},
						},
					}},
				},
			},
			updateExpectedFunc: func(e *expectedMetricsStruct) {
				e.memoryLimitsCounter["increase"]++
			},
		},
		{
			name: "decrease memory limits",
			initialAllocation: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Resources: v1.ResourceRequirements{
							Limits: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("110"),
							},
						},
					}},
				},
			},
			updatedPod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Resources: v1.ResourceRequirements{
							Limits: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("90"),
							},
						},
					}},
				},
			},
			updateExpectedFunc: func(e *expectedMetricsStruct) {
				e.memoryLimitsCounter["decrease"]++
			},
		},

		// CPU requests
		{
			name: "add cpu requests",
			initialAllocation: &v1.Pod{
				Spec: v1.PodSpec{Containers: []v1.Container{{}}},
			},
			updatedPod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceCPU: resource.MustParse("90"),
							},
						},
					}},
				},
			},
			updateExpectedFunc: func(e *expectedMetricsStruct) {
				e.cpuRequestsCounter["add"]++
			},
		},
		{
			name: "remove cpu requests",
			initialAllocation: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceCPU: resource.MustParse("90"),
							},
						},
					}},
				},
			},
			updatedPod: &v1.Pod{
				Spec: v1.PodSpec{Containers: []v1.Container{{}}},
			},
			updateExpectedFunc: func(e *expectedMetricsStruct) {
				e.cpuRequestsCounter["remove"]++
			},
		},
		{
			name: "increase cpu requests",
			initialAllocation: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceCPU: resource.MustParse("90"),
							},
						},
					}},
				},
			},
			updatedPod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceCPU: resource.MustParse("110"),
							},
						},
					}},
				},
			},
			updateExpectedFunc: func(e *expectedMetricsStruct) {
				e.cpuRequestsCounter["increase"]++
			},
		},
		{
			name: "decrease cpu requests",
			initialAllocation: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceCPU: resource.MustParse("110"),
							},
						},
					}},
				},
			},
			updatedPod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceCPU: resource.MustParse("90"),
							},
						},
					}},
				},
			},
			updateExpectedFunc: func(e *expectedMetricsStruct) {
				e.cpuRequestsCounter["decrease"]++
			},
		},

		// CPU limits
		{
			name: "add cpu limits",
			initialAllocation: &v1.Pod{
				Spec: v1.PodSpec{Containers: []v1.Container{{}}},
			},
			updatedPod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Resources: v1.ResourceRequirements{
							Limits: v1.ResourceList{
								v1.ResourceCPU: resource.MustParse("90"),
							},
						},
					}},
				},
			},
			updateExpectedFunc: func(e *expectedMetricsStruct) {
				e.cpuLimitsCounter["add"]++
			},
		},
		{
			name: "remove cpu limits",
			initialAllocation: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Resources: v1.ResourceRequirements{
							Limits: v1.ResourceList{
								v1.ResourceCPU: resource.MustParse("90"),
							},
						},
					}},
				},
			},
			updatedPod: &v1.Pod{
				Spec: v1.PodSpec{Containers: []v1.Container{{}}},
			},
			updateExpectedFunc: func(e *expectedMetricsStruct) {
				e.cpuLimitsCounter["remove"]++
			},
		},
		{
			name: "increase cpu limits",
			initialAllocation: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Resources: v1.ResourceRequirements{
							Limits: v1.ResourceList{
								v1.ResourceCPU: resource.MustParse("90"),
							},
						},
					}},
				},
			},
			updatedPod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Resources: v1.ResourceRequirements{
							Limits: v1.ResourceList{
								v1.ResourceCPU: resource.MustParse("110"),
							},
						},
					}},
				},
			},
			updateExpectedFunc: func(e *expectedMetricsStruct) {
				e.cpuLimitsCounter["increase"]++
			},
		},
		{
			name: "decrease cpu limits",
			initialAllocation: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Resources: v1.ResourceRequirements{
							Limits: v1.ResourceList{
								v1.ResourceCPU: resource.MustParse("110"),
							},
						},
					}},
				},
			},
			updatedPod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Resources: v1.ResourceRequirements{
							Limits: v1.ResourceList{
								v1.ResourceCPU: resource.MustParse("90"),
							},
						},
					}},
				},
			},
			updateExpectedFunc: func(e *expectedMetricsStruct) {
				e.cpuLimitsCounter["decrease"]++
			},
		},

		// Some combinations of things
		{
			name: "add cpu limits + increase memory requests",
			initialAllocation: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("90"),
							},
						},
					}},
				},
			},
			updatedPod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Resources: v1.ResourceRequirements{
							Limits: v1.ResourceList{
								v1.ResourceCPU: resource.MustParse("110"),
							},
							Requests: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("110"),
							},
						},
					}},
				},
			},
			updateExpectedFunc: func(e *expectedMetricsStruct) {
				e.cpuLimitsCounter["add"]++
				e.memoryRequestsCounter["increase"]++
			},
		},
		{
			name: "remove memory limits + decrease cpu requests",
			initialAllocation: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Resources: v1.ResourceRequirements{
							Limits: v1.ResourceList{
								v1.ResourceMemory: resource.MustParse("90"),
							},
							Requests: v1.ResourceList{
								v1.ResourceCPU: resource.MustParse("110"),
							},
						},
					}},
				},
			},
			updatedPod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceCPU: resource.MustParse("90"),
							},
						},
					}},
				},
			},
			updateExpectedFunc: func(e *expectedMetricsStruct) {
				e.memoryLimitsCounter["remove"]++
				e.cpuRequestsCounter["decrease"]++
			},
		},
		{
			name: "increase cpu requests + memory requests",
			initialAllocation: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("90"),
								v1.ResourceMemory: resource.MustParse("90"),
							},
						},
					}},
				},
			},
			updatedPod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("110"),
								v1.ResourceMemory: resource.MustParse("110"),
							},
						},
					}},
				},
			},
			updateExpectedFunc: func(e *expectedMetricsStruct) {
				e.cpuRequestsCounter["increase"]++
				e.memoryRequestsCounter["increase"]++
			},
		},
		{
			name: "decrease all possible values",
			initialAllocation: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("110"),
								v1.ResourceMemory: resource.MustParse("110"),
							},
							Limits: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("110"),
								v1.ResourceMemory: resource.MustParse("110"),
							},
						},
					}},
				},
			},
			updatedPod: &v1.Pod{
				Spec: v1.PodSpec{
					Containers: []v1.Container{{
						Resources: v1.ResourceRequirements{
							Requests: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("90"),
								v1.ResourceMemory: resource.MustParse("90"),
							},
							Limits: v1.ResourceList{
								v1.ResourceCPU:    resource.MustParse("90"),
								v1.ResourceMemory: resource.MustParse("90"),
							},
						},
					}},
				},
			},
			updateExpectedFunc: func(e *expectedMetricsStruct) {
				e.cpuLimitsCounter["decrease"]++
				e.cpuRequestsCounter["decrease"]++
				e.memoryLimitsCounter["decrease"]++
				e.memoryRequestsCounter["decrease"]++
			},
		},
		{
			name: "no resize request",
			initialAllocation: &v1.Pod{
				Spec: v1.PodSpec{Containers: []v1.Container{{}}},
			},
			updatedPod: &v1.Pod{
				Spec: v1.PodSpec{Containers: []v1.Container{{}}},
			},
			updateExpectedFunc: func(e *expectedMetricsStruct) {},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testPod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-pod",
					UID:  "12345",
				},
			}
			initialPod := testPod.DeepCopy()
			updatedPod := testPod.DeepCopy()

			initialPod.Spec = tc.initialAllocation.Spec
			updatedPod.Spec = tc.updatedPod.Spec

			testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
			t.Cleanup(func() { testKubelet.Cleanup() })
			kubelet := testKubelet.kubelet

			kubelet.podManager.AddPod(initialPod)
			require.NoError(t, kubelet.allocationManager.SetAllocatedResources(logger, initialPod))
			kubelet.HandlePodUpdates(tCtx, []*v1.Pod{updatedPod})

			tc.updateExpectedFunc(&expectedMetrics)

			expectedFormat := `
				# HELP kubelet_container_requested_resizes_total [ALPHA] Number of requested resizes, counted at the container level. Different resources on the same container are counted separately. The 'requirement' label refers to 'memory' or 'limits'; the 'operation' label can be one of 'add', 'remove', 'increase' or 'decrease'.
				# TYPE kubelet_container_requested_resizes_total counter
				kubelet_container_requested_resizes_total{operation="add",requirement="requests",resource="memory"} %d
				kubelet_container_requested_resizes_total{operation="add",requirement="requests",resource="cpu"} %d
				kubelet_container_requested_resizes_total{operation="add",requirement="limits",resource="memory"} %d
				kubelet_container_requested_resizes_total{operation="add",requirement="limits",resource="cpu"} %d
				kubelet_container_requested_resizes_total{operation="decrease",requirement="requests",resource="memory"} %d
				kubelet_container_requested_resizes_total{operation="decrease",requirement="requests",resource="cpu"} %d
				kubelet_container_requested_resizes_total{operation="decrease",requirement="limits",resource="memory"} %d
				kubelet_container_requested_resizes_total{operation="decrease",requirement="limits",resource="cpu"} %d
				kubelet_container_requested_resizes_total{operation="increase",requirement="requests",resource="memory"} %d
				kubelet_container_requested_resizes_total{operation="increase",requirement="requests",resource="cpu"} %d
				kubelet_container_requested_resizes_total{operation="increase",requirement="limits",resource="memory"} %d
				kubelet_container_requested_resizes_total{operation="increase",requirement="limits",resource="cpu"} %d
				kubelet_container_requested_resizes_total{operation="remove",requirement="requests",resource="memory"} %d
				kubelet_container_requested_resizes_total{operation="remove",requirement="requests",resource="cpu"} %d
				kubelet_container_requested_resizes_total{operation="remove",requirement="limits",resource="memory"} %d
				kubelet_container_requested_resizes_total{operation="remove",requirement="limits",resource="cpu"} %d
			`

			expected := fmt.Sprintf(expectedFormat,
				expectedMetrics.memoryRequestsCounter["add"],
				expectedMetrics.cpuRequestsCounter["add"],
				expectedMetrics.memoryLimitsCounter["add"],
				expectedMetrics.cpuLimitsCounter["add"],

				expectedMetrics.memoryRequestsCounter["decrease"],
				expectedMetrics.cpuRequestsCounter["decrease"],
				expectedMetrics.memoryLimitsCounter["decrease"],
				expectedMetrics.cpuLimitsCounter["decrease"],

				expectedMetrics.memoryRequestsCounter["increase"],
				expectedMetrics.cpuRequestsCounter["increase"],
				expectedMetrics.memoryLimitsCounter["increase"],
				expectedMetrics.cpuLimitsCounter["increase"],

				expectedMetrics.memoryRequestsCounter["remove"],
				expectedMetrics.cpuRequestsCounter["remove"],
				expectedMetrics.memoryLimitsCounter["remove"],
				expectedMetrics.cpuLimitsCounter["remove"],
			)

			// 去掉计数为 0 的指标行，避免期望值里出现无意义条目。
			re := regexp.MustCompile("(?m)[\r\n]+^.*} 0.*$")
			expected = re.ReplaceAllString(expected, "")

			require.NoError(t, testutil.GatherAndCompare(
				legacyregistry.DefaultGatherer, strings.NewReader(expected), "kubelet_container_requested_resizes_total",
			))
		})
	}
}

// 验证内存介质 emptyDir 的尺寸变化是否触发卷 resize，取决于对应特性开关。
func TestHandlePodUpdates_VolumeResize(t *testing.T) {
	if goruntime.GOOS == "windows" {
		t.Skip("InPlacePodVerticalScaling is not currently supported for Windows")
	}

	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodVerticalScaling, true)
	tCtx := ktesting.Init(t)

	logger := klog.FromContext(tCtx)

	quantity100Mi := resource.MustParse("100Mi")
	quantity200Mi := resource.MustParse("200Mi")

	tests := []struct {
		name                         string
		enableMemoryVolumeResizeGate bool
		initialVolumes               []v1.Volume
		resizedVolumes               []v1.Volume
		expectResizeAction           bool
	}{
		{
			name:                         "feature gate disabled: memory volume size limit changed",
			enableMemoryVolumeResizeGate: false,
			initialVolumes: []v1.Volume{
				{
					Name: "vol-1",
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{
							Medium:    v1.StorageMediumMemory,
							SizeLimit: &quantity100Mi,
						},
					},
				},
			},
			resizedVolumes: []v1.Volume{
				{
					Name: "vol-1",
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{
							Medium:    v1.StorageMediumMemory,
							SizeLimit: &quantity200Mi,
						},
					},
				},
			},
			expectResizeAction: false,
		},
		{
			name:                         "feature gate enabled: memory volume size limit increased",
			enableMemoryVolumeResizeGate: true,
			initialVolumes: []v1.Volume{
				{
					Name: "vol-1",
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{
							Medium:    v1.StorageMediumMemory,
							SizeLimit: &quantity100Mi,
						},
					},
				},
			},
			resizedVolumes: []v1.Volume{
				{
					Name: "vol-1",
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{
							Medium:    v1.StorageMediumMemory,
							SizeLimit: &quantity200Mi,
						},
					},
				},
			},
			expectResizeAction: true,
		},
		{
			name:                         "feature gate enabled: memory volume size limit decreased",
			enableMemoryVolumeResizeGate: true,
			initialVolumes: []v1.Volume{
				{
					Name: "vol-1",
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{
							Medium:    v1.StorageMediumMemory,
							SizeLimit: &quantity200Mi,
						},
					},
				},
			},
			resizedVolumes: []v1.Volume{
				{
					Name: "vol-1",
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{
							Medium:    v1.StorageMediumMemory,
							SizeLimit: &quantity100Mi,
						},
					},
				},
			},
			expectResizeAction: true,
		},
		{
			name:                         "feature gate enabled: memory volume size limit unchanged",
			enableMemoryVolumeResizeGate: true,
			initialVolumes: []v1.Volume{
				{
					Name: "vol-1",
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{
							Medium:    v1.StorageMediumMemory,
							SizeLimit: &quantity100Mi,
						},
					},
				},
			},
			resizedVolumes: []v1.Volume{
				{
					Name: "vol-1",
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{
							Medium:    v1.StorageMediumMemory,
							SizeLimit: &quantity100Mi,
						},
					},
				},
			},
			expectResizeAction: false,
		},
		{
			name:                         "feature gate enabled: non-memory backed emptyDir volume limit changed",
			enableMemoryVolumeResizeGate: true,
			initialVolumes: []v1.Volume{
				{
					Name: "vol-1",
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{
							Medium:    v1.StorageMediumDefault,
							SizeLimit: &quantity100Mi,
						},
					},
				},
			},
			resizedVolumes: []v1.Volume{
				{
					Name: "vol-1",
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{
							Medium:    v1.StorageMediumDefault,
							SizeLimit: &quantity200Mi,
						},
					},
				},
			},
			expectResizeAction: false,
		},
		{
			name:                         "feature gate enabled: pod with no volumes",
			enableMemoryVolumeResizeGate: true,
			initialVolumes:               nil,
			resizedVolumes:               nil,
			expectResizeAction:           false,
		},
		{
			name:                         "feature gate enabled: disk-backed emptyDir volume limit changed (implicit default medium)",
			enableMemoryVolumeResizeGate: true,
			initialVolumes: []v1.Volume{
				{
					Name: "vol-1",
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{
							SizeLimit: &quantity100Mi,
						},
					},
				},
			},
			resizedVolumes: []v1.Volume{
				{
					Name: "vol-1",
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{
							SizeLimit: &quantity200Mi,
						},
					},
				},
			},
			expectResizeAction: false,
		},
		{
			name:                         "feature gate enabled: multiple volumes, one memory-backed limit changed",
			enableMemoryVolumeResizeGate: true,
			initialVolumes: []v1.Volume{
				{
					Name: "vol-1",
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{
							Medium:    v1.StorageMediumMemory,
							SizeLimit: &quantity100Mi,
						},
					},
				},
				{
					Name: "vol-2",
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: "/tmp",
						},
					},
				},
			},
			resizedVolumes: []v1.Volume{
				{
					Name: "vol-1",
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{
							Medium:    v1.StorageMediumMemory,
							SizeLimit: &quantity200Mi,
						},
					},
				},
				{
					Name: "vol-2",
					VolumeSource: v1.VolumeSource{
						HostPath: &v1.HostPathVolumeSource{
							Path: "/tmp",
						},
					},
				},
			},
			expectResizeAction: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodVerticalScalingMemoryBackedVolumes, tt.enableMemoryVolumeResizeGate)

			testPod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name: "test-pod",
					UID:  "12345",
				},
			}
			initialPod := testPod.DeepCopy()
			resizedPod := testPod.DeepCopy()

			initialPod.Spec.Volumes = tt.initialVolumes
			resizedPod.Spec.Volumes = tt.resizedVolumes

			testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
			t.Cleanup(func() { testKubelet.Cleanup() })
			kubelet := testKubelet.kubelet

			kubelet.podManager.AddPod(initialPod)
			require.NoError(t, kubelet.allocationManager.SetAllocatedResources(logger, initialPod))
			kubelet.HandlePodUpdates(tCtx, []*v1.Pod{resizedPod})

			allocatedPod, wasUpdated := kubelet.allocationManager.UpdatePodFromAllocation(resizedPod.DeepCopy())
			assert.Equal(t, tt.expectResizeAction, wasUpdated)
			assert.Equal(t, tt.expectResizeAction, kubelet.allocationManager.HasPendingResizes())

			if tt.expectResizeAction {
				// 分配记录应回退到初始（旧）的限额。
				assert.Equal(t, initialPod.Spec.Volumes[0].EmptyDir.SizeLimit.Value(), allocatedPod.Spec.Volumes[0].EmptyDir.SizeLimit.Value())
			} else if len(resizedPod.Spec.Volumes) > 0 && resizedPod.Spec.Volumes[0].EmptyDir != nil && resizedPod.Spec.Volumes[0].EmptyDir.SizeLimit != nil {
				// 分配记录应保持更新后的（新）限额。
				require.NotEmpty(t, allocatedPod.Spec.Volumes)
				assert.Equal(t, resizedPod.Spec.Volumes[0].EmptyDir.SizeLimit.Value(), allocatedPod.Spec.Volumes[0].EmptyDir.SizeLimit.Value())
			}
		})
	}
}

// 验证 HandlePodReconcile 只在资源需求下降等情况下才重试被延迟的 resize。
func TestHandlePodReconcile_RetryPendingResizes(t *testing.T) {
	if goruntime.GOOS == "windows" {
		t.Skip("InPlacePodVerticalScaling is not currently supported for Windows")
	}
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodVerticalScaling, true)
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DRANodeAllocatableResources, true)
	logger, tCtx := ktesting.NewTestContext(t)

	testKubelet := newTestKubeletExcludeAdmitHandlers(t, false /* controllerAttachDetachEnabled */, true /*enableResizing*/)
	defer testKubelet.Cleanup()
	kubelet := testKubelet.kubelet

	lowCPU := resource.MustParse("500m")
	highCPU := resource.MustParse("1")
	lowMem := resource.MustParse("500Mi")
	highMem := resource.MustParse("1Gi")

	// 把期望资源设得很大，用来验证聚合检查会忽略期望值、只看已分配值。
	enormousCPU := resource.MustParse("2000m")
	enormousMem := resource.MustParse("2000Mi")

	makePodWithResources := func(name string, requests v1.ResourceList, status v1.ResourceList) *v1.Pod {
		return &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: name,
				UID:  types.UID(name),
			},
			Spec: v1.PodSpec{
				Containers: []v1.Container{
					{
						Name:  "c1",
						Image: "i1",
						Resources: v1.ResourceRequirements{
							Requests: requests,
						},
					},
				},
			},
			Status: v1.PodStatus{
				ContainerStatuses: []v1.ContainerStatus{
					{
						Name: "c1",
						Resources: &v1.ResourceRequirements{
							Requests: status,
						},
					},
				},
			},
		}
	}

	terminalPod := makePodWithResources("terminal-pod", v1.ResourceList{v1.ResourceCPU: lowCPU, v1.ResourceMemory: lowMem}, v1.ResourceList{v1.ResourceCPU: lowCPU, v1.ResourceMemory: lowMem})
	terminalPod.Status.Phase = v1.PodFailed

	pendingResizeAllocated := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-pending-resize",
			UID:  types.UID("pod-pending-resize"),
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "c1",
					Image: "i1",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: highCPU, v1.ResourceMemory: highMem},
					},
				},
			},
		},
	}

	pendingResizeDesired := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-pending-resize",
			UID:  types.UID("pod-pending-resize"),
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "c1",
					Image: "i1",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: lowCPU, v1.ResourceMemory: lowMem},
					},
				},
			},
		},
	}

	terminalPodNoAllocation := makePodWithResources("terminal-pod-no-alloc", v1.ResourceList{v1.ResourceCPU: lowCPU, v1.ResourceMemory: lowMem}, v1.ResourceList{v1.ResourceCPU: lowCPU, v1.ResourceMemory: lowMem})
	terminalPodNoAllocation.Status.Phase = v1.PodFailed

	testCases := []struct {
		name                     string
		oldPod                   *v1.Pod
		newPod                   *v1.Pod
		setAllocation            bool
		shouldRetryPendingResize bool
	}{
		{
			name:                     "requests are increasing",
			oldPod:                   makePodWithResources("updated-pod", v1.ResourceList{v1.ResourceCPU: highCPU, v1.ResourceMemory: highMem}, v1.ResourceList{v1.ResourceCPU: lowCPU, v1.ResourceMemory: lowMem}),
			newPod:                   makePodWithResources("updated-pod", v1.ResourceList{v1.ResourceCPU: enormousCPU, v1.ResourceMemory: enormousMem}, v1.ResourceList{v1.ResourceCPU: highCPU, v1.ResourceMemory: highMem}),
			setAllocation:            true,
			shouldRetryPendingResize: false,
		},
		{
			name:                     "requests are unchanged",
			oldPod:                   makePodWithResources("updated-pod", v1.ResourceList{v1.ResourceCPU: lowCPU, v1.ResourceMemory: lowMem}, v1.ResourceList{v1.ResourceCPU: lowCPU, v1.ResourceMemory: lowMem}),
			newPod:                   makePodWithResources("updated-pod", v1.ResourceList{v1.ResourceCPU: enormousCPU, v1.ResourceMemory: enormousMem}, v1.ResourceList{v1.ResourceCPU: lowCPU, v1.ResourceMemory: lowMem}),
			setAllocation:            true,
			shouldRetryPendingResize: false,
		},
		{
			name:                     "requests are decreasing",
			oldPod:                   makePodWithResources("updated-pod", v1.ResourceList{v1.ResourceCPU: lowCPU, v1.ResourceMemory: lowMem}, v1.ResourceList{v1.ResourceCPU: highCPU, v1.ResourceMemory: highMem}),
			newPod:                   makePodWithResources("updated-pod", v1.ResourceList{v1.ResourceCPU: enormousCPU, v1.ResourceMemory: enormousMem}, v1.ResourceList{v1.ResourceCPU: lowCPU, v1.ResourceMemory: lowMem}),
			setAllocation:            true,
			shouldRetryPendingResize: true,
		},
		{
			name:                     "pod is marked as terminal",
			oldPod:                   makePodWithResources("terminal-pod", v1.ResourceList{v1.ResourceCPU: lowCPU, v1.ResourceMemory: lowMem}, v1.ResourceList{v1.ResourceCPU: lowCPU, v1.ResourceMemory: lowMem}),
			newPod:                   terminalPod,
			setAllocation:            true,
			shouldRetryPendingResize: true,
		},
		{
			name:                     "pod is marked as terminal with allocation already purged",
			oldPod:                   makePodWithResources("terminal-pod-no-alloc", v1.ResourceList{v1.ResourceCPU: lowCPU, v1.ResourceMemory: lowMem}, v1.ResourceList{v1.ResourceCPU: lowCPU, v1.ResourceMemory: lowMem}),
			newPod:                   terminalPodNoAllocation,
			setAllocation:            false,
			shouldRetryPendingResize: true,
		},
		{
			name: "requests decreasing due to DRA claim mapping reduction",
			oldPod: func() *v1.Pod {
				p := makePodWithResources("updated-pod", v1.ResourceList{v1.ResourceCPU: lowCPU}, v1.ResourceList{v1.ResourceCPU: lowCPU})
				p.Status.NodeAllocatableResourceClaimStatuses = []v1.NodeAllocatableResourceClaimStatus{
					{
						ResourceClaimName: "claim-1",
						Containers:        []string{"c1"},
						Mapping: []v1.NodeAllocatableMappedResources{
							{Name: v1.ResourceCPU, Quantity: new(resource.MustParse("500m"))},
						},
					},
				}
				return p
			}(),
			newPod: func() *v1.Pod {
				p := makePodWithResources("updated-pod", v1.ResourceList{v1.ResourceCPU: lowCPU}, v1.ResourceList{v1.ResourceCPU: lowCPU})
				p.Status.NodeAllocatableResourceClaimStatuses = []v1.NodeAllocatableResourceClaimStatus{
					{
						ResourceClaimName: "claim-1",
						Containers:        []string{"c1"},
						Mapping: []v1.NodeAllocatableMappedResources{
							{Name: v1.ResourceCPU, Quantity: new(resource.MustParse("100m"))},
						},
					},
				}
				return p
			}(),
			shouldRetryPendingResize: true,
			setAllocation:            true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 本用例中一律拒绝 resize 请求，使其进入延迟队列。
			handler := &testPodAdmitHandler{podsToReject: []*v1.Pod{pendingResizeAllocated}}
			kubelet.allocationManager.AddPodAdmitHandlers(lifecycle.PodAdmitHandlers{handler})

			require.NoError(t, kubelet.allocationManager.SetAllocatedResources(logger, pendingResizeAllocated))
			if tc.setAllocation {
				require.NoError(t, kubelet.allocationManager.SetAllocatedResources(logger, tc.oldPod))
			}

			// HandlePodReconcile 只应改动状态中的资源，不应改动 Spec。
			tc.oldPod.Spec = tc.newPod.Spec

			kubelet.podManager.AddPod(pendingResizeDesired)
			kubelet.podManager.AddPod(tc.oldPod)
			kubelet.allocationManager.PushPendingResize(logger, pendingResizeDesired.UID)

			kubelet.statusManager.ClearPodResizePendingCondition(pendingResizeDesired.UID, metrics.DeferredResizeResolutionAccepted)
			kubelet.HandlePodReconcile(tCtx, []*v1.Pod{tc.newPod})
			require.Equal(t, tc.shouldRetryPendingResize, kubelet.statusManager.IsPodResizeDeferred(pendingResizeDesired.UID))

			if !tc.setAllocation {
				require.False(t, kubelet.allocationManager.HasPodAllocatedResources(tc.newPod.UID),
					"non-allocated pod should not have allocation set after reconcile")
			}

			kubelet.allocationManager.RemovePod(logger, pendingResizeDesired.UID)
			kubelet.podManager.RemovePod((pendingResizeDesired))
			kubelet.podManager.RemovePod(tc.oldPod)
		})
	}
}

// 验证 SyncPod 会检查 Pod 更新新引入的节点声明特性需求，
// 不满足时发出事件，而旧 Pod 状态缺失则跳过检查。
func TestSyncPodNodeDeclaredFeaturesUpdate(t *testing.T) {
	tCtx := ktesting.Init(t)
	cpu1000m := resource.MustParse("1")
	mem1000M := resource.MustParse("1Gi")
	cpu2000m := resource.MustParse("2")
	oldPod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "1111",
			Name:      "pod1",
			Namespace: "ns1",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "c1",
					Image: "i1",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: cpu1000m, v1.ResourceMemory: mem1000M},
					},
				},
			},
		},
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
			ContainerStatuses: []v1.ContainerStatus{
				{
					Name:               "c1",
					AllocatedResources: v1.ResourceList{v1.ResourceCPU: cpu1000m, v1.ResourceMemory: mem1000M},
					Resources:          &v1.ResourceRequirements{},
				},
			},
		},
	}
	newPod := oldPod.DeepCopy()
	newPod.Spec.Containers[0].Resources.Requests[v1.ResourceCPU] = cpu2000m
	createMockFeature := func(t *testing.T, name string, inferForUpdate bool, maxVersionStr string) *ndftesting.MockFeature {
		m := ndftesting.NewMockFeature(t)
		m.SetName(name)
		m.SetInferForUpdate(func(_, _ *ndf.PodInfo) bool { return inferForUpdate })
		if maxVersionStr != "" {
			m.SetMaxVersion(utilversion.MustParseSemantic(maxVersionStr))
		} else {
			m.SetMaxVersion(nil)
		}
		return m
	}

	testCases := []struct {
		name               string
		featureGateEnabled bool
		nodeFeatures       []string
		oldPod             *v1.Pod
		newPod             *v1.Pod
		registeredFeatures []ndf.Feature
		expectEvent        bool
		expectedEventMsg   string
		componentVersion   string
	}{
		{
			name:               "Feature gate disabled",
			featureGateEnabled: false,
			componentVersion:   "1.35.0",
			oldPod:             oldPod,
			newPod:             newPod,
			registeredFeatures: []ndf.Feature{},
			expectEvent:        false,
		},
		{
			name:               "Feature gate enabled, no requirements from update",
			featureGateEnabled: true,
			componentVersion:   "1.35.0",
			oldPod:             oldPod,
			newPod:             newPod,
			nodeFeatures:       []string{"FeatureA"},
			registeredFeatures: []ndf.Feature{createMockFeature(t, "FeatureA", false, "")},
			expectEvent:        false,
		},
		{
			name:               "Feature gate enabled, requirements met",
			featureGateEnabled: true,
			componentVersion:   "1.35.0",
			oldPod:             oldPod,
			newPod:             newPod,
			nodeFeatures:       []string{"FeatureA"},
			registeredFeatures: []ndf.Feature{createMockFeature(t, "FeatureA", true, "")},
			expectEvent:        false,
		},
		{
			name:               "Feature gate enabled, requirements not met",
			featureGateEnabled: true,
			componentVersion:   "1.35.0",
			oldPod:             oldPod,
			newPod:             newPod,
			nodeFeatures:       []string{"FeatureB"},
			registeredFeatures: []ndf.Feature{createMockFeature(t, "FeatureA", true, ""), createMockFeature(t, "FeatureB", false, "")},
			expectEvent:        true,
			expectedEventMsg:   "Pod requires node features that are not available: FeatureA",
		},
		{
			name:               "feature generally available - not declared",
			featureGateEnabled: true,
			componentVersion:   "1.35.0",
			oldPod:             oldPod,
			newPod:             newPod,
			nodeFeatures:       []string{},
			registeredFeatures: []ndf.Feature{createMockFeature(t, "FeatureA", true, "1.34.0")},
			expectEvent:        false,
		},
		{
			name:               "Feature gate enabled, old pod state missing, node declared feature check skipped",
			featureGateEnabled: true,
			componentVersion:   "1.35.0",
			oldPod:             nil,
			newPod:             newPod,
			nodeFeatures:       []string{"FeatureB"},
			registeredFeatures: []ndf.Feature{createMockFeature(t, "FeatureA", true, ""), createMockFeature(t, "FeatureB", false, "")},
			expectEvent:        false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			logger := klog.FromContext(tCtx)
			if !tc.featureGateEnabled {
				featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, utilversion.MustParse("1.36"))
				featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.NodeDeclaredFeatures, false)
			}

			testKubelet := newTestKubelet(t, false)
			defer testKubelet.Cleanup()
			kubelet := testKubelet.kubelet

			// Setup mocks
			recorder := record.NewFakeRecorder(10)
			kubelet.recorder = recorder

			framework := ndf.New(tc.registeredFeatures)
			kubelet.nodeDeclaredFeaturesFramework = framework
			kubelet.nodeDeclaredFeatures = tc.nodeFeatures
			kubelet.nodeDeclaredFeaturesSet = framework.MustMapSorted(kubelet.nodeDeclaredFeatures)
			kubelet.version = utilversion.MustParse("1.35.0")

			if tc.oldPod != nil {
				kubelet.podManager.SetPods([]*v1.Pod{tc.oldPod})
			}

			kubelet.statusManager.SetPodStatus(logger, tc.newPod, v1.PodStatus{Phase: v1.PodRunning})
			require.NoError(t, kubelet.allocationManager.SetAllocatedResources(logger, tc.newPod))
			kubelet.HandlePodUpdates(tCtx, []*v1.Pod{tc.newPod})
			if tc.expectEvent {
				select {
				case event := <-recorder.Events:
					assert.Contains(t, event, tc.expectedEventMsg)
				default:
					t.Errorf("Expected an event but did not receive one.")
				}
			} else {
				select {
				case event := <-recorder.Events:
					t.Errorf("Expected no event, but received: %s", event)
				default:
					// No event received, which is correct.
				}
			}
		})
	}
}

// generateCAAndCertKeyWithOptions 生成自签名证书链，
// 并拆分为 CA 证书、叶子证书和私钥三部分返回。
func generateCAAndCertKeyWithOptions(host string, alternateIPs []net.IP, alternateDNS []string) (ca, certificate, certKey []byte, err error) {
	certs, key, err := cert.GenerateSelfSignedCertKeyWithOptions(cert.SelfSignedCertKeyOptions{
		Host:         host,
		AlternateIPs: alternateIPs,
		AlternateDNS: alternateDNS,
	})
	if err != nil {
		return nil, nil, nil, err
	}

	parsedCerts, err := cert.ParseCertsPEM(certs)
	if err != nil {
		return nil, nil, nil, err
	}

	var leafCert *x509.Certificate
	var caCert *x509.Certificate

	for _, parsedCert := range parsedCerts {
		if parsedCert.IsCA {
			caCert = parsedCert
		} else {
			leafCert = parsedCert
		}
	}

	caCertBytes, err := cert.EncodeCertificates(caCert)
	if err != nil {
		return nil, nil, nil, err
	}

	leafCertBytes, err := cert.EncodeCertificates(leafCert)
	if err != nil {
		return nil, nil, nil, err
	}

	return caCertBytes, leafCertBytes, key, err
}

// 验证 RestartAllContainers 触发成功的 RemoveContainer 后，
// 同步会重新入队一次，并且不会无限循环。
func TestSyncPodRestartAllContainersRequeue(t *testing.T) {
	logger, tCtx := ktesting.NewTestContext(t)
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.NodeDeclaredFeatures, true)
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.RestartAllContainersOnContainerExits, true)

	testKubelet := newTestKubelet(t, false)
	defer testKubelet.Cleanup()
	kubelet := testKubelet.kubelet

	pod := podWithUIDNameNsSpec("12345678", "foo", "new", v1.PodSpec{
		Containers: []v1.Container{
			{
				Name: "bar",
				RestartPolicyRules: []v1.ContainerRestartRule{
					{
						Action: v1.ContainerRestartRuleActionRestartAllContainers,
					},
				},
			},
		},
	})
	kubelet.podManager.SetPods([]*v1.Pod{pod})

	kubelet.statusManager.SetPodStatus(logger, pod, v1.PodStatus{
		Phase: v1.PodRunning,
		Conditions: []v1.PodCondition{
			{
				Type:   v1.AllContainersRestarting,
				Status: v1.ConditionTrue,
			},
		},
	})

	testKubelet.fakeRuntime.SyncResults = &kubecontainer.PodSyncResult{
		SyncResults: []*kubecontainer.SyncResult{{
			Action: kubecontainer.RemoveContainer,
			Target: pod.UID,
			Error:  nil,
		}},
	}

	callCount := 0
	kubelet.podWorkers = &fakePodWorkers{
		syncPodFn: func(ctx context.Context, updateType kubetypes.SyncPodType, pod, mirrorPod *v1.Pod, podStatus *kubecontainer.PodStatus) (bool, func(), error) {
			callCount++
			if callCount > 1 {
				// In the second call the SyncResult will not include the RemoveContainer.
				// This ensures no infinite loop.
				testKubelet.fakeRuntime.SyncResults = &kubecontainer.PodSyncResult{}
			}
			return kubelet.SyncPod(ctx, updateType, pod, mirrorPod, podStatus)
		},
		cache: kubelet.podCache,
		t:     t,
	}

	podStatus := &kubecontainer.PodStatus{
		ContainerStatuses: []*kubecontainer.Status{
			{
				Name: "bar",
			},
		},
	}

	isTerminal, _, err := kubelet.SyncPod(tCtx, kubetypes.SyncPodUpdate, pod, nil, podStatus)
	require.False(t, isTerminal)
	require.NoError(t, err)

	// 直接调用 SyncPod 不会增加 callCount；断言为 1 说明成功的 RemoveContainer
	// 恰好触发了一次经 UpdatePod -> fakePodWorkers.syncPodFn 的递归重排。
	require.Equal(t, 1, callCount)
}

// cleanUpTempDir 恢复全局日志目录并删除临时目录。
func cleanUpTempDir(t *testing.T, tempDir, originalLogsDir string) {
	t.Helper()
	ContainerLogsDir = originalLogsDir
	// Windows 上并发的 goroutine（例如 GC）可能需要一点时间退出并释放文件/目录句柄，
	// 因此 RemoveAll 需要重试。
	var err error
	for range 10 {
		err = os.RemoveAll(tempDir)
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.NoError(t, err)
}
