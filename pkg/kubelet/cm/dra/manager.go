/*
Copyright 2022 The Kubernetes Authors.

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

// Package dra 实现 kubelet 对动态资源分配（DRA）的节点侧管理：准备/释放 ResourceClaim、与 DRA 插件通信并维护健康状态。
package dra

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/component-base/metrics"
	"k8s.io/dynamic-resource-allocation/resourceclaim"
	"k8s.io/klog/v2"

	drahealthv1 "k8s.io/kubelet/pkg/apis/dra-health/v1"
	drapb "k8s.io/kubelet/pkg/apis/dra/v1"
	kubefeatures "k8s.io/kubernetes/pkg/features"
	draplugin "k8s.io/kubernetes/pkg/kubelet/cm/dra/plugin"
	"k8s.io/kubernetes/pkg/kubelet/cm/dra/state"
	"k8s.io/kubernetes/pkg/kubelet/cm/resourceupdates"
	"k8s.io/kubernetes/pkg/kubelet/config"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	kubeletmetrics "k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/kubernetes/pkg/kubelet/pluginmanager/cache"
	schedutil "k8s.io/kubernetes/pkg/scheduler/util"
)

// draManagerStateFileName 是 DRA 管理器持久化状态的文件名。
const draManagerStateFileName = "dra_manager_state"

// defaultReconcilePeriod 是默认的对账周期，用于保持 claimInfo 状态与真实 Pod 一致。
const defaultReconcilePeriod = 60 * time.Second

// DRA 驱动被注销后必须在这段时间内重新注册，否则 kubelet 会删除其 ResourceSlice。
//
// 这段时间既要足够长，以便停止 Pod 并启动替代实例（否则 ResourceSlice 会被误删），
// 又不能太长，否则驱动被移除后仍可能有 Pod 被调度到该节点。
// 30 秒大约够一次简单的容器重启；若 DRA 驱动希望确保 slice 不被清除，应使用滚动更新。
const defaultWipingDelay = 30 * time.Second

// ActivePodsFunc 返回节点上当前活跃的 Pod 列表，供对账循环使用。
type ActivePodsFunc func() []*v1.Pod

// GetNodeFunc 通过 kubelet 的 node lister 返回 Node 对象。
type GetNodeFunc func(context.Context) (*v1.Node, error)

// Manager 负责管理 ResourceClaim：
// 在 Pod 启动前完成资源准备，
// 并在最后一个使用方 Pod 终止后释放资源。
// 所有状态变化都会写入 checkpoint，以便 kubelet 重启后恢复。
type Manager struct {
	// draPlugins 管理已注册的 DRA 插件。
	draPlugins *draplugin.DRAPluginManager

	// cache 缓存 ResourceClaim 的本地准备状态与设备信息。
	cache *claimInfoCache

	// reconcilePeriod 是两次对账之间的间隔。
	reconcilePeriod time.Duration

	// activePods 用于列出节点上的活跃 Pod，
	// 对账循环据此清理已消失 Pod 的 claim 引用。
	activePods ActivePodsFunc

	// sourcesReady 表示 kubelet 配置源是否就绪（如 apiserver 同步完成）。
	// 只有源就绪后才能安全地把 Pod 视为不活跃。
	sourcesReady config.SourcesReady

	// kubeClient 用于从 API server 获取 ResourceClaim。
	kubeClient clientset.Interface

	// healthInfoCache 缓存设备健康信息。
	healthInfoCache *healthInfoCache

	// update 是资源健康变更通知通道，容量 100 避免阻塞插件流。
	update chan resourceupdates.Update
}

// NewManager 创建一个新的 DRA 管理器。
//
// - 不重复包含 Pod 信息，调用方已知道是哪个 Pod；
// - 使用用户熟悉的术语；
// - 不包含命名空间，可从上下文推断；
// - 避免 "failed to ...: failed to ..." 式的重复包装；
// - 无额外信息时不要包装，保持错误简短。
// 这样最终展示给用户的错误更易读。
// 该约定同样适用于本文件其他错误返回。
func NewManager(logger klog.Logger, kubeClient clientset.Interface, stateFileDirectory string) (*Manager, error) {
	claimInfoCache, err := newClaimInfoCache(stateFileDirectory, draManagerStateFileName)
	if err != nil {
		return nil, fmt.Errorf("create ResourceClaim cache: %w", err)
	}

	healthInfoCache, err := newHealthInfoCache(logger, filepath.Join(stateFileDirectory, "dra_health_state"))
	if err != nil {
		return nil, fmt.Errorf("failed to create healthInfo cache: %w", err)
	}

	// TODO: 目前对账周期不可配置，后续可考虑开放。
	// 在此之前统一使用默认值。
	reconcilePeriod := defaultReconcilePeriod

	manager := &Manager{
		cache:           claimInfoCache,
		kubeClient:      kubeClient,
		reconcilePeriod: reconcilePeriod,
		activePods:      nil,
		sourcesReady:    nil,
		healthInfoCache: healthInfoCache,
		update:          make(chan resourceupdates.Update, 100),
	}

	return manager, nil
}

// NewMetricsCollector 返回基于 claimInfo 缓存的指标采集器。
func (m *Manager) NewMetricsCollector() metrics.StableCollector {
	return &claimInfoCollector{cache: m.cache}
}

// GetWatcherHandler 必须在 Start 之后调用，
// 因为它依赖只有 Start 才会传入的参数（例如 context）。
func (m *Manager) GetWatcherHandler() cache.PluginHandler {
	return m.draPlugins
}

// Stop 关闭 DRA 插件管理器并等待所有后台 goroutine 退出。
// 不再使用管理器时必须调用，
// 以避免 goroutine 泄漏和状态文件清理时的竞态。
func (m *Manager) Stop() {
	if m.draPlugins != nil {
		m.draPlugins.Stop()
	}
}

// Start 启动管理器的对账循环。
func (m *Manager) Start(ctx context.Context, activePods ActivePodsFunc, getNode GetNodeFunc, sourcesReady config.SourcesReady) error {
	m.initDRAPluginManager(ctx, getNode, defaultWipingDelay)
	m.activePods = activePods
	m.sourcesReady = sourcesReady
	// 对账循环按固定周期运行，直到 context 被取消。
	go wait.UntilWithContext(ctx, func(ctx context.Context) { m.reconcileLoop(ctx) }, m.reconcilePeriod)
	return nil
}

// initDRAPluginManager 可替代 Start 使用，
// 使管理器可用于 prepare/unprepare 调用；主要供测试使用。
func (m *Manager) initDRAPluginManager(ctx context.Context, getNode GetNodeFunc, wipingDelay time.Duration) {
	m.draPlugins = draplugin.NewDRAPluginManager(ctx, m.kubeClient, getNode, m, wipingDelay)
}

// reconcileLoop 周期性清理 claimInfoCache 中已失效的 Pod 引用。
func (m *Manager) reconcileLoop(ctx context.Context) {
	logger := klog.FromContext(ctx).WithName("dra-manager")
	// 所有配置源就绪后才开始对账，
	// 否则 activePods 可能返回不完整列表，
	// 从而误删仍在使用的 claim 引用。
	if m.sourcesReady == nil || !m.sourcesReady.AllReady() {
		return
	}

	// 收集活跃 Pod UID，用于判断哪些引用已失效。
	activePods := sets.New[string]()
	for _, p := range m.activePods() {
		activePods.Insert(string(p.UID))
	}

	// 汇总所有仍被 claimInfo 引用但已不活跃的 Pod。
	type podClaims struct {
		uid        types.UID
		namespace  string
		claimNames []string
	}
	inactivePodClaims := make(map[string]*podClaims)
	m.cache.RLock()
	for _, claimInfo := range m.cache.claimInfo {
		for podUID := range claimInfo.PodUIDs {
			if activePods.Has(podUID) {
				continue
			}
			if inactivePodClaims[podUID] == nil {
				inactivePodClaims[podUID] = &podClaims{
					uid:        types.UID(podUID),
					namespace:  claimInfo.Namespace,
					claimNames: []string{},
				}
			}
			inactivePodClaims[podUID].claimNames = append(inactivePodClaims[podUID].claimNames, claimInfo.ClaimName)
		}
	}
	m.cache.RUnlock()

	// 逐个释放；失败只记录日志，下轮对账会重试。
	for _, podClaims := range inactivePodClaims {
		if err := m.unprepareResources(ctx, podClaims.uid, podClaims.namespace, podClaims.claimNames); err != nil {
			logger.Info("Unpreparing pod resources in reconcile loop failed, will retry", "podUID", podClaims.uid, "err", err)
		}
	}
}

// PrepareResources 为 Pod 准备全部所需资源：
// 对每个新资源需求发起 NodePrepareResources RPC，
// 处理响应并更新缓存的容器资源信息。
// 只有全部成功才会把 claim 标记为已准备。
func (m *Manager) PrepareResources(ctx context.Context, pod *v1.Pod) error {
	startTime := time.Now()
	err := m.prepareResources(ctx, pod)
	isError := (err != nil)
	kubeletmetrics.DRAOperationsDuration.WithLabelValues("PrepareResources", strconv.FormatBool(isError)).Observe(time.Since(startTime).Seconds())
	if err != nil {
		return fmt.Errorf("prepare dynamic resources: %w", err)
	}
	return nil
}

// prepareResources 是 PrepareResources 的主体实现。
func (m *Manager) prepareResources(ctx context.Context, pod *v1.Pod) error {
	var err error
	logger := klog.FromContext(ctx).WithName("dra-manager")
	logger = klog.LoggerWithValues(logger, "pod", klog.KObj(pod))
	batches := make(map[*draplugin.DRAPlugin][]*drapb.Claim)
	resourceClaims := make(map[types.UID]*resourceapi.ResourceClaim)

	// 先做一次只读校验，不修改 claimInfo 缓存：失败的 Pod 可以直接删除而不会卡住。
	// 若提前写入缓存，释放流程会误以为已调用过准备接口，在驱动从未安装或长期不可用时
	// 会导致释放持续失败。校验通过后再统一更新缓存，减少部分失败的影响。
	podResourceClaims := pod.Spec.ResourceClaims
	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.DRAExtendedResource) {
		// DRA 扩展资源对应的 claim 由调度器生成，也要一并准备。
		if pod.Status.ExtendedResourceClaimStatus != nil {
			extendedResourceClaim := v1.PodResourceClaim{
				ResourceClaimName: &pod.Status.ExtendedResourceClaimStatus.ResourceClaimName,
			}
			podResourceClaims = make([]v1.PodResourceClaim, 0, len(pod.Spec.ResourceClaims)+1)
			podResourceClaims = append(podResourceClaims, pod.Spec.ResourceClaims...)
			podResourceClaims = append(podResourceClaims, extendedResourceClaim)
		}
	}
	infos := make([]struct {
		resourceClaim *resourceapi.ResourceClaim
		podClaim      *v1.PodResourceClaim
		claimInfo     *ClaimInfo
		plugins       map[string]*draplugin.DRAPlugin
	}, len(podResourceClaims))
	for i := range podResourceClaims {
		podClaim := &podResourceClaims[i]
		infos[i].podClaim = podClaim
		logger.V(3).Info("Processing resource", "podClaim", podClaim.Name)
		claimName, mustCheckOwner, err := resourceclaim.Name(pod, podClaim)
		if err != nil {
			return err
		}

		if claimName == nil {
			// 没有生成实际 claim，无需处理。
			continue
		}
		// 从 API server 获取最新 claim，避免使用过期缓存。
		resourceClaim, err := m.kubeClient.ResourceV1().ResourceClaims(pod.Namespace).Get(
			ctx,
			*claimName,
			metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("fetch ResourceClaim %s: %w", *claimName, err)
		}

		if mustCheckOwner {
			if err = resourceclaim.IsForPod(pod, resourceClaim, m.podGroupResourceClaimsEnabled()); err != nil {
				// 错误信息已足够明确，不再包装。
				return err
			}
		}

		// 只有被 claim 预留的 Pod 才能使用该 claim。
		if !resourceclaim.IsReservedForPod(pod, resourceClaim, m.podGroupResourceClaimsEnabled()) {
			return fmt.Errorf("pod %s (%s) is not allowed to use ResourceClaim %s (%s)",
				pod.Name, pod.UID, *claimName, resourceClaim.UID)
		}

		// 到此认为必须准备该 claim，因此需要驱动；
		// 即使 claim 已准备，驱动不可用也视为节点异常并报错。
		// 这样问题能尽早暴露，而不是带着不确定状态继续。
		// 这是有意的严格行为。
		infos[i].resourceClaim = resourceClaim
		claimInfo, err := newClaimInfoFromClaim(resourceClaim)
		if err != nil {
			return fmt.Errorf("ResourceClaim %s: %w", resourceClaim.Name, err)
		}
		infos[i].claimInfo = claimInfo
		infos[i].plugins = make(map[string]*draplugin.DRAPlugin, len(claimInfo.DriverState))
		for driverName, driverState := range claimInfo.DriverState {
			if driverState.Skips(resourceapi.SkipNodeOperationNodePrepareResources) {
				continue
			}
			if plugin := infos[i].plugins[driverName]; plugin != nil {
				continue
			}
			plugin, err := m.draPlugins.GetPlugin(driverName)
			if err != nil {
				// 错误已包含驱动名，不再包装。
				return err
			}
			infos[i].plugins[driverName] = plugin
		}
	}

	// 所有信息就绪后，才在锁内更新 claimInfo 缓存。
	// 此时除内部一致性错误外几乎不会失败。
	err = m.cache.withLock(logger, func() error {
		for i := range podResourceClaims {
			resourceClaim := infos[i].resourceClaim
			podClaim := infos[i].podClaim
			if resourceClaim == nil {
				logger.V(5).Info("No need to prepare resources, no claim generated", "podClaim", podClaim.Name)
				continue
			}
			// 从缓存取 claimInfo，不存在则新建。
			// 新建条目会立即 checkpoint，确保后续失败可被对账发现。
			claimInfo, exists := m.cache.get(resourceClaim.Name, resourceClaim.Namespace)
			if !exists {
				claimInfo = infos[i].claimInfo
				m.cache.add(claimInfo)
				logger.V(6).Info("Created new claim info cache entry", "podClaim", podClaim.Name, "claim", klog.KObj(resourceClaim), "claimInfoEntry", claimInfo)
			} else {
				if claimInfo.ClaimUID != resourceClaim.UID {
					return fmt.Errorf("old ResourceClaim with same name %s and different UID %s still exists (previous pod force-deleted?!)", resourceClaim.Name, claimInfo.ClaimUID)
				}
				// 正在释放的 claim 不允许再被新 Pod 绑定，
				// 否则释放完成删除缓存条目时会把新 Pod 的引用一起丢掉。
				// 因此这里直接拒绝。
				if claimInfo.isUnpreparing() {
					return fmt.Errorf("ResourceClaim %s is being unprepared", resourceClaim.Name)
				}
				logger.V(6).Info("Found existing claim info cache entry", "podClaim", podClaim.Name, "claim", klog.KObj(resourceClaim), "claimInfoEntry", claimInfo)
			}

			// 先记录 Pod 引用，再 checkpoint。
			claimInfo.addPodReference(pod.UID)

			// 若准备失败且 Pod 被删除，对账循环能据此发现并清理。
			// 这就是先写引用后调 RPC 的原因。
			// 反过来则可能留下无法追踪的残留。
			// checkpoint 失败直接中止，避免状态只在内存中。
			if err := m.cache.syncToCheckpoint(); err != nil {
				return fmt.Errorf("checkpoint ResourceClaim cache: %w", err)
			}

			// 已准备的 claim 无需重复调用驱动，但同一 Pod 的其他 claim 仍需处理。
			if claimInfo.isPrepared() {
				logger.V(5).Info("Resources already prepared", "podClaim", podClaim.Name, "claim", klog.KObj(resourceClaim))
				continue
			}

			// 保存 claim 对象，待 RPC 成功后更新 claimInfo。
			// 失败时不会留下“已准备”的错误状态。
			resourceClaims[claimInfo.ClaimUID] = resourceClaim

			// 按驱动分组，随后每个驱动只发一次批量 RPC。
			claim := &drapb.Claim{
				Namespace: claimInfo.Namespace,
				Uid:       string(claimInfo.ClaimUID),
				Name:      claimInfo.ClaimName,
			}
			for driverName, driverState := range claimInfo.DriverState {
				if driverState.Skips(resourceapi.SkipNodeOperationNodePrepareResources) {
					kubeletmetrics.DRANodePrepareSkipsTotal.WithLabelValues(driverName).Inc()
					continue
				}
				plugin := infos[i].plugins[driverName]
				batches[plugin] = append(batches[plugin], claim)
			}
		}

		return nil
	})
	if err != nil {
		// 无需包装错误：这里没有可补充的上下文。
		// 返回的都是上面回调产生的错误。
		return err
	}

	// 按驱动批量调用 NodePrepareResources。
	// 任一错误都会中止整个流程，
	// 继续处理会增加状态一致性复杂度，暂不做。
	for plugin, claims := range batches {
		// 每个驱动一次 RPC 处理该批次的所有 claim。
		response, err := plugin.NodePrepareResources(ctx, &drapb.NodePrepareResourcesRequest{Claims: claims})
		if err != nil {
			// 与具体 claim 无关的通用错误。
			return fmt.Errorf("NodePrepareResources: %w", err)
		}
		for claimUID, result := range response.Claims {
			reqClaim := lookupClaimRequest(claims, claimUID)
			if reqClaim == nil {
				return fmt.Errorf("NodePrepareResources returned result for unknown claim UID %s", claimUID)
			}
			if result.GetError() != "" {
				return fmt.Errorf("NodePrepareResources failed for ResourceClaim %s: %s", reqClaim.Name, result.Error)
			}

			claim := resourceClaims[types.UID(claimUID)]

			// 驱动返回的结果对该 (claim, driver) 组合是权威的，
			// 因此先清空旧设备再重建，
			// 否则部分成功的重试会导致设备重复。
			// 这是幂等性的关键。
			// 重试时其他驱动可能已失败，
			// 但本驱动的设备列表必须保持最新。
			// 因此不能简单追加。
			err := m.cache.withLock(logger, func() error {
				info, exists := m.cache.get(claim.Name, claim.Namespace)
				if !exists {
					return fmt.Errorf("internal error: unable to get claim info for ResourceClaim %s in namespace %s", claim.Name, claim.Namespace)
				}
				info.resetDevices(plugin.DriverName())
				for _, device := range result.GetDevices() {
					info.addDevice(plugin.DriverName(), state.Device{PoolName: device.PoolName,
						DeviceName: device.DeviceName, ShareID: (*types.UID)(device.ShareId),
						RequestNames: device.RequestNames, CDIDeviceIDs: device.CdiDeviceIds})
				}
				return nil
			})
			if err != nil {
				// 这是上面回调返回的错误，无需包装。
				return err
			}
		}

		unfinished := len(claims) - len(response.Claims)
		if unfinished != 0 {
			return fmt.Errorf("NodePrepareResources skipped %d ResourceClaims", unfinished)
		}
	}

	// 在锁内原子地把所有 claim 标记为已准备并写 checkpoint。
	err = m.cache.withLock(logger, func() error {
		// 全部 RPC 成功后才标记已准备，避免半成功状态。
		for _, claim := range resourceClaims {
			info, exists := m.cache.get(claim.Name, claim.Namespace)
			if !exists {
				return fmt.Errorf("internal error: unable to get claim info for ResourceClaim %s in namespace %s", claim.Name, claim.Namespace)
			}
			info.setPrepared()
		}

		// 持久化设备列表，重启后才能恢复 CDI 设备信息。
		// 否则容器可能无法拿到设备。
		if err := m.cache.syncToCheckpoint(); err != nil {
			return fmt.Errorf("checkpoint ResourceClaim state: %w", err)
		}

		return nil
	})
	if err != nil {
		// 这是上面回调返回的错误，无需包装。
		return err
	}

	return nil
}

// lookupClaimRequest 按 UID 在请求列表中查找 claim，找不到返回 nil。
func lookupClaimRequest(claims []*drapb.Claim, claimUID string) *drapb.Claim {
	for _, claim := range claims {
		if claim.Uid == claimUID {
			return claim
		}
	}
	return nil
}

// truncateHealthMessage 将健康消息截断到 API 允许的最大长度。
// 超过上限时截断到 maxLength-3 并追加省略号，
// 保证最终长度不超过上限。
func truncateHealthMessage(message string) string {
	if len(message) <= v1.ResourceHealthMessageMaxLength {
		return message
	}
	return message[:v1.ResourceHealthMessageMaxLength-3] + "..."
}

// GetResources 从 claimInfo 缓存获取容器所需的 CDI 设备信息。
// 调用方据此更新容器配置。
func (m *Manager) GetResources(pod *v1.Pod, container *v1.Container) (*ContainerInfo, error) {
	cdiDevices := []kubecontainer.CDIDevice{}

	// claimRequests 记录本容器对每个 claim 的请求名，用于过滤设备。
	claimRequests := make(map[string][]string)

	// 先收集普通 ResourceClaims，按 claim 名聚合同一 claim 的多个请求。
	containerClaimsMap := make(map[string][]string, len(container.Resources.Claims))
	for _, claim := range container.Resources.Claims {
		if _, ok := containerClaimsMap[claim.Name]; !ok {
			containerClaimsMap[claim.Name] = []string{}
		}
		containerClaimsMap[claim.Name] = append(containerClaimsMap[claim.Name], claim.Request)
	}

	for i := range pod.Spec.ResourceClaims {
		podClaim := &pod.Spec.ResourceClaims[i]
		requests, ok := containerClaimsMap[podClaim.Name]
		if !ok {
			continue
		}

		claimName, _, err := resourceclaim.Name(pod, podClaim)
		if err != nil {
			// 错误信息已足够明确，不再包装。
			return nil, err
		}
		// claim 名为 nil 表示没有生成实际 claim，属于合法情况，直接跳过。
		// 跳过不会影响其他 claim 的处理。
		// 这是有意的宽松处理。
		if claimName == nil {
			continue
		}

		claimRequests[*claimName] = append(claimRequests[*claimName], requests...)
	}

	// 特性开启时，把 DRA 扩展资源对应的 claim 也纳入。
	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.DRAExtendedResource) && pod.Status.ExtendedResourceClaimStatus != nil {
		claimName := pod.Status.ExtendedResourceClaimStatus.ResourceClaimName
		// 容器对 DRA 扩展资源的请求必然通过调度器生成的 claim 分配。
		// 因此只需把本容器的请求名映射到该 claim。
		// 请求名由 RequestMappings 给出。

		// 收集本容器非零的 DRA 扩展资源请求。
		extendedResourceRequests := make(map[string]bool)
		for rName, rValue := range container.Resources.Requests {
			if !rValue.IsZero() && schedutil.IsDRAExtendedResourceName(rName) {
				extendedResourceRequests[rName.String()] = true
			}
		}

		// 只处理属于本容器的映射，且资源确实被请求。
		for _, rm := range pod.Status.ExtendedResourceClaimStatus.RequestMappings {
			// 同一容器对同一资源可以有多个请求。
			if rm.ContainerName == container.Name && extendedResourceRequests[rm.ResourceName] {
				claimRequests[claimName] = append(claimRequests[claimName], rm.RequestName)
			}
		}
	}

	// 读取缓存中的 CDI 设备并汇总。
	for claimName, requestNames := range claimRequests {
		err := m.cache.withRLock(func() error {
			claimInfo, exists := m.cache.get(claimName, pod.Namespace)
			if !exists {
				return fmt.Errorf("internal error: unable to get claim info for ResourceClaim %s in namespace %s", claimName, pod.Namespace)
			}

			for _, requestName := range requestNames {
				// 自 Kubernetes 1.31 起，CDI 设备 ID 不再通过注解传递。
				cdiDevices = append(cdiDevices, claimInfo.cdiDevicesAsList(requestName)...)
			}
			return nil
		})
		if err != nil {
			// 这是上面回调返回的错误，无需包装。
			return nil, err
		}
	}

	return &ContainerInfo{CDIDevices: cdiDevices}, nil
}

// UnprepareResources 对 Pod 拥有的每个 claim 调用驱动的 NodeUnprepareResources API。
// 该函数幂等，可对同一 Pod 多次调用。
// 已成功释放的 claim 会被跳过，
// 因此重复调用不会产生额外 RPC。
func (m *Manager) UnprepareResources(ctx context.Context, pod *v1.Pod) error {
	startTime := time.Now()
	err := m.unprepareResourcesForPod(ctx, pod)
	isError := (err != nil)
	kubeletmetrics.DRAOperationsDuration.WithLabelValues("UnprepareResources", strconv.FormatBool(isError)).Observe(time.Since(startTime).Seconds())
	if err != nil {
		return fmt.Errorf("unprepare dynamic resources: %w", err)
	}
	return nil
}

// unprepareResourcesForPod 收集 Pod 关联的 claim 名并执行释放。
func (m *Manager) unprepareResourcesForPod(ctx context.Context, pod *v1.Pod) error {
	// 优先从缓存获取 claim 名，这是正常路径。
	var claimNames []string
	_ = m.cache.withRLock(func() error {
		claimNames = m.cache.claimNamesForPod(pod.UID)
		return nil
	})

	// 缓存为空可能是 kubelet 重启且 checkpoint 丢失，
	// 此时从 Pod spec/status 重新推导 claim 名。
	// 保证所有相关 claim 都被释放。
	// 这是兜底路径，不应频繁触发。
	if len(claimNames) == 0 {
		for i := range pod.Spec.ResourceClaims {
			claimName, _, err := resourceclaim.Name(pod, &pod.Spec.ResourceClaims[i])
			if err != nil {
				// 错误信息已足够明确，不再包装。
				return err
			}
			if claimName == nil {
				// claim 名为 nil 表示没有生成实际 claim，合法地跳过。
				// 例如 claim 模板未生成对象的情况。
				// 跳过不影响其他 claim。
				continue
			}
			claimNames = append(claimNames, *claimName)
		}
		if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.DRAExtendedResource) {
			if pod.Status.ExtendedResourceClaimStatus != nil {
				claimNames = append(claimNames, pod.Status.ExtendedResourceClaimStatus.ResourceClaimName)
			}
		}
	}

	return m.unprepareResources(ctx, pod.UID, pod.Namespace, claimNames)
}

// unprepareResources 对给定 Pod 的 claim 执行实际的释放流程。
func (m *Manager) unprepareResources(ctx context.Context, podUID types.UID, namespace string, claimNames []string) error {
	logger := klog.FromContext(ctx).WithName("dra-manager")
	batches := make(map[string][]*drapb.Claim)
	claimNamesMap := make(map[types.UID]string)
	for _, claimName := range claimNames {
		// 在锁内更新引用计数，锁外调用驱动。
		err := m.cache.withLock(logger, func() error {
			// 缓存中没有说明该 claim 从未准备成功，无需释放。
			claimInfo, exists := m.cache.get(claimName, namespace)

			// 跳过即可，避免无意义 RPC。
			if !exists {
				return nil
			}

			// 若 claimInfo 未引用该 Pod，说明准备流程从未走到这里，
			// 此时不能释放，否则可能拆掉其他 Pod 仍在使用的 claim。
			// 这是防止误释放的关键检查。
			// 校验阶段失败就会留下这种状态。
			// 因此必须显式判断引用关系。
			// 否则会破坏其他 Pod。
			if !claimInfo.hasPodReference(podUID) {
				return nil
			}

			// 还有其他 Pod 引用时只减少引用计数，不调用驱动。
			if len(claimInfo.PodUIDs) > 1 {
				// 引用计数变化延迟到 UnprepareResources 成功后再 checkpoint。
				// 因为函数只在 checkpoint 成功后返回，
				// 若中途重启，重复调用该幂等函数即可恢复。
				// 这样避免频繁写盘。
				// 重复执行是安全的。
				// 因此可以延迟。
				claimInfo.deletePodReference(podUID)
				return nil
			}

			// 记录 claim 名，RPC 成功后据此删除缓存条目。
			// 失败时条目保留，便于重试。
			claimNamesMap[claimInfo.ClaimUID] = claimInfo.ClaimName

			// 在持锁时标记为“释放中”，
			// 这样并发的 prepareResources 会拒绝绑定该 claim。
			// RPC 在锁外执行，避免阻塞其他操作。
			// 标记与删除之间必须保持一致性。
			// 否则会出现绑定到正在释放的资源。
			// 因此需要 setUnpreparing。
			claimInfo.setUnpreparing()

			// 按驱动分组批量释放。
			claim := &drapb.Claim{
				Namespace: claimInfo.Namespace,
				Uid:       string(claimInfo.ClaimUID),
				Name:      claimInfo.ClaimName,
			}
			for driverName, driverState := range claimInfo.DriverState {
				if driverState.Skips(resourceapi.SkipNodeOperationNodeUnprepareResources) {
					kubeletmetrics.DRANodeUnprepareSkipsTotal.WithLabelValues(driverName).Inc()
					continue
				}
				batches[driverName] = append(batches[driverName], claim)
			}

			return nil
		})
		if err != nil {
			// 这是上面回调返回的错误，无需包装。
			return err
		}
	}

	// 按驱动批量调用 NodeUnprepareResources。
	// 任一错误都会中止，保持与准备流程一致。
	// 继续处理会增加复杂度且收益有限。
	for driverName, claims := range batches {
		// 每个驱动一次 RPC 处理该批次。
		plugin, err := m.draPlugins.GetPlugin(driverName)
		if err != nil {
			// 错误已包含驱动名，不再包装。
			return err
		}
		response, err := plugin.NodeUnprepareResources(ctx, &drapb.NodeUnprepareResourcesRequest{Claims: claims})
		if err != nil {
			// 与具体 claim 无关的通用错误。
			return fmt.Errorf("NodeUnprepareResources: %w", err)
		}

		for claimUID, result := range response.Claims {
			reqClaim := lookupClaimRequest(claims, claimUID)
			if reqClaim == nil {
				return fmt.Errorf("NodeUnprepareResources returned result for unknown claim UID %s", claimUID)
			}
			if result.GetError() != "" {
				return fmt.Errorf("NodeUnprepareResources failed for ResourceClaim %s: %s", reqClaim.Name, result.Error)
			}
		}

		unfinished := len(claims) - len(response.Claims)
		if unfinished != 0 {
			return fmt.Errorf("NodeUnprepareResources skipped %d ResourceClaims", unfinished)
		}
	}

	// 在锁内删除已释放的 claimInfo 并写 checkpoint。
	err := m.cache.withLock(logger, func() error {
		// TODO(#132978): 重新评估该逻辑以支持事后健康更新。
		// 初始实现会立即删除条目，因此已终止 Pod 的迟到健康更新会被丢弃。
		// 未来可考虑用带宽限期的“墓碑”条目替代直接删除。
		// 当前行为已由 TODO(#132978) 跟踪。
		// 删除是简单且安全的做法。

		// 删除刚释放完成的条目。
		for _, claimName := range claimNamesMap {
			claimInfo, _ := m.cache.get(claimName, namespace)
			m.cache.delete(claimName, namespace)
			logger.V(6).Info("Deleted claim info cache entry", "claim", klog.KRef(namespace, claimName), "claimInfoEntry", claimInfo)
		}

		// 原子地把缓存同步到 checkpoint，保证重启后状态一致。
		if err := m.cache.syncToCheckpoint(); err != nil {
			return fmt.Errorf("checkpoint ResourceClaim state: %w", err)
		}
		return nil
	})
	if err != nil {
		// 这是上面回调返回的错误，无需包装。
		return err
	}

	return nil
}

// PodMightNeedToUnprepareResources 判断 Pod 是否可能需要释放资源。
// 有 claim 引用即返回 true，用于避免无谓的释放调用。
func (m *Manager) PodMightNeedToUnprepareResources(uid types.UID) bool {
	m.cache.Lock()
	defer m.cache.Unlock()
	return m.cache.hasPodReference(uid)
}

// GetContainerClaimInfos 返回容器使用的全部 ClaimInfo 副本。
func (m *Manager) GetContainerClaimInfos(pod *v1.Pod, container *v1.Container) ([]*ClaimInfo, error) {
	claimInfos := make([]*ClaimInfo, 0, len(pod.Spec.ResourceClaims))

	// 先建索引，避免对每个 Pod claim 线性扫描。
	containerClaimsMap := make(map[string]bool, len(container.Resources.Claims))
	for _, claim := range container.Resources.Claims {
		containerClaimsMap[claim.Name] = true
	}

	for i, podResourceClaim := range pod.Spec.ResourceClaims {
		// 只处理本容器实际引用的 claim。
		if !containerClaimsMap[podResourceClaim.Name] {
			continue
		}

		claimName, _, err := resourceclaim.Name(pod, &pod.Spec.ResourceClaims[i])
		if err != nil {
			// 错误信息已足够明确，不再包装。
			return nil, err
		}

		if claimName == nil {
			// 无需 ResourceClaim，直接跳过。
			continue
		}

		// 归属校验在准备阶段已完成，这里不再重复。

		err = m.cache.withRLock(func() error {
			claimInfo, exists := m.cache.get(*claimName, pod.Namespace)
			if !exists {
				return fmt.Errorf("unable to get information for ResourceClaim %s", *claimName)
			}
			claimInfos = append(claimInfos, claimInfo.DeepCopy())
			return nil
		})
		if err != nil {
			// 这是上面回调返回的错误，无需包装。
			return nil, err
		}
	}

	if utilfeature.DefaultFeatureGate.Enabled(kubefeatures.DRAExtendedResource) {
		// 处理 Pod 中由 DRA 支持的扩展资源对应的特殊 claim。
		if pod.Status.ExtendedResourceClaimStatus != nil {
			var hasExtendedResourceClaim bool
			for _, n := range pod.Status.ExtendedResourceClaimStatus.RequestMappings {
				if n.ContainerName == container.Name {
					hasExtendedResourceClaim = true
					break
				}
			}
			if !hasExtendedResourceClaim {
				return claimInfos, nil
			}
			claimName := &pod.Status.ExtendedResourceClaimStatus.ResourceClaimName
			err := m.cache.withRLock(func() error {
				claimInfo, exists := m.cache.get(*claimName, pod.Namespace)
				if !exists {
					return fmt.Errorf("unable to get claim info for claim %s in namespace %s", *claimName, pod.Namespace)
				}
				claimInfos = append(claimInfos, claimInfo.DeepCopy())
				return nil
			})
			if err != nil {
				// 这是上面回调返回的错误，无需包装。
				return nil, err
			}
		}
	}
	return claimInfos, nil
}

// UpdateAllocatedResourcesStatus 把已分配 DRA 设备的健康状态写入 Pod 的容器状态。
func (m *Manager) UpdateAllocatedResourcesStatus(logger klog.Logger, pod *v1.Pod, status *v1.PodStatus) {
	logger = logger.WithName("dra-manager")
	logger = klog.LoggerWithValues(logger, "pod", klog.KObj(pod))
	enableHealthMessage := utilfeature.DefaultFeatureGate.Enabled(kubefeatures.ResourceHealthStatusMessage)
	for i := range status.ContainerStatuses {
		containerStatus := &status.ContainerStatuses[i]

		// 按名称找到容器 spec；找不到说明状态与 spec 不一致，跳过。
		var containerSpec *v1.Container
		for j := range pod.Spec.Containers {
			if pod.Spec.Containers[j].Name == containerStatus.Name {
				containerSpec = &pod.Spec.Containers[j]
				break
			}
		}

		// 未使用 claim 的容器无需更新。
		if containerSpec == nil || len(containerSpec.Resources.Claims) == 0 {
			continue
		}

		resourceStatusMap := make(map[v1.ResourceName]*v1.ResourceStatus)
		if containerStatus.AllocatedResourcesStatus != nil {
			for j := range containerStatus.AllocatedResourcesStatus {
				resourceStatusMap[containerStatus.AllocatedResourcesStatus[j].Name] = &containerStatus.AllocatedResourcesStatus[j]
			}
		}

		// 建立 Pod claim 名到实际 claim 名的映射，便于快速查找。
		podClaimStatusMap := make(map[string]string, len(pod.Status.ResourceClaimStatuses))
		for _, podClaimStatus := range pod.Status.ResourceClaimStatuses {
			if podClaimStatus.ResourceClaimName != nil {
				podClaimStatusMap[podClaimStatus.Name] = *podClaimStatus.ResourceClaimName
			}
		}

		// 逐个处理本容器引用的 claim。
		for _, claim := range containerSpec.Resources.Claims {
			// 先查状态映射，
			// 状态未填充时再从 Pod spec 解析，
			// 因为健康更新可能早于状态同步触发。
			// 这是保证早到更新不丢失的关键。
			// 兜底路径只在状态缺失时执行。
			actualClaimName := podClaimStatusMap[claim.Name]

			if actualClaimName == "" {
				for i := range pod.Spec.ResourceClaims {
					if pod.Spec.ResourceClaims[i].Name == claim.Name {
						claimNamePtr, _, err := resourceclaim.Name(pod, &pod.Spec.ResourceClaims[i])
						if err == nil && claimNamePtr != nil {
							actualClaimName = *claimNamePtr
						}
						break
					}
				}
			}

			if actualClaimName == "" {
				logger.V(4).Info("Could not find generated name for resource claim", "container", containerSpec.Name, "claimName", claim.Name)
				continue
			}

			func() {
				m.cache.RLock()
				defer m.cache.RUnlock()

				// 用实际 claim 名查缓存，查不到说明尚未准备完成。
				claimInfo, exists := m.cache.get(actualClaimName, pod.Namespace)
				if !exists {
					logger.V(4).Info("Could not find claim info for resource claim", "container", containerSpec.Name, "claimName", actualClaimName)
					return
				}

				resourceName := v1.ResourceName(fmt.Sprintf("claim:%s", claim.Name))
				if claim.Request != "" {
					resourceName = v1.ResourceName(fmt.Sprintf("claim:%s/%s", claim.Name, claim.Request))
				}

				resStatus, ok := resourceStatusMap[resourceName]
				if !ok {
					newStatus := v1.ResourceStatus{
						Name:      resourceName,
						Resources: []v1.ResourceHealth{},
					}
					// 先 append 再取指针，避免 map 中保存指向临时变量的指针。
					if containerStatus.AllocatedResourcesStatus == nil {
						containerStatus.AllocatedResourcesStatus = []v1.ResourceStatus{}
					}
					containerStatus.AllocatedResourcesStatus = append(containerStatus.AllocatedResourcesStatus, newStatus)
					resStatus = &containerStatus.AllocatedResourcesStatus[len(containerStatus.AllocatedResourcesStatus)-1]
					resourceStatusMap[resourceName] = resStatus
				}

				// 每次重建而非增量更新，保证过期设备不会残留。
				resStatus.Resources = []v1.ResourceHealth{}

				// 多个设备可能映射到同一 CDI 资源 ID，必须去重。
				// 否则状态中会出现重复条目。
				// 例如同一 CDI 设备被多个请求共享时。
				seenResourceIDs := make(map[v1.ResourceID]bool)

				for driverName, driverState := range claimInfo.DriverState {
					for _, device := range driverState.Devices {
						// 只报告属于当前请求的设备。
						// 子请求需归一到基础请求名后比较，
						// 因为设备记录里存的是去掉子请求后缀的名字。
						if claim.Request != "" && len(device.RequestNames) > 0 && !slices.Contains(device.RequestNames, resourceclaim.BaseRequestRef(claim.Request)) {
							continue
						}

						healthInfo := m.healthInfoCache.getHealthInfo(driverName, device.PoolName, device.DeviceName)
						resourceHealth := buildResourceHealth(
							driverName,
							device,
							healthInfo,
							enableHealthMessage,
						)

						// 已加入的 resourceID 不再重复添加。
						if seenResourceIDs[resourceHealth.ResourceID] {
							continue
						}
						seenResourceIDs[resourceHealth.ResourceID] = true

						resStatus.Resources = append(resStatus.Resources, resourceHealth)
					}
				}
			}()
		}

		// 用 map 重建切片，顺带剔除没有健康条目的资源状态。
		finalStatuses := make([]v1.ResourceStatus, 0, len(resourceStatusMap))
		for _, rs := range resourceStatusMap {
			if len(rs.Resources) > 0 {
				finalStatuses = append(finalStatuses, *rs)
			}
		}
		containerStatus.AllocatedResourcesStatus = finalStatuses
	}
}

// toResourceHealthStatus 把内部健康状态转换为 API 枚举，未知值统一映射为 Unknown。
func toResourceHealthStatus(health state.DeviceHealthStatus) v1.ResourceHealthStatus {
	switch health {
	case state.DeviceHealthStatusHealthy:
		return v1.ResourceHealthStatusHealthy
	case state.DeviceHealthStatusUnhealthy:
		return v1.ResourceHealthStatusUnhealthy
	default:
		return v1.ResourceHealthStatusUnknown
	}
}

// buildResourceHealth 构造单条 ResourceHealth，包含健康状态、消息和资源 ID。
func buildResourceHealth(driverName string, device state.Device, healthInfo state.DeviceHealth, enableMessage bool) v1.ResourceHealth {
	// 健康消息受特性门控控制，关闭时不暴露给用户。
	resourceHealth := v1.ResourceHealth{
		Health: toResourceHealthStatus(healthInfo.Health),
	}
	if enableMessage && len(healthInfo.Message) > 0 {
		resourceHealth.Message = &healthInfo.Message
	}
	// 优先使用 CDI 设备 ID 作为资源 ID；
	if len(device.CDIDeviceIDs) > 0 {
		resourceHealth.ResourceID = v1.ResourceID(device.CDIDeviceIDs[0])
	} else {
		resourceHealth.ResourceID = v1.ResourceID(fmt.Sprintf("%s/%s/%s", driverName, device.PoolName, device.DeviceName))
	}
	return resourceHealth
}

// toDeviceHealthStatus 把插件上报的健康状态转换为内部枚举。
func toDeviceHealthStatus(health drahealthv1.HealthStatus) state.DeviceHealthStatus {
	switch health {
	case drahealthv1.HealthStatus_HEALTHY:
		return state.DeviceHealthStatusHealthy
	case drahealthv1.HealthStatus_UNHEALTHY:
		return state.DeviceHealthStatusUnhealthy
	default:
		return state.DeviceHealthStatusUnknown
	}
}

// buildDeviceHealth 把插件上报的设备健康信息转换为内部结构。
func buildDeviceHealth(logger klog.Logger, device *drahealthv1.DeviceHealth) state.DeviceHealth {
	// 未指定、为零或为负时使用默认超时。
	// 正数则按秒转换为 Duration。
	timeout := DefaultHealthTimeout
	timeoutSeconds := device.GetHealthCheckTimeoutSeconds()
	if timeoutSeconds > 0 {
		timeout = time.Duration(timeoutSeconds) * time.Second
	} else if timeoutSeconds < 0 {
		// 负值属于非法输入，记录日志并回退默认值。
		logger.V(4).Info("Ignoring negative health check timeout, using default",
			"poolName", device.GetDevice().GetPoolName(),
			"deviceName", device.GetDevice().GetDeviceName(),
			"providedTimeout", timeoutSeconds,
			"defaultTimeout", DefaultHealthTimeout)
	}
	return state.DeviceHealth{
		PoolName:           device.GetDevice().GetPoolName(),
		DeviceName:         device.GetDevice().GetDeviceName(),
		Health:             toDeviceHealthStatus(device.GetHealth()),
		LastUpdated:        time.Unix(device.GetLastUpdatedTime(), 0),
		HealthCheckTimeout: timeout,
		Message:            truncateHealthMessage(device.GetMessage()),
	}
}

// HandleWatchResourcesStream 持续接收 DRA 插件的健康更新流，并通知受影响的 Pod。
func (m *Manager) HandleWatchResourcesStream(ctx context.Context, stream drahealthv1.DRAResourceHealth_NodeWatchResourcesClient, pluginName string) error {
	logger := klog.FromContext(ctx).WithName("dra-manager")
	logger = klog.LoggerWithValues(logger, "pluginName", pluginName)

	// 流退出时清空该驱动的健康缓存，避免使用过期数据。
	defer func() {
		logger.V(4).Info("Clearing health cache for driver upon stream exit")
		// 清理使用独立 context，即使原 context 已取消也能完成。
		if err := m.healthInfoCache.clearDriver(logger, pluginName); err != nil {
			logger.Error(err, "Failed to clear health info cache for driver")
		}
	}()

	for {
		resp, err := stream.Recv()
		if err != nil {
			// context 取消属于正常关闭。
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				logger.V(4).Info("Stopping health monitoring due to context cancellation", "reason", err)
				return err
			}
			// 服务端正常关闭流会返回 EOF。
			if errors.Is(err, io.EOF) {
				logger.V(4).Info("Stream ended with EOF")
				return nil
			}
			// 驱动声明了健康服务但不支持该调用，
			// 这不算错误，但调用方应停止监听。
			if status.Code(err) == codes.Unimplemented {
				logger.V(4).Info("Driver does not support WatchResources", "reason", err)
				return err
			}
			// 其他错误视为异常，记录后返回。
			logger.Error(err, "Error receiving from WatchResources stream")
			return err
		}

		// 先转换全部设备，再一次性更新缓存。
		devices := make([]state.DeviceHealth, len(resp.GetDevices()))
		for i, d := range resp.GetDevices() {
			devices[i] = buildDeviceHealth(logger, d)
		}

		changedDevices, updateErr := m.healthInfoCache.updateHealthInfo(logger, pluginName, devices)
		if updateErr != nil {
			logger.Error(updateErr, "Failed to update health info cache")
		}
		if len(changedDevices) > 0 {
			logger.V(4).Info("Health info changed, checking affected pods", "changedDevicesCount", len(changedDevices))

			// 快照本插件设备被哪些 Pod 使用。
			// 锁只在构建索引时持有一次，
			// 与变化设备数量无关，
			// 避免高频大报告阻塞 DRA 操作。
			// 这是该设计的主要目的。
			type deviceKey struct {
				poolName   string
				deviceName string
			}
			podsByDevice := make(map[deviceKey][]string)
			m.cache.RLock()
			for _, cInfo := range m.cache.claimInfo {
				if driverState, ok := cInfo.DriverState[pluginName]; ok {
					for _, allocatedDevice := range driverState.Devices {
						key := deviceKey{poolName: allocatedDevice.PoolName, deviceName: allocatedDevice.DeviceName}
						podsByDevice[key] = append(podsByDevice[key], cInfo.PodUIDs.UnsortedList()...)
					}
				}
			}
			m.cache.RUnlock()

			podsToUpdate := sets.New[string]()
			for _, dev := range changedDevices {
				podsToUpdate.Insert(podsByDevice[deviceKey{poolName: dev.PoolName, deviceName: dev.DeviceName}]...)
			}

			if podsToUpdate.Len() > 0 {
				podUIDs := podsToUpdate.UnsortedList()
				logger.Info("Sending health update notification for pods", "pods", podUIDs)
				select {
				// 非阻塞发送：通道满时丢弃本次通知，避免阻塞插件流。
				case m.update <- resourceupdates.Update{PodUIDs: podUIDs}:
				default:
					logger.Error(nil, "DRA health update channel is full, discarding pod update notification", "pods", podUIDs)
				}
			} else {
				logger.V(4).Info("Health info changed, but no active pods found using the affected devices")
			}
		}

	}
}

// Updates 返回资源更新通知通道。
func (m *Manager) Updates() <-chan resourceupdates.Update {
	// 该通道由 HandleWatchResourcesStream 写入，调用方只读。
	return m.update
}

// podGroupResourceClaimsEnabled 判断是否启用 Pod 组级 ResourceClaim 特性。
func (*Manager) podGroupResourceClaimsEnabled() bool {
	return utilfeature.DefaultFeatureGate.Enabled(kubefeatures.DRAWorkloadResourceClaims)
}
