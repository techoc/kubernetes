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

// Package node 实现 cloud-node-controller：为带 cloudprovider 未初始化污点的节点
// 从云厂商补齐 providerID、地址与拓扑标签，并在节点就绪后周期性刷新地址。
package node

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	clientretry "k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	cloudprovider "k8s.io/cloud-provider"
	cloudproviderapi "k8s.io/cloud-provider/api"
	cloudnodeutil "k8s.io/cloud-provider/node/helpers"
	controllersmetrics "k8s.io/component-base/metrics/prometheus/controllers"
	nodeutil "k8s.io/component-helpers/node/util"
	"k8s.io/klog/v2"
)

func init() {
	registerMetrics()
}

// labelReconcileInfo 列出需要对账的节点标签以及对账方式。primaryKey 与 secondaryKey
// 是需要对账的标签键：
//   - 两个键都存在但取值不一致时，以 primaryKey 的取值作为事实来源；
//   - ensureSecondaryExists 为 true 且 secondaryKey 不存在时，用 primaryKey 的取值
//     补写 secondaryKey。
//
// beta 标签是唯一事实来源，GA 标签只是派生副本。这样即使有外部组件
// 仍在写 beta 标签，也能保证两套标签最终一致，避免调度器读到互相矛盾的拓扑信息。
var labelReconcileInfo = []struct {
	primaryKey            string
	secondaryKey          string
	ensureSecondaryExists bool
}{
	{
		// Reconcile the beta and the GA zone label using the beta label as
		// the source of truth
		// TODO: switch the primary key to GA labels in v1.21
		primaryKey:            v1.LabelFailureDomainBetaZone,
		secondaryKey:          v1.LabelTopologyZone,
		ensureSecondaryExists: true,
	},
	{
		// Reconcile the beta and the stable region label using the beta label as
		// the source of truth
		// TODO: switch the primary key to GA labels in v1.21
		primaryKey:            v1.LabelFailureDomainBetaRegion,
		secondaryKey:          v1.LabelTopologyRegion,
		ensureSecondaryExists: true,
	},
	{
		// Reconcile the beta and the stable instance-type label using the beta label as
		// the source of truth
		// TODO: switch the primary key to GA labels in v1.21
		primaryKey:            v1.LabelInstanceType,
		secondaryKey:          v1.LabelInstanceTypeStable,
		ensureSecondaryExists: true,
	},
}

// UpdateNodeSpecBackoff 是移除污点、写入 Node spec 时的冲突重试策略。
// 抖动设为 1.0，避免同一批新节点在冲突后同时重试造成惊群。
var UpdateNodeSpecBackoff = wait.Backoff{
	Steps:    20,
	Duration: 50 * time.Millisecond,
	Jitter:   1.0,
}

// CloudNodeController 是 Node 资源的控制器实现。控制器有两条独立的执行路径：
//
//  1. workqueue + syncNode：只处理带 cloudprovider 污点的新节点，做一次性初始化；
//  2. UpdateNodeStatus 定时循环：对已初始化节点周期性刷新地址。
//
// 两条路径都可能并发处理同一个节点，因此所有写操作都基于 lister 中的最新对象
// 拷贝并通过 patch/update 携带资源版本，冲突时重试。
type CloudNodeController struct {
	nodeInformer coreinformers.NodeInformer
	kubeClient   clientset.Interface

	broadcaster record.EventBroadcaster
	recorder    record.EventRecorder

	cloud cloudprovider.Interface

	nodeStatusUpdateFrequency time.Duration
	workerCount               int32
	statusUpdateWorkerCount   int32

	nodesLister corelisters.NodeLister
	nodesSynced cache.InformerSynced
	workqueue   workqueue.TypedRateLimitingInterface[string] // 只承载需要初始化的节点 key，而不是全量节点，避免周期性重扫。
}

// NewCloudNodeController 创建 CloudNodeController 对象。
// 构造阶段即校验云提供商是否具备 Instances 或 InstancesV2 能力——
// 二者都没有时无法获取节点信息，控制器没有存在意义。
func NewCloudNodeController(
	nodeInformer coreinformers.NodeInformer,
	kubeClient clientset.Interface,
	cloud cloudprovider.Interface,
	nodeStatusUpdateFrequency time.Duration,
	workerCount,
	statusUpdateWorkerCount int32) (*CloudNodeController, error) {

	_, instancesSupported := cloud.Instances()
	_, instancesV2Supported := cloud.InstancesV2()
	if !instancesSupported && !instancesV2Supported {
		return nil, errors.New("cloud provider does not support instances")
	}

	cnc := &CloudNodeController{
		nodeInformer:              nodeInformer,
		kubeClient:                kubeClient,
		cloud:                     cloud,
		nodeStatusUpdateFrequency: nodeStatusUpdateFrequency,
		workerCount:               workerCount,
		statusUpdateWorkerCount:   statusUpdateWorkerCount,
		nodesLister:               nodeInformer.Lister(),
		nodesSynced:               nodeInformer.Informer().HasSynced,
		workqueue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "Nodes"},
		),
	}

	// 使用 shared informer 监听节点的 add/update 事件：只注册 Add/Update，不注册
	// Delete——节点删除无需云侧初始化。控制器启动前已存在的节点会在首次 List 时
	// 以 Add 事件形式补齐。
	cnc.nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    cnc.enqueueNode,
		UpdateFunc: func(oldObj, newObj interface{}) { cnc.enqueueNode(newObj) },
	})

	return cnc, nil
}

// Run will sync informer caches and starting workers.
// This controller updates newly registered nodes with information
// from the cloud provider. This call is blocking so should be called
// via a goroutine
//
//logcheck:context // RunWithContext should be used instead of Run in code which supports contextual logging.
func (cnc *CloudNodeController) Run(stopCh <-chan struct{}, controllerManagerMetrics *controllersmetrics.ControllerManagerMetrics) {
	cnc.RunWithContext(wait.ContextForChannel(stopCh), controllerManagerMetrics)
}

// RunWithContext 会同步 informer 缓存并启动 worker：本控制器把云侧信息写入新注册的
// 节点。该调用会阻塞，因此应通过 goroutine 启动。
//
// 启动顺序很关键——先起事件管道与缓存同步，再启动状态刷新循环和
// 初始化 worker；否则 worker 可能读到空缓存，把尚不存在的节点误判为已删除。
func (cnc *CloudNodeController) RunWithContext(ctx context.Context, controllerManagerMetrics *controllersmetrics.ControllerManagerMetrics) {
	cnc.broadcaster = record.NewBroadcaster(record.WithContext(ctx))
	cnc.recorder = cnc.broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "cloud-node-controller"})
	stopCh := ctx.Done()

	defer utilruntime.HandleCrash()
	defer cnc.workqueue.ShutDown()

	// Start event processing pipeline.
	klog.Infof("Sending events to api server.")
	controllerManagerMetrics.ControllerStarted("cloud-node")
	defer controllerManagerMetrics.ControllerStopped("cloud-node")

	cnc.broadcaster.StartStructuredLogging(0)
	cnc.broadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: cnc.kubeClient.CoreV1().Events("")})
	defer cnc.broadcaster.Shutdown()

	// Wait for the caches to be synced before starting workers
	klog.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, cnc.nodesSynced); !ok {
		klog.Errorf("failed to wait for caches to sync")
		return
	}

	// updateNodeStatus 周期循环会定时轮询云提供商，对账节点地址与标签。
	// 该循环与初始化 worker 并行，但只处理无污点的节点，两者职责不重叠。
	go wait.UntilWithContext(ctx, func(ctx context.Context) {
		if err := cnc.UpdateNodeStatus(ctx); err != nil {
			klog.Errorf("failed to update node status: %v", err)
		}
	}, cnc.nodeStatusUpdateFrequency)

	// These workers initialize the nodes added to the cluster,
	// those that are Tainted with TaintExternalCloudProvider.
	for i := int32(0); i < cnc.workerCount; i++ {
		go wait.UntilWithContext(ctx, cnc.runWorker, time.Second)
	}

	<-stopCh
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// workqueue.
func (cnc *CloudNodeController) runWorker(ctx context.Context) {
	for cnc.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler.
func (cnc *CloudNodeController) processNextWorkItem(ctx context.Context) bool {
	obj, shutdown := cnc.workqueue.Get()
	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer cnc.workqueue.Done.
	err := func(key string) error {
		defer cnc.workqueue.Done(key)

		// Run the syncHandler, passing it the key of the
		// Node resource to be synced.
		if err := cnc.syncHandler(ctx, key); err != nil {
			// 云 API 抖动属于可恢复错误，限速重入队；成功则 Forget 清除退避历史。
			cnc.workqueue.AddRateLimited(key)
			klog.Infof("error syncing '%s': %v, requeuing", key, err)
			return fmt.Errorf("error syncing '%s': %s, requeuing", key, err.Error())
		}

		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		cnc.workqueue.Forget(key)
		return nil
	}(obj)

	if err != nil {
		utilruntime.HandleError(err)
		return true
	}

	return true
}

// syncHandler implements the logic of the controller.
func (cnc *CloudNodeController) syncHandler(ctx context.Context, key string) error {
	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		// 非法 key 重试也不会成功，直接丢弃并上报，避免无限循环。
		utilruntime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	return cnc.syncNode(ctx, name)
}

// UpdateNodeStatus 更新节点状态，例如节点地址。
//
// 每轮把 lister 中的全量节点按 statusUpdateWorkerCount 并行处理。
// 使用 DeepCopy 隔离并发写入，避免多个 goroutine 共享同一缓存对象。
func (cnc *CloudNodeController) UpdateNodeStatus(ctx context.Context) error {
	start := time.Now()
	nodes, err := cnc.nodesLister.List(labels.Everything())
	if err != nil {
		klog.Errorf("Error monitoring node status: %v", err)
		return err
	}
	defer func() {
		klog.V(2).Infof("Update %d nodes status took %v.", len(nodes), time.Since(start))
	}()

	updateNodeFunc := func(piece int) {
		node := nodes[piece].DeepCopy()
		// 带污点的节点尚未完成初始化，由 syncNode 负责；此处跳过可避免
		// 两个路径同时写 Node.Status 造成无谓冲突。
		cloudTaint := getCloudTaint(node.Spec.Taints)
		if cloudTaint != nil {
			klog.V(5).Infof("This node %s is still tainted. Will not process.", node.Name)
			return
		}

		instanceMetadata, err := cnc.getInstanceNodeAddresses(ctx, node)
		if err != nil {
			// 单个节点失败不影响其余节点；下个周期会再次尝试。
			klog.Errorf("Error getting instance metadata for node addresses: %v", err)
			return
		}

		cnc.updateNodeAddress(ctx, node, instanceMetadata)
	}

	workqueue.ParallelizeUntil(ctx, int(cnc.statusUpdateWorkerCount), len(nodes), updateNodeFunc)
	return nil
}

// enqueueNode takes a Node resource and converts it into a key
// string which is then put onto the work queue.
func (cnc *CloudNodeController) enqueueNode(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		utilruntime.HandleError(err)
		return
	}
	cnc.workqueue.Add(key)
}

// reconcileNodeLabels 对账 beta 标签向 GA 标签过渡期间的节点标签。
//
// 节点初始化之后不再走 syncNode，但历史节点可能存在 beta/GA 标签
// 不一致的情况（例如由旧版本 kubelet 创建），这里做一次幂等修补。
func (cnc *CloudNodeController) reconcileNodeLabels(nodeName string) error {
	node, err := cnc.nodeInformer.Lister().Get(nodeName)
	if err != nil {
		// If node not found, just ignore it.
		if apierrors.IsNotFound(err) {
			return nil
		}

		return err
	}

	if node.Labels == nil {
		// Nothing to reconcile.
		return nil
	}

	labelsToUpdate := map[string]string{}
	for _, r := range labelReconcileInfo {
		primaryValue, primaryExists := node.Labels[r.primaryKey]
		secondaryValue, secondaryExists := node.Labels[r.secondaryKey]

		if !primaryExists {
			// 主标签键不存在。只要没有外部组件或其他因素修改节点对象，在我们支持的
			// 版本偏斜范围内不应出现这种情况，这里直接忽略。缺少主标签时不反向推导，
			// 避免把外部写入的 GA 标签当成事实来源。
			continue
		}
		if secondaryExists && primaryValue != secondaryValue {
			// Secondary label exists, but not consistent with the primary
			// label. Need to reconcile.
			labelsToUpdate[r.secondaryKey] = primaryValue

		} else if !secondaryExists && r.ensureSecondaryExists {
			// Apply secondary label based on primary label.
			labelsToUpdate[r.secondaryKey] = primaryValue
		}
	}

	if len(labelsToUpdate) == 0 {
		return nil
	}

	// 使用带冲突重试的 patch，避免覆盖其他控制器同时写入的标签。
	if !cloudnodeutil.AddOrUpdateLabelsOnNode(cnc.kubeClient, labelsToUpdate, node) {
		return fmt.Errorf("failed update labels for node %+v", node)
	}

	return nil
}

// UpdateNodeAddress updates the nodeAddress of a single node
func (cnc *CloudNodeController) updateNodeAddress(ctx context.Context, node *v1.Node, instanceMetadata *cloudprovider.InstanceMetadata) {
	// Do not process nodes that are still tainted
	cloudTaint := getCloudTaint(node.Spec.Taints)
	if cloudTaint != nil {
		klog.V(5).Infof("This node %s is still tainted. Will not process.", node.Name)
		return
	}

	nodeAddresses := instanceMetadata.NodeAddresses
	if len(nodeAddresses) == 0 {
		// 云厂商返回空地址时保留 kubelet 上报的地址，避免把 Node 地址清空。
		klog.V(5).Infof("Skipping node address update for node %q since cloud provider did not return any", node.Name)
		return
	}

	// Check if a hostname address exists in the cloud provided addresses
	hostnameExists := false
	for i := range nodeAddresses {
		if nodeAddresses[i].Type == v1.NodeHostName {
			hostnameExists = true
			break
		}
	}
	// 云厂商返回的地址里没有 Hostname 时，沿用现有节点上由 kubelet 写入的值：
	// 多数云厂商不返回 Hostname，但许多工作负载依赖它。
	if !hostnameExists {
		for _, addr := range node.Status.Addresses {
			if addr.Type == v1.NodeHostName {
				nodeAddresses = append(nodeAddresses, addr)
			}
		}
	}
	// 如果 kubelet 提供了 node IP，则在节点地址列表中优先使用它：--node-ip 注解
	// 代表用户显式选择，优先级高于云侧枚举出的地址。
	nodeAddresses, err := updateNodeAddressesFromNodeIP(node, nodeAddresses)
	if err != nil {
		klog.Errorf("Failed to update node addresses for node %q: %v", node.Name, err)
		return
	}

	// 无变化时不发 patch，减少 apiserver 写压力与 watch 事件风暴。
	if !nodeAddressesChangeDetected(node.Status.Addresses, nodeAddresses) {
		return
	}
	newNode := node.DeepCopy()
	newNode.Status.Addresses = nodeAddresses
	// PatchNodeStatus 只提交 status 子资源，携带原对象作为基准以支持冲突检测。
	if _, _, err := nodeutil.PatchNodeStatus(cnc.kubeClient.CoreV1(), types.NodeName(node.Name), node, newNode); err != nil {
		klog.Errorf("Error patching node with cloud ip addresses = [%v]", err)
	}
}

// nodeModifier 用于在“冲突即重试”的循环中，把对节点对象的改动跨多次更新尝试携带过去。
//
// 把“要改什么”与“何时改”解耦。重试循环每次取最新对象再依次应用，
// 因此每个 modifier 必须是幂等的。
type nodeModifier func(*v1.Node)

// syncNode handles updating existing nodes registered with the cloud taint
// and processes nodes that were added into the cluster, and cloud initialize them if appropriate.
func (cnc *CloudNodeController) syncNode(ctx context.Context, nodeName string) error {
	curNode, err := cnc.nodeInformer.Lister().Get(nodeName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// 节点在入队后已被删除，属于正常竞态。
			return nil
		}

		return err
	}

	cloudTaint := getCloudTaint(curNode.Spec.Taints)
	if cloudTaint == nil {
		// Node object was already initialized, only need to reconcile the labels
		return cnc.reconcileNodeLabels(nodeName)
	}

	klog.Infof("Initializing node %s with cloud provider", nodeName)

	copyNode := curNode.DeepCopy()

	instanceMetadata, err := cnc.getInstanceMetadata(ctx, copyNode)
	if err != nil {
		return fmt.Errorf("failed to get instance metadata for node %s: %v", nodeName, err)
	}
	if instanceMetadata == nil {
		// 外部云提供商返回 nil instanceMetadata 时不做任何处理：nil 表示云侧暂无该
		// 实例信息（例如实例尚未注册），保留污点等下一轮再试。
		klog.Infof("Skip sync node %s because cloud provided nil metadata", nodeName)
		return nil
	}

	nodeModifiers, err := cnc.getNodeModifiersFromCloudProvider(ctx, copyNode, instanceMetadata)
	if err != nil {
		return fmt.Errorf("failed to get node modifiers from cloud provider: %v", err)
	}

	// 移除污点必须是最后一个 modifier：只有前面的写入都成功，节点才算真正初始化完成。
	nodeModifiers = append(nodeModifiers, func(n *v1.Node) {
		n.Spec.Taints = excludeCloudTaint(n.Spec.Taints)
	})

	err = clientretry.RetryOnConflict(UpdateNodeSpecBackoff, func() error {
		// 每次重试都重新读取最新对象，防止覆盖并发写入（例如 kubelet 更新状态）。
		curNode, err = cnc.nodeInformer.Lister().Get(nodeName)
		if err != nil {
			return err
		}

		newNode := curNode.DeepCopy()
		for _, modify := range nodeModifiers {
			modify(newNode)
		}

		_, err = cnc.kubeClient.CoreV1().Nodes().Update(ctx, newNode, metav1.UpdateOptions{})
		if err != nil {
			return err
		}

		// 观测“节点创建到移除污点”的时延，用于评估初始化流程的健康度。
		removeCloudProviderTaintDelay.Observe(time.Since(newNode.ObjectMeta.CreationTimestamp.Time).Seconds())

		// Update 成功后立即调用 updateNodeAddress 写入云提供商提供的 IP 地址，
		// 避免等待下一个 status 周期，缩短地址可用的空窗。
		cnc.updateNodeAddress(ctx, newNode, instanceMetadata)

		klog.Infof("Successfully initialized node %s with cloud provider", nodeName)
		return nil
	})
	if err != nil {
		return err
	}

	cnc.recorder.Event(copyNode, v1.EventTypeNormal, "Synced", "Node synced successfully")
	initialNodeSyncDelay.Observe(time.Since(curNode.ObjectMeta.CreationTimestamp.Time).Seconds())
	return nil
}

// getNodeModifiersFromCloudProvider returns a slice of nodeModifiers that update
// a node object with provider-specific information.
// All of the returned functions are idempotent, because they are used in a retry-if-conflict
// loop, meaning they could get called multiple times.
func (cnc *CloudNodeController) getNodeModifiersFromCloudProvider(
	ctx context.Context,
	node *v1.Node,
	instanceMeta *cloudprovider.InstanceMetadata,
) ([]nodeModifier, error) {

	var nodeModifiers []nodeModifier
	if node.Spec.ProviderID == "" {
		// providerID 一旦写入不可变，因此只在为空时设置，绝不覆盖 kubelet 已写入的值。
		if instanceMeta.ProviderID != "" {
			nodeModifiers = append(nodeModifiers, func(n *v1.Node) { n.Spec.ProviderID = instanceMeta.ProviderID })
		}
	}

	// 如果 kubelet 通过注解给节点指定了 node IP，那么在移除污点之前，必须先确认
	// 它是合法的、可以应用到探测出的节点地址上：先在移除污点前校验 --node-ip，
	// 校验失败则整次初始化失败并重试，避免节点带着一个云侧不存在的地址进入可用状态。
	_, err := updateNodeAddressesFromNodeIP(node, instanceMeta.NodeAddresses)
	if err != nil {
		return nil, fmt.Errorf("provided node ip for node %q is not valid: %w", node.Name, err)
	}

	// 同时写入 beta 与 GA 两套标签，兼容尚未迁移的调度器与工作负载。
	if instanceMeta.InstanceType != "" {
		klog.V(2).Infof("Adding node label from cloud provider: %s=%s", v1.LabelInstanceType, instanceMeta.InstanceType)
		klog.V(2).Infof("Adding node label from cloud provider: %s=%s", v1.LabelInstanceTypeStable, instanceMeta.InstanceType)
		nodeModifiers = append(nodeModifiers, func(n *v1.Node) {
			if n.Labels == nil {
				n.Labels = map[string]string{}
			}
			n.Labels[v1.LabelInstanceType] = instanceMeta.InstanceType
			n.Labels[v1.LabelInstanceTypeStable] = instanceMeta.InstanceType
		})
	}

	if instanceMeta.Zone != "" {
		klog.V(2).Infof("Adding node label from cloud provider: %s=%s", v1.LabelFailureDomainBetaZone, instanceMeta.Zone)
		klog.V(2).Infof("Adding node label from cloud provider: %s=%s", v1.LabelTopologyZone, instanceMeta.Zone)
		nodeModifiers = append(nodeModifiers, func(n *v1.Node) {
			if n.Labels == nil {
				n.Labels = map[string]string{}
			}
			n.Labels[v1.LabelFailureDomainBetaZone] = instanceMeta.Zone
			n.Labels[v1.LabelTopologyZone] = instanceMeta.Zone
		})
	}
	if instanceMeta.Region != "" {
		klog.V(2).Infof("Adding node label from cloud provider: %s=%s", v1.LabelFailureDomainBetaRegion, instanceMeta.Region)
		klog.V(2).Infof("Adding node label from cloud provider: %s=%s", v1.LabelTopologyRegion, instanceMeta.Region)
		nodeModifiers = append(nodeModifiers, func(n *v1.Node) {
			if n.Labels == nil {
				n.Labels = map[string]string{}
			}
			n.Labels[v1.LabelFailureDomainBetaRegion] = instanceMeta.Region
			n.Labels[v1.LabelTopologyRegion] = instanceMeta.Region
		})
	}

	if len(instanceMeta.AdditionalLabels) > 0 {
		klog.V(2).Infof("Adding additional node label(s) from cloud provider: %v", instanceMeta.AdditionalLabels)
		nodeModifiers = append(nodeModifiers, func(n *v1.Node) {
			if n.Labels == nil {
				n.Labels = map[string]string{}
			}

			k8sNamespaceRegex := regexp.MustCompile(`(^|\.)(kubernetes|k8s)\.io/`)
			for k, v := range instanceMeta.AdditionalLabels {
				// 云厂商不应使用 kubernetes 命名空间的标签：保留 k8s 命名空间给核心
				// 组件，防止云厂商覆盖语义标签（如拓扑、污点相关）。
				if isK8sNamespace := k8sNamespaceRegex.MatchString(k); isK8sNamespace {
					klog.Warningf("Discarding node label %s with kubernetes namespace", k)
					continue
				} else if originalVal, ok := n.Labels[k]; ok {
					// 已存在的标签一律不覆盖：用户或准入控制器写入的值优先级更高。
					if originalVal != v {
						klog.Warningf("Discarding node label %s that is already present", k)
					}
					continue
				}
				n.Labels[k] = v
			}
		})
	}

	return nodeModifiers, nil
}

// getInstanceMetadata get providerdID, instance type and nodeAddresses, use Instances if InstancesV2 is off.
// ProviderID is expected to be available, but to keep backward compatibility,
// we should handle some scenarios where it can be missing. It returns an error
// if providerID is missing, except when is not implemented by GetInstanceProviderID.
func (cnc *CloudNodeController) getInstanceMetadata(ctx context.Context, node *v1.Node) (*cloudprovider.InstanceMetadata, error) {
	// kubelet can set the provider ID using the flag and is inmutable
	providerID := node.Spec.ProviderID
	// InstancesV2 require ProviderID to be present
	if instancesV2, ok := cnc.cloud.InstancesV2(); instancesV2 != nil && ok {
		metadata, err := instancesV2.InstanceMetadata(ctx, node)
		if err != nil {
			return nil, err
		}
		// spec.ProviderID 是多个控制器（如 loadbalancer）必需的字段，因此在它被设置
		// 好之前不能移除节点污点。该字段一旦写入即不可变，无需再做对账；而这里只在
		// 初始化阶段设置一次、之后不再对账，所以此刻若拿不到值，该实例以后再也无法
		// 获得它。在外部云提供商成为默认方式之前，该字段由 kubelet 写入，节点创建时
		// 就带有该值。
		// xref: https://issues.k8s.io/123024
		// 云厂商未返回 ProviderID 时回退到 kubelet 已写入的值；两者都缺失则
		// 返回错误，让节点保持污点状态重试——宁可不初始化，也不能让后续控制器拿到空 ID。
		if metadata != nil && metadata.ProviderID == "" {
			if providerID == "" {
				return metadata, fmt.Errorf("cloud provider does not set node provider ID for node %s", node.Name)
			}
			metadata.ProviderID = providerID
		}
		return metadata, nil
	}

	// If InstancesV2 not implement, use Instances.
	instances, ok := cnc.cloud.Instances()
	if !ok {
		return nil, fmt.Errorf("failed to get instances from cloud provider")
	}

	var err error
	if providerID == "" {
		providerID, err = cloudprovider.GetInstanceProviderID(ctx, cnc.cloud, types.NodeName(node.Name))
		if err != nil {
			// This is the only case where ProviderID can be skipped
			if errors.Is(err, cloudprovider.NotImplemented) {
				// if the cloud provider being used does not support provider IDs,
				// we can safely continue since we will attempt to set node
				// addresses given the node name in getNodeAddressesByProviderIDOrName
				klog.Warningf("cloud provider does not set node provider ID, using node name to discover node %s", node.Name)
			} else {
				// 云厂商支持 provider ID 时要把错误上抛，以便后续重新入队重试；
				// 否则污点被移除后 ProviderID 将永远缺失。
				return nil, err
			}
		}
	}

	nodeAddresses, err := getNodeAddressesByProviderIDOrName(ctx, instances, providerID, node.Name)
	if err != nil {
		return nil, err
	}
	instanceType, err := getInstanceTypeByProviderIDOrName(ctx, instances, providerID, node.Name)
	if err != nil {
		return nil, err
	}

	instanceMetadata := &cloudprovider.InstanceMetadata{
		ProviderID:    providerID,
		InstanceType:  instanceType,
		NodeAddresses: nodeAddresses,
	}

	zones, ok := cnc.cloud.Zones()
	if !ok {
		// 未实现 Zones 的云厂商不产生区域/可用区标签，属正常情况。
		return instanceMetadata, nil
	}

	zone, err := getZoneByProviderIDOrName(ctx, zones, providerID, node.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to get zone from cloud provider: %v", err)
	}

	if zone.FailureDomain != "" {
		instanceMetadata.Zone = zone.FailureDomain
	}

	if zone.Region != "" {
		instanceMetadata.Region = zone.Region
	}

	return instanceMetadata, nil
}

// getInstanceAddresses 返回 InstanceMetadata.NodeAddresses。若不支持 InstancesV2，
// 就不会去取 instanceType，相比 getInstanceMetadata 少一次云 API 调用。
//
// 周期性刷新只需要地址，因此省略实例类型与区域查询，把每轮云 API 调用降到最少。
func (cnc *CloudNodeController) getInstanceNodeAddresses(ctx context.Context, node *v1.Node) (*cloudprovider.InstanceMetadata, error) {
	if instancesV2, ok := cnc.cloud.InstancesV2(); instancesV2 != nil && ok {
		return instancesV2.InstanceMetadata(ctx, node)
	}

	// If InstancesV2 not implement, use Instances.
	instances, ok := cnc.cloud.Instances()
	if !ok {
		return nil, fmt.Errorf("failed to get instances from cloud provider")
	}
	nodeAddresses, err := getNodeAddressesByProviderIDOrName(ctx, instances, node.Spec.ProviderID, node.Name)
	if err != nil {
		return nil, err
	}

	return &cloudprovider.InstanceMetadata{
		NodeAddresses: nodeAddresses,
	}, nil
}

// getCloudTaint 返回节点上的“未初始化”污点，不存在时返回 nil。
// 返回指针副本而非切片元素地址，避免调用方持有 lister 缓存对象内部指针。
func getCloudTaint(taints []v1.Taint) *v1.Taint {
	for _, taint := range taints {
		if taint.Key == cloudproviderapi.TaintExternalCloudProvider {
			return &taint
		}
	}
	return nil
}

// excludeCloudTaint 返回去掉“未初始化”污点后的切片。
// 总是构造新切片，不原地修改入参，保证基于缓存对象的拷贝语义安全。
func excludeCloudTaint(taints []v1.Taint) []v1.Taint {
	newTaints := []v1.Taint{}
	for _, taint := range taints {
		if taint.Key == cloudproviderapi.TaintExternalCloudProvider {
			continue
		}
		newTaints = append(newTaints, taint)
	}
	return newTaints
}

// getNodeAddressesByProviderIDOrName 优先按 providerID 查询地址，失败后回退到节点名。
// 回退兼容不支持 providerID 的旧云厂商；两次都失败时把两个错误都返回，
// 便于区分“云 API 故障”与“实例不存在”。
func getNodeAddressesByProviderIDOrName(ctx context.Context, instances cloudprovider.Instances, providerID, nodeName string) ([]v1.NodeAddress, error) {
	nodeAddresses, err := instances.NodeAddressesByProviderID(ctx, providerID)
	if err != nil {
		providerIDErr := err
		nodeAddresses, err = instances.NodeAddresses(ctx, types.NodeName(nodeName))
		if err != nil {
			return nil, fmt.Errorf("error fetching node by provider ID: %v, and error by node name: %v", providerIDErr, err)
		}
	}
	return nodeAddresses, nil
}

// nodeAddressesChangeDetected 比较两组地址是否不同。
// 按类型建立映射后比较，忽略顺序差异——云厂商返回地址顺序并不稳定，
// 若按顺序比较会导致每轮都触发无意义的 patch。
func nodeAddressesChangeDetected(addressSet1, addressSet2 []v1.NodeAddress) bool {
	if len(addressSet1) != len(addressSet2) {
		return true
	}
	addressMap1 := map[v1.NodeAddressType]string{}

	for i := range addressSet1 {
		addressMap1[addressSet1[i].Type] = addressSet1[i].Address
	}

	for _, v := range addressSet2 {
		if addressMap1[v.Type] != v.Address {
			return true
		}
	}
	return false
}

// updateNodeAddressesFromNodeIP 在节点带有 --node-ip 注解时，用该 IP 约束云侧地址列表。
// 注解不存在时原样返回，保持与旧集群的兼容行为。
func updateNodeAddressesFromNodeIP(node *v1.Node, nodeAddresses []v1.NodeAddress) ([]v1.NodeAddress, error) {
	var err error

	providedNodeIP, exists := node.ObjectMeta.Annotations[cloudproviderapi.AnnotationAlphaProvidedIPAddr]
	if exists {
		nodeAddresses, err = cloudnodeutil.GetNodeAddressesFromNodeIP(providedNodeIP, nodeAddresses)
	}

	return nodeAddresses, err
}

// getInstanceTypeByProviderIDOrName will attempt to get the instance type of node using its providerID
// then it's name. If both attempts fail, an error is returned.
func getInstanceTypeByProviderIDOrName(ctx context.Context, instances cloudprovider.Instances, providerID, nodeName string) (string, error) {
	instanceType, err := instances.InstanceTypeByProviderID(ctx, providerID)
	if err != nil {
		providerIDErr := err
		instanceType, err = instances.InstanceType(ctx, types.NodeName(nodeName))
		if err != nil {
			return "", fmt.Errorf("InstanceType: Error fetching by providerID: %v Error fetching by NodeName: %v", providerIDErr, err)
		}
	}
	return instanceType, err
}

// getZoneByProviderIDorName will attempt to get the zone of node using its providerID
// then it's name. If both attempts fail, an error is returned.
func getZoneByProviderIDOrName(ctx context.Context, zones cloudprovider.Zones, providerID, nodeName string) (cloudprovider.Zone, error) {
	zone, err := zones.GetZoneByProviderID(ctx, providerID)
	if err != nil {
		providerIDErr := err
		zone, err = zones.GetZoneByNodeName(ctx, types.NodeName(nodeName))
		if err != nil {
			return cloudprovider.Zone{}, fmt.Errorf("Zone: Error fetching by providerID: %v Error fetching by NodeName: %v", providerIDErr, err)
		}
	}

	return zone, nil
}
