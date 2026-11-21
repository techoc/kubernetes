/*
Copyright 2017 The Kubernetes Authors.

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

// Package kubemark 提供 Kubemark 测试框架的核心控制器和 Hollow Kubelet 实现。
// Kubemark 是一种用于测试 Kubernetes 集群大规模能力的工具，它通过在外部集群中运行
// "空心节点"（hollow nodes）来模拟大量节点，从而测试 API Server 等组件在大规模场景下的表现。
//
// 核心概念：
//   - 外部集群（external cluster）：真实运行的 Kubernetes 集群，用于托管 kubemark 的 ReplicationController 和 Pods
//   - Kubemark 集群（kubemark cluster）：由空心节点组成的虚拟集群，用于模拟大规模节点场景
//   - 空心节点（hollow node）：在外部集群中以 Pod 形式运行、在 kubemark 集群中注册为 Node 的轻量级节点
package kubemark

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	informersv1 "k8s.io/client-go/informers/core/v1"
	kubeclient "k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"

	"k8s.io/klog/v2"
)

const (
	// namespaceKubemark 是 kubemark 相关资源（RC、Pod）所在的命名空间
	namespaceKubemark = "kubemark"
	// nodeGroupLabel 用于标记节点所属的节点组（node group），支持按组管理节点扩缩容
	nodeGroupLabel = "autoscaling.k8s.io/nodegroup"
	// numRetries 是创建/删除操作失败时的重试次数
	numRetries = 3
)

// KubemarkController 是 kubemark 的简化版云控制器，负责管理 kubemark 集群中节点的增删。
// 它通过在外部集群中操作 ReplicationController 来创建和删除空心节点，并通过 label 实现节点组（nodegroup）的概念。
type KubemarkController struct {
	// nodeTemplate 是创建新空心节点时使用的 ReplicationController 模板，从已有的空心节点中获取
	nodeTemplate *apiv1.ReplicationController
	// externalCluster 用于与外部集群交互，操作 RC 和 Pod
	externalCluster externalCluster
	// kubemarkCluster 用于与 kubemark 集群交互，主要是在节点变为 NotReady 后删除节点
	kubemarkCluster kubemarkCluster
	// rand 用于生成节点名称中的随机数，避免名称冲突
	rand *rand.Rand
	// createNodeQueue 是异步创建节点的队列，容量为 1000，避免阻塞调用方
	createNodeQueue chan string
	// nodeGroupQueueSize 记录每个节点组中等待创建的节点数量（尚未实际创建）
	nodeGroupQueueSize map[string]int
	// nodeGroupQueueSizeLock 保护 nodeGroupQueueSize 的并发访问
	nodeGroupQueueSizeLock sync.Mutex
}

// externalCluster 封装了与外部集群的交互逻辑。
// 外部集群是实际运行 kubemark 的 Kubernetes 集群，通过操作 RC 来管理空心节点的创建和删除。
type externalCluster struct {
	// rcLister 用于从本地缓存中列出 ReplicationController
	rcLister listersv1.ReplicationControllerLister
	// rcSynced 检查 RC informer 是否已完成初始同步
	rcSynced cache.InformerSynced
	// podLister 用于从本地缓存中列出 Pod
	podLister listersv1.PodLister
	// podSynced 检查 Pod informer 是否已完成初始同步
	podSynced cache.InformerSynced
	// client 是外部集群的 API 客户端，用于执行写操作（创建、删除等）
	client kubeclient.Interface
}

// kubemarkCluster 封装了与 kubemark 集群的交互逻辑。
// 由于 kubemark 没有真正的云提供商来自动清理节点，此结构体负责在 RC 被删除且节点变为 NotReady 后
// 显式删除 kubemark 集群中的节点。
type kubemarkCluster struct {
	// client 是 kubemark 集群的 API 客户端
	client kubeclient.Interface
	// nodeLister 用于从本地缓存中列出 Node
	nodeLister listersv1.NodeLister
	// nodeSynced 检查 Node informer 是否已完成初始同步
	nodeSynced cache.InformerSynced
	// nodesToDelete 记录需要删除的节点名称，当节点变为 NotReady 时执行实际删除
	nodesToDelete map[string]bool
	// nodesToDeleteLock 保护 nodesToDelete 的并发访问
	nodesToDeleteLock sync.Mutex
}

// NewKubemarkController 创建 KubemarkController 实例，使用给定的客户端与外部集群和 kubemark 集群通信。
//
// 参数说明：
//   - externalClient: 外部集群的 API 客户端，用于操作 RC 和 Pod
//   - externalInformerFactory: 外部集群的 informer 工厂，用于创建 RC 和 Pod 的 informer
//   - kubemarkClient: kubemark 集群的 API 客户端，用于删除节点
//   - kubemarkNodeInformer: kubemark 集群的 Node informer，用于监听节点状态变化
func NewKubemarkController(externalClient kubeclient.Interface, externalInformerFactory informers.SharedInformerFactory,
	kubemarkClient kubeclient.Interface, kubemarkNodeInformer informersv1.NodeInformer) (*KubemarkController, error) {
	// 为外部集群创建 RC informer，用于监听 kubemark 命名空间中的 ReplicationController 变化
	rcInformer := externalInformerFactory.InformerFor(&apiv1.ReplicationController{}, newReplicationControllerInformer)
	// 为外部集群创建 Pod informer，用于监听 kubemark 命名空间中的 Pod 变化
	podInformer := externalInformerFactory.InformerFor(&apiv1.Pod{}, newPodInformer)

	// 构建控制器实例，初始化外部集群和 kubemark 集群的相关组件
	controller := &KubemarkController{
		externalCluster: externalCluster{
			// 从 informer 的 indexer 创建 lister，用于高效查询本地缓存
			rcLister:  listersv1.NewReplicationControllerLister(rcInformer.GetIndexer()),
			rcSynced:  rcInformer.HasSynced,
			podLister: listersv1.NewPodLister(podInformer.GetIndexer()),
			podSynced: podInformer.HasSynced,
			client:    externalClient,
		},
		kubemarkCluster: kubemarkCluster{
			nodeLister:        kubemarkNodeInformer.Lister(),
			nodeSynced:        kubemarkNodeInformer.Informer().HasSynced,
			client:            kubemarkClient,
			nodesToDelete:     make(map[string]bool),
			nodesToDeleteLock: sync.Mutex{},
		},
		// 使用当前时间戳作为随机种子，确保每次启动生成的随机数序列不同
		rand: rand.New(rand.NewSource(time.Now().UnixNano())),
		// 创建有缓冲的 channel，容量 1000，用于异步创建节点请求
		createNodeQueue:        make(chan string, 1000),
		nodeGroupQueueSize:     make(map[string]int),
		nodeGroupQueueSizeLock: sync.Mutex{},
	}

	// 为 kubemark 集群的 Node informer 注册事件处理器
	// 当节点状态更新时，检查是否需要删除该节点（用于清理已标记为删除且变为 NotReady 的节点）
	kubemarkNodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: controller.kubemarkCluster.removeUnneededNodes,
	})

	return controller, nil
}

// WaitForCacheSync 等待控制器中的所有 informer 缓存完成初始同步。
// 在开始处理请求之前调用，确保 lister 返回的数据是完整的。
func (kubemarkController *KubemarkController) WaitForCacheSync(stopCh chan struct{}) bool {
	return cache.WaitForNamedCacheSync("kubemark", stopCh,
		kubemarkController.externalCluster.rcSynced,
		kubemarkController.externalCluster.podSynced,
		kubemarkController.kubemarkCluster.nodeSynced)
}

// Run 启动控制器：加载节点模板并启动异步节点创建工作协程。
// 此方法会阻塞直到 stopCh 收到停止信号。
func (kubemarkController *KubemarkController) Run(stopCh chan struct{}) {
	// 从现有的空心节点中获取 RC 模板，用于后续创建新节点
	// 要求启动时至少有一个 kubemark 节点存在
	nodeTemplate, err := kubemarkController.getNodeTemplate()
	if err != nil {
		klog.Fatalf("failed to get node template: %s", err)
	}
	kubemarkController.nodeTemplate = nodeTemplate

	// 启动后台协程，持续从 createNodeQueue 中消费节点创建请求
	go kubemarkController.runNodeCreation(stopCh)
	// 阻塞等待停止信号
	<-stopCh
}

// GetNodeNamesForNodeGroup 返回指定节点组中所有节点的名称列表。
// 通过查询外部集群中带有对应 nodeGroupLabel 的 Pod 来获取。
func (kubemarkController *KubemarkController) GetNodeNamesForNodeGroup(nodeGroup string) ([]string, error) {
	// 构建 label 选择器，匹配指定节点组的 Pod
	selector := labels.SelectorFromSet(labels.Set{nodeGroupLabel: nodeGroup})
	pods, err := kubemarkController.externalCluster.podLister.List(selector)
	if err != nil {
		return nil, err
	}
	// 提取 Pod 名称作为节点名称
	result := make([]string, 0, len(pods))
	for _, pod := range pods {
		result = append(result, pod.ObjectMeta.Name)
	}
	return result, nil
}

// GetNodeGroupSize 返回指定节点组的当前实际大小（已存在的 RC 数量）。
func (kubemarkController *KubemarkController) GetNodeGroupSize(nodeGroup string) (int, error) {
	// 通过 label 选择器列出属于该节点组的所有 RC
	selector := labels.SelectorFromSet(labels.Set(map[string]string{nodeGroupLabel: nodeGroup}))
	nodes, err := kubemarkController.externalCluster.rcLister.List(selector)
	if err != nil {
		return 0, err
	}
	return len(nodes), nil
}

// GetNodeGroupTargetSize 返回节点组的目标大小，即当前实际大小加上排队等待创建的节点数。
// 用于判断是否需要继续扩容或缩容。
func (kubemarkController *KubemarkController) GetNodeGroupTargetSize(nodeGroup string) (int, error) {
	kubemarkController.nodeGroupQueueSizeLock.Lock()
	defer kubemarkController.nodeGroupQueueSizeLock.Unlock()
	// 获取当前实际存在的节点数
	realSize, err := kubemarkController.GetNodeGroupSize(nodeGroup)
	if err != nil {
		return realSize, err
	}
	// 加上排队中尚未创建的节点数
	return realSize + kubemarkController.nodeGroupQueueSize[nodeGroup], nil
}

// SetNodeGroupSize 通过添加或移除节点来调整节点组的大小。
// 如果目标大小小于当前大小，则删除多余的节点；反之则创建新节点。
func (kubemarkController *KubemarkController) SetNodeGroupSize(nodeGroup string, size int) error {
	// 获取目标大小（包含排队中的节点）
	currSize, err := kubemarkController.GetNodeGroupTargetSize(nodeGroup)
	if err != nil {
		return err
	}
	switch delta := size - currSize; {
	case delta < 0:
		// 缩容场景：需要删除节点
		absDelta := -delta
		nodes, err := kubemarkController.GetNodeNamesForNodeGroup(nodeGroup)
		if err != nil {
			return err
		}
		// 检查是否有足够的节点可以删除
		if len(nodes) < absDelta {
			return fmt.Errorf("can't remove %d nodes from %s nodegroup, not enough nodes: %d", absDelta, nodeGroup, len(nodes))
		}
		// 逐个删除节点，直到达到目标大小
		for i, node := range nodes {
			if i == absDelta {
				return nil
			}
			if err := kubemarkController.RemoveNodeFromNodeGroup(nodeGroup, node); err != nil {
				return err
			}
		}
	case delta > 0:
		// 扩容场景：需要创建新节点
		// 先更新排队计数，再向队列发送创建请求
		kubemarkController.nodeGroupQueueSizeLock.Lock()
		kubemarkController.nodeGroupQueueSize[nodeGroup] += delta
		kubemarkController.nodeGroupQueueSizeLock.Unlock()
		// 将创建请求放入异步队列，由 runNodeCreation 协程处理
		for i := 0; i < delta; i++ {
			kubemarkController.createNodeQueue <- nodeGroup
		}
	}

	return nil
}

// GetNodeGroupForNode 返回指定节点所属的节点组名称。
// 通过查找对应 Pod 的 nodeGroupLabel 来获取。
func (kubemarkController *KubemarkController) GetNodeGroupForNode(node string) (string, error) {
	pod := kubemarkController.getPodByName(node)
	if pod == nil {
		return "", fmt.Errorf("node %s does not exist", node)
	}
	nodeGroup, ok := pod.ObjectMeta.Labels[nodeGroupLabel]
	if ok {
		return nodeGroup, nil
	}
	return "", fmt.Errorf("can't find nodegroup for node %s due to missing label %s", node, nodeGroupLabel)
}

// addNodeToNodeGroup 向指定节点组添加一个新节点。
// 基于 nodeTemplate 创建一个新的 RC，并设置对应的 label 和随机名称。
func (kubemarkController *KubemarkController) addNodeToNodeGroup(nodeGroup string) error {
	// 深拷贝节点模板，避免修改原始模板
	node := kubemarkController.nodeTemplate.DeepCopy()
	// 生成节点名称：节点组名-随机数
	node.Name = fmt.Sprintf("%s-%d", nodeGroup, kubemarkController.rand.Int63())
	// 设置 label：节点组标识和节点名称
	node.Labels = map[string]string{nodeGroupLabel: nodeGroup, "name": node.Name}
	// 将 label 同步到 Pod 模板，确保创建的 Pod 也带有相同的 label
	node.Spec.Template.Labels = node.Labels

	// 重试创建 RC，最多 numRetries 次
	var err error
	for i := 0; i < numRetries; i++ {
		_, err = kubemarkController.externalCluster.client.CoreV1().ReplicationControllers(node.Namespace).Create(context.TODO(), node, metav1.CreateOptions{})
		if err == nil {
			return nil
		}
	}

	return err
}

// RemoveNodeFromNodeGroup 从指定节点组中移除一个节点。
// 先验证节点存在且属于该节点组，然后删除对应的 RC 并标记节点以待后续清理。
func (kubemarkController *KubemarkController) RemoveNodeFromNodeGroup(nodeGroup string, node string) error {
	// 查找对应的 Pod
	pod := kubemarkController.getPodByName(node)
	if pod == nil {
		// 节点不存在则静默返回，不报错
		klog.Warningf("Can't delete node %s from nodegroup %s. Node does not exist.", node, nodeGroup)
		return nil
	}
	// 验证节点确实属于目标节点组
	if pod.ObjectMeta.Labels[nodeGroupLabel] != nodeGroup {
		return fmt.Errorf("can't delete node %s from nodegroup %s. Node is not in nodegroup", node, nodeGroup)
	}
	// 使用 Foreground 级联删除策略，确保 RC 删除时其管理的 Pod 也被删除
	policy := metav1.DeletePropagationForeground
	var err error
	for i := 0; i < numRetries; i++ {
		// 删除外部集群中的 RC
		err = kubemarkController.externalCluster.client.CoreV1().ReplicationControllers(namespaceKubemark).Delete(context.TODO(), pod.ObjectMeta.Labels["name"],
			metav1.DeleteOptions{PropagationPolicy: &policy})
		if err == nil {
			klog.Infof("marking node %s for deletion", node)
			// 标记节点以待删除：由于 kubemark 没有真正的云提供商来自动清理节点，
			// 需要在 RC 删除后、节点变为 NotReady 时手动删除 kubemark 集群中的节点
			kubemarkController.kubemarkCluster.markNodeForDeletion(node)
			return nil
		}
	}
	return fmt.Errorf("Failed to delete node %s: %v", node, err)
}

// getReplicationControllerByName 从外部集群的缓存中按名称查找 RC。
func (kubemarkController *KubemarkController) getReplicationControllerByName(name string) *apiv1.ReplicationController {
	// 列出所有 RC
	rcs, err := kubemarkController.externalCluster.rcLister.List(labels.Everything())
	if err != nil {
		return nil
	}
	// 线性查找匹配名称的 RC
	for _, rc := range rcs {
		if rc.ObjectMeta.Name == name {
			return rc
		}
	}
	return nil
}

// getPodByName 从外部集群的缓存中按名称查找 Pod。
func (kubemarkController *KubemarkController) getPodByName(name string) *apiv1.Pod {
	// 列出所有 Pod
	pods, err := kubemarkController.externalCluster.podLister.List(labels.Everything())
	if err != nil {
		return nil
	}
	// 线性查找匹配名称的 Pod
	for _, pod := range pods {
		if pod.ObjectMeta.Name == name {
			return pod
		}
	}
	return nil
}

// getNodeNameForPod 根据 Pod 名称获取其对应的节点名称（从 Pod 的 "name" label 中读取）。
func (kubemarkController *KubemarkController) getNodeNameForPod(podName string) (string, error) {
	// 列出所有 Pod
	pods, err := kubemarkController.externalCluster.podLister.List(labels.Everything())
	if err != nil {
		return "", err
	}
	// 查找匹配的 Pod 并返回其 "name" label
	for _, pod := range pods {
		if pod.ObjectMeta.Name == podName {
			return pod.Labels["name"], nil
		}
	}
	return "", fmt.Errorf("pod %s not found", podName)
}

// getNodeTemplate 从现有的空心节点中获取 RC 模板，用于后续创建新节点。
// 要求启动时至少有一个 kubemark 节点存在，否则无法获取模板。
func (kubemarkController *KubemarkController) getNodeTemplate() (*apiv1.ReplicationController, error) {
	// 从 kubemark 集群中获取一个空心节点的 Pod 名称
	podName, err := kubemarkController.kubemarkCluster.getHollowNodeName()
	if err != nil {
		return nil, err
	}
	// 根据 Pod 名称获取对应的节点名称（"name" label）
	hollowNodeName, err := kubemarkController.getNodeNameForPod(podName)
	if err != nil {
		return nil, err
	}
	// 查找对应的 RC 并提取其 Pod 模板
	if hollowNode := kubemarkController.getReplicationControllerByName(hollowNodeName); hollowNode != nil {
		nodeTemplate := &apiv1.ReplicationController{
			Spec: apiv1.ReplicationControllerSpec{
				// 复用原 RC 的 Pod 模板
				Template: hollowNode.Spec.Template,
			},
		}

		// 清空 selector，因为后续会设置新的 label
		nodeTemplate.Spec.Selector = nil
		// 设置命名空间为 kubemark
		nodeTemplate.Namespace = namespaceKubemark
		// 每个空心节点对应一个 RC，副本数为 1
		one := int32(1)
		nodeTemplate.Spec.Replicas = &one

		return nodeTemplate, nil
	}
	return nil, fmt.Errorf("can't get hollow node template")
}

// runNodeCreation 是后台工作协程，持续从 createNodeQueue 中消费节点创建请求。
// 每个请求对应一个节点组名称，调用 addNodeToNodeGroup 创建新节点。
func (kubemarkController *KubemarkController) runNodeCreation(stop <-chan struct{}) {
	for {
		select {
		case nodeGroup := <-kubemarkController.createNodeQueue:
			// 加锁保护排队计数
			kubemarkController.nodeGroupQueueSizeLock.Lock()
			err := kubemarkController.addNodeToNodeGroup(nodeGroup)
			if err != nil {
				// 创建失败时保留排队计数，以便后续重试
				klog.Errorf("failed to add node to node group %s: %v", nodeGroup, err)
			} else {
				// 创建成功则减少排队计数
				kubemarkController.nodeGroupQueueSize[nodeGroup]--
			}
			kubemarkController.nodeGroupQueueSizeLock.Unlock()
		case <-stop:
			// 收到停止信号，退出循环
			return
		}
	}
}

// getHollowNodeName 从 kubemark 集群中获取一个空心节点的名称。
// 用于初始化时获取节点模板。
func (kubemarkCluster *kubemarkCluster) getHollowNodeName() (string, error) {
	// 解析 nodeGroupLabel 作为选择器，查找带有该 label 的节点
	selector, _ := labels.Parse(nodeGroupLabel)
	nodes, err := kubemarkCluster.nodeLister.List(selector)
	if err != nil {
		return "", err
	}
	// 返回第一个匹配的节点名称
	for _, node := range nodes {
		return node.Name, nil
	}
	return "", fmt.Errorf("did not find any hollow nodes in the cluster")
}

// removeUnneededNodes 是 Node informer 的 Update 事件处理器。
// 当节点状态更新时，检查该节点是否已标记为删除且变为 NotReady，如果是则执行实际删除。
// 这种延迟删除策略确保在 RC 删除后、节点真正不可用时才清理 kubemark 集群中的节点。
func (kubemarkCluster *kubemarkCluster) removeUnneededNodes(oldObj interface{}, newObj interface{}) {
	// 类型断言，确保新对象是 Node 类型
	node, ok := newObj.(*apiv1.Node)
	if !ok {
		return
	}
	// 遍历节点条件，查找 Ready 状态
	for _, condition := range node.Status.Conditions {
		// 如果节点 NotReady 且已被标记为删除，则执行实际删除
		if condition.Type == apiv1.NodeReady && condition.Status != apiv1.ConditionTrue {
			kubemarkCluster.nodesToDeleteLock.Lock()
			defer kubemarkCluster.nodesToDeleteLock.Unlock()
			if kubemarkCluster.nodesToDelete[node.Name] {
				// 清除删除标记
				kubemarkCluster.nodesToDelete[node.Name] = false
				// 从 kubemark 集群中删除该节点
				if err := kubemarkCluster.client.CoreV1().Nodes().Delete(context.TODO(), node.Name, metav1.DeleteOptions{}); err != nil {
					klog.Errorf("failed to delete node %s from kubemark cluster, err: %v", node.Name, err)
				}
			}
			// 找到 Ready 条件后即可返回，无需继续遍历
			return
		}
	}
}

// markNodeForDeletion 将节点标记为待删除。
// 实际删除会在节点变为 NotReady 时由 removeUnneededNodes 执行。
func (kubemarkCluster *kubemarkCluster) markNodeForDeletion(name string) {
	kubemarkCluster.nodesToDeleteLock.Lock()
	defer kubemarkCluster.nodesToDeleteLock.Unlock()
	kubemarkCluster.nodesToDelete[name] = true
}

// newReplicationControllerInformer 创建 RC 的 SharedIndexInformer。
// 用于监听 kubemark 命名空间中的 ReplicationController 变化。
func newReplicationControllerInformer(kubeClient kubeclient.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	// 创建 ListWatch，监听 kubemark 命名空间下的 RC
	rcListWatch := cache.NewListWatchFromClient(kubeClient.CoreV1().RESTClient(), "replicationcontrollers", namespaceKubemark, fields.Everything())
	return cache.NewSharedIndexInformer(rcListWatch, &apiv1.ReplicationController{}, resyncPeriod, nil)
}

// newPodInformer 创建 Pod 的 SharedIndexInformer。
// 用于监听 kubemark 命名空间中的 Pod 变化。
func newPodInformer(kubeClient kubeclient.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	// 创建 ListWatch，监听 kubemark 命名空间下的 Pod
	podListWatch := cache.NewListWatchFromClient(kubeClient.CoreV1().RESTClient(), "pods", namespaceKubemark, fields.Everything())
	return cache.NewSharedIndexInformer(podListWatch, &apiv1.Pod{}, resyncPeriod, nil)
}
