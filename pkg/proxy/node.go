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

package proxy

import (
	"context"
	"fmt"
	"net"
	"os"
	"reflect"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	v1informers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	kubeproxyconfig "k8s.io/kubernetes/pkg/proxy/apis/config"
	utilnode "k8s.io/kubernetes/pkg/util/node"
)

// NodeManager 根据 NodeIPs 和 PodCIDRs 管理 kube-proxy 的生命周期：
// 它监听 Node 变更事件，并在 NodeIPs 或 PodCIDRs 发生变化时让 kube-proxy 退出重启，
// 以便重新初始化依赖这些地址的规则。
// 注意：只有当 watchPodCIDRs 为 true 时，PodCIDR 的变化才会触发退出。
type NodeManager struct {
	nodeInformer  v1informers.NodeInformer
	nodeLister    corelisters.NodeLister
	exitFunc      func(exitCode int)
	watchPodCIDRs bool

	// 以下字段在构造完成后不再改变，仅在重启进程时重新获取。
	nodeIPs  []net.IP
	podCIDRs []string

	mu   sync.Mutex
	node *v1.Node
}

// NewNodeManager 初始化一个只监听指定 Node 的 informer，等待缓存同步，
// 并在等待 Node 对象出现且具备 NodeIPs（当 watchPodCIDRs 为 true 时还包括 PodCIDRs）后
// 返回 NodeManager。
// 注意：为保持向后兼容，当 watchPodCIDRs 为 false 时，即使未能获取 NodeIPs 也不会返回错误。
func NewNodeManager(ctx context.Context, client clientset.Interface,
	nodeName string, config *kubeproxyconfig.KubeProxyConfiguration,
) (*NodeManager, error) {
	resyncInterval := config.ConfigSyncPeriod.Duration
	watchPodCIDRs := config.DetectLocalMode == kubeproxyconfig.LocalModeNodeCIDR
	return newNodeManager(ctx, client, resyncInterval, nodeName, watchPodCIDRs, os.Exit, time.Second, 30*time.Second, 5*time.Minute)
}

// newNodeManager 是 NewNodeManager 的实现，允许注入退出函数、轮询间隔和超时时间，便于测试。
func newNodeManager(ctx context.Context, client clientset.Interface, resyncInterval time.Duration,
	nodeName string, watchPodCIDRs bool, exitFunc func(int),
	pollInterval, nodeIPsTimeout, podCIDRsTimeout time.Duration,
) (*NodeManager, error) {
	// 创建一个只选择指定 Node 的 informer，避免缓存集群中所有节点带来的内存开销。
	thisNodeInformerFactory := informers.NewSharedInformerFactoryWithOptions(client, resyncInterval,
		informers.WithTransform(func(obj interface{}) (interface{}, error) {
			// kube-proxy 从不需要 managedFields 元数据，因此在此处剥离，
			// 以降低缓存内存占用。
			if accessor, err := meta.Accessor(obj); err == nil {
				accessor.SetManagedFields(nil)
			}
			return obj, nil
		}),
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.FieldSelector = fields.OneTermEqualSelector("metadata.name", nodeName).String()
		}))
	nodeInformer := thisNodeInformerFactory.Core().V1().Nodes()
	nodeLister := nodeInformer.Lister()

	// 启动 informer 并等待其缓存完成同步。
	thisNodeInformerFactory.Start(wait.NeverStop)
	if !cache.WaitForNamedCacheSyncWithContext(ctx, nodeInformer.Informer().HasSynced) {
		return nil, fmt.Errorf("can not sync node informer")
	}

	node, nodeIPs, podCIDRs := getNodeInfo(nodeLister, nodeName)

	if len(nodeIPs) == 0 {
		// 等待 Node 对象创建并被分配 NodeIPs，因为规则初始化依赖这些地址。
		ctx, cancel := context.WithTimeout(ctx, nodeIPsTimeout)
		defer cancel()
		_ = wait.PollUntilContextCancel(ctx, pollInterval, false, func(context.Context) (bool, error) {
			node, nodeIPs, podCIDRs = getNodeInfo(nodeLister, nodeName)
			return len(nodeIPs) != 0, nil
		})
	}

	if watchPodCIDRs && len(podCIDRs) == 0 {
		// 再额外等待 PodCIDRs：它可能晚于 NodeIPs 才由控制器分配。
		ctx, cancel := context.WithTimeout(ctx, podCIDRsTimeout)
		defer cancel()
		_ = wait.PollUntilContextCancel(ctx, pollInterval, false, func(context.Context) (bool, error) {
			node, nodeIPs, podCIDRs = getNodeInfo(nodeLister, nodeName)
			return len(podCIDRs) != 0, nil
		})

		if len(podCIDRs) == 0 {
			if node == nil {
				return nil, fmt.Errorf("timeout waiting for node %q to exist", nodeName)
			} else {
				return nil, fmt.Errorf("timeout waiting for PodCIDR allocation on node %q", nodeName)
			}
		}
	}

	// 为向后兼容，即使没有找到 Node（非 watchPodCIDRs 模式）或其没有 IP，
	// 也继续运行，仅记录错误而不让启动失败。
	if node == nil {
		klog.FromContext(ctx).Error(nil, "Timed out waiting for node to exist", "node", klog.KRef("", nodeName))
	} else if len(nodeIPs) == 0 {
		klog.FromContext(ctx).Error(nil, "Timed out waiting for node to be assigned IPs", "node", klog.KRef("", nodeName))
	}

	return &NodeManager{
		nodeInformer:  nodeInformer,
		nodeLister:    nodeLister,
		exitFunc:      exitFunc,
		watchPodCIDRs: watchPodCIDRs,

		node:     node,
		nodeIPs:  nodeIPs,
		podCIDRs: podCIDRs,
	}, nil
}

// getNodeInfo 从 lister 缓存中读取指定 Node，提取其 NodeIPs 与 PodCIDRs。
// Node 不存在时返回全 nil，调用方据此判断是否需要继续等待。
func getNodeInfo(nodeLister corelisters.NodeLister, nodeName string) (*v1.Node, []net.IP, []string) {
	node, _ := nodeLister.Get(nodeName)
	if node == nil {
		return nil, nil, nil
	}
	nodeIPs, _ := utilnode.GetNodeHostIPs(node)
	return node, nodeIPs, node.Spec.PodCIDRs
}

// NodeIPs 返回 NewNodeManager 中轮询到的 NodeIPs。
// 若 NewNodeManager 超时仍未获得任何 IP，则返回空切片。
func (n *NodeManager) NodeIPs() []net.IP {
	return n.nodeIPs
}

// PodCIDRs 返回 NewNodeManager 中轮询到的 PodCIDRs。
func (n *NodeManager) PodCIDRs() []string {
	return n.podCIDRs
}

// Node 返回最近一次观察到的 Node 对象的深拷贝；若尚未观察到 Node 则返回 nil。
// 使用深拷贝可避免调用方修改共享缓存中的对象。
func (n *NodeManager) Node() *v1.Node {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.node == nil {
		return nil
	}
	return n.node.DeepCopy()
}

// NodeInformer 返回该 NodeManager 使用的 NodeInformer。
func (n *NodeManager) NodeInformer() v1informers.NodeInformer {
	return n.nodeInformer
}

// OnNodeChange 是 Node 创建与更新事件的处理函数。
// 当 NodeIPs 或（启用了 watchPodCIDRs 时的）PodCIDRs 发生变化时，
// 主动让 kube-proxy 退出，由外部监督机制重启以重建规则。
func (n *NodeManager) OnNodeChange(node *v1.Node) {
	// 更新缓存的 Node 对象；写入需加锁，因为读取方可能并发访问。
	n.mu.Lock()
	n.node = node
	n.mu.Unlock()

	// 当 watchPodCIDRs 开启时，一旦发现事件中的 PodCIDRs 与初始化时不同就退出，
	// 因为 PodCIDR 变化会影响本地端点判定逻辑，必须重建规则。
	if n.watchPodCIDRs {
		if !reflect.DeepEqual(n.podCIDRs, node.Spec.PodCIDRs) {
			klog.InfoS("PodCIDRs changed for the node",
				"node", klog.KObj(node), "newPodCIDRs", node.Spec.PodCIDRs, "oldPodCIDRs", n.podCIDRs)
			klog.Flush()
			n.exitFunc(1)
		}
	}

	nodeIPs, _ := utilnode.GetNodeHostIPs(node)

	// 只要事件中的 NodeIPs 与初始化时不同就退出；
	// 因为大量 iptables/IPVS 规则直接绑定节点 IP，必须先刷新缓存再重建。
	if !reflect.DeepEqual(n.nodeIPs, nodeIPs) {
		klog.InfoS("NodeIPs changed for the node",
			"node", klog.KObj(node), "newNodeIPs", nodeIPs, "oldNodeIPs", n.nodeIPs)
		klog.Flush()
		n.exitFunc(1)
	}
}

// OnNodeDelete 是 Node 删除事件的处理函数。
// 节点消失意味着本机网络身份可能失效，因此直接退出以触发重建。
func (n *NodeManager) OnNodeDelete(node *v1.Node) {
	klog.InfoS("Node is being deleted", "node", klog.KObj(node))
	klog.Flush()
	n.exitFunc(1)
}

// OnNodeSynced 在缓存同步完成且所有既有 Node 都已通知后调用。
// 当前无需执行额外工作，保留空实现以满足接口约定。
func (n *NodeManager) OnNodeSynced() {}
