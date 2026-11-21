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

package ipam

import (
	"context"
	"fmt"
	"net"
	"time"

	"k8s.io/kubernetes/pkg/controller/nodeipam/ipam/cidrset"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	informers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	cloudprovider "k8s.io/cloud-provider"
	"k8s.io/klog/v2"
)

// CIDRAllocatorType 表示要使用的分配器类型。
type CIDRAllocatorType string

const (
	// RangeAllocatorType 使用内部 CIDR 区段分配器为节点分配 CIDR 区段。
	RangeAllocatorType CIDRAllocatorType = "RangeAllocator"
	// CloudAllocatorType 借助云厂商的能力为节点分配 CIDR 区段。
	CloudAllocatorType CIDRAllocatorType = "CloudAllocator"
	// IPAMFromClusterAllocatorType 使用 ipam 控制器将节点 CIDR 区段分配从集群同步到云端。
	IPAMFromClusterAllocatorType = "IPAMFromCluster"
	// IPAMFromCloudAllocatorType 使用 ipam 控制器将节点 CIDR 区段分配从云端同步到集群。
	IPAMFromCloudAllocatorType = "IPAMFromCloud"
)

// TODO: 确定这些常量的合理取值。
const (
	// 节点控制器轮询 list nodes 接口的最长总时长。
	apiserverStartupGracePeriod = 10 * time.Minute

	// 节点控制器可并发处理的 NodeSpec 更新数量。
	cidrUpdateWorkers = 30

	// cidrUpdateRetries 是放弃一次 NodeSpec 更新前的重试次数。
	cidrUpdateRetries = 3
)

// nodePollInterval 是列取节点时的轮询间隔。
var nodePollInterval = 10 * time.Second

// CIDRAllocator 是需要为节点分配、占用和回收 CIDR 的组件所实现的接口。
type CIDRAllocator interface {
	// AllocateOrOccupyCIDR 检查给定节点：若其尚无 CIDR 则分配一个有效区段，
	// 若已有则把该区段标记为已占用。
	AllocateOrOccupyCIDR(ctx context.Context, node *v1.Node) error
	// ReleaseCIDR 释放已移除节点占用的 CIDR。
	ReleaseCIDR(logger klog.Logger, node *v1.Node) error
	// Run 启动分配器的全部工作逻辑。
	Run(ctx context.Context)
}

// CIDRAllocatorParams 是创建新的 CIDR 区段分配器所需的参数。
type CIDRAllocatorParams struct {
	// ClusterCIDRs 是集群 CIDR 列表。
	ClusterCIDRs []*net.IPNet
	// ServiceCIDR 是集群的主 Service CIDR。
	ServiceCIDR *net.IPNet
	// SecondaryServiceCIDR 是集群的辅助 Service CIDR。
	SecondaryServiceCIDR *net.IPNet
	// NodeCIDRMaskSizes 是节点 CIDR 掩码长度列表。
	NodeCIDRMaskSizes []int
}

// New 创建一个新的 CIDR 区段分配器。
func New(ctx context.Context, kubeClient clientset.Interface, cloud cloudprovider.Interface, nodeInformer informers.NodeInformer, allocatorType CIDRAllocatorType, allocatorParams CIDRAllocatorParams) (CIDRAllocator, error) {
	nodeList, err := listNodes(ctx, kubeClient)
	if err != nil {
		return nil, err
	}

	switch allocatorType {
	case RangeAllocatorType:
		return NewCIDRRangeAllocator(ctx, kubeClient, nodeInformer, allocatorParams, nodeList)
	default:
		return nil, fmt.Errorf("invalid CIDR allocator type: %v", allocatorType)
	}
}

func listNodes(ctx context.Context, kubeClient clientset.Interface) (*v1.NodeList, error) {
	var nodeList *v1.NodeList
	logger := klog.FromContext(ctx)

	// 必须轮询等待，因为 apiserver 可能尚未就绪；超时错误会触发控制器管理器重启。
	if pollErr := wait.PollUntilContextTimeout(ctx, nodePollInterval, apiserverStartupGracePeriod, true, func(ctx context.Context) (bool, error) {
		var err error
		nodeList, err = kubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{
			FieldSelector: fields.Everything().String(),
			LabelSelector: labels.Everything().String(),
		})
		if err != nil {
			logger.Error(err, "Failed to list all nodes")
			return false, nil
		}
		return true, nil
	}); pollErr != nil {
		return nil, fmt.Errorf("failed to list all nodes in %v, cannot proceed without updating CIDR map",
			apiserverStartupGracePeriod)
	}
	return nodeList, nil
}

// ipnetToStringList 将 net.IPNet 切片转换为字符串形式的 CIDR 列表。
func ipnetToStringList(inCIDRs []*net.IPNet) []string {
	outCIDRs := make([]string, len(inCIDRs))
	for idx, inCIDR := range inCIDRs {
		outCIDRs[idx] = inCIDR.String()
	}
	return outCIDRs
}

// occupyServiceCIDR 在 Service CIDR 与集群 CIDR 相交时，将 Service CIDR 区段标记为已占用。
func occupyServiceCIDR(set *cidrset.CidrSet, clusterCIDR, serviceCIDR *net.IPNet) error {
	if clusterCIDR.Contains(serviceCIDR.IP) || serviceCIDR.Contains(clusterCIDR.IP) {
		if err := set.Occupy(serviceCIDR); err != nil {
			return err
		}
	}
	return nil
}
