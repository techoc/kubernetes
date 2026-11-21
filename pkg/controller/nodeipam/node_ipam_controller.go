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

package nodeipam

import (
	"context"
	"fmt"
	"net"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	cloudprovider "k8s.io/cloud-provider"
	controllersmetrics "k8s.io/component-base/metrics/prometheus/controllers"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller/nodeipam/ipam"
)

// ipamController 抽象 legacy 模式下的 IPAM 控制循环，
// 使 Controller 能以统一方式运行新旧两套分配器。
type ipamController interface {
	Run(ctx context.Context)
}

// Controller 管理节点 IPAM 状态：根据 allocatorType 选择 legacy IPAM
// 或 CIDR 分配器，为节点分配 podCIDR 并监听集群节点变化。
type Controller struct {
	allocatorType ipam.CIDRAllocatorType

	cloud                cloudprovider.Interface
	clusterCIDRs         []*net.IPNet
	serviceCIDR          *net.IPNet
	secondaryServiceCIDR *net.IPNet
	kubeClient           clientset.Interface
	eventBroadcaster     record.EventBroadcaster

	nodeLister         corelisters.NodeLister
	nodeInformerSynced cache.InformerSynced

	legacyIPAM    ipamController
	cidrAllocator ipam.CIDRAllocator
}

// NewNodeIpamController 创建一个新的节点 IP 地址管理控制器，用于与 cloudprovider 同步节点实例信息。
// 如果无法用已分配给节点的 podCIDR 初始化 CIDR 位图则返回错误；
// 由于当前不允许修改 podCIDR，这类错误必须作为致命错误处理。
func NewNodeIpamController(
	ctx context.Context,
	nodeInformer coreinformers.NodeInformer,
	cloud cloudprovider.Interface,
	kubeClient clientset.Interface,
	clusterCIDRs []*net.IPNet,
	serviceCIDR *net.IPNet,
	secondaryServiceCIDR *net.IPNet,
	nodeCIDRMaskSizes []int,
	allocatorType ipam.CIDRAllocatorType) (*Controller, error) {

	if kubeClient == nil {
		return nil, fmt.Errorf("kubeClient is nil when starting Controller")
	}

	// Cloud CIDR 分配器直接使用云厂商提供的 CIDR，不依赖 clusterCIDR 或 nodeCIDRMaskSize，
	// 因此仅对其他分配器校验这两个参数。
	if allocatorType != ipam.CloudAllocatorType {
		if len(clusterCIDRs) == 0 {
			return nil, fmt.Errorf("Controller: Must specify --cluster-cidr if --allocate-node-cidrs is set")
		}

		for idx, cidr := range clusterCIDRs {
			mask := cidr.Mask
			if maskSize, _ := mask.Size(); maskSize > nodeCIDRMaskSizes[idx] {
				return nil, fmt.Errorf("Controller: Invalid --cluster-cidr, mask size of cluster CIDR must be less than or equal to --node-cidr-mask-size configured for CIDR family")
			}
		}
	}

	ic := &Controller{
		cloud:                cloud,
		kubeClient:           kubeClient,
		eventBroadcaster:     record.NewBroadcaster(record.WithContext(ctx)),
		clusterCIDRs:         clusterCIDRs,
		serviceCIDR:          serviceCIDR,
		secondaryServiceCIDR: secondaryServiceCIDR,
		allocatorType:        allocatorType,
	}

	// TODO: 将这类检查抽象为 controller manager 通用的启动方法。
	// 根据分配器类型选择 legacy IPAM（从集群/云获取 CIDR）或通用的 CIDR 分配器。
	if ic.allocatorType == ipam.IPAMFromClusterAllocatorType || ic.allocatorType == ipam.IPAMFromCloudAllocatorType {
		var err error
		ic.legacyIPAM, err = createLegacyIPAM(ctx, ic, nodeInformer, cloud, kubeClient, clusterCIDRs, serviceCIDR, nodeCIDRMaskSizes)
		if err != nil {
			return nil, err
		}
	} else {
		var err error

		allocatorParams := ipam.CIDRAllocatorParams{
			ClusterCIDRs:         clusterCIDRs,
			ServiceCIDR:          ic.serviceCIDR,
			SecondaryServiceCIDR: ic.secondaryServiceCIDR,
			NodeCIDRMaskSizes:    nodeCIDRMaskSizes,
		}

		ic.cidrAllocator, err = ipam.New(ctx, kubeClient, cloud, nodeInformer, ic.allocatorType, allocatorParams)
		if err != nil {
			return nil, err
		}
	}

	ic.nodeLister = nodeInformer.Lister()
	ic.nodeInformerSynced = nodeInformer.Informer().HasSynced

	return ic, nil
}

// Run 启动异步循环，监控集群节点状态并驱动相应的 IPAM 分配器。
// 启动前需等待 node informer 缓存同步完成，否则基于缓存的分配决策会遗漏已有节点。
func (nc *Controller) Run(ctx context.Context) {
	defer utilruntime.HandleCrashWithContext(ctx)

	// 启动事件处理流水线：结构化日志与事件落盘到 API server，
	// 退出时关闭 broadcaster，避免事件写入 goroutine 泄漏。
	nc.eventBroadcaster.StartStructuredLogging(3)
	nc.eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: nc.kubeClient.CoreV1().Events("")})
	defer nc.eventBroadcaster.Shutdown()
	klog.FromContext(ctx).Info("Starting ipam controller")
	defer klog.FromContext(ctx).Info("Shutting down ipam controller")

	if !cache.WaitForNamedCacheSyncWithContext(ctx, nc.nodeInformerSynced) {
		return
	}

	if nc.allocatorType == ipam.IPAMFromClusterAllocatorType || nc.allocatorType == ipam.IPAMFromCloudAllocatorType {
		nc.legacyIPAM.Run(ctx)
	} else {
		nc.cidrAllocator.Run(ctx)
	}
}

// RunWithMetrics 是对 Run 的包装，在启动/停止 nodeipam 控制器时额外记录指标，
// 便于 controller manager 观测各控制器的运行状态。
func (nc *Controller) RunWithMetrics(ctx context.Context, controllerManagerMetrics *controllersmetrics.ControllerManagerMetrics) {
	controllerManagerMetrics.ControllerStarted("nodeipam")
	defer controllerManagerMetrics.ControllerStopped("nodeipam")
	nc.Run(ctx)
}
