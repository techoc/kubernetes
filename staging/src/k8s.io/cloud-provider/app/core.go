/*
Copyright 2018 The Kubernetes Authors.

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

// Package app implements a server that runs a set of active
// components.  This includes node controllers, service and
// route controller, and so on.
package app

import (
	"context"
	"fmt"
	"net"
	"strings"

	cloudprovider "k8s.io/cloud-provider"
	"k8s.io/cloud-provider/app/config"
	cloudnodecontroller "k8s.io/cloud-provider/controllers/node"
	cloudnodelifecyclecontroller "k8s.io/cloud-provider/controllers/nodelifecycle"
	routecontroller "k8s.io/cloud-provider/controllers/route"
	servicecontroller "k8s.io/cloud-provider/controllers/service"
	controllermanagerapp "k8s.io/controller-manager/app"
	"k8s.io/controller-manager/controller"
	"k8s.io/klog/v2"
	netutils "k8s.io/utils/net"

	utilfeature "k8s.io/apiserver/pkg/util/feature"
)

// startCloudNodeController 构造并启动 cloud-node-controller。
//
// 返回的 bool 表示“是否真正启动了控制器”：当云厂商未实现 Instances/InstancesV2
// 时构造会失败，此时只记录警告并返回 false，而不是让整个 CCM 启动失败——
// 缺少实例能力的云提供商仍可使用 Service/Route 等其他控制器。
func startCloudNodeController(ctx context.Context, initContext ControllerInitContext, controlexContext controllermanagerapp.ControllerContext, completedConfig *config.CompletedConfig, cloud cloudprovider.Interface) (controller.Interface, bool, error) {
	// Start the CloudNodeController
	nodeController, err := cloudnodecontroller.NewCloudNodeController(
		completedConfig.SharedInformers.Core().V1().Nodes(),
		// cloud node controller uses existing cluster role from node-controller
		// 复用 node-controller 的 ClusterRole，避免为 CCM 单独扩权。
		completedConfig.ClientBuilder.ClientOrDie(initContext.ClientName),
		cloud,
		completedConfig.ComponentConfig.NodeStatusUpdateFrequency.Duration,
		completedConfig.ComponentConfig.NodeController.ConcurrentNodeSyncs,
		completedConfig.ComponentConfig.NodeController.ConcurrentNodeStatusUpdates,
	)
	if err != nil {
		klog.Warningf("failed to start cloud node controller: %s", err)
		return nil, false, nil
	}

	// Run 内部会等待 informer 缓存同步，必须在独立 goroutine 中执行以免阻塞启动流程。
	go nodeController.Run(ctx.Done(), controlexContext.ControllerManagerMetrics)

	return nil, true, nil
}

// startCloudNodeLifecycleController 构造并启动 cloud-node-lifecycle-controller。
//
// 该控制器负责删除云侧已不存在的 Node、给已关机的实例打上 shutdown 污点。
// 与 node controller 类似，云厂商不支持 instances 接口时只降级而不报错。
func startCloudNodeLifecycleController(ctx context.Context, initContext ControllerInitContext, controlexContext controllermanagerapp.ControllerContext, completedConfig *config.CompletedConfig, cloud cloudprovider.Interface) (controller.Interface, bool, error) {
	// Start the cloudNodeLifecycleController
	cloudNodeLifecycleController, err := cloudnodelifecyclecontroller.NewCloudNodeLifecycleController(
		completedConfig.SharedInformers.Core().V1().Nodes(),
		// cloud node lifecycle controller uses existing cluster role from node-controller
		// 同样复用 node-controller 的 ClusterRole。
		completedConfig.ClientBuilder.ClientOrDie(initContext.ClientName),
		cloud,
		completedConfig.ComponentConfig.NodeLifecycleController.NodeMonitorPeriod.Duration,
		int(completedConfig.ComponentConfig.NodeLifecycleController.ConcurrentNodeLifecycleSyncs),
	)
	if err != nil {
		klog.Warningf("failed to start cloud node lifecycle controller: %s", err)
		return nil, false, nil
	}

	go cloudNodeLifecycleController.Run(ctx, controlexContext.ControllerManagerMetrics)

	return nil, true, nil
}

// startServiceController 构造并启动 service-controller，负责把 Service
// （尤其是 LoadBalancer 类型）同步到云厂商的负载均衡器。
func startServiceController(ctx context.Context, initContext ControllerInitContext, controlexContext controllermanagerapp.ControllerContext, completedConfig *config.CompletedConfig, cloud cloudprovider.Interface) (controller.Interface, bool, error) {
	// Start the service controller
	serviceController, err := servicecontroller.New(
		cloud,
		completedConfig.ClientBuilder.ClientOrDie(initContext.ClientName),
		completedConfig.SharedInformers.Core().V1().Services(),
		completedConfig.SharedInformers.Core().V1().Nodes(),
		completedConfig.ComponentConfig.KubeCloudShared.ClusterName,
		utilfeature.DefaultFeatureGate,
	)
	if err != nil {
		// This error shouldn't fail. It lives like this as a legacy.
		// 历史行为：云厂商未实现 LoadBalancer 接口时只打印错误并跳过该控制器，
		// 不能因为单个可选能力缺失而让 CCM 整体退出。
		klog.Errorf("Failed to start service controller: %v", err)
		return nil, false, nil
	}

	go serviceController.Run(ctx, int(completedConfig.ComponentConfig.ServiceController.ConcurrentServiceSyncs), controlexContext.ControllerManagerMetrics)

	return nil, true, nil
}

// startRouteController 构造并启动 route-controller，把节点的 PodCIDR 同步为云侧路由。
//
// 只有在显式开启 --configure-cloud-routes 且云厂商实现了 Routes 接口时才启动；
// 否则返回 false，由调用方跳过该控制器。
func startRouteController(ctx context.Context, initContext ControllerInitContext, controlexContext controllermanagerapp.ControllerContext, completedConfig *config.CompletedConfig, cloud cloudprovider.Interface) (controller.Interface, bool, error) {
	if !completedConfig.ComponentConfig.KubeCloudShared.ConfigureCloudRoutes {
		klog.Infof("Will not configure cloud provider routes, --configure-cloud-routes: %v", completedConfig.ComponentConfig.KubeCloudShared.ConfigureCloudRoutes)
		return nil, false, nil
	}

	// If CIDRs should be allocated for pods and set on the CloudProvider, then start the route controller
	routes, ok := cloud.Routes()
	if !ok {
		klog.Warning("--configure-cloud-routes is set, but cloud provider does not support routes. Will not configure cloud provider routes.")
		return nil, false, nil
	}

	// failure: bad cidrs in config
	// 解析 clusterCIDR；后续 route controller 用它判断哪些路由由本集群负责。
	clusterCIDRs, dualStack, err := processCIDRs(completedConfig.ComponentConfig.KubeCloudShared.ClusterCIDR)
	if err != nil {
		return nil, false, err
	}

	// failure: more than one cidr but they are not configured as dual stack
	// 多于一个 CIDR 却不同时包含 IPv4/IPv6，说明配置写错了，属于硬错误。
	if len(clusterCIDRs) > 1 && !dualStack {
		return nil, false, fmt.Errorf("len of ClusterCIDRs==%v and they are not configured as dual stack (at least one from each IPFamily", len(clusterCIDRs))
	}

	// failure: more than cidrs is not allowed even with dual stack
	// 双栈最多两个 CIDR，再多就无法与云侧路由一一对应。
	if len(clusterCIDRs) > 2 {
		return nil, false, fmt.Errorf("length of clusterCIDRs is:%v more than max allowed of 2", len(clusterCIDRs))
	}

	routeController, err := routecontroller.New(
		routes,
		completedConfig.ClientBuilder.ClientOrDie(initContext.ClientName),
		completedConfig.SharedInformers.Core().V1().Nodes(),
		completedConfig.ComponentConfig.KubeCloudShared.ClusterName,
		clusterCIDRs,
	)
	if err != nil {
		return nil, false, err
	}
	go routeController.Run(ctx, completedConfig.ComponentConfig.KubeCloudShared.RouteReconciliationPeriod.Duration, controlexContext.ControllerManagerMetrics)

	return nil, true, nil
}

// processCIDRs 是一个处理逗号分隔 CIDR 列表的辅助函数，返回类型化的 CIDR 列表、
// 表示这组 CIDR 是否构成双栈的标志，以及解析任一 CIDR 失败时的错误。
//
// 入参来自 --cluster-cidr，允许以逗号分隔。返回值中的 dualStack 用于
// 校验配置是否自相矛盾（多 CIDR 必须构成双栈）。
func processCIDRs(cidrsList string) ([]*net.IPNet, bool, error) {
	// 先 TrimSpace 再 Split，容忍 "10.0.0.0/8, 192.168.0.0/16" 这类带空格的写法；
	// 单个 CIDR 内部不会被误切分，具体合法性交给 ParseCIDRs 校验。
	cidrsSplit := strings.Split(strings.TrimSpace(cidrsList), ",")

	cidrs, err := netutils.ParseCIDRs(cidrsSplit)
	if err != nil {
		return nil, false, err
	}

	// if cidrs has an error then the previous call will fail
	// safe to ignore error checking on next call
	dualstack, _ := netutils.IsDualStackCIDRs(cidrs)

	return cidrs, dualstack, nil
}
