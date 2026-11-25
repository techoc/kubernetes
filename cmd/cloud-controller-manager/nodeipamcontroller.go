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

// 本文件包含示例 nodeipamcontroller 的相关代码，
// 演示云提供商如何向 cloud-controller-manager 添加外部 controller。

package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	cloudprovider "k8s.io/cloud-provider"
	"k8s.io/cloud-provider/app"
	cloudcontrollerconfig "k8s.io/cloud-provider/app/config"
	genericcontrollermanager "k8s.io/controller-manager/app"
	"k8s.io/controller-manager/controller"
	"k8s.io/klog/v2"
	nodeipamcontrolleroptions "k8s.io/kubernetes/cmd/kube-controller-manager/app/options"
	nodeipamcontroller "k8s.io/kubernetes/pkg/controller/nodeipam"
	nodeipamconfig "k8s.io/kubernetes/pkg/controller/nodeipam/config"
	"k8s.io/kubernetes/pkg/controller/nodeipam/ipam"
	netutils "k8s.io/utils/net"
)

const (
	// defaultNodeMaskCIDRIPv4 是 IPv4 节点 CIDR 的默认掩码大小
	defaultNodeMaskCIDRIPv4 = 24
	// defaultNodeMaskCIDRIPv6 是 IPv6 节点 CIDR 的默认掩码大小
	defaultNodeMaskCIDRIPv6 = 64
)

type nodeIPAMController struct {
	nodeIPAMControllerConfiguration nodeipamconfig.NodeIPAMControllerConfiguration
	nodeIPAMControllerOptions       nodeipamcontrolleroptions.NodeIPAMControllerOptions
}

func (nodeIpamController *nodeIPAMController) StartNodeIpamControllerWrapper(initContext app.ControllerInitContext, completedConfig *cloudcontrollerconfig.CompletedConfig, cloud cloudprovider.Interface) app.InitFunc {
	allErrors := nodeIpamController.nodeIPAMControllerOptions.Validate()
	if len(allErrors) > 0 {
		klog.Fatal("NodeIPAM controller values are not properly set.")
	}
	nodeIpamController.nodeIPAMControllerOptions.ApplyTo(&nodeIpamController.nodeIPAMControllerConfiguration)

	return func(ctx context.Context, controllerContext genericcontrollermanager.ControllerContext) (controller.Interface, bool, error) {
		return startNodeIpamController(ctx, initContext, completedConfig, nodeIpamController.nodeIPAMControllerConfiguration, controllerContext, cloud)
	}
}

func startNodeIpamController(ctx context.Context, initContext app.ControllerInitContext, ccmConfig *cloudcontrollerconfig.CompletedConfig, nodeIPAMConfig nodeipamconfig.NodeIPAMControllerConfiguration, controllerCtx genericcontrollermanager.ControllerContext, cloud cloudprovider.Interface) (controller.Interface, bool, error) {
	var serviceCIDR *net.IPNet
	var secondaryServiceCIDR *net.IPNet

	// 是否应启动 nodeIPAM
	if !ccmConfig.ComponentConfig.KubeCloudShared.AllocateNodeCIDRs {
		return nil, false, nil
	}

	// cloud provider 为 nil 时无法运行 cloud ipam controller（--cloud-provider 未设置或设置为 'external'）
	if cloud == nil && ccmConfig.ComponentConfig.KubeCloudShared.CIDRAllocatorType == string(ipam.CloudAllocatorType) {
		return nil, false, errors.New("--cidr-allocator-type is set to 'CloudAllocator' but cloud provider is not configured")
	}

	// 配置中的 CIDR 不合法则失败
	clusterCIDRs, dualStack, err := processCIDRs(ccmConfig.ComponentConfig.KubeCloudShared.ClusterCIDR)
	if err != nil {
		return nil, false, err
	}

	// 有多个 CIDR 但未配置为双栈时失败
	if len(clusterCIDRs) > 1 && !dualStack {
		return nil, false, fmt.Errorf("len of ClusterCIDRs==%v and they are not configured as dual stack (at least one from each IPFamily", len(clusterCIDRs))
	}

	// 即使启用双栈，也不允许超过两个 CIDR
	if len(clusterCIDRs) > 2 {
		return nil, false, fmt.Errorf("len of clusters is:%v > more than max allowed of 2", len(clusterCIDRs))
	}

	// 处理 service CIDR
	if len(strings.TrimSpace(nodeIPAMConfig.ServiceCIDR)) != 0 {
		_, serviceCIDR, err = netutils.ParseCIDRSloppy(nodeIPAMConfig.ServiceCIDR)
		if err != nil {
			klog.ErrorS(err, "Unsuccessful parsing of service CIDR", "CIDR", nodeIPAMConfig.ServiceCIDR)
		}
	}

	if len(strings.TrimSpace(nodeIPAMConfig.SecondaryServiceCIDR)) != 0 {
		_, secondaryServiceCIDR, err = netutils.ParseCIDRSloppy(nodeIPAMConfig.SecondaryServiceCIDR)
		if err != nil {
			klog.ErrorS(err, "Unsuccessful parsing of service CIDR", "CIDR", nodeIPAMConfig.SecondaryServiceCIDR)
		}
	}

	// 仅当同时提供 serviceCIDR 和 secondaryServiceCIDR 时才执行以下检查
	if serviceCIDR != nil && secondaryServiceCIDR != nil {
		// 必须构成双栈（来自不同的 IPFamilies）
		dualstackServiceCIDR, err := netutils.IsDualStackCIDRs([]*net.IPNet{serviceCIDR, secondaryServiceCIDR})
		if err != nil {
			return nil, false, fmt.Errorf("failed to perform dualstack check on serviceCIDR and secondaryServiceCIDR error:%v", err)
		}
		if !dualstackServiceCIDR {
			return nil, false, fmt.Errorf("serviceCIDR and secondaryServiceCIDR are not dualstack (from different IPfamiles)")
		}
	}

	nodeCIDRMaskSizes, err := setNodeCIDRMaskSizes(nodeIPAMConfig, clusterCIDRs)
	if err != nil {
		return nil, false, err
	}

	nodeIpamController, err := nodeipamcontroller.NewNodeIpamController(
		ctx,
		controllerCtx.InformerFactory.Core().V1().Nodes(),
		cloud,
		controllerCtx.ClientBuilder.ClientOrDie(initContext.ClientName),
		clusterCIDRs,
		serviceCIDR,
		secondaryServiceCIDR,
		nodeCIDRMaskSizes,
		ipam.CIDRAllocatorType(ccmConfig.ComponentConfig.KubeCloudShared.CIDRAllocatorType),
	)
	if err != nil {
		return nil, true, err
	}
	go nodeIpamController.Run(ctx)
	return nil, true, nil
}

// processCIDRs 是处理逗号分隔 CIDR 的辅助函数，返回
// 解析后的 CIDR 列表、
// 一个表示是否为双栈的标志，
// 以及任一 CIDR 解析失败时的错误。
func processCIDRs(cidrsList string) ([]*net.IPNet, bool, error) {
	cidrsSplit := strings.Split(strings.TrimSpace(cidrsList), ",")

	cidrs, err := netutils.ParseCIDRs(cidrsSplit)
	if err != nil {
		return nil, false, err
	}

	// 若 CIDR 有误，上一次调用就会失败，
	// 因此此处可以忽略下一次调用的错误检查。
	dualstack, _ := netutils.IsDualStackCIDRs(cidrs)

	return cidrs, dualstack, nil
}

// setNodeCIDRMaskSizes 分别按 --node-cidr-mask-size-ipv4 和 --node-cidr-mask-size-ipv6
// 的取值返回 IPv4 和 IPv6 节点 CIDR 掩码大小。若未提供这两个值，
// 则返回默认的 IPv4 和 IPv6 CIDR 掩码大小。
func setNodeCIDRMaskSizes(cfg nodeipamconfig.NodeIPAMControllerConfiguration, clusterCIDRs []*net.IPNet) ([]int, error) {

	sortedSizes := func(maskSizeIPv4, maskSizeIPv6 int) []int {
		nodeMaskCIDRs := make([]int, len(clusterCIDRs))

		for idx, clusterCIDR := range clusterCIDRs {
			if netutils.IsIPv6CIDR(clusterCIDR) {
				nodeMaskCIDRs[idx] = maskSizeIPv6
			} else {
				nodeMaskCIDRs[idx] = maskSizeIPv4
			}
		}
		return nodeMaskCIDRs
	}

	// --node-cidr-mask-size 标志与双栈集群不兼容。
	ipv4Mask, ipv6Mask := defaultNodeMaskCIDRIPv4, defaultNodeMaskCIDRIPv6
	isDualstack := len(clusterCIDRs) > 1

	// 情况一：集群为双栈（即有多个 CIDR）
	if isDualstack {
		// 若设置了 --node-cidr-mask-size 则失败，用户必须配置正确的双栈掩码大小（或使用默认值）
		if cfg.NodeCIDRMaskSize != 0 {
			return nil, errors.New("usage of --node-cidr-mask-size is not allowed with dual-stack clusters")
		}

		if cfg.NodeCIDRMaskSizeIPv4 != 0 {
			ipv4Mask = int(cfg.NodeCIDRMaskSizeIPv4)
		}
		if cfg.NodeCIDRMaskSizeIPv6 != 0 {
			ipv6Mask = int(cfg.NodeCIDRMaskSizeIPv6)
		}
		return sortedSizes(ipv4Mask, ipv6Mask), nil
	}

	maskConfigured := cfg.NodeCIDRMaskSize != 0
	maskV4Configured := cfg.NodeCIDRMaskSizeIPv4 != 0
	maskV6Configured := cfg.NodeCIDRMaskSizeIPv6 != 0
	isSingleStackIPv6 := netutils.IsIPv6CIDR(clusterCIDRs[0])

	// 设置了原有的标志
	if maskConfigured {
		// 原有的掩码标志仍是主要依据。
		if maskV4Configured || maskV6Configured {
			return nil, errors.New("usage of --node-cidr-mask-size-ipv4 and --node-cidr-mask-size-ipv6 is not allowed if --node-cidr-mask-size is set. For dual-stack clusters please unset it and use IPFamily specific flags")
		}

		mask := int(cfg.NodeCIDRMaskSize)
		return sortedSizes(mask, mask), nil
	}

	if maskV4Configured {
		if isSingleStackIPv6 {
			return nil, errors.New("usage of --node-cidr-mask-size-ipv4 is not allowed for a single-stack IPv6 cluster")
		}

		ipv4Mask = int(cfg.NodeCIDRMaskSizeIPv4)
	}

	// !maskV4Configured && !maskConfigured && maskV6Configured
	if maskV6Configured {
		if !isSingleStackIPv6 {
			return nil, errors.New("usage of --node-cidr-mask-size-ipv6 is not allowed for a single-stack IPv4 cluster")
		}

		ipv6Mask = int(cfg.NodeCIDRMaskSizeIPv6)
	}
	return sortedSizes(ipv4Mask, ipv6Mask), nil
}
