//go:build linux

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

package ipvs

import (
	"context"
	"fmt"

	"k8s.io/klog/v2"
	utilipvs "k8s.io/kubernetes/pkg/proxy/ipvs/util"
	netutils "k8s.io/utils/net"
)

// CanUseIPVSProxier 检查当前环境能否使用 ipvs Proxier。
// 检查内容包括 ipset 版本和调度器：若已存在使用所配置调度器的虚拟服务器（VS），
// 则直接返回成功；否则尝试用所配置的调度器创建一个测试 VS，以验证调度器确实可用。
// 如有必要，所需内核模块会被自动加载。
func CanUseIPVSProxier(ctx context.Context, ipvs utilipvs.Interface, ipsetver IPSetVersioner, scheduler string) error {
	logger := klog.FromContext(ctx)
	// 已知问题：https://github.com/moby/ipvs/issues/27
	// 内核未编译 ipvs 时，moby/ipvs 不返回错误而是返回 handle==nil，
	// 这会让 ipvs.GetVirtualServers 和 ipvs.AddVirtualServer 误报成功（err==nil）。
	// 该缺陷修复后，内核不支持 ipvs 时传入的 "ipvs" 会是 nil；修复前只能通过“重新读取”绕过。
	if ipvs == nil {
		return fmt.Errorf("ipvs not supported by the kernel")
	}

	// 检查 ipset 版本：IPv6 能力要求 ipset 不低于 6.0，版本过低必须直接拒绝。
	versionString, err := ipsetver.GetVersion()
	if err != nil {
		return fmt.Errorf("error getting ipset version, error: %w", err)
	}
	if !checkMinVersion(versionString) {
		return fmt.Errorf("ipset version: %s is less than min required version: %s", versionString, MinIPSetCheckVersion)
	}

	if scheduler == "" {
		scheduler = defaultScheduler
	}

	// 若已有使用目标调度器的 VS，说明 ipvs 在本节点确定可用，可以跳过后续探测。
	vservers, err := ipvs.GetVirtualServers()
	if err != nil {
		logger.Error(err, "Can't read the ipvs")
		return err
	}
	logger.V(5).Info("Virtual Servers", "count", len(vservers))
	if len(vservers) > 0 {
		// 存在 VS 多半意味着 kube-proxy 刚刚重启：VS 还在即说明 ipvs 可用；
		// 若其中一个 VS 恰好使用配置的调度器，调度器也无需再验证。
		for _, vs := range vservers {
			if vs.Scheduler == scheduler {
				logger.V(5).Info("VS exist, Skipping checks")
				return nil
			}
		}
		logger.V(5).Info("No existing VS uses the configured scheduler", "scheduler", scheduler)
	}

	// 尝试用传入的调度器创建一个测试用虚拟服务器（dummy VS）。
	// 必须选用节点上未被占用的 VIP：这里使用 https://datatracker.ietf.org/doc/html/rfc5737
	// 中 TEST-NET-2 段的 "198.51.100.0"，该地址段保留给文档示例使用。
	// 如果用户执意把这个地址配置成 VS，探测确实会破坏它，但这本身已是非法配置。
	// 如果用户把它配到节点网卡上（而非配成 VS），探测期间流量会被临时路由到 ipvs 并被丢弃；
	// 这同样是非法配置，只是影响较小。
	// 只要用户遵守保留地址约定就不会有问题，但从文档复制粘贴地址并不少见，
	// 因此必须明确说明不得使用 TEST-NET-2 段的地址。
	vs := utilipvs.VirtualServer{
		Address:   netutils.ParseIPSloppy("198.51.100.0"),
		Protocol:  "TCP",
		Port:      20000,
		Scheduler: scheduler,
	}
	if err := ipvs.AddVirtualServer(&vs); err != nil {
		logger.Error(err, "Could not create dummy VS", "scheduler", scheduler)
		return err
	}

	// 为绕过上述缺陷，必须重新读取 VS 列表确认测试 VS 真的创建成功，不能只信任 AddVirtualServer 的返回值。
	vservers, err = ipvs.GetVirtualServers()
	if err != nil {
		logger.Error(err, "ipvs.GetVirtualServers")
		return err
	}
	logger.V(5).Info("Virtual Servers after adding dummy", "count", len(vservers))
	if len(vservers) == 0 {
		logger.Info("Dummy VS not created", "scheduler", scheduler)
		return fmt.Errorf("ipvs not supported") // 该错误判断是对上述 BUG 的绕过措施
	}
	logger.V(5).Info("Dummy VS created", "vs", vs)

	if err := ipvs.DeleteVirtualServer(&vs); err != nil {
		logger.Error(err, "Could not delete dummy VS")
		return err
	}

	return nil
}
