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

package storage

import (
	"errors"
	"fmt"
	"net"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	apiservice "k8s.io/kubernetes/pkg/api/service"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/validation"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/registry/core/service/ipallocator"
	"k8s.io/kubernetes/pkg/registry/core/service/portallocator"
	netutils "k8s.io/utils/net"
)

// Allocators 聚合 Service 使用的各 IP Family ClusterIP 池与统一 NodePort 池，并负责组织可提交或回滚的事务。
type Allocators struct {
	serviceIPAllocatorsByFamily map[api.IPFamily]ipallocator.Interface
	defaultServiceIPFamily      api.IPFamily // 对应 --service-cluster-ip-range[0]
	serviceNodePorts            portallocator.Interface
}

// ServiceNodePort 记录 Service NodePort 的协议和端口号。
type ServiceNodePort struct {
	// 此端口使用的 IP 协议，支持 "TCP" 和 "UDP"。
	Protocol api.Protocol

	// Service 在每个节点上暴露的端口。
	// 若 Service 类型需要 NodePort 而未指定值，则自动分配。
	NodePort int32
}

// 此过渡函数用于兼容 Service REST 存储结构的扁平化改造。
func makeAlloc(defaultFamily api.IPFamily, ipAllocs map[api.IPFamily]ipallocator.Interface, portAlloc portallocator.Interface) Allocators {
	return Allocators{
		defaultServiceIPFamily:      defaultFamily,
		serviceIPAllocatorsByFamily: ipAllocs,
		serviceNodePorts:            portAlloc,
	}
}

// allocateCreate 先推导 IPFamily，再依次预留 ClusterIP 和 NodePort；任一步失败都会撤销此前成功的预留。
func (al *Allocators) allocateCreate(service *api.Service, dryRun bool) (transaction, error) {
	result := metaTransaction{}
	success := false

	defer func() {
		if !success {
			result.Revert()
		}
	}()

	// 在此初始化 IP 地址族字段，确保 dryRun == true 时也能看到初始化结果。
	if err := al.initIPFamilyFields(After{service}, Before{nil}); err != nil {
		return nil, err
	}

	// 分配 ClusterIP。
	//TODO(thockin): 校验本不应允许空 clusterIP，但当前会放行且已有测试覆盖。彻底修复改动较大，留待后续处理。
	if txn, err := al.txnAllocClusterIPs(service, dryRun); err != nil {
		return nil, err
	} else {
		result = append(result, txn)
	}

	// 分配端口。
	if txn, err := al.txnAllocNodePorts(service, dryRun); err != nil {
		return nil, err
	} else {
		result = append(result, txn)
	}

	success = true
	return result, nil
}

// initIPFamilyFields 根据旧对象、IPFamilyPolicy 和集群双栈配置补全 IPFamilies/ClusterIPs，并在访问地址池前完成字段预校验。
func (al *Allocators) initIPFamilyFields(after After, before Before) error {
	oldService, service := before.Service, after.Service

	// ExternalName 类型无需处理地址族。
	if service.Spec.Type == api.ServiceTypeExternalName {
		return nil
	}

	// 我们不希望因为集群在双栈与非双栈之间切换，就自动给 PreferDualStack 的 Service
	// 升配（添加 IP）或降配（移除 IP）。
	// 也就是说，只有同时满足下面两个条件时才会升配或降配：
	//
	// - 把 ipFamilyPolicy 改成 "RequireDualStack" 或 "SingleStack"；
	// - 并且新增或移除了次要的 clusterIP 或 ipFamily。
	if isMatchingPreferDualStackClusterIPFields(after, before) {
		return nil // 无需继续处理。
	}

	// 用户未指定 ipFamilyPolicy 时推断默认值。不设固定默认值，是为了在 ipFamilyPolicy
	// 明确指示时避免意外切换单栈和双栈。分情况处理：
	//   * 创建（POST）：未指定策略时视为 SingleStack。
	//   * 更新（PUT）：未指定策略时沿用旧策略。相比一律假定 SingleStack，
	//     这样允许不指定 ipFamily 的 PUT 将 clusterIPs 从两个值改为一个。
	//   * 更新（PATCH）：未指定策略时沿用旧策略。
	if service.Spec.IPFamilyPolicy == nil {
		if oldService != nil && oldService.Spec.IPFamilyPolicy != nil {
			// 从带策略的旧对象更新时沿用旧策略。
			service.Spec.IPFamilyPolicy = oldService.Spec.IPFamilyPolicy
		} else if service.Spec.ClusterIP == api.ClusterIPNone && len(service.Spec.Selector) == 0 {
			// 特例：无 Selector 的 Headless Service 默认采用双栈。
			requireDualStack := api.IPFamilyPolicyRequireDualStack
			service.Spec.IPFamilyPolicy = &requireDualStack
		} else {
			// 从未指定策略的旧对象更新为需要策略的类型（如 ExternalName）。
			singleStack := api.IPFamilyPolicySingleStack
			service.Spec.IPFamilyPolicy = &singleStack
		}
	}
	// 至此可以假定 ipFamilyPolicy 已设置。

	// 先做宽松预校验，使后续分配代码无需反复处理边界情况。
	if el := validation.ValidateServiceClusterIPsRelatedFields(service, oldService); len(el) != 0 {
		return apierrors.NewInvalid(api.Kind("Service"), service.Name, el)
	}

	//TODO(thockin): 是否应将这部分逻辑移入校验流程？
	el := make(field.ErrorList, 0)

	// 仅更新时需要完成的预处理。
	if oldService != nil {
		if getIPFamilyPolicy(service) == api.IPFamilyPolicySingleStack {
			// 只要 ClusterIPs 和 IPFamilies 未变，将策略设为单栈就表示用户明确意图如此。
			// ClusterIPs[0] 不可变，因此可以安全保留。
			if sameClusterIPs(oldService, service) && len(service.Spec.ClusterIPs) > 1 {
				service.Spec.ClusterIPs = service.Spec.ClusterIPs[0:1]
			}
			if sameIPFamilies(oldService, service) && len(service.Spec.IPFamilies) > 1 {
				service.Spec.IPFamilies = service.Spec.IPFamilies[0:1]
			}
		} else {
			// 策略不是单栈却减少了这些字段时属于错误，用户必须明确指定策略。
			if reducedClusterIPs(After{service}, Before{oldService}) {
				el = append(el, field.Invalid(field.NewPath("spec", "ipFamilyPolicy"), service.Spec.IPFamilyPolicy,
					"must be 'SingleStack' to release the secondary cluster IP"))
			}
			if reducedIPFamilies(After{service}, Before{oldService}) {
				el = append(el, field.Invalid(field.NewPath("spec", "ipFamilyPolicy"), service.Spec.IPFamilyPolicy,
					"must be 'SingleStack' to release the secondary IP family"))
			}
		}
	}

	// 确认 ipFamilyPolicy 与给定的 ipFamilies、clusterIPs 相符；特殊场景处理后还会继续校验。
	if getIPFamilyPolicy(service) == api.IPFamilyPolicySingleStack {
		if len(service.Spec.ClusterIPs) == 2 {
			el = append(el, field.Invalid(field.NewPath("spec", "ipFamilyPolicy"), service.Spec.IPFamilyPolicy,
				"must be 'RequireDualStack' or 'PreferDualStack' when multiple cluster IPs are specified"))
		}
		if len(service.Spec.IPFamilies) == 2 {
			el = append(el, field.Invalid(field.NewPath("spec", "ipFamilyPolicy"), service.Spec.IPFamilyPolicy,
				"must be 'RequireDualStack' or 'PreferDualStack' when multiple IP families are specified"))
		}
	}

	// 从 ClusterIPs 推导 IPFamilies；特殊场景处理后会继续校验。
	for i, ip := range service.Spec.ClusterIPs {
		if ip == api.ClusterIPNone {
			break
		}

		// 前面已校验 IP 格式正确，且已有的 ipFamilies 条目与该 IP 相符。
		fam := familyOf(ip)

		// 未指定对应地址族时补充该族。
		if i >= len(service.Spec.IPFamilies) {
			// 地址族稍后会继续校验，但此处报错能明确指出是用户指定的 IP 不支持，而非自动分配的地址族不支持。
			if _, found := al.serviceIPAllocatorsByFamily[fam]; !found {
				el = append(el, field.Invalid(field.NewPath("spec", "clusterIPs").Index(i), service.Spec.ClusterIPs,
					fmt.Sprintf("%s is not configured on this cluster", fam)))
			} else {
				// 可以安全推导。
				service.Spec.IPFamilies = append(service.Spec.IPFamilies, fam)
			}
		}
	}

	// 存在校验错误时立即返回，避免继续处理而扩大错误范围。
	if len(el) > 0 {
		return apierrors.NewInvalid(api.Kind("Service"), service.Name, el)
	}

	// 特例：无 Selector 的 Headless Service。必须先于其他校验处理，因为它允许一些通常会被拒绝的输入组合。
	if service.Spec.ClusterIP == api.ClusterIPNone && len(service.Spec.Selector) == 0 {
		// 用户未设置 IPFamilies 时，从默认地址族开始。
		if len(service.Spec.IPFamilies) == 0 {
			service.Spec.IPFamilies = []api.IPFamily{al.defaultServiceIPFamily}
		}

		// 这里沿用普通 Service 的双栈行为；单栈集群是例外，校验允许用户创建多地址族的 Headless Service。
		if len(service.Spec.IPFamilies) < 2 {
			if *(service.Spec.IPFamilyPolicy) != api.IPFamilyPolicySingleStack {
				// 补充另一地址族。
				if service.Spec.IPFamilies[0] == api.IPv4Protocol {
					service.Spec.IPFamilies = append(service.Spec.IPFamilies, api.IPv6Protocol)
				} else {
					service.Spec.IPFamilies = append(service.Spec.IPFamilies, api.IPv4Protocol)
				}
			}
		}

		// 此处无需继续处理。
		return nil
	}

	// 以下所有处理必须位于上述特殊场景之后。

	// 非双栈集群不能要求双栈。
	if getIPFamilyPolicy(service) == api.IPFamilyPolicyRequireDualStack {
		if len(al.serviceIPAllocatorsByFamily) < 2 {
			el = append(el, field.Invalid(field.NewPath("spec", "ipFamilyPolicy"), service.Spec.IPFamilyPolicy,
				"this cluster is not configured for dual-stack services"))
		}
	}

	// 用户请求的地址族必须已在集群中配置。
	for i, ipFamily := range service.Spec.IPFamilies {
		if _, found := al.serviceIPAllocatorsByFamily[ipFamily]; !found {
			el = append(el, field.Invalid(field.NewPath("spec", "ipFamilies").Index(i), ipFamily, "not configured on this cluster"))
		}
	}

	// 存在校验错误时无需继续处理。
	if len(el) > 0 {
		return apierrors.NewInvalid(api.Kind("Service"), service.Name, el)
	}

	// 地址族为空时使用集群默认值。
	if len(service.Spec.IPFamilies) == 0 {
		service.Spec.IPFamilies = []api.IPFamily{al.defaultServiceIPFamily}
	}

	// Service 需要双栈且集群配置了两个地址族时，补充缺失的地址族。
	if *(service.Spec.IPFamilyPolicy) != api.IPFamilyPolicySingleStack &&
		len(service.Spec.IPFamilies) == 1 &&
		len(al.serviceIPAllocatorsByFamily) == 2 {

		if service.Spec.IPFamilies[0] == api.IPv4Protocol {
			service.Spec.IPFamilies = append(service.Spec.IPFamilies, api.IPv6Protocol)
		} else if service.Spec.IPFamilies[0] == api.IPv6Protocol {
			service.Spec.IPFamilies = append(service.Spec.IPFamilies, api.IPv4Protocol)
		}
	}

	return nil
}

// txnAllocClusterIPs 记录本次新分配的 ClusterIP：提交时保留并记录日志，回滚时释放刚分配的地址。
func (al *Allocators) txnAllocClusterIPs(service *api.Service, dryRun bool) (transaction, error) {
	// 本次新分配的 ClusterIP 可能在更高层失败，需要释放。
	allocated, err := al.allocClusterIPs(service, dryRun)
	if err != nil {
		return nil, err
	}

	txn := callbackTransaction{
		revert: func() {
			if dryRun {
				return
			}
			actuallyReleased, err := al.releaseIPs(allocated)
			if err != nil {
				klog.ErrorS(err, "failed to clean up after failed service create",
					"service", klog.KObj(service),
					"shouldRelease", allocated,
					"released", actuallyReleased)
			}
		},
		commit: func() {
			if !dryRun {
				if len(allocated) > 0 {
					klog.InfoS("allocated clusterIPs",
						"service", klog.KObj(service),
						"clusterIPs", allocated)
				}
			}
		},
	}
	return txn, nil
}

// allocClusterIPs 按 IPFamilies 逐个申请或保留 ClusterIP，并同步兼容字段 ClusterIP 与新字段 ClusterIPs。
func (al *Allocators) allocClusterIPs(service *api.Service, dryRun bool) (map[api.IPFamily]string, error) {
	// ExternalName 类型的 Service 不会分配 ClusterIP
	if service.Spec.Type == api.ServiceTypeExternalName {
		return nil, nil
	}

	// headless Service 不会分配 ClusterIP
	if len(service.Spec.ClusterIPs) > 0 && service.Spec.ClusterIPs[0] == api.ClusterIPNone {
		return nil, nil
	}

	toAlloc := make(map[api.IPFamily]string)
	// 此时只确定 Service 的 IP 地址族正确；ClusterIPs 可能部分分配（如升级双栈时），也可能全未分配。
	// 下面循环负责补齐这些值；同时已知集群配置了这些地址族。

	// 切片为空时先按地址族数量初始化容量。
	if service.Spec.ClusterIPs == nil {
		service.Spec.ClusterIPs = make([]string, 0, len(service.Spec.IPFamilies))
	}

	for i, ipFamily := range service.Spec.IPFamilies {
		if i > (len(service.Spec.ClusterIPs) - 1) {
			service.Spec.ClusterIPs = append(service.Spec.ClusterIPs, "" /* 仅作占位 */)
		}

		toAlloc[ipFamily] = service.Spec.ClusterIPs[i]
	}

	// 执行分配
	allocated, err := al.allocIPs(service, toAlloc, dryRun)

	// 分配成功则写入 Service
	if err == nil {
		for family, ip := range allocated {
			for i, check := range service.Spec.IPFamilies {
				if family == check {
					service.Spec.ClusterIPs[i] = ip
					// 严格说无需设置 ClusterIP，但测试 REST 不经过对象转换，而直接校验字段；
					// 因此这里同步设置，满足转换后对象应满足的有效性假设。
					if i == 0 {
						service.Spec.ClusterIP = ip
					}
				}
			}
		}
	}

	return allocated, err
}

// allocIPs 按地址族选择对应分配器：空值执行 AllocateNext，显式 IP 则做静态预留；DryRun 时使用影子分配器。
func (al *Allocators) allocIPs(service *api.Service, toAlloc map[api.IPFamily]string, dryRun bool) (map[api.IPFamily]string, error) {
	allocated := make(map[api.IPFamily]string)

	for family, ip := range toAlloc {
		allocator := al.serviceIPAllocatorsByFamily[family] // 预校验后必然存在
		if dryRun {
			allocator = allocator.DryRun()
		}
		if ip == "" {
			var allocatedIP net.IP
			var err error
			if utilfeature.DefaultFeatureGate.Enabled(features.MultiCIDRServiceAllocator) {
				// TODO: 简化这段重复的类型分支逻辑。
				svcAllocator, ok := allocator.(*ipallocator.MetaAllocator)
				if ok {
					allocatedIP, err = svcAllocator.AllocateNextService(service)
				} else {
					allocatedIP, err = allocator.AllocateNext()
				}
			} else {
				allocatedIP, err = allocator.AllocateNext()
			}
			if err != nil {
				if !errors.Is(err, ipallocator.ErrFull) {
					el := field.ErrorList{field.Invalid(field.NewPath("spec", "clusterIPs"), service.Spec.ClusterIPs, fmt.Sprintf("failed to allocate IP: %v", err))}
					return allocated, apierrors.NewInvalid(api.Kind("Service"), service.Name, el)
				}
				return allocated, apierrors.NewInternalError(fmt.Errorf("failed to allocate a serviceIP for Service %q: %w", service.Name, err))
			}
			allocated[family] = allocatedIP.String()
		} else {
			parsedIP := netutils.ParseIPSloppy(ip)
			if parsedIP == nil {
				return allocated, apierrors.NewInternalError(fmt.Errorf("failed to parse service IP %q", ip))
			}
			var err error
			if utilfeature.DefaultFeatureGate.Enabled(features.MultiCIDRServiceAllocator) {
				// TODO: 简化这段重复的类型分支逻辑。
				svcAllocator, ok := allocator.(*ipallocator.MetaAllocator)
				if ok {
					err = svcAllocator.AllocateService(service, parsedIP)
				} else {
					err = allocator.Allocate(parsedIP)
				}
			} else {
				err = allocator.Allocate(parsedIP)
			}
			if err != nil {
				el := field.ErrorList{field.Invalid(field.NewPath("spec", "clusterIPs"), service.Spec.ClusterIPs, fmt.Sprintf("failed to allocate IP %v: %v", ip, err))}
				return allocated, apierrors.NewInvalid(api.Kind("Service"), service.Name, el)
			}
			allocated[family] = ip
		}
	}
	return allocated, nil
}

// releaseIPs 按地址族释放 ClusterIP；地址族已从集群配置移除时记录跳过，不调用不存在的分配器。
func (al *Allocators) releaseIPs(toRelease map[api.IPFamily]string) (map[api.IPFamily]string, error) {
	if toRelease == nil {
		return nil, nil
	}

	released := make(map[api.IPFamily]string)
	for family, ip := range toRelease {
		allocator, ok := al.serviceIPAllocatorsByFamily[family]
		if !ok {
			// 集群可能曾配置双栈，后来切换为单栈。
			klog.InfoS("Not releasing ClusterIP because related family is not enabled", "clusterIP", ip, "family", family)
			continue
		}

		parsedIP := netutils.ParseIPSloppy(ip)
		if parsedIP == nil {
			return released, apierrors.NewInternalError(fmt.Errorf("failed to parse service IP %q", ip))
		}
		if err := allocator.Release(parsedIP); err != nil {
			return released, err
		}
		released[family] = ip
	}

	return released, nil
}

// txnAllocNodePorts 用 PortAllocationOperation 立即预留新端口、延迟释放旧端口：Service 写入成功才提交释放，失败则撤销新预留。
func (al *Allocators) txnAllocNodePorts(service *api.Service, dryRun bool) (transaction, error) {
	// 分配器内部自行跟踪 dry-run 状态。
	nodePortOp := portallocator.StartOperation(al.serviceNodePorts, dryRun)

	txn := callbackTransaction{
		commit: func() {
			nodePortOp.Commit()
			// 这里并非必须调用 Finish()，但分配器包要求调用；为保持后续兼容仍执行。
			nodePortOp.Finish()
		},
		revert: func() {
			// 名字有些奇怪，但没有调用 commit 时它会回滚本次分配
			nodePortOp.Finish()
		},
	}

	// 如有需要，分配 NodePort。
	if service.Spec.Type == api.ServiceTypeNodePort || service.Spec.Type == api.ServiceTypeLoadBalancer {
		if err := initNodePorts(service, nodePortOp); err != nil {
			txn.Revert()
			return nil, err
		}
	}

	// 处理 Service 创建时与 ExternalTraffic 相关的字段。
	if apiservice.NeedsHealthCheck(service) {
		if err := al.allocHealthCheckNodePort(service, nodePortOp); err != nil {
			txn.Revert()
			return nil, apierrors.NewInternalError(err)
		}
	}

	return txn, nil
}

func initNodePorts(service *api.Service, nodePortOp *portallocator.PortAllocationOperation) error {
	svcPortToNodePort := map[int]int{}
	for i := range service.Spec.Ports {
		servicePort := &service.Spec.Ports[i]
		if servicePort.NodePort == 0 && !shouldAllocateNodePorts(service) {
			// 不分配新的端口，但会尊重用户显式指定的端口。
			continue
		}
		allocatedNodePort := svcPortToNodePort[int(servicePort.Port)]
		if allocatedNodePort == 0 {
			// 只在 service.Spec.Ports 列表向后扫描，因为靠前的匹配已经在
			// svcPortToNodePort 中找过了。这里实际上只是想找出
			// 用户自行指定的取值。
			np := findRequestedNodePort(int(servicePort.Port), service.Spec.Ports)
			if np != 0 {
				err := nodePortOp.Allocate(np)
				if err != nil {
					// TODO: 等校验逻辑按版本区分后，这里会变得更复杂。
					el := field.ErrorList{field.Invalid(field.NewPath("spec", "ports").Index(i).Child("nodePort"), np, err.Error())}
					return apierrors.NewInvalid(api.Kind("Service"), service.Name, el)
				}
				servicePort.NodePort = int32(np)
				svcPortToNodePort[int(servicePort.Port)] = np
			} else {
				nodePort, err := nodePortOp.AllocateNext()
				if err != nil {
					// TODO: 这里该返回什么错误？它既不是
					// 字段级校验失败（字段本身合法），
					// 也不算真正的内部错误。
					return apierrors.NewInternalError(fmt.Errorf("failed to allocate a nodePort: %w", err))
				}
				servicePort.NodePort = int32(nodePort)
				svcPortToNodePort[int(servicePort.Port)] = nodePort
			}
		} else if int(servicePort.NodePort) != allocatedNodePort {
			// TODO(xiangpengzhao): 这种情况下需要分配新的 NodePort 吗？
			// 注意：当前实现更好，因为它省下了一个 NodePort。
			if servicePort.NodePort == 0 {
				servicePort.NodePort = int32(allocatedNodePort)
			} else {
				err := nodePortOp.Allocate(int(servicePort.NodePort))
				if err != nil {
					// TODO: 等校验逻辑按版本区分后，这里会变得更复杂。
					el := field.ErrorList{field.Invalid(field.NewPath("spec", "ports").Index(i).Child("nodePort"), servicePort.NodePort, err.Error())}
					return apierrors.NewInvalid(api.Kind("Service"), service.Name, el)
				}
			}
		}
	}

	return nil
}

// allocHealthCheckNodePort 为 Service 分配健康检查节点端口。
func (al *Allocators) allocHealthCheckNodePort(service *api.Service, nodePortOp *portallocator.PortAllocationOperation) error {
	healthCheckNodePort := service.Spec.HealthCheckNodePort
	if healthCheckNodePort != 0 {
		// 如果请求指定了健康检查节点端口，尝试预留该端口。
		err := nodePortOp.Allocate(int(healthCheckNodePort))
		if err != nil {
			return fmt.Errorf("failed to allocate requested HealthCheck NodePort %v: %v",
				healthCheckNodePort, err)
		}
	} else {
		// 如果请求没有指定健康检查节点端口，则分配任意可用端口。
		healthCheckNodePort, err := nodePortOp.AllocateNext()
		if err != nil {
			return fmt.Errorf("failed to allocate a HealthCheck NodePort %v: %v", healthCheckNodePort, err)
		}
		service.Spec.HealthCheckNodePort = int32(healthCheckNodePort)
	}
	return nil
}

// allocateUpdate 将 ClusterIP、NodePort、HealthCheckNodePort 等更新拆成多个子事务，最后统一提交或回滚。
func (al *Allocators) allocateUpdate(after After, before Before, dryRun bool) (transaction, error) {
	result := metaTransaction{}
	success := false

	defer func() {
		if !success {
			result.Revert()
		}
	}()

	// 在此初始化 IP 地址族字段，确保 dryRun == true 时也能看到初始化结果。
	if err := al.initIPFamilyFields(after, before); err != nil {
		return nil, err
	}

	// 分配 ClusterIP。
	//TODO(thockin): 校验本不应允许空 clusterIP，但当前会放行且已有测试覆盖。彻底修复改动较大，留待后续处理。
	if txn, err := al.txnUpdateClusterIPs(after, before, dryRun); err != nil {
		return nil, err
	} else {
		result = append(result, txn)
	}

	// 分配端口。
	if txn, err := al.txnUpdateNodePorts(after, before, dryRun); err != nil {
		return nil, err
	} else {
		result = append(result, txn)
	}

	success = true
	return result, nil
}

func (al *Allocators) txnUpdateClusterIPs(after After, before Before, dryRun bool) (transaction, error) {
	service := after.Service

	allocated, released, err := al.updateClusterIPs(after, before, dryRun)
	if err != nil {
		return nil, err
	}

	// 失败时：新分配的 IP 必须释放回去
	// 失败时：原本将要释放的已分配 IP
	//             绝不能被释放
	// 成功时：应当释放的已分配 IP 会被释放
	//             掉
	txn := callbackTransaction{
		commit: func() {
			if dryRun {
				return
			}
			if len(allocated) > 0 {
				klog.InfoS("allocated clusterIPs",
					"service", klog.KObj(service),
					"clusterIPs", allocated)
			}
			if actuallyReleased, err := al.releaseIPs(released); err != nil {
				klog.ErrorS(err, "failed to clean up after successful service update",
					"service", klog.KObj(service),
					"shouldRelease", released,
					"released", actuallyReleased)
			}
		},
		revert: func() {
			if dryRun {
				return
			}
			if actuallyReleased, err := al.releaseIPs(allocated); err != nil {
				klog.ErrorS(err, "failed to clean up after failed service update",
					"service", klog.KObj(service),
					"shouldRelease", allocated,
					"released", actuallyReleased)
			}
		},
	}
	return txn, nil
}

// 处理更新 Service 时的类型切换、双栈升配与降配。
// 本函数不会真正释放 clusterIP，而是返回一个
// map[family]ip，由调用方在其余操作全部成功
// 完成后再执行释放。
func (al *Allocators) updateClusterIPs(after After, before Before, dryRun bool) (allocated map[api.IPFamily]string, toRelease map[api.IPFamily]string, err error) {
	oldService, service := before.Service, after.Service

	// 我们不希望因为集群在双栈与非双栈之间切换，就自动给 PreferDualStack 的 Service
	// 升配（添加 IP）或降配（移除 IP）。
	// 也就是说，只有同时满足下面两个条件时才会升配或降配：
	//
	// - 把 ipFamilyPolicy 改成 "RequireDualStack" 或 "SingleStack"；
	// - 并且新增或移除了次要的 clusterIP 或 ipFamily。
	if isMatchingPreferDualStackClusterIPFields(after, before) {
		return allocated, toRelease, nil // 无需继续处理。
	}

	// 使用场景：
	// A：Service 从 ExternalName 变为 ClusterIP 类型 ==> 全部分配新 IP
	// B：Service 从 ClusterIP 类型变为 ExternalName ==> 释放全部分配的 IP
	// C：Service 升配为双栈 ==> 部分分配
	// D：Service 从双栈降配 ==> 部分释放

	// 场景 A：
	// Service 从 ExternalName 变为非 ExternalName，需要初始化 ClusterIP。
	if oldService.Spec.Type == api.ServiceTypeExternalName && service.Spec.Type != api.ServiceTypeExternalName {
		allocated, err := al.allocClusterIPs(service, dryRun)
		return allocated, nil, err
	}

	// headless Service 提前返回，无需管理 clusterIPs
	if len(oldService.Spec.ClusterIPs) > 0 && oldService.Spec.ClusterIPs[0] == api.ClusterIPNone {
		return nil, nil, nil
	}

	// 场景 B：
	// Service 从非 ExternalName 变为 ExternalName，若已有 ClusterIP 则需要释放。
	if oldService.Spec.Type != api.ServiceTypeExternalName && service.Spec.Type == api.ServiceTypeExternalName {
		toRelease = make(map[api.IPFamily]string)
		for i, family := range oldService.Spec.IPFamilies {
			toRelease[family] = oldService.Spec.ClusterIPs[i]
		}
		return nil, toRelease, nil
	}

	upgraded := len(oldService.Spec.IPFamilies) == 1 && len(service.Spec.IPFamilies) == 2
	downgraded := len(oldService.Spec.IPFamilies) == 2 && len(service.Spec.IPFamilies) == 1

	// 场景 C：
	if upgraded {
		toAllocate := make(map[api.IPFamily]string)
		// 如果次要 IP 有名称就直接取用，否则加一个占位标记
		if len(service.Spec.ClusterIPs) < 2 {
			service.Spec.ClusterIPs = append(service.Spec.ClusterIPs, "" /* marker */)
		}

		toAllocate[service.Spec.IPFamilies[1]] = service.Spec.ClusterIPs[1]

		// 执行分配
		allocated, err := al.allocIPs(service, toAllocate, dryRun)
		// 分配成功则写入 Service
		if err == nil {
			service.Spec.ClusterIPs[1] = allocated[service.Spec.IPFamilies[1]]
		}

		return allocated, nil, err
	}

	// 场景 D：
	if downgraded {
		toRelease = make(map[api.IPFamily]string)
		toRelease[oldService.Spec.IPFamilies[1]] = oldService.Spec.ClusterIPs[1]
		// 注意：这里不释放 clusterIP，交由 action 自身的清理逻辑处理
		return nil, toRelease, err
	}
	// 既不是升配也不是降配
	return nil, nil, nil
}

func (al *Allocators) txnUpdateNodePorts(after After, before Before, dryRun bool) (transaction, error) {
	oldService, service := before.Service, after.Service

	// 分配器内部自行跟踪 dry-run 状态。
	nodePortOp := portallocator.StartOperation(al.serviceNodePorts, dryRun)

	txn := callbackTransaction{
		commit: func() {
			nodePortOp.Commit()
			// 这里并非必须调用 Finish()，但分配器包要求调用；为保持后续兼容仍执行。
			nodePortOp.Finish()
		},
		revert: func() {
			// 名字有些奇怪，但没有调用 commit 时它会回滚本次分配
			nodePortOp.Finish()
		},
	}

	// Service 从 NodePort/LoadBalancer 变为 ExternalName/ClusterIP，若已有 NodePort 则需要释放。
	if (oldService.Spec.Type == api.ServiceTypeNodePort || oldService.Spec.Type == api.ServiceTypeLoadBalancer) &&
		(service.Spec.Type == api.ServiceTypeExternalName || service.Spec.Type == api.ServiceTypeClusterIP) {
		al.releaseNodePorts(oldService, nodePortOp)
	}

	// Service 从任意类型变为 NodePort/LoadBalancer 时，需要更新 NodePort。
	if service.Spec.Type == api.ServiceTypeNodePort || service.Spec.Type == api.ServiceTypeLoadBalancer {
		if err := al.updateNodePorts(After{service}, Before{oldService}, nodePortOp); err != nil {
			txn.Revert()
			return nil, err
		}
	}

	// 处理与 ExternalTraffic 相关的更新。
	success, err := al.updateHealthCheckNodePort(After{service}, Before{oldService}, nodePortOp)
	if !success || err != nil {
		txn.Revert()
		return nil, err
	}

	return txn, nil
}

func (al *Allocators) releaseNodePorts(service *api.Service, nodePortOp *portallocator.PortAllocationOperation) {
	nodePorts := collectServiceNodePorts(service)

	for _, nodePort := range nodePorts {
		nodePortOp.ReleaseDeferred(nodePort)
	}
}

func (al *Allocators) updateNodePorts(after After, before Before, nodePortOp *portallocator.PortAllocationOperation) error {
	oldService, newService := before.Service, after.Service

	oldNodePortsNumbers := collectServiceNodePorts(oldService)
	newNodePorts := []ServiceNodePort{}
	portAllocated := map[int]bool{}

	for i := range newService.Spec.Ports {
		servicePort := &newService.Spec.Ports[i]
		if servicePort.NodePort == 0 && !shouldAllocateNodePorts(newService) {
			// 不分配新的端口，但会尊重用户显式指定的端口。
			continue
		}
		nodePort := ServiceNodePort{Protocol: servicePort.Protocol, NodePort: servicePort.NodePort}
		if nodePort.NodePort != 0 {
			if !containsNumber(oldNodePortsNumbers, int(nodePort.NodePort)) && !portAllocated[int(nodePort.NodePort)] {
				err := nodePortOp.Allocate(int(nodePort.NodePort))
				if err != nil {
					el := field.ErrorList{field.Invalid(field.NewPath("spec", "ports").Index(i).Child("nodePort"), nodePort.NodePort, err.Error())}
					return apierrors.NewInvalid(api.Kind("Service"), newService.Name, el)
				}
				portAllocated[int(nodePort.NodePort)] = true
			}
		} else {
			nodePortNumber, err := nodePortOp.AllocateNext()
			if err != nil {
				// TODO: 这里该返回什么错误？它既不是
				// 字段级校验失败（字段本身合法），
				// 也不算真正的内部错误。
				return apierrors.NewInternalError(fmt.Errorf("failed to allocate a nodePort: %w", err))
			}
			servicePort.NodePort = int32(nodePortNumber)
			nodePort.NodePort = servicePort.NodePort
		}
		if containsNodePort(newNodePorts, nodePort) {
			return fmt.Errorf("duplicate nodePort: %v", nodePort)
		}
		newNodePorts = append(newNodePorts, nodePort)
	}

	newNodePortsNumbers := collectServiceNodePorts(newService)

	// 下面的比较循环是 O(N^2)，但 N 不会很大
	// （端口数硬上限是 2^16，实际哪怕 4 个端口也算多了）
	for _, oldNodePortNumber := range oldNodePortsNumbers {
		if containsNumber(newNodePortsNumbers, oldNodePortNumber) {
			continue
		}
		nodePortOp.ReleaseDeferred(int(oldNodePortNumber))
	}

	return nil
}

// updateHealthCheckNodePort 负责 HealthCheckNodePort 的分配与释放，
// 并在 Service 更新时按需调整 HealthCheckNodePort。
func (al *Allocators) updateHealthCheckNodePort(after After, before Before, nodePortOp *portallocator.PortAllocationOperation) (bool, error) {
	oldService, service := before.Service, after.Service

	neededHealthCheckNodePort := apiservice.NeedsHealthCheck(oldService)
	oldHealthCheckNodePort := oldService.Spec.HealthCheckNodePort

	needsHealthCheckNodePort := apiservice.NeedsHealthCheck(service)

	switch {
	// 情况 1：从不需要 HealthCheckNodePort 变为需要。
	// 分配一个健康检查节点端口；若用户指定了端口，则尝试预留它。
	// 如有需要，把健康检查节点端口写入 Service 的 HealthCheckNodePort 字段。
	case !neededHealthCheckNodePort && needsHealthCheckNodePort:
		if err := al.allocHealthCheckNodePort(service, nodePortOp); err != nil {
			return false, apierrors.NewInternalError(err)
		}

	// 情况 2：从需要 HealthCheckNodePort 变为不需要。
	// 释放已有的 healthCheckNodePort，并清空 HealthCheckNodePort 字段。
	case neededHealthCheckNodePort && !needsHealthCheckNodePort:
		nodePortOp.ReleaseDeferred(int(oldHealthCheckNodePort))
	}
	return true, nil
}

func (al *Allocators) releaseAllocatedResources(svc *api.Service) {
	al.releaseClusterIPs(svc)

	for _, nodePort := range collectServiceNodePorts(svc) {
		err := al.serviceNodePorts.Release(nodePort)
		if err != nil {
			// 这些情况最终会被对账（reconciliation）或重启流程发现
			utilruntime.HandleError(fmt.Errorf("Error releasing service %s node port %d: %v", svc.Name, nodePort, err))
		}
	}

	if apiservice.NeedsHealthCheck(svc) {
		nodePort := svc.Spec.HealthCheckNodePort
		if nodePort > 0 {
			err := al.serviceNodePorts.Release(int(nodePort))
			if err != nil {
				// 这些情况最终会被对账（reconciliation）或重启流程发现
				utilruntime.HandleError(fmt.Errorf("Error releasing service %s health check node port %d: %v", svc.Name, nodePort, err))
			}
		}
	}
}

// 释放即将删除的 Service 已分配的 ClusterIP
func (al *Allocators) releaseClusterIPs(service *api.Service) (released map[api.IPFamily]string, err error) {
	// ExternalName 类型的 Service 不会分配 ClusterIP
	if service.Spec.Type == api.ServiceTypeExternalName {
		return nil, nil
	}

	// headless Service 不会分配 ClusterIP
	if len(service.Spec.ClusterIPs) > 0 && service.Spec.ClusterIPs[0] == api.ClusterIPNone {
		return nil, nil
	}

	toRelease := make(map[api.IPFamily]string)
	for _, ip := range service.Spec.ClusterIPs {
		if netutils.IsIPv6String(ip) {
			toRelease[api.IPv6Protocol] = ip
		} else {
			toRelease[api.IPv4Protocol] = ip
		}
	}
	return al.releaseIPs(toRelease)
}

func (al *Allocators) Destroy() {
	al.serviceNodePorts.Destroy()
	for _, a := range al.serviceIPAllocatorsByFamily {
		a.Destroy()
	}
}

// 这里是 O(N)，但预期 haystack 规模很小，
// 小到线性查找反而更快
func containsNumber(haystack []int, needle int) bool {
	for _, v := range haystack {
		if v == needle {
			return true
		}
	}
	return false
}

// 这里是 O(N)，但预期 serviceNodePorts 规模很小，
// 小到线性查找反而更快
func containsNodePort(serviceNodePorts []ServiceNodePort, serviceNodePort ServiceNodePort) bool {
	for _, snp := range serviceNodePorts {
		if snp == serviceNodePort {
			return true
		}
	}
	return false
}

// 遍历 Service 端口列表，找到端口号相同且
// NodePort 已指定的那一项并返回其 NodePort，否则返回 0。
func findRequestedNodePort(port int, servicePorts []api.ServicePort) int {
	for i := range servicePorts {
		servicePort := servicePorts[i]
		if port == int(servicePort.Port) && servicePort.NodePort != 0 {
			return int(servicePort.NodePort)
		}
	}
	return 0
}

func shouldAllocateNodePorts(service *api.Service) bool {
	if service.Spec.Type == api.ServiceTypeNodePort {
		return true
	}
	if service.Spec.Type == api.ServiceTypeLoadBalancer {
		return *service.Spec.AllocateLoadBalancerNodePorts
	}
	return false
}

func collectServiceNodePorts(service *api.Service) []int {
	servicePorts := []int{}
	for i := range service.Spec.Ports {
		servicePort := &service.Spec.Ports[i]
		if servicePort.NodePort != 0 {
			servicePorts = append(servicePorts, int(servicePort.NodePort))
		}
	}
	return servicePorts
}

// 判断两个 PreferDualStack 的 Service 的 ClusterIP 字段是否一致
// 前提：old service 是一个合法、已完成默认化处理的 Service（例如从存储中读出）
func isMatchingPreferDualStackClusterIPFields(after After, before Before) bool {
	oldService, service := before.Service, after.Service

	if oldService == nil {
		return false
	}

	if service.Spec.IPFamilyPolicy == nil {
		return false
	}

	// 如果类型发生了变化，就说明这是一次需要
	// 走完整流程的更新。
	if oldService.Spec.Type != service.Spec.Type {
		return false
	}
	// 两者都必须是会分配 IP 的类型
	if service.Spec.Type != api.ServiceTypeClusterIP &&
		service.Spec.Type != api.ServiceTypeNodePort &&
		service.Spec.Type != api.ServiceTypeLoadBalancer {
		return false
	}

	// 两者的 IPFamilyPolicy 都必须是 PreferDualStack
	if service.Spec.IPFamilyPolicy != nil && *(service.Spec.IPFamilyPolicy) != api.IPFamilyPolicyPreferDualStack {
		return false
	}

	if oldService.Spec.IPFamilyPolicy != nil && *(oldService.Spec.IPFamilyPolicy) != api.IPFamilyPolicyPreferDualStack {
		return false
	}

	if !sameClusterIPs(oldService, service) {
		return false
	}

	if !sameIPFamilies(oldService, service) {
		return false
	}

	// 需要在以下方面保持一致：
	// 策略：PreferDualStack
	// ClusterIPs
	// IPFamilies
	return true
}

// 辅助函数，避免各处都做 nil 检查。调用方需要自行
// 校验取到的值是否符合预期。
func getIPFamilyPolicy(svc *api.Service) api.IPFamilyPolicy {
	if svc.Spec.IPFamilyPolicy == nil {
		return "" // 调用方需要自行处理该情况
	}
	return *svc.Spec.IPFamilyPolicy
}

func sameClusterIPs(lhs, rhs *api.Service) bool {
	if len(rhs.Spec.ClusterIPs) != len(lhs.Spec.ClusterIPs) {
		return false
	}

	for i, ip := range rhs.Spec.ClusterIPs {
		if lhs.Spec.ClusterIPs[i] != ip {
			return false
		}
	}

	return true
}

func reducedClusterIPs(after After, before Before) bool {
	oldSvc, newSvc := before.Service, after.Service

	if len(newSvc.Spec.ClusterIPs) == 0 { // 未指定
		return false
	}
	return len(newSvc.Spec.ClusterIPs) < len(oldSvc.Spec.ClusterIPs)
}

func sameIPFamilies(lhs, rhs *api.Service) bool {
	if len(rhs.Spec.IPFamilies) != len(lhs.Spec.IPFamilies) {
		return false
	}

	for i, family := range rhs.Spec.IPFamilies {
		if lhs.Spec.IPFamilies[i] != family {
			return false
		}
	}

	return true
}

func reducedIPFamilies(after After, before Before) bool {
	oldSvc, newSvc := before.Service, after.Service

	if len(newSvc.Spec.IPFamilies) == 0 { // 未指定
		return false
	}
	return len(newSvc.Spec.IPFamilies) < len(oldSvc.Spec.IPFamilies)
}

// 辅助函数：获取给定 IP 的 IP 族。
func familyOf(ip string) api.IPFamily {
	if netutils.IsIPv4String(ip) {
		return api.IPv4Protocol
	}
	if netutils.IsIPv6String(ip) {
		return api.IPv6Protocol
	}
	return api.IPFamily("unknown")
}
