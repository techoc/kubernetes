/*
Copyright 2015 The Kubernetes Authors.

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

package controller

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	eventsv1client "k8s.io/client-go/kubernetes/typed/events/v1"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/retry"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/registry/core/rangeallocation"
	"k8s.io/kubernetes/pkg/registry/core/service/portallocator"
)

// Repair 周期性扫描所有 Service 的 NodePort/HealthCheckNodePort，并与旧 RangeAllocation 对照以重建准确端口位图。
// 实现思路参照 ipallocator/controller/repair.go，此控制器负责修复端口分配状态。
type Repair struct {
	interval      time.Duration
	serviceClient corev1client.ServicesGetter
	portRange     net.PortRange
	alloc         rangeallocation.RangeRegistry
	leaks         map[int]int // 按泄漏端口计数的计数器

	broadcaster events.EventBroadcaster
	recorder    events.EventRecorder
}

// 清理疑似泄漏端口前需要连续检测到泄漏的次数，用于避免端口刚分配但尚未被 Service 使用时发生竞争。
const numRepairsBeforeLeakCleanup = 3

// NewRepair 创建端口修复控制器，定期确保集群内端口分配唯一，并在分配状态不一致时生成提示信息。
func NewRepair(interval time.Duration, serviceClient corev1client.ServicesGetter, eventClient eventsv1client.EventsV1Interface, portRange net.PortRange, alloc rangeallocation.RangeRegistry) *Repair {
	eventBroadcaster := events.NewBroadcaster(&events.EventSinkImpl{Interface: eventClient})
	recorder := eventBroadcaster.NewRecorder(legacyscheme.Scheme, "portallocator-repair-controller")

	registerMetrics()

	return &Repair{
		interval:      interval,
		serviceClient: serviceClient,
		portRange:     portRange,
		alloc:         alloc,
		leaks:         map[int]int{},
		broadcaster:   eventBroadcaster,
		recorder:      recorder,
	}
}

// RunUntil 启动控制器，直到传入的停止通道关闭。
func (c *Repair) RunUntil(onFirstSuccess func(), stopCh chan struct{}) {
	c.broadcaster.StartRecordingToSink(stopCh)
	defer c.broadcaster.Shutdown()

	var once sync.Once
	wait.Until(func() {
		if err := c.runOnce(); err != nil {
			runtime.HandleError(err)
			return
		}
		once.Do(onFirstSuccess)
	}, c.interval, stopCh)
}

// runOnce 执行一轮端口分配状态检查；发生不可恢复的问题时返回错误。
func (c *Repair) runOnce() error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		err := c.doRunOnce()
		if err != nil {
			nodePortRepairReconcileErrors.Inc()
		}
		return err
	})
}

// doRunOnce 执行端口分配状态核对与修复；发生不可恢复的问题时返回错误。
// doRunOnce 从所有 Service 收集实际使用的 NodePort，再与快照比较，修复未登记端口并延迟释放疑似泄漏端口。
func (c *Repair) doRunOnce() error {
	// TODO：如果 Get() 或 ListServices() 使用弱一致性读取，或两者访问了不同的 etcd leader，
	// 就可能破坏防止端口重复分配所依赖的读写顺序保证。ListServices 返回的 ResourceVersion
	// 必须高于 Get() 触发的 etcd 索引；释放逻辑也不能释放已分配端口、但 Service 尚未创建完成的对象。
	// 详见 #8295。

	// 如果 etcd 尚未启动，应先等待一段时间再判定失败；这在 apiserver 与 etcd 同时启动时尤其重要。
	var snapshot *api.RangeAllocation
	var err error
	err = wait.PollUntilContextTimeout(context.Background(), time.Second, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		snapshot, err = c.alloc.Get()
		if err != nil {
			runtime.HandleError(fmt.Errorf("unable to refresh the port allocations: %w", err))
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return fmt.Errorf("unable to refresh the port allocations: %w", err)
	}
	// 如果分配器尚未初始化，则先完成初始化。
	if snapshot.Range == "" {
		snapshot.Range = c.portRange.String()
	}
	// 创建分配器，以便统一执行后续端口核对和修复。
	stored, err := portallocator.NewFromSnapshot(snapshot)
	if err != nil {
		return fmt.Errorf("unable to rebuild allocator from snapshot: %v", err)
	}

	// 此处不传入 ResourceVersion，因为 snapshot 所属集合与 Service 集合不同，二者的资源版本不可比较。
	// 缓存层按集合分别维护 ResourceVersion，这是合理的：理论上不同集合可能存储在不同 etcd，
	// 甚至可能由非 etcd 的存储后端承载。
	list, err := c.serviceClient.Services(metav1.NamespaceAll).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("unable to refresh the port block: %v", err)
	}

	rebuilt, err := portallocator.NewInMemory(c.portRange)
	if err != nil {
		return fmt.Errorf("unable to create port allocator: %v", err)
	}
	// 检查每个 Service 使用的端口，并据此重建预期的分配状态。
	for i := range list.Items {
		svc := &list.Items[i]
		ports := collectServiceNodePorts(svc)
		if len(ports) == 0 {
			continue
		}

		for _, port := range ports {
			switch err := rebuilt.Allocate(port); err {
			case nil:
				if stored.Has(port) {
					// 从旧分配集合中移除该端口，以便随后识别未被 Service 使用的端口。
					stored.Release(port)
				} else {
					// 该端口似乎尚未登记为已分配。
					nodePortRepairPortErrors.WithLabelValues("repair").Inc()
					c.recorder.Eventf(svc, nil, corev1.EventTypeWarning, "PortNotAllocated", "PortAllocation", "Port %d is not allocated; repairing", port)
					runtime.HandleError(fmt.Errorf("the node port %d for service %s/%s is not allocated; repairing", port, svc.Name, svc.Namespace))
				}
				delete(c.leaks, port) // 它正在被使用，因此不可能是泄漏
			case portallocator.ErrAllocated:
				// 端口发生重复分配，需要重新分配。
				nodePortRepairPortErrors.WithLabelValues("duplicate").Inc()
				c.recorder.Eventf(svc, nil, corev1.EventTypeWarning, "PortAlreadyAllocated", "PortAllocation", "Port %d was assigned to multiple services; please recreate service", port)
				runtime.HandleError(fmt.Errorf("the node port %d for service %s/%s was assigned to multiple services; please recreate", port, svc.Name, svc.Namespace))
			case err.(*portallocator.ErrNotInRange):
				// 端口超出配置范围，需要重新分配。
				nodePortRepairPortErrors.WithLabelValues("outOfRange").Inc()
				c.recorder.Eventf(svc, nil, corev1.EventTypeWarning, "PortOutOfRange", "PortAllocation", "Port %d is not within the port range %s; please recreate service", port, c.portRange)
				runtime.HandleError(fmt.Errorf("the port %d for service %s/%s is not within the port range %s; please recreate", port, svc.Name, svc.Namespace, c.portRange))
			case portallocator.ErrFull:
				// 端口池已耗尽，无法完成重新分配。
				nodePortRepairPortErrors.WithLabelValues("full").Inc()
				c.recorder.Eventf(svc, nil, corev1.EventTypeWarning, "PortRangeFull", "PortAllocation", "Port range %s is full; you must widen the port range in order to create new services", c.portRange)
				return fmt.Errorf("the port range %s is full; you must widen the port range in order to create new services", c.portRange)
			default:
				nodePortRepairPortErrors.WithLabelValues("unknown").Inc()
				c.recorder.Eventf(svc, nil, corev1.EventTypeWarning, "UnknownError", "PortAllocation", "Unable to allocate port %d due to an unknown error", port)
				return fmt.Errorf("unable to allocate port %d for service %s/%s due to an unknown error, exiting: %v", port, svc.Name, svc.Namespace, err)
			}
		}
	}

	// 检查旧分配集合中剩余的端口；这些端口可能已经泄漏。
	stored.ForEach(func(port int) {
		count, found := c.leaks[port]
		switch {
		case !found:
			// 标记该端口，待可能存在的并发竞争消退后再清理。
			runtime.HandleError(fmt.Errorf("the node port %d may have leaked: flagging for later clean up", port))
			count = numRepairsBeforeLeakCleanup - 1
			fallthrough
		case count > 0:
			// 在计数器到期前，暂时仍将该端口视为正在使用。
			c.leaks[port] = count - 1
			if err := rebuilt.Allocate(port); err != nil {
				// 此处不增加指标；如果端口确实泄漏，计数器归零后会再次检测到。
				runtime.HandleError(fmt.Errorf("the node port %d may have leaked, but can not be allocated: %v", port, err))
			}
		default:
			nodePortRepairPortErrors.WithLabelValues("leak").Inc()
			// 不将该端口加入重建后的集合，表示清理后它可以重新分配。
			runtime.HandleError(fmt.Errorf("the node port %d appears to have leaked: cleaning up", port))
		}
	})

	// 将重建后的端口分配状态写入持久化存储。
	if err := rebuilt.Snapshot(snapshot); err != nil {
		return fmt.Errorf("unable to snapshot the updated port allocations: %v", err)
	}

	if err := c.alloc.CreateOrUpdate(snapshot); err != nil {
		if errors.IsConflict(err) {
			return err
		}
		return fmt.Errorf("unable to persist the updated port allocations: %v", err)
	}
	return nil
}

// collectServiceNodePorts 汇总 ServicePorts.NodePort 与 HealthCheckNodePort，供端口池修复控制器重建占用集合。注意：
// 1. 相同端口且协议相同的条目会保留重复项；
// 2. 相同端口但协议不同的条目会去重。
func collectServiceNodePorts(service *corev1.Service) []int {
	var servicePorts []int
	// 记录 NodePort 到协议集合的映射。
	seen := make(map[int]sets.Set[string])
	for _, port := range service.Spec.Ports {
		nodePort := int(port.NodePort)
		if nodePort == 0 {
			continue
		}
		proto := string(port.Protocol)
		s := seen[nodePort]
		if s == nil { // 此前未见过该 nodePort
			s = sets.New[string](proto)
			servicePorts = append(servicePorts, nodePort)
		} else if s.Has(proto) { // 相同 nodePort 且协议相同
			servicePorts = append(servicePorts, nodePort)
		} else { // 相同 nodePort 但协议不同
			s.Insert(proto)
		}
		seen[nodePort] = s
	}

	healthPort := int(service.Spec.HealthCheckNodePort)
	if healthPort != 0 {
		s := seen[healthPort]
		// TODO：是否可以安全地假定协议始终为 TCP？
		if s == nil || s.Has(string(corev1.ProtocolTCP)) {
			servicePorts = append(servicePorts, healthPort)
		}
	}

	return servicePorts
}
