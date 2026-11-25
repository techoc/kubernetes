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
	"net"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	eventsv1client "k8s.io/client-go/kubernetes/typed/events/v1"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/retry"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/registry/core/rangeallocation"
	"k8s.io/kubernetes/pkg/registry/core/service/ipallocator"
	netutils "k8s.io/utils/net"
)

// Repair 是旧式 RangeAllocation 的周期性 ClusterIP 修复控制器：根据当前 Service 列表重建快照，发现重复、越界和泄漏地址。
// Repair 是周期性核对 Service ClusterIP 分配状态的控制器，并将准确、紧凑的已分配 IP 集合写回分配器。
// 主要处理：
// - 运维人员操作或未检测到的并发竞争导致的 ClusterIP 重复分配；
// - ClusterIP 与当前配置的地址范围不匹配；
// - 集群崩溃或断电导致地址已分配、但 Service 未成功创建；
// - 自动将旧版本 Kubernetes Service 迁移到原子化 ipallocator 分配模型。
// 该控制器可低频运行，最好在控制面启动时执行。它采用基于当前状态的幂等协调方式；
// 如果执行期间没有并发竞争，一轮处理结束后，所有有效 ClusterIP 都会反映到分配器位图中。
// TODO：必要时为异常 Service 重新分配 IP。
// TODO：完善具体修复策略。
type Repair struct {
	interval      time.Duration
	serviceClient corev1client.ServicesGetter

	networkByFamily   map[v1.IPFamily]*net.IPNet                    // 按 IP 协议族记录控制器负责管理的网络范围。
	allocatorByFamily map[v1.IPFamily]rangeallocation.RangeRegistry // 按 IP 协议族记录对应使用的分配器。

	leaksByFamily map[v1.IPFamily]map[string]int // 按 IP 协议族和 IP 地址记录疑似泄漏次数。
	broadcaster   events.EventBroadcaster
	recorder      events.EventRecorder
}

// 清理疑似泄漏 IP 前需要连续检测到的次数，用于避免 IP 刚分配、尚未被 Service 使用时发生竞争。
const numRepairsBeforeLeakCleanup = 3

// NewRepair 创建 IP 分配修复控制器，定期确保集群内 ClusterIP 唯一分配，并在分配状态不一致时生成提示事件。
func NewRepair(interval time.Duration, serviceClient corev1client.ServicesGetter, eventClient eventsv1client.EventsV1Interface, network *net.IPNet, alloc rangeallocation.RangeRegistry, secondaryNetwork *net.IPNet, secondaryAlloc rangeallocation.RangeRegistry) *Repair {
	eventBroadcaster := events.NewBroadcaster(&events.EventSinkImpl{Interface: eventClient})
	recorder := eventBroadcaster.NewRecorder(legacyscheme.Scheme, "ipallocator-repair-controller")

	// 初始化按 IP 协议族索引的网络、分配器和泄漏计数器。
	networkByFamily := make(map[v1.IPFamily]*net.IPNet)
	allocatorByFamily := make(map[v1.IPFamily]rangeallocation.RangeRegistry)
	leaksByFamily := make(map[v1.IPFamily]map[string]int)

	primary := v1.IPv4Protocol
	secondary := v1.IPv6Protocol
	if netutils.IsIPv6(network.IP) {
		primary = v1.IPv6Protocol
	}

	networkByFamily[primary] = network
	allocatorByFamily[primary] = alloc
	leaksByFamily[primary] = make(map[string]int)

	if secondaryNetwork != nil && secondaryNetwork.IP != nil {
		if primary == v1.IPv6Protocol {
			secondary = v1.IPv4Protocol
		}
		networkByFamily[secondary] = secondaryNetwork
		allocatorByFamily[secondary] = secondaryAlloc
		leaksByFamily[secondary] = make(map[string]int)
	}

	registerMetrics()

	return &Repair{
		interval:      interval,
		serviceClient: serviceClient,

		networkByFamily:   networkByFamily,
		allocatorByFamily: allocatorByFamily,

		leaksByFamily: leaksByFamily,
		broadcaster:   eventBroadcaster,
		recorder:      recorder,
	}
}

// RunUntil 启动周期性修复循环，直到 stopCh 关闭。
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

// runOnce 执行一轮 ClusterIP 分配状态核对；发生不可恢复的问题时返回错误。
func (c *Repair) runOnce() error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		err := c.doRunOnce()
		if err != nil {
			clusterIPRepairReconcileErrors.Inc()
		}
		return err
	})
}

// doRunOnce 分别读取旧快照和当前 Service 列表，重建每个地址族的真实占用集合，再处理泄漏并持久化快照。
func (c *Repair) doRunOnce() error {
	// TODO:（来自 smarterclayton）如果 Get() 或 ListServices() 是弱一致性读取，
	// 或者它们请求了不同的 leader，
	// 那么"同一个 IP 不会被分配两次"所需的顺序保证就会被破坏。
	// ListServices 必须返回比 Get 触发的 etcd index 更高的 ResourceVersion，
	// 且释放逻辑不能释放那些已分配 IP 但尚未创建成功的 Service。
	// 参见 #8295。

	// 如果 etcd 尚未运行，应当先等待一段时间再失败。这一点在 apiserver 和 etcd
	// 同时启动时尤为重要。
	snapshotByFamily := make(map[v1.IPFamily]*api.RangeAllocation)
	storedByFamily := make(map[v1.IPFamily]ipallocator.Interface)

	err := wait.PollUntilContextTimeout(context.Background(), time.Second, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		for family, allocator := range c.allocatorByFamily {
			// 若不存在快照则读取一份
			if _, ok := snapshotByFamily[family]; !ok {
				snapshot, err := allocator.Get()
				if err != nil {
					runtime.HandleError(fmt.Errorf("unable to refresh the service IP block: %w", err))
					return false, nil
				}

				snapshotByFamily[family] = snapshot
			}
		}
		return true, nil
	})

	if err != nil {
		return fmt.Errorf("unable to refresh the service IP block: %w", err)
	}

	// 确保地址段已分配
	for family, snapshot := range snapshotByFamily {
		if snapshot.Range == "" {
			snapshot.Range = c.networkByFamily[family].String()
		}
	}

	// 创建一个分配器，用起来更方便。
	for family, snapshot := range snapshotByFamily {
		stored, err := ipallocator.NewFromSnapshot(snapshot)
		if err != nil {
			return fmt.Errorf("unable to rebuild allocator from snapshots for family:%v with error:%v", family, err)
		}

		storedByFamily[family] = stored
	}

	rebuiltByFamily := make(map[v1.IPFamily]*ipallocator.Range)

	for family, network := range c.networkByFamily {
		rebuilt, err := ipallocator.NewInMemory(network)
		if err != nil {
			return fmt.Errorf("unable to create CIDR range for family %v: %v", family, err)
		}

		rebuiltByFamily[family] = rebuilt
	}
	// 我们显式不传 resourceVersion，因为 snapshot 的 resourceVersion 来自另一个集合，
	// 与 service 集合的不具可比性。缓存层按集合分别维护 RV，
	// 这是合理的，因为理论上这些集合可能存放在不同的 etcd
	//（甚至非 etcd）实例中。
	list, err := c.serviceClient.Services(metav1.NamespaceAll).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("unable to refresh the service IP block: %v", err)
	}

	getFamilyByIP := func(ip net.IP) v1.IPFamily {
		if netutils.IsIPv6(ip) {
			return v1.IPv6Protocol
		}
		return v1.IPv4Protocol
	}

	// 检查每个 Service 的 ClusterIP，并按预期重建状态。
	for _, svc := range list.Items {
		if !helper.IsServiceIPSet(&svc) {
			// 不需要 cluster IP
			continue
		}

		for _, ip := range svc.Spec.ClusterIPs {
			ip := netutils.ParseIPSloppy(ip)
			if ip == nil {
				// cluster IP 已损坏
				clusterIPRepairIPErrors.WithLabelValues("invalid").Inc()
				c.recorder.Eventf(&svc, nil, v1.EventTypeWarning, "ClusterIPNotValid", "ClusterIPValidation", "Cluster IP %s is not a valid IP; please recreate service", ip)
				runtime.HandleError(fmt.Errorf("the cluster IP %s for service %s/%s is not a valid IP; please recreate", ip, svc.Name, svc.Namespace))
				continue
			}

			family := getFamilyByIP(ip)
			if _, ok := rebuiltByFamily[family]; !ok {
				// 该 Service 使用的 IPFamily 已不在集群配置中
				clusterIPRepairIPErrors.WithLabelValues("invalid").Inc()
				c.recorder.Eventf(&svc, nil, v1.EventTypeWarning, "ClusterIPNotValid", "ClusterIPValidation", "Cluster IP %s(%s) is of ip family that is no longer configured on cluster; please recreate service", ip, family)
				runtime.HandleError(fmt.Errorf("the cluster IP %s(%s) for service %s/%s is of ip family that is no longer configured on cluster; please recreate", ip, family, svc.Name, svc.Namespace))
				continue
			}

			// 标记为已占用
			actualAlloc := rebuiltByFamily[family]
			switch err := actualAlloc.Allocate(ip); err {
			case nil:
				actualStored := storedByFamily[family]
				if actualStored.Has(ip) {
					// 从旧集合中移除，以便后续发现泄漏
					actualStored.Release(ip)
				} else {
					// 该 cluster IP 似乎并未被分配
					clusterIPRepairIPErrors.WithLabelValues("repair").Inc()
					c.recorder.Eventf(&svc, nil, v1.EventTypeWarning, "ClusterIPNotAllocated", "ClusterIPAllocation", "Cluster IP [%v]:%s is not allocated; repairing", family, ip)
					runtime.HandleError(fmt.Errorf("the cluster IP [%v]:%s for service %s/%s is not allocated; repairing", family, ip, svc.Name, svc.Namespace))
				}
				delete(c.leaksByFamily[family], ip.String()) // 它正在被使用，因此不可能是泄漏
			case ipallocator.ErrAllocated:
				// cluster IP 重复
				clusterIPRepairIPErrors.WithLabelValues("duplicate").Inc()
				c.recorder.Eventf(&svc, nil, v1.EventTypeWarning, "ClusterIPAlreadyAllocated", "ClusterIPAllocation", "Cluster IP [%v]:%s was assigned to multiple services; please recreate service", family, ip)
				runtime.HandleError(fmt.Errorf("the cluster IP [%v]:%s for service %s/%s was assigned to multiple services; please recreate", family, ip, svc.Name, svc.Namespace))
			case err.(*ipallocator.ErrNotInRange):
				// cluster IP 超出地址段范围
				clusterIPRepairIPErrors.WithLabelValues("outOfRange").Inc()
				c.recorder.Eventf(&svc, nil, v1.EventTypeWarning, "ClusterIPOutOfRange", "ClusterIPAllocation", "Cluster IP [%v]:%s is not within the service CIDR %s; please recreate service", family, ip, c.networkByFamily[family])
				runtime.HandleError(fmt.Errorf("the cluster IP [%v]:%s for service %s/%s is not within the service CIDR %s; please recreate", family, ip, svc.Name, svc.Namespace, c.networkByFamily[family]))
			case ipallocator.ErrFull:
				// 不知为何 IP 已耗尽
				clusterIPRepairIPErrors.WithLabelValues("full").Inc()
				cidr := actualAlloc.CIDR()
				c.recorder.Eventf(&svc, nil, v1.EventTypeWarning, "ServiceCIDRFull", "ClusterIPAllocation", "Service CIDR %v is full; you must widen the CIDR in order to create new services for Cluster IP [%v]:%s", cidr, family, ip)
				return fmt.Errorf("the service CIDR %v is full; you must widen the CIDR in order to create new services for Cluster IP [%v]:%s", cidr, family, ip)
			default:
				clusterIPRepairIPErrors.WithLabelValues("unknown").Inc()
				c.recorder.Eventf(&svc, nil, v1.EventTypeWarning, "UnknownError", "ClusterIPAllocation", "Unable to allocate cluster IP [%v]:%s due to an unknown error", family, ip)
				return fmt.Errorf("unable to allocate cluster IP [%v]:%s for service %s/%s due to an unknown error, exiting: %v", family, ip, svc.Name, svc.Namespace, err)
			}
		}
	}

	// 泄漏检查
	for family, leaks := range c.leaksByFamily {
		c.checkLeaked(leaks, storedByFamily[family], rebuiltByFamily[family])
	}

	// 保存逻辑
	// 把重建出来的状态写回存储。
	for family, rebuilt := range rebuiltByFamily {
		err = c.saveSnapShot(rebuilt, c.allocatorByFamily[family], snapshotByFamily[family])
		if err != nil {
			return err
		}
	}

	return nil
}

// saveSnapShot 将重建后的位图写回 RangeAllocation；ResourceVersion 冲突交给外层 RetryOnConflict 重新执行整轮修复。
func (c *Repair) saveSnapShot(rebuilt *ipallocator.Range, alloc rangeallocation.RangeRegistry, snapshot *api.RangeAllocation) error {
	if err := rebuilt.Snapshot(snapshot); err != nil {
		return fmt.Errorf("unable to snapshot the updated service IP allocations: %v", err)
	}
	if err := alloc.CreateOrUpdate(snapshot); err != nil {
		if errors.IsConflict(err) {
			return err
		}
		return fmt.Errorf("unable to persist the updated service IP allocations: %v", err)
	}

	return nil
}

// checkLeaked 对连续多轮未被 Service 使用的旧分配做延迟回收，给并发创建/写入留下竞态缓冲。
func (c *Repair) checkLeaked(leaks map[string]int, stored ipallocator.Interface, rebuilt *ipallocator.Range) {
	// 检查旧集合中剩余的 IP，它们看起来是被泄漏了。
	stored.ForEach(func(ip net.IP) {
		count, found := leaks[ip.String()]
		switch {
		case !found:
			// 打上标记，等竞态（希望如此）消失后再清理
			runtime.HandleError(fmt.Errorf("the cluster IP %s may have leaked: flagging for later clean up", ip))
			count = numRepairsBeforeLeakCleanup - 1
			fallthrough
		case count > 0:
			// 在计数耗尽之前假装它仍被占用
			leaks[ip.String()] = count - 1
			if err := rebuilt.Allocate(ip); err != nil {
				// 这里不要递增指标：如果确实是泄漏，计数器归零时会检测到
				runtime.HandleError(fmt.Errorf("the cluster IP %s may have leaked, but can not be allocated: %v", ip, err))
			}
		default:
			clusterIPRepairIPErrors.WithLabelValues("leak").Inc()
			// 不要把它加入重建的集合，也就是说它可以被重新复用
			runtime.HandleError(fmt.Errorf("the cluster IP %s appears to have leaked: cleaning up", ip))
		}
	})
}
