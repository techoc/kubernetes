/*
Copyright 2021 The Kubernetes Authors.

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

package nodeshutdown

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/apis/scheduling"
	"k8s.io/kubernetes/pkg/features"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/kubelet/eviction"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/volumemanager"
	"k8s.io/utils/clock"
)

// Manager 接口提供 kubelet 管理节点关机的入口。
type Manager interface {
	lifecycle.PodAdmitHandler

	// Admit 在节点关机期间拒绝新 Pod。
	Admit(ctx context.Context, attrs *lifecycle.PodAdmitAttributes) lifecycle.PodAdmitResult
	// Start 启动关机检测。
	Start(ctx context.Context) error
	// ShutdownStatus 返回当前关机状态。
	ShutdownStatus() error
}

// Config 是 Manager 的构造配置。
type Config struct {
	Logger                           klog.Logger
	VolumeManager                    volumemanager.VolumeManager
	Recorder                         record.EventRecorder
	NodeRef                          *v1.ObjectReference
	GetPodsFunc                      eviction.ActivePodsFunc
	KillPodFunc                      eviction.KillPodFunc
	SyncNodeStatusFunc               func(context.Context)
	ShutdownGracePeriodRequested     time.Duration
	ShutdownGracePeriodCriticalPods  time.Duration
	ShutdownGracePeriodByPodPriority []kubeletconfig.ShutdownGracePeriodByPodPriority
	StateDirectory                   string
	Clock                            clock.Clock
}

// managerStub 是用于不支持关机管理平台的空实现。
type managerStub struct{}

// Admit 始终允许 Pod 准入。
func (managerStub) Admit(_ context.Context, attrs *lifecycle.PodAdmitAttributes) lifecycle.PodAdmitResult {
	return lifecycle.PodAdmitResult{Admit: true}
}

// Start 在非 Linux 平台为空操作，始终返回 nil。
func (managerStub) Start(context.Context) error {
	return nil
}

// ShutdownStatus 在非 Linux 平台为空操作，始终返回 nil。
func (managerStub) ShutdownStatus() error {
	return nil
}

// 关机事件使用的原因、消息与状态文件名。
const (
	NodeShutdownNotAdmittedReason  = "NodeShutdown"
	nodeShutdownNotAdmittedMessage = "Pod was rejected as the node is shutting down."
	localStorageStateFile          = "graceful_node_shutdown_state"

	nodeShutdownReason  = "Terminated"
	nodeShutdownMessage = "Pod was terminated in response to imminent node shutdown."
)

// podManager 按优先级分组终止活跃 Pod。
type podManager struct {
	logger                           klog.Logger
	shutdownGracePeriodByPodPriority []kubeletconfig.ShutdownGracePeriodByPodPriority
	clock                            clock.Clock
	killPodFunc                      eviction.KillPodFunc
	volumeManager                    volumemanager.VolumeManager
}

// newPodManager 根据配置构造按优先级关机的管理器。
func newPodManager(conf *Config) *podManager {
	shutdownGracePeriodByPodPriority := conf.ShutdownGracePeriodByPodPriority

	// 特性未开启或未配置按优先级宽限期时，从旧的两个宽限期字段迁移。
	if !utilfeature.DefaultFeatureGate.Enabled(features.GracefulNodeShutdownBasedOnPodPriority) ||
		len(shutdownGracePeriodByPodPriority) == 0 {
		shutdownGracePeriodByPodPriority = migrateConfig(conf.ShutdownGracePeriodRequested, conf.ShutdownGracePeriodCriticalPods)
	}

	// 按优先级从低到高排序，低优先级先被终止。
	sort.Slice(shutdownGracePeriodByPodPriority, func(i, j int) bool {
		return shutdownGracePeriodByPodPriority[i].Priority < shutdownGracePeriodByPodPriority[j].Priority
	})

	if conf.Clock == nil {
		conf.Clock = clock.RealClock{}
	}

	return &podManager{
		logger:                           conf.Logger,
		shutdownGracePeriodByPodPriority: shutdownGracePeriodByPodPriority,
		clock:                            conf.Clock,
		killPodFunc:                      conf.KillPodFunc,
		volumeManager:                    conf.VolumeManager,
	}
}

// killPods 按优先级分组终止 Pod：低优先级组先处理，每组等待其宽限期。
func (m *podManager) killPods(ctx context.Context, activePods []*v1.Pod) error {
	groups := groupByPriority(m.shutdownGracePeriodByPodPriority, activePods)
	for _, group := range groups {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// 该优先级区间没有 Pod 时直接跳过，不浪费宽限期。
		if len(group.Pods) == 0 {
			continue
		}

		var wg sync.WaitGroup
		wg.Add(len(group.Pods))
		for _, pod := range group.Pods {
			go func(pod *v1.Pod, group podShutdownGroup) {
				defer wg.Done()

				gracePeriodOverride := group.ShutdownGracePeriodSeconds

				// Pod 自身指定的终止宽限期更短时，使用 Pod 的值。
				if pod.Spec.TerminationGracePeriodSeconds != nil && *pod.Spec.TerminationGracePeriodSeconds <= gracePeriodOverride {
					gracePeriodOverride = *pod.Spec.TerminationGracePeriodSeconds
				}

				m.logger.V(1).Info("Shutdown manager killing pod with gracePeriod", "pod", klog.KObj(pod), "gracePeriod", gracePeriodOverride)

				if err := m.killPodFunc(pod, false, &gracePeriodOverride, func(status *v1.PodStatus) {
					// 把 Pod 状态置为 Failed（已成功终止的保持不变），并记录关机原因。
					if status.Phase != v1.PodSucceeded {
						status.Phase = v1.PodFailed
					}
					status.Message = nodeShutdownMessage
					status.Reason = nodeShutdownReason
					podutil.UpdatePodCondition(status, &v1.PodCondition{
						Type:               v1.DisruptionTarget,
						ObservedGeneration: podutil.CalculatePodConditionObservedGeneration(status, pod.Generation, v1.DisruptionTarget),
						Status:             v1.ConditionTrue,
						Reason:             v1.PodReasonTerminationByKubelet,
						Message:            nodeShutdownMessage,
					})
				}); err != nil {
					m.logger.V(1).Info("Shutdown manager failed killing pod", "pod", klog.KObj(pod), "err", err)
				} else {
					m.logger.V(1).Info("Shutdown manager finished killing pod", "pod", klog.KObj(pod))
				}
			}(pod, group)
		}

		// 该时长决定关机管理器等待本组 Pod 终止多久，然后进入下一组。
		var groupTerminationWaitDuration = time.Duration(group.ShutdownGracePeriodSeconds) * time.Second
		var (
			doneCh              = make(chan struct{})
			timer               = m.clock.NewTimer(groupTerminationWaitDuration)
			groupCtx, ctxCancel = context.WithTimeout(ctx, groupTerminationWaitDuration)
		)
		go func() {
			defer close(doneCh)
			defer ctxCancel()
			wg.Wait()
			// 终止信号已发送给所有 Pod，继续下一组前先等待卷卸载完成，
			// 给 CSI 驱动（通常属于最高优先级组）执行卸载的机会。
			// 等待基于 kubelet context，父 context 取消时可提前结束。
			if err := m.volumeManager.WaitForAllPodsUnmount(groupCtx, group.Pods); err != nil {
				var podIdentifiers []string
				for _, pod := range group.Pods {
					podIdentifiers = append(podIdentifiers, fmt.Sprintf("%s/%s", pod.Namespace, pod.Name))
				}

				// 卷卸载只是尽力而为：记录错误后继续。
				// 根据配置，可能是 timer 到期后进入下一组，
				// 也可能是 WaitForAllPodsUnmount 超时后关闭 doneCh 再继续。
				m.logger.Error(err, "Failed while waiting for all the volumes belonging to Pods in this group to unmount", "pods", podIdentifiers)
			}
		}()

		select {
		case <-doneCh:
			timer.Stop()
			m.logger.V(1).Info("Done waiting for all pods in group to terminate", "gracePeriod", group.ShutdownGracePeriodSeconds, "priority", group.Priority)
		case <-timer.C():
			ctxCancel()
			m.logger.V(1).Info("Shutdown manager pod killing time out", "gracePeriod", group.ShutdownGracePeriodSeconds, "priority", group.Priority)
		}
	}

	return nil
}

// periodRequested 返回所有优先级组宽限期之和。
func (m *podManager) periodRequested() time.Duration {
	var sum int64
	for _, period := range m.shutdownGracePeriodByPodPriority {
		sum += period.ShutdownGracePeriodSeconds
	}
	return time.Duration(sum) * time.Second
}

// migrateConfig 把旧的“总宽限期 + 关键 Pod 宽限期”配置转换为按优先级的分组配置。
func migrateConfig(shutdownGracePeriodRequested, shutdownGracePeriodCriticalPods time.Duration) []kubeletconfig.ShutdownGracePeriodByPodPriority {
	if shutdownGracePeriodRequested == 0 {
		return nil
	}
	// 默认优先级组获得总宽限期减去关键 Pod 宽限期的部分。
	defaultPriority := shutdownGracePeriodRequested - shutdownGracePeriodCriticalPods
	if defaultPriority < 0 {
		return nil
	}
	criticalPriority := shutdownGracePeriodRequested - defaultPriority
	if criticalPriority < 0 {
		return nil
	}
	return []kubeletconfig.ShutdownGracePeriodByPodPriority{
		{
			Priority:                   scheduling.DefaultPriorityWhenNoDefaultClassExists,
			ShutdownGracePeriodSeconds: int64(defaultPriority / time.Second),
		},
		{
			Priority:                   scheduling.SystemCriticalPriority,
			ShutdownGracePeriodSeconds: int64(criticalPriority / time.Second),
		},
	}
}

// podShutdownGroup 是同一优先级区间内的 Pod 集合。
type podShutdownGroup struct {
	kubeletconfig.ShutdownGracePeriodByPodPriority
	Pods []*v1.Pod
}

// groupByPriority 按优先级把 Pod 分配到对应宽限期组。
func groupByPriority(shutdownGracePeriodByPodPriority []kubeletconfig.ShutdownGracePeriodByPodPriority, pods []*v1.Pod) []podShutdownGroup {
	groups := make([]podShutdownGroup, 0, len(shutdownGracePeriodByPodPriority))
	for _, period := range shutdownGracePeriodByPodPriority {
		groups = append(groups, podShutdownGroup{
			ShutdownGracePeriodByPodPriority: period,
		})
	}

	for _, pod := range pods {
		var priority int32
		if pod.Spec.Priority != nil {
			priority = *pod.Spec.Priority
		}

		// 用二分查找定位优先级对应的组。
		index := sort.Search(len(groups), func(i int) bool {
			return groups[i].Priority >= priority
		})

		// 越界处理：高于最高优先级的归入最高组，低于最低优先级的归入最低组。
		// 边界优先级归入较低组：当
		// groups[index-1].Priority <= pod priority < groups[index].Priority
		// 时选择 index-1。
		if index == len(groups) {
			index = len(groups) - 1
		} else if index < 0 {
			index = 0
		} else if index > 0 && groups[index].Priority > priority {
			index--
		}

		groups[index].Pods = append(groups[index].Pods, pod)
	}
	return groups
}
