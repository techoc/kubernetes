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

package prober

import (
	"context"

	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/record"
	"k8s.io/component-base/metrics"
	"k8s.io/klog/v2"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/features"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/prober/results"
	"k8s.io/kubernetes/pkg/kubelet/status"
	kubeutil "k8s.io/kubernetes/pkg/kubelet/util"
	"k8s.io/utils/clock"
)

// ProberResults 以 Prometheus 指标的形式记录各类探针按结果累计的执行次数。
var ProberResults = metrics.NewCounterVec(
	&metrics.CounterOpts{
		Subsystem:      "prober",
		Name:           "probe_total",
		Help:           "Cumulative number of a liveness, readiness or startup probe for a container by result.",
		StabilityLevel: metrics.BETA,
	},
	[]string{"probe_type",
		"result",
		"container",
		"pod",
		"namespace",
		"pod_uid"},
)

// ProberDuration 以 Prometheus 指标的形式记录一次成功探针执行的耗时。
var ProberDuration = metrics.NewHistogramVec(
	&metrics.HistogramOpts{
		Subsystem:      "prober",
		Name:           "probe_duration_seconds",
		Help:           "Duration in seconds for a probe response.",
		StabilityLevel: metrics.ALPHA,
	},
	[]string{"probe_type",
		"container",
		"pod",
		"namespace"},
)

// Manager 负责管理 Pod 的探针。对每个声明了探针的容器，AddPod 会为其创建一个探针
// “worker”，worker 周期性地探测所属容器并缓存结果；当需要刷新状态时，Manager 利用
// 缓存的探测结果在 PodStatus 中设置合适的 Ready 状态（见 UpdatePodStatus）。
// 目前不支持在运行中更新探针参数。
type Manager interface {
	// AddPod 为容器上声明的每种探针创建新的探测 worker，每个创建的 Pod 都应调用一次。
	AddPod(ctx context.Context, pod *v1.Pod)

	// StopLivenessAndStartup 在容器终止期间停止其存活性与启动探针。
	StopLivenessAndStartup(pod *v1.Pod)

	// RemovePod 负责清理已删除 Pod 的残留状态，包括终止探测 worker 并删除缓存的探测结果。
	RemovePod(pod *v1.Pod)

	// CleanupPods 负责清理不应继续运行的 Pod。
	// 传入的参数是“期望存在”的 Pod 集合，该集合中的 Pod 不会被清理。
	CleanupPods(desiredPods map[types.UID]sets.Empty)

	// UpdatePodStatus 依据容器的运行状态、缓存的探测结果以及 worker 状态，
	// 修改传入的 PodStatus，为每个容器设置恰当的 Ready 状态。
	UpdatePodStatus(context.Context, *v1.Pod, *v1.PodStatus)
}

type manager struct {
	// workers 按探测标识保存当前活跃的探测 worker
	workers map[probeKey]*worker
	// workerLock 保护 workers 的并发读写
	workerLock sync.RWMutex

	// statusManager 的缓存为探测过程提供 Pod IP 与容器 ID。
	statusManager status.Manager

	// readinessManager 保存就绪探针的结果
	readinessManager results.Manager

	// livenessManager 保存存活性探针的结果
	livenessManager results.Manager

	// startupManager 保存启动探针的结果
	startupManager results.Manager

	// prober 实际执行探测动作。
	prober *prober

	start time.Time
}

// NewManager 创建一个用于 Pod 探针管理的 Manager。
func NewManager(
	statusManager status.Manager,
	livenessManager results.Manager,
	readinessManager results.Manager,
	startupManager results.Manager,
	runner kubecontainer.CommandRunner,
	recorder record.EventRecorderLogger) Manager {

	prober := newProber(runner, recorder)
	return &manager{
		statusManager:    statusManager,
		prober:           prober,
		readinessManager: readinessManager,
		livenessManager:  livenessManager,
		startupManager:   startupManager,
		workers:          make(map[probeKey]*worker),
		start:            clock.RealClock{}.Now(),
	}
}

// probeKey 唯一标识一个容器的某类探针。
type probeKey struct {
	podUID        types.UID
	containerName string
	probeType     probeType
}

// probeType 标识探针的类型：存活性、就绪性或启动。
type probeType int

const (
	liveness probeType = iota
	readiness
	startup

	probeResultSuccessful string = "successful"
	probeResultFailed     string = "failed"
	probeResultUnknown    string = "unknown"
)

// String 返回探针类型的可读名称，便于日志与调试输出。
func (t probeType) String() string {
	switch t {
	case readiness:
		return "Readiness"
	case liveness:
		return "Liveness"
	case startup:
		return "Startup"
	default:
		return "UNKNOWN"
	}
}

// getRestartableInitContainers 返回 Pod 中所有可重启的初始化容器。
// 可重启初始化容器同样可能声明探针，必须与普通容器一起纳入探测管理。
func getRestartableInitContainers(pod *v1.Pod) []v1.Container {
	var restartableInitContainers []v1.Container
	for _, c := range pod.Spec.InitContainers {
		if podutil.IsRestartableInitContainer(&c) {
			restartableInitContainers = append(restartableInitContainers, c)
		}
	}
	return restartableInitContainers
}

// AddPod 为 Pod 中声明了探针的每个容器创建并启动对应的探测 worker。
func (m *manager) AddPod(ctx context.Context, pod *v1.Pod) {
	// 加写锁：AddPod 会创建并注册 worker，与 RemovePod、CleanupPods 等操作互斥。
	m.workerLock.Lock()
	defer m.workerLock.Unlock()

	logger := klog.FromContext(ctx)
	// 将 worker 的上下文与调用方解耦：Pod worker 在 Pod 开始终止时会取消同步上下文，
	// 但探测 worker 必须持续探测直到容器真正停止，以便在优雅终止期间，就绪探针失败
	// 时能够把 Pod 标记为 NotReady。worker 通过自身的停止通道被显式终止
	//（见 RemovePod/CleanupPods）。
	//
	// TODO(#140977): 这也意味着没有任何机制能取消正在进行中的探测。worker.stop()
	// 只向 stopCh 发信号，而该信号仅在两次探测之间被检查，因此一个已经运行中的
	// exec 探测会在一个正在被杀的容器里继续执行，直到它自己的 TimeoutSeconds
	// 超时。修复方案是为每个 worker 提供可取消的上下文并由 stop() 取消它，
	// 而不是使用取消时机过早的 Pod 同步上下文。
	ctx = context.WithoutCancel(ctx)
	// 以 Pod UID 为基础构造探测标识，随后逐个容器补齐容器名与探针类型。
	key := probeKey{podUID: pod.UID}
	for _, c := range append(pod.Spec.Containers, getRestartableInitContainers(pod)...) {
		key.containerName = c.Name

		if c.StartupProbe != nil {
			key.probeType = startup
			// 已存在同标识的 worker 时跳过，避免 Pod 状态重复同步导致重复创建。
			if _, ok := m.workers[key]; ok {
				logger.V(8).Info("Startup probe already exists for container",
					"pod", klog.KObj(pod), "containerName", c.Name)
				continue
			}
			w := newWorker(m, startup, pod, c)
			m.workers[key] = w
			go w.run(ctx)
		}

		if c.ReadinessProbe != nil {
			key.probeType = readiness
			if _, ok := m.workers[key]; ok {
				logger.V(8).Info("Readiness probe already exists for container",
					"pod", klog.KObj(pod), "containerName", c.Name)
				continue
			}
			w := newWorker(m, readiness, pod, c)
			m.workers[key] = w
			go w.run(ctx)
		}

		if c.LivenessProbe != nil {
			key.probeType = liveness
			if _, ok := m.workers[key]; ok {
				logger.V(8).Info("Liveness probe already exists for container",
					"pod", klog.KObj(pod), "containerName", c.Name)
				continue
			}
			w := newWorker(m, liveness, pod, c)
			m.workers[key] = w
			go w.run(ctx)
		}
	}
}

// StopLivenessAndStartup 停止指定 Pod 所有容器上的存活性与启动探针。
// 容器进入终止流程后无需再探测，避免在无意义的探测中消耗资源。
func (m *manager) StopLivenessAndStartup(pod *v1.Pod) {
	// 加读锁即可：stop 只发送停止信号，不修改 workers 映射。
	m.workerLock.RLock()
	defer m.workerLock.RUnlock()

	key := probeKey{podUID: pod.UID}
	for _, c := range pod.Spec.Containers {
		key.containerName = c.Name
		for _, probeType := range [...]probeType{liveness, startup} {
			key.probeType = probeType
			if worker, ok := m.workers[key]; ok {
				worker.stop()
			}
		}
	}
}

// RemovePod 停止并清理指定 Pod 全部容器上的所有类型探针 worker。
func (m *manager) RemovePod(pod *v1.Pod) {
	// 加读锁即可：stop 只发送停止信号，不修改 workers 映射。
	m.workerLock.RLock()
	defer m.workerLock.RUnlock()

	key := probeKey{podUID: pod.UID}
	for _, c := range append(pod.Spec.Containers, getRestartableInitContainers(pod)...) {
		key.containerName = c.Name
		for _, probeType := range [...]probeType{readiness, liveness, startup} {
			key.probeType = probeType
			if worker, ok := m.workers[key]; ok {
				worker.stop()
			}
		}
	}
}

// CleanupPods 停止所有不属于期望 Pod 集合的探测 worker，清理已消失 Pod 的残留探测。
func (m *manager) CleanupPods(desiredPods map[types.UID]sets.Empty) {
	// 加读锁即可：stop 只发送停止信号，不修改 workers 映射。
	m.workerLock.RLock()
	defer m.workerLock.RUnlock()

	for key, worker := range m.workers {
		if _, ok := desiredPods[key.podUID]; !ok {
			worker.stop()
		}
	}
}

// isContainerStarted 判断容器是否已“启动”：启动探针通过、或在无启动探针时已处于运行态。
func (m *manager) isContainerStarted(logger klog.Logger, pod *v1.Pod, containerStatus *v1.ContainerStatus) bool {
	// 容器不处于运行态时必然尚未启动，无需再查探针结果。
	if containerStatus.State.Running == nil {
		return false
	}

	if result, ok := m.startupManager.Get(kubecontainer.ParseContainerID(logger, containerStatus.ContainerID)); ok {
		return result == results.Success
	}

	if !utilfeature.DefaultFeatureGate.Enabled(features.ChangeContainerStatusOnKubeletRestart) && containerStatus.Started != nil && *containerStatus.Started {
		return true
	}

	// 若仍存在该容器尚未执行的启动探针 worker，说明启动探针尚未跑完，容器不算启动。
	if _, exists := m.getWorker(pod.UID, containerStatus.Name, startup); exists {
		return false
	}

	// 没有启动探针，容器一旦进入运行态即可视为启动完成。
	return true
}

// setReadyStateOnKubeletRestart 在 kubelet 重启场景下修正容器的 Ready 状态：
// 若容器在 kubelet 重启前就已启动、声明了就绪探针，但 Pod 当前尚未就绪，
// 则将其 Ready 置为 false，避免对重启前本就就绪的容器出现 Ready 状态反复抖动。
func (m *manager) setReadyStateOnKubeletRestart(logger klog.Logger, ready *bool, pod *v1.Pod, containerStatus *v1.ContainerStatus, containerSpec *v1.Container) {
	var containerStartTime time.Time
	if containerStatus.State.Running != nil {
		containerStartTime = containerStatus.State.Running.StartedAt.Time
	}

	// 仅当容器启动时间早于 kubelet 重启的宽限期时才需要修正，说明它属于重启前就已存在的容器。
	if !containerStartTime.IsZero() && containerStartTime.Before(kubeletRestartGracePeriod(m.start)) {
		// 此时 Pod 可能处于以下两种状态之一：
		// - 尚未注册进 readinessManager，此时直接将容器置为 Ready。
		// - 已注册进 readinessManager，但就绪探针尚未开始执行。
		// 因此这种情况下也需要将容器状态直接置为 Ready。
		if !*ready {
			if _, ok := m.readinessManager.Get(kubecontainer.ParseContainerID(logger, containerStatus.ContainerID)); !ok {
				*ready = true
			}
		}
		if containerSpec.ReadinessProbe != nil {
			podIsReady := false
			for _, c := range pod.Status.Conditions {
				if c.Type == v1.PodReady && c.Status == v1.ConditionTrue {
					podIsReady = true
					break
				}
			}
			if !podIsReady {
				*ready = false
			}
		}
	}
}

// UpdatePodStatus 依据缓存的探测结果刷新 PodStatus 中各容器与初始化容器的 Started/Ready 状态。
func (m *manager) UpdatePodStatus(ctx context.Context, pod *v1.Pod, podStatus *v1.PodStatus) {
	logger := klog.FromContext(ctx)
	for i, c := range podStatus.ContainerStatuses {
		started := m.isContainerStarted(logger, pod, &podStatus.ContainerStatuses[i])
		podStatus.ContainerStatuses[i].Started = &started

		if !started {
			continue
		}

		var ready bool
		if c.State.Running == nil {
			ready = false
		} else if result, ok := m.readinessManager.Get(kubecontainer.ParseContainerID(logger, c.ContainerID)); ok && result == results.Success {
			ready = true
		} else {
			// 检查是否存在尚未执行过结果的就绪探针。
			w, exists := m.getWorker(pod.UID, c.Name, readiness)
			// 无就绪探针的容器始终视为就绪。
			ready = !exists // 未设置 readinessProbe -> 始终就绪
			if exists {
				// 主动触发一次就绪探针的立即执行，以便尽快刷新 Ready 状态。
				select {
				case w.manualTriggerCh <- struct{}{}:
				default: // 非阻塞触发，避免状态同步被探测调度拖慢。
					logger.Info("Failed to trigger a manual run", "probe", w.probeType.String())
				}
			}

			// 仅在未启用相关特性门控时沿用 kubelet 重启的就绪状态修正逻辑。
			if !utilfeature.DefaultFeatureGate.Enabled(features.ChangeContainerStatusOnKubeletRestart) {
				// 根据容器名在 Pod 规格中查找对应的容器定义，供就绪状态修正使用。
				var containerSpec *v1.Container
				for j := range pod.Spec.Containers {
					if pod.Spec.Containers[j].Name == c.Name {
						containerSpec = &pod.Spec.Containers[j]
						break
					}
				}
				if containerSpec != nil {
					m.setReadyStateOnKubeletRestart(logger, &ready, pod, &podStatus.ContainerStatuses[i], containerSpec)
				}
			}
		}
		podStatus.ContainerStatuses[i].Ready = ready
	}

	// 初始化容器的状态刷新逻辑与普通容器一致，但需额外区分可重启初始化容器。
	for i, c := range podStatus.InitContainerStatuses {
		started := m.isContainerStarted(logger, pod, &podStatus.InitContainerStatuses[i])
		podStatus.InitContainerStatuses[i].Started = &started

		initContainer, ok := kubeutil.GetContainerByIndex(pod.Spec.InitContainers, podStatus.InitContainerStatuses, i)
		if !ok {
			logger.V(4).Info("Mismatch between pod spec and status, likely programmer error", "pod", klog.KObj(pod), "containerName", c.Name)
			continue
		}
		// 非可重启初始化容器一旦正常退出（退出码为 0）即视为就绪，无需探针参与。
		if !podutil.IsRestartableInitContainer(&initContainer) {
			if c.State.Terminated != nil && c.State.Terminated.ExitCode == 0 {
				podStatus.InitContainerStatuses[i].Ready = true
			}
			continue
		}

		if !started {
			continue
		}

		var ready bool
		if c.State.Running == nil {
			ready = false
		} else if result, ok := m.readinessManager.Get(kubecontainer.ParseContainerID(logger, c.ContainerID)); ok && result == results.Success {
			ready = true
		} else {
			// 检查是否存在尚未执行过结果的就绪探针。
			w, exists := m.getWorker(pod.UID, c.Name, readiness)
			// 无就绪探针的容器始终视为就绪。
			ready = !exists // 未设置 readinessProbe -> 始终就绪
			if exists {
				// 主动触发一次就绪探针的立即执行，以便尽快刷新 Ready 状态。
				select {
				case w.manualTriggerCh <- struct{}{}:
				default: // 非阻塞触发，避免状态同步被探测调度拖慢。
					logger.Info("Failed to trigger a manual run", "probe", w.probeType.String())
				}
			}
			if !utilfeature.DefaultFeatureGate.Enabled(features.ChangeContainerStatusOnKubeletRestart) {
				m.setReadyStateOnKubeletRestart(logger, &ready, pod, &podStatus.InitContainerStatuses[i], &initContainer)
			}
		}
		podStatus.InitContainerStatuses[i].Ready = ready
	}
}

// getWorker 在只读加锁下按标识查找探测 worker，返回 worker 及是否存在的标记。
func (m *manager) getWorker(podUID types.UID, containerName string, probeType probeType) (*worker, bool) {
	// 仅读取 workers 映射，使用读锁以支持多读者并发。
	m.workerLock.RLock()
	defer m.workerLock.RUnlock()
	worker, ok := m.workers[probeKey{podUID, containerName, probeType}]
	return worker, ok
}

// removeWorker 在 worker 退出后由 worker 自身调用，将其从活跃映射中摘除。
func (m *manager) removeWorker(podUID types.UID, containerName string, probeType probeType) {
	m.workerLock.Lock()
	defer m.workerLock.Unlock()
	delete(m.workers, probeKey{podUID, containerName, probeType})
}

// workerCount 返回当前活跃探测 worker 的总数，供测试断言使用。
func (m *manager) workerCount() int {
	m.workerLock.RLock()
	defer m.workerLock.RUnlock()
	return len(m.workers)
}

// kubeletRestartGracePeriod 返回 kubelet 启动时间之前 10 秒的时间点。
// 该宽限期用于判断容器是否在 kubelet 重启之前就已处于运行状态。
// 若容器的启动时间早于该宽限期，说明它在 kubelet 重启前就在运行，
// 不应立即被标记为失败，以避免对此前已就绪的容器产生不必要的状态变更。
func kubeletRestartGracePeriod(start time.Time) time.Time {
	return start.Add(-time.Second * 10)
}
