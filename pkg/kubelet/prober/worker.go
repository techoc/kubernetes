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
	"math/rand"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/metrics"
	"k8s.io/klog/v2"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/features"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/prober/results"
)

// worker 负责对分配给它的容器进行周期性探测。每个 worker 关联一个 goroutine，
// 运行探测循环，直到容器永久终止或停止通道被关闭。
// worker 通过探针 Manager 的 statusManager 获取最新的容器 ID。
type worker struct {
	// 用于停止探测的通道。
	stopCh chan struct{}

	// 用于手动触发一次探测的通道。
	manualTriggerCh chan struct{}

	// 该探针所属的 Pod（只读）。
	pod *v1.Pod

	// 被探测的容器（只读）。
	container v1.Container

	// 探针配置（只读）。
	spec *v1.Probe

	// worker 对应的探针类型。
	probeType probeType

	// 初始延迟期间的探针结果。
	initialValue results.Result

	// 存放该 worker 探测结果的位置。
	resultsManager results.Manager
	probeManager   *manager

	// 该 worker 最近一次已知的容器 ID。
	containerID kubecontainer.ContainerID
	// 该 worker 最近一次探测结果。
	lastResult results.Result
	// 连续返回相同结果的次数。
	resultRun int

	// 为 true 时暂停探测。
	onHold bool

	// 以下标签用于按结果维度记录 ProberResults 指标。
	proberResultsSuccessfulMetricLabels metrics.Labels
	proberResultsFailedMetricLabels     metrics.Labels
	proberResultsUnknownMetricLabels    metrics.Labels
	// 以下标签用于按结果维度记录 ProberDuration 指标。
	proberDurationSuccessfulMetricLabels metrics.Labels
	proberDurationUnknownMetricLabels    metrics.Labels
}

// isInitContainer 判断 worker 对应的容器是否属于 Pod 的初始化容器。
func (w *worker) isInitContainer() bool {
	for _, initContainer := range w.pod.Spec.InitContainers {
		if initContainer.Name == w.container.Name {
			return true
		}
	}
	return false
}

// newWorker 创建并启动一个新的探针 worker。
func newWorker(
	m *manager,
	probeType probeType,
	pod *v1.Pod,
	container v1.Container) *worker {

	w := &worker{
		stopCh:          make(chan struct{}, 1), // 带缓冲，使 stop() 可以非阻塞调用。
		manualTriggerCh: make(chan struct{}, 1), // 带缓冲，使 prober_manager 可以非阻塞地触发 doProbe。
		pod:             pod,
		container:       container,
		probeType:       probeType,
		probeManager:    m,
	}

	// 按探针类型选择配置、结果管理器与初始值：
	// 就绪探针初始视为失败，存活探针初始视为成功，启动探针初始未知。
	switch probeType {
	case readiness:
		w.spec = container.ReadinessProbe
		w.resultsManager = m.readinessManager
		w.initialValue = results.Failure
	case liveness:
		w.spec = container.LivenessProbe
		w.resultsManager = m.livenessManager
		w.initialValue = results.Success
	case startup:
		w.spec = container.StartupProbe
		w.resultsManager = m.startupManager
		w.initialValue = results.Unknown
	}

	// 预先构造指标标签，避免每次探测时重复分配。
	basicMetricLabels := metrics.Labels{
		"probe_type": w.probeType.String(),
		"container":  w.container.Name,
		"pod":        w.pod.Name,
		"namespace":  w.pod.Namespace,
		"pod_uid":    string(w.pod.UID),
	}

	proberDurationLabels := metrics.Labels{
		"probe_type": w.probeType.String(),
		"container":  w.container.Name,
		"pod":        w.pod.Name,
		"namespace":  w.pod.Namespace,
	}

	w.proberResultsSuccessfulMetricLabels = deepCopyPrometheusLabels(basicMetricLabels)
	w.proberResultsSuccessfulMetricLabels["result"] = probeResultSuccessful

	w.proberResultsFailedMetricLabels = deepCopyPrometheusLabels(basicMetricLabels)
	w.proberResultsFailedMetricLabels["result"] = probeResultFailed

	w.proberResultsUnknownMetricLabels = deepCopyPrometheusLabels(basicMetricLabels)
	w.proberResultsUnknownMetricLabels["result"] = probeResultUnknown

	w.proberDurationSuccessfulMetricLabels = deepCopyPrometheusLabels(proberDurationLabels)
	w.proberDurationUnknownMetricLabels = deepCopyPrometheusLabels(proberDurationLabels)

	return w
}

// run 周期性地探测容器，直到容器终止或收到停止信号。
func (w *worker) run(ctx context.Context) {
	logger := klog.FromContext(ctx)
	probeTickerPeriod := time.Duration(w.spec.PeriodSeconds) * time.Second

	// kubelet 重启后各探针可能几乎同时启动。
	// 让 worker 在探测前随机等待一段时间，分散探测压力；仅在 kubelet 刚启动时执行。
	if probeTickerPeriod > time.Since(w.probeManager.start) {
		time.Sleep(time.Duration(rand.Float64() * float64(probeTickerPeriod)))
	}

	probeTicker := time.NewTicker(probeTickerPeriod)

	// worker 退出时统一清理：停止定时器、删除探测结果与指标。
	defer func() {
		// 清理资源。
		probeTicker.Stop()
		if !w.containerID.IsEmpty() {
			w.resultsManager.Remove(w.containerID)
		}

		w.probeManager.removeWorker(w.pod.UID, w.container.Name, w.probeType)
		ProberResults.Delete(w.proberResultsSuccessfulMetricLabels)
		ProberResults.Delete(w.proberResultsFailedMetricLabels)
		ProberResults.Delete(w.proberResultsUnknownMetricLabels)
		ProberDuration.Delete(w.proberDurationSuccessfulMetricLabels)
		ProberDuration.Delete(w.proberDurationUnknownMetricLabels)
	}()

probeLoop:
	for w.doProbe(ctx) {
		// 等待下一次探测周期。
		select {
		case <-w.stopCh:
			break probeLoop
		case <-probeTicker.C:
			// 到点继续下一次探测。
		case <-w.manualTriggerCh:
			// 手动触发后重置周期定时器，使后续探测从手动执行时刻重新计时。
			probeTicker.Reset(probeTickerPeriod)
			logger.V(4).Info("Triggered Probe by manual run", "probeType", w.probeType, "pod", klog.KObj(w.pod), "podUID", w.pod.UID, "containerName", w.container.Name)
			// 继续下一次探测。
		}
	}
}

// stop 停止探针 worker。worker 会自行完成清理并从 Manager 中移除。
// 多次调用 stop 是安全的。
func (w *worker) stop() {
	select {
	case w.stopCh <- struct{}{}:
	default: // Non-blocking.
	}
}

// doProbe 对容器执行一次探测并记录结果，返回 worker 是否应继续运行。
func (w *worker) doProbe(ctx context.Context) (keepGoing bool) {
	defer func() { recover() }() // 吞掉 panic，日志由 HandleCrash 负责记录。
	defer runtime.HandleCrashWithContext(ctx, func(ctx context.Context, _ interface{}) { keepGoing = true })

	logger := klog.FromContext(ctx)
	startTime := time.Now()
	status, ok := w.probeManager.statusManager.GetPodStatus(w.pod.UID)
	if !ok {
		// Pod 尚未创建或已被删除。
		logger.V(3).Info("No status for pod", "pod", klog.KObj(w.pod))
		return true
	}

	// Pod 已终止时 worker 应退出。
	if status.Phase == v1.PodFailed || status.Phase == v1.PodSucceeded {
		logger.V(3).Info("Pod is terminated, exiting probe worker",
			"pod", klog.KObj(w.pod), "phase", status.Phase)
		return false
	}

	c, ok := podutil.GetContainerStatus(status.ContainerStatuses, w.container.Name)
	if !ok || len(c.ContainerID) == 0 {
		c, ok = podutil.GetContainerStatus(status.InitContainerStatuses, w.container.Name)
		if !ok || len(c.ContainerID) == 0 {
			// 容器尚未创建或已被删除。
			logger.V(3).Info("Probe target container not found",
				"pod", klog.KObj(w.pod), "containerName", w.container.Name)
			return true // Wait for more information.
		}
	}

	isRestartableInitContainer := w.isInitContainer() &&
		w.container.RestartPolicy != nil &&
		*w.container.RestartPolicy == v1.ContainerRestartPolicyAlways

	// 容器 ID 变化说明容器被重建，需清理旧结果并以新容器重新开始探测。
	if w.containerID.String() != c.ContainerID {
		if !w.containerID.IsEmpty() {
			w.resultsManager.Remove(w.containerID)
		}

		w.containerID = kubecontainer.ParseContainerID(logger, c.ContainerID)

		if !utilfeature.DefaultFeatureGate.Enabled(features.ChangeContainerStatusOnKubeletRestart) {
			// kubelet 重启后不要立即把探测结果置为 Failure，
			// 否则会把原本 Ready 的容器误置为 NotReady。
			isRestart := false
			if c.State.Running != nil {
				containerStartTime := c.State.Running.StartedAt.Time
				if !containerStartTime.IsZero() && containerStartTime.Before(kubeletRestartGracePeriod(w.probeManager.start)) {
					isRestart = true
				}
			}

			// 对于带启动探针的可重启初始化容器（sidecar），若容器在 kubelet 重启前
			// 已经启动，则以 Success 预置启动结果，确保 startupManager.Get() 返回有效值，
			// 使 computeInitContainerActions 能正确识别 Pod 初始化状态。
			// 参见 https://github.com/kubernetes/kubernetes/issues/136910
			if isRestartableInitContainer && w.probeType == startup {
				if c.Started != nil && *c.Started {
					w.resultsManager.Set(w.containerID, results.Success, w.pod)
				}
			}

			if !isRestart {
				w.resultsManager.Set(w.containerID, w.initialValue, w.pod)
			}
		} else {
			w.resultsManager.Set(w.containerID, w.initialValue, w.pod)
		}

		// 发现新容器，恢复探测。
		w.onHold = false
	}

	if w.onHold {
		// worker 暂停中，等待新容器出现后再继续。
		return true
	}

	if c.State.Running == nil {
		logger.V(3).Info("Non-running container probed",
			"pod", klog.KObj(w.pod), "containerName", w.container.Name)
		if !w.containerID.IsEmpty() {
			w.resultsManager.Set(w.containerID, results.Failure, w.pod)
		}

		// 若容器不会重启则终止探测。
		if utilfeature.DefaultFeatureGate.Enabled(features.ContainerRestartRules) {
			if c.State.Terminated == nil {
				return true
			}
			containerShouldRestart := podutil.ContainerShouldRestart(w.container, w.pod.Spec, c.State.Terminated.ExitCode)
			allContainersRestarting := utilfeature.DefaultFeatureGate.Enabled(features.RestartAllContainersOnContainerExits) && kubecontainer.ShouldAllContainersRestart(w.pod, nil, &status)
			return containerShouldRestart || allContainersRestarting
		}
		return c.State.Terminated == nil ||
			w.pod.Spec.RestartPolicy != v1.RestartPolicyNever ||
			isRestartableInitContainer
	}

	// Pod 优雅关闭场景：存活/启动探针直接置为成功并停止探测。
	if w.pod.ObjectMeta.DeletionTimestamp != nil && (w.probeType == liveness || w.probeType == startup) {
		logger.V(3).Info("Pod deletion requested, setting probe result to success",
			"probeType", w.probeType, "pod", klog.KObj(w.pod), "containerName", w.container.Name)
		if w.probeType == startup {
			logger.Info("Pod deletion requested before container has fully started",
				"pod", klog.KObj(w.pod), "containerName", w.container.Name)
		}
		// 写入最终结果，保证关闭过程安静无告警。
		w.resultsManager.Set(w.containerID, results.Success, w.pod)
		// 此时停止探测。
		return false
	}

	// 初始延迟（InitialDelaySeconds）期间不执行探测。
	if int32(time.Since(c.State.Running.StartedAt.Time).Seconds()) < w.spec.InitialDelaySeconds {
		return true
	}

	if c.Started != nil && *c.Started {
		// 容器已启动后，启动探针不再产生作用，但 worker 保持运行，
		// 以便容器重启后仍能继续工作。
		if w.probeType == startup {
			return true
		}
	} else {
		// 容器尚未启动完成时，暂停除启动探针外的其他探针。
		if w.probeType != startup {
			return true
		}
	}

	// 注意：exec 探针无法访问 Pod 环境变量或 downward API。
	result, err := w.probeManager.prober.probe(ctx, w.probeType, w.pod, status, w.container, w.containerID)
	if err != nil {
		// 探测器自身出错，丢弃本次结果。
		return true
	}

	switch result {
	case results.Success:
		ProberResults.With(w.proberResultsSuccessfulMetricLabels).Inc()
		ProberDuration.With(w.proberDurationSuccessfulMetricLabels).Observe(time.Since(startTime).Seconds())
	case results.Failure:
		ProberResults.With(w.proberResultsFailedMetricLabels).Inc()
	default:
		ProberResults.With(w.proberResultsUnknownMetricLabels).Inc()
		ProberDuration.With(w.proberDurationUnknownMetricLabels).Observe(time.Since(startTime).Seconds())
	}

	if w.lastResult == result {
		w.resultRun++
	} else {
		w.lastResult = result
		w.resultRun = 1
	}

	// 连续失败或成功次数未达到阈值时，保持原有探测状态不变。
	if (result == results.Failure && w.resultRun < int(w.spec.FailureThreshold)) ||
		(result == results.Success && w.resultRun < int(w.spec.SuccessThreshold)) {
		// 未达到阈值，保持探测状态不变。
		return true
	}

	w.resultsManager.Set(w.containerID, result, w.pod)

	if (w.probeType == liveness && result == results.Failure) || w.probeType == startup {
		// 容器存活/启动检查失败，将被重启。此时停止探测，直到出现新的容器 ID。
		// 这样可以降低触发 #21751 的概率：容器停止过程中执行 `docker exec`
		// 可能导致容器状态损坏。
		// 此外，启动探针达到阈值后也应停止探测直到容器重启，避免多余的探测执行（#117153）。
		w.onHold = true
		w.resultRun = 0
	}

	return true
}

// deepCopyPrometheusLabels 复制一份指标标签 map，
// 避免后续修改影响共享的标签对象。
func deepCopyPrometheusLabels(m metrics.Labels) metrics.Labels {
	ret := make(metrics.Labels, len(m))
	for k, v := range m {
		ret[k] = v
	}
	return ret
}
