/*
Copyright The Kubernetes Authors.

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

package preemption

import (
	"context"
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	apipod "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/scheduler/framework/parallelize"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

// pendingVictim 标识正在被抢占的最后一个受害者 Pod，供 PreEnqueue 检查其删除进度。
type pendingVictim struct {
	namespace string
	name      string
}

// ExecutorPreemptor 表示触发抢占的一方（Pod 或 PodGroup/CompositePodGroup）。
// 它抽象出 Executor 在抢占过程中记录日志所需的元数据。
type ExecutorPreemptor interface {
	klog.KMetadata
	// UID 返回触发抢占对象的 UID。
	UID() types.UID
	// SchedulerName 返回触发抢占对象指定的调度器名字。
	SchedulerName() string
	// Obj 返回触发抢占的对象。
	Obj() runtime.Object
	// Pods 返回构成抢占方的 Pod，以 Pod 名字为键。
	Pods() map[string]*v1.Pod
	// Priority 返回抢占方的优先级。
	Priority() int32
	// Type 返回抢占方的类型。
	Type() string
}

// Executor 根据给定的候选结果执行抢占，支持同步与异步两种模式。
type Executor struct {
	mu sync.RWMutex
	fh fwk.Handle

	podLister corelisters.PodLister

	fts feature.Features

	// preempting 记录正在异步触发抢占的 Pod/PodGroup UID，防止它们在抢占期间重新进入调度周期。
	preempting sets.Set[types.UID]
	// lastVictimsPendingPreemption 记录抢占方正在等待其被驱逐的最后一个受害者 Pod。
	// 与 preempting 配合，避免等待抢占完成时 Pod/PodGroup 重新进入调度周期。
	lastVictimsPendingPreemption map[types.UID]pendingVictim

	// PreemptPod 真正执行对单个 Pod 的抢占。若仅在调度器内存中完成（未调用删除 API）则返回 true。
	// 暴露成字段是为了在测试中替换。
	PreemptPod func(ctx context.Context, c Candidate, preemptor ExecutorPreemptor, victim *v1.Pod, pluginName string) (bool, error)
}

// NewExecutor 创建一个新的抢占执行器。
func NewExecutor(fh fwk.Handle, fts feature.Features) *Executor {
	e := &Executor{
		fh:                           fh,
		podLister:                    fh.SharedInformerFactory().Core().V1().Pods().Lister(),
		preempting:                   sets.New[types.UID](),
		lastVictimsPendingPreemption: make(map[types.UID]pendingVictim),
		fts:                          fts,
	}

	e.PreemptPod = func(ctx context.Context, c Candidate, preemptor ExecutorPreemptor, victim *v1.Pod, pluginName string) (bool, error) {
		logger := klog.FromContext(ctx)

		preemptedInMemory := false
		eventMessage := fmt.Sprintf("Preempted by %s %v on node %v", preemptor.Type(), preemptor.UID(), c.Name())
		// 若受害者还在等待队列，尽量只在内存中抢占，让它退回 backoff 队列；否则必须调用删除 API。
		if waitingPod := e.fh.GetWaitingPod(victim.UID); waitingPod != nil {
			if waitingPod.Preempt(pluginName, "preempted") {
				logger.V(2).Info("Preemptor preempted a waiting pod", "preemptorType", preemptor.Type(), "preemptor", klog.KObj(preemptor), "waitingPod", klog.KObj(victim), "node", c.Name())
				preemptedInMemory = true
			}
		} else if podInPreBind := e.fh.GetPodInPreBind(victim.UID); podInPreBind != nil {
			// 受害者正处于 preBind 阶段时，取消其绑定流程即可。
			if podInPreBind.CancelPod(fmt.Sprintf("preempted by %s", pluginName)) {
				logger.V(2).Info("Preemptor rejected a pod in preBind", "preemptorType", preemptor.Type(), "preemptor", klog.KObj(preemptor), "podInPreBind", klog.KObj(victim), "node", c.Name())
				preemptedInMemory = true
			} else {
				logger.V(5).Info("Failed to reject a pod in preBind, falling back to deletion via api call", "preemptor", klog.KObj(preemptor), "podInPreBind", klog.KObj(victim), "node", c.Name())
			}
		}
		if !preemptedInMemory {
			condition := &v1.PodCondition{
				Type:               v1.DisruptionTarget,
				ObservedGeneration: apipod.CalculatePodConditionObservedGeneration(&victim.Status, victim.Generation, v1.DisruptionTarget),
				Status:             v1.ConditionTrue,
				Reason:             v1.PodReasonPreemptionByScheduler,
				Message:            fmt.Sprintf("%s: preempting to accommodate a higher priority %s", preemptor.SchedulerName(), preemptor.Type()),
			}
			newStatus := victim.Status.DeepCopy()
			updated := apipod.UpdatePodCondition(newStatus, condition)
			if updated {
				if err := util.PatchPodStatus(ctx, fh.ClientSet(), victim.Name, victim.Namespace, &victim.Status, newStatus); err != nil {
					if !apierrors.IsNotFound(err) {
						logger.Error(err, "Could not add DisruptionTarget condition due to preemption", "preemptor", klog.KObj(preemptor), "victim", klog.KObj(victim))
						return false, err
					}
					logger.V(2).Info("Victim Pod is already deleted", "preemptor", klog.KObj(preemptor), "victim", klog.KObj(victim), "node", c.Name())
					return false, nil
				}
			}
			if err := util.DeletePod(ctx, fh.ClientSet(), victim); err != nil {
				if !apierrors.IsNotFound(err) {
					logger.Error(err, "Tried to preempted pod", "pod", klog.KObj(victim), "preemptor", klog.KObj(preemptor))
					return false, err
				}
				logger.V(2).Info("Victim Pod is already deleted", "preemptor", klog.KObj(preemptor), "victim", klog.KObj(victim), "node", c.Name())
				return false, nil
			}
			logger.V(2).Info("Preemptor Pod preempted victim Pod", "preemptor", klog.KObj(preemptor), "victim", klog.KObj(victim), "node", c.Name())
		} else {
			eventMessage += " (in kube-scheduler memory)."
		}

		fh.EventRecorder().WithLogger(logger).Eventf(victim, preemptor.Obj(), v1.EventTypeNormal, "Preempted", "Preempting", eventMessage)

		return preemptedInMemory, nil
	}

	return e
}

// actuatePodPreemption 执行以 preemptorPod 为抢占方、驱逐 victims 的抢占流程。
func (e *Executor) actuatePodPreemption(ctx context.Context, candidate Candidate, preemptorPod *v1.Pod, pluginName string) *fwk.Status {
	podPreemptor := &podExecutorPreemptor{Pod: preemptorPod}
	if e.fts.EnableAsyncPreemption {
		e.prepareCandidateAsync(candidate, podPreemptor, pluginName)
		return nil
	}
	return e.prepareCandidate(ctx, candidate, podPreemptor, pluginName)
}

// actuatePodGroupPreemption 执行以 PodGroup 为抢占方、驱逐 victims 的抢占流程。
func (e *Executor) actuatePodGroupPreemption(ctx context.Context, candidate Candidate, pgInfo fwk.PodGroupInfo, pluginName string) *fwk.Status {
	var podGroupPreemptor ExecutorPreemptor
	if pgInfo.GetCompositePodGroup() != nil {
		podGroupPreemptor = &compositePodGroupExecutorPreemptor{cpg: pgInfo.GetCompositePodGroup(), pods: pgInfo.GetUnscheduledPods()}
	} else {
		podGroupPreemptor = &podGroupExecutorPreemptor{pg: pgInfo.GetPodGroup(), pods: pgInfo.GetUnscheduledPods()}
	}
	if e.fts.EnableAsyncPreemption {
		e.prepareCandidateAsync(candidate, podGroupPreemptor, pluginName)
		return nil
	}
	return e.prepareCandidate(ctx, candidate, podGroupPreemptor, pluginName)
}

// prepareCandidateAsync 启动一个 goroutine 完成以下准备工作：
// - 驱逐受害者 Pod
// - 若受害者在 waitingPod 表中则拒绝其绑定
// - 必要时清除低优先级 Pod 的 nominatedNodeName
// 在该 goroutine 完成之前，抢占方 Pod 不会被重新调度。
//
// 异步抢占的设计见 http://kep.k8s.io/4832。
func (e *Executor) prepareCandidateAsync(c Candidate, preemptor ExecutorPreemptor, pluginName string) {
	observeVictims(preemptor, c)
	// 有意使用新建的 Background context，而不是调度周期的 ctx：本流程可能延续到调度周期结束之后。
	ctx, cancel := context.WithCancel(context.Background())
	logger := klog.FromContext(ctx)

	victimPods := make([]*v1.Pod, 0, len(c.Victims().Pods))
	for _, victim := range c.Victims().Pods {
		if victim.DeletionTimestamp != nil {
			// 优雅删除已开始，无需再发一次删除请求。
			logger.V(2).Info("Victim Pod is already being deleted, skipping the API call for it", "preemptor", klog.KObj(preemptor), "node", c.Name(), "victim", klog.KObj(victim))
			continue
		}
		victimPods = append(victimPods, victim)
	}
	if len(victimPods) == 0 {
		cancel()
		return
	}

	errCh := parallelize.NewResultChannel[error]()
	// PreEnqueue 只关注最后一个受害者的完成状态；若它在内存中被抢占（不会有删除事件），需主动激活抢占方。
	preemptedLastVictimInMemory := false
	preemptPod := func(index int) {
		victim := victimPods[index]
		if _, err := e.PreemptPod(ctx, c, preemptor, victim, pluginName); err != nil {
			errCh.SendWithCancel(err, cancel)
		}
	}

	e.mu.Lock()
	e.preempting.Insert(preemptor.UID())
	e.mu.Unlock()

	go func() {
		logger := klog.FromContext(ctx)
		startTime := time.Now()
		result := metrics.GoroutineResultSuccess
		defer func() {
			metrics.PreemptionExecutionDuration.WithLabelValues(preemptor.Type(), result).Observe(metrics.SinceInSeconds(startTime))
		}()
		defer func() {
			metrics.PreemptionGoroutinesDuration.WithLabelValues(result).Observe(metrics.SinceInSeconds(startTime))
		}()
		defer func() {
			metrics.PreemptionGoroutinesExecutionTotal.WithLabelValues(result).Inc()
		}()
		defer func() {
			if result == metrics.GoroutineResultError || preemptedLastVictimInMemory {
				// API 调用失败或没有删除事件产生时，抢占方的 Pod 最坏可能卡在不可调度池中。
				e.fh.Activate(logger, preemptor.Pods())
			}
		}()
		defer cancel()
		logger.V(2).Info("Start the preemption asynchronously", "preemptor", klog.KObj(preemptor), "node", c.Name(), "numVictims", len(c.Victims().Pods), "numVictimsToDelete", len(victimPods))

		// 被提名到本节点的低优先级 Pod 在抢占后可能不再适合本节点，需清除其提名。
		// 清除提名会更新这些 Pod 并移入活跃队列，让调度器更早为它们寻找新位置，
		// 而不必等到抢占全部完成。
		nominatedPods := getLowerPriorityNominatedPods(e.fh, preemptor.Priority(), c.Name())
		if err := clearNominatedNodeName(ctx, e.fh.ClientSet(), e.fh.APICacher(), nominatedPods...); err != nil {
			utilruntime.HandleErrorWithContext(ctx, err, "Cannot clear 'NominatedNodeName' field from lower priority pods on the same target node", "node", c.Name())
			result = metrics.GoroutineResultError
			// 该错误不影响主流程，继续执行。
		}

		preemptLastVictim := true
		if len(victimPods) > 1 {
			// 抢占方已加入 preempting 集合，避免对同一 Pod 重复发起抢占。
			// 除最后一个受害者外，其余可并行驱逐；最后一个要特殊处理，便于 PreEnqueue 判断驱逐是否仍在进行。
			// 若删除已完成但函数尚未返回，PreEnqueue 会通过 IsPodRunningPreemption 读取
			// lastVictimsPendingPreemption 中最后一个受害者的状态，从而不阻塞抢占方。
			// 这样可避免 Pod 删除事件先于抢占方从 preempting 集合中移除而被队列忽略，
			// 导致抢占方卡在不可调度池。
			e.fh.Parallelizer().Until(ctx, len(victimPods)-1, preemptPod, pluginName)
			if err := errCh.Receive(); err != nil {
				utilruntime.HandleErrorWithContext(ctx, err, "Error occurred during async preemption")
				result = metrics.GoroutineResultError
				preemptLastVictim = false
			}
		}

		// 若前面的受害者抢占失败，可跳过最后一个受害者，让抢占方尽早重回调度周期。
		if preemptLastVictim {
			lastVictim := victimPods[len(victimPods)-1]
			e.mu.Lock()
			e.lastVictimsPendingPreemption[preemptor.UID()] = pendingVictim{namespace: lastVictim.Namespace, name: lastVictim.Name}
			e.mu.Unlock()

			preemptedInMemory, err := e.PreemptPod(ctx, c, preemptor, lastVictim, pluginName)
			if err != nil {
				utilruntime.HandleErrorWithContext(ctx, err, "Error occurred during async preemption of the last victim")
				result = metrics.GoroutineResultError
			} else if preemptedInMemory {
				preemptedLastVictimInMemory = true
			}
		}
		e.mu.Lock()
		e.preempting.Delete(preemptor.UID())
		delete(e.lastVictimsPendingPreemption, preemptor.UID())
		e.mu.Unlock()

		logger.V(2).Info("Async Preemption finished completely", "preemptor", klog.KObj(preemptor), "node", c.Name(), "result", result)
	}()
}

// prepareCandidate 在提名候选节点前做如下准备：
// - 驱逐受害者 Pod
// - 若受害者在 waitingPod 表中则拒绝其绑定
// - 必要时清除低优先级 Pod 的 nominatedNodeName
func (e *Executor) prepareCandidate(ctx context.Context, c Candidate, preemptor ExecutorPreemptor, pluginName string) *fwk.Status {
	observeVictims(preemptor, c)
	startTime := time.Now()
	metricsResult := metrics.GoroutineResultSuccess
	defer func() {
		metrics.PreemptionExecutionDuration.WithLabelValues(preemptor.Type(), metricsResult).Observe(metrics.SinceInSeconds(startTime))
	}()

	fh := e.fh
	cs := e.fh.ClientSet()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	logger := klog.FromContext(ctx)
	errCh := parallelize.NewResultChannel[error]()
	fh.Parallelizer().Until(ctx, len(c.Victims().Pods), func(index int) {
		victim := c.Victims().Pods[index]
		if victim.DeletionTimestamp != nil {
			// 优雅删除已开始，无需再发一次删除请求。
			logger.V(2).Info("Victim Pod is already being deleted, skipping the API call for it", "preemptor", klog.KObj(preemptor), "node", c.Name(), "victim", klog.KObj(victim))
			return
		}
		if _, err := e.PreemptPod(ctx, c, preemptor, victim, pluginName); err != nil {
			errCh.SendWithCancel(err, cancel)
		}
	}, pluginName)
	if err := errCh.Receive(); err != nil {
		metricsResult = metrics.GoroutineResultError
		return fwk.AsStatus(err)
	}

	// 被提名到本节点的低优先级 Pod 在抢占后可能不再适合本节点，需清除其提名。
	// 清除提名会更新这些 Pod 并移入活跃队列，让调度器更早为它们寻找新位置。
	nominatedPods := getLowerPriorityNominatedPods(fh, preemptor.Priority(), c.Name())
	if err := clearNominatedNodeName(ctx, cs, fh.APICacher(), nominatedPods...); err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Cannot clear 'NominatedNodeName' field")
		// 该错误不影响主流程，继续执行。
	}

	return nil
}

// observeVictims 记录本次抢占的受害者数量、工作负载中断数与 PDB 违反数的指标。
func observeVictims(preemptor ExecutorPreemptor, candidate Candidate) {
	numVictims := float64(len(candidate.Victims().Pods))
	if preemptor.Type() == string(fwk.PodGroupKeyType) || preemptor.Type() == string(fwk.CompositePodGroupKeyType) {
		metrics.WorkloadPreemptionVictims.Observe(numVictims)
	} else {
		metrics.PreemptionVictims.Observe(numVictims)
	}

	workloadDisruptions := float64(candidate.NumPodGroupDisruptions())
	if workloadDisruptions > 0 {
		metrics.PreemptionWorkloadDisruptions.WithLabelValues(preemptor.Type()).Observe(workloadDisruptions)
	}

	numPDBViolations := float64(candidate.Victims().NumPDBViolations)
	if numPDBViolations > 0 {
		metrics.PreemptionPDBViolations.WithLabelValues(preemptor.Type()).Add(numPDBViolations)
	}
}

// IsPodRunningPreemption 判断 Pod 当前是否正在异步触发抢占。
func (e *Executor) IsPodRunningPreemption(podUID types.UID) bool {
	return e.isRunningPreemption(podUID)
}

// IsPodGroupRunningPreemption 判断 PodGroup 当前是否正在异步触发抢占。
func (e *Executor) IsPodGroupRunningPreemption(podGroupUID types.UID) bool {
	return e.isRunningPreemption(podGroupUID)
}

// isRunningPreemption 判断给定 UID 的抢占方是否仍在等待抢占完成。
func (e *Executor) isRunningPreemption(uid types.UID) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()

	if !e.preempting.Has(uid) {
		return false
	}

	victim, ok := e.lastVictimsPendingPreemption[uid]
	if !ok {
		// 抢占方已在 preempting 中但最后一个受害者尚未登记，说明异步抢占仍在进行。
		return true
	}
	// 抢占方正在等待最后一个受害者，可通过查询该 Pod 是否已被删除来判断抢占是否完成。
	victimPod, err := e.podLister.Pods(victim.namespace).Get(victim.name)
	if err != nil {
		// 受害者已删除，抢占完成。
		return false
	}
	if victimPod.DeletionTimestamp != nil {
		// 受害者已开始删除，抢占完成。
		return false
	}
	// 最后一个 Pod 的抢占仍在进行。
	return true
}

// clearNominatedNodeName 向 apiserver 提交 patch，把每个 Pod 的 Status.NominatedNodeName 置空。
func clearNominatedNodeName(ctx context.Context, cs clientset.Interface, apiCacher fwk.APICacher, pods ...*v1.Pod) utilerrors.Aggregate {
	var errs []error
	for _, p := range pods {
		if apiCacher != nil {
			// 有 API cacher 时通过它清除提名，避免直接调用 apiserver。
			_, err := apiCacher.PatchPodStatus(p, nil, &fwk.NominatingInfo{NominatedNodeName: "", NominatingMode: fwk.ModeOverride})
			if err != nil {
				errs = append(errs, err)
			}
		} else {
			if len(p.Status.NominatedNodeName) == 0 {
				continue
			}
			podStatusCopy := p.Status.DeepCopy()
			podStatusCopy.NominatedNodeName = ""
			if err := util.PatchPodStatus(ctx, cs, p.Name, p.Namespace, &p.Status, podStatusCopy); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return utilerrors.NewAggregate(errs)
}

// getLowerPriorityNominatedPods 返回被提名到指定节点、且优先级低于给定优先级的 Pod。
// 理论上还可以检查这些 Pod 是否仍适合该节点，只返回不再适合的；但这需要为每个被提名 Pod
// 操作 NodeInfo 与 PreFilter 状态，复杂度不划算——通常每个节点上的被提名 Pod 数量很少。
func getLowerPriorityNominatedPods(pn fwk.PodNominator, priority int32, nodeName string) []*v1.Pod {
	podInfos := pn.NominatedPodsForNode(nodeName)

	if len(podInfos) == 0 {
		return nil
	}

	var lowerPriorityPods []*v1.Pod
	for _, pi := range podInfos {
		if corev1helpers.PodPriority(pi.GetPod()) < priority {
			lowerPriorityPods = append(lowerPriorityPods, pi.GetPod())
		}
	}
	return lowerPriorityPods
}

// podExecutorPreemptor 把单个 Pod 包装成 ExecutorPreemptor。
type podExecutorPreemptor struct {
	*v1.Pod
}

// UID 返回 Pod 的 UID。
func (p *podExecutorPreemptor) UID() types.UID {
	return p.Pod.UID
}

// SchedulerName 返回 Pod 指定的调度器名字。
func (p *podExecutorPreemptor) SchedulerName() string {
	return p.Spec.SchedulerName
}

// GetName 返回 Pod 名字。
func (p *podExecutorPreemptor) GetName() string {
	return p.Name
}

// GetNamespace 返回 Pod 所在命名空间。
func (p *podExecutorPreemptor) GetNamespace() string {
	return p.Namespace
}

// Obj 返回底层 Pod 对象。
func (p *podExecutorPreemptor) Obj() runtime.Object {
	return p
}

// Pods 返回该抢占方包含的单个 Pod。
func (p *podExecutorPreemptor) Pods() map[string]*v1.Pod {
	return map[string]*v1.Pod{p.Name: p.Pod}
}

// Priority 返回 Pod 的优先级。
func (p *podExecutorPreemptor) Priority() int32 {
	return corev1helpers.PodPriority(p.Pod)
}

// Type 返回抢占方类型（Pod）。
func (p *podExecutorPreemptor) Type() string {
	return string(fwk.PodKeyType)
}

// podGroupExecutorPreemptor 把 PodGroup 包装成 ExecutorPreemptor。
type podGroupExecutorPreemptor struct {
	pg   *schedulingv1beta1.PodGroup
	pods []*v1.Pod
}

// UID 返回 PodGroup 的 UID。
func (p *podGroupExecutorPreemptor) UID() types.UID {
	return p.pg.UID
}

// SchedulerName 返回 PodGroup 中 Pod 使用的调度器名字。
func (p *podGroupExecutorPreemptor) SchedulerName() string {
	// 同一 PodGroup 内的 Pod 应使用相同的调度器名字。
	return p.pods[0].Spec.SchedulerName
}

// GetName 返回 PodGroup 名字。
func (p *podGroupExecutorPreemptor) GetName() string {
	return p.pg.Name
}

// GetNamespace 返回 PodGroup 所在命名空间。
func (p *podGroupExecutorPreemptor) GetNamespace() string {
	return p.pg.Namespace
}

// Obj 返回底层 PodGroup 对象。
func (p *podGroupExecutorPreemptor) Obj() runtime.Object {
	return p.pg
}

// Priority 返回 PodGroup 的优先级。
func (p *podGroupExecutorPreemptor) Priority() int32 {
	return util.PodGroupPriority(p.pg)
}

// Pods 返回 PodGroup 中未调度的 Pod，以名字为键。
func (p *podGroupExecutorPreemptor) Pods() map[string]*v1.Pod {
	m := make(map[string]*v1.Pod, len(p.pods))
	for _, pod := range p.pods {
		m[pod.Name] = pod
	}
	return m
}

// Type 返回抢占方类型（PodGroup）。
func (p *podGroupExecutorPreemptor) Type() string {
	return string(fwk.PodGroupKeyType)
}

// compositePodGroupExecutorPreemptor 把 CompositePodGroup 包装成 ExecutorPreemptor。
type compositePodGroupExecutorPreemptor struct {
	cpg  *schedulingv1alpha3.CompositePodGroup
	pods []*v1.Pod
}

// UID 返回 CompositePodGroup 的 UID。
func (p *compositePodGroupExecutorPreemptor) UID() types.UID {
	return p.cpg.UID
}

// SchedulerName 返回 CompositePodGroup 中 Pod 使用的调度器名字。
func (p *compositePodGroupExecutorPreemptor) SchedulerName() string {
	// 同一 CompositePodGroup 内的 Pod 应使用相同的调度器名字。
	return p.pods[0].Spec.SchedulerName
}

// GetName 返回 CompositePodGroup 名字。
func (p *compositePodGroupExecutorPreemptor) GetName() string {
	return p.cpg.Name
}

// GetNamespace 返回 CompositePodGroup 所在命名空间。
func (p *compositePodGroupExecutorPreemptor) GetNamespace() string {
	return p.cpg.Namespace
}

// Obj 返回底层 CompositePodGroup 对象。
func (p *compositePodGroupExecutorPreemptor) Obj() runtime.Object {
	return p.cpg
}

// Priority 返回 CompositePodGroup 的优先级。
func (p *compositePodGroupExecutorPreemptor) Priority() int32 {
	return util.CompositePodGroupPriority(p.cpg)
}

// Pods 返回 CompositePodGroup 中未调度的 Pod，以名字为键。
func (p *compositePodGroupExecutorPreemptor) Pods() map[string]*v1.Pod {
	m := make(map[string]*v1.Pod, len(p.pods))
	for _, pod := range p.pods {
		m[pod.Name] = pod
	}
	return m
}

// Type 返回抢占方类型（CompositePodGroup）。
func (p *compositePodGroupExecutorPreemptor) Type() string {
	return string(fwk.CompositePodGroupKeyType)
}
