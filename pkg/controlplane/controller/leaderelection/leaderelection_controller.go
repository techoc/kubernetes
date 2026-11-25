/*
Copyright 2024 The Kubernetes Authors.

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

package leaderelection

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	v1 "k8s.io/api/coordination/v1"
	v1beta1 "k8s.io/api/coordination/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coordinationv1informers "k8s.io/client-go/informers/coordination/v1"
	coordinationv1beta1 "k8s.io/client-go/informers/coordination/v1beta1"
	coordinationv1client "k8s.io/client-go/kubernetes/typed/coordination/v1"
	coordinationv1beta1client "k8s.io/client-go/kubernetes/typed/coordination/v1beta1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
)

// 协调式选主控制器使用的常量。
const (
	controllerName = "leader-election-controller"

	// Requeue interval 是重新入队 Lease 以检查其是否正常续约的间隔。
	defaultRequeueInterval = 5 * time.Second
	noRequeue              = 0

	defaultLeaseDurationSeconds int32 = 5

	electionDuration = 5 * time.Second

	leaseCandidateValidDuration = 30 * time.Minute
)

// Controller 是选主控制器，观察自荐为候选者的组件身份租约，
// 并为这些租约选举 leader，
// 优先选择版本更高的候选者。
type Controller struct {
	leaseInformer     coordinationv1informers.LeaseInformer
	leaseClient       coordinationv1client.CoordinationV1Interface
	leaseRegistration cache.ResourceEventHandlerRegistration

	leaseCandidateInformer     coordinationv1beta1.LeaseCandidateInformer
	leaseCandidateClient       coordinationv1beta1client.CoordinationV1beta1Interface
	leaseCandidateRegistration cache.ResourceEventHandlerRegistration

	queue workqueue.TypedRateLimitingInterface[types.NamespacedName]

	clock clock.Clock
}

// Run 启动选主控制器及选举 worker。
func (c *Controller) Run(ctx context.Context, workers int) {
	var wg sync.WaitGroup
	defer utilruntime.HandleCrashWithContext(ctx)
	defer wg.Wait()
	defer c.queue.ShutDown()
	defer func() {
		err := c.leaseInformer.Informer().RemoveEventHandler(c.leaseRegistration)
		if err != nil {
			klog.Warning("error removing leaseInformer eventhandler")
		}
		err = c.leaseCandidateInformer.Informer().RemoveEventHandler(c.leaseCandidateRegistration)
		if err != nil {
			klog.Warning("error removing leaseCandidateInformer eventhandler")
		}
	}()

	if !cache.WaitForNamedCacheSyncWithContext(ctx, c.leaseRegistration.HasSynced, c.leaseCandidateRegistration.HasSynced) {
		return
	}

	// 本控制器自身是选主运行的，可能在 informer 启动后才开始，因此启动时需要先 List 一次。
	lcs, err := c.leaseCandidateInformer.Lister().List(labels.Everything())
	if err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Failed to list lease candidates")
		return
	}
	for _, lc := range lcs {
		c.enqueueCandidate(lc)
	}

	klog.Infof("Workers: %d", workers)
	for range workers {
		klog.Infof("Starting worker")
		wg.Go(func() {
			wait.UntilWithContext(ctx, c.runElectionWorker, time.Second)
		})
	}
	<-ctx.Done()
}

// NewController 创建协调式选主控制器。
func NewController(leaseInformer coordinationv1informers.LeaseInformer, leaseCandidateInformer coordinationv1beta1.LeaseCandidateInformer, leaseClient coordinationv1client.CoordinationV1Interface, leaseCandidateClient coordinationv1beta1client.CoordinationV1beta1Interface) (*Controller, error) {
	c := &Controller{
		leaseInformer:          leaseInformer,
		leaseCandidateInformer: leaseCandidateInformer,
		leaseClient:            leaseClient,
		leaseCandidateClient:   leaseCandidateClient,

		queue: workqueue.NewTypedRateLimitingQueueWithConfig(workqueue.DefaultTypedControllerRateLimiter[types.NamespacedName](), workqueue.TypedRateLimitingQueueConfig[types.NamespacedName]{Name: controllerName}),

		clock: clock.RealClock{},
	}
	leaseSynced, err := leaseInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			c.enqueueLease(obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			c.enqueueLease(newObj)
		},
		DeleteFunc: func(oldObj interface{}) {
			c.enqueueLease(oldObj)
		},
	})
	if err != nil {
		return nil, err
	}
	leaseCandidateSynced, err := leaseCandidateInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			c.enqueueCandidate(obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			c.enqueueCandidate(newObj)
		},
		DeleteFunc: func(oldObj interface{}) {
			c.enqueueCandidate(oldObj)
		},
	})
	if err != nil {
		return nil, err
	}
	c.leaseRegistration = leaseSynced
	c.leaseCandidateRegistration = leaseCandidateSynced
	return c, nil
}

// runElectionWorker 启动选举处理 worker。
func (c *Controller) runElectionWorker(ctx context.Context) {
	for c.processNextElectionItem(ctx) {
	}
}

// processNextElectionItem 处理队列中的下一个选举任务。
func (c *Controller) processNextElectionItem(ctx context.Context) bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}

	intervalForRequeue, err := c.reconcileElectionStep(ctx, key)
	if err != nil { // HandleErrorWithContext 即使错误值为 nil 也会记录，故需先过滤。
		utilruntime.HandleErrorWithContext(ctx, err, "Failed to reconcile election step", "key", key)
	}
	if intervalForRequeue != noRequeue {
		defer c.queue.AddAfter(key, intervalForRequeue)
	}
	c.queue.Done(key)
	return true
}

// enqueueCandidate 将候选者变更加入工作队列。
func (c *Controller) enqueueCandidate(obj any) {
	lc, ok := obj.(*v1beta1.LeaseCandidate)
	if !ok {
		return
	}
	if lc == nil {
		return
	}
	// 忽略转为 Pending 的候选者，因为重新选举已在进行中。
	if lc.Spec.PingTime != nil && lc.Spec.RenewTime.Before(lc.Spec.PingTime) {
		return
	}
	c.queue.Add(types.NamespacedName{Namespace: lc.Namespace, Name: lc.Spec.LeaseName})
}

// enqueueLease 将 Lease 变更加入工作队列。
func (c *Controller) enqueueLease(obj any) {
	lease, ok := obj.(*v1.Lease)
	if !ok {
		return
	}
	c.queue.Add(types.NamespacedName{Namespace: lease.Namespace, Name: lease.Name})
}

// electionNeeded 判断是否需要发起新一轮选举。
func (c *Controller) electionNeeded(candidates []*v1beta1.LeaseCandidate, leaseNN types.NamespacedName) (bool, error) {
	lease, err := c.leaseInformer.Lister().Leases(leaseNN.Namespace).Get(leaseNN.Name)
	if err != nil && !apierrors.IsNotFound(err) {
		return false, fmt.Errorf("error reading lease: %w", err)
	} else if apierrors.IsNotFound(err) {
		return true, nil
	}

	if isLeaseExpired(c.clock, lease) || lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity == "" {
		return true, nil
	}

	// 每 15 分钟强制选举以更新所有候选者，每 30 分钟执行一次垃圾回收。
	for _, candidate := range candidates {
		if candidate.Spec.RenewTime != nil && candidate.Spec.RenewTime.Add(leaseCandidateValidDuration/2).Before(c.clock.Now()) {
			return true, nil
		}
	}

	prelimStrategy, err := pickBestStrategy(candidates)
	if err != nil {
		return false, err
	}
	if prelimStrategy != v1.OldestEmulationVersion {
		klog.V(5).Infof("Strategy %q is ignored by CLE", prelimStrategy)
		return false, nil
	}

	prelimElectee := pickBestLeaderOldestEmulationVersion(candidates)
	if prelimElectee == nil {
		return false, nil
	} else if lease != nil && lease.Spec.HolderIdentity != nil && prelimElectee.Name == *lease.Spec.HolderIdentity {
		klog.V(5).Infof("Leader %s is already most optimal for lease %s", prelimElectee.Name, leaseNN)
		return false, nil
	}
	return true, nil
}

// reconcileElectionStep 推进选举状态机一步：
// 查看 Lease 与 LeaseCandidate 当前状态并采取相应动作。
// - 不操作（leader 已最优或仍在等待事件）
// - 请求候选者 ack（更新 LeaseCandidate 的 PingTime）
// - 选出最优候选者并完成选举（更新 Lease 对象）
// 不维护选举状态 map 与锁，
// 而是每次根据租约和可用候选者集合重新计算状态。
// PingTime + electionDuration > now：已请求所有候选者 ack，仍在等待响应。
// PingTime + electionDuration < now：候选者未在规定时间响应，继续选举。
// RenewTime + 5s > now：所有候选者最近 5 秒内已 ack，继续选举。
func (c *Controller) reconcileElectionStep(ctx context.Context, leaseNN types.NamespacedName) (requeue time.Duration, err error) {
	candidates, err := c.listAdmissableCandidates(leaseNN)
	if err != nil {
		return defaultRequeueInterval, err
	} else if len(candidates) == 0 {
		return noRequeue, nil
	}
	klog.V(6).Infof("Reconciling election for %s, candidates: %d", leaseNN, len(candidates))

	// 根据当前租约与候选者判断确实需要发起选举。
	needElection, err := c.electionNeeded(candidates, leaseNN)
	if !needElection {
		return defaultRequeueInterval, err
	}
	if err != nil {
		return defaultRequeueInterval, err
	}

	now := c.clock.Now()
	canVoteYet := true
	g, gCtx := errgroup.WithContext(ctx)
	for _, candidate := range candidates {
		if candidate.Spec.PingTime != nil && candidate.Spec.PingTime.Add(electionDuration).After(now) &&
			candidate.Spec.RenewTime != nil && candidate.Spec.RenewTime.Before(candidate.Spec.PingTime) {

			// 继续等待选举超时。
			canVoteYet = false
			continue
		}
		if candidate.Spec.RenewTime != nil && candidate.Spec.RenewTime.Add(electionDuration).After(now) {
			continue
		}

		if candidate.Spec.PingTime == nil ||
			// PingTime 过期时，仅当候选者已 ack 第一次 ping 才发送新的 PingTime。
			// 用 pingTime <= renewTime 是因为单测假时钟下二者可能相等。
			(candidate.Spec.PingTime.Add(electionDuration).Before(now) && !candidate.Spec.RenewTime.Before(candidate.Spec.PingTime)) {
			clone := candidate.DeepCopy()
			clone.Spec.PingTime = &metav1.MicroTime{Time: now}
			g.Go(func() error {
				_, err := c.leaseCandidateClient.LeaseCandidates(clone.Namespace).Update(gCtx, clone, metav1.UpdateOptions{})
				return err
			})
			canVoteYet = false
		}
	}
	if err := g.Wait(); err != nil {
		return defaultRequeueInterval, err
	}
	if !canVoteYet {
		return defaultRequeueInterval, nil
	}

	// 只要存在未过期的 PingTime，选举就仍在进行中。
	for _, candidate := range candidates {
		if candidate.Spec.PingTime == nil {
			continue // 经过上述检查后不应出现
		}

		if candidate.Spec.RenewTime != nil && !candidate.Spec.RenewTime.Before(candidate.Spec.PingTime) {
			continue // 该候选者已完成续约
		}

		// 候选者的 PingTime 仍在选举时长内说明尚未 ack，需等待其响应。
		if candidate.Spec.PingTime.Add(electionDuration).After(now) {
			// 继续等待选举超时。
			return noRequeue, nil
		}
	}

	var ackedCandidates []*v1beta1.LeaseCandidate
	for _, candidate := range candidates {
		if candidate.Spec.RenewTime.Add(electionDuration).After(now) {
			ackedCandidates = append(ackedCandidates, candidate)
		}
	}
	if len(ackedCandidates) == 0 {
		return noRequeue, fmt.Errorf("no available candidates")
	}

	strategy, err := pickBestStrategy(ackedCandidates)
	if err != nil {
		return noRequeue, err
	}

	leaderLease := &v1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: leaseNN.Namespace,
			Name:      leaseNN.Name,
		},
		Spec: v1.LeaseSpec{
			Strategy:             &strategy,
			LeaseDurationSeconds: ptr.To(defaultLeaseDurationSeconds),
			RenewTime:            &metav1.MicroTime{Time: c.clock.Now()},
		},
	}

	switch strategy {
	case v1.OldestEmulationVersion:
		electee := pickBestLeaderOldestEmulationVersion(ackedCandidates)
		if electee == nil {
			return noRequeue, fmt.Errorf("should not happen, could not find suitable electee")
		}
		leaderLease.Spec.HolderIdentity = &electee.Name
	default:
		// 不设置 holder 身份，留给其他控制器处理；但仍继续创建不含 holder 的租约。
		klog.V(2).Infof("Election for strategy %q is not handled by %s", strategy, controllerName)
	}

	// 创建选主租约。
	_, err = c.leaseClient.Leases(leaseNN.Namespace).Create(ctx, leaderLease, metav1.CreateOptions{})
	if err == nil {
		if leaderLease.Spec.HolderIdentity != nil {
			klog.Infof("Created lease %s for %q", leaseNN, *leaderLease.Spec.HolderIdentity)
		} else {
			klog.Infof("Created lease %s without leader", leaseNN)
		}
		return defaultRequeueInterval, nil
	} else if !apierrors.IsAlreadyExists(err) {
		return noRequeue, err
	}

	// 获取已有租约。
	existing, err := c.leaseClient.Leases(leaseNN.Namespace).Get(ctx, leaseNN.Name, metav1.GetOptions{})
	if err != nil {
		return noRequeue, err
	}
	orig := existing.DeepCopy()

	isExpired := isLeaseExpired(c.clock, existing)
	noHolderIdentity := leaderLease.Spec.HolderIdentity != nil && (existing.Spec.HolderIdentity == nil || *existing.Spec.HolderIdentity == "")
	expiredAndNewHolder := isExpired && leaderLease.Spec.HolderIdentity != nil && *existing.Spec.HolderIdentity != *leaderLease.Spec.HolderIdentity
	strategyChanged := existing.Spec.Strategy == nil || *existing.Spec.Strategy != strategy
	differentHolder := leaderLease.Spec.HolderIdentity != nil && *leaderLease.Spec.HolderIdentity != *existing.Spec.HolderIdentity

	// 更新租约。
	if strategyChanged {
		klog.Infof("Lease %s strategy changed to %q", leaseNN, strategy)
		existing.Spec.Strategy = &strategy
	}
	if noHolderIdentity || expiredAndNewHolder {
		if noHolderIdentity {
			klog.Infof("Lease %s had no holder, setting holder to %q", leaseNN, *leaderLease.Spec.HolderIdentity)
		} else {
			klog.Infof("Lease %s expired, resetting it and setting holder to %q", leaseNN, *leaderLease.Spec.HolderIdentity)
		}

		existing.Spec.PreferredHolder = nil
		existing.Spec.HolderIdentity = leaderLease.Spec.HolderIdentity
		existing.Spec.RenewTime = &metav1.MicroTime{Time: time.Now()}
		existing.Spec.LeaseDurationSeconds = ptr.To(defaultLeaseDurationSeconds)
		existing.Spec.AcquireTime = nil
	} else if differentHolder {
		klog.Infof("Lease %s holder changed from %q to %q", leaseNN, *existing.Spec.HolderIdentity, *leaderLease.Spec.HolderIdentity)
		existing.Spec.PreferredHolder = leaderLease.Spec.HolderIdentity
	}

	if reflect.DeepEqual(existing, orig) {
		if existing.Spec.HolderIdentity != nil {
			klog.V(5).Infof("Lease %s is managed by a third party strategy", *existing.Spec.HolderIdentity)
		} else {
			klog.V(5).Infof("Lease %s already has the most optimal leader %q", leaseNN, "")
		}
		// 需要重新入队，确保能感知租约过期。
		return defaultRequeueInterval, nil
	}

	_, err = c.leaseClient.Leases(leaseNN.Namespace).Update(ctx, existing, metav1.UpdateOptions{})
	if err != nil {
		return noRequeue, err
	}

	return defaultRequeueInterval, nil
}

// listAdmissableCandidates 列出可参与选举的候选者。
func (c *Controller) listAdmissableCandidates(leaseNN types.NamespacedName) ([]*v1beta1.LeaseCandidate, error) {
	leases, err := c.leaseCandidateInformer.Lister().LeaseCandidates(leaseNN.Namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}
	var results []*v1beta1.LeaseCandidate
	for _, l := range leases {
		if l.Spec.LeaseName != leaseNN.Name {
			continue
		}
		if !isLeaseCandidateExpired(c.clock, l) {
			results = append(results, l)
		} else {
			klog.Infof("LeaseCandidate %s is expired", l.Name)
		}
	}
	return results, nil
}
