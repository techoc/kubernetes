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
	"reflect"
	"sync"
	"time"

	v1 "k8s.io/api/coordination/v1"
	v1beta1 "k8s.io/api/coordination/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	coordinationv1beta1client "k8s.io/client-go/kubernetes/typed/coordination/v1beta1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"

	coordinationv1beta1listers "k8s.io/client-go/listers/coordination/v1beta1"
)

// requeueInterval 是成功续期后下一次续期的最短间隔。
// 续期只依赖 PingTime 的推进，过短会产生无谓的写入，过长则可能让 leader 误判候选失效。
const requeueInterval = 5 * time.Minute

// CacheSyncWaiter 抽象出"等待 informer 缓存同步完成"的能力，便于替换实现与测试。
type CacheSyncWaiter interface {
	WaitForCacheSync(stopCh <-chan struct{}) map[reflect.Type]bool
}

// LeaseCandidate 是协调式 leader 选举（CoordinatedLeaderElection）中的候选者控制器。
// 它负责创建/续期 LeaseCandidate 对象，使 server 端能够感知该候选仍存活。
type LeaseCandidate struct {
	leaseClient            coordinationv1beta1client.LeaseCandidateInterface
	leaseCandidateInformer cache.SharedIndexInformer
	leaseCandidateLister   coordinationv1beta1listers.LeaseCandidateLister
	informerFactory        informers.SharedInformerFactory
	hasSynced              cache.InformerSynced

	// At most there will be one item in this Queue (since we only watch one item)
	// 队列中最多只有一项（key 恒为 0），复用限速重试语义。
	queue workqueue.TypedRateLimitingInterface[int]

	name      string
	namespace string

	// controller lease
	leaseName string

	clock clock.Clock

	binaryVersion, emulationVersion string
	strategy                        v1.CoordinatedLeaseStrategy
}

// NewCandidate creates new LeaseCandidate controller that creates a
// LeaseCandidate object if it does not exist and watches changes
// to the corresponding object and renews if PingTime is set.
// WARNING: This is an ALPHA feature. Ensure that the CoordinatedLeaderElection
// feature gate is on.
func NewCandidate(clientset kubernetes.Interface,
	candidateNamespace string,
	candidateName string,
	targetLease string,
	binaryVersion, emulationVersion string,
	strategy v1.CoordinatedLeaseStrategy,
) (*LeaseCandidate, CacheSyncWaiter, error) {
	// 只监听自己这一个候选对象，用 fieldSelector 显著降低 watch 开销。
	fieldSelector := fields.OneTermEqualSelector("metadata.name", candidateName).String()
	// A separate informer factory is required because this must start before informerFactories
	// are started for leader elected components
	// 独立的 informer factory 便于候选控制器先于其他组件启动并进行缓存同步。
	informerFactory := informers.NewSharedInformerFactoryWithOptions(
		clientset, 5*time.Minute,
		informers.WithNamespace(candidateNamespace),
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.FieldSelector = fieldSelector
		}),
	)
	leaseCandidateInformer := informerFactory.Coordination().V1beta1().LeaseCandidates().Informer()
	leaseCandidateLister := informerFactory.Coordination().V1beta1().LeaseCandidates().Lister()

	lc := &LeaseCandidate{
		leaseClient:            clientset.CoordinationV1beta1().LeaseCandidates(candidateNamespace),
		leaseCandidateInformer: leaseCandidateInformer,
		leaseCandidateLister:   leaseCandidateLister,
		informerFactory:        informerFactory,
		name:                   candidateName,
		namespace:              candidateNamespace,
		leaseName:              targetLease,
		clock:                  clock.RealClock{},
		binaryVersion:          binaryVersion,
		emulationVersion:       emulationVersion,
		strategy:               strategy,
	}
	lc.queue = workqueue.NewTypedRateLimitingQueueWithConfig(workqueue.DefaultTypedControllerRateLimiter[int](), workqueue.TypedRateLimitingQueueConfig[int]{Name: "leasecandidate"})

	// server 端通过把 PingTime 推到 RenewTime 之后来"敲门"；一旦观察到这种变化就触发续期。
	h, err := leaseCandidateInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj interface{}) {
			if leasecandidate, ok := newObj.(*v1beta1.LeaseCandidate); ok {
				if leasecandidate.Spec.PingTime != nil && leasecandidate.Spec.PingTime.After(leasecandidate.Spec.RenewTime.Time) {
					lc.enqueueLease()
				}
			}
		},
	})
	if err != nil {
		return nil, nil, err
	}
	lc.hasSynced = h.HasSynced

	return lc, informerFactory, nil
}

// Run 启动 informer、等待缓存同步，然后进入工作循环，直到 ctx 被取消。
func (c *LeaseCandidate) Run(ctx context.Context) {
	logger := klog.FromContext(ctx)
	logger = klog.LoggerWithName(logger, "leasecandidate")
	ctx = klog.NewContext(ctx, logger)

	var wg sync.WaitGroup
	defer func() {
		c.queue.ShutDown()
		wg.Wait()
	}()

	c.informerFactory.Start(ctx.Done())
	// 缓存未同步就续期会基于不完整状态做出错误的创建/更新决定。
	if !cache.WaitForNamedCacheSyncWithContext(ctx, c.hasSynced) {
		return
	}

	// 启动后立即确保一次候选对象存在，避免第一次续期被推迟一个 requeueInterval。
	c.enqueueLease()
	wg.Go(func() {
		c.runWorker(ctx)
	})
	<-ctx.Done()
}

// runWorker 持续从队列取任务处理，直到队列关闭。
func (c *LeaseCandidate) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem 处理单个队列项：成功则按固定间隔重新入队，失败则指数退避重试。
func (c *LeaseCandidate) processNextWorkItem(ctx context.Context) bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(key)

	err := c.ensureLease(ctx)
	if err == nil {
		// 正常路径：定时续期而不是立即重试。
		c.queue.AddAfter(key, requeueInterval)
		return true
	}

	utilruntime.HandleErrorWithContext(ctx, err, "Ensuring lease failed")
	c.queue.AddRateLimited(key)

	return true
}

// enqueueLease 把唯一的任务键放入队列；重复调用会被队列去重。
func (c *LeaseCandidate) enqueueLease() {
	c.queue.Add(0)
}

// ensureLease creates the lease if it does not exist and renew it if it exists. Returns the lease and
// a bool (true if this call created the lease), or any error that occurs.
func (c *LeaseCandidate) ensureLease(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	lease, err := c.leaseCandidateLister.LeaseCandidates(c.namespace).Get(c.name)
	if apierrors.IsNotFound(err) {
		logger.V(2).Info("Creating lease candidate")
		// lease does not exist, create it.
		leaseToCreate := c.newLeaseCandidate()
		if _, err := c.leaseClient.Create(ctx, leaseToCreate, metav1.CreateOptions{}); err != nil {
			return err
		}
		logger.V(2).Info("Created lease candidate")
		return nil
	} else if err != nil {
		return err
	}
	logger.V(2).Info("Lease candidate exists. Renewing.")
	// 深拷贝后再改，避免篡改 lister 缓存中的共享对象。
	clone := lease.DeepCopy()
	clone.Spec.RenewTime = &metav1.MicroTime{Time: c.clock.Now()}
	_, err = c.leaseClient.Update(ctx, clone, metav1.UpdateOptions{})
	if err != nil {
		return err
	}
	return nil
}

// newLeaseCandidate 构造一个新的候选对象，并把 RenewTime 初始化为当前时间。
func (c *LeaseCandidate) newLeaseCandidate() *v1beta1.LeaseCandidate {
	lc := &v1beta1.LeaseCandidate{
		ObjectMeta: metav1.ObjectMeta{
			Name:      c.name,
			Namespace: c.namespace,
		},
		Spec: v1beta1.LeaseCandidateSpec{
			LeaseName:        c.leaseName,
			BinaryVersion:    c.binaryVersion,
			EmulationVersion: c.emulationVersion,
			Strategy:         c.strategy,
		},
	}
	lc.Spec.RenewTime = &metav1.MicroTime{Time: c.clock.Now()}
	return lc
}
