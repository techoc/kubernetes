/*
Copyright 2022 The Kubernetes Authors.

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

package legacytokentracking

import (
	"context"
	"time"

	"golang.org/x/time/rate"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	corev1informers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

// legacy token 跟踪控制器使用的常量。
const (
	ConfigMapName    = "kube-apiserver-legacy-service-account-token-tracking"
	ConfigMapDataKey = "since"
	dateFormat       = "2006-01-02"
)

// 控制器使用的包级变量。
var (
	queueKey = metav1.NamespaceSystem + "/" + ConfigMapName
)

// Controller 维护 `kube-system` 中的时间戳 ConfigMap `kube-apiserver-legacy-service-account-token-tracking`，
// 用于表明集群是否启用了 legacy token 跟踪。
// 在 HA 集群中，所有控制器实例都启用该特性后，ConfigMap 最终会被创建。
// 禁用该特性时，已有的 ConfigMap 会被删除。

type Controller struct {
	configMapClient   corev1client.ConfigMapsGetter
	configMapInformer cache.SharedIndexInformer
	configMapCache    cache.Indexer
	configMapSynced   cache.InformerSynced
	queue             workqueue.TypedRateLimitingInterface[string]

	// 限流器控制 ConfigMap 的创建速率。
	// 在多 apiserver 集群中，这可以避免部分 apiserver 启用、部分禁用时配置反复出现：
	// 否则这些 apiserver 会争相创建/删除，直到全部启用或全部禁用。

	creationRatelimiter *rate.Limiter
	clock               clock.Clock
}

// NewController 返回一个 Controller 实例。
func NewController(cs kubernetes.Interface) *Controller {
	return newController(cs, clock.RealClock{}, rate.NewLimiter(rate.Every(30*time.Minute), 1))
}

// newController 创建使用指定时钟与限流器的控制器，供测试注入。
func newController(cs kubernetes.Interface, cl clock.Clock, limiter *rate.Limiter) *Controller {
	informer := corev1informers.NewFilteredConfigMapInformer(cs, metav1.NamespaceSystem, 12*time.Hour, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}, func(options *metav1.ListOptions) {
		options.FieldSelector = fields.OneTermEqualSelector("metadata.name", ConfigMapName).String()
	})

	c := &Controller{
		configMapClient: cs.CoreV1(),
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "legacy_token_tracking_controller"},
		),
		configMapInformer:   informer,
		configMapCache:      informer.GetIndexer(),
		configMapSynced:     informer.HasSynced,
		creationRatelimiter: limiter,
		clock:               cl,
	}

	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			c.enqueue()
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			c.enqueue()
		},
		DeleteFunc: func(obj interface{}) {
			c.enqueue()
		},
	})

	return c
}

// enqueue 将同步任务加入工作队列。
func (c *Controller) enqueue() {
	c.queue.Add(queueKey)
}

// Run 启动控制器的同步循环。
//
//logcheck:context // RunWithContext should be used instead of Run in code which supports contextual logging.
func (c *Controller) Run(stopCh <-chan struct{}) {
	c.RunWithContext(wait.ContextForChannel(stopCh))
}

// RunWithContext 启动带上下文的控制器同步循环。
func (c *Controller) RunWithContext(ctx context.Context) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	logger := klog.FromContext(ctx)

	logger.Info("Starting legacy_token_tracking_controller")
	defer logger.Info("Shutting down legacy_token_tracking_controller")

	go c.configMapInformer.Run(ctx.Done())
	if !cache.WaitForNamedCacheSyncWithContext(ctx, c.configMapSynced) {
		return
	}

	go wait.UntilWithContext(ctx, c.runWorker, time.Second)

	c.queue.Add(queueKey)

	<-ctx.Done()
	logger.Info("Ending legacy_token_tracking_controller")
}

// runWorker 持续处理工作队列中的任务。
func (c *Controller) runWorker(ctx context.Context) {
	for c.processNext(ctx) {
	}
}

// processNext 处理队列中的下一个任务。
func (c *Controller) processNext(ctx context.Context) bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)

	if err := c.syncConfigMap(ctx); err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Error while syncing ConfigMap", "configmap", key)
		c.queue.AddRateLimited(key)
		return true
	}
	c.queue.Forget(key)
	return true
}

// syncConfigMap 将 legacy token 使用情况同步到 ConfigMap。
func (c *Controller) syncConfigMap(ctx context.Context) error {
	obj, exists, err := c.configMapCache.GetByKey(queueKey)
	if err != nil {
		return err
	}

	now := c.clock.Now()
	if !exists {
		r := c.creationRatelimiter.ReserveN(now, 1)
		if delay := r.DelayFrom(now); delay > 0 {
			c.queue.AddAfter(queueKey, delay)
			r.CancelAt(now)
			return nil
		}

		if _, err = c.configMapClient.ConfigMaps(metav1.NamespaceSystem).Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: metav1.NamespaceSystem, Name: ConfigMapName},
			Data:       map[string]string{ConfigMapDataKey: now.UTC().Format(dateFormat)},
		}, metav1.CreateOptions{}); err != nil {
			if apierrors.IsAlreadyExists(err) {
				return nil
			}
			// 失败的尝试不应消耗创建限流器的配额。
			r.CancelAt(now)
			return err
		}
	} else {
		configMap := obj.(*corev1.ConfigMap)
		if _, err = time.Parse(dateFormat, configMap.Data[ConfigMapDataKey]); err != nil {
			configMap := configMap.DeepCopy()
			if configMap.Data == nil {
				configMap.Data = map[string]string{}
			}
			configMap.Data[ConfigMapDataKey] = now.UTC().Format(dateFormat)
			if _, err = c.configMapClient.ConfigMaps(metav1.NamespaceSystem).Update(ctx, configMap, metav1.UpdateOptions{}); err != nil {
				if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
					return nil
				}
				return err
			}
		}
	}

	return nil
}
