/*
Copyright 2016 The Kubernetes Authors.

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

package bootstrap

import (
	"context"
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	bootstrapapi "k8s.io/cluster-bootstrap/token/api"
	bootstrapsecretutil "k8s.io/cluster-bootstrap/util/secrets"
	"k8s.io/klog/v2"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/controller"
)

// TokenCleanerOptions 包含创建 TokenCleaner 所需的配置项。
type TokenCleanerOptions struct {
	// TokenSecretNamespace 是引导令牌 Secret 所在的命名空间。
	TokenSecretNamespace string

	// SecretResync 是完整重新列举 Secret 的周期。
	// 若为零，则尽可能延迟重新列举，依赖 watch 事件即可。
	SecretResync time.Duration
}

// DefaultTokenCleanerOptions 返回创建 TokenCleaner 的默认配置，
// 默认只清理 kube-system 命名空间下的令牌 Secret。
func DefaultTokenCleanerOptions() TokenCleanerOptions {
	return TokenCleanerOptions{
		TokenSecretNamespace: api.NamespaceSystem,
	}
}

// TokenCleaner 是负责删除过期引导令牌的控制器。
// 它监听 Secret 事件，并通过延迟队列在令牌到期时刻精确触发清理。
type TokenCleaner struct {
	tokenSecretNamespace string

	client clientset.Interface

	// secretLister 用于列举/获取 Secret，其数据由传给 NewTokenCleaner 的共享 informer 填充，
	// 这样同步逻辑无需直接访问 API 服务器。
	secretLister corelisters.SecretLister

	// secretSynced 在 Secret 共享 informer 至少完成一次同步后返回 true。
	// 启动 worker 前必须等待它就绪，避免基于空缓存误判令牌状态。
	secretSynced cache.InformerSynced

	queue workqueue.TypedRateLimitingInterface[string]
}

// NewTokenCleaner 创建一个新的 TokenCleaner。
// 事件处理器通过 FilterFunc 只接收目标命名空间中的引导令牌 Secret，
// 从而避免其他 Secret 的变更无谓地进入工作队列。
func NewTokenCleaner(cl clientset.Interface, secrets coreinformers.SecretInformer, options TokenCleanerOptions) (*TokenCleaner, error) {
	e := &TokenCleaner{
		client:               cl,
		secretLister:         secrets.Lister(),
		secretSynced:         secrets.Informer().HasSynced,
		tokenSecretNamespace: options.TokenSecretNamespace,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: "token_cleaner",
			},
		),
	}

	// 使用带过滤能力的事件处理器，仅把引导令牌 Secret 加入队列；
	// 定时重新列举（resync）也可兜底处理 watch 事件丢失的情况。
	secrets.Informer().AddEventHandlerWithResyncPeriod(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch t := obj.(type) {
				case *v1.Secret:
					return t.Type == bootstrapapi.SecretTypeBootstrapToken && t.Namespace == e.tokenSecretNamespace
				default:
					utilruntime.HandleError(fmt.Errorf("object passed to %T that is not expected: %T", e, obj))
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    e.enqueueSecrets,
				UpdateFunc: func(oldSecret, newSecret interface{}) { e.enqueueSecrets(newSecret) },
			},
		},
		options.SecretResync,
	)

	return e, nil
}

// Run 启动控制器的工作循环，直到上下文结束才返回。
func (tc *TokenCleaner) Run(ctx context.Context) {
	defer utilruntime.HandleCrash()

	logger := klog.FromContext(ctx)
	logger.Info("Starting token cleaner controller")

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down token cleaner controller")
		tc.queue.ShutDown()
		wg.Wait()
	}()

	if !cache.WaitForNamedCacheSyncWithContext(ctx, tc.secretSynced) {
		return
	}

	wg.Go(func() {
		wait.UntilWithContext(ctx, tc.worker, 10*time.Second)
	})
	<-ctx.Done()
}

// enqueueSecrets 从对象中计算出命名空间/名称键并放入工作队列。
func (tc *TokenCleaner) enqueueSecrets(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}
	tc.queue.Add(key)
}

// worker 持续从队列中取出 Secret 并处理，直到队列关闭。
func (tc *TokenCleaner) worker(ctx context.Context) {
	for tc.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem 处理队列中的一个键；队列关闭时返回 false 表示应当退出。
// 同步失败时通过限速重新入队，避免错误导致该键被永久丢弃或无限快速重试。
func (tc *TokenCleaner) processNextWorkItem(ctx context.Context) bool {
	key, quit := tc.queue.Get()
	if quit {
		return false
	}
	defer tc.queue.Done(key)

	if err := tc.syncFunc(ctx, key); err != nil {
		tc.queue.AddRateLimited(key)
		utilruntime.HandleError(fmt.Errorf("Sync %v failed with : %v", key, err))
		return true
	}

	tc.queue.Forget(key)
	return true
}

// syncFunc 同步单个 Secret 键：从 lister 获取对象，若存在且是引导令牌则评估是否清理。
// 对象已被删除不是错误——这正是清理期望达成的结果。
func (tc *TokenCleaner) syncFunc(ctx context.Context, key string) error {
	logger := klog.FromContext(ctx)
	startTime := time.Now()
	defer func() {
		logger.V(4).Info("Finished syncing secret", "secret", key, "elapsedTime", time.Since(startTime))
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}

	ret, err := tc.secretLister.Secrets(namespace).Get(name)
	if apierrors.IsNotFound(err) {
		logger.V(3).Info("Secret has been deleted", "secret", key)
		return nil
	}

	if err != nil {
		return err
	}

	if ret.Type == bootstrapapi.SecretTypeBootstrapToken {
		tc.evalSecret(ctx, ret)
	}
	return nil
}

// evalSecret 判断令牌是否到期：到期则删除，未到期但有余生时间则按 TTL 延迟重新入队。
// 延迟入队使清理发生在到期时刻，而无需高频轮询每个 Secret。
func (tc *TokenCleaner) evalSecret(ctx context.Context, o interface{}) {
	logger := klog.FromContext(ctx)
	secret := o.(*v1.Secret)
	ttl, alreadyExpired := bootstrapsecretutil.GetExpiration(secret, time.Now())
	if alreadyExpired {
		logger.V(3).Info("Deleting expired secret", "secret", klog.KObj(secret))
		var options metav1.DeleteOptions
		// 携带 UID 前置条件，防止删除期间令牌被重建后误删同名的新 Secret。
		if len(secret.UID) > 0 {
			options.Preconditions = &metav1.Preconditions{UID: &secret.UID}
		}
		err := tc.client.CoreV1().Secrets(secret.Namespace).Delete(ctx, secret.Name, options)
		// NotFound 不是真正的错误（已被删除）；
		// Conflict 也不是（UID 前置条件失败，说明同名对象已被替换）。
		if err != nil && !apierrors.IsConflict(err) && !apierrors.IsNotFound(err) {
			logger.V(3).Info("Error deleting secret", "err", err)
		}
	} else if ttl > 0 {
		key, err := controller.KeyFunc(o)
		if err != nil {
			utilruntime.HandleError(err)
			return
		}
		tc.queue.AddAfter(key, ttl)
	}
}
