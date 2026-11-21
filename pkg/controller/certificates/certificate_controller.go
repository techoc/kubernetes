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

// Package certificates 提供一个抽象控制器，可用于构建管理 CSR（证书签名请求）的控制器。
// 具体签名/审批逻辑由注入的 handler 决定，本包负责事件监听、队列与重试框架。
package certificates

import (
	"context"
	"fmt"
	"sync"
	"time"

	"golang.org/x/time/rate"

	certificates "k8s.io/api/certificates/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	certificatesinformers "k8s.io/client-go/informers/certificates/v1"
	clientset "k8s.io/client-go/kubernetes"
	certificateslisters "k8s.io/client-go/listers/certificates/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller"
)

// CertificateController 是管理 CSR 的通用控制器框架，
// 通过 worker 从限速队列中取出请求并交给注入的 handler 处理。
type CertificateController struct {
	// name 是控制器实例的标识，用于日志区分多个同类控制器。
	name string

	kubeClient clientset.Interface

	csrLister  certificateslisters.CertificateSigningRequestLister
	csrsSynced cache.InformerSynced

	handler func(context.Context, *certificates.CertificateSigningRequest) error

	// queue 是限速工作队列：失败的请求按指数退避重试，
	// 同时受全局速率限制，防止异常流量压垮 API 服务器。
	queue workqueue.TypedRateLimitingInterface[string]
}

// NewCertificateController 创建控制器实例，注册 CSR 事件处理器并配置限速队列。
// 指数退避（200ms 到 1000s）处理单个失败项的重试间隔；
// 令牌桶（10 qps，容量 100）限制整体重试速率。
func NewCertificateController(
	ctx context.Context,
	name string,
	kubeClient clientset.Interface,
	csrInformer certificatesinformers.CertificateSigningRequestInformer,
	handler func(context.Context, *certificates.CertificateSigningRequest) error,
) *CertificateController {
	logger := klog.FromContext(ctx)
	cc := &CertificateController{
		name:       name,
		kubeClient: kubeClient,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.NewTypedMaxOfRateLimiter[string](
				workqueue.NewTypedItemExponentialFailureRateLimiter[string](200*time.Millisecond, 1000*time.Second),
				// 10 qps、容量 100：仅限制整体重试速度，不针对单个条目。
				&workqueue.TypedBucketRateLimiter[string]{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
			),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: "certificate",
			},
		),
		handler: handler,
	}

	// 监听 CSR 的新增、更新与删除事件，统一入队处理。
	csrInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			csr := obj.(*certificates.CertificateSigningRequest)
			logger.V(4).Info("Adding certificate request", "csr", csr.Name)
			cc.enqueueCertificateRequest(obj)
		},
		UpdateFunc: func(old, new interface{}) {
			oldCSR := old.(*certificates.CertificateSigningRequest)
			logger.V(4).Info("Updating certificate request", "old", oldCSR.Name)
			cc.enqueueCertificateRequest(new)
		},
		DeleteFunc: func(obj interface{}) {
			csr, ok := obj.(*certificates.CertificateSigningRequest)
			// 删除事件可能收到 tombstone（对象已从缓存移除），需解包取出原始对象。
			if !ok {
				tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
				if !ok {
					logger.V(2).Info("Couldn't get object from tombstone", "object", obj)
					return
				}
				csr, ok = tombstone.Obj.(*certificates.CertificateSigningRequest)
				if !ok {
					logger.V(2).Info("Tombstone contained object that is not a CSR", "object", obj)
					return
				}
			}
			logger.V(4).Info("Deleting certificate request", "csr", csr.Name)
			cc.enqueueCertificateRequest(obj)
		},
	})
	cc.csrLister = csrInformer.Lister()
	cc.csrsSynced = csrInformer.Informer().HasSynced
	return cc
}

// Run 启动控制器的主循环：先等待 informer 缓存同步，再启动指定数量的 worker。
// ctx 取消时会关闭队列并等待所有 worker 退出，保证优雅停机。
func (cc *CertificateController) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrash()

	logger := klog.FromContext(ctx)
	logger.Info("Starting certificate controller", "name", cc.name)

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down certificate controller", "name", cc.name)
		cc.queue.ShutDown()
		wg.Wait()
	}()

	// 必须等缓存同步完成后再处理队列，避免基于空缓存错误地判断 CSR 不存在。
	if !cache.WaitForNamedCacheSyncWithContext(ctx, cc.csrsSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		wg.Go(func() {
			wait.UntilWithContext(ctx, cc.worker, time.Second)
		})
	}
	<-ctx.Done()
}

// worker 持续从队列中取出并处理 CSR，直到队列关闭。
func (cc *CertificateController) worker(ctx context.Context) {
	for cc.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem 处理队列中的一个 key。
// 同步失败时重新限速入队重试；成功时调用 Forget 清除该 key 的失败计数。
// 队列关闭时返回 false，指示 worker 退出。
func (cc *CertificateController) processNextWorkItem(ctx context.Context) bool {
	cKey, quit := cc.queue.Get()
	if quit {
		return false
	}
	defer cc.queue.Done(cKey)

	if err := cc.syncFunc(ctx, cKey); err != nil {
		// 失败后必须限速重试，避免热循环；错误本身也保证该 key 会被再次处理。
		cc.queue.AddRateLimited(cKey)
		if _, ignorable := err.(ignorableError); !ignorable {
			utilruntime.HandleError(fmt.Errorf("Sync %v failed with : %v", cKey, err))
		} else {
			klog.FromContext(ctx).V(4).Info("Sync certificate request failed", "csr", cKey, "err", err)
		}
		return true
	}

	cc.queue.Forget(cKey)
	return true

}

// enqueueCertificateRequest 将对象转换为 namespace/name 形式的 key 并入队。
func (cc *CertificateController) enqueueCertificateRequest(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %+v: %v", obj, err))
		return
	}
	cc.queue.Add(key)
}

// syncFunc 从缓存读取 CSR 并交给 handler 处理。
// 已签发证书或已删除的 CSR 直接跳过；处理前深拷贝，避免修改 informer 共享缓存中的对象。
func (cc *CertificateController) syncFunc(ctx context.Context, key string) error {
	logger := klog.FromContext(ctx)
	startTime := time.Now()
	defer func() {
		logger.V(4).Info("Finished syncing certificate request", "csr", key, "elapsedTime", time.Since(startTime))
	}()
	csr, err := cc.csrLister.Get(key)
	if errors.IsNotFound(err) {
		logger.V(3).Info("csr has been deleted", "csr", key)
		return nil
	}
	if err != nil {
		return err
	}

	if len(csr.Status.Certificate) > 0 {
		// 已有证书的 CSR 无需再处理，直接跳过。
		return nil
	}

	// 必须在副本上操作，否则 handler 对状态的修改会污染共享的 informer 缓存。
	csr = csr.DeepCopy()
	return cc.handler(ctx, csr)
}

// IgnorableError 构造一个"可忽略"错误：这类错误通常源于用户配置错误且会产生大量日志，
// 因此只在更高日志级别记录。但错误仍需返回以触发同步重试。
func IgnorableError(s string, args ...interface{}) ignorableError {
	return ignorableError(fmt.Sprintf(s, args...))
}

// ignorableError 是可忽略错误的内部类型。
type ignorableError string

// Error 实现 error 接口。
func (e ignorableError) Error() string {
	return string(e)
}
