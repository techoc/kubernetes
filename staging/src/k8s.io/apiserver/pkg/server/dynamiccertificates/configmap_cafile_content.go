/*
Copyright 2019 The Kubernetes Authors.

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

package dynamiccertificates

import (
	"bytes"
	"context"
	"crypto/x509"
	"fmt"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	corev1informers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

// ConfigMapCAController provies a CAContentProvider that can dynamically react to configmap changes
// It also fulfills the authenticator interface to provide verifyoptions
//
// 控制器只 watch 单个 ConfigMap 的单个 key：CA 轮换必须以集群内对象为唯一事实来源，
// 而不是本地文件，这样所有 apiserver 副本能在同一时刻看到同一份 bundle。
type ConfigMapCAController struct {
	name string

	configmapLister    corev1listers.ConfigMapLister
	configmapNamespace string
	configmapName      string
	configmapKey       string
	// configMapInformer is tracked so that we can start these on Run
	configMapInformer cache.SharedIndexInformer

	// caBundle is a caBundleAndVerifier that contains the last read, non-zero length content of the file
	//
	// 用 atomic.Value 保存不可变快照：认证热路径上读 CA 不加锁，替换时整对象原子换掉。
	caBundle atomic.Value

	listeners []Listener

	queue workqueue.TypedRateLimitingInterface[string]
	// preRunCaches are the caches to sync before starting the work of this control loop
	preRunCaches []cache.InformerSynced
}

var _ CAContentProvider = &ConfigMapCAController{}
var _ ControllerRunner = &ConfigMapCAController{}

// NewDynamicCAFromConfigMapController returns a CAContentProvider based on a configmap that automatically reloads content.
// It is near-realtime via an informer.
func NewDynamicCAFromConfigMapController(purpose, namespace, name, key string, kubeClient kubernetes.Interface) (*ConfigMapCAController, error) {
	// purpose 用于区分不同用途的 CA（如 client-ca、requestheader-client-ca），会出现在指标与日志中。
	if len(purpose) == 0 {
		return nil, fmt.Errorf("missing purpose for ca bundle")
	}
	if len(namespace) == 0 {
		return nil, fmt.Errorf("missing namespace for ca bundle")
	}
	if len(name) == 0 {
		return nil, fmt.Errorf("missing name for ca bundle")
	}
	if len(key) == 0 {
		return nil, fmt.Errorf("missing key for ca bundle")
	}
	caContentName := fmt.Sprintf("%s::%s::%s::%s", purpose, namespace, name, key)

	// we construct our own informer because we need such a small subset of the information available.  Just one namespace.
	//
	// 不用共享 informer 是为了把 watch 范围压到“单命名空间 + 单对象”，
	// resync 周期设得很长（12h）因为这里依赖 watch 事件而非定期全量。
	uncastConfigmapInformer := corev1informers.NewFilteredConfigMapInformer(kubeClient, namespace, 12*time.Hour, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}, func(listOptions *v1.ListOptions) {
		listOptions.FieldSelector = fields.OneTermEqualSelector("metadata.name", name).String()
	})

	configmapLister := corev1listers.NewConfigMapLister(uncastConfigmapInformer.GetIndexer())

	c := &ConfigMapCAController{
		name:               caContentName,
		configmapNamespace: namespace,
		configmapName:      name,
		configmapKey:       key,
		configmapLister:    configmapLister,
		configMapInformer:  uncastConfigmapInformer,

		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: fmt.Sprintf("DynamicConfigMapCABundle-%s", purpose)},
		),
		preRunCaches: []cache.InformerSynced{uncastConfigmapInformer.HasSynced},
	}

	uncastConfigmapInformer.AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: func(obj interface{}) bool {
			if cast, ok := obj.(*corev1.ConfigMap); ok {
				return cast.Name == c.configmapName && cast.Namespace == c.configmapNamespace
			}
			// 删除事件可能以 tombstone 形式到达，必须解包后再比较，否则会漏掉删除。
			if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				if cast, ok := tombstone.Obj.(*corev1.ConfigMap); ok {
					return cast.Name == c.configmapName && cast.Namespace == c.configmapNamespace
				}
			}
			return true // always return true just in case.  The checks are fairly cheap
		},
		Handler: cache.ResourceEventHandlerFuncs{
			// we have a filter, so any time we're called, we may as well queue. We only ever check one configmap
			// so we don't have to be choosy about our key.
			AddFunc: func(obj interface{}) {
				c.queue.Add(c.keyFn())
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				c.queue.Add(c.keyFn())
			},
			DeleteFunc: func(obj interface{}) {
				c.queue.Add(c.keyFn())
			},
		},
	})

	return c, nil
}

// keyFn 返回队列键；格式与 DeletionHandlingMetaNamespaceKeyFunc 一致，便于排查日志时对照。
func (c *ConfigMapCAController) keyFn() string {
	// this format matches DeletionHandlingMetaNamespaceKeyFunc for our single key
	return c.configmapNamespace + "/" + c.configmapName
}

// AddListener adds a listener to be notified when the CA content changes.
//
// 注意：必须在 Run 之前调用。listeners 切片没有加锁，运行期追加会与 loadCABundle 并发读写。
func (c *ConfigMapCAController) AddListener(listener Listener) {
	c.listeners = append(c.listeners, listener)
}

// loadCABundle determines the next set of content for the file.
func (c *ConfigMapCAController) loadCABundle() error {
	configMap, err := c.configmapLister.ConfigMaps(c.configmapNamespace).Get(c.configmapName)
	if err != nil {
		return err
	}
	caBundle := configMap.Data[c.configmapKey]
	if len(caBundle) == 0 {
		return fmt.Errorf("missing content for CA bundle %q", c.Name())
	}

	// check to see if we have a change. If the values are the same, do nothing.
	//
	// 内容未变时直接返回，避免重建 x509 证书池与无谓地唤醒所有 listener。
	if !c.hasCAChanged([]byte(caBundle)) {
		return nil
	}

	caBundleAndVerifier, err := newCABundleAndVerifier(c.Name(), []byte(caBundle))
	if err != nil {
		return err
	}
	c.caBundle.Store(caBundleAndVerifier)

	// 先换快照再通知：listener 被唤醒后必须能读到新内容，顺序不能反。
	for _, listener := range c.listeners {
		listener.Enqueue()
	}

	return nil
}

// hasCAChanged returns true if the caBundle is different than the current.
func (c *ConfigMapCAController) hasCAChanged(caBundle []byte) bool {
	uncastExisting := c.caBundle.Load()
	if uncastExisting == nil {
		return true
	}

	// check to see if we have a change. If the values are the same, do nothing.
	existing, ok := uncastExisting.(*caBundleAndVerifier)
	if !ok {
		return true
	}
	if !bytes.Equal(existing.caBundle, caBundle) {
		return true
	}

	return false
}

// RunOnce runs a single sync loop
func (c *ConfigMapCAController) RunOnce(ctx context.Context) error {
	// Ignore the error when running once because when using a dynamically loaded ca file, because we think it's better to have nothing for
	// a brief time than completely crash.  If crashing is necessary, higher order logic like a healthcheck and cause failures.
	//
	// 启动阶段容忍失败：此时 informer 尚未同步，读不到 ConfigMap 是正常的；
	// 后续 Run 中的重试会补齐。
	_ = c.loadCABundle()
	return nil
}

// Run starts the kube-apiserver and blocks until stopCh is closed.
func (c *ConfigMapCAController) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrashWithContext(ctx)
	defer c.queue.ShutDown()

	klog.InfoS("Starting controller", "name", c.name)
	defer klog.InfoS("Shutting down controller", "name", c.name)

	// we have a personal informer that is narrowly scoped, start it.
	go c.configMapInformer.Run(ctx.Done())

	// wait for your secondary caches to fill before starting your work
	if !cache.WaitForNamedCacheSyncWithContext(ctx, c.preRunCaches...) {
		return
	}

	// doesn't matter what workers say, only start one.
	//
	// 单 worker 足够：只有一个对象要处理，多 worker 只会增加对同一快照的竞争。
	go wait.Until(c.runWorker, time.Second, ctx.Done())

	// start timer that rechecks every minute, just in case.  this also serves to prime the controller quickly.
	//
	// 周期性兜底轮询：watch 可能因网络问题漏事件，定期重读可最终收敛；
	// 同时保证启动后立即做一次加载，不必等第一个事件。
	go wait.PollImmediateUntil(FileRefreshDuration, func() (bool, error) {
		c.queue.Add(workItemKey)
		return false, nil
	}, ctx.Done())

	<-ctx.Done()
}

func (c *ConfigMapCAController) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem 处理一个队列项；失败时用限速队列重试，避免 ConfigMap 长期不可读时空转。
func (c *ConfigMapCAController) processNextWorkItem() bool {
	dsKey, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(dsKey)

	err := c.loadCABundle()
	if err == nil {
		c.queue.Forget(dsKey)
		return true
	}

	utilruntime.HandleError(fmt.Errorf("%v failed with : %v", dsKey, err))
	c.queue.AddRateLimited(dsKey)

	return true
}

// Name is just an identifier
func (c *ConfigMapCAController) Name() string {
	return c.name
}

// CurrentCABundleContent provides ca bundle byte content
func (c *ConfigMapCAController) CurrentCABundleContent() []byte {
	uncastObj := c.caBundle.Load()
	if uncastObj == nil {
		return nil // this can happen if we've been unable load data from the apiserver for some reason
	}

	return c.caBundle.Load().(*caBundleAndVerifier).caBundle
}

// VerifyOptions provides verifyoptions compatible with authenticators
//
// 第二个返回值为 false 时调用方必须拒绝请求：没有可信 CA 就无从校验客户端证书，
// 宁可拒绝也不能放行。
func (c *ConfigMapCAController) VerifyOptions() (x509.VerifyOptions, bool) {
	uncastObj := c.caBundle.Load()
	if uncastObj == nil {
		// This can happen if we've been unable load data from the apiserver for some reason.
		// In this case, we should not accept any connections on the basis of this ca bundle.
		return x509.VerifyOptions{}, false
	}

	return uncastObj.(*caBundleAndVerifier).verifyOptions, true
}
