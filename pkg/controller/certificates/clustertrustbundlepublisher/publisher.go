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

package clustertrustbundlepublisher

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"sync"
	"time"

	certificatesv1 "k8s.io/api/certificates/v1"
	certificatesv1alpha1 "k8s.io/api/certificates/v1alpha1"
	certificatesv1beta1 "k8s.io/api/certificates/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	certv1informers "k8s.io/client-go/informers/certificates/v1"
	certalpha1informers "k8s.io/client-go/informers/certificates/v1alpha1"
	certbeta1informers "k8s.io/client-go/informers/certificates/v1beta1"
	clientset "k8s.io/client-go/kubernetes"
	certv1listers "k8s.io/client-go/listers/certificates/v1"
	certalphav1listers "k8s.io/client-go/listers/certificates/v1alpha1"
	certbetav1listers "k8s.io/client-go/listers/certificates/v1beta1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

// init 注册本控制器的指标，确保进程启动时指标可用。
func init() {
	registerMetrics()
}

// PublisherRunner 是证书发布控制器的运行入口接口。
type PublisherRunner interface {
	Run(context.Context)
}

// ClusterTrustBundlePublisher 负责将指定签名者的 CA 发布为 ClusterTrustBundle 对象。
type ClusterTrustBundlePublisher[T clusterTrustBundle] struct {
	signerName string
	ca         dynamiccertificates.CAContentProvider

	client clusterTrustBundlesClient[T]

	ctbInformer     cache.SharedIndexInformer
	ctbLister       clusterTrustBundlesLister[T]
	ctbListerSynced cache.InformerSynced

	handlers clusterTrustBundleHandlers[T]

	queue workqueue.TypedRateLimitingInterface[string]
}

// clusterTrustBundle 是涵盖 ClusterTrustBundles 所有 API 版本的类型约束。
type clusterTrustBundle interface {
	certificatesv1alpha1.ClusterTrustBundle | certificatesv1beta1.ClusterTrustBundle | certificatesv1.ClusterTrustBundle
}

// clusterTrustBundlesClient 是与 API 版本无关的 ClusterTrustBundles 客户端抽象。
type clusterTrustBundlesClient[T clusterTrustBundle] interface {
	Create(context.Context, *T, metav1.CreateOptions) (*T, error)
	Update(context.Context, *T, metav1.UpdateOptions) (*T, error)
	Delete(context.Context, string, metav1.DeleteOptions) error
}

// clusterTrustBundlesLister 是与 API 版本无关的 ClusterTrustBundles 查询接口。
type clusterTrustBundlesLister[T clusterTrustBundle] interface {
	Get(string) (*T, error)
	List(labels.Selector) ([]*T, error)
}

// clusterTrustBundleHandlers 封装各 API 版本操作 ClusterTrustBundle 的差异。
type clusterTrustBundleHandlers[T clusterTrustBundle] interface {
	createClusterTrustBundle(bundleName, signerName, trustBundle string) *T
	updateWithTrustBundle(ctbObject *T, newBundle string) *T
	containsTrustBundle(ctbObject *T, bundle string) bool
	getName(ctbObject *T) string
}

var _ clusterTrustBundleHandlers[certificatesv1.ClusterTrustBundle] = &gaHandlers{}
var _ clusterTrustBundleHandlers[certificatesv1beta1.ClusterTrustBundle] = &betaHandlers{}
var _ clusterTrustBundleHandlers[certificatesv1alpha1.ClusterTrustBundle] = &alphaHandlers{}

// gaHandlers 实现 v1 版本 ClusterTrustBundles 的 clusterTrustBundleHandlers。
type gaHandlers struct{}

// createClusterTrustBundle 构造一个 v1 版本的 ClusterTrustBundle 对象。
func (w *gaHandlers) createClusterTrustBundle(bundleName, signerName, trustBundle string) *certificatesv1.ClusterTrustBundle {
	return &certificatesv1.ClusterTrustBundle{
		ObjectMeta: metav1.ObjectMeta{
			Name: bundleName,
		},
		Spec: certificatesv1.ClusterTrustBundleSpec{
			SignerName:  signerName,
			TrustBundle: trustBundle,
		},
	}
}

// updateWithTrustBundle 返回信任包更新为新内容的副本，避免修改 informer 缓存中的对象。
func (w *gaHandlers) updateWithTrustBundle(ctbObject *certificatesv1.ClusterTrustBundle, newBundle string) *certificatesv1.ClusterTrustBundle {
	newObj := ctbObject.DeepCopy()
	newObj.Spec.TrustBundle = newBundle
	return newObj
}

// containsTrustBundle 判断对象中的信任包是否已是目标内容。
func (w *gaHandlers) containsTrustBundle(ctbObject *certificatesv1.ClusterTrustBundle, bundle string) bool {
	return ctbObject.Spec.TrustBundle == bundle
}

// getName 返回 ClusterTrustBundle 对象的名称。
func (w *gaHandlers) getName(ctbObject *certificatesv1.ClusterTrustBundle) string {
	return ctbObject.Name
}

// betaHandlers 实现 v1beta1 版本 ClusterTrustBundles 的 clusterTrustBundleHandlers。
type betaHandlers struct{}

// createClusterTrustBundle 构造一个 v1beta1 版本的 ClusterTrustBundle 对象。
func (w *betaHandlers) createClusterTrustBundle(bundleName, signerName, trustBundle string) *certificatesv1beta1.ClusterTrustBundle {
	return &certificatesv1beta1.ClusterTrustBundle{
		ObjectMeta: metav1.ObjectMeta{
			Name: bundleName,
		},
		Spec: certificatesv1beta1.ClusterTrustBundleSpec{
			SignerName:  signerName,
			TrustBundle: trustBundle,
		},
	}
}

// updateWithTrustBundle 返回信任包更新为新内容的副本，避免修改 informer 缓存中的对象。
func (w *betaHandlers) updateWithTrustBundle(ctbObject *certificatesv1beta1.ClusterTrustBundle, newBundle string) *certificatesv1beta1.ClusterTrustBundle {
	newObj := ctbObject.DeepCopy()
	newObj.Spec.TrustBundle = newBundle
	return newObj
}

// containsTrustBundle 判断对象中的信任包是否已是目标内容。
func (w *betaHandlers) containsTrustBundle(ctbObject *certificatesv1beta1.ClusterTrustBundle, bundle string) bool {
	return ctbObject.Spec.TrustBundle == bundle
}

// getName 返回 ClusterTrustBundle 对象的名称。
func (w *betaHandlers) getName(ctbObject *certificatesv1beta1.ClusterTrustBundle) string {
	return ctbObject.Name
}

// alphaHandlers 实现 v1alpha1 版本 ClusterTrustBundles 的 clusterTrustBundleHandlers。
type alphaHandlers struct{}

// createClusterTrustBundle 构造一个 v1alpha1 版本的 ClusterTrustBundle 对象。
func (w *alphaHandlers) createClusterTrustBundle(bundleName, signerName, trustBundle string) *certificatesv1alpha1.ClusterTrustBundle {
	return &certificatesv1alpha1.ClusterTrustBundle{
		ObjectMeta: metav1.ObjectMeta{
			Name: bundleName,
		},
		Spec: certificatesv1alpha1.ClusterTrustBundleSpec{
			SignerName:  signerName,
			TrustBundle: trustBundle,
		},
	}
}

// updateWithTrustBundle 返回信任包更新为新内容的副本，避免修改 informer 缓存中的对象。
func (w *alphaHandlers) updateWithTrustBundle(ctbObject *certificatesv1alpha1.ClusterTrustBundle, newBundle string) *certificatesv1alpha1.ClusterTrustBundle {
	newObj := ctbObject.DeepCopy()
	newObj.Spec.TrustBundle = newBundle
	return newObj
}

// containsTrustBundle 判断对象中的信任包是否已是目标内容。
func (w *alphaHandlers) containsTrustBundle(ctbObject *certificatesv1alpha1.ClusterTrustBundle, bundle string) bool {
	return ctbObject.Spec.TrustBundle == bundle
}

// getName 返回 ClusterTrustBundle 对象的名称。
func (w *alphaHandlers) getName(ctbObject *certificatesv1alpha1.ClusterTrustBundle) string {
	return ctbObject.Name
}

// caContentListener 将 CA 内容变更回调适配为 dynamiccertificates.Listener。
type caContentListener func()

// Enqueue 在 CA 内容发生变化时触发回调，通知控制器重新入队。
func (f caContentListener) Enqueue() {
	f()
}

// NewGAClusterTrustBundlePublisher 为 v1 API 创建一个 ClusterTrustBundlePublisher。
func NewGAClusterTrustBundlePublisher(
	signerName string,
	caProvider dynamiccertificates.CAContentProvider,
	kubeClient clientset.Interface,
) (
	PublisherRunner,
	error,
) {
	ctbInformer := certv1informers.NewFilteredClusterTrustBundleInformer(kubeClient, 0, cache.Indexers{},
		func(options *metav1.ListOptions) {
			options.FieldSelector = fields.OneTermEqualSelector("spec.signerName", signerName).String()
		})

	return newClusterTrustBundlePublisher(
		signerName,
		caProvider,
		kubeClient.CertificatesV1().ClusterTrustBundles(),
		ctbInformer,
		certv1listers.NewClusterTrustBundleLister(ctbInformer.GetIndexer()),
		&gaHandlers{},
	)
}

// NewBetaClusterTrustBundlePublisher 为 v1beta1 API 创建一个 ClusterTrustBundlePublisher。
func NewBetaClusterTrustBundlePublisher(
	signerName string,
	caProvider dynamiccertificates.CAContentProvider,
	kubeClient clientset.Interface,
) (
	PublisherRunner,
	error,
) {

	ctbInformer := certbeta1informers.NewFilteredClusterTrustBundleInformer(kubeClient, 0, cache.Indexers{},
		func(options *metav1.ListOptions) {
			options.FieldSelector = fields.OneTermEqualSelector("spec.signerName", signerName).String()
		})

	return newClusterTrustBundlePublisher(
		signerName,
		caProvider,
		kubeClient.CertificatesV1beta1().ClusterTrustBundles(),
		ctbInformer,
		certbetav1listers.NewClusterTrustBundleLister(ctbInformer.GetIndexer()),
		&betaHandlers{},
	)
}

// NewAlphaClusterTrustBundlePublisher 为 v1alpha1 API 创建一个 ClusterTrustBundlePublisher。
func NewAlphaClusterTrustBundlePublisher(
	signerName string,
	caProvider dynamiccertificates.CAContentProvider,
	kubeClient clientset.Interface,
) (
	PublisherRunner,
	error,
) {

	ctbInformer := certalpha1informers.NewFilteredClusterTrustBundleInformer(kubeClient, 0, cache.Indexers{},
		func(options *metav1.ListOptions) {
			options.FieldSelector = fields.OneTermEqualSelector("spec.signerName", signerName).String()
		})

	return newClusterTrustBundlePublisher(
		signerName,
		caProvider,
		kubeClient.CertificatesV1alpha1().ClusterTrustBundles(),
		ctbInformer,
		certalphav1listers.NewClusterTrustBundleLister(ctbInformer.GetIndexer()),
		&alphaHandlers{},
	)
}

// newClusterTrustBundlePublisher 为名为 signerName 的签名者创建并维护一个集群信任包对象，
// 该对象的 .spec.TrustBundle 中包含来自 caProvider 的 CA。
func newClusterTrustBundlePublisher[T clusterTrustBundle](
	signerName string,
	caProvider dynamiccertificates.CAContentProvider,
	bundleClient clusterTrustBundlesClient[T],
	ctbInformer cache.SharedIndexInformer,
	ctbLister clusterTrustBundlesLister[T],
	handlers clusterTrustBundleHandlers[T],
) (PublisherRunner, error) {
	if len(signerName) == 0 {
		return nil, fmt.Errorf("signerName cannot be empty")
	}

	p := &ClusterTrustBundlePublisher[T]{
		signerName: signerName,
		ca:         caProvider,
		client:     bundleClient,

		ctbInformer:     ctbInformer,
		ctbLister:       ctbLister,
		ctbListerSynced: ctbInformer.HasSynced,

		handlers: handlers,

		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: "ca_cert_publisher_cluster_trust_bundles",
			},
		),
	}

	// 所有 ClusterTrustBundle 事件都映射为同一个队列键：
	// 协调过程会全量对账，无需区分具体哪个对象发生变化。
	_, err := p.ctbInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			p.queue.Add("")
		},
		UpdateFunc: func(_, _ interface{}) {
			p.queue.Add("")
		},
		DeleteFunc: func(_ interface{}) {
			p.queue.Add("")
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to register ClusterTrustBundle event handler: %w", err)
	}
	p.ca.AddListener(p.caContentChangedListener())

	return p, nil
}

// caContentChangedListener 返回一个监听器，在 CA 内容变更时入队触发协调。
func (p *ClusterTrustBundlePublisher[T]) caContentChangedListener() dynamiccertificates.Listener {
	return caContentListener(func() {
		p.queue.Add("")
	})
}

// Run 启动控制器，运行 informer 与工作协程，直到 ctx 结束。
func (p *ClusterTrustBundlePublisher[T]) Run(ctx context.Context) {
	defer utilruntime.HandleCrash()

	logger := klog.FromContext(ctx)
	logger.Info("Starting ClusterTrustBundle CA cert publisher controller")

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down ClusterTrustBundle CA cert publisher controller")
		p.queue.ShutDown()
		wg.Wait()
	}()

	wg.Go(func() {
		p.ctbInformer.RunWithContext(ctx)
	})

	// 缓存未同步前不能读取 lister，否则会基于不完整数据做出错误更新。
	if !cache.WaitForNamedCacheSyncWithContext(ctx, p.ctbListerSynced) {
		return
	}

	// 首次入队触发一次全量对账，补齐启动前发生的变化。
	p.queue.Add("")
	wg.Go(func() {
		wait.UntilWithContext(ctx, p.runWorker(), time.Second)
	})
	<-ctx.Done()
}

// runWorker 返回一个工作循环，持续处理队列中的任务直到控制器退出。
func (p *ClusterTrustBundlePublisher[T]) runWorker() func(context.Context) {
	return func(ctx context.Context) {
		for p.processNextWorkItem(ctx) {
		}
	}
}

// processNextWorkItem 处理队列中的一个键；返回 false 表示应退出工作循环。
func (p *ClusterTrustBundlePublisher[T]) processNextWorkItem(ctx context.Context) bool {
	key, quit := p.queue.Get()
	if quit {
		return false
	}
	defer p.queue.Done(key)

	if err := p.syncClusterTrustBundle(ctx); err != nil {
		utilruntime.HandleError(fmt.Errorf("syncing %q failed: %w", key, err))
		// 失败时按速率限制重试，避免错误循环压垮 API 服务器。
		p.queue.AddRateLimited(key)
		return true
	}

	// 成功后清空该键的失败计数，下次触发可立即处理。
	p.queue.Forget(key)
	return true
}

// syncClusterTrustBundle 协调集群信任包：创建或更新当前 CA 对应的对象，并清理过期对象。
func (p *ClusterTrustBundlePublisher[T]) syncClusterTrustBundle(ctx context.Context) (err error) {
	startTime := time.Now()
	defer func() {
		recordMetrics(startTime, err)
		klog.FromContext(ctx).V(4).Info("Finished syncing ClusterTrustBundle", "signerName", p.signerName, "elapsedTime", time.Since(startTime))
	}()

	caBundle := string(p.ca.CurrentCABundleContent())
	// 名称包含 CA 内容哈希：CA 轮换后生成新对象，便于新旧信任包并存过渡。
	bundleName := constructBundleName(p.signerName, []byte(caBundle))

	bundle, err := p.ctbLister.Get(bundleName)
	if apierrors.IsNotFound(err) {
		_, err = p.client.Create(ctx, p.handlers.createClusterTrustBundle(bundleName, p.signerName, caBundle), metav1.CreateOptions{})
	} else if err == nil && !p.handlers.containsTrustBundle(bundle, caBundle) {
		updatedBundle := p.handlers.updateWithTrustBundle(bundle, caBundle)
		_, err = p.client.Update(ctx, updatedBundle, metav1.UpdateOptions{})
	}

	if err != nil {
		return err
	}

	signerTrustBundles, err := p.ctbLister.List(labels.Everything())
	if err != nil {
		return err
	}

	// 保留删除错误并在最后返回，以便重新触发协调循环重试清理。
	var deletionError error
	for _, bundleObject := range signerTrustBundles {
		if p.handlers.getName(bundleObject) == bundleName {
			continue
		}

		deleteName := p.handlers.getName(bundleObject)
		if err := p.client.Delete(ctx, deleteName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
			klog.FromContext(ctx).Error(err, "failed to remove a cluster trust bundle", "bundleName", deleteName)
			deletionError = err
		}
	}

	return deletionError
}

// constructBundleName 根据签名者名称和信任包内容生成对象名：名称前缀加上内容哈希的前 12 字节。
func constructBundleName(signerName string, bundleBytes []byte) string {
	// 签名者名称中的斜杠不符合对象名规则，替换为冒号。
	namePrefix := strings.ReplaceAll(signerName, "/", ":") + ":"
	// 取哈希前缀即可区分不同 CA，同时保持名称长度可控。
	bundleHash := sha256.Sum256(bundleBytes)
	return fmt.Sprintf("%s%x", namePrefix, bundleHash[:12])

}
