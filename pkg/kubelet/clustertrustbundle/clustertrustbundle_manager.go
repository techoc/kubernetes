/*
Copyright 2023 The Kubernetes Authors.

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

// Package clustertrustbundle 抽象对 ClusterTrustBundle 的访问，
// 以便 projected volume 能够使用它们。
package clustertrustbundle

import (
	"context"
	"encoding/pem"
	"fmt"
	"math/rand"
	"sync"
	"time"

	certificatesv1 "k8s.io/api/certificates/v1"
	certificatesv1alpha1 "k8s.io/api/certificates/v1alpha1"
	certificatesv1beta1 "k8s.io/api/certificates/v1beta1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	lrucache "k8s.io/apimachinery/pkg/util/cache"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// maxLabelSelectorLength 限制用作缓存键的标签选择器字符串长度（100 KiB），
// 避免超长选择器撑大缓存。
const (
	maxLabelSelectorLength = 100 * 1024
)

// clusterTrustBundle 是用于版本无关 ClusterTrustBundle API 的类型约束。
type clusterTrustBundle interface {
	certificatesv1alpha1.ClusterTrustBundle | certificatesv1beta1.ClusterTrustBundle | certificatesv1.ClusterTrustBundle
}

// clusterTrustBundlesLister 是与 API 版本无关的 ClusterTrustBundle lister。
type clusterTrustBundlesLister[T clusterTrustBundle] interface {
	Get(string) (*T, error)
	List(labels.Selector) ([]*T, error)
}

// clusterTrustBundleHandlers 抽象不同 API 版本 ClusterTrustBundle 的字段访问，
// 使管理器逻辑无需感知具体版本。
type clusterTrustBundleHandlers[T clusterTrustBundle] interface {
	GetName(*T) string
	GetSignerName(*T) string
	GetTrustBundle(*T) string
}

// alphaClusterTrustBundleHandlers 实现 v1alpha1 版本 ClusterTrustBundle 的字段访问。
type alphaClusterTrustBundleHandlers struct{}

// betaClusterTrustBundleHandlers 实现 v1beta1 版本 ClusterTrustBundle 的字段访问。
type betaClusterTrustBundleHandlers struct{}

// gaClusterTrustBundleHandlers 实现 v1 版本 ClusterTrustBundle 的字段访问。
type gaClusterTrustBundleHandlers struct{}

// GetName 返回 ClusterTrustBundle 的名称。
func (b *alphaClusterTrustBundleHandlers) GetName(ctb *certificatesv1alpha1.ClusterTrustBundle) string {
	return ctb.Name
}

// GetSignerName 返回 ClusterTrustBundle 的 signer 名称。
func (b *alphaClusterTrustBundleHandlers) GetSignerName(ctb *certificatesv1alpha1.ClusterTrustBundle) string {
	return ctb.Spec.SignerName
}

// GetTrustBundle 返回 ClusterTrustBundle 的信任包内容。
func (b *alphaClusterTrustBundleHandlers) GetTrustBundle(ctb *certificatesv1alpha1.ClusterTrustBundle) string {
	return ctb.Spec.TrustBundle
}

// GetName 返回 ClusterTrustBundle 的名称。
func (b *betaClusterTrustBundleHandlers) GetName(ctb *certificatesv1beta1.ClusterTrustBundle) string {
	return ctb.Name
}

// GetSignerName 返回 ClusterTrustBundle 的 signer 名称。
func (b *betaClusterTrustBundleHandlers) GetSignerName(ctb *certificatesv1beta1.ClusterTrustBundle) string {
	return ctb.Spec.SignerName
}

// GetTrustBundle 返回 ClusterTrustBundle 的信任包内容。
func (b *betaClusterTrustBundleHandlers) GetTrustBundle(ctb *certificatesv1beta1.ClusterTrustBundle) string {
	return ctb.Spec.TrustBundle
}

// GetName 返回 ClusterTrustBundle 的名称。
func (b *gaClusterTrustBundleHandlers) GetName(ctb *certificatesv1.ClusterTrustBundle) string {
	return ctb.Name
}

// GetSignerName 返回 ClusterTrustBundle 的 signer 名称。
func (b *gaClusterTrustBundleHandlers) GetSignerName(ctb *certificatesv1.ClusterTrustBundle) string {
	return ctb.Spec.SignerName
}

// GetTrustBundle 返回 ClusterTrustBundle 的信任包内容。
func (b *gaClusterTrustBundleHandlers) GetTrustBundle(ctb *certificatesv1.ClusterTrustBundle) string {
	return ctb.Spec.TrustBundle
}

// Manager 抽象获取信任锚点（trust anchor）的能力。
type Manager interface {
	// GetTrustAnchorsByName 返回指定名称 ClusterTrustBundle 中规范化并去重后的信任锚点。
	GetTrustAnchorsByName(ctx context.Context, name string, allowMissing bool) ([]byte, error)
	// GetTrustAnchorsBySigner 返回匹配 signer 与标签选择器的 ClusterTrustBundle 中
	// 规范化并去重后的信任锚点。
	GetTrustAnchorsBySigner(ctx context.Context, signerName string, labelSelector *metav1.LabelSelector, allowMissing bool) ([]byte, error)
}

// InformerManager 是“真正”的管理器实现，使用 informer 跟踪 ClusterTrustBundle 对象。
type InformerManager[T clusterTrustBundle] struct {
	// ctbInformer 监听 ClusterTrustBundle 变更并驱动缓存失效。
	ctbInformer cache.SharedIndexInformer
	// ctbLister 从 informer 缓存中读取 ClusterTrustBundle。
	ctbLister clusterTrustBundlesLister[T]

	// ctbHandlers 提供跨 API 版本的字段访问。
	ctbHandlers clusterTrustBundleHandlers[T]

	// normalizationCache 缓存已规范化、去重的 PEM 信任锚点。
	normalizationCache *lrucache.LRUExpireCache
	// cacheTTL 是缓存条目的存活时间。
	cacheTTL time.Duration
}

// 确保 InformerManager 实现了 Manager 接口。
var _ Manager = (*InformerManager[certificatesv1.ClusterTrustBundle])(nil)

// NewAlphaInformerManager 基于 v1alpha1 API 创建 ClusterTrustBundle informer 管理器。
func NewAlphaInformerManager(
	ctx context.Context, informerFactory informers.SharedInformerFactory, cacheSize int, cacheTTL time.Duration,
) (Manager, error) {
	bundlesInformer := informerFactory.Certificates().V1alpha1().ClusterTrustBundles()
	return newInformerManager(
		ctx, &alphaClusterTrustBundleHandlers{}, bundlesInformer.Informer(), bundlesInformer.Lister(), cacheSize, cacheTTL,
	)
}

// NewBetaInformerManager 基于 v1beta1 API 创建 ClusterTrustBundle informer 管理器。
func NewBetaInformerManager(
	ctx context.Context, informerFactory informers.SharedInformerFactory, cacheSize int, cacheTTL time.Duration,
) (Manager, error) {
	bundlesInformer := informerFactory.Certificates().V1beta1().ClusterTrustBundles()
	return newInformerManager(
		ctx, &betaClusterTrustBundleHandlers{}, bundlesInformer.Informer(), bundlesInformer.Lister(), cacheSize, cacheTTL,
	)
}

// NewGAInformerManager 基于 v1 API 创建 ClusterTrustBundle informer 管理器。
func NewGAInformerManager(
	ctx context.Context, informerFactory informers.SharedInformerFactory, cacheSize int, cacheTTL time.Duration,
) (Manager, error) {
	bundlesInformer := informerFactory.Certificates().V1().ClusterTrustBundles()
	return newInformerManager(
		ctx, &gaClusterTrustBundleHandlers{}, bundlesInformer.Informer(), bundlesInformer.Lister(), cacheSize, cacheTTL,
	)
}

// newInformerManager 返回初始化完成的 InformerManager。
func newInformerManager[T clusterTrustBundle](ctx context.Context, handlers clusterTrustBundleHandlers[T], informer cache.SharedIndexInformer, lister clusterTrustBundlesLister[T], cacheSize int, cacheTTL time.Duration) (Manager, error) {
	// 必须在启动 shared informer factory 之前调用 Informer()，
	// 否则该 informer 不会被注册并启动。
	m := &InformerManager[T]{
		ctbInformer:        informer,
		ctbLister:          lister,
		ctbHandlers:        handlers,
		normalizationCache: lrucache.NewLRUExpireCache(cacheSize),
		cacheTTL:           cacheTTL,
	}

	logger := klog.FromContext(ctx)
	// 让 informer 在发现可能影响缓存条目的更新时使相应缓存失效。
	_, err := m.ctbInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			ctb, ok := obj.(*T)
			if !ok {
				return
			}
			logger.Info("Dropping all cache entries for signer", "signerName", m.ctbHandlers.GetSignerName(ctb))
			m.dropCacheFor(ctb)
		},
		UpdateFunc: func(old, new any) {
			ctb, ok := new.(*T)
			if !ok {
				return
			}
			logger.Info("Dropping cache for ClusterTrustBundle", "signerName", m.ctbHandlers.GetSignerName(ctb))
			m.dropCacheFor(new.(*T))
		},
		DeleteFunc: func(obj any) {
			ctb, ok := obj.(*T)
			if !ok {
				tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
				if !ok {
					return
				}
				ctb, ok = tombstone.Obj.(*T)
				if !ok {
					return
				}
			}
			logger.Info("Dropping cache for ClusterTrustBundle", "signerName", m.ctbHandlers.GetSignerName(ctb))
			m.dropCacheFor(ctb)
		},
	})
	if err != nil {
		return nil, fmt.Errorf("while registering event handler on informer: %w", err)
	}

	return m, nil
}

// dropCacheFor 清除受指定 ClusterTrustBundle 影响的规范化缓存条目。
// 有 signer 名称时按 signer 清除，否则按对象名称清除。
func (m *InformerManager[T]) dropCacheFor(ctb *T) {
	if ctbSignerName := m.ctbHandlers.GetSignerName(ctb); ctbSignerName != "" {
		m.normalizationCache.RemoveAll(func(key any) bool {
			return key.(cacheKeyType).signerName == ctbSignerName
		})
	} else {
		m.normalizationCache.RemoveAll(func(key any) bool {
			return key.(cacheKeyType).ctbName == m.ctbHandlers.GetName(ctb)
		})
	}
}

// GetTrustAnchorsByName 返回单个指定名称 ClusterTrustBundle 中
// 规范化并去重后的信任锚点。
func (m *InformerManager[T]) GetTrustAnchorsByName(ctx context.Context, name string, allowMissing bool) ([]byte, error) {
	if !m.ctbInformer.HasSynced() {
		// informer 尚未同步时缓存可能不完整，直接报错。
		return nil, fmt.Errorf("ClusterTrustBundle informer has not yet synced")
	}

	cacheKey := cacheKeyType{ctbName: name}

	// 命中缓存时直接返回，避免重复解析 PEM。
	if cachedAnchors, ok := m.normalizationCache.Get(cacheKey); ok {
		return cachedAnchors.([]byte), nil
	}

	ctb, err := m.ctbLister.Get(name)
	if k8serrors.IsNotFound(err) && allowMissing {
		// 允许缺失时返回空结果而不是错误。
		return []byte{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("while getting ClusterTrustBundle: %w", err)
	}

	pemTrustAnchors, err := m.normalizeTrustAnchors([]*T{ctb})
	if err != nil {
		return nil, fmt.Errorf("while normalizing trust anchors: %w", err)
	}

	// 缓存规范化结果，后续请求可直接复用。
	m.normalizationCache.Add(cacheKey, pemTrustAnchors, m.cacheTTL)

	return pemTrustAnchors, nil
}

// GetTrustAnchorsBySigner 返回一组选中的 ClusterTrustBundle 中
// 规范化并去重后的信任锚点。
func (m *InformerManager[T]) GetTrustAnchorsBySigner(ctx context.Context, signerName string, labelSelector *metav1.LabelSelector, allowMissing bool) ([]byte, error) {
	if !m.ctbInformer.HasSynced() {
		return nil, fmt.Errorf("ClusterTrustBundle informer has not yet synced")
	}

	// 注意：该函数将 nil 视为“不匹配任何对象”，将非 nil 但为空视为“匹配所有对象”。
	selector, err := metav1.LabelSelectorAsSelector(labelSelector)
	if err != nil {
		return nil, fmt.Errorf("while parsing label selector: %w", err)
	}

	cacheKey := cacheKeyType{signerName: signerName, labelSelector: selector.String()}

	// 限制选择器长度，防止超长字符串撑大缓存键。
	if lsLen := len(cacheKey.labelSelector); lsLen > maxLabelSelectorLength {
		return nil, fmt.Errorf("label selector length (%d) is larger than %d", lsLen, maxLabelSelectorLength)
	}

	// 命中缓存时直接返回。
	if cachedAnchors, ok := m.normalizationCache.Get(cacheKey); ok {
		return cachedAnchors.([]byte), nil
	}

	rawCTBList, err := m.ctbLister.List(selector)
	if err != nil {
		return nil, fmt.Errorf("while listing ClusterTrustBundles matching label selector %v: %w", labelSelector, err)
	}

	ctbList := []*T{}
	for _, ctb := range rawCTBList {
		if m.ctbHandlers.GetSignerName(ctb) == signerName {
			ctbList = append(ctbList, ctb)
		}
	}

	if len(ctbList) == 0 {
		// 没有匹配项时，根据 allowMissing 决定返回空结果还是错误。
		if allowMissing {
			return []byte{}, nil
		}
		return nil, fmt.Errorf("combination of signerName and labelSelector matched zero ClusterTrustBundles")
	}

	pemTrustAnchors, err := m.normalizeTrustAnchors(ctbList)
	if err != nil {
		return nil, fmt.Errorf("while normalizing trust anchors: %w", err)
	}

	// 缓存规范化结果。
	m.normalizationCache.Add(cacheKey, pemTrustAnchors, m.cacheTTL)

	return pemTrustAnchors, nil
}

// normalizeTrustAnchors 从所有 ClusterTrustBundle 中解析 PEM 块，去重后
// 以稳定但随机打乱的顺序重新编码为 PEM。
func (m *InformerManager[T]) normalizeTrustAnchors(ctbList []*T) ([]byte, error) {
	// 对所有 ClusterTrustBundle 中的信任锚点去重。
	trustAnchorSet := sets.Set[string]{}
	for _, ctb := range ctbList {
		rest := []byte(m.ctbHandlers.GetTrustBundle(ctb))
		var b *pem.Block
		for {
			b, rest = pem.Decode(rest)
			if b == nil {
				break
			}
			trustAnchorSet = trustAnchorSet.Insert(string(b.Bytes))
		}
	}

	// 生成稳定排序，并在每次 Kubelet 重启时改变顺序，避免各节点以相同顺序加载。
	trustAnchorList := sets.List(trustAnchorSet)
	rand.Shuffle(len(trustAnchorList), func(i, j int) {
		trustAnchorList[i], trustAnchorList[j] = trustAnchorList[j], trustAnchorList[i]
	})

	pemTrustAnchors := []byte{}
	for _, ta := range trustAnchorList {
		b := &pem.Block{
			Type:  "CERTIFICATE",
			Bytes: []byte(ta),
		}
		pemTrustAnchors = append(pemTrustAnchors, pem.EncodeToMemory(b)...)
	}

	return pemTrustAnchors, nil
}

// cacheKeyType 是规范化缓存的键，按名称或按 signer 加标签选择器区分。
type cacheKeyType struct {
	ctbName       string
	signerName    string
	labelSelector string
}

// NoopManager 总是返回错误，用于 static kubelet 模式。
type NoopManager struct{}

// 确保 NoopManager 实现了 Manager 接口。
var _ Manager = (*NoopManager)(nil)

// GetTrustAnchorsByName 实现 Manager 接口，固定返回不支持错误。
func (m *NoopManager) GetTrustAnchorsByName(ctx context.Context, name string, allowMissing bool) ([]byte, error) {
	return nil, fmt.Errorf("ClusterTrustBundle projection is not supported in static kubelet mode")
}

// GetTrustAnchorsBySigner 实现 Manager 接口，固定返回不支持错误。
func (m *NoopManager) GetTrustAnchorsBySigner(ctx context.Context, signerName string, labelSelector *metav1.LabelSelector, allowMissing bool) ([]byte, error) {
	return nil, fmt.Errorf("ClusterTrustBundle projection is not supported in static kubelet mode")
}

// LazyInformerManager 在调用管理器方法时才决定使用 noop 还是真实管理器。
// 无法在启动时确定，因为某些组件依赖 kubelet 完全运行后才能搭建其 kube-apiserver。
type LazyInformerManager struct {
	// manager 是延迟初始化的实际管理器。
	manager Manager
	// managerLock 保护 manager 的初始化与读取。
	managerLock sync.RWMutex
	// client 用于探测 API 并创建 informer。
	client clientset.Interface
	// cacheSize 是规范化缓存的大小。
	cacheSize int
	// contextWithLogger 携带 logger，供 informer 生命周期使用。
	contextWithLogger context.Context
	// logger 用于记录初始化与同步日志。
	logger klog.Logger
}

// NewLazyInformerManager 创建一个延迟初始化的 ClusterTrustBundle 管理器。
func NewLazyInformerManager(ctx context.Context, kubeClient clientset.Interface, cacheSize int) Manager {
	return &LazyInformerManager{
		client:            kubeClient,
		cacheSize:         cacheSize,
		contextWithLogger: ctx,
		logger:            klog.FromContext(ctx),
		managerLock:       sync.RWMutex{},
	}
}

// GetTrustAnchorsByName 确保管理器已初始化后，按名称获取信任锚点。
func (m *LazyInformerManager) GetTrustAnchorsByName(ctx context.Context, name string, allowMissing bool) ([]byte, error) {
	if err := m.ensureManagerSet(ctx); err != nil {
		return nil, fmt.Errorf("failed to ensure informer manager for ClusterTrustBundles: %w", err)
	}
	return m.manager.GetTrustAnchorsByName(ctx, name, allowMissing)
}

// GetTrustAnchorsBySigner 确保管理器已初始化后，按 signer 获取信任锚点。
func (m *LazyInformerManager) GetTrustAnchorsBySigner(ctx context.Context, signerName string, labelSelector *metav1.LabelSelector, allowMissing bool) ([]byte, error) {
	if err := m.ensureManagerSet(ctx); err != nil {
		return nil, fmt.Errorf("failed to ensure informer manager for ClusterTrustBundles: %w", err)
	}
	return m.manager.GetTrustAnchorsBySigner(ctx, signerName, labelSelector, allowMissing)
}

// isManagerSet 在读锁保护下报告 manager 是否已完成初始化。
func (m *LazyInformerManager) isManagerSet() bool {
	m.managerLock.RLock()
	defer m.managerLock.RUnlock()
	return m.manager != nil
}

// managerConstructor 是各 API 版本 informer 管理器的构造函数类型。
type managerConstructor func(ctx context.Context, informerFactory informers.SharedInformerFactory, cacheSize int, cacheTTL time.Duration) (Manager, error)

// ensureManagerSet 按需探测集群支持的 ClusterTrustBundle API 版本，
// 并初始化对应的 informer 管理器；都不支持时退化为 NoopManager。
func (m *LazyInformerManager) ensureManagerSet(ctx context.Context) error {
	if m.isManagerSet() {
		return nil
	}

	m.managerLock.Lock()
	defer m.managerLock.Unlock()
	// 加锁后需再次检查，防止等待锁期间 manager 已被其他 goroutine 设置。
	if m.manager != nil {
		return nil
	}

	managerSchema := map[schema.GroupVersion]managerConstructor{
		certificatesv1alpha1.SchemeGroupVersion: NewAlphaInformerManager,
		certificatesv1beta1.SchemeGroupVersion:  NewBetaInformerManager,
		certificatesv1.SchemeGroupVersion:       NewGAInformerManager,
	}

	// resync 周期设为 0，仅依赖 watch 事件驱动缓存。
	kubeInformers := informers.NewSharedInformerFactoryWithOptions(m.client, 0)

	var clusterTrustBundleManager Manager
	var foundGV string
	discoveryClient := discovery.ToDiscoveryInterfaceWithContext(m.client.Discovery())
	for _, gv := range []schema.GroupVersion{
		certificatesv1.SchemeGroupVersion,
		certificatesv1beta1.SchemeGroupVersion,
		certificatesv1alpha1.SchemeGroupVersion,
	} {
		ctbAPIAvailable, err := clusterTrustBundlesAvailable(ctx, discoveryClient, gv)
		if err != nil {
			return fmt.Errorf("failed to determine which informer manager to choose: %w", err)
		}

		if !ctbAPIAvailable {
			continue
		}

		clusterTrustBundleManager, err = managerSchema[gv](m.contextWithLogger, kubeInformers, m.cacheSize, 5*time.Minute)
		if err != nil {
			return fmt.Errorf("error starting informer-based ClusterTrustBundle manager: %w", err)
		}
		foundGV = gv.String()
		break
	}

	if clusterTrustBundleManager == nil {
		m.manager = &NoopManager{}
		m.logger.Info("No version of the ClusterTrustBundle API was found, the ClusterTrustBundle informer won't be started")
		return nil
	}

	m.manager = clusterTrustBundleManager
	kubeInformers.Start(m.contextWithLogger.Done())
	m.logger.Info("Started ClusterTrustBundle informer", "apiGroup", foundGV)

	// 随后很可能立即读取缓存，因此等待刚启动的 informer 完成同步。
	synced := true
	// 设置同步超时，避免阻塞调用方过久。
	timeoutContext, cancel := context.WithTimeout(m.contextWithLogger, 10*time.Second)
	defer cancel()
	m.logger.Info("Waiting for ClusterTrustBundle informer to sync")
	for _, ok := range kubeInformers.WaitForCacheSync(timeoutContext.Done()) {
		synced = synced && ok
	}
	if synced {
		m.logger.Info("ClusterTrustBundle informer synced")
	} else {
		m.logger.Info("ClusterTrustBundle informer not synced, continuing to attempt in background")
	}

	return nil
}

// clusterTrustBundlesAvailable 通过 discovery 检查指定 GroupVersion 是否提供
// clustertrustbundles 资源。
func clusterTrustBundlesAvailable(ctx context.Context, discoveryClient discovery.DiscoveryInterfaceWithContext, gv schema.GroupVersion) (bool, error) {
	resList, err := discoveryClient.ServerResourcesForGroupVersionWithContext(ctx, gv.String())
	if k8serrors.IsNotFound(err) {
		return false, nil
	}

	if resList != nil {
		// 即使上面返回错误，对于已成功发现的部分 API 仍可能拿到部分列表。
		for _, r := range resList.APIResources {
			if r.Name == "clustertrustbundles" {
				return true, nil
			}
		}
	}
	return false, err
}
