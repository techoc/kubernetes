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
	"strings"
	"sync"
	"time"

	"k8s.io/klog/v2"

	"fmt"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	informers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	bootstrapapi "k8s.io/cluster-bootstrap/token/api"
	jws "k8s.io/cluster-bootstrap/token/jws"
	api "k8s.io/kubernetes/pkg/apis/core"
)

// SignerOptions 指定 Signer 用于定位 ConfigMap 与 token Secret 的命名空间、名称及 resync 周期。
type SignerOptions struct {
	// ConfigMapNamespace 是待签名 ConfigMap 所在的命名空间。
	ConfigMapNamespace string

	// ConfigMapName 是待签名 ConfigMap 的名称。
	ConfigMapName string

	// TokenSecretNamespace 是 bootstrap token Secret 所在的命名空间。
	TokenSecretNamespace string

	// ConfigMapResync 是 ConfigMap 完整重新 list 的周期；
	// 设为零时尽量推迟重新 list，只在事件触发时才处理。
	ConfigMapResync time.Duration

	// SecretResync 是 Secret 完整重新 list 的周期；
	// 设为零时尽量推迟重新 list，只在事件触发时才处理。
	SecretResync time.Duration
}

// DefaultSignerOptions 返回创建 Signer 时使用的默认选项：
// 公共命名空间中的 cluster-info ConfigMap，以及 kube-system 中的 token Secret。
func DefaultSignerOptions() SignerOptions {
	return SignerOptions{
		ConfigMapNamespace:   api.NamespacePublic,
		ConfigMapName:        bootstrapapi.ConfigMapClusterInfo,
		TokenSecretNamespace: api.NamespaceSystem,
	}
}

// Signer 负责用一组 bootstrap token 对 cluster-info ConfigMap 签名，
// 使新加入的节点可通过 token ID 校验集群信息。
type Signer struct {
	client             clientset.Interface
	configMapKey       string
	configMapName      string
	configMapNamespace string
	secretNamespace    string

	// syncQueue 串行化并合并对 ConfigMap 的更新请求。
	// 队列里始终只有一个键（目标 ConfigMap），因为更新既可能来自 ConfigMap 事件，
	// 也可能来自 Secret 事件；合并处理可以避免并发写入相互覆盖，
	// 也能防止一次事件触发的无意义重复更新。
	syncQueue workqueue.TypedRateLimitingInterface[string]

	secretLister corelisters.SecretLister
	secretSynced cache.InformerSynced

	configMapLister corelisters.ConfigMapLister
	configMapSynced cache.InformerSynced
}

// NewSigner 构造 Signer，并注册 ConfigMap 与 Secret 的过滤事件处理器：
// 只有目标 ConfigMap 或 bootstrap token Secret 的变化才会入队。
func NewSigner(cl clientset.Interface, secrets informers.SecretInformer, configMaps informers.ConfigMapInformer, options SignerOptions) (*Signer, error) {
	e := &Signer{
		client:             cl,
		configMapKey:       options.ConfigMapNamespace + "/" + options.ConfigMapName,
		configMapName:      options.ConfigMapName,
		configMapNamespace: options.ConfigMapNamespace,
		secretNamespace:    options.TokenSecretNamespace,
		secretLister:       secrets.Lister(),
		secretSynced:       secrets.Informer().HasSynced,
		configMapLister:    configMaps.Lister(),
		configMapSynced:    configMaps.Informer().HasSynced,
		syncQueue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: "bootstrap_signer_queue",
			},
		),
	}

	// ConfigMap 事件只关注指定的那一个 ConfigMap；添加和更新都重新签名，
	// 以保证 token 轮转后签名总能被刷新。
	configMaps.Informer().AddEventHandlerWithResyncPeriod(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch t := obj.(type) {
				case *v1.ConfigMap:
					return t.Name == options.ConfigMapName && t.Namespace == options.ConfigMapNamespace
				default:
					utilruntime.HandleError(fmt.Errorf("object passed to %T that is not expected: %T", e, obj))
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    func(_ interface{}) { e.pokeConfigMapSync() },
				UpdateFunc: func(_, _ interface{}) { e.pokeConfigMapSync() },
			},
		},
		options.ConfigMapResync,
	)

	// Secret 事件只关注 bootstrap token 类型的 Secret。
	// 删除事件同样要触发重签：token 消失后，ConfigMap 中对应的旧签名必须被清除，
	// 否则客户端仍可能用已失效的 token 校验出有效签名。
	secrets.Informer().AddEventHandlerWithResyncPeriod(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch t := obj.(type) {
				case *v1.Secret:
					return t.Type == bootstrapapi.SecretTypeBootstrapToken && t.Namespace == e.secretNamespace
				default:
					utilruntime.HandleError(fmt.Errorf("object passed to %T that is not expected: %T", e, obj))
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    func(_ interface{}) { e.pokeConfigMapSync() },
				UpdateFunc: func(_, _ interface{}) { e.pokeConfigMapSync() },
				DeleteFunc: func(_ interface{}) { e.pokeConfigMapSync() },
			},
		},
		options.SecretResync,
	)

	return e, nil
}

// Run 等待缓存同步后启动工作循环，并在 ctx 结束时优雅停止工作队列与 worker。
func (e *Signer) Run(ctx context.Context) {
	defer utilruntime.HandleCrash()

	logger := klog.FromContext(ctx)
	logger.V(5).Info("Starting")

	var wg sync.WaitGroup
	defer func() {
		logger.V(1).Info("Shutting down")
		e.syncQueue.ShutDown()
		wg.Wait()
	}()

	// lister 读取的是本地缓存；缓存未同步时可能看不到目标 ConfigMap 或 token，
	// 进而计算出不完整的签名并写入集群，所以必须先等待同步完成。
	if !cache.WaitForNamedCacheSyncWithContext(ctx, e.configMapSynced, e.secretSynced) {
		return
	}

	// 只启动一个 worker，保证每次只处理一个 ConfigMap 更新，避免并发写冲突。
	wg.Go(func() {
		wait.UntilWithContext(ctx, e.serviceConfigMapQueue, 0)
	})
	<-ctx.Done()
}

// pokeConfigMapSync 把目标 ConfigMap 放入工作队列，触发一次重新签名。
func (e *Signer) pokeConfigMapSync() {
	e.syncQueue.Add(e.configMapKey)
}

// serviceConfigMapQueue 从队列取出一个键并驱动一次签名计算；
// 队列关闭（quit）时直接退出，使 worker 能响应 ctx 取消。
func (e *Signer) serviceConfigMapQueue(ctx context.Context) {
	key, quit := e.syncQueue.Get()
	if quit {
		return
	}
	// 取出键后必须配对调用 Done，否则队列会一直认为该项仍在处理中。
	defer e.syncQueue.Done(key)

	e.signConfigMap(ctx)
}

// signConfigMap 基于 lister 中的最新缓存重新计算签名，
// 仅当签名集合发生变化时才回写 ConfigMap。
func (e *Signer) signConfigMap(ctx context.Context) {
	origCM := e.getConfigMap()

	if origCM == nil {
		return
	}

	var needUpdate = false

	newCM := origCM.DeepCopy()

	logger := klog.FromContext(ctx)

	// 先取出被签名的内容：kubeconfig 变动意味着所有签名都需要重算。
	content, ok := newCM.Data[bootstrapapi.KubeConfigKey]
	if !ok {
		logger.V(3).Info("No key in ConfigMap", "key", bootstrapapi.KubeConfigKey, "configMap", klog.KObj(origCM))
		return
	}

	// 暂存已有签名并从数据中删除，之后按 token 重新生成，
	// 这样既能识别新增/变化的签名，也能发现已失效 token 的残留签名。
	sigs := map[string]string{}
	for key, value := range newCM.Data {
		if strings.HasPrefix(key, bootstrapapi.JWSSignatureKeyPrefix) {
			tokenID := strings.TrimPrefix(key, bootstrapapi.JWSSignatureKeyPrefix)
			sigs[tokenID] = value
			delete(newCM.Data, key)
		}
	}

	// 按当前有效 token 重新计算 detached JWS 签名并写回新副本。
	tokens := e.getTokens(ctx)
	for tokenID, tokenValue := range tokens {
		sig, err := jws.ComputeDetachedSignature(content, tokenID, tokenValue)
		if err != nil {
			utilruntime.HandleError(err)
		}

		// 与旧签名比较，只在内容变化时才标记为需要更新。
		oldSig, _ := sigs[tokenID]
		if sig != oldSig {
			needUpdate = true
		}
		delete(sigs, tokenID)

		newCM.Data[bootstrapapi.JWSSignatureKeyPrefix+tokenID] = sig
	}

	// 处理完后仍有剩余签名，说明对应 token 已被删除，
	// 必须从 ConfigMap 中清除这些签名，因此需要触发一次更新。
	if len(sigs) != 0 {
		needUpdate = true
	}

	// 没有变化就不写回，避免无意义的 Update 事件再次入队形成循环。
	if needUpdate {
		e.updateConfigMap(ctx, newCM)
	}
}

// updateConfigMap 将新签名写回 API Server。
func (e *Signer) updateConfigMap(ctx context.Context, cm *v1.ConfigMap) {
	_, err := e.client.CoreV1().ConfigMaps(cm.Namespace).Update(ctx, cm, metav1.UpdateOptions{})
	if err != nil && !apierrors.IsConflict(err) && !apierrors.IsNotFound(err) {
		// 冲突或对象已删除都无需特殊处理：后续事件或 resync 会基于最新状态重算。
		klog.FromContext(ctx).V(3).Info("Error updating ConfigMap", "err", err)
	}
}

// getConfigMap 从 lister 中取出本控制器负责的 cluster-info ConfigMap。
func (e *Signer) getConfigMap() *v1.ConfigMap {
	configMap, err := e.configMapLister.ConfigMaps(e.configMapNamespace).Get(e.configMapName)

	// 读取失败时直接返回 nil；resync 最终会重新同步，
	// 无需在这里重试，因为缓存暂态错误通常很快自愈。
	if err != nil {
		if !apierrors.IsNotFound(err) {
			utilruntime.HandleError(err)
		}
		return nil
	}

	return configMap
}

// listSecrets 列出目标命名空间中的所有 Secret，并过滤出 bootstrap token 类型。
func (e *Signer) listSecrets() []*v1.Secret {
	secrets, err := e.secretLister.Secrets(e.secretNamespace).List(labels.Everything())
	if err != nil {
		utilruntime.HandleError(err)
		return nil
	}

	items := []*v1.Secret{}
	for _, secret := range secrets {
		// lister 返回命名空间下全部类型的 Secret，这里再按类型过滤一次。
		if secret.Type == bootstrapapi.SecretTypeBootstrapToken {
			items = append(items, secret)
		}
	}
	return items
}

// getTokens 返回 tokenID 到 tokenSecret 的映射，供签名使用；
// 它只纳入通过有效性校验的 token，确保不会用过期或格式错误的 Secret 签名。
func (e *Signer) getTokens(ctx context.Context) map[string]string {
	ret := map[string]string{}
	secretObjs := e.listSecrets()
	for _, secret := range secretObjs {
		tokenID, tokenSecret, ok := validateSecretForSigning(ctx, secret)
		if !ok {
			// 无效 Secret 不参与签名，否则会生成无法被客户端接受的签名。
			continue
		}

		// 同一 tokenID 出现多个 Secret 时行为未定义，记录告警并忽略重复项。
		if _, ok := ret[tokenID]; ok {
			// 正常情况下 Secret 名称与 tokenID 一一对应，不会出现重复；
			// 保留这层保护以防命名约定被破坏。
			klog.FromContext(ctx).V(1).Info("Duplicate bootstrap tokens found for id, ignoring on the duplicate secret", "tokenID", tokenID, "ignoredSecret", klog.KObj(secret))
			continue
		}

		// 校验通过的 token 才可加入签名集合。
		ret[tokenID] = tokenSecret
	}

	return ret
}
