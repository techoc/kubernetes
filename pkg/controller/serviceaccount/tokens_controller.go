/*
Copyright 2014 The Kubernetes Authors.

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

package serviceaccount

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	apiserverserviceaccount "k8s.io/apiserver/pkg/authentication/serviceaccount"
	informers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	clientretry "k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/serviceaccount"
)

// RemoveTokenBackoff 是删除 Secret 后从 ServiceAccount 移除其引用时
// 推荐（经验值）的重试退避参数。导出以供自定义 Secret 控制器使用。
var RemoveTokenBackoff = wait.Backoff{
	Steps:    10,
	Duration: 100 * time.Millisecond,
	Jitter:   1.0,
}

// TokensControllerOptions 包含 TokensController 的配置选项。
type TokensControllerOptions struct {
	// TokenGenerator 用于生成新 token。
	TokenGenerator serviceaccount.TokenGenerator
	// ServiceAccountResync 是完整重新 list ServiceAccount 的周期。
	// 为零时尽可能推迟重新 list。
	ServiceAccountResync time.Duration
	// SecretResync 是完整重新 list Secret 的周期。
	// 为零时尽可能推迟重新 list。
	SecretResync time.Duration
	// RootCA 会被写入 ServiceAccount 对应的 Secret 中。
	RootCA []byte

	// MaxRetries 控制在放弃前对同一 key 的最大重试次数。
	// 为零时使用默认上限。
	MaxRetries int
}

// NewTokensController 创建一个新的 *TokensController。
func NewTokensController(logger klog.Logger, serviceAccounts informers.ServiceAccountInformer, secrets informers.SecretInformer, cl clientset.Interface, options TokensControllerOptions) (*TokensController, error) {
	// 未显式配置时使用默认重试上限，避免无限重试。
	maxRetries := options.MaxRetries
	if maxRetries == 0 {
		maxRetries = 10
	}

	e := &TokensController{
		client: cl,
		token:  options.TokenGenerator,
		rootCA: options.RootCA,

		syncServiceAccountQueue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[serviceAccountQueueKey](),
			workqueue.TypedRateLimitingQueueConfig[serviceAccountQueueKey]{Name: "serviceaccount_tokens_service"},
		),
		syncSecretQueue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[secretQueueKey](),
			workqueue.TypedRateLimitingQueueConfig[secretQueueKey]{Name: "serviceaccount_tokens_service"},
		),

		maxRetries: maxRetries,
	}

	e.serviceAccounts = serviceAccounts.Lister()
	e.serviceAccountSynced = serviceAccounts.Informer().HasSynced
	serviceAccounts.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    e.queueServiceAccountSync,
			UpdateFunc: e.queueServiceAccountUpdateSync,
			DeleteFunc: e.queueServiceAccountSync,
		},
		options.ServiceAccountResync,
	)

	e.secrets = secrets.Lister()
	e.secretSynced = secrets.Informer().HasSynced
	// 只关心 ServiceAccountToken 类型的 Secret，避免无关 Secret 事件进入队列。
	secrets.Informer().AddEventHandlerWithOptions(
		cache.FilteringResourceEventHandler{
			FilterFunc: func(obj interface{}) bool {
				switch t := obj.(type) {
				case *v1.Secret:
					return t.Type == v1.SecretTypeServiceAccountToken
				default:
					utilruntime.HandleErrorWithLogger(logger, nil, "Unexpected object type passed to tokens controller", "type", fmt.Sprintf("%T", obj))
					return false
				}
			},
			Handler: cache.ResourceEventHandlerFuncs{
				AddFunc:    e.queueSecretSync,
				UpdateFunc: e.queueSecretUpdateSync,
				DeleteFunc: e.queueSecretSync,
			},
		},
		cache.HandlerOptions{
			Logger:       &logger,
			ResyncPeriod: &options.SecretResync,
		},
	)

	return e, nil
}

// TokensController 管理 ServiceAccount 对象对应的 ServiceAccountToken Secret。
type TokensController struct {
	client clientset.Interface
	token  serviceaccount.TokenGenerator

	rootCA []byte

	serviceAccounts listersv1.ServiceAccountLister
	secrets         listersv1.SecretLister

	// 该控制器关联两类对象，因此需要同时监听两者并等待各自缓存同步。
	serviceAccountSynced cache.InformerSynced
	secretSynced         cache.InformerSynced

	// syncServiceAccountQueue 处理 ServiceAccount 事件：
	//   * 确保不存在的 ServiceAccount 所对应的 token 被清除
	// key 形如 "<namespace>/<name>/<uid>"。
	syncServiceAccountQueue workqueue.TypedRateLimitingInterface[serviceAccountQueueKey]

	// syncSecretQueue 处理 Secret 事件：
	//   * 删除其 ServiceAccount 已不存在的 token
	//   * 补全缺失的 token/namespace 数据，或修正不匹配的 CA 数据
	//   * 确保被删除 token 的引用从 ServiceAccount 中移除
	// key 为 secretQueueKey{}。
	syncSecretQueue workqueue.TypedRateLimitingInterface[secretQueueKey]

	maxRetries int
}

// Run 启动控制器并阻塞，直到 ctx 被取消。
func (e *TokensController) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrashWithContext(ctx)
	logger := klog.FromContext(ctx)

	var wg sync.WaitGroup
	defer func() {
		logger.V(1).Info("Shutting down")
		// 先关闭两个队列再等待 worker 退出，防止关闭后仍有任务被处理。
		e.syncServiceAccountQueue.ShutDown()
		e.syncSecretQueue.ShutDown()
		wg.Wait()
	}()

	if !cache.WaitForNamedCacheSyncWithContext(ctx, e.serviceAccountSynced, e.secretSynced) {
		return
	}

	logger.V(5).Info("Starting workers")
	for i := 0; i < workers; i++ {
		wg.Go(func() {
			wait.UntilWithContext(ctx, e.syncServiceAccount, 0)
		})
		wg.Go(func() {
			wait.UntilWithContext(ctx, e.syncSecret, 0)
		})
	}
	<-ctx.Done()
}

// queueServiceAccountSync 将 ServiceAccount 事件（含删除事件）转换为队列 key。
// 删除事件可能是 tombstone，需要解包出原始对象。
func (e *TokensController) queueServiceAccountSync(obj interface{}) {
	if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = tombstone.Obj
	}
	if serviceAccount, ok := obj.(*v1.ServiceAccount); ok {
		e.syncServiceAccountQueue.Add(makeServiceAccountKey(serviceAccount))
		return
	}
	utilruntime.HandleError(fmt.Errorf("error decoding object, invalid type: %T", obj))
}

// queueServiceAccountUpdateSync 根据更新后的 ServiceAccount 生成队列 key。
func (e *TokensController) queueServiceAccountUpdateSync(oldObj interface{}, newObj interface{}) {
	if serviceAccount, ok := newObj.(*v1.ServiceAccount); ok {
		e.syncServiceAccountQueue.Add(makeServiceAccountKey(serviceAccount))
	}
}

// retryOrForget 根据 requeue 标记决定限速重入队还是清除重试计数，最后调用 Done。
func retryOrForget[T comparable](logger klog.Logger, queue workqueue.TypedRateLimitingInterface[T], key T, requeue bool, maxRetries int) {
	if !requeue {
		queue.Forget(key)
		return
	}

	// 超过最大重试次数后放弃，避免永久失败的对象无限占用队列。
	requeueCount := queue.NumRequeues(key)
	if requeueCount < maxRetries {
		queue.AddRateLimited(key)
		return
	}

	logger.V(4).Info("retried several times", "key", key, "count", requeueCount)
	queue.Forget(key)
}

// queueSecretSync 将 Secret 事件转换为队列 key。
func (e *TokensController) queueSecretSync(obj interface{}) {
	if secret, ok := obj.(*v1.Secret); ok {
		e.syncSecretQueue.Add(makeSecretQueueKey(secret))
	}
}

// queueSecretUpdateSync 根据更新后的 Secret 生成队列 key。
func (e *TokensController) queueSecretUpdateSync(oldObj interface{}, newObj interface{}) {
	if secret, ok := newObj.(*v1.Secret); ok {
		e.syncSecretQueue.Add(makeSecretQueueKey(secret))
	}
}

// syncServiceAccount 处理一个 ServiceAccount 队列项：
// 若对象已不存在，清理其遗留的 token；获取失败则限速重试。
func (e *TokensController) syncServiceAccount(ctx context.Context) {
	logger := klog.FromContext(ctx)
	key, quit := e.syncServiceAccountQueue.Get()
	if quit {
		return
	}
	defer e.syncServiceAccountQueue.Done(key)

	retry := false
	defer func() {
		retryOrForget(logger, e.syncServiceAccountQueue, key, retry, e.maxRetries)
	}()

	saInfo, err := parseServiceAccountKey(key)
	if err != nil {
		logger.Error(err, "Parsing service account key")
		return
	}

	sa, err := e.getServiceAccount(ctx, saInfo.namespace, saInfo.name, saInfo.uid, false)
	switch {
	case err != nil:
		logger.Error(err, "Getting service account")
		retry = true
	case sa == nil:
		// ServiceAccount 已不存在，清理其遗留 token。
		logger.V(4).Info("Service account deleted, removing tokens", "namespace", saInfo.namespace, "serviceaccount", saInfo.name)
		sa = &v1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: saInfo.namespace, Name: saInfo.name, UID: saInfo.uid}}
		retry, err = e.deleteTokens(ctx, sa)
		if err != nil {
			logger.Error(err, "Error deleting serviceaccount tokens", "namespace", saInfo.namespace, "serviceaccount", saInfo.name)
		}
	}
}

// syncSecret 处理一个 Secret 队列项，覆盖三种情况：
// Secret 已删除时清理引用；ServiceAccount 已删除时删除 token；否则补全 token 数据。
func (e *TokensController) syncSecret(ctx context.Context) {
	key, quit := e.syncSecretQueue.Get()
	if quit {
		return
	}
	defer e.syncSecretQueue.Done(key)

	logger := klog.FromContext(ctx)
	// retry 决定本次处理结束后是否限速重试。
	retry := false
	defer func() {
		retryOrForget(logger, e.syncSecretQueue, key, retry, e.maxRetries)
	}()

	secretInfo, err := parseSecretQueueKey(key)
	if err != nil {
		logger.Error(err, "Parsing secret queue key")
		return
	}

	secret, err := e.getSecret(ctx, secretInfo.namespace, secretInfo.name, secretInfo.uid)
	switch {
	case err != nil:
		logger.Error(err, "Getting secret")
		retry = true
	case secret == nil:
		// Secret 已删除：若其 ServiceAccount 仍存在，需移除对该 Secret 的引用。
		if sa, saErr := e.getServiceAccount(ctx, secretInfo.namespace, secretInfo.saName, secretInfo.saUID, false); saErr == nil && sa != nil {
			// Secret 已不存在，从 ServiceAccount 中移除对它的引用。
			if err := clientretry.RetryOnConflict(RemoveTokenBackoff, func() error {
				return e.removeSecretReference(ctx, secretInfo.namespace, secretInfo.saName, secretInfo.saUID, secretInfo.name)
			}); err != nil {
				logger.Error(err, "Removing secret reference")
			}
		}
	default:
		// Secret 仍存在，先确认其 ServiceAccount 是否存在。
		sa, saErr := e.getServiceAccount(ctx, secretInfo.namespace, secretInfo.saName, secretInfo.saUID, true)
		switch {
		case saErr != nil:
			logger.Error(saErr, "Getting service account")
			retry = true
		case sa == nil:
			// ServiceAccount 已不存在，删除该 token。
			logger.V(4).Info("Service account does not exist, deleting token", "secret", klog.KRef(secretInfo.namespace, secretInfo.name))
			if retriable, err := e.deleteToken(ctx, secretInfo.namespace, secretInfo.name, secretInfo.uid); err != nil {
				logger.Error(err, "Deleting serviceaccount token", "secret", klog.KRef(secretInfo.namespace, secretInfo.name), "serviceAccount", klog.KRef(secretInfo.namespace, secretInfo.saName))
				retry = retriable
			}
		default:
			// 按需补全 token 数据。
			if retriable, err := e.generateTokenIfNeeded(ctx, sa, secret); err != nil {
				logger.Error(err, "Populating serviceaccount token", "secret", klog.KRef(secretInfo.namespace, secretInfo.name), "serviceAccount", klog.KRef(secretInfo.namespace, secretInfo.saName))
				retry = retriable
			}
		}
	}
}

// deleteTokens 删除指定 ServiceAccount 名下的全部 token Secret，
// 汇总各次删除的错误，并在任一删除需要重试时返回 retry=true。
func (e *TokensController) deleteTokens(ctx context.Context, serviceAccount *v1.ServiceAccount) ( /*retry*/ bool, error) {
	tokens, err := e.listTokenSecrets(serviceAccount)
	if err != nil {
		// 缓存查询失败不应重试：这不是瞬时性 API 错误。
		return false, err
	}
	retry := false
	errs := []error{}
	for _, token := range tokens {
		r, err := e.deleteToken(ctx, token.Namespace, token.Name, token.UID)
		if err != nil {
			errs = append(errs, err)
		}
		if r {
			retry = true
		}
	}
	return retry, utilerrors.NewAggregate(errs)
}

// deleteToken 删除指定的 Secret。带上 UID 前置条件可避免误删同名重建的对象。
func (e *TokensController) deleteToken(ctx context.Context, ns, name string, uid types.UID) ( /*retry*/ bool, error) {
	var opts metav1.DeleteOptions
	if len(uid) > 0 {
		opts.Preconditions = &metav1.Preconditions{UID: &uid}
	}
	err := e.client.CoreV1().Secrets(ns).Delete(ctx, name, opts)
	// NotFound 表示已删除，Conflict 表示 UID 前置条件不满足（对象已被替换），均无需重试。
	if err == nil || apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		return false, nil
	}
	// 其他错误可能是瞬时故障，需要重试。
	return true, err
}

// secretUpdateNeeded 依次返回是否需要更新 CA 数据、namespace 数据和 token 数据。
func (e *TokensController) secretUpdateNeeded(secret *v1.Secret) (bool, bool, bool) {
	caData := secret.Data[v1.ServiceAccountRootCAKey]
	needsCA := len(e.rootCA) > 0 && !bytes.Equal(caData, e.rootCA)

	needsNamespace := len(secret.Data[v1.ServiceAccountNamespaceKey]) == 0

	tokenData := secret.Data[v1.ServiceAccountTokenKey]
	needsToken := len(tokenData) == 0

	return needsCA, needsNamespace, needsToken
}

// generateTokenIfNeeded 在 Secret 缺少 CA/namespace/token 数据时将其补全；已完整则不做任何改动。
func (e *TokensController) generateTokenIfNeeded(ctx context.Context, serviceAccount *v1.ServiceAccount, cachedSecret *v1.Secret) ( /* retry */ bool, error) {
	// 先用缓存副本快速判断，避免每次事件都访问 API Server。
	if needsCA, needsNamespace, needsToken := e.secretUpdateNeeded(cachedSecret); !needsCA && !needsToken && !needsNamespace {
		return false, nil
	}

	// 不能修改 informer 缓存中的对象，必须取一份实时副本再更新。
	secrets := e.client.CoreV1().Secrets(cachedSecret.Namespace)
	liveSecret, err := secrets.Get(ctx, cachedSecret.Name, metav1.GetOptions{})
	if err != nil {
		// NotFound 表示 Secret 已删除，无需重试；其他错误需重试。
		return !apierrors.IsNotFound(err), err
	}
	if liveSecret.ResourceVersion != cachedSecret.ResourceVersion {
		// 缓存视图已落后于真实对象；稍后会收到更新事件，届时再重试，避免覆盖他人修改。
		klog.FromContext(ctx).V(2).Info("Secret is not up to date, skipping token population", "secret", klog.KRef(liveSecret.Namespace, liveSecret.Name))
		return false, nil
	}

	needsCA, needsNamespace, needsToken := e.secretUpdateNeeded(liveSecret)
	if !needsCA && !needsToken && !needsNamespace {
		return false, nil
	}

	if liveSecret.Annotations == nil {
		liveSecret.Annotations = map[string]string{}
	}
	if liveSecret.Data == nil {
		liveSecret.Data = map[string][]byte{}
	}

	// 补全 CA 数据。
	if needsCA {
		liveSecret.Data[v1.ServiceAccountRootCAKey] = e.rootCA
	}
	// 补全 namespace 数据。
	if needsNamespace {
		liveSecret.Data[v1.ServiceAccountNamespaceKey] = []byte(liveSecret.Namespace)
	}

	// 生成并写入 token 数据。
	if needsToken {
		c, pc := serviceaccount.LegacyClaims(*serviceAccount, *liveSecret)
		token, err := e.token.GenerateToken(ctx, c, pc)
		if err != nil {
			return false, err
		}
		liveSecret.Data[v1.ServiceAccountTokenKey] = []byte(token)
	}

	// 记录所属 ServiceAccount 的名称与 UID，供后续归属判断使用。
	liveSecret.Annotations[v1.ServiceAccountNameKey] = serviceAccount.Name
	liveSecret.Annotations[v1.ServiceAccountUIDKey] = string(serviceAccount.UID)

	// 将补全后的 Secret 写回 API Server。
	_, err = secrets.Update(ctx, liveSecret, metav1.UpdateOptions{})
	if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
		// Conflict 表示 Secret 被他人更新，稍后会收到更新事件；
		// NotFound 表示 Secret 已不存在，无需再补全 token。两种情况都不重试。
		return false, nil
	}
	if err != nil {
		return true, err
	}
	return false, nil
}

// removeSecretReference 在需要时从 ServiceAccount 中移除对指定 Secret 的引用。
func (e *TokensController) removeSecretReference(ctx context.Context, saNamespace string, saName string, saUID types.UID, secretName string) error {
	// 不能修改 informer 缓存中的对象，取实时副本再更新。
	serviceAccounts := e.client.CoreV1().ServiceAccounts(saNamespace)
	serviceAccount, err := serviceAccounts.Get(ctx, saName, metav1.GetOptions{})
	// ServiceAccount 已不存在时无需清理引用，直接返回成功。
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}

	// UID 不匹配说明对象已被重建，旧引用不应由本次操作清除。
	if len(saUID) > 0 && saUID != serviceAccount.UID {
		return nil
	}

	// 引用已不存在，无需更新。
	if !getSecretReferences(serviceAccount).Has(secretName) {
		return nil
	}

	// 过滤掉目标 Secret 的引用后写回。
	secrets := []v1.ObjectReference{}
	for _, s := range serviceAccount.Secrets {
		if s.Name != secretName {
			secrets = append(secrets, s)
		}
	}
	serviceAccount.Secrets = secrets
	_, err = serviceAccounts.Update(ctx, serviceAccount, metav1.UpdateOptions{})
	// ServiceAccount 已不存在时无需清理引用，直接返回成功。
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

// getServiceAccount 从缓存读取 ServiceAccount；缓存缺失且 fetchOnCacheMiss 为真时回源 API Server。
// 若指定了 UID，还会校验对象身份，防止同名重建对象被误用。
func (e *TokensController) getServiceAccount(ctx context.Context, ns string, name string, uid types.UID, fetchOnCacheMiss bool) (*v1.ServiceAccount, error) {
	// 先查缓存，只有确认不存在（或 UID 不符）时才考虑回源。
	sa, err := e.serviceAccounts.ServiceAccounts(ns).Get(name)
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, err
	}
	if sa != nil {
		// 校验 UID，避免命中同名重建对象。
		if len(uid) == 0 || uid == sa.UID {
			return sa, nil
		}
	}

	if !fetchOnCacheMiss {
		return nil, nil
	}

	// 缓存未命中，回源读取实时对象。
	sa, err = e.client.CoreV1().ServiceAccounts(ns).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	// 再次校验 UID，不匹配视为对象不存在。
	if len(uid) == 0 || uid == sa.UID {
		return sa, nil
	}
	return nil, nil
}

// getSecret 从缓存读取 Secret 并校验 UID；不存在或 UID 不符均返回 nil。
func (e *TokensController) getSecret(ctx context.Context, ns string, name string, uid types.UID) (*v1.Secret, error) {
	// 查缓存，NotFound 表示 Secret 已被删除。
	secret, err := e.secrets.Secrets(ns).Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	// 校验 UID，避免命中同名重建的 Secret。
	if len(uid) == 0 || uid == secret.UID {
		return secret, nil
	}

	return nil, nil
}

// listTokenSecrets 返回所有引用指定 ServiceAccount 名称与 UID 的 ServiceAccountToken Secret。
func (e *TokensController) listTokenSecrets(serviceAccount *v1.ServiceAccount) ([]*v1.Secret, error) {
	namespaceSecrets, err := e.secrets.Secrets(serviceAccount.Namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	items := []*v1.Secret{}
	for _, secret := range namespaceSecrets {
		if apiserverserviceaccount.IsServiceAccountToken(secret, serviceAccount) {
			items = append(items, secret)
		}
	}
	return items, nil
}

// getSecretReferences 提取 ServiceAccount 引用的所有 Secret 名称集合。
func getSecretReferences(serviceAccount *v1.ServiceAccount) sets.String {
	references := sets.NewString()
	for _, secret := range serviceAccount.Secrets {
		references.Insert(secret.Name)
	}
	return references
}

// serviceAccountQueueKey 保存同步 ServiceAccount 所需的信息。
// 其中信息足以查找缓存中的对象，或在对象已不存在时删除其所属的 token。
type serviceAccountQueueKey struct {
	namespace string
	name      string
	uid       types.UID
}

// makeServiceAccountKey 从 ServiceAccount 构造队列 key，保留 UID 以识别重建对象。
func makeServiceAccountKey(sa *v1.ServiceAccount) serviceAccountQueueKey {
	return serviceAccountQueueKey{
		namespace: sa.Namespace,
		name:      sa.Name,
		uid:       sa.UID,
	}
}

// parseServiceAccountKey 解析并校验队列 key，字段缺失时返回错误。
func parseServiceAccountKey(key interface{}) (serviceAccountQueueKey, error) {
	queueKey, ok := key.(serviceAccountQueueKey)
	if !ok || len(queueKey.namespace) == 0 || len(queueKey.name) == 0 || len(queueKey.uid) == 0 {
		return serviceAccountQueueKey{}, fmt.Errorf("invalid serviceaccount key: %#v", key)
	}
	return queueKey, nil
}

// secretQueueKey 保存同步 ServiceAccountToken Secret 所需的信息。
// 其中信息足以查找缓存中的对象，或在对象已不存在时删除其引用。
type secretQueueKey struct {
	namespace string
	name      string
	uid       types.UID
	saName    string
	// 可选字段：当 token 缺少 ServiceAccount UID 注解时为空。
	saUID types.UID
}

// makeSecretQueueKey 从 Secret 的注解与元数据构造队列 key。
func makeSecretQueueKey(secret *v1.Secret) secretQueueKey {
	return secretQueueKey{
		namespace: secret.Namespace,
		name:      secret.Name,
		uid:       secret.UID,
		saName:    secret.Annotations[v1.ServiceAccountNameKey],
		saUID:     types.UID(secret.Annotations[v1.ServiceAccountUIDKey]),
	}
}

// parseSecretQueueKey 解析并校验队列 key，字段缺失时返回错误。
func parseSecretQueueKey(key interface{}) (secretQueueKey, error) {
	queueKey, ok := key.(secretQueueKey)
	if !ok || len(queueKey.namespace) == 0 || len(queueKey.name) == 0 || len(queueKey.uid) == 0 || len(queueKey.saName) == 0 {
		return secretQueueKey{}, fmt.Errorf("invalid secret key: %#v", key)
	}
	return queueKey, nil
}
