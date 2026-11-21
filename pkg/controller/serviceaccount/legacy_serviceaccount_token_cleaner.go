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

package serviceaccount

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	applyv1 "k8s.io/client-go/applyconfigurations/core/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/controlplane/controller/legacytokentracking"
	"k8s.io/kubernetes/pkg/serviceaccount"
	"k8s.io/utils/clock"
)

// 日期标签格式与默认同步间隔；日期只精确到天，便于比较 token 的上次使用时间。
const (
	dateFormat                 = "2006-01-02"
	DefaultCleanerSyncInterval = 24 * time.Hour
)

// TokenCleanerOptions 定义 LegacySATokenCleaner 的清理周期与同步间隔。
type LegacySATokenCleanerOptions struct {
	// CleanUpPeriod 是旧 token 自上次使用以来必须经过的时间，
	// 只有超过该时长才允许删除，避免误删仍可能被引用的凭证。
	CleanUpPeriod time.Duration
	SyncInterval  time.Duration // 连续两轮评估之间的时间间隔
}

// LegacySATokenCleaner 负责清理长时间未使用的旧版 ServiceAccount token（Secret）。
// 它先给候选 token 打上 invalidSince 标记，经过一个完整清理周期后才真正删除，
// 从而给使用者留出感知凭证失效的缓冲期。
type LegacySATokenCleaner struct {
	client           clientset.Interface
	clock            clock.Clock
	saLister         listersv1.ServiceAccountLister
	saInformerSynced cache.InformerSynced

	secretLister         listersv1.SecretLister
	secretInformerSynced cache.InformerSynced

	podLister         listersv1.PodLister
	podInformerSynced cache.InformerSynced

	syncInterval         time.Duration
	minimumSinceLastUsed time.Duration
}

// NewLegacySATokenCleaner 构造旧版 token 清理器，并校验清理周期与同步间隔必须为正数：
// 非法的周期会导致所有 token 被立即删除或永不清理，因此必须在启动时拒绝。
func NewLegacySATokenCleaner(saInformer coreinformers.ServiceAccountInformer, secretInformer coreinformers.SecretInformer, podInformer coreinformers.PodInformer, client clientset.Interface, cl clock.Clock, options LegacySATokenCleanerOptions) (*LegacySATokenCleaner, error) {
	if !(options.CleanUpPeriod > 0) {
		return nil, fmt.Errorf("invalid CleanUpPeriod: %v", options.CleanUpPeriod)
	}
	if !(options.SyncInterval > 0) {
		return nil, fmt.Errorf("invalid SyncInterval: %v", options.SyncInterval)
	}

	tc := &LegacySATokenCleaner{
		client:               client,
		clock:                cl,
		saLister:             saInformer.Lister(),
		saInformerSynced:     saInformer.Informer().HasSynced,
		secretLister:         secretInformer.Lister(),
		secretInformerSynced: secretInformer.Informer().HasSynced,
		podLister:            podInformer.Lister(),
		podInformerSynced:    podInformer.Informer().HasSynced,
		minimumSinceLastUsed: options.CleanUpPeriod,
		syncInterval:         options.SyncInterval,
	}

	return tc, nil
}

// Run 等待三个 informer 同步后，按 syncInterval 周期性调用 evaluateSATokens，
// 直到 ctx 被取消。必须先同步缓存，否则会基于不完整数据误删 token。
func (tc *LegacySATokenCleaner) Run(ctx context.Context) {
	defer utilruntime.HandleCrashWithContext(ctx)

	logger := klog.FromContext(ctx)
	logger.Info("Starting legacy service account token cleaner controller")
	defer logger.Info("Shutting down legacy service account token cleaner controller")

	if !cache.WaitForNamedCacheSyncWithContext(ctx, tc.saInformerSynced, tc.secretInformerSynced, tc.podInformerSynced) {
		return
	}

	wait.UntilWithContext(ctx, tc.evaluateSATokens, tc.syncInterval)
}

// evaluateSATokens 执行一轮清理评估：先确认 token 使用跟踪已运行足够长时间，
// 再遍历 ServiceAccount token 类型的 Secret，依次排除新建、正在删除、近期使用、
// 仍被挂载或已被 ServiceAccount 引用的对象；符合条件的先打 invalidSince 标记，
// 标记超过清理周期后才真正删除。删除时带 ResourceVersion 前置条件，防止
// 并发更新导致误删；冲突和已不存在都视为可接受的并发结果。
func (tc *LegacySATokenCleaner) evaluateSATokens(ctx context.Context) {
	logger := klog.FromContext(ctx)

	now := tc.clock.Now().UTC()
	trackedSince, err := tc.latestPossibleTrackedSinceTime(ctx)
	if err != nil {
		logger.Error(err, "Getting lastest possible tracked_since time")
		return
	}

	if now.Before(trackedSince.Add(tc.minimumSinceLastUsed)) {
		// 跟踪时间不足一个完整清理周期，此时“未使用”不等于“不再使用”，
		// 必须等待，避免把刚启用跟踪时没有 LastUsed 标签的 token 全部误删。
		return
	}

	preserveCreatedOnOrAfter := now.Add(-tc.minimumSinceLastUsed)
	preserveUsedOnOrAfter := now.Add(-tc.minimumSinceLastUsed).Format(dateFormat)

	secretList, err := tc.secretLister.Secrets(metav1.NamespaceAll).List(labels.Everything())
	if err != nil {
		logger.Error(err, "Getting cached secret list")
		return
	}

	namespaceToUsedSecretNames := make(map[string]sets.String)
	for _, secret := range secretList {
		if secret.Type != v1.SecretTypeServiceAccountToken {
			continue
		}
		if !secret.CreationTimestamp.Time.Before(preserveCreatedOnOrAfter) {
			continue
		}

		if secret.DeletionTimestamp != nil {
			continue
		}

		// 缺少 LastUsedLabelKey 意味着自跟踪开始以来该 token 从未被使用；
		// 因此只需判断存在且格式正确、日期又足够新的标签即可跳过。
		lastUsed, ok := secret.Labels[serviceaccount.LastUsedLabelKey]
		if ok {
			_, err := time.Parse(dateFormat, lastUsed)
			if err != nil {
				// 标签格式非法，无法判断最近使用时间；保守起见跳过该 Secret，
				// 不做任何删除决策。
				logger.Error(err, "Parsing lastUsed time", "secret", klog.KRef(secret.Namespace, secret.Name))
				continue
			}
			if lastUsed >= preserveUsedOnOrAfter {
				continue
			}
		}

		sa, saErr := tc.getServiceAccount(secret)

		if saErr != nil {
			logger.Error(saErr, "Getting service account", "secret", klog.KRef(secret.Namespace, secret.Name))
			continue
		}
		if sa == nil || !hasSecretReference(sa, secret.Name) {
			// 找不到对应的 ServiceAccount，或该 Secret 未被其引用，
			// 就无法确认这是自动生成的 token，因此不能清理。
			continue
		}

		mountedSecretNames, err := tc.getMountedSecretNames(secret.Namespace, namespaceToUsedSecretNames)
		if err != nil {
			logger.Error(err, "Resolving mounted secrets", "secret", klog.KRef(secret.Namespace, secret.Name))
			continue
		}
		if mountedSecretNames.Has(secret.Name) {
			// Secret 仍被 Pod 挂载，使用中，不能删除。
			continue
		}

		invalidSince := secret.Labels[serviceaccount.InvalidSinceLabelKey]
		// 未打过 invalidSince 标记或标记格式非法时，用当天日期补上标记；
		// 两阶段清理保证即使当前看起来未被使用，也先宣告失效再延迟删除。
		_, err = time.Parse(dateFormat, invalidSince)
		if err != nil {
			invalidSince = now.Format(dateFormat)
			logger.Info("Mark the auto-generated service account token as invalid", "invalidSince", invalidSince, "secret", klog.KRef(secret.Namespace, secret.Name))
			patchContent, err := json.Marshal(applyv1.Secret(secret.Name, secret.Namespace).WithUID(secret.UID).WithLabels(map[string]string{serviceaccount.InvalidSinceLabelKey: invalidSince}))
			if err != nil {
				logger.Error(err, "Failed to marshal invalid since label")
			} else {
				if _, err := tc.client.CoreV1().Secrets(secret.Namespace).Patch(ctx, secret.Name, types.MergePatchType, patchContent, metav1.PatchOptions{}); err != nil {
					logger.Error(err, "Failed to label legacy service account token secret with invalid since date")
				}
			}
			continue
		}

		if invalidSince >= preserveUsedOnOrAfter {
			continue
		}

		logger.Info("Delete auto-generated service account token", "secret", klog.KRef(secret.Namespace, secret.Name), "creationTime", secret.CreationTimestamp, "lastUsed", lastUsed, "invalidSince", invalidSince)
		if err := tc.client.CoreV1().Secrets(secret.Namespace).Delete(ctx, secret.Name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{ResourceVersion: &secret.ResourceVersion}}); err != nil && !apierrors.IsConflict(err) && !apierrors.IsNotFound(err) {
			logger.Error(err, "Deleting legacy service account token", "secret", klog.KRef(secret.Namespace, secret.Name), "serviceaccount", sa.Name)
		}
	}
}

// getMountedSecretNames 返回指定命名空间中仍被 Pod 挂载的 Secret 名称集合。
// 结果按命名空间缓存在 map 中，因为一轮评估会遍历大量 Secret，
// 重复列举同一命名空间的 Pod 会带来不必要的开销。
func (tc *LegacySATokenCleaner) getMountedSecretNames(secretNamespace string, namespaceToUsedSecretNames map[string]sets.String) (sets.String, error) {
	if secrets, ok := namespaceToUsedSecretNames[secretNamespace]; ok {
		return secrets, nil
	}

	podList, err := tc.podLister.Pods(secretNamespace).List(labels.Everything())
	if err != nil {
		return nil, fmt.Errorf("failed to get pod list from pod cache: %v", err)
	}

	var secrets sets.String
	for _, pod := range podList {
		podutil.VisitPodSecretNames(pod, func(secretName string) bool {
			if secrets == nil {
				secrets = sets.NewString()
			}
			secrets.Insert(secretName)
			return true
		})
	}
	if secrets != nil {
		namespaceToUsedSecretNames[secretNamespace] = secrets
	}
	return secrets, nil
}

// getServiceAccount 根据 Secret 上的注解找到其所属的 ServiceAccount。
// 若注解带有 UID，则必须同时匹配，防止同名 ServiceAccount 被重建后误判。
func (tc *LegacySATokenCleaner) getServiceAccount(secret *v1.Secret) (*v1.ServiceAccount, error) {
	saName := secret.Annotations[v1.ServiceAccountNameKey]
	if len(saName) == 0 {
		return nil, nil
	}
	saUID := types.UID(secret.Annotations[v1.ServiceAccountUIDKey])
	sa, err := tc.saLister.ServiceAccounts(secret.Namespace).Get(saName)
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	// 若注解提供了 UID，必须验证一致；否则同名重建的账户可能指向已失效的旧 token。
	if len(saUID) == 0 || saUID == sa.UID {
		return sa, nil
	}

	return nil, nil
}

// token 使用跟踪的开始日期，并加一天取次日零点：
// 日期标签只精确到天，次日零点才是“可以确定整天都在跟踪”的最早时刻。
// latestPossibleTrackedSinceTime 从 kube-system 命名空间的 ConfigMap 读取
// token 使用跟踪的开始日期，并加一天取次日零点。日期标签只精确到天，
// 次日零点才是可以确定整天都在跟踪的最早时刻。
func (tc *LegacySATokenCleaner) latestPossibleTrackedSinceTime(ctx context.Context) (time.Time, error) {
	configMap, err := tc.client.CoreV1().ConfigMaps(metav1.NamespaceSystem).Get(ctx, legacytokentracking.ConfigMapName, metav1.GetOptions{})
	if err != nil {
		return time.Time{}, err
	}
	trackedSince, exist := configMap.Data[legacytokentracking.ConfigMapDataKey]
	if !exist {
		return time.Time{}, fmt.Errorf("configMap does not have since label")
	}
	trackedSinceTime, err := time.Parse(dateFormat, trackedSince)
	if err != nil {
		return time.Time{}, fmt.Errorf("error parsing trackedSince time: %v", err)
	}
	// 日期只精确到天，因此向上取整到跟踪开始日的次日 00:00，
	// 避免把开始当天不完整的使用记录误当作完整的跟踪数据。
	return trackedSinceTime.AddDate(0, 0, 1), nil
}

// hasSecretReference 判断 ServiceAccount 是否引用了指定名称的 Secret，
// 用于确认待清理的 Secret 是否确实由该账户自动生成。
func hasSecretReference(serviceAccount *v1.ServiceAccount, secretName string) bool {
	for _, secret := range serviceAccount.Secrets {
		if secret.Name == secretName {
			return true
		}
	}
	return false
}
