/*
Copyright 2025 The Kubernetes Authors.

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

package pullmanager

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	kubeletconfiginternal "k8s.io/kubernetes/pkg/kubelet/apis/config"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
)

var _ ImagePullManager = &PullManager{}

// writeRecordWhileMatchingLimit 是在判断是否需要尝试拉取镜像时，停止写入
// 尚未缓存的记录的阈值。这是为了防止在命名空间高频更替时写入无界增长。
const writeRecordWhileMatchingLimit = 100

// PullManager 实现了 ImagePullManager。它通过为进行中和已成功的拉取创建记录，
// 跟踪 kubelet 拉取的镜像。
// 它记录每次成功拉取所使用的凭证，以便区分请求访问 kubelet 节点上
// 已存在镜像的不同租户。
type PullManager struct {
	recordsAccessor PullRecordsAccessor

	imagePolicyEnforcer ImagePullPolicyEnforcer

	imageService kubecontainer.ImageService

	intentAccessors *StripedLockSet // image -> sync.Mutex，为每个镜像的拉取意图串行化
	intentCounters  *sync.Map       // image -> 当前进行中拉取的次数

	pulledAccessors *StripedLockSet // imageRef -> sync.Mutex，为每个 imageRef 的记录读写串行化
}

// NewImagePullManager 创建 PullManager，使用给定的记录访问器、镜像拉取策略
// 强制器与镜像服务，并调用 initialize 恢复上次运行遗留的拉取意图。
func NewImagePullManager(ctx context.Context, recordsAccessor PullRecordsAccessor, imagePullPolicy ImagePullPolicyEnforcer, imageService kubecontainer.ImageService, lockStripesNum int32) (*PullManager, error) {
	m := &PullManager{
		recordsAccessor: recordsAccessor,

		imagePolicyEnforcer: imagePullPolicy,

		imageService: imageService,

		intentAccessors: NewStripedLockSet(lockStripesNum),
		intentCounters:  &sync.Map{},

		pulledAccessors: NewStripedLockSet(lockStripesNum),
	}

	m.initialize(ctx)

	return m, nil
}

// RecordPullIntent 记录一次针对指定镜像的拉取意图，并递增该镜像的进行中拉取计数。
func (f *PullManager) RecordPullIntent(logger klog.Logger, image string) error {
	f.intentAccessors.Lock(image)
	defer f.intentAccessors.Unlock(image)

	if err := f.recordsAccessor.WriteImagePullIntent(logger, image); err != nil {
		return fmt.Errorf("failed to record image pull intent: %w", err)
	}

	f.incrementIntentCounterForImage(image)
	return nil
}

// RecordImagePulled 记录镜像拉取成功：写入（或按需合并）ImagePulledRecord，
// 成功后递减对应镜像的拉取意图计数。
func (f *PullManager) RecordImagePulled(ctx context.Context, image, imageRef string, credentials *kubeletconfiginternal.ImagePullCredentials) {
	logger := klog.FromContext(ctx)
	if err := f.writePulledRecordIfChanged(ctx, image, imageRef, credentials); err != nil {
		logger.Error(err, "failed to write image pulled record", "imageRef", imageRef)
		return
	}

	// 注意：记录写入出错时不递减计数，会留下悬空的 imagePullIntent 以及
	// intentCounters 映射中的引用计数。
	// 这样做是为了让成功拉取的镜像仍被 kubelet 视为已拉取。
	// kubelet 重启时会再次尝试把 imagePullIntent 转换为 pulled 记录。
	f.decrementImagePullIntent(ctx, image)
}

// writePulledRecordIfChanged 把 ImagePulledRecord 写入 f.pulledDir 目录。
// `image` 是 Pod 对象中容器的镜像。
// `imageRef` 是 CRI 使用的镜像引用。
// `credentials` 是应写入新记录或合并进已有记录的一组凭证。
//
// 若 `credentials` 为 nil，表示镜像在未知情况下被拉取。此时应把镜像记录为
// 已跟踪，但不写入凭证，以便在该镜像下次被访问时强制进行凭证校验。
func (f *PullManager) writePulledRecordIfChanged(ctx context.Context, image, imageRef string, credentials *kubeletconfiginternal.ImagePullCredentials) error {
	logger := klog.FromContext(ctx)
	f.pulledAccessors.Lock(imageRef)
	defer f.pulledAccessors.Unlock(imageRef)

	sanitizedImage, err := trimImageTagDigest(image)
	if err != nil {
		return fmt.Errorf("invalid image name %q: %w", image, err)
	}

	pulledRecord, _, err := f.recordsAccessor.GetImagePulledRecord(imageRef)
	if err != nil {
		logger.Info("failed to retrieve an ImagePulledRecord", "image", image, "err", err)
		pulledRecord = nil
	}

	var pulledRecordChanged bool
	if pulledRecord == nil {
		pulledRecordChanged = true
		pulledRecord = &kubeletconfiginternal.ImagePulledRecord{
			LastUpdatedTime:   metav1.Time{Time: time.Now()},
			ImageRef:          imageRef,
			CredentialMapping: make(map[string]kubeletconfiginternal.ImagePullCredentials),
		}
		// 只要某个 imageRef 存在 pulled 记录，就足以将其视为由 kubelet 拉取。
		// 若找不到该镜像的凭证记录，kubelet 应采取安全失败策略，
		// 并在下次访问时强制进行凭证校验。
		if credentials != nil {
			pulledRecord.CredentialMapping[sanitizedImage] = *credentials
		}
	} else {
		pulledRecord, pulledRecordChanged = pulledRecordMergeNewCreds(pulledRecord, sanitizedImage, credentials)
	}

	if !pulledRecordChanged {
		return nil
	}

	return f.recordsAccessor.WriteImagePulledRecord(logger, pulledRecord)
}

// RecordImagePullFailed 记录一次拉取失败：递减该镜像的拉取意图计数。
func (f *PullManager) RecordImagePullFailed(ctx context.Context, image string) {
	f.decrementImagePullIntent(ctx, image)
}

// decrementImagePullIntent 递减指定 `image` 的拉取意图请求次数；
// 当该镜像的引用计数归零时，删除其 ImagePullIntent 文件。
func (f *PullManager) decrementImagePullIntent(ctx context.Context, image string) {
	logger := klog.FromContext(ctx)
	f.intentAccessors.Lock(image)
	defer f.intentAccessors.Unlock(image)

	if f.getIntentCounterForImage(image) <= 1 {
		if err := f.recordsAccessor.DeleteImagePullIntent(logger, image); err != nil {
			logger.Error(err, "failed to remove image pull intent", "image", image)
			return
		}
		// 只有在文件删除成功后才删除意图计数，保证与记录的状态一致
		f.intentCounters.Delete(image)
		return
	}

	f.decrementIntentCounterForImage(image)
}

// MustAttemptImagePull 判断对指定镜像是否必须尝试拉取（即是否需要进行凭证校验）。
// 返回 true 表示需要拉取/校验；凭证策略直接放行或缓存凭证匹配成功时返回 false。
func (f *PullManager) MustAttemptImagePull(ctx context.Context, image, imageRef string, getPodCredentials GetPodCredentials) (bool, error) {
	if len(imageRef) == 0 {
		return true, nil
	}
	logger := klog.FromContext(ctx)

	var resultForMetrics mustAttemptImagePullResult
	defer func() {
		if len(resultForMetrics) == 0 {
			resultForMetrics = checkResultError
		}
		recordMustAttemptImagePullResult(resultForMetrics)
	}()

	var imagePulledByKubelet bool
	var pulledRecord *kubeletconfiginternal.ImagePulledRecord

	err := func() error {
		// 在做出判断期间锁定相关文件，防止决策依据被并发修改
		f.pulledAccessors.Lock(imageRef)
		defer f.pulledAccessors.Unlock(imageRef)
		f.intentAccessors.Lock(image)
		defer f.intentAccessors.Unlock(image)

		var err error
		var exists bool
		pulledRecord, exists, err = f.recordsAccessor.GetImagePulledRecord(imageRef)
		switch {
		case err != nil:
			return err
		case exists:
			imagePulledByKubelet = true
		case pulledRecord != nil:
			imagePulledByKubelet = true
		default:
			// 优化检查：可以先看意图计数，但计数为零可能只是 kubelet 自写入
			// 意图记录后发生了重启，因此必须回退到实际缓存中确认。
			imagePulledByKubelet = f.getIntentCounterForImage(image) > 0
			if imagePulledByKubelet {
				break
			}

			if exists, err := f.recordsAccessor.ImagePullIntentExists(image); err != nil {
				return fmt.Errorf("failed to check existence of an image pull intent: %w", err)
			} else if exists {
				imagePulledByKubelet = true
			}
		}

		return nil
	}()

	if err != nil {
		resultForMetrics = checkResultError
		logger.Error(err, "Unable to access cache records about image pulls")
		return true, nil
	}

	if !f.imagePolicyEnforcer.RequireCredentialVerificationForImage(image, imagePulledByKubelet) {
		resultForMetrics = checkResultCredentialPolicyAllowed
		return false, nil
	}

	if pulledRecord == nil {
		// 过去没有该镜像被成功拉取的完整记录，这里可以直接短路处理
		resultForMetrics = checkResultMustAuthenticate
		return true, nil
	}

	sanitizedImage, err := trimImageTagDigest(image)
	if err != nil {
		resultForMetrics = checkResultError
		logger.Error(err, "failed to parse image name, forcing image credentials reverification", "image", sanitizedImage)
		return true, nil
	}

	cachedCreds, ok := pulledRecord.CredentialMapping[sanitizedImage]
	if !ok {
		resultForMetrics = checkResultMustAuthenticate
		return true, nil
	}

	if cachedCreds.NodePodsAccessible {
		// 该节点上的任何人都能访问此镜像
		resultForMetrics = checkResultCredentialRecordFound
		return false, nil
	}

	if len(cachedCreds.KubernetesSecrets) == 0 && len(cachedCreds.KubernetesServiceAccounts) == 0 {
		resultForMetrics = checkResultMustAuthenticate
		return true, nil
	}

	podSecrets, podServiceAccount, err := getPodCredentials()
	if err != nil {
		resultForMetrics = checkResultError
		return true, err
	}

	for _, podSecret := range podSecrets {
		for _, cachedSecret := range cachedCreds.KubernetesSecrets {

			// 需要检查哈希长度，以防在 keyring 中存储记录时哈希计算失败
			hashesMatch := len(cachedSecret.CredentialHash) > 0 && podSecret.CredentialHash == cachedSecret.CredentialHash
			secretCoordinatesMatch := podSecret.UID == cachedSecret.UID &&
				podSecret.Namespace == cachedSecret.Namespace &&
				podSecret.Name == cachedSecret.Name

			if hashesMatch {
				if !secretCoordinatesMatch && len(cachedCreds.KubernetesSecrets) < writeRecordWhileMatchingLimit {
					// 虽然此刻只是做匹配，但我们希望确保该 secret 在未来也被视为有效，
					// 因此额外向缓存写入一次。
					// 若携带更新哈希的 secret 已存在于缓存，writePulledRecord() 不会做任何事。
					if err := f.writePulledRecordIfChanged(ctx, image, imageRef, &kubeletconfiginternal.ImagePullCredentials{KubernetesSecrets: []kubeletconfiginternal.ImagePullSecret{podSecret}}); err != nil {
						logger.Error(err, "failed to write an image pulled record", "image", image, "imageRef", imageRef)
					}
				}
				resultForMetrics = checkResultCredentialRecordFound
				return false, nil
			}

			if secretCoordinatesMatch {
				if !hashesMatch && len(cachedCreds.KubernetesSecrets) < writeRecordWhileMatchingLimit {
					// 虽然此刻只是做匹配，但我们希望确保更新后的凭证在未来也被视为有效，
					// 因此额外向缓存写入一次。
					// 若哈希在此期间已被更新，writePulledRecord() 不会做任何事。
					if err := f.writePulledRecordIfChanged(ctx, image, imageRef, &kubeletconfiginternal.ImagePullCredentials{KubernetesSecrets: []kubeletconfiginternal.ImagePullSecret{podSecret}}); err != nil {
						logger.Error(err, "failed to write an image pulled record", "image", image, "imageRef", imageRef)
					}
					resultForMetrics = checkResultCredentialRecordFound
					return false, nil
				}
			}
		}
	}

	if podServiceAccount != nil && slices.Contains(cachedCreds.KubernetesServiceAccounts, *podServiceAccount) {
		// 找到了匹配的 service account，无需再拉取镜像
		resultForMetrics = checkResultCredentialRecordFound
		return false, nil
	}

	resultForMetrics = checkResultMustAuthenticate
	return true, nil
}

// PruneUnknownRecords 清理不再被使用的 ImagePulledRecord：删除在 until 之后
// 未更新、且不在给定镜像列表中的记录，用于镜像垃圾回收。
func (f *PullManager) PruneUnknownRecords(ctx context.Context, imageList []string, until time.Time) {
	f.pulledAccessors.GlobalLock()
	defer f.pulledAccessors.GlobalUnlock()

	logger := klog.FromContext(ctx)
	pulledRecords, err := f.recordsAccessor.ListImagePulledRecords()
	if err != nil {
		logger.Error(err, "there were errors listing ImagePulledRecords, garbage collection will proceed with incomplete records list")
	}

	imagesInUse := sets.New(imageList...)
	for _, imageRecord := range pulledRecords {
		if !imageRecord.LastUpdatedTime.Time.Before(until) {
			// 该镜像记录是在 GC 开始之后才更新的，跳过
			continue
		}

		if imagesInUse.Has(imageRecord.ImageRef) {
			continue
		}

		if err := f.recordsAccessor.DeleteImagePulledRecord(logger, imageRecord.ImageRef); err != nil {
			logger.Error(err, "failed to remove an ImagePulledRecord", "imageRef", imageRecord.ImageRef)
		}
	}

}

// initialize 收集上一次 kubelet 运行留下的所有拉取意图记录中的镜像。
// 若 CRI 报告其中某个镜像已被拉取，则把对应的拉取意图转为 pulled 记录，
// 并删除原始的拉取意图。
//
// 该方法不是线程安全的，只能在创建 PullManager 时调用。
func (f *PullManager) initialize(ctx context.Context) {
	logger := klog.FromContext(ctx)
	pullIntents, err := f.recordsAccessor.ListImagePullIntents()
	if err != nil {
		logger.Error(err, "there were errors listing ImagePullIntents, continuing with an incomplete records list")
	}

	if len(pullIntents) == 0 {
		return
	}

	imageObjs, err := f.imageService.ListImages(ctx)
	if err != nil {
		logger.Error(err, "failed to list images")
	}

	inFlightPulls := sets.New[string]()
	for _, intent := range pullIntents {
		inFlightPulls.Insert(intent.Image)
	}

	// CRI 已知的每个镜像可能对应多个 tag 与 digest，这正是 ImagePullIntent
	// 中跟踪的内容——需要为每个镜像遍历所有这些 tag 与 digest。
	for _, imageObj := range imageObjs {
		existingRecordedImages := searchForExistingTagDigest(inFlightPulls, imageObj)

		for _, image := range existingRecordedImages.UnsortedList() {

			if err := f.writePulledRecordIfChanged(ctx, image, imageObj.ID, nil); err != nil {
				logger.Error(err, "failed to write an image pull record", "imageRef", imageObj.ID)
				continue
			}

			if err := f.recordsAccessor.DeleteImagePullIntent(logger, image); err != nil {
				logger.V(2).Info("failed to remove image pull intent file", "imageName", image, "error", err)
			}
		}
	}

}

// incrementIntentCounterForImage 递增指定镜像的进行中拉取意图计数。
func (f *PullManager) incrementIntentCounterForImage(image string) {
	f.intentCounters.Store(image, f.getIntentCounterForImage(image)+1)
}

// decrementIntentCounterForImage 递减指定镜像的进行中拉取意图计数。
func (f *PullManager) decrementIntentCounterForImage(image string) {
	f.intentCounters.Store(image, f.getIntentCounterForImage(image)-1)
}

// getIntentCounterForImage 读取指定镜像的拉取意图计数，不存在时返回 0。
func (f *PullManager) getIntentCounterForImage(image string) int32 {
	intentNumAny, ok := f.intentCounters.Load(image)
	if !ok {
		return 0
	}
	intentNum, ok := intentNumAny.(int32)
	if !ok {
		panic(fmt.Sprintf("expected the intentCounters sync map to only contain int32 values, got %T", intentNumAny))
	}
	return intentNum
}

// searchForExistingTagDigest 遍历 `image` 的 RepoDigests 与 RepoTags，
// 找出所有存在于 `inFlightPulls`（containerImage -> 拉取意图路径的映射）
// 中的镜像 digest/tag。
func searchForExistingTagDigest(inFlightPulls sets.Set[string], image kubecontainer.Image) sets.Set[string] {
	existingRecordedImages := sets.New[string]()
	for _, digest := range image.RepoDigests {
		if ok := inFlightPulls.Has(digest); ok {
			existingRecordedImages.Insert(digest)
		}
	}

	for _, tag := range image.RepoTags {
		if ok := inFlightPulls.Has(tag); ok {
			existingRecordedImages.Insert(tag)
		}
	}

	return existingRecordedImages
}

// kubeSecretCoordinates 是镜像拉取 secret 的唯一坐标，用于合并与去重。
type kubeSecretCoordinates struct {
	UID       string
	Namespace string
	Name      string
}

// pulledRecordMergeNewCreds 把 `newCreds` 中的凭证合并进 `orig` 记录中
// `imageNoTagDigest` 镜像对应的条目。
// `imageNoTagDigest` 是 pod 容器 `image` 字段去除 tag 与 digest 后的内容。
//
// 注意：pulledRecordMergeNewCreds() 常在 PullManager.MustAttemptImagePull()
// 的读路径中被调用，因此应在确实需要时才进行分配（例如 DeepCopy()）。
func pulledRecordMergeNewCreds(orig *kubeletconfiginternal.ImagePulledRecord, imageNoTagDigest string, newCreds *kubeletconfiginternal.ImagePullCredentials) (*kubeletconfiginternal.ImagePulledRecord, bool) {
	if newCreds == nil {
		// 没有新的凭证信息需要记录
		return orig, false
	}

	if !newCreds.NodePodsAccessible && len(newCreds.KubernetesSecrets) == 0 && len(newCreds.KubernetesServiceAccounts) == 0 {
		// 没有任何 secret、service account 凭证或节点级访问权限需要记录
		return orig, false
	}
	selectedCreds, found := orig.CredentialMapping[imageNoTagDigest]
	if !found {
		ret := orig.DeepCopy()
		if ret.CredentialMapping == nil {
			ret.CredentialMapping = make(map[string]kubeletconfiginternal.ImagePullCredentials)
		}
		ret.CredentialMapping[imageNoTagDigest] = *newCreds
		ret.LastUpdatedTime = metav1.Time{Time: time.Now()}
		return ret, true
	}

	if selectedCreds.NodePodsAccessible {
		return orig, false
	}

	switch {
	case newCreds.NodePodsAccessible:
		selectedCreds.NodePodsAccessible = true
		selectedCreds.KubernetesSecrets = nil
		selectedCreds.KubernetesServiceAccounts = nil

		ret := orig.DeepCopy()
		ret.CredentialMapping[imageNoTagDigest] = selectedCreds
		ret.LastUpdatedTime = metav1.Time{Time: time.Now()}
		return ret, true

	case len(newCreds.KubernetesSecrets) > 0:
		var secretsChanged bool
		selectedCreds.KubernetesSecrets, secretsChanged = mergePullSecrets(selectedCreds.KubernetesSecrets, newCreds.KubernetesSecrets)
		if !secretsChanged {
			return orig, false
		}

	case len(newCreds.KubernetesServiceAccounts) > 0:
		var serviceAccountsChanged bool
		selectedCreds.KubernetesServiceAccounts, serviceAccountsChanged = mergePullServiceAccounts(selectedCreds.KubernetesServiceAccounts, newCreds.KubernetesServiceAccounts)
		if !serviceAccountsChanged {
			return orig, false
		}
	}

	ret := orig.DeepCopy()
	ret.CredentialMapping[imageNoTagDigest] = selectedCreds
	ret.LastUpdatedTime = metav1.Time{Time: time.Now()}
	return ret, true
}

// mergePullSecrets 把两片 ImagePullSecret 合并为一片，
// 并按 `Namespace, Name, UID` 键保持对象唯一。
//
// 若 `new` 切片中的某个对象与 `orig` 中的对象具有相同的
// `Namespace, Name, UID` 组合，则结果采用 `new` 中对象的 CredentialHash。
//
// 返回的切片按 Namespace、Name、UID 的顺序排序。
// 同时返回一个指示，说明输入的 secret 集合是否发生了变化。
func mergePullSecrets(orig, new []kubeletconfiginternal.ImagePullSecret) ([]kubeletconfiginternal.ImagePullSecret, bool) {
	credSet := make(map[kubeSecretCoordinates]string)
	for _, secret := range orig {
		credSet[kubeSecretCoordinates{
			UID:       secret.UID,
			Namespace: secret.Namespace,
			Name:      secret.Name,
		}] = secret.CredentialHash
	}

	changed := false
	for _, s := range new {
		key := kubeSecretCoordinates{UID: s.UID, Namespace: s.Namespace, Name: s.Name}
		if existingHash, ok := credSet[key]; !ok || existingHash != s.CredentialHash {
			changed = true
			credSet[key] = s.CredentialHash
		}
	}
	if !changed {
		return orig, false
	}

	ret := make([]kubeletconfiginternal.ImagePullSecret, 0, len(credSet))
	for coords, hash := range credSet {
		ret = append(ret, kubeletconfiginternal.ImagePullSecret{
			UID:            coords.UID,
			Namespace:      coords.Namespace,
			Name:           coords.Name,
			CredentialHash: hash,
		})
	}
	// 无需使用稳定排序版本，因为用于排序的 secret 坐标在该集合中是唯一的
	slices.SortFunc(ret, imagePullSecretLess)

	return ret, true
}

// imagePullSecretLess 是辅助函数，用于确定 ImagePullSecret 切片中的排序规则。
func imagePullSecretLess(a, b kubeletconfiginternal.ImagePullSecret) int {
	if cmp := strings.Compare(a.Namespace, b.Namespace); cmp != 0 {
		return cmp
	}

	if cmp := strings.Compare(a.Name, b.Name); cmp != 0 {
		return cmp
	}

	return strings.Compare(a.UID, b.UID)
}

// mergePullServiceAccounts 把两片 ImagePullServiceAccount 合并为一片，
// 并按 `Namespace, Name, UID` 键保持对象唯一。
// 返回的切片按 Namespace、Name、UID 的顺序排序。
// 同时返回一个指示，说明输入的 service account 集合是否发生了变化。
func mergePullServiceAccounts(orig, new []kubeletconfiginternal.ImagePullServiceAccount) ([]kubeletconfiginternal.ImagePullServiceAccount, bool) {
	credSet := sets.New[kubeletconfiginternal.ImagePullServiceAccount]()
	for _, serviceAccount := range orig {
		credSet.Insert(serviceAccount)
	}

	changed := false
	for _, s := range new {
		if !credSet.Has(s) {
			changed = true
			credSet.Insert(s)
		}
	}
	if !changed {
		return orig, false
	}

	ret := credSet.UnsortedList()
	slices.SortFunc(ret, imagePullServiceAccountLess)

	return ret, true
}

// imagePullServiceAccountLess 是辅助函数，用于确定 ImagePullServiceAccount 切片中的排序规则。
func imagePullServiceAccountLess(a, b kubeletconfiginternal.ImagePullServiceAccount) int {
	if cmp := strings.Compare(a.Namespace, b.Namespace); cmp != 0 {
		return cmp
	}

	if cmp := strings.Compare(a.Name, b.Name); cmp != 0 {
		return cmp
	}

	if cmp := strings.Compare(a.UID, b.UID); cmp != 0 {
		return cmp
	}

	return 0
}
