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

package deployment

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller"
	deploymentutil "k8s.io/kubernetes/pkg/controller/deployment/util"
	"k8s.io/kubernetes/pkg/features"
	labelsutil "k8s.io/kubernetes/pkg/util/labels"
)

const (
	// replicaSetNameSeparator 是用于分隔 Deployment 名称与哈希值的字符。
	replicaSetNameSeparator = "-"
)

// syncStatusOnly 只更新 Deployment 的 Status，不执行任何改变对象的操作。
func (dc *DeploymentController) syncStatusOnly(ctx context.Context, d *apps.Deployment, rsList []*apps.ReplicaSet) error {
	newRS, oldRSs, err := dc.getAllReplicaSetsAndSyncRevision(ctx, d, rsList, false)
	if err != nil {
		return err
	}

	allRSs := append(oldRSs, newRS)
	return dc.syncDeploymentStatus(ctx, allRSs, newRS, d)
}

// sync 负责在扩缩容事件或 Deployment 处于暂停状态时对其进行协调。
func (dc *DeploymentController) sync(ctx context.Context, d *apps.Deployment, rsList []*apps.ReplicaSet) error {
	newRS, oldRSs, err := dc.getAllReplicaSetsAndSyncRevision(ctx, d, rsList, false)
	if err != nil {
		return err
	}
	if err := dc.scale(ctx, d, newRS, oldRSs); err != nil {
		// 扩缩容失败时 Deployment 会被重新入队，因此直接中止本轮同步。
		return err
	}

	// 仅在 Deployment 已暂停且没有正在进行的回滚时清理旧的 ReplicaSet。
	if d.Spec.Paused && getRollbackTo(d) == nil {
		if err := dc.cleanupDeployment(ctx, oldRSs, d); err != nil {
			return err
		}
	}

	allRSs := append(oldRSs, newRS)
	return dc.syncDeploymentStatus(ctx, allRSs, newRS, d)
}

// checkPausedConditions 检查 Deployment 是否处于暂停状态并添加相应的 Progressing 条件。
// 这些条件是必要的：否则当一个暂停时间超过 progressDeadlineSeconds 的 Deployment 恢复后，
// 我们可能误报它没有进展。
func (dc *DeploymentController) checkPausedConditions(ctx context.Context, d *apps.Deployment) error {
	if !deploymentutil.HasProgressDeadline(d) {
		return nil
	}
	cond := deploymentutil.GetDeploymentCondition(d.Status, apps.DeploymentProgressing)
	if cond != nil && cond.Reason == deploymentutil.TimedOutReason {
		// 已经报告过“无进展”时，不要用暂停条件覆盖它。
		return nil
	}
	pausedCondExists := cond != nil && cond.Reason == deploymentutil.PausedDeployReason

	needsUpdate := false
	if d.Spec.Paused && !pausedCondExists {
		condition := deploymentutil.NewDeploymentCondition(apps.DeploymentProgressing, v1.ConditionUnknown, deploymentutil.PausedDeployReason, "Deployment is paused")
		deploymentutil.SetDeploymentCondition(&d.Status, *condition)
		needsUpdate = true
	} else if !d.Spec.Paused && pausedCondExists {
		condition := deploymentutil.NewDeploymentCondition(apps.DeploymentProgressing, v1.ConditionUnknown, deploymentutil.ResumedDeployReason, "Deployment is resumed")
		deploymentutil.SetDeploymentCondition(&d.Status, *condition)
		needsUpdate = true
	}

	if !needsUpdate {
		return nil
	}

	var err error
	_, err = dc.client.AppsV1().Deployments(d.Namespace).UpdateStatus(ctx, d, metav1.UpdateOptions{})
	return err
}

// getAllReplicaSetsAndSyncRevision 返回指定 Deployment 的所有 ReplicaSet（新的和所有旧的），
// 并同步新 ReplicaSet 与 Deployment 的修订号。
//
// rsList 应来自 getReplicaSetsForDeployment(d)。
//
//  1. 获取该 Deployment 关联的所有旧 ReplicaSet，并计算其中的最大修订号（maxOldV）。
//  2. 获取 Pod 模板与 Deployment 匹配的新 ReplicaSet，当其修订号小于 maxOldV+1 时更新为 maxOldV+1。
//     若此步骤失败，将在下一轮 Deployment 同步中重试。
//  3. 把新 ReplicaSet 的修订号拷贝到 Deployment（更新 Deployment 的修订号）。失败时同样在下一轮同步重试。
//
// 注意：当前 Deployment 控制器使用缓存来避免向服务端发起读请求，
// 这可能读到过期的 ReplicaSet 数据，从而得到不正确的 Deployment 状态。
func (dc *DeploymentController) getAllReplicaSetsAndSyncRevision(ctx context.Context, d *apps.Deployment, rsList []*apps.ReplicaSet, createIfNotExisted bool) (*apps.ReplicaSet, []*apps.ReplicaSet, error) {
	_, allOldRSs := deploymentutil.FindOldReplicaSets(d, rsList)

	// 获取带有更新后修订号的新 ReplicaSet。
	newRS, err := dc.getNewReplicaSet(ctx, d, rsList, allOldRSs, createIfNotExisted)
	if err != nil {
		return nil, nil, err
	}

	return newRS, allOldRSs, nil
}

const (
	// 把修订历史长度限制在 100 个条目左右（约 2000 个字符）。
	maxRevHistoryLengthInChars = 2000
)

// getNewReplicaSet 返回与给定 Deployment 预期相符的 ReplicaSet；若新 ReplicaSet 尚不存在则返回 nil。
// 1. 查找已有的新 ReplicaSet（即 Pod 模板与 Deployment 相同的、被其指向的 RS）。
// 2. 若存在新 ReplicaSet，当其修订号小于 maxOldRevision+1 时将其更新；maxOldRevision 为所有旧 RS 中的最大修订号。
// 3. 若不存在新 ReplicaSet 且 createIfNotExisted 为 true，则创建一个新的，使用修订号 maxOldRevision+1 及相应的副本数。
// 注意：被纳管（adopted）的 RS 和 Pod 上会被添加 pod-template-hash 标签。
func (dc *DeploymentController) getNewReplicaSet(ctx context.Context, d *apps.Deployment, rsList, oldRSs []*apps.ReplicaSet, createIfNotExisted bool) (*apps.ReplicaSet, error) {
	logger := klog.FromContext(ctx)
	existingNewRS := deploymentutil.FindNewReplicaSet(d, rsList)

	// 计算所有旧 ReplicaSet 中的最大修订号。
	maxOldRevision := deploymentutil.MaxRevision(logger, oldRSs)
	// 新 ReplicaSet 的修订号取最大旧修订号加一。
	newRevision := strconv.FormatInt(maxOldRevision+1, 10)

	// 新 ReplicaSet 已存在：需要同步其注解（从父 Deployment 拷贝除 annotationsToSkip 外的全部注解，
	// 并更新 revision、desiredReplicas、maxReplicas），同时用最新修订号更新 Deployment 的注解。
	if existingNewRS != nil {
		rsCopy := existingNewRS.DeepCopy()

		// 更新现有新 ReplicaSet 的注解。
		annotationsUpdated := deploymentutil.SetNewReplicaSetAnnotations(ctx, d, rsCopy, newRevision, true, maxRevHistoryLengthInChars)
		minReadySecondsNeedsUpdate := rsCopy.Spec.MinReadySeconds != d.Spec.MinReadySeconds
		if annotationsUpdated || minReadySecondsNeedsUpdate {
			rsCopy.Spec.MinReadySeconds = d.Spec.MinReadySeconds
			return dc.client.AppsV1().ReplicaSets(rsCopy.ObjectMeta.Namespace).Update(ctx, rsCopy, metav1.UpdateOptions{})
		}

		// 应使用 existingNewRS 注解中已有的修订号，因为它早先已经设置好了。
		needsUpdate := deploymentutil.SetDeploymentRevision(d, rsCopy.Annotations[deploymentutil.RevisionAnnotation])
		// 如果没有其他 Progressing 条件记录，而 Deployment 又设置了进度截止时间，
		// 说明老用户可能开始关注发布进度，此时需要从首次发现新 ReplicaSet 的时刻开始计时。
		cond := deploymentutil.GetDeploymentCondition(d.Status, apps.DeploymentProgressing)
		if deploymentutil.HasProgressDeadline(d) && cond == nil {
			msg := fmt.Sprintf("Found new replica set %q", rsCopy.Name)
			condition := deploymentutil.NewDeploymentCondition(apps.DeploymentProgressing, v1.ConditionTrue, deploymentutil.FoundNewRSReason, msg)
			deploymentutil.SetDeploymentCondition(&d.Status, *condition)
			needsUpdate = true
		}

		if needsUpdate {
			var err error
			if _, err = dc.client.AppsV1().Deployments(d.Namespace).UpdateStatus(ctx, d, metav1.UpdateOptions{}); err != nil {
				return nil, err
			}
		}
		return rsCopy, nil
	}

	if !createIfNotExisted {
		return nil, nil
	}

	// 新 ReplicaSet 不存在，创建一个。
	newRSTemplate := *d.Spec.Template.DeepCopy()
	podTemplateSpecHash := controller.ComputeHash(&newRSTemplate, d.Status.CollisionCount)
	newRSTemplate.Labels = labelsutil.CloneAndAddLabel(d.Spec.Template.Labels, apps.DefaultDeploymentUniqueLabelKey, podTemplateSpecHash)
	// 同样把 podTemplateHash 标签加入 selector，确保只选中本 RS 的 Pod。
	newRSSelector := labelsutil.CloneSelectorAndAddLabel(d.Spec.Selector, apps.DefaultDeploymentUniqueLabelKey, podTemplateSpecHash)

	// 构造要创建的新 ReplicaSet。
	newRS := apps.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			// 名称必须可确定性生成，以保证重复同步操作是幂等的。
			Name:            generateReplicaSetName(d.Name, podTemplateSpecHash),
			Namespace:       d.Namespace,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(d, controllerKind)},
			Labels:          newRSTemplate.Labels,
		},
		Spec: apps.ReplicaSetSpec{
			Replicas:        new(int32),
			MinReadySeconds: d.Spec.MinReadySeconds,
			Selector:        newRSSelector,
			Template:        newRSTemplate,
		},
	}
	allRSs := append(oldRSs, &newRS)
	newReplicasCount, err := deploymentutil.NewRSNewReplicas(d, allRSs, &newRS)
	if err != nil {
		return nil, err
	}

	*(newRS.Spec.Replicas) = newReplicasCount
	// 设置新 ReplicaSet 的注解。
	deploymentutil.SetNewReplicaSetAnnotations(ctx, d, &newRS, newRevision, false, maxRevHistoryLengthInChars)
	// 创建新 ReplicaSet。如果同名对象已存在，需要检查是否发生了哈希碰撞；
	// 若是其他错误，则需要在 Deployment 的状态中报告。
	alreadyExists := false
	createdRS, err := dc.client.AppsV1().ReplicaSets(d.Namespace).Create(ctx, &newRS, metav1.CreateOptions{})
	switch {
	// 缓存更新滞后或 Deployment 快速重新同步时可能走到这里。
	case errors.IsAlreadyExists(err):
		alreadyExists = true

		// 取回已存在的 ReplicaSet 副本。
		rs, rsErr := dc.rsLister.ReplicaSets(newRS.Namespace).Get(newRS.Name)
		if rsErr != nil {
			return nil, rsErr
		}

		// 若该 ReplicaSet 归本 Deployment 所有，且其 PodTemplateSpec 与 Deployment 的语义上相等，
		// 就认为它就是我们要找的新 ReplicaSet；否则说明发生了哈希碰撞，
		// 需要递增 Deployment 状态中的 collisionCount，并重新入队以便下次同步再尝试创建。
		controllerRef := metav1.GetControllerOf(rs)
		if controllerRef != nil && controllerRef.UID == d.UID && deploymentutil.EqualIgnoreHash(&d.Spec.Template, &rs.Spec.Template) {
			createdRS = rs
			err = nil
			break
		}

		// 同名 ReplicaSet 的模板不一致，递增 collisionCount 并让 Deployment 重新入队。
		if d.Status.CollisionCount == nil {
			d.Status.CollisionCount = new(int32)
		}
		preCollisionCount := *d.Status.CollisionCount
		*d.Status.CollisionCount++
		// 更新 Deployment 的 collisionCount，并通过返回原始错误触发重新入队。
		_, dErr := dc.client.AppsV1().Deployments(d.Namespace).UpdateStatus(ctx, d, metav1.UpdateOptions{})
		if dErr == nil {
			logger.V(2).Info("Found a hash collision for deployment - bumping collisionCount to resolve it", "deployment", klog.KObj(d), "oldCollisionCount", preCollisionCount, "newCollisionCount", *d.Status.CollisionCount)
		}
		return nil, err
	case errors.HasStatusCause(err, v1.NamespaceTerminatingCause):
		// 命名空间正在终止时，后续所有创建都会失败，可以直接什么都不做。
		return nil, err
	case err != nil:
		msg := fmt.Sprintf("Failed to create new replica set %q: %v", newRS.Name, err)
		if deploymentutil.HasProgressDeadline(d) {
			cond := deploymentutil.NewDeploymentCondition(apps.DeploymentProgressing, v1.ConditionFalse, deploymentutil.FailedRSCreateReason, msg)
			deploymentutil.SetDeploymentCondition(&d.Status, *cond)
			// 此时已经要报告更重要的问题，这个创建错误就不必再关心了。
			// TODO: 识别哪些错误是永久性的，并让 DeploymentIsFailed 同时考虑这些原因。
			// 相关问题：https://github.com/kubernetes/kubernetes/issues/18568
			_, _ = dc.client.AppsV1().Deployments(d.Namespace).UpdateStatus(ctx, d, metav1.UpdateOptions{})
		}
		dc.eventRecorder.Eventf(d, v1.EventTypeWarning, deploymentutil.FailedRSCreateReason, "%s", msg)
		return nil, err
	}
	if !alreadyExists && newReplicasCount > 0 {
		dc.eventRecorder.Eventf(d, v1.EventTypeNormal, "ScalingReplicaSet", "Scaled up replica set %s from 0 to %d", createdRS.Name, newReplicasCount)
	}

	needsUpdate := deploymentutil.SetDeploymentRevision(d, newRevision)
	if !alreadyExists && deploymentutil.HasProgressDeadline(d) {
		msg := fmt.Sprintf("Created new replica set %q", createdRS.Name)
		condition := deploymentutil.NewDeploymentCondition(apps.DeploymentProgressing, v1.ConditionTrue, deploymentutil.NewReplicaSetReason, msg)
		deploymentutil.SetDeploymentCondition(&d.Status, *condition)
		needsUpdate = true
	}
	if needsUpdate {
		_, err = dc.client.AppsV1().Deployments(d.Namespace).UpdateStatus(ctx, d, metav1.UpdateOptions{})
	}
	return createdRS, err
}

// scale 按比例扩缩容以降低风险。否则，扩容会增大新 ReplicaSet 的规模，缩容会减小旧 ReplicaSet 的规模，
// 两者都会加快发布进度；一旦新模板有问题，就可能导致更大比例的副本不可用。
// 本方法只应在扩缩容事件或 Deployment 暂停时调用，不应在正常的发布流程中调用。
func (dc *DeploymentController) scale(ctx context.Context, deployment *apps.Deployment, newRS *apps.ReplicaSet, oldRSs []*apps.ReplicaSet) error {
	// 若只有一个活跃的 ReplicaSet，应直接将其扩到 Deployment 期望的副本数；
	// 若没有活跃的 ReplicaSet，则扩最新的那个。
	if activeOrLatest := deploymentutil.FindActiveOrLatest(newRS, oldRSs); activeOrLatest != nil {
		if *(activeOrLatest.Spec.Replicas) == *(deployment.Spec.Replicas) {
			return nil
		}
		_, _, err := dc.scaleReplicaSet(ctx, activeOrLatest, *(deployment.Spec.Replicas), deployment, false)
		return err
	}

	// 若新 ReplicaSet 已“饱和”（副本数已达期望值），应把旧 ReplicaSet 全部缩容到 0。
	// 这种情况也覆盖了新 ReplicaSet 饱和时的 ReplicaSet 纳管场景。
	if deploymentutil.IsSaturated(deployment, newRS) {
		for _, old := range controller.FilterActiveReplicaSets(oldRSs) {
			if _, _, err := dc.scaleReplicaSet(ctx, old, 0, deployment, false); err != nil {
				return err
			}
		}
		return nil
	}

	// 旧 ReplicaSet 上还有 Pod，而新 ReplicaSet 尚未饱和。
	// 在滚动发布场景下需要按比例扩缩所有 ReplicaSet（新旧都包括）。
	if deploymentutil.IsRollingUpdate(deployment) {
		allRSs := controller.FilterActiveReplicaSets(append(oldRSs, newRS))
		allRSsReplicas := deploymentutil.GetReplicaCountForReplicaSets(allRSs)

		allowedSize := int32(0)
		if *(deployment.Spec.Replicas) > 0 {
			allowedSize = *(deployment.Spec.Replicas) + deploymentutil.MaxSurge(*deployment)
		}

		// 总副本数上可以增加或移除的额外副本数，应按比例分摊给各个活跃的 ReplicaSet。
		deploymentReplicasToAdd := allowedSize - allRSsReplicas

		// 额外副本应按规模从大到小比例分摊到各活跃 ReplicaSet。
		// 当多个 ReplicaSet 规模相同时，扩缩容方向决定处理顺序：
		// 扩容时优先扩较新的 ReplicaSet，缩容时优先缩较旧的 ReplicaSet。
		switch {
		case deploymentReplicasToAdd > 0:
			sort.Sort(controller.ReplicaSetsBySizeNewer(allRSs))

		case deploymentReplicasToAdd < 0:
			sort.Sort(controller.ReplicaSetsBySizeOlder(allRSs))
		}

		// 遍历所有活跃的 ReplicaSet，估算各自应分摊的副本数。
		// deploymentReplicasAdded 的绝对值不应超过 deploymentReplicasToAdd 的绝对值。
		deploymentReplicasAdded := int32(0)
		nameToSize := make(map[string]int32)
		logger := klog.FromContext(ctx)
		for i := range allRSs {
			rs := allRSs[i]

			// 有待增减副本时才估算比例，否则把各 ReplicaSet 的当前副本数写入 nameToSize 即可。
			if deploymentReplicasToAdd != 0 {
				proportion := deploymentutil.GetReplicaSetProportion(logger, rs, *deployment, deploymentReplicasToAdd, deploymentReplicasAdded)

				nameToSize[rs.Name] = *(rs.Spec.Replicas) + proportion
				deploymentReplicasAdded += proportion
			} else {
				nameToSize[rs.Name] = *(rs.Spec.Replicas)
			}
		}

		// 逐个更新所有 ReplicaSet。
		for i := range allRSs {
			rs := allRSs[i]

			// 按比例分摊后若有余数，统一加到规模最大的 ReplicaSet 上。
			if i == 0 && deploymentReplicasToAdd != 0 {
				leftover := deploymentReplicasToAdd - deploymentReplicasAdded
				nameToSize[rs.Name] = nameToSize[rs.Name] + leftover
				if nameToSize[rs.Name] < 0 {
					nameToSize[rs.Name] = 0
				}
			}

			if _, _, err := dc.scaleReplicaSet(ctx, rs, nameToSize[rs.Name], deployment, true); err != nil {
				// 一旦失败立即返回，Deployment 会被重新入队。
				return err
			}
		}
	}
	return nil
}

// scaleReplicaSet 有两种工作模式：
//  1. 惰性更新：forceUpdate 为 false 时，仅在副本数发生变化时才实际操作；
//  2. 常规模式：只要 ReplicaSet 的注解（DesiredReplicasAnnotation 与 MaxReplicasAnnotation）
//     需要更新，或副本数发生变化，就执行更新。
func (dc *DeploymentController) scaleReplicaSet(ctx context.Context, rs *apps.ReplicaSet, newScale int32, deployment *apps.Deployment, forceUpdate bool) (bool, *apps.ReplicaSet, error) {
	// 非强制更新且副本数未变时直接跳过，避免无意义的 API 写操作。
	if !forceUpdate && *(rs.Spec.Replicas) == newScale {
		return false, rs, nil
	}

	sizeNeedsUpdate := *(rs.Spec.Replicas) != newScale
	annotationsNeedUpdate := deploymentutil.ReplicasAnnotationsNeedUpdate(rs, *(deployment.Spec.Replicas), *(deployment.Spec.Replicas)+deploymentutil.MaxSurge(*deployment))

	scaled := false
	var err error
	if sizeNeedsUpdate || annotationsNeedUpdate {
		oldScale := *(rs.Spec.Replicas)
		rsCopy := rs.DeepCopy()
		*(rsCopy.Spec.Replicas) = newScale
		deploymentutil.SetReplicasAnnotations(rsCopy, *(deployment.Spec.Replicas), *(deployment.Spec.Replicas)+deploymentutil.MaxSurge(*deployment))
		rs, err = dc.client.AppsV1().ReplicaSets(rsCopy.Namespace).Update(ctx, rsCopy, metav1.UpdateOptions{})
		if err == nil && sizeNeedsUpdate {
			var scalingOperation string
			if oldScale < newScale {
				scalingOperation = "up"
			} else {
				scalingOperation = "down"
			}
			scaled = true
			dc.eventRecorder.Eventf(deployment, v1.EventTypeNormal, "ScalingReplicaSet", "Scaled %s replica set %s from %d to %d", scalingOperation, rs.Name, oldScale, newScale)
		}
	}
	return scaled, rs, err
}

// cleanupDeployment 负责清理 Deployment：只保留最近 N 个旧 ReplicaSet，N 为 d.Spec.RevisionHistoryLimit。
// 旧 ReplicaSet 保存着 Deployment Pod 模板的历史版本，默认保留出于两个目的：
// 1) 历史追溯；2) 支持回滚。
func (dc *DeploymentController) cleanupDeployment(ctx context.Context, oldRSs []*apps.ReplicaSet, deployment *apps.Deployment) error {
	logger := klog.FromContext(ctx)
	if !deploymentutil.HasRevisionHistoryLimit(deployment) {
		return nil
	}

	// 过滤掉已进入删除流程（设置了 deletionTimestamp）的 ReplicaSet。
	aliveFilter := func(rs *apps.ReplicaSet) bool {
		return rs != nil && rs.ObjectMeta.DeletionTimestamp == nil
	}
	cleanableRSes := controller.FilterReplicaSets(oldRSs, aliveFilter)

	diff := int32(len(cleanableRSes)) - *deployment.Spec.RevisionHistoryLimit
	if diff <= 0 {
		return nil
	}

	sort.Sort(deploymentutil.ReplicaSetsByRevision(cleanableRSes))
	logger.V(4).Info("Looking to cleanup old replica sets for deployment", "deployment", klog.KObj(deployment))

	for i := int32(0); i < diff; i++ {
		rs := cleanableRSes[i]
		// 仍有副本、版本尚未被控制器观察到或正在删除的 ReplicaSet 不能清理。
		if rs.Status.Replicas != 0 || *(rs.Spec.Replicas) != 0 || rs.Generation > rs.Status.ObservedGeneration || rs.DeletionTimestamp != nil {
			continue
		}
		logger.V(4).Info("Trying to cleanup replica set for deployment", "replicaSet", klog.KObj(rs), "deployment", klog.KObj(deployment))
		if err := dc.client.AppsV1().ReplicaSets(rs.Namespace).Delete(ctx, rs.Name, metav1.DeleteOptions{}); err != nil && !errors.IsNotFound(err) {
			// 直接返回错误而不是继续删除其他对象，避免给 API server 造成过大压力。
			return err
		}
	}

	return nil
}

// syncDeploymentStatus 检查状态是否已是最新，必要时写回。
func (dc *DeploymentController) syncDeploymentStatus(ctx context.Context, allRSs []*apps.ReplicaSet, newRS *apps.ReplicaSet, d *apps.Deployment) error {
	newStatus := calculateStatus(allRSs, newRS, d)

	if reflect.DeepEqual(d.Status, newStatus) {
		return nil
	}

	newDeployment := d
	newDeployment.Status = newStatus
	_, err := dc.client.AppsV1().Deployments(newDeployment.Namespace).UpdateStatus(ctx, newDeployment, metav1.UpdateOptions{})
	return err
}

// calculateStatus 根据给定的 ReplicaSet 计算 Deployment 的最新状态。
func calculateStatus(allRSs []*apps.ReplicaSet, newRS *apps.ReplicaSet, deployment *apps.Deployment) apps.DeploymentStatus {
	availableReplicas := deploymentutil.GetAvailableReplicaCountForReplicaSets(allRSs)
	totalReplicas := deploymentutil.GetReplicaCountForReplicaSets(allRSs)
	unavailableReplicas := totalReplicas - availableReplicas
	// 不可用副本数为负说明实际可用副本多于期望值（例如正在缩容），此时应将其归零。
	if unavailableReplicas < 0 {
		unavailableReplicas = 0
	}

	status := apps.DeploymentStatus{
		// TODO: 如果开始重试状态更新，确保不会读取到新的 Generation 值。
		ObservedGeneration:  deployment.Generation,
		Replicas:            deploymentutil.GetActualReplicaCountForReplicaSets(allRSs),
		UpdatedReplicas:     deploymentutil.GetActualReplicaCountForReplicaSets([]*apps.ReplicaSet{newRS}),
		ReadyReplicas:       deploymentutil.GetReadyReplicaCountForReplicaSets(allRSs),
		AvailableReplicas:   availableReplicas,
		UnavailableReplicas: unavailableReplicas,
		CollisionCount:      deployment.Status.CollisionCount,
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.DeploymentReplicaSetTerminatingReplicas) {
		status.TerminatingReplicas = deploymentutil.GetTerminatingReplicaCountForReplicaSets(allRSs)
	}

	// 逐个拷贝条件，避免修改原对象。
	conditions := deployment.Status.Conditions
	for i := range conditions {
		status.Conditions = append(status.Conditions, conditions[i])
	}

	if availableReplicas >= *(deployment.Spec.Replicas)-deploymentutil.MaxUnavailable(*deployment) {
		minAvailability := deploymentutil.NewDeploymentCondition(apps.DeploymentAvailable, v1.ConditionTrue, deploymentutil.MinimumReplicasAvailable, "Deployment has minimum availability.")
		deploymentutil.SetDeploymentCondition(&status, *minAvailability)
	} else {
		noMinAvailability := deploymentutil.NewDeploymentCondition(apps.DeploymentAvailable, v1.ConditionFalse, deploymentutil.MinimumReplicasUnavailable, "Deployment does not have minimum availability.")
		deploymentutil.SetDeploymentCondition(&status, *noMinAvailability)
	}

	return status
}

// isScalingEvent 通过检查活跃 ReplicaSet 上的 desired-replicas 注解，
// 判断给定 Deployment 是否发生了扩缩容更新。
//
// rsList 应来自 getReplicaSetsForDeployment(d)。
func (dc *DeploymentController) isScalingEvent(ctx context.Context, d *apps.Deployment, rsList []*apps.ReplicaSet) (bool, error) {
	newRS, oldRSs, err := dc.getAllReplicaSetsAndSyncRevision(ctx, d, rsList, false)
	if err != nil {
		return false, err
	}
	allRSs := append(oldRSs, newRS)
	logger := klog.FromContext(ctx)
	for _, rs := range controller.FilterActiveReplicaSets(allRSs) {
		desired, ok := deploymentutil.GetDesiredReplicasAnnotation(logger, rs)
		if !ok {
			continue
		}
		if desired != *(d.Spec.Replicas) {
			return true, nil
		}
	}
	return false, nil
}

// generateReplicaSetName 在 Deployment 名称后追加 Pod 模板哈希生成 ReplicaSet 名称；
// 必要时会对名称进行截断，以确保结果不超过名称长度上限。
func generateReplicaSetName(deploymentName, podTemplateSpecHash string) string {
	maxDeploymentNameLength := validation.DNS1123SubdomainMaxLength - len(replicaSetNameSeparator) - len(podTemplateSpecHash)

	if len(deploymentName) > maxDeploymentNameLength && maxDeploymentNameLength > 0 {
		return deploymentName[:maxDeploymentNameLength] + replicaSetNameSeparator + podTemplateSpecHash
	}

	return deploymentName + replicaSetNameSeparator + podTemplateSpecHash
}
