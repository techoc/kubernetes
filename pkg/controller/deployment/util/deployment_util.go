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

package util

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	intstrutil "k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	appsclient "k8s.io/client-go/kubernetes/typed/apps/v1"
	appslisters "k8s.io/client-go/listers/apps/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller"
	labelsutil "k8s.io/kubernetes/pkg/util/labels"
	"k8s.io/utils/integer"
)

const (
	// RevisionAnnotation 是 Deployment 的 ReplicaSet 上的修订注解，记录其发布顺序。
	RevisionAnnotation = "deployment.kubernetes.io/revision"
	// RevisionHistoryAnnotation 维护某个 ReplicaSet 为 Deployment 服务过的所有历史修订号。
	RevisionHistoryAnnotation = "deployment.kubernetes.io/revision-history"
	// DesiredReplicasAnnotation 是记录在 ReplicaSet 注解中的 Deployment 期望副本数。
	// 它有助于把扩缩容事件与发布过程区分开，并判断 Deployment 的新 ReplicaSet 是否真正饱和。
	DesiredReplicasAnnotation = "deployment.kubernetes.io/desired-replicas"
	// MaxReplicasAnnotation 是 Deployment 在某一时刻允许拥有的最大副本数，即
	// deployment.spec.replicas + maxSurge。底层 ReplicaSet 据此在有突发副本时估算各自的比例。
	MaxReplicasAnnotation = "deployment.kubernetes.io/max-replicas"

	// RollbackRevisionNotFound 是“未找到回滚版本”的事件原因。
	RollbackRevisionNotFound = "DeploymentRollbackRevisionNotFound"
	// RollbackTemplateUnchanged 是“模板未改变”的回滚事件原因。
	RollbackTemplateUnchanged = "DeploymentRollbackTemplateUnchanged"
	// RollbackDone 是回滚完成的事件原因。
	RollbackDone = "DeploymentRollback"

	// Deployment 条件的各种原因
	//
	// Progressing：

	// ReplicaSetUpdatedReason 在发布过程中某个 ReplicaSet 被更新时添加到 Deployment 上。
	ReplicaSetUpdatedReason = "ReplicaSetUpdated"
	// FailedRSCreateReason 在无法创建新 ReplicaSet 时添加到 Deployment 上。
	FailedRSCreateReason = "ReplicaSetCreateError"
	// NewReplicaSetReason 在创建新 ReplicaSet 时添加到 Deployment 上。
	NewReplicaSetReason = "NewReplicaSetCreated"
	// FoundNewRSReason 在纳管已有 ReplicaSet 时添加到 Deployment 上。
	FoundNewRSReason = "FoundNewReplicaSet"
	// NewRSAvailableReason 在最新 ReplicaSet 变为可用时添加到 Deployment 上，
	// 即通过就绪检查且运行至少 minReadySeconds 的新 Pod 数量已达到 Deployment 所需的最小可用副本数。
	NewRSAvailableReason = "NewReplicaSetAvailable"
	// TimedOutReason 在最新 ReplicaSet 未能在规定期限（progressDeadlineSeconds）内展现任何进展时添加。
	TimedOutReason = "ProgressDeadlineExceeded"
	// PausedDeployReason 在 Deployment 暂停时添加。一旦暂停，就不应再评估其是否缺乏进展。
	PausedDeployReason = "DeploymentPaused"
	// ResumedDeployReason 在 Deployment 恢复时添加，可避免在发布中途暂停且受截止时间约束的
	// Deployment 被误判为失败。
	ResumedDeployReason = "DeploymentResumed"
	//
	// Available：

	// MinimumReplicasAvailable 在 Deployment 达到最小所需可用副本数时添加。
	MinimumReplicasAvailable = "MinimumReplicasAvailable"
	// MinimumReplicasUnavailable 在 Deployment 未达到最小所需可用副本数时添加。
	MinimumReplicasUnavailable = "MinimumReplicasUnavailable"
)

// NewDeploymentCondition 创建一个新的 Deployment 条件。
func NewDeploymentCondition(condType apps.DeploymentConditionType, status v1.ConditionStatus, reason, message string) *apps.DeploymentCondition {
	return &apps.DeploymentCondition{
		Type:               condType,
		Status:             status,
		LastUpdateTime:     metav1.Now(),
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	}
}

// GetDeploymentCondition 返回指定类型的条件。
func GetDeploymentCondition(status apps.DeploymentStatus, condType apps.DeploymentConditionType) *apps.DeploymentCondition {
	for i := range status.Conditions {
		c := status.Conditions[i]
		if c.Type == condType {
			return &c
		}
	}
	return nil
}

// SetDeploymentCondition 更新 Deployment 状态以包含给定条件。
// 如果要添加的条件已存在且状态与原因相同，则不做更新。
func SetDeploymentCondition(status *apps.DeploymentStatus, condition apps.DeploymentCondition) {
	currentCond := GetDeploymentCondition(*status, condition.Type)
	if currentCond != nil && currentCond.Status == condition.Status && currentCond.Reason == condition.Reason {
		return
	}
	// 条件状态未变时保留原有的 LastTransitionTime，避免误报状态翻转时间。
	if currentCond != nil && currentCond.Status == condition.Status {
		condition.LastTransitionTime = currentCond.LastTransitionTime
	}
	newConditions := filterOutCondition(status.Conditions, condition.Type)
	status.Conditions = append(newConditions, condition)
}

// RemoveDeploymentCondition 移除指定类型的 Deployment 条件。
func RemoveDeploymentCondition(status *apps.DeploymentStatus, condType apps.DeploymentConditionType) {
	status.Conditions = filterOutCondition(status.Conditions, condType)
}

// filterOutCondition 返回过滤掉指定类型条件后的新切片。
func filterOutCondition(conditions []apps.DeploymentCondition, condType apps.DeploymentConditionType) []apps.DeploymentCondition {
	var newConditions []apps.DeploymentCondition
	for _, c := range conditions {
		if c.Type == condType {
			continue
		}
		newConditions = append(newConditions, c)
	}
	return newConditions
}

// ReplicaSetToDeploymentCondition 把 ReplicaSet 条件转换为 Deployment 条件。
// 用于将 ReplicaSet 的失败条件“提升”到 Deployment 层面。
func ReplicaSetToDeploymentCondition(cond apps.ReplicaSetCondition) apps.DeploymentCondition {
	return apps.DeploymentCondition{
		Type:               apps.DeploymentConditionType(cond.Type),
		Status:             cond.Status,
		LastTransitionTime: cond.LastTransitionTime,
		LastUpdateTime:     cond.LastTransitionTime,
		Reason:             cond.Reason,
		Message:            cond.Message,
	}
}

// SetDeploymentRevision 更新 Deployment 的修订号注解，返回是否发生了变更。
func SetDeploymentRevision(deployment *apps.Deployment, revision string) bool {
	updated := false

	if deployment.Annotations == nil {
		deployment.Annotations = make(map[string]string)
	}
	if deployment.Annotations[RevisionAnnotation] != revision {
		deployment.Annotations[RevisionAnnotation] = revision
		updated = true
	}

	return updated
}

// MaxRevision 找出所有 ReplicaSet 中的最大修订号。
func MaxRevision(logger klog.Logger, allRSs []*apps.ReplicaSet) int64 {
	max := int64(0)
	for _, rs := range allRSs {
		if v, err := Revision(rs); err != nil {
			// 修订号解析失败的 ReplicaSet 直接跳过，不参与最大值的计算。
			logger.V(4).Info("Couldn't parse revision for replica set, deployment controller will skip it when reconciling revisions", "replicaSet", klog.KObj(rs), "err", err)
		} else if v > max {
			max = v
		}
	}
	return max
}

// LastRevision 找出所有 ReplicaSet 中第二大的修订号（即上一次发布的修订号）。
func LastRevision(logger klog.Logger, allRSs []*apps.ReplicaSet) int64 {
	max, secMax := int64(0), int64(0)
	for _, rs := range allRSs {
		if v, err := Revision(rs); err != nil {
			// 修订号解析失败的 ReplicaSet 直接跳过，不参与计算。
			logger.V(4).Info("Couldn't parse revision for replica set, deployment controller will skip it when reconciling revisions", "replicaSet", klog.KObj(rs), "err", err)
		} else if v >= max {
			secMax = max
			max = v
		} else if v > secMax {
			secMax = v
		}
	}
	return secMax
}

// Revision 从给定对象的注解中读取并解析修订号。
func Revision(obj runtime.Object) (int64, error) {
	acc, err := meta.Accessor(obj)
	if err != nil {
		return 0, err
	}
	v, ok := acc.GetAnnotations()[RevisionAnnotation]
	if !ok {
		return 0, nil
	}
	return strconv.ParseInt(v, 10, 64)
}

// SetNewReplicaSetAnnotations 通过更新修订号并把必要的 Deployment 注解拷贝过来，
// 正确设置新 ReplicaSet 的注解；当 ReplicaSet 的注解发生变化时返回 true。
func SetNewReplicaSetAnnotations(ctx context.Context, deployment *apps.Deployment, newRS *apps.ReplicaSet, newRevision string, exists bool, revHistoryLimitInChars int) bool {
	logger := klog.FromContext(ctx)
	// 先拷贝 Deployment 的注解（apply 与 revision 类注解除外）。
	annotationChanged := copyDeploymentAnnotationsToReplicaSet(deployment, newRS)
	// 再更新 ReplicaSet 的修订注解。
	if newRS.Annotations == nil {
		newRS.Annotations = make(map[string]string)
	}
	oldRevision, ok := newRS.Annotations[RevisionAnnotation]
	// 新 ReplicaSet 的修订号应当是所有 RS 中最大的。通常它就是 newRevision（所有旧 RS 的最大修订号 + 1）。
	// 但也可能出现这种情况：新 RS 的修订号更新后，某些旧 RS 被删除，导致 newRevision 反而小于新 RS 的修订号。
	// 因此只在新 RS 修订号小于 newRevision 时才更新，避免修订号倒退。

	oldRevisionInt, err := strconv.ParseInt(oldRevision, 10, 64)
	if err != nil {
		if oldRevision != "" {
			logger.Info("Updating replica set revision OldRevision not int", "err", err)
			return false
		}
		// ReplicaSet 注解为空时，把旧修订号初始化为 0。
		oldRevisionInt = 0
	}
	newRevisionInt, err := strconv.ParseInt(newRevision, 10, 64)
	if err != nil {
		logger.Info("Updating replica set revision NewRevision not int", "err", err)
		return false
	}
	if oldRevisionInt < newRevisionInt {
		newRS.Annotations[RevisionAnnotation] = newRevision
		annotationChanged = true
		logger.V(4).Info("Updating replica set revision", "replicaSet", klog.KObj(newRS), "newRevision", newRevision)
	}
	// 如果原先就有修订注解，而该 ReplicaSet 又被赋予了新的修订号，说明正在回滚到该 ReplicaSet。
	// 需要保留旧修订号作为历史信息。
	if ok && oldRevisionInt < newRevisionInt {
		revisionHistoryAnnotation := newRS.Annotations[RevisionHistoryAnnotation]
		oldRevisions := strings.Split(revisionHistoryAnnotation, ",")
		if len(oldRevisions[0]) == 0 {
			newRS.Annotations[RevisionHistoryAnnotation] = oldRevision
		} else {
			totalLen := len(revisionHistoryAnnotation) + len(oldRevision) + 1
			// start 表示在 oldRevisions 中开始丢弃的起始位置。
			start := 0
			for totalLen > revHistoryLimitInChars && start < len(oldRevisions) {
				totalLen = totalLen - len(oldRevisions[start]) - 1
				start++
			}
			if totalLen <= revHistoryLimitInChars {
				oldRevisions = append(oldRevisions[start:], oldRevision)
				newRS.Annotations[RevisionHistoryAnnotation] = strings.Join(oldRevisions, ",")
			} else {
				logger.Info("Not appending revision due to revision history length limit reached", "revisionHistoryLimit", revHistoryLimitInChars)
			}
		}
	}
	// 如果新 ReplicaSet 即将被创建，还需要给它加上副本数相关的注解。
	if !exists && SetReplicasAnnotations(newRS, *(deployment.Spec.Replicas), *(deployment.Spec.Replicas)+MaxSurge(*deployment)) {
		annotationChanged = true
	}
	return annotationChanged
}

var annotationsToSkip = map[string]bool{
	v1.LastAppliedConfigAnnotation: true,
	RevisionAnnotation:             true,
	RevisionHistoryAnnotation:      true,
	DesiredReplicasAnnotation:      true,
	MaxReplicasAnnotation:          true,
	apps.DeprecatedRollbackTo:      true,
}

// skipCopyAnnotation 判断给定键的注解是否应跳过拷贝。
//
// 参见 https://github.com/kubernetes/kubernetes/pull/20035#issuecomment-179558615
func skipCopyAnnotation(key string) bool {
	return annotationsToSkip[key]
}

// copyDeploymentAnnotationsToReplicaSet 把 Deployment 的注解拷贝到 ReplicaSet，
// 并在 ReplicaSet 注解发生变化时返回 true。
// 注意 apply 与 revision 类注解不会被拷贝。
func copyDeploymentAnnotationsToReplicaSet(deployment *apps.Deployment, rs *apps.ReplicaSet) bool {
	rsAnnotationsChanged := false
	if rs.Annotations == nil {
		rs.Annotations = make(map[string]string)
	}
	for k, v := range deployment.Annotations {
		// 新 RS 的修订号由 getNewReplicaSet 自动更新，随后再把新 RS 的修订号拷贝回 Deployment。
		// 不能反向把 Deployment 的修订号拷贝到新 RS，因为 Deployment 修订号的更新可能失败（导致修订号过时），
		// 而新 RS 上的修订号更可靠。
		if _, exist := rs.Annotations[k]; skipCopyAnnotation(k) || (exist && rs.Annotations[k] == v) {
			continue
		}
		rs.Annotations[k] = v
		rsAnnotationsChanged = true
	}
	return rsAnnotationsChanged
}

// SetDeploymentAnnotationsTo 把给定 ReplicaSet 的注解设置为 Deployment 的注解。
// 该操作仅应在 Deployment 回滚到该 ReplicaSet 时执行。
// 注意 apply 与 revision 类注解不会改动。
func SetDeploymentAnnotationsTo(deployment *apps.Deployment, rollbackToRS *apps.ReplicaSet) {
	deployment.Annotations = getSkippedAnnotations(deployment.Annotations)
	for k, v := range rollbackToRS.Annotations {
		if !skipCopyAnnotation(k) {
			deployment.Annotations[k] = v
		}
	}
}

// getSkippedAnnotations 从注解中筛出那些应跳过拷贝的注解。
func getSkippedAnnotations(annotations map[string]string) map[string]string {
	skippedAnnotations := make(map[string]string)
	for k, v := range annotations {
		if skipCopyAnnotation(k) {
			skippedAnnotations[k] = v
		}
	}
	return skippedAnnotations
}

// FindActiveOrLatest 在最多只有一个活跃 ReplicaSet 时返回它（没有活跃 RS 时返回最新的一个）。
// 如果存在多个活跃 ReplicaSet，则返回 nil，让调用方按比例扩缩它们。
func FindActiveOrLatest(newRS *apps.ReplicaSet, oldRSs []*apps.ReplicaSet) *apps.ReplicaSet {
	if newRS == nil && len(oldRSs) == 0 {
		return nil
	}

	sort.Sort(sort.Reverse(controller.ReplicaSetsByCreationTimestamp(oldRSs)))
	allRSs := controller.FilterActiveReplicaSets(append(oldRSs, newRS))

	switch len(allRSs) {
	case 0:
		// 没有活跃的 ReplicaSet 时返回最新的那个（oldRSs 已按创建时间倒序排列）。
		if newRS != nil {
			return newRS
		}
		return oldRSs[0]
	case 1:
		return allRSs[0]
	default:
		return nil
	}
}

// GetDesiredReplicasAnnotation 从 ReplicaSet 注解中读取期望副本数。
func GetDesiredReplicasAnnotation(logger klog.Logger, rs *apps.ReplicaSet) (int32, bool) {
	return getNonNegativeInt32FromAnnotationVerbose(logger, rs, DesiredReplicasAnnotation)
}

// getMaxReplicasAnnotation 从 ReplicaSet 注解中读取最大副本数。
func getMaxReplicasAnnotation(logger klog.Logger, rs *apps.ReplicaSet) (int32, bool) {
	return getNonNegativeInt32FromAnnotationVerbose(logger, rs, MaxReplicasAnnotation)
}

// getNonNegativeInt32FromAnnotationVerbose 读取非负整型注解，解析失败时记录日志。
func getNonNegativeInt32FromAnnotationVerbose(logger klog.Logger, rs *apps.ReplicaSet, annotationKey string) (int32, bool) {
	value, ok, err := getNonNegativeInt32FromAnnotation(rs, annotationKey)
	if err != nil {
		logger.V(2).Info("Could not convert the value with annotation key for the replica set", "annotationValue", rs.Annotations[annotationKey], "annotationKey", annotationKey, "replicaSet", klog.KObj(rs))
	}
	return value, ok
}

// getNonNegativeInt32FromAnnotation 解析指定注解为非负 int32，返回数值、是否存在以及解析错误。
func getNonNegativeInt32FromAnnotation(rs *apps.ReplicaSet, annotationKey string) (int32, bool, error) {
	annotationValue, ok := rs.Annotations[annotationKey]
	if !ok {
		return int32(0), false, nil
	}
	intValue, err := strconv.ParseUint(annotationValue, 10, 32)
	if err != nil {
		return int32(0), false, err
	}
	if intValue > math.MaxInt32 {
		return int32(0), false, fmt.Errorf("value %d is out of range (higher than %d)", intValue, math.MaxInt32)
	}
	return int32(intValue), true, nil
}

// SetReplicasAnnotations 把 desiredReplicas 与 maxReplicas 写入注解，返回是否发生变更。
func SetReplicasAnnotations(rs *apps.ReplicaSet, desiredReplicas, maxReplicas int32) bool {
	updated := false
	if rs.Annotations == nil {
		rs.Annotations = make(map[string]string)
	}
	desiredString := fmt.Sprintf("%d", desiredReplicas)
	if hasString := rs.Annotations[DesiredReplicasAnnotation]; hasString != desiredString {
		rs.Annotations[DesiredReplicasAnnotation] = desiredString
		updated = true
	}
	maxString := fmt.Sprintf("%d", maxReplicas)
	if hasString := rs.Annotations[MaxReplicasAnnotation]; hasString != maxString {
		rs.Annotations[MaxReplicasAnnotation] = maxString
		updated = true
	}
	return updated
}

// ReplicasAnnotationsNeedUpdate 判断副本数相关注解是否需要更新，避免不必要的 API 写操作。
func ReplicasAnnotationsNeedUpdate(rs *apps.ReplicaSet, desiredReplicas, maxReplicas int32) bool {
	if rs.Annotations == nil {
		return true
	}
	desiredString := fmt.Sprintf("%d", desiredReplicas)
	if hasString := rs.Annotations[DesiredReplicasAnnotation]; hasString != desiredString {
		return true
	}
	maxString := fmt.Sprintf("%d", maxReplicas)
	if hasString := rs.Annotations[MaxReplicasAnnotation]; hasString != maxString {
		return true
	}
	return false
}

// MaxUnavailable 返回滚动更新过程中允许的最大不可用 Pod 数。
func MaxUnavailable(deployment apps.Deployment) int32 {
	if !IsRollingUpdate(&deployment) || *(deployment.Spec.Replicas) == 0 {
		return int32(0)
	}
	// 参数合法性已由校验环节保证，这里忽略错误。
	_, maxUnavailable, _ := ResolveFenceposts(deployment.Spec.Strategy.RollingUpdate.MaxSurge, deployment.Spec.Strategy.RollingUpdate.MaxUnavailable, *(deployment.Spec.Replicas))
	if maxUnavailable > *deployment.Spec.Replicas {
		return *deployment.Spec.Replicas
	}
	return maxUnavailable
}

// MinAvailable 返回给定 Deployment 所需的最小可用 Pod 数。
func MinAvailable(deployment *apps.Deployment) int32 {
	if !IsRollingUpdate(deployment) {
		return int32(0)
	}
	return *(deployment.Spec.Replicas) - MaxUnavailable(*deployment)
}

// MaxSurge 返回滚动更新过程中允许的最大超出期望副本数的 Pod 数。
func MaxSurge(deployment apps.Deployment) int32 {
	if !IsRollingUpdate(&deployment) {
		return int32(0)
	}
	// 参数合法性已由校验环节保证，这里忽略错误。
	maxSurge, _, _ := ResolveFenceposts(deployment.Spec.Strategy.RollingUpdate.MaxSurge, deployment.Spec.Strategy.RollingUpdate.MaxUnavailable, *(deployment.Spec.Replicas))
	return maxSurge
}

// GetReplicaSetProportion 根据 1) 父 Deployment 当前规模、2) 需要在各 ReplicaSet 上增减的副本数、
// 3) 到目前为止已在各 ReplicaSet 上增减的副本总数，估算给定 ReplicaSet 应分摊的比例。
func GetReplicaSetProportion(logger klog.Logger, rs *apps.ReplicaSet, d apps.Deployment, deploymentReplicasToAdd, deploymentReplicasAdded int32) int32 {
	if rs == nil || *(rs.Spec.Replicas) == 0 || deploymentReplicasToAdd == 0 || deploymentReplicasToAdd == deploymentReplicasAdded {
		return int32(0)
	}

	rsFraction := getReplicaSetFraction(logger, *rs, d)
	allowed := deploymentReplicasToAdd - deploymentReplicasAdded

	if deploymentReplicasToAdd > 0 {
		// 扩容时取估算值与剩余可增加副本数的较小者，确保不会超出允许增加的副本数。
		return min(rsFraction, allowed)
	}
	// 缩容时取估算值与剩余可移除副本数的较大者（两者皆为负），确保不会超出允许移除的副本数。
	return max(rsFraction, allowed)
}

// getReplicaSetFraction 估算 ReplicaSet 在以下场景中应分摊的副本数：
// 1) 发布过程中的扩缩容事件；2) 对暂停的 Deployment 进行扩缩容。
func getReplicaSetFraction(logger klog.Logger, rs apps.ReplicaSet, d apps.Deployment) int32 {
	// 缩容到 0 时，该 ReplicaSet 应分摊的副本数就是它的全部副本（取负值）。
	if *(d.Spec.Replicas) == int32(0) {
		return -*(rs.Spec.Replicas)
	}

	deploymentMaxReplicas := *(d.Spec.Replicas) + MaxSurge(d)
	deploymentMaxReplicasBeforeScale, ok := getMaxReplicasAnnotation(logger, &rs)
	if !ok || deploymentMaxReplicasBeforeScale == 0 {
		// 注解缺失时回退到 Deployment 当前的副本数。注解可能被人为篡改（删除、设为非法值或 0）。
		// 若其他 ReplicaSet 上的注解值不同（说明 Deployment 曾被扩缩容），这样估算并不精确，
		// 但 GetReplicaSetProportion 中的 min/max 比较至少能保证结果不越界。
		deploymentMaxReplicasBeforeScale = d.Status.Replicas
		if deploymentMaxReplicasBeforeScale == 0 {
			// 极少见的情况：注解缺失且 Pod 创建失败，导致状态副本数为 0。
			return 0
		}
	}

	// GetReplicaSetProportion 不会从 0 按比例扩容，因此这里 rs.spec.replicas 不会为 0。
	scaleBase := *(rs.Spec.Replicas)
	// deploymentMaxReplicasBeforeScale 通常为正值，上面已经确保它不为 0。
	newRSsize := (float64(scaleBase * deploymentMaxReplicas)) / float64(deploymentMaxReplicasBeforeScale)
	return integer.RoundToInt32(newRSsize) - *(rs.Spec.Replicas)
}

// RsListFromClient 返回一个包装给定 client 的 rsListFunc。
func RsListFromClient(c appsclient.AppsV1Interface) RsListFunc {
	return func(namespace string, options metav1.ListOptions) ([]*apps.ReplicaSet, error) {
		rsList, err := c.ReplicaSets(namespace).List(context.TODO(), options)
		if err != nil {
			return nil, err
		}
		var ret []*apps.ReplicaSet
		for i := range rsList.Items {
			ret = append(ret, &rsList.Items[i])
		}
		return ret, err
	}
}

// RsListFunc 按命名空间与 ListOptions 返回 ReplicaSet 列表。
type RsListFunc func(string, metav1.ListOptions) ([]*apps.ReplicaSet, error)

// podListFunc 按命名空间与 ListOptions 返回 Pod 列表。
type podListFunc func(string, metav1.ListOptions) (*v1.PodList, error)

// ListReplicaSets 返回给定 Deployment 所指向的 ReplicaSet 切片。
// 注意这里不会协调 ControllerRef（纳管/弃管），因为只有控制器自身才应做这件事；
// 但会过滤掉 ControllerRef 不匹配的 ReplicaSet。
func ListReplicaSets(deployment *apps.Deployment, getRSList RsListFunc) ([]*apps.ReplicaSet, error) {
	// TODO: Right now we list replica sets by their labels. We should list them by selector, i.e. the replica set's selector
	//       should be a superset of the deployment's selector, see https://github.com/kubernetes/kubernetes/issues/19830.
	namespace := deployment.Namespace
	selector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
	if err != nil {
		return nil, err
	}
	options := metav1.ListOptions{LabelSelector: selector.String()}
	all, err := getRSList(namespace, options)
	if err != nil {
		return nil, err
	}
	// 只保留 ControllerRef 指向该 Deployment 的 ReplicaSet。
	owned := make([]*apps.ReplicaSet, 0, len(all))
	for _, rs := range all {
		if metav1.IsControlledBy(rs, deployment) {
			owned = append(owned, rs)
		}
	}
	return owned, nil
}

// ListPods 返回给定 Deployment 所指向的 Pod 列表。
// 调用方需要先通过 ListReplicaSets() 取得该 Deployment 的 ReplicaSet 列表。
// 注意这里不会协调 ControllerRef（纳管/弃管），因为只有控制器自身才应做这件事；
// 但会过滤掉 ControllerRef 不匹配的 Pod。
func ListPods(deployment *apps.Deployment, rsList []*apps.ReplicaSet, getPodList podListFunc) (*v1.PodList, error) {
	namespace := deployment.Namespace
	selector, err := metav1.LabelSelectorAsSelector(deployment.Spec.Selector)
	if err != nil {
		return nil, err
	}
	options := metav1.ListOptions{LabelSelector: selector.String()}
	all, err := getPodList(namespace, options)
	if err != nil {
		return all, err
	}
	// 只保留 ControllerRef 指向属于本 Deployment 的 ReplicaSet 的 Pod。
	rsMap := make(map[types.UID]bool, len(rsList))
	for _, rs := range rsList {
		rsMap[rs.UID] = true
	}
	owned := &v1.PodList{Items: make([]v1.Pod, 0, len(all.Items))}
	for i := range all.Items {
		pod := &all.Items[i]
		controllerRef := metav1.GetControllerOf(pod)
		if controllerRef != nil && rsMap[controllerRef.UID] {
			owned.Items = append(owned.Items, *pod)
		}
	}
	return owned, nil
}

// EqualIgnoreHash 判断两个 PodTemplateSpec 是否相等，忽略 Labels[pod-template-hash] 取值的差异。
// 忽略 pod-template-hash 的原因是：
//  1. PodTemplateSpec 的 API 发生变化时（例如新增字段）哈希值就会改变；
//  2. Deployment 的模板本身不带哈希标签。
func EqualIgnoreHash(template1, template2 *v1.PodTemplateSpec) bool {
	t1Copy := template1.DeepCopy()
	t2Copy := template2.DeepCopy()
	// 比较前先从标签中移除哈希标签。
	delete(t1Copy.Labels, apps.DefaultDeploymentUniqueLabelKey)
	delete(t2Copy.Labels, apps.DefaultDeploymentUniqueLabelKey)
	return apiequality.Semantic.DeepEqual(t1Copy, t2Copy)
}

// FindNewReplicaSet 返回给定 Deployment 指向的新 ReplicaSet（即 Pod 模板相同的那个）。
func FindNewReplicaSet(deployment *apps.Deployment, rsList []*apps.ReplicaSet) *apps.ReplicaSet {
	sort.Sort(controller.ReplicaSetsByCreationTimestamp(rsList))
	for i := range rsList {
		if EqualIgnoreHash(&rsList[i].Spec.Template, &deployment.Spec.Template) {
			// 极少数情况下（例如集群升级后），Deployment 可能出现多个模板相同的新 ReplicaSet，
			// 参见 https://github.com/kubernetes/kubernetes/issues/40415。
			// 这里确定性地选择最早创建的那个（rsList 已按创建时间排序）。
			return rsList[i]
		}
	}
	// 新 ReplicaSet 不存在。
	return nil
}

// FindOldReplicaSets 从给定 ReplicaSet 切片中找出属于给定 Deployment 的旧 ReplicaSet。
// 返回两个集合：第一个不含副本数为 0 的旧 ReplicaSet，第二个包含所有旧 ReplicaSet。
func FindOldReplicaSets(deployment *apps.Deployment, rsList []*apps.ReplicaSet) ([]*apps.ReplicaSet, []*apps.ReplicaSet) {
	var requiredRSs []*apps.ReplicaSet
	var allRSs []*apps.ReplicaSet
	newRS := FindNewReplicaSet(deployment, rsList)
	for _, rs := range rsList {
		// 过滤掉新 ReplicaSet。
		if newRS != nil && rs.UID == newRS.UID {
			continue
		}
		allRSs = append(allRSs, rs)
		if *(rs.Spec.Replicas) != 0 {
			requiredRSs = append(requiredRSs, rs)
		}
	}
	return requiredRSs, allRSs
}

// SetFromReplicaSetTemplate 用 ReplicaSet 的模板设置 Deployment 的期望 PodTemplateSpec，并移除哈希标签。
func SetFromReplicaSetTemplate(deployment *apps.Deployment, template v1.PodTemplateSpec) *apps.Deployment {
	deployment.Spec.Template.ObjectMeta = template.ObjectMeta
	deployment.Spec.Template.Spec = template.Spec
	deployment.Spec.Template.ObjectMeta.Labels = labelsutil.CloneAndRemoveLabel(
		deployment.Spec.Template.ObjectMeta.Labels,
		apps.DefaultDeploymentUniqueLabelKey)
	return deployment
}

// GetReplicaCountForReplicaSets 返回给定 ReplicaSet 的期望副本数之和。
func GetReplicaCountForReplicaSets(replicaSets []*apps.ReplicaSet) int32 {
	totalReplicas := int32(0)
	for _, rs := range replicaSets {
		if rs != nil {
			totalReplicas += *(rs.Spec.Replicas)
		}
	}
	return totalReplicas
}

// GetActualReplicaCountForReplicaSets 返回给定 ReplicaSet 的实际副本数之和。
func GetActualReplicaCountForReplicaSets(replicaSets []*apps.ReplicaSet) int32 {
	totalActualReplicas := int32(0)
	for _, rs := range replicaSets {
		if rs != nil {
			totalActualReplicas += rs.Status.Replicas
		}
	}
	return totalActualReplicas
}

// GetReadyReplicaCountForReplicaSets 返回给定 ReplicaSet 中处于 Ready 状态的 Pod 数。
func GetReadyReplicaCountForReplicaSets(replicaSets []*apps.ReplicaSet) int32 {
	totalReadyReplicas := int32(0)
	for _, rs := range replicaSets {
		if rs != nil {
			totalReadyReplicas += rs.Status.ReadyReplicas
		}
	}
	return totalReadyReplicas
}

// GetAvailableReplicaCountForReplicaSets 返回给定 ReplicaSet 中处于 Available 状态的 Pod 数。
func GetAvailableReplicaCountForReplicaSets(replicaSets []*apps.ReplicaSet) int32 {
	totalAvailableReplicas := int32(0)
	for _, rs := range replicaSets {
		if rs != nil {
			totalAvailableReplicas += rs.Status.AvailableReplicas
		}
	}
	return totalAvailableReplicas
}

// GetTerminatingReplicaCountForReplicaSets 返回所有 ReplicaSet 中处于终止中的 Pod 数；
// 若某个已被控制器同步过的 ReplicaSet 没有上报该计数，则返回 nil 表示无法计算。
func GetTerminatingReplicaCountForReplicaSets(replicaSets []*apps.ReplicaSet) *int32 {
	terminatingReplicas := int32(0)
	for _, rs := range replicaSets {
		switch {
		case rs == nil:
			// 空对象直接跳过。
		case rs.Status.ObservedGeneration == 0 && rs.Status.TerminatingReplicas == nil:
			// 从未被控制器同步过的 ReplicaSet 不计入终止副本数。
		case rs.Status.TerminatingReplicas == nil:
			// 已被控制器同步但未上报终止副本数的 ReplicaSet 会导致无法求和，只能返回 nil。
			return nil
		default:
			terminatingReplicas += *rs.Status.TerminatingReplicas
		}
	}
	return &terminatingReplicas
}

// IsRollingUpdate 判断 Deployment 的更新策略是否为滚动更新。
func IsRollingUpdate(deployment *apps.Deployment) bool {
	return deployment.Spec.Strategy.Type == apps.RollingUpdateDeploymentStrategyType
}

// DeploymentComplete 判断 Deployment 是否已完成：所有期望副本都已更新且可用，且没有旧 Pod 在运行。
func DeploymentComplete(deployment *apps.Deployment, newStatus *apps.DeploymentStatus) bool {
	return newStatus.UpdatedReplicas == *(deployment.Spec.Replicas) &&
		newStatus.Replicas == *(deployment.Spec.Replicas) &&
		newStatus.AvailableReplicas == *(deployment.Spec.Replicas) &&
		newStatus.ObservedGeneration >= deployment.Generation
}

// DeploymentProgressing 判断 Deployment 是否有进展。
// 通过比较控制器观察到的当前状态与新状态来评估：新副本增加、Pod 变为 Ready/Available，
// 或旧副本减少，都视为有进展。
func DeploymentProgressing(deployment *apps.Deployment, newStatus *apps.DeploymentStatus) bool {
	oldStatus := deployment.Status

	// 需要被缩容的旧副本数 = 总副本数 - 已更新副本数。
	oldStatusOldReplicas := oldStatus.Replicas - oldStatus.UpdatedReplicas
	newStatusOldReplicas := newStatus.Replicas - newStatus.UpdatedReplicas

	return (newStatus.UpdatedReplicas > oldStatus.UpdatedReplicas) ||
		(newStatusOldReplicas < oldStatusOldReplicas) ||
		newStatus.ReadyReplicas > deployment.Status.ReadyReplicas ||
		newStatus.AvailableReplicas > deployment.Status.AvailableReplicas
}

// nowFn 用于单元测试中替换时间源。
var nowFn = time.Now

// DeploymentTimedOut 判断 Deployment 是否已超时：报告进展的条件已超过 progressDeadlineSeconds，
// 或已经存在原因为 TimedOutReason 的 Progressing 条件。
func DeploymentTimedOut(ctx context.Context, deployment *apps.Deployment, newStatus *apps.DeploymentStatus) bool {
	if !HasProgressDeadline(deployment) {
		return false
	}

	// 查找 Progressing 条件：不存在时无从评估进展；已是 TimedOutReason 则说明已经超时，无需重复检查。
	condition := GetDeploymentCondition(*newStatus, apps.DeploymentProgressing)
	if condition == nil {
		return false
	}
	// 若上一次条件是成功的发布完成，就不应再评估进展。场景如下：
	//
	// * progressDeadlineSeconds 小于“当前时间 - 上一次发布完成时间”；
	// * 新建 ReplicaSet 触发了 Deployment 重新同步，而缓存中 Deployment 的状态条件尚未更新为
	//   “发现新 ReplicaSet”。
	//
	// Deployment 会被再次同步，其 Progressing 条件最终会追上真实状态。
	if condition.Reason == NewRSAvailableReason {
		return false
	}
	if condition.Reason == TimedOutReason {
		return true
	}
	logger := klog.FromContext(ctx)
	// 计算当前时间与上次报告进展/创建 ReplicaSet/恢复暂停 Deployment 之间的秒数，
	// 并与 progressDeadlineSeconds 比较。
	from := condition.LastUpdateTime
	now := nowFn()
	delta := time.Duration(*deployment.Spec.ProgressDeadlineSeconds) * time.Second
	timedOut := from.Add(delta).Before(now)

	logger.V(4).Info("Deployment timed out from last progress check", "deployment", klog.KObj(deployment), "timeout", timedOut, "from", from, "now", now)
	return timedOut
}

// NewRSNewReplicas 计算 Deployment 的新 ReplicaSet 应有的副本数。
// 满足以下任一条件时视为正在发布，否则视为扩缩容：
// 1) 新 ReplicaSet 已饱和：其副本数等于 Deployment 的期望副本数；
// 2) 已达 Pod 数量上限：Deployment 期望副本数 + maxSurge 等于所有 ReplicaSet 的副本总数。
func NewRSNewReplicas(deployment *apps.Deployment, allRSs []*apps.ReplicaSet, newRS *apps.ReplicaSet) (int32, error) {
	switch deployment.Spec.Strategy.Type {
	case apps.RollingUpdateDeploymentStrategyType:
		// 检查是否还能扩容。
		maxSurge, err := intstrutil.GetScaledValueFromIntOrPercent(deployment.Spec.Strategy.RollingUpdate.MaxSurge, int(*(deployment.Spec.Replicas)), true)
		if err != nil {
			return 0, err
		}
		// 统计当前所有 ReplicaSet 的 Pod 总数。
		currentPodCount := GetReplicaCountForReplicaSets(allRSs)
		maxTotalPods := *(deployment.Spec.Replicas) + int32(maxSurge)
		if currentPodCount >= maxTotalPods {
			// 已达到上限，不能再扩容。
			return *(newRS.Spec.Replicas), nil
		}
		// 计算可扩容的数量。
		scaleUpCount := maxTotalPods - currentPodCount
		// 不超过 Deployment 的期望副本数。
		scaleUpCount = min(scaleUpCount, *(deployment.Spec.Replicas)-*(newRS.Spec.Replicas))
		return *(newRS.Spec.Replicas) + scaleUpCount, nil
	case apps.RecreateDeploymentStrategyType:
		return *(deployment.Spec.Replicas), nil
	default:
		return 0, fmt.Errorf("deployment type %v isn't supported", deployment.Spec.Strategy.Type)
	}
}

// IsSaturated 通过比较新 ReplicaSet 的规模与 Deployment 的规模来判断它是否饱和。
// Deployment 与 ReplicaSet 都必须认可该 ReplicaSet 能承载 Deployment 的全部期望副本，
// 注解机制正是为此服务；同时该 ReplicaSet 的所有 Pod 都必须可用。
func IsSaturated(deployment *apps.Deployment, rs *apps.ReplicaSet) bool {
	if rs == nil {
		return false
	}
	desiredString := rs.Annotations[DesiredReplicasAnnotation]
	desired, err := strconv.Atoi(desiredString)
	if err != nil {
		return false
	}
	return *(rs.Spec.Replicas) == *(deployment.Spec.Replicas) &&
		int32(desired) == *(deployment.Spec.Replicas) &&
		rs.Status.AvailableReplicas == *(deployment.Spec.Replicas)
}

// WaitForObservedDeployment 轮询等待 Deployment 更新，直到其 ObservedGeneration >= desiredGeneration。
// 轮询超时则返回错误。
func WaitForObservedDeployment(getDeploymentFunc func() (*apps.Deployment, error), desiredGeneration int64, interval, timeout time.Duration) error {
	return wait.PollImmediate(interval, timeout, func() (bool, error) {
		deployment, err := getDeploymentFunc()
		if err != nil {
			return false, err
		}
		return deployment.Status.ObservedGeneration >= desiredGeneration, nil
	})
}

// ResolveFenceposts 同时解析 maxSurge 与 maxUnavailable，必须在一步内完成。例如：
//
// 期望 2，maxUnavailable 1%，surge 0% —— 应先缩旧(-1)，再扩新(+1)，再缩旧(-1)，再扩新(+1)
// 期望 1，maxUnavailable 1%，surge 0% —— 应先缩旧(-1)，再扩新(+1)
// 期望 2，maxUnavailable 25%，surge 1% —— 应先扩新(+1)，再缩旧(-1)，再扩新(+1)，再缩旧(-1)
// 期望 1，maxUnavailable 25%，surge 1% —— 应先扩新(+1)，再缩旧(-1)
// 期望 2，maxUnavailable 0%，surge 1% —— 应先扩新(+1)，再缩旧(-1)，再扩新(+1)，再缩旧(-1)
// 期望 1，maxUnavailable 0%，surge 1% —— 应先扩新(+1)，再缩旧(-1)
func ResolveFenceposts(maxSurge, maxUnavailable *intstrutil.IntOrString, desired int32) (int32, int32, error) {
	surge, err := intstrutil.GetScaledValueFromIntOrPercent(intstrutil.ValueOrDefault(maxSurge, intstrutil.FromInt32(0)), int(desired), true)
	if err != nil {
		return 0, 0, err
	}
	unavailable, err := intstrutil.GetScaledValueFromIntOrPercent(intstrutil.ValueOrDefault(maxUnavailable, intstrutil.FromInt32(0)), int(desired), false)
	if err != nil {
		return 0, 0, err
	}

	if surge == 0 && unavailable == 0 {
		// 校验环节不应允许用户同时显式设置 maxSurge 与 maxUnavailable 为 0，
		// 但 maxUnavailable 向下取整后可能变成 0。若两者都解析为 0，
		// 则把 maxUnavailable 设为 1，因为 surge 可能因配额限制而无法生效。
		unavailable = 1
	}

	return int32(surge), int32(unavailable), nil
}

// HasProgressDeadline 判断 Deployment 是否在进展耗时过长时应上报 "ProgressDeadlineExceeded" 原因。
func HasProgressDeadline(d *apps.Deployment) bool {
	return d.Spec.ProgressDeadlineSeconds != nil && *d.Spec.ProgressDeadlineSeconds != math.MaxInt32
}

// HasRevisionHistoryLimit 判断 Deployment 是否需要保留指定数量的旧 ReplicaSet。
// 保留它们主要是为了支持回滚。RevisionHistoryLimit 可以从 0（不保留）开始；
// 当设置为 math.MaxInt32 时，Deployment 将保留所有历史版本。
func HasRevisionHistoryLimit(d *apps.Deployment) bool {
	return d.Spec.RevisionHistoryLimit != nil && *d.Spec.RevisionHistoryLimit != math.MaxInt32
}

// GetDeploymentsForReplicaSet 返回所有可能匹配某个 ReplicaSet 的 Deployment 列表。
// 其中只有 ReplicaSet 的 ControllerRef 指定的那个才会真正管理它。
// 仅在找不到匹配的 Deployment 时返回错误。
func GetDeploymentsForReplicaSet(deploymentLister appslisters.DeploymentLister, rs *apps.ReplicaSet) ([]*apps.Deployment, error) {
	if len(rs.Labels) == 0 {
		return nil, fmt.Errorf("no deployments found for ReplicaSet %v because it has no labels", rs.Name)
	}

	dList, err := deploymentLister.Deployments(rs.Namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	var deployments []*apps.Deployment
	for _, d := range dList {
		selector, err := metav1.LabelSelectorAsSelector(d.Spec.Selector)
		if err != nil {
			// 选择器非法的 Deployment 与该 ReplicaSet 不匹配。
			continue
		}
		// 若混入选择器为空的 Deployment，它不应匹配任何对象，而不是匹配所有对象。
		if selector.Empty() || !selector.Matches(labels.Set(rs.Labels)) {
			continue
		}
		deployments = append(deployments, d)
	}

	if len(deployments) == 0 {
		return nil, fmt.Errorf("could not find deployments set for ReplicaSet %s in namespace %s with labels: %v", rs.Name, rs.Namespace, rs.Labels)
	}

	return deployments, nil
}

// ReplicaSetsByRevision 按修订号对 ReplicaSet 排序，修订号相同时以创建时间或名称作为决胜键。
// 借助创建时间，排序结果是从旧到新。
type ReplicaSetsByRevision []*apps.ReplicaSet

// Len 返回待排序的 ReplicaSet 数量。
func (o ReplicaSetsByRevision) Len() int { return len(o) }

// Swap 交换 i 与 j 位置上的两个 ReplicaSet。
func (o ReplicaSetsByRevision) Swap(i, j int) { o[i], o[j] = o[j], o[i] }

// Less 按修订号比较两个 ReplicaSet；修订号相同或解析失败时回退到创建时间比较。
func (o ReplicaSetsByRevision) Less(i, j int) bool {
	revision1, err1 := Revision(o[i])
	revision2, err2 := Revision(o[j])
	if err1 != nil || err2 != nil || revision1 == revision2 {
		return controller.ReplicaSetsByCreationTimestamp(o).Less(i, j)
	}
	return revision1 < revision2
}
