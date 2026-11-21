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

package statefulset

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/klog/v2"
	"k8s.io/utils/lru"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/controller/history"
	"k8s.io/kubernetes/pkg/controller/statefulset/metrics"
	"k8s.io/kubernetes/pkg/features"
)

// 并行处理时单批最大在途请求数，取一个经验上合理的值以限制并发规模。
const MaxBatchSize = 500

// StatefulSetControl 封装 StatefulSet 及其子 Pod 的更新控制逻辑。
// 定义为接口是为了便于扩展不同语义的实现，目前仅有默认实现。
type StatefulSetControlInterface interface {
	// UpdateStatefulSet 实现 Pod 与 PVC 的创建、更新、删除控制逻辑。
	// 返回非 nil 错误时调用方会限速重试；不希望重试的错误应由实现方自行消化，
	// 也可以在任意点提前返回，只要更新能在后续被重新执行。
	UpdateStatefulSet(ctx context.Context, set *apps.StatefulSet, pods []*v1.Pod, now time.Time) (*apps.StatefulSetStatus, error)
	// ListRevisions 返回表示 set 各版本的 ControllerRevision 数组；错误为 nil 时结果有效。
	ListRevisions(set *apps.StatefulSet) ([]*apps.ControllerRevision, error)
	// AdoptOrphanRevisions 收养所有匹配 set 选择器的孤儿 ControllerRevision；全部成功时返回 nil。
	AdoptOrphanRevisions(set *apps.StatefulSet, revisions []*apps.ControllerRevision) error
}

// NewDefaultStatefulSetControl 创建 StatefulSetControlInterface 的默认实现，
// 按文档约定的语义调和 StatefulSet：podControl 负责 Pod 与 PVC 的增删改，
// statusUpdater 负责状态更新；除测试外应使用 NewRealStatefulPodControl() 构造的依赖。
func NewDefaultStatefulSetControl(
	podControl *StatefulPodControl,
	statusUpdater StatefulSetStatusUpdaterInterface,
	controllerHistory history.Interface) StatefulSetControlInterface {
	// 初始化版本相等性缓存，避免稳态下重复进行昂贵的版本比较。
	return &defaultStatefulSetControl{podControl, statusUpdater, controllerHistory, lru.New(maxRevisionEqualityCacheEntries)}
}

// defaultStatefulSetControl 是 StatefulSetControlInterface 的默认实现。
type defaultStatefulSetControl struct {
	podControl        *StatefulPodControl
	statusUpdater     StatefulSetStatusUpdaterInterface
	controllerHistory history.Interface

	revisionEqualityCache *lru.Cache
}

// UpdateStatefulSet 执行 StatefulSet 的核心调和循环。
// 默认使用单调更新策略：按序号扩缩容、存在不健康 Pod 时不创建新 Pod、按降序终止 Pod；
// burst 策略放宽这些约束，立即且无序地创建/删除 Pod，使用者需自行承担可用副本数不可预测的一致性后果。
func (ssc *defaultStatefulSetControl) UpdateStatefulSet(ctx context.Context, set *apps.StatefulSet, pods []*v1.Pod, now time.Time) (*apps.StatefulSetStatus, error) {
	// performUpdate 创建新版本时会修改 set，先深拷贝以免污染 informer 缓存中的对象。
	set = set.DeepCopy()

	// 先获取并排序历史版本，后续版本比较依赖有序输入。
	revisions, err := ssc.ListRevisions(set)
	if err != nil {
		return nil, err
	}
	history.SortControllerRevisions(revisions)

	currentRevision, updateRevision, status, err := ssc.performUpdate(ctx, set, pods, revisions, now)
	if err != nil {
		errs := []error{err}
		if agg, ok := err.(utilerrors.Aggregate); ok {
			errs = agg.Errors()
		}
		return nil, utilerrors.NewAggregate(append(errs, ssc.truncateHistory(set, pods, revisions, currentRevision, updateRevision)))
	}

	// 无论更新成功或失败都要裁剪历史版本，避免无限增长。
	return status, ssc.truncateHistory(set, pods, revisions, currentRevision, updateRevision)
}

// performUpdate 计算当前/目标版本并执行实际更新，最后持久化状态；
// 即使更新出错也要尽量写回已计算出的状态。
func (ssc *defaultStatefulSetControl) performUpdate(
	ctx context.Context, set *apps.StatefulSet, pods []*v1.Pod, revisions []*apps.ControllerRevision, now time.Time) (*apps.ControllerRevision, *apps.ControllerRevision, *apps.StatefulSetStatus, error) {
	var currentStatus *apps.StatefulSetStatus
	logger := klog.FromContext(ctx)
	// 计算当前版本与目标版本，必要时创建新版本。
	currentRevision, updateRevision, collisionCount, err := ssc.getStatefulSetRevisions(set, revisions)
	if err != nil {
		return currentRevision, updateRevision, currentStatus, err
	}

	// 即使主更新出错也可能得到部分状态，仍需继续回写。
	currentStatus, err = ssc.updateStatefulSet(ctx, set, currentRevision, updateRevision, collisionCount, pods, now)
	if err != nil && currentStatus == nil {
		return currentRevision, updateRevision, nil, err
	}

	// 更新出错但已有状态时仍写回状态，避免丢失进度（如已删除的副本数）。
	statusErr := ssc.updateStatefulSetStatus(ctx, set, currentStatus)
	if statusErr == nil {
		logger.V(4).Info("Updated status", "statefulSet", klog.KObj(set),
			"replicas", currentStatus.Replicas,
			"readyReplicas", currentStatus.ReadyReplicas,
			"currentReplicas", currentStatus.CurrentReplicas,
			"updatedReplicas", currentStatus.UpdatedReplicas)
	}

	switch {
	case err != nil && statusErr != nil:
		logger.Error(statusErr, "Could not update status", "statefulSet", klog.KObj(set))
		return currentRevision, updateRevision, currentStatus, err
	case err != nil:
		return currentRevision, updateRevision, currentStatus, err
	case statusErr != nil:
		return currentRevision, updateRevision, currentStatus, statusErr
	}

	logger.V(4).Info("StatefulSet revisions", "statefulSet", klog.KObj(set),
		"currentRevision", currentStatus.CurrentRevision,
		"updateRevision", currentStatus.UpdateRevision)

	return currentRevision, updateRevision, currentStatus, nil
}

// ListRevisions 列出 Set 选择器匹配的所有 ControllerRevision。
func (ssc *defaultStatefulSetControl) ListRevisions(set *apps.StatefulSet) ([]*apps.ControllerRevision, error) {
	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		return nil, err
	}
	return ssc.controllerHistory.ListControllerRevisions(set, controllerKind, selector)
}

// AdoptOrphanRevisions 逐个收养孤儿 ControllerRevision，任何一个失败即返回。
func (ssc *defaultStatefulSetControl) AdoptOrphanRevisions(
	set *apps.StatefulSet,
	revisions []*apps.ControllerRevision) error {
	for i := range revisions {
		adopted, err := ssc.controllerHistory.AdoptControllerRevision(set, controllerKind, revisions[i])
		if err != nil {
			return err
		}
		revisions[i] = adopted
	}
	return nil
}

// truncateHistory 裁剪不再使用的 ControllerRevision：状态中的 Current/UpdateRevision 以及
// 仍被 Pod 引用的版本视为存活；其余按版本号从旧到新删除，直到只剩 RevisionHistoryLimit 个历史版本。
// 调用方必须保证 revisions 已排序。
func (ssc *defaultStatefulSetControl) truncateHistory(
	set *apps.StatefulSet,
	pods []*v1.Pod,
	revisions []*apps.ControllerRevision,
	current *apps.ControllerRevision,
	update *apps.ControllerRevision) error {
	history := make([]*apps.ControllerRevision, 0, len(revisions))
	// 先标记存活版本，它们不能被删除。
	live := map[string]bool{}
	if current != nil {
		live[current.Name] = true
	}
	if update != nil {
		live[update.Name] = true
	}
	for i := range pods {
		live[getPodRevision(pods[i])] = true
	}
	// 未被标记的版本都是可裁剪的历史。
	for i := range revisions {
		if !live[revisions[i].Name] {
			history = append(history, revisions[i])
		}
	}
	historyLen := len(history)
	historyLimit := int(*set.Spec.RevisionHistoryLimit)
	if historyLimit < 0 || historyLen <= historyLimit {
		return nil
	}
	// 只删除超出保留上限的最旧部分。
	history = history[:(historyLen - historyLimit)]
	for i := 0; i < len(history); i++ {
		if err := ssc.controllerHistory.DeleteControllerRevision(history[i]); err != nil {
			return err
		}
	}
	return nil
}

// maxRevisionEqualityCacheEntries 是 set/ControllerRevision 相等性缓存的容量上限；
// 一万个条目约占 1MB：键 40 字节、值 16 字节、setUID 字符串 36 字节、版本 ResourceVersion 19 字节。
const maxRevisionEqualityCacheEntries = 10_000

// revisionEqualityKey 缓存键，记录某个 ResourceVersion 的版本与特定 set(UID, generation)
// 推导出的版本相等，用于跳过后续重复比较。
type revisionEqualityKey struct {
	setUID                  types.UID
	setGeneration           int64
	revisionResourceVersion string
}

// setMatchesLatestExistingRevision 判断拟议版本是否与恢复最新已有版本后推导出的结果一致，
// 一致则无需创建新版本。
func setMatchesLatestExistingRevision(set *apps.StatefulSet, proposedRevision *apps.ControllerRevision, latestExistingRevision *apps.ControllerRevision, memory *lru.Cache) bool {
	// 特性关闭时不做语义比较，保持旧行为。
	if !utilfeature.DefaultFeatureGate.Enabled(features.StatefulSetSemanticRevisionComparison) {
		return false
	}
	equalityCacheKey := revisionEqualityKey{setUID: set.UID, setGeneration: set.Generation, revisionResourceVersion: latestExistingRevision.ResourceVersion}
	if _, ok := memory.Get(equalityCacheKey); ok {
		return true
	}
	// 通过实际恢复最新版本并重新构造版本对象来做语义相等性比较。
	latestSet, err := ApplyRevision(set, latestExistingRevision)
	if err != nil {
		return false
	}
	legacyscheme.Scheme.Default(latestSet)
	reconstructedLatestRevision, err := newRevision(latestSet, -1, nil)
	if err != nil {
		return false
	}
	// 命中后写入缓存，稳态下避免重复进行这一昂贵比较。
	if history.EqualRevision(proposedRevision, reconstructedLatestRevision) {
		memory.Add(equalityCacheKey, struct{}{})
		return true
	}
	return false
}

// getStatefulSetRevisions 返回 set 的当前版本与目标版本，以及创建版本时累计的名称冲突次数。
// 检测到更新时可能创建新版本或递增已有版本的 Revision；调用方需保证 revisions 已排序。
func (ssc *defaultStatefulSetControl) getStatefulSetRevisions(
	set *apps.StatefulSet,
	revisions []*apps.ControllerRevision) (*apps.ControllerRevision, *apps.ControllerRevision, int32, error) {
	var currentRevision, updateRevision *apps.ControllerRevision

	revisionCount := len(revisions)
	history.SortControllerRevisions(revisions)

	// 使用局部副本避免直接修改 set.Status。
	var collisionCount int32
	if set.Status.CollisionCount != nil {
		collisionCount = *set.Status.CollisionCount
	}

	// 先按当前 set 构造目标版本，再与历史寻找等价项。
	updateRevision, err := newRevision(set, nextRevision(revisions), &collisionCount)
	if err != nil {
		return nil, nil, collisionCount, err
	}

	// 查找内容等价的已有版本，避免重复创建。
	equalRevisions := history.FindEqualRevisions(revisions, updateRevision)
	equalCount := len(equalRevisions)

	if equalCount > 0 && history.EqualRevision(revisions[revisionCount-1], equalRevisions[equalCount-1]) {
		// 等价版本恰好是最新版本，说明目标版本未变化，直接复用。
		updateRevision = revisions[revisionCount-1]
	} else if equalCount > 0 {
		// 等价版本不是最新的，需回滚：复制该版本并递增 Revision。
		// 使用等价版本的修订号。
		updateRevision, err = ssc.controllerHistory.UpdateControllerRevision(
			equalRevisions[equalCount-1],
			updateRevision.Revision)
		if err != nil {
			return nil, nil, collisionCount, err
		}
	} else if revisionCount > 0 && setMatchesLatestExistingRevision(set, updateRevision, revisions[revisionCount-1], ssc.revisionEqualityCache) {
		updateRevision = revisions[revisionCount-1]
	} else {
		// 找不到等价版本时创建新版本。
		updateRevision, err = ssc.controllerHistory.CreateControllerRevision(set, updateRevision, &collisionCount)
		if err != nil {
			return nil, nil, collisionCount, err
		}
	}

	for i := range revisions {
		if revisions[i].Name == set.Status.CurrentRevision {
			currentRevision = revisions[i]
			break
		}
	}

	// 首次创建时没有当前版本，用目标版本初始化历史。
	if currentRevision == nil {
		currentRevision = updateRevision
	}

	return currentRevision, updateRevision, collisionCount, nil
}

// slowStartBatch 以慢启动方式并发执行 fn：批次从 initialBatchSize 起按成功情况指数增长，
// 任一错误立即停止后续批次，返回成功数与聚合错误。
func slowStartBatch(initialBatchSize int, remaining int, fn func(int) (bool, error)) (int, error) {
	successes := 0
	j := 0
	for batchSize := min(remaining, initialBatchSize); batchSize > 0; batchSize = min(min(2*batchSize, remaining), MaxBatchSize) {
		errCh := make(chan error, batchSize)
		var wg sync.WaitGroup
		wg.Add(batchSize)
		for i := 0; i < batchSize; i++ {
			go func(k int) {
				defer wg.Done()
				// 慢启动模式下无需使用 fn 返回的 shouldExit 标志。
				// 忽略第一个参数，它只与单调更新模式相关。
				if _, err := fn(k); err != nil {
					errCh <- err
				}
			}(j)
			j++
		}
		wg.Wait()
		successes += batchSize - len(errCh)
		close(errCh)
		// 本批出现错误就停止扩大批次，避免对同一错误重复并发请求。
		if len(errCh) > 0 {
			errs := make([]error, 0)
			for err := range errCh {
				errs = append(errs, err)
			}
			return successes, utilerrors.NewAggregate(errs)
		}
		remaining -= batchSize
	}
	return successes, nil
}

// replicaStatus 汇总副本统计结果，供状态更新与并行更新预算计算复用。
type replicaStatus struct {
	replicas          int32
	readyReplicas     int32
	availableReplicas int32
	currentReplicas   int32
	updatedReplicas   int32
}

// computeReplicaStatus 统计一组 Pod 的副本总数、就绪/可用数以及当前/目标版本副本数。
func computeReplicaStatus(pods []*v1.Pod, minReadySeconds int32, currentRevision, updateRevision *apps.ControllerRevision, now time.Time) replicaStatus {
	status := replicaStatus{}
	for _, pod := range pods {
		if isCreated(pod) {
			status.replicas++
		}

		// 就绪副本才能进一步计入可用副本。
		if isRunningAndReady(pod) {
			status.readyReplicas++
			if isRunningAndAvailable(pod, minReadySeconds, now) {
				status.availableReplicas++
			}

		}

		// 版本计数排除正在终止的 Pod，避免把即将消失的副本算入。
		if isCreated(pod) && !isTerminating(pod) {
			revision := getPodRevision(pod)
			if revision == currentRevision.Name {
				status.currentReplicas++
			}
			if revision == updateRevision.Name {
				status.updatedReplicas++
			}
		}
	}
	return status
}

// updateStatus 清空并重新累计状态计数，支持传入多个 Pod 列表（如存活副本与待删除副本）合并统计。
func updateStatus(status *apps.StatefulSetStatus, minReadySeconds int32, currentRevision, updateRevision *apps.ControllerRevision, now time.Time, podLists ...[]*v1.Pod) {
	status.Replicas = 0
	status.ReadyReplicas = 0
	status.AvailableReplicas = 0
	status.CurrentReplicas = 0
	status.UpdatedReplicas = 0
	for _, list := range podLists {
		replicaStatus := computeReplicaStatus(list, minReadySeconds, currentRevision, updateRevision, now)
		status.Replicas += replicaStatus.replicas
		status.ReadyReplicas += replicaStatus.readyReplicas
		status.AvailableReplicas += replicaStatus.availableReplicas
		status.CurrentReplicas += replicaStatus.currentReplicas
		status.UpdatedReplicas += replicaStatus.updatedReplicas
	}
}

// processReplica 处理序号为 i 的存活副本：清理终态 Pod、补建缺失 Pod、补建 PVC、
// 在单调模式下等待前置副本就绪/可用，必要时更新 Pod 以符合版本的身份与存储约束。
func (ssc *defaultStatefulSetControl) processReplica(ctx context.Context, set *apps.StatefulSet, updateSet *apps.StatefulSet, monotonic bool, replicas []*v1.Pod, i int, now time.Time) (bool, error) {
	logger := klog.FromContext(ctx)

	// Succeeded 阶段也需重建：被驱逐或强制停止的 Pod 最终阶段由容器退出码决定，与终止原因无关。
	// 注意 Succeeded 状态的 Pod 也会触发该事件：被驱逐或强制停止的 Pod
	// 最终状态由容器退出码决定，而非终止原因，因此无论退出码如何都应重启 Pod。
	if isTerminalPhase(replicas[i]) {
		if replicas[i].DeletionTimestamp == nil {
			if err := ssc.podControl.DeleteStatefulPod(set, replicas[i]); err != nil {
				return true, err
			}
		}
		// 等当前 Pod 从 etcd 删除后再于下一轮重建，避免同名冲突。
		return true, nil
	}
	// 发现尚未创建的 Pod 时立即创建。
	if !isCreated(replicas[i]) {
		// PVC 过期时本轮不能继续操作该 Pod，否则可能误用旧存储。
		if isStale, err := ssc.podControl.PodClaimIsStale(set, replicas[i]); err != nil {
			return true, err
		} else if isStale {
			// Pod 使用的 PVC 已过期时，本轮无法继续推进。
			return true, err
		}
		if err := ssc.podControl.CreateStatefulPod(ctx, set, replicas[i]); err != nil {
			return true, err
		}
		if monotonic {
			// 单调模式一次只创建一个 Pod，创建后立即返回。
			return true, nil
		}
	}

	// Pending 的 Pod 触发缺失 PVC 的创建，使其能继续推进。
	// Pod 处于 Pending 时触发 PVC 创建，补齐缺失的 PVC。
	if isPending(replicas[i]) {
		logger.V(4).Info(
			"StatefulSet is triggering PVC creation for pending Pod",
			"statefulSet", klog.KObj(set), "pod", klog.KObj(replicas[i]))
		if err := ssc.podControl.createMissingPersistentVolumeClaims(ctx, set, replicas[i]); err != nil {
			return true, err
		}
	}

	// 发现正在终止的 Pod 时必须等待优雅删除完成，才能继续推进。
	if isTerminating(replicas[i]) && monotonic {
		logger.V(4).Info("StatefulSet is waiting for Pod to Terminate",
			"statefulSet", klog.KObj(set), "pod", klog.KObj(replicas[i]))
		return true, nil
	}

	// 单调模式下前置副本必须 Running 且 Ready，否则不能继续。
	// Pod 已创建但尚未 Running/Ready 时无法推进：
	// 必须保证每个 Pod 按序号的前驱都已 Running/Ready。
	if !isRunningAndReady(replicas[i]) && monotonic {
		logger.V(4).Info("StatefulSet is waiting for Pod to be Running and Ready",
			"statefulSet", klog.KObj(set), "pod", klog.KObj(replicas[i]))
		return true, nil
	}

	// 单调模式下前置副本还必须满足 MinReadySeconds 变为 Available。
	// Pod 已创建但不可用时无法推进：
	// 必须保证每个 Pod 按序号的前驱都处于 Available 状态。
	if !isRunningAndAvailable(replicas[i], set.Spec.MinReadySeconds, now) && monotonic {
		logger.V(4).Info("StatefulSet is waiting for Pod to be Available",
			"statefulSet", klog.KObj(set), "pod", klog.KObj(replicas[i]))
		return true, nil
	}

	// 校验 StatefulSet 的各项不变量。
	retentionMatch, err := ssc.podControl.ClaimsMatchRetentionPolicy(ctx, updateSet, replicas[i])
	// Pod 尚未完全更新时检查会报错，此时按匹配处理以免阻塞。
	// Pod 尚未完全更新时预期会出错，此时返回值视为匹配。
	if err != nil {
		retentionMatch = true
	}

	if identityMatches(set, replicas[i]) && storageMatches(set, replicas[i]) && retentionMatch {
		return false, nil
	}

	// 更新前深拷贝，避免修改 informer 缓存中的共享对象。
	// 深拷贝以避免修改共享缓存中的对象。
	replica := replicas[i].DeepCopy()
	if err := ssc.podControl.UpdateStatefulPod(ctx, updateSet, replica); err != nil {
		return true, err
	}

	return false, nil
}

// processCondemned 处理待删除副本 i：单调模式下等待终止完成及不健康的前置副本恢复后，
// 按序号降序终止副本。
func (ssc *defaultStatefulSetControl) processCondemned(ctx context.Context, set *apps.StatefulSet, firstUnhealthyPod *v1.Pod, monotonic bool, condemned []*v1.Pod, i int, now time.Time) (bool, error) {
	logger := klog.FromContext(ctx)
	if isTerminating(condemned[i]) {
		// 单调模式缩容必须等正在终止的 Pod 彻底消失。
		if monotonic {
			logger.V(4).Info("StatefulSet is waiting for Pod to Terminate prior to scale down",
				"statefulSet", klog.KObj(set), "pod", klog.KObj(condemned[i]))
			return true, nil
		}
		return false, nil
	}
	// 单调模式下必须先处理第一个不健康副本，其余副本等待。
	if !isRunningAndReady(condemned[i]) && monotonic && condemned[i] != firstUnhealthyPod {
		logger.V(4).Info("StatefulSet is waiting for Pod to be Running and Ready prior to scale down",
			"statefulSet", klog.KObj(set), "pod", klog.KObj(firstUnhealthyPod))
		return true, nil
	}
	if !isRunningAndAvailable(condemned[i], set.Spec.MinReadySeconds, now) && monotonic && condemned[i] != firstUnhealthyPod {
		logger.V(4).Info("StatefulSet is waiting for Pod to be Available prior to scale down",
			"statefulSet", klog.KObj(set), "pod", klog.KObj(firstUnhealthyPod))
		return true, nil
	}

	logger.V(2).Info("Pod of StatefulSet is terminating for scale down",
		"statefulSet", klog.KObj(set), "pod", klog.KObj(condemned[i]))
	return true, ssc.podControl.DeleteStatefulPod(set, condemned[i])
}

// runForAll 对所有 Pod 执行 fn：单调模式按序执行并在需要时立即返回；
// 非单调模式使用慢启动并发执行。
func runForAll(pods []*v1.Pod, fn func(i int) (bool, error), monotonic bool) (bool, error) {
	// 单调模式严格按序处理，保证稳定性。
	if monotonic {

		for i := range pods {
			if shouldExit, err := fn(i); shouldExit || err != nil {
				return true, err
			}
		}
	} else {
		if _, err := slowStartBatch(1, len(pods), fn); err != nil {
			return true, err
		}
	}
	return false, nil
}

// updateStatefulSet 执行 StatefulSet 的核心更新：增删改 Pod 使系统趋近目标状态。
// 目标状态始终包含 Spec.Replicas 个 Ready 副本；滚动更新要求全部 Pod 处于当前版本，
// OnDelete 不约束版本，Partition 则要求序号小于分区值的 Pod 处于当前版本、其余处于目标版本。
// 错误为 nil 时返回的 Status 有效且必须记录；否则应重试直到成功。
func (ssc *defaultStatefulSetControl) updateStatefulSet(ctx context.Context, set *apps.StatefulSet, currentRevision *apps.ControllerRevision, updateRevision *apps.ControllerRevision, collisionCount int32, pods []*v1.Pod, now time.Time) (*apps.StatefulSetStatus, error) {
	logger := klog.FromContext(ctx)
	currentSet, err := ApplyRevision(set, currentRevision)
	if err != nil {
		return nil, err
	}
	updateSet, err := ApplyRevision(set, updateRevision)
	if err != nil {
		return nil, err
	}

	// Recreate 策略依赖特性开关，关闭时只发警告并返回错误。
	if set.Spec.UpdateStrategy.Type == apps.RecreateStatefulSetStrategyType && !utilfeature.DefaultFeatureGate.Enabled(features.StatefulSetRecreateStrategy) {
		errMsg := fmt.Errorf("statefulset %s/%s uses Recreate strategy but feature gate %s is disabled", set.Namespace, set.Name, features.StatefulSetRecreateStrategy)
		ssc.podControl.recorder.Event(set, v1.EventTypeWarning, "UnknownStrategy", errMsg.Error())
		return nil, errMsg
	}

	// 状态中记录观测到的 generation 与版本信息。
	status := apps.StatefulSetStatus{}
	status.ObservedGeneration = set.Generation
	status.CurrentRevision = currentRevision.Name
	status.UpdateRevision = updateRevision.Name
	status.CollisionCount = new(int32)
	*status.CollisionCount = collisionCount

	status.Conditions = append(status.Conditions, set.Status.Conditions...)

	updateStatus(&status, set.Spec.MinReadySeconds, currentRevision, updateRevision, now, pods)

	replicaCount := int(*set.Spec.Replicas)
	replicas := make([]*v1.Pod, replicaCount)
	condemned := make([]*v1.Pod, 0, len(pods))
	unavailable := 0
	var firstUnavailablePod *v1.Pod

	// 先按序号把 Pod 分成有效副本与待删除副本两类。
	for _, pod := range pods {
		if podInOrdinalRange(pod, set) {
			replicas[getOrdinal(pod)-getStartOrdinal(set)] = pod
		} else if getOrdinal(pod) >= 0 {
			condemned = append(condemned, pod)
		}
		// 序号无法解析（ord < 0）时忽略该 Pod。
	}

	// 为空缺序号构造新 Pod，确保副本连续。
	start, end := getStartOrdinal(set), getEndOrdinal(set)
	for ord := start; ord <= end; ord++ {
		replicaIdx := ord - start
		if replicas[replicaIdx] == nil {
			replicas[replicaIdx] = newVersionedStatefulSetPod(
				currentSet,
				updateSet,
				currentRevision.Name,
				updateRevision.Name, ord)
		}
	}

	// 待删除副本按序号降序排序，便于从大到小终止。
	sort.Sort(descendingOrdinal(condemned))

	// 找出第一个不可用副本，单调更新会在处理它之前阻塞其他副本。
	for i := range replicas {
		if isUnavailable(replicas[i], set.Spec.MinReadySeconds, now) {
			unavailable++
			if firstUnavailablePod == nil {
				firstUnavailablePod = replicas[i]
			}
		}
	}

	for i := len(condemned) - 1; i >= 0; i-- {
		if isUnavailable(condemned[i], set.Spec.MinReadySeconds, now) {
			unavailable++
			if firstUnavailablePod == nil {
				firstUnavailablePod = condemned[i]
			}
		}
	}

	if unavailable > 0 {
		logger.V(4).Info("StatefulSet has unavailable Pods", "statefulSet", klog.KObj(set), "unavailableReplicas", unavailable, "pod", klog.KObj(firstUnavailablePod))
	}

	// 删除中的 StatefulSet 只更新状态，不再操作 Pod。
	// StatefulSet 正在删除时只更新状态，不做其他操作。
	if set.DeletionTimestamp != nil {
		return &status, nil
	}

	monotonic := !allowsBurst(set)

	if set.Spec.UpdateStrategy.Type == apps.RecreateStatefulSetStrategyType {
		if !utilfeature.DefaultFeatureGate.Enabled(features.StatefulSetRecreateStrategy) {
			errMsg := fmt.Errorf("statefulset %s/%s uses Recreate strategy but feature gate %s is disabled", set.Namespace, set.Name, features.StatefulSetRecreateStrategy)
			ssc.podControl.recorder.Event(set, v1.EventTypeWarning, "UnknownStrategy", errMsg.Error())
			return &status, errMsg
		}

		allOldPodsTerminated, err := ssc.recreateDeleteAndWait(ctx, set, updateRevision, replicas, condemned, &status)
		if err != nil {
			updateStatus(&status, set.Spec.MinReadySeconds, currentRevision, updateRevision, now, replicas, condemned)
			return &status, err
		}
		// 仍有旧版本 Pod 未完全终止时直接返回。
		if !allOldPodsTerminated {
			updateStatus(&status, set.Spec.MinReadySeconds, currentRevision, updateRevision, now, replicas, condemned)
			return &status, nil
		}
		if currentRevision.Name != updateRevision.Name {
			setProgressingCondition(&status, v1.ConditionTrue, RecreateInProgressReason, "Waiting for new-revision pods to be created")
		}
	}

	// 先处理存活副本；单调模式下遇到阻塞或错误立即退出后续步骤。
	processReplicaFn := func(i int) (bool, error) {
		return ssc.processReplica(ctx, set, updateSet, monotonic, replicas, i, now)
	}
	if shouldExit, err := runForAll(replicas, processReplicaFn, monotonic); shouldExit || err != nil {
		updateStatus(&status, set.Spec.MinReadySeconds, currentRevision, updateRevision, now, replicas, condemned)
		return &status, err
	}

	// 必要时修正待删除 Pod 的 PVC 属主引用。
	fixPodClaim := func(i int) (bool, error) {
		if matchPolicy, err := ssc.podControl.ClaimsMatchRetentionPolicy(ctx, updateSet, condemned[i]); err != nil {
			return true, err
		} else if !matchPolicy {
			if err := ssc.podControl.UpdatePodClaimForRetentionPolicy(ctx, updateSet, condemned[i]); err != nil {
				return true, err
			}
		}
		return false, nil
	}
	if shouldExit, err := runForAll(condemned, fixPodClaim, monotonic); shouldExit || err != nil {
		updateStatus(&status, set.Spec.MinReadySeconds, currentRevision, updateRevision, now, replicas, condemned)
		return &status, err
	}

	// 走到这里时单调模式下的存活副本均已就绪，可以安全考虑缩容。
	// 此时单调模式下所有当前副本都已 Running、Ready 且 Available，可以考虑终止。
	// 删除前仍需等待所有前驱 Running 且 Available，并按序号递减顺序终止。
	// 该区间不重建 Pod，且缩容优先于更新。
	processCondemnedFn := func(i int) (bool, error) {
		return ssc.processCondemned(ctx, set, firstUnavailablePod, monotonic, condemned, i, now)
	}
	if shouldExit, err := runForAll(condemned, processCondemnedFn, monotonic); shouldExit || err != nil {
		updateStatus(&status, set.Spec.MinReadySeconds, currentRevision, updateRevision, now, replicas, condemned)
		return &status, err
	}

	updateStatus(&status, set.Spec.MinReadySeconds, currentRevision, updateRevision, now, replicas, condemned)

	// OnDelete/Recreate 策略不在此处执行滚动替换。
	if set.Spec.UpdateStrategy.Type == apps.OnDeleteStatefulSetStrategyType || set.Spec.UpdateStrategy.Type == apps.RecreateStatefulSetStrategyType {
		return &status, nil
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.MaxUnavailableStatefulSet) {
		return updateStatefulSetAfterInvariantEstablished(ctx,
			ssc,
			set,
			replicas,
			updateRevision,
			status,
			now,
		)
	}

	// 根据分区配置计算允许破坏性更新的最小序号。
	updateMin := 0
	if set.Spec.UpdateStrategy.RollingUpdate != nil {
		updateMin = int(*set.Spec.UpdateStrategy.RollingUpdate.Partition)
	}
	// 终止序号最大且与更新版本不一致的 Pod。
	for target := len(replicas) - 1; target >= updateMin; target-- {

		if getPodRevision(replicas[target]) != updateRevision.Name && !isTerminating(replicas[target]) {
			logger.V(2).Info("Pod of StatefulSet is terminating for update",
				"statefulSet", klog.KObj(set), "pod", klog.KObj(replicas[target]))
			if err := ssc.podControl.DeleteStatefulPod(set, replicas[target]); err != nil {
				return &status, err
			}
			status.CurrentReplicas--
			return &status, err
		}

		if isUnavailable(replicas[target], set.Spec.MinReadySeconds, now) {
			logger.V(4).Info("StatefulSet is waiting for Pod to update",
				"statefulSet", klog.KObj(set), "pod", klog.KObj(replicas[target]))
			return &status, nil
		}

	}
	return &status, nil
}

// recreateDeleteAndWait 处理 Recreate 策略的前两个阶段：删除所有非目标版本的 Pod，然后等待其彻底消失。
// 旧版本 Pod 全部清除后返回 true，创建阶段交由公共调和循环继续；仍在删除中则返回 false。
func (ssc *defaultStatefulSetControl) recreateDeleteAndWait(ctx context.Context, set *apps.StatefulSet, updateRevision *apps.ControllerRevision, replicas, condemned []*v1.Pod, status *apps.StatefulSetStatus) (bool, error) {
	logger := klog.FromContext(ctx)

	allPods := slices.Concat(replicas, condemned)

	var recreateInProgress atomic.Bool
	// Recreate 需要并行删除所有旧副本，因此使用非单调模式。
	processRecreateFn := func(i int) (bool, error) {
		if allPods[i] == nil || !isCreated(allPods[i]) {
			return false, nil
		}
		// 只要还存在旧版本 Pod，Recreate 就尚未完成。
		if getPodRevision(allPods[i]) != updateRevision.Name {
			recreateInProgress.Store(true)

			if isTerminating(allPods[i]) {
				return false, nil
			}
			logger.V(4).Info("Recreate: deleting old revision pod", "statefulSet", klog.KObj(set), "pod", klog.KObj(allPods[i]))
			if err := ssc.podControl.DeleteStatefulPod(set, allPods[i]); err != nil {
				if !apierrors.IsNotFound(err) {
					return false, err
				}
			}
		}
		return true, nil
	}

	if _, err := runForAll(allPods, processRecreateFn, false); err != nil {
		return false, err
	}

	if recreateInProgress.Load() {
		setProgressingCondition(status, v1.ConditionTrue, RecreateInProgressReason, "Deleting old revision pods")
		return false, nil
	}

	return true, nil
}

// updateStatefulSetAfterInvariantEstablished 在副本不变量满足后按 maxUnavailable 预算执行滚动更新；
// Parallel 模式采用两阶段删除，避免已有不可用副本时耗尽预算造成过大中断。
func updateStatefulSetAfterInvariantEstablished(ctx context.Context, ssc *defaultStatefulSetControl, set *apps.StatefulSet, replicas []*v1.Pod, updateRevision *apps.ControllerRevision, status apps.StatefulSetStatus, now time.Time) (*apps.StatefulSetStatus, error) {

	logger := klog.FromContext(ctx)
	replicaCount := int(*set.Spec.Replicas)
	podManagementPolicy := string(set.Spec.PodManagementPolicy)
	// 根据策略计算破坏性更新目标序列中的最小序号。
	updateMin := 0
	maxUnavailable := 1
	if set.Spec.UpdateStrategy.RollingUpdate != nil {
		updateMin = int(*set.Spec.UpdateStrategy.RollingUpdate.Partition)

		// 特性曾被关闭时字段可能残留大于 1 的值，强制回退为 1 以保持关闭时的预期行为。
		var err error
		maxUnavailable, err = getStatefulSetMaxUnavailable(set.Spec.UpdateStrategy.RollingUpdate.MaxUnavailable, replicaCount)
		if err != nil {
			return &status, err
		}
	}

	// 统计目标区间内的不可用副本，按序号顺序挑选可额外删除的 Pod，
	// 删除后把成功数计入状态。
	unavailablePods := 0
	unavailablePodsNeedingUpdate := 0
	for target := len(replicas) - 1; target >= 0; target-- {
		pod := replicas[target]
		if isUnavailable(pod, set.Spec.MinReadySeconds, now) {
			unavailablePods++
			if set.Spec.PodManagementPolicy == apps.ParallelPodManagement &&
				getPodRevision(pod) != updateRevision.Name &&
				!isTerminating(pod) {
				unavailablePodsNeedingUpdate++
			}
		}
	}
	metrics.UnavailableReplicas.WithLabelValues(set.Namespace, set.Name, podManagementPolicy).Set(float64(unavailablePods))

	// 仅在超出预算且 Parallel 模式下没有可直接替换的旧副本时才短路。
	// Parallel 模式下无可推进的更新。
	if unavailablePods >= maxUnavailable && unavailablePodsNeedingUpdate == 0 {
		if unavailablePods > maxUnavailable {
			logger.V(4).Info("StatefulSet found unavailablePods, more than the allowed maxUnavailable",
				"statefulSet", klog.KObj(set),
				"unavailablePods", unavailablePods,
				"unavailablePodsNeedingUpdate", unavailablePodsNeedingUpdate,
				"maxUnavailable", maxUnavailable)
		}
		return &status, nil
	}

	if set.Spec.PodManagementPolicy == apps.ParallelPodManagement {
		// Parallel 模式两阶段删除：先从高到低清理已不可用的旧副本，再按剩余预算删除可用副本；
		// 单阶段循环会把预算全花在健康副本上，导致实际中断远超 maxUnavailable。
		for target := len(replicas) - 1; target >= updateMin; target-- {
			pod := replicas[target]
			if getPodRevision(pod) != updateRevision.Name &&
				!isRunningAndAvailable(pod, set.Spec.MinReadySeconds, now) &&
				!isTerminating(pod) {
				logger.V(2).Info("StatefulSet terminating unavailable Pod for update",
					"statefulSet", klog.KObj(set), "pod", klog.KObj(pod))
				if err := ssc.podControl.DeleteStatefulPod(set, pod); err != nil {
					return &status, err
				}
				status.CurrentReplicas--
			}
		}

		remainingBudget := maxUnavailable - unavailablePods
		for target := len(replicas) - 1; target >= updateMin && remainingBudget > 0; target-- {
			pod := replicas[target]
			if getPodRevision(pod) != updateRevision.Name &&
				!isUnavailable(pod, set.Spec.MinReadySeconds, now) {
				logger.V(2).Info("StatefulSet terminating Pod for update",
					"statefulSet", klog.KObj(set), "pod", klog.KObj(pod))
				if err := ssc.podControl.DeleteStatefulPod(set, pod); err != nil {
					return &status, err
				}
				remainingBudget--
				status.CurrentReplicas--
			}
		}
		return &status, nil
	}

	// OrderedReady 模式沿用原有单阶段从高到低的替换逻辑。
	podsToDelete := maxUnavailable - unavailablePods
	deletedPods := 0
	for target := len(replicas) - 1; target >= updateMin && deletedPods < podsToDelete; target-- {
		if getPodRevision(replicas[target]) != updateRevision.Name && !isTerminating(replicas[target]) {
			logger.V(2).Info("StatefulSet terminating Pod for update",
				"statefulSet", klog.KObj(set),
				"pod", klog.KObj(replicas[target]))
			if err := ssc.podControl.DeleteStatefulPod(set, replicas[target]); err != nil {
				return &status, err
			}
			deletedPods++
			status.CurrentReplicas--
		}
	}
	return &status, nil
}

// updateStatefulSetStatus 把 status 写回 set；更新完成时补齐完成标记，
// 若状态语义等价则跳过写入以避免无谓的 API 请求。
func (ssc *defaultStatefulSetControl) updateStatefulSetStatus(
	ctx context.Context,
	set *apps.StatefulSet,
	status *apps.StatefulSetStatus) error {
	// 必要时把仍在进行的更新标记为完成。
	completeUpdate(set, status)

	if utilfeature.DefaultFeatureGate.Enabled(features.MaxUnavailableStatefulSet) {
		// 无论更新策略如何都刷新 maxUnavailable 指标。
		// 无论采用何种更新策略都刷新指标。
		podManagementPolicy := string(set.Spec.PodManagementPolicy)
		replicaCount := int(*set.Spec.Replicas)

		var err error
		maxUnavailable := 1
		if set.Spec.UpdateStrategy.RollingUpdate != nil {
			maxUnavailable, err = getStatefulSetMaxUnavailable(set.Spec.UpdateStrategy.RollingUpdate.MaxUnavailable, replicaCount)
			if err != nil {
				return err
			}
		}
		metrics.MaxUnavailable.WithLabelValues(set.Namespace, set.Name, podManagementPolicy).Set(float64(maxUnavailable))
	}
	// 状态没有实质变化时直接返回，避免频繁写 API。
	if !inconsistentStatus(set, status) {
		return nil
	}

	set = set.DeepCopy()
	if err := ssc.statusUpdater.UpdateStatefulSetStatus(ctx, set, status); err != nil {
		return err
	}

	return nil
}

// 编译期断言：默认实现满足控制接口。
var _ StatefulSetControlInterface = &defaultStatefulSetControl{}
