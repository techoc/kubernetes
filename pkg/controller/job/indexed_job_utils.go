/*
Copyright 2021 The Kubernetes Authors.

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

package job

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	batch "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller"
)

// completionIndexEnvName 是注入到 Pod 容器中、用于暴露当前 Pod 完成索引的环境变量名。
// unknownCompletionIndex 表示 Pod 没有有效完成索引（缺失、非数字或负数）时的哨兵值。
const (
	completionIndexEnvName = "JOB_COMPLETION_INDEX"
	unknownCompletionIndex = -1
)

// isIndexedJob 判断 Job 是否使用 Indexed 完成模式。
//   - CompletionMode 为 nil 时按旧行为视为 NonIndexed，返回 false；
//   - 仅当显式设置为 Indexed 时返回 true。
func isIndexedJob(job *batch.Job) bool {
	return job.Spec.CompletionMode != nil && *job.Spec.CompletionMode == batch.IndexedCompletion
}

// hasBackoffLimitPerIndex 判断 Job 是否启用了“按索引重试”特性（BackoffLimitPerIndex）。
// 该配置是否存在决定了许多索引级失败处理逻辑是否生效。
func hasBackoffLimitPerIndex(job *batch.Job) bool {
	return job.Spec.BackoffLimitPerIndex != nil
}

// interval 表示一段连续的完成索引区间 [First, Last]（两端均包含）。
// 用区间而非逐个索引存储，可在索引数量很大时压缩状态体积。
type interval struct {
	// First 是区间的起始索引（包含）。
	First int
	// Last 是区间的结束索引（包含）。
	Last int
}

// orderedIntervals 是按 First 升序排列、且互不重叠的区间列表。
// 本文件中的多数操作都依赖该有序性，例如二分查找 has 与归并 merge。
type orderedIntervals []interval

// calculateSucceededIndexes 返回压缩格式（区间）表示的成功索引旧列表与新列表：
//   - 旧列表仅依据 .status.completedIndexes 解析得到；若该 Job 未通过 finalizer 跟踪，则返回空列表；
//   - 新列表在旧列表基础上并入自上次同步以来新成功的索引。
func calculateSucceededIndexes(logger klog.Logger, job *batch.Job, pods []*v1.Pod) (orderedIntervals, orderedIntervals) {
	prevIntervals := parseIndexesFromString(logger, job.Status.CompletedIndexes, int(*job.Spec.Completions))
	// newSucceeded 收集本次同步中“已成功但尚未计入状态”的索引，用集合去重。
	newSucceeded := sets.New[int]()
	for _, p := range pods {
		ix := getCompletionIndex(p.Annotations)
		// 此类 Pod 需同时满足：成功、索引有效且小于 Completions、并带有 tracking finalizer。
		// 带 finalizer 说明它尚未被计入状态，正是本次要新增的部分。
		if p.Status.Phase == v1.PodSucceeded && ix != unknownCompletionIndex && ix < int(*job.Spec.Completions) && hasJobTrackingFinalizer(p) {
			newSucceeded.Insert(ix)
		}
	}
	// sets.List 返回有序切片，保证 withOrderedIndexes 收到的索引递增。
	result := prevIntervals.withOrderedIndexes(sets.List(newSucceeded))
	return prevIntervals, result
}

// calculateFailedIndexes 以压缩区间格式返回失败索引列表。
// 结果包含 .status.failedIndexes 中已有的索引，以及自上次同步以来新失败的索引。
func calculateFailedIndexes(logger klog.Logger, job *batch.Job, pods []*v1.Pod) *orderedIntervals {
	// prevIntervals 在未设置 .status.failedIndexes 时保持为 nil，表示尚无历史失败记录。
	var prevIntervals orderedIntervals
	if job.Status.FailedIndexes != nil {
		prevIntervals = parseIndexesFromString(logger, *job.Status.FailedIndexes, int(*job.Spec.Completions))
	}
	// newFailed 收集本次同步中“已失败但尚未计入状态”的索引，用集合去重。
	newFailed := sets.New[int]()
	for _, p := range pods {
		ix := getCompletionIndex(p.Annotations)
		// 带有 tracking finalizer 的失败 Pod 才尚未被计入状态，需在此处纳入。
		if ix != unknownCompletionIndex && ix < int(*job.Spec.Completions) && hasJobTrackingFinalizer(p) && isIndexFailed(logger, job, p) {
			newFailed.Insert(ix)
		}
	}
	// sets.List 返回有序切片，保证 withOrderedIndexes 收到的索引递增。
	result := prevIntervals.withOrderedIndexes(sets.List(newFailed))
	return &result
}

// isIndexFailed 判断某个 Pod 的失败是否足以将其完成索引标记为失败。
//   - 若配置了 PodFailurePolicy，且策略动作为 FailIndex，则直接判定索引失败；
//   - 否则要求该 Pod 的失败被计入，并且该索引的失败次数达到 BackoffLimitPerIndex。
func isIndexFailed(logger klog.Logger, job *batch.Job, pod *v1.Pod) bool {
	// isPodFailedCounted 记录该 Pod 的失败是否计入索引失败次数（受 PodFailurePolicy 影响）。
	isPodFailedCounted := false
	if isPodFailed(pod, job) {
		if job.Spec.PodFailurePolicy != nil {
			_, countFailed, action := matchPodFailurePolicy(job.Spec.PodFailurePolicy, pod)
			// FailIndex 是不可恢复的终态动作，一旦命中即判定索引失败。
			if action != nil && *action == batch.PodFailurePolicyActionFailIndex {
				return true
			}
			isPodFailedCounted = countFailed
		} else {
			// 未配置 PodFailurePolicy 时，所有失败 Pod 都计入索引失败次数。
			isPodFailedCounted = true
		}
	}
	return isPodFailedCounted && getIndexFailureCount(logger, pod) >= *job.Spec.BackoffLimitPerIndex
}

// withOrderedIndexes 将新索引并入区间列表；入参 newIndexes 必须递增。
// 每个索引先转成单点区间，再与既有区间归并。
func (oi orderedIntervals) withOrderedIndexes(newIndexes []int) orderedIntervals {
	newIndexIntervals := make(orderedIntervals, len(newIndexes))
	for i, newIndex := range newIndexes {
		newIndexIntervals[i] = interval{newIndex, newIndex}
	}
	return oi.merge(newIndexIntervals)
}

// merge 将 newOi 归并到当前区间列表中，返回新的有序区间列表。
// 采用双指针遍历两个有序列表，保证整体 O(n) 复杂度。
func (oi orderedIntervals) merge(newOi orderedIntervals) orderedIntervals {
	// result 按遍历顺序追加区间，天然保持有序。
	var result orderedIntervals
	i := 0
	j := 0
	// lastInterval 指向 result 末尾区间，用于判断新区间能否与其合并。
	var lastInterval *interval
	// appendOrMergeWithLastInterval 将区间追加到结果：
	//   - 若与末尾区间相邻或重叠，则合并（扩展末尾区间的 Last）；
	//   - 否则作为新区间追加。
	appendOrMergeWithLastInterval := func(thisInterval interval) {
		// First 大于末尾区间 Last+1 时不相邻，必须另起一个区间。
		if lastInterval == nil || thisInterval.First > lastInterval.Last+1 {
			result = append(result, thisInterval)
			lastInterval = &result[len(result)-1]
			// 相邻或重叠时仅扩展末尾区间的右边界；取较大值避免缩小区间。
		} else if lastInterval.Last < thisInterval.Last {
			lastInterval.Last = thisInterval.Last
		}
	}
	for i < len(oi) && j < len(newOi) {
		if oi[i].First < newOi[j].First {
			appendOrMergeWithLastInterval(oi[i])
			i++
		} else {
			appendOrMergeWithLastInterval(newOi[j])
			j++
		}
	}
	for i < len(oi) {
		appendOrMergeWithLastInterval(oi[i])
		i++
	}
	for j < len(newOi) {
		appendOrMergeWithLastInterval(newOi[j])
		j++
	}
	return result
}

// total 返回所有区间覆盖的索引总数，用于快速得到已完成/失败索引的数量。
func (oi orderedIntervals) total() int {
	var count int
	for _, iv := range oi {
		count += iv.Last - iv.First + 1
	}
	return count
}

// String 将区间列表还原为紧凑字符串表示（如 "0,2-4"），与状态字段格式一致。
func (oi orderedIntervals) String() string {
	var builder strings.Builder
	for _, v := range oi {
		if builder.Len() > 0 {
			builder.WriteRune(',')
		}
		builder.WriteString(strconv.Itoa(v.First))
		if v.Last > v.First {
			// 相邻两个数之间用逗号分隔，跳跃区间用连字符表示，以最小化字符串长度。
			if v.Last == v.First+1 {
				builder.WriteRune(',')
			} else {
				builder.WriteRune('-')
			}
			builder.WriteString(strconv.Itoa(v.Last))
		}
	}
	return builder.String()
}

// has 使用二分查找判断索引 ix 是否被任一区间覆盖。
func (oi orderedIntervals) has(ix int) bool {
	lo := 0
	hi := len(oi)
	// 循环不变式：区间 oi[hi].Last（若 hi 有效）大于等于 ix；hi 为第一个满足该条件的下标。
	for hi > lo {
		mid := lo + (hi-lo)/2
		if oi[mid].Last >= ix {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	// hi 等于长度说明所有区间的 Last 都小于 ix，必然不覆盖。
	if hi == len(oi) {
		return false
	}
	return oi[hi].First <= ix
}

// parseIndexesFromString 将状态中的紧凑索引字符串解析为有序区间列表。
//   - 无法解析的区间会被忽略并记录日志；
//   - 超出 completions 的区间在此处截断或停止解析，避免状态中的越界索引影响调度。
func parseIndexesFromString(logger klog.Logger, indexesStr string, completions int) orderedIntervals {
	// 空字符串表示尚无状态记录，返回 nil。
	if indexesStr == "" {
		return nil
	}
	var result orderedIntervals
	var lastInterval *interval
	for _, intervalStr := range strings.Split(indexesStr, ",") {
		limitsStr := strings.Split(intervalStr, "-")
		var inter interval
		var err error
		inter.First, err = strconv.Atoi(limitsStr[0])
		if err != nil {
			logger.Info("Corrupted indexes interval, ignoring", "interval", intervalStr, "err", err)
			continue
		}
		// 起始索引已越界：后续区间起点更大，也必然越界，直接停止解析。
		if inter.First >= completions {
			break
		}
		if len(limitsStr) > 1 {
			inter.Last, err = strconv.Atoi(limitsStr[1])
			if err != nil {
				logger.Info("Corrupted indexes interval, ignoring", "interval", intervalStr, "err", err)
				continue
			}
			// 结束索引越界时截断到最后一个合法索引，保留区间内的有效部分。
			if inter.Last >= completions {
				inter.Last = completions - 1
			}
		} else {
			inter.Last = inter.First
		}
		// 与前一区间相邻时合并，保持区间列表紧凑且互不相邻。
		if lastInterval != nil && lastInterval.Last == inter.First-1 {
			lastInterval.Last = inter.Last
		} else {
			result = append(result, inter)
			lastInterval = &result[len(result)-1]
		}
	}
	return result
}

// firstPendingIndexes 返回前 count 个未被占用且小于 completions 的索引。
// “已占用”包括：活跃 Pod、已成功索引、已失败索引；
// 当 PodReplacementPolicy 为 Failed 时，终止中的 Pod 所占索引也计入。
func firstPendingIndexes(jobCtx *syncJobCtx, count, completions int) []int {
	// count 为 0 时无需分配，直接返回。
	if count == 0 {
		return nil
	}
	// active 收集已有活跃 Pod 占用的索引。
	active := getIndexes(jobCtx.activePods)
	result := make([]int, 0, count)
	// nonPending 汇总所有“不应再分配新 Pod”的索引，后续在区间间隙中挑选候选。
	nonPending := jobCtx.succeededIndexes.withOrderedIndexes(sets.List(active))
	// Failed 替换策略下，终止中的 Pod 仍占着索引，需一并排除以免重复分配。
	if onlyReplaceFailedPods(jobCtx.job) {
		terminating := getIndexes(controller.FilterTerminatingPods(jobCtx.pods))
		nonPending = nonPending.withOrderedIndexes(sets.List(terminating))
	}
	if jobCtx.failedIndexes != nil {
		nonPending = nonPending.merge(*jobCtx.failedIndexes)
	}
	// 算法复杂度受 nonPending 区间数与 count 限制：只在区间间隙及尾部填充候选索引。
	candidate := 0
	for _, sInterval := range nonPending {
		for ; candidate < completions && len(result) < count && candidate < sInterval.First; candidate++ {
			result = append(result, candidate)
		}
		if candidate < sInterval.Last+1 {
			candidate = sInterval.Last + 1
		}
	}
	for ; candidate < completions && len(result) < count; candidate++ {
		result = append(result, candidate)
	}
	return result
}

// getIndexes 返回给定 Pod 集合中所有有效完成索引的集合。
// 无完成索引的 Pod 不会贡献索引。
func getIndexes(pods []*v1.Pod) sets.Set[int] {
	result := sets.New[int]()
	for _, p := range pods {
		ix := getCompletionIndex(p.Annotations)
		if ix != unknownCompletionIndex {
			result.Insert(ix)
		}
	}
	return result
}

// appendDuplicatedIndexPodsForRemoval 扫描活跃 Pod 中的重复完成索引。
//   - 对每个索引，保留一个 Pod 放入 left，其余 n-1 个放入 rm 待删除；
//   - 无完成索引或索引越界的 Pod 全部放入 rm。
func appendDuplicatedIndexPodsForRemoval(rm, left, pods []*v1.Pod, completions int) ([]*v1.Pod, []*v1.Pod) {
	// 先按完成索引排序，使相同索引的 Pod 相邻，便于分组处理。
	sort.Sort(byCompletionIndex(pods))
	lastIndex := unknownCompletionIndex
	firstRepeatPos := 0
	countLooped := 0
	for i, p := range pods {
		ix := getCompletionIndex(p.Annotations)
		// 索引越界（含无索引，-1 >= completions 不成立但无索引在后面按 unknown 分组）的 Pod 从当前位置起全部待删除。
		if ix >= completions {
			rm = append(rm, pods[i:]...)
			break
		}
		// 遇到新索引时，处理上一组相同索引的 Pod。
		if ix != lastIndex {
			rm, left = appendPodsWithSameIndexForRemovalAndRemaining(rm, left, pods[firstRepeatPos:i], lastIndex)
			firstRepeatPos = i
			lastIndex = ix
		}
		countLooped += 1
	}
	return appendPodsWithSameIndexForRemovalAndRemaining(rm, left, pods[firstRepeatPos:countLooped], lastIndex)
}

// getPodsWithDelayedDeletionPerIndex 返回每个索引中“延迟删除”的失败 Pod。
// 启用 BackoffLimitPerIndex 时，需等替换 Pod 创建并携带正确的失败计数注解后，才移除旧 Pod 的 finalizer；
// 因此这里选中每个索引中计数最大、完成时间最晚的失败 Pod 作为延迟删除对象。
func getPodsWithDelayedDeletionPerIndex(logger klog.Logger, jobCtx *syncJobCtx) map[int]*v1.Pod {
	// 当前有活跃 Pod 的索引无需延迟：失败计数注解已在活跃 Pod 上，可安全删除旧 Pod。
	activeIndexes := getIndexes(jobCtx.activePods)

	podsWithDelayedDeletionPerIndex := make(map[int]*v1.Pod)
	// 借助 getValidPodsWithFilter 遍历所有合法 Pod，按条件筛选每个索引的候选。
	getValidPodsWithFilter(jobCtx, nil, func(p *v1.Pod) bool {
		// 只关注失败 Pod。
		if isPodFailed(p, jobCtx.job) {
			if ix := getCompletionIndex(p.Annotations); ix != unknownCompletionIndex && ix < int(*jobCtx.job.Spec.Completions) {
				// 该索引已成功/已失败或仍有活跃 Pod，无需延迟删除。
				if jobCtx.succeededIndexes.has(ix) || jobCtx.failedIndexes.has(ix) || activeIndexes.Has(ix) {
					return false
				}
				// 同一索引已有候选时，保留失败计数更大、完成时间更晚的那个。
				if lastPodWithDelayedDeletion, ok := podsWithDelayedDeletionPerIndex[ix]; ok {
					// 计数相等时以完成时间更新；计数更大时也替换候选。
					if getIndexAbsoluteFailureCount(logger, lastPodWithDelayedDeletion) <= getIndexAbsoluteFailureCount(logger, p) && !getFinishedTime(p).Before(getFinishedTime(lastPodWithDelayedDeletion)) {
						podsWithDelayedDeletionPerIndex[ix] = p
					}
				} else {
					podsWithDelayedDeletionPerIndex[ix] = p
				}
			}
		}
		return false
	})
	return podsWithDelayedDeletionPerIndex
}

// addIndexFailureCountAnnotation 将新的索引失败计数写入待创建 Pod 的模板注解。
// ignored 计数仅在大于 0 时写入，避免产生无意义的零值注解。
func addIndexFailureCountAnnotation(logger klog.Logger, template *v1.PodTemplateSpec, job *batch.Job, podBeingReplaced *v1.Pod) {
	indexFailureCount, indexIgnoredFailureCount := getNewIndexFailureCounts(logger, job, podBeingReplaced)
	template.Annotations[batch.JobIndexFailureCountAnnotation] = strconv.Itoa(int(indexFailureCount))
	if indexIgnoredFailureCount > 0 {
		template.Annotations[batch.JobIndexIgnoredFailureCountAnnotation] = strconv.Itoa(int(indexIgnoredFailureCount))
	}
}

// getNewIndexFailureCounts 计算替换 Pod 应携带的索引失败计数与被忽略失败计数。
//   - 无被替换 Pod 时，两个计数均从 0 开始；
//   - 有被替换 Pod 时继承其计数，并依据 PodFailurePolicy 决定递增哪一项。
func getNewIndexFailureCounts(logger klog.Logger, job *batch.Job, podBeingReplaced *v1.Pod) (int32, int32) {
	if podBeingReplaced != nil {
		// 继承被替换 Pod 上已有的两项计数。
		indexFailureCount := parseIndexFailureCountAnnotation(logger, podBeingReplaced)
		indexIgnoredFailureCount := parseIndexFailureIgnoreCountAnnotation(logger, podBeingReplaced)
		// 配置策略时，按策略结果决定计入失败还是忽略，避免重复计数。
		if job.Spec.PodFailurePolicy != nil {
			_, countFailed, _ := matchPodFailurePolicy(job.Spec.PodFailurePolicy, podBeingReplaced)
			if countFailed {
				indexFailureCount++
			} else {
				indexIgnoredFailureCount++
			}
		} else {
			// 无策略时所有失败都计入失败次数。
			indexFailureCount++
		}
		return indexFailureCount, indexIgnoredFailureCount
	}
	return 0, 0
}

// appendPodsWithSameIndexForRemovalAndRemaining 处理同一索引的一组 Pod。
//   - 索引无效时全部放入 rm；
//   - 仅一个 Pod 时保留在 left；
//   - 多个 Pod 时按活跃时间排序，保留最新的一个，其余放入 rm。
func appendPodsWithSameIndexForRemovalAndRemaining(rm, left, pods []*v1.Pod, ix int) ([]*v1.Pod, []*v1.Pod) {
	// 无有效索引的 Pod 一律待删除。
	if ix == unknownCompletionIndex {
		rm = append(rm, pods...)
		return rm, left
	}
	// 只有一个 Pod 时无需去重，直接保留。
	if len(pods) == 1 {
		left = append(left, pods[0])
		return rm, left
	}
	// 按 ActivePods 排序后保留最新 Pod，删除较旧的重复 Pod。
	sort.Sort(controller.ActivePods(pods))
	rm = append(rm, pods[:len(pods)-1]...)
	left = append(left, pods[len(pods)-1])
	return rm, left
}

// getCompletionIndex 从 Pod 注解中解析完成索引。
//   - 注解缺失、值为非数字或为负数时，返回 unknownCompletionIndex。
func getCompletionIndex(annotations map[string]string) int {
	if annotations == nil {
		return unknownCompletionIndex
	}
	v, ok := annotations[batch.JobCompletionIndexAnnotation]
	if !ok {
		return unknownCompletionIndex
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return unknownCompletionIndex
	}
	if i < 0 {
		return unknownCompletionIndex
	}
	return i
}

// getIndexFailureCount 读取注解中的索引失败次数，该注解在以下情况回退为 0：
//   - 注解不存在（如特性曾被关闭，或被用户手动删除）；
//   - 注解值无法解析为整数（如被恶意用户篡改）；
//   - 注解值为负数或超过 int32 范围。
func getIndexFailureCount(logger klog.Logger, pod *v1.Pod) int32 {
	return parseIndexFailureCountAnnotation(logger, pod)
}

// getIndexAbsoluteFailureCount 返回索引的绝对失败次数：计入失败与忽略失败之和。
// 用于比较候选 Pod 的失败总量，与仅用于判定索引失败的 getIndexFailureCount 不同。
func getIndexAbsoluteFailureCount(logger klog.Logger, pod *v1.Pod) int32 {
	return parseIndexFailureCountAnnotation(logger, pod) + parseIndexFailureIgnoreCountAnnotation(logger, pod)
}

// parseIndexFailureCountAnnotation 读取并解析索引失败计数注解，缺失时记录日志并返回 0。
func parseIndexFailureCountAnnotation(logger klog.Logger, pod *v1.Pod) int32 {
	if value, ok := pod.Annotations[batch.JobIndexFailureCountAnnotation]; ok {
		return parseInt32(logger, value)
	}
	logger.V(3).Info("There is no expected annotation", "annotationKey", batch.JobIndexFailureCountAnnotation, "pod", klog.KObj(pod), "podUID", pod.UID)
	return 0
}

// parseIndexFailureIgnoreCountAnnotation 读取被忽略失败计数注解，缺失时返回 0。
func parseIndexFailureIgnoreCountAnnotation(logger klog.Logger, pod *v1.Pod) int32 {
	if value, ok := pod.Annotations[batch.JobIndexIgnoredFailureCountAnnotation]; ok {
		return parseInt32(logger, value)
	}
	return 0
}

// parseInt32 将字符串安全解析为 int32。
//   - 解析失败或值越界时返回 0，并记录日志，防止恶意注解影响控制逻辑。
func parseInt32(logger klog.Logger, vStr string) int32 {
	if vInt, err := strconv.Atoi(vStr); err != nil {
		logger.Error(err, "Failed to parse the value", "value", vStr)
		return 0
	} else if vInt < 0 || vInt > math.MaxInt32 {
		logger.Info("The value is invalid", "value", vInt)
		return 0
	} else {
		return int32(vInt)
	}
}

// addCompletionIndexEnvVariables 为模板中所有 init 容器与普通容器注入完成索引环境变量。
func addCompletionIndexEnvVariables(template *v1.PodTemplateSpec) {
	for i := range template.Spec.InitContainers {
		addCompletionIndexEnvVariable(&template.Spec.InitContainers[i])
	}
	for i := range template.Spec.Containers {
		addCompletionIndexEnvVariable(&template.Spec.Containers[i])
	}
}

// addCompletionIndexEnvVariable 为单个容器注入 JOB_COMPLETION_INDEX 环境变量。
// 若容器已自行声明同名变量，则保持用户定义不覆盖。
func addCompletionIndexEnvVariable(container *v1.Container) {
	// 用户显式定义的同名变量优先，避免覆盖用户配置。
	for _, v := range container.Env {
		if v.Name == completionIndexEnvName {
			return
		}
	}
	fieldPath := fmt.Sprintf("metadata.labels['%s']", batch.JobCompletionIndexAnnotation)
	container.Env = append(container.Env, v1.EnvVar{
		Name: completionIndexEnvName,
		ValueFrom: &v1.EnvVarSource{
			FieldRef: &v1.ObjectFieldSelector{
				FieldPath: fieldPath,
			},
		},
	})
}

// addCompletionIndexAnnotation 将完成索引写入模板注解，必要时先初始化 map。
func addCompletionIndexAnnotation(template *v1.PodTemplateSpec, index int) {
	if template.Annotations == nil {
		template.Annotations = make(map[string]string, 1)
	}
	template.Annotations[batch.JobCompletionIndexAnnotation] = strconv.Itoa(index)
}

// addCompletionIndexLabel 将完成索引写入模板标签，必要时先初始化 map。
// 标签键与注解保持一致，便于通过标签选择对应索引的 Pod。
func addCompletionIndexLabel(template *v1.PodTemplateSpec, index int) {
	if template.Labels == nil {
		template.Labels = make(map[string]string, 1)
	}
	// 与注解使用相同的键名，确保 Pod 上标签和注解值一致。
	template.Labels[batch.JobCompletionIndexAnnotation] = strconv.Itoa(index)
}

// podGenerateNameWithIndex 为带索引的 Pod 生成 name 前缀。
// 当 Job 名过长时截断前缀，但保留 "-<index>-" 后缀，确保生成的名字仍含索引且符合长度限制。
func podGenerateNameWithIndex(jobName string, index int) string {
	appendIndex := "-" + strconv.Itoa(index) + "-"
	generateNamePrefix := jobName + appendIndex
	// 截断时保留索引后缀，避免不同索引的 Pod 生成相同前缀。
	if len(generateNamePrefix) > names.MaxGeneratedNameLength {
		generateNamePrefix = generateNamePrefix[:names.MaxGeneratedNameLength-len(appendIndex)] + appendIndex
	}
	return generateNamePrefix
}

// byCompletionIndex 按完成索引对 Pod 排序，用于重复索引检测。
type byCompletionIndex []*v1.Pod

// Less 按完成索引升序比较两个 Pod。
func (bci byCompletionIndex) Less(i, j int) bool {
	return getCompletionIndex(bci[i].Annotations) < getCompletionIndex(bci[j].Annotations)
}

// Swap 交换两个 Pod 的位置。
func (bci byCompletionIndex) Swap(i, j int) {
	bci[i], bci[j] = bci[j], bci[i]
}

// Len 返回待排序 Pod 的数量。
func (bci byCompletionIndex) Len() int {
	return len(bci)
}

// completionModeStr 返回 Job 完成模式的字符串表示；未设置时按 NonIndexed 处理。
func completionModeStr(job *batch.Job) string {
	if job.Spec.CompletionMode != nil {
		return string(*job.Spec.CompletionMode)
	}
	return string(batch.NonIndexedCompletion)
}
