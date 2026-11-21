/*
Copyright The Kubernetes Authors.

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
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"

	batch "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apiserver/pkg/util/feature"
	schedulinginformers "k8s.io/client-go/informers/scheduling/v1beta1"
	"k8s.io/client-go/tools/cache"
	workloadbuilder "k8s.io/component-helpers/scheduling/schedulingv1/workloadbuilder"
	"k8s.io/klog/v2"
	jobutil "k8s.io/kubernetes/pkg/api/job"
	apischeduling "k8s.io/kubernetes/pkg/apis/scheduling"
	"k8s.io/kubernetes/pkg/features"
	hashutil "k8s.io/kubernetes/pkg/util/hash"
)

const (
	// Workload.spec.controllerRef 与 PodGroup.spec.workloadRef 只包含名称（不含 UID），
	// 因此这两个索引以 namespace/name 作为键。由于名称可能被复用，调用方在根据查询结果
	// 采取行动前，仍必须通过 ownerRef 的 UID 校验所有权。
	workloadByJobNameIndexKey      = "workloadByJobName"
	podGroupByWorkloadNameIndexKey = "podGroupByWorkloadName"
)

// managementMode 描述 Job 控制器对给定 Job 负责管理哪些调度对象。
type managementMode int

const (
	// manageNone 表示控制器不管理任何调度对象：可能是特性门控关闭、
	// 用户自带了 PodGroup，或者父控制器同时拥有两个对象且未下放管理权。
	manageNone managementMode = iota

	// manageBoth 表示控制器同时拥有 Workload 和 PodGroup。
	manageBoth

	// managePodGroupOnly 适用于非根 Job：父控制器拥有 Workload，
	// 但把运行时 PodGroup 的管理下放给了 Job 控制器。
	managePodGroupOnly
)

// getManagementMode 计算 Job 控制器应为该 Job 管理哪些调度对象。
func getManagementMode(job *batch.Job) managementMode {
	// 特性未开启时不接管任何调度对象。
	if !feature.DefaultFeatureGate.Enabled(features.WorkloadWithJob) {
		return manageNone
	}
	// 用户 Pod 模板里自带调度组，说明用户自行管理调度对象。
	if job.Spec.Template.Spec.SchedulingGroup != nil {
		return manageNone
	}
	// 没有 controller owner 的 Job 是工作负载树的根节点，由本控制器全权管理。
	if metav1.GetControllerOf(job) == nil {
		return manageBoth
	}
	// 非根 Job 只有在父控制器显式下放时才接管 PodGroup。
	if hasDelegatedPodGroup(job) {
		return managePodGroupOnly
	}
	return manageNone
}

// shouldManageWorkloadForJob 判断 Job 控制器是否应为该 Job 创建调度对象（Workload 与 PodGroup）。
func shouldManageWorkloadForJob(job *batch.Job) bool {
	return getManagementMode(job) != manageNone
}

// hasDelegatedPodGroup 判断父控制器是否通过 podGroupTemplate 注解
// 把运行时 PodGroup 的管理权下放给了 Job 控制器。
func hasDelegatedPodGroup(job *batch.Job) bool {
	_, ok := job.Annotations[apischeduling.GroupTemplateNameAnnotation]
	return ok
}

// filterControlledByJob 只保留 controller ownerRef 指向给定 Job 的对象，
// 即本控制器创建并拥有的对象。
// 按 UID 校验可排除名称相同但并非本 Job 创建的对象。
func filterControlledByJob[T metav1.Object](objs []T, job *batch.Job) []T {
	matched := make([]T, 0, len(objs))
	for _, obj := range objs {
		if metav1.IsControlledBy(obj, job) {
			matched = append(matched, obj)
		}
	}
	return matched
}

// ensureWorkloadAndPodGroup 为给定 Job 查找或创建 Workload 与 PodGroup。
// 工作负载集成生效时返回两个对象；Job 应回退到默认调度或将所有权交由父控制器时返回 nil。
func (jm *Controller) ensureWorkloadAndPodGroup(ctx context.Context, job *batch.Job, pods []*v1.Pod) (*schedulingv1beta1.Workload, *schedulingv1beta1.PodGroup, error) {
	mode := getManagementMode(job)
	// 不需要管理时直接返回，避免无谓的 API 查询。
	if mode == manageNone {
		return nil, nil, nil
	}

	// 只为从未启动过的 Job 创建对象：特性门控开启前就已存在的 Job 不会被改动，
	// 已启动的 Job 只发现并复用已有对象，绝不新建，以免干扰运行中的工作负载。
	newJob := isNewJob(job, pods)

	// 情形一：只管理 PodGroup。
	if mode == managePodGroupOnly {
		pg, err := jm.getOrCreateDelegatedPodGroup(ctx, job, newJob)
		if err != nil {
			return nil, nil, err
		}
		return nil, pg, nil
	}

	// 情形二：根 Job，同时管理 Workload 与 PodGroup。
	wl, err := jm.getOrCreateWorkload(ctx, job, newJob)
	if err != nil || wl == nil {
		return nil, nil, err
	}
	pg, err := jm.getOrCreatePodGroup(ctx, job, wl, newJob)
	if err != nil {
		return nil, nil, err
	}

	// 把变化的 gang minCount（弹性伸缩）同步到控制器拥有的 Workload/PodGroup。
	// 此步骤不依赖 Pod 状态，以便运行中的 gang Job 也能被调整规模。
	err = jm.syncGangMinCount(ctx, job, wl, pg)
	if err != nil {
		return nil, nil, err
	}
	return wl, pg, nil
}

// syncGangMinCount 把更新后的 gang 规模同步到控制器拥有的 Workload 和运行时 PodGroup。
// 它根据当前 Job 规约重新编译出期望的 Workload；当解析出的策略为 Gang 时，
// 仅在现存对象的 minCount 与期望值不同时才更新。
//
// Basic 策略无需处理。除 minCount 外，其他 Workload/PodGroup 字段在 API 层面均不可变。
func (jm *Controller) syncGangMinCount(ctx context.Context, job *batch.Job,
	workload *schedulingv1beta1.Workload, podGroup *schedulingv1beta1.PodGroup) error {

	// 从 Job 规约重新编译期望状态，以获取最新的 gang 配置。
	desiredWorkload, err := jm.generateWorkload(job)
	if err != nil || len(desiredWorkload.Spec.PodGroupTemplates) == 0 {
		return err
	}
	gang := desiredWorkload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang
	if gang == nil {
		// Basic（非 gang）策略没有 minCount 需要协调，直接返回。
		return nil
	}
	desiredMinCount := gang.MinCount

	// 只在结构符合 alpha 约束（单个模板）时更新 Workload，避免误改异常对象。
	currentPGTemplate := workload.Spec.PodGroupTemplates
	if len(currentPGTemplate) == 1 &&
		currentPGTemplate[0].SchedulingPolicy.Gang != nil &&
		currentPGTemplate[0].SchedulingPolicy.Gang.MinCount != desiredMinCount {

		err := jm.patchWorkloadMinCount(ctx, workload, desiredMinCount)
		if err != nil {
			return err
		}
	}

	if podGroup != nil &&
		podGroup.Spec.SchedulingPolicy.Gang != nil &&
		podGroup.Spec.SchedulingPolicy.Gang.MinCount != desiredMinCount {

		err := jm.patchPodGroupMinCount(ctx, podGroup, desiredMinCount)
		if err != nil {
			return err
		}
	}
	return nil
}

// patchWorkloadMinCount 更新 Workload 唯一 PodGroupTemplate 上的 gang minCount。
// minCount 是编译后的 Workload 上唯一可变字段，因此采用补丁更新。
func (jm *Controller) patchWorkloadMinCount(ctx context.Context, workload *schedulingv1beta1.Workload, minCount int32) error {
	// 在副本上修改，避免改动 informer 缓存中的对象。
	updatedW := workload.DeepCopy()
	updatedW.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount = minCount

	oldData, err := json.Marshal(workload)
	if err != nil {
		return err
	}

	newData, err := json.Marshal(updatedW)
	if err != nil {
		return err
	}
	// 通过双向合并补丁只提交变化字段，防止覆盖其他控制器并发写入的内容。
	patch, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, &schedulingv1beta1.Workload{})
	if err != nil {
		return err
	}
	_, err = jm.kubeClient.SchedulingV1beta1().Workloads(workload.Namespace).
		Patch(ctx, workload.Name, types.StrategicMergePatchType, patch, metav1.PatchOptions{})
	return err
}

// patchPodGroupMinCount 更新运行时 PodGroup 上的 gang minCount。
func (jm *Controller) patchPodGroupMinCount(ctx context.Context, podGroup *schedulingv1beta1.PodGroup, minCount int32) error {
	// 在副本上修改，避免改动 informer 缓存中的对象。
	updatedPG := podGroup.DeepCopy()
	updatedPG.Spec.SchedulingPolicy.Gang.MinCount = minCount

	oldData, err := json.Marshal(podGroup)
	if err != nil {
		return err
	}

	newData, err := json.Marshal(updatedPG)
	if err != nil {
		return err
	}

	patch, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, &schedulingv1beta1.PodGroup{})
	if err != nil {
		return err
	}
	_, err = jm.kubeClient.SchedulingV1beta1().PodGroups(podGroup.Namespace).
		Patch(ctx, podGroup.Name, types.StrategicMergePatchType, patch, metav1.PatchOptions{})
	return err
}

// findParentWorkload 查找非根 Job 的父级 Workload：
// 即同命名空间下 spec.controllerRef 指向该 Job 父控制器的 Workload。
func (jm *Controller) findParentWorkload(job *batch.Job) (*schedulingv1beta1.Workload, error) {
	// lister 未配置说明 informer 未接入，无法执行查询。
	if jm.workloadLister == nil {
		return nil, fmt.Errorf("workload lister is not configured")
	}
	parent := metav1.GetControllerOf(job)
	if parent == nil {
		return nil, nil
	}
	// 父控制器 APIVersion 无法解析时不再尝试匹配。
	gv, err := schema.ParseGroupVersion(parent.APIVersion)
	if err != nil {
		return nil, nil
	}
	workloads, err := jm.workloadLister.Workloads(job.Namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}
	for _, wl := range workloads {
		// 此处不能使用 metav1.IsControlledBy：它按 UID 匹配对象的
		// metadata.ownerReferences，而 spec.ControllerRef 是基于名称的
		// TypedLocalObjectReference，指向的是 Job 的父控制器而非 Job 本身。
		if ref := wl.Spec.ControllerRef; ref != nil &&
			ref.APIGroup == gv.Group && ref.Kind == parent.Kind && ref.Name == parent.Name {
			return wl, nil
		}
	}
	return nil, nil
}

// getOrCreateDelegatedPodGroup 为父控制器下放 PodGroup 管理权的非根 Job
// 查找或创建运行时 PodGroup。PodGroup 通过下放注解中指定的 PodGroupTemplate
// 关联到父级拥有的 Workload，并且只有指向该 Job 的 controller ownerRef。
func (jm *Controller) getOrCreateDelegatedPodGroup(ctx context.Context,
	job *batch.Job, newJob bool) (*schedulingv1beta1.PodGroup, error) {
	logger := klog.FromContext(ctx)

	// 父级 Workload 可能尚未创建，此时返回 nil 并等待下一次同步重试。
	parentWorkload, err := jm.findParentWorkload(job)
	if err != nil {
		return nil, err
	}
	if parentWorkload == nil {
		logger.V(2).Info("Parent-owned Workload not found yet for delegated Job, will retry",
			"job", klog.KObj(job))
		return nil, nil
	}

	templateName := job.Annotations[apischeduling.GroupTemplateNameAnnotation]
	template, err := getPodGroupTemplateByName(parentWorkload, templateName)
	if err != nil {
		logger.V(2).Info("Delegated PodGroupTemplate not found in parent Workload, skipping",
			"job", klog.KObj(job), "workload", klog.KObj(parentWorkload), "template", templateName)
		return nil, nil
	}

	// Workload 由父控制器拥有并编译，因此应基于其持久化的模板创建 PodGroup，
	// 而不是根据本 Job 的规约重新编译。
	builder := newBuilderFromExistingWorkload(job, parentWorkload)

	matched, err := jm.listPodGroupsForWorkload(parentWorkload)
	if err != nil {
		return nil, err
	}
	var updatedPG *schedulingv1beta1.PodGroup
	for _, pg := range matched {
		if metav1.IsControlledBy(pg, job) {
			updatedPG = pg
			break
		}
	}
	// 只有在 Job 尚无 Pod 且 PodGroup 不存在时才创建，否则只做发现。
	// 与其他创建者并发时会返回 AlreadyExists，Job 随之重新入队，下次同步即可发现已有对象。
	if updatedPG == nil && newJob {
		// 下放模式下的 Job 对应父级拥有的 Workload，因此 PodGroup 不把该 Workload
		// 设为 owner（linkWorkloadOwner=false），父控制器仍是 Workload 的唯一所有者。
		updatedPG, err = jm.createPodGroup(ctx, job, parentWorkload, templateName, false, builder)
		if err != nil {
			return nil, err
		}
	}
	// 父控制器可能在 PodGroup 创建后调整 Workload 模板中的 gang 规模；
	// 下放模式下本控制器负责 PodGroup，因此以模板为准进行同步。
	if err := jm.syncDelegatedPodGroupMinCount(ctx, template, updatedPG); err != nil {
		return nil, err
	}
	return updatedPG, nil
}

// syncDelegatedPodGroupMinCount 把父控制器在其 Workload 的 PodGroupTemplate 上
// 修改的 gang minCount 同步到下放的运行时 PodGroup。
// 下放模式下父控制器的模板是唯一事实来源。
func (jm *Controller) syncDelegatedPodGroupMinCount(ctx context.Context,
	template *schedulingv1beta1.PodGroupTemplate, podGroup *schedulingv1beta1.PodGroup) error {
	if podGroup == nil ||
		template.SchedulingPolicy.Gang == nil || podGroup.Spec.SchedulingPolicy.Gang == nil {
		return nil
	}
	desiredMinCount := template.SchedulingPolicy.Gang.MinCount
	if podGroup.Spec.SchedulingPolicy.Gang.MinCount == desiredMinCount {
		return nil
	}
	return jm.patchPodGroupMinCount(ctx, podGroup, desiredMinCount)
}

// isNewJob 判断 Job 是否从未启动过，避免为特性门控开启前就已存在的 Job 创建调度对象。
//
// 该启发式判断在 alpha 阶段刻意保守，beta 之前会被更可靠的机制取代。
//
// 同时检查 Pod、StartTime、终态计数和 Suspended 条件，是因为每一项单独使用都有盲区：
// Pod 可能被 GC 回收，StartTime 在挂起时会重置，而 Succeeded/Failed 会一直保留；
// 早期就被挂起的 Job 能通过其他检查，Suspended 条件正好覆盖这种情况。
func isNewJob(job *batch.Job, pods []*v1.Pod) bool {
	if len(pods) != 0 ||
		job.Status.StartTime != nil ||
		job.Status.Succeeded != 0 ||
		job.Status.Failed != 0 {
		return false
	}
	return findConditionByType(job.Status.Conditions, batch.JobSuspended) == nil
}

// getOrCreateWorkload 返回该 Job 对应的 Workload，必要时创建。
// 当 Job 应回退到默认调度时返回 (nil, nil)：例如匹配结果不唯一、
// 结构不受支持，或该 Job 并非新 Job。
func (jm *Controller) getOrCreateWorkload(ctx context.Context,
	job *batch.Job, newJob bool) (*schedulingv1beta1.Workload, error) {
	allWorkloads, err := jm.listWorkloadsForJob(job)
	if err != nil {
		return nil, err
	}

	// 只管理本控制器创建的 Workload，依据是指向该 Job 的 controller ownerRef。
	// 忽略用户预先创建（自带）的 Workload，确保控制器调度时所依据的对象归自己所有。
	matched := filterControlledByJob(allWorkloads, job)

	switch len(matched) {
	case 0:
		// 仅在 Job 尚无 Pod 时创建，否则只做发现。
		// 并发创建时返回 AlreadyExists 会使 Job 重新入队，下次同步即可发现已存在的 Workload。
		if !newJob {
			return nil, nil
		}
		return jm.createWorkloadForJob(ctx, job)
	case 1:
		if err := jm.validateWorkloadStructure(job, matched[0]); err != nil {
			return nil, err
		}
		return matched[0], nil
	default:
		utilruntime.HandleError(fmt.Errorf("found %d Workloads referencing Job %s/%s, falling back to default scheduling",
			len(matched), job.Namespace, job.Name))
		return nil, nil
	}
}

// validateWorkloadStructure 检查控制器拥有的 Workload 是否满足 alpha 阶段的约束。
// 结构不受支持时返回错误，使 Job 重新入队并暴露问题，而不是静默回退到默认调度。
//
// TODO(beta): 这里只校验初始形态。用户或其他控制器可能在创建后修改 Workload，
// 破坏我们的假设（例如增删 PodGroupTemplates、更改调度策略）。beta 之前需要决定
// 如何处理这类修改：覆盖回预期状态、回退到默认调度，或通过准入控制拒绝更新。
func (jm *Controller) validateWorkloadStructure(job *batch.Job, wl *schedulingv1beta1.Workload) error {
	// alpha 阶段每个 Workload 只能有一个模板，否则后续按首元素取模板的逻辑不成立。
	if len(wl.Spec.PodGroupTemplates) != 1 {
		return fmt.Errorf("workload %s/%s for job %s/%s has %d PodGroupTemplates, want exactly 1",
			wl.Namespace, wl.Name, job.Namespace, job.Name, len(wl.Spec.PodGroupTemplates))
	}
	return nil
}

// getOrCreatePodGroup 返回该 Workload 对应的 PodGroup，必要时创建。
// 当 Job 应回退到默认调度时返回 (nil, nil)：匹配结果不唯一，
// 或对象并非本控制器所有。
func (jm *Controller) getOrCreatePodGroup(ctx context.Context, job *batch.Job,
	workload *schedulingv1beta1.Workload, newJob bool) (*schedulingv1beta1.PodGroup, error) {
	// Workload 已存在且由其编译 PodGroup，直接从现有模板生成。
	builder := newBuilderFromExistingWorkload(job, workload)

	allPodGroups, err := jm.listPodGroupsForWorkload(workload)
	if err != nil {
		return nil, err
	}
	// 只管理本控制器创建的 PodGroup，依据是指向该 Job 的 controller ownerRef。
	// 忽略用户预先创建（自带）的 PodGroup，确保控制器调度时所依据的对象归自己所有。
	matched := filterControlledByJob(allPodGroups, job)

	switch len(matched) {
	case 0:
		// 仅在 Job 尚无 Pod 时创建，否则只做发现。
		// 这也能补全流程中途重启留下的不完整对象（Workload 存在但 PodGroup 缺失），
		// 但前提同样是 Pod 尚未出现。
		if !newJob {
			return nil, nil
		}
		// 根 Job 拥有其 Workload，因此 PodGroup 以非 controller owner 的方式
		// 关联它（linkWorkloadOwner=true）。空模板名表示选用 Workload 的唯一模板。
		return jm.createPodGroup(ctx, job, workload, "", true, builder)
	case 1:
		return matched[0], nil
	default:
		utilruntime.HandleError(fmt.Errorf("found %d PodGroups referencing Workload %s/%s, falling back to default scheduling",
			len(matched), workload.Namespace, workload.Name))
		return nil, nil
	}
}

// listWorkloadsForJob 返回 Job 命名空间下 spec.controllerRef 指向该 Job 的所有 Workload。
func (jm *Controller) listWorkloadsForJob(job *batch.Job) ([]*schedulingv1beta1.Workload, error) {
	// 索引器未配置时无法通过索引查询。
	if jm.workloadIndexer == nil {
		return nil, fmt.Errorf("workload indexer is not configured")
	}
	key := namespacedNameIndexKey(job.Namespace, job.Name)
	all, err := jm.workloadIndexer.ByIndex(workloadByJobNameIndexKey, key)
	if err != nil {
		return nil, err
	}
	matched := make([]*schedulingv1beta1.Workload, 0, len(all))
	for _, wl := range all {
		workload, ok := wl.(*schedulingv1beta1.Workload)
		if !ok {
			continue
		}
		matched = append(matched, workload)
	}
	return matched, nil
}

// listPodGroupsForWorkload 返回 Workload 命名空间下 spec.workloadRef 引用该 Workload 的所有 PodGroup。
func (jm *Controller) listPodGroupsForWorkload(workload *schedulingv1beta1.Workload) ([]*schedulingv1beta1.PodGroup, error) {
	// 索引器未配置时无法通过索引查询。
	if jm.podGroupIndexer == nil {
		return nil, fmt.Errorf("podgroup indexer is not configured")
	}
	key := namespacedNameIndexKey(workload.Namespace, workload.Name)
	all, err := jm.podGroupIndexer.ByIndex(podGroupByWorkloadNameIndexKey, key)
	if err != nil {
		return nil, err
	}
	matched := make([]*schedulingv1beta1.PodGroup, 0, len(all))
	for _, pg := range all {
		podGroup, ok := pg.(*schedulingv1beta1.PodGroup)
		if !ok {
			continue
		}
		matched = append(matched, podGroup)
	}
	return matched, nil
}

// podGroupTemplateName 返回控制器为 Job 编译的唯一 PodGroupTemplate 的名称。
// alpha 阶段每个 Job 只有一个模板，因此索引固定为 0。
func podGroupTemplateName(job *batch.Job) string {
	return fmt.Sprintf("%s-pgt-%d", job.Name, 0)
}

// buildWorkloadItem 为 Job 组装单节点的逻辑工作负载树。
// Job 默认使用 Basic 调度；用户设置的 spec.scheduling 会覆盖默认值。
func buildWorkloadItem(job *batch.Job) *workloadbuilder.WorkloadItem {
	var input workloadbuilder.WorkloadInput
	// 未指定调度配置时保留零值输入，让构建器使用默认策略。
	if s := job.Spec.Scheduling; s != nil {
		input = jobutil.WorkloadInputForJobV1(s)
	}
	return jobutil.WorkloadItemForJob(podGroupTemplateName(job),
		job.Spec.Template.Spec.PriorityClassName, job.Spec.Parallelism, input, nil)
}

// generateWorkload 通过共享的 workloadbuilder 库把 Job 的 spec.scheduling 编译为 Workload。
// BuildWorkload 还会设置指向该 Job 的 controller ownerReference 和 spec.controllerRef。
func (jm *Controller) generateWorkload(job *batch.Job) (*schedulingv1beta1.Workload, error) {
	return workloadbuilder.NewBuilder(buildWorkloadItem(job), workloadbuilder.BuildOptions{
		Name:      computeWorkloadName(job),
		Namespace: job.Namespace,
		Owner:     metav1.NewControllerRef(job, controllerKind),
	}).BuildWorkload()
}

// createWorkloadForJob 为给定 Job 编译并创建新的 Workload 对象。
// 编译和创建失败都会记录 Event，方便用户排查为何未启用工作负载调度。
func (jm *Controller) createWorkloadForJob(ctx context.Context, job *batch.Job) (*schedulingv1beta1.Workload, error) {
	workload, err := jm.generateWorkload(job)
	if err != nil {
		jm.recorder.Eventf(job, v1.EventTypeWarning, "FailedWorkloadCompilation",
			"Failed to compile Workload for Job %s: %v", job.Name, err)
		return nil, err
	}

	created, err := jm.kubeClient.SchedulingV1beta1().Workloads(job.Namespace).Create(ctx, workload, metav1.CreateOptions{})
	if err != nil {
		jm.recorder.Eventf(job, v1.EventTypeWarning, "FailedWorkloadCreate",
			"Failed to create Workload for Job %s: %v", job.Name, err)
		return nil, err
	}

	jm.recorder.Eventf(job, v1.EventTypeNormal, "WorkloadCreated",
		"Created Workload %s for Job %s", created.Name, job.Name)
	return created, nil
}

// getPodGroupTemplateByName 从 Workload 中返回指定名称的 PodGroupTemplate。
// 名称为空时选择 Workload 的唯一模板。
func getPodGroupTemplateByName(workload *schedulingv1beta1.Workload, name string) (*schedulingv1beta1.PodGroupTemplate, error) {
	// 没有模板时无从创建 PodGroup，直接返回错误。
	if len(workload.Spec.PodGroupTemplates) == 0 {
		return nil, fmt.Errorf("workload %s/%s has no PodGroupTemplates", workload.Namespace, workload.Name)
	}
	if name == "" {
		return &workload.Spec.PodGroupTemplates[0], nil
	}
	for i := range workload.Spec.PodGroupTemplates {
		if workload.Spec.PodGroupTemplates[i].Name == name {
			return &workload.Spec.PodGroupTemplates[i], nil
		}
	}
	return nil, fmt.Errorf("PodGroupTemplate %q not found in workload %s/%s", name, workload.Namespace, workload.Name)
}

// newBuilderFromExistingWorkload 返回一个从已持久化 Workload 生成 PodGroup 的 builder。
// 下放模式下 Job 的 Workload 由父控制器拥有并编译，其持久化模板是唯一事实来源，
// Job 自身的 spec.scheduling 在此不参与决策，只需提供 Job 的 controllerRef 和该 Workload。
func newBuilderFromExistingWorkload(job *batch.Job, workload *schedulingv1beta1.Workload) *workloadbuilder.Builder {
	return workloadbuilder.NewBuilderFromExistingWorkload(workload, workloadbuilder.BuildOptions{
		Owner: metav1.NewControllerRef(job, controllerKind),
	})
}

// createPodGroup 根据 Workload 中名为 templateName 的 PodGroupTemplate 为 Job 创建运行时 PodGroup。
// 它总是携带指向 Job 的 controller ownerRef；linkWorkloadOwner 为 true 时，
// 还会添加指向 Workload 的非 controller ownerRef；下放（父级拥有）的 Workload 不加该引用，
// 以保证父控制器仍是 Workload 的唯一所有者。不设置 BlockOwnerDeletion，
// 避免残留的 PodGroup 阻碍 Workload 被删除。
func (jm *Controller) createPodGroup(ctx context.Context, job *batch.Job, workload *schedulingv1beta1.Workload,
	templateName string, linkWorkloadOwner bool, builder *workloadbuilder.Builder) (*schedulingv1beta1.PodGroup, error) {
	template, err := getPodGroupTemplateByName(workload, templateName)
	if err != nil {
		return nil, err
	}

	podGroup, err := builder.NewPodGroup(computePodGroupName(workload.Name, template.Name), template.Name)
	if err != nil {
		return nil, err
	}
	if linkWorkloadOwner {
		podGroup.OwnerReferences = append(podGroup.OwnerReferences, metav1.OwnerReference{
			APIVersion: schedulingv1beta1.SchemeGroupVersion.String(),
			Kind:       "Workload",
			Name:       workload.Name,
			UID:        workload.UID,
		})
	}

	created, err := jm.kubeClient.SchedulingV1beta1().PodGroups(job.Namespace).Create(ctx, podGroup, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}

	jm.recorder.Eventf(job, v1.EventTypeNormal, "PodGroupCreated",
		"Created PodGroup %s for Job %s", created.Name, job.Name)
	return created, nil
}

// computeWorkloadName 为与某个 Job 关联的 Workload 生成确定性名称。
// 格式为：<截断后的 Job 名>-<哈希>
// 哈希由 Job UID 计算得出，以保证唯一性以及重启后的稳定性。
func computeWorkloadName(job *batch.Job) string {
	hasher := fnv.New32a()
	hashutil.DeepHashObject(hasher, job.UID)
	hash := rand.SafeEncodeString(fmt.Sprint(hasher.Sum32()))

	// 哈希约 10 个字符，再为分隔符 "-" 预留一个字符。
	maxPrefixLen := validation.DNS1123SubdomainMaxLength - len(hash) - 1
	prefix := job.Name
	if len(prefix) > maxPrefixLen {
		prefix = prefix[:maxPrefixLen]
	}
	return fmt.Sprintf("%s-%s", prefix, hash)
}

// computePodGroupName 为与某个 Workload 关联的 PodGroup 生成确定性名称。
// 格式为：<截断后的 Workload 名>-<截断后的模板名>-<哈希>
// 哈希由 Workload 名和模板名共同计算，以保证唯一性。
func computePodGroupName(workloadName, templateName string) string {
	hasher := fnv.New32a()
	// 对完整组合做哈希，避免不同名称组合产生相同结果。
	hasher.Write([]byte(workloadName))
	hasher.Write([]byte(templateName))
	hash := rand.SafeEncodeString(fmt.Sprint(hasher.Sum32()))

	// 截断 Workload 名与模板名以符合 DNS 子域名的长度上限，
	// 同时为哈希和两个 "-" 分隔符预留空间。
	maxAvailable := validation.DNS1123SubdomainMaxLength - len(hash) - 2
	wl := workloadName
	tpl := templateName

	if len(wl)+len(tpl) > maxAvailable {
		// 只截断超出各自公平份额的部分：每部分最多占一半空间，
		// 若某一部分较短，剩余空间全部让给另一部分。
		half := maxAvailable / 2
		switch {
		case len(wl) <= half:
			// Workload 名未超过其一半份额，剩余空间都给模板名。
			tpl = tpl[:maxAvailable-len(wl)]
		case len(tpl) <= half:
			// 模板名未超过其一半份额，剩余空间都给 Workload 名。
			wl = wl[:maxAvailable-len(tpl)]
		default:
			// 两者都超过一半，平均分配（长度为奇数时 Workload 名多占一个字符）。
			wl = wl[:maxAvailable-half]
			tpl = tpl[:half]
		}
	}

	return fmt.Sprintf("%s-%s-%s", wl, tpl, hash)
}

// addSchedulingInformers 接入 Workload 和 PodGroup 的 informer 与索引器，
// 以便这些对象发生变化时把对应的 Job 重新入队。
func (jm *Controller) addSchedulingInformers(logger klog.Logger, workloadInformer schedulinginformers.WorkloadInformer, podGroupInformer schedulinginformers.PodGroupInformer) error {
	// 特性开启后缺少任一 informer 都无法保证完整发现调度对象。
	if workloadInformer == nil {
		return fmt.Errorf("workload informer is required when the feature gate %q is enabled", features.WorkloadWithJob)
	}
	if podGroupInformer == nil {
		return fmt.Errorf("pod group informer is required when the feature gate %q is enabled", features.WorkloadWithJob)
	}
	if err := workloadInformer.Informer().AddIndexers(cache.Indexers{
		workloadByJobNameIndexKey: workloadByJobNameIndexFunc,
	}); err != nil {
		return fmt.Errorf("adding Workload indexer: %w", err)
	}
	if err := podGroupInformer.Informer().AddIndexers(cache.Indexers{
		podGroupByWorkloadNameIndexKey: podGroupByWorkloadNameIndexFunc,
	}); err != nil {
		return fmt.Errorf("adding PodGroup indexer: %w", err)
	}
	jm.workloadLister = workloadInformer.Lister()
	jm.workloadIndexer = workloadInformer.Informer().GetIndexer()
	jm.workloadStoreSynced = workloadInformer.Informer().HasSynced

	jm.podGroupLister = podGroupInformer.Lister()
	jm.podGroupIndexer = podGroupInformer.Informer().GetIndexer()
	jm.podGroupStoreSynced = podGroupInformer.Informer().HasSynced

	return nil
}

// namespacedNameIndexKey 生成用于索引的 namespace/name 键，与索引函数的输出保持一致。
func namespacedNameIndexKey(namespace, name string) string {
	return cache.NewObjectName(namespace, name).String()
}

// workloadControllerRefJobName 从 Workload 的 spec.controllerRef 中提取 Job 名称；
// 当该引用不指向 batch Job 时返回空字符串。
func workloadControllerRefJobName(wl *schedulingv1beta1.Workload) string {
	if wl.Spec.ControllerRef == nil ||
		wl.Spec.ControllerRef.APIGroup != batch.SchemeGroupVersion.Group ||
		wl.Spec.ControllerRef.Kind != "Job" ||
		wl.Spec.ControllerRef.Name == "" {
		return ""
	}
	return wl.Spec.ControllerRef.Name
}

// podGroupWorkloadName 从 PodGroup 的 spec.workloadRef 中提取 Workload 名称；
// 引用未设置时返回空字符串。
func podGroupWorkloadName(pg *schedulingv1beta1.PodGroup) string {
	if pg.Spec.WorkloadRef == nil ||
		pg.Spec.WorkloadRef.WorkloadName == "" {
		return ""
	}
	return pg.Spec.WorkloadRef.WorkloadName
}

// workloadByJobNameIndexFunc 为 Workload 建立按关联 Job 名称的索引键。
// 只有 controllerRef 指向 batch Job 的对象才会被索引。
func workloadByJobNameIndexFunc(obj interface{}) ([]string, error) {
	wl, ok := obj.(*schedulingv1beta1.Workload)
	// 索引器可能收到非 Workload 对象，必须显式报错以免静默污染索引。
	if !ok {
		return nil, fmt.Errorf("unexpected object type %T", obj)
	}
	if name := workloadControllerRefJobName(wl); name != "" {
		return []string{namespacedNameIndexKey(wl.Namespace, name)}, nil
	}
	return nil, nil
}

// podGroupByWorkloadNameIndexFunc 为 PodGroup 建立按关联 Workload 名称的索引键。
// 只有设置了 workloadRef 的对象才会被索引。
func podGroupByWorkloadNameIndexFunc(obj interface{}) ([]string, error) {
	pg, ok := obj.(*schedulingv1beta1.PodGroup)
	// 索引器可能收到非 PodGroup 对象，必须显式报错以免静默污染索引。
	if !ok {
		return nil, fmt.Errorf("unexpected object type %T", obj)
	}
	if name := podGroupWorkloadName(pg); name != "" {
		return []string{namespacedNameIndexKey(pg.Namespace, name)}, nil
	}
	return nil, nil
}
