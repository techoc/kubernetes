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

package resourcequota

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"k8s.io/klog/v2"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/admission"
	resourcequotaapi "k8s.io/apiserver/pkg/admission/plugin/resourcequota/apis/resourcequota"
	quota "k8s.io/apiserver/pkg/quota/v1"
	"k8s.io/apiserver/pkg/quota/v1/generic"
	"k8s.io/client-go/util/workqueue"
)

// Evaluator 是配额准入的对外入口，按请求属性判断是否满足配额约束。
type Evaluator interface {
	// Evaluate 接收一次操作并检查是否满足配额约束，不满足时返回错误。
	// 默认实现在可能的情况下把相关操作按块批量处理。
	Evaluate(a admission.Attributes) error
}

// quotaEvaluator 实现 Evaluator。
//
// 为避免并发请求在同一命名空间上互相踩踏，同一命名空间的请求会被聚合成批，
// 由单个 worker 串行处理：先基于一份“运行中的配额副本”试算，全部通过后再
// 一次性回写配额状态。这既减少了更新冲突，也避免为每个请求单独 fanout。
type quotaEvaluator struct {
	quotaAccessor QuotaAccessor
	// lockAcquisitionFunc 用于在跨 apiserver 实例时获取配额对象的分布式锁，
	// 返回的清理函数必须 defer 调用以释放锁。
	lockAcquisitionFunc func([]corev1.ResourceQuota) func()

	ignoredResources map[schema.GroupResource]struct{}

	// registry：知道如何测量对象用量的注册表。
	registry quota.Registry

	// TODO 以下几项配合使用：按命名空间对条目分桶后批量处理。
	// 这种技术对汇总类操作很有价值，可以避免 fanout、减少资源争用。
	// 如果有其他组件也需要，可以把它抽成公共库。
	// queue 以命名空间为索引，因此按命名空间聚合请求。
	queue      *workqueue.Typed[string]
	workLock   sync.Mutex
	work       map[string][]*admissionWaiter
	dirtyWork  map[string][]*admissionWaiter
	inProgress sets.Set[string]

	// 控制 run 方法，使其能干净地符合 Evaluator 接口。
	workers int
	stopCh  <-chan struct{}
	// init 保证 worker 只启动一次，即使 Evaluate 被并发调用。
	init sync.Once

	// 用于确定哪些资源默认受限。
	config *resourcequotaapi.Configuration
}

// admissionWaiter 表示一个正在等待配额决策的请求。
// 结果字段在 worker goroutine 中写入，finished 关闭后由 Evaluate 读取；
// close(finished) 提供 happens-before 保证，无需额外加锁。
type admissionWaiter struct {
	attributes admission.Attributes
	finished   chan struct{}
	result     error
}

// defaultDeny 是 waiter 的初始结果。
// 只有确认配额更新成功后才被清除，因此任何遗漏路径都会默认拒绝，
// 这是配额控制器的安全兜底。
type defaultDeny struct{}

// Error 返回固定的错误文本；该文本不会暴露给客户端，仅用于内部状态判断。
func (defaultDeny) Error() string {
	return "DEFAULT DENY"
}

// IsDefaultDeny 判断错误是否为尚未决策的 defaultDeny 哨兵值。
func IsDefaultDeny(err error) bool {
	if err == nil {
		return false
	}

	_, ok := err.(defaultDeny)
	return ok
}

// newAdmissionWaiter 创建 waiter，初始结果为 defaultDeny。
func newAdmissionWaiter(a admission.Attributes) *admissionWaiter {
	return &admissionWaiter{
		attributes: a,
		finished:   make(chan struct{}),
		result:     defaultDeny{},
	}
}

// NewQuotaEvaluator 构造一个可使用给定 registry 强制配额约束的准入控制器。
// registry 必须能够处理该准入控制器所拦截 server 持久化的 group/kind。
// 返回接口而非具体类型，便于在测试中替换。
func NewQuotaEvaluator(quotaAccessor QuotaAccessor, ignoredResources map[schema.GroupResource]struct{}, quotaRegistry quota.Registry, lockAcquisitionFunc func([]corev1.ResourceQuota) func(), config *resourcequotaapi.Configuration, workers int, stopCh <-chan struct{}) Evaluator {
	// config 为 nil 时使用空默认配置。
	if config == nil {
		config = &resourcequotaapi.Configuration{}
	}

	evaluator := &quotaEvaluator{
		quotaAccessor:       quotaAccessor,
		lockAcquisitionFunc: lockAcquisitionFunc,

		ignoredResources: ignoredResources,
		registry:         quotaRegistry,

		queue:      workqueue.NewTypedWithConfig(workqueue.TypedQueueConfig[string]{Name: "admission_quota_controller"}),
		work:       map[string][]*admissionWaiter{},
		dirtyWork:  map[string][]*admissionWaiter{},
		inProgress: sets.Set[string]{},

		workers: workers,
		stopCh:  stopCh,
		config:  config,
	}

	// 队列内部有指标导出 goroutine，只有 ShutDown 能停止它；
	// 而评估器并非总会被 start（例如聚合 apiserver 的某些层），
	// 因此在构造时就挂上关闭钩子，避免 goroutine 泄漏。
	go evaluator.shutdownOnStop()

	return evaluator
}

// start 启动 worker；重复调用由 init 保护。
func (e *quotaEvaluator) start() {
	defer utilruntime.HandleCrash()

	for i := 0; i < e.workers; i++ {
		go wait.Until(e.doWork, time.Second, e.stopCh)
	}
}

// shutdownOnStop 在 stopCh 关闭时停止队列。
func (e *quotaEvaluator) shutdownOnStop() {
	<-e.stopCh
	klog.Infof("Shutting down quota evaluator")
	e.queue.ShutDown()
}

// doWork 循环从队列取命名空间并处理，直到队列关闭。
func (e *quotaEvaluator) doWork() {
	workFunc := func() bool {
		ns, admissionAttributes, quit := e.getWork()
		if quit {
			return true
		}
		// 无论是否有待处理项，都必须 completeWork 以归还队列条目。
		defer e.completeWork(ns)
		if len(admissionAttributes) == 0 {
			return false
		}
		e.checkAttributes(ns, admissionAttributes)
		return false
	}
	for {
		if quit := workFunc(); quit {
			klog.Infof("quota evaluator worker shutdown")
			return
		}
	}
}

// checkAttributes 对一个命名空间的整批请求做配额检查。
// 返回前必定关闭所有 waiter 的 finished 通道，因此不会让调用方永久阻塞。
func (e *quotaEvaluator) checkAttributes(ns string, admissionAttributes []*admissionWaiter) {
	// 退出时统一通知所有 waiter。
	defer func() {
		for _, admissionAttribute := range admissionAttributes {
			close(admissionAttribute.finished)
		}
	}()

	quotas, err := e.quotaAccessor.GetQuotas(ns)
	if err != nil {
		for _, admissionAttribute := range admissionAttributes {
			admissionAttribute.result = err
		}
		return
	}
	// 没有配额且未启用“默认受限资源”时，直接放行，避免无谓的锁与更新。
	limitedResourcesDisabled := len(e.config.LimitedResources) == 0
	if len(quotas) == 0 && limitedResourcesDisabled {
		for _, admissionAttribute := range admissionAttributes {
			admissionAttribute.result = nil
		}
		return
	}

	if e.lockAcquisitionFunc != nil {
		releaseLocks := e.lockAcquisitionFunc(quotas)
		defer releaseLocks()
	}

	e.checkQuotas(quotas, admissionAttributes, 3)
}

// checkQuotas 用传入的配额检查准入属性；若某个配额适用，会在检查完所有
// admissionAttributes 之后再尝试更新它。方法分为以下阶段：
//  0. 复制一份配额作为“运行中”副本，既知道需要更新什么，也能与原始值比较
//  1. 逐个检查每个准入属性是否满足*所有*配额。不满足则将该 waiter 标记为失败、
//     运行副本不变；满足则检查是否有配额变化：没变化就把 waiter 标记为成功，
//     有变化则更新运行副本
//  2. 若运行副本没有变化，说明无需更新，直接返回
//  3. 对每个发生变化的配额尝试更新。全部成功则把所有未设置结果的 waiter 标记
//     为成功并返回；若部分更新因冲突失败且还有重试次数，则从缓存重新获取失败
//     配额的最新版本，用该子集递归调用本方法。只评估子集是安全的，因为这些
//     waiter 的其余配额文档已在步骤 1 中评估并标记过了。
func (e *quotaEvaluator) checkQuotas(quotas []corev1.ResourceQuota, admissionAttributes []*admissionWaiter, remainingRetries int) {
	// 再复制一份，与原始值比较以确认确实存在差量。
	originalQuotas, err := copyQuotas(quotas)
	if err != nil {
		utilruntime.HandleError(err)
		return
	}

	atLeastOneChanged := false
	for i := range admissionAttributes {
		admissionAttribute := admissionAttributes[i]
		newQuotas, err := e.checkRequest(quotas, admissionAttribute.attributes)
		if err != nil {
			admissionAttribute.result = err
			continue
		}

		// dry-run 只验证不落账，否则会把试运行的用量永久计入配额。
		if admissionAttribute.attributes.IsDryRun() {
			admissionAttribute.result = nil
			continue
		}

		// 如果新旧配额相同，说明这个请求不会触发任何更新；
		// 也就是没有任何配额文档适用，可以直接放行。
		atLeastOneChangeForThisWaiter := false
		for j := range newQuotas {
			if !quota.Equals(quotas[j].Status.Used, newQuotas[j].Status.Used) {
				atLeastOneChanged = true
				atLeastOneChangeForThisWaiter = true
				break
			}
		}

		if !atLeastOneChangeForThisWaiter {
			admissionAttribute.result = nil
		}

		// 把本请求的增量累积到运行副本上，后续请求看到的是累计后的用量。
		quotas = newQuotas
	}

	// 如果所有请求都没有改变任何配额，无需发起更新，剩余请求按默认拒绝处理。
	if !atLeastOneChanged {
		return
	}

	// 接下来尝试发起更新。这里有点绕：
	// 1. 检查配额是否变化，未变化则跳过。
	// 2. 配额有变化且更新成功，则皆大欢喜。
	// 3. 配额有变化但更新失败，把原始对象加入重试列表。
	var updatedFailedQuotas []corev1.ResourceQuota
	var lastErr error
	for i := range quotas {
		newQuota := quotas[i]

		// 该配额的 status 没有变化则跳过。
		if quota.Equals(originalQuotas[i].Status.Used, newQuota.Status.Used) {
			continue
		}

		if err := e.quotaAccessor.UpdateQuotaStatus(&newQuota); err != nil {
			updatedFailedQuotas = append(updatedFailedQuotas, newQuota)
			lastErr = err
		}
	}

	if len(updatedFailedQuotas) == 0 {
		// 所有更新都成功了。此时仍处于 defaultDeny 的 waiter 只是在等更新成功，
		// 可以把它们标记为成功并通知。
		for _, admissionAttribute := range admissionAttributes {
			if IsDefaultDeny(admissionAttribute.result) {
				admissionAttribute.result = nil
			}
		}
		return
	}

	// 走到这里错误就是致命的：把所有没有结果的 waiter 置为失败并返回。
	if remainingRetries <= 0 {
		for _, admissionAttribute := range admissionAttributes {
			if IsDefaultDeny(admissionAttribute.result) {
				admissionAttribute.result = lastErr
			}
		}
		return
	}

	// 这段重试逻辑存在已知缺陷：可能在与实际从未存在过的配额状态比较——
	// 比如新增了一个配额文档、又更新了旧的，资源同时匹配两者而你只检查了其中一个。
	// 冲突重试：重新拉取命名空间内的配额，只对更新失败的配额重算，
	// 因为其余配额的检查结果仍然有效。
	newQuotas, err := e.quotaAccessor.GetQuotas(quotas[0].Namespace)
	if err != nil {
		// 说明更新失败且无法重试：所有 defaultDeny 的请求都视为失败，需要告知调用方。
		for _, admissionAttribute := range admissionAttributes {
			if IsDefaultDeny(admissionAttribute.result) {
				admissionAttribute.result = lastErr
			}
		}
		return
	}

	// 从缓存中找出更新失败配额的新版本；已被删除的配额在本次重试中跳过——
	// 它都被删了，自然不需要再检查。
	quotasToCheck := []corev1.ResourceQuota{}
	for _, newQuota := range newQuotas {
		for _, oldQuota := range updatedFailedQuotas {
			if newQuota.Name == oldQuota.Name {
				quotasToCheck = append(quotasToCheck, newQuota)
				break
			}
		}
	}
	e.checkQuotas(quotasToCheck, admissionAttributes, remainingRetries-1)
}

// copyQuotas 深拷贝配额列表，用作运行副本。
func copyQuotas(in []corev1.ResourceQuota) ([]corev1.ResourceQuota, error) {
	out := make([]corev1.ResourceQuota, 0, len(in))
	for _, quota := range in {
		out = append(out, *quota.DeepCopy())
	}

	return out, nil
}

// filterLimitedResourcesByGroupResource 过滤出匹配指定 groupResource 的输入项。
func filterLimitedResourcesByGroupResource(input []resourcequotaapi.LimitedResource, groupResource schema.GroupResource) []resourcequotaapi.LimitedResource {
	result := []resourcequotaapi.LimitedResource{}
	for i := range input {
		limitedResource := input[i]
		limitedGroupResource := schema.GroupResource{Group: limitedResource.APIGroup, Resource: limitedResource.Resource}
		if limitedGroupResource == groupResource {
			result = append(result, limitedResource)
		}
	}
	return result
}

// limitedByDefault 根据指定的 usage 与 limitedResources 计算出必须存在于
// 覆盖配额中的资源名集合；无法判断某资源是否默认受限时返回空集合。
func limitedByDefault(usage corev1.ResourceList, limitedResources []resourcequotaapi.LimitedResource) []corev1.ResourceName {
	result := []corev1.ResourceName{}
	for _, limitedResource := range limitedResources {
		for k, v := range usage {
			// 只关注正用量：零或负用量不会消耗配额。
			if v.Sign() == 1 {
				// 命中匹配规则则加入受限集合。
				for _, matchContain := range limitedResource.MatchContains {
					if strings.Contains(string(k), matchContain) {
						result = append(result, k)
						break
					}
				}
			}
		}
	}
	return result
}

// getMatchedLimitedScopes 计算对象命中的受限 scope。
func getMatchedLimitedScopes(evaluator quota.Evaluator, inputObject runtime.Object, limitedResources []resourcequotaapi.LimitedResource) ([]corev1.ScopedResourceSelectorRequirement, error) {
	scopes := []corev1.ScopedResourceSelectorRequirement{}
	for _, limitedResource := range limitedResources {
		matched, err := evaluator.MatchingScopes(inputObject, limitedResource.MatchScopes)
		if err != nil {
			klog.ErrorS(err, "Error while matching limited Scopes")
			return []corev1.ScopedResourceSelectorRequirement{}, err
		}
		scopes = append(scopes, matched...)
	}
	return scopes, nil
}

// checkRequest 验证请求是否超出任何配额约束。返回一份尚未持久化的配额副本，
// 其中记录了请求成功后应有的用量；配额不足时返回错误。
func (e *quotaEvaluator) checkRequest(quotas []corev1.ResourceQuota, a admission.Attributes) ([]corev1.ResourceQuota, error) {
	evaluator := e.registry.Get(a.GetResource().GroupResource())
	// 未注册 evaluator 的资源不受配额约束，原样返回。
	if evaluator == nil {
		return quotas, nil
	}
	return CheckRequest(quotas, a, evaluator, e.config.LimitedResources)
}

// CheckRequest 是 checkRequest 的无状态版本，供外部（如测试或其他组件）复用。
func CheckRequest(quotas []corev1.ResourceQuota, a admission.Attributes, evaluator quota.Evaluator,
	limited []resourcequotaapi.LimitedResource) ([]corev1.ResourceQuota, error) {
	if !evaluator.Handles(a) {
		return quotas, nil
	}

	// 该资源启用了 limited resources 时，始终需要计算用量。
	inputObject := a.GetObject()

	// 检查对象是否匹配 AdmissionConfiguration 的 matchScopes。
	limitedScopes, err := getMatchedLimitedScopes(evaluator, inputObject, limited)
	if err != nil {
		return quotas, nil
	}

	// 计算必须存在于覆盖配额中的资源名集合。
	limitedResourceNames := []corev1.ResourceName{}
	limitedResources := filterLimitedResourcesByGroupResource(limited, a.GetResource().GroupResource())
	if len(limitedResources) > 0 {
		deltaUsage, err := evaluator.Usage(inputObject)
		if err != nil {
			return quotas, err
		}
		limitedResourceNames = limitedByDefault(deltaUsage, limitedResources)
	}
	limitedResourceNamesSet := quota.ToSet(limitedResourceNames)

	// 找出与本请求相关的配额集合：
	// 命中配额但用量尚未统计时拒绝；
	// 输入对象不满足配额约束时拒绝；
	// 没有相关配额时直接返回。
	interestingQuotaIndexes := []int{}
	// 跟踪所有配额累计要求的资源集合；
	// 判断是否命中“默认受限”的约束时需要用到。
	restrictedResourcesSet := sets.String{}
	restrictedScopes := []corev1.ScopedResourceSelectorRequirement{}
	for i := range quotas {
		resourceQuota := quotas[i]
		scopeSelectors := getScopeSelectorsFromQuota(resourceQuota)
		localRestrictedScopes, err := evaluator.MatchingScopes(inputObject, scopeSelectors)
		if err != nil {
			return nil, fmt.Errorf("error matching scopes of quota %s, err: %v", resourceQuota.Name, err)
		}
		restrictedScopes = append(restrictedScopes, localRestrictedScopes...)

		match, err := evaluator.Matches(&resourceQuota, inputObject)
		if err != nil {
			klog.ErrorS(err, "Error occurred while matching resource quota against input object",
				"resourceQuota", resourceQuota)
			return quotas, err
		}
		if !match {
			continue
		}

		hardResources := quota.ResourceNames(resourceQuota.Status.Hard)
		restrictedResources := evaluator.MatchingResources(hardResources)
		if err := evaluator.Constraints(restrictedResources, inputObject); err != nil {
			return nil, admission.NewForbidden(a, fmt.Errorf("failed quota: %s: %v", resourceQuota.Name, err))
		}
		// 配额状态尚未由配额控制器初始化时拒绝请求，
		// 否则会把未知用量当作 0 从而放行超额请求。
		if !hasUsageStats(&resourceQuota, restrictedResources) {
			return nil, admission.NewForbidden(a, fmt.Errorf("status unknown for quota: %s, resources: %s", resourceQuota.Name, prettyPrintResourceNames(restrictedResources)))
		}
		interestingQuotaIndexes = append(interestingQuotaIndexes, i)
		localRestrictedResourcesSet := quota.ToSet(restrictedResources)
		restrictedResourcesSet.Insert(localRestrictedResourcesSet.List()...)
	}

	// 部分资源的用量无法孤立统计，例如资源表示对外部资源的唯一引用数；
	// 部分 evaluator 需要命名空间信息才能统计（例如跨对象去重计数），
	// 而客户端请求体中的对象可能未设置 namespace，这里补齐。
	namespace := a.GetNamespace()
	if accessor, err := meta.Accessor(inputObject); namespace != "" && err == nil {
		if accessor.GetNamespace() == "" {
			accessor.SetNamespace(namespace)
		}
	}
	// 至少有一个配额确定匹配当前对象，
	// 因此需要测量该对象对配额的用量；
	// 更新时需要减去先前的用量；
	// 用量没有变化则直接返回，它对配额没有影响。
	inputUsage, err := evaluator.Usage(inputObject)
	if err != nil {
		return quotas, err
	}

	// 确保输入对象的用量不为负（为负说明资源声明了负需求）。
	if negativeUsage := quota.IsNegative(inputUsage); len(negativeUsage) > 0 {
		return nil, admission.NewForbidden(a, fmt.Errorf("quota usage is negative for resource(s): %s", prettyPrintResourceNames(negativeUsage)))
	}

	// 为每个相关配额索引初始化增量用量映射。
	deltaUsageIndexMap := make(map[int]corev1.ResourceList, len(interestingQuotaIndexes))
	for _, index := range interestingQuotaIndexes {
		deltaUsageIndexMap[index] = inputUsage
	}
	var deltaUsageWhenNoInterestingQuota corev1.ResourceList
	if admission.Create == a.GetOperation() && len(interestingQuotaIndexes) == 0 {
		deltaUsageWhenNoInterestingQuota = inputUsage
	}

	if admission.Update == a.GetOperation() {
		prevItem := a.GetOldObject()
		if prevItem == nil {
			return nil, admission.NewForbidden(a, fmt.Errorf("unable to get previous usage since prior version of object was not found"))
		}

		// 通过旧对象的 resourceVersion 判断这是真正的更新还是“create on update”：
		// 无 RV 说明旧对象不存在，按全量计费更保守。
		metadata, err := meta.Accessor(prevItem)
		if err == nil {
			if len(metadata.GetResourceVersion()) > 0 {
				prevUsage, innerErr := evaluator.Usage(prevItem)
				if innerErr != nil {
					return quotas, innerErr
				}

				deltaUsage := quota.SubtractWithNonNegativeResult(inputUsage, prevUsage)
				if len(interestingQuotaIndexes) == 0 {
					deltaUsageWhenNoInterestingQuota = deltaUsage
				}

				for _, index := range interestingQuotaIndexes {
					resourceQuota := quotas[index]
					match, err := evaluator.Matches(&resourceQuota, prevItem)
					if err != nil {
						klog.ErrorS(err, "Error occurred while matching resource quota against the existing object",
							"resourceQuota", resourceQuota)
						return quotas, err
					}
					// 只有旧对象也匹配该配额时才用增量，否则对象是从无到有地进入配额范围。
					if match {
						deltaUsageIndexMap[index] = deltaUsage
					}
				}
			} else if len(interestingQuotaIndexes) == 0 {
				deltaUsageWhenNoInterestingQuota = inputUsage
			}
		}
	}

	// 忽略 deltaUsageIndexMap 中零用量的条目，它们不会影响配额。
	for index := range deltaUsageIndexMap {
		deltaUsageIndexMap[index] = quota.RemoveZeros(deltaUsageIndexMap[index])
		if len(deltaUsageIndexMap[index]) == 0 {
			delete(deltaUsageIndexMap, index)
		}
	}

	// 若没有剩余的非零用量，直接短路返回。
	if len(interestingQuotaIndexes) != 0 {
		if len(deltaUsageIndexMap) == 0 {
			return quotas, nil
		}
	} else {
		deltaUsage := quota.RemoveZeros(deltaUsageWhenNoInterestingQuota)
		if len(deltaUsage) == 0 {
			return quotas, nil
		}
	}

	// “默认受限”资源必须被某个配额覆盖，否则无法限制其用量，拒绝请求。
	hasNoCoveringQuota := limitedResourceNamesSet.Difference(restrictedResourcesSet)
	if len(hasNoCoveringQuota) > 0 {
		return quotas, admission.NewForbidden(a, fmt.Errorf("insufficient quota to consume: %v", strings.Join(hasNoCoveringQuota.List(), ",")))
	}

	// 验证每个启用了受限访问的 scope 都有对应配额覆盖；
	// 否则拒绝请求。
	scopesHasNoCoveringQuota, err := evaluator.UncoveredQuotaScopes(limitedScopes, restrictedScopes)
	if err != nil {
		return quotas, err
	}
	if len(scopesHasNoCoveringQuota) > 0 {
		return quotas, fmt.Errorf("insufficient quota to match these scopes: %v", scopesHasNoCoveringQuota)
	}

	if len(interestingQuotaIndexes) == 0 {
		return quotas, nil
	}

	// 以下在副本上累加用量，调用方在全部请求检查通过后再持久化。
	outQuotas, err := copyQuotas(quotas)
	if err != nil {
		return nil, err
	}

	for _, index := range interestingQuotaIndexes {
		resourceQuota := outQuotas[index]
		deltaUsage, ok := deltaUsageIndexMap[index]
		if !ok {
			continue
		}

		hardResources := quota.ResourceNames(resourceQuota.Status.Hard)
		requestedUsage := quota.Mask(deltaUsage, hardResources)
		newUsage := quota.Add(resourceQuota.Status.Used, requestedUsage)

		// 更新 status 子资源时不再校验配额，否则配额控制器自身回写 used 会被拦截。
		if a.GetSubresource() != "status" {
			maskedNewUsage := quota.Mask(newUsage, quota.ResourceNames(requestedUsage))

			if allowed, exceeded := quota.LessThanOrEqual(maskedNewUsage, resourceQuota.Status.Hard); !allowed {
				// 错误消息只列出超出的资源，避免泄露完整配额详情。
				failedRequestedUsage := quota.Mask(requestedUsage, exceeded)
				failedUsed := quota.Mask(resourceQuota.Status.Used, exceeded)
				failedHard := quota.Mask(resourceQuota.Status.Hard, exceeded)
				return nil, admission.NewForbidden(a,
					fmt.Errorf("exceeded quota: %s, requested: %s, used: %s, limited: %s",
						resourceQuota.Name,
						prettyPrint(failedRequestedUsage),
						prettyPrint(failedUsed),
						prettyPrint(failedHard)))
			}
		}

		// 更新为新的用量数值。
		outQuotas[index].Status.Used = newUsage
	}

	return outQuotas, nil
}

// getScopeSelectorsFromQuota 把配额的 Scopes 与 ScopeSelector 统一转换成选择器表达式。
func getScopeSelectorsFromQuota(quota corev1.ResourceQuota) []corev1.ScopedResourceSelectorRequirement {
	selectors := []corev1.ScopedResourceSelectorRequirement{}
	for _, scope := range quota.Spec.Scopes {
		selectors = append(selectors, corev1.ScopedResourceSelectorRequirement{
			ScopeName: scope,
			Operator:  corev1.ScopeSelectorOpExists})
	}
	if quota.Spec.ScopeSelector != nil {
		selectors = append(selectors, quota.Spec.ScopeSelector.MatchExpressions...)
	}
	return selectors
}

// Evaluate 是 Evaluator 的实现：把请求交给按命名空间聚合的 worker 并等待结果。
func (e *quotaEvaluator) Evaluate(a admission.Attributes) error {
	// 首次调用时启动 worker，保证即便装配层忘记显式启动也能工作。
	e.init.Do(e.start)

	// 该资源是否被忽略？
	gvr := a.GetResource()
	gr := gvr.GroupResource()
	if _, ok := e.ignoredResources[gr]; ok {
		return nil
	}

	// 如果还不知道如何评估该资源的用量，就创建一个 evaluator。
	evaluator := e.registry.Get(gr)
	if evaluator == nil {
		// 若此前没有注册过 evaluator，则创建一个对象计数评估器。
		// 未知资源退化为“对象计数”评估器，至少能支持 count/<resource> 形式的配额。
		evaluator = generic.NewObjectCountEvaluator(gr, nil, "")
		e.registry.Add(evaluator)
		klog.Infof("quota admission added evaluator for: %s", gr)
	}
	// 检查该类型下的操作是否可能改变任何受配额管理的资源；
	// 没有影响则直接返回。
	if !evaluator.Handles(a) {
		return nil
	}
	waiter := newAdmissionWaiter(a)

	e.addWork(waiter)

	// 超时后返回 500：此处无法确定配额是否满足，不能默认放行。
	select {
	case <-waiter.finished:
	case <-time.After(10 * time.Second):
		return apierrors.NewInternalError(fmt.Errorf("resource quota evaluation timed out"))
	}

	return waiter.result
}

// addWork 把请求加入对应命名空间的待处理列表。
func (e *quotaEvaluator) addWork(a *admissionWaiter) {
	e.workLock.Lock()
	defer e.workLock.Unlock()

	ns := a.attributes.GetNamespace()
	// 先入队再写 map：getWork 取到条目后会获取同一把锁，
	// 因此这里对 work/dirtyWork 的写入必然对它可见。
	e.queue.Add(ns)

	if e.inProgress.Has(ns) {
		// 该命名空间正在处理中，放入 dirty 列表待本轮结束后立即再处理。
		e.dirtyWork[ns] = append(e.dirtyWork[ns], a)
		return
	}

	e.work[ns] = append(e.work[ns], a)
}

// completeWork 结束一个命名空间的处理，并把 dirty 列表提升为下一轮工作。
func (e *quotaEvaluator) completeWork(ns string) {
	e.workLock.Lock()
	defer e.workLock.Unlock()

	e.queue.Done(ns)
	e.work[ns] = e.dirtyWork[ns]
	delete(e.dirtyWork, ns)
	e.inProgress.Delete(ns)
}

// getWork 返回一个命名空间、该命名空间下的工作项列表和一个 shutdown 标志。
// 若非 shutdown，返回后最终必须针对该命名空间调用一次 completeWork
// （无论工作项列表是否为空）。
func (e *quotaEvaluator) getWork() (string, []*admissionWaiter, bool) {
	ns, shutdown := e.queue.Get()
	if shutdown {
		return "", []*admissionWaiter{}, shutdown
	}

	e.workLock.Lock()
	defer e.workLock.Unlock()
	// 取出工作后立即标记 inProgress，之后 addWork 会转入 dirtyWork，
	// 从而保证同一命名空间不会有两个 worker 并发处理。

	work := e.work[ns]
	delete(e.work, ns)
	delete(e.dirtyWork, ns)
	e.inProgress.Insert(ns)
	return ns, work, false
}

// prettyPrint 按错误消息的用途格式化资源列表；
// 输出按资源名升序排列。
func prettyPrint(item corev1.ResourceList) string {
	parts := []string{}
	keys := []string{}
	for key := range item {
		keys = append(keys, string(key))
	}
	// 排序保证错误消息稳定，便于测试断言与日志比对。
	sort.Strings(keys)
	for _, key := range keys {
		value := item[corev1.ResourceName(key)]
		constraint := key + "=" + value.String()
		parts = append(parts, constraint)
	}
	return strings.Join(parts, ",")
}

// prettyPrintResourceNames 把资源名排序后拼接，用于稳定的错误消息。
func prettyPrintResourceNames(a []corev1.ResourceName) string {
	values := []string{}
	for _, value := range a {
		values = append(values, string(value))
	}
	sort.Strings(values)
	return strings.Join(values, ",")
}

// hasUsageStats 当 interestingResources 中每个硬约束都有当前用量值时返回 true。
func hasUsageStats(resourceQuota *corev1.ResourceQuota, interestingResources []corev1.ResourceName) bool {
	interestingSet := quota.ToSet(interestingResources)
	for resourceName := range resourceQuota.Status.Hard {
		if !interestingSet.Has(string(resourceName)) {
			continue
		}
		if _, found := resourceQuota.Status.Used[resourceName]; !found {
			return false
		}
	}
	return true
}
