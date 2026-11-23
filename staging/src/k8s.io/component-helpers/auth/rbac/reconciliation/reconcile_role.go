/*
Copyright 2017 The Kubernetes Authors.

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

package reconciliation

import (
	"fmt"
	"reflect"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/component-helpers/auth/rbac/validation"
)

// ReconcileOperation 描述一次对账最终需要执行的 API 动作。
// 用字符串而不是枚举常量比较，是为了让结果能直接写进日志和事件里，读起来不需要查表。
type ReconcileOperation string

var (
	// ReconcileCreate 表示对象不存在，需要创建。
	ReconcileCreate ReconcileOperation = "create"
	// ReconcileUpdate 表示对象已存在，需要原地更新。
	ReconcileUpdate ReconcileOperation = "update"
	// ReconcileRecreate 表示存在不可变字段变化，需要删除后重建。
	ReconcileRecreate ReconcileOperation = "recreate"
	// ReconcileNone 表示现状与期望一致，无需写 API。
	ReconcileNone ReconcileOperation = "none"
)

// RuleOwnerModifier 把对角色的读写抽象出来，使对账逻辑不依赖具体的客户端实现。
// 这样调用方既能传真实的 API client，也能在测试里传内存假对象。
type RuleOwnerModifier interface {
	// Get 按命名空间和名称读取角色；不存在时返回 IsNotFound 错误。
	Get(namespace, name string) (RuleOwner, error)
	// Create 创建角色，返回服务端创建后的对象。
	Create(RuleOwner) (RuleOwner, error)
	// Update 更新角色，返回服务端更新后的对象。
	Update(RuleOwner) (RuleOwner, error)
}

// RuleOwner 是 Role 与 ClusterRole 的公共视图。Role 和 ClusterRole 在 API 中是两个类型，
// 但对账关心的是命名空间、规则、注解这些相同属性，抽象成接口后同一套对账流程就能同时服务两者。
type RuleOwner interface {
	// GetObject 返回底层 API 对象，供需要 runtime.Object 的代码使用。
	GetObject() runtime.Object
	// GetNamespace 返回命名空间；集群角色返回空串。
	GetNamespace() string
	// GetName 返回角色名称，作为对账定位的主键。
	GetName() string
	// GetLabels 返回标签，供对账合并元数据。
	GetLabels() map[string]string
	// SetLabels 整体替换标签。
	SetLabels(map[string]string)
	// GetAnnotations 返回注解，其中可能包含禁止自动更新的标记。
	GetAnnotations() map[string]string
	// SetAnnotations 整体替换注解。
	SetAnnotations(map[string]string)
	// GetRules 返回当前权限规则。
	GetRules() []rbacv1.PolicyRule
	// SetRules 整体替换权限规则。
	SetRules([]rbacv1.PolicyRule)
	// GetAggregationRule 返回聚合规则；Role 的实现恒返回 nil。
	GetAggregationRule() *rbacv1.AggregationRule
	// SetAggregationRule 设置聚合规则；nil 表示取消聚合。
	SetAggregationRule(*rbacv1.AggregationRule)
	// DeepCopyRuleOwner 深拷贝角色，使对账可以在副本上修改而不影响集群中读到的原对象。
	DeepCopyRuleOwner() RuleOwner
}

// ReconcileRoleOptions 把一次对账的输入和策略打包在一起。
// Confirm 与 RemoveExtraPermissions 是两个正交的开关：前者决定要不要真正写 API，
// 后者决定“合并”还是“覆盖”语义；分开后同一个计算结果既能先 dry-run 预览，也能再确认执行。
type ReconcileRoleOptions struct {
	// Role 是期望的角色状态；对账以它为基准计算差异。
	Role RuleOwner
	// Confirm 决定是否真正写 API；为 false 时只返回 dry-run 的预览结果。
	Confirm bool
	// RemoveExtraPermissions 决定语义：true 为严格覆盖，false 为只补齐缺失规则的“并集”。
	RemoveExtraPermissions bool
	// Client 用于读取现有角色，并在 Confirm=true 时创建或更新角色。
	Client RuleOwnerModifier
}

// ReconcileClusterRoleResult 既是对账的产物，也是 dry-run 的预览报告。
// MissingRules/ExtraRules 等信息与最终 Role 分开保存，是为了让调用方既能直接使用结果对象，
// 也能在没有真正写 API 时知道“将会”发生哪些变化。
type ReconcileClusterRoleResult struct {
	// Role 是对账后得到的角色；dry-run 或对象受保护时，它不会写入 API Server。
	Role RuleOwner

	// MissingRules 是期望中存在、但当前持久化对象里缺失的规则。
	MissingRules []rbacv1.PolicyRule
	// ExtraRules 是当前持久化对象中多出来的权限。
	ExtraRules []rbacv1.PolicyRule

	// MissingAggregationRuleSelectors 是期望中存在、但当前对象缺失的聚合选择器。
	MissingAggregationRuleSelectors []metav1.LabelSelector
	// ExtraAggregationRuleSelectors 是当前对象中多余的聚合选择器。
	ExtraAggregationRuleSelectors []metav1.LabelSelector

	// Operation 是对账需要执行的 API 操作。
	// 无需变更时为 ReconcileNone；Confirm=false（dry-run）或 Protected=true（对象选择退出对账）时，
	// 该操作只是计划值，并未真正执行；其他情况下操作已落盘。
	Operation ReconcileOperation
	// Protected 表示现有角色通过注解声明拒绝自动更新，对账被短路。
	Protected bool
}

// Run 从尝试计数 0 开始执行一次对账。把递归实现藏在私有的 run 里，
// 是为了对外只暴露一个干净的入口，同时让重试逻辑保持内部细节。
func (o *ReconcileRoleOptions) Run() (*ReconcileClusterRoleResult, error) {
	return o.run(0)
}

// run 在对账对象被并发创建或删除时有限次重试。重试只针对这类“对象状态在我们读取之后发生变化”
// 的情况；普通的更新冲突交给更上层的客户端重试机制处理，这里不重复做。
func (o *ReconcileRoleOptions) run(attempts int) (*ReconcileClusterRoleResult, error) {
	// 限制重试次数，避免角色在对账期间反复出现/消失时无限循环。
	// 更新冲突由更上层的客户端重试机制处理，这里不重复做。
	if attempts > 2 {
		return nil, fmt.Errorf("exceeded maximum attempts")
	}

	var result *ReconcileClusterRoleResult

	existing, err := o.Client.Get(o.Role.GetNamespace(), o.Role.GetName())
	switch {
	case errors.IsNotFound(err):
		// 角色不存在时，所有期望规则和聚合选择器都是“缺失”的，直接把期望对象当作创建结果。
		// 这里补一个空的聚合规则只是为了安全地取其选择器列表，并不改变期望对象本身。
		aggregationRule := o.Role.GetAggregationRule()
		if aggregationRule == nil {
			aggregationRule = &rbacv1.AggregationRule{}
		}
		result = &ReconcileClusterRoleResult{
			Role:                            o.Role,
			MissingRules:                    o.Role.GetRules(),
			MissingAggregationRuleSelectors: aggregationRule.ClusterRoleSelectors,
			Operation:                       ReconcileCreate,
		}

	case err != nil:
		return nil, err

	default:
		result, err = computeReconciledRole(existing, o.Role, o.RemoveExtraPermissions)
		if err != nil {
			return nil, err
		}
	}

	// 对象通过注解声明退出对账时，只返回计划不写 API。
	if result.Protected {
		return result, nil
	}
	// dry-run 模式同样只返回计算结果，不执行任何写操作。
	if !o.Confirm {
		return result, nil
	}

	switch result.Operation {
	case ReconcileCreate:
		created, err := o.Client.Create(result.Role)
		// 别的写入方在我们读取“不存在”之后抢先创建了角色，这次创建就会冲突；
		// 重新从 Get 开始对账，才能把已存在对象的实际内容纳入比较。
		if errors.IsAlreadyExists(err) {
			return o.run(attempts + 1)
		}
		if err != nil {
			return nil, err
		}
		result.Role = created

	case ReconcileUpdate:
		updated, err := o.Client.Update(result.Role)
		// 角色在这次更新前被删掉了，之前基于旧对象算出的 update 已无意义；
		// 重跑后会走“不存在 -> 创建”分支，以期望对象为基础重建。
		if errors.IsNotFound(err) {
			return o.run(attempts + 1)
		}
		if err != nil {
			return nil, err
		}
		result.Role = updated

	case ReconcileNone:
		// 已经一致，无需写 API：既减少写压力，也避免刷新 resourceVersion 引发无谓的 watch 事件。

	default:
		return nil, fmt.Errorf("invalid operation: %v", result.Operation)
	}

	return result, nil
}

// computeReconciledRole 计算让现有角色追上期望角色所需的对象与操作。
// 这是纯计算阶段：不接触 API Server，只依据现有对象、期望对象和覆盖策略得出结果；
// dry-run 与真实写入共用它，保证两者看到的差异一致。
func computeReconciledRole(existing, expected RuleOwner, removeExtraPermissions bool) (*ReconcileClusterRoleResult, error) {
	result := &ReconcileClusterRoleResult{Operation: ReconcileNone}

	// 已有对象可以通过注解显式声明“不接受自动更新”。这是运维人员的逃生舱：
	// 一旦打了这个标记，对账到这里就停止，后续任何差异都只报告不落盘。
	result.Protected = (existing.GetAnnotations()[rbacv1.AutoUpdateAnnotationKey] == "false")

	// 以现有对象的深拷贝为底稿，而不是从零构造：保留我们不关心的字段
	// （如 ResourceVersion、其他控制器添加的注解），避免更新时误伤。
	result.Role = existing.DeepCopyRuleOwner()

	// 合并注解与标签；键冲突时以现有对象的值为准，不覆盖别人写的元数据。
	result.Role.SetAnnotations(merge(expected.GetAnnotations(), result.Role.GetAnnotations()))
	if !reflect.DeepEqual(result.Role.GetAnnotations(), existing.GetAnnotations()) {
		result.Operation = ReconcileUpdate
	}
	result.Role.SetLabels(merge(expected.GetLabels(), result.Role.GetLabels()))
	if !reflect.DeepEqual(result.Role.GetLabels(), existing.GetLabels()) {
		result.Operation = ReconcileUpdate
	}

	// 计算多余与缺失的规则。聚合角色的规则由控制器动态汇总，静态 Rules 为空只是表象；
	// 若两边都是聚合角色，比较空规则集会得出毫无意义的“多余规则”，因此直接跳过。
	if expected.GetAggregationRule() == nil || existing.GetAggregationRule() == nil {
		_, result.ExtraRules = validation.Covers(expected.GetRules(), existing.GetRules())
	}
	_, result.MissingRules = validation.Covers(existing.GetRules(), expected.GetRules())

	switch {
	case !removeExtraPermissions && len(result.MissingRules) > 0:
		// 合并语义下只补齐缺失的规则，绝不碰已有对象上的其他授权。
		// 这样 bootstrap 阶段追加权限时不会误删管理员手工加的临时规则。
		result.Role.SetRules(append(result.Role.GetRules(), result.MissingRules...))
		result.Operation = ReconcileUpdate

	case removeExtraPermissions && (len(result.MissingRules) > 0 || len(result.ExtraRules) > 0):
		// 精确语义下只要有任何偏差就整体替换，保证集群状态收敛到期望集合。
		// 这只是把“差异”变成一次全量覆盖，对最终权限集合而言仍然是幂等的。
		result.Role.SetRules(expected.GetRules())
		result.Operation = ReconcileUpdate
	}

	// 同样计算聚合选择器的多余与缺失项，供调用方生成差异报告。
	_, result.ExtraAggregationRuleSelectors = aggregationRuleCovers(expected.GetAggregationRule(), existing.GetAggregationRule())
	_, result.MissingAggregationRuleSelectors = aggregationRuleCovers(existing.GetAggregationRule(), expected.GetAggregationRule())

	switch {
	case expected.GetAggregationRule() == nil && existing.GetAggregationRule() != nil:
		// 普通角色不应该残留聚合规则，否则规则来源会变得含混不清，所以无条件清掉。
		result.Role.SetAggregationRule(nil)
		result.Operation = ReconcileUpdate

	case !removeExtraPermissions && len(result.MissingAggregationRuleSelectors) > 0:
		// 和普通规则一样，合并语义只追加缺失的选择器；
		// 若无聚合规则则就地造一个空规则作为容器，避免碰到 nil 指针。
		aggregationRule := result.Role.GetAggregationRule()
		if aggregationRule == nil {
			aggregationRule = &rbacv1.AggregationRule{}
		}
		aggregationRule.ClusterRoleSelectors = append(aggregationRule.ClusterRoleSelectors, result.MissingAggregationRuleSelectors...)
		result.Role.SetAggregationRule(aggregationRule)
		result.Operation = ReconcileUpdate

	case removeExtraPermissions && (len(result.MissingAggregationRuleSelectors) > 0 || len(result.ExtraAggregationRuleSelectors) > 0):
		// 精确语义下整体替换聚合规则。期望侧为 nil 时这里会直接清空，
		// 与上面“普通角色不该保留聚合规则”的处理保持一致。
		result.Role.SetAggregationRule(expected.GetAggregationRule())
		result.Operation = ReconcileUpdate
	}

	return result, nil
}

// merge 合并多个 map，靠后的 map 中同名键的值优先。
// 调用方按“期望值在前、现有值在后”的顺序传入，于是现有对象上的注解和标签优先，
// 这是合并语义的一部分——对账不应覆盖管理员手工加的元数据。
// 返回 nil 而不是空 map 也是有意的：没合并任何内容时保持字段缺省，避免无谓更新。
func merge(maps ...map[string]string) map[string]string {
	var output map[string]string = nil
	for _, m := range maps {
		if m != nil && output == nil {
			output = map[string]string{}
		}
		for k, v := range m {
			output[k] = v
		}
	}
	return output
}

// aggregationRuleCovers 判断 ownerRule 的选择器是否在语义上覆盖 servantRule 的选择器。
// 返回是否完全覆盖，以及未被覆盖的选择器列表。
// nil 表示“没有聚合规则”，不等价于“有一条空规则的聚合角色”：
// owner 为 nil 时只有 servant 也为 nil 才算覆盖；servant 为 nil 时任何 owner 都天然覆盖它。
// 这个不对称性正是这里要单独列举 nil 分支的原因。
func aggregationRuleCovers(ownerRule, servantRule *rbacv1.AggregationRule) (bool, []metav1.LabelSelector) {
	switch {
	case ownerRule == nil && servantRule == nil:
		return true, []metav1.LabelSelector{}
	case ownerRule == nil && servantRule != nil:
		return false, servantRule.ClusterRoleSelectors
	case ownerRule != nil && servantRule == nil:
		return true, []metav1.LabelSelector{}

	}

	ownerSelectors := ownerRule.ClusterRoleSelectors
	servantSelectors := servantRule.ClusterRoleSelectors
	uncoveredSelectors := []metav1.LabelSelector{}

	// 逐个查找而非排序后比较：选择器的数量很少，直接两重循环更简单，
	// 也天然容忍顺序差异。用语义相等而不是 ==，是为了忽略 map/slice 表示上的无关差别。
	for _, servantSelector := range servantSelectors {
		covered := false
		for _, ownerSelector := range ownerSelectors {
			if equality.Semantic.DeepEqual(ownerSelector, servantSelector) {
				covered = true
				break
			}
		}
		if !covered {
			uncoveredSelectors = append(uncoveredSelectors, servantSelector)
		}
	}

	return (len(uncoveredSelectors) == 0), uncoveredSelectors
}
