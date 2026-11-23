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

// Package reconciliation 提供 RBAC 角色与绑定的“声明式”对账能力。
//
// 本包负责把期望的 RBAC 绑定与集群中已存在的绑定对齐，同时服务于
// RoleBinding 与 ClusterRoleBinding：调用方描述期望状态，由这里决定是创建、
// 更新还是重建，并通过 Client 接口落地。
package reconciliation

import (
	"fmt"
	"reflect"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// RoleBindingModifier 抽象出对 API Server 的读写操作。
// 之所以设成接口，是为了让对账逻辑可以脱离具体的 informer/client 实现，
// 也便于在测试中精确注入 IsNotFound、IsConflict 等错误分支。
type RoleBindingModifier interface {
	// Get 按命名空间和名称读取绑定；不存在时返回 IsNotFound 错误。
	Get(namespace, name string) (RoleBinding, error)
	// Delete 按名称和 UID 删除绑定，UID 作为防止误删的前置条件。
	Delete(namespace, name string, uid types.UID) error
	// Create 创建绑定，返回服务端创建后的对象。
	Create(RoleBinding) (RoleBinding, error)
	// Update 更新绑定，返回服务端更新后的对象。
	Update(RoleBinding) (RoleBinding, error)
}

// RoleBinding 把 RoleBinding 与 ClusterRoleBinding 两种对象共用的字段
// 收敛成同一组方法；命名空间对象返回非空 namespace，集群对象返回空串。
// 对账流程只需要这些元数据，因此不必让代码依赖具体 API 版本。
type RoleBinding interface {
	// GetObject 返回底层 API 对象，供需要 runtime.Object 的代码使用。
	GetObject() runtime.Object
	// GetNamespace 返回命名空间；集群级绑定返回空串。
	GetNamespace() string
	// GetName 返回绑定名称，作为对账定位的主键。
	GetName() string
	// GetUID 返回对象 UID，删除时用作前置条件。
	GetUID() types.UID
	// GetLabels 返回标签，供对账合并元数据。
	GetLabels() map[string]string
	// SetLabels 整体替换标签。
	SetLabels(map[string]string)
	// GetAnnotations 返回注解，其中可能包含禁止自动更新的标记。
	GetAnnotations() map[string]string
	// SetAnnotations 整体替换注解。
	SetAnnotations(map[string]string)
	// GetRoleRef 返回角色引用；它不可变，变化时必须重建绑定。
	GetRoleRef() rbacv1.RoleRef
	// GetSubjects 返回当前绑定的主体列表。
	GetSubjects() []rbacv1.Subject
	// SetSubjects 整体替换主体列表。
	SetSubjects([]rbacv1.Subject)
	// DeepCopyRoleBinding 深拷贝绑定，使对账可以在副本上修改而不影响原对象。
	DeepCopyRoleBinding() RoleBinding
}

// ReconcileRoleBindingOptions 把一次角色绑定对账的输入与策略打包在一起。
//
// 把“期望值”“是否写入”“如何处理多余主体”拆成独立字段，是为了让同一套计算逻辑
// 同时服务 dry-run 与真实写入：前者只想看差异，后者才把差异落到 API Server。
type ReconcileRoleBindingOptions struct {
	// RoleBinding 是期望的绑定状态；对账以它为基准计算差异。
	RoleBinding RoleBinding
	// Confirm 决定是否真正写 API；为 false 时只返回 dry-run 的预览结果。
	// 默认走 dry-run 是刻意的选择：决策与执行分离，调用方可以先看计划再决定是否提交。
	Confirm bool
	// RemoveExtraSubjects 决定语义：true 为严格覆盖，false 为只补齐缺失主体的“并集”。
	// 两种模式对应不同的权限模型：false 表示“至少包含我”，true 表示“必须恰好等于我”。
	RemoveExtraSubjects bool
	// Client 用于读取现有绑定，并在 Confirm=true 时创建/更新/删除绑定。
	Client RoleBindingModifier
}

// ReconcileClusterRoleBindingResult 承载一次对账的结果与差异报告。
//
// 结果对象同时承载“要写入什么”（RoleBinding、Operation）与“为什么”
// （MissingSubjects、ExtraSubjects、Protected），让调用方既能执行也能解释差异。
// 注意操作可能被跳过：dry-run、受保护对象或不需变更时，Operation 仍是计划值，
// 但并不会落到 API Server，所以不能只看 Operation 就断定发生了写入。
type ReconcileClusterRoleBindingResult struct {
	// RoleBinding 是对账后得到的绑定；dry-run 或对象受保护时，它不会写入 API Server。
	RoleBinding RoleBinding

	// MissingSubjects 是期望中存在、但当前持久化绑定里缺失的主体。
	MissingSubjects []rbacv1.Subject
	// ExtraSubjects 是当前持久化绑定中多余的项。
	ExtraSubjects []rbacv1.Subject

	// Operation 是对账需要执行的 API 操作。
	// 无需变更时为 ReconcileNone；Confirm=false（dry-run）或 Protected=true（对象退出对账）时，
	// 该操作只是计划值，并未真正执行；其他情况下操作已落盘。
	Operation ReconcileOperation
	// Protected 表示现有绑定通过注解声明拒绝自动更新，对账被短路。
	// 通过注解 opt-out 的绑定必须原样保留：控制器不能因为“声明式管理”就覆盖
	// 用户手动维护的绑定，为此在返回结果的字段里单独标记跳过原因。
	Protected bool
}

// Run 执行一次对账；重试计数从零开始，run 在遇到“对象状态在读写之间被改变”时自重试。
func (o *ReconcileRoleBindingOptions) Run() (*ReconcileClusterRoleBindingResult, error) {
	return o.run(0)
}

// run 把重试次数显式作为参数传递，而不是放在 Options 上：
// 重试是针对单次对账的内部状态，不应污染调用方持有的 Options。
func (o *ReconcileRoleBindingOptions) run(attempts int) (*ReconcileClusterRoleBindingResult, error) {
	// 限制重试次数，避免绑定在对账期间反复出现/消失时无限循环。
	// 更新冲突由更上层的客户端重试机制处理，这里不重复做。
	if attempts > 3 {
		return nil, fmt.Errorf("exceeded maximum attempts")
	}

	var result *ReconcileClusterRoleBindingResult

	existingBinding, err := o.Client.Get(o.RoleBinding.GetNamespace(), o.RoleBinding.GetName())
	switch {
	case errors.IsNotFound(err):
		// 对象不存在时没有“计算差异”的必要：全部主体都算缺失，直接创建即可。
		// 这里的期望对象是调用方传入的只读描述，不能被后续写入流程修改。
		result = &ReconcileClusterRoleBindingResult{
			RoleBinding:     o.RoleBinding,
			MissingSubjects: o.RoleBinding.GetSubjects(),
			Operation:       ReconcileCreate,
		}

	case err != nil:
		// 只有 NotFound 才是可恢复的“需要创建”，其余错误（如权限或网络问题）
		// 说明对账前提未知，必须原样上抛。
		return nil, err

	default:
		result, err = computeReconciledRoleBinding(existingBinding, o.RoleBinding, o.RemoveExtraSubjects)
		if err != nil {
			return nil, err
		}
	}

	// 对象声明退出对账时只返回计划，不写 API。
	if result.Protected {
		return result, nil
	}
	// dry-run 模式同样只返回计算结果。
	if !o.Confirm {
		return result, nil
	}

	switch result.Operation {
	case ReconcileRecreate:
		// 先删除旧绑定。
		// roleRef 不可变，改变它只能删后重建；UID 作为前置条件可避免误删
		// 刚被别人替换的同名对象。
		err := o.Client.Delete(existingBinding.GetNamespace(), existingBinding.GetName(), existingBinding.GetUID())
		switch {
		case err == nil, errors.IsNotFound(err):
			// 对象已不存在，符合预期，可以继续创建。
		case errors.IsConflict(err):
			// UID 前置条件冲突，说明看到的对象已不是对账时的那一个，
			// 带着旧数据继续没有意义，重跑整个流程。
			return o.run(attempts + 1)
		default:
			// 其他错误原样上抛。
			return nil, err
		}
		// 删除成功后落到 Create 分支，重建的“删+建”两步共享同一重试预算。
		fallthrough
	case ReconcileCreate:
		created, err := o.Client.Create(result.RoleBinding)
		// 在我们读取“不存在”到执行创建之间，别人可能已创建同名对象；
		// 重跑一次以便基于最新对象重新计算，而不是直接把 AlreadyExists 抛给用户。
		if errors.IsAlreadyExists(err) {
			return o.run(attempts + 1)
		}
		if err != nil {
			return nil, err
		}
		result.RoleBinding = created

	case ReconcileUpdate:
		updated, err := o.Client.Update(result.RoleBinding)
		// 更新时对象若已被删除，下一步应该走创建而非更新，因此重跑整个决策流程。
		if errors.IsNotFound(err) {
			return o.run(attempts + 1)
		}
		if err != nil {
			return nil, err
		}
		result.RoleBinding = updated

	case ReconcileNone:
		// 已经一致时不写 API Server：既减少无谓的写压力，也避免刷新 resourceVersion
		// 引发控制器之间不必要的 watch 事件。

	default:
		return nil, fmt.Errorf("invalid operation: %v", result.Operation)
	}

	return result, nil
}

// computeReconciledRoleBinding 计算让现有绑定追上期望绑定所需的对象与操作，
// 涉及主体、角色引用、标签与注解的差异。
//
// 这是纯函数式的“计划”阶段：不接触 API Server，只依据现有对象与期望对象算出
// 结果；dry-run 与真实写入共用它，保证两者看到的差异完全一致。
func computeReconciledRoleBinding(existing, expected RoleBinding, removeExtraSubjects bool) (*ReconcileClusterRoleBindingResult, error) {
	result := &ReconcileClusterRoleBindingResult{Operation: ReconcileNone}

	// 先检查“退出对账”的注解，再计算任何差异：受保护的绑定即使内容不匹配，
	// 也只能原样返回，避免把用户手动管理的绑定悄悄改掉。
	result.Protected = (existing.GetAnnotations()[rbacv1.AutoUpdateAnnotationKey] == "false")

	// roleRef 不同则整体重建绑定：它在 RBAC API 中被视为不可变字段，无法用 patch/Update 改动；
	// 也正因如此这里不做“最小差异”合并，直接采用期望对象。
	if expected.GetRoleRef() != existing.GetRoleRef() {
		result.RoleBinding = expected
		result.Operation = ReconcileRecreate
		return result, nil
	}

	// 以现有对象的深拷贝为底稿，是为了保留调用方不关心的字段（如 resourceVersion、
	// 其他控制器添加的注解），只改动本函数明确负责的部分。
	result.RoleBinding = existing.DeepCopyRoleBinding()

	// 注解与标签采用“期望值优先、保留已有值”的合并：控制器声明的键由控制器负责，
	// 其余键留给其他参与者，避免把别人写的内容抹掉。
	result.RoleBinding.SetAnnotations(merge(expected.GetAnnotations(), result.RoleBinding.GetAnnotations()))
	// 用合并结果与原对象比较，而不是与期望对象比较：只有“确实要改变现状”才标记 Update。
	if !reflect.DeepEqual(result.RoleBinding.GetAnnotations(), existing.GetAnnotations()) {
		result.Operation = ReconcileUpdate
	}
	result.RoleBinding.SetLabels(merge(expected.GetLabels(), result.RoleBinding.GetLabels()))
	if !reflect.DeepEqual(result.RoleBinding.GetLabels(), existing.GetLabels()) {
		result.Operation = ReconcileUpdate
	}

	// 主体差异单独暴露给结果对象：调用方可能只想知道“缺了谁、多了谁”，
	// 而不关心最终要发什么 API 请求。
	result.MissingSubjects, result.ExtraSubjects = diffSubjectLists(expected.GetSubjects(), existing.GetSubjects())

	switch {
	case !removeExtraSubjects && len(result.MissingSubjects) > 0:
		// “并集”模式下不碰已有主体，只在尾部补齐缺失项，保证别人添加的主体不会被清除。
		result.RoleBinding.SetSubjects(append(result.RoleBinding.GetSubjects(), result.MissingSubjects...))
		result.Operation = ReconcileUpdate

	case removeExtraSubjects && (len(result.MissingSubjects) > 0 || len(result.ExtraSubjects) > 0):
		// “精确”模式下直接换成期望列表：既然任意多余主体都要清掉，逐项增删
		// 反而更复杂且容易漏，整体覆盖更可靠。
		result.RoleBinding.SetSubjects(expected.GetSubjects())
		result.Operation = ReconcileUpdate
	}

	return result, nil
}

// contains 报告 item 是否已出现在 list 中；主体是可比较的结构体，可直接用 == 判断。
func contains(list []rbacv1.Subject, item rbacv1.Subject) bool {
	for _, listItem := range list {
		if listItem == item {
			return true
		}
	}
	return false
}

// diffSubjectLists 求两个主体列表的双向差集：
//
//	list1Only = list1 - list2
//	list2Only = list2 - list1
//
// 两个结果都为空说明两列表集合相等。逐项做集合差而非排序后比较，
// 是因为 Subject 数量很少，线性扫描已足够；
// 额外的 contains(listXOnly, ...) 用于把重复项折叠成一份，
// 保证返回结果语义上是集合，而不是原始列表的副本。
func diffSubjectLists(list1 []rbacv1.Subject, list2 []rbacv1.Subject) (list1Only []rbacv1.Subject, list2Only []rbacv1.Subject) {
	for _, list1Item := range list1 {
		if !contains(list2, list1Item) {
			if !contains(list1Only, list1Item) {
				list1Only = append(list1Only, list1Item)
			}
		}
	}
	for _, list2Item := range list2 {
		if !contains(list1, list2Item) {
			if !contains(list2Only, list2Item) {
				list2Only = append(list2Only, list2Item)
			}
		}
	}
	return
}
