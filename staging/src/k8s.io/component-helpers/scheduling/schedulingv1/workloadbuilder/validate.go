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

package workloadbuilder

import (
	"context"
	"fmt"
	"slices"

	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// ValidationInput 是 Validate 的入参，承载更新期校验所需的上下文。
// 零值表示“创建，且没有旧对象”，这是最常见的情形；启用了 DisableDeclarativeValidation
// 的调用方始终传零值即可。只有需要触发更新期专用声明式校验（如字段不可变、ratchet）时才填 OldRoot。
type ValidationInput struct {
	// OldRoot 指向上一次持久化的 WorkloadItem：创建时为 nil，更新时非 nil。
	// Validate 依据它推断操作类型（create 还是 update），从而恰好在设置了 OldRoot 时
	// 才运行更新期专用的声明式检查。
	//
	// 旧对象上只会读取 OldRoot.Name（用于和新 root 关联）与 OldRoot.Input（旧版本数据）。
	// Path、DefaultConfig、Callbacks 以及 Input.*.PathElements 在旧对象上都会被忽略，
	// 因为解析后的配置和错误路径一律取自新 root；调用方无需为旧对象填写这些字段。
	OldRoot *WorkloadItem
}

// Validate 对输入执行两层校验：默认由生成代码完成声明式校验，再加上控制器自身的策略约束。
// 创建时传 ValidationInput 零值；更新时把 OldRoot 设为先前的 WorkloadItem，
// 操作类型（create/update）由 OldRoot 是否为 nil 推断。
// ctx 用于传递给生成代码的校验函数；返回的 field.ErrorList 中，每个节点的 building-block
// 错误都相对于该节点的 Path 定位，便于控制器把错误映射回自己的 API 字段。
func (b *Builder) Validate(ctx context.Context, input ValidationInput) field.ErrorList {
	// 由已有持久化 Workload 构造出的 Builder 没有 WorkloadItem 树可供校验：
	// 该对象在 apiserver 阶段已经校验过，这种 Builder 只用于物化 PodGroup。
	// 因此对它调用 Validate 属于调用方式错误（InternalError），而不是“校验通过”。
	if b.existingWorkload != nil {
		return field.ErrorList{field.InternalError(nil,
			fmt.Errorf("cannot validate a builder constructed from an existing workload"))}
	}
	if b.root == nil {
		// 没有根节点意味着 Builder 构造不完整，无法继续任何校验。
		return field.ErrorList{field.Invalid(nil, nil, "invalid builder: missing root WorkloadItem")}
	}

	// 根路径用于包裹那些无法定位到具体字段的调用错误。
	rootPath := b.root.Path

	// 新旧 root 通过名字关联。名字不一致说明调用方配对了两棵无关的树，
	// 这是调用契约错误（InternalError），而不是面向用户的字段校验错误。
	if input.OldRoot != nil && input.OldRoot.Name != b.root.Name {
		return field.ErrorList{field.InternalError(rootPath,
			fmt.Errorf("old root name %q does not match new root name %q", input.OldRoot.Name, b.root.Name))}
	}

	// 先做全树结构性校验（名字唯一、输入 union 等）；失败时同样按调用错误处理，
	// 因为这类树无法通过 Builder 的构造契约，后续字段级校验已无意义。
	if err := validateWorkloadItemTree(b.root); err != nil {
		return field.ErrorList{field.InternalError(rootPath, err)}
	}

	op := operation.Operation{Type: operation.Create}
	if input.OldRoot != nil {
		// 更新时带上旧对象，声明式校验才能允许"保持不变"等更新专用规则。
		op = operation.Operation{Type: operation.Update}
	}

	return b.validateItem(ctx, op, b.root, input.OldRoot)
}

// validateWorkloadItemTree 对整棵树做遍历，检查每个节点自身的结构约束：
// 节点非 nil、名字非空、名字全树唯一，以及输入块的叶子/复合 union 合法。
// 模板名必须全树唯一，因为 Workload 以它作为模板索引，重名会让生成的对象互相覆盖。
// 采用 seen 集合记录已出现过的名字；返回首个发现的错误，结构错误无需收集全部。
func validateWorkloadItemTree(root *WorkloadItem) error {
	seen := make(map[string]struct{})

	var validate func(item *WorkloadItem) error
	validate = func(item *WorkloadItem) error {
		// nil 节点无法正常读取字段，属于构造错误，直接拒绝。
		if item == nil {
			return fmt.Errorf("workload item cannot be nil")
		}
		// 名字是模板索引和更新时关联新旧节点的键，不能为空。
		if item.Name == "" {
			return fmt.Errorf("workload item name cannot be empty")
		}
		// 重名会让不同节点生成的模板互相覆盖，因此必须在整棵树上查重。
		if _, dup := seen[item.Name]; dup {
			return fmt.Errorf("duplicate workload item name %q: template names must be unique across the Workload", item.Name)
		}
		seen[item.Name] = struct{}{}

		// 检查叶子/复合构建块是否被交叉设置（交叉设置会被静默忽略）。
		if err := validateItemInputUnion(item); err != nil {
			return err
		}

		// 递归校验子树；只要有一个子节点出错就立即向上返回。
		for _, child := range item.Children {
			if err := validate(child); err != nil {
				// 结构错误只需报第一个，继续遍历也得不到更多信息。
				return err
			}
		}
		return nil
	}

	return validate(root)
}

// validateItemInputUnion 强制 WorkloadInput 的“叶子/复合”互斥约束：
// 复合节点不得设置任何叶子构建块（policy/constraints/disruptionMode/resourceClaims 的 PodGroupData），
// 叶子节点也不得设置任何复合构建块（CompositePodGroupData）。
// 交叉设置的值在构建阶段会被静默忽略，等于悄悄丢弃用户输入，所以必须在此拦下并返回错误。
func validateItemInputUnion(item *WorkloadItem) error {
	in := item.Input

	// 复合节点和叶子节点接受的字段集合互斥。
	// 交叉设置的值在编译阶段会被忽略，静默丢弃用户输入，所以必须在这里拦下。
	if isComposite(item) {
		if in.Policy.PodGroupData != nil {
			return fmt.Errorf("composite workload item %q sets leaf building block: policy", item.Name)
		}
		if in.Constraints.PodGroupData != nil {
			return fmt.Errorf("composite workload item %q sets leaf building block: constraints", item.Name)
		}
		if in.DisruptionMode.PodGroupData != nil {
			return fmt.Errorf("composite workload item %q sets leaf building block: disruptionMode", item.Name)
		}
		if len(in.ResourceClaims.PodGroupData) > 0 {
			return fmt.Errorf("composite workload item %q sets leaf building block: resourceClaims", item.Name)
		}
		return nil
	}

	// 叶子节点：反向检查，不允许出现任何复合构建块。
	if in.Policy.CompositePodGroupData != nil {
		return fmt.Errorf("leaf workload item %q sets composite building block: policy", item.Name)
	}
	if in.Constraints.CompositePodGroupData != nil {
		return fmt.Errorf("leaf workload item %q sets composite building block: constraints", item.Name)
	}
	if in.DisruptionMode.CompositePodGroupData != nil {
		return fmt.Errorf("leaf workload item %q sets composite building block: disruptionMode", item.Name)
	}

	return nil
}

// validateItem 对单个节点依次执行声明式校验和控制器策略校验，然后递归校验子节点。
// 各组错误汇总后一并返回，而不是遇错即停，让调用方一次拿到完整的字段错误列表。
// 每个节点的 building-block 错误都相对于自己的 Path 报告，控制器据此映射回自身 API 字段。
// op 为 create/update；item 是当前节点，oldItem 是它在旧树中的对应节点（新建节点为 nil）。
func (b *Builder) validateItem(ctx context.Context, op operation.Operation, item, oldItem *WorkloadItem) field.ErrorList {
	var allErrs field.ErrorList

	if !b.opts.DisableDeclarativeValidation {
		// 有些控制器的 APIServer 校验已经跑过同一套声明式规则，
		// 这里允许关闭以免对相同输入重复校验。
		allErrs = append(allErrs, b.validateItemDeclarative(ctx, op, item, oldItem)...)
	}

	resolvedConfig := resolveSchedulingConfig(item)
	allErrs = append(allErrs, b.validateAllowedSchedulingPolicies(item, resolvedConfig)...)
	allErrs = append(allErrs, b.validateAllowedDisruptionModes(item, resolvedConfig)...)
	allErrs = append(allErrs, b.validateDisruptionModeCompatibleWithSchedulingPolicy(item, resolvedConfig)...)

	// oldItem 是 item 在旧树中的对应节点（创建时或本轮新增的节点为 nil）。
	// 子节点只在同一层级内按名字与旧节点关联；因此即使名字不变，只要层级位置变了，
	// 就会被视为新增节点，从而触发更新期的完整校验（ratchet 只认同一位置）。
	var oldChildrenByName map[string]*WorkloadItem
	if oldItem != nil {
		oldChildrenByName = make(map[string]*WorkloadItem, len(oldItem.Children))
		for _, oldChild := range oldItem.Children {
			oldChildrenByName[oldChild.Name] = oldChild
		}
	}

	// 递归子节点，并按名字取出对应的旧节点；map 中不存在时拿到 nil，按新增处理。
	for _, child := range item.Children {
		allErrs = append(allErrs, b.validateItem(ctx, op, child, oldChildrenByName[child.Name])...)
	}
	return allErrs
}

// appendPathElements 把输入块自带的相对路径元素依次追加到节点根路径之后，
// 使错误能定位到控制器 API 中的具体字段，而不是笼统地指向整个 Workload。
// elements 为空时原样返回 rootPath。
func appendPathElements(rootPath *field.Path, elements []string) *field.Path {
	path := rootPath
	for _, el := range elements {
		path = path.Child(el)
	}
	return path
}

// validateItemDeclarative 按节点类型分派声明式校验：复合节点走复合构建块集合，
// 叶子节点走叶子构建块集合，调用方无需关心当前节点属于哪一种。
// ctx 与 op 原样透传给对应的校验函数，oldItem 提供更新期所需的旧值。
func (b *Builder) validateItemDeclarative(ctx context.Context, op operation.Operation, item, oldItem *WorkloadItem) field.ErrorList {
	if isComposite(item) {
		return b.validateCompositePodGroupItemDeclarative(ctx, op, item, oldItem)
	}
	return b.validatePodGroupItemDeclarative(ctx, op, item, oldItem)
}

// validatePodGroupItemDeclarative 对叶子节点的四个构建块逐项执行声明式校验：
// policy、constraints、disruptionMode 以及 resourceClaims。
// 每个字段块仅在有数据（非 nil）时校验：nil 表示未提供，交由默认化逻辑处理；
// 同时传入旧值以触发更新期专用规则（例如字段不可变）。错误路径由节点 Path 与字段自带的
// PathElements 拼接而成，resourceClaims 还会带上数组下标。
func (b *Builder) validatePodGroupItemDeclarative(ctx context.Context, op operation.Operation,
	item, oldItem *WorkloadItem) field.ErrorList {
	var allErrs field.ErrorList

	// 每个字段块只在有数据时校验；nil 表示未提供，由默认化逻辑处理。
	// 传入旧值是为了触发更新期专用规则（如字段不可变）。
	if item.Input.Policy.PodGroupData != nil {
		path := appendPathElements(item.Path, item.Input.Policy.PathElements)
		var oldData *schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy
		if oldItem != nil && oldItem.Input.Policy.PodGroupData != nil {
			oldData = oldItem.Input.Policy.PodGroupData
		}
		allErrs = append(allErrs, schedulingv1alpha3.Validate_WorkloadPodGroupSchedulingPolicy(ctx, op, path, item.Input.Policy.PodGroupData, oldData)...)
	}
	if item.Input.Constraints.PodGroupData != nil {
		path := appendPathElements(item.Path, item.Input.Constraints.PathElements)
		var oldData *schedulingv1alpha3.WorkloadPodGroupSchedulingConstraints
		if oldItem != nil && oldItem.Input.Constraints.PodGroupData != nil {
			oldData = oldItem.Input.Constraints.PodGroupData
		}
		allErrs = append(allErrs, schedulingv1alpha3.Validate_WorkloadPodGroupSchedulingConstraints(ctx, op, path, item.Input.Constraints.PodGroupData, oldData)...)
	}
	if item.Input.DisruptionMode.PodGroupData != nil {
		path := appendPathElements(item.Path, item.Input.DisruptionMode.PathElements)
		var oldData *schedulingv1alpha3.WorkloadPodGroupDisruptionMode
		if oldItem != nil && oldItem.Input.DisruptionMode.PodGroupData != nil {
			oldData = oldItem.Input.DisruptionMode.PodGroupData
		}
		allErrs = append(allErrs, schedulingv1alpha3.Validate_WorkloadPodGroupDisruptionMode(ctx, op, path, item.Input.DisruptionMode.PodGroupData, oldData)...)
	}

	allErrs = append(allErrs, b.validateResourceClaims(ctx, op, item, oldItem)...)

	return allErrs
}

// validateCompositePodGroupItemDeclarative 对复合节点的 policy、constraints、disruptionMode
// 三个构建块执行声明式校验。与叶子版本类似：仅在数据非 nil 时校验，并传入旧值以启用更新期规则；
// 复合节点没有 resourceClaims 构建块，因此无需校验资源声明。
func (b *Builder) validateCompositePodGroupItemDeclarative(ctx context.Context, op operation.Operation, item, oldItem *WorkloadItem) field.ErrorList {
	var allErrs field.ErrorList

	if item.Input.Policy.CompositePodGroupData != nil {
		path := appendPathElements(item.Path, item.Input.Policy.PathElements)
		var oldData *schedulingv1alpha3.WorkloadCompositePodGroupSchedulingPolicy
		if oldItem != nil && oldItem.Input.Policy.CompositePodGroupData != nil {
			oldData = oldItem.Input.Policy.CompositePodGroupData
		}
		allErrs = append(allErrs, schedulingv1alpha3.Validate_WorkloadCompositePodGroupSchedulingPolicy(ctx, op, path, item.Input.Policy.CompositePodGroupData, oldData)...)
	}
	if item.Input.Constraints.CompositePodGroupData != nil {
		path := appendPathElements(item.Path, item.Input.Constraints.PathElements)
		var oldData *schedulingv1alpha3.WorkloadCompositePodGroupSchedulingConstraints
		if oldItem != nil && oldItem.Input.Constraints.CompositePodGroupData != nil {
			oldData = oldItem.Input.Constraints.CompositePodGroupData
		}
		allErrs = append(allErrs, schedulingv1alpha3.Validate_WorkloadCompositePodGroupSchedulingConstraints(ctx, op, path, item.Input.Constraints.CompositePodGroupData, oldData)...)
	}
	if item.Input.DisruptionMode.CompositePodGroupData != nil {
		path := appendPathElements(item.Path, item.Input.DisruptionMode.PathElements)
		var oldData *schedulingv1alpha3.WorkloadCompositePodGroupDisruptionMode
		if oldItem != nil && oldItem.Input.DisruptionMode.CompositePodGroupData != nil {
			oldData = oldItem.Input.DisruptionMode.CompositePodGroupData
		}
		allErrs = append(allErrs, schedulingv1alpha3.Validate_WorkloadCompositePodGroupDisruptionMode(ctx, op, path, item.Input.DisruptionMode.CompositePodGroupData, oldData)...)
	}

	return allErrs
}

// validateResourceClaims 逐项校验资源声明（resourceClaims）构建块。
// 声明以名字而非位置标识，所以更新时按名字把新项与旧项配对，避免元素顺序变化造成误判；
// 找不到旧项的新声明按“新增”语义校验。错误路径由节点 Path、PathElements 与数组下标
// （path.Index(idx)）共同组成，新老都为空时直接返回 nil。
func (b *Builder) validateResourceClaims(ctx context.Context, op operation.Operation, item, oldItem *WorkloadItem) field.ErrorList {
	newClaims := item.Input.ResourceClaims.PodGroupData
	var oldClaims []schedulingv1alpha3.WorkloadPodGroupResourceClaim
	if oldItem != nil {
		oldClaims = oldItem.Input.ResourceClaims.PodGroupData
	}
	if len(newClaims) == 0 && len(oldClaims) == 0 {
		return nil
	}

	path := appendPathElements(item.Path, item.Input.ResourceClaims.PathElements)

	// 按名字建索引后，新增项自然取不到旧值，会以"新增"语义校验。
	oldByName := make(map[string]*schedulingv1alpha3.WorkloadPodGroupResourceClaim, len(oldClaims))
	for i := range oldClaims {
		oldByName[oldClaims[i].Name] = &oldClaims[i]
	}

	var allErrs field.ErrorList
	for idx := range newClaims {
		data := &newClaims[idx]
		oldData := oldByName[data.Name]
		allErrs = append(allErrs, schedulingv1alpha3.Validate_WorkloadPodGroupResourceClaim(ctx, op, path.Index(idx), data, oldData)...)
	}
	return allErrs
}

// validateAllowedSchedulingPolicies 用控制器的白名单过滤解析后的调度策略。
// 判定基于解析合并后的配置（resolvedConfig）而不是原始输入，避免被回调改写过的策略绕过检查；
// Basic 与 Gang 分别检查，不在白名单中时用 Forbidden 错误报告在策略块对应的字段路径上。
func (b *Builder) validateAllowedSchedulingPolicies(item *WorkloadItem, resolvedConfig *SchedulingConfig) field.ErrorList {
	if resolvedConfig == nil || resolvedConfig.Policy == nil {
		return nil
	}

	path := appendPathElements(item.Path, item.Input.Policy.PathElements)

	var allErrs field.ErrorList
	policy := resolvedConfig.Policy
	if policy.Basic != nil && !slices.Contains(b.opts.AllowedPolicies, BasicPolicy) {
		allErrs = append(allErrs, field.Forbidden(
			path, "basic scheduling policy is not supported by this controller"))
	}
	if policy.Gang != nil && !slices.Contains(b.opts.AllowedPolicies, GangPolicy) {
		allErrs = append(allErrs, field.Forbidden(
			path, "gang scheduling policy is not supported by this controller"))
	}
	return allErrs
}

// validateAllowedDisruptionModes 与调度策略白名单同理：基于解析后的配置判断中断模式，
// Single 与 All 分别检查，不在白名单中的模式用 Forbidden 错误报告在 disruptionMode 块的路径上。
// resolvedConfig 或其 DisruptionMode 为 nil 时表示没有该配置，直接放行。
func (b *Builder) validateAllowedDisruptionModes(item *WorkloadItem, resolvedConfig *SchedulingConfig) field.ErrorList {
	if resolvedConfig == nil || resolvedConfig.DisruptionMode == nil {
		return nil
	}

	path := appendPathElements(item.Path, item.Input.DisruptionMode.PathElements)

	var allErrs field.ErrorList
	dm := resolvedConfig.DisruptionMode
	if dm.Single != nil && !slices.Contains(b.opts.AllowedDisruptionModes, SingleMode) {
		allErrs = append(allErrs, field.Forbidden(
			path, "the disruptionMode `single` is not supported by this controller"))
	}
	if dm.All != nil && !slices.Contains(b.opts.AllowedDisruptionModes, AllMode) {
		allErrs = append(allErrs, field.Forbidden(
			path, "the disruptionMode `all` is not supported by this controller"))
	}
	return allErrs
}

// validateDisruptionModeCompatibleWithSchedulingPolicy 补上声明式校验无法表达的跨字段规则：
// Basic 策略下每个 Pod 独立调度，“全部一起中断”（All）没有意义，因此二者互斥。
// 仅当解析后的策略为 Basic 且中断模式为 All 时返回 Invalid 错误；其余组合放行。
func (b *Builder) validateDisruptionModeCompatibleWithSchedulingPolicy(item *WorkloadItem, resolvedConfig *SchedulingConfig) field.ErrorList {
	if resolvedConfig == nil ||
		resolvedConfig.Policy == nil || resolvedConfig.Policy.Basic == nil ||
		resolvedConfig.DisruptionMode == nil || resolvedConfig.DisruptionMode.All == nil {
		return nil
	}

	// Basic 策略下各 Pod 独立调度，"全部一起中断"没有意义，
	// 这条跨字段规则无法用单个字段的声明式校验表达，只能在这里补上。
	path := appendPathElements(item.Path, item.Input.DisruptionMode.PathElements)
	return field.ErrorList{field.Invalid(
		path, "", "the disruptionMode `all` is not supported with the Basic scheduling policy")}
}
