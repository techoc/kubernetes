/*
Copyright 2020 The Kubernetes Authors.

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

// Package nodeaffinity 把 v1.NodeSelector 等 API 类型转换为可高效求值的运行时选择器。
//
// 提供“立即报错”与“延迟报错”两种构造器，是为了兼容不同调用场景：
// API 校验需要立即拿到所有解析错误，而调度过程中只要在没有任何 term 匹配时
// 才关心错误，这样单个非法 term 不会掩盖其他 term 的匹配结果。
//
// 注意本包处理的是节点亲和性（决定 Pod 能落到哪些节点上），与拓扑分布约束
// （TopologySpreadConstraints，决定 Pod 在拓扑域之间如何分散）是两套独立机制：
// 前者是对单个节点的标签/字段做匹配过滤，后者是对一组节点上同类 Pod 的数量做均衡。
package nodeaffinity

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// NodeSelector 是 v1.NodeSelector 的运行时表示。
//
// 包装 LazyErrorNodeSelector 而非直接持有 term 列表，是为了复用同一套匹配逻辑；
// 区别只在于错误已经在构造阶段被检查过，因此 Match 不需要返回 error。
type NodeSelector struct {
	lazy LazyErrorNodeSelector
}

// LazyErrorNodeSelector 是 v1.NodeSelector 的运行时表示，仅在没有任何 term 匹配时才报告解析错误。
//
// 解析错误保存在每个 term 内部，是因为多个 term 之间是“或”关系：
// 一个 term 解析失败不应阻止其他 term 继续参与匹配。
type LazyErrorNodeSelector struct {
	terms []nodeSelectorTerm
}

// NewNodeSelector 构造 NodeSelector；若发现解析错误，则返回聚合后的全部错误。
//
// 对 API 校验等场景，必须在构造阶段一次性拿到所有错误，
// 因此这里把各 term 的 parseErrs 汇总后返回。
// opts 用于给错误信息附加字段路径前缀，便于调用方定位具体出错的字段。
// ns 必须非 nil；缺失的 requiredDuringScheduling 段应在调用前处理为“无此约束”。
func NewNodeSelector(ns *v1.NodeSelector, opts ...field.PathOption) (*NodeSelector, error) {
	lazy := NewLazyErrorNodeSelector(ns, opts...)
	var errs []error
	// term 之间虽是“或”关系，但任一 term 解析失败都会让整个选择器不可信，
	// 因此汇总全部错误而不是遇到第一个就返回。
	for _, term := range lazy.terms {
		if len(term.parseErrs) > 0 {
			errs = append(errs, term.parseErrs...)
		}
	}
	// 只要存在错误就放弃构造：调用方（如 API 校验）需要完整的错误列表。
	if len(errs) != 0 {
		return nil, errors.Flatten(errors.NewAggregate(errs))
	}
	return &NodeSelector{lazy: *lazy}, nil
}

// NewLazyErrorNodeSelector 创建仅在无任何 term 匹配时才报告解析错误的 NodeSelector。
//
// 不返回 error，因为解析失败也被编码进 nodeSelectorTerm，留给 Match 阶段决定
// 是否报告；field.Path 从调用方传入，保证错误信息带有完整的字段路径。
// ns 必须非 nil，空 NodeSelector 则构造出不匹配任何节点的选择器。
func NewLazyErrorNodeSelector(ns *v1.NodeSelector, opts ...field.PathOption) *LazyErrorNodeSelector {
	p := field.ToPath(opts...)
	parsedTerms := make([]nodeSelectorTerm, 0, len(ns.NodeSelectorTerms))
	path := p.Child("nodeSelectorTerms")
	for i, term := range ns.NodeSelectorTerms {
		// nil 或空 term 不匹配任何对象：
		// 若保留它并按“匹配全部”处理，会意外放宽选择器。
		if isEmptyNodeSelectorTerm(&term) {
			continue
		}
		// 带上数组下标，使解析错误能精确定位到 nodeSelectorTerms[i]。
		p := path.Index(i)
		parsedTerms = append(parsedTerms, newNodeSelectorTerm(&term, p))
	}
	return &LazyErrorNodeSelector{
		terms: parsedTerms,
	}
}

// Match 判断节点的标签与字段是否命中选择器；各 term 之间是“或”关系，
// nil 节点或空 term 不匹配任何对象。
//
// 丢弃 error 是安全的：构造器已经保证 NodeSelector 中没有解析错误。
// node 为 nil 时返回 false。
func (ns *NodeSelector) Match(node *v1.Node) bool {
	// 解析错误已在 NewNodeSelector 中报告，此处无需处理。
	match, _ := ns.lazy.Match(node)
	return match
}

// Match 判断节点的标签与字段是否命中选择器；各 term 之间是“或”关系，
// nil 节点或空 term 不匹配任何对象。仅当没有任何 term 匹配时才返回解析错误。
//
// 延迟报错的语义：只要有一个 term 成功匹配就返回 true，错误被忽略；
// 只有全部 term 都未匹配时，才把累积的解析错误返回给调用方。
func (ns *LazyErrorNodeSelector) Match(node *v1.Node) (bool, error) {
	// nil 节点不匹配任何选择器：调用方可能拿到已删除的节点对象。
	if node == nil {
		return false, nil
	}
	// 标签与字段是两个独立的匹配维度，分别转换成 Set 后交给各自的选择器求值。
	nodeLabels := labels.Set(node.Labels)
	nodeFields := extractNodeFields(node)

	var errs []error
	for _, term := range ns.terms {
		match, tErrs := term.match(nodeLabels, nodeFields)
		if len(tErrs) > 0 {
			// 解析失败的 term 无法判断匹配，记录错误后继续检查其他 term。
			errs = append(errs, tErrs...)
			continue
		}
		// term 之间是“或”关系：首个命中即可短路返回，无需检查其余 term。
		if match {
			return true, nil
		}
	}
	// 走到这里说明所有 term 都未命中（或解析失败），此时才把错误暴露给调用方。
	return false, errors.Flatten(errors.NewAggregate(errs))
}

// PreferredSchedulingTerms 是 []v1.PreferredSchedulingTerm 的运行时表示。
//
// 与必需选择器不同，软约束需要为每个 term 保留权重以便打分求和。
type PreferredSchedulingTerms struct {
	terms []preferredSchedulingTerm
}

// NewPreferredSchedulingTerms 构造 PreferredSchedulingTerms；若发现解析错误，则返回全部错误。
// 权重为 0 的 v1.PreferredSchedulingTerm 会被跳过，不参与解析。
//
// 权重为 0 的 term 不参与打分，因此也无需为其解析付出代价；
// 解析失败的 term 不会被静默丢弃，而是汇总成错误返回，避免悄悄改变调度偏好。
func NewPreferredSchedulingTerms(terms []v1.PreferredSchedulingTerm, opts ...field.PathOption) (*PreferredSchedulingTerms, error) {
	p := field.ToPath(opts...)
	var errs []error
	parsedTerms := make([]preferredSchedulingTerm, 0, len(terms))
	for i, term := range terms {
		// 带上数组下标，便于错误信息定位到具体的 preferred 项。
		path := p.Index(i)
		// 权重为 0 表示此项不产生影响；空 term 也不贡献约束，二者都直接跳过。
		if term.Weight == 0 || isEmptyNodeSelectorTerm(&term.Preference) {
			continue
		}
		parsedTerm := preferredSchedulingTerm{
			nodeSelectorTerm: newNodeSelectorTerm(&term.Preference, path),
			weight:           int(term.Weight),
		}
		// 解析失败的项不能进入候选集合，否则会给出不可信的分数。
		if len(parsedTerm.parseErrs) > 0 {
			errs = append(errs, parsedTerm.parseErrs...)
		} else {
			parsedTerms = append(parsedTerms, parsedTerm)
		}
	}
	// 与必需选择器一致：只要有错误就整体失败，交由调用方处理。
	if len(errs) != 0 {
		return nil, errors.Flatten(errors.NewAggregate(errs))
	}
	return &PreferredSchedulingTerms{terms: parsedTerms}, nil
}

// Score 计算节点得分：所有命中该节点的 term 的权重之和。
//
// 构造器已校验过所有 term，因此这里忽略 match 的错误返回值。
// node 必须非 nil；未命中任何 term 时得分为 0。
func (t *PreferredSchedulingTerms) Score(node *v1.Node) int64 {
	var score int64
	nodeLabels := labels.Set(node.Labels)
	nodeFields := extractNodeFields(node)
	for _, term := range t.terms {
		// 解析错误已在 NewPreferredSchedulingTerms 中报告，此处只关心是否命中。
		// 软约束是加分项：命中才累加权重，未命中不影响得分。
		if ok, _ := term.match(nodeLabels, nodeFields); ok {
			score += int64(term.weight)
		}
	}
	return score
}

// isEmptyNodeSelectorTerm 判断 term 是否既没有标签表达式也没有字段表达式。
// 空 term 既不按“匹配全部”也不按“永不匹配”处理，而是直接忽略：
// 语义上它不贡献任何约束，保留下来只会影响错误聚合结果。
func isEmptyNodeSelectorTerm(term *v1.NodeSelectorTerm) bool {
	return len(term.MatchExpressions) == 0 && len(term.MatchFields) == 0
}

// extractNodeFields 抽取节点可参与字段匹配的属性。
// 当前只有 metadata.name 可用，因此用一个只含该键的 fields.Set 表示；
// 后续新增字段时从这里扩展。节点名为空时不写入该键，避免产生空值匹配。
func extractNodeFields(n *v1.Node) fields.Set {
	f := make(fields.Set)
	if len(n.Name) > 0 {
		f["metadata.name"] = n.Name
	}
	return f
}

// 单个 term 的解析结果：标签与字段选择器分别持有，
// parseErrs 让延迟报错模式下仍能保留原始错误信息。
type nodeSelectorTerm struct {
	matchLabels labels.Selector
	matchFields fields.Selector
	parseErrs   []error
}

// 标签与字段分别解析，彼此独立：一边失败不影响另一边记录的解析结果。
func newNodeSelectorTerm(term *v1.NodeSelectorTerm, path *field.Path) nodeSelectorTerm {
	var parsedTerm nodeSelectorTerm
	var errs []error
	if len(term.MatchExpressions) != 0 {
		p := path.Child("matchExpressions")
		parsedTerm.matchLabels, errs = nodeSelectorRequirementsAsSelector(term.MatchExpressions, p)
		if errs != nil {
			parsedTerm.parseErrs = append(parsedTerm.parseErrs, errs...)
		}
	}
	if len(term.MatchFields) != 0 {
		p := path.Child("matchFields")
		parsedTerm.matchFields, errs = nodeSelectorRequirementsAsFieldSelector(term.MatchFields, p)
		if errs != nil {
			parsedTerm.parseErrs = append(parsedTerm.parseErrs, errs...)
		}
	}
	return parsedTerm
}

// match 判断单个 term 是否匹配节点。
// 解析错误优先于匹配结果返回：有错误的 term 无法给出可信结论。
func (t *nodeSelectorTerm) match(nodeLabels labels.Set, nodeFields fields.Set) (bool, []error) {
	if t.parseErrs != nil {
		return false, t.parseErrs
	}
	// 标签与字段选择器是“与”关系：任一不匹配即判为不匹配。
	if t.matchLabels != nil && !t.matchLabels.Matches(nodeLabels) {
		return false, nil
	}
	// 空字段集合时跳过字段匹配：extractNodeFields 为空意味着节点没有 name，
	// 此时继续匹配只会得到恒假结果，不如直接按“字段维度无约束”处理。
	if t.matchFields != nil && len(nodeFields) > 0 && !t.matchFields.Matches(nodeFields) {
		return false, nil
	}
	return true, nil
}

// validSelectorOperators 列出标签选择器支持的全部操作符，用于 field.NotSupported 错误信息。
// 顺序与文档保持一致，便于用户理解哪些取值合法。
var validSelectorOperators = []v1.NodeSelectorOperator{
	v1.NodeSelectorOpIn,
	v1.NodeSelectorOpNotIn,
	v1.NodeSelectorOpExists,
	v1.NodeSelectorOpDoesNotExist,
	v1.NodeSelectorOpGt,
	v1.NodeSelectorOpLt,
}

// nodeSelectorRequirementsAsSelector 把 []v1.NodeSelectorRequirement 转换为实现 labels.Selector 的选择器。
//
// 逐个 requirement 转换并用 selector.Add 叠加：API 中多个表达式是“与”关系，
// labels.Selector 的 Add 正好表达这种合取。
// 返回的错误列表带字段路径，调用方可以定位到具体哪一条表达式非法。
func nodeSelectorRequirementsAsSelector(nsm []v1.NodeSelectorRequirement, path *field.Path) (labels.Selector, []error) {
	// 空列表返回 labels.Nothing()：它匹配任何标签集合都不成立，
	// 表示“没有约束”时不能用 labels.Everything()，否则会意外放宽条件。
	if len(nsm) == 0 {
		return labels.Nothing(), nil
	}
	var errs []error
	selector := labels.NewSelector()
	for i, expr := range nsm {
		// 记录字段路径，让解析错误能定位到具体的 matchExpressions 下标。
		p := path.Index(i)
		var op selection.Operator
		// 把 API 操作符映射到 apimachinery 标签选择器的操作符；
		// 标签选择器支持集合、存在性与整数大小比较六种操作。
		switch expr.Operator {
		case v1.NodeSelectorOpIn: // 键值必须出现在 values 集合中
			op = selection.In
		case v1.NodeSelectorOpNotIn: // 键值不得出现在 values 集合中
			op = selection.NotIn
		case v1.NodeSelectorOpExists: // 只要求键存在，values 必须为空
			op = selection.Exists
		case v1.NodeSelectorOpDoesNotExist: // 要求键不存在，values 必须为空
			op = selection.DoesNotExist
		case v1.NodeSelectorOpGt: // 键值（按整数）大于给定值
			op = selection.GreaterThan
		case v1.NodeSelectorOpLt: // 键值（按整数）小于给定值
			op = selection.LessThan
		default:
			// 未知操作符：记录 NotSupported 错误并跳过该条，继续检查其余表达式。
			errs = append(errs, field.NotSupported(p.Child("operator"), expr.Operator, validSelectorOperators))
			continue
		}
		// NewRequirement 会校验 key 的合法性与 values 是否符合操作符约束；
		// 合法的 requirement 才叠加进选择器。
		r, err := labels.NewRequirement(expr.Key, op, expr.Values, field.WithPath(p))
		if err != nil {
			errs = append(errs, err)
		} else {
			selector = selector.Add(*r)
		}
	}
	// 只要有一条表达式非法就返回 nil 选择器，避免调用方误用部分构造的结果。
	if len(errs) != 0 {
		return nil, errs
	}
	return selector, nil
}

// validFieldSelectorOperators 列出字段选择器支持的全部操作符。
// 只支持相等/不等：底层 fields.Selector 不支持集合或大小比较。
var validFieldSelectorOperators = []v1.NodeSelectorOperator{
	v1.NodeSelectorOpIn,
	v1.NodeSelectorOpNotIn,
}

// nodeSelectorRequirementsAsFieldSelector 把 []v1.NodeSelectorRequirement 转换为实现 fields.Selector 的选择器。
//
// 与标签选择器不同，这里要求 In/NotIn 恰好一个值，因为 fields 选择器只表达
// 单值相等/不等；多个值无法用当前 fields.Selector 表达。
func nodeSelectorRequirementsAsFieldSelector(nsr []v1.NodeSelectorRequirement, path *field.Path) (fields.Selector, []error) {
	// 与标签选择器一致：空列表用 fields.Nothing()，表示不匹配任何字段集合。
	if len(nsr) == 0 {
		return fields.Nothing(), nil
	}
	var errs []error

	var selectors []fields.Selector
	for i, expr := range nsr {
		// 带上数组下标，使 values/operator 的错误能精确定位到 matchFields[i]。
		p := path.Index(i)
		switch expr.Operator {
		case v1.NodeSelectorOpIn: // 要求字段值等于给定值
			if len(expr.Values) != 1 {
				// 多值或零值都无法映射为单个相等比较，记为非法。
				errs = append(errs, field.Invalid(p.Child("values"), expr.Values, "must have one element"))
			} else {
				selectors = append(selectors, fields.OneTermEqualSelector(expr.Key, expr.Values[0]))
			}

		case v1.NodeSelectorOpNotIn: // 要求字段值不等于给定值
			if len(expr.Values) != 1 {
				errs = append(errs, field.Invalid(p.Child("values"), expr.Values, "must have one element"))
			} else {
				selectors = append(selectors, fields.OneTermNotEqualSelector(expr.Key, expr.Values[0]))
			}

		default:
			// Exists/Gt/Lt 等操作符对字段没有意义，直接记录不支持错误。
			errs = append(errs, field.NotSupported(p.Child("operator"), expr.Operator, validFieldSelectorOperators))
		}
	}

	// 有错误时返回 nil 选择器，防止调用方基于部分结果做出错误调度决策。
	if len(errs) != 0 {
		return nil, errs
	}
	// 多条字段要求是“与”关系，用 AndSelectors 串接成一个选择器。
	return fields.AndSelectors(selectors...), nil
}

// preferredSchedulingTerm 是单个软约束项：内嵌 nodeSelectorTerm 复用匹配逻辑，
// weight 单独保存用于打分。
type preferredSchedulingTerm struct {
	nodeSelectorTerm
	weight int
}

// RequiredNodeAffinity 把 pod spec 中的 nodeSelector（标签）与
// nodeAffinity（nodeSelectorTerm）合并表示。
// 两者是“与”关系：必须同时满足才算节点可用。
type RequiredNodeAffinity struct {
	labelSelector labels.Selector
	nodeSelector  *LazyErrorNodeSelector
}

// GetRequiredNodeAffinity 返回 Pod 的 nodeSelector 与 nodeAffinity 的解析结果。
//
// 对 Pod 对象的便捷封装；调用方若只有 spec 字段（如 workload controller），
// 应直接使用 NewRequiredNodeAffinity。pod 必须非 nil。
func GetRequiredNodeAffinity(pod *v1.Pod) RequiredNodeAffinity {
	return NewRequiredNodeAffinity(pod.Spec.NodeSelector, pod.Spec.Affinity)
}

// NewRequiredNodeAffinity 解析给定的 nodeSelector 与 affinity，返回对应的 RequiredNodeAffinity。
// 对于同时持有这些字段的 Pod，它等价于 GetRequiredNodeAffinity；适用于只有 pod spec
// 字段而没有 Pod 对象的调用方（例如 workload controller 求值 pod 模板）。
// 注意这里使用延迟报错的 nodeSelector：解析错误只在所有 term 都不匹配时才暴露。
func NewRequiredNodeAffinity(nodeSelector map[string]string, affinity *v1.Affinity) RequiredNodeAffinity {
	// 空 nodeSelector 不构造 selector，nil 即表示“无标签约束”。
	var selector labels.Selector
	if len(nodeSelector) > 0 {
		selector = labels.SelectorFromSet(nodeSelector)
	}
	// 保留旧的错误报告行为：nodeAffinity 的解析错误只在所有 term 都不匹配时才暴露。
	var lazyNodeSelector *LazyErrorNodeSelector
	// 三级 nil 检查逐层确认 affinity、NodeAffinity 与 required 段都存在，
	// 任一层缺失都表示没有硬性节点亲和性约束。
	if affinity != nil &&
		affinity.NodeAffinity != nil &&
		affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution != nil {
		lazyNodeSelector = NewLazyErrorNodeSelector(affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution)
	}
	return RequiredNodeAffinity{labelSelector: selector, nodeSelector: lazyNodeSelector}
}

// Match 依据 nodeSelector 与 nodeAffinity 两方面的硬性约束判断 Pod 能否调度到该节点。
//
// 先检查简单的标签选择器：不匹配时立即返回，避免对复杂的 nodeAffinity
// 做无意义的求值；只有标签匹配时才继续检查 nodeAffinity。
// node 必须非 nil，即使两种约束都不存在，本方法也不接受 nil 节点。
func (s RequiredNodeAffinity) Match(node *v1.Node) (bool, error) {
	if s.labelSelector != nil {
		if !s.labelSelector.Matches(labels.Set(node.Labels)) {
			return false, nil
		}
	}
	if s.nodeSelector != nil {
		return s.nodeSelector.Match(node)
	}
	// 两边都没有约束时，任何节点都可用。
	return true, nil
}
