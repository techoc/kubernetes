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
	"fmt"

	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// SchedulingPolicyOption 是控制器可以选择启用的调度策略枚举。
// 采用白名单而非黑名单：新增策略默认拒绝，必须由控制器显式加入列表后才生效，
// 避免新能力在控制器尚未验证前被悄悄启用。
type SchedulingPolicyOption int

const (
	// BasicPolicy 表示允许使用 Basic（基础）调度策略。
	BasicPolicy SchedulingPolicyOption = iota
	// GangPolicy 表示允许使用 Gang（成组）调度策略。
	GangPolicy
)

// DisruptionModeOption 是控制器可以选择启用的中断模式枚举，
// 采用与调度策略相同的白名单思路：控制器只声明自己支持的模式，未列入的模式一律拒绝。
type DisruptionModeOption int

const (
	// SingleMode 表示允许使用 Single（逐个）中断模式。
	SingleMode DisruptionModeOption = iota
	// AllMode 表示允许使用 All（整体）中断模式。
	AllMode
)

// BuildOptions 携带 BuildWorkload 生成的 Workload 对象的身份信息，以及控制器的调度白名单。
type BuildOptions struct {
	// Name 是编译出的 Workload 名称。
	Name string
	// Namespace 是编译出的 Workload 命名空间；物化出的 PodGroup 也使用该命名空间。
	Namespace string
	// Owner 指向控制器对象，靠 ownerRef 让 Workload 随父对象一起被回收。
	// 从 WorkloadItem 树编译时 Owner 必填；从已有 Workload 物化 PodGroup 时可为空，
	// 此时生成的 PodGroup 不带 ownerReferences。
	Owner *metav1.OwnerReference

	// AllowedPolicies 是控制器声明允许的调度策略集合。
	// Validate 会拒绝集合之外的任何策略；空集合表示什么都不允许。
	AllowedPolicies []SchedulingPolicyOption
	// AllowedDisruptionModes 是控制器声明允许的中断模式集合。
	// Validate 会拒绝集合之外的任何模式；空集合表示什么都不允许。
	AllowedDisruptionModes []DisruptionModeOption

	// DisableDeclarativeValidation 表示跳过对输入 building block 运行声明式校验。
	// 像 Job 这样的 in-tree 控制器会将其置为 true，因为它们的 APIServer 校验已经跑过这些规则。
	DisableDeclarativeValidation bool
}

// Builder 把控制器的 WorkloadItem 树编译成调度侧对象，并缓存编译结果。
// 先用 NewBuilder（或 NewBuilderFromExistingWorkload）构造一次，然后按需调用
// Validate、BuildWorkload、NewPodGroup 等方法。其中若干字段采用惰性初始化，
// 目的是让同一个 Builder 能生成多个 PodGroup 而不重复编译。
type Builder struct {
	root *WorkloadItem
	opts BuildOptions

	// workload 缓存 BuildWorkload 的编译结果，使多个 PodGroup 可以复用同一份编译产物。
	workload *schedulingv1beta1.Workload

	// existingWorkload 是经 NewBuilderFromExistingWorkload 传入的已持久化 Workload。
	// 设置后：NewPodGroup 从它物化对象；BuildWorkload 被拒绝，防止调用方重新编译
	// 一个已持久化或由父控制器编写的 Workload；Validate 同样被拒绝，因为该对象
	// 已通过 apiserver 校验，且没有可供校验的 WorkloadItem 树。
	existingWorkload *schedulingv1beta1.Workload

	// podGroupTemplateIndex 与 compositePodGroupTemplateIndex 按名字缓存模板，
	// 在首次物化调用时惰性构建并复用，使反复调用 NewPodGroup/NewCompositePodGroup
	// 的查找为 O(1)，无需每次重新扫描 Workload 的模板树。
	podGroupTemplateIndex          map[string]*schedulingv1beta1.PodGroupTemplate
	compositePodGroupTemplateIndex map[string]*schedulingv1beta1.CompositePodGroupTemplate
}

// NewBuilder 为给定的 WorkloadItem 树和选项创建一个 Builder，
// 适用于从 WorkloadItem 树现场编译 Workload 的控制器。
// 这里只保存输入而不立即编译，把解析推迟到 BuildWorkload，使调用方可以先校验再构建。
func NewBuilder(root *WorkloadItem, opts BuildOptions) *Builder {
	return &Builder{root: root, opts: opts}
}

// NewBuilderFromExistingWorkload 返回一个从已持久化 Workload 物化 PodGroup 的 Builder，
// 而不是从 WorkloadItem 树编译新对象。当 Workload 由父控制器（或手写模板）拥有并编译时，
// 应使用此构造函数。此时 BuildWorkload 与 Validate 都会被拒绝，只有 NewPodGroup 有意义；
// PodGroup 的 controller ownerRef 取自 opts.Owner。
func NewBuilderFromExistingWorkload(workload *schedulingv1beta1.Workload, opts BuildOptions) *Builder {
	return &Builder{opts: opts, existingWorkload: workload}
}

// BuildWorkload 把 WorkloadItem 树编译成 Workload，设置其身份信息和 controllerRef，
// 并缓存结果。从既有 Workload 构造的 Builder 会拒绝编译，
// 防止覆盖持久化对象或父控制器编写的内容。
// 返回值是 Builder 持有的缓存对象；调用方若需修改，应先深拷贝，以免影响后续物化结果。
func (b *Builder) BuildWorkload() (*schedulingv1beta1.Workload, error) {
	if b.existingWorkload != nil {
		return nil, fmt.Errorf("BuildWorkload is not available on a Builder created from an existing Workload: it materializes from the supplied Workload")
	}
	if b.workload != nil {
		return b.workload, nil
	}
	wl, err := b.build()
	if err != nil {
		return nil, err
	}
	b.workload = wl
	return wl, nil
}

// NewPodGroup 从 Builder 的 Workload 中按名字取出 PodGroupTemplate，
// 物化出一个运行时 PodGroup。模板通过惰性构建的名字索引解析，并在多次调用间复用。
// 报错信息会带上可用模板名列表，方便定位是名字写错还是模板根本没编译进去。
func (b *Builder) NewPodGroup(podGroupName, templateName string) (*schedulingv1beta1.PodGroup, error) {
	workload, err := b.resolveWorkload()
	if err != nil {
		return nil, err
	}
	if b.podGroupTemplateIndex == nil {
		// 首次使用时才按名字建索引，让只生成单个 PodGroup 的调用方
		// 不必为建立索引付出额外代价。
		b.podGroupTemplateIndex = indexPodGroupTemplates(workload)
	}
	tmpl := b.podGroupTemplateIndex[templateName]
	if tmpl == nil {
		return nil, fmt.Errorf("podGroupTemplate %q not found in workload %q (have %v)", templateName, workload.Name, podGroupTemplateNames(workload))
	}
	return newPodGroup(workload, tmpl, podGroupName, b.resolveOwners()), nil
}

// NewCompositePodGroup 从 Builder 的 Workload 中按名字取出 CompositePodGroupTemplate，
// 物化出一个运行时 CompositePodGroup。模板通过惰性构建的名字索引解析并复用。
// 父级关联（spec.parentCompositePodGroupName）以及子级 (Composite)PodGroup 由调用方自行填充。
func (b *Builder) NewCompositePodGroup(compositePodGroupName, templateName string) (*schedulingv1alpha3.CompositePodGroup, error) {
	workload, err := b.resolveWorkload()
	if err != nil {
		return nil, err
	}
	if b.compositePodGroupTemplateIndex == nil {
		b.compositePodGroupTemplateIndex = indexCompositePodGroupTemplates(workload)
	}
	tmpl := b.compositePodGroupTemplateIndex[templateName]
	if tmpl == nil {
		return nil, fmt.Errorf("compositePodGroupTemplate %q not found in workload %q (have %v)", templateName, workload.Name, compositePodGroupTemplateNames(workload))
	}
	return newCompositePodGroup(workload, tmpl, compositePodGroupName, b.resolveOwners())
}

// resolveWorkload 统一处理“从既有对象生成”和“从树编译”两条路径，
// 让 NewPodGroup、NewCompositePodGroup 等物化方法无需关心 Builder 是怎么构造的。
// 若 Builder 来自既有对象则返回该对象，否则返回 BuildWorkload 的编译结果；
// 两者都不可用时报错。
func (b *Builder) resolveWorkload() (*schedulingv1beta1.Workload, error) {
	if b.existingWorkload != nil {
		return b.existingWorkload, nil
	}
	if b.workload == nil {
		return nil, fmt.Errorf("no Workload available: call BuildWorkload or use NewBuilderFromExistingWorkload first")
	}
	return b.workload, nil
}

// resolveOwners 返回要打到物化出的 (Composite)PodGroup 上的 ownerReferences；
// 未配置 controller owner 时返回 nil。
func (b *Builder) resolveOwners() []metav1.OwnerReference {
	if b.opts.Owner != nil {
		return []metav1.OwnerReference{*b.opts.Owner}
	}
	return nil
}

// build 编译 WorkloadItem 树并设置 Workload 的身份信息，采用快速失败策略：
// 遇到第一个错误就立即返回。
func (b *Builder) build() (*schedulingv1beta1.Workload, error) {
	spec, err := compileWorkloadItemTree(b.root)
	if err != nil {
		return nil, err
	}

	wl := &schedulingv1beta1.Workload{
		ObjectMeta: metav1.ObjectMeta{
			Name:      b.opts.Name,
			Namespace: b.opts.Namespace,
		},
		Spec: spec,
	}

	if b.opts.Owner == nil {
		// 没有 owner 的 Workload 不会随控制器对象一起删除，容易变成孤儿，
		// 所以在构建时硬性拒绝，而不是留下隐患到运行期。
		return nil, fmt.Errorf("a Workload must be owned by its controller for garbage collection")
	}

	// 从 owner 的 apiVersion 拆出 Group，供 Workload 的 ControllerRef 使用。
	// 解析失败直接报错，避免把无法识别的 owner 写进对象。
	gv, err := schema.ParseGroupVersion(b.opts.Owner.APIVersion)
	if err != nil {
		return nil, fmt.Errorf("invalid owner apiVersion %q: %w", b.opts.Owner.APIVersion, err)
	}

	wl.OwnerReferences = []metav1.OwnerReference{*b.opts.Owner}
	wl.Spec.ControllerRef = &schedulingv1beta1.TypedLocalObjectReference{
		APIGroup: gv.Group,
		Kind:     b.opts.Owner.Kind,
		Name:     b.opts.Owner.Name,
	}

	return wl, nil
}

// isComposite 判断 item 是否为复合节点（即“组的组”，编译成 CompositePodGroupTemplate），
// 而非叶子节点（编译成单个 PodGroupTemplate）。
// 以是否有子节点作为区分依据，是整棵树递归分叉的标准：
// 有子节点编译成 CompositePodGroupTemplate，否则编译成单个 PodGroupTemplate。
func isComposite(item *WorkloadItem) bool {
	return len(item.Children) > 0
}

// compileWorkloadItemTree 把根节点编译成 WorkloadSpec。
// 若根节点是复合节点，则生成一个 CompositePodGroupTemplate（其子节点递归编译）；
// 若是叶子节点，则生成一个 PodGroupTemplate。对象的身份信息由 build 单独填充。
func compileWorkloadItemTree(root *WorkloadItem) (schedulingv1beta1.WorkloadSpec, error) {
	var spec schedulingv1beta1.WorkloadSpec

	if err := validateWorkloadItemTree(root); err != nil {
		// 编译前先校验整棵树，避免带着重复名字或非法的输入组合编译出不可用的对象。
		return spec, err
	}

	if isComposite(root) {
		cpgTemplate, err := compileCompositePodGroupTemplate(root)
		if err != nil {
			return spec, err
		}
		spec.CompositePodGroupTemplates = append(spec.CompositePodGroupTemplates, cpgTemplate)
		return spec, nil
	}

	pgTemplate, err := compilePodGroupTemplate(root)
	if err != nil {
		return spec, err
	}
	spec.PodGroupTemplates = append(spec.PodGroupTemplates, pgTemplate)

	return spec, nil
}

// compilePodGroupTemplate 解析叶子节点配置、运行其回调，然后编译成单个 PodGroupTemplate。
func compilePodGroupTemplate(item *WorkloadItem) (schedulingv1beta1.PodGroupTemplate, error) {
	// resolveSchedulingConfig 内部也会运行 item 的回调。
	resolved := resolveSchedulingConfig(item)

	return buildLeafTemplate(item.Name, resolved)
}

// resolveSchedulingConfig 把 DefaultConfig 与映射后的 Input 合并，再对合并结果
// 运行 item 的回调以应用控制器专属的默认值。
// 它根据 item 是否为复合节点选择对应的输入映射器，因此默认值合并与回调机制
// 对叶子和复合两条路径是同一套。配置会先深拷贝，避免回调改动调用方的配置。
// 解析后的配置依次传给每个回调读写，所有回调执行完毕后返回。
func resolveSchedulingConfig(item *WorkloadItem) *SchedulingConfig {
	resolved := item.DefaultConfig.DeepCopy()
	if resolved == nil {
		// 默认配置可缺省，补一个空配置让后续字段赋值不用处处判空。
		resolved = &SchedulingConfig{}
	}

	userConfig := mapWorkloadInput(item.Input)
	if isComposite(item) {
		userConfig = mapCompositeGroupInput(item.Input)
	}

	if userConfig.Policy != nil {
		resolved.Policy = userConfig.Policy
	}
	if userConfig.Constraints != nil {
		resolved.Constraints = userConfig.Constraints
	}
	if userConfig.DisruptionMode != nil {
		resolved.DisruptionMode = userConfig.DisruptionMode
	}
	if len(userConfig.ResourceClaims) > 0 {
		resolved.ResourceClaims = userConfig.ResourceClaims
	}
	if userConfig.PriorityClassName != "" {
		resolved.PriorityClassName = userConfig.PriorityClassName
	}

	for _, cb := range item.Callbacks {
		if cb != nil {
			// 回调在用户配置合并之后运行，作为控制器做最后默认化的位置；
			// 跳过 nil 便于调用方按需拼接回调列表。
			cb(resolved)
		}
	}

	return resolved
}

// buildLeafTemplate 把解析后的配置转换成一个 PodGroupTemplate。
func buildLeafTemplate(name string, cfg *SchedulingConfig) (schedulingv1beta1.PodGroupTemplate, error) {
	tmpl := schedulingv1beta1.PodGroupTemplate{Name: name}

	if cfg == nil {
		// 空配置等价于“全部走默认值”，统一走一份代码路径。
		cfg = &SchedulingConfig{}
	}

	tmpl.PriorityClassName = cfg.PriorityClassName

	policy, err := compileSchedulingPolicy(cfg.Policy)
	if err != nil {
		return schedulingv1beta1.PodGroupTemplate{}, err
	}
	tmpl.SchedulingPolicy = policy

	if cfg.Constraints != nil && len(cfg.Constraints.Topology) > 0 {
		topology := make([]schedulingv1beta1.TopologyConstraint, len(cfg.Constraints.Topology))
		for i := range cfg.Constraints.Topology {
			topology[i] = schedulingv1beta1.TopologyConstraint{
				Key: cfg.Constraints.Topology[i].Key,
			}
		}
		tmpl.SchedulingConstraints = &schedulingv1beta1.PodGroupSchedulingConstraints{
			Topology: topology,
		}
	}

	if cfg.DisruptionMode != nil {
		tmpl.DisruptionMode = compileDisruptionMode(cfg.DisruptionMode)
	}

	if len(cfg.ResourceClaims) > 0 {
		tmpl.ResourceClaims = compileResourceClaims(cfg.ResourceClaims)
	}

	return tmpl, nil
}

// compileSchedulingPolicy 把 IR 的调度策略映射为 API 策略。nil 策略解析为 Basic；
// Gang 要求事先解析出 MinCount，才能填充 API 中非指针的 MinCount 字段。
// minCount>=1 的取值约束由 Validate 中的声明式校验负责；
// BuildWorkload 不隐式执行 Validate，这里只检查编译期不变量，即 MinCount 已解析为非 nil。
func compileSchedulingPolicy(policy *SchedulingPolicy) (schedulingv1beta1.PodGroupSchedulingPolicy, error) {
	basic := schedulingv1beta1.PodGroupSchedulingPolicy{
		Basic: &schedulingv1beta1.BasicSchedulingPolicy{},
	}

	if policy == nil {
		return basic, nil
	}

	// basic/gang 的 union 约束在编译后的 Workload 提交时由声明式校验负责，
	// 这里只检查编译期不变量：已解析的 gang 策略必须带有 minCount 值。
	switch {
	case policy.Gang != nil:
		if policy.Gang.MinCount == nil {
			return schedulingv1beta1.PodGroupSchedulingPolicy{}, fmt.Errorf("gang scheduling requires minCount to be set after resolution")
		}
		return schedulingv1beta1.PodGroupSchedulingPolicy{
			Gang: &schedulingv1beta1.GangSchedulingPolicy{MinCount: *policy.Gang.MinCount},
		}, nil
	default:
		// nil 策略、仅 Basic 或空策略都解析为 Basic。
		return basic, nil
	}
}

// compileCompositePodGroupTemplate 解析复合节点的配置、运行其回调，
// 并编译成 CompositePodGroupTemplate；同时对每个子节点递归处理
// （复合子节点变成嵌套的 CompositePodGroupTemplate，叶子子节点变成 PodGroupTemplate）。
func compileCompositePodGroupTemplate(item *WorkloadItem) (schedulingv1beta1.CompositePodGroupTemplate, error) {
	resolved := resolveSchedulingConfig(item)

	policy, err := compileCompositeSchedulingPolicy(resolved.Policy)
	if err != nil {
		return schedulingv1beta1.CompositePodGroupTemplate{}, err
	}

	tmpl := schedulingv1beta1.CompositePodGroupTemplate{
		Name:              item.Name,
		SchedulingPolicy:  policy,
		PriorityClassName: resolved.PriorityClassName,
	}

	if resolved.Constraints != nil && len(resolved.Constraints.Topology) > 0 {
		topology := make([]schedulingv1beta1.TopologyConstraint, len(resolved.Constraints.Topology))
		for i := range resolved.Constraints.Topology {
			topology[i] = schedulingv1beta1.TopologyConstraint{
				Key: resolved.Constraints.Topology[i].Key,
			}
		}
		tmpl.SchedulingConstraints = &schedulingv1beta1.CompositePodGroupSchedulingConstraints{
			Topology: topology,
		}
	}

	if resolved.DisruptionMode != nil {
		tmpl.DisruptionMode = compileCompositeDisruptionMode(resolved.DisruptionMode)
	}

	for _, child := range item.Children {
		if child == nil {
			return schedulingv1beta1.CompositePodGroupTemplate{}, fmt.Errorf("composite workload item %q has a nil child", item.Name)
		}
		if isComposite(child) {
			childTmpl, err := compileCompositePodGroupTemplate(child)
			if err != nil {
				return schedulingv1beta1.CompositePodGroupTemplate{}, err
			}
			tmpl.CompositePodGroupTemplates = append(tmpl.CompositePodGroupTemplates, childTmpl)
		} else {
			pgTmpl, err := compilePodGroupTemplate(child)
			if err != nil {
				return schedulingv1beta1.CompositePodGroupTemplate{}, err
			}
			tmpl.PodGroupTemplates = append(tmpl.PodGroupTemplates, pgTmpl)
		}
	}

	return tmpl, nil
}

// compileCompositeSchedulingPolicy 把 IR 调度策略映射为复合类型的 API 策略。
// nil 策略解析为 Basic；Gang 要求事先解析出 MinCount，才能填充必填的 MinGroupCount 字段
// （IR 复用 GangSchedulingPolicy.MinCount 来承载复合的 minGroupCount 值）。
// minGroupCount>=1 的取值约束由 Validate 中的声明式校验负责；
// BuildWorkload 不隐式执行 Validate，这里只检查编译期不变量，即 MinCount 已解析为非 nil。
func compileCompositeSchedulingPolicy(policy *SchedulingPolicy) (schedulingv1beta1.CompositePodGroupSchedulingPolicy, error) {
	basic := schedulingv1beta1.CompositePodGroupSchedulingPolicy{
		Basic: &schedulingv1beta1.CompositeBasicSchedulingPolicy{},
	}

	if policy == nil {
		return basic, nil
	}

	// basic/gang 的 union 约束在提交时由声明式校验负责，
	// 这里只检查编译期不变量：已解析的 gang 策略必须带有 minGroupCount 值。
	switch {
	case policy.Gang != nil:
		if policy.Gang.MinCount == nil {
			return schedulingv1beta1.CompositePodGroupSchedulingPolicy{},
				fmt.Errorf("composite gang scheduling requires minGroupCount to be set after resolution")
		}
		return schedulingv1beta1.CompositePodGroupSchedulingPolicy{
			Gang: &schedulingv1beta1.CompositeGangSchedulingPolicy{
				MinGroupCount: *policy.Gang.MinCount,
			},
		}, nil
	default:
		// nil 策略、仅 Basic 或空策略都解析为 Basic。
		return basic, nil
	}
}

// compileDisruptionMode 把 IR 的中断模式映射为叶子节点的 API 模式。
// 两个成员都未设置时返回 nil；同时设置时按 All 优先转换。
// “恰好一个成员非空”的 union 约束应由调用方通过 Validate 检查，本函数不隐式执行校验。
func compileDisruptionMode(dm *DisruptionMode) *schedulingv1beta1.DisruptionMode {
	switch {
	case dm.All != nil:
		return &schedulingv1beta1.DisruptionMode{All: &schedulingv1beta1.AllDisruptionMode{}}
	case dm.Single != nil:
		return &schedulingv1beta1.DisruptionMode{Single: &schedulingv1beta1.SingleDisruptionMode{}}
	default:
		return nil
	}
}

// compileResourceClaims 把内部 IR 的资源声明逐项映射到 API 类型；
// 只搬运字段，不做校验，取值合法性交给声明式校验处理。
func compileResourceClaims(claims []ResourceClaim) []schedulingv1beta1.PodGroupResourceClaim {
	result := make([]schedulingv1beta1.PodGroupResourceClaim, len(claims))
	for i := range claims {
		result[i] = schedulingv1beta1.PodGroupResourceClaim{
			Name:                      claims[i].Name,
			ResourceClaimName:         claims[i].ResourceClaimName,
			ResourceClaimTemplateName: claims[i].ResourceClaimTemplateName,
		}
	}
	return result
}

// compileCompositeDisruptionMode 把 IR 的中断模式映射为复合节点的 API 模式。
// 两个成员都未设置时返回 nil；同时设置时按 All 优先转换。
// “恰好一个成员非空”的 union 约束应由调用方通过 Validate 检查，本函数不隐式执行校验。
func compileCompositeDisruptionMode(dm *DisruptionMode) *schedulingv1beta1.CompositeDisruptionMode {
	switch {
	case dm.All != nil:
		return &schedulingv1beta1.CompositeDisruptionMode{
			All: &schedulingv1beta1.AllCompositeDisruptionMode{},
		}
	case dm.Single != nil:
		return &schedulingv1beta1.CompositeDisruptionMode{
			Single: &schedulingv1beta1.SingleCompositeDisruptionMode{},
		}
	default:
		return nil
	}
}
