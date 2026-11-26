/*
Copyright 2025 The Kubernetes Authors.

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

// 本文件定义调度框架插件接口。

package framework

import (
	"context"
	"errors"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/google/go-cmp/cmp"         //nolint:depguard
	"github.com/google/go-cmp/cmp/cmpopts" //nolint:depguard
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

// Code 是插件返回的状态码/类型。
//
// 把插件结果抽象成一组语义码而非 bool+error，是因为不同失败原因对应调度队列完全不同的处理策略：
// 有的要退避、有的要进 PostFilter 尝试抢占、有的只是暂时等待。
// 用 int 而非 string 是为了让状态码比较走整数运算，省去每次判等都做字符串匹配。
type Code int

// 这组码是插件与调度器运行时之间的协议：插件只能从中选，运行时只认这些值决定后续动作。
// 以下是 Status 使用的预定义状态码。
// 注意：新增状态码时必须同步更新下方的 `codes` 切片。
const (
	// Success 表示插件正常执行且认为 Pod 可调度。
	// 注意：nil Status 也视为 "Success"。
	Success Code = iota
	// Error 用于插件内部错误、异常输入等非预期失败。
	// 预期内的失败（如 Unschedulable）不应返回此码。
	// 由于是非预期失败，调度队列不会记录导致不可调度的插件。
	// 因此 Pod 很快会重回 activeQ/backoffQ。
	Error
	// Unschedulable 表示插件判定 Pod 当前不可调度。
	// 若由 PreFilter 或 Filter 返回，调度器可能尝试其它
	// PostFilter 插件（如抢占）来让 Pod 可调度。
	// 若其它 PostFilter 也无能为力，应返回 UnschedulableAndUnresolvable。
	// 附带的消息应说明不可调度的原因。
	//
	// 退避被视为对浪费调度周期的惩罚。
	// 上次因 Unschedulable 被拒的 Pod 重新入队时，
	// 需要经历退避。
	Unschedulable
	// UnschedulableAndUnresolvable 表示 Pod 不可调度，且
	// 其它 PostFilter（如抢占）也无法改变结果。
	// PostFilter 对该状态的处理见 PostFilter 接口注释。
	// 只要其它 PostFilter 有可能让 Pod 可调度，就应返回 Unschedulable。
	// （其它 PostFilter 插件执行之后）。
	// 附带的消息应说明不可调度的原因。
	//
	// 退避被视为对浪费调度周期的惩罚。
	// 上次因 UnschedulableAndUnresolvable 被拒的 Pod 重新入队时，
	// 需要经历退避。
	UnschedulableAndUnresolvable
	// Wait 表示 Permit 插件要求暂缓 Pod 调度。
	Wait
	// Skip 用于以下场景：
	// - Bind 插件选择不处理绑定。
	// - PreFilter 返回 Skip 时，配对的 Filter 插件/PreFilterExtensions() 也会被跳过。
	// - PreScore 返回 Skip，使配对的 Score 插件也被跳过。
	Skip
	// Pending 表示调度已成功完成，
	// 但插件希望在此暂停调度/绑定周期。
	//
	// 例如插件需把调度结果通知外部组件，
	// 并在绑定前等待其完成。
	// 与返回不可调度不同，
	// 此时调度器已确定 Pod 可以去哪里，
	// 但需等待外部组件基于该调度结果完成动作。
	//
	// 退避被视为对浪费调度周期的惩罚。
	// 返回 Pending 时不能算作浪费调度周期，
	// 因为调度结果已能推进 Pod 的调度，
	// 尽管那一轮调度本身算失败。
	// 因此这类 Pod 不该承受退避惩罚。
	// 上次因 Pending 被拒的 Pod 重新入队时，
	// 直接进入 activeQ，不经过退避。
	Pending
)

// 字符串表与枚举值用位置一一对应，避免再维护一张 map；代价是新增状态时必须在两处同步插入，
// 所以注释特意强调顺序不能乱。
// 该列表必须与上面的 iota 定义在顺序和数量上严格一致。
var codes = []string{"Success", "Error", "Unschedulable", "UnschedulableAndUnresolvable", "Wait", "Skip", "Pending"}

// 按索引直取字符串，是建立在上面「两处顺序必须一致」的约定之上的；
// 这样 String() 是 O(1)，无需分支或查表结构。
func (c Code) String() string {
	return codes[c]
}

// Status 表示插件执行结果，由状态码、原因列表、可选错误
// 以及失败插件名组成。
// 非 Success 时 reasons 应说明原因。
// Success 时其它字段都应为空。
// 注意：nil Status 也视为 Success。
//
// 把 code、理由、错误、插件名收进一个结构体，是为了让插件用一套统一方式表达结果；
// 字段私有则是为了强制通过方法构造，保证 code 与 reasons/err 的搭配不会自相矛盾。
type Status struct {
	code    Code
	reasons []string
	err     error
	// plugin 是可选字段，记录产生该状态的插件名。
	// 状态码为这三种时由框架自动设置。
	plugin string
}

func (s *Status) WithError(err error) *Status {
	s.err = err
	return s
}

// nil Status 视作 Success，是为了让插件在一切正常时直接返回 nil，
// 不必每次都构造一个 &Status{code: Success} 对象。
// Code 返回 Status 的状态码。
func (s *Status) Code() Code {
	if s == nil {
		return Success
	}
	return s.code
}

// 把多条 reason 用逗号拼成一句，是给日志和事件消费的；
// 程序若要逐条判断，应走 Reasons() 而不是解析这个字符串。
// Message 把所有 reason 拼成一条消息。
func (s *Status) Message() string {
	if s == nil {
		return ""
	}
	return strings.Join(s.Reasons(), ", ")
}

// SetPlugin 把给定插件名写入 s.plugin。
func (s *Status) SetPlugin(plugin string) {
	s.plugin = plugin
}

// WithPlugin 把给定插件名写入 s.plugin，
// 并返回该 Status 对象。
func (s *Status) WithPlugin(plugin string) *Status {
	s.SetPlugin(plugin)
	return s
}

// Plugin 返回产生该状态的插件名。
func (s *Status) Plugin() string {
	return s.plugin
}

// 把 err 的文本放在 reasons 最前面，是为了让错误原因在展示时最醒目，
// 同时避免调用方为了拿错误文本再单独读 err 字段。
// Reasons 返回 Status 的原因列表。
func (s *Status) Reasons() []string {
	if s.err != nil {
		return append([]string{s.err.Error()}, s.reasons...)
	}
	return s.reasons
}

// AppendReason 向 Status 追加一条原因。
func (s *Status) AppendReason(reason string) {
	s.reasons = append(s.reasons, reason)
}

// IsSuccess 仅当 "Status" 为 nil 或 Code 为 "Success" 时返回 true。
func (s *Status) IsSuccess() bool {
	return s.Code() == Success
}

// IsWait 仅当 "Status" 非 nil 且 Code 为 "Wait" 时返回 true。
func (s *Status) IsWait() bool {
	return s.Code() == Wait
}

// IsSkip 仅当 "Status" 非 nil 且 Code 为 "Skip" 时返回 true。
func (s *Status) IsSkip() bool {
	return s.Code() == Skip
}

// 把 Pending 也归为 rejected，是因为调度周期确实没有走到绑定；
// 但它与真正失败的区别由退避策略单独处理，而不是在这个判定里区分。
// IsRejected 在 "Status" 为不可调度（Unschedulable、UnschedulableAndUnresolvable 或 Pending）时返回 true。
func (s *Status) IsRejected() bool {
	code := s.Code()
	return code == Unschedulable || code == UnschedulableAndUnresolvable || code == Pending
}

// IsError 仅当 "Status" 非 nil 且 Code 为 "Error" 时返回 true。
func (s *Status) IsError() bool {
	return s.Code() == Error
}

// Wait 和 Skip 不算错误，是因为它们都是正常控制流：前者等外部放行，后者本插件不处理。
// 优先返回内嵌 err 而非重新构造，是为了保留原始错误链供 errors.Is/As 使用。
// AsError 在状态为 success、wait 或 skip 时返回 nil；否则返回一个 "error" 对象，
// 由 Status 的 reasons 拼接成消息。
func (s *Status) AsError() error {
	if s.IsSuccess() || s.IsWait() || s.IsSkip() {
		return nil
	}
	if s.err != nil {
		return s.err
	}
	return errors.New(s.Message())
}

// 比较 err 时用 EquateErrors，是因为测试里两个错误只要语义等价即可，
// 不必是同一个实例；plugin 字段也参与比较，因为同一个失败由谁报告是行为的一部分。
// Equal 判断两个 Status 是否相等，便于配合 cmp 等做测试。
// 以便用 cmp.Equal 比较。
func (s *Status) Equal(x *Status) bool {
	if s == nil || x == nil {
		return s.IsSuccess() && x.IsSuccess()
	}
	if s.code != x.code {
		return false
	}
	if !cmp.Equal(s.err, x.err, cmpopts.EquateErrors()) {
		return false
	}
	if !cmp.Equal(s.reasons, x.reasons) {
		return false
	}
	return cmp.Equal(s.plugin, x.plugin)
}

func (s *Status) String() string {
	return s.Message()
}

// 深拷贝 reasons 切片而共享 err，是因为 error 对象通常是不可变的，
// 而切片会被 AppendReason 继续追加，不拷贝就会在副本间串改。
// Clone 深拷贝整个 Status 并返回副本。
func (s *Status) Clone() *Status {
	return &Status{
		code:    s.code,
		reasons: slices.Clone(s.reasons),
		err:     s.err,
		plugin:  s.plugin,
	}
}

// 构造函数只填 code 和 reasons，正是前面「成功时其他字段应为空」这条约定的体现。
// NewStatus 按给定参数构造 Status 并返回指针。
func NewStatus(code Code, reasons ...string) *Status {
	s := &Status{
		code:    code,
		reasons: reasons,
	}
	return s
}

// nil error 返回 nil status，是和「nil 即成功」的惯例保持一致；
// 非 nil 一律归为 Error 而非 Unschedulable，因为到这里还不知道失败是否与调度可行性有关。
// AsStatus 把错误包装成 Status。
func AsStatus(err error) *Status {
	if err == nil {
		return nil
	}
	return &Status{
		code: Error,
		err:  err,
	}
}

// 只暴露只读视图，是为了让抢占插件能看到每个节点在 Filter 阶段为何失败，
// 又不能改这份记录——它是所有 PostFilter 插件共享的判断依据。
// NodeToStatusReader 是传给 PostFilter 插件的只读视图。
type NodeToStatusReader interface {
	// Get 返回指定节点的状态。
	// 节点不在表中时返回 AbsentNodesStatus。
	Get(nodeName string) *Status
	// NodesForStatusCode 返回具有指定状态码的节点信息。
	// AbsentNodesStatus 所涵盖的节点也会一并返回。
	NodesForStatusCode(nodeLister NodeInfoLister, code Code) ([]NodeInfo, error)
}

// NodeScoreList 声明节点及其分数。
type NodeScoreList []NodeScore

// NodeScore 是节点名与分数的组合。
type NodeScore struct {
	Name  string
	Score int64
}

// NodePluginScores 是节点名及该节点上各插件分数的组合。
//
// 把原始分和归一化后的分都保留下来，是为了让调试和扩展插件能看到分数是怎么来的，
// 而不只看到一个已经被权重和归一化加工过的终值。
type NodePluginScores struct {
	// Name 是节点名。
	Name string
	// RawScores 保存归一化之前各活跃打分插件的 Score() 输出。
	RawScores []PluginScore
	// Scores 是插件与扩展打分经归一化、加权后的结果。
	Scores []PluginScore
	// TotalScore 是 Scores 中的总分。
	TotalScore int64
	// Randomizer 用于在同分节点间引入随机性
	// 用于同分节点随机化时的稳定排序。
	Randomizer int
}

// PluginScore 是插件/扩展名与分数的组合。
type PluginScore struct {
	// Name 是插件或扩展的名字。
	Name  string
	Score int64
}

// PlacementPluginScores 保存某个 Placement 的打分。
type PlacementPluginScores struct {
	// Placement 用于唯一标识一个 Placement。
	Placement *Placement
	// Scores 是插件与扩展的打分。
	Scores []PluginScore
	// TotalScore 是 Scores 中的总分。
	TotalScore int64
	// Randomizer 用于在同分对象间引入随机性
	// 用于同分 Placement 随机化时的稳定排序。
	Randomizer int
}

const (
	// MaxNodeScore 是 Score 插件应返回的最大分数。
	//
	// Deprecated: use MaxScore instead.
	MaxNodeScore int64 = MaxScore

	// MinNodeScore 是 Score 插件应返回的最小分数。
	//
	// Deprecated: use MinScore instead.
	MinNodeScore int64 = MinScore

	// MaxScore 是 Score/PlacementScore 插件的最大分数。
	MaxScore int64 = 100

	// MinScore 是 Score/PlacementScore 插件的最小分数。
	MinScore int64 = 0

	// MaxTotalScore 是总分上限。
	MaxTotalScore int64 = math.MaxInt64
)

// 区分「不改」和「覆盖」两种模式，是因为插件有时只需要保留已有提名，
// 有时又必须明确清掉或改写它；单一字符串字段无法表达「这次不动」的意图。
type NominatingMode int

const (
	ModeNoop NominatingMode = iota
	ModeOverride
)

// 提名信息被独立成结构体，是因为它同时被抢占结果、Pod 状态更新和缓存操作复用；
// 拆散成多个参数会在这些调用点之间反复传递、也容易出现只传了一半的情况。
type NominatingInfo struct {
	NominatedNodeName string
	NominatingMode    NominatingMode
}

// 对 nil 接收者返回 ModeNoop，是为了让调用方无需先判空就能安全读取模式；
// 「没有提名信息」在语义上就等价于「什么都不做」。
func (ni *NominatingInfo) Mode() NominatingMode {
	if ni == nil {
		return ModeNoop
	}
	return ni.NominatingMode
}

// WaitingPod 表示正处于 Permit 阶段等待中的 Pod。
type WaitingPod interface {
	// GetPod 返回等待中的 Pod。
	GetPod() *v1.Pod
	// GetPendingPlugins 返回尚未放行的 Permit 插件名。
	GetPendingPlugins() []string
	// Allow 声明该等待中的 Pod 被名为 "pluginName" 的插件放行，
	// 若它是最后一个待放行的插件，则发送成功信号。
	// 从而解除对 Pod 的阻塞。
	Allow(pluginName string)
	// Reject 将等待中的 Pod 判为不可调度。
	Reject(pluginName, msg string) bool
	// Preempt 抢占等待中的 Pod；与 Reject 不同，它不标记为不可调度，
	// 使其可被重新调度。
	Preempt(pluginName, msg string) bool
}

// PodInPreBind 表示正处于 PreBind 阶段的 Pod。
type PodInPreBind interface {
	// 若 Pod 尚未标记为 prebound，CancelPod 取消其绑定周期 context。
	// （Pod 尚未标记为 prebound 时）。
	// 取消成功时返回 true。
	CancelPod(reason string) bool

	// MarkPrebound 标记 Pod 已进入 prebound，此后不能再取消绑定周期。
	// 通过 PodInPreBind
	// context 已被取消时返回 false。
	MarkPrebound() bool
}

// 让 PreFilter 提前缩小候选节点集，是为了把原本要在 Filter 阶段逐节点跑的昂贵检查
// 在 O(1) 时间内砍掉大半；nil 表示「我不裁剪」，让各插件可以独立决定是否参与。
// PreFilterResult 封装框架处理 PreFilter 阶段所需的信息。
type PreFilterResult struct {
	// 下游应继续考虑的节点集合；nil 表示
	// 所有节点都符合。
	NodeNames sets.Set[string]
}

// nil 接收者和 nil NodeNames 都视作「不限制」，是为了让未实现该能力的插件
// 直接返回 nil 即可，框架无需再做特殊判空。
func (p *PreFilterResult) AllNodes() bool {
	return p == nil || p.NodeNames == nil
}

// 多个 PreFilter 的结果取交集，是因为每个插件都在声明「只考虑这些节点」；
// 必须同时满足所有插件才算候选。两边都放手时返回 nil，避免无谓地构造一个空结果。
func (p *PreFilterResult) Merge(in *PreFilterResult) *PreFilterResult {
	if p.AllNodes() && in.AllNodes() {
		return nil
	}

	r := PreFilterResult{}
	if p.AllNodes() {
		r.NodeNames = in.NodeNames.Clone()
		return &r
	}
	if in.AllNodes() {
		r.NodeNames = p.NodeNames.Clone()
		return &r
	}

	r.NodeNames = p.NodeNames.Intersection(in.NodeNames)
	return &r
}

// 把提名信息嵌进结果，是为了让抢占插件把「谁让位给谁」的结论传回框架，
// 由框架统一写回 Pod 状态，而不是各插件各自改 API。
// PostFilterResult 封装框架处理 PostFilter 阶段所需的信息。
type PostFilterResult struct {
	*NominatingInfo
}

// 增加一个轻量预检阶段，是为了在真正执行昂贵的 PreBind 前先问一句「这个插件要不要处理」；
// 框架据此把连续声明可并行的插件分组并发执行，缩短绑定路径的串行等待。
// PreBindPreFlightResult 封装 PreBindPreFlight 阶段的结果。
type PreBindPreFlightResult struct {
	// AllowParallel 表示该插件的 PreBind 能否与
	// PreBind 阶段其它插件并行执行。
	// 调度器把连续返回 true 的插件分为一组并行执行。
	// 并并行执行它们。
	// 返回 false 的插件会切断并行组。
	// 并顺序执行。
	// 注意：被跳过的插件会被忽略，但若它返回
	// false，仍会切断相邻插件的并行组。
	AllowParallel bool
}

// Plugin 是所有调度框架插件的父接口。
type Plugin interface {
	Name() string
}

// 在入队前就拦截，是为了把「这个 Pod 现在根本不该调度」的判断提前到事件处理路径上，
// 免得它占用调度周期才被发现不合格。
// PreEnqueuePlugin 是 "PreEnqueue" 插件必须实现的接口。
// 这些插件在 Pod 进入 activeQ/backoffQ 之前调用。
// PreEnqueue 插件应轻量高效，
// 不应做访问外部端点等昂贵操作，否则会阻塞其它
// 用于影响事件处理器中 Pod 的入队。
type PreEnqueuePlugin interface {
	Plugin
	// PreEnqueue 在 Pod 入 activeQ/backoffQ 前调用。
	// 非成功状态会标记 Pod 被 gated 并移入 unschedulableQ。
	// 被 gate 的 Pod 只在 gating 插件注册的事件或通配事件到来时重新评估。
	PreEnqueue(ctx context.Context, p *v1.Pod) *Status
}

// LessFunc 是实体信息的排序函数。
type LessFunc func(entity1, entity2 QueuedEntityInfo) bool

// 只允许一个排序插件，是因为队列顺序必须是一个全序；多个插件各排各的无法合成确定的优先级。
// QueueSortPlugin 是 "QueueSort" 插件必须实现的接口。
// 这些插件用于对调度队列中的实体排序，只能有一个 QueueSort
// 同一时刻只能启用一个队列排序插件。
type QueueSortPlugin interface {
	Plugin
	// Less 用于调度队列实体的排序。
	Less(QueuedEntityInfo, QueuedEntityInfo) bool
}

// 让插件自己声明「哪些事件能让被它拒绝的 Pod 重新可调度」，是为了把无效重试挡在队列之外；
// 不实现也能工作，但框架只能退回保守的全量重排，效率差得多。
// EnqueueExtensions 是可选接口，帮助插件高效地把
// 不可调度 Pod 在内部队列中重新入队。
// 调度器中 PreEnqueue、PreFilter、Filter、Reserve、Permit 插件都可能判定 Pod 不可调度，
// 被这些插件拒绝的 Pod 依据本扩展点重新入队。
// 其它扩展点的失败被视为临时错误（如网络故障），
// 其它扩展点的失败被视为临时错误，此时不使用本扩展点，而是在退避后总是把 Pod 放回 activeQ。
// 这些临时错误无法由特定集群事件解决，
// 只能持续重试直到故障消失。
//
// 会让 Pod 不可调度的插件（PreEnqueue、PreFilter、Filter、Reserve、Permit）必须实现该接口，
// 否则会使用默认实现，重排被该插件拒绝的 Pod 时效率更低。
//
// 另外，EventsToRegister 返回空列表意味着该插件拒绝的 Pod 不会因任何事件而重新入队，
// 这在大多数情况下没有意义（很可能是误用），
// 因为这些 Pod 可能永远滞留在不可调度池中。
//
// 上述扩展点之外的插件即使实现该接口也会被忽略。
type EnqueueExtensions interface {
	Plugin
	// EventsToRegister 返回一系列可能让被该插件拒绝的 Pod
	// 重新可调度的事件。每个事件带一个回调，
	// 用于过滤事件、减少无用的调度重试。
	// 事件在内部调度队列实例化时注册，
	// 并用于动态构建事件处理器。
	// 返回错误时调度器启动失败。
	// 注意：返回的列表必须在启动时确定，
	// 调度器只在启动时求值一次。
	// 不要在运行时改变结果（例如随集群状态变化）。
	//
	// 合理实现该函数能让 Pod 的重调度更准确、更高效。
	//
	// 只在启动时求值一次，是为了让事件处理器集合固定下来；
	// 运行期随集群状态变化会让「谁监听了什么」变得不可追踪。
	EventsToRegister(context.Context) ([]ClusterEventWithHint, error)
}

// 提供增量 AddPod/RemovePod，是为了在抢占等场景里不必对每个候选节点重跑完整的 PreFilter；
// 但传入的 NodeInfo 可能是共享对象，所以实现必须只读它，否则会污染其他节点的可行性判断。
// PreFilterExtensions 由需要增量更新的插件实现，
// 用于对预计算结果提供回调式的增量修改。
//
// 注意：某些场景（如抢占）下传入的 NodeInfo 可能是共享、未快照的
// 实例（尤其是受跨节点受害者影响的远端节点）。实现必须
// 把 nodeInfo 当作只读，绝不能修改它（如调用
// RemovePod 或 AddPodInfo）。此外，Filter 期间直接读 NodeInfo
// （而非 CycleState）的插件会看到这些节点的旧状态。
type PreFilterExtensions interface {
	// AddPod 在调度 podToSchedule 时由框架调用，
	// 用于评估向节点加入 podToAdd 的影响。
	AddPod(ctx context.Context, state CycleState, podToSchedule *v1.Pod, podInfoToAdd PodInfo, nodeInfo NodeInfo) *Status
	// RemovePod 在调度 podToSchedule 时由框架调用，
	// 用于评估从节点移除 podToRemove 的影响。
	RemovePod(ctx context.Context, state CycleState, podToSchedule *v1.Pod, podInfoToRemove PodInfo, nodeInfo NodeInfo) *Status
}

// 作为调度周期的第一个扩展点，PreFilter 的失败会直接终止本轮；
// 把能在这里算好的状态提前算好，后面每个节点才不用重复同样的推导。
// PreFilterPlugin 是 "PreFilter" 插件必须实现的接口。
// 这些插件在调度周期开始时调用。实现 PreFilterPlugin 的插件
// 还应实现 SignPlugin 以启用批处理优化。
type PreFilterPlugin interface {
	Plugin
	// PreFilter 在调度周期开始时调用。所有 PreFilter
	// 插件必须返回成功，否则 Pod 会被拒绝。PreFilter 可选返回
	// PreFilterResult 来影响下游评估哪些节点；当能以 O(1) 时间
	// 确定待处理节点子集时很有用。
	// PreFilterResult 过滤掉的节点被框架视为 "UnschedulableAndUnresolvable"，
	// 即它们不再作为抢占候选。
	//
	// 返回 Skip 时，PreFilterResult 与 Status 中其它字段都被忽略，
	// 本调度周期中配对的 Filter 插件/PreFilterExtensions() 也会被跳过。
	PreFilter(ctx context.Context, state CycleState, p *v1.Pod, nodes []NodeInfo) (*PreFilterResult, *Status)
	// PreFilterExtensions 在插件实现了该扩展时返回相应接口，
	// 否则返回 nil。PreFilter 插件可借此增量修改预处理信息。
	// 框架保证这些扩展的 AddPod/RemovePod 只在 PreFilter 之后调用，
	// 可能作用于克隆的 CycleState，且在同一节点再次执行 Filter 之前
	// 可多次调用这些函数。
	//
	// 用单独的 getter 而非把方法塞进主接口，是为了让不需要增量的插件保持简单；
	// 框架只在提供扩展时才在 NodeInfo 变化后回调它们。
	PreFilterExtensions() PreFilterExtensions
}

// Filter 是每个候选节点都要过的一道关；把「硬约束」放这里、把「偏好」留给 Score，
// 是为了让不可行的节点尽早出局，不浪费后面的打分开销。
// FilterPlugin 是 Filter 插件的接口。这些插件在
// filter 扩展点调用，用于过滤掉无法运行 Pod 的节点。
// 原调度器中这一概念称为 'predicate'。
// 这些插件应在 Status.code 中返回 "Success"、"Unschedulable" 或 "Error"。
// 不过调度器也接受其它合法状态码。
// 非 "Success" 的返回会让该节点被排除在
// 运行 Pod 的候选之外。实现 FilterPlugin 的插件
// 还应实现 SignPlugin 以启用批处理优化。
type FilterPlugin interface {
	Plugin
	// Filter 由调度框架调用。
	// 所有 FilterPlugin 都应返回 "Success" 表示
	// 节点适合该 Pod；不返回 "Success" 时，
	// 应返回 "Unschedulable"、"UnschedulableAndUnresolvable" 或 "Error"。
	//
	// "Error" 会中止 Pod 调度并将其放入退避队列。
	//
	// 对正在评估的节点，Filter 插件应查看传入的
	// nodeInfo 引用中该节点的信息（例如视为运行在该节点上的 Pod），
	// 而不是去 NodeInfoSnapshot 中查找，因为两者不保证一致。
	// 例如抢占时可能传入原始 nodeInfo 的副本，
	// 其中移除了部分 Pod，以评估抢占它们来调度目标 Pod
	// 的可能性。
	//
	// 建议插件检查 context 是否已取消。
	// 一旦取消，应尽快返回
	// UnschedulableAndUnresolvable 状态，并附带
	// `context.Cause(ctx)` 的错误说明。例如当已找到足够多合适节点、
	// 无需继续搜索时，context 就会被取消。
	//
	// 要求插件读传入的 nodeInfo 而非全局快照，是因为抢占场景下这个对象可能是
	// 「假想移走若干 Pod 后」的副本；从快照另查会得到与当前试探不一致的答案。
	Filter(ctx context.Context, state CycleState, pod *v1.Pod, nodeInfo NodeInfo) *Status
}

// PostFilter 是「常规筛选全失败后的补救」扩展点，抢占正是典型实现；
// 把它和 Filter 分开，是为了让补救逻辑只在真的需要时才运行。
// PostFilterPlugin 是 "PostFilter" 插件的接口，在
// Pod 无法调度之后调用。
type PostFilterPlugin interface {
	Plugin
	// PostFilter 在调度周期于 PreFilter 或 Filter 阶段
	// 因 Unschedulable/UnschedulableAndUnresolvable 失败时由框架调用。
	// NodeToStatusReader 保存各节点在 PreFilter/Filter 阶段得到的状态。
	//
	// 若用 PostFilter 实现自定义抢占，忽略 UnschedulableAndUnresolvable 节点是插件自己的责任；
	// NodeToStatusReader 中可能包含这类节点，
	// 即使所有节点都是该状态，框架也会调用 PostFilter 插件。
	//
	// PostFilter 插件应返回以下状态之一：
	// - Unschedulable：插件成功执行，但无法让 Pod 可调度。
	// - Success：插件成功执行且 Pod 已可调度。
	// - Error：插件因内部错误中止。
	//
	// 信息型插件应配置在其它插件之前，并始终返回 Unschedulable。
	// Success 时可选择性返回非 nil 的 PostFilterResult。例如
	// 抢占插件可返回 nominatedNodeName，供框架更新
	// 抢占者 Pod 的 .spec.status.nominatedNodeName 字段。
	//
	// 框架即使看到所有节点都 UnschedulableAndUnresolvable 也仍会调用 PostFilter，
	// 因此插件自己必须尊重这个状态码，否则会把补救花在根本不可能成功的节点上。
	PostFilter(ctx context.Context, state CycleState, pod *v1.Pod, filteredNodeStatusMap NodeToStatusReader) (*PostFilterResult, *Status)
}

// 把 pod group 的可行性检查做成可传入的函数，是为了让 PostFilter 插件
// 在各种补救方案里复用同一套判定，而不是各自实现一遍「这组 Pod 到底能不能一起放下」。
// PodGroupSchedulingFunc 用于检查 PodGroup 调度的可行性。
type PodGroupSchedulingFunc func(ctx context.Context) (*PodGroupAssignments, *Status)

// 按 Pod 记录提名而非整个组共用一个，是因为组里每个 Pod 最终被安排到哪个节点可能不同，
// 抢占后的状态更新必须逐个 Pod 精确落回。
// PodGroupPostFilterResult 保存 PodGroup 的提名节点信息。
type PodGroupPostFilterResult struct {
	// NominatingInfos 把 PodGroup 中的 Pod 映射到其提名节点信息，只包含已提名节点的 Pod。
	NominatingInfos map[types.NamespacedName]*NominatingInfo
}

// 组级补救比单 Pod 复杂：一个节点对单个 Pod 可行，不代表对整个组可行；
// 所以单独设一个扩展点，拿到的是组级状态和整组可行性函数。
// PodGroupPostFilterPlugin 是 "PodGroupPostFilter" 插件的接口，在
// PodGroup 无法调度后调用。
type PodGroupPostFilterPlugin interface {
	Plugin

	// PodGroupPostFilter 在 PodGroup 调度周期失败时由框架调用。
	// 当 PodGroup 调度周期失败时调用。
	//
	// PodGroupPostFilter 插件应返回以下状态之一：
	// - Unschedulable：插件执行成功，但 PodGroup 仍不可调度。
	// - UnschedulableAndUnresolvable：插件执行成功但 PodGroup 不可调度，
	// 且其它 PodGroupPostFilter 插件也无法让它可调度，后续插件不再评估。
	// - Success：插件执行成功且 PodGroup 可调度，后续插件不再评估。
	// - Error：插件因内部错误中止。
	PodGroupPostFilter(ctx context.Context, state PodGroupCycleState, pgInfo PodGroupInfo, pgSchedulingFunc PodGroupSchedulingFunc) (*PodGroupPostFilterResult, *Status)
}

// 作为纯信息扩展点，PreScore 让插件在打分前统一预处理（如算亲和性矩阵），
// 避免每个 Score 插件对同一批节点各算一遍。
// PreScorePlugin 是 "PreScore" 插件的接口。PreScore 是
// 信息型扩展点，插件收到通过过滤阶段的节点列表，
// 可据此更新内部状态或生成日志/指标。实现 PreScorePlugin 的插件
// 还应实现 SignPlugin 以启用批处理优化。
type PreScorePlugin interface {
	Plugin
	// PreScore 在一组节点通过过滤阶段后由调度框架调用。
	// 所有 PreScore 插件必须返回成功，
	// 否则 Pod 会被拒绝。
	// 返回 Skip 时，Status 中其它字段被忽略，
	// 本调度周期中配对的 Score 插件也会被跳过。
	PreScore(ctx context.Context, state CycleState, pod *v1.Pod, nodes []NodeInfo) *Status
}

// 归一化单独抽出来，是因为各插件的原始分制可能完全不同；
// 先各自归一化到统一区间，加权求和才有意义。
// ScoreExtensions 是 Score 扩展功能的接口。
type ScoreExtensions interface {
	// NormalizeScore 对同一插件 "Score" 产生的所有节点分数调用，
	// 成功后会更新分数列表并返回
	// 成功状态。
	NormalizeScore(ctx context.Context, state CycleState, p *v1.Pod, scores NodeScoreList) *Status
}

// Score 只处理已通过 Filter 的节点：再对不可行节点打分没有意义，
// 所以接口签名里拿到的是过滤后的节点集合。
// ScorePlugin 是 "Score" 插件必须实现的接口，用于给通过过滤的节点排序。
// 实现它的插件还应实现 SignPlugin 以启用批处理优化。
type ScorePlugin interface {
	Plugin
	// Score 对每个过滤后的节点调用，必须返回成功及表示节点排名的整数。
	// 所有打分插件必须返回成功，
	// 否则 Pod 会被拒绝。
	Score(ctx context.Context, state CycleState, p *v1.Pod, nodeInfo NodeInfo) (int64, *Status)

	// ScoreExtensions 在插件实现该接口时返回它，否则返回 nil。
	//
	// 与 PreFilterExtensions 同理，用可选 getter 让不需要归一化的插件不必实现空方法。
	ScoreExtensions() ScoreExtensions
}

// Reserve 是把「决定用这个节点」通知给插件状态的时机，位于打分之后、真正绑定之前；
// 放在这里而非绑定后，是为了让资源预留能尽早生效，并在后续失败时有明确的回滚点。
// ReservePlugin 是带 Reserve 和 Unreserve 方法的插件接口，
// 用于更新插件状态。原调度器中称为 'assume'。这些插件
// 在 Status.code 中只应返回 Success 或 Error，但调度器也接受
// 其它合法状态码。非 Success 会导致
// Pod 被拒绝。
type ReservePlugin interface {
	Plugin
	// Reserve 在调度器缓存更新时由框架调用。
	// 若返回失败 Status，调度器会对所有启用的 ReservePlugin
	// 调用 Unreserve。
	Reserve(ctx context.Context, state CycleState, p *v1.Pod, nodeName string) *Status
	// Unreserve 在预留的 Pod 被拒绝、后续插件预留出错或
	// 更晚阶段出错时由框架调用。实现必须幂等，
	// 即使同一插件的 Reserve 未被调用，
	// 调度器也可能调用它。
	//
	// 要求幂等且允许「没 Reserve 过也被调用」，是因为失败可能发生在任意后续阶段；
	// 调用方不应被迫追踪到底走到哪一步才敢回滚。
	Unreserve(ctx context.Context, state CycleState, p *v1.Pod, nodeName string)
}

// PreBind 执行真正有外部副作用的操作（如创建卷）；用 PreBindPreFlight 先筛一遍，
// 是为了别对「根本不归我管」的 Pod 白做这些重活。
// PreBindPlugin 是 "PreBind" 插件必须实现的接口。
// 这些插件在 Pod 被调度之后、绑定之前调用。
type PreBindPlugin interface {
	Plugin
	// PreBindPreFlight 在 PreBind 之前调用，插件应返回两个值：
	// - PreBindPreFlightResult（nil 合法，表示各字段为零值）。
	// - Success、Skip 或 Error 状态。
	// 返回 Success 表示该 PreBind 插件会处理该 Pod。
	// 返回 Skip 表示它与该 Pod 无关，PreBind 会被跳过。
	// 该函数应轻量，不应做创建卷等实际操作。
	PreBindPreFlight(ctx context.Context, state CycleState, p *v1.Pod, nodeName string) (*PreBindPreFlightResult, *Status)

	// PreBind 在绑定 Pod 前调用。所有 PreBind 插件必须返回
	// 成功，否则 Pod 会被拒绝且不会发送绑定。
	PreBind(ctx context.Context, state CycleState, p *v1.Pod, nodeName string) *Status
}

// 绑定成功后再通知插件清理状态；放在这里是因为只有此刻才能确认 Pod 真的落到了节点上。
// PostBindPlugin 是 "PostBind" 插件必须实现的接口。
// 这些插件在 Pod 成功绑定到节点后调用。
type PostBindPlugin interface {
	Plugin
	// PostBind 在 Pod 成功绑定后调用。这些插件是
	// 信息型的，常见用途是清理。若插件需要在 Pod 调度并绑定后
	// 清理状态，应注册到 PostBind 扩展点。
	PostBind(ctx context.Context, state CycleState, p *v1.Pod, nodeName string)
}

// Permit 提供绑定前的最后一道闸门，允许插件把 Pod 挂起等待外部条件；
// 它带着超时返回，是为了避免把绑定流程无限期卡住。
// PermitPlugin 是 "Permit" 插件必须实现的接口。
// 这些插件在 Pod 绑定到节点之前调用。
type PermitPlugin interface {
	Plugin
	// Permit 在绑定 Pod 之前（且在 PreBind 插件之前）调用，
	// 用于阻止或延迟 Pod 的绑定。Permit 插件
	// 必须返回成功或带超时的等待，否则 Pod 会被拒绝。
	// 若等待超时或等待期间 Pod 被拒绝，也会被拒绝。注意插件返回 "wait" 时，
	// 框架只会在其它插件都不拒绝该 Pod 的前提下，
	// 运行完剩余插件后才开始等待。
	Permit(ctx context.Context, state CycleState, p *v1.Pod, nodeName string) (*Status, time.Duration)
}

// 允许多个 Bind 插件按序尝试、谁接手就终止后续，是为了支持不同绑定策略共存；
// 不接手的插件用 Skip 表态，框架才能继续往下问。
// BindPlugin 是 "Bind" 插件必须实现的接口。Bind
// 插件用于把 Pod 绑定到节点。
type BindPlugin interface {
	Plugin
	// 所有 PreBind 插件完成后才会调用 Bind 插件。每个
	// Bind 插件按配置顺序调用，可自行决定是否
	// 处理给定 Pod。若某个 Bind 插件处理了 Pod，
	// 其余 Bind 插件被跳过。若不处理，
	// 必须在 Status 中返回 Skip；返回 Error 时
	// Pod 被拒绝且不会被绑定。
	Bind(ctx context.Context, state CycleState, p *v1.Pod, nodeName string) *Status
}

// 签名按「片段」而非一个整体来定义，是为了让多个插件各自贡献自己关心的维度，
// 框架再去重合并；同 key 只留一个，插件就能安全复用别人的片段而不怕冲突。
// Pod 签名的一部分。所有插件的签名片段会
// 合并成一个统一签名。
type SignFragment struct {
	// 签名片段以 key 标识：同一 key 在同一 Pod 上
	// 应包含相同值。多个插件可以返回相同的 SignFragment，
	// 框架会忽略重复项，便于共享签名器。
	//
	// 片段名见 k8s.io/kube-scheduler/framework/signers.go。新增 in-tree
	// 插件的片段名应加到那里，自定义插件也可使用。
	// 返回单个字段的简单 SignFragment，其名字应遵循所返回字段名。
	// 例如返回 NodeName 的片段，其 key 为：
	//
	// 若 SignFragment 对资源做了处理，名字应包含所用状态的根路径，
	// 再附上函数名加 ()。例如只返回
	// 临时卷的片段可用 key：
	Key string

	// SignFragment 的值必须是可 JSON 序列化的对象。记住要在 Pod 之间比较，
	// 因此插件应确保无序列表已排序等。
	Value any
}

// 用字节串而非结构体表示最终签名，是为了能直接当缓存键比较和哈希；
// 插件的 Value 必须是可 JSON 序列化的，正是为保证同一 Pod 每次得到同样的字节。
// 汇总所有插件结果后得到的 Pod 签名。
type PodSignature []byte

// 签名用于批处理优化：若两个 Pod 的签名相同，调度结果可以复用，省掉重复的 Filter/Score。
// 只要有一个参与筛选或打分的核心插件不实现它，批处理就只能整体关闭——否则会拿到不完整的签名，
// 误把结果不同的 Pod 当成相同。
// SignPlugin 由负责过滤或打分 Pod 的插件实现，
// 以启用批处理和 gang 调度优化。
// 每个插件返回 SignFragment，用于为每个进入调度周期的 Pod 构造单一签名，
// 调度器据此判断能否复用缓存的调度结果，还是需
// 重新计算优先节点。
//
// 启用的打分、预打分、过滤或预过滤插件若未实现该接口，所有 Pod 都会关闭批处理。
type SignPlugin interface {
	Plugin
	// SignPod 返回用于批处理的 SignFragment，每个调度周期都会调用，
	// 用于构造 Pod 的签名构建器（KEP-5598）。
	//
	// 所有插件的签名片段合并成一个签名，同一 key
	// 只保留一个片段。
	//
	// Status 含义：
	// - Success：签名器可为 Pod 签名，并返回一组要纳入的签名片段。
	// - Unschedulable：签名器拒绝为 Pod 签名，即它不适合机会式批处理优化。
	// - Error：签名器遇到 _非预期_ 情况无法生成签名。运行时会像
	// Unschedulable 一样跳过机会式批处理继续调度该 Pod，
	// 但会把错误记入日志。
	//
	// Error 与 Unschedulable 都只让这个 Pod 退出批处理，正常调度照旧；
	// 区别仅在于 Error 还会记日志。把「优化不可用」和「Pod 本身不可调度」分开，
	// 是为了避免签名插件的实现失误影响正常调度路径。
	SignPod(ctx context.Context, pod *v1.Pod) ([]SignFragment, *Status)
}

// 允许 placement 重叠，是因为同一批资源可能以不同方式划给 pod group；
// 交给后续打分再挑最优，比在生成阶段就武断去重更灵活。
// GeneratePlacementsResult 表示 PlacementGeneratePlugin 的结果。
type GeneratePlacementsResult struct {
	// Placements 是插件希望将资源划分成的 Placement 集合。
	// 各分区可以重叠。
	//
	// 没有合法分区时把数组设为 nil 或空。
	Placements []*Placement
}

// 把「候选资源集合从哪来」做成扩展点，是为了让不同拓扑/资源策略的插件都能参与，
// 框架只负责收集候选再交给打分阶段。
// PlacementGeneratePlugin 是生成候选 Placement 的插件接口。
type PlacementGeneratePlugin interface {
	Plugin

	// GeneratePlacements 为给定 PodGroup 在父 Placement 内生成候选 Placement 列表。
	// 每个 Placement 代表一组候选资源，例如匹配某选择器的节点。
	GeneratePlacements(ctx context.Context, state PodGroupCycleState, podGroup PodGroupInfo, parentPlacement *Placement) (*GeneratePlacementsResult, *Status)
}

// 分数与 placement 一起存，是因为归一化阶段需要拿到「这个分是哪个候选的」，
// 才能原地更新对应项而不打乱顺序。
// PlacementScore 保存 Placement 打分插件的结果，供后续归一化使用。
type PlacementScore struct {
	// Placement 是计算分数对应的 Placement。
	Placement *Placement
	// Score 是某 Placement 的分数，用于排序并选出最优 Placement。
	Score int64
}

// 与节点 Score 一样，placement 打分也需要先归一化再加权；
// 接口形状对齐是为了让两处逻辑可以共用同一套处理惯例。
// PlacementScoreExtensions 是 PlacementScore 扩展功能的接口。
type PlacementScoreExtensions interface {
	// NormalizePlacementScore 对同一插件 "ScorePlacement" 产生的所有 Placement 分数调用，
	// 成功后更新分数列表并返回
	// 成功状态。
	NormalizePlacementScore(ctx context.Context, state PodGroupCycleState, podGroup PodGroupInfo, placementScores []PlacementScore) *Status
}

// 只对「对组内足够多 Pod 都可行」的 placement 打分，是为了避免把分数浪费在
// 一眼就不满足 gang 约束的候选上；打分越高代表越适合整组落地。
// PlacementScorePlugin 是给可行 Placement 打分的插件接口。
type PlacementScorePlugin interface {
	Plugin

	// ScorePlacement 计算给定 Placement 的分数。
	// 该函数只对 PodGroup 调度周期中被判定对足够多 Pod 可行的 Placement 调用。
	// PodGroupAssignments 指示该 Placement 中各 Pod 分配到的节点。
	// state 作用域为正在打分的 Placement，可用来访问所属 PodGroup 的周期状态。
	// 返回值为 int64 分数，通常越大表示 Placement 越优。
	// 插件可实现多种打分策略，例如装箱以减少资源碎片。
	ScorePlacement(ctx context.Context, state PlacementCycleState, podGroup PodGroupInfo, placement *PodGroupAssignments) (int64, *Status)

	// PlacementScoreExtensions 在插件实现该接口时返回它，否则返回 nil。
	PlacementScoreExtensions() PlacementScoreExtensions
}

// Handle 在插件初始化时一次性注入，避免插件自己去抓全局单例；
// 这样框架可以精确控制每个插件能看到哪些能力，测试时也容易替换成假的。
// Handle 提供插件可用的数据和工具，在插件初始化时
// 传给插件工厂。插件
// 必须保存并使用它来调用框架功能。
type Handle interface {
	// PodNominator 抽象提名 Pod 的维护操作。
	PodNominator
	// PluginsRunner 抽象运行部分插件的操作。
	PluginsRunner
	// PodActivator 抽象调度队列中的操作。
	PodActivator
	// SnapshotSharedLister 从最新的 NodeInfo 快照返回 lister。快照
	// 在调度周期开始时生成，直到
	// Pod 走完 "Permit" 点之前保持不变。
	//
	// 只能在调度周期内使用：
	// - 绑定阶段不保证信息不变。
	// 因此插件不应在绑定周期（PreBind/Bind/PostBind/Unreserve 插件）使用它，
	// 否则可能发生并发读写错误。
	// - 不保证信息始终最新。
	// 因此不应在 QueueingHint 和 PreEnqueue 中使用，
	// 否则可能基于旧信息决策。
	//
	// 这些场景应改用 SharedInformerFactory() 创建的 Informer 提供的资源。
	//
	// 快照只在调度周期开始时生成并保持到 Permit 结束，是为了让一次调度内的所有插件
	// 看到同一个一致视图；正因如此，它才不能用在绑定阶段或队列提示里——那些地方需要的是最新或实时数据。
	SnapshotSharedLister() SharedLister

	// MutableSnapshotSharedLister 返回支持修改快照的 lister。
	// 它扩展 SharedLister 接口。
	// 仅 PodGroupPostFilter 扩展点可用。
	//
	// 可变更快照被显式限定在 PodGroupPostFilter：只有组级补救需要在同一周期内
	// 「假设性改动 + 回滚」地试探多个 placement，其他扩展点拿到它只会增加误用风险。
	MutableSnapshotSharedLister() MutableSnapshotSharedLister

	// IterateOverWaitingPods 获取读锁并遍历 WaitingPods map。
	//
	// 用回调而非返回切片，是为了让框架在持有读锁期间完成遍历，
	// 不必把内部 map 的引用暴露给插件造成并发隐患。
	IterateOverWaitingPods(callback func(WaitingPod))

	// GetWaitingPod 按 UID 返回等待中的 Pod。
	GetWaitingPod(uid types.UID) WaitingPod

	// RejectWaitingPod 按 UID 拒绝等待中的 Pod。
	// 返回值指示该 Pod 是否确实在等待。
	RejectWaitingPod(uid types.UID) bool

	// AddPodInPreBind 把一个 Pod 加入 preBind 列表。
	AddPodInPreBind(uid types.UID, cancel context.CancelCauseFunc)

	// GetPodInPreBind 按 UID 返回处于绑定周期但尚未绑定的 Pod。
	GetPodInPreBind(uid types.UID) PodInPreBind

	// RemovePodInPreBind 从 preBind 列表中移除 Pod。
	RemovePodInPreBind(uid types.UID)

	// ClientSet 返回 kubernetes clientSet。
	//
	// 暴露客户端给插件做必要的 API 操作，但放在 Handle 上是为了统一配置来源，
	// 免得每个插件各自建一个 client、连错集群或证书过期都不知道。
	ClientSet() clientset.Interface

	// KubeConfig 返回原始 kube config。
	KubeConfig() *restclient.Config

	// EventRecorder 返回事件记录器。
	//
	// 事件记录器由框架统一提供，保证事件格式和上报目标一致；
	// 插件不必也不应自己拼接事件投递逻辑。
	EventRecorder() events.EventRecorderLogger

	SharedInformerFactory() informers.SharedInformerFactory

	// SharedDRAManager 供 DRA 插件获取 DRA 对象并在内存中跟踪其修改。
	// 可注入非默认实现来模拟 DRA 对象状态。
	SharedDRAManager() SharedDRAManager

	// SharedCSIManager 用于获取 CSINode 对象并在内存中跟踪其变化。
	// 可注入非默认实现来模拟 CSINode 对象状态。
	SharedCSIManager() CSIManager

	// RunFilterPluginsWithNominatedPods 对给定节点上的提名 Pod 运行配置的 Filter 插件。
	RunFilterPluginsWithNominatedPods(ctx context.Context, state CycleState, pod *v1.Pod, info NodeInfo) *Status

	// Extenders 返回已注册的调度器扩展。
	Extenders() []Extender

	// Parallelizer 返回供调度器使用的并行度控制器。
	Parallelizer() Parallelizer

	// APIDispatcher 返回可直接派发 API 调用的 APIDispatcher。
	// 启用 SchedulerAsyncAPICalls 特性开关时该值非 nil。
	//
	// 只在异步 API 特性开启时非 nil，是为了让未开启该特性的部署完全不引入异步路径；
	// 插件用前须判空，编译期无法强制这一点。
	APIDispatcher() APIDispatcher

	// APICacher 返回与调度器内部缓存协调 API 调用的 APICacher。
	// 用它保证调度器对集群的视图一致。
	// 启用 SchedulerAsyncAPICalls 特性开关时该值非 nil。
	//
	// 与 APIDispatcher 配套：调度器要「先改缓存再异步落库」时，
	// 必须通过它来发请求，才能保证内部状态和 API 的最终结果一致。
	APICacher() APICacher

	// ProfileName 返回该 profile 关联的 profile 名。
	//
	// 插件从 Handle 就能知道自己属于哪个 profile，不必靠外部注入或硬编码配置名；
	// 同一个二进制里跑多 profile 时，这点尤其重要。
	ProfileName() string

	// PodGroupManager 提供从调度器缓存获取 PodGroup 运行时信息的接口。
	PodGroupManager() PodGroupManager

	// SignPod 为 Pod 创建 PodSignature。
	SignPod(ctx context.Context, pod *v1.Pod) PodSignature
}

// 把并行执行抽象成组件注入，是为了让插件在 CPU 充裕时并行处理节点，
// 而测试环境或低配部署可以替换成串行实现，行为无需分叉。
// Parallelizer 在可行的地方把调度操作分块并行执行，以提升性能与 CPU 利用率。
type Parallelizer interface {
	// Until 在适用时把 doWorkPiece 分块并行执行，最大块数由 pieces 指定。
	Until(ctx context.Context, pieces int, doWorkPiece workqueue.DoWorkPieceFunc, operation string)
}

// 抽象出激活操作，是为了让抢占等插件能把被抢占受害者重新推回队列，
// 而不必直接依赖队列的具体实现类型。
// PodActivator 抽象调度队列中的操作。
type PodActivator interface {
	// Activate 把给定 Pod 移入 activeQ。
	// 若 Pod 不在 unschedulableEntities 或 backoffQ 中但仍在飞行中，
	// 则注册通配事件，以便它回来时重新入队。
	// 若不在两者中且也不在飞行中（完全未知的 Pod），
	// Activate 会忽略它。
	//
	// 对「完全不认识」的 Pod 直接忽略，是因为队列里可能已经没有它的记录；
	// 但在途（in-flight）的 Pod 要登记通配事件，等它回来时再重新入队，避免漏调度。
	Activate(logger klog.Logger, pods map[string]*v1.Pod)
}

// 提名关系是抢占的核心数据：谁被选中、谁让位，都靠它维护；
// 做成接口是为了让 DRA 等场景能注入不同的存储实现。
// PodNominator 抽象提名 Pod 的维护操作。
type PodNominator interface {
	// AddNominatedPod 把给定 Pod 加入 nominator，
	// 已存在则更新。
	AddNominatedPod(logger klog.Logger, pod PodInfo, nominatingInfo *NominatingInfo)
	// DeleteNominatedPodIfExists 从内部缓存删除 nominatedPod，不存在时为空操作。
	DeleteNominatedPodIfExists(pod *v1.Pod)
	// UpdateNominatedPod 用 <newPod> 更新 <oldPod>。
	UpdateNominatedPod(logger klog.Logger, oldPod *v1.Pod, newPodInfo PodInfo)
	// NominatedPodsForNode 返回给定节点上的 nominatedPods。
	NominatedPodsForNode(nodeName string) []PodInfo
}

// 抢占 PostFilter 需要反复问「如果移走这些 Pod，目标 Pod 能不能落下」；
// PluginsRunner 把 Filter/Score/Reserve 等执行能力抽象出来，便于复用和替换实现。
// PluginsRunner 抽象运行部分插件的操作。
// 抢占 PostFilter 插件用它评估驱逐某些运行中的 Pod 后，
// 在该节点上调度目标 Pod 的可行性。
type PluginsRunner interface {
	// RunPreScorePlugins 运行配置的 PreScore 插件。若任一
	// 插件返回非 "Success" 状态，Pod 会被拒绝。
	RunPreScorePlugins(context.Context, CycleState, *v1.Pod, []NodeInfo) *Status
	// RunScorePlugins 运行配置的打分插件。
	// 返回各插件分数及每个节点总分的列表。
	// 还返回 *Status；任一插件返回非成功状态时，
	// 它会被置为非成功。
	RunScorePlugins(context.Context, CycleState, *v1.Pod, []NodeInfo) ([]NodePluginScores, *Status)
	// RunFilterPlugins 对给定节点上的 Pod 运行配置的 Filter 插件。注意传入的 nodeInfo
	// 引用可能与 NodeInfoSnapshot 中的不同（例如视为运行在节点上的 Pod
	// 可能不同）。例如抢占时可能传入原始 nodeInfo 的副本，
	// 其中移除了部分 Pod，以评估抢占它们来调度目标 Pod。
	RunFilterPlugins(context.Context, CycleState, *v1.Pod, NodeInfo) *Status
	// RunPreFilterExtensionAddPod 对配置的 PreFilter 插件调用 AddPod 接口。
	// 任一插件返回非 Success 状态时直接返回。
	RunPreFilterExtensionAddPod(ctx context.Context, state CycleState, podToSchedule *v1.Pod, podInfoToAdd PodInfo, nodeInfo NodeInfo) *Status
	// RunPreFilterExtensionRemovePod 对配置的 PreFilter 插件调用 RemovePod 接口。
	// 任一插件返回非 Success 状态时直接返回。
	RunPreFilterExtensionRemovePod(ctx context.Context, state CycleState, podToSchedule *v1.Pod, podInfoToRemove PodInfo, nodeInfo NodeInfo) *Status
	// RunReservePluginsReserve 运行配置的 Reserve 插件的 Reserve 方法。
	// 任一调用返回错误时不再执行其余插件，直接返回该错误。
	// 此时 Pod 不会被调度。
	RunReservePluginsReserve(ctx context.Context, state CycleState, pod *v1.Pod, nodeName string) *Status
	// RunReservePluginsUnreserve 运行配置的 Reserve 插件的 Unreserve 方法。
	RunReservePluginsUnreserve(ctx context.Context, state CycleState, pod *v1.Pod, nodeName string)
}
