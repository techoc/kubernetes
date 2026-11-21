/*
Copyright 2014 The Kubernetes Authors.

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

package kubelet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/record"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/allocation"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/events"
	"k8s.io/kubernetes/pkg/kubelet/eviction"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
	"k8s.io/kubernetes/pkg/kubelet/util/queue"
	"k8s.io/utils/clock"
)

// OnCompleteFunc 是操作完成时被调用的回调函数。
// 若 err 非 nil，表示操作未成功完成。
type OnCompleteFunc func(err error)

// PodStatusFunc 在 pod 被杀死时被调用，用于覆盖 pod 状态。
type PodStatusFunc func(podStatus *v1.PodStatus)

// KillPodOptions 是 update 类型为 kill 的 pod 更新所使用的选项。
type KillPodOptions struct {
	// 当 kill 请求完成（syncTerminatingPod 无错误完成）、pod 不存在或 pod 已终止时，
	// CompletedCh 会被关闭。关闭可能耗时任意长，但只要 CouldHaveRunningContainers()
	// 返回 false，它就绝不会保持打开状态。
	CompletedCh chan<- struct{}
	// 若本次 kill 由驱逐触发，则 Evict 为 true —— pod 被驱逐后，部分资源
	//（例如已停止的容器）会被比正常运行时更激进地回收。
	Evict bool
	// 若已设置，PodStatusFunc 会在 pod 被杀死时被调用并覆盖 pod 状态。
	// 传入的状态由最新状态填充。
	PodStatusFunc PodStatusFunc
	// PodTerminationGracePeriodSecondsOverride 是 kill 操作时可选的优雅终止时间覆盖值。
	PodTerminationGracePeriodSecondsOverride *int64
}

// MarshalJSON 只序列化 Evict 和优雅终止覆盖值：CompletedCh 与 PodStatusFunc
// 不可序列化，且它们只用于进程内的 kill 协调。
func (k *KillPodOptions) MarshalJSON() ([]byte, error) {
	t := struct {
		Evict                                    bool
		PodTerminationGracePeriodSecondsOverride *int64
	}{
		Evict:                                    k.Evict,
		PodTerminationGracePeriodSecondsOverride: k.PodTerminationGracePeriodSecondsOverride,
	}
	return json.Marshal(t)
}

// UpdatePodOptions 是传给 UpdatePod 操作的选项结构体。
type UpdatePodOptions struct {
	// 更新类型（create、update、sync、kill）。
	UpdateType kubetypes.SyncPodType
	// StartTime 是本次更新创建时刻的可选时间戳。若已设置，当 pod worker
	// 完全处理完该更新时，会将其记录到 PodWorkerDuration 指标中。
	StartTime time.Time
	// 要更新的 pod。必填。
	Pod *v1.Pod
	// 若 Pod 是静态 pod，MirrorPod 为其 mirror pod。当 UpdateType 为 kill
	// 或 terminated 时可选。
	MirrorPod *v1.Pod
	// RunningPod 是已不在配置中的运行时 pod。当 Pod 为 nil 时必填，
	// 当 Pod 已设置时忽略。
	RunningPod *kubecontainer.Pod
	// KillPodOptions 用于覆盖 pod 的默认终止行为，或在操作完成后更新 pod 状态。
	// 由于 pod 可能因多种原因被杀死，PodStatusFunc 会按顺序调用，后发生的 kill
	// 有机会覆盖状态（例如抢占之后可能又被驱逐）。
	KillPodOptions *KillPodOptions
}

// PodWorkerState 从 pod worker 的视角对 pod 状态分类 —— 建立（sync）、
// 容器拆除（terminating）或清理（terminated）。
type PodWorkerState int

const (
	// SyncPod 表示 pod 应当被启动并运行。
	SyncPod PodWorkerState = iota
	// TerminatingPod 表示 pod 不再进行建立，但可能仍有容器在运行且正在被拆除。
	TerminatingPod
	// TerminatedPod 表示 pod 已停止，不会再有运行中的容器，可以执行前台清理。
	TerminatedPod
)

// String 返回 PodWorkerState 的可读名称，便于日志和指标展示。
func (state PodWorkerState) String() string {
	switch state {
	case SyncPod:
		return "sync"
	case TerminatingPod:
		return "terminating"
	case TerminatedPod:
		return "terminated"
	default:
		panic(fmt.Sprintf("the state %d is not defined", state))
	}
}

// PodWorkerSync 是单个 pod worker 在同步时的状态摘要。除 State 外的字段
// 用于向运维人员提供指标计数。
type PodWorkerSync struct {
	// pod 的状态。
	State PodWorkerState
	// 若 pod 已不在传给 SyncKnownPods 的期望集合中，Orphan 为 true。
	Orphan bool
	// 若我们仍保有该 pod 的历史 spec，HasConfig 为 true。
	HasConfig bool
	// 若我们有配置且 pod 来自静态源，Static 为 true。
	Static bool
}

// podWork 表示一次内部待处理的变更。
type podWork struct {
	// WorkType 是要执行的同步类型 —— sync（创建）、terminating（停止容器）、
	// terminated（清理并写入状态）。
	WorkType PodWorkerState

	// Options 包含本次同步所需的数据。
	Options UpdatePodOptions
}

// PodWorkers 是抽象接口，便于测试替换。
type PodWorkers interface {
	// UpdatePod 通知 pod worker 某个 pod 发生了变化，随后由每个 pod UID 对应的
	// goroutine 按 FIFO 顺序处理。pod 状态会被传给 syncPod 方法，直到 pod 被标记
	// 删除、进入终态（Succeeded/Failed）或被 kubelet 驱逐。一旦发生上述情况，
	// syncTerminatingPod 会被反复调用直到成功退出；此后针对该 pod 的所有 UpdatePod()
	// 调用都会被忽略，直到因时间足够久而被遗忘。已终止的 pod 永远不会被重启。
	UpdatePod(ctx context.Context, options UpdatePodOptions)
	// SyncKnownPods 移除那些不在 desiredPods 集合中且已终止足够长时间的 worker。
	// 该方法被调用过一次后，worker 即被视为完成初始化，此后对未知 pod 调用
	// ShouldPodContentBeRemoved 将返回 true。它返回描述每个已知 pod worker 状态的
	// map；调用方负责重新添加那些未作为 knownPods 返回的期望 pod。
	SyncKnownPods(logger klog.Logger, desiredPods []*v1.Pod) (knownPods map[types.UID]PodWorkerSync)

	// 当 SyncTerminatingPod 成功完成后，IsPodKnownTerminated 返回 true —— 此时
	// pod worker 认为该 UID 对应的 pod 已终止。若 pod 被强制删除且 worker 已完成终止，
	// 该方法会返回 false，因此它只应用于从期望集合中过滤 pod（例如在准入阶段）。
	//
	// 供 kubelet 配置循环使用，而非子系统；子系统应使用 ShouldPod*()。
	IsPodKnownTerminated(uid types.UID) bool
	// 在 pod worker 尚未完成同步前，CouldHaveRunningContainers 返回 true；
	// 一旦 worker 看到该 pod（syncPod 可能被调用）仍返回 true；pod 终止后
	//（保证运行中容器已停止）返回 false。
	//
	// 供 kubelet 配置循环使用，而非子系统；子系统应使用 ShouldPod*()。
	CouldHaveRunningContainers(uid types.UID) bool

	// 当 SyncTerminatedPod 成功完成后，ShouldPodBeFinished 返回 true —— 此时
	// pod worker 认为该 pod 已终止且资源已回收。在 worker 完成同步前返回 false
	//（此时 syncPod 可能被调用）。同步完成后，若 pod 仍有同步状态，则直到
	// SyncTerminatedPod 成功前返回 false；若已完成同步但 pod 没有状态，则返回 true。
	//
	// 供子系统同步循环使用，以避免在 pod 已请求终止后仍做后台建立工作。
	// 调用方必须保证在其数据缺失时 syncPod 方法不会阻塞。
	ShouldPodBeFinished(uid types.UID) bool
	// 从 pod 终止被请求开始，直到终止完成且 pod 从配置中移除，IsPodTerminationRequested
	// 都返回 true。清理循环中不应使用它，因为 pod 已清理完毕时它会返回 false ——
	// 应改用 ShouldPodContainersBeTerminating。另外，在 pod worker 仍在初始化容器期间，
	// 该方法也可能返回 true。
	//
	// 供 kubelet 的 sync* 方法使用，而非子系统；子系统应使用 ShouldPod*()。
	IsPodTerminationRequested(uid types.UID) bool

	// 在 pod worker 完成同步前，或 pod 刚开始终止时，ShouldPodContainersBeTerminating
	// 返回 false。该检查与 ShouldPodRuntimeBeRemoved 类似，但在 pod 请求终止后也返回 true。
	//
	// 供子系统同步循环使用，以避免在 pod 已请求终止后仍做后台建立工作。
	// 调用方必须保证在其数据缺失时 syncPod 方法不会阻塞。
	ShouldPodContainersBeTerminating(uid types.UID) bool
	// 若 Kubelet 中的 runtime 管理器应激进清理 pod 的非容器、非磁盘内容资源
	//（例如已挂载的卷），ShouldPodRuntimeBeRemoved 返回 true。当首次同步后 worker
	// 尚未观察到该 pod（意味着它还不能运行），或所有运行中容器都已停止时，返回 true。
	// TODO: 一旦 pod 日志与运行中容器分离，此方法应改为用于控制是否保留容器。
	//
	// 供子系统同步循环使用，以判断何时开始拆除运行中容器所使用的资源。调用方应确保
	// 其拥有的 runtime 内容在终止后不再需要 —— 例如在 docker 中必须保留容器，
	// 以便在 pod 删除前保存 pod 日志。
	ShouldPodRuntimeBeRemoved(uid types.UID) bool
	// 若 Kubelet 中的资源管理器应激进清理与该 pod 相关的所有内容，
	// ShouldPodContentBeRemoved 返回 true。该值在 pod 驱逐期间（希望删除内容以释放资源）
	// 以及在删除请求已导致容器停止之后（更温和的路径）为 true。注意，正在删除的 pod
	// 仍可能被驱逐。
	//
	// 供子系统同步循环使用，以判断何时开始拆除未删除 pod 所使用的资源。内容通常保留到
	// 删除并移出 etcd 或驱逐为止，不过垃圾回收可能在方法返回 false 时释放内容。
	ShouldPodContentBeRemoved(uid types.UID) bool
	// 若给定名称的静态 pod 当前正在终止且尚未完成，IsPodForMirrorPodTerminatingByFullName
	// 返回 true。它仅用于孤儿 mirror pod 清理，避免在 pod 关闭之前就把它从 apiserver 删除。
	IsPodForMirrorPodTerminatingByFullName(podFullname string) bool
}

// podSyncer 描述 pod 状态机的核心生命周期操作。pod 首先被同步，直到自然终止
// （返回 true）或外部组件决定其应终止。一旦 pod 应当终止，SyncTerminatingPod 会被反复
// 调用直到不返回错误；随后 SyncTerminatedPod 被反复调用直到无错误退出，此时 pod 被视为终态。
// 该接口的实现必须线程安全，以支持对多个 pod 同时调用这些方法。
type podSyncer interface {
	// SyncPod 配置 pod 并启动/重启所有容器。若返回 true，表示 pod 已到达终态，
	// 同时是否返回错误表示成功还是失败。若返回错误，表示本次同步未成功，应在之后重试。
	// 这是一个长耗时方法，若 context 被取消，应尽快以 context.Canceled 退出。
	SyncPod(ctx context.Context, updateType kubetypes.SyncPodType, pod *v1.Pod, mirrorPod *v1.Pod, podStatus *kubecontainer.PodStatus) (bool, func(), error)
	// SyncTerminatingPod 尝试确保 pod 的容器不再运行，并收集最终状态。该方法会以逐步缩短的
	// 优雅期被反复调用，直到无错误退出。一旦无错误退出，其他组件才被允许拆除卷、设备等
	// 支撑资源。若 context 被取消，除非已成功完成，否则应返回 context.Canceled；
	// 检测到更短优雅期时也可能成功完成。
	SyncTerminatingPod(ctx context.Context, pod *v1.Pod, podStatus *kubecontainer.PodStatus, gracePeriod *int64, podStatusFn func(*v1.PodStatus)) error
	// 当发现运行中的容器对应一个 kubelet 已不再知晓的 pod 时，会调用
	// SyncTerminatingRuntimePod 来终止这些容器。除非确认所有容器都已停止，
	// 否则不应无错误退出。
	SyncTerminatingRuntimePod(ctx context.Context, runningPod *kubecontainer.Pod) error
	// SyncTerminatedPod 在所有运行中容器停止后被调用，负责立即（而非后台）释放资源。
	// 无错误退出后，pod 即被视为在节点上完成。
	SyncTerminatedPod(ctx context.Context, pod *v1.Pod, podStatus *kubecontainer.PodStatus) error
}

// syncPodFnType 是 podSyncer.SyncPod 的函数签名。
type syncPodFnType func(ctx context.Context, updateType kubetypes.SyncPodType, pod *v1.Pod, mirrorPod *v1.Pod, podStatus *kubecontainer.PodStatus) (bool, func(), error)

// syncTerminatingPodFnType 是 podSyncer.SyncTerminatingPod 的函数签名。
type syncTerminatingPodFnType func(ctx context.Context, pod *v1.Pod, podStatus *kubecontainer.PodStatus, gracePeriod *int64, podStatusFn func(*v1.PodStatus)) error

// syncTerminatingRuntimePodFnType 是 podSyncer.SyncTerminatingRuntimePod 的函数签名。
type syncTerminatingRuntimePodFnType func(ctx context.Context, runningPod *kubecontainer.Pod) error

// syncTerminatedPodFnType 是 podSyncer.SyncTerminatedPod 的函数签名。
type syncTerminatedPodFnType func(ctx context.Context, pod *v1.Pod, podStatus *kubecontainer.PodStatus) error

// podSyncerFuncs 实现 podSyncer，为每个方法接收一个函数。
type podSyncerFuncs struct {
	syncPod                   syncPodFnType
	syncTerminatingPod        syncTerminatingPodFnType
	syncTerminatingRuntimePod syncTerminatingRuntimePodFnType
	syncTerminatedPod         syncTerminatedPodFnType
}

// newPodSyncerFuncs 从已有 podSyncer 提取方法值，构造 podSyncerFuncs。
func newPodSyncerFuncs(s podSyncer) podSyncerFuncs {
	return podSyncerFuncs{
		syncPod:                   s.SyncPod,
		syncTerminatingPod:        s.SyncTerminatingPod,
		syncTerminatingRuntimePod: s.SyncTerminatingRuntimePod,
		syncTerminatedPod:         s.SyncTerminatedPod,
	}
}

var _ podSyncer = podSyncerFuncs{}

// SyncPod 转发到已注入的 syncPod 函数。
func (f podSyncerFuncs) SyncPod(ctx context.Context, updateType kubetypes.SyncPodType, pod *v1.Pod, mirrorPod *v1.Pod, podStatus *kubecontainer.PodStatus) (bool, func(), error) {
	return f.syncPod(ctx, updateType, pod, mirrorPod, podStatus)
}

// SyncTerminatingPod 转发到已注入的 syncTerminatingPod 函数。
func (f podSyncerFuncs) SyncTerminatingPod(ctx context.Context, pod *v1.Pod, podStatus *kubecontainer.PodStatus, gracePeriod *int64, podStatusFn func(*v1.PodStatus)) error {
	return f.syncTerminatingPod(ctx, pod, podStatus, gracePeriod, podStatusFn)
}

// SyncTerminatingRuntimePod 转发到已注入的 syncTerminatingRuntimePod 函数。
func (f podSyncerFuncs) SyncTerminatingRuntimePod(ctx context.Context, runningPod *kubecontainer.Pod) error {
	return f.syncTerminatingRuntimePod(ctx, runningPod)
}

// SyncTerminatedPod 转发到已注入的 syncTerminatedPod 函数。
func (f podSyncerFuncs) SyncTerminatedPod(ctx context.Context, pod *v1.Pod, podStatus *kubecontainer.PodStatus) error {
	return f.syncTerminatedPod(ctx, pod, podStatus)
}

const (
	// resyncInterval 的抖动因子
	workerResyncIntervalJitterFactor = 0.5

	// backOffPeriod 与 backOffOnTransientErrorPeriod 的抖动因子
	workerBackOffPeriodJitterFactor = 0.5

	// 出现瞬时错误时的退避时长。
	backOffOnTransientErrorPeriod = time.Second
)

// podSyncStatus 跟踪每个 pod 在 worker 同步三个阶段（建立、终止中、已终止）之间的迁移。
type podSyncStatus struct {
	// ctx 在正常 pod 同步之间复用。
	// 显式取消后，下一次 startPodSync 会创建新的 ctx。
	//
	// TODO: 把 ctx 从结构体中移除：在 startPodSync 中初始化 context，由 UpdatePod 使用
	// cancelFn，并在关闭时取消父 context 以拆除 worker（如需要）。
	// 注意不要泄漏 context（见 #139823）。
	// 注意长生命周期的 goroutine（例如 prober worker）的生命期会超过单次 startPodSync
	// 的取消 context。
	ctx context.Context
	// cancelFn 若已设置，用于取消当前的 podSyncer 操作。
	cancelFn context.CancelFunc

	// pod 的 fullname
	fullname string

	// 若有更新待处理或正被 pod worker goroutine 处理，working 为 true。
	working bool
	// pendingUpdate 是 pod worker 应观察的最新状态。worker 读取后它会被清空并移入
	// activeUpdate。新更新总是可以替换 pendingUpdate，因为 worker 不保证所有中间状态
	// 都被同步，只保证最近一次。在 worker 开始处理之前，该状态对下游组件不可见。
	pendingUpdate *UpdatePodOptions
	// activeUpdate 是将传给 sync*Pod 函数的最新 pod 状态。一旦 worker 决定启动 pod
	//（startedAt 被设置），pod 就对下游组件可见。若某次调用缺少 pod 或 mirror pod 字段，
	// 会累积上一次已知值；KillPodOptions 也会累积，因为 pod 的优雅期不能被延长。
	// 对于所有作用于运行中 pod 的组件，这是 kubelet 所对账的 pod spec 的事实来源。
	activeUpdate *UpdatePodOptions

	// syncedAt 是 pod worker 首次观察到该 pod 的时间。
	syncedAt time.Time
	// startedAt 是 pod worker 允许该 pod 启动的时间。
	startedAt time.Time
	// terminatingAt 在 pod 被请求杀死时设置 —— 注意它可能在 worker 真正开始终止前就设置，
	// 参见 startedTerminating。
	terminatingAt time.Time
	// terminatedAt 在 worker 成功完成 syncTerminatingPod 调用后设置，表示所有运行中容器
	// 都已停止。
	terminatedAt time.Time
	// gracePeriod 是 terminatingAt 非零后被请求的优雅期。
	gracePeriod int64
	// notifyPostTerminating 会在 pod 进入 terminated 时被关闭。pod 处于 terminated 状态后，
	// 不应再向该列表追加内容。
	notifyPostTerminating []chan<- struct{}
	// statusPostTerminating 是与 kill pod 请求关联的状态变更列表。pod 处于 terminated
	// 状态后，不应再向该列表追加内容。worker 会在每次终止尝试时执行列表中的最后一个函数。
	statusPostTerminating []PodStatusFunc

	// 一旦 pod worker 观察到停止 pod 的请求（退出 syncPod 并观察到 WorkType 为
	// TerminatingPod 的 podWork），startedTerminating 为 true。设置后，kubelet
	// 的其他组件即可安全地假设不会再有容器被启动。
	startedTerminating bool
	// 若 pod 已在 apiserver 上被标记删除，或已无对应配置（此前被删除），deleted 为 true。
	deleted bool
	// 若 kill 表明本次为驱逐，evicted 为 true（被驱逐的 pod 可以被更激进地清理）。
	evicted bool
	// 从 pod worker 完成该 pod 的处理（syncTerminatedPod 无错误退出）起，直到
	// SyncKnownPods 移除该 pod 为止，finished 为 true。终态 pod（Succeeded/Failed）
	// 在 pod 被删除前会一直保留终止状态。
	finished bool
	// 若 worker 得知 pod 在被杀死后仍应存在（update 类型为 create、update 或 sync），
	// restartRequested 为 true。同步已知 pod 时，任何已终止且 restartRequested
	// 为 true 的 pod 的历史记录都会被清除。
	restartRequested bool
	// 若已观察到 pod 存在于 runtime 中，observedRuntime 为 true。在 runtime 中被观察到
	// 的 pod 必须经过 SyncTerminatingRuntimePod 或 SyncTerminatingPod。否则，若 pod
	// 在启动前就被删除或成为孤儿，可以跳过终止方法。
	observedRuntime bool
}

// IsWorking 表示当前有更新正在被 worker 处理。
func (s *podSyncStatus) IsWorking() bool { return s.working }

// IsTerminationRequested 表示已请求终止该 pod（但未必已被 worker 观察到）。
func (s *podSyncStatus) IsTerminationRequested() bool { return !s.terminatingAt.IsZero() }

// IsTerminationStarted 表示 worker 已观察到终止请求，不会再启动新容器。
func (s *podSyncStatus) IsTerminationStarted() bool { return s.startedTerminating }

// IsTerminated 表示所有容器都已停止。
func (s *podSyncStatus) IsTerminated() bool { return !s.terminatedAt.IsZero() }

// IsFinished 表示 syncTerminatedPod 已成功完成。
func (s *podSyncStatus) IsFinished() bool { return s.finished }

// IsEvicted 表示该 pod 因驱逐而被终止。
func (s *podSyncStatus) IsEvicted() bool { return s.evicted }

// IsDeleted 表示该 pod 已被标记删除或已无配置。
func (s *podSyncStatus) IsDeleted() bool { return s.deleted }

// IsStarted 表示 worker 已允许该 pod 启动。
func (s *podSyncStatus) IsStarted() bool { return !s.startedAt.IsZero() }

// WorkType 返回该 pod 在生命周期状态机中的当前状态。
func (s *podSyncStatus) WorkType() PodWorkerState {
	if s.IsTerminated() {
		return TerminatedPod
	}
	if s.IsTerminationRequested() {
		return TerminatingPod
	}
	return SyncPod
}

// mergeLastUpdate 记录新更新中的最新状态。Pod 与 MirrorPod 会被更新，KillPodOptions
// 会被累积。若 RunningPod 已设置，则 Pod 是合成的，除非此前完全没有 pod 状态，否则
// *不会* 作为最后的 pod 状态使用（因为 worker 可能要终止上一次 kubelet 运行遗留、
// 已无配置可见的 pod）。activeUpdate 的内容是 pod worker 下游组件的事实来源。
func (s *podSyncStatus) mergeLastUpdate(other UpdatePodOptions) {
	opts := s.activeUpdate
	if opts == nil {
		opts = &UpdatePodOptions{}
		s.activeUpdate = opts
	}

	// UpdatePodOptions 约定（并由 UpdatePod 保证）Pod 与 RunningPod 至少设置一个。
	// 我们希望保留最近观察到的 Pod，因此仅在没有 Pod 或 RunningPod 为 nil 时才覆盖 Pod。
	if opts.Pod == nil || other.RunningPod == nil {
		opts.Pod = other.Pod
	}
	// 运行时 pod 不会被持久化，但会记住以便重放
	opts.RunningPod = other.RunningPod
	// 若未提供 mirrorPod，则记住最后一个以便重放
	if other.MirrorPod != nil {
		opts.MirrorPod = other.MirrorPod
	}
	// 累积 kill pod 选项
	if other.KillPodOptions != nil {
		opts.KillPodOptions = &KillPodOptions{}
		if other.KillPodOptions.Evict {
			opts.KillPodOptions.Evict = true
		}
		if override := other.KillPodOptions.PodTerminationGracePeriodSecondsOverride; override != nil {
			value := *override
			opts.KillPodOptions.PodTerminationGracePeriodSecondsOverride = &value
		}
	}
	// 不复制 StartTime —— 它纯粹用于跟踪配置从 kubelet 传播到 pod worker 的延迟。
}

// podWorkers 跟踪 pod 上的操作，确保每个 pod 与容器 runtime 及其他子系统完成对账。
// 它同时跟踪哪些 pod 正在启动、哪些正在关闭但仍有运行中容器、哪些最近已终止且保证
// 没有运行中容器。
//
// podWorkers 是节点上任意时刻应处于活动状态的 pod 的事实来源，并通过 UpdatePod 方法
// 与节点期望状态保持同步（期望状态由 kubelet pod 配置循环和 podManager 跟踪）。
// 作用于运行中 pod 的组件应从 pod worker 而非 podManager 获取状态。pod worker 通过
// SyncKnownPods() 定期与 podManager 状态对账，并负责确保所有已观察、但已不在 podManager
// 中的 pod（即不再属于节点期望配置）都完成收尾。
//
// 传给 pod worker 的 pod 处于以下之一：正在同步（应运行）、终止中（有运行中容器但不再
// 启动新容器）、已终止（无运行中容器但可能仍占用资源）或已清理（无剩余资源）。
// 一旦 pod 被置为“拆除”，在以下条件满足前，同一 UID（对应删除或驱逐）不能被再次启动：
//
//  1. pod worker 已完成收尾（syncTerminatingPod 与 syncTerminatedPod 依次无错误退出）
//  2. kubelet 的 housekeeping 调用了 SyncKnownPods，且该 pod 不在已知配置中。
//
// pod worker 为其他 kubelet 循环提供一致的信息：pod 状态以及容器能否运行。
// ShouldPodContentBeRemoved() 跟踪 pod 内容是否仍应存在，在 SyncKnownPods() 被调用过
// 一次后，也包括已不存在的 pod（按约定，所有现存 pod 都应在调用 SyncKnownPods 前
// 通过 UpdatePod 提供）。一般而言，其他同步循环应分离“建立”和“拆除”职责，这里的信息
// 方法通过集中管理状态来协助双方。各时间区间的简单示意如下：
//
// ---|                                         = kubelet 配置至少已同步一次
// -------|                                  |- = pod 存在于 apiserver 配置中
// --------|                  |---------------- = CouldHaveRunningContainers() 为 true
//
//	^- pod 被 pod worker 观察到  .
//	.                                 .
//
// ----------|       |------------------------- = syncPod 正在运行
//
//	. ^- pod worker 循环看到变更并调用 syncPod
//	. .                               .
//
// --------------|                     |------- = ShouldPodContainersBeTerminating() 返回 true
// --------------|                     |------- = IsPodTerminationRequested() 返回 true（pod 已知）
//
//	. .   ^- Kubelet 驱逐 pod       .
//	. .                               .
//
// -------------------|       |---------------- = syncTerminatingPod 运行后无错误退出
//
//	        . .        ^ pod worker 循环退出 syncPod，看到 pod 正在终止，
//					 . .          调用 syncTerminatingPod
//	        . .                               .
//
// ---|    |------------------|              .  = ShouldPodRuntimeBeRemoved() 返回 true（同步后）
//
//	.                ^ syncTerminatingPod 已成功退出
//	.                               .
//
// ----------------------------|       |------- = syncTerminatedPod 运行后无错误退出
//
//	.                         ^ 其他循环可以开始拆除
//	.                               .
//
// ------------------------------------|  |---- = status manager 正在等待 SyncTerminatedPod() 完成
//
//	.                         ^     .
//
// ----------|                               |- = status manager 可以写入 pod 状态
//
//	^ status manager 因 pod 已不在配置中而删除 pod
//
// Kubelet 中的其他组件可以通过 UpdatePod 方法或 killPodNow 包装器请求终止 pod ——
// 这会确保 pod 的各组件被停止，直到 kubelet 重启或（若 pod 状态变更把 phase 设为终态）
// 永久停止。
type podWorkers struct {
	// 保护所有 worker 字段。
	podLock sync.Mutex
	// pod worker 至少完成一次同步后，podsSynced 为 true，意味着所有工作中的 pod
	// 都已通过 UpdatePod() 启动。
	podsSynced bool

	// 跟踪所有运行中的每 pod goroutine —— 每个 goroutine 处理其对应 channel 收到的更新。
	// 向该 channel 发送消息会通知对应 goroutine 消费 podSyncStatuses[uid].pendingUpdate
	//（若已设置）。
	podUpdates map[types.UID]chan struct{}
	// 按 UID 跟踪 pod 的终止状态 —— 同步中、终止中、已终止、已驱逐。
	podSyncStatuses map[types.UID]*podSyncStatus

	// 按 fullname 跟踪所有已启动静态 pod 的 uid
	startedStaticPodsByFullname map[string]types.UID
	// 按 fullname 跟踪所有等待启动的静态 pod 的 uid
	waitingToStartStaticPodsByFullname map[string][]types.UID

	workQueue queue.WorkQueue

	// 该函数用于同步 pod 的期望状态。
	// 注意：该函数必须线程安全 —— 可能同时对不同 pod 调用。
	podSyncer podSyncer

	// workerChannelFn 暴露给测试，以便单元测试在 channel 通信中引入延迟。
	// 每次启动新 worker goroutine 时会调用一次。
	workerChannelFn func(uid types.UID, in chan struct{}) (out <-chan struct{})

	// 使用的 EventRecorder
	recorder record.EventRecorder

	// backOffPeriod 是同步出错时的退避时长。
	backOffPeriod time.Duration

	// resyncInterval 是距离下次同步的等待时长。
	resyncInterval time.Duration

	// podCache 存储所有 pod 的 kubecontainer.PodStatus。
	podCache kubecontainer.ROCache

	// allocationManager 用于为 pod 分配资源
	allocationManager allocation.Manager

	// clock 用于测试时序
	clock clock.PassiveClock
}

// newPodWorkers 构造 podWorkers，初始化所有 map 并使用真实时钟。
func newPodWorkers(
	podSyncer podSyncer,
	recorder record.EventRecorder,
	workQueue queue.WorkQueue,
	resyncInterval, backOffPeriod time.Duration,
	podCache kubecontainer.ROCache,
	allocationManager allocation.Manager,
) PodWorkers {
	return &podWorkers{
		podSyncStatuses:                    map[types.UID]*podSyncStatus{},
		podUpdates:                         map[types.UID]chan struct{}{},
		startedStaticPodsByFullname:        map[string]types.UID{},
		waitingToStartStaticPodsByFullname: map[string][]types.UID{},
		podSyncer:                          podSyncer,
		recorder:                           recorder,
		workQueue:                          workQueue,
		resyncInterval:                     resyncInterval,
		backOffPeriod:                      backOffPeriod,
		podCache:                           podCache,
		allocationManager:                  allocationManager,
		clock:                              clock.RealClock{},
	}
}

// IsPodKnownTerminated 实现 PodWorkers 接口，见接口注释。
func (p *podWorkers) IsPodKnownTerminated(uid types.UID) bool {
	p.podLock.Lock()
	defer p.podLock.Unlock()
	if status, ok := p.podSyncStatuses[uid]; ok {
		return status.IsTerminated()
	}
	// 若 pod 未知，返回 false（pod worker 并不知晓它）
	return false
}

// CouldHaveRunningContainers 实现 PodWorkers 接口，见接口注释。
func (p *podWorkers) CouldHaveRunningContainers(uid types.UID) bool {
	p.podLock.Lock()
	defer p.podLock.Unlock()
	if status, ok := p.podSyncStatuses[uid]; ok {
		return !status.IsTerminated()
	}
	// 所有 pod 同步完成后，任何没有同步状态的 pod 都已知不在运行。
	return !p.podsSynced
}

// ShouldPodBeFinished 实现 PodWorkers 接口，见接口注释。
func (p *podWorkers) ShouldPodBeFinished(uid types.UID) bool {
	p.podLock.Lock()
	defer p.podLock.Unlock()
	if status, ok := p.podSyncStatuses[uid]; ok {
		return status.IsFinished()
	}
	// 所有 pod 同步完成后，任何没有同步状态的 pod 都被认为 SyncTerminatedPod 已完成。
	return p.podsSynced
}

// IsPodTerminationRequested 实现 PodWorkers 接口，见接口注释。
func (p *podWorkers) IsPodTerminationRequested(uid types.UID) bool {
	p.podLock.Lock()
	defer p.podLock.Unlock()
	if status, ok := p.podSyncStatuses[uid]; ok {
		// 此时 pod 可能仍在建立中。
		return status.IsTerminationRequested()
	}
	// 未知 pod 被视为未在终止（清理循环中应使用 ShouldPodContainersBeTerminating，
	// 以免漏清理已从配置中移除的 pod）
	return false
}

// ShouldPodContainersBeTerminating 实现 PodWorkers 接口，见接口注释。
func (p *podWorkers) ShouldPodContainersBeTerminating(uid types.UID) bool {
	p.podLock.Lock()
	defer p.podLock.Unlock()
	if status, ok := p.podSyncStatuses[uid]; ok {
		// 我们等待 pod worker goroutine 观察到终止，这意味着 syncPod 不会再执行，
		// 也就不会启动新容器
		return status.IsTerminationStarted()
	}
	// 同步完成后，若 worker 不认识该 pod，说明我们应将其拆除
	return p.podsSynced
}

// ShouldPodRuntimeBeRemoved 实现 PodWorkers 接口，见接口注释。
func (p *podWorkers) ShouldPodRuntimeBeRemoved(uid types.UID) bool {
	p.podLock.Lock()
	defer p.podLock.Unlock()
	if status, ok := p.podSyncStatuses[uid]; ok {
		return status.IsTerminated()
	}
	// 尚未发送给 pod worker 的 pod，在所有内容同步完成后不应有任何 runtime 组件。
	return p.podsSynced
}

// ShouldPodContentBeRemoved 实现 PodWorkers 接口，见接口注释。
func (p *podWorkers) ShouldPodContentBeRemoved(uid types.UID) bool {
	p.podLock.Lock()
	defer p.podLock.Unlock()
	if status, ok := p.podSyncStatuses[uid]; ok {
		return status.IsEvicted() || (status.IsDeleted() && status.IsTerminated())
	}
	// 尚未发送给 pod worker 的 pod，在所有内容同步完成后不应在磁盘上保留内容。
	return p.podsSynced
}

// IsPodForMirrorPodTerminatingByFullName 实现 PodWorkers 接口，见接口注释。
func (p *podWorkers) IsPodForMirrorPodTerminatingByFullName(podFullName string) bool {
	p.podLock.Lock()
	defer p.podLock.Unlock()
	uid, started := p.startedStaticPodsByFullname[podFullName]
	if !started {
		return false
	}
	status, exists := p.podSyncStatuses[uid]
	if !exists {
		return false
	}
	if !status.IsTerminationRequested() || status.IsTerminated() {
		return false
	}

	return true
}

// isPodStatusCacheTerminal 判断 runtime 缓存中的 pod 状态是否已无任何运行中的容器或
// 就绪的 sandbox —— 即该 pod 在 runtime 侧已是终态。
func isPodStatusCacheTerminal(status *kubecontainer.PodStatus) bool {
	for _, container := range status.ContainerStatuses {
		if container.State == kubecontainer.ContainerStateRunning {
			return false
		}
	}
	for _, sb := range status.SandboxStatuses {
		if sb.State == runtimeapi.PodSandboxState_SANDBOX_READY {
			return false
		}
	}
	return true
}

// UpdatePod 将配置变更或终止状态传递给 pod。pod 处于可运行、终止中或已终止状态；
// 若 pod 在 apiserver 上被删除、被发现处于终态（Succeeded/Failed）或被 kubelet 驱逐，
// 则会转为终止中。
func (p *podWorkers) UpdatePod(ctx context.Context, options UpdatePodOptions) {
	logger := klog.FromContext(ctx)
	// 当 pod 是孤儿（无配置）且只有 runtime 状态时，只执行生命周期的终止部分。
	// 运行时 pod 只包含 pod 的最小信息集
	var isRuntimePod bool
	var uid types.UID
	var name, ns string
	if runningPod := options.RunningPod; runningPod != nil {
		if options.Pod == nil {
			// 这里创建的合成 pod 仅作占位，不被跟踪
			if options.UpdateType != kubetypes.SyncPodKill {
				logger.Info("Pod update is ignored, runtime pods can only be killed", "pod", klog.KRef(runningPod.Namespace, runningPod.Name), "podUID", runningPod.ID, "updateType", options.UpdateType)
				return
			}
			uid, ns, name = runningPod.ID, runningPod.Namespace, runningPod.Name
			isRuntimePod = true
		} else {
			options.RunningPod = nil
			uid, ns, name = options.Pod.UID, options.Pod.Namespace, options.Pod.Name
			logger.Info("Pod update included RunningPod which is only valid when Pod is not specified", "pod", klog.KRef(ns, name), "podUID", uid, "updateType", options.UpdateType)
		}
	} else {
		uid, ns, name = options.Pod.UID, options.Pod.Namespace, options.Pod.Name
	}

	updateLogger := logger.WithValues("pod", klog.KRef(ns, name), "podUID", uid, "updateType", options.UpdateType)
	p.podLock.Lock()
	defer p.podLock.Unlock()

	// 决定如何处理该 pod —— 建立、拆除或忽略
	var firstTime bool
	now := p.clock.Now()
	status, ok := p.podSyncStatuses[uid]
	if !ok {
		updateLogger.V(4).Info("Pod is being synced for the first time")
		firstTime = true
		status = &podSyncStatus{
			syncedAt: now,
			fullname: kubecontainer.BuildPodFullName(name, ns),
		}
		// 若首次同步该 pod，需要确认它是一个活动 pod
		if options.Pod != nil && (options.Pod.Status.Phase == v1.PodFailed || options.Pod.Status.Phase == v1.PodSucceeded) {
			// 检查 pod 是否已不在运行且处于终态；若成立则在 podWorker 中记录其已终止。
			// 这是因为 kubelet 重启后，必须确保终态 pod 在 Pod Admission 中不被视为活动 pod。
			// 见 http://issues.k8s.io/105523
			// 不过 `filterOutInactivePods` 会把正在终止的 pod 视为活动 pod。因此
			// `IsPodKnownTerminated()` 需要返回 true，也就必须设置 `terminatedAt`。
			if statusCache, err := p.podCache.Get(uid); err == nil {
				if isPodStatusCacheTerminal(statusCache) {
					// 此时可以确定：
					// (1) 按配置源，pod 处于终态。
					// (2) 按 runtime 缓存，pod 处于终态。
					// 这意味着该 pod 在过去某个时刻已完成 `SyncTerminatingPod`。它很可能是因为
					// kubelet 重启而首次被同步。
					// 这些 pod 需要完成 SyncTerminatedPod，以确保所有资源被清理、status manager
					// 为 pod 发出最终状态更新。
					// 因此设置 finished: false，确保会发送 Terminated 事件并执行 `SyncTerminatedPod`。
					status = &podSyncStatus{
						terminatedAt:       now,
						terminatingAt:      now,
						syncedAt:           now,
						startedTerminating: true,
						finished:           false,
						fullname:           kubecontainer.BuildPodFullName(name, ns),
					}
				}
			}
		}
		p.podSyncStatuses[uid] = status
	}

	// RunningPods 表示未知的 pod 执行实例，其 pod spec 信息不足以执行除终止外的任何操作。
	// 若在已提供真实 pod 之后收到 RunningPod，则改用最近的 spec。另外，一旦观察到运行时
	// pod，就必须将其驱动到完成，即使它不是由我们启动的。
	pod := options.Pod
	if isRuntimePod {
		status.observedRuntime = true
		switch {
		case status.pendingUpdate != nil && status.pendingUpdate.Pod != nil:
			pod = status.pendingUpdate.Pod
			options.Pod = pod
			options.RunningPod = nil
		case status.activeUpdate != nil && status.activeUpdate.Pod != nil:
			pod = status.activeUpdate.Pod
			options.Pod = pod
			options.RunningPod = nil
		default:
			// 这里仍会使用 RunningPod.ToAPIPod() 作为 pod，但 options.Pod 为 nil，
			// 其他方法必须妥善处理这种情况。
			pod = options.RunningPod.ToAPIPod()
		}
	}

	// 当已终止中的 pod 收到 create 更新时，说明两个相同 UID 的 pod 在时间上非常接近地
	// 被创建（通常是静态 pod，apiserver 极少数情况下也可能如此）—— 标记同步状态，表示
	// pod 终止后应重置为“未运行”，以允许后续 add/update 重新启动 pod worker。
	// 这不适用于首次看到 pod 的情况，例如 kubelet 重启后首次看到已终止的 pod。
	if !firstTime && status.IsTerminationRequested() {
		if options.UpdateType == kubetypes.SyncPodCreate {
			status.restartRequested = true
			updateLogger.V(4).Info("Pod is terminating but has been requested to restart with same UID, will be reconciled later")
			return
		}
	}

	// 一旦某个 UID 的 pod 已终止，它就不能再进入 pod worker（直到 housekeeping 清除该 UID）
	if status.IsFinished() {
		updateLogger.V(4).Info("Pod is finished processing, no further updates")
		return
	}

	// 检查是否转入终止状态
	var becameTerminating bool
	if !status.IsTerminationRequested() {
		switch {
		case isRuntimePod:
			updateLogger.V(4).Info("Pod is orphaned and must be torn down")
			status.deleted = true
			status.terminatingAt = now
			becameTerminating = true
		case pod.DeletionTimestamp != nil:
			updateLogger.V(4).Info("Pod is marked for graceful deletion, begin teardown")
			status.deleted = true
			status.terminatingAt = now
			becameTerminating = true
		case pod.Status.Phase == v1.PodFailed, pod.Status.Phase == v1.PodSucceeded:
			updateLogger.V(4).Info("Pod is in a terminal phase (success/failed), begin teardown")
			status.terminatingAt = now
			becameTerminating = true
		case options.UpdateType == kubetypes.SyncPodKill:
			if options.KillPodOptions != nil && options.KillPodOptions.Evict {
				updateLogger.V(4).Info("Pod is being evicted by the kubelet, begin teardown")
				status.evicted = true
			} else {
				updateLogger.V(4).Info("Pod is being removed by the kubelet, begin teardown")
			}
			status.terminatingAt = now
			becameTerminating = true
		}
	}

	// 一旦 pod 进入终止中，所有更新都视为 kill，且优雅期只能缩短
	var wasGracePeriodShortened bool
	switch {
	case status.IsTerminated():
		// 已终止的 pod 可能仍在等待清理 —— 若因 housekeeping 看到较旧的 runtime pod 缓存版本
		// 而收到运行时 pod 的 kill 请求，在 pod worker 完成前直接忽略即可。
		if isRuntimePod {
			updateLogger.V(3).Info("Pod is waiting for termination, ignoring runtime-only kill until after pod worker is fully terminated")
			return
		}

		if options.KillPodOptions != nil {
			if ch := options.KillPodOptions.CompletedCh; ch != nil {
				close(ch)
			}
		}
		options.KillPodOptions = nil

	case status.IsTerminationRequested():
		if options.KillPodOptions == nil {
			options.KillPodOptions = &KillPodOptions{}
		}

		if ch := options.KillPodOptions.CompletedCh; ch != nil {
			status.notifyPostTerminating = append(status.notifyPostTerminating, ch)
		}
		if fn := options.KillPodOptions.PodStatusFunc; fn != nil {
			status.statusPostTerminating = append(status.statusPostTerminating, fn)
		}

		gracePeriod, gracePeriodShortened := calculateEffectiveGracePeriod(status, pod, options.KillPodOptions)

		wasGracePeriodShortened = gracePeriodShortened
		status.gracePeriod = gracePeriod
		// 始终为 syncTerminatingPod 设置优雅期，避免重复计算；它绝不会为零。
		options.KillPodOptions.PodTerminationGracePeriodSecondsOverride = &gracePeriod

	default:
		// 在终止阶段之外，KillPodOptions 对同步操作无效
		if options.KillPodOptions != nil {
			if ch := options.KillPodOptions.CompletedCh; ch != nil {
				close(ch)
			}
			options.KillPodOptions = nil
		}
	}

	// 若 pod worker goroutine 尚不存在则启动它
	podUpdates, exists := p.podUpdates[uid]
	if !exists {
		// 为 channel 设置缓冲，避免阻塞本方法
		podUpdates = make(chan struct{}, 1)
		p.podUpdates[uid] = podUpdates

		// 确保静态 pod 按 UpdatePod 收到它们的顺序启动
		if kubetypes.IsStaticPod(pod) {
			p.waitingToStartStaticPodsByFullname[status.fullname] =
				append(p.waitingToStartStaticPodsByFullname[status.fullname], uid)
		}

		// 允许测试在 pod 更新 channel 中注入延迟
		var outCh <-chan struct{}
		if p.workerChannelFn != nil {
			outCh = p.workerChannelFn(uid, podUpdates)
		} else {
			outCh = podUpdates
		}

		// 启动一个 pod worker
		go func() {
			// TODO: 这里应改为带退避的 wait.Until 以处理 panic，并接受用于关闭的 context
			defer runtime.HandleCrash()
			defer logger.V(3).Info("Pod worker has stopped", "podUID", uid)
			p.podWorkerLoop(ctx, uid, outCh)
		}()
	}

	// 保留最早的 StartTime，以测量从调用 UpdatePod 到 worker 响应之间的最大延迟
	if status.pendingUpdate != nil && !status.pendingUpdate.StartTime.IsZero() && status.pendingUpdate.StartTime.Before(options.StartTime) {
		options.StartTime = status.pendingUpdate.StartTime
	}

	// 通知 pod worker 有待处理的更新
	status.pendingUpdate = &options
	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling) {
		status.pendingUpdate.Pod, _ = p.allocationManager.UpdatePodFromAllocation(options.Pod)
	}
	status.working = true
	updateLogger.V(4).Info("Notifying pod of pending update", "workType", status.WorkType())
	select {
	case podUpdates <- struct{}{}:
	default:
	}

	if (becameTerminating || wasGracePeriodShortened) && status.cancelFn != nil {
		updateLogger.V(3).Info("Cancelling current pod sync", "workType", status.WorkType())
		status.cancelFn()
		return
	}
}

// calculateEffectiveGracePeriod 为刚进入终止的 pod 设置初始优雅期，或允许提供更短的
// 优雅期，并返回最终应采用的值。
func calculateEffectiveGracePeriod(status *podSyncStatus, pod *v1.Pod, options *KillPodOptions) (int64, bool) {
	// 强制优雅期只能缩短，记录当前值，并确保把计算后的值传递到更下层
	gracePeriod := status.gracePeriod
	overridden := false
	// 该值是根本事实 —— apiserver 负责告诉我们由它计算出的值
	if override := pod.DeletionGracePeriodSeconds; override != nil {
		if gracePeriod == 0 || *override < gracePeriod {
			gracePeriod = *override
			overridden = true
		}
	}
	// 允许 kubelet 的其他部分（即驱逐）请求更快地终止该 pod
	if options != nil {
		if override := options.PodTerminationGracePeriodSecondsOverride; override != nil {
			if gracePeriod == 0 || *override < gracePeriod {
				gracePeriod = *override
				overridden = true
			}
		}
	}
	// 若 kubelet 未提供请求值（优雅终止？），尽最大努力默认使用 pod 期望的值
	if !overridden && gracePeriod == 0 && pod.Spec.TerminationGracePeriodSeconds != nil {
		gracePeriod = *pod.Spec.TerminationGracePeriodSeconds
	}
	// 无论如何，优雅期至少为 1
	if gracePeriod < 1 {
		gracePeriod = 1
	}
	return gracePeriod, status.gracePeriod != 0 && status.gracePeriod != gracePeriod
}

// allowPodStart 尝试启动 pod：允许则返回 true，否则重新入队并返回 false。
// 若因数据缺失导致 pod 永远无法启动，或 pod 在启动前已被终止，canEverStart 为 false。
// 只能在持有 pod 锁时调用。
func (p *podWorkers) allowPodStart(logger klog.Logger, pod *v1.Pod) (canStart bool, canEverStart bool) {
	if !kubetypes.IsStaticPod(pod) {
		// TODO: 是否要允许同名 fullname 的非静态 pod？
		// 注意这可能会使 pod 的强制删除失效。
		return true, true
	}
	status, ok := p.podSyncStatuses[pod.UID]
	if !ok {
		logger.Error(nil, "Pod sync status does not exist, the worker should not be running", "pod", klog.KObj(pod), "podUID", pod.UID)
		return false, false
	}
	if status.IsTerminationRequested() {
		return false, false
	}
	if !p.allowStaticPodStart(status.fullname, pod.UID) {
		p.workQueue.Enqueue(pod.UID, wait.Jitter(p.backOffPeriod, workerBackOffPeriodJitterFactor))
		return false, true
	}
	return true, true
}

// allowStaticPodStart 尝试启动静态 pod，满足以下条件时返回 true：
// 1. 没有其他已启动且 fullname 相同的静态 pod
// 2. uid 与等待启动的第一个有效静态 pod 的 uid 一致
func (p *podWorkers) allowStaticPodStart(fullname string, uid types.UID) bool {
	startedUID, started := p.startedStaticPodsByFullname[fullname]
	if started {
		return startedUID == uid
	}

	waitingPods := p.waitingToStartStaticPodsByFullname[fullname]
	// TODO: 对于 fullname 重叠的静态 pod 更新次数，这里是 O(N)，理想情况下应为 O(1)。
	for i, waitingUID := range waitingPods {
		// pod 是否已终止或被删除？
		status, ok := p.podSyncStatuses[waitingUID]
		if !ok || status.IsTerminationRequested() || status.IsTerminated() {
			continue
		}
		// 队列中的下一个 pod 是别人
		if waitingUID != uid {
			p.waitingToStartStaticPodsByFullname[fullname] = waitingPods[i:]
			return false
		}
		// 轮到我们了，把自己从队列中移除
		waitingPods = waitingPods[i+1:]
		break
	}
	if len(waitingPods) != 0 {
		p.waitingToStartStaticPodsByFullname[fullname] = waitingPods
	} else {
		delete(p.waitingToStartStaticPodsByFullname, fullname)
	}
	p.startedStaticPodsByFullname[fullname] = uid
	return true
}

// cleanupUnstartedPod 在从未启动过的 pod 于启动前收到终止信号时被调用。
// 调用时必须持有 pod 锁。
func (p *podWorkers) cleanupUnstartedPod(logger klog.Logger, pod *v1.Pod, status *podSyncStatus) {
	p.cleanupPodUpdates(pod.UID)

	if status.terminatingAt.IsZero() {
		logger.V(4).Info("Pod worker is complete but did not have terminatingAt set, likely programmer error", "pod", klog.KObj(pod), "podUID", pod.UID)
	}
	if !status.terminatedAt.IsZero() {
		logger.V(4).Info("Pod worker is complete and had terminatedAt set, likely programmer error", "pod", klog.KObj(pod), "podUID", pod.UID)
	}
	status.finished = true
	status.working = false
	status.terminatedAt = p.clock.Now()

	if p.startedStaticPodsByFullname[status.fullname] == pod.UID {
		delete(p.startedStaticPodsByFullname, status.fullname)
	}
}

// startPodSync 在 pod 更新 channel 收到消息时由各 pod worker goroutine 调用。
// 该方法消费一个待处理更新、初始化 context、判断 pod 是否已启动或可启动，并更新缓存的
// pod 状态，使下游组件能观察到 worker goroutine 当前正在尝试做什么。若 ok 为 false，
// 表示没有可用事件；若任一布尔值为 false，返回前需确保执行了相应的清理。
//
// 该方法须保证：要么清空 status.pendingUpdate 并合并进 status.activeUpdate，
// 要么在 pod 无法启动时保持 status.pendingUpdate 不变。未启动的 pod 绝不应有
// activeUpdate，因为它会对下游组件暴露已启动 pod 的状态。
func (p *podWorkers) startPodSync(parentCtx context.Context, podUID types.UID) (ctx context.Context, update podWork, canStart, canEverStart, ok bool) {
	p.podLock.Lock()
	defer p.podLock.Unlock()

	// 确认 pod worker 仍知晓该 pod
	status, ok := p.podSyncStatuses[podUID]
	if !ok {
		// pod 状态已消失，worker 应退出
		return nil, update, false, false, false
	}
	logger := klog.FromContext(parentCtx)
	if !status.working {
		// working 供单元测试观察 worker 当前是否正在处理该 pod
		logger.V(4).Info("Pod should be marked as working by the pod worker, programmer error", "podUID", podUID)
	}
	if status.pendingUpdate == nil {
		// 没有可用更新，说明入队时未添加工作，或存在竞态，两者都不应发生
		status.working = false
		logger.V(4).Info("Pod worker received no pending work, programmer error?", "podUID", podUID)
		return nil, update, false, false, false
	}

	// 消费待处理更新
	update.WorkType = status.WorkType()
	update.Options = *status.pendingUpdate
	status.pendingUpdate = nil
	select {
	case <-p.podUpdates[podUID]:
		// 确保 pod 更新 channel 为空（它只在持锁时写入）
	default:
	}

	if status.ctx == nil || status.ctx.Err() != nil {
		// 创建携带 parentCtx 值的 context，并复用直到它被取消
		status.ctx, status.cancelFn = context.WithCancel(context.WithoutCancel(parentCtx))
	}
	ctx = status.ctx

	// 若已启动，则让我们的状态对下游组件可见
	if status.IsStarted() {
		status.mergeLastUpdate(update.Options)
		return ctx, update, true, true, true
	}

	// 若已在终止中且只有运行时 pod，允许 worker “启动”，因为我们会立即转入终止
	if update.Options.RunningPod != nil && update.WorkType == TerminatingPod {
		status.mergeLastUpdate(update.Options)
		return ctx, update, true, true, true
	}

	// 若收到的更新中 Pod 为 nil（只设置了运行时 pod）且尚未启动，我们只能终止该 pod，
	// 无法启动它。不应要求启动这样的 pod，这里只是防御性检查。
	if update.Options.Pod == nil {
		status.mergeLastUpdate(update.Options)
		logger.V(4).Info("Running pod cannot start ever, programmer error", "pod", klog.KObj(update.Options.Pod), "podUID", podUID, "updateType", update.WorkType)
		return ctx, update, false, false, true
	}

	// 确认可以启动
	canStart, canEverStart = p.allowPodStart(logger, update.Options.Pod)
	switch {
	case !canEverStart:
		p.cleanupUnstartedPod(logger, update.Options.Pod, status)
		status.working = false
		if start := update.Options.StartTime; !start.IsZero() {
			metrics.PodWorkerDuration.WithLabelValues("terminated").Observe(metrics.SinceInSeconds(start))
		}
		logger.V(4).Info("Pod cannot start ever", "pod", klog.KObj(update.Options.Pod), "podUID", podUID, "updateType", update.WorkType)
		return ctx, update, canStart, canEverStart, true
	case !canStart:
		// 这是唯一不启动 pod 的路径，因此需要把变更放回 pendingUpdate
		status.pendingUpdate = &update.Options
		status.working = false
		logger.V(4).Info("Pod cannot start yet", "pod", klog.KObj(update.Options.Pod), "podUID", podUID)
		return ctx, update, canStart, canEverStart, true
	}

	// 标记 pod 已启动
	status.startedAt = p.clock.Now()
	status.mergeLastUpdate(update.Options)

	// 若正在准入一个新 pod，记录其容器数量
	// TODO: 或许应把它移入 syncPod，并在 syncPod 参数中加入执行次数，在首次同步时记录。
	// 留在这里会让一个特别重要的循环变复杂。
	metrics.ContainersPerPodCount.Observe(float64(len(update.Options.Pod.Spec.Containers)))

	return ctx, update, true, true, true
}

// podUIDAndRefForUpdate 从更新中提取 UID 与日志用的对象引用，兼容仅含运行时 pod 的情况。
func podUIDAndRefForUpdate(update UpdatePodOptions) (types.UID, klog.ObjectRef) {
	if update.RunningPod != nil {
		return update.RunningPod.ID, klog.KObj(update.RunningPod.ToAPIPod())
	}
	return update.Pod.UID, klog.KObj(update.Pod)
}

// podWorkerLoop 在 goroutine 中按顺序处理 pod 状态更新，到达最终状态后退出。
// 该循环负责将 pod 驱动经过四个主要阶段：
//
// 1. 等待启动，保证同一 UID 或同一 fullname 的两个 pod 不会同时运行
// 2. 同步，通过对账期望 pod spec 与 pod 运行时状态来编排 pod 建立
// 3. 终止中，确保 pod 内所有运行中容器被停止
// 4. 已终止，清理 pod 删除前必须释放的所有资源
//
// podWorkerLoop 由 UpdatePod 投递的更新和 SyncKnownPods 驱动。若某个同步方法失败，
// p.workerQueue 会按退避时间更新，但触发新的 UpdatePod 调用是 kubelet 的职责。
// SyncKnownPods 只会重试调用方已不再知晓的 pod。当 pod 从 working 转为 terminating
// 或从 terminating 转为 terminated 时，下一次更新会立即入队，无需 kubelet 操作。
func (p *podWorkers) podWorkerLoop(parentCtx context.Context, podUID types.UID, podUpdates <-chan struct{}) {
	var lastSyncTime time.Time
	for range podUpdates {
		ctx, update, canStart, canEverStart, ok := p.startPodSync(parentCtx, podUID)
		// 若没有等待的更新，说明有人初始化了 channel 却没有填写 pendingUpdate。
		if !ok {
			continue
		}
		logger := klog.FromContext(ctx)
		// 若 pod 在被允许启动之前就已终止，退出循环。
		if !canEverStart {
			return
		}
		// 若 pod 尚未就绪，继续等待更多更新。
		if !canStart {
			continue
		}

		podUID, podRef := podUIDAndRefForUpdate(update.Options)

		logger.V(4).Info("Processing pod event", "pod", podRef, "podUID", podUID, "updateType", update.WorkType)
		var isTerminal bool
		err := func() error {
			// worker 负责确保同步方法看到合适的状态：重同步时看到上次同步的结果；
			// 转入终止时无需等待；已终止时看到最近的状态。只有同步和终止会产生 pod 状态变更，
			// 已终止的 pod 则确保最近状态最终到达 api server。
			var status *kubecontainer.PodStatus
			var err error
			switch {
			case update.Options.RunningPod != nil:
				// 收到运行时 pod 时完全不需要状态，因为此时必定在终止，且会跳过对 pod 的更新
			default:
				// 等待通过缓存看到 PLEG 的下一次刷新（最多 2 秒）
				// TODO: 这会在 sync->terminating->terminated 的所有转换以及所有终止重试
				//  （含驱逐）中增加约 1 秒延迟。应让 PLEG 连续运行，并在关键事件
				//  （killPod、sync->terminating）发生时刷新 pod 状态以降低延迟。
				//  降低延迟还能减少已终止容器的状态在我们更新 API server 之前就被垃圾回收
				//  的可能（否则会丢失退出码）。
				status, err = p.podCache.GetNewerThan(update.Options.Pod.UID, lastSyncTime)

				if err != nil {
					// 这是 manage pod 循环遗留的事件，其余事件现在都由 syncPodFn 分发
					p.recorder.Eventf(update.Options.Pod, v1.EventTypeWarning, events.FailedSync, "error determining status: %v", err)
					return err
				}
			}

			// 执行相应动作（非法阶段已由 UpdatePod 阻止）
			var postSync func()
			switch {
			case update.WorkType == TerminatedPod:
				err = p.podSyncer.SyncTerminatedPod(ctx, update.Options.Pod, status)

			case update.WorkType == TerminatingPod:
				var gracePeriod *int64
				if opt := update.Options.KillPodOptions; opt != nil {
					gracePeriod = opt.PodTerminationGracePeriodSecondsOverride
				}
				podStatusFn := p.acknowledgeTerminating(logger, podUID)

				// 若只有运行时 pod，直接终止它
				if update.Options.RunningPod != nil {
					err = p.podSyncer.SyncTerminatingRuntimePod(ctx, update.Options.RunningPod)
				} else {
					err = p.podSyncer.SyncTerminatingPod(ctx, update.Options.Pod, status, gracePeriod, podStatusFn)
				}

			default:
				isTerminal, postSync, err = p.podSyncer.SyncPod(ctx, update.Options.UpdateType, update.Options.Pod, update.Options.MirrorPod, status)
			}

			lastSyncTime = p.clock.Now()
			if postSync != nil {
				postSync()
			}

			return err
		}()

		var phaseTransition bool
		switch {
		case errors.Is(err, context.Canceled):
			// context 被取消时，预期已有更新在队列中
			logger.V(2).Info("Sync exited with context cancellation error", "pod", podRef, "podUID", podUID, "updateType", update.WorkType)

		case err != nil:
			// 将排队重试
			logger.Error(err, "Error syncing pod, skipping", "pod", podRef, "podUID", podUID)

		case update.WorkType == TerminatedPod:
			// 可以关闭 worker
			p.completeTerminated(logger, podUID)
			if start := update.Options.StartTime; !start.IsZero() {
				metrics.PodWorkerDuration.WithLabelValues("terminated").Observe(metrics.SinceInSeconds(start))
			}
			logger.V(4).Info("Processing pod event done", "pod", podRef, "podUID", podUID, "updateType", update.WorkType)
			return

		case update.WorkType == TerminatingPod:
			// 不在配置中的 pod 无需终止，其他循环会清理它们
			if update.Options.RunningPod != nil {
				p.completeTerminatingRuntimePod(logger, podUID)
				if start := update.Options.StartTime; !start.IsZero() {
					metrics.PodWorkerDuration.WithLabelValues(update.Options.UpdateType.String()).Observe(metrics.SinceInSeconds(start))
				}
				logger.V(4).Info("Processing pod event done", "pod", podRef, "podUID", podUID, "updateType", update.WorkType)
				return
			}
			// 否则进入终止阶段
			p.completeTerminating(logger, podUID)
			phaseTransition = true

		case isTerminal:
			// 若 syncPod 表明已到终态，则设置相应状态以转入终止
			logger.V(4).Info("Pod is terminal", "pod", podRef, "podUID", podUID, "updateType", update.WorkType)
			p.completeSync(logger, podUID)
			phaseTransition = true
		}

		// 必要时排队重试，然后若还有下一事件则放入 channel
		p.completeWork(logger, podUID, phaseTransition, err)
		if start := update.Options.StartTime; !start.IsZero() {
			metrics.PodWorkerDuration.WithLabelValues(update.Options.UpdateType.String()).Observe(metrics.SinceInSeconds(start))
		}
		logger.V(4).Info("Processing pod event done", "pod", podRef, "podUID", podUID, "updateType", update.WorkType)
	}
}

// acknowledgeTerminating 在 pod worker 看到终止状态时，在 pod 状态上设置 terminating 标记，
// 使其他组件知道该 pod 不会再启动新容器。随后返回适用于该 pod 的状态函数（若有）。
func (p *podWorkers) acknowledgeTerminating(logger klog.Logger, podUID types.UID) PodStatusFunc {
	p.podLock.Lock()
	defer p.podLock.Unlock()

	status, ok := p.podSyncStatuses[podUID]
	if !ok {
		return nil
	}

	if !status.terminatingAt.IsZero() && !status.startedTerminating {
		logger.V(4).Info("Pod worker has observed request to terminate", "podUID", podUID)
		status.startedTerminating = true
	}

	if l := len(status.statusPostTerminating); l > 0 {
		return status.statusPostTerminating[l-1]
	}
	return nil
}

// completeSync 在 syncPod 成功完成并表明 pod 已到终态、应被终止时调用。这发生在 pod 自然
// 生命周期结束时 —— 任何 RestartPolicy 不是 Always 的 pod 退出。非自然的结束（如驱逐、
// API 驱动的删除或阶段转换）由 UpdatePod 处理。
func (p *podWorkers) completeSync(logger klog.Logger, podUID types.UID) {
	p.podLock.Lock()
	defer p.podLock.Unlock()

	logger.V(4).Info("Pod indicated lifecycle completed naturally and should now terminate", "podUID", podUID)

	status, ok := p.podSyncStatuses[podUID]
	if !ok {
		logger.V(4).Info("Pod had no status in completeSync, programmer error?", "podUID", podUID)
		return
	}

	// 更新 pod 状态
	if status.terminatingAt.IsZero() {
		status.terminatingAt = p.clock.Now()
	} else {
		logger.V(4).Info("Pod worker attempted to set terminatingAt twice, likely programmer error", "podUID", podUID)
	}
	status.startedTerminating = true

	// pod 现已转入终止中，我们希望尽快运行 syncTerminatingPod，因此若没有等待中的更新，
	// 就排入一个合成更新
	p.requeueLastPodUpdate(podUID, status)
}

// completeTerminating 在 syncTerminatingPod 成功完成时调用，意味着没有容器在运行、
// 将来也不会启动容器，可以开始清理。它会更新终止状态，阻止后续同步，并确保其他 kubelet
// 循环知道该 pod 不再运行任何容器。
func (p *podWorkers) completeTerminating(logger klog.Logger, podUID types.UID) {
	p.podLock.Lock()
	defer p.podLock.Unlock()

	logger.V(4).Info("Pod terminated all containers successfully", "podUID", podUID)

	status, ok := p.podSyncStatuses[podUID]
	if !ok {
		return
	}

	// 更新 pod 状态
	if status.terminatingAt.IsZero() {
		logger.V(4).Info("Pod worker was terminated but did not have terminatingAt set, likely programmer error", "podUID", podUID)
	}
	status.terminatedAt = p.clock.Now()
	for _, ch := range status.notifyPostTerminating {
		close(ch)
	}
	status.notifyPostTerminating = nil
	status.statusPostTerminating = nil

	// pod 现已转入已终止，我们希望尽快运行 syncTerminatedPod，因此若没有等待中的更新，
	// 就排入一个合成更新
	p.requeueLastPodUpdate(podUID, status)
}

// completeTerminatingRuntimePod 在 syncTerminatingPod 成功完成时调用，意味着孤儿 pod
// （无配置）已终止，可以退出。由于孤儿 pod 没有 API 表示，此时应退出循环并确保之后
// 不留下任何状态 —— 调用该方法时运行时 pod 才真正终止。
func (p *podWorkers) completeTerminatingRuntimePod(logger klog.Logger, podUID types.UID) {
	p.podLock.Lock()
	defer p.podLock.Unlock()

	logger.V(4).Info("Pod terminated all orphaned containers successfully and worker can now stop", "podUID", podUID)

	p.cleanupPodUpdates(podUID)

	status, ok := p.podSyncStatuses[podUID]
	if !ok {
		return
	}
	if status.terminatingAt.IsZero() {
		logger.V(4).Info("Pod worker was terminated but did not have terminatingAt set, likely programmer error", "podUID", podUID)
	}
	status.terminatedAt = p.clock.Now()
	status.finished = true
	status.working = false

	if p.startedStaticPodsByFullname[status.fullname] == podUID {
		delete(p.startedStaticPodsByFullname, status.fullname)
	}

	// 运行时 pod 是瞬态的，不属于期望状态 —— 一旦到达已终止，就可以放弃跟踪它。
	delete(p.podSyncStatuses, podUID)
}

// completeTerminated 在 syncTerminatedPod 成功完成后调用，意味着可以停止 pod worker。
// 此时 pod 已完成收尾。
func (p *podWorkers) completeTerminated(logger klog.Logger, podUID types.UID) {
	p.podLock.Lock()
	defer p.podLock.Unlock()

	logger.V(4).Info("Pod is complete and the worker can now stop", "podUID", podUID)

	p.cleanupPodUpdates(podUID)

	status, ok := p.podSyncStatuses[podUID]
	if !ok {
		return
	}
	if status.terminatingAt.IsZero() {
		logger.V(4).Info("Pod worker is complete but did not have terminatingAt set, likely programmer error", "podUID", podUID)
	}
	if status.terminatedAt.IsZero() {
		logger.V(4).Info("Pod worker is complete but did not have terminatedAt set, likely programmer error", "podUID", podUID)
	}
	status.finished = true
	status.working = false

	if p.startedStaticPodsByFullname[status.fullname] == podUID {
		delete(p.startedStaticPodsByFullname, status.fullname)
	}
}

// completeWork 在出错时或下一个同步间隔重新入队，然后立即执行任何待处理的工作。
func (p *podWorkers) completeWork(logger klog.Logger, podUID types.UID, phaseTransition bool, syncErr error) {
	// 若上次同步返回错误，则重新入队上次更新。
	switch {
	case phaseTransition:
		p.workQueue.Enqueue(podUID, 0)
	case syncErr == nil:
		// 无错误；按常规重同步间隔重新入队。
		p.workQueue.Enqueue(podUID, wait.Jitter(p.resyncInterval, workerResyncIntervalJitterFactor))
	case strings.Contains(syncErr.Error(), NetworkNotReadyErrorMsg):
		// 网络未就绪；短暂退避后重试，网络可能很快恢复。
		p.workQueue.Enqueue(podUID, wait.Jitter(backOffOnTransientErrorPeriod, workerBackOffPeriodJitterFactor))
	default:
		// 同步过程中出错；退避后重试。若错误带有退避到期时间，则等到该时间而非默认时长，
		// 以免在退避上额外增加时间。
		backoff := p.backOffPeriod
		if backoffAt, isBackoffErr := kubecontainer.MinBackoffExpiration(syncErr); isBackoffErr {
			backoff = backoffAt.Sub(p.clock.Now())
		}
		if backoff < 0 {
			backoff = 0
		} else if backoff > p.resyncInterval {
			backoff = p.resyncInterval
		}
		p.workQueue.Enqueue(podUID, wait.Jitter(backoff, workerBackOffPeriodJitterFactor))
	}

	// 若该 worker 有待处理更新，立即重新入队；否则清除 working 状态
	p.podLock.Lock()
	defer p.podLock.Unlock()
	if status, ok := p.podSyncStatuses[podUID]; ok {
		if status.pendingUpdate != nil {
			select {
			case p.podUpdates[podUID] <- struct{}{}:
				logger.V(4).Info("Requeuing pod due to pending update", "podUID", podUID)
			default:
				logger.V(4).Info("Pending update already queued", "podUID", podUID)
			}
		} else {
			status.working = false
		}
	}
}

// SyncKnownPods 会清除所有已完全终止且不在 desiredPods 列表中的 pod，因此它必须与针对
// 新 pod 的 UpdatePod 调用线程安全地协同。由于 pod worker 依赖 UpdatePod 被调用来驱动
// pod 状态机，若某个 pod 不在期望列表中，pod worker 必须负责投递该更新。该方法返回
// 未完成的已知 worker 的 map，其值为 SyncPodTerminated、SyncPodKill 或 SyncPodSync，
// 分别对应 pod 已终止、终止中或同步中。
func (p *podWorkers) SyncKnownPods(logger klog.Logger, desiredPods []*v1.Pod) map[types.UID]PodWorkerSync {
	workers := make(map[types.UID]PodWorkerSync)
	known := make(map[types.UID]struct{})
	for _, pod := range desiredPods {
		known[pod.UID] = struct{}{}
	}

	p.podLock.Lock()
	defer p.podLock.Unlock()

	p.podsSynced = true
	for uid, status := range p.podSyncStatuses {
		// 对于任何按 UID 仍被期望的 pod，我们保留其 worker 历史。但同步期间有两种情况
		// 需要清除历史：
		//
		// 1. pod 不再被期望（本地版本成为孤儿）
		// 2. pod 先收到 kill 更新，随后又收到 create，说明源配置中复用了该 UID
		//    （对 apiserver 极罕见，对指定固定 UID 的静态 pod 很常见）
		//
		// 前一种情况是为了限制为已删除 pod 保存的信息量；后一种情况是为了尽量减少
		// 重启静态 pod 前的等待时间。若成功移除 worker，则不会把它放入返回的已知 worker
		// map 中，SyncKnownPods 的调用方应发送新的 UpdatePod({UpdateType: Create})。
		_, knownPod := known[uid]
		orphan := !knownPod
		if status.restartRequested || orphan {
			if p.removeTerminatedWorker(logger, uid, status, orphan) {
				// 没有正在运行的 worker，不返回它
				continue
			}
		}

		sync := PodWorkerSync{
			State:  status.WorkType(),
			Orphan: orphan,
		}
		switch {
		case status.activeUpdate != nil:
			if status.activeUpdate.Pod != nil {
				sync.HasConfig = true
				sync.Static = kubetypes.IsStaticPod(status.activeUpdate.Pod)
			}
		case status.pendingUpdate != nil:
			if status.pendingUpdate.Pod != nil {
				sync.HasConfig = true
				sync.Static = kubetypes.IsStaticPod(status.pendingUpdate.Pod)
			}
		}
		workers[uid] = sync
	}
	return workers
}

// removeTerminatedWorker 清理并移除已到达“finished”终态的 worker 状态 —— 即已成功
// 退出 syncTerminatedPod。它按 UID“遗忘”一个 pod，允许以相同 UID 重建另一个 pod。
// kubelet 会保留最近终止 pod 的状态，以防意外重启终态 pod；该状态量与 pod 配置中
// 描述的 pod 数量成正比。若 worker 被完全移除，方法返回 true。
func (p *podWorkers) removeTerminatedWorker(logger klog.Logger, uid types.UID, status *podSyncStatus, orphaned bool) bool {
	if !status.finished {
		// 若 pod worker 尚未到达终态且 pod 仍被知晓，则继续等待。
		if !orphaned {
			logger.V(4).Info("Pod worker has been requested for removal but is still not fully terminated", "podUID", uid)
			return false
		}

		// 所有孤儿 pod 都被视为已删除
		status.deleted = true

		// 当 pod 不再位于期望集合中时，它被视为孤儿，pod worker 负责将其驱动到完成
		//（不能保证有其他组件会通知我们更新）。
		switch {
		case !status.IsStarted() && !status.observedRuntime:
			// pod 尚未启动，可以安全清理 —— 该变更会使 pod worker 在不执行同步的情况下关闭。
			logger.V(4).Info("Pod is orphaned and has not been started", "podUID", uid)
		case !status.IsTerminationRequested():
			// pod 已启动但尚未请求终止 —— 设置相应时间戳并通知 pod worker。由于 pod 已至少
			// 同步过一次，status.activeUpdate 将作为下次同步的兜底。
			status.terminatingAt = p.clock.Now()
			if status.activeUpdate != nil && status.activeUpdate.Pod != nil {
				status.gracePeriod, _ = calculateEffectiveGracePeriod(status, status.activeUpdate.Pod, nil)
			} else {
				status.gracePeriod = 1
			}
			p.requeueLastPodUpdate(uid, status)
			logger.V(4).Info("Pod is orphaned and still running, began terminating", "podUID", uid)
			return false
		default:
			// pod 已在走向终止，通知 pod worker。由于 pod 已至少同步过一次，
			// status.activeUpdate 将作为下次同步的兜底。
			p.requeueLastPodUpdate(uid, status)
			logger.V(4).Info("Pod is orphaned and still terminating, notified the pod worker", "podUID", uid)
			return false
		}
	}

	if status.restartRequested {
		logger.V(4).Info("Pod has been terminated but another pod with the same UID was created, remove history to allow restart", "podUID", uid)
	} else {
		logger.V(4).Info("Pod has been terminated and is no longer known to the kubelet, remove all history", "podUID", uid)
	}
	delete(p.podSyncStatuses, uid)
	p.cleanupPodUpdates(uid)

	if p.startedStaticPodsByFullname[status.fullname] == uid {
		delete(p.startedStaticPodsByFullname, status.fullname)
	}
	return true
}

// killPodNow 返回一个可用于杀死 pod 的 KillPodFunc。
// 它旨在注入到其他需要杀死 pod 的模块中。
func killPodNow(ctx context.Context, podWorkers PodWorkers, recorder record.EventRecorder) eviction.KillPodFunc {
	return func(pod *v1.Pod, isEvicted bool, gracePeriodOverride *int64, statusFn func(*v1.PodStatus)) error {
		// 确定杀死 pod 时使用的优雅期
		gracePeriod := int64(0)
		if gracePeriodOverride != nil {
			gracePeriod = *gracePeriodOverride
		} else if pod.Spec.TerminationGracePeriodSeconds != nil {
			gracePeriod = *pod.Spec.TerminationGracePeriodSeconds
		}

		// 若在合理时间内未收到回调则超时并返回错误。默认超时与优雅期相关
		//（至少 10 秒，等待 kubelet->runtime 的 sigkill 通信完成）
		timeout := gracePeriod + (gracePeriod / 2)
		minTimeout := int64(10)
		if timeout < minTimeout {
			timeout = minTimeout
		}
		timeoutDuration := time.Duration(timeout) * time.Second

		// 打开一个 channel，阻塞等待结果
		ch := make(chan struct{}, 1)
		podWorkers.UpdatePod(
			ctx,
			UpdatePodOptions{
				Pod:        pod,
				UpdateType: kubetypes.SyncPodKill,
				KillPodOptions: &KillPodOptions{
					CompletedCh:                              ch,
					Evict:                                    isEvicted,
					PodStatusFunc:                            statusFn,
					PodTerminationGracePeriodSecondsOverride: gracePeriodOverride,
				},
			})

		// 等待响应或超时
		select {
		case <-ch:
			return nil
		case <-time.After(timeoutDuration):
			recorder.Eventf(pod, v1.EventTypeWarning, events.ExceededGracePeriod, "Container runtime did not kill the pod within specified grace period.")
			return fmt.Errorf("timeout waiting to kill pod")
		}
	}
}

// cleanupPodUpdates 关闭 podUpdates channel 并从 podUpdates map 中移除，
// 使对应的 pod worker 可以停止；同时丢弃未投递的工作。调用时必须持有 pod 锁。
func (p *podWorkers) cleanupPodUpdates(uid types.UID) {
	if ch, ok := p.podUpdates[uid]; ok {
		close(ch)
	}
	delete(p.podUpdates, uid)
}

// requeueLastPodUpdate 在尚无排队更新时，从最近一次已执行的更新创建新的待处理更新，
// 然后通知 pod worker goroutine。调用时必须持有 pod 锁。
func (p *podWorkers) requeueLastPodUpdate(podUID types.UID, status *podSyncStatus) {
	// 若已有排队更新，则直接复用；若从未执行过更新，则无法重放。
	if status.pendingUpdate != nil || status.activeUpdate == nil {
		return
	}
	copied := *status.activeUpdate
	status.pendingUpdate = &copied

	// 通知 pod worker
	status.working = true
	select {
	case p.podUpdates[podUID] <- struct{}{}:
	default:
	}
}
