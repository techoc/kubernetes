/*
Copyright 2015 The Kubernetes Authors.

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

// Package leaderelection 实现一组端点的领导者选举。
// 它利用 endpoints 对象上的注解存储选举状态记录。该实现不保证只有一个
// 客户端在以 leader 身份行动（即不提供 fencing）。
//
// 客户端只依据本地捕获的时间戳推断选举状态。客户端不认为选举记录中的
// 时间戳是准确的，因为这些时间戳可能并非由本地时钟产生。实现不依赖其
// 精确性，只用它们的变化来表明其他客户端已续约 leader 租约。因此该实现
// 可以容忍任意大小的时钟偏移，但不能容忍任意的时钟偏移速率。
//
// 不过，通过合理设置 RenewDeadline 和 LeaseDuration，可以配置对偏移速率
// 的容忍程度。以“最快节点过去的时间与最慢节点过去的时间之比”表示的最大
// 可容忍比例，可以通过把 LeaseDuration 与 RenewDeadline 设为相同比值的
// 配置近似达成。例如，若用户希望容忍某些节点的时间前进速度是其他节点的两倍，
// 可以把 LeaseDuration 设为 60 秒、RenewDeadline 设为 30 秒。
//
// 虽然并非必需，但强烈建议在集群节点间采用某种时钟同步方法。配置该客户端时
// 需要牢记：对偏移速率的容忍度与 master 可用性成反比。
//
// 较大的集群通常对 API 延迟有较宽松的 SLA，配置客户端时应予以考虑。应监控
// leader 切换频率，并逐步增大 RetryPeriod 和 LeaseDuration，直到切换频率稳定
// 且低到可接受。配置该客户端时同样需要牢记：对 API 延迟的容忍度与 master
// 可用性成反比。
//
// 免责声明：这是 alpha API。该库在后续版本中很可能发生重大变化甚至被完全
// 移除。依赖此 API 的风险自负。
//
// 该实现只保证"最多一个客户端认为自己持有租约"的尽力语义，不做 fencing；
// 失去租约的旧 leader 可能仍在短暂执行，受保护的临界区需要自行保证幂等。
package leaderelection

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	rl "k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

const (
	// JitterFactor 给重试间隔加抖动，避免所有副本同时争抢同一个租约。
	JitterFactor = 1.2
)

// NewLeaderElector 根据 LeaderElectionConfig 创建 LeaderElector
func NewLeaderElector(lec LeaderElectionConfig) (*LeaderElector, error) {
	// 时间参数之间必须留有余量，否则网络延迟或时钟抖动会导致频繁丢失租约。
	if lec.LeaseDuration <= lec.RenewDeadline {
		return nil, fmt.Errorf("leaseDuration must be greater than renewDeadline")
	}
	if lec.RenewDeadline <= time.Duration(JitterFactor*float64(lec.RetryPeriod)) {
		return nil, fmt.Errorf("renewDeadline must be greater than retryPeriod*JitterFactor")
	}
	if lec.LeaseDuration < 1 {
		return nil, fmt.Errorf("leaseDuration must be greater than zero")
	}
	if lec.RenewDeadline < 1 {
		return nil, fmt.Errorf("renewDeadline must be greater than zero")
	}
	if lec.RetryPeriod < 1 {
		return nil, fmt.Errorf("retryPeriod must be greater than zero")
	}
	if lec.Callbacks.OnStartedLeading == nil {
		return nil, fmt.Errorf("OnStartedLeading callback must not be nil")
	}
	if lec.Callbacks.OnStoppedLeading == nil {
		return nil, fmt.Errorf("OnStoppedLeading callback must not be nil")
	}

	if lec.Lock == nil {
		return nil, fmt.Errorf("Lock must not be nil.")
	}
	// identity 用于区分参与者；空值会让 IsLeader 永远无法正确判断。
	id := lec.Lock.Identity()
	if id == "" {
		return nil, fmt.Errorf("Lock identity is empty")
	}

	le := LeaderElector{
		config:  lec,
		clock:   clock.RealClock{},
		metrics: globalMetricsFactory.newLeaderMetrics(),
	}
	// 初始化指标为"非 leader"状态，避免进程启动时误报。
	le.metrics.leaderOff(le.config.Name)
	return &le, nil
}

// LeaderElectionConfig 是选举的全部配置。
type LeaderElectionConfig struct {
	// Lock 是用于加锁的资源
	Lock rl.Interface

	// LeaseDuration 是非 leader 候选者等待强制获取领导权的时长，
	// 以最近一次观测到确认（ack）的时间为基准衡量。
	//
	// 客户端必须先完整等待一个 LeaseDuration 且期间未观察到记录变化，
	// 才能尝试接管。当所有客户端都关闭、又用不同名字启动一批新客户端
	// 面对同一个 leader 记录时，它们必须等满整个 LeaseDuration 才能尝试
	// 获取租约。因此 LeaseDuration 应在对时钟偏移速率的容忍范围内尽量短，
	// 以避免上述场景中出现可能的长时间等待。
	//
	// 核心组件默认该值为 15 秒。
	LeaseDuration time.Duration
	// RenewDeadline 是现任 master 在放弃之前重试刷新领导权的时长。
	//
	// 核心组件默认该值为 10 秒。
	RenewDeadline time.Duration
	// RetryPeriod 是 LeaderElector 客户端在两次尝试之间应等待的时长。
	//
	// 核心组件默认该值为 2 秒。
	RetryPeriod time.Duration

	// Callbacks 是在 LeaderElector 的某些生命周期事件中被触发的回调
	Callbacks LeaderCallbacks

	// WatchDog 是关联的健康检查器。
	// 不需要/未配置时 WatchDog 可以为 nil。
	WatchDog *HealthzAdaptor

	// 若希望在 run context 取消时释放锁，应把 ReleaseOnCancel 设为 true。
	// 设为 true 时，必须确保取消 context 前所有受该租约保护的代码已成功完成，
	// 否则可能出现两个进程同时操作关键路径。
	ReleaseOnCancel bool

	// Name 是资源锁的名字，用于调试
	Name string

	// Coordinated 为 true 时启用协调式领导者选举（Coordinated Leader Election）功能。
	// 警告：协调式领导者选举目前是 ALPHA 特性。
	//
	// 协调式选举会参考 PreferredHolder/Strategy，支持优雅交接；当前仍是 alpha。
	Coordinated bool
}

// LeaderCallbacks 是在 LeaderElector 的某些生命周期事件中被触发的回调，
// 均以异步方式调用。
//
// 未来可能增加的回调：
//   - OnChallenge()
//
// 回调均在独立 goroutine 中执行，不能被阻塞太久；OnStoppedLeading 总会被调用。
type LeaderCallbacks struct {
	// OnStartedLeading 在 LeaderElector 客户端开始担任 leader 时被调用
	OnStartedLeading func(context.Context)
	// OnStoppedLeading 在 LeaderElector 客户端停止担任 leader 时被调用。
	// LeaderElector 退出时总会调用该回调，即使它从未开始领导。
	// 使用者不应假设 OnStoppedLeading 只在 OnStartedLeading 之后被调用。
	// 参见：https://github.com/kubernetes/kubernetes/pull/127675#discussion_r1780059887
	OnStoppedLeading func()
	// OnNewLeader 在客户端观察到一个与之前不同的 leader 时被调用，
	// 包括客户端启动后第一次观察到的 leader。
	OnNewLeader func(identity string)
}

// LeaderElector 是领导者选举客户端。
// 除 config 外，运行期状态都由 observedRecordLock 保护。
type LeaderElector struct {
	config LeaderElectionConfig
	// 内部记账字段
	// observedRecord 是最近一次从 API 观察到（或自己写入）的租约记录；
	// observedTime 是观察到它的本地时间，用于判断租约是否过期。
	observedRecord    rl.LeaderElectionRecord
	observedRawRecord []byte
	observedTime      time.Time
	// 用于实现 OnNewLeader()；若领导权变更尚未上报，
	// 该值可能略微滞后于 observedRecord.HolderIdentity。
	reportedLeader string

	// clock 是对时间的封装，便于进行更稳定的测试
	clock clock.Clock

	// 用于保护 observedRecord 和 observedTime
	observedRecordLock sync.RWMutex

	metrics leaderMetricsAdapter
}

// Run 启动领导者选举循环。在选举循环被 ctx 停止、或已不再持有
// leader 租约之前，Run 不会返回。
func (le *LeaderElector) Run(ctx context.Context) {
	// OnStoppedLeading 必须在任何退出路径执行，包括未成功竞选就收到取消。
	defer runtime.HandleCrashWithContext(ctx)
	defer le.config.Callbacks.OnStoppedLeading()

	if !le.acquire(ctx) {
		return // ctx 已发出结束信号
	}
	// 为 OnStartedLeading 派生可取消的 ctx；失去租约时通过 cancel 通知业务停止。
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go le.config.Callbacks.OnStartedLeading(ctx)
	le.renew(ctx)
}

// RunOrDie 用给定配置启动客户端，配置校验失败则 panic。
// RunOrDie 会阻塞，直到选举循环被 ctx 停止或不再持有 leader 租约。
func RunOrDie(ctx context.Context, lec LeaderElectionConfig) {
	le, err := NewLeaderElector(lec)
	if err != nil {
		panic(err)
	}
	// WatchDog 用于健康检查；必须在 Run 之前关联，才能反映后续状态。
	if lec.WatchDog != nil {
		lec.WatchDog.SetLeaderElection(le)
	}
	le.Run(ctx)
}

// GetLeader 返回最近一次观察到的 leader 的 identity；尚未观察到任何 leader
// 时返回空字符串。
// 该函数仅用于提供信息（如监控、日志等）。
func (le *LeaderElector) GetLeader() string {
	return le.getObservedRecord().HolderIdentity
}

// IsLeader 在最近观察到的 leader 是本客户端时返回 true，否则返回 false。
func (le *LeaderElector) IsLeader() bool {
	return le.getObservedRecord().HolderIdentity == le.config.Lock.Identity()
}

// acquire 循环调用 tryAcquireOrRenew，一旦成功立即返回 true；
// ctx 发出结束信号时返回 false。
func (le *LeaderElector) acquire(ctx context.Context) bool {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	succeeded := false
	desc := le.config.Lock.Describe()
	logger := klog.FromContext(ctx)
	logger.Info("Attempting to acquire leader lease...", "lock", desc)
	// JitterUntil 持续重试直至成功；成功后 cancel 让循环退出。
	wait.JitterUntilWithContext(ctx, func(ctx context.Context) {
		if !le.config.Coordinated {
			succeeded = le.tryAcquireOrRenew(ctx)
		} else {
			succeeded = le.tryCoordinatedRenew(ctx)
		}
		le.maybeReportTransition()
		if !succeeded {
			logger.V(4).Info("Failed to acquire lease", "lock", desc)
			return
		}
		le.config.Lock.RecordEvent("became leader")
		le.metrics.leaderOn(le.config.Name)
		logger.Info("Successfully acquired lease", "lock", desc)
		cancel()
	}, le.config.RetryPeriod, JitterFactor, true)
	return succeeded
}

// renew 循环调用 tryAcquireOrRenew，在调用失败或 ctx 发出结束信号时立即返回。
func (le *LeaderElector) renew(ctx context.Context) {
	// 无论续约成功还是失败，退出前都记录"stopped leading"事件。
	defer le.config.Lock.RecordEvent("stopped leading")
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	logger := klog.FromContext(ctx)
	// 每个 RetryPeriod 发起一轮续约，单轮在 RenewDeadline 内重试；超时即认为失去租约。
	wait.UntilWithContext(ctx, func(ctx context.Context) {
		err := wait.PollUntilContextTimeout(ctx, le.config.RetryPeriod, le.config.RenewDeadline, true, func(ctx context.Context) (done bool, err error) {
			// immediate=true 时，即使 context 已取消，PollUntilContextTimeout 仍会调用 condition。
			// 这里短路处理，避免不必要的处理和错误日志。
			if err := ctx.Err(); err != nil {
				return false, err
			}
			if !le.config.Coordinated {
				return le.tryAcquireOrRenew(ctx), nil
			} else {
				return le.tryCoordinatedRenew(ctx), nil
			}
		})
		le.maybeReportTransition()
		desc := le.config.Lock.Describe()
		if err == nil {
			logger.V(5).Info("Successfully renewed lease", "lock", desc)
			return
		}
		le.metrics.leaderOff(le.config.Name)
		logger.Info("Failed to renew lease", "lock", desc, "err", err)
		cancel()
	}, le.config.RetryPeriod)

	// 如果仍持有租约，就主动释放
	if le.config.ReleaseOnCancel {
		le.release(logger)
	}
}

// release 尝试在已获取 leader 租约时释放它。
// 发生冲突时会重试：context 取消与进行中的 renew() 操作竞争时可能出现冲突。
// apiserver 完成更新并递增 resource version 时，客户端会看到 ctx 取消错误。
func (le *LeaderElector) release(logger klog.Logger) bool {
	// 这里刻意使用 Background 而非已取消的 ctx：释放锁需要在原 ctx 取消后继续完成。
	ctx := klog.NewContext(context.Background(), logger)
	timeoutCtx, timeoutCancel := context.WithTimeout(ctx, le.config.RenewDeadline)
	defer timeoutCancel()
	return le.tryRelease(timeoutCtx)
}

// tryRelease 读取当前锁并在自己仍是 leader 时写入一个已过期的最小记录。
func (le *LeaderElector) tryRelease(ctx context.Context) bool {
	logger := klog.FromContext(ctx)
	oldLeaderElectionRecord, _, err := le.config.Lock.Get(ctx)
	if err != nil {
		if !errors.IsNotFound(err) {
			logger.Error(err, "error retrieving resource lock", "lock", le.config.Lock.Describe())
			return false
		}
		logger.Info("lease lock not found", "lock", le.config.Lock.Describe())
		return false
	}

	if !le.IsLeader() {
		return true
	}
	now := metav1.NewTime(le.clock.Now())
	// 写入 LeaseDurationSeconds=1 的空持有者记录，让其他候选者尽快接管。
	leaderElectionRecord := rl.LeaderElectionRecord{
		LeaderTransitions:    oldLeaderElectionRecord.LeaderTransitions,
		LeaseDurationSeconds: 1,
		RenewTime:            now,
		AcquireTime:          now,
	}
	if err := le.config.Lock.Update(ctx, leaderElectionRecord); err != nil {
		if errors.IsConflict(err) {
			// 冲突说明有并发的 update 抢先提交；重新读取后再试。
			logger.V(4).Info("Conflict when releasing lease, retrying", "lock", le.config.Lock.Describe())
			return le.tryRelease(ctx)
		}
		logger.Error(err, "Failed to release lease", "lock", le.config.Lock.Describe())
		return false
	}

	le.setObservedRecord(&leaderElectionRecord)
	return true
}

// tryCoordinatedRenew 检查是否已获取租约，若已获取则尝试续租。
// 成功返回 true，否则返回 false。
func (le *LeaderElector) tryCoordinatedRenew(ctx context.Context) bool {
	logger := klog.FromContext(ctx)
	now := metav1.NewTime(le.clock.Now())
	leaderElectionRecord := rl.LeaderElectionRecord{
		HolderIdentity:       le.config.Lock.Identity(),
		LeaseDurationSeconds: int(le.config.LeaseDuration / time.Second),
		RenewTime:            now,
		AcquireTime:          now,
	}

	// 1. 获取 electionRecord
	oldLeaderElectionRecord, oldLeaderElectionRawRecord, err := le.config.Lock.Get(ctx)
	if err != nil {
		if !errors.IsNotFound(err) {
			logger.Error(err, "Error retrieving lease lock", "lock", le.config.Lock.Describe())
			return false
		}
		logger.Info("Lease lock not found", "lock", le.config.Lock.Describe(), "err", err)
		return false
	}

	// 2. 已获取记录，检查 Identity 与时间
	if !bytes.Equal(le.observedRawRecord, oldLeaderElectionRawRecord) {
		le.setObservedRecord(oldLeaderElectionRecord)

		le.observedRawRecord = oldLeaderElectionRawRecord
	}

	le.observedRecordLock.RLock()
	obsTime := le.observedTime
	le.observedRecordLock.RUnlock()

	hasExpired := obsTime.Add(time.Second * time.Duration(oldLeaderElectionRecord.LeaseDurationSeconds)).Before(now.Time)
	if hasExpired {
		logger.Info("Lease has expired", "lock", le.config.Lock.Describe())
		return false
	}

	if !le.IsLeader() {
		logger.V(6).Info("Lease is held and has not yet expired", "lock", le.config.Lock.Describe(), "holder", oldLeaderElectionRecord.HolderIdentity)
		return false
	}

	// 2b. 若租约已被标记为 "end of term"，不再续约
	if le.IsLeader() && oldLeaderElectionRecord.PreferredHolder != "" {
		logger.V(4).Info("Lease is marked as 'end of term'", "lock", le.config.Lock.Describe())
		// TODO: 持有者可以直接删除租约，而不是等待其过期。
		// 这并不兼容所有控制器，因此需要作为可选行为。
		// 释放前必须确保所有受该租约保护的代码已成功完成，
		// 否则可能出现两个进程同时操作关键路径。
		// 通常这里返回 false 后，进程就会终止。
		// 参见：OnStoppedLeading
		return false
	}

	// 3. 准备尝试更新。此时 leaderElectionRecord 仍是默认值，
	// 更新前先修正它。
	if le.IsLeader() {
		leaderElectionRecord.AcquireTime = oldLeaderElectionRecord.AcquireTime
		leaderElectionRecord.LeaderTransitions = oldLeaderElectionRecord.LeaderTransitions
		leaderElectionRecord.Strategy = oldLeaderElectionRecord.Strategy
		le.metrics.slowpathExercised(le.config.Name)
	} else {
		leaderElectionRecord.LeaderTransitions = oldLeaderElectionRecord.LeaderTransitions + 1
	}

	// 更新锁本身
	if err = le.config.Lock.Update(ctx, leaderElectionRecord); err != nil {
		logger.Error(err, "Failed to update lock", "lock", le.config.Lock.Describe())
		return false
	}

	le.setObservedRecord(&leaderElectionRecord)
	return true
}

// tryAcquireOrRenew 在未获取租约时尝试获取，已获取时尝试续租。
// 成功返回 true，否则返回 false。
//
// 先走乐观快路径（直接 Update），失败再回退到读取-判断-更新的慢路径。
func (le *LeaderElector) tryAcquireOrRenew(ctx context.Context) bool {
	logger := klog.FromContext(ctx)
	now := metav1.NewTime(le.clock.Now())
	leaderElectionRecord := rl.LeaderElectionRecord{
		HolderIdentity:       le.config.Lock.Identity(),
		LeaseDurationSeconds: int(le.config.LeaseDuration / time.Second),
		RenewTime:            now,
		AcquireTime:          now,
	}

	// 1. leader 的快路径：乐观假设上次观察到的记录就是当前版本，直接更新。
	if le.IsLeader() && le.isLeaseValid(now.Time) {
		oldObservedRecord := le.getObservedRecord()
		leaderElectionRecord.AcquireTime = oldObservedRecord.AcquireTime
		leaderElectionRecord.LeaderTransitions = oldObservedRecord.LeaderTransitions

		err := le.config.Lock.Update(ctx, leaderElectionRecord)
		if err == nil {
			le.setObservedRecord(&leaderElectionRecord)
			return true
		}
		logger.V(2).Info("Failed to update lease optimistically, falling back to slow path", "lock", le.config.Lock.Describe(), "err", err)
	}

	// 2. 获取或创建 ElectionRecord
	oldLeaderElectionRecord, oldLeaderElectionRawRecord, err := le.config.Lock.Get(ctx)
	if err != nil {
		if !errors.IsNotFound(err) {
			logger.Error(err, "Error retrieving lease lock", "lock", le.config.Lock.Describe())
			return false
		}
		if err = le.config.Lock.Create(ctx, leaderElectionRecord); err != nil {
			logger.Error(err, "Error initially creating lease lock", "lock", le.config.Lock.Describe())
			return false
		}

		le.setObservedRecord(&leaderElectionRecord)

		return true
	}

	// 3. 已获取记录，检查 Identity 与时间
	if !bytes.Equal(le.observedRawRecord, oldLeaderElectionRawRecord) {
		le.setObservedRecord(oldLeaderElectionRecord)

		le.observedRawRecord = oldLeaderElectionRawRecord
	}
	if len(oldLeaderElectionRecord.HolderIdentity) > 0 && le.isLeaseValid(now.Time) && !le.IsLeader() {
		logger.V(4).Info("Lease is held by and has not yet expired", "lock", le.config.Lock.Describe(), "holder", oldLeaderElectionRecord.HolderIdentity)
		return false
	}

	// 4. 准备尝试更新。此时 leaderElectionRecord 仍是默认值，
	// 更新前先修正它。
	if le.IsLeader() {
		leaderElectionRecord.AcquireTime = oldLeaderElectionRecord.AcquireTime
		leaderElectionRecord.LeaderTransitions = oldLeaderElectionRecord.LeaderTransitions
		le.metrics.slowpathExercised(le.config.Name)
	} else {
		leaderElectionRecord.LeaderTransitions = oldLeaderElectionRecord.LeaderTransitions + 1
	}

	// 更新锁本身
	if err = le.config.Lock.Update(ctx, leaderElectionRecord); err != nil {
		logger.Error(err, "Failed to update lease", "lock", le.config.Lock.Describe())
		return false
	}

	le.setObservedRecord(&leaderElectionRecord)
	return true
}

func (le *LeaderElector) maybeReportTransition() {
	// reportedLeader 可能滞后于 observedRecord，这里只在真正变化时回调一次。
	if le.observedRecord.HolderIdentity == le.reportedLeader {
		return
	}
	le.reportedLeader = le.observedRecord.HolderIdentity
	if le.config.Callbacks.OnNewLeader != nil {
		go le.config.Callbacks.OnNewLeader(le.reportedLeader)
	}
}

// Check 判断当前租约是否已过期超过 timeout 的时间。
// 作为健康检查使用：若已超过 leaseDuration+容忍时间仍认为自己是 leader，
// 说明续约循环可能卡死，返回错误让上层重启进程。
func (le *LeaderElector) Check(maxTolerableExpiredLease time.Duration) error {
	if !le.IsLeader() {
		// 当前不关心本机作为热备的情况
		return nil
	}
	// 若距离租约期限的时长已超过续约超时容忍值，就该开始上报自身不健康。
	// 按理进程应该已经退出，但死锁等情况可能阻止这一点（见 #70819）。
	le.observedRecordLock.RLock()
	lastObservation := le.observedTime
	leaseDuration := le.config.LeaseDuration
	le.observedRecordLock.RUnlock()

	if le.clock.Since(lastObservation) > leaseDuration+maxTolerableExpiredLease {
		return fmt.Errorf("failed election to renew leadership on lease %s", le.config.Name)
	}

	return nil
}

// isLeaseValid 判断本地观察到的租约是否仍在有效期内。
// 只用本地时钟，避免依赖其他节点时钟的准确性。
func (le *LeaderElector) isLeaseValid(now time.Time) bool {
	// 加锁以安全地同时读取时间和记录
	le.observedRecordLock.RLock()
	defer le.observedRecordLock.RUnlock()

	return le.observedTime.Add(time.Second * time.Duration(le.observedRecord.LeaseDurationSeconds)).After(now)
}

// setObservedRecord 设置新的 observedRecord，并把 observedTime 更新为当前时间。
// 调用方需用锁保护临界区。
func (le *LeaderElector) setObservedRecord(observedRecord *rl.LeaderElectionRecord) {
	le.observedRecordLock.Lock()
	defer le.observedRecordLock.Unlock()

	le.observedRecord = *observedRecord
	le.observedTime = le.clock.Now()
}

// getObservedRecord 返回 observedRecord。
// 调用方需用锁保护临界区。
func (le *LeaderElector) getObservedRecord() rl.LeaderElectionRecord {
	le.observedRecordLock.RLock()
	defer le.observedRecordLock.RUnlock()

	return le.observedRecord
}
