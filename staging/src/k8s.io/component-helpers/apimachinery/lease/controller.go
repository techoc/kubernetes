/*
Copyright 2018 The Kubernetes Authors.

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

// Package lease 提供组件租约（Lease）的创建与续租控制，供 kube-apiserver、kubelet 等
// 需要向集群声明“我仍在运行”的组件复用。
package lease

import (
	"context"
	"fmt"
	"sync"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	coordclientset "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"

	"k8s.io/klog/v2"
)

const (
	// maxUpdateRetries 是续租时允许连续立即重试的次数；用尽后才等待下一个续租周期再试，
	// 与节点状态上报的重试策略保持一致。
	// 重试次数刻意取小值：冲突和瞬时错误留到下一轮 sync 再处理，
	// 免得一次 tick 长期占用 reconcilingLock，拖慢停机和状态收敛。
	maxUpdateRetries = 5
	// maxBackoff 是退避等待的时间上限（例如 backoffEnsureLease 中使用）。
	// 给退避设上界，是为了 apiserver 持续不可用时请求间隔不会膨胀到无法接受，
	// 后续就固定按这个间隔慢慢敲。
	maxBackoff = 7 * time.Second
	// shutdownTimeout 是停机时删除租约所用的超时时间。
	// 清理路径自己定一个短超时；否则 apiserver 不可达时停机流程会被无限期挂住。
	shutdownTimeout = 10 * time.Second
)

// Controller 管理本组件（kube-apiserver、kubelet 等）租约的创建与续租。
// 同时保留 Start(stopCh) 和 Run(ctx) 两种入口，是为了让仍在用 channel 关闭语义的调用方
// 与已经迁移到 context 的调用方都能接入；两者最终落到同一个 sync 循环上。
type Controller interface {
	// Run 按续租周期反复同步租约，直到 ctx 被取消。
	Run(ctx context.Context)
	// Start 先清理可能残留的旧租约，再启动后台同步循环；stopCh 关闭时退出。
	Start(stopCh <-chan struct{}) error
	// Stop 中断同步循环并删除租约，避免集群里留下无人续租的对象。
	Stop()
}

// ProcessLeaseFunc 对传入的 Lease 对象做原地加工，返回 error 表示放弃本次租约创建。
// 采用原地修改而非返回新对象的签名，是为了让调用方在这个统一钩子里
// 既可以补 ownerReference，也可以改 spec，而控制器只需透传同一个对象；
// 返回 error 则让钩子有机会直接否决一次创建操作。
type ProcessLeaseFunc func(*coordinationv1.Lease) error

// controller 把所有续租状态放在一个结构体里，是因为 sync 由单个 goroutine 驱动，
// 状态更新天然串行；但 Stop 可能来自另一个 goroutine，所以另外用锁和 close 信号协调。
// 各字段职责：client 是完整客户端、leaseClient 是它暴露的租约子客户端（client 为 nil 时关闭租约）；
// holderIdentity 是本组件在租约中的持有者标识；leaseName/leaseNamespace 定位租约对象；
// leaseDurationSeconds 是声明的有效期；renewInterval 是续租周期；clock 提供可注入的时间源；
// onRepeatedHeartbeatFailure 在连续续租失败时回调；reconcilingLock 用于串行化 sync 与 Stop。
type controller struct {
	client                     clientset.Interface
	leaseClient                coordclientset.LeaseInterface
	holderIdentity             string
	leaseName                  string
	leaseNamespace             string
	leaseDurationSeconds       int32
	renewInterval              time.Duration
	clock                      clock.Clock
	onRepeatedHeartbeatFailure func()

	// latestLease 是控制器最近一次创建或更新的租约，命中它就能走免 GET 的快路径。
	latestLease *coordinationv1.Lease

	// newLeasePostProcessFunc 在每次创建或刷新（更新）租约前对对象做定制，例如补写 OwnerReference。
	// 注意：返回 error 会阻止本次租约创建，但不会阻止已有租约的更新。
	newLeasePostProcessFunc ProcessLeaseFunc

	reconcilingLock sync.Mutex
	// stopCh 由 Stop 关闭，使卡在租约创建重试里的 sync 放弃重试、释放上面这把锁，
	// 从而不会让 Stop 一直阻塞在锁上。
	stopCh   chan struct{}
	stopOnce sync.Once
}

// NewController 构造并返回一个租约控制器。
//
// 参数含义：clock 是可注入时钟，便于测试里固定续租时间；client 是 Kubernetes 客户端，
// 允许为 nil，此时 leaseClient 保持空值，Run/Start/Stop 都会直接返回：
// 这样调用方无需自己写条件分支，就能按配置关闭租约机制；
// holderIdentity 是本组件写入租约的持有者标识；leaseDurationSeconds 是声明的租约有效期；
// onRepeatedHeartbeatFailure 在连续心跳失败时被调用，可用于触发外部告警或自愈；
// renewInterval 是续租周期；leaseName/leaseNamespace 用于定位租约对象；
// newLeasePostProcessFunc 在每次创建/更新前加工租约对象（例如补 ownerReference），可为 nil。
func NewController(clock clock.Clock, client clientset.Interface, holderIdentity string, leaseDurationSeconds int32, onRepeatedHeartbeatFailure func(), renewInterval time.Duration, leaseName, leaseNamespace string, newLeasePostProcessFunc ProcessLeaseFunc) Controller {
	var leaseClient coordclientset.LeaseInterface
	if client != nil {
		leaseClient = client.CoordinationV1().Leases(leaseNamespace)
	}
	return &controller{
		client:                     client,
		leaseClient:                leaseClient,
		holderIdentity:             holderIdentity,
		leaseName:                  leaseName,
		leaseNamespace:             leaseNamespace,
		leaseDurationSeconds:       leaseDurationSeconds,
		renewInterval:              renewInterval,
		clock:                      clock,
		onRepeatedHeartbeatFailure: onRepeatedHeartbeatFailure,
		newLeasePostProcessFunc:    newLeasePostProcessFunc,
		stopCh:                     make(chan struct{}),
	}
}

// Start 先执行启动前的准备工作（删除旧租约），再启动租约控制器。
// 删除旧租约放在启动阶段同步完成，是为了让依赖租约发现 leader 的其他组件
// 尽早看到一个干净状态；删除失败却不该阻止本进程启动，所以只记录日志，
// 后续由 sync 循环负责重建和接管。stopCh 关闭时后台循环退出；返回值始终为 nil。
func (c *controller) Start(stopCh <-chan struct{}) error {
	if c.leaseClient == nil {
		return nil
	}

	// 启动时先删除旧租约：这样新起来的 apiserver 实例不会继承上一实例残留的租约，
	// 否则其他组件可能发现一个已经死掉的对端并继续与它通信。
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	klog.FromContext(ctx).Info("Deleting old lease on startup", "lease", klog.KRef(c.leaseNamespace, c.leaseName))
	if err := c.leaseClient.Delete(ctx, c.leaseName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		// 租约控制器最终会接管并重建租约，这里不必因为一次删除失败而中止启动。
		klog.FromContext(ctx).Error(err, "error deleting old lease", "lease", klog.KRef(c.leaseNamespace, c.leaseName))
	}

	go c.Run(wait.ContextForChannel(stopCh))
	return nil
}

// Stop 优雅地关闭控制器：通知同步循环退出，并删除本组件持有的租约。
// 这里必须持有 reconcilingLock 再做删除：否则 sync 可能在停机的最后一刻
// 又写回一个租约，留下一个没人续租的对象，误导依赖它的组件。
// stopCh 用 sync.Once 关闭，既让退避中的 sync 立刻醒来，也保证 Stop 可重复调用。
func (c *controller) Stop() {
	if c.leaseClient == nil {
		return
	}

	// 先关闭 stopCh 唤醒可能卡在 backoffEnsureLease 里的 sync，
	// 它持有下面要抢的那把锁，否则 Stop 会在锁上一直等下去。
	c.stopOnce.Do(func() { close(c.stopCh) })
	// 拿到锁后再删除，确保与 sync 之间不会竞态。
	c.reconcilingLock.Lock()
	defer c.reconcilingLock.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	klog.FromContext(ctx).Info("Cleaning up lease on exit", "lease", klog.KRef(c.leaseNamespace, c.leaseName))
	if err := c.leaseClient.Delete(ctx, c.leaseName, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		klog.FromContext(ctx).Error(err, "Unable to delete lease", "lease", klog.KRef(c.leaseNamespace, c.leaseName))
	}
}

// Run 驱动控制器的同步循环，直到 ctx 被取消。
// 续租不追求精确周期，所以给 JitterUntil 加了 0.04 的抖动：
// 多个实例启动时不会挤在同一瞬间刷新租约，apiserver 的流量也更平滑。
func (c *controller) Run(ctx context.Context) {
	if c.leaseClient == nil {
		klog.FromContext(ctx).Info("lease controller has nil lease client, will not claim or renew leases")
		return
	}
	wait.JitterUntilWithContext(ctx, c.sync, c.renewInterval, 0.04, true)
}

// sync 是单轮对账：先尝试用缓存的 latestLease 直接更新，失败再回退到查证租约是否存在。
// 这两条路径的取舍是性能与正确性的平衡：快路径省掉一次 GET，慢路径保证租约真的存在。
// 整个函数持有 reconcilingLock，因此 latestLease 的读写不需要额外的并发保护。
func (c *controller) sync(ctx context.Context) {
	// 停机时 stopCh 先于锁被检查，避免刚拿到锁就又发起一轮无意义的 API 请求。
	select {
	case <-c.stopCh:
		return
	default:
	}

	c.reconcilingLock.Lock()
	defer c.reconcilingLock.Unlock()

	if c.latestLease != nil {
		// 只要租约不会被本组件以外的其他代理修改（或极少被修改），就可以乐观地认为
		// 它自上次更新后没变，直接拿当时的版本发起更新，从而省掉 GET，减轻 etcd
		// 与 kube-apiserver 的负担。
		// 若将来其他代理也开始频繁更新这个对象，这条快路径反而会退化成额外的 GET/PUT，
		// 到那时就应该把整个 if 分支删掉。
		err := c.retryUpdateLease(ctx, c.latestLease)
		if err == nil {
			return
		}
		klog.FromContext(ctx).Info("failed to update lease using latest lease, fallback to ensure lease", "err", err)
	}

	lease, created := c.backoffEnsureLease(ctx)
	c.latestLease = lease
	// 刚创建出来的租约已经带有正确的 renewTime，没必要再发一次 UPDATE；
	// 少一次写请求，也避免创建后立即更新引入新的资源版本冲突。
	// 刚创建出来的租约无需再更新，下面只对已存在的租约执行刷新。
	if !created && lease != nil {
		if err := c.retryUpdateLease(ctx, lease); err != nil {
			klog.FromContext(ctx).Error(err, "Will retry updating lease", "interval", c.renewInterval)
		}
	}
}

// backoffEnsureLease 在租约不存在时尝试创建它，并用指数增长的等待间隔避免把 API Server 压垮。
// 返回值是租约对象，以及一个布尔值：true 表示这次调用创建了租约，false 表示租约原本就存在。
// 退避的等待同时监听 ctx 和 stopCh：调用方可能通过任一路径要求停止，
// 而此时本函数正持有 reconcilingLock，不能让 Stop 在锁上无限等待。
func (c *controller) backoffEnsureLease(ctx context.Context) (*coordinationv1.Lease, bool) {
	var (
		lease   *coordinationv1.Lease
		created bool
		err     error
	)
	sleep := 100 * time.Millisecond
	for {
		lease, created, err = c.ensureLease(ctx)
		// 只有 API 调用真的失败才退避；确保租约存在且成功是这里唯一的成功条件。
		if err == nil {
			break
		}
		// 每次翻倍但有上限，防止 apiserver 长时间不可用时重试间隔无界增长。
		sleep = minDuration(2*sleep, maxBackoff)
		klog.FromContext(ctx).Error(err, "Failed to ensure lease exists, will retry", "interval", sleep)
		// 退避等待期间，一旦 ctx 被取消或 Stop 被调用就立刻提前返回。
		select {
		case <-ctx.Done():
			return nil, false
		case <-c.stopCh:
			return nil, false
		case <-time.After(sleep):
		}
	}
	return lease, created
}

// ensureLease 确保租约存在：不存在则创建，已存在则直接返回。
// 返回租约对象、一个布尔值（true 表示这次调用创建了租约）以及可能发生的错误。
func (c *controller) ensureLease(ctx context.Context) (*coordinationv1.Lease, bool, error) {
	lease, err := c.leaseClient.Get(ctx, c.leaseName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		// 构造租约本身失败（通常来自 newLeasePostProcessFunc）时按“没出错”返回，
		// 让这轮直接跳过创建、下一轮再试：构造失败常常是因为依赖信息暂时不可得，
		// 例如节点 UID 还没查到，此时与其报错刷屏，不如等下一次续租再补齐。
		// 租约不存在，准备创建。
		leaseToCreate, err := c.newLease(nil)
		// 构造新租约出错（通常来自 newLeasePostProcessFunc）：既然无法把租约设成期望状态，
		// 这次就干脆不创建，把问题留到下一轮重试，而不是带着半成品写入集群。
		if err != nil {
			return nil, false, nil
		}
		lease, err := c.leaseClient.Create(ctx, leaseToCreate, metav1.CreateOptions{})
		if err != nil {
			return nil, false, err
		}
		return lease, true, nil
	} else if err != nil {
		// 查询租约时遇到非 NotFound 的错误，交给上层退避重试。
		return nil, false, err
	}
	// 租约已经存在，直接返回查到的对象。
	return lease, false, nil
}

// retryUpdateLease 在确认租约已创建后，最多尝试 maxUpdateRetries 次更新。
// 重试用固定次数而非无限循环，是为了让一次 sync 有确定的上界；
// 超过上限就交回主循环，由下一次 tick 重新走完整流程。
// 返回 nil 表示更新成功，否则返回累计失败的错误信息。
func (c *controller) retryUpdateLease(ctx context.Context, base *coordinationv1.Lease) error {
	for i := 0; i < maxUpdateRetries; i++ {
		// 每轮都基于 base 重新构造对象，避免拿到上一轮失败 UPDATE 留下的脏状态。
		leaseToUpdate, err := c.newLease(base)
		if err != nil {
			klog.FromContext(ctx).Error(err, "Failed to prepare lease")
		} else {
			lease, err := c.leaseClient.Update(ctx, leaseToUpdate, metav1.UpdateOptions{})
			if err == nil {
				c.latestLease = lease
				return nil
			}
			klog.FromContext(ctx).Error(err, "Failed to update lease")
			// 乐观锁冲突（OptimisticLockError）意味着缓存的 resourceVersion 已过期，
			// 必须重新 GET 拿到最新版本才能继续；
			// 读失败也继续下一轮，最终仍会返回明确错误，交给 sync 回退处理。
			if apierrors.IsConflict(err) {
				base, _ = c.backoffEnsureLease(ctx)
				continue
			}
		}
		// 第一次失败很可能是正常的短暂抖动，只有连续失败才值得通知调用方，
		// 避免心跳偶尔一次失败就触发外部动作。
		if i > 0 && c.onRepeatedHeartbeatFailure != nil {
			c.onRepeatedHeartbeatFailure()
		}
	}
	return fmt.Errorf("failed %d attempts to update lease", maxUpdateRetries)
}

// newLease 在 base 为 nil 时构造一个全新租约，否则返回 base 的副本并在副本上写入期望状态。
// 注意：返回 error 会阻断本次 CREATE，使其留到下一轮重试；但不会阻断已有租约的刷新（UPDATE）。
func (c *controller) newLease(base *coordinationv1.Lease) (*coordinationv1.Lease, error) {
	// 只使用最小字段集：Lease 里还有其他用于调试或兼容历史行为的字段，
	// 但组件心跳没必要因为把它们也填进来而变复杂。
	var lease *coordinationv1.Lease
	if base == nil {
		lease = &coordinationv1.Lease{
			ObjectMeta: metav1.ObjectMeta{
				Name:      c.leaseName,
				Namespace: c.leaseNamespace,
			},
			Spec: coordinationv1.LeaseSpec{
				HolderIdentity: ptr.To(c.holderIdentity),
			},
		}
	} else {
		// 必须在副本上改状态，不能直接改 base：冲突重试时 base 还作为下一次构造的基准，
		// 就地修改会把失败请求写过的字段带进下一轮。
		lease = base.DeepCopy()
	}
	// 租约时长是可变配置，控制器重启或配置变更后要借每次续租把它同步到最新值。
	lease.Spec.LeaseDurationSeconds = ptr.To(c.leaseDurationSeconds)
	lease.Spec.RenewTime = &metav1.MicroTime{Time: c.clock.Now()}

	// 钩子放在最后执行，让它看到完整的期望状态；这样补写 ownerReference 这类信息时
	// 不必关心前面设置了哪些字段。钩子的错误会造成新建失败，但更新路径仍会继续提交，
	// 因为已有租约缺一点附加信息，比停止心跳的危害小。
	if c.newLeasePostProcessFunc != nil {
		err := c.newLeasePostProcessFunc(lease)
		return lease, err
	}

	return lease, nil
}

// minDuration 返回 a、b 中较小的一个，用于给退避间隔封顶。
func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
