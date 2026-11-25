/*
Copyright 2019 The Kubernetes Authors.

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

package flowcontrol

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/diff"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	fcboot "k8s.io/apiserver/pkg/apis/flowcontrol/bootstrap"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/util/apihelpers"
	fq "k8s.io/apiserver/pkg/util/flowcontrol/fairqueuing"
	fcfmt "k8s.io/apiserver/pkg/util/flowcontrol/format"
	"k8s.io/apiserver/pkg/util/flowcontrol/metrics"
	fcrequest "k8s.io/apiserver/pkg/util/flowcontrol/request"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"

	flowcontrol "k8s.io/api/flowcontrol/v1"
	flowcontrolapplyconfiguration "k8s.io/client-go/applyconfigurations/flowcontrol/v1"
	flowcontrolclient "k8s.io/client-go/kubernetes/typed/flowcontrol/v1"
	flowcontrollister "k8s.io/client-go/listers/flowcontrol/v1"
)

// timeFmt 是日志中时间戳的格式，精确到毫秒即可，避免日志行过长。
const timeFmt = "2006-01-02T15:04:05.999"

const (
	// priorityLevelMaxSeatsPercent 是 nominalCL 的百分比，用作可从 work
	// estimator 分配的最大席位数，限制单个优先级级别可申请的席位占比，
	// 从而限制宽请求对全局并发容量的占用，避免小请求被饿死。
	priorityLevelMaxSeatsPercent = float64(0.15)
)

// 本文件包含一个简单的、本地（即 apiserver 进程内）的控制器，
// 它把 API Priority and Fairness 的配置对象（FlowSchema 和
// PriorityLevelConfiguration）消化为 filter 所使用的数据结构。
// 在开发的第一个阶段，该控制器采用了最简单的做法：只要收到
// 任何配置对象发生变化的通知，或者当某个不再需要的优先级级别
// 变得完全无人使用时，就把所有配置对象整体读取并处理一遍。

const (
	// 优先级级别之间的席位借用通过周期性调整当前并发限制
	// （CurrentCLs）来实现；borrowingAdjustmentPeriod 就是这个周期。
	borrowingAdjustmentPeriod = 10 * time.Second

	// 席位借用的输入是经过平滑的席位需求数据。
	// 该常量控制平滑过程的衰减率，
	// 具体说明见 `priorityLevelState` 的 `seatDemandStats` 字段上的注释。
	// 此处出现的具体数值具有这样的性质：该衰减的半衰期
	// 为 5 分钟。
	// 这只是对合理取值的初步猜测，等我们对借用机制
	// 积累一些经验后很可能会被调整。
	seatDemandSmoothingCoefficient = 0.977
)

// 本包中的函数遵循这样的命名约定：后缀
// "Locked" 表示相关互斥锁必须在调用开始时处于锁定状态，
// 并在返回时保持锁定。对于 configController，
// 后缀 "ReadLocked" 约定为读锁，而仅 "Locked"
// 约定为完全锁。两种后缀都不带，意味着要么
// (a) 调用时不得持有锁、返回时也不持有，要么 (b) 加锁与否
// 无关紧要。

// StartFunction 开始处理一个请求。如果该请求被入队，
// 此函数会把给定的 hashValue 用作熵源，通过 shuffle-sharding
// 把请求打散分配到某个队列中。descr1 和 descr2 取值不参与任何
// 逻辑，只出现在日志消息里。在本请求的排队（如果有的话）
// 结束之前，该方法不会返回。如果 `execute` 为 false，
// 那么 `afterExecution` 无关紧要，且该请求应当被
// 拒绝。否则该请求应当被执行，且
// `afterExecution` 必须被恰好调用一次。
type StartFunction func(ctx context.Context, hashValue uint64) (execute bool, afterExecution func())

// RequestDigest 保存流控所需的请求必要信息
type RequestDigest struct {
	RequestInfo *request.RequestInfo
	User        user.Info
}

// `*configController` 与那些配置 API Priority and Fairness 的 API
// 对象保持最终一致，并为配置好的行为提供过程式接口。
// 本类型以及 cfgMeal 的方法遵循这样的约定：后缀
// "Locked" 表示调用方必须持有 configController 锁。
type configController struct {
	name              string // 在「控制器互相打架」的测试中取值不同
	clock             clock.PassiveClock
	queueSetFactory   fq.QueueSetFactory
	reqsGaugeVec      metrics.RatioedGaugeVec
	execSeatsGaugeVec metrics.RatioedGaugeVec

	// 本控制器在 ObjectMeta ManagedFieldsEntry.Manager 中的呈现名称
	asFieldManager string

	// 给定一个布尔值，表示 FlowSchema 所引用的
	// PriorityLevelConfig 是否存在，返回一个布尔值，表示
	// 该引用是否为悬空引用
	foundToDangling func(bool) bool

	// 当配置对象需要被重新处理时，configQueue 中持有
	// `(interface{})(0)`。
	configQueue workqueue.TypedRateLimitingInterface[int]

	plLister         flowcontrollister.PriorityLevelConfigurationLister
	plInformerSynced cache.InformerSynced

	fsLister         flowcontrollister.FlowSchemaLister
	fsInformerSynced cache.InformerSynced

	flowcontrolClient flowcontrolclient.FlowcontrolV1Interface

	// serverConcurrencyLimit 是对服务器同时正在处理的
	// 非豁免请求总数的限制。该值来自
	// 服务器配置。
	serverConcurrencyLimit int

	// watchTracker 实现了所需的 WatchTracker 接口。
	WatchTracker

	// MaxSeatsTracker 跟踪给定优先级级别本可从 work estimator
	// 分配的最大席位数。本控制器并不强制执行该 tracker 中
	// 存储的最大席位数限制，最大席位数的上下限由 work estimator
	// 自行设定（目前 min=1，max=10）。
	MaxSeatsTracker

	// 最近的更新尝试，按新旧程度递增排序（越靠前越新）。
	// Consumer 会裁剪它以只保留最近一分钟内的条目。
	// 控制器用它把自己限制为：在任意一分钟内
	// 对同一个 FlowSchema 至多更新六次。
	// 该字段只能从那唯一一个 worker goroutine 访问。
	mostRecentUpdates []updateAttempt

	// 访问其后的字段时必须锁定本锁。
	// 对以下任何一项进行写入都需要
	// 持有写锁：
	// - flowSchemas 字段
	// - flowSchemas 字段所持有的切片
	// - priorityLevelStates 字段
	// - priorityLevelStates 字段所持有的映射
	// - 该映射中持有的 priorityLevelState 的任何字段
	lock sync.RWMutex

	// flowSchemas 持有 flow schema 对象，按数值递增（即逻辑上
	// 递减）的匹配优先级排序。本切片中的每个
	// FlowSchema 都是不可变的。
	flowSchemas apihelpers.FlowSchemaSequence

	// priorityLevelStates 将 PriorityLevelConfiguration 对象名
	// 映射到该级别的状态。`flowSchemas` 中每个成员所引用的
	// 名称都在这里有条目。
	priorityLevelStates map[string]*priorityLevelState

	// nominalCLSum 是各 priorityLevelState 记录中 nominalCL 字段之和。
	// 由于计算 nominalCL 值时刻意向上取整，
	// 该和可能超过 serverConcurrencyLimit。
	// 之所以跟踪它，是因为它是分配调整算法的输入之一。
	nominalCLSum int
}

// updateAttempt 记录一次 FlowSchema status 更新尝试，用于限速：
// 同一对象每分钟最多更新 6 次，防止与其它 apiserver 互相打架导致写风暴。
type updateAttempt struct {
	timeUpdated  time.Time
	updatedItems sets.String // FlowSchema 名称
}

// priorityLevelState 保存某个优先级级别专有的状态。
type priorityLevelState struct {
	// 规定该级别的 API 对象或原型。经由该指针
	// 触达的一切内容都是不可变的。
	pl *flowcontrol.PriorityLevelConfiguration

	// qsCompleter 持有由 `pl` 和 `queues`
	// 派生出的 QueueSetCompleter。
	qsCompleter fq.QueueSetCompleter

	// 该优先级级别的 QueueSet。
	// 绝不为 nil。
	queues fq.QueueSet

	// quiescing==true 表示当该优先级级别的所有队列
	// 排空之后，应当将其移除。
	quiescing bool

	// 处于 Controller::Match 与调用返回的
	// StartFunction 之间的 goroutine 数量
	numPending int

	// 跟踪等待中、执行中请求数量的观察者
	reqsGaugePair metrics.RatioedGaugePair

	// 跟踪整个执行过程中占用席位数量的观察者
	execSeatsObs metrics.RatioedGauge

	// 席位需求的积分器，每个 CurrentCL 调整周期重置一次
	seatDemandIntegrator fq.Integrator

	// 席位需求 / nominalCL 的 Gauge
	seatDemandRatioedGauge metrics.RatioedGauge

	// seatDemandStats 通过周期性检查 seatDemandIntegrator 派生而来。
	// 平均值、标准差、高水位线直接取自积分器。
	// envelope = avg + stdDev。
	// Periodically smoothed 会被替换为 `max(envelope, A*smoothed + (1-A)*envelope)`，
	// 其中 A 是 seatDemandSmoothingCoefficient。
	seatDemandStats seatDemandStats

	// nominalCL 是 PriorityLevelConfiguration 中配置的名义并发限制
	nominalCL int

	// minCL 是名义限制减去可借出额度
	minCL int

	// maxCL 是名义限制加上可借入额度
	maxCL int

	// currentCL 是动态推导出的、当前要施加的并发限制
	currentCL int
}

// seatDemandStats 汇总一个调优周期内的席位需求统计。
// update 会丢弃零时长样本，并在偏差为 NaN（样本极少）时按 0 处理，
// 以指数平滑方式得到稳定的需求估计，作为并发配额调整的目标值。
type seatDemandStats struct {
	avg           float64
	stdDev        float64
	highWatermark int
	smoothed      float64
}

// update 由积分器结果更新平滑统计；envelope = avg + stdDev 作为目标，
// smoothed 只允许单调上升或按系数衰减，避免需求估计剧烈抖动。
func (stats *seatDemandStats) update(obs fq.IntegratorResults) {
	stats.highWatermark = int(math.Round(obs.Max))
	if obs.Duration <= 0 {
		return
	}
	if math.IsNaN(obs.Deviation) {
		obs.Deviation = 0
	}
	stats.avg = obs.Average
	stats.stdDev = obs.Deviation
	envelope := obs.Average + obs.Deviation
	stats.smoothed = math.Max(envelope, seatDemandSmoothingCoefficient*stats.smoothed+(1-seatDemandSmoothingCoefficient)*envelope)
}

// NewTestableController 为了便于测试而保留了额外的灵活性
//
// 注意：函数名虽然是 NewTestableController，但返回的是生产用的 configController，
// 仅仅把依赖（时钟、队列工厂、informer 工厂等）显式传入以便测试替换。
func newTestableController(config TestableConfig) *configController {
	cfgCtlr := &configController{
		name:                   config.Name,
		clock:                  config.Clock,
		queueSetFactory:        config.QueueSetFactory,
		reqsGaugeVec:           config.ReqsGaugeVec,
		execSeatsGaugeVec:      config.ExecSeatsGaugeVec,
		asFieldManager:         config.AsFieldManager,
		foundToDangling:        config.FoundToDangling,
		serverConcurrencyLimit: config.ServerConcurrencyLimit,
		flowcontrolClient:      config.FlowcontrolClient,
		priorityLevelStates:    make(map[string]*priorityLevelState),
		WatchTracker:           NewWatchTracker(),
		MaxSeatsTracker:        NewMaxSeatsTracker(),
	}
	klog.V(2).Infof("NewTestableController %q with serverConcurrencyLimit=%d, name=%s, asFieldManager=%q", cfgCtlr.name, cfgCtlr.serverConcurrencyLimit, cfgCtlr.name, cfgCtlr.asFieldManager)
	// 一开始使用较长的延迟，因为冲突发生在不同进程之间，
	// 需要一些时间才会消失。
	cfgCtlr.configQueue = workqueue.NewTypedRateLimitingQueueWithConfig(
		workqueue.NewTypedItemExponentialFailureRateLimiter[int](200*time.Millisecond, 8*time.Hour),
		workqueue.TypedRateLimitingQueueConfig[int]{Name: "priority_and_fairness_config_queue"},
	)
	// 确保数据结构反映必选（mandatory）配置
	cfgCtlr.lockAndDigestConfigObjects(nil, nil)
	fci := config.InformerFactory.Flowcontrol().V1()
	pli := fci.PriorityLevelConfigurations()
	fsi := fci.FlowSchemas()
	cfgCtlr.plLister = pli.Lister()
	cfgCtlr.plInformerSynced = pli.Informer().HasSynced
	cfgCtlr.fsLister = fsi.Lister()
	cfgCtlr.fsInformerSynced = fsi.Informer().HasSynced
	pli.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pl := obj.(*flowcontrol.PriorityLevelConfiguration)
			klog.V(7).Infof("Triggered API priority and fairness config reloading in %s due to creation of PLC %s", cfgCtlr.name, pl.Name)
			cfgCtlr.configQueue.Add(0)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			newPL := newObj.(*flowcontrol.PriorityLevelConfiguration)
			oldPL := oldObj.(*flowcontrol.PriorityLevelConfiguration)
			if !apiequality.Semantic.DeepEqual(oldPL.Spec, newPL.Spec) {
				klog.V(7).Infof("Triggered API priority and fairness config reloading in %s due to spec update of PLC %s", cfgCtlr.name, newPL.Name)
				cfgCtlr.configQueue.Add(0)
			} else {
				klog.V(7).Infof("No trigger API priority and fairness config reloading in %s due to spec non-change of PLC %s", cfgCtlr.name, newPL.Name)
			}
		},
		DeleteFunc: func(obj interface{}) {
			name, _ := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			klog.V(7).Infof("Triggered API priority and fairness config reloading in %s due to deletion of PLC %s", cfgCtlr.name, name)
			cfgCtlr.configQueue.Add(0)

		}})
	fsi.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			fs := obj.(*flowcontrol.FlowSchema)
			klog.V(7).Infof("Triggered API priority and fairness config reloading in %s due to creation of FS %s", cfgCtlr.name, fs.Name)
			cfgCtlr.configQueue.Add(0)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			newFS := newObj.(*flowcontrol.FlowSchema)
			oldFS := oldObj.(*flowcontrol.FlowSchema)
			// Spec 或 Status 的变更都与之相关。令人担忧的是，
			// 未来的某个版本我们可能想要与现在实现
			// 不同的行为。最难回答的问题之一是：在有多个
			// kube-apiserver 的集群中如何滚动发布新版本
			// ——无论哪个服务器在什么时候崩溃并重启，
			// 都要能正常工作。如果本 handler 只对
			// Spec 的变更做出反应，那么存在这样一个场景：
			// 滚动更新会把旧的 Status 保留下来。该
			// 场景以如下子序列收尾：在删除最后一台旧服务器
			// 之前部署最后一台新服务器，而在这两个操作
			// 之间，最后一台旧服务器崩溃并恢复。所选的
			// 解决方案是让本控制器坚持维护它自己
			// 建立起来的那个特定状态。
			if !(apiequality.Semantic.DeepEqual(oldFS.Spec, newFS.Spec) &&
				apiequality.Semantic.DeepEqual(oldFS.Status, newFS.Status)) {
				klog.V(7).Infof("Triggered API priority and fairness config reloading in %s due to spec and/or status update of FS %s", cfgCtlr.name, newFS.Name)
				cfgCtlr.configQueue.Add(0)
			} else {
				klog.V(7).Infof("No trigger of API priority and fairness config reloading in %s due to spec and status non-change of FS %s", cfgCtlr.name, newFS.Name)
			}
		},
		DeleteFunc: func(obj interface{}) {
			name, _ := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			klog.V(7).Infof("Triggered API priority and fairness config reloading in %s due to deletion of FS %s", cfgCtlr.name, name)
			cfgCtlr.configQueue.Add(0)

		}})
	return cfgCtlr
}

func (cfgCtlr *configController) Run(stopCh <-chan struct{}) error {
	defer utilruntime.HandleCrash()

	// 在我们结束时让 config worker 也停止
	defer cfgCtlr.configQueue.ShutDown()

	klog.Info("Starting API Priority and Fairness config controller")
	// 必须等两个 informer 完成初次同步，否则会用不完整的配置启动限流，
	// 可能把请求错误地归入 catch-all。
	if ok := cache.WaitForCacheSync(stopCh, cfgCtlr.plInformerSynced, cfgCtlr.fsInformerSynced); !ok {
		return fmt.Errorf("Never achieved initial sync")
	}

	klog.Info("Running API Priority and Fairness config worker")
	go wait.Until(cfgCtlr.runWorker, time.Second, stopCh)

	klog.Info("Running API Priority and Fairness periodic rebalancing process")
	go wait.Until(cfgCtlr.updateBorrowing, borrowingAdjustmentPeriod, stopCh)

	<-stopCh
	klog.Info("Shutting down API Priority and Fairness config worker")
	return nil
}

// updateBorrowing 周期性（borrowingAdjustmentPeriod）重新计算各级别的
// CurrentCL。调用方需持锁？——不需要：方法内部自行加写锁。
func (cfgCtlr *configController) updateBorrowing() {
	cfgCtlr.lock.Lock()
	defer cfgCtlr.lock.Unlock()
	cfgCtlr.updateBorrowingLocked(true, cfgCtlr.priorityLevelStates)
}

// updateBorrowingLocked 是席位借用算法的核心：先读取各优先级的席位需求，
// 再在“最小保证（minCL）—最大可借（maxCL）”区间内按公平分配求解 CurrentCL。
// 调用方必须持有写锁。
func (cfgCtlr *configController) updateBorrowingLocked(setCompleters bool, plStates map[string]*priorityLevelState) {
	items := make([]allocProblemItem, 0, len(plStates))
	nonExemptPLNames := make([]string, 0, len(plStates))
	idxOfNonExempt := map[string]int{} // 非豁免类在 items 中的下标
	cclOfExempt := map[string]int{}    // 豁免类的 minCurrentCL
	var minCLSum, minCurrentCLSum int  // 对非豁免类求和
	remainingServerCL := cfgCtlr.nominalCLSum
	for plName, plState := range plStates {
		// 每个调整周期读取并重置需求积分器，得到本周期的高水位与平滑值。
		obs := plState.seatDemandIntegrator.Reset()
		plState.seatDemandStats.update(obs)
		var minCurrentCL int
		if plState.pl.Spec.Type == flowcontrol.PriorityLevelEnablementExempt {
			// Exempt 级别不参与公平分配，直接按需求高水位保证其并发，
			// 并从可分配总量中扣除。
			minCurrentCL = max(plState.minCL, plState.seatDemandStats.highWatermark)
			cclOfExempt[plName] = minCurrentCL
			remainingServerCL -= minCurrentCL
		} else {
			// 该优先级级别调整后并发限制的下界取以下两者中的较小值：
			// - 上一个调整周期内席位需求的高水位，以及
			// - 其配置的并发限制。
			// 但是：不希望这低于来自配置的下界。
			// 更详细的解释参见 KEP-1040。
			minCurrentCL = max(plState.minCL, min(plState.nominalCL, plState.seatDemandStats.highWatermark))
			idxOfNonExempt[plName] = len(items)
			nonExemptPLNames = append(nonExemptPLNames, plName)
			items = append(items, allocProblemItem{
				lowerBound: float64(minCurrentCL),
				upperBound: float64(plState.maxCL),
				target:     math.Max(float64(minCurrentCL), plState.seatDemandStats.smoothed),
			})
			minCLSum += plState.minCL
			minCurrentCLSum += minCurrentCL
		}
	}
	if len(items) == 0 && cfgCtlr.nominalCLSum > 0 {
		// 有名义容量却没有任何可调级别，说明配置内部不一致；只记录错误并
		// 保留现状，避免把容量清零导致服务不可用。
		klog.ErrorS(nil, "Impossible: no priority levels", "plStates", cfgCtlr.priorityLevelStates)
		return
	}
	var allocs []float64
	var shareFrac, fairFrac float64
	var err error
	// 三段式：剩余容量不足最小保证时按比例压缩；只够最小保证时给定 minCurrentCL；
	// 否则做公平分配。
	if remainingServerCL <= minCLSum {
		metrics.SetFairFrac(0)
	} else if remainingServerCL <= minCurrentCLSum {
		shareFrac = float64(remainingServerCL-minCLSum) / float64(minCurrentCLSum-minCLSum)
		metrics.SetFairFrac(0)
	} else {
		allocs, fairFrac, err = computeConcurrencyAllocation(cfgCtlr.nominalCLSum, items)
		if err != nil {
			// 分配求解失败时退回到当前值（而不是清零），保证限流配置始终可用。
			klog.ErrorS(err, "Unable to derive new concurrency limits", "plNames", nonExemptPLNames, "items", items)
			allocs = make([]float64, len(items))
			for idx, plName := range nonExemptPLNames {
				plState := plStates[plName]
				allocs[idx] = float64(plState.currentCL)
			}
		}
		metrics.SetFairFrac(float64(fairFrac))
	}
	for plName, plState := range plStates {
		idx, isNonExempt := idxOfNonExempt[plName]
		if setCompleters {
			qsCompleter, err := queueSetCompleterForPL(cfgCtlr.queueSetFactory, plState.queues,
				plState.pl, plState.reqsGaugePair, plState.execSeatsObs,
				metrics.NewUnionGauge(plState.seatDemandIntegrator, plState.seatDemandRatioedGauge))
			if err != nil {
				klog.ErrorS(err, "Inconceivable!  Configuration error in existing priority level", "pl", plState.pl)
				continue
			}
			plState.qsCompleter = qsCompleter
		}
		var currentCL int
		if !isNonExempt {
			currentCL = cclOfExempt[plName]
		} else if remainingServerCL <= minCLSum {
			currentCL = plState.minCL
		} else if remainingServerCL <= minCurrentCLSum {
			minCurrentCL := max(plState.minCL, min(plState.nominalCL, plState.seatDemandStats.highWatermark))
			currentCL = plState.minCL + int(math.Round(float64(minCurrentCL-plState.minCL)*shareFrac))
		} else {
			currentCL = int(math.Round(float64(allocs[idx])))
		}
		relChange := relDiff(float64(currentCL), float64(plState.currentCL))
		plState.currentCL = currentCL
		metrics.NotePriorityLevelConcurrencyAdjustment(plState.pl.Name, float64(plState.seatDemandStats.highWatermark), plState.seatDemandStats.avg, plState.seatDemandStats.stdDev, plState.seatDemandStats.smoothed, float64(items[idx].target), currentCL)
		logLevel := klog.Level(4)
		if relChange >= 0.05 {
			logLevel = 2
		}
		var concurrencyDenominator int
		if currentCL > 0 {
			concurrencyDenominator = currentCL
		} else {
			concurrencyDenominator = int(math.Max(1, math.Round(float64(cfgCtlr.serverConcurrencyLimit)/10)))
		}
		plState.seatDemandRatioedGauge.SetDenominator(float64(concurrencyDenominator))
		klog.V(logLevel).InfoS("Update CurrentCL", "plName", plName, "seatDemandHighWatermark", plState.seatDemandStats.highWatermark, "seatDemandAvg", plState.seatDemandStats.avg, "seatDemandStdev", plState.seatDemandStats.stdDev, "seatDemandSmoothed", plState.seatDemandStats.smoothed, "fairFrac", fairFrac, "currentCL", currentCL, "concurrencyDenominator", concurrencyDenominator, "backstop", err != nil)
		plState.queues = plState.qsCompleter.Complete(fq.DispatchingConfig{ConcurrencyLimit: currentCL, ConcurrencyDenominator: concurrencyDenominator})
	}
}

// runWorker 是那唯一一个 worker goroutine 的逻辑。我们
// 把它限制为一个，以免围绕对 `cfgCtlr.mostRecentUpdates`
// 的访问需要显式的同步。
func (cfgCtlr *configController) runWorker() {
	for cfgCtlr.processNextWorkItem() {
	}
}

// processNextWorkItem 处理工作队列中的一个条目。
// 只在那唯一一个 worker goroutine 中调用它。
//
// 失败走 rate-limited 重试；需要延迟的情况（如状态更新过于频繁）用
// AddAfter 而不消耗重试退避。
func (cfgCtlr *configController) processNextWorkItem() bool {
	obj, shutdown := cfgCtlr.configQueue.Get()
	if shutdown {
		return false
	}

	func(obj int) {
		defer cfgCtlr.configQueue.Done(obj)
		specificDelay, err := cfgCtlr.syncOne()
		switch {
		case err != nil:
			klog.Error(err)
			cfgCtlr.configQueue.AddRateLimited(obj)
		case specificDelay > 0:
			cfgCtlr.configQueue.AddAfter(obj, specificDelay)
		default:
			cfgCtlr.configQueue.Forget(obj)
		}
	}(obj)

	return true
}

// syncOne 执行一次完整同步。它读取所有用于配置
// API Priority and Fairness 的 API 对象，并相应地更新
// 本地的 configController。
// 只在那唯一一个 worker goroutine 中调用它
func (cfgCtlr *configController) syncOne() (specificDelay time.Duration, err error) {
	klog.V(5).Infof("%s syncOne at %s", cfgCtlr.name, cfgCtlr.clock.Now().Format(timeFmt))
	all := labels.Everything()
	newPLs, err := cfgCtlr.plLister.List(all)
	if err != nil {
		return 0, fmt.Errorf("unable to list PriorityLevelConfiguration objects: %w", err)
	}
	newFSs, err := cfgCtlr.fsLister.List(all)
	if err != nil {
		return 0, fmt.Errorf("unable to list FlowSchema objects: %w", err)
	}
	return cfgCtlr.digestConfigObjects(newPLs, newFSs)
}

// cfgMeal 是消化那些配置 API Priority and Fairness 的 API
// 对象的过程中所涉及的数据。所有配置对象是一起消化的，
// 因为这是处理对象之间各种依赖关系的最简单方式。
// 消化过程对配置对象做四遍扫描 —— 三遍扫
// PriorityLevelConfiguration、一遍扫 FlowSchema —— 各遍
// 之间的分工依据这些依赖关系而定。
//
// 所有对象一次性处理的好处是：FlowSchema 引用不到 PL 时可以直接在同一轮
// 判定为 Dangling，而不需要多轮收敛。
type cfgMeal struct {
	cfgCtlr *configController

	newPLStates map[string]*priorityLevelState

	// 新配置中各优先级级别并发份额（concurrency share）之和
	shareSum float64

	// 这些字段用于跟踪哪些必选的优先级级别配置
	// 对象已经被消化
	haveExemptPL, haveCatchAllPL bool

	// 缓冲待执行的 FlowSchema status 更新。在不持有锁时
	// 执行这些更新，以避免死锁：此类请求会触发
	// 对本控制器的回调，而若在持锁等待该请求完成时
	// 执行更新就会造成死锁。
	fsStatusUpdates []fsStatusUpdate

	maxWaitingRequests, maxExecutingRequests int
}

// 一组缓冲的 FlowSchema status 更新
//
// 同时保留新值与旧值，便于日志中展示差异，也方便判断是否真的需要写入。
type fsStatusUpdate struct {
	flowSchema *flowcontrol.FlowSchema
	condition  flowcontrol.FlowSchemaCondition
	oldValue   flowcontrol.FlowSchemaCondition
}

// digestConfigObjects 拿到所有用于配置 cfgCtlr 的 API 对象，
// 并写出由此决定的新 configState。
// 只在那唯一一个 worker goroutine 中调用它
//
// 持锁完成“消化”，随后在锁外写回 FlowSchema status —— 写 API 可能再次触发
// 本控制器，持锁写会造成死锁。
func (cfgCtlr *configController) digestConfigObjects(newPLs []*flowcontrol.PriorityLevelConfiguration, newFSs []*flowcontrol.FlowSchema) (time.Duration, error) {
	fsStatusUpdates := cfgCtlr.lockAndDigestConfigObjects(newPLs, newFSs)
	var errs []error
	currResult := updateAttempt{
		timeUpdated:  cfgCtlr.clock.Now(),
		updatedItems: sets.String{},
	}
	var suggestedDelay time.Duration
	for _, fsu := range fsStatusUpdates {
		// 若该名称应当跳过，则表示将需要一次延迟，但继续处理其余条目
		if cfgCtlr.shouldDelayUpdate(fsu.flowSchema.Name) {
			if suggestedDelay == 0 {
				suggestedDelay = time.Duration(30+rand.Intn(45)) * time.Second
			}
			continue
		}

		// 如果要发起一次更新，务必跟踪每一个被更新的名称，
		// 以便知道是否更新得太频繁。
		currResult.updatedItems.Insert(fsu.flowSchema.Name)
		if klogV := klog.V(4); klogV.Enabled() {
			klogV.Infof("%s writing Condition %s to FlowSchema %s, which had ResourceVersion=%s, because its previous value was %s, diff: %s",
				cfgCtlr.name, fsu.condition, fsu.flowSchema.Name, fsu.flowSchema.ResourceVersion, fcfmt.Fmt(fsu.oldValue), diff.Diff(fsu.oldValue, fsu.condition))
		}

		if err := apply(cfgCtlr.flowcontrolClient.FlowSchemas(), fsu, cfgCtlr.asFieldManager); err != nil {
			if apierrors.IsNotFound(err) {
				// 该对象已被删除。删除通知很快就会到来，
				// 这里无需再做任何处理。
				klog.V(5).Infof("%s at %s: attempted update of concurrently deleted FlowSchema %s; nothing more needs to be done", cfgCtlr.name, cfgCtlr.clock.Now().Format(timeFmt), fsu.flowSchema.Name)
			} else {
				errs = append(errs, fmt.Errorf("failed to set a status.condition for FlowSchema %s: %w", fsu.flowSchema.Name, err))
			}
		}
	}
	cfgCtlr.addUpdateResult(currResult)

	return suggestedDelay, utilerrors.NewAggregate(errs)
}

// apply 用 server-side apply 只写 status，避免与其他控制器争用 spec。
func apply(client flowcontrolclient.FlowSchemaInterface, fsu fsStatusUpdate, asFieldManager string) error {
	applyOptions := metav1.ApplyOptions{FieldManager: asFieldManager, Force: true}

	// condition 字段持有我们要更新的新 condition。
	// TODO: 当一个 flowschema 有多个 condition 时，这里会失效
	_, err := client.ApplyStatus(context.TODO(), toFlowSchemaApplyConfiguration(fsu), applyOptions)
	return err
}

// toFlowSchemaApplyConfiguration 把内部 fsStatusUpdate 转换为 SSA 的
// ApplyConfiguration；目前只维护 Dangling 一个 condition。
func toFlowSchemaApplyConfiguration(fsUpdate fsStatusUpdate) *flowcontrolapplyconfiguration.FlowSchemaApplyConfiguration {
	condition := flowcontrolapplyconfiguration.FlowSchemaCondition().
		WithType(fsUpdate.condition.Type).
		WithStatus(fsUpdate.condition.Status).
		WithReason(fsUpdate.condition.Reason).
		WithLastTransitionTime(fsUpdate.condition.LastTransitionTime).
		WithMessage(fsUpdate.condition.Message)

	return flowcontrolapplyconfiguration.FlowSchema(fsUpdate.flowSchema.Name).
		WithStatus(flowcontrolapplyconfiguration.FlowSchemaStatus().
			WithConditions(condition),
		)
}

// shouldDelayUpdate 检查某个 flowschema 是否更新得过于频繁，
// 若需要延迟则返回 true。
// 只在那唯一一个 worker goroutine 中调用它
//
// 顺带清理超过一分钟的历史记录，避免切片无限增长。
func (cfgCtlr *configController) shouldDelayUpdate(flowSchemaName string) bool {
	numUpdatesInPastMinute := 0
	oneMinuteAgo := cfgCtlr.clock.Now().Add(-1 * time.Minute)
	for idx, update := range cfgCtlr.mostRecentUpdates {
		if oneMinuteAgo.After(update.timeUpdated) {
			// 本条目及其之后的条目都已不再相关
			cfgCtlr.mostRecentUpdates = cfgCtlr.mostRecentUpdates[:idx]
			return false
		}
		if update.updatedItems.Has(flowSchemaName) {
			numUpdatesInPastMinute++
			if numUpdatesInPastMinute > 5 {
				return true
			}
		}
	}
	return false
}

// addUpdateResult 追加这条更新结果。之所以不用环形缓冲，是因为
// 它很小且有速率限制。
// 只在那唯一一个 worker goroutine 中调用它
func (cfgCtlr *configController) addUpdateResult(result updateAttempt) {
	cfgCtlr.mostRecentUpdates = append([]updateAttempt{result}, cfgCtlr.mostRecentUpdates...)
}

// lockAndDigestConfigObjects 在持有写锁的情况下完成整个消化流程，并在
// 返回后由调用方在锁外执行 status 更新。
func (cfgCtlr *configController) lockAndDigestConfigObjects(newPLs []*flowcontrol.PriorityLevelConfiguration, newFSs []*flowcontrol.FlowSchema) []fsStatusUpdate {
	cfgCtlr.lock.Lock()
	defer cfgCtlr.lock.Unlock()
	meal := cfgMeal{
		cfgCtlr:     cfgCtlr,
		newPLStates: make(map[string]*priorityLevelState),
	}

	meal.digestNewPLsLocked(newPLs)
	meal.digestFlowSchemasLocked(newFSs)
	meal.processOldPLsLocked()

	// 补上缺失的必选 PriorityLevelConfiguration 对象
	if !meal.haveExemptPL {
		meal.imaginePL(fcboot.MandatoryPriorityLevelConfigurationExempt)
	}
	if !meal.haveCatchAllPL {
		meal.imaginePL(fcboot.MandatoryPriorityLevelConfigurationCatchAll)
	}

	meal.finishQueueSetReconfigsLocked()

	// 新配置已经构建完成
	cfgCtlr.priorityLevelStates = meal.newPLStates
	klog.V(5).InfoS("Switched to new API Priority and Fairness configuration", "maxWaitingRequests", meal.maxWaitingRequests, "maxExecutinRequests", meal.maxExecutingRequests)

	metrics.GetWaitingReadonlyConcurrency().SetDenominator(float64(meal.maxWaitingRequests))
	metrics.GetWaitingMutatingConcurrency().SetDenominator(float64(meal.maxWaitingRequests))
	metrics.GetExecutingReadonlyConcurrency().SetDenominator(float64(meal.maxExecutingRequests))
	metrics.GetExecutingMutatingConcurrency().SetDenominator(float64(meal.maxExecutingRequests))

	return meal.fsStatusUpdates
}

// 消化新一批 PriorityLevelConfiguration 对象。
// 对损坏的对象，假装它们不存在。
//
// 复用已有 priorityLevelState 以保留队列与需求统计；这样配置更新不会丢弃
// 正在排队的请求。spec 非法的对象被跳过，其 FlowSchema 会因引用失效被摘除。
func (meal *cfgMeal) digestNewPLsLocked(newPLs []*flowcontrol.PriorityLevelConfiguration) {
	for _, pl := range newPLs {
		state := meal.cfgCtlr.priorityLevelStates[pl.Name]
		if state == nil {
			labelValues := []string{pl.Name}
			state = &priorityLevelState{
				reqsGaugePair:          metrics.RatioedGaugeVecPhasedElementPair(meal.cfgCtlr.reqsGaugeVec, 1, 1, labelValues),
				execSeatsObs:           meal.cfgCtlr.execSeatsGaugeVec.NewForLabelValuesSafe(0, 1, labelValues),
				seatDemandIntegrator:   fq.NewNamedIntegrator(meal.cfgCtlr.clock, pl.Name),
				seatDemandRatioedGauge: metrics.ApiserverSeatDemands.NewForLabelValuesSafe(0, 1, []string{pl.Name}),
			}
		}
		qsCompleter, err := queueSetCompleterForPL(meal.cfgCtlr.queueSetFactory, state.queues,
			pl, state.reqsGaugePair, state.execSeatsObs,
			metrics.NewUnionGauge(state.seatDemandIntegrator, state.seatDemandRatioedGauge))
		if err != nil {
			klog.Warningf("Ignoring PriorityLevelConfiguration object %s because its spec (%s) is broken: %s", pl.Name, fcfmt.Fmt(pl.Spec), err)
			continue
		}
		meal.newPLStates[pl.Name] = state
		state.pl = pl
		state.qsCompleter = qsCompleter
		if state.quiescing { // 曾经不再需要，但现在又需要了
			klog.V(3).Infof("Priority level %q was undesired and has become desired again", pl.Name)
			state.quiescing = false
		}
		nominalConcurrencyShares, _, _ := plSpecCommons(state.pl)
		meal.shareSum += float64(*nominalConcurrencyShares)
		meal.haveExemptPL = meal.haveExemptPL || pl.Name == flowcontrol.PriorityLevelConfigurationNameExempt
		meal.haveCatchAllPL = meal.haveCatchAllPL || pl.Name == flowcontrol.PriorityLevelConfigurationNameCatchAll
	}
}

// 消化给定的 FlowSchema 对象。引用了缺失或损坏优先级
// 级别的对象，不会交给 filter
// 使用。我们在保留旧优先级级别之前先做这一步，
// 以便请求停止流向那些级别、且 FlowSchemaStatus
// 值能反映这一点。本函数还会补上所有缺失的必选
// FlowSchema 对象。给定的对象必须名称各不相同。
//
// 重名对象只可能来自直接的 API 写入（校验层已禁止），因此直接 panic：
// 继续执行会导致优先级顺序不确定，比崩溃更难排查。
func (meal *cfgMeal) digestFlowSchemasLocked(newFSs []*flowcontrol.FlowSchema) {
	fsSeq := make(apihelpers.FlowSchemaSequence, 0, len(newFSs))
	fsMap := make(map[string]*flowcontrol.FlowSchema, len(newFSs))
	var haveExemptFS, haveCatchAllFS bool
	for i, fs := range newFSs {
		otherFS := fsMap[fs.Name]
		if otherFS != nil {
			// 禁止本客户端这样做。
			panic(fmt.Sprintf("Given two FlowSchema objects with the same name: %s and %s", fcfmt.Fmt(otherFS), fcfmt.Fmt(fs)))
		}
		fsMap[fs.Name] = fs
		_, goodPriorityRef := meal.newPLStates[fs.Spec.PriorityLevelConfiguration.Name]

		// 确保对象的 status 反映其优先级级别引用是否
		// 已损坏。
		//
		// TODO: 考虑在服务器尚未开始处理请求时干脆不尝试。
		meal.presyncFlowSchemaStatus(fs, meal.cfgCtlr.foundToDangling(goodPriorityRef), fs.Spec.PriorityLevelConfiguration.Name)

		if !goodPriorityRef {
			klog.V(6).Infof("Ignoring FlowSchema %s because of bad priority level reference %q", fs.Name, fs.Spec.PriorityLevelConfiguration.Name)
			continue
		}
		fsSeq = append(fsSeq, newFSs[i])
		haveExemptFS = haveExemptFS || fs.Name == flowcontrol.FlowSchemaNameExempt
		haveCatchAllFS = haveCatchAllFS || fs.Name == flowcontrol.FlowSchemaNameCatchAll
	}
	// 按匹配时使用的顺序排序
	sort.Sort(fsSeq)

	// 以正确的位置补上缺失的必选 FlowSchema
	if !haveExemptFS {
		fsSeq = append(apihelpers.FlowSchemaSequence{fcboot.MandatoryFlowSchemaExempt}, fsSeq...)
	}
	if !haveCatchAllFS {
		fsSeq = append(fsSeq, fcboot.MandatoryFlowSchemaCatchAll)
	}

	meal.cfgCtlr.flowSchemas = fsSeq
	klogV := klog.V(5)
	if klogV.Enabled() {
		for _, fs := range fsSeq {
			klogV.Infof("Using FlowSchema %s", fcfmt.Fmt(fs))
		}
	}
}

// 考虑先前配置中的所有优先级级别。
// 保留那些仍在新配置中、属于必选行为、或仍然繁忙的级别；
// 其余的：若没有队列就直接丢弃，否则若尚未开始
// 就启动静默（quiescing）流程。
//
// 关键不变量：只要还有请求在该级别排队/执行，就不能删除状态，否则请求会
// 失去归属；此时只标记 quiescing，待排空后再回收。
func (meal *cfgMeal) processOldPLsLocked() {
	for plName, plState := range meal.cfgCtlr.priorityLevelStates {
		if meal.newPLStates[plName] != nil {
			// 仍然是需要的，且已更新
			continue
		}
		if plName == flowcontrol.PriorityLevelConfigurationNameExempt && !meal.haveExemptPL || plName == flowcontrol.PriorityLevelConfigurationNameCatchAll && !meal.haveCatchAllPL {
			// 顺便说一句，我们知道 Spec 中关于排队的规定没有变化，
			// 因为就排队而言，必选对象的 Spec 是不可变的。
			klog.V(3).Infof("Retaining mandatory priority level %q despite lack of API object", plName)
		} else {
			if plState.numPending == 0 && plState.queues.IsIdle() {
				// QueueSet 已排空完毕，
				// 也不会有其他
				// goroutine 再来使用
				klog.V(3).Infof("Removing undesired priority level %q, Type=%v", plName, plState.pl.Spec.Type)
				meal.cfgCtlr.MaxSeatsTracker.ForgetPriorityLevel(plName)
				continue
			}
			if !plState.quiescing {
				klog.V(3).Infof("Priority level %q became undesired", plName)
				plState.quiescing = true
			}
		}
		var err error
		plState.qsCompleter, err = queueSetCompleterForPL(meal.cfgCtlr.queueSetFactory, plState.queues,
			plState.pl, plState.reqsGaugePair, plState.execSeatsObs,
			metrics.NewUnionGauge(plState.seatDemandIntegrator, plState.seatDemandRatioedGauge))
		if err != nil {
			// 这不可能发生，因为 queueSetCompleterForPL 已经批准过这份配置
			panic(fmt.Sprintf("%s from name=%q spec=%s", err, plName, fcfmt.Fmt(plState.pl.Spec)))
		}
		// 我们特意在这里把滞留的优先级级别也包含进来，
		// 好让它们的队列分到一些并发、
		// 继续排空。在这段过渡期内，滞留的
		// 优先级级别继续按照常规方式，
		// 由所有 share 值共同决定其并发
		// 分配。
		nominalConcurrencyShares, _, _ := plSpecCommons(plState.pl)
		meal.shareSum += float64(*nominalConcurrencyShares)
		meal.haveExemptPL = meal.haveExemptPL || plName == flowcontrol.PriorityLevelConfigurationNameExempt
		meal.haveCatchAllPL = meal.haveCatchAllPL || plName == flowcontrol.PriorityLevelConfigurationNameCatchAll
		meal.newPLStates[plName] = plState
	}
}

// 对新配置中的所有优先级级别，把服务器的总并发限制
// 在它们之间分配，并创建/更新它们的 QueueSet。
//
// concurrencyLimit 按 nominalConcurrencyShares 占比分配；lendable/borrowing
// 百分比决定 minCL/maxCL，从而支持级别之间的容量借用。
func (meal *cfgMeal) finishQueueSetReconfigsLocked() {
	for plName, plState := range meal.newPLStates {
		nominalConcurrencyShares, lendablePercent, borrowingLimitPercent := plSpecCommons(plState.pl)
		// 这里使用 math.Ceil 意味着结果之和可能
		// 略大于 serverConcurrencyLimit，但
		// 差值可以忽略不计。
		concurrencyLimit := int(math.Ceil(float64(meal.cfgCtlr.serverConcurrencyLimit) * float64(*nominalConcurrencyShares) / meal.shareSum))
		var lendableCL, borrowingCL int
		if lendablePercent != nil {
			lendableCL = int(math.Round(float64(concurrencyLimit) * float64(*lendablePercent) / 100))
		}
		if borrowingLimitPercent != nil {
			borrowingCL = int(math.Round(float64(concurrencyLimit) * float64(*borrowingLimitPercent) / 100))
		} else {
			borrowingCL = meal.cfgCtlr.serverConcurrencyLimit
		}

		metrics.SetPriorityLevelConfiguration(plName, concurrencyLimit, concurrencyLimit-lendableCL, concurrencyLimit+borrowingCL)
		cfgChanged := plState.nominalCL != concurrencyLimit || plState.minCL != concurrencyLimit-lendableCL || plState.maxCL != concurrencyLimit+borrowingCL
		plState.nominalCL = concurrencyLimit
		plState.minCL = concurrencyLimit - lendableCL
		plState.maxCL = concurrencyLimit + borrowingCL
		meal.maxExecutingRequests += concurrencyLimit
		if limited := plState.pl.Spec.Limited; limited != nil {
			if qCfg := limited.LimitResponse.Queuing; qCfg != nil {
				meal.maxWaitingRequests += int(qCfg.Queues * qCfg.QueueLengthLimit)

				// 可从 work estimator 分配的最大席位数按 MAX(1, MIN(0.15 * nominalCL, nominalCL/handSize)) 计算。
				// 这样做是为了让最大席位数相对于总可用并发保持比例，且最小值为 1。
				// 选择名义并发的 15% 是因为在使用 apiserver 默认总并发 600
				// （--max-requests-inflight=400、--max-mutating-requests-inflight=200）的默认优先级级别上，
				// 它保持了此前最大席位数 10。
				// 这确保 inflight 请求较多的集群继续以 10 为最大席位数上限，
				// 而 inflight 请求较少的集群使用的最大席位数不会超过 nominalCL/handSize。
				// 计算出的最大席位数可能得出任意高的值，但 work estimator 目前把最大席位数限制在 10。
				handSize := plState.pl.Spec.Limited.LimitResponse.Queuing.HandSize
				maxSeats := uint64(math.Max(1, math.Min(math.Ceil(float64(concurrencyLimit)*priorityLevelMaxSeatsPercent), float64(int32(concurrencyLimit)/handSize))))
				meal.cfgCtlr.MaxSeatsTracker.SetMaxSeats(plName, maxSeats)
			}
		}
		if plState.queues == nil {
			initialCL := concurrencyLimit - lendableCL/2
			klog.V(2).Infof("Introducing queues for priority level %q: config=%s, nominalCL=%d, lendableCL=%d, borrowingCL=%d, currentCL=%d, quiescing=%v (shares=%v, shareSum=%v)", plName, fcfmt.Fmt(plState.pl.Spec), concurrencyLimit, lendableCL, borrowingCL, initialCL, plState.quiescing, nominalConcurrencyShares, meal.shareSum)
			plState.seatDemandStats = seatDemandStats{}
			plState.currentCL = initialCL
		} else {
			logLevel := klog.Level(5)
			if cfgChanged {
				logLevel = 2
			}
			klog.V(logLevel).Infof("Retaining queues for priority level %q: config=%s, nominalCL=%d, lendableCL=%d, borrowingCL=%d, currentCL=%d, quiescing=%v, numPending=%d (shares=%v, shareSum=%v)", plName, fcfmt.Fmt(plState.pl.Spec), concurrencyLimit, lendableCL, borrowingCL, plState.currentCL, plState.quiescing, plState.numPending, nominalConcurrencyShares, meal.shareSum)
		}
	}
	meal.cfgCtlr.nominalCLSum = meal.maxExecutingRequests
	meal.cfgCtlr.updateBorrowingLocked(false, meal.newPLStates)
}

// queueSetCompleterForPL 为给定的优先级级别配置返回一个合适的
// QueueSetCompleter。若给定对象以对本包构成问题的方式
// 畸形，则返回 nil 和一个错误。
//
// 这里会校验 “Type 与 union 字段一致”“Exempt 名字必须固定”等结构性约束，
// 不满足时把该 PL 当作不存在，而不是让控制器 panic。
func queueSetCompleterForPL(qsf fq.QueueSetFactory, queues fq.QueueSet, pl *flowcontrol.PriorityLevelConfiguration, reqsIntPair metrics.RatioedGaugePair, execSeatsObs metrics.RatioedGauge, seatDemandGauge metrics.Gauge) (fq.QueueSetCompleter, error) {
	if (pl.Spec.Type == flowcontrol.PriorityLevelEnablementLimited) != (pl.Spec.Limited != nil) {
		return nil, errors.New("broken union structure at the top, for Limited")
	}
	if (pl.Spec.Type == flowcontrol.PriorityLevelEnablementExempt) != (pl.Spec.Exempt != nil) {
		return nil, errors.New("broken union structure at the top, for Exempt")
	}
	if (pl.Spec.Type == flowcontrol.PriorityLevelEnablementExempt) != (pl.Name == flowcontrol.PriorityLevelConfigurationNameExempt) {
		// 本包不打算处理一个优先级级别在 exempt 与非 exempt 之间动态切换的情况。
		return nil, errors.New("non-alignment between name and type")
	}
	qcQS := fq.QueuingConfig{Name: pl.Name}
	if pl.Spec.Limited != nil {
		if (pl.Spec.Limited.LimitResponse.Type == flowcontrol.LimitResponseTypeReject) != (pl.Spec.Limited.LimitResponse.Queuing == nil) {
			return nil, errors.New("broken union structure for limit response")
		}
		qcAPI := pl.Spec.Limited.LimitResponse.Queuing
		if qcAPI != nil {
			qcQS = fq.QueuingConfig{Name: pl.Name,
				DesiredNumQueues: int(qcAPI.Queues),
				QueueLengthLimit: int(qcAPI.QueueLengthLimit),
				HandSize:         int(qcAPI.HandSize),
			}
		}
	} else {
		qcQS = fq.QueuingConfig{Name: pl.Name, DesiredNumQueues: -1}
	}
	var qsc fq.QueueSetCompleter
	var err error
	if queues != nil {
		qsc, err = queues.BeginConfigChange(qcQS)
	} else {
		qsc, err = qsf.BeginConstruction(qcQS, reqsIntPair, execSeatsObs, seatDemandGauge)
	}
	if err != nil {
		err = fmt.Errorf("priority level %q has QueuingConfiguration %#+v, which is invalid: %w", pl.Name, qcQS, err)
	}
	return qsc, err
}

// presyncFlowSchemaStatus 计算该 FlowSchema 期望的 Dangling condition；
// 仅当与现有值不同才记录一次待写 status，减少无谓的 API 写入。
func (meal *cfgMeal) presyncFlowSchemaStatus(fs *flowcontrol.FlowSchema, isDangling bool, plName string) {
	danglingCondition := apihelpers.GetFlowSchemaConditionByType(fs, flowcontrol.FlowSchemaConditionDangling)
	if danglingCondition == nil {
		danglingCondition = &flowcontrol.FlowSchemaCondition{
			Type: flowcontrol.FlowSchemaConditionDangling,
		}
	}
	desiredStatus := flowcontrol.ConditionFalse
	var desiredReason, desiredMessage string
	if isDangling {
		desiredStatus = flowcontrol.ConditionTrue
		desiredReason = "NotFound"
		desiredMessage = fmt.Sprintf("This FlowSchema references the PriorityLevelConfiguration object named %q but there is no such object", plName)
	} else {
		desiredReason = "Found"
		desiredMessage = fmt.Sprintf("This FlowSchema references the PriorityLevelConfiguration object named %q and it exists", plName)
	}
	if danglingCondition.Status == desiredStatus && danglingCondition.Reason == desiredReason && danglingCondition.Message == desiredMessage {
		return
	}
	now := meal.cfgCtlr.clock.Now()
	meal.fsStatusUpdates = append(meal.fsStatusUpdates, fsStatusUpdate{
		flowSchema: fs,
		condition: flowcontrol.FlowSchemaCondition{
			Type:               flowcontrol.FlowSchemaConditionDangling,
			Status:             desiredStatus,
			LastTransitionTime: metav1.NewTime(now),
			Reason:             desiredReason,
			Message:            desiredMessage,
		},
		oldValue: *danglingCondition})
}

// imaginePL 添加一个基于必选级别之一、但目前（此刻）并不作为
// 真实 API 对象存在的优先级级别。
//
// 这是自举（bootstrap）机制：即使 operator 删除了 mandatory 对象，
// apiserver 仍按内置原型提供 exempt 与 catch-all 级别。
func (meal *cfgMeal) imaginePL(proto *flowcontrol.PriorityLevelConfiguration) {
	klog.V(3).Infof("No %s PriorityLevelConfiguration found, imagining one", proto.Name)
	labelValues := []string{proto.Name}
	reqsGaugePair := metrics.RatioedGaugeVecPhasedElementPair(meal.cfgCtlr.reqsGaugeVec, 1, 1, labelValues)
	execSeatsObs := meal.cfgCtlr.execSeatsGaugeVec.NewForLabelValuesSafe(0, 1, labelValues)
	seatDemandIntegrator := fq.NewNamedIntegrator(meal.cfgCtlr.clock, proto.Name)
	seatDemandRatioedGauge := metrics.ApiserverSeatDemands.NewForLabelValuesSafe(0, 1, []string{proto.Name})
	qsCompleter, err := queueSetCompleterForPL(meal.cfgCtlr.queueSetFactory, nil, proto, reqsGaugePair,
		execSeatsObs, metrics.NewUnionGauge(seatDemandIntegrator, seatDemandRatioedGauge))
	if err != nil {
		// 这不可能发生，因为 proto 是必选
		// 对象之一，且它们没有错误
		panic(err)
	}
	meal.newPLStates[proto.Name] = &priorityLevelState{
		pl:                     proto,
		qsCompleter:            qsCompleter,
		reqsGaugePair:          reqsGaugePair,
		execSeatsObs:           execSeatsObs,
		seatDemandIntegrator:   seatDemandIntegrator,
		seatDemandRatioedGauge: seatDemandRatioedGauge,
	}
	nominalConcurrencyShares, _, _ := plSpecCommons(proto)
	meal.shareSum += float64(*nominalConcurrencyShares)
}

// startRequest 对请求进行分类，并在适当时将其入队。
// 当且仅当请求将被拒绝时，返回 nil Request。
// 返回的 bool 表示该请求是否免于
// 限制。startWaitingTime 是请求开始在其队列中
// 等待的时刻，若从未等待则为 `Time{}`。
//
// 只读取配置（RLock），不阻塞在队列上；真正的排队由 StartRequest 负责。
func (cfgCtlr *configController) startRequest(ctx context.Context, rd RequestDigest,
	noteFn func(fs *flowcontrol.FlowSchema, pl *flowcontrol.PriorityLevelConfiguration, flowDistinguisher string),
	workEstimator func() fcrequest.WorkEstimate,
	queueNoteFn fq.QueueNoteFn) (fs *flowcontrol.FlowSchema, pl *flowcontrol.PriorityLevelConfiguration, isExempt bool, req fq.Request, startWaitingTime time.Time) {
	klog.V(7).Infof("startRequest(%#+v)", rd)
	cfgCtlr.lock.RLock()
	defer cfgCtlr.lock.RUnlock()
	var selectedFlowSchema, catchAllFlowSchema *flowcontrol.FlowSchema
	for _, fs := range cfgCtlr.flowSchemas {
		if matchesFlowSchema(rd, fs) {
			selectedFlowSchema = fs
			break
		}
		if fs.Name == flowcontrol.FlowSchemaNameCatchAll {
			catchAllFlowSchema = fs
		}
	}
	if selectedFlowSchema == nil {
		// 这不应该发生。如果 requestDigest 的 User 属于
		// system:authenticated 或 system:unauthenticated，catch-all flow
		// schema 应当能匹配它。但万一那个不变量被打破，
		// 仍然回退到 catch-all flow schema。
		if catchAllFlowSchema == nil {
			// 这绝对、绝对不应该发生！APF 保证任何时刻都存在
			// 两个不可删除的 flow schema：一个 exempt flow schema 和一个
			// catch-all flow schema。
			panic(fmt.Sprintf("no fallback catch-all flow schema found for request %#+v and user %#+v", rd.RequestInfo, rd.User))
		}
		selectedFlowSchema = catchAllFlowSchema
		klog.Warningf("no match found for request %#+v and user %#+v; selecting catchAll=%s as fallback flow schema", rd.RequestInfo, rd.User, fcfmt.Fmt(selectedFlowSchema))
	}
	plName := selectedFlowSchema.Spec.PriorityLevelConfiguration.Name
	plState := cfgCtlr.priorityLevelStates[plName]
	var numQueues int32
	var hashValue uint64
	var flowDistinguisher string
	if plState.pl.Spec.Type != flowcontrol.PriorityLevelEnablementExempt {
		if plState.pl.Spec.Limited.LimitResponse.Type == flowcontrol.LimitResponseTypeQueue {
			numQueues = plState.pl.Spec.Limited.LimitResponse.Queuing.Queues
		}
		if numQueues > 1 {
			flowDistinguisher = computeFlowDistinguisher(rd, selectedFlowSchema.Spec.DistinguisherMethod)
			hashValue = hashFlowID(selectedFlowSchema.Name, flowDistinguisher)
		}
	}

	noteFn(selectedFlowSchema, plState.pl, flowDistinguisher)
	workEstimate := workEstimator()

	if plState.pl.Spec.Type != flowcontrol.PriorityLevelEnablementExempt {
		startWaitingTime = cfgCtlr.clock.Now()
	}
	klog.V(7).Infof("startRequest(%#+v) => fsName=%q, distMethod=%#+v, plName=%q, numQueues=%d", rd, selectedFlowSchema.Name, selectedFlowSchema.Spec.DistinguisherMethod, plName, numQueues)
	req, idle := plState.queues.StartRequest(ctx, &workEstimate, hashValue, flowDistinguisher, selectedFlowSchema.Name, rd.RequestInfo, rd.User, queueNoteFn)
	if idle {
		cfgCtlr.maybeReapReadLocked(plName, plState)
	}
	return selectedFlowSchema, plState.pl, plState.pl.Spec.Type == flowcontrol.PriorityLevelEnablementExempt, req, startWaitingTime
}

// maybeReap 会在具名优先级级别不再被使用时，移除它的
// 最后一点内部痕迹。在获得「该优先级级别
// 已不再需要且空闲」的线索之后调用本函数。
//
// 这里只向配置队列投递一个事件，真正的删除发生在下一次 syncOne。
func (cfgCtlr *configController) maybeReap(plName string) {
	cfgCtlr.lock.RLock()
	defer cfgCtlr.lock.RUnlock()
	plState := cfgCtlr.priorityLevelStates[plName]
	if plState == nil {
		klog.V(7).Infof("plName=%s, plState==nil", plName)
		return
	}
	useless := plState.quiescing && plState.numPending == 0 && plState.queues.IsIdle()
	klog.V(7).Infof("plState.quiescing=%v, plState.numPending=%d, useless=%v", plState.quiescing, plState.numPending, useless)
	if !useless {
		return
	}
	klog.V(3).Infof("Triggered API priority and fairness config reloading because priority level %s is undesired and idle", plName)
	cfgCtlr.configQueue.Add(0)
}

// maybeReapLocked 要求调用方已经持有 cfgCtlr 的锁，
// 并会在具名优先级级别不再被使用时移除它的最后一点
// 内部痕迹。当同时满足 (1) plState.queues 非
// nil 且报告空闲，并且 (2) 此后 cfgCtlr 的锁未曾
// 释放时，调用本函数。
//
// 名字中的 “ReadLocked” 指调用方持有读锁：本函数只读字段并投递事件，
// 不在持锁期间做任何可能阻塞的写操作。
func (cfgCtlr *configController) maybeReapReadLocked(plName string, plState *priorityLevelState) {
	if !(plState.quiescing && plState.numPending == 0) {
		return
	}
	klog.V(3).Infof("Triggered API priority and fairness config reloading because priority level %s is undesired and idle", plName)
	cfgCtlr.configQueue.Add(0)
}

// computeFlowDistinguisher 按给定方法提取 flow distinguisher
//
// 区分符用于把同一条流打散到不同队列，实现 shuffle sharding；method 为 nil
// 表示不做区分（返回空串，所有请求落入同一队列）。
func computeFlowDistinguisher(rd RequestDigest, method *flowcontrol.FlowDistinguisherMethod) string {
	if method == nil {
		return ""
	}
	switch method.Type {
	case flowcontrol.FlowDistinguisherMethodByUserType:
		return rd.User.GetName()
	case flowcontrol.FlowDistinguisherMethodByNamespaceType:
		return rd.RequestInfo.Namespace
	default:
		// 这一行永远不该执行到
		panic("invalid flow-distinguisher method")
	}
}

// hashFlowID 把 (FlowSchema 名, 区分符) 哈希为 64 位值，供队列选择使用。
// 使用 0 字节分隔，避免 "ab"+"c" 与 "a"+"bc" 产生相同输入。
func hashFlowID(fsName, fDistinguisher string) uint64 {
	hash := sha256.New()
	var sep = [1]byte{0}
	hash.Write([]byte(fsName))
	hash.Write(sep[:])
	hash.Write([]byte(fDistinguisher))
	var sum [32]byte
	hash.Sum(sum[:0])
	return binary.LittleEndian.Uint64(sum[:8])
}

// relDiff 返回相对变化量，分母取两者绝对值的较大者；两者都为 0 时定义为 0。
func relDiff(x, y float64) float64 {
	diff := math.Abs(x - y)
	den := math.Max(math.Abs(x), math.Abs(y))
	if den == 0 {
		return 0
	}
	return diff / den
}

// plSpecCommons 返回给定优先级级别配置的（NominalConcurrencyShares、LendablePercent、BorrowingLimitPercent）
//
// Exempt 级别没有 borrowing 概念，BorrowingLimitPercent 恒为 nil；
// NominalConcurrencyShares 未设置时按 0 处理。
func plSpecCommons(pl *flowcontrol.PriorityLevelConfiguration) (*int32, *int32, *int32) {
	if limiter := pl.Spec.Limited; limiter != nil {
		return limiter.NominalConcurrencyShares, limiter.LendablePercent, limiter.BorrowingLimitPercent
	}
	limiter := pl.Spec.Exempt
	var nominalConcurrencyShares int32
	if limiter.NominalConcurrencyShares != nil {
		nominalConcurrencyShares = *limiter.NominalConcurrencyShares
	}
	return &nominalConcurrencyShares, limiter.LendablePercent, nil
}
