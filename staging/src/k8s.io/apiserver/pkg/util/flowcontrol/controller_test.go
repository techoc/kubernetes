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

// Package flowcontrol 实现 API 优先级与公平性（APF）的配置控制器与请求过滤器。
// 本文件是 configController 的测试：用轻量的假 QueueSetFactory 观察配置
// 消化（digest）过程中队列集的生命周期，以及 FlowSchema 的匹配结果。
package flowcontrol

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	flowcontrol "k8s.io/api/flowcontrol/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	fcboot "k8s.io/apiserver/pkg/apis/flowcontrol/bootstrap"
	"k8s.io/apiserver/pkg/util/flowcontrol/debug"
	fq "k8s.io/apiserver/pkg/util/flowcontrol/fairqueuing"
	fcfmt "k8s.io/apiserver/pkg/util/flowcontrol/format"
	"k8s.io/apiserver/pkg/util/flowcontrol/metrics"
	fcrequest "k8s.io/apiserver/pkg/util/flowcontrol/request"
	"k8s.io/client-go/informers"
	clientsetfake "k8s.io/client-go/kubernetes/fake"
	fcclient "k8s.io/client-go/kubernetes/typed/flowcontrol/v1"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
)

// 有些测试会打印大量调试日志，显著拖慢测试甚至导致超时，
// 因此调试日志默认关闭，仅在本地排查匹配或队列问题时临时打开。
var testDebugLogs = false

// TestMain 注册 klog 的命令行标志（测试可用 -v 调整日志级别），然后运行测试集。
func TestMain(m *testing.M) {
	klog.InitFlags(nil)
	os.Exit(m.Run())
}

// mandPLs 按名称索引内置的强制优先级配置。
// 强制配置不可删除，所以测试计算“期望存活的 PL 集合”时总要并入它们。
var mandPLs = func() map[string]*flowcontrol.PriorityLevelConfiguration {
	ans := make(map[string]*flowcontrol.PriorityLevelConfiguration)
	for _, mand := range fcboot.MandatoryPriorityLevelConfigurations {
		ans[mand.Name] = mand
	}
	return ans
}()

// 一般用法下，调用方检查该布尔值时其结果可能已经不准确；
// 加锁是为了避免与 controller 的 reconcile 循环并发读写 priorityLevelStates。
func (cfgCtlr *configController) hasPriorityLevelState(plName string) bool {
	cfgCtlr.lock.Lock()
	defer cfgCtlr.lock.Unlock()
	return cfgCtlr.priorityLevelStates[plName] != nil
}

// ctlrTestState 既是 controller 的测试夹具，也实现了 fq.QueueSetFactory，
// 用最小化的队列集替代真实 fairqueuing 实现，使测试可以精确断言
// 哪些队列集被创建、哪些仍有活跃请求。
type ctlrTestState struct {
	t               *testing.T
	cfgCtlr         *configController
	fcIfc           fcclient.FlowcontrolV1Interface
	existingPLs     map[string]*flowcontrol.PriorityLevelConfiguration
	existingFSs     map[string]*flowcontrol.FlowSchema
	heldRequestsMap map[string][]heldRequest
	requestWG       sync.WaitGroup
	lock            sync.Mutex
	queues          map[string]*ctlrTestQueueSet
}

// heldRequest 表示一个已经进入队列、但被测试故意挂起未完成的请求，
// 用来模拟“配置变更时仍有请求在执行”的场景。
type heldRequest struct {
	rd       RequestDigest
	finishCh chan struct{}
}

// 编译期断言：ctlrTestState 必须满足 QueueSetFactory 接口，
// 这样它才能作为 QueueSetFactory 注入 controller。
var _ fq.QueueSetFactory = (*ctlrTestState)(nil)

// ctlrTestQueueSetCompleter 是构建队列集过程中的中间态，
// 与真实实现一样支持“从零构建”和“原地重配置”两种来源。
type ctlrTestQueueSetCompleter struct {
	cts *ctlrTestState
	cqs *ctlrTestQueueSet
	qc  fq.QueuingConfig
}

// ctlrTestQueueSet 记录某个优先级队列集的活跃请求数，
// IsIdle 与 controller 的“是否可安全删除”判断都依赖 countActive。
type ctlrTestQueueSet struct {
	cts         *ctlrTestState
	qc          fq.QueuingConfig
	dc          fq.DispatchingConfig
	countActive int
}

// ctlrTestRequest 保存请求的描述信息，便于调试日志输出，
// 并在 Finish 时把活跃计数还回去。
type ctlrTestRequest struct {
	cqs            *ctlrTestQueueSet
	qsName         string
	descr1, descr2 interface{}
}

// BeginConstruction 返回一个 completer，Complete 时才真正创建并登记队列集；
// 这与真实工厂一致，保证 controller 先拿到 QueuingConfig 再完成构建。
func (cts *ctlrTestState) BeginConstruction(qc fq.QueuingConfig, rip metrics.RatioedGaugePair, eso metrics.RatioedGauge, sdi metrics.Gauge) (fq.QueueSetCompleter, error) {
	return ctlrTestQueueSetCompleter{cts, nil, qc}, nil
}

// BeginConfigChange 复用已有队列集，仅在 Complete 时替换其配置，
// 这样测试可以验证“配置变更不会丢失排队中的请求”。
func (cqs *ctlrTestQueueSet) BeginConfigChange(qc fq.QueuingConfig) (fq.QueueSetCompleter, error) {
	return ctlrTestQueueSetCompleter{cqs.cts, cqs, qc}, nil
}

// Dump 返回空的队列集快照；调试接口在测试中无意义。
func (cqs *ctlrTestQueueSet) Dump(bool) debug.QueueSetDump {
	return debug.QueueSetDump{}
}

// Complete 登记或更新队列集：首次构建时按名称建表，重配置时原地更新 qc/dc。
// 持有 cts.lock 是因为 controller 可能在另一个 goroutine 中读取队列集。
func (cqc ctlrTestQueueSetCompleter) Complete(dc fq.DispatchingConfig) fq.QueueSet {
	cqc.cts.lock.Lock()
	defer cqc.cts.lock.Unlock()
	qs := cqc.cqs
	if qs == nil {
		qs = &ctlrTestQueueSet{cts: cqc.cts, qc: cqc.qc, dc: dc}
		cqc.cts.queues[cqc.qc.Name] = qs
	} else {
		qs.qc, qs.dc = cqc.qc, dc
	}
	return qs
}

// IsIdle 表示队列集是否可被回收；只有零活跃请求时 controller 才允许删除它。
func (cqs *ctlrTestQueueSet) IsIdle() bool {
	cqs.cts.lock.Lock()
	defer cqs.cts.lock.Unlock()
	klog.V(7).Infof("For %p QS %s, countActive==%d", cqs, cqs.qc.Name, cqs.countActive)
	return cqs.countActive == 0
}

// StartRequest 立刻返回一个未完成的请求并递增活跃计数；
// 第二个返回值恒为 false，因为测试队列集从不让请求等待空闲座位。
func (cqs *ctlrTestQueueSet) StartRequest(ctx context.Context, width *fcrequest.WorkEstimate, hashValue uint64, flowDistinguisher, fsName string, descr1, descr2 interface{}, queueNoteFn fq.QueueNoteFn) (req fq.Request, idle bool) {
	cqs.cts.lock.Lock()
	defer cqs.cts.lock.Unlock()
	cqs.countActive++
	if testDebugLogs {
		cqs.cts.t.Logf("Queued %q %#+v %#+v for %p QS=%s, countActive:=%d", fsName, descr1, descr2, cqs, cqs.qc.Name, cqs.countActive)
	}
	return &ctlrTestRequest{cqs, cqs.qc.Name, descr1, descr2}, false
}

// Finish 执行请求的实际工作并递减活跃计数；返回是否已无活跃请求，
// 对应真实实现中“最后一个请求离开时队列集变空闲”的语义。
func (ctr *ctlrTestRequest) Finish(execute func()) bool {
	execute()
	ctr.cqs.cts.lock.Lock()
	defer ctr.cqs.cts.lock.Unlock()
	ctr.cqs.countActive--
	if testDebugLogs {
		ctr.cqs.cts.t.Logf("Finished %#+v %#+v for %p QS=%s, countActive:=%d", ctr.descr1, ctr.descr2, ctr.cqs, ctr.cqs.qc.Name, ctr.cqs.countActive)
	}
	return ctr.cqs.countActive == 0
}

// getQueueSetNames 返回所有已登记队列集的名称快照（含空闲的）。
func (cts *ctlrTestState) getQueueSetNames() sets.String {
	cts.lock.Lock()
	defer cts.lock.Unlock()
	return sets.StringKeySet(cts.queues)
}

// getNonIdleQueueSetNames 返回仍有活跃请求的队列集名称；
// 这些队列集不允许被回收，因此可用于检测“过早删除”。
func (cts *ctlrTestState) getNonIdleQueueSetNames() sets.String {
	cts.lock.Lock()
	defer cts.lock.Unlock()
	ans := sets.NewString()
	for name, qs := range cts.queues {
		if qs.countActive > 0 {
			ans.Insert(name)
		}
	}
	return ans
}

// hasNonIdleQueueSet 判断指定名称的队列集是否存在且仍有活跃请求。
func (cts *ctlrTestState) hasNonIdleQueueSet(name string) bool {
	cts.lock.Lock()
	defer cts.lock.Unlock()
	qs := cts.queues[name]
	return qs != nil && qs.countActive > 0
}

// addHeldRequest 把一个挂起请求记到其期望的 PL 名下，
// 之后测试再按 PL 是否仍然存在决定何时放行它。
func (cts *ctlrTestState) addHeldRequest(plName string, rd RequestDigest, finishCh chan struct{}) {
	cts.lock.Lock()
	defer cts.lock.Unlock()
	hrs := cts.heldRequestsMap[plName]
	hrs = append(hrs, heldRequest{rd, finishCh})
	cts.heldRequestsMap[plName] = hrs
	if testDebugLogs {
		cts.t.Logf("Holding %#+v for %s, count:=%d", rd, plName, len(hrs))
	}
}

// popHeldRequest 取走任意一个挂起请求（后进先出），并返回该请求所属 PL
// 以及该 PL 名下剩余的挂起请求数。没有挂起请求时返回 nil。
// 返回计数是为了让调试日志能反映该 PL 还剩多少请求未完成。
func (cts *ctlrTestState) popHeldRequest() (plName string, hr *heldRequest, nCount int) {
	cts.lock.Lock()
	defer cts.lock.Unlock()
	var hrs []heldRequest
	for {
		for plName, hrs = range cts.heldRequestsMap {
			goto GotOne
		}
		return "", nil, 0
	GotOne:
		if nhr := len(hrs); nhr > 0 {
			hrv := hrs[nhr-1]
			hrs = hrs[:nhr-1]
			hr = &hrv
		}
		if len(hrs) == 0 {
			delete(cts.heldRequestsMap, plName)
		} else {
			cts.heldRequestsMap[plName] = hrs
		}
		if hr != nil {
			nCount = len(hrs)
			return
		}
	}
}

// mandQueueSetNames 是内置强制 FlowSchema/PL 对应的队列集名称集合，
// 它们在任何一次消化后都必须存在。
var mandQueueSetNames = func() sets.String {
	mandQueueSetNames := sets.NewString()
	for _, mpl := range fcboot.MandatoryPriorityLevelConfigurations {
		mandQueueSetNames.Insert(mpl.Name)
	}
	return mandQueueSetNames
}()

// TestConfigConsumer 是配置控制器的随机化测试：反复生成并消化
// FlowSchema/PL 配置，同时发送请求，验证队列集只增不减到“应当存在”的程度。
// 外层固定种子保证可复现，每个 trial 再从外层 RNG 派生独立种子。
func TestConfigConsumer(t *testing.T) {
	rngOuter := rand.New(rand.NewSource(1234567890123456789))
	for i := 1; i <= 10; i++ {
		rng := rand.New(rand.NewSource(int64(rngOuter.Uint64())))
		t.Run(fmt.Sprintf("trial%d:", i), func(t *testing.T) {
			clientset := clientsetfake.NewSimpleClientset()
			informerFactory := informers.NewSharedInformerFactory(clientset, 0)
			flowcontrolClient := clientset.FlowcontrolV1()
			cts := &ctlrTestState{t: t,
				fcIfc:           flowcontrolClient,
				existingFSs:     map[string]*flowcontrol.FlowSchema{},
				existingPLs:     map[string]*flowcontrol.PriorityLevelConfiguration{},
				heldRequestsMap: map[string][]heldRequest{},
				queues:          map[string]*ctlrTestQueueSet{},
			}
			ctlr := newTestableController(TestableConfig{
				Name:                   "Controller",
				Clock:                  clock.RealClock{},
				AsFieldManager:         ConfigConsumerAsFieldManager,
				FoundToDangling:        func(found bool) bool { return !found },
				InformerFactory:        informerFactory,
				FlowcontrolClient:      flowcontrolClient,
				ServerConcurrencyLimit: 100, // server concurrency limit
				ReqsGaugeVec:           metrics.PriorityLevelConcurrencyGaugeVec,
				ExecSeatsGaugeVec:      metrics.PriorityLevelExecutionSeatsGaugeVec,
				QueueSetFactory:        cts,
			})
			cts.cfgCtlr = ctlr
			persistingPLNames := sets.NewString()
			trialStep := fmt.Sprintf("trial%d-0", i)
			_, _, desiredPLNames, newBadPLNames := genPLs(rng, trialStep, persistingPLNames, 0)
			_, _, newFTRs, newCatchAlls := genFSs(t, rng, trialStep, desiredPLNames, newBadPLNames, 0)
			for j := 0; ; {
				if testDebugLogs {
					t.Logf("For %s, desiredPLNames=%#+v", trialStep, desiredPLNames)
					t.Logf("For %s, newFTRs=%#+v", trialStep, newFTRs)
				}
				// 重新计算“应当存活”的 PL：强制 PL 永不删除，非空闲 PL 因仍有请求
				// 也必须保留，其余加上本步期望的 PL 即为最终集合。
				nextPLNames := sets.NewString()
				for oldPLName := range persistingPLNames {
					if mandPLs[oldPLName] != nil || cts.hasNonIdleQueueSet(oldPLName) {
						nextPLNames.Insert(oldPLName)
					}
				}
				persistingPLNames = nextPLNames.Union(desiredPLNames)
				expectedQueueSetNames := persistingPLNames.Union(mandQueueSetNames)
				allQueueSetNames := cts.getQueueSetNames()
				missingQueueSetNames := expectedQueueSetNames.Difference(allQueueSetNames)
				if len(missingQueueSetNames) > 0 {
					t.Errorf("Fail: missing QueueSets %v", missingQueueSetNames)
				}
				nonIdleQueueSetNames := cts.getNonIdleQueueSetNames()
				extraQueueSetNames := nonIdleQueueSetNames.Difference(expectedQueueSetNames)
				if len(extraQueueSetNames) > 0 {
					t.Errorf("Fail: unexpected QueueSets %v", extraQueueSetNames)
				}
				for plName, hr, nCount := cts.popHeldRequest(); hr != nil; plName, hr, nCount = cts.popHeldRequest() {
					desired := desiredPLNames.Has(plName) || mandPLs[plName] != nil
					if testDebugLogs {
						t.Logf("Releasing held request %#+v, desired=%v, plName=%s, count:=%d", hr.rd, desired, plName, nCount)
					}
					close(hr.finishCh)
				}
				cts.requestWG.Wait()
				for _, ftr := range newFTRs {
					checkNewFS(cts, rng, trialStep, ftr, newCatchAlls)
				}

				j++
				if j > 20 {
					break
				}

				// Calculate expected survivors

				// Now create a new config and digest it
				trialStep = fmt.Sprintf("trial%d-%d", i, j)
				var newPLs []*flowcontrol.PriorityLevelConfiguration
				var newFSs []*flowcontrol.FlowSchema
				newPLs, _, desiredPLNames, newBadPLNames = genPLs(rng, trialStep, persistingPLNames, 1+rng.Intn(4))
				newFSs, _, newFTRs, newCatchAlls = genFSs(t, rng, trialStep, desiredPLNames, newBadPLNames, 1+rng.Intn(6))

				if testDebugLogs {
					for _, newPL := range newPLs {
						t.Logf("For %s, digesting newPL=%s", trialStep, fcfmt.Fmt(newPL))
					}
					for _, newFS := range newFSs {
						t.Logf("For %s, digesting newFS=%s", trialStep, fcfmt.Fmt(newFS))
					}
				}
				_ = ctlr.lockAndDigestConfigObjects(newPLs, newFSs)
			}
			// 收尾：放行所有仍挂起的请求，等待它们退出，避免 goroutine 泄漏。
			for plName, hr, nCount := cts.popHeldRequest(); hr != nil; plName, hr, nCount = cts.popHeldRequest() {
				if testDebugLogs {
					desired := desiredPLNames.Has(plName) || mandPLs[plName] != nil
					t.Logf("Releasing held request %#+v, desired=%v, plName=%s, count:=%d", hr.rd, desired, plName, nCount)
				}
				close(hr.finishCh)
			}
			cts.requestWG.Wait()
		})
	}
}

// TestAPFControllerWithGracefulShutdown 验证 controller 在关闭 stopCh 后
// 能干净地退出 Run：先等到它完成首次 reconcile，再关闭并检查返回错误为 nil。
func TestAPFControllerWithGracefulShutdown(t *testing.T) {
	const plName = "test-ps"
	fs := &flowcontrol.FlowSchema{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-fs",
		},
		Spec: flowcontrol.FlowSchemaSpec{
			MatchingPrecedence: 100,
			PriorityLevelConfiguration: flowcontrol.PriorityLevelConfigurationReference{
				Name: plName,
			},
			DistinguisherMethod: &flowcontrol.FlowDistinguisherMethod{
				Type: flowcontrol.FlowDistinguisherMethodByUserType,
			},
		},
	}

	pl := &flowcontrol.PriorityLevelConfiguration{
		ObjectMeta: metav1.ObjectMeta{
			Name: plName,
		},
		Spec: flowcontrol.PriorityLevelConfigurationSpec{
			Type: flowcontrol.PriorityLevelEnablementLimited,
			Limited: &flowcontrol.LimitedPriorityLevelConfiguration{
				NominalConcurrencyShares: ptr.To(int32(10)),
				LimitResponse: flowcontrol.LimitResponse{
					Type: flowcontrol.LimitResponseTypeReject,
				},
			},
		},
	}

	clientset := clientsetfake.NewSimpleClientset(fs, pl)
	informerFactory := informers.NewSharedInformerFactory(clientset, time.Second)
	flowcontrolClient := clientset.FlowcontrolV1()
	cts := &ctlrTestState{t: t,
		fcIfc:           flowcontrolClient,
		existingFSs:     map[string]*flowcontrol.FlowSchema{},
		existingPLs:     map[string]*flowcontrol.PriorityLevelConfiguration{},
		heldRequestsMap: map[string][]heldRequest{},
		queues:          map[string]*ctlrTestQueueSet{},
	}
	controller := newTestableController(TestableConfig{
		Name:                   "Controller",
		Clock:                  clock.RealClock{},
		AsFieldManager:         ConfigConsumerAsFieldManager,
		FoundToDangling:        func(found bool) bool { return !found },
		InformerFactory:        informerFactory,
		FlowcontrolClient:      flowcontrolClient,
		ServerConcurrencyLimit: 100,
		ReqsGaugeVec:           metrics.PriorityLevelConcurrencyGaugeVec,
		ExecSeatsGaugeVec:      metrics.PriorityLevelExecutionSeatsGaugeVec,
		QueueSetFactory:        cts,
	})

	stopCh, controllerCompletedCh := make(chan struct{}), make(chan struct{})
	var controllerErr error

	// 先启动 informer 并等待缓存同步，否则 controller 的首次 digest 可能看到空配置。
	informerFactory.Start(stopCh)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	status := informerFactory.WaitForCacheSync(ctx.Done())
	if names := unsynced(status); len(names) > 0 {
		t.Fatalf("WaitForCacheSync did not successfully complete, resources=%#v", names)
	}

	// controller.Run 阻塞到 stopCh 关闭，放到独立 goroutine 中并记录错误。
	go func() {
		defer close(controllerCompletedCh)
		controllerErr = controller.Run(stopCh)
	}()

	// ensure that the controller has run its first loop.
	err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		return controller.hasPriorityLevelState(plName), nil
	})
	if err != nil {
		t.Errorf("expected the controller to reconcile the priority level configuration object: %s, error: %s", plName, err)
	}

	// 关闭后必须等待 Run 返回，才能确认退出路径没有报错。
	close(stopCh)
	t.Log("waiting for the controller Run function to shutdown gracefully")
	<-controllerCompletedCh

	if controllerErr != nil {
		t.Errorf("expected nil error from controller Run function, but got: %#v", controllerErr)
	}
}

// unsynced 从 informer 同步结果中挑出尚未同步的资源类型名称，用于生成清晰的失败信息。
func unsynced(status map[reflect.Type]bool) []string {
	names := make([]string, 0)

	for objType, synced := range status {
		if !synced {
			names = append(names, objType.Name())
		}
	}

	return names
}

// checkNewFS 为一个 FlowSchema 发出其所有测试请求，并断言 controller 选中的
// FlowSchema 与优先级配置符合预期。大部分请求立即完成，少量被挂起以制造
// 跨 digest 的活跃请求；startWG 保证所有请求都真正开始执行后才返回。
func checkNewFS(cts *ctlrTestState, rng *rand.Rand, trialName string, ftr *fsTestingRecord, catchAlls map[bool]*flowcontrol.FlowSchema) {
	t := cts.t
	ctlr := cts.cfgCtlr
	fs := ftr.fs
	expectedPLName := fs.Spec.PriorityLevelConfiguration.Name
	ctx := context.Background()
	// Use this to make sure all these requests have started executing
	// before the next reconfiguration
	var startWG sync.WaitGroup
	for matches, digests1 := range ftr.digests {
		for isResource, digests2 := range digests1 {
			for _, rd := range digests2 {
				finishCh := make(chan struct{})
				rdu := uniqify(rd)
				cts.requestWG.Add(1)
				startWG.Add(1)
				go func(matches, isResource bool, rdu RequestDigest) {
					expectedMatch := matches && ftr.wellFormed && (fsPrecedes(fs, catchAlls[isResource]) || fs.Name == catchAlls[isResource].Name)
					ctlr.Handle(ctx, rdu, func(matchFS *flowcontrol.FlowSchema, matchPL *flowcontrol.PriorityLevelConfiguration, _ string) {
						matchIsExempt := matchPL.Spec.Type == flowcontrol.PriorityLevelEnablementExempt
						if testDebugLogs {
							t.Logf("Considering FlowSchema %s, expectedMatch=%v, isResource=%v: Handle(%#+v) => note(fs=%s, pl=%s, isExempt=%v)", fs.Name, expectedMatch, isResource, rdu, matchFS.Name, matchPL.Name, matchIsExempt)
						}
						if a := matchFS.Name == fs.Name; expectedMatch != a {
							t.Errorf("Fail at %s/%s: rd=%#+v, expectedMatch=%v, actualMatch=%v, matchFSName=%q, catchAlls=%#+v", trialName, fs.Name, rdu, expectedMatch, a, matchFS.Name, catchAlls)
						}
						if matchFS.Name == fs.Name {
							if fs.Spec.PriorityLevelConfiguration.Name != matchPL.Name {
								t.Errorf("Fail at %s/%s: expected=%v, actual=%v", trialName, fs.Name, fs.Spec.PriorityLevelConfiguration.Name, matchPL.Name)
							}
						}
					}, func() fcrequest.WorkEstimate {
						return fcrequest.WorkEstimate{InitialSeats: 1}
					}, func(inQueue bool) {
					}, func() {
						startWG.Done()
						_ = <-finishCh
					})
					cts.requestWG.Done()
				}(matches, isResource, rdu)
				// 80% 的请求立即放行；其余挂起，等下一次 digest 时再按 PL 存活情况释放。
				if rng.Float32() < 0.8 {
					if testDebugLogs {
						t.Logf("Immediate request %#+v, plName=%s", rdu, expectedPLName)
					}
					close(finishCh)
				} else {
					cts.addHeldRequest(expectedPLName, rdu, finishCh)
				}
			}
		}
	}
	startWG.Wait()
}

// genPLs 随机生成一批优先级配置，并返回“好的”与“坏的”名称集合：
// 坏名称引用不存在的 PL，用于测试 FlowSchema 指向悬空 PL 时的降级行为。
// 为保持配置在多次 digest 之间部分延续，旧 PL 有 2/3 的概率被重新生成；
// 强制 PL 则以 1/2 的概率被显式包含（即使不包含，controller 也会内置它们）。
func genPLs(rng *rand.Rand, trial string, oldPLNames sets.String, n int) (pls []*flowcontrol.PriorityLevelConfiguration, plMap map[string]*flowcontrol.PriorityLevelConfiguration, goodNames, badNames sets.String) {
	pls = make([]*flowcontrol.PriorityLevelConfiguration, 0, n)
	plMap = make(map[string]*flowcontrol.PriorityLevelConfiguration, n)
	goodNames = sets.NewString()
	badNames = sets.NewString(trial+"-nopl1", trial+"-nopl2")
	addGood := func(pl *flowcontrol.PriorityLevelConfiguration) {
		pls = append(pls, pl)
		plMap[pl.Name] = pl
		goodNames.Insert(pl.Name)
	}
	for i := 1; i <= n; i++ {
		pl := genPL(rng, fmt.Sprintf("%s-pl%d", trial, i))
		addGood(pl)
	}
	for oldPLName := range oldPLNames {
		if _, has := mandPLs[oldPLName]; has {
			continue
		}
		if rng.Float32() < 0.67 {
			pl := genPL(rng, oldPLName)
			addGood(pl)
		}
	}
	for _, pl := range mandPLs {
		if n > 0 && rng.Float32() < 0.5 && !(goodNames.Has(pl.Name) || badNames.Has(pl.Name)) {
			addGood(pl)
		}
	}
	return
}

// genFSs 随机生成一批 FlowSchema，并同时维护“当前优先级最高”的 catch-all
// FlowSchema（区分资源请求与非资源请求），供 checkNewFS 判断匹配优先级。
// 强制 catch-all 与强制 exempt FlowSchema 按概率出现，以覆盖内置配置的交互。
func genFSs(t *testing.T, rng *rand.Rand, trial string, goodPLNames, badPLNames sets.String, n int) (newFSs []*flowcontrol.FlowSchema, newFSMap map[string]*flowcontrol.FlowSchema, newFTRs map[string]*fsTestingRecord, catchAlls map[bool]*flowcontrol.FlowSchema) {
	newFTRs = map[string]*fsTestingRecord{}
	catchAlls = map[bool]*flowcontrol.FlowSchema{
		false: fcboot.MandatoryFlowSchemaCatchAll,
		true:  fcboot.MandatoryFlowSchemaCatchAll}
	newFSMap = map[string]*flowcontrol.FlowSchema{}
	add := func(ftr *fsTestingRecord) {
		newFSs = append(newFSs, ftr.fs)
		newFSMap[ftr.fs.Name] = ftr.fs
		newFTRs[ftr.fs.Name] = ftr
		if ftr.wellFormed {
			if ftr.matchesAllNonResourceRequests && fsPrecedes(ftr.fs, catchAlls[false]) {
				catchAlls[false] = ftr.fs
			}
			if ftr.matchesAllResourceRequests && fsPrecedes(ftr.fs, catchAlls[true]) {
				catchAlls[true] = ftr.fs
			}
		}
		if testDebugLogs {
			t.Logf("For trial %s, adding wf=%v FlowSchema %s", trial, ftr.wellFormed, fcfmt.Fmt(ftr.fs))
		}
	}
	if n == 0 || rng.Float32() < 0.5 {
		add(mandFTRCatchAll)
	}
	for i := 1; i <= n; i++ {
		ftr := genFS(t, rng, fmt.Sprintf("%s-fs%d", trial, i), false, goodPLNames, badPLNames)
		add(ftr)
	}
	if n == 0 || rng.Float32() < 0.5 {
		add(mandFTRExempt)
	}
	return
}

// fsPrecedes 判断 a 是否比 b 更早匹配：先比较 MatchingPrecedence，再按名称字典序
// 打破平局，与 controller 的匹配顺序保持一致。
func fsPrecedes(a, b *flowcontrol.FlowSchema) bool {
	if a.Spec.MatchingPrecedence < b.Spec.MatchingPrecedence {
		return true
	}
	if a.Spec.MatchingPrecedence == b.Spec.MatchingPrecedence {
		return a.Name < b.Name
	}
	return false
}
