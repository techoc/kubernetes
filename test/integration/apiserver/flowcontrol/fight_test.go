/*
Copyright 2021 The Kubernetes Authors.

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
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"

	flowcontrol "k8s.io/api/flowcontrol/v1"
	utilfc "k8s.io/apiserver/pkg/util/flowcontrol"
	fqtesting "k8s.io/apiserver/pkg/util/flowcontrol/fairqueuing/testing"
	"k8s.io/apiserver/pkg/util/flowcontrol/metrics"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/clock"
	testclocks "k8s.io/utils/clock/testing"
)

/*
fightTest 配置一个测试，用于观察 API Priority and Fairness 配置

	controller 在如何设置 FlowSchemaStatus 存在分歧时如何相互争用。
	具体而言，它们设置用于指示对 PriorityLevelConfiguration
	的引用是否完整的 condition。所测试的场景是
	两组 controller，其中一组正常设置该 condition，
	另一组 controller 则设置
	相反的值。

	这是一个行为测试：它实例化这些 controller 并让它们
	近乎正常地运行。测试目标是让 controller
	运行略少于 2 分钟。测试通过读取时钟来获取
	每个 controller 运行时长的上下界，并据此计算
	每个 FlowSchemaStatus 应发生的写入次数的界限。
	测试创建一个 informer 来观察写入。计算出的
	写入次数下界非常宽松，只假设每 10 秒可以完成
	一次写入。
*/
type fightTest struct {
	t              *testing.T
	ctx            context.Context
	loopbackConfig *rest.Config
	teamSize       int
	stopCh         chan struct{}
	now            time.Time
	clk            *testclocks.FakeClock
	ctlrs          map[bool][]utilfc.Interface

	countsMutex sync.Mutex

	// writeCounts 将 FlowSchema.Name 映射到写入次数
	writeCounts map[string]int
}

func newFightTest(t *testing.T, loopbackConfig *rest.Config, teamSize int) *fightTest {
	now := time.Now()
	ft := &fightTest{
		t:              t,
		ctx:            context.Background(),
		loopbackConfig: loopbackConfig,
		teamSize:       teamSize,
		stopCh:         make(chan struct{}),
		now:            now,
		clk:            testclocks.NewFakeClock(now),
		ctlrs: map[bool][]utilfc.Interface{
			false: make([]utilfc.Interface, teamSize),
			true:  make([]utilfc.Interface, teamSize)},
		writeCounts: map[string]int{},
	}
	return ft
}

func (ft *fightTest) createMainInformer() {
	myConfig := rest.CopyConfig(ft.loopbackConfig)
	myConfig = rest.AddUserAgent(myConfig, "audience")
	myClientset := clientset.NewForConfigOrDie(myConfig)
	informerFactory := informers.NewSharedInformerFactory(myClientset, 0)
	inf := informerFactory.Flowcontrol().V1().FlowSchemas().Informer()
	inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			fs := obj.(*flowcontrol.FlowSchema)
			ft.countWrite(fs)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			fs := newObj.(*flowcontrol.FlowSchema)
			ft.countWrite(fs)
		},
	})
	go inf.Run(ft.stopCh)
	if !cache.WaitForCacheSync(ft.stopCh, inf.HasSynced) {
		ft.t.Errorf("Failed to sync main informer cache")
	}
}

func (ft *fightTest) countWrite(fs *flowcontrol.FlowSchema) {
	ft.countsMutex.Lock()
	defer ft.countsMutex.Unlock()
	ft.writeCounts[fs.Name]++
}

func (ft *fightTest) createController(invert bool, i int) {
	fieldMgr := fmt.Sprintf("testController%d%v", i, invert)
	myConfig := rest.CopyConfig(ft.loopbackConfig)
	myConfig = rest.AddUserAgent(myConfig, fieldMgr)
	myClientset := clientset.NewForConfigOrDie(myConfig)
	fcIfc := myClientset.FlowcontrolV1()
	informerFactory := informers.NewSharedInformerFactory(myClientset, 0)
	foundToDangling := func(found bool) bool { return !found }
	if invert {
		foundToDangling = func(found bool) bool { return found }
	}
	ctlr := utilfc.NewTestable(utilfc.TestableConfig{
		Name:                   fieldMgr,
		FoundToDangling:        foundToDangling,
		Clock:                  clock.RealClock{},
		AsFieldManager:         fieldMgr,
		InformerFactory:        informerFactory,
		FlowcontrolClient:      fcIfc,
		ServerConcurrencyLimit: 200, // server 并发限制
		ReqsGaugeVec:           metrics.PriorityLevelConcurrencyGaugeVec,
		ExecSeatsGaugeVec:      metrics.PriorityLevelExecutionSeatsGaugeVec,
		QueueSetFactory:        fqtesting.NewNoRestraintFactory(),
	})
	ft.ctlrs[invert][i] = ctlr
	informerFactory.Start(ft.stopCh)
	go ctlr.Run(ft.stopCh)
}

func (ft *fightTest) evaluate(tBeforeCreate, tAfterCreate time.Time) {
	tBeforeLock := time.Now()
	ft.countsMutex.Lock()
	defer ft.countsMutex.Unlock()
	tAfterLock := time.Now()
	minFightSecs := tBeforeLock.Sub(tAfterCreate).Seconds()
	maxFightSecs := tAfterLock.Sub(tBeforeCreate).Seconds()
	minTotalWrites := int(minFightSecs / 10)
	maxWritesPerWriter := 6 * int(math.Ceil(maxFightSecs/60))
	maxTotalWrites := (1 + ft.teamSize*2) * maxWritesPerWriter
	for flowSchemaName, writeCount := range ft.writeCounts {
		if writeCount < minTotalWrites {
			ft.t.Errorf("There were a total of %d writes to FlowSchema %s but there should have been at least %d from %s to %s", writeCount, flowSchemaName, minTotalWrites, tAfterCreate, tBeforeLock)
		} else if writeCount > maxTotalWrites {
			ft.t.Errorf("There were a total of %d writes to FlowSchema %s but there should have been no more than %d from %s to %s", writeCount, flowSchemaName, maxTotalWrites, tBeforeCreate, tAfterLock)
		} else {
			ft.t.Logf("There were a total of %d writes to FlowSchema %s over %v, %v seconds", writeCount, flowSchemaName, minFightSecs, maxFightSecs)
		}
	}
}
func TestConfigConsumerFight(t *testing.T) {
	_, kubeConfig, closeFn := setup(t, 100, 100)
	defer closeFn()
	const teamSize = 3
	ft := newFightTest(t, kubeConfig, teamSize)
	tBeforeCreate := time.Now()
	ft.createMainInformer()
	ft.foreach(ft.createController)
	tAfterCreate := time.Now()
	time.Sleep(110 * time.Second)
	ft.evaluate(tBeforeCreate, tAfterCreate)
	close(ft.stopCh)
}

func (ft *fightTest) foreach(visit func(invert bool, i int)) {
	for i := 0; i < ft.teamSize; i++ {
		// 以下枚举的顺序不是确定的，
		// 这样很好。
		invert := rand.Intn(2) == 0
		visit(invert, i)
		visit(!invert, i)
	}
}
