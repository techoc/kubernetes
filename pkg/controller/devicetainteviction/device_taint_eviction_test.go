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

package devicetainteviction

// 本文件是设备污点驱逐控制器的单元测试：既覆盖事件处理器与工作队列的确定性状态迁移，
// 也覆盖在 synctest bubble 中真正启动控制器后的完整驱逐流程（含指标验证）。
import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gstruct"
	gomegatypes "github.com/onsi/gomega/types"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	schedulingapi "k8s.io/api/scheduling/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientfeatures "k8s.io/client-go/features"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	core "k8s.io/client-go/testing"
	"k8s.io/component-base/featuregate"
	metricstestutil "k8s.io/component-base/metrics/testutil"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller/devicetainteviction/metrics"
	"k8s.io/kubernetes/pkg/controller/tainteviction"
	controllertestutil "k8s.io/kubernetes/pkg/controller/testutil"
	"k8s.io/kubernetes/pkg/features"
	st "k8s.io/kubernetes/pkg/scheduler/testing"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
)

// init 为整个测试包打开 DRA 相关特性门，并关闭 WatchList 客户端语义。
// 必须在包初始化阶段完成，因为后面的工具函数和测试对象构造都依赖这些特性。
func init() {
	// 这里不能直接调用 Default*Mutable*FeatureGate，否则 hack/verify-test-featuregates.sh 会报错；
	// 通过 utilfeature 的设置接口间接修改，可以让校验脚本识别为测试专用的门控配置。
	if err := utilfeature.DefaultFeatureGate.(featuregate.MutableVersionedFeatureGate).SetFromMap(map[string]bool{
		string(features.DRADeviceTaints):           true,
		string(features.DRADeviceTaintRules):       true,
		string(features.DRAWorkloadResourceClaims): true,
		string(features.GenericWorkload):           true, // DRAWorkloadResourceClaims 的依赖项
	}); err != nil {
		// 特性门配置失败意味着后续测试前提不成立，直接 panic 让测试尽早失败。
		panic(err)
	}

	// 必须关闭 WatchList 语义：我们不仅使用 fake client，还对它做了包装，
	// 这使得 client-go 无法通过 IsWatchListSemanticsUnSupported 自动识别；
	// 与其实现该接口，不如在测试初始化时显式关闭（见下方说明）。
	// 这里也不能用 clientfeaturestesting.SetFeatureDuringTest，因为它需要 testing.TB，
	// 而 init 阶段没有 TB，所以只能自己设置。
	if err := clientfeatures.FeatureGates().(interface {
		Set(clientfeatures.Feature, bool) error
	}).Set(clientfeatures.WatchListClient, false); err != nil {
		panic(err)
	}
}

// 用构造函数封装重复的对象构造，减少样板代码。

// metav1Time 把 time.Time 包装成 metav1.Time 指针，
// 减少在测试数据和期望值中反复构造 &metav1.Time{Time: ...} 的样板代码。
func metav1Time(time time.Time) *metav1.Time {
	return &metav1.Time{Time: time}
}

// ac 构造一个 allocatedClaim 测试值，可选地携带一条驱逐原因（eviction）。
// eviction 使用可变参数是为了让大多数「未受污点影响」的用例无需显式传 nil。
func ac(claim *resourceapi.ResourceClaim, eviction ...*evictionAndReason) allocatedClaim {
	ac := allocatedClaim{
		ResourceClaim: claim,
	}
	switch len(eviction) {
	case 1:
		ac.eviction = eviction[0]
	case 0:
		// 没有驱逐信息是合法的：claim 尚未被判定受污点影响。
	default:
		// 多于一条驱逐信息说明测试用例写错了，立即暴露而不是静默丢弃。
		panic("wrong number of evictions")
	}
	return ac
}

// l 用可变参数构造切片字面量，让表格驱动的测试数据（l(a, b, c)）更紧凑。
func l[T any](items ...T) []T {
	return items
}

// setup 创建一个已安装索引器、可直接调用 handle* 方法的控制器测试上下文。
// 控制器并未启动；队列被替换为 mockQueue，以便精确断言入队/延迟时间及顺序。
func setup(tCtx ktesting.TContext, workloadResourceClaimsEnabled bool) *testContext {
	fakeClientset := fake.NewClientset()
	informerFactory := informers.NewSharedInformerFactory(fakeClientset, 0)
	controller := newWithFeatures(fakeClientset,
		informerFactory.Core().V1().Pods(),
		informerFactory.Resource().V1().ResourceClaims(),
		informerFactory.Resource().V1().ResourceSlices(),
		informerFactory.Resource().V1().DeviceTaintRules(),
		informerFactory.Resource().V1().DeviceClasses(),
		"device-taint-eviction",
		workloadResourceClaimsEnabled,
	)
	tContext := &testContext{
		TContext:        tCtx,
		Controller:      controller,
		recorder:        controllertestutil.NewFakeRecorder(),
		client:          fakeClientset,
		informerFactory: informerFactory,
	}
	// 用 mock 队列替换真实限速队列，才能在测试中精确检查 Ready/Later 状态。
	controller.workqueue = &tContext.mockQueue
	tContext.logger = tCtx.Logger()
	// 事件日志始终写入测试 logger，不受 -v 影响，方便失败时定位事件顺序。
	controller.eventLogger = &tContext.logger
	tContext.Controller.recorder = tContext.recorder

	tCtx.ExpectNoError(controller.addIndexers())

	return tContext
}

// testContext 把控制器、fake 客户端、informer 工厂和测试替身（mock 队列、事件记录器）捆绑在一起。
// 内嵌 Controller 让测试可以直接访问其未导出字段和方法。
type testContext struct {
	ktesting.TContext
	*Controller
	mockQueue       Mock[workItem]
	recorder        *controllertestutil.FakeRecorder
	client          *fake.Clientset
	informerFactory informers.SharedInformerFactory
}

// state 描述测试期望的内部状态快照。
// 各字段与控制器内部缓存一一对应，便于用 assertEqual 做全量比较。
type state struct {
	pods            []*v1.Pod
	allocatedClaims []allocatedClaim
	slices          []*resourceapi.ResourceSlice
	rules           []*resourceapi.DeviceTaintRule
	ruleStats       map[types.UID]taintRuleStats

	// Pod 可能过去曾被入队，而在从 deletePodAt 移除时没有出队；
	// 因此这两个字段的期望状态必须分别描述，不能从一个推导出另一个。

	deletePodAt evictMap
	queued      MockState[workItem]
}

// step 描述处理完就绪工作项后的期望状态以及时间推进量。
type step struct {
	pods        []*v1.Pod
	rules       []*resourceapi.DeviceTaintRule
	ruleStats   map[types.UID]taintRuleStats
	deletePodAt evictMap

	// queuedProcessed 是处理完所有 Ready 项之后队列的期望状态。
	queuedProcessed MockState[workItem]
	// advance 是本步骤要向前推进的虚拟时间，用于让延迟项变为 Ready。
	advance time.Duration
	// queuedShifted 是扣掉 advance 后、剩余延迟项被重新排期时的队列期望状态。
	queuedShifted MockState[workItem]
}

// evictMap 按对象引用记录每个待驱逐 Pod 及其驱逐时间与原因。
type evictMap map[tainteviction.NamespacedObject]evictionAndReason

// allocatedClaimsAsMap 把切片形式的期望 claim 转成以命名空间/名称为键的映射，
// 与控制器内部的 allocatedClaims 缓存结构保持一致，便于直接比较。
func (s state) allocatedClaimsAsMap() map[types.NamespacedName]allocatedClaim {
	claims := make(map[types.NamespacedName]allocatedClaim)
	for _, claim := range s.allocatedClaims {
		claims[types.NamespacedName{Namespace: claim.Namespace, Name: claim.Name}] = claim
	}
	return claims
}

// slicesAsMap 把 ResourceSlice 列表按 driver/pool 聚合为控制器内部的 pool 结构，
// 并计算每个 pool 的最大 Generation（控制器据此判断切片版本新旧）。
func (s state) slicesAsMap() map[poolID]pool {
	pools := make(map[poolID]pool)
	for _, slice := range s.slices {
		id := poolID{driverName: slice.Spec.Driver, poolName: slice.Spec.Pool.Name}
		pool := pools[id]
		if pool.slices == nil {
			pool.slices = make(map[string]*resourceapi.ResourceSlice)
		}
		pool.slices[slice.Name] = slice
		pools[id] = pool
	}
	// pool 的 maxGeneration 由其所有 slice 中最大的 Generation 决定；
	// 用 MinInt64 初始化可保证空 pool 之外的任何代数值都会覆盖它。
	for id, pool := range pools {
		maxGeneration := int64(math.MinInt64)
		for _, slice := range pool.slices {
			if slice.Spec.Pool.Generation > maxGeneration {
				maxGeneration = slice.Spec.Pool.Generation
			}
		}
		pool.maxGeneration = maxGeneration
		pools[id] = pool
	}
	return pools
}

// assertEqual 用 cmp.Diff 而不是 testify 的 spew+diff 来比较值，
// 这样失败时能给出字段级别的完整上下文。
//   - 通过 cmp.Exporter 允许比较未导出字段（测试内部状态多为未导出）；
//   - 通过 EquateEmpty 把 nil 与空 map/slice 视为相等，避免无意义的差异；
//   - what 用于标识比较对象，便于在多处断言中定位失败。
func assertEqual[T any](t interface {
	Helper()
	Errorf(string, ...any)
}, expected, actual T, what string, opts ...cmp.Option) bool {
	t.Helper()

	// Exporter 打开未导出字段比较；EquateEmpty 让空集合的等价形式不产生噪音。
	opts = append(opts, cmp.Exporter(func(reflect.Type) bool { return true }), cmpopts.EquateEmpty())

	if diff := cmp.Diff(expected, actual, opts...); diff != "" {
		t.Errorf("Unexpected diff for %s (-expected, +actual):\n%s", what, diff)
		return false
	}
	return true
}

// testCase 是表格驱动测试的一个用例：给定初始状态与事件序列，断言事件处理后的状态、
// 队列内容以及 Worker 耗空队列后的最终状态与事件。
type testCase struct {
	// workloadResourceClaimsEnabled 控制是否启用 PodGroup 相关的工作负载 claim 支持。
	workloadResourceClaimsEnabled bool

	// initialState 是注入控制器各缓存的初始状态。
	initialState state

	// events 是要喂给 handle* 方法的新旧对象对。
	// 对象可以是 slice、claim 或 pod，用 [add]、[remove]、[update] 生成对象对；
	// 也可以是一个对象对列表，此时按列表出现顺序依次应用（用于构造顺序敏感的场景）。
	events []any

	// finalState 是所有事件处理完毕（尚未处理工作队列）后的期望状态。
	finalState state

	// process 描述对 Ready 工作项的逐轮处理结果，可能有多轮：
	// 每轮结束后推进时间，使延迟项就绪。默认值为 []step{{}}（直接清空队列）。
	process []step

	// wantEvents 是期望记录到的事件列表，比较时会忽略元数据与时间字段。
	wantEvents []*v1.Event
}

// add 生成「新增对象」事件对：旧对象为 nil。
func add[T any](obj *T) [2]*T {
	return [2]*T{nil, obj}
}

// remove 生成「删除对象」事件对：新对象为 nil。
func remove[T any](obj *T) [2]*T {
	return [2]*T{obj, nil}
}

// update 生成「更新对象」事件对，携带更新前后的对象。
func update[T any](oldObj, newObj *T) [2]*T {
	return [2]*T{oldObj, newObj}
}

// 测试用例共享的常量、对象夹具与期望事件。
// 大多数夹具基于同一组节点/驱动/Pod/claim 名称，只改变其中一个维度以隔离行为差异。
var (
	podKind      = v1.SchemeGroupVersion.WithKind("Pod")
	podGroupKind = schedulingapi.SchemeGroupVersion.WithKind("PodGroup")
	nodeName     = "worker"
	nodeName2    = "worker-2"
	driver       = "some-driver"
	podName      = "my-pod"
	podUID       = "1234"
	podGroupName = "my-podgroup"
	podGroupUID  = "5678"
	className    = "my-resource-class"
	resourceName = "my-resource"
	claimName    = podName + "-" + resourceName
	namespace    = "default"
	// taintTime 是 synctest 时间气泡的起始时间。
	// 所有测试都在该气泡内运行，因此污点时间与当前时钟之间的差值是可预测的。
	taintTime  = metav1Time(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC))
	taintKey   = "example.com/taint"
	taintValue = "something"

	// 所有 slice 都使用内部格式；传给 client-go 时会自动转换回 v1 API。

	simpleSlice = st.MakeResourceSlice(nodeName, driver).
			Device("instance").
			Obj()
	slice = st.MakeResourceSlice(nodeName, driver).
		Device("instance").
		Device("instance-no-schedule", resourceapi.DeviceTaint{Key: taintKey, Effect: resourceapi.DeviceTaintEffectNoSchedule}).
		Device("instance-no-execute", resourceapi.DeviceTaint{Key: taintKey, Effect: resourceapi.DeviceTaintEffectNone, TimeAdded: taintTime}, resourceapi.DeviceTaint{Key: taintKey, Effect: resourceapi.DeviceTaintEffectNoExecute, TimeAdded: taintTime}).
		Obj()
	sliceReplaced = func() *resourceapi.ResourceSlice {
		slice := slice.DeepCopy()
		slice.Name += "-updated"
		slice.Spec.Pool.Generation++
		return slice
	}()
	// sliceOtherDevices 把所有设备改名，用于验证「已分配设备不在该 slice 中」时不驱逐。
	sliceOtherDevices = func() *resourceapi.ResourceSlice {
		slice := slice.DeepCopy()
		for i := range slice.Spec.Devices {
			slice.Spec.Devices[i].Name += "-other"
		}
		return slice
	}()
	taint = &resourceapi.DeviceTaint{Key: taintKey, Effect: resourceapi.DeviceTaintEffectNoExecute, Value: taintValue, TimeAdded: taintTime}
	// sliceTainted 给第一个设备加上 NoExecute 污点，是最常用的「触发驱逐」夹具。
	sliceTainted = func() *resourceapi.ResourceSlice {
		slice := slice.DeepCopy()
		slice.Spec.Devices[0].Taints = []resourceapi.DeviceTaint{*taint}
		return slice
	}()
	// sliceTaintedExtended 额外增加一个带相同污点的设备，
	// 用于验证「无关 slice 变化」不会改变已计算的驱逐时间。
	sliceTaintedExtended = func() *resourceapi.ResourceSlice {
		slice := sliceTainted.DeepCopy()
		slice.Spec.Devices = append(slice.Spec.Devices, *slice.Spec.Devices[0].DeepCopy())
		slice.Spec.Devices[len(slice.Spec.Devices)-1].Name += "-other"
		return slice
	}()
	// sliceTaintedNone 把所有污点效果的 Effect 改为 None（仅观测、不驱逐）。
	sliceTaintedNone = func() *resourceapi.ResourceSlice {
		slice := sliceTainted.DeepCopy()
		for i := range slice.Spec.Devices {
			for j := range slice.Spec.Devices[i].Taints {
				slice.Spec.Devices[i].Taints[j].Effect = resourceapi.DeviceTaintEffectNone
			}
		}
		return slice
	}()
	// sliceTaintedUnknown 使用非法 Effect，用于验证控制器忽略未知污点效果。
	sliceTaintedUnknown = func() *resourceapi.ResourceSlice {
		slice := sliceTainted.DeepCopy()
		for i := range slice.Spec.Devices {
			for j := range slice.Spec.Devices[i].Taints {
				slice.Spec.Devices[i].Taints[j].Effect = "unknown-effect"
			}
		}
		return slice
	}()
	// sliceTaintedNoSchedule 把污点降为 NoSchedule：
	// 已有 Pod 不应被驱逐，但驱逐记录应被取消。
	sliceTaintedNoSchedule = func() *resourceapi.ResourceSlice {
		slice := sliceTainted.DeepCopy()
		for i := range slice.Spec.Devices {
			for j := range slice.Spec.Devices[i].Taints {
				slice.Spec.Devices[i].Taints[j].Effect = resourceapi.DeviceTaintEffectNoSchedule
			}
		}
		return slice
	}()
	taintOther = &resourceapi.DeviceTaint{Key: taintKey, Effect: resourceapi.DeviceTaintEffectNoExecute, Value: taintValue + "-other", TimeAdded: taintTime}
	// sliceTaintedValueOther 改变污点 Value，模拟「原污点被容忍、新污点不被容忍」，
	// 用于验证驱逐时间会被重新计算。
	sliceTaintedValueOther = func() *resourceapi.ResourceSlice {
		slice := sliceTainted.DeepCopy()
		slice.Spec.Devices[0].Taints[0] = *taintOther
		return slice
	}()
	taint1 = &resourceapi.DeviceTaint{Key: taintKey, Effect: resourceapi.DeviceTaintEffectNoExecute, TimeAdded: taintTime}
	taint2 = &resourceapi.DeviceTaint{Key: taintKey + "-other", Effect: resourceapi.DeviceTaintEffectNoExecute, TimeAdded: taintTime}
	// sliceTaintedTwice 给同一设备加两个不同的污点，
	// 用于验证部分容忍时按「未被容忍的污点」决定驱逐时间。
	sliceTaintedTwice = func() *resourceapi.ResourceSlice {
		slice := slice.DeepCopy()
		slice.Spec.Devices[0].Taints = []resourceapi.DeviceTaint{*taint1, *taint2}
		return slice
	}()
	// sliceUntainted 去掉 NoSchedule/NoExecute 污点，用于验证污点消失时取消驱逐。
	sliceUntainted = func() *resourceapi.ResourceSlice {
		slice := slice.DeepCopy()
		slice.Spec.Devices[1].Taints = nil
		slice.Spec.Devices[2].Taints = nil
		return slice
	}()
	// slice2 表示另一个节点上的 slice，且 Generation 更高，
	// 用于构造多 pool、以及「高版本 slice 遮蔽低版本」的场景。
	slice2 = func() *resourceapi.ResourceSlice {
		slice := slice.DeepCopy()
		slice.Name += "-2"
		slice.Spec.NodeName = &nodeName2
		slice.Spec.Pool.Name = nodeName2
		slice.Spec.Pool.Generation++
		return slice
	}()
	// ruleEvict 是作用于 driver 的 NoExecute 规则，触发后立即驱逐。
	ruleEvict = &resourceapi.DeviceTaintRule{
		ObjectMeta: metav1.ObjectMeta{
			Name: "evict",
			UID:  "1234",
		},

		Spec: resourceapi.DeviceTaintRuleSpec{
			DeviceSelector: &resourceapi.DeviceTaintSelector{
				Driver: new(driver),
			},
			Taint: resourceapi.DeviceTaint{
				Key:       taint.Key,
				Value:     taint.Value,
				Effect:    resourceapi.DeviceTaintEffect(taint.Effect),
				TimeAdded: taint.TimeAdded,
			},
		},
	}
	// ruleEvictInstance1 只选中名为 instance 的设备，用于多规则归因场景。
	ruleEvictInstance1 = &resourceapi.DeviceTaintRule{
		ObjectMeta: metav1.ObjectMeta{
			Name: "evict-instance",
			UID:  "1234",
		},

		Spec: resourceapi.DeviceTaintRuleSpec{
			DeviceSelector: &resourceapi.DeviceTaintSelector{
				Driver: new(driver),
				Device: new("instance"),
			},
			Taint: resourceapi.DeviceTaint{
				Key:       taint.Key,
				Value:     taint.Value,
				Effect:    resourceapi.DeviceTaintEffect(taint.Effect),
				TimeAdded: taintTime,
			},
		},
	}
	// taintTimeLater 比 taintTime 晚 40 秒，让第二条规则的生效时间落在该时刻。
	taintTimeLater = metav1Time(taintTime.Add(40 * time.Second))
	// ruleEvictInstance2Later 只选中另一设备，且生效时间更晚，
	// 用于观察不同规则触发时间对驱逐归因与队列排期的影响。
	ruleEvictInstance2Later = &resourceapi.DeviceTaintRule{
		ObjectMeta: metav1.ObjectMeta{
			Name: "evict-instance-no-execute",
			UID:  "5678",
		},

		Spec: resourceapi.DeviceTaintRuleSpec{
			DeviceSelector: &resourceapi.DeviceTaintSelector{
				Driver: new(driver),
				Device: new("instance-no-execute"),
			},
			Taint: resourceapi.DeviceTaint{
				Key:       taint.Key,
				Value:     taint.Value,
				Effect:    resourceapi.DeviceTaintEffect(taint.Effect),
				TimeAdded: taintTimeLater,
			},
		},
	}
	// ruleNone 与 ruleEvict 选择器相同，但 Effect 为 None：
	// 只更新规则状态，不产生驱逐。
	ruleNone = &resourceapi.DeviceTaintRule{
		ObjectMeta: metav1.ObjectMeta{
			Name: "evict",
			UID:  "1234",
		},

		Spec: resourceapi.DeviceTaintRuleSpec{
			DeviceSelector: &resourceapi.DeviceTaintSelector{
				Driver: new(driver),
			},
			Taint: resourceapi.DeviceTaint{
				Key:       taint.Key,
				Value:     taint.Value,
				Effect:    resourceapi.DeviceTaintEffectNone,
				TimeAdded: taint.TimeAdded,
			},
		},
	}
	// ruleEvictOther 的 Taint Value 与 ruleEvict 不同，
	// 用于验证多条规则同时促成驱逐时的归因合并。
	ruleEvictOther = &resourceapi.DeviceTaintRule{
		ObjectMeta: metav1.ObjectMeta{
			Name: "evict-other",
			UID:  "1234-other",
		},

		Spec: resourceapi.DeviceTaintRuleSpec{
			DeviceSelector: &resourceapi.DeviceTaintSelector{
				Device: new("instance"),
			},
			Taint: resourceapi.DeviceTaint{
				Key:       taint.Key,
				Value:     taint.Value,
				Effect:    resourceapi.DeviceTaintEffect(taint.Effect),
				TimeAdded: taint.TimeAdded,
			},
		},
	}
	// claim 是未分配、未归属的 claim 基础模板。
	claim = st.MakeResourceClaim().
		Name(claimName).
		Namespace(namespace).
		Request(className).
		Obj()
	// allocationResult 把 instance 设备分配给 req-1，是 claim 的「已分配」状态基础。
	allocationResult = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{{
				Driver:  driver,
				Pool:    nodeName,
				Device:  "instance",
				Request: "req-1",
			}},
		},
	}
	// allocationResultOtherDevices 分配的是另外两个带不同污点的设备，
	// 用于容忍度匹配与多污点场景。
	allocationResultOtherDevices = &resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: []resourceapi.DeviceRequestAllocationResult{
				{
					Driver:  driver,
					Pool:    nodeName,
					Device:  "instance-no-schedule",
					Request: "req-1",
				},
				{
					Driver:  driver,
					Pool:    nodeName,
					Device:  "instance-no-execute",
					Request: "req-1",
				},
			},
		},
	}
	// inUseClaim 是最常用的「Pod 正在使用的 claim」：归属 Pod 且保留给该 Pod。
	inUseClaim = st.FromResourceClaim(claim).
			OwnerReference(podName, podUID, podKind).
			Allocation(allocationResult).
			ReservedForPod(podName, types.UID(podUID)).
			Obj()
	// inUseClaimByPodGroup 归属 PodGroup 而非 Pod，用于工作负载 claim（PodGroup）场景。
	inUseClaimByPodGroup = st.FromResourceClaim(claim).
				OwnerReference(podGroupName, podGroupUID, podGroupKind).
				Allocation(allocationResult).
				ReservedForPodGroup(podGroupName, types.UID(podGroupUID)).
				Obj()
	// inUseClaimOtherNamespace 位于另一个命名空间，且 UID 不同，
	// 用于验证驱逐按命名空间聚合统计事件。
	inUseClaimOtherNamespace = st.FromResourceClaim(claim).
					Namespace(namespace+"-other").
					OwnerReference(podName, podUID+"-2", podKind). // 下方定义了 podWithClaimNameOtherNamespace。
					Allocation(allocationResultOtherDevices).
					ReservedForPod(podName, types.UID(podUID+"-2")).
					Obj()
	// inUseClaimOtherName 名称与 inUseClaim 不同，归属另一个 Pod，
	// 用于验证同一命名空间内多个 Pod 的驱逐统计。
	inUseClaimOtherName = st.FromResourceClaim(claim).
				Name(claimName+"-other").
				OwnerReference(podName+"-other", podUID+"-3", podKind). // 下方定义了 podWithClaimNameOtherName。
				Allocation(allocationResultOtherDevices).
				ReservedForPod(podName+"-other", types.UID(podUID+"-3")).
				Obj()
	// inUseClaimOtherNameShared 同时被两个 Pod 预留，
	// 用于验证一个 Pod 引用多个 claim 时只被驱逐一次。
	inUseClaimOtherNameShared = st.FromResourceClaim(claim).
					Name(claimName+"-other").
					OwnerReference(podName+"-other", podUID+"-3", podKind). // 下方定义了 podWithClaimNameOtherName。
					Allocation(allocationResultOtherDevices).
					ReservedFor(resourceapi.ResourceClaimConsumerReference{Resource: "pods", Name: podName, UID: types.UID(podUID)}, resourceapi.ResourceClaimConsumerReference{Resource: "pods", Name: podName + "-other", UID: types.UID(podUID + "-3")}).
					Obj()
	// tolerationDuration 取 1 小时：确保测试在 synctest bubble 中运行一小时后
	// 也不会走到容忍期终点，从而稳定地区分「立即驱逐」与「延迟驱逐」。
	tolerationDuration = 60 * 60 * time.Second
	// inUseClaimWithToleration 对 taintKey/taintValue 设置 1 小时容忍，
	// 用于验证驱逐被推迟到容忍期结束。
	inUseClaimWithToleration = func() *resourceapi.ResourceClaim {
		claim := inUseClaim.DeepCopy()
		claim.Status.Allocation.Devices.Results[0].Tolerations = []resourceapi.DeviceToleration{{
			Key:               taintKey,
			Operator:          resourceapi.DeviceTolerationOpEqual,
			Value:             taintValue,
			Effect:            resourceapi.DeviceTaintEffectNoExecute,
			TolerationSeconds: new(int64(tolerationDuration.Seconds())),
		}}
		return claim
	}()
	// inUseClaimOld 的 UID 与 Pod 属主引用不匹配，表示「旧属主」的 claim：
	// 控制器不应据此驱逐当前 Pod。
	inUseClaimOld = st.FromResourceClaim(inUseClaim).
			OwnerReference(podName, podUID+"-other", podKind).
			UID("other").
			Obj()
	// unscheduledPodWithClaimName 尚未被调度（无 NodeName），控制器不应驱逐它。
	unscheduledPodWithClaimName = st.MakePod().Name(podName).Namespace(namespace).
					UID(podUID).
					PodResourceClaims(v1.PodResourceClaim{Name: resourceName, ResourceClaimName: &claimName}).
					Obj()
	// podWithClaimName 引用 inUseClaim 且已调度到 nodeName，是最常用的待驱逐 Pod 夹具。
	podWithClaimName = st.MakePod().Name(podName).Namespace(namespace).
				UID(podUID).
				PodResourceClaims(v1.PodResourceClaim{Name: resourceName, ResourceClaimName: &claimName}).
				Node(nodeName).
				Obj()
	// podWithPodGroup 只带 PodGroup 名、不引用 claim，用于验证不会误驱逐同组其它 Pod。
	podWithPodGroup = st.MakePod().Name(podName).Namespace(namespace).
			UID(podUID).
			Node(nodeName).
			PodGroupName(podGroupName).
			Obj()
	// podWithPodGroupClaim 同时引用 claim 和 PodGroup，用于工作负载 claim 的驱逐路径。
	podWithPodGroupClaim = st.MakePod().Name(podName).Namespace(namespace).
				UID(podUID).
				PodResourceClaims(v1.PodResourceClaim{Name: resourceName, ResourceClaimName: &claimName}).
				Node(nodeName).
				PodGroupName(podGroupName).
				Obj()
	// podWithTwoClaimNames 同时引用两个 claim，用于验证多 claim 的驱逐归因去重。
	podWithTwoClaimNames = st.MakePod().Name(podName).Namespace(namespace).
				UID(podUID).
				PodResourceClaims(v1.PodResourceClaim{Name: resourceName, ResourceClaimName: &inUseClaim.Name}).
				PodResourceClaims(v1.PodResourceClaim{Name: resourceName + "-other", ResourceClaimName: &inUseClaimOtherName.Name}).
				Node(nodeName).
				Obj()
	// podWithClaimNameOtherUID 名称相同但 UID 不同，表示 claim 被另一个 Pod 实例使用：
	// 不应驱逐这个「错误」的 Pod。
	podWithClaimNameOtherUID = st.MakePod().Name(podName).Namespace(namespace).
					UID(podUID + "-other").
					PodResourceClaims(v1.PodResourceClaim{Name: resourceName, ResourceClaimName: &claimName}).
					Node(nodeName).
					Obj()
	// podWithClaimNameOtherNamespace 位于另一个命名空间，与同名 claim 不构成引用关系。
	podWithClaimNameOtherNamespace = st.MakePod().Name(podName).Namespace(namespace + "-other").
					UID(podUID + "-2").
					PodResourceClaims(v1.PodResourceClaim{Name: resourceName, ResourceClaimName: &claimName}).
					Node(nodeName).
					Obj()
	// podWithClaimNameOtherName 引用另一名称的 claim，用于验证跨名称引用匹配。
	podWithClaimNameOtherName = st.MakePod().Name(podName + "-other").Namespace(namespace).
					UID(podUID + "-3").
					PodResourceClaims(v1.PodResourceClaim{Name: resourceName, ResourceClaimName: &inUseClaimOtherName.Name}).
					Node(nodeName).
					Obj()
	// podWithClaimTemplate 通过 ResourceClaimTemplate 引用 claim：
	// spec 中只有模板名，实际 claim 名要查 status。
	podWithClaimTemplate = st.MakePod().Name(podName).Namespace(namespace).
				UID(podUID).
				PodResourceClaims(v1.PodResourceClaim{Name: resourceName, ResourceClaimTemplateName: &claimName}).
				Node(nodeName).
				Obj()
	// podWithClaimTemplateInStatus 在 status 中解析出了实际的 claim 名称，
	// 因此控制器能够识别并驱逐它。
	podWithClaimTemplateInStatus = func() *v1.Pod {
		pod := podWithClaimTemplate.DeepCopy()
		pod.Status.ResourceClaimStatuses = []v1.PodResourceClaimStatus{
			{
				Name:              pod.Spec.ResourceClaims[0].Name,
				ResourceClaimName: &claimName,
			},
		}
		return pod
	}()
	// podWithClaimTemplateNoClaimInStatus 的 status 中 claim 名为 nil：
	// 虽然现实中罕见，但语义上表示 Pod 决定不带该 claim 运行。
	podWithClaimTemplateNoClaimInStatus = func() *v1.Pod {
		pod := podWithClaimTemplate.DeepCopy()
		pod.Status.ResourceClaimStatuses = []v1.PodResourceClaimStatus{
			{
				Name: pod.Spec.ResourceClaims[0].Name,
				// 这是合法的：已决定让 Pod 在没有生成对应 claim 的情况下运行；
				// 实践中不会用到，这里仅覆盖该边界情况。
				ResourceClaimName: nil,
			},
		}
		return pod
	}()
	// podWithoutClaimsScheduled 是已调度但不引用任何 claim 的 Pod。
	podWithoutClaimsScheduled = st.MakePod().Name(podName).Namespace(namespace).
					UID(podUID).
					Node(nodeName).
					Obj()
	// podWithExtendedResourceClaimInStatus 通过 extended resource claim status（而非 spec）
	// 引用 claim，用于验证这条补充查找路径。
	podWithExtendedResourceClaimInStatus = func() *v1.Pod {
		pod := podWithoutClaimsScheduled.DeepCopy()
		pod.Status.ExtendedResourceClaimStatus = &v1.PodExtendedResourceClaimStatus{
			ResourceClaimName: claimName,
		}
		return pod
	}()
	// cancelPodEviction 是期望的「取消驱逐」事件夹具。
	cancelPodEviction = &v1.Event{
		InvolvedObject: v1.ObjectReference{
			Kind:       "Pod",
			APIVersion: "v1",
			Namespace:  namespace,
			Name:       podName,
			UID:        types.UID(podUID),
		},
		Reason:  "DeviceTaintManagerEviction",
		Message: "Cancelling deletion",
		Type:    v1.EventTypeNormal,
		Source: v1.EventSource{
			Component: "nodeControllerTest",
		},
		Count: 1,
	}
	// deletePodEvent 是期望的「标记删除」事件夹具。
	deletePodEvent = &v1.Event{
		InvolvedObject: v1.ObjectReference{
			Kind:       "Pod",
			APIVersion: "v1",
			Namespace:  namespace,
			Name:       podName,
			UID:        types.UID(podUID),
		},
		Reason:  "DeviceTaintManagerEviction",
		Message: "Marking for deletion",
		Type:    v1.EventTypeNormal,
		Source: v1.EventSource{
			Component: "nodeControllerTest",
		},
		Count: 1,
	}
	// deletePodEventOtherNamespace 对应另一命名空间中被驱逐 Pod 的事件。
	deletePodEventOtherNamespace = &v1.Event{
		InvolvedObject: v1.ObjectReference{
			Kind:       "Pod",
			APIVersion: "v1",
			Namespace:  namespace + "-other",
			Name:       podName,
			UID:        types.UID(podUID + "-2"),
		},
		Reason:  "DeviceTaintManagerEviction",
		Message: "Marking for deletion",
		Type:    v1.EventTypeNormal,
		Source: v1.EventSource{
			Component: "nodeControllerTest",
		},
		Count: 1,
	}
	// deletePodEventOtherName 对应另一名称 Pod 的事件。
	deletePodEventOtherName = &v1.Event{
		InvolvedObject: v1.ObjectReference{
			Kind:       "Pod",
			APIVersion: "v1",
			Namespace:  namespace,
			Name:       podName + "-other",
			UID:        types.UID(podUID + "-3"),
		},
		Reason:  "DeviceTaintManagerEviction",
		Message: "Marking for deletion",
		Type:    v1.EventTypeNormal,
		Source: v1.EventSource{
			Component: "nodeControllerTest",
		},
		Count: 1,
	}
)

// newEvictionTime 构造 evictionAndReason 测试值。
// args 按三元组 (slice, 设备名, 污点索引) 描述来自 slice 的原因，
// 或按单个 *DeviceTaintRule 描述来自规则的原因。
func newEvictionTime(when *metav1.Time, args ...any) *evictionAndReason {
	// 时间为 nil 表示「没有驱逐」，直接返回 nil 以匹配控制器内部表示。
	if when == nil {
		return nil
	}

	var reason []trackedTaint
	i := 0
	for i < len(args) {
		switch obj := args[i].(type) {
		case *resourceapi.ResourceSlice:
			// slice 带三个参数：slice 对象、设备名、污点索引，必须整体消费。
			reason = append(reason, trackedTaint{slice: sliceDeviceTaint{slice: obj, deviceName: args[i+1].(string), taintIndex: args[i+2].(int)}})
			i += 3
		case *resourceapi.DeviceTaintRule:
			// 规则原因只需要规则本身。
			reason = append(reason, trackedTaint{rule: obj})
			i++
		default:
			// 未知参数类型说明测试数据写错，避免静默跳过导致断言失真。
			panic(fmt.Sprintf("unsupported argument type %T", args[i]))
		}
	}

	return &evictionAndReason{
		when:   *when,
		reason: reason,
	}
}

// newWorkItems 批量构造 Pod/规则引用的工作项，供队列状态断言使用。
func newWorkItems(objs ...metav1.Object) []workItem {
	var items []workItem
	for _, obj := range objs {
		items = append(items, newWorkItem(obj))
	}
	return items
}

// newWorkItem 根据对象类型转换出对应的 workItem（podRef 或 ruleRef）。
func newWorkItem(obj metav1.Object) workItem {
	ref := newObject(obj)
	var item workItem
	switch obj.(type) {
	case *resourceapi.DeviceTaintRule:
		item.ruleRef = ref
	case *v1.Pod:
		item.podRef = ref
	default:
		// 工作队列只处理 Pod 与 DeviceTaintRule，其它类型属于测试书写错误。
		panic(fmt.Sprintf("invalid type %T", obj))
	}
	return item
}

// newDelayedWorkItems 按 (对象, 延迟时间) 成对构造延迟工作项，
// 用于断言 AddAfter 的排期结果。
func newDelayedWorkItems(objsAndDelay ...any) []MockDelayedItem[workItem] {
	var items []MockDelayedItem[workItem]
	for i := 0; i < len(objsAndDelay); i += 2 {
		item := newWorkItem(objsAndDelay[i].(metav1.Object))
		delay := objsAndDelay[i+1].(time.Duration)
		items = append(items, MockDelayedItem[workItem]{item, delay})
	}
	return items
}

// matchDeletionEvent 匹配 Pod 被标记删除的事件。
func matchDeletionEvent() gomegatypes.GomegaMatcher {
	return matchEvent("Marking for deletion")
}

// matchCancellationEvent 匹配驱逐被取消的事件。
func matchCancellationEvent() gomegatypes.GomegaMatcher {
	return matchEvent("Cancelling deletion")
}

// matchEvent 按组件、Reason、消息、计数和 InvolvedObject 精确匹配事件集合。
// 用 ConsistOf 是为了忽略事件顺序，只关心「记录了哪些事件」。
func matchEvent(message string) gomegatypes.GomegaMatcher {
	return gomega.ConsistOf(gstruct.MatchFields(gstruct.IgnoreExtras, gstruct.Fields{
		"Source":  gomega.HaveField("Component", gomega.Equal("device-taint-eviction")),
		"Reason":  gomega.Equal("DeviceTaintManagerEviction"),
		"Message": gomega.Equal(message),
		"Count":   gomega.Equal(int32(1)),
		"InvolvedObject": gomega.Equal(v1.ObjectReference{
			Kind:       "Pod",
			APIVersion: "v1",
			Namespace:  namespace,
			Name:       podName,
			UID:        types.UID(podUID),
		}),
	}))
}

// listEvents 列出集群中的所有事件；失败直接让测试终止，避免后续断言产生误导性错误。
func listEvents(tCtx ktesting.TContext) []v1.Event {
	events, err := tCtx.Client().CoreV1().Events("").List(tCtx, metav1.ListOptions{})
	tCtx.ExpectNoError(err, "list events")
	return events.Items
}

// inProgress 复制规则并设置 EvictionInProgress 条件的期望值。
// 深拷贝是必要的：同一 rule 夹具会被多个用例共享，直接修改会污染其它用例。
func inProgress(rule *resourceapi.DeviceTaintRule, status bool, reason, message string, when *metav1.Time) *resourceapi.DeviceTaintRule {
	rule = rule.DeepCopy()
	condition := metav1.Condition{
		Type:               resourceapi.DeviceTaintConditionEvictionInProgress,
		Status:             metav1.ConditionFalse,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: rule.Generation,
		LastTransitionTime: *when,
	}
	// status 为 false 是默认情况；只有显式为 true 时才改成 ConditionTrue。
	if status {
		condition.Status = metav1.ConditionTrue
	}
	rule.Status.Conditions = []metav1.Condition{condition}
	return rule
}

// TestController 覆盖事件处理逻辑与工作项处理：
//   - 不启动控制器，而是在 synctest bubble 中直接调用 handle* 方法；
//   - 每个用例以已知状态出发，把一组相互独立的事件按所有排列顺序依次应用，
//     最终状态必须在所有排列下一致——模拟 informer 更新被感知的随机顺序；
//   - 随后逐轮处理待办工作，必要时推进时间以到达「Later」队列中的工作项。
func TestController(t *testing.T) { testController(ktesting.Init(t)) }

// testController 执行 TestController 的实际逻辑（接收 ktesting.TContext 以便创建 bubble）。
func testController(tCtx ktesting.TContext) {
	tCtx.Parallel()

	for name, tc := range map[string]testCase{
		"empty": {},
		"populate-pools": {
			events: []any{
				add(slice),
				add(slice2),
			},
			finalState: state{
				slices: l(slice, slice2),
			},
		},
		"update-pools": {
			initialState: state{
				slices: l(slice, slice2),
			},
			events: []any{
				update(slice, sliceUntainted),
				remove(slice2),
			},
			finalState: state{
				slices: l(sliceUntainted),
			},
		},
		"untainted-claim": {
			events: []any{
				add(slice),
				add(slice2),
				add(inUseClaim),
			},
			finalState: state{
				slices:          l(slice, slice2),
				allocatedClaims: l(ac(inUseClaim)),
			},
		},
		"tainted-claim-through-resourceslice": {
			events: []any{
				add(sliceTainted),
				add(slice2),
				add(inUseClaim),
			},
			finalState: state{
				slices:          l(sliceTainted, slice2),
				allocatedClaims: l(ac(inUseClaim, newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
			},
		},
		"rule-status": {
			events: []any{
				add(ruleEvict),
			},
			finalState: state{
				queued: MockState[workItem]{Ready: newWorkItems(ruleEvict)},
			},
			process: []step{{
				rules: l(inProgress(ruleEvict, false, "NotStarted", "", taintTime)),
			}},
		},
		"tainted-claim-through-rule": {
			events: []any{
				add(ruleEvict),
				add(inUseClaim),
			},
			finalState: state{
				allocatedClaims: l(ac(inUseClaim, newEvictionTime(taintTime, ruleEvict))),
				queued:          MockState[workItem]{Ready: newWorkItems(ruleEvict)},
			},
			process: []step{{
				rules: l(inProgress(ruleEvict, false, "NotStarted", "", taintTime)),
			}},
		},
		"evict-pod-resourceclaim": {
			events: []any{
				add(sliceTainted),
				add(slice2),
				add(inUseClaim),
				add(podWithClaimName),
			},
			finalState: state{
				slices:          l(sliceTainted, slice2),
				allocatedClaims: l(ac(inUseClaim, newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
				deletePodAt:     evictMap{newObject(podWithClaimName): *newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0)},
				queued:          MockState[workItem]{Ready: newWorkItems(podWithClaimName)},
			},
			wantEvents: l(deletePodEvent),
		},
		"evict-pod-resourceclaim-podgroup": {
			workloadResourceClaimsEnabled: true,
			events: []any{
				add(sliceTainted),
				add(slice2),
				add(inUseClaimByPodGroup),
				add(podWithPodGroupClaim),
			},
			finalState: state{
				slices:          l(sliceTainted, slice2),
				allocatedClaims: l(ac(inUseClaimByPodGroup, newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
				deletePodAt:     evictMap{newObject(podWithPodGroupClaim): *newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0)},
				queued:          MockState[workItem]{Ready: newWorkItems(podWithPodGroupClaim)},
			},
			wantEvents: l(deletePodEvent),
		},
		"no-evict-pod-in-podgroup-without-claim": {
			workloadResourceClaimsEnabled: true,
			events: []any{
				add(sliceTainted),
				add(slice2),
				add(inUseClaimByPodGroup),
				add(podWithPodGroup),
			},
			finalState: state{
				slices:          l(sliceTainted, slice2),
				allocatedClaims: l(ac(inUseClaimByPodGroup, newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
			},
		},
		"evict-pod-rule": {
			events: []any{
				add(inUseClaim),
				add(podWithClaimName),
				add(ruleEvict),
			},
			finalState: state{
				allocatedClaims: l(ac(inUseClaim, newEvictionTime(taintTime, ruleEvict))),
				deletePodAt:     evictMap{newObject(podWithClaimName): *newEvictionTime(taintTime, ruleEvict)},
				queued:          MockState[workItem]{Ready: newWorkItems(ruleEvict, podWithClaimName), Later: newDelayedWorkItems(ruleEvict, ruleStatusPeriod)},
			},
			process: []step{
				{
					ruleStats: map[types.UID]taintRuleStats{ruleEvict.UID: {numEvictedPods: 1}},
					// 初次更新，构造驱逐前提。
					rules:           l(inProgress(ruleEvict, true, "PodsPendingEviction", "1 pod needs to be evicted in 1 namespace.", taintTime)),
					queuedProcessed: MockState[workItem]{Later: newDelayedWorkItems(ruleEvict, ruleStatusPeriod)},
					advance:         ruleStatusPeriod,
					queuedShifted:   MockState[workItem]{Ready: newWorkItems(ruleEvict)},
				},
				{
					ruleStats: map[types.UID]taintRuleStats{ruleEvict.UID: {numEvictedPods: 1}},
					// 最终更新，验证驱逐后的状态。
					rules: l(inProgress(ruleEvict, false, "Completed", "1 pod evicted since starting the controller.", metav1Time(taintTime.Add(ruleStatusPeriod)))),
				},
			},
			wantEvents: l(deletePodEvent),
		},
		"evict-pod-rule-later": {
			events: []any{
				add(inUseClaimWithToleration),
				add(podWithClaimName),
				add(ruleEvict),
			},
			finalState: state{
				allocatedClaims: l(ac(inUseClaimWithToleration, newEvictionTime(metav1Time(taintTime.Add(tolerationDuration)), ruleEvict))),
				deletePodAt:     evictMap{newObject(podWithClaimName): *newEvictionTime(metav1Time(taintTime.Add(tolerationDuration)), ruleEvict)},
				queued:          MockState[workItem]{Ready: newWorkItems(ruleEvict), Later: newDelayedWorkItems(podWithClaimName, tolerationDuration, ruleEvict, ruleStatusPeriod)},
			},
			process: []step{
				{
					// 初次更新，构造驱逐前提。
					deletePodAt:     evictMap{newObject(podWithClaimName): *newEvictionTime(metav1Time(taintTime.Add(tolerationDuration)), ruleEvict)},
					pods:            l(podWithClaimName),
					rules:           l(inProgress(ruleEvict, true, "PodsPendingEviction", "1 pod needs to be evicted in 1 namespace.", taintTime)),
					queuedProcessed: MockState[workItem]{Later: newDelayedWorkItems(podWithClaimName, tolerationDuration, ruleEvict, ruleStatusPeriod)},
					advance:         ruleStatusPeriod,
					queuedShifted:   MockState[workItem]{Ready: newWorkItems(ruleEvict), Later: newDelayedWorkItems(podWithClaimName, tolerationDuration-ruleStatusPeriod)},
				},
				{
					// 处理 Pod 驱逐，验证工作队列执行结果。
					deletePodAt:     evictMap{newObject(podWithClaimName): *newEvictionTime(metav1Time(taintTime.Add(tolerationDuration)), ruleEvict)},
					pods:            l(podWithClaimName),
					rules:           l(inProgress(ruleEvict, true, "PodsPendingEviction", "1 pod needs to be evicted in 1 namespace.", metav1Time(taintTime.Add(ruleStatusPeriod)))),
					queuedProcessed: MockState[workItem]{Later: newDelayedWorkItems(podWithClaimName, tolerationDuration-ruleStatusPeriod)},
					advance:         tolerationDuration - ruleStatusPeriod,
					queuedShifted:   MockState[workItem]{Ready: newWorkItems(podWithClaimName)},
				},
				{
					// Pod 已删除，但 condition 尚未更新。
					ruleStats:       map[types.UID]taintRuleStats{ruleEvict.UID: {numEvictedPods: 1}},
					rules:           l(inProgress(ruleEvict, true, "PodsPendingEviction", "1 pod needs to be evicted in 1 namespace.", metav1Time(taintTime.Add(ruleStatusPeriod)))),
					queuedProcessed: MockState[workItem]{Later: newDelayedWorkItems(ruleEvict, ruleStatusPeriod)},
					advance:         ruleStatusPeriod,
					queuedShifted:   MockState[workItem]{Ready: newWorkItems(ruleEvict)},
				},
				{
					// 最终更新，验证驱逐后的状态。
					ruleStats: map[types.UID]taintRuleStats{ruleEvict.UID: {numEvictedPods: 1}},
					rules:     l(inProgress(ruleEvict, false, "Completed", "1 pod evicted since starting the controller.", metav1Time(taintTime.Add(tolerationDuration+ruleStatusPeriod)))),
				},
			},
			wantEvents: l(deletePodEvent),
		},
		"evict-many-pods-many-namespaces": {
			events: []any{
				add(inUseClaim),
				add(podWithClaimName),
				add(inUseClaimOtherNamespace),
				add(podWithClaimNameOtherNamespace),
				add(ruleEvict),
			},
			finalState: state{
				allocatedClaims: l(ac(inUseClaim, newEvictionTime(taintTime, ruleEvict)), ac(inUseClaimOtherNamespace, newEvictionTime(taintTime, ruleEvict))),
				deletePodAt:     evictMap{newObject(podWithClaimName): *newEvictionTime(taintTime, ruleEvict), newObject(podWithClaimNameOtherNamespace): *newEvictionTime(taintTime, ruleEvict)},
				queued:          MockState[workItem]{Ready: newWorkItems(ruleEvict, podWithClaimName, podWithClaimNameOtherNamespace), Later: newDelayedWorkItems(ruleEvict, ruleStatusPeriod)},
			},
			process: []step{
				{
					ruleStats: map[types.UID]taintRuleStats{ruleEvict.UID: {numEvictedPods: 2}},
					// 初次更新，构造驱逐前提。
					rules:           l(inProgress(ruleEvict, true, "PodsPendingEviction", "2 pods need to be evicted in 2 different namespaces.", taintTime)),
					queuedProcessed: MockState[workItem]{Later: newDelayedWorkItems(ruleEvict, ruleStatusPeriod)},
					advance:         ruleStatusPeriod,
					queuedShifted:   MockState[workItem]{Ready: newWorkItems(ruleEvict)},
				},
				{
					ruleStats: map[types.UID]taintRuleStats{ruleEvict.UID: {numEvictedPods: 2}},
					// 最终更新，验证驱逐后的状态。
					rules: l(inProgress(ruleEvict, false, "Completed", "2 pods evicted since starting the controller.", metav1Time(taintTime.Add(ruleStatusPeriod)))),
				},
			},
			wantEvents: l(deletePodEvent, deletePodEventOtherNamespace),
		},
		"evict-many-pods-same-namespace": {
			events: []any{
				add(inUseClaim),
				add(podWithClaimName),
				add(inUseClaimOtherName),
				add(podWithClaimNameOtherName),
				add(ruleEvict),
			},
			finalState: state{
				allocatedClaims: l(ac(inUseClaim, newEvictionTime(taintTime, ruleEvict)), ac(inUseClaimOtherName, newEvictionTime(taintTime, ruleEvict))),
				deletePodAt:     evictMap{newObject(podWithClaimName): *newEvictionTime(taintTime, ruleEvict), newObject(podWithClaimNameOtherName): *newEvictionTime(taintTime, ruleEvict)},
				queued:          MockState[workItem]{Ready: newWorkItems(ruleEvict, podWithClaimName, podWithClaimNameOtherName), Later: newDelayedWorkItems(ruleEvict, ruleStatusPeriod)},
			},
			process: []step{
				{
					ruleStats: map[types.UID]taintRuleStats{ruleEvict.UID: {numEvictedPods: 2}},
					// 初次更新，构造驱逐前提。
					rules:           l(inProgress(ruleEvict, true, "PodsPendingEviction", "2 pods need to be evicted in 1 namespace.", taintTime)),
					queuedProcessed: MockState[workItem]{Later: newDelayedWorkItems(ruleEvict, ruleStatusPeriod)},
					advance:         ruleStatusPeriod,
					queuedShifted:   MockState[workItem]{Ready: newWorkItems(ruleEvict)},
				},
				{
					ruleStats: map[types.UID]taintRuleStats{ruleEvict.UID: {numEvictedPods: 2}},
					// 最终更新，验证驱逐后的状态。
					rules: l(inProgress(ruleEvict, false, "Completed", "2 pods evicted since starting the controller.", metav1Time(taintTime.Add(ruleStatusPeriod)))),
				},
			},
			wantEvents: l(deletePodEvent, deletePodEventOtherName),
		},
		"none-pod-rule": {
			events: []any{
				add(slice),
				add(inUseClaim),
				add(podWithClaimName),
				add(ruleNone),
			},
			finalState: state{
				slices:          l(slice),
				allocatedClaims: l(ac(inUseClaim)),
				queued:          MockState[workItem]{Ready: newWorkItems(ruleNone)},
			},
			process: []step{
				{
					pods:  l(podWithClaimName),
					rules: l(inProgress(ruleNone, false, "NoEffect", "3 published devices selected. 1 allocated device selected. 1 pod would be evicted in 1 namespace if the effect was NoExecute. This information will not be updated again. Recreate the DeviceTaintRule to trigger an update.", taintTime)),
				},
			},
		},
		"none-many-pods-many-namespaces": {
			events: []any{
				add(inUseClaim),
				add(podWithClaimName),
				add(inUseClaimOtherNamespace),
				add(podWithClaimNameOtherNamespace),
				add(ruleNone),
			},
			finalState: state{
				allocatedClaims: l(ac(inUseClaim), ac(inUseClaimOtherNamespace)),
				queued:          MockState[workItem]{Ready: newWorkItems(ruleNone)},
			},
			process: []step{
				{
					pods:  l(podWithClaimName, podWithClaimNameOtherNamespace),
					rules: l(inProgress(ruleNone, false, "NoEffect", "0 published devices selected. 3 allocated devices selected. 2 pods would be evicted in 2 different namespaces if the effect was NoExecute. This information will not be updated again. Recreate the DeviceTaintRule to trigger an update.", taintTime)),
				},
			},
		},
		"none-many-pods-same-namespace": {
			events: []any{
				add(inUseClaim),
				add(podWithClaimName),
				add(inUseClaimOtherName),
				add(podWithClaimNameOtherName),
				add(ruleNone),
			},
			finalState: state{
				allocatedClaims: l(ac(inUseClaim), ac(inUseClaimOtherName)),
				queued:          MockState[workItem]{Ready: newWorkItems(ruleNone)},
			},
			process: []step{
				{
					pods:  l(podWithClaimName, podWithClaimNameOtherName),
					rules: l(inProgress(ruleNone, false, "NoEffect", "0 published devices selected. 3 allocated devices selected. 2 pods would be evicted in 1 namespace if the effect was NoExecute. This information will not be updated again. Recreate the DeviceTaintRule to trigger an update.", taintTime)),
				},
			},
		},
		"none-podgroup-rule": {
			workloadResourceClaimsEnabled: true,
			events: []any{
				add(slice),
				add(inUseClaimByPodGroup),
				add(podWithPodGroupClaim),
				add(ruleNone),
			},
			finalState: state{
				slices:          l(slice),
				allocatedClaims: l(ac(inUseClaimByPodGroup)),
				queued:          MockState[workItem]{Ready: newWorkItems(ruleNone)},
			},
			process: []step{
				{
					pods:  l(podWithPodGroupClaim),
					rules: l(inProgress(ruleNone, false, "NoEffect", "3 published devices selected. 1 allocated device selected. 1 pod would be evicted in 1 namespace if the effect was NoExecute. This information will not be updated again. Recreate the DeviceTaintRule to trigger an update.", taintTime)),
				},
			},
		},
		"multiple-claims-same-rule": {
			events: []any{
				add(inUseClaim),
				add(inUseClaimOtherNameShared),
				add(podWithTwoClaimNames),
				add(ruleEvict),
			},
			finalState: state{
				allocatedClaims: l(ac(inUseClaim, newEvictionTime(taintTime, ruleEvict)), ac(inUseClaimOtherNameShared, newEvictionTime(taintTime, ruleEvict))),
				deletePodAt:     evictMap{newObject(podWithTwoClaimNames): *newEvictionTime(taintTime, ruleEvict)},
				queued:          MockState[workItem]{Ready: newWorkItems(ruleEvict, podWithTwoClaimNames), Later: newDelayedWorkItems(ruleEvict, ruleStatusPeriod)},
			},
			process: []step{
				{
					ruleStats: map[types.UID]taintRuleStats{ruleEvict.UID: {numEvictedPods: 1}},
					// 初次更新，构造驱逐前提。
					rules:           l(inProgress(ruleEvict, true, "PodsPendingEviction", "1 pod needs to be evicted in 1 namespace.", taintTime)),
					queuedProcessed: MockState[workItem]{Later: newDelayedWorkItems(ruleEvict, ruleStatusPeriod)},
					advance:         ruleStatusPeriod,
					queuedShifted:   MockState[workItem]{Ready: newWorkItems(ruleEvict)},
				},
				{
					ruleStats: map[types.UID]taintRuleStats{ruleEvict.UID: {numEvictedPods: 1}},
					// 最终更新，验证驱逐后的状态。
					rules: l(inProgress(ruleEvict, false, "Completed", "1 pod evicted since starting the controller.", metav1Time(taintTime.Add(ruleStatusPeriod)))),
				},
			},
			wantEvents: l(deletePodEvent),
		},
		"multiple-claims-different-rules-order-1": {
			// // 两条规则触发驱逐的时间不同：第二条规则在稍晚的时刻才加入。
			// // 此时驱逐是否应同时归因于两条规则值得商榷；当前实现认为是，
			// // 理由是只要时间足够，第二条规则最终也会触发驱逐。
			// //
			// // 此处的事件顺序很重要：工作队列的内容取决于先观察到哪条规则，
			// // 见下一个用例。测试比较 `queued` 状态时不考虑顺序，因此队列实际顺序
			// // 在测试中不可见。通常这无关紧要，但这里确实有影响。
			events: []any{
				[]any{
					add(inUseClaim),
					add(inUseClaimOtherNameShared),
					add(podWithTwoClaimNames),
					add(ruleEvictInstance1),
					add(ruleEvictInstance2Later),
				},
			},
			finalState: state{
				allocatedClaims: l(ac(inUseClaim, newEvictionTime(taintTime, ruleEvictInstance1)), ac(inUseClaimOtherNameShared, newEvictionTime(taintTimeLater, ruleEvictInstance2Later))),
				deletePodAt:     evictMap{newObject(podWithTwoClaimNames): *newEvictionTime(taintTime, ruleEvictInstance1, ruleEvictInstance2Later)},
				queued:          MockState[workItem]{Ready: newWorkItems(ruleEvictInstance1, ruleEvictInstance2Later, podWithTwoClaimNames), Later: newDelayedWorkItems(ruleEvictInstance1, ruleStatusPeriod, ruleEvictInstance2Later, ruleStatusPeriod)},
			},
			process: []step{
				{
					ruleStats: map[types.UID]taintRuleStats{ruleEvictInstance1.UID: {numEvictedPods: 1}, ruleEvictInstance2Later.UID: {numEvictedPods: 1}},
					// 先更新 ruleEvictInstance1，再更新触发时间更晚的 ruleEvictInstance2Later。
					rules:           l(inProgress(ruleEvictInstance1, true, "PodsPendingEviction", "1 pod needs to be evicted in 1 namespace.", taintTime), inProgress(ruleEvictInstance2Later, false, "Completed", "1 pod evicted since starting the controller.", taintTime)),
					queuedProcessed: MockState[workItem]{Later: newDelayedWorkItems(ruleEvictInstance1, ruleStatusPeriod, ruleEvictInstance2Later, ruleStatusPeriod)},
					advance:         ruleStatusPeriod,
					queuedShifted:   MockState[workItem]{Ready: newWorkItems(ruleEvictInstance1, ruleEvictInstance2Later)},
				},
				{
					ruleStats: map[types.UID]taintRuleStats{ruleEvict.UID: {numEvictedPods: 1}, ruleEvictInstance2Later.UID: {numEvictedPods: 1}},
					// 最终更新，验证驱逐后的状态。
					rules: l(inProgress(ruleEvictInstance1, false, "Completed", "1 pod evicted since starting the controller.", metav1Time(taintTime.Add(ruleStatusPeriod))), inProgress(ruleEvictInstance2Later, false, "Completed", "1 pod evicted since starting the controller.", metav1Time(taintTime.Add(ruleStatusPeriod)))),
				},
			},
			wantEvents: l(deletePodEvent),
		},
		"multiple-claims-different-rules-order-2": {
			events: []any{
				[]any{
					add(inUseClaim),
					add(inUseClaimOtherNameShared),
					add(podWithTwoClaimNames),
					add(ruleEvictInstance2Later), // 顺序反转后，Pod 会被安排延迟驱逐，且无法取消。
					add(ruleEvictInstance1),
				},
			},
			finalState: state{
				allocatedClaims: l(ac(inUseClaim, newEvictionTime(taintTime, ruleEvictInstance1)), ac(inUseClaimOtherNameShared, newEvictionTime(taintTimeLater, ruleEvictInstance2Later))),
				deletePodAt:     evictMap{newObject(podWithTwoClaimNames): *newEvictionTime(taintTime, ruleEvictInstance1, ruleEvictInstance2Later)},
				queued:          MockState[workItem]{Ready: newWorkItems(ruleEvictInstance1, ruleEvictInstance2Later, podWithTwoClaimNames), Later: newDelayedWorkItems(podWithTwoClaimNames, ruleEvictInstance2Later.Spec.Taint.TimeAdded.Sub(taintTime.Time), ruleEvictInstance2Later, ruleStatusPeriod, ruleEvictInstance1, ruleStatusPeriod)},
			},
			process: []step{
				// Pod 的驱逐时间被安排得较晚，需要多次推进时间才会被处理。
				{
					ruleStats: map[types.UID]taintRuleStats{ruleEvictInstance1.UID: {numEvictedPods: 1}, ruleEvictInstance2Later.UID: {numEvictedPods: 1}},
					// 驱逐前先更新两条规则。
					rules:           l(inProgress(ruleEvictInstance1, true, "PodsPendingEviction", "1 pod needs to be evicted in 1 namespace.", taintTime), inProgress(ruleEvictInstance2Later, true, "PodsPendingEviction", "1 pod needs to be evicted in 1 namespace.", taintTime)),
					queuedProcessed: MockState[workItem]{Later: newDelayedWorkItems(podWithTwoClaimNames, ruleEvictInstance2Later.Spec.Taint.TimeAdded.Sub(taintTime.Time), ruleEvictInstance2Later, ruleStatusPeriod, ruleEvictInstance1, ruleStatusPeriod)},
					advance:         ruleStatusPeriod,
					queuedShifted:   MockState[workItem]{Ready: newWorkItems(ruleEvictInstance1, ruleEvictInstance2Later), Later: newDelayedWorkItems(podWithTwoClaimNames, ruleEvictInstance2Later.Spec.Taint.TimeAdded.Sub(taintTime.Time)-ruleStatusPeriod)},
				},
				{
					ruleStats: map[types.UID]taintRuleStats{ruleEvict.UID: {numEvictedPods: 1}, ruleEvictInstance2Later.UID: {numEvictedPods: 1}},
					// 最终更新，验证驱逐后的状态。
					rules:           l(inProgress(ruleEvictInstance1, false, "Completed", "1 pod evicted since starting the controller.", metav1Time(taintTime.Add(ruleStatusPeriod))), inProgress(ruleEvictInstance2Later, false, "Completed", "1 pod evicted since starting the controller.", metav1Time(taintTime.Add(ruleStatusPeriod)))),
					queuedProcessed: MockState[workItem]{Later: newDelayedWorkItems(podWithTwoClaimNames, ruleEvictInstance2Later.Spec.Taint.TimeAdded.Sub(taintTime.Time)-ruleStatusPeriod)},
					advance:         ruleEvictInstance2Later.Spec.Taint.TimeAdded.Sub(taintTime.Time) - ruleStatusPeriod,
					queuedShifted:   MockState[workItem]{Ready: newWorkItems(podWithTwoClaimNames)},
				},
				{
					ruleStats: map[types.UID]taintRuleStats{ruleEvict.UID: {numEvictedPods: 1}, ruleEvictInstance2Later.UID: {numEvictedPods: 1}},
					rules:     l(inProgress(ruleEvictInstance1, false, "Completed", "1 pod evicted since starting the controller.", metav1Time(taintTime.Add(ruleStatusPeriod))), inProgress(ruleEvictInstance2Later, false, "Completed", "1 pod evicted since starting the controller.", metav1Time(taintTime.Add(ruleStatusPeriod)))),
					// Pod 被直接从工作队列移除，无需进一步处理。
				},
			},
			wantEvents: l(deletePodEvent),
		},
		"evict-pod-resourceclaim-again": {
			initialState: state{
				pods:            l(podWithClaimName),
				slices:          l(sliceTainted, slice2),
				allocatedClaims: l(ac(inUseClaim, newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
				deletePodAt:     evictMap{newObject(podWithClaimName): *newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0)},
			},
			events: []any{
				[]any{remove(sliceTainted), add(sliceTainted)},
			},
			finalState: state{
				slices:          l(sliceTainted, slice2),
				allocatedClaims: l(ac(inUseClaim, newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
				deletePodAt:     evictMap{newObject(podWithClaimName): *newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0)},
				queued:          MockState[workItem]{Ready: newWorkItems(podWithClaimName)},
			},
			// // 控制器是否应快速响应 slice 变化（此处为删除）值得商榷：
			// // 一方面污点消失时希望尽快取消驱逐；另一方面，它也可能恢复此前状态并发出事件，
			// // 正如本用例所示。
			// //
			// // 目前代码能够可靠地立即取消驱逐。
			wantEvents: l(cancelPodEviction, deletePodEvent),
		},
		"evict-pod-after-scheduling": {
			initialState: state{
				pods:            l(unscheduledPodWithClaimName),
				slices:          l(sliceTainted, slice2),
				allocatedClaims: l(ac(inUseClaim, newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
			},
			events: []any{
				// 正常情况下带污点的节点不应被调度，
				// 但调度器可能尚未感知到该污点。
				update(unscheduledPodWithClaimName, podWithClaimName),
			},
			finalState: state{
				slices:          l(sliceTainted, slice2),
				allocatedClaims: l(ac(inUseClaim, newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
				deletePodAt:     evictMap{newObject(podWithClaimName): *newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0)},
				queued:          MockState[workItem]{Ready: newWorkItems(podWithClaimName)},
			},
			wantEvents: l(deletePodEvent),
		},
		"evict-pod-resourceclaim-unrelated-changes": {
			initialState: state{
				pods:            l(podWithClaimName),
				slices:          l(sliceTainted, slice2),
				allocatedClaims: l(ac(inUseClaim, newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
				deletePodAt:     evictMap{newObject(podWithClaimName): *newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0)},
			},
			events: []any{
				update(sliceTainted, sliceTaintedExtended),
				update(inUseClaim, inUseClaim),             // 无实际变化，足以覆盖部分代码路径。
				update(podWithClaimName, podWithClaimName), // 同上。
			},
			finalState: state{
				slices:          l(sliceTaintedExtended, slice2),
				allocatedClaims: l(ac(inUseClaim, newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
				deletePodAt:     evictMap{newObject(podWithClaimName): *newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0)},
				queued:          MockState[workItem]{Ready: newWorkItems(podWithClaimName)},
			},
			wantEvents: l(deletePodEvent),
		},
		"evict-pod-resourceclaimtemplate": {
			events: []any{
				add(sliceTainted),
				add(slice2),
				add(inUseClaim),
				add(podWithClaimTemplateInStatus),
			},
			finalState: state{
				slices:          l(sliceTainted, slice2),
				allocatedClaims: l(ac(inUseClaim, newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
				deletePodAt:     evictMap{newObject(podWithClaimTemplateInStatus): *newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0)},
				queued:          MockState[workItem]{Ready: newWorkItems(podWithClaimTemplateInStatus)},
			},
			wantEvents: l(deletePodEvent),
		},
		"evict-pod-extended-resourceclaim": {
			events: []any{
				add(sliceTainted),
				add(slice2),
				add(inUseClaim),
				add(podWithExtendedResourceClaimInStatus),
			},
			finalState: state{
				slices:          l(sliceTainted, slice2),
				allocatedClaims: l(ac(inUseClaim, newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
				deletePodAt:     evictMap{newObject(podWithExtendedResourceClaimInStatus): *newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0)},
				queued:          MockState[workItem]{Ready: newWorkItems(podWithExtendedResourceClaimInStatus)},
			},
			wantEvents: l(deletePodEvent),
		},
		"evict-pod-extended-resourceclaim-status-update": {
			initialState: state{
				pods:            l(podWithoutClaimsScheduled),
				slices:          l(sliceTainted, slice2),
				allocatedClaims: l(ac(inUseClaim, newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
			},
			events: []any{
				update(podWithoutClaimsScheduled, podWithExtendedResourceClaimInStatus),
			},
			finalState: state{
				slices:          l(sliceTainted, slice2),
				allocatedClaims: l(ac(inUseClaim, newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
				deletePodAt:     evictMap{newObject(podWithExtendedResourceClaimInStatus): *newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0)},
				queued:          MockState[workItem]{Ready: newWorkItems(podWithExtendedResourceClaimInStatus)},
			},
			wantEvents: l(deletePodEvent),
		},
		"no-evict-extended-resourceclaim-wrong-owner": {
			events: []any{
				add(sliceTainted),
				add(slice2),
				add(inUseClaimOld), // Pod 不是属主
				add(podWithExtendedResourceClaimInStatus),
			},
			finalState: state{
				slices:          l(sliceTainted, slice2),
				allocatedClaims: l(ac(inUseClaimOld, newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
			},
		},
		"evict-pod-later": {
			events: []any{
				add(sliceTainted),
				add(slice2),
				add(inUseClaimWithToleration),
				add(podWithClaimName),
			},
			finalState: state{
				slices:          l(sliceTainted, slice2),
				allocatedClaims: l(ac(inUseClaimWithToleration, newEvictionTime(metav1Time(taintTime.Add(tolerationDuration)), sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
				deletePodAt:     evictMap{newObject(podWithClaimName): *newEvictionTime(metav1Time(taintTime.Add(tolerationDuration)), sliceTainted, sliceTainted.Spec.Devices[0].Name, 0)},
				queued:          MockState[workItem]{Later: newDelayedWorkItems(podWithClaimName, tolerationDuration)},
			},
			process: []step{
				// 先推进时间到驱逐点，再执行删除。
				{
					deletePodAt:     evictMap{newObject(podWithClaimName): *newEvictionTime(metav1Time(taintTime.Add(tolerationDuration)), sliceTainted, sliceTainted.Spec.Devices[0].Name, 0)},
					pods:            l(podWithClaimName),
					queuedProcessed: MockState[workItem]{Later: newDelayedWorkItems(podWithClaimName, tolerationDuration)},
					advance:         tolerationDuration,
					queuedShifted:   MockState[workItem]{Ready: newWorkItems(podWithClaimName)},
				},
				{},
			},
			wantEvents: l(deletePodEvent),
		},
		"evict-pod-later-many": {
			events: []any{
				add(sliceTainted),
				add(slice2),
				add(func() *resourceapi.ResourceClaim {
					claim := inUseClaim.DeepCopy()
					claim.Status.Allocation.Devices.Results[0].Tolerations = []resourceapi.DeviceToleration{
						{
							Key:               taintKey + "-other",
							Operator:          resourceapi.DeviceTolerationOpEqual,
							Value:             taintValue,
							Effect:            resourceapi.DeviceTaintEffectNoExecute,
							TolerationSeconds: new(int64(20)),
						},
						{
							Operator:          resourceapi.DeviceTolerationOpExists,
							Effect:            resourceapi.DeviceTaintEffectNoExecute,
							TolerationSeconds: new(int64(60)),
						},
						{
							Operator:          resourceapi.DeviceTolerationOpExists,
							Effect:            resourceapi.DeviceTaintEffectNoExecute,
							TolerationSeconds: new(int64(30)),
						},
					}
					return claim
				}()),
				add(podWithClaimName),
			},
			finalState: state{
				slices: l(sliceTainted, slice2),
				allocatedClaims: l(ac(func() *resourceapi.ResourceClaim {
					claim := inUseClaim.DeepCopy()
					claim.Status.Allocation.Devices.Results[0].Tolerations = []resourceapi.DeviceToleration{
						{
							Key:               taintKey + "-other",
							Operator:          resourceapi.DeviceTolerationOpEqual,
							Value:             taintValue,
							Effect:            resourceapi.DeviceTaintEffectNoExecute,
							TolerationSeconds: new(int64(20)),
						},
						{
							Operator:          resourceapi.DeviceTolerationOpExists,
							Effect:            resourceapi.DeviceTaintEffectNoExecute,
							TolerationSeconds: new(int64(60)),
						},
						{
							Operator:          resourceapi.DeviceTolerationOpExists,
							Effect:            resourceapi.DeviceTaintEffectNoExecute,
							TolerationSeconds: new(int64(30)),
						},
					}
					return claim
				}(), newEvictionTime(metav1Time(taintTime.Add(30*time.Second)), sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
				deletePodAt: evictMap{newObject(podWithClaimName): *newEvictionTime(metav1Time(taintTime.Add(30*time.Second)), sliceTainted, sliceTainted.Spec.Devices[0].Name, 0)},
				queued:      MockState[workItem]{Later: newDelayedWorkItems(podWithClaimName, 30*time.Second)},
			},
			process: []step{
				// 先推进时间到驱逐点，再执行删除。
				{
					deletePodAt:     evictMap{newObject(podWithClaimName): *newEvictionTime(metav1Time(taintTime.Add(30*time.Second)), sliceTainted, sliceTainted.Spec.Devices[0].Name, 0)},
					pods:            l(podWithClaimName),
					queuedProcessed: MockState[workItem]{Later: newDelayedWorkItems(podWithClaimName, 30*time.Second)},
					advance:         30 * time.Second,
					queuedShifted:   MockState[workItem]{Ready: newWorkItems(podWithClaimName)},
				},
				{},
			},
			wantEvents: l(deletePodEvent),
		},
		"evict-pod-toleration-mismatch": {
			events: []any{
				add(sliceTainted),
				add(slice2),
				add(func() *resourceapi.ResourceClaim {
					claim := inUseClaim.DeepCopy()
					claim.Status.Allocation.Devices.Results[0].Tolerations = []resourceapi.DeviceToleration{{
						Key:               taintKey + "-other",
						Operator:          resourceapi.DeviceTolerationOpEqual,
						Value:             taintValue,
						Effect:            resourceapi.DeviceTaintEffectNoExecute,
						TolerationSeconds: new(int64(60)),
					}}
					return claim
				}()),
				add(podWithClaimName),
			},
			finalState: state{
				slices: l(sliceTainted, slice2),
				allocatedClaims: l(ac(func() *resourceapi.ResourceClaim {
					claim := inUseClaim.DeepCopy()
					claim.Status.Allocation.Devices.Results[0].Tolerations = []resourceapi.DeviceToleration{{
						Key:               taintKey + "-other",
						Operator:          resourceapi.DeviceTolerationOpEqual,
						Value:             taintValue,
						Effect:            resourceapi.DeviceTaintEffectNoExecute,
						TolerationSeconds: new(int64(60)),
					}}
					return claim
				}(), newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
				deletePodAt: evictMap{newObject(podWithClaimName): *newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0)},
				queued:      MockState[workItem]{Ready: newWorkItems(podWithClaimName)},
			},
			wantEvents: l(deletePodEvent),
		},
		"no-evict-pod-toleration-forever": {
			events: []any{
				add(sliceTainted),
				add(slice2),
				add(func() *resourceapi.ResourceClaim {
					claim := inUseClaim.DeepCopy()
					claim.Status.Allocation.Devices.Results[0].Tolerations = []resourceapi.DeviceToleration{{
						Operator: resourceapi.DeviceTolerationOpExists,
						Effect:   resourceapi.DeviceTaintEffectNoExecute,
					}}
					return claim
				}()),
				add(podWithClaimName),
			},
			finalState: state{
				slices: l(sliceTainted, slice2),
				allocatedClaims: l(ac(func() *resourceapi.ResourceClaim {
					claim := inUseClaim.DeepCopy()
					claim.Status.Allocation.Devices.Results[0].Tolerations = []resourceapi.DeviceToleration{{
						Operator: resourceapi.DeviceTolerationOpExists,
						Effect:   resourceapi.DeviceTaintEffectNoExecute,
					}}
					return claim
				}())),
			},
			process: []step{{pods: l(podWithClaimName)}},
		},
		"no-evict-pod-toleration-forever-many": {
			events: []any{
				add(sliceTainted),
				add(slice2),
				add(func() *resourceapi.ResourceClaim {
					claim := inUseClaim.DeepCopy()
					claim.Status.Allocation.Devices.Results[0].Tolerations = []resourceapi.DeviceToleration{
						{
							Operator:          resourceapi.DeviceTolerationOpExists,
							Effect:            resourceapi.DeviceTaintEffectNoExecute,
							TolerationSeconds: new(int64(60)),
						},
						{
							Operator: resourceapi.DeviceTolerationOpExists,
							Effect:   resourceapi.DeviceTaintEffectNoExecute,
						},
					}
					return claim
				}()),
				add(podWithClaimName),
			},
			finalState: state{
				slices: l(sliceTainted, slice2),
				allocatedClaims: l(ac(func() *resourceapi.ResourceClaim {
					claim := inUseClaim.DeepCopy()
					claim.Status.Allocation.Devices.Results[0].Tolerations = []resourceapi.DeviceToleration{
						{
							Operator:          resourceapi.DeviceTolerationOpExists,
							Effect:            resourceapi.DeviceTaintEffectNoExecute,
							TolerationSeconds: new(int64(60)),
						},
						{
							Operator: resourceapi.DeviceTolerationOpExists,
							Effect:   resourceapi.DeviceTaintEffectNoExecute,
						},
					}
					return claim
				}())),
			},
			process: []step{{pods: l(podWithClaimName)}},
		},
		"evict-pod-partial-toleration": {
			events: []any{
				add(sliceTaintedTwice),
				add(slice2),
				add(func() *resourceapi.ResourceClaim {
					claim := inUseClaim.DeepCopy()
					claim.Status.Allocation.Devices.Results[0].Tolerations = []resourceapi.DeviceToleration{{
						Operator: resourceapi.DeviceTolerationOpExists,
						Key:      taint1.Key,
						Effect:   resourceapi.DeviceTaintEffectNoExecute,
					}}
					return claim
				}()),
				add(podWithClaimName),
			},
			finalState: state{
				slices: l(sliceTaintedTwice, slice2),
				allocatedClaims: l(ac(func() *resourceapi.ResourceClaim {
					claim := inUseClaim.DeepCopy()
					claim.Status.Allocation.Devices.Results[0].Tolerations = []resourceapi.DeviceToleration{{
						Operator: resourceapi.DeviceTolerationOpExists,
						Key:      taint1.Key,
						Effect:   resourceapi.DeviceTaintEffectNoExecute,
					}}
					return claim
				}(), newEvictionTime(taintTime, sliceTaintedTwice, sliceTainted.Spec.Devices[0].Name, 1))),
				deletePodAt: evictMap{newObject(podWithClaimName): *newEvictionTime(taintTime, sliceTaintedTwice, sliceTainted.Spec.Devices[0].Name, 1)},
				queued:      MockState[workItem]{Ready: newWorkItems(podWithClaimName)},
			},
			wantEvents: l(deletePodEvent),
		},
		"evict-pod-many-taints": {
			events: []any{
				add(ruleEvict),
				add(ruleEvictOther),
				add(sliceTaintedTwice),
				add(slice2),
				add(func() *resourceapi.ResourceClaim {
					claim := inUseClaim.DeepCopy()
					claim.Status.Allocation.Devices.Results[0].Tolerations = []resourceapi.DeviceToleration{
						{
							Operator:          resourceapi.DeviceTolerationOpExists,
							Key:               taint1.Key,
							Effect:            resourceapi.DeviceTaintEffectNoExecute,
							TolerationSeconds: new(int64(60)),
						},
						{
							Operator:          resourceapi.DeviceTolerationOpExists,
							Key:               taint2.Key,
							Effect:            resourceapi.DeviceTaintEffectNoExecute,
							TolerationSeconds: new(int64(30)),
						},
					}
					return claim
				}()),
				add(podWithClaimName),
			},
			finalState: state{
				slices: l(sliceTaintedTwice, slice2),
				allocatedClaims: l(ac(func() *resourceapi.ResourceClaim {
					claim := inUseClaim.DeepCopy()
					claim.Status.Allocation.Devices.Results[0].Tolerations = []resourceapi.DeviceToleration{
						{
							Operator:          resourceapi.DeviceTolerationOpExists,
							Key:               taint1.Key,
							Effect:            resourceapi.DeviceTaintEffectNoExecute,
							TolerationSeconds: new(int64(60)),
						},
						{
							Operator:          resourceapi.DeviceTolerationOpExists,
							Key:               taint2.Key,
							Effect:            resourceapi.DeviceTaintEffectNoExecute,
							TolerationSeconds: new(int64(30)),
						},
					}
					return claim
				}(), newEvictionTime(metav1Time(taintTime.Add(30*time.Second)), ruleEvict, ruleEvictOther, sliceTaintedTwice, sliceTaintedTwice.Spec.Devices[0].Name, 0, sliceTaintedTwice, sliceTaintedTwice.Spec.Devices[0].Name, 1))),
				deletePodAt: evictMap{newObject(podWithClaimName): *newEvictionTime(metav1Time(taintTime.Add(30*time.Second)), ruleEvict, ruleEvictOther, sliceTaintedTwice, sliceTaintedTwice.Spec.Devices[0].Name, 0, sliceTaintedTwice, sliceTaintedTwice.Spec.Devices[0].Name, 1)},
				queued:      MockState[workItem]{Ready: newWorkItems(ruleEvict, ruleEvictOther), Later: newDelayedWorkItems(podWithClaimName, 30*time.Second, ruleEvict, ruleStatusPeriod, ruleEvictOther, ruleStatusPeriod)},
			},
			process: []step{
				{
					deletePodAt:     evictMap{newObject(podWithClaimName): *newEvictionTime(metav1Time(taintTime.Add(30*time.Second)), ruleEvict, ruleEvictOther, sliceTaintedTwice, sliceTaintedTwice.Spec.Devices[0].Name, 0, sliceTaintedTwice, sliceTaintedTwice.Spec.Devices[0].Name, 1)},
					pods:            l(podWithClaimName),
					rules:           l(inProgress(ruleEvict, true, "PodsPendingEviction", "1 pod needs to be evicted in 1 namespace.", taintTime), inProgress(ruleEvictOther, true, "PodsPendingEviction", "1 pod needs to be evicted in 1 namespace.", taintTime)),
					queuedProcessed: MockState[workItem]{Later: newDelayedWorkItems(podWithClaimName, 30*time.Second, ruleEvict, ruleStatusPeriod, ruleEvictOther, ruleStatusPeriod)},
					advance:         ruleStatusPeriod,
					queuedShifted:   MockState[workItem]{Ready: newWorkItems(ruleEvict, ruleEvictOther), Later: newDelayedWorkItems(podWithClaimName, 30*time.Second-ruleStatusPeriod)},
				},
				// 先推进时间到驱逐点，再执行删除。
				{
					deletePodAt:     evictMap{newObject(podWithClaimName): *newEvictionTime(metav1Time(taintTime.Add(30*time.Second)), ruleEvict, ruleEvictOther, sliceTaintedTwice, sliceTaintedTwice.Spec.Devices[0].Name, 0, sliceTaintedTwice, sliceTaintedTwice.Spec.Devices[0].Name, 1)},
					pods:            l(podWithClaimName),
					rules:           l(inProgress(ruleEvict, true, "PodsPendingEviction", "1 pod needs to be evicted in 1 namespace.", metav1Time(taintTime.Add(ruleStatusPeriod))), inProgress(ruleEvictOther, true, "PodsPendingEviction", "1 pod needs to be evicted in 1 namespace.", metav1Time(taintTime.Add(ruleStatusPeriod)))),
					queuedProcessed: MockState[workItem]{Later: newDelayedWorkItems(podWithClaimName, 20*time.Second)},
					advance:         20 * time.Second,
					queuedShifted:   MockState[workItem]{Ready: newWorkItems(podWithClaimName)},
				},
				{
					ruleStats: map[types.UID]taintRuleStats{ruleEvict.UID: {numEvictedPods: 1}, ruleEvictOther.UID: {numEvictedPods: 1}},
					// 尚未更新。
					rules:           l(inProgress(ruleEvict, true, "PodsPendingEviction", "1 pod needs to be evicted in 1 namespace.", metav1Time(taintTime.Add(ruleStatusPeriod))), inProgress(ruleEvictOther, true, "PodsPendingEviction", "1 pod needs to be evicted in 1 namespace.", metav1Time(taintTime.Add(ruleStatusPeriod)))),
					queuedProcessed: MockState[workItem]{Later: newDelayedWorkItems(ruleEvict, ruleStatusPeriod, ruleEvictOther, ruleStatusPeriod)},
					advance:         ruleStatusPeriod,
					queuedShifted:   MockState[workItem]{Ready: newWorkItems(ruleEvict, ruleEvictOther)},
				},
				{
					ruleStats: map[types.UID]taintRuleStats{ruleEvict.UID: {numEvictedPods: 1}, ruleEvictOther.UID: {numEvictedPods: 1}},

					// 最终更新，验证驱逐后的状态。
					rules: l(inProgress(ruleEvict, false, "Completed", "1 pod evicted since starting the controller.", metav1Time(taintTime.Add(30*time.Second+ruleStatusPeriod))), inProgress(ruleEvictOther, false, "Completed", "1 pod evicted since starting the controller.", metav1Time(taintTime.Add(30*time.Second+ruleStatusPeriod)))),
				},
			},
			wantEvents: l(deletePodEvent),
		},
		"no-evict-pod-not-scheduled": {
			events: []any{
				add(sliceTainted),
				add(slice2),
				add(inUseClaim),
				add(func() *v1.Pod {
					pod := podWithClaimName.DeepCopy()
					pod.Spec.NodeName = ""
					return pod
				}()),
			},
			finalState: state{
				slices:          l(sliceTainted, slice2),
				allocatedClaims: l(ac(inUseClaim, newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
			},
		},
		"no-evict-no-taint": {
			events: []any{
				add(simpleSlice),
				add(inUseClaim),
				add(podWithClaimName),
			},
			finalState: state{
				slices:          l(simpleSlice),
				allocatedClaims: l(ac(inUseClaim)),
			},
		},
		"no-evict-no-taint-update": {
			initialState: state{
				pods:            l(podWithClaimName),
				slices:          l(simpleSlice),
				allocatedClaims: l(ac(inUseClaim)),
			},
			events: []any{
				update(simpleSlice, simpleSlice),
			},
			finalState: state{
				slices:          l(simpleSlice),
				allocatedClaims: l(ac(inUseClaim)),
			},
		},
		"no-evict-pod-resourceclaimtemplate-no-status": {
			events: []any{
				add(sliceTainted),
				add(slice2),
				add(inUseClaim),
				add(podWithClaimTemplate),
			},
			finalState: state{
				slices:          l(sliceTainted, slice2),
				allocatedClaims: l(ac(inUseClaim, newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
			},
		},
		"no-evict-pod-resourceclaimtemplate-no-claim": {
			// // 为 Pod 生成了 claim 却又不列出该 claim 并不合理，但为完整性仍覆盖该场景。
			events: []any{
				add(sliceTainted),
				add(slice2),
				add(inUseClaim),
				add(podWithClaimTemplateNoClaimInStatus),
			},
			finalState: state{
				slices:          l(sliceTainted, slice2),
				allocatedClaims: l(ac(inUseClaim, newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
			},
		},
		"no-evict-other-device": {
			events: []any{
				add(sliceOtherDevices),
				add(slice2),
				add(inUseClaim),
				add(podWithClaimTemplateInStatus),
			},
			finalState: state{
				slices:          l(sliceOtherDevices, slice2),
				allocatedClaims: l(ac(inUseClaim)),
			},
		},
		"no-evict-wrong-pod": {
			events: []any{
				add(sliceTainted),
				add(slice2),
				add(inUseClaim),
				add(podWithClaimNameOtherUID),
			},
			finalState: state{
				slices:          l(sliceTainted, slice2),
				allocatedClaims: l(ac(inUseClaim, newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
			},
		},
		"evict-wrong-pod-replaced": {
			initialState: state{
				pods:            l(podWithClaimNameOtherUID),
				slices:          l(sliceTainted, slice2),
				allocatedClaims: l(ac(inUseClaim, newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
			},
			events: []any{
				[]any{remove(podWithClaimNameOtherUID), add(podWithClaimName)},
			},
			finalState: state{
				slices:          l(sliceTainted, slice2),
				allocatedClaims: l(ac(inUseClaim, newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
				deletePodAt:     evictMap{newObject(podWithClaimName): *newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0)},
				queued:          MockState[workItem]{Ready: newWorkItems(podWithClaimName)},
			},
			wantEvents: l(deletePodEvent),
		},
		"cancel-eviction-remove-taint": {
			initialState: state{
				pods:            l(podWithClaimTemplateInStatus),
				slices:          l(sliceTainted, slice2),
				allocatedClaims: l(ac(inUseClaim, newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
				deletePodAt:     evictMap{newObject(podWithClaimTemplateInStatus): *newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0)},
			},
			events: []any{
				update(sliceTainted, slice),
			},
			finalState: state{
				slices:          l(slice, slice2),
				allocatedClaims: l(ac(inUseClaim)),
			},
			wantEvents: l(cancelPodEviction),
		},
		"cancel-eviction-reduce-taint": {
			initialState: state{
				pods:            l(podWithClaimTemplateInStatus),
				slices:          l(sliceTainted, slice2),
				allocatedClaims: l(ac(inUseClaim, newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
				deletePodAt:     evictMap{newObject(podWithClaimTemplateInStatus): *newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0)},
			},
			events: []any{
				update(sliceTainted, sliceTaintedNoSchedule),
			},
			finalState: state{
				slices:          l(sliceTaintedNoSchedule, slice2),
				allocatedClaims: l(ac(inUseClaim)),
			},
			wantEvents: l(cancelPodEviction),
		},
		"ignore-none-effect": {
			initialState: state{
				pods:            l(podWithClaimTemplateInStatus),
				slices:          l(sliceTaintedNone, slice2),
				allocatedClaims: l(ac(inUseClaim)),
			},
			finalState: state{
				slices:          l(sliceTaintedNone, slice2),
				allocatedClaims: l(ac(inUseClaim)),
			},
		},
		"ignore-unknown-effect": {
			initialState: state{
				pods:            l(podWithClaimTemplateInStatus),
				slices:          l(sliceTaintedUnknown, slice2),
				allocatedClaims: l(ac(inUseClaim)),
			},
			finalState: state{
				slices:          l(sliceTaintedUnknown, slice2),
				allocatedClaims: l(ac(inUseClaim)),
			},
		},
		"eviction-change-taint": {
			initialState: state{
				pods:            l(podWithClaimTemplateInStatus),
				slices:          l(sliceTainted, slice2),
				allocatedClaims: l(ac(inUseClaimWithToleration, newEvictionTime(metav1Time(taintTime.Add(tolerationDuration)), sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
				deletePodAt:     evictMap{newObject(podWithClaimTemplateInStatus): *newEvictionTime(metav1Time(taintTime.Add(tolerationDuration)), sliceTainted, sliceTainted.Spec.Devices[0].Name, 0)},
			},
			events: []any{
				// 从可容忍 60 秒的污点切换到不容忍的污点。
				update(sliceTainted, sliceTaintedValueOther),
			},
			finalState: state{
				slices:          l(sliceTaintedValueOther, slice2),
				allocatedClaims: l(ac(inUseClaimWithToleration, newEvictionTime(taintTime, sliceTaintedValueOther, sliceTainted.Spec.Devices[0].Name, 0))),
				deletePodAt:     evictMap{newObject(podWithClaimTemplateInStatus): *newEvictionTime(taintTime, sliceTaintedValueOther, sliceTainted.Spec.Devices[0].Name, 0)},
				queued:          MockState[workItem]{Ready: newWorkItems(podWithClaimName)},
			},
			wantEvents: l(deletePodEvent),
		},
		"cancel-eviction-remove-taint-in-new-slice": {
			initialState: state{
				pods:            l(podWithClaimTemplateInStatus),
				slices:          l(sliceTainted, slice2),
				allocatedClaims: l(ac(inUseClaim, newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
				deletePodAt:     evictMap{newObject(podWithClaimTemplateInStatus): *newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0)},
			},
			events: []any{
				// 把使用中的设备迁移到另一个 slice，同时移除原污点。
				remove(sliceTainted),
				add(sliceReplaced),
			},
			finalState: state{
				slices:          l(sliceReplaced, slice2),
				allocatedClaims: l(ac(inUseClaim)),
			},
			wantEvents: l(cancelPodEviction),
		},
		"cancel-eviction-remove-slice": {
			initialState: state{
				pods:            l(podWithClaimTemplateInStatus),
				slices:          l(sliceTainted, slice2),
				allocatedClaims: l(ac(inUseClaim, newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
				deletePodAt:     evictMap{newObject(podWithClaimTemplateInStatus): *newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0)},
			},
			events: []any{
				remove(sliceTainted),
			},
			finalState: state{
				slices:          l(slice2),
				allocatedClaims: l(ac(inUseClaim)),
			},
			wantEvents: l(cancelPodEviction),
		},
		"cancel-eviction-pod-deletion": {
			initialState: state{
				pods:   l(podWithClaimName),
				slices: l(sliceTainted, slice2),
				allocatedClaims: l(ac(func() *resourceapi.ResourceClaim {
					claim := inUseClaim.DeepCopy()
					claim.Status.Allocation.Devices.Results[0].Tolerations = []resourceapi.DeviceToleration{{
						Operator:          resourceapi.DeviceTolerationOpExists,
						Effect:            resourceapi.DeviceTaintEffectNoExecute,
						TolerationSeconds: new(int64(60)),
					}}
					return claim
				}(), newEvictionTime(metav1Time(taintTime.Add(60*time.Second))))),
				deletePodAt: evictMap{newObject(podWithClaimName): *newEvictionTime(metav1Time(taintTime.Add(60 * time.Second)))},
			},
			events: []any{
				remove(podWithClaimName),
			},
			finalState: state{
				slices: l(sliceTainted, slice2),
				allocatedClaims: l(ac(func() *resourceapi.ResourceClaim {
					claim := inUseClaim.DeepCopy()
					claim.Status.Allocation.Devices.Results[0].Tolerations = []resourceapi.DeviceToleration{{
						Operator:          resourceapi.DeviceTolerationOpExists,
						Effect:            resourceapi.DeviceTaintEffectNoExecute,
						TolerationSeconds: new(int64(60)),
					}}
					return claim
				}(), newEvictionTime(metav1Time(taintTime.Add(60*time.Second))))),
			},
		},
		"no-evict-wrong-resourceclaim": {
			events: []any{
				add(sliceTainted),
				add(slice2),
				add(inUseClaimOld), // Pod 不是属主
				add(podWithClaimTemplateInStatus),
			},
			finalState: state{
				slices:          l(sliceTainted, slice2),
				allocatedClaims: l(ac(inUseClaimOld, newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
			},
		},
		"evict-wrong-resourceclaim-replaced": {
			initialState: state{
				pods:            l(podWithClaimTemplateInStatus),
				slices:          l(sliceTainted, slice2),
				allocatedClaims: l(ac(inUseClaimOld, newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
			},
			events: []any{
				update(inUseClaimOld, inUseClaim),
			},
			finalState: state{
				slices:          l(sliceTainted, slice2),
				allocatedClaims: l(ac(inUseClaim, newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
				deletePodAt:     evictMap{newObject(podWithClaimTemplateInStatus): *newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0)},
				queued:          MockState[workItem]{Ready: newWorkItems(podWithClaimTemplateInStatus)},
			},
			wantEvents: l(deletePodEvent),
		},
		"no-evict-resourceclaim-deallocated": {
			initialState: state{
				pods:            l(podWithClaimTemplateInStatus),
				slices:          l(sliceTainted, slice2),
				allocatedClaims: l(ac(inUseClaim, newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
				deletePodAt:     evictMap{newObject(podWithClaimTemplateInStatus): *newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0)},
			},
			events: []any{
				// // 在 Pod 已被调度后移除其分配结果。
				// // 正常情况是 Pod 已终止；异常情况是强制状态更新，类似强制删除。
				// //
				// // 控制器是否应通过驱逐 Pod 来“修复”这类异常状态值得商榷。
				// // 当前不会这样做，因为这不是控制器的职责，且处理不当反而会引发问题，
				// // 例如驱逐仍可正常运行的 Pod。
				update(inUseClaim, claim),
			},
			finalState: state{
				slices: l(sliceTainted, slice2),
			},
			wantEvents: l(cancelPodEviction),
		},
		"no-evict-resourceclaim-deleted": {
			initialState: state{
				pods:            l(podWithClaimTemplateInStatus),
				slices:          l(sliceTainted, slice2),
				allocatedClaims: l(ac(inUseClaim, newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0))),
				deletePodAt:     evictMap{newObject(podWithClaimTemplateInStatus): *newEvictionTime(taintTime, sliceTainted, sliceTainted.Spec.Devices[0].Name, 0)},
			},
			events: []any{
				// // 与 "no-evict-resourceclaim-deallocated" 类似，可能是正常情况
				// // （Pod 已终止）或异常情况（强制删除）。
				remove(inUseClaim),
			},
			finalState: state{
				slices: l(sliceTainted, slice2),
			},
			wantEvents: l(cancelPodEviction),
		},
	} {
		tCtx.Run(name, func(tCtx ktesting.TContext) {
			tCtx.Parallel()
			numEvents := len(tc.events)
			if numEvents <= 1 {
				// 事件数不超过 1 时没有排列组合的必要，直接在单个 bubble 中运行。
				tCtx.SyncTest("", func(tCtx ktesting.TContext) {
					tContext := setup(tCtx, tc.workloadResourceClaimsEnabled)
					testHandlers(tContext, tc)
				})
				return
			}

			permutation := make([]int, numEvents)
			var permutate func(depth int)
			permutate = func(depth int) {
				if depth >= numEvents {
					// 得到一个完整排列：按该顺序重排事件并定义子测试。
					events := make([]any, numEvents)
					for i := range numEvents {
						events[i] = tc.events[permutation[i]]
					}
					tc := tc
					tc.events = events
					name := strings.Trim(fmt.Sprintf("%v", permutation), "[]")
					tCtx.Run(name, func(tCtx ktesting.TContext) {
						tCtx.Parallel()
						tCtx.SyncTest("", func(tCtx ktesting.TContext) {
							tContext := setup(tCtx, tc.workloadResourceClaimsEnabled)
							testHandlers(tContext, tc)
						})
					})
					return
				}
				for i := range numEvents {
					if slices.Index(permutation[0:depth], i) != -1 {
						// 该事件已被当前排列的前缀使用，跳过。
						continue
					}
					// 把事件 i 放到当前位置，再递归填充后续位置。
					permutation[depth] = i
					permutate(depth + 1)
				}
			}
			permutate(0)
		})
	}
}

// testHandlers 在一个用例中执行：装载初始状态 -> 应用事件 -> 断言中间状态 ->
// 逐轮清空工作队列并推进时间 -> 断言最终状态与事件。
func testHandlers(tContext *testContext, tc testCase) {
	// 队列初始必须为空；若用例声明了初始队列内容，说明用例本身需要修正。
	assertEqual(tContext, MockState[workItem]{}, tc.initialState.queued, "initial work queue state")
	// 即使用例不关心规则统计，也要有非 nil 的 map，
	// 否则控制器写入时会 panic。
	tContext.taintRuleStats = tc.initialState.ruleStats
	if tContext.taintRuleStats == nil {
		tContext.taintRuleStats = make(map[types.UID]taintRuleStats)
	}

	// 当前测试只需浅拷贝 slice 与 map。
	if len(tc.initialState.deletePodAt) > 0 {
		tContext.deletePodAt = maps.Clone(tc.initialState.deletePodAt)
	}
	tContext.allocatedClaims = tc.initialState.allocatedClaimsAsMap()
	tContext.pools = tc.initialState.slicesAsMap()

	// Pod 与 DeviceTaintRule 是控制器会从 informer 缓存读取的对象，
	// 因此只有这两类需要同步到 podStore；slice/claim 由事件参数直接传入。
	// 同时也要写入 fake client 的 tracker：informer 没有运行，
	// 所有 API 读写必须手动在缓存与 tracker 之间保持一致。
	podStore := tContext.informerFactory.Core().V1().Pods().Informer().GetStore()
	for _, pod := range tc.initialState.pods {
		tContext.ExpectNoError(podStore.Add(pod))
		tContext.ExpectNoError(tContext.client.Tracker().Add(pod))
	}
	// Pod 被 API 删除时，同步从 informer 缓存移除，
	// 模拟真实环境中 watch 事件带来的缓存更新。
	tContext.client.PrependReactor("delete", "pods", func(action core.Action) (handled bool, ret runtime.Object, err error) {
		if err := podStore.Delete(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: action.GetNamespace(), Name: action.(core.DeleteAction).GetName()}}); err != nil {
			return false, nil, fmt.Errorf("delete pod in informer cache: %w", err)
		}
		return false, nil, nil
	})
	ruleStore := tContext.informerFactory.Resource().V1().DeviceTaintRules().Informer().GetStore()
	for _, rule := range tc.initialState.rules {
		tContext.ExpectNoError(ruleStore.Add(rule))
		tContext.ExpectNoError(tContext.client.Tracker().Add(rule))
	}

	for _, event := range tc.events {
		switch event := event.(type) {
		case []any:
			for _, event := range event {
				applyEventPair(tContext, event)
			}
		default:
			applyEventPair(tContext, event)
		}
	}

	// 队列内容比较按 UID 排序：入队顺序由事件排列决定，并非被测行为。
	queueCmpOpts := []cmp.Option{
		cmpopts.SortSlices(compareWorkItems),
		cmpopts.SortSlices(compareDelayedWorkItems),
	}

	assertEqual(tContext, tc.finalState.ruleStats, tContext.taintRuleStats, "taintRuleStats")
	assertEqual(tContext, tc.finalState.deletePodAt, tContext.deletePodAt, "deletePodAt")
	// allocatedClaim 在 Worker 处理队列前后应保持不变，先在这里断言一次。
	assertEqual(tContext, tc.finalState.allocatedClaimsAsMap(), tContext.allocatedClaims, "allocated claims")
	if !assertEqual(tContext, tc.finalState.slicesAsMap(), tContext.pools, "pools") {
		for key := range tContext.pools {
			tContext.Assert(tContext.pools[key]).Should(gomega.Equal(tc.finalState.slicesAsMap()[key]), "pool")
		}
	}
	assertEqual(tContext, tc.finalState.queued, tContext.mockQueue.State(), "work queue after event handlers", queueCmpOpts...)
	tContext.Assert(tc.finalState.pods).Should(gomega.BeEmpty(), "pods not checked for final state")
	tContext.Assert(tc.finalState.rules).Should(gomega.BeEmpty(), "rules not checked for final state")

	process := tc.process
	if process == nil && len(tc.finalState.queued.Ready) > 0 {
		// 未显式声明 process 但队列中有 Ready 项时，默认期望 Worker 清空队列并删除 Pod。
		process = []step{{}}
	}
	for i, state := range process {
		prefix := fmt.Sprintf("process #%d: ", i)

		// worker 会一直处理到 Ready 队列为空：
		// 规则状态等字段允许变化，但 allocatedClaims 等字段必须保持稳定。
		tContext.Log(prefix + "handling ready work items")
		tContext.Controller.worker(tContext)

		assertEqual(tContext, state.ruleStats, tContext.taintRuleStats, prefix+"taintRuleStats")
		assertEqual(tContext, state.deletePodAt, tContext.deletePodAt, prefix+"deletePodAt")
		assertEqual(tContext, tc.finalState.allocatedClaimsAsMap(), tContext.allocatedClaims, prefix+"allocated claims")
		pods, err := tContext.client.CoreV1().Pods("").List(tContext, metav1.ListOptions{})
		tContext.ExpectNoError(err, prefix+"list pods")
		assertEqual(tContext, state.pods, trimPods(pods.Items), prefix+"pods after flushing work queue")
		rules, err := tContext.client.ResourceV1().DeviceTaintRules().List(tContext, metav1.ListOptions{})
		tContext.ExpectNoError(err, prefix+"list rules")
		actualRules := trimRules(rules.Items)
		assertEqual(tContext, state.rules, actualRules, prefix+"rules after flushing work queue")

		// 推进虚拟时间，让延迟到期的项转为 Ready；
		// mock 队列不会自动重排延迟，需要取消后以剩余时长重新 AddAfter。
		assertEqual(tContext, state.queuedProcessed, tContext.mockQueue.State(), prefix+"work queue after processing", queueCmpOpts...)
		time.Sleep(state.advance)
		for _, item := range tContext.mockQueue.State().Later {
			tContext.mockQueue.CancelAfter(item.Item)
			tContext.mockQueue.AddAfter(item.Item, item.Duration-state.advance)
		}
		assertEqual(tContext, state.queuedShifted, tContext.mockQueue.State(), prefix+"work queue after moving time forward", queueCmpOpts...)
	}

	assertEqual(tContext, tc.wantEvents, tContext.recorder.Events, "overall events",
		cmpopts.IgnoreTypes(metav1.ObjectMeta{}, metav1.Time{}),
		cmpopts.SortSlices(compareEvents))
}

// 以下比较函数用于在 cmpopts.SortSlices 中对事件/工作项做稳定排序。
// 目前涉及的字段足以区分测试中的对象，必要时再扩展。

// compareEvents 先按事件涉及对象的 UID、再按 Kind 排序。
func compareEvents(a, b *v1.Event) int {
	if cmp := strings.Compare(string(a.InvolvedObject.UID), string(b.InvolvedObject.UID)); cmp != 0 {
		return cmp
	}
	return strings.Compare(a.Kind, b.Kind)
}

// compareWorkItems 先按 Pod UID、再按规则 UID 排序工作项。
func compareWorkItems(a, b workItem) int {
	if cmp := strings.Compare(string(a.podRef.UID), string(b.podRef.UID)); cmp != 0 {
		return cmp
	}
	return strings.Compare(string(a.ruleRef.UID), string(b.ruleRef.UID))
}

// compareDelayedWorkItems 先按剩余延迟、再按工作项本身排序，
// 让延迟队列的断言不受插入顺序影响。
func compareDelayedWorkItems(a, b MockDelayedItem[workItem]) int {
	delta := a.Duration - b.Duration
	if delta > 0 {
		return 1
	}
	if delta < 0 {
		return -1
	}
	return compareWorkItems(a.Item, b.Item)
}

// applyEventPair 把一个 (旧对象, 新对象) 事件对同时应用到 informer 缓存、fake client
// 以及控制器的事件处理器上，保证测试环境中的「API 状态」和「控制器感知」一致。
func applyEventPair(tContext *testContext, event any) {
	switch pair := event.(type) {
	case [2]*resourceapi.ResourceSlice:
		tContext.handleSliceChange(pair[0], pair[1])
	case [2]*resourceapi.ResourceClaim:
		tContext.handleClaimChange(pair[0], pair[1])
	case [2]*v1.Pod:
		// Pod 的处理比较特殊：先更新缓存与 tracker，再调用 handlePodChange，
		// 因为处理器会从 informer 缓存读取 Pod 的最新状态。
		store := tContext.informerFactory.Core().V1().Pods().Informer().GetStore()
		switch {
		case pair[0] != nil && pair[1] != nil:
			// 新旧对象都存在 -> 更新；深拷贝避免 fake client 持有测试夹具的别名。
			obj := pair[1].DeepCopy()
			tContext.ExpectNoError(store.Update(obj))
			tContext.ExpectNoError(tContext.client.Tracker().Update(v1.SchemeGroupVersion.WithResource("pods"), obj, pair[1].Namespace))
		case pair[0] != nil:
			// 新对象为 nil -> 删除。
			tContext.ExpectNoError(store.Delete(pair[0]))
			tContext.ExpectNoError(tContext.client.Tracker().Delete(v1.SchemeGroupVersion.WithResource("pods"), pair[0].Namespace, pair[0].Name))
		default:
			// 旧对象为 nil -> 新增。
			obj := pair[1].DeepCopy()
			tContext.ExpectNoError(store.Add(obj))
			tContext.ExpectNoError(tContext.client.Tracker().Add(obj))
		}
		tContext.handlePodChange(pair[0], pair[1])
	case [2]*resourceapi.DeviceTaintRule:
		// 规则与 Pod 同理：控制器会从缓存读取规则，因此必须先同步缓存与 tracker。
		store := tContext.informerFactory.Resource().V1().DeviceTaintRules().Informer().GetStore()
		switch {
		case pair[0] != nil && pair[1] != nil:
			obj := pair[1].DeepCopy()
			tContext.ExpectNoError(store.Update(obj))
			tContext.ExpectNoError(tContext.client.Tracker().Update(resourceapi.SchemeGroupVersion.WithResource("devicetaintrules"), obj, pair[1].Namespace))
		case pair[0] != nil:
			tContext.ExpectNoError(store.Delete(pair[0]))
			tContext.ExpectNoError(tContext.client.Tracker().Delete(resourceapi.SchemeGroupVersion.WithResource("devicetaintrules"), pair[0].Namespace, pair[0].Name))
		default:
			obj := pair[1].DeepCopy()
			tContext.ExpectNoError(store.Add(obj))
			tContext.ExpectNoError(tContext.client.Tracker().Add(obj))
		}
		tContext.handleRuleChange(pair[0], pair[1])
	default:
		// 不支持的事件类型属于测试用例错误，明确失败比静默忽略更容易定位。
		tContext.Fatalf("unexpected event type %T", event)
	}
}

// trimPods 去掉 Pod 上由 API server 填写的 ManagedFields，
// 使从 fake client 读出的对象能与测试夹具直接比较。
func trimPods(objs []v1.Pod) (trimmed []*v1.Pod) {
	for _, in := range objs {
		out := in.DeepCopy()
		out.ManagedFields = nil
		trimmed = append(trimmed, out)
	}
	return trimmed
}

// trimRules 去掉规则的 ManagedFields 与 Kind/APIVersion，
// 这些字段由 fake client 的序列化过程填写，不属于被测状态。
func trimRules(objs []resourceapi.DeviceTaintRule) (trimmed []*resourceapi.DeviceTaintRule) {
	for _, in := range objs {
		out := in.DeepCopy()
		out.ManagedFields = nil
		out.Kind = ""
		out.APIVersion = ""
		trimmed = append(trimmed, out)
	}
	return trimmed
}

// newTestController 创建一个真正启动 informer 与 worker 的控制器，
// 用于端到端（synctest）测试；与 setup 中「只调 handler」的轻量上下文相对。
func newTestController(tCtx ktesting.TContext) *Controller {
	informerFactory := informers.NewSharedInformerFactory(tCtx.Client(), 0)
	controller := New(tCtx.Client(),
		informerFactory.Core().V1().Pods(),
		informerFactory.Resource().V1().ResourceClaims(),
		informerFactory.Resource().V1().ResourceSlices(),
		informerFactory.Resource().V1().DeviceTaintRules(),
		informerFactory.Resource().V1().DeviceClasses(),
		"device-taint-eviction",
	)
	// 使用独立的指标实例，避免各子测试之间共享全局指标状态。
	controller.metrics = metrics.New()
	// 事件日志始终写入测试 logger，不受 -v 影响。
	logger := klog.FromContext(tCtx)
	controller.eventLogger = &logger

	// 启动 informer 并在测试结束时关闭，避免 goroutine 泄漏到下一个子测试。
	informerFactory.StartWithContext(tCtx)
	tCtx.Cleanup(informerFactory.Shutdown)

	return controller
}

// testPodDeletionsMetrics 按期望的 Pod 删除延迟校验控制器记录的指标。
// latencies 中每个元素代表一个被删除的 Pod：
//   - 直方图各 bucket 的累计值由「延迟 <= bucket 上界」的条目数决定；
//   - sum 为所有延迟之和，count 为条目总数；
//   - 计数器累加删除的 Pod 数量。
func testPodDeletionsMetrics(controller *Controller, latencies ...time.Duration) error {
	// countLE 统计不超过上界 le 的延迟数量；le == -1 用作哨兵，表示统计全部（+Inf bucket）。
	countLE := func(le float64) int {
		total := 0
		for _, latency := range latencies {
			if le == -1 || latency.Seconds() <= le {
				total++
			}
		}
		return total
	}
	var totalDuration time.Duration
	for _, latency := range latencies {
		totalDuration += latency
	}

	// 按 Prometheus 文本格式拼出期望输出，再交给 metricstestutil 比对。
	var expected strings.Builder
	expected.WriteString(`# HELP device_taint_eviction_controller_pod_deletion_duration_seconds [ALPHA] Latency, in seconds, between the time when a device taint effect has been activated and a Pod's deletion via DeviceTaintEvictionController.
# TYPE device_taint_eviction_controller_pod_deletion_duration_seconds histogram
`)
	for _, le := range []float64{0.005, 0.025, 0.1, 0.5, 1, 2.5, 10, 30, 60, 120, 180, 240} {
		expected.WriteString(fmt.Sprintf("device_taint_eviction_controller_pod_deletion_duration_seconds_bucket{le=\"%f\"} %d\n", le, countLE(le)))
	}
	expected.WriteString(fmt.Sprintf("device_taint_eviction_controller_pod_deletion_duration_seconds_bucket{le=\"+Inf\"} %d\n", countLE(-1)))
	expected.WriteString(fmt.Sprintf("device_taint_eviction_controller_pod_deletion_duration_seconds_sum %f\n", totalDuration.Seconds()))
	expected.WriteString(fmt.Sprintf("device_taint_eviction_controller_pod_deletion_duration_seconds_count %d\n", len(latencies)))
	expected.WriteString(fmt.Sprintf(`# HELP device_taint_eviction_controller_pod_deletions_total [ALPHA] Total number of Pods deleted by DeviceTaintEvictionController since its start.
# TYPE device_taint_eviction_controller_pod_deletions_total counter
device_taint_eviction_controller_pod_deletions_total %d
`, len(latencies)))

	// 只采集本控制器注册的两个指标，避免全局注册表中的其它指标干扰比对。
	names := []string{
		controller.metrics.PodDeletionsTotal.FQName(),
		controller.metrics.PodDeletionsLatency.FQName(),
	}

	return metricstestutil.GatherAndCompare(controller.metrics, strings.NewReader(expected.String()), names...)
}

// TestEviction 覆盖「启动控制器并驱逐一个 Pod」的完整流程：
//   - 场景与上面的 "evict-pod-resourceclaim" 相同，但控制器是真实运行的；
//   - initial/add/update/delete 四种初始对象与调用组合都达到相同终态，
//     从而覆盖所有事件处理器；
//   - 测试运行在 synctest bubble（https://pkg.go.dev/testing/synctest）中，
//     可以等待所有 goroutine 阻塞后再检查状态，使时序完全确定。
func TestEviction(t *testing.T) { testEviction(ktesting.Init(t)) }

// testEviction 执行 TestEviction 的实际逻辑。
func testEviction(tCtx ktesting.TContext) {
	tCtx.Parallel()

	// do 统一记录步骤名并检查操作错误，便于失败时定位是哪一步 API 调用出错。
	do := func(tCtx ktesting.TContext, what string, action func(tCtx ktesting.TContext) error) {
		tCtx.Log(what)
		tCtx.ExpectNoError(action(tCtx))
	}

	pod := podWithClaimName.DeepCopy()
	// 四个子场景分别用不同的初始对象/后续 API 调用组合达到同一终态。
	for name, tt := range map[string]struct {
		initialObjects []runtime.Object
		afterSync      func(tCtx ktesting.TContext)
	}{
		"initial": {
			initialObjects: []runtime.Object{
				sliceTainted,
				inUseClaim,
				pod,
			},
		},
		"add": {
			afterSync: func(tCtx ktesting.TContext) {
				do(tCtx, "create pod", func(tCtx ktesting.TContext) error {
					_, err := tCtx.Client().CoreV1().Pods(pod.Namespace).Create(tCtx, pod, metav1.CreateOptions{})
					return err
				})
				do(tCtx, "create slice", func(tCtx ktesting.TContext) error {
					_, err := tCtx.Client().ResourceV1().ResourceSlices().Create(tCtx, sliceTainted, metav1.CreateOptions{})
					return err
				})
				do(tCtx, "create claim", func(tCtx ktesting.TContext) error {
					_, err := tCtx.Client().ResourceV1().ResourceClaims(inUseClaim.Namespace).Create(tCtx, inUseClaim, metav1.CreateOptions{})
					return err
				})
			},
		},
		"update": {
			initialObjects: []runtime.Object{
				slice,
				claim,
				func() *v1.Pod {
					pod := pod.DeepCopy()
					pod.Spec.NodeName = ""
					return pod
				}(),
			},
			afterSync: func(tCtx ktesting.TContext) {
				do(tCtx, "update pod", func(tCtx ktesting.TContext) error {
					_, err := tCtx.Client().CoreV1().Pods(pod.Namespace).Update(tCtx, pod, metav1.UpdateOptions{})
					return err
				})
				do(tCtx, "update slice", func(tCtx ktesting.TContext) error {
					_, err := tCtx.Client().ResourceV1().ResourceSlices().Update(tCtx, sliceTainted, metav1.UpdateOptions{})
					return err
				})
				do(tCtx, "update claim", func(tCtx ktesting.TContext) error {
					_, err := tCtx.Client().ResourceV1().ResourceClaims(inUseClaim.Namespace).UpdateStatus(tCtx, inUseClaim, metav1.UpdateOptions{})
					return err
				})
			},
		},
		"delete": {
			initialObjects: []runtime.Object{
				sliceTainted,
				func() *resourceapi.ResourceSlice {
					// 该 slice 的 Generation 更高，会「遮蔽」sliceTainted 的污点，
					// 直到它被删除，从而覆盖 delete 事件触发的重新计算。
					slice := slice.DeepCopy()
					slice.Name += "-other"
					slice.Spec.Pool.Generation += 100
					return slice
				}(),
				claim,
				pod,
			},
			afterSync: func(tCtx ktesting.TContext) {
				do(tCtx, "delete slice", func(tCtx ktesting.TContext) error {
					return tCtx.Client().ResourceV1().ResourceSlices().Delete(tCtx, slice.Name+"-other", metav1.DeleteOptions{})
				})
				do(tCtx, "delete claim", func(tCtx ktesting.TContext) error {
					return tCtx.Client().ResourceV1().ResourceClaims(inUseClaim.Namespace).Delete(tCtx, inUseClaim.Name, metav1.DeleteOptions{})
				})

				// 删除后重新创建 claim，使流程回到与其它场景相同的「受污点 claim 在使用中」状态。
				do(tCtx, "create claim", func(tCtx ktesting.TContext) error {
					_, err := tCtx.Client().ResourceV1().ResourceClaims(inUseClaim.Namespace).Create(tCtx, inUseClaim, metav1.CreateOptions{})
					return err
				})
			},
		},
	} {
		tCtx.SyncTest(name, func(tCtx ktesting.TContext) {
			start := time.Now()
			fakeClientset := fake.NewClientset(tt.initialObjects...)
			tCtx = tCtx.WithClients(nil, nil, fakeClientset, nil, nil)

			var podGets int
			var podUpdates int
			var updatedPod *v1.Pod
			var podDeletions int

			// 下列 reactor 只计数并断言操作目标，返回 false 让 fake client 继续执行默认行为。

			fakeClientset.PrependReactor("get", "pods", func(action core.Action) (handled bool, ret runtime.Object, err error) {
				podGets++
				podName := action.(core.GetAction).GetName()
				tCtx.Assert(podName).Should(gomega.Equal(podWithClaimName.Name), "name of pod to patch")
				return false, nil, nil
			})
			fakeClientset.PrependReactor("patch", "pods", func(action core.Action) (handled bool, ret runtime.Object, err error) {
				podUpdates++
				podName := action.(core.PatchAction).GetName()
				tCtx.Assert(podName).Should(gomega.Equal(podWithClaimName.Name), "name of pod to get")
				return false, nil, nil
			})
			fakeClientset.PrependReactor("delete", "pods", func(action core.Action) (handled bool, ret runtime.Object, err error) {
				podDeletions++
				podName := action.(core.DeleteAction).GetName()
				tCtx.Assert(podName).Should(gomega.Equal(podWithClaimName.Name), "name of pod to delete")
				obj, err := fakeClientset.Tracker().Get(v1.SchemeGroupVersion.WithResource("pods"), pod.Namespace, pod.Name)
				tCtx.ExpectNoError(err)
				updatedPod = obj.(*v1.Pod)
				return false, nil, nil
			})
			controller := newTestController(tCtx)

			var wg sync.WaitGroup
			defer func() {
				// 测试结束前取消控制器上下文并等待其 goroutine 退出，避免跨用例干扰。
				tCtx.Log("Waiting for goroutine termination...")
				tCtx.Cancel("time to stop")
				wg.Wait()
			}()
			wg.Go(func() {
				tCtx.AssertNoError(controller.Run(tCtx, 10 /* worker 数量 */), "eviction controller failed")
			})

			// 等待控制器完成 informer 同步后再施加后续变更，确保事件不会在启动阶段丢失。
			tCtx.Wait()
			if controller.hasSynced.Load() <= 0 {
				tCtx.Fatal("controller should have synced")
			}
			if tt.afterSync != nil {
				tt.afterSync(tCtx)
				tCtx.Wait()
			}

			// bubble 中所有 goroutine 已阻塞，API 调用次数完全确定：
			// 驱逐一个 Pod 恰好需要 get、patch、delete 各一次。
			tCtx.Assert(podGets).Should(gomega.Equal(1), "get pod once")
			tCtx.Assert(podUpdates).Should(gomega.Equal(1), "update pod once")
			tCtx.Assert(podDeletions).Should(gomega.Equal(1), "delete pod once")

			_, err := fakeClientset.CoreV1().Pods(pod.Namespace).Get(tCtx, pod.Name, metav1.GetOptions{})
			switch {
			case err == nil:
				tCtx.Fatalf("Pod should have been deleted, it still exists")
			case apierrors.IsNotFound(err):
				// 期望状态：Pod 已被删除。
			default:
				tCtx.Fatalf("Retrieving pod failed: %v", err)
			}

			// 校验删除前写入的 DisruptionTarget 条件，确保 Pod 被正确标记为驱逐目标。
			pod := pod.DeepCopy()
			pod.Status.Conditions = []v1.PodCondition{{
				Type:    v1.DisruptionTarget,
				Status:  v1.ConditionTrue,
				Reason:  "DeletionByDeviceTaintManager",
				Message: "Device Taint manager: deleting due to NoExecute taint",
			}}
			if diff := cmp.Diff(pod, updatedPod, cmpopts.IgnoreTypes(metav1.Time{}, metav1.TypeMeta{}), cmpopts.IgnoreFields(metav1.ObjectMeta{}, "ManagedFields")); diff != "" {
				tCtx.Errorf("unexpected modified pod (-want, +got):\n%s", diff)
			}

			// bubble 中时间不会自行流逝，因此从启动到驱逐耗时应精确为 0。
			tCtx.Expect(time.Since(start)).To(gomega.Equal(0*time.Second), "time to evict pod")

			// 删除后指标也会更新，这是控制器对 Pod 做的最后一件事。
			// 此时所有 goroutine 已持久阻塞、队列为空，无需用 Consistently 轮询等待。
			tCtx.Assert(controller.workqueue.Len()).Should(gomega.Equal(0), "work queue empty")
			tCtx.Assert(listEvents(tCtx)).Should(matchDeletionEvent())
			tCtx.ExpectNoError(testPodDeletionsMetrics(controller, 0*time.Second), "pod eviction done")
		})
	}
}

// TestDeviceTaintRule 覆盖规则效果变化的完整流程：
//   - 先以 None 效果模拟（只报告影响、不驱逐）；
//   - 再把规则更新为 NoExecute，触发 Pod 驱逐；
//   - immediate/delayed/slow 三个子场景分别覆盖立即驱逐、带容忍延迟、删除调用被阻塞的情形。
//
// 测试运行在 synctest bubble（https://pkg.go.dev/testing/synctest）中，
// 可等待 goroutine 静止后断言状态。
func TestDeviceTaintRule(t *testing.T) { testDeviceTaintRule(ktesting.Init(t)) }

// testDeviceTaintRule 执行 TestDeviceTaintRule 的三个子场景。
func testDeviceTaintRule(tCtx ktesting.TContext) {
	tCtx.Parallel()
	tCtx.SyncTest("immediate", func(tCtx ktesting.TContext) { synctestDeviceTaintRule(tCtx, false, false) })
	tCtx.SyncTest("delayed", func(tCtx ktesting.TContext) { synctestDeviceTaintRule(tCtx, true, false) })
	tCtx.SyncTest("slow", func(tCtx ktesting.TContext) { synctestDeviceTaintRule(tCtx, true, true) })
}

// synctestDeviceTaintRule 是 TestDeviceTaintRule 的实际场景实现：
// toleration 控制是否为 claim 添加 30 秒容忍，slowDelete 控制是否阻塞 Pod 删除调用。
func synctestDeviceTaintRule(tCtx ktesting.TContext, toleration, slowDelete bool) {
	rule := ruleNone.DeepCopy()
	claim := inUseClaim.DeepCopy()
	tolerationSeconds := int64(0)
	if toleration {
		tolerationSeconds = 30
		claim.Status.Allocation.Devices.Results[0].Tolerations = []resourceapi.DeviceToleration{{
			Effect:            resourceapi.DeviceTaintEffectNoExecute,
			Value:             rule.Spec.Taint.Value,
			TolerationSeconds: &tolerationSeconds,
		}}
	}
	fakeClientset := fake.NewClientset(podWithClaimName, claim, rule)
	blockDelete := make(chan struct{})
	client := clientset.Interface(fakeClientset)
	if slowDelete {
		// 用包装客户端在 Delete 前注入阻塞点，模拟删除请求耗时较长的情形。
		client = &myFakeClient{client, func() {
			// 与主 goroutine 同步：主 goroutine 推进一段虚拟时间后才关闭 channel，
			// 从而让这次删除产生可观测的延迟。
			tCtx.Logf("Delaying pod deletion")
			<-blockDelete
			tCtx.Logf("Proceeding with pod deletion")
		}}
	}
	tCtx = tCtx.WithClients(nil, nil, client, nil, nil)
	controller := newTestController(tCtx)

	var wg sync.WaitGroup
	defer func() {
		tCtx.Log("Waiting for goroutine termination...")
		tCtx.Cancel("time to stop")
		wg.Wait()
	}()
	wg.Go(func() {
		// 只开一个 worker 保证顺序执行：并发 worker 会导致状态更新顺序不确定，
		// 使 Status 断言出现随机失败（flaky）。
		tCtx.AssertNoError(controller.Run(tCtx, 1 /* worker 数量 */), "eviction controller failed")
	})

	// 等待 informer 同步完成，确保后续对规则的更新能被控制器感知。
	tCtx.Wait()
	if controller.hasSynced.Load() <= 0 {
		tCtx.Fatal("controller should have synced")
	}

	start := metav1.Now()
	tCtx.Logf("TIME: start")
	check(tCtx, "initial processing: ", l(inProgress(ruleNone, false, "NoEffect", "0 published devices selected. 1 allocated device selected. 1 pod would be evicted in 1 namespace if the effect was NoExecute. This information will not be updated again. Recreate the DeviceTaintRule to trigger an update.", &start /* 等待缓存同步完成前已处理 */)), l(podWithClaimName))

	// 推进 20 秒虚拟时间，保证规则更新前后的时间戳不同，
	// 便于断言规则状态中 LastTransitionTime 的变化。
	time.Sleep(20 * time.Second)
	updated := metav1.Now()
	// fake.NewClientset 不会复制对象，必须手动深拷贝以免污染共享夹具。
	rule = rule.DeepCopy()
	rule.Spec.Taint.Effect = resourceapi.DeviceTaintEffectNoExecute
	// 真实 apiserver 在 1.36 会自动更新 TimeAdded，但单元测试中必须手动设置。
	rule.Spec.Taint.TimeAdded = &updated
	rule, err := tCtx.Client().ResourceV1().DeviceTaintRules().Update(tCtx, rule, metav1.UpdateOptions{})
	tCtx.ExpectNoError(err, "update rule")

	// 等待驱逐相关处理结束。
	tCtx.Wait()
	tCtx.Logf("TIME: eviction %s since start", updated.Sub(start.Time))
	// 此刻规则状态只在驱逐前更新过一次，驱逐后的状态更新还在队列中等待。
	var expectedPods []*v1.Pod
	if tolerationSeconds > 0 {
		// 有容忍时驱逐被推迟，此刻 Pod 应仍然存在。
		expectedPods = append(expectedPods, podWithClaimName)
	}
	check(tCtx, "evict: ", l(inProgress(rule, true, "PodsPendingEviction", "1 pod needs to be evicted in 1 namespace.", &updated)), expectedPods)

	var slowDeleteDelay time.Duration
	if tolerationSeconds > 0 {
		// 推进时间越过容忍期，使驱逐工作项到期。
		time.Sleep(time.Duration(tolerationSeconds) * time.Second)
		if slowDelete {
			// 额外的删除耗时会计入删除延迟直方图。
			slowDeleteDelay = 3 * time.Second
			time.Sleep(slowDeleteDelay)
			close(blockDelete)
		}
		tCtx.Wait()
		// 此时 Pod 已删除，但规则状态的后续更新仍未完成。
		check(tCtx, "evict: ", l(inProgress(rule, true, "PodsPendingEviction", "1 pod needs to be evicted in 1 namespace.", &updated)), nil)
	}

	// AddAfter 只会排期、不会推进时间，这里手动推进以触发规则状态的定期更新。
	time.Sleep(ruleStatusPeriod)
	slept := metav1.Now()
	tCtx.Logf("TIME: slept %s since start", slept.Sub(start.Time))
	tCtx.Wait()
	done := metav1.Now()
	tCtx.Logf("TIME: done %s since start", done.Sub(start.Time))
	check(tCtx, "done: ", l(inProgress(rule, false, "Completed", "1 pod evicted since starting the controller.", &done)), nil)
	assertEqual(tCtx, map[types.UID]taintRuleStats{rule.UID: {numEvictedPods: 1}}, controller.taintRuleStats, "taint rule statistics should have counted the pod")

	// 删除规则后验证控制器会清理 taintRuleStats，避免长期运行时内存泄漏。
	err = tCtx.Client().ResourceV1().DeviceTaintRules().Delete(tCtx, rule.Name, metav1.DeleteOptions{})
	tCtx.ExpectNoError(err, "delete rule")
	tCtx.Wait()
	deleted := metav1.Now()
	tCtx.Logf("TIME: deleted %s since start", deleted.Sub(start.Time))
	tCtx.Assert(controller.taintRuleStats).Should(gomega.BeEmpty(), "taint rule statistics should have dropped the deleted rule")

	tCtx.Assert(controller.workqueue.Len()).Should(gomega.Equal(0), "work queue empty")
	// bubble 内不存在真实的排队延迟：唯一的延迟来自显式注入的 slowDelete。
	tCtx.ExpectNoError(testPodDeletionsMetrics(controller, slowDeleteDelay))
}

// check 对照 fake client 中的实际对象与期望的 Pod/规则状态。
// prefix 用于在失败信息中标识检查发生的阶段。
func check(tCtx ktesting.TContext, prefix string, expectRules []*resourceapi.DeviceTaintRule, expectPods []*v1.Pod) {
	tCtx.Helper()

	opts := []cmp.Option{
		// 期望对象没有 ManagedFields，而 API 读出的对象有，比较时忽略。
		cmpopts.IgnoreFields(metav1.ObjectMeta{}, "ManagedFields"),
		// 序列化会把 metav1.Time 四舍五入到秒，比较时也做同样处理以消除精度差异。
		cmpopts.AcyclicTransformer("RoundTime", func(t metav1.Time) metav1.Time {
			return metav1.Time{Time: t.Round(time.Second)}
		}),
	}

	actualPods, err := tCtx.Client().CoreV1().Pods("").List(tCtx, metav1.ListOptions{})
	tCtx.ExpectNoError(err, prefix+"list pods")
	assertEqual(tCtx, expectPods, trimPods(actualPods.Items), prefix+"pods", opts...)
	rules, err := tCtx.Client().ResourceV1().DeviceTaintRules().List(tCtx, metav1.ListOptions{})
	tCtx.ExpectNoError(err, prefix+"list rules")
	assertEqual(tCtx, expectRules, trimRules(rules.Items), prefix+"rules", opts...)
}

// myFakeClient 及其子包装器只有一个目的：在 CoreV1().Pods().Delete 前注入回调。
// 不能用 fakeClientset.PrependReactor 实现，因为 reactor 在 fake 的互斥锁持有期间被调用；
// 若此时其它 goroutine 也发起 API 调用，synctest bubble 会死锁——
// 因为持有互斥锁不被视为「持久阻塞」，尽管在我们的场景中确实如此。
type myFakeClient struct {
	clientset.Interface
	deletePodCallback func()
}

// CoreV1 返回包装后的 CoreV1 接口，把删除回调继续向下传递。
func (mf *myFakeClient) CoreV1() corev1.CoreV1Interface {
	return &myFakeCoreV1{mf.Interface.CoreV1(), mf.deletePodCallback}
}

// myFakeCoreV1 用于把回调传递到 Pods 子接口。
type myFakeCoreV1 struct {
	corev1.CoreV1Interface
	deletePodCallback func()
}

// Pods 返回包装后的 Pod 接口，继续携带删除回调。
func (mf *myFakeCoreV1) Pods(namespace string) corev1.PodInterface {
	return &myFakePods{mf.CoreV1Interface.Pods(namespace), mf.deletePodCallback}
}

// myFakePods 在 Delete 调用前执行注入的回调。
type myFakePods struct {
	corev1.PodInterface
	deletePodCallback func()
}

// Delete 先执行回调（例如阻塞等待主 goroutine 放行），再委托给真实的 fake 客户端。
func (mf *myFakePods) Delete(ctx context.Context, name string, opts metav1.DeleteOptions) error {
	mf.deletePodCallback()
	return mf.PodInterface.Delete(ctx, name, opts)
}

// TestCancelEviction 验证两种取消驱逐的路径：
//   - 控制器删除 Pod 之前，Pod 自己被删除；
//   - 或包含污点的 slice 被删除。
//
// 两种情况都应取消待执行的驱逐。
func TestCancelEviction(t *testing.T) { testCancelEviction(ktesting.Init(t)) }

// testCancelEviction 分别用「删除 Pod」和「删除 slice」运行取消场景。
func testCancelEviction(tCtx ktesting.TContext) {
	tCtx.Parallel()
	tCtx.SyncTest("pod-deleted", func(tCtx ktesting.TContext) { doCancelEviction(tCtx, true) })
	tCtx.SyncTest("slice-deleted", func(tCtx ktesting.TContext) { doCancelEviction(tCtx, false) })
}

// doCancelEviction 执行取消驱逐场景：deletePod 为 true 时删除 Pod，否则删除 slice。
func doCancelEviction(tCtx ktesting.TContext, deletePod bool) {
	// 给 claim 设置 60 秒容忍，为测试留出足够时间执行取消操作，
	// 避免控制器在测试介入前就完成驱逐。
	pod := podWithClaimName.DeepCopy()
	slice := sliceTainted.DeepCopy()
	slice.Spec.Devices[0].Taints[0].TimeAdded = metav1Time(time.Now())
	claim := inUseClaim.DeepCopy()
	tolerationSeconds := int64(60)
	claim.Status.Allocation.Devices.Results[0].Tolerations = []resourceapi.DeviceToleration{{
		Operator:          resourceapi.DeviceTolerationOpExists,
		Effect:            resourceapi.DeviceTaintEffectNoExecute,
		TolerationSeconds: &tolerationSeconds,
	}}
	fakeClientset := fake.NewClientset(
		slice,
		claim,
		pod,
	)
	pod, err := fakeClientset.CoreV1().Pods(pod.Namespace).Get(tCtx, pod.Name, metav1.GetOptions{})
	tCtx.ExpectNoError(err, "get pod before eviction")
	tCtx.Assert(pod).Should(gomega.Equal(podWithClaimName), "test pod")

	var podGets int
	var podUpdates int
	var podDeletions int

	fakeClientset.PrependReactor("get", "pods", func(action core.Action) (handled bool, ret runtime.Object, err error) {
		podGets++
		podName := action.(core.GetAction).GetName()
		tCtx.Assert(podName).Should(gomega.Equal(podWithClaimName.Name), "name of pod to patch")
		return false, nil, nil
	})
	fakeClientset.PrependReactor("patch", "pods", func(action core.Action) (handled bool, ret runtime.Object, err error) {
		podUpdates++
		podName := action.(core.PatchAction).GetName()
		tCtx.Assert(podName).Should(gomega.Equal(podWithClaimName.Name), "name of pod to get")
		return false, nil, nil
	})
	fakeClientset.PrependReactor("delete", "pods", func(action core.Action) (handled bool, ret runtime.Object, err error) {
		podDeletions++
		podName := action.(core.DeleteAction).GetName()
		tCtx.Assert(podName).Should(gomega.Equal(podWithClaimName.Name), "name of pod to delete")
		return false, nil, nil
	})

	tCtx = tCtx.WithClients(nil, nil, fakeClientset, nil, nil)
	controller := newTestController(tCtx)

	podEvicting := false
	controller.evictPodHook = func(podRef tainteviction.NamespacedObject, eviction evictionAndReason) {
		tCtx.Assert(podRef).Should(gomega.Equal(newObject(pod)))
		podEvicting = true
	}
	controller.cancelEvictHook = func(podRef tainteviction.NamespacedObject) bool {
		tCtx.Assert(podRef).Should(gomega.Equal(newObject(pod)))
		podEvicting = false
		return false
	}

	var wg sync.WaitGroup
	defer func() {
		tCtx.Log("Waiting for goroutine termination...")
		tCtx.Cancel("time to stop")
		wg.Wait()
	}()
	wg.Go(func() {
		tCtx.AssertNoError(controller.Run(tCtx, 10 /* worker 数量 */), "eviction controller failed")
	})

	// 最终 Pod 被安排驱逐。
	tCtx.Wait()
	tCtx.Expect(podEvicting).To(gomega.BeTrueBecause("pod pending eviction"))

	// 此时可以删除 Pod 或 slice。
	if deletePod {
		tCtx.ExpectNoError(fakeClientset.CoreV1().Pods(pod.Namespace).Delete(tCtx, pod.Name, metav1.DeleteOptions{}))
	} else {
		tCtx.ExpectNoError(fakeClientset.ResourceV1().ResourceSlices().Delete(tCtx, slice.Name, metav1.DeleteOptions{}))
	}

	// 删除后不久应看到驱逐被取消。
	tCtx.Wait()
	tCtx.Expect(podEvicting).To(gomega.BeFalseBecause("pod no longer pending eviction"))

	// 是否产生事件取决于删除时 Pod 是否仍存在。
	if !deletePod {
		tCtx.Expect(listEvents(tCtx)).Should(matchCancellationEvent())
	}

	matchEvents := matchCancellationEvent()
	if deletePod {
		matchEvents = gomega.BeEmpty()
		tCtx.Assert(podDeletions).Should(gomega.Equal(1), "Pod should have been deleted exactly once by test.")
	} else {
		tCtx.Assert(podDeletions).Should(gomega.Equal(0), "Pod should not have been deleted.")
	}

	// // synctest.Wait 不会推进时间：它在所有 goroutine 都阻塞等待时间流逝
	// // （例如 time.Sleep）时返回。
	// // 要清空工作队列，必须把时间推进到足以越过 AddAfter 的延迟。
	time.Sleep(10 * time.Duration(tolerationSeconds) * time.Second)
	tCtx.Wait()

	tCtx.Assert(controller.workqueue.Len()).Should(gomega.Equal(0), "work queue empty")
	gomega.NewGomegaWithT(tCtx).Expect(controller.workqueue.Len()).Should(gomega.Equal(0), "work queue empty")
	tCtx.Assert(podGets).Should(gomega.Equal(0), "Worker should not have needed to get the pod.")
	tCtx.Assert(podUpdates).Should(gomega.Equal(0), "Worker should not have needed to update the pod.")
	tCtx.Assert(controller.maybeDeletePodCount).Should(gomega.Equal(int64(1)), "Work queue should have processed pod.")
	tCtx.Assert(listEvents(tCtx)).Should(matchEvents)
	tCtx.ExpectNoError(testPodDeletionsMetrics(controller))
}

// TestParallelPodDeletion 覆盖「控制器即将驱逐 Pod 时，Pod 已被别人删除」的竞态：
// 控制器应把 NotFound 当作正常情况处理，不产生事件也不计删除指标。
func TestParallelPodDeletion(t *testing.T) { testParallelPodDeletion(ktesting.Init(t)) }

// testParallelPodDeletion 在 synctest bubble 中执行上述竞态场景。
func testParallelPodDeletion(tCtx ktesting.TContext) {
	tCtx.Parallel()
	tCtx.SyncTest("", synctestParallelPodDeletion)
}

// synctestParallelPodDeletion 是 TestParallelPodDeletion 的实际场景实现。
func synctestParallelPodDeletion(tCtx ktesting.TContext) {
	// 场景与上面的 "evict-pod-resourceclaim" 相同。
	pod := podWithClaimName.DeepCopy()
	fakeClientset := fake.NewClientset(
		sliceTainted,
		slice2,
		inUseClaim,
		pod,
	)
	tCtx = tCtx.WithClients(nil, nil, fakeClientset, nil, nil)

	pod, err := fakeClientset.CoreV1().Pods(pod.Namespace).Get(tCtx, pod.Name, metav1.GetOptions{})
	tCtx.ExpectNoError(err, "get pod before eviction")
	tCtx.Assert(pod).Should(gomega.Equal(podWithClaimName), "test pod")

	var podGets int
	var podDeletions int

	fakeClientset.PrependReactor("get", "pods", func(action core.Action) (handled bool, ret runtime.Object, err error) {
		podGets++
		podName := action.(core.GetAction).GetName()
		tCtx.Assert(podName).Should(gomega.Equal(podWithClaimName.Name), "name of patched pod")

		// 该方法在驱逐发生前直接调用，这里假设 Pod 已被删除。
		err = fakeClientset.Tracker().Delete(v1.SchemeGroupVersion.WithResource("pods"), pod.Namespace, pod.Name)
		tCtx.AssertNoError(err, "delete pod")
		return true, nil, apierrors.NewNotFound(v1.SchemeGroupVersion.WithResource("pods").GroupResource(), pod.Name)
	})
	fakeClientset.PrependReactor("delete", "pods", func(action core.Action) (handled bool, ret runtime.Object, err error) {
		podDeletions++
		podName := action.(core.DeleteAction).GetName()
		tCtx.Assert(podName).Should(gomega.Equal(podWithClaimName.Name), "name of deleted pod")
		return false, nil, nil
	})
	controller := newTestController(tCtx)

	var wg sync.WaitGroup
	defer func() {
		tCtx.Log("Waiting for goroutine termination...")
		tCtx.Cancel("time to stop")
		wg.Wait()
	}()
	wg.Go(func() {
		tCtx.AssertNoError(controller.Run(tCtx, 10 /* worker 数量 */), "eviction controller failed")
	})

	// 该场景不应产生任何事件。
	tCtx.Wait()
	tCtx.Assert(podGets).Should(gomega.Equal(1), "number of pod get calls")
	tCtx.Assert(podDeletions).Should(gomega.Equal(0), "number of pod delete calls")
	tCtx.Assert(controller.workqueue.Len()).Should(gomega.Equal(0), "Work queue should be empty now.")
	tCtx.Assert(listEvents(tCtx)).Should(gomega.BeEmpty())
	tCtx.ExpectNoError(testPodDeletionsMetrics(controller))
}

// TestRetry 覆盖驱逐尝试因临时错误失败后必须重试的场景：
// 第一次 Get Pod 返回 InternalError，控制器应按默认指数退避（5ms）重试并成功删除 Pod。
func TestRetry(t *testing.T) { testRetry(ktesting.Init(t)) }

// testRetry 在 synctest bubble 中执行重试场景。
func testRetry(tCtx ktesting.TContext) {
	tCtx.Parallel()
	tCtx.SyncTest("", synctestRetry)
}

// synctestRetry 是 TestRetry 的实际场景实现。
func synctestRetry(tCtx ktesting.TContext) {
	// 场景与上面的 "evict-pod-resourceclaim" 相同。
	pod := podWithClaimName.DeepCopy()
	fakeClientset := fake.NewClientset(
		sliceTainted,
		slice2,
		inUseClaim,
		pod,
	)
	tCtx = tCtx.WithClients(nil, nil, fakeClientset, nil, nil)

	pod, err := fakeClientset.CoreV1().Pods(pod.Namespace).Get(tCtx, pod.Name, metav1.GetOptions{})
	tCtx.ExpectNoError(err, "get pod before eviction")
	tCtx.Assert(pod).Should(gomega.Equal(podWithClaimName), "test pod")

	// reactor 可能被多个 worker 并发调用，用互斥锁保护计数器。
	var mutex sync.Mutex
	var podGets int
	var podDeletions int

	fakeClientset.PrependReactor("get", "pods", func(action core.Action) (handled bool, ret runtime.Object, err error) {
		mutex.Lock()
		defer mutex.Unlock()
		podGets++
		podName := action.(core.GetAction).GetName()
		tCtx.Assert(podName).Should(gomega.Equal(podWithClaimName.Name), "name of patched pod")

		// 第一次 Get 模拟临时错误，验证工作队列会重试；后续调用走正常路径。
		if podGets == 1 {
			return true, nil, apierrors.NewInternalError(errors.New("fake error"))
		}
		return false, nil, nil
	})
	fakeClientset.PrependReactor("delete", "pods", func(action core.Action) (handled bool, ret runtime.Object, err error) {
		mutex.Lock()
		defer mutex.Unlock()
		podDeletions++
		podName := action.(core.DeleteAction).GetName()
		tCtx.Assert(podName).Should(gomega.Equal(podWithClaimName.Name), "name of deleted pod")
		return false, nil, nil
	})
	controller := newTestController(tCtx)

	var wg sync.WaitGroup
	defer func() {
		tCtx.Log("Waiting for goroutine termination...")
		tCtx.Cancel("time to stop")
		wg.Wait()
	}()
	wg.Go(func() {
		tCtx.AssertNoError(controller.Run(tCtx, 10 /* worker 数量 */), "eviction controller failed")
	})

	// 默认指数退避的第一次重试间隔为 5ms，该延迟会计入删除指标。
	expectLatencies := []time.Duration{5 * time.Millisecond /* 默认指数退避 */}

	// 最终 Pod 被删除并记录事件。
	tCtx.Eventually(func(tCtx ktesting.TContext) error {
		tCtx.Expect(listEvents(tCtx)).To(matchDeletionEvent())
		return testPodDeletionsMetrics(controller, expectLatencies...)
	}).WithTimeout(30*time.Second).Should(gomega.Succeed(), "pod eviction done")

	// 事件与指标都就绪后，再核对 API 调用次数：两次 Get（含失败重试）、一次 Delete。
	tCtx.Wait()
	tCtx.Assert(podGets).Should(gomega.Equal(2), "number of pod get calls")
	tCtx.Assert(podDeletions).Should(gomega.Equal(1), "number of pod delete calls")
	tCtx.Assert(listEvents(tCtx)).Should(matchDeletionEvent())
	tCtx.Assert(controller.workqueue.Len()).Should(gomega.Equal(0), "Work queue should be empty now.")
	tCtx.ExpectNoError(testPodDeletionsMetrics(controller, expectLatencies...))
}

// BenchmarkTaintUntaint 测量完整流程的开销：
//   - 新 DeviceTaintRule 使 claim 被视为受污点影响；
//   - 控制器开始驱逐其消费者 Pod；
//   - 规则移除后撤销驱逐决定。
func BenchmarkTaintUntaint(b *testing.B) {
	tCtx := ktesting.Init(b)
	tContext := setup(tCtx, false) // 因 DRAWorkloadResourceClaims 尚为 alpha 而将其关闭
	podStore := tContext.informerFactory.Core().V1().Pods().Informer().GetStore()
	// 基准测试中关闭事件日志输出；如需观察事件可注释掉这一行。
	tContext.Controller.eventLogger = nil

	// // 条件必须精确写作 "b.Loop"，才能启用编译器对基准测试的特殊支持。
	for b.Loop() {
		// 构造场景：依次加入 slice、claim 与 Pod。
		tContext.handleSliceChange(nil, slice)
		tContext.handleClaimChange(nil, inUseClaimWithToleration)
		tContext.ExpectNoError(podStore.Add(podWithClaimName), "add pod")
		tContext.handlePodChange(nil, podWithClaimName)
		tContext.Expect(tContext.deletePodAt).To(gomega.BeEmpty())

		// slice 变为受污点状态，触发驱逐决定。
		tContext.handleSliceChange(slice, sliceTainted)

		// informer 事件处理器是同步调用的，因此无需等待即可断言结果。
		tContext.Expect(tContext.deletePodAt).NotTo(gomega.BeEmpty())

		// 污点消失后，驱逐决定应被撤销。
		tContext.handleSliceChange(sliceTainted, slice)
		tContext.Expect(tContext.deletePodAt).To(gomega.BeEmpty())

		tContext.handlePodChange(podWithClaimName, nil)
		tContext.ExpectNoError(podStore.Delete(podWithClaimName), "remove pod")
		tContext.handleClaimChange(inUseClaimWithToleration, nil)
		tContext.handleSliceChange(slice, nil)
	}
}
