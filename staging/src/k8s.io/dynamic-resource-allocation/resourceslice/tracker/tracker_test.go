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

package tracker

import (
	stdcmp "cmp"
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	resourceinformers "k8s.io/client-go/informers/resource/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	_ "k8s.io/klog/v2/ktesting/init"
	"k8s.io/utils/ptr"
)

// handlerEventType 表示事件处理器收到的事件类型（增/改/删）。
type handlerEventType string

const (
	// handlerEventAdd 表示新增事件。
	// handlerEventUpdate 表示更新事件。
	// handlerEventDelete 表示删除事件。
	handlerEventAdd    handlerEventType = "add"
	handlerEventUpdate handlerEventType = "update"
	handlerEventDelete handlerEventType = "delete"
)

// handlerEvent 记录一次回调的类型及其携带的新旧对象。
type handlerEvent struct {
	// event 为事件类型。
	// oldObj 为更新/删除事件中的旧对象。
	// newObj 为新增/更新事件中的新对象。
	event  handlerEventType
	oldObj *resourceapi.ResourceSlice
	newObj *resourceapi.ResourceSlice
}

// add 构造一对 (nil, obj)，表示一次新增输入事件。
func add[T any](obj *T) [2]*T {
	return [2]*T{nil, obj}
}

// remove 构造一对 (obj, nil)，表示一次删除输入事件。
func remove[T any](obj *T) [2]*T {
	return [2]*T{obj, nil}
}

// update 构造一对 (oldObj, newObj)，表示一次更新输入事件。
func update[T any](oldObj, newObj *T) [2]*T {
	return [2]*T{oldObj, newObj}
}

// removeRule(<name>) removes a DeviceTaintRule by name.
// removeRule 表示按名称删除规则，用于模拟“删除时拿不到对象”的场景。
type removeRule string

// runInputEvents 按给定排列顺序依次应用输入事件，并为每步附加带编号的 logger 名称。
func runInputEvents(tCtx *testContext, events []any, permutation []int) {
	// 按排列索引逐个取出事件。
	for _, i := range permutation {
		// 将扁平索引映射回原始事件并生成名称。
		event, name := lookupEvent(events, i)
		stepCtx := tCtx.withLoggerName(fmt.Sprintf("event #%s", name))
		// 应用到 informer store 并触发对应回调。
		applyEventPair(stepCtx, event)
	}
}

// lookupEvent is the opposite of flatten: it takes an index
// after flattening and maps it back to the event in the original
// event hierarchy. To do so, it descends into the second level where necessary.
// lookupEvent 将扁平化后的索引还原为原始事件及其层级路径名称，是 flatten 的逆操作。
func lookupEvent(events []any, index int) (any, string) {
	// numEvents 记录已跳过的事件数。
	numEvents := 0
	// 事件可能是单层或嵌套列表。
	for i := range events {
		// 嵌套列表需再展开一层，索引格式为“外层/内层”。
		if e, ok := events[i].([]any); ok {
			for j := range e {
				if numEvents == index {
					return e[j], fmt.Sprintf("%d/%d", i, j)
				}
				numEvents++
			}
		} else {
			// 单层事件的索引就是外层序号。
			if numEvents == index {
				return events[i], fmt.Sprintf("%d", i)
			}
			numEvents++
		}
	}
	// 索引越界时直接 panic，提示测试用例构造错误。
	panic(fmt.Sprintf("invalid event index #%d", index))
}

// applyEventPair 根据事件对的种类（slice / 规则 / 按名删除规则）更新 informer store 并触发相应回调。
func applyEventPair(tCtx *testContext, event any) {
	// 按输入事件的具体类型分派。
	switch pair := event.(type) {
	// ResourceSlice 事件：同时维护 store 与调用 tracker 的回调。
	case [2]*resourceapi.ResourceSlice:
		store := tCtx.resourceSlices.GetStore()
		switch {
		// 两者非空表示更新。
		case pair[0] != nil && pair[1] != nil:
			err := store.Update(pair[1])
			require.NoError(tCtx, err)
			tCtx.resourceSliceUpdate(tCtx.Context)(pair[0], pair[1])
		// 仅旧对象非空表示删除。
		case pair[0] != nil:
			err := store.Delete(pair[0])
			require.NoError(tCtx, err)
			tCtx.resourceSliceDelete(tCtx.Context)(resourceinformers.DeletedResourceSlice{OptionalObj: pair[0]})
		// 其余情况视为新增。
		default:
			err := store.Add(pair[1])
			require.NoError(tCtx, err)
			tCtx.resourceSliceAdd(tCtx.Context)(pair[1])
		}
	// DeviceTaintRule 事件，处理方式与 slice 类似。
	case [2]*resourceapi.DeviceTaintRule:
		store := tCtx.deviceTaints.GetStore()
		switch {
		case pair[0] != nil && pair[1] != nil:
			err := store.Update(pair[1])
			require.NoError(tCtx, err)
			tCtx.deviceTaintUpdate(tCtx.Context)(pair[0], pair[1])
		case pair[0] != nil:
			err := store.Delete(pair[0])
			require.NoError(tCtx, err)
			tCtx.deviceTaintDelete(tCtx.Context)(resourceinformers.DeletedDeviceTaintRule{OptionalObj: pair[0]})
		default:
			err := store.Add(pair[1])
			require.NoError(tCtx, err)
			tCtx.deviceTaintAdd(tCtx.Context)(pair[1])
		}
	// 按名删除规则：先从 store 删除，再以 FinalStateUnknown 触发回调。
	case removeRule:
		name := string(pair)
		store := tCtx.deviceTaints.GetStore()
		// 先确认规则存在。
		obj, exists, err := store.GetByKey(name)
		require.NoError(tCtx, err)
		// 不存在则测试失败。
		if !exists {
			tCtx.Fatalf("cannot delete %s, not found", name)
		}
		// 从 store 移除。
		err = store.Delete(obj)
		require.NoError(tCtx, err)
		// Let's pretend we don't have it...
		// 模拟 informer 只拿到删除 tombstone 的情形。
		tCtx.deviceTaintDelete(tCtx.Context)(resourceinformers.DeletedDeviceTaintRule{FinalStateUnknown: &cache.DeletedFinalStateUnknown{Key: name}})
	}
}

// testContext 聚合 testing.T、上下文、Tracker 与 fake clientset，方便测试复用。
type testContext struct {
	// 嵌入 testing.T，便于直接调用断言与 Fatalf。
	*testing.T
	// 嵌入上下文，供 tracker 与 klog 使用。
	context.Context
	// 嵌入被测 Tracker。
	*Tracker
	// 嵌入 fake Kubernetes 客户端，用于检查生成的 Event。
	*fake.Clientset
}

// withLoggerName 返回一个附加了具名 logger 的上下文副本，便于区分不同事件步骤的日志。
func (t *testContext) withLoggerName(name string) *testContext {
	// 取原 logger 并附加名称。
	logger := klog.FromContext(t.Context)
	logger = klog.LoggerWithName(logger, name)
	// 构造新的 testContext，仅替换 Context 字段。
	t = &testContext{
		T:         t.T,
		Context:   klog.NewContext(t.Context, logger),
		Tracker:   t.Tracker,
		Clientset: t.Clientset,
	}
	return t
}

var (
	// now 是固定的测试基准时间，用于污点的 TimeAdded。
	// driver1、driver2 是两个测试驱动名；pool1、pool2 是两个测试资源池名。
	// device0Name、device1Name、device2Name 是三个测试设备名。
	// sliceWithDevices 深拷贝 slice 并替换其设备列表，避免污染共享 fixtures。
	// slice1NoDevices、slice2NoDevices 是无设备的基准 slice，分别属于驱动1/池1 与驱动2/池2。
	// unchangedSlice 是用于验证“无变化”的占位 slice。
	// deviceWithName、deviceWithTaints 分别返回设置名称、污点后的设备副本。
	// emptyDevice 是构造其它设备的基础；device0、device1、device2 是派生出的具名设备。
	// deviceTaint1、deviceTaint2 是两个 Key 不同的测试污点，便于观察合并结果。
	// deviceTaints 是由 deviceTaint1 组成的污点列表；device1Tainted、device2Tainted 是对应的已打补丁设备。
	// devices、threeDevices、threeDevicesOneTainted、devices2、taintedDevices、taintedDevices2 是常见的设备列表组合。
	// existingDeviceTaints 等模拟设备原本已有污点，用于验证污点合并而非覆盖。
	// slice1、slice1Tainted、slice1AlreadyTainted、slice1MergedTaints 等是由基准 slice 与设备列表组合出的带设备 slice。
	// deviceTaint 从已有污点复制字段，构造等价的污点对象。
	// taintAllDevicesRule 匹配所有设备（空选择器）；taintNoSelectorRule 不带选择器，按实现不匹配任何设备。
	// taintPoolDevicesRule、taintDriverDevicesRule、taintNamedDevicesRule 分别按资源池、驱动、设备名生成选择器，均为深拷贝。
	// taintPool1DevicesRule 等是由通用规则派生出的具体规则实例。
	now, _      = time.Parse(time.RFC3339, "2006-01-02T15:04:05Z")
	driver1     = "driver1.example.com"
	driver2     = "driver2.example.com"
	pool1       = "pool-1"
	pool2       = "pool-2"
	device0Name = "device-0"
	device1Name = "device-1"
	device2Name = "device-2"

	sliceWithDevices = func(slice *resourceapi.ResourceSlice, devices []resourceapi.Device) *resourceapi.ResourceSlice {
		slice = slice.DeepCopy()
		slice.Spec.Devices = devices
		return slice
	}
	slice1NoDevices = &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "s1",
		},
		Spec: resourceapi.ResourceSliceSpec{
			Driver: driver1,
			Pool: resourceapi.ResourcePool{
				Name: pool1,
			},
		},
	}
	slice2NoDevices = &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name: "s2",
		},
		Spec: resourceapi.ResourceSliceSpec{
			Driver: driver2,
			Pool: resourceapi.ResourcePool{
				Name: pool2,
			},
		},
	}
	unchangedSlice = &resourceapi.ResourceSlice{ObjectMeta: metav1.ObjectMeta{Name: "no-change"}}

	deviceWithName = func(device resourceapi.Device, name string) resourceapi.Device {
		device.Name = name
		return device
	}
	deviceWithTaints = func(device resourceapi.Device, taints []resourceapi.DeviceTaint) resourceapi.Device {
		device.Taints = taints
		return device
	}
	emptyDevice = resourceapi.Device{}
	device0     = deviceWithName(emptyDevice, device0Name)
	device1     = deviceWithName(emptyDevice, device1Name)
	device2     = deviceWithName(emptyDevice, device2Name)

	deviceTaint1 = resourceapi.DeviceTaint{
		Key:       "example.com/taint",
		Value:     "tainted",
		Effect:    resourceapi.DeviceTaintEffectNoExecute,
		TimeAdded: &metav1.Time{Time: now},
	}
	deviceTaint2 = resourceapi.DeviceTaint{
		Key:       "example.com/taint2",
		Value:     "tainted2",
		Effect:    resourceapi.DeviceTaintEffectNoExecute,
		TimeAdded: &metav1.Time{Time: now},
	}
	deviceTaints   = []resourceapi.DeviceTaint{deviceTaint1}
	device1Tainted = deviceWithTaints(device1, deviceTaints)
	device2Tainted = deviceWithTaints(device2, deviceTaints)
	devices        = []resourceapi.Device{device1}
	threeDevices   = []resourceapi.Device{
		device0,
		device1,
		device2,
	}
	threeDevicesOneTainted = []resourceapi.Device{
		device0,
		device1Tainted,
		device2,
	}
	devices2        = []resourceapi.Device{device2}
	taintedDevices  = []resourceapi.Device{device1Tainted}
	taintedDevices2 = []resourceapi.Device{device2Tainted}

	existingDeviceTaints   = []resourceapi.DeviceTaint{deviceTaint2}
	existingDevice1Tainted = deviceWithTaints(device1, existingDeviceTaints)
	existingTaintedDevices = []resourceapi.Device{existingDevice1Tainted}
	mergedDeviceTaints     = []resourceapi.DeviceTaint{deviceTaint2, deviceTaint1}
	mergedDevice1Tainted   = deviceWithTaints(device1, mergedDeviceTaints)
	mergedTaintedDevices   = []resourceapi.Device{mergedDevice1Tainted}

	slice1               = sliceWithDevices(slice1NoDevices, devices)
	slice1Tainted        = sliceWithDevices(slice1, taintedDevices)
	slice1AlreadyTainted = sliceWithDevices(slice1, existingTaintedDevices)
	slice1MergedTaints   = sliceWithDevices(slice1, mergedTaintedDevices)
	slice2               = sliceWithDevices(slice2NoDevices, devices2)
	slice2Tainted        = sliceWithDevices(slice2, taintedDevices2)

	deviceTaint = func(taint resourceapi.DeviceTaint) resourceapi.DeviceTaint {
		return resourceapi.DeviceTaint{
			Key:       taint.Key,
			Value:     taint.Value,
			Effect:    resourceapi.DeviceTaintEffect(taint.Effect),
			TimeAdded: taint.TimeAdded,
		}
	}
	taintAllDevicesRule = &resourceapi.DeviceTaintRule{
		ObjectMeta: metav1.ObjectMeta{
			Name: "rule",
		},
		Spec: resourceapi.DeviceTaintRuleSpec{
			DeviceSelector: &resourceapi.DeviceTaintSelector{},
			Taint:          deviceTaint(deviceTaint1),
		},
	}
	taintNoSelectorRule = &resourceapi.DeviceTaintRule{
		ObjectMeta: metav1.ObjectMeta{
			Name: "rule-no-selector",
		},
		Spec: resourceapi.DeviceTaintRuleSpec{
			Taint: deviceTaint(deviceTaint1),
		},
	}
	taintPoolDevicesRule = func(rule *resourceapi.DeviceTaintRule, pool string) *resourceapi.DeviceTaintRule {
		rule = rule.DeepCopy()
		rule.Spec.DeviceSelector = &resourceapi.DeviceTaintSelector{
			Pool: &pool,
		}
		return rule
	}
	taintDriverDevicesRule = func(rule *resourceapi.DeviceTaintRule, driver string) *resourceapi.DeviceTaintRule {
		rule = rule.DeepCopy()
		rule.Spec.DeviceSelector = &resourceapi.DeviceTaintSelector{
			Driver: &driver,
		}
		return rule
	}
	taintNamedDevicesRule = func(rule *resourceapi.DeviceTaintRule, name string) *resourceapi.DeviceTaintRule {
		rule = rule.DeepCopy()
		rule.Spec.DeviceSelector = &resourceapi.DeviceTaintSelector{
			Device: &name,
		}
		return rule
	}
	taintPool1DevicesRule   = taintPoolDevicesRule(taintAllDevicesRule, pool1)
	taintPool2DevicesRule   = taintPoolDevicesRule(taintAllDevicesRule, pool2)
	taintDriver1DevicesRule = taintDriverDevicesRule(taintAllDevicesRule, driver1)
	taintDevice1Rule        = taintNamedDevicesRule(taintAllDevicesRule, device1Name)
)

// TestListPatchedResourceSlices 覆盖 ResourceSlice 与污点规则的增/改/删组合，验证缓存中的补丁结果以及对外发布的事件。
func TestListPatchedResourceSlices(t *testing.T) {
	// test 描述单个测试用例的输入与期望。
	type test struct {
		// events contains pairs of old and new objects which will
		// be passed to event handler methods.
		// Objects can be slices and device taint rules.
		// [add], [remove], and [update] can be used to produce
		// such pairs.
		//
		// Alternatively, it can also contain a list of such pairs.
		// Those will be applied in the order in which they appear
		// in each event entry, but not necessarily in consecutive
		// order. Other events may be placed in between as long as
		// the order in those nested lists is preserved.
		// events 是由“旧对象, 新对象”组成的输入序列，可嵌套以施加相对顺序约束。
		// 嵌套列表内的事件保持相对顺序，但允许与其它事件交错。
		// expectedPatchedSlices 为期望的最终补丁结果（比较前按名称排序）。
		events                []any
		expectedPatchedSlices []*resourceapi.ResourceSlice
		// The exact events that are emitted for a sequence of events is
		// highly dependent on the order in which those events are received.
		// We punt on determining a set of validation criteria for every
		// possible sequence and only check them against the first
		// permutation: the order in which the events are defined.
		// expectedHandlerEvents 只在未打乱顺序的第一种排列下断言，因为事件序列强依赖到达顺序。
		// expectEvents 用于断言生成的 Event，默认要求为空。
		// expectUnhandledErrors 用于断言未被处理的错误，默认要求为空。
		expectedHandlerEvents []handlerEvent
		expectEvents          func(t *assert.CollectT, events *v1.EventList)
		expectUnhandledErrors func(t *testing.T, errs []error)
	}
	// 定义各测试场景。
	tests := map[string]test{
		// 场景：只新增两个 slice，无规则。期望原样保留，并收到两条 Add 事件。
		"add-slices-no-patches": {
			events: []any{
				add(slice1),
				add(slice2),
			},
			expectedPatchedSlices: []*resourceapi.ResourceSlice{
				slice1,
				slice2,
			},
			expectedHandlerEvents: []handlerEvent{
				{event: handlerEventAdd, newObj: slice1},
				{event: handlerEventAdd, newObj: slice2},
			},
		},
		// 场景：先新增无设备 slice 再更新为带设备 slice。期望最终为带设备版本，并依次收到 Add/Update 事件。
		"update-slices-no-patches": {
			events: []any{
				[]any{
					add(slice1NoDevices),
					update(slice1NoDevices, slice1),
				},
				[]any{
					add(slice2NoDevices),
					update(slice2NoDevices, slice2),
				},
				add(unchangedSlice),
			},
			expectedPatchedSlices: []*resourceapi.ResourceSlice{
				slice1,
				slice2,
				unchangedSlice,
			},
			expectedHandlerEvents: []handlerEvent{
				{event: handlerEventAdd, newObj: slice1NoDevices},
				{event: handlerEventUpdate, oldObj: slice1NoDevices, newObj: slice1},
				{event: handlerEventAdd, newObj: slice2NoDevices},
				{event: handlerEventUpdate, oldObj: slice2NoDevices, newObj: slice2},
				{event: handlerEventAdd, newObj: unchangedSlice},
			},
		},
		// 场景：新增后删除 slice。期望缓存中仅剩未变更的 slice，并收到 Add/Delete 事件。
		"delete-slices": {
			events: []any{
				[]any{add(slice1), remove(slice1)},
				[]any{add(slice2), remove(slice2)},
				add(unchangedSlice),
			},
			expectedPatchedSlices: []*resourceapi.ResourceSlice{
				unchangedSlice,
			},
			expectedHandlerEvents: []handlerEvent{
				{event: handlerEventAdd, newObj: slice1},
				{event: handlerEventDelete, oldObj: slice1},
				{event: handlerEventAdd, newObj: slice2},
				{event: handlerEventDelete, oldObj: slice2},
				{event: handlerEventAdd, newObj: unchangedSlice},
			},
		},
		// 场景：先有 slice 再新增“匹配所有”的规则。期望 slice 被加上污点，并收到更新事件。
		"patch-all-slices": {
			events: []any{
				add(slice1),
				add(taintAllDevicesRule),
			},
			expectedPatchedSlices: []*resourceapi.ResourceSlice{
				slice1Tainted,
			},
			expectedHandlerEvents: []handlerEvent{
				{event: handlerEventAdd, newObj: slice1},
				{event: handlerEventUpdate, oldObj: slice1, newObj: slice1Tainted},
			},
		},
		// 场景：规则从池1更新为池2。期望池1的 slice 不受影响、池2的 slice 被加污点。
		"update-patch": {
			events: []any{
				[]any{
					add(taintPool1DevicesRule),
					update(taintPool1DevicesRule, taintPool2DevicesRule),
				},
				add(slice1),
				add(slice2),
			},
			expectedPatchedSlices: []*resourceapi.ResourceSlice{
				slice1,
				slice2Tainted,
			},
			expectedHandlerEvents: []handlerEvent{
				{event: handlerEventAdd, newObj: slice1},
				{event: handlerEventAdd, newObj: slice2Tainted},
			},
		},
		// 场景：删除已知规则对象。期望被其打补丁的 slice 恢复原状。
		"remove-patch-known-deleted-rule": {
			events: []any{[]any{
				add(taintPool2DevicesRule),
				add(slice1),
				add(slice2),
				remove(taintPool2DevicesRule),
			}},
			expectedPatchedSlices: []*resourceapi.ResourceSlice{
				slice1,
				slice2,
			},
			expectedHandlerEvents: []handlerEvent{
				{event: handlerEventAdd, newObj: slice1},
				{event: handlerEventAdd, newObj: slice2Tainted},
				{event: handlerEventUpdate, oldObj: slice2Tainted, newObj: slice2},
			},
		},
		// 场景：仅凭名称删除规则（拿到 tombstone）。期望同样能撤销补丁。
		"remove-patch-unknown-deleted-rule": {
			events: []any{[]any{
				add(taintPool2DevicesRule),
				add(slice1),
				add(slice2),
				removeRule(taintPool2DevicesRule.Name),
			}},
			expectedPatchedSlices: []*resourceapi.ResourceSlice{
				slice1,
				slice2,
			},
			expectedHandlerEvents: []handlerEvent{
				{event: handlerEventAdd, newObj: slice1},
				{event: handlerEventAdd, newObj: slice2Tainted},
				{event: handlerEventUpdate, oldObj: slice2Tainted, newObj: slice2},
			},
		},
		// 场景：规则新增到已带污点的设备。期望新旧污点合并而非覆盖。
		"merge-taints": {
			events: []any{
				add(taintAllDevicesRule),
				add(slice1AlreadyTainted),
			},
			expectedPatchedSlices: []*resourceapi.ResourceSlice{
				slice1MergedTaints,
			},
			expectedHandlerEvents: []handlerEvent{
				{event: handlerEventAdd, newObj: slice1MergedTaints},
			},
		},
		// 场景：按驱动过滤的规则。期望只影响匹配驱动的 slice。
		"add-taint-for-driver": {
			events: []any{
				add(taintDriver1DevicesRule),
				add(slice1),
				add(slice2),
			},
			expectedPatchedSlices: []*resourceapi.ResourceSlice{
				slice1Tainted,
				slice2,
			},
			expectedHandlerEvents: []handlerEvent{
				{event: handlerEventAdd, newObj: slice1Tainted},
				{event: handlerEventAdd, newObj: slice2},
			},
		},
		// 场景：按资源池过滤的规则。期望只影响匹配池的 slice。
		"add-taint-for-pool": {
			events: []any{
				add(taintPool1DevicesRule),
				add(slice1),
				add(slice2),
			},
			expectedPatchedSlices: []*resourceapi.ResourceSlice{
				slice1Tainted,
				slice2,
			},
			expectedHandlerEvents: []handlerEvent{
				{event: handlerEventAdd, newObj: slice1Tainted},
				{event: handlerEventAdd, newObj: slice2},
			},
		},
		// 场景：按设备名过滤的规则。期望只影响含该设备的 slice。
		"add-taint-for-device": {
			events: []any{
				add(taintDevice1Rule),
				add(slice1),
				add(slice2),
			},
			expectedPatchedSlices: []*resourceapi.ResourceSlice{
				slice1Tainted,
				slice2,
			},
			expectedHandlerEvents: []handlerEvent{
				{event: handlerEventAdd, newObj: slice1Tainted},
				{event: handlerEventAdd, newObj: slice2},
			},
		},
		// 场景：规则同时指定驱动、资源池、设备名。期望仅三者都匹配的 slice 被打补丁。
		"filter-all-criteria": {
			events: []any{
				add(
					taintDriverDevicesRule(
						taintPoolDevicesRule(
							taintNamedDevicesRule(
								taintAllDevicesRule,
								device1Name,
							),
							pool1,
						),
						driver1,
					),
				),
				add(slice1),
				add(slice2),
			},
			expectedPatchedSlices: []*resourceapi.ResourceSlice{
				slice1Tainted,
				slice2,
			},
			expectedHandlerEvents: []handlerEvent{
				{event: handlerEventAdd, newObj: slice1Tainted},
				{event: handlerEventAdd, newObj: slice2},
			},
		},
		// 场景：已打补丁的 slice 自身发生设备增删。期望补丁随新设备列表重新计算并发布更新事件。
		"update-patched-slice": {
			events: []any{
				add(taintDevice1Rule),
				[]any{
					add(slice1),
					update(slice1, sliceWithDevices(slice1, threeDevices)),
				},
				[]any{
					add(sliceWithDevices(slice2, threeDevices)),
					update(sliceWithDevices(slice2, threeDevices), sliceWithDevices(slice2, devices)),
				},
			},
			expectedPatchedSlices: []*resourceapi.ResourceSlice{
				sliceWithDevices(slice1, threeDevicesOneTainted),
				sliceWithDevices(slice2, taintedDevices),
			},
			expectedHandlerEvents: []handlerEvent{
				{event: handlerEventAdd, newObj: slice1Tainted},
				{event: handlerEventUpdate, oldObj: slice1Tainted, newObj: sliceWithDevices(slice1, threeDevicesOneTainted)},
				{event: handlerEventAdd, newObj: sliceWithDevices(slice2, threeDevicesOneTainted)},
				{event: handlerEventUpdate, oldObj: sliceWithDevices(slice2, threeDevicesOneTainted), newObj: sliceWithDevices(slice2, taintedDevices)},
			},
		},
		// 场景：规则缺少选择器。期望不匹配任何设备，slice 保持不变。
		"nil-selector-matches-none": {
			events: []any{
				add(slice1),
				add(slice2),
				add(taintNoSelectorRule),
			},
			expectedPatchedSlices: []*resourceapi.ResourceSlice{
				slice1,
				slice2,
			},
			expectedHandlerEvents: []handlerEvent{
				{event: handlerEventAdd, newObj: slice1},
				{event: handlerEventAdd, newObj: slice2},
			},
		},
	}

	// setup 构造测试上下文：fake 客户端、informer 工厂以及启用污点规则的 Tracker。
	setup := func(t *testing.T) *testContext {
		// 创建带 klog 的测试上下文。
		_, ctx := ktesting.NewTestContext(t)

		// fake 客户端用于校验生成的 Event。
		kubeClient := fake.NewSimpleClientset()
		// 创建共享 informer 工厂；测试直接操作其 store，不启动 informer。
		informerFactory := informers.NewSharedInformerFactoryWithOptions(kubeClient, 10*time.Minute)

		// 显式启用污点规则，使 Tracker 走完整处理路径。
		opts := Options{
			EnableDeviceTaintRules: true,
			SliceInformer:          informerFactory.Resource().V1().ResourceSlices(),
			TaintInformer:          informerFactory.Resource().V1().DeviceTaintRules(),
			KubeClient:             kubeClient,
		}
		// 直接构造 Tracker，跳过 StartTracker（测试中不需要真正的同步监控）。
		tracker, err := newTracker(ctx, opts)
		require.NoError(t, err)

		// 组装并返回测试上下文。
		return &testContext{
			T:         t,
			Context:   ctx,
			Tracker:   tracker,
			Clientset: kubeClient,
		}
	}

	// testHandlers 执行单个排列：注册记录事件的处理器、应用输入事件，并对事件、错误、缓存结果与 Kubernetes Event 做断言。
	testHandlers := func(tCtx *testContext, test test, permutation []int) {
		// 标记当前是否为打乱顺序的排列。
		isPermutated := false
		// 只要有一位不满足 i==j，就说明顺序被打乱。
		for i, j := range permutation {
			if i != j {
				isPermutated = true
				break
			}
		}

		// 收集处理器实际收到的事件。
		var handlerEvents []handlerEvent
		// 用函数式处理器记录 Add/Update/Delete。
		handler := cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				handlerEvents = append(handlerEvents, handlerEvent{event: handlerEventAdd, newObj: obj.(*resourceapi.ResourceSlice)})
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				handlerEvents = append(handlerEvents, handlerEvent{event: handlerEventUpdate, oldObj: oldObj.(*resourceapi.ResourceSlice), newObj: newObj.(*resourceapi.ResourceSlice)})
			},
			DeleteFunc: func(obj interface{}) {
				handlerEvents = append(handlerEvents, handlerEvent{event: handlerEventDelete, oldObj: obj.(*resourceapi.ResourceSlice)})
			},
		}
		// 注册处理器，补发的 Add 事件也会被记录。
		_, _ = tCtx.AddEventHandler(handler)

		// 用自定义 handleError 捕获未处理错误。
		var unhandledErrors []error
		// 替换默认错误处理器，便于断言“无错误”。
		tCtx.handleError = func(_ context.Context, err error, _ string, _ ...any) {
			unhandledErrors = append(unhandledErrors, err)
		}

		// 按排列顺序施加所有输入事件。
		runInputEvents(tCtx, test.events, permutation)

		// 仅对第一种（定义顺序）排列断言精确的事件序列。
		if !isPermutated {
			assert.Equal(tCtx, test.expectedHandlerEvents, handlerEvents)
		}

		// 未指定断言函数时默认要求无未处理错误。
		expectUnhandledErrors := test.expectUnhandledErrors
		if expectUnhandledErrors == nil {
			expectUnhandledErrors = func(t *testing.T, errs []error) {
				assert.Empty(t, errs)
			}
		}
		expectUnhandledErrors(tCtx.T, unhandledErrors)

		// Check ResourceSlices
		// 断言最终缓存结果与期望一致。
		patchedResourceSlices, err := tCtx.ListPatchedResourceSlices()
		require.NoError(tCtx, err, "list patched resource slices")
		// 比较前按名称排序，消除顺序影响。
		sortResourceSlicesFunc := func(s1, s2 *resourceapi.ResourceSlice) int {
			return stdcmp.Compare(s1.Name, s2.Name)
		}
		// 对期望值与实际值都排序。
		slices.SortFunc(test.expectedPatchedSlices, sortResourceSlicesFunc)
		slices.SortFunc(patchedResourceSlices, sortResourceSlicesFunc)
		assert.Equal(tCtx, test.expectedPatchedSlices, patchedResourceSlices)
		// 未指定时默认要求不产生 Event。
		expectEvents := test.expectEvents
		if expectEvents == nil {
			expectEvents = func(t *assert.CollectT, events *v1.EventList) {
				assert.Empty(t, events.Items)
			}
		}
		// Events are generated asynchronously. While shutting down the event recorder will flush all
		// pending events, it is not possible to determine when exactly that flush is complete.
		// 在 1 秒内每 10ms 检查一次 Event 列表。
		assert.EventuallyWithT(
			tCtx,
			func(t *assert.CollectT) {
				// 从 fake 客户端列出全部 Event 并交给断言函数。
				events, err := tCtx.CoreV1().Events("").List(tCtx.Context, metav1.ListOptions{})
				require.NoError(t, err, "list events")
				expectEvents(t, events)
			},
			1*time.Second,
			10*time.Millisecond,
			"did not observe expected events",
		)
	}

	// 遍历每个测试场景。
	for name, tc := range tests {
		// 每个场景作为子测试运行。
		t.Run(name, func(t *testing.T) {
			// flatten does one level of flattening of events, counting all events.
			// It also returns a slice of pairs of indices representing ranges which were
			// flattened (= came from the second level) and which therefore must
			// remain in that order.
			// flatten 统计事件总数，并记录嵌套列表的索引范围（这些范围内的事件必须保持相对顺序）。
			flatten := func(events []any) (int, [][2]int) {
				// 统计扁平化后的事件数。
				numEvents := 0
				var ranges [][2]int
				for _, e := range events {
					// 区分单层事件与嵌套列表。
					switch e := e.(type) {
					// 嵌套列表记录其扁平索引区间。
					case []any:
						ranges = append(ranges, [2]int{numEvents, numEvents + len(e)})
						numEvents += len(e)
					default:
						numEvents++
					}
				}
				return numEvents, ranges
			}
			// 得到事件总数与顺序约束。
			numEvents, constraints := flatten(tc.events)

			// 只有一个事件列表时无需排列，直接按原序运行。
			if len(tc.events) <= 1 {
				// No permutations possible.
				// 构造 0..numEvents-1 的自然顺序排列。
				var permutation []int
				for i := 0; i < numEvents; i++ {
					permutation = append(permutation, i)
				}
				// 构造上下文并执行。
				tContext := setup(t)
				testHandlers(tContext, tc, permutation)
				return
			}

			// 多事件场景：生成所有满足约束的排列。
			permutation := make([]int, numEvents)
			// 递归填充排列的第 depth 位。
			var permutate func(depth int)
			// 深度优先生成排列。
			permutate = func(depth int) {
				// 排列完成，创建一个子测试运行它。
				if depth >= numEvents {
					// Define a sub-test which runs the current permutation of events.
					// 用排列内容作为子测试名。
					name := strings.Trim(fmt.Sprintf("%v", permutation), "[]")
					t.Run(name, func(t *testing.T) {
						tContext := setup(t)
						// No need to clone the slice, we don't run in parallel.
						// 排列不并行运行，无需克隆切片。
						testHandlers(tContext, tc, permutation)
					})
					return
				}
				// 带标签的循环，便于在发现顺序冲突时跳过当前候选。
			nexti:
				// 为当前位置挑选一个尚未使用的事件索引。
				for i := range numEvents {
					// 已使用则跳过。
					if slices.Contains(permutation[0:depth], i) {
						// Already taken.
						continue
					}
					// 检查顺序约束。
					for _, constraint := range constraints {
						// 不在当前区间内则不受该约束限制。
						if i < constraint[0] || i > constraint[1] {
							continue
						}
						// 若区间内还有未放置的事件，不能先放后面的，否则会打乱相对顺序。
						for j := i + 1; j < constraint[1]; j++ {
							if slices.Contains(permutation[0:depth], j) {
								// Invalid permutation, would change order
								// of sub-events.
								// 该候选会破坏嵌套列表内事件的相对顺序，跳过。
								continue nexti
							}
						}
					}

					// Pick it for the current position in permutation,
					// continue with next position.
					// 选定当前位置并递归处理下一位。
					permutation[depth] = i
					permutate(depth + 1)
				}
			}
			// 从第 0 位开始生成排列。
			permutate(0)
		})
	}
}

// BenchmarkEventHandlers 对多种“slice 数量 × 规则数量 × 触发方式”组合基准测试事件处理开销。
func BenchmarkEventHandlers(b *testing.B) {
	// 基准中污点使用当前时间。
	now := time.Now()
	// 每个基准包含预置的 slice、规则以及要循环执行的处理函数。
	benchmarks := map[string]struct {
		resourceSlices []*resourceapi.ResourceSlice
		taintRules     []*resourceapi.DeviceTaintRule
		loop           func(ctx context.Context, b *testing.B, tracker *Tracker, resourceSlices []*resourceapi.ResourceSlice, taintRules []*resourceapi.DeviceTaintRule, i int)
	}{
		// 基准1：无规则时新增 slice 的开销（1000 个 slice，每个 64 个设备）。
		"resource-slice-add-no-taint-rules": {
			// 构造 1000 个无设备污点的 slice。
			resourceSlices: func() []*resourceapi.ResourceSlice {
				resourceSlices := make([]*resourceapi.ResourceSlice, 1000)
				for i := range resourceSlices {
					resourceSlices[i] = &resourceapi.ResourceSlice{
						ObjectMeta: metav1.ObjectMeta{
							Name: "slice-" + strconv.Itoa(i),
						},
						Spec: resourceapi.ResourceSliceSpec{
							Devices: slices.Repeat([]resourceapi.Device{}, 64),
						},
					}
				}
				return resourceSlices
			}(),
			// 每轮触发一次 slice 新增回调。
			loop: func(ctx context.Context, b *testing.B, tracker *Tracker, resourceSlices []*resourceapi.ResourceSlice, _ []*resourceapi.DeviceTaintRule, i int) {
				tracker.resourceSliceAdd(ctx)(resourceSlices[i%len(resourceSlices)])
			},
		},
		// 基准2：一条匹配全部 slice 的规则新增时，需重算 500 个 slice。
		"one-patch-to-many-slices-add-taint-rule": {
			resourceSlices: func() []*resourceapi.ResourceSlice {
				resourceSlices := make([]*resourceapi.ResourceSlice, 500)
				for i := range resourceSlices {
					resourceSlices[i] = &resourceapi.ResourceSlice{
						ObjectMeta: metav1.ObjectMeta{
							Name: "slice-" + strconv.Itoa(i),
						},
						Spec: resourceapi.ResourceSliceSpec{
							Devices: slices.Repeat([]resourceapi.Device{{}}, 64),
						},
					}
				}
				return resourceSlices
			}(),
			// 空选择器规则匹配所有 slice。
			taintRules: []*resourceapi.DeviceTaintRule{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "taintRule",
					},
					Spec: resourceapi.DeviceTaintRuleSpec{
						DeviceSelector: &resourceapi.DeviceTaintSelector{}, // all slices
						Taint: resourceapi.DeviceTaint{
							Key:       "example.com/taint",
							Value:     "tainted",
							Effect:    resourceapi.DeviceTaintEffectNoExecute,
							TimeAdded: &metav1.Time{Time: now},
						},
					},
				},
			},
			loop: func(ctx context.Context, b *testing.B, tracker *Tracker, resourceSlices []*resourceapi.ResourceSlice, taintRules []*resourceapi.DeviceTaintRule, i int) {
				tracker.deviceTaintAdd(ctx)(taintRules[i%len(taintRules)])
			},
		},
		// 基准3：已存在全局规则时新增 slice。
		"one-patch-to-many-slices-add-slice": {
			resourceSlices: func() []*resourceapi.ResourceSlice {
				resourceSlices := make([]*resourceapi.ResourceSlice, 500)
				for i := range resourceSlices {
					resourceSlices[i] = &resourceapi.ResourceSlice{
						ObjectMeta: metav1.ObjectMeta{
							Name: "slice-" + strconv.Itoa(i),
						},
						Spec: resourceapi.ResourceSliceSpec{
							Devices: slices.Repeat([]resourceapi.Device{{}}, 64),
						},
					}
				}
				return resourceSlices
			}(),
			taintRules: []*resourceapi.DeviceTaintRule{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "taintRule",
					},
					Spec: resourceapi.DeviceTaintRuleSpec{
						DeviceSelector: &resourceapi.DeviceTaintSelector{}, // all slices
						Taint: resourceapi.DeviceTaint{
							Key:       "example.com/taint",
							Value:     "tainted",
							Effect:    resourceapi.DeviceTaintEffectNoExecute,
							TimeAdded: &metav1.Time{Time: now},
						},
					},
				},
			},
			loop: func(ctx context.Context, b *testing.B, tracker *Tracker, resourceSlices []*resourceapi.ResourceSlice, _ []*resourceapi.DeviceTaintRule, i int) {
				tracker.resourceSliceAdd(ctx)(resourceSlices[i%len(resourceSlices)])
			},
		},
		// 基准4：500 个 slice 中仅一个设备匹配规则，衡量索引反查效率。
		"one-patched-device-among-many-slices-add-taint-rule": {
			resourceSlices: func() []*resourceapi.ResourceSlice {
				// 500 个 slice、每个 64 个设备。
				nSlices := 500
				nDevices := 64
				resourceSlices := make([]*resourceapi.ResourceSlice, nSlices)
				for i := range resourceSlices {
					resourceSlices[i] = &resourceapi.ResourceSlice{
						ObjectMeta: metav1.ObjectMeta{
							Name: "slice-" + strconv.Itoa(i),
						},
						Spec: resourceapi.ResourceSliceSpec{
							Pool: resourceapi.ResourcePool{
								Name: "pool-" + strconv.Itoa(i),
							},
							Devices: func() []resourceapi.Device {
								devices := make([]resourceapi.Device, nDevices)
								for j := range devices {
									devices[j] = resourceapi.Device{
										Name: "device-" + strconv.Itoa(j),
									}
								}
								return devices
							}(),
						},
					}
				}
				// 仅将中间 slice 的一个设备改名为“patchme”作为唯一匹配项。
				resourceSlices[nSlices/2].Spec.Devices[nDevices/2].Name = "patchme"
				return resourceSlices
			}(),
			taintRules: []*resourceapi.DeviceTaintRule{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "taintRule",
					},
					Spec: resourceapi.DeviceTaintRuleSpec{
						DeviceSelector: &resourceapi.DeviceTaintSelector{
							Device: ptr.To("patchme"),
						},
						Taint: resourceapi.DeviceTaint{
							Key:       "example.com/taint",
							Value:     "tainted",
							Effect:    resourceapi.DeviceTaintEffectNoExecute,
							TimeAdded: &metav1.Time{Time: now},
						},
					},
				},
			},
			// 每轮触发一次规则新增回调。
			loop: func(ctx context.Context, b *testing.B, tracker *Tracker, resourceSlices []*resourceapi.ResourceSlice, taintRules []*resourceapi.DeviceTaintRule, i int) {
				tracker.deviceTaintAdd(ctx)(taintRules[i%len(taintRules)])
			},
		},
		// 基准5：规则按“池+设备”精确匹配，新增该 slice 时的开销。
		"one-patched-device-among-many-slices-add-slice": {
			resourceSlices: func() []*resourceapi.ResourceSlice {
				resourceSlices := make([]*resourceapi.ResourceSlice, 500)
				for i := range resourceSlices {
					resourceSlices[i] = &resourceapi.ResourceSlice{
						ObjectMeta: metav1.ObjectMeta{
							Name: "slice-" + strconv.Itoa(i),
						},
						Spec: resourceapi.ResourceSliceSpec{
							Pool: resourceapi.ResourcePool{
								Name: "pool-" + strconv.Itoa(i),
							},
							Devices: func() []resourceapi.Device {
								nDevices := 64
								devices := slices.Repeat([]resourceapi.Device{{}}, nDevices)
								// 每个 slice 的中间设备都叫“patchme”，但规则还限定了池名。
								devices[nDevices/2].Name = "patchme"
								return devices
							}(),
						},
					}
				}
				return resourceSlices
			}(),
			taintRules: []*resourceapi.DeviceTaintRule{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "patch",
					},
					Spec: resourceapi.DeviceTaintRuleSpec{
						DeviceSelector: &resourceapi.DeviceTaintSelector{
							Pool:   ptr.To("pool-250"),
							Device: ptr.To("patchme"),
						},
						Taint: resourceapi.DeviceTaint{
							Key:       "example.com/taint",
							Value:     "tainted",
							Effect:    resourceapi.DeviceTaintEffectNoExecute,
							TimeAdded: &metav1.Time{Time: now},
						},
					},
				},
			},
			// 只新增受规则影响的那个 slice（索引 250）。
			loop: func(ctx context.Context, b *testing.B, tracker *Tracker, resourceSlices []*resourceapi.ResourceSlice, patches []*resourceapi.DeviceTaintRule, i int) {
				tracker.resourceSliceAdd(ctx)(resourceSlices[250]) // the slice affected by the patch
			},
		},
		// 基准6：500 个 slice 各有自己的规则，新增规则时需精确定位到单个 slice。
		"one-patch-for-each-of-many-slices-add-taint-rule": {
			resourceSlices: func() []*resourceapi.ResourceSlice {
				resourceSlices := make([]*resourceapi.ResourceSlice, 500)
				for i := range resourceSlices {
					resourceSlices[i] = &resourceapi.ResourceSlice{
						ObjectMeta: metav1.ObjectMeta{
							Name: "slice-" + strconv.Itoa(i),
						},
						Spec: resourceapi.ResourceSliceSpec{
							Pool: resourceapi.ResourcePool{
								Name: "pool-" + strconv.Itoa(i),
							},
							Devices: slices.Repeat([]resourceapi.Device{{}}, 64),
						},
					}
				}
				return resourceSlices
			}(),
			// 为每个池生成一条对应的规则。
			taintRules: func() []*resourceapi.DeviceTaintRule {
				patches := make([]*resourceapi.DeviceTaintRule, 500)
				for i := range patches {
					patches[i] = &resourceapi.DeviceTaintRule{
						ObjectMeta: metav1.ObjectMeta{
							Name: "taint-rule-" + strconv.Itoa(i),
						},
						Spec: resourceapi.DeviceTaintRuleSpec{
							DeviceSelector: &resourceapi.DeviceTaintSelector{
								Pool: ptr.To("pool-" + strconv.Itoa(i)),
							},
							Taint: resourceapi.DeviceTaint{
								Key:       "example.com/taint",
								Value:     "tainted",
								Effect:    resourceapi.DeviceTaintEffectNoExecute,
								TimeAdded: &metav1.Time{Time: now},
							},
						},
					}
				}
				return patches
			}(),
			loop: func(ctx context.Context, b *testing.B, tracker *Tracker, resourceSlices []*resourceapi.ResourceSlice, taintRules []*resourceapi.DeviceTaintRule, i int) {
				tracker.deviceTaintAdd(ctx)(taintRules[i%len(taintRules)])
			},
		},
	}

	// newBenchTracker 构造基准用 Tracker，并将意外错误直接记为基准失败。
	newBenchTracker := func(ctx context.Context) *Tracker {
		kubeClient := fake.NewSimpleClientset()
		informerFactory := informers.NewSharedInformerFactoryWithOptions(kubeClient, 10*time.Minute)
		opts := Options{
			EnableDeviceTaintRules: true,
			SliceInformer:          informerFactory.Resource().V1().ResourceSlices(),
			TaintInformer:          informerFactory.Resource().V1().DeviceTaintRules(),
			KubeClient:             kubeClient,
		}
		// 构造 Tracker。
		tracker, err := newTracker(ctx, opts)
		require.NoError(b, err)
		// 基准中出现未处理错误即失败。
		tracker.handleError = func(_ context.Context, err error, _ string, _ ...any) {
			b.Error("unexpected unhandled error:", err)
		}
		return tracker
	}

	// 依次运行各基准。
	for name, benchmark := range benchmarks {
		// 每个基准作为子基准运行。
		b.Run(name, func(b *testing.B) {
			// 降低日志级别以减少干扰。
			logger, ctx := ktesting.NewTestContext(b)
			ctx = klog.NewContext(ctx, logger.V(2))
			tracker := newBenchTracker(ctx)

			// 预置 slice 到索引器（不计入计时）。
			for _, slice := range benchmark.resourceSlices {
				err := tracker.resourceSlices.GetIndexer().Add(slice)
				require.NoError(b, err)
			}

			// 预置规则到索引器。
			for _, taintRule := range benchmark.taintRules {
				err := tracker.deviceTaints.GetIndexer().Add(taintRule)
				require.NoError(b, err)
			}

			// 重置计时，只测量循环部分。
			b.ResetTimer()
			// 执行 b.N 次被测操作。
			for i := range b.N {
				benchmark.loop(ctx, b, tracker, benchmark.resourceSlices, benchmark.taintRules, i)
			}
		})
	}
}
