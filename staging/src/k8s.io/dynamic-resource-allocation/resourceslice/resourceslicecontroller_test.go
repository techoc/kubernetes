/*
Copyright 2024 The Kubernetes Authors.

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

package resourceslice

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/dynamic-resource-allocation/internal/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/utils/ptr"
)

// init 注册 klog 的命令行 flag，使测试可用 -v 等参数控制日志输出。
func init() {
	klog.InitFlags(nil)
}

// TestControllerSyncPool 是 syncPool 的主表驱动测试：
// 给定一批初始 ResourceSlice 与驱动期望的 DriverResources，创建控制器并手动排空工作队列后，
// 断言 apiserver 中最终存在的切片、创建/更新/删除统计以及错误回调内容完全匹配预期。
// 期望值中的 ResourceVersion 递增用于区分 Update 与 Create API 调用。
func TestControllerSyncPool(t *testing.T) {

	// 测试公用的 fixture：切片命名前缀、设备名、节点选择器和污点时间戳。
	var (
		// fixture 变量分组说明：ownerName 与 nodeUID 组成节点本地资源的属主引用；driverName 与 poolName 决定切片的驱动与池名；
		// deviceName 及其编号变体是基础设备名；generateName1 到 4 是索引编码的预分配名称前缀，resourceSlice1 到 3 是对应实际名称；
		// attrs 是触发更新的新属性；nodeSelector 与 otherNodeSelector 是两个互斥节点选择器；timeAdded 及其变体是污点时间戳，用于验证复制与秒级舍入。
		ownerName      = "owner"
		nodeUID        = types.UID("node-uid")
		driverName     = "driver"
		poolName       = "pool"
		deviceName     = "device"
		deviceName1    = "device-1"
		deviceName2    = "device-2"
		deviceName3    = "device-3"
		deviceName4    = "device-4"
		generateName1  = encodeIndex(0, resourceSliceIndexMinLength) + "-" + driverName + "-" + ownerName + "-"
		generateName2  = encodeIndex(1, resourceSliceIndexMinLength) + "-" + driverName + "-" + ownerName + "-"
		generateName3  = encodeIndex(2, resourceSliceIndexMinLength) + "-" + driverName + "-" + ownerName + "-"
		generateName4  = encodeIndex(3, resourceSliceIndexMinLength) + "-" + driverName + "-" + ownerName + "-"
		resourceSlice1 = generateName1 + "0"
		resourceSlice2 = generateName2 + "1"
		resourceSlice3 = generateName3 + "2"
		attrs          = map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
			"new-attribute": {StringValue: ptr.To("value")},
		}
		nodeSelector = &v1.NodeSelector{
			NodeSelectorTerms: []v1.NodeSelectorTerm{{
				MatchFields: []v1.NodeSelectorRequirement{{
					Key:    "name",
					Values: []string{"node-a"},
				}},
			}},
		}
		otherNodeSelector = &v1.NodeSelector{
			NodeSelectorTerms: []v1.NodeSelectorTerm{{
				MatchFields: []v1.NodeSelectorRequirement{{
					Key:    "name",
					Values: []string{"node-b"},
				}},
			}},
		}
		// 污点时间戳精确到秒；后续变体用于验证时间复制与舍入行为。
		timeAdded                 = metav1.Time{Time: time.Now().Round(time.Second)}
		timeAddedLater            = metav1.Time{Time: timeAdded.Add(time.Minute)}
		timeAddedEarlier          = metav1.Time{Time: timeAdded.Add(-time.Minute)}
		timeAddedEarlierSubSecond = metav1.Time{Time: timeAddedEarlier.Add(100 * time.Millisecond)}
	)

	// 表驱动用例：键为场景名，值包含特性开关、初始对象、期望状态、统计与错误。
	testCases := map[string]struct {
		// 表驱动用例字段说明：features 模拟 apiserver 关闭特性后的字段丢弃；syncDelay 为同步延迟，0 表示事件立即入队；
		// nodeUID 非空时 owner 为 Node，为空时为普通应用资源；noOwner 为 true 时完全不设置 owner；
		// reconcilePoolWithName 非空时只协调指定 pool（issue #137011）且不设置 NodeName；
		// initialObjects 与 initialOtherObjects 分别是初始切片和其他对象；inputDriverResources 是驱动上报的期望状态，nil 表示不调用 Update；
		// expectedResourceSlices 与 expectedStats 分别是期望的最终切片和 API 调用统计；expectedErrors 是期望的错误信息集合。
		features               features
		syncDelay              *time.Duration
		nodeUID                types.UID
		noOwner                bool
		reconcilePoolWithName  string
		initialObjects         []runtime.Object
		initialOtherObjects    []runtime.Object
		inputDriverResources   *DriverResources
		expectedResourceSlices []resourceapi.ResourceSlice
		expectedStats          Stats
		expectedErrors         []string
	}{
		// 场景：池中无切片，驱动期望一个空设备切片，应新建并携带 Node 属主与 NodeName。
		"create-slice": {
			nodeUID:        nodeUID,
			initialObjects: []runtime.Object{},
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Slices: []Slice{{Devices: []resourceapi.Device{}}},
					},
				},
			},
			expectedStats: Stats{
				NumCreates: 1,
			},
			expectedResourceSlices: []resourceapi.ResourceSlice{
				*MakeResourceSlice().Name(resourceSlice1).GenerateName(generateName1).
					ResourceVersion("1").
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).Obj(),
			},
		},
		// 场景：新建切片携带 SkipNodeOperations 字段，应原样写入。
		"create-slice-with-skip-node-operations": {
			nodeUID:        nodeUID,
			initialObjects: []runtime.Object{},
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Slices: []Slice{{
							Devices:            []resourceapi.Device{},
							SkipNodeOperations: []resourceapi.SkipNodeOperation{resourceapi.SkipNodeOperationAll},
						}},
					},
				},
			},
			expectedStats: Stats{
				NumCreates: 1,
			},
			expectedResourceSlices: []resourceapi.ResourceSlice{
				*MakeResourceSlice().Name(resourceSlice1).GenerateName(generateName1).
					ResourceVersion("1").
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{}).
					SkipNodeOperations(resourceapi.SkipNodeOperationAll).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).Obj(),
			},
		},
		// 场景：现有切片与期望完全一致（含 Generation），不应产生任何写操作。
		"keep-slice-unchanged": {
			nodeUID: nodeUID,
			initialObjects: []runtime.Object{
				MakeResourceSlice().Name(resourceSlice1).GenerateName(generateName1).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).Obj(),
			},
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Generation: 1,
						Slices:     []Slice{{Devices: []resourceapi.Device{newDevice(deviceName)}}},
					},
				},
			},
			expectedResourceSlices: []resourceapi.ResourceSlice{
				*MakeResourceSlice().Name(resourceSlice1).GenerateName(generateName1).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).Obj(),
			},
		},
		// 场景：驱动上报的污点没有 TimeAdded，应保留现有切片上的时间，避免无谓更新。
		"keep-taint-unchanged": {
			nodeUID: nodeUID,
			initialObjects: []runtime.Object{
				MakeResourceSlice().Name(resourceSlice1).GenerateName(generateName1).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).
					Devices([]resourceapi.Device{
						newDevice(
							deviceName,
							resourceapi.DeviceTaint{
								Effect:    resourceapi.DeviceTaintEffectNoExecute,
								TimeAdded: &timeAdded,
							},
						)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).
					Obj(),
			},
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Generation: 1,
						Slices: []Slice{{Devices: []resourceapi.Device{
							newDevice(
								deviceName,
								resourceapi.DeviceTaint{
									Effect: resourceapi.DeviceTaintEffectNoExecute,
								},
							),
						}}},
					},
				},
			},
			expectedResourceSlices: []resourceapi.ResourceSlice{
				*MakeResourceSlice().Name(resourceSlice1).GenerateName(generateName1).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).
					Devices([]resourceapi.Device{
						newDevice(
							deviceName,
							resourceapi.DeviceTaint{
								Effect:    resourceapi.DeviceTaintEffectNoExecute,
								TimeAdded: &timeAdded,
							},
						)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).
					Obj(),
			},
		},
		// 场景：驱动新增两个污点并保留原有污点；已有时间被复制、缺失时间由 reactor 补齐并舍入，触发一次更新。
		"add-taints": {
			nodeUID: nodeUID,
			initialObjects: []runtime.Object{
				MakeResourceSlice().Name(resourceSlice1).GenerateName(generateName1).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).
					Devices([]resourceapi.Device{
						newDevice(
							deviceName,
							resourceapi.DeviceTaint{
								Effect:    resourceapi.DeviceTaintEffectNoExecute,
								TimeAdded: &timeAdded,
							},
						)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).
					Obj(),
			},
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Generation: 1,
						Slices: []Slice{{Devices: []resourceapi.Device{
							newDevice(
								deviceName,
								[]resourceapi.DeviceTaint{
									{
										Effect: resourceapi.DeviceTaintEffectNoExecute,
									},
									{
										Key:    "example.com/tainted",
										Effect: resourceapi.DeviceTaintEffectNoSchedule,
									},
									{
										Key:       "example.com/tainted2",
										Effect:    resourceapi.DeviceTaintEffectNoExecute,
										TimeAdded: &timeAddedEarlierSubSecond, // 控制器与 apiserver 往返时都会取整。
									},
								},
							),
						}}},
					},
				},
			},
			expectedStats: Stats{
				NumUpdates: 1,
			},
			expectedResourceSlices: []resourceapi.ResourceSlice{
				*MakeResourceSlice().Name(resourceSlice1).GenerateName(generateName1).
					ResourceVersion("1").
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).
					Devices([]resourceapi.Device{
						newDevice(
							deviceName,
							[]resourceapi.DeviceTaint{
								{
									Effect:    resourceapi.DeviceTaintEffectNoExecute,
									TimeAdded: &timeAdded,
								},
								{
									Key:       "example.com/tainted",
									Effect:    resourceapi.DeviceTaintEffectNoSchedule,
									TimeAdded: &timeAddedLater,
								},
								{
									Key:       "example.com/tainted2",
									Effect:    resourceapi.DeviceTaintEffectNoExecute,
									TimeAdded: &timeAddedEarlier,
								},
							},
						)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).
					Obj(),
			},
		},
		// 场景：关闭 DRADeviceTaints 后污点字段被 apiserver 丢弃，控制器记录字段丢弃错误。
		"drop-taints": {
			features: features{disableDeviceTaints: true},
			nodeUID:  nodeUID,
			initialObjects: []runtime.Object{
				MakeResourceSlice().Name(resourceSlice1).GenerateName(generateName1).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).
					Devices([]resourceapi.Device{newDevice(deviceName)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).
					Obj(),
			},
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Generation: 1,
						Slices: []Slice{{Devices: []resourceapi.Device{
							newDevice(
								deviceName,
								resourceapi.DeviceTaint{
									Effect: resourceapi.DeviceTaintEffectNoExecute,
								},
							),
						}}},
					},
				},
			},
			expectedStats: Stats{
				NumUpdates: 1,
			},
			expectedResourceSlices: []resourceapi.ResourceSlice{
				*MakeResourceSlice().Name(resourceSlice1).GenerateName(generateName1).
					ResourceVersion("1").
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).
					Devices([]resourceapi.Device{newDevice(deviceName)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).
					Obj(),
			},
			expectedErrors: []string{`update ResourceSlice: pool "pool", slice #0: some fields were dropped by the apiserver, probably because these features are disabled: DRADeviceTaints`},
		},
		// 场景：关闭 DRAConsumableCapacity 后 AllowMultipleAllocations 被丢弃，记录错误。
		"drop-consumable-capacity-field": {
			features: features{disableConsumableCapacity: true},
			nodeUID:  nodeUID,
			initialObjects: []runtime.Object{
				MakeResourceSlice().Name(resourceSlice1).GenerateName(generateName1).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).
					Devices([]resourceapi.Device{newDevice(deviceName)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).
					Obj(),
			},
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Generation: 1,
						Slices: []Slice{{Devices: []resourceapi.Device{
							newDevice(
								deviceName,
								allowMultipleAllocationsField(true),
							),
						}}},
					},
				},
			},
			expectedStats: Stats{
				NumUpdates: 1,
			},
			expectedResourceSlices: []resourceapi.ResourceSlice{
				*MakeResourceSlice().Name(resourceSlice1).GenerateName(generateName1).
					ResourceVersion("1").
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).
					Devices([]resourceapi.Device{newDevice(deviceName)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).
					Obj(),
			},
			expectedErrors: []string{`update ResourceSlice: pool "pool", slice #0: some fields were dropped by the apiserver, probably because these features are disabled: DRAConsumableCapacity`},
		},
		// 场景：关闭 DRAOptionalNodeOperations 后 SkipNodeOperations 被丢弃，记录错误。
		"drop-optional-node-operations-field": {
			features: features{disableOptionalNodeOperations: true},
			nodeUID:  nodeUID,
			initialObjects: []runtime.Object{
				MakeResourceSlice().Name(resourceSlice1).GenerateName(generateName1).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).
					Devices([]resourceapi.Device{newDevice(deviceName)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).
					Obj(),
			},
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Generation: 1,
						Slices: []Slice{{
							Devices:            []resourceapi.Device{newDevice(deviceName)},
							SkipNodeOperations: []resourceapi.SkipNodeOperation{resourceapi.SkipNodeOperationAll},
						}},
					},
				},
			},
			expectedStats: Stats{
				NumUpdates: 1,
			},
			expectedResourceSlices: []resourceapi.ResourceSlice{
				*MakeResourceSlice().Name(resourceSlice1).GenerateName(generateName1).
					ResourceVersion("1").
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).
					Devices([]resourceapi.Device{newDevice(deviceName)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).
					Obj(),
			},
			expectedErrors: []string{`update ResourceSlice: pool "pool", slice #0: some fields were dropped by the apiserver, probably because these features are disabled: DRAOptionalNodeOperations`},
		},
		// 场景：关闭 DRADeviceCompatibilityGroups 后 CompatibilityGroups 被丢弃；仍创建两个切片但记录错误。
		"drop-compatibility-groups-field": {
			features: features{disableCompatibilityGroups: true},
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Generation: 1,
						Slices: []Slice{
							{
								PerDeviceNodeSelection: new(true),
								SharedCounters: []resourceapi.CounterSet{{
									Name: "gpu-0",
									Counters: map[string]resourceapi.Counter{
										"mem": {Value: resource.MustParse("1")},
									},
								}},
							},
							{
								PerDeviceNodeSelection: new(true),
								Devices: []resourceapi.Device{
									newDevice(
										deviceName,
										nodeNameField(ownerName),
										[]resourceapi.DeviceCounterConsumption{{
											CounterSet: "gpu-0",
											Counters: map[string]resourceapi.Counter{
												"mem": {Value: resource.MustParse("1")},
											},
											CompatibilityGroups: []string{"group1", "group2"},
										}},
									),
								},
							},
						},
					},
				},
			},
			expectedStats: Stats{
				NumCreates: 2,
			},
			expectedResourceSlices: []resourceapi.ResourceSlice{
				*MakeResourceSlice().Name(resourceSlice1).GenerateName(generateName1).
					ResourceVersion("1").
					AppOwnerReferences(ownerName).
					AllNodes(false).
					NodeName("").
					NodeSelector(nil).
					PerDeviceNodeSelection(true).
					SharedCounters([]resourceapi.CounterSet{{
						Name: "gpu-0",
						Counters: map[string]resourceapi.Counter{
							"mem": {Value: resource.MustParse("1")},
						},
					}}).
					Driver(driverName).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 2}).
					Obj(),
				*MakeResourceSlice().Name(resourceSlice2).GenerateName(generateName2).
					ResourceVersion("1").
					AppOwnerReferences(ownerName).
					AllNodes(false).
					NodeName("").
					NodeSelector(nil).
					PerDeviceNodeSelection(true).
					Driver(driverName).
					Devices([]resourceapi.Device{
						newDevice(
							deviceName,
							nodeNameField(ownerName),
							resourceapi.DeviceCounterConsumption{
								CounterSet: "gpu-0",
								Counters: map[string]resourceapi.Counter{
									"mem": {Value: resource.MustParse("1")},
								},
							},
						),
					}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 2}).
					Obj(),
			},
			expectedErrors: []string{`create ResourceSlice: pool "pool", slice #1: some fields were dropped by the apiserver, probably because these features are disabled: DRADeviceCompatibilityGroups`},
		},
		// 场景：驱动不再上报任何 pool，现有切片应被删除；syncDelay=0 确保初始对象立即触发协调。
		"remove-pool": {
			nodeUID:   nodeUID,
			syncDelay: ptr.To(time.Duration(0)), // 确保初始对象触发 pool 的立即同步。
			initialObjects: []runtime.Object{
				MakeResourceSlice().Name(resourceSlice1).UID(resourceSlice1).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).Obj(),
			},
			inputDriverResources: &DriverResources{},
			expectedStats: Stats{
				NumDeletes: 1,
			},
			expectedResourceSlices: nil,
		},
		// 场景：一个现有、一个期望切片，按索引匹配后原地更新而非删除重建。
		"one-existing-and-one-desired-slice-should-be-updated-inplace": {
			nodeUID: nodeUID,
			initialObjects: []runtime.Object{
				MakeResourceSlice().Name(resourceSlice1).UID(resourceSlice1).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).Obj(),
			},
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Slices: []Slice{{Devices: []resourceapi.Device{newDevice(deviceName)}}}},
				},
			},
			expectedStats: Stats{
				NumUpdates: 1,
			},
			expectedResourceSlices: []resourceapi.ResourceSlice{
				*MakeResourceSlice().Name(resourceSlice1).UID(resourceSlice1).ResourceVersion("1").
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{{Name: deviceName}}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).Obj(),
			},
		},
		// 场景：两个现有、两个期望切片，首个空设备切片被原地更新，第二个保持不变。
		"update-slice-when-more-than-one-existing-or-desired-slice": {
			nodeUID: nodeUID,
			initialObjects: []runtime.Object{
				MakeResourceSlice().Name(resourceSlice1).GenerateName(generateName1).UID(resourceSlice1).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 2}).Obj(),
				MakeResourceSlice().Name(resourceSlice2).GenerateName(generateName2).UID(resourceSlice2).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{{Name: deviceName2}}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 2}).Obj(),
			},
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Slices: []Slice{
							{Devices: []resourceapi.Device{{Name: deviceName1}}},
							{Devices: []resourceapi.Device{{Name: deviceName2}}},
						},
					},
				},
			},
			expectedStats: Stats{
				NumUpdates: 1,
			},
			expectedResourceSlices: []resourceapi.ResourceSlice{
				*MakeResourceSlice().Name(resourceSlice1).GenerateName(generateName1).UID(resourceSlice1).ResourceVersion("1").
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName1)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 2}).Obj(),
				*MakeResourceSlice().Name(resourceSlice2).GenerateName(generateName2).UID(resourceSlice2).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName2)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 2}).Obj(),
			},
		},
		// 场景：两个内容相同的现有切片但期望只有一个，应删除冗余切片。
		"delete-redundant-slice": {
			nodeUID: nodeUID,
			initialObjects: []runtime.Object{
				MakeResourceSlice().Name(resourceSlice1).UID(resourceSlice1).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).Obj(),
				MakeResourceSlice().Name(resourceSlice2).UID(resourceSlice2).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).Obj(),
			},
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Slices: []Slice{{Devices: []resourceapi.Device{newDevice(deviceName)}}}},
				},
			},
			expectedStats: Stats{
				NumDeletes: 1,
			},
			expectedResourceSlices: []resourceapi.ResourceSlice{
				*MakeResourceSlice().Name(resourceSlice1).UID(resourceSlice1).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).Obj(),
			},
		},
		// 场景：设备属性发生变化，应就地更新切片并递增 ResourceVersion。
		"update-slice": {
			nodeUID: nodeUID,
			initialObjects: []runtime.Object{
				MakeResourceSlice().Name(resourceSlice1).UID(resourceSlice1).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).Obj(),
			},
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Slices: []Slice{{
							Devices: []resourceapi.Device{newDevice(deviceName, attrs)},
						}},
					},
				},
			},
			expectedStats: Stats{
				NumUpdates: 1,
			},
			expectedResourceSlices: []resourceapi.ResourceSlice{
				*MakeResourceSlice().Name(resourceSlice1).UID(resourceSlice1).ResourceVersion("1").
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName, attrs)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).Obj(),
			},
		},
		// 场景：多设备切片中仅一个设备的属性变化，仍只做一次更新并保留其余设备。
		"update-slice-many-devices": {
			nodeUID: nodeUID,
			initialObjects: []runtime.Object{
				MakeResourceSlice().Name(resourceSlice1).UID(resourceSlice1).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName1), newDevice(deviceName2)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).Obj(),
			},
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Slices: []Slice{{
							Devices: []resourceapi.Device{
								newDevice(deviceName1),
								newDevice(deviceName2, attrs),
							},
						}},
					},
				},
			},
			expectedStats: Stats{
				NumUpdates: 1,
			},
			expectedResourceSlices: []resourceapi.ResourceSlice{
				*MakeResourceSlice().Name(resourceSlice1).UID(resourceSlice1).ResourceVersion("1").
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).
					Devices([]resourceapi.Device{
						newDevice(deviceName1),
						newDevice(deviceName2, attrs),
					}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).Obj(),
			},
		},
		// 场景：三个现有切片与期望完全一致，不产生任何写操作。
		"multiple-resourceslices-existing-no-changes": {
			nodeUID: nodeUID,
			initialObjects: []runtime.Object{
				MakeResourceSlice().Name(resourceSlice1).UID(resourceSlice1).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName1)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 3}).Obj(),
				MakeResourceSlice().Name(resourceSlice2).UID(resourceSlice2).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName2)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 3}).Obj(),
				MakeResourceSlice().Name(resourceSlice3).UID(resourceSlice3).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName3)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 3}).Obj(),
			},
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Slices: []Slice{
							{Devices: []resourceapi.Device{newDevice(deviceName1)}},
							{Devices: []resourceapi.Device{newDevice(deviceName2)}},
							{Devices: []resourceapi.Device{newDevice(deviceName3)}},
						},
					},
				},
			},
			expectedResourceSlices: []resourceapi.ResourceSlice{
				*MakeResourceSlice().Name(resourceSlice1).UID(resourceSlice1).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName1)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 3}).Obj(),
				*MakeResourceSlice().Name(resourceSlice2).UID(resourceSlice2).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName2)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 3}).Obj(),
				*MakeResourceSlice().Name(resourceSlice3).UID(resourceSlice3).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName3)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 3}).Obj(),
			},
		},
		// 场景：现有切片的 pool.Generation 不一致，全部删除后按索引重新创建三个切片。
		"multiple-resourceslices-existing-with-different-resource-pool-generation": {
			nodeUID: nodeUID,
			initialObjects: []runtime.Object{
				MakeResourceSlice().Name(resourceSlice1).UID(resourceSlice1).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).Obj(),
				MakeResourceSlice().Name(resourceSlice2).UID(resourceSlice2).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 2, ResourceSliceCount: 1}).Obj(),
				MakeResourceSlice().Name(resourceSlice3).UID(resourceSlice3).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).Obj(),
			},
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Slices: []Slice{{
							Devices: []resourceapi.Device{
								{
									Name: deviceName,
								},
							},
						}},
					},
				},
			},
			expectedStats: Stats{
				NumCreates: 1,
				NumDeletes: 3,
			},
			expectedResourceSlices: []resourceapi.ResourceSlice{
				*MakeResourceSlice().Name(resourceSlice1).GenerateName(generateName1).
					ResourceVersion("1").
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 3, ResourceSliceCount: 1}).Obj(),
			},
		},
		// 场景：三个切片中仅一个设备属性变化，Generation 不变，只更新该切片。
		"multiple-resourceslices-existing-one-changed": {
			nodeUID: nodeUID,
			initialObjects: []runtime.Object{
				MakeResourceSlice().Name(resourceSlice1).UID(resourceSlice1).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName1)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 3}).Obj(),
				MakeResourceSlice().Name(resourceSlice2).UID(resourceSlice2).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName2)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 3}).Obj(),
				MakeResourceSlice().Name(resourceSlice3).UID(resourceSlice3).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName3)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 3}).Obj(),
			},
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Slices: []Slice{
							{Devices: []resourceapi.Device{newDevice(deviceName1)}},
							{Devices: []resourceapi.Device{newDevice(deviceName2, attrs)}},
							{Devices: []resourceapi.Device{newDevice(deviceName3)}},
						},
					},
				},
			},
			expectedStats: Stats{
				NumUpdates: 1,
			},
			expectedResourceSlices: []resourceapi.ResourceSlice{
				*MakeResourceSlice().Name(resourceSlice1).UID(resourceSlice1).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName1)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 3}).Obj(),
				*MakeResourceSlice().Name(resourceSlice2).UID(resourceSlice2).ResourceVersion("1").
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName2, attrs)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 3}).Obj(),
				*MakeResourceSlice().Name(resourceSlice3).UID(resourceSlice3).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName3)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 3}).Obj(),
			},
		},
		// 场景：两个切片的设备属性变化导致 Generation 递增，三个切片全部被更新。
		"multiple-resourceslices-existing-two-changed": {
			nodeUID: nodeUID,
			initialObjects: []runtime.Object{
				MakeResourceSlice().Name(resourceSlice1).UID(resourceSlice1).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName1)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 3}).Obj(),
				MakeResourceSlice().Name(resourceSlice2).UID(resourceSlice2).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName2)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 3}).Obj(),
				MakeResourceSlice().Name(resourceSlice3).UID(resourceSlice3).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName3)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 3}).Obj(),
			},
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Slices: []Slice{
							{Devices: []resourceapi.Device{newDevice(deviceName1)}},
							{Devices: []resourceapi.Device{newDevice(deviceName2, attrs)}},
							{Devices: []resourceapi.Device{newDevice(deviceName3, attrs)}},
						},
					},
				},
			},
			expectedStats: Stats{
				NumUpdates: 3,
			},
			expectedResourceSlices: []resourceapi.ResourceSlice{
				*MakeResourceSlice().Name(resourceSlice1).UID(resourceSlice1).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).ResourceVersion("1").
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName1)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 2, ResourceSliceCount: 3}).Obj(),
				*MakeResourceSlice().Name(resourceSlice2).UID(resourceSlice2).ResourceVersion("1").
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName2, attrs)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 2, ResourceSliceCount: 3}).Obj(),
				*MakeResourceSlice().Name(resourceSlice3).UID(resourceSlice3).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).ResourceVersion("1").
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName3, attrs)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 2, ResourceSliceCount: 3}).Obj(),
			},
		},
		// 场景：期望少一个切片，Generation 递增，两个保留切片全部更新并删除一个。
		"multiple-resourceslices-existing-one-removed": {
			nodeUID: nodeUID,
			initialObjects: []runtime.Object{
				MakeResourceSlice().Name(resourceSlice1).UID(resourceSlice1).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName1)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 3}).Obj(),
				MakeResourceSlice().Name(resourceSlice2).UID(resourceSlice2).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName2)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 3}).Obj(),
				MakeResourceSlice().Name(resourceSlice3).UID(resourceSlice3).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName3)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 3}).Obj(),
			},
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Slices: []Slice{
							{Devices: []resourceapi.Device{newDevice(deviceName1)}},
							{Devices: []resourceapi.Device{newDevice(deviceName2)}},
						},
					},
				},
			},
			expectedStats: Stats{
				NumUpdates: 2,
				NumDeletes: 1,
			},
			expectedResourceSlices: []resourceapi.ResourceSlice{
				*MakeResourceSlice().Name(resourceSlice1).UID(resourceSlice1).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).ResourceVersion("1").
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName1)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 2, ResourceSliceCount: 2}).Obj(),
				*MakeResourceSlice().Name(resourceSlice2).UID(resourceSlice2).ResourceVersion("1").
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName2)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 2, ResourceSliceCount: 2}).Obj(),
			},
		},
		// 场景：期望多一个切片，Generation 递增，三个旧切片全部更新并新建一个索引切片。
		"multiple-resourceslices-existing-one-added": {
			nodeUID: nodeUID,
			initialObjects: []runtime.Object{
				MakeResourceSlice().Name(resourceSlice1).UID(resourceSlice1).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName1)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 3}).Obj(),
				MakeResourceSlice().Name(resourceSlice2).UID(resourceSlice2).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName2)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 3}).Obj(),
				MakeResourceSlice().Name(resourceSlice3).UID(resourceSlice3).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName3)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 3}).Obj(),
			},
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Slices: []Slice{
							{Devices: []resourceapi.Device{newDevice(deviceName1)}},
							{Devices: []resourceapi.Device{newDevice(deviceName2)}},
							{Devices: []resourceapi.Device{newDevice(deviceName3)}},
							{Devices: []resourceapi.Device{newDevice(deviceName4)}},
						},
					},
				},
			},
			expectedStats: Stats{
				NumUpdates: 3,
				NumCreates: 1,
			},
			expectedResourceSlices: []resourceapi.ResourceSlice{
				*MakeResourceSlice().Name(resourceSlice1).UID(resourceSlice1).ResourceVersion("1").
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName1)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 2, ResourceSliceCount: 4}).Obj(),
				*MakeResourceSlice().Name(resourceSlice2).UID(resourceSlice2).ResourceVersion("1").
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName2)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 2, ResourceSliceCount: 4}).Obj(),
				*MakeResourceSlice().Name(resourceSlice3).UID(resourceSlice3).ResourceVersion("1").
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName3)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 2, ResourceSliceCount: 4}).Obj(),
				*MakeResourceSlice().Name(generateName4+"0").GenerateName(generateName4).
					ResourceVersion("1").
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName4)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 2, ResourceSliceCount: 4}).Obj(),
			},
		},
		// 场景：网络附加设备（无 owner、AllNodes=true），新建切片不携带 NodeName 与 NodeSelector。
		"add-one-network-device-all-nodes": {
			initialObjects: []runtime.Object{},
			noOwner:        true,
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Slices:   []Slice{{Devices: []resourceapi.Device{newDevice(deviceName)}}},
						AllNodes: true,
					},
				},
			},
			expectedStats: Stats{
				NumCreates: 1,
			},
			expectedResourceSlices: []resourceapi.ResourceSlice{
				*MakeResourceSlice().Name(encodeIndex(0, resourceSliceIndexMinLength) + "-" + driverName + "-0").
					GenerateName(encodeIndex(0, resourceSliceIndexMinLength) + "-" + driverName + "-").
					ResourceVersion("1").
					AllNodes(true).
					NodeName("").
					NodeSelector(nil).
					PerDeviceNodeSelection(false).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).Obj(),
			},
		},
		// 场景：带 pool 级 NodeSelector 的非节点本地资源，新建切片携带 App 属主与选择器。
		"add-one-network-device-some-nodes": {
			initialObjects: []runtime.Object{},
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						NodeSelector: nodeSelector,
						Slices:       []Slice{{Devices: []resourceapi.Device{newDevice(deviceName)}}},
					},
				},
			},
			expectedStats: Stats{
				NumCreates: 1,
			},
			expectedResourceSlices: []resourceapi.ResourceSlice{
				*MakeResourceSlice().Name(generateName1 + "0").GenerateName(generateName1).
					ResourceVersion("1").
					AppOwnerReferences(ownerName).NodeSelector(nodeSelector).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).Obj(),
			},
		},
		// 场景：按 pool 名协调时，即使 owner 是 Node 也不为切片设置 NodeName。
		"reconcile-with-pool-name-create-slice-no-node-name": {
			nodeUID:               nodeUID,
			reconcilePoolWithName: poolName,
			initialObjects:        []runtime.Object{},
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Slices:   []Slice{{Devices: []resourceapi.Device{}}},
						AllNodes: true,
					},
				},
			},
			expectedStats: Stats{NumCreates: 1},
			expectedResourceSlices: []resourceapi.ResourceSlice{
				*MakeResourceSlice().Name(resourceSlice1).GenerateName(generateName1).
					ResourceVersion("1").
					NodeOwnerReferences(ownerName, string(nodeUID)).
					AllNodes(true).
					NodeName("").
					NodeSelector(nil).
					PerDeviceNodeSelection(false).
					Driver(driverName).Devices([]resourceapi.Device{}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).Obj(),
			},
		},
		// 场景：按 pool 名协调且带 NodeSelector，选择器应被写入切片。
		"reconcile-with-pool-name-with-node-selector": {
			nodeUID:               nodeUID,
			reconcilePoolWithName: poolName,
			initialObjects:        []runtime.Object{},
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						NodeSelector: nodeSelector,
						Slices:       []Slice{{Devices: []resourceapi.Device{newDevice(deviceName)}}},
					},
				},
			},
			expectedStats: Stats{NumCreates: 1},
			expectedResourceSlices: []resourceapi.ResourceSlice{
				*MakeResourceSlice().Name(resourceSlice1).GenerateName(generateName1).
					ResourceVersion("1").
					NodeOwnerReferences(ownerName, string(nodeUID)).
					NodeSelector(nodeSelector).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).Obj(),
			},
		},
		// 场景：按 pool 名协调且启用逐设备节点选择，标志与设备级选择器都应保留。
		"reconcile-with-pool-name-with-perdevice-node-selector": {
			nodeUID:               nodeUID,
			reconcilePoolWithName: poolName,
			initialObjects:        []runtime.Object{},
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Slices: []Slice{{Devices: []resourceapi.Device{newDevice(deviceName, nodeSelector)}, PerDeviceNodeSelection: ptr.To(true)}},
					},
				},
			},
			expectedStats: Stats{NumCreates: 1},
			expectedResourceSlices: []resourceapi.ResourceSlice{
				*MakeResourceSlice().Name(resourceSlice1).GenerateName(generateName1).
					ResourceVersion("1").
					NodeOwnerReferences(ownerName, string(nodeUID)).
					PerDeviceNodeSelection(true).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName, nodeSelector)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).Obj(),
			},
		},
		// 场景：pool 的 NodeSelector 改变，应更新现有切片的 NodeSelector。
		"update-node-selector": {
			initialObjects: []runtime.Object{
				MakeResourceSlice().Name(resourceSlice1).UID(resourceSlice1).
					AppOwnerReferences(ownerName).NodeSelector(nodeSelector).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).Obj(),
			},
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						NodeSelector: otherNodeSelector,
						Slices:       []Slice{{Devices: []resourceapi.Device{newDevice(deviceName)}}},
					},
				},
			},
			expectedStats: Stats{
				NumUpdates: 1,
			},
			expectedResourceSlices: []resourceapi.ResourceSlice{
				*MakeResourceSlice().Name(resourceSlice1).UID(resourceSlice1).ResourceVersion("1").
					AppOwnerReferences(ownerName).NodeSelector(otherNodeSelector).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).Obj(),
			},
		},
		// 场景：可分区设备——一个切片放共享计数器，另一个放消费设备，均标记为逐设备选择。
		"create-partitionable-devices": {
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Generation: 1,
						Slices: []Slice{
							{
								PerDeviceNodeSelection: ptr.To(true),
								SharedCounters: []resourceapi.CounterSet{{
									Name: "gpu-0",
									Counters: map[string]resourceapi.Counter{
										"mem": {Value: resource.MustParse("1")},
									},
								}},
							},
							{
								PerDeviceNodeSelection: ptr.To(true),
								Devices: []resourceapi.Device{
									newDevice(
										deviceName,
										nodeNameField(ownerName),
										[]resourceapi.DeviceCounterConsumption{{
											CounterSet: "gpu-0",
											Counters: map[string]resourceapi.Counter{
												"mem": {Value: resource.MustParse("1")},
											},
										}},
									),
								},
							},
						},
					},
				},
			},
			expectedStats: Stats{
				NumCreates: 2,
			},
			expectedResourceSlices: []resourceapi.ResourceSlice{
				*MakeResourceSlice().Name(resourceSlice1).GenerateName(generateName1).
					ResourceVersion("1").
					AppOwnerReferences(ownerName).
					AllNodes(false).
					NodeName("").
					NodeSelector(nil).
					PerDeviceNodeSelection(true).
					SharedCounters([]resourceapi.CounterSet{{
						Name: "gpu-0",
						Counters: map[string]resourceapi.Counter{
							"mem": {Value: resource.MustParse("1")},
						},
					}}).
					Driver(driverName).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 2}).
					Obj(),
				*MakeResourceSlice().Name(resourceSlice2).GenerateName(generateName2).
					ResourceVersion("1").
					AppOwnerReferences(ownerName).
					AllNodes(false).
					NodeName("").
					NodeSelector(nil).
					PerDeviceNodeSelection(true).
					Driver(driverName).
					Devices([]resourceapi.Device{
						newDevice(
							deviceName,
							nodeNameField(ownerName),
							resourceapi.DeviceCounterConsumption{
								CounterSet: "gpu-0",
								Counters: map[string]resourceapi.Counter{
									"mem": {Value: resource.MustParse("1")},
								},
							},
						),
					}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 2}).
					Obj(),
			},
		},
		// 场景：关闭 DRAPartitionableDevices 后逐设备选择与共享计数器字段被丢弃，记录两条错误。
		"drop-partitionable-devices": {
			features: features{disablePartitionableDevices: true},
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Generation: 1,
						Slices: []Slice{
							{
								PerDeviceNodeSelection: ptr.To(true),
								SharedCounters: []resourceapi.CounterSet{{
									Name: "gpu-0",
									Counters: map[string]resourceapi.Counter{
										"mem": {Value: resource.MustParse("1")},
									},
								}},
							},
							{
								PerDeviceNodeSelection: ptr.To(true),
								Devices: []resourceapi.Device{
									newDevice(
										deviceName,
										nodeNameField(ownerName),
										resourceapi.DeviceCounterConsumption{
											CounterSet: "gpu-0",
											Counters: map[string]resourceapi.Counter{
												"mem": {Value: resource.MustParse("1")},
											},
										},
									),
								},
							},
						},
					},
				},
			},
			expectedStats: Stats{
				NumCreates: 2,
			},
			expectedResourceSlices: []resourceapi.ResourceSlice{
				*MakeResourceSlice().Name(resourceSlice1).GenerateName(generateName1).
					ResourceVersion("1").
					AppOwnerReferences(ownerName).
					AllNodes(false).
					NodeName("").
					NodeSelector(nil).
					PerDeviceNodeSelection(false). // 应当丢弃。
					Driver(driverName).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 2}).
					Obj(),
				*MakeResourceSlice().Name(resourceSlice2).GenerateName(generateName2).
					ResourceVersion("1").
					AppOwnerReferences(ownerName).
					AllNodes(false).
					NodeName("").
					NodeSelector(nil).
					PerDeviceNodeSelection(false). // 应当丢弃。
					Driver(driverName).
					Devices([]resourceapi.Device{newDevice(deviceName)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 2}).
					Obj(),
			},
			expectedErrors: []string{
				`create ResourceSlice: pool "pool", slice #0: some fields were dropped by the apiserver, probably because these features are disabled: DRAPartitionableDevices`,
				`create ResourceSlice: pool "pool", slice #1: some fields were dropped by the apiserver, probably because these features are disabled: DRAPartitionableDevices`,
			},
		},
		// 场景：设备携带绑定条件、失败条件与 BindsToNode，应完整写入切片。
		"create-device-with-binding-condition": {
			nodeUID: nodeUID,
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Generation: 1,
						Slices: []Slice{{
							Devices: func() []resourceapi.Device {
								d := newDevice(deviceName)
								d.BindingConditions = []string{"condition1", "condition2"}
								d.BindingFailureConditions = []string{"failure-condition1"}
								d.BindsToNode = ptr.To(true)
								return []resourceapi.Device{d}
							}(),
						}},
					},
				},
			},
			expectedStats: Stats{
				NumCreates: 1,
			},
			expectedResourceSlices: []resourceapi.ResourceSlice{
				*MakeResourceSlice().Name(resourceSlice1).GenerateName(generateName1).
					ResourceVersion("1").
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).
					Devices(func() []resourceapi.Device {
						d := newDevice(deviceName)
						d.BindingConditions = []string{"condition1", "condition2"}
						d.BindingFailureConditions = []string{"failure-condition1"}
						d.BindsToNode = ptr.To(true)
						return []resourceapi.Device{d}
					}()).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).
					Obj(),
			},
		},
		// 场景：关闭 DRADeviceBindingConditions 后绑定条件字段被丢弃，记录错误。
		"drop-device-with-binding-condition": {
			features: features{disableBindingConditions: true},
			nodeUID:  nodeUID,
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Generation: 1,
						Slices: []Slice{{
							Devices: func() []resourceapi.Device {
								d := newDevice(deviceName)
								d.BindingConditions = []string{"condition1", "condition2"}
								d.BindingFailureConditions = []string{"failure-condition1"}
								d.BindsToNode = ptr.To(true)
								return []resourceapi.Device{d}
							}(),
						}},
					},
				},
			},
			expectedStats: Stats{
				NumCreates: 1,
			},
			expectedResourceSlices: []resourceapi.ResourceSlice{
				*MakeResourceSlice().Name(resourceSlice1).GenerateName(generateName1).
					ResourceVersion("1").
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).
					Devices([]resourceapi.Device{newDevice(deviceName)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).
					Obj(),
			},
			expectedErrors: []string{`create ResourceSlice: pool "pool", slice #0: some fields were dropped by the apiserver, probably because these features are disabled: DRADeviceBindingConditions`},
		},
		// 场景：同一 pool 中出现重名 CounterSet，池校验失败，不创建任何切片。
		"detect-resource-pools-with-duplicate-counter-sets": {
			nodeUID: nodeUID,
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Generation: 1,
						Slices: []Slice{
							{
								SharedCounters: []resourceapi.CounterSet{
									{
										Name: "counterset",
									},
								},
							},
							{
								SharedCounters: []resourceapi.CounterSet{
									{
										Name: "counterset",
									},
								},
							},
						},
					},
				},
			},
			expectedStats: Stats{
				NumCreates: 0,
			},
			expectedErrors: []string{`pool validation failed: found duplicate counter set "counterset" in pool "pool"`},
		},
		// 场景：同一 pool 中出现重名设备，池校验失败，不创建任何切片。
		"detect-duplicate-devices": {
			nodeUID: nodeUID,
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Generation: 1,
						Slices: []Slice{
							{
								Devices: []resourceapi.Device{
									{
										Name: deviceName,
									},
								},
							},
							{
								Devices: []resourceapi.Device{
									{
										Name: deviceName,
									},
								},
							},
						},
					},
				},
			},
			expectedStats: Stats{
				NumCreates: 0,
			},
			expectedErrors: []string{`pool validation failed: found duplicate device "device" in pool "pool"`},
		},
		// 场景：设备引用不存在的 CounterSet，池校验失败。
		"detect-device-referencing-unknown-counter-set": {
			nodeUID: nodeUID,
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Generation: 1,
						Slices: []Slice{
							{
								Devices: []resourceapi.Device{
									{
										Name: deviceName,
										ConsumesCounters: []resourceapi.DeviceCounterConsumption{
											{
												CounterSet: "counterset",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedStats: Stats{
				NumCreates: 0,
			},
			expectedErrors: []string{`pool validation failed: counter set "counterset" referenced by device "device" not found`},
		},
		// 场景：设备引用 CounterSet 中不存在的计数器，池校验失败。
		"detect-device-referencing-unknown-counter-in-counter-set": {
			nodeUID: nodeUID,
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Generation: 1,
						Slices: []Slice{
							{
								SharedCounters: []resourceapi.CounterSet{
									{
										Name: "counterset",
										Counters: map[string]resourceapi.Counter{
											"memory": {
												Value: resource.MustParse("8Gi"),
											},
										},
									},
								},
							},
							{
								Devices: []resourceapi.Device{
									{
										Name: "device",
										ConsumesCounters: []resourceapi.DeviceCounterConsumption{
											{
												CounterSet: "counterset",
												Counters: map[string]resourceapi.Counter{
													"cpu": {
														Value: resource.MustParse("4"),
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectedStats: Stats{
				NumCreates: 0,
			},
			expectedErrors: []string{`pool validation failed: counter "cpu" referenced by device "device" not found in counter set "counterset"`},
		},
		// 场景：三个随机命名的旧切片全部替换为索引化命名的新切片（3 建 3 删）。
		"migration-from-random-naming-to-index-based-naming": {
			nodeUID: nodeUID,
			initialObjects: []runtime.Object{
				MakeResourceSlice().Name("random-name1").
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName1)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 3}).Obj(),
				MakeResourceSlice().Name("random-name2").
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName2)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 3}).Obj(),
				MakeResourceSlice().Name("random-name3").
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName3)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 3}).Obj(),
			},
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Slices: []Slice{
							{Devices: []resourceapi.Device{newDevice(deviceName1)}},
							{Devices: []resourceapi.Device{newDevice(deviceName2)}},
							{Devices: []resourceapi.Device{newDevice(deviceName3)}},
						},
					},
				},
			},
			expectedStats: Stats{
				NumCreates: 3,
				NumDeletes: 3,
			},
			expectedResourceSlices: []resourceapi.ResourceSlice{
				*MakeResourceSlice().Name(resourceSlice1).GenerateName(generateName1).
					ResourceVersion("1").
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName1)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 2, ResourceSliceCount: 3}).Obj(),
				*MakeResourceSlice().Name(resourceSlice2).GenerateName(generateName2).
					ResourceVersion("1").
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName2)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 2, ResourceSliceCount: 3}).Obj(),
				*MakeResourceSlice().Name(resourceSlice3).GenerateName(generateName3).
					ResourceVersion("1").
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName3)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 2, ResourceSliceCount: 3}).Obj(),
			},
		},
		// 场景：随机命名与索引命名混存，仅符合索引约定的切片被原地更新，其余重建（2 建 2 删 1 更新）。
		"migration-mixed-naming": {
			nodeUID: nodeUID,
			initialObjects: []runtime.Object{
				MakeResourceSlice().Name("random-name1").
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName1)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 3}).Obj(),
				MakeResourceSlice().Name(resourceSlice2).GenerateName(generateName2).UID(resourceSlice2).
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName2)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 3}).Obj(),
				MakeResourceSlice().Name("random-name3").
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName3)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 3}).Obj(),
			},
			inputDriverResources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {
						Slices: []Slice{
							{Devices: []resourceapi.Device{newDevice(deviceName1)}},
							{Devices: []resourceapi.Device{newDevice(deviceName2)}},
							{Devices: []resourceapi.Device{newDevice(deviceName3)}},
						},
					},
				},
			},
			expectedStats: Stats{
				NumCreates: 2, // 针对索引 0 和 2
				NumDeletes: 2, // 针对 random-name1 和 random-name3
				NumUpdates: 1, // resourceSlice2 更新到 Generation 2
			},
			expectedResourceSlices: []resourceapi.ResourceSlice{
				*MakeResourceSlice().Name(generateName1+"0").GenerateName(generateName1).
					ResourceVersion("1").
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName1)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 2, ResourceSliceCount: 3}).Obj(),
				*MakeResourceSlice().Name(resourceSlice2).GenerateName(generateName2).UID(resourceSlice2).ResourceVersion("1").
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName2)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 2, ResourceSliceCount: 3}).Obj(),
				*MakeResourceSlice().Name(generateName3+"1").GenerateName(generateName3).
					ResourceVersion("1").
					NodeOwnerReferences(ownerName, string(nodeUID)).NodeName(ownerName).
					Driver(driverName).Devices([]resourceapi.Device{newDevice(deviceName3)}).
					Pool(resourceapi.ResourcePool{Name: poolName, Generation: 2, ResourceSliceCount: 3}).Obj(),
			},
		},
	}
	// 遍历所有用例，每个用例在独立的测试上下文中运行。
	for name, test := range testCases {
		t.Run(name, func(t *testing.T) {
			// 创建带 klog 的测试上下文，便于控制器输出可追踪的日志。
			_, ctx := ktesting.NewTestContext(t)
			// 汇总初始对象：先放其他类型对象，再放 ResourceSlice。
			inputObjects := make([]runtime.Object, 0, len(test.initialObjects)+len(test.initialOtherObjects))
			// 深拷贝初始对象，避免不同用例间相互污染。
			for _, initialOtherObject := range test.initialOtherObjects {
				inputObjects = append(inputObjects, initialOtherObject.DeepCopyObject())
			}
			// 初始切片必须是 ResourceSlice 指针，否则测试直接失败。
			for _, initialObject := range test.initialObjects {
				if _, ok := initialObject.(*resourceapi.ResourceSlice); !ok {
					t.Fatalf("test.initialObjects have to be of type *resourceapi.ResourceSlice")
				}
				inputObjects = append(inputObjects, initialObject.DeepCopyObject())
			}
			// 构造 fake client，并用 reactor 模拟版本号递增、GenerateName 与禁用字段丢弃。
			kubeClient := createTestClient(test.features, timeAddedLater, inputObjects...)
			// 使用 mock 工作队列，便于精确断言入队与延迟状态。
			var queue workqueue.Mock[string]
			// 按 nodeUID 与 noOwner 构造期望的 owner：Node、普通应用资源或 nil。
			owner := &Owner{
				APIVersion: "v1",
				Kind:       "Node",
				Name:       ownerName,
				UID:        test.nodeUID,
			}
			if test.nodeUID == "" {
				owner = &Owner{
					APIVersion: "apps/v1",
					Kind:       "Something",
					Name:       ownerName,
				}
			}
			if test.noOwner {
				owner = nil
			}
			// 收集控制器通过 ErrorHandler 报告的错误。
			var controllerErrors []error
			// 用用例参数创建控制器；newController 会初始化 informer 并将 pool 入队，随后手动调用 run 排空队列。
			ctrl, err := newController(ctx, Options{
				DriverName: driverName,
				KubeClient: kubeClient,
				Owner:      owner,
				Resources:  test.inputDriverResources,
				Queue:      &queue,
				SyncDelay:  test.syncDelay,
				ErrorHandler: func(ctx context.Context, err error, msg string) {
					controllerErrors = append(controllerErrors, fmt.Errorf("%s: %w", msg, err))
				},
				ReconcilePoolWithName: test.reconcilePoolWithName,
			})
			defer ctrl.Stop()
			require.NoError(t, err, "unexpected controller creation error")

			// 手动运行控制器，处理队列中的工作项直到队列清空。
			// 处理过程会与 informer 入队竞争；首轮应已达到期望状态，后续轮次都应为空操作。
			ctrl.run(ctx)

			// 列出 apiserver 中的全部切片，排序后与期望逐项比较，同时校验调用统计。
			resourceSlices, err := kubeClient.ResourceV1().ResourceSlices().List(ctx, metav1.ListOptions{})
			require.NoError(t, err, "list resource slices")

			// 排序消除列表顺序差异（map 中取出的初始对象顺序不稳定）。
			sortResourceSlices(test.expectedResourceSlices)
			sortResourceSlices(resourceSlices.Items)
			assert.Equal(t, test.expectedResourceSlices, resourceSlices.Items)
			assert.Equal(t, test.expectedStats, ctrl.GetStats())

			// 队列状态断言：informer 可能在 ctrl.run 返回前后入队，Later 因而可能是 TTL 复核延迟或普通同步延迟，无法比较。
			actualState := queue.State()
			actualState.Later = nil
			// syncDelay 为 0 时事件处理会立即入队，与 ctrl.run 竞争，因此 Ready 也无法比较。
			if test.syncDelay != nil && *test.syncDelay == 0 {
				actualState.Ready = nil
			}
			// 期望队列为空，因此使用零值状态作为比较基准。
			var expectState workqueue.MockState[string]
			assert.Equal(t, expectState, actualState)

			// 二次协调：再次入队所有 pool，状态未变时统计不应增长，用于验证幂等性。
			if test.inputDriverResources != nil {
				for poolName := range test.inputDriverResources.Pools {
					queue.Add(poolName)
				}
				ctrl.run(ctx)
				assert.Equal(t, test.expectedStats, ctrl.GetStats(), "statistics after re-sync")
			}

			// 同步了两次可能导致错误重复，先按错误信息去重再比较。
			var dedupedControllerErrors []error
			errorMsgs := sets.New[string]()
			for _, err := range controllerErrors {
				errorMsg := err.Error()
				if errorMsgs.Has(errorMsg) {
					continue
				}
				errorMsgs.Insert(errorMsg)
				dedupedControllerErrors = append(dedupedControllerErrors, err)
			}

			// 停止控制器后再断言错误，避免后台协程继续追加错误。
			ctrl.Stop()
			// 按“有无错误、数量、集合内容”逐层比较，给出可读的失败信息。
			switch {
			case len(test.expectedErrors) != 0 && len(dedupedControllerErrors) == 0:
				t.Errorf("expected errors, got none: %s", joinErrors(test.expectedErrors))
			case len(test.expectedErrors) == 0 && len(dedupedControllerErrors) > 0:
				t.Errorf("expected no error, got:\n  %s", joinErrors(formatErrors(controllerErrors)))
			case len(test.expectedErrors) != len(dedupedControllerErrors):
				t.Errorf("expected %d errors, got %d:\n  %s", len(test.expectedErrors), len(dedupedControllerErrors), joinErrors(formatErrors(dedupedControllerErrors)))
			default:
				expectedErrorsSet := sets.New(test.expectedErrors...)
				actualErrorsSet := sets.New(errsToStrings(dedupedControllerErrors)...)
				if !expectedErrorsSet.Equal(actualErrorsSet) {
					t.Errorf("expected errors:\n  %s\ngot:\n  %s", joinErrors(test.expectedErrors), joinErrors(formatErrors(dedupedControllerErrors)))
				}
			}
		})
	}
}

// TestControllerUpdateDeleteRecreate 回归“更新后被删除的切片无法重建”的 bug：
// 控制器更新切片会在 MutationCache 中留下条目，若切片随后被 apiserver 删除，
// 陈旧缓存会让控制器误以为切片仍存在而跳过创建。修复方式是 informer 删除回调调用
// OnDelete 在 informer 删除回调中、工作项处理前清除缓存，避免陈旧更新对象阻止重建。
// 测试先等待一次更新，再删除切片，最后断言发生了重建。
func TestControllerUpdateDeleteRecreate(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)

	// 驱动名与资源池名常量。
	const (
		driverName = "driver"
		poolName   = "pool"
	)
	// 索引化 GenerateName 及对应的实际切片名。
	generateName := encodeIndex(0, resourceSliceIndexMinLength) + "-" + driverName + "-"
	sliceName := generateName + "0"

	// 初始切片：面向所有节点且携带旧设备，等待被更新为新设备。
	initialSlice := MakeResourceSlice().
		Name(sliceName).
		UID("original-uid").
		GenerateName(generateName).
		ResourceVersion("1").
		Driver(driverName).
		AllNodes(true).
		Devices([]resourceapi.Device{newDevice("old-device")}).
		Pool(resourceapi.ResourcePool{Name: poolName, Generation: 1, ResourceSliceCount: 1}).
		Obj()

	// fake client 使用零值时间戳（初始切片无污点）；syncDelay=0 让事件立即入队。
	kubeClient := createTestClient(features{}, metav1.Time{}, initialSlice)
	syncDelay := time.Duration(0)

	// 启动真实 informer 循环，期望状态为一个新设备切片。
	ctrl, err := StartController(ctx, Options{
		DriverName: driverName,
		KubeClient: kubeClient,
		Resources: &DriverResources{
			Pools: map[string]Pool{
				poolName: {
					AllNodes:   true,
					Generation: 1,
					Slices:     []Slice{{Devices: []resourceapi.Device{newDevice("new-device")}}},
				},
			},
		},
		SyncDelay: &syncDelay,
	})
	require.NoError(t, err)
	defer ctrl.Stop()

	// 等待控制器完成第一次更新。
	require.Eventually(t, func() bool {
		return ctrl.GetStats().NumUpdates >= 1
	}, 5*time.Second, 10*time.Millisecond, "controller should update the initial slice")

	// 模拟驱动重启后 kubelet 删除切片，验证删除回调清除了陈旧缓存。
	require.NoError(t, kubeClient.ResourceV1().ResourceSlices().Delete(ctx, sliceName, metav1.DeleteOptions{}))

	// 等待控制器重新创建被删除的切片。
	require.Eventually(t, func() bool {
		return ctrl.GetStats().NumCreates >= 1
	}, 5*time.Second, 10*time.Millisecond, "controller should recreate the deleted slice")
}

// TestControllerUpdateReconcilePoolWithNameValidation 验证按 pool 名协调时的 Update 校验：
// pool 集合必须为空或恰好包含指定名称的一个 pool，否则通过 ErrorHandler 报告错误。
func TestControllerUpdateReconcilePoolWithNameValidation(t *testing.T) {
	const poolName = "pool"

	// 表驱动用例：输入驱动资源与期望错误信息。
	testcases := map[string]struct {
		// 表驱动用例字段说明：resources 是驱动上报的 pool 集合；expectedErrors 是期望错误中应包含的子串，为空表示期望无错误。
		resources      *DriverResources
		expectedErrors []string
	}{
		// 用例：两个 pool（含一个名称不符）时应报错。
		"multiple pools returns error": {
			resources: &DriverResources{
				Pools: map[string]Pool{
					poolName:     {Slices: []Slice{{Devices: []resourceapi.Device{}}}},
					"other-pool": {Slices: []Slice{{Devices: []resourceapi.Device{}}}},
				},
			},
			expectedErrors: []string{"found 2 pools; expected exactly one pool with this name"},
		},

		// 用例：仅有一个名称不符的 pool 时应报错。
		"wrong pool only returns error": {
			resources: &DriverResources{
				Pools: map[string]Pool{
					"other-pool": {Slices: []Slice{{Devices: []resourceapi.Device{}}}},
				},
			},
			expectedErrors: []string{"found 1 pools; expected exactly one pool with this name"},
		},

		// 用例：空 pool 集合合法。
		"empty pools succeeds": {
			resources: &DriverResources{Pools: map[string]Pool{}},
		},

		// 用例：恰好一个同名 pool 合法。
		"single matching pool succeeds": {
			resources: &DriverResources{
				Pools: map[string]Pool{
					poolName: {Slices: []Slice{{Devices: []resourceapi.Device{}}}},
				},
			},
		},
	}

	// 手工构造只含校验所需字段的最小 Controller，直接调用 Update 并检查错误回调。
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			// errorHandler 同时充当断言：有预期错误时校验内容，否则要求无错误。
			ctrl := &Controller{
				reconcilePoolWithName: poolName,
				queue:                 ptr.To(workqueue.Mock[string]{}),
				errorHandler: func(_ context.Context, err error, _ string) {
					if len(tc.expectedErrors) > 0 {
						require.Error(t, err)
						for _, expectedError := range tc.expectedErrors {
							assert.Contains(t, err.Error(), expectedError)
						}
						return
					}

					require.NoError(t, err)
				},
			}
			ctrl.Update(tc.resources)
		})
	}
}

// TestControllerPoolNameFieldSelector 验证按 pool 名协调时，控制器优先在 list/watch 请求中
// 携带 pool name 字段选择器；当 apiserver 返回 BadRequest 时自动降级为本地过滤，
// 并保证 sliceStore 的 poolNameIndex 中只保留目标 pool 的切片。
func TestControllerPoolNameFieldSelector(t *testing.T) {
	// 驱动名与目标 pool 名。
	const (
		driverName = "test-driver"
		poolName   = "pool-a"
	)

	// 表驱动用例：分别在 list 与 watch 上模拟不支持字段选择器，并验证回退过滤。
	testCases := map[string]struct {
		// 表驱动用例字段说明：rejectList 与 rejectWatch 分别让 list 与 watch 请求返回 BadRequest；initialObjects 是初始切片；
		// expectedListSelectors 与 expectedWatchSelectors 是期望的请求选择器标记序列；
		// checkListFallbackFiltering 与 checkWatchFallbackFiltering 分别验证 list 与 watch 回退后的本地过滤结果。
		rejectList                  bool
		rejectWatch                 bool
		initialObjects              []runtime.Object
		expectedListSelectors       []bool
		expectedWatchSelectors      []bool
		checkListFallbackFiltering  bool
		checkWatchFallbackFiltering bool
	}{
		// 场景：list/watch 均支持字段选择器，所有请求都应携带选择器。
		"field-selector-supported": {
			expectedListSelectors:  []bool{true},
			expectedWatchSelectors: []bool{true},
		},
		// 场景：list 不支持选择器，先尝试带选择器再退回不带选择器的 list；watch 同理降级。
		"field-selector-unsupported-for-list": {
			rejectList: true,
			initialObjects: []runtime.Object{
				resourceSliceForPool(poolName, "matching"),
				resourceSliceForPool("other-pool", "other"),
			},
			expectedListSelectors:      []bool{true, false},
			expectedWatchSelectors:     []bool{false},
			checkListFallbackFiltering: true,
		},
		// 场景：watch 不支持选择器，需回退到无选择器的 watch 并在本地过滤事件。
		"field-selector-unsupported-for-watch": {
			rejectWatch:                 true,
			expectedListSelectors:       []bool{true},
			expectedWatchSelectors:      []bool{true, false},
			checkWatchFallbackFiltering: true,
		},
	}

	// 模拟 apiserver 不支持该字段选择器时返回的错误。
	badRequestError := apierrors.NewBadRequest(fmt.Sprintf("field label not supported for %s: %s", resourceapi.SchemeGroupVersion.WithKind("ResourceSlice"), resourceapi.ResourceSliceSelectorPoolName))

	// 逐用例运行：注入 reactor、启动控制器、检查请求序列与索引内容。
	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			// 构造带初始对象的 fake client，并前置拦截 list 与 watch。
			kubeClient := fake.NewSimpleClientset(testCase.initialObjects...)
			// list reactor：仅当用例要求且请求带目标 pool 选择器时返回 BadRequest。
			kubeClient.PrependReactor("list", "resourceslices", func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
				if testCase.rejectList && hasPoolNameFieldSelector(action, poolName) {
					return true, nil, badRequestError
				}
				return false, nil, nil
			})
			// watch reactor：对带选择器的 watch 请求返回 BadRequest，触发回退逻辑。
			kubeClient.PrependWatchReactor("resourceslices", func(action k8stesting.Action) (handled bool, ret watch.Interface, err error) {
				if testCase.rejectWatch && hasPoolNameFieldSelector(action, poolName) {
					return true, nil, badRequestError
				}
				return false, nil, nil
			})

			// SyncDelay 设为 1 小时，避免 informer 事件触发的延时同步在断言期间再次入队。
			ctrl, err := newController(ctx, Options{
				DriverName:            driverName,
				KubeClient:            kubeClient,
				ReconcilePoolWithName: poolName,
				SyncDelay:             new(time.Hour),
			})
			require.NoError(t, err)
			t.Cleanup(ctrl.Stop)

			// watch 请求由 informer 异步发出，需等待数量达到预期后再断言。
			require.Eventually(t, func() bool {
				return len(resourceSliceSelectorRequests(kubeClient.Actions(), "watch", poolName)) == len(testCase.expectedWatchSelectors)
			}, 5*time.Second, 10*time.Millisecond, "wait for ResourceSlice watch requests")
			// 校验 list/watch 请求中选择器标记的序列，以及控制器是否决定使用字段选择器。
			assert.Equal(t, testCase.expectedListSelectors, resourceSliceSelectorRequests(kubeClient.Actions(), "list", poolName))
			assert.Equal(t, testCase.expectedWatchSelectors, resourceSliceSelectorRequests(kubeClient.Actions(), "watch", poolName))
			assert.Equal(t, !testCase.rejectList && !testCase.rejectWatch, ctrl.usePoolNameFieldSelector.Load())

			// list 回退场景：确认本地索引里只有目标 pool 的切片。
			if testCase.checkListFallbackFiltering {
				matching, err := ctrl.sliceStore.ByIndex(poolNameIndex, poolName)
				require.NoError(t, err)
				assert.Len(t, matching, 1)
				other, err := ctrl.sliceStore.ByIndex(poolNameIndex, "other-pool")
				require.NoError(t, err)
				assert.Empty(t, other)
			}

			// watch 回退场景：新建匹配与不匹配的切片，确认 watcher 在本地完成过滤。
			if testCase.checkWatchFallbackFiltering {
				_, err := kubeClient.ResourceV1().ResourceSlices().Create(ctx, resourceSliceForPool(poolName, "matching"), metav1.CreateOptions{})
				require.NoError(t, err)
				_, err = kubeClient.ResourceV1().ResourceSlices().Create(ctx, resourceSliceForPool("other-pool", "other"), metav1.CreateOptions{})
				require.NoError(t, err)
				require.Eventually(t, func() bool {
					matching, err := ctrl.sliceStore.ByIndex(poolNameIndex, poolName)
					if err != nil || len(matching) != 1 {
						return false
					}
					other, err := ctrl.sliceStore.ByIndex(poolNameIndex, "other-pool")
					return err == nil && len(other) == 0
				}, 5*time.Second, 10*time.Millisecond, "wait for the fallback watcher to filter ResourceSlices")
			}
		})
	}
}

// hasPoolNameFieldSelector 判断某个 list/watch action 是否携带了精确的 pool name 字段选择器。
func hasPoolNameFieldSelector(action k8stesting.Action, poolName string) bool {
	// 从 ListAction 或 WatchAction 中取出字段选择器；其他 action 直接返回 false。
	var fieldSelector fields.Selector
	switch action := action.(type) {
	case k8stesting.ListAction:
		fieldSelector = action.GetListRestrictions().Fields
	case k8stesting.WatchAction:
		fieldSelector = action.GetWatchRestrictions().Fields
	default:
		return false
	}

	// 要求选择器精确匹配目标 pool 名。
	value, found := fieldSelector.RequiresExactMatch(resourceapi.ResourceSliceSelectorPoolName)
	return found && value == poolName
}

// resourceSliceSelectorRequests 按动作顺序返回指定 verb 的 ResourceSlice 请求是否携带目标 pool 选择器。
func resourceSliceSelectorRequests(actions []k8stesting.Action, verb, poolName string) []bool {
	// 收集每个匹配请求的选择器标记。
	var requests []bool
	// 只统计指定 verb 的 ResourceSlice 请求。
	for _, action := range actions {
		if action.Matches(verb, "resourceslices") {
			requests = append(requests, hasPoolNameFieldSelector(action, poolName))
		}
	}
	return requests
}

// joinErrors 将错误信息列表拼接成缩进的多行字符串，便于测试失败时阅读。
func joinErrors(errors []string) string {
	return strings.Join(errors, "\n  ")
}

// errsToStrings 把错误切片转换为字符串切片，用于按集合比较。
func errsToStrings(errs []error) []string {
	var strings []string
	// 逐条转换为错误文本。
	for _, err := range errs {
		strings = append(strings, err.Error())
	}
	return strings
}

// formatError 格式化单个错误：若是 DroppedFieldsError，额外附上期望与实际 Spec 的差异。
func formatError(err error) string {
	// 若错误携带被丢弃字段的信息，则附上期望与实际 Spec 的差异。
	var droppedFields *DroppedFieldsError
	if errors.As(err, &droppedFields) {
		return fmt.Sprintf("%v\n%s", err, cmp.Diff(droppedFields.DesiredSlice.Spec, droppedFields.ActualSlice.Spec))
	} else {
		return err.Error()
	}
}

// formatErrors 批量格式化错误列表。
func formatErrors(errs []error) []string {
	var errMsgs []string
	for _, err := range errs {
		errMsgs = append(errMsgs, formatError(err))
	}
	return errMsgs
}

// sortResourceSlices 按 Name 排序切片；Name 均为空时按 GenerateName 排序。
func sortResourceSlices(slices []resourceapi.ResourceSlice) {
	// 比较函数：优先按 Name，Name 均为空时退化为按 GenerateName 比较。
	sort.Slice(slices, func(i, j int) bool {
		if len(slices[i].Name) == 0 && len(slices[j].Name) == 0 {
			return slices[i].ObjectMeta.GenerateName < slices[j].ObjectMeta.GenerateName
		}
		return slices[i].Name < slices[j].Name
	})
}

// features 模拟 apiserver 关闭若干 DRA 特性门控的行为，用于验证字段被丢弃后的报错。
type features struct {
	// 各字段对应一个 DRA 特性门控：disableBindingConditions 对应 DRADeviceBindingConditions，disableDeviceTaints 对应 DRADeviceTaints，
	// disablePartitionableDevices 对应 DRAPartitionableDevices，disableConsumableCapacity 对应 DRAConsumableCapacity，
	// disableOptionalNodeOperations 对应 DRAOptionalNodeOperations，disableCompatibilityGroups 对应 DRADeviceCompatibilityGroups。
	disableBindingConditions      bool
	disableDeviceTaints           bool
	disablePartitionableDevices   bool
	disableConsumableCapacity     bool
	disableOptionalNodeOperations bool
	disableCompatibilityGroups    bool
}

// createTestClient 构造 fake clientset，并挂载 create/update reactor 以模拟真实 apiserver 的部分行为。
func createTestClient(features features, timeAdded metav1.Time, objects ...runtime.Object) *fake.Clientset {
	// 先用初始对象构造 fake client。
	fakeClient := fake.NewSimpleClientset(objects...)
	// 挂载 create 与 update reactor，模拟 apiserver 对切片对象的处理。
	fakeClient.PrependReactor("create", "resourceslices", createResourceSliceCreateReactor(features, timeAdded))
	fakeClient.PrependReactor("update", "resourceslices", createResourceSliceUpdateReactor(features, timeAdded))
	return fakeClient
}

// 返回给 fake client 的 create reactor：用自增计数器把 GenerateName 变成真实 Name，
// 统一设置 ResourceVersion，并模拟禁用字段丢弃与污点时间补全。
// 通过 client.PrependReactor 挂载即可生效。
func createResourceSliceCreateReactor(features features, timeAdded metav1.Time) func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
	// 名称序号：模拟 apiserver 为 GenerateName 分配实际名称。
	nameCounter := 0
	// reactor 可能被并发调用，序号分配需加锁保护。
	var mutex sync.Mutex
	return func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		mutex.Lock()
		defer mutex.Unlock()
		// 从 action 中取出待创建的切片对象。
		resourceslice := action.(k8stesting.CreateAction).GetObject().(*resourceapi.ResourceSlice)
		// ResourceVersion 从 1 开始，避免空字符串或 0 被 MutationCache 视为同一版本。
		resourceslice.ResourceVersion = "1"
		// Name 为空时按 GenerateName 加自增序号生成实际名称。
		if resourceslice.Name == "" && resourceslice.GenerateName != "" {
			resourceslice.Name = fmt.Sprintf("%s%d", resourceslice.GenerateName, nameCounter)
		}
		nameCounter++
		// 模拟 apiserver 因特性门控关闭而剔除字段。
		dropDisabledFields(features, resourceslice)
		// 模拟 apiserver 为缺失时间戳的污点补上当前时间。
		addTimeAdded(timeAdded, resourceslice)
		return false, nil, nil
	}
}

// createResourceSliceUpdateReactor 为 fake client 实现 update 时的 ResourceVersion 递增：
// 空版本号按 0 处理，其余按整数解析后加一；同时模拟字段丢弃与污点时间补全。
func createResourceSliceUpdateReactor(features features, timeAdded metav1.Time) func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
	return func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
		// 取出待更新的切片对象。
		resourceslice := action.(k8stesting.UpdateAction).GetObject().(*resourceapi.ResourceSlice)
		// 初始对象可能没有版本号，此时按 0 处理。
		rev := 0
		if resourceslice.ResourceVersion != "" {
			oldRev, err := strconv.Atoi(resourceslice.ResourceVersion)
			if err != nil {
				return false, nil, fmt.Errorf("ResourceVersion %q should have been an int: %w", resourceslice.ResourceVersion, err)
			}
			rev = oldRev
		}
		// 每次更新递增版本号，供控制器区分 Update 与 Create。
		rev++
		resourceslice.ResourceVersion = fmt.Sprintf("%d", rev)
		// 更新同样会经过 apiserver 的字段裁剪。
		dropDisabledFields(features, resourceslice)
		// 更新时同样补齐污点时间。
		addTimeAdded(timeAdded, resourceslice)
		return false, nil, nil
	}
}

// dropDisabledFields 模拟 apiserver 在特性门控关闭时从 ResourceSlice 中剔除对应字段。
func dropDisabledFields(features features, resourceslice *resourceapi.ResourceSlice) {
	// DRADeviceTaints：清空所有设备的污点。
	if features.disableDeviceTaints {
		for i := range resourceslice.Spec.Devices {
			resourceslice.Spec.Devices[i].Taints = nil
		}
	}
	// DRAPartitionableDevices：清空逐设备选择、共享计数器以及设备级节点选择与计数器消费。
	if features.disablePartitionableDevices {
		resourceslice.Spec.PerDeviceNodeSelection = nil
		resourceslice.Spec.SharedCounters = nil
		for i := range resourceslice.Spec.Devices {
			resourceslice.Spec.Devices[i].NodeName = nil
			resourceslice.Spec.Devices[i].NodeSelector = nil
			resourceslice.Spec.Devices[i].ConsumesCounters = nil
		}
	}
	// DRADeviceBindingConditions：清空绑定条件、失败条件与 BindsToNode。
	if features.disableBindingConditions {
		for i := range resourceslice.Spec.Devices {
			resourceslice.Spec.Devices[i].BindingConditions = nil
			resourceslice.Spec.Devices[i].BindingFailureConditions = nil
			resourceslice.Spec.Devices[i].BindsToNode = nil
		}
	}
	// DRAConsumableCapacity：清空 AllowMultipleAllocations。
	if features.disableConsumableCapacity {
		for i := range resourceslice.Spec.Devices {
			resourceslice.Spec.Devices[i].AllowMultipleAllocations = nil
		}
	}
	// DRAOptionalNodeOperations：清空 SkipNodeOperations。
	if features.disableOptionalNodeOperations {
		resourceslice.Spec.SkipNodeOperations = nil
	}
	// DRADeviceCompatibilityGroups：清空计数器消费中的兼容性组。
	if features.disableCompatibilityGroups {
		for i := range resourceslice.Spec.Devices {
			for j := range resourceslice.Spec.Devices[i].ConsumesCounters {
				resourceslice.Spec.Devices[i].ConsumesCounters[j].CompatibilityGroups = nil
			}
		}
	}
}

// addTimeAdded 为所有尚未设置 TimeAdded 的设备污点补上给定时间，模拟 apiserver 行为。
func addTimeAdded(timeAdded metav1.Time, resourceslice *resourceapi.ResourceSlice) {
	// 遍历所有设备及其污点。
	for i := range resourceslice.Spec.Devices {
		for e := range resourceslice.Spec.Devices[i].Taints {
			taint := &resourceslice.Spec.Devices[i].Taints[e]
			// 仅在缺失时补全，保留驱动或现有切片中已有的时间戳。
			if taint.TimeAdded == nil {
				taint.TimeAdded = &timeAdded
			}
		}
	}
}

// 包装 ResourceSlice，提供链式 setter，方便在测试中以声明式方式构造对象。
type ResourceSliceWrapper struct {
	// 内嵌被包装的 ResourceSlice。
	resourceapi.ResourceSlice
}

// 返回一个空的切片包装器：空的 ObjectMeta 与空的 Spec.Pool。
func MakeResourceSlice() *ResourceSliceWrapper {
	// 初始化空的元数据与 Spec.Pool。
	return &ResourceSliceWrapper{
		resourceapi.ResourceSlice{
			ObjectMeta: metav1.ObjectMeta{},
			Spec: resourceapi.ResourceSliceSpec{
				Pool: resourceapi.ResourcePool{},
			},
		},
	}
}

// 取出被包装的资源切片指针。
func (r *ResourceSliceWrapper) Obj() *resourceapi.ResourceSlice {
	return &r.ResourceSlice
}

// 设置 ObjectMeta.Name。
func (r *ResourceSliceWrapper) Name(name string) *ResourceSliceWrapper {
	r.ObjectMeta.Name = name
	return r
}

// 设置 ObjectMeta.GenerateName。
func (r *ResourceSliceWrapper) GenerateName(generateName string) *ResourceSliceWrapper {
	r.ObjectMeta.GenerateName = generateName
	return r
}

// 设置 ObjectMeta.UID。
func (r *ResourceSliceWrapper) UID(uid string) *ResourceSliceWrapper {
	r.ObjectMeta.UID = types.UID(uid)
	return r
}

// 设置 ObjectMeta.ResourceVersion。
func (r *ResourceSliceWrapper) ResourceVersion(rev string) *ResourceSliceWrapper {
	r.ObjectMeta.ResourceVersion = rev
	return r
}

// 设置指向 v1.Node 的 Controller 属主引用。
func (r *ResourceSliceWrapper) NodeOwnerReferences(nodeName, nodeUID string) *ResourceSliceWrapper {
	r.ObjectMeta.OwnerReferences = []metav1.OwnerReference{
		{
			APIVersion: "v1",
			Kind:       "Node",
			Name:       nodeName,
			UID:        types.UID(nodeUID),
			Controller: ptr.To(true),
		},
	}
	return r
}

// 设置指向虚构应用控制器（apps/v1）的属主引用，用于非节点本地资源。
func (r *ResourceSliceWrapper) AppOwnerReferences(appName string) *ResourceSliceWrapper {
	r.ObjectMeta.OwnerReferences = []metav1.OwnerReference{
		{
			APIVersion: "apps/v1",
			Kind:       "Something",
			Name:       appName,
			Controller: ptr.To(true),
		},
	}
	return r
}

// 设置 spec.driver。
func (r *ResourceSliceWrapper) Driver(driver string) *ResourceSliceWrapper {
	r.Spec.Driver = driver
	return r
}

// 设置 spec.pool（包含池名、Generation 与切片计数）。
func (r *ResourceSliceWrapper) Pool(pool resourceapi.ResourcePool) *ResourceSliceWrapper {
	r.Spec.Pool = pool
	return r
}

// 设置 spec.nodeName；空字符串会被 refIfNotZero 转成 nil。
func (r *ResourceSliceWrapper) NodeName(nodeName string) *ResourceSliceWrapper {
	r.Spec.NodeName = refIfNotZero(nodeName)
	return r
}

// 设置 spec.nodeSelector。
func (r *ResourceSliceWrapper) NodeSelector(nodeSelector *v1.NodeSelector) *ResourceSliceWrapper {
	r.Spec.NodeSelector = nodeSelector
	return r
}

// 设置 spec.allNodes；false 会被 refIfNotZero 转成 nil。
func (r *ResourceSliceWrapper) AllNodes(allNodes bool) *ResourceSliceWrapper {
	r.Spec.AllNodes = refIfNotZero(allNodes)
	return r
}

// 设置 spec.devices。
func (r *ResourceSliceWrapper) Devices(devices []resourceapi.Device) *ResourceSliceWrapper {
	r.Spec.Devices = devices
	return r
}

// 设置 spec.perDeviceNodeSelection；true 写入指针，false 置为 nil。
func (r *ResourceSliceWrapper) PerDeviceNodeSelection(perDeviceNodeSelection bool) *ResourceSliceWrapper {
	if perDeviceNodeSelection {
		r.Spec.PerDeviceNodeSelection = ptr.To(true)
	} else {
		r.Spec.PerDeviceNodeSelection = nil
	}
	return r
}

// 设置 spec.sharedCounters。
func (r *ResourceSliceWrapper) SharedCounters(counters []resourceapi.CounterSet) *ResourceSliceWrapper {
	r.Spec.SharedCounters = counters
	return r
}

// 设置 spec.skipNodeOperations。
func (r *ResourceSliceWrapper) SkipNodeOperations(skip ...resourceapi.SkipNodeOperation) *ResourceSliceWrapper {
	r.Spec.SkipNodeOperations = skip
	return r
}

// 以下类型用于在 newDevice 的可变参数中区分要设置的设备字段。
type nodeNameField string
type allowMultipleAllocationsField bool

// newDevice 构造一个设备，并按可变参数的类型设置可选字段（属性、污点、计数器消费、节点选择等）。
func newDevice(name string, fields ...any) resourceapi.Device {
	// 至少设置设备名。
	device := resourceapi.Device{
		Name: name,
	}
	for _, field := range fields {
		// 按参数的动态类型分派到对应字段；不支持的类型直接 panic。
		switch f := field.(type) {
		case map[resourceapi.QualifiedName]resourceapi.DeviceAttribute:
			device.Attributes = f
		case resourceapi.DeviceTaint:
			device.Taints = append(device.Taints, f)
		case []resourceapi.DeviceTaint:
			device.Taints = append(device.Taints, f...)
		case resourceapi.DeviceCounterConsumption:
			device.ConsumesCounters = append(device.ConsumesCounters, f)
		case []resourceapi.DeviceCounterConsumption:
			device.ConsumesCounters = append(device.ConsumesCounters, f...)
		case nodeNameField:
			device.NodeName = ptr.To(string(f))
		case allowMultipleAllocationsField:
			device.AllowMultipleAllocations = ptr.To(bool(f))
		case *v1.NodeSelector:
			device.NodeSelector = f
		// 未识别的参数类型属于测试代码错误，直接 panic。
		default:
			panic(fmt.Sprintf("unsupported resourceapi.Device field type %T", field))
		}
	}
	return device
}

// TestGetIndexLength 验证 getIndexLength 在不同切片数量下的索引编码宽度：
// 数量超过 0xFFFFF 时编码位数从 5 增至 6。
func TestGetIndexLength(t *testing.T) {
	// 表驱动用例：切片数量与期望的编码长度。
	testCases := []struct {
		// 表驱动用例字段：numSlices 是切片数量（可为负数以覆盖边界），expected 是期望的编码位数。
		numSlices int
		expected  int
	}{
		// 边界值：负数、0 与十六进制上限附近；超过 0xFFFFF 后位数加一。
		{-1, 5},
		{0, 5},
		{1, 5},
		{16, 5},
		{0x10000, 5},
		{0x100000, 5},
		{0x100001, 6},
		{0x1000000, 6},
	}

	// 以切片数量作为子测试名逐项断言。
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%d", tc.numSlices), func(t *testing.T) {
			assert.Equal(t, tc.expected, getIndexLength(tc.numSlices))
		})
	}
}

// TestEncodeIndex 验证 encodeIndex 将整数编码为定长小写 base16 字符串，长度由调用方指定。
func TestEncodeIndex(t *testing.T) {
	// 表驱动用例：索引、目标长度与期望字符串。
	testCases := []struct {
		// 表驱动用例字段：index 是待编码的索引，expectedLength 是目标编码长度，expected 是期望的 base16 字符串。
		index          int
		expectedLength int
		expected       string
	}{
		// 覆盖 0 到 36 的进位，以及目标长度不同时的补零行为。
		{0, 5, "00000"},
		{1, 5, "00001"},
		{9, 5, "00009"},
		{10, 5, "0000a"},
		{26, 5, "0001a"},
		{27, 5, "0001b"},
		{35, 5, "00023"},
		{36, 5, "00024"},
		{100000, 5, "186a0"},
		{0, 6, "000000"},
		{1048576, 6, "100000"},
	}

	// 以索引作为子测试名逐项断言。
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%d", tc.index), func(t *testing.T) {
			assert.Equal(t, tc.expected, encodeIndex(tc.index, tc.expectedLength))
		})
	}
}

// TestDecodeIndex 验证 decodeIndex 能从“索引-后缀”形式的名称中解析出整数索引，
// 并覆盖带后缀、非法输入与溢出等场景。
func TestDecodeIndex(t *testing.T) {
	// 表驱动用例：名称、期望索引长度、期望结果或错误。
	testCases := []struct {
		// 表驱动用例字段：name 是待解析的名称，expectedLength 是索引部分的期望长度，expected 是期望解析出的整数，
		// expectError 表示是否期望失败，expectedError 是错误信息中应包含的子串。
		name           string
		expectedLength int
		expected       int
		expectError    bool
		expectedError  string
	}{
		// 正常场景：索引后跟连字符，且可带 GenerateName 生成的后缀。
		{name: "00000-", expectedLength: 5, expected: 0},
		{name: "00001-", expectedLength: 5, expected: 1},
		{name: "00009-", expectedLength: 5, expected: 9},
		{name: "0000a-", expectedLength: 5, expected: 10},
		{name: "0001a-", expectedLength: 5, expected: 26},
		{name: "0001b-", expectedLength: 5, expected: 27},
		{name: "00023-", expectedLength: 5, expected: 35},
		{name: "00024-", expectedLength: 5, expected: 36},
		{name: "186a0-", expectedLength: 5, expected: 100000},
		{name: "100000-", expectedLength: 6, expected: 1048576},
		// 平台相关：超过 MaxInt32 但仍在 int64 范围内的值，在 32 位平台上可能无法转为 int。
		{
			name:           "80000000-",
			expectedLength: 8,
			expected:       getExpectedForLargeInt(int64(math.MaxInt32) + 1),
			expectError:    getExpectedForLargeInt(int64(math.MaxInt32)+1) == -1,
			expectedError:  "failed to parse index",
		},
		// 溢出场景：超出 int64 的十六进制字符串应解析失败。
		{name: "8000000000000000-", expectedLength: 16, expectError: true, expectedError: "failed to parse index"}, // 可能的最大字符串（超出 int64）
		// 带后缀：名称中的 driver/owner/suffix 部分不应影响索引解析。
		{name: "00000-driver-owner-suffix", expectedLength: 5, expected: 0},
		{name: "00001-driver-owner-suffix", expectedLength: 5, expected: 1},
		// 非法输入：缺少分隔符、非 base16 字符、负数形式以及长度不符。
		{name: "00000", expectedLength: 5, expectError: true, expectedError: "invalid index length or missing separator"},  // 缺少分隔符
		{name: "0000g-", expectedLength: 5, expectError: true, expectedError: "failed to parse index"},                     // 'g' is not base16
		{name: "-0001-", expectedLength: 5, expectError: true, expectedError: "invalid index length or missing separator"}, // 负十六进制（破折号是分隔符，故 strings.Index 为 0）
		// 长度不符：索引部分长度与期望不一致时报错。
		{name: "00000-", expectedLength: 6, expectError: true, expectedError: "invalid index length or missing separator"},
		{name: "100000-", expectedLength: 5, expectError: true, expectedError: "invalid index length or missing separator"},
	}

	// 以名称作为子测试名，分别验证成功结果与错误内容。
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := decodeIndex(tc.name, tc.expectedLength)
			if tc.expectError {
				require.Error(t, err)
				if tc.expectedError != "" {
					assert.Contains(t, err.Error(), tc.expectedError)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, actual)
			}
		})
	}
}

// 平台相关的期望值计算：超过当前平台 int 上限时返回 -1，用于构造“应报错”的预期。
func getExpectedForLargeInt(val int64) int {
	if val > int64(math.MaxInt) {
		return -1
	}
	return int(val)
}
