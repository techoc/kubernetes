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

package allocatortesting

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/onsi/gomega"
	"github.com/onsi/gomega/gstruct"
	"github.com/onsi/gomega/types"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/component-helpers/nodedeclaredfeatures/features/draoptionalnodeoperations"
	"k8s.io/dynamic-resource-allocation/cel"
	"k8s.io/dynamic-resource-allocation/structured/internal"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/utils/ptr"
)

// 以下类型别名把 structured/internal 包中的类型重新导出，供测试调用方使用：
//   - Allocator 是 internal.Allocator 的类型别名，向测试调用方暴露分配器接口。
//   - DeviceClassLister 是 internal.DeviceClassLister 的类型别名，用于向分配器提供设备类查询能力。
//   - Features 是 internal.Features 的类型别名，描述分配器支持的功能特性集合。
//   - DeviceID 是 internal.DeviceID 的类型别名，表示已分配设备的唯一标识（driver/pool/device）。
//   - SharedDeviceID 是 internal.SharedDeviceID 的类型别名，表示允许被多次分配的共享设备标识。
//   - ConsumedCapacityCollection 是 internal.ConsumedCapacityCollection 的类型别名，表示已消耗容量的集合。
//   - ConsumedCapacity 是 internal.ConsumedCapacity 的类型别名，表示单个份额消耗的容量明细。
//   - AllocatedState 是 internal.AllocatedState 的类型别名，表示传入分配器的已分配状态。
type Allocator = internal.Allocator
type DeviceClassLister = internal.DeviceClassLister
type Features = internal.Features
type DeviceID = internal.DeviceID

type SharedDeviceID = internal.SharedDeviceID
type ConsumedCapacityCollection = internal.ConsumedCapacityCollection
type ConsumedCapacity = internal.ConsumedCapacity
type AllocatedState = internal.AllocatedState

// MakeDeviceID 转发 internal.MakeDeviceID，由 driver、pool、device 三段组成一个 DeviceID。
func MakeDeviceID(driver, pool, device string) DeviceID {
	return internal.MakeDeviceID(driver, pool, device)
}

// 本常量块汇总各测试用例共用的固定标识符：地域、节点、设备类、驱动、资源池、请求、声明、切片与设备名等。
const (
	region1     = "region-1"
	region2     = "region-2"
	node1       = "node-1"
	node2       = "node-2"
	classA      = "class-a"
	classB      = "class-b"
	driverA     = "driver-a"
	driverB     = "driver-b"
	pool1       = "pool-1"
	pool2       = "pool-2"
	pool3       = "pool-3"
	pool4       = "pool-4"
	req0        = "req-0"
	req1        = "req-1"
	req2        = "req-2"
	req3        = "req-3"
	subReq0     = "subReq-0"
	subReq1     = "subReq-1"
	req0SubReq0 = "req-0/subReq-0"
	req0SubReq1 = "req-0/subReq-1"
	req1SubReq0 = "req-1/subReq-0"
	req1SubReq1 = "req-1/subReq-1"
	req2SubReq0 = "req-2/subReq-0"
	req2SubReq1 = "req-2/subReq-1"
	claim0      = "claim-0"
	claim1      = "claim-1"
	slice1      = "slice-1"
	slice2      = "slice-2"
	slice3      = "slice-3"
	slice4      = "slice-4"
	device0     = "device-0"
	device1     = "device-1"
	device2     = "device-2"
	device3     = "device-3"
	device4     = "device-4"
	counterSet1 = "counter-set-1"
	counterSet2 = "counter-set-2"
	capacity0   = "capacity-0"
	capacity1   = "capacity-1"
)

// 本变量块定义测试用的通用数量值与一个固定份额 ID；数量值涵盖整数与小数（milli）两种形态。
var (
	fixedShareID = apitypes.UID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")

	zero  = *resource.NewQuantity(0, resource.BinarySI)
	one   = *resource.NewQuantity(1, resource.BinarySI)
	two   = *resource.NewQuantity(2, resource.BinarySI)
	three = *resource.NewQuantity(3, resource.BinarySI)
	four  = *resource.NewQuantity(4, resource.BinarySI)

	pointOne     = resource.MustParse("100m")
	pointTwo     = resource.MustParse("200m")
	pointTwoFive = resource.MustParse("250m")
	// pointThree uses NewMilliQuantity so its s field matches allocator output (s:"").
	// pointThree 使用 NewMilliQuantity 构造，保证其内部 s 字段为空，与分配器输出的表示一致。
	pointThree = *resource.NewMilliQuantity(300, resource.DecimalSI)
)

// init 提高 klog 的默认日志详细度（调度器逐节点过滤时 Allocate 使用极高阈值），并把配置注册到命令行 flag。
func init() {
	// Bump up the default verbosity for testing. Allocate uses very
	// high thresholds because it is used in the scheduler's per-node
	// filter operation.
	ktesting.DefaultConfig = ktesting.NewConfig(ktesting.Verbosity(7))
	ktesting.DefaultConfig.AddFlags(flag.CommandLine)
}

// Test objects generators

// 本常量块定义节点标签键与地域相关的固定取值，供节点选择器和切片选择器复用。
const (
	fieldNameKey     = "metadata.name"
	regionKey        = "region"
	planetKey        = "planet"
	planetValueEarth = "earth"
)

// generate a node object given a name and a region
// node 根据名称与地域构造一个测试用 Node 对象，并附带固定的 planet=earth 标签。参数 name 为节点名，region 为地域标签值。
func node(name, region string) *v1.Node {
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				regionKey: region,
				planetKey: planetValueEarth,
			},
		},
	}
}

// nodeWithDRAOptionalNodeOperations returns a node declaring DRAOptionalNodeOperations feature support
// required by devices that define SkipNodeOperations.
// nodeWithDRAOptionalNodeOperations 在 node 基础上声明 DRAOptionalNodeOperations 特性，用于测试定义了 SkipNodeOperations 的设备所依赖的节点能力。参数 name 为节点名，region 为地域标签值。
func nodeWithDRAOptionalNodeOperations(name, region string) *v1.Node {
	n := node(name, region)
	n.Status.DeclaredFeatures = append(n.Status.DeclaredFeatures, draoptionalnodeoperations.DRAOptionalNodeOperationsFeatureGate)
	return n
}

// generate a DeviceClass object with the given name and a driver CEL selector.
// driver name is assumed to be the same as the class name.
// class 构造一个测试用 DeviceClass，其 CEL 选择器按 driver 名称匹配设备；按约定 driver 名与类名相同。参数 name 为类名，driver 为驱动名。
func class(name, driver string) *resourceapi.DeviceClass {
	return &resourceapi.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: resourceapi.DeviceClassSpec{
			Selectors: []resourceapi.DeviceSelector{
				{
					CEL: &resourceapi.CELDeviceSelector{
						Expression: fmt.Sprintf(`device.driver == "%s"`, driver),
					},
				},
			},
		},
	}
}

// generate a DeviceClass object with the given name and a driver CEL selector.
// driver name is assumed to be the same as the class name.
// shared condition is explicitly set.
// classWithAllowMultipleAllocations 构造一个 DeviceClass，其 CEL 选择器同时匹配 driver 名和 device.allowMultipleAllocations 的取值。参数 multialloc 指定期望的 allowMultipleAllocations 布尔值。
func classWithAllowMultipleAllocations(name, driver string, multialloc bool) *resourceapi.DeviceClass {
	return &resourceapi.DeviceClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: resourceapi.DeviceClassSpec{
			Selectors: []resourceapi.DeviceSelector{
				{
					CEL: &resourceapi.CELDeviceSelector{
						Expression: fmt.Sprintf(`device.driver == "%s" && device.allowMultipleAllocations == %v`, driver, multialloc),
					},
				},
			},
		},
	}
}

// generate a DeviceConfiguration object with the given driver and attribute.
// deviceConfiguration 构造一个 Opaque 类型的设备配置：driver 指定目标驱动，attribute 同时作为 JSON 参数键和值前缀（值为 attribute+"Value"）。
func deviceConfiguration(driver, attribute string) resourceapi.DeviceConfiguration {
	return resourceapi.DeviceConfiguration{
		Opaque: &resourceapi.OpaqueDeviceConfiguration{
			Driver: driver,
			Parameters: runtime.RawExtension{
				Raw: []byte(fmt.Sprintf("{\"%s\":\"%s\"}", attribute, attribute+"Value")),
			},
		},
	}
}

// generate a DeviceClass object with the given name and attribute.
// attribute is used to generate device configuration parameters in a form of JSON {attribute: attributeValue}.
// classWithConfig 在 class 基础上为设备类添加一条设备配置（Spec.Config），配置参数由 attribute 生成，形如 {"attribute": "attributeValue"}。
func classWithConfig(name, driver, attribute string) *resourceapi.DeviceClass {
	class := class(name, driver)
	class.Spec.Config = []resourceapi.DeviceClassConfiguration{
		{
			DeviceConfiguration: deviceConfiguration(driver, attribute),
		},
	}
	return class
}

// generate a ResourceClaim object with the given name and device requests.
// claimWithRequests 构造一个 ResourceClaim：包含给定的设备约束列表与若干设备请求。参数 name 为声明名，constraints 为约束列表，requests 为可变数量的请求；返回包装对象以便链式调用。
func claimWithRequests(name string, constraints []resourceapi.DeviceConstraint, requests ...resourceapi.DeviceRequest) wrapResourceClaim {
	return wrapResourceClaim{&resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: resourceapi.ResourceClaimSpec{
			Devices: resourceapi.DeviceClaim{
				Requests:    requests,
				Constraints: constraints,
			},
		},
	}}
}

// generate a DeviceRequest object with the given name, class and selectors.
// request 构造一个精确计数（ExactCount）模式的 DeviceRequest。参数 name 为请求名，class 为设备类名，count 为请求数量，selectors 为可选的 CEL 设备选择器。
func request(name, class string, count int64, selectors ...resourceapi.DeviceSelector) resourceapi.DeviceRequest {
	return resourceapi.DeviceRequest{
		Name: name,
		Exactly: &resourceapi.ExactDeviceRequest{
			Count:           count,
			AllocationMode:  resourceapi.DeviceAllocationModeExactCount,
			DeviceClassName: class,
			Selectors:       selectors,
		},
	}
}

// deviceRequest 在 request 基础上包装为 wrapDeviceRequest，便于通过 withCapacityRequest、withSelectors 等方法链式追加配置。
func deviceRequest(name, class string, count int64) wrapDeviceRequest {
	return wrapDeviceRequest{
		request(name, class, count),
	}
}

// allDeviceRequest 构造一个 All 分配模式（请求某设备类中的全部设备）的 DeviceRequest 并包装为 wrapDeviceRequest。参数 name 为请求名，class 为设备类名。
func allDeviceRequest(name, class string) wrapDeviceRequest {
	return wrapDeviceRequest{
		resourceapi.DeviceRequest{
			Name: name,
			Exactly: &resourceapi.ExactDeviceRequest{
				AllocationMode:  resourceapi.DeviceAllocationModeAll,
				DeviceClassName: class,
			},
		},
	}
}

// subRequest 构造一个 DeviceSubRequest 并包装为 wrapDeviceSubRequest。参数 name 为子请求名，class 为设备类名，count 为数量；fields 可为 DeviceSelector、DeviceToleration 或 []DeviceDerivedAttribute，其他类型会 panic。
func subRequest(name, class string, count int64, fields ...any) wrapDeviceSubRequest {
	subRequest := resourceapi.DeviceSubRequest{
		Name:            name,
		Count:           count,
		AllocationMode:  resourceapi.DeviceAllocationModeExactCount,
		DeviceClassName: class,
	}

	// 按可选字段的动态类型分别追加选择器、容忍或派生属性。
	for _, field := range fields {
		switch typedField := field.(type) {
		case resourceapi.DeviceSelector:
			subRequest.Selectors = append(subRequest.Selectors, typedField)
		case resourceapi.DeviceToleration:
			subRequest.Tolerations = append(subRequest.Tolerations, typedField)
		case []resourceapi.DeviceDerivedAttribute:
			subRequest.DerivedAttributes = append(subRequest.DerivedAttributes, typedField...)
		default:
			panic(fmt.Sprintf("unsupported field for DeviceSubRequest: %T", field))
		}
	}

	return wrapDeviceSubRequest{subRequest}
}

// wrapDeviceRequest 包装 resourceapi.DeviceRequest，提供基于深拷贝的不可变化式构造方法 obj/withCapacityRequest/withSelectors。
type wrapDeviceRequest struct{ resourceapi.DeviceRequest }

// obj 返回被包装的原始 DeviceRequest。
func (in wrapDeviceRequest) obj() resourceapi.DeviceRequest {
	return in.DeviceRequest
}

// withCapacityRequest 返回一份深拷贝，并为精确请求设置容量需求；参数 quantity 为容量值。
func (in wrapDeviceRequest) withCapacityRequest(quantity *resource.Quantity) wrapDeviceRequest {
	out := in.DeepCopy()
	out.Exactly.Capacity = capacityRequests(quantity)
	return wrapDeviceRequest{*out}
}

// withSelectors 返回一份深拷贝，并将精确请求的选择器替换为给定的 selectors。
func (in wrapDeviceRequest) withSelectors(selectors ...resourceapi.DeviceSelector) wrapDeviceRequest {
	out := in.DeepCopy()
	out.Exactly.Selectors = selectors
	return wrapDeviceRequest{*out}
}

// wrapDeviceSubRequest 包装 resourceapi.DeviceSubRequest，提供链式修改分配模式与容量需求的方法。
type wrapDeviceSubRequest struct{ resourceapi.DeviceSubRequest }

// obj 返回被包装的原始 DeviceSubRequest。
func (in wrapDeviceSubRequest) obj() resourceapi.DeviceSubRequest {
	return in.DeviceSubRequest
}

// withAllocationMode 返回一份深拷贝，并将子请求的分配模式改为 mode。
func (in wrapDeviceSubRequest) withAllocationMode(mode resourceapi.DeviceAllocationMode) wrapDeviceSubRequest {
	out := in.DeepCopy()
	out.AllocationMode = mode
	return wrapDeviceSubRequest{*out}
}

// withCapacityRequest 返回一份深拷贝，并为子请求设置容量需求。
func (in wrapDeviceSubRequest) withCapacityRequest(quantity *resource.Quantity) wrapDeviceSubRequest {
	out := in.DeepCopy()
	out.Capacity = capacityRequests(quantity)
	return wrapDeviceSubRequest{*out}
}

// genereate a DeviceRequest with the given name and list of prioritized requests.
// requestWithPrioritizedList 构造一个 FirstAvailable（优先级列表）模式的 DeviceRequest。参数 name 为请求名，prioritizedRequests 为按优先级排列的子请求，会解包为 DeviceSubRequest 列表。
func requestWithPrioritizedList(name string, prioritizedRequests ...wrapDeviceSubRequest) wrapDeviceRequest {
	var subreqs []resourceapi.DeviceSubRequest
	for _, subreq := range prioritizedRequests {
		subreqs = append(subreqs, subreq.obj())
	}
	return wrapDeviceRequest{resourceapi.DeviceRequest{
		Name:           name,
		FirstAvailable: subreqs,
	}}
}

// generate a ResourceClaim object with the given name and with a single request with given class and constraints.
// claimWithRequest 构造一个只含单个请求的 ResourceClaim：请求名为 req、数量固定为 1、设备类为 class，并可附带设备约束。
func claimWithRequest(name, req, class string, constraints ...resourceapi.DeviceConstraint) wrapResourceClaim {
	claim := claimWithRequests(name, constraints, request(req, class, 1))
	return claim
}

// generate a ResourceClaim object with the given name.
// claim 构造一个仅含名称元数据的空 ResourceClaim，便于随后用 withRequests/withConstraints 等方法填充。
func claim(name string) wrapResourceClaim {
	return wrapResourceClaim{&resourceapi.ResourceClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
	}}
}

// wrapResourceClaim 包装 *resourceapi.ResourceClaim，提供基于深拷贝的链式构造方法 obj/withTolerations/withRequests/withConstraints/withConfigs。
type wrapResourceClaim struct{ *resourceapi.ResourceClaim }

// obj 返回被包装的原始 ResourceClaim 指针。
func (in wrapResourceClaim) obj() *resourceapi.ResourceClaim {
	return in.ResourceClaim
}

// withTolerations 返回一份深拷贝，并在声明所有精确请求的容忍列表末尾追加给定的 tolerations。
func (in wrapResourceClaim) withTolerations(tolerations ...resourceapi.DeviceToleration) wrapResourceClaim {
	out := in.DeepCopy()
	for i := range out.Spec.Devices.Requests {
		out.Spec.Devices.Requests[i].Exactly.Tolerations = append(out.Spec.Devices.Requests[i].Exactly.Tolerations, tolerations...)
	}
	return wrapResourceClaim{out}
}

// withRequests 返回一份深拷贝，并将声明的请求列表整体替换为给定的 requests（会解包 wrapDeviceRequest）。
func (in wrapResourceClaim) withRequests(requests ...wrapDeviceRequest) wrapResourceClaim {
	out := in.DeepCopy()
	out.Spec.Devices.Requests = make([]resourceapi.DeviceRequest, len(requests))
	for i, request := range requests {
		out.Spec.Devices.Requests[i] = resourceapi.DeviceRequest(request.DeviceRequest)
	}
	return wrapResourceClaim{out}
}

// withConstraints 返回一份深拷贝，并将声明的设备约束列表替换为给定的 constraints。
func (in wrapResourceClaim) withConstraints(constraints ...resourceapi.DeviceConstraint) wrapResourceClaim {
	out := in.DeepCopy()
	out.Spec.Devices.Constraints = constraints
	return wrapResourceClaim{out}
}

// withConfigs 返回一份深拷贝，并将声明的设备配置列表替换为给定的 configs。
func (in wrapResourceClaim) withConfigs(configs []resourceapi.DeviceClaimConfiguration) wrapResourceClaim {
	out := in.DeepCopy()
	out.Spec.Devices.Config = configs
	return wrapResourceClaim{out}
}

// generate a ResourceClaim object with the given name, request, class, and attribute.
// attribute is used to generate parameters in a form of JSON {attribute: attributeValue}.
// claimWithDeviceConfig 构造一个单请求 ResourceClaim，并为其添加一条设备配置，配置参数由 attribute 生成，形如 {"attribute": "attributeValue"}。
func claimWithDeviceConfig(name, request, class, driver, attribute string) wrapResourceClaim {
	claim := claimWithRequest(name, request, class)
	claim.Spec.Devices.Config = []resourceapi.DeviceClaimConfiguration{
		{
			DeviceConfiguration: deviceConfiguration(driver, attribute),
		},
	}
	return claim
}

// deviceClaimConfig 构造一条 DeviceClaimConfiguration：把设备配置 deviceConfig 关联到给定的请求名列表 requests。
func deviceClaimConfig(requests []string, deviceConfig resourceapi.DeviceConfiguration) resourceapi.DeviceClaimConfiguration {
	return resourceapi.DeviceClaimConfiguration{
		Requests:            requests,
		DeviceConfiguration: deviceConfig,
	}
}

// fromCounters 是 device() 的特殊容量标记：表示设备容量应从其 ConsumesCounters 引用的共享计数器推导，而非显式给出。
const (
	fromCounters = "fromCounters"
)

// generate a Device object with the given name, capacity and attributes.
// device 构造一个测试用 Device 并包装为 wrapDevice。参数 name 为设备名，attributes 为设备属性；capacity 可为容量映射、字符串标记 fromCounters 或 nil：容量映射会转为 DeviceCapacity，fromCounters 则在追加计数器消耗时从共享计数器推导容量。
func device(name string, capacity any, attributes map[resourceapi.QualifiedName]resourceapi.DeviceAttribute) wrapDevice {
	device := resourceapi.Device{
		Name:       name,
		Attributes: attributes,
	}

	// 标记该设备的容量是否需要从共享计数器推导。
	var capacityFromCounters bool
	switch capacity := capacity.(type) {
	// 情形一：容量为 Quantity 映射，转换为 DeviceCapacity。
	case map[resourceapi.QualifiedName]resource.Quantity:
		device.Capacity = toDeviceCapacity(capacity)
	// 情形二：容量已是 DeviceCapacity 映射，直接使用。
	case map[resourceapi.QualifiedName]resourceapi.DeviceCapacity:
		device.Capacity = capacity
	// 情形三：字符串标记，仅接受 fromCounters。
	case string:
		if capacity == fromCounters {
			capacityFromCounters = true
		} else {
			panic(fmt.Sprintf("unexpected capacity value %q", capacity))
		}
	// 情形四：无容量。
	case nil:
		// nothing to do
	default:
		panic(fmt.Sprintf("unexpected capacity type %T: %+v", capacity, capacity))
	}

	return wrapDevice{Device: device, capacityFromCounters: capacityFromCounters}
}

// wrapDevice 包装 resourceapi.Device，提供链式设置污点、计数器消耗、节点选择、绑定条件等字段的方法；capacityFromCounters 记录容量是否需从计数器推导。
type wrapDevice struct {
	resourceapi.Device
	capacityFromCounters bool
}

// obj 返回被包装的原始 Device。
func (in wrapDevice) obj() resourceapi.Device {
	return in.Device
}

// withTaints 返回一份深拷贝，并在设备污点列表末尾追加给定的 taints。
func (in wrapDevice) withTaints(taints ...resourceapi.DeviceTaint) wrapDevice {
	inDevice := resourceapi.Device(in.Device)
	device := inDevice.DeepCopy()
	device.Taints = append(device.Taints, taints...)
	return wrapDevice{Device: *device}
}

// withDeviceCounterConsumption 返回一份深拷贝，并在 ConsumesCounters 列表末尾追加计数器消耗声明；若设备标记为 fromCounters，则同时把这些计数器值推导为设备容量。
func (in wrapDevice) withDeviceCounterConsumption(deviceCounterConsumption ...resourceapi.DeviceCounterConsumption) wrapDevice {
	inDevice := in.Device
	device := inDevice.DeepCopy()
	device.ConsumesCounters = append(device.ConsumesCounters, deviceCounterConsumption...)
	if in.capacityFromCounters {
		c := make(map[resourceapi.QualifiedName]resourceapi.DeviceCapacity)
		for _, dcc := range device.ConsumesCounters {
			for name, cap := range dcc.Counters {
				ccap := resourceapi.DeviceCapacity{
					Value: cap.Value,
				}
				c[resourceapi.QualifiedName(name)] = ccap
			}
		}
		device.Capacity = c
	}
	return wrapDevice{Device: *device}
}

// withNodeSelection 返回一份深拷贝，并按 nodeSelection 类型设置设备节点约束：*v1.NodeSelector 赋给 NodeSelector，nodeSelectionAll 表示所有节点，普通字符串表示具体节点名，nodeSelectionPerDevice 在此处不支持。
func (in wrapDevice) withNodeSelection(nodeSelection any) wrapDevice {
	inDevice := in.Device
	device := inDevice.DeepCopy()
	// 设备层面的节点选择：切片级选择器、全部节点或具体节点名。
	switch nodeSelection := nodeSelection.(type) {
	case *v1.NodeSelector:
		device.NodeSelector = nodeSelection
	case string:
		if nodeSelection == nodeSelectionAll {
			device.AllNodes = func() *bool {
				r := true
				return &r
			}()
		} else if nodeSelection == nodeSelectionPerDevice {
			panic("nodeSelectionPerDevice is not supported for devices")
		} else {
			device.NodeName = &nodeSelection
		}
	default:
		panic(fmt.Sprintf("unexpected nodeSelection type %T: %+v", nodeSelection, nodeSelection))
	}
	return wrapDevice{Device: *device}
}

// withBindingConditions 返回一份深拷贝，并设置设备的绑定就绪条件与绑定失败条件。
func (in wrapDevice) withBindingConditions(bindingConditions, bindingFailureConditions []string) wrapDevice {
	inDevice := in.Device
	device := inDevice.DeepCopy()
	device.BindingConditions = bindingConditions
	device.BindingFailureConditions = bindingFailureConditions
	return wrapDevice{Device: *device}
}

// withBindsToNode 返回一份深拷贝，并设置设备是否绑定到节点（BindsToNode）。
func (in wrapDevice) withBindsToNode(bindsToNode bool) wrapDevice {
	inDevice := in.Device
	device := inDevice.DeepCopy()
	device.BindsToNode = ptr.To(bindsToNode)
	return wrapDevice{Device: *device}
}

// withAllowMultipleAllocations 返回一份深拷贝，并将设备的 AllowMultipleAllocations 设为 true（允许被多个声明共享分配）。
func (in wrapDevice) withAllowMultipleAllocations() wrapDevice {
	inDevice := in.Device
	device := inDevice.DeepCopy()
	device.AllowMultipleAllocations = ptr.To(true)
	return wrapDevice{Device: *device}
}

// withCapacityRequestPolicyRange adds capacity with default requestPolicy (2,2,4)
// withCapacityRequestPolicyRange 为给定容量添加默认请求策略范围（default=2，min=2，step=2，max=4）。参数 capacity 为容量名到容量值的映射。
func (in wrapDevice) withCapacityRequestPolicyRange(capacity map[resourceapi.QualifiedName]resource.Quantity) wrapDevice {
	inDevice := in.Device
	device := inDevice.DeepCopy()
	if device.Capacity == nil {
		device.Capacity = make(map[resourceapi.QualifiedName]resourceapi.DeviceCapacity, len(capacity))
	}
	for name, quantity := range capacity {
		device.Capacity[name] = resourceapi.DeviceCapacity{
			Value: quantity,
			RequestPolicy: &resourceapi.CapacityRequestPolicy{
				Default: ptr.To(two),
				ValidRange: &resourceapi.CapacityRequestPolicyRange{
					Min:  ptr.To(two),
					Step: ptr.To(two),
					Max:  ptr.To(four),
				},
			},
		}
	}
	return wrapDevice{Device: *device}
}

// withFractionalCapacityRequestPolicyRange adds capacity with a fractional requestPolicy
// (min=200m, step=100m, max=1, default=200m).
// withFractionalCapacityRequestPolicyRange 为给定容量添加小数请求策略范围（min=200m，step=100m，max=1，default=200m）。参数 capacity 为容量名到容量值的映射。
func (in wrapDevice) withFractionalCapacityRequestPolicyRange(capacity map[resourceapi.QualifiedName]resource.Quantity) wrapDevice {
	inDevice := in.Device
	device := inDevice.DeepCopy()
	if device.Capacity == nil {
		device.Capacity = make(map[resourceapi.QualifiedName]resourceapi.DeviceCapacity, len(capacity))
	}
	for name, quantity := range capacity {
		device.Capacity[name] = resourceapi.DeviceCapacity{
			Value: quantity,
			RequestPolicy: &resourceapi.CapacityRequestPolicy{
				Default: &pointTwo,
				ValidRange: &resourceapi.CapacityRequestPolicyRange{
					Min:  &pointTwo,
					Step: &pointOne,
					Max:  &one,
				},
			},
		}
	}
	return wrapDevice{Device: *device}
}

// withCapacityRequestPolicyValidValues adds capacity with default valid values (1)
// withCapacityRequestPolicyValidValues 为给定容量添加离散合法值请求策略：defaultValue 作为默认值且为最小值，additionalValidValues 为额外合法值，capacity 为容量名到容量值的映射。
func (in wrapDevice) withCapacityRequestPolicyValidValues(defaultValue resource.Quantity, capacity map[resourceapi.QualifiedName]resource.Quantity, additionalValidValues []resource.Quantity) wrapDevice {
	inDevice := in.Device
	device := inDevice.DeepCopy()
	if device.Capacity == nil {
		device.Capacity = make(map[resourceapi.QualifiedName]resourceapi.DeviceCapacity, len(capacity))
	}
	for name, quantity := range capacity {
		validValues := []resource.Quantity{defaultValue}            // default is minimum
		validValues = append(validValues, additionalValidValues...) // append with additional valid values
		device.Capacity[name] = resourceapi.DeviceCapacity{
			Value: quantity,
			RequestPolicy: &resourceapi.CapacityRequestPolicy{
				Default:     ptr.To(defaultValue),
				ValidValues: validValues,
			},
		}
	}
	return wrapDevice{Device: *device}
}

// deviceCounterConsumption 构造一个 DeviceCounterConsumption：指定计数器集 counterSet 及消耗的计数器映射 counters（Quantity 会转为 Counter）。
func deviceCounterConsumption(counterSet string, counters map[string]resource.Quantity) resourceapi.DeviceCounterConsumption {
	return resourceapi.DeviceCounterConsumption{
		CounterSet: counterSet,
		Counters:   toCounters(counters),
	}
}

// deviceCounterConsumptionWithGroups 在 deviceCounterConsumption 基础上额外设置兼容性组 compatibilityGroups，用于按计数器集跟踪设备的兼容组。
func deviceCounterConsumptionWithGroups(counterSet string, counters map[string]resource.Quantity, compatibilityGroups ...string) resourceapi.DeviceCounterConsumption {
	return resourceapi.DeviceCounterConsumption{
		CounterSet:          counterSet,
		Counters:            toCounters(counters),
		CompatibilityGroups: compatibilityGroups,
	}
}

// nodeSelectionAll 与 nodeSelectionPerDevice 是 slice()、withNodeSelection() 的特殊节点选择标记：前者表示切片对所有节点可见，后者表示按设备分别选择节点。
const (
	nodeSelectionAll       = "nodeSelectionAll"
	nodeSelectionPerDevice = "nodeSelectionPerDevice"
)

// generate a ResourceSlice object with the given name, node,
// driver and pool names and generation.
// The nodeSelection parameter may be a string with the value
// nodeSelectionAll for all nodes, the value nodeSelectionPerDevice
// for per device node selection, or any other value to set the
// node name. Providing a node selectors sets the NodeSelector field.
// slice 构造一个 ResourceSlice。参数 name 为切片名；pool 可为 ResourcePool 或池名字符串（后者生成默认 Generation=1、切片数=1 的池）；driver 为驱动名；nodeSelection 可为 *v1.NodeSelector、nodeSelectionAll、nodeSelectionPerDevice 或具体节点名字符串。
func slice(name string, nodeSelection, pool any, driver string) *resourceapi.ResourceSlice {
	var resourcePool resourceapi.ResourcePool
	// pool 支持直接传入 ResourcePool，或传入池名字符串（使用默认的 Generation 与切片数）。
	switch poolSelection := pool.(type) {
	case resourceapi.ResourcePool:
		resourcePool = poolSelection
	case string:
		resourcePool = resourceapi.ResourcePool{
			Name:               poolSelection,
			ResourceSliceCount: 1,
			Generation:         1,
		}
	default:
		panic(fmt.Sprintf("unexpected pool type %T: %+v", poolSelection, poolSelection))
	}

	slice := &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: resourceapi.ResourceSliceSpec{
			Driver: driver,
			Pool:   resourcePool,
		},
	}
	// nodeSelection 支持多种形态：节点选择器、全部节点、逐设备选择或具体节点名。
	switch nodeSelection := nodeSelection.(type) {
	case *v1.NodeSelector:
		slice.Spec.NodeSelector = nodeSelection
	case string:
		if nodeSelection == nodeSelectionAll {
			slice.Spec.AllNodes = ptr.To(true)
		} else if nodeSelection == nodeSelectionPerDevice {
			slice.Spec.PerDeviceNodeSelection = func() *bool {
				r := true
				return &r
			}()
		} else {
			slice.Spec.NodeName = ptr.To(nodeSelection)
		}
	default:
		panic(fmt.Sprintf("unexpected nodeSelection type %T: %+v", nodeSelection, nodeSelection))
	}

	return slice
}

// sliceWithDevices 基于 slice 构造切片，并把给定的 devices（解包后的 Device）追加到 Spec.Devices，返回可继续链式设置的包装对象。
func sliceWithDevices(name string, nodeSelection, pool any, driver string, devices ...wrapDevice) wrapResourceSliceWithDevices {
	slice := slice(name, nodeSelection, pool, driver)
	for _, device := range devices {
		slice.Spec.Devices = append(slice.Spec.Devices, resourceapi.Device(device.Device))
	}
	return wrapResourceSliceWithDevices{ResourceSlice: slice}
}

// wrapResourceSliceWithDevices 包装 *resourceapi.ResourceSlice（含设备），提供 obj 与 withSkipNodeOperations 方法。
type wrapResourceSliceWithDevices struct {
	*resourceapi.ResourceSlice
}

// obj 返回被包装的原始 ResourceSlice 指针。
func (in wrapResourceSliceWithDevices) obj() *resourceapi.ResourceSlice {
	return in.ResourceSlice
}

// withSkipNodeOperations 返回一份深拷贝，并设置切片的可跳过节点操作列表。
func (in wrapResourceSliceWithDevices) withSkipNodeOperations(skip ...resourceapi.SkipNodeOperation) wrapResourceSliceWithDevices {
	out := in.DeepCopy()
	out.Spec.SkipNodeOperations = skip
	return wrapResourceSliceWithDevices{ResourceSlice: out}
}

// sliceWithCounterSets 基于 slice 构造切片，并把给定的 counterSets 设置为共享计数器（Spec.SharedCounters）。
func sliceWithCounterSets(name string, nodeSelection, pool any, driver string, counterSets ...resourceapi.CounterSet) wrapResourceSliceWithCounterSets {
	slice := slice(name, nodeSelection, pool, driver)
	slice.Spec.SharedCounters = counterSets
	return wrapResourceSliceWithCounterSets{ResourceSlice: slice}
}

// wrapResourceSliceWithCounterSets 包装 *resourceapi.ResourceSlice（含共享计数器），提供 obj 方法。
type wrapResourceSliceWithCounterSets struct {
	*resourceapi.ResourceSlice
}

// obj 返回被包装的原始 ResourceSlice 指针。
func (in wrapResourceSliceWithCounterSets) obj() *resourceapi.ResourceSlice {
	return in.ResourceSlice
}

// resourcePool 构造一个 ResourcePool：指定池名 name 与切片总数 count，Generation 固定为 1。
func resourcePool(name string, count int64) resourceapi.ResourcePool {
	return resourceapi.ResourcePool{
		Name:               name,
		ResourceSliceCount: count,
		Generation:         1,
	}
}

// deviceAllocationResult 构造一个 DeviceRequestAllocationResult：指定请求名、驱动、池、设备、是否管理员访问以及容忍列表；adminAccess 为 false 时不设置该字段。
func deviceAllocationResult(request, driver, pool, device string, adminAccess bool, tolerations ...resourceapi.DeviceToleration) resourceapi.DeviceRequestAllocationResult {
	r := resourceapi.DeviceRequestAllocationResult{
		Request:     request,
		Driver:      driver,
		Pool:        pool,
		Device:      device,
		Tolerations: tolerations,
	}
	if adminAccess {
		r.AdminAccess = &adminAccess
	}
	return r
}

// deviceAllocationResultWithSkipNodeOperations 在 deviceAllocationResult 基础上设置可跳过的节点操作列表（非管理员访问）。
func deviceAllocationResultWithSkipNodeOperations(request, driver, pool, device string, skip ...resourceapi.SkipNodeOperation) resourceapi.DeviceRequestAllocationResult {
	r := deviceAllocationResult(request, driver, pool, device, false)
	r.SkipNodeOperations = skip
	return r
}

// deviceRequestAllocationResult can replace deviceAllocationResult
// deviceRequestAllocationResult 构造一个可链式设置的 wrapDeviceRequestAllocationResult（用 withConsumedCapacity 追加已消耗容量信息），可替代 deviceAllocationResult 使用。
func deviceRequestAllocationResult(request, driver, pool, device string) wrapDeviceRequestAllocationResult {
	return wrapDeviceRequestAllocationResult{
		&resourceapi.DeviceRequestAllocationResult{
			Request: request,
			Driver:  driver,
			Pool:    pool,
			Device:  device,
		},
	}
}

// wrapDeviceRequestAllocationResult 包装 *resourceapi.DeviceRequestAllocationResult，提供 obj 与 withConsumedCapacity 方法。
type wrapDeviceRequestAllocationResult struct {
	*resourceapi.DeviceRequestAllocationResult
}

// obj 返回被包装的原始 DeviceRequestAllocationResult 指针。
func (in wrapDeviceRequestAllocationResult) obj() *resourceapi.DeviceRequestAllocationResult {
	return in.DeviceRequestAllocationResult
}

// withConsumedCapacity 返回一份深拷贝，并设置份额 ID shareID 与各容量名的已消耗量 consumedCapacity。
func (in wrapDeviceRequestAllocationResult) withConsumedCapacity(shareID *apitypes.UID,
	consumedCapacity map[resourceapi.QualifiedName]resource.Quantity) resourceapi.DeviceRequestAllocationResult {
	out := in.DeepCopy()
	out.ShareID = shareID
	out.ConsumedCapacity = consumedCapacity
	return *out
}

// capacityRequests 构造一个 CapacityRequirements，其容量请求由 requirements(request) 生成。
func capacityRequests(request *resource.Quantity) *resourceapi.CapacityRequirements {
	return &resourceapi.CapacityRequirements{
		Requests: requirements(request),
	}
}

// requirements 返回一个容量名到容量的映射：request 非空时以 capacity0 为键记录该容量，否则返回空映射。
func requirements(request *resource.Quantity) map[resourceapi.QualifiedName]resource.Quantity {
	r := make(map[resourceapi.QualifiedName]resource.Quantity, 0)
	if request != nil {
		r[capacity0] = *request
	}
	return r
}

// multipleDeviceAllocationResults 为单个请求生成连续的分配结果：从 startIndex 起共 count 个设备，设备名形如 device-N。
func multipleDeviceAllocationResults(request, driver, pool string, count, startIndex int) []resourceapi.DeviceRequestAllocationResult {
	var results []resourceapi.DeviceRequestAllocationResult
	for i := startIndex; i < startIndex+count; i++ {
		results = append(results, deviceAllocationResult(request, driver, pool, fmt.Sprintf("device-%d", i), false))
	}
	return results
}

// nodeLabelSelector creates a node selector with a label match for "key" in "values".
// nodeLabelSelector 构造一个节点选择器，按标签 key 以 In 操作符匹配给定的 values。
func nodeLabelSelector(key string, values ...string) *v1.NodeSelector {
	requirements := []v1.NodeSelectorRequirement{{
		Key:      key,
		Operator: v1.NodeSelectorOpIn,
		Values:   values,
	}}
	selector := &v1.NodeSelector{NodeSelectorTerms: []v1.NodeSelectorTerm{{MatchExpressions: requirements}}}
	return selector
}

// localNodeSelector returns a node selector for a specific node.
// localNodeSelector 构造选择特定节点的节点选择器：按 metadata.name 字段（fieldNameKey）精确选择 nodeName，通过交换 MatchExpressions 与 MatchFields 实现。
func localNodeSelector(nodeName string) *v1.NodeSelector {
	selector := nodeLabelSelector(fieldNameKey, nodeName)
	// Swap the requirements: we need to select by field, not label.
	selector.NodeSelectorTerms[0].MatchFields, selector.NodeSelectorTerms[0].MatchExpressions = selector.NodeSelectorTerms[0].MatchExpressions, selector.NodeSelectorTerms[0].MatchFields
	return selector
}

// allocationResult returns a matcher for one AllocationResult pointer with a list of
// embedded device allocation results. The order of those results doesn't matter.
// allocationResult 返回一个 Gomega 匹配器，用于断言单个 AllocationResult：Results 与给定结果集合一致（顺序无关）、Config 为 nil、NodeSelector 匹配 selector、AllocationTimestamp 为 nil。
func allocationResult(selector *v1.NodeSelector, results ...resourceapi.DeviceRequestAllocationResult) types.GomegaMatcher {
	return gstruct.MatchFields(0, gstruct.Fields{
		"Devices": gstruct.MatchFields(0, gstruct.Fields{
			"Results": gomega.ConsistOf(results), // Order is irrelevant.
			"Config":  gomega.BeNil(),
		}),
		"NodeSelector":        matchNodeSelector(selector),
		"AllocationTimestamp": gomega.BeNil(),
	})
}

// matchNodeSelector returns a matcher for a node selector. The order
// of terms, requirements, and values is irrelevant.
// matchNodeSelector 返回节点选择器的匹配器：selector 为 nil 时断言字段为 nil，否则递归匹配其 NodeSelectorTerms（顺序无关）。
func matchNodeSelector(selector *v1.NodeSelector) types.GomegaMatcher {
	if selector == nil {
		return gomega.BeNil()
	}
	return gomega.HaveField("NodeSelectorTerms", matchNodeSelectorTerms(selector.NodeSelectorTerms))
}

// matchNodeSelectorTerms 为给定的节点选择器项列表生成匹配器集合，匹配顺序无关。
func matchNodeSelectorTerms(terms []v1.NodeSelectorTerm) types.GomegaMatcher {
	var matchTerms []types.GomegaMatcher
	for _, term := range terms {
		matchTerms = append(matchTerms, matchNodeSelectorTerm(term))
	}
	return gomega.ConsistOf(matchTerms)
}

// matchNodeSelectorTerm 为单个 NodeSelectorTerm 生成匹配器，分别匹配 MatchExpressions 与 MatchFields。
func matchNodeSelectorTerm(term v1.NodeSelectorTerm) types.GomegaMatcher {
	return gstruct.MatchFields(0, gstruct.Fields{
		"MatchExpressions": matchNodeSelectorRequirements(term.MatchExpressions),
		"MatchFields":      matchNodeSelectorRequirements(term.MatchFields),
	})
}

// matchNodeSelectorRequirements 为节点选择器需求列表生成匹配器集合，匹配顺序无关。
func matchNodeSelectorRequirements(requirements []v1.NodeSelectorRequirement) types.GomegaMatcher {
	var matchRequirements []types.GomegaMatcher
	for _, requirement := range requirements {
		matchRequirements = append(matchRequirements, matchNodeSelectorRequirement(requirement))
	}
	return gomega.ConsistOf(matchRequirements)
}

// matchNodeSelectorRequirement 为单个 NodeSelectorRequirement 生成匹配器，匹配 Key、Operator，并以顺序无关方式匹配 Values。
func matchNodeSelectorRequirement(requirement v1.NodeSelectorRequirement) types.GomegaMatcher {
	return gstruct.MatchFields(0, gstruct.Fields{
		"Key":      gomega.Equal(requirement.Key),
		"Operator": gomega.Equal(requirement.Operator),
		"Values":   gomega.ConsistOf(requirement.Values),
	})
}

// allocationResultWithConfig 构造一个含单条设备配置的 AllocationResult：配置来源为 source，参数由 deviceConfiguration(driver, attribute) 生成，并附带节点选择器与分配结果列表。
func allocationResultWithConfig(selector *v1.NodeSelector, driver string, source resourceapi.AllocationConfigSource, attribute string, results ...resourceapi.DeviceRequestAllocationResult) resourceapi.AllocationResult {
	return resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: results,
			Config: []resourceapi.DeviceAllocationConfiguration{
				{
					Source:              source,
					DeviceConfiguration: deviceConfiguration(driver, attribute),
				},
			},
		},
		NodeSelector: selector,
	}
}

// allocationResultWithConfigs 构造一个含给定配置列表 configs 和分配结果列表 results 的 AllocationResult，并附带节点选择器。
func allocationResultWithConfigs(selector *v1.NodeSelector, results []resourceapi.DeviceRequestAllocationResult, configs []resourceapi.DeviceAllocationConfiguration) resourceapi.AllocationResult {
	return resourceapi.AllocationResult{
		Devices: resourceapi.DeviceAllocationResult{
			Results: results,
			Config:  configs,
		},
		NodeSelector: selector,
	}
}

// Helpers

// convert a list of objects to a slice
// objects 把可变参数列表原样转换为切片，便于在测试用例中就地构造 []T。
func objects[T any](objs ...T) []T {
	return objs
}

// convert a list of wrapper objects to a slice
// unwrap 把一组实现了 wrapper[T] 的包装对象解包为 []T。
func unwrap[T any, O wrapper[T]](objs ...O) []T {
	out := make([]T, len(objs))
	for i, obj := range objs {
		out[i] = obj.obj()
	}
	return out
}

// wrapper 是所有测试包装类型需实现的接口，obj() 返回被包装的原始对象。
type wrapper[T any] interface {
	obj() T
}

// convert a list of wrapper objects to a slice
// unwrapResourceSlices 把一组实现 resourceSliceWrapper 的切片包装对象解包为 []*resourceapi.ResourceSlice。
func unwrapResourceSlices(objs ...resourceSliceWrapper) []*resourceapi.ResourceSlice {
	out := make([]*resourceapi.ResourceSlice, len(objs))
	for i, obj := range objs {
		out[i] = obj.obj()
	}
	return out
}

// resourceSliceWrapper 是资源切片包装类型需实现的接口，obj() 返回被包装的 ResourceSlice。
type resourceSliceWrapper interface {
	obj() *resourceapi.ResourceSlice
}

// generate a ResourceSlice object with the given parameters and no devices
// sliceWithNoDevices 构造一个不含任何设备的 ResourceSlice（用于测试空池场景）。
func sliceWithNoDevices(name string, nodeSelection, pool any, driver string) wrapResourceSliceWithDevices {
	return sliceWithDevices(name, nodeSelection, pool, driver)
}

// generate a ResourceSlice object with the given parameters and one device "device-1"
// sliceWithOneDevice 构造一个仅含 device-1 一台设备的 ResourceSlice。
func sliceWithOneDevice(name string, nodeSelection, pool any, driver string) wrapResourceSliceWithDevices {
	return sliceWithDevices(name, nodeSelection, pool, driver, device(device1, nil, nil))
}

// generate a ResourceSclie object with the given parameters and the specified number of devices.
// sliceWithMultipleDevices 构造一个含 count 台设备（device-0、device-1……）的 ResourceSlice。
func sliceWithMultipleDevices(name string, nodeSelection, pool any, driver string, count int) wrapResourceSliceWithDevices {
	var devices []wrapDevice
	for i := 0; i < count; i++ {
		devices = append(devices, device(fmt.Sprintf("device-%d", i), nil, nil))
	}
	return sliceWithDevices(name, nodeSelection, pool, driver, devices...)
}

// counterSet 构造一个 CounterSet：指定名称 name 与计数器映射 counters（Quantity 会转为 Counter）。
func counterSet(name string, counters map[string]resource.Quantity) resourceapi.CounterSet {
	return resourceapi.CounterSet{
		Name:     name,
		Counters: toCounters(counters),
	}
}

// toDeviceCapacity 把容量名到 Quantity 的映射转换为容量名到 DeviceCapacity 的映射。
func toDeviceCapacity(capacity map[resourceapi.QualifiedName]resource.Quantity) map[resourceapi.QualifiedName]resourceapi.DeviceCapacity {
	out := make(map[resourceapi.QualifiedName]resourceapi.DeviceCapacity, len(capacity))
	for name, quantity := range capacity {
		out[name] = resourceapi.DeviceCapacity{Value: quantity}
	}
	return out
}

// toCounters 把计数器名到 Quantity 的映射转换为计数器名到 Counter 的映射。
func toCounters(counters map[string]resource.Quantity) map[string]resourceapi.Counter {
	out := make(map[string]resourceapi.Counter, len(counters))
	for name, quantity := range counters {
		out[string(name)] = resourceapi.Counter{Value: quantity}
	}
	return out
}

// deviceRequestAllocationResultWithBindingConditions returns an DeviceRequestAllocationResult object for testing purposes,
// specifying the driver, pool, device, usage restriction, binding conditions,
// binding failure conditions, and binding timeout.
// deviceRequestAllocationResultWithBindingConditions 构造一个带绑定条件、绑定失败条件（及可选容忍列表）的分配结果，用于验证绑定条件相关的分配行为。
func deviceRequestAllocationResultWithBindingConditions(request, driver, pool, device string, bindingConditions, bindingFailureConditions []string, tolerations ...resourceapi.DeviceToleration) resourceapi.DeviceRequestAllocationResult {
	return resourceapi.DeviceRequestAllocationResult{
		Request:                  request,
		Driver:                   driver,
		Pool:                     pool,
		Device:                   device,
		BindingConditions:        bindingConditions,
		BindingFailureConditions: bindingFailureConditions,
		Tolerations:              tolerations,
	}
}

// AllocatorTestCase 描述一个分配器测试用例：声明输入（特性、待分配声明、已分配设备/共享设备/容量、类、切片、节点）与期望输出（结果、错误、allocateOne 调用次数）。
// 各字段含义：features 为本用例开启的特性集合（依赖不支持特性的用例会被跳过）；
// claimsToAllocate 为待分配的 ResourceClaim 列表；allocatedDevices 为已分配的设备 ID 列表；
// allocatedSharedDeviceIDs 为已分配的共享设备 ID 集合；allocatedCapacityDevices 为已消耗的容量集合；
// classes 与 slices 分别为可用的设备类与资源切片列表；node 为本次分配针对的节点；
// expectResults 为期望的分配结果；expectError 用于断言错误；
// expectNumAllocateOneInvocations（及其按通道覆盖）为期望的 allocateOne 调用次数。
type AllocatorTestCase struct {
	features                 Features
	claimsToAllocate         []wrapResourceClaim
	allocatedDevices         []DeviceID
	allocatedSharedDeviceIDs sets.Set[SharedDeviceID]
	allocatedCapacityDevices ConsumedCapacityCollection
	classes                  []*resourceapi.DeviceClass
	slices                   []*resourceapi.ResourceSlice
	node                     *v1.Node

	// expectResults 为期望的分配结果列表。
	expectResults []any
	expectError   types.GomegaMatcher // can be used to check for no error or match specific error

	// Expected allocateOne invocations, asserted for every variant. A variant
	// that searches differently overrides this through the map below.
	// expectNumAllocateOneInvocations 为每个变体都断言的 allocateOne 调用次数；搜索方式不同的变体可通过下方映射覆盖。
	expectNumAllocateOneInvocations int64

	// expectNumAllocateOneInvocationsByChannel overrides expectNumAllocateOneInvocations with
	// different values for specific implementations (e.g. "experimental").
	//
	// Ignored unless expectNumAllocateOneInvocations is also set.
	// expectNumAllocateOneInvocations should contain the "best" result, so
	// expectNumAllocateOneInvocationsByChannel is only needed as long as we have "worse"
	// implementations.
	// expectNumAllocateOneInvocationsByChannel 按不同实现（如 experimental）分别覆盖调用次数，仅在 expectNumAllocateOneInvocations 已设置时生效。
	expectNumAllocateOneInvocationsByChannel map[internal.AllocatorChannel]int64
}

// requestWithDerivedAttributes 在 request 基础上为精确请求设置派生属性 derivedAttrs，并可附带选择器。
func requestWithDerivedAttributes(name, class string, count int64, derivedAttrs []resourceapi.DeviceDerivedAttribute, selectors ...resourceapi.DeviceSelector) resourceapi.DeviceRequest {
	req := request(name, class, count, selectors...)
	req.Exactly.DerivedAttributes = derivedAttrs
	return req
}

// TestAllocator runs as many of the shared tests against a specific allocator implementation as possible.
// Test cases which depend on features that are not supported by the implementation are silently skipped.
// TestAllocator 针对某个具体的分配器实现运行尽可能多的共享测试用例；依赖该实现不支持的特性的用例会被静默跳过。参数 supportedFeatures 声明该实现支持的特性，newAllocator 是用于构造分配器实例的工厂函数。
func TestAllocator(t *testing.T,
	supportedFeatures Features,
	newAllocator func(
		ctx context.Context,
		features Features,
		allocateState AllocatedState,
		classLister DeviceClassLister,
		slices []*resourceapi.ResourceSlice,
		celCache *cel.Cache,
	) (Allocator, error)) {
	// 以下为本文件各测试用例复用的属性全限定名（含驱动前缀）与污染相关 fixture。
	nonExistentAttribute := resourceapi.FullyQualifiedName(driverA + "/" + "NonExistentAttribute")
	boolAttribute := resourceapi.FullyQualifiedName(driverA + "/" + "boolAttribute")
	stringAttribute := resourceapi.FullyQualifiedName(driverA + "/" + "stringAttribute")
	versionAttribute := resourceapi.FullyQualifiedName(driverA + "/" + "driverVersion")
	intAttribute := resourceapi.FullyQualifiedName(driverA + "/" + "numa")
	numaNodeAttribute := resourceapi.FullyQualifiedName("resource.kubernetes.io/numaNode")
	// 污点及其对应的容忍定义：None、NoSchedule、NoExecute 三种效果，以及按 Exists/Equal 匹配的容忍。
	taintKey := "taint-key"
	taintValue := "taint-value"
	taintValue2 := "taint-value-2"
	taintNone := resourceapi.DeviceTaint{
		Key:    taintKey,
		Value:  taintValue,
		Effect: resourceapi.DeviceTaintEffectNone,
	}
	taintNoSchedule := resourceapi.DeviceTaint{
		Key:    taintKey,
		Value:  taintValue,
		Effect: resourceapi.DeviceTaintEffectNoSchedule,
	}
	taintNoExecute := resourceapi.DeviceTaint{
		Key:    taintKey,
		Value:  taintValue2,
		Effect: resourceapi.DeviceTaintEffectNoExecute,
	}
	tolerationNoSchedule := resourceapi.DeviceToleration{
		Operator: resourceapi.DeviceTolerationOpExists,
		Key:      taintKey,
		Effect:   resourceapi.DeviceTaintEffectNoSchedule,
	}
	tolerationNoExecute := resourceapi.DeviceToleration{
		Operator: resourceapi.DeviceTolerationOpEqual,
		Key:      taintKey,
		Value:    taintValue2,
		Effect:   resourceapi.DeviceTaintEffectNoExecute,
	}
	tolerationNoScheduleKey1 := resourceapi.DeviceToleration{
		Operator: resourceapi.DeviceTolerationOpExists,
		Key:      "key1",
		Effect:   resourceapi.DeviceTaintEffectNoSchedule,
	}

	testcases := map[string]AllocatorTestCase{
		// "empty": 空输入用例：无声明、无类、无切片，验证无分配结果且无错误。
		"empty": {},
		// Two requests in one claim, both landing on the same counter set with the
		// same compatibility group: they can be co-allocated.
		// "compatibility-groups-compatible": 同一声明内两个请求落在同一计数器集且兼容组一致（均为 mig），验证二者可被共同分配。
		"compatibility-groups-compatible": {
			features: Features{PartitionableDevices: true, CompatibilityGroups: true},
			claimsToAllocate: objects(claim(claim0).withRequests(
				deviceRequest(req0, classA, 1),
				deviceRequest(req1, classA, 1),
			)),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumptionWithGroups(counterSet1, map[string]resource.Quantity{capacity0: two}, "mig"),
					),
					device(device2, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumptionWithGroups(counterSet1, map[string]resource.Quantity{capacity0: two}, "mig"),
					),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1, map[string]resource.Quantity{capacity0: four}),
				),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req1, driverA, pool1, device2, false),
			)},
		},
		// Two requests in one claim, the only candidates are an incompatible pair on
		// the same counter set (mig vs vgpu): no combination satisfies the claim.
		// "compatibility-groups-incompatible": 唯一候选的两个设备在同一计数器集上分属互斥兼容组（mig 与 vgpu），验证无任何组合能满足声明、分配失败。
		"compatibility-groups-incompatible": {
			features: Features{PartitionableDevices: true, CompatibilityGroups: true},
			claimsToAllocate: objects(claim(claim0).withRequests(
				deviceRequest(req0, classA, 1),
				deviceRequest(req1, classA, 1),
			)),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumptionWithGroups(counterSet1, map[string]resource.Quantity{capacity0: two}, "mig"),
					),
					device(device2, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumptionWithGroups(counterSet1, map[string]resource.Quantity{capacity0: two}, "vgpu"),
					),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1, map[string]resource.Quantity{capacity0: four}),
				),
			),
			node:          node(node1, region1),
			expectResults: nil,
		},
		// A single-device claim whose first candidate (device1) declares group "mig"
		// and carries a NoSchedule taint, while the second (device2) declares the
		// disjoint group "vgpu". device1 is rejected by its taint; the compatibility
		// narrowing it consumed on counterSet1 must be rolled back so device2, which
		// shares no group with it, can still be allocated.
		// "compatibility-groups-taint-rollback": 首选设备 device1 带 NoSchedule 污点且属 mig 组、次选 device2 属 vgpu 组；device1 因污点被拒后其兼容组收窄须回滚，使不共享组的 device2 仍可分配。
		"compatibility-groups-taint-rollback": {
			features: Features{PartitionableDevices: true, CompatibilityGroups: true, DeviceTaints: true},
			claimsToAllocate: objects(claim(claim0).withRequests(
				deviceRequest(req0, classA, 1),
			)),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, nil).withTaints(taintNoSchedule).withDeviceCounterConsumption(
						deviceCounterConsumptionWithGroups(counterSet1, map[string]resource.Quantity{capacity0: two}, "mig"),
					),
					device(device2, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumptionWithGroups(counterSet1, map[string]resource.Quantity{capacity0: two}, "vgpu"),
					),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1, map[string]resource.Quantity{capacity0: four}),
				),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device2, false),
			)},
		},
		// An incompatible device is already allocated on the counter set; its groups
		// are read live from the slice. The only remaining candidate shares no
		// group, so the claim is unschedulable.
		// "compatibility-groups-incompatible-with-allocated-peer": 计数器集上已分配属 mig 组的设备，候选设备属 vgpu 组且组信息从切片实时读取，二者不共享组，声明不可调度。
		"compatibility-groups-incompatible-with-allocated-peer": {
			features:         Features{PartitionableDevices: true, CompatibilityGroups: true},
			claimsToAllocate: objects(claim(claim0).withRequests(deviceRequest(req0, classA, 1))),
			allocatedDevices: []DeviceID{MakeDeviceID(driverA, pool1, device1)},
			classes:          objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumptionWithGroups(counterSet1, map[string]resource.Quantity{capacity0: one}, "mig"),
					),
					device(device2, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumptionWithGroups(counterSet1, map[string]resource.Quantity{capacity0: one}, "vgpu"),
					),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1, map[string]resource.Quantity{capacity0: four}),
				),
			),
			node:          node(node1, region1),
			expectResults: nil,
		},
		// A compatible device is already allocated on the counter set; the candidate
		// shares the group the peer declares in the slice and is admitted.
		// "compatibility-groups-compatible-with-allocated-peer": 计数器集上已分配属 mig 组的设备，候选设备同样声明 mig 组，验证其被接纳并分配。
		"compatibility-groups-compatible-with-allocated-peer": {
			features:         Features{PartitionableDevices: true, CompatibilityGroups: true},
			claimsToAllocate: objects(claim(claim0).withRequests(deviceRequest(req0, classA, 1))),
			allocatedDevices: []DeviceID{MakeDeviceID(driverA, pool1, device1)},
			classes:          objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumptionWithGroups(counterSet1, map[string]resource.Quantity{capacity0: one}, "mig"),
					),
					device(device2, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumptionWithGroups(counterSet1, map[string]resource.Quantity{capacity0: one}, "mig"),
					),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1, map[string]resource.Quantity{capacity0: four}),
				),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device2, false),
			)},
		},
		// With the feature disabled, a slice with a device declaring
		// compatibility groups is ignored during pool gathering (the scheduler
		// cannot validate it), which leaves its pool incomplete and blocks the
		// pool as a whole. A separate pool without groups is still allocatable.
		// This is the feature-off safety behavior and is verified across all
		// allocator variants.
		// "compatibility-groups-disabled-skips-grouped-pool": 特性关闭时声明兼容组的设备所在池在池收集阶段被忽略、导致该池不完整被阻断；无组的另一池仍可分配，验证特性关闭的安全行为。
		"compatibility-groups-disabled-skips-grouped-pool": {
			// CompatibilityGroups is set explicitly to false so this case keeps
			// exercising the feature-disabled path unchanged after the feature
			// graduates to on-by-default. The scope is the pool: device1's pool1
			// is off-limits, but device2 in the group-free pool2 is still
			// allocated.
			features: Features{PartitionableDevices: true, CompatibilityGroups: false},
			claimsToAllocate: objects(claim(claim0).withRequests(
				deviceRequest(req0, classA, 1),
			)),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumptionWithGroups(counterSet1, map[string]resource.Quantity{capacity0: two}, "mig"),
					),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1, map[string]resource.Quantity{capacity0: four}),
				),
				sliceWithDevices(slice3, node1, resourcePool(pool2, 2), driverA,
					device(device2, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet2, map[string]resource.Quantity{capacity0: two}),
					),
				),
				sliceWithCounterSets(slice4, node1, resourcePool(pool2, 2), driverA,
					counterSet(counterSet2, map[string]resource.Quantity{capacity0: four}),
				),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool2, device2, false),
			)},
		},
		// With the feature disabled, a candidate that declares no groups is not
		// allocatable either when another device of the same pool declares them:
		// only the slice with the grouped device1 gets ignored during pool
		// gathering, but that leaves the pool incomplete, which also blocks
		// device2 in the remaining device slice - whether or not device1 is
		// allocated, so that the outcome does not depend on allocation order.
		// Verified across all allocator variants, including stable.
		// "compatibility-groups-disabled-skips-ungrouped-device-in-grouped-pool": 特性关闭时同池若有设备声明兼容组，未声明组的候选设备也不可分配（池不完整），且结果与分配顺序无关。
		"compatibility-groups-disabled-skips-ungrouped-device-in-grouped-pool": {
			features:         Features{PartitionableDevices: true, CompatibilityGroups: false},
			claimsToAllocate: objects(claim(claim0).withRequests(deviceRequest(req0, classA, 1))),
			classes:          objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 3), driverA,
					device(device1, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumptionWithGroups(counterSet1, map[string]resource.Quantity{capacity0: one}, "mig"),
					),
				),
				sliceWithDevices(slice2, node1, resourcePool(pool1, 3), driverA,
					device(device2, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1, map[string]resource.Quantity{capacity0: one}),
					),
				),
				sliceWithCounterSets(slice3, node1, resourcePool(pool1, 3), driverA,
					counterSet(counterSet1, map[string]resource.Quantity{capacity0: four}),
				),
			),
			node:          node(node1, region1),
			expectResults: nil,
		},
		// Groups are tracked per counter set: two devices whose groups are
		// disjoint can still be co-allocated when they draw from different
		// counter sets of the same pool.
		// "compatibility-groups-multiple-counter-sets": 兼容组按计数器集分别跟踪：两个设备组名互斥但分属同一池的不同计数器集，仍可共同分配。
		"compatibility-groups-multiple-counter-sets": {
			features: Features{PartitionableDevices: true, CompatibilityGroups: true},
			claimsToAllocate: objects(claim(claim0).withRequests(
				deviceRequest(req0, classA, 1),
				deviceRequest(req1, classA, 1),
			)),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumptionWithGroups(counterSet1, map[string]resource.Quantity{capacity0: two}, "mig"),
					),
					device(device2, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumptionWithGroups(counterSet2, map[string]resource.Quantity{capacity0: two}, "vgpu"),
					),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1, map[string]resource.Quantity{capacity0: four}),
					counterSet(counterSet2, map[string]resource.Quantity{capacity0: four}),
				),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req1, driverA, pool1, device2, false),
			)},
		},
		// A device drawing from several counter sets must be admitted by every
		// one of them. device2 is unopposed on counterSet2 (nothing else draws
		// from that set) but shares no group with device1 on counterSet1, so
		// no combination satisfies the claim - including the backtracked order
		// where device2 goes first and its narrowing of both counter sets has
		// to be rolled back.
		// "compatibility-groups-multiple-counter-sets-spanning-device": 一个设备从多个计数器集消耗容量时必须被每个集合接纳；device2 在 counterSet1 与 device1 组互斥，验证无组合可满足（含回溯路径）。
		"compatibility-groups-multiple-counter-sets-spanning-device": {
			features: Features{PartitionableDevices: true, CompatibilityGroups: true},
			claimsToAllocate: objects(claim(claim0).withRequests(
				deviceRequest(req0, classA, 1),
				deviceRequest(req1, classA, 1),
			)),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumptionWithGroups(counterSet1, map[string]resource.Quantity{capacity0: one}, "mig"),
					),
					device(device2, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumptionWithGroups(counterSet1, map[string]resource.Quantity{capacity0: one}, "vgpu"),
						deviceCounterConsumptionWithGroups(counterSet2, map[string]resource.Quantity{capacity0: one}, "vgpu"),
					),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1, map[string]resource.Quantity{capacity0: four}),
					counterSet(counterSet2, map[string]resource.Quantity{capacity0: four}),
				),
			),
			node:          node(node1, region1),
			expectResults: nil,
		},
		// The first subrequest asks for two devices, but only one candidate is
		// compatible with the "mig" device taken by req0 (device3 declares the
		// disjoint "vgpu"), so it fails and its tentative narrowing is rolled
		// back before the allocator falls back to the second subrequest.
		// "compatibility-groups-with-prioritized-list": 优先级列表场景：首个子请求要两台设备，但仅一台与 req0 占用的 mig 组兼容（device3 属互斥 vgpu），验证首个子请求失败并回滚后回退到第二个子请求。
		"compatibility-groups-with-prioritized-list": {
			features: Features{PartitionableDevices: true, CompatibilityGroups: true, PrioritizedList: true},
			claimsToAllocate: objects(claim(claim0).withRequests(
				deviceRequest(req0, classA, 1),
				requestWithPrioritizedList(req1,
					subRequest(subReq0, classA, 2),
					subRequest(subReq1, classA, 1),
				),
			)),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumptionWithGroups(counterSet1, map[string]resource.Quantity{capacity0: one}, "mig"),
					),
					device(device2, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumptionWithGroups(counterSet1, map[string]resource.Quantity{capacity0: one}, "mig"),
					),
					device(device3, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumptionWithGroups(counterSet1, map[string]resource.Quantity{capacity0: one}, "vgpu"),
					),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1, map[string]resource.Quantity{capacity0: four}),
				),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req1SubReq1, driverA, pool1, device2, false),
			)},
		},
		// Compatibility groups steer allocation among shareable devices: req1
		// does not fit on device1 (req0's share exhausted its capacity) and is
		// rejected on device2, which declares the disjoint "vgpu" group on the
		// same counter set, before landing on device3.
		// "compatibility-groups-with-consumable-capacity": 可共享设备场景：req1 在 device1 上容量不足、在属互斥 vgpu 组的 device2 上被拒，最终分配到 device3，验证兼容组引导可消耗容量的分配。
		"compatibility-groups-with-consumable-capacity": {
			features: Features{PartitionableDevices: true, CompatibilityGroups: true, ConsumableCapacity: true},
			claimsToAllocate: objects(claim(claim0).withRequests(
				deviceRequest(req0, classA, 1).withCapacityRequest(new(one)),
				deviceRequest(req1, classA, 1).withCapacityRequest(new(one)),
			)),
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumptionWithGroups(counterSet1, map[string]resource.Quantity{capacity0: one}, "mig"),
					).withAllowMultipleAllocations(),
					device(device2, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumptionWithGroups(counterSet1, map[string]resource.Quantity{capacity0: one}, "vgpu"),
					).withAllowMultipleAllocations(),
					device(device3, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumptionWithGroups(counterSet1, map[string]resource.Quantity{capacity0: one}, "mig"),
					).withAllowMultipleAllocations(),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1, map[string]resource.Quantity{capacity0: four}),
				),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceRequestAllocationResult(req0, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: one}),
				deviceRequestAllocationResult(req1, driverA, pool1, device3).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: one}),
			)},
		},
		// Two claims allocated in a single call must satisfy the constraint
		// jointly: the second claim's device is checked against the rolling
		// intersection left by the first claim's device.
		// "compatibility-groups-multiple-claims": 同一次调用中的两个声明须联合满足约束：第二个声明的设备依据第一个声明设备留下的组交集校验，验证两者均可分配。
		"compatibility-groups-multiple-claims": {
			features: Features{PartitionableDevices: true, CompatibilityGroups: true},
			claimsToAllocate: objects(
				claimWithRequest(claim0, req0, classA),
				claimWithRequest(claim1, req0, classA),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumptionWithGroups(counterSet1, map[string]resource.Quantity{capacity0: one}, "mig"),
					),
					device(device2, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumptionWithGroups(counterSet1, map[string]resource.Quantity{capacity0: one}, "mig"),
					),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1, map[string]resource.Quantity{capacity0: four}),
				),
			),
			node: node(node1, region1),
			expectResults: []any{
				allocationResult(localNodeSelector(node1), deviceAllocationResult(req0, driverA, pool1, device1, false)),
				allocationResult(localNodeSelector(node1), deviceAllocationResult(req0, driverA, pool1, device2, false)),
			},
		},
		// Each claim could be satisfied on its own, but not both in one call:
		// whichever device goes to the first claim, the other claim's only
		// remaining candidate declares a disjoint group on the same counter
		// set. The rolling intersection spans claims, so the joint allocation
		// is correctly refused.
		// "compatibility-groups-multiple-claims-incompatible": 两个声明单独均可满足，但同一次调用中因滚动组交集跨越声明而无法同时满足，验证联合分配被正确拒绝。
		"compatibility-groups-multiple-claims-incompatible": {
			features: Features{PartitionableDevices: true, CompatibilityGroups: true},
			claimsToAllocate: objects(
				claimWithRequest(claim0, req0, classA),
				claimWithRequest(claim1, req0, classA),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumptionWithGroups(counterSet1, map[string]resource.Quantity{capacity0: one}, "mig"),
					),
					device(device2, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumptionWithGroups(counterSet1, map[string]resource.Quantity{capacity0: one}, "vgpu"),
					),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1, map[string]resource.Quantity{capacity0: four}),
				),
			),
			node:          node(node1, region1),
			expectResults: nil,
		},
		// "simple": 基础用例：一个请求、一个类、一台设备，验证正常分配出本地节点选择器和设备结果。
		"simple": {
			claimsToAllocate: objects(claimWithRequest(claim0, req0, classA)),
			classes:          objects(class(classA, driverA)),
			slices:           unwrapResourceSlices(sliceWithOneDevice(slice1, node1, pool1, driverA)),
			node:             node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
			)},
		},
		// "optional-node-operations": 节点声明了 DRAOptionalNodeOperations 特性且切片设置了 SkipNodeOperations，验证分配结果携带可跳过节点操作。
		"optional-node-operations": {
			features:         Features{OptionalNodeOperations: true},
			claimsToAllocate: objects(claimWithRequest(claim0, req0, classA)),
			classes:          objects(class(classA, driverA)),
			slices:           unwrapResourceSlices(sliceWithOneDevice(slice1, node1, pool1, driverA).withSkipNodeOperations(resourceapi.SkipNodeOperationAll)),
			node:             nodeWithDRAOptionalNodeOperations(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResultWithSkipNodeOperations(req0, driverA, pool1, device1, resourceapi.SkipNodeOperationAll),
			)},
		},
		// "optional-node-operations-missing-feature": 切片设置了 SkipNodeOperations 但节点未声明对应特性，验证分配结果为空。
		"optional-node-operations-missing-feature": {
			features:         Features{OptionalNodeOperations: true},
			claimsToAllocate: objects(claimWithRequest(claim0, req0, classA)),
			classes:          objects(class(classA, driverA)),
			slices:           unwrapResourceSlices(sliceWithOneDevice(slice1, node1, pool1, driverA).withSkipNodeOperations(resourceapi.SkipNodeOperationAll)),
			node:             node(node1, region1),

			expectResults: []any{},
		},
		// "other-node": 同一节点上存在不同驱动/池的切片，验证只分配到与目标节点匹配的 driverA/pool2 设备。
		"other-node": {
			claimsToAllocate: objects(claimWithRequest(claim0, req0, classA)),
			classes:          objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithOneDevice(slice1, node1, pool1, driverB),
				sliceWithOneDevice(slice2, node2, pool2, driverA),
			),
			node: node(node2, region2),

			expectResults: []any{allocationResult(
				localNodeSelector(node2),
				deviceAllocationResult(req0, driverA, pool2, device1, false),
			)},
		},
		// "small-and-large": 两个请求分别要求内存≥1Gi 与≥2Gi，验证按容量大小分别分配到 device1 与 device2。
		"small-and-large": {
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				nil,
				request(req0, classA, 1, resourceapi.DeviceSelector{
					CEL: &resourceapi.CELDeviceSelector{
						Expression: fmt.Sprintf(`device.capacity["%s"].memory.compareTo(quantity("1Gi")) >= 0`, driverA),
					}}),
				request(req1, classA, 1, resourceapi.DeviceSelector{
					CEL: &resourceapi.CELDeviceSelector{
						Expression: fmt.Sprintf(`device.capacity["%s"].memory.compareTo(quantity("2Gi")) >= 0`, driverA),
					}}),
			)),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, map[resourceapi.QualifiedName]resource.Quantity{
					"memory": resource.MustParse("1Gi"),
				}, nil),
				device(device2, map[resourceapi.QualifiedName]resource.Quantity{
					"memory": resource.MustParse("2Gi"),
				}, nil),
			)),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req1, driverA, pool1, device2, false),
			)},
		},
		// "small-and-large-backtrack-requests": 设备顺序颠倒使“小”请求先占用“大”设备，验证初始决策被撤销（回溯）后得到正确分配。
		"small-and-large-backtrack-requests": {
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				nil,
				request(req0, classA, 1, resourceapi.DeviceSelector{
					CEL: &resourceapi.CELDeviceSelector{
						Expression: fmt.Sprintf(`device.capacity["%s"].memory.compareTo(quantity("1Gi")) >= 0`, driverA),
					}}),
				request(req1, classA, 1, resourceapi.DeviceSelector{
					CEL: &resourceapi.CELDeviceSelector{
						Expression: fmt.Sprintf(`device.capacity["%s"].memory.compareTo(quantity("2Gi")) >= 0`, driverA),
					}}),
			)),
			classes: objects(class(classA, driverA)),
			// Reversing the order in which the devices are listed causes the "large" device to
			// be allocated for the "small" request, leaving the "large" request unsatisfied.
			// The initial decision needs to be undone before a solution is found.
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device2, map[resourceapi.QualifiedName]resource.Quantity{
					"memory": resource.MustParse("2Gi"),
				}, nil),
				device(device1, map[resourceapi.QualifiedName]resource.Quantity{
					"memory": resource.MustParse("1Gi"),
				}, nil),
			)),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req1, driverA, pool1, device2, false),
			)},
		},
		// "small-and-large-backtrack-claims": 与上例类似但两个请求分属不同声明，验证跨声明的回溯能得到正确分配。
		"small-and-large-backtrack-claims": {
			claimsToAllocate: objects(
				claimWithRequests(
					claim0,
					nil,
					request(req0, classA, 1, resourceapi.DeviceSelector{
						CEL: &resourceapi.CELDeviceSelector{
							Expression: fmt.Sprintf(`device.capacity["%s"].memory.compareTo(quantity("1Gi")) >= 0`, driverA),
						}})),
				claimWithRequests(
					claim1,
					nil,
					request(req1, classA, 1, resourceapi.DeviceSelector{
						CEL: &resourceapi.CELDeviceSelector{
							Expression: fmt.Sprintf(`device.capacity["%s"].memory.compareTo(quantity("2Gi")) >= 0`, driverA),
						}}),
				)),
			classes: objects(class(classA, driverA)),
			// Reversing the order in which the devices are listed causes the "large" device to
			// be allocated for the "small" request, leaving the "large" request unsatisfied.
			// The initial decision needs to be undone before a solution is found.
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device2, map[resourceapi.QualifiedName]resource.Quantity{
					"memory": resource.MustParse("2Gi"),
				}, nil),
				device(device1, map[resourceapi.QualifiedName]resource.Quantity{
					"memory": resource.MustParse("1Gi"),
				}, nil),
			)),
			node: node(node1, region1),

			expectResults: []any{
				allocationResult(localNodeSelector(node1), deviceAllocationResult(req0, driverA, pool1, device1, false)),
				allocationResult(localNodeSelector(node1), deviceAllocationResult(req1, driverA, pool1, device2, false)),
			},
		},
		// "devices-split-across-different-slices": 一个请求要两台设备且分别位于 pool1 与 pool2 的不同切片，验证可跨切片/跨池分配。
		"devices-split-across-different-slices": {
			claimsToAllocate: objects(claimWithRequests(claim0, nil, resourceapi.DeviceRequest{
				Name: req0,
				Exactly: &resourceapi.ExactDeviceRequest{
					Count:           2,
					AllocationMode:  resourceapi.DeviceAllocationModeExactCount,
					DeviceClassName: classA,
				},
			})),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithOneDevice(slice1, node1, pool1, driverA),
				sliceWithOneDevice(slice2, node1, pool2, driverA),
			),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req0, driverA, pool2, device1, false),
			)},
		},
		// "obsolete-slice": 同池中存在 Generation 更新的切片使旧切片过期，验证过期切片被忽略、分配结果为空。
		"obsolete-slice": {
			claimsToAllocate: objects(claimWithRequest(claim0, req0, classA)),
			classes:          objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices("slice-1-obsolete", node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, nil),
				),
				func() wrapResourceSliceWithDevices {
					slice := sliceWithDevices("slice-1-obsolete", node1, resourcePool(pool1, 2), driverA,
						device(device2, nil, nil),
					)
					// This makes the other slice obsolete.
					slice.Spec.Pool.Generation++
					return slice
				}(),
			),
			node: node(node1, region1),

			expectResults: []any{},
		},
		// "duplicate-slice": 模拟切片控制器因缓存过期创建了名称不同但内容重复的切片，验证报错 invalid resource pools were encountered。
		"duplicate-slice": {
			claimsToAllocate: objects(claimWithRequest(claim0, req0, classA)),
			classes:          objects(class(classA, driverA)),
			slices: func() []*resourceapi.ResourceSlice {
				// This simulates the problem that can
				// (theoretically) occur when the resource
				// slice controller wants to publish a pool
				// with two slices but ends up creating some
				// identical slices under different names
				// because its informer cache was out-dated on
				// another sync (see
				// resourceslicecontroller.go).
				sliceA := sliceWithOneDevice(slice1, node1, pool1, driverA).obj()
				sliceA.Spec.Pool.ResourceSliceCount = 2
				sliceB := sliceA.DeepCopy()
				sliceB.Name += "-2"
				return []*resourceapi.ResourceSlice{sliceA, sliceB}
			}(),
			node: node(node1, region1),

			expectError: gomega.MatchError(gomega.ContainSubstring("invalid resource pools were encountered")),
		},
		// "no-slices": 无切片输入，验证无错误且分配结果为空。
		"no-slices": {
			claimsToAllocate: objects(claimWithRequest(claim0, req0, classA)),
			classes:          objects(class(classA, driverA)),
			slices:           nil,
			node:             node(node1, region1),

			expectResults: nil,
		},
		// "not-enough-suitable-devices": 只有一台设备但有两个声明都要分配它，验证无结果（设备不足）。
		"not-enough-suitable-devices": {
			claimsToAllocate: objects(claimWithRequest(claim0, req0, classA), claimWithRequest(claim0, req1, classA)),
			classes:          objects(class(classA, driverA)),
			slices:           unwrapResourceSlices(sliceWithOneDevice(slice1, node1, pool1, driverA)),

			node: node(node1, region1),

			expectResults: nil,
		},
		// "no-classes": 无 DeviceClass，验证报错 could not retrieve device class class-a。
		"no-classes": {
			claimsToAllocate: objects(claimWithRequest(claim0, req0, classA)),
			classes:          nil,
			slices:           unwrapResourceSlices(sliceWithOneDevice(slice1, node1, pool1, driverA)),
			node:             node(node1, region1),

			expectResults: nil,
			expectError:   gomega.MatchError(gomega.ContainSubstring("could not retrieve device class class-a")),
		},
		// "unknown-class": 请求引用未知类名，验证报错 could not retrieve device class unknown-class。
		"unknown-class": {
			claimsToAllocate: objects(claimWithRequest(claim0, req0, "unknown-class")),
			classes:          objects(class(classA, driverA)),
			slices:           unwrapResourceSlices(sliceWithOneDevice(slice1, node1, pool1, driverA)),
			node:             node(node1, region1),

			expectResults: nil,
			expectError:   gomega.MatchError(gomega.ContainSubstring("could not retrieve device class unknown-class")),
		},
		// "empty-class": 请求的设备类名为空，验证报错 missing device class name (unsupported request type?)。
		"empty-class": {
			claimsToAllocate: objects(claimWithRequest(claim0, req0, "")),
			classes:          objects(class(classA, driverA)),
			slices:           unwrapResourceSlices(sliceWithOneDevice(slice1, node1, pool1, driverA)),
			node:             node(node1, region1),

			expectResults: nil,
			expectError:   gomega.MatchError(gomega.ContainSubstring("claim claim-0, request req-0: missing device class name (unsupported request type?)")),
		},
		// "no-claims-to-allocate": 无待分配声明，验证无结果且无错误。
		"no-claims-to-allocate": {
			claimsToAllocate: nil,
			classes:          objects(class(classA, driverA)),
			slices:           unwrapResourceSlices(sliceWithOneDevice(slice1, node1, pool1, driverA)),
			node:             node(node1, region1),

			expectResults: nil,
		},
		// "all-devices-single": All 模式请求单台设备，验证分配到该设备。
		"all-devices-single": {
			claimsToAllocate: objects(claimWithRequests(claim0, nil, resourceapi.DeviceRequest{
				Name: req0,
				Exactly: &resourceapi.ExactDeviceRequest{
					AllocationMode:  resourceapi.DeviceAllocationModeAll,
					DeviceClassName: classA,
				},
			})),
			classes: objects(class(classA, driverA)),
			slices:  unwrapResourceSlices(sliceWithOneDevice(slice1, node1, pool1, driverA)),
			node:    node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
			)},
		},
		// "all-devices-with-consumed-counters": All 模式请求且设备有共享计数器消耗（开启 PartitionableDevices），验证仍正确分配该设备。
		"all-devices-with-consumed-counters": {
			features: Features{
				PartitionableDevices: true,
			},
			claimsToAllocate: objects(claimWithRequests(claim0, nil, resourceapi.DeviceRequest{
				Name: req0,
				Exactly: &resourceapi.ExactDeviceRequest{
					AllocationMode:  resourceapi.DeviceAllocationModeAll,
					DeviceClassName: classA,
				},
			})),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							},
						),
					),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1,
						map[string]resource.Quantity{
							"memory": resource.MustParse("8Gi"),
						},
					),
				),
			),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
			)},
		},
		// "all-devices-many": All 模式请求跨两个池的两台设备，验证两台均被分配。
		"all-devices-many": {
			claimsToAllocate: objects(claimWithRequests(claim0, nil, resourceapi.DeviceRequest{
				Name: req0,
				Exactly: &resourceapi.ExactDeviceRequest{
					AllocationMode:  resourceapi.DeviceAllocationModeAll,
					DeviceClassName: classA,
				},
			})),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithOneDevice(slice1, node1, pool1, driverA),
				sliceWithOneDevice(slice1, node1, pool2, driverA),
			),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req0, driverA, pool2, device1, false),
			)},
		},
		// "all-devices-of-the-incomplete-pool": All 模式请求的池切片数声明大于实际（池不完整），验证报错 pool is currently being updated。
		"all-devices-of-the-incomplete-pool": {
			claimsToAllocate: objects(claimWithRequests(claim0, nil, resourceapi.DeviceRequest{
				Name: req0,
				Exactly: &resourceapi.ExactDeviceRequest{
					AllocationMode:  resourceapi.DeviceAllocationModeAll,
					Count:           1,
					DeviceClassName: classA,
				},
			})),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				func() wrapResourceSliceWithDevices {
					slice := sliceWithOneDevice(slice1, node1, pool1, driverA)
					// This makes the pool incomplete, one other slice is missing.
					slice.Spec.Pool.ResourceSliceCount++
					return slice
				}(),
			),
			node: node(node1, region1),

			expectResults: nil,
			expectError:   gomega.MatchError(gomega.ContainSubstring("claim claim-0, request req-0: asks for all devices, but resource pool driver-a/pool-1 is currently being updated")),
		},
		// "all-devices-plus-another": 一个 All 模式声明加一个不同驱动的普通声明，验证两者分配互不影响。
		"all-devices-plus-another": {
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil, resourceapi.DeviceRequest{
					Name: req0,
					Exactly: &resourceapi.ExactDeviceRequest{
						AllocationMode:  resourceapi.DeviceAllocationModeAll,
						DeviceClassName: classA,
					},
				}),
				claimWithRequests(claim1, nil, resourceapi.DeviceRequest{
					Name: req0,
					Exactly: &resourceapi.ExactDeviceRequest{
						AllocationMode:  resourceapi.DeviceAllocationModeExactCount,
						Count:           1,
						DeviceClassName: classB,
					},
				}),
			),
			classes: objects(
				class(classA, driverA),
				class(classB, driverB),
			),
			slices: unwrapResourceSlices(
				sliceWithOneDevice(slice1, node1, pool1, driverA),
				sliceWithOneDevice(slice1, node1, pool1, driverB),
			),
			node: node(node1, region1),

			expectResults: []any{
				allocationResult(
					localNodeSelector(node1),
					deviceAllocationResult(req0, driverA, pool1, device1, false),
				),
				allocationResult(
					localNodeSelector(node1),
					deviceAllocationResult(req0, driverB, pool1, device1, false),
				),
			},
		},
		// "all-devices-plus-another-reversed": 上例的声明顺序颠倒，验证结果顺序随之对应。
		"all-devices-plus-another-reversed": {
			claimsToAllocate: objects(
				claimWithRequests(claim1, nil, resourceapi.DeviceRequest{
					Name: req0,
					Exactly: &resourceapi.ExactDeviceRequest{
						AllocationMode:  resourceapi.DeviceAllocationModeExactCount,
						Count:           1,
						DeviceClassName: classB,
					},
				}),
				claimWithRequests(claim0, nil, resourceapi.DeviceRequest{
					Name: req0,
					Exactly: &resourceapi.ExactDeviceRequest{
						AllocationMode:  resourceapi.DeviceAllocationModeAll,
						DeviceClassName: classA,
					},
				}),
			),
			classes: objects(
				class(classA, driverA),
				class(classB, driverB),
			),
			slices: unwrapResourceSlices(
				sliceWithOneDevice(slice1, node1, pool1, driverA),
				sliceWithOneDevice(slice1, node1, pool1, driverB),
			),
			node: node(node1, region1),

			expectResults: []any{
				allocationResult(
					localNodeSelector(node1),
					deviceAllocationResult(req0, driverB, pool1, device1, false),
				),
				allocationResult(
					localNodeSelector(node1),
					deviceAllocationResult(req0, driverA, pool1, device1, false),
				),
			},
		},
		// "all-devices-many-plus-another": All 模式要两台设备再加一个普通声明，验证三台设备均正确分配。
		"all-devices-many-plus-another": {
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil, resourceapi.DeviceRequest{
					Name: req0,
					Exactly: &resourceapi.ExactDeviceRequest{
						AllocationMode:  resourceapi.DeviceAllocationModeAll,
						DeviceClassName: classA,
					},
				}),
				claimWithRequests(claim1, nil, resourceapi.DeviceRequest{
					Name: req0,
					Exactly: &resourceapi.ExactDeviceRequest{
						AllocationMode:  resourceapi.DeviceAllocationModeExactCount,
						Count:           1,
						DeviceClassName: classB,
					},
				}),
			),
			classes: objects(
				class(classA, driverA),
				class(classB, driverB),
			),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil),
					device(device2, nil, nil),
				),
				sliceWithOneDevice(slice1, node1, pool1, driverB),
			),
			node: node(node1, region1),

			expectResults: []any{
				allocationResult(
					localNodeSelector(node1),
					deviceAllocationResult(req0, driverA, pool1, device1, false),
					deviceAllocationResult(req0, driverA, pool1, device2, false),
				),
				allocationResult(
					localNodeSelector(node1),
					deviceAllocationResult(req0, driverB, pool1, device1, false),
				),
			},
		},
		// "all-devices-many-plus-another-reversed": 上例声明顺序颠倒，验证结果顺序相应变化。
		"all-devices-many-plus-another-reversed": {
			claimsToAllocate: objects(
				claimWithRequests(claim1, nil, resourceapi.DeviceRequest{
					Name: req0,
					Exactly: &resourceapi.ExactDeviceRequest{
						AllocationMode:  resourceapi.DeviceAllocationModeExactCount,
						Count:           1,
						DeviceClassName: classB,
					},
				}),
				claimWithRequests(claim0, nil, resourceapi.DeviceRequest{
					Name: req0,
					Exactly: &resourceapi.ExactDeviceRequest{
						AllocationMode:  resourceapi.DeviceAllocationModeAll,
						DeviceClassName: classA,
					},
				}),
			),
			classes: objects(
				class(classA, driverA),
				class(classB, driverB),
			),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil),
					device(device2, nil, nil),
				),
				sliceWithOneDevice(slice1, node1, pool1, driverB),
			),
			node: node(node1, region1),

			expectResults: []any{
				allocationResult(
					localNodeSelector(node1),
					deviceAllocationResult(req0, driverB, pool1, device1, false),
				),
				allocationResult(
					localNodeSelector(node1),
					deviceAllocationResult(req0, driverA, pool1, device1, false),
					deviceAllocationResult(req0, driverA, pool1, device2, false),
				),
			},
		},
		// "all-devices-no-solution": 一台设备被两个声明（一个普通、一个 All 模式）争用，验证无解。
		"all-devices-no-solution": {
			// One device, two claims both trying to allocate it.
			claimsToAllocate: objects(
				claimWithRequests(claim1, nil, resourceapi.DeviceRequest{
					Name: req0,
					Exactly: &resourceapi.ExactDeviceRequest{
						AllocationMode:  resourceapi.DeviceAllocationModeExactCount,
						Count:           1,
						DeviceClassName: classA,
					},
				}),
				claimWithRequests(claim0, nil, resourceapi.DeviceRequest{
					Name: req0,
					Exactly: &resourceapi.ExactDeviceRequest{
						AllocationMode:  resourceapi.DeviceAllocationModeAll,
						DeviceClassName: classA,
					},
				}),
			),
			classes: objects(
				class(classA, driverA),
			),
			slices: unwrapResourceSlices(
				sliceWithOneDevice(slice1, node1, pool1, driverA),
			),
			node: node(node1, region1),
		},
		// "all-devices-no-solution-reversed": 上例声明顺序颠倒，验证仍无解。
		"all-devices-no-solution-reversed": {
			// One device, two claims both trying to allocate it.
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil, resourceapi.DeviceRequest{
					Name: req0,
					Exactly: &resourceapi.ExactDeviceRequest{
						AllocationMode:  resourceapi.DeviceAllocationModeAll,
						DeviceClassName: classA,
					},
				}),
				claimWithRequests(claim1, nil, resourceapi.DeviceRequest{
					Name: req0,
					Exactly: &resourceapi.ExactDeviceRequest{
						AllocationMode:  resourceapi.DeviceAllocationModeExactCount,
						Count:           1,
						DeviceClassName: classA,
					},
				}),
			),
			classes: objects(
				class(classA, driverA),
			),
			slices: unwrapResourceSlices(
				sliceWithOneDevice(slice1, node1, pool1, driverA),
			),
			node: node(node1, region1),
		},
		// "all-devices-slice-without-devices": All 模式请求的切片不含设备，验证无结果。
		"all-devices-slice-without-devices": {
			claimsToAllocate: objects(claimWithRequests(claim0, nil, resourceapi.DeviceRequest{
				Name: req0,
				Exactly: &resourceapi.ExactDeviceRequest{
					AllocationMode:  resourceapi.DeviceAllocationModeAll,
					DeviceClassName: classA,
				},
			})),
			classes:       objects(class(classA, driverA)),
			slices:        unwrapResourceSlices(sliceWithNoDevices(slice1, node1, pool1, driverA)),
			node:          node(node1, region1),
			expectResults: nil,
		},
		// "all-devices-no-slices": All 模式请求但无切片，验证无结果。
		"all-devices-no-slices": {
			claimsToAllocate: objects(claimWithRequests(claim0, nil, resourceapi.DeviceRequest{
				Name: req0,
				Exactly: &resourceapi.ExactDeviceRequest{
					AllocationMode:  resourceapi.DeviceAllocationModeAll,
					DeviceClassName: classA,
				},
			})),
			classes:       objects(class(classA, driverA)),
			slices:        nil,
			node:          node(node1, region1),
			expectResults: nil,
		},
		// "all-devices-some-allocated": All 模式请求且池中一台设备已被分配，验证分配失败（All 要求全部可用）。
		"all-devices-some-allocated": {
			claimsToAllocate: objects(claimWithRequests(claim0, nil, resourceapi.DeviceRequest{
				Name: req0,
				Exactly: &resourceapi.ExactDeviceRequest{
					AllocationMode:  resourceapi.DeviceAllocationModeAll,
					DeviceClassName: classA,
				},
			})),
			allocatedDevices: []DeviceID{
				MakeDeviceID(driverA, pool1, device1),
			},
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA, device(device1, nil, nil), device(device2, nil, nil)),
			),
			node:          node(node1, region1),
			expectResults: nil,
		},
		// "all-devices-some-allocated-admin-access": 开启 AdminAccess 时，All 模式请求且一台设备已分配，验证两台设备仍以管理员访问模式分配。
		"all-devices-some-allocated-admin-access": {
			features: Features{
				AdminAccess: true,
			},
			claimsToAllocate: func() []wrapResourceClaim {
				c := claimWithRequest(claim0, req0, classA)
				c.Spec.Devices.Requests[0].Exactly.AdminAccess = ptr.To(true)
				c.Spec.Devices.Requests[0].Exactly.AllocationMode = resourceapi.DeviceAllocationModeAll
				return []wrapResourceClaim{c}
			}(),
			allocatedDevices: []DeviceID{
				MakeDeviceID(driverA, pool1, device1),
			},
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA, device(device1, nil, nil), device(device2, nil, nil)),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, true),
				deviceAllocationResult(req0, driverA, pool1, device2, true),
			)},
		},
		// "all-devices-some-allocating-admin-access": 开启 AdminAccess 时，同一声明内第一个请求已占用设备使第二个 All 模式请求无法满足，验证无结果。
		"all-devices-some-allocating-admin-access": {
			features: Features{
				AdminAccess: true,
			},
			claimsToAllocate: func() []wrapResourceClaim {
				c := claimWithRequests(claim0, nil, request(req0, classA, 1), request(req1, classA, 1))
				c.Spec.Devices.Requests[0].Exactly.AdminAccess = ptr.To(true)
				// Second request for `All` cannot be fulfilled because
				// the first request claims a device.
				c.Spec.Devices.Requests[1].Exactly.AdminAccess = ptr.To(true)
				c.Spec.Devices.Requests[1].Exactly.AllocationMode = resourceapi.DeviceAllocationModeAll
				return []wrapResourceClaim{c}
			}(),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA, device(device1, nil, nil), device(device2, nil, nil)),
			),
			node:          node(node1, region1),
			expectResults: nil,
		},
		// "count-devices-some-allocated-admin-access": 开启 AdminAccess 时，普通计数请求要两台设备且一台已分配，验证两台设备均以管理员访问模式分配。
		"count-devices-some-allocated-admin-access": {
			features: Features{
				AdminAccess: true,
			},
			claimsToAllocate: func() []wrapResourceClaim {
				c := claimWithRequest(claim0, req0, classA)
				c.Spec.Devices.Requests[0].Exactly.AdminAccess = ptr.To(true)
				c.Spec.Devices.Requests[0].Exactly.AllocationMode = resourceapi.DeviceAllocationModeExactCount
				c.Spec.Devices.Requests[0].Exactly.Count = 2
				return []wrapResourceClaim{c}
			}(),
			allocatedDevices: []DeviceID{
				MakeDeviceID(driverA, pool1, device1),
			},
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA, device(device1, nil, nil), device(device2, nil, nil)),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, true),
				deviceAllocationResult(req0, driverA, pool1, device2, true),
			)},
		},
		// "separate-claims-share-device-admin-access": 开启 AdminAccess 时，两个独立声明各自请求管理员访问同一设备，验证两者共享该设备且均为管理员访问。
		"separate-claims-share-device-admin-access": {
			features: Features{
				AdminAccess: true,
			},
			claimsToAllocate: func() []wrapResourceClaim {
				c1 := claimWithRequest(claim0, req0, classA)
				c1.Spec.Devices.Requests[0].Exactly.AdminAccess = ptr.To(true)

				c2 := claimWithRequest(claim1, req0, classA)
				c2.Spec.Devices.Requests[0].Exactly.AdminAccess = ptr.To(true)
				return []wrapResourceClaim{c1, c2}
			}(),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA, device(device1, nil, nil), device(device2, nil, nil)),
			),
			node: node(node1, region1),
			expectResults: []any{
				allocationResult(
					localNodeSelector(node1),
					deviceAllocationResult(req0, driverA, pool1, device1, true),
				),
				allocationResult(
					localNodeSelector(node1),
					deviceAllocationResult(req0, driverA, pool1, device1, true),
				),
			},
		},
		// "admin-access-claims-device-allocating-for-non-admin-access": 普通声明与管理员声明在同一节点上分配设备，验证各自访问模式正确（普通在前、管理员在后）。
		"admin-access-claims-device-allocating-for-non-admin-access": {
			features: Features{
				AdminAccess: true,
			},
			claimsToAllocate: func() []wrapResourceClaim {
				admin := claimWithRequest(claim1, req0, classA)
				admin.Spec.Devices.Requests[0].Exactly.AdminAccess = ptr.To(true)
				return []wrapResourceClaim{claimWithRequest(claim0, req0, classA), admin}
			}(),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA, device(device1, nil, nil), device(device2, nil, nil)),
			),
			node: node(node1, region1),
			expectResults: []any{
				allocationResult(
					localNodeSelector(node1),
					deviceAllocationResult(req0, driverA, pool1, device1, false),
				),
				allocationResult(
					localNodeSelector(node1),
					deviceAllocationResult(req0, driverA, pool1, device1, true),
				),
			},
		},
		// "all-devices-slice-without-devices-prioritized-list": 优先级列表场景：首选子请求为 All 模式但切片无设备，回退到次选子请求并分配其设备。
		"all-devices-slice-without-devices-prioritized-list": {
			features: Features{
				PrioritizedList: true,
			},
			claimsToAllocate: objects(
				func() wrapResourceClaim {
					claim := claim(claim0).withRequests(
						requestWithPrioritizedList(req0,
							subRequest(subReq0, classA, 1),
							subRequest(subReq1, classB, 1),
						),
					)
					claim.Spec.Devices.Requests[0].FirstAvailable[0].AllocationMode = resourceapi.DeviceAllocationModeAll
					claim.Spec.Devices.Requests[0].FirstAvailable[0].Count = 0
					return claim
				}(),
			),
			classes: objects(class(classA, driverA), class(classB, driverB)),
			slices: unwrapResourceSlices(
				sliceWithNoDevices(slice1, node1, pool1, driverA),
				sliceWithOneDevice(slice2, node1, pool2, driverB),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0SubReq1, driverB, pool2, device1, false),
			)},
		},
		// "all-devices-no-slices-prioritized-list": 优先级列表场景：首选子请求为 All 模式但无切片，回退到次选子请求并分配其设备。
		"all-devices-no-slices-prioritized-list": {
			features: Features{
				PrioritizedList: true,
			},
			claimsToAllocate: objects(
				func() wrapResourceClaim {
					claim := claim(claim0).withRequests(
						requestWithPrioritizedList(req0,
							subRequest(subReq0, classA, 1),
							subRequest(subReq1, classB, 1),
						),
					)
					claim.Spec.Devices.Requests[0].FirstAvailable[0].AllocationMode = resourceapi.DeviceAllocationModeAll
					claim.Spec.Devices.Requests[0].FirstAvailable[0].Count = 0
					return claim
				}(),
			),
			classes: objects(class(classA, driverA), class(classB, driverB)),
			slices: unwrapResourceSlices(
				sliceWithOneDevice(slice2, node1, pool2, driverB),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0SubReq1, driverB, pool2, device1, false),
			)},
		},
		// "all-devices-some-allocated-prioritized-list": 优先级列表场景：首选子请求为 All 模式且部分设备已分配，回退到次选子请求并分配其设备。
		"all-devices-some-allocated-prioritized-list": {
			features: Features{
				PrioritizedList: true,
			},
			claimsToAllocate: objects(
				func() wrapResourceClaim {
					claim := claim(claim0).withRequests(
						requestWithPrioritizedList(req0,
							subRequest(subReq0, classA, 1),
							subRequest(subReq1, classB, 1),
						),
					)
					claim.Spec.Devices.Requests[0].FirstAvailable[0].AllocationMode = resourceapi.DeviceAllocationModeAll
					claim.Spec.Devices.Requests[0].FirstAvailable[0].Count = 0
					return claim
				}(),
			),
			allocatedDevices: []DeviceID{
				MakeDeviceID(driverA, pool1, device1),
			},
			classes: objects(class(classA, driverA), class(classB, driverB)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA, device(device1, nil, nil), device(device2, nil, nil)),
				sliceWithOneDevice(slice2, node1, pool2, driverB),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0SubReq1, driverB, pool2, device1, false),
			)},
		},
		// "network-attached-device": 网络附加设备（切片按标签选择器对区域内所有节点可见），验证结果携带标签选择器而非本地节点选择器。
		"network-attached-device": {
			claimsToAllocate: objects(claimWithRequest(claim0, req0, classA)),
			classes:          objects(class(classA, driverA)),
			slices:           unwrapResourceSlices(sliceWithOneDevice(slice1, nodeLabelSelector(regionKey, region1), pool1, driverA)),
			node:             node(node1, region1),

			expectResults: []any{allocationResult(
				nodeLabelSelector(regionKey, region1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
			)},
		},
		// "unsuccessful-allocation-network-attached-device": 网络附加设备选择器指向 region-1，但目标节点在 region-2，验证无可用设备、分配失败。
		"unsuccessful-allocation-network-attached-device": {
			claimsToAllocate: objects(claimWithRequest(claim0, req0, classA)),
			classes:          objects(class(classA, driverA)),
			slices:           unwrapResourceSlices(sliceWithOneDevice(slice1, nodeLabelSelector(regionKey, region1), pool1, driverA)),
			// Wrong region, no devices available.
			node: node(node2, region2),

			expectResults: nil,
		},
		// "many-network-attached-devices": All 模式要四台来自不同选择器的网络附加设备，验证结果的选择器为各选择器的并集。
		"many-network-attached-devices": {
			claimsToAllocate: objects(claimWithRequests(claim0, nil, request(req0, classA, 4))),
			classes:          objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithOneDevice(slice1, nodeLabelSelector(regionKey, region1), pool1, driverA),
				sliceWithOneDevice(slice1, nodeSelectionAll, pool2, driverA),
				sliceWithOneDevice(slice1, nodeLabelSelector(planetKey, planetValueEarth), pool3, driverA),
				sliceWithOneDevice(slice1, localNodeSelector(node1), pool4, driverA),
			),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				// A union of the individual selectors.
				&v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{{
						MatchExpressions: []v1.NodeSelectorRequirement{
							{Key: regionKey, Operator: v1.NodeSelectorOpIn, Values: []string{region1}},
							{Key: planetKey, Operator: v1.NodeSelectorOpIn, Values: []string{planetValueEarth}},
						},
						MatchFields: []v1.NodeSelectorRequirement{
							{Key: fieldNameKey, Operator: v1.NodeSelectorOpIn, Values: []string{node1}},
						},
					}},
				},
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req0, driverA, pool2, device1, false),
				deviceAllocationResult(req0, driverA, pool3, device1, false),
				deviceAllocationResult(req0, driverA, pool4, device1, false),
			)},
		},
		// "local-and-network-attached-devices": 同时分配网络附加设备与本地设备，验证一旦存在本地设备则节点选择器收敛为该本地节点。
		"local-and-network-attached-devices": {
			claimsToAllocate: objects(claimWithRequests(claim0, nil, request(req0, classA, 2))),
			classes:          objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithOneDevice(slice1, nodeLabelSelector(regionKey, region1), pool1, driverA),
				sliceWithOneDevice(slice1, node1, pool2, driverA),
			),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				// Once there is any node-local device, the selector is for that node.
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req0, driverA, pool2, device1, false),
			)},
		},
		// "several-different-drivers": 两个声明分别来自 driverA 与 driverB，验证各自分配到对应驱动的设备。
		"several-different-drivers": {
			claimsToAllocate: objects(claimWithRequest(claim0, req0, classA), claimWithRequest(claim0, req0, classB)),
			classes:          objects(class(classA, driverA), class(classB, driverB)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil),
					device(device2, nil, nil),
				),
				sliceWithOneDevice(slice1, node1, pool1, driverB),
			),
			node: node(node1, region1),

			expectResults: []any{
				allocationResult(localNodeSelector(node1), deviceAllocationResult(req0, driverA, pool1, device1, false)),
				allocationResult(localNodeSelector(node1), deviceAllocationResult(req0, driverB, pool1, device1, false)),
			},
		},
		// "already-allocated-devices": 待分配设备已被分配，验证无结果。
		"already-allocated-devices": {
			claimsToAllocate: objects(claimWithRequest(claim0, req0, classA)),
			allocatedDevices: []DeviceID{
				MakeDeviceID(driverA, pool1, device1),
				MakeDeviceID(driverA, pool1, device2),
			},
			classes: objects(class(classA, driverA)),
			slices:  unwrapResourceSlices(sliceWithOneDevice(slice1, node1, pool1, driverA)),
			node:    node(node1, region1),

			expectResults: nil,
		},
		// "admin-access-disabled": 请求管理员访问但特性关闭，验证报错 admin access is requested, but the feature is disabled。
		"admin-access-disabled": {
			features: Features{
				AdminAccess: false,
			},
			claimsToAllocate: func() []wrapResourceClaim {
				c := claimWithRequest(claim0, req0, classA)
				c.Spec.Devices.Requests[0].Exactly.AdminAccess = ptr.To(true)
				return []wrapResourceClaim{c}
			}(),
			classes: objects(class(classA, driverA)),
			slices:  unwrapResourceSlices(sliceWithOneDevice(slice1, node1, pool1, driverA)),
			node:    node(node1, region1),

			expectResults: nil,
			expectError:   gomega.MatchError(gomega.ContainSubstring("claim claim-0, request req-0: admin access is requested, but the feature is disabled")),
		},
		// "admin-access-enabled": 管理员访问特性开启，验证即使设备已分配，All 模式请求仍以管理员访问模式分配到设备。
		"admin-access-enabled": {
			features: Features{
				AdminAccess: true,
			},
			claimsToAllocate: func() []wrapResourceClaim {
				c := claimWithRequest(claim0, req0, classA)
				c.Spec.Devices.Requests[0].Exactly.AdminAccess = ptr.To(true)
				return []wrapResourceClaim{c}
			}(),
			allocatedDevices: []DeviceID{
				MakeDeviceID(driverA, pool1, device1),
				MakeDeviceID(driverA, pool1, device2),
			},
			classes: objects(class(classA, driverA)),
			slices:  unwrapResourceSlices(sliceWithOneDevice(slice1, node1, pool1, driverA)),
			node:    node(node1, region1),

			expectResults: []any{
				allocationResult(localNodeSelector(node1), deviceAllocationResult(req0, driverA, pool1, device1, true)),
			},
		},
		// "with-constraint": 单声明含两个请求并附带 int/version/string/bool 四类属性匹配约束，两台设备属性一致，验证两请求分别分配到两台设备。
		"with-constraint": {
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{
					{MatchAttribute: &intAttribute},
					{MatchAttribute: &versionAttribute},
					{MatchAttribute: &stringAttribute},
					{MatchAttribute: &boolAttribute},
				},
				request(req0, classA, 1),
				request(req1, classA, 1),
			),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"driverVersion":   {VersionValue: ptr.To("1.0.0")},
					"numa":            {IntValue: ptr.To(int64(1))},
					"stringAttribute": {StringValue: ptr.To("stringAttributeValue")},
					"boolAttribute":   {BoolValue: ptr.To(true)},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"driverVersion":   {VersionValue: ptr.To("1.0.0")},
					"numa":            {IntValue: ptr.To(int64(1))},
					"stringAttribute": {StringValue: ptr.To("stringAttributeValue")},
					"boolAttribute":   {BoolValue: ptr.To(true)},
				}),
			)),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req1, driverA, pool1, device2, false),
			)},
		},
		// "with-constraint-non-existent-attribute": 约束引用不存在的属性，验证无结果（无法满足约束）。
		"with-constraint-non-existent-attribute": {
			claimsToAllocate: objects(claimWithRequest(claim0, req0, classA, resourceapi.DeviceConstraint{
				MatchAttribute: &nonExistentAttribute,
			})),
			classes: objects(class(classA, driverA)),
			slices:  unwrapResourceSlices(sliceWithOneDevice(slice1, node1, pool1, driverA)),
			node:    node(node1, region1),

			expectResults: nil,
		},
		// "with-constraint-not-matching-int-attribute": 请求两台设备但按 int 属性约束，两台设备 numa 值不同，验证无结果。
		"with-constraint-not-matching-int-attribute": {
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{MatchAttribute: &intAttribute}},
				request(req0, classA, 2)),
			),
			classes: objects(class(classA, driverA), class(classB, driverB)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"numa": {IntValue: ptr.To(int64(1))},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"numa": {IntValue: ptr.To(int64(2))},
				}),
			)),
			node: node(node1, region1),

			expectResults: nil,
		},
		// "with-constraint-not-matching-int-attribute-all-devices": All 模式请求加 int 属性约束且设备属性不匹配，验证报错 a claim constraint would not be satisfied。
		"with-constraint-not-matching-int-attribute-all-devices": {
			claimsToAllocate: objects(
				func() wrapResourceClaim {
					claim := claimWithRequests(
						claim0,
						[]resourceapi.DeviceConstraint{{MatchAttribute: &intAttribute}},
						request(req0, classA, 0),
					)
					claim.Spec.Devices.Requests[0].Exactly.AllocationMode = resourceapi.DeviceAllocationModeAll
					return claim
				}(),
			),
			classes: objects(class(classA, driverA), class(classB, driverB)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"numa": {IntValue: ptr.To(int64(1))},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"numa": {IntValue: ptr.To(int64(2))},
				}),
			)),
			node: node(node1, region1),

			expectError: gomega.MatchError(gomega.ContainSubstring("claim claim-0, request req-0: cannot add device driver-a/pool-1/device-2 because a claim constraint would not be satisfied")),
		},
		// "partitionable-all-mode-constraint-must-error-with-reserved-counter": 覆盖 allocateDevice 的 must 错误路径：设备先预留共享计数器再因约束失败，验证回滚分支被触发且返回约束不满足错误。
		"partitionable-all-mode-constraint-must-error-with-reserved-counter": {
			// Covers allocateDevice's must=true error path when the rejected device
			// has already reserved a shared counter. An all-mode request over two
			// devices with mismatched constraint attributes fails on the second
			// device, which consumes the counter, so rollbackDevice runs with a
			// reserved counter before the must-error is returned. The error aborts the
			// whole allocation, so this asserts the error contract and exercises the
			// counter-release branch; the released counter is not separately
			// observable through the allocation result.
			features: Features{
				PartitionableDevices: true,
			},
			claimsToAllocate: objects(
				func() wrapResourceClaim {
					claim := claimWithRequests(
						claim0,
						[]resourceapi.DeviceConstraint{{MatchAttribute: &intAttribute}},
						request(req0, classA, 0),
					)
					claim.Spec.Devices.Requests[0].Exactly.AllocationMode = resourceapi.DeviceAllocationModeAll
					return claim
				}(),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"numa": {IntValue: new(int64(1))},
					}),
					// device2 mismatches the constraint and consumes the single
					// counter, so it reserves the counter first and then fails the
					// constraint, reaching the must-error path with state to undo.
					device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"numa": {IntValue: new(int64(2))},
					}).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1, map[string]resource.Quantity{"c": resource.MustParse("1")}),
					),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1, map[string]resource.Quantity{"c": resource.MustParse("1")}),
				),
			),
			node:        node(node1, region1),
			expectError: gomega.MatchError(gomega.ContainSubstring("claim claim-0, request req-0: cannot add device driver-a/pool-1/device-2 because a claim constraint would not be satisfied")),
		},
		// "with-constraint-not-matching-string-attribute": 按 string 属性约束且两台设备取值不同，验证无结果。
		"with-constraint-not-matching-string-attribute": {
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{MatchAttribute: &stringAttribute}},
				request(req0, classA, 2)),
			),
			classes: objects(class(classA, driverA), class(classB, driverB)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValue: ptr.To("stringAttributeValue")},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValue: ptr.To("stringAttributeValue2")},
				}),
			)),
			node: node(node1, region1),

			expectResults: nil,
		},
		// "with-constraint-not-matching-bool-attribute": 按 bool 属性约束且两台设备取值不同，验证无结果。
		"with-constraint-not-matching-bool-attribute": {
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{MatchAttribute: &boolAttribute}},
				request(req0, classA, 2)),
			),
			classes: objects(class(classA, driverA), class(classB, driverB)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"boolAttribute": {BoolValue: ptr.To(true)},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"boolAttribute": {BoolValue: ptr.To(false)},
				}),
			)),
			node: node(node1, region1),

			expectResults: nil,
		},
		// "with-constraint-not-matching-version-attribute": 按版本属性约束且两台设备版本不同，验证无结果。
		"with-constraint-not-matching-version-attribute": {
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{MatchAttribute: &versionAttribute}},
				request(req0, classA, 2)),
			),
			classes: objects(class(classA, driverA), class(classB, driverB)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"driverVersion": {VersionValue: ptr.To("1.0.0")},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"driverVersion": {VersionValue: ptr.To("2.0.0")},
				}),
			)),
			node: node(node1, region1),

			expectResults: nil,
		},
		// 本用例验证约束只作用于被点名的请求：约束要求 req0 与同请求内设备具有相同的 driverVersion。
		// 构造条件：claim 含 req0、req1 两个请求并带一条 Requests=[req0] 的 MatchAttribute 约束；池中 device1 为 1.0.0、device2 为 2.0.0。
		// 期望结果：req0 分配到 device1，req1 不受约束限制分配到 device2，均落在 node1；分配成功。
		"with-constraint-for-request": {
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{
					{
						// 约束仅作用于 req0。
						Requests:       []string{req0},
						MatchAttribute: &versionAttribute,
					},
				},
				request(req0, classA, 1),
				request(req1, classA, 1),
			)),
			classes: objects(class(classA, driverA), class(classB, driverB)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"driverVersion": {VersionValue: ptr.To("1.0.0")},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"driverVersion": {VersionValue: ptr.To("2.0.0")},
				}),
			)),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req1, driverA, pool1, device2, false),
			)},
		},
		// 本用例验证约束冲突时分配器的回溯能力：先尝试 req0 的 driverVersion 约束，再检查全局 stringAttribute 约束。
		// 构造条件：三条设备分别为 (1.0.0,a)、(2.0.0,b)、(3.0.0,b)；全局约束要求所有设备 stringAttribute 相同，首条 a 不满足而必须被回溯。
		// 期望结果：req0 分配到 device2(2.0.0,b)，req1 分配到 device3(3.0.0,b)；两个请求由 driverVersion 约束保持一致且满足全局属性约束。
		"with-constraint-for-request-retry": {
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{
					{
						Requests:       []string{req0},
						MatchAttribute: &versionAttribute,
					},
					{
						MatchAttribute: &stringAttribute,
					},
				},
				request(req0, classA, 1),
				request(req1, classA, 1),
			)),
			classes: objects(class(classA, driverA), class(classB, driverB)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				// This device does not satisfy the second
				// match attribute, so the allocator must
				// backtrack.
				// device1 的 stringAttribute 为 a，不满足随后的全局约束，必须回溯跳过。
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"driverVersion":   {VersionValue: ptr.To("1.0.0")},
					"stringAttribute": {StringValue: ptr.To("a")},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"driverVersion":   {VersionValue: ptr.To("2.0.0")},
					"stringAttribute": {StringValue: ptr.To("b")},
				}),
				device(device3, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"driverVersion":   {VersionValue: ptr.To("3.0.0")},
					"stringAttribute": {StringValue: ptr.To("b")},
				}),
			)),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device2, false),
				deviceAllocationResult(req1, driverA, pool1, device3, false),
			)},
		},
		// 本用例验证 DeviceClass 上的配置能够传播到分配结果。
		// 构造条件：classA 带 classAttribute 配置，claim 仅有 req0 一个请求，池中只有一台设备。
		// 期望结果：分配成功后结果携带 Source=Class、Requests=nil（作用于全部请求）的 classAttribute 配置。
		"with-class-device-config": {
			claimsToAllocate: objects(claimWithRequest(claim0, req0, classA)),
			classes:          objects(classWithConfig(classA, driverA, "classAttribute")),
			slices:           unwrapResourceSlices(sliceWithOneDevice(slice1, node1, pool1, driverA)),
			node:             node(node1, region1),

			expectResults: []any{
				allocationResultWithConfigs(
					localNodeSelector(node1),
					objects(deviceAllocationResult(req0, driverA, pool1, device1, false)),
					[]resourceapi.DeviceAllocationConfiguration{
						{
							// 配置来源为 DeviceClass，Requests 为 nil 表示作用于全部请求。
							Source:              resourceapi.AllocationConfigSourceClass,
							Requests:            nil,
							DeviceConfiguration: deviceConfiguration(driverA, "classAttribute"),
						},
					},
				),
			},
		},
		// 本用例验证多请求、多设备类场景下类配置按请求分组传播。
		// 构造条件：req0、req1 使用 classA（配置 classAttribute-A），req2 使用 classB（配置 classAttribute-B）；driverA 池有两台设备、driverB 池有一台。
		// 期望结果：req0/req1 分别分配到 driverA 的 device0、device1 并共享 classAttribute-A 配置（Requests=[req0,req1]）；req2 分配到 driverB 的 device1 并带 classAttribute-B 配置（Requests=[req2]）。
		"with-class-device-config-with-multiple-request-and-configs": {
			claimsToAllocate: objects(claimWithRequests(claim0, []resourceapi.DeviceConstraint{}, request(req0, classA, 1), request(req1, classA, 1), request(req2, classB, 1))),
			classes: objects(
				// classA 的配置最终作用于 req0 与 req1。
				classWithConfig(classA, driverA, "classAttribute-A"),
				// classB 的配置仅作用于 req2。
				classWithConfig(classB, driverB, "classAttribute-B"),
			),
			slices: unwrap(
				sliceWithMultipleDevices(slice1, node1, pool1, driverA, 2),
				sliceWithOneDevice(slice2, node1, pool1, driverB),
			),
			node: node(node1, region1),

			expectResults: []any{
				allocationResultWithConfigs(
					localNodeSelector(node1),
					objects(
						deviceAllocationResult(req0, driverA, pool1, device0, false),
						deviceAllocationResult(req1, driverA, pool1, device1, false),
						deviceAllocationResult(req2, driverB, pool1, device1, false),
					),
					[]resourceapi.DeviceAllocationConfiguration{
						{
							Source:              resourceapi.AllocationConfigSourceClass,
							Requests:            []string{req0, req1},
							DeviceConfiguration: deviceConfiguration(driverA, "classAttribute-A"),
						},
						{
							Source:              resourceapi.AllocationConfigSourceClass,
							Requests:            []string{req2},
							DeviceConfiguration: deviceConfiguration(driverB, "classAttribute-B"),
						},
					},
				),
			},
		},
		// 本用例验证 ResourceClaim 上的配置可以只覆盖部分请求。
		// 构造条件：claim 含 req0、req1、req2 三个请求，配置只点名 req0 与 req1；池中提供三台设备。
		// 期望结果：三个请求各分配一台设备，结果中仅有一条 Source=Claim、Requests=[req0,req1] 的 deviceAttribute 配置。
		"with-claim-config-for-two-requests": {
			claimsToAllocate: func() []wrapResourceClaim {
				c := claimWithRequests(claim0, nil, request(req0, classA, 1), request(req1, classA, 1), request(req2, classA, 1))
				c.Spec.Devices.Config = []resourceapi.DeviceClaimConfiguration{
					{
						// 配置仅作用于 req0 和 req1，req2 不带该配置。
						Requests:            []string{req0, req1},
						DeviceConfiguration: deviceConfiguration(driverA, "deviceAttribute"),
					},
				}
				return []wrapResourceClaim{c}
			}(),
			classes: objects(
				class(classA, driverA),
			),
			slices: unwrap(
				sliceWithMultipleDevices(slice1, node1, pool1, driverA, 3),
			),
			node: node(node1, region1),

			expectResults: []any{
				allocationResultWithConfigs(
					localNodeSelector(node1),
					objects(
						deviceAllocationResult(req0, driverA, pool1, device0, false),
						deviceAllocationResult(req1, driverA, pool1, device1, false),
						deviceAllocationResult(req2, driverA, pool1, device2, false),
					),
					[]resourceapi.DeviceAllocationConfiguration{
						{
							Source:              resourceapi.AllocationConfigSourceClaim,
							Requests:            []string{req0, req1},
							DeviceConfiguration: deviceConfiguration(driverA, "deviceAttribute"),
						},
					},
				),
			},
		},
		// 本用例验证最简单的 claim 级设备配置场景。
		// 构造条件：claim 带一个作用于 req0 的 deviceAttribute 配置，单请求单设备。
		// 期望结果：分配成功后结果携带 Source=Claim 的 deviceAttribute 配置。
		"claim-with-device-config": {
			// 声明携带一个作用于 req0 的设备级配置。
			claimsToAllocate: objects(claimWithDeviceConfig(claim0, req0, classA, driverA, "deviceAttribute")),
			classes:          objects(class(classA, driverA)),
			slices:           unwrapResourceSlices(sliceWithOneDevice(slice1, node1, pool1, driverA)),
			node:             node(node1, region1),

			expectResults: []any{
				allocationResultWithConfig(
					localNodeSelector(node1),
					driverA,
					resourceapi.AllocationConfigSourceClaim,
					"deviceAttribute",
					deviceAllocationResult(req0, driverA, pool1, device1, false),
				),
			},
		},
		// 本用例验证遇到未知（未来扩展）选择器类型时的错误路径。
		// 构造条件：把 Exactly.Selectors 设为一个空的 DeviceSelector，模拟尚不支持的选择器。
		// 期望错误：包含 CEL expression empty (unsupported selector type?)。
		"unknown-selector": {
			claimsToAllocate: objects(
				func() wrapResourceClaim {
					claim := claimWithRequest(claim0, req0, classA)
					claim.Spec.Devices.Requests[0].Exactly.Selectors = []resourceapi.DeviceSelector{
						// 空选择器模拟未来新增、当前尚不支持的选择器类型。
						{ /* empty = unknown future selector */ },
					}
					return claim
				}(),
			),
			classes: objects(class(classA, driverA)),
			slices:  unwrapResourceSlices(sliceWithOneDevice(slice1, node1, pool1, driverA)),
			node:    node(node1, region1),

			expectError: gomega.MatchError(gomega.ContainSubstring("CEL expression empty (unsupported selector type?)")),
		},
		// 本用例验证未知分配模式（count mode）被明确拒绝。
		// 构造条件：把请求的 AllocationMode 设为自定义字符串 future-mode。
		// 期望错误：包含 unsupported count mode future-mode。
		"unknown-allocation-mode": {
			claimsToAllocate: objects(
				func() wrapResourceClaim {
					claim := claimWithRequest(claim0, req0, classA)
					// 设置一个当前不支持的 count mode。
					claim.Spec.Devices.Requests[0].Exactly.AllocationMode = resourceapi.DeviceAllocationMode("future-mode")
					return claim
				}(),
			),
			classes: objects(class(classA, driverA)),
			slices:  unwrapResourceSlices(sliceWithOneDevice(slice1, node1, pool1, driverA)),
			node:    node(node1, region1),

			expectError: gomega.MatchError(gomega.ContainSubstring("unsupported count mode future-mode")),
		},
		// 本用例验证遇到未知（未来扩展）约束类型时的错误路径。
		// 构造条件：Constraints 中放入一个空的 DeviceConstraint。
		// 期望错误：包含 empty constraint (unsupported constraint type?)。
		"unknown-constraint": {
			claimsToAllocate: objects(
				func() wrapResourceClaim {
					claim := claimWithRequest(claim0, req0, classA)
					claim.Spec.Devices.Constraints = []resourceapi.DeviceConstraint{
						// 空约束模拟未来新增、当前尚不支持的约束类型。
						{ /* empty = unknown */ },
					}
					return claim
				}(),
			),
			classes: objects(class(classA, driverA)),
			slices:  unwrapResourceSlices(sliceWithOneDevice(slice1, node1, pool1, driverA)),
			node:    node(node1, region1),

			expectError: gomega.MatchError(gomega.ContainSubstring("empty constraint (unsupported constraint type?)")),
		},
		// 本用例验证单设备请求中非法 CEL 表达式在编译阶段即被拒绝。
		// 构造条件：选择器表达式引用未声明变量 noSuchVar。
		// 期望错误：包含 undeclared reference。
		"invalid-CEL-one-device": {
			claimsToAllocate: objects(
				func() wrapResourceClaim {
					claim := claimWithRequest(claim0, req0, classA)
					claim.Spec.Devices.Requests[0].Exactly.Selectors = []resourceapi.DeviceSelector{
						// 引用未声明变量，CEL 编译应当失败。
						{CEL: &resourceapi.CELDeviceSelector{Expression: "noSuchVar"}},
					}
					return claim
				}(),
			),
			classes: objects(class(classA, driverA)),
			slices:  unwrapResourceSlices(sliceWithOneDevice(slice1, node1, pool1, driverA)),
			node:    node(node1, region1),

			expectError: gomega.MatchError(gomega.ContainSubstring("undeclared reference")),
		},
		// 本用例验证 DeviceClass 选择器中的非法 CEL 同样会报错。
		// 构造条件：把 classA 的 CEL 表达式改写为 noSuchVar。
		// 期望错误：包含 undeclared reference。
		"invalid-CEL-one-device-class": {
			claimsToAllocate: objects(claimWithRequest(claim0, req0, classA)),
			classes: objects(
				func() *resourceapi.DeviceClass {
					c := class(classA, driverA)
					// 把类的选择器改为非法表达式。
					c.Spec.Selectors[0].CEL.Expression = "noSuchVar"
					return c
				}(),
			),
			slices: unwrapResourceSlices(sliceWithOneDevice(slice1, node1, pool1, driverA)),
			node:   node(node1, region1),

			expectError: gomega.MatchError(gomega.ContainSubstring("undeclared reference")),
		},
		// 本用例验证 All 分配模式下也会校验 CEL 表达式。
		// 构造条件：请求使用 AllocationModeAll，同时选择器表达式引用 noSuchVar。
		// 期望错误：包含 undeclared reference。
		"invalid-CEL-all-devices": {
			claimsToAllocate: objects(
				func() wrapResourceClaim {
					claim := claimWithRequest(claim0, req0, classA)
					claim.Spec.Devices.Requests[0].Exactly.Selectors = []resourceapi.DeviceSelector{
						{CEL: &resourceapi.CELDeviceSelector{Expression: "noSuchVar"}},
					}
					// All 模式下同样需要先完成 CEL 校验。
					claim.Spec.Devices.Requests[0].Exactly.AllocationMode = resourceapi.DeviceAllocationModeAll
					return claim
				}(),
			),
			classes: objects(class(classA, driverA)),
			slices:  unwrapResourceSlices(sliceWithOneDevice(slice1, node1, pool1, driverA)),
			node:    node(node1, region1),

			expectError: gomega.MatchError(gomega.ContainSubstring("undeclared reference")),
		},
		// 本用例验证单个请求的设备数量超过声明上限时被拒绝。
		// 构造条件：req0 请求 500 台设备，超过 AllocationResultsMaxSize。
		// 期望错误：包含 exceeds the claim limit。
		"too-many-devices-single-request": {
			// 500 超过单个 ResourceClaim 的结果数量上限。
			claimsToAllocate: objects(claimWithRequests(claim0, nil, request(req0, classA, 500))),
			classes:          objects(class(classA, driverA)),

			expectError: gomega.MatchError(gomega.ContainSubstring("exceeds the claim limit")),
		},
		// 本用例验证请求数量恰好等于上限时应当被接受。
		// 构造条件：req0 的 count 恰为 AllocationResultsMaxSize。
		// 期望结果：未设置 expectError，即分配成功且不报错。
		"many-devices-okay": {
			// 恰好等于上限，不应触发超限检查。
			claimsToAllocate: objects(claimWithRequests(claim0, nil, request(req0, classA, resourceapi.AllocationResultsMaxSize))),
			classes:          objects(class(classA, driverA)),
		},
		// 本用例验证多个请求合计数量超过上限时被拒绝。
		// 构造条件：req0 取上限值、req1 再取 1 台，池中设备数量也足够。
		// 期望错误：包含 exceeds the claim limit。
		"too-many-devices-total": {
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil,
					request(req0, classA, resourceapi.AllocationResultsMaxSize),
					// 与 req0 相加后超过声明上限。
					request(req1, classA, 1),
				),
			),
			classes: objects(class(classA, driverA)),
			slices:  unwrapResourceSlices(sliceWithMultipleDevices(slice1, node1, pool1, driverA, resourceapi.AllocationResultsMaxSize+1)),
			node:    node(node1, region1),

			expectError: gomega.MatchError(gomega.ContainSubstring("exceeds the claim limit")),
		},
		// Two counts that are each representable but together overflow the native
		// int accumulator to a negative value, rejected before the sum is formed.
		// The value comes from math.MaxInt so it also triggers on 32-bit builds.
		// 本用例验证 sum 检查在整数溢出前就拦截请求。
		// 构造条件：两个请求各取 math.MaxInt/2+1，相加会使原生 int 累加器溢出为负数；取值来自 math.MaxInt 以兼容 32 位平台。
		// 期望错误：包含 exceeds the claim limit（在形成总和之前即被拒绝）。
		"exact-count-total-overflows-claim-limit": {
			claimsToAllocate: objects(claim(claim0).withRequests(
				deviceRequest(req0, classA, int64(math.MaxInt/2+1)),
				deviceRequest(req1, classA, int64(math.MaxInt/2+1)),
			)),
			classes:     objects(class(classA, driverA)),
			expectError: gomega.MatchError(gomega.ContainSubstring("exceeds the claim limit")),
		},
		// Four counts that would wrap a naive int accumulator back to zero rather
		// than negative; each is rejected against remaining capacity before summing.
		// 本用例验证能够绕回零的溢出同样被拦截。
		// 构造条件：四个请求各取 math.MaxInt/2+1，朴素 int 累加会绕回 0 而非负数；每个请求在求和前先与剩余容量比较。
		// 期望错误：包含 exceeds the claim limit。
		"exact-count-total-would-wrap-to-zero": {
			claimsToAllocate: objects(claim(claim0).withRequests(
				deviceRequest(req0, classA, int64(math.MaxInt/2+1)),
				deviceRequest(req1, classA, int64(math.MaxInt/2+1)),
				deviceRequest(req2, classA, int64(math.MaxInt/2+1)),
				deviceRequest(req3, classA, int64(math.MaxInt/2+1)),
			)),
			classes:     objects(class(classA, driverA)),
			expectError: gomega.MatchError(gomega.ContainSubstring("exceeds the claim limit")),
		},
		// req0 fits on its own, so the rejection has to name req1 as the one
		// that pushes the claim over.
		// 本用例验证报错信息能定位到导致超限的具体请求。
		// 构造条件：req0=20 单独可行，req1=20 使其超出上限。
		// 期望错误：同时包含 request req-1 与 exceeds the claim limit。
		"exact-count-sum-exceeds-claim-limit": {
			claimsToAllocate: objects(claim(claim0).withRequests(
				deviceRequest(req0, classA, 20),
				deviceRequest(req1, classA, 20),
			)),
			classes: objects(class(classA, driverA)),
			expectError: gomega.MatchError(gomega.And(
				gomega.ContainSubstring("request "+req1),
				gomega.ContainSubstring("exceeds the claim limit"),
			)),
		},
		// Two counts whose sum exactly fills the per-claim limit both allocate;
		// this pins the pre-flight comparison as > and not >=.
		// 本用例验证上限比较使用严格大于（>）而非大于等于（>=）。
		// 构造条件：两个请求各取上限的一半，池中设备数恰为上限；总和正好填满上限。
		// 期望结果：分配成功，两段结果分别包含上限一半数量的设备。
		"exact-count-sum-exactly-fills-claim-limit": {
			claimsToAllocate: objects(claim(claim0).withRequests(
				deviceRequest(req0, classA, resourceapi.AllocationResultsMaxSize/2),
				deviceRequest(req1, classA, resourceapi.AllocationResultsMaxSize/2),
			)),
			classes: objects(class(classA, driverA)),
			slices:  unwrapResourceSlices(sliceWithMultipleDevices(slice1, node1, pool1, driverA, resourceapi.AllocationResultsMaxSize)),
			node:    node(node1, region1),
			// 总和恰好等于上限时仍应成功分配。
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				slices.Concat(
					multipleDeviceAllocationResults(req0, driverA, pool1, resourceapi.AllocationResultsMaxSize/2, 0),
					multipleDeviceAllocationResults(req1, driverA, pool1, resourceapi.AllocationResultsMaxSize/2, resourceapi.AllocationResultsMaxSize/2),
				)...,
			)},
		},
		// 本用例名称具有误导性：实际构造的仍是超限请求，而非非法 CEL。
		// 构造条件：req0 请求 500 台设备，且未提供任何切片。
		// 期望错误：包含 exceeds the claim limit，与 too-many-devices-single-request 一致。
		"all-devices-invalid-CEL": {
			// 实际是数量超限场景，并非名称所示的非法 CEL。
			claimsToAllocate: objects(claimWithRequests(claim0, nil, request(req0, classA, 500))),
			classes:          objects(class(classA, driverA)),

			expectError: gomega.MatchError(gomega.ContainSubstring("exceeds the claim limit")),
		},
		// 本用例验证优先列表的首选子请求不可用时回退到后续子请求。
		// 构造条件：启用 PrioritizedList；subReq0 使用 classB 但池中只有 driverA 的一台设备，subReq1 使用 classA 且有一台设备。
		// 期望结果：回退到 subReq1，分配 driverA 池的 device1。
		"prioritized-list-first-unavailable": {
			features: Features{
				// 启用优先列表特性。
				PrioritizedList: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					requestWithPrioritizedList(req0,
						// 首选子请求在池中无可用设备。
						subRequest(subReq0, classB, 1),
						subRequest(subReq1, classA, 1),
					))),
			classes: objects(class(classA, driverA), class(classB, driverB)),
			slices:  unwrapResourceSlices(sliceWithOneDevice(slice1, node1, pool1, driverA)),
			node:    node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0SubReq1, driverA, pool1, device1, false),
			)},
		},
		// 本用例验证所有备选子请求都无法满足时分配失败。
		// 构造条件：两个子请求各需 2 台设备，但 classA 与 classB 各只有 1 台。
		// 期望结果：expectResults 为 nil，即没有任何可用分配。
		"prioritized-list-non-available": {
			features: Features{
				PrioritizedList: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					requestWithPrioritizedList(req0,
						subRequest(subReq0, classB, 2),
						subRequest(subReq1, classA, 2),
					))),
			classes: objects(class(classA, driverA), class(classB, driverB)),
			slices: unwrapResourceSlices(
				sliceWithOneDevice(slice1, node1, pool1, driverA),
				sliceWithOneDevice(slice2, node1, pool2, driverB),
			),
			node: node(node1, region1),

			// 所有备选数量均不足，分配失败。
			expectResults: nil,
		},
		// 本用例验证优先列表场景下 claim 配置能作用于被选中的子请求。
		// 构造条件：两个子请求分别配置 foo、bar；只有 classB 的 subReq1 有两台设备，classA 无设备。
		// 期望结果：分配 subReq1 的两台设备，结果带 Source=Claim、Requests=nil 的 bar 配置（覆盖全部已分配请求后折叠为 nil）。
		"prioritized-list-device-config": {
			features: Features{
				PrioritizedList: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					requestWithPrioritizedList(req0,
						subRequest(subReq0, classA, 1),
						subRequest(subReq1, classB, 2),
					),
				).withConfigs(
					objects(
						deviceClaimConfig([]string{req0SubReq0}, deviceConfiguration(driverA, "foo")),
						deviceClaimConfig([]string{req0SubReq1}, deviceConfiguration(driverB, "bar")),
					),
				),
			),
			classes: objects(class(classA, driverA), class(classB, driverB)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverB,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{}),
			)),
			node: node(node1, region1),

			expectResults: []any{allocationResultWithConfigs(
				localNodeSelector(node1),
				objects(
					deviceAllocationResult(req0SubReq1, driverB, pool1, device1, false),
					deviceAllocationResult(req0SubReq1, driverB, pool1, device2, false),
				),
				[]resourceapi.DeviceAllocationConfiguration{
					{
						// 配置覆盖全部已分配请求，Requests 折叠为 nil。
						Source:              resourceapi.AllocationConfigSourceClaim,
						Requests:            nil,
						DeviceConfiguration: deviceConfiguration(driverB, "bar"),
					},
				},
			)},
		},
		// 本用例验证类配置在优先列表回退后正确传播。
		// 构造条件：classA、classB 各带配置；subReq0 需 2 台但 classA 只有 1 台，subReq1 需 2 台且 classB 有 2 台。
		// 期望结果：分配 classB 的 device1、device2，结果带 classAttribute=bar 的 Class 配置且 Requests=nil。
		"prioritized-list-class-config": {
			features: Features{
				PrioritizedList: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					requestWithPrioritizedList(req0,
						subRequest(subReq0, classA, 2),
						subRequest(subReq1, classB, 2),
					))),
			classes: objects(
				// classA 的候选数量不足，最终不会被选中。
				classWithConfig(classA, driverA, "foo"),
				// 最终选中的是 classB，其配置随结果返回。
				classWithConfig(classB, driverB, "bar"),
			),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverB,
					device(device1, nil, nil),
					device(device2, nil, nil),
				),
				sliceWithDevices(slice2, node1, pool2, driverA,
					device(device3, nil, nil),
				),
			),
			node: node(node1, region1),

			expectResults: []any{allocationResultWithConfigs(
				localNodeSelector(node1),
				objects(
					deviceAllocationResult(req0SubReq1, driverB, pool1, device1, false),
					deviceAllocationResult(req0SubReq1, driverB, pool1, device2, false),
				),
				[]resourceapi.DeviceAllocationConfiguration{
					{
						Source:              resourceapi.AllocationConfigSourceClass,
						Requests:            nil,
						DeviceConfiguration: deviceConfiguration(driverB, "bar"),
					},
				},
			)},
		},
		// 本用例验证 claim 配置限定于某个请求的子请求时，不会被折叠为全局配置。
		// 构造条件：配置点名 req0 的两个子请求，另有普通请求 req1；池中两台设备分别分配给 subReq0 与 req1。
		// 期望结果：结果中配置的 Requests 保持为 [req0SubReq0, req0SubReq1]，未被清空。
		"claim-config-scoped-to-subrequests-of-one-request": {
			features: Features{
				PrioritizedList: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					requestWithPrioritizedList(req0,
						subRequest(subReq0, classA, 1),
						subRequest(subReq1, classB, 1),
					),
					deviceRequest(req1, classA, 1),
				).withConfigs(
					objects(
						deviceClaimConfig([]string{req0SubReq0, req0SubReq1}, deviceConfiguration(driverA, "scoped")),
					),
				),
			),
			classes: objects(class(classA, driverA), class(classB, driverB)),
			slices: unwrap(
				sliceWithMultipleDevices(slice1, node1, pool1, driverA, 2),
			),
			node: node(node1, region1),

			expectResults: []any{allocationResultWithConfigs(
				localNodeSelector(node1),
				objects(
					deviceAllocationResult(req0SubReq0, driverA, pool1, device0, false),
					deviceAllocationResult(req1, driverA, pool1, device1, false),
				),
				[]resourceapi.DeviceAllocationConfiguration{
					{
						Source:              resourceapi.AllocationConfigSourceClaim,
						Requests:            []string{req0SubReq0, req0SubReq1},
						DeviceConfiguration: deviceConfiguration(driverA, "scoped"),
					},
				},
			)},
		},
		// 本用例验证配置覆盖全部顶层请求时 Requests 被清空。
		// 构造条件：配置点名 req0 的一个子请求与 req1，等价于覆盖所有顶层请求。
		// 期望结果：分配 subReq0→device0、req1→device1，结果中配置的 Requests 被折叠为 nil。
		"claim-config-covering-all-requests-is-cleared": {
			features: Features{
				PrioritizedList: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					requestWithPrioritizedList(req0,
						subRequest(subReq0, classA, 1),
						subRequest(subReq1, classB, 1),
					),
					deviceRequest(req1, classA, 1),
				).withConfigs(
					objects(
						// Covers req0 (via its selected subrequest) and req1 (by name),
						// i.e. every top-level request, so Requests must collapse to nil.
						// 覆盖 req0（经被选子请求）与 req1，即所有顶层请求。
						deviceClaimConfig([]string{req0SubReq0, req1}, deviceConfiguration(driverA, "all")),
					),
				),
			),
			classes: objects(class(classA, driverA), class(classB, driverB)),
			slices: unwrap(
				sliceWithMultipleDevices(slice1, node1, pool1, driverA, 2),
			),
			node: node(node1, region1),

			expectResults: []any{allocationResultWithConfigs(
				localNodeSelector(node1),
				objects(
					deviceAllocationResult(req0SubReq0, driverA, pool1, device0, false),
					deviceAllocationResult(req1, driverA, pool1, device1, false),
				),
				[]resourceapi.DeviceAllocationConfiguration{
					{
						Source:              resourceapi.AllocationConfigSourceClaim,
						Requests:            nil,
						DeviceConfiguration: deviceConfiguration(driverA, "all"),
					},
				},
			)},
		},
		// 本用例验证子请求上的 CEL 容量表达式参与候选过滤。
		// 构造条件：req0 要求 memory>=1Gi；req1 的两个子请求分别要求 >=4Gi（subReq0）与 >=2Gi（subReq1）；池中设备为 2Gi、2Gi、1Gi。
		// 期望结果：req0 分配到 1Gi 的 device3；subReq0 因无 >=4Gi 设备失败，回退 subReq1 分配 2Gi 的 device1、device2。
		"prioritized-list-subrequests-with-expressions": {
			features: Features{
				PrioritizedList: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					deviceRequest(req0, classA, 1).withSelectors(
						resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.capacity["%s"].memory.compareTo(quantity("1Gi")) >= 0`, driverA),
							},
						},
					),
					requestWithPrioritizedList(req1,
						subRequest(subReq0, classA, 1, resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.capacity["%s"].memory.compareTo(quantity("4Gi")) >= 0`, driverA),
							}}),
						subRequest(subReq1, classA, 2, resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.capacity["%s"].memory.compareTo(quantity("2Gi")) >= 0`, driverA),
							}}),
					),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, map[resourceapi.QualifiedName]resource.Quantity{
					"memory": resource.MustParse("2Gi"),
				}, nil),
				device(device2, map[resourceapi.QualifiedName]resource.Quantity{
					"memory": resource.MustParse("2Gi"),
				}, nil),
				device(device3, map[resourceapi.QualifiedName]resource.Quantity{
					"memory": resource.MustParse("1Gi"),
				}, nil),
			)),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device3, false),
				deviceAllocationResult(req1SubReq1, driverA, pool1, device1, false),
				deviceAllocationResult(req1SubReq1, driverA, pool1, device2, false),
			)},
		},
		// 本用例验证约束引用父请求时，子请求的选择也必须满足该约束。
		// 构造条件：约束要求 req0 与 req1 的 driverVersion 相同；req0 需 >=8Gi（仅 pool1 的 device1，版本 1.0.0）；subReq0 需 >=2Gi（device2，版本 2.0.0），subReq1 需 >=1Gi（device3，版本 1.0.0）。
		// 期望结果：subReq0 因版本不匹配被拒，回退 subReq1；req0→pool1/device1，req1SubReq1→pool2/device3。
		"prioritized-list-subrequests-with-constraints-ref-parent-request": {
			features: Features{
				PrioritizedList: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withConstraints(
					resourceapi.DeviceConstraint{
						Requests:       []string{req0, req1},
						MatchAttribute: &versionAttribute,
					},
				).withRequests(
					deviceRequest(req0, classA, 1).withSelectors(resourceapi.DeviceSelector{
						CEL: &resourceapi.CELDeviceSelector{
							Expression: fmt.Sprintf(`device.capacity["%s"].memory.compareTo(quantity("8Gi")) >= 0`, driverA),
						}},
					),
					requestWithPrioritizedList(req1,
						subRequest(subReq0, classA, 1, resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.capacity["%s"].memory.compareTo(quantity("2Gi")) >= 0`, driverA),
							}},
						),
						subRequest(subReq1, classA, 1, resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.capacity["%s"].memory.compareTo(quantity("1Gi")) >= 0`, driverA),
							}},
						),
					),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1,
						map[resourceapi.QualifiedName]resource.Quantity{
							"memory": resource.MustParse("8Gi"),
						},
						map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
							"driverVersion": {VersionValue: ptr.To("1.0.0")},
						},
					),
				),
				sliceWithDevices(slice2, node1, pool2, driverA,
					device(device2,
						map[resourceapi.QualifiedName]resource.Quantity{
							"memory": resource.MustParse("2Gi"),
						},
						map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
							"driverVersion": {VersionValue: ptr.To("2.0.0")},
						},
					),
					device(device3,
						map[resourceapi.QualifiedName]resource.Quantity{
							"memory": resource.MustParse("1Gi"),
						},
						map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
							"driverVersion": {VersionValue: ptr.To("1.0.0")},
						},
					),
				),
			),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req1SubReq1, driverA, pool2, device3, false),
			)},
		},
		// 本用例验证约束引用某个子请求时，只有该子请求被选中才会施加约束。
		// 构造条件：约束点名 req0 与 req1SubReq0；req0 取 8Gi/版本 1.0.0 的 device1；subReq0 取 2Gi/版本 2.0.0 的 device2（版本不匹配），subReq1 取同设备但不受该约束。
		// 期望结果：subReq0 被约束拒绝，回退 subReq1；req0→pool1/device1，req1SubReq1→pool2/device2。
		"prioritized-list-subrequests-with-constraints-ref-sub-request": {
			features: Features{
				PrioritizedList: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withConstraints(
					resourceapi.DeviceConstraint{
						Requests:       []string{req0, req1SubReq0},
						MatchAttribute: &versionAttribute,
					},
				).withRequests(
					deviceRequest(req0, classA, 1).withSelectors(resourceapi.DeviceSelector{
						CEL: &resourceapi.CELDeviceSelector{
							Expression: fmt.Sprintf(`device.capacity["%s"].memory.compareTo(quantity("8Gi")) >= 0`, driverA),
						}},
					),
					requestWithPrioritizedList(req1,
						subRequest(subReq0, classA, 1, resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.capacity["%s"].memory.compareTo(quantity("2Gi")) >= 0`, driverA),
							}},
						),
						subRequest(subReq1, classA, 1, resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.capacity["%s"].memory.compareTo(quantity("1Gi")) >= 0`, driverA),
							}},
						),
					),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1,
						map[resourceapi.QualifiedName]resource.Quantity{
							"memory": resource.MustParse("8Gi"),
						},
						map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
							"driverVersion": {VersionValue: ptr.To("1.0.0")},
						},
					),
				),
				sliceWithDevices(slice2, node1, pool2, driverA,
					device(device2,
						map[resourceapi.QualifiedName]resource.Quantity{
							"memory": resource.MustParse("2Gi"),
						},
						map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
							"driverVersion": {VersionValue: ptr.To("2.0.0")},
						},
					),
				),
			),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req1SubReq1, driverA, pool2, device2, false),
			)},
		},
		// 本用例验证 All 模式子请求在设备已被占用时的处理。
		// 构造条件：subReq0 改为 AllocationModeAll 且 count=0；池中 device1、device2，其中 device1 已被其他声明占用。
		// 期望结果：subReq0 无法满足，回退到 count=1 的 subReq1，分配空闲的 device2。
		"prioritized-list-subrequests-with-allocation-mode-all": {
			features: Features{
				PrioritizedList: true,
			},
			claimsToAllocate: objects(
				func() wrapResourceClaim {
					claim := claim(claim0).withRequests(
						requestWithPrioritizedList(req0,
							subRequest(subReq0, classA, 1),
							subRequest(subReq1, classA, 1),
						),
					)
					// 把首个子请求改为 All 模式。
					claim.Spec.Devices.Requests[0].FirstAvailable[0].AllocationMode = resourceapi.DeviceAllocationModeAll
					claim.Spec.Devices.Requests[0].FirstAvailable[0].Count = 0
					return claim
				}(),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil),
					device(device2, nil, nil),
				),
			),
			allocatedDevices: []DeviceID{
				MakeDeviceID(driverA, pool1, device1),
			},
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0SubReq1, driverA, pool1, device2, false),
			)},
		},
		// 本用例验证多请求场景下 All 模式子请求无法满足时回退到普通子请求。
		// 构造条件：req0 普通请求 1 台；req1 的 subReq0 为 All 模式、subReq1 为 count=1；池中仅两台设备。
		// 期望结果：req0 分配 device1，subReq0 因需要全部设备而不可用，subReq1 分配 device2。
		"prioritized-list-allocation-mode-all-multiple-requests": {
			features: Features{
				PrioritizedList: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					deviceRequest(req0, classA, 1),
					requestWithPrioritizedList(req1,
						subRequest(subReq0, classA, 0).withAllocationMode(resourceapi.DeviceAllocationModeAll),
						subRequest(subReq1, classA, 1),
					),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil),
					device(device2, nil, nil),
				),
			),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req1SubReq1, driverA, pool1, device2, false),
			)},
		},
		// 本用例验证特性关闭时携带子请求的声明被明确拒绝。
		// 构造条件：Features.PrioritizedList=false，但请求使用了 requestWithPrioritizedList。
		// 期望结果：expectResults 为 nil，并报错包含 has subrequests, but the DRAPrioritizedList feature is disabled。
		"prioritized-list-disabled": {
			features: Features{
				PrioritizedList: false,
			},
			claimsToAllocate: objects(
				func() wrapResourceClaim {
					claim := claim(claim0).withRequests(
						requestWithPrioritizedList(req0,
							subRequest(subReq0, classA, 1),
						),
					)
					return claim
				}(),
			),
			classes: objects(class(classA, driverA)),
			slices:  unwrapResourceSlices(sliceWithOneDevice(slice1, node1, pool1, driverA)),
			node:    node(node1, region1),

			expectResults: nil,
			expectError:   gomega.MatchError(gomega.ContainSubstring("claim claim-0, request req-0: has subrequests, but the DRAPrioritizedList feature is disabled")),
		},
		// 本用例验证优先列表与普通请求竞争同一批设备时的选择。
		// 构造条件：req1 需 >=8Gi（device1）；req0 的 subReq0 需 >=8Gi、subReq1 需 >=4Gi；池中 pool1 有 8Gi、pool2 有 4Gi。
		// 期望结果：req1 占用 device1 后 subReq0 无 8Gi 设备，回退 subReq1；req1→device1，req0SubReq1→device2。
		"prioritized-list-multi-request": {
			features: Features{
				PrioritizedList: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					deviceRequest(req1, classA, 1).withSelectors(
						resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.capacity["%s"].memory.compareTo(quantity("8Gi")) >= 0`, driverA),
							}},
					),
					requestWithPrioritizedList(req0,
						subRequest(subReq0, classA, 1, resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.capacity["%s"].memory.compareTo(quantity("8Gi")) >= 0`, driverA),
							}},
						),
						subRequest(subReq1, classA, 1, resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.capacity["%s"].memory.compareTo(quantity("4Gi")) >= 0`, driverA),
							}},
						),
					),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1,
						map[resourceapi.QualifiedName]resource.Quantity{
							"memory": resource.MustParse("8Gi"),
						},
						map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{},
					),
				),
				sliceWithDevices(slice2, node1, pool2, driverA,
					device(device2,
						map[resourceapi.QualifiedName]resource.Quantity{
							"memory": resource.MustParse("4Gi"),
						},
						map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{},
					),
				),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req1, driverA, pool1, device1, false),
				deviceAllocationResult(req0SubReq1, driverA, pool2, device2, false),
			)},
		},
		// 本用例验证优先列表选择错误时需要跨请求回溯。
		// 构造条件：req0 的 subReq0 需 >=8Gi、subReq1 需 >=4Gi；req1 需 >=8Gi；池中 pool1 有 8Gi、pool2 有 4Gi。
		// 期望结果：若先满足 subReq0 占用 8Gi 设备，req1 将无法满足；回溯后 subReq1→device2，req1→device1。
		"prioritized-list-with-backtracking": {
			features: Features{
				PrioritizedList: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					requestWithPrioritizedList(req0,
						subRequest(subReq0, classA, 1, resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.capacity["%s"].memory.compareTo(quantity("8Gi")) >= 0`, driverA),
							}},
						),
						subRequest(subReq1, classA, 1, resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.capacity["%s"].memory.compareTo(quantity("4Gi")) >= 0`, driverA),
							}},
						),
					),
					deviceRequest(req1, classA, 1).withSelectors(
						resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.capacity["%s"].memory.compareTo(quantity("8Gi")) >= 0`, driverA),
							}},
					),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1,
						map[resourceapi.QualifiedName]resource.Quantity{
							"memory": resource.MustParse("8Gi"),
						},
						map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{},
					),
				),
				sliceWithDevices(slice2, node1, pool2, driverA,
					device(device2,
						map[resourceapi.QualifiedName]resource.Quantity{
							"memory": resource.MustParse("4Gi"),
						},
						map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{},
					),
				),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0SubReq1, driverA, pool2, device2, false),
				deviceAllocationResult(req1, driverA, pool1, device1, false),
			)},
		},
		// 本用例验证数量超限的子请求会被跳过并尝试下一个备选。
		// 构造条件：subReq0 请求 500 台（超过上限），subReq1 请求 1 台；池中仅一台设备。
		// 期望结果：subReq0 被跳过，subReq1 成功分配 device1。
		"prioritized-list-too-many-in-first-subrequest": {
			features: Features{
				PrioritizedList: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(requestWithPrioritizedList(req0,
					subRequest(subReq0, classB, 500),
					subRequest(subReq1, classA, 1),
				))),
			classes: objects(class(classA, driverA), class(classB, driverB)),
			slices:  unwrapResourceSlices(sliceWithOneDevice(slice1, node1, pool1, driverA)),
			node:    node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0SubReq1, driverA, pool1, device1, false),
			)},
		},
		// A prioritized list is worth at least its smallest alternative, so the
		// pre-flight check reads that. min(20, 24) leaves the claim over the
		// limit before any device is looked at.
		// 本用例验证优先列表的预检查取最小备选数量。
		// 构造条件：req0=20；req1 的优先列表备选为 20 与 24，最小值为 20；合计 40 超过上限。
		// 期望错误：同时包含 request req-1 与 exceeds the claim limit。
		"prioritized-list-minimum-sum-exceeds-claim-limit": {
			features: Features{PrioritizedList: true},
			claimsToAllocate: objects(claim(claim0).withRequests(
				deviceRequest(req0, classA, 20),
				requestWithPrioritizedList(req1,
					subRequest(subReq0, classA, 20),
					subRequest(subReq1, classA, 24),
				),
			)),
			classes: objects(class(classA, driverA)),
			expectError: gomega.MatchError(gomega.And(
				gomega.ContainSubstring("request "+req1),
				gomega.ContainSubstring("exceeds the claim limit"),
			)),
		},
		// The oversized alternative sits behind a device already allocated for the
		// claim, which is what overflowed the per-request check. Only the pinned
		// invocation count separates pruning it from descending into it.
		// 本用例验证溢出的子请求在搜索前被剪枝而非递归展开。
		// 构造条件：req0=1；req1 的 subReq0 取 math.MaxInt（溢出），subReq1 取 1；两台可用设备。
		// 期望结果：req0→device1、req1SubReq1→device2；同时固定 AllocateOne 调用次数为 8 以验证剪枝。
		"prioritized-list-overflowing-first-subrequest-is-pruned": {
			features: Features{PrioritizedList: true},
			claimsToAllocate: objects(claim(claim0).withRequests(
				deviceRequest(req0, classA, 1),
				requestWithPrioritizedList(req1,
					subRequest(subReq0, classA, int64(math.MaxInt)),
					subRequest(subReq1, classA, 1),
				),
			)),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, nil),
				device(device2, nil, nil),
			)),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req1SubReq1, driverA, pool1, device2, false),
			)},
			// 固定调用次数，确认溢出子请求被剪枝而不是深入搜索。
			expectNumAllocateOneInvocations: 8,
		},
		// 本用例验证可分区设备的基础分配：设备消费计数器容量。
		// 构造条件：启用 PartitionableDevices；单设备消费 memory 4Gi，counterSet 总容量 8Gi。
		// 期望结果：req0 成功分配 device1。
		"partitionable-devices-single-device": {
			features: Features{
				// 启用可分区设备特性。
				PartitionableDevices: true,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil, request(req0, classA, 1)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							},
						),
					),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1,
						map[string]resource.Quantity{
							"memory": resource.MustParse("8Gi"),
						},
					),
				),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
			)},
		},
		// 本用例验证大数量计数器在分配与回退过程中不被修改。
		// 构造条件：device1 已分配且消费 c=9e18（总量 1e19），device2 消费 c=1。
		// 期望结果：req0 分配到 device2；使用极大数量确保精度与数量本身不因分配而发生改变。
		"partitionable-devices-counter-quantities-are-not-modified": {
			features: Features{
				PartitionableDevices: true,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil, request(req0, classA, 1)),
			),
			allocatedDevices: []DeviceID{
				// device1 已被占用且几乎耗尽计数，应跳过。
				MakeDeviceID(driverA, pool1, device1),
			},
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1, map[string]resource.Quantity{"c": resource.MustParse("9000000000000000000")}),
					),
					device(device2, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1, map[string]resource.Quantity{"c": resource.MustParse("1")}),
					),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1, map[string]resource.Quantity{"c": resource.MustParse("10000000000000000000")}),
				),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device2, false),
			)},
		},
		// 本用例验证计数器缓存按池 ID（含驱动）隔离，而非仅按池名。
		// 构造条件：两个驱动各有名为 pool-1 的池，但 c 的容量/消费分别为 1 与 2。
		// 期望结果：两个声明分别分配到各自池中的 device1、device2；若缓存仅以池名为键则会串用导致错误拒绝。
		"partitionable-devices-counter-cache-scoped-by-pool-ID": {
			features: Features{
				PartitionableDevices: true,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil, request(req0, classA, 1)),
				claimWithRequests(claim1, nil, request(req1, classB, 1)),
			),
			classes: objects(class(classA, driverA), class(classB, driverB)),
			slices: unwrapResourceSlices(
				// driverA / pool "pool-1": counterSet1 c:1, device1 consumes c:1.
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1, map[string]resource.Quantity{"c": resource.MustParse("1")}),
					),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1, map[string]resource.Quantity{"c": resource.MustParse("1")}),
				),
				// driverB / pool "pool-1": same pool name, different driver. Its capacity and
				// consumption differ (c:2) on purpose, so that leaving *either* cache keyed by
				// pool name alone fails this case: device2 would be checked against driverA's
				// cached c:1 and wrongly rejected.
				sliceWithDevices(slice3, node1, resourcePool(pool1, 2), driverB,
					device(device2, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1, map[string]resource.Quantity{"c": resource.MustParse("2")}),
					),
				),
				sliceWithCounterSets(slice4, node1, resourcePool(pool1, 2), driverB,
					counterSet(counterSet1, map[string]resource.Quantity{"c": resource.MustParse("2")}),
				),
			),
			node: node(node1, region1),
			// driver-a/pool-1 and driver-b/pool-1 are distinct pools with independent counter
			// sets, so both devices allocate. The differing capacities make this guard both
			// caches: keying availableCounters or consumedCounters by pool name alone lets
			// driver A's cache leak into driver B's pool and fails the case.
			expectResults: []any{
				allocationResult(localNodeSelector(node1), deviceAllocationResult(req0, driverA, pool1, device1, false)),
				allocationResult(localNodeSelector(node1), deviceAllocationResult(req1, driverB, pool1, device2, false)),
			},
		},
		// 本用例验证计数器缓存不跨驱动的 schema 串用。
		// 构造条件：两驱动同池名但使用不同 counterSet（c 与 d）；device2 消费 d=2 超过总量 d=1。
		// 期望结果：expectResults 为空切片，device2 因超消费被拒绝；若缓存按池名复用会错误接受。
		"partitionable-devices-counter-cache-not-crossed-between-driver-schemas": {
			features: Features{
				PartitionableDevices: true,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil, request(req0, classA, 1)),
				claimWithRequests(claim1, nil, request(req1, classB, 1)),
			),
			classes: objects(class(classA, driverA), class(classB, driverB)),
			slices: unwrapResourceSlices(
				// driverA / pool "pool-1": counterSet1 with c:1, device1 consumes c:1 (valid).
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1, map[string]resource.Quantity{"c": resource.MustParse("1")}),
					),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1, map[string]resource.Quantity{"c": resource.MustParse("1")}),
				),
				// driverB / pool "pool-1": same pool name but a different counter set
				// (counterSet2 with d:1). device2 over-consumes it (d:2) and must be rejected.
				sliceWithDevices(slice3, node1, resourcePool(pool1, 2), driverB,
					device(device2, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet2, map[string]resource.Quantity{"d": resource.MustParse("2")}),
					),
				),
				sliceWithCounterSets(slice4, node1, resourcePool(pool1, 2), driverB,
					counterSet(counterSet2, map[string]resource.Quantity{"d": resource.MustParse("1")}),
				),
			),
			node: node(node1, region1),
			// device2 over-consumes its own counter (d:2 > d:1), so it must be rejected and
			// the allocation cannot be satisfied. If the caches are keyed by pool name,
			// device2 reuses driver A's cached counterSet1 and its own counterSet2 is never
			// compared, so it is wrongly accepted (over-allocation).
			// 期望无任何分配结果：超消费设备必须被拒绝。
			expectResults: []any{},
		},
		// 本用例验证设备因污点被拒后其已预留的计数器被释放。
		// 构造条件：启用 PartitionableDevices 与 DeviceTaints；device1 带 NoSchedule 污点且消费 c=1，device2 干净且同样消费 c=1，总容量 c=1。
		// 期望结果：device1 预留计数后因污点被拒，释放后 device2 成功分配。
		"partitionable-devices-taint-releases-counter": {
			features: Features{
				PartitionableDevices: true,
				DeviceTaints:         true,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil, request(req0, classA, 1)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					// 带污点设备先预留计数，随后因污点被拒绝。
					device(device1, nil, nil).withTaints(taintNoSchedule).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1, map[string]resource.Quantity{"c": resource.MustParse("1")}),
					),
					device(device2, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1, map[string]resource.Quantity{"c": resource.MustParse("1")}),
					),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1, map[string]resource.Quantity{"c": resource.MustParse("1")}),
				),
			),
			node: node(node1, region1),
			// device1 is rejected by its NoSchedule taint after its counter is
			// reserved. Releasing that reservation lets the clean device2 allocate.
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device2, false),
			)},
		},
		// 本用例验证设备因属性约束被拒后其计数器被释放。
		// 构造条件：全局 MatchAttribute 约束；三台设备分别为 red、blue、blue，各消费 c=1，总容量 c=2。
		// 期望结果：red 先被选中后两台 blue 因属性不匹配被拒，释放计数后两台 blue 成功分配。
		"partitionable-devices-constraint-releases-counter": {
			features: Features{
				PartitionableDevices: true,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0,
					[]resourceapi.DeviceConstraint{{MatchAttribute: &stringAttribute}},
					request(req0, classA, 2),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"stringAttribute": {StringValue: new("red")},
					}).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1, map[string]resource.Quantity{"c": resource.MustParse("1")}),
					),
					device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"stringAttribute": {StringValue: new("blue")},
					}).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1, map[string]resource.Quantity{"c": resource.MustParse("1")}),
					),
					device(device3, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"stringAttribute": {StringValue: new("blue")},
					}).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1, map[string]resource.Quantity{"c": resource.MustParse("1")}),
					),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1, map[string]resource.Quantity{"c": resource.MustParse("2")}),
				),
			),
			node: node(node1, region1),
			// device1 (red) is accepted first; the blue candidates are rejected by
			// the match-attribute constraint after their counters are reserved.
			// Releasing those reservations lets the two blue devices allocate.
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device2, false),
				deviceAllocationResult(req0, driverA, pool1, device3, false),
			)},
		},
		// 本用例验证回溯撤销多份额分配时必须释放计数器预留。
		// 构造条件：启用 PartitionableDevices 与 ConsumableCapacity；device1 允许多次分配并占用唯一共享计数，device2 不消费计数，device3 满足 req1 且需要同一计数。
		// 期望结果：req0 先试 device1 预留计数，因 req1 只能用 device3 而回溯；释放后 req0→device2、req1→device3。
		"partitionable-consumable-capacity-backtrack-releases-counter": {
			features: Features{
				PartitionableDevices: true,
				ConsumableCapacity:   true,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil,
					request(req0, classA, 1, resourceapi.DeviceSelector{
						CEL: &resourceapi.CELDeviceSelector{
							Expression: fmt.Sprintf(`device.attributes["%s"].kind == "shared"`, driverA),
						}}),
					request(req1, classA, 1, resourceapi.DeviceSelector{
						CEL: &resourceapi.CELDeviceSelector{
							Expression: fmt.Sprintf(`device.attributes["%s"].kind == "counteronly"`, driverA),
						}}),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					// device1 allows multiple allocations and consumes the single
					// shared counter. It is listed first, so req0 reserves its counter
					// on the first try.
					device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"kind": {StringValue: new("shared")},
					}).withAllowMultipleAllocations().withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1, map[string]resource.Quantity{"c": resource.MustParse("1")}),
					),
					// device2 consumes no counter and is the correct choice for req0
					// once device1 has been backtracked.
					device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"kind": {StringValue: new("shared")},
					}),
					// device3 is the only device that satisfies req1 and needs the same
					// counter device1 reserved.
					device(device3, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"kind": {StringValue: new("counteronly")},
					}).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1, map[string]resource.Quantity{"c": resource.MustParse("1")}),
					),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1, map[string]resource.Quantity{"c": resource.MustParse("1")}),
				),
			),
			node: node(node1, region1),
			// device1 allows multiple allocations and reserves the single shared
			// counter when req0 tries it first. req1 can only use device3, which needs
			// that same counter, so the search must backtrack device1. The backtracking
			// undo has to release device1's counter reservation; otherwise req0 cannot
			// fall back to device2 and req1 cannot take device3.
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device2, false),
				deviceAllocationResult(req1, driverA, pool1, device3, false),
			)},
		},
		// 本用例验证候选因容量不足被拒时要回滚其对约束的引用。
		// 构造条件：claim0 先耗尽 device1 的容量；claim1 带 MatchAttribute 约束，其 all-mode 子请求包含 device1（构建候选时容量尚足，但函数内检查时已满）。
		// 期望结果：被拒候选的约束条目回滚后，回退子请求可选 device2；claim0 分配 device1 并带消耗容量，claim1 分配 device2。
		"consumable-capacity-rejection-rolls-back-constraints": {
			features: Features{
				ConsumableCapacity: true,
				PrioritizedList:    true,
			},
			claimsToAllocate: objects(
				// claim0 consumes all of device1's capacity first. Its request is not
				// subject to claim1's constraint, so it does not set the reference value.
				claim(claim0).withRequests(
					deviceRequest(req0, classA, 1).
						withCapacityRequest(new(two)).
						withSelectors(resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.attributes["%s"].stringAttribute == "cap"`, driverA),
							}}),
				),
				// claim1 has a match-attribute constraint. Its first (all-mode)
				// subrequest includes device1, which was capacity-sufficient when the
				// candidate list was built but is now full, so it is rejected on the
				// in-function capacity check after being added to the constraint. The
				// fallback subrequest then needs device2, whose attribute differs.
				claim(claim1).
					withConstraints(resourceapi.DeviceConstraint{MatchAttribute: &stringAttribute}).
					withRequests(requestWithPrioritizedList(req0,
						subRequest(subReq0, classA, 1, resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.attributes["%s"].stringAttribute == "cap"`, driverA),
							}}).
							withAllocationMode(resourceapi.DeviceAllocationModeAll).
							withCapacityRequest(new(two)),
						subRequest(subReq1, classA, 1, resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.attributes["%s"].stringAttribute == "fb"`, driverA),
							}}),
					)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, map[resourceapi.QualifiedName]resource.Quantity{capacity0: two},
						map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
							"stringAttribute": {StringValue: new("cap")},
						}).withAllowMultipleAllocations(),
					device(device2, nil,
						map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
							"stringAttribute": {StringValue: new("fb")},
						}),
				),
			),
			node: node(node1, region1),
			// claim1's all-mode subReq0 adds device1 to the match constraint and is then
			// rejected because claim0 already consumed device1's capacity. Rolling back
			// that rejected candidate's constraint entry is what lets the fallback
			// subReq1 take device2; otherwise device1's leaked attribute value blocks it.
			expectResults: []any{
				allocationResult(localNodeSelector(node1),
					deviceRequestAllocationResult(req0, driverA, pool1, device1).
						withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: two})),
				allocationResult(localNodeSelector(node1),
					deviceAllocationResult(req0SubReq1, driverA, pool1, device2, false)),
			},
		},
		// 本用例验证空容量多份额回溯时保留设备的共享标记与计数所有权。
		// 构造条件：claim0 作为首个份额占用 device1 的计数；claim1 的 req1 先试 device1 作第二份额（跳过计数）后因约束回溯到 device2，req3 再次共享 device1。
		// 期望结果：req0 与 req3 均以空消耗容量共享 device1，req1→device2、req2→device3；device1 的计数所有权不因 req1 回溯而丢失。
		"partitionable-empty-capacity-multiple-share-backtrack-keeps-counter-ownership": {
			features: Features{
				PartitionableDevices: true,
				ConsumableCapacity:   true,
			},
			claimsToAllocate: objects(
				// claim0 pins device1 as the first share and reserves its single
				// shared counter.
				claimWithRequests(claim0, nil,
					request(req0, classA, 1, resourceapi.DeviceSelector{
						CEL: &resourceapi.CELDeviceSelector{
							Expression: fmt.Sprintf(`device.attributes["%s"].kind == "shared"`, driverA),
						}}),
				),
				// claim1's req1 first tries device1 as a second share (skipping the
				// counter, since claim0 already reserved it), but the match-attribute
				// constraint with req2 forces it to backtrack onto device2. req3 then
				// shares device1 again, after req1 has already left it.
				claimWithRequests(claim1,
					[]resourceapi.DeviceConstraint{
						{MatchAttribute: &stringAttribute, Requests: []string{req1, req2}},
					},
					request(req1, classA, 1, resourceapi.DeviceSelector{
						CEL: &resourceapi.CELDeviceSelector{
							Expression: fmt.Sprintf(`device.attributes["%s"].kind == "shared" || device.attributes["%s"].kind == "fallback"`, driverA, driverA),
						}}),
					request(req2, classA, 1, resourceapi.DeviceSelector{
						CEL: &resourceapi.CELDeviceSelector{
							Expression: fmt.Sprintf(`device.attributes["%s"].kind == "gate"`, driverA),
						}}),
					request(req3, classA, 1, resourceapi.DeviceSelector{
						CEL: &resourceapi.CELDeviceSelector{
							Expression: fmt.Sprintf(`device.attributes["%s"].kind == "shared"`, driverA),
						}}),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					// device1: allow-multiple, no capacity, consumes the single counter.
					// kind steers the selectors; stringAttribute steers the constraint.
					device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"kind":            {StringValue: new("shared")},
						"stringAttribute": {StringValue: new("red")},
					}).withAllowMultipleAllocations().withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1, map[string]resource.Quantity{"c": resource.MustParse("1")}),
					),
					// device2: the fallback for req1, matching req2's stringAttribute.
					device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"kind":            {StringValue: new("fallback")},
						"stringAttribute": {StringValue: new("blue")},
					}),
					// device3: the only device req2 can take.
					device(device3, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"kind":            {StringValue: new("gate")},
						"stringAttribute": {StringValue: new("blue")},
					}),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1, map[string]resource.Quantity{"c": resource.MustParse("1")}),
				),
			),
			node: node(node1, region1),
			// claim0 reserves device1's counter as the first share. claim1's req1 tries
			// device1 as a second share, skips the counter, then backtracks onto device2
			// because of the constraint with req2. Rolling back that empty-capacity share
			// must not drop device1's shared marker, because claim0's share still holds
			// the counter. req3 then shares device1 again without recharging the counter.
			expectResults: []any{
				allocationResult(localNodeSelector(node1),
					deviceRequestAllocationResult(req0, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, nil)),
				allocationResult(localNodeSelector(node1),
					deviceAllocationResult(req1, driverA, pool1, device2, false),
					deviceAllocationResult(req2, driverA, pool1, device3, false),
					deviceRequestAllocationResult(req3, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, nil)),
			},
		},
		// 本用例验证优先列表与可分区设备组合时按容量选择候选。
		// 构造条件：启用 PrioritizedList 与 PartitionableDevices；req0 普通请求 1 台；req1 的 subReq0 需 >=6Gi、subReq1 需 >=4Gi；counterSet 总 memory 为 8Gi，设备消费分别为 4Gi、6Gi、4Gi。
		// 期望结果：req0 取 device1 后仅余 4Gi 容量，subReq0 的 >=6Gi 无法满足，回退 subReq1 取 device3。
		"partitionable-devices-prioritized-list": {
			features: Features{
				// 优先列表与可分区设备同时启用。
				PrioritizedList:      true,
				PartitionableDevices: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					deviceRequest(req0, classA, 1),
					requestWithPrioritizedList(req1,
						subRequest(subReq0, classA, 1, resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.capacity["%s"].memory.compareTo(quantity("6Gi")) >= 0`, driverA),
							}},
						),
						subRequest(subReq1, classA, 1, resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.capacity["%s"].memory.compareTo(quantity("4Gi")) >= 0`, driverA),
							}},
						),
					),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							},
						),
					),
					device(device2, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"memory": resource.MustParse("6Gi"),
							},
						),
					),
					device(device3, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							},
						),
					),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1,
						map[string]resource.Quantity{
							"memory": resource.MustParse("8Gi"),
						},
					),
				),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req1SubReq1, driverA, pool1, device3, false),
			)},
		},
		// 本用例验证多个请求共享同一计数器池时的容量筛选。
		// 构造条件：两个请求各 1 台；设备消费 memory 分别为 4Gi、6Gi、4Gi，总容量 8Gi。
		// 期望结果：req0→device1（4Gi），剩余 4Gi 使 device2（6Gi）不可用，req1→device3（4Gi）。
		"partitionable-devices-multiple-devices": {
			features: Features{
				PartitionableDevices: true,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil,
					request(req0, classA, 1),
					request(req1, classA, 1),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							},
						),
					),
					device(device2, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"memory": resource.MustParse("6Gi"),
							},
						),
					),
					device(device3, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							},
						),
					),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1,
						map[string]resource.Quantity{
							"memory": resource.MustParse("8Gi"),
						},
					),
				),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req1, driverA, pool1, device3, false),
			)},
		},
		// 本用例验证设备同时消费多个 counterSet 时的分配。
		// 构造条件：counterSet1 为 memory 18Gi、counterSet2 为 cpus 8；三台设备各消费两组计数；优先列表按 memory 容量过滤。
		// 期望结果：req0→device1，req1SubReq1→device3；两组计数器均需满足。
		"partitionable-devices-multiple-capacity-pools": {
			features: Features{
				PrioritizedList:      true,
				PartitionableDevices: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					deviceRequest(req0, classA, 1),
					requestWithPrioritizedList(req1,
						subRequest(subReq0, classA, 1, resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.capacity["%s"].memory.compareTo(quantity("6Gi")) >= 0`, driverA),
							}},
						),
						subRequest(subReq1, classA, 1, resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.capacity["%s"].memory.compareTo(quantity("4Gi")) >= 0`, driverA),
							}},
						),
					),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							},
						),
						deviceCounterConsumption(counterSet2,
							map[string]resource.Quantity{
								"cpus": resource.MustParse("4"),
							},
						),
					),
					device(device2, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"memory": resource.MustParse("6Gi"),
							},
						),
						deviceCounterConsumption(counterSet2,
							map[string]resource.Quantity{
								"cpus": resource.MustParse("6"),
							},
						),
					),
					device(device3, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							},
						),
						deviceCounterConsumption(counterSet2,
							map[string]resource.Quantity{
								"cpus": resource.MustParse("4"),
							},
						),
					),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1,
						map[string]resource.Quantity{
							"memory": resource.MustParse("18Gi"),
						},
					),
					counterSet(counterSet2,
						map[string]resource.Quantity{
							"cpus": resource.MustParse("8"),
						},
					),
				),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req1SubReq1, driverA, pool1, device3, false),
			)},
		},
		// 本用例验证计数器集合中包含多种资源时的容量计算。
		// 构造条件：两个 counterSet 各含 cpus 与 memory，三台设备消费组合不同。
		// 期望结果：按各计数余量依次分配，req0→device2、req1→device3。
		"partitionable-devices-multiple-counters": {
			features: Features{
				PartitionableDevices: true,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil,
					request(req0, classA, 1),
					request(req1, classA, 1),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
								"cpus":   resource.MustParse("6"),
							},
						),
						deviceCounterConsumption(counterSet2,
							map[string]resource.Quantity{
								"cpus":   resource.MustParse("4"),
								"memory": resource.MustParse("2Gi"),
							},
						),
					),
					device(device2, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"memory": resource.MustParse("6Gi"),
								"cpus":   resource.MustParse("4"),
							},
						),
						deviceCounterConsumption(counterSet2,
							map[string]resource.Quantity{
								"cpus":   resource.MustParse("6"),
								"memory": resource.MustParse("6Gi"),
							},
						),
					),
					device(device3, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
								"cpus":   resource.MustParse("4"),
							},
						),
						deviceCounterConsumption(counterSet2,
							map[string]resource.Quantity{
								"cpus":   resource.MustParse("4"),
								"memory": resource.MustParse("4Gi"),
							},
						),
					),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1,
						map[string]resource.Quantity{
							"cpus":   resource.MustParse("8"),
							"memory": resource.MustParse("18Gi"),
						},
					),
					counterSet(counterSet2,
						map[string]resource.Quantity{
							"cpus":   resource.MustParse("12"),
							"memory": resource.MustParse("18Gi"),
						},
					),
				),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device2, false),
				deviceAllocationResult(req1, driverA, pool1, device3, false),
			)},
		},
		// 本用例验证已分配设备来自多个池时，新分配转向仍有容量的池。
		// 构造条件：pool1 与 pool2 各有 counterSet(cpus 8, memory 18Gi)；pool1 的 device2、device3 与 pool2 的 device1 已被占用。
		// 期望结果：req0→pool2/device2，req1→pool2/device3。
		"partitionable-devices-multiple-pools": {
			features: Features{
				PartitionableDevices: true,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil,
					request(req0, classA, 1),
					request(req1, classA, 1),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"cpus":   resource.MustParse("6"),
								"memory": resource.MustParse("4Gi"),
							},
						),
					),
					device(device2, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"cpus":   resource.MustParse("4"),
								"memory": resource.MustParse("6Gi"),
							},
						),
					),
					device(device3, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"cpus":   resource.MustParse("4"),
								"memory": resource.MustParse("4Gi"),
							},
						),
					),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1,
						map[string]resource.Quantity{
							"cpus":   resource.MustParse("8"),
							"memory": resource.MustParse("18Gi"),
						},
					),
				),
				sliceWithDevices(slice3, node1, resourcePool(pool2, 2), driverA,
					device(device1, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"cpus":   resource.MustParse("6"),
								"memory": resource.MustParse("4Gi"),
							},
						),
					),
					device(device2, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"cpus":   resource.MustParse("1"),
								"memory": resource.MustParse("7Gi"),
							},
						),
					),
					device(device3, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"cpus":   resource.MustParse("1"),
								"memory": resource.MustParse("7Gi"),
							},
						),
					),
				),
				sliceWithCounterSets(slice4, node1, resourcePool(pool2, 2), driverA,
					counterSet(counterSet1,
						map[string]resource.Quantity{
							"cpus":   resource.MustParse("8"),
							"memory": resource.MustParse("18Gi"),
						},
					),
				),
			),
			allocatedDevices: []DeviceID{
				MakeDeviceID(driverA, pool1, device2),
				MakeDeviceID(driverA, pool1, device3),
				// 两个池都有设备被占用，迫使新请求落在 pool2。
				MakeDeviceID(driverA, pool2, device1),
			},
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool2, device2, false),
				deviceAllocationResult(req1, driverA, pool2, device3, false),
			)},
		},
		// 本用例验证 counterSet 中未被设备消费的计数器不影响分配。
		// 构造条件：counterSet 含 memory 8Gi 与未使用的 unused 1Gi；三台设备分别消费 memory 5Gi、4Gi、4Gi；请求 2 台。
		// 期望结果：分配 device2、device3（合计 8Gi），unused 计数被忽略。
		"partitionable-devices-unused-counters-in-counter-set": {
			features: Features{
				PartitionableDevices: true,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil, request(req0, classA, 2)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"memory": resource.MustParse("5Gi"),
							},
						),
					),
					device(device2, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							},
						),
					),
					device(device3, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							},
						),
					),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1,
						map[string]resource.Quantity{
							"memory": resource.MustParse("8Gi"),
							"unused": resource.MustParse("1Gi"),
						},
					),
				),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device2, false),
				deviceAllocationResult(req0, driverA, pool1, device3, false),
			)},
		},
		// 本用例验证已分配设备耗尽容量时不再分配。
		// 构造条件：counterSet 总 memory 18Gi，device2 消费 16Gi 且已分配，仅余 2Gi 不足 device1 的 4Gi。
		// 期望结果：expectResults 为 nil，分配失败。
		"partitionable-devices-no-capacity-available": {
			features: Features{
				PartitionableDevices: true,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil,
					request(req0, classA, 1),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							},
						),
					),
					device(device2, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"memory": resource.MustParse("16Gi"),
							},
						),
					),
				),
				sliceWithCounterSets(slice2, node1, pool1, driverA,
					counterSet(counterSet1,
						map[string]resource.Quantity{
							"memory": resource.MustParse("18Gi"),
						},
					),
				),
			),
			allocatedDevices: []DeviceID{
				MakeDeviceID(driverA, pool1, device2),
			},
			// 剩余容量不足，分配失败。
			node:          node(node1, region1),
			expectResults: nil,
		},
		// 本用例验证已分配设备超消费导致池不可用时的处理。
		// 构造条件：counterSet 总 memory 18Gi，已分配的 device2 消费 20Gi（超额）。
		// 期望结果：expectResults 为 nil，池被视为无可用容量。
		"partitionable-devices-overallocated-capacity-pool": {
			features: Features{
				PartitionableDevices: true,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil,
					request(req0, classA, 1),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							},
						),
					),
					device(device2, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"memory": resource.MustParse("20Gi"),
							},
						),
					),
				),
				sliceWithCounterSets(slice2, node1, pool1, driverA,
					counterSet(counterSet1,
						map[string]resource.Quantity{
							"memory": resource.MustParse("18Gi"),
						},
					),
				),
			),
			allocatedDevices: []DeviceID{
				MakeDeviceID(driverA, pool1, device2),
			},
			node:          node(node1, region1),
			expectResults: nil,
		},
		// 本用例验证特性关闭时带计数消费的设备不可用。
		// 构造条件：PartitionableDevices=false，唯一设备带 counter 消费。
		// 期望结果：expectResults 为 nil。
		"partitionable-devices-disabled-device-consumes-counters": {
			features: Features{
				// 特性关闭时不接受带计数消费的设备。
				PartitionableDevices: false,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil,
					request(req0, classA, 1),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							},
						),
					),
				),
				sliceWithCounterSets(slice2, node1, pool1, driverA,
					counterSet(counterSet1,
						map[string]resource.Quantity{
							"memory": resource.MustParse("18Gi"),
						},
					),
				),
			),
			node:          node(node1, region1),
			expectResults: nil,
		},
		// 本用例验证特性关闭时无计数消费的设备仍可正常分配。
		// 构造条件：PartitionableDevices=false；device1 带计数消费，device2 无消费。
		// 期望结果：跳过 device1，分配 device2。
		"partitionable-devices-disabled-other-device-do-not-consume-counters": {
			features: Features{
				PartitionableDevices: false,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil,
					request(req0, classA, 1),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							},
						),
					),
					device(device2, nil, nil),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1,
						map[string]resource.Quantity{
							"memory": resource.MustParse("18Gi"),
						},
					),
				),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device2, false),
			)},
		},
		// 本用例验证特性关闭时逐设备节点选择不受支持。
		// 构造条件：PartitionableDevices=false，切片使用 nodeSelectionPerDevice 并指定设备节点。
		// 期望结果：expectResults 为 nil。
		"partitionable-devices-disabled-per-device-node-selection": {
			features: Features{
				PartitionableDevices: false,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil,
					request(req0, classA, 1),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, nodeSelectionPerDevice, pool1, driverA,
					device(device2, nil, nil).withNodeSelection(node1),
				),
			),
			node:          node(node1, region1),
			expectResults: nil,
		},
		// 本用例验证设备以具体节点名声明节点选择时的筛选。
		// 构造条件：启用可分区设备与优先列表；切片为逐设备节点选择；device1 绑定 node1(4Gi)、device2 绑定 node2(6Gi)，当前调度节点为 node1。
		// 期望结果：device2 因节点不匹配被排除；subReq0 的 >=6Gi 无候选，回退 subReq1 分配 device1。
		"partitionable-devices-per-device-node-selection-nodename": {
			features: Features{
				PartitionableDevices: true,
				PrioritizedList:      true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					requestWithPrioritizedList(req0,
						subRequest(subReq0, classA, 1, resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.capacity["%s"].memory.compareTo(quantity("6Gi")) >= 0`, driverA),
							}},
						),
						subRequest(subReq1, classA, 1, resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.capacity["%s"].memory.compareTo(quantity("4Gi")) >= 0`, driverA),
							}},
						),
					),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				// 该切片声明逐设备节点选择。
				sliceWithDevices(slice1, nodeSelectionPerDevice, resourcePool(pool1, 2), driverA,
					device(device1, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							},
						),
					).withNodeSelection(node1),
					device(device2, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"memory": resource.MustParse("6Gi"),
							},
						),
					).withNodeSelection(node2),
				),
				sliceWithCounterSets(slice2, nodeSelectionPerDevice, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1,
						map[string]resource.Quantity{
							"memory": resource.MustParse("18Gi"),
						},
					),
				),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0SubReq1, driverA, pool1, device1, false),
			)},
		},
		// 本用例验证设备以节点标签选择器声明节点选择时，结果带有对应 NodeSelector。
		// 构造条件：设备通过 nodeLabelSelector(regionKey, region1) 声明节点选择，当前节点 region=region1。
		// 期望结果：分配 device1，且结果中的 NodeSelector 为 region In [region1]。
		"partitionable-devices-per-device-node-selection-node-selector": {
			features: Features{
				PartitionableDevices: true,
				PrioritizedList:      true,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil, request(req0, classA, 1)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, nodeSelectionPerDevice, resourcePool(pool1, 2), driverA,
					device(device1, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							},
						),
					).withNodeSelection(nodeLabelSelector(regionKey, region1)),
				),
				sliceWithCounterSets(slice2, nodeSelectionPerDevice, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1,
						map[string]resource.Quantity{
							"memory": resource.MustParse("18Gi"),
						},
					),
				),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				&v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{{
						MatchExpressions: []v1.NodeSelectorRequirement{
							{Key: regionKey, Operator: v1.NodeSelectorOpIn, Values: []string{region1}},
						},
					}},
				},
				deviceAllocationResult(req0, driverA, pool1, device1, false),
			)},
		},
		// 本用例验证多台设备节点选择合并后的 NodeSelector。
		// 构造条件：driverA 设备的节点选择分别为标签选择器、指定节点名、全部；driverB 设备无逐设备选择；两个请求各取 3 台。
		// 期望结果：结果 NodeSelector 为 metadata.name In [node1]，六个请求设备全部成功分配。
		"partitionable-devices-per-device-node-selection-node-selector-multiple-devices": {
			features: Features{
				PartitionableDevices: true,
				PrioritizedList:      true,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil,
					request(req0, classA, 3),
					request(req1, classB, 3),
				),
			),
			classes: objects(class(classA, driverA), class(classB, driverB)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, nodeSelectionPerDevice, resourcePool(pool1, 2), driverA,
					device(device1, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							},
						),
					).withNodeSelection(nodeLabelSelector(regionKey, region1)),
					device(device2, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							},
						),
					).withNodeSelection(node1),
					device(device3, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							},
						),
					).withNodeSelection(nodeSelectionAll),
				),
				sliceWithCounterSets(slice2, nodeSelectionPerDevice, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1,
						map[string]resource.Quantity{
							"memory": resource.MustParse("18Gi"),
						},
					),
				),
				sliceWithDevices(slice3, node1, resourcePool(pool2, 2), driverB,
					device(device1, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet2,
							map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							},
						),
					),
					device(device2, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet2,
							map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							},
						),
					),
					device(device3, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet2,
							map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							},
						),
					),
				),
				sliceWithCounterSets(slice4, node1, resourcePool(pool2, 2), driverB,
					counterSet(counterSet2,
						map[string]resource.Quantity{
							"memory": resource.MustParse("12Gi"),
						},
					),
				),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				&v1.NodeSelector{
					NodeSelectorTerms: []v1.NodeSelectorTerm{{
						MatchFields: []v1.NodeSelectorRequirement{
							{Key: fieldNameKey, Operator: v1.NodeSelectorOpIn, Values: []string{node1}},
						},
					}},
				},
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req0, driverA, pool1, device2, false),
				deviceAllocationResult(req0, driverA, pool1, device3, false),
				deviceAllocationResult(req1, driverB, pool2, device1, false),
				deviceAllocationResult(req1, driverB, pool2, device2, false),
				deviceAllocationResult(req1, driverB, pool2, device3, false),
			)},
		},
		// 本用例验证两台设备分别带 NoSchedule 与 NoExecute 污点且声明无容忍时都不能分配。
		// 构造条件：DeviceTaints=true，两台设备各带一种污点，claim 无 toleration。
		// 期望结果：未设置 expectResults，即没有可用分配。
		"tainted-two-devices": {
			features: Features{
				DeviceTaints: true,
			},
			claimsToAllocate: objects(claimWithRequest(claim0, req0, classA)),
			classes:          objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, nil).withTaints(taintNoSchedule),
				device(device2, nil, nil).withTaints(taintNoExecute),
			)),
			node: node(node1, region1),
		},
		// 本用例验证单设备带两个污点且无任何容忍时不可分配。
		// 构造条件：唯一设备同时带 NoSchedule 与 NoExecute，claim 无 toleration。
		// 期望结果：无分配结果。
		"tainted-one-device-two-taints": {
			features: Features{
				DeviceTaints: true,
			},
			claimsToAllocate: objects(claimWithRequest(claim0, req0, classA)),
			classes:          objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, nil).withTaints(taintNoSchedule, taintNoExecute),
			)),
			node: node(node1, region1),
		},
		// 本用例验证容忍可以匹配设备的污点。
		// 构造条件：claim 带 tolerationNoExecute；device1 为 NoSchedule（不被容忍）、device2 为 NoExecute（被容忍）。
		// 期望结果：仅分配 device2，并在结果中记录对应的容忍。
		"tainted-two-devices-tolerated": {
			features: Features{
				DeviceTaints: true,
			},
			claimsToAllocate: objects(claimWithRequest(claim0, req0, classA).withTolerations(tolerationNoExecute)),
			classes:          objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, nil).withTaints(taintNoSchedule),
				device(device2, nil, nil).withTaints(taintNoExecute),
			)),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				// 容忍信息随分配结果一并返回。
				deviceAllocationResult(req0, driverA, pool1, device2, false, tolerationNoExecute), // Only second device's taints are tolerated.
			)},
		},
		// 本用例验证优先列表中子请求级别的容忍。
		// 构造条件：subReq0 无容忍、subReq1 带 tolerationNoExecute；两台设备分别为 NoSchedule 与 NoExecute。
		// 期望结果：subReq0 无法满足，回退 subReq1 分配 device2 并记录容忍。
		"tainted-two-devices-prioritized-list-tolerated": {
			features: Features{
				DeviceTaints:    true,
				PrioritizedList: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(requestWithPrioritizedList(req0,
					subRequest(subReq0, classA, 1),
					subRequest(subReq1, classA, 1, tolerationNoExecute),
				))),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, nil).withTaints(taintNoSchedule),
				device(device2, nil, nil).withTaints(taintNoExecute),
			)),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0SubReq1, driverA, pool1, device2, false, tolerationNoExecute), // Only second device's taints are tolerated.
			)},
		},
		// 本用例验证 NoEffect 污点不影响调度。
		// 构造条件：设备带 taintNone，声明无容忍。
		// 期望结果：正常分配 device1。
		"tainted-no-effect": {
			features: Features{
				DeviceTaints: true,
			},
			claimsToAllocate: objects(claimWithRequest(claim0, req0, classA)),
			classes:          objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, nil).withTaints(taintNone),
			)),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
			)},
		},
		// 本用例验证声明同时容忍设备的两个污点后分配成功。
		// 构造条件：设备带 NoSchedule 与 NoExecute，声明带两个对应 toleration。
		// 期望结果：分配成功，结果中记录两个容忍。
		"tainted-one-device-two-taints-both-tolerated": {
			features: Features{
				DeviceTaints: true,
			},
			claimsToAllocate: objects(claimWithRequest(claim0, req0, classA).withTolerations(tolerationNoSchedule, tolerationNoExecute)),
			classes:          objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, nil).withTaints(taintNoSchedule, taintNoExecute),
			)),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false, tolerationNoSchedule, tolerationNoExecute),
			)},
		},
		// 本用例验证污点特性关闭时污点被忽略。
		// 构造条件：DeviceTaints=false，设备带两个污点，声明无容忍。
		// 期望结果：仍然成功分配 device1。
		"tainted-disabled": {
			features: Features{
				// 特性关闭，污点检查被跳过。
				DeviceTaints: false,
			},
			claimsToAllocate: objects(claimWithRequest(claim0, req0, classA)),
			classes:          objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, nil).withTaints(taintNoSchedule, taintNoExecute),
			)),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
			)},
		},
		// 本用例验证优先列表中的所有候选设备均因污点不可用时分配失败。
		// 构造条件：DeviceTaints=true 且 PrioritizedList=true；subReq0 无设备，subReq1 的设备带 NoSchedule 且无容忍。
		// 期望结果：无分配结果。
		"tainted-prioritized-list": {
			features: Features{
				DeviceTaints:    true,
				PrioritizedList: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					requestWithPrioritizedList(req0,
						subRequest(subReq0, classB, 1),
						subRequest(subReq1, classA, 1),
					))),
			classes: objects(class(classA, driverA), class(classB, driverB)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, nil).withTaints(taintNoSchedule),
			)),
			node: node(node1, region1),
		},
		// 本用例验证污点特性关闭时优先列表仍可分配带污点设备。
		// 构造条件：DeviceTaints=false、PrioritizedList=true；subReq1 设备带 NoSchedule。
		// 期望结果：回退到 subReq1 并成功分配 device1。
		"tainted-prioritized-list-disabled": {
			features: Features{
				DeviceTaints:    false,
				PrioritizedList: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					requestWithPrioritizedList(req0,
						subRequest(subReq0, classB, 1),
						subRequest(subReq1, classA, 1),
					))),
			classes: objects(class(classA, driverA), class(classB, driverB)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, nil).withTaints(taintNoSchedule),
			)),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0SubReq1, driverA, pool1, device1, false),
			)},
		},
		// 本用例验证请求 admin access 且设备已被占用时分配失败。
		// 构造条件：DeviceTaints 与 AdminAccess 均开启，请求设置 AdminAccess=true；device1、device2 均已分配，池中唯一设备带 NoSchedule。
		// 期望结果：无分配结果。
		"tainted-admin-access": {
			features: Features{
				// 请求 admin access 且目标设备已被占用。
				DeviceTaints: true,
				AdminAccess:  true,
			},
			claimsToAllocate: func() []wrapResourceClaim {
				c := claimWithRequest(claim0, req0, classA)
				c.Spec.Devices.Requests[0].Exactly.AdminAccess = ptr.To(true)
				return []wrapResourceClaim{c}
			}(),
			allocatedDevices: []DeviceID{
				MakeDeviceID(driverA, pool1, device1),
				MakeDeviceID(driverA, pool1, device2),
			},
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, nil).withTaints(taintNoSchedule),
			)),
			node: node(node1, region1),
		},
		// 本用例验证污点特性关闭时 admin access 请求仍可分配。
		// 构造条件：DeviceTaints=false、AdminAccess=true，请求设置 AdminAccess=true。
		// 期望结果：分配 device1 且结果中 adminAccess 标记为 true。
		"tainted-admin-access-disabled": {
			features: Features{
				DeviceTaints: false,
				AdminAccess:  true,
			},
			claimsToAllocate: func() []wrapResourceClaim {
				c := claimWithRequest(claim0, req0, classA)
				c.Spec.Devices.Requests[0].Exactly.AdminAccess = ptr.To(true)
				return []wrapResourceClaim{c}
			}(),
			allocatedDevices: []DeviceID{
				MakeDeviceID(driverA, pool1, device1),
				MakeDeviceID(driverA, pool1, device2),
			},
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, nil).withTaints(taintNoSchedule),
			)),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, true),
			)},
		},
		// 本用例验证 All 模式下唯一设备带污点且不可容忍时分配失败。
		// 构造条件：DeviceTaints=true，请求使用 AllocationModeAll，池中唯一设备带 NoSchedule。
		// 期望结果：无分配结果。
		"tainted-all-devices-single": {
			features: Features{
				DeviceTaints: true,
			},
			claimsToAllocate: objects(claimWithRequests(claim0, nil, resourceapi.DeviceRequest{
				Name: req0,
				Exactly: &resourceapi.ExactDeviceRequest{
					AllocationMode:  resourceapi.DeviceAllocationModeAll,
					DeviceClassName: classA,
				},
			})),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, nil).withTaints(taintNoSchedule),
			)),
			node: node(node1, region1),
		},
		// 本用例验证污点特性关闭时 All 模式可分配带污点设备。
		// 构造条件：DeviceTaints=false，其他条件与上例相同。
		// 期望结果：成功分配 device1。
		"tainted-all-devices-single-disabled": {
			features: Features{
				DeviceTaints: false,
			},
			claimsToAllocate: objects(claimWithRequests(claim0, nil, resourceapi.DeviceRequest{
				Name: req0,
				Exactly: &resourceapi.ExactDeviceRequest{
					AllocationMode:  resourceapi.DeviceAllocationModeAll,
					DeviceClassName: classA,
				},
			})),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, nil).withTaints(taintNoSchedule),
			)),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
			)},
		},
		// 本用例验证 All 模式子请求在设备已被占用时回退到普通子请求。
		// 构造条件：subReq0 为 AllocationModeAll，subReq1 请求 1 台；池中 device2 已被占用。
		// 期望结果：subReq0 无法满足，subReq1 分配空闲的 device1。
		"prioritized-list-allocation-mode-all": {
			features: Features{
				PrioritizedList: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					requestWithPrioritizedList(req0,
						subRequest(subReq0, classA, 0).withAllocationMode(resourceapi.DeviceAllocationModeAll),
						subRequest(subReq1, classA, 1),
					),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, nil),
				device(device2, nil, nil),
			)),
			allocatedDevices: []DeviceID{
				// 已有设备被占用，All 模式子请求无法满足。
				MakeDeviceID(driverA, pool1, device2),
			},
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0SubReq1, driverA, pool1, device1, false),
			)},
		},
		// 本用例验证 All 模式子请求的选择器无匹配设备时会尝试下一个子请求。
		// 构造条件：两个 All 模式子请求分别按 generation v6、v5 选择；池中只有 v5 的 device1。
		// 期望结果：subReq0（v6）无匹配设备，回退 subReq1（v5）分配 device1。
		"prioritized-list-allocation-mode-all-with-selectors": {
			// When using allocationMode:All, at least one device must be allocated, otherwise
			// DRA will attempt the next subRequest.
			features: Features{
				PrioritizedList: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					requestWithPrioritizedList(req0,
						subRequest(subReq0, classA, 0, resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.attributes["%s"].generation == "v6"`, driverA),
							},
						}).withAllocationMode(resourceapi.DeviceAllocationModeAll),
						subRequest(subReq1, classA, 0, resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.attributes["%s"].generation == "v5"`, driverA),
							},
						}).withAllocationMode(resourceapi.DeviceAllocationModeAll),
					),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"generation": {StringValue: ptr.To("v5")},
					}),
				),
			),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0SubReq1, driverA, pool1, device1, false),
			)},
		},
		// 本用例验证 All 模式在多个资源池中都选中设备时会全部分配。
		// 构造条件：subReq1（v5）在 pool1 与 pool2 各匹配一台 device1；两个子请求均为 All 模式。
		// 期望结果：subReq1 同时分配 pool1 与 pool2 的两台设备。
		"prioritized-list-allocation-mode-all-with-selectors-multiple-pools": {
			// All devices selected by the DeviceClass will be allocated, even if they
			// are spread across different resource pools.
			features: Features{
				PrioritizedList: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					requestWithPrioritizedList(req0,
						subRequest(subReq0, classA, 0, resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.attributes["%s"].generation == "v6"`, driverA),
							},
						}).withAllocationMode(resourceapi.DeviceAllocationModeAll),
						subRequest(subReq1, classA, 0, resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.attributes["%s"].generation == "v5"`, driverA),
							},
						}).withAllocationMode(resourceapi.DeviceAllocationModeAll),
					),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"generation": {StringValue: ptr.To("v5")},
					}),
				),
				sliceWithDevices(slice2, node1, pool2, driverA,
					device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"generation": {StringValue: ptr.To("v5")},
					}),
				),
			),
			node: node(node1, region1),

			expectResults: []any{
				allocationResult(
					localNodeSelector(node1),
					deviceAllocationResult(req0SubReq1, driverA, pool1, device1, false),
					deviceAllocationResult(req0SubReq1, driverA, pool2, device1, false),
				),
			},
		},
		// 本用例验证跨节点的资源池不会被纳入当前节点的分配结果。
		// 构造条件：v5 设备位于 node1/pool1 与 node2/pool2，v6 设备在 node2/pool2；调度节点为 node1。
		// 期望结果：仅分配当前节点上的 pool1/device1，node2 上的设备被忽略。
		"prioritized-list-allocation-mode-all-with-selectors-multiple-pools-across-nodes": {
			// All devices selected by the DeviceClass will be allocated, even if they
			// are spread across different resource pools, as long as they are available
			// on the node.
			features: Features{
				PrioritizedList: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					requestWithPrioritizedList(req0,
						subRequest(subReq0, classA, 0, resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.attributes["%s"].generation == "v6"`, driverA),
							},
						}).withAllocationMode(resourceapi.DeviceAllocationModeAll),
						subRequest(subReq1, classA, 0, resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.attributes["%s"].generation == "v5"`, driverA),
							},
						}).withAllocationMode(resourceapi.DeviceAllocationModeAll),
					),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"generation": {StringValue: ptr.To("v5")},
					}),
				),
				sliceWithDevices(slice2, node2, pool2, driverA,
					device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"generation": {StringValue: ptr.To("v5")},
					}),
					device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"generation": {StringValue: ptr.To("v6")},
					}),
				),
			),
			node: node(node1, region1),

			expectResults: []any{
				allocationResult(
					localNodeSelector(node1),
					deviceAllocationResult(req0SubReq1, driverA, pool1, device1, false),
				),
			},
		},
		// 本用例验证 All 模式只会分配 DeviceClass 选择器选中的设备子集。
		// 构造条件：DeviceClass 使用自定义选择器（driver==driverA 且 selected==true）；池中三台设备只有 device3 的 selected 为 true 且 generation 为 v5。
		// 期望结果：subReq0（v6）无匹配，subReq1（v5）只分配被类选择器选中的 device3。
		"prioritized-list-allocation-mode-all-with-selectors-deviceclass-subset": {
			// Only devices selected by the deviceclass will be considered
			features: Features{
				PrioritizedList: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					requestWithPrioritizedList(req0,
						subRequest(subReq0, classA, 0, resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.attributes["%s"].generation == "v6"`, driverA),
							},
						}).withAllocationMode(resourceapi.DeviceAllocationModeAll),
						subRequest(subReq1, classA, 0, resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.attributes["%s"].generation == "v5"`, driverA),
							},
						}).withAllocationMode(resourceapi.DeviceAllocationModeAll),
					),
				),
			),
			classes: objects(
				&resourceapi.DeviceClass{
					ObjectMeta: metav1.ObjectMeta{
						Name: classA,
					},
					Spec: resourceapi.DeviceClassSpec{
						Selectors: []resourceapi.DeviceSelector{
							{
								CEL: &resourceapi.CELDeviceSelector{
									Expression: fmt.Sprintf(`device.driver == "%s" && device.attributes["%s"].selected`, driverA, driverA),
								},
							},
						},
					},
				},
			),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"generation": {StringValue: ptr.To("v6")},
						"selected":   {BoolValue: ptr.To(false)},
					}),
					device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"generation": {StringValue: ptr.To("v5")},
						"selected":   {BoolValue: ptr.To(false)},
					}),
					device(device3, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"generation": {StringValue: ptr.To("v5")},
						"selected":   {BoolValue: ptr.To(true)},
					}),
				),
			),
			node: node(node1, region1),

			expectResults: []any{
				allocationResult(
					localNodeSelector(node1),
					deviceAllocationResult(req0SubReq1, driverA, pool1, device3, false),
				),
			},
		},
		// 本用例验证单个请求可分配的设备数量能够达到 AllocationResultsMaxSize 上限。
		// 构造条件：claim0 的 req0 请求数量直接设为上限值；slice1 提供恰好上限数量的设备。
		// 期望结果：req0 一次性分配池中全部设备（device-0 起始的整批结果），不触发数量截断或报错。
		"max-number-devices": {
			claimsToAllocate: objects(
				claimWithRequests(
					claim0, nil, request(req0, classA, resourceapi.AllocationResultsMaxSize),
				),
			),
			classes: objects(class(classA, driverA)),
			slices:  unwrapResourceSlices(sliceWithMultipleDevices(slice1, node1, pool1, driverA, resourceapi.AllocationResultsMaxSize)),
			node:    node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				multipleDeviceAllocationResults(req0, driverA, pool1, resourceapi.AllocationResultsMaxSize, 0)...,
			)},
		},
		// 本用例验证可分区设备上的 CEL 属性选择器与设备计数器消耗能够协同工作。
		// 特性开关：PartitionableDevices 开启。
		// 构造条件：req0 请求 2 台，req1 带 CEL 选择器只接受 special 为 true 的设备；三台设备分别声明不同的 cpu/mem 计数器消耗，counterset 给出总量。
		// 期望结果：req0 分配到 special 为 false 的 device2、device3，req1 独占 special 为 true 的 device1。
		"partitionable-devices-with-attribute-selector": {
			features: Features{
				PartitionableDevices: true,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil,
					request(req0, classA, 2),
					request(req1, classA, 1, resourceapi.DeviceSelector{
						CEL: &resourceapi.CELDeviceSelector{
							Expression: fmt.Sprintf(`device.attributes["%s"].special`, driverA),
						},
					}),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil,
						map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
							"special": {
								BoolValue: ptr.To(true),
							},
						},
					).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"cpu1": resource.MustParse("1"),
							},
						),
						deviceCounterConsumption(counterSet2,
							map[string]resource.Quantity{
								"mem": resource.MustParse("10Gi"),
							},
						),
					),
					device(device2, nil,
						map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
							"special": {
								BoolValue: ptr.To(false),
							},
						},
					).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"cpu2": resource.MustParse("1"),
							},
						),
						deviceCounterConsumption(counterSet2,
							map[string]resource.Quantity{
								"mem": resource.MustParse("10Gi"),
							},
						),
					),
					device(device3, nil,
						map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
							"special": {
								BoolValue: ptr.To(false),
							},
						},
					).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"cpu3": resource.MustParse("1"),
							},
						),
						deviceCounterConsumption(counterSet2,
							map[string]resource.Quantity{
								"mem": resource.MustParse("10Gi"),
							},
						),
					),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1,
						map[string]resource.Quantity{
							"cpu1": resource.MustParse("1"),
							"cpu2": resource.MustParse("1"),
							"cpu3": resource.MustParse("1"),
						},
					),
					counterSet(counterSet2,
						map[string]resource.Quantity{
							"mem": resource.MustParse("30Gi"),
						},
					),
				),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device2, false),
				deviceAllocationResult(req0, driverA, pool1, device3, false),
				deviceAllocationResult(req1, driverA, pool1, device1, false),
			)},
		},
		// 本用例验证优先列表中子请求数量达到分配结果上限时的回退行为及分配器调用次数。
		// 特性开关：PrioritizedList 开启。
		// 构造条件：两个请求各含两个子请求，首选子请求数量为上限、次选为上限的一半；池中设备总数为上限的两倍。
		// 期望结果：两个请求都因首选子请求超出上限而回退到 subReq1，各分配上限一半的设备；expectNumAllocateOneInvocations 固定为 75。
		"prioritized-list-max-allocation-limit-request": {
			features: Features{
				PrioritizedList: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					requestWithPrioritizedList(req0,
						subRequest(subReq0, classA, resourceapi.AllocationResultsMaxSize),
						subRequest(subReq1, classA, resourceapi.AllocationResultsMaxSize/2),
					),
					requestWithPrioritizedList(req1,
						subRequest(subReq0, classA, resourceapi.AllocationResultsMaxSize),
						subRequest(subReq1, classA, resourceapi.AllocationResultsMaxSize/2),
					),
				),
			),
			classes: objects(class(classA, driverA)),
			slices:  unwrapResourceSlices(sliceWithMultipleDevices(slice1, node1, pool1, driverA, resourceapi.AllocationResultsMaxSize*2)),
			node:    node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				slices.Concat(
					multipleDeviceAllocationResults(req0SubReq1, driverA, pool1, resourceapi.AllocationResultsMaxSize/2, 0),
					multipleDeviceAllocationResults(req1SubReq1, driverA, pool1, resourceapi.AllocationResultsMaxSize/2, resourceapi.AllocationResultsMaxSize/2),
				)...,
			)},
			expectNumAllocateOneInvocations: 75,
		},
		// 本用例验证优先列表中 All 模式子请求不可满足时回退，并与另一类的普通请求共同分配。
		// 特性开关：PrioritizedList 开启。
		// 构造条件：req0 的 subReq0 为 All 模式但池中设备数不足以容纳上限减一的请求，subReq1 请求 2 台；req1 请求 classB 的 2 台设备。
		// 期望结果：req0 回退到 subReq1 并分配 driverA 的 device-0/device-1，req1 分配 driverB 的 device-0/device-1；调用次数为 42。
		"prioritized-list-max-allocation-allocation-mode-all": {
			features: Features{
				PrioritizedList: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					requestWithPrioritizedList(req0,
						subRequest(subReq0, classA, 0).withAllocationMode(resourceapi.DeviceAllocationModeAll),
						subRequest(subReq1, classA, 2),
					),
					deviceRequest(req1, classB, 2),
				),
			),
			classes: objects(class(classA, driverA), class(classB, driverB)),
			slices: unwrapResourceSlices(
				sliceWithMultipleDevices(slice1, node1, pool1, driverA, resourceapi.AllocationResultsMaxSize-1),
				sliceWithMultipleDevices(slice2, node1, pool2, driverB, 2),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0SubReq1, driverA, pool1, "device-0", false),
				deviceAllocationResult(req0SubReq1, driverA, pool1, "device-1", false),
				deviceAllocationResult(req1, driverB, pool2, "device-0", false),
				deviceAllocationResult(req1, driverB, pool2, "device-1", false),
			)},
			expectNumAllocateOneInvocations: 42,
		},
		// 本用例验证设备上的绑定条件（binding conditions）被原样写入分配结果。
		// 特性开关：DeviceBindingAndStatus 开启。
		// 构造条件：唯一设备 device1 通过 withBindingConditions 携带 IsPrepare/BindingFailed 条件。
		// 期望结果：分配成功，req0 的结果条目为带绑定条件的专用结构，NodeSelector 指向 node1。
		"device-binding-conditions": {
			features: Features{
				DeviceBindingAndStatus: true,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil, request(req0, classA, 1))),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, nil).withBindingConditions([]string{"IsPrepare"}, []string{"BindingFailed"}))),
			node: node(node1, region1),

			expectResults: []any{
				resourceapi.AllocationResult{
					Devices: resourceapi.DeviceAllocationResult{
						Results: []resourceapi.DeviceRequestAllocationResult{
							deviceRequestAllocationResultWithBindingConditions(req0, driverA, pool1, device1, []string{"IsPrepare"}, []string{"BindingFailed"}),
						},
					},
					NodeSelector: localNodeSelector(node1),
				},
			},
		},
		// 本用例验证多台设备各自的绑定条件在不同请求的结果条目中分别保留。
		// 特性开关：DeviceBindingAndStatus 开启。
		// 构造条件：三台设备分别携带三组不同的绑定条件；req0 请求 2 台，req1 请求 1 台。
		// 期望结果：req0 分配 device1、device2，req1 分配 device3，每个结果条目携带对应设备自己的条件，互不串味。
		"binding-conditions-multiple-devices": {
			features: Features{
				DeviceBindingAndStatus: true,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil,
					request(req0, classA, 2),
					request(req1, classA, 1))),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, nil).withBindingConditions([]string{"IsPrepare"}, []string{"BindingFailed"}),
				device(device2, nil, nil).withBindingConditions([]string{"IsPrepare2"}, []string{"BindingFailed2"}),
				device(device3, nil, nil).withBindingConditions([]string{"IsPrepare3"}, []string{"BindingFailed3"}),
			)),
			node: node(node1, region1),
			expectResults: []any{
				resourceapi.AllocationResult{
					Devices: resourceapi.DeviceAllocationResult{
						Results: []resourceapi.DeviceRequestAllocationResult{
							deviceRequestAllocationResultWithBindingConditions(req0, driverA, pool1, device1, []string{"IsPrepare"}, []string{"BindingFailed"}),
							deviceRequestAllocationResultWithBindingConditions(req0, driverA, pool1, device2, []string{"IsPrepare2"}, []string{"BindingFailed2"}),
							deviceRequestAllocationResultWithBindingConditions(req1, driverA, pool1, device3, []string{"IsPrepare3"}, []string{"BindingFailed3"}),
						},
					},
					NodeSelector: localNodeSelector(node1),
				},
			},
		},
		// 本用例验证同一池中带绑定条件与不带绑定条件的设备可以混合分配。
		// 特性开关：DeviceBindingAndStatus 开启。
		// 构造条件：device1 无绑定条件，device2 带 IsPrepare/BindingFailed；req0 与 req1 各请求 1 台。
		// 期望结果：req0 得到普通结果条目（device1），req1 得到带绑定条件的结果条目（device2）。
		"binding-conditions-with-and-without": {
			features: Features{
				DeviceBindingAndStatus: true,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil,
					request(req0, classA, 1),
					request(req1, classA, 1))),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, nil),
				device(device2, nil, nil).withBindingConditions([]string{"IsPrepare"}, []string{"BindingFailed"}),
			)),
			node: node(node1, region1),
			expectResults: []any{
				resourceapi.AllocationResult{
					Devices: resourceapi.DeviceAllocationResult{
						Results: []resourceapi.DeviceRequestAllocationResult{
							deviceAllocationResult(req0, driverA, pool1, device1, false),
							deviceRequestAllocationResultWithBindingConditions(req1, driverA, pool1, device2, []string{"IsPrepare"}, []string{"BindingFailed"}),
						},
					},
					NodeSelector: localNodeSelector(node1),
				},
			},
		},
		// 本用例验证绑定条件特性门关闭时，带绑定条件的设备不会被分配。
		// 特性开关：DeviceBindingAndStatus 显式设为 false。
		// 构造条件：唯一设备 device1 仍带有绑定条件；请求 1 台。
		// 期望结果：expectResults 为 nil，即没有任何可用分配。
		"binding-conditions-without-feature-gate": {
			features: Features{
				DeviceBindingAndStatus: false,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil, request(req0, classA, 1))),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, nil).withBindingConditions([]string{"IsPrepare"}, []string{"BindingFailed"}))),
			node:          node(node1, region1),
			expectResults: nil,
		},
		// 本用例验证 bindsToNode 设备受 slice 上节点标签选择器的限制。
		// 特性开关：DeviceBindingAndStatus 开启。
		// 构造条件：slice 使用 nodeLabelSelector(planet=earth)，device1 设置 bindsToNode(true)，目标节点 node1 带有该标签。
		// 期望结果：device1 被认为可用于 node1，req0 正常分配到 device1。
		"node-restriction": {
			features: Features{
				DeviceBindingAndStatus: true,
			},
			claimsToAllocate: objects(claimWithRequest(claim0, req0, classA)),
			classes:          objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, nodeLabelSelector(planetKey, planetValueEarth), pool1, driverA,
				device(device1, nil, nil).withBindsToNode(true))),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
			)},
		},
		// 本用例验证可分区设备的绑定条件同样写入分配结果。
		// 特性开关：PartitionableDevices 与 DeviceBindingAndStatus 同时开启。
		// 构造条件：device1 从 counterset 消费 4Gi 内存并带绑定条件，counterset 总量 8Gi；请求 1 台。
		// 期望结果：device1 被分配，结果条目保留 IsPrepare/BindingFailed 条件。
		"partitionable-devices-with-binding-conditions": {
			features: Features{
				PartitionableDevices:   true,
				DeviceBindingAndStatus: true,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil, request(req0, classA, 1)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, fromCounters, nil).
						withDeviceCounterConsumption(
							deviceCounterConsumption(counterSet1, map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							}),
						).
						withBindingConditions([]string{"IsPrepare"}, []string{"BindingFailed"}),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1, map[string]resource.Quantity{
						"memory": resource.MustParse("8Gi"),
					}),
				),
			),
			node: node(node1, region1),
			expectResults: []any{
				resourceapi.AllocationResult{
					Devices: resourceapi.DeviceAllocationResult{
						Results: []resourceapi.DeviceRequestAllocationResult{
							deviceRequestAllocationResultWithBindingConditions(req0, driverA, pool1, device1, []string{"IsPrepare"}, []string{"BindingFailed"}),
						},
					},
					NodeSelector: localNodeSelector(node1),
				},
			},
		},
		// 本用例验证多个可分区设备各自独立的绑定条件均被保留。
		// 特性开关：PartitionableDevices 与 DeviceBindingAndStatus 同时开启。
		// 构造条件：device1、device2 各从同一个 8Gi counterset 消费 4Gi，且分别带不同的绑定条件；请求 2 台。
		// 期望结果：两台设备都被分配，结果条目分别携带各自的条件组。
		"partitionable-devices-with-binding-conditions-multiple": {
			features: Features{
				PartitionableDevices:   true,
				DeviceBindingAndStatus: true,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil,
					request(req0, classA, 2),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, fromCounters, nil).
						withDeviceCounterConsumption(
							deviceCounterConsumption(counterSet1, map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							}),
						).
						withBindingConditions([]string{"IsPrepare"}, []string{"BindingFailed"}),
					device(device2, fromCounters, nil).
						withDeviceCounterConsumption(
							deviceCounterConsumption(counterSet1, map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							}),
						).
						withBindingConditions([]string{"IsPrepare2"}, []string{"BindingFailed2"}),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1, map[string]resource.Quantity{
						"memory": resource.MustParse("8Gi"),
					}),
				),
			),
			node: node(node1, region1),
			expectResults: []any{
				resourceapi.AllocationResult{
					Devices: resourceapi.DeviceAllocationResult{
						Results: []resourceapi.DeviceRequestAllocationResult{
							deviceRequestAllocationResultWithBindingConditions(req0, driverA, pool1, device1, []string{"IsPrepare"}, []string{"BindingFailed"}),
							deviceRequestAllocationResultWithBindingConditions(req0, driverA, pool1, device2, []string{"IsPrepare2"}, []string{"BindingFailed2"}),
						},
					},
					NodeSelector: localNodeSelector(node1),
				},
			},
		},
		// 本用例验证部分设备无绑定条件时，结果条目按设备实际情况分别生成。
		// 特性开关：PartitionableDevices 与 DeviceBindingAndStatus 同时开启。
		// 构造条件：请求 3 台；device1 消费 2Gi 带一组条件，device2 消费 2Gi 无额外字段，device3 消费 4Gi 带另一组条件；counterset 总量 8Gi。
		// 期望结果：三条分配结果依次携带 device1、device2（nil 条件）、device3 的条件，NodeSelector 一致。
		"partitionable-devices-with-binding-conditions-some-devices-no-conditions": {
			features: Features{
				PartitionableDevices:   true,
				DeviceBindingAndStatus: true,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil,
					request(req0, classA, 3),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, fromCounters, nil).
						withDeviceCounterConsumption(
							deviceCounterConsumption(counterSet1, map[string]resource.Quantity{
								"memory": resource.MustParse("2Gi"),
							}),
						).
						withBindingConditions([]string{"IsPrepare"}, []string{"BindingFailed"}),
					device(device2, fromCounters, nil).
						withDeviceCounterConsumption(
							deviceCounterConsumption(counterSet1, map[string]resource.Quantity{
								"memory": resource.MustParse("2Gi"),
							}),
						),
					device(device3, fromCounters, nil).
						withDeviceCounterConsumption(
							deviceCounterConsumption(counterSet1, map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							}),
						).
						withBindingConditions([]string{"IsReady"}, []string{"BindingTimeout"}),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1, map[string]resource.Quantity{
						"memory": resource.MustParse("8Gi"),
					}),
				),
			),
			node: node(node1, region1),
			expectResults: []any{
				resourceapi.AllocationResult{
					Devices: resourceapi.DeviceAllocationResult{
						Results: []resourceapi.DeviceRequestAllocationResult{
							deviceRequestAllocationResultWithBindingConditions(req0, driverA, pool1, device1, []string{"IsPrepare"}, []string{"BindingFailed"}),
							deviceRequestAllocationResultWithBindingConditions(req0, driverA, pool1, device2, nil, nil),
							deviceRequestAllocationResultWithBindingConditions(req0, driverA, pool1, device3, []string{"IsReady"}, []string{"BindingTimeout"}),
						},
					},
					NodeSelector: localNodeSelector(node1),
				},
			},
		},
		// 本用例验证绑定条件信息在跨越多个 slice 与 counterset 分组时仍然正确。
		// 特性开关：PartitionableDevices 与 DeviceBindingAndStatus 同时开启。
		// 构造条件：两组 counterset 分别由 slice4/slice3 与 slice2/slice1 提供；device1 带绑定条件，device2 无；请求 2 台。
		// 期望结果：结果中 device2 为无额外字段条目，device1 为带绑定条件条目，各自量纲正确。
		"partitionable-devices-with-binding-conditions-multi-slices": {
			features: Features{
				PartitionableDevices:   true,
				DeviceBindingAndStatus: true,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil, request(req0, classA, 2)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice4, node1, resourcePool(pool1, 4), driverA,
					device(device1, fromCounters, nil).
						withDeviceCounterConsumption(
							deviceCounterConsumption(counterSet1, map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							}),
						).
						withBindingConditions([]string{"IsPrepare"}, []string{"BindingFailed"}),
				),
				sliceWithCounterSets(slice3, node1, resourcePool(pool1, 4), driverA,
					counterSet(counterSet1, map[string]resource.Quantity{
						"memory": resource.MustParse("8Gi"),
					}),
				),
				sliceWithDevices(slice2, node1, resourcePool(pool1, 4), driverA,
					device(device2, fromCounters, nil).
						withDeviceCounterConsumption(
							deviceCounterConsumption(counterSet2, map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							}),
						),
				),
				sliceWithCounterSets(slice1, node1, resourcePool(pool1, 4), driverA,
					counterSet(counterSet2, map[string]resource.Quantity{
						"memory": resource.MustParse("8Gi"),
					}),
				),
			),
			node: node(node1, region1),
			expectResults: []any{
				resourceapi.AllocationResult{
					Devices: resourceapi.DeviceAllocationResult{
						Results: []resourceapi.DeviceRequestAllocationResult{
							deviceRequestAllocationResultWithBindingConditions(req0, driverA, pool1, device2, nil, nil),
							deviceRequestAllocationResultWithBindingConditions(req0, driverA, pool1, device1, []string{"IsPrepare"}, []string{"BindingFailed"}),
						},
					},
					NodeSelector: localNodeSelector(node1),
				},
			},
		},
		// 本用例验证带 NoSchedule 污点且无容忍的设备在满足数量要求时仍导致分配失败。
		// 特性开关：PartitionableDevices、DeviceBindingAndStatus、DeviceTaints 同时开启。
		// 构造条件：device1 消费 4Gi、带绑定条件并打上 NoSchedule 污点，device2 消费 4Gi 无污点；请求 2 台，总量 8Gi。
		// 期望结果：expectResults 为 nil，因为只有 device2 可用，凑不满两台。
		"partitionable-devices-with-binding-conditions-and-taints": {
			features: Features{
				PartitionableDevices:   true,
				DeviceBindingAndStatus: true,
				DeviceTaints:           true,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil, request(req0, classA, 2)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, fromCounters, nil).
						withDeviceCounterConsumption(
							deviceCounterConsumption(counterSet1, map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							}),
						).
						withBindingConditions([]string{"IsPrepare"}, []string{"BindingFailed"}).
						withTaints(resourceapi.DeviceTaint{
							Key:    "key1",
							Value:  "value1",
							Effect: resourceapi.DeviceTaintEffectNoSchedule,
						}),
					device(device2, fromCounters, nil).
						withDeviceCounterConsumption(
							deviceCounterConsumption(counterSet1, map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							}),
						),
				),
				sliceWithCounterSets(slice2, node1, pool1, driverA,
					counterSet(counterSet1, map[string]resource.Quantity{
						"memory": resource.MustParse("8Gi"),
					}),
				),
			),
			node:          node(node1, region1),
			expectResults: nil,
		},
		// 本用例验证添加容忍后带污点的可分区设备可以分配，且容忍信息进入结果。
		// 特性开关：PartitionableDevices、DeviceBindingAndStatus、DeviceTaints 同时开启。
		// 构造条件：claim 增加 tolerationNoScheduleKey1 容忍；device1 仍带污点和绑定条件，device2 为普通设备；请求 2 台。
		// 期望结果：device1 与 device2 都被分配，两个结果条目均记录该容忍信息。
		"partitionable-devices-with-binding-conditions-and-taints-tolerated": {
			features: Features{
				PartitionableDevices:   true,
				DeviceBindingAndStatus: true,
				DeviceTaints:           true,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil, request(req0, classA, 2)).withTolerations(tolerationNoScheduleKey1),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, fromCounters, nil).
						withDeviceCounterConsumption(
							deviceCounterConsumption(counterSet1, map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							}),
						).
						withBindingConditions([]string{"IsPrepare"}, []string{"BindingFailed"}).
						withTaints(resourceapi.DeviceTaint{
							Key:    "key1",
							Value:  "value1",
							Effect: resourceapi.DeviceTaintEffectNoSchedule,
						}),
					device(device2, fromCounters, nil).
						withDeviceCounterConsumption(
							deviceCounterConsumption(counterSet1, map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							}),
						),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1, map[string]resource.Quantity{
						"memory": resource.MustParse("8Gi"),
					}),
				),
			),
			node: node(node1, region1),
			expectResults: []any{
				resourceapi.AllocationResult{
					Devices: resourceapi.DeviceAllocationResult{
						Results: []resourceapi.DeviceRequestAllocationResult{
							deviceRequestAllocationResultWithBindingConditions(req0, driverA, pool1, device1, []string{"IsPrepare"}, []string{"BindingFailed"}, tolerationNoScheduleKey1),
							deviceRequestAllocationResultWithBindingConditions(req0, driverA, pool1, device2, nil, nil, tolerationNoScheduleKey1),
						},
					},
					NodeSelector: localNodeSelector(node1),
				},
			},
		},
		// 本用例验证 ConsumableCapacity 特性关闭时，带容量请求的请求直接报错。
		// 特性开关：仅开启 DeviceBindingAndStatus 以强制走实验分配器，ConsumableCapacity 未开启。
		// 构造条件：req0 通过 withCapacityRequest 声明容量请求；提供一个普通设备。
		// 期望结果：expectError 匹配包含『DRAConsumableCapacity feature is disabled』的明确错误信息，expectResults 为 nil。
		"consumable-capacity-disabled-feature": {
			features: Features{
				DeviceBindingAndStatus: true, // add to forcefully use experimenting allocator
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(deviceRequest(req0, classA, 1).withCapacityRequest(ptr.To(one))),
			),
			classes:       objects(class(classA, driverA)),
			slices:        unwrap(sliceWithOneDevice(slice1, node1, pool1, driverA)),
			node:          node(node1, region1),
			expectResults: nil,
			expectError:   gomega.MatchError(gomega.ContainSubstring("claim claim-0, request req-0: has capacity requests, but the DRAConsumableCapacity feature is disabled")),
		},
		// 本用例验证特性关闭时的报错同样适用于优先列表中的子请求。
		// 特性开关：PrioritizedList 与 DeviceBindingAndStatus 开启，ConsumableCapacity 未开启。
		// 构造条件：req0 的两个子请求 subReq0、subReq1 都携带容量请求。
		// 期望结果：报错信息指向 subReq-0 并提示特性被禁用，而不是静默跳过。
		"consumable-capacity-disabled-feature-with-prioritized-list": {
			features: Features{
				PrioritizedList:        true,
				DeviceBindingAndStatus: true, // add to forcefully use experimenting allocator
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					requestWithPrioritizedList(
						req0,
						subRequest(subReq0, classA, 1).withCapacityRequest(ptr.To(one)),
						subRequest(subReq1, classA, 1).withCapacityRequest(ptr.To(one)),
					),
				),
			),
			classes:       objects(class(classA, driverA)),
			slices:        unwrap(sliceWithOneDevice(slice1, node1, pool1, driverA)),
			node:          node(node1, region1),
			expectResults: nil,
			expectError:   gomega.MatchError(gomega.ContainSubstring("claim claim-0, subrequest subReq-0: has capacity requests, but the DRAConsumableCapacity feature is disabled")),
		},
		// 本用例验证设备声明了容量但缺少策略且请求带容量时无法满足。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：设备 allowMultipleAllocations 且声明 capacity1=one，但没有 CapacityRequestPolicy；req0 带容量请求。
		// 期望结果：expectResults 为空切片，即没有可行的分配。
		"consumable-capacity-multi-allocatable-device-with-missing-capacity-request": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(deviceRequest(req0, classA, 1).withCapacityRequest(ptr.To(one))),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, map[resourceapi.QualifiedName]resource.Quantity{capacity1: one}, nil).withAllowMultipleAllocations(),
				),
			),
			node:          node(node1, region1),
			expectResults: []any{},
		},
		// 本用例验证上一场景在 AllocationModeAll 下同样失败。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：请求使用 allDeviceRequest 且带容量请求；DeviceClass 允许多分配，设备有容量但无请求策略。
		// 期望结果：expectResults 为空切片。
		"consumable-capacity-multi-allocatable-device-allocation-mode-all-with-missing-capacity-request": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(allDeviceRequest(req0, classA).withCapacityRequest(ptr.To(one))),
			),
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, map[resourceapi.QualifiedName]resource.Quantity{capacity1: one}, nil).withAllowMultipleAllocations(),
				),
			),
			node:          node(node1, region1),
			expectResults: []any{},
		},
		// 本用例验证设备未定义容量且请求也不带容量时，多台设备可共享同一设备份额。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：两个 claim 各请求 1 台，DeviceClass 允许多分配，设备 allowMultipleAllocations 且无容量定义。
		// 期望结果：两个结果都指向 device1，均使用 fixedShareID 且消耗容量映射为 nil（不消耗任何容量）。
		"consumable-capacity-multi-allocatable-device-without-capacity-without-capacity-request": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(deviceRequest(req0, classA, 1)),
				claim(claim1).withRequests(deviceRequest(req0, classA, 1)),
			),
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil).withAllowMultipleAllocations(),
				),
			),
			node: node(node1, region1),
			expectResults: []any{ // both share the same device; no capacity is consumed since none is defined.
				allocationResult(
					localNodeSelector(node1),
					deviceRequestAllocationResult(req0, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, nil),
				),
				allocationResult(
					localNodeSelector(node1),
					deviceRequestAllocationResult(req0, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, nil),
				),
			},
		},
		// 本用例验证无请求策略且请求不带容量时，不同请求分别占用不同设备。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：两台可多分配设备各声明 capacity0=one，两个 claim 各请求 1 台且无容量请求。
		// 期望结果：分别分配到 device1、device2，各自消耗 capacity0 的 one（完整容量）。
		"consumable-capacity-multi-allocatable-device-without-policy-without-capacity-request": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(deviceRequest(req0, classA, 1)),
				claim(claim1).withRequests(deviceRequest(req0, classA, 1)),
			),
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, map[resourceapi.QualifiedName]resource.Quantity{capacity0: one}, nil).withAllowMultipleAllocations(),
					device(device2, map[resourceapi.QualifiedName]resource.Quantity{capacity0: one}, nil).withAllowMultipleAllocations(),
				),
			),
			node: node(node1, region1),
			expectResults: []any{ // both should get full capacity for different devices
				allocationResult(
					localNodeSelector(node1),
					deviceRequestAllocationResult(req0, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: one}),
				),
				allocationResult(
					localNodeSelector(node1),
					deviceRequestAllocationResult(req0, driverA, pool1, device2).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: one}),
				),
			},
		},
		// 本用例验证单台设备容量不足以覆盖两个请求时无法同时满足。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：唯一可多分配设备声明 capacity0=two；两个 claim 各请求 1 台且无容量请求。
		// 期望结果：expectResults 为空，因为两个请求各需完整容量 two，设备只够一个。
		"consumable-capacity-multi-allocatable-device-without-policy-without-capacity-request-block": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(deviceRequest(req0, classA, 1)),
				claim(claim1).withRequests(deviceRequest(req0, classA, 1)),
			),
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, map[resourceapi.QualifiedName]resource.Quantity{capacity0: two}, nil).withAllowMultipleAllocations(),
				),
			),
			node:          node(node1, region1),
			expectResults: []any{}, // should not satisfy both
		},
		// 本用例验证明确的小容量请求可以在同一台可多分配设备上共存。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：设备 capacity0=two；两个请求各显式请求 capacity0=one。
		// 期望结果：两个结果都落在 device1 上，各自消耗 capacity0 的 one。
		"consumable-capacity-multi-allocatable-device-without-policy-with-capacity-request": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(deviceRequest(req0, classA, 1).withCapacityRequest(ptr.To(one))),
				claim(claim1).withRequests(deviceRequest(req0, classA, 1).withCapacityRequest(ptr.To(one))),
			),
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, map[resourceapi.QualifiedName]resource.Quantity{capacity0: two}, nil).withAllowMultipleAllocations(),
				),
			),
			node: node(node1, region1),
			expectResults: []any{
				allocationResult(
					localNodeSelector(node1),
					deviceRequestAllocationResult(req0, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: one}),
				),
				allocationResult(
					localNodeSelector(node1),
					deviceRequestAllocationResult(req0, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: one}),
				),
			},
		},
		// 本用例验证请求不带容量时，由设备请求策略的默认值决定消耗量。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：设备带 CapacityRequestPolicyRange（默认请求为 two），两个请求都不带容量请求。
		// 期望结果：两个请求都应用到默认请求值 two，各自在 device1 上消耗 capacity0 的 two。
		"consumable-capacity-multi-allocatable-device-with-policy-without-capacity-request": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil, request(req0, classA, 1)),
				claimWithRequests(claim1, nil, request(req0, classA, 1)),
			),
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil).withAllowMultipleAllocations().withCapacityRequestPolicyRange(map[resourceapi.QualifiedName]resource.Quantity{capacity0: four}),
				),
			),
			node: node(node1, region1),

			expectResults: []any{ // both requests applied the default request (two)
				allocationResult(
					localNodeSelector(node1),
					deviceRequestAllocationResult(req0, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: two}),
				),
				allocationResult(
					localNodeSelector(node1),
					deviceRequestAllocationResult(req0, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: two}),
				),
			},
		},
		// 本用例验证合法值策略的默认请求值为零时的处理。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：设备策略 withCapacityRequestPolicyValidValues 默认 zero、候选值 one；两个请求均无容量请求。
		// 期望结果：两个请求各自消耗 capacity0 的 zero，仍共享同一设备。
		"consumable-capacity-multi-allocatable-device-with-zero-request-policy": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil, request(req0, classA, 1)),
				claimWithRequests(claim1, nil, request(req0, classA, 1)),
			),
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil).withAllowMultipleAllocations().withCapacityRequestPolicyValidValues(zero, map[resourceapi.QualifiedName]resource.Quantity{capacity0: one}, nil),
				),
			),
			node: node(node1, region1),

			expectResults: []any{
				allocationResult(
					localNodeSelector(node1),
					deviceRequestAllocationResult(req0, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: zero}),
				),
				allocationResult(
					localNodeSelector(node1),
					deviceRequestAllocationResult(req0, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: zero}),
				),
			},
		},
		// 本用例验证请求的容量值不在合法值列表中时分配失败。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：req0 请求 capacity0=one，而设备策略只接受 zero。
		// 期望结果：expectResults 为空切片。
		"consumable-capacity-multi-allocatable-device-violates-zero-request-policy": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(deviceRequest(req0, classA, 1).withCapacityRequest(ptr.To(one))),
			),
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil).withAllowMultipleAllocations().withCapacityRequestPolicyValidValues(zero, map[resourceapi.QualifiedName]resource.Quantity{capacity0: one}, nil),
				),
			),
			node: node(node1, region1),

			expectResults: []any{},
		},
		// 本用例验证请求容量低于范围策略下限时向上取整到默认最小值。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：两个请求各请求 capacity0=one，设备范围策略的最小/默认值为 two。
		// 期望结果：两个请求都分配到 device1，各自消耗 capacity0 的 two（向上取整到默认下限）。
		"consumable-capacity-multi-allocatable-device-with-range-policy-with-capacity-request": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(deviceRequest(req0, classA, 1).withCapacityRequest(ptr.To(one))),
				claim(claim1).withRequests(deviceRequest(req0, classA, 1).withCapacityRequest(ptr.To(one))),
			),
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil).withAllowMultipleAllocations().withCapacityRequestPolicyRange(map[resourceapi.QualifiedName]resource.Quantity{capacity0: four}),
				),
			),
			node: node(node1, region1),
			expectResults: []any{ // both requests allocated to the same device
				allocationResult(
					localNodeSelector(node1),
					// capacity must be rounded up to default minimum (two)
					deviceRequestAllocationResult(req0, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: two}),
				),
				allocationResult(
					localNodeSelector(node1),
					// capacity must be rounded up to default minimum (two)
					deviceRequestAllocationResult(req0, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: two}),
				),
			},
		},
		// 本用例验证请求容量按范围策略的 Step 向上取整。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：请求 capacity0=3，设备策略范围 (min=2, step=2, max=4)。
		// 期望结果：消耗容量按 Step 取整为 2+2=4，即 capacity0 的 four。
		"consumable-capacity-multi-allocatable-device-with-range-policy-step": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(deviceRequest(req0, classA, 1).withCapacityRequest(resource.NewQuantity(3, resource.BinarySI))),
			),
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil).withAllowMultipleAllocations().withCapacityRequestPolicyRange(map[resourceapi.QualifiedName]resource.Quantity{capacity0: four}),
				),
			),
			node: node(node1, region1),
			expectResults: []any{
				allocationResult(
					localNodeSelector(node1),
					// capacity must be rounded up with Step (3 to 2+2 = 4)
					deviceRequestAllocationResult(req0, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: four}),
				),
			},
		},
		// 本用例验证超过 MaxInt64 的容量请求在整数取整路径上直接中止。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：请求值 2^64（超过 int64），设备带 Step 范围策略；英文注释说明 Quantity.Value() 会回绕。
		// 期望结果：expectError 匹配包含『cannot be represented』的错误，不静默跳过设备。
		"consumable-capacity-range-policy-request-above-int64-rejected": {
			features: Features{
				ConsumableCapacity: true,
			},
			// A capacity request above MaxInt64 cannot be rounded on the integer Step path
			// without reading it through Quantity.Value(), which wraps. The request is not
			// representable, so allocation aborts with a specific error instead of skipping.
			claimsToAllocate: objects(
				claim(claim0).withRequests(deviceRequest(req0, classA, 1).withCapacityRequest(new(resource.MustParse("18446744073709551616")))),
			),
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil).withAllowMultipleAllocations().withCapacityRequestPolicyRange(map[resourceapi.QualifiedName]resource.Quantity{capacity0: four}),
				),
			),
			node:        node(node1, region1),
			expectError: gomega.MatchError(gomega.ContainSubstring("cannot be represented")),
		},
		// 本用例验证取整后超过 MaxInt64 的值同样被拒绝。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：请求值恰为 MaxInt64，按 Step 取整会越过上界并回绕为负数。
		// 期望结果：expectError 匹配『cannot be represented』，而非按回绕值分配。
		"consumable-capacity-range-policy-rounded-value-overflow-rejected": {
			features: Features{
				ConsumableCapacity: true,
			},
			// Rounding a MaxInt64 request up to the next Step multiple passes MaxInt64 and
			// would wrap to a negative value. The rounded value is not representable, so
			// allocation aborts with a specific error instead of acting on the wrapped read.
			claimsToAllocate: objects(
				claim(claim0).withRequests(deviceRequest(req0, classA, 1).withCapacityRequest(new(resource.MustParse("9223372036854775807")))),
			),
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil).withAllowMultipleAllocations().withCapacityRequestPolicyRange(map[resourceapi.QualifiedName]resource.Quantity{capacity0: four}),
				),
			),
			node:        node(node1, region1),
			expectError: gomega.MatchError(gomega.ContainSubstring("cannot be represented")),
		},
		// 本用例验证溢出是 fail-closed 行为：中止分配，而不是跳过该设备再试别的设备。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：device1 带 Step 范围导致溢出；device2 无 Step 且容量巨大，本可精确匹配同一请求。
		// 期望结果：仍以『cannot be represented』错误中止，不会退而分配 device2，从而避免用户可控越界值引发回溯。
		"consumable-capacity-range-policy-overflow-aborts-not-skip": {
			features: Features{
				ConsumableCapacity: true,
			},
			// device-1 has a Step range, so a request above MaxInt64 overflows its integer
			// rounding. device-2 has no Step and could satisfy the same request by exact
			// comparison, but a not-representable request is fail-closed: allocation aborts
			// rather than skipping device-1 to allocate device-2. Aborting on the first
			// overflowing device is what stops the backtracking a user-controlled
			// out-of-range request would otherwise force.
			claimsToAllocate: objects(
				claim(claim0).withRequests(deviceRequest(req0, classA, 1).withCapacityRequest(new(resource.MustParse("18446744073709551616")))),
			),
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil).withAllowMultipleAllocations().withCapacityRequestPolicyRange(map[resourceapi.QualifiedName]resource.Quantity{capacity0: four}),
					device(device2, map[resourceapi.QualifiedName]resource.Quantity{capacity0: resource.MustParse("36893488147419103232")}, nil).withAllowMultipleAllocations(),
				),
			),
			node:        node(node1, region1),
			expectError: gomega.MatchError(gomega.ContainSubstring("cannot be represented")),
		},
		// 本用例验证 AllocationModeAll 的预检阶段同样会因越界容量请求中止。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：请求使用 allDeviceRequest 并携带 2^64 的容量请求；设备带 Step 范围策略。
		// 期望结果：expectError 匹配『cannot be represented』，不会把溢出设备从集合中悄悄剔除后分配其余设备。
		"consumable-capacity-range-policy-allocation-mode-all-overflow-aborts": {
			features: Features{
				ConsumableCapacity: true,
			},
			// The all-devices pre-scan in validateDeviceRequest validates capacity too. An
			// out-of-range request must abort there, not silently drop the overflowing
			// device from the set and allocate whatever remains. The count-mode cases above
			// exercise allocateOne; this one exercises the AllocationModeAll pre-scan.
			claimsToAllocate: objects(
				claim(claim0).withRequests(allDeviceRequest(req0, classA).withCapacityRequest(new(resource.MustParse("18446744073709551616")))),
			),
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil).withAllowMultipleAllocations().withCapacityRequestPolicyRange(map[resourceapi.QualifiedName]resource.Quantity{capacity0: four}),
				),
			),
			node:        node(node1, region1),
			expectError: gomega.MatchError(gomega.ContainSubstring("cannot be represented")),
		},
		// 本用例验证遗留对象中的负容量请求必须中止分配。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：请求值为 -5（假定来自校验更宽松时存入的旧对象），设备声明 capacity0=10。
		// 期望结果：expectError 匹配包含『negative』的错误，避免记录负消耗导致超分。
		"consumable-capacity-negative-request-aborts": {
			features: Features{
				ConsumableCapacity: true,
			},
			// A stored request or slice from before stricter validation could carry a
			// negative capacity, which admission does not re-check on existing objects. A
			// negative value must abort rather than record negative consumed capacity and
			// over-allocate the device.
			claimsToAllocate: objects(
				claim(claim0).withRequests(deviceRequest(req0, classA, 1).withCapacityRequest(new(resource.MustParse("-5")))),
			),
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, map[resourceapi.QualifiedName]resource.Quantity{capacity0: resource.MustParse("10")}, nil).withAllowMultipleAllocations(),
				),
			),
			node:        node(node1, region1),
			expectError: gomega.MatchError(gomega.ContainSubstring("negative")),
		},
		// 本用例验证合法值策略中请求值恰好命中候选值时直接采用该值。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：请求 capacity0=2，设备策略默认 one、候选值列表含 two。
		// 期望结果：分配成功，消耗容量恰为 capacity0 的 two，无需取整。
		"consumable-capacity-multi-allocatable-device-with-valid-values-policy": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(deviceRequest(req0, classA, 1).withCapacityRequest(resource.NewQuantity(2, resource.BinarySI))),
			),
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil).withAllowMultipleAllocations().withCapacityRequestPolicyValidValues(one, map[resourceapi.QualifiedName]resource.Quantity{capacity0: four},
						[]resource.Quantity{two}),
				),
			),
			node: node(node1, region1),
			expectResults: []any{
				allocationResult(
					localNodeSelector(node1),
					deviceRequestAllocationResult(req0, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: two}),
				),
			},
		},
		// 本用例验证请求值不在合法值列表时向上取整到最近的合法值。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：请求 capacity0=2，候选值列表只有 three，因此必须显式加入该值。
		// 期望结果：消耗容量取整为 capacity0 的 three。
		"consumable-capacity-multi-allocatable-device-with-valid-values-policy-round-up": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(deviceRequest(req0, classA, 1).withCapacityRequest(resource.NewQuantity(2, resource.BinarySI))),
			),
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil).withAllowMultipleAllocations().withCapacityRequestPolicyValidValues(one, map[resourceapi.QualifiedName]resource.Quantity{capacity0: three},
						[]resource.Quantity{three}), // capacity value must be explicitly added
				),
			),
			node: node(node1, region1),
			expectResults: []any{
				allocationResult(
					localNodeSelector(node1),
					deviceRequestAllocationResult(req0, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: three}),
				),
			},
		},
		// 本用例验证两个请求的容量总和超过设备可共享上限时失败。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：两个请求分别请求 one 与 two，设备范围策略上限为 two。
		// 期望结果：expectResults 为空切片，无法同时满足。
		"consumable-capacity-multi-allocatable-device-with-exceeded-consumable-capacity-request": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(deviceRequest(req0, classA, 1).withCapacityRequest(ptr.To(one))),
				claim(claim1).withRequests(deviceRequest(req0, classA, 1).withCapacityRequest(ptr.To(two))),
			),
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil).withAllowMultipleAllocations().withCapacityRequestPolicyRange(map[resourceapi.QualifiedName]resource.Quantity{capacity0: two}),
				),
			),
			node: node(node1, region1),

			expectResults: []any{},
		},
		// 本用例验证请求量超过默认最大共享策略上限时失败。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：请求 capacity0=6，设备范围策略给出的最大值为 10，但默认最大共享请求上限为 4。
		// 期望结果：expectResults 为空——即使 6 小于 10，超过默认上限 4 仍不分配。
		"consumable-capacity-multi-allocatable-device-with-request-over-max-sharing-policy": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(deviceRequest(req0, classA, 1).withCapacityRequest(resource.NewQuantity(6, resource.BinarySI))),
			),
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil).withAllowMultipleAllocations().withCapacityRequestPolicyRange(map[resourceapi.QualifiedName]resource.Quantity{capacity0: *resource.NewQuantity(10, resource.BinarySI)}),
				),
			),
			node: node(node1, region1),

			expectResults: []any{}, // default max requestPolicy is 4, should not allocate even though 6 < 10
		},
		// 本用例验证设备已有分配容量时，新请求只消耗剩余可用容量。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：allocatedCapacityDevices 记录 device1 已消耗 two；新请求 two，设备范围策略总量 four。
		// 期望结果：仍可分配到 device1，新结果消耗 capacity0 的 two。
		"consumable-capacity-multi-allocatable-device-with-allocated-capacity-use-remaining": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(deviceRequest(req0, classA, 1).withCapacityRequest(ptr.To(two))),
			),
			allocatedCapacityDevices: map[DeviceID]ConsumedCapacity{
				MakeDeviceID(driverA, pool1, device1): {
					capacity0: ptr.To(two),
				},
			},
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil).withAllowMultipleAllocations().withCapacityRequestPolicyRange(map[resourceapi.QualifiedName]resource.Quantity{capacity0: four}),
				),
			),
			node: node(node1, region1),

			expectResults: []any{
				allocationResult(
					localNodeSelector(node1),
					deviceRequestAllocationResult(req0, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: two}),
				),
			},
		},
		// 本用例验证设备已被占满后分配器转到下一台设备。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：device1 已消耗 two 且上限为 two；device2 同样上限 two；请求 two。
		// 期望结果：分配到 device2 并消耗 capacity0 的 two。
		"consumable-capacity-multi-allocatable-device-with-allocated-capacity-next-device": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(deviceRequest(req0, classA, 1).withCapacityRequest(ptr.To(two))),
			),
			allocatedCapacityDevices: map[DeviceID]ConsumedCapacity{
				MakeDeviceID(driverA, pool1, device1): {
					capacity0: ptr.To(two),
				},
			},
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil).withAllowMultipleAllocations().withCapacityRequestPolicyRange(map[resourceapi.QualifiedName]resource.Quantity{capacity0: two}),
					device(device2, nil, nil).withAllowMultipleAllocations().withCapacityRequestPolicyRange(map[resourceapi.QualifiedName]resource.Quantity{capacity0: two}),
				),
			),
			node: node(node1, region1),

			expectResults: []any{
				allocationResult(
					localNodeSelector(node1),
					deviceRequestAllocationResult(req0, driverA, pool1, device2).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: two}),
				),
			},
		},
		// 本用例验证容量分配与 matchAttribute 亲缘约束冲突时的回溯能力。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：claim0 带 stringAttribute 亲缘约束，req0/req1 各需两单位容量；device1/device2 的该属性值不同，其中 device2 上限较小；claim1 另有单台请求。
		// 期望结果：先尝试 device2 无法满足后回溯，req0 与 req1 均分配到 device1 各消耗 two，claim1 分配到 device2 消耗 two。
		"consumable-capacity-with-multi-allocatable-device-backtrack": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				// 2 requests with two per each
				claim(claim0).withConstraints(resourceapi.DeviceConstraint{MatchAttribute: &stringAttribute}).withRequests(
					deviceRequest(req0, classA, 1).withCapacityRequest(ptr.To(two)),
					deviceRequest(req1, classA, 1).withCapacityRequest(ptr.To(two)),
				),
				claim(claim1).withRequests(deviceRequest(req0, classA, 1).withCapacityRequest(ptr.To(two))),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device2, nil,
						map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
							"stringAttribute": {StringValue: ptr.To("stringAttributeValue1")},
						}).withAllowMultipleAllocations().withCapacityRequestPolicyRange(map[resourceapi.QualifiedName]resource.Quantity{capacity0: two}),
					device(device1, nil,
						map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
							"stringAttribute": {StringValue: ptr.To("stringAttributeValue2")},
						}).withAllowMultipleAllocations().withCapacityRequestPolicyRange(map[resourceapi.QualifiedName]resource.Quantity{capacity0: four}),
				),
			),
			node: node(node1, region1),
			expectResults: []any{
				allocationResult(localNodeSelector(node1),
					deviceRequestAllocationResult(req0, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: two}),
					deviceRequestAllocationResult(req1, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: two}),
				),
				allocationResult(localNodeSelector(node1), deviceRequestAllocationResult(req0, driverA, pool1, device2).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: two})),
			},
		},
		// 本用例验证优先列表子请求携带容量时按优先级逐步回退。
		// 特性开关：ConsumableCapacity 与 PrioritizedList 同时开启。
		// 构造条件：req0 的三个子请求分别请求容量 four、two 与不请求容量；设备范围策略上限为 two。
		// 期望结果：高容量子请求被跳过，最终选中不带超额容量的 req0SubReq1，在 device1 上消耗 capacity0 的 two。
		"consumable-capacity-multi-allocatable-device-with-subrequest": {
			features: Features{
				ConsumableCapacity: true,
				PrioritizedList:    true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					requestWithPrioritizedList(req0,
						subRequest(subReq0, classA, 1).withCapacityRequest(ptr.To(four)),
						subRequest(subReq1, classA, 1).withCapacityRequest(ptr.To(two)),
						subRequest(subReq1, classA, 1),
					),
				),
			),
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil).withAllowMultipleAllocations().withCapacityRequestPolicyRange(map[resourceapi.QualifiedName]resource.Quantity{capacity0: two}),
				),
			),
			node: node(node1, region1),
			expectResults: []any{
				allocationResult(
					localNodeSelector(node1),
					deviceRequestAllocationResult(req0SubReq1, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: two}),
				),
			},
		},
		// 本用例验证设备可共享容量已用尽时不再分配。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：device1 已消耗 two，范围策略上限也是 two；新请求仍要 two。
		// 期望结果：expectResults 为空切片。
		"consumable-capacity-multi-allocatable-device-with-no-available-consumable-capacity": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(deviceRequest(req0, classA, 1).withCapacityRequest(ptr.To(two))),
			),
			allocatedCapacityDevices: map[DeviceID]ConsumedCapacity{
				MakeDeviceID(driverA, pool1, device1): {
					capacity0: ptr.To(two),
				},
			},
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil).withAllowMultipleAllocations().withCapacityRequestPolicyRange(map[resourceapi.QualifiedName]resource.Quantity{capacity0: two}),
				),
			),
			node: node(node1, region1),

			expectResults: []any{},
		},
		// 本用例验证 All 模式会过滤掉完全未声明所需容量的设备。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：device1 声明 capacity0=two，device2 无容量声明，两者均 allowMultipleAllocations；请求以 All 模式要求 two。
		// 期望结果：只有 device1 被分配并消耗 two（英文注释：only match device is allocated）。
		"consumable-capacity-multi-allocatable-device-allocation-mode-all-filter-out-no-capacity": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(allDeviceRequest(req0, classA).withCapacityRequest(ptr.To(two))),
			),
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, map[resourceapi.QualifiedName]resource.Quantity{capacity0: two}, nil).withAllowMultipleAllocations(),
					device(device2, nil, nil).withAllowMultipleAllocations(),
				),
			),
			node: node(node1, region1),
			expectResults: []any{ // only match device is allocated
				allocationResult(
					localNodeSelector(node1),
					deviceRequestAllocationResult(req0, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: two}),
				),
			},
		},
		// 本用例验证 All 模式会过滤掉容量低于请求量的设备。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：device1 声明 capacity0=two，device2 仅 capacity0=one；All 模式请求 two。
		// 期望结果：只分配 device1 并消耗 two。
		"consumable-capacity-multi-allocatable-device-allocation-mode-all-filter-out-insufficient-capacity": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(allDeviceRequest(req0, classA).withCapacityRequest(ptr.To(two))),
			),
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, map[resourceapi.QualifiedName]resource.Quantity{capacity0: two}, nil).withAllowMultipleAllocations(),
					device(device2, map[resourceapi.QualifiedName]resource.Quantity{capacity0: one}, nil).withAllowMultipleAllocations(),
				),
			),
			node: node(node1, region1),
			expectResults: []any{ // only match device is allocated
				allocationResult(
					localNodeSelector(node1),
					deviceRequestAllocationResult(req0, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: two}),
				),
			},
		},
		// 本用例验证计数请求与 All 模式请求并存且总容量充足时的分配。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：req0 请求 1 台容量 one，req1 为 All 模式容量 one；device1 总量 two、device2 总量 one。
		// 期望结果：req0 得 device1，req1 同时得 device1 与 device2，三条结果各消耗 one。
		"consumable-capacity-multi-allocatable-device-allocation-mode-all-allocating-sufficient": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					deviceRequest(req0, classA, 1).withCapacityRequest(ptr.To(one)),
					allDeviceRequest(req1, classA).withCapacityRequest(ptr.To(one)),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, map[resourceapi.QualifiedName]resource.Quantity{capacity0: two}, nil).withAllowMultipleAllocations(),
					device(device2, map[resourceapi.QualifiedName]resource.Quantity{capacity0: one}, nil).withAllowMultipleAllocations(),
				),
			),
			node: node(node1, region1),
			expectResults: []any{
				allocationResult(
					localNodeSelector(node1),
					deviceRequestAllocationResult(req0, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: one}),
					deviceRequestAllocationResult(req1, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: one}),
					deviceRequestAllocationResult(req1, driverA, pool1, device2).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: one}),
				),
			},
		},
		// 本用例验证 All 模式在考虑已有占用后仍有足够容量时全部分配。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：device1 已消耗 one 且总量 two，device2 总量 one；请求 All 模式容量 one。
		// 期望结果：device1 与 device2 都被分配，各自再消耗 one。
		"consumable-capacity-multi-allocatable-device-allocation-mode-all-in-use-sufficient": {
			features: Features{
				ConsumableCapacity: true,
			},
			allocatedCapacityDevices: map[DeviceID]ConsumedCapacity{
				MakeDeviceID(driverA, pool1, device1): {
					capacity0: ptr.To(one),
				},
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					allDeviceRequest(req0, classA).withCapacityRequest(ptr.To(one)),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, map[resourceapi.QualifiedName]resource.Quantity{capacity0: two}, nil).withAllowMultipleAllocations(),
					device(device2, map[resourceapi.QualifiedName]resource.Quantity{capacity0: one}, nil).withAllowMultipleAllocations(),
				),
			),
			node: node(node1, region1),
			expectResults: []any{
				allocationResult(
					localNodeSelector(node1),
					deviceRequestAllocationResult(req0, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: one}),
					deviceRequestAllocationResult(req0, driverA, pool1, device2).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: one}),
				),
			},
		},
		// 本用例验证计数请求先占用容量后导致 All 模式容量不足时整体失败。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：req0 先请求 1 台容量 one，req1 为 All 模式容量 one；device1、device2 各仅有 one。
		// 期望结果：expectResults 为空切片。
		"consumable-capacity-multi-allocatable-device-allocation-mode-all-allocating-insufficient": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					deviceRequest(req0, classA, 1).withCapacityRequest(ptr.To(one)),
					allDeviceRequest(req1, classA).withCapacityRequest(ptr.To(one)),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, map[resourceapi.QualifiedName]resource.Quantity{capacity0: one}, nil).withAllowMultipleAllocations(),
					device(device2, map[resourceapi.QualifiedName]resource.Quantity{capacity0: one}, nil).withAllowMultipleAllocations(),
				),
			),
			node:          node(node1, region1),
			expectResults: []any{},
		},
		// 本用例验证 All 模式不会因某设备耗尽而退化为仅分配其余设备。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：device1 已消耗 one 且总量仅 one，device2 总量 one；请求 All 模式容量 one。
		// 期望结果：expectResults 为空——device1 仍匹配但已耗尽，All 失败且不只分配 device2。
		"consumable-capacity-multi-allocatable-device-allocation-mode-all-in-use-insufficient": {
			features: Features{
				ConsumableCapacity: true,
			},
			allocatedCapacityDevices: map[DeviceID]ConsumedCapacity{
				MakeDeviceID(driverA, pool1, device1): {
					capacity0: ptr.To(one),
				},
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					allDeviceRequest(req0, classA).withCapacityRequest(ptr.To(one)),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, map[resourceapi.QualifiedName]resource.Quantity{capacity0: one}, nil).withAllowMultipleAllocations(),
					device(device2, map[resourceapi.QualifiedName]resource.Quantity{capacity0: one}, nil).withAllowMultipleAllocations(),
				),
			),
			node:          node(node1, region1),
			expectResults: []any{}, // device-1 capacity-exhausted but still matches; All fails, not device-2 only
		},
		// 本用例验证独占设备（不允许多分配）也可以满足容量请求。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：DeviceClass 关闭 allowMultipleAllocations；设备声明 capacity0=one，请求 one。
		// 期望结果：按普通独占方式分配 device1，结果不含共享份额标识。
		"consumable-capacity-dedicated-device-with-consumable-capacity-request": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(deviceRequest(req0, classA, 1).withCapacityRequest(ptr.To(one))),
			),
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, false)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, map[resourceapi.QualifiedName]resource.Quantity{capacity0: one}, nil),
				),
			),
			node: node(node1, region1),

			expectResults: []any{
				allocationResult(
					localNodeSelector(node1),
					deviceAllocationResult(req0, driverA, pool1, device1, false),
				),
			},
		},
		// 本用例验证独占设备不能同时满足多个容量请求。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：DeviceClass 关闭多分配；设备范围策略上限 two，两个请求各 one。
		// 期望结果：expectResults 为空切片。
		"consumable-capacity-dedicated-device-with-multiple-consumable-capacity-request": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(deviceRequest(req0, classA, 1).withCapacityRequest(ptr.To(one))),
				claim(claim1).withRequests(deviceRequest(req0, classA, 1).withCapacityRequest(ptr.To(one))),
			),
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, false)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil).withCapacityRequestPolicyRange(map[resourceapi.QualifiedName]resource.Quantity{capacity0: two}),
				),
			),
			node:          node(node1, region1),
			expectResults: []any{},
		},
		// 本用例验证超大容量请求的取整过程不会修改原始请求对象。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：req0 请求 two、req1 请求 10^21；设备 capacity0 值为 10^21+2，策略默认 10^21 且最小值为 two。
		// 期望结果：两个请求都分配到 device1，消耗容量分别为 two 与 10^21；请求对象保持不变。
		"consumable-capacity-requests-are-not-modified": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					deviceRequest(req0, classA, 1).withCapacityRequest(ptr.To(two)),
					deviceRequest(req1, classA, 1).withCapacityRequest(ptr.To(resource.MustParse("1000000000000000000000"))),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
						capacity0: {
							Value: resource.MustParse("1000000000000000000002"),
							RequestPolicy: &resourceapi.CapacityRequestPolicy{
								Default: ptr.To(resource.MustParse("1000000000000000000000")),
								ValidRange: &resourceapi.CapacityRequestPolicyRange{
									Min: &two,
								},
							},
						},
					}, nil).withAllowMultipleAllocations(),
				),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceRequestAllocationResult(req0, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: two}),
				deviceRequestAllocationResult(req1, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: resource.MustParse("1000000000000000000000")}),
			)},
		},
		// 本用例验证 DeviceClass 不允许多分配时会排除声明了 allowMultipleAllocations 的设备。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：classA 的 allowMultipleAllocations 为 false，而设备自身带 allowMultipleAllocations 且有容量。
		// 期望结果：expectResults 为空切片。
		"allow-multiple-allocations-exclude-multi-allocatable-device-by-class-selector": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(deviceRequest(req0, classA, 1).withCapacityRequest(ptr.To(one))),
			),
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, false)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, map[resourceapi.QualifiedName]resource.Quantity{capacity0: one}, nil).withAllowMultipleAllocations(),
				),
			),
			node: node(node1, region1),

			expectResults: []any{},
		},
		// 本用例验证已被独占分配的设备不再参与多分配。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：allocatedDevices 已包含 device1；DeviceClass 允许多分配且设备也允许。
		// 期望结果：expectResults 为空切片。
		"allow-multiple-allocations-not-allocate-multi-allocatable-device-which-already-dedicated": {
			features: Features{
				ConsumableCapacity: true,
			},
			allocatedDevices: []DeviceID{
				MakeDeviceID(driverA, pool1, device1),
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(deviceRequest(req0, classA, 1).withCapacityRequest(ptr.To(one))),
			),
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil).withAllowMultipleAllocations().withCapacityRequestPolicyRange(map[resourceapi.QualifiedName]resource.Quantity{capacity0: two}),
				),
			),
			node: node(node1, region1),

			expectResults: []any{},
		},
		// 本用例验证设备从可多分配改为独占后，已有容量分配记录会阻止新分配。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：allocatedCapacityDevices 记录 device1 已消耗 one；设备不再 allowMultipleAllocations，仅保留范围策略。
		// 期望结果：expectResults 为空切片。
		"allow-multiple-allocations-not-allocate-dedicated-device-which-change-from-multi-allocatable": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(deviceRequest(req0, classA, 1).withCapacityRequest(ptr.To(one))),
			),
			allocatedCapacityDevices: map[DeviceID]ConsumedCapacity{
				MakeDeviceID(driverA, pool1, device1): {
					capacity0: ptr.To(one),
				},
			},
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil).withCapacityRequestPolicyRange(map[resourceapi.QualifiedName]resource.Quantity{capacity0: two}),
				),
			),
			node: node(node1, region1),

			expectResults: []any{},
		},
		// 本用例验证可分区设备、多分配与优先列表三者组合时的选择。
		// 特性开关：PrioritizedList、PartitionableDevices、ConsumableCapacity 同时开启。
		// 构造条件：req0 请求 1 台；req1 的优先列表先按容量大于等于 6Gi 选择、再退到 4Gi；三台设备从同一 counterset 消费 4Gi/6Gi/4Gi，均 allowMultipleAllocations。
		// 期望结果：req0 得 device1 消耗 4Gi，req1 的次选子请求得 device3 消耗 4Gi。
		"allow-multiple-allocations-with-partitionable-device": {
			features: Features{
				PrioritizedList:      true,
				PartitionableDevices: true,
				ConsumableCapacity:   true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					deviceRequest(req0, classA, 1),
					requestWithPrioritizedList(req1,
						subRequest(subReq0, classA, 1, resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.capacity["%s"].memory.compareTo(quantity("6Gi")) >= 0`, driverA),
							}},
						),
						subRequest(subReq1, classA, 1, resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.capacity["%s"].memory.compareTo(quantity("4Gi")) >= 0`, driverA),
							}},
						),
					),
				),
			),
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							},
						),
					).withAllowMultipleAllocations(),
					device(device2, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"memory": resource.MustParse("6Gi"),
							},
						),
					).withAllowMultipleAllocations(),
					device(device3, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								"memory": resource.MustParse("4Gi"),
							},
						),
					).withAllowMultipleAllocations(),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1,
						map[string]resource.Quantity{
							"memory": resource.MustParse("8Gi"),
						},
					),
				),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceRequestAllocationResult(req0, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{"memory": resource.MustParse("4Gi")}),
				deviceRequestAllocationResult(req1SubReq1, driverA, pool1, device3).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{"memory": resource.MustParse("4Gi")}),
			)},
		},
		// 本用例验证跨分配周期的共享设备计数器不会被重复计费。
		// 特性开关：PartitionableDevices 与 ConsumableCapacity 同时开启。
		// 构造条件：allocatedSharedDeviceIDs 与 allocatedCapacityDevices 记录 device1 已有共享份额及 one 的消耗；请求带 mode=b 选择器，但 device1 的 mode=a、device2 的 mode=b。
		// 期望结果：expectResults 为空——跨周期的计数器/容量不能复用，无法满足请求。
		"allow-multiple-allocations-with-shared-device-counter-cross-cycle": {
			features: Features{
				PartitionableDevices: true,
				ConsumableCapacity:   true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					deviceRequest(req0, classA, 1).
						withCapacityRequest(ptr.To(one)).
						withSelectors(resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.attributes["%s"].mode == "b"`, driverA),
							},
						}),
				),
			),
			allocatedSharedDeviceIDs: sets.New(
				internal.MakeSharedDeviceID(MakeDeviceID(driverA, pool1, device1), &fixedShareID),
			),
			allocatedCapacityDevices: ConsumedCapacityCollection{
				MakeDeviceID(driverA, pool1, device1): ConsumedCapacity{
					capacity0: ptr.To(one),
				},
			},
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, fromCounters, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"mode": {StringValue: ptr.To("a")},
					}).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1, map[string]resource.Quantity{
							capacity0: one,
						}),
					).withAllowMultipleAllocations(),
					device(device2, fromCounters, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"mode": {StringValue: ptr.To("b")},
					}).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1, map[string]resource.Quantity{
							capacity0: one,
						}),
					).withAllowMultipleAllocations(),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1, map[string]resource.Quantity{
						capacity0: one,
					}),
				),
			),
			node:          node(node1, region1),
			expectResults: []any{},
		},
		// 本用例验证已持久化的共享分配不会给设备计数器再次充电。
		// 特性开关：PartitionableDevices 与 ConsumableCapacity 同时开启。
		// 构造条件：allocatedSharedDeviceIDs 仅含 device1 的 shareID（无 AggregatedCapacity）；设备从 counterset 消费 c=1，总量也为 c=1。
		// 期望结果：device1 仍被分配，结果使用 fixedShareID 且消耗容量映射为 nil。
		"partitionable-consumable-capacity-shared-device-counter-not-recharged": {
			features: Features{
				PartitionableDevices: true,
				ConsumableCapacity:   true,
			},
			// device1 already has a persisted shared allocation. It is represented
			// only by a share ID, with no AggregatedCapacity entry.
			allocatedSharedDeviceIDs: sets.New(
				internal.MakeSharedDeviceID(MakeDeviceID(driverA, pool1, device1), &fixedShareID),
			),
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil, request(req0, classA, 1)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, nil).
						withAllowMultipleAllocations().
						withDeviceCounterConsumption(
							deviceCounterConsumption(counterSet1, map[string]resource.Quantity{
								"c": resource.MustParse("1"),
							}),
						),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1, map[string]resource.Quantity{
						"c": resource.MustParse("1"),
					}),
				),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceRequestAllocationResult(req0, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, nil),
			)},
		},
		// 本用例验证 PrioritizedList、PartitionableDevices、ConsumableCapacity 组合下跨多个容量池的回溯分配。
		// 特性开关：PrioritizedList、PartitionableDevices、ConsumableCapacity 同时开启。
		// 构造条件：req0 请求容量 one、req2 请求 two，req1 的优先列表偏好 capacity0 大于等于 4；三台设备消费两组 counterset，有效组合仅有 [device1,device3] 或 [device2]。
		// 期望结果：req0 得 device1，req2 得 device3，req1 的次选子请求得 device1；各结果按请求量与策略消耗 capacity0/capacity1。
		"consumable-capacity-with-partitionable-device-multiple-capacity-pools": {
			// This test case combines integration of PrioritizedList, PartitionableDevices, and ConsumableCapacity features.
			features: Features{
				PrioritizedList:      true,
				PartitionableDevices: true,
				ConsumableCapacity:   true,
			},
			// There are two basic requests (req0 and req2), requesting 1 and 2 units of capacity0, respectively.
			// In addition, there is one request (req1) with a prioritized list.
			// The prioritized request prefers devices with capacity0 >= 4 over those with capacity0 >= 2.
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					deviceRequest(req0, classA, 1).withCapacityRequest(ptr.To(one)),
					deviceRequest(req2, classA, 1).withCapacityRequest(ptr.To(two)),
					requestWithPrioritizedList(req1,
						subRequest(subReq0, classA, 1, resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.capacity["%s"]["%s"].compareTo(quantity("4")) >= 0`, driverA, capacity0),
							}}).withCapacityRequest(ptr.To(one)),
						subRequest(subReq1, classA, 1, resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.capacity["%s"]["%s"].compareTo(quantity("2")) >= 0`, driverA, capacity0),
							}}).withCapacityRequest(ptr.To(one)),
					),
				),
			),
			classes: objects(class(classA, driverA)),
			// There three device option consuming counters from the PartitionableDevices's CounterSet.
			// All devices can be allocated multiple times with `allowMultipleAllocations=true`.
			// CounterSet (4, 8)
			//   device1, device2, and device3 consume (2,4), (4,4), and (2,4) for (capacity0, capacity1) respectively.
			//   i.e., only two options are valid those are [device1, device3] or [device2].
			// Capacity.RequestPolicy of ConsumableCapacity forces the capacity1 consuming with range policy (min,step,max)=(2,2,4).
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								capacity0: two,
							},
						),
						deviceCounterConsumption(counterSet2,
							map[string]resource.Quantity{
								capacity1: four,
							},
						),
					).withAllowMultipleAllocations().withCapacityRequestPolicyRange((map[resourceapi.QualifiedName]resource.Quantity{capacity1: four})),
					device(device2, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								capacity0: four,
							},
						),
						deviceCounterConsumption(counterSet2,
							map[string]resource.Quantity{
								capacity1: four,
							},
						),
					).withAllowMultipleAllocations().withCapacityRequestPolicyRange((map[resourceapi.QualifiedName]resource.Quantity{capacity1: four})),
					device(device3, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1,
							map[string]resource.Quantity{
								capacity0: two,
							},
						),
						deviceCounterConsumption(counterSet2,
							map[string]resource.Quantity{
								capacity1: four,
							},
						),
					).withAllowMultipleAllocations().withCapacityRequestPolicyRange((map[resourceapi.QualifiedName]resource.Quantity{capacity1: four})),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1,
						map[string]resource.Quantity{
							capacity0: four,
						},
					),
					counterSet(counterSet2,
						map[string]resource.Quantity{
							capacity1: resource.MustParse("8"),
						},
					),
				),
			),
			node: node(node1, region1),
			// Expected results:
			//   - [req0] The scheduler should be able to allocate device1 to req0.
			//     1 unit of capacity0 is consumed according to the requested capacity, and 2 units of capacity1 are consumed according to the RequestPolicy.
			//   - [req2] device1 does not have enough capacity0 remaining for req2, which requests 2 units (only 1 unit remains).
			//     The scheduler must also skip device2 due to the CounterSet constraint.
			//     Therefore, device3 is allocated to req2, providing 2 units of capacity0 and 2 units of capacity1.
			//   - [req1] The first prioritized subrequest of req1 cannot find a device because device2 cannot be selected.
			//     The second prioritized subrequest is then chosen and consumes the remaining 1 unit of capacity0 on device1.
			//     Consequently, device1 is allocated to subreq1 of req1 with 1 unit of capacity0 and 2 units of capacity1.
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceRequestAllocationResult(req0, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: one, capacity1: two}),
				deviceRequestAllocationResult(req2, driverA, pool1, device3).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: two, capacity1: two}),
				deviceRequestAllocationResult(req1SubReq1, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: one, capacity1: two}),
			)},
		},
		// 本用例验证多个 distinct 约束跨不同属性与不同请求组同时生效。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：三个 DistinctAttribute 约束分别针对 boolAttribute（req0/req1）、versionAttribute（req0-req2）、intAttribute（req0-req3）；四台设备各属性值互异或按设计重复。
		// 期望结果：req0 到 req3 依次分配 device1、device2、device3、device4，同时满足三组约束。
		"with-distinct-constraints": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withConstraints(
					resourceapi.DeviceConstraint{DistinctAttribute: &boolAttribute, Requests: []string{req0, req1}},
					resourceapi.DeviceConstraint{DistinctAttribute: &versionAttribute, Requests: []string{req0, req1, req2}},
					resourceapi.DeviceConstraint{DistinctAttribute: &intAttribute, Requests: []string{req0, req1, req2, req3}},
				).withRequests(
					deviceRequest(req0, classA, 1),
					deviceRequest(req1, classA, 1),
					deviceRequest(req2, classA, 1),
					deviceRequest(req3, classA, 1),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"driverVersion": {VersionValue: ptr.To("1.0.0")},
						"numa":          {IntValue: ptr.To(int64(0))},
						"boolAttribute": {BoolValue: ptr.To(true)},
					}).withAllowMultipleAllocations(),
					device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"driverVersion": {VersionValue: ptr.To("1.0.1")},
						"numa":          {IntValue: ptr.To(int64(1))},
						"boolAttribute": {BoolValue: ptr.To(false)},
					}).withAllowMultipleAllocations(),
					device(device3, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"driverVersion": {VersionValue: ptr.To("1.0.2")},
						"numa":          {IntValue: ptr.To(int64(2))},
						"boolAttribute": {BoolValue: ptr.To(true)},
					}).withAllowMultipleAllocations(),
					device(device4, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"driverVersion": {VersionValue: ptr.To("1.0.0")},
						"numa":          {IntValue: ptr.To(int64(3))},
						"boolAttribute": {BoolValue: ptr.To(true)},
					}).withAllowMultipleAllocations(),
				),
			),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceRequestAllocationResult(req0, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, nil),
				deviceRequestAllocationResult(req1, driverA, pool1, device2).withConsumedCapacity(&fixedShareID, nil),
				deviceRequestAllocationResult(req2, driverA, pool1, device3).withConsumedCapacity(&fixedShareID, nil),
				deviceRequestAllocationResult(req3, driverA, pool1, device4).withConsumedCapacity(&fixedShareID, nil),
			)},
		},
		// 本用例验证单请求需要 3 台设备但 distinct 属性值只有 2 种时失败。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：请求 request(req0, classA, 3) 并带 DistinctAttribute(stringAttribute)；三台设备的值为 value1、value2、value1。
		// 期望结果：expectResults 为 nil，无法凑齐 3 个互异值。
		"distinct-constraint-single-request-count-3-not-distinct": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{DistinctAttribute: &stringAttribute}},
				request(req0, classA, 3)),
			),
			classes: objects(class(classA, driverA)),
			// req0 asks for 3 devices under a DistinctAttribute(stringAttribute)
			// constraint. Three devices exist with stringAttribute values
			// value1, value2, value1 -- only two distinct values.
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValue: new("value1")},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValue: new("value2")},
				}),
				device(device3, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValue: new("value1")},
				}),
			)),
			node: node(node1, region1),
			// Expected results:
			//   - [req0] No allocation. Satisfying req0 requires 3 devices with
			//     distinct stringAttribute values, but device1 and device3 both
			//     have "value1", so no set of 3 distinct-valued devices exists.
			expectResults: nil,
		},
		// 本用例验证三台设备的 distinct 属性值互异时全部可分配。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：同样的 3 台请求，设备值为 value1、value2、value3。
		// 期望结果：device1、device2、device3 全部分配给 req0。
		"distinct-constraint-single-request-count-3-all-distinct": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{DistinctAttribute: &stringAttribute}},
				request(req0, classA, 3)),
			),
			classes: objects(class(classA, driverA)),
			// req0 asks for 3 devices under a DistinctAttribute(stringAttribute)
			// constraint. Three devices exist with distinct values
			// value1, value2, value3.
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValue: new("value1")},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValue: new("value2")},
				}),
				device(device3, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValue: new("value3")},
				}),
			)),
			node: node(node1, region1),
			// Expected results:
			//   - [req0] All three devices are allocated: their stringAttribute
			//     values (value1, value2, value3) are pairwise distinct, so the
			//     constraint holds.
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req0, driverA, pool1, device2, false),
				deviceAllocationResult(req0, driverA, pool1, device3, false),
			)},
		},
		// 本用例验证 distinct 约束跨两个请求生效时，总数所需互异值不足会失败。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：约束覆盖 req0 与 req1，req0 需 2 台、req1 需 1 台；三台设备值仍为 value1、value2、value1。
		// 期望结果：expectResults 为 nil，三台设备无法全部互异。
		"distinct-constraint-two-requests-with-count-not-distinct": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withConstraints(
					resourceapi.DeviceConstraint{DistinctAttribute: &stringAttribute, Requests: []string{req0, req1}},
				).withRequests(
					deviceRequest(req0, classA, 2),
					deviceRequest(req1, classA, 1),
				),
			),
			classes: objects(class(classA, driverA)),
			// req0 asks for 2 devices and req1 for 1 device; a single
			// DistinctAttribute(stringAttribute) constraint covers both requests,
			// so all 3 allocated devices must be distinct. Three devices exist
			// with values value1, value2, value1 -- only two distinct values.
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValue: new("value1")},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValue: new("value2")},
				}),
				device(device3, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValue: new("value1")},
				}),
			)),
			node: node(node1, region1),
			// Expected results:
			//   - No allocation. The constraint spans req0 and req1, so the 3
			//     devices across both requests must be pairwise distinct. Because
			//     device1 and device3 share "value1", 3 distinct-valued devices
			//     cannot be chosen.
			expectResults: nil,
		},
		// 本用例验证设备未设置 distinct 属性时不参与分配。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：约束指定 boolAttribute，但唯一设备属性映射为 nil。
		// 期望结果：expectResults 为空切片。
		"with-distinct-constraints-attribute-not-set": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withConstraints(
					resourceapi.DeviceConstraint{DistinctAttribute: &boolAttribute, Requests: []string{req0}},
				).withRequests(
					deviceRequest(req0, classA, 1),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil).withAllowMultipleAllocations(),
				),
			),
			node: node(node1, region1),

			expectResults: []any{},
		},
		// 本用例验证设备属性类型不被支持时报出明确错误。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：设备的 boolAttribute 为空 DeviceAttribute{}，没有可识别的值类型。
		// 期望结果：expectError 匹配包含『unsupported attribute value』的错误。
		"with-distinct-constraints-unknown-type-attribute": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withConstraints(
					resourceapi.DeviceConstraint{DistinctAttribute: &boolAttribute, Requests: []string{req0}},
				).withRequests(
					deviceRequest(req0, classA, 1),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"boolAttribute": {}}).withAllowMultipleAllocations(),
				),
			),
			node:          node(node1, region1),
			expectResults: []any{},
			expectError:   gomega.MatchError(gomega.ContainSubstring("unsupported attribute value")),
		},
		// 本用例验证 distinct 约束与优先列表子请求组合，且只作用于约束中列出的请求。
		// 特性开关：ConsumableCapacity 与 PrioritizedList 同时开启。
		// 构造条件：三个请求都带倾向 boolAttribute 的优先子请求，但约束只覆盖 req0 与 req1；设备为 device1(true)、device2(false)。
		// 期望结果：req0 用 subReq0 得 device1，req1 因约束改用 subReq1 得 device2，不受约束的 req2 仍用 subReq0 得 device1。
		"with-distinct-constraints-with-subrequests": {
			features: Features{
				ConsumableCapacity: true,
				PrioritizedList:    true,
			},
			// There are three requests, all preferring the boolAttribute.
			// However, the DeviceConstraint applies only to the first two requests.
			claimsToAllocate: objects(
				claim(claim0).withConstraints(
					resourceapi.DeviceConstraint{DistinctAttribute: &boolAttribute, Requests: []string{req0, req1}}).
					withRequests(
						requestWithPrioritizedList(req0,
							subRequest(subReq0, classA, 1, resourceapi.DeviceSelector{
								CEL: &resourceapi.CELDeviceSelector{
									Expression: fmt.Sprintf(`device.attributes["%s"].boolAttribute`, driverA),
								}}),
							subRequest(subReq1, classA, 1),
						),
						requestWithPrioritizedList(req1,
							subRequest(subReq0, classA, 1, resourceapi.DeviceSelector{
								CEL: &resourceapi.CELDeviceSelector{
									Expression: fmt.Sprintf(`device.attributes["%s"].boolAttribute`, driverA),
								}}),
							subRequest(subReq1, classA, 1),
						),
						requestWithPrioritizedList(req2,
							subRequest(subReq0, classA, 1, resourceapi.DeviceSelector{
								CEL: &resourceapi.CELDeviceSelector{
									Expression: fmt.Sprintf(`device.attributes["%s"].boolAttribute`, driverA),
								}}),
							subRequest(subReq1, classA, 1),
						),
					),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"boolAttribute": {BoolValue: ptr.To(true)},
					}).withAllowMultipleAllocations(),
					device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"boolAttribute": {BoolValue: ptr.To(false)},
					}).withAllowMultipleAllocations(),
				),
			),
			node: node(node1, region1),
			// Because the constraint applies only to req0 and req1,
			// subreq0 cannot satisfy req1, so device2 (subreq1) is allocated instead.
			// However, req2 has no such constraint and can use device1 (subreq0).
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceRequestAllocationResult(req0SubReq0, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, nil),
				deviceRequestAllocationResult(req1SubReq1, driverA, pool1, device2).withConsumedCapacity(&fixedShareID, nil),
				deviceRequestAllocationResult(req2SubReq0, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, nil),
			)},
		},
		// 本用例验证单请求多台设备时属性未设置的设备不能满足 distinct 约束。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：请求 3 台，约束为 DistinctAttribute(intAttribute)，三台设备属性均为 nil。
		// 期望结果：expectResults 为空切片。
		"distinct-constraint-single-request-multiple-devices-attribute-not-set": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0,
					[]resourceapi.DeviceConstraint{{DistinctAttribute: &intAttribute}},
					request(req0, classA, 3),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil),
					device(device2, nil, nil),
					device(device3, nil, nil),
				),
			),
			node:          node(node1, region1),
			expectResults: []any{},
		},
		// 本用例验证 distinct 属性值种类不足以覆盖请求台数时失败。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：请求 3 台，四台设备的 numa 值为 1、2、2、2，只有两种互异值。
		// 期望结果：expectResults 为空切片。
		"distinct-constraint-single-request-not-enough-distinct-values": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0,
					[]resourceapi.DeviceConstraint{{DistinctAttribute: &intAttribute}},
					request(req0, classA, 3),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"numa": {IntValue: new(int64(1))},
					}),
					device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"numa": {IntValue: new(int64(2))},
					}),
					device(device3, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"numa": {IntValue: new(int64(2))},
					}),
					device(device4, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"numa": {IntValue: new(int64(2))},
					}),
				),
			),
			node:          node(node1, region1),
			expectResults: []any{},
		},
		// 本用例验证存在足够互异属性值时可正确挑选设备。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：请求 3 台，四台设备 numa 值为 1、2、1、3，互异值有三种。
		// 期望结果：分配 device1（值1）、device2（值2）、device4（值3），跳过重复的 device3。
		"distinct-constraint-single-request-multiple-devices-attribute-set": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0,
					[]resourceapi.DeviceConstraint{{DistinctAttribute: &intAttribute}},
					request(req0, classA, 3),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"numa": {IntValue: new(int64(1))},
					}),
					device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"numa": {IntValue: new(int64(2))},
					}),
					device(device3, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"numa": {IntValue: new(int64(1))},
					}),
					device(device4, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"numa": {IntValue: new(int64(3))},
					}),
				),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req0, driverA, pool1, device2, false),
				deviceAllocationResult(req0, driverA, pool1, device4, false),
			)},
		},
		// 本用例验证单台可多分配设备无法满足需要互异属性值的 distinct 约束。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：两个请求各需容量 one；唯一设备 allowMultipleAllocations 且 stringAttribute 固定，上限 two。
		// 期望结果：expectResults 为空切片，因为两个份额的属性值相同。
		"distinct-constraint-one-multi-allocatable-device-with-distinct-constraint": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withConstraints(resourceapi.DeviceConstraint{DistinctAttribute: &stringAttribute}).withRequests(
					deviceRequest(req0, classA, 1).withCapacityRequest(ptr.To(one)),
					deviceRequest(req1, classA, 1).withCapacityRequest(ptr.To(one)),
				)),
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil,
						map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"stringAttribute": {StringValue: ptr.To("stringAttributeValue")}},
					).withAllowMultipleAllocations().withCapacityRequestPolicyRange(map[resourceapi.QualifiedName]resource.Quantity{capacity0: two}),
				),
			),
			node:          node(node1, region1),
			expectResults: []any{},
		},
		// 本用例验证两台属性不同的可多分配设备可分别满足 distinct 约束下的两个请求。
		// 特性开关：ConsumableCapacity 开启。
		// 构造条件：两个请求各请求容量 two；两台设备分别带 stringAttributeValue1/2，均 allowMultipleAllocations 且上限 four。
		// 期望结果：req0 得 device1、req1 得 device2，各消耗 capacity0 的 two。
		"distinct-constraint-two-multi-allocatable-devices-with-distinct-constraint": {
			features: Features{
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withConstraints(resourceapi.DeviceConstraint{DistinctAttribute: &stringAttribute}).withRequests(
					deviceRequest(req0, classA, 1).withCapacityRequest(ptr.To(two)),
					deviceRequest(req1, classA, 1).withCapacityRequest(ptr.To(two)),
				),
			),
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil,
						map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"stringAttribute": {StringValue: ptr.To("stringAttributeValue1")}},
					).withAllowMultipleAllocations().withCapacityRequestPolicyRange(map[resourceapi.QualifiedName]resource.Quantity{capacity0: four}),
				),
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device2, nil,
						map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{"stringAttribute": {StringValue: ptr.To("stringAttributeValue2")}},
					).withAllowMultipleAllocations().withCapacityRequestPolicyRange(map[resourceapi.QualifiedName]resource.Quantity{capacity0: four}),
				),
			),
			node: node(node1, region1),

			expectResults: []any{
				allocationResult(
					localNodeSelector(node1),
					deviceRequestAllocationResult(req0, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: two}),
					deviceRequestAllocationResult(req1, driverA, pool1, device2).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{capacity0: two}),
				),
			},
		},

		// 以下为 DRAFractionalCapacityRange（小数容量范围）特性的测试用例。
		// Test cases for DRAFractionalCapacityRange feature
		// 本用例验证小数容量特性关闭时，取整走整数路径（Value() 对小数量向上取整到 1）。
		// 特性开关：仅 ConsumableCapacity 开启，FractionalCapacityRange 刻意不设置。
		// 构造条件：请求 200m，设备使用 withFractionalCapacityRequestPolicyRange（默认 one、min/step 为整数语义）。
		// 期望结果：消耗 capacity0 为 NewQuantity(1, DecimalSI)，即整数路径结果而非 300m。
		"consumable-capacity-fractional-range-gate-disabled": {
			// FractionalCapacityRange is off: step arithmetic uses Value(), which rounds
			// sub-integer quantities up to 1.  With min=1, step=1, request=1:
			// added=0, n=0 → consumed = min + step*0 = 1 (DecimalSI, from step's format).
			features: Features{
				ConsumableCapacity: true,
				// FractionalCapacityRange intentionally not set
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(deviceRequest(req0, classA, 1).withCapacityRequest(&pointTwo)),
			),
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil).withAllowMultipleAllocations().withFractionalCapacityRequestPolicyRange(
						map[resourceapi.QualifiedName]resource.Quantity{capacity0: one},
					),
				),
			),
			node: node(node1, region1),
			expectResults: []any{
				allocationResult(
					localNodeSelector(node1),
					// Integer path: Value() of 200m and 100m both round up to 1,
					// so min=1, step=1, added=0 → result = NewQuantity(1, DecimalSI).
					deviceRequestAllocationResult(req0, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{
						capacity0: *resource.NewQuantity(1, resource.DecimalSI),
					}),
				),
			},
		},
		// 本用例验证小数容量特性开启后按毫单位算术向上取整到下一档。
		// 特性开关：ConsumableCapacity 与 FractionalCapacityRange 同时开启。
		// 构造条件：请求 250m，范围策略 min=200m、step=100m；250m 位于 200m 与 300m 之间。
		// 期望结果：按 (250-200)/100 向上取整一档，消耗 capacity0 为 300m（pointThree）。
		"consumable-capacity-fractional-range-gate-enabled-round-up": {
			// FractionalCapacityRange on: 250m is between min=200m and next step=300m,
			// so it is rounded up to 300m (milli-arithmetic: (250-200)/100 rounds up to 1 step).
			features: Features{
				ConsumableCapacity:      true,
				FractionalCapacityRange: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(deviceRequest(req0, classA, 1).withCapacityRequest(&pointTwoFive)),
			),
			classes: objects(classWithAllowMultipleAllocations(classA, driverA, true)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil).withAllowMultipleAllocations().withFractionalCapacityRequestPolicyRange(
						map[resourceapi.QualifiedName]resource.Quantity{capacity0: one},
					),
				),
			),
			node: node(node1, region1),
			expectResults: []any{
				allocationResult(
					localNodeSelector(node1),
					// Milli path: (250m - 200m) / 100m = 0.5 → ceil → 1 step → 200m + 100m = 300m.
					deviceRequestAllocationResult(req0, driverA, pool1, device1).withConsumedCapacity(&fixedShareID, map[resourceapi.QualifiedName]resource.Quantity{
						capacity0: pointThree,
					}),
				),
			},
		},

		// 以下为 DRAListTypeAttributes（列表类型属性的 matchAttributes/distinctAttributes）特性的测试用例。
		// Test cases for DRAListTypeAttributes features (matchAttributes/distinctAttributes)
		// 本用例验证列表属性特性开启后，标量字符串属性仍可满足 matchAttribute 约束。
		// 特性开关：ListTypeAttributes 开启。
		// 构造条件：两台设备的 stringAttribute 都是标量值 value1；请求 2 台并带 MatchAttribute(stringAttribute)。
		// 期望结果：两台设备都分配给 req0。
		"list-attributes-match-constaint-scalar-string-values": {
			features: Features{
				ListTypeAttributes: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{MatchAttribute: &stringAttribute}},
				request(req0, classA, 2)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValue: new("value1")},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValue: new("value1")},
				}),
			)),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req0, driverA, pool1, device2, false),
			)},
		},
		// 本用例验证字符串列表属性存在公共元素时满足 matchAttribute 约束。
		// 特性开关：ListTypeAttributes 开启。
		// 构造条件：device1 列表为 [value1,value2,value3]，device2 为 [value2,value3,value4]，公共元素为 value2/value3；请求 2 台。
		// 期望结果：两台设备都分配给 req0。
		"list-attributes-match-constaint-list-of-string-values-with-common-elements": {
			features: Features{
				ListTypeAttributes: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{MatchAttribute: &stringAttribute}},
				request(req0, classA, 2)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{"value1", "value2", "value3"}},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{"value2", "value3", "value4"}},
				}),
			)),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req0, driverA, pool1, device2, false),
			)},
		},
		// 本用例验证列表之间只有两两部分重叠、缺乏全体公共元素时不满足约束。
		// 特性开关：ListTypeAttributes 开启。
		// 构造条件：三台设备列表分别为 [value1,value2]、[value2,value3]、[value3,value1]，请求 3 台。
		// 期望结果：expectResults 为 nil，因为不存在同时属于三个列表的元素。
		"list-attributes-match-constaint-list-of-string-values-with-partially-common-elements": {
			features: Features{
				ListTypeAttributes: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{MatchAttribute: &stringAttribute}},
				request(req0, classA, 3)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{"value1", "value2"}},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{"value2", "value3"}},
				}),
				device(device3, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{"value3", "value1"}},
				}),
			)),
			node: node(node1, region1),

			expectResults: nil,
		},
		// 本用例验证回溯时必须恢复已计算的属性交集，避免后续选择被错误裁剪。
		// 特性开关：ListTypeAttributes 开启。
		// 构造条件：device1 列表 [value1,value2]、device2 [value1]、device3 与 device4 均为 [value2]；请求 3 台并带 MatchAttribute。
		// 期望结果：先尝试 device2 失败后回溯，最终选中 device1、device3、device4。
		"list-attributes-match-constraint-backtrack-needs-intersection-restore": {
			features: Features{
				ListTypeAttributes: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{MatchAttribute: &stringAttribute}},
				request(req0, classA, 3),
			)),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{"value1", "value2"}},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{"value1"}},
				}),
				device(device3, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{"value2"}},
				}),
				device(device4, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{"value2"}},
				}),
			)),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req0, driverA, pool1, device3, false),
				deviceAllocationResult(req0, driverA, pool1, device4, false),
			)},
		},
		// 本用例验证两个列表属性无公共元素时无法满足跨请求的 matchAttribute。
		// 特性开关：ListTypeAttributes 开启。
		// 构造条件：两个请求各需 1 台，device1 列表 [value1,value2]、device2 列表 [value3,value4]。
		// 期望结果：expectResults 为 nil。
		"list-attributes-match-constaint-list-of-string-values-without-common-elements": {
			features: Features{
				ListTypeAttributes: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{MatchAttribute: &stringAttribute}},
				request(req0, classA, 1),
				request(req0, classA, 1),
			)),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{"value1", "value2"}},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{"value3", "value4"}},
				}),
			)),
			node: node(node1, region1),

			expectResults: nil,
		},
		// 本用例验证标量字符串与字符串列表混合时，公共元素仍可满足约束。
		// 特性开关：ListTypeAttributes 开启。
		// 构造条件：device1 为标量 value1，device2 为列表 [value1,value2]；请求 2 台。
		// 期望结果：两台设备都分配给 req0。
		"list-attributes-match-constaint-mixed-scalar-and-list-of-string-values-with-common-elements": {
			features: Features{
				ListTypeAttributes: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{MatchAttribute: &stringAttribute}},
				request(req0, classA, 2),
			)),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValue: new("value1")},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{"value1", "value2"}},
				}),
			)),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req0, driverA, pool1, device2, false),
			)},
		},
		// 本用例验证混合类型下只有部分重叠、无全体公共元素时失败。
		// 特性开关：ListTypeAttributes 开启。
		// 构造条件：device1 标量 value1，device2 列表 [value1,value2]，device3 列表 [value2,value3]；请求 3 台。
		// 期望结果：expectResults 为 nil。
		"list-attributes-match-constaint-mixed-scalar-and-list-of-string-values-with-partially-common-elements": {
			features: Features{
				ListTypeAttributes: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{MatchAttribute: &stringAttribute}},
				request(req0, classA, 3)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValue: new("value1")},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{"value1", "value2"}},
				}),
				device(device3, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{"value2", "value3"}},
				}),
			)),
			node: node(node1, region1),

			expectResults: nil,
		},
		// 本用例验证混合类型且无公共元素时失败。
		// 特性开关：ListTypeAttributes 开启。
		// 构造条件：device1 标量 value1，device2 列表 [value2,value3]；请求 2 台。
		// 期望结果：expectResults 为 nil。
		"list-attributes-match-constaint-mixed-scalar-and-list-of-string-values-without-common-elements": {
			features: Features{
				ListTypeAttributes: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{MatchAttribute: &stringAttribute}},
				request(req0, classA, 2)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValue: new("value1")},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{"value2", "value3"}},
				}),
			)),
			node: node(node1, region1),

			expectResults: nil,
		},
		// 本用例验证整数列表属性的公共元素同样满足 matchAttribute 约束。
		// 特性开关：ListTypeAttributes 开启。
		// 构造条件：约束改为 MatchAttribute(numa)，device1 列表 [0,1,2]、device2 列表 [1,2,3]，请求 2 台。
		// 期望结果：两台设备都分配给 req0。
		"list-attributes-match-constaint-list-of-int-values-with-common-elements": {
			features: Features{
				ListTypeAttributes: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{MatchAttribute: &intAttribute}},
				request(req0, classA, 2)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"numa": {IntValues: []int64{0, 1, 2}},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"numa": {IntValues: []int64{1, 2, 3}},
				}),
			)),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req0, driverA, pool1, device2, false),
			)},
		},
		// KEP-6072: standardized numaNode attribute, list form. Two devices whose
		// numaNode lists share an element (physical node + same-socket equidistant
		// nodes) are co-placed by matchAttribute set intersection.
		// 本用例验证 KEP-6072 标准 numaNode 列表属性的集合交集匹配。
		// 特性开关：ListTypeAttributes 开启。
		// 构造条件：MatchAttribute 指向 resource.kubernetes.io/numaNode，device1 列表为 [4]、device2 列表为 [4,5,6,7]，请求 2 台。
		// 期望结果：两设备共享元素 4，满足共置约束，均分配给 req0。
		"numanode-matchattribute-overlapping-lists-are-co-placed": {
			features: Features{
				ListTypeAttributes: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{MatchAttribute: &numaNodeAttribute}},
				request(req0, classA, 2)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"resource.kubernetes.io/numaNode": {IntValues: []int64{4}},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"resource.kubernetes.io/numaNode": {IntValues: []int64{4, 5, 6, 7}},
				}),
			)),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req0, driverA, pool1, device2, false),
			)},
		},
		// KEP-6072: numaNode lists with no common element (different NUMA domains)
		// cannot satisfy matchAttribute, so no allocation is made.
		// 本用例验证 numaNode 列表无公共元素时无法满足 MatchAttribute 约束。
		// 特性开关：ListTypeAttributes 开启。
		// 构造条件：device1 列表为 [0]、device2 列表为 [4,5,6,7]，分属不同 NUMA 域，请求 2 台。
		// 期望结果：expectResults 为 nil，不进行分配。
		"numanode-matchattribute-disjoint-lists-are-not-allocated": {
			features: Features{
				ListTypeAttributes: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{MatchAttribute: &numaNodeAttribute}},
				request(req0, classA, 2)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"resource.kubernetes.io/numaNode": {IntValues: []int64{0}},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"resource.kubernetes.io/numaNode": {IntValues: []int64{4, 5, 6, 7}},
				}),
			)),
			node: node(node1, region1),

			expectResults: nil,
		},
		// 本用例验证布尔列表属性含公共元素时满足 MatchAttribute。
		// 特性开关：ListTypeAttributes 开启。
		// 构造条件：MatchAttribute(boolAttribute)，device1 为 [true,false]、device2 为 [true]。
		// 期望结果：两设备共有 true，均分配给 req0。
		"list-attributes-match-constaint-list-of-bool-values-with-common-elements": {
			features: Features{
				ListTypeAttributes: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{MatchAttribute: &boolAttribute}},
				request(req0, classA, 2)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"boolAttribute": {BoolValues: []bool{true, false}},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"boolAttribute": {BoolValues: []bool{true}},
				}),
			)),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req0, driverA, pool1, device2, false),
			)},
		},
		// 本用例验证版本列表属性含公共元素时满足 MatchAttribute。
		// 特性开关：ListTypeAttributes 开启。
		// 构造条件：MatchAttribute(versionAttribute)，两设备版本列表分别为 [1.0.0,1.1.0,2.0.0] 与 [1.1.0,2.0.0,2.1.0]。
		// 期望结果：两设备共有 1.1.0 与 2.0.0，均分配给 req0。
		"list-attributes-match-constaint-list-of-version-values-with-common-elements": {
			features: Features{
				ListTypeAttributes: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{MatchAttribute: &versionAttribute}},
				request(req0, classA, 2)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"driverVersion": {VersionValues: []string{"1.0.0", "1.1.0", "2.0.0"}},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"driverVersion": {VersionValues: []string{"1.1.0", "2.0.0", "2.1.0"}},
				}),
			)),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req0, driverA, pool1, device2, false),
			)},
		},
		// 本用例验证列表属性的空值或未知类型会报明确错误。
		// 特性开关：ListTypeAttributes 开启。
		// 构造条件：MatchAttribute(stringAttribute)，device1 属性为空 DeviceAttribute{}，device2 为空字符串列表 []。
		// 期望结果：expectResults 为 nil，expectError 匹配『unsupported attribute value』。
		"list-attributes-match-constaint-with-empty-or-unknown-type": {
			features: Features{
				ListTypeAttributes: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{MatchAttribute: &stringAttribute}},
				request(req0, classA, 2)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{}},
				}),
			)),
			node:          node(node1, region1),
			expectResults: nil,
			expectError:   gomega.MatchError(gomega.ContainSubstring("unsupported attribute value")),
		},
		// 本用例验证关闭列表属性特性后不再按列表语义匹配。
		// 特性开关：ListTypeAttributes 关闭。
		// 构造条件：MatchAttribute(stringAttribute)，两设备字符串列表虽有公共元素但特性关闭。
		// 期望结果：无法满足约束，expectResults 为 nil。
		"list-attributes-disabled-match-constraint-with-lists": {
			features: Features{
				ListTypeAttributes: false,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{MatchAttribute: &stringAttribute}},
				request(req0, classA, 2)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{"value1", "value2", "value3"}},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{"value2", "value3", "value4"}},
				}),
			)),
			node: node(node1, region1),

			expectResults: nil,
		},
		// 本用例验证开启列表特性后标量字符串值仍可用于 DistinctAttribute 判重。
		// 特性开关：ListTypeAttributes 与 ConsumableCapacity 开启。
		// 构造条件：DistinctAttribute(stringAttribute)，device1 为 value1、device2 为 value2，请求 2 台。
		// 期望结果：两设备取值互异，均分配给 req0。
		"list-attributes-distinct-constraint-scalar-string-values-all-distinct": {
			features: Features{
				ListTypeAttributes: true,
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{DistinctAttribute: &stringAttribute}},
				request(req0, classA, 2)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValue: new("value1")},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValue: new("value2")},
				}),
			)),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req0, driverA, pool1, device2, false),
			)},
		},
		// 本用例验证标量字符串值重复时违反 DistinctAttribute。
		// 特性开关：ListTypeAttributes 与 ConsumableCapacity 开启。
		// 构造条件：DistinctAttribute(stringAttribute)，device1 与 device2 同为 value1，请求 2 台。
		// 期望结果：expectResults 为 nil。
		"list-attributes-distinct-constraint-scalar-string-values-not-distinct": {
			features: Features{
				ListTypeAttributes: true,
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{DistinctAttribute: &stringAttribute}},
				request(req0, classA, 2)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValue: new("value1")},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValue: new("value1")},
				}),
			)),
			node: node(node1, region1),

			expectResults: nil,
		},
		// 本用例验证单请求需 3 台但只有 2 个互异标量值时失败。
		// 特性开关：ListTypeAttributes 与 ConsumableCapacity 开启。
		// 构造条件：req0 请求 3 台并带 DistinctAttribute(stringAttribute)，三台设备值为 value1、value2、value1。
		// 期望结果：device1 与 device3 同值，无法凑齐 3 个互异值，expectResults 为 nil。
		"list-attributes-distinct-constraint-single-request-count-3-not-distinct": {
			features: Features{
				ListTypeAttributes: true,
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{DistinctAttribute: &stringAttribute}},
				request(req0, classA, 3)),
			),
			classes: objects(class(classA, driverA)),
			// Same scenario as distinct-constraint-single-request-count-3-not-distinct
			// but with the ListTypeAttributes feature enabled: req0 asks for 3
			// devices under a DistinctAttribute constraint, and the 3 devices carry
			// values value1, value2, value1 -- only two distinct values.
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValue: new("value1")},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValue: new("value2")},
				}),
				device(device3, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValue: new("value1")},
				}),
			)),
			node: node(node1, region1),
			// Expected results:
			//   - [req0] No allocation. device1 and device3 both have "value1",
			//     so 3 devices with distinct stringAttribute values cannot be found.
			expectResults: nil,
		},
		// 本用例验证 3 个互异标量值可全部分配。
		// 特性开关：ListTypeAttributes 与 ConsumableCapacity 开启。
		// 构造条件：与上一用例相同但设备值为 value1、value2、value3。
		// 期望结果：三台设备均分配给 req0。
		"list-attributes-distinct-constraint-single-request-count-3-all-distinct": {
			features: Features{
				ListTypeAttributes: true,
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{DistinctAttribute: &stringAttribute}},
				request(req0, classA, 3)),
			),
			classes: objects(class(classA, driverA)),
			// Same scenario as distinct-constraint-single-request-count-3-all-distinct
			// but with the ListTypeAttributes feature enabled: req0 asks for 3
			// devices under a DistinctAttribute constraint, and the 3 devices carry
			// distinct values value1, value2, value3.
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValue: new("value1")},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValue: new("value2")},
				}),
				device(device3, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValue: new("value3")},
				}),
			)),
			node: node(node1, region1),
			// Expected results:
			//   - [req0] All three devices are allocated: their stringAttribute
			//     values (value1, value2, value3) are pairwise distinct.
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req0, driverA, pool1, device2, false),
				deviceAllocationResult(req0, driverA, pool1, device3, false),
			)},
		},
		// 本用例验证字符串列表值按集合判重满足 DistinctAttribute。
		// 特性开关：ListTypeAttributes 与 ConsumableCapacity 开启。
		// 构造条件：三设备列表为 [value1,value2]、[value2,value3]、[value3,value4]，req0 请求 2 台。
		// 期望结果：device1 与 device3 列表无公共元素，分配这两台。
		"list-attributes-distinct-constraint-list-of-string-values-all-distinct": {
			features: Features{
				ListTypeAttributes: true,
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{DistinctAttribute: &stringAttribute}},
				request(req0, classA, 2)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{"value1", "value2"}},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{"value2", "value3"}},
				}),
				device(device3, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{"value3", "value4"}},
				}),
			)),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req0, driverA, pool1, device3, false),
			)},
		},
		// 本用例验证字符串列表含公共元素时违反 DistinctAttribute。
		// 特性开关：ListTypeAttributes 与 ConsumableCapacity 开启。
		// 构造条件：device1 列表为 [value1,value2,value3]、device2 列表为 [value2,value4,value5]，共享 value2。
		// 期望结果：expectResults 为 nil。
		"list-attributes-distinct-constraint-list-of-string-values-with-common-elements": {
			features: Features{
				ListTypeAttributes: true,
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{DistinctAttribute: &stringAttribute}},
				request(req0, classA, 2)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{"value1", "value2", "value3"}},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{"value2", "value4", "value5"}},
				}),
			)),
			node: node(node1, region1),

			expectResults: nil,
		},
		// 本用例验证标量与列表混合取值也能按集合判重。
		// 特性开关：ListTypeAttributes 与 ConsumableCapacity 开启。
		// 构造条件：device1 为标量 value1、device2 为列表 [value1,value2]、device3 为列表 [value2,value3]，请求 2 台。
		// 期望结果：device1 与 device3 取值集合不相交，分配这两台。
		"list-attributes-distinct-constraint-mixed-scalar-and-list-all-distinct": {
			features: Features{
				ListTypeAttributes: true,
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{DistinctAttribute: &stringAttribute}},
				request(req0, classA, 2)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValue: new("value1")},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{"value1", "value2"}},
				}),
				device(device3, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{"value2", "value3"}},
				}),
			)),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req0, driverA, pool1, device3, false),
			)},
		},
		// 本用例验证标量与列表混合取值含公共元素时约束失败。
		// 特性开关：ListTypeAttributes 与 ConsumableCapacity 开启。
		// 构造条件：device1 为标量 value1、device2 为列表 [value1,value2]，两者共享 value1。
		// 期望结果：expectResults 为 nil。
		"list-attributes-distinct-constraint-mixed-scalar-and-list-with-common-elements": {
			features: Features{
				ListTypeAttributes: true,
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{DistinctAttribute: &stringAttribute}},
				request(req0, classA, 2)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValue: new("value1")},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{"value1", "value2"}},
				}),
			)),
			node: node(node1, region1),

			expectResults: nil,
		},
		// 本用例验证整数列表值的集合判重。
		// 特性开关：ListTypeAttributes 与 ConsumableCapacity 开启。
		// 构造条件：numa 列表为 [0,1]、[1,2]、[2,3]，req0 请求 2 台。
		// 期望结果：device1 与 device3 列表无交集，分配这两台。
		"list-attributes-distinct-constraint-list-of-int-values-all-distinct": {
			features: Features{
				ListTypeAttributes: true,
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{DistinctAttribute: &intAttribute}},
				request(req0, classA, 2)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"numa": {IntValues: []int64{0, 1}},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"numa": {IntValues: []int64{1, 2}},
				}),
				device(device3, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"numa": {IntValues: []int64{2, 3}},
				}),
			)),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req0, driverA, pool1, device3, false),
			)},
		},
		// 本用例验证布尔列表值的集合判重。
		// 特性开关：ListTypeAttributes 与 ConsumableCapacity 开启。
		// 构造条件：boolAttribute 列表为 [true]、[true,false]、[false]，req0 请求 2 台。
		// 期望结果：device1 与 device3 无公共取值，分配这两台。
		"list-attributes-distinct-constraint-list-of-bool-values-all-distinct": {
			features: Features{
				ListTypeAttributes: true,
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{DistinctAttribute: &boolAttribute}},
				request(req0, classA, 2)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"boolAttribute": {BoolValues: []bool{true}},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"boolAttribute": {BoolValues: []bool{true, false}},
				}),
				device(device3, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"boolAttribute": {BoolValues: []bool{false}},
				}),
			)),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req0, driverA, pool1, device3, false),
			)},
		},
		// 本用例验证版本列表值的集合判重。
		// 特性开关：ListTypeAttributes 与 ConsumableCapacity 开启。
		// 构造条件：版本列表为 [1.0.0,1.1.0]、[1.1.0,2.0.0]、[2.0.0,2.1.0]，相邻设备共享一个版本。
		// 期望结果：device1 与 device3 无公共版本，分配这两台。
		"list-attributes-distinct-constraint-list-of-version-values-all-distinct": {
			features: Features{
				ListTypeAttributes: true,
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{DistinctAttribute: &versionAttribute}},
				request(req0, classA, 2)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"driverVersion": {VersionValues: []string{"1.0.0", "1.1.0"}},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"driverVersion": {VersionValues: []string{"1.1.0", "2.0.0"}},
				}),
				device(device3, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"driverVersion": {VersionValues: []string{"2.0.0", "2.1.0"}},
				}),
			)),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req0, driverA, pool1, device3, false),
			)},
		},
		// 本用例验证 DistinctAttribute 遇到空或未知类型列表值时报错。
		// 特性开关：ListTypeAttributes 与 ConsumableCapacity 开启。
		// 构造条件：DistinctAttribute(versionAttribute)，device1 为空版本列表 []、device2 为 nil。
		// 期望结果：expectResults 为 nil，expectError 匹配『unsupported attribute value』。
		"list-attributes-distinct-constaint-with-empty-or-unknown-type": {
			features: Features{
				ListTypeAttributes: true,
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{DistinctAttribute: &versionAttribute}},
				request(req0, classA, 2)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"driverVersion": {VersionValues: []string{}},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"driverVersion": {VersionValues: nil},
				}),
			)),
			node: node(node1, region1),

			expectResults: nil,
			expectError:   gomega.MatchError(gomega.ContainSubstring("unsupported attribute value")),
		},
		// 本用例验证关闭列表特性后不按列表语义做 DistinctAttribute 判重。
		// 特性开关：ListTypeAttributes 关闭、ConsumableCapacity 开启。
		// 构造条件：DistinctAttribute(stringAttribute)，两设备带公共字符串元素的列表。
		// 期望结果：无法满足约束，expectResults 为 nil。
		"list-attributes-disabled-distinct-constraint-with-lists": {
			features: Features{
				ListTypeAttributes: false,
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{{DistinctAttribute: &stringAttribute}},
				request(req0, classA, 2)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{"value1", "value2", "value3"}},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{"value2", "value3", "value4"}},
				}),
			)),
			node: node(node1, region1),

			expectResults: nil,
		},
		// 本用例验证 DistinctAttribute 需要回溯并撤销已占用的列表值。
		// 特性开关：ListTypeAttributes 与 ConsumableCapacity 开启。
		// 构造条件：三个请求各需 1 台且共享 DistinctAttribute(stringAttribute)，五台设备的取值列表两两存在重叠。
		// 期望结果：回溯后依次分配 device3、device4、device0，三者取值集合互不相交。
		"list-attributes-distinct-constraint-backtrack-needs-remove": {
			features: Features{
				ListTypeAttributes: true,
				ConsumableCapacity: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withConstraints(
					resourceapi.DeviceConstraint{
						Requests:          []string{req0, req1, req2},
						DistinctAttribute: &stringAttribute,
					},
				).withRequests(
					deviceRequest(req0, classA, 1),
					deviceRequest(req1, classA, 1),
					deviceRequest(req2, classA, 1),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{"value1", "value2", "value3"}},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{"value3", "value4", "value5"}},
				}),
				device(device3, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{"value1", "value2"}},
				}),
				device(device4, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{"value3", "value4"}},
				}),
				device(device0, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"stringAttribute": {StringValues: []string{"value5", "value6"}},
				}),
			)),
			node: node(node1, region1),

			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device3, false),
				deviceAllocationResult(req1, driverA, pool1, device4, false),
				deviceAllocationResult(req2, driverA, pool1, device0, false),
			)},
		},

		// 本用例验证 All 分配模式在多主机资源池中只分配当前节点上的设备。
		// 构造条件：请求使用 DeviceAllocationModeAll，pool1 由 node1 与 node2 各一台设备组成，目标节点为 node1。
		// 期望结果：仅分配 node1 上的 device1，不跨节点分配 device2。
		"allocation-mode-all-with-multi-host-resource-pool": {
			claimsToAllocate: objects(claimWithRequests(claim0, nil, resourceapi.DeviceRequest{
				Name: req0,
				Exactly: &resourceapi.ExactDeviceRequest{
					AllocationMode:  resourceapi.DeviceAllocationModeAll,
					DeviceClassName: classA,
				},
			})),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, nil),
				),
				sliceWithDevices(slice2, node2, resourcePool(pool1, 2), driverA,
					device(device2, nil, nil),
				),
			),
			node: node(node1, region1),

			expectResults: []any{
				allocationResult(
					localNodeSelector(node1),
					deviceAllocationResult(req0, driverA, pool1, device1, false),
				),
			},
		},
		// 本用例验证当前节点 slice 版本落后时多主机资源池不分配设备。
		// 构造条件：pool1 声明含 2 个 slice，当前节点 slice1 为 generation 1，另一节点 slice2 为 generation 2。
		// 期望结果：当前节点视图过期导致池不完整，未指定期望结果，即不发生分配。
		"multi-host-resource-pool-with-stale-slice-for-current-node": {
			claimsToAllocate: objects(claimWithRequest(claim0, req0, classA)),
			classes:          objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				func() wrapResourceSliceWithDevices {
					s := sliceWithDevices(slice1, node1, pool1, driverA,
						device(device1, nil, nil),
					)
					s.Spec.Pool.Generation = 1
					s.Spec.Pool.ResourceSliceCount = 2
					return s
				}(),
				func() wrapResourceSliceWithDevices {
					s := sliceWithDevices(slice2, node2, pool1, driverA,
						device(device2, nil, nil),
					)
					s.Spec.Pool.Generation = 2
					s.Spec.Pool.ResourceSliceCount = 2
					return s
				}(),
			),
			node: node(node1, region1),
		},

		// update-from-node-1-to-node-2 and vice-versa is dangerous,
		// DRA drivers should not do this: there is no guarantee that
		// the scheduler sees the change and then might allocate a
		// device for a node where it is no longer available.
		//
		// We do not have to support such a change exactly as expected
		// by these test cases if it makes the implementation too
		// expensive because of additional checks.
		//
		// The support for allocation from incomplete pools is also under
		// debate. It's currently done because without scoring it makes
		// no difference how many slices are available, as long as one
		// device can be found.

		// 本用例验证资源池从节点 1 整体迁移到节点 2 时的行为。
		// 构造条件：旧 generation 1 的 node1 有两个 slice，新 generation 2 的 node2 有两个完整 slice，目标节点仍为 node1。
		// 期望结果：当前实现忽略更新，仍从旧池分配 device1；DRA 驱动不应依赖更优处理。
		"multi-host-resource-pool-complete-update-from-node-1-to-node-2": {
			claimsToAllocate: objects(claimWithRequest(claim0, req0, classA)),
			classes:          objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				// Node 1, generation 1.
				func() wrapResourceSliceWithDevices {
					s := sliceWithDevices(slice1, node1, pool1, driverA,
						device(device1, nil, nil),
					)
					s.Spec.Pool.Generation = 1
					s.Spec.Pool.ResourceSliceCount = 2
					return s
				}(),
				func() wrapResourceSliceWithDevices {
					s := sliceWithDevices(slice1, node1, pool1, driverA,
						device(device2, nil, nil),
					)
					s.Spec.Pool.Generation = 1
					s.Spec.Pool.ResourceSliceCount = 2
					return s
				}(),
				// Node 2, generation 2.
				func() wrapResourceSliceWithDevices {
					s := sliceWithDevices(slice2, node2, pool1, driverA,
						device(device1, nil, nil),
					)
					s.Spec.Pool.Generation = 2
					s.Spec.Pool.ResourceSliceCount = 2
					return s
				}(),
				func() wrapResourceSliceWithDevices {
					s := sliceWithDevices(slice2, node2, pool1, driverA,
						device(device2, nil, nil),
					)
					s.Spec.Pool.Generation = 2
					s.Spec.Pool.ResourceSliceCount = 2
					return s
				}(),
			),
			node: node(node1, region1),

			// We'd like this to *not* be allocated, but the current implementation ignores
			// the updated slices. Allocating from the old pool is also what would happen
			// if the scheduler hadn't received the update yet, so DRA drivers shouldn't
			// depend on better handling of this situation.
			expectResults: []any{
				allocationResult(
					localNodeSelector(node1),
					deviceAllocationResult(req0, driverA, pool1, device1, false),
				),
			},
		},

		// 本用例验证资源池从节点 2 整体迁移到节点 1 时的行为。
		// 构造条件：generation 1 在 node2、generation 2 在 node1，两边均有完整 slice，目标节点为 node1。
		// 期望结果：仍按现有实现从池中分配 device1。
		"multi-host-resource-pool-complete-update-from-node-2-to-node-1": {
			claimsToAllocate: objects(claimWithRequest(claim0, req0, classA)),
			classes:          objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				// Node 2, generation 1.
				func() wrapResourceSliceWithDevices {
					s := sliceWithDevices(slice1, node2, pool1, driverA,
						device(device1, nil, nil),
					)
					s.Spec.Pool.Generation = 1
					s.Spec.Pool.ResourceSliceCount = 2
					return s
				}(),
				func() wrapResourceSliceWithDevices {
					s := sliceWithDevices(slice1, node2, pool1, driverA,
						device(device2, nil, nil),
					)
					s.Spec.Pool.Generation = 1
					s.Spec.Pool.ResourceSliceCount = 2
					return s
				}(),
				// Node 1, generation 2.
				func() wrapResourceSliceWithDevices {
					s := sliceWithDevices(slice2, node1, pool1, driverA,
						device(device1, nil, nil),
					)
					s.Spec.Pool.Generation = 2
					s.Spec.Pool.ResourceSliceCount = 2
					return s
				}(),
				func() wrapResourceSliceWithDevices {
					s := sliceWithDevices(slice2, node1, pool1, driverA,
						device(device2, nil, nil),
					)
					s.Spec.Pool.Generation = 2
					s.Spec.Pool.ResourceSliceCount = 2
					return s
				}(),
			),
			node: node(node1, region1),

			expectResults: []any{
				allocationResult(
					localNodeSelector(node1),
					deviceAllocationResult(req0, driverA, pool1, device1, false),
				),
			},
		},

		// 本用例验证池迁移过程中新节点 slice 不完整时的行为。
		// 构造条件：旧节点 generation 1 池完整，新节点 generation 2 仅有一个 slice（声明 2 个），目标节点为旧节点。
		// 期望结果：当前实现忽略更新，从旧池分配 device1。
		"multi-host-resource-pool-incomplete-update-from-node-1-to-node-2": {
			claimsToAllocate: objects(claimWithRequest(claim0, req0, classA)),
			classes:          objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				// Node 1, generation 1.
				func() wrapResourceSliceWithDevices {
					s := sliceWithDevices(slice1, node1, pool1, driverA,
						device(device1, nil, nil),
					)
					s.Spec.Pool.Generation = 1
					s.Spec.Pool.ResourceSliceCount = 2
					return s
				}(),
				func() wrapResourceSliceWithDevices {
					s := sliceWithDevices(slice1, node1, pool1, driverA,
						device(device2, nil, nil),
					)
					s.Spec.Pool.Generation = 1
					s.Spec.Pool.ResourceSliceCount = 2
					return s
				}(),
				// Node 2, generation 2.
				func() wrapResourceSliceWithDevices {
					s := sliceWithDevices(slice2, node2, pool1, driverA,
						device(device1, nil, nil),
					)
					s.Spec.Pool.Generation = 2
					s.Spec.Pool.ResourceSliceCount = 2
					return s
				}(),
			),
			node: node(node1, region1),

			// We'd like this to *not* be allocated, but the current implementation ignores
			// the updated slices. Allocating from the old pool is also what would happen
			// if the scheduler hadn't received the update yet, so DRA drivers shouldn't
			// depend on better handling of this situation.
			expectResults: []any{
				allocationResult(
					localNodeSelector(node1),
					deviceAllocationResult(req0, driverA, pool1, device1, false),
				),
			},
		},

		// 本用例验证目标节点上新池 slice 不完整时不分配。
		// 构造条件：generation 1 在 node2 池完整，目标节点 node1 的 generation 2 池只有一个 slice（声明 2 个）。
		// 期望结果：slice2 不完整，不发生分配（expectResults 缺省为 nil）。
		"multi-host-resource-pool-incomplete-update-from-node-2-to-node-1": {
			claimsToAllocate: objects(claimWithRequest(claim0, req0, classA)),
			classes:          objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				// Node 2, generation 1.
				func() wrapResourceSliceWithDevices {
					s := sliceWithDevices(slice1, node2, pool1, driverA,
						device(device1, nil, nil),
					)
					s.Spec.Pool.Generation = 1
					s.Spec.Pool.ResourceSliceCount = 2
					return s
				}(),
				func() wrapResourceSliceWithDevices {
					s := sliceWithDevices(slice1, node2, pool1, driverA,
						device(device2, nil, nil),
					)
					s.Spec.Pool.Generation = 1
					s.Spec.Pool.ResourceSliceCount = 2
					return s
				}(),
				// Node 1, generation 2.
				func() wrapResourceSliceWithDevices {
					s := sliceWithDevices(slice2, node1, pool1, driverA,
						device(device1, nil, nil),
					)
					s.Spec.Pool.Generation = 2
					s.Spec.Pool.ResourceSliceCount = 2
					return s
				}(),
			),
			node: node(node1, region1),
			// This does not get allocated since slice2 is incomplete.
		},
		// 本用例验证同一资源池内不同 slice 使用相同 counterSet 名称会报错。
		// 特性开关：PartitionableDevices 开启。
		// 构造条件：pool1 的 slice2 与 slice4 各包含一个名为 counterSet1 的计数器集。
		// 期望结果：expectError 匹配『invalid resource pools were encountered』。
		"partitionable-devices-same-counter-set-name-in-different-resource-slices": {
			features: Features{
				PartitionableDevices: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					deviceRequest(req0, classA, 1),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 4), driverA,
					device(device1, nil, nil),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 4), driverA,
					counterSet(counterSet1, nil),
				),
				sliceWithDevices(slice3, node1, resourcePool(pool1, 4), driverA,
					device(device2, nil, nil),
				),
				sliceWithCounterSets(slice4, node1, resourcePool(pool1, 4), driverA,
					counterSet(counterSet1, nil),
				),
			),
			node:        node(node1, region1),
			expectError: gomega.MatchError(gomega.ContainSubstring("invalid resource pools were encountered")),
		},
		// 本用例验证设备引用不存在的 counterSet 会报错。
		// 特性开关：PartitionableDevices 开启。
		// 构造条件：device1 的消费记录指向 counterSet2，而池中只定义了 counterSet1。
		// 期望结果：expectError 匹配『invalid resource pools were encountered』。
		"partitionable-devices-device-counter-consumption-references-unknown-counter-set": {
			features: Features{
				PartitionableDevices: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					deviceRequest(req0, classA, 1),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet2, nil),
					),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1, nil),
				),
			),
			node:        node(node1, region1),
			expectError: gomega.MatchError(gomega.ContainSubstring("invalid resource pools were encountered")),
		},
		// 本用例验证设备引用 counterSet 中不存在的计数器名会报错。
		// 特性开关：PartitionableDevices 开启。
		// 构造条件：device1 消费名为 memory 的计数器，counterSet1 却只提供 other-memory。
		// 期望结果：expectError 匹配『invalid resource pools were encountered』。
		"partitionable-devices-device-counter-consumption-references-unknown-counter-in-counter-set": {
			features: Features{
				PartitionableDevices: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					deviceRequest(req0, classA, 1),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1, map[string]resource.Quantity{
							"memory": resource.MustParse("8Gi"),
						}),
					),
				),
				sliceWithCounterSets(slice2, node1, resourcePool(pool1, 2), driverA,
					counterSet(counterSet1, map[string]resource.Quantity{
						"other-memory": resource.MustParse("8Gi"),
					}),
				),
			),
			node:        node(node1, region1),
			expectError: gomega.MatchError(gomega.ContainSubstring("invalid resource pools were encountered")),
		},
		// 本用例验证设备引用其他资源池中的 counterSet 会报错。
		// 特性开关：PartitionableDevices 开启。
		// 构造条件：device1 位于 pool1 并引用 counterSet1，而该计数器集定义在 pool2。
		// 期望结果：expectError 匹配『invalid resource pools were encountered』。
		"partitionable-devices-device-counter-consumption-references-counter-set-in-other-pool": {
			features: Features{
				PartitionableDevices: true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					deviceRequest(req0, classA, 1),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1, map[string]resource.Quantity{
							"memory": resource.MustParse("8Gi"),
						}),
					),
				),
				sliceWithCounterSets(slice2, node1, pool2, driverA,
					counterSet(counterSet1, map[string]resource.Quantity{
						"memory": resource.MustParse("8Gi"),
					}),
				),
			),
			node:        node(node1, region1),
			expectError: gomega.MatchError(gomega.ContainSubstring("invalid resource pools were encountered")),
		},
		// 本用例验证跨节点已分配设备对同一 counterSet 的消耗会影响本节点候选设备容量。
		// 特性开关：PartitionableDevices 与 PrioritizedList 开启。
		// 构造条件：counterSet1 共 8Gi，node1 的 device1/device2 与已分配的 node2 device3 均引用它；优先列表先要求 ≥6Gi 再要求 ≥2Gi。
		// 期望结果：第一备选无法满足，回退到 subReq1，分配 node1 上的 device2。
		"partitionable-devices-devices-on-different-nodes-consume-same-counter-set": {
			features: Features{
				PartitionableDevices: true,
				PrioritizedList:      true,
			},
			claimsToAllocate: objects(
				claim(claim0).withRequests(
					requestWithPrioritizedList(req0,
						subRequest(subReq0, classA, 1, resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.capacity["%s"].memory.compareTo(quantity("6Gi")) >= 0`, driverA),
							},
						}),
						subRequest(subReq1, classA, 1, resourceapi.DeviceSelector{
							CEL: &resourceapi.CELDeviceSelector{
								Expression: fmt.Sprintf(`device.capacity["%s"].memory.compareTo(quantity("2Gi")) >= 0`, driverA),
							},
						}),
					),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithCounterSets(slice1, node1, resourcePool(pool1, 3), driverA,
					counterSet(counterSet1, map[string]resource.Quantity{
						"memory": resource.MustParse("8Gi"),
					}),
				),
				sliceWithDevices(slice2, node1, resourcePool(pool1, 3), driverA,
					device(device1, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1, map[string]resource.Quantity{
							"memory": resource.MustParse("6Gi"),
						}),
					),
					device(device2, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1, map[string]resource.Quantity{
							"memory": resource.MustParse("2Gi"),
						}),
					),
				),
				sliceWithDevices(slice3, node2, resourcePool(pool1, 3), driverA,
					device(device3, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1, map[string]resource.Quantity{
							"memory": resource.MustParse("6Gi"),
						}),
					),
				),
			),
			allocatedDevices: []DeviceID{
				MakeDeviceID(driverA, pool1, device3),
			},
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0SubReq1, driverA, pool1, device2, false),
			)},
		},
		// 本用例验证设备容量按目标节点 counterSet 的剩余量计算。
		// 特性开关：PartitionableDevices 开启。
		// 构造条件：counterSet1 为 8Gi，已分配的 node1 device1 消耗 6Gi，node2 上 device2 消耗 6Gi、device3 消耗 2Gi，目标节点为 node2。
		// 期望结果：在 node2 上把 device3 分配给 req0。
		"partitionable-devices-devices-consume-counter-set-in-different-nodes": {
			features: Features{
				PartitionableDevices: true,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil, request(req0, classA, 1)),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithCounterSets(slice1, node1, resourcePool(pool1, 3), driverA,
					counterSet(counterSet1, map[string]resource.Quantity{
						"memory": resource.MustParse("8Gi"),
					}),
				),
				sliceWithDevices(slice2, node1, resourcePool(pool1, 3), driverA,
					device(device1, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1, map[string]resource.Quantity{
							"memory": resource.MustParse("6Gi"),
						}),
					),
				),
				sliceWithDevices(slice3, node2, resourcePool(pool1, 3), driverA,
					device(device2, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1, map[string]resource.Quantity{
							"memory": resource.MustParse("6Gi"),
						}),
					),
					device(device3, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1, map[string]resource.Quantity{
							"memory": resource.MustParse("2Gi"),
						}),
					),
				),
			),
			allocatedDevices: []DeviceID{
				MakeDeviceID(driverA, pool1, device1),
			},
			node: node(node2, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node2),
				deviceAllocationResult(req0, driverA, pool1, device3, false),
			)},
		},
		// 本用例验证非目标节点 slice 中的非法引用同样导致整体校验失败。
		// 特性开关：PartitionableDevices 开启。
		// 构造条件：node2 的 device3 引用不存在的 counterSet2，而目标节点为 node1。
		// 期望结果：expectError 匹配『invalid resource pools were encountered』。
		"partitionable-devices-validation-error-on-non-targeting-slice": {
			features: Features{
				PartitionableDevices: true,
			},
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil,
					request(req0, classA, 1),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithCounterSets(slice1, node1, resourcePool(pool1, 3), driverA,
					counterSet(counterSet1, map[string]resource.Quantity{
						"memory": resource.MustParse("8Gi"),
					}),
				),
				sliceWithDevices(slice2, node1, resourcePool(pool1, 3), driverA,
					device(device1, fromCounters, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet1, map[string]resource.Quantity{
							"memory": resource.MustParse("6Gi"),
						}),
					),
				),
				sliceWithDevices(slice3, node2, resourcePool(pool1, 3), driverA,
					device(device3, nil, nil).withDeviceCounterConsumption(
						deviceCounterConsumption(counterSet2, map[string]resource.Quantity{
							"memory": resource.MustParse("6Gi"),
						}),
					),
				),
			),
			node:        node(node1, region1),
			expectError: gomega.MatchError(gomega.ContainSubstring("invalid resource pools were encountered")),
		},
		// 本用例验证同一池内不同 slice 可面向不同节点，且只选择目标节点设备。
		// 构造条件：pool1 的 slice1 指向 node2（device1）、slice2 指向 node1（device2），目标节点为 node1。
		// 期望结果：仅分配 node1 上的 device2。
		"different-resourceslices-in-pool-can-target-different-nodes": {
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil,
					request(req0, classA, 1),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node2, resourcePool(pool1, 2), driverA,
					device(device1, nil, nil),
				),
				sliceWithDevices(slice2, node1, resourcePool(pool1, 2), driverA,
					device(device2, nil, nil),
				),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device2, false),
			)},
		},
		// 本用例验证存在有效池时无效池不会导致整体失败。
		// 构造条件：pool1 的两个 slice 重复包含 device1（池无效），pool2 含有效的 device2。
		// 期望结果：从 pool2 分配 device2，忽略无效池。
		"invalid-pool-is-ok-if-devices-can-be-allocated-from-other": {
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil,
					request(req0, classA, 1),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, nil),
				),
				sliceWithDevices(slice2, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, nil),
				),
				sliceWithDevices(slice3, node1, resourcePool(pool2, 1), driverA,
					device(device2, nil, nil),
				),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool2, device2, false),
			)},
		},
		// 本用例验证无其他有效设备可选时无效池导致报错。
		// 构造条件：与前例相同，但 pool2 的 device2 已被分配，无法使用。
		// 期望结果：expectError 匹配『invalid resource pools were encountered』。
		"invalid-pool-is-error-if-no-other-device-can-be-allocated": {
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil,
					request(req0, classA, 1),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, nil),
				),
				sliceWithDevices(slice2, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, nil),
				),
				sliceWithDevices(slice3, node1, resourcePool(pool2, 1), driverA,
					device(device2, nil, nil),
				),
			),
			allocatedDevices: []DeviceID{
				MakeDeviceID(driverA, pool2, device2),
			},
			node:        node(node1, region1),
			expectError: gomega.MatchError(gomega.ContainSubstring("invalid resource pools were encountered")),
		},
		// 本用例验证资源池不完整时不进行分配。
		// 构造条件：pool1 声明含 2 个 slice，实际只提供 1 个含 device1 的 slice。
		// 期望结果：未指定期望结果，即不发生分配。
		"no-allocation-from-incomplete-pools": {
			claimsToAllocate: objects(
				claimWithRequests(claim0, nil,
					request(req0, classA, 1),
				),
			),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, nil),
				),
			),
			node: node(node1, region1),
		},
		// 本用例验证单请求、单池、单 slice 下的组合枚举次数。
		// 构造条件：req0 请求 5 台，slice1 仅有 4 台设备，注定失败但会枚举全部组合。
		// 期望结果：无分配结果；AllocateOne 调用 16 次，Stable 通道调用 65 次。
		"check-combinations-within-single-request-single-pool-single-slice": {
			claimsToAllocate: objects(claimWithRequests(claim0, nil,
				request(req0, classA, 5),
			)),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, nil),
				device(device2, nil, nil),
				device(device3, nil, nil),
				device(device4, nil, nil),
			)),
			node:                            node(node1, region1),
			expectResults:                   nil,
			expectNumAllocateOneInvocations: 16,
			expectNumAllocateOneInvocationsByChannel: map[internal.AllocatorChannel]int64{
				internal.Stable: 65,
			},
		},
		// 本用例验证跨多个 slice 的组合枚举次数与单 slice 一致。
		// 构造条件：req0 请求 5 台，pool1 的两个 slice 各含 2 台设备。
		// 期望结果：无分配结果；AllocateOne 调用 16 次，Stable 通道 65 次。
		"check-combinations-within-single-request-single-pool-multiple-slices": {
			claimsToAllocate: objects(claimWithRequests(claim0, nil,
				request(req0, classA, 5),
			)),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, nil),
					device(device2, nil, nil),
				),
				sliceWithDevices(slice2, node1, resourcePool(pool1, 2), driverA,
					device(device3, nil, nil),
					device(device4, nil, nil),
				),
			),
			node:                            node(node1, region1),
			expectResults:                   nil,
			expectNumAllocateOneInvocations: 16,
			expectNumAllocateOneInvocationsByChannel: map[internal.AllocatorChannel]int64{
				internal.Stable: 65,
			},
		},
		// 本用例验证跨多个资源池的组合枚举次数保持一致。
		// 构造条件：req0 请求 5 台，pool1 有两个各含 1 台设备的 slice，pool2 有一个含 2 台设备的 slice。
		// 期望结果：无分配结果；AllocateOne 调用 16 次，Stable 通道 65 次。
		"check-combinations-within-single-request-multiple-pools-multiple-slices": {
			claimsToAllocate: objects(claimWithRequests(claim0, nil,
				request(req0, classA, 5),
			)),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, nil),
				),
				sliceWithDevices(slice2, node1, resourcePool(pool1, 2), driverA,
					device(device2, nil, nil),
				),
				sliceWithDevices(slice3, node1, pool2, driverA,
					device(device3, nil, nil),
					device(device4, nil, nil),
				),
			),
			node:                            node(node1, region1),
			expectResults:                   nil,
			expectNumAllocateOneInvocations: 16,
			expectNumAllocateOneInvocationsByChannel: map[internal.AllocatorChannel]int64{
				internal.Stable: 65,
			},
		},
		// 本用例验证跨多个单设备池的组合枚举次数保持一致。
		// 构造条件：req0 请求 5 台，pool1 至 pool4 各含 1 台设备。
		// 期望结果：无分配结果；AllocateOne 调用 16 次，Stable 通道 65 次。
		"check-combinations-within-single-request-many-pools": {
			claimsToAllocate: objects(claimWithRequests(claim0, nil,
				request(req0, classA, 5),
			)),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil),
				),
				sliceWithDevices(slice2, node1, pool2, driverA,
					device(device2, nil, nil),
				),
				sliceWithDevices(slice3, node1, pool3, driverA,
					device(device3, nil, nil),
				),
				sliceWithDevices(slice4, node1, pool4, driverA,
					device(device4, nil, nil),
				),
			),
			node:                            node(node1, region1),
			expectResults:                   nil,
			expectNumAllocateOneInvocations: 16,
			expectNumAllocateOneInvocationsByChannel: map[internal.AllocatorChannel]int64{
				internal.Stable: 65,
			},
		},
		// 本用例验证分配失败后回溯并跳过重复设备组合。
		// 构造条件：req1 需 2 台通用设备、req2 需 type=X 的 device1；设备顺序使分配器先尝试 {device1,device2} 而失败。
		// 期望结果：回溯后 req1 分得 device2+device3、req2 分得 device1；AllocateOne 调用 12 次，Stable 通道 14 次。
		"check-combinations-with-backtracking": {
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				nil,
				// req-1 needs two generic devices.
				request(req1, classA, 2),
				// req-2 needs a specific device.
				request(req2, classA, 1, resourceapi.DeviceSelector{
					CEL: &resourceapi.CELDeviceSelector{
						Expression: fmt.Sprintf(`device.attributes["%s"].type == "X"`, driverA),
					},
				}),
			)),
			classes: objects(class(classA, driverA)),
			// The order of devices is chosen such that the allocator
			// will initially pick {device1, device2} for req1. This will fail
			// because req-2 needs device1. The allocator has to
			// backtrack. The correct solution is {device2, device3} for req-1
			// and {device1} for req-2. The optimized allocator avoids
			// testing {device2, device1} for req-1 and thus finds the solution
			// faster.
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"type": {StringValue: ptr.To("X")},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"type": {StringValue: ptr.To("Y")},
				}),
				device(device3, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"type": {StringValue: ptr.To("Y")},
				}),
			)),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req1, driverA, pool1, device2, false),
				deviceAllocationResult(req1, driverA, pool1, device3, false),
				deviceAllocationResult(req2, driverA, pool1, device1, false),
			)},
			expectNumAllocateOneInvocations: 12,
			expectNumAllocateOneInvocationsByChannel: map[internal.AllocatorChannel]int64{
				internal.Stable: 14,
			},
		},
		// 本用例验证设备分散在多个 slice/池时同样需要回溯。
		// 特性开关：DeviceBindingAndStatus 开启。
		// 构造条件：device1 在 pool1/slice1、device2 在 pool1/slice2、device3 在 pool2 且带绑定条件；req2 指定 device1。
		// 期望结果：req1 分得 device2 与带绑定条件的 device3，req2 分得 device1；调用次数 12/Stable 14。
		"check-combinations-with-backtracking-across-slices-and-pools": {
			features: Features{
				DeviceBindingAndStatus: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				nil,
				// req-1 needs two generic devices.
				request(req1, classA, 2),
				// req-2 needs a specific device.
				request(req2, classA, 1, resourceapi.DeviceSelector{
					CEL: &resourceapi.CELDeviceSelector{
						Expression: fmt.Sprintf(`device.attributes["%s"].type == "X"`, driverA),
					},
				}),
			)),
			classes: objects(class(classA, driverA)),
			// The order of devices is chosen such that the allocator
			// will initially pick {device1, device2} for req1. This will fail
			// because req-2 needs device1. The allocator has to
			// backtrack. The correct solution is {device2, device3} for req-1
			// and {device1} for req-2. The optimized allocator avoids
			// testing {device2, device1} for req-1 and thus finds the solution
			// faster.
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, resourcePool(pool1, 2), driverA,
					device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"type": {StringValue: ptr.To("X")},
					}),
				),
				sliceWithDevices(slice2, node1, resourcePool(pool1, 2), driverA,
					device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"type": {StringValue: ptr.To("Y")},
					}),
				),
				// Use a binding condition here to make sure pool2 is searched after pool1 when
				// trying to allocate devices. This makes sure we see the same results every time.
				sliceWithDevices(slice3, node1, pool2, driverA,
					device(device3, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"type": {StringValue: ptr.To("Y")},
					}).withBindingConditions([]string{"IsPrepare"}, []string{}),
				),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req1, driverA, pool1, device2, false),
				deviceRequestAllocationResultWithBindingConditions(req1, driverA, pool2, device3, []string{"IsPrepare"}, []string{}),
				deviceAllocationResult(req2, driverA, pool1, device1, false),
			)},
			expectNumAllocateOneInvocations: 12,
			expectNumAllocateOneInvocationsByChannel: map[internal.AllocatorChannel]int64{
				internal.Stable: 14,
			},
		},
		// 本用例验证优先列表发生回退时的组合枚举次数。
		// 特性开关：PrioritizedList 开启。
		// 构造条件：req1 第一备选需 3 台 Y（仅 2 台）故回退到 2 台通用；req2 第一备选需 2 台通用故回退到 1 台 X。
		// 期望结果：req1 用 subReq1 分得 device2+device3，req2 用 subReq1 分得 device1；AllocateOne 26 次、Stable 32 次。
		"check-combinations-with-prioritized-list": {
			features: Features{
				PrioritizedList: true,
			},
			claimsToAllocate: objects(claim(claim0).withRequests(
				// The first alternative can't be satisfied since there aren't enough devices
				// of type Y, but the allocator will try all devices before finding out.
				requestWithPrioritizedList(req1,
					subRequest(subReq0, classA, 3, resourceapi.DeviceSelector{
						CEL: &resourceapi.CELDeviceSelector{
							Expression: fmt.Sprintf(`device.attributes["%s"].type == "Y"`, driverA),
						},
					}),
					subRequest(subReq1, classA, 2),
				),
				// The first subrequest asks for too many devices, so the second
				// have to be chosen.
				requestWithPrioritizedList(req2,
					subRequest(subReq0, classA, 2),
					subRequest(subReq1, classA, 1, resourceapi.DeviceSelector{
						CEL: &resourceapi.CELDeviceSelector{
							Expression: fmt.Sprintf(`device.attributes["%s"].type == "X"`, driverA),
						},
					}),
				),
			)),
			classes: objects(class(classA, driverA)),
			// The order of devices is chosen such that the allocator
			// will initially pick {device1, device2} for req1. This will fail
			// because req-2 needs device1. The allocator has to
			// backtrack. The correct solution is {device2, device3} for req-1
			// and {device1} for req-2. The optimized allocator avoids
			// testing {device2, device1} for req-1 and thus finds the solution
			// faster.
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"type": {StringValue: ptr.To("X")},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"type": {StringValue: ptr.To("Y")},
				}),
				device(device3, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"type": {StringValue: ptr.To("Y")},
				}),
			)),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req1SubReq1, driverA, pool1, device2, false),
				deviceAllocationResult(req1SubReq1, driverA, pool1, device3, false),
				deviceAllocationResult(req2SubReq1, driverA, pool1, device1, false),
			)},
			expectNumAllocateOneInvocations: 26,
			expectNumAllocateOneInvocationsByChannel: map[internal.AllocatorChannel]int64{
				internal.Stable: 32,
			},
		},

		// "derived-attributes" is the primary test case. It mirrors the
		// cross-request hardware alignment example from the KEP.
		// It defines two requests and synthesizes a virtual grouping
		// key ("sharedNumaNode") across their differing
		// attribute names. The allocator must backtrack past the
		// incompatible first device to successfully co-allocate.
		// 本用例验证跨请求派生属性映射可实现硬件对齐（KEP 示例的简化版）。
		// 特性开关：DerivedAttributes 开启。
		// 构造条件：req0 从 driverA 的 numa、req1 从 driverB 的 numaNode 派生同名属性 derived/sharedNumaNode 并要求匹配，首台不兼容设备需回溯。
		// 期望结果：req0 分配 numa-1 的 device1，req1 分配 numa-1 的 device3。
		"derived-attributes": {
			features: Features{
				DerivedAttributes: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{
					{
						Requests:       []string{req0, req1},
						MatchAttribute: ptr.To(resourceapi.FullyQualifiedName("derived/sharedNumaNode")),
					},
				},
				requestWithDerivedAttributes(
					req0,
					classA,
					1,
					[]resourceapi.DeviceDerivedAttribute{
						{
							Name:       "derived/sharedNumaNode",
							Expression: fmt.Sprintf(`device.attributes["%s"]["numa"]`, driverA),
						},
					},
				),
				requestWithDerivedAttributes(
					req1,
					classB,
					1,
					[]resourceapi.DeviceDerivedAttribute{
						{
							Name:       "derived/sharedNumaNode",
							Expression: fmt.Sprintf(`device.attributes["%s"]["numaNode"]`, driverB),
						},
					},
				),
			)),
			classes: objects(
				class(classA, driverA),
				class(classB, driverB),
			),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"numa": {StringValue: new("numa-0")},
					}),
					device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"numa": {StringValue: new("numa-1")},
					}),
				),
				sliceWithDevices(slice2, node1, pool2, driverB,
					device(device3, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"numaNode": {StringValue: new("numa-1")},
					}),
					device(device4, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"numaNode": {StringValue: new("numa-2")},
					}),
				),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0, driverA, pool1, device1, false),
				deviceAllocationResult(req1, driverB, pool2, device3, false),
			)},
		},
		// "derived-attributes-with-prioritized-list" verifies that
		// the allocator correctly evaluates derived attributes that are
		// defined differently across alternatives in a prioritized list.
		// It expects the allocator to skip the first alternative (because its
		// derived attribute violates the cross-request match constraint)
		// and successfully allocate using the second alternative which
		// correctly satisfies the constraint with the other request.
		// 本用例验证优先列表各备选可定义不同的派生属性。
		// 特性开关：DerivedAttributes 与 PrioritizedList 开启。
		// 构造条件：req0 第一备选派生 foo=A（与 req1 的 baz=B 不匹配），第二备选派生 bar=B；两请求要求匹配 derived/match。
		// 期望结果：跳过第一备选，req0 用 subReq1 分配 device1，req1 分配 device2。
		"derived-attributes-with-prioritized-list": {
			features: Features{
				DerivedAttributes: true,
				PrioritizedList:   true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{
					{
						Requests:       []string{req0, req1},
						MatchAttribute: ptr.To(resourceapi.FullyQualifiedName("derived/match")),
					},
				},
				requestWithPrioritizedList(req0,
					// First alternative pulls physical attribute "foo" ("A").
					// This won't match req1 which pulls "baz" ("B").
					subRequest(subReq0, classA, 1, []resourceapi.DeviceDerivedAttribute{
						{
							Name:       resourceapi.FullyQualifiedName("derived/match"),
							Expression: fmt.Sprintf(`device.attributes["%s"]["foo"]`, driverA),
						},
					}),
					// Second alternative pulls physical attribute "bar" ("B").
					// This matches req1.
					subRequest(subReq1, classA, 1, []resourceapi.DeviceDerivedAttribute{
						{
							Name:       resourceapi.FullyQualifiedName("derived/match"),
							Expression: fmt.Sprintf(`device.attributes["%s"]["bar"]`, driverA),
						},
					}),
				).obj(),
				requestWithDerivedAttributes(
					req1,
					classB, // Notice req1 uses classB (driverB)
					1,
					[]resourceapi.DeviceDerivedAttribute{
						{
							Name:       resourceapi.FullyQualifiedName("derived/match"),
							Expression: fmt.Sprintf(`device.attributes["%s"]["baz"]`, driverB),
						},
					},
				),
			)),
			classes: objects(
				class(classA, driverA),
				class(classB, driverB),
			),
			slices: unwrapResourceSlices(
				sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"foo": {StringValue: new("A")},
						"bar": {StringValue: new("B")},
					}),
				),
				sliceWithDevices(slice2, node1, pool2, driverB,
					device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
						"baz": {StringValue: new("B")},
					}),
				),
			),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				deviceAllocationResult(req0SubReq1, driverA, pool1, device1, false),
				deviceAllocationResult(req1, driverB, pool2, device2, false),
			)},
		},
		// "derived-attributes-override" verifies that derived
		// attributes shadow/override physical attributes of the same
		// name. It also proves that this shadowing is strictly
		// request-scoped and does not leak or affect other requests
		// (which continue to fall back to physical attributes).
		// 本用例验证派生属性会遮蔽同名物理属性，且遮蔽仅对当前请求生效。
		// 特性开关：DerivedAttributes 开启。
		// 构造条件：req0 把 dra.example.com/myAttr 派生为常量 value-2，req1 未派生故使用物理值；两设备物理值为 value-1/value-2。
		// 期望结果：req0 可分配 device1（派生值恒为 value-2），req1 只能分配物理值为 value-2 的 device2。
		"derived-attributes-override": {
			features: Features{
				DerivedAttributes: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{
					{
						// Require match on "myAttr".
						Requests:       []string{req0, req1},
						MatchAttribute: ptr.To(resourceapi.FullyQualifiedName("dra.example.com/myAttr")),
					},
				},
				requestWithDerivedAttributes(
					req0,
					classA,
					1,
					[]resourceapi.DeviceDerivedAttribute{
						{
							// Overrides physical attribute to "value-2".
							Name:       resourceapi.FullyQualifiedName("dra.example.com/myAttr"),
							Expression: `"value-2"`,
						},
					},
				),
				// Falls back to physical attribute.
				request(req1, classA, 1),
			)),
			classes: objects(class(classA, "dra.example.com")),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, "dra.example.com",
				// Mismatched physical attributes.
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"myAttr": {StringValue: new("value-1")},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"myAttr": {StringValue: new("value-2")},
				}),
			)),
			node: node(node1, region1),
			expectResults: []any{allocationResult(
				localNodeSelector(node1),
				// req0 overridden to "value-2" -> device1. req1 uses physical "value-2" -> device2.
				deviceAllocationResult(req0, "dra.example.com", pool1, device1, false),
				deviceAllocationResult(req1, "dra.example.com", pool1, device2, false),
			)},
		},
		// "derived-attributes-cel-missing-attribute" verifies that
		// CEL runtime evaluation errors (such as looking up a key
		// that isn't defined on a candidate device) are caught
		// immediately. The allocator aborts allocation and
		// propagates the explicit CEL error, preventing silent
		// topology matching failures.
		// 本用例验证派生属性 CEL 表达式引用不存在的属性时立即报错。
		// 特性开关：DerivedAttributes 开启。
		// 构造条件：req0 的派生表达式读取 missingAttr，而设备上只有 existingAttr。
		// 期望结果：expectError 匹配『no such key: missingAttr』，不静默忽略拓扑匹配失败。
		"derived-attributes-cel-missing-attribute": {
			features: Features{
				DerivedAttributes: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{
					{
						Requests:       []string{req0},
						MatchAttribute: ptr.To(resourceapi.FullyQualifiedName("derived/missing")),
					},
				},
				requestWithDerivedAttributes(
					req0,
					classA,
					1,
					[]resourceapi.DeviceDerivedAttribute{
						{
							Name:       resourceapi.FullyQualifiedName("derived/missing"),
							Expression: fmt.Sprintf(`device.attributes["%s"]["missingAttr"]`, driverA),
						},
					},
				),
			)),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"existingAttr": {StringValue: new("yes")},
				}),
			)),
			node:        node(node1, region1),
			expectError: gomega.MatchError(gomega.ContainSubstring("no such key: missingAttr")),
		},
		// "derived-attributes-type-mismatch" verifies that when CEL
		// dynamically evaluates to different value types for the same
		// attribute across candidate devices (e.g., string vs int),
		// the MatchAttribute constraint safely rejects the mismatch
		// rather than panicking or throwing an unhandled error.
		// 本用例验证同一派生属性在不同设备上求值类型不一致时被安全拒绝。
		// 特性开关：DerivedAttributes 开启。
		// 构造条件：req0 请求 2 台，派生 derived/myAttr 读取 static；device1 为字符串、device2 为整数。
		// 期望结果：类型不一致违反约束，expectResults 为 nil，不 panic。
		"derived-attributes-type-mismatch": {
			features: Features{
				DerivedAttributes: true,
			},
			claimsToAllocate: objects(claimWithRequests(
				claim0,
				[]resourceapi.DeviceConstraint{
					{
						Requests:       []string{req0},
						MatchAttribute: ptr.To(resourceapi.FullyQualifiedName("derived/myAttr")),
					},
				},
				requestWithDerivedAttributes(
					req0,
					classA,
					2,
					[]resourceapi.DeviceDerivedAttribute{
						{
							Name:       resourceapi.FullyQualifiedName("derived/myAttr"),
							Expression: fmt.Sprintf(`device.attributes["%s"]["static"]`, driverA),
						},
					},
				),
			)),
			classes: objects(class(classA, driverA)),
			slices: unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
				device(device1, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"static": {StringValue: new("value")},
				}),
				device(device2, nil, map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					"static": {IntValue: new(int64(1))},
				}),
			)),
			node:          node(node1, region1),
			expectResults: nil,
		},
	}

	// 按各实现声明支持的特性运行全部分配用例。
	RunTestAllocator(t, supportedFeatures, newAllocator, testcases)

	// interrupt 子测试覆盖正常、即将超时、已到截止时间、已取消四种上下文状态，
	// 验证分配器在中断信号下要么正常完成，要么返回与上下文一致的错误。
	t.Run("interrupt", func(t *testing.T) {
		for _, name := range []string{"off", "timeout", "deadline", "cancel"} {
			t.Run(name, func(t *testing.T) {
				_, ctx := ktesting.NewTestContext(t)
				g := gomega.NewWithT(t)

				// This testcase is a smaller variant of the one in https://github.com/kubernetes/kubernetes/issues/131730#issuecomment-2873598287.
				// That one took over 30 seconds, this one here only 0.07 seconds.
				// But even that is too long when we interrupt in the near future or
				// even before starting...
				classLister := informerLister[resourceapi.DeviceClass]{
					objs: []*resourceapi.DeviceClass{class(classA, driverA)},
				}
				claimsToAllocate := unwrap(claimWithRequests(claim0, nil,
					request(req0, classA, 6),
				))
				slices := unwrapResourceSlices(sliceWithDevices(slice1, node1, pool1, driverA,
					device(device1, nil, nil),
					device(device2, nil, nil),
					device(device3, nil, nil),
					device(device4, nil, nil),
					device("device-5", nil, nil),
				))
				node := node(node1, region1)

				// 按场景构造已结束或即将结束的上下文。
				switch name {
				case "off":
				case "timeout":
					c, cancel := context.WithTimeout(ctx, time.Nanosecond)
					defer cancel()
					ctx = c
				case "deadline":
					c, cancel := context.WithDeadline(ctx, time.Now())
					defer cancel()
					ctx = c
				case "cancel":
					c, cancel := context.WithCancel(ctx)
					cancel()
					ctx = c
				}

				// 构造分配器并立即执行一次代价较高的分配以观察中断行为。
				allocator, err := newAllocator(ctx, Features{}, AllocatedState{}, classLister, slices, cel.NewCache(1, cel.Features{}))
				g.Expect(err).ToNot(gomega.HaveOccurred())
				_, err = allocator.Allocate(ctx, node, claimsToAllocate)
				t.Logf("got error %v", err)
				// 上下文已结束时要求错误链包含 ctx.Err()，否则要求分配不报错。
				if ctx.Err() != nil {
					if !errors.Is(err, ctx.Err()) {
						t.Fatalf("expected %v, got error: %v", ctx.Err(), err)
					}
				} else {
					if err != nil {
						t.Fatalf("expected no error, got %v", err)
					}
				}
			})
		}
	})
}

// RunTestAllocator 遍历 testcases 并以子测试方式运行每个用例：
// 检查所需特性是否受支持，构造确定性的 Lister 与分配前状态，执行分配，
// 比对结果与错误，并校验调用统计及输入对象未被修改。
func RunTestAllocator(t *testing.T,
	supportedFeatures Features,
	newAllocator func(
		ctx context.Context,
		features Features,
		allocateState AllocatedState,
		classLister DeviceClassLister,
		slices []*resourceapi.ResourceSlice,
		celCache *cel.Cache,
	) (Allocator, error),
	testcases map[string]AllocatorTestCase) {
	// 逐个用例运行，彼此独立以方便单独定位失败。
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			g := gomega.NewWithT(t)

			// 计算用例所需特性与当前实现支持特性的差集。
			required := tc.features.Set()
			supported := supportedFeatures.Set()
			missing := required.Difference(supported)
			// 缺少任一必需特性则跳过，否则断言必然失败。
			if missing.Len() > 0 {
				// Skip the test, at least one of its required features isn't supported
				// and the test would fail.
				t.Skipf("SKIP: required feature(s) %v not supported by allocator", sets.List(missing))
			}

			// Listing objects is deterministic and returns them in the same
			// order as in the test case. That makes the allocation result
			// also deterministic.
			// 按用例声明的顺序填充类 Lister，保证分配结果可复现。
			var classLister informerLister[resourceapi.DeviceClass]
			for _, class := range tc.classes {
				classLister.objs = append(classLister.objs, class.DeepCopy())
			}
			claimsToAllocate := slices.Clone(tc.claimsToAllocate)
			allocatedDevices := slices.Clone(tc.allocatedDevices)
			allocatedShare := tc.allocatedCapacityDevices.Clone()
			var slices []*resourceapi.ResourceSlice
			if tc.slices != nil {
				slices = make([]*resourceapi.ResourceSlice, len(tc.slices))
				for i, slice := range tc.slices {
					slices[i] = slice.DeepCopy()
				}
			}
			// 汇总已分配设备、共享设备 ID 与聚合容量作为分配前状态。
			allocatedState := AllocatedState{
				AllocatedDevices:         sets.New(allocatedDevices...),
				AllocatedSharedDeviceIDs: tc.allocatedSharedDeviceIDs,
				AggregatedCapacity:       allocatedShare,
			}
			// 使用用例特性创建分配器，CEL 缓存按特性开关启用相应求值能力。
			allocator, err := newAllocator(ctx, tc.features, allocatedState, classLister, slices, cel.NewCache(1, cel.Features{
				EnableConsumableCapacity: tc.features.ConsumableCapacity,
				EnableListTypeAttributes: tc.features.ListTypeAttributes,
			}))
			g.Expect(err).ToNot(gomega.HaveOccurred())

			// 需要校验调用次数时，分配器必须实现扩展统计接口，否则跳过。
			if _, ok := allocator.(internal.AllocatorExtended); tc.expectNumAllocateOneInvocations > 0 && !ok {
				t.Skipf("%T does not support the AllocatorStats interface", allocator)
			}
			if tc.node == nil {
				// 未指定节点时回退到默认节点。
				tc.node = node(node1, region1)
			}
			// 执行分配并准备对错误进行匹配。
			results, err := allocator.Allocate(ctx, tc.node, unwrap(claimsToAllocate...))
			matchError := tc.expectError
			if matchError == nil {
				matchError = gomega.Not(gomega.HaveOccurred())
			}
			// 按期望匹配错误：未声明期望错误时要求分配成功。
			g.Expect(err).To(matchError)

			t.Logf("name: %s", name)
			// replace any share id with fixed value for testing
			// 将不确定的 ShareID 规范化为固定值并记录消耗的容量，便于结果比对。
			for ri, result := range results {
				for ai, allocation := range result.Devices.Results {
					if allocation.ShareID != nil {
						results[ri].Devices.Results[ai].ShareID = &fixedShareID
					}
					t.Logf("allocated capacity: %v", allocation.ConsumedCapacity)
				}
			}
			// 分配结果与期望结果按顺序无关的方式比对。
			g.Expect(results).To(gomega.ConsistOf(tc.expectResults...))

			// Objects that the allocator had access to should not have been modified.
			// 校验分配器没有修改输入对象。
			g.Expect(claimsToAllocate).To(gomega.HaveExactElements(tc.claimsToAllocate))
			g.Expect(allocatedDevices).To(gomega.HaveExactElements(tc.allocatedDevices))
			g.Expect(slices).To(gomega.Equal(tc.slices))
			g.Expect(classLister.objs).To(gomega.ConsistOf(tc.classes))

			// 校验 AllocateOne 调用次数，允许按分配器通道使用覆盖值。
			if expectNumAllocateOneInvocations := tc.expectNumAllocateOneInvocations; expectNumAllocateOneInvocations > 0 {
				stats := allocator.(internal.AllocatorExtended).GetStats()
				if override, ok := tc.expectNumAllocateOneInvocationsByChannel[allocator.Channel()]; ok {
					expectNumAllocateOneInvocations = override
				}
				g.Expect(stats.NumAllocateOneInvocations).To(gomega.Equal(expectNumAllocateOneInvocations))
			}
		})
	}
}

// informerLister 是轻量的泛型 Lister 测试替身：按切片顺序保存对象，
// 并可注入错误，用于模拟 DeviceClassLister 等 informer 视图。
type informerLister[T any] struct {
	objs []*T
	err  error
}

// List 返回内部保存的对象切片及注入的错误，顺序与构造时一致。
func (l informerLister[T]) List() (ret []*T, err error) {
	return l.objs, l.err
}

// Get 按 metadata.name 查找对象，未找到时返回标准的 NotFound 错误。
func (l informerLister[T]) Get(name string) (*T, error) {
	for _, obj := range l.objs {
		// 通过 meta.Accessor 提取对象名进行匹配。
		accessor, err := meta.Accessor(obj)
		if err != nil {
			return nil, err
		}
		if accessor.GetName() == name {
			return obj, nil
		}
	}
	// 遍历完仍未匹配则返回 NotFound。
	return nil, apierrors.NewNotFound(schema.GroupResource{}, "not found")
}
