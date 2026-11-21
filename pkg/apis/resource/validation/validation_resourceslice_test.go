/*
Copyright 2022 The Kubernetes Authors.

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

package validation

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/apis/core"
	resourceapi "k8s.io/kubernetes/pkg/apis/resource"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/utils/ptr"

	_ "k8s.io/kubernetes/pkg/apis/resource/install"
)

// testAttributes 返回覆盖四种标量类型的合法属性集，作为各用例的公共底座，避免重复构造。
func testAttributes() map[resourceapi.QualifiedName]resourceapi.DeviceAttribute {
	return map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
		"int":     {IntValue: ptr.To(int64(42))},
		"string":  {StringValue: ptr.To("hello world")},
		"version": {VersionValue: ptr.To("1.2.3")},
		"bool":    {BoolValue: ptr.To(true)},
	}
}

// testCapacity 返回单一合法容量，减少用例样板代码。
func testCapacity() map[resourceapi.QualifiedName]resourceapi.DeviceCapacity {
	return map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
		"memory": {Value: resource.MustParse("1Gi")},
	}
}

// testCounters 返回单一合法 counter，供共享计数与消费计数用例复用。
func testCounters() map[string]resourceapi.Counter {
	return map[string]resourceapi.Counter{
		"memory": {Value: resource.MustParse("1Gi")},
	}
}

// testMultipleCounters 按数量生成 counter，专用于数量上限相关用例。
func testMultipleCounters(count int) map[string]resourceapi.Counter {
	counters := make(map[string]resourceapi.Counter)
	for i := 0; i < count; i++ {
		counters[fmt.Sprintf("memory-%d", i)] = resourceapi.Counter{Value: resource.MustParse("1Gi")}
	}
	return counters
}

// testResourceSlice 构造字段齐全的合法 ResourceSlice；各用例只做最小化改动，便于定位失败原因。
func testResourceSlice(name, nodeName, driverName string, numDevices int) *resourceapi.ResourceSlice {
	slice := &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: resourceapi.ResourceSliceSpec{
			NodeName: &nodeName,
			Driver:   driverName,
			Pool: resourceapi.ResourcePool{
				Name:               nodeName,
				ResourceSliceCount: 1,
			},
		},
	}
	for d := range numDevices {
		device := resourceapi.Device{
			Name:       fmt.Sprintf("device-%d", d),
			Attributes: testAttributes(),
			Capacity:   testCapacity(),
		}
		slice.Spec.Devices = append(slice.Spec.Devices, device)
	}
	return slice
}

// testResourceSliceWithSharedCounters 去掉 devices 改用 sharedCounters，构造共享计数场景。
func testResourceSliceWithSharedCounters(name, poolName, driverName string, numCounterSets int) *resourceapi.ResourceSlice {
	slice := testResourceSlice(name, poolName, driverName, 1)
	slice.Spec.Devices = nil
	for i := 0; i < numCounterSets; i++ {
		counterSet := resourceapi.CounterSet{
			Name:     fmt.Sprintf("counterset-%d", i),
			Counters: testCounters(),
		}
		slice.Spec.SharedCounters = append(slice.Spec.SharedCounters, counterSet)
	}
	return slice
}

// testResourceSliceWithBindingConditions 给所有设备统一设置绑定条件，便于验证成对与互斥规则。
func testResourceSliceWithBindingConditions(name, nodeName, driverName string, numDevices int, bindingConditions, bindingFailureConditions []string) *resourceapi.ResourceSlice {
	slice := testResourceSlice(name, nodeName, driverName, numDevices)
	for i := range slice.Spec.Devices {
		slice.Spec.Devices[i].BindingConditions = bindingConditions
		slice.Spec.Devices[i].BindingFailureConditions = bindingFailureConditions
	}
	return slice
}

// testNodeAllocatableResourceCapacity 提供映射所需的容量键，使引用校验有据可依。
func testNodeAllocatableResourceCapacity() map[resourceapi.QualifiedName]resourceapi.DeviceCapacity {
	return map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
		"dra.example.com/cpu":    {Value: resource.MustParse("100")},
		"dra.example.com/memory": {Value: resource.MustParse("100Gi")},
	}
}

// testResourceSliceWithNodeAllocatableResources 构造带节点可分配资源映射的设备，用于映射相关校验。
func testResourceSliceWithNodeAllocatableResources(name, nodeName, driverName string, numDevices int) *resourceapi.ResourceSlice {
	slice := &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: resourceapi.ResourceSliceSpec{
			NodeName: &nodeName,
			Driver:   driverName,
			Pool: resourceapi.ResourcePool{
				Name:               nodeName,
				ResourceSliceCount: 1,
			},
		},
	}
	for d := range numDevices {
		device := resourceapi.Device{
			Name:       fmt.Sprintf("device-%d", d),
			Attributes: testAttributes(),
			Capacity:   testNodeAllocatableResourceCapacity(),
			NodeAllocatableResources: map[v1.ResourceName]resourceapi.NodeAllocatableResource{
				v1.ResourceCPU: {
					Mapping: &resourceapi.NodeAllocatableMapping{
						CapacityMultiplier: new(resource.MustParse("1")),
						CapacityKey:        new[resourceapi.QualifiedName]("dra.example.com/cpu"),
					},
				},
				"memory": {
					Mapping: &resourceapi.NodeAllocatableMapping{
						CapacityKey:        new[resourceapi.QualifiedName]("dra.example.com/cpu"),
						CapacityMultiplier: new(resource.MustParse("1")),
					},
				},
			},
		}
		slice.Spec.Devices = append(slice.Spec.Devices, device)
	}
	return slice
}

// testResourceSliceWithConsumableCapacity 构造允许多次分配且带 requestPolicy 的容量，用于可消费容量校验。
func testResourceSliceWithConsumableCapacity(name, nodeName, driverName string, numDevices int) *resourceapi.ResourceSlice {
	slice := &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: resourceapi.ResourceSliceSpec{
			NodeName: &nodeName,
			Driver:   driverName,
			Pool: resourceapi.ResourcePool{
				Name:               nodeName,
				ResourceSliceCount: 1,
			},
		},
	}
	for d := range numDevices {
		device := resourceapi.Device{
			Name:                     fmt.Sprintf("device-%d", d),
			AllowMultipleAllocations: new(true),
			Capacity: map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
				"a": {
					Value: resource.MustParse("100"),
					RequestPolicy: &resourceapi.CapacityRequestPolicy{
						Default: new(resource.MustParse("10")),
						ValidValues: []resource.Quantity{
							resource.MustParse("5"),
							resource.MustParse("10"),
							resource.MustParse("100"),
						},
					},
				},
			},
		}
		slice.Spec.Devices = append(slice.Spec.Devices, device)
	}
	return slice
}

// updateConsumableCapacity 深拷贝后修改指定设备的容量：map 中的值不可原地改字段，否则会污染其他用例。
func updateConsumableCapacity(slice *resourceapi.ResourceSlice, deviceNum int, update func(cap *resourceapi.DeviceCapacity)) {
	cap := slice.Spec.Devices[deviceNum].Capacity["a"]
	cap = *cap.DeepCopy()
	update(&cap)
	slice.Spec.Devices[deviceNum].Capacity["a"] = cap
}

// consumeableCapacityPath 统一构造容量字段路径，减少断言样板并与实现路径保持一致。
func consumeableCapacityPath(deviceNum int) *field.Path {
	return field.NewPath("spec", "devices").Index(deviceNum).Child("capacity").Key("a")
}

// TestValidateResourceSlice 覆盖创建校验的各个维度：元数据、节点选择、设备属性与容量、计数器、特性开关等。
func TestValidateResourceSlice(t *testing.T) {
	goodName := "foo"
	badName := "!@#$%^"
	driverName := "test.example.com"
	now := metav1.Now()
	badValue := "spaces not allowed"

	badMultipleListValue := resourceapi.DeviceAttribute{
		IntValues: []int64{1}, BoolValues: []bool{true},
	}
	badMultipleListValueWithEmptyList := resourceapi.DeviceAttribute{
		IntValues: []int64{1}, BoolValues: []bool{true}, StringValues: []string{},
	}
	badListStringValueTooLong := resourceapi.DeviceAttribute{
		StringValues: []string{strings.Repeat("x", resourceapi.DeviceAttributeMaxValueLength+1)},
	}
	badListVersionValueTooLong := resourceapi.DeviceAttribute{
		VersionValues: []string{strings.Repeat("x", resourceapi.DeviceAttributeMaxValueLength+1)},
	}
	badListAttributeTooManyValueWithMixedTypes := resourceapi.DeviceAttribute{
		// 单个属性键中混入多种类型，共 (ResourceSliceMaxAttributeValuesPerDevice+1) 个值。
		// 注意：这会同时触发 "exactly one value must be specified" 错误，
		//       以及 "the total number of attribute values must not exceed" 错误。
		StringValues:  slices.Repeat([]string{"x"}, resourceapi.ResourceSliceMaxAttributeValuesPerDevice-2),
		IntValues:     []int64{int64(1)},
		BoolValues:    []bool{true},
		VersionValues: []string{"1.0.0"},
	}

	// 场景表覆盖创建校验的各维度；wantFailures 为空表示期望通过。
	scenarios := map[string]struct {
		slice                                        *resourceapi.ResourceSlice
		wantFailures                                 field.ErrorList
		consumableCapacityFeatureGate                bool
		fractionalCapacityRangeFeatureGate           bool
		enableDRANodeAllocatableResourcesFeatureGate bool
	}{
		// 基线：满配设备且字段全部合法，确保校验器不误报。
		"good": {
			slice: testResourceSlice(goodName, goodName, driverName, resourceapi.ResourceSliceMaxDevices),
		},
		// 设备数超过 ResourceSliceMaxDevices，验证常规上限。
		"too-large": {
			wantFailures: field.ErrorList{field.TooMany(field.NewPath("spec", "devices"), resourceapi.ResourceSliceMaxDevices+1, resourceapi.ResourceSliceMaxDevices)},
			slice:        testResourceSlice(goodName, goodName, goodName, resourceapi.ResourceSliceMaxDevices+1),
		},
		// 带 taint 的设备上限更高，验证高级特性放宽限制。
		"good-taints": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, goodName, resourceapi.ResourceSliceMaxDevicesWithAdvancedFeatures)
				for i := range slice.Spec.Devices {
					slice.Spec.Devices[i].Taints = []resourceapi.DeviceTaint{{Key: "example.com/taint", Effect: resourceapi.DeviceTaintEffectNoExecute}}
				}
				return slice
			}(),
		},
		// 带 taint 时设备数超过放宽后的上限，验证放宽不等于无限制。
		"too-large-taints": {
			wantFailures: field.ErrorList{field.TooMany(field.NewPath("spec", "devices"), resourceapi.ResourceSliceMaxDevicesWithAdvancedFeatures+1, resourceapi.ResourceSliceMaxDevicesWithAdvancedFeatures)},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, goodName, resourceapi.ResourceSliceMaxDevicesWithAdvancedFeatures+1)
				for i := range slice.Spec.Devices {
					slice.Spec.Devices[i].Taints = []resourceapi.DeviceTaint{{Key: "example.com/taint", Effect: resourceapi.DeviceTaintEffectNoExecute}}
				}
				return slice
			}(),
		},
		// "string" 是裸键，会被限定到驱动自己的域名下。
		// 裸键 "string" 归到驱动域名下，验证限定名能匹配到该属性。
		"good-partition-type-attribute": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].ConsumesCounters = createConsumesCounters(1)
				slice.Spec.PartitionTypeAttribute = ptr.To(resourceapi.FullyQualifiedName(driverName + "/string"))
				return slice
			}(),
		},
		// 不消费 counter 的设备无需携带该属性。
		// 不消费 counter 的设备无需携带该属性，验证豁免逻辑。
		"good-partition-type-attribute-with-exempt-device": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 2)
				slice.Spec.Devices[0].ConsumesCounters = createConsumesCounters(1)
				slice.Spec.Devices[1].Attributes = nil
				slice.Spec.PartitionTypeAttribute = ptr.To(resourceapi.FullyQualifiedName(driverName + "/string"))
				return slice
			}(),
		},
		// 名称格式由声明式校验负责，这里只验证业务约束；
		// 相关覆盖见声明式校验测试。
		// 没有设备消费 counter 时设置该字段无意义，验证被拒绝。
		"partition-type-attribute-without-partitionable-devices": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("spec", "partitionTypeAttribute"), driverName+"/string", "may only be set on a slice which declares devices that consume counters")},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Spec.PartitionTypeAttribute = ptr.To(resourceapi.FullyQualifiedName(driverName + "/string"))
				return slice
			}(),
		},
		// 共享计数 slice 上没有可分区设备，验证同样不适用。
		"partition-type-attribute-on-shared-counters-slice": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("spec", "partitionTypeAttribute"), driverName+"/string", "may only be set on a slice which declares devices that consume counters")},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithSharedCounters(goodName, goodName, driverName, 1)
				slice.Spec.PartitionTypeAttribute = ptr.To(resourceapi.FullyQualifiedName(driverName + "/string"))
				return slice
			}(),
		},
		// 消费 counter 的设备必须提供被引用的属性，否则分区类型无从取值。
		"partition-type-attribute-missing-on-partitionable-device": {
			wantFailures: field.ErrorList{field.Required(field.NewPath("spec", "devices").Index(0).Child("attributes").Key("gpu.example.com/profile"), "device consumes counters and `partitionTypeAttribute` names this attribute, so it must be set")},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].ConsumesCounters = createConsumesCounters(1)
				slice.Spec.PartitionTypeAttribute = ptr.To(resourceapi.FullyQualifiedName("gpu.example.com/profile"))
				return slice
			}(),
		},
		// 被引用的属性必须是字符串值，验证非字符串被拒绝。
		"partition-type-attribute-not-a-string": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("spec", "devices").Index(0).Child("attributes").Key(driverName+"/int"), resourceapi.DeviceAttribute{IntValue: ptr.To(int64(42))}, "must be a string value because `partitionTypeAttribute` names this attribute")},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].ConsumesCounters = createConsumesCounters(1)
				slice.Spec.PartitionTypeAttribute = ptr.To(resourceapi.FullyQualifiedName(driverName + "/int"))
				return slice
			}(),
		},
		// 设备同时以裸键（合法字符串）和显式限定键（非字符串）声明该属性。
		// 显式形式优先，因此结果确定，
		// 不会因 map 遍历顺序而抖动。
		// 裸键与显式限定键并存时以显式形式为准，避免结果依赖 map 遍历顺序。
		"partition-type-attribute-explicit-form-wins": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("spec", "devices").Index(0).Child("attributes").Key(driverName+"/string"), resourceapi.DeviceAttribute{IntValue: ptr.To(int64(1))}, "must be a string value because `partitionTypeAttribute` names this attribute")},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].ConsumesCounters = createConsumesCounters(1)
				slice.Spec.Devices[0].Attributes[resourceapi.QualifiedName(driverName+"/string")] = resourceapi.DeviceAttribute{IntValue: ptr.To(int64(1))}
				slice.Spec.PartitionTypeAttribute = ptr.To(resourceapi.FullyQualifiedName(driverName + "/string"))
				return slice
			}(),
		},
		// name 缺失，验证对象身份必填。
		"missing-name": {
			wantFailures: field.ErrorList{field.Required(field.NewPath("metadata", "name"), "name or generateName is required")},
			slice:        testResourceSlice("", goodName, driverName, 1),
		},
		// name 不符合 RFC 1123 子域格式，验证命名约束。
		"bad-name": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("metadata", "name"), badName, "a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')")},
			slice:        testResourceSlice(badName, goodName, driverName, 1),
		},
		// generateName 与 name 共存合法，验证不会误拒。
		"generate-name": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.GenerateName = "prefix-"
				return slice
			}(),
		},
		// UID 由 apiserver 分配，验证校验器不干预服务端字段。
		"uid": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.UID = "ac051fac-2ead-46d9-b8b4-4e0fbeb7455d"
				return slice
			}(),
		},
		// resourceVersion 属于并发控制元数据，验证不干预。
		"resource-version": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.ResourceVersion = "1"
				return slice
			}(),
		},
		// generation 由 apiserver 维护，验证不干预。
		"generation": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Generation = 100
				return slice
			}(),
		},
		// creationTimestamp 由服务端生成，验证放行。
		"creation-timestamp": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.CreationTimestamp = now
				return slice
			}(),
		},
		// 删除宽限期是标准元数据，验证放行。
		"deletion-grace-period-seconds": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.DeletionGracePeriodSeconds = ptr.To[int64](10)
				return slice
			}(),
		},
		// ownerReferences 表示归属关系，验证不会被 DRA 校验误拒。
		"owner-references": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.OwnerReferences = []metav1.OwnerReference{
					{
						APIVersion: "v1",
						Kind:       "pod",
						Name:       "foo",
						UID:        "ac051fac-2ead-46d9-b8b4-4e0fbeb7455d",
					},
				}
				return slice
			}(),
		},
		// finalizers 是清理钩子，验证不会被误拒。
		"finalizers": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Finalizers = []string{
					"example.com/foo",
				}
				return slice
			}(),
		},
		// managedFields 由 server-side apply 填充，验证放行。
		"managed-fields": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.ManagedFields = []metav1.ManagedFieldsEntry{
					{
						FieldsType: "FieldsV1",
						Operation:  "Apply",
						APIVersion: "apps/v1",
						Manager:    "foo",
					},
				}
				return slice
			}(),
		},
		// 合法 label，验证元数据通用校验与 DRA 校验不冲突。
		"good-labels": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Labels = map[string]string{
					"apps.kubernetes.io/name": "test",
				}
				return slice
			}(),
		},
		// label 值含空格，验证元数据校验在 DRA 链路中仍然生效。
		"bad-labels": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("metadata", "labels"), badValue, "a valid label must be an empty string or consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyValue',  or 'my_value',  or '12345', regex used for validation is '(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?')")},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Labels = map[string]string{
					"hello-world": badValue,
				}
				return slice
			}(),
		},
		// 合法 annotation，验证不误拒。
		"good-annotations": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Annotations = map[string]string{
					"foo": "bar",
				}
				return slice
			}(),
		},
		// annotation key 非法，验证元数据校验生效。
		"bad-annotations": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("metadata", "annotations"), badName, "name part must consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyName',  or 'my.name',  or '123-abc', regex used for validation is '([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]')")},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Annotations = map[string]string{
					badName: "hello world",
				}
				return slice
			}(),
		},
		// helper 用同一字符串构造 nodeName 与 pool.name，验证两处同时报错。
		"bad-nodename": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "pool", "name"), badName, "a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')"),
				field.Invalid(field.NewPath("spec", "nodeName"), badName, "a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')"),
			},
			slice: testResourceSlice(goodName, badName, driverName, 1),
		},
		// 多段 pool 名含 "/"，在 nodeName 位置非法，验证各自格式约束独立生效。
		"bad-multi-pool-name": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "pool", "name"), badName, "a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')"),
				field.Invalid(field.NewPath("spec", "pool", "name"), badName, "a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')"),
				field.Invalid(field.NewPath("spec", "nodeName"), badName+"/"+badName, "a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')"),
			},
			slice: testResourceSlice(goodName, badName+"/"+badName, driverName, 1),
		},
		// pool 名达到长度上限但合法，验证边界值不越界。
		"good-pool-name": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Spec.Pool.Name = strings.Repeat("x", resourceapi.PoolNameMaxLength)
				return slice
			}(),
		},
		// pool 名超长、resourceSliceCount 为 0、generation 为负，一次覆盖多个字段校验。
		"bad-pool": {
			wantFailures: field.ErrorList{
				field.TooLongMaxLength(field.NewPath("spec", "pool", "name"), strings.Repeat("x/", resourceapi.PoolNameMaxLength/2)+"xy", resourceapi.PoolNameMaxLength),
				field.Invalid(field.NewPath("spec", "pool", "resourceSliceCount"), int64(0), "must be greater than zero"),
				field.Invalid(field.NewPath("spec", "pool", "generation"), int64(-1), "must be greater than or equal to zero"),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Spec.Pool.Name = strings.Repeat("x/", resourceapi.PoolNameMaxLength/2) + "xy"
				slice.Spec.Pool.ResourceSliceCount = 0
				slice.Spec.Pool.Generation = -1
				return slice
			}(),
		},
		// pool.name 必填，验证缺失被拦截。
		"missing-pool-name": {
			wantFailures: field.ErrorList{
				field.Required(field.NewPath("spec", "pool", "name"), ""),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Spec.Pool.Name = ""
				return slice
			}(),
		},
		// 空 nodeSelector 同时触发 core 与 DRA 两层校验，验证两层都生效。
		"bad-empty-node-selector": {
			wantFailures: field.ErrorList{
				field.Required(field.NewPath("spec", "nodeSelector", "nodeSelectorTerms"), "must have at least one node selector term"),                             // 来自 core 校验。
				field.Invalid(field.NewPath("spec", "nodeSelector", "nodeSelectorTerms"), []core.NodeSelectorTerm(nil), "must have exactly one node selector term"), // 来自 DRA 校验。
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Spec.NodeName = nil
				slice.Spec.NodeSelector = &core.NodeSelector{}
				return slice
			}(),
		},
		// nodeName 与 nodeSelector 同时设置，验证节点选择字段互斥。
		"bad-node-selection": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("spec"), "{`nodeName`, `nodeSelector`}", "exactly one of `nodeName`, `nodeSelector`, `allNodes`, `perDeviceNodeSelection` is required, but multiple fields are set")},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Spec.NodeName = ptr.To("worker")
				slice.Spec.NodeSelector = &core.NodeSelector{
					NodeSelectorTerms: []core.NodeSelectorTerm{{MatchFields: []core.NodeSelectorRequirement{{Key: "metadata.name", Operator: core.NodeSelectorOpIn, Values: []string{"worker"}}}}},
				}
				return slice
			}(),
		},
		// nodeName 与 allNodes 同时设置，验证互斥覆盖 allNodes。
		"bad-node-selection-all-nodes": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("spec"), "{`nodeName`, `allNodes`}", "exactly one of `nodeName`, `nodeSelector`, `allNodes`, `perDeviceNodeSelection` is required, but multiple fields are set")},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Spec.NodeName = ptr.To("worker")
				slice.Spec.AllNodes = ptr.To(true)
				return slice
			}(),
		},
		// 所有节点选择字段为空，验证必须且只能指定一种。
		"empty-node-selection": {
			wantFailures: field.ErrorList{field.Required(field.NewPath("spec"), "exactly one of `nodeName`, `nodeSelector`, `allNodes`, `perDeviceNodeSelection` is required")},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Spec.NodeName = nil
				return slice
			}(),
		},
		// driver 名非法，验证域名格式校验。
		"bad-drivername": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("spec", "driver"), badName, "a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')")},
			slice:        testResourceSlice(goodName, goodName, badName, 1),
		},
		// sharedCounters 与 devices 互斥，验证不能同时出现。
		"both-shared-counters-and-devices": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec"), "", "only one of `sharedCounters` or `devices` is allowed"),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Spec.SharedCounters = []resourceapi.CounterSet{
					{
						Name:     "counterset",
						Counters: testCounters(),
					},
				}
				return slice
			}(),
		},
		// 设备名非法，验证每个设备名按 RFC 1123 label 校验。
		"bad-devices": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices").Index(1).Child("name"), badName, "a lowercase RFC 1123 label must consist of lower case alphanumeric characters or '-', and must start and end with an alphanumeric character (e.g. 'my-name',  or '123-abc', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?')"),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, goodName, 3)
				slice.Spec.Devices[1].Name = badName
				return slice
			}(),
		},
		// 覆盖属性键非法、union 多值、version 非法与超长、string 超长等多种错误。
		"bad-attribute": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices").Index(1).Child("attributes"), badName, "a valid C identifier must start with alphabetic character or '_', followed by a string of alphanumeric characters or '_' (e.g. 'my_name',  or 'MY_NAME',  or 'MyName', regex used for validation is '[A-Za-z_][A-Za-z0-9_]*')"),
				field.Invalid(field.NewPath("spec", "devices").Index(1).Child("attributes").Key(badName), "", "exactly one value must be specified").MarkCoveredByDeclarative(),
				field.Invalid(field.NewPath("spec", "devices").Index(2).Child("attributes").Key(goodName), resourceapi.DeviceAttribute{StringValue: ptr.To("x"), VersionValue: ptr.To("1.2.3")}, "exactly one value must be specified").MarkCoveredByDeclarative(),
				field.Invalid(field.NewPath("spec", "devices").Index(3).Child("attributes").Key(goodName).Child("version"), strings.Repeat("x", resourceapi.DeviceAttributeMaxValueLength+1), "must be a string compatible with semver.org spec 2.0.0"),
				field.TooLongMaxLength(field.NewPath("spec", "devices").Index(3).Child("attributes").Key(goodName).Child("version"), strings.Repeat("x", resourceapi.DeviceAttributeMaxValueLength+1), resourceapi.DeviceAttributeMaxValueLength),
				field.TooLongMaxLength(field.NewPath("spec", "devices").Index(4).Child("attributes").Key(goodName).Child("string"), strings.Repeat("x", resourceapi.DeviceAttributeMaxValueLength+1), resourceapi.DeviceAttributeMaxValueLength),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, goodName, 5)
				slice.Spec.Devices[1].Attributes = map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					resourceapi.QualifiedName(badName): {},
				}
				slice.Spec.Devices[2].Attributes = map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					resourceapi.QualifiedName(goodName): {StringValue: ptr.To("x"), VersionValue: ptr.To("1.2.3")},
				}
				slice.Spec.Devices[3].Attributes = map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					resourceapi.QualifiedName(goodName): {VersionValue: ptr.To(strings.Repeat("x", resourceapi.DeviceAttributeMaxValueLength+1))},
				}
				slice.Spec.Devices[4].Attributes = map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					resourceapi.QualifiedName(goodName): {StringValue: ptr.To(strings.Repeat("x", resourceapi.DeviceAttributeMaxValueLength+1))},
				}
				return slice
			}(),
		},
		// 四种列表属性的合法形态，验证列表值被接受。
		"good-list-attribute": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, goodName, 4)
				slice.Spec.Devices[0].Attributes = map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					resourceapi.QualifiedName(goodName): {BoolValues: []bool{true}},
				}
				slice.Spec.Devices[1].Attributes = map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					resourceapi.QualifiedName(goodName): {IntValues: []int64{1}},
				}
				slice.Spec.Devices[2].Attributes = map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					resourceapi.QualifiedName(goodName): {StringValues: []string{"x"}},
				}
				slice.Spec.Devices[3].Attributes = map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					resourceapi.QualifiedName(goodName): {VersionValues: []string{"1.2.3"}},
				}
				return slice
			}(),
		},
		// 列表属性的非法形态：多类型并存、空列表、超长值，验证联合类型与长度约束。
		"bad-list-attribute": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices").Index(0).Child("attributes").Key(goodName), badMultipleListValue, "exactly one value must be specified").WithOrigin("union").MarkCoveredByDeclarative(),
				field.Invalid(field.NewPath("spec", "devices").Index(1).Child("attributes").Key(goodName).Child("strings"), []string{}, "must not be empty if specified"),
				field.Invalid(field.NewPath("spec", "devices").Index(1).Child("attributes").Key(goodName), badMultipleListValueWithEmptyList, "exactly one value must be specified").WithOrigin("union").MarkCoveredByDeclarative(),
				field.TooLongMaxLength(field.NewPath("spec", "devices").Index(2).Child("attributes").Key(goodName).Child("strings").Index(0), badListStringValueTooLong.StringValues[0], resourceapi.DeviceAttributeMaxValueLength).WithOrigin("maxBytes").MarkCoveredByDeclarative(),
				field.Invalid(field.NewPath("spec", "devices").Index(3).Child("attributes").Key(goodName).Child("versions").Index(0), badListVersionValueTooLong.VersionValues[0], "must be a string compatible with semver.org spec 2.0.0"),
				field.TooLongMaxLength(field.NewPath("spec", "devices").Index(3).Child("attributes").Key(goodName).Child("versions").Index(0), badListVersionValueTooLong.VersionValues[0], resourceapi.DeviceAttributeMaxValueLength),
				field.Invalid(field.NewPath("spec", "devices").Index(4).Child("attributes").Key(goodName).Child("bools"), []bool{}, "must not be empty if specified"),
				field.Invalid(field.NewPath("spec", "devices").Index(5).Child("attributes").Key(goodName).Child("ints"), []int64{}, "must not be empty if specified"),
				field.Invalid(field.NewPath("spec", "devices").Index(6).Child("attributes").Key(goodName).Child("strings"), []string{}, "must not be empty if specified"),
				field.Invalid(field.NewPath("spec", "devices").Index(7).Child("attributes").Key(goodName).Child("versions"), []string{}, "must not be empty if specified"),
			},

			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, goodName, 8)
				// 同一属性键指定多个列表值
				slice.Spec.Devices[0].Attributes = map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					resourceapi.QualifiedName(goodName): badMultipleListValue,
				}
				slice.Spec.Devices[1].Attributes = map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					resourceapi.QualifiedName(goodName): badMultipleListValueWithEmptyList,
				}
				// 列表值（string 与 version）超长
				slice.Spec.Devices[2].Attributes = map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					resourceapi.QualifiedName(goodName): badListStringValueTooLong,
				}
				slice.Spec.Devices[3].Attributes = map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					resourceapi.QualifiedName(goodName): badListVersionValueTooLong,
				}
				// 非 nil 但为空的列表值
				slice.Spec.Devices[4].Attributes = map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					resourceapi.QualifiedName(goodName): {BoolValues: []bool{}},
				}
				slice.Spec.Devices[5].Attributes = map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					resourceapi.QualifiedName(goodName): {IntValues: []int64{}},
				}
				slice.Spec.Devices[6].Attributes = map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					resourceapi.QualifiedName(goodName): {StringValues: []string{}},
				}
				slice.Spec.Devices[7].Attributes = map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					resourceapi.QualifiedName(goodName): {VersionValues: []string{}},
				}
				return slice
			}(),
		},
		// 列表属性使设备上限提高，验证高级特性下的边界值。
		"max-devices-with-list-of-int-attributes": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, goodName, resourceapi.ResourceSliceMaxDevicesWithAdvancedFeatures)
				for i := range slice.Spec.Devices {
					slice.Spec.Devices[i].Attributes["ints"] = resourceapi.DeviceAttribute{IntValues: []int64{1, 2, 3}}
				}
				return slice
			}(),
		},
		// 列表属性场景下设备数超限，验证放宽后的上限仍被执行。
		"too-many-devices-with-list-of-int-attributes": {
			wantFailures: field.ErrorList{field.TooMany(field.NewPath("spec", "devices"), resourceapi.ResourceSliceMaxDevicesWithAdvancedFeatures+1, resourceapi.ResourceSliceMaxDevicesWithAdvancedFeatures)},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, goodName, resourceapi.ResourceSliceMaxDevicesWithAdvancedFeatures+1)
				slice.Spec.Devices[0].Attributes["ints"] = resourceapi.DeviceAttribute{IntValues: []int64{1, 2, 3}}
				return slice
			}(),
		},
		// bool 列表属性场景下的设备数边界值。
		"max-devices-with-list-of-bool-attributes": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, goodName, resourceapi.ResourceSliceMaxDevicesWithAdvancedFeatures)
				for i := range slice.Spec.Devices {
					slice.Spec.Devices[i].Attributes["bools"] = resourceapi.DeviceAttribute{BoolValues: []bool{true, false, true}}
				}
				return slice
			}(),
		},
		// bool 列表属性场景下设备数超限。
		"too-many-devices-with-list-of-bool-attributes": {
			wantFailures: field.ErrorList{field.TooMany(field.NewPath("spec", "devices"), resourceapi.ResourceSliceMaxDevicesWithAdvancedFeatures+1, resourceapi.ResourceSliceMaxDevicesWithAdvancedFeatures)},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, goodName, resourceapi.ResourceSliceMaxDevicesWithAdvancedFeatures+1)
				slice.Spec.Devices[0].Attributes["bools"] = resourceapi.DeviceAttribute{BoolValues: []bool{true, false, true}}
				return slice
			}(),
		},
		// string 列表属性场景下的设备数边界值。
		"max-devices-with-list-of-string-attributes": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, goodName, resourceapi.ResourceSliceMaxDevicesWithAdvancedFeatures)
				for i := range slice.Spec.Devices {
					slice.Spec.Devices[i].Attributes["strings"] = resourceapi.DeviceAttribute{StringValues: []string{"a", "b", "c"}}
				}
				return slice
			}(),
		},
		// string 列表属性场景下设备数超限。
		"too-many-devices-with-list-of-string-attributes": {
			wantFailures: field.ErrorList{field.TooMany(field.NewPath("spec", "devices"), resourceapi.ResourceSliceMaxDevicesWithAdvancedFeatures+1, resourceapi.ResourceSliceMaxDevicesWithAdvancedFeatures)},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, goodName, resourceapi.ResourceSliceMaxDevicesWithAdvancedFeatures+1)
				slice.Spec.Devices[0].Attributes["strings"] = resourceapi.DeviceAttribute{StringValues: []string{"a", "b", "c"}}
				return slice
			}(),
		},
		// version 列表属性场景下的设备数边界值。
		"max-devices-with-list-of-version-attributes": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, goodName, resourceapi.ResourceSliceMaxDevicesWithAdvancedFeatures)
				for i := range slice.Spec.Devices {
					slice.Spec.Devices[i].Attributes["versions"] = resourceapi.DeviceAttribute{VersionValues: []string{"1.0.0", "2.0.0", "3.0.0"}}
				}
				return slice
			}(),
		},
		// version 列表属性场景下设备数超限。
		"too-many-devices-with-list-of-version-attributes": {
			wantFailures: field.ErrorList{field.TooMany(field.NewPath("spec", "devices"), resourceapi.ResourceSliceMaxDevicesWithAdvancedFeatures+1, resourceapi.ResourceSliceMaxDevicesWithAdvancedFeatures)},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, goodName, resourceapi.ResourceSliceMaxDevicesWithAdvancedFeatures+1)
				slice.Spec.Devices[0].Attributes["versions"] = resourceapi.DeviceAttribute{VersionValues: []string{"1.0.0", "2.0.0", "3.0.0"}}
				return slice
			}(),
		},
		// 属性键的域名和 ID 都达到长度上限但合法，验证长度边界。
		"good-attribute-names": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, goodName, 2)
				slice.Spec.Devices[1].Attributes = map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					resourceapi.QualifiedName(strings.Repeat("x", resourceapi.DeviceMaxIDLength)):                                                                {StringValue: ptr.To("y")},
					resourceapi.QualifiedName(strings.Repeat("x", resourceapi.DeviceMaxDomainLength) + "/" + strings.Repeat("y", resourceapi.DeviceMaxIDLength)): {StringValue: ptr.To("z")},
				}
				return slice
			}(),
		},
		// 属性 ID 含 "." 且超长，验证 C 标识符格式与长度双重校验。
		"bad-attribute-c-identifier": {
			wantFailures: field.ErrorList{
				field.TooLongMaxLength(field.NewPath("spec", "devices").Index(1).Child("attributes"), strings.Repeat(".", resourceapi.DeviceMaxIDLength+1), resourceapi.DeviceMaxIDLength),
				field.Invalid(field.NewPath("spec", "devices").Index(1).Child("attributes"), strings.Repeat(".", resourceapi.DeviceMaxIDLength+1), "a valid C identifier must start with alphabetic character or '_', followed by a string of alphanumeric characters or '_' (e.g. 'my_name',  or 'MY_NAME',  or 'MyName', regex used for validation is '[A-Za-z_][A-Za-z0-9_]*')"),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, goodName, 2)
				slice.Spec.Devices[1].Attributes = map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					resourceapi.QualifiedName(strings.Repeat(".", resourceapi.DeviceMaxIDLength+1)): {StringValue: ptr.To("y")},
				}
				return slice
			}(),
		},
		// 属性域名含 "_" 且超长，验证域名格式与长度校验。
		"bad-attribute-domain": {
			wantFailures: field.ErrorList{
				field.TooLong(field.NewPath("spec", "devices").Index(1).Child("attributes"), strings.Repeat("_", resourceapi.DeviceMaxDomainLength+1), resourceapi.DeviceMaxDomainLength),
				field.Invalid(field.NewPath("spec", "devices").Index(1).Child("attributes"), strings.Repeat("_", resourceapi.DeviceMaxDomainLength+1), "a lowercase RFC 1123 subdomain must consist of lower case alphanumeric characters, '-' or '.', and must start and end with an alphanumeric character (e.g. 'example.com', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*')"),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, goodName, 2)
				slice.Spec.Devices[1].Attributes = map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					resourceapi.QualifiedName(strings.Repeat("_", resourceapi.DeviceMaxDomainLength+1) + "/y"): {StringValue: ptr.To("z")},
				}
				return slice
			}(),
		},
		// 域名与 ID 同时超长，验证两段长度分别报错。
		"bad-key-too-long": {
			wantFailures: field.ErrorList{
				field.TooLong(field.NewPath("spec", "devices").Index(1).Child("attributes"), strings.Repeat("x", resourceapi.DeviceMaxDomainLength+1), resourceapi.DeviceMaxDomainLength),
				field.TooLongMaxLength(field.NewPath("spec", "devices").Index(1).Child("attributes"), strings.Repeat("y", resourceapi.DeviceMaxIDLength+1), resourceapi.DeviceMaxIDLength),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, goodName, 2)
				slice.Spec.Devices[1].Attributes = map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					resourceapi.QualifiedName(strings.Repeat("x", resourceapi.DeviceMaxDomainLength+1) + "/" + strings.Repeat("y", resourceapi.DeviceMaxIDLength+1)): {StringValue: ptr.To("z")},
				}
				return slice
			}(),
		},
		// "/" 形式键的域名与 ID 均为空，验证两段非空约束。
		"bad-attribute-empty-domain-and-c-identifier": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices").Index(1).Child("attributes"), "", "the domain must not be empty"),
				field.Invalid(field.NewPath("spec", "devices").Index(1).Child("attributes"), "", "the name must not be empty"),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, goodName, 2)
				slice.Spec.Devices[1].Attributes = map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					resourceapi.QualifiedName("/"): {StringValue: ptr.To("z")},
				}
				return slice
			}(),
		},
		// attributes 与 capacity 各自未超限但合计超限，验证联合计数。
		"combined-attributes-capacity-length": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices").Index(3), resourceapi.ResourceSliceMaxAttributesAndCapacitiesPerDevice+1, fmt.Sprintf("the total number of attributes and capacities must not exceed %d", resourceapi.ResourceSliceMaxAttributesAndCapacitiesPerDevice)),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, goodName, 5)
				slice.Spec.Devices[0].Attributes = map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{}
				slice.Spec.Devices[0].Capacity = nil
				for i := 0; i < resourceapi.ResourceSliceMaxAttributesAndCapacitiesPerDevice; i++ {
					slice.Spec.Devices[0].Attributes[resourceapi.QualifiedName(fmt.Sprintf("attr_%d", i))] = resourceapi.DeviceAttribute{StringValue: ptr.To("x")}
				}
				slice.Spec.Devices[1].Attributes = map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{}
				slice.Spec.Devices[1].Capacity = map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{}
				quantity := resource.MustParse("1Gi")
				capacity := resourceapi.DeviceCapacity{Value: quantity}
				for i := 0; i < resourceapi.ResourceSliceMaxAttributesAndCapacitiesPerDevice; i++ {
					slice.Spec.Devices[1].Capacity[resourceapi.QualifiedName(fmt.Sprintf("cap_%d", i))] = capacity
				}

				// attributes 与 capacity 合计超限一个。
				slice.Spec.Devices[3].Attributes = slice.Spec.Devices[0].Attributes
				slice.Spec.Devices[3].Capacity = map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
					"cap": capacity,
				}
				return slice
			}(),
		},
		// 属性值总数达上限，验证按列表长度而非键数计数。
		"max-attribute-values-with-list": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices").Index(1), resourceapi.ResourceSliceMaxAttributeValuesPerDevice+1, fmt.Sprintf("the total number of attribute values must not exceed %d", resourceapi.ResourceSliceMaxAttributeValuesPerDevice)),
				field.Invalid(field.NewPath("spec", "devices").Index(2), resourceapi.ResourceSliceMaxAttributeValuesPerDevice+1, fmt.Sprintf("the total number of attribute values must not exceed %d", resourceapi.ResourceSliceMaxAttributeValuesPerDevice)),
				field.Invalid(field.NewPath("spec", "devices").Index(3), resourceapi.ResourceSliceMaxAttributeValuesPerDevice+1, fmt.Sprintf("the total number of attribute values must not exceed %d", resourceapi.ResourceSliceMaxAttributeValuesPerDevice)),
				field.Invalid(field.NewPath("spec", "devices").Index(4), resourceapi.ResourceSliceMaxAttributeValuesPerDevice+1, fmt.Sprintf("the total number of attribute values must not exceed %d", resourceapi.ResourceSliceMaxAttributeValuesPerDevice)),
				field.Invalid(field.NewPath("spec", "devices").Index(5), resourceapi.ResourceSliceMaxAttributeValuesPerDevice+1, fmt.Sprintf("the total number of attribute values must not exceed %d", resourceapi.ResourceSliceMaxAttributeValuesPerDevice)),
				field.Invalid(field.NewPath("spec", "devices").Index(6), resourceapi.ResourceSliceMaxAttributeValuesPerDevice+1, fmt.Sprintf("the total number of attribute values must not exceed %d", resourceapi.ResourceSliceMaxAttributeValuesPerDevice)),
				field.Invalid(field.NewPath("spec", "devices").Index(7), resourceapi.ResourceSliceMaxAttributeValuesPerDevice+1, fmt.Sprintf("the total number of attribute values must not exceed %d", resourceapi.ResourceSliceMaxAttributeValuesPerDevice)),
				field.Invalid(field.NewPath("spec", "devices").Index(8), resourceapi.ResourceSliceMaxAttributeValuesPerDevice+1, fmt.Sprintf("the total number of attribute values must not exceed %d", resourceapi.ResourceSliceMaxAttributeValuesPerDevice)),
				field.Invalid(field.NewPath("spec", "devices").Index(9), resourceapi.ResourceSliceMaxAttributeValuesPerDevice+1, fmt.Sprintf("the total number of attribute values must not exceed %d", resourceapi.ResourceSliceMaxAttributeValuesPerDevice)),
				field.Invalid(field.NewPath("spec", "devices").Index(9).Child("attributes").Key("mixed_list_attrs"), badListAttributeTooManyValueWithMixedTypes, "exactly one value must be specified").MarkCoveredByDeclarative(),
				field.Invalid(field.NewPath("spec", "devices").Index(10), resourceapi.ResourceSliceMaxAttributeValuesPerDevice+1, fmt.Sprintf("the total number of attribute values must not exceed %d", resourceapi.ResourceSliceMaxAttributeValuesPerDevice)),
				field.Invalid(field.NewPath("spec", "devices").Index(11), resourceapi.ResourceSliceMaxAttributeValuesPerDevice+1, fmt.Sprintf("the total number of attribute values must not exceed %d", resourceapi.ResourceSliceMaxAttributeValuesPerDevice)),
				field.Invalid(field.NewPath("spec", "devices").Index(12), resourceapi.ResourceSliceMaxAttributeValuesPerDevice+1, fmt.Sprintf("the total number of attribute values must not exceed %d", resourceapi.ResourceSliceMaxAttributeValuesPerDevice)),
				field.Invalid(field.NewPath("spec", "devices").Index(13), resourceapi.ResourceSliceMaxAttributeValuesPerDevice+1, fmt.Sprintf("the total number of attribute values must not exceed %d", resourceapi.ResourceSliceMaxAttributeValuesPerDevice)),
			},
			slice: func() *resourceapi.ResourceSlice {
				maxStringAttributeValuesInList := func(maxAttributeValues int) map[resourceapi.QualifiedName]resourceapi.DeviceAttribute {
					attributes := map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{}
					// 用 (ResourceSliceMaxAttributesAndCapacitiesPerDevice - 1) 个键承载 (ResourceSliceMaxAttributeValuesPerDevice) 个值，
					// 留出一个键的余量以避免触发 ResourceSliceMaxAttributesAndCapacitiesPerDevice，
					// 同时恰好触到 ResourceSliceMaxAttributeValuesPerDevice 上限。
					for i := range resourceapi.ResourceSliceMaxAttributesAndCapacitiesPerDevice - 1 {
						stringList := []string{"x"}
						if i == 0 {
							// 第一个列表吸收全部剩余额度，使总值刚好达到上限。
							stringList = slices.Repeat(stringList, maxAttributeValues-(resourceapi.ResourceSliceMaxAttributesAndCapacitiesPerDevice-2))
						}
						attributes[resourceapi.QualifiedName(fmt.Sprintf("list_attr_%d", i))] = resourceapi.DeviceAttribute{
							StringValues: stringList,
						}
					}
					return attributes
				}

				slice := testResourceSlice(goodName, goodName, goodName, 14)

				// 合法：字符串列表恰好用满 ResourceSliceMaxAttributeValues 个值。
				slice.Spec.Devices[0].Attributes = maxStringAttributeValuesInList(resourceapi.ResourceSliceMaxAttributeValuesPerDevice)
				slice.Spec.Devices[0].Capacity = nil

				// 非法：额外 int 标量属性使总值超限。
				slice.Spec.Devices[1].Attributes = maxStringAttributeValuesInList(resourceapi.ResourceSliceMaxAttributeValuesPerDevice)
				slice.Spec.Devices[1].Attributes[resourceapi.QualifiedName("extra_attr")] = resourceapi.DeviceAttribute{IntValue: new(int64(1))}
				slice.Spec.Devices[1].Capacity = nil

				// 非法：额外 bool 标量属性使总值超限。
				slice.Spec.Devices[2].Attributes = maxStringAttributeValuesInList(resourceapi.ResourceSliceMaxAttributeValuesPerDevice)
				slice.Spec.Devices[2].Attributes[resourceapi.QualifiedName("extra_attr")] = resourceapi.DeviceAttribute{BoolValue: new(true)}
				slice.Spec.Devices[2].Capacity = nil

				// 非法：额外 string 标量属性使总值超限。
				slice.Spec.Devices[3].Attributes = maxStringAttributeValuesInList(resourceapi.ResourceSliceMaxAttributeValuesPerDevice)
				slice.Spec.Devices[3].Attributes[resourceapi.QualifiedName("extra_attr")] = resourceapi.DeviceAttribute{StringValue: ptr.To("x")}
				slice.Spec.Devices[3].Capacity = nil

				// 非法：额外 version 标量属性使总值超限。
				slice.Spec.Devices[4].Attributes = maxStringAttributeValuesInList(resourceapi.ResourceSliceMaxAttributeValuesPerDevice)
				slice.Spec.Devices[4].Attributes[resourceapi.QualifiedName("extra_attr")] = resourceapi.DeviceAttribute{VersionValue: ptr.To("1.0.0")}
				slice.Spec.Devices[4].Capacity = nil

				// 非法：额外 int 列表属性使总值超限。
				slice.Spec.Devices[5].Attributes = maxStringAttributeValuesInList(resourceapi.ResourceSliceMaxAttributeValuesPerDevice)
				slice.Spec.Devices[5].Attributes[resourceapi.QualifiedName("extra_attr")] = resourceapi.DeviceAttribute{IntValues: []int64{int64(1)}}
				slice.Spec.Devices[5].Capacity = nil

				// 非法：额外 bool 列表属性使总值超限。
				slice.Spec.Devices[6].Attributes = maxStringAttributeValuesInList(resourceapi.ResourceSliceMaxAttributeValuesPerDevice)
				slice.Spec.Devices[6].Attributes[resourceapi.QualifiedName("extra_attr")] = resourceapi.DeviceAttribute{BoolValues: []bool{true}}
				slice.Spec.Devices[6].Capacity = nil

				// 非法：额外 string 列表属性使总值超限。
				slice.Spec.Devices[7].Attributes = maxStringAttributeValuesInList(resourceapi.ResourceSliceMaxAttributeValuesPerDevice)
				slice.Spec.Devices[7].Attributes[resourceapi.QualifiedName("extra_attr")] = resourceapi.DeviceAttribute{StringValues: []string{"extra"}}
				slice.Spec.Devices[7].Capacity = nil

				// 非法：额外 version 列表属性使总值超限。
				slice.Spec.Devices[8].Attributes = maxStringAttributeValuesInList(resourceapi.ResourceSliceMaxAttributeValuesPerDevice)
				slice.Spec.Devices[8].Attributes[resourceapi.QualifiedName("extra_attr")] = resourceapi.DeviceAttribute{VersionValues: []string{"1.0.0"}}
				slice.Spec.Devices[8].Capacity = nil

				// 非法：单属性内混合多种列表类型且总值超限。
				slice.Spec.Devices[9].Attributes = map[resourceapi.QualifiedName]resourceapi.DeviceAttribute{
					resourceapi.QualifiedName("mixed_list_attrs"): badListAttributeTooManyValueWithMixedTypes,
				}
				slice.Spec.Devices[9].Capacity = nil

				// 非法：额外两个 bool 列表值使总值超限。
				// 用于确认 BoolValues 按列表长度计数，而非退化为标量计数。
				slice.Spec.Devices[10].Attributes = maxStringAttributeValuesInList(resourceapi.ResourceSliceMaxAttributeValuesPerDevice - 1)
				slice.Spec.Devices[10].Attributes[resourceapi.QualifiedName("extra_attr")] = resourceapi.DeviceAttribute{BoolValues: []bool{true, false}}
				slice.Spec.Devices[10].Capacity = nil

				// 非法：额外两个 int 列表值使总值超限。
				slice.Spec.Devices[11].Attributes = maxStringAttributeValuesInList(resourceapi.ResourceSliceMaxAttributeValuesPerDevice - 1)
				slice.Spec.Devices[11].Attributes[resourceapi.QualifiedName("extra_attr")] = resourceapi.DeviceAttribute{IntValues: []int64{1, 2}}
				slice.Spec.Devices[11].Capacity = nil

				// 非法：额外两个 string 列表值使总值超限。
				slice.Spec.Devices[12].Attributes = maxStringAttributeValuesInList(resourceapi.ResourceSliceMaxAttributeValuesPerDevice - 1)
				slice.Spec.Devices[12].Attributes[resourceapi.QualifiedName("extra_attr")] = resourceapi.DeviceAttribute{StringValues: []string{"a", "b"}}
				slice.Spec.Devices[12].Capacity = nil

				// 非法：额外两个 version 列表值使总值超限。
				slice.Spec.Devices[13].Attributes = maxStringAttributeValuesInList(resourceapi.ResourceSliceMaxAttributeValuesPerDevice - 1)
				slice.Spec.Devices[13].Attributes[resourceapi.QualifiedName("extra_attr")] = resourceapi.DeviceAttribute{VersionValues: []string{"1.0.0", "1.0.1"}}
				slice.Spec.Devices[13].Capacity = nil
				return slice
			}(),
		},
		// 单次分配容量不允许 requestPolicy，验证 allowMultipleAllocations 前置条件。
		"forbidden-request-policy-on-single-allocatable-capacity": {
			wantFailures: field.ErrorList{
				field.Forbidden(field.NewPath("spec", "devices").Index(1).Child("capacity").Key("cap").Child("requestPolicy"), "allowMultipleAllocations must be true"),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, goodName, 2)
				capacity := resourceapi.DeviceCapacity{
					Value: resource.MustParse("1Gi"),
					RequestPolicy: &resourceapi.CapacityRequestPolicy{
						Default: ptr.To(resource.MustParse("1Mi")),
						ValidRange: &resourceapi.CapacityRequestPolicyRange{
							Min: ptr.To(resource.MustParse("1Mi")),
						},
					},
				}
				slice.Spec.Devices[1].Capacity = map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
					"cap": capacity,
				}
				return slice
			}(),
			consumableCapacityFeatureGate: true,
		},
		// validRange.step 为 0，验证步长必须为正。
		"invalid-request-policy-zero-step": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices").Index(1).Child("capacity").Key("cap").Child("requestPolicy").Child("validRange", "step"), "0", "must be greater than zero"),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, goodName, 2)
				slice.Spec.Devices[1].AllowMultipleAllocations = ptr.To(true)
				capacity := resourceapi.DeviceCapacity{
					Value: resource.MustParse("1Gi"),
					RequestPolicy: &resourceapi.CapacityRequestPolicy{
						Default: ptr.To(resource.MustParse("10Mi")),
						ValidRange: &resourceapi.CapacityRequestPolicyRange{
							Min:  ptr.To(resource.MustParse("1Mi")),
							Max:  ptr.To(resource.MustParse("100Mi")),
							Step: ptr.To(resource.MustParse("0")),
						},
					},
				}
				slice.Spec.Devices[1].Capacity = map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
					"cap": capacity,
				}
				return slice
			}(),
			consumableCapacityFeatureGate: true,
		},
		// validRange.step 为负，验证步长正数约束。
		"invalid-request-policy-negative-step": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices").Index(1).Child("capacity").Key("cap").Child("requestPolicy").Child("validRange", "step"), "-1Mi", "must be greater than zero"),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, goodName, 2)
				slice.Spec.Devices[1].AllowMultipleAllocations = ptr.To(true)
				capacity := resourceapi.DeviceCapacity{
					Value: resource.MustParse("1Gi"),
					RequestPolicy: &resourceapi.CapacityRequestPolicy{
						Default: ptr.To(resource.MustParse("10Mi")),
						ValidRange: &resourceapi.CapacityRequestPolicyRange{
							Min:  ptr.To(resource.MustParse("1Mi")),
							Max:  ptr.To(resource.MustParse("100Mi")),
							Step: ptr.To(resource.MustParse("-1Mi")),
						},
					},
				}
				slice.Spec.Devices[1].Capacity = map[resourceapi.QualifiedName]resourceapi.DeviceCapacity{
					"cap": capacity,
				}
				return slice
			}(),
			consumableCapacityFeatureGate: true,
		},
		// 整数模式下可消费容量的边界集合，验证取整、范围重叠与极值处理。
		"consumable-capacity-integer": {
			consumableCapacityFeatureGate:      true,
			fractionalCapacityRangeFeatureGate: false,
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithConsumableCapacity(goodName, goodName, driverName, 12)
				updateConsumableCapacity(slice, 1, func(cap *resourceapi.DeviceCapacity) { cap.Value = resource.MustParse("-1") })
				updateConsumableCapacity(slice, 2, func(cap *resourceapi.DeviceCapacity) { cap.RequestPolicy.Default = new(resource.MustParse("-1")) })
				updateConsumableCapacity(slice, 3, func(cap *resourceapi.DeviceCapacity) { cap.RequestPolicy.ValidValues = nil }) // 合法（未施加消费约束）。
				updateConsumableCapacity(slice, 4, func(cap *resourceapi.DeviceCapacity) {
					// 非法：同时设置了 ValidValues。
					cap.RequestPolicy.ValidRange = &resourceapi.CapacityRequestPolicyRange{Min: new(resource.MustParse("10"))}
				})
				updateConsumableCapacity(slice, 5, func(cap *resourceapi.DeviceCapacity) {
					// 合法：仅设置 ValidRange。
					cap.RequestPolicy.ValidRange = &resourceapi.CapacityRequestPolicyRange{Min: new(resource.MustParse("1"))}
					cap.RequestPolicy.ValidValues = nil
				})
				updateConsumableCapacity(slice, 6, func(cap *resourceapi.DeviceCapacity) {
					// 整数模式下向下取整，仍然合法。
					fraction := resource.MustParse("0.1")
					cap.RequestPolicy.Default.Add(fraction)
					cap.RequestPolicy.ValidValues[0].Add(fraction)
					cap.RequestPolicy.ValidValues[1].Add(fraction)
				})
				updateConsumableCapacity(slice, 7, func(cap *resourceapi.DeviceCapacity) {
					// 非法：default 小于 min。
					cap.RequestPolicy.Default = new(resource.MustParse("1.05"))
					cap.RequestPolicy.ValidRange = &resourceapi.CapacityRequestPolicyRange{Min: new(resource.MustParse("1.1"))}
					cap.RequestPolicy.ValidValues = nil
				})
				updateConsumableCapacity(slice, 8, func(cap *resourceapi.DeviceCapacity) {
					fraction := resource.MustParse("0.1")
					min := cap.RequestPolicy.ValidValues[1]
					minFrac := min.DeepCopy()
					minFrac.Sub(fraction)
					cap.RequestPolicy.ValidValues[0] = minFrac // 更小，但整数取整后等于 Value[1]，构成非法重复。

				})
				updateConsumableCapacity(slice, 9, func(cap *resourceapi.DeviceCapacity) {
					// 合法：大整数。
					cap.Value.Set(math.MaxInt64)
					cap.RequestPolicy.Default.Set(math.MaxInt64)
					cap.RequestPolicy.ValidValues = nil
					cap.RequestPolicy.ValidRange = &resourceapi.CapacityRequestPolicyRange{
						Min:  resource.NewQuantity(0, resource.DecimalSI),
						Step: resource.NewQuantity(1, resource.DecimalSI),
						Max:  &cap.Value,
					}
				})
				updateConsumableCapacity(slice, 10, func(cap *resourceapi.DeviceCapacity) {
					// 合法：毫级小数。
					maxMilli := *resource.NewMilliQuantity(math.MaxInt64, resource.DecimalSI)
					cap.Value = maxMilli
					cap.RequestPolicy.Default = &maxMilli
					cap.RequestPolicy.ValidValues = nil
					cap.RequestPolicy.ValidRange = &resourceapi.CapacityRequestPolicyRange{
						Min:  resource.NewMilliQuantity(0, resource.DecimalSI),
						Step: resource.NewMilliQuantity(1, resource.DecimalSI),
						Max:  &cap.Value,
					}
				})
				updateConsumableCapacity(slice, 11, func(cap *resourceapi.DeviceCapacity) {
					// 非法小数：范围过大，但整数模式取整后合法。
					cap.Value.Set(math.MaxInt64)
					cap.RequestPolicy.Default.Set(math.MaxInt64)
					cap.RequestPolicy.ValidValues = nil
					cap.RequestPolicy.ValidRange = &resourceapi.CapacityRequestPolicyRange{
						Min:  resource.NewMilliQuantity(0, resource.DecimalSI),
						Step: resource.NewMilliQuantity(1, resource.DecimalSI),
						Max:  &cap.Value,
					}
				})
				return slice
			}(),
			wantFailures: field.ErrorList{
				field.Invalid(consumeableCapacityPath(1).Child("requestPolicy", "validValues").Index(0), "5", "option is larger than capacity value: -1"),
				field.Invalid(consumeableCapacityPath(1).Child("requestPolicy", "validValues").Index(1), "10", "option is larger than capacity value: -1"),
				field.Invalid(consumeableCapacityPath(1).Child("requestPolicy", "validValues").Index(2), "100", "option is larger than capacity value: -1"),
				field.Invalid(consumeableCapacityPath(2).Child("requestPolicy", "validValues"), "-1", "default value is not valid according to the requestPolicy"),
				field.Forbidden(consumeableCapacityPath(4).Child("requestPolicy"), `exactly one policy can be specified, cannot specify "validValues" and "validRange" at the same time`),
				field.Invalid(consumeableCapacityPath(7).Child("requestPolicy", "validRange", "default"), "1050m", "default is less than min: 1100m"),
				field.Duplicate(consumeableCapacityPath(8).Child("requestPolicy", "validValues").Index(1), "10"),
			},
		},
		// 毫精度模式下同样的边界集合，验证精度差异导致的判定变化。
		"consumable-capacity-fractional": {
			consumableCapacityFeatureGate:      true,
			fractionalCapacityRangeFeatureGate: true,
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithConsumableCapacity(goodName, goodName, driverName, 12)
				updateConsumableCapacity(slice, 1, func(cap *resourceapi.DeviceCapacity) { cap.Value = resource.MustParse("-1") })
				updateConsumableCapacity(slice, 2, func(cap *resourceapi.DeviceCapacity) { cap.RequestPolicy.Default = new(resource.MustParse("-1")) })
				updateConsumableCapacity(slice, 3, func(cap *resourceapi.DeviceCapacity) { cap.RequestPolicy.ValidValues = nil }) // 合法（未施加消费约束）。
				updateConsumableCapacity(slice, 4, func(cap *resourceapi.DeviceCapacity) {
					// 非法：同时设置了 ValidValues。
					cap.RequestPolicy.ValidRange = &resourceapi.CapacityRequestPolicyRange{Min: new(resource.MustParse("10"))}
				})
				updateConsumableCapacity(slice, 5, func(cap *resourceapi.DeviceCapacity) {
					// 合法：仅设置 ValidRange。
					cap.RequestPolicy.ValidRange = &resourceapi.CapacityRequestPolicyRange{Min: new(resource.MustParse("1"))}
					cap.RequestPolicy.ValidValues = nil
				})
				updateConsumableCapacity(slice, 6, func(cap *resourceapi.DeviceCapacity) {
					// 整数模式下向下取整，仍然合法。
					fraction := resource.MustParse("0.1")
					cap.RequestPolicy.Default.Add(fraction)
					cap.RequestPolicy.ValidValues[0].Add(fraction)
					cap.RequestPolicy.ValidValues[1].Add(fraction)
				})
				updateConsumableCapacity(slice, 7, func(cap *resourceapi.DeviceCapacity) {
					// 非法：default 小于 min。
					cap.RequestPolicy.Default = new(resource.MustParse("1.05"))
					cap.RequestPolicy.ValidRange = &resourceapi.CapacityRequestPolicyRange{Min: new(resource.MustParse("1.1"))}
					cap.RequestPolicy.ValidValues = nil
				})
				updateConsumableCapacity(slice, 8, func(cap *resourceapi.DeviceCapacity) {
					fraction := resource.MustParse("0.1")
					min := cap.RequestPolicy.ValidValues[1]
					minFrac := min.DeepCopy()
					minFrac.Sub(fraction)
					cap.RequestPolicy.ValidValues[0] = minFrac // 毫精度下不构成重复。

				})
				updateConsumableCapacity(slice, 9, func(cap *resourceapi.DeviceCapacity) {
					// 合法：大整数。
					cap.Value.Set(math.MaxInt64)
					cap.RequestPolicy.Default.Set(math.MaxInt64)
					cap.RequestPolicy.ValidValues = nil
					cap.RequestPolicy.ValidRange = &resourceapi.CapacityRequestPolicyRange{
						Min:  resource.NewQuantity(0, resource.DecimalSI),
						Step: resource.NewQuantity(1, resource.DecimalSI),
						Max:  resource.NewQuantity(math.MaxInt, resource.DecimalSI),
					}
				})
				updateConsumableCapacity(slice, 10, func(cap *resourceapi.DeviceCapacity) {
					// 合法：毫级小数。
					maxMilli := *resource.NewMilliQuantity(math.MaxInt64, resource.DecimalSI)
					cap.Value = maxMilli
					cap.RequestPolicy.Default = &maxMilli
					cap.RequestPolicy.ValidValues = nil
					cap.RequestPolicy.ValidRange = &resourceapi.CapacityRequestPolicyRange{
						Min:  resource.NewMilliQuantity(0, resource.DecimalSI),
						Step: resource.NewMilliQuantity(1, resource.DecimalSI),
						Max:  &maxMilli,
					}
				})
				updateConsumableCapacity(slice, 11, func(cap *resourceapi.DeviceCapacity) {
					// 非法小数：范围过大。
					cap.Value.Set(math.MaxInt64)
					cap.RequestPolicy.Default.Set(math.MaxInt64)
					cap.RequestPolicy.ValidValues = nil
					cap.RequestPolicy.ValidRange = &resourceapi.CapacityRequestPolicyRange{
						Min:  resource.NewMilliQuantity(0, resource.DecimalSI),
						Step: resource.NewMilliQuantity(1, resource.DecimalSI),
						Max:  &cap.Value,
					}
				})
				return slice
			}(),
			wantFailures: field.ErrorList{
				field.Invalid(consumeableCapacityPath(1).Child("requestPolicy", "validValues").Index(0), "5", "option is larger than capacity value: -1"),
				field.Invalid(consumeableCapacityPath(1).Child("requestPolicy", "validValues").Index(1), "10", "option is larger than capacity value: -1"),
				field.Invalid(consumeableCapacityPath(1).Child("requestPolicy", "validValues").Index(2), "100", "option is larger than capacity value: -1"),
				field.Invalid(consumeableCapacityPath(2).Child("requestPolicy", "validValues"), "-1", "default value is not valid according to the requestPolicy"),
				field.Forbidden(consumeableCapacityPath(4).Child("requestPolicy"), `exactly one policy can be specified, cannot specify "validValues" and "validRange" at the same time`),
				field.Invalid(consumeableCapacityPath(7).Child("requestPolicy", "validRange", "default"), "1050m", "default is less than min: 1100m"),
				field.Invalid(consumeableCapacityPath(11).Child("requestPolicy", "validRange", "default"), fmt.Sprintf("%d", math.MaxInt), "value cannot be represented as a milli value"),
				field.Invalid(consumeableCapacityPath(11).Child("requestPolicy", "validRange", "max"), fmt.Sprintf("%d", math.MaxInt), "value cannot be represented as a milli value"),
			},
		},
		// nodeSelector 的 label value 非法，验证节点选择器值被校验。
		"invalid-node-selecor-label-value": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("spec", "nodeSelector", "nodeSelectorTerms").Index(0).Child("matchExpressions").Index(0).Child("values").Index(0), "-1", "a valid label must be an empty string or consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyValue',  or 'my_value',  or '12345', regex used for validation is '(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?')")},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, goodName, 3)
				slice.Spec.NodeName = nil
				slice.Spec.NodeSelector = &core.NodeSelector{
					NodeSelectorTerms: []core.NodeSelectorTerm{{
						MatchExpressions: []core.NodeSelectorRequirement{{
							Key:      "foo",
							Operator: core.NodeSelectorOpIn,
							Values:   []string{"-1"},
						}},
					}},
				}
				return slice
			}(),
		},
		// taint 的合法与非法形态：空字段、非法 key/value/effect，验证完整约束。
		"taints": {
			wantFailures: func() field.ErrorList {
				fldPath := field.NewPath("spec", "devices").Index(0).Child("taints")
				return field.ErrorList{
					field.Invalid(fldPath.Index(2).Child("key"), "", "name part must be non-empty"),
					field.Invalid(fldPath.Index(2).Child("key"), "", "name part must consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyName',  or 'my.name',  or '123-abc', regex used for validation is '([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]')"),
					field.Required(fldPath.Index(2).Child("effect"), "").MarkCoveredByDeclarative(),

					field.Invalid(fldPath.Index(3).Child("key"), badName, "name part must consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyName',  or 'my.name',  or '123-abc', regex used for validation is '([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9]')"),
					field.Invalid(fldPath.Index(3).Child("value"), badName, "a valid label must be an empty string or consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyValue',  or 'my_value',  or '12345', regex used for validation is '(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?')"),
					field.NotSupported(fldPath.Index(3).Child("effect"), resourceapi.DeviceTaintEffect("some-other-effect"), []resourceapi.DeviceTaintEffect{resourceapi.DeviceTaintEffectNoExecute, resourceapi.DeviceTaintEffectNoSchedule, resourceapi.DeviceTaintEffectNone}).MarkCoveredByDeclarative(),
				}
			}(),
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, goodName, 3)
				slice.Spec.Devices[0].Taints = []resourceapi.DeviceTaint{
					{
						// 最小合法 taint。
						Key:    "example.com/taint",
						Effect: resourceapi.DeviceTaintEffectNoExecute,
					},
					{
						// 完整合法 taint，使用另一组 key 与 effect。
						Key:       "taint",
						Value:     "tainted",
						Effect:    resourceapi.DeviceTaintEffectNoSchedule,
						TimeAdded: ptr.To(metav1.Now()),
					},
					{
						// 非法：全部字段为空。
					},
					{
						// 非法字符串。
						Key:    badName,
						Value:  badName,
						Effect: "some-other-effect",
					},
				}
				return slice
			}(),
		},
		// taint 数量超限，验证 DeviceTaintsMaxLength。
		"too-many-taints": {
			wantFailures: field.ErrorList{
				field.TooMany(field.NewPath("spec", "devices").Index(0).Child("taints"), resourceapi.DeviceTaintsMaxLength+1, resourceapi.DeviceTaintsMaxLength),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				for i := 0; i < resourceapi.DeviceTaintsMaxLength+1; i++ {
					slice.Spec.Devices[0].Taints = append(slice.Spec.Devices[0].Taints, resourceapi.DeviceTaint{
						Key:    "example.com/taint",
						Effect: resourceapi.DeviceTaintEffectNoExecute,
					})
				}
				return slice
			}(),
		},
		// perDeviceNodeSelection 与 nodeName 同时设置，验证二者互斥。
		"bad-PerDeviceNodeSelection": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec"), "{`nodeName`, `perDeviceNodeSelection`}", "exactly one of `nodeName`, `nodeSelector`, `allNodes`, `perDeviceNodeSelection` is required, but multiple fields are set"),
				field.Required(field.NewPath("spec", "devices").Index(0), "exactly one of `nodeName`, `nodeSelector`, or `allNodes` is required when `perDeviceNodeSelection` is set to true in the ResourceSlice spec"),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Spec.NodeName = ptr.To("worker")
				slice.Spec.PerDeviceNodeSelection = func() *bool {
					r := true
					return &r
				}()
				return slice
			}(),
		},
		// perDeviceNodeSelection 显式为 false 非法，验证只能未设置或为 true。
		"invalid-false-PerDeviceNodeSelection": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "perDeviceNodeSelection"), false, "must be either unset or set to true"),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Spec.NodeName = ptr.To("worker")
				slice.Spec.PerDeviceNodeSelection = func() *bool {
					r := false
					return &r
				}()
				return slice
			}(),
		},
		// allNodes 显式为 false 非法，验证只能未设置或为 true。
		"invalid-false-AllNodes": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "allNodes"), false, "must be either unset or set to true"),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Spec.NodeName = ptr.To("worker")
				slice.Spec.AllNodes = func() *bool {
					r := false
					return &r
				}()
				return slice
			}(),
		},
		// nodeName 为空字符串，验证只能未设置或非空。
		"invalid-empty-NodeName": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "nodeName"), "", "must be either unset or set to a non-empty string"),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Spec.NodeName = ptr.To("")
				slice.Spec.AllNodes = func() *bool {
					r := true
					return &r
				}()
				return slice
			}(),
		},
		// 设备级 nodeName 为空且 allNodes 为 false，验证设备级字段合法性。
		"invalid-node-selector-in-basicdevice": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices").Index(0).Child("nodeName"), "", "must not be empty"),
				field.Invalid(field.NewPath("spec", "devices").Index(0).Child("allNodes"), false, "must be either unset or set to true"),
				field.Required(field.NewPath("spec", "devices").Index(0), "exactly one of `nodeName`, `nodeSelector`, or `allNodes` is required when `perDeviceNodeSelection` is set to true in the ResourceSlice spec"),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Spec.PerDeviceNodeSelection = func() *bool {
					r := true
					return &r
				}()
				slice.Spec.NodeName = nil
				slice.Spec.Devices[0].NodeName = func() *string {
					r := ""
					return &r
				}()
				slice.Spec.Devices[0].AllNodes = func() *bool {
					r := false
					return &r
				}()
				return slice
			}(),
		},
		// 设备级 nodeName 与 allNodes 同时设置，验证设备级互斥。
		"bad-node-selector-in-basicdevice": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices").Index(0), "{`nodeName`, `allNodes`}", "exactly one of `nodeName`, `nodeSelector`, or `allNodes` is required when `perDeviceNodeSelection` is set to true in the ResourceSlice spec"),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Spec.PerDeviceNodeSelection = func() *bool {
					r := true
					return &r
				}()
				slice.Spec.NodeName = nil
				slice.Spec.Devices[0].NodeName = func() *string {
					r := "worker"
					return &r
				}()
				slice.Spec.Devices[0].AllNodes = func() *bool {
					r := true
					return &r
				}()
				return slice
			}(),
		},
		// counterSet.name 缺失，验证共享计数集的必填字段。
		"missing-name-shared-counters": {
			wantFailures: field.ErrorList{
				field.Required(field.NewPath("spec", "sharedCounters").Index(0).Child("name"), "").MarkCoveredByDeclarative(),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithSharedCounters(goodName, goodName, driverName, 1)
				slice.Spec.SharedCounters = []resourceapi.CounterSet{
					{
						Counters: testCounters(),
					},
				}
				return slice
			}(),
		},
		// counterSet.name 非法，验证 label 格式校验。
		"bad-name-shared-counters": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "sharedCounters").Index(0).Child("name"), badName, "a lowercase RFC 1123 label must consist of lower case alphanumeric characters or '-', and must start and end with an alphanumeric character (e.g. 'my-name',  or '123-abc', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?')").MarkCoveredByDeclarative(),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithSharedCounters(goodName, goodName, driverName, 0)
				slice.Spec.SharedCounters = []resourceapi.CounterSet{
					{
						Name:     badName,
						Counters: testCounters(),
					},
				}
				return slice
			}(),
		},
		// counterSet.counters 缺失，验证必填。
		"missing-counter-shared-counters": {
			wantFailures: field.ErrorList{
				field.Required(field.NewPath("spec", "sharedCounters").Index(0).Child("counters"), "").MarkCoveredByDeclarative(),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithSharedCounters(goodName, goodName, driverName, 0)
				slice.Spec.SharedCounters = []resourceapi.CounterSet{
					{
						Name: goodName,
					},
				}
				return slice
			}(),
		},
		// counter 名非法，验证 counter 键的格式。
		"bad-countername-shared-counters": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "sharedCounters").Index(0).Child("counters"), badName, "a lowercase RFC 1123 label must consist of lower case alphanumeric characters or '-', and must start and end with an alphanumeric character (e.g. 'my-name',  or '123-abc', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?')").MarkCoveredByDeclarative(),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithSharedCounters(goodName, goodName, driverName, 1)
				slice.Spec.SharedCounters = []resourceapi.CounterSet{
					{
						Name: goodName,
						Counters: map[string]resourceapi.Counter{
							badName: {Value: resource.MustParse("1Gi")},
						},
					},
				}
				return slice
			}(),
		},
		// counterSet 名重复，验证唯一性。
		"duplicate-shared-counters": {
			wantFailures: field.ErrorList{
				field.Duplicate(field.NewPath("spec", "sharedCounters").Index(1), goodName).MarkCoveredByDeclarative(),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithSharedCounters(goodName, goodName, driverName, 1)
				slice.Spec.SharedCounters = []resourceapi.CounterSet{
					{
						Name:     goodName,
						Counters: testCounters(),
					},
					{
						Name:     goodName,
						Counters: testCounters(),
					},
				}
				return slice
			}(),
		},
		// counterSet 数量达上限，验证边界。
		"max-shared-counters": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithSharedCounters(goodName, goodName, driverName, 1)
				slice.Spec.SharedCounters = createSharedCounters(resourceapi.ResourceSliceMaxCounterSets)
				return slice
			}(),
		},
		// counterSet 数量超限，验证 ResourceSliceMaxCounterSets。
		"too-many-shared-counters": {
			wantFailures: field.ErrorList{
				field.TooMany(field.NewPath("spec", "sharedCounters"), resourceapi.ResourceSliceMaxCounterSets+1, resourceapi.ResourceSliceMaxCounterSets).MarkCoveredByDeclarative(),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithSharedCounters(goodName, goodName, driverName, 1)
				slice.Spec.SharedCounters = createSharedCounters(resourceapi.ResourceSliceMaxCounterSets + 1)
				return slice
			}(),
		},
		// 单个 counterSet 内 counter 数达上限，验证边界。
		"max-counters-in-counter-set": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithSharedCounters(goodName, goodName, driverName, 1)
				slice.Spec.SharedCounters[0].Counters = testMultipleCounters(resourceapi.ResourceSliceMaxCountersPerCounterSet)
				return slice
			}(),
		},
		// 单个 counterSet 内 counter 数超限，验证上限。
		"too-many-counters-in-counter-set": {
			wantFailures: field.ErrorList{
				field.TooMany(field.NewPath("spec", "sharedCounters").Index(0).Child("counters"), resourceapi.ResourceSliceMaxCountersPerCounterSet+1, resourceapi.ResourceSliceMaxCountersPerCounterSet),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithSharedCounters(goodName, goodName, driverName, 1)
				slice.Spec.SharedCounters[0].Counters = testMultipleCounters(resourceapi.ResourceSliceMaxCountersPerCounterSet + 1)
				return slice
			}(),
		},
		// consumesCounters 中 counterSet 名缺失，验证必填。
		"missing-name-counterset-consumes-counter": {
			wantFailures: field.ErrorList{
				field.Required(field.NewPath("spec", "devices").Index(0).Child("consumesCounters").Index(0).Child("counterSet"), "").MarkCoveredByDeclarative(),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].ConsumesCounters = []resourceapi.DeviceCounterConsumption{
					{
						Counters: testCounters(),
					},
				}
				return slice
			}(),
		},
		// consumesCounters 中 counterSet 名非法，验证格式。
		"bad-name-counterset-consumes-counter": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices").Index(0).Child("consumesCounters").Index(0).Child("counterSet"), badName, "a lowercase RFC 1123 label must consist of lower case alphanumeric characters or '-', and must start and end with an alphanumeric character (e.g. 'my-name',  or '123-abc', regex used for validation is '[a-z0-9]([-a-z0-9]*[a-z0-9])?')").MarkCoveredByDeclarative(),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].ConsumesCounters = []resourceapi.DeviceCounterConsumption{
					{
						CounterSet: badName,
						Counters:   testCounters(),
					},
				}
				return slice
			}(),
		},
		// consumesCounters 中 counterSet 名重复，验证唯一性。
		"duplicate-counterset-consumes-counter": {
			wantFailures: field.ErrorList{
				field.Duplicate(field.NewPath("spec", "devices").Index(0).Child("consumesCounters").Index(1), goodName).MarkCoveredByDeclarative()},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].ConsumesCounters = []resourceapi.DeviceCounterConsumption{
					{
						CounterSet: goodName,
						Counters:   testCounters(),
					},
					{
						CounterSet: goodName,
						Counters:   testCounters(),
					},
				}
				return slice
			}(),
		},
		// consumesCounters 中 counters 缺失，验证必填。
		"missing-counter-consumes-counter": {
			wantFailures: field.ErrorList{
				field.Required(field.NewPath("spec", "devices").Index(0).Child("consumesCounters").Index(0).Child("counters"), "").MarkCoveredByDeclarative(),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].ConsumesCounters = []resourceapi.DeviceCounterConsumption{
					{
						CounterSet: "counterset-0",
					},
				}
				return slice
			}(),
		},
		// 单设备 consumesCounters 数达上限，验证边界。
		"max-device-counter-consumption-for-device": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].Attributes = nil
				slice.Spec.Devices[0].Capacity = nil
				slice.Spec.Devices[0].ConsumesCounters = createConsumesCounters(resourceapi.ResourceSliceMaxDeviceCounterConsumptionsPerDevice)
				return slice
			}(),
		},
		// 单设备 consumesCounters 数超限，验证上限。
		"too-many-device-counter-consumption-for-device": {
			wantFailures: field.ErrorList{
				field.TooMany(field.NewPath("spec", "devices").Index(0).Child("consumesCounters"), resourceapi.ResourceSliceMaxDeviceCounterConsumptionsPerDevice+1, resourceapi.ResourceSliceMaxDeviceCounterConsumptionsPerDevice).MarkCoveredByDeclarative(),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].Attributes = nil
				slice.Spec.Devices[0].Capacity = nil
				slice.Spec.Devices[0].ConsumesCounters = createConsumesCounters(resourceapi.ResourceSliceMaxDeviceCounterConsumptionsPerDevice + 1)
				return slice
			}(),
		},
		// 单次消费中 counter 数达上限，验证边界。
		"max-counters-in-device-counter-consumption": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].Attributes = nil
				slice.Spec.Devices[0].Capacity = nil
				slice.Spec.Devices[0].ConsumesCounters = []resourceapi.DeviceCounterConsumption{
					{
						CounterSet: "counterset-0",
						Counters:   testMultipleCounters(resourceapi.ResourceSliceMaxCountersPerDeviceCounterConsumption),
					},
				}
				return slice
			}(),
		},
		// 单次消费中 counter 数超限，验证上限。
		"too-many-counters-in-device-counter-consumption": {
			wantFailures: field.ErrorList{
				field.TooMany(field.NewPath("spec", "devices").Index(0).Child("consumesCounters").Index(0).Child("counters"), resourceapi.ResourceSliceMaxCountersPerDeviceCounterConsumption+1, resourceapi.ResourceSliceMaxCountersPerDeviceCounterConsumption),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].Attributes = nil
				slice.Spec.Devices[0].Capacity = nil
				slice.Spec.Devices[0].ConsumesCounters = []resourceapi.DeviceCounterConsumption{
					{
						CounterSet: "counterset-0",
						Counters:   testMultipleCounters(resourceapi.ResourceSliceMaxCountersPerDeviceCounterConsumption + 1),
					},
				}
				return slice
			}(),
		},
		// 使用 consumesCounters 时设备数上限提高，验证高级特性边界。
		"max-number-of-devices-with-consumes-counters": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, goodName, resourceapi.ResourceSliceMaxDevicesWithAdvancedFeatures)
				for i := range slice.Spec.Devices {
					slice.Spec.Devices[i].ConsumesCounters = []resourceapi.DeviceCounterConsumption{
						{
							CounterSet: "counterset-0",
							Counters:   testCounters(),
						},
					}
				}
				return slice
			}(),
		},
		// 使用 consumesCounters 时设备数超限，验证高级特性上限。
		"too-many-devices-with-consumes-counters": {
			wantFailures: field.ErrorList{field.TooMany(field.NewPath("spec", "devices"), resourceapi.ResourceSliceMaxDevicesWithAdvancedFeatures+1, resourceapi.ResourceSliceMaxDevicesWithAdvancedFeatures)},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, goodName, resourceapi.ResourceSliceMaxDevicesWithAdvancedFeatures+1)
				slice.Spec.Devices[0].ConsumesCounters = []resourceapi.DeviceCounterConsumption{
					{
						CounterSet: "counterset-0",
						Counters:   testCounters(),
					},
				}
				return slice
			}(),
		},
		// 两类绑定条件均合法，验证基本配对。
		"good-binding-conditions": {
			slice: testResourceSliceWithBindingConditions(goodName, goodName, driverName, 1, []string{"example.com/condition1", "condition2"}, []string{"example.com/condition3", "condition4"}),
		},
		// bindingConditions 数量超限，验证 BindingConditionsMaxSize。
		"too-many-binding-conditions": {
			wantFailures: field.ErrorList{field.TooMany(field.NewPath("spec", "devices").Index(0).Child("bindingConditions"), resourceapi.BindingConditionsMaxSize+1, resourceapi.BindingConditionsMaxSize).MarkCoveredByDeclarative()},
			slice:        testResourceSliceWithBindingConditions(goodName, goodName, driverName, 1, []string{"condition1", "condition2", "condition3", "condition4", "condition5"}, []string{"condition6", "condition7"}),
		},
		// bindingFailureConditions 数量超限，验证同一上限。
		"too-many-binding-failure-conditions": {
			wantFailures: field.ErrorList{field.TooMany(field.NewPath("spec", "devices").Index(0).Child("bindingFailureConditions"), resourceapi.BindingConditionsMaxSize+1, resourceapi.BindingConditionsMaxSize).MarkCoveredByDeclarative()},
			slice:        testResourceSliceWithBindingConditions(goodName, goodName, driverName, 1, []string{"condition1", "condition2"}, []string{"condition3", "condition4", "condition5", "condition6", "condition7"}),
		},
		// condition 名含非法字符，验证格式校验。
		"invalid-binding-conditions": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("spec", "devices").Index(0).Child("bindingConditions").Index(1), "condition2!", conditionValidationErrMessage)},
			slice:        testResourceSliceWithBindingConditions(goodName, goodName, driverName, 1, []string{"condition1", "condition2!"}, []string{"condition3", "condition4"}),
		},
		// failure condition 名含非法字符，验证格式校验。
		"invalid-binding-failure-conditions": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("spec", "devices").Index(0).Child("bindingFailureConditions").Index(0), "condition3!", conditionValidationErrMessage)},
			slice:        testResourceSliceWithBindingConditions(goodName, goodName, driverName, 1, []string{"condition1", "condition2"}, []string{"condition3!", "condition4"}),
		},
		// 只有 failureConditions，验证二者必须成对出现。
		"invalid-slice-has-binding-failure-but-no-binding-conditions": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("spec", "devices").Index(0).Child("bindingConditions"), []string(nil), "bindingConditions are required to use bindingFailureConditions")},
			slice:        testResourceSliceWithBindingConditions(goodName, goodName, driverName, 1, nil, []string{"condition1", "condition2"}),
		},
		// 只有 bindingConditions，验证成对约束的反向情形。
		"invalid-slice-has-binding-conditions-but-no-binding-failure-conditions": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("spec", "devices").Index(0).Child("bindingFailureConditions"), []string(nil), "bindingFailureConditions are required to use bindingConditions")},
			slice:        testResourceSliceWithBindingConditions(goodName, goodName, driverName, 1, []string{"condition1", "condition2"}, nil),
		},
		// bindingConditions 内有重复项，验证唯一性。
		"duplicate-binding-conditions": {
			wantFailures: field.ErrorList{field.Duplicate(field.NewPath("spec", "devices").Index(0).Child("bindingConditions").Index(1), "condition1")},
			slice:        testResourceSliceWithBindingConditions(goodName, goodName, driverName, 1, []string{"condition1", "condition1"}, []string{"condition2", "condition3"}),
		},
		// failureConditions 内有重复项，验证唯一性。
		"duplicate-binding-failure-conditions": {
			wantFailures: field.ErrorList{field.Duplicate(field.NewPath("spec", "devices").Index(0).Child("bindingFailureConditions").Index(1), "condition3")},
			slice:        testResourceSliceWithBindingConditions(goodName, goodName, driverName, 1, []string{"condition1", "condition2"}, []string{"condition3", "condition3"}),
		},
		// 两类 condition 有交集，验证不允许重叠。
		"overlapping-binding-conditions": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("spec", "devices").Index(0).Child("bindingFailureConditions").Index(0), "condition1", "bindingFailureConditions must not overlap with bindingConditions")},
			slice:        testResourceSliceWithBindingConditions(goodName, goodName, driverName, 1, []string{"condition1", "condition2"}, []string{"condition1", "condition3"}),
		},
		// 正确的容量映射，验证 feature gate 开启时合法映射被接受。
		"correct-node-allocatable-resource-mappings": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithNodeAllocatableResources(goodName, goodName, driverName, 1)
				return slice
			}(),
			enableDRANodeAllocatableResourcesFeatureGate: true,
		},
		// 用两种等价写法构造映射，验证指针构造方式不影响校验结果。
		"node-allocatable-resources-both-quantityfrom": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithNodeAllocatableResources(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].NodeAllocatableResources = map[v1.ResourceName]resourceapi.NodeAllocatableResource{
					v1.ResourceCPU: {
						Mapping: &resourceapi.NodeAllocatableMapping{
							CapacityKey:        ptr.To[resourceapi.QualifiedName]("dra.example.com/cpu"),
							CapacityMultiplier: ptr.To(resource.MustParse("1")),
						},
					},
				}
				return slice
			}(),
			enableDRANodeAllocatableResourcesFeatureGate: true,
		},

		// capacityKey 为空，验证非空约束。
		"bad-node-allocatable-resources-capacityKey-empty": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices").Index(0).Child("nodeAllocatableResources").Key(string(v1.ResourceCPU)).Child("mapping").Child("capacityKey"), "", "capacityKey must not be an empty string"),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithNodeAllocatableResources(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].NodeAllocatableResources = map[v1.ResourceName]resourceapi.NodeAllocatableResource{
					v1.ResourceCPU: {
						Mapping: &resourceapi.NodeAllocatableMapping{
							CapacityKey:        ptr.To[resourceapi.QualifiedName](""),
							CapacityMultiplier: ptr.To(resource.MustParse("1")),
						},
					},
				}
				return slice
			}(),
			enableDRANodeAllocatableResourcesFeatureGate: true,
		},
		// capacityKey 指向不存在的容量，验证引用完整性。
		"bad-node-allocatable-resources-capacity-key-not-found": {
			wantFailures: field.ErrorList{
				field.NotFound(field.NewPath("spec", "devices").Index(0).Child("nodeAllocatableResources").Key(string(v1.ResourceCPU)).Child("mapping").Child("capacityKey"), resourceapi.QualifiedName("nonexistent")),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithNodeAllocatableResources(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].NodeAllocatableResources = map[v1.ResourceName]resourceapi.NodeAllocatableResource{
					v1.ResourceCPU: {
						Mapping: &resourceapi.NodeAllocatableMapping{
							CapacityKey:        ptr.To[resourceapi.QualifiedName]("nonexistent"),
							CapacityMultiplier: ptr.To(resource.MustParse("1")),
						},
					},
				}
				return slice
			}(),
			enableDRANodeAllocatableResourcesFeatureGate: true,
		},
		// deviceMultiplier 为负，验证必须为正。
		"bad-node-allocatable-resources-invalid-device-multiplier-negative": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices").Index(0).Child("nodeAllocatableResources").Key(string(v1.ResourceCPU)).Child("mapping").Child("deviceMultiplier"), "-1", "must be positive"),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].NodeAllocatableResources = map[v1.ResourceName]resourceapi.NodeAllocatableResource{
					v1.ResourceCPU: {
						Mapping: &resourceapi.NodeAllocatableMapping{
							DeviceMultiplier: ptr.To(resource.MustParse("-1")),
						},
					},
				}
				return slice
			}(),
			enableDRANodeAllocatableResourcesFeatureGate: true,
		},
		// capacityMultiplier 为负，验证必须为正。
		"bad-node-allocatable-resources-invalid-capacity-multiplier-negative": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices").Index(0).Child("nodeAllocatableResources").Key(string(v1.ResourceCPU)).Child("mapping").Child("capacityMultiplier"), "-1", "must be positive"),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithNodeAllocatableResources(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].NodeAllocatableResources = map[v1.ResourceName]resourceapi.NodeAllocatableResource{
					v1.ResourceCPU: {
						Mapping: &resourceapi.NodeAllocatableMapping{
							CapacityKey:        ptr.To[resourceapi.QualifiedName]("dra.example.com/cpu"),
							CapacityMultiplier: ptr.To(resource.MustParse("-1")),
						},
					},
				}
				return slice
			}(),
			enableDRANodeAllocatableResourcesFeatureGate: true,
		},
		// deviceMultiplier 为 0，验证必须为正。
		"bad-node-allocatable-resources-invalid-device-multiplier-zero": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices").Index(0).Child("nodeAllocatableResources").Key(string(v1.ResourceCPU)).Child("mapping").Child("deviceMultiplier"), "0", "must be positive"),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].NodeAllocatableResources = map[v1.ResourceName]resourceapi.NodeAllocatableResource{
					v1.ResourceCPU: {
						Mapping: &resourceapi.NodeAllocatableMapping{
							DeviceMultiplier: ptr.To(resource.MustParse("0")),
						},
					},
				}
				return slice
			}(),
			enableDRANodeAllocatableResourcesFeatureGate: true,
		},
		// capacityMultiplier 为 0，验证必须为正。
		"bad-node-allocatable-resources-invalid-capacity-multiplier-zero": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices").Index(0).Child("nodeAllocatableResources").Key(string(v1.ResourceCPU)).Child("mapping").Child("capacityMultiplier"), "0", "must be positive"),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithNodeAllocatableResources(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].NodeAllocatableResources = map[v1.ResourceName]resourceapi.NodeAllocatableResource{
					v1.ResourceCPU: {
						Mapping: &resourceapi.NodeAllocatableMapping{
							CapacityKey:        ptr.To[resourceapi.QualifiedName]("dra.example.com/cpu"),
							CapacityMultiplier: ptr.To(resource.MustParse("0")),
						},
					},
				}
				return slice
			}(),
			enableDRANodeAllocatableResourcesFeatureGate: true,
		},
		// 映射的 capacityKey 不在设备容量中，验证跨字段引用。
		"capacity key not in spec.devices[].capacity": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithNodeAllocatableResources(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].NodeAllocatableResources = map[v1.ResourceName]resourceapi.NodeAllocatableResource{
					v1.ResourceMemory: {
						Mapping: &resourceapi.NodeAllocatableMapping{
							CapacityKey:        ptr.To[resourceapi.QualifiedName]("dra.example.com/hugepages"),
							CapacityMultiplier: ptr.To(resource.MustParse("1")),
						},
					},
				}
				return slice
			}(),
			enableDRANodeAllocatableResourcesFeatureGate: true,
			wantFailures: field.ErrorList{
				field.NotFound(field.NewPath("spec", "devices").Index(0).Child("nodeAllocatableResources").Key(string(v1.ResourceMemory)).Child("mapping").Child("capacityKey"), resourceapi.QualifiedName("dra.example.com/hugepages")),
			},
		},
		// 被映射资源不是节点可分配资源，验证资源名白名单。
		"mapped resource is not a node allocatable resource": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithNodeAllocatableResources(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].NodeAllocatableResources = map[v1.ResourceName]resourceapi.NodeAllocatableResource{
					v1.ResourceName("dra.example.com/gpu"): {
						Mapping: &resourceapi.NodeAllocatableMapping{
							CapacityKey:        ptr.To[resourceapi.QualifiedName]("dra.example.com/cpu"),
							CapacityMultiplier: ptr.To(resource.MustParse("1")),
						},
					},
				}
				return slice
			}(),
			enableDRANodeAllocatableResourcesFeatureGate: true,
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices").Index(0).Child("nodeAllocatableResources").Key("dra.example.com/gpu"), v1.ResourceName("dra.example.com/gpu"), "must be a node allocatable resource name"),
			},
		},
		// 非标准资源名（无域名前缀），验证命名规则。
		"non standard resource name": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithNodeAllocatableResources(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].NodeAllocatableResources = map[v1.ResourceName]resourceapi.NodeAllocatableResource{
					v1.ResourceName("abc"): {
						Overhead: &resourceapi.NodeAllocatableOverhead{
							PerPod: ptr.To(resource.MustParse("1")),
						},
					},
				}
				return slice
			}(),
			enableDRANodeAllocatableResourcesFeatureGate: true,
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices").Index(0).Child("nodeAllocatableResources").Key("abc"), v1.ResourceName("abc"), "must be a node allocatable resource name"),
			},
		},
		// kubernetes.io 域保留给系统，验证不允许使用。
		"kubernetes.io namespace is rejected": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithNodeAllocatableResources(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].NodeAllocatableResources = map[v1.ResourceName]resourceapi.NodeAllocatableResource{
					v1.ResourceName("kubernetes.io/foo"): {
						Overhead: &resourceapi.NodeAllocatableOverhead{
							PerPod: ptr.To(resource.MustParse("1")),
						},
					},
				}
				return slice
			}(),
			enableDRANodeAllocatableResourcesFeatureGate: true,
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices").Index(0).Child("nodeAllocatableResources").Key("kubernetes.io/foo"), v1.ResourceName("kubernetes.io/foo"), "must be a node allocatable resource name"),
			},
		},
		// ephemeral-storage 不在可映射范围内，验证被拒绝。
		"ephemeral-storage is rejected": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithNodeAllocatableResources(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].NodeAllocatableResources = map[v1.ResourceName]resourceapi.NodeAllocatableResource{
					v1.ResourceName("ephemeral-storage"): {
						Overhead: &resourceapi.NodeAllocatableOverhead{
							PerPod: ptr.To(resource.MustParse("2Gi")),
						},
					},
				}
				return slice
			}(),
			enableDRANodeAllocatableResourcesFeatureGate: true,
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices").Index(0).Child("nodeAllocatableResources").Key("ephemeral-storage"), v1.ResourceName("ephemeral-storage"), "must be a node allocatable resource name"),
			},
		},
		// hugepages-* 是合法节点可分配资源，验证白名单放行。
		"valid hugepages resource": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithNodeAllocatableResources(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].NodeAllocatableResources = map[v1.ResourceName]resourceapi.NodeAllocatableResource{
					v1.ResourceName("hugepages-2Mi"): {
						Overhead: &resourceapi.NodeAllocatableOverhead{
							PerPod: ptr.To(resource.MustParse("2Mi")),
						},
					},
				}
				return slice
			}(),
			enableDRANodeAllocatableResourcesFeatureGate: true,
		},

		// overhead 两项均未设置，验证至少一个必填。
		"bad-node-allocatable-resources-overhead-neither-perpod-nor-percontainer-set": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices").Index(0).Child("nodeAllocatableResources").Key(string(v1.ResourceMemory)).Child("overhead"), "", "at least one of perPod or perContainer must be set"),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithNodeAllocatableResources(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].NodeAllocatableResources = map[v1.ResourceName]resourceapi.NodeAllocatableResource{
					v1.ResourceMemory: {
						Overhead: &resourceapi.NodeAllocatableOverhead{},
					},
				}
				return slice
			}(),
			enableDRANodeAllocatableResourcesFeatureGate: true,
		},
		// perPod 为负，验证非负约束。
		"bad-node-allocatable-resources-overhead-negative-perpod": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices").Index(0).Child("nodeAllocatableResources").Key(string(v1.ResourceMemory)).Child("overhead").Child("perPod"), "-1Gi", "must be non-negative"),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithNodeAllocatableResources(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].NodeAllocatableResources = map[v1.ResourceName]resourceapi.NodeAllocatableResource{
					v1.ResourceMemory: {
						Overhead: &resourceapi.NodeAllocatableOverhead{
							PerPod: ptr.To(resource.MustParse("-1Gi")),
						},
					},
				}
				return slice
			}(),
			enableDRANodeAllocatableResourcesFeatureGate: true,
		},
		// perContainer 为负，验证非负约束。
		"bad-node-allocatable-resources-overhead-negative-percontainer": {
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "devices").Index(0).Child("nodeAllocatableResources").Key(string(v1.ResourceMemory)).Child("overhead").Child("perContainer"), "-500Mi", "must be non-negative"),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithNodeAllocatableResources(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].NodeAllocatableResources = map[v1.ResourceName]resourceapi.NodeAllocatableResource{
					v1.ResourceMemory: {
						Overhead: &resourceapi.NodeAllocatableOverhead{
							PerContainer: ptr.To(resource.MustParse("-500Mi")),
						},
					},
				}
				return slice
			}(),
			enableDRANodeAllocatableResourcesFeatureGate: true,
		},
		// 两种 overhead 均合法，验证可同时设置。
		"good-node-allocatable-resources-overhead": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithNodeAllocatableResources(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].NodeAllocatableResources = map[v1.ResourceName]resourceapi.NodeAllocatableResource{
					v1.ResourceMemory: {
						Overhead: &resourceapi.NodeAllocatableOverhead{
							PerPod:       ptr.To(resource.MustParse("1Gi")),
							PerContainer: ptr.To(resource.MustParse("500Mi")),
						},
					},
				}
				return slice
			}(),
			enableDRANodeAllocatableResourcesFeatureGate: true,
		},
		// mapping 与 overhead 同时设置合法，验证二者可共存。
		"good-node-allocatable-resources-both-mapping-and-overhead-set": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithNodeAllocatableResources(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].NodeAllocatableResources = map[v1.ResourceName]resourceapi.NodeAllocatableResource{
					v1.ResourceCPU: {
						Mapping: &resourceapi.NodeAllocatableMapping{
							CapacityKey:        new[resourceapi.QualifiedName]("dra.example.com/cpu"),
							CapacityMultiplier: new(resource.MustParse("1")),
						},
						Overhead: &resourceapi.NodeAllocatableOverhead{
							PerPod: new(resource.MustParse("1Gi")),
						},
					},
				}
				return slice
			}(),
			enableDRANodeAllocatableResourcesFeatureGate: true,
		},
		// mapping 与 overhead 均未设置，验证至少一个必填。
		"bad-node-allocatable-resources-neither-mapping-nor-overhead-set": {
			wantFailures: field.ErrorList{
				field.Required(field.NewPath("spec", "devices").Index(0).Child("nodeAllocatableResources").Key(string(v1.ResourceCPU)), "at least one of mapping or overhead must be set"),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithNodeAllocatableResources(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].NodeAllocatableResources = map[v1.ResourceName]resourceapi.NodeAllocatableResource{
					v1.ResourceCPU: {},
				}
				return slice
			}(),
			enableDRANodeAllocatableResourcesFeatureGate: true,
		},
		// 只设 capacityKey 未设 multiplier，验证成对约束。
		"bad-node-allocatable-resources-capacity-key-set-without-multiplier": {
			wantFailures: field.ErrorList{
				field.Required(field.NewPath("spec", "devices").Index(0).Child("nodeAllocatableResources").Key(string(v1.ResourceCPU)).Child("mapping").Child("capacityMultiplier"), "capacityMultiplier is required when capacityKey is set").MarkCoveredByDeclarative(),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithNodeAllocatableResources(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].NodeAllocatableResources = map[v1.ResourceName]resourceapi.NodeAllocatableResource{
					v1.ResourceCPU: {
						Mapping: &resourceapi.NodeAllocatableMapping{
							CapacityKey: new[resourceapi.QualifiedName]("dra.example.com/cpu"),
						},
					},
				}
				return slice
			}(),
			enableDRANodeAllocatableResourcesFeatureGate: true,
		},
		// 只设 multiplier 未设 capacityKey，验证成对约束的反向。
		"bad-node-allocatable-resources-multiplier-set-without-capacity-key": {
			wantFailures: field.ErrorList{
				field.Required(field.NewPath("spec", "devices").Index(0).Child("nodeAllocatableResources").Key(string(v1.ResourceCPU)).Child("mapping").Child("capacityKey"), "capacityKey is required when capacityMultiplier is set").MarkCoveredByDeclarative(),
			},
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithNodeAllocatableResources(goodName, goodName, driverName, 1)
				slice.Spec.Devices[0].NodeAllocatableResources = map[v1.ResourceName]resourceapi.NodeAllocatableResource{
					v1.ResourceCPU: {
						Mapping: &resourceapi.NodeAllocatableMapping{
							CapacityMultiplier: new(resource.MustParse("1")),
						},
					},
				}
				return slice
			}(),
			enableDRANodeAllocatableResourcesFeatureGate: true,
		},
		// All 与 Prepare 同时出现合法，验证 All 可与其他项共存。
		"valid: skipNodeOperations All and Prepare": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Spec.SkipNodeOperations = []resourceapi.SkipNodeOperation{resourceapi.SkipNodeOperationAll, resourceapi.SkipNodeOperationNodePrepareResources}
				return slice
			}(),
		},
		// Prepare 与 Unprepare 成对出现合法，验证配对规则。
		"valid: skipNodeOperations Prepare and Unprepare": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Spec.SkipNodeOperations = []resourceapi.SkipNodeOperation{resourceapi.SkipNodeOperationNodePrepareResources, resourceapi.SkipNodeOperationNodeUnprepareResources}
				return slice
			}(),
		},
		// 只跳过 Prepare 未跳过 Unprepare，验证二者必须成对。
		"invalid: skipNodeOperations Prepare only": {
			slice: func() *resourceapi.ResourceSlice {
				slice := testResourceSlice(goodName, goodName, driverName, 1)
				slice.Spec.SkipNodeOperations = []resourceapi.SkipNodeOperation{resourceapi.SkipNodeOperationNodePrepareResources}
				return slice
			}(),
			wantFailures: field.ErrorList{
				field.Invalid(field.NewPath("spec", "skipNodeOperations"), []resourceapi.SkipNodeOperation{resourceapi.SkipNodeOperationNodePrepareResources}, "NodePrepareResources cannot be skipped unless NodeUnprepareResources is also skipped"),
			},
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.DRANodeAllocatableResources: scenario.enableDRANodeAllocatableResourcesFeatureGate,
				features.DRAConsumableCapacity:       scenario.consumableCapacityFeatureGate,
				features.DRAFractionalCapacityRange:  scenario.fractionalCapacityRangeFeatureGate,
			})
			// 逐用例设置开关，避免某个用例的状态泄漏到后续用例。
			errs := ValidateResourceSlice(scenario.slice)
			assertFailures(t, scenario.wantFailures, errs)
		})
	}
}

// TestValidateResourceSliceUpdate 覆盖更新校验：不可变字段、旧值豁免，以及特性开关下的增量行为。
func TestValidateResourceSliceUpdate(t *testing.T) {
	name := "valid"
	validResourceSlice := testResourceSlice(name, name, name, 1)
	invalidResourceSliceWithTaints := validResourceSlice.DeepCopy()
	invalidResourceSliceWithTaints.Spec.Devices[0].Taints = []resourceapi.DeviceTaint{
		{
			Key:    "unhealthy-power",
			Effect: resourceapi.DeviceTaintEffectNoExecute,
		},
		{
			Key:    "unhealthy-mem",
			Effect: "some-other-effect",
		},
	}

	scenarios := map[string]struct {
		consumableCapacityFeatureGate      bool
		fractionalCapacityRangeFeatureGate bool
		oldResourceSlice                   *resourceapi.ResourceSlice
		update                             func(slice *resourceapi.ResourceSlice) *resourceapi.ResourceSlice
		wantFailures                       field.ErrorList
	}{
		// 无变化的更新应直接通过，验证更新校验不误报。
		"valid-no-op-update": {
			oldResourceSlice: validResourceSlice,
			update:           func(slice *resourceapi.ResourceSlice) *resourceapi.ResourceSlice { return slice },
		},
		// 修改 name，验证元数据名称不可变。
		"invalid-name-update": {
			oldResourceSlice: validResourceSlice,
			update: func(slice *resourceapi.ResourceSlice) *resourceapi.ResourceSlice {
				slice.Name += "-update"
				return slice
			},
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("metadata", "name"), name+"-update", "field is immutable")},
		},
		// 修改 nodeName，验证节点归属不可变。
		"invalid-update-nodename": {
			wantFailures:     field.ErrorList{field.Invalid(field.NewPath("spec", "nodeName"), ptr.To(name+"-updated"), "field is immutable")},
			oldResourceSlice: validResourceSlice,
			update: func(slice *resourceapi.ResourceSlice) *resourceapi.ResourceSlice {
				slice.Spec.NodeName = ptr.To(*slice.Spec.NodeName + "-updated")
				return slice
			},
		},
		// 修改 driver，验证驱动名不可变。
		"invalid-update-drivername": {
			wantFailures:     field.ErrorList{field.Invalid(field.NewPath("spec", "driver"), name+"-updated", "field is immutable")},
			oldResourceSlice: validResourceSlice,
			update: func(slice *resourceapi.ResourceSlice) *resourceapi.ResourceSlice {
				slice.Spec.Driver += "-updated"
				return slice
			},
		},
		// 修改 pool.name，验证池名不可变。
		"invalid-update-pool": {
			wantFailures:     field.ErrorList{field.Invalid(field.NewPath("spec", "pool", "name"), validResourceSlice.Spec.Pool.Name+"-updated", "field is immutable")},
			oldResourceSlice: validResourceSlice,
			update: func(slice *resourceapi.ResourceSlice) *resourceapi.ResourceSlice {
				slice.Spec.Pool.Name += "-updated"
				return slice
			},
		},
		// 更新 nodeSelector 为非法 label 值，验证更新路径也走完整校验。
		"invalid-update-to-invalid-nodeselector-label-value": {
			wantFailures: field.ErrorList{field.Invalid(field.NewPath("spec", "nodeSelector", "nodeSelectorTerms").Index(0).Child("matchExpressions").Index(0).Child("values").Index(0), "-1", "a valid label must be an empty string or consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character (e.g. 'MyValue',  or 'my_value',  or '12345', regex used for validation is '(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])?')")},
			oldResourceSlice: func() *resourceapi.ResourceSlice {
				slice := validResourceSlice.DeepCopy()
				slice.Spec.NodeName = nil
				slice.Spec.NodeSelector = &core.NodeSelector{
					NodeSelectorTerms: []core.NodeSelectorTerm{{
						MatchExpressions: []core.NodeSelectorRequirement{{
							Key:      "foo",
							Operator: core.NodeSelectorOpIn,
							Values:   []string{"bar"},
						}},
					}},
				}
				return slice
			}(),
			update: func(slice *resourceapi.ResourceSlice) *resourceapi.ResourceSlice {
				slice.Spec.NodeSelector = &core.NodeSelector{
					NodeSelectorTerms: []core.NodeSelectorTerm{{
						MatchExpressions: []core.NodeSelectorRequirement{{
							Key:      "foo",
							Operator: core.NodeSelectorOpIn,
							Values:   []string{"-1"},
						}},
					}},
				}
				return slice
			},
		},
		// 给已有设备加入非法 effect，验证新写入值受声明式校验约束。
		"invalid-new-effect-in-old-device": {
			wantFailures:     field.ErrorList{field.NotSupported(field.NewPath("spec", "devices").Index(0).Child("taints").Index(1).Child("effect"), resourceapi.DeviceTaintEffect("some-other-effect"), []resourceapi.DeviceTaintEffect{resourceapi.DeviceTaintEffectNoExecute, resourceapi.DeviceTaintEffectNoSchedule, resourceapi.DeviceTaintEffectNone})}.MarkCoveredByDeclarative(),
			oldResourceSlice: validResourceSlice,
			update: func(slice *resourceapi.ResourceSlice) *resourceapi.ResourceSlice {
				slice.Spec.Devices[0].Taints = invalidResourceSliceWithTaints.Spec.Devices[0].Taints
				return slice
			},
		},
		// 旧对象已含非法 effect（历史遗留），验证增量校验不会因旧值报错。
		"valid-old-effect": {
			oldResourceSlice: invalidResourceSliceWithTaints,
			update: func(slice *resourceapi.ResourceSlice) *resourceapi.ResourceSlice {
				slice.Spec.Devices[0].Attributes["foo"] = resourceapi.DeviceAttribute{StringValue: ptr.To("bar")}
				return slice
			},
		},
		// 新增设备带非法 effect，验证新设备不享受旧值豁免。
		"invalid-new-effect-in-new-device": {
			wantFailures:     field.ErrorList{field.NotSupported(field.NewPath("spec", "devices").Index(1).Child("taints").Index(1).Child("effect"), resourceapi.DeviceTaintEffect("some-other-effect"), []resourceapi.DeviceTaintEffect{resourceapi.DeviceTaintEffectNoExecute, resourceapi.DeviceTaintEffectNoSchedule, resourceapi.DeviceTaintEffectNone})}.MarkCoveredByDeclarative(),
			oldResourceSlice: invalidResourceSliceWithTaints,
			update: func(slice *resourceapi.ResourceSlice) *resourceapi.ResourceSlice {
				device := slice.Spec.Devices[0].DeepCopy()
				device.Name += "-other"
				slice.Spec.Devices = append(slice.Spec.Devices, *device)
				return slice
			},
		},
		// 整数模式下新写入的 validValues 出现重复，验证被拦截。
		"consumable-capacity-valid-values-integer": {
			consumableCapacityFeatureGate:      true,
			fractionalCapacityRangeFeatureGate: false,
			wantFailures:                       field.ErrorList{field.Duplicate(consumeableCapacityPath(0).Child("requestPolicy", "validValues").Index(1), "10")},
			oldResourceSlice:                   testResourceSliceWithConsumableCapacity(name, name, name, 1),
			update: func(slice *resourceapi.ResourceSlice) *resourceapi.ResourceSlice {
				updateConsumableCapacity(slice, 0, func(cap *resourceapi.DeviceCapacity) {
					fraction := resource.MustParse("0.1")
					min := cap.RequestPolicy.ValidValues[1]
					minFrac := min.DeepCopy()
					minFrac.Sub(fraction)
					cap.RequestPolicy.ValidValues[0] = minFrac // 整数模式下构成重复。

				})
				return slice
			},
		},
		// 同样的重复值若已在旧对象中，验证更新时被豁免以兼容历史数据。
		"consumable-capacity-valid-values-integer-okay-if-stored": {
			consumableCapacityFeatureGate:      true,
			fractionalCapacityRangeFeatureGate: false,
			wantFailures:                       nil,
			oldResourceSlice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithConsumableCapacity(name, name, name, 1)
				updateConsumableCapacity(slice, 0, func(cap *resourceapi.DeviceCapacity) {
					fraction := resource.MustParse("0.1")
					min := cap.RequestPolicy.ValidValues[1]
					minFrac := min.DeepCopy()
					minFrac.Sub(fraction)
					cap.RequestPolicy.ValidValues[0] = minFrac // 整数模式下构成重复。
				})
				return slice
			}(),
			update: func(slice *resourceapi.ResourceSlice) *resourceapi.ResourceSlice { return slice },
		},
		// 新写入 range.min 为负，验证边界校验。
		"consumable-capacity-valid-range-out-of-int64-bound": {
			consumableCapacityFeatureGate: true,
			wantFailures:                  field.ErrorList{field.Invalid(consumeableCapacityPath(0).Child("requestPolicy", "validRange", "min"), "-1", "must be greater than or equal to 0")},
			oldResourceSlice:              testResourceSliceWithConsumableCapacity(name, name, name, 1),
			update: func(slice *resourceapi.ResourceSlice) *resourceapi.ResourceSlice {
				updateConsumableCapacity(slice, 0, func(cap *resourceapi.DeviceCapacity) {
					cap.RequestPolicy = &resourceapi.CapacityRequestPolicy{
						Default:    new(resource.MustParse("0")),
						ValidRange: &resourceapi.CapacityRequestPolicyRange{Min: new(resource.MustParse("-1")), Step: new(resource.MustParse("1"))},
					}
				})
				return slice
			},
		},
		// 同样的越界值若已存储，验证更新时豁免。
		"consumable-capacity-valid-range-out-of-int64-bound-okay-if-stored": {
			consumableCapacityFeatureGate: true,
			wantFailures:                  nil,
			oldResourceSlice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithConsumableCapacity(name, name, name, 1)
				updateConsumableCapacity(slice, 0, func(cap *resourceapi.DeviceCapacity) {
					cap.RequestPolicy = &resourceapi.CapacityRequestPolicy{
						Default:    new(resource.MustParse("0")),
						ValidRange: &resourceapi.CapacityRequestPolicyRange{Min: new(resource.MustParse("-1")), Step: new(resource.MustParse("1"))},
					}
				})
				return slice
			}(),
			update: func(slice *resourceapi.ResourceSlice) *resourceapi.ResourceSlice { return slice },
		},
		// 毫精度下微小差值不构成重复，验证精度对判定的影响。
		"consumable-capacity-valid-values-milli-values": {
			consumableCapacityFeatureGate:      true,
			fractionalCapacityRangeFeatureGate: true,
			wantFailures:                       nil,
			oldResourceSlice:                   testResourceSliceWithConsumableCapacity(name, name, name, 1),
			update: func(slice *resourceapi.ResourceSlice) *resourceapi.ResourceSlice {
				updateConsumableCapacity(slice, 0, func(cap *resourceapi.DeviceCapacity) {
					fraction := resource.MustParse("0.1")
					min := cap.RequestPolicy.ValidValues[1]
					minFrac := min.DeepCopy()
					minFrac.Sub(fraction)
					cap.RequestPolicy.ValidValues[0] = minFrac // 毫精度下可接受。

				})
				return slice
			},
		},
		// 整数模式取整后超大范围合法，验证取整逻辑。
		"consumable-capacity-valid-range-integer": {
			consumableCapacityFeatureGate:      true,
			fractionalCapacityRangeFeatureGate: false,
			wantFailures:                       nil,
			oldResourceSlice:                   testResourceSliceWithConsumableCapacity(name, name, name, 1),
			update: func(slice *resourceapi.ResourceSlice) *resourceapi.ResourceSlice {
				updateConsumableCapacity(slice, 0, func(cap *resourceapi.DeviceCapacity) {
					// 非法小数：范围过大，但整数模式取整后合法。
					cap.Value.Set(math.MaxInt64)
					cap.RequestPolicy.Default.Set(math.MaxInt64)
					cap.RequestPolicy.ValidValues = nil
					cap.RequestPolicy.ValidRange = &resourceapi.CapacityRequestPolicyRange{
						Min:  resource.NewMilliQuantity(0, resource.DecimalSI),
						Step: resource.NewMilliQuantity(1, resource.DecimalSI),
						Max:  &cap.Value,
					}
				})
				return slice
			},
		},
		// 小数模式下同一范围非法但已存储，验证更新豁免。
		"consumable-capacity-valid-range-integer-okay-if-stored": {
			consumableCapacityFeatureGate:      true,
			fractionalCapacityRangeFeatureGate: true,
			wantFailures:                       nil,
			oldResourceSlice: func() *resourceapi.ResourceSlice {
				slice := testResourceSliceWithConsumableCapacity(name, name, name, 1)
				updateConsumableCapacity(slice, 0, func(cap *resourceapi.DeviceCapacity) {
					// 非法小数：范围过大。
					cap.Value.Set(math.MaxInt64)
					cap.RequestPolicy.Default.Set(math.MaxInt64)
					cap.RequestPolicy.ValidValues = nil
					cap.RequestPolicy.ValidRange = &resourceapi.CapacityRequestPolicyRange{
						Min:  resource.NewMilliQuantity(0, resource.DecimalSI),
						Step: resource.NewMilliQuantity(1, resource.DecimalSI),
						Max:  &cap.Value,
					}
				})
				return slice
			}(),
			update: func(slice *resourceapi.ResourceSlice) *resourceapi.ResourceSlice { return slice },
		},
		// 毫精度下范围超出可表示精度，验证被拒绝。
		"consumable-capacity-valid-range-milli-values": {
			consumableCapacityFeatureGate:      true,
			fractionalCapacityRangeFeatureGate: true,
			wantFailures: field.ErrorList{
				field.Invalid(consumeableCapacityPath(0).Child("requestPolicy", "validRange", "default"), fmt.Sprintf("%d", math.MaxInt), "value cannot be represented as a milli value"),
				field.Invalid(consumeableCapacityPath(0).Child("requestPolicy", "validRange", "max"), fmt.Sprintf("%d", math.MaxInt), "value cannot be represented as a milli value"),
			},
			oldResourceSlice: testResourceSliceWithConsumableCapacity(name, name, name, 1),
			update: func(slice *resourceapi.ResourceSlice) *resourceapi.ResourceSlice {
				updateConsumableCapacity(slice, 0, func(cap *resourceapi.DeviceCapacity) {
					// 非法小数：范围过大。
					cap.Value.Set(math.MaxInt64)
					cap.RequestPolicy.Default.Set(math.MaxInt64)
					cap.RequestPolicy.ValidValues = nil
					cap.RequestPolicy.ValidRange = &resourceapi.CapacityRequestPolicyRange{
						Min:  resource.NewMilliQuantity(0, resource.DecimalSI),
						Step: resource.NewMilliQuantity(1, resource.DecimalSI),
						Max:  &cap.Value,
					}
				})
				return slice
			},
		},
	}

	for name, scenario := range scenarios {
		t.Run(name, func(t *testing.T) {
			featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featuregatetesting.FeatureOverrides{
				features.DRAConsumableCapacity:      scenario.consumableCapacityFeatureGate,
				features.DRAFractionalCapacityRange: scenario.fractionalCapacityRangeFeatureGate,
			})
			// 更新校验要求 old/new 都带 resourceVersion，这里统一补齐。
			scenario.oldResourceSlice.ResourceVersion = "1"
			errs := ValidateResourceSliceUpdate(scenario.update(scenario.oldResourceSlice.DeepCopy()), scenario.oldResourceSlice)
			assertFailures(t, scenario.wantFailures, errs)
		})
	}
}

// createSharedCounters 生成指定数量的 counterSet，用于共享计数上限用例。
func createSharedCounters(count int) []resourceapi.CounterSet {
	sharedCounters := make([]resourceapi.CounterSet, count)
	for i := 0; i < count; i++ {
		sharedCounters[i] = resourceapi.CounterSet{
			Name:     fmt.Sprintf("counterset-%d", i),
			Counters: testCounters(),
		}
	}
	return sharedCounters
}

// createConsumesCounters 生成指定数量的设备 counter 消费项，用于消费上限用例。
func createConsumesCounters(count int) []resourceapi.DeviceCounterConsumption {
	consumeCapacity := make([]resourceapi.DeviceCounterConsumption, count)
	for i := 0; i < count; i++ {
		consumeCapacity[i] = resourceapi.DeviceCounterConsumption{
			CounterSet: fmt.Sprintf("counterset-%d", i),
			Counters:   testCounters(),
		}
	}
	return consumeCapacity
}
