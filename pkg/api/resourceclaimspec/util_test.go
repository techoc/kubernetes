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

package resourceclaimspec

import (
	"testing"

	"github.com/stretchr/testify/assert"

	apiresource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/apis/resource"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/utils/ptr"
)

// testCapacity 是最小的容量请求，仅用于让 claim 带上“特性使用中”的字段。
var testCapacity = map[resource.QualifiedName]apiresource.Quantity{
	resource.QualifiedName("test-capacity"): apiresource.MustParse("1"),
}

// obj 是使用 Exactly 请求的基线 claim，不含任何需要特性门控的字段。
var obj = &resource.ResourceClaim{
	ObjectMeta: metav1.ObjectMeta{
		Name:      "valid-claim",
		Namespace: "kube-system",
	},
	Spec: resource.ResourceClaimSpec{
		Devices: resource.DeviceClaim{
			Requests: []resource.DeviceRequest{
				{
					Name: "req-0",
					Exactly: &resource.ExactDeviceRequest{
						DeviceClassName: "class",
						AllocationMode:  resource.DeviceAllocationModeAll,
					},
				},
			},
		},
	},
}

// objWithPrioritizedList 使用 FirstAvailable（优先级列表）请求，
// 用于覆盖特性字段藏在子请求里的场景。
var objWithPrioritizedList = &resource.ResourceClaim{
	ObjectMeta: metav1.ObjectMeta{
		Name:      "valid-claim",
		Namespace: "kube-system",
	},
	Spec: resource.ResourceClaimSpec{
		Devices: resource.DeviceClaim{
			Requests: []resource.DeviceRequest{
				{
					Name: "req-0",
					FirstAvailable: []resource.DeviceSubRequest{
						{
							Name:            "subreq-0",
							DeviceClassName: "class",
							AllocationMode:  resource.DeviceAllocationModeExactCount,
							Count:           1,
						},
					},
				},
			},
		},
	},
}

// modifySpecDeviceRequestWithCapacityRequests 把容量请求注入指定请求分支：
// 开启优先级列表时写入 FirstAvailable[0]，否则写入 Exactly。
// 这样同一个辅助函数能覆盖两种结构上不同的请求形态。
func modifySpecDeviceRequestWithCapacityRequests(resourceClaim *resource.ResourceClaim,
	capacity map[resource.QualifiedName]apiresource.Quantity, prioritizedListFeature bool) {
	if capacity != nil {
		if prioritizedListFeature {
			resourceClaim.Spec.Devices.Requests[0].FirstAvailable[0].Capacity = &resource.CapacityRequirements{
				Requests: capacity,
			}
		} else {
			resourceClaim.Spec.Devices.Requests[0].Exactly.Capacity = &resource.CapacityRequirements{
				Requests: capacity,
			}
		}
	}
}

// addDistinctAttribute 增加一个带 DistinctAttribute 的约束，
// 它是独立于 capacity 的另一类 DRA 特性使用信号。
func addDistinctAttribute(resourceClaim *resource.ResourceClaim) {
	distinctConstraint := resource.DeviceConstraint{
		Requests:          []string{"req-0"},
		DistinctAttribute: ptr.To(resource.FullyQualifiedName("driver-a/attr")),
	}
	resourceClaim.Spec.Devices.Constraints = append(resourceClaim.Spec.Devices.Constraints, distinctConstraint)
}

// TestDRAConsumableCapacityFeatureInUse 验证“可消费容量”特性是否被使用的判定：
// nil spec 与不含容量字段的 claim 返回 false；Exactly、FirstAvailable 子请求
// 中出现容量请求时返回 true，同时带 DistinctAttribute 也不影响该判定。
func TestDRAConsumableCapacityFeatureInUse(t *testing.T) {
	testcases := map[string]struct {
		obj    *resource.ResourceClaim
		expect bool
	}{
		// nil claim 对应 nil spec，判定函数必须能安全处理。
		"consumable-capacity-empty": {
			obj:    nil,
			expect: false,
		},
		// 基线 claim 没有容量字段，不应误判为使用中。
		"consumable-capacity-no-inuse": {
			obj:    obj,
			expect: false,
		},
		// Exactly 请求中带容量字段：应判为使用中。
		"consumable-capacity-with-inuse-fields": {
			obj: func() *resource.ResourceClaim {
				// 在副本上修改，避免污染被多个用例共享的基线对象。
				obj := obj.DeepCopy()
				modifySpecDeviceRequestWithCapacityRequests(obj, testCapacity, false)
				return obj
			}(),
			expect: true,
		},
		// DistinctAttribute 是另一类使用信号，叠加后依然应判为使用中。
		"consumable-capacity--with-inuse-fields-with-distinct-attribute": {
			obj: func() *resource.ResourceClaim {
				obj := obj.DeepCopy()
				modifySpecDeviceRequestWithCapacityRequests(obj, testCapacity, false)
				addDistinctAttribute(obj)
				return obj
			}(),
			expect: true,
		},
		// 容量请求位于 FirstAvailable 子请求中时也必须被识别。
		"consumable-capacity--with-inuse-fields-in-subrequests": {
			obj: func() *resource.ResourceClaim {
				obj := objWithPrioritizedList.DeepCopy()
				modifySpecDeviceRequestWithCapacityRequests(obj, testCapacity, true)
				return obj
			}(),
			expect: true,
		},
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			var spec *resource.ResourceClaimSpec
			if tc.obj != nil {
				// 只把 spec 传给被检查函数；nil claim 保持 nil spec 不变。
				spec = &tc.obj.Spec
			}
			assert.Equal(t, DRAConsumableCapacityFeatureInUse(spec), tc.expect)
		})
	}
}

// TestDRADerivedAttributesFeatureInUse 验证派生属性特性是否被使用的判定：
// Exactly 与 FirstAvailable 两种请求分支中只要出现 DerivedAttributes 即返回 true，
// nil spec 和不含该字段的 claim 返回 false。
func TestDRADerivedAttributesFeatureInUse(t *testing.T) {
	testcases := map[string]struct {
		obj    *resource.ResourceClaim
		expect bool
	}{
		// nil 输入不能 panic，且应判为未使用。
		"derived-attributes-empty": {
			obj:    nil,
			expect: false,
		},
		// 基线 claim 不含派生属性，应判为未使用。
		"derived-attributes-no-inuse": {
			obj:    obj,
			expect: false,
		},
		// Exactly 分支中带 DerivedAttributes：应判为使用中。
		"derived-attributes-with-inuse-fields-exactly": {
			obj: func() *resource.ResourceClaim {
				obj := obj.DeepCopy()
				obj.Spec.Devices.Requests[0].Exactly.DerivedAttributes = []resource.DeviceDerivedAttribute{
					{
						Name:       "sharedNumaNode",
						Expression: `device.attributes["dra.example.com"]["numa"]`,
					},
				}
				return obj
			}(),
			expect: true,
		},
		// 同样的字段位于 FirstAvailable 子请求里也应被识别。
		"derived-attributes-with-inuse-fields-first-available": {
			obj: func() *resource.ResourceClaim {
				obj := objWithPrioritizedList.DeepCopy()
				obj.Spec.Devices.Requests[0].FirstAvailable[0].DerivedAttributes = []resource.DeviceDerivedAttribute{
					{
						Name:       "sharedNumaNode",
						Expression: `device.attributes["dra.example.com"]["numa"]`,
					},
				}
				return obj
			}(),
			expect: true,
		},
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			var spec *resource.ResourceClaimSpec
			if tc.obj != nil {
				spec = &tc.obj.Spec
			}
			assert.Equal(t, draDerivedAttributesInUse(spec), tc.expect)
		})
	}
}

// TestDropDisabledDRADerivedAttributesFields 验证 DropDisabledFields 对派生属性字段的裁剪规则：
// 创建请求在门控关闭时丢弃该字段；门控开启时保留；更新请求中若旧对象已使用该字段，
// 即使门控关闭也要保留，以免破坏既有对象。
func TestDropDisabledDRADerivedAttributesFields(t *testing.T) {
	claim := obj.DeepCopy()
	claim.Spec.Devices.Requests[0].Exactly.DerivedAttributes = []resource.DeviceDerivedAttribute{
		{
			Name:       "sharedNumaNode",
			Expression: `device.attributes["dra.example.com"]["numa"]`,
		},
	}

	t.Run("drop-on-create-when-feature-gate-disabled", func(t *testing.T) {
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DRADerivedAttributes, false)
		newClaim := claim.DeepCopy()
		// 创建时旧对象为 nil，门控关闭意味着该字段必须被丢弃。
		DropDisabledFields(&newClaim.Spec, nil)
		assert.Nil(t, newClaim.Spec.Devices.Requests[0].Exactly.DerivedAttributes)
	})

	t.Run("keep-on-create-when-feature-gate-enabled", func(t *testing.T) {
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DRADerivedAttributes, true)
		newClaim := claim.DeepCopy()
		// 门控开启时没有理由裁剪，字段应原样保留。
		DropDisabledFields(&newClaim.Spec, nil)
		assert.NotNil(t, newClaim.Spec.Devices.Requests[0].Exactly.DerivedAttributes)
	})

	t.Run("keep-on-update-when-feature-gate-disabled-but-in-use", func(t *testing.T) {
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.DRADerivedAttributes, false)
		newClaim := claim.DeepCopy()
		oldClaim := claim.DeepCopy()
		// 旧对象已经使用该字段，更新时必须容忍它，否则会破坏兼容性。
		DropDisabledFields(&newClaim.Spec, &oldClaim.Spec)
		assert.NotNil(t, newClaim.Spec.Devices.Requests[0].Exactly.DerivedAttributes)
	})
}
