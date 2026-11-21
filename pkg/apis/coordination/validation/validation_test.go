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

package validation

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/kubernetes/pkg/apis/coordination"
	"k8s.io/utils/ptr"
)

// TestValidateLease 验证 ValidateLease 会同时校验 ObjectMeta 中的 name 与 namespace。
// 这里两个字段都故意填成非法值，因此应各产生一条错误，共两条；借此确认两个字段都被检查到，
// 而不是只校验了其中一个。
func TestValidateLease(t *testing.T) {
	lease := &coordination.Lease{
		ObjectMeta: metav1.ObjectMeta{
			// name 含 '+' 且含大写字母，不是合法的 DNS 子域名，校验应失败；
			// namespace 含 '=' 与 '_' 等非法字符，校验同样应失败。
			Name:      "invalidName++",
			Namespace: "==invalid_Namespace==",
		},
	}
	errs := ValidateLease(lease)
	// 恰好两条错误：一条来自 name，一条来自 namespace。
	if len(errs) != 2 {
		t.Errorf("unexpected list of errors: %#v", errs.ToAggregate().Error())
	}
}

// TestValidateLeaseSpec 以表驱动方式验证 LeaseSpec 的字段校验规则。
// 表格覆盖：基本合法配置、带 PreferredHolder 的合法配置、leaseDurationSeconds 为 0 且
// leaseTransitions 为 -1 的非法配置，以及上述非法配置叠加 PreferredHolder 的情况。
// 关键点在于：duration 必须为正数、transitions 不得为负；PreferredHolder 本身合法，
// 不能掩盖其他字段的非法性。
func TestValidateLeaseSpec(t *testing.T) {
	holder := "holder"
	// 0 是 leaseDurationSeconds 的非法下界（要求大于 0）。
	leaseDuration := int32(0)
	// -1 是 leaseTransitions 的非法值（要求非负）。
	leaseTransitions := int32(-1)
	preferredHolder := "holder2"

	testcases := []struct {
		spec coordination.LeaseSpec
		err  bool
	}{
		{
			// 合法
			coordination.LeaseSpec{
				HolderIdentity:       &holder,
				LeaseDurationSeconds: ptr.To[int32](10),
				LeaseTransitions:     ptr.To[int32](1),
			},
			false,
		},
		{
			// 带 PreferredHolder 时合法：仅当 Strategy 为 OldestEmulationVersion
			// 时 PreferredHolder 才被允许，这里满足该前提。
			coordination.LeaseSpec{
				HolderIdentity:       &holder,
				LeaseDurationSeconds: ptr.To[int32](10),
				LeaseTransitions:     ptr.To[int32](1),
				Strategy:             ptr.To(coordination.OldestEmulationVersion),
				PreferredHolder:      ptr.To("someotherholder"),
			},
			false,
		},
		{
			// 非法：duration 为 0、transitions 为 -1，二者都不满足取值约束。
			coordination.LeaseSpec{
				HolderIdentity:       &holder,
				LeaseDurationSeconds: &leaseDuration,
				LeaseTransitions:     &leaseTransitions,
			},
			true,
		},
		{
			// 非法：即使带上 PreferredHolder，duration/transitions 的非法性依然存在，
			// 说明校验不会因新增字段而跳过其他字段。
			coordination.LeaseSpec{
				HolderIdentity:       &holder,
				LeaseDurationSeconds: &leaseDuration,
				LeaseTransitions:     &leaseTransitions,
				PreferredHolder:      &preferredHolder,
			},
			true,
		},
	}

	for _, tc := range testcases {
		// 用固定的 "foo" 路径检查错误路径生成不影响是否报错的判定。
		errs := ValidateLeaseSpec(&tc.spec, field.NewPath("foo"))
		// 期望报错却没有报，或不该报错却报错，都视为测试失败。
		if tc.err && len(errs) == 0 {
			t.Error("Expected err, got no err")
		} else if !tc.err && len(errs) != 0 {
			t.Errorf("Expected no err, got err %v", errs)
		}
	}
}

// TestValidateLeaseSpecUpdate 验证 Lease 更新时的整体校验：既要检查 update 对象自身的合法性，
// 也要检查新旧对象之间 ResourceVersion 是否递增这一不变量。
// 第一段构造的 update 含有与 TestValidateLeaseSpec 相同的非法 duration/transitions，因此预期三条错误；
// 第二段换成合法字段并设置更大的 ResourceVersion，预期零错误。
func TestValidateLeaseSpecUpdate(t *testing.T) {
	holder := "holder"
	// 非法的 duration 与 transitions，用于触发字段级校验错误。
	leaseDuration := int32(0)
	leaseTransitions := int32(-1)
	lease := &coordination.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "holder",
			Namespace: "holder-namespace",
		},
		Spec: coordination.LeaseSpec{
			HolderIdentity:       &holder,
			LeaseDurationSeconds: &leaseDuration,
			LeaseTransitions:     &leaseTransitions,
		},
	}
	oldHolder := "oldHolder"
	oldLeaseDuration := int32(3)
	oldLeaseTransitions := int32(3)
	oldLease := &coordination.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "holder",
			Namespace: "holder-namespace",
		},
		Spec: coordination.LeaseSpec{
			HolderIdentity:       &oldHolder,
			LeaseDurationSeconds: &oldLeaseDuration,
			LeaseTransitions:     &oldLeaseTransitions,
		},
	}
	errs := ValidateLeaseUpdate(lease, oldLease)
	// 两条非法字段各一条错误，外加 update 未设置 ResourceVersion 导致的一条错误。
	if len(errs) != 3 {
		t.Errorf("unexpected list of errors: %#v", errs.ToAggregate().Error())
	}

	// 合法取值：正数的 duration 与非负的 transitions。
	validLeaseDuration := int32(10)
	validLeaseTransitions := int32(20)
	validLease := &coordination.Lease{
		ObjectMeta: oldLease.ObjectMeta,
		Spec: coordination.LeaseSpec{
			HolderIdentity:       &holder,
			LeaseDurationSeconds: &validLeaseDuration,
			LeaseTransitions:     &validLeaseTransitions,
		},
	}
	// 更新对象的 ResourceVersion 必须比旧对象大，这里旧对象未设置，故 "2" 合法。
	validLease.ObjectMeta.ResourceVersion = "2"
	errs = ValidateLeaseUpdate(validLease, oldLease)
	// 合法更新不应产生任何错误。
	if len(errs) != 0 {
		t.Errorf("unexpected list of errors for valid update: %#v", errs.ToAggregate().Error())
	}
}

// TestValidateLeaseCandidate 验证 ValidateLeaseCandidate 会校验 LeaseCandidate 的 ObjectMeta。
// 与 Lease 相同，这里 name 和 namespace 均为非法值，因此预期至少产生一条错误，
// 确保元数据校验确实被调用。
func TestValidateLeaseCandidate(t *testing.T) {
	lease := &coordination.LeaseCandidate{
		ObjectMeta: metav1.ObjectMeta{
			// name 含 '+' 与大写字母，不是合法的 DNS 子域名；namespace 含 '=' 与 '_'，同样不合法。
			Name:      "invalidName++",
			Namespace: "==invalid_Namespace==",
		},
	}
	errs := ValidateLeaseCandidate(lease)
	// 元数据非法时应返回错误。
	if len(errs) == 0 {
		t.Errorf("expected invalid LeaseCandidate")
	}
}

// TestValidateLeaseCandidateSpec 以表驱动方式验证 LeaseCandidateSpec 的字段校验。
// 表格覆盖：默认策略下的合法配置、自定义策略（不要求 emulationVersion）的合法配置，
// 以及多个错误场景——缺少 binaryVersion、缺少 LeaseName、binaryVersion 格式非法、
// emulationVersion 低于 binaryVersion、策略格式非法、缺少策略、使用内置策略却缺少 emulationVersion。
// 这些用例保护的核心不变量是：binaryVersion 与 LeaseName 必填，版本号须为合法语义化版本且
// emulationVersion 不小于 binaryVersion，策略须合法；仅内置策略 OldestEmulationVersion 才要求 emulationVersion。
func TestValidateLeaseCandidateSpec(t *testing.T) {
	testcases := []struct {
		name      string
		shouldErr bool
		spec      *coordination.LeaseCandidateSpec
	}{
		{
			"valid",
			false,
			&coordination.LeaseCandidateSpec{
				BinaryVersion:    "1.30.0",
				EmulationVersion: "1.30.0",
				LeaseName:        "test",
				Strategy:         coordination.OldestEmulationVersion,
			},
		},
		{
			// 自定义策略不强制要求 emulationVersion，因此该用例合法。
			"valid custom strategy should not require emulationVersion",
			false,
			&coordination.LeaseCandidateSpec{
				LeaseName:     "test",
				BinaryVersion: "1.30.0",
				Strategy:      coordination.CoordinatedLeaseStrategy("custom.com/foo"),
			},
		},
		{
			// binaryVersion 为必填项，缺失应报错。
			"binaryVersion is required",
			true,
			&coordination.LeaseCandidateSpec{
				LeaseName: "test",
				Strategy:  coordination.CoordinatedLeaseStrategy("custom.com/foo"),
			},
		},
		{
			// LeaseName 为空，候选者必须关联一个租约。
			"no lease name",
			true,
			&coordination.LeaseCandidateSpec{
				BinaryVersion:    "1.30.0",
				EmulationVersion: "1.30.0",
			},
		},
		{
			// "1.30.1.6" 有四个版本段，不是合法语义化版本。
			"bad binaryVersion",
			true,
			&coordination.LeaseCandidateSpec{
				BinaryVersion: "1.30.1.6",
				LeaseName:     "test",
			},
		},
		{
			// emulationVersion 低于 binaryVersion，违反"不得低于二进制版本"的约束。
			"emulation should be greater than or equal to binary version",
			true,
			&coordination.LeaseCandidateSpec{
				EmulationVersion: "1.30.0",
				BinaryVersion:    "1.29.0",
				LeaseName:        "test",
			},
		},
		{
			// "foo" 不是合法的策略格式（需为内置值或带域名的路径）。
			"strategy bad",
			true,
			&coordination.LeaseCandidateSpec{
				BinaryVersion:    "1.30.1",
				EmulationVersion: "1.30.1",
				LeaseName:        "test",
				Strategy:         coordination.CoordinatedLeaseStrategy("foo"),
			},
		},
		{
			// 策略为必填项，缺失应报错。
			"strategy missing",
			true,
			&coordination.LeaseCandidateSpec{
				BinaryVersion:    "1.30.1",
				EmulationVersion: "1.30.1",
				LeaseName:        "test",
			},
		},
		{
			// 使用内置策略 OldestEmulationVersion 时必须提供 emulationVersion。
			"strategy good but emulationVersion missing",
			true,
			&coordination.LeaseCandidateSpec{
				BinaryVersion: "1.30.1",
				LeaseName:     "test",
				Strategy:      coordination.OldestEmulationVersion,
			},
		},
	}

	for _, tc := range testcases {
		// 使用固定路径 "foo"，重点关注是否报错而非错误路径。
		errs := ValidateLeaseCandidateSpec(tc.spec, field.NewPath("foo"))
		// 对比期望与实际的报错情况；错误信息中带上用例名便于定位。
		if len(errs) > 0 && !tc.shouldErr {
			t.Errorf("unexpected list of errors: %#v", errs.ToAggregate().Error())
		} else if len(errs) == 0 && tc.shouldErr {
			t.Errorf("Expected err, got no error for tc: %s", tc.name)
		}
	}
}

// TestValidateLeaseCandidateUpdate 验证 LeaseCandidate 更新时的校验规则。
// 表格覆盖两个场景：字段未变化的合法更新，以及修改 LeaseName 的非法更新。
// LeaseName 一经创建不可更改，因为候选者与租约的关联关系必须保持稳定，修改会破坏协调语义。
func TestValidateLeaseCandidateUpdate(t *testing.T) {
	testcases := []struct {
		name   string
		old    coordination.LeaseCandidate
		update coordination.LeaseCandidate
		err    bool
	}{
		{
			// 新旧 Spec 完全一致，属于合法更新。
			name: "valid update",
			old: coordination.LeaseCandidate{
				Spec: coordination.LeaseCandidateSpec{
					BinaryVersion:    "1.30.0",
					EmulationVersion: "1.30.0",
					LeaseName:        "test",
					Strategy:         coordination.OldestEmulationVersion,
				},
			},
			update: coordination.LeaseCandidate{
				Spec: coordination.LeaseCandidateSpec{
					BinaryVersion:    "1.30.0",
					EmulationVersion: "1.30.0",
					LeaseName:        "test",
					Strategy:         coordination.OldestEmulationVersion,
				},
			},
			err: false,
		},
		{
			// 修改 LeaseName 违反不可变字段约束，应报错。
			name: "update LeaseName should fail",
			old: coordination.LeaseCandidate{
				Spec: coordination.LeaseCandidateSpec{
					BinaryVersion:    "1.30.0",
					EmulationVersion: "1.30.0",
					LeaseName:        "test",
					Strategy:         coordination.OldestEmulationVersion,
				},
			},
			update: coordination.LeaseCandidate{
				Spec: coordination.LeaseCandidateSpec{
					BinaryVersion:    "1.30.0",
					EmulationVersion: "1.30.0",
					LeaseName:        "test-update",
					Strategy:         coordination.OldestEmulationVersion,
				},
			},
			err: true,
		},
	}

	for _, tc := range testcases {
		// 为新旧对象设置相同的 ResourceVersion，排除版本校验干扰，只验证 Spec 的不变性规则。
		tc.old.ResourceVersion = "1"
		tc.update.ResourceVersion = "1"
		errs := ValidateLeaseCandidateUpdate(&tc.update, &tc.old)
		// 对比期望与实际报错；错误信息中带上用例名便于定位。
		if tc.err && len(errs) == 0 {
			t.Errorf("Expected err, got no err for tc: %s", tc.name)
		} else if !tc.err && len(errs) != 0 {
			t.Errorf("Expected no err, got err %v for tc: %s", errs, tc.name)

		}
	}
}

// TestValidateCoordinatedLeaseStrategy 验证协调租约策略名称的格式校验。
// 表格覆盖：无斜杠的裸名称（非法，自定义策略须形如域名/名称）、斜杠过多导致嵌套过深的路径（非法）、
// 内置策略 OldestEmulationVersion（合法），以及合法的自定义策略路径 "example.com/foobar"。
// 该校验确保自定义策略带有域名前缀且层级固定，避免命名冲突与歧义。
func TestValidateCoordinatedLeaseStrategy(t *testing.T) {
	testcases := []struct {
		strategy coordination.CoordinatedLeaseStrategy
		err      bool
	}{
		{
			// "foobar" 缺少域名前缀，不是合法的自定义策略格式。
			coordination.CoordinatedLeaseStrategy("foobar"),
			true,
		},
		{
			// 三段路径嵌套过深，超出允许的层级。
			coordination.CoordinatedLeaseStrategy("example.com/foobar/toomanyslashes"),
			true,
		},
		{
			// 内置策略为合法取值。

			coordination.CoordinatedLeaseStrategy(coordination.OldestEmulationVersion),
			false,
		},
		{
			// 标准的两段式自定义策略路径合法。
			coordination.CoordinatedLeaseStrategy("example.com/foobar"),
			false,
		},
	}

	for _, tc := range testcases {
		// 使用固定路径 "foo"，重点关注策略本身是否被判定为合法。
		errs := ValidateCoordinatedLeaseStrategy(tc.strategy, field.NewPath("foo"))
		if tc.err && len(errs) == 0 {
			t.Error("Expected err, got no err")
		} else if !tc.err && len(errs) != 0 {
			t.Errorf("Expected no err, got err %v", errs)
		}
	}
}
