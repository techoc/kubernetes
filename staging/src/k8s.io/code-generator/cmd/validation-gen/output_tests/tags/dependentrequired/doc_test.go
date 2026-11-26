/*
Copyright The Kubernetes Authors.

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

package dependentrequired

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
)

// TestStruct 覆盖单向依赖的基本语义：只有触发字段已设置而依赖字段未设置时报错；
// 以及基于“设置状态”而非值的 ratcheting。
func TestStruct(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 单向：仅 dependent 设置是合法的。
	st.Value(&Struct{Dependent: ptr.To("d")}).ExpectValid()

	st.Value(&Struct{Trigger: ptr.To("t"), Dependent: ptr.To("d")}).ExpectValid()

	st.Value(&Struct{Trigger: ptr.To("t")}).ExpectMatches(
		field.ErrorMatcher{}.ByType().ByField().ByOrigin(),
		field.ErrorList{
			field.Required(field.NewPath("dependent"), "").WithOrigin("dependentRequired"),
		},
	)

	// Ratchet：无关字段变化、trigger 与 dependent 的设置状态不变 → 跳过。
	st.Value(&Struct{Trigger: ptr.To("t"), OtherField: ptr.To("new")}).
		OldValue(&Struct{Trigger: ptr.To("t"), OtherField: ptr.To("old")}).
		ExpectValid()

	// Ratchet：trigger 值变化但设置状态不变 → 跳过（与 union 同理）。
	st.Value(&Struct{Trigger: ptr.To("t")}).
		OldValue(&Struct{Trigger: ptr.To("old")}).
		ExpectValid()

	// 新清空的 dependent → 触发。
	st.Value(&Struct{Trigger: ptr.To("t")}).
		OldValue(&Struct{Trigger: ptr.To("t"), Dependent: ptr.To("d")}).
		ExpectMatches(
			field.ErrorMatcher{}.ByType().ByField().ByOrigin(),
			field.ErrorList{
				field.Required(field.NewPath("dependent"), "").WithOrigin("dependentRequired"),
			},
		)
}

// TestMultiDependent 一个触发字段声明多个依赖：每个依赖独立报错。
func TestMultiDependent(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 重复标签 → 各自独立生效，作用于各自的 dependent 路径。
	st.Value(&MultiDependent{Trigger: ptr.To("t")}).ExpectMatches(
		field.ErrorMatcher{}.ByType().ByField().ByOrigin(),
		field.ErrorList{
			field.Required(field.NewPath("dependentA"), "").WithOrigin("dependentRequired"),
			field.Required(field.NewPath("dependentB"), "").WithOrigin("dependentRequired"),
		},
	)
}

// TestMultiTrigger 多个触发字段依赖同一字段：错误路径相同，由 origin 归并。
func TestMultiTrigger(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 不同 trigger → 在同一路径上各自独立生效。
	// ByOrigin 会合并两个来源的实际值。
	st.Value(&MultiTrigger{TriggerA: ptr.To("a"), TriggerB: ptr.To("b")}).ExpectMatches(
		field.ErrorMatcher{}.ByType().ByField().ByOrigin(),
		field.ErrorList{
			field.Required(field.NewPath("dependent"), "").WithOrigin("dependentRequired"),
		},
	)
}

// TestAllKinds 覆盖“已设置”的判定：指针非 nil、slice/map 非空、内建类型非零值。
func TestAllKinds(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 每种类型的提取器都会触发。
	st.Value(&AllKinds{
		PtrTrigger:   ptr.To("t"),
		SliceTrigger: []string{"x"},
		MapTrigger:   map[string]string{"k": "v"},
		IntTrigger:   1,
	}).ExpectMatches(
		field.ErrorMatcher{}.ByType().ByField().ByOrigin(),
		field.ErrorList{
			field.Required(field.NewPath("ptrDep"), "").WithOrigin("dependentRequired"),
			field.Required(field.NewPath("sliceDep"), "").WithOrigin("dependentRequired"),
			field.Required(field.NewPath("mapDep"), "").WithOrigin("dependentRequired"),
			field.Required(field.NewPath("intDep"), "").WithOrigin("dependentRequired"),
		},
	)

	// 空 slice/map 与整数零值 = “未设置”。
	st.Value(&AllKinds{
		SliceTrigger: []string{},
		MapTrigger:   map[string]string{},
		IntTrigger:   0,
	}).ExpectValid()
}
