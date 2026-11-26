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

package dependentforbidden

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// TestStruct 覆盖依赖约束的核心语义与 ratcheting：
// dependent 单独存在合法，trigger 单独存在合法，二者同时存在则 Forbidden；
// 若旧值中两者已经同时存在，则更新时跳过（避免存量对象永久不可写）。
func TestStruct(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 单向：仅 dependent 设置是合法的。
	st.Value(&Struct{Dependent: new("d")}).ExpectValid()

	// 仅 trigger 设置是合法的。
	st.Value(&Struct{Trigger: new("t")}).ExpectValid()

	// 两者都设置 → 禁止。
	st.Value(&Struct{Trigger: new("t"), Dependent: new("d")}).ExpectMatches(
		field.ErrorMatcher{}.ByType().ByField().ByOrigin(),
		field.ErrorList{
			field.Forbidden(field.NewPath("dependent"), "").WithOrigin("dependentForbidden"),
		},
	)

	// Ratchet：无关字段变化、trigger 与 dependent 的设置状态不变 → 跳过。
	st.Value(&Struct{Trigger: new("t"), Dependent: new("d"), OtherField: new("new")}).
		OldValue(&Struct{Trigger: new("t"), Dependent: new("d"), OtherField: new("old")}).
		ExpectValid()

	// Ratchet：trigger 值变化但设置状态不变 → 跳过。
	st.Value(&Struct{Trigger: new("t"), Dependent: new("d")}).
		OldValue(&Struct{Trigger: new("old"), Dependent: new("d")}).
		ExpectValid()

	// 新设置的 dependent → 触发。
	st.Value(&Struct{Trigger: new("t"), Dependent: new("d")}).
		OldValue(&Struct{Trigger: new("t")}).
		ExpectMatches(
			field.ErrorMatcher{}.ByType().ByField().ByOrigin(),
			field.ErrorList{
				field.Forbidden(field.NewPath("dependent"), "").WithOrigin("dependentForbidden"),
			},
		)
}

// TestMultiDependent 验证重复标签生成的多条独立约束，
// 每个 dependent 各自报错于自己的路径。
func TestMultiDependent(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 重复标签 → 各自独立生效，作用于各自的 dependent 路径。
	st.Value(&MultiDependent{Trigger: new("t"), DependentA: new("a"), DependentB: new("b")}).ExpectMatches(
		field.ErrorMatcher{}.ByType().ByField().ByOrigin(),
		field.ErrorList{
			field.Forbidden(field.NewPath("dependentA"), "").WithOrigin("dependentForbidden"),
			field.Forbidden(field.NewPath("dependentB"), "").WithOrigin("dependentForbidden"),
		},
	)
}

// TestMultiTrigger 验证多个 trigger 指向同一 dependent 时，
// 匹配器按 origin 合并实际错误即可。
func TestMultiTrigger(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 不同 trigger → 在同一路径上各自独立生效。
	// ByOrigin 会合并两个来源的实际值。
	st.Value(&MultiTrigger{TriggerA: new("a"), TriggerB: new("b"), Dependent: new("d")}).ExpectMatches(
		field.ErrorMatcher{}.ByType().ByField().ByOrigin(),
		field.ErrorList{
			field.Forbidden(field.NewPath("dependent"), "").WithOrigin("dependentForbidden"),
		},
	)
}

// TestAllKinds 验证四类“已设置”判定：
// 指针非 nil、slice/map 非空、整型非零才视为设置；空集合与零值不算。
func TestAllKinds(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 当 trigger 与 dependent 都设置时，每种类型的提取器都会触发。
	st.Value(&AllKinds{
		PtrTrigger:   new("t"),
		PtrDep:       new("d"),
		SliceTrigger: []string{"x"},
		SliceDep:     []string{"y"},
		MapTrigger:   map[string]string{"k": "v"},
		MapDep:       map[string]string{"k": "v"},
		IntTrigger:   1,
		IntDep:       1,
	}).ExpectMatches(
		field.ErrorMatcher{}.ByType().ByField().ByOrigin(),
		field.ErrorList{
			field.Forbidden(field.NewPath("ptrDep"), "").WithOrigin("dependentForbidden"),
			field.Forbidden(field.NewPath("sliceDep"), "").WithOrigin("dependentForbidden"),
			field.Forbidden(field.NewPath("mapDep"), "").WithOrigin("dependentForbidden"),
			field.Forbidden(field.NewPath("intDep"), "").WithOrigin("dependentForbidden"),
		},
	)

	// trigger 已设置但 dependent “未设置”（空 slice/map、零值整数、nil 指针）→ 合法。
	st.Value(&AllKinds{
		PtrTrigger:   new("t"),
		SliceTrigger: []string{"x"},
		MapTrigger:   map[string]string{"k": "v"},
		IntTrigger:   1,
		SliceDep:     []string{},
		MapDep:       map[string]string{},
		IntDep:       0,
	}).ExpectValid()
}
