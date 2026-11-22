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

package validate

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// DependentRequired verifies that when triggerIsSet(obj) is true, dependentIsSet(obj)
// is also true; otherwise reports an error at fldPath.Child(dependentName).
// On Update, the check is skipped if neither side's set-ness changed from oldObj,
// so unrelated updates can proceed past a pre-existing violation.
// 中文补充：允许“既有的非法数据继续存在”，避免历史对象因新增校验而无法做无关更新（ratcheting）。
func DependentRequired[T any](_ context.Context, op operation.Operation, fldPath *field.Path, obj, oldObj *T,
	triggerName string, triggerIsSet ExtractorFn[*T, bool],
	dependentName string, dependentIsSet ExtractorFn[*T, bool],
) field.ErrorList {
	if obj == nil {
		return nil
	}
	// Update 时若触发位与依赖位都未变化，说明本次修改与这条约束无关，放行。
	if op.Type == operation.Update && oldObj != nil {
		if triggerIsSet(obj) == triggerIsSet(oldObj) && dependentIsSet(obj) == dependentIsSet(oldObj) {
			return nil
		}
	}
	// 只在触发器被设置时才要求依赖项存在。
	if !triggerIsSet(obj) {
		return nil
	}
	if dependentIsSet(obj) {
		return nil
	}
	return field.ErrorList{
		field.Required(fldPath.Child(dependentName),
			fmt.Sprintf("must be set when %s is set", triggerName)).
			WithOrigin("dependentRequired"),
	}
}

// DependentForbidden verifies that when triggerIsSet(obj) is true, dependentIsSet(obj)
// is false; otherwise reports an error at fldPath.Child(dependentName).
// On Update, the check is skipped if neither side's set-ness changed from oldObj,
// so unrelated updates can proceed past a pre-existing violation.
// 中文补充：与 DependentRequired 对称，但语义相反——触发器存在时依赖项必须不存在。
func DependentForbidden[T any](_ context.Context, op operation.Operation, fldPath *field.Path, obj, oldObj *T,
	triggerName string, triggerIsSet ExtractorFn[*T, bool],
	dependentName string, dependentIsSet ExtractorFn[*T, bool],
) field.ErrorList {
	if obj == nil {
		return nil
	}
	// 同 DependentRequired：两者状态都未变化时不做判定，兼容历史数据。
	if op.Type == operation.Update && oldObj != nil {
		if triggerIsSet(obj) == triggerIsSet(oldObj) && dependentIsSet(obj) == dependentIsSet(oldObj) {
			return nil
		}
	}
	if !triggerIsSet(obj) {
		return nil
	}
	// 触发器未设置或依赖项未设置，都不会违反“互斥”约束。
	if !dependentIsSet(obj) {
		return nil
	}
	return field.ErrorList{
		field.Forbidden(fldPath.Child(dependentName),
			fmt.Sprintf("may not be set when %s is set", triggerName)).
			WithOrigin("dependentForbidden"),
	}
}
