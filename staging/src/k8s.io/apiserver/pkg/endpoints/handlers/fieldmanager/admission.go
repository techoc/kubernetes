/*
Copyright 2021 The Kubernetes Authors.

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

package fieldmanager

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/util/managedfields"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/warning"
)

// InvalidManagedFieldsAfterMutatingAdmissionWarningFormat is the warning that a client receives
// when a create/update/patch request results in invalid managedFields after going through the admission chain.
const InvalidManagedFieldsAfterMutatingAdmissionWarningFormat = ".metadata.managedFields was in an invalid state after admission; this could be caused by an outdated mutating admission controller; please fix your requests: %v"

// NewManagedFieldsValidatingAdmissionController validates the managedFields after calling
// the provided admission and resets them to their original state if they got changed to an invalid value
//
// 包装器而非改造每个插件：managedFields 只能由服务器写入，任何插件误改都
// 应被回滚并提示，而不是让无效状态进入存储。
func NewManagedFieldsValidatingAdmissionController(wrap admission.Interface) admission.Interface {
	if wrap == nil {
		return nil
	}
	return &managedFieldsValidatingAdmissionController{wrap: wrap}
}

// managedFieldsValidatingAdmissionController 把准入调用包裹在 managedFields 快照/校验之间。
type managedFieldsValidatingAdmissionController struct {
	wrap admission.Interface
}

var _ admission.Interface = &managedFieldsValidatingAdmissionController{}
var _ admission.MutationInterface = &managedFieldsValidatingAdmissionController{}
var _ admission.ValidationInterface = &managedFieldsValidatingAdmissionController{}

// Handles calls the wrapped admission.Interface if applicable
func (admit *managedFieldsValidatingAdmissionController) Handles(operation admission.Operation) bool {
	return admit.wrap.Handles(operation)
}

// Admit calls the wrapped admission.Interface if applicable and resets the managedFields to their state before admission if they
// got modified in an invalid way
func (admit *managedFieldsValidatingAdmissionController) Admit(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) (err error) {
	mutationInterface, isMutationInterface := admit.wrap.(admission.MutationInterface)
	if !isMutationInterface {
		return nil
	}
	objectMeta, err := meta.Accessor(a.GetObject())
	if err != nil {
		// the object we are dealing with doesn't have object metadata defined
		// in that case we don't have to keep track of the managedField
		// just call the wrapped admission
		return mutationInterface.Admit(ctx, a, o)
	}
	// 在调用前保存引用切片；随后插件可能替换整个 managedFields，故此处只保存浅拷贝即可，
	// 因为回滚时直接整块替换。
	managedFieldsBeforeAdmission := objectMeta.GetManagedFields()
	if err := mutationInterface.Admit(ctx, a, o); err != nil {
		return err
	}
	managedFieldsAfterAdmission := objectMeta.GetManagedFields()
	if err := managedfields.ValidateManagedFields(managedFieldsAfterAdmission); err != nil {
		// 回滚而非拒绝：历史插件可能无意破坏该字段，拒绝会导致升级后请求全挂。
		// 以 warning 告知客户端，便于定位有问题的插件。
		objectMeta.SetManagedFields(managedFieldsBeforeAdmission)
		warning.AddWarning(ctx, "",
			fmt.Sprintf(InvalidManagedFieldsAfterMutatingAdmissionWarningFormat,
				err.Error()),
		)
	}
	return nil
}

// Validate calls the wrapped admission.Interface if aplicable
func (admit *managedFieldsValidatingAdmissionController) Validate(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) (err error) {
	if validationInterface, isValidationInterface := admit.wrap.(admission.ValidationInterface); isValidationInterface {
		return validationInterface.Validate(ctx, a, o)
	}
	return nil
}
