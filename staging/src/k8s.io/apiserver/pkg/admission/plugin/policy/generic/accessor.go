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

package generic

import (
	"k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/types"
)

// PolicyAccessor is the narrow view of an admission policy that the generic
// dispatcher needs. Both ValidatingAdmissionPolicy and MutatingAdmissionPolicy
// implement it, so the dispatcher logic can be shared between the two plugins.
type PolicyAccessor interface {
	// GetName returns the policy name (cluster-scoped).
	GetName() string
	// GetNamespace is always empty for current cluster-scoped policies; it is
	// present so a future namespaced policy variant can reuse this interface.
	GetNamespace() string
	// GetParamKind describes the Kind of the policy's param resource, or nil
	// when the policy takes no parameters.
	GetParamKind() *v1.ParamKind
	// GetMatchConstraints restricts which requests the policy applies to.
	GetMatchConstraints() *v1.MatchResources
	// GetFailurePolicy decides behavior when evaluation fails; nil means Fail.
	GetFailurePolicy() *v1.FailurePolicyType
}

// BindingAccessor is the narrow view of a policy binding that the generic
// dispatcher needs. ValidatingAdmissionPolicyBinding and
// MutatingAdmissionPolicyBinding both implement it.
type BindingAccessor interface {
	GetName() string
	GetNamespace() string

	// GetPolicyName returns the name of the (Validating/Mutating)AdmissionPolicy,
	// which is cluster-scoped, so namespace is usually left blank.
	// But we leave the door open to add a namespaced vesion in the future
	GetPolicyName() types.NamespacedName
	// GetParamRef points at the param object this binding supplies, if any.
	GetParamRef() *v1.ParamRef

	// GetMatchResources further narrows the policy's match constraints.
	GetMatchResources() *v1.MatchResources
}
